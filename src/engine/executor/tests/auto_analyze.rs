//! Integration tests for background Auto-ANALYZE
//! (`Database::auto_analyze_stale_tables`) and the `mydb_auto_analyze_*`
//! system variables.

use super::*;

/// Fetch a table's `(row_count, modified_rows_since_analyze)` from the
/// catalog by unqualified name in the default `test` database.
fn table_stats(engine: &StorageQueryEngine, name: &str) -> Option<(u64, u64)> {
    let catalog = engine.database().catalog();
    catalog
        .tables
        .values()
        .find(|t| t.name == name && t.database == "test")
        .map(|t| (t.stats.row_count, t.stats.modified_rows_since_analyze))
}

#[test]
fn auto_analyze_refreshes_stale_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    // Seed enough rows that the table is over the small-table (< 64)
    // floor, then ANALYZE to establish a baseline row_count and reset
    // modified_rows_since_analyze.
    for i in 0..200 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 2))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE t").unwrap();
    let (rows0, _) = table_stats(&engine, "t").expect("table t");
    assert!(rows0 >= 200, "baseline row_count = {rows0}");

    // Mutate well past 10% of the row count so the table is stale.
    for i in 200..260 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 2))
            .unwrap();
    }

    // The table must now register as stale via the live delta.
    let table_id = engine.database().table_id_for_test("t").unwrap();
    let delta = engine.database().table_modification_delta(table_id);
    assert!(delta >= 60, "expected accumulated delta, got {delta}");

    // Background pass refreshes >= 1 table.
    let refreshed = engine.database().auto_analyze_stale_tables(0.1).unwrap();
    assert!(
        refreshed >= 1,
        "expected >= 1 table refreshed, got {refreshed}"
    );

    // After refresh: the delta is drained and the row_count reflects
    // the new total, so the table is no longer stale.
    let delta_after = engine.database().table_modification_delta(table_id);
    assert_eq!(delta_after, 0, "delta should be drained after refresh");
    let (rows1, modified1) = table_stats(&engine, "t").expect("table t");
    assert!(rows1 >= 260, "row_count after refresh = {rows1}");
    assert_eq!(modified1, 0, "modified_rows reset after refresh");

    // Second pass with no new mutations: nothing stale, nothing refreshed.
    let refreshed2 = engine.database().auto_analyze_stale_tables(0.1).unwrap();
    assert_eq!(refreshed2, 0, "no stale tables on a clean second pass");
}

#[test]
fn auto_analyze_skips_unmodified_and_empty_tables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Empty table: never stale (zero pending modifications).
    engine
        .execute("CREATE TABLE empty_t (id INT PRIMARY KEY)")
        .unwrap();
    // A freshly analyzed populated table with no further changes.
    engine
        .execute("CREATE TABLE stable_t (id INT PRIMARY KEY)")
        .unwrap();
    for i in 0..100 {
        engine
            .execute(&format!("INSERT INTO stable_t VALUES ({i})"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE stable_t").unwrap();

    let refreshed = engine.database().auto_analyze_stale_tables(0.1).unwrap();
    assert_eq!(refreshed, 0, "no table should be stale, got {refreshed}");
}

#[test]
fn auto_analyze_does_not_touch_system_databases() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // A pass on a fresh database (only system catalogs + no user tables)
    // must refresh nothing and never error on information_schema / mysql /
    // performance_schema / sys.
    let refreshed = engine.database().auto_analyze_stale_tables(0.1).unwrap();
    assert_eq!(refreshed, 0);
}

#[test]
fn auto_analyze_ratio_sysvar_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("SET GLOBAL mydb_auto_analyze_ratio = 0.25")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute("SHOW VARIABLES LIKE 'mydb_auto_analyze_ratio'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    let val = match &rs.rows[0][1] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected value {other:?}"),
    };
    assert!(
        val.starts_with("0.25"),
        "ratio sysvar should read back 0.25, got {val}"
    );
    // Restore default so the process-global atomic doesn't leak into
    // other tests sharing the same process.
    engine
        .execute("SET GLOBAL mydb_auto_analyze_ratio = 0.1")
        .unwrap();
}

#[test]
fn auto_analyze_interval_and_disable_sysvars() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("SET GLOBAL mydb_auto_analyze_interval_s = 600")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute("SHOW VARIABLES LIKE 'mydb_auto_analyze_interval_s'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows[0][1],
        Value::String("600".to_string()),
        "interval sysvar roundtrip"
    );

    engine
        .execute("SET GLOBAL mydb_auto_analyze_disable = 1")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute("SHOW VARIABLES LIKE 'mydb_auto_analyze_disable'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let disabled = match &rs.rows[0][1] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected value {other:?}"),
    };
    assert!(
        disabled == "ON" || disabled == "1",
        "disable sysvar should read truthy, got {disabled}"
    );

    // Restore process-global defaults.
    engine
        .execute("SET GLOBAL mydb_auto_analyze_disable = 0")
        .unwrap();
    engine
        .execute("SET GLOBAL mydb_auto_analyze_interval_s = 60")
        .unwrap();
}

#[test]
fn stats_health_view_reflects_analyze_and_staleness() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE sh (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 0..200 {
        engine
            .execute(&format!("INSERT INTO sh VALUES ({i}, {})", i % 7))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE sh").unwrap();

    // Right after ANALYZE: the table appears, is not stale, has analyzed
    // columns and a populated TABLE_ROWS.
    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT TABLE_ROWS, MODIFIED_ROWS, IS_STALE, HAS_HISTOGRAM, ANALYZED_COLUMNS \
             FROM information_schema.mydb_stats_health \
             WHERE TABLE_SCHEMA='test' AND TABLE_NAME='sh'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1, "stats_health must list table sh once");
    let row = &rs.rows[0];
    let table_rows = match &row[0] {
        Value::Int(n) => *n,
        other => panic!("TABLE_ROWS not int: {other:?}"),
    };
    assert!(table_rows >= 200, "TABLE_ROWS = {table_rows}");
    assert_eq!(row[2], Value::Int(0), "IS_STALE should be 0 after ANALYZE");
    let analyzed_cols = match &row[4] {
        Value::Int(n) => *n,
        other => panic!("ANALYZED_COLUMNS not int: {other:?}"),
    };
    assert!(analyzed_cols >= 1, "ANALYZED_COLUMNS = {analyzed_cols}");

    // Mutate past the staleness ratio and re-query: IS_STALE flips to 1
    // and MODIFIED_ROWS folds the live delta.
    for i in 200..260 {
        engine
            .execute(&format!("INSERT INTO sh VALUES ({i}, {})", i % 7))
            .unwrap();
    }
    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT MODIFIED_ROWS, IS_STALE \
             FROM information_schema.mydb_stats_health \
             WHERE TABLE_SCHEMA='test' AND TABLE_NAME='sh'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let modified = match &rs.rows[0][0] {
        Value::Int(n) => *n,
        other => panic!("MODIFIED_ROWS not int: {other:?}"),
    };
    assert!(modified >= 60, "MODIFIED_ROWS = {modified}");
    assert_eq!(rs.rows[0][1], Value::Int(1), "IS_STALE should be 1 now");
}

#[test]
fn stats_health_view_excludes_system_schemas() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE only_one (id INT PRIMARY KEY)")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT TABLE_SCHEMA, TABLE_NAME \
             FROM information_schema.mydb_stats_health",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    for row in &rs.rows {
        let schema = match &row[0] {
            Value::String(s) => s.clone(),
            other => panic!("schema not string: {other:?}"),
        };
        assert!(
            !matches!(
                schema.as_str(),
                "information_schema" | "mysql" | "performance_schema" | "sys"
            ),
            "stats_health must not list system schema {schema}"
        );
    }
    assert!(
        rs.rows
            .iter()
            .any(|r| matches!(&r[1], Value::String(s) if s == "only_one")),
        "user table must appear in stats_health"
    );
}

#[test]
fn online_analyze_allows_reads_and_writes_while_scanning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
    let hook = engine.database().install_analyze_test_hook();
    let worker = engine.clone();
    let analyze = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 88001;
        worker.execute_with_session(&mut session, "ANALYZE TABLE t")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let (send, recv) = std::sync::mpsc::channel();
    let worker = engine.clone();
    let dml = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 88002;
        let results = [
            "SELECT * FROM t",
            "INSERT INTO t VALUES(3,30)",
            "UPDATE t SET v=11 WHERE id=1",
            "DELETE FROM t WHERE id=2",
        ]
        .map(|sql| worker.execute_with_session(&mut session, sql));
        send.send(results).unwrap();
    });
    let during = recv.recv_timeout(Duration::from_secs(2));
    hook.release();
    analyze.join().unwrap().unwrap();
    dml.join().unwrap();
    assert!(reached, "ANALYZE did not reach scan hook");
    for result in during.expect("reads and DML must finish before ANALYZE resumes") {
        result.unwrap();
    }
}

#[test]
fn online_analyze_snapshot_survives_commit_and_rollback() {
    for partitioned in [false, true] {
        for commit in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let engine = std::sync::Arc::new(open_test_engine(dir.path()));
            let partition = if partitioned {
                " PARTITION BY HASH(id) PARTITIONS 2"
            } else {
                ""
            };
            engine.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY, v INT, s VARCHAR(40), INDEX vi(v)){partition}")).unwrap();
            engine
                .execute("INSERT INTO t VALUES(1,10,'old'),(2,20,'keep'),(3,NULL,NULL)")
                .unwrap();
            engine.execute("ANALYZE TABLE t").unwrap();
            let db = engine.database();
            let snapshots_before = db.active_snapshot_count();
            let mut writer = SessionContext::default();
            writer.connection_id = 88101;
            for sql in [
                "BEGIN",
                "UPDATE t SET v=1000,s='new' WHERE id=1",
                "DELETE FROM t WHERE id=2",
                "INSERT INTO t VALUES(4,2000,'insert')",
            ] {
                engine.execute_with_session(&mut writer, sql).unwrap();
            }
            let hook = db.install_analyze_test_hook();
            let worker = engine.clone();
            let analyze = std::thread::spawn(move || {
                let mut session = SessionContext::default();
                session.connection_id = 88102;
                worker.execute_with_session(&mut session, "ANALYZE TABLE t UPDATE HISTOGRAM ON v,s")
            });
            let reached = hook.wait_until_reached(Duration::from_secs(10));
            let finish = engine
                .execute_with_session(&mut writer, if commit { "COMMIT" } else { "ROLLBACK" });
            hook.release();
            let result = analyze.join().unwrap();
            assert!(reached, "snapshot hook was not reached: {result:?}");
            finish.unwrap();
            result.unwrap();
            let table = db.table("test.t").unwrap();
            let v = table.stats.columns.get("v").unwrap();
            assert_eq!(
                v.min,
                Some(Value::Int(10)),
                "partitioned={partitioned}, commit={commit}"
            );
            assert_eq!(v.max, Some(Value::Int(20)));
            assert_eq!(v.null_count, 1);
            assert_eq!(v.distinct_count, 2);
            assert_eq!(table.stats.columns.get("s").unwrap().null_count, 1);
            assert_eq!(
                db.active_snapshot_count(),
                snapshots_before,
                "snapshot leaked"
            );
            // The rebuilt zone map must admit values outside the old range.
            let QueryResponse::ResultSet(actual) = engine
                .execute("SELECT id FROM t WHERE v>=1000 ORDER BY id")
                .unwrap()
            else {
                panic!("result set");
            };
            assert_eq!(
                actual.rows,
                if commit {
                    vec![vec![Value::Int(1)], vec![Value::Int(4)]]
                } else {
                    vec![]
                }
            );
            engine.execute("ANALYZE TABLE t").unwrap();
            let refreshed = db.table("test.t").unwrap();
            assert_eq!(
                refreshed.stats.columns["v"].max,
                Some(Value::Int(if commit { 2000 } else { 20 }))
            );
        }
    }
}

#[test]
fn online_analyze_serializes_maintenance_but_other_tables_continue() {
    for contender in [
        "ANALYZE TABLE t",
        "ALTER TABLE t ADD COLUMN extra INT",
        "ANALYZE TABLE t DROP HISTOGRAM ON v",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(open_test_engine(dir.path()));
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
            .unwrap();
        engine
            .execute("CREATE TABLE u(id INT PRIMARY KEY)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
        let hook = engine.database().install_analyze_test_hook();
        let worker = engine.clone();
        let first = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 88201;
            worker.execute_with_session(&mut session, "ANALYZE TABLE t")
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let other_table = engine.execute("ANALYZE TABLE u");
        let worker = engine.clone();
        let (send, recv) = std::sync::mpsc::channel();
        let second = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 88202;
            send.send(worker.execute_with_session(&mut session, contender))
                .unwrap();
        });
        let deadline = std::time::Instant::now() + Duration::from_millis(500);
        let mut pending = false;
        while std::time::Instant::now() < deadline {
            pending = engine
                .database()
                .metadata_lock_snapshot()
                .iter()
                .any(|lock| lock.owner_thread_id == Some(88202) && lock.lock_status == "PENDING");
            if pending {
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        let premature = recv.try_recv();
        hook.release();
        first.join().unwrap().unwrap();
        second.join().unwrap();
        assert!(reached);
        other_table.unwrap();
        assert!(pending, "maintenance did not wait: {contender}");
        assert!(matches!(
            premature,
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        recv.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
    }
}

#[test]
fn online_analyze_retains_concurrent_modifications_and_auto_refresh_skips_busy_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    let rows = (1..=200)
        .map(|i| format!("({i},{i})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO t VALUES{rows}"))
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();
    engine.execute("UPDATE t SET v=v+1").unwrap();
    let db = engine.database();
    let table_id = db.table("test.t").unwrap().id;
    let hook = db.install_analyze_test_hook();
    let worker = engine.clone();
    let analyze = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 88301;
        worker.execute_with_session(&mut session, "ANALYZE TABLE t")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let auto = db.auto_analyze_stale_tables(0.1);
    let mut writer = SessionContext::default();
    writer.connection_id = 88302;
    let insert =
        engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(201,9999),(202,9999)");
    hook.release();
    analyze.join().unwrap().unwrap();
    assert!(reached);
    assert_eq!(
        auto.unwrap(),
        0,
        "busy table must be skipped, not waited on"
    );
    insert.unwrap();
    assert_eq!(
        db.table_modification_delta(table_id),
        2,
        "scan erased concurrent DML"
    );
    let table = db.table("test.t").unwrap();
    assert_eq!(
        table.stats.row_count as i128 + db.table_live_row_delta(table_id) as i128,
        202
    );
    assert_eq!(table.stats.columns["v"].max, Some(Value::Int(201)));
    engine.execute("ANALYZE TABLE t").unwrap();
    assert_eq!(db.table_modification_delta(table_id), 0);
    assert_eq!(db.table_live_row_delta(table_id), 0);
    assert_eq!(db.table("test.t").unwrap().stats.row_count, 202);
}

#[test]
fn online_analyze_auto_refresh_rejects_recreated_table_and_releases_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    let rows = (1..=200)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO t VALUES{rows}"))
        .unwrap();
    let db = engine.database();
    let before = db.active_snapshot_count();
    let hook = db.install_analyze_test_hook();
    let worker = engine.clone();
    let analyze = std::thread::spawn(move || worker.database().auto_analyze_stale_tables(0.1));
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    engine.execute("DROP TABLE t").unwrap();
    engine.execute("CREATE TABLE t(s VARCHAR(20))").unwrap();
    let replacement = db.table("test.t").unwrap();
    hook.release();
    let refreshed = analyze.join().unwrap().unwrap();
    assert!(reached);
    assert_eq!(refreshed, 0);
    let table = db.table("test.t").unwrap();
    assert!(!table.stats.columns.contains_key("id"));
    assert_eq!(
        table.stats.analyzed_at_unix_ms,
        replacement.stats.analyzed_at_unix_ms
    );
    assert_eq!(table.schema_version, replacement.schema_version);
    assert_eq!(db.active_snapshot_count(), before);
}

#[test]
fn online_analyze_large_scan_releases_heap_latch_and_preserves_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, padding VARCHAR(2048))")
        .unwrap();
    let db = engine.database();
    let txn = db.begin_txn().unwrap();
    let padding = "x".repeat(2000);
    db.insert_rows(
        txn,
        "test.t",
        (1..=4096)
            .map(|id| {
                vec![
                    Value::Int(id),
                    Value::Int(id),
                    Value::String(padding.clone()),
                ]
            })
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    let hook = db.install_analyze_batch_test_hook();
    let worker = engine.clone();
    let analyze = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 88401;
        worker.execute_with_session(&mut session, "ANALYZE TABLE t")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(15));
    let worker = engine.clone();
    let (send, recv) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 88402;
        let results = [
            "UPDATE t SET v=100000 WHERE id IN (1,4096)",
            "DELETE FROM t WHERE id=2",
            "INSERT INTO t VALUES(4097,200000,'new')",
        ]
        .map(|sql| worker.execute_with_session(&mut session, sql));
        send.send(results).unwrap();
    });
    let during = recv.recv_timeout(Duration::from_secs(5));
    hook.release();
    let result = analyze.join().unwrap();
    writer.join().unwrap();
    assert!(reached, "batch hook not reached: {result:?}");
    for result in during.expect("heap batch held a latch needed by DML") {
        result.unwrap();
    }
    result.unwrap();
    let table = db.table("test.t").unwrap();
    assert!(
        table.stats.heap_page_count >= 256,
        "fixture must exercise large scans"
    );
    assert_eq!(table.stats.columns["v"].min, Some(Value::Int(1)));
    assert_eq!(table.stats.columns["v"].max, Some(Value::Int(4096)));
    let QueryResponse::ResultSet(actual) = engine
        .execute("SELECT id FROM t WHERE v>=100000 ORDER BY id")
        .unwrap()
    else {
        panic!("result set");
    };
    assert_eq!(
        actual.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(4096)],
            vec![Value::Int(4097)]
        ]
    );
}

#[test]
fn online_analyze_cancel_preserves_stats_deltas_and_releases_guards() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,10)").unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();
    engine.execute("INSERT INTO t VALUES(2,20)").unwrap();
    let db = engine.database();
    let before = db.table("test.t").unwrap();
    let delta = db.table_modification_delta(before.id);
    let live_delta = db.table_live_row_delta(before.id);
    let snapshots = db.active_snapshot_count();
    let txns = db.active_transaction_snapshot().len();
    let token = crate::engine::cancel::QueryCancelToken::noop();
    let worker_token = token.clone();
    let hook = db.install_analyze_test_hook();
    let worker = engine.clone();
    let analyze = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 88501;
        session.current_query_token = Some(worker_token);
        worker.execute_with_session(&mut session, "ANALYZE TABLE t")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    token.cancel_local();
    hook.release();
    let result = analyze.join().unwrap();
    assert!(reached);
    assert_eq!(result.unwrap_err().code(), 1317);
    assert_eq!(
        db.table("test.t").unwrap().schema_version,
        before.schema_version
    );
    assert_eq!(db.table_modification_delta(before.id), delta);
    assert_eq!(db.table_live_row_delta(before.id), live_delta);
    assert_eq!(db.active_snapshot_count(), snapshots);
    assert_eq!(db.active_transaction_snapshot().len(), txns);
    engine.execute("ANALYZE TABLE t").unwrap();
}

#[test]
fn online_analyze_does_not_double_count_deferred_commit_population() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1)").unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();
    let db = engine.database();
    let id = db.table("test.t").unwrap().id;
    let txn = db.begin_txn().unwrap();
    db.insert_rows(txn, "test.t", vec![vec![Value::Int(2)]])
        .unwrap();
    // Row 2 is MVCC-visible, but its signed row delta has not been published.
    let mut deferred = db.commit_txn_deferred(txn.txn_id).unwrap();
    let hook = db.install_analyze_test_hook();
    let worker = engine.clone();
    let analyze = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 88601;
        worker.execute_with_session(&mut session, "ANALYZE TABLE t")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let finish = db.finish_deferred_commits(&mut [&mut deferred]);
    hook.release();
    let result = analyze.join().unwrap();
    assert!(reached, "ANALYZE waited for deferred commit completion");
    finish.unwrap();
    result.unwrap();
    let table = db.table("test.t").unwrap();
    assert_eq!(table.stats.columns["id"].max, Some(Value::Int(2)));
    assert_eq!(
        table.stats.row_count as i128 + db.table_live_row_delta(id) as i128,
        2
    );
    engine.execute("ANALYZE TABLE t").unwrap();
    assert_eq!(db.table("test.t").unwrap().stats.row_count, 2);
    assert_eq!(db.table_live_row_delta(id), 0);
}

/// Manual comparison: run this same fixture on both revisions. Setup and
/// warmup are excluded; no timing assertion depends on machine load.
#[test]
#[ignore = "manual ANALYZE throughput comparison"]
fn online_analyze_warm_throughput_benchmark() {
    for width in [16usize, 768] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, s VARCHAR(1024), INDEX vi(v))")
            .unwrap();
        let db = engine.database();
        let txn = db.begin_txn().unwrap();
        let padding = "x".repeat(width);
        db.insert_rows(
            txn,
            "test.t",
            (1..=8192)
                .map(|id| {
                    vec![
                        Value::Int(id),
                        Value::Int(id % 1000),
                        Value::String(padding.clone()),
                    ]
                })
                .collect(),
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        for _ in 0..3 {
            db.analyze_table("test.t").unwrap();
        }
        let mut times = Vec::new();
        for _ in 0..20 {
            let start = std::time::Instant::now();
            db.analyze_table("test.t").unwrap();
            times.push(start.elapsed().as_micros() as u64);
        }
        assert_eq!(db.table("test.t").unwrap().stats.row_count, 8192);
        eprintln!("ANALYZE_BENCH width={width} us={times:?}");
    }
}
