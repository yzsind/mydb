//! Public monitor contracts and the complete structured diagnostic surface.

use super::support::execute_materialized as exec;
use super::*;

fn report(engine: &StorageQueryEngine, session: &mut SessionContext) -> String {
    let QueryResponse::ResultSet(rs) = exec(engine, session, "SHOW ENGINE MYDB STATUS").unwrap()
    else {
        panic!("expected monitor");
    };
    assert_eq!(rs.rows.len(), 1);
    rs.rows[0][2].to_display_string()
}

#[test]
fn engine_monitor_sections_metadata_and_checkpoint() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let initial = report(&engine, &mut session);
    let mut previous = 0;
    for section in [
        "OVERVIEW",
        "TRANSACTIONS",
        "LOCKS",
        "WAL AND CHECKPOINT",
        "BUFFER POOL AND I/O",
        "MEMORY AND EXECUTION",
        "UNDO AND PURGE",
        "CACHES",
    ] {
        let position = initial.find(&format!("\n{section}\n")).unwrap();
        assert!(position > previous);
        previous = position;
    }
    assert!(initial.contains(" UTC"));
    assert!(initial.contains("near-time snapshots"));
    assert!(initial.contains("never"));
    assert!(initial.contains("Oldest wait edges (up to 10):\n  none"));
    assert!(initial.len() < 16 * 1024);
    for sql in ["SHOW ENGINE MYDB STATUS", "SHOW ENGINE INNODB STATUS"] {
        let columns = engine
            .describe_prepared_statement(&mut session, sql)
            .unwrap();
        assert_eq!(
            columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            ["Type", "Name", "Status"]
        );
        assert!(columns
            .iter()
            .all(|c| c.column_type == ColumnType::VarString));
    }
    exec(
        &engine,
        &mut session,
        "CREATE TABLE monitor_t (id INT PRIMARY KEY)",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO monitor_t VALUES (1)").unwrap();
    engine.checkpoint().unwrap();
    let after = report(&engine, &mut session);
    assert!(after.contains("Last successful checkpoint"));
    assert!(!after.contains("never"));
    assert!(after.contains("manual"));
    assert!(after.contains("Last checkpoint duration"));
}

#[test]
fn engine_metrics_complete_filterable_and_exact() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let names: std::collections::BTreeSet<_> =
        crate::engine::engine_status::metric_pairs(&engine.db)
            .into_iter()
            .map(|(name, _)| name)
            .collect();
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT METRIC_NAME, METRIC_VALUE FROM information_schema.MYDB_ENGINE_STATUS",
    )
    .unwrap() else {
        panic!("expected metrics");
    };
    assert_eq!(rs.columns[0].name, "METRIC_NAME");
    assert_eq!(rs.columns[1].column_type, ColumnType::Blob);
    let actual: std::collections::BTreeSet<_> =
        rs.rows.iter().map(|r| r[0].to_display_string()).collect();
    assert_eq!(actual, names);
    assert_eq!(rs.rows.len(), names.len(), "metric names must be unique");
    for key in [
        "durability",
        "current_lsn",
        "hash_join_bloom_built",
        "hnsw_indexes",
        "zone_map_enabled",
        "controlled_memory_current_bytes",
    ] {
        assert!(names.contains(key), "missing {key}");
    }
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut session,
        "SELECT CAST(METRIC_VALUE AS UNSIGNED) AS lsn FROM information_schema.MYDB_ENGINE_STATUS WHERE METRIC_NAME='current_lsn'").unwrap() else {
        panic!("expected lsn");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(
        rs.rows[0][0].to_display_string(),
        engine.db.storage_status().current_lsn.to_string()
    );
}

#[test]
fn engine_monitor_and_metrics_require_process() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    exec(
        &engine,
        &mut root,
        "CREATE USER 'monitor_user'@'%' IDENTIFIED BY 'pw'",
    )
    .unwrap();
    let mut user = SessionContext::default();
    user.user = "monitor_user".into();
    user.login_user = "monitor_user".into();
    user.authenticated_as = crate::engine::auth::AccountKey::new("monitor_user", "%");
    user.auth_epoch = engine.db.auth_catalog().epoch;
    for sql in ["SHOW ENGINE MYDB STATUS", "SHOW ENGINE INNODB STATUS"] {
        assert_eq!(exec(&engine, &mut user, sql).unwrap_err().code(), 1227);
    }
    let sql = "SELECT * FROM information_schema.MYDB_ENGINE_STATUS";
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut user, sql).unwrap() else {
        panic!("expected metrics");
    };
    assert!(rs.rows.is_empty());
    exec(
        &engine,
        &mut root,
        "GRANT PROCESS ON *.* TO 'monitor_user'@'%'",
    )
    .unwrap();
    user.auth_epoch = engine.db.auth_catalog().epoch;
    assert!(report(&engine, &mut user).contains("MYDB MONITOR OUTPUT"));
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut user, sql).unwrap() else {
        panic!("expected metrics");
    };
    assert!(!rs.rows.is_empty());
}

#[test]
fn engine_monitor_and_metrics_do_not_commit_user_transaction() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE monitor_t (id INT PRIMARY KEY)",
    )
    .unwrap();
    exec(&engine, &mut session, "BEGIN").unwrap();
    exec(&engine, &mut session, "INSERT INTO monitor_t VALUES (1)").unwrap();
    let txn = session.current_txn;
    for sql in [
        "SHOW ENGINE MYDB STATUS",
        "SHOW ENGINE INNODB STATUS",
        "SELECT * FROM information_schema.MYDB_ENGINE_STATUS",
    ] {
        exec(&engine, &mut session, sql).unwrap();
        assert_eq!(session.current_txn, txn);
        let QueryResponse::ResultSet(rs) =
            engine.execute("SELECT COUNT(*) FROM monitor_t").unwrap()
        else {
            panic!("expected count");
        };
        assert_eq!(rs.rows[0][0].to_display_string(), "0");
    }
    exec(&engine, &mut session, "ROLLBACK").unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM monitor_t").unwrap()
    else {
        panic!("expected count");
    };
    assert_eq!(rs.rows[0][0].to_display_string(), "0");
}

#[test]
fn engine_monitor_bounds_transaction_details_and_sql() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut transactions = Vec::new();
    for id in 1..=14 {
        let txn = engine.db.begin_txn().unwrap();
        let handle = engine
            .registry
            .register(id, "root".into(), "localhost".into(), "test".into());
        handle.set_current_txn(Some(txn.txn_id));
        handle.mark_busy(
            "Query",
            Some(format!("SELECT '\n{}'", "中".repeat(300))),
            "test",
        );
        transactions.push(txn);
    }
    let text = report(&engine, &mut SessionContext::default());
    let details = engine.db.engine_monitor_details(10);
    for txn in &transactions {
        engine.db.rollback_txn(txn.txn_id).unwrap();
    }
    assert_eq!(details.transaction_count, 14);
    assert_eq!(details.transactions.len(), 10);
    assert_eq!(
        details
            .transactions
            .iter()
            .map(|t| t.transaction_id)
            .collect::<Vec<_>>(),
        transactions
            .iter()
            .take(10)
            .map(|t| t.txn_id)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        text.lines()
            .filter(|line| line.starts_with("  trx="))
            .count(),
        10
    );
    assert!(text.contains("4 more omitted"));
    assert!(text.contains("\\n"));
    assert!(text.contains("..."));
    assert!(!text.contains(&"中".repeat(300)));
}

#[test]
fn engine_monitor_reports_waiter_blocker_and_cleared_wait() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = Arc::new(engine_with_lock_wait_ms(dir.path(), 10_000));
    let mut owner = SessionContext::default();
    exec(
        &engine,
        &mut owner,
        "CREATE TABLE monitor_lock (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    exec(&engine, &mut owner, "INSERT INTO monitor_lock VALUES (1,0)").unwrap();
    exec(&engine, &mut owner, "BEGIN").unwrap();
    exec(
        &engine,
        &mut owner,
        "UPDATE monitor_lock SET v=1 WHERE id=1",
    )
    .unwrap();
    let owner_txn = owner.current_txn.unwrap();
    let handle = engine
        .registry
        .register(101, "root".into(), "localhost".into(), "test".into());
    handle.set_current_txn(Some(owner_txn));
    let other = Arc::clone(&engine);
    let worker = std::thread::spawn(move || {
        let mut waiter = SessionContext::default();
        waiter.connection_id = 102;
        exec(&other, &mut waiter, "BEGIN").unwrap();
        exec(&other, &mut waiter, "INSERT INTO monitor_lock VALUES (2,0)").unwrap();
        let handle = other
            .registry
            .register(102, "root".into(), "localhost".into(), "test".into());
        handle.set_current_txn(waiter.current_txn);
        let result = exec(
            &other,
            &mut waiter,
            "UPDATE monitor_lock SET v=2 WHERE id=1",
        );
        exec(&other, &mut waiter, "ROLLBACK").unwrap();
        result
    });
    let deadline = Instant::now() + Duration::from_secs(3);
    while engine.db.mysql_status().active_lock_waits == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    let text = report(&engine, &mut SessionContext::default());
    let storage_waits = engine.db.storage_status().active_lock_waits;
    let mysql_waits = engine.db.mysql_status().active_lock_waits;
    let QueryResponse::ResultSet(metrics) = engine.execute(
        "SELECT METRIC_VALUE FROM information_schema.MYDB_ENGINE_STATUS WHERE METRIC_NAME='active_lock_waits'"
    ).unwrap() else { panic!("expected wait metric"); };
    exec(&engine, &mut owner, "ROLLBACK").unwrap();
    worker.join().unwrap().unwrap();
    assert!(
        text.contains(&format!("blocked_by={owner_txn} connection=101")),
        "{text}"
    );
    assert!(text.contains("connection=102"), "{text}");
    assert!(text.contains("object=test.monitor_lock"), "{text}");
    assert_eq!(storage_waits, 1);
    assert_eq!(mysql_waits, 1);
    assert_eq!(metrics.rows[0][0].to_display_string(), "1");
    assert_eq!(engine.db.mysql_status().active_lock_waits, 0);
    assert!(report(&engine, &mut SessionContext::default())
        .contains("Oldest wait edges (up to 10):\n  none"));
}
