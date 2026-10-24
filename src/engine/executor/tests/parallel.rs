use super::*;

/// Drive a parallel query through `StorageQueryEngine`. Returns the
/// row payload sorted lexicographically by the first column so equality
/// comparisons against the serial baseline are stable. The test setup
/// always uses degree=4 explicitly because env-driven defaults vary
/// across CI hosts.
fn execute_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    let mut rows = result.rows;
    rows.sort_by(|a, b| {
        let ka = match a.first() {
            Some(Value::Int(v)) => *v,
            _ => 0,
        };
        let kb = match b.first() {
            Some(Value::Int(v)) => *v,
            _ => 0,
        };
        ka.cmp(&kb)
    });
    rows
}

fn execute_rows_in_result_order(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    result.rows
}

fn populate_nested_cte_query(engine: &StorageQueryEngine) -> String {
    engine.execute("CREATE TABLE products (product_id INT PRIMARY KEY, code VARCHAR(20), shade VARCHAR(20)) ZONE_MAP=OFF").unwrap();
    insert_fixture_rows(
        engine,
        "test.products",
        (1..=1000).map(|n| {
            vec![
                Value::Int(n),
                Value::String(format!("p{}", n % 250)),
                if n % 7 == 0 {
                    Value::Null
                } else {
                    Value::String(if n % 3 == 0 { "blue" } else { "red" }.into())
                },
            ]
        }),
    );
    for table in ["sales_a", "sales_b", "sales_c"] {
        engine.execute(&format!("CREATE TABLE {table} (id INT PRIMARY KEY, product INT, amount DECIMAL(12,2)) ZONE_MAP=OFF")).unwrap();
        // Retain every row, NULL and exact DECIMAL cent while bypassing
        // repeated SQL expression evaluation in these admission tests.
        insert_fixture_rows(
            engine,
            &format!("test.{table}"),
            (1..=4000).map(|n| {
                vec![
                    Value::Int(n),
                    Value::Int(n % 1000 + 1),
                    if n % 11 == 0 {
                        Value::Null
                    } else {
                        Value::DecimalCompact(n, 2)
                    },
                ]
            }),
        );
        engine.execute(&format!("ANALYZE TABLE {table}")).unwrap();
    }
    engine.execute("ANALYZE TABLE products").unwrap();
    let branches = ["a", "b", "c"].map(|name| format!(
        "{name} AS (SELECT /*+ PARALLEL(2) */ code, SUM(amount) AS total \
         FROM sales_{name}, products WHERE product=product_id \
         AND code IN (SELECT /*+ PARALLEL(2) */ code FROM products WHERE shade IN ('blue','green')) \
         GROUP BY code)"
    ));
    format!("WITH {} SELECT code, SUM(total) FROM (SELECT * FROM a UNION ALL SELECT * FROM b UNION ALL SELECT * FROM c) u GROUP BY code ORDER BY code", branches.join(","))
}

#[test]
fn parallel_cte_nested_in_scans_share_query_admission() {
    let _tuning = auto_parallel_test_tuning();
    // Force a single admission slot on every host. Individual scans retain
    // their explicit two-worker plans, including the nested IN subqueries.
    let cpus = std::thread::available_parallelism().unwrap().get();
    let _degree = EnvGuardSimple::set("MYDB_PARALLEL_DEGREE", &cpus.to_string());
    let _cache = EnvGuardSimple::set("MYDB_CTE_RESULT_CACHE_ENTRIES", "0");
    let _vec = EnvGuardSimple::set("MYDB_VECTORIZED", "on");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let query = populate_nested_cte_query(&engine);
    let tree = explain_text(&engine, &format!("EXPLAIN FORMAT=TREE {query}"));
    assert!(tree.matches("Parallel table scan").count() >= 6, "{tree}");
    assert!(
        tree.contains("Apply"),
        "exercise an eval-time subquery: {tree}"
    );
    let serial = {
        let _serial = EnvGuardSimple::set("MYDB_PARALLEL_DISABLE", "1");
        execute_rows_in_result_order(&engine, &query)
    };
    assert!(!serial.is_empty());
    let before = crate::util::exec_permit::status_snapshot();
    let parallel = execute_rows_in_result_order(&engine, &query);
    assert_eq!(parallel, serial);
    let after = crate::util::exec_permit::status_snapshot();
    assert!(after.parallel_read_pressure_entries >= before.parallel_read_pressure_entries + 6);
    assert_eq!(after.parallel_read_pressure_active, 0);
    // EXPLAIN ANALYZE also materializes UNION/CTE branches on workers and
    // must keep the same query identity throughout that execution path.
    engine.execute(&format!("EXPLAIN ANALYZE {query}")).unwrap();
    assert_eq!(
        crate::util::exec_permit::status_snapshot().parallel_read_pressure_active,
        0
    );
}

#[test]
fn parallel_cte_gate_wait_inherits_parent_deadline() {
    assert_parallel_cte_gate_wait_cancellation(true);
}

#[test]
fn parallel_cte_gate_wait_inherits_kill_query() {
    assert_parallel_cte_gate_wait_cancellation(false);
}

fn assert_parallel_cte_gate_wait_cancellation(deadline: bool) {
    use std::sync::{mpsc, Arc};
    use std::time::Duration;
    let _tuning = auto_parallel_test_tuning();
    let cpus = std::thread::available_parallelism().unwrap().get();
    let _degree = EnvGuardSimple::set("MYDB_PARALLEL_DEGREE", &cpus.to_string());
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(open_test_engine(dir.path()));
    let query = populate_nested_cte_query(&engine);
    let holder_budget = super::super::mem_budget::MemBudget::new(0);
    let holder = crate::util::exec_permit::enter_parallel_read_pressure(
        holder_budget.query_id(),
        true,
        || Ok(()),
    )
    .unwrap()
    .unwrap();
    let registry = Arc::clone(engine.registry());
    let handle = registry.register(553, "root".into(), "local".into(), "test".into());
    let (tx, rx) = mpsc::channel();
    let (started_tx, started_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 553;
        session.set_cancel_handle(Some(handle.cancel_handle()));
        if deadline {
            engine
                .execute_with_session(&mut session, "SET max_execution_time=100")
                .unwrap();
        }
        started_tx.send(()).unwrap();
        let result = engine.execute_with_session(&mut session, &query);
        tx.send(result.err().map(|error| error.code())).unwrap();
    });
    started_rx.recv().unwrap();
    if !deadline {
        std::thread::sleep(Duration::from_millis(100));
        registry.kill_query(553).unwrap();
    }
    let result = rx.recv_timeout(Duration::from_secs(1));
    // Unblock a broken implementation before joining/asserting.
    drop(holder);
    worker.join().unwrap();
    assert_eq!(result.unwrap(), Some(if deadline { 3024 } else { 1317 }));
    assert_eq!(
        crate::util::exec_permit::status_snapshot().parallel_read_pressure_active,
        0
    );
}

fn populate_t(engine: &StorageQueryEngine, n: i64) {
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(200), x INT) ZONE_MAP=OFF")
        .unwrap();
    let payload = "x".repeat(40);
    // Query execution is the subject of these fixtures. Preserve the rows
    // and commit batches without rebuilding and parsing INSERT text each time.
    for batch_start in (0..n).step_by(500) {
        let batch_end = (batch_start + 500).min(n);
        insert_fixture_rows(
            engine,
            "test.t",
            (batch_start..batch_end).map(|id| {
                vec![
                    Value::Int(id),
                    Value::String(payload.clone()),
                    Value::Int(id * 7 % 113),
                ]
            }),
        );
    }
}

#[test]
fn native_parallel_temporal_group_keeps_session_zone_and_zero_semantics() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in ["SET time_zone = '+00:00'", "SET sql_mode = ''",
        "CREATE TABLE temporal_workers (k INT, ts TIMESTAMP NULL) ZONE_MAP=OFF",
        "INSERT INTO temporal_workers VALUES (1,'2021-03-14 06:30:00'),(1,'2021-03-14 07:30:00'),(2,NULL),(2,'0000-00-00 00:00:00')"] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    let query = "SELECT /*+ PARALLEL(4) */ k, EXTRACT(HOUR FROM ts), COUNT(*) FROM temporal_workers GROUP BY k, EXTRACT(HOUR FROM ts) ORDER BY k, EXTRACT(HOUR FROM ts)";
    for zone in ["+00:00", "+08:00", "America/New_York"] {
        engine
            .execute_with_session(&mut session, &format!("SET time_zone = '{zone}'"))
            .unwrap();
        let mut run = |mode| {
            with_test_env(&[("MYDB_VECTORIZED", Some(mode))], || {
                let QueryResponse::ResultSet(result) =
                    engine.execute_with_session(&mut session, query).unwrap()
                else {
                    panic!()
                };
                result.rows
            })
        };
        let oracle = run("off");
        assert_eq!(run("on"), oracle, "time zone {zone}");
    }
}

#[test]
fn parallel_scan_preserves_mutation_snapshot_and_schema_semantics() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1200);
    engine.execute("ANALYZE TABLE t").unwrap();
    engine
        .execute("CREATE TABLE scan_unrelated (id INT)")
        .unwrap();
    let query = "SELECT /*+ PARALLEL(4) */ id, v, x FROM t ORDER BY id";
    let before = execute_rows_in_result_order(&engine, query);
    assert_eq!(execute_rows_in_result_order(&engine, query), before);
    engine
        .execute("INSERT INTO scan_unrelated VALUES (1)")
        .unwrap();
    assert_eq!(execute_rows_in_result_order(&engine, query), before);
    let mut writer = SessionContext::default();
    engine
        .execute_with_session(&mut writer, "START TRANSACTION")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE t SET x = 888 WHERE id = 1")
        .unwrap();
    // Readers must not see the pending writer until its commit.
    assert_eq!(
        execute_rows_in_result_order(&engine, query)[1][2],
        Value::Int(7)
    );
    assert_eq!(
        execute_rows_in_result_order(&engine, query)[1][2],
        Value::Int(7)
    );
    engine.execute_with_session(&mut writer, "COMMIT").unwrap();
    assert_eq!(
        execute_rows_in_result_order(&engine, query)[1][2],
        Value::Int(888)
    );
    assert_eq!(
        execute_rows_in_result_order(&engine, query)[1][2],
        Value::Int(888)
    );
    engine.execute("UPDATE t SET x = 777 WHERE id = 3").unwrap();
    let changed = execute_rows_in_result_order(&engine, query);
    assert_eq!(changed[3][2], Value::Int(777));
    assert_eq!(execute_rows_in_result_order(&engine, query), changed);
    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine
        .execute_with_session(&mut reader, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    let QueryResponse::ResultSet(snapshot) =
        engine.execute_with_session(&mut reader, query).unwrap()
    else {
        panic!()
    };
    engine.execute("DELETE FROM t WHERE id = 3").unwrap();
    let QueryResponse::ResultSet(repeated) =
        engine.execute_with_session(&mut reader, query).unwrap()
    else {
        panic!()
    };
    assert_eq!(snapshot.rows, repeated.rows);
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
    assert_eq!(execute_rows_in_result_order(&engine, query).len(), 1199);
    engine
        .execute("ALTER TABLE t ADD COLUMN z INT DEFAULT 9")
        .unwrap();
    let rows = execute_rows_in_result_order(
        &engine,
        "SELECT /*+ PARALLEL(4) */ id, z FROM t ORDER BY id",
    );
    assert_eq!(rows.len(), 1199);
    assert!(rows.iter().all(|row| row[1] == Value::Int(9)));
}

#[test]
fn filtered_parallel_scan_preserves_predicates_and_empty_morsels() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1200);
    engine.execute("ANALYZE TABLE t").unwrap();
    let query = "SELECT /*+ PARALLEL(4) */ id, v FROM t WHERE v LIKE '%absent%'";
    assert!(execute_rows(&engine, query).is_empty());
    assert!(execute_rows(&engine, query).is_empty());
    let other = "SELECT /*+ PARALLEL(4) */ id, v FROM t WHERE v LIKE '%xxx%'";
    assert_eq!(execute_rows(&engine, other).len(), 1200);
    assert_eq!(execute_rows(&engine, other).len(), 1200);
    assert!(execute_rows(&engine, query).is_empty());
    engine
        .execute("UPDATE t SET v = 'absent' WHERE id = 8")
        .unwrap();
    assert_eq!(execute_rows(&engine, query).len(), 1);
    assert_eq!(execute_rows(&engine, query).len(), 1);
}

#[test]
fn parallel_seq_scan_matches_serial_full_table() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);

    let serial = execute_rows(&engine, "SELECT id FROM t");
    let parallel = execute_rows(&engine, "SELECT /*+ PARALLEL(4) */ id FROM t");
    assert_eq!(serial, parallel);
    assert_eq!(serial.len(), 1500);
}

#[test]
fn parallel_seq_scan_matches_serial_with_where_filter() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);

    let serial = execute_rows(&engine, "SELECT id FROM t WHERE x = 7");
    let parallel = execute_rows(&engine, "SELECT /*+ PARALLEL(4) */ id FROM t WHERE x = 7");
    assert_eq!(serial, parallel);
    assert!(!serial.is_empty(), "filter should match some rows");
}

#[test]
fn parallel_seq_scan_with_aggregate_matches_serial_count() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);

    // Aggregate sits above Exchange; the executor should fuse the parallel
    // scan directly into the single aggregate group without materialising
    // one PlanRow per input row.
    let QueryResponse::ResultSet(serial) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    let QueryResponse::ResultSet(parallel) = engine
        .execute("SELECT /*+ PARALLEL(4) */ COUNT(*) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(serial.rows, parallel.rows);
    assert_eq!(serial.rows[0][0], Value::Int(1500));
}

#[test]
fn parallel_filter_only_count_string_predicate_matches_value_oracle() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE text_count (\
                 id INT PRIMARY KEY, \
                 s CHAR(16) COLLATE utf8mb4_0900_ai_ci NULL, \
                 b CHAR(16) COLLATE utf8mb4_bin NULL)",
        )
        .unwrap();
    for batch_start in (0..1500).step_by(250) {
        let mut sql = String::from("INSERT INTO text_count VALUES ");
        for id in batch_start..batch_start + 250 {
            if id > batch_start {
                sql.push(',');
            }
            let (s, b) = match id % 5 {
                0 => ("'Alpha'", "'Alpha'"),
                1 => ("'ALPHA '", "'ALPHA '"),
                2 => ("'Àlpha'", "'Àlpha'"),
                3 => ("'beta'", "'beta'"),
                _ => ("NULL", "NULL"),
            };
            sql.push_str(&format!("({id},{s},{b})"));
        }
        engine.execute(&sql).unwrap();
    }

    // COUNT(id) forces an output-column decode and therefore remains on the
    // existing Value-based evaluator. COUNT(*) has no post-filter payload and
    // is eligible for the borrowed-string predicate path; the answers must
    // agree for every ordered comparison, including PAD SPACE, CI/accent
    // folding, binary collation, and NULL rejection.
    for predicate in [
        "s = 'alpha'",
        "s <> 'alpha'",
        "s < 'alpha'",
        "s <= 'alpha'",
        "s > 'alpha'",
        "s >= 'alpha'",
        "b = 'Alpha'",
        "b <> 'Alpha'",
        "s IS NULL",
        "s BETWEEN 'alpha' AND 'beta'",
        "s IN ('alpha', 'beta')",
        "s LIKE 'alp%'",
        "(s >= 'alpha' AND s <= 'beta') OR s IS NULL",
    ] {
        let fast = execute_rows_in_result_order(
            &engine,
            &format!(
                "SELECT /*+ FULL_SCAN(text_count) PARALLEL(2) */ COUNT(*) \
                 FROM text_count WHERE {predicate}"
            ),
        );
        let oracle = execute_rows_in_result_order(
            &engine,
            &format!(
                "SELECT /*+ FULL_SCAN(text_count) PARALLEL(2) */ COUNT(id) \
                 FROM text_count WHERE {predicate}"
            ),
        );
        assert_eq!(fast, oracle, "borrowed predicate drift for {predicate}");
    }
}

#[test]
fn parallel_large_string_comparisons_count_star_match_all_oracles() {
    let _tuning = auto_parallel_test_tuning();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE text_count_large (\
                 id INT PRIMARY KEY, \
                 pad VARCHAR(96) COLLATE utf8mb4_0900_ai_ci NOT NULL)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO text_count_large VALUES \
             (0,'00000000000-00000000000-00000000000-00000000000-00000000000'),\
             (1,'25000000000-00000000000-00000000000-00000000000-00000000000'),\
             (2,'50000000000-00000000000-00000000000-00000000000-00000000000'),\
             (3,'75000000000-00000000000-00000000000-00000000000-00000000000'),\
             (4,'98996621624-36689827414-04092488557-09587706818-65008859162'),\
             (5,'Alpha'),(6,'ALPHA '),(7,'Àlpha')",
        )
        .unwrap();
    // 8 -> 1,024 rows spanning multiple pages. VecMode::On and PARALLEL(2)
    // below force the production vector/parallel route directly, so a larger
    // duplicate corpus only repeats the same long-string comparisons.
    for _ in 0..7 {
        engine
            .execute("INSERT INTO text_count_large SELECT id + (SELECT COUNT(*) FROM text_count_large), pad FROM text_count_large")
            .unwrap();
    }
    engine.execute("ANALYZE TABLE text_count_large").unwrap();

    fn scalar_i64(engine: &StorageQueryEngine, sql: &str) -> i64 {
        let rows = execute_rows_in_result_order(engine, sql);
        assert_eq!(rows.len(), 1, "expected one aggregate row for {sql}");
        match &rows[0][0] {
            Value::Int(value) => *value,
            Value::UInt(value) => *value as i64,
            Value::DecimalCompact(value, scale) if *scale == 0 => *value,
            Value::Decimal(value) | Value::String(value) => value.parse().unwrap(),
            other => panic!("expected integer aggregate for {sql}, got {other:?}"),
        }
    }

    crate::engine::executor::vec::config::with_vec_mode(
        crate::engine::executor::vec::config::VecMode::On,
        || {
            for predicate in [
                "pad = '98996621624-36689827414-04092488557-09587706818-6500885916'",
                "pad <> '98996621624-36689827414-04092488557-09587706818-6500885916'",
                "pad >= '98996621624-36689827414-04092488557-09587706818-6500885916'",
                "pad <= '98996621624-36689827414-04092488557-09587706818-6500885916'",
                "pad = 'alpha'",
                "pad <> 'alpha'",
            ] {
                let hint = "/*+ FULL_SCAN(text_count_large) PARALLEL(2) */";
                let star = scalar_i64(
                    &engine,
                    &format!("SELECT {hint} COUNT(*) FROM text_count_large WHERE {predicate}"),
                );
                let column = scalar_i64(
                    &engine,
                    &format!("SELECT {hint} COUNT(id) FROM text_count_large WHERE {predicate}"),
                );
                let sum = scalar_i64(
                    &engine,
                    &format!("SELECT {hint} COALESCE(SUM({predicate}), 0) FROM text_count_large"),
                );
                assert_eq!(star, column, "COUNT(*) / COUNT(id) drift for {predicate}");
                assert_eq!(star, sum, "COUNT(*) / SUM(predicate) drift for {predicate}");
            }
            let explain = explain_text(
                &engine,
                "EXPLAIN ANALYZE SELECT /*+ FULL_SCAN(text_count_large) PARALLEL(2) */ \
                 COUNT(*) FROM text_count_large \
                 WHERE pad <> '98996621624-36689827414-04092488557-09587706818-6500885916'",
            );
            assert!(
                explain.contains("execution_mode=vector")
                    && explain.contains("batches=")
                    && !explain.contains("scalar_rows="),
                "unified expression observability missing: {explain}"
            );
        },
    );
}

#[test]
fn parallel_seq_scan_reads_user_variables() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);

    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET @limit = 1000")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut session,
            "SELECT /*+ PARALLEL(4) */ COUNT(*) FROM t WHERE id <= @limit",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1001)]]);
}

#[test]
fn parallel_seq_scan_with_group_by_matches_serial() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);

    let serial = execute_rows(&engine, "SELECT x, COUNT(*) FROM t GROUP BY x ORDER BY x");
    let parallel = execute_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ x, COUNT(*) FROM t GROUP BY x ORDER BY x",
    );
    assert_eq!(serial, parallel);
}

#[test]
fn parallel_hash_aggregate_having_subquery_inherits_cte_scope() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "WITH cutoff AS (SELECT 20 AS min_sum), \
                  grouped AS (\
                    SELECT /*+ PARALLEL(4) */ x, SUM(id) AS s \
                    FROM t \
                    GROUP BY x \
                    HAVING SUM(id) > (SELECT * FROM cutoff)\
                  ) \
             SELECT COUNT(*) FROM grouped",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(113)]]);
}

#[test]
fn first_no_parallel_hint_wins_conflict() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 100);

    // Conflicting hints are first-wins. Result should still be correct.
    let rows = execute_rows(&engine, "SELECT /*+ NO_PARALLEL PARALLEL(8) */ id FROM t");
    assert_eq!(rows.len(), 100);

    // EXPLAIN should NOT contain "Parallel" markers.
    let QueryResponse::ResultSet(result) = engine
        .execute("EXPLAIN SELECT /*+ NO_PARALLEL PARALLEL(8) */ id FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let plan_text: String = result
        .rows
        .iter()
        .flat_map(|row| {
            row.iter().map(|v| match v {
                Value::String(s) => s.clone(),
                other => format!("{other:?}"),
            })
        })
        .collect::<Vec<_>>()
        .join(" | ");
    assert!(
        !plan_text.contains("Parallel"),
        "expected no Parallel marker but got: {plan_text}"
    );
}

#[test]
fn parallel_hint_surfaces_in_explain() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);

    // `WHERE x = ...` forces a SeqScan (x is not indexed), so the
    // hint actually applies. Selecting id alone would let the planner
    // pick an IndexFastScan over PRIMARY.
    let QueryResponse::ResultSet(result) = engine
        .execute("EXPLAIN SELECT /*+ PARALLEL(4) */ id FROM t WHERE x = 7")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let plan_text: String = result
        .rows
        .iter()
        .flat_map(|row| {
            row.iter().map(|v| match v {
                Value::String(s) => s.clone(),
                other => format!("{other:?}"),
            })
        })
        .collect::<Vec<_>>()
        .join(" | ");
    assert!(
        plan_text.contains("Parallel scan"),
        "expected 'Parallel scan' in EXPLAIN, got: {plan_text}"
    );
    assert!(
        plan_text.contains("workers=4"),
        "expected 'workers=4' in EXPLAIN, got: {plan_text}"
    );
    assert!(
        plan_text.contains("Exchange"),
        "expected 'Exchange' marker in EXPLAIN, got: {plan_text}"
    );
}

#[test]
fn parallel_seq_scan_degree_one_runs_inline() {
    let _lock = auto_parallel_test_lock();
    // degree=1 should be a no-op (no thread spawn) and still produce
    // the correct result.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 200);
    let serial = execute_rows(&engine, "SELECT id FROM t");
    let parallel = execute_rows(&engine, "SELECT /*+ PARALLEL(1) */ id FROM t");
    assert_eq!(serial, parallel);
}

#[test]
fn parallel_seq_scan_more_workers_than_pages_still_correct() {
    let _lock = auto_parallel_test_lock();
    // Tiny table — PARALLEL(16) but only a handful of pages. The
    // effective_degree cap should keep us correct.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 50);
    let serial = execute_rows(&engine, "SELECT id FROM t");
    let parallel = execute_rows(&engine, "SELECT /*+ PARALLEL(16) */ id FROM t");
    assert_eq!(serial, parallel);
}

#[test]
fn parallel_seq_scan_with_projection_matches_serial() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 2000);

    // Joining selects a subset of columns — exercises the projected
    // path inside the parallel worker.
    let serial = execute_rows(&engine, "SELECT id, x FROM t WHERE x > 50 ORDER BY id");
    let parallel = execute_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ id, x FROM t WHERE x > 50 ORDER BY id",
    );
    assert_eq!(serial, parallel);
}

#[test]
fn parallel_hint_preserves_index_satisfied_order_by() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, v INT, INDEX idx_k(k))")
        .unwrap();
    for batch in (0..1000).collect::<Vec<_>>().chunks(100) {
        let mut sql = String::from("INSERT INTO t VALUES ");
        for (i, id) in batch.iter().enumerate() {
            if i > 0 {
                sql.push(',');
            }
            let k = (id * 37) % 1000;
            sql.push_str(&format!("({id}, {k}, {})", id * 3));
        }
        engine.execute(&sql).unwrap();
    }

    let plan_text = explain_text(
        &engine,
        "EXPLAIN SELECT /*+ PARALLEL(4) */ k FROM t ORDER BY k LIMIT 25",
    );
    assert!(
        !plan_text.contains("Exchange") && !plan_text.contains("Parallel index-only scan"),
        "index-satisfied ORDER BY must not be broken by unordered gather: {plan_text}"
    );

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT /*+ PARALLEL(4) */ k FROM t ORDER BY k LIMIT 25")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let values: Vec<i64> = result
        .rows
        .iter()
        .map(|row| match row[0] {
            Value::Int(v) => v,
            ref other => panic!("expected int k, got {other:?}"),
        })
        .collect();
    let mut sorted = values.clone();
    sorted.sort();
    assert_eq!(values, sorted);
    assert_eq!(values.first(), Some(&0));
    assert_eq!(values.last(), Some(&24));
}

#[test]
fn parallel_hint_preserves_streaming_aggregate_and_distinct_inputs() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t_with_groups(&engine, 600, 30);
    engine.execute("CREATE INDEX idx_g ON t(g)").unwrap();

    let agg_plan = explain_text(
        &engine,
        "EXPLAIN SELECT /*+ PARALLEL(4) */ g, SUM(v) FROM t WHERE g >= 0 GROUP BY g",
    );
    assert!(
        agg_plan
            .to_ascii_lowercase()
            .contains("streaming aggregate"),
        "expected streaming aggregate over index order: {agg_plan}"
    );
    assert!(
        !agg_plan.contains("Exchange") && !agg_plan.contains("Parallel index-only scan"),
        "streaming aggregate input order must stay serial: {agg_plan}"
    );

    let distinct_plan = explain_text(
        &engine,
        "EXPLAIN SELECT /*+ PARALLEL(4) */ DISTINCT g FROM t",
    );
    assert!(
        distinct_plan
            .to_ascii_lowercase()
            .contains("streaming distinct"),
        "expected streaming distinct over index order: {distinct_plan}"
    );
    assert!(
        !distinct_plan.contains("Exchange") && !distinct_plan.contains("Parallel index-only scan"),
        "streaming distinct input order must stay serial: {distinct_plan}"
    );
}

#[test]
fn parallel_scan_filter_errors_are_propagated() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);

    let err = engine
        .execute("SELECT /*+ PARALLEL(4) */ id FROM t WHERE definitely_missing_function(x)")
        .expect_err("unsupported function in parallel scan filter must be returned");
    assert_eq!(err.code(), 1305);
}

#[test]
fn filtered_index_in_uses_native_parallel_batch_source() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};

    let _lock = auto_parallel_test_lock();
    let _degree = EnvGuardSimple::set("MYDB_PARALLEL_DEGREE", "4");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE pf_dim (k INT PRIMARY KEY, active INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO pf_dim VALUES \
             (0,1),(1,1),(2,1),(3,1),(4,1),(5,0),(6,0),(7,0),(8,0),(9,0), \
             (10,0),(11,0),(12,0),(13,0),(14,0),(15,0),(16,0),(17,0),(18,0),(19,0)",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE pf_fact (k INT, v INT, KEY idx_k(k))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO pf_fact VALUES \
             (0,0),(1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80),(9,90), \
             (10,100),(11,110),(12,120),(13,130),(14,140),(15,150),(16,160),(17,170),(18,180),(19,190)",
        )
        .unwrap();
    for _ in 0..6 {
        engine
            .execute("INSERT INTO pf_fact SELECT k, v FROM pf_fact")
            .unwrap();
    }
    engine.execute("ANALYZE TABLE pf_fact").unwrap();
    engine.execute("ANALYZE TABLE pf_dim").unwrap();

    let sql = "SELECT COUNT(*), SUM(v) FROM pf_fact \
               WHERE k IN (SELECT k FROM pf_dim WHERE active=1) \
                 AND v BETWEEN 50 AND 100";
    let plan = explain_text(&engine, &format!("EXPLAIN {sql}"));
    assert!(
        plan.contains("IN subquery"),
        "test must exercise IndexInLookup:\n{plan}"
    );
    let expected = with_vec_mode(VecMode::Off, || execute_rows_in_result_order(&engine, sql));
    let before = crate::engine::executor::select::vec_bridge::PARALLEL_FILTERED_INDEX_IN_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let budget = std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
        128 * 1024,
    ));
    let actual = crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
        with_vec_mode(VecMode::On, || execute_rows_in_result_order(&engine, sql))
    });
    assert_eq!(
        budget.used(),
        0,
        "heap batches and shared RowIds release after workers retire"
    );
    assert!(
        budget.peak_used() <= 512 * 1024,
        "bounded parallel heap reconstruction"
    );
    let after = crate::engine::executor::select::vec_bridge::PARALLEL_FILTERED_INDEX_IN_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(actual, expected);
    assert!(
        after > before,
        "a compilable residual filter must retain the native parallel IndexIn source"
    );
}

// --- Phase 2: CBO auto-parallelism --------------------------------------

/// Serialise the CBO-auto-parallel tests. They poke process-global
/// env vars (MYDB_PARALLEL_*) and CBO atomics (parallel_setup_cost),
/// so running them in parallel cross-contaminates state and produces
/// flaky failures.
fn auto_parallel_test_lock() -> std::sync::MutexGuard<'static, ()> {
    use std::sync::{Mutex, OnceLock};
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// Lower the row-count gate so test populations under 1500 rows still
/// trigger CBO auto-parallel. Must wrap any test that exercises auto
/// mode because the production default (50_000) is too high for unit
/// tests.
pub(super) struct AutoParallelTuning {
    _lock: std::sync::MutexGuard<'static, ()>,
    _threshold: EnvGuardSimple,
    _min_pages: EnvGuardSimple,
    _degree: EnvGuardSimple,
    _disable: EnvGuardSimple,
    _setup_cost_guard: SetupCostGuard,
}

/// Override `cbo_parallel_setup_cost` directly via the SET-GLOBAL
/// plumbing so the cost-based decision actually picks parallel for
/// our small unit-test populations. The env-var path won't help because
/// `init_from_env` only fires once per process.
struct SetupCostGuard {
    prior: f64,
}
impl SetupCostGuard {
    fn set(value: f64) -> Self {
        let prior = crate::engine::cbo::consts::consts().parallel_setup_cost();
        crate::engine::cbo::consts::set_variable("cbo_parallel_setup_cost", value)
            .expect("set parallel_setup_cost");
        Self { prior }
    }
}
impl Drop for SetupCostGuard {
    fn drop(&mut self) {
        let _ = crate::engine::cbo::consts::set_variable("cbo_parallel_setup_cost", self.prior);
    }
}

struct EnvGuardSimple {
    key: &'static str,
    prior: Option<String>,
}
impl EnvGuardSimple {
    fn set(key: &'static str, val: &str) -> Self {
        let prior = std::env::var(key).ok();
        std::env::set_var(key, val);
        Self { key, prior }
    }
    fn unset(key: &'static str) -> Self {
        let prior = std::env::var(key).ok();
        std::env::remove_var(key);
        Self { key, prior }
    }
}
impl Drop for EnvGuardSimple {
    fn drop(&mut self) {
        match self.prior.take() {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}
pub(super) fn auto_parallel_test_tuning() -> AutoParallelTuning {
    AutoParallelTuning {
        _lock: auto_parallel_test_lock(),
        _threshold: EnvGuardSimple::set("MYDB_PARALLEL_THRESHOLD_ROWS", "100"),
        _min_pages: EnvGuardSimple::set("MYDB_PARALLEL_MIN_PAGES", "1"),
        _degree: EnvGuardSimple::set("MYDB_PARALLEL_DEGREE", "4"),
        _disable: EnvGuardSimple::unset("MYDB_PARALLEL_DISABLE"),
        // Production default is 100/worker. Unit-test populations are
        // tiny so the per-worker setup tax dominates real I/O savings;
        // drop it for the duration of these auto-parallel tests.
        _setup_cost_guard: SetupCostGuard::set(0.001),
    }
}

fn explain_text(engine: &StorageQueryEngine, sql: &str) -> String {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    result
        .rows
        .iter()
        .flat_map(|row| {
            row.iter().map(|v| match v {
                Value::String(s) => s.clone(),
                other => format!("{other:?}"),
            })
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn first_cost(plan_text: &str, needle: &str) -> f64 {
    let start = plan_text
        .find(needle)
        .unwrap_or_else(|| panic!("expected {needle:?} in plan: {plan_text}"));
    let rest = &plan_text[start..];
    let marker = "cost=";
    let cost_start = rest
        .find(marker)
        .unwrap_or_else(|| panic!("expected cost marker after {needle:?}: {plan_text}"))
        + marker.len();
    let rest = &rest[cost_start..];
    let cost_end = rest
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .unwrap_or(rest.len());
    rest[..cost_end].parse::<f64>().unwrap()
}

/// Without a PRIMARY KEY, bare COUNT(*) once returned zero beyond the parallel threshold,
/// while COUNT(col) was correct. Parallel bare aggregation must match serial execution.
#[test]
fn parallel_bare_count_star_on_pk_less_table_matches_serial() {
    let _tuning = auto_parallel_test_tuning();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE c (t VARCHAR(120))").unwrap();
    engine
        .execute("INSERT INTO c VALUES ('a'),('b'),('c'),('d'),('e'),('f'),('g'),('h')")
        .unwrap();
    // Double to 1024 rows, well beyond the tuned 100-row threshold.
    for _ in 0..7 {
        engine.execute("INSERT INTO c SELECT t FROM c").unwrap();
    }
    engine.execute("ANALYZE TABLE c").unwrap();

    let plan = explain_text(&engine, "EXPLAIN SELECT COUNT(*) FROM c");
    eprintln!("count(*) plan: {plan}");
    let col = execute_rows_in_result_order(&engine, "SELECT COUNT(t) FROM c");
    assert_eq!(col, vec![vec![Value::Int(1024)]]);
    let star = execute_rows_in_result_order(&engine, "SELECT COUNT(*) FROM c");
    assert_eq!(
        star,
        vec![vec![Value::Int(1024)]],
        "bare COUNT(*) must agree with COUNT(col); plan: {plan}"
    );
    // Use forced serial execution as the oracle.
    let serial = {
        let _off = EnvGuardSimple::set("MYDB_PARALLEL_DISABLE", "1");
        execute_rows_in_result_order(&engine, "SELECT COUNT(*) FROM c")
    };
    assert_eq!(serial, vec![vec![Value::Int(1024)]]);

    // Filtered COUNT(*) projects the filter column, while the CountStar slot reads no input column.
    let filtered = execute_rows_in_result_order(&engine, "SELECT COUNT(*) FROM c WHERE t > 'd'");
    assert_eq!(filtered, vec![vec![Value::Int(512)]], "e..h = 4/8 of rows");
    // Mix CountStar and column aggregates in one batch.
    let mixed = execute_rows_in_result_order(&engine, "SELECT COUNT(*), COUNT(t), MIN(t) FROM c");
    assert_eq!(
        mixed,
        vec![vec![
            Value::Int(1024),
            Value::Int(1024),
            Value::String("a".into())
        ]]
    );
}

/// After setting the default database, parallel COUNT(*) on an unqualified table without WHERE
/// must match the qualified name and actual scanned count; it previously returned zero.
#[test]
fn parallel_count_star_unqualified_in_non_default_database() {
    let _tuning = auto_parallel_test_tuning();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE DATABASE shop")
        .unwrap();
    engine
        .execute_with_session(&mut session, "USE shop")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE orders (id INT PRIMARY KEY, total INT)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO orders VALUES (1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80)",
        )
        .unwrap();
    // Double with increasing PKs to 1024 rows, beyond the tuned 100-row threshold,
    // to trigger parallel index aggregation.
    let mut next = 9i64;
    for _ in 0..7 {
        let existing = {
            let QueryResponse::ResultSet(rs) = engine
                .execute_with_session(&mut session, "SELECT id FROM orders")
                .unwrap()
            else {
                panic!("expected rows");
            };
            rs.rows
                .iter()
                .map(|r| match &r[0] {
                    Value::Int(n) => *n,
                    other => panic!("unexpected id {other:?}"),
                })
                .collect::<Vec<_>>()
        };
        let values = existing
            .iter()
            .map(|_| {
                let id = next;
                next += 1;
                format!("({id},{})", id * 10)
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute_with_session(&mut session, &format!("INSERT INTO orders VALUES {values}"))
            .unwrap();
    }
    engine
        .execute_with_session(&mut session, "ANALYZE TABLE orders")
        .unwrap();

    let count = |session: &mut SessionContext, sql: &str| -> i64 {
        let QueryResponse::ResultSet(rs) = engine.execute_with_session(session, sql).unwrap()
        else {
            panic!("expected result set for {sql}");
        };
        match &rs.rows[0][0] {
            Value::Int(n) => *n,
            other => panic!("expected int count for {sql}, got {other:?}"),
        }
    };

    let plan = {
        let QueryResponse::ResultSet(rs) = engine
            .execute_with_session(&mut session, "EXPLAIN SELECT COUNT(*) FROM orders")
            .unwrap()
        else {
            panic!("expected explain rows");
        };
        format!("{:?}", rs.rows)
    };
    let scanned = {
        let QueryResponse::ResultSet(rs) = engine
            .execute_with_session(&mut session, "SELECT id FROM orders")
            .unwrap()
        else {
            panic!("expected result set");
        };
        rs.rows.len() as i64
    };
    let qualified = count(&mut session, "SELECT COUNT(*) FROM shop.orders");
    let unqualified = count(&mut session, "SELECT COUNT(*) FROM orders");

    assert_eq!(scanned, 1024, "scan should see 1024 rows");
    assert_eq!(qualified, 1024, "qualified COUNT(*) should be 1024");
    assert_eq!(
        unqualified, qualified,
        "unqualified COUNT(*) must match qualified; plan: {plan}"
    );
    assert!(
        plan.contains("Parallel index aggregate"),
        "regression guard must exercise the parallel index aggregate path; plan: {plan}"
    );
}

#[test]
fn cbo_auto_parallel_kicks_in_for_large_seq_scan() {
    let _tuning = auto_parallel_test_tuning();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);
    // ANALYZE refreshes table stats so the CBO sees the row count.
    engine.execute("ANALYZE TABLE t").unwrap();

    // No hint at all — CBO should auto-parallelize this big SeqScan.
    let plan_text = explain_text(&engine, "EXPLAIN SELECT id FROM t WHERE x = 7");
    assert!(
        plan_text.contains("Parallel scan"),
        "CBO should auto-parallel large WHERE-on-x scan; got: {plan_text}"
    );
}

#[test]
fn cbo_auto_parallel_is_suppressed_by_small_unordered_limit() {
    let _tuning = auto_parallel_test_tuning();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 2500);
    engine.execute("ANALYZE TABLE t").unwrap();

    let plan_text = explain_text(&engine, "EXPLAIN SELECT id FROM t WHERE x = 7 LIMIT 3");
    assert!(
        !plan_text.contains("Parallel scan") && !plan_text.contains("Exchange"),
        "small unordered LIMIT should keep a bounded serial scan; got: {plan_text}"
    );

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM t WHERE x = 7 LIMIT 3")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn parallel_seq_scan_cost_matches_amdahl_plus_setup() {
    // Parallel cost should follow `serial / degree + degree * setup`,
    // matching `cbo::operator_cost::parallel_seq_scan`. The previous
    // "half serial cost" approximation ignored degree and setup tax,
    // so forced PARALLEL(4) on a tiny scan looked cheaper than serial.
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);

    let serial_plan = explain_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT /*+ NO_PARALLEL */ id FROM t WHERE x = 7",
    );
    let parallel_plan = explain_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT /*+ PARALLEL(4) */ id FROM t WHERE x = 7",
    );

    let serial_cost = first_cost(&serial_plan, "Table scan");
    let parallel_cost = first_cost(&parallel_plan, "Parallel table scan");
    let setup = crate::engine::cbo::consts::consts().parallel_setup_cost();
    let expected = serial_cost / 4.0 + 4.0 * setup;
    assert!(
        (parallel_cost - expected).abs() < 0.01,
        "expected parallel cost ≈ serial/degree + degree*setup; serial={serial_cost} parallel={parallel_cost} expected={expected}\nserial plan:\n{serial_plan}\nparallel plan:\n{parallel_plan}"
    );
}

#[test]
fn cbo_auto_parallel_skips_small_table() {
    let _tuning = auto_parallel_test_tuning();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Population intentionally below the (lowered-for-test) threshold.
    populate_t(&engine, 30);
    engine.execute("ANALYZE TABLE t").unwrap();

    let plan_text = explain_text(&engine, "EXPLAIN SELECT id FROM t WHERE x = 7");
    assert!(
        !plan_text.contains("Parallel scan"),
        "CBO should keep tiny table serial; got: {plan_text}"
    );
}

#[test]
fn cbo_auto_parallel_respects_no_parallel_hint() {
    let _tuning = auto_parallel_test_tuning();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);
    engine.execute("ANALYZE TABLE t").unwrap();

    let plan_text = explain_text(
        &engine,
        "EXPLAIN SELECT /*+ NO_PARALLEL */ id FROM t WHERE x = 7",
    );
    assert!(
        !plan_text.contains("Parallel scan"),
        "NO_PARALLEL must disable CBO auto-parallel; got: {plan_text}"
    );
}

#[test]
fn cbo_auto_parallel_threshold_env_blocks_parallelism() {
    let _lock = auto_parallel_test_lock();
    // Set threshold so high that NO scan qualifies.
    let _t = EnvGuardSimple::set("MYDB_PARALLEL_THRESHOLD_ROWS", "10000000");
    let _m = EnvGuardSimple::set("MYDB_PARALLEL_MIN_PAGES", "1");
    let _d = EnvGuardSimple::unset("MYDB_PARALLEL_DISABLE");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);
    engine.execute("ANALYZE TABLE t").unwrap();

    let plan_text = explain_text(&engine, "EXPLAIN SELECT id FROM t WHERE x = 7");
    assert!(
        !plan_text.contains("Parallel scan"),
        "threshold should suppress; got: {plan_text}"
    );
}

#[test]
fn cbo_auto_parallel_can_be_globally_disabled() {
    let _lock = auto_parallel_test_lock();
    let _t = EnvGuardSimple::set("MYDB_PARALLEL_THRESHOLD_ROWS", "100");
    let _m = EnvGuardSimple::set("MYDB_PARALLEL_MIN_PAGES", "1");
    let _d = EnvGuardSimple::set("MYDB_PARALLEL_DISABLE", "1");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);
    engine.execute("ANALYZE TABLE t").unwrap();

    let plan_text = explain_text(&engine, "EXPLAIN SELECT id FROM t WHERE x = 7");
    assert!(
        !plan_text.contains("Parallel scan"),
        "MYDB_PARALLEL_DISABLE should win even over CBO; got: {plan_text}"
    );
}

#[test]
fn cbo_auto_parallel_correctness_matches_serial() {
    let _tuning = auto_parallel_test_tuning();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);
    engine.execute("ANALYZE TABLE t").unwrap();

    // Compare auto-parallel result against explicit-serial result.
    let auto_rows = execute_rows(&engine, "SELECT id FROM t WHERE x = 13");
    let serial_rows = execute_rows(&engine, "SELECT /*+ NO_PARALLEL */ id FROM t WHERE x = 13");
    assert_eq!(auto_rows, serial_rows);
    assert!(!auto_rows.is_empty());
}

// --- Phase 6: parallel-execution metrics --------------------------------

#[test]
fn parallel_metrics_increment_on_parallel_query() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 800);

    // Use a query that's guaranteed to plan as SeqScan: filter on
    // `x` (not indexed), so the PK index cannot satisfy the predicate.
    let analyze = explain_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE SELECT /*+ PARALLEL(4) */ id FROM t WHERE x = 7",
    );
    assert!(
        analyze.contains("Exchange")
            && analyze.contains("execution_mode=vector")
            && analyze.contains("workers=4"),
        "parallel execution metrics must be operator-scoped: {analyze}"
    );
}

#[test]
fn parallel_metrics_show_engine_status_surface() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 800);
    let _ = execute_rows(&engine, "SELECT /*+ PARALLEL(4) */ id FROM t WHERE x = 7");

    let QueryResponse::ResultSet(result) = engine.execute("SHOW ENGINE MYDB STATUS").unwrap()
    else {
        panic!("expected result set");
    };
    // Each row is (variable, value); we just need the variable column
    // to mention our new metrics.
    let dump: String = result
        .rows
        .iter()
        .flat_map(|row| {
            row.iter().map(|v| match v {
                Value::String(s) => s.clone(),
                other => format!("{other:?}"),
            })
        })
        .collect::<Vec<_>>()
        .join(" | ");
    for metric in &[
        "exchange_count",
        "parallel_seq_scan_count",
        "parallel_zone_map_scan_count",
        "parallel_workers_spawned",
        "parallel_worker_panic_count",
        "txn_admission_waits",
        "txn_admission_wait_us",
        "mixed_admission_waits",
        "mixed_admission_wait_us",
        "parallel_read_pressure_entries",
        "parallel_read_pressure_suppressed_rw_txn",
        "parallel_read_pressure_active",
    ] {
        assert!(
            dump.contains(metric),
            "expected SHOW ENGINE MYDB STATUS to surface {metric}; got: {dump}"
        );
    }
}

// --- Phase 5: ParallelHashJoin all join types ---------------------------

fn populate_join_tables(engine: &StorageQueryEngine, n_left: i64, n_right: i64) {
    engine
        .execute("CREATE TABLE l (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE r (id INT PRIMARY KEY, k INT, w INT)")
        .unwrap();
    for (table, count, modulus, multiplier) in
        [("test.l", n_left, 50, 2), ("test.r", n_right, 30, 3)]
    {
        for start in (0..count).step_by(500) {
            insert_fixture_rows(
                engine,
                table,
                (start..(start + 500).min(count)).map(|id| {
                    vec![
                        Value::Int(id),
                        Value::Int(id % modulus),
                        Value::Int(id * multiplier),
                    ]
                }),
            );
        }
    }
}

fn populate_covering_join_tables(engine: &StorageQueryEngine, n_left: i64, n_right: i64) {
    engine
        .execute("CREATE TABLE l2 (id INT PRIMARY KEY, k INT NOT NULL, pad INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE r2 (id INT PRIMARY KEY, k INT NOT NULL, pad INT)")
        .unwrap();
    for (table, count, multiplier) in [("test.l2", n_left, 5), ("test.r2", n_right, 7)] {
        insert_fixture_rows(
            engine,
            table,
            (0..count).map(|n| {
                vec![
                    Value::Int(n),
                    Value::Int(n % 257),
                    Value::Int(n * multiplier),
                ]
            }),
        );
    }
    engine.execute("CREATE INDEX idx_l2_k ON l2(k)").unwrap();
    engine.execute("CREATE INDEX idx_r2_k ON r2(k)").unwrap();
}

fn raw_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    let mut rows = result.rows;
    rows.sort_by_cached_key(|row| {
        row.iter()
            .map(|value| match value {
                Value::Int(value) => *value,
                _ => 0,
            })
            .collect::<Vec<_>>()
    });
    rows
}

/// Force the parallel hash-join code path by lowering the row-count
/// threshold. The default (10_000) is too high for unit-test data
/// sizes. Stays in scope so other tests aren't affected.
struct HashJoinTestTuning {
    _lock: std::sync::MutexGuard<'static, ()>,
    _threshold: EnvGuardSimple,
    _parallelism: EnvGuardSimple,
}
fn hash_join_test_tuning() -> HashJoinTestTuning {
    HashJoinTestTuning {
        _lock: auto_parallel_test_lock(),
        _threshold: EnvGuardSimple::set("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD", "1"),
        _parallelism: EnvGuardSimple::set("MYDB_HASH_JOIN_PARALLELISM", "4"),
    }
}

#[test]
fn parallel_inner_join_matches_serial() {
    let _tune = hash_join_test_tuning();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_join_tables(&engine, 600, 400);

    let serial_threshold = EnvGuardSimple::set("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD", "999999999");
    let serial = raw_rows(
        &engine,
        "SELECT l.id, r.id FROM l INNER JOIN r ON l.k = r.k",
    );
    drop(serial_threshold);

    let parallel = raw_rows(
        &engine,
        "SELECT l.id, r.id FROM l INNER JOIN r ON l.k = r.k",
    );
    assert_eq!(serial, parallel);
    assert!(!serial.is_empty());
}

#[test]
fn parallel_left_join_matches_serial() {
    let _tune = hash_join_test_tuning();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_join_tables(&engine, 600, 400);

    let serial_threshold = EnvGuardSimple::set("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD", "999999999");
    let serial = raw_rows(&engine, "SELECT l.id, r.id FROM l LEFT JOIN r ON l.k = r.k");
    drop(serial_threshold);

    let parallel = raw_rows(&engine, "SELECT l.id, r.id FROM l LEFT JOIN r ON l.k = r.k");
    assert_eq!(serial, parallel);
}

#[test]
fn parallel_semi_join_via_in_subquery_matches_serial() {
    let _tune = hash_join_test_tuning();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_join_tables(&engine, 600, 400);

    let serial_threshold = EnvGuardSimple::set("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD", "999999999");
    let serial = raw_rows(&engine, "SELECT id FROM l WHERE k IN (SELECT k FROM r)");
    drop(serial_threshold);

    let parallel = raw_rows(&engine, "SELECT id FROM l WHERE k IN (SELECT k FROM r)");
    assert_eq!(serial, parallel);
    assert!(!serial.is_empty());
}

#[test]
fn parallel_anti_join_via_not_in_matches_serial() {
    let _tune = hash_join_test_tuning();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_join_tables(&engine, 600, 400);

    // Use NOT EXISTS; NOT IN with NULLs has subtle semantics that
    // muddy the test. NOT EXISTS is always non-null-safe ANTI.
    let serial_threshold = EnvGuardSimple::set("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD", "999999999");
    let serial = raw_rows(
        &engine,
        "SELECT id FROM l WHERE NOT EXISTS (SELECT 1 FROM r WHERE r.k = l.k)",
    );
    drop(serial_threshold);

    let parallel = raw_rows(
        &engine,
        "SELECT id FROM l WHERE NOT EXISTS (SELECT 1 FROM r WHERE r.k = l.k)",
    );
    assert_eq!(serial, parallel);
}

// --- Phase 4: ParallelSort ----------------------------------------------

#[test]
fn parallel_sort_matches_serial_basic() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);

    let serial = execute_rows(
        &engine,
        "SELECT /*+ NO_PARALLEL */ id FROM t WHERE x > 50 ORDER BY id",
    );
    let parallel = execute_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ id FROM t WHERE x > 50 ORDER BY id",
    );
    assert_eq!(serial, parallel);
    assert_eq!(serial.len(), parallel.len());
    // Verify globally sorted (already true since execute_rows resorts,
    // but also confirm via a direct check).
    let ids: Vec<i64> = parallel
        .iter()
        .map(|r| match r[0] {
            Value::Int(v) => v,
            _ => 0,
        })
        .collect();
    let mut sorted_ids = ids.clone();
    sorted_ids.sort();
    assert_eq!(ids, sorted_ids, "parallel-sort output must be sorted");
}

#[test]
fn parallel_sort_descending_order_matches_serial() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 800);

    fn raw_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        result.rows
    }

    // Don't re-sort with execute_rows — we want to compare the actual
    // ordering produced by the engine.
    let serial = raw_rows(
        &engine,
        "SELECT /*+ NO_PARALLEL */ id FROM t WHERE x > 50 ORDER BY id DESC",
    );
    let parallel = raw_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ id FROM t WHERE x > 50 ORDER BY id DESC",
    );
    assert_eq!(serial, parallel);
}

#[test]
fn parallel_sort_with_aggregate_matches_serial() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t_with_groups(&engine, 1500, 30);

    fn raw_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        result.rows
    }

    let serial = raw_rows(
        &engine,
        "SELECT /*+ NO_PARALLEL */ g, COUNT(*) FROM t GROUP BY g ORDER BY g",
    );
    let parallel = raw_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ g, COUNT(*) FROM t GROUP BY g ORDER BY g",
    );
    assert_eq!(serial, parallel);
}

#[test]
fn parallel_sort_disable_env_falls_back_to_serial() {
    let _lock = auto_parallel_test_lock();
    let _disable_sort = EnvGuardSimple::set("MYDB_PARALLEL_SORT_DISABLE", "1");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 800);

    fn raw_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        result.rows
    }

    let parallel = raw_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ id FROM t WHERE x > 50 ORDER BY id",
    );
    let serial = raw_rows(
        &engine,
        "SELECT /*+ NO_PARALLEL */ id FROM t WHERE x > 50 ORDER BY id",
    );
    assert_eq!(parallel, serial);
}

#[test]
fn parallel_sort_surfaces_in_explain() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);

    let plan_text = explain_text(
        &engine,
        "EXPLAIN SELECT /*+ PARALLEL(4) */ id FROM t WHERE x > 50 ORDER BY id",
    );
    assert!(
        plan_text.contains("Parallel sort"),
        "expected 'Parallel sort' in EXPLAIN, got: {plan_text}"
    );
}

// --- Phase 4b: ParallelTopK (LIMIT pushdown into the parallel sort) ----

#[test]
fn parallel_topk_matches_serial_basic() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 4000);

    fn raw_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        result.rows
    }

    let serial = raw_rows(
        &engine,
        "SELECT /*+ NO_PARALLEL */ id FROM t WHERE x > 50 ORDER BY id LIMIT 50",
    );
    let parallel = raw_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ id FROM t WHERE x > 50 ORDER BY id LIMIT 50",
    );
    assert_eq!(
        serial, parallel,
        "ParallelTopK must match serial TopK exactly"
    );
}

/// The worker exchange is a bounded execution buffer, not a spillable Top-N
/// work area.  At degree=9 its conservative two-batch-per-worker reservation
/// is larger than the default ~129 MiB query watermark under a 1 GiB process
/// target, while remaining far below the unified process hard limit.
#[test]
fn parallel_topk_degree_nine_uses_actual_queue_memory() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 4000);

    let sql = "SELECT /*+ PARALLEL(9) */ id, v FROM t \
               WHERE v LIKE '%x%' ORDER BY id LIMIT 10";
    let plan = explain_text(&engine, &format!("EXPLAIN {sql}"));
    assert!(
        plan.contains("Parallel TopK") || plan.contains("parallel top-K"),
        "test must exercise ParallelTopK: {plan}"
    );

    let limit = 129 * 1024 * 1024;
    let budget = std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::new(limit));
    let rows = crate::engine::executor::vec::config::with_vec_mode(
        crate::engine::executor::vec::config::VecMode::On,
        || {
            crate::engine::executor::mem_budget::with_query_budget(
                std::sync::Arc::clone(&budget),
                || execute_rows_in_result_order(&engine, sql),
            )
        },
    );

    assert_eq!(rows.len(), 10);
    assert!(
        budget.peak_used() > 0 && budget.peak_used() < limit,
        "small input must not reserve nine workers' maximum batch capacities: peak={} limit={limit}",
        budget.peak_used()
    );
    assert_eq!(budget.used(), 0, "Top-N reservation leaked after query");
}

#[test]
fn parallel_topk_with_offset_matches_serial() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 4000);

    fn raw_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        result.rows
    }

    let serial = raw_rows(
        &engine,
        "SELECT /*+ NO_PARALLEL */ id FROM t WHERE x > 50 ORDER BY id DESC LIMIT 100 OFFSET 30",
    );
    let parallel = raw_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ id FROM t WHERE x > 50 ORDER BY id DESC LIMIT 100 OFFSET 30",
    );
    assert_eq!(serial, parallel);
}

#[test]
fn parallel_topk_with_aggregate_matches_serial() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t_with_groups(&engine, 2000, 50);

    fn raw_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        result.rows
    }

    // Equal counts have no cross-plan SQL order; make the comparison total.
    let serial = raw_rows(
        &engine,
        "SELECT /*+ NO_PARALLEL */ g, COUNT(*) c FROM t GROUP BY g ORDER BY c DESC, g ASC LIMIT 10",
    );
    let parallel = raw_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ g, COUNT(*) c FROM t GROUP BY g ORDER BY c DESC, g ASC LIMIT 10",
    );
    assert_eq!(serial, parallel);
}

#[test]
fn parallel_topk_disable_env_falls_back_to_serial() {
    let _lock = auto_parallel_test_lock();
    // Same kill switch as ParallelSort: disabling parallel sort also
    // disables the TopK upgrade since it sits in the same pass.
    let _disable_sort = EnvGuardSimple::set("MYDB_PARALLEL_SORT_DISABLE", "1");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 2000);

    fn raw_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        result.rows
    }

    let parallel = raw_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ id FROM t WHERE x > 50 ORDER BY id LIMIT 25",
    );
    let serial = raw_rows(
        &engine,
        "SELECT /*+ NO_PARALLEL */ id FROM t WHERE x > 50 ORDER BY id LIMIT 25",
    );
    assert_eq!(parallel, serial);
}

#[test]
fn parallel_topk_surfaces_in_explain() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 4000);

    let plan_text = explain_text(
        &engine,
        "EXPLAIN SELECT /*+ PARALLEL(4) */ id FROM t WHERE x > 50 ORDER BY id LIMIT 100",
    );
    assert!(
        plan_text.contains("Parallel TopK") || plan_text.contains("parallel top-K"),
        "expected 'Parallel TopK' in EXPLAIN, got: {plan_text}"
    );
}

#[test]
fn distinct_topk_keeps_parallel_scan_without_full_parallel_sort() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 4000);

    let serial = raw_rows(
        &engine,
        "SELECT /*+ NO_PARALLEL */ DISTINCT x FROM t WHERE x > 20 ORDER BY x DESC LIMIT 25",
    );
    let parallel = raw_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ DISTINCT x FROM t WHERE x > 20 ORDER BY x DESC LIMIT 25",
    );
    assert_eq!(serial, parallel);

    let plan = explain_text(
        &engine,
        "EXPLAIN SELECT /*+ PARALLEL(4) */ DISTINCT x FROM t WHERE x > 20 ORDER BY x DESC LIMIT 25",
    );
    assert!(plan.contains("distinct top-K"), "unexpected plan: {plan}");
    assert!(plan.contains("Parallel scan"), "unexpected plan: {plan}");
    assert!(!plan.contains("Parallel sort"), "unexpected plan: {plan}");
}

#[test]
fn parallel_topk_small_k_with_large_input() {
    // Stress: 10 000 rows, k=10. ParallelTopK must still produce the
    // exact same set as the serial heap. Down from 50k — large enough to
    // span multiple per-worker shards but ~5x cheaper.
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 10_000);

    fn raw_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
            panic!("expected result set");
        };
        result.rows
    }

    let serial = raw_rows(
        &engine,
        "SELECT /*+ NO_PARALLEL */ id, x FROM t ORDER BY x ASC, id ASC LIMIT 10",
    );
    let parallel = raw_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ id, x FROM t ORDER BY x ASC, id ASC LIMIT 10",
    );
    assert_eq!(serial, parallel);
    assert_eq!(parallel.len(), 10);
}

// --- Phase 3: ParallelHashAggregate -------------------------------------

fn populate_t_with_groups(engine: &StorageQueryEngine, n: i64, num_groups: i64) {
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.t",
        (0..n).map(|n| {
            vec![
                Value::Int(n),
                n.checked_rem(num_groups)
                    .map(Value::Int)
                    .unwrap_or(Value::Null),
                Value::Int(n * 3),
            ]
        }),
    );
}

#[test]
fn parallel_hash_aggregate_count_matches_serial() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t_with_groups(&engine, 1500, 50);

    let serial = execute_rows(&engine, "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY g");
    let parallel = execute_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ g, COUNT(*) FROM t GROUP BY g ORDER BY g",
    );
    assert_eq!(serial, parallel);
    assert_eq!(serial.len(), 50);
}

#[test]
fn parallel_hash_aggregate_sum_min_max_match_serial() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t_with_groups(&engine, 1500, 30);

    let serial = execute_rows(
        &engine,
        "SELECT g, SUM(v), MIN(v), MAX(v) FROM t GROUP BY g ORDER BY g",
    );
    let parallel = execute_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ g, SUM(v), MIN(v), MAX(v) FROM t GROUP BY g ORDER BY g",
    );
    assert_eq!(serial, parallel);
    assert_eq!(serial.len(), 30);
}

#[test]
fn native_parallel_batch_scan_partitions_morsels_once_and_early_close_does_not_block() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parallel_batch_once (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let values = (0..1000)
        .map(|id| format!("({id},{})", id % 17))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO parallel_batch_once VALUES {values}"))
        .unwrap();

    crate::engine::executor::vec::config::with_vec_mode(
        crate::engine::executor::vec::config::VecMode::On,
        || {
            let rows = execute_rows(
                &engine,
                "SELECT /*+ PARALLEL(4) */ COUNT(*) FROM parallel_batch_once",
            );
            assert_eq!(rows, vec![vec![Value::Int(1000)]]);

            // LIMIT closes the scan while worker queues can still be full.
            // The cursor-owned cancellation branch must wake every sender so
            // BatchOperator::drop can join without deadlocking.
            let rows = execute_rows(
                &engine,
                "SELECT /*+ PARALLEL(4) */ id FROM parallel_batch_once LIMIT 1",
            );
            assert_eq!(rows.len(), 1);
        },
    );
}

fn explain_text_with_session(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
) -> String {
    let QueryResponse::ResultSet(result) = engine.execute_with_session(session, sql).unwrap()
    else {
        panic!("expected result set for: {sql}");
    };
    result
        .rows
        .iter()
        .flat_map(|row| row.iter().map(Value::to_display_string))
        .collect::<Vec<_>>()
        .join(" | ")
}

/// A parallel read inside a read-write transaction is part of that
/// transaction, not an independent analytical workload. It must retain the
/// exact same physical plan while declining to serialize peer writers through
/// mixed-workload admission. Explicit read-only transactions remain eligible.
#[test]
fn parallel_read_pressure_respects_transaction_access_mode_without_plan_drift() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);
    engine.execute("CREATE INDEX idx_t_x ON t(x)").unwrap();

    let table_sql = "SELECT /*+ PARALLEL(4) FULL_SCAN(t) */ id FROM t WHERE x >= 0 LIMIT 1";
    let index_sql = "SELECT /*+ PARALLEL(4) INDEX_FFS(t idx_t_x) */ x FROM t WHERE x >= 0 LIMIT 1";
    let mut session = SessionContext::default();

    let autocommit_plan = explain_text_with_session(
        &engine,
        &mut session,
        &format!("EXPLAIN FORMAT=TREE {table_sql}"),
    );
    assert!(
        autocommit_plan.contains("Parallel table scan"),
        "test must exercise parallel table scan: {autocommit_plan}"
    );
    let before = crate::util::exec_permit::status_snapshot();
    let rows = explain_text_with_session(&engine, &mut session, table_sql);
    assert!(!rows.is_empty());
    let after_autocommit = crate::util::exec_permit::status_snapshot();
    assert!(
        after_autocommit.parallel_read_pressure_entries > before.parallel_read_pressure_entries
    );
    assert_eq!(after_autocommit.parallel_read_pressure_active, 0);

    engine
        .execute_with_session(&mut session, "SET autocommit=0")
        .unwrap();
    let before_read_only_pool = crate::util::exec_permit::status_snapshot();
    let _ = explain_text_with_session(&engine, &mut session, table_sql);
    let after_read_only_pool = crate::util::exec_permit::status_snapshot();
    assert!(
        after_read_only_pool.parallel_read_pressure_entries
            > before_read_only_pool.parallel_read_pressure_entries,
        "autocommit=0 alone must not hide an otherwise read-only pool connection"
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET autocommit=1")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (2000,'writer',1)")
        .unwrap();
    assert!(session.write_intent_seen);
    let post_write_autocommit_plan = explain_text_with_session(
        &engine,
        &mut session,
        &format!("EXPLAIN FORMAT=TREE {table_sql}"),
    );
    engine
        .execute_with_session(&mut session, "SET autocommit=0")
        .unwrap();
    // Earlier read-only probes established an analytical lease. Let it expire
    // so this section proves the pure read-write suppression state rather than
    // the deliberately promoted mixed-workload state.
    std::thread::sleep(std::time::Duration::from_millis(
        crate::util::exec_permit::parallel_read_pressure_grace_ms_for_test() + 10,
    ));
    let rw_plan = explain_text_with_session(
        &engine,
        &mut session,
        &format!("EXPLAIN FORMAT=TREE {table_sql}"),
    );
    assert_eq!(
        rw_plan, post_write_autocommit_plan,
        "transaction mode changed the plan"
    );
    let before_rw = crate::util::exec_permit::status_snapshot();
    let _ = explain_text_with_session(&engine, &mut session, table_sql);
    let index_plan = explain_text_with_session(
        &engine,
        &mut session,
        &format!("EXPLAIN FORMAT=TREE {index_sql}"),
    );
    assert!(
        index_plan.contains("Parallel index-only scan"),
        "test must exercise parallel index scan: {index_plan}"
    );
    let _ = explain_text_with_session(&engine, &mut session, index_sql);
    let after_rw = crate::util::exec_permit::status_snapshot();
    assert_eq!(
        after_rw.parallel_read_pressure_entries, before_rw.parallel_read_pressure_entries,
        "read-write transaction unexpectedly declared analytical pressure"
    );
    assert!(
        after_rw.parallel_read_pressure_suppressed_rw_txn
            >= before_rw
                .parallel_read_pressure_suppressed_rw_txn
                .saturating_add(2),
        "both table and index scans must suppress cross-session pressure"
    );
    assert_eq!(after_rw.parallel_read_pressure_active, 0);

    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET autocommit=1")
        .unwrap();
    engine
        .execute_with_session(&mut session, "START TRANSACTION READ ONLY")
        .unwrap();
    let before_ro = crate::util::exec_permit::status_snapshot();
    let _ = explain_text_with_session(&engine, &mut session, table_sql);
    let after_ro = crate::util::exec_permit::status_snapshot();
    assert!(
        after_ro.parallel_read_pressure_entries > before_ro.parallel_read_pressure_entries,
        "explicit read-only transaction must retain analytical pressure"
    );
    assert_eq!(after_ro.parallel_read_pressure_active, 0);
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
}

/// Path B (`try_fast_int_key_agg`) now folds MIN/MAX into the arena's
/// `InlineAggState` (the same single-source accumulator the row path uses).
/// `MYDB_FAST_INT_AGG=off` restores the generic two-phase parallel row path as
/// the oracle; on/off must be byte-identical under PARALLEL(4).
#[test]
fn parallel_fast_int_key_agg_minmax_matches_row_fallback() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t_with_groups(&engine, 1500, 30);

    let sql = "SELECT /*+ PARALLEL(4) */ g, SUM(v), MIN(v), MAX(v), COUNT(*), AVG(v) \
               FROM t GROUP BY g ORDER BY g";
    let oracle = with_test_env(&[("MYDB_FAST_INT_AGG", Some("off"))], || {
        execute_rows(&engine, sql)
    });
    let arena = with_test_env(&[("MYDB_FAST_INT_AGG", Some("on"))], || {
        execute_rows(&engine, sql)
    });
    assert_eq!(
        oracle, arena,
        "Path B int-key arena MIN/MAX diverged from generic parallel row path"
    );
    assert_eq!(oracle.len(), 30);
}

#[test]
fn parallel_fast_int_key_agg_having_matches_row_fallback() {
    let _lock = auto_parallel_test_lock();
    let _vectorized = EnvGuardSimple::set("MYDB_VECTORIZED", "off");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE arena_having (g INT, v INT)")
        .unwrap();
    for batch_start in (0..3000).step_by(500) {
        let mut sql = String::from("INSERT INTO arena_having VALUES ");
        for id in batch_start..(batch_start + 500) {
            if id > batch_start {
                sql.push(',');
            }
            sql.push_str(&format!("({}, {})", id % 1500, id * 3));
        }
        engine.execute(&sql).unwrap();
    }
    engine.execute("ANALYZE TABLE arena_having").unwrap();

    let sql = "SELECT /*+ PARALLEL(4) */ g, SUM(v), COUNT(*), AVG(v) \
               FROM arena_having GROUP BY g HAVING SUM(v) > 9000";
    let oracle = crate::engine::executor::vec::config::with_vec_mode(
        crate::engine::executor::vec::config::VecMode::Off,
        || execute_rows(&engine, sql),
    );
    let before = native_batch_hits();
    let arena = crate::engine::executor::vec::config::with_vec_mode(
        crate::engine::executor::vec::config::VecMode::On,
        || execute_rows(&engine, sql),
    );
    let after = native_batch_hits();
    assert_eq!(
        oracle, arena,
        "int-key arena HAVING finalize diverged from generic parallel row path"
    );
    assert!(!oracle.is_empty());
    assert!(oracle.len() < 1500, "HAVING should discard some groups");
    assert!(
        after > before,
        "parallel aggregate did not execute in the common batch pipeline"
    );
}

#[test]
fn parallel_hash_aggregate_avg_matches_serial() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t_with_groups(&engine, 600, 12);

    let serial = execute_rows(&engine, "SELECT g, AVG(v) FROM t GROUP BY g ORDER BY g");
    let parallel = execute_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ g, AVG(v) FROM t GROUP BY g ORDER BY g",
    );
    assert_eq!(serial, parallel);
}

#[test]
fn parallel_hash_aggregate_having_clause_matches_serial() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t_with_groups(&engine, 1200, 40);

    let serial = execute_rows(
        &engine,
        "SELECT g, COUNT(*) FROM t GROUP BY g HAVING COUNT(*) > 25 ORDER BY g",
    );
    let parallel = execute_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ g, COUNT(*) FROM t GROUP BY g HAVING COUNT(*) > 25 ORDER BY g",
    );
    assert_eq!(serial, parallel);
}

#[test]
fn parallel_hash_aggregate_having_scalar_subquery_matches_serial() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t_with_groups(&engine, 1200, 40);

    let serial = execute_rows(
        &engine,
        "SELECT /*+ NO_PARALLEL */ g + 0 AS k, SUM(v) AS s
         FROM t
         GROUP BY g + 0
         HAVING g + 0 < 10 AND SUM(v) > (SELECT SUM(v) / 80 FROM t)
         ORDER BY k",
    );
    let parallel_sql = "SELECT /*+ PARALLEL(4) */ g + 0 AS k, SUM(v) AS s
         FROM t
         GROUP BY g + 0
         HAVING g + 0 < 10 AND SUM(v) > (SELECT SUM(v) / 80 FROM t)
         ORDER BY k";
    let plan_text = explain_text(&engine, &format!("EXPLAIN {parallel_sql}"));
    assert!(
        plan_text.contains("Parallel aggregate"),
        "expected 'Parallel aggregate' in EXPLAIN, got: {plan_text}"
    );
    let analyze = explain_analyze_tree(&engine, parallel_sql);
    assert!(
        analyze.contains("execution_mode=vector")
            && analyze.contains("native_aggregate_rows=1200"),
        "an uncorrelated scalar HAVING subquery must be resolved before the batch HashAggregate preflight: {analyze}"
    );
    let parallel = execute_rows(&engine, parallel_sql);
    assert_eq!(serial, parallel);
    assert_eq!(parallel.len(), 10);
}

#[test]
fn parallel_hash_aggregate_disable_env_falls_back_to_serial() {
    let _lock = auto_parallel_test_lock();
    let _disable_agg = EnvGuardSimple::set("MYDB_PARALLEL_AGGREGATE_DISABLE", "1");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t_with_groups(&engine, 800, 20);

    // Result must still be correct even though the parallel-aggregate
    // path is disabled.
    let parallel = execute_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ g, COUNT(*) FROM t GROUP BY g ORDER BY g",
    );
    let serial = execute_rows(
        &engine,
        "SELECT /*+ NO_PARALLEL */ g, COUNT(*) FROM t GROUP BY g ORDER BY g",
    );
    assert_eq!(parallel, serial);
}

#[test]
fn parallel_hash_aggregate_surfaces_in_explain() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t_with_groups(&engine, 1500, 30);

    let plan_text = explain_text(
        &engine,
        "EXPLAIN SELECT /*+ PARALLEL(4) */ g, COUNT(*) FROM t GROUP BY g",
    );
    assert!(
        plan_text.contains("Parallel aggregate"),
        "expected 'Parallel aggregate' in EXPLAIN, got: {plan_text}"
    );
}

#[test]
fn parallel_global_text_count_distinct_matches_serial() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE global_distinct (\
                 id INT PRIMARY KEY, \
                 g INT NOT NULL, \
                 v TEXT COLLATE utf8mb4_0900_ai_ci NOT NULL) ZONE_MAP=OFF",
        )
        .unwrap();
    for batch_start in (0..4000).step_by(500) {
        let mut sql = String::from("INSERT INTO global_distinct VALUES ");
        for id in batch_start..batch_start + 500 {
            if id > batch_start {
                sql.push(',');
            }
            let value = match id % 5 {
                0 => "A",
                1 => "á",
                2 => "B",
                3 => "Русский",
                _ => "русский",
            };
            sql.push_str(&format!("({id},{},'{value}')", id % 7));
        }
        engine.execute(&sql).unwrap();
    }

    let serial = execute_rows(
        &engine,
        "SELECT /*+ NO_PARALLEL */ COUNT(DISTINCT v) FROM global_distinct",
    );
    let parallel_sql = "SELECT /*+ PARALLEL(4) */ COUNT(DISTINCT v) FROM global_distinct";
    let tree = explain_text(&engine, &format!("EXPLAIN FORMAT=TREE {parallel_sql}"));
    assert!(
        tree.contains("Parallel aggregate") && tree.contains("workers=4"),
        "global decomposable aggregate must run worker-local partials: {tree}"
    );
    assert!(
        !tree.contains("est_mem=104"),
        "DISTINCT memory must include estimated set entries: {tree}"
    );
    let parallel = execute_rows(&engine, parallel_sql);
    assert_eq!(serial, parallel);
    assert_eq!(parallel, vec![vec![Value::Int(3)]]);

    let grouped = "SELECT /*+ PARALLEL(4) */ g, COUNT(DISTINCT v) \
                   FROM global_distinct GROUP BY g";
    let grouped_tree = explain_text(&engine, &format!("EXPLAIN FORMAT=TREE {grouped}"));
    assert!(
        grouped_tree.contains("Parallel aggregate") && !grouped_tree.contains("-> Sort"),
        "a full sort does not eliminate DISTINCT accumulator state: {grouped_tree}"
    );
    assert_eq!(
        execute_rows(&engine, grouped),
        execute_rows(
            &engine,
            "SELECT /*+ NO_PARALLEL */ g, COUNT(DISTINCT v) \
             FROM global_distinct GROUP BY g",
        )
    );

    let analyze = explain_analyze_tree(&engine, parallel_sql);
    assert!(
        analyze.contains("workers=4")
            && analyze.contains("expression_mode=compiled")
            && analyze.contains("native_expression_rows=4000")
            && analyze.contains("aggregate_mode=typed_batch"),
        "parallel native distinct-key metrics missing: {analyze}"
    );
}

#[test]
fn parallel_rollup_aggregate_matches_serial_and_surfaces_in_explain() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t_with_groups(&engine, 1500, 30);

    let sql = "SELECT GROUPING(g), g, COUNT(*), SUM(v) \
               FROM t \
               GROUP BY g WITH ROLLUP \
               ORDER BY GROUPING(g), g";
    let serial = execute_rows_in_result_order(&engine, sql);
    let parallel_sql = "SELECT /*+ PARALLEL(4) */ GROUPING(g), g, COUNT(*), SUM(v) \
                        FROM t \
                        GROUP BY g WITH ROLLUP \
                        ORDER BY GROUPING(g), g";
    let plan_text = explain_text(&engine, &format!("EXPLAIN FORMAT=TREE {parallel_sql}"));
    assert!(
        plan_text.contains("Parallel rollup aggregate"),
        "expected 'Parallel rollup aggregate' in EXPLAIN, got: {plan_text}"
    );
    let parallel = execute_rows_in_result_order(&engine, parallel_sql);
    assert_eq!(serial, parallel);
}

#[test]
fn parallel_two_phase_rollup_matches_serial_multilevel_with_distinct() {
    // P0-B: each worker runs a local two-phase rollup (finest + local prefix
    // derivation) and scatters every level into the partition maps; cross-worker
    // groups combine via `merge_rollup_group_state`. Validate that the parallel
    // result is identical to the serial two-phase path (itself differential-
    // tested against plain GROUP BY in `rollup.rs`) on a multi-level rollup,
    // INCLUDING SUM(DISTINCT)/COUNT(DISTINCT) over values that repeat heavily
    // across partitions — the case that exercises the cross-worker distinct
    // union (and would have caught the old double-counting `DistinctReducer`).
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE r (id INT PRIMARY KEY, a INT, b INT, amt INT)")
        .unwrap();
    for batch_start in (0..2000).step_by(500) {
        let batch_end = (batch_start + 500).min(2000);
        let mut sql = String::from("INSERT INTO r VALUES ");
        for id in batch_start..batch_end {
            if id > batch_start {
                sql.push(',');
            }
            // a in 0..4, b in 0..5, amt in 0..10 — amt repeats across every
            // (a,b) group, so coarser levels truly re-deduplicate on merge.
            sql.push_str(&format!("({id}, {}, {}, {})", id % 4, id % 5, id % 10));
        }
        engine.execute(&sql).unwrap();
    }

    let aggs = "COUNT(*), SUM(amt), MIN(amt), MAX(amt), AVG(amt), \
                COUNT(DISTINCT amt), SUM(DISTINCT amt)";
    let order = "ORDER BY GROUPING(a, b), GROUPING(a), a, b";
    let serial_sql = format!("SELECT a, b, {aggs} FROM r GROUP BY a, b WITH ROLLUP {order}");
    let parallel_sql =
        format!("SELECT /*+ PARALLEL(4) */ a, b, {aggs} FROM r GROUP BY a, b WITH ROLLUP {order}");

    let plan_text = explain_text(&engine, &format!("EXPLAIN FORMAT=TREE {parallel_sql}"));
    assert!(
        plan_text.contains("Parallel rollup aggregate"),
        "expected 'Parallel rollup aggregate' in EXPLAIN, got: {plan_text}"
    );

    let serial = execute_rows_in_result_order(&engine, &serial_sql);
    let parallel = execute_rows_in_result_order(&engine, &parallel_sql);
    assert_eq!(serial, parallel);
}

#[test]
fn parallel_rollup_window_rank_matches_serial_with_boundary_ties() {
    // Exercise the Q67 operator shape across two derived-table boundaries.
    // Five grouping keys deliberately keep this on the wide row-partitioned
    // path (rather than the compact vector fast path). Include ties at the
    // rank boundary to verify per-worker Top-K emission remains the serial
    // oracle's exact result.
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE rt (id INT PRIMARY KEY, g VARCHAR(8), h INT, i INT, j INT, k INT, v INT)",
        )
        .unwrap();
    for batch_start in (0..1600).step_by(400) {
        let batch_end = (batch_start + 400).min(1600);
        let mut sql = String::from("INSERT INTO rt VALUES ");
        for id in batch_start..batch_end {
            if id > batch_start {
                sql.push(',');
            }
            sql.push_str(&format!(
                "({id}, 'g{}', {}, {}, {}, {}, {})",
                id % 8,
                id % 200,
                id % 7,
                id % 11,
                id % 13,
                id % 5
            ));
        }
        engine.execute(&sql).unwrap();
    }

    let serial = execute_rows_in_result_order(
        &engine,
        "SELECT g, h, s, rk FROM ( \
             SELECT g, h, s, RANK() OVER (PARTITION BY g ORDER BY s DESC) rk \
             FROM (SELECT /*+ NO_PARALLEL */ g, h, SUM(v) s \
                   FROM rt GROUP BY g, h, i, j, k WITH ROLLUP) r1 \
         ) r2 WHERE rk <= 2 ORDER BY g IS NULL, g, rk, h",
    );
    let parallel_sql = "SELECT g, h, s, rk FROM ( \
             SELECT g, h, s, RANK() OVER (PARTITION BY g ORDER BY s DESC) rk \
             FROM (SELECT /*+ PARALLEL(4) */ g, h, SUM(v) s \
                   FROM rt GROUP BY g, h, i, j, k WITH ROLLUP) r1 \
         ) r2 WHERE rk <= 2 ORDER BY g IS NULL, g, rk, h";
    let plan_text = explain_text(&engine, &format!("EXPLAIN FORMAT=TREE {parallel_sql}"));
    assert!(
        plan_text.contains("Parallel rollup aggregate"),
        "expected parallel ROLLUP below the window, got: {plan_text}"
    );
    let parallel = execute_rows_in_result_order(&engine, parallel_sql);
    assert_eq!(serial, parallel);
}

#[test]
fn execution_context_carries_rollup_window_topk_hint_to_worker() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let hint = crate::engine::executor::select::plan::RollupWindowTopKHint {
        partition_by: vec![crate::parser::Expr::Identifier("g".into())],
        order_by: vec![crate::parser::OrderBy {
            expr: crate::parser::Expr::Identifier("s".into()),
            asc: false,
            nulls_first: None,
        }],
        k: 7,
        kind: crate::engine::executor::dispatch::RankKind::Rank,
    };
    let guard = crate::engine::executor::select::plan::set_rollup_window_topk_hint(Some(hint));
    let txn = engine
        .database()
        .read_only_statement_view(crate::storage::IsolationLevel::ReadCommitted, None);
    let context =
        crate::engine::executor::select::execution_context::ExecutionContext::capture(txn);
    drop(guard);

    let db = std::sync::Arc::clone(engine.database());
    let propagated = std::thread::spawn(move || {
        context.run(&db, || {
            crate::engine::executor::select::plan::current_rollup_window_topk_hint()
                .map(|hint| hint.k)
        })
    })
    .join()
    .unwrap();
    assert_eq!(propagated, Some(7));
}

#[test]
fn parallel_fused_hash_aggregate_spills_under_tight_budget() {
    let _lock = auto_parallel_test_lock();
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Stay above the auto router's CPU break-even point; 1,000 rows
    // deliberately remains row mode and therefore cannot exercise worker
    // spill even with a PARALLEL hint.
    populate_t_with_groups(&engine, 1_500, 1_500);

    // Every worker inherits the shared query budget. It must spill raw
    // partition state and merge it without restarting the scan.
    let budget = Arc::new(MemBudget::new(8 * 1024));
    let spill_before = crate::storage::spill_bytes_written();
    let response = with_query_budget(Arc::clone(&budget), || {
        engine.execute(
            "SELECT /*+ PARALLEL(4) */ g, COUNT(*), SUM(v), MIN(v), MAX(v), AVG(v) \
             FROM t GROUP BY g",
        )
    })
    .expect("parallel fused aggregate must complete via worker spill");
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1_500);
    let explain = explain_text(
        &engine,
        "EXPLAIN ANALYZE FORMAT=TREE \
         SELECT /*+ PARALLEL(4) */ g, COUNT(*), SUM(v), MIN(v), MAX(v), AVG(v) \
         FROM t GROUP BY g",
    );
    assert!(
        explain.contains("execution_mode=vector") && explain.contains("workers=4"),
        "parallel aggregate must report the vec worker pipeline: {explain}"
    );
    assert!(
        crate::storage::spill_bytes_written() > spill_before,
        "tight parallel aggregate budget must write spill segments"
    );
    // Charge is released via ScopedCharge / OwnedMemCharge drop at
    // query teardown, so the per-query counter is back to zero.
    assert_eq!(
        budget.used(),
        0,
        "per-query budget must be back to zero after force-charge teardown"
    );
}

#[test]
fn parallel_int_key_arena_retries_spill_after_worker_overflow() {
    let _lock = auto_parallel_test_lock();
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t_with_groups(&engine, 1_500, 1_500);

    let budget = Arc::new(MemBudget::new(32 * 1024));
    let response = with_vec_mode(VecMode::Off, || {
        with_query_budget(Arc::clone(&budget), || {
            engine.execute(
                "SELECT /*+ PARALLEL(4) */ g, SUM(v) \
                 FROM t GROUP BY g",
            )
        })
    })
    .expect("parallel int-key arena must retry through row hash-agg spill");
    let QueryResponse::ResultSet(mut result) = response else {
        panic!("expected result set");
    };
    let analyze = with_vec_mode(VecMode::Off, || {
        with_query_budget(Arc::clone(&budget), || {
            explain_analyze_tree(
                &engine,
                "SELECT /*+ PARALLEL(4) */ g, SUM(v) FROM t GROUP BY g",
            )
        })
    });
    assert!(
        analyze.contains("spill_bytes=") && analyze.contains("aggregate_mode=scalar_batch"),
        "parallel scalar aggregate should spill in the common operator: {analyze}"
    );
    assert_eq!(
        budget.used(),
        0,
        "parallel int-key spill retry must release query budget"
    );

    result.rows.sort_by(|a, b| match (&a[0], &b[0]) {
        (Value::Int(l), Value::Int(r)) => l.cmp(r),
        other => panic!("expected integer group keys, got {other:?}"),
    });
    assert_eq!(result.rows.len(), 1_500);
    assert_eq!(result.rows[0][0], Value::Int(0));
    assert_eq!(result.rows[0][1].to_display_string(), "0");
    assert_eq!(result.rows[1_499][0], Value::Int(1_499));
    assert_eq!(result.rows[1_499][1].to_display_string(), "4497");
}

#[test]
fn parallel_fused_hash_aggregate_releases_group_memory_budget() {
    let _lock = auto_parallel_test_lock();
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t_with_groups(&engine, 1500, 75);

    let budget = Arc::new(MemBudget::new(1_000_000));
    let QueryResponse::ResultSet(result) = with_query_budget(Arc::clone(&budget), || {
        engine.execute(
            "SELECT /*+ PARALLEL(4) */ g, COUNT(*), SUM(v), MIN(v), MAX(v), AVG(v) \
             FROM t GROUP BY g",
        )
    })
    .unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 75);
    assert_eq!(
        budget.used(),
        0,
        "parallel fused aggregate must release worker group budget"
    );
}

#[test]
fn parallel_disable_env_short_circuits_parallel_hint() {
    let _lock = auto_parallel_test_lock();
    // MYDB_PARALLEL_DISABLE=1 must beat /*+ PARALLEL(n) */.
    // We can't safely set env globally (race with other tests), so we
    // probe the planner output instead.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 100);

    // Use a scope guard to temporarily set the env var. Tests run in
    // serial within a single binary so this is safe within this test.
    struct EnvGuard {
        key: &'static str,
        prior: Option<String>,
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match self.prior.take() {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }
    let _guard = EnvGuard {
        key: "MYDB_PARALLEL_DISABLE",
        prior: std::env::var("MYDB_PARALLEL_DISABLE").ok(),
    };
    std::env::set_var("MYDB_PARALLEL_DISABLE", "1");

    let QueryResponse::ResultSet(result) = engine
        .execute("EXPLAIN SELECT /*+ PARALLEL(4) */ id FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let plan_text: String = result
        .rows
        .iter()
        .flat_map(|row| {
            row.iter().map(|v| match v {
                Value::String(s) => s.clone(),
                other => format!("{other:?}"),
            })
        })
        .collect::<Vec<_>>()
        .join(" | ");
    assert!(
        !plan_text.contains("Parallel"),
        "MYDB_PARALLEL_DISABLE should suppress parallel; got: {plan_text}"
    );
}

#[test]
fn parallel_hint_pushes_through_hash_join() {
    // PARALLEL(n) must reach SeqScans on both sides of a HashJoin, not
    // stop at the join node. Otherwise large-table joins (TPC-H Q3 etc.)
    // stay fully serial despite the hint.
    let _tune = hash_join_test_tuning();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_join_tables(&engine, 600, 400);

    let tree = explain_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT /*+ PARALLEL(4) */ l.id, r.id \
         FROM l INNER JOIN r ON l.k = r.k",
    );
    let parallel_count = tree.matches("Parallel table scan").count();
    assert!(
        parallel_count >= 2,
        "expected parallel scans on both join sides, got {parallel_count} in: {tree}"
    );
    assert!(
        tree.contains("workers=4"),
        "expected 'workers=4' in EXPLAIN, got: {tree}"
    );

    // Correctness check: result must match the serial baseline.
    let serial_threshold = EnvGuardSimple::set("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD", "999999999");
    let serial = raw_rows(
        &engine,
        "SELECT l.id, r.id FROM l INNER JOIN r ON l.k = r.k",
    );
    drop(serial_threshold);
    let parallel = raw_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ l.id, r.id FROM l INNER JOIN r ON l.k = r.k",
    );
    assert_eq!(serial, parallel);
    assert!(!serial.is_empty());
}

#[test]
fn parallel_hint_pushes_through_index_fast_scan_hash_join() {
    let _tune = hash_join_test_tuning();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_covering_join_tables(&engine, 1200, 1400);

    let tree = explain_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT /*+ PARALLEL(4) HASH_JOIN(l2,r2) \
         INDEX_FFS(l2 idx_l2_k) INDEX_FFS(r2 idx_r2_k) */ COUNT(*) \
         FROM l2 INNER JOIN r2 ON l2.k = r2.k",
    );
    let parallel_count = tree.matches("Parallel index-only scan").count();
    assert!(
        parallel_count >= 2,
        "expected parallel index-only scans on both join sides, got {parallel_count} in: {tree}"
    );
    assert!(
        tree.contains("workers=4"),
        "expected 'workers=4' in EXPLAIN, got: {tree}"
    );

    let serial = raw_rows(
        &engine,
        "SELECT /*+ NO_PARALLEL HASH_JOIN(l2,r2) INDEX_FFS(l2 idx_l2_k) INDEX_FFS(r2 idx_r2_k) */ COUNT(*) \
        FROM l2 INNER JOIN r2 ON l2.k = r2.k",
    );
    let batch_parallel_before =
        crate::engine::executor::select::vec_bridge::PARALLEL_INDEX_BATCH_SCAN_HITS
            .load(std::sync::atomic::Ordering::Relaxed);
    let parallel = raw_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) HASH_JOIN(l2,r2) INDEX_FFS(l2 idx_l2_k) INDEX_FFS(r2 idx_r2_k) */ COUNT(*) \
        FROM l2 INNER JOIN r2 ON l2.k = r2.k",
    );
    let batch_parallel_after =
        crate::engine::executor::select::vec_bridge::PARALLEL_INDEX_BATCH_SCAN_HITS
            .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(serial, parallel);
    assert_eq!(serial.len(), 1);
    assert!(
        batch_parallel_after >= batch_parallel_before + 2,
        "both planned parallel index leaves must execute with native workers"
    );
}

#[test]
fn parallel_noncovering_index_range_matches_serial() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE range_fact (
                id INT PRIMARY KEY,
                d DATE NOT NULL,
                v INT NOT NULL,
                INDEX idx_d (d)
            )",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO range_fact \
             SELECT n, CASE MOD(n,3) \
                         WHEN 0 THEN '1995-09-01' \
                         WHEN 1 THEN '1995-09-02' \
                         ELSE '1995-09-03' END, MOD(n,97) \
             FROM generate_series(0,1999) AS gs(n)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE range_fact").unwrap();

    let predicate = "d >= '1995-09-01' AND d < '1995-09-04'";
    let serial = execute_rows(
        &engine,
        &format!(
            "SELECT /*+ NO_PARALLEL USE_INDEX(range_fact idx_d) */ id, v \
             FROM range_fact WHERE {predicate}"
        ),
    );
    let parallel_sql = format!(
        "SELECT /*+ PARALLEL(4) USE_INDEX(range_fact idx_d) */ id, v \
         FROM range_fact WHERE {predicate}"
    );
    let tree = explain_text(&engine, &format!("EXPLAIN FORMAT=TREE {parallel_sql}"));
    assert!(
        tree.contains("Parallel index range scan on range_fact using idx_d"),
        "expected a non-covering parallel range plan: {tree}"
    );

    let parallel = crate::engine::executor::vec::with_vec_mode(
        crate::engine::executor::vec::VecMode::On,
        || execute_rows(&engine, &parallel_sql),
    );
    assert_eq!(parallel, serial);
    assert_eq!(parallel.len(), 2_000);
    assert_eq!(parallel.first(), Some(&vec![Value::Int(0), Value::Int(0)]));
    assert_eq!(
        parallel.last(),
        Some(&vec![Value::Int(1_999), Value::Int(1_999 % 97)])
    );
}

#[test]
fn parallel_index_fast_scan_uses_leaf_visibility_shortcut() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (
                id INT PRIMARY KEY,
                k INT NOT NULL,
                INDEX k_idx (k)
            )",
        )
        .unwrap();
    let total = 640;
    for start in (0..total).step_by(160) {
        let values = (start..start + 160)
            .map(|id| format!("({id},{})", id % 997))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO t VALUES {values}"))
            .unwrap();
    }

    let sql = "SELECT /*+ PARALLEL(4) INDEX_FFS(t k_idx) */ k FROM t";
    let tree = explain_text(&engine, &format!("EXPLAIN FORMAT=TREE {sql}"));
    assert!(
        tree.contains("Parallel index-only scan"),
        "regression must exercise parallel IndexFastScan: {tree}"
    );

    // Move the retained-txn watermark past the committed insert batches
    // so their leaf bounds are eligible for the all-visible proof.
    engine.db_for_tests().gc_committed_txn_slots();
    let before = engine.db_for_tests().storage_status();
    let rows = raw_rows(&engine, sql);
    let after = engine.db_for_tests().storage_status();
    assert_eq!(rows.len(), total);
    assert!(
        after.index_page_max_trx_shortcut_hits > before.index_page_max_trx_shortcut_hits,
        "parallel covering scan must consume each leaf's page_max_trx_id"
    );
}

#[test]
fn fused_count_dual_covering_index_spills_under_tight_budget() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};

    let _tune = hash_join_test_tuning();
    let _streaming = EnvGuardSimple::unset("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");
    let _partitions = EnvGuardSimple::set("MYDB_HASH_JOIN_GRACE_PARTITIONS", "2");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_covering_join_tables(&engine, 2_000, 2_200);

    let sql = "SELECT /*+ PARALLEL(4) HASH_JOIN(l2,r2) \
               INDEX_FFS(l2 idx_l2_k) INDEX_FFS(r2 idx_r2_k) */ COUNT(*) \
               FROM l2 INNER JOIN r2 ON l2.k = r2.k";
    let generous = std::sync::Arc::new(MemBudget::new(16 * 1024 * 1024));
    let expected = with_query_budget(std::sync::Arc::clone(&generous), || raw_rows(&engine, sql));
    assert_eq!(
        generous.used(),
        0,
        "baseline query leaked its memory charge"
    );

    // Keep the regression genuinely below the in-memory footprint so it
    // continues to exercise the same governed Grace-spill path.
    let tight = std::sync::Arc::new(MemBudget::new(128 * 1024));
    let parallel_before =
        crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_GRACE_PARALLEL_WORKERS
            .load(std::sync::atomic::Ordering::Relaxed);
    let actual = with_query_budget(std::sync::Arc::clone(&tight), || raw_rows(&engine, sql));
    let parallel_after =
        crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_GRACE_PARALLEL_WORKERS
            .load(std::sync::atomic::Ordering::Relaxed);
    let analyze = with_query_budget(std::sync::Arc::clone(&tight), || {
        explain_analyze_tree(&engine, sql)
    });

    assert_eq!(actual, expected);
    assert_eq!(tight.used(), 0, "spilled query leaked its memory charge");
    assert!(
        parallel_after > parallel_before,
        "governed Grace replay must use more than one partition worker"
    );
    assert!(
        analyze.contains("spill_bytes="),
        "dual covering-index COUNT(*) should expose spill under a 128 KiB query budget: {analyze}"
    );
}

#[test]
fn fused_count_dual_covering_index_uses_compact_map_and_page_visibility() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};

    let _tune = hash_join_test_tuning();
    let _streaming = EnvGuardSimple::unset("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_covering_join_tables(&engine, 800, 600);

    let oracle = raw_rows(
        &engine,
        "SELECT /*+ FULL_SCAN(l2) FULL_SCAN(r2) HASH_JOIN(l2,r2) */ COUNT(*) \
         FROM l2 INNER JOIN r2 ON l2.k = r2.k",
    );
    let sql = "SELECT /*+ PARALLEL(4) HASH_JOIN(l2,r2) \
               INDEX_FFS(l2 idx_l2_k) INDEX_FFS(r2 idx_r2_k) */ COUNT(*) \
               FROM l2 INNER JOIN r2 ON l2.k = r2.k";
    let spill_before = crate::engine::executor::select::plan::HASH_JOIN_SPILL_BYTES
        .load(std::sync::atomic::Ordering::Relaxed);
    let status_before = engine.db_for_tests().storage_status();
    let budget = std::sync::Arc::new(MemBudget::new(16 * 1024 * 1024));
    let actual = with_query_budget(std::sync::Arc::clone(&budget), || raw_rows(&engine, sql));
    let status_after = engine.db_for_tests().storage_status();
    let spill_after = crate::engine::executor::select::plan::HASH_JOIN_SPILL_BYTES
        .load(std::sync::atomic::Ordering::Relaxed);

    assert_eq!(actual, oracle);
    assert_eq!(budget.used(), 0, "compact count map leaked its charge");
    assert_eq!(
        spill_after, spill_before,
        "an admitted compact integer map must not write Grace partitions"
    );
    assert!(
        status_after.index_page_max_trx_shortcut_hits
            > status_before.index_page_max_trx_shortcut_hits,
        "both fused covering-index streams must use leaf visibility metadata"
    );
    assert_eq!(
        status_after.index_page_max_trx_shortcut_misses,
        status_before.index_page_max_trx_shortcut_misses,
        "bulk-built all-visible leaves must not fall back to heap headers"
    );
}

#[test]
fn fused_count_dual_index_i64_spill_declines_string_keys_without_losing_rows() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};

    let _tune = hash_join_test_tuning();
    let _streaming = EnvGuardSimple::unset("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");
    let _partitions = EnvGuardSimple::set("MYDB_HASH_JOIN_GRACE_PARTITIONS", "2");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE ls (id INT PRIMARY KEY, k VARCHAR(16) NOT NULL, INDEX idx_ls_k(k))")
        .unwrap();
    engine
        .execute("CREATE TABLE rs (id INT PRIMARY KEY, k VARCHAR(16) NOT NULL, INDEX idx_rs_k(k))")
        .unwrap();
    for (table, rows) in [("ls", 300_i64), ("rs", 320_i64)] {
        for batch_start in (0..rows).step_by(80) {
            let batch_end = (batch_start + 80).min(rows);
            let mut insert = format!("INSERT INTO {table} VALUES ");
            for id in batch_start..batch_end {
                if id > batch_start {
                    insert.push(',');
                }
                insert.push_str(&format!("({id}, 'k{}')", id % 57));
            }
            engine.execute(&insert).unwrap();
        }
    }

    let sql = "SELECT /*+ PARALLEL(4) HASH_JOIN(ls,rs) \
               INDEX_FFS(ls idx_ls_k) INDEX_FFS(rs idx_rs_k) */ COUNT(*) \
               FROM ls INNER JOIN rs ON ls.k = rs.k";
    let generous = std::sync::Arc::new(MemBudget::new(16 * 1024 * 1024));
    let expected = with_query_budget(std::sync::Arc::clone(&generous), || raw_rows(&engine, sql));
    let tight = std::sync::Arc::new(MemBudget::new(32 * 1024));
    let actual = with_query_budget(std::sync::Arc::clone(&tight), || raw_rows(&engine, sql));

    assert_eq!(actual, expected);
    assert_ne!(actual, vec![vec![Value::Int(0)]]);
    assert_eq!(tight.used(), 0, "fallback query leaked its memory charge");
}

#[test]
fn fused_count_hash_join_streams_parallel_covering_index_probe() {
    let _tune = hash_join_test_tuning();
    let _streaming = EnvGuardSimple::unset("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE d (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE f (id INT PRIMARY KEY, k INT, INDEX idx_f_k(k))")
        .unwrap();
    engine
        .execute("INSERT INTO d VALUES (1, 1), (2, 2), (3, 2), (4, NULL), (5, 7)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO f VALUES \
             (10, 1), (11, 2), (12, 2), (13, 3), (14, NULL), (15, 7), (16, 7)",
        )
        .unwrap();

    let tree = explain_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT /*+ PARALLEL(4) HASH_JOIN(d,f) INDEX_FFS(f idx_f_k) */ \
         COUNT(*) FROM d JOIN f ON f.k = d.k",
    );
    assert!(
        tree.contains("Parallel index-only scan"),
        "expected covering parallel index probe in plan: {tree}"
    );

    let counter_before = native_batch_hits();
    let parallel = raw_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) HASH_JOIN(d,f) INDEX_FFS(f idx_f_k) */ \
         COUNT(*) FROM d JOIN f ON f.k = d.k",
    );
    let counter_after = native_batch_hits();

    assert_eq!(parallel, vec![vec![Value::Int(7)]]);
    assert!(
        counter_after > counter_before,
        "covering index probe should use the common batch pipeline"
    );
    let status = explain_analyze_tree(
        &engine,
        "SELECT /*+ PARALLEL(4) HASH_JOIN(d,f) INDEX_FFS(f idx_f_k) */ \
         COUNT(*) FROM d JOIN f ON f.k = d.k",
    );
    assert!(
        status.contains("execution_mode=vector") && !status.contains("conversion_rows="),
        "join→aggregate must not cross a PlanRow conversion boundary: {status}"
    );

    let _fallback = EnvGuardSimple::set("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", "1");
    let materialized = raw_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) HASH_JOIN(d,f) INDEX_FFS(f idx_f_k) */ \
         COUNT(*) FROM d JOIN f ON f.k = d.k",
    );
    assert_eq!(parallel, materialized);
}

#[test]
fn fused_hash_join_streams_covering_index_probe_ungrouped_aggregates() {
    let _tune = hash_join_test_tuning();
    let _streaming = EnvGuardSimple::unset("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE d (id INT PRIMARY KEY, k INT, label VARCHAR(8))")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE f (id INT PRIMARY KEY, k INT, v INT, extra INT, \
             INDEX idx_f_k_v (k, v))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO d VALUES \
             (1, 1, 'a'), (2, 2, 'b'), (3, 2, 'c'), (4, NULL, 'n'), (5, 7, 'z')",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO f VALUES \
             (10, 1, 5, 100), (11, 2, 7, 101), (12, 2, NULL, 102), \
             (13, 3, 11, 103), (14, NULL, 13, 104), (15, 7, 17, 105), (16, 7, 19, 106)",
        )
        .unwrap();

    let sql = "SELECT /*+ PARALLEL(4) HASH_JOIN(d,f) INDEX_FFS(f idx_f_k_v) */ \
         COUNT(*), COUNT(f.v), SUM(f.v), AVG(f.v), MIN(f.v), MAX(f.v) \
         FROM d JOIN f ON f.k = d.k";
    let tree = explain_text(&engine, &format!("EXPLAIN FORMAT=TREE {sql}"));
    assert!(
        tree.contains("Parallel index-only scan"),
        "expected covering parallel index probe in plan: {tree}"
    );

    let counter_before = native_batch_hits();
    let parallel = raw_rows(&engine, sql);
    let counter_after = native_batch_hits();

    let _fallback = EnvGuardSimple::set("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", "1");
    let materialized = raw_rows(&engine, sql);
    assert_eq!(parallel, materialized);
    assert_eq!(
        parallel,
        vec![vec![
            Value::Int(7),
            Value::Int(5),
            Value::DecimalCompact(55, 0),
            Value::DecimalCompact(110000, 4),
            Value::Int(5),
            Value::Int(19),
        ]]
    );
    assert!(counter_after > counter_before);
    let analyze = explain_analyze_tree(&engine, sql);
    assert!(
        !analyze.contains("conversion_rows=") && analyze.contains("native_aggregate_rows="),
        "covering index join→aggregate metrics missing: {analyze}"
    );
}

#[test]
fn fused_hash_join_streams_covering_index_probe_composite_string_key() {
    let _tune = hash_join_test_tuning();
    let _streaming = EnvGuardSimple::unset("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE d (id INT PRIMARY KEY, k1 INT, k2 VARCHAR(8))")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE f (id INT PRIMARY KEY, k1 INT, k2 VARCHAR(8), v INT, \
             INDEX idx_f_k1_k2_v (k1, k2, v))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO d VALUES \
             (1, 1, 'a'), (2, 1, 'a'), (3, 2, 'b'), (4, 2, NULL), (5, 3, 'c')",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO f VALUES \
             (10, 1, 'a', 5), (11, 1, 'a', 7), (12, 2, 'b', 11), \
             (13, 2, NULL, 13), (14, 3, 'x', 17), (15, 3, 'c', 19)",
        )
        .unwrap();

    let sql = "SELECT /*+ PARALLEL(4) HASH_JOIN(d,f) INDEX_FFS(f idx_f_k1_k2_v) */ \
         COUNT(*), SUM(f.v), MIN(f.v), MAX(f.v) \
         FROM d JOIN f ON f.k1 = d.k1 AND f.k2 = d.k2";
    let counter_before = native_batch_hits();
    let parallel = raw_rows(&engine, sql);
    let counter_after = native_batch_hits();
    let _fallback = EnvGuardSimple::set("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", "1");
    let materialized = raw_rows(&engine, sql);

    assert_eq!(parallel, materialized);
    assert_eq!(
        parallel,
        vec![vec![
            Value::Int(6),
            Value::DecimalCompact(54, 0),
            Value::Int(5),
            Value::Int(19),
        ]]
    );
    assert!(counter_after > counter_before);
}

#[test]
fn fused_hash_join_streams_covering_index_probe_grouped_aggregates() {
    let _tune = hash_join_test_tuning();
    let _streaming = EnvGuardSimple::unset("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE d (id INT PRIMARY KEY, k INT, bucket INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE f (id INT PRIMARY KEY, k INT, p INT, v INT, \
             INDEX idx_f_k_p_v (k, p, v))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO d VALUES \
             (1, 1, 10), (2, 2, 20), (3, 2, 20), (4, 4, 40), (5, NULL, 50)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO f VALUES \
             (10, 1, 100, 5), (11, 2, 100, 7), (12, 2, 200, NULL), \
             (13, 3, 100, 11), (14, NULL, 300, 13), (15, 4, 200, 17)",
        )
        .unwrap();

    let grouped_before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let build_group = execute_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) HASH_JOIN(d,f) INDEX_FFS(f idx_f_k_p_v) */ \
         d.bucket, COUNT(*), COUNT(f.v), SUM(f.v), MIN(f.v), MAX(f.v) \
         FROM d JOIN f ON f.k = d.k GROUP BY d.bucket ORDER BY d.bucket",
    );
    let probe_group = execute_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) HASH_JOIN(d,f) INDEX_FFS(f idx_f_k_p_v) */ \
         f.p, COUNT(*), SUM(f.v), MIN(f.v), MAX(f.v) \
         FROM d JOIN f ON f.k = d.k GROUP BY f.p ORDER BY f.p",
    );
    let mixed_group = execute_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) HASH_JOIN(d,f) INDEX_FFS(f idx_f_k_p_v) */ \
         d.bucket, f.p, COUNT(*), SUM(f.v) \
         FROM d JOIN f ON f.k = d.k GROUP BY d.bucket, f.p ORDER BY d.bucket, f.p",
    );
    let grouped_after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);

    let _fallback = EnvGuardSimple::set("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", "1");
    assert_eq!(
        build_group,
        execute_rows(
            &engine,
            "SELECT /*+ PARALLEL(4) HASH_JOIN(d,f) INDEX_FFS(f idx_f_k_p_v) */ \
             d.bucket, COUNT(*), COUNT(f.v), SUM(f.v), MIN(f.v), MAX(f.v) \
             FROM d JOIN f ON f.k = d.k GROUP BY d.bucket ORDER BY d.bucket",
        )
    );
    assert_eq!(
        probe_group,
        execute_rows(
            &engine,
            "SELECT /*+ PARALLEL(4) HASH_JOIN(d,f) INDEX_FFS(f idx_f_k_p_v) */ \
             f.p, COUNT(*), SUM(f.v), MIN(f.v), MAX(f.v) \
             FROM d JOIN f ON f.k = d.k GROUP BY f.p ORDER BY f.p",
        )
    );
    assert_eq!(
        mixed_group,
        execute_rows(
            &engine,
            "SELECT /*+ PARALLEL(4) HASH_JOIN(d,f) INDEX_FFS(f idx_f_k_p_v) */ \
             d.bucket, f.p, COUNT(*), SUM(f.v) \
             FROM d JOIN f ON f.k = d.k GROUP BY d.bucket, f.p ORDER BY d.bucket, f.p",
        )
    );
    assert!(
        grouped_after >= grouped_before + 3,
        "expected all grouped index joins to reach the common batch pipeline"
    );
}

#[test]
fn fused_hash_join_streaming_index_probe_fallback_probe_preserved_outer() {
    let _tune = hash_join_test_tuning();
    let _streaming = EnvGuardSimple::unset("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE d (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE f (id INT PRIMARY KEY, k INT, INDEX idx_f_k(k))")
        .unwrap();
    engine
        .execute("INSERT INTO d VALUES (1, 1), (2, 2)")
        .unwrap();
    engine
        .execute("INSERT INTO f VALUES (10, 1), (11, 2), (12, 3), (13, NULL)")
        .unwrap();

    let fallback_before = native_batch_hits();
    let parallel = raw_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) HASH_JOIN(d,f) INDEX_FFS(f idx_f_k) */ COUNT(*) \
         FROM d RIGHT JOIN f ON f.k = d.k",
    );
    let fallback_after = native_batch_hits();
    let _fallback = EnvGuardSimple::set("MYDB_HASH_JOIN_FUSED_STREAM_PROBE_DISABLE", "1");
    let materialized = raw_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) HASH_JOIN(d,f) INDEX_FFS(f idx_f_k) */ COUNT(*) \
         FROM d RIGHT JOIN f ON f.k = d.k",
    );
    assert_eq!(parallel, materialized);
    assert_eq!(parallel, vec![vec![Value::Int(4)]]);
    assert!(fallback_after > fallback_before);
}

fn populate_covering_agg_table(engine: &StorageQueryEngine, n: i64) {
    engine.execute("CREATE TABLE agg_t (id INT PRIMARY KEY, g INT NOT NULL, v INT NULL, n INT NULL, pad VARCHAR(80), INDEX idx_g_v_n (g, v, n))").unwrap();
    let payload = "payload".repeat(4);
    for start in (0..n).step_by(500) {
        insert_fixture_rows(
            engine,
            "test.agg_t",
            (start..(start + 500).min(n)).map(|id| {
                vec![
                    Value::Int(id),
                    Value::Int(id % 17),
                    if id % 11 == 0 {
                        Value::Null
                    } else {
                        Value::Int(id * 13 % 1009)
                    },
                    if id % 7 == 0 {
                        Value::Null
                    } else {
                        Value::Int(id * 5 % 503)
                    },
                    Value::String(payload.clone()),
                ]
            }),
        );
    }
}

#[test]
fn parallel_covering_index_aggregate_explain_and_count() {
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row_count = 1_200;
    populate_covering_agg_table(&engine, row_count);

    let tree = explain_text(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT /*+ PARALLEL(4) */ COUNT(*) FROM agg_t",
    );
    assert!(
        tree.contains("Parallel index aggregate") && tree.contains("workers=4"),
        "expected parallel covering index aggregate, got: {tree}"
    );

    let serial = raw_rows(&engine, "SELECT /*+ NO_PARALLEL */ COUNT(*) FROM agg_t");
    let parallel = raw_rows(&engine, "SELECT /*+ PARALLEL(4) */ COUNT(*) FROM agg_t");
    assert_eq!(serial, parallel);
    assert_eq!(parallel[0][0], Value::Int(row_count));

    let QueryResponse::ResultSet(analyze) = engine
        .execute("EXPLAIN ANALYZE FORMAT=TREE SELECT /*+ PARALLEL(4) */ COUNT(*) FROM agg_t")
        .unwrap()
    else {
        panic!("expected explain result set");
    };
    let text = analyze
        .rows
        .iter()
        .flat_map(|row| row.iter())
        .map(|value| match value {
            Value::String(s) => s.clone(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        text.contains("Parallel index aggregate"),
        "expected parallel aggregate in ANALYZE output, got: {text}"
    );
    assert!(
        (text.contains("physical_reads=") || text.contains("cache_reads="))
            && text.contains("cache_hit_rate="),
        "parallel analyze output should include block access stats: {text}"
    );
    assert!(text.starts_with("Query I/O: physical_reads="), "{text}");
    assert_eq!(text.matches("Query I/O:").count(), 1, "{text}");
    let root = text.lines().nth(1).unwrap();
    assert!(root.starts_with("-> Parallel index aggregate"), "{text}");
    assert!(root.contains("io_scope=operator_"), "{text}");
    assert!(!text.contains("query_physical_reads="), "{text}");
    println!("{text}");

    let sql = "SELECT COUNT(*), COUNT(v), SUM(v), AVG(v), MIN(v), MAX(v) FROM agg_t";
    let serial = raw_rows(&engine, &format!("SELECT /*+ NO_PARALLEL */ {}", &sql[7..]));
    let parallel = raw_rows(&engine, &format!("SELECT /*+ PARALLEL(4) */ {}", &sql[7..]));
    assert_eq!(serial, parallel);
    assert_eq!(parallel.len(), 1);

    let distinct_sql = "SELECT COUNT(DISTINCT v), SUM(DISTINCT v), AVG(DISTINCT v) FROM agg_t";
    let serial = raw_rows(
        &engine,
        &format!("SELECT /*+ NO_PARALLEL */ {}", &distinct_sql[7..]),
    );
    let parallel = raw_rows(
        &engine,
        &format!("SELECT /*+ PARALLEL(4) */ {}", &distinct_sql[7..]),
    );
    assert_eq!(serial, parallel);

    let serial = execute_rows(
        &engine,
        "SELECT /*+ NO_PARALLEL */ g, COUNT(*), COUNT(n), SUM(v), AVG(v), MIN(v), MAX(v) \
         FROM agg_t GROUP BY g",
    );
    let parallel = execute_rows(
        &engine,
        "SELECT /*+ PARALLEL(4) */ g, COUNT(*), COUNT(n), SUM(v), AVG(v), MIN(v), MAX(v) \
         FROM agg_t GROUP BY g",
    );
    assert_eq!(serial, parallel);
    assert_eq!(parallel.len(), 17);
}

/// TPC-H Q12 join orientation must not flip when `MYDB_PARALLEL_DEGREE`
/// grows.  `estimate_index_nlj_operator_cost` discounts the batched INLJ
/// cost by the parallel degree to model the executor's two layers of
/// outer-row partitioning (`apply_parallel_hints` wraps the outer SeqScan
/// in `Exchange`, and `execute_batched_inlj_noncovering` runs the inner
/// probe loop under `std::thread::scope`).  Before this regression test
/// was added the discount only divided the INLJ *increment* and was
/// gated on `per_match >= 2`, so the orders→lineitem direction
/// (per_match≈4) shrank with degree while the reverse PK probe direction
/// (per_match=1, dominated by a 6M-row lineitem scan + 5-conjunct
/// filter) did not.  At MYDB_PARALLEL_DEGREE=4 that asymmetry made the
/// 1.5M-probe orders-driven plan win on cost over the 30k-probe
/// lineitem-driven plan, even though the latter was 2× faster in wall
/// time.  Wide lineitem rows here push the lineitem-scan cost above the
/// orders-scan cost so the same cost comparison fires at unit scale.
#[test]
fn tpch_q12_inlj_direction_stable_across_parallel_degree() {
    use std::sync::{Mutex, OnceLock};
    static SERIALIZE: OnceLock<Mutex<()>> = OnceLock::new();
    let _serialize = SERIALIZE
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let _outer_threshold = EnvGuardSimple::set("MYDB_INLJ_OUTER_PARALLEL_THRESHOLD", "100");

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE orders ( \
             o_orderkey INT PRIMARY KEY, \
             o_orderpriority VARCHAR(20) NOT NULL \
             )",
        )
        .unwrap();
    // Pad lineitem with a wide VARCHAR so its serial scan cost exceeds
    // the orders scan cost, mirroring TPC-H sf=1 where lineitem is the
    // dominant table.  Without the padding the lineitem outer is too
    // cheap to expose the asymmetric-discount bug at this row count.
    engine
        .execute(
            "CREATE TABLE lineitem ( \
             l_orderkey INT NOT NULL, \
             l_shipmode VARCHAR(10) NOT NULL, \
             l_commitdate DATE NOT NULL, \
             l_receiptdate DATE NOT NULL, \
             l_shipdate DATE NOT NULL, \
             l_comment VARCHAR(220) NOT NULL, \
             INDEX idx_lineitem_orderkey (l_orderkey) \
             )",
        )
        .unwrap();

    // 1000 orders × 4 lineitem each = 4000 lineitem rows. per_match≈4 in
    // the orders→lineitem direction; per_match=1 in the reverse (orders
    // PK).  Both directions clear `MYDB_INLJ_OUTER_PARALLEL_THRESHOLD`
    // (overridden to 100 above) and `right_rows >= 1000`, so the discount
    // applies to either orientation the planner considers.
    let priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT", "5-LOW"];
    let mut orders_sql = String::from("INSERT INTO orders VALUES ");
    for i in 1..=1000 {
        if i > 1 {
            orders_sql.push(',');
        }
        let prio = priorities[(i as usize) % priorities.len()];
        orders_sql.push_str(&format!("({i}, '{prio}')"));
    }
    engine.execute(&orders_sql).unwrap();

    let padding = "x".repeat(200);
    for batch_start in (1..=1000).step_by(250) {
        let batch_end = (batch_start + 250).min(1001);
        let mut lineitem_sql = String::from("INSERT INTO lineitem VALUES ");
        let mut first = true;
        for ok in batch_start..batch_end {
            for lk in 0..4 {
                if !first {
                    lineitem_sql.push(',');
                }
                first = false;
                let mode = if (ok + lk) % 50 == 0 { "MAIL" } else { "RAIL" };
                let receipt = if (ok + lk) % 2 == 0 {
                    "1994-06-15"
                } else {
                    "1996-06-15"
                };
                lineitem_sql.push_str(&format!(
                    "({ok}, '{mode}', '1994-01-03', '{receipt}', '1994-01-01', '{padding}')"
                ));
            }
        }
        engine.execute(&lineitem_sql).unwrap();
    }
    engine.execute("ANALYZE TABLE orders").unwrap();
    engine.execute("ANALYZE TABLE lineitem").unwrap();

    let mut chosen_inlj_costs: Vec<f64> = Vec::new();
    for degree in ["1", "2", "4"] {
        let _degree_guard = EnvGuardSimple::set("MYDB_PARALLEL_DEGREE", degree);
        // Append a degree-specific marker comment so each iteration plans
        // afresh — the plan cache is keyed by AST fingerprint and would
        // otherwise serve the first iteration's plan to all three.
        let q12 = format!(
            "EXPLAIN FORMAT=TREE SELECT /*+ PARALLEL(4) */ /* deg={degree} */ l_shipmode, \
                  SUM(CASE WHEN o_orderpriority='1-URGENT' OR o_orderpriority='2-HIGH' \
                           THEN 1 ELSE 0 END) AS high_line_count \
              FROM orders INNER JOIN lineitem ON o_orderkey=l_orderkey \
              WHERE l_shipmode IN ('MAIL','SHIP') \
                AND l_commitdate<l_receiptdate \
                AND l_shipdate<l_commitdate \
                AND l_receiptdate>='1994-01-01' \
                AND l_receiptdate<DATE '1994-01-01'+INTERVAL '1' YEAR \
              GROUP BY l_shipmode \
              ORDER BY l_shipmode"
        );
        let QueryResponse::ResultSet(tree) = engine.execute(&q12).unwrap() else {
            panic!("expected explain result set at degree={degree}");
        };
        let text = match &tree.rows[0][0] {
            Value::String(s) => s.clone(),
            other => panic!("expected tree text at degree={degree}, got {other:?}"),
        };
        // The lineitem-driven direction (per_match=1, PK probe on orders)
        // is the only safe choice for this query shape.  If the discount
        // gating regresses to `per_match >= 2`, the orders-driven plan
        // wins at sf=1 — but at this unit-test scale the lineitem-driven
        // plan still wins on raw cost, so we additionally pin the cost
        // *trajectory* (see below).
        assert!(
            text.contains("Index lookup on orders using PRIMARY"),
            "Q12 should drive from filtered lineitem and probe orders PK at \
             MYDB_PARALLEL_DEGREE={degree}: {text}"
        );
        assert!(
            !text.contains("Index lookup on lineitem"),
            "Q12 must not flip to driving from orders and probing lineitem at \
             MYDB_PARALLEL_DEGREE={degree}: {text}"
        );
        // Pull the INLJ node's reported cumulative work. Parallelism changes
        // critical-path latency, not total I/O/CPU work, so this value must
        // remain stable across degrees while the chosen direction stays the
        // same.
        let needle = "Index nested loop join (cost=";
        let start = text
            .find(needle)
            .unwrap_or_else(|| panic!("INLJ row missing at degree={degree}: {text}"))
            + needle.len();
        let rest = &text[start..];
        let end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        let cost: f64 = rest[..end]
            .parse()
            .unwrap_or_else(|_| panic!("INLJ cost parse failed at degree={degree}: {text}"));
        chosen_inlj_costs.push(cost);
    }

    let (c1, c2, c4) = (
        chosen_inlj_costs[0],
        chosen_inlj_costs[1],
        chosen_inlj_costs[2],
    );
    assert!(
        (c2 - c1).abs() <= 0.01 && (c4 - c1).abs() <= 0.01,
        "parallel degree must not divide cumulative INLJ work; saw d=1:{c1:.2} \
         d=2:{c2:.2} d=4:{c4:.2}"
    );
}

// ---------- Parallel ungrouped aggregation regression ----------
//
// Repro for the "parallel bare aggregate returns Empty set" bug: a
// no-WHERE, no-GROUP-BY aggregate over a parallel seq scan routed into
// `try_vec_parallel_hash_aggregate_fused` projected zero columns
// (COUNT(*) contributes none), and a zero-column `Batch` carries
// `num_rows == 0`, so every scanned row was dropped and the merge
// produced no groups — the whole query returned 0 rows instead of the
// implicit single group. Related family members covered below: zero
// surviving input rows must still emit the implicit group, and the
// implicit group must not be split across hash partitions when some
// batches carry a selection and others don't.

/// Bare `COUNT(*)` with no WHERE over a parallel scan must return
/// exactly one row, on both the vec and the row fused paths.
#[test]
fn parallel_bare_count_star_no_where_returns_one_row() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);

    for mode in [VecMode::On, VecMode::Auto, VecMode::Off] {
        let rows = with_vec_mode(mode, || {
            execute_rows_in_result_order(&engine, "SELECT /*+ PARALLEL(4) */ COUNT(*) FROM t")
        });
        assert_eq!(rows, vec![vec![Value::Int(1500)]], "vec mode {mode:?}");
    }
}

/// Same shape with multiple bare aggregates (forces a non-empty scan
/// projection, so the vec fused path genuinely runs end to end).
#[test]
fn parallel_bare_multi_aggregate_no_where_matches_serial() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);

    let sql = "SELECT /*+ PARALLEL(4) */ COUNT(*), SUM(x), MIN(x), MAX(x) FROM t";
    let serial = with_vec_mode(VecMode::Off, || {
        execute_rows_in_result_order(&engine, "SELECT COUNT(*), SUM(x), MIN(x), MAX(x) FROM t")
    });
    assert_eq!(serial.len(), 1, "serial baseline must be a single row");
    for mode in [VecMode::On, VecMode::Off] {
        let rows = with_vec_mode(mode, || execute_rows_in_result_order(&engine, sql));
        assert_eq!(rows, serial, "vec mode {mode:?}");
    }
}

/// Zero surviving rows (filter rejects everything): the implicit
/// single group must still be emitted — COUNT(*) → 0, SUM/MIN → NULL.
#[test]
fn parallel_bare_aggregate_all_rows_filtered_emits_zero_group() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);

    let sql = "SELECT /*+ PARALLEL(4) */ COUNT(*), SUM(x), MIN(x) FROM t WHERE x < 0";
    for mode in [VecMode::On, VecMode::Off] {
        let rows = with_vec_mode(mode, || execute_rows_in_result_order(&engine, sql));
        assert_eq!(
            rows,
            vec![vec![Value::Int(0), Value::Null, Value::Null]],
            "vec mode {mode:?}"
        );
    }
}

/// Mixed-selection repro: a filter that rejects every row in some
/// workers' page ranges (their batches carry a selection) but keeps
/// every row in others' (no selection). The implicit empty-GROUP-BY
/// group must not be split across hash partitions by the two consume
/// paths — exactly one output row, with the same values as serial.
#[test]
fn parallel_bare_aggregate_partial_filter_single_group() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    populate_t(&engine, 1500);

    let sql = "SELECT /*+ PARALLEL(4) */ COUNT(*), SUM(x) FROM t WHERE id >= 500";
    let serial = with_vec_mode(VecMode::Off, || {
        execute_rows_in_result_order(&engine, "SELECT COUNT(*), SUM(x) FROM t WHERE id >= 500")
    });
    assert_eq!(serial.len(), 1);
    assert_eq!(serial[0][0], Value::Int(1000));
    for mode in [VecMode::On, VecMode::Off] {
        let rows = with_vec_mode(mode, || execute_rows_in_result_order(&engine, sql));
        assert_eq!(rows, serial, "vec mode {mode:?}");
    }
}

/// Empty table: bare aggregates with a PARALLEL hint must return the
/// implicit single group (COUNT(*) → 0, SUM → NULL), never 0 rows.
#[test]
fn parallel_bare_aggregate_empty_table_returns_zero_group() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE empty_t (id INT PRIMARY KEY, x INT)")
        .unwrap();

    for mode in [VecMode::On, VecMode::Off] {
        let rows = with_vec_mode(mode, || {
            execute_rows_in_result_order(
                &engine,
                "SELECT /*+ PARALLEL(4) */ COUNT(*), SUM(x) FROM empty_t",
            )
        });
        assert_eq!(
            rows,
            vec![vec![Value::Int(0), Value::Null]],
            "vec mode {mode:?}"
        );
    }
}

/// Regression repro: COUNT(col) over a NULL-able column and
/// COUNT(*) WHERE col IS NOT NULL must match the serial answer in
/// both vec modes (reported wrong under workers=2 on TPC-C data).
#[test]
fn parallel_count_nullable_column_matches_serial() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE tn2 (id INT PRIMARY KEY, v VARCHAR(200), d DATETIME)")
        .unwrap();
    let payload = "x".repeat(40);
    let n: i64 = 20_000;
    for batch_start in (0..n).step_by(500) {
        let mut sql = String::from("INSERT INTO tn2 VALUES ");
        for id in batch_start..batch_start + 500 {
            if id > batch_start {
                sql.push(',');
            }
            if id % 3 == 0 {
                sql.push_str(&format!("({id}, '{payload}', NULL)"));
            } else {
                sql.push_str(&format!("({id}, '{payload}', '2026-01-01 00:00:00')"));
            }
        }
        engine.execute(&sql).unwrap();
    }
    let expected_not_null = (0..n).filter(|id| id % 3 != 0).count() as i64;

    for mode in [VecMode::On, VecMode::Off] {
        let rows = with_vec_mode(mode, || {
            execute_rows_in_result_order(&engine, "SELECT /*+ PARALLEL(2) */ COUNT(d) FROM tn2")
        });
        assert_eq!(
            rows,
            vec![vec![Value::Int(expected_not_null)]],
            "COUNT(d) vec mode {mode:?}"
        );
        let rows = with_vec_mode(mode, || {
            execute_rows_in_result_order(
                &engine,
                "SELECT /*+ PARALLEL(2) */ COUNT(*) FROM tn2 WHERE d IS NOT NULL",
            )
        });
        assert_eq!(
            rows,
            vec![vec![Value::Int(expected_not_null)]],
            "COUNT(*) WHERE IS NOT NULL vec mode {mode:?}"
        );
    }
}

/// Clustered-NULL variant: whole leading pages are NULL, so some scan
/// batches carry a selection mask (filter rejected rows) while others
/// don't. That mix used to route the implicit single group to two
/// different hash-agg partitions (fast path → partition 0, selection
/// path → fxhash("") partition), and the cross-worker merge emitted TWO
/// result rows whose counts summed to the true answer.
#[test]
fn parallel_count_filtered_clustered_nulls_single_row() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let _lock = auto_parallel_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE tn3 (id INT PRIMARY KEY, v VARCHAR(200), d DATETIME)")
        .unwrap();
    let payload = "x".repeat(40);
    let n: i64 = 20_000;
    for batch_start in (0..n).step_by(500) {
        let mut sql = String::from("INSERT INTO tn3 VALUES ");
        for id in batch_start..batch_start + 500 {
            if id > batch_start {
                sql.push(',');
            }
            if id < n / 3 {
                sql.push_str(&format!("({id}, '{payload}', NULL)"));
            } else {
                sql.push_str(&format!("({id}, '{payload}', '2026-01-01 00:00:00')"));
            }
        }
        engine.execute(&sql).unwrap();
    }
    let expected = n - n / 3;

    for mode in [VecMode::On, VecMode::Off] {
        for sql in [
            "SELECT /*+ PARALLEL(2) */ COUNT(*) FROM tn3 WHERE d IS NOT NULL",
            "SELECT /*+ PARALLEL(2) */ COUNT(d) FROM tn3",
            "SELECT /*+ PARALLEL(2) */ COUNT(*), COUNT(d), SUM(d IS NOT NULL) FROM tn3",
        ] {
            let rows = with_vec_mode(mode, || execute_rows_in_result_order(&engine, sql));
            assert_eq!(
                rows.len(),
                1,
                "{sql} vec mode {mode:?} must emit one row, got {rows:?}"
            );
            assert_eq!(
                rows[0][rows[0].len().min(2) - 1],
                Value::Int(expected),
                "{sql} vec mode {mode:?}"
            );
        }
    }
}

/// Regression (TPC-DS q95 shape): a CTE referenced inside a WHERE `IN`-subquery
/// that JOINs a real table with the CTE, while the outer query is scanned in
/// parallel. The non-decorrelated subquery filter is evaluated *inside parallel
/// worker threads*, which start with an empty thread-local CTE registry — so the
/// worker could not resolve the CTE when re-planning the eval-time subquery and
/// raised 1146 "table 'c' does not exist". Worker threads must inherit the
/// parent's lexical CTE snapshot. The parallel answer must equal the serial
/// oracle (and must not be a silent empty set).
#[test]
fn parallel_scan_resolves_cte_referenced_inside_where_subquery_join() {
    let _tuning = auto_parallel_test_tuning();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE big (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine.execute("CREATE TABLE ret (rk INT)").unwrap();
    // >100 rows so the outer scan of `big` crosses the auto-parallel threshold.
    for batch_start in (0..600).step_by(200) {
        let mut sql = String::from("INSERT INTO big VALUES ");
        for id in batch_start..batch_start + 200 {
            if id > batch_start {
                sql.push(',');
            }
            sql.push_str(&format!("({id}, {})", id % 50));
        }
        engine.execute(&sql).unwrap();
    }
    engine
        .execute("INSERT INTO ret VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10)")
        .unwrap();

    // `c` is a real CTE (scans `big`); the IN-subquery joins the real table
    // `ret` with the CTE `c` — the exact q95 `web_returns, ws_wh` shape.
    let sql = "WITH c AS (SELECT k FROM big WHERE k < 20) \
               SELECT b.id FROM big b \
               WHERE b.k IN (SELECT r.rk FROM ret r, c WHERE r.rk = c.k)";

    // Serial oracle.
    let serial = {
        let _off = EnvGuardSimple::set("MYDB_PARALLEL_DISABLE", "1");
        execute_rows(&engine, sql)
    };
    // Parallel path (auto-parallel tuning + explicit hint).
    let parallel = execute_rows(
        &engine,
        "WITH c AS (SELECT k FROM big WHERE k < 20) \
         SELECT /*+ PARALLEL(4) */ b.id FROM big b \
         WHERE b.k IN (SELECT r.rk FROM ret r, c WHERE r.rk = c.k)",
    );

    assert!(
        !serial.is_empty(),
        "serial oracle must return rows (the subquery resolves to k in 0..=10)"
    );
    assert_eq!(
        parallel, serial,
        "parallel scan must resolve the CTE inside the WHERE-subquery join and \
         match the serial oracle; got {parallel:?} vs {serial:?}"
    );
}

/// Companion to the scan case: a CTE referenced by a multi-table subquery that
/// lands as a parallel HASH-JOIN *residual* (it references both join inputs, so
/// it can't be pushed to either scan, and it's multi-table so it isn't
/// decorrelated). The residual is evaluated in parallel probe workers, which
/// must inherit the parent CTE snapshot or fail with 1146. Parallel must equal
/// the serial oracle.
#[test]
fn parallel_hash_join_residual_resolves_cte_subquery() {
    let _tuning = auto_parallel_test_tuning();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, sk INT, k INT)")
        .unwrap();
    engine.execute("CREATE TABLE s (sk INT, tag INT)").unwrap();
    engine.execute("CREATE TABLE ret (rk INT)").unwrap();
    // >100 rows so the join goes parallel under the tuning.
    for batch_start in (0..600).step_by(200) {
        let mut sql = String::from("INSERT INTO a VALUES ");
        for id in batch_start..batch_start + 200 {
            if id > batch_start {
                sql.push(',');
            }
            sql.push_str(&format!("({id}, {}, {})", id % 8, id % 50));
        }
        engine.execute(&sql).unwrap();
    }
    engine
        .execute("INSERT INTO s VALUES (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7)")
        .unwrap();
    engine
        .execute("INSERT INTO ret VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10)")
        .unwrap();

    // `(a.k + s.sk)` references both join inputs → forced onto the join residual
    // rather than a scan filter; the subquery joins real table `ret` with CTE
    // `c` → not decorrelated → evaluated eval-time inside parallel probe workers.
    let parallel_sql = "WITH c AS (SELECT k FROM a WHERE k < 20) \
         SELECT /*+ PARALLEL(4) */ a.id FROM a JOIN s ON a.sk = s.sk \
         WHERE (a.k + s.sk) IN (SELECT r.rk FROM ret r, c WHERE r.rk = c.k)";
    let serial_sql = "WITH c AS (SELECT k FROM a WHERE k < 20) \
         SELECT a.id FROM a JOIN s ON a.sk = s.sk \
         WHERE (a.k + s.sk) IN (SELECT r.rk FROM ret r, c WHERE r.rk = c.k)";

    let serial = {
        let _off = EnvGuardSimple::set("MYDB_PARALLEL_DISABLE", "1");
        execute_rows(&engine, serial_sql)
    };
    let parallel = execute_rows(&engine, parallel_sql);
    assert!(!serial.is_empty(), "serial oracle must return rows");
    assert_eq!(
        parallel, serial,
        "parallel hash-join residual must resolve the CTE subquery and match \
         serial; got {parallel:?} vs {serial:?}"
    );
}

#[test]
fn parallel_grouped_in_subquery_preserves_having() {
    let _tuning = auto_parallel_test_tuning();
    let _vec = EnvGuardSimple::set("MYDB_VECTORIZED", "on");
    let _cache = EnvGuardSimple::set("MYDB_PLAN_CACHE", "off");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE headers(id INT PRIMARY KEY, price DECIMAL(12,2))")
        .unwrap();
    engine.execute("CREATE TABLE details(id INT PRIMARY KEY, header_id INT, qty DECIMAL(12,2), INDEX header_idx(header_id))").unwrap();
    engine
        .execute("INSERT INTO headers SELECT n, 1000-n FROM generate_series(1,2000) g(n)")
        .unwrap();
    engine.execute("INSERT INTO details SELECT n, MOD(n-1,2000)+1, CASE WHEN MOD(n,100)=0 THEN 160 ELSE 100 END FROM generate_series(1,4000) g(n)").unwrap();
    engine.execute("ANALYZE TABLE headers, details").unwrap();
    let inner = "SELECT header_id FROM details GROUP BY header_id HAVING SUM(qty)>315";
    let expected: Vec<Vec<Value>> = (1..=20).map(|n| vec![Value::Int(n * 100)]).collect();
    assert_eq!(execute_rows(&engine, inner), expected, "standalone HAVING");
    let sql = "SELECT h.id FROM headers h, details d WHERE h.id IN (SELECT header_id FROM details GROUP BY header_id HAVING SUM(qty)>315) AND h.id=d.header_id GROUP BY h.id, h.price ORDER BY h.price DESC LIMIT 10";
    let tree = explain_text(&engine, &format!("EXPLAIN FORMAT=TREE {sql}"));
    assert_eq!(execute_rows(&engine, sql), expected[..10], "{tree}");
}

#[test]
fn aggregate_preflight_rebinds_having_constants_between_executions() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE agg_input (k INT, v DECIMAL(12,2))")
        .unwrap();
    engine
        .execute("INSERT INTO agg_input VALUES (1,1.25),(2,2.50),(3,3.75),(4,4.50),(5,NULL)")
        .unwrap();
    engine.execute("CREATE TABLE agg_cutoff (v INT)").unwrap();
    engine.execute("INSERT INTO agg_cutoff VALUES (0)").unwrap();
    let sql = "SELECT /*+ PARALLEL(2) */ k % 2 AS g, SUM(v) AS s FROM agg_input \
               GROUP BY g HAVING s > (SELECT v FROM agg_cutoff) ORDER BY g";
    for (cutoff, expected_groups) in [(0, vec![0, 1]), (6, vec![0]), (8, vec![]), (0, vec![0, 1])] {
        engine
            .execute(&format!("UPDATE agg_cutoff SET v = {cutoff}"))
            .unwrap();
        let response = engine.execute(sql).unwrap().materialize().unwrap();
        let rows = support::result_rows(response);
        assert_eq!(rows.len(), expected_groups.len(), "cutoff={cutoff}");
        for (row, group) in rows.iter().zip(expected_groups) {
            assert_eq!(
                crate::engine::expr::compare_values(&row[0], &Value::Int(group)).unwrap(),
                std::cmp::Ordering::Equal
            );
            let expected_sum = if group == 0 { 7 } else { 5 };
            assert_eq!(
                crate::engine::expr::compare_values(&row[1], &Value::Int(expected_sum)).unwrap(),
                std::cmp::Ordering::Equal
            );
        }
    }
    engine.execute("INSERT INTO agg_cutoff VALUES (1)").unwrap();
    assert!(engine
        .execute(sql)
        .and_then(QueryResponse::materialize)
        .unwrap_err()
        .is(crate::error::catalog::ER_SUBQUERY_NO_1_ROW));
    engine
        .execute("DELETE FROM agg_cutoff WHERE v = 1")
        .unwrap();
    assert_eq!(
        support::result_rows(engine.execute(sql).unwrap().materialize().unwrap()).len(),
        2
    );
}
