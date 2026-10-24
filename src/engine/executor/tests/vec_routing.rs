//! Phase 7 integration tests — vectorized routing through the legacy
//! SQL surface.
//!
//! Tests use the thread-local `vec::config::with_vec_mode` override so
//! multiple tests can run in parallel without clobbering the process-
//! global env var. Each test also compares the vectorized output
//! against the row-based ground truth: when the bridge routes the
//! operator into the columnar path, the answer must match the legacy
//! path bit-for-bit.

use super::*;
use crate::engine::executor::vec::config::{with_vec_mode, VecMode};

#[test]
fn text_string_projection_uses_compiled_kernels_and_matches_streaming_oracle() {
    use crate::engine::executor::select::vec_bridge::{
        try_native_result_batch_input, VecResultProjection,
    };
    let _guard = test_env_lock();
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE string_projection (id VARCHAR(32) PRIMARY KEY, note LONGTEXT, raw LONGBLOB, dt DATETIME(6), n DECIMAL(12,3), f DOUBLE(8,3), bits BIT(9))").unwrap();
    engine.execute("INSERT INTO string_projection VALUES ('a',NULL,NULL,NULL,NULL,1.250,b'100000001'),('b','','','2026-06-05 10:37:50.390748',12.340,1.250,b'100000001'),('c',REPEAT('中🙂éabc',1024),X'FF00C380','2026-06-05 10:37:50.000001',-12.340,1.250,b'100000001')").unwrap();
    engine.execute("INSERT INTO string_projection SELECT CONCAT('row-',LPAD(n,4,'0')),REPEAT('中🙂éabc',32),X'FF00C380','2026-06-05 10:37:50.390748',12.340,1.250,b'100000001' FROM generate_series(1,300) gs(n)").unwrap();

    for expression in [
        "CRC32(CONCAT_WS(',',id,dt,n,LEFT(note,100),LEFT(raw,3)))",
        "CONCAT_WS(',',id,LEFT(note,0),RIGHT(note,2),SUBSTRING(note,2,4))",
        "CRC32(CONCAT_WS(NULL,id,note,raw))",
        "CRC32(CONCAT_WS('',note,raw))",
        "LOWER(LEFT(note,4))",
    ] {
        let sql = format!("SELECT id,{expression} FROM string_projection WHERE id >= 'a' AND id < 'z' ORDER BY id");
        let crate::parser::Statement::Select(select) =
            crate::parser::parse_statement(&sql).unwrap()
        else {
            unreachable!()
        };
        let planned = crate::planner::plan_select(engine.db_for_tests(), &select).unwrap();
        let txn = engine.db_for_tests().begin_read_only_txn().unwrap();
        {
            let input = with_vec_mode(VecMode::On, || {
                try_native_result_batch_input(
                    engine.db_for_tests(),
                    txn,
                    &planned.physical,
                    &select.projection,
                    &[],
                )
                .unwrap()
                .unwrap()
            });
            assert!(
                input.projection.iter().all(|projection| match projection {
                    VecResultProjection::Column(_) => true,
                    VecResultProjection::Expr(program) => !program.capability().legacy_escape,
                    VecResultProjection::Scalar(_) => false,
                }),
                "string expression fell back to whole-tree interpretation: {expression}"
            );
        }
        engine.db_for_tests().rollback_txn(txn.txn_id).unwrap();
        let streamed = |mode| {
            with_vec_mode(mode, || {
                let mut session = SessionContext::default();
                extract_rows(
                    engine
                        .execute_streaming_with_session_for_test(&mut session, &sql)
                        .unwrap()
                        .materialize()
                        .unwrap(),
                )
            })
        };
        assert_eq!(
            streamed(VecMode::On),
            streamed(VecMode::Off),
            "streaming string projection mismatch: {expression}"
        );
    }

    // TEXT numeric coercions and declared floating/BIT rendering still need
    // their source-aware oracle, even when nested inside string functions.
    for expression in [
        "CRC32(CONCAT_WS(',',note + 0,id))",
        "CONCAT_WS(',',note,f)",
        "CRC32(CONCAT_WS(',',note,bits))",
        "CRC32(LEFT(note,'bad'))",
    ] {
        let sql = format!("SELECT {expression} FROM string_projection ORDER BY id");
        let mut session = SessionContext::default();
        let expected = with_vec_mode(VecMode::Off, || {
            // Materialize through the session helper so deferred worker
            // diagnostics are merged before SHOW WARNINGS. Reading only the
            // dispatcher's early snapshot races the streaming producer.
            let _streaming = crate::engine::executor::select::force_streaming_response_scope();
            extract_rows(engine.execute_with_session(&mut session, &sql).unwrap())
        });
        let expected_warnings = extract_rows(
            engine
                .execute_with_session(&mut session, "SHOW WARNINGS")
                .unwrap(),
        );
        let actual = with_vec_mode(VecMode::On, || {
            let _streaming = crate::engine::executor::select::force_streaming_response_scope();
            extract_rows(engine.execute_with_session(&mut session, &sql).unwrap())
        });
        assert_eq!(actual, expected, "source coercion mismatch: {expression}");
        let actual_warnings = extract_rows(
            engine
                .execute_with_session(&mut session, "SHOW WARNINGS")
                .unwrap(),
        );
        assert_eq!(
            actual_warnings, expected_warnings,
            "source diagnostics mismatch: {expression}"
        );
    }
}

fn check_blocking_scan_predicate_writer_progress(mode: VecMode, partitioned: bool, indexed: bool) {
    use std::sync::{mpsc, Arc};
    use std::time::{Duration, Instant};

    static NEXT_CONNECTION: std::sync::atomic::AtomicU32 =
        std::sync::atomic::AtomicU32::new(987_000);
    let owner_id = NEXT_CONNECTION.fetch_add(3, std::sync::atomic::Ordering::Relaxed);
    let reader_id = owner_id + 1;
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(open_test_engine(dir.path()));
    let partitioning = if partitioned {
        " PARTITION BY HASH(bucket) PARTITIONS 4"
    } else {
        ""
    };
    engine.execute(&format!("CREATE TABLE latch_probe (bucket INT, id INT, v INT, note VARCHAR(32), PRIMARY KEY(bucket,id), KEY by_bucket(bucket,id)){partitioning}")).unwrap();
    engine.execute("INSERT INTO latch_probe SELECT n DIV 128, MOD(n,128), 10, 'old' FROM generate_series(0,1279) gs(n)").unwrap();
    engine.execute("ANALYZE TABLE latch_probe").unwrap();
    let gate = format!("scan_{}", dir.path().file_name().unwrap().to_string_lossy());
    let marker = format!("{gate}_ready");
    let mut owner = SessionContext::default();
    owner.connection_id = owner_id;
    engine
        .execute_with_session(&mut owner, &format!("SELECT GET_LOCK('{gate}',0)"))
        .unwrap();

    let reader_engine = Arc::clone(&engine);
    let hint = if indexed {
        "FORCE INDEX(by_bucket)"
    } else {
        "IGNORE INDEX(PRIMARY,by_bucket)"
    };
    let limit = if indexed { " LIMIT 128" } else { "" };
    let sql = format!("SELECT id,v,note FROM latch_probe {hint} WHERE bucket=0 AND CASE WHEN id=64 THEN CASE WHEN GET_LOCK('{marker}',0)=1 THEN GET_LOCK('{gate}',15) ELSE 0 END ELSE 1 END=1 ORDER BY id{limit}");
    let reader = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = reader_id;
        let result = with_vec_mode(mode, || {
            reader_engine
                .execute_streaming_with_session_for_test(&mut session, &sql)?
                .materialize()
        });
        reader_engine
            .execute_with_session(&mut session, "SELECT RELEASE_ALL_LOCKS()")
            .unwrap();
        result
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    let reached = loop {
        let rows = extract_rows(
            engine
                .execute_with_session(&mut owner, &format!("SELECT IS_USED_LOCK('{marker}')"))
                .unwrap(),
        );
        if rows == vec![vec![Value::UInt(u64::from(reader_id))]] {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    let writer_engine = Arc::clone(&engine);
    let (sent, received) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        let result =
            writer_engine.execute("UPDATE latch_probe SET v=99 WHERE bucket=0 AND id IN (64,127)");
        sent.send(result).unwrap();
    });
    // Always release the SQL gate before asserting, including on the buggy
    // path, so a regression fails without leaving a blocked test thread.
    let early = received.recv_timeout(Duration::from_secs(2));
    engine
        .execute_with_session(&mut owner, &format!("SELECT RELEASE_LOCK('{gate}')"))
        .unwrap();
    let progressed = early.is_ok();
    early
        .unwrap_or_else(|_| received.recv_timeout(Duration::from_secs(5)).unwrap())
        .unwrap();
    writer.join().unwrap();
    let rows = extract_rows(reader.join().unwrap().unwrap());
    assert!(reached, "scan did not reach its row-dependent SQL gate");
    assert!(
        progressed,
        "scan held a page latch while waiting for another transaction"
    );
    assert_eq!(
        rows,
        (0..128)
            .map(|id| vec![Value::Int(id), Value::Int(10), Value::String("old".into())])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        query_rows(
            &engine,
            "SELECT v FROM latch_probe WHERE bucket=0 AND id IN (64,127) ORDER BY id"
        ),
        vec![vec![Value::Int(99)], vec![Value::Int(99)]]
    );
}

#[test]
fn batch_blocking_scan_predicate_does_not_hold_page_latch() {
    check_blocking_scan_predicate_writer_progress(VecMode::On, false, false);
}

#[test]
fn scalar_blocking_scan_predicate_does_not_hold_page_latch() {
    check_blocking_scan_predicate_writer_progress(VecMode::Off, false, false);
}

#[test]
fn partitioned_batch_blocking_scan_predicate_does_not_hold_page_latch() {
    check_blocking_scan_predicate_writer_progress(VecMode::On, true, false);
}

#[test]
fn partitioned_scalar_blocking_scan_predicate_does_not_hold_page_latch() {
    check_blocking_scan_predicate_writer_progress(VecMode::Off, true, false);
}

#[test]
fn indexed_batch_blocking_scan_predicate_does_not_hold_page_latch() {
    check_blocking_scan_predicate_writer_progress(VecMode::On, false, true);
}

#[test]
fn indexed_scalar_blocking_scan_predicate_does_not_hold_page_latch() {
    check_blocking_scan_predicate_writer_progress(VecMode::Off, false, true);
}

struct EnvRestore {
    key: &'static str,
    prior: Option<String>,
}

impl Drop for EnvRestore {
    fn drop(&mut self) {
        match self.prior.take() {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}

fn set_env_for_test(key: &'static str, value: &str) -> EnvRestore {
    let prior = std::env::var(key).ok();
    std::env::set_var(key, value);
    EnvRestore { key, prior }
}

fn extract_rows(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(r) => r.rows,
        QueryResponse::StreamingResultSet(mut r) => {
            let mut out = Vec::new();
            while let Ok(Some(row)) = r.cursor.next_row() {
                out.push(row);
            }
            out
        }
        other => panic!("expected result set, got {other:?}"),
    }
}

fn query_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    extract_rows(engine.execute(sql).unwrap())
}

fn query_result(engine: &StorageQueryEngine, sql: &str) -> ResultSet {
    match engine.execute(sql).unwrap() {
        QueryResponse::ResultSet(result) => result,
        QueryResponse::StreamingResultSet(mut result) => {
            let mut rows = Vec::new();
            while let Some(row) = result.cursor.next_row().unwrap() {
                rows.push(row);
            }
            ResultSet {
                columns: result.columns,
                rows,
            }
        }
        other => panic!("expected result set, got {other:?}"),
    }
}

#[test]
fn build_side_grouped_join_aggregate_uses_match_batches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, j BIGINT, k BIGINT, v BIGINT)")
        .unwrap();
    engine
        .execute("CREATE TABLE u (id BIGINT PRIMARY KEY, j BIGINT, payload BIGINT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO t SELECT n, MOD(n,100), MOD(n,128), n*17 \
             FROM generate_series(0,999) AS gs(n)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO u SELECT n, MOD(n,100), n*3 \
             FROM generate_series(0,999) AS gs(n)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE t").unwrap();
    engine.execute("ANALYZE TABLE u").unwrap();
    let sql = "SELECT t.k, COUNT(*), SUM(u.payload) FROM t INNER JOIN u ON t.j = u.j WHERE t.v > 100 GROUP BY t.k ORDER BY SUM(u.payload) DESC LIMIT 50";
    let expected = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let actual = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(actual, expected);
    let tree = super::explain_analyze_tree(&engine, sql);
    assert!(
        tree.contains("aggregate_mode=join_matches"),
        "adjacent build-grouped HashJoin -> Aggregate produced pair batches:\n{tree}"
    );
}

#[test]
fn simple_select_reaches_protocol_directly_from_batch_operator() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE direct_batch_result (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO direct_batch_result VALUES \
             (1,10),(2,20),(3,30),(4,40),(5,50),(6,60)",
        )
        .unwrap();
    let sql = "SELECT id, v + 1 AS vp FROM direct_batch_result";
    let expected = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let (actual, streamed) = with_vec_mode(VecMode::On, || {
        let response = engine.execute_streaming_for_test(sql).unwrap();
        let streamed = matches!(response, QueryResponse::StreamingResultSet(_));
        (extract_rows(response), streamed)
    });
    let after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(actual, expected);
    assert!(streamed, "simple SELECT unexpectedly forced eager response");
    assert_eq!(
        actual,
        vec![
            vec![Value::Int(1), Value::Int(11)],
            vec![Value::Int(2), Value::Int(21)],
            vec![Value::Int(3), Value::Int(31)],
            vec![Value::Int(4), Value::Int(41)],
            vec![Value::Int(5), Value::Int(51)],
            vec![Value::Int(6), Value::Int(61)],
        ]
    );
    assert!(after > before, "final result unexpectedly crossed PlanRow");
}

#[test]
fn topn_reaches_protocol_as_ordered_batches_without_plan_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE direct_batch_topn (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let mut insert = String::from("INSERT INTO direct_batch_topn VALUES ");
    for id in 0..200i64 {
        if id > 0 {
            insert.push(',');
        }
        insert.push_str(&format!("({id},{})", (id * 7919) % 137));
    }
    engine.execute(&insert).unwrap();
    let sql = "SELECT id, v + 1 FROM direct_batch_topn \
               ORDER BY v + id DESC, id ASC LIMIT 7 OFFSET 11";
    let expected = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));

    let crate::parser::Statement::Select(select) = crate::parser::parse_statement(sql).unwrap()
    else {
        unreachable!()
    };
    let planned = crate::planner::plan_select(engine.db_for_tests(), &select).unwrap();
    assert!(planned.physical.any(|node| matches!(
        node,
        crate::planner::PhysicalPlan::TopK { .. } | crate::planner::PhysicalPlan::TopWindow { .. }
    )));

    let before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let (actual, streamed) = with_vec_mode(VecMode::On, || {
        let response = engine.execute_streaming_for_test(sql).unwrap();
        let streamed = matches!(response, QueryResponse::StreamingResultSet(_));
        (extract_rows(response), streamed)
    });
    let after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(actual, expected);
    assert!(streamed, "blocking Top-N did not retain the batch boundary");
    assert!(after > before, "Top-N output unexpectedly crossed PlanRow");
}

#[test]
fn full_sort_reaches_protocol_as_batches_with_expression_keys() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE direct_batch_sort (\
             id INT PRIMARY KEY, v INT, s VARCHAR(32) COLLATE utf8mb4_0900_ai_ci)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO direct_batch_sort VALUES \
             (1,7,'Zulu'),(2,NULL,'alpha'),(3,4,'ALPHA'),(4,10,'beta'),\
             (5,-2,'Éclair'),(6,1,NULL),(7,7,'zulu')",
        )
        .unwrap();
    let sql = "SELECT id, v, s FROM direct_batch_sort \
               ORDER BY v % 3 ASC, s DESC, id ASC";
    let crate::parser::Statement::Select(select) = crate::parser::parse_statement(sql).unwrap()
    else {
        unreachable!()
    };
    let planned = crate::planner::plan_select(engine.db_for_tests(), &select).unwrap();
    assert!(planned
        .physical
        .any(|node| matches!(node, crate::planner::PhysicalPlan::Sort { .. })));
    assert!(with_vec_mode(VecMode::On, || {
        crate::engine::executor::select::vec_bridge::should_route_result_to_batch(
            &planned.physical,
            &select.projection,
        )
    }));

    let expected = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let (actual, streamed) = with_vec_mode(VecMode::On, || {
        let response = engine.execute_streaming_for_test(sql).unwrap();
        let streamed = matches!(response, QueryResponse::StreamingResultSet(_));
        (extract_rows(response), streamed)
    });
    let after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(actual, expected);
    assert!(streamed, "full Sort left the BatchOperator protocol");
    assert!(after > before, "full Sort rebuilt PlanRows");
}

#[test]
fn distinct_reaches_protocol_as_batches_and_composes_with_limit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE direct_batch_distinct (id INT PRIMARY KEY, k INT, payload INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO direct_batch_distinct VALUES \
             (1,3,30),(2,1,10),(3,3,31),(4,2,20),(5,1,11),(6,4,40)",
        )
        .unwrap();
    let sql = "SELECT DISTINCT k FROM direct_batch_distinct LIMIT 3";
    let expected = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let (actual, streamed) = with_vec_mode(VecMode::On, || {
        let response = engine.execute_streaming_for_test(sql).unwrap();
        let streamed = matches!(response, QueryResponse::StreamingResultSet(_));
        (extract_rows(response), streamed)
    });
    let after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(actual, expected);
    assert_eq!(
        actual,
        vec![
            vec![Value::Int(3)],
            vec![Value::Int(1)],
            vec![Value::Int(2)]
        ]
    );
    assert!(streamed);
    assert!(
        after > before,
        "DISTINCT/Limit crossed the PlanRow boundary"
    );
}

#[test]
fn streaming_distinct_uses_adjacent_batch_keys_across_batch_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE direct_batch_stream_distinct (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO direct_batch_stream_distinct \
             SELECT n, n DIV 625 FROM generate_series(0,4999) AS gs(n)",
        )
        .unwrap();
    let sql = "SELECT DISTINCT k FROM direct_batch_stream_distinct ORDER BY k";
    let crate::parser::Statement::Select(select) = crate::parser::parse_statement(sql).unwrap()
    else {
        unreachable!()
    };
    let planned = crate::planner::plan_select(engine.db_for_tests(), &select).unwrap();
    assert!(planned
        .physical
        .any(|node| matches!(node, crate::planner::PhysicalPlan::StreamingDistinct { .. })));

    let expected = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let (actual, streamed) = with_vec_mode(VecMode::On, || {
        let response = engine.execute_streaming_for_test(sql).unwrap();
        let streamed = matches!(response, QueryResponse::StreamingResultSet(_));
        (extract_rows(response), streamed)
    });
    let after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(actual, expected);
    assert_eq!(
        actual,
        (0..8).map(|key| vec![Value::Int(key)]).collect::<Vec<_>>()
    );
    assert!(streamed);
    assert!(after > before, "StreamingDistinct rebuilt PlanRows");
}

#[test]
fn btree_lookup_range_and_multi_lookup_are_native_batch_sources() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE direct_batch_index (\
             id INT PRIMARY KEY, k INT, payload INT, INDEX idx_k_id (k, id))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO direct_batch_index VALUES \
             (1,1,11),(2,2,20),(3,2,21),(4,3,30),(5,3,31),\
             (6,4,40),(7,5,50),(8,NULL,80)",
        )
        .unwrap();
    // Leave a stale index entry behind: the covering source must still use
    // the heap MVCC header and never expose the deleted row.
    engine
        .execute("DELETE FROM direct_batch_index WHERE id = 3")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE direct_batch_prefix (\
             id INT PRIMARY KEY, s VARCHAR(32), INDEX idx_s (s(2)))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO direct_batch_prefix VALUES \
             (1,'abcd'),(2,'abef'),(3,'zzzz'),(4,NULL)",
        )
        .unwrap();

    for sql in [
        // Covering forward range.
        "SELECT k, id FROM direct_batch_index FORCE INDEX (idx_k_id) \
         WHERE k BETWEEN 2 AND 5 ORDER BY k, id",
        // Non-covering reverse range plus a residual evaluated on the batch.
        "SELECT id, payload FROM direct_batch_index FORCE INDEX (idx_k_id) \
         WHERE k BETWEEN 1 AND 5 AND payload % 2 = 0 ORDER BY k DESC, id DESC",
        // Non-covering point lookup.
        "SELECT id, payload FROM direct_batch_index FORCE INDEX (idx_k_id) \
         WHERE k = 3 ORDER BY id",
        // Covering multi-lookup with runtime-stable key ordering.
        "SELECT k, id FROM direct_batch_index FORCE INDEX (idx_k_id) \
         WHERE k IN (5, 1, 3) ORDER BY k, id",
        // Prefix indexes cannot discharge the full equality predicate. The
        // non-covering batch source must re-evaluate it on heap values.
        "SELECT id, s FROM direct_batch_prefix FORCE INDEX (idx_s) \
         WHERE s = 'abcd' ORDER BY id",
    ] {
        let crate::parser::Statement::Select(select) = crate::parser::parse_statement(sql).unwrap()
        else {
            unreachable!()
        };
        let planned = crate::planner::plan_select(engine.db_for_tests(), &select).unwrap();
        assert!(planned.physical.any(|node| matches!(
            node,
            crate::planner::PhysicalPlan::IndexLookup { .. }
                | crate::planner::PhysicalPlan::IndexMultiLookup { .. }
                | crate::planner::PhysicalPlan::IndexRangeScan { .. }
                | crate::planner::PhysicalPlan::IndexFastScan { .. }
        )));
        assert!(with_vec_mode(VecMode::On, || {
            crate::engine::executor::select::vec_bridge::should_route_result_to_batch(
                &planned.physical,
                &select.projection,
            )
        }));

        let expected = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
        let before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
            .load(std::sync::atomic::Ordering::Relaxed);
        let (actual, streamed) = with_vec_mode(VecMode::On, || {
            let response = engine.execute_streaming_for_test(sql).unwrap();
            let streamed = matches!(response, QueryResponse::StreamingResultSet(_));
            (extract_rows(response), streamed)
        });
        let after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(actual, expected, "native index batch mismatch: {sql}");
        assert!(streamed, "index source left BatchOperator: {sql}");
        assert!(after > before, "index source rebuilt PlanRows: {sql}");
    }
}

#[test]
fn partitioned_btree_fans_out_inside_the_native_batch_source() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE direct_batch_part (id INT, v INT, payload INT, INDEX idx_v(v)) \
             PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN (10), \
             PARTITION p1 VALUES LESS THAN (20), PARTITION pmax VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO direct_batch_part VALUES \
             (1,3,10),(2,1,20),(11,3,30),(12,2,40),(21,4,50),(22,8,60)",
        )
        .unwrap();
    let sql = "SELECT id, payload FROM direct_batch_part FORCE INDEX(idx_v) \
               WHERE v BETWEEN 1 AND 4 ORDER BY id";
    let expected = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let (actual, streamed) = with_vec_mode(VecMode::On, || {
        let response = engine.execute_streaming_for_test(sql).unwrap();
        let streamed = matches!(response, QueryResponse::StreamingResultSet(_));
        (extract_rows(response), streamed)
    });
    let after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(actual, expected);
    assert!(streamed);
    assert!(after > before, "partitioned index scan rebuilt PlanRows");
}

#[test]
fn join_aggregate_matrix_stays_in_one_batch_pipeline_without_shape_switches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE direct_join_l (id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE direct_join_r (id INT, v INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO direct_join_l VALUES \
             (1,1,10),(2,1,20),(3,2,30),(4,2,NULL),(5,3,50)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO direct_join_r VALUES \
             (1,2),(1,4),(2,8),(3,NULL),(3,16),(9,32)",
        )
        .unwrap();

    // A global aggregate is one implicit SQL group. It must consume join
    // match multiplicities directly instead of materialising every joined
    // pair merely because GROUP BY is empty.
    let global_sql = "SELECT /*+ HASH_JOIN(l, r) */ \
                      COUNT(*), COUNT(l.v), COUNT(r.v), \
                      SUM(l.v), SUM(r.v), AVG(r.v), MIN(r.v), MAX(r.v) \
                      FROM direct_join_l l JOIN direct_join_r r ON l.id = r.id";
    let global_expected = with_vec_mode(VecMode::Off, || query_rows(&engine, global_sql));
    let global_actual = with_vec_mode(VecMode::On, || query_rows(&engine, global_sql));
    assert_eq!(global_actual, global_expected);
    let global_explain = with_vec_mode(VecMode::On, || {
        query_rows(
            &engine,
            &format!("EXPLAIN ANALYZE FORMAT=TREE {global_sql}"),
        )
    })
    .into_iter()
    .flatten()
    .map(|value| value.to_display_string())
    .collect::<Vec<_>>()
    .join("\n");
    assert!(
        global_explain.contains("aggregate_mode=join_matches"),
        "global aggregate materialised join rows instead of consuming matches:\n{global_explain}"
    );

    for sql in [
        // Build-only, probe-only and mixed-side aggregate inputs share the
        // same joined pair batches, including one-to-many multiplicity.
        "SELECT l.g, COUNT(*), SUM(r.v), AVG(r.v), MIN(r.v), MAX(r.v), \
                SUM(l.v), SUM(l.v + r.v) \
         FROM direct_join_l l JOIN direct_join_r r ON l.id = r.id \
         GROUP BY l.g",
        // Residual and NULL-extension rows remain in the same pipeline.
        "SELECT /*+ HASH_JOIN(l, r) */ \
                l.g, COUNT(*), COUNT(r.v), SUM(r.v), AVG(r.v), MIN(r.v), MAX(r.v) \
         FROM direct_join_l l LEFT JOIN direct_join_r r \
              ON l.id = r.id AND (r.v IS NULL OR r.v >= 4) \
         GROUP BY l.g",
        // Computed keys are private batch columns consumed by the same hash
        // kernel and projected away before aggregation.
        "SELECT /*+ HASH_JOIN(l, r) */ l.g, COUNT(*), SUM(r.v), AVG(r.v) \
         FROM direct_join_l l JOIN direct_join_r r ON l.id + 1 = r.id \
         GROUP BY l.g",
        // FULL JOIN records accepted residual pairs in a build-match bitmap,
        // then emits both NULL-extension streams through the same batches.
        "SELECT /*+ HASH_JOIN(l, r) */ COALESCE(l.g, -1), COUNT(*), \
                COUNT(l.v), COUNT(r.v), SUM(l.v), SUM(r.v) \
         FROM direct_join_l l FULL OUTER JOIN direct_join_r r \
              ON l.id = r.id AND l.v + r.v >= 10 \
         GROUP BY COALESCE(l.g, -1)",
    ] {
        let crate::parser::Statement::Select(select) = crate::parser::parse_statement(sql).unwrap()
        else {
            unreachable!()
        };
        let planned = crate::planner::plan_select(engine.db_for_tests(), &select).unwrap();
        assert!(
            with_vec_mode(VecMode::On, || {
                crate::engine::executor::select::vec_bridge::should_route_result_to_batch(
                    &planned.physical,
                    &select.projection,
                )
            }),
            "join aggregate failed batch preflight: {sql}\n{:#?}",
            planned.physical,
        );
        let txn = engine.db_for_tests().begin_read_only_txn().unwrap();
        let native = with_vec_mode(VecMode::On, || {
            crate::engine::executor::select::vec_bridge::try_native_result_batch_input(
                engine.db_for_tests(),
                txn,
                &planned.physical,
                &select.projection,
                &[],
            )
            .unwrap()
            .is_some()
        });
        engine.db_for_tests().rollback_txn(txn.txn_id).unwrap();
        assert!(
            native,
            "join aggregate did not compile to native batches: {sql}\n{:#?}",
            planned.physical
        );
        let sort = |mut rows: Vec<Vec<Value>>| {
            rows.sort_by_cached_key(|row| format!("{:?}", row.first()));
            rows
        };
        let expected = sort(with_vec_mode(VecMode::Off, || query_rows(&engine, sql)));
        let before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
            .load(std::sync::atomic::Ordering::Relaxed);
        let (actual, streamed) = with_vec_mode(VecMode::On, || {
            let response = engine.execute_streaming_for_test(sql).unwrap();
            let streamed = matches!(response, QueryResponse::StreamingResultSet(_));
            (sort(extract_rows(response)), streamed)
        });
        let after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(actual, expected, "join aggregate mismatch: {sql}");
        assert!(streamed, "join aggregate left BatchOperator: {sql}");
        assert!(
            after > before,
            "join aggregate rebuilt joined PlanRows: {sql}"
        );
    }

    let extra_values = (0..100)
        .map(|value| format!("(1,{})", value + 100))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO direct_join_r VALUES {extra_values}"))
        .unwrap();
    let direct_sql = "SELECT /*+ HASH_JOIN(l, r) */ l.g, COUNT(r.v), \
                      SUM(r.v), AVG(r.v), MIN(r.v), MAX(r.v) \
                      FROM direct_join_l l LEFT JOIN direct_join_r r ON l.id = r.id \
                      GROUP BY l.g";
    let explain = with_vec_mode(VecMode::On, || {
        query_rows(
            &engine,
            &format!("EXPLAIN ANALYZE FORMAT=TREE {direct_sql}"),
        )
    })
    .into_iter()
    .flatten()
    .map(|value| value.to_display_string())
    .collect::<Vec<_>>()
    .join("\n");
    assert!(
        explain.contains("aggregate_mode=join_matches"),
        "build-preserved join did not feed match indices directly into HashAggregate:\n{explain}"
    );

    // Admission failure is an internal layout change, not an executor
    // fallback: the already-opened join must continue through ordinary
    // spillable pair batches without rescanning either child.
    let sort_groups = |mut rows: Vec<Vec<Value>>| {
        rows.sort_by_cached_key(|row| format!("{:?}", row.first()));
        rows
    };
    let expected = sort_groups(with_vec_mode(VecMode::Off, || {
        query_rows(&engine, direct_sql)
    }));
    let budget = std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::new(5_000));
    let (actual, constrained_explain) = with_vec_mode(VecMode::On, || {
        crate::engine::executor::mem_budget::with_query_budget(
            std::sync::Arc::clone(&budget),
            || {
                let actual = query_rows(&engine, direct_sql);
                let explain = query_rows(
                    &engine,
                    &format!("EXPLAIN ANALYZE FORMAT=TREE {direct_sql}"),
                )
                .into_iter()
                .flatten()
                .map(|value| value.to_display_string())
                .collect::<Vec<_>>()
                .join("\n");
                (actual, explain)
            },
        )
    });
    assert_eq!(sort_groups(actual), expected);
    assert!(
        !constrained_explain.contains("aggregate_mode=join_matches"),
        "over-budget dense join state should stay in the same graph using ordinary pair batches:\n{constrained_explain}"
    );
    assert_eq!(budget.used(), 0, "join/aggregate reservations leaked");
}

#[test]
fn grouped_count_join_uses_exact_match_multiplicity() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE count_build (k INT, v INT NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE TABLE count_probe (k INT, accepted INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO count_build VALUES \
             (1,10),(1,11),(1,12),(1,13),(2,20),(2,21),(2,22)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO count_probe VALUES \
             (1,1),(1,0),(1,1),(2,0),(2,1)",
        )
        .unwrap();
    let sql = "SELECT /*+ HASH_JOIN(b, p) */ b.k, COUNT(p.accepted), \
               COUNT(CASE WHEN b.v>=12 THEN 1 ELSE NULL END) \
               FROM count_build b JOIN count_probe p ON b.k=p.k GROUP BY b.k";
    let mut expected = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    expected.sort_by_key(|row| row[0].to_display_string());
    let before = crate::engine::executor::select::vec_bridge::JOIN_AGG_MULTIPLICITY_ROWS_SAVED
        .load(std::sync::atomic::Ordering::Relaxed);
    let mut actual = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    actual.sort_by_key(|row| row[0].to_display_string());
    assert_eq!(actual, expected);
    let plan = query_rows(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {sql}"))
        .into_iter()
        .flatten()
        .map(|value| value.to_display_string())
        .collect::<Vec<_>>()
        .join("\n");
    let after = crate::engine::executor::select::vec_bridge::JOIN_AGG_MULTIPLICITY_ROWS_SAVED
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        after > before,
        "COUNT aggregate did not consume join multiplicities directly:\n{plan}"
    );

    let decomposable_sql = "SELECT /*+ HASH_JOIN(b, p) */ b.k, \
        SUM(b.v), AVG(b.v), SUM(p.accepted), AVG(p.accepted) \
        FROM count_build b JOIN count_probe p ON b.k=p.k GROUP BY b.k";
    let mut expected = with_vec_mode(VecMode::Off, || query_rows(&engine, decomposable_sql));
    expected.sort_by_key(|row| row[0].to_display_string());
    let mut actual = with_vec_mode(VecMode::On, || query_rows(&engine, decomposable_sql));
    actual.sort_by_key(|row| row[0].to_display_string());
    assert_eq!(actual, expected);
}

#[test]
fn grouped_aggregates_reach_protocol_without_plan_row_caches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE direct_batch_agg (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO direct_batch_agg VALUES \
             (1,1,10),(2,1,20),(3,1,NULL),(4,2,7),(5,2,9),(6,3,NULL)",
        )
        .unwrap();
    let sql = "SELECT k, COUNT(*), COUNT(v), SUM(v * 2), AVG(v), MIN(v), MAX(v) \
               FROM direct_batch_agg GROUP BY k";
    let sort = |mut rows: Vec<Vec<Value>>| {
        rows.sort_by_cached_key(|row| format!("{:?}", row[0]));
        rows
    };
    let expected = sort(with_vec_mode(VecMode::Off, || query_rows(&engine, sql)));
    let before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let (actual, streamed) = with_vec_mode(VecMode::On, || {
        let response = engine.execute_streaming_for_test(sql).unwrap();
        let streamed = matches!(response, QueryResponse::StreamingResultSet(_));
        (sort(extract_rows(response)), streamed)
    });
    let after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(actual, expected);
    assert!(streamed, "HashAggregate forced a PlanRow result set");
    assert!(
        after > before,
        "HashAggregate output crossed PlanRow caches"
    );
}

#[test]
fn global_aggregates_share_the_batch_protocol_including_empty_input() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE direct_global_agg (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO direct_global_agg VALUES (1,10),(2,20),(3,NULL)")
        .unwrap();

    for sql in [
        "SELECT COUNT(*), COUNT(v), SUM(v * 2), AVG(v), MIN(v), MAX(v) \
         FROM direct_global_agg",
        "SELECT COUNT(*), COUNT(v), SUM(v * 2), AVG(v), MIN(v), MAX(v) \
         FROM direct_global_agg WHERE v < 0",
    ] {
        let expected = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
        let before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
            .load(std::sync::atomic::Ordering::Relaxed);
        let (actual, streamed) = with_vec_mode(VecMode::On, || {
            let response = engine.execute_streaming_for_test(sql).unwrap();
            let streamed = matches!(response, QueryResponse::StreamingResultSet(_));
            (extract_rows(response), streamed)
        });
        let after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(actual, expected, "global aggregate mismatch: {sql}");
        assert!(streamed, "global aggregate left the batch protocol: {sql}");
        assert!(
            after > before,
            "global aggregate rebuilt PlanRow state: {sql}"
        );
    }
}

#[test]
fn auto_scalar_aggregate_drains_native_batches_without_stream_worker() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE inline_global_agg (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO inline_global_agg VALUES (1,10),(2,20),(3,NULL)")
        .unwrap();
    let sql = "SELECT COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM inline_global_agg";

    let expected = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let response = with_vec_mode(VecMode::Auto, || engine.execute(sql).unwrap());
    assert!(
        matches!(&response, QueryResponse::ResultSet(_)),
        "auto scalar aggregate should finish on the current blocking worker"
    );
    assert_eq!(extract_rows(response), expected);
    let after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        after > before,
        "scalar aggregate left the native batch graph"
    );

    let forced = with_vec_mode(VecMode::Auto, || {
        engine.execute_streaming_for_test(sql).unwrap()
    });
    assert!(
        matches!(&forced, QueryResponse::StreamingResultSet(_)),
        "explicit streaming scope must retain cursor semantics"
    );
    assert_eq!(extract_rows(forced), expected);
}

#[test]
fn auto_limit_one_finishes_without_top_level_stream_worker() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE inline_limit_one (id INT PRIMARY KEY, g INT, v INT, INDEX(g))")
        .unwrap();
    engine
        .execute("INSERT INTO inline_limit_one VALUES (1,1,30),(2,1,20),(3,1,10),(4,2,40)")
        .unwrap();
    let sql = "SELECT v + 1 AS projected FROM inline_limit_one WHERE g = 1 ORDER BY id LIMIT 1";

    let expected = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let response = with_vec_mode(VecMode::Auto, || engine.execute(sql).unwrap());
    assert!(
        matches!(&response, QueryResponse::ResultSet(_)),
        "a semantic one-row cap should finish on the current blocking worker"
    );
    assert_eq!(extract_rows(response), expected);

    let forced = with_vec_mode(VecMode::Auto, || {
        engine.execute_streaming_for_test(sql).unwrap()
    });
    assert!(
        matches!(&forced, QueryResponse::StreamingResultSet(_)),
        "explicit streaming scope must retain cursor semantics"
    );
    assert_eq!(extract_rows(forced), expected);
}

#[test]
fn hash_aggregate_flows_directly_into_topn_batches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE direct_agg_topn (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO direct_agg_topn VALUES \
             (1,1,10),(2,1,20),(3,2,7),(4,2,9),(5,3,100),(6,4,NULL)",
        )
        .unwrap();
    let sql = "SELECT k, SUM(v * 2), AVG(v), MIN(v), MAX(v) \
               FROM direct_agg_topn GROUP BY k \
               ORDER BY SUM(v * 2) DESC, k ASC LIMIT 3";
    let expected = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let (actual, streamed) = with_vec_mode(VecMode::On, || {
        let response = engine.execute_streaming_for_test(sql).unwrap();
        let streamed = matches!(response, QueryResponse::StreamingResultSet(_));
        (extract_rows(response), streamed)
    });
    let after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(actual, expected);
    assert!(streamed, "HashAggregate→TopN left the batch protocol");
    assert!(after > before, "HashAggregate→TopN rebuilt PlanRow caches");
}

#[test]
fn complex_having_projection_and_order_by_stay_in_the_batch_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE direct_agg_having (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO direct_agg_having VALUES \
             (1,1,10),(2,1,20),(3,1,NULL),(4,2,7),(5,2,9),\
             (6,3,100),(7,4,NULL)",
        )
        .unwrap();

    for sql in [
        "SELECT k, COALESCE(SUM(v), 0) + COUNT(*) AS score, AVG(v) * 2 \
         FROM direct_agg_having GROUP BY k \
         HAVING COALESCE(SUM(v), 0) + COUNT(*) > 10 \
         ORDER BY COALESCE(SUM(v), 0) - COUNT(*) DESC, k ASC LIMIT 3",
        "SELECT COALESCE(SUM(v), 0) + COUNT(*) \
         FROM direct_agg_having WHERE v < 0 HAVING COUNT(*) = 0",
        "SELECT COUNT(*) FROM direct_agg_having WHERE v < 0 \
         HAVING COALESCE(SUM(v), 0) = 0",
        "SELECT COUNT(*) FROM direct_agg_having WHERE v < 0 HAVING SUM(v) IS NOT NULL",
    ] {
        let crate::parser::Statement::Select(select) = crate::parser::parse_statement(sql).unwrap()
        else {
            unreachable!()
        };
        let planned = crate::planner::plan_select(engine.db_for_tests(), &select).unwrap();
        assert!(
            with_vec_mode(VecMode::On, || {
                crate::engine::executor::select::vec_bridge::should_route_result_to_batch(
                    &planned.physical,
                    &select.projection,
                )
            }),
            "complex aggregate failed batch preflight: {sql}\n{:#?}",
            planned.physical,
        );
        let expected = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
        let before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
            .load(std::sync::atomic::Ordering::Relaxed);
        let (actual, streamed) = with_vec_mode(VecMode::On, || {
            let response = engine.execute_streaming_for_test(sql).unwrap();
            let streamed = matches!(response, QueryResponse::StreamingResultSet(_));
            (extract_rows(response), streamed)
        });
        let after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(actual, expected, "batch aggregate mismatch: {sql}");
        assert!(streamed, "aggregate expression left batch protocol: {sql}");
        assert!(
            after > before,
            "HAVING/projection/ORDER BY rebuilt PlanRow state: {sql}"
        );
    }
}

#[test]
fn computed_group_keys_share_compute_columns_and_hash_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE direct_group_expr (\
             id INT PRIMARY KEY, k INT, v INT, d DATE)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO direct_group_expr VALUES \
             (1,1,10,'2023-01-01'),(2,2,20,'2023-06-01'),\
             (3,3,30,'2024-01-01'),(4,4,NULL,'2024-07-01'),\
             (5,NULL,7,'2025-01-01'),(6,7,9,'2025-03-01')",
        )
        .unwrap();

    for sql in [
        "SELECT k % 3 AS bucket, COUNT(*), SUM(v * 2) \
         FROM direct_group_expr GROUP BY k % 3 \
         HAVING COALESCE(SUM(v), 0) >= 7 ORDER BY bucket",
        "SELECT CASE WHEN k IS NULL THEN -1 ELSE k % 2 END AS bucket, \
         COUNT(v), AVG(v) FROM direct_group_expr \
         GROUP BY CASE WHEN k IS NULL THEN -1 ELSE k % 2 END \
         ORDER BY bucket",
        "SELECT YEAR(d) AS y, MIN(v), MAX(v), COUNT(*) \
         FROM direct_group_expr GROUP BY YEAR(d) \
         HAVING COUNT(*) >= 1 ORDER BY YEAR(d) DESC LIMIT 2",
    ] {
        let expected = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
        let crate::parser::Statement::Select(select) = crate::parser::parse_statement(sql).unwrap()
        else {
            unreachable!()
        };
        let planned = crate::planner::plan_select(engine.db_for_tests(), &select).unwrap();
        assert!(planned
            .physical
            .any(|node| matches!(node, crate::planner::PhysicalPlan::HashAggregate { .. })));
        assert!(
            with_vec_mode(VecMode::On, || {
                crate::engine::executor::select::vec_bridge::should_route_result_to_batch(
                    &planned.physical,
                    &select.projection,
                )
            }),
            "computed GROUP BY failed batch preflight: {sql}\n{:#?}",
            planned.physical,
        );

        let before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
            .load(std::sync::atomic::Ordering::Relaxed);
        let (actual, streamed) = with_vec_mode(VecMode::On, || {
            let response = engine.execute_streaming_for_test(sql).unwrap();
            let streamed = matches!(response, QueryResponse::StreamingResultSet(_));
            (extract_rows(response), streamed)
        });
        let after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(actual, expected, "computed GROUP BY mismatch: {sql}");
        assert!(streamed, "computed GROUP BY left batch protocol: {sql}");
        assert!(after > before, "computed GROUP BY rebuilt PlanRows: {sql}");
    }
}

#[test]
fn vec_projection_is_transparent_inside_native_aggregate_subtree() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE projection_vec (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO projection_vec VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    let crate::parser::Statement::Select(select) =
        crate::parser::parse_statement("SELECT id, v FROM projection_vec").unwrap()
    else {
        unreachable!()
    };
    let planned = crate::planner::plan_select(engine.db_for_tests(), &select).unwrap();
    // The optimizer is allowed to remove a redundant SQL Projection, so wrap
    // the planned leaf explicitly to exercise the physical compiler boundary.
    let wrapped = crate::planner::PhysicalPlan::Projection {
        estimated_rows: crate::planner::estimated_rows(&planned.physical),
        cost: crate::engine::cbo::Cost::ZERO,
        projection: vec![crate::parser::SelectItem::Wildcard],
        distinct: false,
        input: Box::new(planned.physical),
    };
    let before = crate::engine::executor::select::vec_bridge::VEC_PROJECTION_INPUT_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let txn = engine
        .db_for_tests()
        .read_only_statement_view(crate::storage::IsolationLevel::ReadCommitted, None);
    let mut input = with_vec_mode(VecMode::On, || {
        crate::engine::executor::select::vec_bridge::try_native_vec_input_stream(
            engine.db_for_tests(),
            txn,
            &wrapped,
        )
        .unwrap()
        .expect("transparent Projection must preserve the native input")
    });
    let mut vector = Vec::new();
    while let Some(batch) = input.stream.next_batch().unwrap() {
        for row in 0..batch.num_rows() {
            vector.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| column.value_at(row))
                    .collect::<Vec<_>>(),
            );
        }
    }
    let after = crate::engine::executor::select::vec_bridge::VEC_PROJECTION_INPUT_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        after > before,
        "native aggregate must compile through the transparent Projection"
    );
    let row = with_vec_mode(VecMode::Off, || {
        query_rows(&engine, "SELECT id, v FROM projection_vec")
    });
    assert_eq!(vector, row);
}

#[test]
fn vec_topn_preserves_unsigned_values_and_matches_row_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE meta_vec (id INT PRIMARY KEY, u BIGINT UNSIGNED, \
             s VARCHAR(12) COLLATE utf8mb4_bin)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO meta_vec VALUES \
             (1, 18446744073709551615, 'A'), \
             (2, 9223372036854775808, 'a'), \
             (3, 7, 'z')",
        )
        .unwrap();
    let sql = "SELECT u AS u_alias, s FROM meta_vec ORDER BY u DESC LIMIT 3";
    let row = with_vec_mode(VecMode::Off, || query_result(&engine, sql));
    let vector = with_vec_mode(VecMode::On, || query_result(&engine, sql));
    assert_eq!(vector.rows, row.rows);
    assert_eq!(vector.columns, row.columns);
    assert_eq!(vector.rows[0][0], Value::UInt(u64::MAX));
}

#[test]
fn vec_topn_json_and_timestamp_match_row_values_and_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE logical_vec (id INT PRIMARY KEY, j JSON, ts TIMESTAMP(6))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO logical_vec VALUES \
             (1, JSON_OBJECT('n', 1), '2038-01-19 03:14:07.123456'), \
             (2, JSON_OBJECT('n', 2), '2024-05-31 12:34:56.000001')",
        )
        .unwrap();
    let sql = "SELECT j, ts FROM logical_vec ORDER BY id LIMIT 2";
    let row = with_vec_mode(VecMode::Off, || query_result(&engine, sql));
    let vector = with_vec_mode(VecMode::On, || query_result(&engine, sql));
    assert_eq!(vector, row);
}

#[test]
fn vec_hash_agg_preserves_exact_result_types_and_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE agg_meta_vec (id INT PRIMARY KEY, k INT, d DECIMAL(8,2), \
             i INT, u BIGINT UNSIGNED)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO agg_meta_vec VALUES \
             (1, 1, 1.25, 3, 9223372036854775808), \
             (2, 1, 2.50, 7, 1), (3, 2, 9.75, 5, 4)",
        )
        .unwrap();
    let sql = "SELECT k, SUM(d) AS sd, AVG(i) AS ai, BIT_OR(u) AS bu \
               FROM agg_meta_vec GROUP BY k ORDER BY k";
    let row = with_vec_mode(VecMode::Off, || query_result(&engine, sql));
    let vector = with_vec_mode(VecMode::On, || query_result(&engine, sql));
    assert_eq!(vector.rows, row.rows);
    assert_eq!(vector.columns, row.columns);
    assert!(matches!(
        vector.columns[1].column_type,
        ColumnType::Decimal { scale: 2, .. }
    ));
    assert!(matches!(
        vector.columns[2].column_type,
        ColumnType::Decimal { .. }
    ));
}

#[test]
fn vec_topn_matches_legacy_for_order_by_limit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let mut vals = String::from("INSERT INTO t VALUES ");
    for i in 0..200i64 {
        if i > 0 {
            vals.push_str(", ");
        }
        // Values intentionally not in order; some duplicates.
        let v = (i * 7919) % 137;
        vals.push_str(&format!("({i}, {v})"));
    }
    engine.execute(&vals).unwrap();

    let legacy = query_rows(
        &engine,
        "SELECT id, v FROM t ORDER BY v ASC, id ASC LIMIT 10",
    );
    let vec_rows = with_vec_mode(VecMode::On, || {
        query_rows(
            &engine,
            "SELECT id, v FROM t ORDER BY v ASC, id ASC LIMIT 10",
        )
    });
    assert_eq!(legacy, vec_rows, "vec Top-N must equal legacy sort+limit");
    assert_eq!(legacy.len(), 10);
}

#[test]
fn vec_topn_offset_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let mut vals = String::from("INSERT INTO t VALUES ");
    for i in 0..100i64 {
        if i > 0 {
            vals.push_str(", ");
        }
        vals.push_str(&format!("({i}, {})", 100 - i));
    }
    engine.execute(&vals).unwrap();

    let legacy = query_rows(&engine, "SELECT id FROM t ORDER BY v ASC LIMIT 5 OFFSET 10");
    let vec_rows = with_vec_mode(VecMode::On, || {
        query_rows(&engine, "SELECT id FROM t ORDER BY v ASC LIMIT 5 OFFSET 10")
    });
    assert_eq!(legacy, vec_rows);
}

#[test]
fn vec_topn_desc_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 30), (2, 10), (3, 50), (4, 40), (5, 20)")
        .unwrap();

    let legacy = query_rows(&engine, "SELECT id FROM t ORDER BY v DESC LIMIT 3");
    let vec_rows = with_vec_mode(VecMode::On, || {
        query_rows(&engine, "SELECT id FROM t ORDER BY v DESC LIMIT 3")
    });
    assert_eq!(legacy, vec_rows);
}

#[test]
fn vec_topn_with_nulls_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 5), (2, NULL), (3, 1), (4, NULL), (5, 3)")
        .unwrap();

    let legacy = query_rows(&engine, "SELECT id FROM t ORDER BY v ASC, id ASC LIMIT 4");
    let vec_rows = with_vec_mode(VecMode::On, || {
        query_rows(&engine, "SELECT id FROM t ORDER BY v ASC, id ASC LIMIT 4")
    });
    assert_eq!(legacy, vec_rows);
}

#[test]
fn vec_hash_agg_count_star_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT)")
        .unwrap();
    let mut vals = String::from("INSERT INTO t VALUES ");
    for i in 0..500i64 {
        if i > 0 {
            vals.push_str(", ");
        }
        vals.push_str(&format!("({i}, {})", i % 7));
    }
    engine.execute(&vals).unwrap();

    fn sort_rows(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
        rows.sort_by(|a, b| {
            let ai = match &a[0] {
                Value::Int(x) => *x,
                _ => panic!(),
            };
            let bi = match &b[0] {
                Value::Int(x) => *x,
                _ => panic!(),
            };
            ai.cmp(&bi)
        });
        rows
    }

    let legacy = sort_rows(query_rows(&engine, "SELECT k, COUNT(*) FROM t GROUP BY k"));
    let vec_rows = sort_rows(with_vec_mode(VecMode::On, || {
        query_rows(&engine, "SELECT k, COUNT(*) FROM t GROUP BY k")
    }));
    assert_eq!(legacy, vec_rows);
    assert_eq!(legacy.len(), 7);
}

#[test]
fn vec_hash_agg_sum_min_max_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    let mut vals = String::from("INSERT INTO t VALUES ");
    for i in 0..300i64 {
        if i > 0 {
            vals.push_str(", ");
        }
        vals.push_str(&format!("({i}, {}, {})", i % 5, i * 3));
    }
    engine.execute(&vals).unwrap();

    fn sort_rows(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
        rows.sort_by(|a, b| {
            let ai = match &a[0] {
                Value::Int(x) => *x,
                _ => panic!(),
            };
            let bi = match &b[0] {
                Value::Int(x) => *x,
                _ => panic!(),
            };
            ai.cmp(&bi)
        });
        rows
    }

    let sql = "SELECT k, SUM(v), MIN(v), MAX(v) FROM t GROUP BY k";
    let legacy = sort_rows(query_rows(&engine, sql));
    let vec_rows = sort_rows(with_vec_mode(VecMode::On, || query_rows(&engine, sql)));
    assert_eq!(legacy, vec_rows);
}

#[test]
fn vec_topn_over_hash_agg_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    let mut vals = String::from("INSERT INTO t VALUES ");
    for i in 0..300i64 {
        if i > 0 {
            vals.push_str(", ");
        }
        vals.push_str(&format!("({i}, {}, {})", i % 7, i * 3));
    }
    engine.execute(&vals).unwrap();

    let sql = "SELECT k, SUM(v) AS total FROM t GROUP BY k ORDER BY k DESC LIMIT 3";
    let legacy = query_rows(&engine, sql);
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows, "vec TopN-over-HashAgg must equal legacy");
}

/// Regression: TPC-H Q18 shape — parallel GROUP BY with an aggregate in
/// the projection, ORDER BY + LIMIT on top. The `ParallelHashAggregate`
/// node is not matched by the fused vec TopN-over-HashAggregate path, so
/// the generic vec TopN must NOT round-trip its aggregate rows through a
/// `Batch` (that drops `agg_values`, leaving `SUM(...)` unresolvable and
/// raising "invalid use of group function").
#[test]
fn vec_topn_over_parallel_hash_agg_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE customer (c_custkey INT PRIMARY KEY, c_name VARCHAR(32))")
        .unwrap();
    engine
        .execute("CREATE TABLE orders (o_orderkey INT PRIMARY KEY, o_custkey INT, o_totalprice INT, o_orderdate INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE lineitem (l_id INT PRIMARY KEY, l_orderkey INT, l_quantity INT)")
        .unwrap();
    for c in 0..20i64 {
        engine
            .execute(&format!("INSERT INTO customer VALUES ({c}, 'cust{c}')"))
            .unwrap();
    }
    for o in 0..60i64 {
        engine
            .execute(&format!(
                "INSERT INTO orders VALUES ({o}, {}, {}, {})",
                o % 20,
                1000 + o,
                o * 7
            ))
            .unwrap();
    }
    let mut li = 0i64;
    for o in 0..60i64 {
        for _ in 0..3 {
            engine
                .execute(&format!(
                    "INSERT INTO lineitem VALUES ({li}, {o}, {})",
                    (li % 13) + 1
                ))
                .unwrap();
            li += 1;
        }
    }

    let sql = "SELECT /*+ PARALLEL(4) */ c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) AS total_qty \
               FROM customer INNER JOIN orders ON c_custkey = o_custkey \
               INNER JOIN lineitem ON o_orderkey = l_orderkey \
               WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 5) \
               GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice \
               ORDER BY o_totalprice DESC, o_orderdate LIMIT 10";
    let legacy = query_rows(&engine, sql);
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows, "vec Q18 shape must equal legacy");
}

#[test]
fn parallel_count_topk_pushdown_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE count_topk (\
                id INT PRIMARY KEY, user_id BIGINT NOT NULL, \
                event_time TIMESTAMP NOT NULL, phrase VARCHAR(32) NOT NULL)",
        )
        .unwrap();
    let mut values = String::from("INSERT INTO count_topk VALUES ");
    for id in 0..800i64 {
        if id > 0 {
            values.push_str(", ");
        }
        let (user_id, minute, phrase) = if id < 500 {
            (7, 31, "hot")
        } else {
            (id, id % 60, "cold")
        };
        values.push_str(&format!(
            "({id}, {user_id}, '2024-05-31 12:{minute:02}:00', '{phrase}')"
        ));
    }
    engine.execute(&values).unwrap();
    let sql = "SELECT /*+ PARALLEL(4) */ user_id, \
               EXTRACT(MINUTE FROM event_time) AS m, phrase, COUNT(*) \
               FROM count_topk GROUP BY user_id, m, phrase \
               ORDER BY COUNT(*) DESC LIMIT 1";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let before = super::super::select::vec_bridge::AGG_COUNT_TOPK_PUSHDOWN_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let vector = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    let after = super::super::select::vec_bridge::AGG_COUNT_TOPK_PUSHDOWN_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(vector, legacy);
    assert_eq!(vector[0].last(), Some(&Value::Int(500)));
    assert!(after > before, "parallel COUNT Top-K pushdown did not run");
}

#[test]
fn count_topk_raw_probe_preserves_case_sensitive_groups() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE cs_topk (id INT PRIMARY KEY, category VARCHAR(64) COLLATE utf8mb4_0900_as_cs NOT NULL)").unwrap();
    let mut insert = String::from("INSERT INTO cs_topk VALUES ");
    for id in 0..2000 {
        if id > 0 {
            insert.push(',');
        }
        let category = match id {
            0..700 => "Alpha".to_string(),
            700..1200 => "alpha".to_string(),
            1200..1500 => "Álpha".to_string(),
            _ => format!("cold-{id}"),
        };
        insert.push_str(&format!("({id},'{category}')"));
    }
    engine.execute(&insert).unwrap();
    engine.execute("ANALYZE TABLE cs_topk").unwrap();
    let mut oracle = with_vec_mode(VecMode::Off, || {
        query_rows(
            &engine,
            "SELECT category, COUNT(*) FROM cs_topk GROUP BY category",
        )
    });
    oracle.sort_by_key(|row| match row[1] {
        Value::Int(count) => std::cmp::Reverse(count),
        _ => panic!(),
    });
    oracle.truncate(3);
    for _ in 0..3 {
        let actual = with_vec_mode(VecMode::On, || {
            query_rows(&engine, "SELECT /*+ PARALLEL(4) */ category, COUNT(*) AS c FROM cs_topk GROUP BY category ORDER BY c DESC LIMIT 3")
        });
        assert_eq!(actual, oracle);
    }
}

#[test]
fn stats_proven_count_topk_is_exact_for_string_and_integer_keys() {
    let dir = tempfile::tempdir().unwrap();
    // Keep fixture population out of buffer-pressure waits; query/spill budgets stay unchanged.
    let engine = open_test_engine_with_buffer_pool_pages(dir.path(), 2048);
    engine
        .execute("CREATE TABLE string_topk (id INT PRIMARY KEY, category VARCHAR(64) NOT NULL)")
        .unwrap();
    let mut values = String::from("INSERT INTO string_topk VALUES ");
    for id in 0..2_000i64 {
        if id > 0 {
            values.push_str(", ");
        }
        let category = match id {
            0..700 => "Alpha",
            700..1_200 => "beta",
            1_200..1_500 => "GAMMA",
            _ => "cold",
        };
        let category = if category == "cold" {
            format!("cold-{id}")
        } else {
            category.to_string()
        };
        values.push_str(&format!("({id}, '{category}')"));
    }
    engine.execute(&values).unwrap();
    engine.execute("ANALYZE TABLE string_topk").unwrap();
    let sql = "SELECT /*+ PARALLEL(4) */ category, COUNT(*) AS c FROM string_topk \
               GROUP BY category ORDER BY c DESC LIMIT 3";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let before = super::super::select::vec_bridge::STATS_PROVEN_COUNT_TOPK_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let vector = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    let after = super::super::select::vec_bridge::STATS_PROVEN_COUNT_TOPK_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(vector, legacy);
    assert_eq!(
        vector.iter().map(|row| row[1].clone()).collect::<Vec<_>>(),
        vec![Value::Int(700), Value::Int(500), Value::Int(300)]
    );
    assert!(after > before, "stats-proven COUNT Top-K did not run");

    let distinct_sql = "SELECT /*+ PARALLEL(4) */ category, COUNT(DISTINCT id) AS c \
                        FROM string_topk GROUP BY category ORDER BY c DESC LIMIT 3";
    let distinct_legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, distinct_sql));
    let distinct_before = super::super::select::vec_bridge::STATS_PROVEN_COUNT_TOPK_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let distinct_vector = with_vec_mode(VecMode::On, || query_rows(&engine, distinct_sql));
    let distinct_after = super::super::select::vec_bridge::STATS_PROVEN_COUNT_TOPK_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(distinct_vector, distinct_legacy);
    assert_eq!(
        distinct_vector
            .iter()
            .map(|row| row[1].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(700), Value::Int(500), Value::Int(300)]
    );
    assert!(
        distinct_after > distinct_before,
        "stats-proven COUNT(DISTINCT integer) Top-K did not run"
    );

    // The fourth place is tied with hundreds of omitted one-row groups, so a
    // strict proof is impossible. The accelerator must reopen the ordinary
    // exact HashAggregate rather than treating the sketch as an answer.
    let fallback_before = super::super::select::vec_bridge::STATS_PROVEN_COUNT_TOPK_FALLBACKS
        .load(std::sync::atomic::Ordering::Relaxed);
    let fallback_rows = with_vec_mode(VecMode::On, || {
        query_rows(
            &engine,
            "SELECT /*+ PARALLEL(4) */ category, COUNT(*) AS c FROM string_topk \
             GROUP BY category ORDER BY c DESC LIMIT 4",
        )
    });
    let fallback_after = super::super::select::vec_bridge::STATS_PROVEN_COUNT_TOPK_FALLBACKS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        fallback_rows
            .iter()
            .take(3)
            .map(|row| row[1].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(700), Value::Int(500), Value::Int(300)],
        "fallback rows: {fallback_rows:?}"
    );
    assert!(
        fallback_after > fallback_before,
        "unproven COUNT Top-K did not use exact fallback"
    );

    engine
        .execute(
            "CREATE TABLE integer_topk (\
                id INT NOT NULL, user_id BIGINT NOT NULL, \
                PRIMARY KEY (id, user_id))",
        )
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.integer_topk",
        (0..70_000i64).map(|n| {
            let key = match n {
                0..20_000 => 9_000_000_000_000_000_001,
                20_000..35_000 => 9_000_000_000_000_000_002,
                35_000..45_000 => 9_000_000_000_000_000_003,
                _ => n,
            };
            vec![Value::Int(n), Value::Int(key)]
        }),
    );
    engine.execute("ANALYZE TABLE integer_topk").unwrap();
    let sql = "SELECT /*+ PARALLEL(4) */ user_id, COUNT(*) AS c \
               FROM integer_topk FORCE INDEX (PRIMARY) \
               GROUP BY user_id ORDER BY c DESC LIMIT 3";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let before = super::super::select::vec_bridge::STATS_PROVEN_COUNT_TOPK_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let vector = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    let after = super::super::select::vec_bridge::STATS_PROVEN_COUNT_TOPK_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(vector, legacy);
    assert_eq!(
        vector.iter().map(|row| row[1].clone()).collect::<Vec<_>>(),
        vec![Value::Int(20000), Value::Int(15000), Value::Int(10000)]
    );
    assert!(
        after > before,
        "stats-proven integer COUNT Top-K did not run"
    );
}
/// A rejected integer proof must still pass through the ordering operator.
#[test]
fn integer_count_topk_failed_proof_preserves_order_and_offset() {
    let dir = tempfile::tempdir().unwrap();
    // Keep fixture population out of buffer-pressure waits; query/spill budgets stay unchanged.
    let engine = open_test_engine_with_buffer_pool_pages(dir.path(), 2048);
    engine
        .execute("CREATE TABLE sparse_topk (id INT PRIMARY KEY, x INT)")
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.sparse_topk",
        (0..70_000i64).map(|n| {
            let key = match n {
                0..10 => -1,
                10..19 => -2,
                19..27 => -3,
                _ => n,
            };
            vec![Value::Int(n), Value::Int(key)]
        }),
    );
    engine.execute("ANALYZE TABLE sparse_topk").unwrap();
    for suffix in ["LIMIT 3", "LIMIT 2 OFFSET 1"] {
        let sql = format!("SELECT /*+ PARALLEL(4) */ x, x - 1, COUNT(*) AS c FROM sparse_topk GROUP BY x, x - 1 ORDER BY c DESC {suffix}");
        let oracle_sql = sql.replace("ORDER BY c DESC", "HAVING COUNT(*) > 0 ORDER BY c DESC");
        let oracle = with_vec_mode(VecMode::Off, || query_rows(&engine, &oracle_sql));
        assert_eq!(
            oracle[0][2],
            Value::Int(if suffix == "LIMIT 3" { 10 } else { 9 })
        );
        let actual = with_vec_mode(VecMode::On, || query_rows(&engine, &sql));
        assert_eq!(actual, oracle);
    }
    // A one-row fourth place ties omitted groups and cannot be proven.
    let before = super::super::select::vec_bridge::STATS_PROVEN_COUNT_TOPK_FALLBACKS
        .load(std::sync::atomic::Ordering::Relaxed);
    let actual = with_vec_mode(VecMode::On, || {
        query_rows(&engine,
        "SELECT /*+ PARALLEL(4) */ x, COUNT(*) AS c FROM sparse_topk GROUP BY x ORDER BY c DESC LIMIT 4")
    });
    assert_eq!(
        actual.iter().map(|row| row[1].clone()).collect::<Vec<_>>(),
        vec![Value::Int(10), Value::Int(9), Value::Int(8), Value::Int(1)]
    );
    assert!(
        super::super::select::vec_bridge::STATS_PROVEN_COUNT_TOPK_FALLBACKS
            .load(std::sync::atomic::Ordering::Relaxed)
            > before
    );
}
#[test]
fn integer_count_topk_stale_stats_null_unsigned_and_budget_fallback() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    // Keep fixture population out of buffer-pressure waits; query/spill budgets stay unchanged.
    let engine = open_test_engine_with_buffer_pool_pages(dir.path(), 2048);
    engine
        .execute("CREATE TABLE stale_integer_topk (id INT PRIMARY KEY, x BIGINT UNSIGNED)")
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.stale_integer_topk",
        (0..70_000i64).map(|n| vec![Value::Int(n), Value::UInt(n as u64)]),
    );
    engine.execute("ANALYZE TABLE stale_integer_topk").unwrap();
    engine.execute("INSERT INTO stale_integer_topk SELECT n, CASE WHEN n < 73000 THEN 18446744073709551615 ELSE NULL END FROM generate_series(70000, 74999) AS gs(n)").unwrap();
    let sql = "SELECT /*+ PARALLEL(4) */ x, COUNT(*) AS c FROM stale_integer_topk GROUP BY x ORDER BY c DESC LIMIT 2";
    let expected = vec![
        vec![Value::UInt(u64::MAX), Value::Int(3000)],
        vec![Value::Null, Value::Int(2000)],
    ];
    let oracle = with_vec_mode(VecMode::Off, || {
        query_rows(
            &engine,
            &sql.replace("ORDER BY", "HAVING COUNT(*) > 0 ORDER BY"),
        )
    });
    assert_eq!(oracle, expected);
    // Exercise refusal, proof without optional discovery, and full proof.
    // All paths return exact unsigned/NULL groups and release reservations.
    for mib in [32, 256, 512] {
        let budget = Arc::new(MemBudget::new(mib * 1024 * 1024));
        let actual = with_query_budget(budget.clone(), || {
            with_vec_mode(VecMode::On, || query_rows(&engine, sql))
        });
        assert_eq!(actual, expected, "budget={mib} MiB");
        assert_eq!(budget.used(), 0);
    }
}
#[test]
fn stats_proven_count_topk_handles_filtered_composite_keys() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE composite_topk (\
                id INT PRIMARY KEY, region SMALLINT NOT NULL, \
                category VARCHAR(64) NOT NULL, event_time DATETIME NOT NULL)",
        )
        .unwrap();
    let mut values = String::from("INSERT INTO composite_topk VALUES ");
    for id in 0..2_000i64 {
        if id > 0 {
            values.push_str(", ");
        }
        let (region, category) = match id {
            0..700 => (1, "alpha".to_string()),
            700..1_200 => (2, "beta".to_string()),
            1_200..1_500 => (1, "gamma".to_string()),
            _ => (id % 4, format!("cold-{id}")),
        };
        let minute = match id {
            0..700 => 1,
            700..1_200 => 2,
            1_200..1_500 => 3,
            _ => id % 60,
        };
        values.push_str(&format!(
            "({id}, {region}, '{category}', '2024-01-01 00:{minute:02}:00')"
        ));
    }
    engine.execute(&values).unwrap();
    engine.execute("ANALYZE TABLE composite_topk").unwrap();
    let sql = "SELECT /*+ PARALLEL(4) */ region, category, COUNT(*) AS c \
               FROM composite_topk WHERE category <> '' \
               GROUP BY region, category ORDER BY c DESC LIMIT 3";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let before = super::super::select::vec_bridge::STATS_PROVEN_COUNT_TOPK_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let vector = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    let after = super::super::select::vec_bridge::STATS_PROVEN_COUNT_TOPK_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(vector, legacy);
    assert_eq!(
        vector.iter().map(|row| row[2].clone()).collect::<Vec<_>>(),
        vec![Value::Int(700), Value::Int(500), Value::Int(300)]
    );
    assert!(
        after > before,
        "filtered composite COUNT Top-K proof did not run"
    );

    let computed_sql =
        "SELECT /*+ PARALLEL(4) */ EXTRACT(MINUTE FROM event_time) AS m, category, COUNT(*) AS c \
                        FROM composite_topk WHERE category <> '' \
                        GROUP BY m, category ORDER BY c DESC LIMIT 3";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, computed_sql));
    let before = super::super::select::vec_bridge::STATS_PROVEN_COUNT_TOPK_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let vector = with_vec_mode(VecMode::On, || query_rows(&engine, computed_sql));
    let after = super::super::select::vec_bridge::STATS_PROVEN_COUNT_TOPK_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(vector, legacy);
    assert_eq!(
        vector.iter().map(|row| row[2].clone()).collect::<Vec<_>>(),
        vec![Value::Int(700), Value::Int(500), Value::Int(300)]
    );
    assert!(
        after > before,
        "stats-proven computed-key COUNT Top-K did not run"
    );
}

/// Regression for the A optimization: vec parallel hash aggregate must
/// support HAVING — previously `having.is_some()` short-circuited to the
/// row-based fused path, which is the dominant cost in TPC-H Q18's
/// subquery (1.5M groups). Three HAVING shapes are exercised:
///   1. HAVING references an aggregate that's also in SELECT
///   2. HAVING references an aggregate that's only in HAVING (Q18 shape)
///   3. HAVING combines a GROUP BY column with an aggregate
/// All must match the row-based ground truth bit-for-bit.
#[test]
fn vec_parallel_hash_agg_with_having_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    // ~600 rows over 30 distinct group keys so the parallel path can
    // actually partition pages; the q18-shape subquery exercises the
    // "HAVING-only aggregate" branch.
    let mut vals = String::from("INSERT INTO t VALUES ");
    for i in 0..600i64 {
        if i > 0 {
            vals.push_str(", ");
        }
        vals.push_str(&format!("({i}, {}, {})", i % 30, (i * 7) % 17));
    }
    engine.execute(&vals).unwrap();

    fn sort_rows(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
        rows.sort_by(|a, b| match (&a[0], &b[0]) {
            (Value::Int(x), Value::Int(y)) => x.cmp(y),
            _ => std::cmp::Ordering::Equal,
        });
        rows
    }

    for sql in [
        // (1) Aggregate appears in both SELECT and HAVING.
        "SELECT /*+ PARALLEL(4) */ k, SUM(v) FROM t GROUP BY k HAVING SUM(v) > 100",
        // (2) HAVING-only aggregate — q18 subquery shape (SUM not in SELECT).
        "SELECT /*+ PARALLEL(4) */ k FROM t GROUP BY k HAVING SUM(v) > 100",
        // (3) HAVING with multiple aggregates (MIN/MAX share the cache).
        "SELECT /*+ PARALLEL(4) */ k FROM t GROUP BY k HAVING MIN(v) = 0",
        // (4) HAVING references a GROUP BY column.
        "SELECT /*+ PARALLEL(4) */ k, COUNT(*) FROM t GROUP BY k HAVING k > 10",
    ] {
        let legacy = sort_rows(query_rows(&engine, sql));
        let vec_rows = sort_rows(with_vec_mode(VecMode::On, || query_rows(&engine, sql)));
        assert_eq!(legacy, vec_rows, "vec HAVING result mismatch for: {sql}");
    }
}

#[test]
fn vec_hash_agg_sum_mixed_numeric_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, v VARCHAR(16))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 1, '1'), (2, 1, '2.5'), (3, 2, '3')")
        .unwrap();

    fn sort_rows(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
        rows.sort_by(|a, b| match (&a[0], &b[0]) {
            (Value::Int(x), Value::Int(y)) => x.cmp(y),
            _ => std::cmp::Ordering::Equal,
        });
        rows
    }

    let sql = "SELECT k, SUM(v), AVG(v) FROM t GROUP BY k";
    let legacy = sort_rows(query_rows(&engine, sql));
    let vec_rows = sort_rows(with_vec_mode(VecMode::On, || query_rows(&engine, sql)));
    assert_eq!(legacy, vec_rows);
}

#[test]
fn vec_distinct_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT)")
        .unwrap();
    let mut vals = String::from("INSERT INTO t VALUES ");
    for i in 0..200i64 {
        if i > 0 {
            vals.push_str(", ");
        }
        vals.push_str(&format!("({i}, {})", i % 11));
    }
    engine.execute(&vals).unwrap();

    fn sort_rows(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
        rows.sort_by(|a, b| {
            let ai = match &a[0] {
                Value::Int(x) => *x,
                _ => panic!(),
            };
            let bi = match &b[0] {
                Value::Int(x) => *x,
                _ => panic!(),
            };
            ai.cmp(&bi)
        });
        rows
    }

    let legacy = sort_rows(query_rows(&engine, "SELECT DISTINCT k FROM t"));
    let vec_rows = sort_rows(with_vec_mode(VecMode::On, || {
        query_rows(&engine, "SELECT DISTINCT k FROM t")
    }));
    assert_eq!(legacy, vec_rows);
    assert_eq!(legacy.len(), 11);
}

#[test]
fn bounded_unique_inner_join_skips_hash_state_and_preserves_residual() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE bounded_l (id INT PRIMARY KEY, payload INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE bounded_r (id INT PRIMARY KEY, payload INT)")
        .unwrap();
    engine
        .execute("INSERT INTO bounded_l VALUES (1, 10), (2, 30)")
        .unwrap();
    engine
        .execute("INSERT INTO bounded_r VALUES (1, 20), (2, 25)")
        .unwrap();

    let sql = "SELECT /*+ HASH_JOIN(l, r) */ l.payload, r.payload \
               FROM bounded_l l JOIN bounded_r r ON l.id = r.id \
               WHERE l.id = 1 AND r.id = 1 AND l.payload < r.payload";
    let crate::parser::Statement::Select(select) = crate::parser::parse_statement(sql).unwrap()
    else {
        unreachable!()
    };
    let planned = crate::planner::plan_select(engine.db_for_tests(), &select).unwrap();
    assert!(planned
        .physical
        .any(|node| matches!(node, crate::planner::PhysicalPlan::HashJoin { .. })));

    let before = super::plan::BOUNDED_INNER_JOIN_HITS.load(std::sync::atomic::Ordering::Relaxed);
    let rows = query_rows(&engine, sql);
    let after = super::plan::BOUNDED_INNER_JOIN_HITS.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(rows, vec![vec![Value::Int(10), Value::Int(20)]]);
    assert!(after > before, "bounded unique join kernel did not run");

    let no_match = query_rows(
        &engine,
        "SELECT /*+ HASH_JOIN(l, r) */ l.payload, r.payload \
         FROM bounded_l l JOIN bounded_r r ON l.id = r.id \
         WHERE l.id = 2 AND r.id = 2 AND l.payload < r.payload",
    );
    assert!(no_match.is_empty(), "residual predicate was not preserved");
}

#[test]
fn vec_hash_join_matches_legacy_for_inner_equi_join() {
    let _guard = super::test_env_lock();
    let _restore = set_env_for_test("MYDB_VEC_HASH_JOIN", "1");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE customer (id INT PRIMARY KEY, nation INT, name VARCHAR(16))")
        .unwrap();
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, cust_id INT, amount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO customer VALUES (1, 10, 'alice'), (2, 20, 'bob'), (3, 10, 'cara')")
        .unwrap();
    engine
        .execute(
            "INSERT INTO orders VALUES \
             (100, 1, 50), (101, 1, 70), (102, 3, 20), (103, 99, 999)",
        )
        .unwrap();

    let sql = "SELECT /*+ HASH_JOIN(c, o) */ c.name, o.id, o.amount \
               FROM customer c JOIN orders o ON c.id = o.cust_id \
               ORDER BY o.id";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows);
    assert_eq!(
        vec_rows,
        vec![
            vec![
                Value::String("alice".into()),
                Value::Int(100),
                Value::Int(50),
            ],
            vec![
                Value::String("alice".into()),
                Value::Int(101),
                Value::Int(70),
            ],
            vec![
                Value::String("cara".into()),
                Value::Int(102),
                Value::Int(20)
            ],
        ]
    );
}

/// B2: vector residual predicates match row-by-row evaluation.
#[test]
fn vec_hash_join_matches_legacy_with_residual() {
    let _guard = super::test_env_lock();
    let _restore = set_env_for_test("MYDB_VEC_HASH_JOIN", "1");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, t INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, t INT)").unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 100), (1, 200), (2, 100), (NULL, 50), (3, 10)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (1, 150), (2, 50), (NULL, 1), (3, 10)")
        .unwrap();

    let sql = "SELECT l.id, l.t, r.t FROM l JOIN r ON l.id = r.id AND l.t > r.t \
               ORDER BY l.t, r.t";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows);
    // l(1,200)>r(1,150) and l(2,100)>r(2,50) pass; equal t=10 does not satisfy greater-than.
    assert_eq!(vec_rows.len(), 2);
}

/// B2: RIGHT JOIN with preserved right probe mirrors LEFT handling.
#[test]
fn vec_hash_join_matches_legacy_for_right_join() {
    let _guard = super::test_env_lock();
    let _restore = set_env_for_test("MYDB_VEC_HASH_JOIN", "1");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, v INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, w INT)").unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 10), (1, 11), (3, 30)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (1, 100), (2, 200), (NULL, 300)")
        .unwrap();

    let sql = "SELECT l.id, l.v, r.id, r.w FROM l RIGHT JOIN r ON l.id = r.id \
               ORDER BY r.w, l.v";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows);
    // Preserve all r rows: id=1 matches twice; id=2 and NULL each receive one NULL extension.
    assert_eq!(vec_rows.len(), 4);
}

/// B2: null-safe equality keys use 0xFF sentinels so NULL matches NULL.
#[test]
fn vec_hash_join_matches_legacy_for_null_safe_eq() {
    let _guard = super::test_env_lock();
    let _restore = set_env_for_test("MYDB_VEC_HASH_JOIN", "1");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, v INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, w INT)").unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 10), (NULL, 20), (3, 30)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (1, 100), (NULL, 200), (4, 400)")
        .unwrap();

    let sql = "SELECT l.v, r.w FROM l JOIN r ON l.id <=> r.id ORDER BY l.v";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows);
    // Two rows: 1 null-safe-equals 1 and NULL null-safe-equals NULL.
    assert_eq!(vec_rows.len(), 2);
    assert_eq!(vec_rows[1], vec![Value::Int(20), Value::Int(200)]);
}

/// B2: LEFT joined residuals evaluate each pair before deciding NULL extension,
/// unlike the INNER Selection bitmap path.
#[test]
fn vec_hash_join_matches_legacy_for_left_with_residual() {
    let _guard = super::test_env_lock();
    let _restore = set_env_for_test("MYDB_VEC_HASH_JOIN", "1");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE l (id INT, t INT)").unwrap();
    engine.execute("CREATE TABLE r (id INT, t INT)").unwrap();
    engine
        .execute("INSERT INTO l VALUES (1, 100), (1, 200), (2, 100), (4, 50)")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES (1, 150), (2, 500)")
        .unwrap();

    // l.id=1,t=200 passes; l.id=1,t=100 and l.id=2 with 100<500 fail
    // and require NULL extensions; unmatched l.id=4 also requires one.
    let sql = "SELECT l.id, l.t, r.t FROM l LEFT JOIN r ON l.id = r.id AND l.t > r.t \
               ORDER BY l.t, l.id, r.t";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows);
    assert_eq!(vec_rows.len(), 4);
}

#[test]
fn vec_path_handles_empty_input() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT)")
        .unwrap();
    // No rows inserted.

    let legacy_topn = query_rows(&engine, "SELECT id FROM t ORDER BY id LIMIT 5");
    let vec_topn = with_vec_mode(VecMode::On, || {
        query_rows(&engine, "SELECT id FROM t ORDER BY id LIMIT 5")
    });
    assert_eq!(legacy_topn, vec_topn);
    assert!(legacy_topn.is_empty());

    let legacy_dist = query_rows(&engine, "SELECT DISTINCT k FROM t");
    let vec_dist = with_vec_mode(VecMode::On, || {
        query_rows(&engine, "SELECT DISTINCT k FROM t")
    });
    assert_eq!(legacy_dist, vec_dist);

    let legacy_agg = query_rows(&engine, "SELECT k, COUNT(*) FROM t GROUP BY k");
    let vec_agg = with_vec_mode(VecMode::On, || {
        query_rows(&engine, "SELECT k, COUNT(*) FROM t GROUP BY k")
    });
    assert_eq!(legacy_agg, vec_agg);
    assert!(legacy_agg.is_empty());
}

/// Regression test for a performance cliff that hit expression
/// aggregate fallback when `MYDB_VECTORIZED=1` was enabled.
///
/// Shape:
///   SELECT SUM(price * discount) FROM rows
///   WHERE shipdate >= ... AND shipdate < ... AND discount BETWEEN ...
///
/// The ungrouped aggregate is not handled by the vector hash-aggregate
/// bridge. It used to make `try_vec_hash_aggregate` start an upstream
/// stream and then fall back, causing the legacy path to scan the table
/// a second time. The bridge must reject it before starting that stream.
///
/// Use EXPLAIN ANALYZE's operator counters instead of a wall-clock ratio:
/// nextest deliberately runs expensive tests concurrently, so comparing
/// two millisecond-scale samples is not a stable single-scan oracle.
#[test]
fn expression_aggregate_fallback_is_correct_and_single_pass() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE lineitem (
                id INT PRIMARY KEY,
                l_shipdate INT,
                l_discount DOUBLE,
                l_quantity INT,
                l_price DOUBLE
            )",
        )
        .unwrap();
    // Operator loop counters, rather than wall time, prove single-pass
    // execution, so a compact fixture is sufficient.
    let mut sql = String::from("INSERT INTO lineitem VALUES ");
    for i in 0..512i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!(
            "({i}, {}, {}, {}, {})",
            // l_shipdate as int days; mix some in/out of [700..900]
            (i % 600) + 500,
            // l_discount as 0.0..0.10 by sliding tenths
            (i % 11) as f64 / 100.0,
            // l_quantity 1..50
            (i % 50) + 1,
            // l_price 100..200
            100.0 + (i % 100) as f64,
        ));
    }
    engine.execute(&sql).unwrap();

    // Aggregate argument is a Binary; the bridge must not start an upstream
    // stream just to bail out.
    let q = "SELECT SUM(l_price * l_discount) FROM lineitem \
             WHERE l_shipdate >= 700 AND l_shipdate < 900 \
               AND l_discount >= 0.02 AND l_discount <= 0.06 \
               AND l_quantity < 30";

    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, q));

    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, q));

    assert_eq!(
        legacy, vec_rows,
        "expression aggregate: vec result must equal legacy"
    );

    let explain_rows = with_vec_mode(VecMode::On, || {
        query_rows(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {q}"))
    });
    let explain = explain_rows
        .iter()
        .flatten()
        .map(Value::to_display_string)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        explain
            .lines()
            .any(|line| line.contains("Table scan on lineitem") && line.contains("loops=1")),
        "expression aggregate should scan lineitem exactly once: {explain}"
    );
}

/// Companion test: the bridge's static gate accepts genuinely
/// translatable aggregate shapes (simple SUM/MIN/MAX on a column), so
/// the regression fix didn't lock us out of the cases we *do* want to
/// vectorise.
#[test]
fn simple_group_by_sum_still_routes_through_vec() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    let mut sql = String::from("INSERT INTO t VALUES ");
    for i in 0..500i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({i}, {}, {})", i % 4, i));
    }
    engine.execute(&sql).unwrap();

    let q = "SELECT k, SUM(v) FROM t GROUP BY k";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, q));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, q));
    fn sort_rows(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
        rows.sort_by(|a, b| match (&a[0], &b[0]) {
            (Value::Int(x), Value::Int(y)) => x.cmp(y),
            _ => std::cmp::Ordering::Equal,
        });
        rows
    }
    assert_eq!(sort_rows(legacy), sort_rows(vec_rows));
}

#[test]
fn vec_off_selects_scalar_kernels_in_the_same_batch_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 1), (2, 2), (3, 1)")
        .unwrap();

    let before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let legacy = with_vec_mode(VecMode::Off, || {
        let response = engine
            .execute_streaming_for_test("SELECT DISTINCT k FROM t")
            .unwrap();
        assert!(matches!(response, QueryResponse::StreamingResultSet(_)));
        extract_rows(response)
    });
    let after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let with_on = with_vec_mode(VecMode::On, || {
        query_rows(&engine, "SELECT DISTINCT k FROM t")
    });
    assert!(
        after > before,
        "off mode left the common BatchOperator graph"
    );
    // Both modes use the same physical graph but independent scalar/vector
    // expression and aggregate kernels.
    fn sort_rows(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
        rows.sort_by(|a, b| {
            let ai = match &a[0] {
                Value::Int(x) => *x,
                _ => panic!(),
            };
            let bi = match &b[0] {
                Value::Int(x) => *x,
                _ => panic!(),
            };
            ai.cmp(&bi)
        });
        rows
    }
    assert_eq!(sort_rows(legacy), sort_rows(with_on));
}

// ---- LEFT JOIN ----

#[test]
fn vec_left_join_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let _vhj = set_env_for_test("MYDB_VEC_HASH_JOIN", "1");
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, cust_id INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (10, 1), (20, 1), (30, 2), (40, NULL), (50, 99)")
        .unwrap();

    let sql = "SELECT o.id, o.cust_id, c.id, c.name FROM orders o LEFT JOIN customers c ON o.cust_id = c.id ORDER BY o.id";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows, "LEFT JOIN vec must match legacy");
    assert_eq!(vec_rows.len(), 5);
    // o.id=40 has NULL cust_id → no match → c columns NULL
    assert_eq!(vec_rows[3][2], Value::Null);
    // o.id=50 has cust_id=99 → no match → c columns NULL
    assert_eq!(vec_rows[4][2], Value::Null);
}

#[test]
fn vec_left_join_all_match() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let _vhj = set_env_for_test("MYDB_VEC_HASH_JOIN", "1");
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, w INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (1, 100), (2, 200)")
        .unwrap();

    let sql = "SELECT a.id, b.w FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows);
    assert_eq!(vec_rows.len(), 2);
}

// ---- SEMI JOIN (EXISTS) ----

#[test]
fn vec_semi_join_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let _vhj = set_env_for_test("MYDB_VEC_HASH_JOIN", "1");
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, cust_id INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (10, 1), (20, 1), (30, 3), (40, NULL)")
        .unwrap();

    let sql = "SELECT o.id, o.cust_id FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.cust_id) ORDER BY o.id";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows, "SEMI JOIN vec must match legacy");
    // Only o.id=10 and o.id=20 match (cust_id=1 exists in customers)
    assert_eq!(vec_rows.len(), 2);
}

// ---- ANTI JOIN (NOT EXISTS) ----

#[test]
fn vec_anti_join_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let _vhj = set_env_for_test("MYDB_VEC_HASH_JOIN", "1");
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, cust_id INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (10, 1), (20, 3), (30, NULL), (40, 99)")
        .unwrap();

    let sql = "SELECT o.id, o.cust_id FROM orders o WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.id = o.cust_id) ORDER BY o.id";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows, "ANTI JOIN vec must match legacy");
    // o.id=20 (cust_id=3 not in customers), o.id=30 (NULL), o.id=40 (99 not in customers)
    assert_eq!(vec_rows.len(), 3);
}

#[test]
fn vec_anti_join_residual_pair_batches_match_scalar_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let _vhj = set_env_for_test("MYDB_VEC_HASH_JOIN", "1");
    engine
        .execute("CREATE TABLE ar_probe (id INT PRIMARY KEY, k INT, amount INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE ar_build (id INT PRIMARY KEY, k INT, threshold INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO ar_probe VALUES \
             (1,1,5),(2,1,15),(3,2,25),(4,3,35),(5,NULL,45)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO ar_build VALUES \
             (10,1,10),(11,1,20),(12,2,20),(13,3,40)",
        )
        .unwrap();

    // Equality keys build the candidate buckets; the correlated inequality
    // remains a mixed-side residual. SEMI/ANTI must evaluate those candidates
    // in bounded pair batches and retain existence semantics per probe row.
    for sql in [
        "SELECT p.id FROM ar_probe p WHERE NOT EXISTS \
         (SELECT 1 FROM ar_build b WHERE b.k = p.k AND b.threshold < p.amount) \
         ORDER BY p.id",
        "SELECT p.id FROM ar_probe p WHERE EXISTS \
         (SELECT 1 FROM ar_build b WHERE b.k = p.k AND b.threshold < p.amount) \
         ORDER BY p.id",
    ] {
        let scalar = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
        let vector = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
        assert_eq!(vector, scalar, "residual SEMI/ANTI mismatch: {sql}");
    }
}

#[test]
fn vec_anti_join_empty_build() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let _vhj = set_env_for_test("MYDB_VEC_HASH_JOIN", "1");
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, cust_id INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE customers (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (10, 1), (20, 2)")
        .unwrap();
    // customers is empty

    let sql = "SELECT o.id FROM orders o WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.id = o.cust_id) ORDER BY o.id";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows);
    assert_eq!(vec_rows.len(), 2);
}

#[test]
fn vec_semi_empty_build_does_not_open_probe_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let _vhj = set_env_for_test("MYDB_VEC_HASH_JOIN", "1");
    engine
        .execute("CREATE TABLE empty_probe (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine.execute("CREATE TABLE empty_build (k INT)").unwrap();
    insert_fixture_rows(
        &engine,
        "test.empty_probe",
        (1..=10_000).map(|n| vec![Value::Int(n), Value::Int(n % 100)]),
    );
    engine.execute("ANALYZE TABLE empty_probe").unwrap();
    engine.execute("ANALYZE TABLE empty_build").unwrap();

    let sql = "SELECT /*+ HASH_JOIN(empty_probe, empty_build) */ p.id \
               FROM empty_probe p WHERE EXISTS \
               (SELECT 1 FROM empty_build b WHERE b.k = p.k)";
    let tree = super::explain_analyze_tree(&engine, sql);
    assert!(
        tree.lines().any(|line| {
            line.contains("Table scan on p")
                && (line.contains("(never executed)")
                    || line.contains("actual time=0.000ms rows=0"))
        }),
        "empty SEMI build must short-circuit before scanning its probe:\n{tree}"
    );
    assert!(query_rows(&engine, sql).is_empty());
}

// -----------------------------------------------------------------
// Direct storage→Batch scan (StorageBatchStream) oracle tests
// -----------------------------------------------------------------

#[test]
fn vec_direct_batch_scan_hash_agg() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE sales (region VARCHAR(32), amount INT)")
        .unwrap();
    let mut vals = String::from("INSERT INTO sales VALUES ");
    for i in 0..3000i64 {
        if i > 0 {
            vals.push_str(", ");
        }
        let region = if i % 3 == 0 {
            "east"
        } else if i % 3 == 1 {
            "west"
        } else {
            "north"
        };
        vals.push_str(&format!("('{}', {})", region, i));
    }
    engine.execute(&vals).unwrap();

    let sql = "SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region ORDER BY region";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows);
    assert_eq!(vec_rows.len(), 3);
}

#[test]
fn vec_direct_batch_scan_topn() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(64), price INT)")
        .unwrap();
    let mut vals = String::from("INSERT INTO items VALUES ");
    for i in 0..2500i64 {
        if i > 0 {
            vals.push_str(", ");
        }
        vals.push_str(&format!("({}, 'item_{}', {})", i, i, i * 7 % 1000));
    }
    engine.execute(&vals).unwrap();

    let sql = "SELECT id, price FROM items ORDER BY price DESC LIMIT 10";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows);
    assert_eq!(vec_rows.len(), 10);
}

#[test]
fn vec_direct_batch_scan_with_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE nullable_t (k VARCHAR(16), v INT)")
        .unwrap();
    let mut vals = String::from("INSERT INTO nullable_t VALUES ");
    for i in 0..2000i64 {
        if i > 0 {
            vals.push_str(", ");
        }
        if i % 5 == 0 {
            vals.push_str(&format!("('g{}', NULL)", i % 4));
        } else {
            vals.push_str(&format!("('g{}', {})", i % 4, i));
        }
    }
    engine.execute(&vals).unwrap();

    let sql = "SELECT k, SUM(v), COUNT(*), COUNT(v) FROM nullable_t GROUP BY k ORDER BY k";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows);
}

#[test]
fn vec_direct_batch_scan_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE colors (name VARCHAR(32))")
        .unwrap();
    let mut vals = String::from("INSERT INTO colors VALUES ");
    let palette = ["red", "green", "blue", "yellow"];
    for i in 0..3000i64 {
        if i > 0 {
            vals.push_str(", ");
        }
        vals.push_str(&format!("('{}')", palette[i as usize % palette.len()]));
    }
    engine.execute(&vals).unwrap();

    let sql = "SELECT DISTINCT name FROM colors ORDER BY name";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows);
    assert_eq!(vec_rows.len(), 4);
}

// -----------------------------------------------------------------
// Filtered storage→Batch scan (FilteredStorageBatchStream) oracle tests
// -----------------------------------------------------------------

#[test]
fn vec_filtered_batch_scan_hash_agg() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE lineitem_mini (
                l_orderkey INT, l_partkey INT, l_suppkey INT,
                l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2),
                l_discount DECIMAL(15,2), l_tax DECIMAL(15,2),
                l_returnflag CHAR(1), l_linestatus CHAR(1),
                l_shipdate DATE,
                PRIMARY KEY(l_orderkey)
            )",
        )
        .unwrap();
    let mut vals = String::from("INSERT INTO lineitem_mini VALUES ");
    for i in 0..3000i64 {
        if i > 0 {
            vals.push_str(", ");
        }
        let flag = if i % 3 == 0 {
            "R"
        } else if i % 3 == 1 {
            "A"
        } else {
            "N"
        };
        let status = if i % 2 == 0 { "O" } else { "F" };
        let day = 1 + (i % 28) as u32;
        let month = 1 + (i % 12) as u32;
        vals.push_str(&format!(
            "({i}, {partkey}, {suppkey}, {qty}.00, {price}.00, 0.05, 0.08, '{flag}', '{status}', '1995-{month:02}-{day:02}')",
            partkey = i % 200,
            suppkey = i % 50,
            qty = 1 + i % 50,
            price = 100 + i % 900,
        ));
    }
    engine.execute(&vals).unwrap();

    let sql = "SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice), COUNT(*) \
               FROM lineitem_mini \
               WHERE l_shipdate <= '1995-06-15' \
               GROUP BY l_returnflag, l_linestatus \
               ORDER BY l_returnflag, l_linestatus";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows);
    assert!(!vec_rows.is_empty());
}

#[test]
fn vec_filtered_batch_scan_topn() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE products (id INT PRIMARY KEY, category VARCHAR(32), price INT, stock INT)")
        .unwrap();
    let mut vals = String::from("INSERT INTO products VALUES ");
    let cats = ["electronics", "books", "clothing", "food"];
    for i in 0..2500i64 {
        if i > 0 {
            vals.push_str(", ");
        }
        vals.push_str(&format!(
            "({}, '{}', {}, {})",
            i,
            cats[i as usize % cats.len()],
            10 + i % 500,
            i % 100
        ));
    }
    engine.execute(&vals).unwrap();

    let sql = "SELECT id, price FROM products WHERE category = 'electronics' ORDER BY price DESC LIMIT 10";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows);
    assert_eq!(vec_rows.len(), 10);
}

#[test]
fn vec_filtered_batch_scan_not_like_decodes_directly_into_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE orders_like (\
                id INT PRIMARY KEY, cust_id INT, comment VARCHAR(128), payload BIGINT\
             )",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO orders_like VALUES \
             (1,10,'ordinary order',100),\
             (2,10,'special pending requests',200),\
             (3,20,'requests are ordinary',300),\
             (4,20,'special xx requests',400),\
             (5,30,NULL,500)",
        )
        .unwrap();

    let sql = "SELECT cust_id, SUM(payload), COUNT(*) FROM orders_like \
               WHERE comment NOT LIKE '%special%requests%' \
               GROUP BY cust_id ORDER BY cust_id";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows);
    assert_eq!(vec_rows.len(), 2);
    assert_eq!(vec_rows[0][0], Value::Int(10));
    assert_eq!(vec_rows[1][0], Value::Int(20));
}

#[test]
fn vec_char_predicate_matches_unpadded_literal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE region (r_regionkey INT PRIMARY KEY, r_name CHAR(25) NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO region VALUES (1, 'ASIA'), (2, 'EUROPE')")
        .unwrap();

    let sql = "SELECT COUNT(*) FROM region WHERE r_name = 'ASIA'";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows);
    assert_eq!(vec_rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn vec_filtered_batch_scan_low_selectivity() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE wide_t (id INT PRIMARY KEY, a INT, b INT, c INT, d INT, e VARCHAR(64), f INT)")
        .unwrap();
    let mut vals = String::from("INSERT INTO wide_t VALUES ");
    for i in 0..3000i64 {
        if i > 0 {
            vals.push_str(", ");
        }
        vals.push_str(&format!(
            "({i}, {a}, {b}, {c}, {d}, 'text_{i}', {f})",
            a = i * 3,
            b = i * 7 % 1000,
            c = i % 50,
            d = i * 11 % 500,
            f = i % 200,
        ));
    }
    engine.execute(&vals).unwrap();

    let sql = "SELECT c, SUM(b), SUM(d), COUNT(*) FROM wide_t WHERE a < 150 GROUP BY c ORDER BY c";
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, vec_rows);
    assert!(!vec_rows.is_empty());
}

// ---- Expression-based aggregate (TPC-H Q1 pattern) ----

/// Verify that aggregates whose arguments are arithmetic expressions
/// (e.g. `SUM(price * discount)`) produce correct results through the
/// vectorized path with `ComputeColumnsBatchStream`.
#[test]
fn vec_expression_aggregate_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE lineitem (
                id INT PRIMARY KEY,
                l_returnflag VARCHAR(1),
                l_linestatus VARCHAR(1),
                l_quantity INT,
                l_extendedprice DOUBLE,
                l_discount DOUBLE,
                l_tax DOUBLE,
                l_shipdate INT
            )",
        )
        .unwrap();

    let mut sql = String::from("INSERT INTO lineitem VALUES ");
    for i in 0..3000i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        let flag = if i % 3 == 0 {
            "A"
        } else if i % 3 == 1 {
            "N"
        } else {
            "R"
        };
        let status = if i % 2 == 0 { "F" } else { "O" };
        sql.push_str(&format!(
            "({i}, '{flag}', '{status}', {qty}, {price}, {disc}, {tax}, {ship})",
            qty = (i % 50) + 1,
            price = 100.0 + (i % 100) as f64,
            disc = (i % 11) as f64 / 100.0,
            tax = (i % 5) as f64 / 100.0,
            ship = (i % 3000) + 500,
        ));
    }
    engine.execute(&sql).unwrap();

    // TPC-H Q1 pattern: expression aggregates with GROUP BY
    let q = "SELECT l_returnflag, l_linestatus, \
             SUM(l_quantity), \
             SUM(l_extendedprice), \
             SUM(l_extendedprice * (1 - l_discount)), \
             SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)), \
             AVG(l_quantity), \
             AVG(l_extendedprice), \
             AVG(l_discount), \
             COUNT(*) \
             FROM lineitem \
             GROUP BY l_returnflag, l_linestatus \
             ORDER BY l_returnflag, l_linestatus";

    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, q));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, q));

    assert_eq!(
        legacy.len(),
        vec_rows.len(),
        "row count mismatch: legacy={} vec={}",
        legacy.len(),
        vec_rows.len(),
    );
    // Compare group keys exactly.
    for (i, (l, v)) in legacy.iter().zip(vec_rows.iter()).enumerate() {
        assert_eq!(
            l[0], v[0],
            "row {i} l_returnflag mismatch: legacy={:?} vec={:?}",
            l[0], v[0]
        );
        assert_eq!(
            l[1], v[1],
            "row {i} l_linestatus mismatch: legacy={:?} vec={:?}",
            l[1], v[1]
        );
    }
    // Compare numeric aggregates with tolerance for floating-point.
    for (i, (l, v)) in legacy.iter().zip(vec_rows.iter()).enumerate() {
        for col in 2..l.len() {
            let lf = value_as_f64(&l[col]);
            let vf = value_as_f64(&v[col]);
            let diff = (lf - vf).abs();
            let tol = lf.abs().max(1.0) * 1e-6;
            assert!(
                diff < tol,
                "row {i} col {col} mismatch: legacy={lf} vec={vf} diff={diff}",
            );
        }
    }
}

/// Verify that expression aggregates with a WHERE filter also produce
/// correct results (exercises FilteredStorageBatchStream + ComputeColumnsBatchStream).
#[test]
fn vec_expression_aggregate_with_filter_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE sales (
                id INT PRIMARY KEY,
                region VARCHAR(8),
                quantity INT,
                price DOUBLE,
                discount DOUBLE
            )",
        )
        .unwrap();

    let mut sql = String::from("INSERT INTO sales VALUES ");
    for i in 0..2000i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        let region = match i % 4 {
            0 => "east",
            1 => "west",
            2 => "north",
            _ => "south",
        };
        sql.push_str(&format!(
            "({i}, '{region}', {qty}, {price}, {disc})",
            qty = (i % 30) + 1,
            price = 50.0 + (i % 80) as f64,
            disc = (i % 10) as f64 / 100.0,
        ));
    }
    engine.execute(&sql).unwrap();

    let q = "SELECT region, SUM(price * quantity), SUM(price * (1 - discount)), COUNT(*) \
             FROM sales \
             WHERE quantity > 10 \
             GROUP BY region \
             ORDER BY region";

    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, q));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, q));

    assert_eq!(legacy.len(), vec_rows.len());
    for (i, (l, v)) in legacy.iter().zip(vec_rows.iter()).enumerate() {
        assert_eq!(l[0], v[0], "row {i} region mismatch");
        for col in 1..l.len() {
            let lf = value_as_f64(&l[col]);
            let vf = value_as_f64(&v[col]);
            let diff = (lf - vf).abs();
            let tol = lf.abs().max(1.0) * 1e-6;
            assert!(
                diff < tol,
                "row {i} col {col}: legacy={lf} vec={vf} diff={diff}",
            );
        }
    }
}

/// Verify expression aggregate through TopN (ORDER BY ... LIMIT) path.
#[test]
fn vec_topn_over_expression_aggregate_matches_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE products (
                id INT PRIMARY KEY,
                category VARCHAR(16),
                price DOUBLE,
                qty INT
            )",
        )
        .unwrap();

    let mut sql = String::from("INSERT INTO products VALUES ");
    for i in 0..1500i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        let cat = match i % 5 {
            0 => "electronics",
            1 => "clothing",
            2 => "food",
            3 => "books",
            _ => "toys",
        };
        sql.push_str(&format!(
            "({i}, '{cat}', {price}, {qty})",
            price = 10.0 + (i % 200) as f64,
            qty = (i % 20) + 1,
        ));
    }
    engine.execute(&sql).unwrap();

    let q = "SELECT category, SUM(price * qty) as revenue, COUNT(*) \
             FROM products \
             GROUP BY category \
             ORDER BY SUM(price * qty) DESC \
             LIMIT 3";

    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, q));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, q));

    assert_eq!(legacy.len(), vec_rows.len());
    for (i, (l, v)) in legacy.iter().zip(vec_rows.iter()).enumerate() {
        assert_eq!(l[0], v[0], "row {i} category mismatch");
        for col in 1..l.len() {
            let lf = value_as_f64(&l[col]);
            let vf = value_as_f64(&v[col]);
            let diff = (lf - vf).abs();
            let tol = lf.abs().max(1.0) * 1e-6;
            assert!(
                diff < tol,
                "row {i} col {col}: legacy={lf} vec={vf} diff={diff}",
            );
        }
    }
}

fn value_as_f64(v: &Value) -> f64 {
    match v {
        Value::Int(x) => *x as f64,
        Value::UInt(x) => *x as f64,
        Value::Float(x) => *x as f64,
        Value::Double(x) => *x,
        Value::Null => 0.0,
        Value::DecimalCompact(mag, scale) => *mag as f64 / 10f64.powi(*scale as i32),
        Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
        Value::Decimal(s) => s.parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    }
}

#[test]
fn ordered_and_holistic_aggregates_stay_in_the_batch_pipeline() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE holistic (g INT, ord INT, k VARCHAR(8), v INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO holistic VALUES \
             (1, 2, 'b', 20), (1, 1, 'a', 10), (1, 3, 'c', NULL), \
             (2, 2, 'y', 8), (2, 1, 'x', 7)",
        )
        .unwrap();
    let sql = "SELECT g, GROUP_CONCAT(k ORDER BY ord SEPARATOR '|'), \
                      JSON_ARRAYAGG(v), JSON_OBJECTAGG(k, v) \
               FROM holistic GROUP BY g ORDER BY g";

    crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .store(0, Ordering::Relaxed);
    let scalar = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    assert!(
        crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
            .load(Ordering::Relaxed)
            > 0,
        "scalar aggregate kernel left the common BatchOperator graph"
    );
    let vector = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(scalar, vector);
}

#[test]
fn explain_analyze_reports_vector_runtime_counters() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let rows = with_vec_mode(VecMode::On, || {
        query_rows(
            &engine,
            "EXPLAIN ANALYZE FORMAT=TREE \
             SELECT generate_series, SUM(generate_series), AVG(generate_series) \
             FROM generate_series(1, 5000) GROUP BY generate_series",
        )
    });
    let explain = rows
        .iter()
        .flatten()
        .filter_map(crate::types::value_str)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        explain.contains("execution_mode=vector"),
        "missing vector execution mode:\n{explain}"
    );
    assert!(
        explain.contains("batches="),
        "missing batch count:\n{explain}"
    );
    assert!(
        !explain.contains("workers="),
        "single-worker default should be omitted:\n{explain}"
    );
}

#[test]
fn vec_temporal_functions_match_row_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE vt (id INT PRIMARY KEY, d DATE, dt DATETIME(6))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO vt VALUES \
             (1, '2024-05-31', '2024-05-31 12:34:56.987654'), \
             (2, '1999-12-31', '1999-12-31 23:59:59'), \
             (3, NULL, NULL), \
             (4, '0000-00-00', '0000-00-00 00:00:00'), \
             (5, '2024-02-29', '2024-02-29 00:00:01')",
        )
        .unwrap();

    // Cover typed YEAR/MONTH/DAY/HOUR/QUARTER extraction and new kernels
    // DATEDIFF/DATE_ADD/DATE_FORMAT/WEEK/TO_DAYS with mixed zero and NULL values.
    for sql in &[
        "SELECT id, YEAR(dt), MONTH(dt), DAY(dt), HOUR(dt), MINUTE(dt), SECOND(dt) FROM vt ORDER BY id",
        "SELECT id, YEAR(d), QUARTER(d), DAYOFWEEK(d), WEEKDAY(d) FROM vt ORDER BY id",
        "SELECT id, DATEDIFF(dt, '2024-01-01'), TO_DAYS(d), LAST_DAY(d) FROM vt ORDER BY id",
        "SELECT id, DATE_ADD(d, INTERVAL 1 MONTH), DATE_FORMAT(dt, '%Y/%m/%d %H:%i') FROM vt ORDER BY id",
        "SELECT id FROM vt WHERE YEAR(dt) = 2024 ORDER BY id",
    ] {
        let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
        let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
        assert_eq!(legacy, vec_rows, "vec/row divergence for: {sql}");
    }

    // Hash aggregate group order may differ legally between paths; compare multisets.
    let sql = "SELECT YEAR(dt), COUNT(*) FROM vt GROUP BY YEAR(dt)";
    let normalize = |mut rows: Vec<Vec<Value>>| {
        rows.sort_by_cached_key(|r| format!("{r:?}"));
        rows
    };
    let legacy = normalize(with_vec_mode(VecMode::Off, || query_rows(&engine, sql)));
    let vec_rows = normalize(with_vec_mode(VecMode::On, || query_rows(&engine, sql)));
    assert_eq!(legacy, vec_rows, "vec/row divergence for: {sql}");

    // ORDER BY 1 references the select expression and makes both paths sort
    // YEAR(dt) ascending with NULL first; see tests/order_by_ordinal.rs.
    let sql = "SELECT YEAR(dt), COUNT(*) FROM vt GROUP BY YEAR(dt) ORDER BY 1";
    let expected = vec![
        vec![Value::Null, Value::Int(1)],
        vec![Value::Int(0), Value::Int(1)],
        vec![Value::Int(1999), Value::Int(1)],
        vec![Value::Int(2024), Value::Int(2)],
    ];
    let legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    let vec_rows = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
    assert_eq!(legacy, expected, "row path not sorted by ordinal: {sql}");
    assert_eq!(vec_rows, expected, "vec path not sorted by ordinal: {sql}");
}

#[test]
fn stats_proven_count_topk_repeated_strings_keep_nulls_and_distinct_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE repeated_topk (id INT PRIMARY KEY, category VARCHAR(128), v INT)")
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.repeated_topk",
        (0..4000).map(|id| {
            let category = match id {
                0..1200 => Value::String("HTTP://Example/АБВЁЙ".into()),
                1200..2100 => Value::Null,
                2100..2700 => Value::String(String::new()),
                // Retain repeated residual groups as well as candidate counts.
                _ => Value::String(format!("residual-{}", id / 4)),
            };
            vec![
                Value::Int(id),
                category,
                if id % 7 == 0 {
                    Value::Null
                } else {
                    Value::Int(id % 41)
                },
            ]
        }),
    );
    engine.execute("ANALYZE TABLE repeated_topk").unwrap();
    for aggregate in ["COUNT(*)", "COUNT(DISTINCT v)"] {
        for predicate in ["", "WHERE category IS NULL OR category <> 'residual-999'"] {
            let sql = format!("SELECT /*+ PARALLEL(4) */ category, {aggregate} AS c FROM repeated_topk {predicate} GROUP BY category ORDER BY c DESC LIMIT 3");
            let mut legacy = with_vec_mode(VecMode::Off, || query_rows(&engine, &sql));
            let mut vector = with_vec_mode(VecMode::On, || query_rows(&engine, &sql));
            legacy.sort_by_cached_key(|row| format!("{row:?}"));
            vector.sort_by_cached_key(|row| format!("{row:?}"));
            assert_eq!(vector, legacy, "{sql}");
        }
    }
}

#[test]
fn count_topk_low_budget_discovers_hot_groups_missing_from_statistics() {
    use super::super::select::vec_bridge::{
        STATS_PROVEN_COUNT_TOPK_FALLBACKS, STATS_PROVEN_COUNT_TOPK_HITS,
    };
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::{atomic::Ordering, Arc};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE low_budget_topk (id INT PRIMARY KEY, category VARCHAR(128) NOT NULL)",
        )
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.low_budget_topk",
        (0..1000).map(|n| {
            vec![
                Value::Int(n),
                Value::String(format!("{}{n}", "x".repeat(112))),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE low_budget_topk").unwrap();
    insert_fixture_rows(
        &engine,
        "test.low_budget_topk",
        (1000..4500).map(|n| {
            vec![
                Value::Int(n),
                Value::String(if n < 3000 { "new-hot" } else { "second-hot" }.into()),
            ]
        }),
    );
    let sql = "SELECT /*+ PARALLEL(4) */ category, COUNT(*) AS c FROM low_budget_topk GROUP BY category ORDER BY c DESC LIMIT 2";
    let expected = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
    for limit_mb in [31, 129] {
        let budget = Arc::new(MemBudget::new(limit_mb * 1024 * 1024));
        let hits = STATS_PROVEN_COUNT_TOPK_HITS.load(Ordering::Relaxed);
        let fallbacks = STATS_PROVEN_COUNT_TOPK_FALLBACKS.load(Ordering::Relaxed);
        let actual = with_query_budget(budget.clone(), || {
            with_vec_mode(VecMode::On, || query_rows(&engine, sql))
        });
        assert_eq!(actual, expected);
        assert_eq!(
            actual.iter().map(|r| r[1].clone()).collect::<Vec<_>>(),
            vec![Value::Int(2000), Value::Int(1500)]
        );
        if limit_mb == 129 {
            assert!(STATS_PROVEN_COUNT_TOPK_HITS.load(Ordering::Relaxed) > hits);
            assert_eq!(
                STATS_PROVEN_COUNT_TOPK_FALLBACKS.load(Ordering::Relaxed),
                fallbacks
            );
        }
        assert_eq!(budget.used(), 0);
    }
}

/// Top-N specialization must inspect metadata before opening an eager join.
/// Ordinary aggregates and computed group keys reject the hidden-FIRST_VALUE
/// optimization; neither rejection may consume and replay the join input.
#[test]
fn joined_topn_aggregate_rejection_is_single_pass() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE dim (id INT, g DATE, label VARCHAR(12))")
        .unwrap();
    engine
        .execute("CREATE TABLE fact (id INT, amount DECIMAL(12,2), discount DECIMAL(4,2))")
        .unwrap();
    engine.execute("INSERT INTO dim VALUES (1,'2020-01-01','a'),(1,'2020-01-02','b'),(2,'2020-01-01','a'),(3,NULL,'c'),(NULL,'2020-01-03','d')").unwrap();
    engine.execute("INSERT INTO fact VALUES (1,10.25,0.10),(1,20.00,NULL),(2,30.00,0.20),(3,40.50,0.00),(9,50.00,0.50),(NULL,60.00,0.10)").unwrap();
    for sql in [
        "SELECT /*+ HASH_JOIN(d,f) */ d.g,d.label,COUNT(*),SUM(f.amount*(1-f.discount)) AS revenue FROM dim d JOIN fact f ON d.id=f.id GROUP BY d.g,d.label ORDER BY d.g,revenue DESC,d.label LIMIT 10",
        "SELECT /*+ HASH_JOIN(d,f) */ YEAR(d.g) AS y,COUNT(*),MIN(f.amount),MAX(f.amount),AVG(f.amount) FROM dim d JOIN fact f ON d.id=f.id GROUP BY YEAR(d.g) ORDER BY y LIMIT 10",
    ] {
        let oracle = with_vec_mode(VecMode::Off, || query_rows(&engine, sql));
        let actual = with_vec_mode(VecMode::On, || query_rows(&engine, sql));
        assert_eq!(actual, oracle, "{sql}");
        let rows = with_vec_mode(VecMode::On, || query_rows(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {sql}")));
        let explain = rows.iter().flatten().map(Value::to_display_string).collect::<Vec<_>>().join("\n");
        let scans = explain.lines().filter(|line| line.to_ascii_lowercase().contains("scan on ")).collect::<Vec<_>>();
        assert!(!scans.is_empty(), "{explain}");
        assert!(scans.iter().all(|line| line.contains("loops=1")), "join input was replayed: {explain}");
    }
}

#[test]
fn hash_join_topn_limits_share_bounded_build_admission() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use crate::planner::{BuildSide, PhysicalPlan};
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    // Keep the wide LOB fixture resident during setup. The separate 1 MiB
    // query budget below still forces both LIMIT variants through join spill.
    let engine = open_test_engine_with_buffer_pool_pages(dir.path(), 2048);
    engine
        .execute("CREATE TABLE quota_status (id INT PRIMARY KEY, state VARCHAR(16))")
        .unwrap();
    engine
        .execute("CREATE TABLE quota_payload (id INT PRIMARY KEY, note MEDIUMTEXT)")
        .unwrap();
    engine.execute("INSERT INTO quota_status SELECT n, CASE WHEN n < 12 THEN 'active' ELSE 'done' END FROM generate_series(0,31) gs(n)").unwrap();
    engine.execute("INSERT INTO quota_payload SELECT n, REPEAT('x',8192) FROM generate_series(0,511) gs(n)").unwrap();
    let expected = (0..12)
        .rev()
        .map(|id| vec![Value::Int(id), Value::String("x".repeat(8192))])
        .collect::<Vec<_>>();
    for limit in [100, 100000] {
        let sql = format!("SELECT /*+ HASH_JOIN(l,r) NO_PARALLEL */ l.id,r.note FROM quota_status l JOIN quota_payload r IGNORE INDEX(PRIMARY) ON l.id=r.id WHERE l.state IN ('active','starting','abnormal') ORDER BY l.id DESC LIMIT {limit}");
        let crate::parser::Statement::Select(select) =
            crate::parser::parse_statement(&sql).unwrap()
        else {
            unreachable!()
        };
        let mut planned = crate::planner::plan_select(engine.db_for_tests(), &select).unwrap();
        let mut joins = 0;
        planned.physical.walk_mut(|node| {
            if let PhysicalPlan::HashJoin {
                left, build_side, ..
            } = node
            {
                // Model an underestimated wide build without relying on the
                // cost model to choose a particular side from tiny fixtures.
                let payload_on_left = left.any(|child| {
                    matches!(child,
                    PhysicalPlan::SeqScan { table, .. }
                    | PhysicalPlan::ZoneMapScan { table, .. }
                    | PhysicalPlan::ParallelSeqScan { table, .. }
                    | PhysicalPlan::ParallelZoneMapScan { table, .. }
                    if table.alias.as_deref() == Some("r"))
                });
                *build_side = if payload_on_left {
                    BuildSide::Left
                } else {
                    BuildSide::Right
                };
                joins += 1;
            }
        });
        assert_eq!(joins, 1, "fixture must exercise a hash join");
        let txn = engine
            .db_for_tests()
            .read_only_statement_view(crate::storage::IsolationLevel::ReadCommitted, None);
        let budget = Arc::new(MemBudget::process_tracked_for_test(1024 * 1024));
        let spilled_before = crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_SPILL_BYTES
            .load(std::sync::atomic::Ordering::Relaxed);
        let rows = with_vec_mode(VecMode::On, || {
            with_query_budget(budget.clone(), || {
                extract_rows(
                    crate::engine::executor::select::plan::execute_physical_select(
                        engine.db_for_tests(),
                        txn,
                        &select,
                        &planned.physical,
                    )
                    .unwrap(),
                )
            })
        });
        assert_eq!(
            rows, expected,
            "LIMIT {limit} must not change memory correctness"
        );
        assert_eq!(budget.used(), 0, "LIMIT {limit} leaked its budget");
        assert!(
            crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_SPILL_BYTES
                .load(std::sync::atomic::Ordering::Relaxed)
                > spilled_before,
            "LIMIT {limit} must exercise the oversized build's spill path"
        );
    }
}

#[test]
fn unique_build_probe_aggregates_keep_typed_parallel_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE typed_dim (k BIGINT, p BIGINT, sk VARCHAR(8), price DECIMAL(12,3))")
        .unwrap();
    engine.execute("INSERT INTO typed_dim VALUES (1,11,'one',1.125),(2,12,'two',2.250),(3,13,'three',NULL),(NULL,NULL,NULL,4.500)").unwrap();
    engine.execute("CREATE TABLE typed_fact (k BIGINT,p BIGINT,sk VARCHAR(8),g BIGINT,label VARCHAR(8) COLLATE utf8mb4_general_ci,v DECIMAL(20,3),u BIGINT UNSIGNED,w DOUBLE)").unwrap();
    for start in (0..2048).step_by(256) {
        let values = (start..start + 256)
            .map(|i| {
                let key = i % 5;
                let (k, p, sk) = match key {
                    0 => ("1", "11", "'one'"),
                    1 => ("2", "12", "'two'"),
                    2 => ("3", "13", "'three'"),
                    3 => ("4", "14", "'missing'"),
                    _ => ("NULL", "NULL", "NULL"),
                };
                let group = if i % 13 == 0 {
                    "NULL".into()
                } else {
                    (i % 31).to_string()
                };
                let value = if i % 7 == 0 {
                    "NULL".into()
                } else {
                    format!("{}.125", i % 19)
                };
                let label = if i % 2 == 0 { "Ab" } else { "ab" };
                format!("({k},{p},{sk},{group},'{label}',{value},18446744073709551615,0.5)")
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO typed_fact VALUES {values}"))
            .unwrap();
    }
    for predicate in ["f.k=d.k", "f.k=d.k AND f.p=d.p", "f.sk=d.sk", "f.k<=>d.k"] {
        for group in ["f.g", "f.label"] {
            // Collation-equivalent keys may retain either original spelling.
            let projection = if group == "f.label" {
                "LOWER(f.label)"
            } else {
                group
            };
            let sql = format!("SELECT /*+ HASH_JOIN(f,d) PARALLEL(4) */ {projection},COUNT(*),COUNT(f.v),SUM(f.v),AVG(f.v),MIN(f.v),MAX(f.v),SUM(d.price),SUM(f.u),SUM(f.w) FROM typed_fact f JOIN typed_dim d ON {predicate} GROUP BY {group} ORDER BY {group}");
            let expected = with_vec_mode(VecMode::Off, || query_rows(&engine, &sql));
            let actual = with_vec_mode(VecMode::On, || query_rows(&engine, &sql));
            assert_eq!(actual, expected, "{sql}");
            let text = super::explain_analyze_tree(&engine, &sql);
            assert!(text.contains("Hash join"), "{text}");
            assert!(
                !text.contains("aggregate_mode=join_matches"),
                "unique keys have no multiplicity to fold: {text}"
            );
            assert!(text.contains("aggregate_mode=grouped_typed"), "{text}");
        }
    }
    let empty = "SELECT f.g,SUM(f.v),AVG(f.v),COUNT(*) FROM typed_fact f JOIN typed_dim d ON f.k=d.k WHERE d.k=999 GROUP BY f.g";
    assert!(query_rows(&engine, empty).is_empty());
}
