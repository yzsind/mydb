use super::support::query_rows;
use super::*;
use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};

fn fixture() -> (tempfile::TempDir, StorageQueryEngine) {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    // Wide pagination inputs and their indexes must fit independently of the
    // per-query sort/spill budgets. Buffer eviction is covered separately.
    let engine = open_test_engine_with_buffer_pool_pages(dir.path(), 4_096);
    (dir, engine)
}

fn analyzed(engine: &StorageQueryEngine, sql: &str) -> String {
    query_rows(engine, &format!("EXPLAIN ANALYZE {sql}"))
        .iter()
        .flat_map(|row| row.iter())
        .map(Value::to_display_string)
        .collect::<Vec<_>>()
        .join("\n")
}

fn max_metric(tree: &str, name: &str) -> u64 {
    tree.split(&format!("{name}="))
        .skip(1)
        .filter_map(|tail| {
            tail.split(|c: char| !c.is_ascii_digit())
                .next()?
                .parse()
                .ok()
        })
        .max()
        .unwrap_or(0)
}

#[test]
fn pagination_tuple_residual_keeps_bounded_heap_fetches_and_sql_null_semantics() {
    let (_dir, engine) = fixture();
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,k INT NULL,s VARCHAR(32) COLLATE utf8mb4_bin,payload VARCHAR(128),stamp DATETIME,INDEX ki(k,id),INDEX si(s,id),INDEX ti(stamp,id))").unwrap();
    insert_fixture_rows(
        &engine,
        "test.t",
        (0..4000).map(|id| {
            vec![
                Value::Int(id),
                if id % 13 == 0 {
                    Value::Null
                } else {
                    Value::Int(id % 30)
                },
                Value::String(format!("tag-{:03}", id % 30)),
                Value::String("x".repeat(100)),
                Value::DateTime((1_735_689_600 + id % 30) * 1_000_000),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE t").unwrap();
    for (index, predicate, order) in [
        ("ki", "(k,id)>(3,200)", "k,id"),
        ("ki", "(k,id)<=(20,2000)", "k DESC,id DESC"),
        ("ki", "(k,id)>(NULL,200)", "k,id"),
        ("ki", "(k,id)=(NULL,200)", "k,id"),
        ("ki", "(k,id)<>(3,NULL)", "k,id"),
        ("ki", "(ABS(k),id)>(3,200)", "k,id"),
        ("ti", "(stamp,id)>('2025-01-01 00:00:03',200)", "stamp,id"),
        ("si", "(s,id)>('tag-003',200)", "s,id"),
        ("si", "(s,id)<('tag-020',2000)", "s DESC,id DESC"),
    ] {
        for offset in [0, 7] {
            let sql = format!("SELECT id,k,s,payload FROM t FORCE INDEX({index}) WHERE {predicate} ORDER BY {order} LIMIT 11 OFFSET {offset}");
            let oracle = format!("SELECT id,k,s,payload FROM t IGNORE INDEX(PRIMARY,ki,si,ti) WHERE {predicate} ORDER BY {order} LIMIT 11 OFFSET {offset}");
            assert_eq!(
                query_rows(&engine, &sql),
                query_rows(&engine, &oracle),
                "{sql}"
            );
        }
    }
    let tree = analyzed(
        &engine,
        "SELECT id,payload FROM t FORCE INDEX(ki) WHERE (k,id)>(3,200) ORDER BY k,id LIMIT 11",
    );
    assert!(tree.contains("Index range scan"), "{tree}");
    assert!(max_metric(&tree, "heap_rows_fetched") <= 32, "{tree}");
    assert!(tree.contains("row_goal=11"), "{tree}");
}

#[test]
fn pagination_distinct_prefix_handles_types_bounds_residuals_and_reverse_order() {
    let (_dir, engine) = fixture();
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,k INT NULL,s VARCHAR(32) COLLATE utf8mb4_bin,d DECIMAL(8,2),flag INT,INDEX ki(k,id),INDEX si(s,id),INDEX di(d,id),INDEX ksi(k,s,id))").unwrap();
    insert_fixture_rows(
        &engine,
        "test.t",
        (0..4000).map(|id| {
            vec![
                Value::Int(id),
                if id % 17 == 0 {
                    Value::Null
                } else {
                    Value::Int(id % 20)
                },
                Value::String(format!("数据-{:02}", id % 20)),
                Value::Decimal(format!("{}.25", id % 20)),
                Value::Int(id / 100),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE t").unwrap();
    for (projection, index, order, predicate) in [
        ("k", "ki", "k", "1=1"),
        ("k", "ki", "k DESC", "k>=3 AND k<=17"),
        ("k", "ki", "k", "flag>=30"),
        ("s", "si", "s", "1=1"),
        ("s", "si", "s DESC", "s>='数据-03' AND s<'数据-18'"),
        ("d", "di", "d DESC", "1=1"),
        ("k,s", "ksi", "k,s", "1=1"),
        ("s,k", "ksi", "k DESC,s DESC", "flag>=30"),
    ] {
        let sql = format!("SELECT DISTINCT {projection} FROM t FORCE INDEX({index}) WHERE {predicate} ORDER BY {order} LIMIT 5 OFFSET 3");
        let oracle = format!("SELECT DISTINCT {projection} FROM t IGNORE INDEX(PRIMARY,ki,si,di,ksi) WHERE {predicate} ORDER BY {order} LIMIT 5 OFFSET 3");
        assert_eq!(
            query_rows(&engine, &sql),
            query_rows(&engine, &oracle),
            "{sql}"
        );
    }
    let tree = analyzed(
        &engine,
        "SELECT DISTINCT s FROM t FORCE INDEX(si) ORDER BY s LIMIT 5 OFFSET 3",
    );
    assert!(max_metric(&tree, "index_entries_examined") <= 8, "{tree}");
    assert!(max_metric(&tree, "index_entries_examined") > 0, "{tree}");
    // Writes may leave invisible or retired keys at a group's first position.
    engine.execute("DELETE FROM t WHERE id<100").unwrap();
    assert_eq!(
        query_rows(
            &engine,
            "SELECT DISTINCT k FROM t FORCE INDEX(ki) ORDER BY k LIMIT 8"
        ),
        query_rows(
            &engine,
            "SELECT DISTINCT k FROM t IGNORE INDEX(PRIMARY,ki,si,di,ksi) ORDER BY k LIMIT 8"
        )
    );
}

#[test]
fn pagination_ordered_lookup_join_keeps_matches_multiplicity_and_outer_rows() {
    let (_dir, engine) = fixture();
    engine
        .execute("CREATE TABLE fact(id INT PRIMARY KEY,k INT,payload VARCHAR(128),INDEX ki(k,id))")
        .unwrap();
    engine
        .execute("CREATE TABLE dim(id INT PRIMARY KEY,k INT,INDEX ki(k))")
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.fact",
        (0..6000).map(|id| {
            vec![
                Value::Int(id),
                Value::Int(id % 100),
                Value::String("x".repeat(100)),
            ]
        }),
    );
    insert_fixture_rows(
        &engine,
        "test.dim",
        (0..50).map(|id| vec![Value::Int(id), Value::Int(id % 20)]),
    );
    engine.execute("ANALYZE TABLE fact,dim").unwrap();
    for (join, condition, order) in [
        ("JOIN", "f.k=d.id", "f.k,f.id"),
        ("LEFT JOIN", "f.k=d.id", "f.k DESC,f.id DESC"),
        ("JOIN", "f.k=d.k", "f.id,d.id"),
    ] {
        let sql = format!("SELECT f.id,f.k,d.id FROM fact f {join} dim d ON {condition} WHERE f.id>=3500 ORDER BY {order} LIMIT 13 OFFSET 7");
        let expected = query_rows(&engine, &format!("SELECT /*+ HASH_JOIN(f,d) */ f.id,f.k,d.id FROM fact f {join} dim d ON {condition} WHERE f.id>=3500 ORDER BY {order} LIMIT 13 OFFSET 7"));
        assert_eq!(query_rows(&engine, &sql), expected, "{sql}");
    }
    let tree = analyzed(
        &engine,
        "SELECT f.id,f.payload,d.k FROM fact f JOIN dim d ON f.k=d.id ORDER BY f.k,f.id LIMIT 13",
    );
    assert!(tree.contains("Index nested loop join"), "{tree}");
    assert!(!tree.contains("TopK"), "{tree}");
    assert!(!tree.contains("rows=6000 loops=1"), "{tree}");
}

#[test]
fn pagination_deep_join_spills_without_materializing_wide_output() {
    let (_dir, engine) = fixture();
    engine
        .execute("CREATE TABLE fact(id INT PRIMARY KEY,k INT,payload VARCHAR(512))")
        .unwrap();
    engine
        .execute("CREATE TABLE dim(k INT,name VARCHAR(32))")
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.fact",
        (0..20000).map(|id| {
            vec![
                Value::Int(id),
                Value::Int(id % 100),
                Value::String("x".repeat(500)),
            ]
        }),
    );
    insert_fixture_rows(
        &engine,
        "test.dim",
        (0..100).map(|id| vec![Value::Int(id), Value::String(format!("tenant-{id}"))]),
    );
    engine.execute("ANALYZE TABLE fact,dim").unwrap();
    let sql = "SELECT f.id,f.payload,d.name FROM fact f JOIN dim d ON f.k=d.k ORDER BY f.k,f.id LIMIT 17 OFFSET 18000";
    let expected = query_rows(&engine, sql);
    let budget = Arc::new(MemBudget::new(8 * 1024 * 1024));
    let actual = with_query_budget(budget.clone(), || query_rows(&engine, sql));
    assert_eq!(actual, expected);
    assert_eq!(budget.used(), 0);
    assert!(budget.peak_used() > 0);
    assert!(
        budget.peak_used() <= budget.limit(),
        "peak={}",
        budget.peak_used()
    );
}

#[test]
fn pagination_resident_sort_window_preserves_typed_keys_and_selection() {
    use crate::engine::executor::select::vec_bridge::RESIDENT_WINDOW_HITS;
    use crate::engine::executor::sort::key_encode::with_sort_key_encode;
    use std::sync::atomic::Ordering;
    let (_dir, engine) = fixture();
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,k INT NULL,s VARCHAR(32) COLLATE utf8mb4_0900_ai_ci,d DECIMAL(12,2),stamp DATETIME,f FLOAT NULL,INDEX ki(k,id),INDEX si(s,id),INDEX di(d,id),INDEX ti(stamp,id),INDEX fi(f,id))").unwrap();
    // Every page goal remains above the 10000-row Top-N boundary.
    insert_fixture_rows(
        &engine,
        "test.t",
        (0..16384).map(|id| {
            vec![
                Value::Int(id),
                if id % 17 == 0 {
                    Value::Null
                } else {
                    Value::Int(id % 131 - 65)
                },
                Value::String(["A", "a", "É", "e", "汉字"][id as usize % 5].to_owned()),
                Value::Decimal(format!("{}.25", id % 131)),
                Value::DateTime((1_735_689_600 + id % 131) * 1_000_000),
                if id % 11 == 0 {
                    Value::Null
                } else {
                    Value::Float((id % 131 - 65) as f32 / 3.0)
                },
            ]
        }),
    );
    engine.execute("ANALYZE TABLE t").unwrap();
    for (columns, index, order, predicate) in [
        ("id,k", "ki", "k DESC,id ASC", "1=1"),
        ("id,k", "ki", "k ASC,id DESC", "k>=-65 AND id%3<>0"),
        ("id,k", "ki", "k+1 DESC,id ASC", "1=1"),
        ("id,k", "ki", "k*-3 ASC,id DESC", "k>=-65"),
        ("id,s", "si", "LOWER(s) DESC,id ASC", "1=1"),
        ("id,d", "di", "d DESC,id ASC", "1=1"),
        ("id,stamp", "ti", "stamp ASC,id DESC", "1=1"),
        ("id,f", "fi", "f ASC,id DESC", "1=1"),
    ] {
        let base = format!(
            "SELECT {columns} FROM t FORCE INDEX({index}) WHERE {predicate} ORDER BY {order}"
        );
        // Each order has an id tie-breaker. One full legacy sort supplies the
        // exact slices for all four independent resident-window executions.
        let sorted = with_sort_key_encode(false, || query_rows(&engine, &base));
        for offset in [10000, 12000, 16379, 20000] {
            let sql = format!("{base} LIMIT 17 OFFSET {offset}");
            let start = offset.min(sorted.len());
            let expected = &sorted[start..(start + 17).min(sorted.len())];
            let before = RESIDENT_WINDOW_HITS.load(Ordering::Relaxed);
            let budget = Arc::new(MemBudget::new(16 * 1024 * 1024));
            let actual = with_query_budget(budget.clone(), || query_rows(&engine, &sql));
            assert_eq!(actual, expected, "{sql}");
            assert!(
                RESIDENT_WINDOW_HITS.load(Ordering::Relaxed) > before,
                "resident selection missed: {sql}\n{}",
                analyzed(&engine, &sql)
            );
            assert_eq!(budget.used(), 0, "{sql}");
            assert!(budget.peak_used() <= budget.limit(), "{sql}");
        }
    }
}

#[test]
fn pagination_resident_window_replays_consumed_batches_on_quota_miss() {
    use crate::engine::executor::select::vec_bridge::{
        RESIDENT_WINDOW_REPLAYS, RESIDENT_WINDOW_REPLAYS_WITH_PREFIX,
    };
    use crate::engine::executor::sort::key_encode::with_sort_key_encode;
    use std::sync::atomic::Ordering;
    let (_dir, engine) = fixture();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,k INT,INDEX ki(k,id))")
        .unwrap();
    // Retain the original quota: the spiller's fixed buffer must also fit.
    // The prefix counter rules out an immediate no-input fallback.
    insert_fixture_rows(
        &engine,
        "test.t",
        (0..60000).map(|id| vec![Value::Int(id), Value::Int(id % 173)]),
    );
    engine.execute("ANALYZE TABLE t").unwrap();
    for order in ["k DESC,id ASC", "k+1 ASC,id DESC"] {
        let sql =
            format!("SELECT id,k FROM t FORCE INDEX(ki) ORDER BY {order} LIMIT 19 OFFSET 54000");
        let expected = with_sort_key_encode(false, || query_rows(&engine, &sql));
        let before = RESIDENT_WINDOW_REPLAYS.load(Ordering::Relaxed);
        let prefix_before = RESIDENT_WINDOW_REPLAYS_WITH_PREFIX.load(Ordering::Relaxed);
        let budget = Arc::new(MemBudget::new(4 * 1024 * 1024));
        let actual = with_query_budget(budget.clone(), || query_rows(&engine, &sql));
        assert_eq!(actual, expected, "{sql}");
        assert!(RESIDENT_WINDOW_REPLAYS.load(Ordering::Relaxed) > before);
        assert!(
            RESIDENT_WINDOW_REPLAYS_WITH_PREFIX.load(Ordering::Relaxed) > prefix_before,
            "quota miss must replay batches already consumed from the index"
        );
        assert_eq!(budget.used(), 0);
        // External merge admits a bounded spill buffer above its soft query
        // watermark. Retained input must not turn that into unbounded growth.
        assert!(
            budget.peak_used() <= budget.limit() + 256 * 1024,
            "peak={} limit={}",
            budget.peak_used(),
            budget.limit()
        );
    }
}

#[test]
fn pagination_resident_expression_rechecks_prepared_arithmetic_domain() {
    use crate::engine::api::{PreparedSelectMemoState, QueryEngine};
    use crate::engine::executor::sort::key_encode::with_sort_key_encode;
    let (_dir, engine) = fixture();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,k INT,INDEX ki(k,id))")
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.t",
        (0..20000).map(|id| vec![Value::Int(id), Value::Int(id % 173 - 86)]),
    );
    engine.execute("ANALYZE TABLE t").unwrap();
    let sql = "SELECT id,k FROM t FORCE INDEX(ki) ORDER BY k+?,id DESC LIMIT 19 OFFSET 18000";
    let mut session = SessionContext::default();
    let mut oracle_session = SessionContext::default();
    let parsed = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let mut memo = PreparedSelectMemoState::default();
    for value in [1, -3, i64::MAX, 0, 1] {
        let params = vec![Value::Int(value)];
        let actual = engine
            .execute_prepared_select(&mut session, &mut memo, &parsed, sql, &params)
            .expect("compiled SELECT supported")
            .and_then(QueryResponse::materialize);
        let expected = with_sort_key_encode(false, || {
            crate::engine::params::with_params(params, || {
                engine
                    .execute_prepared_statement(&mut oracle_session, sql, (*parsed).clone())
                    .expect("legacy SELECT supported")
                    .and_then(QueryResponse::materialize)
            })
        });
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => assert_eq!(
                super::support::result_rows(actual),
                super::support::result_rows(expected)
            ),
            (Err(actual), Err(expected)) => assert_eq!(actual.to_string(), expected.to_string()),
            (actual, expected) => {
                panic!("binding {value}: actual={actual:?}, expected={expected:?}")
            }
        }
    }
}

#[test]
fn pagination_text_limit_shapes_preserve_unsigned_domains_and_rebinding() {
    use crate::engine::api::QueryEngine;
    let (_dir, engine) = fixture();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1),(2),(3),(4),(5)")
        .unwrap();
    let session = SessionContext::default();
    let sql = "SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 1";
    assert_eq!(
        query_rows(&engine, sql),
        vec![vec![Value::Int(2)], vec![Value::Int(3)]]
    );
    let (_, params, _) = engine
        .cached_text_statement(&session, sql)
        .expect("literal LIMIT shape published");
    assert_eq!(params, vec![Value::UInt(2), Value::UInt(1)]);
    let (_, params, _) = engine
        .cached_text_statement(&session, "SELECT id FROM t ORDER BY id LIMIT 1 OFFSET 3")
        .expect("LIMIT shape rebound");
    assert_eq!(params, vec![Value::UInt(1), Value::UInt(3)]);
    for invalid in [
        "SELECT id FROM t ORDER BY id LIMIT -1 OFFSET 0",
        "SELECT id FROM t ORDER BY id LIMIT '2' OFFSET 1",
        "SELECT id FROM t ORDER BY id LIMIT 2.5 OFFSET 1",
    ] {
        assert!(
            engine.cached_text_statement(&session, invalid).is_none(),
            "{invalid}"
        );
        assert!(engine.execute(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn pagination_index_offset_seek_preserves_bounds_reverse_and_snapshot_visibility() {
    let (_dir, engine) = fixture();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,k INT NULL,payload VARCHAR(32),INDEX ki(k,id))")
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.t",
        (0..16000).map(|id| {
            vec![
                Value::Int(id),
                if id % 19 == 0 {
                    Value::Null
                } else {
                    Value::Int(id % 97)
                },
                Value::String(format!("row-{id}")),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE t").unwrap();
    let mut reader = SessionContext::default();
    let mut writer = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    let queries=[
        "SELECT id,k FROM t FORCE INDEX(ki) ORDER BY k,id LIMIT 13 OFFSET 12000",
        "SELECT id,k FROM t FORCE INDEX(ki) WHERE k>=10 AND k<=90 ORDER BY k DESC,id DESC LIMIT 13 OFFSET 8000",
        "SELECT id,k,payload FROM t FORCE INDEX(ki) ORDER BY k,id LIMIT 13 OFFSET 12000",
        "SELECT id,k FROM t FORCE INDEX(ki) WHERE k>=10 AND id%3<>0 ORDER BY k,id LIMIT 13 OFFSET 5000",
        "SELECT id FROM t ORDER BY id DESC LIMIT 13 OFFSET 20000",
        "SELECT DISTINCT k FROM t FORCE INDEX(ki) ORDER BY k LIMIT 120 OFFSET 0",
        "SELECT DISTINCT k FROM t FORCE INDEX(ki) ORDER BY k LIMIT 13 OFFSET 50",
        "SELECT DISTINCT k FROM t FORCE INDEX(ki) ORDER BY k DESC LIMIT 13 OFFSET 50",
    ];
    let expected = queries
        .iter()
        .map(|sql| {
            super::support::result_rows(engine.execute_with_session(&mut reader, sql).unwrap())
        })
        .collect::<Vec<_>>();
    engine
        .execute_with_session(&mut writer, "UPDATE t SET k=1000 WHERE id%101=0")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "DELETE FROM t WHERE id%103=0")
        .unwrap();
    for (sql, expected) in queries.iter().zip(expected) {
        let actual =
            super::support::result_rows(engine.execute_with_session(&mut reader, sql).unwrap());
        assert_eq!(actual, expected, "{sql}");
    }
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
    for (predicate, order) in [
        ("1=1", "k,id"),
        ("k>=10 AND k<=90", "k DESC,id DESC"),
        ("k>=10 AND id%3<>0", "k,id"),
    ] {
        let sql=format!("SELECT id,k,payload FROM t FORCE INDEX(ki) WHERE {predicate} ORDER BY {order} LIMIT 13 OFFSET 5000");
        let oracle = sql.replace("FORCE INDEX(ki)", "IGNORE INDEX(PRIMARY,ki)");
        assert_eq!(
            query_rows(&engine, &sql),
            query_rows(&engine, &oracle),
            "{sql}"
        );
    }
    let tree = analyzed(
        &engine,
        "SELECT id,k FROM t FORCE INDEX(ki) ORDER BY k,id LIMIT 13 OFFSET 12000",
    );
    assert!(max_metric(&tree, "index_entries_examined") < 1000, "{tree}");
}

#[test]
fn pagination_resident_heap_scan_and_set_windows_keep_decimal_and_wide_payloads() {
    use crate::engine::executor::select::vec_bridge::{
        RESIDENT_WINDOW_HITS, RESIDENT_WINDOW_REPLAYS,
    };
    use crate::engine::executor::sort::key_encode::with_sort_key_encode;
    use std::sync::atomic::Ordering;
    let (_dir, engine) = fixture();
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,d DECIMAL(12,2),stamp DATETIME,payload VARCHAR(512))").unwrap();
    insert_fixture_rows(
        &engine,
        "test.t",
        (0..24000).map(|id| {
            vec![
                Value::Int(id),
                Value::Decimal(format!("{}.25", id % 131)),
                Value::DateTime((1_735_689_600 + id % 97) * 1_000_000),
                Value::String("x".repeat(256)),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE t").unwrap();
    let small = "SELECT id,d FROM test.t ORDER BY d,id LIMIT 17";
    let expected = with_sort_key_encode(false, || query_rows(&engine, small));
    let before = RESIDENT_WINDOW_HITS.load(Ordering::Relaxed);
    let budget = Arc::new(MemBudget::new(32 * 1024 * 1024));
    let actual = with_query_budget(budget.clone(), || query_rows(&engine, small));
    assert_eq!(actual, expected);
    assert_eq!(RESIDENT_WINDOW_HITS.load(Ordering::Relaxed), before);
    assert_eq!(budget.used(), 0);
    for sql in [
        "SELECT id,d FROM test.t ORDER BY d,id LIMIT 17 OFFSET 18000",
        "SELECT id,stamp,payload FROM test.t WHERE id%3<>0 ORDER BY stamp,id LIMIT 17 OFFSET 12000",
        "SELECT id,d FROM test.t WHERE id<12000 UNION ALL SELECT id,d FROM test.t WHERE id>=12000 ORDER BY d,id LIMIT 17 OFFSET 18000",
    ] {
        let expected=with_sort_key_encode(false,||query_rows(&engine,sql));
        let before=RESIDENT_WINDOW_HITS.load(Ordering::Relaxed);
        let replays=RESIDENT_WINDOW_REPLAYS.load(Ordering::Relaxed);
        let budget=Arc::new(MemBudget::new(32*1024*1024));
        let actual=with_query_budget(budget.clone(),||query_rows(&engine,sql));
        assert_eq!(actual,expected,"{sql}");
        assert!(RESIDENT_WINDOW_HITS.load(Ordering::Relaxed)>before,"missed resident input, replays={}\n{}",RESIDENT_WINDOW_REPLAYS.load(Ordering::Relaxed)-replays,analyzed(&engine,sql));
        assert_eq!(budget.used(),0);
    }
}

#[test]
fn pagination_derived_lookup_cache_rechecks_live_rows_and_source_descriptors() {
    let (_dir, engine) = fixture();
    engine
        .execute("CREATE TABLE src(id INT PRIMARY KEY,k INT,INDEX ki(k,id))")
        .unwrap();
    engine
        .execute("CREATE TABLE dst(id INT PRIMARY KEY,v VARCHAR(32))")
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.src",
        (0..100).map(|id| vec![Value::Int(id), Value::Int(id % 17)]),
    );
    insert_fixture_rows(
        &engine,
        "test.dst",
        (0..100).map(|id| vec![Value::Int(id), Value::String(format!("v-{id}"))]),
    );
    engine.execute("ANALYZE TABLE src,dst").unwrap();
    let sorted = "SELECT p.id,p.k,d.v FROM (SELECT id,k FROM src ORDER BY k,id LIMIT 7 OFFSET 13) p JOIN dst d ON d.id=p.id ORDER BY d.v DESC,p.id";
    let expected = crate::engine::executor::sort::key_encode::with_sort_key_encode(false, || {
        query_rows(&engine, sorted)
    });
    let before = crate::engine::executor::select::vec_bridge::RESIDENT_WINDOW_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(query_rows(&engine, sorted), expected);
    assert!(
        crate::engine::executor::select::vec_bridge::RESIDENT_WINDOW_HITS
            .load(std::sync::atomic::Ordering::Relaxed)
            > before
    );
    let sql="SELECT p.id,p.k,d.v FROM (SELECT id,k FROM src ORDER BY k,id LIMIT 7 OFFSET 13) p JOIN dst d ON d.id=p.id ORDER BY p.k,p.id";
    let oracle="SELECT s.id,s.k,d.v FROM src s JOIN dst d ON d.id=s.id ORDER BY s.k,s.id LIMIT 7 OFFSET 13";
    for _ in 0..3 {
        assert_eq!(query_rows(&engine, sql), query_rows(&engine, oracle));
    }
    let before = engine
        .db_for_tests()
        .storage_status()
        .select_compiled_plan_hits;
    query_rows(&engine, sql);
    assert!(
        engine
            .db_for_tests()
            .storage_status()
            .select_compiled_plan_hits
            > before
    );
    engine
        .execute("UPDATE src SET k=30-k WHERE id%3=0")
        .unwrap();
    engine
        .execute("UPDATE dst SET v='changed' WHERE id%2=0")
        .unwrap();
    assert_eq!(query_rows(&engine, sql), query_rows(&engine, oracle));
    engine
        .execute("ALTER TABLE src MODIFY COLUMN k BIGINT")
        .unwrap();
    let actual = engine.execute(sql).unwrap().materialize().unwrap();
    let expected = engine.execute(oracle).unwrap().materialize().unwrap();
    let (QueryResponse::ResultSet(actual), QueryResponse::ResultSet(expected)) = (actual, expected)
    else {
        panic!("expected results")
    };
    assert_eq!(actual.rows, expected.rows);
    assert_eq!(
        actual.columns[1].column_type,
        expected.columns[1].column_type
    );
    engine.execute("DELETE FROM src WHERE id<40").unwrap();
    assert_eq!(query_rows(&engine, sql), query_rows(&engine, oracle));
}

#[test]
fn pagination_reordered_join_wildcards_keep_sql_column_order_and_metadata() {
    let (_dir, engine) = fixture();
    engine
        .execute("CREATE TABLE a(id INT PRIMARY KEY,label VARCHAR(16))")
        .unwrap();
    engine.execute("CREATE TABLE b(id INT PRIMARY KEY,a_id INT,stamp INT,payload VARCHAR(32),INDEX si(stamp,id))").unwrap();
    insert_fixture_rows(
        &engine,
        "test.a",
        (0..17).map(|id| vec![Value::Int(id), Value::String(format!("name-{id}"))]),
    );
    insert_fixture_rows(
        &engine,
        "test.b",
        (0..12000).map(|id| {
            vec![
                Value::Int(id),
                Value::Int(id % 17),
                Value::Int((id * 73) % 12000),
                Value::String(format!("payload-{id}")),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE a,b").unwrap();
    let tail = " FROM a JOIN b ON a.id=b.a_id ORDER BY b.stamp,b.id";
    let tree = analyzed(&engine, &format!("SELECT *{tail} LIMIT 3 OFFSET 7"));
    assert!(tree.contains("Index nested loop"), "{tree}");
    for mode in ["auto", "on", "off"] {
        crate::util::env::with_vars(&[("MYDB_VECTORIZED", Some(mode))], || {
            for limit in [0, 1, 3, 20] {
                let oracle=format!("SELECT a.id,a.label,b.id,b.a_id,b.stamp,b.payload{tail} LIMIT {limit} OFFSET 7");
                let QueryResponse::ResultSet(expected) =
                    engine.execute(&oracle).unwrap().materialize().unwrap()
                else {
                    panic!("expected rows")
                };
                for projection in ["*", "a.*,b.*"] {
                    let sql = format!("SELECT {projection}{tail} LIMIT {limit} OFFSET 7");
                    for _ in 0..2 {
                        let QueryResponse::ResultSet(actual) =
                            engine.execute(&sql).unwrap().materialize().unwrap()
                        else {
                            panic!("expected rows")
                        };
                        assert_eq!(actual.rows, expected.rows, "{mode} {sql}");
                        assert_eq!(actual.columns, expected.columns, "{mode} {sql}");
                    }
                    let derived =
                        format!("SELECT * FROM ({sql}) q(aid,label,bid,a_id,stamp,payload)");
                    assert_eq!(
                        query_rows(&engine, &derived),
                        expected.rows,
                        "{mode} {derived}"
                    );
                }
            }
        });
    }
    engine
        .execute("ALTER TABLE a ADD extra INT DEFAULT 11")
        .unwrap();
    let sql = format!("SELECT *{tail} LIMIT 3 OFFSET 7");
    let oracle =
        format!("SELECT a.id,a.label,a.extra,b.id,b.a_id,b.stamp,b.payload{tail} LIMIT 3 OFFSET 7");
    let QueryResponse::ResultSet(actual) = engine.execute(&sql).unwrap().materialize().unwrap()
    else {
        panic!("expected rows")
    };
    let QueryResponse::ResultSet(expected) =
        engine.execute(&oracle).unwrap().materialize().unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(actual.rows, expected.rows);
    assert_eq!(actual.columns, expected.columns);
}

#[test]
fn pagination_hash_join_resident_pages_preserve_fanout_outer_rows_and_typed_keys() {
    use crate::engine::executor::select::vec_bridge::RESIDENT_WINDOW_HITS;
    use crate::engine::executor::sort::key_encode::with_sort_key_encode;
    use std::sync::atomic::Ordering;
    let (_dir, engine) = fixture();
    engine.execute("CREATE TABLE fact(id INT PRIMARY KEY,k INT NULL,s VARCHAR(32) COLLATE utf8mb4_0900_ai_ci,d DECIMAL(12,2),stamp DATETIME,payload VARCHAR(128))").unwrap();
    engine
        .execute("CREATE TABLE dim(id INT PRIMARY KEY,k INT,label VARCHAR(32))")
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.fact",
        (0..12000).map(|id| {
            vec![
                Value::Int(id),
                if id % 17 == 0 {
                    Value::Null
                } else {
                    Value::Int(id % 64)
                },
                Value::String(["A", "a", "É", "e", "汉字"][id as usize % 5].into()),
                Value::Decimal(format!("{}.25", id % 131)),
                Value::DateTime((1_735_689_600 + id % 131) * 1_000_000),
                Value::String("x".repeat(120)),
            ]
        }),
    );
    insert_fixture_rows(
        &engine,
        "test.dim",
        (0..150).map(|id| {
            vec![
                Value::Int(id),
                Value::Int(id % 40),
                Value::String(format!("dim-{id}")),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE fact,dim").unwrap();
    for (join, order, predicate) in [
        ("JOIN", "f.k DESC,d.id,f.id", "1=1"),
        ("LEFT JOIN", "f.s DESC,d.id,f.id", "f.id%3<>0"),
        ("LEFT JOIN", "f.d,f.stamp DESC,d.id,f.id", "1=1"),
    ] {
        let base = format!("SELECT /*+ HASH_JOIN(f,d) */ f.id,f.k,f.s,f.d,f.stamp,f.payload,d.id,d.label FROM fact f {join} dim d ON f.k=d.k WHERE {predicate} ORDER BY {order}");
        let sorted = with_sort_key_encode(false, || query_rows(&engine, &base));
        for offset in [18000, 40000] {
            let sql = format!("{base} LIMIT 17 OFFSET {offset}");
            let begin = offset.min(sorted.len());
            let expected = &sorted[begin..(begin + 17).min(sorted.len())];
            let before = RESIDENT_WINDOW_HITS.load(Ordering::Relaxed);
            let budget = Arc::new(MemBudget::new(64 * 1024 * 1024));
            assert_eq!(
                with_query_budget(budget.clone(), || query_rows(&engine, &sql)),
                expected,
                "{sql}"
            );
            assert!(
                RESIDENT_WINDOW_HITS.load(Ordering::Relaxed) > before,
                "{sql}"
            );
            assert_eq!(budget.used(), 0);
            assert!(budget.peak_used() <= budget.limit());
            let budget = Arc::new(MemBudget::new(1024 * 1024));
            assert_eq!(
                with_query_budget(budget.clone(), || query_rows(&engine, &sql)),
                expected,
                "spill: {sql}"
            );
            assert_eq!(budget.used(), 0);
        }
    }
}

#[test]
fn pagination_single_partition_windows_match_spill_and_rank_bounds() {
    let (_dir, engine) = fixture();
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY,k INT NULL,d DECIMAL(12,2),stamp DATETIME,s VARCHAR(32))").unwrap();
    insert_fixture_rows(
        &engine,
        "test.t",
        (0..512).map(|id| {
            vec![
                Value::Int(id),
                if id % 11 == 0 {
                    Value::Null
                } else {
                    Value::Int(id % 31 - 15)
                },
                if id % 7 == 0 {
                    Value::Null
                } else {
                    Value::Decimal(format!("{}.25", id % 47))
                },
                Value::DateTime((1_735_689_600 + id % 29) * 1_000_000),
                Value::String(format!("数据-{id}")),
            ]
        }),
    );
    for sql in [
        "SELECT id,s,ROW_NUMBER() OVER (ORDER BY k DESC,id),RANK() OVER (ORDER BY k),DENSE_RANK() OVER (ORDER BY stamp DESC) FROM t WHERE id%3<>0 ORDER BY id",
        "SELECT id,SUM(d) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),AVG(d) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING),MIN(stamp) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),COUNT(*) OVER () FROM t ORDER BY id",
        "SELECT id,LAG(k,2) OVER (ORDER BY id),LEAD(d) OVER (ORDER BY id),FIRST_VALUE(stamp) OVER (ORDER BY k),LAST_VALUE(stamp) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t ORDER BY id",
        "SELECT id,rn FROM (SELECT id,ROW_NUMBER() OVER (ORDER BY k DESC,id) rn FROM t) q WHERE rn>400 AND rn<=420 ORDER BY rn",
        "SELECT id,ROW_NUMBER() OVER (ORDER BY k,id),COUNT(*) OVER () FROM t WHERE id<0 ORDER BY id",
    ] {
        let low = Arc::new(MemBudget::new(32 * 1024));
        let expected = with_query_budget(low.clone(), || query_rows(&engine, sql));
        assert_eq!(low.used(), 0);
        let high = Arc::new(MemBudget::new(16 * 1024 * 1024));
        assert_eq!(with_query_budget(high.clone(), || query_rows(&engine, sql)), expected, "{sql}");
        assert_eq!(high.used(), 0);
        assert!(high.peak_used() <= high.limit());
    }
    let tree = analyzed(
        &engine,
        "SELECT id,ROW_NUMBER() OVER (ORDER BY k DESC,id) FROM t",
    );
    assert!(tree.contains("window_mode=columnar_partitions"), "{tree}");
    let rows = query_rows(&engine, "SELECT id,rn FROM (SELECT id,ROW_NUMBER() OVER (ORDER BY id DESC) rn FROM t) q WHERE rn>400 AND rn<=420 ORDER BY rn");
    assert_eq!(
        rows,
        (401..=420)
            .map(|rank| vec![Value::Int(512 - rank), Value::Int(rank)])
            .collect::<Vec<_>>()
    );
}

#[test]
fn window_final_pages_match_full_results_for_types_expressions_and_distinct() {
    let (_dir, engine) = fixture();
    engine.execute("CREATE TABLE window_pages(id INT PRIMARY KEY,k INT NULL,d DECIMAL(30,3),s VARCHAR(64) COLLATE utf8mb4_0900_ai_ci,stamp DATETIME)").unwrap();
    insert_fixture_rows(
        &engine,
        "test.window_pages",
        (0..301).map(|id| {
            vec![
                Value::Int(id),
                if id % 17 == 0 {
                    Value::Null
                } else {
                    Value::Int(id % 13)
                },
                Value::Decimal(format!("{}.125", id - 150)),
                if id % 19 == 0 {
                    Value::Null
                } else {
                    Value::String(["École", "ecole", "中文", "A", "a"][id as usize % 5].into())
                },
                Value::DateTime((1_735_689_600 + id % 23) * 1_000_000),
            ]
        }),
    );
    for base in [
        "SELECT id,k,d,s,stamp,COUNT(*) OVER() AS total FROM window_pages ORDER BY s DESC NULLS FIRST,d,id",
        "SELECT id,COUNT(*) OVER() AS total FROM window_pages ORDER BY stamp DESC,id",
        "SELECT id AS z,COUNT(k) OVER(PARTITION BY k) AS c,SUM(d) OVER() AS total FROM window_pages ORDER BY 2 DESC,1",
        "SELECT id,RANK() OVER(ORDER BY k) AS r,ROW_NUMBER() OVER(ORDER BY d DESC) AS rn FROM window_pages ORDER BY r DESC,id",
        "SELECT id,SUM(d) OVER(ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)+id AS x FROM window_pages ORDER BY x DESC,id",
        "SELECT id,COUNT(*) OVER() AS total FROM window_pages ORDER BY ABS(k),id",
        "SELECT DISTINCT k,COUNT(*) OVER() AS c FROM window_pages ORDER BY k",
        "SELECT id,COUNT(*) OVER() AS total FROM window_pages WHERE id<0 ORDER BY id",
    ] {
        let all=query_rows(&engine,base);
        for (offset,limit) in [(0,0),(0,1),(2,7),(150,11),(299,20),(301,2),(1000,7)] {
            let sql=format!("{base} LIMIT {limit} OFFSET {offset}");
            let actual=query_rows(&engine,&sql);
            assert_eq!(actual,all.iter().skip(offset).take(limit).cloned().collect::<Vec<_>>(),"{sql}");
        }
    }
}

#[test]
fn window_final_page_preserves_random_draws_warnings_and_bound_limits() {
    let (_dir, engine) = fixture();
    engine
        .execute("CREATE TABLE window_observable(id INT PRIMARY KEY)")
        .unwrap();
    insert_fixture_rows(
        &engine,
        "test.window_observable",
        (0..101).map(|id| vec![Value::Int(id)]),
    );
    let mut session = SessionContext::default();
    fn run(
        engine: &StorageQueryEngine,
        session: &mut SessionContext,
        sql: &str,
    ) -> (Vec<Vec<Value>>, Vec<Vec<Value>>, Vec<Vec<Value>>) {
        engine
            .execute_with_session(session, "SET @@rand_seed1=10000000,@@rand_seed2=1000000")
            .unwrap();
        engine
            .execute_with_session(session, "SET sql_mode='ERROR_FOR_DIVISION_BY_ZERO'")
            .unwrap();
        let rows = super::support::result_rows(engine.execute_with_session(session, sql).unwrap());
        let warnings = super::support::result_rows(
            engine
                .execute_with_session(session, "SHOW WARNINGS")
                .unwrap(),
        );
        let next = super::support::result_rows(
            engine
                .execute_with_session(session, "SELECT RAND()")
                .unwrap(),
        );
        (rows, warnings, next)
    }
    for sql in [
        "SELECT id,COUNT(*) OVER()+RAND() AS x FROM window_observable FORCE INDEX(PRIMARY) ORDER BY x,id",
        "SELECT id,COUNT(*) OVER() AS c FROM window_observable FORCE INDEX(PRIMARY) ORDER BY RAND(),id",
        "SELECT id,COUNT(*) OVER()+1/0 AS x FROM window_observable FORCE INDEX(PRIMARY) ORDER BY id",
    ] {
        let (full,warnings,next)=run(&engine,&mut session,sql);
        for (offset,limit) in [(0,0),(3,7),(1000,7)] {
            let (page,actual_warnings,actual_next)=run(&engine,&mut session,&format!("{sql} LIMIT {limit} OFFSET {offset}"));
            assert_eq!(page,full.iter().skip(offset).take(limit).cloned().collect::<Vec<_>>(),"{sql}, offset={offset}, limit={limit}");
            assert_eq!(actual_warnings,warnings,"discarded projections retain diagnostics: {sql}");
            assert_eq!(actual_next,next,"discarded sort keys retain their random draws: {sql}");
        }
    }
    engine.execute_with_session(&mut session,"PREPARE window_page FROM 'SELECT id,COUNT(*) OVER() AS c FROM window_observable ORDER BY id DESC LIMIT ? OFFSET ?'").unwrap();
    for (limit, offset) in [(1, 0), (7, 11), (20, 99), (20, 1000)] {
        engine
            .execute_with_session(&mut session, &format!("SET @n={limit},@o={offset}"))
            .unwrap();
        let actual = super::support::result_rows(
            engine
                .execute_with_session(&mut session, "EXECUTE window_page USING @n,@o")
                .unwrap(),
        );
        let expected = (0..101)
            .rev()
            .skip(offset)
            .take(limit)
            .map(|id| vec![Value::Int(id), Value::Int(101)])
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "bound offset={offset}, limit={limit}");
    }
}
