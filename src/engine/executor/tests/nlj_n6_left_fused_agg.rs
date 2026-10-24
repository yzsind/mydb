//! WS-N6b: fuse ungrouped aggregation over LEFT INLJ.
//!
//! Previously ungrouped fusion accepted only covering INNER; extend to covering LEFT,
//! so SELECT COUNT(*), SUM(...) FROM small LEFT JOIN big_with_pk
//! also fuses. Unmatched outer rows enter accumulators through NULL extension,
//! incrementing count_star while NULL leaves SUM unchanged.

use super::*;

fn setup(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE big_pk (k INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE small_outer (id INT PRIMARY KEY, k INT, lv INT)")
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.big_pk",
        (0..1000).map(|i| vec![Value::Int(i), Value::Int(i * 7)]),
    );
    insert_fixture_rows(
        engine,
        "test.small_outer",
        (0..200).map(|i| {
            vec![
                Value::Int(i),
                Value::Int(if i % 2 == 0 { i } else { 2000 + i }),
                Value::Int(i * 3),
            ]
        }),
    );
    engine.execute("ANALYZE TABLE big_pk").unwrap();
    engine.execute("ANALYZE TABLE small_outer").unwrap();
}

fn run(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    result.rows
}

/// LEFT fused and unfused aggregation must agree.
#[test]
fn left_fused_agg_count_sum_matches_generic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);

    let sql = "SELECT /*+ INDEX_NLJ(small_outer, big_pk) NO_HASH_JOIN(small_outer, big_pk) */ \
               COUNT(*), SUM(big_pk.v), SUM(small_outer.lv) \
               FROM small_outer LEFT JOIN big_pk ON small_outer.k = big_pk.k";

    let unfused = with_test_env(&[("MYDB_INLJ_AGG_FUSED_DISABLE", Some("1"))], || {
        run(&engine, sql)
    });
    let fused = run(&engine, sql);
    assert_eq!(unfused, fused, "LEFT fused agg diverged from unfused");

    // All 200 outer rows emit, so COUNT(*)=200.
    // 100 matched keys 0,2,4,...,198 use big_pk.v=k*7.
    // SUM(big_pk.v) = 0+14+28+...+1386 = 7*(0+2+4+...+198) = 7*9900 = 69300
    // SUM(small_outer.lv) = 0+3+6+...+597 = 3*(0+1+...+199) = 59700
    // SUM(INT) returns DecimalCompact(N,0), matching MySQL fixed-point output.
    assert_eq!(
        fused[0],
        vec![
            Value::Int(200),
            Value::DecimalCompact(69300, 0),
            Value::DecimalCompact(59700, 0),
        ]
    );
}

/// LEFT INLJ aggregation must use the unified batch-stream result boundary.
#[test]
fn left_fused_agg_counter_advances() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);

    let sql = "SELECT /*+ INDEX_NLJ(small_outer, big_pk) NO_HASH_JOIN(small_outer, big_pk) */ \
               COUNT(*) \
               FROM small_outer LEFT JOIN big_pk ON small_outer.k = big_pk.k";

    let before = native_batch_hits();
    run(&engine, sql);
    let after = native_batch_hits();
    assert!(
        after > before,
        "LEFT INLJ aggregate did not reach the unified batch pipeline (before={before}, after={after})"
    );
}

/// NULL outer keys never match in LEFT joins but still contribute to row counts.
#[test]
fn left_fused_agg_null_outer_key_still_counted() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE pk_r (k INT PRIMARY KEY, v INT)")
        .unwrap();
    let mut sql = String::from("INSERT INTO pk_r VALUES ");
    for i in 0..1000 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({}, {})", i, i * 2));
    }
    engine.execute(&sql).unwrap();
    engine
        .execute("CREATE TABLE outer_l (id INT PRIMARY KEY, k INT, lv INT)")
        .unwrap();
    // Thirty rows, including ten NULL keys.
    engine
        .execute(
            "INSERT INTO outer_l VALUES \
             (1, 1, 10), (2, 2, 20), (3, NULL, 30), (4, 3, 40), (5, NULL, 50), \
             (6, 4, 60), (7, NULL, 70), (8, 5, 80), (9, NULL, 90), (10, 6, 100)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE pk_r").unwrap();
    engine.execute("ANALYZE TABLE outer_l").unwrap();

    let sql = "SELECT /*+ INDEX_NLJ(outer_l, pk_r) NO_HASH_JOIN(outer_l, pk_r) */ \
               COUNT(*), SUM(pk_r.v), SUM(outer_l.lv) \
               FROM outer_l LEFT JOIN pk_r ON outer_l.k = pk_r.k";

    let unfused = with_test_env(&[("MYDB_INLJ_AGG_FUSED_DISABLE", Some("1"))], || {
        run(&engine, sql)
    });
    let fused = run(&engine, sql);
    assert_eq!(unfused, fused);
    // COUNT(*)=10 since all rows emit; SUM(pk_r.v)=2+4+6+8+10+12=42.
    // SUM(outer_l.lv) = 10+20+30+40+50+60+70+80+90+100 = 550
    assert_eq!(
        fused[0],
        vec![
            Value::Int(10),
            Value::DecimalCompact(42, 0),
            Value::DecimalCompact(550, 0),
        ]
    );
}
