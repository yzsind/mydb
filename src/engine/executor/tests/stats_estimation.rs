//! Phase 2b + Phase 3 (full) statistics-estimation tests:
//! - `rows_estimation_source` provenance surfaced in EXPLAIN FORMAT=JSON.
//! - composite / multi-column NDV used for correlated AND predicates and
//!   equi-join cardinality.

use super::*;

fn explain_json(engine: &StorageQueryEngine, sql: &str) -> String {
    match engine
        .execute(&format!("EXPLAIN FORMAT=JSON {sql}"))
        .unwrap()
    {
        QueryResponse::ResultSet(rs) => match &rs.rows[0][0] {
            Value::String(text) => text.clone(),
            other => panic!("expected JSON string, got {other:?}"),
        },
        other => panic!("expected result set, got {other:?}"),
    }
}

#[test]
fn explain_json_reports_ndv_source_for_analyzed_equality() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 0..200 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {})", i % 20))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE t").unwrap();

    let json = explain_json(&engine, "SELECT * FROM t WHERE v = 5");
    assert!(
        json.contains("\"rows_estimation_source\""),
        "missing source field:\n{json}"
    );
    // After ANALYZE the column has NDV stats (and possibly a histogram), so
    // the estimate must come from real statistics, never the default tier.
    assert!(
        json.contains("\"rows_estimation_source\": \"ndv\"")
            || json.contains("\"rows_estimation_source\": \"histogram\""),
        "expected stats-backed source:\n{json}"
    );
    assert!(
        !json.contains("\"rows_estimation_source\": \"default\""),
        "analyzed column must not fall back to default:\n{json}"
    );
}

#[test]
fn explain_json_reports_default_source_for_non_column_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 0..200 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {})", i % 20))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE t").unwrap();

    // `v + 1 = 5` is not a `column <op> literal` leaf, so the estimator
    // has no per-column statistic to lean on — provenance is Default.
    let json = explain_json(&engine, "SELECT * FROM t WHERE v + 1 = 5");
    assert!(
        json.contains("\"rows_estimation_source\": \"default\""),
        "expected default source for opaque predicate:\n{json}"
    );
}

/// Joint NDV for the `(a, b)` set as collected by ANALYZE, by unqualified
/// table name in the default `test` database.
fn joint_ndv(engine: &StorageQueryEngine, table: &str, cols: &[&str]) -> Option<u64> {
    let catalog = engine.database().catalog();
    let owned: Vec<String> = cols.iter().map(|c| c.to_string()).collect();
    catalog
        .tables
        .values()
        .find(|t| t.name == table && t.database == "test")
        .and_then(|t| t.stats.multi_column_ndv(&owned))
}

#[test]
fn analyze_collects_multi_column_ndv_capturing_correlation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE mc (id INT PRIMARY KEY, a INT, b INT, INDEX idx_ab (a, b))")
        .unwrap();
    // a and b move together: 50 distinct values each, but only 50 distinct
    // *pairs* (perfectly correlated). The independence assumption would put
    // the joint NDV near 50*50 = 2500; the real joint NDV is 50.
    let values = (0..250)
        .map(|i| format!("({i},{},{})", i % 50, i % 50))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO mc VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE mc").unwrap();

    let ndv = joint_ndv(&engine, "mc", &["a", "b"]).expect("multi-column group must be collected");
    assert!(
        (30..=80).contains(&ndv),
        "joint NDV should capture correlation (~50), got {ndv}"
    );
    // Order-independent: the same group is found with the columns reversed.
    let ndv_rev = joint_ndv(&engine, "mc", &["b", "a"]).expect("group lookup is set-based");
    assert_eq!(ndv, ndv_rev);
}

#[test]
fn explain_join_uses_multi_column_ndv_without_regression() {
    // End-to-end smoke: a 2-column equi-join on a correlated composite key
    // plans and runs with the multi-column join path engaged. We don't
    // assert an exact row estimate (brittle) — only that the feature path
    // doesn't break planning/execution.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE l (a INT, b INT, v INT, INDEX idx_ab (a, b))")
        .unwrap();
    engine
        .execute("CREATE TABLE r (a INT, b INT, w INT, INDEX idx_ab (a, b))")
        .unwrap();
    let left = (0..150)
        .map(|i| format!("({},{},{i})", i % 50, i % 50))
        .collect::<Vec<_>>()
        .join(",");
    let right = (0..150)
        .map(|i| format!("({},{},{i})", i % 50, i % 50))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO l VALUES {left}"))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO r VALUES {right}"))
        .unwrap();
    engine.execute("ANALYZE TABLE l").unwrap();
    engine.execute("ANALYZE TABLE r").unwrap();

    let json = explain_json(
        &engine,
        "SELECT l.v, r.w FROM l JOIN r ON l.a = r.a AND l.b = r.b",
    );
    assert!(json.contains("query_block"), "join failed to plan:\n{json}");

    // And it actually executes.
    let resp = engine
        .execute("SELECT COUNT(*) FROM l JOIN r ON l.a = r.a AND l.b = r.b")
        .unwrap();
    assert!(matches!(resp, QueryResponse::ResultSet(_)));
}
