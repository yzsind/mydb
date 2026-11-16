//! WS-P0-3: residual support for streaming-inner hash join.
//!
//! Previously try_execute_streaming_inner_hash_join rejected residual.is_some(),
//! falling back to materialization and losing zero-materialization probe execution
//! and parallel page-scan benefits.
//!
//! JoinedResidualPre::compile_streaming now compiles residuals into
//! a dual-source evaluator over build PlanRow and probe &[Value], running
//! after equality matching and before emit in the hot loop. Unsupported expressions
//! or subqueries still fall back. MYDB_HASH_JOIN_STREAMING_INNER_RESIDUAL_DISABLE=on
//! forces the previous behavior.
//!
//! Compare result multisets with the switch on/off across residual forms.

use super::support::query_rows as rows_of;
use super::*;

fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by_cached_key(|row| format!("{row:?}"));
    rows
}

struct ScopedEnv {
    key: &'static str,
    prev: Option<String>,
}
impl ScopedEnv {
    fn set(key: &'static str, val: &str) -> Self {
        let prev = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, val);
        }
        Self { key, prev }
    }
}
impl Drop for ScopedEnv {
    fn drop(&mut self) {
        unsafe {
            match &self.prev {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

/// Build tl: 100 rows, id PK, k=id%10, w=id%5; probe tr: 500 rows, k=id%10,
/// v=id%7. Equality produces 100*500/10=5000 pairs; residuals select different subsets.
/// Unindexed join keys and a build below the memory limit allow the planner
/// to choose hash streaming with the applicable preserved-probe shape.
fn setup(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE tl (id INT PRIMARY KEY, k INT, w INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE tr (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.tl",
        (0..100).map(|i| vec![Value::Int(i), Value::Int(i % 10), Value::Int(i % 5)]),
    );
    insert_fixture_rows(
        engine,
        "test.tr",
        (0..500).map(|i| vec![Value::Int(i), Value::Int(i % 10), Value::Int(i % 7)]),
    );
}

fn diff_run(engine: &StorageQueryEngine, sql: &str) {
    let _g_off = ScopedEnv::set("MYDB_HASH_JOIN_STREAMING_INNER_RESIDUAL_DISABLE", "on");
    let expected = sorted(rows_of(engine, sql));
    drop(_g_off);

    let _g_on = ScopedEnv::set("MYDB_HASH_JOIN_STREAMING_INNER_RESIDUAL_DISABLE", "off");
    let got = sorted(rows_of(engine, sql));
    assert_eq!(
        got, expected,
        "streaming residual on/off result mismatch: {sql}"
    );
}

#[test]
fn streaming_residual_inner_cross_side_compare() {
    // tr.v > tl.w: a classic cross-side residual comparison.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(
        &engine,
        "SELECT tl.id, tr.id, tl.w, tr.v FROM tl JOIN tr ON tl.k=tr.k AND tr.v > tl.w \
         ORDER BY tl.id, tr.id",
    );
}

#[test]
fn streaming_residual_inner_arithmetic() {
    // tl.w + tr.v >= 5: an expression using both sides.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(
        &engine,
        "SELECT tl.id, tr.id FROM tl JOIN tr ON tl.k=tr.k AND tl.w + tr.v >= 5 \
         ORDER BY tl.id, tr.id",
    );
}

#[test]
fn streaming_residual_left_build_preserved_outer() {
    // build_is_outer_preserved_shape: build=left + LEFT JOIN
    // After ANALYZE this shape uses streaming-inner; unmatched preserved-side rows
    // must receive NULL extensions exactly as on the materialized path.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    // ANALYZE selects LEFT/build-left because tl is smaller than tr*0.7.
    engine.execute("ANALYZE TABLE tl").unwrap();
    engine.execute("ANALYZE TABLE tr").unwrap();
    diff_run(
        &engine,
        "SELECT tl.id, tr.id, tr.v FROM tl LEFT JOIN tr ON tl.k=tr.k AND tr.v > tl.w \
         ORDER BY tl.id, tr.id",
    );
}

#[test]
fn streaming_residual_inner_probe_only() {
    // tr.v > 3 references only probe columns and could be split into probe_only,
    // but is handled by the shared joined evaluator here with identical semantics.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(
        &engine,
        "SELECT tl.id, tr.id FROM tl JOIN tr ON tl.k=tr.k AND tr.v > 3 \
         ORDER BY tl.id, tr.id",
    );
}

#[test]
fn streaming_residual_inner_build_only() {
    // tl.w < 3 references only build columns and could be classified as build_only.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(
        &engine,
        "SELECT tl.id, tr.id FROM tl JOIN tr ON tl.k=tr.k AND tl.w < 3 \
         ORDER BY tl.id, tr.id",
    );
}

#[test]
fn residual_join_uses_common_batch_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);

    let before = native_batch_hits();
    let _ = rows_of(
        &engine,
        "SELECT tl.id, tr.id FROM tl JOIN tr ON tl.k=tr.k AND tr.v > tl.w",
    );
    let after = native_batch_hits();
    assert!(
        after > before,
        "expected the common batch pipeline, got before={before} after={after}"
    );
}
