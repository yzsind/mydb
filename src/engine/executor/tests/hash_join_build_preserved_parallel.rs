//! WS-P1-1: parallel probing for build-preserved LEFT/RIGHT joins.
//!
//! Previously try_execute_inner_hash_join_parallel_probe accepted only
//! INNER and probe-preserved outer joins; LEFT/build-left and
//! RIGHT/build-right used serial do_in_memory_hash_join.
//!
//! Each worker now maintains matched_local: Vec<bool>; the main thread OR-merges
//! them and serially emits NULL extensions for unmatched build rows.
//! MYDB_HASH_JOIN_PARALLEL_PROBE_BUILD_PRESERVED_DISABLE=1 forces serial fallback.
//!
//! Compare result multisets with the switch on/off for INNER, LEFT, RIGHT, and residual joins.

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

/// Smaller build and larger probe make ANALYZE select build-left for LEFT JOIN.
/// Build has 100 rows, with each k appearing every three rows; some build rows remain unmatched.
/// Probe has 400 rows, k=id%30, exceeding build size and the lowered test threshold.
/// MYDB_HASH_JOIN_PARALLELISM_THRESHOLD=100 lowers the admission gate.
fn setup(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE p (id INT PRIMARY KEY, k INT, w INT)")
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.b",
        (0..100).map(|i| vec![Value::Int(i), Value::Int(i * 7 % 50), Value::Int(i)]),
    );
    insert_fixture_rows(
        engine,
        "test.p",
        (0..400).map(|i| vec![Value::Int(i), Value::Int(i % 30), Value::Int(i)]),
    );
    engine.execute("ANALYZE TABLE b").unwrap();
    engine.execute("ANALYZE TABLE p").unwrap();
}

fn diff_run(engine: &StorageQueryEngine, sql: &str) {
    // Set parallelism_threshold=100 so 100+400=500 triggers parallel probing.
    let _g_th = ScopedEnv::set("MYDB_HASH_JOIN_PARALLELISM_THRESHOLD", "100");

    let _g_off = ScopedEnv::set("MYDB_HASH_JOIN_PARALLEL_PROBE_BUILD_PRESERVED_DISABLE", "1");
    let expected = sorted(rows_of(engine, sql));
    drop(_g_off);

    let _g_on = ScopedEnv::set("MYDB_HASH_JOIN_PARALLEL_PROBE_BUILD_PRESERVED_DISABLE", "0");
    let got = sorted(rows_of(engine, sql));
    assert_eq!(
        got, expected,
        "build_preserved parallel on/off result mismatch: {sql}"
    );
}

#[test]
fn build_preserved_left_join_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(
        &engine,
        "SELECT b.id, b.k, p.id, p.w FROM b LEFT JOIN p ON b.k = p.k \
         ORDER BY b.id, p.id",
    );
}

#[test]
fn build_preserved_left_join_with_residual_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(
        &engine,
        "SELECT b.id, b.k, p.id FROM b LEFT JOIN p ON b.k = p.k AND p.w > b.v \
         ORDER BY b.id, p.id",
    );
}

#[test]
fn build_preserved_right_join_equivalent() {
    // For the reverse RIGHT JOIN/build-right case, r must be smaller than l.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(
        &engine,
        "SELECT p.id, p.k, b.id FROM p RIGHT JOIN b ON p.k = b.k \
         ORDER BY b.id, p.id",
    );
}

#[test]
fn build_preserved_join_uses_common_batch_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);

    let before = native_batch_hits();

    let _ = rows_of(&engine, "SELECT b.id, p.id FROM b LEFT JOIN p ON b.k = p.k");
    let after = native_batch_hits();
    assert!(
        after > before,
        "expected the common batch pipeline, got before={before} after={after}"
    );
}
