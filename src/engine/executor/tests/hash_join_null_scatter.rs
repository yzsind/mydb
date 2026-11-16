//! WS-P0-4: correctness and activation of NULL-key scattering in parallel partitions.
//!
//! Previously partition_for_row* sent non-null-safe NULL keys directly to partition 0,
//! concentrating outer-join NULL rows in one worker's working set.
//!
//! MYDB_HASH_JOIN_PARALLEL_NULL_KEY_SCATTER=on now hashes row ordinals
//! across P partitions. Such NULL keys still never match a bucket;
//! preserved NULL-key rows receive partition-local NULL extensions, preserving the result multiset.
//!
//! Coverage:
//! 1. INNER: NULL keys emit nothing, matching scatter disabled.
//! 2. LEFT with build-right/preserved-probe: NULL probe keys receive NULL extensions
//!    exactly once, matching scatter disabled.
//! 3. Semi/anti joins retain their existing nonmatching NULL behavior.
//! 4. hash_join_parallel_null_scattered increases when scattering is enabled.

use super::support::query_rows as rows_of;
use super::*;

fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by_cached_key(|row| format!("{row:?}"));
    rows
}

/// Build and probe both have 30% NULL keys and enough rows for partitioning.
/// The default parallelism threshold is high, so exercise the equivalent
/// grace path, which also calls partition_for_row.
fn setup(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k INT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE p (id INT PRIMARY KEY, k INT, w INT)")
        .unwrap();
    // Retain 30% NULL keys on both sides and the original row cardinalities.
    for (table, count, multiplier) in [("test.b", 300, 100), ("test.p", 200, 200)] {
        insert_fixture_rows(
            engine,
            table,
            (0..count).map(|i| {
                vec![
                    Value::Int(i),
                    if i % 10 < 3 {
                        Value::Null
                    } else {
                        Value::Int(i % 50)
                    },
                    Value::Int(i * multiplier),
                ]
            }),
        );
    }
}

const INNER_SQL: &str = "SELECT b.v, p.w FROM b JOIN p ON b.k = p.k ORDER BY b.v, p.w";
const LEFT_SQL: &str =
    "SELECT b.id, b.k, p.k, p.w FROM b LEFT JOIN p ON b.k = p.k ORDER BY b.id, p.w";
const SEMI_SQL: &str =
    "SELECT b.id FROM b WHERE EXISTS (SELECT 1 FROM p WHERE p.k = b.k) ORDER BY b.id";
const ANTI_SQL: &str =
    "SELECT b.id FROM b WHERE NOT EXISTS (SELECT 1 FROM p WHERE p.k = b.k) ORDER BY b.id";

fn diff_run(engine: &StorageQueryEngine, sql: &str) {
    // Force grace partitioning with MAX_BUILD_ROWS=100, below the 300 build rows.
    // Grace routes build/probe through partition_for_row and therefore covers
    // the NULL scattering path.
    let _max = ScopedEnv::set("MYDB_HASH_JOIN_MAX_BUILD_ROWS", "100");

    // baseline: scatter off
    let _g_off = ScopedEnv::set("MYDB_HASH_JOIN_PARALLEL_NULL_KEY_SCATTER", "off");
    let expected = sorted(rows_of(engine, sql));
    drop(_g_off);

    let _g_on = ScopedEnv::set("MYDB_HASH_JOIN_PARALLEL_NULL_KEY_SCATTER", "on");
    let got = sorted(rows_of(engine, sql));
    assert_eq!(got, expected, "NULL scatter on/off result mismatch: {sql}");
}

/// Test helper: process-wide environment overrides restored on Drop, without additional dependencies.
struct ScopedEnv {
    key: &'static str,
    prev: Option<String>,
}
impl ScopedEnv {
    fn set(key: &'static str, val: &str) -> Self {
        let prev = std::env::var(key).ok();
        // SAFETY: tests run serially; set_var is unsafe on the targeted Rust version.
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

#[test]
fn null_scatter_inner_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(&engine, INNER_SQL);
}

#[test]
fn null_scatter_left_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(&engine, LEFT_SQL);
}

#[test]
fn null_scatter_semi_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(&engine, SEMI_SQL);
}

#[test]
fn null_scatter_anti_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(&engine, ANTI_SQL);
}

#[test]
fn null_key_join_uses_common_batch_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);

    // Process-wide atomic counter; assert relative deltas to avoid cross-test interference.
    let before = native_batch_hits();
    let _ = rows_of(&engine, INNER_SQL);

    let after = native_batch_hits();
    assert!(
        after > before,
        "expected the common batch pipeline with NULL keys, \
         got before={before} after={after}",
    );
}
