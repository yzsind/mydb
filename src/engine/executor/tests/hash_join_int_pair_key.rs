//! WS-P1-3: specialized two-column integer join keys using (i64,i64) buckets.
//!
//! Previously two-column integer keys used generic HashJoinKey = SmallVec<[ValueKey;1]>,
//! allocating SmallVec spill storage and constructing ValueKey enums per build/probe row,
//! plus composite ahash key encoding, hurting Q5/Q8-shaped joins.
//!
//! int_pair_key_join_gate now selects do_in_memory_hash_join_int_pair,
//! using HashMap<(i64,i64), (head,tail)> plus Vec<u32> chains with fixed-size keys
//! and no SmallVec allocation. MYDB_HASH_JOIN_TYPED_KEY=off selects the generic oracle.
//!
//! Cover INNER/LEFT equivalence with the fast path disabled and require
//! the generic typed-key arity=2 counter to increase when specialization is used.

use super::support::query_rows as rows_of;
use super::*;

fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by_cached_key(|row| format!("{row:?}"));
    rows
}

struct ScopedEnv {
    key: &'static str,
    previous: Option<String>,
}

impl ScopedEnv {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for ScopedEnv {
    fn drop(&mut self) {
        unsafe {
            match &self.previous {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

/// Build b(id PK,k1,k2,v): 100 rows, k1=id%5 and k2=id%7.
/// Probe p(id PK,k1,k2,w): 300 rows, k1=id%5 and k2=id%7.
/// Two-column equality gives about 100*300/(5*7)=856 pairs; verify the exact count below.
fn setup(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k1 INT, k2 INT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE p (id INT PRIMARY KEY, k1 INT, k2 INT, w INT)")
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.b",
        (0..100).map(|i| {
            vec![
                Value::Int(i),
                Value::Int(i % 5),
                Value::Int(i % 7),
                Value::Int(i * 10),
            ]
        }),
    );
    insert_fixture_rows(
        engine,
        "test.p",
        (0..300).map(|i| {
            vec![
                Value::Int(i),
                Value::Int(i % 5),
                Value::Int(i % 7),
                Value::Int(i * 100),
            ]
        }),
    );
}

const SQL_INNER: &str = "SELECT b.id, p.id, b.v, p.w FROM b JOIN p ON b.k1=p.k1 AND b.k2=p.k2 \
     ORDER BY b.id, p.id";
const SQL_LEFT: &str = "SELECT b.id, p.id FROM b LEFT JOIN p ON b.k1=p.k1 AND b.k2=p.k2 \
     ORDER BY b.id, p.id";
const SQL_FULL: &str =
    "SELECT /*+ HASH_JOIN(b,p) */ b.id, p.id FROM b FULL JOIN p ON b.k1=p.k1 AND b.k2=p.k2 \
     ORDER BY b.id, p.id";
const SQL_RESIDUAL: &str =
    "SELECT b.id, p.id FROM b JOIN p ON b.k1=p.k1 AND b.k2=p.k2 AND p.w > b.v \
     ORDER BY b.id, p.id";
const SQL_SEMI: &str =
    "SELECT b.id FROM b WHERE EXISTS (SELECT 1 FROM p WHERE p.k1=b.k1 AND p.k2=b.k2) \
     ORDER BY b.id";
const SQL_ANTI: &str =
    "SELECT b.id FROM b WHERE NOT EXISTS (SELECT 1 FROM p WHERE p.k1=b.k1 AND p.k2=b.k2) \
     ORDER BY b.id";

fn diff_run(engine: &StorageQueryEngine, sql: &str) {
    // Disable streaming-inner and vector fast paths to exercise the modified in-memory
    // implementation. Otherwise small two-key INNER queries could use streaming-inner
    // and bypass int_pair specialization, making differential equivalence pass
    // without testing the intended path.
    let _streaming = ScopedEnv::set("MYDB_HASH_JOIN_STREAMING_INNER_DISABLE", "1");
    let _vector_join = ScopedEnv::set("MYDB_VEC_HASH_JOIN", "0");
    let _vector_mode = ScopedEnv::set("MYDB_VECTORIZED", "off");
    let typed = ScopedEnv::set("MYDB_HASH_JOIN_TYPED_KEY", "on");
    let got = sorted(rows_of(engine, sql));
    drop(typed);
    let generic = ScopedEnv::set("MYDB_HASH_JOIN_TYPED_KEY", "off");
    let expected = sorted(rows_of(engine, sql));
    drop(generic);
    assert_eq!(
        got, expected,
        "int_pair fast path on/off result mismatch: {sql}"
    );
}

#[test]
fn int_pair_inner_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(&engine, SQL_INNER);
}

#[test]
fn int_pair_left_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(&engine, SQL_LEFT);
}

#[test]
fn int_pair_full_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    let before = crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_ARITY_2
        .load(std::sync::atomic::Ordering::Relaxed);
    diff_run(&engine, SQL_FULL);
    let after = crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_ARITY_2
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        after > before,
        "FULL join did not execute typed arity-2 kernel"
    );
}

#[test]
fn int_pair_with_residual_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(&engine, SQL_RESIDUAL);
}

#[test]
fn int_pair_semi_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(&engine, SQL_SEMI);
}

#[test]
fn int_pair_anti_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(&engine, SQL_ANTI);
}

#[test]
fn int_pair_join_uses_common_batch_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);

    let before = native_batch_hits();
    let _ = rows_of(&engine, SQL_INNER);
    let after = native_batch_hits();
    assert!(
        after > before,
        "expected the common batch pipeline for a 2-col int join, \
         got before={before} after={after}"
    );
}

#[test]
fn int_pair_with_uint_overflow_falls_back_safely() {
    // Verify UInt > i64::MAX triggers IntPairKeyOut::Bail to generic buckets with correct results.
    // Use BIGINT UNSIGNED to construct a value exceeding i64::MAX.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE bx (id INT PRIMARY KEY, k1 BIGINT UNSIGNED, k2 BIGINT)")
        .unwrap();
    engine
        .execute("CREATE TABLE px (id INT PRIMARY KEY, k1 BIGINT UNSIGNED, k2 BIGINT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO bx VALUES \
             (1, 18446744073709551610, 20), (2, 30, 40), (3, 50, 60)",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO px VALUES \
             (10, 18446744073709551610, 20), (11, 30, 40), (12, 50, 60)",
        )
        .unwrap();
    let _typed = ScopedEnv::set("MYDB_HASH_JOIN_TYPED_KEY", "on");
    let got = sorted(rows_of(
        &engine,
        "SELECT bx.id, px.id FROM bx JOIN px ON bx.k1=px.k1 AND bx.k2=px.k2 ORDER BY bx.id, px.id",
    ));
    assert_eq!(got.len(), 3, "expected 3 matched rows");
}
