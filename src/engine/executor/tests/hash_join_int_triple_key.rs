//! Typed-key regression for integer join keys of arbitrary arity.
//!
//! Previously integer keys of arity three or more bypassed single/pair specialization,
//! using generic HashJoinKey = SmallVec<[ValueKey;1]> buckets with one spill allocation
//! and N ValueKey enum constructions per build/probe row.
//!
//! int_arr_key_join_gate::<N> now selects do_in_memory_hash_join_int_arr::<N>,
//! using HashMap<[i64; N], (head,tail)> and Vec<u32> chains with fixed-size keys
//! and no SmallVec allocation. Reuse MYDB_HASH_JOIN_TYPED_KEY; disabling it disables
//! all single/pair/array specializations and provides a differential oracle.
//!
//! Cover INNER, LEFT, chained LEFT (q78 shape), NULL keys, and residuals,
//! checking equivalence with disabled fast paths and generic typed-key arity counters.
//! UInt > i64::MAX must still correctly fall back through IntArrKeyOut::Bail.

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

/// b(id PK, k1, k2, k3, v):k1=id%3, k2=id%5, k3=id%7。
/// p(id PK,k1,k2,k3,w) uses the same shape; three-key matches repeat every lcm(3,5,7)=105 rows.
/// Include one k2 IS NULL row to exercise IntArrKeyOut::Null.
fn setup(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, k1 INT, k2 INT, k3 INT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE p (id INT PRIMARY KEY, k1 INT, k2 INT, k3 INT, w INT)")
        .unwrap();
    insert_fixture_rows(
        engine,
        "test.b",
        (0..120).map(|i| {
            vec![
                Value::Int(i),
                Value::Int(i % 3),
                Value::Int(i % 5),
                Value::Int(i % 7),
                Value::Int(i * 10),
            ]
        }),
    );
    insert_fixture_rows(
        engine,
        "test.p",
        (0..360).map(|i| {
            vec![
                Value::Int(i),
                Value::Int(i % 3),
                Value::Int(i % 5),
                Value::Int(i % 7),
                Value::Int(i * 100),
            ]
        }),
    );
    engine
        .execute("INSERT INTO b VALUES (200, 1, NULL, 2, 9999)")
        .unwrap();
    engine
        .execute("INSERT INTO p VALUES (400, 1, NULL, 2, 9999)")
        .unwrap();
}

const SQL_INNER: &str =
    "SELECT b.id, p.id, b.v, p.w FROM b JOIN p ON b.k1=p.k1 AND b.k2=p.k2 AND b.k3=p.k3 \
     ORDER BY b.id, p.id";
const SQL_LEFT: &str =
    "SELECT b.id, p.id FROM b LEFT JOIN p ON b.k1=p.k1 AND b.k2=p.k2 AND b.k3=p.k3 \
     ORDER BY b.id, p.id";
const SQL_FULL: &str =
    "SELECT /*+ HASH_JOIN(b,p) */ b.id, p.id FROM b FULL JOIN p ON b.k1=p.k1 AND b.k2=p.k2 AND b.k3=p.k3 \
     ORDER BY b.id, p.id";
const SQL_RESIDUAL: &str =
    "SELECT b.id, p.id FROM b JOIN p ON b.k1=p.k1 AND b.k2=p.k2 AND b.k3=p.k3 AND p.w > b.v \
     ORDER BY b.id, p.id";
const SQL_SEMI: &str = "SELECT b.id FROM b \
     WHERE EXISTS (SELECT 1 FROM p WHERE p.k1=b.k1 AND p.k2=b.k2 AND p.k3=b.k3) ORDER BY b.id";
const SQL_ANTI: &str = "SELECT b.id FROM b \
     WHERE NOT EXISTS (SELECT 1 FROM p WHERE p.k1=b.k1 AND p.k2=b.k2 AND p.k3=b.k3) ORDER BY b.id";

/// A spine LEFT JOIN a LEFT JOIN b uses the same three-column integer key at both levels.
const SQL_LEFT_CHAIN: &str = "SELECT s.id, a.id, c.id \
     FROM b s LEFT JOIN b a ON s.k1=a.k1 AND s.k2=a.k2 AND s.k3=a.k3 \
     LEFT JOIN p c ON s.k1=c.k1 AND s.k2=c.k2 AND s.k3=c.k3 \
     WHERE s.id < 40 ORDER BY s.id, a.id, c.id LIMIT 100";

fn diff_run(engine: &StorageQueryEngine, sql: &str) {
    // Disable streaming-inner and vector fast paths to exercise the modified in-memory
    // Exercise the intended implementation path.
    let _g_si = ScopedEnv::set("MYDB_HASH_JOIN_STREAMING_INNER_DISABLE", "1");
    let _g_vec = ScopedEnv::set("MYDB_VEC_HASH_JOIN", "0");
    let _g_mode = ScopedEnv::set("MYDB_VECTORIZED", "off");

    let _g_on = ScopedEnv::set("MYDB_HASH_JOIN_TYPED_KEY", "on");
    let got = sorted(rows_of(engine, sql));
    drop(_g_on);

    let _g_off = ScopedEnv::set("MYDB_HASH_JOIN_TYPED_KEY", "off");
    let expected = sorted(rows_of(engine, sql));
    assert_eq!(
        got, expected,
        "int_arr(3-col) fast path on/off result mismatch: {sql}"
    );
}

#[test]
fn int_triple_inner_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(&engine, SQL_INNER);
}

#[test]
fn int_triple_left_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(&engine, SQL_LEFT);
}

#[test]
fn int_triple_full_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    let before = crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_ARITY_3
        .load(std::sync::atomic::Ordering::Relaxed);
    diff_run(&engine, SQL_FULL);
    let after = crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_ARITY_3
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        after > before,
        "FULL join did not execute typed arity-3 kernel"
    );
}

#[test]
fn int_triple_left_chain_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(&engine, SQL_LEFT_CHAIN);
}

#[test]
fn int_triple_residual_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(&engine, SQL_RESIDUAL);
}

#[test]
fn int_triple_semi_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(&engine, SQL_SEMI);
}

#[test]
fn int_triple_anti_equivalent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);
    diff_run(&engine, SQL_ANTI);
}

#[test]
fn int_triple_join_uses_common_batch_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup(&engine);

    let before = native_batch_hits();
    let _ = rows_of(&engine, SQL_INNER);
    let after = native_batch_hits();
    assert!(
        after > before,
        "expected the common batch pipeline for a 3-col int join, \
         got before={before} after={after}"
    );
}

#[test]
fn int_quad_four_col_equivalent() {
    // Four-column integer keys also exercise int_arr_key_join_gate::<4> correctly.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE b4 (id INT PRIMARY KEY, k1 INT, k2 INT, k3 INT, k4 INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE p4 (id INT PRIMARY KEY, k1 INT, k2 INT, k3 INT, k4 INT)")
        .unwrap();
    let mut s = String::from("INSERT INTO b4 VALUES ");
    for i in 0..80 {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            "({}, {}, {}, {}, {})",
            i,
            i % 2,
            i % 3,
            i % 5,
            i % 7
        ));
    }
    engine.execute(&s).unwrap();
    let mut s = String::from("INSERT INTO p4 VALUES ");
    for i in 0..210 {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            "({}, {}, {}, {}, {})",
            i,
            i % 2,
            i % 3,
            i % 5,
            i % 7
        ));
    }
    engine.execute(&s).unwrap();
    let sql = "SELECT b4.id, p4.id FROM b4 JOIN p4 \
         ON b4.k1=p4.k1 AND b4.k2=p4.k2 AND b4.k3=p4.k3 AND b4.k4=p4.k4 ORDER BY b4.id, p4.id";
    let _g_si = ScopedEnv::set("MYDB_HASH_JOIN_STREAMING_INNER_DISABLE", "1");
    let _g_vec = ScopedEnv::set("MYDB_VEC_HASH_JOIN", "0");
    let _g_off = ScopedEnv::set("MYDB_HASH_JOIN_TYPED_KEY", "off");
    let expected = sorted(rows_of(&engine, sql));
    drop(_g_off);
    let _g_on = ScopedEnv::set("MYDB_HASH_JOIN_TYPED_KEY", "on");
    let got = sorted(rows_of(&engine, sql));
    assert_eq!(got, expected, "int_arr(4-col) fast path mismatch");
}

#[test]
fn int_triple_with_uint_overflow_falls_back_safely() {
    // BIGINT UNSIGNED > i64::MAX triggers IntArrKeyOut::Bail to generic buckets without changing results.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE bx (id INT PRIMARY KEY, k1 BIGINT UNSIGNED, k2 BIGINT, k3 BIGINT)")
        .unwrap();
    engine
        .execute("CREATE TABLE px (id INT PRIMARY KEY, k1 BIGINT UNSIGNED, k2 BIGINT, k3 BIGINT)")
        .unwrap();
    engine
        .execute("INSERT INTO bx VALUES (1, 18446744073709551610, 20, 7), (2, 30, 40, 8)")
        .unwrap();
    engine
        .execute("INSERT INTO px VALUES (10, 18446744073709551610, 20, 7), (11, 30, 40, 8)")
        .unwrap();
    let _g_on = ScopedEnv::set("MYDB_HASH_JOIN_TYPED_KEY", "on");
    let got = sorted(rows_of(
        &engine,
        "SELECT bx.id, px.id FROM bx JOIN px ON bx.k1=px.k1 AND bx.k2=px.k2 AND bx.k3=px.k3 \
         ORDER BY bx.id, px.id",
    ));
    assert_eq!(
        got.len(),
        2,
        "expected 2 matched rows (incl. the >i64::MAX key)"
    );
}

#[test]
fn dynamic_integer_key_supports_five_and_eight_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE b8 (id INT PRIMARY KEY, k1 INT, k2 INT, k3 INT, k4 INT, \
             k5 INT, k6 INT, k7 INT, k8 INT)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE p8 (id INT PRIMARY KEY, k1 INT, k2 INT, k3 INT, k4 INT, \
             k5 INT, k6 INT, k7 INT, k8 INT)",
        )
        .unwrap();
    let mut build = String::from("INSERT INTO b8 VALUES ");
    let mut probe = String::from("INSERT INTO p8 VALUES ");
    for i in 0..80 {
        if i > 0 {
            build.push(',');
            probe.push(',');
        }
        let keys = (1..=8)
            .map(|column| ((i * (column + 2)) % (column * 7 + 3)).to_string())
            .collect::<Vec<_>>()
            .join(",");
        build.push_str(&format!("({i},{keys})"));
        probe.push_str(&format!("({},{keys})", i + 1000));
    }
    engine.execute(&build).unwrap();
    engine.execute(&probe).unwrap();

    let queries = [
        "SELECT /*+ HASH_JOIN(b8,p8) */ b8.id,p8.id FROM b8 FULL JOIN p8 ON b8.k1=p8.k1 AND b8.k2=p8.k2 \
         AND b8.k3=p8.k3 AND b8.k4=p8.k4 AND b8.k5=p8.k5 ORDER BY b8.id,p8.id",
        "SELECT /*+ HASH_JOIN(b8,p8) */ b8.id,p8.id FROM b8 FULL JOIN p8 ON b8.k1=p8.k1 AND b8.k2=p8.k2 \
         AND b8.k3=p8.k3 AND b8.k4=p8.k4 AND b8.k5=p8.k5 AND b8.k6=p8.k6 \
         AND b8.k7=p8.k7 AND b8.k8=p8.k8 ORDER BY b8.id,p8.id",
    ];
    let _streaming = ScopedEnv::set("MYDB_HASH_JOIN_STREAMING_INNER_DISABLE", "1");
    let _vector = ScopedEnv::set("MYDB_VEC_HASH_JOIN", "0");
    let _mode = ScopedEnv::set("MYDB_VECTORIZED", "off");
    let before = crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_ARITY_5_PLUS
        .load(std::sync::atomic::Ordering::Relaxed);
    for sql in queries {
        let _off = ScopedEnv::set("MYDB_HASH_JOIN_TYPED_KEY", "off");
        let expected = rows_of(&engine, sql);
        drop(_off);
        let _on = ScopedEnv::set("MYDB_HASH_JOIN_TYPED_KEY", "on");
        let actual = rows_of(&engine, sql);
        assert_eq!(actual, expected, "dynamic typed-key mismatch: {sql}");
    }
    let after = crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_ARITY_5_PLUS
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        after >= before + 2,
        "dynamic typed-key kernel was not engaged"
    );
}
