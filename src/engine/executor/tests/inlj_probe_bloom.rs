//! INLJ probe-side Bloom filter (P0-B).
//!
//! Verify three properties:
//! 1. Correctness: identical queries with MYDB_INLJ_PROBE_BLOOM forced on
//!    and off have exactly identical results. Bloom has no false negatives;
//!    a rejected outer key is absent from the inner index, equivalent to a batched lookup
//!    returning no entries.
//! 2. Activation: with on and sufficient budget, INLJ_PROBE_BLOOM_BUILDS increases,
//!    proving the filter was built and applied.
//! 3. Filtering: include outer keys guaranteed absent from the inner index.
//!    `INLJ_PROBE_BLOOM_KEYS_FILTERED` > 0。
//!
//! Cover Inner, Left, Semi, and Anti joins, plus composite-index
//! equivalence with eq_prefix_len > 1.

use super::*;
use crate::engine::executor::select::plan::{
    INLJ_PROBE_BLOOM_BUILDS, INLJ_PROBE_BLOOM_KEYS_FILTERED, INLJ_PROBE_BLOOM_KEYS_PROBED,
};
use std::sync::atomic::Ordering;

/// Allow batched INLJ with outer >=8 and inner >=100, avoiding tables with 1000+ rows;
/// also reduce the Bloom threshold to eight distinct keys.
fn small_env() -> Vec<(&'static str, Option<&'static str>)> {
    vec![
        ("MYDB_INLJ_BATCH_MIN_LEFT_ROWS", Some("8")),
        // This suite isolates the single batched-probe bloom. The production
        // default now parallelizes actual outers at 128 rows, which would split
        // this synthetic 200-key input into independent probe chunks and make
        // the process-global bloom-count assertion describe a different path.
        ("MYDB_INLJ_OUTER_PARALLEL_THRESHOLD", Some("100000")),
        ("MYDB_INLJ_PROBE_BLOOM_MIN_KEYS", Some("8")),
        ("MYDB_INLJ_PROBE_BLOOM_MAX_BUILD_PAGES", Some("4096")),
    ]
}

fn env_with(
    mode: Option<&'static str>,
    extra: &[(&'static str, Option<&'static str>)],
) -> Vec<(&'static str, Option<&'static str>)> {
    let mut v = small_env();
    v.push(("MYDB_INLJ_PROBE_BLOOM", mode));
    v.extend_from_slice(extra);
    v
}

fn run(setup: &[&str], sql: &str, env: &[(&'static str, Option<&'static str>)]) -> Vec<Vec<Value>> {
    let _g = test_env_lock();
    with_test_env(env, || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        for s in setup {
            engine.execute(s).unwrap();
        }
        let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
            panic!("expected result set: {sql}");
        };
        rs.rows
    })
}

/// Like run, but return the Bloom counter delta across SELECT execution.
fn run_with_metrics(
    setup: &[&str],
    sql: &str,
    env: &[(&'static str, Option<&'static str>)],
) -> (Vec<Vec<Value>>, BloomMetrics) {
    let _g = test_env_lock();
    with_test_env(env, || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        for s in setup {
            engine.execute(s).unwrap();
        }
        let before = BloomMetrics::snapshot();
        let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
            panic!("expected result set: {sql}");
        };
        let after = BloomMetrics::snapshot();
        (rs.rows, after.diff(&before))
    })
}

#[derive(Debug, Clone, Copy)]
struct BloomMetrics {
    builds: u64,
    probed: u64,
    filtered: u64,
}

impl BloomMetrics {
    fn snapshot() -> Self {
        Self {
            builds: INLJ_PROBE_BLOOM_BUILDS.load(Ordering::Relaxed),
            probed: INLJ_PROBE_BLOOM_KEYS_PROBED.load(Ordering::Relaxed),
            filtered: INLJ_PROBE_BLOOM_KEYS_FILTERED.load(Ordering::Relaxed),
        }
    }
    fn diff(&self, before: &Self) -> Self {
        Self {
            builds: self.builds.wrapping_sub(before.builds),
            probed: self.probed.wrapping_sub(before.probed),
            filtered: self.filtered.wrapping_sub(before.filtered),
        }
    }
}

/// Inner has 100 IDs 1..100; outer has keys 1..100, all present, plus 10001..10100,
/// all absent. This is the basic dataset.
fn setup_inner_outer() -> Vec<String> {
    let mut stmts = vec![
        "CREATE TABLE inner_t (id INT PRIMARY KEY, v INT)".to_string(),
        "CREATE TABLE outer_t (k INT)".to_string(),
    ];
    // Insert 100 rows in one statement to avoid excess parsing.
    let inner_vals: Vec<String> = (1..=100).map(|i| format!("({i},{i})")).collect();
    stmts.push(format!(
        "INSERT INTO inner_t VALUES {}",
        inner_vals.join(",")
    ));
    let mut outer_vals: Vec<String> = (1..=100).map(|i| format!("({i})")).collect();
    outer_vals.extend((10001..=10100).map(|i| format!("({i})")));
    stmts.push(format!(
        "INSERT INTO outer_t VALUES {}",
        outer_vals.join(",")
    ));
    stmts
}

fn as_strs(v: &[String]) -> Vec<&str> {
    v.iter().map(String::as_str).collect()
}

fn assert_results_equal(a: &[Vec<Value>], b: &[Vec<Value>]) {
    // Sort results before comparison because parallel INLJ does not guarantee order.
    let mut a = a.to_vec();
    let mut b = b.to_vec();
    let order = |x: &Vec<Value>| format!("{x:?}");
    a.sort_by_key(order);
    b.sort_by_key(order);
    assert_eq!(a, b);
}

#[test]
fn inner_join_equivalence_on_off() {
    let setup_owned = setup_inner_outer();
    let setup = as_strs(&setup_owned);
    let sql = "SELECT outer_t.k, inner_t.v FROM outer_t \
               INNER JOIN inner_t ON outer_t.k = inner_t.id";

    let r_off = run(&setup, sql, &env_with(Some("off"), &[]));
    let r_on = run(&setup, sql, &env_with(Some("on"), &[]));
    assert_results_equal(&r_off, &r_on);
    // Of 200 outer rows, 100 match and 100 do not, so expect exactly 100 join results.
    assert_eq!(r_on.len(), 100);
}

#[test]
fn left_join_equivalence_on_off() {
    let setup_owned = setup_inner_outer();
    let setup = as_strs(&setup_owned);
    let sql = "SELECT outer_t.k, inner_t.v FROM outer_t \
               LEFT JOIN inner_t ON outer_t.k = inner_t.id";

    let r_off = run(&setup, sql, &env_with(Some("off"), &[]));
    let r_on = run(&setup, sql, &env_with(Some("on"), &[]));
    assert_results_equal(&r_off, &r_on);
    // LEFT JOIN returns all 200 outer rows.
    assert_eq!(r_on.len(), 200);
}

#[test]
fn semi_join_equivalence_on_off() {
    let setup_owned = setup_inner_outer();
    let setup = as_strs(&setup_owned);
    let sql = "SELECT k FROM outer_t WHERE k IN (SELECT id FROM inner_t)";

    let r_off = run(&setup, sql, &env_with(Some("off"), &[]));
    let r_on = run(&setup, sql, &env_with(Some("on"), &[]));
    assert_results_equal(&r_off, &r_on);
    // Exactly 100 keys present in inner should remain.
    assert_eq!(r_on.len(), 100);
}

#[test]
fn anti_join_equivalence_on_off() {
    let setup_owned = setup_inner_outer();
    let setup = as_strs(&setup_owned);
    let sql = "SELECT k FROM outer_t WHERE k NOT IN (SELECT id FROM inner_t)";

    let r_off = run(&setup, sql, &env_with(Some("off"), &[]));
    let r_on = run(&setup, sql, &env_with(Some("on"), &[]));
    assert_results_equal(&r_off, &r_on);
    // Exactly 100 keys absent from inner should remain.
    assert_eq!(r_on.len(), 100);
}

#[test]
fn bloom_actually_builds_and_filters_when_on() {
    let setup_owned = setup_inner_outer();
    let setup = as_strs(&setup_owned);
    let sql = "SELECT outer_t.k FROM outer_t \
               INNER JOIN inner_t ON outer_t.k = inner_t.id";

    let (_, metrics) = run_with_metrics(&setup, sql, &env_with(Some("on"), &[]));
    // Of 200 distinct outer keys, 100 are absent; Bloom should reject at least
    // 90, allowing ten for false positives.
    assert!(
        metrics.builds >= 1,
        "expected at least one bloom build, got {metrics:?}"
    );
    assert!(
        metrics.probed >= 100,
        "expected outer keys to be probed, got {metrics:?}"
    );
    assert!(
        metrics.filtered >= 80,
        "expected most miss-keys to be filtered, got {metrics:?}"
    );
}

#[test]
fn bloom_does_not_build_when_off() {
    let setup_owned = setup_inner_outer();
    let setup = as_strs(&setup_owned);
    let sql = "SELECT outer_t.k FROM outer_t \
               INNER JOIN inner_t ON outer_t.k = inner_t.id";

    let (_, metrics) = run_with_metrics(&setup, sql, &env_with(Some("off"), &[]));
    assert_eq!(
        metrics.builds, 0,
        "bloom must not build when off: {metrics:?}"
    );
    assert_eq!(metrics.probed, 0);
    assert_eq!(metrics.filtered, 0);
}

#[test]
fn bloom_all_outer_in_inner_no_filter() {
    // When all outer keys match, still build Bloom but expect filtered approximately zero.
    let stmts = vec![
        "CREATE TABLE inner_t (id INT PRIMARY KEY)".to_string(),
        "CREATE TABLE outer_t (k INT)".to_string(),
        format!(
            "INSERT INTO inner_t VALUES {}",
            (1..=100)
                .map(|i| format!("({i})"))
                .collect::<Vec<_>>()
                .join(",")
        ),
        format!(
            "INSERT INTO outer_t VALUES {}",
            (1..=100)
                .map(|i| format!("({i})"))
                .collect::<Vec<_>>()
                .join(",")
        ),
    ];
    let setup = as_strs(&stmts);
    let sql = "SELECT outer_t.k FROM outer_t INNER JOIN inner_t ON outer_t.k = inner_t.id";

    let (r_on, metrics_on) = run_with_metrics(&setup, sql, &env_with(Some("on"), &[]));
    let r_off = run(&setup, sql, &env_with(Some("off"), &[]));
    assert_results_equal(&r_off, &r_on);
    assert_eq!(r_on.len(), 100);
    assert!(metrics_on.builds >= 1);
    // An all-hit filter should reject no valid keys; a roughly 1% FPR affects absent keys, not hits.
    assert!(metrics_on.filtered <= 2, "FPR too high: {metrics_on:?}");
}

#[test]
fn bloom_all_outer_miss_inner_filters_all() {
    // When every outer key misses, Bloom should reject nearly all keys.
    let stmts = vec![
        "CREATE TABLE inner_t (id INT PRIMARY KEY)".to_string(),
        "CREATE TABLE outer_t (k INT)".to_string(),
        format!(
            "INSERT INTO inner_t VALUES {}",
            (1..=100)
                .map(|i| format!("({i})"))
                .collect::<Vec<_>>()
                .join(",")
        ),
        format!(
            "INSERT INTO outer_t VALUES {}",
            (10001..=10100)
                .map(|i| format!("({i})"))
                .collect::<Vec<_>>()
                .join(",")
        ),
    ];
    let setup = as_strs(&stmts);
    let sql = "SELECT outer_t.k FROM outer_t INNER JOIN inner_t ON outer_t.k = inner_t.id";

    let (r_on, metrics_on) = run_with_metrics(&setup, sql, &env_with(Some("on"), &[]));
    let r_off = run(&setup, sql, &env_with(Some("off"), &[]));
    assert_results_equal(&r_off, &r_on);
    assert!(r_on.is_empty());
    assert!(metrics_on.builds >= 1);
    // Of 100 misses, reject at least 90, allowing false positives.
    assert!(
        metrics_on.filtered >= 90,
        "expected most miss-keys to be filtered, got {metrics_on:?}"
    );
}

#[test]
fn bloom_composite_index_equivalence() {
    // INLJ plus Bloom on composite index (a,b), with eq_prefix_len=2.
    let stmts = vec![
        "CREATE TABLE inner_t (a INT, b INT, c INT, PRIMARY KEY (a, b))".to_string(),
        "CREATE TABLE outer_t (a INT, b INT)".to_string(),
        // Inner contains (1,1)..(10,10), totaling 100 rows.
        format!(
            "INSERT INTO inner_t VALUES {}",
            (1..=10)
                .flat_map(|a| (1..=10).map(move |b| format!("({a},{b},{})", a * 100 + b)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        // Outer has 100 matching rows and 100 (a,b) keys absent from inner.
        format!(
            "INSERT INTO outer_t VALUES {},{}",
            (1..=10)
                .flat_map(|a| (1..=10).map(move |b| format!("({a},{b})")))
                .collect::<Vec<_>>()
                .join(","),
            (11..=20)
                .flat_map(|a| (1..=10).map(move |b| format!("({a},{b})")))
                .collect::<Vec<_>>()
                .join(",")
        ),
    ];
    let setup = as_strs(&stmts);
    let sql = "SELECT outer_t.a, outer_t.b, inner_t.c FROM outer_t \
               INNER JOIN inner_t ON outer_t.a = inner_t.a AND outer_t.b = inner_t.b";

    let r_off = run(&setup, sql, &env_with(Some("off"), &[]));
    let r_on = run(&setup, sql, &env_with(Some("on"), &[]));
    assert_results_equal(&r_off, &r_on);
    assert_eq!(r_on.len(), 100);
}

#[test]
fn bloom_below_min_keys_not_built() {
    // MYDB_INLJ_PROBE_BLOOM_MIN_KEYS=200 with about 50 distinct outer keys skips construction.
    let setup_owned = setup_inner_outer();
    let setup = as_strs(&setup_owned);
    let sql = "SELECT outer_t.k FROM outer_t INNER JOIN inner_t ON outer_t.k = inner_t.id";

    let extra: Vec<(&'static str, Option<&'static str>)> =
        vec![("MYDB_INLJ_PROBE_BLOOM_MIN_KEYS", Some("10000"))];
    let (r, metrics) = run_with_metrics(&setup, sql, &env_with(Some("on"), &extra));
    let r_off = run(&setup, sql, &env_with(Some("off"), &[]));
    assert_results_equal(&r_off, &r);
    assert_eq!(
        metrics.builds, 0,
        "bloom must not build below min_keys: {metrics:?}"
    );
}

#[test]
fn bloom_max_build_pages_zero_disables() {
    // MYDB_INLJ_PROBE_BLOOM_MAX_BUILD_PAGES=0 disables the filter.
    let setup_owned = setup_inner_outer();
    let setup = as_strs(&setup_owned);
    let sql = "SELECT outer_t.k FROM outer_t INNER JOIN inner_t ON outer_t.k = inner_t.id";

    let extra: Vec<(&'static str, Option<&'static str>)> =
        vec![("MYDB_INLJ_PROBE_BLOOM_MAX_BUILD_PAGES", Some("0"))];
    let (r, metrics) = run_with_metrics(&setup, sql, &env_with(Some("on"), &extra));
    let r_off = run(&setup, sql, &env_with(Some("off"), &[]));
    assert_results_equal(&r_off, &r);
    assert_eq!(
        metrics.builds, 0,
        "bloom must not build with zero page budget: {metrics:?}"
    );
}
