//! P0-A2 + P0-B regression: `SharedCteSelected` selection-based pushdown and
//! cross-reference hash-build cache must be invisible to query results.
//!
//! Two differential layers:
//!  * **A2**: a CTE referenced N times in a comma-join, each reference filtered
//!    on a distinct partition, must return the same rows as a baseline single
//!    reference filtered the same way (the Arc-COW selection path is just a
//!    rewrite of the per-reference deep-copy).
//!  * **B**: a CTE self-joined on a column key must return the same rows whether
//!    the hash build is cached cross-reference or built fresh per reference.
//!    We exercise the year_total self-join shape (4 references on customer_id,
//!    each filtered on a different (sale_type, dyear)).

use super::*;

fn extract_rows(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(r) => r.rows,
        other => panic!("expected result set, got {other:?}"),
    }
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    extract_rows(engine.execute(sql).unwrap())
}

fn sort_rows(mut v: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    v.sort_by_cached_key(|row| format!("{row:?}"));
    v
}

/// A2: 4-way self join on a CTE with distinct per-reference filters must
/// not change row content vs the legacy `Owned(kept)` deep-copy path.
#[test]
fn cte_n_way_self_join_with_distinct_filters_matches_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE seed ( \
                cust INT NOT NULL, \
                sale CHAR(1) NOT NULL, \
                dyear INT NOT NULL, \
                amt INT NOT NULL \
            )",
        )
        .unwrap();
    // 6 customers × 4 (sale, dyear) combinations = 24 rows.
    for cust in 1..=6 {
        for (sale, year) in [("s", 2001), ("s", 2002), ("w", 2001), ("w", 2002)] {
            engine
                .execute(&format!(
                    "INSERT INTO seed VALUES ({cust}, '{sale}', {year}, {})",
                    cust * 10 + year,
                ))
                .unwrap();
        }
    }

    let actual = sort_rows(rows(
        &engine,
        "WITH yt AS (SELECT cust, sale, dyear, amt FROM seed) \
         SELECT t1.cust, t1.amt, t2.amt, t3.amt, t4.amt \
         FROM yt t1, yt t2, yt t3, yt t4 \
         WHERE t1.cust = t2.cust AND t1.cust = t3.cust AND t1.cust = t4.cust \
           AND t1.sale = 's' AND t1.dyear = 2001 \
           AND t2.sale = 's' AND t2.dyear = 2002 \
           AND t3.sale = 'w' AND t3.dyear = 2001 \
           AND t4.sale = 'w' AND t4.dyear = 2002",
    ));

    // Each customer contributes exactly one row: (cust, cust*10+2001,
    // cust*10+2002, cust*10+2001, cust*10+2002).
    let expected: Vec<Vec<Value>> = (1..=6)
        .map(|c| {
            vec![
                Value::Int(c),
                Value::Int(c * 10 + 2001),
                Value::Int(c * 10 + 2002),
                Value::Int(c * 10 + 2001),
                Value::Int(c * 10 + 2002),
            ]
        })
        .collect();
    assert_eq!(
        actual, expected,
        "selection-based pushdown must preserve row content across 4 references"
    );
}

/// B: hash-build cache hit/miss paths must agree. We force a CTE self-join
/// that exercises the cache (right side = CTE-backed, simple-column key)
/// and compare against an Owned-rows baseline (right side = a regular
/// subquery whose rows aren't `SharedCte`).
#[test]
fn cte_self_join_with_hash_build_cache_matches_owned_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE base (k INT NOT NULL, v INT NOT NULL)")
        .unwrap();
    for k in 1..=20 {
        engine
            .execute(&format!("INSERT INTO base VALUES ({k}, {})", k * 7))
            .unwrap();
    }

    // CTE-backed self join: cache participates (cte_handle Some, simple
    // column key, Inner join).
    let cached = sort_rows(rows(
        &engine,
        "WITH c AS (SELECT k, v FROM base) \
         SELECT a.k, a.v, b.v FROM c a, c b WHERE a.k = b.k AND a.k <= 5",
    ));

    // Same join via inline derived tables (rows materialised as `Owned`):
    // the hash join goes through the original local-build path. Used as
    // an oracle.
    let oracle = sort_rows(rows(
        &engine,
        "SELECT a.k, a.v, b.v \
         FROM (SELECT k, v FROM base) a, (SELECT k, v FROM base) b \
         WHERE a.k = b.k AND a.k <= 5",
    ));

    assert_eq!(
        cached, oracle,
        "cross-reference shared hash build must produce the same rows as the local-build path"
    );
}

/// B: cache key uses column *positions*, so two references with different
/// aliases must hit the same cache slot. Re-runs the same SQL inside one
/// engine session twice (sharing the registry's CteStore) and asserts row
/// content stability — the second invocation hits the cache built by the
/// first within its statement, but a fresh statement gets a fresh registry
/// so this also exercises the from-scratch path. Both must agree.
#[test]
fn cte_hash_build_cache_repeated_invocation_is_stable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)")
        .unwrap();
    for k in 1..=12 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({k}, {k})"))
            .unwrap();
    }
    let q = "WITH c AS (SELECT k, v FROM t) \
             SELECT a.k FROM c a, c b, c d \
             WHERE a.k = b.k AND a.k = d.k AND a.v >= 4";
    let first = sort_rows(rows(&engine, q));
    let second = sort_rows(rows(&engine, q));
    assert_eq!(
        first, second,
        "repeated invocations must be deterministic under the shared hash-build cache"
    );
    assert!(
        !first.is_empty(),
        "expected at least one matching row; got empty"
    );
}

/// T2-1 gate: a CTE referenced exactly once (`consumer_count == 1`) must not
/// engage the cross-reference build cache — the shared full-store build never
/// amortises with a single consumer. Joining the CTE once against a base table
/// drives the `consumer_count > 1` false branch; the result must still be
/// correct via the ordinary local-build hash join.
#[test]
fn single_consumer_cte_join_skips_cache_and_matches_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE base (k INT NOT NULL, v INT NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE TABLE dim (k INT NOT NULL, label INT NOT NULL)")
        .unwrap();
    for k in 1..=10 {
        engine
            .execute(&format!("INSERT INTO base VALUES ({k}, {})", k * 3))
            .unwrap();
        if k % 2 == 0 {
            engine
                .execute(&format!("INSERT INTO dim VALUES ({k}, {})", k + 100))
                .unwrap();
        }
    }

    // `c` is referenced once → consumer_count == 1 → cache gate is closed.
    let actual = sort_rows(rows(
        &engine,
        "WITH c AS (SELECT k, v FROM base) \
         SELECT c.k, c.v, dim.label FROM c, dim WHERE c.k = dim.k",
    ));
    // Oracle: the same join with `c` inlined as a derived table (always Owned,
    // never CTE-backed, so the cache is structurally impossible).
    let oracle = sort_rows(rows(
        &engine,
        "SELECT c.k, c.v, dim.label \
         FROM (SELECT k, v FROM base) c, dim WHERE c.k = dim.k",
    ));
    assert_eq!(
        actual, oracle,
        "single-consumer CTE join must match the cache-free oracle"
    );
    assert_eq!(actual.len(), 5, "expected 5 even-keyed matches");
}
