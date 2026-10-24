//! A2 regression coverage for HashAggregateCore's DISTINCT aggregates:
//! `COUNT(DISTINCT x)`, `SUM(DISTINCT x)`, `AVG(DISTINCT x)`.
//!
//! Strategy mirrors the A1 suite: the row-based accumulators
//! (`select::aggregate::CountDistinctAcc`, `DistinctReducer<SumAcc>` /
//! `DistinctReducer<AvgAcc>`) are the correctness oracle. Each query runs
//! twice — the oracle with `with_vec_mode(VecMode::Off, …)` (forced row
//! path) and the comparison with `with_vec_mode(VecMode::On, …)` (forced
//! columnar path). Forcing both sides is required because `MYDB_VECTORIZED`
//! is cached in a `OnceLock` (env changes don't take effect per-test) and
//! `Auto` would route by row-count.
//!
//! Engagement note: the vec distinct code paths themselves are exercised
//! unconditionally by the direct `HashAggregateCore` unit tests in
//! `vec::hash_agg::tests::distinct_*` (serial, parallel snapshot-merge,
//! spill round-trip). These SQL tests prove the bridge actually *routes*
//! distinct aggregates into that vec path and that the result threads back
//! through the projection layer identically to the row path.

use super::support::query_rows as rows_of;
use super::*;
use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
use std::sync::Arc;

fn engine_with(setup: &[&str]) -> (tempfile::TempDir, StorageQueryEngine) {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for s in setup {
        engine.execute(s).unwrap();
    }
    (dir, engine)
}

fn assert_vec_matches_row(setup: &[&str], sql: &str) {
    let (_dir, engine) = engine_with(setup);
    let row = with_vec_mode(VecMode::Off, || rows_of(&engine, sql));
    let vec = with_vec_mode(VecMode::On, || rows_of(&engine, sql));
    assert_eq!(
        row, vec,
        "vec distinct output diverged from row oracle for: {sql}"
    );
}

fn assert_vec_spill_matches_row(setup: &[&str], sql: &str) {
    let (_dir, engine) = engine_with(setup);
    let row = with_vec_mode(VecMode::Off, || rows_of(&engine, sql));
    let tiny = Arc::new(MemBudget::new(64 * 1024));
    let vec_spill = with_vec_mode(VecMode::On, || {
        with_query_budget(Arc::clone(&tiny), || rows_of(&engine, sql))
    });
    assert_eq!(
        row, vec_spill,
        "vec distinct spill output diverged from row oracle for: {sql}"
    );
    assert_eq!(tiny.used(), 0, "no charge may leak after the query");
}

fn distinct_setup() -> [&'static str; 2] {
    [
        "CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT, d DECIMAL(10,2))",
        "INSERT INTO t VALUES \
           (1,0,10,1.50),(2,0,20,1.50),(3,0,10,2.25),(4,0,20,2.25),(5,0,30,3.00),\
           (6,1,5,9.99),(7,1,5,9.99),(8,1,5,9.99),\
           (9,2,42,7.00),\
           (10,3,NULL,NULL),(11,3,7,1.10),(12,3,7,2.20),(13,3,NULL,3.30)",
    ]
}

#[test]
fn count_distinct_grouped_matches_row() {
    let s = distinct_setup();
    assert_vec_matches_row(
        &s,
        "SELECT g, COUNT(DISTINCT v) FROM t GROUP BY g ORDER BY g",
    );
}

#[test]
fn sum_distinct_grouped_matches_row() {
    let s = distinct_setup();
    assert_vec_matches_row(&s, "SELECT g, SUM(DISTINCT v) FROM t GROUP BY g ORDER BY g");
}

#[test]
fn avg_distinct_grouped_matches_row() {
    let s = distinct_setup();
    assert_vec_matches_row(&s, "SELECT g, AVG(DISTINCT v) FROM t GROUP BY g ORDER BY g");
}

#[test]
fn count_sum_avg_distinct_together_matches_row() {
    let s = distinct_setup();
    assert_vec_matches_row(
        &s,
        "SELECT g, COUNT(DISTINCT v), SUM(DISTINCT v), AVG(DISTINCT v) \
         FROM t GROUP BY g ORDER BY g",
    );
}

#[test]
fn distinct_over_decimal_column_matches_row() {
    let s = distinct_setup();
    assert_vec_matches_row(
        &s,
        "SELECT g, COUNT(DISTINCT d), SUM(DISTINCT d), AVG(DISTINCT d) \
         FROM t GROUP BY g ORDER BY g",
    );
}

#[test]
fn distinct_implicit_single_group_matches_row() {
    let s = distinct_setup();
    assert_vec_matches_row(
        &s,
        "SELECT COUNT(DISTINCT v), SUM(DISTINCT v), AVG(DISTINCT v) FROM t",
    );
}

#[test]
fn distinct_all_null_group_matches_row() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)",
        "INSERT INTO t VALUES (1,9,NULL),(2,9,NULL),(3,8,5),(4,8,5),(5,8,6)",
    ];
    // g=9 is all-NULL: COUNT(DISTINCT)=0, SUM/AVG(DISTINCT)=NULL.
    assert_vec_matches_row(
        &setup,
        "SELECT g, COUNT(DISTINCT v), SUM(DISTINCT v), AVG(DISTINCT v) \
         FROM t GROUP BY g ORDER BY g",
    );
}

#[test]
fn distinct_mixed_with_non_distinct_matches_row() {
    let s = distinct_setup();
    // Distinct and plain aggregates side by side over the same column.
    assert_vec_matches_row(
        &s,
        "SELECT g, COUNT(*), COUNT(DISTINCT v), SUM(v), SUM(DISTINCT v), \
                MIN(v), MAX(v), AVG(DISTINCT v) \
         FROM t GROUP BY g ORDER BY g",
    );
}

#[test]
fn distinct_over_expression_matches_row() {
    let s = distinct_setup();
    // Expression argument routes through ComputeColumnsBatchStream before
    // the dedup set is built.
    assert_vec_matches_row(
        &s,
        "SELECT g, COUNT(DISTINCT v + 1), SUM(DISTINCT v * 2) \
         FROM t GROUP BY g ORDER BY g",
    );
}

// ---------------------------------------------------------------------
// Spill round-trip — tight budget forces the dedup sets to disk.
// ---------------------------------------------------------------------

fn many_group_setup() -> Vec<String> {
    let mut setup = vec!["CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)".to_string()];
    let mut next = 0usize;
    while next < 4000 {
        let take = (4000 - next).min(200);
        let mut vals = Vec::with_capacity(take);
        for i in 0..take {
            let r = next + i;
            // ~211 groups, a handful of distinct values per group, heavily
            // duplicated so dedup actually collapses rows.
            vals.push(format!("({},{},{})", r, r % 211, (r % 17) * 10));
        }
        setup.push(format!("INSERT INTO t VALUES {}", vals.join(",")));
        next += take;
    }
    setup
}

#[test]
fn distinct_spill_round_trip_matches_row() {
    let setup = many_group_setup();
    let setup_ref: Vec<&str> = setup.iter().map(|s| s.as_str()).collect();
    assert_vec_spill_matches_row(
        &setup_ref,
        "SELECT g, COUNT(DISTINCT v), SUM(DISTINCT v), AVG(DISTINCT v) \
         FROM t GROUP BY g ORDER BY g",
    );
}

/// P0-2: vec-side GROUP BY string key must fold through the default
/// collation the same way row-side ValueKey (`with_default_string_key`)
/// does; otherwise the vec path silently splits 'foo' / 'FOO' into
/// distinct groups on any query touching ≥1024 rows (default
/// MYDB_VECTORIZED=auto route). This is the direct differential that
/// would have caught the bug.
#[test]
fn p0_2_vec_group_by_string_matches_row_default_collation() {
    let mut setup = vec!["CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(10))".to_string()];
    // Build a bulk INSERT with ~3000 rows split across 'foo','FOO','Foo'
    let mut vals = Vec::with_capacity(1500);
    for id in 1..=3000 {
        let v = match id % 3 {
            0 => "foo",
            1 => "FOO",
            _ => "Foo",
        };
        vals.push(format!("({id},'{v}')"));
        if vals.len() == 500 {
            setup.push(format!("INSERT INTO t VALUES {}", vals.join(",")));
            vals.clear();
        }
    }
    if !vals.is_empty() {
        setup.push(format!("INSERT INTO t VALUES {}", vals.join(",")));
    }
    let setup_ref: Vec<&str> = setup.iter().map(|s| s.as_str()).collect();
    // Default collation is case/accent-insensitive: expect 1 group of 3000.
    assert_vec_matches_row(&setup_ref, "SELECT COUNT(*) FROM t GROUP BY v ORDER BY 1");
    // Sanity: the row-side answer for GROUP BY v under utf8mb4_0900_ai_ci
    // is one group.
    let (_dir, engine) = engine_with(&setup_ref);
    let out = with_vec_mode(VecMode::Off, || {
        rows_of(&engine, "SELECT COUNT(*) FROM t GROUP BY v")
    });
    assert_eq!(
        out.len(),
        1,
        "default collation groups all 3 casings: {out:?}"
    );
    assert_eq!(out[0][0], Value::Int(3000));
}
