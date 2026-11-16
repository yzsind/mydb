//! A1 regression coverage for HashAggregateCore's extended aggregates:
//! BIT_AND / BIT_OR / BIT_XOR and the variance/stddev family
//! (VAR_POP / VAR_SAMP / STDDEV_POP / STDDEV_SAMP, plus the STD /
//! STDDEV / VARIANCE aliases).
//!
//! Strategy: the row-based accumulators (`select::aggregate::BitAcc`,
//! `VarianceAcc` / the `eval.rs` aggregate arm) are the correctness
//! oracle. Each query runs twice — the oracle with
//! `with_vec_mode(VecMode::Off, …)` (forced row path) and the comparison
//! with `with_vec_mode(VecMode::On, …)` (forced columnar path). Forcing
//! both sides is required: `MYDB_VECTORIZED` is cached in a `OnceLock`
//! so the env var does NOT take effect per-test, and `Auto` would route
//! by row-count, making the comparison fixture-size dependent. The
//! non-floating result rows must be byte-identical (this caught a real divergence
//! where BIT_* `Value::UInt` was folded to `Value::Int` by the columnar
//! output builder's signed I64 lane).
//!
//! Spill round-trip is exercised with a tight `MemBudget`, which pushes
//! the vec HashAgg to serialise partition state to disk and merge it
//! back at finalize — the riskiest new code (BIT_* / variance partial
//! serialisation).

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

/// Forced row oracle (`Off`) vs forced vec path (`On`). Forcing both
/// sides removes any dependency on the `Auto` row-count threshold so the
/// comparison is always row-vs-vec regardless of fixture size.
fn assert_vec_matches_row(setup: &[&str], sql: &str) {
    let (_dir, engine) = engine_with(setup);
    let row = with_vec_mode(VecMode::Off, || rows_of(&engine, sql));
    let vec = with_vec_mode(VecMode::On, || rows_of(&engine, sql));
    assert_eq!(
        row, vec,
        "vec aggregate output diverged from row oracle for: {sql}"
    );
}

/// Forced row oracle vs forced vec path under a tight budget (forces
/// the vec HashAgg to spill its BIT_* / variance partial state to disk
/// and merge it back).
fn assert_vec_spill_matches_row(setup: &[&str], sql: &str) {
    let (_dir, engine) = engine_with(setup);
    let row = with_vec_mode(VecMode::Off, || rows_of(&engine, sql));
    let tiny = Arc::new(MemBudget::new(64 * 1024));
    let vec_spill = with_vec_mode(VecMode::On, || {
        with_query_budget(Arc::clone(&tiny), || rows_of(&engine, sql))
    });
    assert_eq!(row.len(), vec_spill.len(), "row count for {sql}");
    for (expected, actual) in row.iter().zip(&vec_spill) {
        assert_eq!(expected.len(), actual.len());
        for (expected, actual) in expected.iter().zip(actual) {
            match (expected, actual) {
                // Smaller scan batches change where Welford partial states
                // are merged. Floating-point addition is not associative;
                // permit only rounding-scale error, retaining exact type and
                // value checks for NULL, integer/BIT_* and every other lane.
                (Value::Double(a), Value::Double(b)) if a.is_finite() && b.is_finite() => {
                    let tolerance = 4.0 * f64::EPSILON * a.abs().max(b.abs()).max(1.0);
                    assert!(
                        (a - b).abs() <= tolerance,
                        "variance mismatch {a} vs {b} for {sql}"
                    );
                }
                _ => assert_eq!(expected, actual, "spill output for {sql}"),
            }
        }
    }
    assert_eq!(tiny.used(), 0, "no charge may leak after the query");
}

fn bit_setup() -> [&'static str; 2] {
    [
        "CREATE TABLE t (id INT PRIMARY KEY, g INT, v BIGINT UNSIGNED)",
        "INSERT INTO t VALUES \
           (1,0,12),(2,0,10),(3,0,7),(4,1,255),(5,1,0),(6,1,128),\
           (7,2,1),(8,2,2),(9,2,4),(10,2,8),(11,3,NULL),(12,3,15)",
    ]
}

#[test]
fn bit_and_or_xor_grouped_matches_row() {
    let s = bit_setup();
    assert_vec_matches_row(
        &s,
        "SELECT g, BIT_AND(v), BIT_OR(v), BIT_XOR(v) FROM t GROUP BY g ORDER BY g",
    );
}

#[test]
fn bit_aggregates_implicit_group_matches_row() {
    let s = bit_setup();
    assert_vec_matches_row(&s, "SELECT BIT_AND(v), BIT_OR(v), BIT_XOR(v) FROM t");
}

#[test]
fn bit_all_null_group_returns_identity() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, g INT, v BIGINT UNSIGNED)",
        "INSERT INTO t VALUES (1,9,NULL),(2,9,NULL),(3,8,5),(4,8,3)",
    ];
    // BIT_AND over an all-NULL group must be u64::MAX, BIT_OR/XOR must be 0.
    assert_vec_matches_row(
        &setup,
        "SELECT g, BIT_AND(v), BIT_OR(v), BIT_XOR(v) FROM t GROUP BY g ORDER BY g",
    );
}

fn var_setup() -> [&'static str; 2] {
    [
        "CREATE TABLE t (id INT PRIMARY KEY, g INT, v DOUBLE)",
        "INSERT INTO t VALUES \
           (1,0,2.0),(2,0,4.0),(3,0,4.0),(4,0,4.0),(5,0,5.0),(6,0,5.0),(7,0,7.0),(8,0,9.0),\
           (9,1,1.0),(10,1,1.0),\
           (11,2,42.0),\
           (12,3,NULL),(13,3,3.5),(14,3,6.5)",
    ]
}

#[test]
fn variance_stddev_family_grouped_matches_row() {
    let s = var_setup();
    assert_vec_matches_row(
        &s,
        "SELECT g, VAR_POP(v), VAR_SAMP(v), STDDEV_POP(v), STDDEV_SAMP(v) \
         FROM t GROUP BY g ORDER BY g",
    );
}

#[test]
fn variance_aliases_match_row() {
    let s = var_setup();
    assert_vec_matches_row(
        &s,
        "SELECT g, STD(v), STDDEV(v), VARIANCE(v) FROM t GROUP BY g ORDER BY g",
    );
}

#[test]
fn variance_single_row_group_sample_is_null() {
    let s = var_setup();
    assert_vec_matches_row(
        &s,
        "SELECT g, VAR_SAMP(v), STDDEV_SAMP(v), VAR_POP(v) FROM t GROUP BY g ORDER BY g",
    );
}

#[test]
fn variance_over_int_column_matches_row() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)",
        "INSERT INTO t VALUES (1,0,10),(2,0,20),(3,0,30),(4,1,5),(5,1,5),(6,1,5)",
    ];
    assert_vec_matches_row(
        &setup,
        "SELECT g, VAR_POP(v), STDDEV_SAMP(v) FROM t GROUP BY g ORDER BY g",
    );
}

#[test]
fn variance_result_keeps_double_type_across_derived_table() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)",
        "INSERT INTO t VALUES (1,1,0),(2,1,0),(3,1,10),(4,2,5),(5,2,5)",
    ];
    let sql = "SELECT g, sample_stddev FROM (\
               SELECT g, STDDEV_SAMP(v) AS sample_stddev, AVG(v) AS mean_v \
               FROM t GROUP BY g) derived \
               WHERE sample_stddev / mean_v > 1 ORDER BY g";
    let (_dir, engine) = engine_with(&setup);
    for mode in [VecMode::Off, VecMode::On] {
        let rows = with_vec_mode(mode, || rows_of(&engine, sql));
        assert_eq!(rows.len(), 1, "unexpected rows in {mode:?}: {rows:?}");
        assert_eq!(rows[0][0], Value::Int(1));
        let Value::Double(stddev) = rows[0][1] else {
            panic!("derived STDDEV_SAMP lost DOUBLE type: {:?}", rows[0][1]);
        };
        assert!((stddev - 5.773_502_691_896_258).abs() < 1e-12);
    }
}

#[test]
fn mixed_standard_and_extended_aggs_match_row() {
    let s = var_setup();
    assert_vec_matches_row(
        &s,
        "SELECT g, COUNT(*), SUM(v), MIN(v), MAX(v), VAR_POP(v), BIT_XOR(id) \
         FROM t GROUP BY g ORDER BY g",
    );
}

#[test]
fn string_min_max_count_mixed_null_unicode_matches_row() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(64))",
        "INSERT INTO t VALUES (1,'Zulu'),(2,'alpha'),(3,NULL),(4,'中文'),(5,'alpha '),(6,'Éclair')",
    ];
    assert_vec_matches_row(&setup, "SELECT COUNT(v), MIN(v), MAX(v) FROM t");
}

// ---------------------------------------------------------------------
// Spill round-trip
// ---------------------------------------------------------------------

fn many_group_setup(value_expr: impl Fn(usize) -> String) -> Vec<String> {
    let mut setup =
        vec!["CREATE TABLE t (id INT PRIMARY KEY, g INT, v BIGINT UNSIGNED)".to_string()];
    // Insert in chunks to stay under max_allowed_packet.
    let mut next = 0usize;
    while next < 2000 {
        let take = (2000 - next).min(200);
        let mut vals = Vec::with_capacity(take);
        for i in 0..take {
            let r = next + i;
            vals.push(format!("({},{},{})", r, r % 257, value_expr(r)));
        }
        setup.push(format!("INSERT INTO t VALUES {}", vals.join(",")));
        next += take;
    }
    setup
}

#[test]
fn bit_aggregates_spill_round_trip_matches_row() {
    let setup = many_group_setup(|r| ((r as u64).wrapping_mul(2654435761) & 0xFFFF).to_string());
    let setup_ref: Vec<&str> = setup.iter().map(|s| s.as_str()).collect();
    assert_vec_spill_matches_row(
        &setup_ref,
        "SELECT g, BIT_AND(v), BIT_OR(v), BIT_XOR(v) FROM t GROUP BY g ORDER BY g",
    );
}

#[test]
fn variance_spill_round_trip_matches_row() {
    let setup = many_group_setup(|r| (r % 97).to_string());
    let setup_ref: Vec<&str> = setup.iter().map(|s| s.as_str()).collect();
    assert_vec_spill_matches_row(
        &setup_ref,
        "SELECT g, VAR_POP(v), VAR_SAMP(v), STDDEV_POP(v), STDDEV_SAMP(v) \
         FROM t GROUP BY g ORDER BY g",
    );
}
