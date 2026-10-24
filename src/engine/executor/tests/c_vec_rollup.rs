//! C regression coverage for the vectorized ROLLUP / GROUPING SETS
//! phase-1 fast path.
//!
//! The row two-phase ROLLUP (`aggregate_rollup_plan_rows`) is the
//! correctness oracle. Each query runs twice — `with_vec_mode(Off)` forces
//! the row path, `with_vec_mode(On)` engages the vectorized finest
//! grouping (`vec_rollup_finest_groups` → seeded row accumulators → the
//! same phase-2 derive + emit). The full result set (key columns,
//! super-aggregate NULL keys, GROUPING outputs, aggregate values, ordering
//! after the query's own ORDER BY) must be byte-identical.
//!
//! The vec path accepts batch-compilable group expressions and aggregate
//! inputs for mergeable SUM/COUNT/MIN/MAX/AVG state. DISTINCT / ordered /
//! holistic state remains on the row oracle until its partial state can be
//! seeded into the common ROLLUP merge phase.

use super::support::query_rows as rows_of;
use super::*;
use crate::engine::executor::vec::config::{with_vec_mode, VecMode};

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
        "vec ROLLUP output diverged from row oracle for: {sql}"
    );
}

fn sales_setup() -> [&'static str; 2] {
    [
        "CREATE TABLE sales (id INT PRIMARY KEY, region INT, product INT, qty INT, price DECIMAL(10,2))",
        "INSERT INTO sales VALUES \
           (1,1,10,5,2.50),(2,1,10,3,2.50),(3,1,20,7,1.00),(4,1,20,2,1.00),\
           (5,2,10,4,3.00),(6,2,30,9,5.00),(7,2,30,1,5.00),(8,2,30,6,5.00),\
           (9,3,10,NULL,4.00),(10,3,20,8,NULL),(11,3,20,8,2.00)",
    ]
}

#[test]
fn rollup_sum_count_matches_row() {
    let s = sales_setup();
    assert_vec_matches_row(
        &s,
        "SELECT region, product, SUM(qty), COUNT(*), COUNT(qty) \
         FROM sales GROUP BY region, product WITH ROLLUP \
         ORDER BY region, product",
    );
}

#[test]
fn rollup_avg_min_max_matches_row() {
    let s = sales_setup();
    assert_vec_matches_row(
        &s,
        "SELECT region, product, AVG(qty), MIN(qty), MAX(qty), SUM(price) \
         FROM sales GROUP BY region, product WITH ROLLUP \
         ORDER BY region, product",
    );
}

#[test]
fn rollup_single_key_matches_row() {
    let s = sales_setup();
    assert_vec_matches_row(
        &s,
        "SELECT region, SUM(qty), AVG(price), COUNT(*) \
         FROM sales GROUP BY region WITH ROLLUP ORDER BY region",
    );
}

#[test]
fn rollup_three_keys_matches_row() {
    let s = sales_setup();
    assert_vec_matches_row(
        &s,
        "SELECT region, product, qty, SUM(price), COUNT(*) \
         FROM sales GROUP BY region, product, qty WITH ROLLUP \
         ORDER BY region, product, qty",
    );
}

#[test]
fn rollup_with_grouping_function_matches_row() {
    let s = sales_setup();
    assert_vec_matches_row(
        &s,
        "SELECT region, product, GROUPING(region), GROUPING(product), SUM(qty) \
         FROM sales GROUP BY region, product WITH ROLLUP \
         ORDER BY region, product",
    );
}

#[test]
fn rollup_decimal_sum_matches_row() {
    let s = sales_setup();
    assert_vec_matches_row(
        &s,
        "SELECT region, product, SUM(price), AVG(price) \
         FROM sales GROUP BY region, product WITH ROLLUP \
         ORDER BY region, product",
    );
}

#[test]
fn rollup_expression_keys_and_inputs_match_row() {
    let s = sales_setup();
    assert_vec_matches_row(
        &s,
        "SELECT region % 2, product + 1, SUM(qty * product), \
                AVG(price * qty), MIN(qty + 1), MAX(qty - 1) \
         FROM sales GROUP BY region % 2, product + 1 WITH ROLLUP \
         ORDER BY region % 2, product + 1",
    );
}

#[test]
fn rollup_all_null_measure_group_matches_row() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, g INT, h INT, v INT)",
        "INSERT INTO t VALUES (1,1,1,NULL),(2,1,1,NULL),(3,1,2,5),(4,2,1,7)",
    ];
    assert_vec_matches_row(
        &setup,
        "SELECT g, h, SUM(v), AVG(v), COUNT(v), MIN(v), MAX(v) \
         FROM t GROUP BY g, h WITH ROLLUP ORDER BY g, h",
    );
}

#[test]
fn rollup_empty_input_matches_row() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)",
        "INSERT INTO t VALUES (1,1,1)",
        "DELETE FROM t",
    ];
    // Empty input must still emit the grand-total row in the already-open
    // batch pipeline; it must not restart the scan through the row path.
    assert_vec_matches_row(
        &setup,
        "SELECT g, SUM(v), COUNT(*) FROM t GROUP BY g WITH ROLLUP",
    );
}

#[test]
fn rollup_having_matches_row() {
    let s = sales_setup();
    assert_vec_matches_row(
        &s,
        "SELECT region, product, SUM(qty) \
         FROM sales GROUP BY region, product WITH ROLLUP \
         HAVING SUM(qty) > 5 ORDER BY region, product",
    );
}

#[test]
fn grouping_sets_matches_row() {
    let s = sales_setup();
    assert_vec_matches_row(
        &s,
        "SELECT region, product, SUM(qty), COUNT(*) \
         FROM sales GROUP BY GROUPING SETS ((region, product), (region), ()) \
         ORDER BY region, product",
    );
}

#[test]
fn grouping_sets_expression_keys_and_inputs_match_row() {
    let s = sales_setup();
    assert_vec_matches_row(
        &s,
        "SELECT region % 2, product + 1, SUM(qty * product), AVG(price * qty) \
         FROM sales GROUP BY GROUPING SETS (\
           (region % 2, product + 1), (region % 2), ()) \
         ORDER BY region % 2, product + 1",
    );
}

// Excluded shapes: `On` falls back to the row path, so this asserts no
// regression on the fallback boundary (DISTINCT inside a ROLLUP).
#[test]
fn rollup_count_distinct_falls_back_and_matches_row() {
    let s = sales_setup();
    assert_vec_matches_row(
        &s,
        "SELECT region, COUNT(DISTINCT product), SUM(qty) \
         FROM sales GROUP BY region WITH ROLLUP ORDER BY region",
    );
}

// Larger fixture so the finest grouping spans many groups across batches —
// exercises the partition snapshot collection + phase-2 derive at scale.
fn many_group_setup() -> Vec<String> {
    let mut setup = vec!["CREATE TABLE big (id INT PRIMARY KEY, a INT, b INT, v INT)".to_string()];
    let mut next = 0usize;
    while next < 3000 {
        let take = (3000 - next).min(200);
        let mut vals = Vec::with_capacity(take);
        for i in 0..take {
            let r = next + i;
            vals.push(format!("({},{},{},{})", r, r % 13, r % 7, (r % 50) + 1));
        }
        setup.push(format!("INSERT INTO big VALUES {}", vals.join(",")));
        next += take;
    }
    setup
}

#[test]
fn rollup_many_groups_matches_row() {
    let setup = many_group_setup();
    let setup_ref: Vec<&str> = setup.iter().map(|s| s.as_str()).collect();
    assert_vec_matches_row(
        &setup_ref,
        "SELECT a, b, SUM(v), COUNT(*), AVG(v), MIN(v), MAX(v) \
         FROM big GROUP BY a, b WITH ROLLUP ORDER BY a, b",
    );
}

// Performance harness for the *serial* vec ROLLUP fast path (the only one
// the vectorized finest grouping is wired into). 80k rows stays under the
// parallel threshold so both sides use `execute_hash_rollup_aggregate`:
// Off = serial row two-phase, On = vectorized finest + seeded two-phase.
// Run with:
//   cargo test --release -p mydb c_vec_rollup::perf_rollup_vec_vs_row -- --ignored --nocapture
const PERF_ROWS: usize = 80_000;
const PERF_GROUPS: usize = 2500; // 50 × 50

#[test]
#[ignore]
fn perf_rollup_vec_vs_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE big (id INT PRIMARY KEY, a INT, b INT, v INT, p DECIMAL(12,2))")
        .unwrap();
    let mut next = 0usize;
    while next < PERF_ROWS {
        let take = (PERF_ROWS - next).min(2000);
        let mut vals = String::with_capacity(take * 24);
        for i in 0..take {
            let r = next + i;
            if i > 0 {
                vals.push(',');
            }
            // 50 × 50 = 2500 finest groups; phase 1 probes every row.
            vals.push_str(&format!(
                "({},{},{},{},{}.{:02})",
                r,
                r % 50,
                (r / 50) % 50,
                (r % 97) as i64,
                (r % 1000) as i64,
                (r % 100) as i64
            ));
        }
        engine
            .execute(&format!("INSERT INTO big VALUES {vals}"))
            .unwrap();
        next += take;
    }

    let sql = "SELECT a, b, SUM(v), COUNT(*), AVG(v), MIN(v), MAX(v), SUM(p) \
               FROM big GROUP BY a, b WITH ROLLUP";

    let median = |mode: VecMode| -> std::time::Duration {
        let _ = with_vec_mode(mode, || rows_of(&engine, sql)); // warm
        let mut samples: Vec<std::time::Duration> = (0..7)
            .map(|_| {
                let t = std::time::Instant::now();
                let _ = with_vec_mode(mode, || rows_of(&engine, sql));
                t.elapsed()
            })
            .collect();
        samples.sort();
        samples[samples.len() / 2]
    };

    // Interleave to damp thermal drift, take the best of two medians.
    let row = median(VecMode::Off).min(median(VecMode::Off));
    let vec = median(VecMode::On).min(median(VecMode::On));
    eprintln!(
        "ROLLUP {} rows / {} groups (serial): row={:?} vec={:?} speedup={:.2}x",
        PERF_ROWS,
        PERF_GROUPS,
        row,
        vec,
        row.as_secs_f64() / vec.as_secs_f64()
    );
}

#[test]
fn wide_rollup_prefix_walk_matches_row_for_nulls_and_weighted_averages() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;
    let (_dir, engine) = engine_with(&[
        "CREATE TABLE wide_rollup (id INT PRIMARY KEY, a VARCHAR(40), b VARCHAR(40), c INT, d DATE, e DECIMAL(8,2), f INT, v DECIMAL(12,2))",
    ]);
    for base in (0..1024).step_by(128) {
        let values = (base..base + 128)
            .map(|id| {
                format!(
                    "({id},{},'product-{}',{},'2024-01-{:02}',{}.25,{},{})",
                    if id % 19 == 0 {
                        "NULL".into()
                    } else {
                        format!("'region-{}'", id % 5)
                    },
                    id % 100,
                    id % 7,
                    id % 28 + 1,
                    id % 3,
                    id % 128,
                    if id % 8 == 0 {
                        "NULL".into()
                    } else {
                        format!("{}.50", id % 31)
                    },
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO wide_rollup VALUES {values}"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE wide_rollup").unwrap();
    let sql = "SELECT a,b,c,d,e,f,GROUPING(a,b,c,d,e,f),SUM(v),AVG(v),MIN(v),MAX(v),COUNT(v),COUNT(*) FROM wide_rollup GROUP BY a,b,c,d,e,f WITH ROLLUP ORDER BY a,b,c,d,e,f,GROUPING(a,b,c,d,e,f)";
    let run = |mode, limit| {
        let budget = Arc::new(MemBudget::new(limit));
        let rows = with_vec_mode(mode, || {
            with_query_budget(budget.clone(), || {
                match engine.execute(sql).unwrap().materialize().unwrap() {
                    QueryResponse::ResultSet(result) => result.rows,
                    other => panic!("expected rows, got {other:?}"),
                }
            })
        });
        assert_eq!(budget.used(), 0);
        rows
    };
    let expected = run(VecMode::Off, 64 * 1024 * 1024);
    assert_eq!(run(VecMode::On, 64 * 1024 * 1024), expected);
    assert_eq!(run(VecMode::On, 64 * 1024), expected);
}
