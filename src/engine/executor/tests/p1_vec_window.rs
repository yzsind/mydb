//! P1-4 regression coverage for the vec window fast path
//! (`dispatch::try_vec_window_fast_path`).
//!
//! The fast path is intentionally narrow this round — it only handles
//! ROW_NUMBER / RANK / DENSE_RANK / COUNT / MIN / MAX with ROWS frames
//! (or no frame) and NoOthers exclusion. SUM/AVG are routed through the
//! legacy NumericAggState path so the Decimal output type matches.
//!
//! Each test selects vector and scalar kernels with `MYDB_VECTORIZED`'s
//! thread-local test override. Both execute the same BatchOperator graph.

use super::*;
use crate::engine::executor::vec::config::{with_vec_mode, VecMode};

fn rows_with_env(
    vars: &[(&'static str, Option<&str>)],
    setup: &[&str],
    sql: &str,
) -> Vec<Vec<Value>> {
    with_test_env(vars, || {
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

fn assert_vec_matches_legacy(setup: &[&str], sql: &str) {
    let vec_on = with_vec_mode(VecMode::On, || rows_with_env(&[], setup, sql));
    let vec_off = with_vec_mode(VecMode::Off, || rows_with_env(&[], setup, sql));
    assert_eq!(vec_on, vec_off, "vec window output diverged from legacy");
}

// ---------------------------------------------------------------------
// Ranking functions (frame-agnostic)
// ---------------------------------------------------------------------

#[test]
fn row_number_partition_by_order_by_matches_legacy() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, cls INT, v INT)",
        "INSERT INTO t VALUES (1,1,10),(2,1,20),(3,1,30),(4,2,100),(5,2,200),(6,2,300)",
    ];
    let sql = "SELECT id, cls, \
               ROW_NUMBER() OVER (PARTITION BY cls ORDER BY v) AS rn \
               FROM t ORDER BY id";
    assert_vec_matches_legacy(&setup, sql);
}

#[test]
fn rank_with_ties_matches_legacy() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (1,10),(2,10),(3,20),(4,20),(5,30)",
    ];
    let sql = "SELECT id, v, \
               RANK() OVER (ORDER BY v) AS r, \
               DENSE_RANK() OVER (ORDER BY v) AS dr \
               FROM t ORDER BY id";
    assert_vec_matches_legacy(&setup, sql);
}

#[test]
fn rank_with_multiple_partitions_matches_legacy() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, p INT, v INT)",
        "INSERT INTO t VALUES \
           (1,1,5),(2,1,5),(3,1,8),(4,2,1),(5,2,2),(6,2,2),(7,2,3)",
    ];
    let sql = "SELECT id, p, v, \
               RANK() OVER (PARTITION BY p ORDER BY v) AS r \
               FROM t ORDER BY id";
    assert_vec_matches_legacy(&setup, sql);
}

// ---------------------------------------------------------------------
// COUNT — Int output, byte-identical to legacy
// ---------------------------------------------------------------------

#[test]
fn count_star_over_rows_frame_matches_legacy() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (1,1),(2,2),(3,3),(4,4),(5,5)",
    ];
    let sql = "SELECT id, \
               COUNT(*) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS c \
               FROM t ORDER BY id";
    assert_vec_matches_legacy(&setup, sql);
}

#[test]
fn count_col_skips_nulls_over_unbounded_frame_matches_legacy() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (1,10),(2,NULL),(3,30),(4,NULL),(5,50)",
    ];
    let sql = "SELECT id, \
               COUNT(v) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS c \
               FROM t ORDER BY id";
    assert_vec_matches_legacy(&setup, sql);
}

// ---------------------------------------------------------------------
// MIN / MAX — pass-through of input value
// ---------------------------------------------------------------------

#[test]
fn min_max_sliding_window_matches_legacy() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (1,5),(2,3),(3,8),(4,1),(5,9),(6,2),(7,7)",
    ];
    let sql = "SELECT id, \
               MIN(v) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS m, \
               MAX(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS mx \
               FROM t ORDER BY id";
    assert_vec_matches_legacy(&setup, sql);
}

#[test]
fn min_max_unbounded_prefix_partitioned_matches_legacy() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, p INT, v DECIMAL(7,2))",
        "INSERT INTO t VALUES \
           (1,1,5.00),(2,1,NULL),(3,1,7.25),(4,1,6.50),\
           (5,2,NULL),(6,2,2.00),(7,2,9.75),(8,2,4.25)",
    ];
    let sql = "SELECT id, p, \
               MAX(v) OVER (PARTITION BY p ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS mx, \
               MIN(v) OVER (PARTITION BY p ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS mn \
               FROM t ORDER BY id";
    assert_vec_matches_legacy(&setup, sql);
}

#[test]
fn min_max_forward_rows_frame_matches_legacy() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (1,4),(2,NULL),(3,1),(4,9),(5,2),(6,8),(7,NULL),(8,3)",
    ];
    let sql = "SELECT id, \
               MIN(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) AS mn, \
               MAX(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) AS mx \
               FROM t ORDER BY id";
    assert_vec_matches_legacy(&setup, sql);
}

// ---------------------------------------------------------------------
// SUM / AVG must NOT take the vec path (Decimal type fidelity)
// ---------------------------------------------------------------------

#[test]
fn avg_over_int_returns_decimal_via_legacy_path() {
    // AVG of INT in MySQL returns DECIMAL with `div_precision_increment`
    // (=4 by default) extra scale. The vec fast path is intentionally
    // not invoked for AVG so this contract holds.
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (1,2),(2,4),(3,6),(4,8),(5,10)",
    ];
    let sql = "SELECT id, \
               AVG(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS a \
               FROM t ORDER BY id";
    // The result must be DecimalCompact for both kernels.
    let with_vec_on = with_vec_mode(VecMode::On, || rows_with_env(&[], &setup, sql));
    let with_vec_off = with_vec_mode(VecMode::Off, || rows_with_env(&[], &setup, sql));
    assert_eq!(with_vec_on, with_vec_off);
    for row in &with_vec_on {
        assert!(
            matches!(row[1], Value::DecimalCompact(_, _) | Value::Decimal(_)),
            "AVG output must be DECIMAL-typed; got {:?}",
            row[1]
        );
    }
}

#[test]
fn sum_over_int_matches_legacy_decimal_output() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (1,1),(2,2),(3,3),(4,4),(5,5)",
    ];
    let sql = "SELECT id, \
               SUM(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s \
               FROM t ORDER BY id";
    assert_vec_matches_legacy(&setup, sql);
}

// ---------------------------------------------------------------------
// D1: SUM/AVG now route through the vec NumericAggState sliding path for
// exact (integer/decimal), uniform-scale arguments. Float arguments and
// mixed-scale arguments must fall back to legacy (still byte-identical
// because both run the same legacy path). All comparisons are
// vector kernel vs scalar kernel.
// ---------------------------------------------------------------------

#[test]
fn sum_decimal_sliding_frame_matches_legacy() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(10,2))",
        "INSERT INTO t VALUES (1,1.10),(2,2.20),(3,3.30),(4,4.40),(5,5.50),(6,6.60)",
    ];
    let sql = "SELECT id, \
               SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s \
               FROM t ORDER BY id";
    assert_vec_matches_legacy(&setup, sql);
}

#[test]
fn avg_decimal_sliding_frame_matches_legacy() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(10,2))",
        "INSERT INTO t VALUES (1,1.10),(2,2.20),(3,3.30),(4,4.40),(5,5.50),(6,6.60)",
    ];
    let sql = "SELECT id, \
               AVG(v) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS a \
               FROM t ORDER BY id";
    assert_vec_matches_legacy(&setup, sql);
}

#[test]
fn sum_avg_int_partitioned_matches_legacy() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, p INT, v INT)",
        "INSERT INTO t VALUES \
           (1,0,10),(2,0,20),(3,0,30),(4,1,5),(5,1,15),(6,1,25),(7,1,35)",
    ];
    let sql = "SELECT id, p, \
               SUM(v) OVER (PARTITION BY p ORDER BY id) AS s, \
               AVG(v) OVER (PARTITION BY p ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS a \
               FROM t ORDER BY id";
    assert_vec_matches_legacy(&setup, sql);
}

#[test]
fn sum_int_with_negatives_and_nulls_matches_legacy() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (1,-5),(2,NULL),(3,10),(4,-3),(5,NULL),(6,8)",
    ];
    let sql = "SELECT id, \
               SUM(v) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s, \
               AVG(v) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS a \
               FROM t ORDER BY id";
    assert_vec_matches_legacy(&setup, sql);
}

#[test]
fn sum_unbounded_both_ends_matches_legacy() {
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, p INT, v INT)",
        "INSERT INTO t VALUES (1,0,3),(2,0,1),(3,0,4),(4,1,1),(5,1,5)",
    ];
    let sql = "SELECT id, p, \
               SUM(v) OVER (PARTITION BY p) AS s \
               FROM t ORDER BY id";
    assert_vec_matches_legacy(&setup, sql);
}

#[test]
fn sum_avg_over_double_stays_legacy_and_matches() {
    // DOUBLE argument: the fast path's exactness gate bails, so both the
    // vector and scalar selections both use the exact f64 oracle lane and are
    // trivially identical. Guards against the gate accidentally routing
    // float (which would drift).
    let setup = [
        "CREATE TABLE t (id INT PRIMARY KEY, v DOUBLE)",
        "INSERT INTO t VALUES (1,1.5),(2,2.25),(3,3.125),(4,4.0625),(5,5.0)",
    ];
    let sql = "SELECT id, \
               SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s, \
               AVG(v) OVER (ORDER BY id) AS a \
               FROM t ORDER BY id";
    assert_vec_matches_legacy(&setup, sql);
}

#[test]
fn sum_large_int_running_matches_legacy() {
    // Exercise a running SUM over a wider partition so the slide does
    // many add/subtract steps; exact i128 must match legacy fresh sums.
    let mut setup = vec!["CREATE TABLE t (id INT PRIMARY KEY, v INT)".to_string()];
    let mut vals = Vec::new();
    for i in 1..=300i64 {
        vals.push(format!("({},{})", i, (i * 7919) % 1000 - 500));
    }
    setup.push(format!("INSERT INTO t VALUES {}", vals.join(",")));
    let setup_ref: Vec<&str> = setup.iter().map(|s| s.as_str()).collect();
    let sql = "SELECT id, \
               SUM(v) OVER (ORDER BY id ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS s, \
               AVG(v) OVER (ORDER BY id ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS a \
               FROM t ORDER BY id";
    assert_vec_matches_legacy(&setup_ref, sql);
}

#[test]
fn partition_window_spools_wide_input_and_restores_input_order() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE wsp (id INT PRIMARY KEY, k INT, v INT, payload VARCHAR(200))")
        .unwrap();
    let payload = "x".repeat(160);
    let mut sums = [0i64; 32];
    for start in (0..4096).step_by(256) {
        let values = (start..start + 256)
            .map(|id| {
                sums[id % 32] += (id % 17) as i64;
                format!("({id},{},{},'{payload}')", id % 32, id % 17)
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO wsp VALUES {values}"))
            .unwrap();
    }
    let budget = Arc::new(MemBudget::new(256 * 1024));
    let QueryResponse::ResultSet(result) = with_query_budget(budget.clone(), || engine.execute(
        "SELECT id, k, payload, SUM(v) OVER (PARTITION BY k) AS total, ROW_NUMBER() OVER (PARTITION BY k ORDER BY id) AS rn FROM wsp ORDER BY id"
    )).unwrap() else { panic!("result set"); };
    assert_eq!(result.rows.len(), 4096);
    for (id, row) in result.rows.iter().enumerate() {
        assert_eq!(row[0], Value::Int(id as i64));
        assert_eq!(row[2], Value::String(payload.clone()));
        assert_eq!(
            crate::engine::expr::numeric(&row[3]).unwrap(),
            sums[id % 32] as f64
        );
        assert_eq!(row[4], Value::Int((id / 32 + 1) as i64));
    }
    drop(result);
    assert_eq!(budget.used(), 0);
}

#[test]
fn window_post_projection_keeps_requested_streaming_response() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE streamed_window (v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO streamed_window VALUES (3),(1),(2)")
        .unwrap();
    let response = engine
        .execute_streaming_for_test(
            "SELECT v, ROW_NUMBER() OVER (ORDER BY v) + 10 AS shifted FROM streamed_window",
        )
        .unwrap();
    assert!(
        matches!(response, QueryResponse::StreamingResultSet(_)),
        "window projection must not collect a requested stream into a ResultSet"
    );
    let QueryResponse::ResultSet(result) = response.materialize().unwrap() else {
        panic!("result set");
    };
    let mut rows = result.rows;
    rows.sort_by_key(|row| match row[0] {
        Value::Int(value) => value,
        _ => panic!("integer"),
    });
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(1), Value::Int(11)],
            vec![Value::Int(2), Value::Int(12)],
            vec![Value::Int(3), Value::Int(13)]
        ]
    );
}
