//! End-to-end tests for the PostgreSQL-compatible `generate_series(...)` table
//! function (M1 parser + M2 integer-family planner/executor). See
//! `docs/generate-series-design.md`.

use super::*;

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    result.rows
}

fn ints(values: &[i64]) -> Vec<Vec<Value>> {
    values.iter().map(|&n| vec![Value::Int(n)]).collect()
}

#[test]
fn integer_series_basic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT * FROM generate_series(1, 5)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.columns.len(), 1);
    assert_eq!(rs.columns[0].name, "generate_series");
    assert_eq!(rs.rows, ints(&[1, 2, 3, 4, 5]));
}

#[test]
fn integer_series_with_step() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        rows(&engine, "SELECT * FROM generate_series(1, 10, 2)"),
        ints(&[1, 3, 5, 7, 9])
    );
}

#[test]
fn integer_series_negative_step() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        rows(&engine, "SELECT * FROM generate_series(5, 1, -1)"),
        ints(&[5, 4, 3, 2, 1])
    );
}

#[test]
fn empty_when_direction_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert!(rows(&engine, "SELECT * FROM generate_series(5, 1)").is_empty());
    assert!(rows(&engine, "SELECT * FROM generate_series(1, 5, -1)").is_empty());
}

#[test]
fn single_row_when_start_equals_stop() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        rows(&engine, "SELECT * FROM generate_series(3, 3)"),
        ints(&[3])
    );
}

#[test]
fn negative_range() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        rows(&engine, "SELECT * FROM generate_series(-2, 2)"),
        ints(&[-2, -1, 0, 1, 2])
    );
}

#[test]
fn zero_step_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine
        .execute("SELECT * FROM generate_series(1, 5, 0)")
        .unwrap_err();
    assert_eq!(err.code(), 1210, "{}", err.message());
}

#[test]
fn null_argument_yields_empty() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert!(rows(&engine, "SELECT * FROM generate_series(1, NULL)").is_empty());
    assert!(rows(&engine, "SELECT * FROM generate_series(NULL, 5)").is_empty());
    assert!(rows(&engine, "SELECT * FROM generate_series(1, 5, NULL)").is_empty());
}

#[test]
fn wrong_arity_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert!(engine.execute("SELECT * FROM generate_series(1)").is_err());
    assert!(engine
        .execute("SELECT * FROM generate_series(1, 2, 3, 4)")
        .is_err());
}

#[test]
fn expression_arguments_are_evaluated() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Arguments may be constant expressions, not just literals.
    assert_eq!(
        rows(&engine, "SELECT * FROM generate_series(1, 2 * 3)"),
        ints(&[1, 2, 3, 4, 5, 6])
    );
}

#[test]
fn column_alias_renames_output() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT n FROM generate_series(1, 3) AS t(n)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.columns[0].name, "n");
    assert_eq!(rs.rows, ints(&[1, 2, 3]));
}

#[test]
fn qualified_column_reference() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        rows(&engine, "SELECT t.n FROM generate_series(1, 3) AS t(n)"),
        ints(&[1, 2, 3])
    );
}

#[test]
fn with_ordinality_appends_counter() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT * FROM generate_series(10, 12) WITH ORDINALITY")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.columns.len(), 2);
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(10), Value::Int(1)],
            vec![Value::Int(11), Value::Int(2)],
            vec![Value::Int(12), Value::Int(3)],
        ]
    );
}

#[test]
fn where_filter_on_series_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        rows(
            &engine,
            "SELECT n FROM generate_series(1, 10) AS t(n) WHERE n > 7"
        ),
        ints(&[8, 9, 10])
    );
}

#[test]
fn order_by_desc_and_limit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        rows(
            &engine,
            "SELECT n FROM generate_series(1, 100) AS t(n) ORDER BY n DESC LIMIT 3"
        ),
        ints(&[100, 99, 98])
    );
}

#[test]
fn count_over_series() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM generate_series(1, 100)"),
        vec![vec![Value::Int(100)]]
    );
}

#[test]
fn explain_reports_function_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "EXPLAIN SELECT * FROM generate_series(1, 5)",
        "EXPLAIN FORMAT=TREE SELECT * FROM generate_series(1, 5)",
        "EXPLAIN FORMAT=JSON SELECT * FROM generate_series(1, 5)",
    ] {
        let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
            panic!("expected result set for: {sql}");
        };
        let text = rs
            .rows
            .iter()
            .flatten()
            .map(|v| format!("{v:?}"))
            .collect::<String>();
        assert!(
            text.contains("generate_series") || text.to_lowercase().contains("function"),
            "EXPLAIN missing function scan for `{sql}`: {text}"
        );
    }
}

// ---- M4: numeric (fixed-point) family ----

#[test]
fn numeric_series_decimal_step() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // 1.0, 1.5, 2.0 at scale 1
    assert_eq!(
        rows(&engine, "SELECT * FROM generate_series(1.0, 2.0, 0.5)"),
        vec![
            vec![Value::DecimalCompact(10, 1)],
            vec![Value::DecimalCompact(15, 1)],
            vec![Value::DecimalCompact(20, 1)],
        ]
    );
}

#[test]
fn numeric_series_mixed_int_and_decimal_promotes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // int start/stop + decimal step → promote whole series to decimal
    assert_eq!(
        rows(&engine, "SELECT * FROM generate_series(1, 2, 0.5)"),
        vec![
            vec![Value::DecimalCompact(10, 1)],
            vec![Value::DecimalCompact(15, 1)],
            vec![Value::DecimalCompact(20, 1)],
        ]
    );
}

#[test]
fn numeric_series_negative_step() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        rows(&engine, "SELECT * FROM generate_series(2.0, 1.0, -0.5)"),
        vec![
            vec![Value::DecimalCompact(20, 1)],
            vec![Value::DecimalCompact(15, 1)],
            vec![Value::DecimalCompact(10, 1)],
        ]
    );
}

#[test]
fn integer_args_stay_integer_family() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // pure integer bounds must NOT be promoted to decimal
    assert_eq!(
        rows(&engine, "SELECT * FROM generate_series(1, 3)"),
        ints(&[1, 2, 3])
    );
}

#[test]
fn numeric_series_column_type_is_decimal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT * FROM generate_series(1.0, 2.0, 0.5)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        matches!(
            rs.columns[0].column_type,
            crate::types::ColumnType::Decimal { scale: 1, .. }
        ),
        "got {:?}",
        rs.columns[0].column_type
    );
}

#[test]
fn numeric_series_count_over_decimal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // 0.5, 1.0, 1.5, 2.0 → 4 rows
    assert_eq!(
        rows(
            &engine,
            "SELECT COUNT(*) FROM generate_series(0.5, 2.0, 0.5)"
        ),
        vec![vec![Value::Int(4)]]
    );
}

// ---- M3: JOIN support ----

fn seed_t(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1), (2)").unwrap();
}

#[test]
fn join_table_with_series_on_condition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_t(&engine);
    // t.id=1 → gs.n in {1}; t.id=2 → gs.n in {1,2}
    assert_eq!(
        rows(
            &engine,
            "SELECT t.id, gs.n FROM t JOIN generate_series(1, 3) AS gs(n) \
             ON gs.n <= t.id ORDER BY t.id, gs.n"
        ),
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Int(1)],
            vec![Value::Int(2), Value::Int(2)],
        ]
    );
}

#[test]
fn join_series_on_left_side() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_t(&engine);
    let batch_before = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(Ordering::Relaxed);
    // series on the left, real table on the right
    assert_eq!(
        rows(
            &engine,
            "SELECT gs.n, t.id FROM generate_series(1, 2) AS gs(n) \
             JOIN t ON t.id = gs.n ORDER BY gs.n"
        ),
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Int(2)],
        ]
    );
    let batch_after = crate::engine::executor::select::vec_bridge::NATIVE_RESULT_BATCH_HITS
        .load(Ordering::Relaxed);
    assert!(
        batch_after > batch_before,
        "generate_series join must stay in the common batch graph"
    );
}

#[test]
fn left_join_series_preserves_unmatched() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_t(&engine);
    // gs.n = 1 only; t.id=2 has no match → NULL preserved
    assert_eq!(
        rows(
            &engine,
            "SELECT t.id, gs.n FROM t LEFT JOIN generate_series(1, 1) AS gs(n) \
             ON gs.n = t.id ORDER BY t.id"
        ),
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Null],
        ]
    );
}

#[test]
fn cross_join_series_cartesian() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_t(&engine);
    // 2 table rows × 3 series rows = 6
    assert_eq!(
        rows(
            &engine,
            "SELECT COUNT(*) FROM t CROSS JOIN generate_series(1, 3)"
        ),
        vec![vec![Value::Int(6)]]
    );
}

#[test]
fn join_series_group_by_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_t(&engine);
    // each t.id joined with gs.n <= t.id, count per id
    assert_eq!(
        rows(
            &engine,
            "SELECT t.id, COUNT(*) FROM t JOIN generate_series(1, 3) AS gs(n) \
             ON gs.n <= t.id GROUP BY t.id ORDER BY t.id"
        ),
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Int(2)],
        ]
    );
}

// ---- M4b: timestamp + interval family ----

fn formatted_dates(engine: &StorageQueryEngine, sql: &str) -> Vec<String> {
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    // The single projected column is DATE_FORMAT(...) → a string value.
    rs.rows
        .iter()
        .map(|r| {
            let dbg = format!("{:?}", r[0]);
            // Value::String("2026-01-01") / SmallString(...) → extract the quoted text.
            dbg.split('"').nth(1).unwrap_or(&dbg).to_string()
        })
        .collect()
}

#[test]
fn timestamp_series_daily() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        formatted_dates(
            &engine,
            "SELECT DATE_FORMAT(d, '%Y-%m-%d') FROM \
             generate_series('2026-01-01', '2026-01-04', INTERVAL '1' DAY) AS t(d)"
        ),
        vec!["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"]
    );
}

#[test]
fn timestamp_series_hourly_count_and_type() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT * FROM generate_series('2026-01-01 00:00:00', \
             '2026-01-01 03:00:00', INTERVAL '1' HOUR)",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 4); // 00,01,02,03
    assert!(
        matches!(
            rs.columns[0].column_type,
            crate::types::ColumnType::DateTime
        ),
        "got {:?}",
        rs.columns[0].column_type
    );
}

#[test]
fn timestamp_series_monthly_clamps_day() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Jan 31 + 1 month → Feb 28 (clamped), etc.
    let got = formatted_dates(
        &engine,
        "SELECT DATE_FORMAT(d, '%Y-%m-%d') FROM \
         generate_series('2026-01-31', '2026-04-30', INTERVAL '1' MONTH) AS t(d)",
    );
    assert_eq!(got.len(), 4);
    assert_eq!(got[0], "2026-01-31");
    assert_eq!(got[1], "2026-02-28");
}

#[test]
fn timestamp_series_descending() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        formatted_dates(
            &engine,
            "SELECT DATE_FORMAT(d, '%Y-%m-%d') FROM \
             generate_series('2026-01-03', '2026-01-01', INTERVAL '-1' DAY) AS t(d)"
        ),
        vec!["2026-01-03", "2026-01-02", "2026-01-01"]
    );
}

#[test]
fn timestamp_series_empty_when_direction_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // ascending interval but start > stop → empty
    assert!(rows(
        &engine,
        "SELECT * FROM generate_series('2026-01-05', '2026-01-01', INTERVAL '1' DAY)"
    )
    .is_empty());
}

// ---- M5: native vectorized integer leaf (ColumnVector::I64 direct) ----

#[test]
fn vectorized_integer_series_agg_matches_legacy() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // > DEFAULT_BATCH_ROWS (2048) to exercise multi-batch emission
    let sql = "SELECT COUNT(*), SUM(n), MIN(n), MAX(n) \
               FROM generate_series(1, 5000) AS t(n)";
    let vec = with_vec_mode(VecMode::On, || rows(&engine, sql));
    let legacy = with_vec_mode(VecMode::Off, || rows(&engine, sql));
    assert_eq!(vec, legacy, "vectorized vs legacy mismatch");
    // COUNT=5000, SUM(1..5000)=12502500, MIN=1, MAX=5000
    assert_eq!(vec[0][0], Value::Int(5000));
    assert_eq!(vec[0][2], Value::Int(1));
    assert_eq!(vec[0][3], Value::Int(5000));
}

#[test]
fn vectorized_series_with_step_and_negative_match_legacy() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "SELECT COUNT(*), SUM(n) FROM generate_series(1, 10000, 7) AS t(n)",
        "SELECT COUNT(*), SUM(n) FROM generate_series(10000, 1, -3) AS t(n)",
        "SELECT SUM(n) FROM generate_series(-2500, 2500) AS t(n)",
    ] {
        let vec = with_vec_mode(VecMode::On, || rows(&engine, sql));
        let legacy = with_vec_mode(VecMode::Off, || rows(&engine, sql));
        assert_eq!(vec, legacy, "mismatch for `{sql}`");
    }
}

#[test]
fn vectorized_series_with_ordinality_matches_legacy() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let sql = "SELECT SUM(n), SUM(ord) \
               FROM generate_series(100, 3000) WITH ORDINALITY AS t(n, ord)";
    let vec = with_vec_mode(VecMode::On, || rows(&engine, sql));
    let legacy = with_vec_mode(VecMode::Off, || rows(&engine, sql));
    assert_eq!(vec, legacy);
}

#[test]
fn vectorized_grouped_series_matches_legacy() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // GROUP BY a simple column engages the vec hash-agg native I64 leaf
    // (`try_build_generate_series_batch` → ColumnVector::I64 direct).
    let sql = "SELECT n, COUNT(*) FROM generate_series(1, 5000) AS t(n) \
               GROUP BY n ORDER BY n LIMIT 5";
    let vec = with_vec_mode(VecMode::On, || rows(&engine, sql));
    let legacy = with_vec_mode(VecMode::Off, || rows(&engine, sql));
    assert_eq!(vec, legacy);
    assert_eq!(vec[0][0], Value::Int(1));
}

#[test]
fn vectorized_filtered_series_stays_in_the_batch_pipeline() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let sql = "SELECT n, COUNT(*) FROM generate_series(1, 5000) AS t(n) \
               WHERE n % 7 = 0 GROUP BY n ORDER BY n LIMIT 5";
    let vec = with_vec_mode(VecMode::On, || rows(&engine, sql));
    let legacy = with_vec_mode(VecMode::Off, || rows(&engine, sql));
    assert_eq!(vec, legacy);
    assert_eq!(
        vec.first().and_then(|row| row.first()),
        Some(&Value::Int(7))
    );

    let explain = with_vec_mode(VecMode::On, || {
        rows(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {sql}"))
    })
    .into_iter()
    .flatten()
    .filter_map(|value| crate::types::value_str(&value).map(str::to_owned))
    .collect::<Vec<_>>()
    .join("\n");
    assert!(
        explain.contains("execution_mode=vector"),
        "filtered series unexpectedly left the batch pipeline:\n{explain}"
    );
}
