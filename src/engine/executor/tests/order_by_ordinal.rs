//! ORDER BY ordinal regression: an integer literal refers to the Nth select item.
//! Previously ordinals were evaluated as constant keys, silently disabling sorting
//! and allowing row/vector group-order differences; out-of-range ordinals were also accepted.
//!
//! parser::resolve_order_by_refs replaces ordinals with select expressions,
//! benefiting planner Sort/TopK, vector bridges, and in-memory CTE paths uniformly.
//! Wildcard widths are unknown at parse time, so ordinal references return explicit
//! 1235 instead of silently failing to sort.

use super::*;
use crate::engine::executor::vec::config::{with_vec_mode, VecMode};

fn rows_of(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(r) => r.rows,
        QueryResponse::StreamingResultSet(mut r) => {
            let mut out = Vec::new();
            while let Ok(Some(row)) = r.cursor.next_row() {
                out.push(row);
            }
            out
        }
        other => panic!("expected result set, got {other:?}"),
    }
}

fn setup_vt(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE vt (id INT PRIMARY KEY, d DATE, dt DATETIME(6))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO vt VALUES \
             (1, '2024-05-31', '2024-05-31 12:34:56.987654'), \
             (2, '1999-12-31', '1999-12-31 23:59:59'), \
             (3, NULL, NULL), \
             (4, '0000-00-00', '0000-00-00 00:00:00'), \
             (5, '2024-02-29', '2024-02-29 00:00:01')",
        )
        .unwrap();
}

/// Reported shape: select expression, GROUP BY that expression, and ORDER BY ordinal.
/// Row and vector paths must sort YEAR(dt) ascending with NULL first, not preserve group order.
#[test]
fn group_by_expression_order_by_ordinal_sorts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_vt(&engine);
    let expected = vec![
        vec![Value::Null, Value::Int(1)],
        vec![Value::Int(0), Value::Int(1)],
        vec![Value::Int(1999), Value::Int(1)],
        vec![Value::Int(2024), Value::Int(2)],
    ];
    for sql in &[
        "SELECT YEAR(dt), COUNT(*) FROM vt GROUP BY YEAR(dt) ORDER BY 1",
        // The same applies when GROUP BY uses an ordinal resolved separately by the planner.
        "SELECT YEAR(dt), COUNT(*) FROM vt GROUP BY 1 ORDER BY 1",
    ] {
        let legacy = with_vec_mode(VecMode::Off, || rows_of(engine.execute(sql).unwrap()));
        let vec_rows = with_vec_mode(VecMode::On, || rows_of(engine.execute(sql).unwrap()));
        assert_eq!(legacy, expected, "row path not sorted by ordinal: {sql}");
        assert_eq!(vec_rows, expected, "vec path not sorted by ordinal: {sql}");
    }
}

/// An ordinal may reference an aggregate, e.g. ORDER BY 2 for COUNT(*).
#[test]
fn order_by_ordinal_aggregate_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_vt(&engine);
    let sql = "SELECT YEAR(dt), COUNT(*) FROM vt GROUP BY YEAR(dt) ORDER BY 2 DESC, 1";
    let expected = vec![
        vec![Value::Int(2024), Value::Int(2)],
        vec![Value::Null, Value::Int(1)],
        vec![Value::Int(0), Value::Int(1)],
        vec![Value::Int(1999), Value::Int(1)],
    ];
    let legacy = with_vec_mode(VecMode::Off, || rows_of(engine.execute(sql).unwrap()));
    let vec_rows = with_vec_mode(VecMode::On, || rows_of(engine.execute(sql).unwrap()));
    assert_eq!(legacy, expected, "row path not sorted by ordinal: {sql}");
    assert_eq!(vec_rows, expected, "vec path not sorted by ordinal: {sql}");
}

/// Without grouping, ordinals reference ordinary/expression columns, with DESC and multiple keys.
#[test]
fn order_by_ordinal_plain_select() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_vt(&engine);
    // YEAR(dt) ascending with NULL first, then id ascending within each year.
    let rows = rows_of(
        engine
            .execute("SELECT id, YEAR(dt) FROM vt ORDER BY 2, 1")
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(3), Value::Null],
            vec![Value::Int(4), Value::Int(0)],
            vec![Value::Int(2), Value::Int(1999)],
            vec![Value::Int(1), Value::Int(2024)],
            vec![Value::Int(5), Value::Int(2024)],
        ]
    );
    let rows = rows_of(engine.execute("SELECT id FROM vt ORDER BY 1 DESC").unwrap());
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(5)],
            vec![Value::Int(4)],
            vec![Value::Int(3)],
            vec![Value::Int(2)],
            vec![Value::Int(1)],
        ]
    );
}

/// Zero/out-of-range ordinals must return 1054 rather than being silently accepted.
#[test]
fn order_by_ordinal_out_of_range_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_vt(&engine);
    for (sql, pos) in &[
        ("SELECT id FROM vt ORDER BY 5", 5),
        ("SELECT id, dt FROM vt ORDER BY 0", 0),
    ] {
        let err = engine.execute(sql).unwrap_err();
        assert_eq!(err.code(), 1054, "wrong code for {sql}: {err:?}");
        assert!(
            err.message()
                .contains(&format!("Unknown column '{pos}' in 'order clause'")),
            "wrong message for {sql}: {}",
            err.message()
        );
    }
    // ORDER BY -1 is a constant expression, not an ordinal; MySQL accepts it.
    engine.execute("SELECT id FROM vt ORDER BY -1").unwrap();
}

/// Wildcard widths are bound against the schema before resolving ordinals.
#[test]
fn order_by_ordinal_with_wildcard_resolves_after_binding() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_vt(&engine);
    let positional = engine.execute("SELECT * FROM vt ORDER BY 2").unwrap();
    let named = engine.execute("SELECT * FROM vt ORDER BY dt").unwrap();
    let (QueryResponse::ResultSet(positional), QueryResponse::ResultSet(named)) =
        (positional, named)
    else {
        panic!("expected rows");
    };
    assert_eq!(positional.rows, named.rows);
}

/// UNION ORDER BY ordinals bind output columns in the executor after materialization.
#[test]
fn order_by_ordinal_union() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_vt(&engine);
    let rows = rows_of(
        engine
            .execute(
                "SELECT id FROM vt WHERE id <= 2 \
                 UNION SELECT id FROM vt WHERE id >= 4 ORDER BY 1 DESC",
            )
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Int(5)],
            vec![Value::Int(4)],
            vec![Value::Int(2)],
            vec![Value::Int(1)],
        ]
    );
    let err = engine
        .execute("SELECT id FROM vt UNION SELECT id FROM vt ORDER BY 3")
        .unwrap_err();
    assert_eq!(err.code(), 1054, "union ordinal out of range: {err:?}");
}
