//! Regression tests for the 2026-11-19 functions/expressions release audit.

use super::support::execute_materialized as exec;
use super::*;

fn query_rows(sql: &str) -> Vec<Vec<Value>> {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    query_rows_in(&engine, &mut s, sql)
}

fn query_rows_in(
    engine: &StorageQueryEngine,
    s: &mut SessionContext,
    sql: &str,
) -> Vec<Vec<Value>> {
    match exec(engine, s, sql).unwrap_or_else(|e| panic!("{sql}: {e:?}")) {
        QueryResponse::ResultSet(rs) => rs.rows,
        other => panic!("{sql}: expected result set, got {other:?}"),
    }
}

fn scalar(sql: &str) -> Value {
    query_rows(sql).remove(0).remove(0)
}

fn text(value: &str) -> Value {
    Value::String(value.into())
}

fn i(value: i64) -> Value {
    Value::Int(value)
}

fn u(value: u64) -> Value {
    Value::UInt(value)
}

/// FE-1: hex/bit literals are unsigned integers wherever a number is expected.
#[test]
fn audit_1119_hex_and_bit_literals_are_numeric_in_numeric_contexts() {
    let cases: &[(&str, Value)] = &[
        ("SELECT MOD(-1, 0xFF)", i(-1)),
        ("SELECT ELT(0x2, 'a', 'b')", text("b")),
        ("SELECT SUBSTRING('abcdef', 0x2, 0x3)", text("bcd")),
        ("SELECT LPAD('x', 0x5, 'y')", text("yyyyx")),
        ("SELECT 0xFF IN (255)", i(1)),
        ("SELECT 255 IN (0xFF)", i(1)),
        ("SELECT CASE 0xFF WHEN 255 THEN 'y' ELSE 'n' END", text("y")),
        ("SELECT b'1' IS FALSE", i(0)),
        ("SELECT 0x00 IS TRUE", i(0)),
        ("SELECT b'1010' IS NOT FALSE", i(1)),
        ("SELECT IF(0x00, 't', 'f')", text("f")),
        ("SELECT 0xFF BETWEEN 200 AND 300", i(1)),
        ("SELECT LEFT('abcdef', 0x2)", text("ab")),
        ("SELECT 0xFF + 1", u(256)),
        (
            "SELECT CASE WHEN 0xFF = 255 THEN 'y' ELSE 'n' END",
            text("y"),
        ),
        ("SELECT NULLIF(0xFF, 255)", Value::Null),
        ("SELECT HEX(NULLIF(0xFF, 254))", text("FF")),
        ("SELECT b'1010' - 127", i(-117)),
    ];
    for (sql, expected) in cases {
        assert_eq!(&scalar(sql), expected, "{sql}");
    }
}

/// FE-2: temporal function results read as numbers in numeric context.
#[test]
fn audit_1119_textual_temporal_results_are_numeric_in_numeric_contexts() {
    let cases: &[(&str, Value)] = &[
        (
            "SELECT DATE_ADD('2026-11-19', INTERVAL 1 DAY) + 0",
            i(20261120),
        ),
        ("SELECT ADDDATE('2026-11-19', 1) + 0", i(20261120)),
        ("SELECT MAKEDATE(2026, 60) + 0", i(20260301)),
        (
            "SELECT ADDTIME('2026-11-19 10:00:00', '01:00:00') + 0",
            i(20261119110000),
        ),
        (
            "SELECT DATE_ADD('2026-11-19', INTERVAL 1 DAY) = 20261120",
            i(1),
        ),
        (
            "SELECT DATE_ADD('2026-11-19', INTERVAL 1 DAY) > 20261119",
            i(1),
        ),
    ];
    for (sql, expected) in cases {
        let value = scalar(sql);
        let same = match (&value, expected) {
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::UInt(a), Value::Int(b)) => i64::try_from(*a).ok() == Some(*b),
            (Value::Decimal(a), Value::Int(b)) => a.trim_end_matches(".0") == b.to_string(),
            (a, b) => a == b,
        };
        assert!(same, "{sql}: {value:?}");
    }
}

/// FE-5: a negative approximate number converts through BIGINT first.
#[test]
fn audit_1119_negative_double_cast_to_unsigned_wraps() {
    for (sql, expected) in [
        ("SELECT CAST(-1.5e0 AS UNSIGNED)", u(u64::MAX - 1)),
        ("SELECT CAST(-1.4e0 AS UNSIGNED)", u(u64::MAX)),
        ("SELECT CAST(-2.5e0 AS UNSIGNED)", u(u64::MAX - 1)),
        ("SELECT CAST(-0.4e0 AS UNSIGNED)", u(0)),
        ("SELECT CAST(1.5e0 AS UNSIGNED)", u(2)),
    ] {
        assert_eq!(scalar(sql), expected, "{sql}");
    }
}

/// FE-7: ROLLUP over an empty input has no groups and therefore no rows.
#[test]
fn audit_1119_rollup_over_empty_input_returns_no_rows() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE TABLE fr(g INT, x INT)").unwrap();
    exec(
        &engine,
        &mut s,
        "INSERT INTO fr VALUES (1, 10), (1, 20), (2, 5)",
    )
    .unwrap();
    for sql in [
        "SELECT g, SUM(x) FROM fr WHERE 1 = 0 GROUP BY g WITH ROLLUP",
        "SELECT g, COUNT(*) FROM fr WHERE x > 100 GROUP BY g WITH ROLLUP",
        "SELECT g, x, COUNT(*) FROM fr WHERE 1 = 0 GROUP BY g, x WITH ROLLUP",
    ] {
        assert!(query_rows_in(&engine, &mut s, sql).is_empty(), "{sql}");
    }
    // The non-empty case still emits the grand total, and an ungrouped
    // aggregate over empty input still returns its single row.
    let rows = query_rows_in(
        &engine,
        &mut s,
        "SELECT g, SUM(x) FROM fr GROUP BY g WITH ROLLUP",
    );
    assert_eq!(rows.len(), 3);
    let rows = query_rows_in(&engine, &mut s, "SELECT COUNT(*) FROM fr WHERE 1 = 0");
    assert_eq!(rows, vec![vec![i(0)]]);
}

/// FE-15: an omitted or NULL mode selects the default week mode.
#[test]
fn audit_1119_week_with_null_mode_uses_default_mode() {
    assert_eq!(
        scalar("SELECT WEEK('2024-01-07', NULL)"),
        scalar("SELECT WEEK('2024-01-07')")
    );
    assert_eq!(
        scalar("SELECT YEARWEEK('2024-01-07', NULL)"),
        scalar("SELECT YEARWEEK('2024-01-07')")
    );
    assert_eq!(scalar("SELECT WEEK(NULL, 3)"), Value::Null);
    assert_eq!(scalar("SELECT WEEK('2024-01-07', 3)"), i(1));
}

/// FE-3 / FE-4: GREATEST/LEAST honor temporal operands and explicit collations.
#[test]
fn audit_1119_greatest_least_temporal_and_collation_semantics() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(
        &engine,
        &mut s,
        "CREATE TABLE fd(id INT PRIMARY KEY, d DATE, s VARCHAR(20))",
    )
    .unwrap();
    exec(
        &engine,
        &mut s,
        "INSERT INTO fd VALUES (1,'2020-01-01','2019-12-31'),(2,'2020-01-02','x'),(3,NULL,'2021-01-01')",
    )
    .unwrap();
    let rows = query_rows_in(
        &engine,
        &mut s,
        "SELECT id, GREATEST(d, s) FROM fd ORDER BY id",
    );
    let shown: Vec<String> = rows.iter().map(|row| row[1].to_display_string()).collect();
    assert_eq!(shown, ["2020-01-01", "2020-01-02", "NULL"]);
    // The unparsable operand is skipped (with a warning), not an error.
    let rows = query_rows_in(
        &engine,
        &mut s,
        "SELECT id, CASE d WHEN s THEN 'eq' ELSE 'ne' END FROM fd ORDER BY id",
    );
    assert_eq!(
        rows.iter().map(|row| row[1].clone()).collect::<Vec<_>>(),
        [text("ne"), text("ne"), text("ne")]
    );
    assert_eq!(
        scalar("SELECT GREATEST(CAST('2020-01-01' AS DATE), 'x')").to_display_string(),
        "2020-01-01"
    );
    assert_eq!(
        scalar("SELECT LEAST(CAST('2020-01-01' AS DATE), '2019-01-01')").to_display_string(),
        "2019-01-01"
    );
    assert_eq!(
        scalar("SELECT GREATEST('a', 'B' COLLATE utf8mb4_bin)"),
        text("a")
    );
    assert_eq!(
        scalar("SELECT LEAST('a' COLLATE utf8mb4_bin, 'B')"),
        text("B")
    );
    assert_eq!(scalar("SELECT GREATEST('a', 'B')"), text("B"));
}

/// FE-6: `_binary` operands are binary strings, so bit operators work byte-wise.
#[test]
fn audit_1119_binary_introducer_bitwise_is_bytewise() {
    assert_eq!(
        scalar("SELECT HEX(_binary'ab' ^ _binary'cd')"),
        text("0206")
    );
    assert_eq!(scalar("SELECT HEX(~_binary'ab')"), text("9E9D"));
    assert_eq!(scalar("SELECT HEX(_binary'ab' << 3)"), text("0B10"));
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    let err = exec(&engine, &mut s, "SELECT _binary'ab' & _binary'abc'").unwrap_err();
    assert_eq!(err.code(), 3513);
}

/// FE-1 residual: an introducer's octets belong to the introducer charset.
#[test]
fn audit_1119_convert_of_introducer_literal_decodes_source_charset() {
    assert_eq!(
        scalar("SELECT HEX(CONVERT(_latin1 X'E9' USING utf8mb4))"),
        text("C3A9")
    );
}

/// FE-9: `@v := expr` has the type of the assigned value.
#[test]
fn audit_1119_user_variable_assignment_has_value_type() {
    use crate::engine::expr_type::column_type::expr_result_type;
    use crate::types::ColumnType;
    let parse = |sql: &str| crate::parser::parse_expr_fragment(sql).unwrap();
    assert_eq!(expr_result_type(&parse("@a := 1")), ColumnType::LongLong);
    assert_eq!(expr_result_type(&parse("@a := 1.5e0")), ColumnType::Double);
    assert_eq!(expr_result_type(&parse("@a := 'x'")), ColumnType::VarString);
}
