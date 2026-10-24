//! MySQL names an unaliased `CAST(<expr> AS <type>)` / `CONVERT(...)`
//! result-set column by its full original SQL text (`CAST(1 AS SIGNED)`), not
//! by the CAST body alone. sqlparser gives `Expr::Cast` / `Expr::Convert` a
//! span covering only the inner expression, so the naive source-span capture
//! used to drop the `CAST(` … `)` wrapper and emit `1 AS SIGNED` — where the
//! `AS <type>` misreads as an alias. `capture_select_item_text` recovers the
//! wrapper; these assert the derived column names round-trip MySQL 8.0.

use super::*;

fn column_name(engine: &StorageQueryEngine, sql: &str) -> String {
    match engine.execute(sql).unwrap() {
        QueryResponse::ResultSet(rs) => {
            assert_eq!(
                rs.columns.len(),
                1,
                "expected a single output column for {sql}"
            );
            rs.columns[0].name.clone()
        }
        other => panic!("expected ResultSet, got {other:?}"),
    }
}

#[test]
fn cast_unaliased_column_name_reproduces_full_cast_text() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // (SQL, expected result-set column name) — matching MySQL 8.0 exactly.
    let cases = [
        ("SELECT CAST(123 AS JSON)", "CAST(123 AS JSON)"),
        ("SELECT CAST(1 AS SIGNED)", "CAST(1 AS SIGNED)"),
        ("SELECT CAST(1 AS UNSIGNED)", "CAST(1 AS UNSIGNED)"),
        ("SELECT CAST('x' AS CHAR)", "CAST('x' AS CHAR)"),
        ("SELECT CAST('x' AS CHAR(10))", "CAST('x' AS CHAR(10))"),
        (
            "SELECT CAST(1 AS DECIMAL(10,2))",
            "CAST(1 AS DECIMAL(10,2))",
        ),
        (
            "SELECT CAST('2020-01-01' AS DATE)",
            "CAST('2020-01-01' AS DATE)",
        ),
        ("SELECT CAST('x' AS BINARY)", "CAST('x' AS BINARY)"),
    ];
    for (sql, expected) in cases {
        assert_eq!(
            column_name(&engine, sql),
            expected,
            "column name for `{sql}`"
        );
    }
}

#[test]
fn convert_unaliased_column_name_reproduces_full_convert_text() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let cases = [
        ("SELECT CONVERT(1, SIGNED)", "CONVERT(1, SIGNED)"),
        ("SELECT CONVERT('x', CHAR(4))", "CONVERT('x', CHAR(4))"),
        (
            "SELECT CONVERT('x' USING utf8mb4)",
            "CONVERT('x' USING utf8mb4)",
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(
            column_name(&engine, sql),
            expected,
            "column name for `{sql}`"
        );
    }
}

#[test]
fn explicit_alias_overrides_cast_source_text() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(column_name(&engine, "SELECT CAST(1 AS SIGNED) AS n"), "n");
}

#[test]
fn nested_cast_recovers_every_wrapper() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        column_name(&engine, "SELECT CAST(CAST(1 AS SIGNED) AS CHAR)"),
        "CAST(CAST(1 AS SIGNED) AS CHAR)"
    );
}
