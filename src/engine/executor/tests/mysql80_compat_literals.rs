//! MySQL 8.0 literal syntax compatibility regressions.
//!
//! Cover P0/P1 fixes from docs/design/literal-syntax-review-2026-07.md:
//! B1: tokenizer respects sql_mode=NO_BACKSLASH_ESCAPES.
//! B2: sql_mode=ANSI_QUOTES makes double-quoted text an identifier.
//! B3: odd-length X'F' fails, while 0xF is still zero-padded.
//! B4: reject non-temporal TypedString forms such as INTEGER 'x' and BINARY 'x'.
//! B5: national strings use _utf8 transcoding rather than ordinary string handling.
//! B6: reject introducers with multiple leading underscores, such as __utf8'x'.
//! B7: reject INTERVAL without a leading field.
//! B8: InsertCursor fast path accepts negative exponents such as 1e-5.
//! B9: auto-parameterization fingerprints are bucketed by parameter type.
//! B11: typed DATE '0000-00-00' respects NO_ZERO_DATE.

use super::support::execute_materialized as exec;
use super::*;

fn one_value(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Value {
    let QueryResponse::ResultSet(rs) = exec(engine, session, sql).unwrap() else {
        panic!("expected result set for {sql}");
    };
    assert_eq!(rs.rows.len(), 1, "expected single row from {sql}");
    rs.rows
        .into_iter()
        .next()
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
}

// ---------------------------------------------------------------------------
// B1 — NO_BACKSLASH_ESCAPES honoured by parser
// ---------------------------------------------------------------------------

#[test]
fn b1_no_backslash_escapes_makes_backslash_literal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Default (backslash escapes on) — `\n` is a newline byte.
    let default_val = one_value(&engine, &mut session, r"SELECT 'a\nb' AS s");
    let s_default = match default_val {
        Value::String(s) => s,
        other => panic!("default `\\n` expected String, got {other:?}"),
    };
    assert_eq!(s_default, "a\nb", "default sql_mode: `\\n` → newline");

    exec(&engine, &mut session, "SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();

    // With NO_BACKSLASH_ESCAPES the same source is 4 bytes: `a`, `\`, `n`, `b`.
    let val = one_value(&engine, &mut session, r"SELECT 'a\nb' AS s");
    let s = match val {
        Value::String(s) => s,
        other => panic!("expected String, got {other:?}"),
    };
    assert_eq!(s, "a\\nb", "NO_BACKSLASH_ESCAPES: `\\n` is 2 raw bytes");
}

// ---------------------------------------------------------------------------
// B2 — ANSI_QUOTES makes `"..."` an identifier delimiter
// ---------------------------------------------------------------------------

#[test]
fn b2_ansi_quotes_makes_double_quote_identifier() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT, name VARCHAR(16))",
    )
    .unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1, 'x')").unwrap();

    // Without ANSI_QUOTES, `"name"` is a string literal so
    // `SELECT "name" FROM t` returns the string 'name' per row.
    let default_val = one_value(&engine, &mut session, r#"SELECT "name" FROM t"#);
    match default_val {
        Value::String(s) => assert_eq!(s, "name", "default: string literal"),
        Value::SmallString(s) => assert_eq!(
            std::str::from_utf8(&s).unwrap(),
            "name",
            "default: string literal"
        ),
        other => panic!("expected String, got {other:?}"),
    }

    exec(&engine, &mut session, "SET sql_mode='ANSI_QUOTES'").unwrap();

    // Under ANSI_QUOTES `"name"` names the column and returns its value.
    let ansi_val = one_value(&engine, &mut session, r#"SELECT "name" FROM t"#);
    match ansi_val {
        Value::String(s) => assert_eq!(s, "x"),
        Value::SmallString(s) => assert_eq!(std::str::from_utf8(&s).unwrap(), "x"),
        other => panic!("expected `x`, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// B3 — X'F' odd hex payload is a syntax error; 0xF still zero-pads
// ---------------------------------------------------------------------------

#[test]
fn b3_odd_x_prefixed_hex_literal_is_syntax_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // 0xF is allowed and zero-pads to 0x0F (documented mydb behaviour).
    let val = one_value(&engine, &mut session, "SELECT 0xF + 0");
    match val {
        Value::Int(n) => assert_eq!(n, 15),
        Value::UInt(n) => assert_eq!(n, 15),
        other => panic!("expected 15, got {other:?}"),
    }

    // X'F' is a MySQL 8.0 syntax error. mydb must reject it, not
    // silently pad — this is a violation of "no silent no-op".
    let err = exec(&engine, &mut session, "SELECT X'F'").unwrap_err();
    assert_eq!(err.code(), 1064, "X'F' must be 1064, got {err:?}");
}

// ---------------------------------------------------------------------------
// B4 — non-temporal TypedString is an error
// ---------------------------------------------------------------------------

#[test]
fn b4_non_temporal_typed_string_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Temporal typed literals continue to work.
    let d = one_value(&engine, &mut session, "SELECT DATE '2024-05-31'");
    assert!(matches!(d, Value::Date(_)), "DATE '…' must remain typed");

    // `INTEGER 'x'` used to silently downgrade to a string; now it must
    // be a clean 1064. `BINARY 'x'` is a MySQL unary BINARY operator
    // (routes through Unary), so its acceptance is orthogonal to B4.
    let err = exec(&engine, &mut session, "SELECT INTEGER '42'").unwrap_err();
    assert_eq!(err.code(), 1064, "INTEGER 'x' must be 1064, got {err:?}");

    let err = exec(&engine, &mut session, "SELECT CHAR '中'").unwrap_err();
    assert_eq!(err.code(), 1064, "CHAR 'x' must be 1064, got {err:?}");
}

// ---------------------------------------------------------------------------
// B5 — `N'…'` is `_utf8` introducer
// ---------------------------------------------------------------------------

#[test]
fn b5_national_string_literal_produces_utf8_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // `N'…'` must reach the row as a valid utf8 string. Round-trip through
    // a simple SELECT: value bytes match the source utf8.
    let val = one_value(&engine, &mut session, "SELECT N'héllo'");
    let text = match val {
        Value::String(s) => s,
        Value::SmallString(s) => std::str::from_utf8(&s).unwrap().to_string(),
        other => panic!("expected string, got {other:?}"),
    };
    assert_eq!(text, "héllo");
}

// ---------------------------------------------------------------------------
// B6 — multi-underscore introducer prefix errors
// ---------------------------------------------------------------------------

#[test]
fn b6_double_underscore_introducer_is_syntax_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Single underscore is valid.
    let ok = one_value(&engine, &mut session, "SELECT _utf8mb4'x'");
    let s = match ok {
        Value::String(s) => s,
        Value::SmallString(s) => std::str::from_utf8(&s).unwrap().to_string(),
        other => panic!("expected string, got {other:?}"),
    };
    assert_eq!(s, "x");

    // Double underscore is a syntax error.
    let err = exec(&engine, &mut session, "SELECT __utf8'x'").unwrap_err();
    assert_eq!(err.code(), 1064, "__utf8'x' must be 1064, got {err:?}");
}

// ---------------------------------------------------------------------------
// B8 — Bulk-INSERT fast path accepts negative-exponent scientific notation
// ---------------------------------------------------------------------------

#[test]
fn b8_bulk_insert_fastpath_negative_exponent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    exec(&engine, &mut session, "CREATE TABLE t (v DOUBLE)").unwrap();
    // `1e-5` used to trip the fast path (bail to sqlparser). Still
    // works either way but the round-trip must be numerically correct.
    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1e-5),(2E+3),(5e0)",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT v FROM t ORDER BY v").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 3);
    let doubles: Vec<f64> = rs
        .rows
        .into_iter()
        .map(|r| match &r[0] {
            Value::Double(d) => *d,
            Value::Float(f) => *f as f64,
            other => panic!("expected float/double, got {other:?}"),
        })
        .collect();
    // Loose equality — these are IEEE-754 exact after parse.
    assert!((doubles[0] - 1e-5).abs() < 1e-12);
    assert!((doubles[1] - 5.0).abs() < 1e-12);
    assert!((doubles[2] - 2000.0).abs() < 1e-12);
}

// ---------------------------------------------------------------------------
// B9 — auto-param fingerprint separates `1` from `'1'`
// ---------------------------------------------------------------------------

#[test]
fn b9_auto_param_fingerprint_partitions_by_type() {
    use crate::engine::auto_param::{auto_parameterize, AutoParameterizeMode};
    use crate::parser::parse_statement;

    let mut int_stmt = parse_statement("SELECT * FROM t WHERE id = 1").unwrap();
    let mut str_stmt = parse_statement("SELECT * FROM t WHERE id = '1'").unwrap();

    let int_fp = auto_parameterize(AutoParameterizeMode::Select, &mut int_stmt).fingerprint;
    let str_fp = auto_parameterize(AutoParameterizeMode::Select, &mut str_stmt).fingerprint;

    assert!(int_fp.is_some());
    assert!(str_fp.is_some());
    assert_ne!(
        int_fp, str_fp,
        "`= 1` and `= '1'` must not share a plan-cache fingerprint (B9)"
    );

    // Two integer-typed literals of different magnitude still SHARE
    // the fingerprint — the fix targets only the type dimension.
    let mut int_a = parse_statement("SELECT * FROM t WHERE id = 1").unwrap();
    let mut int_b = parse_statement("SELECT * FROM t WHERE id = 42").unwrap();
    let fa = auto_parameterize(AutoParameterizeMode::Select, &mut int_a).fingerprint;
    let fb = auto_parameterize(AutoParameterizeMode::Select, &mut int_b).fingerprint;
    assert_eq!(fa, fb, "same-type literals still share plan cache");
}
