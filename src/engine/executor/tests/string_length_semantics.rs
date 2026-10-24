//! End-to-end tests for CHAR(N) read semantics and the
//! length / range constraints on VARCHAR / BINARY / VARBINARY / BIT.
//!
//! MySQL 8.0:
//!  - `CHAR(N)` trims trailing pad spaces on read unless PAD_CHAR_TO_FULL_LENGTH
//!    is enabled. mydb does not support that legacy SQL mode yet.
//!    `VARCHAR(N)` keeps the exact stored text.
//!  - `VARCHAR(N)` / `VARBINARY(N)` reject overlong inputs with
//!    SQLSTATE `22001` (MySQL error 1406).
//!  - `BINARY(N)` right-pads with `0x00` on write and stores the padded
//!    bytes verbatim.
//!  - `BIT(N)` rejects values outside `[0, 2^N - 1]` with SQLSTATE
//!    `22003` (MySQL error 1264).

use super::support::query_rows as rows;
use super::*;

fn cell(engine: &StorageQueryEngine, sql: &str) -> String {
    let r = rows(engine, sql);
    assert_eq!(r.len(), 1, "expected one row");
    assert_eq!(r[0].len(), 1, "expected one column");
    r[0][0].to_display_string()
}

#[test]
fn string_extent_arguments_saturate_exact_unsigned_and_decimal_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    assert_eq!(
        rows(
            &engine,
            "SELECT LEFT('hello', 18446744073709551615), \
                    LEFT('hello', -18446744073709551615), \
                    LOCATE('lo', 'hello', -18446744073709551616), \
                    SUBSTRING('hello', 18446744073709551616, 1), \
                    INSERT('hello', 18446744073709551617, 1, 'hi'), \
                    REPEAT('hello', -18446744073709551617)"
        ),
        vec![vec![
            Value::String("hello".into()),
            Value::String(String::new()),
            Value::Int(0),
            Value::String(String::new()),
            Value::String("hello".into()),
            Value::String(String::new()),
        ]]
    );
}

#[test]
fn static_string_extent_conversion_publishes_fix_and_runtime_warnings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    for (sql, expected_warnings) in [
        ("SELECT LOCATE('lo','hello',-18446744073709551616)", 1),
        ("SELECT LEFT('hello',-18446744073709551616)", 2),
        (
            "SELECT SUBSTRING('hello',-18446744073709551616,-18446744073709551616)",
            4,
        ),
        ("SELECT REPEAT('hello',-18446744073709551616)", 2),
        ("SELECT SPACE(-18446744073709551616)", 2),
        ("SELECT LPAD('hello',-18446744073709551616,'1')", 2),
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
        assert_eq!(
            session.warnings.len(),
            expected_warnings,
            "unexpected warning count for {sql}"
        );
        assert!(
            session
                .warnings
                .iter()
                .all(|warning| warning.code() == 1292),
            "all extent diagnostics must be ER_TRUNCATED_WRONG_VALUE for {sql}"
        );
    }
}

#[test]
fn pad_result_metadata_uses_declared_extent_and_string_decimals() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET NAMES latin1")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE pad_meta (i INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO pad_meta VALUES (1)")
        .unwrap();

    for function in ["LPAD", "RPAD"] {
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(
                &mut session,
                &format!("SELECT {function}(i, 7, ' ') FROM pad_meta"),
            )
            .unwrap()
        else {
            panic!("expected result set");
        };
        assert_eq!(result.columns[0].source.length, Some(7));
        assert_eq!(result.columns[0].source.decimals, Some(31));
    }
}

#[test]
fn unhex_longtext_metadata_is_half_width_longblob() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE unhex_meta (x LONGTEXT)")
        .unwrap();
    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT UNHEX(x) FROM unhex_meta").unwrap()
    else {
        panic!("expected result set");
    };
    let column = &result.columns[0];
    assert_eq!(column.column_type, crate::types::ColumnType::Blob);
    assert_eq!(column.source.length, Some(2_147_483_648));
    assert_eq!(column.source.decimals, Some(31));
    assert_ne!(column.source.flags & crate::mysql::protocol::BINARY_FLAG, 0);
}

#[test]
fn format_float_metadata_reserves_grouping_and_max_decimals() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET NAMES latin1")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE format_meta (a FLOAT)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT FORMAT(a, 2) FROM format_meta")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.columns[0].source.length, Some(48));
    assert_eq!(result.columns[0].source.decimals, Some(31));
}

#[test]
fn char_n_trims_trailing_pad_spaces_on_read() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, c CHAR(5))")
        .unwrap();
    // Storage trims trailing spaces (compact form), and the default
    // MySQL 8 visible value is compact as well.
    engine
        .execute("INSERT INTO t VALUES (1, 'ab'), (2, 'cdefg'), (3, '')")
        .unwrap();
    assert_eq!(cell(&engine, "SELECT c FROM t WHERE id=1"), "ab");
    assert_eq!(cell(&engine, "SELECT c FROM t WHERE id=2"), "cdefg");
    assert_eq!(cell(&engine, "SELECT c FROM t WHERE id=3"), "");
}

#[test]
fn varchar_does_not_pad_on_read() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(5))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 'ab')").unwrap();
    // VARCHAR keeps the exact stored characters — no pad.
    assert_eq!(cell(&engine, "SELECT v FROM t WHERE id=1"), "ab");
}

#[test]
fn varchar_overflow_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(3))")
        .unwrap();
    let err = engine
        .execute("INSERT INTO t VALUES (1, 'abcd')")
        .expect_err("overlong VARCHAR must error");
    assert_eq!(
        err.code(),
        1406,
        "expected MySQL error 1406, got {}",
        err.code()
    );
    assert_eq!(err.sql_state(), "22001");
}

#[test]
fn char_overflow_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, c CHAR(3))")
        .unwrap();
    let err = engine
        .execute("INSERT INTO t VALUES (1, 'abcd')")
        .expect_err("overlong CHAR must error");
    assert_eq!(err.code(), 1406);
    assert_eq!(err.sql_state(), "22001");
}

#[test]
fn binary_right_pads_with_zero() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, b BINARY(4))")
        .unwrap();
    // Use an ASCII literal so the test is independent of how mydb
    // currently parses `X'..'` hex literals (which it tends to treat as
    // text). The constraint we care about — right-pad to N bytes with
    // 0x00 — is the same regardless.
    engine.execute("INSERT INTO t VALUES (1, 'ab')").unwrap();
    assert_eq!(
        cell(&engine, "SELECT HEX(b) FROM t WHERE id=1"),
        "61620000" // 'a' 'b' 0x00 0x00
    );
}

#[test]
fn varbinary_overflow_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, b VARBINARY(2))")
        .unwrap();
    let err = engine
        .execute("INSERT INTO t VALUES (1, X'AABBCC')")
        .expect_err("overlong VARBINARY must error");
    assert_eq!(err.code(), 1406);
    assert_eq!(err.sql_state(), "22001");
}

#[test]
fn bit_n_range_is_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, b BIT(3))")
        .unwrap();
    // 0..=7 ok
    engine.execute("INSERT INTO t VALUES (1, 7)").unwrap();
    // 8 overflows BIT(3)
    let err = engine
        .execute("INSERT INTO t VALUES (2, 8)")
        .expect_err("BIT(3) must reject 8");
    assert_eq!(err.code(), 1406);
    assert_eq!(err.sql_state(), "22001");
    // negative value also rejected
    let err2 = engine
        .execute("INSERT INTO t VALUES (3, -1)")
        .expect_err("BIT(3) must reject -1");
    assert_eq!(err2.code(), 1264);
}

#[test]
fn char_padding_groups_logically_equal_values() {
    // CHAR storage strips trailing pad spaces, so values that differ only
    // by those pads land in the same logical group even under MySQL 8's
    // default NO PAD collation.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, c CHAR(5))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'ab'), (2, 'ab   '), (3, 'cd')")
        .unwrap();
    // Both inserted values store as the same compact CHAR value.
    let r = rows(&engine, "SELECT COUNT(*) FROM t GROUP BY c ORDER BY c");
    let counts: Vec<i64> = r
        .into_iter()
        .map(|row| match row[0] {
            Value::Int(v) => v,
            Value::UInt(v) => v as i64,
            _ => panic!("expected an integer count, got {:?}", row[0]),
        })
        .collect();
    assert_eq!(counts, vec![2, 1]);
}

#[test]
fn char_predicate_matches_unpadded_literal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE region (r_regionkey INT PRIMARY KEY, r_name CHAR(25) NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO region VALUES (1, 'ASIA'), (2, 'EUROPE')")
        .unwrap();

    let r = rows(
        &engine,
        "SELECT r_regionkey FROM region WHERE r_name = 'ASIA'",
    );
    assert_eq!(r, vec![vec![Value::Int(1)]]);
}

// G1: non-strict sql_mode string truncation warns 1265.

#[test]
fn nonstrict_string_truncates_with_warning_1265() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(3), c CHAR(3))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'",
        )
        .unwrap();
    // Nonspace overflow truncates with warning 1265.
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 'abcd', 'wxyz')")
        .unwrap();
    let warns: Vec<i64> = match engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    {
        QueryResponse::ResultSet(r) => r
            .rows
            .into_iter()
            .map(|row| match row[1] {
                Value::Int(c) => c,
                Value::UInt(c) => c as i64,
                ref o => panic!("bad code {o:?}"),
            })
            .collect(),
        other => panic!("expected result set, got {other:?}"),
    };
    assert!(
        warns.iter().filter(|c| **c == 1265).count() >= 2,
        "expected two 1265 warnings (VARCHAR + CHAR), got {warns:?}"
    );
    let row = match engine
        .execute_with_session(&mut session, "SELECT v, c FROM t WHERE id = 1")
        .unwrap()
    {
        QueryResponse::ResultSet(r) => r.rows.into_iter().next().unwrap(),
        other => panic!("expected result set, got {other:?}"),
    };
    assert_eq!(row[0].to_display_string(), "abc", "VARCHAR truncated to 3");
    assert_eq!(row[1].to_display_string(), "wxy", "CHAR truncated to 3");
}

#[test]
fn bare_char_defaults_to_one_and_bounded_float_text_compacts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE c1 (v CHAR)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO c1 VALUES (5000.0)")
        .unwrap();
    let warnings = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap();
    let QueryResponse::ResultSet(warnings) = warnings else {
        panic!("expected warning result set");
    };
    assert!(warnings.rows.iter().any(|row| row[1] == Value::Int(1265)));
    let QueryResponse::ResultSet(c1) = engine
        .execute_with_session(&mut session, "SELECT v FROM c1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(c1.rows, vec![vec![Value::String("5".to_string())]]);

    engine
        .execute_with_session(&mut session, "CREATE TABLE c6 (v CHAR(6))")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO c6 VALUES (2e5),(2e6),(2e-4),(2e-5)",
        )
        .unwrap();
    let QueryResponse::ResultSet(c6) = engine
        .execute_with_session(&mut session, "SELECT v FROM c6 ORDER BY v")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let actual: Vec<String> = c6
        .rows
        .iter()
        .map(|row| row[0].to_display_string())
        .collect();
    assert_eq!(actual, vec!["0.0002", "200000", "2e-5", "2e6"]);
}

#[test]
fn varchar_trailing_space_overflow_publishes_note() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(3))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'",
        )
        .unwrap();
    // VARCHAR writes dropping only trailing pad spaces succeed but emit Note 1265.
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 'abc  ')")
        .unwrap();
    let warnings = match engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    {
        QueryResponse::ResultSet(r) => r.rows,
        other => panic!("expected result set, got {other:?}"),
    };
    assert_eq!(warnings.len(), 1, "expected one VARCHAR truncation note");
    assert_eq!(warnings[0][0].to_display_string(), "Note");
    assert_eq!(warnings[0][1].to_display_string(), "1265");
}

#[test]
fn unary_binary_disables_pad_space_comparison() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT BINARY 'a ' = 'a', BINARY 'a ' = 'a '")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(0), Value::Int(1)]]);
}

// G6 probe: enforce column character-set representability at INSERT.

#[test]
fn g6_insert_rejects_unrepresentable_char_for_column_charset() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(16) CHARACTER SET latin1)")
        .unwrap();
    // U+4E2D is unrepresentable in latin1 and returns MySQL error 1366, incorrect string value.
    let err = engine
        .execute("INSERT INTO t VALUES (1, '中')")
        .unwrap_err();
    assert_eq!(err.code(), 1366, "expected 1366, got {err:?}");
    // Characters representable in latin1 insert normally.
    engine.execute("INSERT INTO t VALUES (2, 'café')").unwrap();
    let out = rows(&engine, "SELECT v FROM t WHERE id = 2");
    assert_eq!(out[0][0].to_display_string(), "café");
}

#[test]
fn ctas_infers_expression_lob_from_static_bound() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE repeat_ctas AS SELECT REPEAT('a', 66000) AS payload")
        .unwrap();
    assert_eq!(
        cell(&engine, "SELECT LENGTH(payload) FROM repeat_ctas"),
        "66000"
    );
    let table = engine.database().table("test.repeat_ctas").unwrap();
    assert!(
        matches!(
            table.columns[0].data_type,
            crate::storage::DataTypeKind::Text { max_len }
                if max_len == crate::storage::schema::MEDIUM_LOB_MAX
        ),
        "expected MEDIUMTEXT for the bound REPEAT domain: {:?}",
        table.columns[0].data_type
    );
}

#[test]
fn insert_ignore_truncates_binary_values_instead_of_dropping_the_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = crate::engine::api::SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE binary_ignore (b BINARY(2), vb VARBINARY(2))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT IGNORE INTO binary_ignore VALUES (x'412020', x'424220')",
        )
        .unwrap();
    let warnings = match engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    {
        QueryResponse::ResultSet(result) => result.rows,
        other => panic!("expected warnings result set, got {other:?}"),
    };
    assert_eq!(
        match engine
            .execute_with_session(&mut session, "SELECT HEX(b), HEX(vb) FROM binary_ignore",)
            .unwrap()
        {
            QueryResponse::ResultSet(result) => result.rows,
            other => panic!("expected result set, got {other:?}"),
        },
        vec![vec![
            Value::String("4120".into()),
            Value::String("4242".into())
        ]]
    );
    assert!(
        warnings
            .iter()
            .filter(|row| row.get(1).is_some_and(|v| v.to_display_string() == "1265"))
            .count()
            >= 2,
        "both truncated columns must report warning 1265: {warnings:?}"
    );

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE text_pad_truncation (c CHAR(2), vc VARCHAR(2))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO text_pad_truncation VALUES (x'412020', x'412020')",
        )
        .unwrap();
}

#[test]
fn insert_values_expressions_resolve_target_columns_left_to_right() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE insert_expr (
                a VARCHAR(100) DEFAULT 'test',
                b VARCHAR(100) DEFAULT 'fallback'
            )",
        )
        .unwrap();
    engine
        .execute("INSERT INTO insert_expr VALUES (SUBSTR(a, 1, 3), CONCAT(a, '-ok'))")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT a, b FROM insert_expr"),
        vec![vec![
            Value::String("tes".into()),
            Value::String("tes-ok".into())
        ]]
    );
}
