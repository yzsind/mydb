//! M1 JSON error regressions pin MySQL 8.0-compatible error emission.
//!
//! Cover 3141/3143/3149/3154/3157/3158/3903; internal corrupt-jsonb error 3140
//! cannot be triggered by user SQL and is outside this coverage.
//!
//! Each test asserts the error code, sometimes SQLSTATE or message keywords,
//! preventing strict errors from silently becoming permissive success or generic 3141.

use super::support::execute_materialized as exec;
use super::*;

fn err(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> QueryError {
    exec(engine, session, sql).expect_err("expected an error")
}

fn assert_code(sql: &str, want: u16, got: &QueryError) {
    assert_eq!(
        got.code(),
        want,
        "{sql}: expected error {want}, got {} ({})",
        got.code(),
        got.message()
    );
}

fn cell(engine: &StorageQueryEngine, sql: &str) -> String {
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set for `{sql}`");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.rows[0].len(), 1);
    rs.rows[0][0].to_display_string()
}

// ── 3141 ER_INVALID_JSON_TEXT_IN_PARAM ────────────────────────────────

#[test]
fn cast_invalid_text_to_json_reports_3141() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    // Malformed JSON in a function argument (json_doc_arg) → 3141.
    let e = err(&engine, &mut session, "SELECT CAST('{invalid' AS JSON)");
    assert_code("CAST 3141", 3141, &e);
}

#[test]
fn json_length_bad_text_reports_3141() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let e = err(&engine, &mut session, "SELECT JSON_LENGTH('not-json')");
    assert_code("JSON_LENGTH 3141", 3141, &e);
}

// ── 3143 ER_INVALID_JSON_PATH ─────────────────────────────────────────

#[test]
fn json_extract_bad_path_reports_3143() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    // Path must start with `$` — a bare key raises 3143.
    let e = err(
        &engine,
        &mut session,
        "SELECT JSON_EXTRACT('{\"a\":1}', 'a')",
    );
    assert_code("JSON_EXTRACT 3143", 3143, &e);
}

// ── 3149 ER_INVALID_JSON_PATH_ARRAY_CELL (wildcard in single-value) ───

#[test]
fn json_length_accepts_multi_match_path_since_mysql_8026() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        cell(
            &engine,
            r#"SELECT JSON_LENGTH('{"a":[1,2],"b":[3,4,5]}', '$.*')"#
        ),
        "2"
    );
    assert_eq!(
        cell(&engine, r#"SELECT JSON_LENGTH('[0,1,2,3]', '$[1 to 2]')"#),
        "2"
    );
}

#[test]
fn json_keys_rejects_wildcard_path_with_3149() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    // `$**.x` is the recursive-descent form the path parser accepts; the
    // wildcard check must fire at *use* time (JSON_KEYS is single-value)
    // even for a parseable wildcard path.
    let e = err(
        &engine,
        &mut session,
        r#"SELECT JSON_KEYS('{"a":{"x":1}}', '$**.x')"#,
    );
    assert_code("JSON_KEYS **.x", 3149, &e);
}

// ── 3154 ER_JSON_BAD_ONE_OR_ALL_ARG ───────────────────────────────────

#[test]
fn json_contains_path_bad_scope_reports_3154() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    // 'foo' is not 'one' / 'all' — MySQL raises 3154 rather than silently
    // treating any non-'one' value as 'all'.
    let e = err(
        &engine,
        &mut session,
        "SELECT JSON_CONTAINS_PATH('{\"a\":1}', 'foo', '$.a')",
    );
    assert_code("JSON_CONTAINS_PATH 3154", 3154, &e);
}

#[test]
fn json_search_bad_scope_reports_3154() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let e = err(
        &engine,
        &mut session,
        r#"SELECT JSON_SEARCH('{"a":"x"}', 'any', 'x')"#,
    );
    assert_code("JSON_SEARCH 3154", 3154, &e);
}

#[test]
fn json_contains_path_one_is_case_insensitive() {
    // 'ONE' and 'All' must both be accepted (parse is case-insensitive) —
    // regression guard so the strict validator doesn't over-reject.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs1 = exec(
        &engine,
        &mut session,
        "SELECT JSON_CONTAINS_PATH('{\"a\":1}', 'ONE', '$.a')",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = rs1 else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0].to_display_string(), "1");
    let rs2 = exec(
        &engine,
        &mut session,
        "SELECT JSON_CONTAINS_PATH('{\"a\":1}', 'All', '$.a')",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = rs2 else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0].to_display_string(), "1");
}

// ── 3157 ER_JSON_DOCUMENT_TOO_DEEP ────────────────────────────────────

#[test]
fn parse_101_layers_reports_3157() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    // Build a document exactly 101 levels deep: 101 opening `[` + 101 `]`.
    // MySQL's limit is 100 levels, so this document must fail with 3157.
    let depth = 101;
    let mut doc = String::with_capacity(depth * 2);
    for _ in 0..depth {
        doc.push('[');
    }
    for _ in 0..depth {
        doc.push(']');
    }
    let sql = format!("SELECT CAST('{doc}' AS JSON)");
    let e = err(&engine, &mut session, &sql);
    assert_code("CAST too-deep 3157", 3157, &e);
}

#[test]
fn parse_100_layers_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    // Exactly 100 levels: at the boundary, must succeed. Anti-regression
    // for a future "off-by-one" that would reject at the cap.
    let depth = 100;
    let mut doc = String::with_capacity(depth * 2);
    for _ in 0..depth {
        doc.push('[');
    }
    for _ in 0..depth {
        doc.push(']');
    }
    let sql = format!("SELECT JSON_DEPTH(CAST('{doc}' AS JSON))");
    let rs = exec(&engine, &mut session, &sql).unwrap();
    let QueryResponse::ResultSet(rs) = rs else {
        panic!("expected result set");
    };
    // JSON_DEPTH: nested arrays of depth n → n.
    assert_eq!(rs.rows[0][0].to_display_string(), depth.to_string());
}

// ── 3158 ER_JSON_DOCUMENT_NULL_KEY ────────────────────────────────────

#[test]
fn json_object_null_key_reports_3158() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    // Previously silently synthesised `{"": 1}` — MySQL raises 3158.
    let e = err(&engine, &mut session, "SELECT JSON_OBJECT(NULL, 1)");
    assert_code("JSON_OBJECT NULL key", 3158, &e);
}

#[test]
fn json_objectagg_null_key_reports_3158() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE oag (id INT PRIMARY KEY, k VARCHAR(10), v INT)",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "INSERT INTO oag VALUES (1, NULL, 42)",
    )
    .unwrap();
    // Aggregate a NULL key across a row: MySQL 8.0 raises 3158 (same as
    // JSON_OBJECT). Previously the non-window path silently dropped the
    // row, leaving the caller with an empty object.
    let e = err(
        &engine,
        &mut session,
        "SELECT JSON_OBJECTAGG(k, v) FROM oag",
    );
    assert_code("JSON_OBJECTAGG NULL key", 3158, &e);
}

// ── JSON_CONTAINS / JSON_OVERLAPS numeric-subtype equivalence ────────

#[test]
fn json_document_arguments_reject_sql_numeric_scalars_before_paths() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "SELECT JSON_STORAGE_FREE(34)",
        "SELECT JSON_CONTAINS(1, '{}')",
        "SELECT JSON_INSERT(1, '', 2)",
        "SELECT JSON_REPLACE(23533, 'key3', NULL)",
    ] {
        let got = err(&engine, &mut session, sql);
        assert_eq!(got.code(), 3146, "{sql}: {got}");
    }
}

#[test]
fn json_contains_treats_int_and_float_as_equal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    // Regression: serde_json's PartialEq split Number(1) from Number(1.0)
    // so this used to return 0. MySQL 8.0 returns 1 (same numeric value).
    let rs = exec(
        &engine,
        &mut session,
        "SELECT JSON_CONTAINS('[1.0]', '1'), JSON_CONTAINS('[1]', '1.0')",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = rs else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0].to_display_string(), "1");
    assert_eq!(rs.rows[0][1].to_display_string(), "1");
}

#[test]
fn json_overlaps_treats_int_and_float_as_equal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        "SELECT JSON_OVERLAPS('[1]', '[1.0]'), JSON_OVERLAPS('[1.0,2]', '[2.0]')",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = rs else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0].to_display_string(), "1");
    assert_eq!(rs.rows[0][1].to_display_string(), "1");
}

// ── 3903 ER_INVALID_JSON_VALUE_FOR_FUNC_INDEX (MVI) ──────────────────

#[test]
fn mvi_insert_non_numeric_element_reports_3903() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    // Table with a multi-valued index over the elements of `$.tags`
    // CAST AS UNSIGNED ARRAY. A non-numeric element must fail — the
    // previous silent `.unwrap_or(0)` would have inserted key 0.
    exec(
        &engine,
        &mut session,
        r#"CREATE TABLE mv (
            id INT PRIMARY KEY,
            doc JSON,
            KEY mv_tags ((CAST(doc->'$.tags' AS UNSIGNED ARRAY)))
        )"#,
    )
    .unwrap();
    let e = err(
        &engine,
        &mut session,
        r#"INSERT INTO mv VALUES (1, '{"tags":["abc"]}')"#,
    );
    assert_code("MVI non-numeric 3903", 3903, &e);
}

#[test]
fn mvi_insert_negative_into_unsigned_reports_3904() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        r#"CREATE TABLE mv (
            id INT PRIMARY KEY,
            doc JSON,
            KEY mv_tags ((CAST(doc->'$.tags' AS UNSIGNED ARRAY)))
        )"#,
    )
    .unwrap();
    let e = err(
        &engine,
        &mut session,
        r#"INSERT INTO mv VALUES (2, '{"tags":[-5]}')"#,
    );
    assert_code("MVI negative→unsigned 3904", 3904, &e);
}

#[test]
fn mvi_insert_nested_element_reports_3903() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        r#"CREATE TABLE mv (
            id INT PRIMARY KEY,
            doc JSON,
            KEY mv_tags ((CAST(doc->'$.tags' AS SIGNED ARRAY)))
        )"#,
    )
    .unwrap();
    // A nested array element has no scalar cast — previously coerce_i64
    // returned 0 → wrong index key.
    let e = err(
        &engine,
        &mut session,
        r#"INSERT INTO mv VALUES (3, '{"tags":[[1,2]]}')"#,
    );
    assert_code("MVI nested→signed 3903", 3903, &e);
}

#[test]
fn mvi_valid_elements_still_insert() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(
        &engine,
        &mut session,
        r#"CREATE TABLE mv (
            id INT PRIMARY KEY,
            doc JSON,
            KEY mv_tags ((CAST(doc->'$.tags' AS UNSIGNED ARRAY)))
        )"#,
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        r#"INSERT INTO mv VALUES (1, '{"tags":[1,2,3]}')"#,
    )
    .unwrap();
    // A whole-value float encoded through JSON's number_text() should
    // still cast losslessly (JSON writes `1.0` as `1.0`; the strict
    // parser accepts it as long as the fraction reduces to 0).
    exec(
        &engine,
        &mut session,
        r#"INSERT INTO mv VALUES (2, '{"tags":[10,20]}')"#,
    )
    .unwrap();
    let rs = exec(&engine, &mut session, "SELECT COUNT(*) FROM mv").unwrap();
    let QueryResponse::ResultSet(rs) = rs else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0].to_display_string(), "2");
}
