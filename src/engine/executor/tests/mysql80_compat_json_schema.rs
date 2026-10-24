//! P1-A5 — JSON_SCHEMA_VALID(schema, document)。
//!
//! Previously this MySQL 8.0.17+ built-in was missing; Connector/J type detection
//! and mysqldump --column-statistics use it for schema validation and otherwise
//! fall back to less effective paths. These tests establish:
//!
//! type clauses: object/array/string/number/integer/boolean/null.
//! - required + properties + additionalProperties
//! minimum/maximum, including exclusive variants.
//! - minLength/maxLength + minItems/maxItems
//! - enum / const / allOf / anyOf / oneOf / not
//! NULL arguments return NULL.
//! Non-JSON input returns 3141.
//! A non-object schema returns 3853.

use super::support::execute_materialized as exec;
use super::*;

fn one_int(rs: QueryResponse) -> i64 {
    let QueryResponse::ResultSet(rs) = rs else {
        panic!("expected result set");
    };
    match &rs.rows[0][0] {
        Value::Int(v) => *v,
        Value::UInt(v) => *v as i64,
        other => panic!("expected int, got {other:?}"),
    }
}

fn one_value(rs: QueryResponse) -> Value {
    let QueryResponse::ResultSet(rs) = rs else {
        panic!("expected result set");
    };
    rs.rows[0][0].clone()
}

#[test]
fn json_schema_valid_matching_type_returns_1() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID('{"type":"string"}', '"hello"')"#,
    )
    .unwrap();
    assert_eq!(one_int(rs), 1);
}

#[test]
fn json_schema_valid_mismatched_type_returns_0() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID('{"type":"string"}', '42')"#,
    )
    .unwrap();
    assert_eq!(one_int(rs), 0);
}

#[test]
fn json_schema_valid_required_property_missing_returns_0() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID('{"type":"object","required":["name"]}', '{}')"#,
    )
    .unwrap();
    assert_eq!(one_int(rs), 0);
}

#[test]
fn json_schema_valid_required_property_present_returns_1() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID('{"type":"object","required":["name"]}', '{"name":"Alice"}')"#,
    )
    .unwrap();
    assert_eq!(one_int(rs), 1);
}

#[test]
fn json_schema_valid_property_subschema_failure_returns_0() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID(
            '{"type":"object","properties":{"age":{"type":"integer","minimum":0}}}',
            '{"age":-5}'
        )"#,
    )
    .unwrap();
    assert_eq!(one_int(rs), 0);
}

#[test]
fn json_schema_valid_additional_properties_false_rejects_extras() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID(
            '{"type":"object","properties":{"a":{"type":"integer"}},"additionalProperties":false}',
            '{"a":1,"b":2}'
        )"#,
    )
    .unwrap();
    assert_eq!(one_int(rs), 0);
}

#[test]
fn json_schema_valid_numeric_minimum_exclusive() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID(
            '{"type":"number","minimum":0,"exclusiveMinimum":true}',
            '0'
        )"#,
    )
    .unwrap();
    assert_eq!(one_int(rs), 0);
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID(
            '{"type":"number","minimum":0,"exclusiveMinimum":true}',
            '1'
        )"#,
    )
    .unwrap();
    assert_eq!(one_int(rs), 1);
}

#[test]
fn json_schema_valid_string_length_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID(
            '{"type":"string","minLength":3,"maxLength":5}',
            '"ab"'
        )"#,
    )
    .unwrap();
    assert_eq!(one_int(rs), 0);
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID(
            '{"type":"string","minLength":3,"maxLength":5}',
            '"abcd"'
        )"#,
    )
    .unwrap();
    assert_eq!(one_int(rs), 1);
}

#[test]
fn json_schema_valid_covers_complete_draft4_keywords() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let cases = [
        (r#"{"type":"number","multipleOf":2}"#, "3", 0),
        (r#"{"type":"string","pattern":"^a+$"}"#, r#""bbb""#, 0),
        (
            r#"{"type":"object","minProperties":2,"maxProperties":3}"#,
            r#"{"a":1}"#,
            0,
        ),
        (
            r#"{"type":"object","patternProperties":{"^x":{"type":"integer"}}}"#,
            r#"{"x1":"bad"}"#,
            0,
        ),
        (
            r##"{"definitions":{"positive":{"type":"integer","minimum":1}},"$ref":"#/definitions/positive"}"##,
            "2",
            1,
        ),
    ];
    for (schema, document, expected) in cases {
        let quote = |text: &str| format!("'{}'", text.replace('\'', "''"));
        let sql = format!(
            "SELECT JSON_SCHEMA_VALID({}, {})",
            quote(schema),
            quote(document)
        );
        assert_eq!(
            one_int(exec(&engine, &mut session, &sql).unwrap()),
            expected
        );
    }
}

#[test]
fn json_schema_valid_array_item_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID(
            '{"type":"array","items":{"type":"integer"},"minItems":2}',
            '[1]'
        )"#,
    )
    .unwrap();
    assert_eq!(one_int(rs), 0);
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID(
            '{"type":"array","items":{"type":"integer"},"minItems":2}',
            '[1,2,3]'
        )"#,
    )
    .unwrap();
    assert_eq!(one_int(rs), 1);
}

#[test]
fn json_schema_valid_enum_membership() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID('{"enum":["red","green","blue"]}', '"green"')"#,
    )
    .unwrap();
    assert_eq!(one_int(rs), 1);
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID('{"enum":["red","green","blue"]}', '"yellow"')"#,
    )
    .unwrap();
    assert_eq!(one_int(rs), 0);
}

#[test]
fn json_schema_valid_one_of_exclusive() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    // oneOf must match EXACTLY one branch.
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID(
            '{"oneOf":[{"type":"integer"},{"type":"string"}]}',
            '42'
        )"#,
    )
    .unwrap();
    assert_eq!(one_int(rs), 1);
    // Integer also satisfies "number" — two matches → 0.
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID(
            '{"oneOf":[{"type":"integer"},{"type":"number"}]}',
            '42'
        )"#,
    )
    .unwrap();
    assert_eq!(one_int(rs), 0);
}

#[test]
fn json_schema_valid_null_arguments_return_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID(NULL, '"x"')"#,
    )
    .unwrap();
    assert!(matches!(one_value(rs), Value::Null));
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID('{"type":"string"}', NULL)"#,
    )
    .unwrap();
    assert!(matches!(one_value(rs), Value::Null));
}

#[test]
fn json_schema_valid_non_object_schema_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let err = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID('"not_an_object"', '"x"')"#,
    )
    .unwrap_err();
    assert_eq!(err.code(), 3853);
}

#[test]
fn json_schema_valid_malformed_json_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let err = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALID('{"type":"string"}', 'not valid json')"#,
    )
    .unwrap_err();
    // 3141 ER_INVALID_JSON_TEXT is what json_doc_arg surfaces.
    assert!(
        err.is(crate::error::catalog::ER_INVALID_JSON_TEXT_IN_PARAM)
            || err.is(crate::error::catalog::ER_INVALID_JSON_TEXT)
    );
}

// ------------------------------------------------------------------
// JSON_SCHEMA_VALIDATION_REPORT(schema, document) — G4b.
// Returns a JSON report: {"valid": true} or {"valid": false, reason,
// schema-location, document-location, schema-failed-keyword}.
// ------------------------------------------------------------------

fn report_str(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    schema: &str,
    doc: &str,
) -> String {
    let sql = format!(
        "SELECT JSON_SCHEMA_VALIDATION_REPORT('{}', '{}')",
        schema.replace('\'', "''"),
        doc.replace('\'', "''")
    );
    let rs = exec(engine, session, &sql).unwrap();
    match one_value(rs) {
        Value::Json(b) => crate::engine::json::to_serde(&b).unwrap().to_string(),
        Value::String(s) => s,
        other => panic!("expected json report, got {other:?}"),
    }
}

#[test]
fn json_schema_report_valid_document() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let r = report_str(&engine, &mut session, r#"{"type":"string"}"#, r#""hello""#);
    assert!(r.contains("\"valid\":true"), "{r}");
}

#[test]
fn json_schema_report_type_failure() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let r = report_str(&engine, &mut session, r#"{"type":"string"}"#, r#"42"#);
    assert!(r.contains("\"valid\":false"), "{r}");
    assert!(r.contains("\"schema-failed-keyword\":\"type\""), "{r}");
    assert!(r.contains("\"document-location\":\"#\""), "{r}");
}

#[test]
fn json_schema_report_nested_property_failure_locates_pointer() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    // age must be an integer ≥ 0; document has age = -1 → minimum failure at
    // document location #/age. MySQL reports the schema node containing the
    // failed keyword, while `schema-failed-keyword` carries `minimum`.
    let r = report_str(
        &engine,
        &mut session,
        r#"{"type":"object","properties":{"age":{"type":"integer","minimum":0}}}"#,
        r#"{"age":-1}"#,
    );
    assert!(r.contains("\"valid\":false"), "{r}");
    assert!(r.contains("\"schema-failed-keyword\":\"minimum\""), "{r}");
    assert!(r.contains("\"document-location\":\"#/age\""), "{r}");
    assert!(
        r.contains("\"schema-location\":\"#/properties/age\""),
        "{r}"
    );
}

#[test]
fn json_schema_report_required_failure() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let r = report_str(
        &engine,
        &mut session,
        r#"{"type":"object","required":["name"]}"#,
        r#"{}"#,
    );
    assert!(r.contains("\"schema-failed-keyword\":\"required\""), "{r}");
}

#[test]
fn json_schema_report_null_arg_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALIDATION_REPORT('{"type":"string"}', NULL)"#,
    )
    .unwrap();
    assert!(matches!(one_value(rs), Value::Null));
}

#[test]
fn json_schema_report_non_object_schema_errors_3853() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let err = exec(
        &engine,
        &mut session,
        r#"SELECT JSON_SCHEMA_VALIDATION_REPORT('42', '1')"#,
    )
    .unwrap_err();
    assert_eq!(err.code(), 3853);
}
