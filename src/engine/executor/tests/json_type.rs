//! End-to-end tests for the high-performance binary JSON data type.
//!
//! Exercises the whole pillar through the SQL engine: storage round-trip of
//! a real JSON column (binary `jsonb` on disk), MySQL-canonical
//! normalisation, type-precedence comparison / ordering, the function suite
//! evaluated over a stored column (binary path), large-document off-page
//! externalisation, DISTINCT canonicalisation, and a functional index over
//! a JSON path expression.

use super::support::query_rows as rows;
use super::*;
use crate::types::ColumnType;

/// The single scalar cell of a one-row, one-column query, rendered as text.
fn cell(engine: &StorageQueryEngine, sql: &str) -> String {
    let r = rows(engine, sql);
    assert_eq!(r.len(), 1, "expected exactly one row for `{sql}`");
    assert_eq!(r[0].len(), 1, "expected exactly one column for `{sql}`");
    r[0][0].to_display_string()
}

fn column_types(engine: &StorageQueryEngine, sql: &str) -> Vec<ColumnType> {
    match engine.execute(sql).unwrap() {
        QueryResponse::ResultSet(rs) => rs.columns.iter().map(|c| c.column_type).collect(),
        other => panic!("expected a result set for `{sql}`, got {other:?}"),
    }
}

#[test]
fn json_column_round_trips_in_canonical_form() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE jt (id INT PRIMARY KEY, doc JSON)")
        .unwrap();
    // Keys deliberately out of order; nested array order must be preserved.
    engine
        .execute(r#"INSERT INTO jt VALUES (1, '{"b":2,"a":1,"nested":{"y":[3,2,1]}}')"#)
        .unwrap();

    // Read back: object keys sorted by (length, bytes); array order kept;
    // MySQL spacing after `:` and `,`.
    assert_eq!(
        cell(&engine, "SELECT doc FROM jt WHERE id = 1"),
        r#"{"a": 1, "b": 2, "nested": {"y": [3, 2, 1]}}"#
    );
}

#[test]
fn json_string_comparison_keeps_sql_strings_as_string_scalars() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE jt (id INT PRIMARY KEY, doc JSON)")
        .unwrap();
    engine
        .execute(r#"INSERT INTO jt VALUES (1, '"1"'), (2, '1')"#)
        .unwrap();

    let result = rows(
        &engine,
        "SELECT id FROM jt WHERE doc IN ('1', '2') ORDER BY id",
    );
    assert_eq!(result, vec![vec![Value::Int(1)]]);
    assert_eq!(cell(&engine, "SELECT doc = 1 FROM jt WHERE id = 2"), "1");
    assert_eq!(cell(&engine, "SELECT doc = '1' FROM jt WHERE id = 2"), "0");
}

#[test]
fn json_column_and_function_report_json_result_type() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE jt (id INT PRIMARY KEY, doc JSON)")
        .unwrap();
    engine
        .execute(r#"INSERT INTO jt VALUES (1, '{"a":1}')"#)
        .unwrap();

    assert_eq!(
        column_types(&engine, "SELECT doc FROM jt"),
        vec![ColumnType::Json]
    );
    // JSON-document-returning functions and `->` advertise MYSQL_TYPE_JSON;
    // `->>` and JSON_TYPE return text.
    assert_eq!(
        column_types(
            &engine,
            "SELECT JSON_EXTRACT(doc,'$.a'), doc->'$.a', doc->>'$.a', JSON_TYPE(doc), \
                    JSON_PRETTY(doc), \
                    JSON_SCHEMA_VALIDATION_REPORT('{\"type\":\"object\"}', doc) FROM jt"
        ),
        vec![
            ColumnType::Json,
            ColumnType::Json,
            ColumnType::VarString,
            ColumnType::VarString,
            ColumnType::VarString,
            ColumnType::Json,
        ]
    );
}

#[test]
fn cast_as_json_normalises_keys_and_dedups() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Out-of-order keys + a duplicate key (last wins) + whitespace.
    assert_eq!(
        cell(&engine, r#"SELECT CAST('{ "z":1, "a":2, "a":9 }' AS JSON)"#),
        r#"{"a": 9, "z": 1}"#
    );
    // A double keeps its decimal point; an integer does not.
    assert_eq!(
        cell(&engine, "SELECT CAST('[1, 1.0, 2.50]' AS JSON)"),
        "[1, 1.0, 2.5]"
    );
}

#[test]
fn inserting_invalid_json_into_json_column_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE jt (id INT PRIMARY KEY, doc JSON)")
        .unwrap();
    let err = engine
        .execute("INSERT INTO jt VALUES (1, 'not json')")
        .unwrap_err();
    assert_eq!(
        err.code(),
        3140,
        "invalid JSON insert must raise (got {err:?})"
    );
}

#[test]
fn json_values_order_by_mysql_type_precedence() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE jo (id INT, v JSON)").unwrap();
    // MySQL ascending order: NULL < number < string < object < array < boolean.
    engine
        .execute(
            r#"INSERT INTO jo VALUES
                (3, '"abc"'), (6, 'true'), (1, 'null'),
                (5, '[1,2]'), (2, '10'), (4, '{"k":1}')"#,
        )
        .unwrap();
    let ids: Vec<String> = rows(&engine, "SELECT id FROM jo ORDER BY v")
        .into_iter()
        .map(|r| r[0].to_display_string())
        .collect();
    assert_eq!(ids, vec!["1", "2", "3", "4", "5", "6"]);
}

/// `ORDER BY` over a JSON column whose numbers include a double and integers
/// that round to the *same* double must not crash. The comparator used to fall
/// back to a lossy f64 compare, which is not transitive — `sort_by` detected
/// the broken total order and panicked ("comparison function does not correctly
/// implement a total order"). This is the reduced form of MySQL's WL#8539
/// json_functions ordering test that triggered the crash. Ordering here matches
/// MySQL: numbers rank by exact decimal value, so the double
/// `9.223372036854776e18` sits between the largest signed BIGINT and
/// `…776001`, and equals the integer `…776000`.
#[test]
fn json_order_by_mixed_numbers_is_a_total_order_and_never_panics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE jn (id INT, v JSON)").unwrap();
    engine
        .execute(
            r#"INSERT INTO jn VALUES
                (1, '9223372036854775806'),
                (2, '9223372036854775807'),
                (3, '9.223372036854776e18'),
                (4, '9223372036854776000'),
                (5, '9223372036854776001'),
                (6, '18446744073709551615'),
                (7, '1e65'),
                (8, '1e100'),
                (9, '-1e81'),
                (10, '3.14'),
                (11, '0.0'),
                (12, '-0.0')"#,
        )
        .unwrap();
    // Ascending order by exact numeric value (id is the payload we read back).
    let ids: Vec<String> = rows(&engine, "SELECT id FROM jn ORDER BY v, id")
        .into_iter()
        .map(|r| r[0].to_display_string())
        .collect();
    assert_eq!(
        ids,
        vec![
            "9",  // -1e81
            "11", // 0.0  (== -0.0; equal values, ascending id tiebreak)
            "12", // -0.0
            "10", // 3.14
            "1",  // 9223372036854775806
            "2",  // 9223372036854775807
            "3",  // 9.223372036854776e18  (== id 4)
            "4",  // 9223372036854776000
            "5",  // 9223372036854776001
            "6",  // 18446744073709551615
            "7",  // 1e65
            "8",  // 1e100
        ]
    );
    // DESC must also complete without panicking on the same data.
    assert_eq!(
        rows(
            &engine,
            "SELECT COUNT(*) FROM (SELECT v FROM jn ORDER BY v DESC) x"
        )[0][0]
            .to_display_string(),
        "12"
    );
}

#[test]
fn json_function_suite_over_stored_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE jt (id INT PRIMARY KEY, doc JSON)")
        .unwrap();
    engine
        .execute(r#"INSERT INTO jt VALUES (1, '{"id":7,"tags":["x","y"],"meta":{"active":true}}')"#)
        .unwrap();

    let r = rows(
        &engine,
        "SELECT \
            JSON_EXTRACT(doc,'$.id'), \
            doc->'$.id', \
            doc->>'$.tags[0]', \
            JSON_TYPE(doc->'$.meta'), \
            JSON_LENGTH(doc->'$.tags'), \
            JSON_KEYS(doc), \
            JSON_CONTAINS(doc->'$.tags', '\"x\"'), \
            JSON_DEPTH(doc) \
         FROM jt WHERE id = 1",
    );
    let r: Vec<String> = r[0].iter().map(|v| v.to_display_string()).collect();
    assert_eq!(r[0], "7"); // JSON_EXTRACT scalar
    assert_eq!(r[1], "7"); // -> returns JSON
    assert_eq!(r[2], "x"); // ->> unquotes the string
    assert_eq!(r[3], "OBJECT"); // JSON_TYPE of a nested object
    assert_eq!(r[4], "2"); // JSON_LENGTH of the array
    assert_eq!(r[5], r#"["id", "meta", "tags"]"#); // keys in (len, bytes) order
    assert_eq!(r[6], "1"); // JSON_CONTAINS
    assert_eq!(r[7], "3"); // JSON_DEPTH
}

#[test]
fn json_extract_wildcard_and_recursive_paths() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Recursive descent finds every `.x` at any depth (autowrapped).
    assert_eq!(
        cell(
            &engine,
            r#"SELECT JSON_EXTRACT('{"a":{"x":1},"b":{"x":2}}', '$**.x')"#
        ),
        "[1, 2]"
    );
    // `[*]` returns all array elements.
    assert_eq!(
        cell(&engine, "SELECT JSON_EXTRACT('[10,20,30]', '$[*]')"),
        "[10, 20, 30]"
    );
    // `.*` returns all object members (autowrapped).
    assert_eq!(
        cell(&engine, r#"SELECT JSON_EXTRACT('{"a":1,"b":2}', '$.*')"#),
        "[1, 2]"
    );
}

#[test]
fn large_json_document_externalises_and_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE jt (id INT PRIMARY KEY, doc JSON)")
        .unwrap();
    // ~5 KB of payload — well over MYDB_LOB_INLINE_MAX (4096), so the row
    // stores a 24-byte LobRef and the bytes live off-page.
    engine
        .execute("INSERT INTO jt VALUES (1, CONCAT('{\"s\":\"', REPEAT('x', 5000), '\"}'))")
        .unwrap();

    assert_eq!(
        cell(&engine, "SELECT JSON_LENGTH(doc) FROM jt WHERE id = 1"),
        "1"
    );
    assert_eq!(
        cell(
            &engine,
            "SELECT CHAR_LENGTH(doc->>'$.s') FROM jt WHERE id = 1"
        ),
        "5000"
    );
}

#[test]
fn distinct_json_canonicalises_equal_documents() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE jd (v JSON)").unwrap();
    // Three documents that are all canonically equal (key order differs).
    engine
        .execute(
            r#"INSERT INTO jd VALUES ('{"a":1,"b":2}'), ('{"b":2,"a":1}'), ('{ "a":1, "b":2 }')"#,
        )
        .unwrap();
    assert_eq!(
        cell(
            &engine,
            "SELECT COUNT(*) FROM (SELECT DISTINCT v FROM jd) t"
        ),
        "1"
    );
}

#[test]
fn update_json_column_with_json_set() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE jt (id INT PRIMARY KEY, doc JSON)")
        .unwrap();
    engine
        .execute(r#"INSERT INTO jt VALUES (1, '{"id":7,"v":1}')"#)
        .unwrap();
    engine
        .execute("UPDATE jt SET doc = JSON_SET(doc, '$.v', 42, '$.new', 'hi') WHERE id = 1")
        .unwrap();
    assert_eq!(
        cell(&engine, "SELECT doc FROM jt WHERE id = 1"),
        r#"{"v": 42, "id": 7, "new": "hi"}"#
    );
}

#[test]
fn functional_index_over_json_path_returns_correct_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE ji (id INT PRIMARY KEY, doc JSON, \
             INDEX idx_uid ((CAST(doc->>'$.uid' AS UNSIGNED))))",
        )
        .unwrap();
    engine
        .execute(
            r#"INSERT INTO ji VALUES (1, '{"uid":100}'), (2, '{"uid":200}'), (3, '{"uid":300}')"#,
        )
        .unwrap();

    // Query on the indexed expression returns the right row.
    let r = rows(
        &engine,
        "SELECT id FROM ji WHERE CAST(doc->>'$.uid' AS UNSIGNED) = 200",
    );
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0].to_display_string(), "2");

    // The functional index is maintained across an UPDATE.
    engine
        .execute(r#"UPDATE ji SET doc = '{"uid":999}' WHERE id = 1"#)
        .unwrap();
    let r = rows(
        &engine,
        "SELECT id FROM ji WHERE CAST(doc->>'$.uid' AS UNSIGNED) = 999",
    );
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0].to_display_string(), "1");
}

#[test]
fn json_multi_valued_index_member_of_is_maintained_and_planned() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE jm (id INT PRIMARY KEY, doc JSON, \
             INDEX idx_tags ((CAST(doc->'$.tags' AS UNSIGNED ARRAY))))",
        )
        .unwrap();
    engine
        .execute(
            r#"INSERT INTO jm VALUES
               (1, '{"tags":[1,2,2]}'),
               (2, '{"tags":[2,3]}'),
               (3, '{"tags":[4]}')"#,
        )
        .unwrap();

    let ids: Vec<String> = rows(
        &engine,
        "SELECT id FROM jm WHERE 2 MEMBER OF(doc->'$.tags') ORDER BY id",
    )
    .into_iter()
    .map(|row| row[0].to_display_string())
    .collect();
    assert_eq!(ids, vec!["1", "2"]);

    engine
        .execute(r#"UPDATE jm SET doc = '{"tags":[5,6]}' WHERE id = 1"#)
        .unwrap();
    let ids: Vec<String> = rows(
        &engine,
        "SELECT id FROM jm WHERE 2 MEMBER OF(doc->'$.tags') ORDER BY id",
    )
    .into_iter()
    .map(|row| row[0].to_display_string())
    .collect();
    assert_eq!(ids, vec!["2"]);

    engine.execute("DELETE FROM jm WHERE id = 2").unwrap();
    let ids = rows(
        &engine,
        "SELECT id FROM jm WHERE 2 MEMBER OF(doc->'$.tags') ORDER BY id",
    );
    assert!(ids.is_empty());

    for i in 10..230 {
        engine
            .execute(&format!(
                r#"INSERT INTO jm VALUES ({i}, '{{"tags":[{},{},{}]}}')"#,
                i,
                i + 1000,
                i + 2000
            ))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE jm").unwrap();
    let explain = rows(
        &engine,
        "EXPLAIN SELECT id FROM jm WHERE 42 MEMBER OF(doc->'$.tags')",
    );
    let chosen = explain
        .iter()
        .any(|row| matches!(row.get(6), Some(Value::String(s)) if s == "idx_tags"));
    assert!(chosen, "EXPLAIN should choose idx_tags, got {explain:?}");

    let ids: Vec<String> = rows(
        &engine,
        "SELECT id FROM jm WHERE 42 MEMBER OF(doc->'$.tags') ORDER BY id",
    )
    .into_iter()
    .map(|row| row[0].to_display_string())
    .collect();
    assert_eq!(ids, vec!["42"]);
}

#[test]
fn json_multi_valued_repairs_preserve_keys_retained_across_partial_updates() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE mvi_versions(id INT PRIMARY KEY,doc JSON,INDEX tags((CAST(doc->'$.tags' AS UNSIGNED ARRAY))))").unwrap();
    engine
        .execute(r#"INSERT INTO mvi_versions VALUES(1,'{"tags":[10,20,30]}')"#)
        .unwrap();
    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut reader, "SELECT doc FROM mvi_versions")
        .unwrap();
    engine
        .execute(r#"UPDATE mvi_versions SET doc='{"tags":[10,21,31]}' WHERE id=1"#)
        .unwrap();
    let mut writer = SessionContext::default();
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut writer,
            r#"UPDATE mvi_versions SET doc='{"tags":[11,21,31]}' WHERE id=1"#,
        )
        .unwrap();
    for value in [10, 20, 30, 11, 21, 31] {
        let sql = format!(
            "SELECT id FROM mvi_versions FORCE INDEX(tags) WHERE {value} MEMBER OF(doc->'$.tags')"
        );
        let QueryResponse::ResultSet(result) =
            engine.execute_with_session(&mut reader, &sql).unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(
            result.rows,
            if [10, 20, 30].contains(&value) {
                vec![vec![Value::Int(1)]]
            } else {
                vec![]
            },
            "{sql}"
        );
    }
    engine
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
}

#[test]
fn json_multi_valued_index_common_json_predicates_are_planned() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE jm2 (id INT PRIMARY KEY, doc JSON, \
             INDEX idx_tags ((CAST(doc->'$.tags' AS UNSIGNED ARRAY))))",
        )
        .unwrap();
    for i in 1..260 {
        engine
            .execute(&format!(
                r#"INSERT INTO jm2 VALUES ({i}, '{{"tags":[{i},{a},{b}]}}')"#,
                a = i + 1000,
                b = i + 2000
            ))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE jm2").unwrap();

    let ids: Vec<String> = rows(
        &engine,
        "SELECT id FROM jm2 WHERE JSON_CONTAINS(doc->'$.tags', '42') ORDER BY id",
    )
    .into_iter()
    .map(|row| row[0].to_display_string())
    .collect();
    assert_eq!(ids, vec!["42"]);
    let explain = rows(
        &engine,
        "EXPLAIN SELECT id FROM jm2 WHERE JSON_CONTAINS(doc->'$.tags', '42')",
    );
    assert!(
        explain
            .iter()
            .any(|row| matches!(row.get(6), Some(Value::String(s)) if s == "idx_tags")),
        "EXPLAIN should choose idx_tags for JSON_CONTAINS, got {explain:?}"
    );

    let ids: Vec<String> = rows(
        &engine,
        "SELECT id FROM jm2 WHERE JSON_OVERLAPS(doc->'$.tags', '[42, 77]') ORDER BY id",
    )
    .into_iter()
    .map(|row| row[0].to_display_string())
    .collect();
    assert_eq!(ids, vec!["42", "77"]);
    let explain = rows(
        &engine,
        "EXPLAIN SELECT id FROM jm2 WHERE JSON_OVERLAPS(doc->'$.tags', '[42, 77]')",
    );
    assert!(
        explain
            .iter()
            .any(|row| matches!(row.get(6), Some(Value::String(s)) if s == "idx_tags")),
        "EXPLAIN should choose idx_tags for JSON_OVERLAPS, got {explain:?}"
    );
}

#[test]
fn json_multi_valued_index_supports_composite_and_rejects_invalid_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let err = engine
        .execute(
            "CREATE TABLE jmu (id INT PRIMARY KEY, doc JSON, \
             UNIQUE INDEX ux_tags ((CAST(doc->'$.tags' AS UNSIGNED ARRAY))))",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1235);

    engine
        .execute(
            "CREATE TABLE jmc (id INT PRIMARY KEY, doc JSON, \
             INDEX idx_tags ((CAST(doc->'$.tags' AS UNSIGNED ARRAY)), id))",
        )
        .unwrap();
    engine
        .execute(
            r#"INSERT INTO jmc VALUES
               (1, '{"tags":[1,2]}'),
               (2, '{"tags":[2,3]}'),
               (3, '{"tags":[4]}')"#,
        )
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM jmc WHERE 2 MEMBER OF(doc->'$.tags') ORDER BY id"
        )
        .into_iter()
        .map(|row| row[0].to_display_string())
        .collect::<Vec<_>>(),
        vec!["1", "2"]
    );

    engine
        .execute("CREATE TABLE jma (id INT PRIMARY KEY, doc JSON)")
        .unwrap();
    let err = engine
        .execute("CREATE FULLTEXT INDEX ft_tags ON jma ((CAST(doc->'$.tags' AS CHAR ARRAY)))")
        .unwrap_err();
    assert_eq!(err.code(), 1235);
}

// N3 (docs/ddl-audit-2026-07-07-followup.md): batched ADD INDEX with two or more indexes
// including JSON MVI must expand elements rather than build whole-array B-tree keys,
// or MEMBER OF element lookups miss rows. EXPLAIN confirms the mvi index path.
#[test]
fn n3_batched_add_index_builds_correct_multi_valued_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE jm (id INT PRIMARY KEY, doc JSON, tag INT)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO jm VALUES \
             (1, '{\"tags\":[10,20]}', 5), \
             (2, '{\"tags\":[30]}', 6), \
             (3, '{\"tags\":[20,40]}', 7)",
        )
        .unwrap();
    // Two AddIndex actions, MVI and ordinary, use fused bulk offline construction.
    engine
        .execute(
            "ALTER TABLE jm ADD INDEX mvi ((CAST(doc->'$.tags' AS UNSIGNED ARRAY))), \
             ADD INDEX kx (tag)",
        )
        .unwrap();
    // MVI lookup for element 20 must find IDs 1 and 3; the former whole-array
    // B-tree construction missed both rows.
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM jm WHERE 20 MEMBER OF (doc->'$.tags') ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let ids: Vec<String> = rs.rows.iter().map(|r| r[0].to_display_string()).collect();
    assert_eq!(
        ids,
        vec!["1".to_string(), "3".to_string()],
        "MVI element lookup must be exact"
    );
    // The ordinary secondary index still works.
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM jm WHERE tag = 6").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0].to_display_string(), "2");
}

// --- Phase 0: two P0 regressions -----------------------------------------

/// P0-1: `JSON_SET` with a missing intermediate object path must be a no-op
/// (MySQL returns the document unchanged), not panic the server.
#[test]
fn json_set_missing_intermediate_path_is_noop_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // `$.x` does not exist, so `$.x.y` cannot be set — MySQL leaves the doc.
    assert_eq!(
        cell(&engine, r#"SELECT JSON_SET('{"a":1}', '$.x.y', 5)"#),
        r#"{"a": 1}"#
    );
    assert_eq!(
        cell(&engine, r#"SELECT JSON_INSERT('{"a":1}', '$.p.q.r', 9)"#),
        r#"{"a": 1}"#
    );
    assert_eq!(
        cell(&engine, r#"SELECT JSON_REPLACE('{"a":1}', '$.x.y', 5)"#),
        r#"{"a": 1}"#
    );
    // A present intermediate path still works.
    assert_eq!(
        cell(&engine, r#"SELECT JSON_SET('{"a":{"b":1}}', '$.a.c', 2)"#),
        r#"{"a": {"b": 1, "c": 2}}"#
    );
}

#[test]
fn json_mutators_autowrap_non_array_for_index_paths() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    assert_eq!(cell(&engine, "SELECT JSON_SET('1', '$[0]', 99)"), "99");
    assert_eq!(cell(&engine, "SELECT JSON_REPLACE('1', '$[0]', 99)"), "99");
    assert_eq!(
        cell(&engine, r#"SELECT JSON_SET('{"a":1}', '$[0]', 99)"#),
        "99"
    );
    assert_eq!(cell(&engine, "SELECT JSON_SET('1', '$[1]', 99)"), "[1, 99]");
    assert_eq!(cell(&engine, "SELECT JSON_INSERT('1', '$[0]', 99)"), "1");
    assert_eq!(
        cell(&engine, "SELECT JSON_INSERT('1', '$[1]', 99)"),
        "[1, 99]"
    );
    assert_eq!(cell(&engine, "SELECT JSON_REPLACE('1', '$[1]', 99)"), "1");
    assert_eq!(
        cell(&engine, "SELECT JSON_ARRAY_APPEND('1', '$[0]', 99)"),
        "[1, 99]"
    );
    assert_eq!(
        cell(&engine, "SELECT JSON_ARRAY_APPEND('1', '$[1]', 99)"),
        "1"
    );
    assert_eq!(
        cell(&engine, "SELECT JSON_ARRAY_APPEND('[]', '$', NULL)"),
        "[null]"
    );
    assert_eq!(
        cell(&engine, "SELECT JSON_ARRAY_INSERT('[1]', '$[0]', NULL)"),
        "[null, 1]"
    );

    // Existing arrays retain their ordinary replacement semantics.
    assert_eq!(
        cell(&engine, "SELECT JSON_SET('[1,2]', '$[1]', 99)"),
        "[1, 99]"
    );
}

/// P0-2: a SQL string flowing into a JSON producer stays a JSON string; it is
/// never re-parsed into a number/structure. Only `CAST(x AS JSON)` parses text.
#[test]
fn json_producers_keep_sql_strings_as_json_strings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(cell(&engine, "SELECT JSON_ARRAY('123')"), r#"["123"]"#);
    // The string is kept verbatim (not re-parsed / re-canonicalised).
    assert_eq!(
        cell(&engine, r#"SELECT JSON_ARRAY('{"a":1}')"#),
        r#"["{\"a\":1}"]"#
    );
    assert_eq!(
        cell(&engine, "SELECT JSON_OBJECT('k', '1')"),
        r#"{"k": "1"}"#
    );
    assert_eq!(
        cell(&engine, r#"SELECT JSON_SET('{}', '$.v', '42')"#),
        r#"{"v": "42"}"#
    );
    // A string '1' is not the number 1 → not a member.
    assert_eq!(cell(&engine, "SELECT '1' MEMBER OF(JSON_ARRAY(1))"), "0");
    assert_eq!(cell(&engine, "SELECT '1' MEMBER OF(JSON_ARRAY('1'))"), "1");
    // CAST still parses text into a JSON structure.
    assert_eq!(cell(&engine, "SELECT CAST('123' AS JSON)"), "123");
    // A numeric literal is still a JSON number.
    assert_eq!(cell(&engine, "SELECT JSON_ARRAY(123)"), "[123]");
}

/// P1-5: JSON_KEYS returns keys in MySQL `(length, bytes)` order, not the
/// alphabetical order the old serde BTreeMap path produced.
#[test]
fn json_keys_uses_mysql_length_then_bytes_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Alphabetical would give ["aa","b","ccc"]; MySQL orders by length first.
    assert_eq!(
        cell(&engine, r#"SELECT JSON_KEYS('{"b":1,"aa":2,"ccc":3}')"#),
        r#"["b", "aa", "ccc"]"#
    );
    // With a path into a nested object.
    assert_eq!(
        cell(
            &engine,
            r#"SELECT JSON_KEYS('{"o":{"zz":1,"a":2}}', '$.o')"#
        ),
        r#"["a", "zz"]"#
    );
}

/// P2-10 + perf: read functions run on the binary engine and accept the full
/// path grammar (wildcards / `[last-N]`), and are O(1)/O(n) without a DOM
/// decode. Also covers JSON_TYPE / JSON_DEPTH on the binary form.
#[test]
fn json_read_functions_on_binary_engine_support_wildcards() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(cell(&engine, r#"SELECT JSON_TYPE('[1,2,3]')"#), "ARRAY");
    assert_eq!(cell(&engine, r#"SELECT JSON_TYPE('{"a":1}')"#), "OBJECT");
    assert_eq!(cell(&engine, r#"SELECT JSON_TYPE('true')"#), "BOOLEAN");
    assert_eq!(cell(&engine, r#"SELECT JSON_TYPE('9')"#), "INTEGER");
    assert_eq!(
        cell(&engine, r#"SELECT JSON_DEPTH('{"a":[1,{"b":2}]}')"#),
        "4"
    );
    assert_eq!(cell(&engine, r#"SELECT JSON_LENGTH('[1,2,3,4]')"#), "4");
    // `[last]` selects the trailing element (nested array of length 2).
    assert_eq!(
        cell(&engine, r#"SELECT JSON_LENGTH('[1,[7,8]]', '$[last]')"#),
        "2"
    );
}

/// MEMBER OF binary path: scalar right operand treated as a single-element
/// array; string vs number stays type-distinct.
#[test]
fn json_member_of_binary_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(cell(&engine, "SELECT 2 MEMBER OF('[1,2,3]')"), "1");
    assert_eq!(cell(&engine, "SELECT 4 MEMBER OF('[1,2,3]')"), "0");
    assert_eq!(cell(&engine, "SELECT 1 MEMBER OF('{\"a\":1}')"), "0");
    // Non-array scalar right operand: single-element membership.
    assert_eq!(cell(&engine, "SELECT 1 MEMBER OF(CAST(1 AS JSON))"), "1");
}

/// P1-3: JSON_MERGE_PATCH follows RFC 7396 — a nested null in the patch
/// deletes the key even when the target key is absent (recurse into `{}`).
#[test]
fn json_merge_patch_strips_nested_null_per_rfc7396() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        cell(
            &engine,
            r#"SELECT JSON_MERGE_PATCH('{}', '{"a":{"b":null}}')"#
        ),
        r#"{"a": {}}"#
    );
    assert_eq!(
        cell(
            &engine,
            r#"SELECT JSON_MERGE_PATCH('{"a":1,"b":2}', '{"a":null,"c":3}')"#
        ),
        r#"{"b": 2, "c": 3}"#
    );
    // A non-object target under an object patch is replaced by `{}` first.
    assert_eq!(
        cell(&engine, r#"SELECT JSON_MERGE_PATCH('1', '{"a":1}')"#),
        r#"{"a": 1}"#
    );
}

/// P1-4: JSON_OVERLAPS handles scalar-vs-array and uses element *equality*,
/// not a recursive overlap.
#[test]
fn json_overlaps_scalar_array_and_equality() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Scalar member of array.
    assert_eq!(cell(&engine, "SELECT JSON_OVERLAPS('[1,2,3]', '2')"), "1");
    assert_eq!(cell(&engine, "SELECT JSON_OVERLAPS('[1,2,3]', '5')"), "0");
    // Two arrays share an element.
    assert_eq!(
        cell(&engine, "SELECT JSON_OVERLAPS('[1,2,3]', '[3,4,5]')"),
        "1"
    );
    // Nested-array elements compare by equality, not recursive overlap.
    assert_eq!(
        cell(&engine, "SELECT JSON_OVERLAPS('[[1,2]]', '[[2,3]]')"),
        "0"
    );
    // Objects share a key with an equal value.
    assert_eq!(
        cell(
            &engine,
            r#"SELECT JSON_OVERLAPS('{"a":1,"b":2}', '{"a":1}')"#
        ),
        "1"
    );
    assert_eq!(
        cell(&engine, r#"SELECT JSON_OVERLAPS('{"a":1}', '{"a":9}')"#),
        "0"
    );
}

/// P1-7: JSON_SEARCH honours a custom escape char, restricts to path args,
/// and reports valid-identifier keys unquoted.
#[test]
fn json_search_escape_and_path_scope() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Valid identifier key → unquoted path.
    assert_eq!(
        cell(
            &engine,
            r#"SELECT JSON_SEARCH('{"name":"abc"}', 'one', 'abc')"#
        ),
        r#""$.name""#
    );
    // Custom escape char: '|' escapes the '%' so it is a literal.
    assert_eq!(
        cell(
            &engine,
            r#"SELECT JSON_SEARCH('{"k":"10%"}', 'one', '10|%', '|')"#
        ),
        r#""$.k""#
    );
    // Path scope: only search under $.b.
    assert_eq!(
        cell(
            &engine,
            r#"SELECT JSON_SEARCH('{"a":"x","b":{"c":"x"}}', 'all', 'x', NULL, '$.b')"#
        ),
        r#""$.b.c""#
    );
}

#[test]
fn json_search_rejects_nonconstant_or_multichar_escape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine
        .execute(r#"SELECT JSON_SEARCH('{"a":"x"}', 'one', 'x', 'ab')"#)
        .unwrap_err();
    assert_eq!(err.code(), 1210);

    engine
        .execute("CREATE TABLE escapes (e VARCHAR(2))")
        .unwrap();
    engine.execute("INSERT INTO escapes VALUES ('!')").unwrap();
    let err = engine
        .execute(r#"SELECT JSON_SEARCH('{"a":"x"}', 'one', 'x', e) FROM escapes"#)
        .unwrap_err();
    assert_eq!(err.code(), 1210);
}

#[test]
fn json_path_quoted_members_decode_unicode_escapes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        cell(&engine, r#"SELECT JSON_EXTRACT('{"a":1}', '$."\\u0061"')"#),
        "1"
    );
    assert_eq!(
        cell(
            &engine,
            r#"SELECT JSON_EXTRACT('{"😀":2}', '$."\\uD83D\\uDE00"')"#,
        ),
        "2"
    );
}

#[test]
fn json_mutators_and_aggregates_preserve_rich_scalar_tags() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let result = rows(
        &engine,
        "SELECT \
           JSON_TYPE(JSON_EXTRACT(JSON_SET(JSON_OBJECT('d', CAST('2026-08-20' AS DATE)), '$.x', 1), '$.d')), \
           JSON_TYPE(JSON_EXTRACT(JSON_ARRAY_APPEND(JSON_ARRAY(CAST('12.340' AS DECIMAL(10,3))), '$', 1), '$[0]')), \
           JSON_TYPE(JSON_EXTRACT(JSON_MERGE_PATCH(JSON_OBJECT('t', CAST('100:00:00' AS TIME)), '{\"x\":1}'), '$.t'))",
    );
    assert_eq!(result[0][0].to_display_string(), "DATE");
    assert_eq!(result[0][1].to_display_string(), "DECIMAL");
    assert_eq!(result[0][2].to_display_string(), "TIME");

    engine
        .execute("CREATE TABLE rich (id INT PRIMARY KEY, d DATE, n DECIMAL(10,3))")
        .unwrap();
    engine
        .execute("INSERT INTO rich VALUES (1, '2026-08-20', 12.340)")
        .unwrap();
    let result = rows(
        &engine,
        "SELECT \
           JSON_TYPE(JSON_EXTRACT(JSON_ARRAYAGG(d), '$[0]')), \
           JSON_TYPE(JSON_EXTRACT(JSON_OBJECTAGG(id, n), '$.\"1\"')) \
         FROM rich",
    );
    assert_eq!(result[0][0].to_display_string(), "DATE");
    assert_eq!(result[0][1].to_display_string(), "DECIMAL");

    let result = rows(
        &engine,
        "SELECT JSON_TYPE(JSON_EXTRACT(JSON_ARRAYAGG(d) OVER (), '$[0]')) FROM rich",
    );
    assert_eq!(result[0][0].to_display_string(), "DATE");
}

#[test]
fn json_temporal_comparison_uses_temporal_values_not_text_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        cell(
            &engine,
            "SELECT JSON_ARRAY(CAST('100:00:00' AS TIME)) > JSON_ARRAY(CAST('99:00:00' AS TIME))",
        ),
        "1"
    );
}

#[test]
fn json_producers_enforce_maximum_document_depth() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Scalar depth is 1, so 99 wrappers form the largest legal input depth
    // (100). One direct JSON producer wrapper must reject depth 101.
    let nested = format!("{}0{}", "[".repeat(99), "]".repeat(99));
    let err = engine
        .execute(&format!("SELECT JSON_ARRAY(CAST('{}' AS JSON))", nested))
        .unwrap_err();
    assert_eq!(err.code(), 3157);

    let err = engine
        .execute(&format!(
            "SELECT JSON_EXTRACT(CAST('{}' AS JSON), '$', '$')",
            nested
        ))
        .unwrap_err();
    assert_eq!(err.code(), 3157);
}

#[test]
fn json_column_rejects_invalid_utf8_binary_input() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE jutf8 (doc JSON)").unwrap();
    let err = engine
        .execute("INSERT INTO jutf8 VALUES (_binary X'80')")
        .unwrap_err();
    assert_eq!(err.code(), 3144);

    let err = engine
        .execute("SELECT JSON_EXTRACT(_binary X'80', '$')")
        .unwrap_err();
    assert_eq!(err.code(), 3144);
}

/// P2-8: a NULL path makes JSON_SET/INSERT/REPLACE/REMOVE return NULL.
#[test]
fn json_mutators_null_path_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        cell(&engine, r#"SELECT JSON_SET('{"a":1}', NULL, 2)"#),
        "NULL"
    );
    assert_eq!(
        cell(&engine, r#"SELECT JSON_REMOVE('{"a":1}', NULL)"#),
        "NULL"
    );
    assert_eq!(
        cell(&engine, r#"SELECT JSON_REPLACE('{"a":1}', NULL, 2)"#),
        "NULL"
    );
}

/// T2: a direct index on a JSON column is rejected with MySQL error 3152
/// (indexing is only via a generated column / multi-valued path), instead of
/// silently building a b-tree over the raw jsonb bytes.
#[test]
fn direct_index_on_json_column_is_rejected_3152() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE jt (id INT PRIMARY KEY, doc JSON)")
        .unwrap();
    let err = engine
        .execute("CREATE INDEX ix ON jt (doc)")
        .expect_err("direct JSON index must be rejected");
    assert_eq!(err.code(), 3152, "expected MySQL error 3152, got {err:?}");
    // Inline form is rejected too.
    let err = engine
        .execute("CREATE TABLE jt2 (id INT PRIMARY KEY, doc JSON, INDEX(doc))")
        .expect_err("inline JSON index must be rejected");
    assert_eq!(err.code(), 3152);
}

/// T3: `information_schema.columns` reports NULL charset/collation for JSON.
#[test]
fn json_column_reports_null_charset_and_collation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE jt (id INT PRIMARY KEY, doc JSON, txt VARCHAR(10))")
        .unwrap();
    let r = rows(
        &engine,
        "SELECT DATA_TYPE, CHARACTER_SET_NAME, COLLATION_NAME \
         FROM information_schema.columns \
         WHERE table_name='jt' AND column_name='doc'",
    );
    assert_eq!(r[0][0].to_display_string(), "json");
    assert!(
        matches!(r[0][1], Value::Null),
        "charset must be NULL: {:?}",
        r[0][1]
    );
    assert!(
        matches!(r[0][2], Value::Null),
        "collation must be NULL: {:?}",
        r[0][2]
    );
    // A real character column still reports its charset (sanity).
    let r = rows(
        &engine,
        "SELECT CHARACTER_SET_NAME FROM information_schema.columns \
         WHERE table_name='jt' AND column_name='txt'",
    );
    assert!(!matches!(r[0][0], Value::Null));
}

// ---------------------------------------------------------------------------
// Phase 1 completion: unified binary-path engine regressions after retiring the weak parser.
// ---------------------------------------------------------------------------

/// The mutators and JSON_CONTAINS address a single location, so a
/// wildcard path must raise MySQL error 3149 rather than silently no-op (which
/// the removed weak parser did).
#[test]
fn json_single_location_ops_reject_wildcard_paths_3149() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        r#"SELECT JSON_SET('{"a":1}', '$.*', 2)"#,
        r#"SELECT JSON_INSERT('{"a":1}', '$**.b', 2)"#,
        r#"SELECT JSON_REPLACE('{"a":1}', '$[*]', 2)"#,
        r#"SELECT JSON_REMOVE('{"a":1}', '$.*')"#,
        r#"SELECT JSON_ARRAY_APPEND('[1]', '$[*]', 2)"#,
        r#"SELECT JSON_ARRAY_INSERT('[1]', '$[*]', 2)"#,
        r#"SELECT JSON_CONTAINS('{"a":1}', '1', '$.*')"#,
    ] {
        let err = engine
            .execute(sql)
            .expect_err(&format!("wildcard path must be rejected: {sql}"));
        assert_eq!(err.code(), 3149, "expected 3149 for `{sql}`, got {err:?}");
    }
}

/// JSON_CONTAINS_PATH, by contrast, *is* defined over wildcard paths and must
/// still evaluate them (previously routed through the weak parser, so a
/// wildcard silently reported "not present").
#[test]
fn json_contains_path_evaluates_wildcards() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        cell(
            &engine,
            r#"SELECT JSON_CONTAINS_PATH('{"a":1,"b":2}', 'one', '$.*')"#
        ),
        "1"
    );
    assert_eq!(
        cell(
            &engine,
            r#"SELECT JSON_CONTAINS_PATH('[{"x":1}]', 'all', '$**.x')"#
        ),
        "1"
    );
    assert_eq!(
        cell(
            &engine,
            r#"SELECT JSON_CONTAINS_PATH('{"a":1}', 'all', '$.a', '$.zzz')"#
        ),
        "0"
    );
}

/// JSON_ARRAY_INSERT requires the path to end in an array cell `[N]`; a path to
/// an object member or the root raises MySQL error 3165 (was a silent no-op).
#[test]
fn json_array_insert_non_array_cell_path_is_3165() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        r#"SELECT JSON_ARRAY_INSERT('{"a":1}', '$.a', 2)"#,
        r#"SELECT JSON_ARRAY_INSERT('[1,2]', '$', 2)"#,
    ] {
        let err = engine
            .execute(sql)
            .expect_err(&format!("non-cell path must be rejected: {sql}"));
        assert_eq!(err.code(), 3165, "expected 3165 for `{sql}`, got {err:?}");
    }
}

/// The unified engine gives every mutator `[last - N]` support, which the old
/// weak parser lacked (it only understood `[last]`).
#[test]
fn json_mutators_support_last_minus_n_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // `[last-1]` is the second-from-last element.
    assert_eq!(
        cell(
            &engine,
            r#"SELECT JSON_REPLACE('[10,20,30]', '$[last-1]', 99)"#
        ),
        "[10, 99, 30]"
    );
    assert_eq!(
        cell(&engine, r#"SELECT JSON_REMOVE('[10,20,30]', '$[last-2]')"#),
        "[20, 30]"
    );
    // `[last]` in JSON_ARRAY_INSERT inserts *before* the last element.
    assert_eq!(
        cell(
            &engine,
            r#"SELECT JSON_ARRAY_INSERT('[1,2,3]', '$[last]', 9)"#
        ),
        "[1, 2, 9, 3]"
    );
}

// ---------------------------------------------------------------------------
// Phase 3 jsonb extension: preserve DECIMAL/DATE/TIME/DATETIME types and precision.
// ---------------------------------------------------------------------------

/// A SQL DECIMAL value cast to JSON keeps DECIMAL type and full precision,
/// while a JSON *text* `'3.14'` parses to DOUBLE (MySQL's split rule).
#[test]
fn json_decimal_value_vs_text_type_and_precision() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        cell(&engine, "SELECT JSON_TYPE(CAST(3.14 AS JSON))"),
        "DECIMAL"
    );
    assert_eq!(
        cell(&engine, "SELECT JSON_TYPE(CAST('3.14' AS JSON))"),
        "DOUBLE"
    );
    // Full precision preserved (an f64 would round this to 10000000000000000).
    assert_eq!(
        cell(&engine, "SELECT CAST(9999999999999999.9 AS JSON)"),
        "9999999999999999.9"
    );
    // A decimal renders as a bare JSON number, not a quoted string.
    assert_eq!(
        cell(&engine, "SELECT JSON_ARRAY(CAST(1.50 AS JSON))"),
        "[1.50]"
    );
}

/// Temporal values cast to JSON report DATE / TIME / DATETIME and render as
/// quoted strings in JSON text (MySQL semantics).
#[test]
fn json_temporal_types_and_rendering() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        cell(
            &engine,
            "SELECT JSON_TYPE(CAST(CAST('2020-01-02' AS DATE) AS JSON))"
        ),
        "DATE"
    );
    assert_eq!(
        cell(
            &engine,
            "SELECT JSON_TYPE(CAST(CAST('12:34:56' AS TIME) AS JSON))"
        ),
        "TIME"
    );
    assert_eq!(
        cell(
            &engine,
            "SELECT JSON_TYPE(CAST(CAST('2020-01-02 12:34:56' AS DATETIME) AS JSON))"
        ),
        "DATETIME"
    );
    // A DATE inside a JSON array renders as a quoted string.
    assert_eq!(
        cell(&engine, "SELECT JSON_ARRAY(CAST('2020-01-02' AS DATE))"),
        r#"["2020-01-02"]"#
    );
    // `->>` unquotes it back to the plain text.
    assert_eq!(
        cell(
            &engine,
            "SELECT JSON_ARRAY(CAST('2020-01-02' AS DATE))->>'$[0]'"
        ),
        "2020-01-02"
    );
}

/// A decimal stored into a real JSON column round-trips as DECIMAL through the
/// heap (the storage write path also uses the tagged scalar encoder).
#[test]
fn json_column_stores_decimal_with_type() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE jt (id INT PRIMARY KEY, doc JSON)")
        .unwrap();
    engine
        .execute("INSERT INTO jt VALUES (1, CAST(123.456 AS JSON))")
        .unwrap();
    assert_eq!(
        cell(&engine, "SELECT JSON_TYPE(doc) FROM jt WHERE id=1"),
        "DECIMAL"
    );
    assert_eq!(cell(&engine, "SELECT doc FROM jt WHERE id=1"), "123.456");
}

/// Exact numeric comparison across sub-types: a decimal equals the same
/// integer/double value, and a u64 above i64::MAX orders correctly (P2-9 — the
/// old f64 fallback mis-compared large unsigned integers).
#[test]
fn json_numeric_comparison_is_exact_across_subtypes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // 2 (integer) vs 2.00 (decimal) compare equal on the binary form.
    assert_eq!(
        cell(&engine, "SELECT CAST(2.00 AS JSON) = CAST(2 AS JSON)"),
        "1"
    );
    // Two u64 values above i64::MAX, differing only in the low bits, order
    // strictly — an f64 fallback would round both to the same double.
    assert_eq!(
        cell(
            &engine,
            "SELECT CAST('18446744073709551615' AS JSON) > CAST('18446744073709551614' AS JSON)"
        ),
        "1"
    );
}

#[test]
fn nested_json_lob_survives_update_rollback_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE nested_lob (id INT PRIMARY KEY, doc JSON)")
        .unwrap();
    let mut document = serde_json::json!({"payload": "中\\\"'🙂".repeat(12000), "n": u64::MAX});
    for _ in 0..64 {
        document = serde_json::json!({"nested": document});
    }
    let text = document.to_string();
    let quoted = text.replace('\\', "\\\\").replace('\'', "''");
    engine
        .execute(&format!("INSERT INTO nested_lob VALUES (1, '{quoted}')"))
        .unwrap();
    let original = cell(&engine, "SELECT doc FROM nested_lob WHERE id=1");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&original).unwrap(),
        document
    );
    // An unchanged external JSON value exercises overflow-chain reuse.
    engine
        .execute("UPDATE nested_lob SET id=2 WHERE id=1")
        .unwrap();
    let mut session = SessionContext::default();
    session.connection_id = 9701;
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut session,
            "UPDATE nested_lob SET doc=JSON_OBJECT('changed', true) WHERE id=2",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        cell(&engine, "SELECT doc FROM nested_lob WHERE id=2"),
        original
    );
    engine.checkpoint().unwrap();
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        cell(&engine, "SELECT doc FROM nested_lob WHERE id=2"),
        original
    );
}
