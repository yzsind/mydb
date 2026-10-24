use super::*;

fn one_row(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Value> {
    let QueryResponse::ResultSet(result) = engine.execute_with_session(session, sql).unwrap()
    else {
        panic!("expected result set for: {sql}");
    };
    assert_eq!(result.rows.len(), 1, "expected one row for: {sql}");
    result.rows.into_iter().next().unwrap()
}

#[test]
fn charset_defaults_inherit_and_surface_in_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE DATABASE ch DEFAULT CHARACTER SET latin1 COLLATE latin1_bin",
        )
        .unwrap();
    engine.execute_with_session(&mut session, "USE ch").unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (name VARCHAR(20), code VARCHAR(20) CHARACTER SET ascii)",
        )
        .unwrap();

    let row = one_row(&engine, &mut session, "SHOW CREATE TABLE t");
    let create_sql = row[1].to_display_string();
    assert!(create_sql.contains("DEFAULT CHARSET=latin1 COLLATE=latin1_bin"));

    let QueryResponse::ResultSet(columns) = engine
        .execute_with_session(&mut session, "SHOW FULL COLUMNS FROM t")
        .unwrap()
    else {
        panic!("expected columns");
    };
    assert_eq!(columns.columns[1].name, "Type");
    assert_eq!(columns.columns[2].name, "Collation");
    assert_eq!(columns.rows[0][2], Value::String("latin1_bin".into()));
    assert_eq!(columns.rows[1][2], Value::String("ascii_general_ci".into()));

    let row = one_row(
        &engine,
        &mut session,
        "SELECT character_set_name, collation_name \
         FROM information_schema.columns \
         WHERE table_schema='ch' AND table_name='t' AND column_name='code'",
    );
    assert_eq!(row[0], Value::String("ascii".into()));
    assert_eq!(row[1], Value::String("ascii_general_ci".into()));
}

#[test]
fn show_full_columns_uses_null_collation_for_binary_types() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (b BLOB, vb VARBINARY(8), s VARCHAR(8))",
        )
        .unwrap();
    let QueryResponse::ResultSet(columns) = engine
        .execute_with_session(&mut session, "SHOW FULL COLUMNS FROM t")
        .unwrap()
    else {
        panic!("expected columns");
    };

    assert_eq!(columns.rows[0][2], Value::Null);
    assert_eq!(columns.rows[1][2], Value::Null);
    assert_eq!(
        columns.rows[2][2],
        Value::String(crate::charset::DEFAULT_COLLATION.to_string())
    );
}

#[test]
fn set_names_updates_session_charset_variables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci")
        .unwrap();
    let row = one_row(
        &engine,
        &mut session,
        "SELECT @@character_set_client, @@character_set_connection, \
                @@character_set_results, @@collation_connection",
    );
    assert_eq!(row[0], Value::String("utf8mb4".into()));
    assert_eq!(row[1], Value::String("utf8mb4".into()));
    assert_eq!(row[2], Value::String("utf8mb4".into()));
    assert_eq!(row[3], Value::String("utf8mb4_0900_ai_ci".into()));

    engine
        .execute_with_session(&mut session, "SET NAMES DEFAULT")
        .unwrap();
    let row = one_row(&engine, &mut session, "SELECT @@collation_connection");
    assert_eq!(row[0], Value::String("utf8mb4_0900_ai_ci".into()));
}

#[test]
fn character_set_results_accepts_null_for_mysql_clients() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET character_set_results = NULL")
        .unwrap();
    let row = one_row(&engine, &mut session, "SELECT @@character_set_results");
    assert_eq!(row[0], Value::Null);

    let QueryResponse::ResultSet(vars) = engine
        .execute_with_session(&mut session, "SHOW VARIABLES LIKE 'character_set_results'")
        .unwrap()
    else {
        panic!("expected variables");
    };
    assert_eq!(vars.rows[0][1], Value::String("NULL".into()));

    engine
        .execute_with_session(&mut session, "SET character_set_results = utf8mb4")
        .unwrap();
    let row = one_row(&engine, &mut session, "SELECT @@character_set_results");
    assert_eq!(row[0], Value::String("utf8mb4".into()));
}

#[test]
fn default_utf8mb4_0900_ai_ci_collation_drives_compare_and_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let row = one_row(
        &engine,
        &mut session,
        "SELECT 'a' = 'A', 'e' = 'é', 'a   ' = 'a'",
    );
    assert_eq!(row, vec![Value::Int(1), Value::Int(1), Value::Int(0)]);

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1,'a'),(2,'A'),(3,'á')")
        .unwrap();
    let row = one_row(&engine, &mut session, "SELECT COUNT(DISTINCT v) FROM t");
    assert_eq!(row[0], Value::Int(1));
    let row = one_row(&engine, &mut session, "SELECT COUNT(*) FROM t GROUP BY v");
    assert_eq!(row[0], Value::Int(3));
}

#[test]
fn count_distinct_uses_column_pad_space_collation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (v VARCHAR(400)) \
             CHARACTER SET latin1 COLLATE latin1_swedish_ci",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t VALUES \
             ('A'),('a'),('a '),('a   '),('B'),('b'),('b '),('b   ')",
        )
        .unwrap();

    let row = one_row(&engine, &mut session, "SELECT COUNT(DISTINCT v) FROM t");
    assert_eq!(row[0], Value::Int(2));
}

#[test]
fn explicit_binary_collation_compares_bytewise() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let row = one_row(
        &engine,
        &mut session,
        "SELECT 'a' COLLATE utf8mb4_bin = 'A', \
                'a' COLLATE utf8mb4_bin <> 'A'",
    );
    assert_eq!(row, vec![Value::Int(0), Value::Int(1)]);
}

#[test]
fn in_list_aggregates_all_operand_collations_with_mysql_errnos() {
    let _env = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE coll_in (
                a CHAR(1) CHARACTER SET latin1 COLLATE latin1_general_ci,
                b CHAR(1) CHARACTER SET latin1 COLLATE latin1_swedish_ci,
                c CHAR(1) CHARACTER SET latin1 COLLATE latin1_danish_ci
            )",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO coll_in VALUES ('A','B','C'),('a','c','c')",
        )
        .unwrap();

    for vectorized in ["off", "on", "auto"] {
        with_test_env(&[("MYDB_VECTORIZED", Some(vectorized))], || {
            let two = engine
                .execute_with_session(&mut session, "SELECT * FROM coll_in WHERE a IN (b)")
                .unwrap_err();
            assert_eq!(two.code(), 1267, "mode={vectorized}: {}", two.message());

            let three = engine
                .execute_with_session(&mut session, "SELECT * FROM coll_in WHERE a IN (b,c)")
                .unwrap_err();
            assert_eq!(three.code(), 1270, "mode={vectorized}: {}", three.message());

            let many = engine
                .execute_with_session(&mut session, "SELECT * FROM coll_in WHERE 'a' IN (a,b,c)")
                .unwrap_err();
            assert_eq!(many.code(), 1271, "mode={vectorized}: {}", many.message());

            engine
                .execute_with_session(
                    &mut session,
                    "SELECT * FROM coll_in
                     WHERE 'a' COLLATE latin1_general_ci IN (a,b,c)",
                )
                .unwrap();

            let projection = engine
                .execute_with_session(
                    &mut session,
                    "SELECT a, a IN ('a') FROM coll_in ORDER BY BINARY a",
                )
                .unwrap();
            let QueryResponse::ResultSet(projection_rows) = &projection else {
                panic!("expected projection result set");
            };
            assert!(
                projection_rows
                    .rows
                    .iter()
                    .all(|row| row.get(1) == Some(&Value::Int(1))),
                "mode={vectorized}, projection={projection:?}"
            );
            let equality = engine
                .execute_with_session(
                    &mut session,
                    "SELECT a FROM coll_in WHERE a = 'a' ORDER BY BINARY a",
                )
                .unwrap();
            let QueryResponse::ResultSet(equality_rows) = &equality else {
                panic!("expected equality result set");
            };
            assert_eq!(
                equality_rows.rows.len(),
                2,
                "mode={vectorized}, projection={projection:?}"
            );
            let QueryResponse::ResultSet(rows) = engine
                .execute_with_session(
                    &mut session,
                    "SELECT a FROM coll_in WHERE a IN ('a') ORDER BY BINARY a",
                )
                .unwrap()
            else {
                panic!("expected result set");
            };
            assert_eq!(
                rows.rows.len(),
                2,
                "mode={vectorized}, projection={projection:?}, equality={equality:?}"
            );
        });
    }
}

#[test]
fn ascii_and_latin1_assignment_validation_is_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, a VARCHAR(20) CHARACTER SET ascii, \
                            l VARCHAR(20) CHARACTER SET latin1)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 'abc', 'é')")
        .unwrap();

    let ascii_err = engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (2, 'é', 'ok')")
        .unwrap_err();
    assert_eq!(ascii_err.code(), 1366);

    let latin1_err = engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (3, 'abc', '中')")
        .unwrap_err();
    assert_eq!(latin1_err.code(), 1366);
}

#[test]
fn create_table_like_preserves_charset_and_collation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE src (name VARCHAR(20) CHARACTER SET ascii COLLATE ascii_bin) \
             DEFAULT CHARSET=latin1 COLLATE=latin1_bin",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE dst LIKE src")
        .unwrap();

    let row = one_row(&engine, &mut session, "SHOW CREATE TABLE dst");
    let create_sql = row[1].to_display_string();
    assert!(create_sql.contains("DEFAULT CHARSET=latin1 COLLATE=latin1_bin"));

    let row = one_row(
        &engine,
        &mut session,
        "SELECT character_set_name, collation_name \
         FROM information_schema.columns \
         WHERE table_name='dst' AND column_name='name'",
    );
    assert_eq!(row[0], Value::String("ascii".into()));
    assert_eq!(row[1], Value::String("ascii_bin".into()));
}

#[test]
fn indexes_and_unique_constraints_use_column_collation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20), INDEX(v))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1,'a'),(2,'A'),(3,'b')")
        .unwrap();
    let row = one_row(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t WHERE v = 'A'",
    );
    assert_eq!(row[0], Value::Int(2));

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE pad_idx (id INT PRIMARY KEY AUTO_INCREMENT, \
             v TEXT, INDEX v_idx (v(20))) DEFAULT CHARSET latin1",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO pad_idx(v) VALUES ('Chevy ')")
        .unwrap();
    let pad_schema = engine.database().table("test.pad_idx").unwrap();
    let pad_column = &pad_schema.columns[1];
    let pad_index = pad_schema
        .indexes
        .iter()
        .find(|index| index.name.eq_ignore_ascii_case("v_idx"))
        .unwrap();
    let stored_key = pad_schema
        .index_key(
            pad_index,
            &[Value::Int(1), Value::String("Chevy ".to_string())],
        )
        .unwrap();
    let target = crate::storage::ParamTarget::for_column_with_collation(
        &pad_column.data_type,
        pad_column.unsigned,
        pad_column.collation.as_deref(),
    );
    let probe_key = crate::storage::IndexKey::from(vec![pad_index.apply_prefix_to_key_part(
        0,
        crate::storage::KeyPart::for_target(Value::String("Chevy".to_string()), target).unwrap(),
    )]);
    assert_eq!(
        stored_key, probe_key,
        "PAD SPACE stored/probe key mismatch: column={pad_column:?}"
    );
    assert_eq!(
        engine
            .database()
            .prefix_index(&pad_schema, pad_index, probe_key.clone())
            .unwrap()
            .len(),
        1,
        "PAD SPACE probe key must locate the persisted prefix-index entry"
    );
    let between_expr = crate::parser::parse_expr_fragment("v BETWEEN 'Chevy' AND 'Chevy'").unwrap();
    assert_eq!(
        crate::engine::expr::eval_expr(
            &between_expr,
            &pad_schema,
            &[Value::Int(1), Value::String("Chevy ".into())],
        )
        .unwrap(),
        Value::Int(1),
        "legacy row evaluator lost the column collation for BETWEEN"
    );
    let equality_expr = crate::parser::parse_expr_fragment("v = 'Chevy'").unwrap();
    assert_eq!(
        crate::engine::expr::eval_expr(
            &equality_expr,
            &pad_schema,
            &[Value::Int(1), Value::String("Chevy ".into())],
        )
        .unwrap(),
        Value::Int(1),
        "legacy row evaluator lost the column collation for equality"
    );
    let row = one_row(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM pad_idx WHERE v = 'Chevy'",
    );
    let plan = one_row(
        &engine,
        &mut session,
        "EXPLAIN FORMAT=TREE SELECT COUNT(*) FROM pad_idx WHERE v = 'Chevy'",
    );
    assert_eq!(
        row[0],
        Value::Int(1),
        "PAD SPACE equality must not depend on whether a prefix index is chosen; plan={plan:?}"
    );
    let projected_between = one_row(
        &engine,
        &mut session,
        "SELECT v BETWEEN 'Chevy' AND 'Chevy' FROM pad_idx",
    );
    let row = one_row(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM pad_idx WHERE v BETWEEN 'Chevy' AND 'Chevy'",
    );
    let between_plan = one_row(
        &engine,
        &mut session,
        "EXPLAIN FORMAT=TREE SELECT COUNT(*) FROM pad_idx \
         WHERE v BETWEEN 'Chevy' AND 'Chevy'",
    );
    assert_eq!(
        row[0],
        Value::Int(1),
        "BETWEEN must use the same PAD SPACE collation as scalar comparisons; \
         projection={projected_between:?}, plan={between_plan:?}"
    );

    engine
        .execute_with_session(&mut session, "CREATE TABLE u (v VARCHAR(20) UNIQUE)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO u VALUES ('a')")
        .unwrap();
    let duplicate = engine
        .execute_with_session(&mut session, "INSERT INTO u VALUES ('A')")
        .unwrap_err();
    assert_eq!(duplicate.code(), 1062);

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE ubin (v VARCHAR(20) COLLATE utf8mb4_bin UNIQUE)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO ubin VALUES ('a'),('A')")
        .unwrap();
    let row = one_row(&engine, &mut session, "SELECT COUNT(*) FROM ubin");
    assert_eq!(row[0], Value::Int(2));
}

#[test]
fn unknown_charset_and_collation_return_mysql_style_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let charset_err = engine
        .execute_with_session(&mut session, "SET NAMES imaginary_charset")
        .unwrap_err();
    assert_eq!(charset_err.code(), 1115);

    let collation_err = engine
        .execute_with_session(&mut session, "SET NAMES utf8mb4 COLLATE imaginary_ci")
        .unwrap_err();
    assert_eq!(collation_err.code(), 1273);

    let mismatch_err = engine
        .execute_with_session(&mut session, "SET NAMES latin1 COLLATE utf8mb4_0900_ai_ci")
        .unwrap_err();
    assert_eq!(mismatch_err.code(), 1253);

    let client_charset_err = engine
        .execute_with_session(&mut session, "SET NAMES utf16")
        .unwrap_err();
    assert_eq!(client_charset_err.code(), 1231);
}

#[test]
fn set_names_utf8mb3_emits_mysql_deprecation_warning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET NAMES utf8mb3")
        .unwrap();
    assert!(
        session
            .warnings
            .iter()
            .any(|warning| warning.code() == 1287),
        "SET NAMES utf8mb3 warnings: {:?}",
        session.warnings
    );

    one_row(
        &engine,
        &mut session,
        "SELECT CONVERT('text' USING utf8mb3)",
    );
    assert!(
        session
            .warnings
            .iter()
            .any(|warning| warning.code() == 1287),
        "CONVERT USING utf8mb3 warnings: {:?}",
        session.warnings
    );
}

#[test]
fn utf8mb3_client_reports_supplementary_character_conversion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET NAMES utf8mb3")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE supplementary_𩸽 (a INT)")
        .unwrap();
    assert!(
        session
            .warnings
            .iter()
            .any(|warning| warning.code() == 1300),
        "supplementary utf8mb3 warnings: {:?}",
        session.warnings
    );
}

#[test]
fn digest_utf8mb3_prefilter_only_routes_supplementary_text() {
    assert!(!digest_text_may_contain_supplementary(
        "SELECT a FROM ordinary_table WHERE id = ?"
    ));
    assert!(!digest_text_may_contain_supplementary(
        "SELECT 名称 FROM 商品 WHERE 编号 = ?"
    ));
    assert!(digest_text_may_contain_supplementary(
        "CREATE TABLE supplementary_𩸽 (a INT)"
    ));
}

#[test]
fn supplementary_literals_and_comments_do_not_emit_digest_identifier_warning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    one_row(&engine, &mut session, "SELECT '𩸽'");
    assert!(
        session
            .warnings
            .iter()
            .all(|warning| warning.code() != 1300),
        "supplementary literal warnings: {:?}",
        session.warnings
    );

    one_row(&engine, &mut session, "SELECT 1 /* 𩸽 */");
    assert!(
        session
            .warnings
            .iter()
            .all(|warning| warning.code() != 1300),
        "supplementary comment warnings: {:?}",
        session.warnings
    );
}

#[test]
fn sha2_null_or_invalid_length_warns_but_null_input_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let row = one_row(&engine, &mut session, "SELECT SHA2('x', NULL)");
    assert_eq!(row[0], Value::Null);
    assert!(session
        .warnings
        .iter()
        .any(|warning| warning.code() == 1583));

    let row = one_row(&engine, &mut session, "SELECT SHA2(NULL, 224)");
    assert_eq!(row[0], Value::Null);
    assert!(
        session.warnings.is_empty(),
        "NULL input should propagate silently: {:?}",
        session.warnings
    );
}

#[test]
fn sha2_protocol_metadata_tracks_digest_width_and_connection_charset() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let parsed = crate::parser::parse_expr_fragment("SHA2('1', 224)").unwrap();
    assert_eq!(
        crate::engine::expr::expression_metadata_length(&parsed),
        Some(224),
        "parsed SHA2 expression: {parsed:?}"
    );

    for (charset, expected_collation, expected_length, expected_flags) in [
        ("binary", 63, 56, crate::mysql::protocol::BINARY_FLAG),
        ("utf8mb3", 33, 168, 0),
        ("latin1", 8, 56, 0),
    ] {
        engine
            .execute_with_session(&mut session, &format!("SET NAMES {charset}"))
            .unwrap();
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(&mut session, "SELECT SHA2('1', 224)")
            .unwrap()
        else {
            panic!("SHA2 SELECT did not return a result set");
        };
        let source = &result.columns[0].source;
        assert_eq!(source.collation_id, Some(expected_collation));
        assert_eq!(source.length, Some(expected_length));
        assert_eq!(source.decimals, Some(31));
        assert_eq!(
            source.flags & crate::mysql::protocol::BINARY_FLAG,
            expected_flags
        );
    }
}

#[test]
fn statement_digest_hashes_mysql_tokens_and_preserves_latin1_result_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let row = one_row(
        &engine,
        &mut session,
        "SELECT STATEMENT_DIGEST(CONVERT('SELECT 1 AS Å' USING latin1))",
    );
    assert_eq!(
        row[0],
        Value::String("0ccda2949946c24efb22d29e2daf034b2835b25ad2689011f810ab515fa36ca9".into())
    );

    engine
        .execute_with_session(&mut session, "SET NAMES latin1")
        .unwrap();
    let row = one_row(
        &engine,
        &mut session,
        "SELECT STATEMENT_DIGEST_TEXT(CONVERT('SELECT 1 AS Å' USING latin1))",
    );
    assert_eq!(row[0], Value::String("SELECT ? AS `Å`".into()));
}

#[test]
fn show_charset_and_collation_expose_mysql80_surface() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(charsets) = engine
        .execute_with_session(&mut session, "SHOW CHARACTER SET")
        .unwrap()
    else {
        panic!("expected charsets");
    };
    assert_eq!(charsets.rows.len(), 41);
    assert!(charsets.rows.iter().any(|row| {
        row[0] == Value::String("gb18030".into())
            && row[2] == Value::String("gb18030_chinese_ci".into())
    }));

    let row = one_row(
        &engine,
        &mut session,
        "SELECT collation_name, pad_attribute \
         FROM information_schema.collations \
         WHERE collation_name='utf8mb4_0900_ai_ci'",
    );
    assert_eq!(row[0], Value::String("utf8mb4_0900_ai_ci".into()));
    assert_eq!(row[1], Value::String("NO PAD".into()));
}

fn row_strings(rs: QueryResponse) -> Vec<Vec<String>> {
    let QueryResponse::ResultSet(rs) = rs else {
        panic!("expected result set");
    };
    rs.rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|v| match v {
                    Value::String(s) => s,
                    Value::SmallString(b) => String::from_utf8(b.as_slice().to_vec()).unwrap(),
                    other => panic!("expected string value, got {other:?}"),
                })
                .collect()
        })
        .collect()
}

/// P0-1: `WHERE col = 'literal'` on a column declared `COLLATE utf8mb4_bin`
/// must stay case-sensitive under the residual filter (seq-scan) path, not
/// just the index-driven path. Previously `compare_values` always compared
/// under the server default `utf8mb4_0900_ai_ci`, so the seq-scan branch
/// returned rows the column collation would have excluded — and identical
/// queries returned different rows depending on whether the CBO picked
/// the index or not.
#[test]
fn p0_1_seq_scan_equality_respects_column_collation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE ct (v VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO ct VALUES ('a'),('A'),('b'),('B')",
        )
        .unwrap();
    let rows = row_strings(
        engine
            .execute_with_session(&mut session, "SELECT v FROM ct WHERE v='A'")
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![vec!["A".to_string()]],
        "utf8mb4_bin equality must be case-sensitive under seq-scan"
    );
    engine
        .execute_with_session(&mut session, "DELETE FROM ct WHERE v='A'")
        .unwrap();
    let rows = row_strings(
        engine
            .execute_with_session(&mut session, "SELECT v FROM ct ORDER BY v")
            .unwrap(),
    );
    // Depending on ORDER BY collation, exact ordering may vary; assert set.
    let set: std::collections::BTreeSet<String> = rows
        .into_iter()
        .map(|r| r.into_iter().next().unwrap())
        .collect();
    let expected: std::collections::BTreeSet<String> =
        ["a", "B", "b"].iter().map(|s| s.to_string()).collect();
    assert_eq!(
        set, expected,
        "DELETE with utf8mb4_bin must remove only the 'A' row"
    );
}

/// P0-1: for the default collation column, existing case-insensitive
/// equality behaviour is preserved (no regression).
#[test]
fn p0_1_default_collation_equality_stays_case_insensitive() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE dc (v VARCHAR(10))")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO dc VALUES ('a'),('A'),('b'),('B')",
        )
        .unwrap();
    let rows = row_strings(
        engine
            .execute_with_session(&mut session, "SELECT v FROM dc WHERE v='A' ORDER BY v")
            .unwrap(),
    );
    let set: std::collections::BTreeSet<String> = rows
        .into_iter()
        .map(|r| r.into_iter().next().unwrap())
        .collect();
    assert_eq!(
        set,
        ["A".to_string(), "a".to_string()]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        "default utf8mb4_0900_ai_ci must remain case-insensitive"
    );
}

#[test]
fn distinct_explicit_collations_are_always_an_illegal_mix() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    for sql in [
        "SELECT '1' COLLATE latin1_swedish_ci = '1' COLLATE latin1_general_ci",
        "SELECT 'a' COLLATE latin1_general_ci = 'A' COLLATE latin1_bin",
    ] {
        let err = engine.execute(sql).expect_err(sql);
        assert_eq!(err.code(), 1267, "{sql}: {err:?}");
        assert_eq!(err.sql_state(), "HY000");
    }
}

#[test]
fn equal_strength_character_set_introducers_reject_incompatible_charsets() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine
        .execute("SELECT _latin1'1' = _latin2'1'")
        .expect_err("equal-strength incompatible introducers must fail");
    assert_eq!(err.code(), 1267, "{err:?}");
    assert_eq!(err.sql_state(), "HY000");
}

#[test]
fn empty_input_still_rejects_incompatible_column_collations_at_bind_time() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE ct_mix (\
             a VARCHAR(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci,\
             b VARCHAR(10) CHARACTER SET latin2 COLLATE latin2_general_ci)",
        )
        .unwrap();

    for sql in [
        "SELECT a = b FROM ct_mix",
        "SELECT CONCAT(a, b) FROM ct_mix",
    ] {
        let err = engine.execute(sql).expect_err(sql);
        assert_eq!(err.code(), 1267, "{sql}: {err:?}");
        assert_eq!(err.sql_state(), "HY000");
    }
}

#[test]
fn unicode_superset_and_concat_none_derivation_are_bound_without_false_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE ct_superset (\
             u1 VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,\
             u2 VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs,\
             legacy VARCHAR(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci)",
        )
        .unwrap();

    // Unicode is the conversion superset for a legacy character set.
    engine
        .execute("SELECT u1 = legacy FROM ct_superset")
        .unwrap();
    engine
        .execute("SELECT CONCAT(u1, legacy) FROM ct_superset")
        .unwrap();

    // Equal-strength collations within one character set conflict for a
    // comparison, but CONCAT may produce a binary/no-derivation result.
    let err = engine
        .execute("SELECT u1 = u2 FROM ct_superset")
        .expect_err("comparison must reject distinct implicit collations");
    assert_eq!(err.code(), 1267);
    engine
        .execute("SELECT CONCAT(u1, u2) FROM ct_superset")
        .unwrap();
    let err = engine
        .execute("SELECT CONCAT(u1, u2) = CONCAT(u1, u2) FROM ct_superset")
        .expect_err("two no-derivation string results cannot be compared");
    assert_eq!(err.code(), 1267);
    engine
        .execute("SELECT CONCAT(CONCAT(u1, u2), CONCAT(u1, u2)) FROM ct_superset")
        .unwrap();
}

#[test]
fn substring_index_converts_delimiter_instead_of_aggregating_collations() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        &mut SessionContext::default(),
        "SELECT SUBSTRING_INDEX(_latin1'abcdabcdabcd', _latin2'd', 2)",
    );
    assert_eq!(row[0], Value::String("abcdabc".into()));
}

#[test]
fn char_using_decodes_bytes_and_reports_invalid_sequences_as_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let row = one_row(
        &engine,
        &mut session,
        "SELECT CHAR(65 USING utf8mb4), SUBSTRING('abc', CAST(2 AS UNSIGNED INT)), \
         HEX(WEIGHT_STRING('aa' AS BINARY(3))), \
         HEX(WEIGHT_STRING(CAST('aa' AS BINARY(3))))",
    );
    assert_eq!(row[0], Value::String("A".into()));
    assert_eq!(row[1], Value::String("bc".into()));
    assert_eq!(row[2], Value::String("616100".into()));
    assert_eq!(row[3], Value::String("616100".into()));

    let row = one_row(
        &engine,
        &mut session,
        "SELECT CHAR(0xff, 0x8f USING utf8mb3)",
    );
    assert_eq!(row[0], Value::Null);
    let QueryResponse::ResultSet(warnings) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected warnings");
    };
    assert!(warnings
        .rows
        .iter()
        .any(|row| { matches!(row.get(1), Some(Value::Int(1300)) | Some(Value::UInt(1300))) }));
}

#[test]
fn string_functions_reject_unconvertible_constant_bytes_at_bind_time() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE text_target (a VARCHAR(10) CHARACTER SET utf8mb4)")
        .unwrap();

    for sql in [
        "SELECT LOCATE(x'ff', _utf8mb4'abcdef')",
        "SELECT FIELD(_utf8mb4'abc', x'ff')",
        "SELECT CONCAT(a, x'ff') FROM text_target",
        "SELECT SUBSTRING_INDEX(_latin1'hello', _latin2 x'a3', 2)",
        "SELECT REPLACE(_latin1'hello', _latin2 x'a3', 'hei')",
        "SELECT LTRIM(WEIGHT_STRING(1))",
        "SELECT RTRIM(WEIGHT_STRING(1))",
    ] {
        let err = engine.execute(sql).expect_err(sql);
        assert_eq!(err.code(), 3854, "{sql}: {err:?}");
        assert_eq!(err.sql_state(), "HY000");
    }
}

#[test]
fn connection_collation_accepts_server_side_unicode_repertoires() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "SET SESSION collation_connection = 'utf16_unicode_ci'",
        )
        .unwrap();
    assert_eq!(session.character_set_connection, "utf16");
    assert_eq!(session.collation_connection, "utf16_unicode_ci");

    engine
        .execute_with_session(
            &mut session,
            "SET SESSION character_set_connection = 'utf32'",
        )
        .unwrap();
    assert_eq!(session.character_set_connection, "utf32");

    // Protocol input is a narrower repertoire than expression conversion.
    let err = engine
        .execute_with_session(&mut session, "SET SESSION character_set_client = 'utf16'")
        .unwrap_err();
    assert_eq!((err.code(), err.sql_state().as_ref()), (1231, "42000"));
}

#[test]
fn binary_columns_compare_to_folded_hex_literals_without_text_conversion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE binary_cmp (id BINARY(16) PRIMARY KEY, parent_id VARBINARY(16)); \
             INSERT INTO binary_cmp VALUES \
             (X'79cea9ab6fe14a8ebdfed711a7727763', X'93222796caba43ca979f5c96eb6898b7')",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT id FROM binary_cmp \
             WHERE parent_id = X'93222796caba43ca979f5c96eb6898b7'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn binary_columns_accept_high_bytes_in_all_hex_literal_forms() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE binary_high (id INT PRIMARY KEY, v VARBINARY(100), b BINARY(2), t BLOB, KEY kv(v))").unwrap();
    for (id, literal) in [(1, "0xFF"), (2, "X'80'"), (3, "0x0080FF")] {
        engine
            .execute(&format!(
                "INSERT INTO binary_high (id, v, b, t) VALUES ({id}, {literal}, 0x80FF, {literal})"
            ))
            .unwrap();
        let QueryResponse::ResultSet(rows) = engine
            .execute(&format!(
                "SELECT id FROM binary_high FORCE INDEX (kv) WHERE v={literal}"
            ))
            .unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(rows.rows.len(), 1);
    }
}
