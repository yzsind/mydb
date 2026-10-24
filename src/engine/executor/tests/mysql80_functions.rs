//! Coverage for additional MySQL 8.0 compatibility functions.
//! Cover SUBSTRING_INDEX, MID, WEIGHT_STRING, YEARWEEK, TIME_FORMAT,
//! JSON_ARRAY_INSERT / JSON_MERGE / JSON_STORAGE_SIZE / JSON_STORAGE_FREE /
//! INTERVAL / ANY_VALUE / UUID_SHORT / GROUPING / NAME_CONST / SYSTEM_USER /
//! FORMAT_BYTES / FORMAT_PICO_TIME / STATEMENT_DIGEST / STATEMENT_DIGEST_TEXT /
//! VALIDATE_PASSWORD_STRENGTH, and the GET_LOCK family.
use super::support::query_one_row as one_row;
use super::*;

#[test]
fn substring_index_handles_positive_and_negative_counts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let row = one_row(
        &engine,
        "SELECT \
            SUBSTRING_INDEX('www.mysql.com', '.', 2), \
            SUBSTRING_INDEX('www.mysql.com', '.', -2), \
            SUBSTRING_INDEX('a,b,c,d', ',', 0), \
            SUBSTRING_INDEX('a,b,c,d', ',', 100), \
            SUBSTRING_INDEX('a,b,c,d', ',', -100), \
            SUBSTRING_INDEX('abc', '', 2)",
    );
    assert_eq!(row[0], Value::String("www.mysql".into()));
    assert_eq!(row[1], Value::String("mysql.com".into()));
    assert_eq!(row[2], Value::String(String::new()));
    assert_eq!(row[3], Value::String("a,b,c,d".into()));
    assert_eq!(row[4], Value::String("a,b,c,d".into()));
    assert_eq!(row[5], Value::String(String::new()));

    // Both signs use non-overlapping matches scanned from the left.
    // The final `aa` starts at byte six, leaving the suffix `a1`.
    let row = one_row(
        &engine,
        "SELECT \
            SUBSTRING_INDEX('aaaaaaaaa1', 'aa', -1), \
            SUBSTRING_INDEX('aaaaaaaaa1', 'aa', -2), \
            SUBSTRING_INDEX('aaaa', 'aa', 2)",
    );
    assert_eq!(row[0], Value::String("a1".into()));
    assert_eq!(row[1], Value::String("aaa1".into()));
    assert_eq!(row[2], Value::String("aa".into()));
}

#[test]
fn soundex_uses_mysql_variable_length_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT SOUNDEX(''), SOUNDEX('he'), \
                SOUNDEX('hello all folks'), SOUNDEX('#3556 in bugdb')",
    );
    assert_eq!(
        row,
        vec![
            Value::String(String::new()),
            Value::String("H000".into()),
            Value::String("H4142".into()),
            Value::String("I51231".into()),
        ]
    );
}

#[test]
fn benchmark_rejects_null_and_negative_counts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT BENCHMARK(NULL, 1 + 1), BENCHMARK(-1, 1), BENCHMARK(0, 1)",
    );
    assert_eq!(row, vec![Value::Null, Value::Null, Value::Int(0)]);
}

#[test]
fn mid_is_alias_for_substring() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT MID('abcdef', 2, 3), MID('abcdef', -3, 2), MID('abcdef', 4)",
    );
    assert_eq!(row[0], Value::String("bcd".into()));
    assert_eq!(row[1], Value::String("de".into()));
    assert_eq!(row[2], Value::String("def".into()));
}

#[test]
fn greatest_uses_one_string_context_for_mixed_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT GREATEST(-29858, 0x667adb23, 'zzzz', 0, 15720), \
                LEAST(20, '3', 100)",
    );
    assert_eq!(row[0], Value::String("zzzz".into()));
    assert_eq!(row[1], Value::Int(100));
}

#[test]
fn weight_string_returns_case_folded_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(&engine, "SELECT WEIGHT_STRING('AbCd')");
    assert_eq!(
        row[0],
        Value::Bytes(vec![0x1c, 0x47, 0x1c, 0x60, 0x1c, 0x7a, 0x1c, 0x8f])
    );

    // Same case-folded values produce identical sortable bytes.
    let row2 = one_row(
        &engine,
        "SELECT WEIGHT_STRING('aBcD') = WEIGHT_STRING('AbCd')",
    );
    assert_eq!(row2[0], Value::Int(1));

    engine
        .execute("CREATE TABLE ws_czech (ch VARCHAR(2) COLLATE latin2_czech_cs)")
        .unwrap();
    engine.execute("INSERT INTO ws_czech VALUES ('O')").unwrap();
    let czech = one_row(&engine, "SELECT HEX(WEIGHT_STRING(ch)) FROM ws_czech");
    assert_eq!(
        czech[0],
        Value::String("140127014D014D00".into()),
        "WEIGHT_STRING must use the source column collation, not collation_connection",
    );

    let QueryResponse::ResultSet(metadata) =
        engine.execute("SELECT WEIGHT_STRING('test') AS w").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(metadata.columns[0].column_type, ColumnType::Blob);
    assert_eq!(metadata.columns[0].source.length, Some(8));
    assert_ne!(
        metadata.columns[0].source.flags & crate::mysql::protocol::BINARY_FLAG,
        0
    );
    engine
        .execute("CREATE TABLE ws_ctas SELECT WEIGHT_STRING('test') AS w")
        .unwrap();
    let ws_ctas = engine.db.table("test.ws_ctas").unwrap();
    assert_eq!(
        ws_ctas.columns[0].data_type,
        crate::storage::DataTypeKind::VarBinary { len: Some(8) },
    );
}

#[test]
fn yearweek_matches_mysql_calc_week_default_mode() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT \
            YEARWEEK('1987-01-01'), \
            YEARWEEK('1996-01-01'), \
            YEARWEEK('2000-01-01'), \
            YEARWEEK('2025-05-28'), \
            YEARWEEK('2025-12-31'), \
            YEARWEEK('2024-01-01', 1)",
    );
    // YEARWEEK defaults to mode 0 with WEEK_YEAR added internally; expectations come from MySQL,
    // including the FIRST_WEEKDAY bit inversion in week_mode() and documented examples.
    // YEARWEEK('1987-01-01') = 198652）。
    assert_eq!(row[0], Value::Int(198652));
    assert_eq!(row[1], Value::Int(199553));
    assert_eq!(row[2], Value::Int(199952));
    assert_eq!(row[3], Value::Int(202521));
    assert_eq!(row[4], Value::Int(202552));
    // YEARWEEK mode 1 starts Monday; 2024-01-01 is Monday in week 1.
    assert_eq!(row[5], Value::Int(202401));
}

#[test]
fn time_format_renders_specifiers() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT \
            TIME_FORMAT('13:05:09', '%H:%i:%s'), \
            TIME_FORMAT('13:05:09', '%h:%i %p'), \
            TIME_FORMAT('13:05:09', '%r'), \
            TIME_FORMAT('25:10:30', '%T'), \
            TIME_FORMAT('00:00:00', '%h %p')",
    );
    assert_eq!(row[0], Value::String("13:05:09".into()));
    assert_eq!(row[1], Value::String("01:05 PM".into()));
    assert_eq!(row[2], Value::String("01:05:09 PM".into()));
    assert_eq!(row[3], Value::String("25:10:30".into()));
    assert_eq!(row[4], Value::String("12 AM".into()));
}

#[test]
fn json_array_insert_inserts_at_given_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT \
            JSON_ARRAY_INSERT('[1,2,3]', '$[1]', 99), \
            JSON_ARRAY_INSERT('[1,2,3]', '$[0]', \"x\"), \
            JSON_ARRAY_INSERT('[1,2,3]', '$[100]', 5)",
    );
    // JSON-returning functions now yield a real JSON value rendered in
    // MySQL's canonical form (spaces after `,`).
    assert_eq!(row[0].to_display_string(), "[1, 99, 2, 3]");
    assert_eq!(row[1].to_display_string(), "[\"x\", 1, 2, 3]");
    assert_eq!(row[2].to_display_string(), "[1, 2, 3, 5]");
}

#[test]
fn json_merge_alias_behaves_like_merge_preserve() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT \
            JSON_MERGE('[1,2]', '[3,4]'), \
            JSON_MERGE_PRESERVE('[1,2]', '[3,4]')",
    );
    assert_eq!(row[0], row[1]);
    assert_eq!(row[0].to_display_string(), "[1, 2, 3, 4]");
    let row = one_row(
        &engine,
        "SELECT JSON_MERGE_PRESERVE('0', '[1,2]'), JSON_MERGE_PRESERVE('[1,2]', '3')",
    );
    assert_eq!(row[0].to_display_string(), "[0, 1, 2]");
    assert_eq!(row[1].to_display_string(), "[1, 2, 3]");
}

#[test]
fn json_storage_size_and_free_return_consistent_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT JSON_STORAGE_SIZE('[1,2,3]'), JSON_STORAGE_FREE('[1,2,3]')",
    );
    assert!(matches!(row[0], Value::Int(n) if n > 0));
    // JSON_STORAGE_FREE always returns 0 without in-place JSON updates.
    assert_eq!(row[1], Value::Int(0));
}

#[test]
fn json_value_supports_returning_and_response_clauses() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        r#"SELECT
            JSON_VALUE('{"n":"42"}', '$.n' RETURNING UNSIGNED),
            JSON_VALUE('{"n":1}', '$.missing' RETURNING SIGNED DEFAULT 7 ON EMPTY),
            JSON_VALUE('{"n":[]}', '$.n' RETURNING CHAR DEFAULT 'bad' ON ERROR),
            JSON_VALUE('{"n":"abc"}', '$.n' RETURNING UNSIGNED DEFAULT 9 ON ERROR),
            JSON_VALUE('{"n":"invalid"}', '$.n' RETURNING DATE DEFAULT '2000-01-01' ON ERROR),
            JSON_VALUE('{"n":"2026-07-28"}', '$.n' RETURNING DATE)"#,
    );
    assert_eq!(row[0], Value::UInt(42));
    assert_eq!(row[1], Value::Int(7));
    assert_eq!(row[2], Value::String("[]".into()));
    assert_eq!(row[3], Value::UInt(9));
    assert_eq!(row[4].to_display_string(), "2000-01-01");
    assert_eq!(row[5].to_display_string(), "2026-07-28");

    let row = one_row(
        &engine,
        r#"SELECT
          JSON_VALUE('{"n":[1]}', '$.n'),
          JSON_TYPE(JSON_VALUE(
            JSON_OBJECT('d', CAST('2026-08-20' AS DATE)), '$.d'
            RETURNING JSON NULL ON EMPTY NULL ON ERROR))"#,
    );
    assert_eq!(row[0], Value::String("[1]".into()));
    assert_eq!(row[1].to_display_string(), "DATE");

    let err = engine
        .execute(r#"SELECT JSON_VALUE('{"n":1}', '$.missing' ERROR ON EMPTY)"#)
        .unwrap_err();
    assert_eq!(err.code(), 3966);
}

#[test]
fn json_table_expands_rows_and_nested_paths() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let before = native_batch_hits();
    let QueryResponse::ResultSet(result) = engine
        .execute(
            r#"SELECT jt.ord, jt.id, jt.name, jt.has_tags, jt.tag, jt.missing
               FROM JSON_TABLE(
                 '[{"id":1,"name":"a","tags":["x","y"]},{"id":2,"name":"b"}]',
                 '$[*]' COLUMNS (
                   ord FOR ORDINALITY,
                   id INT PATH '$.id',
                   name VARCHAR(10) PATH '$.name',
                   has_tags INT EXISTS PATH '$.tags',
                   missing INT PATH '$.missing' DEFAULT '7' ON EMPTY,
                   NESTED PATH '$.tags[*]' COLUMNS (
                     tag VARCHAR(10) PATH '$'
                   )
                 )
               ) AS jt
               ORDER BY jt.ord, jt.tag"#,
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 3);
    assert_eq!(
        result.rows[0],
        vec![
            Value::UInt(1),
            Value::Int(1),
            Value::String("a".into()),
            Value::Int(1),
            Value::String("x".into()),
            Value::Int(7),
        ]
    );
    assert_eq!(result.rows[1][4], Value::String("y".into()));
    assert_eq!(
        result.rows[2],
        vec![
            Value::UInt(2),
            Value::Int(2),
            Value::String("b".into()),
            Value::Int(0),
            Value::Null,
            Value::Int(7),
        ]
    );
    assert!(
        native_batch_hits() > before,
        "JSON_TABLE did not use its native BatchOperator source"
    );
}

#[test]
fn json_table_preserves_unsigned_and_jsonb_scalar_types_and_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = engine
        .execute(
            r#"SELECT jt.u, JSON_TYPE(jt.d)
               FROM JSON_TABLE(
                 JSON_OBJECT('u', 18446744073709551615, 'd', CAST('2026-08-20' AS DATE)),
                 '$' COLUMNS (
                   u BIGINT UNSIGNED PATH '$.u' ERROR ON ERROR,
                   d JSON PATH '$.d' ERROR ON ERROR
                 )
               ) AS jt"#,
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::UInt(u64::MAX));
    assert_ne!(
        result.columns[0].source.flags & crate::mysql::protocol::UNSIGNED_FLAG,
        0
    );
    assert_eq!(result.rows[0][1].to_display_string(), "DATE");

    let err = engine
        .execute(
            r#"SELECT jt.v FROM JSON_TABLE('[1]', '$' COLUMNS (
                 v INT PATH '$' ERROR ON ERROR
               )) AS jt"#,
        )
        .unwrap_err();
    assert_eq!(err.code(), 3666);

    let err = engine
        .execute(
            r#"SELECT jt.v FROM JSON_TABLE('256', '$' COLUMNS (
                 v TINYINT UNSIGNED PATH '$' ERROR ON ERROR
               )) AS jt"#,
        )
        .unwrap_err();
    assert_eq!(err.code(), 3669);

    let row = one_row(
        &engine,
        r#"SELECT jt.d FROM JSON_TABLE('"not-a-date"', '$' COLUMNS (
             d DATE PATH '$' NULL ON ERROR
           )) AS jt"#,
    );
    assert_eq!(row[0], Value::Null);
}

#[test]
fn json_table_sibling_nested_paths_are_union_not_cross_product() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = engine
        .execute(
            r#"SELECT jt.a, jt.b
               FROM JSON_TABLE(
                 '{"a":[1,2],"b":[3,4]}',
                 '$' COLUMNS (
                   NESTED PATH '$.a[*]' COLUMNS (a INT PATH '$'),
                   NESTED PATH '$.b[*]' COLUMNS (b INT PATH '$')
                 )
               ) AS jt"#,
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Null],
            vec![Value::Int(2), Value::Null],
            vec![Value::Null, Value::Int(3)],
            vec![Value::Null, Value::Int(4)],
        ]
    );
}

#[test]
fn json_table_is_implicitly_lateral_to_left_tables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE docs (id INT PRIMARY KEY, payload JSON)")
        .unwrap();
    engine
        .execute(
            r#"INSERT INTO docs VALUES
               (1, '{"items":[10,20]}'),
               (2, '{"items":[30]}')"#,
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute(
            r#"SELECT d.id, jt.ord, jt.item
               FROM docs AS d,
                    JSON_TABLE(d.payload, '$.items[*]' COLUMNS (
                      ord FOR ORDINALITY,
                      item INT PATH '$'
                    )) AS jt
               ORDER BY d.id, jt.ord"#,
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::UInt(1), Value::Int(10)],
            vec![Value::Int(1), Value::UInt(2), Value::Int(20)],
            vec![Value::Int(2), Value::UInt(1), Value::Int(30)],
        ]
    );
}

#[test]
fn json_table_column_names_are_case_insensitive() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine
        .execute(
            r#"SELECT *
               FROM JSON_TABLE(
                 '{"a":1}', '$' COLUMNS (
                   value INT PATH '$.a',
                   VALUE INT PATH '$.a'
                 )
               ) AS jt"#,
        )
        .unwrap_err();
    assert_eq!(err.code(), 1060);
}

#[test]
fn interval_returns_position_in_sorted_list() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT \
            INTERVAL(23, 1, 15, 17, 30, 44, 200), \
            INTERVAL(10, 1, 10, 100, 1000), \
            INTERVAL(-1, 1, 10), \
            INTERVAL(NULL, 1, 2)",
    );
    assert_eq!(row[0], Value::Int(3));
    assert_eq!(row[1], Value::Int(2));
    assert_eq!(row[2], Value::Int(0));
    assert_eq!(row[3], Value::Int(-1));
}

#[test]
fn any_value_returns_passthrough() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // ANY_VALUE is scalar identity. GROUP BY exemptions depend on planning;
    // test only scalar/nonaggregate passthrough here.
    let row = one_row(
        &engine,
        "SELECT ANY_VALUE(42), ANY_VALUE('foo'), ANY_VALUE(NULL)",
    );
    assert_eq!(row[0], Value::Int(42));
    assert_eq!(row[1], Value::String("foo".into()));
    assert_eq!(row[2], Value::Null);
}

#[test]
fn uuid_short_is_monotonic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(&engine, "SELECT UUID_SHORT(), UUID_SHORT()");
    let (Value::UInt(a), Value::UInt(b)) = (&row[0], &row[1]) else {
        panic!("expected UInt UUID_SHORT values, got {:?}", row);
    };
    assert!(*b > *a, "UUID_SHORT must increase monotonically: {a} < {b}");
}

#[test]
fn grouping_requires_group_by_context() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        engine.execute("SELECT GROUPING(1)").unwrap_err().code(),
        1111
    );
}

#[test]
fn name_const_returns_value_arg() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(&engine, "SELECT NAME_CONST('x', 42)");
    assert_eq!(row[0], Value::Int(42));
    for sql in [
        "SELECT NAME_CONST('x', UPPER('value'))",
        "SELECT NAME_CONST('x', SQRT(4))",
        "SELECT NAME_CONST('x', -(1 OR 2))",
    ] {
        let err = engine.execute(sql).unwrap_err();
        assert_eq!(err.code(), 1210, "{sql}: {err:?}");
    }
}

#[test]
fn system_user_matches_user() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(&engine, "SELECT USER(), SYSTEM_USER(), SESSION_USER()");
    assert_eq!(row[0], row[1]);
    assert_eq!(row[1], row[2]);
}

#[test]
fn format_bytes_renders_powers_of_1024() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT \
            FORMAT_BYTES(0), \
            FORMAT_BYTES(1023), \
            FORMAT_BYTES(1024), \
            FORMAT_BYTES(1024 * 1024), \
            FORMAT_BYTES(1024 * 1024 * 1024)",
    );
    assert_eq!(row[0], Value::String("0 bytes".into()));
    assert_eq!(row[1], Value::String("1023 bytes".into()));
    assert_eq!(row[2], Value::String("1.00 KiB".into()));
    assert_eq!(row[3], Value::String("1.00 MiB".into()));
    assert_eq!(row[4], Value::String("1.00 GiB".into()));
}

#[test]
fn format_pico_time_renders_si_units() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT \
            FORMAT_PICO_TIME(0), \
            FORMAT_PICO_TIME(500), \
            FORMAT_PICO_TIME(1500), \
            FORMAT_PICO_TIME(2500000), \
            FORMAT_PICO_TIME(1500000000000)",
    );
    assert_eq!(row[0], Value::String("0 ps".into()));
    assert_eq!(row[1], Value::String("500 ps".into()));
    assert_eq!(row[2], Value::String("1.50 ns".into()));
    assert_eq!(row[3], Value::String("2.50 us".into()));
    assert_eq!(row[4], Value::String("1.50 s".into()));
}

#[test]
fn statement_digest_normalizes_literals() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT \
            STATEMENT_DIGEST_TEXT('SELECT * FROM t WHERE id = 7'), \
            STATEMENT_DIGEST_TEXT('SELECT * FROM t WHERE id = 99'), \
            STATEMENT_DIGEST('SELECT 1') = STATEMENT_DIGEST('SELECT 2')",
    );
    assert_eq!(row[0], row[1]);
    // Different literals produce the same normalized digest.
    assert_eq!(row[2], Value::Int(1));
}

#[test]
fn digest_functions_hash_binary_values_as_octets() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT \
         SHA2(X'FF', 224), \
         SHA2(X'BD', 256), \
         SHA1(X'00FF'), \
         MD5(X'00FF')",
    );
    assert_eq!(
        row[0],
        Value::String("e33f9d75e6ae1369dbabf81b96b4591ae46bba30b591a6b6c62542b5".into())
    );
    assert_eq!(
        row[1],
        Value::String("68325720aabd7c82f30f554b313d0570c95accbb7dc4b5aae11204c08ffe732b".into())
    );
    assert_eq!(
        row[2],
        Value::String("aa3e5dcdd77b153f2e59bd0d8794fde33cb4e486".into())
    );
    assert_eq!(
        row[3],
        Value::String("d07d34efac6328007ad67c7e0a985e00".into())
    );
}

#[test]
fn validate_password_strength_grades_passwords() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT \
            VALIDATE_PASSWORD_STRENGTH(''), \
            VALIDATE_PASSWORD_STRENGTH('abc'), \
            VALIDATE_PASSWORD_STRENGTH('abcdef'), \
            VALIDATE_PASSWORD_STRENGTH('abcdef12'), \
            VALIDATE_PASSWORD_STRENGTH('Abcdef12'), \
            VALIDATE_PASSWORD_STRENGTH('Abcdef12!')",
    );
    assert_eq!(row[0], Value::Int(0));
    assert_eq!(row[1], Value::Int(0));
    assert_eq!(row[2], Value::Int(25));
    assert_eq!(row[3], Value::Int(75));
    assert_eq!(row[4], Value::Int(100));
    assert_eq!(row[5], Value::Int(100));
}

#[test]
fn get_lock_release_lock_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Use a unique lock name to avoid concurrent-test collisions.
    let lock_name = format!("mydb_test_lock_{}", std::process::id());

    // IS_FREE_LOCK returns 1 when unheld.
    let row = one_row(&engine, &format!("SELECT IS_FREE_LOCK('{}')", lock_name));
    assert_eq!(row[0], Value::Int(1));

    // GET_LOCK acquires successfully.
    let row = one_row(&engine, &format!("SELECT GET_LOCK('{}', 0)", lock_name));
    assert_eq!(row[0], Value::Int(1));

    // IS_FREE_LOCK now returns 0.
    let row = one_row(&engine, &format!("SELECT IS_FREE_LOCK('{}')", lock_name));
    assert_eq!(row[0], Value::Int(0));

    // IS_USED_LOCK returns the non-NULL current connection_id.
    let row = one_row(&engine, &format!("SELECT IS_USED_LOCK('{}')", lock_name));
    assert!(matches!(row[0], Value::UInt(_)));

    // RELEASE_LOCK returns 1 on successful release.
    let row = one_row(&engine, &format!("SELECT RELEASE_LOCK('{}')", lock_name));
    assert_eq!(row[0], Value::Int(1));

    // A repeated RELEASE_LOCK returns NULL because the lock no longer exists.
    let row = one_row(&engine, &format!("SELECT RELEASE_LOCK('{}')", lock_name));
    assert_eq!(row[0], Value::Null);
}

#[test]
fn release_all_locks_returns_count() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Ensure a clean starting state; repeated in-process tests have distinct connection IDs and remain independent.
    let suffix = std::process::id();
    let a = format!("mydb_release_all_a_{}", suffix);
    let b = format!("mydb_release_all_b_{}", suffix);

    let row = one_row(&engine, &format!("SELECT GET_LOCK('{}', 0)", a));
    assert_eq!(row[0], Value::Int(1));
    let row = one_row(&engine, &format!("SELECT GET_LOCK('{}', 0)", b));
    assert_eq!(row[0], Value::Int(1));

    let row = one_row(&engine, "SELECT RELEASE_ALL_LOCKS()");
    // One connection holding two locks must release two.
    assert_eq!(row[0], Value::UInt(2));

    // Releasing again returns 0.
    let row = one_row(&engine, "SELECT RELEASE_ALL_LOCKS()");
    assert_eq!(row[0], Value::UInt(0));
}

#[test]
fn row_count_and_found_rows_track_previous_statement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    <StorageQueryEngine as QueryEngine>::execute(
        &engine,
        &mut session,
        "CREATE TABLE rc_t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    <StorageQueryEngine as QueryEngine>::execute(
        &engine,
        &mut session,
        "INSERT INTO rc_t VALUES (1,10),(2,20),(3,30)",
    )
    .unwrap();
    let QueryResponse::ResultSet(row_count) =
        <StorageQueryEngine as QueryEngine>::execute(&engine, &mut session, "SELECT ROW_COUNT()")
            .unwrap()
    else {
        panic!("expected ROW_COUNT result set");
    };
    assert_eq!(row_count.rows[0][0], Value::Int(3));

    engine
        .execute_with_session(&mut session, "SELECT id FROM rc_t WHERE id <= 2")
        .unwrap();
    let QueryResponse::ResultSet(found_rows) =
        <StorageQueryEngine as QueryEngine>::execute(&engine, &mut session, "SELECT FOUND_ROWS()")
            .unwrap()
    else {
        panic!("expected FOUND_ROWS result set");
    };
    assert_eq!(found_rows.rows[0][0], Value::UInt(2));
}

#[test]
fn temporal_precision_and_mysql_date_format_specifiers() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT \
            LENGTH(NOW(6)), \
            LENGTH(CURTIME(3)), \
            DATE_FORMAT('2026-05-07 01:02:03.123456', '%a %b %D %f %r %T %j %w')",
    );
    assert_eq!(row[0], Value::Int(26));
    assert_eq!(row[1], Value::Int(12));
    assert_eq!(
        row[2],
        Value::String("Thu May 7th 123456 01:02:03 AM 01:02:03 127 4".into())
    );
}

#[test]
fn date_add_supports_mysql_composite_interval_units() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT \
            DATE_ADD('2026-01-01 00:00:00', INTERVAL '1 02:03:04' DAY_SECOND), \
            DATE_ADD('2026-01-01', INTERVAL '1-2' YEAR_MONTH), \
            DATE_SUB('2026-01-02 03:04:05', INTERVAL '1:02' HOUR_MINUTE)",
    );
    assert_eq!(row[0], Value::String("2026-01-02 02:03:04".into()));
    assert_eq!(row[1], Value::String("2027-03-01".into()));
    assert_eq!(row[2], Value::String("2026-01-02 02:02:05".into()));
}

#[test]
fn uuid_uses_mysql_v1_shape_and_convert_using_works() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        "SELECT UUID(), CONVERT('abc' USING binary), CONVERT('abc' USING utf8mb4), CHARSET(CONVERT('abc' USING binary)), COLLATION(CONVERT('abc' USING binary))",
    );
    let Value::String(uuid) = &row[0] else {
        panic!("expected UUID string");
    };
    assert_eq!(uuid.len(), 36);
    assert_eq!(&uuid[14..15], "1", "UUID() should be version 1");
    assert_eq!(row[1], Value::Bytes(b"abc".to_vec()));
    assert_eq!(row[2], Value::String("abc".into()));
    assert_eq!(row[3], Value::String("binary".into()));
    assert_eq!(row[4], Value::String("binary".into()));
}

#[test]
fn json_path_supports_escaped_keys_and_last_array_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        r#"SELECT JSON_EXTRACT('{"a.b":[10,20,30]}', '$."a.b"[last]'), JSON_REMOVE('[1,2,3]', '$[last]')"#,
    );
    assert_eq!(row[0].to_display_string(), "30");
    assert_eq!(row[1].to_display_string(), "[1, 2]");
}

#[test]
fn json_member_of_matches_mysql_array_membership_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let row = one_row(
        &engine,
        r#"SELECT
            2 MEMBER OF('[1,2,3]'),
            'x' MEMBER OF(JSON_ARRAY('x', 'y')),
            4 MEMBER OF('[1,2,3]'),
            1 MEMBER OF('{"a":1}'),
            NULL MEMBER OF('[1]'),
            NOT (4 MEMBER OF('[1,2,3]'))"#,
    );
    assert_eq!(row[0], Value::Int(1));
    assert_eq!(row[1], Value::Int(1));
    assert_eq!(row[2], Value::Int(0));
    assert_eq!(row[3], Value::Int(0));
    assert_eq!(row[4], Value::Null);
    assert_eq!(row[5], Value::Int(1));

    let err = engine
        .execute("SELECT 1 MEMBER OF('not json')")
        .unwrap_err();
    assert_eq!(err.code(), 3141);
    assert_eq!(err.sql_state(), "22032");
}
