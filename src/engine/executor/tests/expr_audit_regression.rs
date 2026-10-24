//! Regressions for doc_old/functions-expressions-bugs-audit.md.
use super::*;

fn check(sql: &str, expected: &[&str]) {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("{sql}")
    };
    let actual: Vec<_> = result.rows[0]
        .iter()
        .map(crate::storage::value_to_string)
        .collect();
    assert_eq!(actual, expected, "{sql}");
}

#[test]
fn audit_exact_division() {
    check(
        "SELECT 9223372036854775807 / 3, 18446744073709551615 / 2, 18446744073709551615 DIV 1.0",
        &[
            "3074457345618258602.3333",
            "9223372036854775807.5000",
            "18446744073709551615",
        ],
    );
}

#[test]
fn audit_wide_unary() {
    check("SELECT ABS(CAST('-1234567890123456789012345678901234567890.25' AS DECIMAL(65,2))), CEIL(CAST('-1234567890123456789012345678901234567890.25' AS DECIMAL(65,2))), FLOOR(CAST('-1234567890123456789012345678901234567890.25' AS DECIMAL(65,2)))",
        &["1234567890123456789012345678901234567890.25", "-1234567890123456789012345678901234567890", "-1234567890123456789012345678901234567891"]);
}

#[test]
fn audit_substring_nonoverlap() {
    check(
        "SELECT SUBSTRING_INDEX('aaa','aa',-1), SUBSTRING_INDEX('aaaaaaaaa1','aa',-4)",
        &["a", "aaaaaaa1"],
    );
}

#[test]
fn audit_ord_binary_and_uuid_rows() {
    for mode in [
        crate::engine::executor::vec::config::VecMode::Off,
        crate::engine::executor::vec::config::VecMode::On,
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        let mut session = SessionContext::default();
        let _scope = crate::engine::executor::vec::config::enter_mode(mode);
        engine.execute("CREATE TABLE t(b VARBINARY(20))").unwrap();
        engine
            .execute("INSERT INTO t VALUES (x'E69CAC'), (x'E69CAC'), (x'E69CAC')")
            .unwrap();
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(&mut session, "SELECT ORD(b), UUID_SHORT() FROM t")
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(result.rows.len(), 3);
        for row in &result.rows {
            assert_eq!(row[0], Value::Int(230));
        }
        assert_ne!(result.rows[0][1], result.rows[1][1]);
        assert_ne!(result.rows[1][1], result.rows[2][1]);
    }
}

#[test]
fn audit_regex_multiline_replace() {
    check(
        r"SELECT HEX(REGEXP_REPLACE('a\na','^a$','X',1,0,'m'))",
        &["580A58"],
    );
}

#[test]
fn audit_json_value_wildcards_boolean() {
    check(
        r#"SELECT JSON_VALUE('["abc"]','$[*]'), JSON_VALUE('["abc","def"]','$[*]' DEFAULT 'oops' ON ERROR), JSON_VALUE('{"a":true}','$.a')"#,
        &["abc", "oops", "true"],
    );
}

#[test]
fn audit_unix_fraction() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    // Fractional Unix timestamps must not depend on the host's local zone.
    for (zone, expected) in [
        ("+00:00", "32503679999.999999"),
        ("+08:00", "32503651199.999999"),
    ] {
        engine
            .execute_with_session(&mut session, &format!("SET time_zone='{zone}'"))
            .unwrap();
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(
                &mut session,
                "SELECT UNIX_TIMESTAMP('2999-12-31 23:59:59.999999')",
            )
            .unwrap()
        else {
            panic!("expected timestamp result");
        };
        assert_eq!(result.rows[0][0].to_display_string(), expected, "{zone}");
    }
}

#[test]
fn audit_string_parameter_rounding() {
    check("SELECT ELT(1.5,'a','b'), MAKE_SET(1.5,'a','b'), EXPORT_SET(1.5,'a','b',',',2.5), LENGTH(EXPORT_SET(1,'a','b','',-1)), SOUNDEX('£abc')", &["b", "b", "b,a,b", "64", "A120"]);
}

#[test]
fn audit_interval_right_alignment() {
    check("SELECT DATE_ADD('2020-01-01', INTERVAL '1:1' DAY_SECOND), DATE_ADD('2020-01-01', INTERVAL '12' HOUR_MINUTE), DATE_ADD('2020-01-01 00:00:00', INTERVAL '100' SECOND_MICROSECOND), DATE_ADD('2020-01-01', INTERVAL '121212' DAY_SECOND)", &["2020-01-01 00:01:01", "2020-01-01 00:12:00", "2020-01-01 00:00:00.100000", "2020-01-02 09:40:12"]);
}

#[test]
fn audit_temporal_bounds_and_parsing() {
    check("SELECT TIMEDIFF('2020-01-01','12:00:00'), ADDTIME('9999-12-31 23:59:59','1:1:1'), STR_TO_DATE('99-12-31','%Y-%m-%d'), STR_TO_DATE('2020 1 0','%Y %U %w'), CAST('-00:00:00.5' AS TIME(0))", &["NULL", "NULL", "1999-12-31", "2020-01-05", "-00:00:01"]);
}

#[test]
fn audit_json_search_scope_and_dedup() {
    check(
        r#"SELECT CAST(JSON_SEARCH('["abc", {"x":"abc"}]','all','abc',NULL,'$','$[1]') AS CHAR), CAST(JSON_SEARCH('["abc", {"x":"abc"}]','all','abc',NULL,'$[*]') AS CHAR), JSON_VALUE('{"y":99}','$.y' RETURNING YEAR)"#,
        &[r#"["$[0]", "$[1].x"]"#, r#"["$[0]", "$[1].x"]"#, "NULL"],
    );
}

#[test]
fn audit_collation_and_binary_like() {
    check("SELECT REGEXP_LIKE('abc','ABC' COLLATE utf8mb4_bin), 'abc' LIKE 'ABC' COLLATE utf8mb4_bin, 0xC341 LIKE 0xFF41", &["0", "0", "0"]);
}

#[test]
fn audit_error_codes() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for (sql, code) in [
        ("SELECT POW(-1,0.5)", 1690),
        ("SELECT RANDOM_BYTES(-1)", 1690),
        ("SELECT GET_LOCK('',0)", 3057),
        ("SELECT REGEXP_SUBSTR('abc','b',0)", 1583),
        ("SELECT REGEXP_INSTR('abc','b',1,1,2)", 1210),
        ("SELECT JSON_DEPTH(1)", 3146),
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), code, "{sql}");
    }
}

#[test]
fn audit_explicit_collation_conflict() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let sql = "SELECT 'A' COLLATE utf8mb4_bin = 'a' COLLATE utf8mb4_general_ci";
    assert_eq!(engine.execute(sql).unwrap_err().code(), 1267);
}

#[test]
fn audit_column_wide_and_binary() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE audit_cols (v VARCHAR(10) COLLATE utf8mb4_general_ci, d DECIMAL(25,0))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO audit_cols VALUES ('e',9223372036854775809),('E',9223372036854775809)",
        )
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT v = BINARY 'E', CEIL(d), FLOOR(d) FROM audit_cols ORDER BY v = BINARY 'E'")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(result.rows[0][0], Value::Int(0));
    for row in &result.rows {
        assert_eq!(
            crate::storage::value_to_string(&row[1]),
            "9223372036854775809"
        );
    }
}

#[test]
fn audit_rollup_non_group_order() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine.execute("CREATE TABLE rt(g INT, a INT)").unwrap();
    engine
        .execute("INSERT INTO rt VALUES (1,1),(1,2),(2,3)")
        .unwrap();
    let sql = "SELECT g, SUM(a) FROM rt GROUP BY g WITH ROLLUP ORDER BY a";
    engine
        .execute_with_session(&mut session, "SET sql_mode='ONLY_FULL_GROUP_BY'")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut session, sql)
            .unwrap_err()
            .code(),
        1055
    );
    engine
        .execute_with_session(&mut session, "SET sql_mode=''")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine.execute_with_session(&mut session, sql).unwrap()
    else {
        panic!()
    };
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn audit_unselected_database() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.current_database.clear();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT DATABASE()")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(result.rows[0][0], Value::Null);
    assert_eq!(
        engine
            .execute_with_session(&mut session, "CREATE TABLE x(i INT)")
            .unwrap_err()
            .code(),
        1046
    );
    engine
        .execute_with_session(&mut session, "CREATE TABLE test.x(i INT)")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut session, "SELECT * FROM x")
            .unwrap_err()
            .code(),
        1046
    );
    engine
        .execute_with_session(&mut session, "SELECT * FROM test.x")
        .unwrap();
}

#[test]
fn audit_uca_weights() {
    check("SELECT HEX(WEIGHT_STRING('a')), HEX(WEIGHT_STRING('a' COLLATE utf8mb4_bin)), HEX(WEIGHT_STRING('ab')), WEIGHT_STRING('é') = WEIGHT_STRING('e')", &["1C47","000061","1C471C60","1"]);
}

#[test]
fn audit_variance_overflow() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE av(v DOUBLE)").unwrap();
    engine
        .execute("INSERT INTO av VALUES (1e308),(-1e308)")
        .unwrap();
    assert_eq!(
        engine
            .execute("SELECT VAR_POP(v) FROM av")
            .unwrap_err()
            .code(),
        1690
    );
}

#[test]
fn audit_zero_dates_obey_explicit_mode() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode='NO_ZERO_DATE'")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut session,
            "SELECT QUARTER('0000-00-00'), DATE_FORMAT('0000-00-00','%d'), YEAR('0000-00-00')",
        )
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(result.rows[0], vec![Value::Null; 3]);
    engine
        .execute_with_session(&mut session, "SET sql_mode=''")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT YEAR('0000-00-00')")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(result.rows[0][0], Value::Int(0));
}

#[test]
fn audit_benchmark_count_not_silently_capped() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET @n=0")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SELECT BENCHMARK(1000001,@n:=@n+1)")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT @n")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        crate::storage::value_to_string(&result.rows[0][0]),
        "1000001"
    );
}

#[test]
fn audit_regex_column_collations_and_flags() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE rx(ci VARCHAR(10), cs VARCHAR(10) COLLATE utf8mb4_bin)")
        .unwrap();
    engine
        .execute("INSERT INTO rx VALUES ('abc','abc')")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("SELECT REGEXP_LIKE(ci,'ABC'), REGEXP_LIKE(cs,'ABC'), REGEXP_LIKE(cs,'ABC','i'), REGEXP_REPLACE(cs,'ABC','x') FROM rx").unwrap() else { panic!() };
    assert_eq!(
        result.rows[0],
        vec![
            Value::Int(1),
            Value::Int(0),
            Value::Int(1),
            Value::String("abc".into())
        ]
    );
}

#[test]
fn audit_compact_fraction_rounding() {
    check("SELECT CAST('20200101000000.9999995' AS DATETIME(6)), CAST('2020-01-01 00:00:00.9999995' AS DATETIME(6))", &["2020-01-01 00:00:01", "2020-01-01 00:00:01"]);
}

#[test]
fn audit_regex_escaped_end_anchor() {
    check(r"SELECT REGEXP_REPLACE('a$b','^a\\$','X')", &["Xb"]);
}

#[test]
fn audit_uca_base_does_not_apply_language_contractions() {
    check(
        "SELECT HEX(WEIGHT_STRING('l·l')), HEX(WEIGHT_STRING('й')), HEX(WEIGHT_STRING('𗀀'))",
        &["1D77028B1D77", "2080", "FB008000"],
    );
}

#[test]
fn audit_div_is_independent_of_division_display_scale() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for scale in [0, 30] {
        engine
            .execute_with_session(
                &mut session,
                &format!("SET div_precision_increment={scale}"),
            )
            .unwrap();
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(
                &mut session,
                "SELECT 1 DIV 0.6, 18446744073709551615 DIV 1.0",
            )
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(result.rows[0], vec![Value::Int(1), Value::UInt(u64::MAX)]);
    }
}

#[test]
fn audit_temporal_column_precision_metadata() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE tf(d DATETIME(6), s VARCHAR(40))")
        .unwrap();
    engine
        .execute("INSERT INTO tf VALUES ('2020-01-01 12:34:56.123456','12:34:56.123456')")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT UNIX_TIMESTAMP(d), TIME(d), TIME(s) FROM tf")
        .unwrap()
    else {
        panic!()
    };
    assert!(matches!(
        result.columns[0].column_type,
        ColumnType::Decimal { scale: 6, .. }
    ));
    assert_eq!(result.columns[1].source.decimals, Some(6));
    assert_eq!(result.columns[2].source.decimals, Some(6));
}

#[test]
fn audit_null_lock_timeout_does_not_wait() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    let mut owner = SessionContext::default();
    owner.connection_id = 9001;
    let mut contender = SessionContext::default();
    contender.connection_id = 9002;
    engine
        .execute_with_session(&mut owner, "SELECT GET_LOCK('expr_null_timeout',0)")
        .unwrap();
    let other = engine.clone();
    let release = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(100));
        other
            .execute_with_session(&mut owner, "SELECT RELEASE_LOCK('expr_null_timeout')")
            .unwrap();
    });
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut contender, "SELECT GET_LOCK('expr_null_timeout',NULL)")
        .unwrap()
    else {
        panic!()
    };
    release.join().unwrap();
    engine
        .execute_with_session(&mut contender, "SELECT RELEASE_ALL_LOCKS()")
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int(0));
}

#[test]
fn audit_unsupported_uca_tailoring_is_explicit() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        engine
            .execute("SELECT WEIGHT_STRING('a' COLLATE utf8mb4_unicode_ci)")
            .unwrap_err()
            .code(),
        1235
    );
}

#[test]
fn audit_binary_like_wildcards_count_bytes() {
    check("SELECT 0xE69CAC LIKE '_', 0xE69CAC LIKE '___', 0xFF41 LIKE '_A', 0xFF41 LIKE '%B', 0xFF41 LIKE '%A'", &["0","1","1","0","1"]);
}

#[test]
fn audit_binary_like_terminal_escape() {
    check("SELECT 0x25 LIKE '%' ESCAPE '%', 0x41 LIKE '%' ESCAPE '%', 0x5F LIKE '_' ESCAPE '_', 0x41 LIKE '_' ESCAPE '_'", &["1","0","1","0"]);
}

#[test]
fn audit_computed_binary_like_column() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE bl(v VARCHAR(10))").unwrap();
    engine.execute("INSERT INTO bl VALUES ('a')").unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT CAST(v AS BINARY) LIKE 'A' FROM bl")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(result.rows[0][0], Value::Int(0));
}
