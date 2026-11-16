use super::*;

fn check(sql: &str, expected: &[&str]) {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("{sql}")
    };
    assert_eq!(result.rows.len(), 1, "{sql}");
    let actual: Vec<_> = result.rows[0]
        .iter()
        .map(Value::to_display_string)
        .collect();
    assert_eq!(actual, expected, "{sql}");
}

macro_rules! expr_case {
    ($name:ident, $sql:expr, $expected:expr) => {
        #[test]
        fn $name() {
            check($sql, $expected);
        }
    };
}
expr_case!(
    audit1106_time_literal,
    "SELECT TIME'10:00:00.5', SECOND(TIME'10:00:00.5')",
    &["10:00:00.5", "0"]
);
expr_case!(audit1106_base64, "SELECT HEX(FROM_BASE64('QR==')), HEX(FROM_BASE64('QUJ=')), FROM_BASE64('Q==='), HEX(FROM_BASE64(' Q R== '))", &["41", "4142", "NULL", "41"]);
expr_case!(
    audit1106_make_set,
    "SELECT HEX(MAKE_SET(3,x'FF',x'FE')), MAKE_SET(5,'a',NULL,'c')",
    &["FF2CFE", "a,c"]
);
expr_case!(audit1106_json_before_head, "SELECT JSON_SET('[1,2,3]','$[last-7]',9), JSON_ARRAY_INSERT('[1,2,3]','$[last-7]',9), JSON_INSERT('[1,2]','$[last-7]',9), JSON_REPLACE('[1,2]','$[last-7]',9)", &["[9, 1, 2, 3]", "[9, 1, 2, 3]", "[9, 1, 2]", "[1, 2]"]);
expr_case!(audit1106_uuid_null_flag, "SELECT HEX(UUID_TO_BIN('6ccd780c-baba-1026-9564-5b8c656024db',NULL)), BIN_TO_UUID(x'6CCD780CBABA102695645B8C656024DB',NULL)", &["6CCD780CBABA102695645B8C656024DB", "6ccd780c-baba-1026-9564-5b8c656024db"]);
expr_case!(
    audit1106_format_unknown,
    "SELECT DATE_FORMAT('2026-01-01','%q'), TIME_FORMAT('25:20:30','%r')",
    &["q", "01:20:30 AM"]
);
expr_case!(audit1106_str_to_date, "SELECT STR_TO_DATE('2026-366','%Y-%j'), STR_TO_DATE('PM','%p'), STR_TO_DATE('13 PM','%H %p'), STR_TO_DATE('2026-01-0','%x-%v-%w')", &["2027-01-01", "NULL", "NULL", "2026-01-04"]);
expr_case!(
    audit1106_date_time_only,
    "SELECT DATE('10:20:30')",
    &["NULL"]
);
expr_case!(
    audit1106_negative_extract,
    "SELECT EXTRACT(HOUR FROM '-10:20:30'), EXTRACT(MINUTE FROM '-10:20:30')",
    &["-10", "-20"]
);
expr_case!(audit1106_mask_margins, "SELECT MASK_INNER('abcdef',-1,1), MASK_OUTER('abcdef',1,-1), MASK_INNER('abcdef',9223372036854775807,1)", &["NULL", "NULL", "abcdef"]);
expr_case!(
    audit1106_empty_regex,
    "SELECT REGEXP_INSTR('','a',2), REGEXP_LIKE('Ａ','[[:xdigit:]]')",
    &["0", "1"]
);
expr_case!(
    audit1106_thread_unknown,
    "SELECT PS_THREAD_ID(999999999)",
    &["NULL"]
);

#[test]
fn audit1106_error_contracts() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for (sql, code) in [
        ("SELECT PERIOD_ADD(0,1)", 1210),
        ("SELECT SLEEP(NULL)", 1210),
        ("SELECT SLEEP(-1)", 1210),
        ("SELECT GREATEST(1)", 1582),
        ("SELECT _latin1'a' COLLATE utf8mb4_bin", 1253),
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), code, "{sql}");
    }
}

expr_case!(audit1106_full_regex_folding,
    "SELECT REGEXP_LIKE('straße','strasse','i'), REGEXP_LIKE('strasse','straße','i'), REGEXP_LIKE('ﬁ','fi','i'), REGEXP_LIKE('İ','i̇','i'), REGEXP_LIKE('ß','s{2}','i'), REGEXP_LIKE('ss','[ß]','i'), REGEXP_SUBSTR('xstraßeY','(strasse)',1,1,'i'), REGEXP_INSTR('xstraßeY','strasse',1,1,1,'i')",
    &["1", "1", "1", "1", "0", "0", "straße", "8"]);
expr_case!(audit1106_field_first_collation,
    "SELECT FIELD('a',_binary'A',_binary'a'), FIELD(_binary'a','A','a'), FIELD('a' COLLATE utf8mb4_bin,'A' COLLATE utf8mb4_0900_ai_ci)",
    &["1", "2", "0"]);
expr_case!(audit1106_gtid_empty_contributions,
    "SELECT GTID_SUBSET('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:3-1',''), GTID_SUBTRACT('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa','')", &["1", ""]);

#[test]
fn audit1106_charsets_and_filter_paths() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE oct(s VARCHAR(10) CHARACTER SET latin1)")
        .unwrap();
    engine.execute("INSERT INTO oct VALUES ('é')").unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("SELECT CRC32(s),TO_BASE64(s),ASCII(s),ORD(s),HEX(COMPRESS(s)),HEX(CHAR(0xE9 USING latin1)),CHARSET(CHAR(0xE9 USING latin1)) FROM oct").unwrap().materialize().unwrap() else { panic!() };
    assert_eq!(
        result.rows[0]
            .iter()
            .map(Value::to_display_string)
            .collect::<Vec<_>>(),
        [
            "198489425",
            "6Q==",
            "233",
            "233",
            "01000000789C7B090000EA00EA",
            "E9",
            "latin1"
        ]
    );
    engine
        .execute("CREATE TABLE pred(k INT,s VARCHAR(10) COLLATE utf8mb4_0900_ai_ci)")
        .unwrap();
    engine
        .execute("INSERT INTO pred VALUES(42,'ABC'),(43,'abc')")
        .unwrap();
    for mode in [
        crate::engine::executor::vec::config::VecMode::Off,
        crate::engine::executor::vec::config::VecMode::On,
    ] {
        let _mode = crate::engine::executor::vec::config::enter_mode(mode);
        for condition in [
            "s IN (_binary'abc')",
            "s LIKE _binary'abc'",
            "s IN (_binary'abc',_binary'd',_binary'e',_binary'f')",
        ] {
            let QueryResponse::ResultSet(rows) = engine
                .execute(&format!("SELECT k FROM pred WHERE {condition} ORDER BY k"))
                .unwrap()
                .materialize()
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(
                rows.rows,
                vec![vec![Value::Int(42)], vec![Value::Int(43)]],
                "{condition}"
            );
        }
    }
}

#[test]
fn audit1106_ctas_and_temporal_assignment() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let QueryResponse::ResultSet(metadata) = engine.execute("SELECT CONV('1',10,2)").unwrap()
    else {
        panic!()
    };
    assert_eq!(
        metadata.columns[0].source.length,
        Some(260),
        "{:?}",
        metadata.columns[0]
    );
    for (expression, ty) in [
        ("CAST('ab' AS CHAR(5))", "varchar(5)"),
        ("FROM_DAYS(739921)", "date"),
        ("ROUND(123.456,-1)", "decimal(4,0)"),
        ("TIME('10:00:00')", "time"),
        ("1+0.5", "decimal(3,1)"),
        ("CONV('1',10,2)", "varchar(65)"),
    ] {
        engine.execute("DROP TABLE IF EXISTS ct").unwrap();
        engine
            .execute(&format!("CREATE TABLE ct AS SELECT {expression} AS x"))
            .unwrap();
        let QueryResponse::ResultSet(rows) = engine.execute("SHOW COLUMNS FROM ct").unwrap() else {
            panic!()
        };
        assert_eq!(rows.rows[0][1].to_display_string(), ty, "{expression}");
    }
    engine.execute("CREATE TABLE tm(t TIME(3))").unwrap();
    assert_eq!(
        engine
            .execute("INSERT INTO tm VALUES('838:59:59.9999')")
            .unwrap_err()
            .code(),
        1292
    );
    for value in ["838:59:59.999", "-838:59:59.999"] {
        assert_eq!(
            engine
                .execute(&format!("INSERT INTO tm VALUES('{value}')"))
                .unwrap_err()
                .code(),
            1292
        );
    }
    engine
        .execute("SET sql_mode='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO'")
        .unwrap();
    assert_eq!(
        engine
            .execute("CREATE TABLE dz AS SELECT 1/0 AS x")
            .unwrap_err()
            .code(),
        1365
    );
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT CAST(0x6162 AS BINARY) UNION SELECT 'ab'")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(rows.rows.len(), 1);
}

expr_case!(
    audit1106_fraction_rounding,
    "SELECT TIME('10:20:30.9999999'), TIME('-10:20:30.9999999'), CAST('2026-01-05 abc' AS DATE)",
    &["10:20:31", "-10:20:31", "2026-01-05"]
);
expr_case!(
    audit1106_ssn_structure,
    "SELECT MASK_SSN('123-45-6789')",
    &["XXX-XX-6789"]
);
expr_case!(audit1106_timezone_gap_matches_loaded_mysql, "SELECT CONVERT_TZ('2026-03-08 02:30:00','America/New_York','+00:00'), CONVERT_TZ('2026-11-01 01:30:00','America/New_York','+00:00')", &["2026-03-08 07:00:00", "2026-11-01 05:30:00"]);

expr_case!(audit1106_json_valid_input_domains, "SELECT JSON_VALID(5), JSON_VALID(1.5), JSON_VALID(TRUE), JSON_VALID('5'), JSON_VALID(CAST('5' AS JSON)), PS_THREAD_ACCOUNT(NULL), PS_THREAD_ACCOUNT(999999999)", &["0", "0", "0", "1", "1", "NULL", "NULL"]);

expr_case!(
    audit1106_json_scalar_before_head,
    "SELECT JSON_SET('1','$[last-7]',9),JSON_INSERT('1','$[last-7]',9)",
    &["[9, 1]", "[9, 1]"]
);
expr_case!(
    audit1106_time_format_range,
    "SELECT TIME_FORMAT('900:20:30','%T'),TIME_FORMAT('-900:20:30','%T')",
    &["838:59:59", "-838:59:59"]
);
#[test]
fn audit1106_str_to_date_locale_independent() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    e.execute("SET lc_time_names='de_DE'").unwrap();
    let QueryResponse::ResultSet(r) = e
        .execute("SELECT STR_TO_DATE('January 05 2026','%M %d %Y')")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(r.rows[0][0].to_display_string(), "2026-01-05");
}
#[test]
fn audit1106_locks_fold_names_and_count_reentry() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let mut a = SessionContext::default();
    a.connection_id = 8911;
    let mut b = SessionContext::default();
    b.connection_id = 8912;
    for (owner, sql, expected) in [
        (true, "SELECT GET_LOCK('MiXeDAudit1106',0)", "1"),
        (false, "SELECT GET_LOCK('MIXEDAudit1106',0)", "0"),
        (true, "SELECT GET_LOCK('mixedaudit1106',0)", "1"),
        (true, "SELECT RELEASE_ALL_LOCKS()", "2"),
    ] {
        let QueryResponse::ResultSet(r) = e
            .execute_with_session(if owner { &mut a } else { &mut b }, sql)
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(r.rows[0][0].to_display_string(), expected);
    }
}

#[test]
fn audit1106_invalid_locale_rejected_without_mutation() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    e.execute_with_session(&mut session, "SET lc_time_names='de_DE'")
        .unwrap();
    for sql in [
        "SET lc_time_names='not_a_locale'",
        "SET GLOBAL lc_time_names='not_a_locale'",
    ] {
        assert_eq!(
            e.execute_with_session(&mut session, sql)
                .unwrap_err()
                .code(),
            1649
        );
    }
    assert_eq!(session.sysvars.lc_time_names, "de_DE");
}

expr_case!(audit1106_regex_repeated_literal_markers, "SELECT REGEXP_LIKE('straße','s{2}','i'),REGEXP_LIKE('ss','s{2}','i'),REGEXP_SUBSTR('xstrassestrasseY','(straße){2}',1,1,'i')", &["0","1","strassestrasse"]);
#[test]
fn audit1106_addtime_clamping_warns() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let QueryResponse::ResultSet(r) = e
        .execute_with_session(&mut session, "SELECT ADDTIME('838:59:59','00:00:01')")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(r.rows[0][0].to_display_string(), "838:59:59");
    let QueryResponse::ResultSet(r) = e
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(r.rows[0][1].to_display_string(), "1292");
}
expr_case!(
    audit1106_time_format_fraction_at_max,
    "SELECT TIME_FORMAT('838:59:59.999999','%T.%f')",
    &["838:59:59.000000"]
);
#[test]
fn audit1106_union_metadata_preserves_literal_domains() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let QueryResponse::ResultSet(r) = e
        .execute("SELECT CAST(0x6162 AS BINARY) UNION SELECT 'ab'")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        r.columns[0].source.collation_id,
        Some(63),
        "{:?}",
        r.columns[0]
    );
    assert_eq!(r.rows, vec![vec![Value::Bytes(b"ab".to_vec())]]);
    e.execute("CREATE TABLE un AS SELECT 1 AS x UNION SELECT 1.5")
        .unwrap();
    let QueryResponse::ResultSet(r) = e.execute("SHOW COLUMNS FROM un").unwrap() else {
        panic!()
    };
    assert_eq!(r.rows[0][1].to_display_string(), "decimal(2,1)");
    let QueryResponse::ResultSet(r) = e
        .execute("SELECT CAST('{\"x\":1}' AS JSON) UNION SELECT 'a'")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(r.columns[0].source.collation_id, Some(46));
}

#[test]
fn audit1106_fractional_time_respects_truncation_mode() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for (mode, time, micro) in [
        ("", "10:20:31", "0"),
        ("TIME_TRUNCATE_FRACTIONAL", "10:20:30.999999", "999999"),
    ] {
        e.execute_with_session(&mut session, &format!("SET sql_mode='{mode}'"))
            .unwrap();
        let QueryResponse::ResultSet(r) = e
            .execute_with_session(
                &mut session,
                "SELECT TIME('10:20:30.9999999'),MICROSECOND('10:20:30.9999999')",
            )
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(
            r.rows[0]
                .iter()
                .map(Value::to_display_string)
                .collect::<Vec<_>>(),
            [time, micro]
        );
    }
}

#[test]
fn audit1106_thread_account_uses_requested_connection() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let _other =
        e.registry()
            .register(1822, "other".into(), "127.0.0.1:8123".into(), "test".into());
    let QueryResponse::ResultSet(r) = e
        .execute("SELECT PS_THREAD_ACCOUNT(1822),PS_THREAD_ACCOUNT(999999999)")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(r.rows[0][0].to_display_string(), "other@127.0.0.1");
    assert!(matches!(r.rows[0][1], Value::Null));
}

#[test]
fn audit1106_thread_lookup_cache_tracks_live_registry() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let sql = "SELECT PS_THREAD_ID(1822),PS_THREAD_ACCOUNT(1822)";
    let QueryResponse::ResultSet(r) = e.execute(sql).unwrap() else {
        panic!()
    };
    assert!(r.rows[0].iter().all(|v| matches!(v, Value::Null)));
    let _handle =
        e.registry()
            .register(1822, "other".into(), "127.0.0.1:8123".into(), "test".into());
    let QueryResponse::ResultSet(r) = e.execute(sql).unwrap() else {
        panic!()
    };
    assert_eq!(
        r.rows[0]
            .iter()
            .map(Value::to_display_string)
            .collect::<Vec<_>>(),
        ["1822", "other@127.0.0.1"]
    );
    e.registry().unregister(1822);
    let QueryResponse::ResultSet(r) = e.execute(sql).unwrap() else {
        panic!()
    };
    assert!(r.rows[0].iter().all(|v| matches!(v, Value::Null)));
}

expr_case!(
    audit1106_regex_escaped_expanding_literals,
    r"SELECT REGEXP_LIKE('ss','\\x{DF}','i'),REGEXP_LIKE('ss','\\u00DF','i'),REGEXP_LIKE('ss','[\\x{DF}]','i')",
    &["1", "1", "0"]
);

#[test]
fn audit1106_protocol_const_lane_reads_live_registry() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.connection_id = 9009;
    let _handle =
        e.registry()
            .register(1822, "other".into(), "127.0.0.1:8123".into(), "test".into());
    let sql = "SELECT PS_THREAD_ID(1822),PS_THREAD_ACCOUNT(1822)";
    let ast = std::sync::Arc::new(crate::parser::parse_statement(sql).unwrap());
    let r = <StorageQueryEngine as QueryEngine>::execute_parsed(&e, &mut session, sql, ast.clone())
        .unwrap()
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(r) = r else {
        panic!()
    };
    assert_eq!(
        r.rows[0]
            .iter()
            .map(Value::to_display_string)
            .collect::<Vec<_>>(),
        ["1822", "other@127.0.0.1"]
    );
    e.registry().unregister(1822);
    let r = <StorageQueryEngine as QueryEngine>::execute_parsed(&e, &mut session, sql, ast)
        .unwrap()
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(r) = r else {
        panic!()
    };
    assert!(r.rows[0].iter().all(|v| matches!(v, Value::Null)));
}

#[test]
fn audit1106_covering_integer_projection_keeps_nulls_unsigned_and_fallback_types() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE ip(id INT PRIMARY KEY,i BIGINT,u BIGINT UNSIGNED,d DECIMAL(20,4),s VARCHAR(16),KEY k(i,u,d,s))").unwrap();
    e.execute("INSERT INTO ip VALUES(1,-9223372036854775808,18446744073709551615,1234.5000,'a'),(2,9223372036854775807,9223372036854775808,-2.5000,'b'),(3,NULL,NULL,NULL,NULL),(4,0,0,0,'c')").unwrap();
    for mode in [
        crate::engine::executor::vec::config::VecMode::Off,
        crate::engine::executor::vec::config::VecMode::On,
    ] {
        let _mode = crate::engine::executor::vec::config::enter_mode(mode);
        let mut results = Vec::new();
        for hint in ["FORCE INDEX(k)", "IGNORE INDEX(k)"] {
            let QueryResponse::ResultSet(r) = e
                .execute(&format!("SELECT i,u,d,s FROM ip {hint} ORDER BY i,u"))
                .unwrap()
                .materialize()
                .unwrap()
            else {
                panic!()
            };
            results.push(
                r.rows
                    .iter()
                    .map(|row| row.iter().map(Value::to_display_string).collect::<Vec<_>>())
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(results[0], results[1], "{mode:?}");
        assert_eq!(results[0][1][1], "18446744073709551615");
        assert_eq!(results[0][3][1], "9223372036854775808");
    }
}

#[test]
fn audit1106_series_metadata_in_derived_cte_and_prepared_queries() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "SELECT * FROM (SELECT * FROM generate_series(1,5)) t",
        "SELECT n FROM (SELECT * FROM generate_series(5,1,-1) AS s(n)) t ORDER BY n",
        "WITH c AS (SELECT * FROM generate_series(1,5) AS s(n)) SELECT n FROM c ORDER BY n",
        "SELECT n FROM (SELECT * FROM generate_series(1,5) WITH ORDINALITY AS s(n,ord)) t ORDER BY ord",
    ] {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap() else {
            panic!("{sql}");
        };
        let actual: Vec<_> = result.rows.iter().map(|row| row[0].to_display_string()).collect();
        assert_eq!(actual, ["1", "2", "3", "4", "5"], "{sql}");
        let mut session = SessionContext::default();
        let described = engine.describe_prepared_statement(&mut session, sql).unwrap();
        assert_eq!(described.len(), result.columns.len(), "{sql}");
        for (metadata, executed) in described.iter().zip(&result.columns) {
            assert_eq!(metadata.name, executed.name, "{sql}");
            assert_eq!(metadata.column_type, executed.column_type, "{sql}");
        }
        let template = engine
            .prepare_statement_template(&mut session, sql, parse_statement(sql).unwrap())
            .unwrap();
        let QueryResponse::ResultSet(prepared) = engine
            .execute_qualified_parsed_statement(&mut session, sql, template)
            .unwrap().materialize().unwrap() else { panic!("{sql}") };
        assert_eq!(prepared.rows, result.rows, "{sql}");
    }
}

expr_case!(audit1106_regex_scalar_boolean_fast_positive_and_full_fold_fallback,
    "SELECT REGEXP_LIKE(CONCAT('v1','中文'),'v[0-9]+'),REGEXP_LIKE(CONCAT('stra','ße'),'strasse','i'),REGEXP_LIKE(CONCAT('s','s'),'ß','i'),REGEXP_LIKE(CONCAT('x','中文'),'v[0-9]+'),REGEXP_LIKE(CONCAT('ss',CHAR(10 USING utf8mb4)),'^ss$','i'),REGEXP_LIKE(CONCAT('ß',CHAR(10 USING utf8mb4)),'^ss$','i')",
    &["1","1","1","0","1","1"]);
