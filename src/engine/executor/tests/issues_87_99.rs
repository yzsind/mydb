//! General regressions for the temporal and JSON result contracts in issues 87–99.
use super::*;

fn result(e: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> ResultSet {
    match e
        .execute_with_session(s, sql)
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"))
        .materialize()
        .unwrap()
    {
        QueryResponse::ResultSet(rs) => rs,
        other => panic!("{sql}: {other:?}"),
    }
}

fn text(e: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> Vec<Vec<String>> {
    result(e, s, sql)
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(|v| v.to_display_string()).collect())
        .collect()
}

fn all_modes(check: impl Fn(&StorageQueryEngine, &mut SessionContext)) {
    for mode in [
        vec::config::VecMode::Off,
        vec::config::VecMode::On,
        vec::config::VecMode::Auto,
    ] {
        let _mode = vec::config::enter_mode(mode);
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let e = open_test_engine(dir.path());
        let mut s = SessionContext::default();
        for sql in [
            "SET sql_mode=''",
            "SET time_zone='+00:00'",
            "SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci",
        ] {
            e.execute_with_session(&mut s, sql).unwrap();
        }
        check(&e, &mut s);
    }
}

#[test]
fn issue87_create_clock_persists_and_alter_preserves_identity() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    {
        let e = open_relaxed_test_engine(dir.path());
        let mut s = SessionContext::default();
        for sql in [
            "SET time_zone='+00:00'",
            "SET timestamp=1700000000.125",
            "CREATE TABLE clock_t(id INT)",
            "SET timestamp=1700000100",
            "CREATE TABLE clock_like LIKE clock_t",
            "CREATE TABLE clock_ctas AS SELECT 1 id",
            "ALTER TABLE clock_t ADD note INT",
        ] {
            e.execute_with_session(&mut s, sql).unwrap();
        }
        assert_eq!(text(&e,&mut s,"SELECT TABLE_NAME,CREATE_TIME FROM information_schema.tables WHERE TABLE_SCHEMA='test' AND TABLE_NAME LIKE 'clock_%' ORDER BY TABLE_NAME"),
            vec![vec!["clock_ctas","2023-11-14 22:15:00"],vec!["clock_like","2023-11-14 22:15:00"],vec!["clock_t","2023-11-14 22:13:20"]]);
        e.shutdown().unwrap();
    }
    let e = open_relaxed_test_engine(dir.path());
    let mut s = SessionContext::default();
    e.execute_with_session(&mut s, "SET time_zone='+08:00'")
        .unwrap();
    assert_eq!(text(&e,&mut s,"SELECT CREATE_TIME FROM information_schema.tables WHERE TABLE_SCHEMA='test' AND TABLE_NAME='clock_t'"),vec![vec!["2023-11-15 06:13:20"]]);
}

#[test]
fn issue89_quote_is_character_in_scalar_vector_empty_and_ctas() {
    all_modes(|e, s| {
        e.execute("CREATE TABLE quotes(id INT PRIMARY KEY,v VARCHAR(40))")
            .unwrap();
        e.execute("INSERT INTO quotes VALUES(1,'abc'),(2,NULL),(3,'a\"b')")
            .unwrap();
        let rs = result(e, s, "SELECT JSON_QUOTE(v) FROM quotes ORDER BY id");
        assert_eq!(rs.columns[0].column_type, ColumnType::VarString);
        assert!(matches!(
            rs.rows[0][0],
            Value::String(_) | Value::SmallString(_)
        ));
        assert_eq!(
            text(e, s, "SELECT JSON_QUOTE(v) FROM quotes ORDER BY id"),
            vec![vec!["\"abc\""], vec!["NULL"], vec!["\"a\\\"b\""]]
        );
        for sql in [
            "SELECT JSON_QUOTE(NULL)",
            "SELECT JSON_QUOTE(v) FROM quotes WHERE id<0",
        ] {
            let column = &result(e, s, sql).columns[0];
            assert_eq!(column.column_type, ColumnType::VarString);
            assert_ne!(column.source.wire_type, Some(6));
        }
        e.execute("CREATE TABLE quoted AS SELECT JSON_QUOTE(v) q FROM quotes")
            .unwrap();
        e.execute("INSERT INTO quoted VALUES('xyz')").unwrap();
        e.execute("CREATE TABLE quote_literal AS SELECT JSON_QUOTE('abc') q")
            .unwrap();
        assert_eq!(
            text(e, s, "SHOW COLUMNS FROM quote_literal")[0][1],
            "varchar(20)"
        );
        assert_eq!(text(e, s, "SELECT COUNT(*) FROM quoted"), vec![vec!["4"]]);
        assert_eq!(
            text(e, s, "SELECT JSON_ARRAY(JSON_QUOTE('abc'))"),
            vec![vec!["[\"\\\"abc\\\"\"]"]]
        );
    });
}

#[test]
fn issue90_time_conversion_clips_but_arithmetic_reports_overflow() {
    all_modes(|e, s| {
        assert_eq!(
            text(e, s, "SELECT MICROSECOND('838:59:59.0000009')"),
            vec![vec!["0"]]
        );
        assert_eq!(text(e,s,"SELECT MICROSECOND('838:59:59.9999999'),HOUR('839:00:00.0000009'),TIME_TO_SEC('839:00:00.0000009')"),vec![vec!["0","838","3020399"]]);
        assert_eq!(text(e,s,"SELECT HOUR('839:00:00'),MINUTE('839:00:00'),SECOND('839:00:00'),TIME_TO_SEC('-839:00:00'),MICROSECOND('838:59:59.999999')"),vec![vec!["838","59","59","-3020399","0"]]);
        e.execute("CREATE TABLE times(id INT PRIMARY KEY,t TIME(6),v VARCHAR(40))")
            .unwrap();
        e.execute("INSERT INTO times VALUES(1,'838:59:59','839:00:00'),(2,'-838:59:59','-839:00:00'),(3,'12:34:56.123456','12:34:56.123456')").unwrap();
        assert_eq!(
            text(
                e,
                s,
                "SELECT HOUR(v),MINUTE(v),SECOND(v),TIME_TO_SEC(v) FROM times ORDER BY id"
            ),
            vec![
                vec!["838", "59", "59", "3020399"],
                vec!["838", "59", "59", "-3020399"],
                vec!["12", "34", "56", "45296"]
            ]
        );
        assert_eq!(text(e,s,"SELECT DATE_ADD(t,INTERVAL 1 SECOND),DATE_SUB(t,INTERVAL 1 SECOND) FROM times WHERE id<3 ORDER BY id"),vec![vec!["NULL","838:59:58"],vec!["-838:59:58","NULL"]]);
        assert_eq!(
            text(e, s, "SELECT MICROSECOND('12:34:56.123456')"),
            vec![vec!["123456"]]
        );
    });
}

#[test]
fn issue91_incomplete_dates_keep_components_and_obey_sql_mode() {
    all_modes(|e, s| {
        assert_eq!(
            text(
                e,
                s,
                "SELECT MONTHNAME('2001-11-00'),QUARTER('2001-11-00'),QUARTER('2005-00-00')"
            ),
            vec![vec!["November", "4", "0"]]
        );
        e.execute_with_session(s, "CREATE TABLE dates(d DATE)")
            .unwrap();
        e.execute_with_session(s, "INSERT INTO dates VALUES('2001-11-00'),('2005-00-00')")
            .unwrap();
        assert_eq!(
            text(e, s, "SELECT MONTHNAME(d),QUARTER(d) FROM dates"),
            vec![vec!["November", "4"], vec!["NULL", "0"]]
        );
        assert_eq!(text(e,s,"SELECT STR_TO_DATE('0000-00-00','%Y-%m-%d'),STR_TO_DATE('2024-00-01','%Y-%m-%d'),STR_TO_DATE('2001-11-00 12:34:56.123456','%Y-%m-%d %H:%i:%s.%f')"),vec![vec!["0000-00-00","2024-00-01","2001-11-00 12:34:56.123456"]]);
        e.execute_with_session(s, "SET sql_mode='ALLOW_INVALID_DATES'")
            .unwrap();
        assert_eq!(
            text(e, s, "SELECT MONTHNAME('2024-02-31'),QUARTER('2024-02-31')"),
            vec![vec!["February", "1"]]
        );
        assert_eq!(
            text(
                e,
                s,
                "SELECT STR_TO_DATE('2023-02-29','%Y-%m-%d'),STR_TO_DATE('2024-02-31','%Y-%m-%d')"
            ),
            vec![vec!["2023-02-29", "2024-02-31"]]
        );
        e.execute_with_session(s, "SET sql_mode='NO_ZERO_DATE,NO_ZERO_IN_DATE'")
            .unwrap();
        assert_eq!(text(e,s,"SELECT STR_TO_DATE('0000-00-00','%Y-%m-%d'),STR_TO_DATE('2001-11-00','%Y-%m-%d'),STR_TO_DATE('2024-02-31','%Y-%m-%d')"),vec![vec!["NULL","NULL","NULL"]]);
    });
}

#[test]
fn issue92_from_days_lower_boundary_keeps_normal_roundtrips() {
    all_modes(|e, s| {
        assert_eq!(text(e,s,"SELECT FROM_DAYS(-1),FROM_DAYS(0),FROM_DAYS(1),FROM_DAYS(59),FROM_DAYS(60),FROM_DAYS(365),FROM_DAYS(366),FROM_DAYS(367),FROM_DAYS(TO_DAYS('2024-02-29'))"),vec![vec!["0000-00-00","0000-00-00","0000-00-00","0000-00-00","0000-00-00","0000-00-00","0001-01-01","0001-01-02","2024-02-29"]]);
    });
}

#[test]
fn issue94_extract_compound_duration_and_datetime_domains() {
    all_modes(|e, s| {
        assert_eq!(
            text(
                e,
                s,
                "SELECT EXTRACT(DAY_HOUR FROM 'bad'),EXTRACT(DAY_SECOND FROM 20240203)"
            ),
            vec![vec!["NULL", "NULL"]]
        );
        e.execute("CREATE TABLE durations(v VARCHAR(40))").unwrap();
        e.execute(
            "INSERT INTO durations VALUES('-12:34:56.123456'),('12:34:56.123456'),('1 12:34:56')",
        )
        .unwrap();
        assert_eq!(text(e,s,"SELECT EXTRACT(DAY_HOUR FROM v),EXTRACT(DAY_MINUTE FROM v),EXTRACT(DAY_SECOND FROM v),EXTRACT(DAY_MICROSECOND FROM v) FROM durations"),vec![vec!["-12","-1234","-123456","-123456123456"],vec!["12","1234","123456","123456123456"],vec!["36","3634","363456","363456000000"]]);
        assert_eq!(text(e,s,"SELECT EXTRACT(DAY_HOUR FROM '2024-02-03 12:34:56'),EXTRACT(DAY_HOUR FROM CAST('-12:34:56' AS TIME))"),vec![vec!["312","-12"]]);
    });
}

#[test]
fn issue95_year_cast_distinguishes_no_digits_from_character_zero() {
    all_modes(|e, s| {
        assert_eq!(text(e,s,"SELECT CAST('bad' AS YEAR),CAST('abc' AS YEAR),CAST('' AS YEAR),CAST('  bad' AS YEAR),CAST('0' AS YEAR),CAST('00' AS YEAR),CAST('0bad' AS YEAR),CAST('12bad' AS YEAR),CAST(0 AS YEAR),CAST(NULL AS YEAR)"),vec![vec!["NULL","NULL","NULL","NULL","2000","2000","2000","2012","0000","NULL"]]);
        e.execute("CREATE TABLE years(v VARCHAR(20))").unwrap();
        e.execute("INSERT INTO years VALUES('bad'),('0'),('12bad'),(NULL)")
            .unwrap();
        assert_eq!(
            text(e, s, "SELECT CAST(v AS YEAR) FROM years"),
            vec![vec!["NULL"], vec!["2000"], vec!["2012"], vec!["NULL"]]
        );
    });
}

#[test]
fn issue96_json_search_validates_mode_paths_before_null_search() {
    all_modes(|e, s| {
        for (sql, code) in [
            (r#"SELECT JSON_SEARCH('{"a":1}','bad',NULL)"#, 3154),
            (
                r#"SELECT JSON_SEARCH('{"a":1}','one',NULL,NULL,'bad')"#,
                3143,
            ),
            (
                r#"SELECT JSON_SEARCH('{"a":1}','all',NULL,NULL,'$**')"#,
                3143,
            ),
        ] {
            assert_eq!(
                e.execute_with_session(s, sql).unwrap_err().code(),
                code,
                "{sql}"
            );
        }
        assert_eq!(
            text(
                e,
                s,
                r#"SELECT JSON_SEARCH('{"a":1}','one',NULL),JSON_SEARCH(NULL,'bad',NULL),JSON_SEARCH('{"a":1}',NULL,NULL,NULL,'bad')"#
            ),
            vec![vec!["NULL", "NULL", "NULL"]]
        );
    });
}

#[test]
fn issue97_json_value_time_has_its_own_conversion_contract() {
    all_modes(|e, s| {
        assert_eq!(
            text(
                e,
                s,
                r#"SELECT JSON_VALUE('{"v":12.345}','$.v' RETURNING TIME),JSON_VALUE('{"v":"123"}','$.v' RETURNING TIME),JSON_VALUE('{"v":"123"}','$.v' RETURNING TIME DEFAULT '01:02:03' ON ERROR),CAST('123' AS TIME)"#
            ),
            vec![vec!["NULL", "NULL", "01:02:03", "00:01:23"]]
        );
        for value in [
            r#""12:34:56""#,
            r#""12:34""#,
            r#""1 12:34:56""#,
            r#""2024-01-02 12:34:56""#,
        ] {
            let sql = format!("SELECT JSON_VALUE('{{\"v\":{value}}}','$.v' RETURNING TIME)");
            assert_ne!(text(e, s, &sql), vec![vec!["NULL"]], "{value}");
        }
        for sql in [
            r#"SELECT JSON_VALUE('{"v":"123"}','$.v' RETURNING TIME ERROR ON ERROR)"#,
            r#"SELECT JSON_VALUE('{"v":[1]}','$.v' RETURNING SIGNED ERROR ON ERROR)"#,
        ] {
            assert_eq!(e.execute_with_session(s, sql).unwrap_err().code(), 1690);
        }
    });
}

#[test]
fn issue98_json_metadata_is_static_for_scalar_aggregate_and_window() {
    all_modes(|e, s| {
        e.execute("CREATE TABLE jsons(id INT PRIMARY KEY,j JSON)")
            .unwrap();
        e.execute(r#"INSERT INTO jsons VALUES(1,'{"a":1}'),(2,'null')"#)
            .unwrap();
        for sql in [r#"SELECT JSON_VALUE('{"a":[1,2]}','$.a' RETURNING JSON)"#,
            "SELECT JSON_VALUE('{}','$.a' RETURNING JSON)","SELECT JSON_ARRAYAGG(j),JSON_OBJECTAGG(id,j) FROM jsons",
            "SELECT JSON_ARRAYAGG(j),JSON_OBJECTAGG(id,j) FROM jsons WHERE id<0",
            "SELECT JSON_ARRAYAGG(j) OVER(ORDER BY id),JSON_OBJECTAGG(id,j) OVER(ORDER BY id) FROM jsons"] {
            let rs=result(e,s,sql);
            assert!(rs.columns.iter().all(|c|c.column_type==ColumnType::Json),"{sql}: {:?}",rs.columns);
        }
    });
}

#[test]
fn issue99_text_fraction_and_prepared_time_keep_distinct_result_types() {
    all_modes(|e, s| {
        assert_eq!(text(e,s,"SELECT DATE_ADD('2024-02-29 23:59:59.999999',INTERVAL 1 MICROSECOND),ADDDATE('2024-02-29 23:59:59.999999',INTERVAL 1 MICROSECOND),ADDTIME('23:59:59.999999','00:00:00.000001'),SUBTIME('24:00:00.000001','00:00:00.000001')"),vec![vec!["2024-03-01 00:00:00","2024-03-01 00:00:00","24:00:00","24:00:00"]]);
        assert_eq!(text(e,s,"SELECT DATE_ADD('2024-02-29 23:59:59.123456',INTERVAL 1 MICROSECOND),ADDTIME('12:00:00.123456','00:00:00.000001')"),vec![vec!["2024-02-29 23:59:59.123457","12:00:00.123457"]]);
        e.execute_with_session(
            s,
            "PREPARE ps FROM 'SELECT ADDTIME(?,''00:00:00.000001'') AS result'",
        )
        .unwrap();
        e.execute_with_session(s, "SET @p='23:59:59.999999'")
            .unwrap();
        let rs = result(e, s, "EXECUTE ps USING @p");
        assert_eq!(rs.columns[0].column_type, ColumnType::Time);
        assert_eq!(rs.rows[0][0], Value::Time(86_400_000_000));
    });
}
