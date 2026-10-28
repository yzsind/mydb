use super::*;

fn strings(engine: &StorageQueryEngine, sql: &str) -> Vec<String> {
    let QueryResponse::ResultSet(result) = engine
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
    else {
        panic!("not a result set")
    };
    result.rows[0]
        .iter()
        .map(crate::storage::value_to_string)
        .collect()
}

macro_rules! scalar_case {
    ($name:ident, $sql:expr, [$($expected:expr),*]) => {
        #[test]
        fn $name() {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let engine = open_test_engine(dir.path());
            assert_eq!(strings(&engine, $sql), [$($expected),*]);
        }
    };
}
scalar_case!(
    e05_timestamp_invalid,
    "SELECT TIMESTAMP('2024-01-01 00:00:00','xx')",
    ["NULL"]
);
scalar_case!(
    e06_yearweek_invalid,
    "SELECT YEARWEEK('garbage'),YEARWEEK('2024-02-30'),YEARWEEK(2024)",
    ["NULL", "NULL", "NULL"]
);
scalar_case!(e07_missing_time, "SELECT STR_TO_DATE('2024-03-10','%Y-%m-%d %H:%i:%s'),STR_TO_DATE('02:30','%H:%i:%s'),STR_TO_DATE('2024-03-10 02','%Y-%m-%d %H:%i:%s')", ["2024-03-10 00:00:00","02:30:00","2024-03-10 02:00:00"]);
scalar_case!(e08_composite_overflow, "SELECT DATE_ADD('9999-12-31',INTERVAL '1-2' YEAR_MONTH),DATE_ADD('9999-12-31',INTERVAL '1 2' DAY_HOUR)", ["NULL","NULL"]);
scalar_case!(e09_hex_cast, "SELECT CAST(X'1F' AS DECIMAL(10,2)),CAST(0x1F AS DOUBLE),CAST(B'11111' AS FLOAT),CAST(X'3132' AS SIGNED)", ["31.00","31","31","12594"]);
scalar_case!(
    e11_ascii_binary,
    "SELECT ASCII(X'FF'),ASCII(X'FE'),ASCII(X''),ASCII('é')",
    ["255", "254", "0", "195"]
);
scalar_case!(
    e12_binary_trim,
    "SELECT HEX(LTRIM(X'20FF20')),HEX(RTRIM(X'20FF20'))",
    ["FF20", "20FF"]
);
scalar_case!(
    e13_from_days,
    "SELECT FROM_DAYS(1),FROM_DAYS(59),FROM_DAYS(60),FROM_DAYS(365),FROM_DAYS(366)",
    [
        "0000-01-01",
        "0000-02-28",
        "0000-03-01",
        "0000-12-31",
        "0001-01-01"
    ]
);
scalar_case!(
    e14_conv_negative,
    "SELECT CONV(-9223372036854775809,10,16)",
    ["7FFFFFFFFFFFFFFF"]
);
scalar_case!(
    e15_epoch_truth,
    "SELECT CAST('1970-01-01' AS DATE) IS TRUE, IF(CAST('1970-01-01' AS DATETIME),'y','n')",
    ["1", "y"]
);
scalar_case!(
    e16_time_format,
    "SELECT TIME_FORMAT('10:30:00','%Y-%H')",
    ["0000-10"]
);
scalar_case!(
    e17_bit_coercion,
    "SELECT '1.5'<<3,63>>2.5e0,-1.5|'12.5'",
    ["8", "15", "18446744073709551614"]
);
scalar_case!(
    e18_short_dates,
    "SELECT YEAR('123'),DATE('123'),MINUTE('123'),SECOND('123'),DATEDIFF('10:30:00','2024-01-01')",
    ["NULL", "NULL", "1", "23", "NULL"]
);
scalar_case!(
    e19_numeric_time,
    "SELECT MICROSECOND(DEGREES(1))",
    ["295780"]
);
scalar_case!(
    e22_regex_replacement,
    r"SELECT REGEXP_REPLACE('abc','(a)','\\1')",
    ["1bc"]
);
scalar_case!(
    e27_regex_occurrence,
    "SELECT REGEXP_INSTR('abcabc','abc',1,0),REGEXP_REPLACE('a1a2a3','a','X',1,-1)",
    ["1", "X1a2a3"]
);

#[test]
fn e01_temporal_comparison_routes() {
    for mode in [
        crate::engine::executor::vec::config::VecMode::Off,
        crate::engine::executor::vec::config::VecMode::On,
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        let _scope = crate::engine::executor::vec::config::enter_mode(mode);
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,dt DATETIME(6),tm TIME(6),KEY k(dt))")
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES(1,'2020-01-01 00:00:00.500001','10:00:00.500001')")
            .unwrap();
        for sql in ["SELECT COUNT(*) FROM t WHERE dt='2020-01-01 00:00:00.5000005'", "SELECT COUNT(*) FROM t IGNORE INDEX(k) WHERE dt='2020-01-01 00:00:00.5000005'", "SELECT COUNT(*) FROM t WHERE tm='10:00:00.5000005'", "SELECT COUNT(*) FROM t WHERE dt BETWEEN '2020-01-01 00:00:00.5000004' AND '2020-01-01 00:00:00.5000006'"] {
            assert_eq!(strings(&engine,sql),["1"],"{mode:?}: {sql}");
        }
    }
}

#[test]
fn e04_e23_addtime_typed_and_numeric() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(tm TIME)").unwrap();
    engine.execute("INSERT INTO t VALUES('08:00:00')").unwrap();
    assert_eq!(
        strings(&engine, "SELECT ADDTIME(tm,'01:00:00')+0 FROM t"),
        ["90000"]
    );
    assert_eq!(strings(&engine,"SELECT ADDTIME('00:00:07','00:00:01')+0,TRUNCATE(ADDTIME(-7,1),2),ADDTIME(-7,1)='-00:00:06'"),["8","-6","1"]);
    engine.execute("UPDATE t SET tm='838:59:59'").unwrap();
    assert_eq!(
        strings(&engine, "SELECT ADDTIME(tm,'839:00:00') FROM t"),
        ["NULL"]
    );
}

#[test]
fn e24_e25_json_aggregate_types() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT,a INT,b INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,1,1),(2,2,1),(3,1,2)")
        .unwrap();
    assert_eq!(
        strings(&engine, "SELECT JSON_ARRAYAGG(a=b) FROM t"),
        ["[true, false, false]"]
    );
    assert_eq!(strings(&engine,"SELECT JSON_ARRAYAGG(a) OVER(ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) FROM t"),["NULL"]);
}

#[test]
fn e26_e28_regex_errors() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for (sql, code) in [
        ("SELECT REGEXP_INSTR('abc','b',10)", 3686),
        ("SELECT REGEXP_SUBSTR('abc','b',10)", 3686),
        ("SELECT REGEXP_REPLACE('abc','b','x',10)", 3686),
        ("SELECT REGEXP_REPLACE('abc','b','x',0)", 1583),
        ("SELECT REGEXP_REPLACE('abc','abc','$1')", 3686),
        ("SELECT REGEXP_LIKE('a','*a')", 3688),
        ("SELECT REGEXP_LIKE('a','a{2,1}')", 3693),
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), code, "{sql}");
    }
}

#[test]
fn e29_year_string_ranges() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(y YEAR)").unwrap();
    engine.execute("INSERT INTO t VALUES(2001),(1999)").unwrap();
    assert_eq!(
        strings(&engine, "SELECT y<'99',y='01','99'<y FROM t"),
        ["0", "1", "1"]
    );
}

#[test]
fn e02_temporal_branch_string_domain() {
    for mode in [
        crate::engine::executor::vec::config::VecMode::Off,
        crate::engine::executor::vec::config::VecMode::On,
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        let _scope = crate::engine::executor::vec::config::enter_mode(mode);
        engine
            .execute("CREATE TABLE t(id INT,d DATE,dt DATETIME(6),tm TIME)")
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES(1,'2024-02-29','2024-02-29 12:00:00','08:00:00')")
            .unwrap();
        assert_eq!(strings(&engine,"SELECT CASE WHEN id>0 THEN d ELSE 'none' END,IF(id>0,dt,'n/a'),COALESCE(d,0),GREATEST(d,'2020-01-01'),CASE WHEN id>0 THEN tm ELSE 'none' END FROM t"),["2024-02-29","2024-02-29 12:00:00.000000","2024-02-29","2024-02-29","08:00:00"]);
    }
}

#[test]
fn e10_packet_guards() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        strings(
            &engine,
            "SELECT EXPORT_SET(18446744073709551615,REPEAT('a',1100000),'x',',',64) IS NULL"
        ),
        ["1"]
    );
    assert_eq!(strings(&engine,"SELECT MAKE_SET(15,REPEAT('a',17000000),REPEAT('b',17000000),REPEAT('c',17000000),REPEAT('d',17000000)) IS NULL"),["1"]);
    assert_eq!(
        strings(
            &engine,
            "SELECT EXPORT_SET(3,'é','x',',',3),MAKE_SET(7,'a',NULL,'')"
        ),
        ["é,é,x", "a,"]
    );
}

#[test]
fn e20_e21_temporal_comparison_diagnostics() {
    for mode in [
        crate::engine::executor::vec::config::VecMode::Off,
        crate::engine::executor::vec::config::VecMode::On,
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        let _scope = crate::engine::executor::vec::config::enter_mode(mode);
        engine
            .execute("CREATE TABLE t(id INT,tm TIME,dt DATETIME(6),KEY k(dt))")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,'00:00:00','2020-01-01'),(2,'00:00:00','2020-01-01'),(3,'00:00:00','2020-01-01')").unwrap();
        let mut session = SessionContext::default();
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(&mut session, "SELECT COUNT(*) FROM t WHERE tm='zz'")
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(result.rows, vec![vec![Value::Int(0)]]);
        let QueryResponse::ResultSet(warnings) = engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(warnings.rows.len(), 1, "{mode:?}: {:?}", warnings.rows);
        for _ in 0..4 {
            assert_eq!(
                engine
                    .execute("SELECT COUNT(*) FROM t WHERE dt='0000-00-00'")
                    .unwrap_err()
                    .code(),
                1525
            );
        }
    }
}

#[test]
fn e03_prepared_date_arithmetic() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET @d='2024-01-01 10:00:00'")
        .unwrap();
    for (function, expected) in [
        ("DATE_ADD", "2024-01-02 10:00:00"),
        ("DATE_SUB", "2023-12-31 10:00:00"),
    ] {
        engine
            .execute_with_session(
                &mut session,
                &format!("PREPARE s FROM 'SELECT {function}(?,INTERVAL 1 DAY)'"),
            )
            .unwrap();
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(&mut session, "EXECUTE s USING @d")
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(
            crate::storage::value_to_string(&result.rows[0][0]),
            expected
        );
        assert!(
            matches!(result.rows[0][0], Value::DateTime(_)),
            "{result:?}"
        );
        assert_eq!(result.columns[0].column_type, ColumnType::DateTime);
    }
}

#[test]
fn year_string_order_maps_two_digit_years() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(y YEAR)").unwrap();
    engine.execute("INSERT INTO t VALUES(1999)").unwrap();
    assert_eq!(
        strings(&engine, "SELECT y<'99','99'<y,y<99,99<y FROM t"),
        ["0", "0", "0", "1"]
    );
}

scalar_case!(
    regex_replacement_escape_and_capture_boundaries,
    r"SELECT REGEXP_REPLACE('abc','(?<x>a)','${x}'),REGEXP_REPLACE('abc','(a)','\\u0062'),REGEXP_REPLACE('abc','(a)','$12'),REGEXP_REPLACE('abc','a','$2',1,2)",
    ["abc", "bbc", "a2bc", "abc"]
);

scalar_case!(temporal_string_branches_preserve_precision,
    "SELECT IF(1,CAST('2024-01-01 10:00:00' AS DATETIME(6)),'n/a'),CASE WHEN 1 THEN CAST('08:00:00.5' AS TIME(6)) ELSE 'none' END,IF(1,CAST('08:00:00' AS TIME),'n/a')",
    ["2024-01-01 10:00:00.000000","08:00:00.500000","08:00:00"]);

scalar_case!(
    numeric_time_rounding_rejects_range_carry,
    "SELECT HOUR(8385959.9999999),MICROSECOND(8385959.9999999),MICROSECOND(59.9999999)",
    ["NULL", "NULL", "0"]
);

scalar_case!(invalid_time_text_has_no_zero_duration_identity,
    "SELECT CAST('00:00:00' AS TIME)='zz',CAST('00:00:00' AS TIME)<=>'zz',CAST('00:00:00' AS TIME)!='zz'",
    ["0","0","1"]);
