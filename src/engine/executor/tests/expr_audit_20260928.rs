use super::*;

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    match engine
        .execute(sql)
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"))
    {
        QueryResponse::ResultSet(result) => result.rows,
        other => panic!("{sql}: {other:?}"),
    }
}

fn strings(engine: &StorageQueryEngine, sql: &str) -> Vec<String> {
    rows(engine, sql)[0]
        .iter()
        .map(crate::storage::value_to_string)
        .collect()
}

#[test]
fn expression_audit_temporal_exact_and_generated() {
    for mode in [
        crate::engine::executor::vec::config::VecMode::Off,
        crate::engine::executor::vec::config::VecMode::On,
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        let _scope = crate::engine::executor::vec::config::enter_mode(mode);
        engine.execute("CREATE TABLE t(id INT PRIMARY KEY, d DATE, dt DATETIME(6), tm TIME(6), g BIGINT AS(d+1) STORED)").unwrap();
        engine.execute("INSERT INTO t(id,d,dt,tm) VALUES(1,'2020-01-01','2020-01-01 10:11:12.123456','10:11:12.500000')").unwrap();
        assert_eq!(
            strings(&engine, "SELECT dt+0,d+0,g,tm+0 FROM t"),
            [
                "20200101101112.123456",
                "20200101",
                "20200102",
                "101112.500000"
            ]
        );
        assert_eq!(
            strings(&engine, "SELECT dt-0,dt*1,dt/1 FROM t"),
            [
                "20200101101112.123456",
                "20200101101112.123456",
                "20200101101112.1234560000"
            ]
        );
        assert_eq!(
            strings(
                &engine,
                "SELECT CAST('00:00:00.000001' AS TIME(6))+0,CAST('-00:00:00.000001' AS TIME(6))+0"
            ),
            ["0.000001", "-0.000001"]
        );
        engine.execute("UPDATE t SET d='2020-02-01'").unwrap();
        assert_eq!(strings(&engine, "SELECT g FROM t"), ["20200202"]);
        assert_eq!(
            strings(
                &engine,
                "SELECT tm+INTERVAL 1 HOUR,TIMEDIFF(tm,'08:00:00') FROM t"
            ),
            ["11:11:12.5", "02:11:12.5"]
        );
    }
}

#[test]
fn expression_audit_coercions() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(strings(&engine,"SELECT -(x'414243'), -(b'101'), ABS(x'41'), SPACE(b'1'), b'101' AND 1, NOT b'101', UNHEX('E9')=0xE9"), ["-4276803","-5","65"," ","1","0","1"]);
    assert_eq!(
        strings(
            &engine,
            "SELECT '123abc' DIV 2,'3.9abc' DIV 2,'2020-01-01' DIV 1,'9007199254740993x' DIV 1"
        ),
        ["61", "1", "2020", "9007199254740993"]
    );
    assert_eq!(strings(&engine,"SELECT CAST(1e20 AS SIGNED),CAST('99999999999999999999' AS SIGNED),CAST(9223372036854775808 AS SIGNED)"), ["9223372036854775807","-1","-9223372036854775808"]);
    assert_eq!(
        rows(&engine, "SELECT CAST(1900 AS YEAR),CAST(2156 AS YEAR)"),
        vec![vec![Value::Null, Value::Null]]
    );
}

#[test]
fn expression_audit_time_and_string_boundaries() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        strings(
            &engine,
            "SELECT LPAD('中文',4,''), RPAD('abc',2,''),LPAD('abc',0,'')"
        ),
        ["", "ab", ""]
    );
    assert_eq!(strings(&engine,"SELECT WEEK('10:11:12'),YEAR('10:11:12'),DATE_FORMAT('10:11:12','%Y-%m-%d'),DATE_ADD('10:11:12',INTERVAL 1 DAY),LAST_DAY('2020-02-00')"), ["45","2010","2010-11-12","2010-11-13","2020-02-29"]);
    assert_eq!(strings(&engine,"SELECT CAST('10:11:12.999999' AS TIME(3)),CAST('-00:00:00.999999' AS TIME(3)),ADDTIME(59,2.5)"), ["10:11:13","-00:00:01","00:01:01.500000"]);
    assert_eq!(rows(&engine,"SELECT TIME_FORMAT('abc','%H'),TO_SECONDS('FF'),ADDTIME(CAST('838:59:59' AS TIME),'1:00:00')"),vec![vec![Value::Null;3]]);
    assert_eq!(
        rows(&engine, "SELECT BINARY(1)"),
        vec![vec![Value::Bytes(b"1".to_vec())]]
    );
}

#[test]
fn expression_audit_enum_string_order() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(e ENUM('small','medium','large'))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES ('small')").unwrap();
    assert_eq!(
        strings(
            &engine,
            "SELECT e<'medium',e>'medium',e<2,e=1,e='missing',e<>'missing' FROM t"
        ),
        ["0", "1", "1", "1", "0", "1"]
    );
}

#[test]
fn expression_audit_prepared_exact() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET @s='0.123456789012345678901234567890'")
        .unwrap();
    engine
        .execute_with_session(&mut session, "PREPARE s FROM 'SELECT ?+0'")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "EXECUTE s USING @s")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        crate::storage::value_to_string(&result.rows[0][0]),
        "0.123456789012345678901234567890"
    );
}

#[test]
fn expression_audit_warning_contracts() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "SELECT DAYNAME('0000-00-00')",
        "SELECT CAST('' AS SIGNED)",
        "SELECT CAST('' AS DECIMAL)",
        "SELECT CAST('abc' AS CHAR(2))",
        "SELECT NOT('abc')",
        "SELECT CAST('2020-01-01' AS YEAR)",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap()
        else {
            panic!()
        };
        assert!(
            result.rows.iter().any(|row| row[1] == Value::Int(1292)),
            "{sql}: {:?}",
            result.rows
        );
    }
}

#[test]
fn expression_audit_binary_and_numeric_descriptors_on_empty_result() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(b BLOB,tm TIME(6),dt DATETIME(6))")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine.execute("SELECT CHAR(233),CONCAT(b,'!'),SUBSTRING(b,2),BIN_TO_UUID(UUID_TO_BIN('6ccd780c-baba-1026-9564-5b8c656024db')),MOD(10,3),INTERVAL(5,1,2,3),tm+INTERVAL 1 HOUR,TIMEDIFF(tm,'08:00:00'),dt+0 FROM t").unwrap() else { panic!() };
    assert!(result.rows.is_empty());
    for column in &result.columns[..3] {
        assert_eq!(column.source.collation_id, Some(63));
    }
    assert_ne!(result.columns[3].source.collation_id, Some(63));
    for column in &result.columns[4..6] {
        assert_eq!(column.column_type, ColumnType::LongLong);
    }
    assert_eq!(result.columns[6].source.decimals, Some(6));
    assert_eq!(result.columns[7].source.decimals, Some(6));
    assert!(matches!(
        result.columns[8].column_type,
        ColumnType::Decimal { scale: 6, .. }
    ));
}
