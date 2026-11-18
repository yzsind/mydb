use super::*;

#[test]
fn optional_parentheses_preserve_function_and_column_identity() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let mut run = |sql| engine.execute_with_session(&mut session, sql);
    run("SET time_zone='+00:00', timestamp=1700000000").unwrap();
    let sql = "SELECT utc_timestamp = UTC_TIMESTAMP(), UTC_DATE = UTC_DATE(), \
               UTC_TIME = UTC_TIME(), CURRENT_USER = CURRENT_USER(), \
               t.UTC_TIMESTAMP, t.CURRENT_TIMESTAMP, t.CURRENT_USER \
               FROM (SELECT 42 AS `UTC_TIMESTAMP`, 43 AS `CURRENT_TIMESTAMP`, \
                            44 AS `CURRENT_USER`) AS t";
    let QueryResponse::ResultSet(result) = run(sql).unwrap() else {
        panic!("expected rows");
    };
    assert_eq!(result.rows[0], [1, 1, 1, 1, 42, 43, 44].map(Value::Int));

    run("SET @v=UTC_TIMESTAMP").unwrap();
    run("SET @fractional=UTC_TIMESTAMP(3)").unwrap();
    run("CREATE TABLE defaults_t (v DATETIME DEFAULT (UTC_TIMESTAMP))").unwrap();
    run("INSERT INTO defaults_t VALUES ()").unwrap();
    run("INSERT INTO defaults_t VALUES (UTC_TIMESTAMP)").unwrap();
    run("CREATE VIEW clock_v AS SELECT UTC_TIMESTAMP AS v").unwrap();
    for sql in [
        "SELECT @v = '2023-11-14 22:13:20'",
        "SELECT @v <> 'UTC_TIMESTAMP', @fractional = '2023-11-14 22:13:20.000'",
        "SELECT COUNT(*) = 2 FROM defaults_t WHERE v=UTC_TIMESTAMP()",
        "SELECT v=UTC_TIMESTAMP() FROM clock_v",
    ] {
        let QueryResponse::ResultSet(result) = run(sql).unwrap() else {
            panic!("expected rows: {sql}");
        };
        assert!(
            result.rows[0].iter().all(|value| value == &Value::Int(1)),
            "{sql}"
        );
    }
    assert_eq!(run("SET @v=NOW").unwrap_err().code(), 1054);
    assert_eq!(run("SET @v=no_such_column").unwrap_err().code(), 1054);
}

#[test]
fn function_tokens_follow_spacing_and_qualification_rules() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let mut run = |sql| engine.execute_with_session(&mut session, sql);
    run("SET sql_mode=''").unwrap();
    for (sql, code) in [
        ("SELECT NOW", 1054),
        ("SELECT DATABASE", 1064),
        ("SELECT SCHEMA", 1064),
        ("SELECT DEFAULT", 1064),
        ("SELECT `SCHEMA`()", 1630),
        ("SELECT NOW ()", 1630),
        ("SELECT COUNT (1)", 1630),
        ("SELECT COUNT (*)", 1064),
        ("SELECT CAST (1 AS SIGNED)", 1584),
        ("SELECT JSON_ARRAYAGG /* comment */ (1)", 1630),
        ("SELECT JSON_OBJECTAGG ('k', 1)", 1630),
        ("SELECT ST_COLLECT (ST_GeomFromText('POINT(1 2)'))", 1630),
        ("SELECT 1 AS CURRENT_TIMESTAMP", 1064),
        ("SELECT t. UTC_DATE FROM (SELECT 1 AS `UTC_DATE`) t", 1064),
        (
            "SELECT CURRENT_USER .x FROM (SELECT 1 AS x) AS `CURRENT_USER`",
            1064,
        ),
        ("CREATE TABLE UTC_DATE (i INT)", 1064),
        ("CREATE TABLE t (CURRENT_USER INT)", 1064),
        ("CREATE TABLE count(i INT)", 1064),
        ("CREATE TABLE t (v DATETIME DEFAULT UTC_TIMESTAMP)", 1064),
        (
            "CREATE TABLE t (v DATETIME ON UPDATE UTC_TIMESTAMP())",
            1064,
        ),
        ("CREATE TABLE t (v DATETIME DEFAULT NOW ())", 1064),
        (
            "CREATE TABLE t (v DATETIME(6) DEFAULT CURRENT_TIMESTAMP(7))",
            1067,
        ),
        (
            "CREATE TABLE t (v DATETIME(6) ON UPDATE CURRENT_TIMESTAMP(7))",
            1294,
        ),
    ] {
        let error = run(sql).unwrap_err();
        assert_eq!(error.code(), code, "{sql}: {error}");
    }

    run("SELECT t .UTC_DATE FROM (SELECT 1 AS `UTC_DATE`) t").unwrap();
    run("SELECT CURRENT_USER.x FROM (SELECT 1 AS x) AS `CURRENT_USER`").unwrap();
    run("CREATE TABLE count (i INT)").unwrap();
    run("CREATE FUNCTION now () RETURNS INT DETERMINISTIC NO SQL RETURN 42").unwrap();
    run("CREATE FUNCTION `UTC_TIMESTAMP`() RETURNS INT DETERMINISTIC NO SQL RETURN 43").unwrap();
    for sql in [
        "SELECT NOW (), `NOW`(), test.NOW (), `UTC_TIMESTAMP`()",
        "SELECT NOW/* comment */(), `NOW` (), test.`NOW`(), test.UTC_TIMESTAMP ()",
    ] {
        let QueryResponse::ResultSet(result) = run(sql).unwrap() else {
            panic!("expected rows: {sql}");
        };
        assert_eq!(result.rows[0], [42, 42, 42, 43].map(Value::Int), "{sql}");
    }

    run("CREATE VIEW spaced_routine AS SELECT NOW () AS v").unwrap();
    run("SET sql_mode='ANSI'").unwrap();
    let QueryResponse::ResultSet(result) = run(r#"SELECT v, "NOW"() FROM spaced_routine"#).unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(result.rows[0], [42, 42].map(Value::Int));

    run("SET sql_mode='IGNORE_SPACE'").unwrap();
    assert_eq!(run("CREATE TABLE sum (i INT)").unwrap_err().code(), 1064);
    run("CREATE TABLE `sum`(i INT)").unwrap();
    run("SELECT COUNT /* comment */ (*), JSON_ARRAYAGG (1)").unwrap();
    // IGNORE_SPACE changes tokenization only when a call can follow.
    run("SELECT NOW FROM (SELECT 1 AS NOW) AS t").unwrap();
}

#[test]
fn temporal_precision_is_validated_at_prepare_time() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let mut run = |sql| engine.execute_with_session(&mut session, sql);
    for (sql, code) in [
        ("SELECT UTC_TIMESTAMP(1+2)", 1064),
        ("SELECT UTC_TIME(-1)", 1064),
        ("SELECT NOW('3')", 1064),
        ("SELECT LOCALTIME(NULL)", 1064),
        ("SELECT CURTIME(3.0)", 1064),
        ("SELECT UTC_DATE(0)", 1064),
        ("SELECT CURRENT_USER(1)", 1064),
        ("PREPARE s FROM 'SELECT UTC_TIMESTAMP(?)'", 1064),
        ("PREPARE s FROM 'SELECT NOW ()'", 1630),
        ("PREPARE s FROM 'SELECT CURRENT_TIMESTAMP(7)'", 1426),
    ] {
        let error = run(sql).unwrap_err();
        assert_eq!(error.code(), code, "{sql}: {error}");
    }
    run("SET time_zone='+00:00', timestamp=1700000000.123456").unwrap();
    let QueryResponse::ResultSet(result) = run(
        "SELECT UTC_TIMESTAMP(0003), UTC_TIME(6), UTC_TIMESTAMP(256), \
                UTC_TIMESTAMP(3) + 0, UTC_TIME(3) + 0",
    )
    .unwrap()
    .materialize()
    .unwrap() else {
        panic!("expected rows");
    };
    let row = &result.rows[0];
    assert_eq!(
        crate::storage::value_to_string(&row[0]),
        "2023-11-14 22:13:20.123"
    );
    assert_eq!(crate::storage::value_to_string(&row[1]), "22:13:20.123456");
    assert_eq!(
        crate::storage::value_to_string(&row[2]),
        "2023-11-14 22:13:20"
    );
    assert_eq!(
        crate::storage::value_to_string(&row[3]),
        "20231114221320.123"
    );
    assert_eq!(crate::storage::value_to_string(&row[4]), "221320.123");
}
