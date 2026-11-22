use super::support::execute_materialized as exec;
use super::*;

fn rows(engine: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    match exec(engine, s, sql).unwrap_or_else(|e| panic!("{sql}: {e:?}")) {
        QueryResponse::ResultSet(rs) => rs.rows,
        other => panic!("{sql}: {other:?}"),
    }
}

#[test]
fn parser_release_1121_between_and_numeric_wrappers() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for (expr, expected) in [
        ("2 BETWEEN 1 AND 3 IS TRUE", 1),
        ("2 BETWEEN 1 AND 3 IS NOT NULL", 1),
        ("0 BETWEEN NULL AND 3 IS TRUE", 0),
        ("'a' BETWEEN b'1' AND 2", 0),
        ("IF(1,b'1',0) IS TRUE", 1),
        ("NULLIF(b'1',0) IS TRUE", 1),
        ("(SELECT b'1') IS TRUE", 1),
        ("(CASE WHEN 1 THEN b'1' END + 0) = 1", 1),
        ("IF(1,b'1','2') = 1", 1),
        ("IF(0,b'1',_binary'2') + 0 = 2", 1),
        ("(SELECT b'1' c HAVING c='a') IS NULL", 1),
        ("1 + INTERVAL(0,1) * 2 = 1", 1),
    ] {
        assert_eq!(
            rows(&engine, &mut s, &format!("SELECT {expr}")),
            vec![vec![Value::Int(expected)]],
            "{expr}"
        );
    }
}

#[test]
fn parser_release_1121_between_uses_runtime_numeric_literal_provenance() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(
        &engine,
        &mut s,
        "CREATE TABLE bounds(n INT PRIMARY KEY,b VARBINARY(8))",
    )
    .unwrap();
    exec(
        &engine,
        &mut s,
        "INSERT INTO bounds VALUES(1,X'01'),(2,X'01'),(3,X'01')",
    )
    .unwrap();
    for mode in ["off", "on"] {
        crate::util::env::with_vars(&[("MYDB_VECTORIZED", Some(mode))], || {
            for bound in ["n", "ABS(n)", "n+0.5"] {
                let sql = format!("SELECT 'a' BETWEEN b'1' AND {bound} FROM bounds ORDER BY n");
                assert_eq!(
                    rows(&engine, &mut s, &sql),
                    vec![vec![Value::Int(0)]; 3],
                    "{mode}: {sql}"
                );
            }
            assert!(rows(
                &engine,
                &mut s,
                "SELECT n FROM bounds WHERE 'a' BETWEEN b'1' AND n"
            )
            .is_empty());
            assert_eq!(
                rows(
                    &engine,
                    &mut s,
                    "SELECT 'a' BETWEEN b AND n FROM bounds ORDER BY n"
                ),
                vec![vec![Value::Int(1)]; 3]
            );
            assert_eq!(rows(&engine, &mut s, "SELECT n FROM bounds FORCE INDEX(PRIMARY) WHERE n BETWEEN b'1' AND b'11' ORDER BY n"), vec![vec![Value::Int(1)],vec![Value::Int(2)],vec![Value::Int(3)]]);
        });
    }
    assert_eq!(
        rows(&engine, &mut s, "SELECT 'a' BETWEEN b'1' AND 'z'"),
        vec![vec![Value::Int(1)]]
    );
    for bound in [
        "IF(1,2,'z')",
        "IF(0,2,'z')",
        "CASE WHEN 1 THEN 2 ELSE 'z' END",
    ] {
        let sql = format!("SELECT '1' BETWEEN 0x31 AND {bound}");
        assert_eq!(
            rows(&engine, &mut s, &sql),
            vec![vec![Value::Int(1)]],
            "{sql}"
        );
    }
    for (bound, expected) in [("(SELECT 50)", 0), ("(SELECT 'z')", 1)] {
        let sql = format!("SELECT '1' BETWEEN 0x31 AND {bound}");
        assert_eq!(
            rows(&engine, &mut s, &sql),
            vec![vec![Value::Int(expected)]],
            "{sql}"
        );
    }
    for value in [
        "NULL",
        "'a'",
        "b",
        "CAST('1' AS JSON)",
        "DATE '2024-01-02'",
        "CAST(2020 AS YEAR)",
    ] {
        let sql = format!(
            "SELECT {value} BETWEEN b'1' AND n,{value} BETWEEN b'1' AND 2 FROM bounds WHERE n=2"
        );
        let result = rows(&engine, &mut s, &sql);
        assert_eq!(result[0][0], result[0][1], "{sql}");
    }
    exec(&engine, &mut s, "CREATE TABLE nullable_bounds(n INT,t INT)").unwrap();
    exec(
        &engine,
        &mut s,
        "INSERT INTO nullable_bounds VALUES(NULL,1)",
    )
    .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut s,
            "SELECT 'a' BETWEEN IF(t,b'1',b'1') AND n FROM nullable_bounds"
        ),
        vec![vec![Value::Int(0)]]
    );
    exec(&engine, &mut s, "CREATE TABLE year_bounds(y YEAR,h INT)").unwrap();
    exec(
        &engine,
        &mut s,
        "INSERT INTO year_bounds VALUES(2000,2005),(2020,2025)",
    )
    .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut s,
            "SELECT y BETWEEN b'1' AND h FROM year_bounds ORDER BY y"
        ),
        vec![vec![Value::Int(0)], vec![Value::Int(1)]]
    );
    exec(
        &engine,
        &mut s,
        "PREPARE p FROM 'SELECT ''a'' BETWEEN b''1'' AND ?'",
    )
    .unwrap();
    exec(&engine, &mut s, "SET @bound=2").unwrap();
    assert_eq!(
        rows(&engine, &mut s, "EXECUTE p USING @bound"),
        vec![vec![Value::Int(0)]]
    );
    exec(&engine, &mut s, "DEALLOCATE PREPARE p").unwrap();
    exec(
        &engine,
        &mut s,
        "PREPARE p FROM 'SELECT ''a'' BETWEEN b''1'' AND ?'",
    )
    .unwrap();
    exec(&engine, &mut s, "SET @bound='z'").unwrap();
    assert_eq!(
        rows(&engine, &mut s, "EXECUTE p USING @bound"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn parser_release_1121_table_aliases_and_query_scopes() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE TABLE u(a INT)").unwrap();
    exec(&engine, &mut s, "INSERT INTO u VALUES(1),(2),(3)").unwrap();
    for sql in [
        "SELECT * FROM u,u",
        "SELECT * FROM u x JOIN u x ON 1=1",
        "WITH c AS (SELECT 1) SELECT * FROM c,c",
        "SELECT * FROM (u x JOIN u y USING(a)),u x",
        "SELECT * FROM u,test.u",
        "SELECT * FROM u u,test.u",
    ] {
        assert_eq!(
            exec(&engine, &mut s, sql).unwrap_err().code(),
            1066,
            "{sql}"
        );
    }
    assert_eq!(
        rows(&engine, &mut s, "SELECT * FROM u x JOIN u y USING(a,a)").len(),
        3
    );
    assert_eq!(
        rows(
            &engine,
            &mut s,
            "SELECT x.a FROM (u x JOIN u y ON x.a=y.a),(u z JOIN u w ON z.a=w.a) WHERE x.a=z.a"
        )
        .len(),
        3
    );
    exec(&engine, &mut s, "CREATE DATABASE other_scope1121").unwrap();
    exec(&engine, &mut s, "CREATE TABLE other_scope1121.u(a INT)").unwrap();
    exec(&engine, &mut s, "INSERT INTO other_scope1121.u VALUES(4)").unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut s,
            "SELECT COUNT(*) FROM test.u x,other_scope1121.u x"
        ),
        vec![vec![Value::Int(3)]]
    );
    assert_eq!(
        rows(
            &engine,
            &mut s,
            "SELECT a,(SELECT MAX(a) FROM u x) FROM u x"
        )
        .len(),
        3
    );
    assert_eq!(
        rows(&engine, &mut s, "SELECT * FROM (SELECT 1 a),(SELECT 2 b)"),
        vec![vec![Value::Int(1), Value::Int(2)]]
    );
}

#[test]
fn parser_release_1121_text_width_and_values_order() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(
        &engine,
        &mut s,
        "CREATE TABLE t(a TEXT(100)) CHARSET utf8mb4",
    )
    .unwrap();
    exec(&engine, &mut s, "INSERT INTO t VALUES(REPEAT('x',300))").unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT LENGTH(a) FROM t"),
        vec![vec![Value::Int(300)]]
    );
    let result = rows(&engine, &mut s, "VALUES ROW(2),ROW(1) ORDER BY 1");
    assert_eq!(result, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

#[test]
fn parser_release_1121_text_charset_inheritance_and_alter() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(
        &engine,
        &mut s,
        "CREATE DATABASE encoding1121 CHARSET latin1",
    )
    .unwrap();
    exec(&engine, &mut s, "USE encoding1121").unwrap();
    exec(&engine, &mut s, "CREATE TABLE widths(a TEXT(100),b TEXT(100) CHARSET utf8mb4,c TEXT(65535) CHARSET utf8mb3,d TEXT(16777215) CHARSET utf8mb4,e BLOB(100),f TEXT(100) CHARSET binary)").unwrap();
    for (row, expected) in rows(&engine, &mut s, "SHOW COLUMNS FROM widths")
        .iter()
        .zip([
            "tinytext",
            "text",
            "mediumtext",
            "longtext",
            "tinyblob",
            "tinyblob",
        ])
    {
        assert_eq!(crate::engine::expr::str_value(&row[1]), expected);
    }
    exec(&engine, &mut s, "ALTER TABLE widths MODIFY b TEXT(100)").unwrap();
    exec(
        &engine,
        &mut s,
        "INSERT INTO widths(b) VALUES(REPEAT('x',300))",
    )
    .unwrap();
    exec(
        &engine,
        &mut s,
        "CREATE TEMPORARY TABLE temporary_widths(a TEXT(100)) CHARSET utf8mb4",
    )
    .unwrap();
    exec(
        &engine,
        &mut s,
        "ALTER TABLE temporary_widths ADD b TEXT(100)",
    )
    .unwrap();
    exec(
        &engine,
        &mut s,
        "INSERT INTO temporary_widths VALUES(REPEAT('x',300),REPEAT('x',300))",
    )
    .unwrap();
}

#[test]
fn parser_release_1121_recursive_limits_and_wide_results() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "SET cte_max_recursion_depth=8").unwrap();
    let sql = "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c LIMIT 5) SELECT n FROM c ORDER BY n";
    assert_eq!(
        rows(&engine, &mut s, sql),
        (1..=5).map(|n| vec![Value::Int(n)]).collect::<Vec<_>>()
    );
    let sql = "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<7 UNION ALL SELECT n+1 FROM c WHERE n<7) SELECT COUNT(*) FROM c";
    assert_eq!(rows(&engine, &mut s, sql), vec![vec![Value::Int(127)]]);
    let sql = "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c UNION ALL SELECT n*2 FROM c) SELECT * FROM c";
    assert_eq!(exec(&engine, &mut s, sql).unwrap_err().code(), 3636);
    for (limit, count) in [("LIMIT 0", 0), ("LIMIT 1", 1), ("LIMIT 3 OFFSET 1", 3)] {
        let sql = format!("WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c {limit}) SELECT COUNT(*) FROM c");
        assert_eq!(rows(&engine, &mut s, &sql), vec![vec![Value::Int(count)]]);
    }
    let sql = "WITH RECURSIVE c(n) AS (SELECT 1 UNION SELECT n+1 FROM c WHERE n<3 LIMIT 5) SELECT n FROM c ORDER BY n";
    assert_eq!(
        rows(&engine, &mut s, sql),
        (1..=3).map(|n| vec![Value::Int(n)]).collect::<Vec<_>>()
    );
}

#[test]
fn parser_release_1121_signal_and_date_interval() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    let e = exec(
        &engine,
        &mut s,
        "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='audit',MYSQL_ERRNO=30001",
    )
    .unwrap_err();
    assert_eq!(e.code(), 30001);
    assert_eq!(e.message(), "audit");
    assert_eq!(exec(&engine, &mut s, "RESIGNAL").unwrap_err().code(), 1645);
    assert_eq!(
        rows(
            &engine,
            &mut s,
            "SELECT ('2024-01-02' + INTERVAL 1 DAY * 2) = 40480206"
        ),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(
            &engine,
            &mut s,
            "SELECT CONVERT('abc',CHAR CHARACTER SET utf8mb4)"
        ),
        vec![vec![Value::String("abc".into())]]
    );
}

#[test]
fn parser_release_1121_statement_functionality() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "SET @a.b=3").unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT @a.b"),
        vec![vec![Value::Int(3)]]
    );
    exec(
        &engine,
        &mut s,
        "SET NAMES utf8mb4 COLLATE utf8mb4_bin, CHARACTER SET latin1",
    )
    .unwrap();
    assert_eq!(s.character_set_client, "latin1");
    assert_eq!(s.collation_connection, crate::charset::DEFAULT_COLLATION);
    assert_eq!(
        rows(&engine, &mut s, "SELECT 1 COLLATE utf8mb4_bin"),
        vec![vec![Value::String("1".into())]]
    );
    exec(&engine, &mut s, "CREATE TABLE t(a INT)").unwrap();
    exec(&engine, &mut s, "INSERT INTO t ((((SELECT 1))))").unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT DISTINCT DISTINCT a FROM t"),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(&engine, &mut s, "SELECT (VALUES ROW(1))"),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(&engine, &mut s, "SELECT CAST(1 AS NATIONAL CHAR(3))"),
        vec![vec![Value::String("1".into())]]
    );
    for sql in [
        "SELECT * FROM (SELECT 1 x,2 x) d",
        "SELECT * FROM (SELECT a,a FROM t) d",
        "SELECT * FROM (SELECT * FROM t x JOIN t y) d",
    ] {
        assert!(!rows(&engine, &mut s, sql).is_empty(), "{sql}");
    }
    assert_eq!(
        exec(&engine, &mut s, "SELECT d.x FROM (SELECT 1 x,2 x) d")
            .unwrap_err()
            .code(),
        1052
    );
    exec(&engine, &mut s, "CREATE TABLE iq(a INT)").unwrap();
    for sql in [
        "INSERT INTO iq VALUES ((SELECT 1)),(((SELECT 2)))",
        "INSERT INTO iq VALUE ((SELECT 3))",
        "INSERT INTO iq SET a=((SELECT 4))",
    ] {
        exec(&engine, &mut s, sql).unwrap();
    }
    assert_eq!(
        rows(&engine, &mut s, "SELECT a FROM iq ORDER BY a"),
        (1..=4).map(|n| vec![Value::Int(n)]).collect::<Vec<_>>()
    );
    exec(&engine, &mut s, "CREATE TABLE value(value INT)").unwrap();
    exec(&engine, &mut s, "INSERT INTO value(value) ((((SELECT 5))))").unwrap();
    exec(&engine, &mut s, "INSERT value VALUE ((SELECT 6))").unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SELECT value FROM value ORDER BY value"),
        vec![vec![Value::Int(5)], vec![Value::Int(6)]]
    );
    assert!(matches!(
        exec(&engine, &mut s, "SELECT 1 INTO @v").unwrap(),
        QueryResponse::OkAffected { affected_rows: 1 }
    ));
}
