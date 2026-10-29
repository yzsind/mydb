//! Regression cases from bugs-100-verified.md; assertions describe SQL semantics.
use super::*;

fn rows(e: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(r) = e
        .execute(sql)
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"))
    else {
        panic!("{sql}")
    };
    r.rows
}

#[test]
fn verified100_partition_update_unique_and_atomicity() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute(
        "CREATE TABLE p (id INT, v INT, PRIMARY KEY(id,v)) PARTITION BY HASH(id) PARTITIONS 2",
    )
    .unwrap();
    e.execute("INSERT INTO p VALUES (2,10),(2,20),(1,20)")
        .unwrap();
    for sql in [
        "UPDATE p SET v=20 WHERE id=2 AND v=10",
        "UPDATE p SET id=2 WHERE id=1",
        "UPDATE p SET v=20 WHERE id=2",
    ] {
        assert_eq!(e.execute(sql).unwrap_err().code(), 1062, "{sql}");
        assert_eq!(
            rows(&e, "SELECT id,v FROM p ORDER BY id,v"),
            vec![
                vec![Value::Int(1), Value::Int(20)],
                vec![Value::Int(2), Value::Int(10)],
                vec![Value::Int(2), Value::Int(20)]
            ]
        );
    }
}

#[test]
fn verified100_index_payload_case_update() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE t(id INT PRIMARY KEY,s VARCHAR(20),KEY ks(s))")
        .unwrap();
    e.execute("INSERT INTO t VALUES(1,'a'),(2,'b')").unwrap();
    e.execute("UPDATE t SET s=UPPER(s)").unwrap();
    assert_eq!(
        rows(&e, "SELECT s FROM t FORCE INDEX(ks) ORDER BY s"),
        vec![
            vec![Value::String("A".into())],
            vec![Value::String("B".into())]
        ]
    );
}

#[test]
fn verified100_datetime_rounding_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE t(d DATETIME)").unwrap();
    assert_eq!(
        e.execute("INSERT INTO t VALUES('9999-12-31 23:59:59.5')")
            .unwrap_err()
            .code(),
        1441
    );
    assert!(rows(&e, "SELECT * FROM t").is_empty());
    e.execute("INSERT INTO t VALUES('9999-12-31 23:59:59.4')")
        .unwrap();
}

#[test]
fn verified100_inline_check_and_functional_index_names() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE t(b INT CONSTRAINT cb CHECK(b<100),s VARCHAR(20),INDEX((LOWER(s))),INDEX((UPPER(s))))").unwrap();
    assert_eq!(
        e.execute("INSERT INTO t VALUES(100,'a')")
            .unwrap_err()
            .code(),
        3819
    );
    e.execute("ALTER TABLE t DROP INDEX functional_index")
        .unwrap();
    e.execute("ALTER TABLE t DROP INDEX functional_index_2")
        .unwrap();
    let r = rows(&e, "SHOW CREATE TABLE t");
    let Value::String(s) = &r[0][1] else { panic!() };
    assert_eq!(s.matches("CHECK").count(), 1);
}

#[test]
fn verified100_ctas_auto_increment_default() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE t(id INT PRIMARY KEY AUTO_INCREMENT,v INT)")
        .unwrap();
    e.execute("INSERT INTO t(v) VALUES(1)").unwrap();
    e.execute("CREATE TABLE c AS SELECT * FROM t").unwrap();
    e.execute("INSERT INTO c(v) VALUES(2)").unwrap();
    assert_eq!(
        rows(&e, "SELECT id FROM c ORDER BY id"),
        vec![vec![Value::Int(0)], vec![Value::Int(1)]]
    );
}

#[test]
fn verified100_view_survives_compatible_alter_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE t(id INT,a INT)").unwrap();
    e.execute("INSERT INTO t VALUES(1,10)").unwrap();
    e.execute("CREATE VIEW v AS SELECT id,a,a*2 AS b FROM t")
        .unwrap();
    e.execute("CREATE VIEW star AS SELECT * FROM t").unwrap();
    e.execute("ALTER TABLE t ADD COLUMN c INT").unwrap();
    assert_eq!(
        rows(&e, "SELECT * FROM v"),
        vec![vec![Value::Int(1), Value::Int(10), Value::Int(20)]]
    );
    assert_eq!(rows(&e, "SELECT * FROM star")[0].len(), 2);
    e.execute("ALTER TABLE t DROP COLUMN a").unwrap();
    assert_eq!(e.execute("SELECT * FROM v").unwrap_err().code(), 1356);
    e.execute("ALTER TABLE t ADD COLUMN a INT DEFAULT 7")
        .unwrap();
    assert_eq!(
        rows(&e, "SELECT * FROM v"),
        vec![vec![Value::Int(1), Value::Int(7), Value::Int(14)]]
    );
}

#[test]
fn verified100_window_collation_and_expression() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE t(c VARCHAR(10),b VARCHAR(10) COLLATE utf8mb4_bin,v INT)")
        .unwrap();
    e.execute("INSERT INTO t VALUES('a','a',1),('A','A',2),('b','b',4)")
        .unwrap();
    let r = rows(&e, "SELECT SUM(v) OVER(ORDER BY c) FROM t ORDER BY v");
    assert_eq!(
        r.iter()
            .map(|r| crate::storage::value_to_string(&r[0]))
            .collect::<Vec<_>>(),
        vec!["3", "3", "7"]
    );
    assert_eq!(
        rows(
            &e,
            "SELECT ROW_NUMBER() OVER(ORDER BY b),COUNT(*) OVER(PARTITION BY b) FROM t ORDER BY v"
        ),
        vec![
            vec![Value::Int(2), Value::Int(1)],
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(3), Value::Int(1)]
        ]
    );
    assert_eq!(
        rows(
            &e,
            "SELECT COUNT(*) OVER(PARTITION BY v%2) FROM t ORDER BY v"
        ),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(2)]
        ]
    );
}

#[test]
fn verified100_rollup_null_groups() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE t(id INT PRIMARY KEY,g VARCHAR(10),v INT)")
        .unwrap();
    e.execute("INSERT INTO t VALUES(1,'a',1),(2,NULL,2),(3,'b',3)")
        .unwrap();
    for _ in 0..10 {
        let r = rows(&e, "SELECT g FROM t GROUP BY g WITH ROLLUP");
        assert_eq!(r.iter().filter(|r| r[0] == Value::Null).count(), 2, "{r:?}");
        assert_eq!(r.len(), 4);
    }
}

#[test]
fn verified100_scalar_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    let cases = [
        ("CAST('24-01-02' AS DATE)", "2024-01-02"),
        ("YEAR('2024/01/02')", "2024"),
        ("MAKEDATE(2024,367)", "2025-01-01"),
        ("MAKEDATE(11,31)", "2011-01-31"),
        ("CAST(1030 AS TIME)", "00:10:30"),
        ("CAST('10:70:00' AS TIME)", "NULL"),
        ("CAST('10:30:00' AS DATETIME)", "NULL"),
        ("LPAD('hello',10,'')", ""),
        ("SOUNDEX('中')", "中000"),
        ("ORD(BINARY '中')", "228"),
        ("TIME_FORMAT('25:00:00','%p')", "AM"),
        ("CONVERT_TZ('2024-01-01','bad','UTC')", "NULL"),
    ];
    for (expr, want) in cases {
        let r = rows(&e, &format!("SELECT {expr}"));
        assert_eq!(crate::storage::value_to_string(&r[0][0]), want, "{expr}");
    }
    e.execute("CREATE TABLE t(a INT)").unwrap();
    e.execute("INSERT INTO t VALUES(NULL),(1)").unwrap();
    assert_eq!(
        rows(
            &e,
            "SELECT a FROM t WHERE a NOT IN(SELECT a FROM t WHERE 1=0)"
        )
        .len(),
        2
    );
}

#[test]
fn verified100_prepared_limit_source_order_and_negative() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE t(id INT)",
        "INSERT INTO t VALUES(1),(2),(3),(4),(5)",
        "PREPARE p FROM 'SELECT id FROM t ORDER BY id LIMIT ?,?'",
        "SET @o=2,@n=3",
    ] {
        e.execute_with_session(&mut s, sql).unwrap();
    }
    let QueryResponse::ResultSet(r) = e
        .execute_with_session(&mut s, "EXECUTE p USING @o,@n")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int(3)],
            vec![Value::Int(4)],
            vec![Value::Int(5)]
        ]
    );
    e.execute_with_session(&mut s, "SET @n=-1").unwrap();
    assert!(e
        .execute_with_session(&mut s, "EXECUTE p USING @o,@n")
        .is_err());
}

#[test]
fn verified100_grouping_sets_binary_totals() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute(
        "CREATE TABLE t(a VARCHAR(10) COLLATE utf8mb4_bin,b VARCHAR(10) COLLATE utf8mb4_bin,v INT)",
    )
    .unwrap();
    e.execute("INSERT INTO t VALUES('x','p',1),('X','p',2),('x','q',3)")
        .unwrap();
    let r = rows(&e, "SELECT a,b,SUM(v) FROM t GROUP BY a,b WITH ROLLUP");
    assert_eq!(r.len(), 6, "{r:?}");
    let r = rows(
        &e,
        "SELECT a,b,SUM(v) FROM t GROUP BY GROUPING SETS((a,b),(a),(b),())",
    );
    assert_eq!(r.len(), 8, "{r:?}");
    let total = r
        .iter()
        .find(|r| r[0] == Value::Null && r[1] == Value::Null)
        .unwrap();
    assert_eq!(crate::storage::value_to_string(&total[2]), "6");
}

#[test]
fn verified100_json_empty_and_invalid() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    assert_eq!(
        rows(
            &e,
            "SELECT JSON_ARRAYAGG(id) FROM (SELECT 1 AS id) t WHERE id>100"
        ),
        vec![vec![Value::Null]]
    );
    e.execute("CREATE TABLE t(id INT,j VARCHAR(20))").unwrap();
    e.execute("INSERT INTO t VALUES(1,'bad')").unwrap();
    assert_eq!(
        rows(
            &e,
            "SELECT JSON_ARRAYAGG(id),JSON_OBJECTAGG(id,j) FROM t WHERE id>100"
        ),
        vec![vec![Value::Null, Value::Null]]
    );
    assert_eq!(
        e.execute("SELECT j->'$.a' FROM t").unwrap_err().code(),
        3141
    );
    assert_eq!(
        e.execute("SELECT JSON_REMOVE('{}','$')")
            .unwrap_err()
            .code(),
        3153
    );
}

#[test]
fn verified100_tuple_subquery_null_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE a(x INT,y INT)").unwrap();
    e.execute("CREATE TABLE b(x INT,y INT)").unwrap();
    e.execute("INSERT INTO a VALUES(1,1),(1,2),(2,1),(NULL,3)")
        .unwrap();
    e.execute("INSERT INTO b VALUES(1,1),(2,1),(3,NULL)")
        .unwrap();
    assert_eq!(
        rows(
            &e,
            "SELECT x,y FROM a WHERE (x,y) IN(SELECT x,y FROM b) ORDER BY x,y"
        ),
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Int(1)]
        ]
    );
    assert_eq!(
        rows(
            &e,
            "SELECT x,y FROM a WHERE (x,y) NOT IN(SELECT x,y FROM b WHERE 1=0)"
        )
        .len(),
        4
    );
}

#[test]
fn verified100_date_write_and_json_types() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE t(d DATE,dt DATETIME)").unwrap();
    e.execute("INSERT INTO t VALUES('24-01-02','99-12-31 23:59:59')")
        .unwrap();
    let r = rows(&e, "SELECT d,dt FROM t");
    assert_eq!(crate::storage::value_to_string(&r[0][0]), "2024-01-02");
    assert_eq!(
        crate::storage::value_to_string(&r[0][1]),
        "1999-12-31 23:59:59"
    );
    let cases = [
        (
            "JSON_EXTRACT('[[1,2],[3,4]]','$**[0]')",
            "[[1, 2], 1, 2, 3, 4]",
        ),
        ("JSON_ARRAY(BINARY 'ABC')", "[\"base64:type15:QUJD\"]"),
        (
            "JSON_ARRAY(TIMESTAMP '2020-01-01 10:00:00')",
            "[\"2020-01-01 10:00:00.000000\"]",
        ),
        ("JSON_VALUE('bad','$.x')", "NULL"),
        (
            "JSON_VALUE('{\"a\":{\"b\":1}}','$.a' RETURNING CHAR(20))",
            "{\"b\": 1}",
        ),
        ("JSON_EXTRACT('[1e-5]','$[0]')", "0.00001"),
    ];
    for (expr, want) in cases {
        assert_eq!(
            crate::storage::value_to_string(&rows(&e, &format!("SELECT {expr}"))[0][0]),
            want,
            "{expr}"
        );
    }
    assert_eq!(
        e.execute("SELECT JSON_QUOTE(BINARY 'x')")
            .unwrap_err()
            .code(),
        3144
    );
}

#[test]
fn verified100_function_and_index_validation() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    for (expr, want) in [
        ("LOWER('ABC' COLLATE utf8mb4_bin)", "abc"),
        ("HEX(CHAR(-1))", "FFFFFFFF"),
        ("CHAR(67.9)", "D"),
        ("SUBSTRING_INDEX('a,b,c',',',1.5)", "a,b"),
        ("FORMAT(1234567.89,2,NULL)", "1,234,567.89"),
        ("FORMAT(1234567.89,2,'ar_SA')", "1234567.89"),
        ("REGEXP_REPLACE('abc','(b)','[$1]',1,1)", "a[b]c"),
    ] {
        assert_eq!(
            crate::storage::value_to_string(&rows(&e, &format!("SELECT {expr}"))[0][0]),
            want,
            "{expr}"
        );
    }
    assert_eq!(
        e.execute("SELECT REGEXP_LIKE('a','')").unwrap_err().code(),
        3685
    );
    assert_eq!(
        e.execute("SELECT REGEXP_LIKE('a','a','q')")
            .unwrap_err()
            .code(),
        1210
    );
    e.execute("CREATE TABLE t(i INT,g GEOMETRY)").unwrap();
    assert_eq!(
        e.execute("CREATE FULLTEXT INDEX fi ON t(i)")
            .unwrap_err()
            .code(),
        1283
    );
    assert_eq!(
        e.execute("ALTER TABLE t ADD SPATIAL INDEX(g)")
            .unwrap_err()
            .code(),
        1252
    );
}

#[test]
fn verified100_relational_scope_and_union_types() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE t(id INT,mgr INT)").unwrap();
    e.execute("INSERT INTO t VALUES(1,NULL),(2,1),(3,1),(4,2)")
        .unwrap();
    assert_eq!(
        rows(
            &e,
            "SELECT COUNT(*) FROM (t a JOIN t b ON a.id=b.id),t c WHERE c.mgr=a.id"
        ),
        vec![vec![Value::Int(3)]]
    );
    assert_eq!(
        rows(&e, "SELECT 2 AS v UNION SELECT '11' ORDER BY v"),
        vec![
            vec![Value::String("11".into())],
            vec![Value::String("2".into())]
        ]
    );
    e.execute("CREATE TABLE h(a INT,b INT)").unwrap();
    e.execute("INSERT INTO h VALUES(1,10),(2,20)").unwrap();
    assert_eq!(
        rows(&e, "SELECT a AS b FROM h GROUP BY a,b HAVING b>15"),
        vec![vec![Value::Int(2)]]
    );
    e.execute("EXPLAIN FORMAT=TRADITIONAL SELECT 1").unwrap();
    e.execute("SELECT * FROM h LOCK IN SHARE MODE").unwrap();
}

#[test]
fn verified100_json_table_siblings_and_error_policy() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    let r=rows(&e,"SELECT * FROM JSON_TABLE('[{\"a\":[1]},{\"b\":[2]}]','$[*]' COLUMNS(NESTED PATH '$.a[*]' COLUMNS(a INT PATH '$'),NESTED PATH '$.b[*]' COLUMNS(b INT PATH '$'))) j");
    assert_eq!(
        r,
        vec![
            vec![Value::Int(1), Value::Null],
            vec![Value::Null, Value::Int(2)]
        ]
    );
    let r=rows(&e,"SELECT * FROM JSON_TABLE('[{\"x\":1},{\"x\":\"bad\"},{}]','$[*]' COLUMNS(v INT PATH '$.x' DEFAULT '-2' ON EMPTY DEFAULT '-1' ON ERROR)) j");
    assert_eq!(
        r,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(-1)],
            vec![Value::Int(-2)]
        ]
    );
}

#[test]
fn verified100_charset_and_remaining_scalar_paths() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE t(id INT,s VARCHAR(10) CHARACTER SET latin1)")
        .unwrap();
    e.execute("INSERT INTO t VALUES(1,'café'),(2,'ABC'),(3,'abc')")
        .unwrap();
    assert_eq!(
        rows(
            &e,
            "SELECT HEX(s),LENGTH(s),CHAR_LENGTH(s) FROM t WHERE id=1"
        ),
        vec![vec![
            Value::String("636166E9".into()),
            Value::Int(4),
            Value::Int(4)
        ]]
    );
    assert_eq!(
        rows(&e, "SELECT id FROM t WHERE s LIKE BINARY 'a%'"),
        vec![vec![Value::Int(3)]]
    );
    for (sql, want) in [
        ("SELECT EXTRACT(HOUR_MINUTE FROM '03:04')", "304"),
        ("SELECT EXTRACT(DAY_HOUR FROM '2024-01-02 03')", "203"),
        ("SELECT -(-9223372036854775808)", "9223372036854775808"),
        ("SELECT TRUNCATE(1e15,-15)", "1e15"),
        ("SELECT TO_DAYS('0000-01-01')", "1"),
        ("SELECT FROM_DAYS(365)", "0000-12-31"),
    ] {
        assert_eq!(
            crate::storage::value_to_string(&rows(&e, sql)[0][0]),
            want,
            "{sql}"
        );
    }
}
