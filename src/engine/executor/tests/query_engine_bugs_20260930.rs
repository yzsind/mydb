use super::*;

fn setup(sqls: &[&str]) -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in sqls {
        run(&engine, &mut session, sql);
    }
    (dir, engine, session)
}
fn run(e: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> QueryResponse {
    <StorageQueryEngine as QueryEngine>::execute(e, s, sql)
        .and_then(QueryResponse::materialize)
        .expect(sql)
}
fn rows(e: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> Vec<Vec<String>> {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let mut result = None;
    for mode in [VecMode::Off, VecMode::On] {
        let QueryResponse::ResultSet(rs) = with_vec_mode(mode, || run(e, s, sql)) else {
            panic!("{sql}")
        };
        let actual: Vec<Vec<String>> = rs
            .rows
            .iter()
            .map(|r| r.iter().map(Value::to_display_string).collect())
            .collect();
        if let Some(expected) = &result {
            if !sql.contains("RAND()") {
                assert_eq!(&actual, expected, "row/vector mismatch: {sql}");
            }
        }
        result = Some(actual);
    }
    result.unwrap()
}
#[test]
fn qe0930_decimal_wide_sum() {
    let (_d,e,mut s)=setup(&["CREATE TABLE t(g INT,v DECIMAL(38,0))", "INSERT INTO t VALUES(1,99999999999999999999999999999999999999),(1,99999999999999999999999999999999999999),(1,NULL),(2,-99999999999999999999999999999999999999),(2,-99999999999999999999999999999999999999)"]);
    for suffix in ["WHERE g=1", "WHERE g=1 GROUP BY g"] {
        assert_eq!(
            rows(
                &e,
                &mut s,
                &format!("SELECT SUM(v),AVG(v),COUNT(v) FROM t {suffix}")
            ),
            vec![vec![
                "199999999999999999999999999999999999998",
                "99999999999999999999999999999999999999.0000",
                "2"
            ]]
        );
    }
    assert_eq!(
        rows(&e, &mut s, "SELECT SUM(v) FROM t WHERE g=2")[0][0],
        "-199999999999999999999999999999999999998"
    );
    assert_eq!(rows(&e, &mut s, "SELECT SUM(v) FROM t")[0][0], "0");
}
#[test]
fn qe0930_derived_join_scope() {
    let (_d, e, mut s) = setup(&[
        "CREATE TABLE o(x INT)",
        "CREATE TABLE t1(k INT)",
        "CREATE TABLE t2(a INT)",
        "INSERT INTO o VALUES(1),(2)",
        "INSERT INTO t1 VALUES(1),(2)",
        "INSERT INTO t2 VALUES(1),(2),(3)",
    ]);
    assert_eq!(rows(&e,&mut s,"SELECT o.x,(SELECT COUNT(*) FROM t1 JOIN (SELECT a AS x FROM t2) d ON d.x=t1.k WHERE x=o.x) FROM o ORDER BY o.x"),vec![vec!["1","1"],vec!["2","1"]]);
}
#[test]
fn qe0930_dml_collation() {
    let (_d, e, mut s) = setup(&[
        "CREATE TABLE t(id INT PRIMARY KEY,name VARCHAR(10) COLLATE utf8mb4_bin)",
        "INSERT INTO t VALUES(1,'b'),(2,'B'),(3,'a')",
    ]);
    run(&e, &mut s, "UPDATE t SET name='X' ORDER BY name LIMIT 1");
    assert_eq!(
        rows(&e, &mut s, "SELECT id FROM t WHERE name='X'"),
        vec![vec!["2"]]
    );
    run(&e, &mut s, "DELETE FROM t ORDER BY name LIMIT 1");
    assert_eq!(
        rows(&e, &mut s, "SELECT id FROM t ORDER BY id"),
        vec![vec!["1"], vec!["3"]]
    );
}
#[test]
fn qe0930_temporal_numeric_index() {
    let (_d,e,mut s)=setup(&["CREATE TABLE t(id INT PRIMARY KEY,d DATE,dt DATETIME,tm TIME,KEY(d),KEY(dt),KEY(tm))","INSERT INTO t VALUES(1,'2024-01-01','2024-01-01 10:00:00','10:00:00'),(2,'2024-01-02','2024-01-02 10:00:00','23:59:59')"]);
    for pred in [
        "d=20240101",
        "dt=20240101100000",
        "tm=100000",
        "d BETWEEN 20240101 AND 20240101",
        "tm>=100000 AND tm<110000",
    ] {
        assert_eq!(
            rows(&e, &mut s, &format!("SELECT id FROM t WHERE {pred}")),
            vec![vec!["1"]],
            "{pred}"
        );
    }
}
#[test]
fn qe0930_partition_bounds() {
    let (_d,e,mut s)=setup(&["CREATE TABLE t(a INT,b VARCHAR(10)) PARTITION BY RANGE COLUMNS(a,b)(PARTITION p0 VALUES LESS THAN(5,'a'),PARTITION p1 VALUES LESS THAN(5,'m'),PARTITION p2 VALUES LESS THAN(5,'z'),PARTITION p3 VALUES LESS THAN(9,'a'))","INSERT INTO t VALUES(1,'x'),(5,'b'),(5,'q'),(5,'zz')"]);
    for pred in ["a<=5", "a BETWEEN 1 AND 5", "a>'0' AND a<=5"] {
        assert_eq!(
            rows(&e, &mut s, &format!("SELECT COUNT(*) FROM t WHERE {pred}")),
            vec![vec!["4"]],
            "{pred}"
        );
    }
    run(&e,&mut s,"CREATE TABLE r(a INT) PARTITION BY RANGE(a)(PARTITION p0 VALUES LESS THAN(5),PARTITION p1 VALUES LESS THAN(10),PARTITION p2 VALUES LESS THAN(100))");
    run(&e, &mut s, "INSERT INTO r VALUES(3),(5),(7),(50)");
    for pred in ["a>'5' AND a<10", "a>5 AND a<'10'", "a BETWEEN '6' AND 9"] {
        assert_eq!(
            rows(&e, &mut s, &format!("SELECT a FROM r WHERE {pred}")),
            vec![vec!["7"]],
            "{pred}"
        );
    }
}
#[test]
fn qe0930_date_arithmetic_string() {
    let (_d, e, mut s) = setup(&[
        "CREATE TABLE t(ts TIMESTAMP NULL,dur INT)",
        "INSERT INTO t VALUES('2023-01-01',0)",
    ]);
    for pred in ["DATE(ts)+dur<'2023-11-08'", "'2023-11-08'>DATE(ts)+dur"] {
        assert!(rows(&e, &mut s, &format!("SELECT * FROM t WHERE {pred}")).is_empty());
    }
}
#[test]
fn qe0930_volatile_subquery() {
    let (_d, e, mut s) = setup(&[
        "CREATE TABLE t(x INT)",
        "INSERT INTO t VALUES(1),(2),(3),(4),(5),(6),(7),(8)",
    ]);
    let result = rows(
        &e,
        &mut s,
        "SELECT (SELECT x FROM t ORDER BY RAND() LIMIT 1) FROM t a CROSS JOIN t b",
    );
    assert!(
        result.iter().any(|r| r != &result[0]),
        "RAND subquery was cached"
    );
}
#[test]
fn qe0930_group_concat_collation() {
    let (_d, e, mut s) = setup(&[
        "CREATE TABLE t(s VARCHAR(20) COLLATE utf8mb4_bin)",
        "INSERT INTO t VALUES('a'),('A'),('a'),(NULL)",
    ]);
    assert_eq!(
        rows(
            &e,
            &mut s,
            "SELECT COUNT(DISTINCT s),GROUP_CONCAT(DISTINCT s ORDER BY s) FROM t"
        ),
        vec![vec!["2", "A,a"]]
    );
    assert_eq!(
        rows(
            &e,
            &mut s,
            "SELECT GROUP_CONCAT(DISTINCT s COLLATE utf8mb4_bin ORDER BY s) FROM t"
        ),
        vec![vec!["A,a"]]
    );
    assert_eq!(
        rows(
            &e,
            &mut s,
            "SELECT GROUP_CONCAT(DISTINCT s COLLATE utf8mb4_0900_ai_ci) FROM t"
        )[0][0]
            .split(',')
            .count(),
        1
    );
}
#[test]
fn qe0930_null_union_metadata() {
    let (_d, e, mut s) = setup(&[]);
    for sql in ["SELECT 1 UNION SELECT NULL", "SELECT NULL UNION SELECT 1"] {
        let QueryResponse::ResultSet(rs) = run(&e, &mut s, sql) else {
            panic!()
        };
        assert_eq!(rs.columns[0].column_type, ColumnType::LongLong, "{sql}");
        assert!(rs.rows.iter().any(|r| matches!(r[0], Value::Null)));
    }
}
#[test]
fn qe0930_scalar_subquery_metadata() {
    let (_d, e, mut s) = setup(&[]);
    let QueryResponse::ResultSet(rs) = run(&e, &mut s, "SELECT (SELECT 1)+(SELECT 2)") else {
        panic!()
    };
    assert_eq!(rs.columns[0].column_type, ColumnType::LongLong);
    assert_eq!(rs.rows, vec![vec![Value::Int(3)]]);
}
#[test]
fn qe0930_validation() {
    let (_d, e, mut s) = setup(&[
        "CREATE TABLE t(a INT,v INT)",
        "INSERT INTO t VALUES(1,1),(2,2)",
    ]);
    for (sql, code) in [
        ("SELECT DATE '2024-02-30'", 1525),
        ("SELECT a,COUNT(*) FROM t", 1140),
        ("SELECT a,COUNT(*) FROM t GROUP BY v>0", 1055),
        ("SELECT DISTINCT a FROM t ORDER BY v", 3065),
    ] {
        let err = <StorageQueryEngine as QueryEngine>::execute(&e, &mut s, sql)
            .and_then(QueryResponse::materialize)
            .expect_err(sql);
        assert_eq!(err.code(), code, "{sql}: {err}");
    }
}

#[test]
fn qe0930_preserved_extensions() {
    let (_d, e, mut s) = setup(&[
        "CREATE TABLE t(id INT PRIMARY KEY,v INT,tm TIME)",
        "INSERT INTO t VALUES(1,1,'10:00:00'),(2,2,'23:59:59')",
    ]);
    assert_eq!(
        rows(
            &e,
            &mut s,
            "SELECT COUNT(*) FROM t WHERE tm>'09:59:59.999999'"
        ),
        vec![vec!["2"]]
    );
    run(&e, &mut s, "ALTER TABLE t ADD KEY(tm)");
    assert_eq!(
        rows(
            &e,
            &mut s,
            "SELECT COUNT(*) FROM t WHERE tm>'09:59:59.999999'"
        ),
        vec![vec!["2"]]
    );
    assert_eq!(
        rows(&e, &mut s, "SELECT TIME '10:00:00'='10:00:00.000000'"),
        vec![vec!["1"]]
    );
    run(&e, &mut s, "UPDATE t SET v=(SELECT MAX(v) FROM t)+1");
    assert_eq!(
        rows(&e, &mut s, "SELECT v FROM t ORDER BY id"),
        vec![vec!["3"], vec!["3"]]
    );
    assert_eq!(
        rows(&e, &mut s, "SELECT PI()"),
        vec![vec!["3.141592653589793"]]
    );
    run(&e, &mut s, "SET sql_mode=''");
    assert_eq!(rows(&e, &mut s, "SELECT id,COUNT(*) FROM t")[0][1], "2");
}
#[test]
fn qe0930_group_concat_metadata() {
    let (_d, e, mut s) = setup(&[
        "CREATE TABLE t(s VARCHAR(20) COLLATE utf8mb4_bin,b VARBINARY(20))",
        "INSERT INTO t VALUES('a','a')",
    ]);
    for (sql, collation) in [
        ("SELECT GROUP_CONCAT(s) FROM t", 46),
        ("SELECT GROUP_CONCAT(s ORDER BY s) FROM t", 46),
        ("SELECT GROUP_CONCAT(b) FROM t", 63),
    ] {
        let QueryResponse::ResultSet(rs) = run(&e, &mut s, sql) else {
            panic!()
        };
        assert_eq!(rs.columns[0].source.collation_id, Some(collation), "{sql}");
    }
}
#[test]
fn qe0930_distinct_order_valid() {
    let (_d, e, mut s) = setup(&[
        "CREATE TABLE t(a INT,b INT)",
        "INSERT INTO t VALUES(1,3),(2,2)",
    ]);
    for sql in [
        "SELECT DISTINCT a FROM t ORDER BY a+1",
        "SELECT DISTINCT a+b AS c FROM t ORDER BY c",
        "SELECT DISTINCT a+b FROM t ORDER BY a+b",
        "SELECT DISTINCT * FROM t ORDER BY b",
        "SELECT DISTINCT t.a FROM t ORDER BY a",
        "SELECT DISTINCT a+b FROM t ORDER BY t.a+t.b",
        "SELECT DISTINCT a FROM t ORDER BY (SELECT 1)",
    ] {
        run(&e, &mut s, sql);
    }
}

#[test]
fn qe0930_dml_external_sort_collation() {
    let (_d, e, mut s) = setup(&[
        "CREATE TABLE t(id INT PRIMARY KEY,name VARCHAR(10) COLLATE utf8mb4_bin)",
        "SET sort_buffer_size=32768",
    ]);
    let values = (1..=5100)
        .map(|id| format!("({id},'{}')", if id <= 5000 { "a" } else { "B" }))
        .collect::<Vec<_>>()
        .join(",");
    run(&e, &mut s, &format!("INSERT INTO t VALUES {values}"));
    run(&e, &mut s, "DELETE FROM t ORDER BY name,id LIMIT 5000");
    assert_eq!(
        rows(&e, &mut s, "SELECT COUNT(*),MIN(id),MAX(id) FROM t"),
        vec![vec!["100", "4901", "5000"]]
    );
}
