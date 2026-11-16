//! End-to-end regressions for doc_old/executor-bugs-audit.md.
use super::*;
fn run(setup: &[&str], sql: &str, expected: &[&[&str]]) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for statement in setup {
        engine.execute(statement).unwrap();
    }
    let response = engine.execute(sql).unwrap().materialize().unwrap();
    let QueryResponse::ResultSet(result) = response else {
        panic!("{sql}")
    };
    for row in &result.rows {
        assert_eq!(row.len(), result.columns.len(), "protocol row width: {sql}");
    }
    let actual: Vec<Vec<String>> = result
        .rows
        .iter()
        .map(|r| r.iter().map(crate::storage::value_to_string).collect())
        .collect();
    assert_eq!(actual, expected, "{sql}");
}

#[test]
fn exec_audit_bytes_count() {
    run(
        &[
            "CREATE TABLE t(id INT, b VARBINARY(64))",
            "INSERT INTO t VALUES (1,NULL),(2,0x6162),(3,NULL)",
        ],
        "SELECT COUNT(b) FROM t",
        &[&["1"]],
    );
}

#[test]
fn exec_audit_double_decimal_join() {
    run(
        &[
            "CREATE TABLE a(k DOUBLE)",
            "CREATE TABLE b(k DECIMAL(10,1))",
            "INSERT INTO a VALUES (1.5)",
            "INSERT INTO b VALUES (1.5)",
        ],
        "SELECT COUNT(*) FROM a JOIN b ON a.k=b.k",
        &[&["1"]],
    );
}

#[test]
fn exec_audit_date_datetime_join() {
    run(
        &[
            "CREATE TABLE a(k DATE)",
            "CREATE TABLE b(k DATETIME)",
            "INSERT INTO a VALUES ('2024-01-01')",
            "INSERT INTO b VALUES ('2024-01-01 00:00:00')",
        ],
        "SELECT COUNT(*) FROM a JOIN b ON a.k=b.k",
        &[&["1"]],
    );
}

#[test]
fn exec_audit_distinct_limit() {
    run(&["CREATE TABLE t1(a INT,b INT)","CREATE TABLE t2(a INT,c INT)","INSERT INTO t1 VALUES (1,10),(1,11),(2,20)","INSERT INTO t2 VALUES (1,100),(2,200)"], "SELECT DISTINCT d.a FROM t1 JOIN t2 ON t1.a=t2.a JOIN LATERAL (SELECT t1.a AS a) d ON TRUE LIMIT 2", &[&["1"],&["2"]]);
}

#[test]
fn exec_audit_rank_peers() {
    run(
        &["CREATE TABLE t(id INT)", "INSERT INTO t VALUES (1),(2),(3)"],
        "SELECT RANK() OVER (), DENSE_RANK() OVER () FROM t",
        &[&["1", "1"], &["1", "1"], &["1", "1"]],
    );
}

#[test]
fn exec_audit_memo_binary() {
    run(
        &[
            "CREATE TABLE tm(s VARCHAR(10) COLLATE utf8mb4_bin)",
            "INSERT INTO tm VALUES ('A'),('a')",
            "CREATE TABLE tl(s VARCHAR(10) COLLATE utf8mb4_bin,n INT)",
            "INSERT INTO tl VALUES ('A',10),('B',20)",
        ],
        "SELECT m.s,(SELECT n FROM tl WHERE tl.s=m.s) FROM tm m",
        &[&["A", "10"], &["a", "NULL"]],
    );
}

#[test]
fn exec_audit_not_in_null() {
    run(
        &[
            "CREATE TABLE t(x INT)",
            "INSERT INTO t VALUES (1),(2),(NULL)",
        ],
        "SELECT * FROM t WHERE x NOT IN (2,NULL)",
        &[],
    );
}

#[test]
fn exec_audit_having_rows() {
    run(
        &[
            "CREATE TABLE t1(a INT,b INT)",
            "CREATE TABLE t2(a INT,c INT)",
            "INSERT INTO t1 VALUES (1,10),(1,11),(2,20)",
            "INSERT INTO t2 VALUES (1,100),(2,200)",
        ],
        "SELECT t1.a FROM (t1 JOIN t2 ON t1.a=t2.a) JOIN (SELECT 1 AS x) d ON d.x=1 HAVING t1.a>0",
        &[&["1"], &["1"], &["2"]],
    );
}

#[test]
fn exec_audit_join_order() {
    run(
        &[
            "CREATE TABLE l(k INT PRIMARY KEY,a INT)",
            "CREATE TABLE r(k INT)",
            "INSERT INTO l VALUES (1,3),(2,1),(3,2)",
            "INSERT INTO r VALUES (2)",
        ],
        "SELECT l.k,r.k FROM l LEFT JOIN r ON l.k<>r.k ORDER BY r.k,l.k",
        &[&["2", "NULL"], &["1", "2"], &["3", "2"]],
    );
}

#[test]
fn exec_audit_anti_neq() {
    run(&["CREATE TABLE tdec(k INT,c DECIMAL(10,1))","CREATE TABLE sdbl(k INT,d DOUBLE)","INSERT INTO tdec VALUES (1,1.5)","INSERT INTO sdbl VALUES (1,1.5)"], "SELECT k FROM sdbl WHERE NOT EXISTS (SELECT 1 FROM tdec WHERE tdec.k=sdbl.k AND tdec.c<>sdbl.d)", &[&["1"]]);
}

#[test]
fn exec_audit_union_numeric() {
    run(&[], "SELECT 1.5 UNION SELECT 1.5e0", &[&["1.5"]]);
}

#[test]
fn exec_audit_union_year() {
    run(
        &["CREATE TABLE t(yr YEAR)", "INSERT INTO t VALUES (2024)"],
        "SELECT 2024 FROM DUAL UNION SELECT yr FROM t",
        &[&["2024"]],
    );
}

#[test]
fn exec_audit_const_having() {
    run(&[], "SELECT 1 HAVING 0", &[]);
}

#[test]
fn exec_audit_empty_right_join() {
    run(
        &[
            "CREATE TABLE e(id INT)",
            "CREATE TABLE e2(id INT,x INT)",
            "INSERT INTO e2 VALUES (5,55)",
        ],
        "SELECT * FROM (e JOIN e2 ON e.id=e2.id) RIGHT JOIN (SELECT 1 AS x) b ON b.x=e2.id",
        &[&["NULL", "NULL", "NULL", "1"]],
    );
}

#[test]
fn exec_audit_lag_default() {
    run(
        &[
            "CREATE TABLE t(id INT,p INT)",
            "INSERT INTO t VALUES (1,10),(2,20)",
        ],
        "SELECT id,LAG(id,1,p) OVER (ORDER BY id),LEAD(id,1,p+1) OVER (ORDER BY id) FROM t",
        &[&["1", "10", "2"], &["2", "1", "21"]],
    );
}

#[test]
fn exec_audit_topn_ties() {
    run(
        &[
            "CREATE TABLE t(k INT,v VARCHAR(5))",
            "INSERT INTO t VALUES (1,'z'),(1,'a'),(2,'m')",
        ],
        "SELECT v FROM t ORDER BY k LIMIT 1",
        &[&["z"]],
    );
}

#[test]
fn exec_audit_in_arity() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "SELECT 1 IN (SELECT 1,2)",
        "SELECT 1 NOT IN (SELECT 1,2 WHERE 0)",
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 1241);
    }
}

#[test]
fn exec_audit_wide_ambiguity() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(engine.execute("SELECT d.a FROM (SELECT 1 AS a,2 AS a,3 AS b,4 AS c,5 AS d,6 AS e,7 AS f,8 AS g,9 AS h,10 AS i) d").unwrap_err().code(), 1052);
}

#[test]
fn exec_audit_binary_groups_large() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(k VARCHAR(50) COLLATE utf8mb4_bin)")
        .unwrap();
    let values = (0..1000)
        .map(|i| format!("('{}{}')", if i < 500 { "k" } else { "K" }, i % 500))
        .collect::<Vec<_>>()
        .join(",");
    for _ in 0..40 {
        engine
            .execute(&format!("INSERT INTO t VALUES {values}"))
            .unwrap();
    }
    for sql in [
        "SELECT COUNT(*) FROM (SELECT DISTINCT k FROM t) q",
        "SELECT COUNT(*),MIN(c),MAX(c) FROM (SELECT k,COUNT(*) c FROM t GROUP BY k) q",
    ] {
        let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
        else {
            panic!()
        };
        assert_eq!(
            crate::storage::value_to_string(&result.rows[0][0]),
            "1000",
            "{sql}"
        );
        if result.rows[0].len() > 1 {
            assert_eq!(result.rows[0][1..], [Value::Int(40), Value::Int(40)]);
        }
    }
}

#[test]
fn exec_audit_join_order_single_key() {
    run(
        &[
            "CREATE TABLE l(k INT PRIMARY KEY,a INT)",
            "CREATE TABLE r(k INT)",
            "INSERT INTO l VALUES (1,3),(2,1),(3,2)",
            "INSERT INTO r VALUES (2)",
        ],
        "SELECT l.k,r.k FROM l LEFT JOIN r ON l.k<>r.k ORDER BY r.k",
        &[&["2", "NULL"], &["1", "2"], &["3", "2"]],
    );
}

#[test]
fn exec_audit_ntile_large() {
    run(
        &[
            "CREATE TABLE t(id INT)",
            "INSERT INTO t VALUES (1),(2),(3),(4),(5)",
        ],
        "SELECT NTILE(9223372036854775807) OVER (ORDER BY id),NTILE(3) OVER (ORDER BY id) FROM t",
        &[
            &["1", "1"],
            &["2", "1"],
            &["3", "2"],
            &["4", "2"],
            &["5", "3"],
        ],
    );
}

#[test]
fn exec_audit_having_alias_and_null() {
    run(&[], "SELECT 1 AS x HAVING x=1", &[&["1"]]);
    run(&[], "SELECT 1 AS x HAVING NULL", &[]);
    run(&[], "SELECT COUNT(*) AS c HAVING c=1", &[&["1"]]);
}

#[test]
fn exec_audit_cross_type_join_nulls_and_precision() {
    run(
        &[
            "CREATE TABLE a(k DOUBLE)",
            "CREATE TABLE b(k DECIMAL(20,2))",
            "INSERT INTO a VALUES (NULL),(1.5),(2),(3.25)",
            "INSERT INTO b VALUES (NULL),(1.50),(2.00),(3.26)",
        ],
        "SELECT COUNT(*) FROM a JOIN b ON a.k<=>b.k",
        &[&["3"]],
    );
    run(
        &[
            "CREATE TABLE a(k BIGINT)",
            "CREATE TABLE b(k DECIMAL(20,0))",
            "INSERT INTO a VALUES (9007199254740992),(9007199254740993)",
            "INSERT INTO b VALUES (9007199254740993)",
        ],
        "SELECT a.k FROM a JOIN b ON a.k=b.k",
        &[&["9007199254740993"]],
    );
    run(
        &[
            "CREATE TABLE a(k DATE)",
            "CREATE TABLE b(k DATETIME(6))",
            "INSERT INTO a VALUES ('2024-01-01')",
            "INSERT INTO b VALUES ('2024-01-01 00:00:00'),('2024-01-01 00:00:00.000001')",
        ],
        "SELECT COUNT(*) FROM a JOIN b ON a.k=b.k",
        &[&["1"]],
    );
}

#[test]
fn exec_audit_cross_type_cte_fallback_join() {
    run(
        &[
            "CREATE TABLE a(k DOUBLE)",
            "CREATE TABLE b(k DECIMAL(20,2))",
            "INSERT INTO a VALUES (1.5),(2)",
            "INSERT INTO b VALUES (1.50),(2.00)",
        ],
        "SELECT d.k FROM a JOIN b ON a.k=b.k JOIN LATERAL (SELECT a.k AS k) d ON TRUE ORDER BY d.k",
        &[&["1.5"], &["2"]],
    );
}

#[test]
fn exec_audit_cte_fallback_binary_distinct() {
    run(
        &[
            "CREATE TABLE a(k VARCHAR(10) COLLATE utf8mb4_bin)",
            "INSERT INTO a VALUES ('A'),('a'),('A')",
        ],
        "SELECT DISTINCT d.k FROM a JOIN LATERAL (SELECT a.k AS k) d ON TRUE ORDER BY d.k LIMIT 2",
        &[&["A"], &["a"]],
    );
}

#[test]
fn exec_audit_window_partition_and_huge_offsets() {
    run(&["CREATE TABLE t(p INT,id INT)", "INSERT INTO t VALUES (1,1),(1,2),(2,3)"],
        "SELECT id,RANK() OVER (PARTITION BY p),DENSE_RANK() OVER (PARTITION BY p),LAG(id,9223372036854775807,p) OVER (ORDER BY id),LEAD(id,9223372036854775807,p) OVER (ORDER BY id) FROM t ORDER BY id",
        &[&["1","1","1","1","1"],&["2","1","1","1","1"],&["3","1","1","2","2"]]);
}

#[test]
fn exec_audit_distinct_offset_and_having() {
    run(&["CREATE TABLE t(a INT)","INSERT INTO t VALUES (1),(1),(2),(2),(3),(3)"],
        "SELECT DISTINCT d.a FROM t JOIN LATERAL (SELECT t.a AS a) d ON TRUE HAVING d.a>1 ORDER BY d.a LIMIT 1 OFFSET 1", &[&["3"]]);
}

#[test]
fn exec_audit_temporal_join_preserves_stored_zero_under_strict_mode() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "SET SESSION sql_mode=''",
        "CREATE TABLE a(k DATE)",
        "CREATE TABLE b(k DATETIME(6))",
        "INSERT INTO a VALUES ('0000-00-00'),('2024-01-01')",
        "INSERT INTO b VALUES ('0000-00-00 00:00:00'),('2024-01-01 00:00:00')",
        "SET SESSION sql_mode='STRICT_ALL_TABLES,NO_ZERO_DATE'",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT COUNT(*) FROM a JOIN b ON a.k=b.k")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(result.rows, vec![vec![Value::Int(2)]]);
}
