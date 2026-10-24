use super::*;

fn engine() -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine)
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    match engine
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
    {
        QueryResponse::ResultSet(result) => result.rows,
        other => panic!("expected rows: {other:?}"),
    }
}

#[test]
fn cte_audit_nested_scope() {
    let (_dir, e) = engine();
    e.execute("CREATE TABLE x(v INT)").unwrap();
    e.execute("INSERT INTO x VALUES (7)").unwrap();
    for sql in [
        "WITH x AS (SELECT 42 v) SELECT * FROM (WITH y AS (SELECT * FROM x) SELECT * FROM y) d",
        "WITH a AS (SELECT 42 v), b AS (WITH c AS (SELECT * FROM a) SELECT * FROM c) SELECT * FROM b",
        "WITH b AS (WITH c AS (SELECT * FROM a) SELECT * FROM c), a AS (SELECT 42 v) SELECT * FROM b",
        "WITH x AS (SELECT 42 v) SELECT * FROM (WITH x AS (SELECT * FROM x) SELECT * FROM x) d",
        "WITH b AS (WITH x AS (SELECT * FROM x) SELECT * FROM x), x AS (SELECT 42 v) SELECT * FROM b",
    ] {
        assert_eq!(rows(&e, sql), vec![vec![Value::Int(42)]], "{sql}");
    }
    assert_eq!(
        rows(
            &e,
            "WITH v AS (SELECT 1 a), w AS (WITH v AS (SELECT 2 a) SELECT * FROM v) SELECT * FROM w"
        ),
        vec![vec![Value::Int(2)]]
    );
    assert_eq!(rows(&e, "SELECT * FROM x"), vec![vec![Value::Int(7)]]);
}

#[test]
fn cte_audit_recursive_assignment() {
    let (_dir, e) = engine();
    for union in ["UNION ALL", "UNION"] {
        let sql = format!("WITH RECURSIVE c(n) AS (SELECT 1 {union} SELECT n+0.5 FROM c WHERE n<3) SELECT * FROM c");
        assert_eq!(
            rows(&e, &sql),
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)]
            ]
        );
    }
    let err = e.execute("WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT CONCAT('x',n) FROM c) SELECT * FROM c").unwrap_err();
    assert_eq!(err.code(), 1366);
}

#[test]
fn cte_audit_validation() {
    let (_dir, e) = engine();
    for (sql, code) in [
        (
            "WITH c AS (SELECT 1 a), C AS (SELECT 2 a) SELECT * FROM c",
            1066,
        ),
        ("WITH c AS (SELECT 1 a, 2 a) SELECT * FROM c", 1060),
        ("WITH c(a,a) AS (SELECT 1,2) SELECT * FROM c", 1060),
        (
            "WITH RECURSIVE c AS (SELECT 1 UNION ALL SELECT 1,2 FROM c WHERE 1=0) SELECT * FROM c",
            1222,
        ),
    ] {
        assert_eq!(e.execute(sql).unwrap_err().code(), code, "{sql}");
    }
}

#[test]
fn recursive_cte_rejects_all_registered_aggregate_families() {
    let (_dir, e) = engine();
    for expression in [
        "SUM(n)",
        "BIT_AND(n)",
        "BIT_OR(n)",
        "BIT_XOR(n)",
        "STDDEV_POP(n)",
        "STDDEV_SAMP(n)",
        "VAR_POP(n)",
        "VAR_SAMP(n)",
        "JSON_ARRAYAGG(n)",
        "JSON_OBJECTAGG('key', n)",
        "COALESCE(STDDEV_POP(n),0)",
        "SUM(n) BETWEEN 0 AND 10",
    ] {
        let sql = format!("WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT {expression} FROM c WHERE n<2) SELECT * FROM c");
        assert_eq!(
            e.execute(&sql).unwrap_err().code(),
            crate::error::catalog::ER_CTE_RECURSIVE_FORBIDS_AGGREGATION.code(),
            "{sql}"
        );
    }
    // An aggregate in an independent inner query belongs to that query.
    assert_eq!(rows(&e, "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+(SELECT MAX(1)) FROM c WHERE n<3) SELECT * FROM c"),
        vec![vec![Value::Int(1)], vec![Value::Int(2)], vec![Value::Int(3)]]);
}

#[test]
fn cte_audit_query_slots() {
    let (_dir, e) = engine();
    for sql in [
        "SELECT (WITH c AS (SELECT 4 v) SELECT * FROM c)",
        "SELECT 4 WHERE 4 IN (WITH c AS (SELECT 4 v) SELECT * FROM c)",
        "SELECT 4 WHERE EXISTS (WITH c AS (SELECT 4 v) SELECT * FROM c)",
        "SELECT MAX(n) FROM (WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<4) SELECT * FROM c) d",
    ] {
        assert_eq!(rows(&e, sql), vec![vec![Value::Int(4)]]);
    }
    e.execute("CREATE VIEW vr AS WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<4) SELECT * FROM c").unwrap();
    assert_eq!(rows(&e, "SELECT MAX(n) FROM vr"), vec![vec![Value::Int(4)]]);
}

#[test]
fn cte_audit_prepare() {
    let (_dir, e) = engine();
    let mut session = SessionContext::default();
    e.execute_with_session(&mut session, "PREPARE st FROM 'WITH RECURSIVE c(n) AS (SELECT ? UNION ALL SELECT n+1 FROM c WHERE n<?) SELECT COUNT(*) FROM c'").unwrap();
    e.execute_with_session(&mut session, "SET @a=1, @b=5")
        .unwrap();
    let QueryResponse::ResultSet(result) = e
        .execute_with_session(&mut session, "EXECUTE st USING @a,@b")
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(result.rows, vec![vec![Value::Int(5)]]);
}

#[test]
fn cte_audit_multiple_recursive_branches() {
    let (_dir, e) = engine();
    assert_eq!(rows(&e, "WITH RECURSIVE c AS (SELECT 1 n UNION ALL SELECT n+1 FROM c WHERE n<6 UNION ALL SELECT n*10 FROM c WHERE n<30) SELECT COUNT(*) FROM c"), vec![vec![Value::Int(14)]]);
}

#[test]
fn cte_audit_order_ordinal() {
    let (_dir, e) = engine();
    assert_eq!(
        rows(
            &e,
            "SELECT * FROM (SELECT 1 n UNION ALL SELECT 3 UNION ALL SELECT 2) d ORDER BY 1 DESC"
        ),
        vec![
            vec![Value::Int(3)],
            vec![Value::Int(2)],
            vec![Value::Int(1)]
        ]
    );
}

#[test]
fn cte_audit_lock_base_row() {
    let (_dir, e) = engine();
    e.execute("CREATE TABLE locks(id INT PRIMARY KEY,v INT)")
        .unwrap();
    e.execute("INSERT INTO locks VALUES (1,10),(2,20)").unwrap();
    let mut owner = SessionContext::default();
    let mut contender = SessionContext::default();
    e.execute_with_session(&mut owner, "BEGIN").unwrap();
    e.execute_with_session(
        &mut owner,
        "WITH c AS (SELECT * FROM locks) SELECT * FROM c WHERE id=1 FOR UPDATE",
    )
    .unwrap();
    assert_eq!(
        e.execute_with_session(
            &mut contender,
            "SELECT * FROM locks WHERE id=1 FOR UPDATE NOWAIT"
        )
        .unwrap_err()
        .code(),
        3572
    );
    e.execute_with_session(
        &mut contender,
        "SELECT * FROM locks WHERE id=2 FOR UPDATE NOWAIT",
    )
    .unwrap();
    e.execute_with_session(&mut owner, "ROLLBACK").unwrap();
}

#[test]
fn cte_audit_recursive_errors_and_distinct() {
    let (_dir, e) = engine();
    for (sql, code) in [
        ("WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT ROW_NUMBER() OVER () FROM c) SELECT * FROM c", 3575),
        ("WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT SUM(n) FROM c) SELECT * FROM c", 3575),
        ("WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c a,c b) SELECT * FROM c", 3577),
        ("WITH RECURSIVE c(n) AS (SELECT n+1 FROM c UNION ALL SELECT 1) SELECT * FROM c", 3574),
    ] {
        assert_eq!(e.execute(sql).unwrap_err().code(), code, "{sql}");
    }
    assert_eq!(
        rows(
            &e,
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION SELECT n+0.4 FROM c) SELECT * FROM c"
        ),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(rows(&e, "WITH RECURSIVE c(n) AS (SELECT 1 UNION SELECT n+1 FROM c WHERE n<3 UNION SELECT n+1 FROM c WHERE n<3) SELECT * FROM c"), vec![vec![Value::Int(1)],vec![Value::Int(2)],vec![Value::Int(3)]]);
}

#[test]
fn cte_audit_derived_preserves_transaction_and_depth() {
    let (_dir, e) = engine();
    e.execute("CREATE TABLE t(n INT)").unwrap();
    let mut session = SessionContext::default();
    e.execute_with_session(&mut session, "BEGIN").unwrap();
    e.execute_with_session(&mut session, "INSERT INTO t VALUES (7)")
        .unwrap();
    let sql = "SELECT * FROM (WITH RECURSIVE c(n) AS (SELECT n FROM t UNION ALL SELECT n+1 FROM c WHERE n<8) SELECT * FROM c) d";
    let QueryResponse::ResultSet(result) = e.execute_with_session(&mut session, sql).unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(result.rows, vec![vec![Value::Int(7)], vec![Value::Int(8)]]);
    e.execute_with_session(&mut session, "SET cte_max_recursion_depth=1")
        .unwrap();
    assert_eq!(
        e.execute_with_session(&mut session, sql)
            .unwrap_err()
            .code(),
        3636
    );
    e.execute_with_session(&mut session, "ROLLBACK").unwrap();
    assert!(rows(&e, "SELECT * FROM t").is_empty());
}

#[test]
fn cte_audit_nested_scope_restores_after_error() {
    let (_dir, e) = engine();
    for value in [42, 57] {
        let sql = format!("WITH x AS (SELECT {value} v) SELECT * FROM (WITH y AS (SELECT * FROM x) SELECT a.v FROM y a JOIN y b ON a.v=b.v) d");
        assert_eq!(rows(&e, &sql), vec![vec![Value::Int(value)]]);
    }
    assert!(e.execute("WITH x AS (SELECT 42 v) SELECT * FROM (WITH y AS (SELECT * FROM x) SELECT missing FROM y) d").is_err());
    assert_eq!(e.execute("SELECT * FROM x").unwrap_err().code(), 1146);
}

#[test]
fn cte_audit_correlated_expression_with() {
    let (_dir, e) = engine();
    e.execute("CREATE TABLE outer_t(v INT)").unwrap();
    e.execute("INSERT INTO outer_t VALUES (1),(2),(3)").unwrap();
    for sql in [
        "SELECT v FROM outer_t WHERE EXISTS (WITH c AS (SELECT 2 v) SELECT * FROM c WHERE c.v=outer_t.v)",
        "SELECT v FROM outer_t WHERE v IN (WITH c AS (SELECT 2 v) SELECT v FROM c WHERE c.v=outer_t.v)",
        "SELECT v FROM outer_t WHERE (WITH c AS (SELECT 2 v) SELECT COUNT(*) FROM c WHERE c.v=outer_t.v)=1",
        "SELECT v FROM outer_t WHERE EXISTS (WITH c AS (SELECT 2 v) SELECT a.v FROM c a JOIN c b ON a.v=b.v WHERE a.v=outer_t.v)",
    ] {
        assert_eq!(rows(&e, sql), vec![vec![Value::Int(2)]], "{sql}");
    }
}
