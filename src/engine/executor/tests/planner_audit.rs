//! Regression oracles for doc_old/planner-bugs-audit.md.
use super::*;

fn setup(sql: &str) -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for statement in sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        engine
            .execute(statement)
            .unwrap_or_else(|e| panic!("{statement}: {e:?}"));
    }
    (dir, engine)
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<String>> {
    let QueryResponse::ResultSet(result) =
        execute_permissive(engine, sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
    else {
        panic!("not a result set: {sql}")
    };
    result
        .rows
        .iter()
        .map(|r| r.iter().map(Value::to_display_string).collect())
        .collect()
}

fn expect(engine: &StorageQueryEngine, sql: &str, expected: &[&[&str]]) {
    let expected: Vec<Vec<String>> = expected
        .iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect();
    assert_eq!(rows(engine, sql), expected, "{sql}");
}

#[test]
fn planner_audit_in_subquery_storage_types_and_intersection() {
    for (ty, values) in [
        ("DATE", "'2024-01-01','2024-01-02'"),
        ("DECIMAL(10,2)", "10.50,20.25"),
        ("TIME", "'01:02:03','02:03:04'"),
        ("DATETIME", "'2024-01-01 01:02:03','2024-01-02 02:03:04'"),
        ("YEAR", "2020,2021"),
        ("FLOAT", "1.5,2.5"),
        ("BIGINT UNSIGNED", "9223372036854775808,9223372036854775809"),
    ] {
        let v: Vec<_> = values.split(',').collect();
        let (_dir, e) = setup(&format!("CREATE TABLE t(id INT PRIMARY KEY, k {ty}, KEY ik(k)); CREATE TABLE s(k {ty}); CREATE TABLE s2(k {ty}); INSERT INTO t VALUES (1,{}),(2,{}); INSERT INTO s VALUES ({}); INSERT INTO s2 VALUES ({})",v[0],v[1],v[0],v[1]));
        expect(
            &e,
            "SELECT id FROM t WHERE k IN (SELECT k FROM s LIMIT 2)",
            &[&["1"]],
        );
        expect(&e,"SELECT id FROM t WHERE k IN (SELECT k FROM s LIMIT 2) AND k IN (SELECT k FROM s2 LIMIT 2)", &[]);
    }
}

#[test]
fn planner_audit_numeric_string_and_like_scan_oracle() {
    let (_dir,e)=setup("CREATE TABLE t(id INT PRIMARY KEY,k INT,KEY ik(k)); INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,12),(5,NULL),(6,-2),(7,0)");
    for predicate in [
        "k < '1e3'",
        "k = '12abc'",
        "k IN ('10','20','12abc')",
        "k LIKE '1%'",
        "k < '12.5'",
        "k < 12.5",
        "k > -2.5",
        "k BETWEEN -2.5 AND 12.5",
        "k > '-2.5'",
        "k BETWEEN '-2.5' AND '12.5'",
        "k = 'nonnumeric'",
    ] {
        assert_eq!(
            rows(
                &e,
                &format!("SELECT id FROM t WHERE {predicate} ORDER BY id")
            ),
            rows(
                &e,
                &format!("SELECT id FROM t IGNORE INDEX(ik) WHERE {predicate} ORDER BY id")
            ),
            "{predicate}"
        );
    }
    expect(&e, "SELECT id FROM t WHERE k = '12abc'", &[&["4"]]);
    expect(
        &e,
        "SELECT id FROM t WHERE k LIKE '1%' ORDER BY id",
        &[&["1"], &["4"]],
    );
}

#[test]
fn planner_audit_projection_alias_and_enum() {
    let (_dir,e)=setup("CREATE TABLE t(a INT,b INT,d INT); INSERT INTO t VALUES(1,100,7); CREATE TABLE en(c ENUM('a','b','c')); INSERT INTO en VALUES('a'),('b'),(NULL)");
    expect(
        &e,
        "SELECT b AS a,a+0 AS c FROM t GROUP BY d",
        &[&["100", "1"]],
    );
    for predicate in ["c IN (1)", "c BETWEEN 1 AND 1"] {
        expect(
            &e,
            &format!("SELECT c FROM en WHERE {predicate}"),
            &[&["a"]],
        );
    }
    expect(
        &e,
        "SELECT c=1,CASE WHEN c=1 THEN 'x' ELSE 'n' END FROM en WHERE c='a'",
        &[&["1", "x"]],
    );
    expect(
        &e,
        "SELECT CASE c WHEN 1 THEN 'x' ELSE 'n' END FROM en WHERE c='a'",
        &[&["x"]],
    );
    expect(
        &e,
        "SELECT c FROM en WHERE c NOT IN (1) ORDER BY c",
        &[&["b"]],
    );
    expect(&e, "SELECT c FROM en WHERE c IN (1,NULL)", &[&["a"]]);
}

#[test]
fn planner_audit_scalar_join_on_and_union_limit() {
    let (_dir,e)=setup("CREATE TABLE o(id INT PRIMARY KEY,k INT); INSERT INTO o VALUES(1,10),(2,20); CREATE TABLE t(a INT,x INT); INSERT INTO t VALUES(10,100),(10,101),(20,200); CREATE TABLE v(a INT,b INT,x INT); INSERT INTO v VALUES(10,10,1),(10,20,2),(20,20,3); CREATE TABLE f(dt INT); INSERT INTO f VALUES(1),(2),(3),(4),(5); CREATE TABLE r(dt INT); INSERT INTO r VALUES(11); CREATE TABLE d(dd INT,x INT); INSERT INTO d VALUES(1,1),(2,2),(3,3),(4,4),(5,5)");
    expect(&e,"SELECT o.id,(SELECT MAX(v.x) FROM t JOIN v ON v.a=t.a AND v.b=o.k) AS s FROM o ORDER BY o.id", &[&["1","1"],&["2","3"]]);
    expect(&e,"SELECT u.dt FROM ((SELECT dt FROM f ORDER BY dt LIMIT 3) UNION ALL (SELECT dt FROM r)) u,d WHERE u.dt=d.dd AND d.x>1 ORDER BY u.dt", &[&["2"],&["3"]]);
}

#[test]
fn planner_audit_anti_having_and_dropped_columns() {
    let (_dir,e)=setup("CREATE TABLE o1(k INT PRIMARY KEY,v INT); CREATE TABLE o2(k INT PRIMARY KEY); CREATE TABLE l(k INT); INSERT INTO o1 VALUES(1,1),(2,100); INSERT INTO o2 VALUES(1),(2); INSERT INTO l VALUES(1); CREATE TABLE t(a INT,b INT,c INT); INSERT INTO t VALUES(1,10,100),(2,20,200); ALTER TABLE t DROP COLUMN b");
    expect(&e,"SELECT o1.k FROM o1 JOIN o2 ON o1.k=o2.k WHERE NOT EXISTS(SELECT 1 FROM l WHERE l.k=o1.k) HAVING o1.v>50", &[&["2"]]);
    expect(
        &e,
        "SELECT t.a,t.c,o1.v FROM t JOIN o1 ON t.a=o1.k ORDER BY t.a",
        &[&["1", "100", "1"], &["2", "200", "100"]],
    );
}

#[test]
fn planner_audit_fulltext_aggregate_window_and_invisible() {
    let (_dir,e)=setup("CREATE TABLE ft(id INT PRIMARY KEY,body TEXT,FULLTEXT KEY ftx(body)); INSERT INTO ft VALUES(1,'hello world'),(2,'hello there'); CREATE TABLE t(id INT PRIMARY KEY,b INT,KEY ib(b)); INSERT INTO t VALUES(1,20),(2,10); ALTER TABLE t ALTER INDEX ib INVISIBLE");
    expect(&e, "SELECT MIN(body) FROM ft", &[&["hello there"]]);
    expect(&e,"SELECT id,RANK() OVER(ORDER BY MATCH(body) AGAINST('world')) AS r FROM ft WHERE MATCH(body) AGAINST('hello') ORDER BY id", &[&["1","2"],&["2","1"]]);
    let plan = rows(&e, "EXPLAIN FORMAT=TREE SELECT MIN(b) FROM t");
    assert!(!format!("{plan:?}").contains("using ib"));
}

#[test]
fn planner_audit_lateral_and_mixed_collation() {
    let (_dir,e)=setup("CREATE TABLE t1(id INT,a INT); CREATE TABLE t2(id INT,b INT); INSERT INTO t1 VALUES(1,42); INSERT INTO t2 VALUES(1,5); CREATE TABLE ca(v VARCHAR(10) COLLATE utf8mb4_general_ci); CREATE TABLE cb(v VARCHAR(10) COLLATE utf8mb4_bin); INSERT INTO ca VALUES('X'); INSERT INTO cb VALUES('X')");
    expect(
        &e,
        "SELECT * FROM t1 JOIN LATERAL(SELECT t1.a AS x) d ON TRUE JOIN t2 USING(id)",
        &[&["1", "42", "42", "5"]],
    );
    expect(
        &e,
        "SELECT * FROM ca JOIN cb ON ca.v=cb.v WHERE ca.v='x'",
        &[&["X", "X"]],
    );
}

#[test]
fn planner_audit_bare_and_nested_correlations() {
    let (_dir,e)=setup("CREATE TABLE o(id INT,cust INT,city VARCHAR(20)); CREATE TABLE i(cust INT,city_name VARCHAR(20)); INSERT INTO o VALUES(1,10,'beijing'); INSERT INTO i VALUES(10,'BJ'); CREATE TABLE oo(okey INT,nationkey INT); INSERT INTO oo VALUES(1,10); CREATE TABLE l(lkey INT,okey INT,pskey INT); INSERT INTO l VALUES(1,1,100); CREATE TABLE p(pskey INT,suppkey INT); INSERT INTO p VALUES(100,7); CREATE TABLE s(suppkey INT,nationkey INT); INSERT INTO s VALUES(7,10)");
    expect(
        &e,
        "SELECT id FROM o WHERE cust IN(SELECT cust FROM i WHERE city_name=city)",
        &[],
    );
    expect(&e,"SELECT okey FROM oo WHERE EXISTS(SELECT 1 FROM l,p WHERE l.pskey=p.pskey AND l.okey=oo.okey AND p.suppkey=(SELECT suppkey FROM s WHERE s.nationkey=oo.nationkey))", &[&["1"]]);
}

#[test]
fn planner_audit_sort_collation_and_scalar_join_dependency() {
    let (_dir,e)=setup("CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(20) COLLATE utf8mb4_bin,KEY iv(v)); INSERT INTO t VALUES(1,'a'),(2,'Z'),(3,'B'),(4,'z'); CREATE TABLE a(v INT); CREATE TABLE b(v INT); CREATE TABLE c(k INT); CREATE TABLE d(k INT,m INT); INSERT INTO a VALUES(6); INSERT INTO b VALUES(2); INSERT INTO c VALUES(1); INSERT INTO d VALUES(1,3)");
    expect(
        &e,
        "SELECT v FROM t FORCE INDEX(iv) ORDER BY v",
        &[&["B"], &["Z"], &["a"], &["z"]],
    );
    expect(
        &e,
        "SELECT v FROM t IGNORE INDEX(iv) ORDER BY v",
        &[&["B"], &["Z"], &["a"], &["z"]],
    );
    expect(
        &e,
        "SELECT a.v FROM a,b,c WHERE a.v=b.v*(SELECT MAX(m) FROM d WHERE d.k=c.k)",
        &[&["6"]],
    );
}

#[test]
fn planner_audit_mvi_membership_keeps_json_types() {
    let (_dir,e)=setup("CREATE TABLE t(id INT PRIMARY KEY,j JSON,INDEX im((CAST(j AS UNSIGNED ARRAY)))); INSERT INTO t VALUES(1,'[1,2,3]')");
    for predicate in [
        "1 MEMBER OF(j)",
        "1.5 MEMBER OF(j)",
        "2.9 MEMBER OF(j)",
        "'1' MEMBER OF(j)",
    ] {
        assert_eq!(
            rows(&e, &format!("SELECT id FROM t WHERE {predicate}")),
            rows(
                &e,
                &format!("SELECT id FROM t IGNORE INDEX(im) WHERE {predicate}")
            ),
            "{predicate}"
        );
    }
    expect(&e, "SELECT id FROM t WHERE 1.5 MEMBER OF(j)", &[]);
    expect(&e, "SELECT id FROM t WHERE '1' MEMBER OF(j)", &[]);
}

#[test]
fn planner_audit_diagnostics_and_values_metadata() {
    let (_dir, e) =
        setup("CREATE TABLE t(id INT PRIMARY KEY,b INT,KEY ib(b)); INSERT INTO t VALUES(1,2)");
    let result = e
        .execute("SELECT * FROM (VALUES ROW(NULL),ROW(12.5)) v")
        .unwrap();
    let QueryResponse::ResultSet(result) = result else {
        panic!("result set required")
    };
    assert!(
        matches!(result.columns[0].column_type, ColumnType::Decimal { .. }),
        "{:?}",
        result.columns
    );
    let no_pred = rows(&e, "EXPLAIN SELECT * FROM t");
    assert_eq!(no_pred[0][5], "NULL", "possible_keys: {no_pred:?}");
    let plan = rows(
        &e,
        "EXPLAIN FORMAT=TREE SELECT * FROM t WHERE b IS NOT NULL AND b LIKE '2%'",
    );
    assert!(!format!("{plan:?}").contains("<expr>"));
}

#[test]
fn planner_audit_boundary_geometry_and_mysql_right_using() {
    let (_dir,e)=setup("CREATE TABLE t1(id INT,a INT); CREATE TABLE t2(id INT,b INT); INSERT INTO t1 VALUES(1,42); INSERT INTO t2 VALUES(1,5),(2,6)");
    expect(
        &e,
        "SELECT * FROM t1 RIGHT JOIN t2 USING(id) ORDER BY id",
        &[&["1", "5", "42"], &["2", "6", "NULL"]],
    );
    expect(&e,"SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'),ST_GeomFromText('POINT(0 0)')),ST_Contains(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'),ST_GeomFromText('POINT(1 1)'))", &[&["0","1"]]);
}

#[test]
fn planner_audit_dynamic_vector_and_index_hint() {
    let (_dir,e)=setup("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(3) NOT NULL,INDEX iv(v) USING HNSW); INSERT INTO t VALUES(1,'[0,0,0]'),(2,'[5,5,5]'),(3,'[1,1,1]')");
    let query = "SELECT /*+ HNSW_EF(64) */ id FROM t ORDER BY DISTANCE(v,@q) LIMIT 1";
    let mut session = SessionContext::default();
    for (vector, expected) in [("[0.1,0.1,0.1]", "1"), ("[9,9,9]", "2")] {
        e.execute_with_session(&mut session, &format!("SET @q='{vector}'"))
            .unwrap();
        let response = e
            .execute_with_session(&mut session, query)
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(result) = response else {
            panic!("expected rows")
        };
        assert_eq!(result.rows[0][0].to_display_string(), expected);
    }
    for hint in ["IGNORE INDEX(iv)", "IGNORE INDEX FOR ORDER BY(iv)"] {
        let plan = rows(&e, &format!("EXPLAIN FORMAT=TREE SELECT /*+ HNSW_EF(64) */ id FROM t {hint} ORDER BY DISTANCE(v,'[0,0,0]') LIMIT 1"));
        assert!(!format!("{plan:?}").contains("using iv"));
    }
}

#[test]
fn planner_audit_in_subquery_cross_domain_preserves_comparison() {
    let (_dir,e)=setup("CREATE TABLE t(k INT,KEY ik(k)); INSERT INTO t VALUES(1),(2); CREATE TABLE s(k DECIMAL(10,2)); INSERT INTO s VALUES(1.50); CREATE TABLE c(v VARCHAR(10),KEY iv(v)); INSERT INTO c VALUES('01'),('1'); CREATE TABLE n(k INT); INSERT INTO n VALUES(1)");
    expect(
        &e,
        "SELECT k FROM t WHERE k IN(SELECT k FROM s LIMIT 1)",
        &[],
    );
    expect(
        &e,
        "SELECT v FROM c WHERE v IN(SELECT k FROM n LIMIT 1) ORDER BY v",
        &[&["01"], &["1"]],
    );
}

#[test]
fn planner_audit_numeric_text_comparison_domains() {
    for ty in ["FLOAT", "DOUBLE", "DECIMAL(10,2)"] {
        let (_dir, e) = setup(&format!("CREATE TABLE t(id INT PRIMARY KEY,k {ty},KEY ik(k)); INSERT INTO t VALUES(1,12),(2,12.5),(3,20),(4,NULL)"));
        for predicate in ["k='12abc'", "k<'1e3'", "k<'12.51'", "k IN('12abc','20')"] {
            assert_eq!(
                rows(
                    &e,
                    &format!("SELECT id FROM t WHERE {predicate} ORDER BY id")
                ),
                rows(
                    &e,
                    &format!("SELECT id FROM t IGNORE INDEX(ik) WHERE {predicate} ORDER BY id")
                ),
                "{ty}: {predicate}"
            );
        }
    }
}

#[test]
fn planner_audit_same_domain_different_width_keeps_in_lookup() {
    let (_dir,e) = setup("CREATE TABLE t(k BIGINT,KEY ik(k)); CREATE TABLE s(k INT); INSERT INTO t VALUES(1),(2),(3); INSERT INTO s VALUES(2)");
    let sql = "SELECT k FROM t FORCE INDEX(ik) WHERE k IN(SELECT k FROM s LIMIT 1)";
    expect(&e, sql, &[&["2"]]);
    let Statement::Select(select) = parse_statement(sql).unwrap() else {
        panic!("select")
    };
    let plan = crate::planner::plan_select(e.db_for_tests(), &select).unwrap();
    fn has_in_lookup(plan: &crate::planner::PhysicalPlan) -> bool {
        matches!(plan, crate::planner::PhysicalPlan::IndexInLookup { .. })
            || plan.children().into_iter().any(has_in_lookup)
    }
    assert!(has_in_lookup(&plan.physical), "{:?}", plan.physical);
}
