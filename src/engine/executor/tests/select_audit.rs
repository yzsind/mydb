use super::*;

fn fixture(sql: &[&str]) -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for query in sql {
        engine.execute(query).unwrap();
    }
    (dir, engine)
}
fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
    else {
        panic!("expected rows: {sql}")
    };
    result.rows
}
#[test]
fn select_audit_in_coercion() {
    let (_d, e) = fixture(&[
        "CREATE TABLE a(x INT)",
        "INSERT INTO a VALUES (1)",
        "CREATE TABLE b(v VARCHAR(4))",
        "INSERT INTO b VALUES ('1'),('2')",
    ]);
    assert_eq!(
        rows(&e, "SELECT x FROM a WHERE x IN (SELECT v FROM b LIMIT 2)"),
        vec![vec![Value::Int(1)]]
    );
    assert!(rows(
        &e,
        "SELECT x FROM a WHERE x NOT IN (SELECT v FROM b LIMIT 2)"
    )
    .is_empty());
}
#[test]
fn select_audit_in_binary_collation() {
    let (_d, e) = fixture(&[
        "CREATE TABLE a(s VARCHAR(10) COLLATE utf8mb4_bin)",
        "INSERT INTO a VALUES ('a')",
        "CREATE TABLE b(v VARCHAR(10) COLLATE utf8mb4_bin)",
        "INSERT INTO b VALUES ('A')",
    ]);
    assert!(rows(&e, "SELECT s FROM a WHERE s IN (SELECT v FROM b LIMIT 1)").is_empty());
}
#[test]
fn select_audit_distinct_binary_collation() {
    let (_d, e) = fixture(&[
        "CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(10) COLLATE utf8mb4_bin)",
        "INSERT INTO t VALUES (1,'A'),(2,'a'),(3,'b')",
    ]);
    assert_eq!(
        rows(&e, "SELECT DISTINCT v FROM t WHERE id BETWEEN 1 AND 3").len(),
        3
    );
}
#[test]
fn select_audit_outer_order() {
    let (_d, e) = fixture(&[
        "CREATE TABLE o(id INT,bias INT)",
        "INSERT INTO o VALUES (1,5),(2,-5)",
        "CREATE TABLE i(id INT,prio INT)",
        "INSERT INTO i VALUES (1,3),(2,4)",
    ]);
    assert_eq!(
        rows(
            &e,
            "SELECT (SELECT i.id FROM i ORDER BY i.prio * o.bias LIMIT 1) FROM o"
        ),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
}
#[test]
fn select_audit_outer_aggregate() {
    let (_d, e) = fixture(&[
        "CREATE TABLE t(g INT,qty INT)",
        "INSERT INTO t VALUES (1,10),(1,20),(1,NULL)",
    ]);
    assert_eq!(
        rows(&e, "SELECT g,(SELECT COUNT(qty)+0) FROM t GROUP BY g"),
        vec![vec![Value::Int(1), Value::Int(2)]]
    );
    assert_eq!(
        rows(
            &e,
            "SELECT g FROM t GROUP BY g HAVING (SELECT SUM(qty)*2)>30"
        ),
        vec![vec![Value::Int(1)]]
    );
}
#[test]
fn select_audit_row_subquery() {
    let (_d, e) = fixture(&[
        "CREATE TABLE o(s VARCHAR(10) COLLATE utf8mb4_bin)",
        "INSERT INTO o VALUES ('a')",
        "CREATE TABLE i(v VARCHAR(10) COLLATE utf8mb4_bin)",
        "INSERT INTO i VALUES ('a')",
    ]);
    assert_eq!(
        rows(&e, "SELECT s FROM o WHERE (s,1) = (SELECT v,1 FROM i)"),
        vec![vec![Value::String("a".into())]]
    );
    e.execute("UPDATE i SET v='A'").unwrap();
    assert!(rows(&e, "SELECT s FROM o WHERE (s,1) = (SELECT v,1 FROM i)").is_empty());
}
#[test]
fn select_audit_cte_join_collation() {
    let (_d, e) = fixture(&[
        "CREATE TABLE t1(id INT PRIMARY KEY,s VARCHAR(10) COLLATE utf8mb4_bin)",
        "CREATE TABLE t2(id INT PRIMARY KEY,s VARCHAR(10) COLLATE utf8mb4_bin)",
        "CREATE TABLE t3(id INT)",
        "INSERT INTO t1 VALUES (1,'A')",
        "INSERT INTO t2 VALUES (1,'a'),(2,'zz')",
        "INSERT INTO t3 VALUES (1)",
    ]);
    assert!(rows(
        &e,
        "SELECT * FROM (t1 JOIN t3 ON t1.id=t3.id) JOIN (SELECT id,s FROM t2) d ON t1.s=d.s"
    )
    .is_empty());
}
#[test]
fn select_audit_cte_aggregate_order() {
    let (_d, e) = fixture(&[
        "CREATE TABLE t1(id INT,v INT)",
        "CREATE TABLE t3(id INT)",
        "INSERT INTO t1 VALUES (1,30),(2,10),(3,20)",
        "INSERT INTO t3 VALUES (1),(2),(3)",
    ]);
    let r=rows(&e,"SELECT t1.id,SUM(t1.v) s FROM (t1 JOIN t3 ON t1.id=t3.id) JOIN (SELECT 1 x) d ON d.x=1 GROUP BY t1.id ORDER BY SUM(t1.v)+1");
    assert_eq!(
        r.iter().map(|r| r[0].clone()).collect::<Vec<_>>(),
        vec![Value::Int(2), Value::Int(3), Value::Int(1)]
    );
}
#[test]
fn select_audit_union_temporal() {
    let (_d, e) = fixture(&[
        "CREATE TABLE d1(d DATE)",
        "CREATE TABLE d2(dt DATETIME)",
        "INSERT INTO d1 VALUES ('2024-01-01')",
        "INSERT INTO d2 VALUES ('2024-01-01 00:00:00')",
    ]);
    assert_eq!(
        rows(&e, "SELECT d FROM d1 UNION SELECT dt FROM d2").len(),
        1
    );
    let r = rows(&e, "SELECT d FROM d1 UNION ALL SELECT dt FROM d2");
    assert_eq!(r[0], r[1]);
}

fn session_rows(e: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(r) = e
        .execute_with_session(s, sql)
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("expected rows")
    };
    r.rows
}
#[test]
fn select_audit_const_where_side_effects() {
    let (_d, e) = fixture(&[]);
    let mut s = SessionContext::default();
    e.execute_with_session(&mut s, "SET @a=0").unwrap();
    assert!(session_rows(&e, &mut s, "SELECT @a:=@a+1 FROM DUAL WHERE 0").is_empty());
    assert_eq!(
        session_rows(&e, &mut s, "SELECT @a"),
        vec![vec![Value::Int(0)]]
    );
}
#[test]
fn select_audit_into_empty_warning() {
    let (_d, e) = fixture(&["CREATE TABLE t(id INT)"]);
    let mut s = SessionContext::default();
    e.execute_with_session(&mut s, "SET @a=42").unwrap();
    e.execute_with_session(&mut s, "SELECT id INTO @a FROM t")
        .unwrap();
    let w = session_rows(&e, &mut s, "SHOW WARNINGS");
    assert_eq!(w.len(), 1);
    assert_eq!(w[0][1], Value::Int(1329));
    assert_eq!(
        session_rows(&e, &mut s, "SELECT @a"),
        vec![vec![Value::Int(42)]]
    );
}
#[test]
fn select_audit_prepared_limit_and_semantics() {
    use crate::engine::api::{PreparedSelectMemoState, QueryEngine};
    for cache_enabled in [true, false] {
        let (_d, mut e) = fixture(&["CREATE TABLE t(id INT)", "INSERT INTO t VALUES (1),(2),(3)"]);
        e.plan_cache_enabled = cache_enabled;
        let mut s = SessionContext::default();
        let sql = "SELECT id FROM t ORDER BY id";
        let parsed = e.prepare_cached(&mut s, sql, 0).unwrap();
        let mut memo = PreparedSelectMemoState::default();
        for limit in [1, 2, 0, 3] {
            e.execute_with_session(&mut s, &format!("SET sql_select_limit={limit}"))
                .unwrap();
            let r = e
                .execute_prepared_select(&mut s, &mut memo, &parsed, sql, &[])
                .unwrap()
                .unwrap()
                .materialize()
                .unwrap();
            let QueryResponse::ResultSet(r) = r else {
                panic!("expected result")
            };
            assert_eq!(r.rows.len(), limit);
        }
        e.execute_with_session(&mut s, "SET sql_mode=''").unwrap();
        let sql = "SELECT id,COUNT(*) FROM t";
        let parsed = e.prepare_cached(&mut s, sql, 0).unwrap();
        let mut memo = PreparedSelectMemoState::default();
        e.execute_prepared_select(&mut s, &mut memo, &parsed, sql, &[])
            .unwrap()
            .unwrap();
        e.execute_with_session(&mut s, "SET sql_mode='ONLY_FULL_GROUP_BY'")
            .unwrap();
        let result = e
            .execute_prepared_select(&mut s, &mut memo, &parsed, sql, &[])
            .unwrap_or_else(|| {
                e.execute_prepared_statement(&mut s, sql, (*parsed).clone())
                    .unwrap()
            });
        assert!(
            result.is_err(),
            "session sql_mode change must invalidate semantic preparation"
        );
    }
}
#[test]
fn select_audit_skip_locked_limit() {
    let (_d, e) = fixture(&[
        "CREATE TABLE a(id INT PRIMARY KEY,aid INT)",
        "CREATE TABLE b(id INT PRIMARY KEY)",
        "INSERT INTO a VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6)",
        "INSERT INTO b VALUES (1),(2),(3),(4),(5),(6)",
    ]);
    let mut s1 = SessionContext::default();
    let mut s2 = SessionContext::default();
    e.execute_with_session(&mut s1, "BEGIN").unwrap();
    session_rows(&e, &mut s1, "SELECT * FROM b WHERE id<=3 FOR UPDATE");
    let r=session_rows(&e,&mut s2,"SELECT a.id,b.id FROM a JOIN b ON a.aid=b.id ORDER BY a.id LIMIT 2 OFFSET 1 FOR UPDATE SKIP LOCKED");
    assert_eq!(
        r,
        vec![
            vec![Value::Int(5), Value::Int(5)],
            vec![Value::Int(6), Value::Int(6)]
        ]
    );
    e.execute_with_session(&mut s1, "ROLLBACK").unwrap();
}
#[test]
fn select_audit_failed_export_retry() {
    let (d, e) = fixture(&[]);
    let file = d.path().join("retry.txt");
    let sql = format!(
        "SELECT * FROM missing INTO OUTFILE '{}'",
        crate::util::file::sql_path(&file)
    );
    assert!(e.execute(&sql).is_err());
    assert!(!file.exists());
    e.execute(&format!(
        "SELECT 1 INTO OUTFILE '{}'",
        crate::util::file::sql_path(&file)
    ))
    .unwrap();
    assert_eq!(std::fs::read(file).unwrap(), b"1\n");
}
#[test]
fn select_audit_volatile_subqueries() {
    let (_d, e) = fixture(&[
        "CREATE TABLE o(g INT)",
        "INSERT INTO o VALUES (1),(1),(1),(1)",
        "CREATE TABLE i(g INT)",
        "INSERT INTO i VALUES (1)",
    ]);
    for projection in [
        "EXISTS(SELECT 1 FROM i WHERE i.g=o.g AND (@n:=@n+1)>0)",
        "g IN (SELECT (@n:=@n+1) FROM i WHERE i.g=o.g LIMIT 1)",
        "g IN (SELECT (@n:=@n+1) FROM i WHERE i.g=o.g)",
        "g = ANY(SELECT (@n:=@n+1) FROM i WHERE i.g=o.g)",
        "(g,1)=(SELECT (@n:=@n+1),1 FROM i WHERE i.g=o.g)",
    ] {
        let mut s = SessionContext::default();
        e.execute_with_session(&mut s, "SET @n=0").unwrap();
        session_rows(&e, &mut s, &format!("SELECT {projection} FROM o"));
        assert_eq!(
            session_rows(&e, &mut s, "SELECT @n"),
            vec![vec![Value::Int(4)]],
            "{projection}"
        );
    }
}
#[test]
fn select_audit_outfile_escaping() {
    let (d, e) = fixture(&[]);
    let file = d.path().join("control.txt");
    e.execute(&format!(
        "SELECT CONCAT('a',CHAR(26),'b') INTO OUTFILE '{}'",
        crate::util::file::sql_path(&file)
    ))
    .unwrap();
    assert_eq!(std::fs::read(file).unwrap(), b"a\x1ab\n");
    let file = d.path().join("enclosed.txt");
    e.execute(&format!(
        "SELECT 'a,b' INTO OUTFILE '{}' FIELDS TERMINATED BY ',' ENCLOSED BY '\"'",
        crate::util::file::sql_path(&file)
    ))
    .unwrap();
    assert_eq!(std::fs::read(file).unwrap(), b"\"a,b\"\n");
    let file = d.path().join("empty_line.txt");
    e.execute(&format!(
        "SELECT 1 INTO OUTFILE '{}' FIELDS TERMINATED BY ',' LINES TERMINATED BY ''",
        crate::util::file::sql_path(&file)
    ))
    .unwrap();
    assert_eq!(std::fs::read(file).unwrap(), b"1,");
}
#[test]
fn select_audit_grace_mixed_lanes() {
    let _guard = test_env_lock();
    with_test_env(
        &[
            ("MYDB_HASH_JOIN_MAX_BUILD_ROWS", Some("25")),
            ("MYDB_VECTORIZED", Some("on")),
            ("MYDB_VEC_HASH_JOIN", Some("1")),
            ("MYDB_HASH_JOIN_PARALLELISM", Some("1")),
        ],
        || {
            for ty in ["BIGINT UNSIGNED", "DOUBLE", "DECIMAL(12,2)"] {
                let (_d, e) = fixture(&[
                    "CREATE TABLE a(k INT,v INT)",
                    &format!("CREATE TABLE b(k {ty},v INT)"),
                ]);
                let values = (0..100)
                    .map(|n| format!("({n},{n})"))
                    .collect::<Vec<_>>()
                    .join(",");
                e.execute(&format!("INSERT INTO a VALUES {values}"))
                    .unwrap();
                e.execute(&format!("INSERT INTO b VALUES {values}"))
                    .unwrap();
                let sql = "SELECT COUNT(*) FROM a JOIN (SELECT k,v FROM b) d ON a.k=d.k";
                let before = crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_SPILL_BYTES
                    .load(std::sync::atomic::Ordering::Relaxed);
                let filtered =
                    with_test_env(&[("MYDB_VEC_GRACE_BLOOM", Some("on"))], || rows(&e, sql));
                let after = crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_SPILL_BYTES
                    .load(std::sync::atomic::Ordering::Relaxed);
                assert!(after > before, "must exercise Grace spill for {ty}");
                let unfiltered =
                    with_test_env(&[("MYDB_VEC_GRACE_BLOOM", Some("off"))], || rows(&e, sql));
                assert_eq!(filtered, unfiltered, "{ty}");
                assert_eq!(filtered, vec![vec![Value::Int(100)]], "{ty}");
            }
        },
    );
}
#[test]
fn select_audit_subquery_edges() {
    let (_d, e) = fixture(&[
        "CREATE TABLE o(g INT,qty INT)",
        "INSERT INTO o VALUES (1,10),(1,20),(1,NULL)",
        "CREATE TABLE i(v VARCHAR(10))",
        "INSERT INTO i VALUES ('1'),(NULL)",
    ]);
    assert_eq!(rows(&e,"SELECT 1 IN (SELECT v FROM i LIMIT 2),2 IN (SELECT v FROM i LIMIT 2),NULL IN (SELECT v FROM i LIMIT 0)"),vec![vec![Value::Int(1),Value::Null,Value::Int(0)]]);
    assert_eq!(
        rows(&e, "SELECT (SELECT COUNT(qty)+0) FROM o"),
        vec![vec![Value::Int(2)]]
    );
    let r = rows(
        &e,
        "SELECT g,(SELECT COUNT(qty)+COUNT(*)) FROM o GROUP BY g",
    );
    assert_eq!(r, vec![vec![Value::Int(1), Value::Int(3)]]);
    assert_eq!(
        rows(
            &e,
            "SELECT (SELECT qty AS g FROM o ORDER BY g DESC LIMIT 1) FROM i LIMIT 1"
        ),
        vec![vec![Value::Int(20)]]
    );
    assert_eq!(rows(&e,"SELECT (1,2)=(SELECT 1,2), (SELECT 1,2)=(1,2), (1,NULL)=(SELECT 1,NULL), (1,NULL)<=>(SELECT 1,NULL)"),vec![vec![Value::Int(1),Value::Int(1),Value::Null,Value::Int(1)]]);
    assert_eq!(
        e.execute("SELECT (1,2)=(SELECT g,qty FROM o)")
            .unwrap_err()
            .code(),
        1242
    );
}

#[test]
fn select_audit_numeric_in_domains() {
    for ty in ["DOUBLE", "DECIMAL(65,20)", "BIGINT UNSIGNED"] {
        let (_d, e) = fixture(&[
            &format!("CREATE TABLE a(v {ty})"),
            &format!("CREATE TABLE b(v {ty})"),
            "INSERT INTO a VALUES (1),(3),(NULL)",
            "INSERT INTO b VALUES (1),(2)",
        ]);
        let r = rows(
            &e,
            "SELECT v IN (SELECT v FROM b LIMIT 2),v NOT IN (SELECT v FROM b LIMIT 2) FROM a",
        );
        assert_eq!(
            r,
            vec![
                vec![Value::Int(1), Value::Int(0)],
                vec![Value::Int(0), Value::Int(1)],
                vec![Value::Null, Value::Null]
            ],
            "{ty}"
        );
    }
}

#[test]
fn select_audit_skip_locked_zero_limit_takes_no_locks() {
    let (_d, e) = fixture(&[
        "CREATE TABLE a(id INT PRIMARY KEY)",
        "CREATE TABLE b(id INT PRIMARY KEY)",
        "INSERT INTO a VALUES(1)",
        "INSERT INTO b VALUES(1)",
    ]);
    let mut s1 = SessionContext::default();
    let mut s2 = SessionContext::default();
    e.execute_with_session(&mut s1, "BEGIN").unwrap();
    e.execute_with_session(&mut s2, "BEGIN").unwrap();
    assert!(session_rows(
        &e,
        &mut s1,
        "SELECT a.id FROM a JOIN b ON a.id=b.id LIMIT 0 OFFSET 1 FOR UPDATE SKIP LOCKED"
    )
    .is_empty());
    assert_eq!(
        session_rows(&e, &mut s2, "SELECT id FROM a FOR UPDATE NOWAIT"),
        vec![vec![Value::Int(1)]]
    );
    e.execute_with_session(&mut s1, "ROLLBACK").unwrap();
    e.execute_with_session(&mut s2, "ROLLBACK").unwrap();
}
