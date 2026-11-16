use super::*;

fn result(engine: &StorageQueryEngine, sql: &str) -> ResultSet {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected rows: {sql}");
    };
    result
}

#[test]
fn left_join_is_null_with_inner_neighbors_preserves_nullable_and_outer_cases() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE multi_l(k INT,v INT)",
        "CREATE TABLE multi_r(k INT,tag INT,payload TEXT)",
        "CREATE TABLE multi_d(k INT,keep_row INT)",
        "INSERT INTO multi_l VALUES(1,10),(2,20),(3,30),(NULL,40)",
        "INSERT INTO multi_r VALUES(1,NULL,'wide'),(1,2,'duplicate'),(NULL,3,'null')",
        "INSERT INTO multi_d VALUES(1,1),(2,1),(2,1),(3,1),(9,1),(NULL,1)",
    ] {
        engine.execute(sql).unwrap();
    }
    for suffix in [
        "JOIN multi_d c ON a.k=c.k",
        "RIGHT JOIN multi_d c ON a.k=c.k",
    ] {
        for on in ["a.k=b.k", "a.k<=>b.k", "a.k=b.k AND b.tag>1"] {
            for witness in ["b.k", "b.tag"] {
                for projection in ["a.k,a.v", "a.k,COUNT(*)", "a.k,COUNT(b.tag)"] {
                    let grouped = if projection.contains("COUNT") {
                        "GROUP BY a.k"
                    } else {
                        ""
                    };
                    let sql = format!("SELECT {projection} FROM multi_l a LEFT JOIN multi_r b ON {on} {suffix} WHERE {witness} IS NULL {grouped}");
                    let oracle = sql.replace(
                        &format!("{witness} IS NULL"),
                        &format!("CASE WHEN {witness} IS NULL THEN 1 ELSE 0 END=1"),
                    );
                    let mut actual = result(&engine, &sql);
                    let mut expected = result(&engine, &oracle);
                    actual.rows.sort_by_cached_key(|row| format!("{row:?}"));
                    expected.rows.sort_by_cached_key(|row| format!("{row:?}"));
                    assert_eq!(actual.rows, expected.rows, "{sql}");
                    assert_eq!(actual.columns, expected.columns, "{sql}");
                }
            }
        }
    }
    let explain = super::explain_analyze_tree(&engine,
        "SELECT a.k FROM multi_l a LEFT JOIN multi_r b ON a.k=b.k JOIN multi_d c ON a.k=c.k WHERE b.k IS NULL");
    assert!(explain.to_lowercase().contains("anti"), "{explain}");
}

#[test]
fn left_join_is_null_prunes_payload_and_preserves_schema() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE l (id VARCHAR(64), v INT)",
        "CREATE TABLE r (id VARCHAR(64) PRIMARY KEY, payload JSON, note TEXT)",
        "INSERT INTO l VALUES ('a',1),('B',2),('missing',3),('missing',4),(NULL,5)",
        "INSERT INTO r VALUES ('A','{\"n\":1}','wide'),('b','{}','other')",
    ] {
        engine.execute(sql).unwrap();
    }
    for hint in ["", "/*+ HASH_JOIN(r) */", "/*+ INDEX_NLJ(r) */"] {
        let sql = format!(
            "SELECT {hint} * FROM l LEFT JOIN r ON l.id=r.id WHERE r.id IS NULL ORDER BY l.v"
        );
        let rs = result(&engine, &sql);
        assert_eq!(
            rs.rows,
            vec![
                vec![
                    Value::String("missing".into()),
                    Value::Int(3),
                    Value::Null,
                    Value::Null,
                    Value::Null
                ],
                vec![
                    Value::String("missing".into()),
                    Value::Int(4),
                    Value::Null,
                    Value::Null,
                    Value::Null
                ],
                vec![
                    Value::Null,
                    Value::Int(5),
                    Value::Null,
                    Value::Null,
                    Value::Null
                ],
            ],
            "{hint}"
        );
        assert_eq!(
            rs.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "v", "id", "payload", "note"]
        );
        let plan = explain_analyze_tree(&engine, &sql);
        assert!(plan.contains("anti join"), "{plan}");
        assert!(plan.contains("NULL extension"), "{plan}");
        assert!(plan.contains("execution_mode=vector"), "{plan}");
        assert!(!plan.contains("spill_reason=memory_limit"), "{plan}");
        if hint.contains("INDEX_NLJ") {
            let json = result(&engine, &format!("EXPLAIN FORMAT=JSON {sql}"));
            assert!(
                json.rows[0][0]
                    .to_display_string()
                    .contains("\"covering\": true"),
                "{json:?}"
            );
        }
    }
}

#[test]
fn left_join_is_null_nullable_key_duplicates_and_residuals() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE l (k INT, v INT)",
        "CREATE TABLE r (k INT, v INT, note TEXT)",
        "INSERT INTO l VALUES (1,10),(2,20),(3,30),(NULL,40)",
        "INSERT INTO r VALUES (1,5,'x'),(1,15,'y'),(2,5,'z'),(NULL,50,'null')",
    ] {
        engine.execute(sql).unwrap();
    }
    for hint in ["/*+ HASH_JOIN(r) */", "/*+ NL_JOIN(r) */"] {
        let sql = format!("SELECT {hint} l.k,l.v,r.* FROM l LEFT JOIN r ON l.k=r.k AND r.v>l.v WHERE r.k IS NULL AND l.v>=20 AND COALESCE(r.note,'ok')='ok' ORDER BY l.v LIMIT 2");
        assert_eq!(
            result(&engine, &sql).rows,
            vec![
                vec![
                    Value::Int(2),
                    Value::Int(20),
                    Value::Null,
                    Value::Null,
                    Value::Null
                ],
                vec![
                    Value::Int(3),
                    Value::Int(30),
                    Value::Null,
                    Value::Null,
                    Value::Null
                ],
            ]
        );
        let plan = explain_analyze_tree(&engine, &sql);
        assert!(plan.contains("anti join"), "{plan}");
    }
    assert_eq!(result(&engine, "SELECT COUNT(*),COUNT(r.note),SUM(l.v) FROM l LEFT JOIN r ON l.k=r.k AND r.v>l.v WHERE r.k IS NULL").rows,
        vec![vec![Value::Int(3), Value::Int(0), Value::DecimalCompact(90, 0)]]);
}

#[test]
fn left_join_is_null_rejects_unsafe_null_tests() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE l (k INT)",
        "CREATE TABLE r (k INT, v INT)",
        "INSERT INTO l VALUES (1),(2),(NULL)",
        "INSERT INTO r VALUES (1,NULL),(NULL,42)",
    ] {
        engine.execute(sql).unwrap();
    }
    for (sql, count) in [
        (
            "SELECT * FROM l LEFT JOIN r ON l.k=r.k WHERE r.v IS NULL",
            3,
        ),
        (
            "SELECT * FROM l LEFT JOIN r ON l.k<=>r.k WHERE r.k IS NULL",
            2,
        ),
        (
            "SELECT * FROM l LEFT JOIN r ON l.k=r.k WHERE r.k IS NULL OR l.k=1",
            3,
        ),
    ] {
        assert_eq!(result(&engine, sql).rows.len(), count);
        assert!(!explain_analyze_tree(&engine, sql).contains("NULL extension"));
    }
}

#[test]
fn left_join_is_null_not_null_witness_and_empty_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE l (k INT)",
        "CREATE TABLE r (k INT, witness INT NOT NULL)",
        "INSERT INTO l VALUES (1),(2),(NULL)",
    ] {
        engine.execute(sql).unwrap();
    }
    let sql = "SELECT * FROM l LEFT JOIN r ON l.k<=>r.k WHERE r.witness IS NULL ORDER BY l.k";
    assert_eq!(result(&engine, sql).rows.len(), 3);
    engine
        .execute("INSERT INTO r VALUES (NULL,1),(1,2)")
        .unwrap();
    assert_eq!(
        result(&engine, sql).rows,
        vec![vec![Value::Int(2), Value::Null, Value::Null]]
    );
    engine.execute("DELETE FROM l").unwrap();
    assert!(result(&engine, sql).rows.is_empty());
    assert_eq!(result(&engine, sql).columns.len(), 3);
}

#[test]
fn left_join_is_null_expression_differential() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE l (k INT, v VARCHAR(20))",
        "CREATE TABLE r (k INT, tag INT NOT NULL, amount DECIMAL(12,2), payload JSON)",
        "INSERT INTO l VALUES (1,'one'),(2,'two'),(3,'three'),(NULL,'null')",
        "INSERT INTO r VALUES (1,1,2.34,'{}'),(1,2,NULL,'[]'),(NULL,3,5.67,NULL)",
    ] {
        engine.execute(sql).unwrap();
    }
    for projection in [
        "*",
        "b.*",
        "a.v,COALESCE(b.amount,9),b.payload",
        "COUNT(*),COUNT(b.tag),MAX(b.amount)",
    ] {
        for condition in ["a.k=b.k", "a.k=b.k AND b.amount>3", "a.k<=>b.k"] {
            for witness in ["b.k", "b.tag"] {
                let optimized = format!("SELECT /*+ HASH_JOIN(b) */ {projection} FROM l a LEFT JOIN r b ON {condition} WHERE {witness} IS NULL");
                // CASE keeps the equivalent predicate opaque to the anti-join proof.
                let reference = format!("SELECT /*+ HASH_JOIN(b) */ {projection} FROM l a LEFT JOIN r b ON {condition} WHERE CASE WHEN {witness} IS NULL THEN 1 ELSE 0 END=1");
                let mut actual = result(&engine, &optimized);
                let mut expected = result(&engine, &reference);
                actual.rows.sort_by_cached_key(|row| format!("{row:?}"));
                expected.rows.sort_by_cached_key(|row| format!("{row:?}"));
                assert_eq!(actual.rows, expected.rows, "{optimized}");
                assert_eq!(
                    actual
                        .columns
                        .iter()
                        .map(|c| (&c.name, c.column_type))
                        .collect::<Vec<_>>(),
                    expected
                        .columns
                        .iter()
                        .map(|c| (&c.name, c.column_type))
                        .collect::<Vec<_>>(),
                    "{optimized}"
                );
            }
        }
    }
    assert!(result(
        &engine,
        "SELECT l.k FROM l LEFT JOIN r ON l.k=r.k WHERE r.k IS NULL AND r.tag=1"
    )
    .rows
    .is_empty());
    engine.execute("DELETE FROM r").unwrap();
    assert_eq!(
        result(
            &engine,
            "SELECT COUNT(*) FROM l LEFT JOIN r ON TRUE WHERE r.tag IS NULL"
        )
        .rows,
        vec![vec![Value::Int(4)]]
    );
}
