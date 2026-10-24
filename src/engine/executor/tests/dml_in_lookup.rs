use super::*;

#[test]
fn dml_in_lookup_materializes_only_distinct_index_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE in_dml (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO in_dml SELECT n,n FROM generate_series(1,100) g(n)")
        .unwrap();
    let table = engine.db.table("test.in_dml").unwrap();
    let selection = Expr::InList {
        expr: Box::new(Expr::Identifier("id".into())),
        list: [1, 7, 7, 99, 200]
            .into_iter()
            .map(|v| Expr::Literal(Literal::Int(v)))
            .collect(),
        negated: false,
    };
    let txn = engine
        .db
        .read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let ids = candidate_row_ids_for_write(&engine.db, txn, &table, Some(&selection))
        .unwrap()
        .expect("indexed IN must not fall back to a heap scan");
    assert_eq!(ids.len(), 3);
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(unique.len(), ids.len());
}

fn rows(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(r) => r.rows,
        QueryResponse::SharedResultSet(r) => r.rows,
        QueryResponse::StreamingResultSet(mut r) => {
            let mut rows = Vec::new();
            while let Some(row) = r.cursor.next_row().unwrap() {
                rows.push(row);
            }
            rows
        }
        other => panic!("expected rows: {other:?}"),
    }
}

fn affected(engine: &StorageQueryEngine, sql: &str, expected: u64) {
    assert!(
        matches!(engine.execute(sql).unwrap(), QueryResponse::OkAffected { affected_rows } if affected_rows == expected),
        "{sql}"
    );
}

#[test]
fn dml_in_lookup_types_residuals_and_fallbacks() {
    for (kind, values, predicate, expected) in [
        (
            "INT",
            "(1,1),(2,2),(3,3),(4,NULL)",
            "k IN (1,1,NULL,3,999) AND id<>3",
            1,
        ),
        (
            "BIGINT UNSIGNED",
            "(1,18446744073709551615),(2,9007199254740993),(3,0)",
            "k IN (18446744073709551615,0)",
            2,
        ),
        (
            "DECIMAL(30,4)",
            "(1,12345678901234567890.1234),(2,-1.2500),(3,2.5000)",
            "k IN (-1.25,12345678901234567890.1234)",
            2,
        ),
        ("DOUBLE", "(1,-1.5),(2,2.5),(3,3.5)", "k IN (-1.5,2.5)", 2),
        ("FLOAT", "(1,-1.5),(2,2.5),(3,3.5)", "k IN (-1.5,2.5)", 2),
        (
            "VARCHAR(30) COLLATE utf8mb4_general_ci",
            "(1,'Alpha'),(2,'ALPHA'),(3,'beta')",
            "k IN ('alpha','alpha')",
            2,
        ),
        ("VARCHAR(30)", "(1,'1'),(2,'01'),(3,'2')", "k IN (1)", 2),
        (
            "VARBINARY(30)",
            "(1,'a'),(2,'b'),(3,'c')",
            "k IN ('a','c')",
            2,
        ),
        (
            "DATE",
            "(1,'2024-01-01'),(2,'2024-02-01'),(3,'2024-03-01')",
            "k IN ('2024-01-01','2024-03-01')",
            2,
        ),
        ("INT", "(1,1),(2,2),(3,NULL)", "k NOT IN (1,NULL)", 0),
        ("INT", "(1,1),(2,2),(3,3)", "k IN (id,2)", 3),
        ("INT", "(1,1),(2,2),(3,3)", "k IN (ABS(-1),1+2)", 2),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        engine
            .execute(&format!(
                "CREATE TABLE typed_in (id INT PRIMARY KEY, k {kind}, KEY ik(k))"
            ))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO typed_in VALUES {values}"))
            .unwrap();
        affected(
            &engine,
            &format!("DELETE FROM typed_in WHERE {predicate}"),
            expected,
        );
    }
}

#[test]
fn dml_in_lookup_prefix_composite_functional_and_partitioned() {
    for (ddl, insert, predicate, expected) in [
        ("CREATE TABLE t (id INT PRIMARY KEY, k VARCHAR(30), KEY ik(k(2)))", "(1,'abc'),(2,'abd'),(3,'xyz')", "k IN ('abc','abc')", 1),
        ("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, KEY iab(a,b))", "(1,1,2),(2,1,3),(3,2,2),(4,1,4)", "a=1 AND b IN (2,3,3)", 2),
        ("CREATE TABLE t (id INT PRIMARY KEY, k VARCHAR(30), KEY ik((LOWER(k))))", "(1,'Alpha'),(2,'Beta'),(3,'Gamma')", "LOWER(k) IN ('alpha','beta')", 2),
        ("CREATE TABLE t (id INT PRIMARY KEY, k INT, KEY ik(k)) PARTITION BY HASH(id) PARTITIONS 2", "(1,1),(2,1),(3,2),(4,3)", "k IN (1,2)", 3),
        ("CREATE TABLE t (id INT PRIMARY KEY, k VARCHAR(30), KEY ik(k(2))) PARTITION BY HASH(id) PARTITIONS 2", "(1,'abc'),(2,'abd'),(3,'abc'),(4,'xyz')", "k IN ('abc')", 2),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        engine.execute(ddl).unwrap();
        engine.execute(&format!("INSERT INTO t VALUES {insert}")).unwrap();
        affected(&engine, &format!("DELETE FROM t WHERE {predicate}"), expected);
    }
}

#[test]
fn dml_in_lookup_update_materializes_before_mutation_and_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, KEY ik(k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,1),(2,2),(3,3)")
        .unwrap();
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "UPDATE t SET k=k+1 WHERE k IN (1,2,2)")
        .unwrap();
    assert_eq!(
        rows(
            engine
                .execute_with_session(&mut session, "SELECT k FROM t ORDER BY id")
                .unwrap()
        ),
        vec![
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(3)]
        ]
    );
    engine
        .execute_with_session(&mut session, "DELETE FROM t WHERE k IN (2,3)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        rows(engine.execute("SELECT k FROM t ORDER BY id").unwrap()),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
}

#[test]
fn dml_in_lookup_binds_then_sorts_and_deduplicates_parameters() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let table = engine.db.table("test.t").unwrap();
    let expr = Expr::InList {
        expr: Box::new(Expr::Identifier("id".into())),
        list: vec![
            Expr::Placeholder(0),
            Expr::Placeholder(1),
            Expr::Placeholder(2),
        ],
        negated: false,
    };
    let txn = engine
        .db
        .read_only_statement_view(IsolationLevel::ReadCommitted, None);
    for params in [
        vec![Value::Int(3), Value::Int(1), Value::Int(3)],
        vec![Value::Int(2), Value::Null, Value::Int(2)],
    ] {
        let expected = if params[0] == Value::Int(3) { 2 } else { 1 };
        let ids = crate::engine::params::with_params(params, || {
            candidate_row_ids_for_write(&engine.db, txn, &table, Some(&expr))
        })
        .unwrap()
        .unwrap();
        assert_eq!(ids.len(), expected);
    }
}

#[test]
fn dml_in_lookup_lossy_numeric_comparison_keeps_all_matches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (9007199254740992),(9007199254740993),(7)")
        .unwrap();
    let table = engine.db.table("test.t").unwrap();
    let predicate = Expr::InList {
        expr: Box::new(Expr::Identifier("id".into())),
        list: vec![Expr::BoundValue(Value::Double(9007199254740992.0))],
        negated: false,
    };
    assert!(crate::planner::write_in_index_access(&table, &predicate).is_none());
    affected(&engine, "DELETE FROM t WHERE id IN (9007199254740992e0)", 2);
}

#[test]
fn dml_in_lookup_triggers_returning_limit_and_fk() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("CREATE TABLE audit (id INT)").unwrap();
    engine.execute("CREATE TRIGGER log_delete AFTER DELETE ON t FOR EACH ROW INSERT INTO audit VALUES (OLD.id)").unwrap();
    engine
        .execute("INSERT INTO t VALUES (1),(2),(3),(4)")
        .unwrap();
    affected(
        &engine,
        "DELETE FROM t WHERE id IN (1,1,2,3) ORDER BY id DESC LIMIT 2",
        2,
    );
    assert_eq!(
        rows(engine.execute("SELECT id FROM audit ORDER BY id").unwrap()),
        vec![vec![Value::Int(2)], vec![Value::Int(3)]]
    );
    assert_eq!(
        rows(
            engine
                .execute("DELETE FROM t WHERE id IN (4,99) RETURNING id")
                .unwrap()
        ),
        vec![vec![Value::Int(4)]]
    );
    engine.execute("INSERT INTO t VALUES (4)").unwrap();
    engine.execute("CREATE TABLE child (id INT PRIMARY KEY, parent_id INT, FOREIGN KEY(parent_id) REFERENCES t(id))").unwrap();
    engine.execute("INSERT INTO child VALUES (1,1)").unwrap();
    assert!(engine.execute("DELETE FROM t WHERE id IN (1,4)").is_err());
    assert_eq!(
        rows(engine.execute("SELECT id FROM t ORDER BY id").unwrap()),
        vec![vec![Value::Int(1)], vec![Value::Int(4)]]
    );
}

#[test]
fn dml_in_lookup_vector_delete_commit_rollback_and_restart() {
    for algorithm in ["HNSW", "IVF"] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, emb VECTOR(3) NOT NULL)")
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES (1,'[1,0,0]'),(2,'[2,0,0]'),(3,'[3,0,0]'),(4,'[4,0,0]')")
            .unwrap();
        let options = if algorithm == "IVF" {
            " COMMENT 'NLIST=2 NPROBE=2'"
        } else {
            ""
        };
        engine
            .execute(&format!(
                "CREATE INDEX ie ON t(emb) USING {algorithm}{options}"
            ))
            .unwrap();
        let knn = "SELECT /*+ IVF_NPROBE(2) */ id FROM t ORDER BY DISTANCE(emb,'[1,0,0]') LIMIT 4";
        let all = vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(4)],
        ];
        assert_eq!(rows(engine.execute(knn).unwrap()), all, "{algorithm}");
        let mut session = SessionContext::default();
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut session, "DELETE FROM t WHERE id IN (1,1,3,NULL)")
            .unwrap();
        assert_eq!(
            rows(engine.execute_with_session(&mut session, knn).unwrap()),
            vec![vec![Value::Int(2)], vec![Value::Int(4)]],
            "{algorithm}"
        );
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
        assert_eq!(rows(engine.execute(knn).unwrap()), all, "{algorithm}");
        affected(&engine, "DELETE FROM t WHERE id IN (1,1,3,NULL)", 2);
        let remaining = vec![vec![Value::Int(2)], vec![Value::Int(4)]];
        assert_eq!(rows(engine.execute(knn).unwrap()), remaining, "{algorithm}");
        drop(engine);
        let engine = open_strict_test_engine(dir.path());
        assert_eq!(
            rows(engine.execute(knn).unwrap()),
            remaining,
            "{algorithm} restart"
        );
    }
}
