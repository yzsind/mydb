use super::*;

fn engine() -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine)
}

fn rows(response: QueryResponse) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = response.materialize().unwrap() else {
        panic!("expected rows")
    };
    result.rows
}

#[test]
fn txn_audit_snapshot_index_predicates_cover_composed_queries() {
    let (_dir, engine) = engine();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,k INT,v INT,KEY ik(k))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,5,10),(2,5,20),(3,6,30)")
        .unwrap();
    let mut reader = SessionContext::default();
    for sql in [
        "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "START TRANSACTION WITH CONSISTENT SNAPSHOT",
        "SELECT COUNT(*) FROM t",
    ] {
        engine.execute_with_session(&mut reader, sql).unwrap();
    }
    engine.execute("UPDATE t SET k=1005 WHERE id=1").unwrap();
    engine.execute("UPDATE t SET id=50000 WHERE id=3").unwrap();
    for sql in [
        "SELECT * FROM t WHERE k=1005",
        "SELECT * FROM t WHERE id BETWEEN 49990 AND 50010",
        "SELECT x.id FROM t x JOIN t y ON x.id=y.id WHERE x.k=1005",
        "SELECT * FROM (SELECT * FROM t WHERE k=1005) q",
        "WITH c AS (SELECT * FROM t WHERE k=1005) SELECT * FROM c",
        "SELECT id FROM t WHERE k=1005 UNION ALL SELECT id FROM t WHERE k=1005",
        "SELECT DISTINCT k FROM t WHERE k=1005",
        "SELECT k,SUM(v) FROM t WHERE k=1005 GROUP BY k",
    ] {
        assert!(
            rows(engine.execute_with_session(&mut reader, sql).unwrap()).is_empty(),
            "{sql}"
        );
    }
    assert_eq!(
        rows(
            engine
                .execute_with_session(&mut reader, "SELECT COUNT(v) FROM t WHERE k=1005")
                .unwrap()
        ),
        vec![vec![Value::Int(0)]]
    );
    assert_eq!(
        rows(
            engine
                .execute_with_session(&mut reader, "SELECT MIN(k) FROM t WHERE k>0")
                .unwrap()
        ),
        vec![vec![Value::Int(5)]]
    );
    engine
        .execute_with_session(&mut reader, "ROLLBACK")
        .unwrap();
}

#[test]
fn txn_audit_forwarded_predicate_first_preserves_limit_and_offset() {
    let (_dir, engine) = engine();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,a INT,b VARCHAR(5000),c INT)")
        .unwrap();
    let values = (1..=3000)
        .map(|i| format!("({i},{i},'x',{i})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    engine
        .execute("UPDATE t SET b=REPEAT('y',4000) WHERE id IN (2,3)")
        .unwrap();
    for (limit, expected) in [
        ("LIMIT 5", vec![1, 2, 3, 4, 5]),
        ("LIMIT 3 OFFSET 1", vec![2, 3, 4]),
    ] {
        let result = rows(
            engine
                .execute(&format!(
                    "SELECT id,a,b,c FROM t WHERE id>=1 ORDER BY id {limit}"
                ))
                .unwrap(),
        );
        assert_eq!(
            result.iter().map(|r| r[0].clone()).collect::<Vec<_>>(),
            expected.into_iter().map(Value::Int).collect::<Vec<_>>()
        );
    }
}

#[test]
fn txn_audit_ignore_foreign_key_failure_compensates_all_cascades() {
    let (_dir, engine) = engine();
    for sql in [
        "CREATE TABLE p(id INT PRIMARY KEY)",
        "CREATE TABLE c1(id INT,FOREIGN KEY(id) REFERENCES p(id) ON UPDATE CASCADE ON DELETE CASCADE)",
        "CREATE TABLE c2(id INT,FOREIGN KEY(id) REFERENCES p(id) ON UPDATE RESTRICT ON DELETE RESTRICT)",
        "INSERT INTO p VALUES(1)", "INSERT INTO c1 VALUES(1)", "INSERT INTO c2 VALUES(1)",
    ] { engine.execute(sql).unwrap(); }
    for sql in [
        "UPDATE IGNORE p SET id=100",
        "DELETE IGNORE FROM p",
        "UPDATE IGNORE p JOIN c1 ON p.id=c1.id SET p.id=100",
        "DELETE IGNORE p FROM p JOIN c1 ON p.id=c1.id",
    ] {
        assert_eq!(
            engine.execute(sql).unwrap(),
            QueryResponse::OkAffected { affected_rows: 0 },
            "{sql}"
        );
        for table in ["p", "c1", "c2"] {
            assert_eq!(
                rows(engine.execute(&format!("SELECT id FROM {table}")).unwrap()),
                vec![vec![Value::Int(1)]],
                "{sql} / {table}"
            );
        }
    }
}

#[test]
fn txn_audit_sql_prepare_uses_execution_schema_metadata() {
    let (_dir, engine) = engine();
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t(id INT,k INT,d DATE)",
        "INSERT INTO t VALUES(100,1,'2020-01-02')",
        "PREPARE p FROM 'SELECT * FROM t'",
        "ALTER TABLE t MODIFY k INT FIRST",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "EXECUTE p")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(
        result
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["k", "id", "d"]
    );
    assert_eq!(result.rows[0][0], Value::Int(1));
    engine
        .execute_with_session(&mut session, "ALTER TABLE t MODIFY d BIGINT")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "EXECUTE p")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("expected rows")
    };
    assert!(matches!(
        result.columns[2].column_type,
        crate::types::ColumnType::LongLong
    ));
}

#[test]
fn txn_audit_multi_update_freezes_join_before_changing_join_keys() {
    let (_dir, engine) = engine();
    for sql in [
        "CREATE TABLE a(id INT PRIMARY KEY,v INT)",
        "CREATE TABLE b(id INT PRIMARY KEY,v INT)",
        "INSERT INTO a VALUES(1,10)",
        "INSERT INTO b VALUES(1,20)",
        "UPDATE a JOIN b ON a.id=b.id SET a.id=2,b.v=a.v+1",
    ] {
        engine.execute(sql).unwrap();
    }
    assert_eq!(
        rows(engine.execute("SELECT * FROM a").unwrap()),
        vec![vec![Value::Int(2), Value::Int(10)]]
    );
    assert_eq!(
        rows(engine.execute("SELECT * FROM b").unwrap()),
        vec![vec![Value::Int(1), Value::Int(11)]]
    );
}

#[test]
fn txn_audit_failed_load_preserves_prior_statement_and_never_resurrects_rows() {
    for (degree, count) in [(1, 4), (2, 4), (4, 20000)] {
        let (dir, engine) = engine();
        let path = dir.path().join("input.csv");
        let mut data = (1..=count)
            .map(|i| format!("{i},{i}\n"))
            .collect::<String>();
        data.push_str("1,999\n");
        std::fs::write(&path, data).unwrap();
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
            .unwrap();
        let mut session = SessionContext::default();
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut session, "INSERT INTO t VALUES(-1,7)")
            .unwrap();
        let sql = format!(
            "LOAD DATA /*+ PARALLEL({degree}) */ INFILE '{}' INTO TABLE t FIELDS TERMINATED BY ','",
            crate::util::file::sql_path(&path)
        );
        assert_eq!(
            engine
                .execute_with_session(&mut session, &sql)
                .unwrap_err()
                .code(),
            1062
        );
        engine.execute_with_session(&mut session, "COMMIT").unwrap();
        assert_eq!(
            rows(engine.execute("SELECT * FROM t ORDER BY id").unwrap()),
            vec![vec![Value::Int(-1), Value::Int(7)]]
        );
        engine.execute("INSERT INTO t VALUES(1,9)").unwrap();
        assert_eq!(
            rows(engine.execute("SELECT v FROM t WHERE id=1").unwrap()),
            vec![vec![Value::Int(9)]]
        );
    }
}

#[test]
fn txn_audit_duplicate_diagnostics_include_key_value_for_batch_and_single_rows() {
    let (_dir, engine) = engine();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    let batch = engine.execute("INSERT INTO t VALUES(17),(17)").unwrap_err();
    engine.execute("INSERT INTO t VALUES(17)").unwrap();
    let single = engine.execute("INSERT INTO t VALUES(17)").unwrap_err();
    assert_eq!(batch.code(), 1062);
    assert_eq!(batch.message(), single.message());
    assert!(batch.message().contains("17"));
}

#[test]
fn txn_audit_rollback_clears_session_even_if_temp_file_cleanup_fails() {
    let (dir, engine) = engine();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TEMPORARY TABLE t(id INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES(1)")
        .unwrap();
    let (visible, internal) = session
        .temp_tables
        .iter()
        .next()
        .map(|(name, table)| (name.clone(), table.internal_key.clone()))
        .unwrap();
    let table = engine.database().table(&internal).unwrap();
    let path =
        crate::storage::naming::temp_tables_dir(dir.path()).join(format!("{}.heap", table.id));
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    session
        .temp_table_journal
        .push(crate::engine::api::TempTableJournalEntry::Create {
            visible_key: visible,
            internal_key: internal,
        });
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(engine
        .execute_with_session(&mut session, "ROLLBACK")
        .is_err());
    assert!(session.current_txn.is_none());
    assert!(!session.explicit_txn);
    assert!(session.active_txn_isolation.is_none());
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
}

#[test]
fn txn_audit_functional_index_rollback_ignores_later_strict_mode() {
    for expression in ["1/(k-1)", "CAST(k AS UNSIGNED)"] {
        let (_dir, engine) = engine();
        let mut session = SessionContext::default();
        let text = expression.starts_with("CAST");
        let data_type = if text { "VARCHAR(20)" } else { "INT" };
        let before = if text { "'not-a-number'" } else { "1" };
        for sql in [
            "SET sql_mode=''".to_owned(),
            format!(
                "CREATE TABLE t(id INT PRIMARY KEY,k {data_type},UNIQUE KEY ix(({expression})))"
            ),
            format!("INSERT INTO t VALUES(1,{before})"),
            "BEGIN".to_owned(),
            "SAVEPOINT s".to_owned(),
            "UPDATE t SET k=2 WHERE id=1".to_owned(),
            "SET sql_mode='STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ZERO_DATE'".to_owned(),
            "ROLLBACK TO s".to_owned(),
            "ROLLBACK".to_owned(),
        ] {
            engine.execute_with_session(&mut session, &sql).unwrap();
        }
        let expected = if text {
            Value::String("not-a-number".into())
        } else {
            Value::Int(1)
        };
        assert_eq!(
            rows(engine.execute("SELECT k FROM t").unwrap()),
            vec![vec![expected]]
        );
        let filter = if text {
            "CAST(k AS UNSIGNED)=0"
        } else {
            "(1/(k-1)) IS NULL"
        };
        engine
            .execute_with_session(&mut session, "SET sql_mode=''")
            .unwrap();
        assert_eq!(
            rows(
                engine
                    .execute_with_session(
                        &mut session,
                        &format!("SELECT id FROM t FORCE INDEX(ix) WHERE {filter}")
                    )
                    .unwrap()
            ),
            vec![vec![Value::Int(1)]]
        );
        // The maintenance scope must not change subsequent strict input checks.
        engine
            .execute_with_session(
                &mut session,
                "SET sql_mode='STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO'",
            )
            .unwrap();
        assert!(engine
            .execute_with_session(&mut session, "INSERT INTO t VALUES(2,'not-a-number')")
            .is_err());
        engine
            .execute_with_session(&mut session, "UPDATE t SET k=2 WHERE id=1")
            .unwrap();
        assert!(engine
            .execute_with_session(&mut session, &format!("UPDATE t SET k={before} WHERE id=1"))
            .is_err());
        engine
            .execute_with_session(&mut session, "SET sql_mode=''")
            .unwrap();
        engine
            .execute_with_session(&mut session, &format!("UPDATE t SET k={before} WHERE id=1"))
            .unwrap();
        engine
            .execute_with_session(
                &mut session,
                "SET sql_mode='STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO'",
            )
            .unwrap();
        engine
            .execute_with_session(&mut session, "DELETE FROM t WHERE id=1")
            .unwrap();
    }
}

#[test]
fn txn_audit_functional_index_compensation_survives_reopen() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    {
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        let mut session = SessionContext::default();
        for sql in [
            "SET sql_mode=''",
            "CREATE TABLE t(id INT PRIMARY KEY,k INT,KEY ix((1/(k-1))))",
            "INSERT INTO t VALUES(1,1)",
            "BEGIN",
            "UPDATE t SET k=2 WHERE id=1",
            "SET sql_mode='STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO'",
            "ROLLBACK",
        ] {
            engine.execute_with_session(&mut session, sql).unwrap();
        }
    }
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    assert_eq!(
        rows(engine.execute("SELECT k FROM t").unwrap()),
        vec![vec![Value::Int(1)]]
    );
    engine.execute("UPDATE t SET k=2 WHERE id=1").unwrap();
}

#[test]
fn txn_audit_load_duplicate_diagnostics_cover_encoded_key_domains() {
    for (definition, data, value) in [
        ("id INT PRIMARY KEY,v INT", "1,10\n1,20\n", "1"),
        (
            "id VARCHAR(20) COLLATE utf8mb4_bin PRIMARY KEY,v INT",
            "Alpha,10\nAlpha,20\n",
            "Alpha",
        ),
        (
            "a DECIMAL(12,2),b INT,PRIMARY KEY(a,b)",
            "1.25,7\n1.25,7\n",
            "1.25-7",
        ),
    ] {
        let (dir, engine) = engine();
        engine
            .execute(&format!("CREATE TABLE t({definition})"))
            .unwrap();
        let path = dir.path().join("duplicates.csv");
        std::fs::write(&path, data).unwrap();
        let error = engine
            .execute(&format!(
                "LOAD DATA /*+ PARALLEL(1) */ INFILE '{}' INTO TABLE t FIELDS TERMINATED BY ','",
                crate::util::file::sql_path(&path)
            ))
            .unwrap_err();
        assert_eq!(error.code(), 1062);
        assert!(error.message().starts_with("Duplicate entry "), "{error:?}");
        assert!(error.message().contains(value), "{error:?}");
    }
}
