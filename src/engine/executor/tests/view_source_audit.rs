use super::support::execute_materialized as exec;
use super::*;

fn setup() -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    setup_engine(false)
}

fn setup_engine(persistent: bool) -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = if persistent {
        open_strict_test_engine(dir.path())
    } else {
        open_test_engine(dir.path())
    };
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, val INT, secret INT)",
        "INSERT INTO t VALUES (1,100,42),(2,110,43)",
        "CREATE VIEW v AS SELECT id, val FROM t",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    (dir, engine, session)
}

#[test]
fn view_audit_hidden_columns() {
    let (_dir, engine, mut session) = setup();
    for sql in [
        "UPDATE v SET secret=99 WHERE id=1",
        "UPDATE v SET val=secret WHERE id=1",
        "UPDATE v SET val=(SELECT secret) WHERE id=1",
        "SELECT (SELECT secret) FROM v FOR UPDATE",
        "DELETE FROM v WHERE EXISTS (SELECT 1 WHERE secret=42)",
        "INSERT INTO v (id, secret) VALUES (3,99)",
        "INSERT INTO v VALUES (1,100) ON DUPLICATE KEY UPDATE secret=99",
        "DELETE FROM v WHERE secret=42",
        "SELECT secret FROM v FOR UPDATE",
        "SELECT id FROM v ORDER BY secret FOR UPDATE",
        "DELETE FROM v ORDER BY secret LIMIT 1",
        "UPDATE v SET val=1 ORDER BY secret LIMIT 1",
        "UPDATE v SET val=1 RETURNING secret",
    ] {
        let err = exec(&engine, &mut session, sql).expect_err(sql);
        assert_eq!(err.code(), 1054, "{sql}: {err:?}");
    }
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT secret FROM t ORDER BY id").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(42)], vec![Value::Int(43)]]);
}

#[test]
fn view_audit_local_check() {
    let (_dir, engine, mut session) = setup();
    for sql in [
        "CREATE VIEW low AS SELECT id,val FROM t WHERE val<200 WITH CASCADED CHECK OPTION",
        "CREATE VIEW high AS SELECT id,val FROM low WHERE val>50 WITH LOCAL CHECK OPTION",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    for sql in [
        "INSERT INTO high VALUES (3,999)",
        "UPDATE high SET val=999 WHERE id=1",
    ] {
        assert_eq!(exec(&engine, &mut session, sql).unwrap_err().code(), 1369);
    }
    exec(&engine, &mut session, "INSERT INTO high VALUES (3,150)").unwrap();
}

#[test]
fn view_audit_nested_wildcard_dml() {
    let (_dir, engine, mut session) = setup();
    for sql in [
        "CREATE VIEW outer_v AS SELECT v.* FROM v",
        "INSERT INTO outer_v VALUES (3,120)",
        "UPDATE outer_v SET val=130 WHERE id=3",
        "DELETE FROM outer_v WHERE id=3",
        "INSERT INTO outer_v SELECT 4,140",
    ] {
        exec(&engine, &mut session, sql).expect(sql);
    }
}

#[test]
fn view_audit_assignment_subquery() {
    let (_dir, engine, mut session) = setup();
    exec(
        &engine,
        &mut session,
        "CREATE VIEW m AS SELECT MAX(id) AS n FROM t",
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        "UPDATE t SET val=(SELECT n FROM m) WHERE id=1",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT val FROM t WHERE id=1").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn view_audit_dependency_revalidation() {
    let (_dir, engine, mut session) = setup();
    for sql in [
        "CREATE VIEW dependent AS SELECT * FROM v",
        "ALTER VIEW v AS SELECT id FROM t",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    assert_eq!(
        exec(&engine, &mut session, "SELECT * FROM dependent")
            .unwrap_err()
            .code(),
        1356
    );
    exec(
        &engine,
        &mut session,
        "CREATE OR REPLACE VIEW v AS SELECT id,val FROM t",
    )
    .unwrap();
    exec(&engine, &mut session, "SELECT * FROM dependent").unwrap();
    for sql in [
        "CREATE DATABASE other",
        "CREATE VIEW other.cross_v AS SELECT * FROM test.v",
        "DROP DATABASE test",
    ] {
        exec(&engine, &mut session, sql).expect(sql);
    }
    assert_eq!(
        exec(&engine, &mut session, "SELECT * FROM other.cross_v")
            .unwrap_err()
            .code(),
        1356
    );
}

#[test]
fn view_audit_reject_ignored_locks() {
    let (_dir, engine, mut session) = setup();
    for sql in [
        "CREATE VIEW locked AS SELECT id FROM t FOR UPDATE",
        "CREATE VIEW locked AS SELECT id FROM t WHERE id IN (SELECT id FROM t FOR SHARE)",
    ] {
        assert_eq!(
            exec(&engine, &mut session, sql).expect_err(sql).code(),
            1235
        );
    }
}

#[test]
fn view_audit_temporary_subquery() {
    let (_dir, engine, mut session) = setup();
    exec(&engine, &mut session, "CREATE TEMPORARY TABLE tmp (id INT)").unwrap();
    assert_eq!(
        exec(
            &engine,
            &mut session,
            "CREATE VIEW bad AS SELECT id FROM t WHERE id IN (SELECT id FROM tmp)"
        )
        .unwrap_err()
        .code(),
        1352
    );
}

#[test]
fn view_audit_materialized_cte() {
    let (_dir, engine, mut session) = setup();
    for sql in [
        "CREATE VIEW cte_v AS WITH c AS (SELECT id FROM t) SELECT * FROM c UNION ALL SELECT * FROM c",
        "CREATE VIEW cte_renamed(a) AS WITH c(x) AS (SELECT id FROM t), d AS (SELECT x FROM c) SELECT * FROM d UNION ALL SELECT * FROM c",
    ] { exec(&engine, &mut session, sql).expect(sql); }
    for sql in [
        "SELECT * FROM cte_v ORDER BY id",
        "SELECT * FROM cte_renamed ORDER BY a",
    ] {
        let QueryResponse::ResultSet(rs) = exec(&engine, &mut session, sql).expect(sql) else {
            panic!()
        };
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(2)]
            ]
        );
    }
}

#[test]
fn view_audit_scope_and_replay() {
    let (_dir, engine, mut session) = setup();
    for sql in [
        "CREATE VIEW renamed(k,n) AS SELECT id,val FROM t",
        "UPDATE renamed SET n=CASE WHEN n>0 THEN n+1 ELSE 0 END WHERE k=1",
        "UPDATE renamed SET n=(SELECT MAX(secret) FROM t) WHERE k=2",
        "SELECT k AS x FROM renamed ORDER BY x FOR UPDATE",
        "CREATE VIEW third AS SELECT * FROM renamed",
        "CREATE VIEW fourth AS SELECT * FROM third",
    ] {
        exec(&engine, &mut session, sql).expect(sql);
    }
    for sql in [
        "SELECT k FROM renamed GROUP BY secret FOR UPDATE",
        "SELECT k FROM renamed HAVING secret>0 FOR UPDATE",
    ] {
        assert_eq!(
            exec(&engine, &mut session, sql).expect_err(sql).code(),
            1054
        );
    }
    for name in ["v", "renamed"] {
        let QueryResponse::ResultSet(rs) =
            exec(&engine, &mut session, &format!("SHOW CREATE VIEW {name}")).unwrap()
        else {
            panic!()
        };
        let ddl = rs.rows[0][1]
            .to_display_string()
            .replace(&format!("VIEW `{name}`"), &format!("VIEW `{name}_copy`"));
        exec(&engine, &mut session, &ddl).unwrap();
        let QueryResponse::ResultSet(original) =
            exec(&engine, &mut session, &format!("SELECT * FROM {name}")).unwrap()
        else {
            panic!()
        };
        let QueryResponse::ResultSet(copy) =
            exec(&engine, &mut session, &format!("SELECT * FROM {name}_copy")).unwrap()
        else {
            panic!()
        };
        assert_eq!(original.rows, copy.rows);
        assert_eq!(
            original.columns.iter().map(|c| &c.name).collect::<Vec<_>>(),
            copy.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
        );
    }
    exec(
        &engine,
        &mut session,
        "ALTER VIEW renamed AS SELECT id FROM t",
    )
    .unwrap();
    assert_eq!(
        exec(&engine, &mut session, "SELECT * FROM fourth")
            .unwrap_err()
            .code(),
        1356
    );
    exec(
        &engine,
        &mut session,
        "ALTER VIEW renamed(k,n) AS SELECT id,val FROM t",
    )
    .unwrap();
    exec(&engine, &mut session, "SELECT * FROM fourth").unwrap();
}

#[test]
fn view_audit_check_option_matrix() {
    let (_dir, engine, mut session) = setup();
    for sql in [
        "CREATE VIEW unchecked AS SELECT id,val FROM t WHERE val<200",
        "CREATE VIEW local_v AS SELECT id,val FROM unchecked WHERE val>50 WITH LOCAL CHECK OPTION",
        "CREATE VIEW cascaded_v AS SELECT id,val FROM unchecked WHERE val>50 WITH CASCADED CHECK OPTION",
        "INSERT INTO local_v VALUES (3,999)",
    ] { exec(&engine, &mut session, sql).expect(sql); }
    assert_eq!(
        exec(
            &engine,
            &mut session,
            "INSERT INTO cascaded_v VALUES (4,999)"
        )
        .unwrap_err()
        .code(),
        1369
    );
    assert_eq!(
        exec(&engine, &mut session, "INSERT INTO local_v VALUES (5,1)")
            .unwrap_err()
            .code(),
        1369
    );
}

#[test]
fn view_audit_cte_materialized_once_and_reopen() {
    let (dir, engine, mut session) = setup_engine(true);
    exec(&engine, &mut session, "CREATE VIEW random_v AS WITH c AS (SELECT RAND() AS x) SELECT a.x=b.x AS same FROM c a JOIN c b").unwrap();
    for _ in 0..3 {
        let QueryResponse::ResultSet(rs) =
            exec(&engine, &mut session, "SELECT same FROM random_v").unwrap()
        else {
            panic!()
        };
        assert_eq!(rs.rows[0][0].to_display_string(), "1");
    }
    exec(
        &engine,
        &mut session,
        "CREATE VIEW nested_v AS SELECT * FROM v",
    )
    .unwrap();
    engine.checkpoint().unwrap();
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "INSERT INTO nested_v VALUES (3,150)").unwrap();
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut session, "SELECT same FROM random_v").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows[0][0].to_display_string(), "1");
}

#[test]
fn view_audit_join_locks_both_sources() {
    let (_dir, engine, mut session) = setup();
    for sql in [
        "CREATE TABLE right_t (id INT PRIMARY KEY, n INT)",
        "INSERT INTO right_t VALUES (1,9),(2,8)",
        "CREATE VIEW renamed AS SELECT id AS rid, val AS rval FROM t",
        "BEGIN",
    ] {
        exec(&engine, &mut session, sql).unwrap();
    }
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut session,
        "SELECT * FROM renamed JOIN right_t ON rid=right_t.id WHERE rid=1 FOR UPDATE",
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(
        rs.columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["rid", "rval", "id", "n"]
    );
    assert_eq!(
        rs.rows,
        vec![vec![
            Value::Int(1),
            Value::Int(100),
            Value::Int(1),
            Value::Int(9)
        ]]
    );
    let mut other = SessionContext::default();
    exec(&engine, &mut other, "BEGIN").unwrap();
    for table in ["t", "right_t"] {
        assert_eq!(
            exec(
                &engine,
                &mut other,
                &format!("SELECT * FROM {table} WHERE id=1 FOR UPDATE NOWAIT")
            )
            .unwrap_err()
            .code(),
            3572
        );
        exec(
            &engine,
            &mut other,
            &format!("SELECT * FROM {table} WHERE id=2 FOR UPDATE NOWAIT"),
        )
        .unwrap();
    }
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut other, "SELECT rid FROM renamed JOIN right_t ON rid=right_t.id ORDER BY rid FOR UPDATE SKIP LOCKED").unwrap() else { panic!() };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
    exec(&engine, &mut other, "ROLLBACK").unwrap();
    exec(&engine, &mut session, "COMMIT").unwrap();
    exec(
        &engine,
        &mut other,
        "SELECT * FROM renamed JOIN right_t ON rid=right_t.id FOR UPDATE NOWAIT",
    )
    .unwrap();
}

#[test]
fn view_audit_wildcard_with_subquery_predicate() {
    let (_dir, engine, mut session) = setup();
    for sql in [
        "CREATE VIEW filtered AS SELECT * FROM v WHERE id IN (SELECT id FROM t)",
        "UPDATE filtered SET val=120 WHERE id=1",
        "ALTER VIEW v AS SELECT id FROM t",
    ] {
        exec(&engine, &mut session, sql).expect(sql);
    }
    assert_eq!(
        exec(&engine, &mut session, "SELECT * FROM filtered")
            .unwrap_err()
            .code(),
        1356
    );
}

#[test]
fn view_audit_join_rechecks_after_lock_wait() {
    let (_dir, engine, mut blocker) = setup();
    for sql in [
        "CREATE TABLE r (n INT PRIMARY KEY)",
        "INSERT INTO r VALUES(100)",
        "BEGIN",
        "SELECT id FROM t WHERE id=1 FOR UPDATE",
    ] {
        exec(&engine, &mut blocker, sql).unwrap();
    }
    std::thread::scope(|scope| {
        let (send, recv) = std::sync::mpsc::channel();
        let engine_ref = &engine;
        let worker = scope.spawn(move || {
            let mut reader = SessionContext::default();
            send.send(()).unwrap();
            exec(
                engine_ref,
                &mut reader,
                "SELECT v.id FROM v JOIN r ON v.val=r.n FOR UPDATE",
            )
            .unwrap()
        });
        recv.recv().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        exec(&engine, &mut blocker, "UPDATE t SET val=200 WHERE id=1").unwrap();
        exec(&engine, &mut blocker, "COMMIT").unwrap();
        let QueryResponse::ResultSet(result) = worker.join().unwrap() else {
            panic!()
        };
        assert!(
            result.rows.is_empty(),
            "join returned pre-lock values: {:?}",
            result.rows
        );
    });
}

#[test]
fn view_audit_nested_declared_types() {
    let (_dir, engine, mut session) = setup();
    for sql in ["CREATE TABLE typed (id INT NOT NULL, d DECIMAL(12,3), s VARCHAR(12), dt DATETIME(3), b VARBINARY(8), j JSON)", "CREATE VIEW typed_v AS SELECT * FROM typed", "CREATE VIEW typed_v2 AS SELECT typed_v.* FROM typed_v"] {
        exec(&engine, &mut session, sql).unwrap();
    }
    let base = engine
        .database()
        .view_definition("test", "typed_v")
        .unwrap();
    let nested = engine
        .database()
        .view_definition("test", "typed_v2")
        .unwrap();
    for (a, b) in base.columns.iter().zip(&nested.columns) {
        assert_eq!(a.data_type, b.data_type, "{}", a.name);
        assert_eq!(a.nullable, b.nullable, "{}", a.name);
    }
}

#[test]
fn view_review_sql_prepare() {
    let (_dir, engine, mut s) = setup();
    exec(
        &engine,
        &mut s,
        "PREPARE q FROM 'SELECT * FROM v WHERE id=?'",
    )
    .unwrap();
    exec(&engine, &mut s, "SET @id=1").unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, "EXECUTE q USING @id").unwrap() else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1), Value::Int(100)]]);
    exec(&engine, &mut s, "ALTER VIEW v AS SELECT id FROM t").unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, "EXECUTE q USING @id").unwrap() else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn view_review_multi_update_delete() {
    let (_dir, engine, mut s) = setup();
    for sql in [
        "CREATE TABLE u (id INT PRIMARY KEY, val INT)",
        "INSERT INTO u VALUES (1,7),(2,8)",
        "CREATE VIEW renamed AS SELECT id AS k,val AS x FROM t WHERE val<110",
        "UPDATE renamed r JOIN u ON u.id=r.k SET r.x=105,u.val=9 WHERE r.k=1",
    ] {
        exec(&engine, &mut s, sql).expect(sql);
    }
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut s,
        "SELECT t.val,u.val FROM t JOIN u ON t.id=u.id ORDER BY t.id",
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(105), Value::Int(9)],
            vec![Value::Int(110), Value::Int(8)]
        ]
    );
    exec(
        &engine,
        &mut s,
        "DELETE renamed FROM renamed JOIN u ON u.id=renamed.k WHERE u.val=9",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, "SELECT id FROM t").unwrap() else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn view_review_locking_subqueries() {
    let (_dir, engine, mut s) = setup();
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut s,
        "SELECT id FROM t WHERE id IN (SELECT id FROM t WHERE val=110) FOR UPDATE",
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
    for sql in [
        "SELECT v.id FROM test.v WHERE v.id IN (SELECT id FROM t WHERE val=110) FOR UPDATE",
        "SELECT test.v.id FROM test.v WHERE test.v.id=2 FOR UPDATE",
    ] {
        let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, sql).expect(sql) else {
            panic!()
        };
        assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
    }
    for source in ["t", "v"] {
        for predicate in [
            "id IN (SELECT id FROM t WHERE val=110)",
            "val=(SELECT MAX(val) FROM v)",
            "EXISTS (SELECT 1 FROM t x WHERE x.id=outer_t.id AND x.val=110)",
        ] {
            let sql = format!("SELECT id FROM {source} outer_t WHERE {predicate} FOR UPDATE");
            let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, &sql).expect(&sql) else {
                panic!()
            };
            assert_eq!(rs.rows, vec![vec![Value::Int(2)]], "{sql}");
        }
    }
}

#[test]
fn view_review_metadata_and_errors() {
    let (_dir, engine, mut s) = setup();
    for sql in [
        "ALTER VIEW t AS SELECT 1",
        "DROP VIEW t",
        "DROP VIEW IF EXISTS t",
    ] {
        assert_eq!(
            exec(&engine, &mut s, sql).unwrap_err().code(),
            1347,
            "{sql}"
        );
    }
    exec(
        &engine,
        &mut s,
        "CREATE VIEW IF NOT EXISTS v AS SELECT id FROM t",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, "SHOW WARNINGS").unwrap() else {
        panic!()
    };
    assert!(rs.rows.iter().any(|r| r[1].to_display_string() == "1050"));
    exec(
        &engine,
        &mut s,
        "CREATE VIEW agg AS SELECT MAX(id) AS id FROM t",
    )
    .unwrap();
    assert_eq!(
        exec(&engine, &mut s, "INSERT INTO agg VALUES (3)")
            .unwrap_err()
            .code(),
        1471
    );
    exec(
        &engine,
        &mut s,
        "CREATE TABLE req (id INT PRIMARY KEY, n INT NOT NULL)",
    )
    .unwrap();
    exec(&engine, &mut s, "CREATE VIEW vr AS SELECT id FROM req").unwrap();
    assert_eq!(
        exec(&engine, &mut s, "INSERT INTO vr VALUES (1)")
            .unwrap_err()
            .code(),
        1423
    );
}

#[test]
fn view_review_system_table() {
    let (_dir, engine, mut s) = setup();
    exec(
        &engine,
        &mut s,
        "CREATE VIEW vsys AS SELECT * FROM information_schema.tables",
    )
    .unwrap();
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut s,
        "SELECT TABLE_NAME FROM vsys WHERE TABLE_SCHEMA='test' AND TABLE_NAME='t'",
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::String("t".into())]]);
}

#[test]
fn view_review_deep_chain() {
    let (_dir, engine, mut s) = setup();
    let mut previous = "v".to_owned();
    for n in 0..24 {
        let name = format!("deep_{n}");
        exec(
            &engine,
            &mut s,
            &format!("CREATE VIEW {name} AS SELECT id FROM {previous}"),
        )
        .unwrap();
        previous = name;
    }
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut s,
        &format!("SELECT * FROM {previous} ORDER BY id"),
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

#[test]
fn view_review_join_insert() {
    let (_dir, engine, mut s) = setup();
    for sql in [
        "CREATE TABLE u (id INT PRIMARY KEY, b INT)",
        "CREATE VIEW jv AS SELECT t.id,t.val,u.b FROM t JOIN u ON t.id=u.id",
        "INSERT INTO jv (id,val) VALUES (3,120)",
    ] {
        exec(&engine, &mut s, sql).expect(sql);
    }
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut s, "SELECT val FROM t WHERE id=3").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(120)]]);
    assert!(exec(&engine, &mut s, "INSERT INTO jv (id,b) VALUES (4,4)").is_err());
}

#[test]
fn view_review_subquery_scopes() {
    let (_dir, engine, mut s) = setup();
    for sql in [
        "CREATE VIEW renamed(k,n) AS SELECT id,val FROM t",
        "UPDATE renamed SET n=(SELECT MAX(secret) FROM t) WHERE k=2",
        "UPDATE renamed r SET n=200 WHERE EXISTS (SELECT 1 FROM t x WHERE x.id=r.k AND x.secret=42)",
        "UPDATE renamed SET n=n+1 WHERE k IN (SELECT k FROM renamed WHERE n=200)",
        "INSERT INTO renamed(k,n) VALUES(1,1) ON DUPLICATE KEY UPDATE n=(SELECT MAX(secret) FROM t)",
        "CREATE TABLE other_t (k INT,n INT)",
        "INSERT INTO other_t VALUES(2,700)",
        "UPDATE renamed r SET n=(SELECT n FROM other_t r WHERE r.k=2) WHERE r.k=2",
    ] {exec(&engine,&mut s,sql).expect(sql);}
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut s, "SELECT id,val,secret FROM t ORDER BY id").unwrap()
    else {
        panic!()
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::Int(43), Value::Int(42)],
            vec![Value::Int(2), Value::Int(700), Value::Int(43)]
        ]
    );
    for sql in [
        "UPDATE renamed SET n=(SELECT secret) WHERE k=1",
        "INSERT INTO renamed(k,n) VALUES(1,1) ON DUPLICATE KEY UPDATE n=(SELECT secret)",
        "UPDATE renamed SET n=0 WHERE k IN (SELECT k FROM renamed WHERE secret=42)",
    ] {
        assert_eq!(
            exec(&engine, &mut s, sql).unwrap_err().code(),
            1054,
            "{sql}"
        );
    }
}

#[test]
fn view_review_definer_and_column_errors() {
    let (_dir, engine, mut root) = setup();
    for sql in [
        "CREATE USER 'view_def'@'%' IDENTIFIED BY 'pw'",
        "CREATE USER 'view_user'@'%' IDENTIFIED BY 'pw'",
        "GRANT SELECT ON test.t TO 'view_def'@'%'",
        "CREATE DEFINER='view_def'@'%' VIEW secured AS SELECT id,val FROM t",
        "GRANT SELECT ON test.secured TO 'view_user'@'%'",
        "GRANT SELECT(id) ON test.v TO 'view_user'@'%'",
    ] {
        exec(&engine, &mut root, sql).expect(sql);
    }
    let mut user = SessionContext::default();
    user.authenticated_as = crate::engine::auth::AccountKey::new("view_user", "%");
    exec(&engine, &mut user, "SELECT * FROM secured").unwrap();
    exec(&engine, &mut user, "SELECT id FROM v").unwrap();
    let err = exec(&engine, &mut user, "SELECT val FROM v").unwrap_err();
    assert_eq!(err.code(), 1143, "{err:?}");
    exec(
        &engine,
        &mut root,
        "REVOKE SELECT ON test.t FROM 'view_def'@'%'",
    )
    .unwrap();
    let err = exec(&engine, &mut user, "SELECT * FROM secured").unwrap_err();
    assert_eq!(err.code(), 1356, "{err:?}");
    assert!(!err.message().contains("test.t"));
    exec(&engine, &mut root, "DROP USER 'view_def'@'%'").unwrap();
    assert_eq!(
        exec(&engine, &mut user, "SELECT * FROM secured")
            .unwrap_err()
            .code(),
        1449
    );
}

#[test]
fn view_review_joined_target_and_same_names() {
    let (_dir, engine, mut s) = setup();
    for sql in [
        "CREATE TABLE u (id INT PRIMARY KEY,val INT)",
        "INSERT INTO u VALUES (1,10),(2,20)",
        "CREATE VIEW uv AS SELECT id,val AS secret FROM u",
        "UPDATE t JOIN uv x ON t.id=x.id SET t.secret=x.secret,x.secret=t.val WHERE t.id=1",
        "DELETE x FROM t JOIN uv x ON t.id=x.id WHERE t.id=1",
    ] {
        exec(&engine, &mut s, sql).expect(sql);
    }
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut s, "SELECT secret FROM t WHERE id=1").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(10)]]);
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, "SELECT id,val FROM u").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2), Value::Int(20)]]);
}

#[test]
fn view_review_subquery_alias_and_function_units() {
    let (_dir, engine, mut s) = setup();
    for sql in [
        "CREATE VIEW renamed(k,n) AS SELECT id,val FROM t",
        "UPDATE renamed SET n=TIMESTAMPDIFF(DAY,'2020-01-01','2020-01-03') WHERE k=1",
        "UPDATE renamed SET n=(SELECT MAX(secret) AS n FROM t HAVING n>0 ORDER BY n) WHERE k=2",
    ] {
        exec(&engine, &mut s, sql).expect(sql);
    }
    let QueryResponse::ResultSet(rs) =
        exec(&engine, &mut s, "SELECT val FROM t ORDER BY id").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)], vec![Value::Int(43)]]);
}

#[test]
fn view_review_nonmergeable_depth_is_bounded() {
    let (_dir, engine, mut s) = setup();
    let mut last = "v".to_owned();
    let mut rejected = false;
    for i in 0..34 {
        let name = format!("materialized_{i}");
        let result = exec(
            &engine,
            &mut s,
            &format!("CREATE ALGORITHM=TEMPTABLE VIEW {name} AS SELECT id FROM {last}"),
        );
        if let Err(err) = result {
            assert_eq!(err.code(), 1235);
            rejected = true;
            break;
        }
        last = name;
    }
    assert!(
        rejected,
        "non-mergeable view chains need an execution-depth bound"
    );
}

#[test]
fn view_audit_quoted_dot_never_binds_another_view() {
    let (_dir, engine, mut session) = setup();
    let database = session.current_database.clone();
    for sql in [
        format!("CREATE OR REPLACE VIEW `{database}.v` AS SELECT 99 AS id"),
        format!("ALTER VIEW `{database}.v` AS SELECT 99 AS id"),
        format!("DROP VIEW `{database}.v`"),
    ] {
        assert_eq!(
            exec(&engine, &mut session, &sql).unwrap_err().code(),
            1064,
            "{sql}"
        );
    }
    let QueryResponse::ResultSet(result) =
        exec(&engine, &mut session, "SELECT id FROM v ORDER BY id").unwrap()
    else {
        panic!()
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    exec(
        &engine,
        &mut session,
        &format!("ALTER VIEW `{database}`.`v` AS SELECT id FROM t"),
    )
    .unwrap();
    exec(
        &engine,
        &mut session,
        &format!("DROP VIEW `{database}`.`v`"),
    )
    .unwrap();
}

#[test]
fn query_binding_qualifies_nested_window_and_json_table_sources() {
    let (_dir, engine, session) = setup();
    for sql in [
        "SELECT ROW_NUMBER() OVER w FROM t WINDOW w AS (ORDER BY (SELECT id FROM t LIMIT 1))",
        "SELECT * FROM JSON_TABLE((SELECT '[1]' FROM t LIMIT 1), '$[*]' COLUMNS(n INT PATH '$')) j",
        "SELECT * FROM (VALUES ROW((SELECT id FROM t LIMIT 1))) AS v",
    ] {
        let mut statement = crate::parser::parse_statement(sql).unwrap();
        super::super::qualification::qualify_statement_tables(
            engine.database(),
            &session,
            &mut statement,
        )
        .unwrap();
        struct Names(Vec<String>);
        impl crate::parser::visit::AstVisitor for Names {
            fn enter_select(&mut self, select: &Select) {
                self.0
                    .extend(select.from.iter().map(|table| table.name.clone()));
            }
        }
        let mut names = Names(Vec::new());
        crate::parser::visit::visit_statement(&mut names, &statement);
        assert!(
            !names.0.iter().any(|name| name == "t"),
            "{sql}: {:?}",
            names.0
        );
    }
}

#[test]
fn fuzz_round1_unsigned_catalog_expressions_survive_reopen() {
    let (dir, engine, mut session) = setup_engine(true);
    exec(
        &engine,
        &mut session,
        "CREATE VIEW unsigned_v AS SELECT ABS(0xa) AS u, -0xa AS n, ~0xa AS bits",
    )
    .unwrap();
    exec(&engine, &mut session,
        "CREATE TABLE unsigned_defaults (id INT PRIMARY KEY, u BIGINT UNSIGNED DEFAULT (ABS(0xa)), CHECK (u >= ABS(0xa)))").unwrap();
    let QueryResponse::ResultSet(expected) = exec(
        &engine,
        &mut session,
        "SELECT ABS(0xa) AS u, -0xa AS n, ~0xa AS bits",
    )
    .unwrap() else {
        panic!()
    };
    for engine in [Some(engine), None] {
        let engine = engine.unwrap_or_else(|| open_strict_test_engine(dir.path()));
        let mut session = SessionContext::default();
        let QueryResponse::ResultSet(actual) =
            exec(&engine, &mut session, "SELECT * FROM unsigned_v").unwrap()
        else {
            panic!()
        };
        assert_eq!(actual.rows, expected.rows);
        for (actual, expected) in actual.columns.iter().zip(&expected.columns) {
            assert_eq!(actual.column_type, expected.column_type);
            assert_eq!(
                actual.source.flags & crate::mysql::protocol::UNSIGNED_FLAG,
                expected.source.flags & crate::mysql::protocol::UNSIGNED_FLAG
            );
        }
        exec(
            &engine,
            &mut session,
            "INSERT INTO unsigned_defaults (id) VALUES (1)",
        )
        .unwrap();
        let QueryResponse::ResultSet(rows) =
            exec(&engine, &mut session, "SELECT u FROM unsigned_defaults").unwrap()
        else {
            panic!()
        };
        assert_eq!(rows.rows, vec![vec![Value::UInt(10)]]);
        assert_eq!(
            exec(
                &engine,
                &mut session,
                "INSERT INTO unsigned_defaults VALUES (2, 9)"
            )
            .unwrap_err()
            .code(),
            3819
        );
        exec(&engine, &mut session, "DELETE FROM unsigned_defaults").unwrap();
        engine.checkpoint().unwrap();
    }
}
