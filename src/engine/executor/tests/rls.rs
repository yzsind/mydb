//! Row security behavior across identities and execution vehicles.
use super::*;

fn user(name: &str, connection_id: u32) -> SessionContext {
    SessionContext {
        user: name.into(),
        login_user: name.into(),
        authenticated_as: AccountKey::new(name, "%"),
        connection_id,
        ..SessionContext::default()
    }
}
fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    match engine
        .execute_with_session(session, sql)
        .unwrap()
        .materialize()
        .unwrap()
    {
        QueryResponse::ResultSet(result) => result.rows,
        other => panic!("expected rows for {sql}: {other:?}"),
    }
}
fn fixture() -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    fixture_with_durability(false)
}
fn fixture_with_durability(
    durable: bool,
) -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    std::fs::create_dir_all("/tmp/mydb/rls/tests").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb/rls/tests").unwrap();
    let engine = if durable {
        open_strict_test_engine(dir.path())
    } else {
        open_test_engine(dir.path())
    };
    let mut root = SessionContext::default();
    for sql in [
        "CREATE TABLE secured(id INT PRIMARY KEY, tenant_id INT, payload VARCHAR(30))",
        "INSERT INTO secured VALUES (1,1,'one'),(2,2,'two'),(3,NULL,'unknown')",
        "CREATE USER alice",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON test.secured TO alice",
        "ALTER TABLE secured ENABLE ROW LEVEL SECURITY",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    (dir, engine, root)
}

#[test]
fn rls_policy_storage_relocation_retains_the_same_logical_version() {
    let (_dir, engine, mut root) = fixture();
    engine
        .execute_with_session(
            &mut root,
            "CREATE POLICY tenant ON secured USING(tenant_id=1)",
        )
        .unwrap();
    let original = engine.db_handle().table("test.secured").unwrap();
    let header = crate::storage::TableSchema {
        columns: Vec::new(),
        row_security: Arc::new(crate::engine::rls::RowSecurity {
            policies: Vec::new(),
            ..original.row_security.as_ref().clone()
        }),
        ..original.as_ref().clone()
    };
    let store = crate::engine::rls::CatalogStore::new(std::iter::once(&Arc::new(header))).unwrap();
    let captured = store.snapshot();
    let mut moved = original.as_ref().clone();
    for _ in 0..3 {
        moved.id += 100;
        store
            .prepare(
                vec![(
                    moved.object_id,
                    Some((moved.id, moved.row_security.clone())),
                )],
                |_, _| panic!("relocation cannot depend on a retired DD primary key"),
            )
            .unwrap()
            .unwrap()
            .publish();
        assert!(store
            .prepare(
                vec![(
                    moved.object_id,
                    Some((moved.id, moved.row_security.clone()))
                )],
                |_, _| panic!("duplicate publication cannot load old policies"),
            )
            .unwrap()
            .is_none());
    }
    let previous = store.snapshot();
    let mut changed = moved.row_security.as_ref().clone();
    changed.version += 1;
    changed.policies.clear();
    let mut loads = 0;
    let publication = store
        .prepare(
            vec![(moved.object_id, Some((moved.id, Arc::new(changed.clone()))))],
            |storage, version| {
                loads += 1;
                assert_eq!(storage, moved.id);
                assert_eq!(version, original.row_security.version);
                Ok(original.row_security.clone())
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(loads, 1, "a changed version still requires the old payload");
    publication.publish();
    moved.row_security = Arc::new(changed);
    for snapshot in [captured, previous] {
        let resolved = snapshot
            .resolve(&moved, |_, _| {
                panic!("old roots must retain their policy payload")
            })
            .unwrap();
        assert_eq!(resolved.version, original.row_security.version);
        assert_eq!(resolved.policies, original.row_security.policies);
    }
    assert!(store
        .prepare(
            vec![(
                moved.object_id,
                Some((moved.id, original.row_security.clone()))
            )],
            |_, _| panic!("version regression must fail before loading"),
        )
        .is_err());
}

#[test]
fn rls_nested_late_binding_keeps_policy_versions_across_publication() {
    let (_dir, engine, mut root) = fixture();
    engine.execute_with_session(&mut root, "CREATE POLICY tenant ON secured USING(tenant_id=1)").unwrap();
    let alice = user("alice", 2);
    let db = engine.db_handle();
    let old_table = db.table("test.secured").unwrap();
    for change in [
        "ALTER POLICY tenant ON secured USING(tenant_id=2)",
        "ALTER TABLE secured DISABLE ROW LEVEL SECURITY",
        "DROP POLICY tenant ON secured",
    ] {
        // Capture before touching the target: nested access must not depend
        // on which descriptors happened to have been loaded by the caller.
        let captured = crate::engine::expr::with_session_expr_info(&alice, || {
            let _statement = crate::engine::rls::EvaluationScope::enter_statement(&db);
            let captured = crate::engine::expr::current_session_expr_info().unwrap();
            std::thread::scope(|threads| {
                threads.spawn(|| engine.execute_with_session(&mut root, change).unwrap()).join().unwrap();
            });
            let mut nested = alice.clone();
            let values = rows(&engine, &mut nested, "SELECT id FROM secured ORDER BY id");
            if change.starts_with("ALTER POLICY") {
                assert_eq!(values, vec![vec![Value::Int(1)]]);
            } else if change.contains("DISABLE") {
                assert_eq!(values, vec![vec![Value::Int(2)]]);
            } else {
                assert_eq!(values.len(), 3);
            }
            captured
        });
        if change.starts_with("ALTER POLICY") {
            let mut nested = alice.clone();
            let engine = &engine;
            std::thread::scope(|threads| {
                threads.spawn(move || crate::engine::expr::with_session_expr_info_snapshot(captured, || {
                    assert_eq!(rows(engine, &mut nested, "SELECT id FROM secured"), vec![vec![Value::Int(1)]]);
                })).join().unwrap();
            });
        }
    }
    engine.execute_with_session(&mut root, "ALTER TABLE secured ENABLE ROW LEVEL SECURITY").unwrap();
    let mut alice = alice;
    assert!(rows(&engine, &mut alice, "SELECT id FROM secured").is_empty());
    crate::engine::expr::with_session_expr_info(&alice, || {
        // A stale full descriptor may not restore an old policy.
        let predicate = crate::engine::rls::binding(&db, &old_table, crate::parser::rls::Command::Select, false).unwrap().unwrap();
        assert_eq!(predicate.expression, Expr::Literal(Literal::Bool(false)));
    });
}

#[test]
fn rls_definer_default_roles_are_frozen_until_the_next_statement() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE USER bob",
        "CREATE ROLE dormant",
        "GRANT SUPER ON *.* TO dormant",
        "GRANT dormant TO bob",
        "SET DEFAULT ROLE NONE TO bob",
        "GRANT SELECT ON test.secured TO bob",
        "CREATE POLICY tenant ON secured USING(tenant_id=1)",
        "CREATE DEFINER='bob'@'%' FUNCTION read_hidden() RETURNS VARCHAR(30) READS SQL DATA SQL SECURITY DEFINER RETURN (SELECT payload FROM secured WHERE id=2)",
        "GRANT EXECUTE ON FUNCTION test.read_hidden TO alice,bob",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 2);
    let db = engine.db_handle();
    let caller = alice.clone();
    crate::engine::expr::with_session_expr_info(&caller, || {
        let _statement = crate::engine::rls::EvaluationScope::enter_statement(&db);
        std::thread::scope(|threads| {
            threads.spawn(|| engine.execute_with_session(&mut root, "SET DEFAULT ROLE dormant TO bob").unwrap()).join().unwrap();
        });
        assert_eq!(rows(&engine, &mut alice, "SELECT read_hidden()"), vec![vec![Value::Null]]);
    });
    assert_eq!(rows(&engine, &mut alice, "SELECT read_hidden()"), vec![vec![Value::String("two".into())]]);
}

#[test]
fn rls_nested_scopes_and_workers_inherit_the_authorization_root() {
    let (_dir, engine, mut root) = fixture();
    engine
        .execute_with_session(&mut root, "CREATE TABLE nested_target(id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "ALTER TABLE nested_target ENABLE ROW LEVEL SECURITY",
        )
        .unwrap();
    let alice = user("alice", 2);
    let db = engine.db_handle();
    let table = db.table("test.nested_target").unwrap();
    let captured = crate::engine::expr::with_session_expr_info(&alice, || {
        assert!(crate::engine::rls::binding(
            &db,
            &table,
            crate::parser::rls::Command::Select,
            false
        )
        .unwrap()
        .is_some());
        let identity = crate::engine::rls::session_signature(&db, &alice);
        engine
            .execute_with_session(&mut root, "GRANT SUPER ON *.* TO alice")
            .unwrap();
        crate::engine::expr::with_session_expr_info(&alice, || {
            assert!(crate::engine::rls::binding(
                &db,
                &table,
                crate::parser::rls::Command::Insert,
                true
            )
            .unwrap()
            .is_some());
            assert_eq!(identity, crate::engine::rls::session_signature(&db, &alice));
        });
        let _policy = crate::engine::rls::PolicyScope::enter(table.id).unwrap();
        crate::engine::expr::current_session_expr_info().unwrap()
    });
    let new_statement = crate::engine::expr::with_session_expr_info(&alice, || {
        assert!(crate::engine::rls::binding(
            &db,
            &table,
            crate::parser::rls::Command::Select,
            false
        )
        .unwrap()
        .is_none());
        crate::engine::expr::current_session_expr_info().unwrap()
    });
    std::thread::scope(|threads| {
        threads
            .spawn(|| {
                crate::engine::expr::with_session_expr_info_snapshot(new_statement, || {
                    crate::engine::expr::with_session_expr_info_snapshot(captured, || {
                        assert!(crate::engine::rls::binding(
                            &db,
                            &table,
                            crate::parser::rls::Command::Select,
                            false
                        )
                        .unwrap()
                        .is_some());
                        assert!(crate::engine::rls::policy_active());
                        assert!(crate::engine::rls::PolicyScope::enter(table.id).is_err());
                    });
                    assert!(!crate::engine::rls::policy_active());
                    assert!(crate::engine::rls::binding(
                        &db,
                        &table,
                        crate::parser::rls::Command::Select,
                        false
                    )
                    .unwrap()
                    .is_none());
                })
            })
            .join()
            .unwrap();
    });
    crate::engine::expr::with_session_expr_info(&alice, || {
        assert!(crate::engine::rls::binding(
            &db,
            &table,
            crate::parser::rls::Command::Select,
            false
        )
        .unwrap()
        .is_none());
        assert!(!crate::engine::rls::policy_active());
    });
}

#[test]
fn rls_numeric_range_predicates_retain_index_access() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE INDEX tenant_key ON secured(tenant_id,id)",
        "CREATE POLICY tenant ON secured USING(tenant_id=1)",
        "GRANT ROW_SECURITY_ADMIN ON *.* TO alice",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 2);
    for predicate in [
        "id BETWEEN 1 AND 3",
        "id IN(1,2,3,NULL)",
        "NOT(id<1 OR id>3)",
        "id<=>1",
    ] {
        let sql =
            format!("SELECT id FROM secured FORCE INDEX(tenant_key) WHERE {predicate} ORDER BY id");
        assert_eq!(rows(&engine, &mut alice, &sql), vec![vec![Value::Int(1)]]);
        let plan = format!("{:?}", rows(&engine, &mut alice, &format!("EXPLAIN {sql}")));
        assert!(!plan.contains("CASE WHEN"), "{predicate}: {plan}");
    }
}

#[test]
fn rls_sparse_updates_decode_old_and_new_policy_columns() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE TABLE sparse(id INT PRIMARY KEY,tenant INT,v INT,check_value INT,secret LONGTEXT)",
        "INSERT INTO sparse VALUES(1,1,10,0,REPEAT('a',20000)),(2,2,20,0,REPEAT('b',20000)),(3,1,30,-1,REPEAT('c',20000))",
        "UPDATE sparse SET v=9 WHERE id=1",
        "UPDATE sparse SET v=10 WHERE id=1",
        "ALTER TABLE sparse ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY tenant ON sparse USING(tenant=1) WITH CHECK(tenant=1 AND check_value>=0)",
        "GRANT SELECT,UPDATE ON test.sparse TO alice",
    ] { engine.execute_with_session(&mut root, sql).unwrap(); }
    let mut alice = user("alice", 2);
    engine
        .execute_with_session(
            &mut alice,
            "PREPARE u FROM 'UPDATE sparse SET v=v+? WHERE id=?'",
        )
        .unwrap();
    engine
        .execute_with_session(&mut alice, "SET @delta=1,@id=1")
        .unwrap();
    let before = crate::storage::prepared_point_sparse_patch_hits_for_test();
    let sql = "UPDATE sparse SET v=v+? WHERE id=?";
    let parsed = engine.prepare_cached(&mut alice, sql, 2).unwrap();
    let mut state = crate::engine::api::PreparedDmlState::default();
    engine
        .execute_prepared_dml(
            &mut alice,
            &mut state,
            &parsed,
            sql,
            &[Value::Int(1), Value::Int(1)],
        )
        .expect("scalar policies retain the prepared DML vehicle")
        .unwrap();
    assert!(crate::storage::prepared_point_sparse_patch_hits_for_test() > before);
    engine
        .execute_with_session(&mut alice, "SET @id=2")
        .unwrap();
    engine
        .execute_with_session(&mut alice, "EXECUTE u USING @delta,@id")
        .unwrap();
    engine
        .execute_with_session(&mut alice, "SET @id=3")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut alice, "EXECUTE u USING @delta,@id")
            .unwrap_err()
            .code(),
        50130
    );
    assert_eq!(
        rows(
            &engine,
            &mut root,
            "SELECT id,v,LENGTH(secret) FROM sparse ORDER BY id"
        ),
        vec![
            vec![Value::Int(1), Value::Int(11), Value::Int(20000)],
            vec![Value::Int(2), Value::Int(20), Value::Int(20000)],
            vec![Value::Int(3), Value::Int(30), Value::Int(20000)],
        ]
    );
}

#[test]
fn rls_stable_target_bindings_survive_rename_like_and_restart() {
    let (dir, engine, mut root) = fixture_with_durability(true);
    for sql in [
        "CREATE TABLE lookup_rows(id INT PRIMARY KEY)",
        "INSERT INTO lookup_rows VALUES(1),(2)",
        "GRANT SELECT ON test.lookup_rows TO alice",
        "CREATE POLICY tenant ON secured USING(secured.tenant_id=1 AND EXISTS(SELECT 1 FROM lookup_rows l WHERE l.id=secured.id))",
        "ALTER TABLE secured CHANGE tenant_id renamed_tenant INT FIRST",
        "ALTER TABLE secured RENAME COLUMN id TO renamed_id",
        "ALTER TABLE secured RENAME TO renamed_rows",
        "CREATE TABLE copied_rows LIKE renamed_rows",
        "INSERT INTO copied_rows SELECT * FROM renamed_rows",
        "GRANT SELECT ON test.renamed_rows TO alice",
        "GRANT SELECT ON test.copied_rows TO alice",
    ] { engine.execute_with_session(&mut root, sql).unwrap(); }
    let mut alice = user("alice", 2);
    for table in ["renamed_rows", "copied_rows"] {
        assert_eq!(
            rows(
                &engine,
                &mut alice,
                &format!("SELECT renamed_id FROM {table}")
            ),
            vec![vec![Value::Int(1)]]
        );
        assert_eq!(
            engine
                .execute_with_session(
                    &mut root,
                    &format!("ALTER TABLE {table} DROP COLUMN renamed_tenant")
                )
                .unwrap_err()
                .code(),
            50130
        );
        let shown = format!(
            "{:?}",
            rows(
                &engine,
                &mut root,
                &format!("SHOW CREATE POLICY tenant ON {table}")
            )
        );
        assert!(
            shown.contains("renamed_tenant") && shown.contains("renamed_id"),
            "{shown}"
        );
    }
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    for table in ["renamed_rows", "copied_rows"] {
        assert_eq!(
            rows(
                &engine,
                &mut alice,
                &format!("SELECT renamed_id FROM {table}")
            ),
            vec![vec![Value::Int(1)]]
        );
    }
}

#[test]
fn rls_expression_settings_are_bound_per_clause_and_restore_the_caller() {
    let (_dir, engine, mut root) = fixture();
    for sql in ["SET sql_mode='NO_UNSIGNED_SUBTRACTION'", "SET collation_connection='utf8mb4_bin'",
        "CREATE POLICY tenant ON secured USING((CAST(0 AS UNSIGNED)-1)<0 AND (tenant_id=1 OR 'a'='A')) WITH CHECK((CAST(0 AS UNSIGNED)-1)<0 AND tenant_id=1)"] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 2);
    for mode in ["", "NO_UNSIGNED_SUBTRACTION"] {
        engine
            .execute_with_session(&mut alice, &format!("SET sql_mode='{mode}'"))
            .unwrap();
        for collation in ["utf8mb4_bin", "utf8mb4_general_ci"] {
            engine
                .execute_with_session(
                    &mut alice,
                    &format!("SET collation_connection='{collation}'"),
                )
                .unwrap();
            assert_eq!(
                rows(&engine, &mut alice, "SELECT id FROM secured"),
                vec![vec![Value::Int(1)]]
            );
        }
    }
    engine
        .execute_with_session(&mut alice, "SET sql_mode=''")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut alice, "SELECT CAST(0 AS UNSIGNED)-1 FROM secured")
            .unwrap_err()
            .code(),
        1690
    );
    engine
        .execute_with_session(&mut root, "SET sql_mode=''")
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "ALTER POLICY tenant ON secured USING(EXISTS(SELECT 1 WHERE tenant_id=1))",
        )
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM secured"),
        vec![vec![Value::Int(1)]]
    );
    engine
        .execute_with_session(
            &mut alice,
            "UPDATE secured SET payload='changed' WHERE id=1",
        )
        .unwrap();
    engine
        .execute_with_session(&mut root, "SET sql_mode='NO_UNSIGNED_SUBTRACTION'")
        .unwrap();
    engine.execute_with_session(&mut root, "ALTER POLICY tenant ON secured USING(EXISTS(SELECT 1 WHERE tenant_id=1 AND (CAST(0 AS UNSIGNED)-1)<0))").unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM secured"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn rls_explain_details_require_admin_and_validation_still_runs() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE POLICY private_rule ON secured USING(tenant_id=7319927)",
        "CREATE SQL SECURITY INVOKER VIEW plan_view AS SELECT * FROM secured",
        "GRANT SELECT,SHOW VIEW ON test.plan_view TO alice",
        "CREATE TABLE plain_plan(id INT PRIMARY KEY)",
        "GRANT SELECT ON test.plain_plan TO alice",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 2);
    for table in ["secured", "plan_view"] {
        for format in ["TRADITIONAL", "TREE", "JSON"] {
            let shown = format!(
                "{:?}",
                rows(
                    &engine,
                    &mut alice,
                    &format!("EXPLAIN FORMAT={format} SELECT id FROM {table}")
                )
            );
            assert!(!shown.contains("7319927"), "{shown}");
            assert!(shown.contains("ROW_SECURITY_ADMIN"), "{shown}");
        }
    }
    assert!(engine
        .execute_with_session(&mut alice, "EXPLAIN SELECT nonexistent_column FROM secured")
        .is_err());
    let shown = format!(
        "{:?}",
        rows(
            &engine,
            &mut alice,
            "EXPLAIN SELECT id FROM plain_plan WHERE id=1"
        )
    );
    assert!(!shown.contains("ROW_SECURITY_ADMIN"), "{shown}");
    engine
        .execute_with_session(&mut root, "GRANT ROW_SECURITY_ADMIN ON *.* TO alice")
        .unwrap();
    assert!(format!(
        "{:?}",
        rows(
            &engine,
            &mut alice,
            "EXPLAIN FORMAT=TREE SELECT id FROM secured"
        )
    )
    .contains("7319927"));
    engine
        .execute_with_session(&mut root, "REVOKE ROW_SECURITY_ADMIN ON *.* FROM alice")
        .unwrap();
    assert!(!format!(
        "{:?}",
        rows(
            &engine,
            &mut alice,
            "EXPLAIN ANALYZE FORMAT=TREE SELECT id FROM secured"
        )
    )
    .contains("7319927"));
}

#[test]
fn rls_policy_settings_yield_to_stored_function_security_and_creation_mode() {
    let (_dir, engine, mut root) = fixture();
    for sql in ["SET sql_mode='NO_UNSIGNED_SUBTRACTION'",
        "CREATE FUNCTION negative_value() RETURNS BIGINT NO SQL RETURN CAST(0 AS UNSIGNED)-1",
        "CREATE FUNCTION defining_user() RETURNS VARCHAR(100) NO SQL SQL SECURITY DEFINER RETURN CURRENT_USER()",
        "SET sql_mode=''",
        "CREATE POLICY tenant ON secured USING(tenant_id=1 AND negative_value()=-1 AND defining_user()='root@localhost')",
        "GRANT EXECUTE ON FUNCTION test.negative_value TO alice",
        "GRANT EXECUTE ON FUNCTION test.defining_user TO alice",
    ] { engine.execute_with_session(&mut root, sql).unwrap(); }
    let mut alice = user("alice", 2);
    engine
        .execute_with_session(&mut alice, "SET sql_mode='NO_UNSIGNED_SUBTRACTION'")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM secured"),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(&engine, &mut alice, "SELECT CURRENT_USER()"),
        vec![vec![Value::String("alice@%".into())]]
    );
}

#[test]
fn rls_multi_table_dml_filters_sources_and_each_target_command() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE POLICY tenant ON secured USING(tenant_id=1)",
        "CREATE TABLE sink(id INT PRIMARY KEY,payload VARCHAR(30))",
        "INSERT INTO sink VALUES(1,'safe'),(2,'safe')",
        "GRANT SELECT,UPDATE,DELETE ON test.sink TO alice",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 2);
    engine
        .execute_with_session(
            &mut alice,
            "UPDATE sink d JOIN secured s ON d.id=s.id SET d.payload=s.payload",
        )
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut alice,
            "SELECT id,payload FROM sink ORDER BY id"
        ),
        vec![
            vec![Value::Int(1), Value::String("one".into())],
            vec![Value::Int(2), Value::String("safe".into())],
        ]
    );
    engine
        .execute_with_session(
            &mut alice,
            "DELETE d FROM sink d JOIN secured s ON d.id=s.id WHERE d.id=2",
        )
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM sink ORDER BY id"),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
    for sql in [
        "ALTER POLICY tenant ON secured USING(TRUE)",
        "CREATE POLICY write_limit ON secured AS RESTRICTIVE FOR UPDATE USING(tenant_id=1)",
        "CREATE TABLE helper(id INT PRIMARY KEY)",
        "INSERT INTO helper VALUES(2)",
        "GRANT SELECT ON test.helper TO alice",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    engine
        .execute_with_session(&mut alice, "SET @leak=NULL")
        .unwrap();
    engine
        .execute_with_session(
            &mut alice,
            "UPDATE secured s JOIN helper h ON s.id=h.id SET s.payload=(@leak:=s.payload)",
        )
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT @leak"),
        vec![vec![Value::Null]]
    );
    engine
        .execute_with_session(&mut root, "GRANT SUPER ON *.* TO alice")
        .unwrap();
    engine
        .execute_with_session(
            &mut alice,
            "UPDATE sink d JOIN secured s ON d.id=s.id SET d.payload=s.payload",
        )
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT payload FROM sink WHERE id=2"),
        vec![vec![Value::String("two".into())]]
    );
}

#[test]
fn rls_multi_table_write_only_targets_do_not_inherit_a_sibling_read_requirement() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE TABLE writes(id INT PRIMARY KEY,tenant INT,v INT)",
        "INSERT INTO writes VALUES(1,1,0),(2,2,0)",
        "ALTER TABLE writes ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY writes_only ON writes FOR UPDATE USING(tenant=2) WITH CHECK(tenant=2)",
        "CREATE POLICY read_rows ON writes FOR SELECT USING(tenant=1)",
        "GRANT SELECT,UPDATE ON test.writes TO alice",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 2);
    engine
        .execute_with_session(
            &mut alice,
            "UPDATE writes w CROSS JOIN writes r SET w.v=r.v+1",
        )
        .unwrap();
    assert_eq!(
        rows(&engine, &mut root, "SELECT id,v FROM writes ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(0)],
            vec![Value::Int(2), Value::Int(1)],
        ]
    );
}

#[test]
fn rls_multi_table_check_failure_preserves_prior_transaction_statements() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE TABLE atomic_sink(id INT PRIMARY KEY,v INT)",
        "INSERT INTO atomic_sink VALUES(1,0)",
        "CREATE TABLE checked_target(id INT PRIMARY KEY,v INT)",
        "INSERT INTO checked_target VALUES(1,0)",
        "ALTER TABLE checked_target ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY bounded ON checked_target USING(TRUE) WITH CHECK(v<=0)",
        "GRANT SELECT,INSERT,UPDATE ON test.atomic_sink TO alice",
        "GRANT SELECT,UPDATE ON test.checked_target TO alice",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 2);
    engine.execute_with_session(&mut alice, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut alice, "INSERT INTO atomic_sink VALUES(2,5)")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(
                &mut alice,
                "UPDATE atomic_sink a JOIN checked_target b ON a.id=b.id SET a.v=1,b.v=1"
            )
            .unwrap_err()
            .code(),
        50130
    );
    engine.execute_with_session(&mut alice, "COMMIT").unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut root,
            "SELECT id,v FROM atomic_sink ORDER BY id"
        ),
        vec![
            vec![Value::Int(1), Value::Int(0)],
            vec![Value::Int(2), Value::Int(5)],
        ]
    );
    assert_eq!(
        rows(&engine, &mut root, "SELECT v FROM checked_target"),
        vec![vec![Value::Int(0)]]
    );
}

#[test]
fn rls_locking_reads_filter_before_user_predicates_and_apply_reference_commands() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE POLICY tenant ON secured USING(tenant_id=1)",
        "CREATE TABLE lock_helper(id INT PRIMARY KEY)",
        "INSERT INTO lock_helper VALUES(1),(2)",
        "GRANT SELECT,UPDATE ON test.lock_helper TO alice",
        "CREATE SQL SECURITY INVOKER VIEW lock_view AS SELECT * FROM secured",
        "GRANT SELECT,UPDATE ON test.lock_view TO alice",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 2);
    for mode in ["FOR UPDATE", "FOR SHARE"] {
        engine
            .execute_with_session(&mut alice, "SET @leak=NULL")
            .unwrap();
        assert!(rows(
            &engine,
            &mut alice,
            &format!("SELECT id FROM secured WHERE id=2 AND (@leak:=payload) IS NOT NULL {mode}")
        )
        .is_empty());
        assert_eq!(
            rows(&engine, &mut alice, "SELECT @leak"),
            vec![vec![Value::Null]]
        );
        assert!(rows(&engine, &mut alice, &format!("SELECT s.id FROM secured s JOIN lock_helper h ON s.id=h.id WHERE h.id=2 AND (@leak:=s.payload) IS NOT NULL {mode}")).is_empty());
        assert_eq!(
            rows(&engine, &mut alice, "SELECT @leak"),
            vec![vec![Value::Null]]
        );
        assert_eq!(rows(&engine, &mut alice, &format!("SELECT v.id FROM lock_view v JOIN lock_helper h ON v.id=h.id ORDER BY v.id {mode}")), vec![vec![Value::Int(1)]]);
    }
    engine
        .execute_with_session(&mut root, "ALTER POLICY tenant ON secured USING(TRUE)")
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "CREATE POLICY write_limit ON secured AS RESTRICTIVE FOR UPDATE USING(tenant_id=1)",
        )
        .unwrap();
    assert_eq!(rows(&engine, &mut alice, "SELECT s.id FROM secured s JOIN lock_helper h ON s.id=h.id ORDER BY s.id FOR UPDATE OF h"), vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    assert_eq!(rows(&engine, &mut alice, "SELECT s.id FROM secured s JOIN lock_helper h ON s.id=h.id ORDER BY s.id FOR UPDATE OF s"), vec![vec![Value::Int(1)]]);
    assert_eq!(
        rows(
            &engine,
            &mut alice,
            "SELECT id FROM secured ORDER BY id FOR SHARE"
        ),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn rls_scanned_text_updates_bind_parameter_types_before_caching() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE TABLE counters(id INT PRIMARY KEY,tenant INT,v INT NOT NULL)",
        "INSERT INTO counters VALUES(1,1,0),(2,2,0)",
        "UPDATE counters SET v=1 WHERE id=1",
        "UPDATE counters SET v=0 WHERE id=1",
        "ALTER TABLE counters ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY tenant ON counters USING(tenant=1)",
        "GRANT SELECT,UPDATE ON test.counters TO alice",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 2);
    let before = crate::storage::prepared_point_sparse_patch_hits_for_test();
    for _ in 0..3 {
        engine
            .execute_with_session(&mut alice, "UPDATE counters SET v=v+1 WHERE id=1")
            .unwrap();
    }
    assert!(crate::storage::prepared_point_sparse_patch_hits_for_test() >= before + 3);
    engine
        .execute_with_session(&mut alice, "UPDATE counters SET v=v+1 WHERE id=2")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut root, "SELECT id,v FROM counters ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(3)],
            vec![Value::Int(2), Value::Int(0)],
        ]
    );
    engine
        .execute_with_session(&mut root, "GRANT SUPER ON *.* TO alice")
        .unwrap();
    engine
        .execute_with_session(&mut alice, "UPDATE counters SET v=v+1 WHERE id=2")
        .unwrap();
    engine
        .execute_with_session(&mut root, "REVOKE SUPER ON *.* FROM alice")
        .unwrap();
    engine
        .execute_with_session(&mut alice, "UPDATE counters SET v=v+1 WHERE id=2")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut root, "SELECT v FROM counters WHERE id=2"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn rls_default_deny_and_super_bypass() {
    let (_dir, engine, mut root) = fixture();
    let mut alice = user("alice", 2);
    assert!(rows(&engine, &mut alice, "SELECT * FROM secured").is_empty());
    assert_eq!(
        rows(&engine, &mut root, "SELECT COUNT(*) FROM secured"),
        vec![vec![Value::Int(3)]]
    );
    engine
        .execute_with_session(
            &mut root,
            "CREATE POLICY tenant ON secured USING (tenant_id=1)",
        )
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM secured ORDER BY id"),
        vec![vec![Value::Int(1)]]
    );
    engine
        .execute_with_session(&mut root, "GRANT SUPER ON *.* TO alice")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT COUNT(*) FROM secured"),
        vec![vec![Value::Int(3)]]
    );
    engine
        .execute_with_session(&mut root, "REVOKE SUPER ON *.* FROM alice")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT COUNT(*) FROM secured"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn literal_free_cached_reads_use_validated_zero_parameter_templates() {
    let (_dir, engine, root) = fixture();
    for (sql, eligible) in [
        ("SELECT COUNT(*),SUM(tenant_id) FROM secured", true),
        ("SELECT id FROM secured WHERE tenant_id IS NOT NULL", true),
        ("SELECT @tenant FROM secured", false),
        ("SELECT @@sql_mode FROM secured", false),
        ("SELECT id FROM secured WHERE id=?", false),
        ("SELECT id FROM secured LIMIT ?", false),
        ("SELECT SLEEP(tenant_id) FROM secured", false),
        ("SELECT id FROM secured FOR UPDATE", false),
        ("SELECT (SELECT tenant_id FROM secured) FROM secured", false),
    ] {
        engine.parse_cached_statement(&root, sql).unwrap();
        let template = engine.cached_text_statement(&root, sql);
        assert_eq!(template.is_some(), eligible, "{sql}");
        if let Some((_, values, original)) = template {
            assert!(values.is_empty());
            assert_eq!(original.as_ref(), sql);
        }
    }
}

#[test]
fn rls_system_identity_marker_alone_never_bypasses() {
    let (_dir, engine, _) = fixture();
    let mut alice = user("alice", 13);
    alice.internal_system_session = true;
    alice.current_user_is_system = true;
    assert!(rows(&engine, &mut alice, "SELECT * FROM secured").is_empty());
    assert!(engine
        .execute_with_session(&mut alice, "ALTER TABLE secured DISABLE ROW LEVEL SECURITY")
        .is_err());
    assert!(engine
        .execute_with_session(&mut alice, "SET RLS CONTEXT 'tenant_id'='2'")
        .is_err());
}

#[test]
fn rls_view_references_bind_distinct_effective_identities() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE USER bob",
        "GRANT SELECT ON test.secured TO bob",
        "CREATE POLICY a ON secured TO alice USING(tenant_id=1)",
        "CREATE POLICY b ON secured TO bob USING(tenant_id=2)",
        "CREATE DEFINER=bob SQL SECURITY DEFINER VIEW scoped AS SELECT * FROM secured",
        "CREATE SQL SECURITY INVOKER VIEW invoked AS SELECT * FROM secured",
        "CREATE SQL SECURITY DEFINER VIEW elevated AS SELECT * FROM secured",
        "GRANT SELECT ON test.invoked TO bob",
        "CREATE DEFINER=bob SQL SECURITY DEFINER VIEW nested_scoped AS SELECT * FROM invoked",
        "GRANT SELECT ON test.scoped TO alice",
        "GRANT SELECT ON test.invoked TO alice",
        "GRANT SELECT ON test.elevated TO alice",
        "GRANT SELECT ON test.nested_scoped TO alice",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 14);
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM scoped"),
        vec![vec![Value::Int(2)]]
    );
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM invoked"),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM nested_scoped"),
        vec![vec![Value::Int(2)]]
    );
    assert_eq!(
        rows(&engine, &mut alice, "SELECT COUNT(*) FROM elevated"),
        vec![vec![Value::Int(3)]]
    );
    assert_eq!(
        rows(
            &engine,
            &mut alice,
            "SELECT a.id,b.id FROM secured a CROSS JOIN scoped b"
        ),
        vec![vec![Value::Int(1), Value::Int(2)]]
    );
    assert_eq!(
        rows(
            &engine,
            &mut alice,
            "SELECT a.id,b.id FROM invoked a CROSS JOIN scoped b"
        ),
        vec![vec![Value::Int(1), Value::Int(2)]]
    );
    engine
        .execute_with_session(&mut root, "GRANT SUPER ON *.* TO bob")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT COUNT(*) FROM scoped"),
        vec![vec![Value::Int(3)]]
    );
    engine
        .execute_with_session(&mut root, "REVOKE SUPER ON *.* FROM bob")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM scoped"),
        vec![vec![Value::Int(2)]]
    );
    engine
        .execute_with_session(
            &mut root,
            "ALTER POLICY b ON secured USING(tenant_id=CAST(RLS_CONTEXT('tenant_id') AS UNSIGNED))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut root, "GRANT RLS_CONTEXT_ADMIN ON *.* TO alice")
        .unwrap();
    engine
        .execute_with_session(&mut alice, "SET RLS CONTEXT 'tenant_id'='1'")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM scoped"),
        vec![vec![Value::Int(1)]]
    );
    engine
        .execute_with_session(&mut alice, "SET RLS CONTEXT 'tenant_id'='2'")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM scoped"),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn rls_context_local_and_connection_reset() {
    let (_dir, engine, mut root) = fixture();
    engine.execute_with_session(&mut root, "CREATE POLICY tenant ON secured USING (tenant_id=CAST(RLS_CONTEXT('tenant_id') AS UNSIGNED))").unwrap();
    let mut alice = user("alice", 2);
    assert!(engine
        .execute_with_session(&mut alice, "SET RLS CONTEXT 'tenant_id'='1'")
        .is_err());
    engine
        .execute_with_session(&mut root, "GRANT RLS_CONTEXT_ADMIN ON *.* TO alice")
        .unwrap();
    engine
        .execute_with_session(&mut alice, "SET RLS CONTEXT 'tenant_id'='1'")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM secured"),
        vec![vec![Value::Int(1)]]
    );
    engine.execute_with_session(&mut alice, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut alice, "SET LOCAL RLS CONTEXT 'tenant_id'='2'")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM secured"),
        vec![vec![Value::Int(2)]]
    );
    engine.execute_with_session(&mut alice, "ROLLBACK").unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM secured"),
        vec![vec![Value::Int(1)]]
    );
    let mut fresh = alice.fresh_after_connection_reset();
    assert!(rows(&engine, &mut fresh, "SELECT id FROM secured").is_empty());
}

#[test]
fn rls_context_deferred_index_bounds_follow_every_execution() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE INDEX tenant_idx ON secured(tenant_id)",
        "CREATE POLICY tenant ON secured USING(tenant_id=CAST(RLS_CONTEXT('tenant_id') AS UNSIGNED))",
        "GRANT RLS_CONTEXT_ADMIN ON *.* TO alice",
        "GRANT ROW_SECURITY_ADMIN ON *.* TO alice",
    ] { engine.execute_with_session(&mut root, sql).unwrap(); }
    let mut alice = user("alice", 11);
    engine
        .execute_with_session(
            &mut alice,
            "PREPARE scoped FROM 'SELECT id FROM secured FORCE INDEX(tenant_idx) ORDER BY id'",
        )
        .unwrap();
    for context in [None, Some("1"), Some("2"), None, Some("1")] {
        let sql = context.map_or_else(
            || "RESET RLS CONTEXT".to_string(),
            |value| format!("SET RLS CONTEXT 'tenant_id'='{value}'"),
        );
        engine.execute_with_session(&mut alice, &sql).unwrap();
        let expected = context.map_or_else(Vec::new, |value| {
            vec![vec![Value::Int(value.parse().unwrap())]]
        });
        for sql in [
            "SELECT id FROM secured FORCE INDEX(tenant_idx) ORDER BY id",
            "EXECUTE scoped",
        ] {
            assert_eq!(
                rows(&engine, &mut alice, sql),
                expected,
                "{context:?}: {sql}"
            );
        }
        let explain = rows(
            &engine,
            &mut alice,
            "EXPLAIN SELECT id FROM secured FORCE INDEX(tenant_idx)",
        );
        assert!(
            explain.iter().any(|row| row
                .iter()
                .any(|v| matches!(v, Value::String(name) if name == "ref"))),
            "{explain:?}"
        );
    }
}

#[test]
fn rls_super_does_not_grant_management() {
    let (_dir, engine, mut root) = fixture();
    engine
        .execute_with_session(&mut root, "GRANT SUPER ON *.* TO alice")
        .unwrap();
    let mut alice = user("alice", 2);
    for sql in [
        "ALTER TABLE secured DISABLE ROW LEVEL SECURITY",
        "CREATE POLICY bad ON secured USING (TRUE)",
        "SET RLS CONTEXT 'tenant_id'='1'",
    ] {
        assert!(
            engine.execute_with_session(&mut alice, sql).is_err(),
            "{sql}"
        );
    }
    engine
        .execute_with_session(&mut alice, "SET row_security=OFF")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT COUNT(*) FROM secured"),
        vec![vec![Value::Int(3)]]
    );
}

#[test]
fn rls_writes_are_filtered_and_fail_atomically() {
    let (_dir, engine, mut root) = fixture();
    engine
        .execute_with_session(
            &mut root,
            "CREATE POLICY tenant ON secured USING(tenant_id=1)",
        )
        .unwrap();
    let mut alice = user("alice", 2);
    for sql in [
        "INSERT INTO secured VALUES(10,1,'allowed'),(11,2,'denied')",
        "INSERT IGNORE INTO secured VALUES(12,2,'denied')",
        "UPDATE secured SET tenant_id=2 WHERE id=1",
    ] {
        let error = engine.execute_with_session(&mut alice, sql).unwrap_err();
        assert_eq!(error.code(), 50130, "{sql}: {error:?}");
    }
    engine
        .execute_with_session(&mut alice, "UPDATE secured SET payload='hidden' WHERE id=2")
        .unwrap();
    engine
        .execute_with_session(&mut alice, "DELETE FROM secured WHERE id=2")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut root,
            "SELECT id,tenant_id,payload FROM secured ORDER BY id"
        ),
        vec![
            vec![Value::Int(1), Value::Int(1), Value::String("one".into())],
            vec![Value::Int(2), Value::Int(2), Value::String("two".into())],
            vec![Value::Int(3), Value::Null, Value::String("unknown".into())]
        ]
    );
}

#[test]
fn rls_outer_join_setop_and_aggregate_paths() {
    let (_dir, engine, mut root) = fixture();
    engine
        .execute_with_session(
            &mut root,
            "CREATE POLICY tenant ON secured USING(tenant_id=1)",
        )
        .unwrap();
    let mut alice = user("alice", 2);
    assert_eq!(
        rows(
            &engine,
            &mut alice,
            "SELECT a.id,b.id FROM secured a LEFT JOIN secured b ON a.id=b.id ORDER BY a.id"
        ),
        vec![vec![Value::Int(1), Value::Int(1)]]
    );
    assert_eq!(
        rows(
            &engine,
            &mut alice,
            "SELECT id FROM secured UNION ALL SELECT id FROM secured ORDER BY id"
        ),
        vec![vec![Value::Int(1)], vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(
            &engine,
            &mut alice,
            "SELECT COUNT(*),MIN(id),MAX(id) FROM secured"
        ),
        vec![vec![Value::Int(1), Value::Int(1), Value::Int(1)]]
    );
}

#[test]
fn rls_role_super_and_explicit_bypass_follow_active_roles() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE ROLE elevated",
        "GRANT SUPER ON *.* TO elevated",
        "GRANT elevated TO alice",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 2);
    assert!(rows(&engine, &mut alice, "SELECT id FROM secured").is_empty());
    engine
        .execute_with_session(&mut alice, "SET ROLE elevated")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT COUNT(*) FROM secured"),
        vec![vec![Value::Int(3)]]
    );
    engine
        .execute_with_session(&mut alice, "SET ROLE NONE")
        .unwrap();
    assert!(rows(&engine, &mut alice, "SELECT id FROM secured").is_empty());
    engine
        .execute_with_session(&mut root, "GRANT BYPASS_RLS ON *.* TO alice")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT COUNT(*) FROM secured"),
        vec![vec![Value::Int(3)]]
    );
    engine
        .execute_with_session(&mut root, "REVOKE BYPASS_RLS ON *.* FROM alice")
        .unwrap();
    assert!(rows(&engine, &mut alice, "SELECT id FROM secured").is_empty());
}

#[test]
fn rls_global_all_and_independent_bypass_are_account_scoped() {
    let (_dir, engine, mut root) = fixture();
    let mut alice = user("alice", 15);
    for grant in [
        "GRANT ALL PRIVILEGES ON test.* TO alice",
        "GRANT ALL PRIVILEGES ON test.secured TO alice",
    ] {
        engine.execute_with_session(&mut root, grant).unwrap();
        assert!(rows(&engine, &mut alice, "SELECT id FROM secured").is_empty());
    }
    engine
        .execute_with_session(&mut root, "GRANT SUPER,BYPASS_RLS ON *.* TO alice")
        .unwrap();
    engine
        .execute_with_session(&mut root, "REVOKE SUPER ON *.* FROM alice")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT COUNT(*) FROM secured"),
        vec![vec![Value::Int(3)]]
    );
    engine
        .execute_with_session(&mut root, "REVOKE BYPASS_RLS ON *.* FROM alice")
        .unwrap();
    assert!(rows(&engine, &mut alice, "SELECT id FROM secured").is_empty());
    engine
        .execute_with_session(&mut root, "GRANT ALL PRIVILEGES ON *.* TO alice")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT COUNT(*) FROM secured"),
        vec![vec![Value::Int(3)]]
    );
    engine
        .execute_with_session(&mut root, "CREATE USER 'alice'@'localhost'")
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            "GRANT SELECT ON test.secured TO 'alice'@'localhost'",
        )
        .unwrap();
    let mut other = user("alice", 16);
    other.authenticated_as = AccountKey::new("alice", "localhost");
    assert!(rows(&engine, &mut other, "SELECT id FROM secured").is_empty());
}

#[test]
fn rls_subquery_requires_dependency_privileges() {
    let (_dir, engine, mut root) = fixture();
    for sql in ["CREATE TABLE membership(tenant_id INT PRIMARY KEY)", "INSERT INTO membership VALUES(1)", "GRANT SELECT ON test.membership TO alice", "CREATE POLICY tenant ON secured USING(EXISTS(SELECT 1 FROM membership m WHERE m.tenant_id=secured.tenant_id))"] { engine.execute_with_session(&mut root, sql).unwrap(); }
    let mut alice = user("alice", 2);
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM secured ORDER BY id"),
        vec![vec![Value::Int(1)]]
    );
    engine
        .execute_with_session(&mut root, "REVOKE SELECT ON test.membership FROM alice")
        .unwrap();
    assert!(engine
        .execute_with_session(&mut alice, "SELECT id FROM secured")
        .is_err());
}

#[test]
fn rls_relational_authorization_preserves_nulls_duplicates_and_rebinding() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE TABLE membership(tenant_id INT)",
        "INSERT INTO membership VALUES(1),(1),(NULL)",
        "GRANT SELECT ON test.membership TO alice",
        "CREATE POLICY member ON secured USING(EXISTS(SELECT 1 FROM membership m WHERE m.tenant_id=secured.tenant_id))",
    ] { engine.execute_with_session(&mut root, sql).unwrap(); }
    let mut alice = user("alice", 12);
    for query in [
        "SELECT id FROM secured ORDER BY id",
        "SELECT id FROM secured WHERE id>0 ORDER BY id",
    ] {
        assert_eq!(rows(&engine, &mut alice, query), vec![vec![Value::Int(1)]]);
    }
    assert_eq!(
        rows(
            &engine,
            &mut alice,
            "SELECT a.id,b.id FROM secured a JOIN secured b ON a.id=b.id"
        ),
        vec![vec![Value::Int(1), Value::Int(1)]]
    );
    engine
        .execute_with_session(&mut root, "GRANT ROW_SECURITY_ADMIN ON *.* TO alice")
        .unwrap();
    let plan = rows(&engine, &mut alice, "EXPLAIN SELECT COUNT(*) FROM secured");
    let Statement::Select(executable) =
        crate::parser::parse_statement("SELECT COUNT(*) FROM test.secured").unwrap()
    else {
        panic!("expected SELECT");
    };
    crate::engine::expr::with_session_expr_info(&alice, || {
        let physical =
            crate::planner::plan_prepared_physical_select(&engine.db_handle(), &executable)
                .unwrap();
        let actual = format!("{physical:?}");
        assert!(
            actual.contains("HashJoin") && !actual.contains("Apply {"),
            "{actual}"
        );
    });
    assert!(
        plan.iter()
            .flatten()
            .any(|v| matches!(v, Value::String(s) if s.contains("Using semi join"))),
        "{plan:?}"
    );
    engine
        .execute_with_session(
            &mut root,
            "UPDATE membership SET tenant_id=2 WHERE tenant_id=1",
        )
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM secured ORDER BY id"),
        vec![vec![Value::Int(2)]]
    );
    engine.execute_with_session(&mut root, "ALTER POLICY member ON secured USING(NOT EXISTS(SELECT 1 FROM membership m WHERE m.tenant_id=secured.tenant_id))").unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM secured ORDER BY id"),
        vec![vec![Value::Int(1)], vec![Value::Int(3)]]
    );
    let plan = rows(&engine, &mut alice, "EXPLAIN SELECT COUNT(*) FROM secured");
    assert!(
        plan.iter()
            .flatten()
            .any(|v| matches!(v, Value::String(s) if s.contains("Using anti join"))),
        "{plan:?}"
    );
    engine.execute_with_session(&mut root,
        "ALTER POLICY member ON secured USING(EXISTS(SELECT 1 FROM membership secured WHERE secured.tenant_id=2))").unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT COUNT(*) FROM secured a"),
        vec![vec![Value::Int(3)]]
    );
}

#[test]
fn rls_policy_combination_and_command_default_deny() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE POLICY one ON secured FOR SELECT USING(tenant_id=1)",
        "CREATE POLICY two ON secured FOR SELECT USING(tenant_id=2)",
        "CREATE POLICY ceiling ON secured AS RESTRICTIVE FOR SELECT USING(id<2)",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 2);
    let security = engine
        .db_handle()
        .table("test.secured")
        .unwrap()
        .row_security
        .clone();
    assert_eq!(security.policies.len(), 3, "{security:?}");
    assert_eq!(
        rows(
            &engine,
            &mut root,
            "SELECT id FROM secured WHERE (tenant_id=1 OR tenant_id=2) AND id<2 ORDER BY id"
        ),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM secured ORDER BY id"),
        vec![vec![Value::Int(1)]],
        "{security:?}"
    );
    assert_eq!(
        engine
            .execute_with_session(&mut alice, "INSERT INTO secured VALUES(10,1,'blocked')")
            .unwrap_err()
            .code(),
        50130
    );
    for sql in ["DROP POLICY one ON secured", "DROP POLICY two ON secured"] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    assert!(rows(&engine, &mut alice, "SELECT id FROM secured").is_empty());
}

#[test]
fn rls_restrictive_only_does_not_bind_unreachable_authorization_queries() {
    let (_dir, engine, mut root) = fixture();
    for sql in ["CREATE TABLE secret(id INT)", "INSERT INTO secret VALUES(1)",
        "CREATE POLICY restriction ON secured AS RESTRICTIVE USING(EXISTS(SELECT 1 FROM secret s WHERE s.id=secured.id))"] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 20);
    assert!(rows(&engine, &mut alice, "SELECT * FROM secured").is_empty());
    engine
        .execute_with_session(&mut root, "CREATE POLICY allow_rows ON secured USING(TRUE)")
        .unwrap();
    assert!(engine
        .execute_with_session(&mut alice, "SELECT * FROM secured")
        .is_err());
    engine
        .execute_with_session(&mut root, "GRANT SELECT ON test.secret TO alice")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM secured"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn rls_cached_prepared_select_observes_super_revocation() {
    let (_dir, engine, mut root) = fixture();
    engine
        .execute_with_session(
            &mut root,
            "CREATE POLICY tenant ON secured USING(tenant_id=1)",
        )
        .unwrap();
    let mut alice = user("alice", 2);
    for sql in [
        "PREPARE point FROM 'SELECT id FROM secured WHERE id=?'",
        "SET @id=2",
    ] {
        engine.execute_with_session(&mut alice, sql).unwrap();
    }
    assert!(rows(&engine, &mut alice, "EXECUTE point USING @id").is_empty());
    engine
        .execute_with_session(&mut root, "GRANT SUPER ON *.* TO alice")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "EXECUTE point USING @id"),
        vec![vec![Value::Int(2)]]
    );
    engine
        .execute_with_session(&mut root, "REVOKE SUPER ON *.* FROM alice")
        .unwrap();
    assert!(rows(&engine, &mut alice, "EXECUTE point USING @id").is_empty());
}

#[test]
fn rls_conflicting_insert_and_replace_cannot_touch_hidden_rows() {
    let (_dir, engine, mut root) = fixture();
    engine
        .execute_with_session(
            &mut root,
            "CREATE POLICY tenant ON secured USING(tenant_id=1)",
        )
        .unwrap();
    let mut alice = user("alice", 2);
    for sql in ["INSERT INTO secured VALUES(2,1,'attack') ON DUPLICATE KEY UPDATE tenant_id=1,payload='attack'", "REPLACE INTO secured VALUES(2,1,'attack')"] {
        assert_eq!(engine.execute_with_session(&mut alice, sql).unwrap_err().code(), 50130, "{sql}");
    }
    assert_eq!(
        rows(
            &engine,
            &mut root,
            "SELECT tenant_id,payload FROM secured WHERE id=2"
        ),
        vec![vec![Value::Int(2), Value::String("two".into())]]
    );
}

#[test]
fn rls_fk_cascade_is_atomic_and_super_bypasses_child_policy() {
    let (_dir, engine, mut root) = fixture();
    for sql in ["CREATE POLICY tenant ON secured USING(tenant_id=1)", "CREATE TABLE children(id INT PRIMARY KEY,parent_id INT,tenant_id INT,FOREIGN KEY(parent_id) REFERENCES secured(id) ON DELETE CASCADE)", "INSERT INTO children VALUES(1,1,2)", "ALTER TABLE children ENABLE ROW LEVEL SECURITY", "CREATE POLICY child_tenant ON children USING(tenant_id=1)"] { engine.execute_with_session(&mut root, sql).unwrap(); }
    let mut alice = user("alice", 2);
    assert_eq!(
        engine
            .execute_with_session(&mut alice, "DELETE FROM secured WHERE id=1")
            .unwrap_err()
            .code(),
        50130
    );
    assert_eq!(
        rows(&engine, &mut root, "SELECT COUNT(*) FROM children"),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(
            &engine,
            &mut root,
            "SELECT COUNT(*) FROM secured WHERE id=1"
        ),
        vec![vec![Value::Int(1)]]
    );
    engine
        .execute_with_session(&mut root, "DELETE FROM secured WHERE id=1")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut root, "SELECT COUNT(*) FROM children"),
        vec![vec![Value::Int(0)]]
    );
}

#[test]
fn rls_returning_rejection_rolls_back_insert() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE POLICY read_tenant ON secured FOR SELECT USING(tenant_id=1)",
        "CREATE POLICY insert_any ON secured FOR INSERT WITH CHECK(TRUE)",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 2);
    assert_eq!(
        engine
            .execute_with_session(
                &mut alice,
                "INSERT INTO secured VALUES(20,2,'hidden') RETURNING id"
            )
            .unwrap_err()
            .code(),
        50130
    );
    assert_eq!(
        rows(
            &engine,
            &mut root,
            "SELECT COUNT(*) FROM secured WHERE id=20"
        ),
        vec![vec![Value::Int(0)]]
    );
}

#[test]
fn rls_restart_preserves_policies_and_dependencies() {
    let (dir, engine, mut root) = fixture_with_durability(true);
    for sql in [
        "CREATE POLICY tenant ON secured TO alice USING(tenant_id=1)",
        "DROP USER alice",
    ] {
        let result = engine.execute_with_session(&mut root, sql);
        if sql.starts_with("DROP") {
            assert!(result.is_err());
        } else {
            result.unwrap();
        }
    }
    let mut before = user("alice", 2);
    assert_eq!(
        rows(&engine, &mut before, "SELECT id FROM secured ORDER BY id"),
        vec![vec![Value::Int(1)]]
    );
    engine.db_handle().checkpoint().unwrap();
    engine.shutdown().unwrap();
    drop(engine);
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    let mut alice = user("alice", 2);
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM secured ORDER BY id"),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        engine
            .execute_with_session(&mut alice, "INSERT INTO secured VALUES(20,2,'hidden')")
            .unwrap_err()
            .code(),
        50130
    );
    engine.shutdown().unwrap();
}

#[test]
fn rls_metadata_and_context_collation() {
    let (_dir, engine, mut root) = fixture();
    engine
        .execute_with_session(
            &mut root,
            "CREATE POLICY tenant ON secured USING(tenant_id=1)",
        )
        .unwrap();
    assert_eq!(rows(&engine, &mut root, "SELECT POLICY_NAME,COMMAND,KIND FROM information_schema.mydb_row_security_policies WHERE TABLE_NAME='secured'"), vec![vec![Value::String("tenant".into()),Value::String("ALL".into()),Value::String("PERMISSIVE".into())]]);
    let mut alice = user("alice", 2);
    assert!(rows(
        &engine,
        &mut alice,
        "SELECT POLICY_NAME FROM information_schema.mydb_row_security_policies"
    )
    .is_empty());
    assert_eq!(
        rows(
            &engine,
            &mut root,
            "SELECT COLLATION(RLS_CONTEXT('missing'))"
        ),
        vec![vec![Value::String("utf8mb4_bin".into())]]
    );
}

#[test]
fn rls_materialized_cte_inherits_tenant_context() {
    let (_dir, engine, mut root) = fixture();
    for sql in ["CREATE POLICY tenant ON secured USING(tenant_id=CAST(RLS_CONTEXT('tenant_id') AS UNSIGNED))", "GRANT RLS_CONTEXT_ADMIN ON *.* TO alice"] { engine.execute_with_session(&mut root, sql).unwrap(); }
    let mut alice = user("alice", 2);
    engine
        .execute_with_session(&mut alice, "SET RLS CONTEXT 'tenant_id'='2'")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut alice,
            "WITH c AS (SELECT id FROM secured) SELECT a.id FROM c a JOIN c b ON a.id=b.id"
        ),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn rls_partition_truncate_requires_bypass() {
    let (_dir, engine, mut root) = fixture();
    for sql in ["CREATE TABLE partitioned(id INT PRIMARY KEY,tenant_id INT) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)", "INSERT INTO partitioned VALUES(1,1),(2,2)", "ALTER TABLE partitioned ENABLE ROW LEVEL SECURITY", "CREATE POLICY tenant ON partitioned USING(tenant_id=1)", "GRANT SELECT,ALTER ON test.partitioned TO alice"] { engine.execute_with_session(&mut root, sql).unwrap(); }
    let mut alice = user("alice", 2);
    assert_eq!(
        engine
            .execute_with_session(&mut alice, "ALTER TABLE partitioned TRUNCATE PARTITION p0")
            .unwrap_err()
            .code(),
        50130
    );
    assert_eq!(
        rows(&engine, &mut root, "SELECT COUNT(*) FROM partitioned"),
        vec![vec![Value::Int(2)]]
    );
    engine
        .execute_with_session(&mut root, "ALTER TABLE partitioned TRUNCATE PARTITION p0")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut root, "SELECT COUNT(*) FROM partitioned"),
        vec![vec![Value::Int(0)]]
    );
}

#[test]
fn rls_partition_exchange_checks_the_staging_participant() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE TABLE part(id INT PRIMARY KEY) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)",
        "CREATE TABLE stage(id INT PRIMARY KEY)", "INSERT INTO stage VALUES(1),(2)",
        "GRANT ALL PRIVILEGES ON test.* TO alice",
        "ALTER TABLE stage ENABLE ROW LEVEL SECURITY", "CREATE POLICY p ON stage USING(id=1)",
        "CREATE POLICY stage_lookup ON secured USING(EXISTS(SELECT 1 FROM stage s WHERE s.id=secured.id))",
    ] { engine.execute_with_session(&mut root, sql).unwrap(); }
    let mut alice = user("alice", 19);
    let stage_object_id = engine.db_handle().table("test.stage").unwrap().object_id;
    let sql = "ALTER TABLE part EXCHANGE PARTITION p0 WITH TABLE stage";
    assert_eq!(
        engine
            .execute_with_session(&mut alice, sql)
            .unwrap_err()
            .code(),
        50130
    );
    assert!(rows(&engine, &mut root, "SELECT * FROM part").is_empty());
    assert_eq!(
        rows(&engine, &mut root, "SELECT id FROM stage ORDER BY id"),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
    engine
        .execute_with_session(&mut root, "GRANT SUPER ON *.* TO alice")
        .unwrap();
    engine.execute_with_session(&mut alice, sql).unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM part ORDER BY id"),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
    assert_eq!(
        engine.db_handle().table("test.stage").unwrap().object_id,
        stage_object_id
    );
    engine
        .execute_with_session(&mut root, "REVOKE SUPER ON *.* FROM alice")
        .unwrap();
    assert!(rows(&engine, &mut alice, "SELECT id FROM secured").is_empty());
    engine
        .execute_with_session(&mut root, "INSERT INTO stage VALUES(1),(2)")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM secured ORDER BY id"),
        vec![vec![Value::Int(1)]]
    );
    assert!(engine
        .execute_with_session(&mut root, "DROP TABLE stage")
        .is_err());
}

#[test]
fn rls_partition_exchange_requires_bypass_for_either_protected_participant() {
    for (protect_part, protect_stage) in [(true, false), (false, true), (true, true)] {
        let (_dir, engine, mut root) = fixture();
        for sql in [
            "CREATE TABLE part(id INT PRIMARY KEY) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)",
            "CREATE TABLE stage(id INT PRIMARY KEY)",
            "INSERT INTO part VALUES(3)",
            "INSERT INTO stage VALUES(1),(2)",
            "GRANT ALL PRIVILEGES ON test.* TO alice",
        ] {
            engine.execute_with_session(&mut root, sql).unwrap();
        }
        for (name, enabled) in [("part", protect_part), ("stage", protect_stage)] {
            if enabled {
                engine
                    .execute_with_session(
                        &mut root,
                        &format!("ALTER TABLE {name} ENABLE ROW LEVEL SECURITY"),
                    )
                    .unwrap();
            }
        }
        let mut alice = user("alice", 19);
        let exchange = "ALTER TABLE part EXCHANGE PARTITION p0 WITH TABLE stage";
        assert_eq!(
            engine
                .execute_with_session(&mut alice, exchange)
                .unwrap_err()
                .code(),
            50130
        );
        assert_eq!(
            rows(&engine, &mut root, "SELECT id FROM part"),
            vec![vec![Value::Int(3)]]
        );
        assert_eq!(
            rows(&engine, &mut root, "SELECT id FROM stage ORDER BY id"),
            vec![vec![Value::Int(1)], vec![Value::Int(2)]]
        );
        engine
            .execute_with_session(&mut root, "GRANT BYPASS_RLS ON *.* TO alice")
            .unwrap();
        engine.execute_with_session(&mut alice, exchange).unwrap();
        assert_eq!(
            rows(&engine, &mut alice, "SELECT id FROM part ORDER BY id"),
            vec![vec![Value::Int(1)], vec![Value::Int(2)]]
        );
        assert_eq!(
            rows(&engine, &mut alice, "SELECT id FROM stage"),
            vec![vec![Value::Int(3)]]
        );
        engine
            .execute_with_session(&mut root, "REVOKE BYPASS_RLS ON *.* FROM alice")
            .unwrap();
        assert_eq!(
            engine
                .execute_with_session(&mut alice, exchange)
                .unwrap_err()
                .code(),
            50130
        );
        engine
            .execute_with_session(&mut root, "GRANT SUPER ON *.* TO alice")
            .unwrap();
        engine.execute_with_session(&mut alice, exchange).unwrap();
        assert_eq!(
            rows(&engine, &mut alice, "SELECT id FROM part"),
            vec![vec![Value::Int(3)]]
        );
        assert_eq!(
            rows(&engine, &mut alice, "SELECT id FROM stage ORDER BY id"),
            vec![vec![Value::Int(1)], vec![Value::Int(2)]]
        );
    }
}

#[test]
fn exchange_preserves_heap_indexes_lobs_and_logical_identity_in_all_durability_modes() {
    std::fs::create_dir_all("/tmp/mydb/rls/tests").unwrap();
    for durability in [
        crate::storage::DurabilityMode::Memory,
        crate::storage::DurabilityMode::Relaxed,
        crate::storage::DurabilityMode::Strict,
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb/rls/tests").unwrap();
        let mut config = memory_test_storage_config();
        config.durability = durability;
        let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            config,
        )
        .unwrap();
        let mut root = SessionContext::default();
        let columns = "id INT NOT NULL,bucket INT NOT NULL,n DECIMAL(32,4),stamp DATETIME(6),payload LONGTEXT,doc JSON,g GEOMETRY,v VECTOR(3),PRIMARY KEY(id,bucket),KEY kn(n)";
        for sql in [
            format!("CREATE TABLE part({columns}) PARTITION BY RANGE(bucket)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)"),
            format!("CREATE TABLE stage({columns})"),
            "INSERT INTO part VALUES(9,2,9.5,'2026-01-02 03:04:05.123456',REPEAT('o',21000),'{\"k\":9}',ST_GeomFromText('POINT(9 2)'),'[9,2,1]'),(50,50,NULL,NULL,'stay',NULL,NULL,NULL)".into(),
            "INSERT INTO stage VALUES(1,2,1.2500,'2026-01-03 03:04:05.654321',REPEAT('x',20000),'{\"k\":1}',ST_GeomFromText('POINT(1 2)'),'[1,2,3]'),(2,3,2.2500,NULL,REPEAT('y',22000),'{\"k\":2}',NULL,NULL)".into(),
        ] { engine.execute_with_session(&mut root, &sql).unwrap(); }
        let object_id = engine.db_handle().table("test.stage").unwrap().object_id;
        let outgoing = rows(
            &engine,
            &mut root,
            "SELECT * FROM part PARTITION(p0) ORDER BY id",
        );
        let incoming = rows(&engine, &mut root, "SELECT * FROM stage ORDER BY id");
        engine
            .execute_with_session(
                &mut root,
                "ALTER TABLE part EXCHANGE PARTITION p0 WITH TABLE stage",
            )
            .unwrap();
        assert_eq!(
            rows(
                &engine,
                &mut root,
                "SELECT * FROM part PARTITION(p0) ORDER BY id"
            ),
            incoming,
            "{durability:?}"
        );
        assert_eq!(
            rows(&engine, &mut root, "SELECT * FROM stage ORDER BY id"),
            outgoing,
            "{durability:?}"
        );
        assert_eq!(
            rows(
                &engine,
                &mut root,
                "SELECT id FROM part FORCE INDEX(kn) WHERE n=2.2500"
            ),
            vec![vec![Value::Int(2)]],
            "{durability:?}"
        );
        engine.execute_with_session(&mut root, "INSERT INTO part VALUES(3,4,3.7500,NULL,REPEAT('z',23000),'{\"k\":3}',ST_GeomFromText('POINT(3 4)'),'[3,4,5]')").unwrap();
        let updated = rows(
            &engine,
            &mut root,
            "SELECT * FROM part PARTITION(p0) ORDER BY id",
        );
        engine
            .execute_with_session(
                &mut root,
                "ALTER TABLE part EXCHANGE PARTITION p0 WITH TABLE stage",
            )
            .unwrap();
        assert_eq!(
            rows(&engine, &mut root, "SELECT * FROM stage ORDER BY id"),
            updated,
            "{durability:?}"
        );
        assert_eq!(
            rows(
                &engine,
                &mut root,
                "SELECT id FROM stage IGNORE INDEX(PRIMARY,kn) WHERE n>1 ORDER BY id"
            ),
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)]
            ],
            "{durability:?}"
        );
        assert_eq!(
            rows(&engine, &mut root, "SELECT id FROM part ORDER BY id"),
            vec![vec![Value::Int(9)], vec![Value::Int(50)]],
            "{durability:?}"
        );
        assert_eq!(
            engine.db_handle().table("test.stage").unwrap().object_id,
            object_id
        );
    }
}

#[test]
fn rls_session_context_set_overrides_local_immediately() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "SET RLS CONTEXT 'k'='1'",
        "BEGIN",
        "SET LOCAL RLS CONTEXT 'k'='2'",
        "SAVEPOINT earlier",
        "SET RLS CONTEXT 'k'='3'",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    assert_eq!(
        rows(&engine, &mut root, "SELECT RLS_CONTEXT('k')"),
        vec![vec![Value::String("3".into())]]
    );
    engine
        .execute_with_session(&mut root, "ROLLBACK TO SAVEPOINT earlier")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut root, "SELECT RLS_CONTEXT('k')"),
        vec![vec![Value::String("2".into())]]
    );
    engine.execute_with_session(&mut root, "COMMIT").unwrap();
    assert_eq!(
        rows(&engine, &mut root, "SELECT RLS_CONTEXT('k')"),
        vec![vec![Value::String("3".into())]]
    );
}

#[test]
fn rls_update_check_evaluates_volatile_expression_once() {
    let (_dir, engine, mut root) = fixture();
    engine
        .execute_with_session(
            &mut root,
            "CREATE POLICY random_check ON secured USING(TRUE) WITH CHECK(RAND()<1)",
        )
        .unwrap();
    let mut alice = user("alice", 2);
    let seed = "SET @@rand_seed1=1,@@rand_seed2=2";
    engine.execute_with_session(&mut alice, seed).unwrap();
    let expected = rows(&engine, &mut alice, "SELECT RAND(),RAND()")[0][1].clone();
    engine.execute_with_session(&mut alice, seed).unwrap();
    engine
        .execute_with_session(
            &mut alice,
            "UPDATE secured SET payload='updated' WHERE id=1",
        )
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT RAND()"),
        vec![vec![expected]]
    );
}

#[test]
fn rls_replace_authorizes_old_row_before_delete_triggers() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE POLICY tenant ON secured USING(tenant_id=1)",
        "CREATE TRIGGER capture BEFORE DELETE ON secured FOR EACH ROW SET @leak=OLD.payload",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 17);
    assert_eq!(
        engine
            .execute_with_session(&mut alice, "REPLACE INTO secured VALUES(2,1,'proposed')")
            .unwrap_err()
            .code(),
        50130
    );
    assert_eq!(
        rows(&engine, &mut alice, "SELECT @leak"),
        vec![vec![Value::Null]]
    );
    engine
        .execute_with_session(&mut alice, "REPLACE INTO secured VALUES(1,1,'replacement')")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT @leak"),
        vec![vec![Value::String("one".into())]]
    );
    assert_eq!(
        rows(&engine, &mut root, "SELECT payload FROM secured WHERE id=2"),
        vec![vec![Value::String("two".into())]]
    );
}

#[test]
fn rls_replace_insert_check_evaluates_volatile_expression_once() {
    let (_dir, engine, mut root) = fixture();
    engine
        .execute_with_session(
            &mut root,
            "CREATE POLICY random_check ON secured USING(TRUE) WITH CHECK(RAND()<1)",
        )
        .unwrap();
    let mut alice = user("alice", 18);
    let seed = "SET @@rand_seed1=1,@@rand_seed2=2";
    engine.execute_with_session(&mut alice, seed).unwrap();
    let expected = rows(&engine, &mut alice, "SELECT RAND(),RAND()")[0][1].clone();
    engine.execute_with_session(&mut alice, seed).unwrap();
    engine
        .execute_with_session(&mut alice, "REPLACE INTO secured VALUES(1,1,'replacement')")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT RAND()"),
        vec![vec![expected]]
    );
}

#[test]
fn rls_function_policy_binds_database_and_invocation_context() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE DATABASE policy_db",
        "CREATE TABLE policy_db.t(id INT PRIMARY KEY,tenant_id INT)",
        "INSERT INTO policy_db.t VALUES(1,1),(2,2)",
        "CREATE FUNCTION policy_db.allow_row(x INT) RETURNS INT DETERMINISTIC NO SQL RETURN x=1",
        "CREATE FUNCTION test.allow_row(x INT) RETURNS INT DETERMINISTIC NO SQL RETURN 1",
        "GRANT SELECT ON policy_db.t TO alice",
        "GRANT EXECUTE ON FUNCTION policy_db.allow_row TO alice",
        "GRANT EXECUTE ON FUNCTION test.allow_row TO alice",
        "ALTER TABLE policy_db.t ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY tenant ON policy_db.t USING(allow_row(tenant_id))",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 2);
    for _ in 0..3 {
        assert_eq!(
            rows(
                &engine,
                &mut alice,
                "SELECT id FROM policy_db.t ORDER BY id"
            ),
            vec![vec![Value::Int(1)]]
        );
    }
}

#[test]
fn rls_function_declaring_reads_cannot_write() {
    let (_dir, engine, mut root) = fixture();
    for sql in ["CREATE TABLE policy_audit(id INT)", "CREATE FUNCTION bad_policy(x INT) RETURNS INT READS SQL DATA BEGIN INSERT INTO policy_audit VALUES(x); RETURN 1; END", "GRANT EXECUTE ON FUNCTION test.bad_policy TO alice", "CREATE POLICY tenant ON secured USING(bad_policy(id)=1)"] { engine.execute_with_session(&mut root, sql).unwrap(); }
    let mut alice = user("alice", 2);
    assert_eq!(
        engine
            .execute_with_session(&mut alice, "SELECT id FROM secured")
            .unwrap_err()
            .code(),
        50130
    );
    assert_eq!(
        rows(&engine, &mut root, "SELECT COUNT(*) FROM policy_audit"),
        vec![vec![Value::Int(0)]]
    );
}

#[test]
fn rls_update_applies_select_policy_to_unmodified_column_reads() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE POLICY no_read ON secured FOR SELECT USING(FALSE)",
        "CREATE POLICY update_all ON secured FOR UPDATE USING(TRUE) WITH CHECK(TRUE)",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 2);
    engine
        .execute_with_session(&mut alice, "UPDATE secured SET payload='constant'")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut root,
            "SELECT COUNT(*) FROM secured WHERE payload='constant'"
        ),
        vec![vec![Value::Int(3)]]
    );
    engine
        .execute_with_session(
            &mut alice,
            "UPDATE secured SET payload=CONCAT(tenant_id,'read')",
        )
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut root,
            "SELECT COUNT(*) FROM secured WHERE payload='constant'"
        ),
        vec![vec![Value::Int(3)]]
    );
}

#[test]
fn rls_function_policy_cannot_read_mutable_session_variables() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE FUNCTION session_policy(x INT) RETURNS INT NO SQL RETURN x=@tenant",
        "GRANT EXECUTE ON FUNCTION test.session_policy TO alice",
        "CREATE POLICY tenant ON secured USING(session_policy(tenant_id))",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 2);
    engine
        .execute_with_session(&mut alice, "SET @tenant=2")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut alice, "SELECT id FROM secured")
            .unwrap_err()
            .code(),
        50130
    );
}

#[test]
fn rls_precompiled_dml_rebinds_the_original_statement_after_privilege_changes() {
    let (_dir, engine, mut root) = fixture();
    for sql in [
        "CREATE POLICY read_policy ON secured FOR SELECT USING(tenant_id=1)",
        "CREATE POLICY write_policy ON secured FOR UPDATE USING(TRUE) WITH CHECK(TRUE)",
        "GRANT SUPER ON *.* TO alice",
    ] { engine.execute_with_session(&mut root, sql).unwrap(); }
    let mut alice = user("alice", 2);
    let compile = |session: &SessionContext| {
        let statement = Arc::new(crate::parser::parse_statement("UPDATE test.secured SET payload='changed' WHERE id=2").unwrap());
        crate::engine::params::with_params_slice(&[], || {
            crate::engine::executor::prepared_dml::compile_prepared_dml(
                engine.database(), session, &statement, engine.prepared_dml_cache.current_epoch()).unwrap()
        })
    };
    let privileged = compile(&alice);
    assert!(privileged.point_write.is_some());
    engine.execute_with_session(&mut root, "REVOKE SUPER ON *.* FROM alice").unwrap();
    let response = engine.execute_prepared_dml_core(&mut alice, &privileged, &[], Instant::now()).0.unwrap();
    assert!(matches!(response, QueryResponse::OkAffected { affected_rows: 0 }));
    assert_eq!(rows(&engine, &mut root, "SELECT payload FROM secured WHERE id=2"), vec![vec![Value::String("two".into())]]);
    let protected = compile(&alice);
    engine.execute_with_session(&mut root, "GRANT SUPER ON *.* TO alice").unwrap();
    let response = engine.execute_prepared_dml_core(&mut alice, &protected, &[], Instant::now()).0.unwrap();
    assert!(matches!(response, QueryResponse::OkAffected { affected_rows: 1 }));
    assert_eq!(rows(&engine, &mut root, "SELECT payload FROM secured WHERE id=2"), vec![vec![Value::String("changed".into())]]);
}

#[test]
fn rls_release_audit_dependency_rename_is_rejected_atomically() {
    let (_dir, engine, mut root) = fixture();
    for sql in ["CREATE TABLE dep(tenant_id INT)", "INSERT INTO dep VALUES(1)", "GRANT SELECT ON test.dep TO alice", "CREATE POLICY pdep ON secured FOR SELECT USING(EXISTS(SELECT 1 FROM test.dep WHERE dep.tenant_id=secured.tenant_id))", "CREATE TABLE unrelated(id INT)"] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut alice = user("alice", 771);
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM secured"),
        vec![vec![Value::Int(1)]]
    );
    for sql in [
        "RENAME TABLE dep TO dep2",
        "ALTER TABLE dep RENAME TO dep2",
        "RENAME TABLE unrelated TO moved,dep TO dep2",
        "RENAME TABLE dep TO tmp,unrelated TO dep,tmp TO unrelated",
    ] {
        let err = engine.execute_with_session(&mut root, sql).unwrap_err();
        assert!(
            err.message()
                .contains("referenced by a row security policy"),
            "{sql}: {err:?}"
        );
        assert!(engine.db_handle().table("test.dep").is_ok());
        assert!(engine.db_handle().table("test.unrelated").is_ok());
        assert!(engine.db_handle().table("test.moved").is_err());
        assert_eq!(
            rows(&engine, &mut alice, "SELECT id FROM secured"),
            vec![vec![Value::Int(1)]]
        );
    }
    engine
        .execute_with_session(&mut root, "RENAME TABLE secured TO renamed")
        .unwrap();
    engine
        .execute_with_session(&mut root, "GRANT SELECT ON test.renamed TO alice")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut alice, "SELECT id FROM renamed"),
        vec![vec![Value::Int(1)]]
    );
    engine
        .execute_with_session(&mut root, "DROP POLICY pdep ON renamed")
        .unwrap();
    engine
        .execute_with_session(&mut root, "ALTER TABLE dep RENAME TO dep2")
        .unwrap();
}
