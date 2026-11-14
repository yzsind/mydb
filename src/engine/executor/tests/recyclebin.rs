use super::*;

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    match engine
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .materialize()
        .unwrap()
    {
        QueryResponse::ResultSet(result) => result.rows,
        QueryResponse::SharedResultSet(result) => result.rows,
        other => panic!("expected rows: {other:?}"),
    }
}

#[test]
fn recyclebin_drop_query_restore_and_new_identity() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v VARCHAR(30), KEY(v))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,'one'),(2,NULL)")
        .unwrap();
    let original = engine.db.table("t").unwrap();
    engine.execute("DROP TABLE t").unwrap();
    assert!(engine.execute("SELECT * FROM t").is_err());
    let entries = engine.db.recyclebin_entries().unwrap();
    assert_eq!(entries.len(), 1);
    let name = entries[0].name();
    assert_eq!(
        rows(
            &engine,
            &format!("SELECT * FROM __mydb_recyclebin__.{name} ORDER BY id")
        ),
        vec![
            vec![Value::Int(1), Value::String("one".into())],
            vec![Value::Int(2), Value::Null]
        ]
    );
    assert_eq!(rows(&engine, "SHOW RECYCLEBIN FROM test LIKE 't'").len(), 1);
    assert_eq!(
        rows(&engine, "SELECT * FROM information_schema.MYDB_RECYCLEBIN").len(),
        1
    );
    assert!(engine
        .execute(&format!(
            "INSERT INTO __mydb_recyclebin__.{name} VALUES (3,'bad')"
        ))
        .is_err());
    assert!(engine
        .execute(&format!(
            "SELECT * FROM __mydb_recyclebin__.{name} FOR UPDATE"
        ))
        .is_err());
    assert!(engine
        .execute(&format!(
            "CREATE VIEW v AS SELECT * FROM __mydb_recyclebin__.{name}"
        ))
        .is_err());
    engine.execute(&format!("RESTORE TABLE {name}")).unwrap();
    let restored = engine.db.table("t").unwrap();
    assert_eq!(original.id, restored.id);
    assert_ne!(original.object_id, restored.object_id);
    assert!(engine.db.recyclebin_entries().unwrap().is_empty());
    assert_eq!(
        rows(&engine, "SELECT id FROM t WHERE v='one'"),
        vec![vec![Value::Int(1)]]
    );
    assert!(engine.execute(&format!("RESTORE TABLE {name}")).is_err());
}

#[test]
fn recyclebin_truncate_versions_and_auto_increment() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t(v) VALUES (10),(20)").unwrap();
    engine.execute("TRUNCATE TABLE t").unwrap();
    let entry = engine.db.recyclebin_entries().unwrap().pop().unwrap();
    engine.execute("INSERT INTO t(v) VALUES (30)").unwrap();
    assert_eq!(
        rows(&engine, "SELECT * FROM t"),
        vec![vec![Value::Int(1), Value::Int(30)]]
    );
    assert!(engine
        .execute(&format!("RESTORE TABLE {}", entry.name()))
        .is_err());
    engine
        .execute(&format!("RESTORE TABLE {} TO recovered", entry.name()))
        .unwrap();
    engine
        .execute("INSERT INTO recovered(v) VALUES (40)")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT id FROM recovered ORDER BY id"),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
    engine.execute("TRUNCATE TABLE t").unwrap();
    engine.execute("TRUNCATE TABLE t").unwrap();
    let entries = engine.db.recyclebin_entries().unwrap();
    assert_eq!(entries.len(), 2);
    assert_ne!(entries[0].id, entries[1].id);
}

#[test]
fn recyclebin_drop_database_retains_only_tables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE DATABASE d",
        "CREATE TABLE d.t(id INT PRIMARY KEY)",
        "INSERT INTO d.t VALUES(9)",
        "CREATE VIEW d.v AS SELECT * FROM d.t",
        "DROP DATABASE d",
    ] {
        engine.execute(sql).unwrap();
    }
    let entry = engine.db.recyclebin_entries().unwrap().pop().unwrap();
    assert_eq!(entry.operation, "DROP DATABASE");
    assert!(engine
        .execute(&format!("RESTORE TABLE {}", entry.name()))
        .is_err());
    engine
        .execute(&format!("RESTORE TABLE {} TO test.saved", entry.name()))
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT * FROM saved"),
        vec![vec![Value::Int(9)]]
    );
}

#[test]
fn recyclebin_purge_and_session_policy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT)").unwrap();
    engine.execute("DROP TABLE t").unwrap();
    let entry = engine.db.recyclebin_entries().unwrap().pop().unwrap();
    engine
        .execute(&format!("PURGE RECYCLEBIN {}", entry.name()))
        .unwrap();
    assert!(engine.db.recyclebin_entry(&entry.name()).unwrap().purging);
    assert!(engine
        .execute(&format!("RESTORE TABLE {}", entry.name()))
        .is_err());
    assert_eq!(engine.db.maintain_recyclebin().unwrap(), 1);
    assert!(engine.db.recyclebin_entries().unwrap().is_empty());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET SESSION mydb_recyclebin=OFF")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t(id INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "DROP TABLE t")
        .unwrap();
    assert!(engine.db.recyclebin_entries().unwrap().is_empty());
}

#[test]
fn recyclebin_purge_all_spans_bounded_batches() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    for index in 0..130 {
        engine
            .execute(&format!("CREATE TABLE t{index}(id INT)"))
            .unwrap();
        engine.execute(&format!("DROP TABLE t{index}")).unwrap();
    }
    let entries = engine.db.recyclebin_entries().unwrap();
    let first = entries.iter().min_by_key(|entry| entry.id).unwrap().name();
    let already_purging = entries.iter().max_by_key(|entry| entry.id).unwrap().name();
    engine
        .execute(&format!("PURGE RECYCLEBIN {already_purging}"))
        .unwrap();
    let owner = 9_999_999;
    engine
        .db
        .acquire_mdl(
            owner,
            Some(owner),
            crate::storage::MdlDuration::Transaction,
            vec![crate::storage::MdlRequest {
                key: crate::storage::MdlObjectKey::table(crate::recyclebin::DATABASE, &first),
                lock_type: crate::storage::MdlLockType::SharedRead,
            }],
            "recyclebin/purge-all-reader",
        )
        .unwrap();
    let db = Arc::clone(&engine.db);
    let worker = std::thread::spawn(move || {
        let _timeout = crate::recyclebin::MdlTimeoutGuard::enter(Some(10));
        db.purge_recyclebin_all()
    });
    let pending = || {
        engine.db.metadata_lock_snapshot().iter().any(|row| {
            row.object_name.as_deref() == Some(first.as_str()) && row.lock_status == "PENDING"
        })
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while !pending() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    let reached_first_batch = pending();
    let added = engine
        .execute("CREATE TABLE later(id INT)")
        .and_then(|_| engine.execute("DROP TABLE later"));
    engine
        .db
        .release_mdl_duration(owner, crate::storage::MdlDuration::Transaction);
    let result = worker.join().unwrap();
    assert!(reached_first_batch);
    added.unwrap();
    result.unwrap();
    let entries = engine.db.recyclebin_entries().unwrap();
    assert_eq!(entries.len(), 131);
    assert!(entries
        .iter()
        .filter(|entry| entry.origin_table != "later")
        .all(|entry| entry.purging));
    assert!(
        !entries
            .iter()
            .find(|entry| entry.origin_table == "later")
            .unwrap()
            .purging,
        "PURGE ALL must not chase entries created after it started"
    );
    engine.execute("PURGE RECYCLEBIN ALL").unwrap();
    for _ in 0..16 {
        engine.db.maintain_recyclebin().unwrap();
        if engine.db.recyclebin_entries().unwrap().is_empty() {
            break;
        }
    }
    assert!(engine.db.recyclebin_entries().unwrap().is_empty());
}

#[test]
fn recyclebin_restart_retains_lob_and_index_data() {
    let dir = tempfile::tempdir().unwrap();
    let name;
    {
        let engine = open_relaxed_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY, v LONGTEXT, KEY(id))")
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES(1, REPEAT('x',20000))")
            .unwrap();
        engine
            .execute("UPDATE t SET v=REPEAT('y',30000) WHERE id=1")
            .unwrap();
        engine.execute("DROP TABLE t").unwrap();
        name = engine
            .db
            .recyclebin_entries()
            .unwrap()
            .pop()
            .unwrap()
            .name();
        engine.shutdown().unwrap();
    }
    let engine = open_relaxed_test_engine(dir.path());
    assert_eq!(
        rows(
            &engine,
            &format!("SELECT LENGTH(v) FROM __mydb_recyclebin__.{name} WHERE id=1")
        ),
        vec![vec![Value::Int(30000)]]
    );
    engine.execute(&format!("RESTORE TABLE {name}")).unwrap();
    assert_eq!(
        rows(&engine, "SELECT LEFT(v,1) FROM t WHERE id=1"),
        vec![vec![Value::String("y".into())]]
    );
}

#[test]
fn recyclebin_parser_rejects_trailing_and_malformed_sql() {
    for sql in [
        "SHOW RECYCLEBIN nonsense",
        "PURGE RECYCLEBIN",
        "RESTORE TABLE",
        "RESTORE TABLE r_1 TO a b",
        "SHOW RECYCLEBIN LIKE t",
    ] {
        assert!(crate::parser::parse_statement(sql).is_err(), "{sql}");
    }
}

#[test]
fn recyclebin_partition_generated_json_and_check_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, j JSON, g INT GENERATED ALWAYS AS (v+1) STORED, CONSTRAINT positive CHECK(v>0)) PARTITION BY HASH(id) PARTITIONS 3").unwrap();
    engine
        .execute("INSERT INTO t(id,v,j) VALUES(1,10,'{\"a\":1}'),(2,20,NULL)")
        .unwrap();
    let before = rows(
        &engine,
        "SELECT id,v,g,JSON_EXTRACT(j,'$.a') FROM t ORDER BY id",
    );
    engine.execute("DROP TABLE t").unwrap();
    let name = engine
        .db
        .recyclebin_entries()
        .unwrap()
        .pop()
        .unwrap()
        .name();
    engine.execute(&format!("RESTORE TABLE {name}")).unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT id,v,g,JSON_EXTRACT(j,'$.a') FROM t ORDER BY id"
        ),
        before
    );
    assert!(engine.execute("INSERT INTO t(id,v) VALUES(3,-1)").is_err());
}

#[test]
fn recyclebin_permissions_rechecked_after_revoke() {
    use crate::engine::auth::AccountKey;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE USER alice",
        "GRANT SELECT ON *.* TO alice",
        "CREATE TABLE t(id INT)",
        "INSERT INTO t VALUES(1)",
        "DROP TABLE t",
    ] {
        engine.execute(sql).unwrap();
    }
    let name = engine
        .db
        .recyclebin_entries()
        .unwrap()
        .pop()
        .unwrap()
        .name();
    let sql = format!("SELECT * FROM __mydb_recyclebin__.{name}");
    let mut alice = SessionContext {
        user: "alice".into(),
        login_user: "alice".into(),
        authenticated_as: AccountKey::new("alice", "%"),
        connection_id: 2,
        ..SessionContext::default()
    };
    for query in [
        sql.as_str(),
        "SHOW RECYCLEBIN",
        "SELECT * FROM information_schema.MYDB_RECYCLEBIN",
    ] {
        assert!(
            engine.execute_with_session(&mut alice, query).is_err(),
            "{query}"
        );
    }
    engine
        .execute("GRANT RECYCLEBIN_ADMIN ON *.* TO alice")
        .unwrap();
    assert!(engine.execute_with_session(&mut alice, &sql).is_ok());
    engine
        .execute_with_session(&mut alice, &format!("PREPARE archived FROM '{sql}'"))
        .unwrap();
    engine
        .execute_with_session(&mut alice, "EXECUTE archived")
        .unwrap();
    engine
        .execute("REVOKE RECYCLEBIN_ADMIN ON *.* FROM alice")
        .unwrap();
    assert!(engine.execute_with_session(&mut alice, &sql).is_err());
    assert!(engine
        .execute_with_session(&mut alice, "EXECUTE archived")
        .is_err());
}

#[test]
fn recyclebin_owned_sequence_and_missing_dependency_are_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE t(id BIGINT)",
        "CREATE SEQUENCE s OWNED BY t.id",
        "INSERT INTO t VALUES(1)",
        "DROP TABLE t",
    ] {
        engine.execute(sql).unwrap();
    }
    assert!(engine.db.resolve_sequence("test", "s").is_err());
    let name = engine
        .db
        .recyclebin_entries()
        .unwrap()
        .pop()
        .unwrap()
        .name();
    assert!(engine
        .db
        .recyclebin_entry(&name)
        .unwrap()
        .omitted
        .iter()
        .any(|n| n.contains("OWNED SEQUENCE")));
    engine.execute(&format!("RESTORE TABLE {name}")).unwrap();
    assert!(engine
        .execute("INSERT INTO t VALUES(NEXTVAL('s'))")
        .is_err());
    engine.execute("CREATE SEQUENCE s START WITH 2").unwrap();
    engine
        .execute("INSERT INTO t VALUES(NEXTVAL('s'))")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT * FROM t ORDER BY id"),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
}

#[test]
fn recyclebin_multi_drop_preflight_keeps_every_table_on_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE p(id INT PRIMARY KEY)",
        "CREATE TABLE c(id INT PRIMARY KEY, p INT, FOREIGN KEY(p) REFERENCES p(id))",
        "INSERT INTO p VALUES(1)",
        "INSERT INTO c VALUES(1,1)",
    ] {
        engine.execute(sql).unwrap();
    }
    assert!(engine.execute("DROP TABLE c,missing").is_err());
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM c"),
        vec![vec![Value::Int(1)]]
    );
    assert!(engine.db.recyclebin_entries().unwrap().is_empty());
    assert!(engine.execute("DROP TABLE p").is_err());
    engine.execute("DROP TABLE p,c").unwrap();
    let entries = engine.db.recyclebin_entries().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].operation_id, entries[1].operation_id);
}

#[test]
fn recyclebin_restore_rebinds_surviving_foreign_keys() {
    for action in ["RESTRICT", "CASCADE", "SET NULL"] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_relaxed_test_engine(dir.path());
        let mut session = SessionContext::default();
        for sql in ["CREATE TABLE p(id INT PRIMARY KEY)".to_string(),
            format!("CREATE TABLE c(id INT PRIMARY KEY,pid INT,CONSTRAINT edge FOREIGN KEY(pid) REFERENCES p(id) ON DELETE {action} ON UPDATE CASCADE)"),
            "INSERT INTO p VALUES(1)".into(),"INSERT INTO c VALUES(1,1)".into(),
            "SET foreign_key_checks=0".into(),"DROP TABLE p".into()] { engine.execute_with_session(&mut session,&sql).unwrap(); }
        let name = engine
            .db
            .recyclebin_entries()
            .unwrap()
            .pop()
            .unwrap()
            .name();
        engine.execute(&format!("RESTORE TABLE {name}")).unwrap();
        assert!(engine.execute("INSERT INTO c VALUES(2,99)").is_err());
        engine.execute("UPDATE p SET id=2 WHERE id=1").unwrap();
        assert_eq!(
            rows(&engine, "SELECT pid FROM c"),
            vec![vec![Value::Int(2)]]
        );
        if action == "RESTRICT" {
            assert_eq!(
                engine
                    .execute("DELETE FROM p WHERE id=2")
                    .unwrap_err()
                    .code(),
                1451
            );
        } else {
            engine.execute("DELETE FROM p WHERE id=2").unwrap();
            assert_eq!(
                rows(&engine, "SELECT pid FROM c"),
                if action == "CASCADE" {
                    Vec::new()
                } else {
                    vec![vec![Value::Null]]
                }
            );
        }
        engine.shutdown().unwrap();
        drop(engine);
        let engine = open_relaxed_test_engine(dir.path());
        if action == "RESTRICT" {
            assert_eq!(
                engine
                    .execute("DELETE FROM p WHERE id=2")
                    .unwrap_err()
                    .code(),
                1451
            );
        }
    }
}

#[test]
fn recyclebin_restore_rejects_incompatible_existing_reference() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE p(id INT PRIMARY KEY)",
        "INSERT INTO p VALUES(1)",
        "DROP TABLE p",
    ] {
        engine.execute(sql).unwrap();
    }
    let name = engine
        .db
        .recyclebin_entries()
        .unwrap()
        .pop()
        .unwrap()
        .name();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET foreign_key_checks=0")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE c(pid VARCHAR(10),CONSTRAINT edge FOREIGN KEY(pid) REFERENCES p(id))",
        )
        .unwrap();
    assert_eq!(
        engine
            .execute(&format!("RESTORE TABLE {name}"))
            .unwrap_err()
            .code(),
        3780
    );
    assert!(engine.db.recyclebin_entry(&name).is_ok());
    assert!(engine.db.table("p").is_err());
    engine
        .execute(&format!("RESTORE TABLE {name} TO independent"))
        .unwrap();
    engine
        .execute("DELETE FROM independent WHERE id=1")
        .unwrap();
}

#[test]
fn recyclebin_fk_parent_mdl_protects_existing_edge_changes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE DATABASE child_db",
        "CREATE TABLE p(id INT PRIMARY KEY)",
        "CREATE TABLE child_db.c(pid INT,CONSTRAINT edge FOREIGN KEY(pid) REFERENCES test.p(id))",
    ] {
        engine.execute(sql).unwrap();
    }
    engine
        .db
        .acquire_mdl(
            777,
            Some(777),
            crate::storage::MdlDuration::Transaction,
            vec![crate::storage::MdlRequest {
                key: crate::storage::MdlObjectKey::table("test", "p"),
                lock_type: crate::storage::MdlLockType::Exclusive,
            }],
            "recyclebin/fk-parent-stability",
        )
        .unwrap();
    let mut session = SessionContext::default();
    session.sysvars.lock_wait_timeout = Some(0);
    for sql in [
        "ALTER TABLE child_db.c DROP FOREIGN KEY edge",
        "DROP TABLE child_db.c",
        "RENAME TABLE child_db.c TO child_db.renamed",
        "DROP DATABASE child_db",
    ] {
        assert_eq!(
            engine
                .execute_with_session(&mut session, sql)
                .unwrap_err()
                .code(),
            1205,
            "{sql}"
        );
    }
    engine
        .db
        .release_mdl_duration(777, crate::storage::MdlDuration::Transaction);
    engine
        .execute("ALTER TABLE child_db.c DROP FOREIGN KEY edge")
        .unwrap();
}

#[test]
fn recyclebin_repeated_check_names_are_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    for _ in 0..3 {
        engine
            .execute("CREATE TABLE t(id INT, CONSTRAINT c CHECK(id>0))")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1)").unwrap();
        engine.execute("DROP TABLE t").unwrap();
    }
    assert_eq!(engine.db.recyclebin_entries().unwrap().len(), 3);
    engine.shutdown().unwrap();
    drop(engine);
    let engine = open_relaxed_test_engine(dir.path());
    assert_eq!(engine.db.recyclebin_entries().unwrap().len(), 3);
}

#[test]
fn recyclebin_pressure_and_retention_do_not_require_new_recycling() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT)").unwrap();
    engine.execute("DROP TABLE t").unwrap();
    engine.db.set_recyclebin_test_free_percent(1);
    assert_eq!(engine.db.maintain_recyclebin_until(&|| true).unwrap(), 0);
    assert_eq!(engine.db.recyclebin_entries().unwrap().len(), 1);
    engine
        .db
        .set_recyclebin_variable("mydb_recyclebin_min_free_pct", 0)
        .unwrap();
    assert_eq!(engine.db.maintain_recyclebin().unwrap(), 0);
    engine
        .db
        .set_recyclebin_variable("mydb_recyclebin_min_free_pct", 10)
        .unwrap();
    engine
        .db
        .set_recyclebin_variable("mydb_recyclebin", 0)
        .unwrap();
    for _ in 0..3 {
        engine.db.maintain_recyclebin().unwrap();
    }
    assert!(engine.db.recyclebin_entries().unwrap().is_empty());
    assert_eq!(engine.db.recyclebin_status()[1], 1);
    assert!(engine
        .db
        .set_recyclebin_variable("mydb_recyclebin_target_free_pct", 5)
        .is_err());
}

#[test]
fn recyclebin_pressure_prioritizes_expiration_then_original_age() {
    for expired in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_relaxed_test_engine(dir.path());
        engine
            .db
            .set_recyclebin_variable("mydb_recyclebin_retention_seconds", 7200)
            .unwrap();
        engine.execute("CREATE TABLE older(id INT)").unwrap();
        engine.execute("DROP TABLE older").unwrap();
        engine
            .db
            .set_recyclebin_variable(
                "mydb_recyclebin_retention_seconds",
                if expired { 1 } else { 3600 },
            )
            .unwrap();
        engine.execute("CREATE TABLE newer(id INT)").unwrap();
        engine.execute("DROP TABLE newer").unwrap();
        if expired {
            std::thread::sleep(Duration::from_millis(1100));
        }
        engine.db.set_recyclebin_test_free_percent(1);
        engine.db.fail_next_ddl_cleanup_for_test();
        assert_eq!(engine.db.maintain_recyclebin().unwrap(), 1);
        let pending = engine.db.recyclebin_entries().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(
            pending[0].origin_table,
            if expired { "newer" } else { "older" }
        );
        assert_eq!(
            pending[0].purge_reason,
            if expired { "EXPIRED" } else { "SPACE_PRESSURE" }
        );
    }
}

#[test]
fn recyclebin_failed_publication_preserves_live_objects_and_bindings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1)").unwrap();
    engine.db.fail_next_catalog_commit_for_test();
    assert!(engine.execute("DROP TABLE t").is_err());
    assert!(engine.db.recyclebin_entries().unwrap().is_empty());
    engine.execute("INSERT INTO t VALUES(2)").unwrap();
    engine.execute("DROP TABLE t").unwrap();
    let name = engine
        .db
        .recyclebin_entries()
        .unwrap()
        .pop()
        .unwrap()
        .name();
    engine.db.fail_next_catalog_commit_for_test();
    assert!(engine.execute(&format!("RESTORE TABLE {name}")).is_err());
    assert!(engine.db.table("t").is_err());
    assert!(!engine.db.recyclebin_entry(&name).unwrap().purging);
    engine.db.fail_next_catalog_commit_for_test();
    assert!(engine.execute(&format!("PURGE RECYCLEBIN {name}")).is_err());
    assert!(!engine.db.recyclebin_entry(&name).unwrap().purging);
    engine.execute(&format!("RESTORE TABLE {name}")).unwrap();
    engine.execute("INSERT INTO t VALUES(3)").unwrap();
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(3)]]
    );
    for sql in [
        "CREATE DATABASE d",
        "CREATE TABLE d.u(id INT)",
        "INSERT INTO d.u VALUES(9)",
        "CREATE VIEW d.v AS SELECT * FROM d.u",
    ] {
        engine.execute(sql).unwrap();
    }
    engine.db.fail_next_catalog_commit_for_test();
    assert!(engine.execute("DROP DATABASE d").is_err());
    assert_eq!(
        rows(&engine, "SELECT * FROM d.v"),
        vec![vec![Value::Int(9)]]
    );
    engine.execute("INSERT INTO d.u VALUES(10)").unwrap();
    assert!(engine.db.recyclebin_entries().unwrap().is_empty());
}

#[test]
fn recyclebin_retirement_cleanup_respects_backup_capture() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    engine.execute("SET GLOBAL mydb_recyclebin=OFF").unwrap();
    engine
        .execute("CREATE TABLE retired(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO retired VALUES(1)").unwrap();
    let table = engine.db.table("retired").unwrap();
    let heap = crate::storage::naming::table_object_generation_path(
        dir.path(),
        table.id,
        table.active_generation.max(1),
        "heap",
    );
    engine.db.fail_next_ddl_cleanup_for_test();
    engine.execute("DROP TABLE retired").unwrap();
    assert!(heap.exists());
    assert_eq!(engine.db.catalog_retirement_count_for_test(), 1);
    engine
        .db
        .capture
        .active
        .store(true, std::sync::atomic::Ordering::Release);
    let result = engine.db.maintain_catalog_retirements_until(|| false);
    engine
        .db
        .capture
        .active
        .store(false, std::sync::atomic::Ordering::Release);
    assert_eq!(
        result.unwrap(),
        0,
        "capture must retain every physical root"
    );
    assert!(heap.exists());
    let guard = engine.db.backup_maintenance_capture_guard().unwrap();
    assert_eq!(
        engine
            .db
            .maintain_catalog_retirements_until(|| false)
            .unwrap(),
        0
    );
    assert!(heap.exists());
    drop(guard);
    assert_eq!(
        engine
            .db
            .maintain_catalog_retirements_until(|| false)
            .unwrap(),
        1
    );
    assert!(!heap.exists());
    assert_eq!(engine.db.catalog_retirement_count_for_test(), 0);
}

#[test]
fn recyclebin_expiration_busy_reader_and_failed_cleanup_retry() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    engine
        .db
        .set_recyclebin_variable("mydb_recyclebin_retention_seconds", 1)
        .unwrap();
    engine
        .db
        .set_recyclebin_variable("mydb_recyclebin_min_free_pct", 0)
        .unwrap();
    engine.execute("CREATE TABLE t(id INT)").unwrap();
    engine.execute("DROP TABLE t").unwrap();
    let name = engine
        .db
        .recyclebin_entries()
        .unwrap()
        .pop()
        .unwrap()
        .name();
    engine
        .db
        .acquire_mdl(
            777,
            Some(777),
            crate::storage::MdlDuration::Transaction,
            vec![crate::storage::MdlRequest {
                key: crate::storage::MdlObjectKey::table(crate::recyclebin::DATABASE, &name),
                lock_type: crate::storage::MdlLockType::SharedRead,
            }],
            "recyclebin/read-lease-test",
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(1100));
    assert_eq!(engine.db.maintain_recyclebin().unwrap(), 0);
    assert!(!engine.db.recyclebin_entry(&name).unwrap().purging);
    engine
        .db
        .release_mdl_duration(777, crate::storage::MdlDuration::Transaction);
    engine.db.fail_next_ddl_cleanup_for_test();
    for _ in 0..3 {
        engine.db.maintain_recyclebin().unwrap();
        if engine.db.recyclebin_entry(&name).unwrap().purging {
            break;
        }
    }
    let entry = engine.db.recyclebin_entry(&name).unwrap();
    assert!(entry.purging);
    assert!(!entry.last_error.is_empty());
    for _ in 0..3 {
        engine.db.maintain_recyclebin().unwrap();
    }
    assert!(engine.db.recyclebin_entries().unwrap().is_empty());
}

#[test]
fn recyclebin_undo_purge_preserves_current_lob_and_count() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    engine.db.set_undo_retention_ms(0).unwrap();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,v LONGTEXT)",
        "INSERT INTO t VALUES(1,REPEAT('a',18000)),(2,REPEAT('b',19000))",
        "UPDATE t SET v=REPEAT('c',22000) WHERE id=1",
        "DELETE FROM t WHERE id=2",
        "TRUNCATE TABLE t",
    ] {
        engine.execute(sql).unwrap();
    }
    let name = engine
        .db
        .recyclebin_entries()
        .unwrap()
        .pop()
        .unwrap()
        .name();
    for _ in 0..32 {
        engine.db.purge_obsolete_undo();
    }
    assert_eq!(
        rows(
            &engine,
            &format!("SELECT COUNT(*) FROM __mydb_recyclebin__.{name}")
        ),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(
            &engine,
            &format!("SELECT LENGTH(v) FROM __mydb_recyclebin__.{name}")
        ),
        vec![vec![Value::Int(22000)]]
    );
    engine
        .execute(&format!("RESTORE TABLE {name} TO saved"))
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT id,LENGTH(v) FROM saved"),
        vec![vec![Value::Int(1), Value::Int(22000)]]
    );
    engine.shutdown().unwrap();
    drop(engine);
    let engine = open_relaxed_test_engine(dir.path());
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM saved"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn recyclebin_special_indexes_retain_physical_contents() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,emb VECTOR(3) NOT NULL,body TEXT,FULLTEXT KEY ft(body))",
        "CREATE INDEX ann ON t(emb) USING HNSW",
        "INSERT INTO t VALUES(1,'[1,0,0]','hello world'),(2,'[0,1,0]','goodbye world')",
    ] {
        engine.execute(sql).unwrap();
    }
    let before = rows(
        &engine,
        "SELECT id FROM t ORDER BY DISTANCE(emb,'[1,0,0]') LIMIT 1",
    );
    engine.execute("TRUNCATE TABLE t").unwrap();
    let name = engine
        .db
        .recyclebin_entries()
        .unwrap()
        .pop()
        .unwrap()
        .name();
    assert_eq!(rows(&engine, &format!("SELECT id FROM __mydb_recyclebin__.{name} ORDER BY DISTANCE(emb,'[1,0,0]') LIMIT 1")), before);
    engine
        .execute(&format!("RESTORE TABLE {name} TO saved"))
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM saved WHERE MATCH(body) AGAINST('hello')"
        ),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM saved ORDER BY DISTANCE(emb,'[1,0,0]') LIMIT 1"
        ),
        before
    );
}

#[test]
fn recyclebin_purge_covers_vector_publications_after_recycling() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(3) NOT NULL,INDEX ann(v) USING HNSW)",
        "INSERT INTO t VALUES(1,'[1,2,3]'),(2,'[3,2,1]')",
        "DROP TABLE t",
    ] {
        engine.execute(sql).unwrap();
    }
    let entry = engine.db.recyclebin_entries().unwrap().pop().unwrap();
    engine.db.checkpoint().unwrap();
    assert_eq!(
        rows(
            &engine,
            &format!(
                "SELECT id FROM __mydb_recyclebin__.{} ORDER BY DISTANCE(v,'[1,2,3]') LIMIT 1",
                entry.name()
            )
        ),
        vec![vec![Value::Int(1)]]
    );
    engine.db.checkpoint().unwrap();
    engine
        .execute(&format!("PURGE RECYCLEBIN {}", entry.name()))
        .unwrap();
    for _ in 0..3 {
        engine.db.maintain_recyclebin().unwrap();
    }
    engine.db.checkpoint().unwrap();
    assert!(engine.db.recyclebin_entries().unwrap().is_empty());
    assert!(!dir
        .path()
        .join("objects/tables")
        .join(entry.table_id.to_string())
        .exists());
    assert!(!dir
        .path()
        .join("objects/indexes")
        .join(entry.table_id.to_string())
        .exists());
}

#[test]
fn recyclebin_old_snapshot_cannot_cross_publication() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1)").unwrap();
    let mut session = SessionContext {
        connection_id: 2,
        ..SessionContext::default()
    };
    engine
        .execute_with_session(
            &mut session,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    engine.execute("DROP TABLE t").unwrap();
    let name = engine
        .db
        .recyclebin_entries()
        .unwrap()
        .pop()
        .unwrap()
        .name();
    let error = engine
        .execute_with_session(
            &mut session,
            &format!("SELECT * FROM __mydb_recyclebin__.{name}"),
        )
        .unwrap_err();
    assert_eq!(error.code(), 1412);
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
}

#[test]
fn recyclebin_restore_preserves_quoted_names_and_locks_target_schema() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE `a b`(id INT)").unwrap();
    engine.execute("INSERT INTO `a b` VALUES(7)").unwrap();
    engine.execute("DROP TABLE `a b`").unwrap();
    let name = engine
        .db
        .recyclebin_entries()
        .unwrap()
        .pop()
        .unwrap()
        .name();
    engine
        .execute(&format!("RESTORE TABLE {} TO `c``d`", name.to_uppercase()))
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT * FROM `c``d`"),
        vec![vec![Value::Int(7)]]
    );
    engine.execute("DROP TABLE `c``d`").unwrap();
    let name = engine
        .db
        .recyclebin_entries()
        .unwrap()
        .pop()
        .unwrap()
        .name();
    engine
        .db
        .acquire_mdl(
            777,
            Some(777),
            crate::storage::MdlDuration::Transaction,
            vec![crate::storage::MdlRequest {
                key: crate::storage::MdlObjectKey::schema("test"),
                lock_type: crate::storage::MdlLockType::Exclusive,
            }],
            "recyclebin/drop-schema-test",
        )
        .unwrap();
    let mut session = SessionContext::default();
    session.sysvars.lock_wait_timeout = Some(0);
    let err = engine
        .execute_with_session(&mut session, &format!("RESTORE TABLE {name} TO target"))
        .unwrap_err();
    assert_eq!(err.code(), 1205);
    assert!(engine.db.recyclebin_entry(&name).is_ok());
    engine
        .db
        .release_mdl_duration(777, crate::storage::MdlDuration::Transaction);
    engine
        .execute(&format!("RESTORE TABLE {name} TO target"))
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT * FROM target"),
        vec![vec![Value::Int(7)]]
    );
}

#[test]
fn recyclebin_binlog_restore_spools_atomic_create_and_rows() {
    use crate::binlog::{self, BinlogConfig};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    binlog::init_global(
        dir.path().join("binlog"),
        BinlogConfig {
            enabled: true,
            ..BinlogConfig::default()
        },
    )
    .unwrap();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v LONGBLOB)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,REPEAT('a',100000))")
        .unwrap();
    for offset in [1, 2, 4, 8, 16, 32] {
        engine
            .execute(&format!("INSERT INTO t SELECT id+{offset},v FROM t"))
            .unwrap();
    }
    engine.execute("DROP TABLE t").unwrap();
    let name = engine
        .db
        .recyclebin_entries()
        .unwrap()
        .pop()
        .unwrap()
        .name();
    engine
        .execute("SET GLOBAL mydb_temp_file_limit_per_query_bytes=1024")
        .unwrap();
    assert!(engine.execute(&format!("RESTORE TABLE {name}")).is_err());
    assert!(engine.db.recyclebin_entry(&name).is_ok());
    assert!(engine.db.table("t").is_err());
    engine
        .execute("SET GLOBAL mydb_temp_file_limit_per_query_bytes=0")
        .unwrap();
    let before = binlog::current_position().unwrap();
    engine.execute(&format!("RESTORE TABLE {name}")).unwrap();
    let events = binlog::manager()
        .unwrap()
        .events_after(&before.file, before.position)
        .unwrap();
    assert_eq!(events.first().unwrap().event_type, 0x21);
    let create = events
        .iter()
        .position(|e| {
            e.event_type == 0x02 && String::from_utf8_lossy(&e.bytes).contains("CREATE TABLE `t`")
        })
        .unwrap();
    assert!(events.iter().skip(create + 1).any(|e| e.event_type == 0x1e));
    assert_eq!(events.last().unwrap().event_type, 0x10);
    assert!(!events
        .iter()
        .any(|e| String::from_utf8_lossy(&e.bytes).contains("RESTORE TABLE")));
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(64)]]
    );
    engine.execute("INSERT INTO t VALUES(100,NULL)").unwrap();
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(65)]]
    );
    for sql in [
        "CREATE TABLE counted(id BIGINT AUTO_INCREMENT PRIMARY KEY)",
        "INSERT INTO counted VALUES(999)",
        "DELETE FROM counted",
        "DROP TABLE counted",
    ] {
        engine.execute(sql).unwrap();
    }
    let name = engine
        .db
        .recyclebin_entries()
        .unwrap()
        .into_iter()
        .find(|e| e.origin_table == "counted")
        .unwrap()
        .name();
    let before = binlog::current_position().unwrap();
    engine.execute(&format!("RESTORE TABLE {name}")).unwrap();
    let events = binlog::manager()
        .unwrap()
        .events_after(&before.file, before.position)
        .unwrap();
    assert!(events.iter().any(|event| event.event_type == 0x02
        && String::from_utf8_lossy(&event.bytes).contains("AUTO_INCREMENT=1000")));
}

#[test]
fn recyclebin_crash_matrix_recovers_atomic_ownership() {
    for (operation, point, committed) in [
        ("drop", "after_prepare", false),
        ("drop", "before_commit", false),
        ("drop", "after_dd_rows", false),
        ("drop", "after_commit", true),
        ("truncate", "after_prepare", false),
        ("truncate", "before_commit", false),
        ("truncate", "after_dd_rows", false),
        ("truncate", "after_commit", true),
        ("restore", "before_commit", false),
        ("restore", "after_dd_rows", false),
        ("restore", "after_commit", true),
        ("purge", "before_commit", false),
        ("purge", "after_dd_rows", false),
        ("purge", "after_commit", true),
        ("cleanup", "after_file_delete", true),
        ("cleanup", "before_cleanup_commit", true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let name;
        {
            let engine = open_strict_test_engine(dir.path());
            engine
                .execute("CREATE TABLE t(id INT PRIMARY KEY, v LONGTEXT)")
                .unwrap();
            engine
                .execute("INSERT INTO t VALUES(7,REPEAT('z',20000))")
                .unwrap();
            if matches!(operation, "restore" | "purge" | "cleanup") {
                engine.execute("DROP TABLE t").unwrap();
            }
            name = engine
                .db
                .recyclebin_entries()
                .unwrap()
                .first()
                .map(|e| e.name())
                .unwrap_or_default();
            engine.shutdown().unwrap();
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "engine::executor::tests::recyclebin::recyclebin_crash_child",
            ])
            .env("MYDB_RECYCLEBIN_TEST_DIR", dir.path())
            .env("MYDB_RECYCLEBIN_TEST_OPERATION", operation)
            .env("MYDB_RECYCLEBIN_TEST_NAME", &name)
            .env("MYDB_RECYCLEBIN_TEST_CRASH", point)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "{operation}/{point}");
        let engine = open_strict_test_engine(dir.path());
        let entries = engine.db.recyclebin_entries().unwrap();
        match operation {
            "drop" | "truncate" if !committed => {
                assert!(entries.is_empty());
                assert_eq!(rows(&engine, "SELECT id FROM t"), vec![vec![Value::Int(7)]]);
            }
            "drop" | "truncate" => {
                assert_eq!(entries.len(), 1);
                assert_eq!(
                    rows(
                        &engine,
                        &format!(
                            "SELECT id,LENGTH(v) FROM __mydb_recyclebin__.{}",
                            entries[0].name()
                        )
                    ),
                    vec![vec![Value::Int(7), Value::Int(20000)]]
                );
                if operation == "truncate" {
                    assert!(rows(&engine, "SELECT * FROM t").is_empty());
                }
            }
            "restore" => {
                assert_eq!(entries.is_empty(), committed);
                assert_eq!(engine.db.table("t").is_ok(), committed);
            }
            _ => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].purging, committed);
                if committed {
                    for _ in 0..3 {
                        engine.db.maintain_recyclebin().unwrap();
                    }
                    assert!(engine.db.recyclebin_entries().unwrap().is_empty());
                }
            }
        }
    }
}

#[test]
#[ignore = "subprocess crash fixture invoked by recyclebin_crash_matrix_recovers_atomic_ownership"]
fn recyclebin_crash_child() {
    let directory = std::env::var("MYDB_RECYCLEBIN_TEST_DIR").unwrap();
    let operation = std::env::var("MYDB_RECYCLEBIN_TEST_OPERATION").unwrap();
    let name = std::env::var("MYDB_RECYCLEBIN_TEST_NAME").unwrap();
    let engine = open_strict_test_engine(std::path::Path::new(&directory));
    match operation.as_str() {
        "drop" => {
            engine.execute("DROP TABLE t").unwrap();
        }
        "truncate" => {
            engine.execute("TRUNCATE TABLE t").unwrap();
        }
        "restore" => {
            engine.execute(&format!("RESTORE TABLE {name}")).unwrap();
        }
        "purge" => {
            engine.execute(&format!("PURGE RECYCLEBIN {name}")).unwrap();
        }
        "cleanup" => {
            engine.execute(&format!("PURGE RECYCLEBIN {name}")).unwrap();
            engine.db.maintain_recyclebin().unwrap();
        }
        _ => panic!("invalid crash fixture"),
    }
    panic!("crash point was not reached");
}

#[test]
#[ignore = "run separately from the regression suite to avoid timing contention"]
fn recyclebin_performance_probe() {
    use serde_json::json;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    engine
        .execute("CREATE TABLE hot(id INT PRIMARY KEY,v BIGINT,s VARCHAR(32),j JSON)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO hot SELECT n,0,'payload','{\"k\":1}' FROM GENERATE_SERIES(1,256) AS g(n)",
        )
        .unwrap();
    let reads: Vec<_> = (1..=256)
        .map(|id| format!("SELECT v,LENGTH(s),JSON_EXTRACT(j,'$.k') FROM hot WHERE id={id}"))
        .collect();
    let writes: Vec<_> = (1..=256)
        .map(|id| format!("UPDATE hot SET v=v+1 WHERE id={id}"))
        .collect();
    let run = || {
        let mut latency = Vec::new();
        let start = Instant::now();
        for i in 0..3000 {
            let begin = Instant::now();
            engine
                .execute(if i % 5 == 0 {
                    &writes[i % 256]
                } else {
                    &reads[i % 256]
                })
                .unwrap();
            latency.push(begin.elapsed().as_nanos() as u64);
        }
        let elapsed = start.elapsed().as_secs_f64();
        latency.sort_unstable();
        (3000.0 / elapsed, latency[latency.len() * 95 / 100])
    };
    run();
    let mut trials = Vec::new();
    for round in 0..5 {
        for enabled in [0, 1] {
            engine
                .db
                .set_recyclebin_variable("mydb_recyclebin", enabled)
                .unwrap();
            let (ops, p95) = run();
            trials.push(json!({"round":round,"mode":if enabled==0 {"off"} else {"on"},"ops_per_second":ops,"p95_ns":p95}));
        }
    }
    engine
        .db
        .set_recyclebin_variable("mydb_recyclebin", 1)
        .unwrap();
    for i in 0..1000 {
        engine
            .execute(&format!(
                "CREATE TABLE discarded_{i}(id BIGINT PRIMARY KEY,v VARCHAR(32))"
            ))
            .unwrap();
        engine
            .execute(&format!("DROP TABLE discarded_{i}"))
            .unwrap();
    }
    run();
    for round in 0..5 {
        let (ops, p95) = run();
        trials.push(json!({"round":round,"mode":"backlog_1000","ops_per_second":ops,"p95_ns":p95}));
    }
    let mut scale = Vec::new();
    for count in [1000, 10000, 100000] {
        engine
            .execute("CREATE TABLE bulk(id BIGINT PRIMARY KEY,v VARCHAR(64))")
            .unwrap();
        engine
            .execute(&format!(
                "INSERT INTO bulk SELECT n,REPEAT('x',64) FROM GENERATE_SERIES(1,{count}) AS g(n)"
            ))
            .unwrap();
        engine.db.checkpoint().unwrap();
        let original = engine.db.table("bulk").unwrap();
        let before = engine.db.storage_status();
        let start = Instant::now();
        engine.execute("DROP TABLE bulk").unwrap();
        let dropped = start.elapsed().as_micros();
        let entry = engine
            .db
            .recyclebin_entries()
            .unwrap()
            .into_iter()
            .find(|e| e.table_id == original.id)
            .unwrap();
        let start = Instant::now();
        engine
            .execute(&format!("RESTORE TABLE {}", entry.name()))
            .unwrap();
        let restored = start.elapsed().as_micros();
        let after = engine.db.storage_status();
        assert_eq!(engine.db.table("bulk").unwrap().id, original.id);
        scale.push(json!({"rows":count,"drop_us":dropped,"restore_us":restored,
            "physical_read_ios":after.buffer_pool_read_ios-before.buffer_pool_read_ios,
            "wal_heap_payload_bytes":after.wal_heap_payload_bytes-before.wal_heap_payload_bytes}));
        let _policy = crate::recyclebin::PolicyGuard::enter(Some(false));
        engine.db.drop_table("bulk").unwrap();
    }
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    std::fs::write(
        "/tmp/mydb/recyclebin-performance.json",
        serde_json::to_vec_pretty(&json!({"trials":trials,"ddl_scale":scale})).unwrap(),
    )
    .unwrap();
}
