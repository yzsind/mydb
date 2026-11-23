use super::*;

fn fixture() -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let e = open_test_engine(dir.path());
    (dir, e)
}
fn rows(e: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    match e.execute(sql).unwrap().materialize().unwrap() {
        QueryResponse::ResultSet(rs) => rs.rows,
        other => panic!("{sql}: {other:?}"),
    }
}
fn exec(e: &StorageQueryEngine, sql: &[&str]) {
    for s in sql {
        e.execute(s).unwrap();
    }
}

#[test]
fn five_audit_fk_index_rebind_and_drop_column() {
    for action in ["CASCADE", "RESTRICT", "SET NULL"] {
        for ddl in [
            "ALTER TABLE c ADD INDEX alt(pid); ALTER TABLE c DROP INDEX fk",
            "ALTER TABLE c DROP COLUMN b",
        ] {
            let (_d, e) = fixture();
            exec(&e,&["CREATE TABLE p(id INT PRIMARY KEY)", &format!("CREATE TABLE c(id INT PRIMARY KEY,pid INT,b INT,INDEX fk(pid,b),CONSTRAINT f FOREIGN KEY(pid) REFERENCES p(id) ON DELETE {action})"), "INSERT INTO p VALUES(1)", "INSERT INTO c VALUES(1,1,9)", ddl]);
            let deleted = e.execute("DELETE FROM p WHERE id=1");
            if action == "RESTRICT" {
                assert!(deleted
                    .unwrap_err()
                    .is(crate::error::catalog::ER_ROW_IS_REFERENCED_2));
            } else {
                deleted.unwrap();
                assert_eq!(
                    rows(&e, "SELECT pid FROM c"),
                    if action == "CASCADE" {
                        vec![]
                    } else {
                        vec![vec![Value::Null]]
                    }
                );
            }
        }
    }
}
#[test]
fn five_audit_fk_column_change_guards() {
    let (_d, e) = fixture();
    exec(&e,&["CREATE TABLE p(id VARCHAR(20) COLLATE utf8mb4_general_ci PRIMARY KEY)", "CREATE TABLE c(id INT,pid VARCHAR(20) COLLATE utf8mb4_general_ci,FOREIGN KEY(pid) REFERENCES p(id))"]);
    for sql in [
        "ALTER TABLE c MODIFY pid VARCHAR(20) COLLATE utf8mb4_bin",
        "ALTER TABLE p MODIFY id VARCHAR(20) COLLATE utf8mb4_bin",
    ] {
        assert!(e.execute(sql).is_err(), "{sql}");
    }
    exec(
        &e,
        &[
            "CREATE TABLE np(id INT PRIMARY KEY)",
            "CREATE TABLE nc(pid INT,FOREIGN KEY(pid) REFERENCES np(id) ON DELETE SET NULL)",
        ],
    );
    assert!(e
        .execute("ALTER TABLE nc CHANGE pid renamed INT NOT NULL")
        .unwrap_err()
        .is(crate::error::catalog::ER_FK_COLUMN_NOT_NULL));
}
#[test]
fn five_audit_fk_parent_index_requires_complete_unique_key() {
    let (_d, e) = fixture();
    exec(
        &e,
        &[
            "CREATE TABLE p(id INT,v INT,UNIQUE KEY u(id),UNIQUE KEY longer(id,v))",
            "CREATE TABLE c(pid INT,FOREIGN KEY(pid) REFERENCES p(id))",
        ],
    );
    assert!(e
        .execute("ALTER TABLE p DROP INDEX u")
        .unwrap_err()
        .is(crate::error::catalog::ER_DROP_INDEX_FK));
}
#[test]
fn five_audit_temp_add_check_validates_existing_rows() {
    let (_d, e) = fixture();
    let mut session = SessionContext::default();
    for sql in ["CREATE TEMPORARY TABLE t(a INT)", "INSERT INTO t VALUES(1)"] {
        e.execute_with_session(&mut session, sql).unwrap();
    }
    assert!(e
        .execute_with_session(
            &mut session,
            "ALTER TABLE t ADD COLUMN b INT DEFAULT -1 CHECK(b>0)"
        )
        .is_err());
    let result = e
        .execute_with_session(&mut session, "SELECT * FROM t")
        .unwrap()
        .materialize()
        .unwrap();
    assert_eq!(support::result_rows(result), vec![vec![Value::Int(1)]]);
}
#[test]
fn five_audit_single_row_storage_fk_is_enforced() {
    let (_d, e) = fixture();
    exec(
        &e,
        &[
            "CREATE TABLE p(id INT PRIMARY KEY)",
            "CREATE TABLE c(id INT PRIMARY KEY,pid INT,FOREIGN KEY(pid) REFERENCES p(id))",
        ],
    );
    let db = e.database();
    let txn = db.begin_txn().unwrap();
    let result = db.insert_row(txn, "test.c", vec![Value::Int(1), Value::Int(999)]);
    db.rollback_txn(txn.txn_id).unwrap();
    assert!(result
        .unwrap_err()
        .is(crate::error::catalog::ER_NO_REFERENCED_ROW_2));
}
#[test]
fn five_audit_auto_increment_converted_zero() {
    for ty in ["INT", "BIGINT UNSIGNED"] {
        for zero in ["'0'", "0.0", "'0.0'", "'0e0'"] {
            let (_d, e) = fixture();
            exec(
                &e,
                &[
                    &format!("CREATE TABLE t(id {ty} AUTO_INCREMENT PRIMARY KEY,v INT)"),
                    "INSERT INTO t(v) VALUES(1)",
                    &format!("INSERT INTO t VALUES({zero},2)"),
                ],
            );
            assert_eq!(
                rows(&e, "SELECT CAST(id AS SIGNED) FROM t WHERE v=2"),
                vec![vec![Value::Int(2)]],
                "{ty} {zero}"
            );
        }
    }
}
#[test]
fn five_audit_explicit_insert_id_preserves_session_id() {
    let (_d, e) = fixture();
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE t(id INT AUTO_INCREMENT PRIMARY KEY,v INT)",
        "INSERT INTO t(v) VALUES(1)",
    ] {
        e.execute_with_session(&mut session, sql).unwrap();
    }
    for sql in [
        "INSERT INTO t VALUES('50',2)",
        "INSERT IGNORE INTO t VALUES(100,2)",
        "REPLACE INTO t VALUES(200,3)",
        "INSERT INTO t VALUES(300,4) ON DUPLICATE KEY UPDATE v=VALUES(v)",
    ] {
        e.execute_with_session(&mut session, sql).unwrap();
        assert!(session.statement_insert_id >= 50);
        assert_eq!(
            session
                .last_insert_id
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "{sql}"
        );
    }
}
#[test]
fn five_audit_odku_values_generated_id() {
    let (_d, e) = fixture();
    exec(
        &e,
        &[
            "CREATE TABLE t(id INT AUTO_INCREMENT PRIMARY KEY,u INT UNIQUE,v INT)",
            "INSERT INTO t(u,v) VALUES(1,0)",
            "INSERT INTO t(u,v) VALUES(1,9) ON DUPLICATE KEY UPDATE v=VALUES(id)",
        ],
    );
    assert_eq!(rows(&e, "SELECT v FROM t"), vec![vec![Value::Int(2)]]);
}
#[test]
fn five_audit_auto_increment_readded_uses_current_data() {
    let (_d, e) = fixture();
    exec(
        &e,
        &[
            "CREATE TABLE t(id INT AUTO_INCREMENT PRIMARY KEY,v INT)",
            "INSERT INTO t VALUES(1000,1)",
            "ALTER TABLE t MODIFY id INT NOT NULL",
            "DELETE FROM t",
            "ALTER TABLE t MODIFY id INT AUTO_INCREMENT",
            "INSERT INTO t(v) VALUES(9)",
        ],
    );
    assert_eq!(rows(&e, "SELECT id FROM t"), vec![vec![Value::Int(1)]]);
}
#[test]
fn five_audit_exchange_rejects_auto_increment_mismatch() {
    let (_d, e) = fixture();
    exec(&e,&["CREATE TABLE p(id INT NOT NULL,k INT NOT NULL,PRIMARY KEY(id,k)) PARTITION BY RANGE(k)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)","CREATE TABLE s(id INT NOT NULL AUTO_INCREMENT,k INT NOT NULL,PRIMARY KEY(id,k))"]);
    assert!(e
        .execute("ALTER TABLE p EXCHANGE PARTITION p0 WITH TABLE s")
        .unwrap_err()
        .is(crate::error::catalog::ER_TABLES_DIFFERENT_METADATA));
}
#[test]
fn five_audit_limit_null_matches_mysql_and_flush_alias() {
    let (_d, e) = fixture();
    assert_eq!(
        crate::engine::params::limit_param_usize(&Value::Null).unwrap(),
        0
    );
    e.execute("ALTER SYSTEM FLUSH SQL CACHE").unwrap();
    e.execute("ALTER SYSTEM FLUSH DATA CACHE").unwrap();
}

#[test]
fn five_audit_shared_identity_ignores_binding_timing_but_memo_checks_domains() {
    let (_d, e) = fixture();
    let session = SessionContext::default();
    let db = e.database();
    let unbound = super::super::plan_semantics(&db, &session);
    crate::engine::params::with_params_slice(&[Value::Int(1)], || {
        assert_eq!(unbound, super::super::plan_semantics(&db, &session));
        let memo = crate::engine::cache::PreparedPlanSemantics::capture(&db, &session);
        assert!(memo.matches_session(&db, &session));
        crate::engine::params::with_params_slice(&[Value::String("1".into())], || {
            assert!(!memo.matches_session(&db, &session))
        });
    });
}
#[test]
fn five_audit_replication_preserves_zero_row_image() {
    let (_d, e) = fixture();
    e.execute("CREATE TABLE t(id INT AUTO_INCREMENT PRIMARY KEY,v INT)")
        .unwrap();
    let db = e.database();
    crate::replication::with_replication_applier_pub(|| {
        let txn = db.begin_txn()?;
        db.insert_rows(txn, "test.t", vec![vec![Value::Int(0), Value::Int(7)]])?;
        db.commit_txn(txn.txn_id)?;
        Ok(())
    })
    .unwrap();
    assert_eq!(rows(&e, "SELECT id FROM t"), vec![vec![Value::Int(0)]]);
    e.execute("INSERT INTO t(v) VALUES(8)").unwrap();
    assert_eq!(
        rows(&e, "SELECT id FROM t WHERE v=8"),
        vec![vec![Value::Int(1)]]
    );
}
#[test]
fn five_audit_ai_durable_publication_failure_stops_database() {
    let (_d, e) = fixture();
    let db = e.database();
    let result = crate::engine::ai::catalog_faults::with_failure("before_publish", || {
        db.mutate_ai_catalog(|catalog| {
            catalog.next_id += 1;
            Ok(())
        })
    });
    assert!(result.is_err());
    assert!(!db.is_operational());
}

#[test]
fn five_audit_binlog_reset_rejected_on_replica() {
    for sql in ["RESET MASTER", "RESET BINARY LOGS AND GTIDS"] {
        let statement = crate::parser::parse_statement(sql).unwrap();
        assert!(crate::replication::is_replica_write_statement(&statement));
        assert!(crate::statement_effects::statement_effects(&statement).requires_implicit_commit);
    }
}
#[test]
fn five_audit_analyze_invalidates_internal_plan_cache() {
    let (_d, e) = fixture();
    e.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    let before = e.plan_cache.current_epoch();
    e.execute("ANALYZE TABLE t").unwrap();
    assert!(e.plan_cache.current_epoch() > before);
}

#[test]
fn five_audit_cold_foreign_keys_survive_rebuild_and_direct_add() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let open = || {
        let mut config = memory_test_storage_config();
        config.durability = crate::storage::DurabilityMode::Strict;
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap()
    };
    {
        let e = open();
        exec(
            &e,
            &[
                "CREATE TABLE p(id INT PRIMARY KEY)",
                "CREATE TABLE a(pid INT,FOREIGN KEY(pid) REFERENCES p(id))",
                "CREATE TABLE c(pid INT,FOREIGN KEY(pid) REFERENCES p(id) ON DELETE CASCADE)",
                "INSERT INTO p VALUES(1),(2)",
                "INSERT INTO a VALUES(1)",
                "INSERT INTO c VALUES(2)",
                "CREATE TABLE coldp(id INT PRIMARY KEY)",
                "CREATE TABLE newchild(pid INT)",
            ],
        );
        e.shutdown().unwrap();
    }
    let e = open();
    rows(&e, "SELECT * FROM a");
    rows(&e, "SELECT * FROM p");
    e.execute("CREATE TABLE unrelated(z INT)").unwrap();
    e.execute("DELETE FROM p WHERE id=2").unwrap();
    assert!(rows(&e, "SELECT * FROM c").is_empty());
    e.database()
        .add_foreign_key(
            "test.newchild",
            crate::parser::ForeignKeyDef {
                name: Some("new_fk".into()),
                columns: vec!["pid".into()],
                referenced_table: "test.coldp".into(),
                referenced_columns: vec!["id".into()],
                on_delete: None,
                on_update: None,
            },
        )
        .unwrap();
    e.shutdown().unwrap();
}

#[test]
fn five_audit_load_missing_required_field_obeys_mode_and_set() {
    let (dir, e) = fixture();
    let path = dir.path().join("load.tsv");
    std::fs::write(&path, b"1\n").unwrap();
    e.execute("CREATE TABLE t(id INT PRIMARY KEY,must VARCHAR(10) NOT NULL)")
        .unwrap();
    let load = format!(
        "LOAD DATA INFILE '{}' INTO TABLE t (id)",
        crate::util::file::sql_path(&path)
    );
    assert!(e
        .execute(&load)
        .unwrap_err()
        .is(crate::error::catalog::ER_NO_DEFAULT_FOR_FIELD));
    assert!(rows(&e, "SELECT * FROM t").is_empty());
    e.execute(&format!("{load} SET must='set'")).unwrap();
    assert_eq!(
        rows(&e, "SELECT must FROM t"),
        vec![vec![Value::String("set".into())]]
    );
    e.execute("DELETE FROM t").unwrap();
    let mut session = SessionContext::default();
    e.execute_with_session(&mut session, "SET sql_mode=''")
        .unwrap();
    e.execute_with_session(&mut session, &load).unwrap();
    assert_eq!(
        rows(&e, "SELECT must FROM t"),
        vec![vec![Value::String("".into())]]
    );
    assert!(session.warnings.iter().any(|w| w.code() == 1364));
}

#[test]
fn five_audit_triggered_odku_candidate_is_allocated_once() {
    let (_d, e) = fixture();
    exec(
        &e,
        &[
            "CREATE TABLE t(id INT AUTO_INCREMENT PRIMARY KEY,u INT UNIQUE,v INT)",
            "CREATE TRIGGER bi BEFORE INSERT ON t FOR EACH ROW SET NEW.v=NEW.v+1",
            "INSERT INTO t(u,v) VALUES(1,0)",
            "INSERT INTO t(u,v) VALUES(1,9) ON DUPLICATE KEY UPDATE v=VALUES(id)",
        ],
    );
    assert_eq!(rows(&e, "SELECT v FROM t"), vec![vec![Value::Int(2)]]);
    e.execute("INSERT INTO t(u,v) VALUES(2,0)").unwrap();
    assert_eq!(
        rows(&e, "SELECT id FROM t WHERE u=2"),
        vec![vec![Value::Int(3)]]
    );
}

#[test]
fn five_audit_direct_add_fk_loads_cold_parent() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let open = || {
        let mut config = memory_test_storage_config();
        config.durability = crate::storage::DurabilityMode::Strict;
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap()
    };
    {
        let e = open();
        exec(
            &e,
            &[
                "CREATE TABLE p(id INT PRIMARY KEY)",
                "CREATE TABLE c(pid INT)",
            ],
        );
        e.shutdown().unwrap();
    }
    let e = open();
    let db = e.database();
    db.table("test.c").unwrap();
    db.add_foreign_key(
        "test.c",
        crate::parser::ForeignKeyDef {
            name: None,
            columns: vec!["pid".into()],
            referenced_table: "test.p".into(),
            referenced_columns: vec!["id".into()],
            on_delete: None,
            on_update: None,
        },
    )
    .unwrap();
    assert!(e
        .execute("INSERT INTO c VALUES(999)")
        .unwrap_err()
        .is(crate::error::catalog::ER_NO_REFERENCED_ROW_2));
    e.shutdown().unwrap();
}
