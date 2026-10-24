use super::support::execute_materialized as exec;
use super::*;

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected rows for {sql}");
    };
    result.rows
}

#[test]
fn cold_rename_and_drop_index_preserve_dictionary_children() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(20),KEY vi(v))")
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES(1,'retained')")
            .unwrap();
    }
    {
        let engine = open_strict_test_engine(dir.path());
        engine.database().set_catalog_cache_mb(0);
        drop(engine.database().table("test.t").unwrap());
        assert!(engine.database().catalog().tables["test.t"]
            .columns
            .is_empty());
        engine.execute("RENAME TABLE t TO x,x TO renamed").unwrap();
        assert_eq!(
            rows(
                &engine,
                "SELECT id,v FROM renamed FORCE INDEX(vi) WHERE v='retained'"
            ),
            vec![vec![Value::Int(1), Value::String("retained".into())]]
        );
    }
    let engine = open_strict_test_engine(dir.path());
    engine.database().set_catalog_cache_mb(0);
    engine.execute("DROP INDEX vi ON renamed").unwrap();
    assert_eq!(
        rows(&engine, "SELECT id FROM renamed"),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        engine
            .database()
            .table("test.renamed")
            .unwrap()
            .indexes
            .len(),
        1
    );
    engine
        .execute("ALTER TABLE renamed RENAME COLUMN v TO changed")
        .unwrap();
    engine
        .execute("CREATE FULLTEXT INDEX words ON renamed(changed)")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT changed FROM renamed WHERE id=1"),
        vec![vec![Value::String("retained".into())]]
    );
    engine.execute("DROP INDEX words ON renamed").unwrap();
}

#[test]
fn drop_cascades_trigger_dictionary_across_reopen_and_name_reuse() {
    for drop_sql in ["DROP TABLE t", "DROP TABLE t,u", "DROP DATABASE d"] {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = open_strict_test_engine(dir.path());
            let mut session = SessionContext::default();
            for sql in [
                "CREATE DATABASE d",
                "USE d",
                "CREATE TABLE t(id INT PRIMARY KEY,v INT)",
                "CREATE TABLE u(id INT)",
                "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET NEW.v=99",
                drop_sql,
            ] {
                exec(&engine, &mut session, sql).unwrap();
            }
        }
        let engine = open_strict_test_engine(dir.path());
        let mut session = SessionContext::default();
        for sql in [
            "CREATE DATABASE IF NOT EXISTS d",
            "USE d",
            "CREATE TABLE t(id INT PRIMARY KEY,v INT)",
            "INSERT INTO t VALUES(1,2)",
        ] {
            exec(&engine, &mut session, sql).unwrap();
        }
        assert!(engine.database().trigger_definition("d", "tr").is_none());
        assert_eq!(
            rows(&engine, "SELECT v FROM d.t"),
            vec![vec![Value::Int(2)]]
        );
    }
}

#[test]
fn view_and_sequence_require_existing_schema() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE VIEW missing.v AS SELECT 1 AS n",
        "CREATE SEQUENCE missing.s",
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 1049);
    }
    assert!(engine.database().view_definition("missing", "v").is_none());
    assert!(engine.database().resolve_sequence("missing", "s").is_err());
}

#[test]
fn sequence_restart_validates_candidate_and_invalidates_session_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut a = SessionContext::default();
    a.connection_id = 14101;
    let mut b = SessionContext::default();
    b.connection_id = 14102;
    exec(
        &engine,
        &mut a,
        "CREATE SEQUENCE s MINVALUE 1 MAXVALUE 1000 CACHE 10",
    )
    .unwrap();
    exec(&engine, &mut a, "SELECT NEXTVAL('s')").unwrap();
    let before = engine.database().resolve_sequence("test", "s").unwrap();
    assert!(exec(
        &engine,
        &mut b,
        "ALTER SEQUENCE s INCREMENT BY 2 RESTART WITH 2000"
    )
    .is_err());
    assert_eq!(
        engine
            .database()
            .resolve_sequence("test", "s")
            .unwrap()
            .increment,
        before.increment
    );
    exec(&engine, &mut b, "ALTER SEQUENCE s RESTART WITH 500").unwrap();
    let QueryResponse::ResultSet(current) = exec(&engine, &mut a, "SELECT CURRVAL('s')").unwrap()
    else {
        panic!()
    };
    assert_eq!(current.rows, vec![vec![Value::Int(1)]]);
    assert_eq!(
        engine.database().resolve_sequence("test", "s").unwrap().id,
        before.id
    );

    let QueryResponse::ResultSet(result) = exec(&engine, &mut a, "SELECT NEXTVAL('s')").unwrap()
    else {
        panic!()
    };
    assert_eq!(result.rows, vec![vec![Value::Int(500)]]);
    exec(&engine, &mut b, "CREATE DATABASE other_db").unwrap();
    assert_eq!(
        exec(&engine, &mut b, "ALTER SEQUENCE s RENAME TO other_db.s2")
            .unwrap_err()
            .code(),
        1450
    );
    assert!(engine.database().resolve_sequence("test", "s").is_ok());
}

#[test]
fn sequence_failed_dictionary_publish_restores_definition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE SEQUENCE s MAXVALUE 1000").unwrap();
    engine.database().fail_next_catalog_commit_for_test();
    assert!(engine
        .execute("ALTER SEQUENCE s INCREMENT BY 3 RESTART WITH 99")
        .is_err());
    assert_eq!(
        engine
            .database()
            .resolve_sequence("test", "s")
            .unwrap()
            .increment,
        1
    );
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        rows(&engine, "SELECT NEXTVAL('s')"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn event_namespace_and_expiration_have_no_business_side_effects() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE DATABASE d",
        "CREATE TABLE sink(v INT)",
        "CREATE EVENT e ON SCHEDULE AT '2099-01-01 00:00:00' DISABLE DO SELECT 1",
        "CREATE EVENT d.e ON SCHEDULE AT '2099-01-01 00:00:00' DISABLE DO SELECT 2",
        "CREATE EVENT past ON SCHEDULE AT '2000-01-01 00:00:00' DO INSERT INTO sink VALUES(42)",
    ] {
        engine.execute(sql).unwrap();
    }
    assert!(engine.database().event_definition("d", "e").is_some());
    assert!(engine.database().event_definition("test", "past").is_none());
    assert!(rows(&engine, "SELECT * FROM sink").is_empty());
}

#[test]
fn drop_trigger_waits_for_existing_table_writer() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine
        .execute("CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET NEW.v=99")
        .unwrap();
    let mut writer = SessionContext::default();
    writer.connection_id = 14201;
    exec(&engine, &mut writer, "BEGIN").unwrap();
    exec(&engine, &mut writer, "INSERT INTO t VALUES(1,2)").unwrap();
    let mut ddl = SessionContext::default();
    ddl.connection_id = 14202;
    let blocked = exec(&engine, &mut ddl, "DROP TRIGGER tr");
    assert!(blocked.is_err(), "DROP crossed an active writer");
    assert!(engine.database().trigger_definition("test", "tr").is_some());
    exec(&engine, &mut writer, "COMMIT").unwrap();
    exec(&engine, &mut ddl, "DROP TRIGGER tr").unwrap();
    assert_eq!(rows(&engine, "SELECT v FROM t"), vec![vec![Value::Int(99)]]);
}

#[test]
fn force_rebuild_changes_generation_and_preserves_data_and_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v VARCHAR(20),KEY vi(v))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,'kept')").unwrap();
    let before = engine.database().table("test.t").unwrap().active_generation;
    engine.execute("ALTER TABLE t FORCE").unwrap();
    assert!(engine.database().table("test.t").unwrap().active_generation > before);
    assert_eq!(
        rows(&engine, "SELECT id FROM t FORCE INDEX(vi) WHERE v='kept'"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn truncate_publishes_empty_identity_and_resets_all_partition_indexes() {
    for suffix in ["", " PARTITION BY HASH(id) PARTITIONS 3"] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute(&format!(
                "CREATE TABLE t(id INT PRIMARY KEY AUTO_INCREMENT,v TEXT,KEY vi(v(10))){suffix}"
            ))
            .unwrap();
        engine
            .execute("INSERT INTO t(v) VALUES(REPEAT('x',20000))")
            .unwrap();
        let before = engine.database().table("test.t").unwrap();
        engine.database().fail_next_catalog_commit_for_test();
        assert!(engine.execute("TRUNCATE TABLE t").is_err());
        assert_eq!(engine.database().table("test.t").unwrap().id, before.id);
        assert_eq!(
            rows(&engine, "SELECT LENGTH(v) FROM t"),
            vec![vec![Value::Int(20000)]]
        );
        engine.execute("TRUNCATE TABLE t").unwrap();
        let after = engine.database().table("test.t").unwrap();
        assert_ne!(after.id, before.id);
        assert!(after
            .storage_ids()
            .iter()
            .all(|id| !before.storage_ids().contains(id)));
        engine.execute("INSERT INTO t(v) VALUES('new')").unwrap();
        assert_eq!(
            rows(&engine, "SELECT id FROM t FORCE INDEX(vi) WHERE v='new'"),
            vec![vec![Value::Int(1)]]
        );
        drop(engine);
        let engine = open_strict_test_engine(dir.path());
        assert_eq!(
            rows(&engine, "SELECT id,v FROM t"),
            vec![vec![Value::Int(1), Value::String("new".into())]]
        );
    }
}

#[test]
fn drop_database_dictionary_failure_preserves_all_objects_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        for sql in [
            "CREATE DATABASE d",
            "CREATE TABLE d.t(id INT PRIMARY KEY)",
            "INSERT INTO d.t VALUES(7)",
            "CREATE SEQUENCE d.s",
            "CREATE VIEW d.v AS SELECT id FROM d.t",
        ] {
            engine.execute(sql).unwrap();
        }
        engine.database().fail_next_catalog_commit_for_test();
        assert!(engine.execute("DROP DATABASE d").is_err());
    }
    let engine = open_strict_test_engine(dir.path());
    assert!(engine.database().database_exists("d"));
    assert_eq!(
        rows(&engine, "SELECT * FROM d.v"),
        vec![vec![Value::Int(7)]]
    );
    assert!(engine.database().resolve_sequence("d", "s").is_ok());
    engine.execute("DROP DATABASE d").unwrap();
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert!(!engine.database().database_exists("d"));
    assert!(engine.database().resolve_sequence("d", "s").is_err());
}

#[test]
fn alter_database_changes_only_defaults_and_is_durable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE DATABASE d DEFAULT CHARACTER SET latin1")
        .unwrap();
    engine
        .execute("CREATE TABLE d.old_table(v VARCHAR(20))")
        .unwrap();
    engine
        .execute("ALTER DATABASE d DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
        .unwrap();
    engine
        .execute("CREATE TABLE d.new_table(v VARCHAR(20))")
        .unwrap();
    assert_eq!(
        engine.database().table("d.old_table").unwrap().charset,
        "latin1"
    );
    assert_eq!(
        engine.database().table("d.new_table").unwrap().collation,
        "utf8mb4_bin"
    );
    assert!(engine
        .execute("ALTER DATABASE d CHARACTER SET latin1 COLLATE utf8mb4_bin")
        .is_err());
    let mut session = SessionContext::default();
    session.current_database = "d".into();
    exec(
        &engine,
        &mut session,
        "ALTER DATABASE DEFAULT COLLATE utf8mb4_general_ci",
    )
    .unwrap();
    assert_eq!(
        engine.database().schema_collation("d").as_deref(),
        Some("utf8mb4_general_ci")
    );
    exec(
        &engine,
        &mut session,
        "ALTER SCHEMA DEFAULT COLLATE utf8mb4_bin",
    )
    .unwrap();
    assert!(exec(
        &engine,
        &mut session,
        "ALTER DATABASE `` DEFAULT COLLATE latin1_bin"
    )
    .is_err());
    assert!(exec(&engine, &mut session, "CREATE DATABASE ``").is_err());
    session.current_database.clear();
    assert_eq!(
        exec(
            &engine,
            &mut session,
            "ALTER DATABASE DEFAULT COLLATE utf8mb4_bin"
        )
        .unwrap_err()
        .code(),
        1046
    );
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        engine.database().schema_collation("d").as_deref(),
        Some("utf8mb4_bin")
    );
}

#[test]
fn rename_index_preserves_dirty_handles_and_failed_publication() {
    for durable in [false, true] {
        for partition in ["", " PARTITION BY HASH(id) PARTITIONS 2"] {
            let dir = tempfile::tempdir().unwrap();
            let engine = if durable {
                open_strict_test_engine(dir.path())
            } else {
                open_test_engine(dir.path())
            };
            let unique = if partition.is_empty() { "UNIQUE " } else { "" };
            engine
                .execute(&format!(
                    "CREATE TABLE t(id INT PRIMARY KEY,v INT,{unique}KEY vi(v)){partition}"
                ))
                .unwrap();
            engine.execute("INSERT INTO t VALUES(1,7),(2,8)").unwrap();
            engine.execute("UPDATE t SET v=9 WHERE id=2").unwrap();
            engine
                .execute("ALTER TABLE t RENAME INDEX vi TO renamed")
                .unwrap();
            let verify = |engine: &StorageQueryEngine, index: &str| {
                assert_eq!(
                    rows(
                        engine,
                        &format!("SELECT id,v FROM t FORCE INDEX({index}) WHERE v>=0 ORDER BY id")
                    ),
                    vec![
                        vec![Value::Int(1), Value::Int(7)],
                        vec![Value::Int(2), Value::Int(9)]
                    ]
                );
                assert_eq!(
                    engine
                        .execute(if partition.is_empty() {
                            "INSERT INTO t VALUES(3,9)"
                        } else {
                            "INSERT INTO t VALUES(2,9)"
                        })
                        .unwrap_err()
                        .code(),
                    1062
                );
            };
            verify(&engine, "renamed");
            engine.database().fail_next_catalog_commit_for_test();
            assert!(engine
                .execute("ALTER TABLE t RENAME INDEX renamed TO rejected")
                .is_err());
            verify(&engine, "renamed");
            engine
                .execute("ALTER TABLE t RENAME INDEX renamed TO final_name")
                .unwrap();
            verify(&engine, "final_name");
            if durable {
                drop(engine);
                let engine = open_strict_test_engine(dir.path());
                verify(&engine, "final_name");
            }
        }
    }
}

#[test]
fn rename_special_indexes_retains_query_results_across_rollback_and_restart() {
    for (definition, values, query) in [
        ("v TEXT,FULLTEXT KEY original(v)", "'alpha bravo'", "SELECT id FROM t WHERE MATCH(v) AGAINST('alpha' IN BOOLEAN MODE)"),
        ("v GEOMETRY NOT NULL,SPATIAL KEY original(v)", "ST_GeomFromText('POINT(1 1)')", "SELECT id FROM t FORCE INDEX(renamed) WHERE MBRContains(ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))'),v)"),
        ("v VECTOR(3),KEY original(v) USING HNSW", "'[1,0,0]'", "SELECT id FROM t FORCE INDEX(renamed) ORDER BY DISTANCE(v,'[1,0,0]') LIMIT 1"),
        ("v VECTOR(3),KEY original(v) USING IVF_SQ8", "'[1,0,0]'", "SELECT id FROM t FORCE INDEX(renamed) ORDER BY DISTANCE(v,'[1,0,0]') LIMIT 1"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_strict_test_engine(dir.path());
        engine.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY,{definition})")).unwrap();
        engine.execute(&format!("INSERT INTO t VALUES(1,{values})")).unwrap();
        assert_eq!(rows(&engine, &query.replace("renamed", "original")), vec![vec![Value::Int(1)]]);
        engine.execute("ALTER TABLE t RENAME INDEX original TO renamed").unwrap();
        assert_eq!(rows(&engine, query), vec![vec![Value::Int(1)]]);
        engine.database().fail_next_catalog_commit_for_test();
        assert!(engine.execute("ALTER TABLE t RENAME INDEX renamed TO rejected").is_err());
        assert_eq!(rows(&engine, query), vec![vec![Value::Int(1)]]);
        drop(engine);
        let engine = open_strict_test_engine(dir.path());
        assert_eq!(rows(&engine, query), vec![vec![Value::Int(1)]]);
    }
}
