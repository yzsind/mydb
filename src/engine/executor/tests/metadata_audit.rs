use super::*;

fn dir() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::tempdir_in("/tmp/mydb").unwrap()
}

fn result(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(rows) = engine.execute(sql).unwrap() else {
        panic!("{sql}")
    };
    rows.rows
}

#[test]
fn metadata_audit_trigger_cold_descriptor() {
    let dir = dir();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO parent VALUES(1)").unwrap();
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY AUTO_INCREMENT, p INT, v DECIMAL(10,2), g INT AS (p+1) STORED, KEY vi(v), CONSTRAINT ck CHECK(v>=0), CONSTRAINT fk FOREIGN KEY(p) REFERENCES parent(id))").unwrap();
    engine.execute("INSERT INTO t(p,v) VALUES(1,2.5)").unwrap();
    let definition = result(&engine, "SHOW CREATE TABLE t");
    let data = result(&engine, "SELECT * FROM t");
    drop(engine);
    for sql in [
        "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW SET @x=1",
        "DROP TRIGGER tr",
    ] {
        let engine = open_strict_test_engine(dir.path());
        engine.database().set_catalog_cache_mb(0);
        let version = engine.database().table("test.t").unwrap().schema_version;
        engine
            .database()
            .force_cold_online_descriptor_for_test("test.t");
        engine.execute(sql).unwrap();
        assert_eq!(result(&engine, "SHOW CREATE TABLE t"), definition);
        assert_eq!(result(&engine, "SELECT * FROM t"), data);
        assert_eq!(
            engine.database().table("test.t").unwrap().schema_version,
            version + 1
        );
        drop(engine);
        let engine = open_strict_test_engine(dir.path());
        assert_eq!(result(&engine, "SHOW CREATE TABLE t"), definition);
        assert_eq!(result(&engine, "SELECT * FROM t"), data);
        assert_eq!(
            engine.database().table("test.t").unwrap().schema_version,
            version + 1
        );
    }
}

#[test]
fn metadata_audit_relation_namespace() {
    let dir = dir();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE SEQUENCE s START 100").unwrap();
    for sql in [
        "CREATE VIEW s AS SELECT 1 AS v",
        "CREATE OR REPLACE VIEW s AS SELECT 1 AS v",
        "CREATE VIEW IF NOT EXISTS s AS SELECT 1 AS v",
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 1050, "{sql}");
    }
    assert_eq!(
        result(&engine, "SELECT NEXTVAL('test.s')")[0][0].to_display_string(),
        "100"
    );
    engine.execute("DROP SEQUENCE s").unwrap();
    engine.execute("CREATE VIEW s AS SELECT 1 AS v").unwrap();
    assert_eq!(
        engine.execute("CREATE SEQUENCE s").unwrap_err().code(),
        1050
    );
}

#[test]
fn metadata_audit_event_rename() {
    let dir = dir();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE EVENT ev ON SCHEDULE EVERY 1 HOUR DISABLE DO SELECT 1")
        .unwrap();
    let before = engine.database().event_definition("test", "ev").unwrap();
    assert_eq!(
        engine
            .execute("ALTER EVENT ev RENAME TO ghost.ev2")
            .unwrap_err()
            .code(),
        1049
    );
    assert_eq!(
        engine
            .database()
            .event_definition("test", "ev")
            .unwrap()
            .schema_version,
        before.schema_version
    );
    engine.execute("CREATE DATABASE dest").unwrap();
    engine
        .execute("CREATE EVENT dest.ev2 ON SCHEDULE EVERY 1 HOUR DISABLE DO SELECT 2")
        .unwrap();
    assert_eq!(
        engine
            .execute("ALTER EVENT ev RENAME TO dest.ev2")
            .unwrap_err()
            .code(),
        1537
    );
    engine.execute("ALTER EVENT ev RENAME TO dest.ev3").unwrap();
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert!(engine.database().event_definition("test", "ev").is_none());
    assert_eq!(
        engine
            .database()
            .event_definition("dest", "ev3")
            .unwrap()
            .schema_version,
        before.schema_version + 1
    );
}

#[test]
fn metadata_audit_commit_failure_fail_stops() {
    let dir = dir();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1)").unwrap();
    engine.database().fail_next_catalog_commit_for_test();
    assert!(engine
        .execute("CREATE VIEW v AS SELECT * FROM t")
        .unwrap_err()
        .message()
        .contains("injected catalog commit failure"));
    for sql in [
        "SELECT * FROM t",
        "INSERT INTO t VALUES(2)",
        "CREATE DATABASE failed",
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 1053, "{sql}");
    }
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        result(&engine, "SELECT * FROM t"),
        vec![vec![Value::Int(1)]]
    );
    assert!(engine.execute("SELECT * FROM v").is_err());
}

#[test]
fn metadata_audit_event_runtime_state() {
    let dir = dir();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE EVENT ev ON SCHEDULE EVERY 1 HOUR DISABLE DO SELECT 1")
        .unwrap();
    let version = engine
        .database()
        .event_definition("test", "ev")
        .unwrap()
        .schema_version;
    let recorded = engine
        .database()
        .record_event_run("test", "ev", version, 12345)
        .unwrap()
        .unwrap();
    assert_eq!(recorded.last_executed_ms, 12345);
    assert_eq!(recorded.schema_version, version);
    engine
        .database()
        .set_event_status("test", "ev", crate::storage::EventStatusSchema::Enabled)
        .unwrap();
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    let event = engine.database().event_definition("test", "ev").unwrap();
    assert_eq!(event.last_executed_ms, 12345);
    assert_eq!(event.schema_version, version);
    assert_eq!(event.status, crate::storage::EventStatusSchema::Enabled);
    engine.execute("ALTER EVENT ev DISABLE").unwrap();
    assert_eq!(
        engine
            .database()
            .event_definition("test", "ev")
            .unwrap()
            .schema_version,
        version + 1
    );
}

#[test]
fn metadata_audit_explicit_view_rebind_and_names() {
    let dir = dir();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE DATABASE MyCaseDb").unwrap();
    engine.execute("CREATE TABLE MyCaseDb.Bt(a INT)").unwrap();
    engine
        .execute("CREATE VIEW v AS SELECT a FROM MyCaseDb.Bt")
        .unwrap();
    engine.execute("DROP TABLE MyCaseDb.Bt").unwrap();
    engine.execute("CREATE TABLE mycasedb.bt(a INT)").unwrap();
    engine.execute("INSERT INTO MYCASEDB.BT VALUES(7)").unwrap();
    assert_eq!(
        result(&engine, "SELECT * FROM v"),
        vec![vec![Value::Int(7)]]
    );
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        result(&engine, "SELECT * FROM v"),
        vec![vec![Value::Int(7)]]
    );
    engine
        .execute("CREATE OR REPLACE VIEW v AS SELECT a FROM MyCaseDb.Bt")
        .unwrap();
    assert_eq!(
        result(&engine, "SELECT * FROM v"),
        vec![vec![Value::Int(7)]]
    );
    assert_eq!(
        result(&engine, "SELECT @@global.lower_case_table_names")[0][0].to_display_string(),
        "1"
    );
    assert_eq!(
        result(
            &engine,
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='mycasedb'"
        ),
        vec![vec![Value::String("mycasedb".into())]]
    );
}

#[test]
fn metadata_audit_trigger_version_survives_eviction() {
    let dir = dir();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY, v INT) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,7),(2,8)").unwrap();
    let data = result(&engine, "SELECT * FROM t ORDER BY id");
    let definition = result(&engine, "SHOW CREATE TABLE t");
    let mut version = engine.database().table("test.t").unwrap().schema_version;
    for cache in [0, 1] {
        engine.database().set_catalog_cache_mb(cache);
        for n in 0..3 {
            for sql in [
                format!("CREATE TRIGGER tr{n} AFTER INSERT ON t FOR EACH ROW SET @x=1"),
                format!("DROP TRIGGER tr{n}"),
            ] {
                engine
                    .database()
                    .force_cold_online_descriptor_for_test("test.t");
                engine.execute(&sql).unwrap();
                version += 1;
                assert_eq!(
                    engine.database().table("test.t").unwrap().schema_version,
                    version
                );
                assert_eq!(result(&engine, "SHOW CREATE TABLE t"), definition);
                assert_eq!(result(&engine, "SELECT * FROM t ORDER BY id"), data);
            }
        }
    }
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        engine.database().table("test.t").unwrap().schema_version,
        version
    );
    assert_eq!(result(&engine, "SHOW CREATE TABLE t"), definition);
    assert_eq!(result(&engine, "SELECT * FROM t ORDER BY id"), data);
}
