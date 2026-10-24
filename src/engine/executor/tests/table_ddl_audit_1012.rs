//! Regression coverage for the 2026-10-12 table DDL audit.
use super::*;

fn fixture() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::Builder::new()
        .prefix("ddl-audit-1012-")
        .tempdir_in("/tmp/mydb")
        .unwrap()
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap().materialize().unwrap()
    else {
        panic!("expected rows: {sql}");
    };
    result.rows
}

#[test]
fn table_ddl_audit_invalid_instant_does_not_commit() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    let mut session = SessionContext::default();
    for lock in ["NONE", "SHARED", "EXCLUSIVE"] {
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut session, "INSERT INTO t VALUES(1)")
            .unwrap();
        assert_eq!(
            engine
                .execute_with_session(
                    &mut session,
                    &format!("ALTER TABLE t ADD b INT, ALGORITHM=INSTANT, LOCK={lock}")
                )
                .unwrap_err()
                .code(),
            1221
        );
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
        assert_eq!(
            rows(&engine, "SELECT COUNT(*) FROM t"),
            vec![vec![Value::Int(0)]]
        );
    }
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES(1)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ALTER TABLE t ADD b INT")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn table_ddl_audit_exchange_requires_explicit_target_lock() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE p(id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY)")
        .unwrap();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "LOCK TABLES p WRITE")
        .unwrap();
    let result = engine.execute_with_session(
        &mut session,
        "ALTER TABLE p EXCHANGE PARTITION p0 WITH TABLE t",
    );
    engine
        .execute_with_session(&mut session, "UNLOCK TABLES")
        .unwrap();
    assert_eq!(result.unwrap_err().code(), 1100);
}

#[test]
fn table_ddl_audit_copy_pins_zero_retention_undo() {
    for partition in ["", "PARTITION BY HASH(id) PARTITIONS 2"] {
        let dir = fixture();
        let mut config = memory_test_storage_config();
        config.durability = crate::storage::DurabilityMode::Relaxed;
        config.undo_retention = Duration::ZERO;
        let engine = Arc::new(
            StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                config,
            )
            .unwrap(),
        );
        engine
            .execute(&format!(
                "CREATE TABLE t(id INT PRIMARY KEY,v INT,KEY vi(v)) {partition}"
            ))
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES(1,10),(2,20),(3,30)")
            .unwrap();
        let hook = engine.database().install_ddl_backfill_test_hook();
        let worker = engine.clone();
        let ddl = std::thread::spawn(move || {
            let mut s = SessionContext::default();
            s.connection_id = 9101;
            worker.execute_with_session(&mut s, "ALTER TABLE t ADD g INT AS(v*2) STORED")
        });
        let reached = hook.wait_until_reached(Duration::from_secs(5));
        let mut s = SessionContext::default();
        s.connection_id = 9102;
        let writes = (|| -> Result<(), QueryError> {
            for _ in 0..4 {
                engine.execute_with_session(&mut s, "UPDATE t SET v=v+1")?;
                engine.database().purge_obsolete_undo();
            }
            Ok(())
        })();
        hook.release();
        let result = ddl.join().unwrap();
        assert!(reached, "copy hook not reached: {result:?}");
        writes.unwrap();
        result.unwrap();
        assert_eq!(
            rows(&engine, "SELECT id,v,g FROM t ORDER BY id"),
            vec![
                vec![Value::Int(1), Value::Int(14), Value::Int(28)],
                vec![Value::Int(2), Value::Int(24), Value::Int(48)],
                vec![Value::Int(3), Value::Int(34), Value::Int(68)]
            ]
        );
    }
}

#[test]
fn table_ddl_audit_failed_unique_has_no_statistics_ghost() {
    let dir = fixture();
    let engine = Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,u INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,1),(2,2)").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut s = SessionContext::default();
        s.connection_id = 9201;
        worker.execute_with_session(&mut s, "ALTER TABLE t ADD UNIQUE KEY ku(u)")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(5));
    let mut s = SessionContext::default();
    s.connection_id = 9202;
    let write = engine.execute_with_session(&mut s, "INSERT INTO t VALUES(3,1)");
    // Populate system-view caches while the private index exists.
    let _ = rows(&engine, "SELECT * FROM information_schema.statistics");
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "build hook not reached: {result:?}");
    write.unwrap();
    assert_eq!(result.unwrap_err().code(), 1062);
    for sql in ["SELECT index_name FROM information_schema.statistics WHERE table_schema='test' AND table_name='t'", "SELECT index_name FROM information_schema.statistics WHERE index_name='ku'"] {
        assert!(rows(&engine,sql).iter().all(|row|row[0]!=Value::String("ku".into())),"{sql}");
    }
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(3)]]
    );
    engine.execute("DELETE FROM t WHERE id=3").unwrap();
    engine
        .execute("ALTER TABLE t ADD UNIQUE KEY ku(u)")
        .unwrap();
    assert_eq!(
        engine
            .execute("INSERT INTO t VALUES(4,1)")
            .unwrap_err()
            .code(),
        1062
    );
}

#[test]
fn table_ddl_audit_drop_add_layout_preserves_values() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,a INT,b INT DEFAULT 7,c CHAR(5))",
        "INSERT INTO t VALUES(1,1,11,'abc')",
        "ALTER TABLE t DROP a",
        "ALTER TABLE t ADD d INT DEFAULT 9",
        "INSERT INTO t(id,b,c,d) VALUES(2,22,'xy',99),(3,NULL,'z',NULL)",
    ] {
        engine.execute(sql).unwrap();
    }
    assert_eq!(
        rows(&engine, "SELECT b,d,c FROM t ORDER BY id"),
        vec![
            vec![Value::Int(11), Value::Int(9), Value::String("abc".into())],
            vec![Value::Int(22), Value::Int(99), Value::String("xy".into())],
            vec![Value::Null, Value::Null, Value::String("z".into())]
        ]
    );
}
