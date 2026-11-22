use super::*;

#[test]
fn availability_gtid_monitoring_error_does_not_quarantine_database() {
    std::fs::create_dir_all("/tmp/mydb/availability").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb/availability").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE healthy (id INT PRIMARY KEY)")
        .unwrap();
    let db = engine.db_handle();
    db.set_replication_property("mysql-imported-gtids", Some("invalid-gtid"))
        .unwrap();
    db.invalidate_imported_gtids();
    for sql in [
        "SELECT @@global.gtid_executed",
        "SELECT @@global.gtid_purged",
        "SHOW GLOBAL VARIABLES",
        "SHOW SESSION VARIABLES",
        "SELECT * FROM information_schema.global_variables",
        "SELECT * FROM performance_schema.global_variables",
        "SELECT * FROM performance_schema.session_variables",
        "SELECT * FROM performance_schema.variables_by_thread",
        "SELECT * FROM sys.metrics",
    ] {
        let result = engine.execute(sql);
        assert!(
            db.ensure_operational().is_ok(),
            "monitoring quarantined the database: {sql}"
        );
        assert_eq!(
            result.unwrap_err().code(),
            crate::error::catalog::ER_MALFORMED_GTID_SET_SPECIFICATION.code(),
            "{sql}"
        );
    }
    engine.execute("SELECT @@version").unwrap();
    engine
        .execute("SELECT * FROM performance_schema.variables_info")
        .unwrap();
    engine.execute("INSERT INTO healthy VALUES (1)").unwrap();
    assert_eq!(
        support::query_rows(&engine, "SELECT id FROM healthy")[0][0],
        Value::Int(1)
    );
    db.persist_imported_gtids("").unwrap();
    engine.execute("SELECT @@global.gtid_executed").unwrap();
}

#[test]
fn availability_vector_statistics_error_does_not_quarantine_database() {
    std::fs::create_dir_all("/tmp/mydb/availability").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb/availability").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE monitored (id INT PRIMARY KEY, v VECTOR(3) NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO monitored VALUES (1, '[1,2,3]')")
        .unwrap();
    engine
        .execute("CREATE INDEX vi ON monitored(v) USING HNSW")
        .unwrap();
    engine
        .execute("CREATE TABLE healthy (id INT PRIMARY KEY)")
        .unwrap();
    let db = engine.db_handle();
    let table = db.table("test.monitored").unwrap();
    let index = table.indexes.iter().find(|i| i.name == "vi").unwrap();
    let path = crate::storage::naming::index_object_path_by_id(
        dir.path(),
        table.id,
        table.active_generation.max(1),
        index.index_id,
        "vmanifest",
    );
    let original = std::fs::read(&path).unwrap();
    std::fs::write(&path, b"invalid vector manifest").unwrap();
    for sql in [
        "SHOW TABLE STATUS LIKE 'monitored'",
        "SHOW FULL TABLES",
        "SELECT * FROM information_schema.tables WHERE table_name='monitored'",
        "SELECT * FROM information_schema.partitions WHERE table_name='monitored'",
        "SELECT * FROM information_schema.files",
        "SELECT * FROM information_schema.innodb_tablespaces",
        "SELECT * FROM information_schema.innodb_tablestats",
    ] {
        let result = engine.execute(sql);
        assert!(
            db.ensure_operational().is_ok(),
            "statistics quarantined the database: {sql}"
        );
        assert_eq!(
            result.unwrap_err().code(),
            crate::error::catalog::ER_INDEX_CORRUPT.code(),
            "{sql}"
        );
    }
    engine.execute("SHOW INDEX FROM monitored").unwrap();
    engine
        .execute("EXPLAIN SELECT id FROM monitored WHERE id=1")
        .unwrap();
    engine
        .execute("SELECT * FROM information_schema.statistics WHERE table_name='monitored'")
        .unwrap();
    engine.execute("INSERT INTO healthy VALUES (1)").unwrap();
    assert_eq!(
        support::query_rows(&engine, "SELECT id FROM healthy")[0][0],
        Value::Int(1)
    );
    std::fs::write(&path, original).unwrap();
    engine
        .execute("SHOW TABLE STATUS LIKE 'monitored'")
        .unwrap();
    assert_eq!(
        support::query_rows(&engine, "SELECT id FROM monitored")[0][0],
        Value::Int(1)
    );
}
