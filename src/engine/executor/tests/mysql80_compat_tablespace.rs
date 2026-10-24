//! Named TABLESPACE catalogs are unsupported; physical files remain observable.
use super::*;

#[test]
fn tablespace_statements_reject_without_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE existing_scope_table(id INT)")
        .unwrap();
    for sql in [
        "CREATE TABLESPACE ts ADD DATAFILE 'ts.ibd' ENGINE=InnoDB",
        "CREATE UNDO TABLESPACE uts",
        "DROP TABLESPACE ts",
        "DROP UNDO TABLESPACE uts",
        "ALTER TABLESPACE ts ADD DATAFILE 'other.ibd'",
        "CREATE TABLE named_space_table (id INT) TABLESPACE ts",
        "CREATE TEMPORARY TABLE named_space_temp (id INT) TABLESPACE ts",
        "CREATE TABLE IF NOT EXISTS existing_scope_table (id INT) TABLESPACE ts",
        "SELECT * FROM information_schema.TABLESPACES",
        "SELECT * FROM information_schema.TABLESPACES_EXTENSIONS",
    ] {
        let err = engine
            .execute(sql)
            .and_then(QueryResponse::materialize)
            .unwrap_err();
        assert_eq!(err.code(), 1235, "{sql}: {err:?}");
    }
    assert!(!dir.path().join("system/tablespaces.json").exists());
    assert!(engine.execute("SELECT * FROM named_space_table").is_err());
}

#[test]
fn unsupported_tablespace_does_not_implicitly_commit_or_register_a_temp_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let exec = |session: &mut SessionContext, sql: &str| {
        <StorageQueryEngine as QueryEngine>::execute(&engine, session, sql)
            .and_then(QueryResponse::materialize)
    };
    exec(&mut session, "CREATE TABLE scope_txn(id INT)").unwrap();
    exec(&mut session, "BEGIN").unwrap();
    exec(&mut session, "INSERT INTO scope_txn VALUES(1)").unwrap();
    for sql in [
        "CREATE TABLE IF NOT EXISTS scope_txn(id INT) TABLESPACE ts",
        "CREATE TEMPORARY TABLE scope_temp(id INT) TABLESPACE ts",
    ] {
        assert_eq!(exec(&mut session, sql).unwrap_err().code(), 1235);
    }
    exec(&mut session, "ROLLBACK").unwrap();
    let QueryResponse::ResultSet(rows) = exec(&mut session, "SELECT * FROM scope_txn").unwrap()
    else {
        panic!("expected rows");
    };
    assert!(rows.rows.is_empty());
    assert!(exec(&mut session, "SELECT * FROM scope_temp").is_err());
}
