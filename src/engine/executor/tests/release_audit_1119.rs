use super::support::execute_materialized as exec;
use super::*;

#[test]
fn audit_1119_binlog_switch_rejects_active_transactions_atomically() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    for begin in ["BEGIN", "SET autocommit=0"] {
        exec(&engine, &mut s, begin).unwrap();
        if begin == "SET autocommit=0" {
            exec(&engine, &mut s, "INSERT INTO t VALUES(1)").unwrap();
        }
        let e = exec(&engine, &mut s, "SET sql_log_bin=0").unwrap_err();
        assert_eq!(e.code(), 1694);
        assert!(s.sql_log_bin);
        exec(&engine, &mut s, "ROLLBACK").unwrap();
    }
    exec(&engine, &mut s, "SET sql_log_bin=0").unwrap();
    assert!(!s.sql_log_bin);
}

#[test]
fn audit_1119_xa_idle_allows_only_queries_without_table_access() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut s, "XA START 'idle'").unwrap();
    assert_eq!(
        exec(&engine, &mut s, "XA ROLLBACK 'idle'")
            .unwrap_err()
            .code(),
        1399
    );
    exec(&engine, &mut s, "INSERT INTO t VALUES(1)").unwrap();
    exec(&engine, &mut s, "XA END 'idle'").unwrap();
    exec(&engine, &mut s, "SELECT 1, @@autocommit").unwrap();
    exec(&engine, &mut s, "USE test").unwrap();
    for sql in [
        "SELECT * FROM t",
        "SELECT (SELECT COUNT(*) FROM t)",
        "SHOW TABLES",
        "BEGIN",
        "SET autocommit=1",
    ] {
        assert_eq!(
            exec(&engine, &mut s, sql).unwrap_err().code(),
            1399,
            "{sql}"
        );
    }
    exec(&engine, &mut s, "XA ROLLBACK 'idle'").unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, "SELECT COUNT(*) FROM t").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows[0][0], Value::Int(0));
}

#[test]
fn audit_1119_replica_management_commits_before_execution_and_rejects_xa() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    exec(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut s, "BEGIN").unwrap();
    exec(&engine, &mut s, "INSERT INTO t VALUES(1)").unwrap();
    let _ = exec(&engine, &mut s, "START REPLICA");
    exec(&engine, &mut s, "ROLLBACK").unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut s, "SELECT COUNT(*) FROM t").unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows[0][0], Value::Int(1));
    exec(&engine, &mut s, "XA START 'admin'").unwrap();
    for sql in ["START REPLICA", "STOP REPLICA", "RESET MASTER"] {
        assert_eq!(
            exec(&engine, &mut s, sql).unwrap_err().code(),
            1399,
            "{sql}"
        );
    }
    exec(&engine, &mut s, "XA END 'admin'").unwrap();
    exec(&engine, &mut s, "XA ROLLBACK 'admin'").unwrap();
}

#[test]
fn audit_1119_keyring_requires_global_execute() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    exec(&engine, &mut root, "CREATE USER 'keyreader'@'%'").unwrap();
    exec(
        &engine,
        &mut root,
        "SELECT KEYRING_KEY_STORE('audit1119','AES','secret')",
    )
    .unwrap();
    let mut user = SessionContext::default();
    user.user = "keyreader".into();
    user.authenticated_as = crate::engine::auth::AccountKey::new("keyreader", "%");
    for f in [
        "KEYRING_KEY_FETCH('audit1119')",
        "KEYRING_KEY_REMOVE('audit1119')",
        "KEYRING_KEY_LENGTH('audit1119')",
        "KEYRING_KEY_TYPE('audit1119')",
        "KEYRING_KEY_STORE('audit1119other','AES','x')",
        "KEYRING_KEY_GENERATE('audit1119other','AES',128)",
    ] {
        assert_eq!(
            exec(&engine, &mut user, &format!("SELECT {f}"))
                .unwrap_err()
                .code(),
            1227,
            "{f}"
        );
    }
    exec(
        &engine,
        &mut root,
        "GRANT EXECUTE ON test.* TO 'keyreader'@'%'",
    )
    .unwrap();
    assert_eq!(
        exec(&engine, &mut user, "SELECT KEYRING_KEY_FETCH('audit1119')")
            .unwrap_err()
            .code(),
        1227
    );
    exec(
        &engine,
        &mut root,
        "GRANT EXECUTE ON *.* TO 'keyreader'@'%'",
    )
    .unwrap();
    exec(&engine, &mut user, "SELECT KEYRING_KEY_FETCH('audit1119')").unwrap();
    exec(
        &engine,
        &mut root,
        "REVOKE EXECUTE ON *.* FROM 'keyreader'@'%'",
    )
    .unwrap();
    assert_eq!(
        exec(&engine, &mut user, "SELECT KEYRING_KEY_FETCH('audit1119')")
            .unwrap_err()
            .code(),
        1227
    );
    exec(&engine, &mut root, "SELECT KEYRING_KEY_REMOVE('audit1119')").unwrap();
}
