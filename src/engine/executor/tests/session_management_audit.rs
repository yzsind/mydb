use super::*;

#[test]
fn session_audit_diagnostics_and_atomic_set() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    let mut run = |sql: &str| engine.execute_with_session(&mut s, sql);
    run("CREATE TABLE session_ids(id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)").unwrap();
    run("INSERT INTO session_ids(v) VALUES(1),(2),(3)").unwrap();
    assert!(run("SELECT * FROM session_missing_table").is_err());
    if let QueryResponse::ResultSet(r) = run("SELECT ROW_COUNT()").unwrap() {
        assert_eq!(r.rows, vec![vec![Value::Int(-1)]]);
    } else {
        panic!("expected result set");
    }
    engine
        .execute_with_session(&mut s, "SELECT LAST_INSERT_ID(0)")
        .unwrap();
    assert_eq!(
        s.last_insert_id.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    let r = engine
        .execute_with_session(&mut s, "SELECT LAST_INSERT_ID(5), LAST_INSERT_ID()")
        .unwrap();
    if let QueryResponse::ResultSet(r) = r {
        assert_eq!(r.rows, vec![vec![Value::UInt(5), Value::Int(5)]]);
    } else {
        panic!("expected result set");
    }
    engine
        .execute_with_session(&mut s, "SET time_zone='+08:00'")
        .unwrap();
    for bad in [
        "time_zone='Not/AZone'",
        "character_set_client='not_a_charset'",
        "sql_mode='not_a_mode'",
        "optimizer_switch='not_a_flag=on'",
        "default_storage_engine='missing'",
        "timestamp=253402300800",
    ] {
        assert!(
            engine
                .execute_with_session(&mut s, &format!("SET time_zone='+05:30', {bad}"))
                .is_err(),
            "{bad}"
        );
        assert_eq!(s.sysvars.time_zone, "+08:00", "partial SET: {bad}");
    }
    assert!(engine
        .execute_with_session(&mut s, "SET autocommit=0, time_zone='Not/AZone'")
        .is_err());
    assert!(s.autocommit);
    for name in ["timestamp", "sql_log_off", "rand_seed1", "rand_seed2"] {
        let err = engine
            .execute_with_session(&mut s, &format!("SELECT @@global.{name}"))
            .unwrap_err();
        assert!(
            err.is(crate::error::catalog::ER_INCORRECT_GLOBAL_LOCAL_VAR),
            "{name}: {err:?}"
        );
    }
    let err = engine
        .execute_with_session(&mut s, "SET GLOBAL wait_timeout=-1")
        .unwrap_err();
    assert!(err.is(crate::error::catalog::ER_WRONG_VALUE_FOR_VAR));
    assert!(engine
        .execute_with_session(&mut s, "SET GLOBAL @@session.autocommit=0")
        .is_err());
}

#[test]
fn session_audit_autocommit_and_isolation() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    let mut other = SessionContext::default();
    engine
        .execute_with_session(&mut s, "CREATE TABLE session_commit(a INT)")
        .unwrap();
    for sql in [
        "SET autocommit=1, wait_timeout=100",
        "SET autocommit=DEFAULT",
        "SET @@session.autocommit=1",
    ] {
        engine
            .execute_with_session(&mut s, "SET autocommit=0")
            .unwrap();
        assert!(!s.in_transaction());
        engine
            .execute_with_session(&mut s, "SELECT * FROM session_commit")
            .unwrap();
        assert!(
            s.in_transaction(),
            "a lazy read transaction must set IN_TRANS"
        );
        engine.execute_with_session(&mut s, "COMMIT").unwrap();
        assert!(!s.in_transaction());
        engine
            .execute_with_session(&mut s, "INSERT INTO session_commit VALUES(1)")
            .unwrap();
        assert!(s.in_transaction());
        assert!(engine
            .execute_with_session(&mut s, "SET autocommit=1, time_zone='Not/AZone'")
            .is_err());
        assert!(!s.autocommit);
        engine.execute_with_session(&mut s, sql).unwrap();
        assert!(!s.in_transaction(), "{sql}");
        let result = engine
            .execute_with_session(&mut other, "SELECT * FROM session_commit")
            .unwrap();
        if let QueryResponse::ResultSet(r) = result {
            assert!(!r.rows.is_empty());
        } else {
            panic!("expected result set");
        }
        engine
            .execute_with_session(&mut other, "DELETE FROM session_commit")
            .unwrap();
    }
    engine
        .execute_with_session(&mut s, "START TRANSACTION")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET transaction_isolation='SERIALIZABLE'")
        .unwrap();
    engine
        .execute_with_session(&mut s, "SET SESSION transaction_isolation='READ-COMMITTED'")
        .unwrap();
    assert!(engine
        .execute_with_session(&mut s, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .is_err());
    engine.execute_with_session(&mut s, "ROLLBACK").unwrap();
    engine
        .execute_with_session(&mut s, "XA START 'session_audit_xa'")
        .unwrap();
    for sql in [
        "SET autocommit=1, wait_timeout=100",
        "SET autocommit=DEFAULT",
    ] {
        assert!(engine.execute_with_session(&mut s, sql).is_err(), "{sql}");
    }
    engine
        .execute_with_session(&mut s, "XA END 'session_audit_xa'")
        .unwrap();
    engine
        .execute_with_session(&mut s, "XA ROLLBACK 'session_audit_xa'")
        .unwrap();
}

#[test]
fn session_audit_persist_failure_keeps_runtime_value() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    let original = crate::engine::sysvar::builtin::connection::connect_timeout_secs();
    // A directory at the atomic-write temporary path deterministically fails
    // before the live global value can be changed (no disk-full dependency).
    let blocked = engine
        .db
        .persisted_variables()
        .file_path()
        .with_extension("cnf.tmp");
    std::fs::create_dir_all(&blocked).unwrap();
    assert!(engine
        .execute_with_session(&mut s, "SET PERSIST connect_timeout=123")
        .is_err());
    assert_eq!(
        crate::engine::sysvar::builtin::connection::connect_timeout_secs(),
        original
    );
    assert!(engine
        .db
        .persisted_variables()
        .get("connect_timeout")
        .is_none());
    assert!(engine
        .execute_with_session(&mut s, "SET PERSIST_ONLY version='fake'")
        .is_err());
    assert!(engine.db.persisted_variables().get("version").is_none());
}

#[test]
fn session_audit_streaming_insert_id_and_routine_restoration() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(&mut s, "CREATE TABLE streaming_ids(v BIGINT UNSIGNED)")
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO streaming_ids VALUES(2),(0),(8)")
        .unwrap();
    let result = engine
        .execute_with_session(
            &mut s,
            "SELECT LAST_INSERT_ID(v),LAST_INSERT_ID() FROM streaming_ids",
        )
        .unwrap();
    if let QueryResponse::ResultSet(r) = result {
        assert_eq!(
            r.rows,
            vec![
                vec![Value::UInt(2), Value::Int(2)],
                vec![Value::UInt(0), Value::Int(0)],
                vec![Value::UInt(8), Value::Int(8)]
            ]
        );
    } else {
        panic!("expected result set");
    }
    assert_eq!(
        s.last_insert_id.load(std::sync::atomic::Ordering::Relaxed),
        8
    );
    engine
        .execute_with_session(
            &mut s,
            "CREATE FUNCTION audit_insert_id() RETURNS BIGINT RETURN LAST_INSERT_ID(77)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut s, "SELECT audit_insert_id()")
        .unwrap();
    assert_eq!(
        s.last_insert_id.load(std::sync::atomic::Ordering::Relaxed),
        8,
        "stored functions restore caller's insert id"
    );
    engine
        .execute_with_session(&mut s, "SELECT LAST_INSERT_ID(NULL)")
        .unwrap();
    assert_eq!(
        s.last_insert_id.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[test]
fn session_audit_function_insert_id_does_not_override_outer_auto_increment() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for sql in [
        "CREATE TABLE function_ids(id INT AUTO_INCREMENT PRIMARY KEY, v INT)",
        "CREATE FUNCTION function_insert_id() RETURNS BIGINT RETURN LAST_INSERT_ID(77)",
        "INSERT INTO function_ids(v) VALUES(function_insert_id())",
    ] {
        engine.execute_with_session(&mut s, sql).unwrap();
    }
    assert_eq!(
        s.last_insert_id.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(s.statement_insert_id, 1);
}

#[test]
fn session_audit_compiled_batch_keeps_each_update_info() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    engine
        .execute_with_session(
            &mut s,
            "CREATE TABLE batch_ids(id INT AUTO_INCREMENT PRIMARY KEY,v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut s, "INSERT INTO batch_ids(v) VALUES(7)")
        .unwrap();
    for _ in 0..3 {
        let (results, outcome) = crate::engine::api::QueryEngine::execute_text_dml_batch(
            &engine, &mut s,
            "INSERT INTO batch_ids(v) VALUES(8);INSERT INTO batch_ids(v) VALUES(9);UPDATE batch_ids SET v=v WHERE id=1",
        ).expect("compiled mixed DML batch");
        outcome.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[1].insert_id, results[0].insert_id + 1);
        assert_eq!(results[2].insert_id, 0);
        assert_eq!(results[2].matched_rows, Some(1));
        assert_eq!(results[2].info, "Rows matched: 1 Changed: 0 Warnings: 0");
    }
}
