use super::*;

fn run(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> QueryResponse {
    engine
        .execute_with_session(session, sql)
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"))
}

fn reject(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str, code: u16) {
    let err = engine.execute_with_session(session, sql).err().expect(sql);
    assert_eq!(err.code(), code, "{sql}: {err:?}");
}

#[test]
fn transaction_audit_begin_resolves_new_access_and_chain_preserves_active_access() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    run(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    run(&engine, &mut s, "START TRANSACTION READ ONLY");
    run(&engine, &mut s, "COMMIT AND CHAIN");
    reject(&engine, &mut s, "INSERT INTO t VALUES(1)", 1792);
    run(&engine, &mut s, "START TRANSACTION");
    run(&engine, &mut s, "INSERT INTO t VALUES(1)");
    run(&engine, &mut s, "ROLLBACK");
    run(&engine, &mut s, "SET SESSION TRANSACTION READ ONLY");
    run(&engine, &mut s, "START TRANSACTION READ WRITE");
    run(&engine, &mut s, "START TRANSACTION");
    reject(&engine, &mut s, "INSERT INTO t VALUES(1)", 1792);
}

#[test]
fn transaction_audit_xa_consumes_one_shot_access_in_both_directions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    run(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    run(&engine, &mut s, "SET TRANSACTION READ ONLY");
    run(&engine, &mut s, "XA START 'ro'");
    reject(&engine, &mut s, "INSERT INTO t VALUES(1)", 1792);
    run(&engine, &mut s, "XA ROLLBACK 'ro'");
    run(&engine, &mut s, "SET SESSION TRANSACTION READ ONLY");
    run(&engine, &mut s, "SET TRANSACTION READ WRITE");
    run(&engine, &mut s, "XA START 'rw'");
    run(&engine, &mut s, "INSERT INTO t VALUES(1)");
    run(&engine, &mut s, "XA ROLLBACK 'rw'");
    reject(&engine, &mut s, "INSERT INTO t VALUES(1)", 1792);
}

#[test]
fn transaction_audit_consistent_snapshot_uses_next_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut a = SessionContext::default();
    let mut b = SessionContext::default();
    run(&engine, &mut a, "CREATE TABLE t(id INT PRIMARY KEY, v INT)");
    run(&engine, &mut a, "INSERT INTO t VALUES(1,10)");
    for (old, new, expected) in [
        ("READ COMMITTED", "REPEATABLE READ", 10),
        ("REPEATABLE READ", "READ COMMITTED", 99),
    ] {
        run(
            &engine,
            &mut a,
            &format!("SET SESSION TRANSACTION ISOLATION LEVEL {old}"),
        );
        run(&engine, &mut a, "BEGIN");
        run(
            &engine,
            &mut a,
            &format!("SET SESSION TRANSACTION ISOLATION LEVEL {new}"),
        );
        run(
            &engine,
            &mut a,
            "START TRANSACTION WITH CONSISTENT SNAPSHOT",
        );
        assert_eq!(a.txn_snapshot_guard.is_some(), new == "REPEATABLE READ");
        run(&engine, &mut b, "UPDATE t SET v=99 WHERE id=1");
        let QueryResponse::ResultSet(rs) = run(&engine, &mut a, "SELECT v FROM t WHERE id=1")
        else {
            panic!()
        };
        assert_eq!(rs.rows, vec![vec![Value::Int(expected)]]);
        run(&engine, &mut a, "ROLLBACK");
        run(&engine, &mut b, "UPDATE t SET v=10 WHERE id=1");
    }
}

#[test]
fn transaction_audit_rejected_write_preserves_pending_until_completion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    run(&engine, &mut s, "CREATE TABLE t(id INT PRIMARY KEY)");
    for autocommit in [false, true] {
        run(
            &engine,
            &mut s,
            if autocommit {
                "SET autocommit=1"
            } else {
                "SET autocommit=0"
            },
        );
        run(&engine, &mut s, "SET TRANSACTION READ ONLY");
        reject(&engine, &mut s, "INSERT INTO t VALUES(1)", 1792);
        assert!(s.current_txn.is_none());
        assert!(s.active_txn_isolation.is_none());
        assert_eq!(s.next_txn_read_only, Some(true));
        reject(&engine, &mut s, "INSERT INTO t VALUES(1)", 1792);
        run(&engine, &mut s, "SET TRANSACTION READ ONLY");
        run(&engine, &mut s, "ROLLBACK");
        assert!(s.next_txn_read_only.is_none());
        run(&engine, &mut s, "INSERT INTO t VALUES(1)");
        run(&engine, &mut s, "ROLLBACK");
        run(&engine, &mut s, "DELETE FROM t");
        run(&engine, &mut s, "COMMIT");
    }
}

#[test]
fn transaction_audit_autocommit_encoding_error_releases_owner_and_locks() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    crate::binlog::init_global(
        dir.path().into(),
        crate::binlog::BinlogConfig {
            enabled: true,
            ..Default::default()
        },
    )
    .unwrap();
    let mut a = SessionContext::default();
    let mut b = SessionContext::default();
    run(
        &engine,
        &mut a,
        "CREATE TABLE t(id INT PRIMARY KEY, ts TIMESTAMP)",
    );
    reject(
        &engine,
        &mut a,
        "INSERT INTO t VALUES(1,'2107-01-01 00:00:00')",
        1264,
    );
    assert!(a.current_txn.is_none());
    run(&engine, &mut b, "SET innodb_lock_wait_timeout=1");
    run(
        &engine,
        &mut b,
        "INSERT INTO t VALUES(1,'2020-01-01 00:00:00')",
    );
}

#[test]
fn transaction_audit_nowait_upgrade_finishes_before_blocker_releases() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(open_test_engine(dir.path()));
    let mut a = SessionContext::default();
    let mut b = SessionContext::default();
    run(&engine, &mut a, "CREATE TABLE t(id INT PRIMARY KEY)");
    run(&engine, &mut a, "INSERT INTO t VALUES(1)");
    for session in [&mut a, &mut b] {
        run(&engine, session, "BEGIN");
        run(&engine, session, "SELECT * FROM t WHERE id=1 FOR SHARE");
    }
    let (send, receive) = std::sync::mpsc::channel();
    let worker_engine = Arc::clone(&engine);
    let worker = std::thread::spawn(move || {
        let result = worker_engine
            .execute_with_session(&mut a, "SELECT * FROM t WHERE id=1 FOR UPDATE NOWAIT");
        send.send(result.err().map(|err| err.code())).unwrap();
        a
    });
    let result = receive.recv_timeout(std::time::Duration::from_secs(2));
    run(&engine, &mut b, "ROLLBACK");
    let mut a = worker.join().unwrap();
    assert_eq!(result.unwrap(), Some(3572));
    assert!(a.in_transaction());
    run(&engine, &mut a, "ROLLBACK");
}

#[test]
fn transaction_audit_empty_completion_resets_or_chains_one_shot_characteristics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    for ending in [
        "COMMIT",
        "ROLLBACK",
        "COMMIT AND CHAIN",
        "ROLLBACK AND CHAIN",
    ] {
        run(
            &engine,
            &mut s,
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
        );
        run(&engine, &mut s, ending);
        if ending.ends_with("CHAIN") {
            assert!(s.read_only_txn);
            assert_eq!(
                s.active_txn_isolation,
                Some(crate::storage::IsolationLevel::RepeatableRead)
            );
        } else {
            assert!(s.next_txn_isolation.is_none());
            assert!(s.next_txn_read_only.is_none());
            assert!(!s.in_transaction());
        }
        run(&engine, &mut s, "ROLLBACK");
    }
}
