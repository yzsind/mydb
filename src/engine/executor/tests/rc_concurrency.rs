use super::*;

fn session() -> SessionContext {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(100);
    SessionContext {
        connection_id: NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        ..short_lock_wait_session()
    }
}

fn fixture() -> (tempfile::TempDir, Arc<StorageQueryEngine>) {
    fixture_with_durability(crate::storage::DurabilityMode::Memory)
}

fn fixture_with_durability(
    durability: crate::storage::DurabilityMode,
) -> (tempfile::TempDir, Arc<StorageQueryEngine>) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let mut config = memory_test_storage_config();
    config.lock_wait = Duration::from_millis(80);
    config.durability = durability;
    let engine = Arc::new(
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap(),
    );
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT UNIQUE)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 1), (2, 2)")
        .unwrap();
    (dir, engine)
}
fn sql(e: &StorageQueryEngine, s: &mut SessionContext, q: &str) {
    e.execute_with_session(s, q).unwrap();
}
fn rows(e: &StorageQueryEngine, s: &mut SessionContext, q: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(r) = e.execute_with_session(s, q).unwrap() else {
        panic!("result set")
    };
    r.rows
}

#[test]
fn rc_timeout_preserves_prior_writes_and_rolls_back_partial_statement() {
    let (_dir, e) = fixture();
    let mut a = session();
    let mut b = session();
    sql(&e, &mut b, "BEGIN");
    sql(&e, &mut b, "UPDATE t SET v=20 WHERE id=2");
    sql(&e, &mut a, "BEGIN");
    sql(&e, &mut a, "INSERT INTO t VALUES (10,10)");
    let txn = a.current_txn;
    assert_eq!(
        e.execute_with_session(&mut a, "UPDATE t SET v=v+100 WHERE id<=2")
            .unwrap_err()
            .code(),
        1205
    );
    assert_eq!(a.current_txn, txn);
    assert_eq!(
        rows(&e, &mut a, "SELECT id,v FROM t ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Int(2)],
            vec![Value::Int(10), Value::Int(10)]
        ]
    );
    sql(&e, &mut a, "INSERT INTO t VALUES (11,11)");
    sql(&e, &mut a, "COMMIT");
    sql(&e, &mut b, "ROLLBACK");
    assert_eq!(
        rows(&e, &mut a, "SELECT COUNT(*) FROM t")[0][0],
        Value::Int(4)
    );
}

#[test]
fn rc_committed_unique_conflict_is_duplicate_not_deadlock() {
    for conflict in ["(30,302)", "(32,301)"] {
        let (_dir, e) = fixture();
        let mut a = session();
        let mut b = session();
        sql(&e, &mut a, "BEGIN");
        sql(&e, &mut a, "INSERT INTO t VALUES (31,310)");
        sql(&e, &mut b, "INSERT INTO t VALUES (30,301)");
        assert_eq!(
            e.execute_with_session(&mut a, &format!("INSERT INTO t VALUES {conflict}"))
                .unwrap_err()
                .code(),
            1062
        );
        sql(&e, &mut a, "COMMIT");
        assert_eq!(
            rows(&e, &mut a, "SELECT COUNT(*) FROM t WHERE id=31")[0][0],
            Value::Int(1)
        );
    }
}

#[test]
fn rc_drop_database_and_rename_wait_for_readers_and_writers() {
    for ddl in [
        "DROP DATABASE audit_rc",
        "RENAME TABLE audit_rc.t TO audit_rc.u",
        "ALTER TABLE audit_rc.t RENAME TO audit_rc.u",
    ] {
        for write in [false, true] {
            let (_dir, e) = fixture();
            let mut a = session();
            let mut b = session();
            sql(&e, &mut a, "CREATE DATABASE audit_rc");
            sql(&e, &mut a, "CREATE TABLE audit_rc.t (id INT PRIMARY KEY)");
            sql(&e, &mut a, "BEGIN");
            sql(
                &e,
                &mut a,
                if write {
                    "INSERT INTO audit_rc.t VALUES (1)"
                } else {
                    "SELECT * FROM audit_rc.t"
                },
            );
            let started = Instant::now();
            assert_eq!(
                e.execute_with_session(&mut b, ddl).unwrap_err().code(),
                1205
            );
            assert!(started.elapsed() >= Duration::from_millis(60));
            sql(&e, &mut a, "SELECT * FROM audit_rc.t");
            sql(&e, &mut a, "COMMIT");
            sql(&e, &mut b, ddl);
        }
    }
}

#[test]
fn rc_ftwrl_disconnect_and_multiple_owners() {
    let (_dir, e) = fixture();
    let mut a = session();
    let mut b = session();
    let mut c = session();
    sql(&e, &mut a, "FLUSH TABLES WITH READ LOCK");
    sql(&e, &mut b, "FLUSH TABLES WITH READ LOCK");
    e.close_session(&mut b).unwrap();
    assert_eq!(
        e.execute_with_session(&mut c, "INSERT INTO t VALUES (3,3)")
            .unwrap_err()
            .code(),
        1205
    );
    e.close_session(&mut a).unwrap();
    sql(&e, &mut c, "INSERT INTO t VALUES (3,3)");
    assert!(e.db.metadata_lock_snapshot().is_empty());
}

#[test]
fn rc_ftwrl_waits_for_write_transaction() {
    let (_dir, e) = fixture();
    let mut a = session();
    let mut b = session();
    sql(&e, &mut a, "BEGIN");
    sql(&e, &mut a, "INSERT INTO t VALUES (3,3)");
    let result = e.execute_with_session(&mut b, "FLUSH TABLES WITH READ LOCK");
    // Always clean up the lock, including on the unfixed implementation.
    sql(&e, &mut b, "UNLOCK TABLES");
    assert_eq!(result.unwrap_err().code(), 1205);
    sql(&e, &mut a, "COMMIT");
    sql(&e, &mut b, "FLUSH TABLES WITH READ LOCK");
    assert_eq!(
        e.execute_with_session(&mut a, "INSERT INTO t VALUES (4,4)")
            .unwrap_err()
            .code(),
        1205
    );
    sql(&e, &mut b, "UNLOCK TABLES");
    sql(&e, &mut a, "INSERT INTO t VALUES (4,4)");
}

#[test]
fn rc_mdl_uses_session_timeout_independently_of_row_timeout() {
    let (_dir, e) = fixture();
    let mut a = session();
    let mut b = session();
    sql(&e, &mut a, "LOCK TABLES t WRITE");
    sql(&e, &mut b, "SET SESSION innodb_lock_wait_timeout=3");
    sql(&e, &mut b, "SET SESSION lock_wait_timeout=1");
    let started = Instant::now();
    assert_eq!(
        e.execute_with_session(&mut b, "SELECT * FROM t")
            .unwrap_err()
            .code(),
        1205
    );
    let elapsed = started.elapsed();
    sql(&e, &mut a, "UNLOCK TABLES");
    assert!(
        elapsed >= Duration::from_millis(900) && elapsed < Duration::from_secs(3),
        "{elapsed:?}"
    );
}

fn wait_pending(e: &StorageQueryEngine, owner: u32) {
    let started = Instant::now();
    while !e
        .db
        .metadata_lock_snapshot()
        .iter()
        .any(|r| r.owner_thread_id == Some(owner as u64) && r.lock_status == "PENDING")
    {
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "no pending MDL for {owner}"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn rc_mdl_deadlock_releases_lazy_read_transaction() {
    let (_dir, e) = fixture();
    let mut a = session();
    let mut b = session();
    sql(&e, &mut a, "SET SESSION lock_wait_timeout=2");
    sql(&e, &mut b, "SET SESSION lock_wait_timeout=2");
    sql(&e, &mut a, "BEGIN");
    sql(&e, &mut b, "BEGIN");
    sql(&e, &mut a, "SELECT * FROM t");
    sql(&e, &mut b, "SELECT * FROM t");
    assert!(a.current_txn.is_none() && b.current_txn.is_none());
    let owner = b.connection_id;
    let worker = Arc::clone(&e);
    let handle = std::thread::spawn(move || {
        let result = worker.execute_with_session(&mut b, "ALTER TABLE t DROP COLUMN v");
        worker.close_session(&mut b).unwrap();
        result
    });
    wait_pending(&e, owner);
    let started = Instant::now();
    let result = e.execute_with_session(&mut a, "ALTER TABLE t RENAME COLUMN v TO renamed_v");
    let other = handle.join().unwrap();
    assert_eq!(result.unwrap_err().code(), 1213);
    assert!(started.elapsed() < Duration::from_secs(1));
    other.unwrap();
    // Like MySQL, the deadlock rolls the transaction back but keeps the
    // session inside BEGIN: the next statement opens a new transaction.
    assert!(a.explicit_txn && a.current_txn.is_none());
    assert!(e.db.metadata_lock_snapshot().is_empty());
}

#[test]
fn rc_repeated_lock_tables_releases_old_locks_like_mysql() {
    let (_dir, e) = fixture();
    let mut a = session();
    let mut b = session();
    let mut c = session();
    sql(&e, &mut b, "CREATE TABLE u (id INT)");
    sql(&e, &mut a, "SET SESSION lock_wait_timeout=2");
    sql(&e, &mut a, "LOCK TABLES t WRITE");
    sql(&e, &mut b, "LOCK TABLES u READ");
    let owner = a.connection_id;
    let worker = Arc::clone(&e);
    let handle = std::thread::spawn(move || {
        let result = worker.execute_with_session(&mut a, "LOCK TABLES t WRITE, u WRITE");
        worker.close_session(&mut a).unwrap();
        result
    });
    wait_pending(&e, owner);
    sql(&e, &mut c, "UPDATE t SET v=3 WHERE id=1");
    sql(&e, &mut b, "UNLOCK TABLES");
    handle.join().unwrap().unwrap();
}

#[test]
fn rc_ftwrl_survives_begin_and_is_scoped_to_database_instance() {
    let (_dir, e) = fixture();
    let (_other_dir, other) = fixture();
    let mut a = session();
    let mut b = session();
    sql(&e, &mut a, "FLUSH TABLES WITH READ LOCK");
    sql(&e, &mut a, "BEGIN");
    assert!(e
        .db
        .metadata_lock_snapshot()
        .iter()
        .any(|r| r.object_type == "GLOBAL" && r.lock_duration == "EXPLICIT"));
    sql(&e, &mut b, "UNLOCK TABLES");
    assert_eq!(
        e.execute_with_session(&mut b, "INSERT INTO t VALUES (3,3)")
            .unwrap_err()
            .code(),
        1205
    );
    sql(&other, &mut b, "INSERT INTO t VALUES (3,3)");
    e.reset_session(&mut a).unwrap();
    sql(&e, &mut b, "INSERT INTO t VALUES (3,3)");
}

#[test]
fn rc_drop_rechecks_tables_created_while_waiting() {
    let (_dir, e) = fixture();
    let mut a = session();
    let mut b = session();
    let mut c = session();
    sql(&e, &mut a, "CREATE DATABASE audit_create");
    sql(&e, &mut a, "CREATE TABLE audit_create.t (id INT)");
    sql(&e, &mut a, "BEGIN");
    sql(&e, &mut a, "SELECT * FROM audit_create.t");
    sql(&e, &mut b, "SET SESSION lock_wait_timeout=2");
    let owner = b.connection_id;
    let worker = Arc::clone(&e);
    let handle = std::thread::spawn(move || {
        worker.execute_with_session(&mut b, "DROP DATABASE audit_create")
    });
    wait_pending(&e, owner);
    sql(&e, &mut c, "CREATE TABLE audit_create.u (id INT)");
    sql(&e, &mut c, "BEGIN");
    sql(&e, &mut c, "INSERT INTO audit_create.u VALUES (7)");
    sql(&e, &mut a, "COMMIT");
    let start = Instant::now();
    while !e.db.metadata_lock_snapshot().iter().any(|r| {
        r.owner_thread_id == Some(owner as u64)
            && r.object_name.as_deref() == Some("u")
            && r.lock_status == "PENDING"
    }) {
        assert!(start.elapsed() < Duration::from_secs(1));
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(
        rows(&e, &mut c, "SELECT * FROM audit_create.u"),
        vec![vec![Value::Int(7)]]
    );
    sql(&e, &mut c, "COMMIT");
    handle.join().unwrap().unwrap();
    assert!(!e.db.database_exists("audit_create"));
}

#[test]
fn rc_rename_locks_destination_in_other_schema() {
    // File-moving DDL needs a durable fixture, like mysql80_compat_rename_table.
    let (_dir, e) = fixture_with_durability(crate::storage::DurabilityMode::Strict);
    let mut a = session();
    let mut b = session();
    sql(&e, &mut a, "CREATE DATABASE audit_dest");
    sql(&e, &mut a, "CREATE TABLE audit_dest.u (id INT)");
    sql(&e, &mut a, "BEGIN");
    sql(&e, &mut a, "SELECT * FROM audit_dest.u");
    assert_eq!(
        e.execute_with_session(&mut b, "RENAME TABLE t TO audit_dest.u")
            .unwrap_err()
            .code(),
        1205
    );
    sql(&e, &mut a, "COMMIT");
    assert_eq!(
        e.execute_with_session(&mut b, "RENAME TABLE t TO audit_dest.u")
            .unwrap_err()
            .code(),
        1050
    );
    sql(&e, &mut b, "DROP TABLE audit_dest.u");
    sql(&e, &mut b, "RENAME TABLE t TO audit_dest.u");
    assert_eq!(
        rows(&e, &mut b, "SELECT COUNT(*) FROM audit_dest.u")[0][0],
        Value::Int(2)
    );
}

#[test]
fn rc_ftwrl_waits_for_prepared_xa_and_global_mdl_is_released() {
    let (_dir, e) = fixture();
    let mut a = session();
    let mut b = session();
    sql(&e, &mut a, "XA START 'audit_rc'");
    sql(&e, &mut a, "INSERT INTO t VALUES (3,3)");
    sql(&e, &mut a, "XA END 'audit_rc'");
    sql(&e, &mut a, "XA PREPARE 'audit_rc'");
    assert_eq!(
        e.execute_with_session(&mut b, "FLUSH TABLES WITH READ LOCK")
            .unwrap_err()
            .code(),
        1205
    );
    sql(&e, &mut a, "XA COMMIT 'audit_rc'");
    sql(&e, &mut b, "FLUSH TABLES WITH READ LOCK");
    e.close_session(&mut b).unwrap();
    assert!(e.db.metadata_lock_snapshot().is_empty());
}

#[test]
fn rc_prepared_execution_timeout_retains_transaction() {
    let (_dir, e) = fixture();
    let mut a = session();
    let mut b = session();
    sql(
        &e,
        &mut a,
        "PREPARE upd FROM 'UPDATE t SET v=99 WHERE id=2'",
    );
    sql(&e, &mut a, "BEGIN");
    sql(&e, &mut a, "INSERT INTO t VALUES (3,3)");
    sql(&e, &mut b, "BEGIN");
    sql(&e, &mut b, "UPDATE t SET v=20 WHERE id=2");
    assert_eq!(
        e.execute_with_session(&mut a, "EXECUTE upd")
            .unwrap_err()
            .code(),
        1205
    );
    sql(&e, &mut a, "COMMIT");
    sql(&e, &mut b, "ROLLBACK");
    assert_eq!(
        rows(&e, &mut a, "SELECT COUNT(*) FROM t WHERE id=3")[0][0],
        Value::Int(1)
    );
}

#[test]
fn rc_mdl_global_timeout_fallback_and_session_override() {
    let (_dir, e) = fixture();
    let mut a = session();
    let mut b = session();
    let old = crate::engine::api::configured_global_lock_wait_timeout();
    sql(&e, &mut a, "SET GLOBAL lock_wait_timeout=1");
    sql(&e, &mut a, "LOCK TABLES t WRITE");
    let started = Instant::now();
    let result = e.execute_with_session(&mut b, "SELECT * FROM t");
    let elapsed = started.elapsed();
    sql(&e, &mut a, "UNLOCK TABLES");
    crate::engine::api::set_global_lock_wait_timeout(old.unwrap_or(0));
    assert_eq!(result.unwrap_err().code(), 1205);
    assert!(elapsed >= Duration::from_millis(900));
    assert!(e.db.metadata_lock_snapshot().is_empty());
}

#[test]
fn rc_drop_revalidation_wait_uses_metadata_timeout() {
    let (_dir, e) = fixture();
    let mut a = session();
    let mut b = session();
    sql(&e, &mut a, "CREATE DATABASE audit_late");
    sql(&e, &mut a, "CREATE TABLE audit_late.t (id INT)");
    sql(&e, &mut a, "BEGIN");
    sql(&e, &mut a, "SELECT * FROM audit_late.t");
    sql(&e, &mut b, "SET SESSION lock_wait_timeout=1");
    // Model an object discovered only in the second catalog snapshot, after
    // the initial schema X grant. Ordinary dispatch performs this same
    // second-stage acquisition before it mutates the catalog.
    e.db.acquire_mdl(
        b.connection_id as u64,
        Some(b.connection_id as u64),
        MdlDuration::Statement,
        vec![crate::storage::MdlRequest {
            key: crate::storage::MdlObjectKey::schema("audit_late"),
            lock_type: crate::storage::MdlLockType::Exclusive,
        }],
        "test/drop-revalidation",
    )
    .unwrap();
    let started = Instant::now();
    let result = super::super::dispatch::execute_qualified_statement(
        &e.db,
        &mut b,
        parse_statement("DROP DATABASE audit_late").unwrap(),
        &e.registry,
    );
    let elapsed = started.elapsed();
    e.db.release_mdl_duration(b.connection_id as u64, MdlDuration::Statement);
    assert_eq!(result.unwrap_err().code(), 1205);
    assert!(elapsed >= Duration::from_millis(900));
    sql(&e, &mut a, "SELECT * FROM audit_late.t");
    sql(&e, &mut a, "COMMIT");
    sql(&e, &mut b, "DROP DATABASE audit_late");
}

#[test]
fn rc_audit_xa_start_preserves_local_transaction() {
    for begin in ["BEGIN", "SET autocommit=0"] {
        let (_dir, e) = fixture();
        let mut a = session();
        let mut b = session();
        sql(&e, &mut a, begin);
        sql(&e, &mut a, "INSERT INTO t VALUES (9,9)");
        let txn = a.current_txn;
        let err = e
            .execute_with_session(&mut a, "XA START 'audit'")
            .unwrap_err();
        assert_eq!(err.code(), 1400);
        assert_eq!(a.current_txn, txn);
        assert!(a.xa_branch.is_none());
        assert!(rows(&e, &mut b, "SELECT * FROM t WHERE id=9").is_empty());
        sql(&e, &mut a, "ROLLBACK");
        assert!(rows(&e, &mut b, "SELECT * FROM t WHERE id=9").is_empty());
    }
}

#[test]
fn rc_audit_lock_of_preserves_scope_and_rejects_ambiguous_targets() {
    let (_dir, e) = fixture();
    let mut a = session();
    for q in [
        "SELECT * FROM t FOR UPDATE OF t",
        "SELECT * FROM t FOR SHARE OF t",
        "SELECT * FROM t a JOIN t b ON a.id=b.id FOR UPDATE OF a NOWAIT",
        "SELECT * FROM (SELECT * FROM t FOR UPDATE OF t) q",
        "SELECT (SELECT id FROM t WHERE id=1 FOR SHARE OF t) AS n",
    ] {
        assert!(e.execute_with_session(&mut a, q).is_ok(), "{q}");
    }
    for q in [
        "SELECT * FROM t FOR UPDATE FOR SHARE",
        "SELECT * FROM t FOR UPDATE OF missing",
        "SELECT * FROM t a FOR SHARE OF t",
    ] {
        assert!(e.execute_with_session(&mut a, q).is_err(), "{q}");
    }
}

#[test]
fn rc_audit_ftwrl_waits_until_unlock() {
    let (_dir, e) = fixture();
    let mut owner = session();
    sql(&e, &mut owner, "FLUSH TABLES WITH READ LOCK");
    let (send, recv) = std::sync::mpsc::channel();
    let worker_engine = e.clone();
    let worker = std::thread::spawn(move || {
        let mut writer = session();
        writer.sysvars.lock_wait_timeout = Some(3);
        send.send(
            worker_engine.execute_with_session(&mut writer, "UPDATE t SET v=v+10 WHERE id=1"),
        )
        .unwrap();
    });
    let premature = recv.recv_timeout(Duration::from_millis(150));
    sql(&e, &mut owner, "UNLOCK TABLES");
    worker.join().unwrap();
    assert!(
        matches!(premature, Err(std::sync::mpsc::RecvTimeoutError::Timeout)),
        "{premature:?}"
    );
    recv.recv_timeout(Duration::from_secs(3)).unwrap().unwrap();
    assert_eq!(
        rows(&e, &mut owner, "SELECT v FROM t WHERE id=1"),
        vec![vec![Value::Int(11)]]
    );
}

fn registered_audit_session(e: &StorageQueryEngine) -> SessionContext {
    let mut s = session();
    let handle = e.registry().register(
        s.connection_id,
        "root".into(),
        "localhost".into(),
        "test".into(),
    );
    s.cancel_handle = Some(handle.cancel_handle());
    s.refresh_cancel_generations();
    s
}

#[test]
fn rc_audit_lazy_transaction_metadata_lifecycle() {
    for isolation in ["READ-COMMITTED", "REPEATABLE-READ"] {
        let (_dir, e) = fixture();
        let mut a = registered_audit_session(&e);
        let mut b = session();
        sql(
            &e,
            &mut a,
            &format!("SET transaction_isolation='{isolation}'"),
        );
        sql(&e, &mut a, "SET autocommit=0");
        let query = format!("SELECT trx_state, trx_rows_modified FROM information_schema.innodb_trx WHERE trx_mysql_thread_id={}", a.connection_id);
        assert!(rows(&e, &mut b, &query).is_empty());
        for end in ["COMMIT", "ROLLBACK"] {
            sql(&e, &mut a, "SELECT * FROM t");
            assert!(
                a.current_txn.is_none(),
                "monitoring must preserve lazy allocation"
            );
            assert_eq!(
                rows(&e, &mut b, &query),
                vec![vec![Value::String("RUNNING".into()), Value::Int(0)]]
            );
            assert_eq!(
                e.execute_with_session(&mut a, "XA START 'read'")
                    .unwrap_err()
                    .code(),
                1400
            );
            sql(&e, &mut a, end);
            assert!(rows(&e, &mut b, &query).is_empty());
        }
        sql(&e, &mut a, "SELECT * FROM t");
        sql(&e, &mut a, "UPDATE t SET v=8 WHERE id=1");
        assert_eq!(
            rows(&e, &mut b, &query).len(),
            1,
            "upgrading must not duplicate entries"
        );
        sql(&e, &mut a, "ROLLBACK");
        sql(&e, &mut a, "SELECT * FROM t");
        e.close_session(&mut a).unwrap();
        assert!(rows(&e, &mut b, &query).is_empty());
    }
}

#[test]
fn rc_audit_xa_start_allows_idle_autocommit_off() {
    let (_dir, e) = fixture();
    let mut a = session();
    sql(&e, &mut a, "SET autocommit=0");
    sql(&e, &mut a, "XA START 'idle'");
    sql(&e, &mut a, "XA END 'idle'");
    sql(&e, &mut a, "XA ROLLBACK 'idle'");
}

#[test]
fn rc_audit_timeout_parameter_bounds_are_strict() {
    let (_dir, e) = fixture();
    let mut a = session();
    for value in ["0", "-1", "1073741825"] {
        let err = e
            .execute_with_session(
                &mut a,
                &format!("SET SESSION innodb_lock_wait_timeout={value}"),
            )
            .unwrap_err();
        assert!(err.is(crate::error::catalog::ER_WRONG_VALUE_FOR_VAR));
    }
    for value in [1, 1073741824] {
        sql(
            &e,
            &mut a,
            &format!("SET SESSION innodb_lock_wait_timeout={value}"),
        );
        assert_eq!(a.sysvars.innodb_lock_wait_timeout, Some(value));
    }
}

#[test]
fn rc_audit_ftwrl_timeout_and_cancel_preserve_connection() {
    let (_dir, e) = fixture();
    let mut owner = session();
    let mut writer = registered_audit_session(&e);
    writer.sysvars.lock_wait_timeout = Some(1);
    sql(&e, &mut owner, "FLUSH TABLES WITH READ LOCK");
    let started = Instant::now();
    let err = e
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (9,9)")
        .unwrap_err();
    assert_eq!(err.code(), 1205);
    assert!(started.elapsed() >= Duration::from_millis(900));
    let id = writer.connection_id;
    writer.sysvars.lock_wait_timeout = Some(10);
    let worker_engine = e.clone();
    let worker = std::thread::spawn(move || {
        let result = worker_engine.execute_with_session(&mut writer, "INSERT INTO t VALUES (9,9)");
        (writer, result)
    });
    std::thread::sleep(Duration::from_millis(100));
    e.registry().kill_query(id).unwrap();
    let (mut writer, result) = worker.join().unwrap();
    assert_eq!(result.unwrap_err().code(), 1317);
    writer.refresh_cancel_generations();
    sql(&e, &mut owner, "UNLOCK TABLES");
    sql(&e, &mut writer, "INSERT INTO t VALUES (9,9)");
}

#[test]
fn rc_audit_ftwrl_prepared_waits_for_all_owners_and_disconnect() {
    let (_dir, e) = fixture();
    let mut a = session();
    let mut b = session();
    let mut writer = session();
    writer.sysvars.lock_wait_timeout = Some(5);
    let query = "UPDATE t SET v=v+? WHERE id=?";
    let parsed = e.prepare_cached(&mut writer, query, 2).unwrap();
    let mut state = crate::engine::api::PreparedDmlState::default();
    e.execute_prepared_dml(
        &mut writer,
        &mut state,
        &parsed,
        query,
        &[Value::Int(3), Value::Int(1)],
    )
    .unwrap()
    .unwrap();
    sql(&e, &mut a, "FLUSH TABLES WITH READ LOCK");
    sql(&e, &mut b, "FLUSH TABLES WITH READ LOCK");
    let (send, recv) = std::sync::mpsc::channel();
    let worker_engine = e.clone();
    let worker = std::thread::spawn(move || {
        send.send(worker_engine.execute_prepared_dml(
            &mut writer,
            &mut state,
            &parsed,
            query,
            &[Value::Int(10), Value::Int(1)],
        ))
        .unwrap();
    });
    let first = recv.recv_timeout(Duration::from_millis(100));
    sql(&e, &mut a, "UNLOCK TABLES");
    let second = recv.recv_timeout(Duration::from_millis(100));
    e.close_session(&mut b).unwrap();
    worker.join().unwrap();
    assert!(matches!(
        first,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    assert!(matches!(
        second,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    recv.recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        rows(&e, &mut a, "SELECT v FROM t WHERE id=1"),
        vec![vec![Value::Int(14)]]
    );
}

#[test]
fn select_first_write_transaction_enables_wal_batching_without_losing_savepoint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_relaxed_test_engine(dir.path());
    let mut session = session();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE promoted_wal (id INT PRIMARY KEY, v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO promoted_wal VALUES (1, 10)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET autocommit=0")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SELECT * FROM promoted_wal WHERE id=1")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SAVEPOINT before_write")
        .unwrap();
    let id = session.current_txn.unwrap();
    assert_eq!(engine.db.buffered_txn_wal_records_for_test(id), 0);
    engine
        .execute_with_session(&mut session, "UPDATE promoted_wal SET v=20 WHERE id=1")
        .unwrap();
    assert!(engine.db.buffered_txn_wal_records_for_test(id) > 0,
        "a read-first explicit transaction must use the same WAL batching as a write-first transaction");
    engine
        .execute_with_session(&mut session, "ROLLBACK TO SAVEPOINT before_write")
        .unwrap();
    engine
        .execute_with_session(&mut session, "UPDATE promoted_wal SET v=30 WHERE id=1")
        .unwrap();
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
    engine
        .execute_with_session(&mut session, "SET autocommit=1")
        .unwrap();
    let rows = engine
        .db
        .scan_rows(
            "test.promoted_wal",
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
        )
        .unwrap();
    assert_eq!(rows[0].values, vec![Value::Int(1), Value::Int(30)]);
}

#[test]
fn rc_deadlock_victim_stays_in_begin_so_rollback_undoes_later_writes() {
    let (_dir, e) = fixture();
    e.execute("INSERT INTO t VALUES (3, 3)").unwrap();
    let mut a = session();
    let mut b = session();
    for s in [&mut a, &mut b] {
        sql(&e, s, "SET SESSION innodb_lock_wait_timeout=5");
        sql(&e, s, "BEGIN");
    }
    sql(&e, &mut a, "UPDATE t SET v=v+10 WHERE id=1");
    sql(&e, &mut b, "UPDATE t SET v=v+10 WHERE id=2");
    let worker = Arc::clone(&e);
    let handle = std::thread::spawn(move || {
        let result = worker.execute_with_session(&mut a, "UPDATE t SET v=v+10 WHERE id=2");
        (a, result)
    });
    std::thread::sleep(Duration::from_millis(200));
    let victim = e.execute_with_session(&mut b, "UPDATE t SET v=v+10 WHERE id=1");
    let (mut a, waiter) = handle.join().unwrap();
    assert_eq!(victim.unwrap_err().code(), 1213);
    waiter.unwrap();
    sql(&e, &mut a, "ROLLBACK");
    // MySQL keeps the victim inside BEGIN: this write opens a new transaction
    // that the following ROLLBACK must undo instead of autocommitting it.
    sql(&e, &mut b, "UPDATE t SET v=100 WHERE id=3");
    sql(&e, &mut b, "ROLLBACK");
    let mut c = session();
    assert_eq!(
        rows(&e, &mut c, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Int(2)],
            vec![Value::Int(3), Value::Int(3)],
        ]
    );
}
