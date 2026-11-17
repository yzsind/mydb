use super::*;

const A: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const B: &str = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee";

fn fixture() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::tempdir_in("/tmp/mydb").unwrap()
}

fn scalar(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> String {
    let QueryResponse::ResultSet(rows) = engine.execute_with_session(session, sql).unwrap() else {
        panic!("expected rows: {sql}");
    };
    rows.rows[0][0].to_display_string()
}

#[test]
fn dump_gtid_import_replacement_append_constraints_and_restart_without_binlog() {
    let dir = fixture();
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    let mut root = SessionContext::default();
    engine
        .execute_with_session(
            &mut root,
            &format!("SET @@GLOBAL.GTID_PURGED='{A}:1-3:4-5'"),
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut root,
            &format!("SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '{B}:1-2'"),
        )
        .unwrap();
    let expected = format!("{A}:1-5,{B}:1-2");
    for name in ["gtid_executed", "gtid_purged"] {
        assert_eq!(
            scalar(&engine, &mut root, &format!("SELECT @@GLOBAL.{name}")),
            expected
        );
    }
    assert_eq!(
        scalar(
            &engine,
            &mut root,
            "SELECT COUNT(*) FROM mysql.gtid_executed"
        ),
        "2"
    );
    for value in [format!("+{A}:5-6"), format!("{A}:1-4"), String::new()] {
        let err = engine
            .execute_with_session(&mut root, &format!("SET GLOBAL gtid_purged='{value}'"))
            .unwrap_err();
        assert_eq!(err.code(), 3546, "{value}: {err:?}");
        assert_eq!(
            scalar(&engine, &mut root, "SELECT @@GLOBAL.gtid_purged"),
            expected
        );
    }
    drop(engine);
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    assert_eq!(
        scalar(&engine, &mut root, "SELECT @@GLOBAL.gtid_purged"),
        expected
    );
    engine
        .execute_with_session(
            &mut root,
            &format!("SET GLOBAL gtid_purged='{A}:1-8,{B}:1-2'"),
        )
        .unwrap();
}

#[test]
fn dump_gtid_invalid_sets_scope_type_and_permissions_leave_history_unchanged() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for value in [
        "invalid".to_owned(),
        format!("{A}:0"),
        format!("{A}:9223372036854775808"),
        format!("{A}:1::2"),
    ] {
        let err = engine
            .execute_with_session(&mut root, &format!("SET GLOBAL gtid_purged='{value}'"))
            .unwrap_err();
        assert_eq!(err.code(), 1231, "{value}: {err:?}");
    }
    for (sql, code) in [
        ("SET SESSION gtid_purged=''", 1229),
        ("SET GLOBAL gtid_executed=''", 1238),
        ("SET GLOBAL gtid_purged=1", 1232),
        ("SET GLOBAL gtid_purged=NULL", 1231),
    ] {
        assert_eq!(
            engine
                .execute_with_session(&mut root, sql)
                .unwrap_err()
                .code(),
            code,
            "{sql}"
        );
    }
    engine
        .execute_with_session(
            &mut root,
            "CREATE USER 'dump_unpriv'@'%' IDENTIFIED BY 'pass'",
        )
        .unwrap();
    let mut user = SessionContext::default();
    user.user = "dump_unpriv".into();
    user.login_user = "dump_unpriv".into();
    user.authenticated_as = crate::engine::auth::AccountKey::new("dump_unpriv", "%");
    user.auth_epoch = engine.db.auth_catalog().epoch;
    let err = engine
        .execute_with_session(&mut user, &format!("SET GLOBAL gtid_purged='{A}:1'"))
        .unwrap_err();
    assert_eq!(err.code(), 1227);
    assert_eq!(
        scalar(&engine, &mut root, "SELECT @@GLOBAL.gtid_purged"),
        ""
    );
}

#[test]
fn dump_lock_all_mysql_virtual_tables_and_keep_missing_hidden_errors() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    root.current_database = "mysql".into();
    let names = crate::engine::mysql_schema::MYSQL_DEFS
        .iter()
        .map(|def| format!("`{}` READ LOCAL", def.name))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute_with_session(&mut root, &format!("LOCK TABLES {names}"))
        .unwrap();
    assert_eq!(
        scalar(&engine, &mut root, "SELECT COUNT(*) FROM columns_priv"),
        "0"
    );
    engine
        .execute_with_session(&mut root, "UNLOCK TABLES")
        .unwrap();
    for (sql, code) in [
        ("LOCK TABLES mysql.missing_dump_table READ", 1146),
        ("LOCK TABLES mysql.tables READ", 3554),
    ] {
        assert_eq!(
            engine
                .execute_with_session(&mut root, sql)
                .unwrap_err()
                .code(),
            code
        );
    }
    engine
        .execute_with_session(&mut root, "LOCK TABLES mysql.columns_priv AS cp READ")
        .unwrap();
    engine
        .execute_with_session(&mut root, "SELECT * FROM mysql.columns_priv AS cp")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut root, "SELECT * FROM mysql.columns_priv")
            .unwrap_err()
            .code(),
        1100
    );
    engine
        .execute_with_session(&mut root, "UNLOCK TABLES")
        .unwrap();
}

#[test]
fn dump_mysql_read_lock_blocks_auth_mutation_and_unlock_releases_it() {
    use std::time::Duration;
    let dir = fixture();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    let mut owner = SessionContext::default();
    owner.connection_id = 71;
    engine
        .execute_with_session(&mut owner, "LOCK TABLES mysql.columns_priv READ")
        .unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = engine.clone();
    let handle = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 72;
        started_tx.send(()).unwrap();
        done_tx
            .send(worker.execute_with_session(
                &mut session,
                "CREATE USER 'dump_waiter'@'%' IDENTIFIED BY 'pass'",
            ))
            .unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());
    assert_eq!(
        scalar(
            &engine,
            &mut owner,
            "SELECT COUNT(*) FROM mysql.columns_priv"
        ),
        "0"
    );
    assert_eq!(
        engine
            .execute_with_session(
                &mut owner,
                "CREATE USER 'dump_self'@'%' IDENTIFIED BY 'pass'"
            )
            .unwrap_err()
            .code(),
        1192
    );
    engine
        .execute_with_session(&mut owner, "UNLOCK TABLES")
        .unwrap();
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    handle.join().unwrap();
    assert_eq!(
        scalar(
            &engine,
            &mut owner,
            "SELECT COUNT(*) FROM mysql.user WHERE User='dump_waiter'"
        ),
        "1"
    );
}

#[test]
fn dump_mysql_write_lock_blocks_other_readers_and_system_dml_still_errors() {
    use std::time::Duration;
    let dir = fixture();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    let mut owner = SessionContext::default();
    owner.connection_id = 81;
    engine
        .execute_with_session(&mut owner, "LOCK TABLES mysql.user WRITE")
        .unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = engine.clone();
    let handle = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 82;
        tx.send(worker.execute_with_session(&mut session, "SELECT COUNT(*) FROM mysql.user"))
            .unwrap();
    });
    assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
    assert!(engine
        .execute_with_session(&mut owner, "DELETE FROM mysql.user")
        .is_err());
    engine
        .execute_with_session(&mut owner, "UNLOCK TABLES")
        .unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
    handle.join().unwrap();
}

#[test]
fn dump_gtid_import_fails_without_waiting_for_an_inflight_replica_batch() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    let gate = crate::storage::capture::logical_apply_gate(&engine.db).unwrap();
    let _batch = gate.read_arc();
    let mut root = SessionContext::default();
    let err = engine
        .execute_with_session(&mut root, &format!("SET GLOBAL gtid_purged='{A}:1-10'"))
        .unwrap_err();
    assert!(err.is(crate::error::catalog::ER_CANT_SET_GTID_PURGED_WHEN_OWNED_GTIDS_IS_NOT_EMPTY));
    assert_eq!(
        scalar(&engine, &mut root, "SELECT @@GLOBAL.gtid_purged"),
        ""
    );
}

#[test]
fn dump_gtid_mysql_set_syntax_normalizes_without_dropping_valid_intervals() {
    let dir = fixture();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for value in [A.to_owned(), format!("{A}:3-1"), " , ".into(), "+".into()] {
        engine
            .execute_with_session(&mut root, &format!("SET GLOBAL gtid_purged='{value}'"))
            .unwrap();
        assert_eq!(
            scalar(&engine, &mut root, "SELECT @@GLOBAL.gtid_purged"),
            ""
        );
    }
    engine
        .execute_with_session(
            &mut root,
            &format!(
                "SET GLOBAL gtid_purged=', {} : +1 - +3 : 5 ,,'",
                A.to_ascii_uppercase()
            ),
        )
        .unwrap();
    assert_eq!(
        scalar(&engine, &mut root, "SELECT @@GLOBAL.gtid_purged"),
        format!("{A}:1-3:5")
    );
}

#[test]
fn dump_gtid_physical_wal_apply_refreshes_imported_history_without_restart() {
    let source_dir = fixture();
    let source = open_strict_test_engine(source_dir.path());
    source.checkpoint().unwrap();
    let baseline = source.db.wal_log().lock().last_lsn();
    let replica_dir = fixture();
    crate::util::file::copy_fixture_tree(source_dir.path(), replica_dir.path()).unwrap();
    let replica = open_strict_test_engine(replica_dir.path());
    let mut root = SessionContext::default();
    assert_eq!(
        scalar(&replica, &mut root, "SELECT @@GLOBAL.gtid_purged"),
        ""
    );
    source
        .execute_with_session(&mut root, &format!("SET GLOBAL gtid_purged='{A}:1-10'"))
        .unwrap();
    source.db.wal_log().lock().flush().unwrap();
    let scan = crate::wal::LogManager::scan(source_dir.path()).unwrap();
    for record in scan.records.iter().filter(|r| r.lsn > baseline) {
        replica.db.apply_wal_record(record).unwrap();
    }
    assert_eq!(
        scalar(&replica, &mut root, "SELECT @@GLOBAL.gtid_purged"),
        format!("{A}:1-10")
    );
    assert_eq!(
        scalar(&replica, &mut root, "SELECT @@GLOBAL.gtid_executed"),
        format!("{A}:1-10")
    );
}

#[test]
fn dump_privilege_read_lock_also_blocks_automatic_routine_grants() {
    use std::time::Duration;
    let dir = fixture();
    let engine = std::sync::Arc::new(open_test_engine(dir.path()));
    let mut owner = SessionContext::default();
    owner.connection_id = 91;
    engine
        .execute_with_session(&mut owner, "LOCK TABLES mysql.procs_priv READ")
        .unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = engine.clone();
    let handle = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 92;
        tx.send(worker.execute_with_session(
            &mut session,
            "CREATE FUNCTION dump_locked_fn() RETURNS INT DETERMINISTIC RETURN 1",
        ))
        .unwrap();
    });
    assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
    engine
        .execute_with_session(&mut owner, "UNLOCK TABLES")
        .unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
    handle.join().unwrap();
    assert_eq!(scalar(&engine, &mut owner, "SELECT dump_locked_fn()"), "1");
}
