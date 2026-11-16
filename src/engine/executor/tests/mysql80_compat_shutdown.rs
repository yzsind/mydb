//! MySQL 8.0 SHUTDOWN/RESTART compatibility:
//! docs/shutdown-mysql80-compat-design.md。
//!
//! Coverage:
//! Accounts without SHUTDOWN privilege are denied for either command;
//! global privilege denial uses privilege_denied and error 1142.
//! With MYDB_RESTART_DISABLE, RESTART returns 3707 without initiating shutdown.
//! Root SHUTDOWN/RESTART returns OK and sets the process-wide shutdown flag.
//! Direct engine execution does not re-exec; only the server graceful path does.

use super::*;
use crate::engine::auth::AccountKey;
use crate::mysql::shutdown;

fn alice_session(engine: &StorageQueryEngine) -> SessionContext {
    let mut s = SessionContext::default();
    s.user = "alice".to_string();
    s.login_user = "alice".to_string();
    s.authenticated_as = AccountKey::new("alice", "%");
    s.auth_epoch = engine.db.auth_catalog().epoch;
    s
}

#[test]
fn shutdown_denied_without_privilege() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    engine
        .execute_with_session(&mut root, "CREATE USER 'alice'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    let mut alice = alice_session(&engine);
    let err = engine
        .execute_with_session(&mut alice, "SHUTDOWN")
        .unwrap_err();
    assert_eq!(
        err.code(),
        1142,
        "SHUTDOWN without the SHUTDOWN privilege must be denied: {err:?}"
    );
}

#[test]
fn restart_denied_without_privilege() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    engine
        .execute_with_session(&mut root, "CREATE USER 'alice'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    let mut alice = alice_session(&engine);
    // Denial happens in authorize_statement, before the capability gate,
    // so this holds regardless of platform / MYDB_RESTART_DISABLE.
    let err = engine
        .execute_with_session(&mut alice, "RESTART")
        .unwrap_err();
    assert_eq!(
        err.code(),
        1142,
        "RESTART without the SHUTDOWN privilege must be denied: {err:?}"
    );
}

#[test]
fn restart_disabled_returns_3707_without_shutting_down() {
    std::env::set_var("MYDB_RESTART_DISABLE", "1");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    let was_shutting_down = shutdown::is_shutting_down();
    let err = engine
        .execute_with_session(&mut root, "RESTART")
        .unwrap_err();
    std::env::remove_var("MYDB_RESTART_DISABLE");
    assert_eq!(
        err.code(),
        3707,
        "RESTART when in-process restart is disabled must return ER_RESTART_SERVER_FAILED: {err:?}"
    );
    // The capability gate returns *before* triggering shutdown: a server
    // that was not already shutting down must stay up.
    if !was_shutting_down {
        assert!(
            !shutdown::is_shutting_down(),
            "3707 capability gate must not trigger shutdown"
        );
    }
}

#[test]
fn shutdown_grant_allows_and_sets_coordinator() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    engine
        .execute_with_session(&mut root, "CREATE USER 'alice'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    engine
        .execute_with_session(&mut root, "GRANT SHUTDOWN ON *.* TO 'alice'@'%'")
        .unwrap();
    // SHOW GRANTS reflects the SHUTDOWN privilege.
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(&mut alice_session(&engine), "SHOW GRANTS")
        .unwrap()
    else {
        panic!("expected SHOW GRANTS result set");
    };
    let has_shutdown = rs
        .rows
        .iter()
        .any(|row| matches!(row.first(), Some(Value::String(s)) if s.contains("SHUTDOWN")));
    assert!(has_shutdown, "granted account should show SHUTDOWN");

    // Executing SHUTDOWN as the granted account returns OK and arms the
    // process-level shutdown coordinator. (Going through the engine does
    // not re-exec / exit — that only happens in the server accept loop.)
    let mut alice = alice_session(&engine);
    let resp = engine.execute_with_session(&mut alice, "SHUTDOWN").unwrap();
    assert!(matches!(resp, QueryResponse::Ok), "SHUTDOWN → OK");
    assert!(
        shutdown::is_shutting_down(),
        "SHUTDOWN must arm the shutdown coordinator"
    );
}

#[test]
fn restart_sets_restart_intent_when_supported() {
    shutdown::cache_launch_args();
    std::env::remove_var("MYDB_RESTART_DISABLE");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    let was_shutting_down = shutdown::is_shutting_down();
    let resp = engine.execute_with_session(&mut root, "RESTART").unwrap();
    assert!(
        matches!(resp, QueryResponse::Ok),
        "RESTART → OK when supported"
    );
    assert!(shutdown::is_shutting_down());
    // Only assert the pinned intent if this call is the one that armed the
    // coordinator (the first request wins the intent).
    if !was_shutting_down && shutdown::restart_supported() {
        assert_eq!(shutdown::intent(), shutdown::ShutdownIntent::Restart);
    }
}
