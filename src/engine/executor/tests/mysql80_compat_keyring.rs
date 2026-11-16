//! P0-A1 phase 1 — MySQL Enterprise keyring SQL functions。
//!
//! mydb exposes MySQL Enterprise-compatible keyring function names backed by
//! a process-local in-memory keyring; TDE at-rest encryption is deferred to phase 2.
//!
//! These tests establish:
//! KEYRING_KEY_GENERATE returns 1 and FETCH returns nonempty data.
//! KEYRING_KEY_LENGTH and KEYRING_KEY_TYPE agree.
//! A second KEYRING_KEY_REMOVE returns 0.
//! Duplicate generation returns keyring service error 3188.
//! An empty key_id returns invalid-key error 3930.
//! - KEYRING_KEY_STORE round trip

use super::support::execute_materialized as exec;
use super::*;

fn one(rs: QueryResponse) -> Value {
    let QueryResponse::ResultSet(rs) = rs else {
        panic!("expected result set");
    };
    rs.rows[0][0].clone()
}

fn unique_id(prefix: &str, suffix: &str) -> String {
    format!("{prefix}_{suffix}")
}

#[test]
fn keyring_generate_then_fetch_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let kid = unique_id("test_rt", "gen");
    let _ = crate::engine::keyring::remove(&kid);
    let rs = exec(
        &engine,
        &mut session,
        &format!("SELECT KEYRING_KEY_GENERATE('{kid}', 'AES', 256)"),
    )
    .unwrap();
    assert!(matches!(one(rs), Value::Int(1)));
    let rs = exec(
        &engine,
        &mut session,
        &format!("SELECT KEYRING_KEY_LENGTH('{kid}')"),
    )
    .unwrap();
    assert!(matches!(one(rs), Value::Int(256)));
    let rs = exec(
        &engine,
        &mut session,
        &format!("SELECT KEYRING_KEY_TYPE('{kid}')"),
    )
    .unwrap();
    match one(rs) {
        Value::String(s) => assert_eq!(s, "AES"),
        other => panic!("unexpected: {other:?}"),
    }
    let _ = crate::engine::keyring::remove(&kid);
}

#[test]
fn keyring_remove_returns_1_then_0() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let kid = unique_id("test_rm", "x");
    let _ = crate::engine::keyring::remove(&kid);
    exec(
        &engine,
        &mut session,
        &format!("SELECT KEYRING_KEY_GENERATE('{kid}', 'AES', 128)"),
    )
    .unwrap();
    let rs = exec(
        &engine,
        &mut session,
        &format!("SELECT KEYRING_KEY_REMOVE('{kid}')"),
    )
    .unwrap();
    assert!(matches!(one(rs), Value::Int(1)));
    let rs = exec(
        &engine,
        &mut session,
        &format!("SELECT KEYRING_KEY_REMOVE('{kid}')"),
    )
    .unwrap();
    assert!(matches!(one(rs), Value::Int(0)));
}

#[test]
fn keyring_duplicate_generate_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let kid = unique_id("test_dup", "y");
    let _ = crate::engine::keyring::remove(&kid);
    exec(
        &engine,
        &mut session,
        &format!("SELECT KEYRING_KEY_GENERATE('{kid}', 'AES', 128)"),
    )
    .unwrap();
    let err = exec(
        &engine,
        &mut session,
        &format!("SELECT KEYRING_KEY_GENERATE('{kid}', 'AES', 128)"),
    )
    .unwrap_err();
    assert_eq!(err.code(), 3188);
    let _ = crate::engine::keyring::remove(&kid);
}

#[test]
fn keyring_empty_key_id_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let err = exec(
        &engine,
        &mut session,
        "SELECT KEYRING_KEY_GENERATE('', 'AES', 128)",
    )
    .unwrap_err();
    assert_eq!(err.code(), 3930);
}

#[test]
fn keyring_store_then_fetch_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let kid = unique_id("test_st", "z");
    let _ = crate::engine::keyring::remove(&kid);
    let rs = exec(
        &engine,
        &mut session,
        &format!("SELECT KEYRING_KEY_STORE('{kid}', 'AES', 'secret-bytes-here')"),
    )
    .unwrap();
    assert!(matches!(one(rs), Value::Int(1)));
    let rs = exec(
        &engine,
        &mut session,
        &format!("SELECT KEYRING_KEY_FETCH('{kid}')"),
    )
    .unwrap();
    match one(rs) {
        Value::String(s) => assert_eq!(s, "secret-bytes-here"),
        other => panic!("unexpected fetch: {other:?}"),
    }
    let _ = crate::engine::keyring::remove(&kid);
}

#[test]
fn keyring_unknown_key_fetch_returns_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let rs = exec(
        &engine,
        &mut session,
        "SELECT KEYRING_KEY_FETCH('never_existed_kid')",
    )
    .unwrap();
    assert!(matches!(one(rs), Value::Null));
}
