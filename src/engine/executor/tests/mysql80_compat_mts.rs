//! P1-C1 phase 1: accept and persist MTS (multi-threaded slave) sysvars.
//!
//! The replication applier is currently single-threaded. Phase 1 accepts MySQL
//! MTS configuration variables such as slave_parallel_type, replica_preserve_commit_order,
//! and slave_checkpoint_period so my.cnf and SET GLOBAL no longer fail with 1193.
//! An actual parallel applier worker pool is deferred to phase 2.

use super::*;

#[test]
fn slave_parallel_type_accepts_logical_clock_and_database() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    <StorageQueryEngine as QueryEngine>::execute(
        &engine,
        &mut session,
        "SET GLOBAL slave_parallel_type = 'LOGICAL_CLOCK'",
    )
    .unwrap();
    <StorageQueryEngine as QueryEngine>::execute(
        &engine,
        &mut session,
        "SET GLOBAL slave_parallel_type = 'DATABASE'",
    )
    .unwrap();
}

#[test]
fn replica_parallel_type_alias_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    <StorageQueryEngine as QueryEngine>::execute(
        &engine,
        &mut session,
        "SET GLOBAL replica_parallel_type = 'LOGICAL_CLOCK'",
    )
    .unwrap();
}

#[test]
fn preserve_commit_order_accepts_bool() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    <StorageQueryEngine as QueryEngine>::execute(
        &engine,
        &mut session,
        "SET GLOBAL slave_preserve_commit_order = ON",
    )
    .unwrap();
    <StorageQueryEngine as QueryEngine>::execute(
        &engine,
        &mut session,
        "SET GLOBAL replica_preserve_commit_order = OFF",
    )
    .unwrap();
}

#[test]
fn checkpoint_period_accepts_uint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    <StorageQueryEngine as QueryEngine>::execute(
        &engine,
        &mut session,
        "SET GLOBAL slave_checkpoint_period = 600",
    )
    .unwrap();
    <StorageQueryEngine as QueryEngine>::execute(
        &engine,
        &mut session,
        "SET GLOBAL replica_checkpoint_group = 1024",
    )
    .unwrap();
}

#[test]
fn parallel_type_rejects_invalid_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let err = <StorageQueryEngine as QueryEngine>::execute(
        &engine,
        &mut session,
        "SET GLOBAL slave_parallel_type = 'STATEMENT'",
    )
    .unwrap_err();
    // ER_WRONG_VALUE_FOR_VAR (1231) per MySQL.
    assert!(
        err.is(crate::error::catalog::ER_WRONG_VALUE_FOR_VAR)
            || err.is(crate::error::catalog::ER_PARSE_ERROR),
        "expected 1231 / 1064, got {}",
        err.code()
    );
}
