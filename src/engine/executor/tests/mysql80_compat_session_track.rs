//! P2-A2: actual CLIENT_SESSION_TRACK SCHEMA and STATE_CHANGE support.
//!
//! Previously the handshake advertised CLIENT_SESSION_TRACK but USE database
//! returned no session-state-info payload. Connector/J/mysqlsh then retained
//! the old currentSchema after USE other_db, causing later unqualified SELECT
//! queries to fail due to schema mismatch.
//!
//! These tests establish:
//! USE sets session.pending_session_track_schema to the new database name.
//! - session.session_state_changed = true(state-change channel)
//! The notification is consumed with take and does not leak into later OK packets.
//! Dropping the current database and falling back to DEFAULT also notifies schema change.
//! Ordinary SELECT without USE must not enqueue a SCHEMA payload.

use super::support::execute_materialized as exec;
use super::*;

#[test]
fn use_database_queues_session_track_schema() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.session_track_state_change = true;
    exec(&engine, &mut session, "CREATE DATABASE other").unwrap();

    exec(&engine, &mut session, "USE other").unwrap();

    assert_eq!(
        session.pending_session_track_schema.as_deref(),
        Some("other")
    );
    assert!(session.session_state_changed);
}

#[test]
fn take_session_track_payload_drains_and_resets_flags() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    session.session_track_state_change = true;
    exec(&engine, &mut session, "CREATE DATABASE other").unwrap();
    exec(&engine, &mut session, "USE other").unwrap();

    let (payload, changed) = session.take_session_track_payload();
    assert!(changed, "state-change bit must fire");
    assert!(!payload.is_empty(), "schema payload should be non-empty");
    // Layout:
    //   [SCHEMA entry] then [STATE_CHANGE entry], in that order.
    // SCHEMA = [0x01, outer=6, inner=5, b"other"] = 8 bytes
    // STATE_CHANGE = [0x02, outer=2, inner=1, b'1'] = 4 bytes
    assert_eq!(payload[0], 0x01); // SCHEMA type
    assert_eq!(&payload[3..8], b"other");
    assert_eq!(payload[8], 0x02); // STATE_CHANGE type
    assert_eq!(payload[payload.len() - 1], b'1');

    // Draining twice in a row yields nothing — the queue is reset.
    let (payload2, changed2) = session.take_session_track_payload();
    assert!(payload2.is_empty());
    assert!(!changed2);
}

#[test]
fn select_does_not_queue_schema_change() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1)").unwrap();
    exec(&engine, &mut session, "SELECT id FROM t").unwrap();

    assert!(session.pending_session_track_schema.is_none());
    assert!(!session.session_state_changed);
}

#[test]
fn drop_current_database_queues_default_schema() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE DATABASE soon_gone").unwrap();
    exec(&engine, &mut session, "USE soon_gone").unwrap();
    // Drain the USE-triggered payload so the next assertion isolates
    // the DROP-triggered one.
    session.take_session_track_payload();

    exec(&engine, &mut session, "DROP DATABASE soon_gone").unwrap();

    // Session fell back to the default database — client must learn.
    assert_eq!(
        session.pending_session_track_schema.as_deref(),
        Some(""),
        "DROP of the current database clears the selected schema"
    );
    assert!(session.session_state_changed);
}

#[test]
fn drop_unrelated_database_does_not_queue_schema_change() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE DATABASE keep_me").unwrap();
    exec(&engine, &mut session, "CREATE DATABASE bye").unwrap();
    exec(&engine, &mut session, "USE keep_me").unwrap();
    session.take_session_track_payload();

    // Dropping a different database leaves the current one alone, so
    // there's no schema-change to advertise.
    exec(&engine, &mut session, "DROP DATABASE bye").unwrap();
    assert!(session.pending_session_track_schema.is_none());
    assert!(!session.session_state_changed);
}

#[test]
fn use_same_database_still_emits_schema_track() {
    // MySQL 8.0 fires SESSION_TRACK_SCHEMA on every `USE` regardless of
    // whether the name changed — clients re-sync their cached state on
    // the back of it, and skipping the notify on no-op `USE` would
    // surprise tools that depend on it.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "USE test").unwrap();
    assert_eq!(
        session.pending_session_track_schema.as_deref(),
        Some("test")
    );
}
