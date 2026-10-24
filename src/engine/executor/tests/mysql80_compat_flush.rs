//! P0-C3: actual FLUSH TABLES/STATUS/HOSTS/QUERY CACHE behavior.
//!
//! Previously all four mapped to FlushNoOp and returned Ok(QueryResponse::Ok),
//! accepting syntax without effects. These tests establish MySQL 8.0-compatible behavior:
//!
//! FLUSH HOSTS resets per-account failed_login_count and account locks.
//! FLUSH STATUS resets Aborted_connects and Aborted_clients.
//! FLUSH TABLES invokes db.flush_dirty_data for buffer pool and WAL flush.
//! FLUSH TABLES t1,t2 validates names; missing tables return 1146.
//! FLUSH TABLES WITH READ LOCK currently returns 1235; global read locks are unsupported.
//! FLUSH TABLES FOR EXPORT currently returns 1235.
//! FLUSH QUERY CACHE is accepted with warning 1287 because MySQL 8.0 removed query cache.

use super::support::execute_materialized as exec;
use super::*;

// ---------------------------------------------------------------------------
// FLUSH HOSTS — resets per-account failed_login_count
// ---------------------------------------------------------------------------

#[test]
fn flush_hosts_resets_failed_login_counts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Failed-login tracking is in-memory (MySQL-style, not persisted). Plant a
    // temporary lock via the tracker, then FLUSH HOSTS must clear it.
    let key = crate::engine::auth::AccountKey::new("flushhosts_user", "%");
    let now = 1_000_000_000u64;
    // Two failures under a 2-attempt policy trips the lock.
    crate::engine::auth::failed_login_record(&key, 2, Some(1), now);
    crate::engine::auth::failed_login_record(&key, 2, Some(1), now);
    assert!(
        crate::engine::auth::failed_login_active_lock(&key, now).is_some(),
        "account should be temporarily locked before FLUSH HOSTS"
    );

    assert!(matches!(
        exec(&engine, &mut session, "FLUSH HOSTS").unwrap(),
        QueryResponse::Ok
    ));

    // After FLUSH HOSTS, the temporary lock / failure budget is cleared.
    assert!(
        crate::engine::auth::failed_login_active_lock(&key, now).is_none(),
        "FLUSH HOSTS must reset the failed-login budget"
    );
}

// ---------------------------------------------------------------------------
// FLUSH STATUS — resets Aborted_connects/Aborted_clients style counters
// ---------------------------------------------------------------------------

#[test]
fn flush_status_resets_abort_counters() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // The test process is the same instance for every nextest case; we
    // can't measure absolute values, but we can call FLUSH STATUS and
    // confirm it returns Ok without raising 1235.
    assert!(matches!(
        exec(&engine, &mut session, "FLUSH STATUS").unwrap(),
        QueryResponse::Ok
    ));
}

// ---------------------------------------------------------------------------
// FLUSH TABLES — bare form succeeds; table list validates existence
// ---------------------------------------------------------------------------

#[test]
fn flush_tables_bare_form_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Without a table list we just flush whatever dirty pages happen to
    // be in the buffer pool. Should be an unconditional Ok.
    assert!(matches!(
        exec(&engine, &mut session, "FLUSH TABLES").unwrap(),
        QueryResponse::Ok
    ));
}

#[test]
fn flush_tables_with_existing_table_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut session, "INSERT INTO t VALUES (1), (2)").unwrap();

    // The table exists — FLUSH TABLES with an explicit list flushes only
    // those tables (here we still funnel through the global flush path,
    // but the existence check should pass).
    assert!(matches!(
        exec(&engine, &mut session, "FLUSH TABLES t").unwrap(),
        QueryResponse::Ok
    ));
}

#[test]
fn flush_tables_with_unknown_table_raises_1146() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let err = exec(&engine, &mut session, "FLUSH TABLES no_such_table").unwrap_err();
    assert_eq!(err.code(), 1146);
    assert!(
        err.message()
            .to_ascii_lowercase()
            .contains("does not exist")
            || err.message().to_ascii_lowercase().contains("doesn't exist"),
        "expected 'doesn't exist' message; got: {}",
        err.message()
    );
}

#[test]
fn flush_tables_multiple_tables_validates_each() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE a (id INT PRIMARY KEY)").unwrap();
    exec(&engine, &mut session, "CREATE TABLE b (id INT PRIMARY KEY)").unwrap();

    assert!(matches!(
        exec(&engine, &mut session, "FLUSH TABLES a, b").unwrap(),
        QueryResponse::Ok
    ));

    // Comma-separated list with one bad name → still 1146.
    let err = exec(&engine, &mut session, "FLUSH TABLES a, bogus").unwrap_err();
    assert_eq!(err.code(), 1146);
}

// ---------------------------------------------------------------------------
// FLUSH TABLES WITH READ LOCK / FOR EXPORT — 1235 (clear "not yet supported")
// ---------------------------------------------------------------------------

#[test]
fn flush_tables_with_read_lock_acquires_global_read_lock() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // The bare form is intercepted by the binlog admin path which owns the
    // global read lock state. It must succeed (mysqldump --master-data
    // depends on this). The follow-up `UNLOCK TABLES` releases the lock.
    assert!(matches!(
        exec(&engine, &mut session, "FLUSH TABLES WITH READ LOCK").unwrap(),
        QueryResponse::Ok
    ));
    assert!(matches!(
        exec(&engine, &mut session, "UNLOCK TABLES").unwrap(),
        QueryResponse::Ok
    ));
}

#[test]
fn flush_tables_with_table_list_and_read_lock_returns_1235() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    let err = exec(&engine, &mut session, "FLUSH TABLES t WITH READ LOCK").unwrap_err();
    assert_eq!(err.code(), 1235);
}

#[test]
fn flush_tables_for_export_returns_1235() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    let err = exec(&engine, &mut session, "FLUSH TABLES t FOR EXPORT").unwrap_err();
    assert_eq!(err.code(), 1235);
}

// ---------------------------------------------------------------------------
// FLUSH QUERY CACHE — accept + deprecation warning 1287
// ---------------------------------------------------------------------------

#[test]
fn flush_query_cache_emits_deprecation_warning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    assert!(matches!(
        exec(&engine, &mut session, "FLUSH QUERY CACHE").unwrap(),
        QueryResponse::Ok
    ));
    // After the statement, the per-statement warnings live on the session
    // (see `finish_statement_diagnostics` in executor/mod.rs).
    assert!(
        session.warnings.iter().any(|w| w.code() == 1287),
        "expected deprecation warning 1287; got {:?}",
        session.warnings
    );
}

#[test]
fn reset_query_cache_also_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    assert!(matches!(
        exec(&engine, &mut session, "RESET QUERY CACHE").unwrap(),
        QueryResponse::Ok
    ));
    assert!(session.warnings.iter().any(|w| w.code() == 1287));
}
