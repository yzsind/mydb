//! P1-D3: count actual slow-log Rows_examined.
//!
//! Previously slow_log.rows_examined equaled rows_sent, so scanning one million rows
//! for SELECT id FROM t WHERE v=999 and returning zero rows also logged zero examined rows.
//! This hid full scans from administrators. These tests establish:
//!
//! record_rows_examined/take_rows_examined accumulate actual counts.
//! clear_thread_wait_samples resets rows_examined per statement.
//! Multiple record calls accumulate across table/partition scans.

use super::*;

#[test]
fn rows_examined_counter_accumulates_and_drains() {
    crate::engine::perf_events::clear_thread_wait_samples();
    crate::engine::perf_events::record_rows_examined(5);
    crate::engine::perf_events::record_rows_examined(7);
    let total = crate::engine::perf_events::take_rows_examined();
    assert_eq!(total, 12);
    // Drained — next take is zero.
    assert_eq!(crate::engine::perf_events::take_rows_examined(), 0);
}

#[test]
fn rows_examined_zero_increment_is_noop() {
    crate::engine::perf_events::clear_thread_wait_samples();
    crate::engine::perf_events::record_rows_examined(0);
    assert_eq!(crate::engine::perf_events::take_rows_examined(), 0);
}

#[test]
fn sequential_handler_reads_exclude_index_examined_rows() {
    crate::engine::perf_events::clear_thread_wait_samples();
    crate::engine::perf_events::record_rows_examined(2);
    crate::engine::perf_events::record_scan_rows_examined(5);
    assert_eq!(crate::engine::perf_events::take_rows_examined(), 7);
    assert_eq!(crate::engine::perf_events::take_scan_rows_examined(), 5);
}

#[test]
fn clear_thread_wait_samples_also_clears_rows_examined() {
    crate::engine::perf_events::record_rows_examined(42);
    crate::engine::perf_events::clear_thread_wait_samples();
    assert_eq!(crate::engine::perf_events::take_rows_examined(), 0);
}

#[test]
fn scan_rows_bumps_rows_examined_counter() {
    // End-to-end: a SELECT that drives `Database::scan_rows` should
    // bump the per-thread counter by the number of visible rows. We
    // use the executor entry point so we exercise the same call path
    // dispatch.rs uses for slow-log accounting.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    <StorageQueryEngine as QueryEngine>::execute(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    )
    .unwrap();
    <StorageQueryEngine as QueryEngine>::execute(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50)",
    )
    .unwrap();

    crate::engine::perf_events::clear_thread_wait_samples();
    // Direct storage-layer scan_rows: opens a snapshot and walks the
    // five visible rows, bumping the rows-examined counter.
    let txn = engine.db.begin_txn().unwrap();
    let rows = engine.db.scan_rows("test.t", txn).unwrap();
    assert_eq!(rows.len(), 5);
    let _ = engine.db.rollback_txn(txn.txn_id);
    let examined = crate::engine::perf_events::take_rows_examined();
    let sequential = crate::engine::perf_events::take_scan_rows_examined();
    assert!(
        examined >= 5,
        "expected at least 5 rows examined, got {examined}"
    );
    assert!(sequential >= 5);
}

#[test]
fn streamed_select_records_examined_rows_after_cursor_consumption() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE observed_rows (id INT PRIMARY KEY, v INT)",
        "INSERT INTO observed_rows VALUES (1,10),(2,20),(3,30)",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    let before = crate::engine::status_counters::rows_read();
    let response = engine
        .execute_with_session(&mut session, "SELECT id,v FROM observed_rows WHERE v >= 0")
        .unwrap();
    let QueryResponse::ResultSet(rows) = response.materialize().unwrap() else {
        panic!("expected materialized result set");
    };
    assert_eq!(rows.rows.len(), 3);
    assert!(crate::engine::status_counters::rows_read() >= before + 3);

    let response = engine
        .execute_with_session(
            &mut session,
            "SELECT ROWS_EXAMINED FROM performance_schema.events_statements_history WHERE SQL_TEXT = 'SELECT id,v FROM observed_rows WHERE v >= 0' LIMIT 1",
        )
        .unwrap()
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(events) = response else {
        panic!("expected PFS result set");
    };
    assert_eq!(events.rows.len(), 1);
    assert!(
        events.rows[0][0]
            .to_display_string()
            .parse::<u64>()
            .unwrap()
            >= 3
    );
}
