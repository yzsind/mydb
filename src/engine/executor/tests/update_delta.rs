// Phase 2e integration tests for the column-level UPDATE patch path.
//
// The patch path is gated behind `MYDB_UPDATE_DELTA` (env) and a
// per-process `set_update_delta_enabled_for_test(true)` toggle that
// these tests use to flip it on, then off again afterwards so they
// don't leak state into the rest of the executor test suite.
//
// Coverage:
//   * basic UPDATE round-trip — narrow INT-only table, verify the
//     observable values after UPDATE match the legacy path's,
//   * multi-row UPDATE — verify N rows all flip without dropping any,
//   * rollback — UPDATE inside BEGIN; ROLLBACK; verify the old
//     values come back (this exercises the UndoKind::UpdateDelta
//     reverse-apply in `rollback_transaction`),
//   * fallback for NULL transitions — change a NULLable col from
//     non-null to NULL; the patch path bails and the legacy path
//     covers correctness.

use super::*;
use crate::storage::set_update_delta_enabled_for_test;

struct DeltaGuard;

impl DeltaGuard {
    fn enable() -> Self {
        set_update_delta_enabled_for_test(true);
        Self
    }
}

impl Drop for DeltaGuard {
    fn drop(&mut self) {
        set_update_delta_enabled_for_test(false);
    }
}

#[test]
fn update_delta_narrow_int_only_round_trip() {
    let _g = DeltaGuard::enable();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)")
        .unwrap();
    // First UPDATE: drives the row's MVCC header from 2B (post-insert)
    // to 18B; this one goes through the full-row fallback (header
    // size change).
    engine
        .execute("UPDATE t SET v = v + 1 WHERE id = 1")
        .unwrap();
    // Second UPDATE on the same row: header sizes match, should
    // take the in-place patch path.
    engine
        .execute("UPDATE t SET v = v + 2 WHERE id = 1")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id, k, v FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(10), Value::Int(103)],
            vec![Value::Int(2), Value::Int(20), Value::Int(200)],
            vec![Value::Int(3), Value::Int(30), Value::Int(300)],
        ]
    );
}

#[test]
fn update_delta_multi_row_one_statement() {
    let _g = DeltaGuard::enable();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .unwrap();
    // Warm up: get every row past the first-update header-grow.
    engine
        .execute("UPDATE t SET v = v + 0 WHERE id > 0")
        .unwrap();
    // Patch path UPDATE — flip v on all 4 rows in one statement.
    engine
        .execute("UPDATE t SET v = v + 100 WHERE id > 0")
        .unwrap();

    let QueryResponse::ResultSet(result) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(110)],
            vec![Value::Int(2), Value::Int(120)],
            vec![Value::Int(3), Value::Int(130)],
            vec![Value::Int(4), Value::Int(140)],
        ]
    );
}

#[test]
fn update_delta_rollback_restores_original_values() {
    let _g = DeltaGuard::enable();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    // Drive past the first-update header growth so the next UPDATE
    // exercises the patch path.
    engine.execute("UPDATE t SET v = v + 0").unwrap();

    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "UPDATE t SET v = v + 999 WHERE id = 1")
        .unwrap();
    // Inside the txn the new value is visible.
    let QueryResponse::ResultSet(mid) = engine
        .execute_with_session(&mut session, "SELECT v FROM t WHERE id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(mid.rows, vec![vec![Value::Int(1009)]]);
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();

    // After ROLLBACK the original value is restored — the
    // UndoKind::UpdateDelta reverse-apply ran on the heap row.
    let QueryResponse::ResultSet(after) =
        engine.execute("SELECT id, v FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        after.rows,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(20)],
        ]
    );
}

#[test]
fn update_delta_falls_back_on_null_transition() {
    let _g = DeltaGuard::enable();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 7)").unwrap();
    // First UPDATE grows header — already excluded.
    engine.execute("UPDATE t SET v = 8 WHERE id = 1").unwrap();
    // Non-null → NULL transition: patch path bails; fallback path
    // handles the NULL bitmap update correctly.
    engine
        .execute("UPDATE t SET v = NULL WHERE id = 1")
        .unwrap();
    let QueryResponse::ResultSet(after) =
        engine.execute("SELECT id, v FROM t WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(after.rows, vec![vec![Value::Int(1), Value::Null]]);
}

#[test]
fn update_delta_off_by_default_still_correct() {
    // Make sure the legacy path still works when the toggle is off.
    set_update_delta_enabled_for_test(false);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    engine.execute("UPDATE t SET v = 99 WHERE id = 1").unwrap();
    let QueryResponse::ResultSet(after) =
        engine.execute("SELECT id, v FROM t WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(after.rows, vec![vec![Value::Int(1), Value::Int(99)]]);
}
