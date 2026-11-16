//! MySQL 8.0 transaction-statement compatibility regressions:
//! COMMIT/ROLLBACK completion clauses (`[WORK] AND [NO] CHAIN [NO] RELEASE`),
//! `completion_type`, and `START TRANSACTION` characteristic lists
//! (`WITH CONSISTENT SNAPSHOT` / access mode combinations).

use super::*;
use crate::storage::IsolationLevel;

// ---- M1: COMMIT / ROLLBACK completion clauses + completion_type ----

#[test]
fn commit_and_chain_reopens_transaction_preserving_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "START TRANSACTION")
        .unwrap();
    assert!(session.in_transaction());
    assert_eq!(session.effective_isolation(), IsolationLevel::Serializable);

    engine
        .execute_with_session(&mut session, "COMMIT AND CHAIN")
        .unwrap();
    // A new transaction is open immediately, with the same isolation level.
    assert!(
        session.in_transaction(),
        "COMMIT AND CHAIN must re-open a txn"
    );
    assert_eq!(session.effective_isolation(), IsolationLevel::Serializable);

    engine
        .execute_with_session(&mut session, "COMMIT AND NO CHAIN")
        .unwrap();
    assert!(
        !session.explicit_txn,
        "COMMIT AND NO CHAIN must end the txn"
    );
}

#[test]
fn rollback_and_chain_reopens_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "START TRANSACTION")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK AND CHAIN")
        .unwrap();
    assert!(
        session.in_transaction(),
        "ROLLBACK AND CHAIN must re-open a txn"
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert!(!session.explicit_txn);
}

#[test]
fn completion_type_drives_default_chain_and_is_readable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // Default is NO_CHAIN.
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT @@completion_type")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::String("NO_CHAIN".into())]]);

    // completion_type = 1 (CHAIN): a bare COMMIT now chains by default.
    engine
        .execute_with_session(&mut session, "SET completion_type = 1")
        .unwrap();
    assert_eq!(session.sysvars.completion_type, 1);
    engine
        .execute_with_session(&mut session, "START TRANSACTION")
        .unwrap();
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
    assert!(
        session.in_transaction(),
        "completion_type=CHAIN must re-open a txn on bare COMMIT"
    );

    // NO CHAIN on the statement overrides the completion_type default.
    engine
        .execute_with_session(&mut session, "COMMIT AND NO CHAIN")
        .unwrap();
    assert!(!session.explicit_txn);

    // Named form round-trips.
    engine
        .execute_with_session(&mut session, "SET completion_type = 'RELEASE'")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT @@completion_type")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::String("RELEASE".into())]]);
}

#[test]
fn commit_release_flags_connection_for_close() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "START TRANSACTION")
        .unwrap();
    engine
        .execute_with_session(&mut session, "COMMIT RELEASE")
        .unwrap();
    assert!(
        session.close_after_response,
        "COMMIT RELEASE must flag the connection for close"
    );
}

#[test]
fn commit_work_keyword_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "START TRANSACTION")
        .unwrap();
    engine
        .execute_with_session(&mut session, "COMMIT WORK")
        .unwrap();
    assert!(!session.explicit_txn);
}

// ---- M2: START TRANSACTION characteristic list ----

#[test]
fn start_transaction_read_only_and_read_write_set_access_mode() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "START TRANSACTION READ ONLY")
        .unwrap();
    assert!(session.read_only_txn);
    engine.execute_with_session(&mut session, "COMMIT").unwrap();

    engine
        .execute_with_session(&mut session, "START TRANSACTION READ WRITE")
        .unwrap();
    assert!(!session.read_only_txn);
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
}

#[test]
fn start_transaction_combined_consistent_snapshot_and_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();

    // CONSISTENT SNAPSHOT is effective under explicit REPEATABLE READ and
    // pins the snapshot; READ ONLY sets the access mode.
    engine
        .execute_with_session(
            &mut session,
            "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY",
        )
        .unwrap();
    assert!(session.read_only_txn);
    assert!(
        session.txn_snapshot_scn.is_some(),
        "consistent snapshot must pin a read view at BEGIN"
    );
    engine.execute_with_session(&mut session, "COMMIT").unwrap();

    // Reversed order parses identically.
    engine
        .execute_with_session(
            &mut session,
            "START TRANSACTION READ ONLY, WITH CONSISTENT SNAPSHOT",
        )
        .unwrap();
    assert!(session.read_only_txn);
    assert!(session.txn_snapshot_scn.is_some());
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
}

// ---- M5: SET TRANSACTION scope precision + savepoint / read-only errors ----

#[test]
fn set_transaction_no_scope_in_transaction_errors_1568() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "START TRANSACTION")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .unwrap_err();
    assert_eq!(err.code(), 1568);
    assert_eq!(err.sql_state(), "25001");
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
}

#[test]
fn set_session_and_global_transaction_allowed_mid_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "START TRANSACTION")
        .unwrap();
    // SESSION and GLOBAL scope are permitted while a transaction is active
    // (they just do not affect the current transaction) — must not error.
    engine
        .execute_with_session(
            &mut session,
            "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        )
        .unwrap();
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
}

#[test]
fn set_transaction_next_scope_applies_only_to_next_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    // No-scope SET TRANSACTION targets only the next transaction.
    engine
        .execute_with_session(&mut session, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .unwrap();
    engine
        .execute_with_session(&mut session, "START TRANSACTION")
        .unwrap();
    assert_eq!(session.effective_isolation(), IsolationLevel::Serializable);
    engine.execute_with_session(&mut session, "COMMIT").unwrap();

    // The following transaction reverts to the session default (READ COMMITTED).
    engine
        .execute_with_session(&mut session, "START TRANSACTION")
        .unwrap();
    assert_eq!(session.effective_isolation(), IsolationLevel::ReadCommitted);
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
}

#[test]
fn rollback_to_and_release_nonexistent_savepoint_error_1305() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "START TRANSACTION")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "ROLLBACK TO SAVEPOINT nope")
        .unwrap_err();
    assert_eq!(err.code(), 1305);
    assert_eq!(err.sql_state(), "42000");
    let err = engine
        .execute_with_session(&mut session, "RELEASE SAVEPOINT nope")
        .unwrap_err();
    assert_eq!(err.code(), 1305);
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
}

#[test]
fn read_only_transaction_dml_errors_1792() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut setup = SessionContext::default();
    engine
        .execute_with_session(&mut setup, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();

    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "START TRANSACTION READ ONLY")
        .unwrap();
    let err = engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1)")
        .unwrap_err();
    assert_eq!(err.code(), 1792);
    assert_eq!(err.sql_state(), "25006");
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
}

// ---- M6: implicit commit audit + events_transactions_current ----

#[test]
fn admin_statements_implicitly_commit_pending_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();

    // Each admin/DDL statement below must implicitly commit the row inserted
    // just before it, so a later ROLLBACK cannot undo it.
    for (i, stmt) in [
        "OPTIMIZE TABLE t",
        "CHECK TABLE t",
        "FLUSH STATUS",
        "FLUSH TABLES",
    ]
    .into_iter()
    .enumerate()
    {
        let mut s = SessionContext::default();
        let id = 100 + i as i64; // distinct pk per case
        engine.execute_with_session(&mut s, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut s, &format!("INSERT INTO t VALUES ({id})"))
            .unwrap();
        engine.execute_with_session(&mut s, stmt).unwrap();
        // The transaction is already committed; ROLLBACK is a no-op.
        engine.execute_with_session(&mut s, "ROLLBACK").unwrap();

        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(
                &mut SessionContext::default(),
                &format!("SELECT id FROM t WHERE id = {id}"),
            )
            .unwrap()
        else {
            panic!("expected result set");
        };
        assert_eq!(
            result.rows.len(),
            1,
            "`{stmt}` must implicitly commit the prior INSERT"
        );
    }
}

#[test]
fn rename_table_implicitly_commits() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE keep (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "CREATE TABLE ren (id INT PRIMARY KEY)")
        .unwrap();

    // Insert into `keep`, then RENAME an unrelated table. The RENAME must
    // implicitly commit the pending INSERT so a later ROLLBACK cannot undo it.
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO keep VALUES (1)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "RENAME TABLE ren TO ren2")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut SessionContext::default(), "SELECT id FROM keep")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(1)]],
        "RENAME TABLE must implicitly commit the prior INSERT"
    );
}

#[test]
fn create_temporary_table_does_not_implicitly_commit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "CREATE TABLE base (id INT PRIMARY KEY)")
        .unwrap();

    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO base VALUES (1)")
        .unwrap();
    // CREATE TEMPORARY TABLE must NOT implicitly commit the pending txn.
    engine
        .execute_with_session(&mut session, "CREATE TEMPORARY TABLE tmp (x INT)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut SessionContext::default(), "SELECT id FROM base")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        result.rows.is_empty(),
        "CREATE TEMPORARY TABLE must not commit the pending INSERT"
    );
}

#[test]
fn events_transactions_current_reflects_active_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut writer = SessionContext::default();
    engine
        .execute_with_session(&mut writer, "CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();

    // Open a read-write transaction and leave it active.
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "INSERT INTO t VALUES (1)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut writer,
            "SELECT EVENT_NAME, STATE, ACCESS_MODE, ISOLATION_LEVEL FROM \
             performance_schema.events_transactions_current",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        result.rows.iter().any(|r| {
            matches!(r.first(), Some(Value::String(n)) if n == "transaction")
                && matches!(r.get(1), Some(Value::String(st)) if st == "ACTIVE")
                && matches!(r.get(2), Some(Value::String(am)) if am == "READ WRITE")
                && matches!(r.get(3), Some(Value::String(iso)) if iso == "READ COMMITTED")
        }),
        "active transaction must appear in events_transactions_current, got {:?}",
        result.rows
    );

    engine.execute_with_session(&mut writer, "COMMIT").unwrap();
}

// ---- M4: ROLLBACK TO SAVEPOINT keeps row locks (MySQL/InnoDB semantics) ----

#[test]
fn rollback_to_savepoint_keeps_row_locks_acquired_after_it() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut owner = SessionContext::default();
    let mut waiter = SessionContext::default();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "UPDATE t SET v = 11 WHERE id = 1")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "SAVEPOINT sp")
        .unwrap();
    // Lock row 2 (a pre-existing row) after the savepoint.
    engine
        .execute_with_session(&mut owner, "UPDATE t SET v = 21 WHERE id = 2")
        .unwrap();
    // ROLLBACK TO SAVEPOINT undoes the row-2 change but, per MySQL/InnoDB,
    // KEEPS the X lock on row 2 until the transaction ends.
    engine
        .execute_with_session(&mut owner, "ROLLBACK TO SAVEPOINT sp")
        .unwrap();

    // A concurrent writer must still block on row 2 and time out (1205).
    engine
        .execute_with_session(&mut waiter, "SET SESSION innodb_lock_wait_timeout = 1")
        .unwrap();
    engine
        .execute_with_session(&mut waiter, "SET SESSION autocommit = OFF")
        .unwrap();
    let blocked = engine
        .execute_with_session(&mut waiter, "UPDATE t SET v = 99 WHERE id = 2")
        .unwrap_err();
    assert_eq!(
        blocked.code(),
        1205,
        "ROLLBACK TO SAVEPOINT must retain the row-2 lock (got {:?})",
        blocked
    );

    // The row-2 change was still rolled back to the savepoint (v = 20).
    engine.execute_with_session(&mut owner, "COMMIT").unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut SessionContext::default(),
            "SELECT id, v FROM t ORDER BY id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Int(1), Value::Int(11)],
            vec![Value::Int(2), Value::Int(20)],
        ]
    );
}

// ---- M3: lock-timeout system variables ----

#[test]
fn innodb_lock_wait_timeout_round_trips_session_and_global() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(&mut session, "SET SESSION innodb_lock_wait_timeout = 7")
        .unwrap();
    assert_eq!(session.sysvars.innodb_lock_wait_timeout, Some(7));
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT @@session.innodb_lock_wait_timeout")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows, vec![vec![Value::String("7".into())]]);

    // lock_wait_timeout (MDL) is also a real, settable sysvar.
    engine
        .execute_with_session(&mut session, "SET SESSION lock_wait_timeout = 123")
        .unwrap();
    assert_eq!(session.sysvars.lock_wait_timeout, Some(123));
}

#[test]
fn session_innodb_lock_wait_timeout_drives_real_row_lock_timeout() {
    let dir = tempfile::tempdir().unwrap();
    // Default engine config.lock_wait is 30s; the session override must win so
    // the blocked waiter times out in ~1s rather than hanging for 30s.
    let engine = open_test_engine(dir.path());
    let mut owner = SessionContext::default();
    let mut waiter = SessionContext::default();

    engine
        .execute_with_session(&mut owner, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute_with_session(&mut owner, "INSERT INTO t VALUES (1, 10)")
        .unwrap();

    engine.execute_with_session(&mut owner, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut owner, "SELECT id FROM t WHERE id = 1 FOR UPDATE")
        .unwrap();

    engine
        .execute_with_session(&mut waiter, "SET SESSION innodb_lock_wait_timeout = 1")
        .unwrap();
    engine
        .execute_with_session(&mut waiter, "SET SESSION autocommit = OFF")
        .unwrap();

    let started = std::time::Instant::now();
    let blocked = engine
        .execute_with_session(&mut waiter, "UPDATE t SET v = 11 WHERE id = 1")
        .unwrap_err();
    let elapsed = started.elapsed();

    assert_eq!(
        blocked.code(),
        1205,
        "expected ER_LOCK_WAIT_TIMEOUT_EXCEEDED"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(10),
        "session timeout override should fire near 1s, took {:?}",
        elapsed
    );

    engine.execute_with_session(&mut owner, "ROLLBACK").unwrap();
}

#[test]
fn consistent_snapshot_ignored_with_warning_outside_repeatable_read() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(
        result.rows.iter().any(|r| {
            matches!(r.get(2), Some(Value::String(msg)) if msg.contains("CONSISTENT SNAPSHOT"))
        }),
        "non-RR CONSISTENT SNAPSHOT must raise a warning, got {:?}",
        result.rows
    );
    assert!(session
        .warnings
        .iter()
        .any(|warning| warning.code() == crate::error::catalog::WARN_OPTION_IGNORED.code().get()));
    engine.execute_with_session(&mut session, "COMMIT").unwrap();
}
