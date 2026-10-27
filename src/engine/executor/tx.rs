use super::*;
use crate::xa::{XaBranch, XaBranchState, Xid};
use std::sync::atomic::Ordering as AtomicOrdering;

#[derive(Default)]
struct LockingRead(bool);
impl crate::parser::visit::AstVisitor for LockingRead {
    fn enter_select(&mut self, select: &Select) {
        self.0 |= select.for_update || select.lock_clause.is_some();
    }
    fn walk_expr_children(&self, _: &Expr) -> bool {
        !self.0
    }
}

pub(super) fn select_has_locking_read(stmt: &Select) -> bool {
    if stmt.for_update || stmt.lock_clause.is_some() {
        return true;
    }
    let mut locks = LockingRead::default();
    crate::parser::visit::visit_select(&mut locks, stmt);
    locks.0
}

pub(super) fn query_has_locking_read(query: &crate::parser::QueryExpr) -> bool {
    let mut locks = LockingRead::default();
    crate::parser::visit::visit_query(&mut locks, query);
    locks.0
}

pub(super) fn can_use_read_only_select(session: &SessionContext, stmt: &Select) -> bool {
    session.autocommit
        && !session.explicit_txn
        && session.current_txn.is_none()
        && !select_has_locking_read(stmt)
}

pub(super) fn can_use_lazy_read_only_transaction_select(
    session: &SessionContext,
    stmt: &Select,
) -> bool {
    !select_has_locking_read(stmt)
        && session.effective_isolation() != IsolationLevel::Serializable
        && session.current_txn.is_none()
        && session.xa_branch.is_none()
        && (session.explicit_txn || !session.autocommit)
}

pub(super) fn can_use_current_transaction_read_select(
    session: &SessionContext,
    stmt: &Select,
) -> bool {
    !select_has_locking_read(stmt)
        && stmt.lock_clause.is_none()
        && session.effective_isolation() != IsolationLevel::Serializable
        && session.current_txn.is_some()
        && session.xa_branch.is_none()
}

/// Shared view policy for cached and uncached prepared reads. Keep the
/// statement guard alive through execution and RR's pin through transaction end.
pub(super) fn prepared_select_read_view(
    db: &Arc<Database>,
    session: &mut SessionContext,
    select: &Select,
) -> Result<(TxnView, Option<crate::storage::SnapshotGuard>), QueryError> {
    Ok(
        if tx::can_use_current_transaction_read_select(session, select) {
            tx::current_transaction_read_select_view(db, session)?
        } else if session.explicit_txn || !session.autocommit {
            let isolation = session.start_transaction_isolation();
            let pinned = if isolation.uses_repeatable_snapshot() {
                session.txn_snapshot_scn
            } else {
                None
            };
            if let Some(snapshot_scn) = pinned {
                // `txn_snapshot_guard` pins this exact RR view until
                // COMMIT/ROLLBACK, so constructing the immutable TxnView is
                // lock-free after the first consistent read.
                debug_assert!(session.txn_snapshot_guard.is_some());
                (
                    db.read_only_statement_view(isolation, Some(snapshot_scn)),
                    None,
                )
            } else {
                // Snapshot selection and initial registration are atomic with
                // respect to purge's sharded snapshot scan. With retention=0 a
                // purge between the old two calls could discard the exact
                // before-image this statement was about to read.
                let (txn, guard) = db.read_only_statement_view_with_guard(isolation, pinned);
                if !isolation.uses_repeatable_snapshot() {
                    session.txn_snapshot_scn = Some(txn.snapshot_scn);
                } else if session.txn_snapshot_scn.is_none() {
                    session.pin_transaction_snapshot(db, txn.snapshot_scn);
                }
                (txn, Some(guard))
            }
        } else {
            let isolation = session.autocommit_statement_isolation();
            let (txn, guard) = db.read_only_statement_view_with_guard(isolation, None);
            (txn, Some(guard))
        },
    )
}

pub(super) fn begin_explicit_txn(
    db: &Arc<Database>,
    session: &mut SessionContext,
) -> Result<(), QueryError> {
    session.explicit_table_locks.clear();
    db.release_explicit_table_mdl(session.connection_id as u64);
    if session.in_transaction() {
        commit_session_txn(db, session)?;
    }
    // The read view is established lazily by the first consistent read:
    // READ COMMITTED takes a fresh snapshot per statement; REPEATABLE READ
    // pins one transaction-wide on the first SELECT. Nothing is pinned at
    // BEGIN itself (MySQL pins at BEGIN only for WITH CONSISTENT SNAPSHOT).
    session.clear_transaction_snapshot();
    session.binlog_snapshot_status = None;
    session.start_transaction_isolation();
    session.advance_mdl_txn_generation();
    session.explicit_txn = true;
    // `SET TRANSACTION READ ONLY` (no SESSION/GLOBAL) wins for this one
    // transaction; falling back to whatever the session-level access
    // mode is. Once consumed, the next-txn slot is cleared so the
    // subsequent BEGIN returns to the session's normal mode. When the
    // slot is empty, leave `read_only_txn` alone — the
    // `transaction_read_only` sysvar backend writes it directly, so a
    // prior `SET SESSION TRANSACTION READ ONLY` is preserved.
    if let Some(read_only) = session.next_txn_read_only.take() {
        session.read_only_txn = read_only;
    }
    Ok(())
}

/// M1.1 - Begin a session-level read-only transaction.
pub(super) fn begin_explicit_read_only_txn(
    db: &Arc<Database>,
    session: &mut SessionContext,
) -> Result<(), QueryError> {
    session.explicit_table_locks.clear();
    db.release_explicit_table_mdl(session.connection_id as u64);
    if session.in_transaction() {
        commit_session_txn(db, session)?;
    }
    let isolation = session.start_transaction_isolation();
    let txn = db.begin_read_only_txn()?;
    session.set_current_txn(Some(txn.txn_id));
    session.advance_mdl_txn_generation();
    // READ ONLY is an access characteristic, not a request to freeze a read
    // view. RC refreshes it per statement; RR pins it on the first consistent
    // read. WITH CONSISTENT SNAPSHOT uses the separate eager-pin path below.
    session.clear_transaction_snapshot();
    session.binlog_snapshot_status = None;
    session.explicit_txn = true;
    session.read_only_txn = true;
    session.active_txn_isolation = Some(isolation);
    Ok(())
}

pub(super) fn begin_consistent_snapshot_txn(
    db: &Arc<Database>,
    session: &mut SessionContext,
) -> Result<(), QueryError> {
    session.explicit_table_locks.clear();
    db.release_explicit_table_mdl(session.connection_id as u64);
    if session.in_transaction() {
        commit_session_txn(db, session)?;
    }
    let isolation = session.start_transaction_isolation();
    // Capture a binlog position aligned with the snapshot when binary logging
    // is enabled (for `mysqldump --single-transaction --source-data` style
    // consistency). MySQL's WITH CONSISTENT SNAPSHOT does not itself require
    // binlog, so with logging off we still pin the read view — just without a
    // position to report.
    let binlog_available = crate::binlog::require_manager().is_ok();
    let _binlog_commit_gate = binlog_available.then(crate::binlog::commit_gate);
    if binlog_available {
        crate::binlog::drain_commits_for_snapshot()?;
        db.fence_binlog_snapshot_visibility()?;
    }
    let (snapshot, snapshot_guard) = db.read_only_statement_view_with_guard(isolation, None);
    let txn = db.begin_read_only_txn()?;
    let status = if binlog_available {
        crate::binlog::current_status_snapshot().ok().map(Arc::new)
    } else {
        None
    };
    session.set_current_txn(Some(txn.txn_id));
    session.advance_mdl_txn_generation();
    session.pin_transaction_snapshot_with_guard(snapshot.snapshot_scn, snapshot_guard);
    session.binlog_snapshot_status = status;
    session.explicit_txn = true;
    session.read_only_txn = true;
    session.active_txn_isolation = Some(isolation);
    Ok(())
}

/// `START TRANSACTION`/`BEGIN` with the MySQL 8.0 characteristic list.
/// Applies the access mode (`READ ONLY`/`READ WRITE`) and, when requested,
/// the `WITH CONSISTENT SNAPSHOT` snapshot pin (only effective at RR — at
/// other isolation levels it is ignored with a warning, matching MySQL).
pub(super) fn begin_transaction_with(
    db: &Arc<Database>,
    session: &mut SessionContext,
    read_only: Option<bool>,
    consistent_snapshot: bool,
) -> Result<(), QueryError> {
    // Resolve a new transaction only after ending the previous one. Active
    // characteristics belong to the old transaction; only CHAIN inherits them.
    if session.in_transaction() {
        commit_session_txn(db, session)?;
    }
    // An explicit access mode on START TRANSACTION wins for this one
    // transaction (overrides any pending SET TRANSACTION next-txn value).
    if let Some(ro) = read_only {
        session.next_txn_read_only = Some(ro);
    }
    // Effective access mode for this transaction: explicit clause > pending
    // SET TRANSACTION > session default (`transaction_read_only`).
    let effective_ro = session
        .next_txn_read_only
        .unwrap_or(session.sysvars.transaction_read_only);

    if consistent_snapshot {
        if session.effective_isolation() == IsolationLevel::RepeatableRead {
            if effective_ro {
                // Read-only consistent snapshot: pin the snapshot at BEGIN.
                session.next_txn_read_only = None;
                return begin_consistent_snapshot_txn(db, session);
            }
            begin_explicit_txn(db, session)?;
            let binlog_available = crate::binlog::require_manager().is_ok();
            let _binlog_commit_gate = binlog_available.then(crate::binlog::commit_gate);
            if binlog_available {
                if let Err(err) = crate::binlog::drain_commits_for_snapshot()
                    .and_then(|()| db.fence_binlog_snapshot_visibility())
                {
                    let _ = rollback_session_txn(db, session);
                    return Err(err);
                }
            }
            let (snapshot, guard) =
                db.read_only_statement_view_with_guard(session.effective_isolation(), None);
            session.pin_transaction_snapshot_with_guard(snapshot.snapshot_scn, guard);
            session.binlog_snapshot_status = binlog_available
                .then(|| crate::binlog::current_status_snapshot().ok())
                .flatten()
                .map(Arc::new);
            return Ok(());
        } else {
            // MySQL: WITH CONSISTENT SNAPSHOT has no effect outside RR and
            // raises a warning.
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::WARN_OPTION_IGNORED,
                "WITH CONSISTENT SNAPSHOT is only effective at the REPEATABLE READ isolation level"
                    .to_string(),
            );
        }
    }

    if effective_ro {
        // Eager read-only transaction fast path (skips WAL BeginTxn).
        session.next_txn_read_only = None;
        return begin_explicit_read_only_txn(db, session);
    }
    begin_explicit_txn(db, session)
}

/// COMMIT/ROLLBACK with the completion clause. `chain`/`release` are the
/// explicit statement clauses (`None` = defer to `completion_type`); when both
/// resolve, the transaction ends, then a `CHAIN` re-opens an identical
/// transaction and a `RELEASE` flags the connection for close.
pub(super) fn finish_transaction_with_completion(
    db: &Arc<Database>,
    session: &mut SessionContext,
    chain: Option<bool>,
    release: Option<bool>,
    is_rollback: bool,
) -> Result<(), QueryError> {
    // completion_type: 0=NO_CHAIN, 1=CHAIN, 2=RELEASE. Explicit clauses win.
    let ct = session.sysvars.completion_type;
    let do_chain = chain.unwrap_or(ct == 1);
    let do_release = release.unwrap_or(ct == 2);

    // Capture the ending transaction's characteristics for CHAIN before the
    // commit/rollback clears them.
    let chain_isolation = session.effective_isolation();
    let chain_read_only = if session.active_txn_isolation.is_none() {
        session.next_txn_read_only.unwrap_or(session.read_only_txn)
    } else {
        session.read_only_txn
    };

    if is_rollback {
        rollback_session_txn(db, session)?;
    } else {
        commit_session_txn(db, session)?;
    }

    if do_chain {
        // Re-open a new transaction with the same isolation level and access
        // mode as the one that just ended (MySQL AND CHAIN semantics).
        session.next_txn_isolation = Some(chain_isolation);
        session.next_txn_read_only = Some(chain_read_only);
        if chain_read_only {
            session.next_txn_read_only = None;
            begin_explicit_read_only_txn(db, session)?;
        } else {
            begin_explicit_txn(db, session)?;
        }
    }
    if do_release {
        session.close_after_response = true;
    }
    Ok(())
}

pub(super) fn commit_session_txn(
    db: &Arc<Database>,
    session: &mut SessionContext,
) -> Result<(), QueryError> {
    if let Some(txn_id) = session.current_txn {
        if db.is_txn_active(txn_id) {
            match db.commit_txn(txn_id) {
                Ok(_) => {}
                Err(err) if is_inactive_transaction_error(&err) && !db.is_txn_active(txn_id) => {}
                Err(err) => return Err(err),
            }
        }
    }
    cleanup_temp_tables_after_commit(db, session);
    session.created_temp_table_not_rollbackable = false;
    session.dropped_temp_table_not_rollbackable = false;
    session.set_current_txn(None);
    session.clear_transaction_snapshot();
    session.binlog_snapshot_status = None;
    session.explicit_txn = false;
    session.read_only_txn = session.sysvars.transaction_read_only;
    session.clear_active_transaction_isolation();
    // Explicit completion resets one-shot characteristics even when a prior
    // access-check failure never allocated or started a transaction.
    session.next_txn_isolation = None;
    session.next_txn_read_only = None;
    db.release_mdl_duration(session.connection_id as u64, MdlDuration::Transaction);
    session.advance_mdl_txn_generation();
    Ok(())
}

pub(super) fn rollback_session_txn(
    db: &Arc<Database>,
    session: &mut SessionContext,
) -> Result<(), QueryError> {
    if let Some(txn_id) = session.current_txn {
        if db.is_txn_active(txn_id) {
            match db.rollback_txn(txn_id) {
                Ok(()) => {}
                Err(err) if is_inactive_transaction_error(&err) && !db.is_txn_active(txn_id) => {}
                Err(err) => return Err(err),
            }
        }
    }
    rollback_temp_journal_to(db, session, 0)?;
    if session.created_temp_table_not_rollbackable {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_CREATED_TEMP_TABLE,
            "The creation of some temporary tables could not be rolled back.".to_string(),
        );
    }
    if session.dropped_temp_table_not_rollbackable {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_DROPPED_TEMP_TABLE,
            "Some temporary tables were dropped, but these operations could not be rolled back."
                .to_string(),
        );
    }
    session.created_temp_table_not_rollbackable = false;
    session.dropped_temp_table_not_rollbackable = false;
    session.set_current_txn(None);
    session.clear_transaction_snapshot();
    session.binlog_snapshot_status = None;
    session.explicit_txn = false;
    session.read_only_txn = session.sysvars.transaction_read_only;
    session.clear_active_transaction_isolation();
    // Explicit completion resets one-shot characteristics even when a prior
    // access-check failure never allocated or started a transaction.
    session.next_txn_isolation = None;
    session.next_txn_read_only = None;
    db.release_mdl_duration(session.connection_id as u64, MdlDuration::Transaction);
    session.advance_mdl_txn_generation();
    Ok(())
}

fn is_inactive_transaction_error(err: &QueryError) -> bool {
    err.is(crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK)
}

pub(super) fn implicit_commit_for_ddl(
    db: &Arc<Database>,
    session: &mut SessionContext,
) -> Result<(), QueryError> {
    if session.xa_branch.is_some() {
        return Err(xa_err(
            crate::error::catalog::ER_XAER_RMFAIL,
            "XAER_RMFAIL: DDL cannot implicitly commit an XA branch",
        ));
    }
    commit_session_txn(db, session)
}

/// The lock-free `session_statement_view` hint: an RR read-only
/// transaction's snapshot SCN after its read view has been pinned.
pub(super) fn session_read_only_snapshot(session: &SessionContext) -> Option<u64> {
    if session.read_only_txn
        && session
            .active_txn_isolation
            .is_some_and(IsolationLevel::uses_repeatable_snapshot)
        && session.txn_snapshot_guard.is_some()
    {
        session.txn_snapshot_scn
    } else {
        None
    }
}

pub(super) fn ensure_statement_txn(
    db: &Arc<Database>,
    session: &mut SessionContext,
) -> Result<TxnView, QueryError> {
    ensure_statement_txn_kind(db, session, /* dml = */ false)
}

/// DML-flavoured `ensure_statement_txn`. When the executor knows the
/// upcoming statement will write (INSERT / UPDATE / DELETE / REPLACE /
/// LOAD DATA), this picks `Database::begin_dml_txn`, which flags the
/// slot as a writer under the SAME TM.write that allocated it. The
/// vanilla path still works (`push_undo_with_flush` marks the writer
/// lazily on the first undo), but it costs one extra TM write per
/// autocommit DML statement — visible as ~2.4% lock wait under the
/// sysbench-prepare profile.
pub(super) fn ensure_statement_dml_txn(
    db: &Arc<Database>,
    session: &mut SessionContext,
) -> Result<TxnView, QueryError> {
    ensure_statement_txn_kind(db, session, /* dml = */ true)
}

fn ensure_statement_txn_kind(
    db: &Arc<Database>,
    session: &mut SessionContext,
    dml: bool,
) -> Result<TxnView, QueryError> {
    if session
        .xa_branch
        .as_ref()
        .is_some_and(|b| b.state == XaBranchState::Idle)
    {
        return Err(xa_err(
            crate::error::catalog::ER_XAER_RMFAIL,
            "XAER_RMFAIL: XA branch is IDLE",
        ));
    }
    let isolation = session.start_transaction_isolation();
    if dml && session.read_only_txn {
        return Err(error(
            crate::error::catalog::ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION,
            "Cannot execute statement in a READ ONLY transaction.",
        ));
    }
    let newly_started_dml = dml && session.current_txn.is_none();
    if session.current_txn.is_none() {
        let txn = if dml {
            db.begin_dml_txn()?
        } else {
            db.begin_txn()?
        };
        session.set_current_txn(Some(txn.txn_id));
        if !isolation.uses_repeatable_snapshot() {
            session.txn_snapshot_scn = Some(txn.snapshot_scn);
        }
    }
    let txn_id = session.current_txn.unwrap();
    if dml
        && !session.txn_runtime.as_ref().is_some_and(|runtime| {
            runtime.txn_id == txn_id && runtime.wal_buffer_ready.load(AtomicOrdering::Acquire)
        })
    {
        let runtime = session.ensure_txn_runtime(txn_id)?;
        // begin_dml_txn already initialized its WAL buffer. Existing read or
        // explicit transactions still need the canonical promotion checks.
        if !newly_started_dml {
            db.enable_txn_wal_buffering_for_write(txn_id)?;
        }
        runtime
            .wal_buffer_ready
            .store(true, AtomicOrdering::Release);
    }
    let mut view =
        db.session_statement_view(txn_id, isolation, session_read_only_snapshot(session));
    view.isolation = isolation;
    // B1 — propagate the session's connection id so commit/rollback
    // can donate any unused AUTO_INCREMENT tail to this connection's
    // idle pool instead of abandoning it.
    view.connection_id = session.connection_id;
    // READ COMMITTED advances the read view each statement. This path is a
    // current read (DML / locking read) which always observes the latest
    // committed version, so under REPEATABLE READ it must NOT advance the
    // transaction's fixed read view (pinned by the first consistent SELECT).
    if !isolation.uses_repeatable_snapshot() {
        session.txn_snapshot_scn = Some(view.snapshot_scn);
    }
    Ok(view)
}

pub(super) fn ensure_temp_statement_txn(
    db: &Arc<Database>,
    session: &mut SessionContext,
) -> Result<TxnView, QueryError> {
    let isolation = session.start_transaction_isolation();
    if session.current_txn.is_none() {
        let txn = db.begin_temp_txn()?;
        session.set_current_txn(Some(txn.txn_id));
        if !isolation.uses_repeatable_snapshot() {
            session.txn_snapshot_scn = Some(txn.snapshot_scn);
        }
    }
    let txn_id = session.current_txn.unwrap();
    let mut view =
        db.session_statement_view(txn_id, isolation, session_read_only_snapshot(session));
    view.isolation = isolation;
    // B1 — same session-id flow as `ensure_statement_txn`.
    view.connection_id = session.connection_id;
    if !isolation.uses_repeatable_snapshot() {
        session.txn_snapshot_scn = Some(view.snapshot_scn);
    }
    Ok(view)
}

pub(super) fn nonlocking_select_transaction_view(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
) -> TxnView {
    let ro_snapshot = session_read_only_snapshot(session);
    if txn.isolation.uses_repeatable_snapshot() {
        let mut view = db.session_statement_view(txn.txn_id, txn.isolation, ro_snapshot);
        view.connection_id = session.connection_id;
        if let Some(snapshot_scn) = session.txn_snapshot_scn {
            view.snapshot_scn = snapshot_scn;
        } else {
            session.pin_transaction_snapshot(db, view.snapshot_scn);
        }
        view
    } else if txn.isolation == IsolationLevel::ReadUncommitted {
        let mut view = db.session_statement_view(txn.txn_id, txn.isolation, ro_snapshot);
        view.connection_id = session.connection_id;
        view.dirty_read = true;
        view
    } else {
        txn
    }
}

pub(super) fn current_transaction_read_select_view(
    db: &Arc<Database>,
    session: &mut SessionContext,
) -> Result<(TxnView, Option<crate::storage::SnapshotGuard>), QueryError> {
    let txn = ensure_statement_txn(db, session)?;
    let isolation = txn.isolation;
    if isolation.uses_repeatable_snapshot() {
        if let Some(snapshot_scn) = session.txn_snapshot_scn {
            debug_assert!(session.txn_snapshot_guard.is_some());
            let mut view = db.session_statement_view(txn.txn_id, isolation, Some(snapshot_scn));
            view.connection_id = session.connection_id;
            return Ok((view, None));
        }
        let (mut view, guard) = db.session_statement_view_with_guard(
            txn.txn_id,
            isolation,
            None,
            session.read_only_txn,
        );
        view.connection_id = session.connection_id;
        session.pin_transaction_snapshot_with_guard(view.snapshot_scn, guard);
        return Ok((view, None));
    }

    let (mut view, guard) =
        db.session_statement_view_with_guard(txn.txn_id, isolation, None, session.read_only_txn);
    view.connection_id = session.connection_id;
    view.dirty_read = isolation == IsolationLevel::ReadUncommitted;
    session.txn_snapshot_scn = Some(view.snapshot_scn);
    Ok((view, Some(guard)))
}

pub(super) fn autocommit_read_only_view_with_guard(
    db: &Arc<Database>,
    session: &mut SessionContext,
) -> (TxnView, crate::storage::SnapshotGuard) {
    let isolation = session.autocommit_statement_isolation();
    db.read_only_statement_view_with_guard(isolation, None)
}

pub(super) fn lazy_read_only_transaction_view_with_guard(
    db: &Arc<Database>,
    session: &mut SessionContext,
) -> (TxnView, Option<crate::storage::SnapshotGuard>) {
    let isolation = session.start_transaction_isolation();
    if let Some(handle) = &session.cancel_handle {
        handle.session_handle_ref().note_lazy_transaction();
    }
    if isolation.uses_repeatable_snapshot() {
        if let Some(snapshot_scn) = session.txn_snapshot_scn {
            debug_assert!(session.txn_snapshot_guard.is_some());
            return (
                db.read_only_statement_view(isolation, Some(snapshot_scn)),
                None,
            );
        }
        let (txn, guard) = db.read_only_statement_view_with_guard(isolation, None);
        session.pin_transaction_snapshot_with_guard(txn.snapshot_scn, guard);
        return (txn, None);
    }
    let (txn, guard) = db.read_only_statement_view_with_guard(isolation, None);
    session.txn_snapshot_scn = Some(txn.snapshot_scn);
    (txn, Some(guard))
}

pub(super) fn atomically_guard_statement_view(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
) -> (TxnView, crate::storage::SnapshotGuard) {
    let pinned = session_read_only_snapshot(session);
    let (mut view, guard) = db.session_statement_view_with_guard(
        txn.txn_id,
        txn.isolation,
        pinned,
        txn.read_only || session.read_only_txn,
    );
    view.connection_id = session.connection_id;
    view.dirty_read = txn.isolation == IsolationLevel::ReadUncommitted;
    if !txn.isolation.uses_repeatable_snapshot() {
        session.txn_snapshot_scn = Some(view.snapshot_scn);
    }
    (view, guard)
}

pub(super) fn finish_statement_txn<T>(
    db: &Arc<Database>,
    session: &mut SessionContext,
    result: Result<T, QueryError>,
) -> Result<T, QueryError> {
    if session.autocommit && !session.explicit_txn {
        // Retain the owner until commit or compensation reaches a terminal
        // state. A pre-publication commit error must not orphan row/key locks.
        let txn_id = session.current_txn;
        let result = match result {
            Ok(value) => {
                let committed = txn_id.map_or(Ok(0), |id| db.commit_txn(id));
                match committed {
                    Ok(_) => {
                        cleanup_temp_tables_after_commit(db, session);
                        Ok(value)
                    }
                    Err(err) => {
                        // Published commits cannot be rolled back. Storage
                        // quarantines failures of their durability barrier.
                        if let Some(id) = txn_id.filter(|id| db.is_txn_active(*id)) {
                            db.rollback_txn(id)?;
                            rollback_temp_journal_to(db, session, 0)?;
                        }
                        Err(err)
                    }
                }
            }
            Err(err) => {
                if let Some(txn_id) = txn_id.filter(|id| db.is_txn_active(*id)) {
                    db.rollback_txn(txn_id)?;
                }
                rollback_temp_journal_to(db, session, 0)?;
                Err(err)
            }
        };
        session.set_current_txn(None);
        session.clear_transaction_snapshot();
        session.binlog_snapshot_status = None;
        session.clear_active_transaction_isolation();
        session.read_only_txn = session.sysvars.transaction_read_only;
        result
    } else {
        result
    }
}

/// Shared statement lifetime after the entry has selected its transaction kind.
/// Guards remain alive through execution, rollback and autocommit finalization.
#[inline]
fn execute_statement_transaction<F>(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    execute: F,
) -> Result<QueryResponse, QueryError>
where
    F: FnOnce(&Arc<Database>, &mut SessionContext, TxnView) -> Result<QueryResponse, QueryError>,
{
    let txn_runtime = session.ensure_txn_runtime(txn.txn_id)?;
    let _statement_guard =
        (Arc::strong_count(&txn_runtime) > 2).then(|| txn_runtime.enter_statement());
    let (txn, _snapshot_guard) = atomically_guard_statement_view(db, session, txn);
    let temp_savepoint = session.temp_table_journal.len();
    let savepoint = (!session.autocommit || session.explicit_txn)
        .then(|| db.statement_savepoint(txn.txn_id))
        .transpose()?;
    let result = execute(db, session, txn);
    let result = match (result, savepoint) {
        (Err(err), Some(savepoint)) => {
            let _ = rollback_temp_journal_to(db, session, temp_savepoint);
            if is_lock_wait_pending(&err) {
                let _ = db.rollback_to_statement_savepoint_preserving_wait(txn.txn_id, savepoint);
            } else {
                let _ = db.rollback_to_statement_savepoint(txn.txn_id, savepoint);
            }
            Err(err)
        }
        (Err(err), None) => {
            let _ = rollback_temp_journal_to(db, session, temp_savepoint);
            Err(err)
        }
        (result, _) => result,
    };
    finish_statement_txn(db, session, result)
}

pub(super) fn execute_in_transaction_with_session<F>(
    db: &Arc<Database>,
    session: &mut SessionContext,
    execute: F,
) -> Result<QueryResponse, QueryError>
where
    F: FnOnce(&Arc<Database>, &mut SessionContext, TxnView) -> Result<QueryResponse, QueryError>,
{
    let txn = ensure_statement_txn(db, session)?;
    execute_statement_transaction(db, session, txn, execute)
}

pub(super) fn execute_temp_in_transaction_with_session<F>(
    db: &Arc<Database>,
    session: &mut SessionContext,
    execute: F,
) -> Result<QueryResponse, QueryError>
where
    F: FnOnce(&Arc<Database>, &mut SessionContext, TxnView) -> Result<QueryResponse, QueryError>,
{
    session.note_write_intent();
    let txn = ensure_temp_statement_txn(db, session)?;
    execute_statement_transaction(db, session, txn, execute)
}

/// DML-flavoured `execute_in_transaction_with_session`. Identical to
/// the general path except that the implicit autocommit txn it opens
/// (when there is no explicit one in flight) goes through
/// [`Database::begin_dml_txn`], which flags the slot as a writer
/// under the same `TransactionManager` write lock that allocated it.
/// Skipping the lazy `mark_writer` shaves one TM.write per autocommit
/// INSERT — visible savings on concurrent bulk loaders.
pub(super) fn execute_dml_in_transaction_with_session<F>(
    db: &Arc<Database>,
    session: &mut SessionContext,
    execute: F,
) -> Result<QueryResponse, QueryError>
where
    F: FnOnce(&Arc<Database>, &mut SessionContext, TxnView) -> Result<QueryResponse, QueryError>,
{
    // Failed writes still classify the session as a read-write workload.
    session.note_write_intent();
    let txn = ensure_statement_dml_txn(db, session)?;
    execute_statement_transaction(db, session, txn, execute)
}

/// Transaction wrapper for a compiled exact locking point read attempted as
/// an async-worker scheduling probe. The point-read executor performs no
/// mutation before its sole row-lock acquire, and the nonblocking acquire does
/// not enqueue waiter state. Its internal `LockWouldBlock` marker can therefore
/// return directly for blocking-pool replay. User-visible errors and normal
/// blocking execution retain the ordinary statement-savepoint semantics.
pub(super) fn execute_retryable_locking_point_read_with_session<F>(
    db: &Arc<Database>,
    session: &mut SessionContext,
    execute: F,
) -> Result<QueryResponse, QueryError>
where
    F: FnOnce(&Arc<Database>, &mut SessionContext, TxnView) -> Result<QueryResponse, QueryError>,
{
    let txn = ensure_statement_txn(db, session)?;
    let txn_runtime = session.ensure_txn_runtime(txn.txn_id)?;
    let _statement_guard =
        (Arc::strong_count(&txn_runtime) > 2).then(|| txn_runtime.enter_statement());
    let (txn, _snapshot_guard) = atomically_guard_statement_view(db, session, txn);
    let temp_savepoint = session.temp_table_journal.len();
    let savepoint = (!session.autocommit || session.explicit_txn)
        .then(|| db.locking_read_lock_watermark(txn.txn_id));
    let result = execute(db, session, txn);
    if result
        .as_ref()
        .err()
        .is_some_and(crate::storage::is_lock_would_block_error)
    {
        debug_assert_eq!(session.temp_table_journal.len(), temp_savepoint);
        return result;
    }
    let result = match (result, savepoint) {
        (Err(err), Some(watermark)) => {
            let savepoint = db.materialize_locking_read_savepoint(txn.txn_id, watermark)?;
            let _ = rollback_temp_journal_to(db, session, temp_savepoint);
            if is_lock_wait_pending(&err) {
                let _ = db.rollback_to_statement_savepoint_preserving_wait(txn.txn_id, savepoint);
            } else {
                let _ = db.rollback_to_statement_savepoint(txn.txn_id, savepoint);
            }
            Err(err)
        }
        (Err(err), None) => {
            let _ = rollback_temp_journal_to(db, session, temp_savepoint);
            Err(err)
        }
        (result, _) => result,
    };
    finish_statement_txn(db, session, result)
}

/// Execute an owning statement that invokes a stored function or trigger with
/// write side effects as one atomic unit.
///
/// A read-shaped outer statement normally has no transaction of its own. Its
/// nested routine DML would therefore autocommit one statement at a time,
/// leaking earlier writes when a later routine statement, duplicate key, or
/// SIGNAL fails. Temporarily mark an autocommit session as an explicit
/// transaction so every nested DML reuses the transaction opened here, then
/// commit or roll it back at the real outer statement boundary. Existing user
/// transactions retain their state and use a statement savepoint.
pub(super) fn execute_atomic_routine_statement<T, F>(
    db: &Arc<Database>,
    session: &mut SessionContext,
    execute: F,
) -> Result<T, QueryError>
where
    F: FnOnce(&Arc<Database>, &mut SessionContext) -> Result<T, QueryError>,
{
    let txn = ensure_statement_dml_txn(db, session)?;
    let (txn, _snapshot_guard) = atomically_guard_statement_view(db, session, txn);
    let temp_savepoint = session.temp_table_journal.len();
    let user_transaction = !session.autocommit || session.explicit_txn;
    let savepoint = user_transaction
        .then(|| db.statement_savepoint(txn.txn_id))
        .transpose()?;
    let held_autocommit = session.autocommit && !session.explicit_txn;
    if held_autocommit {
        session.explicit_txn = true;
    }
    session.routine_atomic_depth += 1;
    let result = execute(db, session);
    session.routine_atomic_depth -= 1;
    if held_autocommit {
        session.explicit_txn = false;
    }
    let result = match (result, savepoint) {
        (Err(err), Some(savepoint)) => {
            let _ = rollback_temp_journal_to(db, session, temp_savepoint);
            if is_lock_wait_pending(&err) {
                let _ = db.rollback_to_statement_savepoint_preserving_wait(txn.txn_id, savepoint);
            } else {
                let _ = db.rollback_to_statement_savepoint(txn.txn_id, savepoint);
            }
            Err(err)
        }
        (Err(err), None) => {
            let _ = rollback_temp_journal_to(db, session, temp_savepoint);
            Err(err)
        }
        (result, _) => result,
    };
    let result = finish_statement_txn(db, session, result);
    if held_autocommit {
        // The temporary explicit transaction acquired transaction-duration MDL,
        // including on failure. Ordinary autocommit statements never need this
        // extra manager visit. Invalidate the lazy-transaction memo as well.
        db.release_mdl_duration(session.connection_id as u64, MdlDuration::Transaction);
        session.advance_mdl_txn_generation();
    }
    result
}

pub(super) fn rollback_temp_journal_to(
    db: &Arc<Database>,
    session: &mut SessionContext,
    len: usize,
) -> Result<(), QueryError> {
    while session.temp_table_journal.len() > len {
        let entry = session
            .temp_table_journal
            .pop()
            .expect("journal length checked");
        match entry {
            crate::engine::api::TempTableJournalEntry::Create {
                visible_key,
                internal_key,
            } => {
                if session
                    .temp_tables
                    .get(&visible_key)
                    .is_some_and(|table| table.internal_key == internal_key)
                {
                    session.temp_tables.remove(&visible_key);
                }
                db.drop_temp_table(&internal_key)?;
            }
            crate::engine::api::TempTableJournalEntry::Drop {
                visible_key,
                internal_key,
                on_commit,
            } => {
                session.add_temp_table(visible_key, internal_key, on_commit);
            }
        }
    }
    Ok(())
}

fn cleanup_temp_tables_after_commit(db: &Arc<Database>, session: &mut SessionContext) {
    // Durable commit has succeeded. Cleanup failure cannot be reported as a
    // transaction failure: clients could retry already committed writes.
    if let Err(err) = commit_temp_tables(db, session) {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_GET_ERRNO,
            format!(
                "Transaction committed; temporary table cleanup failed: {}",
                err.message()
            ),
        );
    }
}

pub(super) fn commit_temp_tables(
    db: &Arc<Database>,
    session: &mut SessionContext,
) -> Result<(), QueryError> {
    let dropped = session
        .temp_table_journal
        .iter()
        .filter_map(|entry| match entry {
            crate::engine::api::TempTableJournalEntry::Drop { internal_key, .. } => {
                Some(internal_key.clone())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut first_error = None;
    for internal_key in dropped {
        if let Err(err) = db.drop_temp_table(&internal_key) {
            first_error.get_or_insert(err);
        }
    }
    session.temp_table_journal.clear();

    let actions = session
        .temp_tables
        .iter()
        .map(|(visible, table)| (visible.clone(), table.internal_key.clone(), table.on_commit))
        .collect::<Vec<_>>();
    for (visible_key, internal_key, on_commit) in actions {
        match on_commit {
            crate::parser::TempTableOnCommit::PreserveRows => {}
            crate::parser::TempTableOnCommit::DeleteRows => {
                if let Err(err) = db.truncate_temp_table(&internal_key) {
                    // A partially truncated table cannot safely be reused.
                    session.temp_tables.remove(&visible_key);
                    let _ = db.drop_temp_table(&internal_key);
                    first_error.get_or_insert(err);
                }
            }
            crate::parser::TempTableOnCommit::Drop => {
                session.temp_tables.remove(&visible_key);
                if let Err(err) = db.drop_temp_table(&internal_key) {
                    first_error.get_or_insert(err);
                }
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

pub(super) fn close_temp_tables(
    db: &Arc<Database>,
    session: &mut SessionContext,
) -> Result<(), QueryError> {
    let internal_keys = session
        .temp_tables
        .values()
        .map(|table| table.internal_key.clone())
        .collect::<Vec<_>>();
    session.temp_tables.clear();
    session.temp_table_journal.clear();
    session.created_temp_table_not_rollbackable = false;
    session.dropped_temp_table_not_rollbackable = false;
    let mut first_error = None;
    for internal_key in internal_keys {
        if let Err(error) = db.drop_temp_table(&internal_key) {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

// ───────────────────────── XA / distributed transactions ─────────────────────────

fn xa_err(spec: crate::error::ErrorSpec, message: impl Into<String>) -> QueryError {
    spec.error(message)
}

fn xa_outside_msg(action: &str) -> String {
    format!(
        "XAER_RMFAIL: {} is not allowed while an XA branch is bound",
        action
    )
}

/// `Begin/Commit/Rollback` and friends must error out while a
/// session holds an ACTIVE or IDLE XA branch. Called from dispatch before
/// the normal local-transaction handlers run.
pub(super) fn ensure_no_xa_branch_for_local_txn(
    session: &SessionContext,
    action: &str,
) -> Result<(), QueryError> {
    if session.xa_branch.is_some() {
        return Err(xa_err(
            crate::error::catalog::ER_XAER_RMFAIL,
            xa_outside_msg(action),
        ));
    }
    Ok(())
}

/// Savepoints operate on the branch transaction only while it is ACTIVE.
pub(super) fn ensure_xa_savepoint_allowed(session: &SessionContext) -> Result<(), QueryError> {
    if session
        .xa_branch
        .as_ref()
        .is_some_and(|b| b.state != XaBranchState::Active)
    {
        return Err(xa_err(
            crate::error::catalog::ER_XAER_RMFAIL,
            "XAER_RMFAIL: XA branch is not ACTIVE",
        ));
    }
    Ok(())
}

pub(super) fn xa_start(
    db: &Arc<Database>,
    session: &mut SessionContext,
    xid: Xid,
) -> Result<(), QueryError> {
    if !session.redo_enabled {
        return Err(error(
            crate::error::catalog::ER_INNODB_REDO_DISABLED,
            "XA transactions require mydb_redo_enabled=ON",
        ));
    }
    if session.xa_branch.is_some() {
        return Err(xa_err(
            crate::error::catalog::ER_XAER_RMFAIL,
            "XAER_RMFAIL: another XA branch is already in this session",
        ));
    }
    if db.prepared_xa_contains(&xid) {
        return Err(xa_err(
            crate::error::catalog::ER_XAER_DUPID,
            "XAER_DUPID: an XA branch with the same XID is already PREPARED",
        ));
    }
    // autocommit=0 alone is not an active transaction. Never commit local
    // work (including a lazy consistent read) when entering XA.
    if session.current_txn.is_some()
        || session.explicit_txn
        || session.active_txn_isolation.is_some()
    {
        return Err(xa_err(
            crate::error::catalog::ER_XAER_OUTSIDE,
            "XAER_OUTSIDE: some work is done outside the transaction",
        ));
    }
    session.start_transaction_isolation();
    let txn = db.begin_txn()?;
    if let Err(err) = db.reserve_xa(&xid, txn.txn_id) {
        db.rollback_txn(txn.txn_id)?;
        session.clear_active_transaction_isolation();
        return Err(err);
    }
    session.set_current_txn(Some(txn.txn_id));
    session.clear_transaction_snapshot();
    session.binlog_snapshot_status = None;
    session.explicit_txn = true;
    session.xa_branch = Some(XaBranch {
        xid,
        state: XaBranchState::Active,
        txn_id: txn.txn_id,
    });
    Ok(())
}

pub(super) fn xa_end(
    _db: &Arc<Database>,
    session: &mut SessionContext,
    xid: Xid,
) -> Result<(), QueryError> {
    let branch = session.xa_branch.as_mut().ok_or_else(|| {
        xa_err(
            crate::error::catalog::ER_XAER_RMFAIL,
            "XAER_RMFAIL: no XA branch is active in this session",
        )
    })?;
    if branch.xid != xid {
        return Err(xa_err(
            crate::error::catalog::ER_XAER_NOTA,
            "XAER_NOTA: XID does not match the session's active XA branch",
        ));
    }
    if branch.state != XaBranchState::Active {
        return Err(xa_err(
            crate::error::catalog::ER_XAER_RMFAIL,
            "XAER_RMFAIL: XA branch is not in the ACTIVE state",
        ));
    }
    branch.state = XaBranchState::Idle;
    // Detach `current_txn`: subsequent statements in this session won't
    // implicitly target the XA branch's txn until XA PREPARE / COMMIT.
    session.set_current_txn(None);
    session.clear_transaction_snapshot();
    Ok(())
}

pub(super) fn xa_prepare(
    db: &Arc<Database>,
    session: &mut SessionContext,
    xid: Xid,
) -> Result<(), QueryError> {
    let Some(branch) = session.xa_branch.as_ref() else {
        return Err(xa_err(
            crate::error::catalog::ER_XAER_RMFAIL,
            "XAER_RMFAIL: no XA branch is bound to this session",
        ));
    };
    if branch.xid != xid {
        return Err(xa_err(
            crate::error::catalog::ER_XAER_NOTA,
            "XAER_NOTA: XID does not match the session's XA branch",
        ));
    }
    if branch.state != XaBranchState::Idle {
        return Err(xa_err(
            crate::error::catalog::ER_XAER_RMFAIL,
            "XAER_RMFAIL: XA PREPARE requires the branch to be in IDLE state (call XA END first)",
        ));
    }
    let txn_id = branch.txn_id;
    let owner_id = session.connection_id as u64;
    db.prepare_xa(txn_id, owner_id, xid)?;
    db.transfer_transaction_mdl_to_prepared(owner_id, txn_id);
    session.advance_mdl_txn_generation();
    session.xa_branch = None;
    session.explicit_txn = false;
    session.read_only_txn = session.sysvars.transaction_read_only;
    session.clear_active_transaction_isolation();
    session.clear_transaction_snapshot();
    session.binlog_snapshot_status = None;
    Ok(())
}

pub(super) fn xa_commit(
    db: &Arc<Database>,
    session: &mut SessionContext,
    xid: Xid,
    one_phase: bool,
) -> Result<(), QueryError> {
    if session
        .xa_branch
        .as_ref()
        .is_some_and(|branch| branch.xid != xid)
    {
        return Err(xa_err(
            crate::error::catalog::ER_XAER_RMFAIL,
            "XAER_RMFAIL: another branch is bound to this session",
        ));
    }
    if one_phase {
        if session.xa_branch.is_none() && db.prepared_xa_contains(&xid) {
            return Err(xa_err(
                crate::error::catalog::ER_XAER_RMFAIL,
                "XAER_RMFAIL: XA COMMIT ONE PHASE requires an IDLE branch; the branch is PREPARED",
            ));
        }
        let branch = session.xa_branch.as_ref().ok_or_else(|| {
            xa_err(
                crate::error::catalog::ER_XAER_NOTA,
                "XAER_NOTA: no XA branch is bound to this session for ONE PHASE commit",
            )
        })?;
        if branch.xid != xid {
            return Err(xa_err(
                crate::error::catalog::ER_XAER_NOTA,
                "XAER_NOTA: XID does not match the session's XA branch",
            ));
        }
        if branch.state != XaBranchState::Idle {
            return Err(xa_err(crate::error::catalog::ER_XAER_RMFAIL,
                "XAER_RMFAIL: XA COMMIT ONE PHASE requires the branch to be IDLE (call XA END first)",
            ));
        }
        let txn_id = branch.txn_id;
        // Re-attach so commit_session_txn drives the commit.
        session.set_current_txn(Some(txn_id));
        let result = commit_session_txn(db, session);
        if result.is_ok() {
            db.release_xa(&xid, txn_id);
            session.xa_branch = None;
        }
        return result;
    }
    // Two-phase XA COMMIT: branch must be PREPARED (detached from session).
    if session.xa_branch.is_some() {
        return Err(xa_err(
            crate::error::catalog::ER_XAER_RMFAIL,
            "XAER_RMFAIL: branch is still bound to this session; PREPARE it first or use ONE PHASE",
        ));
    }
    db.commit_prepared(&xid)?;
    Ok(())
}

pub(super) fn xa_rollback(
    db: &Arc<Database>,
    session: &mut SessionContext,
    xid: Xid,
) -> Result<(), QueryError> {
    if let Some(branch) = session.xa_branch.as_ref() {
        if branch.xid == xid {
            let txn_id = branch.txn_id;
            // Always allow ROLLBACK from ACTIVE or IDLE — re-attach so the
            // existing rollback path runs.
            session.set_current_txn(Some(txn_id));
            let result = rollback_session_txn(db, session);
            if result.is_ok() {
                db.release_xa(&xid, txn_id);
                session.xa_branch = None;
            }
            return result;
        }
    }
    if session.xa_branch.is_some() {
        return Err(xa_err(
            crate::error::catalog::ER_XAER_RMFAIL,
            "XAER_RMFAIL: another branch is bound to this session",
        ));
    }
    // Detached: must be PREPARED in the engine-global registry.
    db.rollback_prepared(&xid)?;
    Ok(())
}

pub(super) fn xa_recover_result(db: &Arc<Database>, convert_xid: bool) -> QueryResponse {
    let columns = vec![
        Column {
            name: "formatID".to_string(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        },
        Column {
            name: "gtrid_length".to_string(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        },
        Column {
            name: "bqual_length".to_string(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        },
        Column {
            name: "data".to_string(),
            column_type: ColumnType::VarString,
            source: Default::default(),
        },
    ];
    let rows = db
        .list_prepared_xa()
        .into_iter()
        .map(|p| {
            vec![
                Value::Int(p.xid.format_id),
                Value::Int(p.xid.gtrid_len()),
                Value::Int(p.xid.bqual_len()),
                Value::String(p.xid.render_data(convert_xid)),
            ]
        })
        .collect();
    QueryResponse::ResultSet(ResultSet { columns, rows })
}

pub(super) fn select_lock_wait_timeout_guard(
    session: &SessionContext,
    select: &Select,
) -> Option<crate::storage::LockWaitTimeoutGuard> {
    (select.for_update
        || select.lock_clause.is_some()
        || session.effective_isolation() == IsolationLevel::Serializable)
        .then(|| {
            crate::storage::LockWaitTimeoutGuard::new(Some(
                session
                    .sysvars
                    .innodb_lock_wait_timeout
                    .unwrap_or_else(crate::engine::api::global_innodb_lock_wait_timeout),
            ))
        })
}

pub(super) fn execute_select_in_transaction(
    db: &Arc<Database>,
    session: &mut SessionContext,
    stmt: Select,
    registry: &crate::engine::session_registry::SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let _row_lock_timeout = select_lock_wait_timeout_guard(session, &stmt);
    execute_in_transaction_with_session(db, session, |db, session, txn| {
        if !stmt.for_update && txn.isolation == IsolationLevel::Serializable {
            acquire_serializable_select_table_locks(db, session, txn, &stmt)?;
        }
        if let Some(response) = super::locking_join::execute(db, txn, &stmt, registry)? {
            return Ok(response);
        }
        let locked_row_ids = lock_select_rows_for_update(db, txn, &stmt)?;
        let ro_snapshot = session_read_only_snapshot(session);
        let txn = if stmt.for_update || txn.isolation == IsolationLevel::Serializable {
            db.session_statement_view(txn.txn_id, txn.isolation, ro_snapshot)
        } else {
            nonlocking_select_transaction_view(db, session, txn)
        };
        if let Some(row_ids) = locked_row_ids {
            if let Some(response) = execute_locked_single_table_select(db, txn, &stmt, row_ids)? {
                return Ok(response);
            }
        }
        execute_select(db, txn, stmt, registry)
    })
}

/// Borrowed-AST twin of [`execute_select_in_transaction`] for the
/// prepared-DML path (`FOR UPDATE` selects that don't qualify as
/// locking point reads — ORDER BY / LIMIT / non-unique lookups). Every
/// step up to and including the locked single-table executor only needs
/// `&Select`; the cached AST is cloned only when falling through to the
/// general `execute_select`, so the common per-execute `Select::clone`
/// disappears.
pub(super) fn execute_select_in_transaction_borrowed(
    db: &Arc<Database>,
    session: &mut SessionContext,
    stmt: &Select,
    registry: &crate::engine::session_registry::SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let _row_lock_timeout = select_lock_wait_timeout_guard(session, &stmt);
    execute_in_transaction_with_session(db, session, |db, session, txn| {
        if !stmt.for_update && txn.isolation == IsolationLevel::Serializable {
            acquire_serializable_select_table_locks(db, session, txn, stmt)?;
        }
        if let Some(response) = super::locking_join::execute(db, txn, stmt, registry)? {
            return Ok(response);
        }
        let locked_row_ids = lock_select_rows_for_update(db, txn, stmt)?;
        let ro_snapshot = session_read_only_snapshot(session);
        let txn = if stmt.for_update || txn.isolation == IsolationLevel::Serializable {
            db.session_statement_view(txn.txn_id, txn.isolation, ro_snapshot)
        } else {
            nonlocking_select_transaction_view(db, session, txn)
        };
        if let Some(row_ids) = locked_row_ids {
            if let Some(response) = execute_locked_single_table_select(db, txn, stmt, row_ids)? {
                return Ok(response);
            }
        }
        execute_select(db, txn, stmt.clone(), registry)
    })
}
