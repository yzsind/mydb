//! Compile-once / late-bind fast path for prepared INSERT / UPDATE / DELETE
//! and narrow locking reads.
//!
//! The legacy DML execute path runs every COM_STMT_EXECUTE through eight
//! dispatch wrappers
//! (`execute_prepared_statement` →
//! `with_query_hint` →
//! `execute_qualified_parsed_statement` →
//! `execute_statement_with_lock_wait_mode` →
//! `execute_statement_with_qualification` →
//! `execute_mdl_protected_statement` →
//! `execute_qualified_statement` →
//! `execute_insert/update/delete`).
//! Each step rebuilds state the prepared handle already proved is stable:
//! the per-statement `session.clone()` and `Statement::clone()` in
//! `execute_statement_with_lock_wait_mode`, the per-execute privilege +
//! MDL request vectors, the view / temp-table / pfs / sys probes in
//! `execute_statement`, the `qualify_statement_tables` walk, etc.
//!
//! For the simple-shape DML that drives sysbench (`UPDATE t SET k=k+1
//! WHERE id=?`, `DELETE FROM t WHERE id=?`, `INSERT INTO t (...) VALUES
//! (?, ?, ?, ?)`) and TPC-C's prepared `SELECT ... FOR UPDATE` point reads,
//! none of that work changes between executes — only the bound parameters
//! do. This module memoises the unchanging bits on a `PreparedDmlPlan` held
//! by the owning `PreparedStatement`. Reuse skips the wrappers and goes
//! straight to the same `execute_insert / execute_update / execute_delete /
//! execute_select_in_transaction` functions the legacy path eventually
//! reaches, with the cached AST borrowed (no clone) and the session borrowed
//! mutably only for the txn / savepoint / lock-wait retry machinery — never
//! deep-cloned.
//!
//! The cached path is *differentially identical* to the legacy
//! per-execute pipeline for the subset it accepts: any statement /
//! session shape it cannot prove equivalent is rejected at compile time
//! and falls back to the legacy clone+dispatch path. Eligibility is
//! intentionally narrow:
//!
//!   * single-table INSERT / UPDATE / DELETE only — joins, multi-table
//!     UPDATE/DELETE, partition clauses, view-rewritten targets,
//!     INSERT … SELECT, IGNORE / REPLACE / ON DUPLICATE KEY UPDATE,
//!     subqueries in the WHERE / assignment / row exprs are rejected.
//!   * the target must be a real base table (not a view, not a temp
//!     table, not in `performance_schema` / `mysql` / `sys` / any other
//!     system database).
//!   * the table must not have BEFORE/AFTER triggers for the relevant
//!     event, must not be partitioned, and (for UPDATE/DELETE) must not
//!     declare generated columns the assignment list could touch.
//!   * INSERT VALUES rows must not contain subqueries or `DEFAULT(...)`
//!     references that need a per-row catalog lookup beyond what
//!     `bind_column_defaults` resolves at compile time.
//!
//! Any DDL bumps the engine plan-cache epoch (same source-of-truth
//! `PreparedSelectMemo` uses), which invalidates the memo and forces the
//! first execute after the DDL to recompile.

use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::engine::api::{
    PreparedDmlPlan, PreparedLockingPointRead, PreparedPointAssignment, PreparedPointUpdate,
    PreparedPointWrite, PreparedPointWriteKind, QueryResponse, SessionContext,
};
use crate::engine::executor::dispatch::{
    acquire_prepared_statement_mdl, authorize_prepared_statement_with_epoch,
    prepared_required_privileges, prepared_statement_mdl_requests, statement_refs_temp_table,
    PreparedMdlGuard,
};
use crate::engine::executor::indexes::{
    best_index_access, index_access_covers_all_conjuncts, index_access_covers_all_conjuncts_memo,
    EngineIndexAccess,
};
use crate::engine::executor::select::{simple_projection_indexes, single_table_result_columns};
use crate::engine::executor::tx::{
    execute_dml_in_transaction_with_session, execute_retryable_locking_point_read_with_session,
    execute_select_in_transaction_borrowed,
};
use crate::engine::executor::{dml as dml_exec, insert as insert_exec, triggers};
use crate::engine::session_registry::SessionRegistry;
use crate::parser::{
    BinaryOp, Delete, Expr, Insert, InsertSource, LockClauseMode, LockWaitMode, Select, SelectItem,
    Statement, Update,
};
use crate::storage::{
    is_system_database, split_table_name, Database, IndexKey, KeyPart, MdlDuration, TxnView,
};
use crate::types::{QueryError, Value};

static SPARSE_POINT_BATCH_HITS: AtomicU64 = AtomicU64::new(0);

pub(super) fn sparse_point_batch_hits() -> u64 {
    SPARSE_POINT_BATCH_HITS.load(Ordering::Relaxed)
}

#[cfg(test)]
static HOMOGENEOUS_POINT_UPDATE_BATCHES: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static HOMOGENEOUS_POINT_UPDATE_LOGICAL_ROWS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static HOMOGENEOUS_POINT_UPDATE_PHYSICAL_ROWS: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
pub(super) fn homogeneous_point_update_batch_stats_for_test() -> (u64, u64, u64) {
    (
        HOMOGENEOUS_POINT_UPDATE_BATCHES.load(Ordering::Relaxed),
        HOMOGENEOUS_POINT_UPDATE_LOGICAL_ROWS.load(Ordering::Relaxed),
        HOMOGENEOUS_POINT_UPDATE_PHYSICAL_ROWS.load(Ordering::Relaxed),
    )
}

/// Structural eligibility for the cached DML path. Returns the qualified
/// table name when the statement is in scope, `None` otherwise. The
/// caller stores `None` results as `PreparedDmlState::Ineligible` so
/// later executes don't reclassify.
fn eligible_dml(statement: &Statement) -> Option<&str> {
    match statement {
        Statement::Insert(insert) => eligible_insert(insert),
        Statement::Update(update) => eligible_update(update),
        Statement::Delete(delete) => eligible_delete(delete),
        Statement::Select(select) => eligible_locking_select(select),
        _ => None,
    }
}

#[inline]
fn is_simple_single_table_select(stmt: &Select) -> bool {
    stmt.joins.is_empty()
        && stmt.from.len() == 1
        && stmt.derived.is_none()
        && stmt.values.is_none()
        && stmt.json_table.is_none()
        && !stmt.distinct
        && stmt.group_by.is_empty()
        && stmt.having.is_none()
        && stmt.order_by.is_empty()
        && stmt.limit.is_none()
        && stmt.offset.is_none()
}

fn eligible_locking_select(stmt: &Select) -> Option<&str> {
    if !stmt.for_update && stmt.lock_clause.is_none() {
        return None;
    }
    if !is_simple_single_table_select(stmt) {
        return None;
    }
    let primary = stmt.from.first()?;
    if !primary.partitions.is_empty() {
        return None;
    }
    if stmt
        .selection
        .as_ref()
        .is_some_and(|expr| expr_contains_subquery(expr))
    {
        return None;
    }
    for item in &stmt.projection {
        match item {
            SelectItem::Expr { expr, .. } if !expr_contains_subquery(expr) => {}
            _ => return None,
        }
    }
    stmt.table.as_deref()
}

fn eligible_insert(stmt: &Insert) -> Option<&str> {
    // Restrict to plain INSERT against a base table. Anything that needs
    // conflict handling (IGNORE / REPLACE / ON DUPLICATE KEY UPDATE) walks a
    // different storage path; those fall back to the legacy dispatcher.
    if stmt.ignore || stmt.replace || stmt.on_duplicate_key_update.is_some() {
        return None;
    }
    if !stmt.partitions.is_empty() {
        return None;
    }
    if stmt.view_check_predicate.is_some() {
        return None;
    }
    if stmt.insert_alias.is_some() {
        return None;
    }
    match &stmt.source {
        InsertSource::Values(rows) => {
            // Reject rows that embed subqueries — `execute_insert`'s
            // `eval_const_runtime` path materialises constants only;
            // subqueries would need the legacy `materialize_subqueries`
            // traversal that the dispatcher arranges around `execute_insert`.
            for row in rows {
                for expr in row {
                    if expr_contains_subquery(expr) {
                        return None;
                    }
                }
            }
        }
        // P4 — INSERT … SELECT is eligible for the compile-once prepared plan:
        // it streams through `execute_insert_borrowed`'s Select arm, which
        // clones only the source SELECT (not the whole statement) and reuses
        // the memoized privilege / MDL / qualification work. The compile-time
        // gates in `compile_prepared_dml` (partitioned table, INSERT triggers,
        // view target) still exclude the shapes the borrowed path can't take.
        InsertSource::Query(_) => {}
    }
    Some(stmt.table.as_str())
}

fn eligible_update(stmt: &Update) -> Option<&str> {
    if !stmt.joins.is_empty() {
        return None;
    }
    if !stmt.partitions.is_empty() {
        return None;
    }
    if stmt.view_check_predicate.is_some() {
        return None;
    }
    if let Some(selection) = &stmt.selection {
        if expr_contains_subquery(selection) {
            return None;
        }
    }
    for assignment in &stmt.assignments {
        if expr_contains_subquery(&assignment.value) {
            return None;
        }
    }
    Some(stmt.table.as_str())
}

fn eligible_delete(stmt: &Delete) -> Option<&str> {
    if !stmt.joins.is_empty() || !stmt.targets.is_empty() {
        return None;
    }
    if !stmt.partitions.is_empty() {
        return None;
    }
    if let Some(selection) = &stmt.selection {
        if expr_contains_subquery(selection) {
            return None;
        }
    }
    Some(stmt.table.as_str())
}

/// Recursive walk that mirrors the structural shapes `materialize_subqueries`
/// can replace. If we ever store a cached statement that needs subquery
/// materialisation we'd have to do it once per execute (the subquery
/// result depends on the live transactional view), which defeats the
/// memo. Falling back to the legacy path keeps semantics correct.
pub(super) fn expr_contains_subquery(expr: &Expr) -> bool {
    crate::parser::visit::value_expr_contains_subquery(expr)
}

/// Compile a reusable plan for an eligible prepared DML statement. The
/// cached state holds whatever is provably stable across executes for
/// the given (session, AST) pair: the qualified target, the privilege
/// vector, the MDL request set, and the catalog generation snapshot we
/// validate the memo against. The bound parameters are *not* part of
/// the memo — they bind late through the thread-local parameter
/// context, just like the SELECT fast path does for `Expr::Placeholder`.
pub(super) fn compile_prepared_dml(
    db: &Arc<Database>,
    session: &SessionContext,
    parsed: &Arc<Statement>,
    epoch: u64,
) -> Option<PreparedDmlPlan> {
    crate::engine::expr::with_session_expr_info(session, || {
        compile_prepared_dml_in_context(db, session, parsed, epoch)
    })
}

fn compile_prepared_dml_in_context(
    db: &Arc<Database>,
    session: &SessionContext,
    parsed: &Arc<Statement>,
    epoch: u64,
) -> Option<PreparedDmlPlan> {
    let table_name = eligible_dml(parsed)?;
    // This path bypasses view expansion and only tracks the target schema.
    // Source views (including nested expression queries) require the regular
    // dispatcher for expansion, security context and dependency invalidation.
    if matches!(parsed.as_ref(), Statement::Insert(stmt) if matches!(stmt.source, InsertSource::Query(_)))
        && super::dispatch::collect_statement_table_uses(parsed)
            .iter()
            .any(|usage| {
                db.view_definition(&session.current_database, &usage.table)
                    .is_some()
            })
    {
        return None;
    }
    if super::routines::statement_references_stored_function(db, &session.current_database, parsed)
    {
        return None;
    }
    if statement_refs_temp_table(session, parsed) {
        // Session temp tables route through the temp-statement-txn
        // wrapper and need per-execute resolution; not modelled here.
        return None;
    }
    // Reject any statement whose target sits in a system database.
    // performance_schema / sys are intercepted by `try_pfs_dml_*` /
    // `try_sys_dml_*` in the dispatcher; the `mysql` data dictionary is
    // rejected by `reject_system_catalog_dml`. Skip them here so the
    // cached path doesn't have to replicate those checks per execute.
    let (database, _table) = split_table_name(&session.current_database, table_name);
    if is_system_database(&database) || database.eq_ignore_ascii_case("performance_schema") {
        return None;
    }
    // Updatable-view rewrites mutate the statement on every execute;
    // reject view targets entirely so we never need to redo the
    // rewrite. A bona-fide base table is identified by failing the view
    // lookup.
    if db
        .view_definition(&session.current_database, table_name)
        .is_some()
    {
        return None;
    }
    let table = db.table(table_name).ok()?;
    // Partitioned tables route through `partition_router::with_pruned_partitions`
    // and per-row partition routing — supportable in principle but
    // intentionally out of scope for the first cut.
    if table.is_partitioned() {
        return None;
    }
    // Trigger paths fan into `execute_insert_rows_with_triggers` /
    // trigger-row update / trigger-row delete loops; their semantics
    // (firing OLD/NEW, possibly nested DML) are not on the fast path.
    match parsed.as_ref() {
        Statement::Insert(_) => {
            if triggers::table_has_insert_triggers(db, table_name) {
                return None;
            }
        }
        Statement::Update(_) => {
            if triggers::table_has_update_triggers(db, table_name) {
                return None;
            }
        }
        Statement::Delete(_) => {
            if triggers::table_has_delete_triggers(db, table_name) {
                return None;
            }
        }
        Statement::Select(_) => {}
        _ => return None,
    }
    let enum_statement = table
        .columns
        .iter()
        .any(|column| {
            matches!(
                column.data_type,
                crate::storage::DataTypeKind::Enum { .. }
                    | crate::storage::DataTypeKind::Set { .. }
            )
        })
        .then(|| {
            let mut statement = parsed.as_ref().clone();
            crate::planner::rewrite_enum_set_dml(db, &mut statement);
            Arc::new(statement)
        });
    let parsed = enum_statement.as_ref().unwrap_or(parsed);
    let locking_point_read = match parsed.as_ref() {
        Statement::Select(select) => compile_locking_point_read(&table, select),
        _ => None,
    };
    let point_write = compile_point_write(&table, parsed.as_ref());
    let range_update = if point_write.is_none() {
        compile_range_update(&table, parsed.as_ref())
    } else {
        None
    };
    let qualified_name = Arc::from(table.qualified_name());
    let table_id = table.id;
    let table_schema_version = table.schema_version;
    drop(table);
    let required_privileges = prepared_required_privileges(session, parsed);
    let mdl_requests = prepared_statement_mdl_requests(db, session, parsed);
    db.record_prepared_dml_compile();
    Some(PreparedDmlPlan {
        semantics: super::plan_semantics(session),
        optimizer_switch: Arc::from(session.sysvars.optimizer_switch.as_str()),
        statement: Arc::clone(parsed),
        epoch,
        cbo_epoch: crate::engine::cbo::cbo_epoch(),
        table_key: qualified_name,
        table_id,
        table_schema_version,
        required_privileges: required_privileges.into(),
        mdl_requests: mdl_requests.into(),
        locking_point_read,
        point_write,
        range_update,
        authorization_account: session.authenticated_as.clone(),
        authorized_auth_epoch: std::sync::atomic::AtomicU64::new(0),
        validated_catalog_epoch: std::sync::atomic::AtomicU64::new(0),
        locking_read_cover_memo: std::sync::atomic::AtomicU64::new(0),
        point_write_cover_memo: std::sync::atomic::AtomicU64::new(0),
    })
}

/// True iff the memo is still consistent with the live catalog. Same
/// contract as `compiled_select::reusable_memo`: a matching plan-cache
/// epoch *and* an unchanged single-table schema version. Any DDL that
/// touches the target table bumps `schema_version`; broader catalog
/// surgery (RENAME, DROP DATABASE, …) bumps the engine epoch. Either
/// invalidation forces recompile on the next execute.
pub(super) fn memo_is_valid(
    db: &Database,
    dp: &PreparedDmlPlan,
    session: &SessionContext,
    current_epoch: u64,
) -> bool {
    // Capture BEFORE reading the descriptor: publishing concurrently after
    // validation must leave a different epoch for the under-MDL check.
    let catalog_epoch = db.catalog_publication_epoch();
    let valid =
        super::compiled_select::semantics_match(&dp.semantics, &dp.optimizer_switch, session)
            && dp.epoch == current_epoch
            && dp.cbo_epoch == crate::engine::cbo::cbo_epoch()
            && db.table_key_identity_version_matches(
                &dp.table_key,
                dp.table_id,
                dp.table_schema_version,
            );
    if valid
        && dp
            .validated_catalog_epoch
            .load(std::sync::atomic::Ordering::Relaxed)
            != catalog_epoch
    {
        dp.validated_catalog_epoch
            .store(catalog_epoch, std::sync::atomic::Ordering::Release);
    }
    valid
}

fn revalidate_prepared_dml_after_mdl(
    db: &Database,
    dp: &PreparedDmlPlan,
) -> Result<(), QueryError> {
    let catalog_epoch = db.catalog_publication_epoch();
    if dp
        .validated_catalog_epoch
        .load(std::sync::atomic::Ordering::Acquire)
        != catalog_epoch
    {
        if !db.table_key_identity_version_matches(
            &dp.table_key,
            dp.table_id,
            dp.table_schema_version,
        ) {
            return Err(crate::error::catalog::ER_NEED_REPREPARE
                .error("prepared DML definition changed while acquiring metadata locks"));
        }
        dp.validated_catalog_epoch
            .store(catalog_epoch, std::sync::atomic::Ordering::Release);
    }
    Ok(())
}

fn release_prepared_dml_mdl(
    db: &Database,
    dp: &PreparedDmlPlan,
    connection_id: u64,
    guard: PreparedMdlGuard,
) {
    match guard {
        PreparedMdlGuard::Fast(grant) => db.release_fast_mdl(grant),
        PreparedMdlGuard::Statement => {
            db.release_cached_mdl_duration(connection_id, MdlDuration::Statement, &dp.mdl_requests)
        }
        PreparedMdlGuard::None => {}
    }
}

/// Execute a validated [`PreparedDmlPlan`]. Mirrors the legacy chain
/// `execute_statement` → `execute_mdl_protected_statement` →
/// `execute_qualified_statement` →
/// `execute_in_transaction_with_session` → `execute_{insert,update,delete}`,
/// minus everything `compile_prepared_dml` proved redundant.
///
/// The session is borrowed mutably because the inner DML uses
/// `execute_in_transaction_with_session` (statement txn / savepoint /
/// temp journal). It is *not* cloned — that's the cost the legacy path
/// pays inside `execute_statement_with_lock_wait_mode` that we avoid.
/// The caller has already installed the thread-local bind parameters
/// and session expr-info context for late `Expr::Placeholder` binding.
pub(super) fn execute_prepared_dml_plan(
    db: &Arc<Database>,
    session: &mut SessionContext,
    registry: &SessionRegistry,
    dp: &PreparedDmlPlan,
    connection_id: u64,
) -> Result<QueryResponse, QueryError> {
    authorize_prepared_dml(db, session, dp)?;
    let mdl_guard = acquire_prepared_statement_mdl(db, session, &dp.statement, &dp.mdl_requests)?;
    // The optimistic cache validation ran before MDL acquisition. A DDL may
    // have published while this statement waited, changing both the bound
    // schema and FK lock dependencies. Reject before starting any DML or
    // evaluating expressions; the caller can safely rebind this statement
    // without replaying or discarding an existing transaction.
    if let Err(err) = revalidate_prepared_dml_after_mdl(db, dp) {
        release_prepared_dml_mdl(db, dp, connection_id, mdl_guard);
        return Err(err);
    }
    // Propagate the session's `foreign_key_checks` flag for the duration
    // of this statement — the legacy `execute_statement` entry-point
    // sets it before calling `execute_qualified_statement`, and the
    // storage layer reads it through a thread-local set here. The RAII
    // guard restores the previous thread-local on return so a pooled worker
    // thread never leaks an OFF setting into later internal/background DML.
    let _fk_checks_guard = crate::storage::FkChecksGuard::new(session.foreign_key_checks);
    let result = if let Some(plan) = dp.locking_point_read.as_ref().filter(|plan| {
        let Statement::Select(statement) = dp.statement.as_ref() else {
            return false;
        };
        statement.selection.as_ref().is_some_and(|selection| {
            index_access_covers_all_conjuncts_memo(
                &dp.locking_read_cover_memo,
                &plan.table,
                &plan.index,
                selection,
                &plan.key,
            )
        })
    }) {
        execute_retryable_locking_point_read_with_session(db, session, |db, session, txn| {
            execute_locking_point_read(db, session, txn, plan)
        })
    } else {
        execute_dml_in_transaction_with_session(db, session, |db, session, txn| {
            match dp.statement.as_ref() {
                Statement::Insert(stmt) => {
                    // Plain `INSERT … VALUES` executes against the borrowed
                    // cached AST with zero clone; only `INSERT … SELECT`
                    // (which streams through the owned pipeline) falls back
                    // to cloning the inner struct.
                    if let Some(resp) =
                        insert_exec::execute_insert_borrowed(db, session, txn, stmt, registry)?
                    {
                        return Ok(resp);
                    }
                    insert_exec::execute_insert(db, session, txn, stmt.clone(), registry)
                }
                Statement::Update(stmt) => {
                    if let Some(point) = dp.point_write.as_ref().filter(|point| {
                        matches!(point.kind, PreparedPointWriteKind::Update(_))
                            && stmt.selection.as_ref().is_some_and(|selection| {
                                index_access_covers_all_conjuncts_memo(
                                    &dp.point_write_cover_memo,
                                    &point.table,
                                    &point.index,
                                    selection,
                                    &point.key,
                                )
                            })
                    }) {
                        return dml_exec::execute_prepared_point_update(
                            db, session, txn, stmt, point,
                        );
                    }
                    if let Some(range) = &dp.range_update {
                        return dml_exec::execute_prepared_range_update(
                            db, session, txn, stmt, range,
                        );
                    }
                    if let Some(resp) =
                        dml_exec::execute_update_borrowed_simple(db, session, txn, stmt)?
                    {
                        return Ok(resp);
                    }
                    dml_exec::execute_update(db, session, txn, stmt.clone(), registry)
                }
                Statement::Delete(stmt) => {
                    if let Some(point) = dp
                        .point_write
                        .as_ref()
                        .filter(|point| matches!(point.kind, PreparedPointWriteKind::Delete))
                    {
                        return dml_exec::execute_prepared_point_delete(
                            db, session, txn, stmt, point,
                        );
                    }
                    if let Some(resp) =
                        dml_exec::execute_delete_borrowed_simple(db, session, txn, stmt)?
                    {
                        return Ok(resp);
                    }
                    dml_exec::execute_delete(db, session, txn, stmt.clone(), registry)
                }
                Statement::Select(stmt) => {
                    // Borrowed variant: the locked single-table executor
                    // answers FOR UPDATE selects without cloning the cached
                    // AST; only the general-select fallback clones.
                    execute_select_in_transaction_borrowed(db, session, stmt, registry)
                }
                _ => Err(crate::error::catalog::ER_UNKNOWN_ERROR
                    .error("internal error: prepared DML plan holds an unsupported statement")),
            }
        })
    };
    // The guard encodes which release pairs with the acquire:
    // transaction-duration tickets (`None`) are released at
    // commit/rollback; only the ticketed statement-duration case takes
    // the global MDL mutex here.
    match mdl_guard {
        PreparedMdlGuard::Fast(grant) => db.release_fast_mdl(grant),
        PreparedMdlGuard::Statement => {
            db.release_cached_mdl_duration(connection_id, MdlDuration::Statement, &dp.mdl_requests);
        }
        PreparedMdlGuard::None => {}
    }
    result
}

#[derive(Clone, Copy)]
struct PointBatchOperation<'a> {
    ordinal: usize,
    params: &'a [Value],
}

/// Return the late-bound delta parameter for the exact
/// `decimal_col = decimal_col +/- ?` shape. Repeated updates of one key can
/// sum a monotonic exact delta and evaluate/coerce once without changing any
/// intermediate overflow boundary.
fn exact_decimal_counter_param(
    statement: &Statement,
    point: &PreparedPointWrite,
) -> Option<(usize, u8)> {
    let Statement::Update(stmt) = statement else {
        return None;
    };
    let [assignment] = stmt.assignments.as_slice() else {
        return None;
    };
    let column_index = point.table.column_index(&assignment.column).ok()?;
    let crate::storage::DataTypeKind::Decimal {
        scale: Some(column_scale),
        ..
    } = point.table.columns.get(column_index)?.data_type
    else {
        return None;
    };
    let Expr::Binary { left, op, right } = &assignment.value else {
        return None;
    };
    let is_target = |expr: &Expr| match expr {
        Expr::Identifier(name) => name.eq_ignore_ascii_case(&assignment.column),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .is_some_and(|name| name.eq_ignore_ascii_case(&assignment.column)),
        _ => false,
    };
    let parameter = match (op, left.as_ref(), right.as_ref()) {
        (BinaryOp::Add, target, Expr::Placeholder(parameter)) if is_target(target) => *parameter,
        (BinaryOp::Add, Expr::Placeholder(parameter), target) if is_target(target) => *parameter,
        (BinaryOp::Sub, target, Expr::Placeholder(parameter)) if is_target(target) => *parameter,
        _ => return None,
    };
    Some((parameter, column_scale))
}

fn exact_decimal_sign(value: &crate::util::decimal::ExactDecimal) -> i8 {
    use crate::util::decimal::ExactDecimal;
    use num_bigint::Sign;
    match value {
        ExactDecimal::I64(value, _) => value.signum() as i8,
        ExactDecimal::I128(value, _) => value.signum() as i8,
        ExactDecimal::Big(value, _) => match value.sign() {
            Sign::Minus => -1,
            Sign::NoSign => 0,
            Sign::Plus => 1,
        },
    }
}

fn combined_monotonic_decimal_params(
    group: &[PointBatchOperation<'_>],
    parameter: usize,
    column_scale: u8,
) -> Option<Vec<Value>> {
    use crate::util::decimal::{exact_binary, DecimalBinaryOp, DecimalContext, ExactDecimal};
    let first = group.first()?.params;
    let mut sum = ExactDecimal::I64(0, 0);
    let mut direction = 0i8;
    for operation in group {
        let delta = operation.params.get(parameter)?.exact_decimal()?;
        if delta.scale() > column_scale {
            // Per-child column-scale rounding could differ from rounding one
            // summed high-scale delta; retain scalar evaluation.
            return None;
        }
        let sign = exact_decimal_sign(&delta);
        if sign != 0 {
            if direction != 0 && direction != sign {
                // A mixed-sign sequence can cross a range boundary even when
                // its final sum fits, so final-value validation is insufficient.
                return None;
            }
            direction = sign;
        }
        sum = exact_binary(
            sum.as_ref(),
            DecimalBinaryOp::Add,
            delta.as_ref(),
            DecimalContext::default(),
        )
        .ok()??;
    }
    let mut combined = first.to_vec();
    *combined.get_mut(parameter)? = Value::from_exact_decimal(sum);
    Some(combined)
}

/// Execute a homogeneous `;`-joined exact-point UPDATE batch as one physical
/// storage batch. Connector/J's `rewriteBatchedStatements` emits this shape
/// for `PreparedStatement::executeBatch`: without this bridge the text batch
/// cache avoids parsing, but still re-enters the complete statement/savepoint,
/// heap, undo, zone-map, and accounting pipeline once per child.
///
/// Eligibility is deliberately narrower than ordinary prepared point UPDATE:
/// no changed index/FK/generated/check/on-update/auto-increment dependency is
/// allowed. Distinct target rows therefore commute. Repeated keys are folded
/// left-to-right in the expression closure and become one physical rewrite;
/// the logical DML counters are restored afterwards. The caller attempts this
/// under nonblocking row locks and falls back to the scalar child loop on any
/// error, preserving the established per-child error and wait semantics.
pub(super) fn execute_homogeneous_point_update_batch(
    db: &Arc<Database>,
    session: &mut SessionContext,
    dp: &PreparedDmlPlan,
    param_sets: &[&[Value]],
    mut statements: Option<&mut Vec<crate::engine::api::DmlStatementResult>>,
) -> Result<Option<QueryResponse>, QueryError> {
    if param_sets.len() < 2 || session.autocommit || session.explicit_txn {
        // Autocommit multi-statements commit independently. Combining them
        // would change durability/error boundaries. JDBC setAutoCommit(false)
        // is represented by autocommit=false, explicit_txn=false.
        return Ok(None);
    }
    let Some(point) = dp.point_write.as_ref() else {
        return Ok(None);
    };
    let PreparedPointWriteKind::Update(update) = &point.kind else {
        return Ok(None);
    };
    let Statement::Update(statement) = dp.statement.as_ref() else {
        return Ok(None);
    };
    if !param_sets.iter().all(|params| {
        crate::engine::params::with_params_slice(params, || {
            statement.selection.as_ref().is_some_and(|selection| {
                index_access_covers_all_conjuncts(&point.table, &point.index, selection, &point.key)
            })
        })
    }) {
        return Ok(None);
    }
    let table = &point.table;
    if table.is_partitioned()
        || !update.affected_indexes.is_empty()
        || !update.on_update_columns.is_empty()
        || !table.check_constraints.is_empty()
        || table
            .foreign_keys
            .iter()
            .any(|fk| fk.affected_by_changed(&update.changed_columns))
        || table.fk_referenced_by.iter().any(|fk| {
            fk.referenced_col_idx
                .iter()
                .any(|column| update.changed_columns.contains(column))
        })
        || table
            .columns
            .iter()
            .any(|column| column.generated_expr.is_some())
        || update.changed_columns.iter().any(|&column| {
            table
                .columns
                .get(column)
                .is_some_and(|column| column.auto_increment)
        })
    {
        return Ok(None);
    }

    // The grouping map owns one resolved key and one small operation record
    // per child for this call only. Charge the upper-bound container capacity
    // (not current len) plus parameter-owned payload capacity conservatively;
    // the reservation is released after the batch/fallback decision.
    use super::memory_footprint::AllocatedBytes;
    // Prefer descriptor-only point mutations when they avoid decoding payload
    // columns. Key columns do not count as payload: a narrow counter table
    // should retain the grouped storage path. FK presence is not a cost signal.
    let sparse_candidate = table.columns.iter().enumerate().any(|(position, column)| {
        !update.required_columns.contains(&position)
            && !point
                .index
                .columns
                .iter()
                .any(|key| key.eq_ignore_ascii_case(&column.name))
    });
    if !update.replay_safe || crate::engine::api::statement_warning_count() != 0 {
        // Either path may roll back its trial and replay a successful prefix.
        // Volatile expressions and an existing condition area stay scalar.
        return Ok(None);
    }
    let grouping_bytes = param_sets
        .len()
        .saturating_mul(
            std::mem::size_of::<crate::storage::IndexKey>()
                .saturating_mul(2)
                .saturating_add(std::mem::size_of::<PointBatchOperation<'_>>())
                .saturating_add(std::mem::size_of::<crate::storage::RowId>())
                .saturating_add(128),
        )
        .saturating_add(
            param_sets
                .iter()
                .flat_map(|params| params.iter())
                .map(AllocatedBytes::allocated_bytes)
                .sum::<usize>()
                .saturating_mul(2),
        );
    let _grouping_charge =
        match crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
            grouping_bytes,
        ) {
            Ok(charge) => charge,
            Err(err) => return Err(err),
        };

    let authorize = authorize_prepared_dml(db, session, dp);
    if let Err(err) = authorize {
        return Err(err);
    }
    let mdl_guard =
        match acquire_prepared_statement_mdl(db, session, &dp.statement, &dp.mdl_requests) {
            Ok(guard) => guard,
            Err(err) => return Err(err),
        };
    let connection_id = session.connection_id as u64;
    if let Err(err) = revalidate_prepared_dml_after_mdl(db, dp) {
        release_prepared_dml_mdl(db, dp, connection_id, mdl_guard);
        return Err(err);
    }
    let _fk_checks_guard = crate::storage::FkChecksGuard::new(session.foreign_key_checks);
    for _ in param_sets {
        db.record_prepared_dml_reuse();
    }
    let mut logical_results = statements
        .as_ref()
        .map(|_| vec![(0u64, 0u64, crate::engine::api::SqlWarnings::new()); param_sets.len()]);
    let result = execute_dml_in_transaction_with_session(db, session, |db, session, txn| {
        let mut operations =
            BTreeMap::<crate::storage::IndexKey, Vec<PointBatchOperation<'_>>>::new();
        for (ordinal, params) in param_sets.iter().copied().enumerate() {
            let key =
                crate::engine::params::with_params_slice(params, || point.key.resolve_params())?;
            operations
                .entry(key)
                .or_default()
                .push(PointBatchOperation { ordinal, params });
        }
        if sparse_candidate && operations.len() == param_sets.len() {
            // Distinct targets have nothing to coalesce. Avoid full-row
            // collection while sharing the transaction/savepoint/MDL envelope.
            // Repeated keys retain grouping and its single physical rewrite.
            let Statement::Update(statement) = &dp.statement.as_ref() else {
                unreachable!()
            };
            let mut last = QueryResponse::Ok;
            let mut changed = 0u64;
            for (ordinal, params) in param_sets.iter().enumerate() {
                last = crate::engine::params::with_params_slice(params, || {
                    dml_exec::execute_prepared_point_update(db, session, txn, statement, point)
                })?;
                if let QueryResponse::OkAffected { affected_rows } = &last {
                    changed = changed.saturating_add(*affected_rows);
                    if let Some(results) = &mut logical_results {
                        results[ordinal] = (
                            *affected_rows,
                            crate::engine::api::take_statement_matched_rows()
                                .unwrap_or(*affected_rows),
                            crate::engine::api::take_statement_warnings(),
                        );
                    }
                }
            }
            #[cfg(test)]
            {
                HOMOGENEOUS_POINT_UPDATE_BATCHES.fetch_add(1, Ordering::Relaxed);
                HOMOGENEOUS_POINT_UPDATE_LOGICAL_ROWS.fetch_add(changed, Ordering::Relaxed);
                HOMOGENEOUS_POINT_UPDATE_PHYSICAL_ROWS.fetch_add(changed, Ordering::Relaxed);
            }
            SPARSE_POINT_BATCH_HITS.fetch_add(1, Ordering::Relaxed);
            return Ok(last);
        }

        let resolved_keys = operations.keys().cloned().collect::<Vec<_>>();
        let row_ids = db
            .lookup_unique_index_for_write_many(txn, table, &point.index, &resolved_keys)?
            .into_iter()
            .map(|(_, row_id)| row_id)
            .collect::<Vec<_>>();
        // The ordinary multi-statement COM_QUERY route already runs on the
        // blocking pool. Acquire every row lock before the storage collector:
        // its internal batch lock pass then becomes an allocation-free owned
        // lock hit instead of surfacing LockWaitPending and replaying all
        // children through the scalar executor.
        for &row_id in &row_ids {
            db.lock_row_id_for_update_blocking(txn, table, row_id)?;
        }

        let last_ordinal = param_sets.len() - 1;
        let mut last_affected = 0u64;
        let mut logical_changed = 0u64;
        let decimal_counter = exact_decimal_counter_param(&dp.statement, point);
        let table_name = table.qualified_name();
        let physical_changed = db.update_rows_with_changed_columns_noop(
            txn,
            &table_name,
            Some(row_ids),
            Some(&update.changed_columns),
            Some(&update.user_changed_columns),
            |_| Ok(true),
            |values| {
                let key = table.index_key(&point.index, values)?;
                let Some(group) = operations.get(&key) else {
                    return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                        .error("point-update batch lost its resolved target key"));
                };
                let mut current = values.to_vec();
                if group.len() > 1 && logical_results.is_none() {
                    if let Some((parameter, column_scale)) = decimal_counter {
                        if let Some(combined) =
                            combined_monotonic_decimal_params(group, parameter, column_scale)
                        {
                            let before: smallvec::SmallVec<[Value; 8]> = update
                                .user_changed_columns
                                .iter()
                                .map(|&column| current.get(column).cloned().unwrap_or(Value::Null))
                                .collect();
                            crate::engine::params::with_params_slice(&combined, || {
                                for assignment in update.assignments.iter() {
                                    let value = assignment.eval(table, &current)?;
                                    current[assignment.column_index] = value;
                                }
                                crate::storage::coerce_updated_row(
                                    table,
                                    &mut current,
                                    Some(&update.changed_columns),
                                )
                            })?;
                            let changed = update.user_changed_columns.iter().enumerate().any(
                                |(position, &column)| before.get(position) != current.get(column),
                            );
                            if changed {
                                logical_changed =
                                    logical_changed.saturating_add(group.len() as u64);
                            }
                            if group
                                .iter()
                                .any(|operation| operation.ordinal == last_ordinal)
                            {
                                last_affected = u64::from(changed);
                            }
                            return Ok(current);
                        }
                    }
                }
                for operation in group {
                    let before: smallvec::SmallVec<[Value; 8]> = update
                        .user_changed_columns
                        .iter()
                        .map(|&column| current.get(column).cloned().unwrap_or(Value::Null))
                        .collect();
                    crate::engine::params::with_params_slice(operation.params, || {
                        for assignment in update.assignments.iter() {
                            let value = assignment.eval(table, &current)?;
                            current[assignment.column_index] = value;
                        }
                        crate::storage::coerce_updated_row(
                            table,
                            &mut current,
                            Some(&update.changed_columns),
                        )
                    })?;
                    let changed = update
                        .user_changed_columns
                        .iter()
                        .enumerate()
                        .any(|(position, &column)| before.get(position) != current.get(column));
                    if changed {
                        logical_changed = logical_changed.saturating_add(1);
                    }
                    if operation.ordinal == last_ordinal {
                        last_affected = u64::from(changed);
                    }
                    if let Some(results) = &mut logical_results {
                        results[operation.ordinal] = (
                            u64::from(changed),
                            1,
                            crate::engine::api::take_statement_warnings(),
                        );
                    }
                }
                Ok(current)
            },
        )? as u64;
        db.record_coalesced_point_updates(
            txn.txn_id,
            table,
            logical_changed.saturating_sub(physical_changed),
        )?;
        #[cfg(test)]
        {
            HOMOGENEOUS_POINT_UPDATE_BATCHES.fetch_add(1, Ordering::Relaxed);
            HOMOGENEOUS_POINT_UPDATE_LOGICAL_ROWS.fetch_add(logical_changed, Ordering::Relaxed);
            HOMOGENEOUS_POINT_UPDATE_PHYSICAL_ROWS.fetch_add(physical_changed, Ordering::Relaxed);
        }
        Ok(QueryResponse::OkAffected {
            affected_rows: last_affected,
        })
    });
    match mdl_guard {
        PreparedMdlGuard::Fast(grant) => db.release_fast_mdl(grant),
        PreparedMdlGuard::Statement => {
            db.release_cached_mdl_duration(connection_id, MdlDuration::Statement, &dp.mdl_requests);
        }
        PreparedMdlGuard::None => {}
    }
    // Any execution-time failure is replayed through the established scalar
    // child loop. The transaction wrapper has already rolled this attempt to
    // its statement savepoint, so the replay preserves successful-prefix and
    // exact child-error semantics. Admission/auth/governor failures above are
    // returned directly because replay cannot change them.
    match result {
        Ok(response) => {
            if let (Some(out), Some(results)) = (statements.as_deref_mut(), logical_results) {
                for (affected_rows, matched_rows, warnings) in results {
                    out.push(crate::engine::api::DmlStatementResult {
                        affected_rows,
                        matched_rows: Some(matched_rows),
                        insert_id: 0,
                        info: format!(
                            "Rows matched: {matched_rows} Changed: {affected_rows} Warnings: {}",
                            warnings.len()
                        ),
                        warnings,
                        autocommit: session.autocommit,
                        in_transaction: session.in_transaction(),
                    });
                }
                if let Some(last) = out.last() {
                    session.set_warnings(last.warnings.clone());
                    session.statement_insert_id = 0;
                    session.pending_statement_matched_rows = last.matched_rows;
                    session.pending_statement_info = Some(last.info.clone());
                    session.last_row_count = last.affected_rows as i64;
                    session.found_rows = 0;
                }
            }
            Ok(Some(response))
        }
        Err(_) => {
            // Both paths require an empty condition area on entry. Discard
            // trial warnings before scalar prefix replay to avoid duplicates.
            crate::engine::api::clear_statement_warnings();
            Ok(None)
        }
    }
}

// Only row/parameter/literal computations may be replayed after a batch
// savepoint rollback. Functions, variables and subqueries stay scalar: an
// attempted RAND/sequence/user-variable effect cannot be rolled back.
fn point_batch_expr_replay_safe(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Literal(_)
        | Expr::BoundValue(_)
        | Expr::Placeholder(_) => true,
        Expr::Binary { left, right, .. } => {
            point_batch_expr_replay_safe(left) && point_batch_expr_replay_safe(right)
        }
        Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            point_batch_expr_replay_safe(expr)
        }
        _ => false,
    }
}

fn compile_update(
    table: &Arc<crate::storage::TableSchema>,
    stmt: &Update,
) -> Option<PreparedPointUpdate> {
    let mut assignments: Vec<PreparedPointAssignment> = Vec::with_capacity(stmt.assignments.len());
    let mut changed_columns = Vec::with_capacity(stmt.assignments.len());
    let mut required_columns = std::collections::HashSet::new();
    for assignment in &stmt.assignments {
        dml_exec::validate_assignment_qualifier(
            assignment,
            &stmt.table,
            stmt.primary_alias.as_deref(),
        )
        .ok()?;
        let column_idx = table.column_index(&assignment.column).ok()?;
        // Generated-column assignment validation has canonical error
        // handling in the general DML path; keep that shape on the
        // fallback rather than duplicating it here.
        if table.columns[column_idx].generated_expr.is_some() {
            return None;
        }
        if dml_exec::update_expr_contains_default(&assignment.value) {
            return None;
        }
        if !changed_columns.contains(&column_idx) {
            changed_columns.push(column_idx);
        }
        let literal = if let Expr::Literal(lit) = &assignment.value {
            Some(Expr::BoundValue(super::insert::insert_literal_value(
                lit.clone(),
                &table.columns[column_idx].data_type,
            )))
        } else {
            None
        };
        let compiled = crate::engine::expr_program::CompiledTableExpr::compile_once(
            literal.as_ref().unwrap_or(&assignment.value),
            table,
        );
        if !compiled.try_collect_referenced_columns(&mut required_columns) {
            return None;
        }
        required_columns.insert(column_idx);
        // Keep every assignment in source order. MySQL evaluates
        // single-table UPDATE assignments from left to right, even
        // when the same column appears more than once.
        assignments.push(PreparedPointAssignment {
            column_index: column_idx,
            expression: compiled,
        });
    }
    let user_changed_columns = changed_columns.clone();
    let on_update_columns = dml_exec::on_update_timestamp_columns(table, &changed_columns);
    required_columns.extend(on_update_columns.iter().copied());
    let mut required_columns: Vec<usize> = required_columns.into_iter().collect();
    required_columns.sort_unstable();
    changed_columns.extend(on_update_columns.iter().copied());
    let effective_changed_columns =
        crate::storage::expand_changed_columns_for_generated(table, Some(&changed_columns))
            .unwrap_or_else(|| changed_columns.clone());
    let affected_indexes: Arc<[usize]> = table
        .indexes
        .iter()
        .enumerate()
        .filter_map(|(idx, index)| {
            crate::storage::index_may_be_affected_by_columns(
                table,
                index,
                Some(&effective_changed_columns),
            )
            .then_some(idx)
        })
        .collect::<Vec<_>>()
        .into();
    // Sparse point updates still need every key component of an
    // affected ordinary index, including unchanged covering columns.
    // Functional and specialized indexes retain the full-row path.
    for &position in affected_indexes.iter() {
        let index = &table.indexes[position];
        if index.key_expressions.iter().all(Option::is_none) {
            for column in index.columns.iter().chain(&index.included_columns) {
                required_columns.push(table.column_index(column).ok()?);
            }
        }
    }
    required_columns.sort_unstable();
    required_columns.dedup();
    Some(PreparedPointUpdate {
        replay_safe: stmt
            .assignments
            .iter()
            .all(|assignment| point_batch_expr_replay_safe(&assignment.value)),
        assignments: assignments.into(),
        changed_columns: changed_columns.into(),
        user_changed_columns: user_changed_columns.into(),
        on_update_columns: on_update_columns.into(),
        decode_recipe: crate::storage::RowDecodePlan::try_compile_cached(
            &required_columns,
            table.row_codec.as_ref(),
        ),
        required_columns: required_columns.into(),
        affected_indexes,
    })
}

/// Prove that a parameterized exact key predicate cannot invoke temporal
/// validation. Temporal keys and every expression outside this small grammar
/// retain the normal per-execute checks (including parameter diagnostics).
fn point_predicate_has_no_temporal_conversion(
    expr: &Expr,
    table: &crate::storage::TableSchema,
) -> bool {
    match expr {
        Expr::Placeholder(_) => true,
        Expr::Identifier(name) => table.find_column_index(name).is_some_and(|index| {
            !matches!(
                table.columns[index].data_type,
                crate::storage::DataTypeKind::Date
                    | crate::storage::DataTypeKind::DateTime { .. }
                    | crate::storage::DataTypeKind::Timestamp { .. }
                    | crate::storage::DataTypeKind::Time { .. }
            )
        }),
        Expr::CompoundIdentifier(parts) => parts.last().is_some_and(|name| {
            point_predicate_has_no_temporal_conversion(&Expr::Identifier(name.clone()), table)
        }),
        Expr::Binary {
            left,
            op:
                crate::parser::BinaryOp::Eq
                | crate::parser::BinaryOp::NullSafeEq
                | crate::parser::BinaryOp::And,
            right,
        } => {
            point_predicate_has_no_temporal_conversion(left, table)
                && point_predicate_has_no_temporal_conversion(right, table)
        }
        _ => false,
    }
}

fn compile_point_write(
    table: &Arc<crate::storage::TableSchema>,
    statement: &Statement,
) -> Option<PreparedPointWrite> {
    let (selection, kind) = match statement {
        Statement::Update(stmt)
            if stmt.order_by.is_empty()
                && stmt.limit.is_none()
                && stmt.returning.is_none()
                && stmt.view_check_predicate.is_none() =>
        {
            (
                stmt.selection.as_ref()?,
                PreparedPointWriteKind::Update(compile_update(table, stmt)?),
            )
        }
        Statement::Delete(stmt)
            if stmt.order_by.is_empty() && stmt.limit.is_none() && stmt.returning.is_none() =>
        {
            (stmt.selection.as_ref()?, PreparedPointWriteKind::Delete)
        }
        _ => return None,
    };
    let Some((index, EngineIndexAccess::Lookup { key, exact: true })) =
        best_index_access(table, selection)
    else {
        return None;
    };
    if !index.unique
        || !super::indexes::index_access_template_covers_all_conjuncts(
            table, index, selection, &key,
        )
    {
        return None;
    }
    Some(PreparedPointWrite {
        needs_temporal_validation: !point_predicate_has_no_temporal_conversion(selection, table),
        table: Arc::clone(table),
        index: index.clone(),
        key,
        kind,
    })
}

fn compile_locking_point_read(
    table: &Arc<crate::storage::TableSchema>,
    stmt: &Select,
) -> Option<PreparedLockingPointRead> {
    if !stmt.for_update
        || stmt.lock_clause.as_ref().is_some_and(|clause| {
            !clause.targets.is_empty()
                || clause.mode != LockClauseMode::ForUpdate
                || clause.wait != LockWaitMode::Default
        })
        || !is_simple_single_table_select(stmt)
    {
        return None;
    }
    let selection = stmt.selection.as_ref()?;
    let Some((index, EngineIndexAccess::Lookup { key, exact })) =
        best_index_access(table, selection)
    else {
        return None;
    };
    if !exact
        || !(index.unique || index.primary)
        || !super::indexes::index_access_template_covers_all_conjuncts(
            table, index, selection, &key,
        )
    {
        return None;
    }
    // Keep the compiled shape narrow even though the shared locked-row
    // executor below can project more general expressions.
    let projection_indexes = simple_projection_indexes(stmt, table)?;
    // Moving an owned value is safe only once. Keep duplicate projections on
    // the general locked-row executor, which clones according to SQL
    // semantics; the exact point kernel stays allocation-minimal.
    if projection_indexes
        .iter()
        .enumerate()
        .any(|(pos, idx)| projection_indexes[..pos].contains(idx))
    {
        return None;
    }
    let table_alias = stmt.from.first().and_then(|from| from.alias.as_deref());
    let result_columns = single_table_result_columns(stmt, table, table_alias)
        .ok()?
        .into();
    let key_column_indexes = (!index.has_prefix_columns()
        && index.included_columns.is_empty()
        && index
            .key_expressions
            .iter()
            .all(|expression| expression.is_none())
        && !index.is_spatial()
        && !index.is_vector()
        && !index.is_fulltext())
    .then(|| {
        index
            .columns
            .iter()
            .map(|name| table.column_index(name).ok())
            .collect::<Option<Vec<_>>>()
    })
    .flatten();
    let mut read_columns = projection_indexes.clone();
    let key_positions = key_column_indexes.map(|key_columns| {
        key_columns
            .into_iter()
            .map(|column| {
                if let Some(position) = read_columns
                    .iter()
                    .position(|candidate| *candidate == column)
                {
                    position
                } else {
                    let position = read_columns.len();
                    read_columns.push(column);
                    position
                }
            })
            .collect::<Vec<_>>()
    });
    let columns: Arc<[usize]> = read_columns.into();
    Some(PreparedLockingPointRead {
        table: Arc::clone(table),
        index: index.clone(),
        key,
        projection: crate::engine::cache::CompiledProjectedRead {
            output_map: None,
            decode_plan: crate::storage::RowDecodePlan::compile_with_codec(
                &columns,
                table.row_codec.as_ref(),
            ),
            projected_plan: crate::storage::ProjectedReadPlan::new(&columns, table),
            columns,
        },
        key_positions: key_positions.map(Into::into),
        result_columns,
    })
}

fn execute_locking_point_read(
    db: &Arc<Database>,
    session: &mut SessionContext,
    txn: TxnView,
    plan: &PreparedLockingPointRead,
) -> Result<QueryResponse, QueryError> {
    let resolved_key = plan.key.resolve_params()?;
    // This operator produces a handle. Looking in an empty transaction cache
    // before every first FOR UPDATE only added a guaranteed miss; the later
    // point UPDATE is the useful consumer.
    let candidate =
        db.lookup_unique_index_for_write_one(txn, &plan.table, &plan.index, resolved_key.clone())?;
    let locked_row_id = match candidate {
        Some(row_id) => {
            // `compile_locking_point_read` accepts only the default FOR UPDATE
            // clause. Lock the one candidate directly; the projected current
            // read below is the authoritative post-lock existence/key check.
            db.lock_row_id_for_update_blocking(txn, &plan.table, row_id)?;
            Some(row_id)
        }
        None => None,
    };
    // A locking SELECT is a current read: after waiting for the row lock it
    // must observe the newest committed image, even when the gap-aware
    // snapshot frontier is temporarily behind a later commit that already
    // published and released this row. Refresh the statement view before the
    // post-lock key recheck and projected heap read.
    let ro_snapshot = if session.read_only_txn {
        session.txn_snapshot_scn
    } else {
        None
    };
    let fresh_txn = db.session_statement_view(txn.txn_id, txn.isolation, ro_snapshot);
    // A locking read is a current read, including across a temporary gap in
    // the contiguous published-SCN frontier. The row lock excludes another
    // active writer here; an all-committed horizon therefore selects the
    // newest committed/own version while ordinary MVCC logic still walks past
    // a rolled-back top version.
    let current_read_txn = TxnView {
        snapshot_scn: u64::MAX,
        read_only: false,
        snapshot_registered: false,
        ..fresh_txn
    };
    let mut rows = Vec::with_capacity(usize::from(locked_row_id.is_some()));
    if let Some(row_id) = locked_row_id {
        // The unique-key probe happened before the row lock was acquired. A
        // concurrent writer can move the row between those operations, so
        // re-probe under the held row lock before trusting the point-key proof.
        // A match makes the WHERE recheck redundant: compilation admitted only
        // exact unique probes whose key covers every conjunct.
        let current = if plan.key_positions.is_some() {
            Some(row_id)
        } else {
            db.lookup_unique_index_for_write_one(
                fresh_txn,
                &plan.table,
                &plan.index,
                resolved_key.clone(),
            )?
        };
        if current == Some(row_id) {
            if let Some(mut row) = db.read_row_columns_at_with_plan(
                plan.table.id,
                &plan.table,
                row_id,
                current_read_txn,
                &plan.projection.columns,
                Some(&plan.projection.decode_plan),
                Some(&plan.projection.projected_plan),
            )? {
                let key_matches = if let Some(key_positions) = &plan.key_positions {
                    let mut parts = smallvec::SmallVec::<[KeyPart; 4]>::new();
                    for &position in key_positions.iter() {
                        let column_index = plan.projection.columns[position];
                        let column = &plan.table.columns[column_index];
                        parts.push(KeyPart::from_column_value_with_collation(
                            &row[position],
                            &column.data_type,
                            column.collation.as_deref(),
                        ));
                    }
                    IndexKey::from(parts) == resolved_key
                } else {
                    true
                };
                if key_matches {
                    row.truncate(plan.result_columns.len());
                    rows.push(row);
                }
            }
        }
    }
    if crate::engine::api::shared_result_metadata_allowed() {
        Ok(QueryResponse::SharedResultSet(
            crate::engine::api::SharedResultSet {
                columns: Arc::clone(&plan.result_columns),
                rows,
            },
        ))
    } else {
        Ok(QueryResponse::ResultSet(crate::engine::api::ResultSet {
            columns: plan.result_columns.to_vec(),
            rows,
        }))
    }
}

fn authorize_prepared_dml(
    db: &Arc<Database>,
    session: &SessionContext,
    dp: &PreparedDmlPlan,
) -> Result<(), QueryError> {
    if prepared_select_auth_cache_allowed(session)
        && dp.authorization_account == session.authenticated_as
    {
        db.ensure_operational()?;
        let current_epoch = crate::engine::auth::authorization_generation();
        if current_epoch != 0 && dp.authorized_auth_epoch.load(Ordering::Acquire) == current_epoch {
            return Ok(());
        }
    }
    let authorized_epoch = authorize_prepared_statement_with_epoch(
        db,
        session,
        &dp.statement,
        &dp.required_privileges,
    )?;
    if prepared_select_auth_cache_allowed(session)
        && dp.authorization_account == session.authenticated_as
    {
        dp.authorized_auth_epoch
            .store(authorized_epoch, Ordering::Release);
    }
    Ok(())
}

fn prepared_select_auth_cache_allowed(session: &SessionContext) -> bool {
    session.active_roles.is_empty()
        && session.view_security_overrides.is_empty()
        && session.temp_tables.is_empty()
}

/// Indexed UPDATEs share assignment compilation with exact point writes.
/// Keep key-changing and complex statement shapes on the batch DML path.
fn compile_range_update(
    table: &Arc<crate::storage::TableSchema>,
    statement: &Statement,
) -> Option<crate::engine::api::PreparedRangeUpdate> {
    let Statement::Update(stmt) = statement else {
        return None;
    };
    if !stmt.order_by.is_empty()
        || stmt.limit.is_some()
        || stmt.returning.is_some()
        || stmt.view_check_predicate.is_some()
        || stmt.ignore
        || table.columns.iter().any(|c| c.generated_expr.is_some())
    {
        return None;
    }
    let selection = stmt.selection.as_ref()?;
    super::indexes::plan_write_candidates(table, selection)?;
    let mut update = compile_update(table, stmt)?;
    // Snapshot candidate order and uniqueness reservations must retain the
    // established bulk path when a unique key changes.
    if update
        .affected_indexes
        .iter()
        .any(|&i| table.indexes[i].enforces_uniqueness())
    {
        return None;
    }
    let predicate = crate::engine::expr_program::CompiledTableExpr::compile_once(selection, table);
    let mut columns = update.required_columns.iter().copied().collect();
    if !predicate.try_collect_referenced_columns(&mut columns) {
        return None;
    }
    let mut columns: Vec<usize> = columns.into_iter().collect();
    columns.sort_unstable();
    update.decode_recipe =
        crate::storage::RowDecodePlan::try_compile_cached(&columns, table.row_codec.as_ref());
    update.required_columns = columns.into();
    Some(crate::engine::api::PreparedRangeUpdate {
        table: Arc::clone(table),
        predicate,
        update,
    })
}
