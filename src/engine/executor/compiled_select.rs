//! Shared compiled SELECT artifacts and prepared-handle L0 memo execution.
//!
//! Background (sysbench `oltp_point_select` bottleneck B): every
//! COM_STMT_EXECUTE used to deep-clone the prepared AST twice (once in the
//! session command loop, once inside the lock-wait retry loop), clone the
//! whole `SessionContext` for expression-eval info, and re-run the planner.
//! Profiling showed the planner itself is negligible for point selects
//! (~0.3–0.5% of on-CPU) — the real waste is the per-execute allocation
//! churn from those clones. This module removes all of it: the first
//! execute compiles a reusable [`PreparedSelectMemo`] (held on the
//! `PreparedStatement`), and subsequent executes borrow it, bind the
//! parameters late (placeholders survive in the plan as `KeyPart::Param`),
//! and run the physical executor directly — no AST clone, no session
//! clone, no re-planning.
//!
//! The compiled runner is *differentially identical* to the generic
//! per-execute planner: it normalizes + plans exactly the same `Select`,
//! executes via the same [`execute_physical_select`] the shared plan cache
//! uses, and validates the memo against the same `(epoch, deps)` contract,
//! so a DDL that would change the plan invalidates the memo. Anything not
//! provably equivalent (writes, FOR UPDATE, derived/CTE/system/temp
//! tables, window functions, partition clauses) is rejected at compile
//! time and uses the statement-specific execution vehicle.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::engine::api::{
    PreparedSelectMemo, PreparedSelectTemplate, QueryResponse, SessionContext,
};
use crate::engine::executor::dispatch::{
    acquire_prepared_select_mdl, authorize_prepared_statement_with_epoch,
    prepared_required_privileges, prepared_statement_mdl_requests, select_has_window_functions,
    statement_refs_temp_table,
};
use crate::engine::executor::select::{
    collect_plan_deps, execute_compiled_select, is_system_table_query, plan_deps_match_catalog,
    plan_object_deps_match_catalog, prepare_select_for_planning,
};
use crate::parser::{Select, Statement};
use crate::storage::{Database, TxnView};
use crate::types::QueryError;

/// Structural eligibility: only the SELECT shapes that `execute_select`
/// routes straight to the planner + [`execute_physical_select`] are
/// candidates. Everything `execute_select` special-cases *before* the
/// plan-cache branch (VALUES / JSON_TABLE / derived / CTE-join / system /
/// const / partition-clause) or that needs a different execution vehicle
/// (FOR UPDATE, window functions) is excluded, so the compiled path
/// can never diverge from the generic dispatcher's executor choice.
fn eligible_select(statement: &Statement) -> Option<&Select> {
    let Statement::Select(select) = statement else {
        return None;
    };
    // A physical SELECT requires at least one primary source here. Join trees
    // are supported by the same compiled-plan cache and executor.
    if select.from.len() != 1 {
        return None;
    }
    let primary = select.from.first()?;
    // An explicit `PARTITION (...)` clause needs pruning that
    // `execute_select` installs before the planner — not modeled here.
    // (A `FROM (subquery)` derived source surfaces as `select.derived`,
    // excluded below.)
    if !primary.partitions.is_empty() {
        return None;
    }
    // Must resolve to a concrete table name (rules out `SELECT 1`, which
    // `execute_select` sends to `execute_const_select`).
    if select.table.is_none() {
        return None;
    }
    // Reads only: FOR UPDATE / FOR SHARE / LOCK take row locks and need the
    // lock-wait retry loop + transactional view, not the read-only view.
    if select.for_update || select.lock_clause.is_some() {
        return None;
    }
    if select.values.is_some() || select.json_table.is_some() || select.derived.is_some() {
        return None;
    }
    if select_has_window_functions(select) {
        return None;
    }
    Some(select)
}

/// Build the reusable semantic half of a prepared SELECT while returning the
/// first freshly costed physical artifact for immediate execution.  The
/// artifact is intentionally not stored in [`PreparedSelectTemplate`].
pub(super) fn compile_uncached_prepared_select(
    db: &Arc<Database>,
    session: &SessionContext,
    parsed: &Arc<Statement>,
    params: &[crate::types::Value],
    epoch: u64,
) -> Option<(
    Arc<PreparedSelectTemplate>,
    crate::engine::cache::CompiledSelectPlan,
)> {
    let select = eligible_select(parsed)?;
    if super::routines::statement_references_stored_function(db, &session.current_database, parsed)
    {
        return None;
    }
    if statement_refs_temp_table(session, parsed) {
        return None;
    }
    let table_name = select.table.as_deref()?;
    if is_system_table_query(table_name) || db.table(table_name).is_err() {
        return None;
    }

    let mut normalized = select.clone();
    super::dispatch::apply_sql_select_limit(session, &mut normalized);
    prepare_select_for_planning(db, &mut normalized).ok()?;
    let normalized = Arc::new(normalized);
    let physical = Arc::new(
        crate::planner::with_planning_params(params, || {
            crate::planner::plan_prepared_physical_select(db, &normalized)
        })
        .ok()?,
    );
    let deps = collect_plan_deps(&physical);
    let object_deps = crate::engine::cache::current_plan_object_deps();
    let semantic_statement = Statement::Select((*normalized).clone());
    let required_privileges: Arc<[crate::engine::auth::RequiredPrivilege]> =
        prepared_required_privileges(session, &semantic_statement).into();
    let mdl_requests: Arc<[crate::storage::MdlRequest]> =
        prepared_statement_mdl_requests(db, session, &semantic_statement).into();

    // The debug representation conservatively covers every owned textual AST
    // payload and node delimiter.  Add the concrete vector capacities retained
    // beside it.  The reservation is optional: under pressure this optimization
    // is skipped rather than exceeding the unified memory target.
    let ast_shape_bytes = format!("{normalized:?}").capacity();
    let reserved_bytes = ast_shape_bytes
        .saturating_add(std::mem::size_of::<PreparedSelectTemplate>())
        .saturating_add(deps.len() * std::mem::size_of::<crate::engine::cache::PlanDep>())
        .saturating_add(object_deps.len() * std::mem::size_of::<crate::engine::cache::ObjectDep>())
        .saturating_add(
            required_privileges.len()
                * std::mem::size_of::<crate::engine::auth::RequiredPrivilege>(),
        )
        .saturating_add(mdl_requests.len() * std::mem::size_of::<crate::storage::MdlRequest>())
        .saturating_add(session.sysvars.optimizer_switch.len());
    let memory_charge =
        crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
            reserved_bytes,
        )?;
    let template = Arc::new(PreparedSelectTemplate {
        select: Arc::clone(&normalized),
        deps,
        object_deps,
        semantics: super::plan_semantics(session),
        optimizer_switch: Arc::from(session.sysvars.optimizer_switch.as_str()),
        required_privileges,
        mdl_requests,
        authorization_account: session.authenticated_as.clone(),
        authorized_auth_epoch: std::sync::atomic::AtomicU64::new(0),
        _memory_charge: memory_charge,
    });
    // Operator lowering also proves that deferred index-key casts are exact.
    // Publish bindings only after physical planning; the proof depends on the
    // comparison domains tracked by the prepared memo's cache signature.
    let compiled = crate::engine::params::with_params_slice(params, || {
        crate::engine::cache::CompiledSelectPlan::new(
            normalized,
            physical,
            smallvec::SmallVec::new(),
            smallvec::SmallVec::new(),
            epoch,
        )
    });
    db.record_prepared_plan_compile();
    db.record_select_operator_compile();
    Some((template, compiled))
}

pub(super) fn uncached_template_is_valid(
    db: &Database,
    template: &PreparedSelectTemplate,
    session: &SessionContext,
) -> bool {
    semantics_match(&template.semantics, &template.optimizer_switch, session)
        && plan_deps_match_catalog(db, &template.deps)
        && plan_object_deps_match_catalog(db, &template.object_deps)
}

pub(super) fn semantics_match(
    semantics: &crate::engine::cache::PlanSemantics,
    optimizer_switch: &str,
    session: &SessionContext,
) -> bool {
    semantics.sql_mode_fingerprint == crate::binlog::sql_mode_str_to_bits(&session.sql_mode)
        && optimizer_switch == session.sysvars.optimizer_switch
        && semantics.sql_select_limit == session.sysvars.sql_select_limit
        && semantics.use_invisible_indexes == session.sysvars.use_invisible_indexes
        && semantics.time_zone.as_ref() == session.sysvars.time_zone
        && semantics.character_set_client.as_ref() == session.character_set_client
        && semantics.character_set_connection.as_ref() == session.character_set_connection
        && semantics.collation_connection.as_ref() == session.collation_connection
        && semantics.div_precision_increment
            == session.sysvars.div_precision_increment.min(30) as u8
        && semantics.group_concat_max_len == session.group_concat_max_len
}

/// Re-cost and compile an access path from a normalized template.  No result
/// from this function is retained by the prepared handle.
pub(super) fn compile_uncached_physical_select(
    db: &Arc<Database>,
    template: &PreparedSelectTemplate,
    params: &[crate::types::Value],
    epoch: u64,
) -> Result<crate::engine::cache::CompiledSelectPlan, QueryError> {
    let physical = Arc::new(crate::planner::with_planning_params(params, || {
        crate::planner::plan_prepared_physical_select(db, &template.select)
    })?);
    db.record_prepared_plan_compile();
    db.record_select_operator_compile();
    Ok(crate::engine::params::with_params_slice(params, || {
        crate::engine::cache::CompiledSelectPlan::new(
            Arc::clone(&template.select),
            physical,
            smallvec::SmallVec::new(),
            smallvec::SmallVec::new(),
            epoch,
        )
    }))
}

/// Compile a reusable plan for an eligible prepared SELECT. Returns `None`
/// when the statement needs another execution vehicle (the caller caches this as
/// `Ineligible` so it isn't re-attempted) or cannot be planned right now.
///
/// `epoch` is the engine plan-cache epoch snapshot stamped into the memo;
/// the caller compares it on every reuse so any DDL invalidates the plan.
#[cfg(test)]
pub(super) fn compile_prepared_select(
    db: &Arc<Database>,
    session: &SessionContext,
    parsed: &Arc<Statement>,
    params: &[crate::types::Value],
    plan_cache: &crate::engine::cache::PlanCache,
) -> Option<PreparedSelectMemo> {
    compile_prepared_select_from_memo(db, session, parsed, params, plan_cache, None)
}

pub(super) fn compile_prepared_select_from_memo(
    db: &Arc<Database>,
    session: &SessionContext,
    parsed: &Arc<Statement>,
    params: &[crate::types::Value],
    plan_cache: &crate::engine::cache::PlanCache,
    previous: Option<&PreparedSelectMemo>,
) -> Option<PreparedSelectMemo> {
    let previous = previous.filter(|memo| {
        memo_catalog_is_valid(db, memo, plan_cache.current_epoch())
            && semantics_match(&memo.semantics, &memo.optimizer_switch, session)
            && memo.authorization_account == session.authenticated_as
    });
    let (normalized, structural_fingerprint) = if let Some(previous) = previous {
        // Semantic preparation is parameter-independent (also used by the
        // cache-off template). Only the physical choice changes with a tier.
        // Retain current LIMIT validation, normally done during preparation.
        crate::engine::params::validate_limit(previous.presentation_select.limit.as_ref()).ok()?;
        crate::engine::params::validate_limit(previous.presentation_select.offset.as_ref()).ok()?;
        (
            Arc::clone(&previous.presentation_select),
            Arc::clone(&previous.structural_fingerprint),
        )
    } else {
        let select = eligible_select(parsed)?;
        if super::routines::statement_references_stored_function(
            db,
            &session.current_database,
            parsed,
        ) {
            return None;
        }
        // Temp tables get re-qualified per execute; keep them out of the
        // cross-session compiled cache (matches `should_use_shared_plan_cache`).
        if statement_refs_temp_table(session, parsed) {
            return None;
        }
        let table_name = select.table.as_deref()?;
        // `information_schema` / `performance_schema` / `mysql` dictionary reads
        // go through `execute_system_select`, not the physical executor.
        if is_system_table_query(table_name) {
            return None;
        }
        // Guard the skipped view expansion: the prepared AST is qualified but not
        // view-expanded, so a view reference (which is not a base table) must not
        // take this path. A real base table resolves here.
        db.table(table_name).ok()?;
        // Normalize (predicate rewrite / decorrelation) once, exactly as
        // `execute_select` does per execute, then plan. The placeholders are
        // preserved as deferred `KeyPart::Param` parts in the resulting plan.
        let mut normalized = select.clone();
        super::dispatch::apply_sql_select_limit(session, &mut normalized);
        prepare_select_for_planning(db, &mut normalized).ok()?;
        let semantic_statement = Statement::Select(normalized.clone());
        let fingerprint = crate::engine::auto_param::structural_fingerprint(&semantic_statement);
        (Arc::new(normalized), Arc::<str>::from(fingerprint))
    };
    let selectivity = crate::planner::parameter_selectivity_signature(db, &normalized, params);
    let mut parameter_plan_signature =
        crate::engine::auto_param::parameter_plan_signature(&normalized, params);
    let mut typed_fingerprint = structural_fingerprint.to_string();
    typed_fingerprint.push(':');
    typed_fingerprint.push_str(&parameter_plan_signature);
    typed_fingerprint.push_str(&selectivity);
    let key = crate::engine::cache::StatementKey {
        sql: Arc::from(typed_fingerprint),
        database: Arc::from(session.current_database.as_str()),
        semantics: super::plan_semantics(session),
    };
    let compiled = if let Some(compiled) =
        plan_cache.get_select(&key, &normalized, |deps, objects| {
            plan_deps_match_catalog(db, deps) && plan_object_deps_match_catalog(db, objects)
        }) {
        db.record_select_compiled_plan_hit();
        compiled
    } else {
        // Character-column equality is only index-semantic when the bound
        // parameter is in the string comparison domain. The planning scope
        // also supplies values to Zone Map costing, without exposing them to
        // general constant folding or replacing deferred execution bounds.
        let physical = Arc::new(
            crate::planner::with_planning_params(params, || {
                crate::planner::plan_prepared_physical_select(db, &normalized)
            })
            .ok()?,
        );
        let deps = collect_plan_deps(&physical);
        let objects = previous
            .map_or_else(crate::engine::cache::current_plan_object_deps, |memo| {
                memo.compiled.object_deps.clone()
            });
        db.record_prepared_plan_compile();
        db.record_select_operator_compile();
        crate::engine::params::with_params_slice(params, || {
            plan_cache.insert(key, Arc::clone(&normalized), physical, deps, objects)
        })
    };
    if let Some(previous) = previous {
        compiled.initialize_execution_envelope(
            previous.compiled.required_privileges().into(),
            previous.compiled.mdl_requests().into(),
        );
    } else {
        let statement = Statement::Select((*normalized).clone());
        let required_privileges = prepared_required_privileges(session, &statement);
        let mdl_requests = prepared_statement_mdl_requests(db, session, &statement);
        compiled.initialize_execution_envelope(required_privileges.into(), mdl_requests.into());
    }
    if !has_parameter_independent_cardinality(&compiled) {
        parameter_plan_signature.push_str(&selectivity);
    }
    Some(PreparedSelectMemo {
        semantics: super::plan_semantics(session),
        optimizer_switch: Arc::from(session.sysvars.optimizer_switch.as_str()),
        compiled,
        presentation_select: normalized,
        structural_fingerprint,
        parameter_plan_signature: Arc::from(parameter_plan_signature),
        authorization_account: session.authenticated_as.clone(),
        authorized_auth_epoch: std::sync::atomic::AtomicU64::new(0),
        alternates: parking_lot::Mutex::new(Vec::new()),
    })
}

/// Alternate memos retained per prepared handle.
const MAX_ALTERNATE_MEMOS: usize = 3;

/// Make `memo` the active memo of its handle, keeping `previous` (and the
/// alternates it carried) available for a later switch back.
pub(super) fn adopt_previous_memo(memo: &PreparedSelectMemo, previous: &Arc<PreparedSelectMemo>) {
    let mut carried = std::mem::take(&mut *previous.alternates.lock());
    carried.insert(0, Arc::clone(previous));
    carried.truncate(MAX_ALTERNATE_MEMOS);
    *memo.alternates.lock() = carried;
}

/// A still-valid memo of this handle for `params`: the active one, or an
/// alternate compiled for the current parameter tier (which then becomes
/// active). `None` means the caller must compile.
///
/// Prepared statements are per-session handles, but the memo epoch is the
/// same process-wide statement/plan-cache epoch bumped before DDL executes,
/// so a mismatch forces recompile. Engine-internal tests can execute DDL
/// without the MySQL session invalidation hook, so an equal epoch still checks
/// the single eligible base table by catalog key/version. That remains O(1)
/// and avoids a whole-catalog clone/table-id scan on every execute.
pub(super) fn reusable_memo(
    db: &Database,
    rp: &Arc<PreparedSelectMemo>,
    current_epoch: u64,
    session: &SessionContext,
    params: &[crate::types::Value],
) -> Option<Arc<PreparedSelectMemo>> {
    let mut selectivity = None;
    let valid = |memo: &PreparedSelectMemo, selectivity: &mut Option<String>| {
        semantics_match(&memo.semantics, &memo.optimizer_switch, session)
            && memo_catalog_is_valid(db, memo, current_epoch)
            && *memo.parameter_plan_signature
                == current_parameter_signature(db, memo, params, selectivity)
    };
    if valid(rp, &mut selectivity) {
        return Some(Arc::clone(rp));
    }
    let mut alternates = rp.alternates.lock();
    let position = alternates
        .iter()
        .position(|memo| valid(memo, &mut selectivity))?;
    let chosen = alternates.remove(position);
    drop(alternates);
    adopt_previous_memo(&chosen, rp);
    Some(chosen)
}

/// The signature `memo` would record for `params`. The selectivity tier is
/// computed at most once per EXECUTE and shared by every candidate memo.
fn current_parameter_signature(
    db: &Database,
    memo: &PreparedSelectMemo,
    params: &[crate::types::Value],
    selectivity: &mut Option<String>,
) -> String {
    let mut signature =
        crate::engine::auto_param::parameter_plan_signature(&memo.presentation_select, params);
    if !has_parameter_independent_cardinality(&memo.compiled) {
        signature.push_str(selectivity.get_or_insert_with(|| {
            crate::planner::parameter_selectivity_signature(db, &memo.presentation_select, params)
        }));
    }
    signature
}

/// The lowered operator already proved an exact, residual-free unique probe.
/// Individual columns of a composite key may be highly skewed, but changing
/// their values cannot turn this probe into a broad scan. Keep domain and
/// LIMIT/OFFSET guards, without re-estimating those marginal frequencies on
/// every EXECUTE. Other operators retain parameter-sensitive costing.
fn has_parameter_independent_cardinality(
    compiled: &crate::engine::cache::CompiledSelectPlan,
) -> bool {
    matches!(
        compiled.executable.as_ref(),
        crate::engine::cache::CompiledOperator::ProjectedUniquePointLookup(_)
    )
}

fn memo_catalog_is_valid(db: &Database, rp: &PreparedSelectMemo, current_epoch: u64) -> bool {
    rp.compiled.epoch == current_epoch
        && rp.compiled.cbo_epoch == crate::engine::cbo::cbo_epoch()
        && plan_deps_match_catalog(db, &rp.compiled.deps)
        && plan_object_deps_match_catalog(db, &rp.compiled.object_deps)
}

/// Execute a validated [`PreparedSelectMemo`]. Mirrors
/// `execute_mdl_protected_statement` + the `Statement::Select` dispatch arm's
/// autocommit read-only branch: authorize, acquire statement MDL, take a
/// read-only statement view, run the physical plan, release MDL. The caller
/// has already installed the bind parameters and session expr-info context.
///
/// Takes `&SessionContext` only — every step on this path reads the session
/// immutably, which is what lets the caller avoid a `session.clone()`.
pub(super) fn run_compiled_select(
    db: &Arc<Database>,
    session: &SessionContext,
    rp: &PreparedSelectMemo,
    txn: TxnView,
    connection_id: u64,
) -> Result<QueryResponse, QueryError> {
    let _row_lock_timeout =
        super::tx::select_lock_wait_timeout_guard(session, &rp.presentation_select);
    let select = &rp.presentation_select;
    authorize_prepared_select(db, session, rp)?;
    run_checked_compiled_select(
        db,
        session,
        select,
        &rp.compiled,
        txn,
        connection_id,
        rp.compiled.mdl_requests(),
        &rp.compiled.deps,
        &rp.compiled.object_deps,
    )
}

/// Execute one freshly planned artifact through the cached semantic envelope.
/// MDL and authorization remain live checks on every execution exactly as on
/// the shared-plan path.
pub(super) fn run_uncached_compiled_select(
    db: &Arc<Database>,
    session: &SessionContext,
    template: &PreparedSelectTemplate,
    compiled: &crate::engine::cache::CompiledSelectPlan,
    txn: TxnView,
    connection_id: u64,
) -> Result<QueryResponse, QueryError> {
    let _row_lock_timeout = super::tx::select_lock_wait_timeout_guard(session, &template.select);
    authorize_prepared_template(db, session, template)?;
    run_checked_compiled_select(
        db,
        session,
        &template.select,
        compiled,
        txn,
        connection_id,
        &template.mdl_requests,
        &template.deps,
        &template.object_deps,
    )
}

/// Authorization and timeout scope belong to the caller; both artifacts
/// acquire MDL before checking dependencies or charging a question.
#[inline]
fn run_checked_compiled_select(
    db: &Arc<Database>,
    session: &SessionContext,
    select: &Select,
    compiled: &crate::engine::cache::CompiledSelectPlan,
    txn: TxnView,
    connection_id: u64,
    mdl_requests: &[crate::storage::MdlRequest],
    deps: &[crate::engine::cache::PlanDep],
    object_deps: &[crate::engine::cache::ObjectDep],
) -> Result<QueryResponse, QueryError> {
    let mdl_guard = acquire_prepared_select_mdl(db, session, select, mdl_requests)?;
    let mdl_scope = super::stream_mdl::StatementScope::enter(db, connection_id);
    let result =
        if !plan_deps_match_catalog(db, deps) || !plan_object_deps_match_catalog(db, object_deps) {
            Err(crate::storage::error(
                crate::error::catalog::ER_NEED_REPREPARE,
                "table definition changed before compiled SELECT acquired metadata locks",
            ))
        } else {
            charge_prepared_select_question(session)
                .and_then(|()| execute_compiled_select(db, txn, select, compiled))
        };
    // Preserve transaction-duration tickets, atomic fast grants and the
    // ticketed statement-duration release policy carried by the guard.
    super::stream_mdl::finish(db, connection_id, mdl_guard, result, mdl_scope.outermost())
}

// Charge only after MDL/dependency validation: the protocol retries pending
// locks and stale plans, and those pre-execution attempts must not charge twice.
fn charge_prepared_select_question(session: &SessionContext) -> Result<(), QueryError> {
    if session.internal_system_session {
        return Ok(());
    }
    crate::engine::auth::charge_question(
        &session.authenticated_as,
        session.account_limits.max_questions,
    )
}

fn authorize_prepared_template(
    db: &Arc<Database>,
    session: &SessionContext,
    template: &PreparedSelectTemplate,
) -> Result<(), QueryError> {
    if prepared_select_auth_cache_allowed(session)
        && template.authorization_account == session.authenticated_as
    {
        db.ensure_operational()?;
        let current_epoch = crate::engine::auth::authorization_generation();
        if current_epoch != 0
            && template.authorized_auth_epoch.load(Ordering::Acquire) == current_epoch
        {
            return Ok(());
        }
    }
    let authorized_epoch = authorize_prepared_statement_with_epoch(
        db,
        session,
        &Statement::Select((*template.select).clone()),
        &template.required_privileges,
    )?;
    if prepared_select_auth_cache_allowed(session)
        && template.authorization_account == session.authenticated_as
    {
        template
            .authorized_auth_epoch
            .store(authorized_epoch, Ordering::Release);
    }
    Ok(())
}

fn authorize_prepared_select(
    db: &Arc<Database>,
    session: &SessionContext,
    rp: &PreparedSelectMemo,
) -> Result<(), QueryError> {
    if prepared_select_auth_cache_allowed(session)
        && rp.authorization_account == session.authenticated_as
    {
        db.ensure_operational()?;
        let current_epoch = crate::engine::auth::authorization_generation();
        if current_epoch != 0 && rp.authorized_auth_epoch.load(Ordering::Acquire) == current_epoch {
            return Ok(());
        }
    }

    let authorized_epoch = authorize_prepared_statement_with_epoch(
        db,
        session,
        &Statement::Select((*rp.presentation_select).clone()),
        rp.compiled.required_privileges(),
    )?;
    if prepared_select_auth_cache_allowed(session)
        && rp.authorization_account == session.authenticated_as
    {
        rp.authorized_auth_epoch
            .store(authorized_epoch, Ordering::Release);
    }
    Ok(())
}

fn prepared_select_auth_cache_allowed(session: &SessionContext) -> bool {
    session.active_roles.is_empty()
        && session.view_security_overrides.is_empty()
        && session.temp_tables.is_empty()
}
