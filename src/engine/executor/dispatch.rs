pub(crate) use setop::{
    SETOP_SPILL_BYTES_READ, SETOP_SPILL_BYTES_WRITTEN, SETOP_SPILL_COUNT, SETOP_SPILL_FILE_OPENS,
    SETOP_SPILL_MAX_DEPTH, SETOP_SPILL_MERGE_PASSES, SETOP_SPILL_PARTITIONS,
    SETOP_SPILL_REPARTITIONS,
};
#[path = "alter_plan.rs"]
mod alter_plan;
use alter_plan::{alter_table_statement_mdl, plan_persistent_alter};
mod mdl;
pub(super) use mdl::{
    acquire_prepared_select_mdl, acquire_prepared_statement_mdl,
    acquire_serializable_select_table_locks, collect_routine_table_uses, collect_select_table_uses,
    collect_statement_table_uses, prepared_statement_mdl_requests, PreparedMdlGuard,
};
use mdl::{
    authorize_view_references_and_collect_security, collect_expr_table_uses,
    collect_statement_table_uses_scoped, drop_database_mdl_requests,
    execute_mdl_protected_statement, statement_invokes_trigger,
};
mod views;
use views::{
    effective_view_security_account, materialize_alter_view_options, materialize_view_options,
    reject_unsupported_view_query, revalidate_altered_table_views, stored_view_query, trail_key,
    validate_view_definition, view_query_from_query, warn_if_view_definer_missing,
};
pub(super) use views::{
    expand_views, revalidate_routine_views, rewrite_delete_through_view,
    rewrite_insert_through_view, rewrite_update_through_view,
};
mod setop;
pub(in crate::engine::executor) use setop::{coerce_setop_value, set_row_key};
pub(crate) use setop::{
    combine_set_rows_spillable_charged, combine_set_sources_spillable,
    unify_setop_declared_column_metadata, IncrementalSetOutput,
};
pub(super) use setop::{
    execute_set_operation_branch_response, execute_set_operation_dispatch, execute_subquery,
    execute_subquery_response, response_into_cursor,
};
use setop::{inline_single_use_with_in_select, synthetic_set_op_schema};
pub(super) mod ai;
mod session;
use session::{
    apply_autocommit, apply_transaction_characteristics, apply_transaction_isolation_scope,
    execute_reset_persist, execute_set_character_set_statement, execute_set_names_statement,
    parser_isolation_to_storage,
};
pub(super) use session::{
    apply_sql_select_limit, enforce_account_resource_limits, enforce_session_write_access,
    ensure_xa_statement_allowed, execute_set_statement,
};

mod window;
pub(crate) use window::{
    cancellable_window_sort, compute_window_partitions, evaluate_window_prefix_values,
    evaluate_window_with_partitions, window_parallel_degree, RankKind, WindowPrefixState,
};
use window::{
    eval_expr_against_columns, execute_select_with_window, expr_column_position, window_const_i64,
};
pub(super) use window::{
    execute_windowed_select, execute_windowed_select_for_analyze, plan_windowed_select_for_explain,
    rollup_window_topk_hint_for_window_derived, select_has_window_functions, set_window_topk_hint,
};

use crate::engine::auth::{grant_option_level, scope_equal};
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use super::ddl_deprecation::{
    emit as emit_copied_numeric_deprecations,
    for_column_def as copied_numeric_deprecations_for_column_def,
    for_schema as copied_numeric_deprecations_for_schema,
};
use super::qualification::{
    qualify_alter_rename_target, qualify_persistent_table_name, qualify_statement_tables,
    qualify_table_name,
};
use super::*;
use crate::engine::auth::{
    account_id, checked_password_auth_string, current_time_ms, default_auth_plugin,
    privilege_denied, AccountKey, GrantEntry, GrantOrigin, Privilege, PrivilegeScope,
    RequiredPrivilege, RoleEdge, SslRequirement, UserAccount,
};
use crate::engine::session_registry::SessionRegistry;
use crate::parser::{
    AccountIdent, AlterLock, AlterTable, GrantKind, GrantScope, Join, JoinType, KillTarget,
    PrivilegeSpec, RoutineSecurity, SetAssignment, SetDefaultRole, SetRole, SetScope,
    TempTableOnCommit, UserSslRequirement,
};
use crate::storage::{
    compile_index_key_expressions, RoutineKindSchema, RoutineSecuritySchema, ViewColumnDef,
    ViewDependency, ViewRoutineDependency, ViewSchema,
};

/// Look up the table and compile every functional key part in
/// `parser_key_expressions` against it. ALTER ADD INDEX / CREATE INDEX
/// share this helper; CREATE TABLE compiles inline against the freshly
/// built [`TableSchema`] in `storage::Database::create_table`.
fn build_index_key_expressions(
    db: &Database,
    table_name: &str,
    columns: &[String],
    parser_key_expressions: &[Option<String>],
    parser_key_expression_multi_valued: &[bool],
) -> Result<Vec<Option<crate::storage::IndexExpression>>, QueryError> {
    if parser_key_expressions.iter().all(Option::is_none) {
        return Ok(Vec::new());
    }
    let table = db.table(table_name)?;
    let slots = parser_key_expressions
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            (
                expr.clone(),
                parser_key_expression_multi_valued
                    .get(idx)
                    .copied()
                    .unwrap_or(false),
            )
        })
        .collect::<Vec<_>>();
    compile_index_key_expressions(&table, columns, &slots)
}

/// Map an ALTER action to the cheapest algorithm mydb can use for it.
/// `INSTANT` means catalog-only; `INPLACE` means same-table heap work
/// (e.g. a heap scan to populate a new index). DROP INDEX is INSTANT
/// even though it touches a file because no row data moves.
/// F1: map a partition action (from `stmt.partition_actions`) to the
/// cheapest algorithm mydb can honour. COALESCE/REORGANIZE/REPARTITION/
/// REMOVE PARTITIONING physically move every row and are COPY; ADD/
/// DROP/TRUNCATE/ANALYZE/CHECK/OPTIMIZE/REPAIR/REBUILD/EXCHANGE work
/// at partition-file granularity and are INPLACE.
fn algorithm_for_partition_action(
    action: &crate::parser_partition::PartitionAction,
) -> AlterAlgorithm {
    use crate::parser_partition::PartitionAction as PA;
    match action {
        PA::CoalescePartitions(_)
        | PA::ReorganizePartitions { .. }
        | PA::PartitionBy(_)
        | PA::AddPartitionCount(_)
        | PA::RemovePartitioning => AlterAlgorithm::Copy,
        PA::AddPartitions(_)
        | PA::DropPartitions(_)
        | PA::TruncatePartitions(_)
        | PA::ExchangePartition { .. }
        | PA::AnalyzePartitions(_)
        | PA::CheckPartitions(_)
        | PA::OptimizePartitions(_)
        | PA::RepairPartitions(_)
        | PA::RebuildPartitions(_) => AlterAlgorithm::Inplace,
    }
}

fn algorithm_for_action(action: &AlterAction) -> AlterAlgorithm {
    match action {
        AlterAction::AddColumn(column)
            if column.generated.is_some() || column.checks.iter().any(|check| check.enforced) =>
        {
            AlterAlgorithm::Copy
        }
        AlterAction::ModifyColumn(column) if column.checks.iter().any(|check| check.enforced) => {
            AlterAlgorithm::Inplace
        }
        AlterAction::ChangeColumn { new_def, .. }
            if new_def.checks.iter().any(|check| check.enforced) =>
        {
            AlterAlgorithm::Inplace
        }
        AlterAction::Noop { .. }
        | AlterAction::SetColumnVisibility { .. }
        | AlterAction::AddColumn(_)
        | AlterAction::DropColumn(_)
        | AlterAction::ModifyColumn(_)
        | AlterAction::ChangeColumn { .. }
        | AlterAction::RenameColumn { .. }
        | AlterAction::MoveColumn { .. }
        | AlterAction::SetColumnComment { .. }
        | AlterAction::SetColumnDefault { .. }
        | AlterAction::SetTableOptions { .. }
        | AlterAction::SetStorageProperties { .. }
        | AlterAction::DropIndex(_)
        | AlterAction::DropPrimaryKey
        | AlterAction::RenameIndex { .. }
        | AlterAction::RenameTable { .. }
        | AlterAction::AlterIndexVisibility { .. }
        | AlterAction::SetAutoIncrement(_) => AlterAlgorithm::Instant,
        AlterAction::AddCheck(check) if !check.enforced => AlterAlgorithm::Instant,
        AlterAction::AlterCheck {
            enforced: false, ..
        } => AlterAlgorithm::Instant,
        AlterAction::AddCheck(_) | AlterAction::AddIndex(_) | AlterAction::AlterCheck { .. } => {
            AlterAlgorithm::Inplace
        }
        AlterAction::Rebuild | AlterAction::ConvertToCharset { .. } => AlterAlgorithm::Copy,
        // ADD FOREIGN KEY may build a backing index and scans the child
        // heap for orphan rows — same-table work, so INPLACE.
        AlterAction::AddForeignKey(_) => AlterAlgorithm::Inplace,
        AlterAction::DropCheck { .. } | AlterAction::DropForeignKey { .. } => {
            AlterAlgorithm::Instant
        }
    }
}

fn algorithm_label(algo: AlterAlgorithm) -> &'static str {
    match algo {
        AlterAlgorithm::Default => "DEFAULT",
        AlterAlgorithm::Instant => "INSTANT",
        AlterAlgorithm::Inplace => "INPLACE",
        AlterAlgorithm::Copy => "COPY",
    }
}

fn alter_lock_label(lock: AlterLock) -> &'static str {
    match lock {
        AlterLock::Default => "DEFAULT",
        AlterLock::None => "NONE",
        AlterLock::Shared => "SHARED",
        AlterLock::Exclusive => "EXCLUSIVE",
    }
}

fn alter_algorithm_rank(algo: AlterAlgorithm) -> u8 {
    match algo {
        AlterAlgorithm::Instant | AlterAlgorithm::Default => 0,
        AlterAlgorithm::Inplace => 1,
        AlterAlgorithm::Copy => 2,
    }
}

/// Reject a user-supplied ALGORITHM= hint when mydb cannot honour the
/// contract. Matches MySQL 8.0 semantics:
/// * `Default` always accepted (executor picks).
/// * `Instant` accepted only when the action is INSTANT-eligible.
/// * `Inplace` accepted for any action that does not require a shadow
///   table rewrite.
/// * `Copy` is accepted; the executor/storage layer may still run a
///   cheaper implementation for metadata-only actions.
pub(super) fn validate_alter_algorithm(
    requested: AlterAlgorithm,
    action: &AlterAction,
) -> Result<(), QueryError> {
    validate_alter_algorithm_for_required(requested, algorithm_for_action(action))
}

fn validate_alter_lock(requested: AlterLock) -> Result<(), QueryError> {
    match requested {
        AlterLock::Default | AlterLock::Exclusive => Ok(()),
        AlterLock::None | AlterLock::Shared => Err(error(
            crate::error::catalog::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON,
            format!(
                "LOCK={} is not supported for ALTER TABLE; mydb currently requires LOCK=EXCLUSIVE",
                alter_lock_label(requested),
            ),
        )),
    }
}

fn validate_alter_algorithm_for_required(
    requested: AlterAlgorithm,
    required: AlterAlgorithm,
) -> Result<(), QueryError> {
    match requested {
        AlterAlgorithm::Default | AlterAlgorithm::Copy => Ok(()),
        AlterAlgorithm::Instant => {
            if required == AlterAlgorithm::Instant {
                Ok(())
            } else {
                // MySQL 8.0 reports ER_ALTER_OPERATION_NOT_SUPPORTED_REASON
                // (1846 / 0A000) when a requested ALGORITHM cannot be
                // honoured — the same code the INPLACE branch below uses.
                // (The previous 4092 is not a code MySQL emits here, so
                // clients that special-case 1845/1846 to downgrade the
                // algorithm never matched it.)
                Err(error(
                    crate::error::catalog::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON,
                    format!(
                        "ALGORITHM={} is not supported for this operation. \
                         Try ALGORITHM={}.",
                        algorithm_label(requested),
                        algorithm_label(required),
                    ),
                ))
            }
        }
        AlterAlgorithm::Inplace => {
            if alter_algorithm_rank(required) <= alter_algorithm_rank(AlterAlgorithm::Inplace) {
                Ok(())
            } else {
                Err(error(
                    crate::error::catalog::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON,
                    format!(
                        "ALGORITHM={} is not supported for this operation; \
                         the engine requires {}",
                        algorithm_label(requested),
                        algorithm_label(required),
                    ),
                ))
            }
        }
    }
}

pub(super) fn is_ddl_statement(statement: &Statement) -> bool {
    crate::statement_effects::is_ddl_or_maintenance(statement)
}

fn is_session_temp_table_ref(session: &SessionContext, name: &str) -> bool {
    // Hot path: the vast majority of sessions never CREATE TEMPORARY TABLE.
    // Resolving a name to a temp-visible key allocates three Strings
    // (split_table_name + table_key); skipping the lookup when the temp
    // map is empty drops three per-statement allocations for every
    // table reference in the query.
    if session.temp_tables.is_empty() {
        return false;
    }
    session.resolve_temp_table_name(name).is_some()
        || session
            .temp_tables
            .values()
            .any(|table| table.internal_key.eq_ignore_ascii_case(name))
}

pub(super) fn statement_is_temp_ddl(session: &SessionContext, statement: &Statement) -> bool {
    match statement {
        Statement::CreateTable(stmt) => stmt.temporary,
        Statement::CreateTableLike { temporary, .. } => *temporary,
        Statement::CreateTableAsSelect { table, .. } => table.temporary,
        Statement::DropTable {
            temporary, names, ..
        } => {
            *temporary
                || names
                    .iter()
                    .any(|name| is_session_temp_table_ref(session, name))
        }
        Statement::TruncateTable { name } => is_session_temp_table_ref(session, name),
        Statement::AlterTable(stmt) => is_session_temp_table_ref(session, &stmt.table),
        Statement::CreateIndex(stmt) => is_session_temp_table_ref(session, &stmt.table),
        Statement::DropIndex { table, .. } => is_session_temp_table_ref(session, table),
        Statement::Explain { target, .. } => statement_is_temp_ddl(session, target),
        Statement::WithCte(with) => statement_is_temp_ddl(session, &with.body),
        Statement::SetOperation(_) => false,
        _ => false,
    }
}

pub(super) fn statement_refs_temp_table<'q>(
    session: &SessionContext,
    statement: impl Into<crate::parser::QueryRef<'q>>,
) -> bool {
    let statement = statement.into();
    if session.temp_tables.is_empty() {
        return false;
    }
    // The shared visitor includes JOIN-derived tables, assignment subqueries,
    // window expressions and other nested reads, not just the DML target.
    if collect_statement_table_uses(statement)
        .iter()
        .any(|usage| is_session_temp_table_ref(session, &usage.table))
    {
        return true;
    }
    match statement {
        crate::parser::QueryRef::Other(Statement::Insert(stmt)) => {
            is_session_temp_table_ref(session, &stmt.table)
                || match &stmt.source {
                    InsertSource::Query(query) => statement_refs_temp_table(session, query),
                    InsertSource::Values(rows) => rows
                        .iter()
                        .flatten()
                        .any(|expr| expr_refs_temp_table(session, expr)),
                }
        }
        crate::parser::QueryRef::Other(Statement::LoadData(stmt)) => {
            is_session_temp_table_ref(session, &stmt.table)
        }
        crate::parser::QueryRef::Other(Statement::Update(stmt)) => {
            is_session_temp_table_ref(session, &stmt.table)
                || stmt.joins.iter().any(|join| {
                    is_session_temp_table_ref(session, &join.table.name)
                        || join
                            .condition
                            .as_ref()
                            .is_some_and(|expr| expr_refs_temp_table(session, expr))
                })
                || stmt
                    .selection
                    .as_ref()
                    .is_some_and(|expr| expr_refs_temp_table(session, expr))
        }
        crate::parser::QueryRef::Other(Statement::Delete(stmt)) => {
            is_session_temp_table_ref(session, &stmt.table)
                || stmt.joins.iter().any(|join| {
                    is_session_temp_table_ref(session, &join.table.name)
                        || join
                            .condition
                            .as_ref()
                            .is_some_and(|expr| expr_refs_temp_table(session, expr))
                })
                || stmt
                    .selection
                    .as_ref()
                    .is_some_and(|expr| expr_refs_temp_table(session, expr))
        }
        crate::parser::QueryRef::Select(stmt) => select_refs_temp_table(session, stmt),
        crate::parser::QueryRef::Rrf(rrf) => rrf
            .branches
            .iter()
            .any(|branch| statement_refs_temp_table(session, &branch.query)),
        crate::parser::QueryRef::SetOperation(set) => {
            statement_refs_temp_table(session, &set.left)
                || statement_refs_temp_table(session, &set.right)
        }
        crate::parser::QueryRef::WithCte(with) => {
            with.ctes
                .iter()
                .any(|cte| statement_refs_temp_table(session, &cte.query))
                || statement_refs_temp_table(session, &with.body)
        }
        crate::parser::QueryRef::Other(Statement::Explain { target, .. }) => {
            statement_refs_temp_table(session, target)
        }
        crate::parser::QueryRef::Other(Statement::SelectIntoVars { inner, .. })
        | crate::parser::QueryRef::Other(Statement::SelectIntoFile { inner, .. }) => {
            statement_refs_temp_table(session, inner)
        }
        crate::parser::QueryRef::Other(Statement::CreateTableLike { source, .. }) => {
            is_session_temp_table_ref(session, source)
        }
        crate::parser::QueryRef::Other(Statement::CreateTableAsSelect { query, .. }) => {
            statement_refs_temp_table(session, query)
        }
        crate::parser::QueryRef::Other(Statement::Describe { table })
        | crate::parser::QueryRef::Other(Statement::ShowColumns { table, .. })
        | crate::parser::QueryRef::Other(Statement::ShowIndex { table, .. })
        | crate::parser::QueryRef::Other(Statement::ShowCreateTable { table }) => {
            is_session_temp_table_ref(session, table)
        }
        crate::parser::QueryRef::Other(statement) => statement_is_temp_ddl(session, statement),
    }
}

fn select_refs_temp_table(session: &SessionContext, select: &Select) -> bool {
    let mut visitor = TempTableReferences {
        session,
        found: false,
    };
    crate::parser::visit::visit_select(&mut visitor, select);
    visitor.found
}

fn expr_refs_temp_table(session: &SessionContext, expr: &Expr) -> bool {
    let mut visitor = TempTableReferences {
        session,
        found: false,
    };
    crate::parser::visit::visit_expr(&mut visitor, expr);
    visitor.found
}

struct TempTableReferences<'a> {
    session: &'a SessionContext,
    found: bool,
}
impl crate::parser::visit::AstVisitor for TempTableReferences<'_> {
    fn enter_select(&mut self, select: &Select) {
        if !self.found {
            self.found = select
                .from
                .iter()
                .chain(select.joins.iter().map(|j| &j.table))
                .any(|table| is_session_temp_table_ref(self.session, &table.name));
        }
    }
    fn walk_select_children(&self, _: &Select) -> bool {
        !self.found
    }
    fn walk_expr_children(&self, _: &Expr) -> bool {
        !self.found
    }
}

/// True when `statement` cannot generate WAL or otherwise mutate
/// database state. Used to classify statement batches (e.g. whether a
/// multi-statement packet contains any write). Checkpoint triggering no
/// longer hangs off this — it moved to the background checkpointer.
///
/// Conservative on purpose: anything that could mutate the database, lock
/// rows, run user-defined statements, or change session state we don't
/// fully understand returns `false`.
pub(crate) fn statement_is_pure_read<'q>(
    statement: impl Into<crate::parser::QueryRef<'q>>,
) -> bool {
    let statement = statement.into();
    match statement {
        crate::parser::QueryRef::Select(stmt) => !stmt.for_update,
        crate::parser::QueryRef::Other(Statement::Explain {
            target, analyze, ..
        }) => !*analyze || statement_is_pure_read(target),
        crate::parser::QueryRef::WithCte(stmt) => statement_is_pure_read(&stmt.body),
        crate::parser::QueryRef::Rrf(rrf) => rrf
            .branches
            .iter()
            .all(|branch| statement_is_pure_read(&branch.query)),
        crate::parser::QueryRef::SetOperation(stmt) => {
            statement_is_pure_read(&stmt.left) && statement_is_pure_read(&stmt.right)
        }
        crate::parser::QueryRef::Other(Statement::Describe { .. })
        | crate::parser::QueryRef::Other(Statement::ShowDatabases { .. })
        | crate::parser::QueryRef::Other(Statement::ShowTables { .. })
        | crate::parser::QueryRef::Other(Statement::ShowTableStatus { .. })
        | crate::parser::QueryRef::Other(Statement::ShowColumns { .. })
        | crate::parser::QueryRef::Other(Statement::ShowIndex { .. })
        | crate::parser::QueryRef::Other(Statement::ShowCreateDatabase { .. })
        | crate::parser::QueryRef::Other(Statement::ShowCreateTable { .. })
        | crate::parser::QueryRef::Other(Statement::ShowCreateRoutine { .. })
        | crate::parser::QueryRef::Other(Statement::ShowRoutineStatus { .. })
        | crate::parser::QueryRef::Other(Statement::ShowCreateTrigger { .. })
        | crate::parser::QueryRef::Other(Statement::ShowTriggers { .. })
        | crate::parser::QueryRef::Other(Statement::ShowCreateEvent { .. })
        | crate::parser::QueryRef::Other(Statement::ShowEvents { .. })
        | crate::parser::QueryRef::Other(Statement::ShowCharset { .. })
        | crate::parser::QueryRef::Other(Statement::ShowCollation { .. })
        | crate::parser::QueryRef::Other(Statement::ShowVariables { .. })
        | crate::parser::QueryRef::Other(Statement::ShowStatus { .. })
        | crate::parser::QueryRef::Other(Statement::ShowDiagnostics { .. })
        | crate::parser::QueryRef::Other(Statement::ShowCountDiagnostics { .. })
        | crate::parser::QueryRef::Other(Statement::ShowEngines)
        | crate::parser::QueryRef::Other(Statement::ShowEngineMydbStatus)
        | crate::parser::QueryRef::Other(Statement::ShowMydbMemory)
        | crate::parser::QueryRef::Other(Statement::ShowMydbMemorySessions)
        | crate::parser::QueryRef::Other(Statement::ShowEngine { .. })
        | crate::parser::QueryRef::Other(Statement::ShowPlugins)
        | crate::parser::QueryRef::Other(Statement::ShowOpenTables { .. })
        | crate::parser::QueryRef::Other(Statement::ShowProcesslist { .. })
        | crate::parser::QueryRef::Other(Statement::ShowGrants)
        | crate::parser::QueryRef::Other(Statement::ShowGrantsFor { .. })
        | crate::parser::QueryRef::Other(Statement::ShowCreateUser { .. })
        | crate::parser::QueryRef::Other(Statement::BinlogAdmin { .. })
        | crate::parser::QueryRef::Other(Statement::ShowPrivileges) => true,
        _ => false,
    }
}

pub(super) fn execute_statement(
    db: &Arc<Database>,
    session: &mut SessionContext,
    statement: Statement,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    // A stored function may execute DML from a SELECT expression. Its writes
    // must see the complete current maintenance/constraint index set, never
    // the outer read's snapshot-filtered catalog descriptor.
    let _read_schema = crate::storage::read_schema::ReadSchemaScope::enter(None);
    let _metadata_timeout =
        crate::storage::MetadataWaitTimeoutGuard::new(session.sysvars.lock_wait_timeout);
    // Set operations and planned CTEs can open physical scans without ever
    // entering execute_select. Install the registry at the statement boundary
    // before any plan captures its worker context.
    select::with_session_registry(registry.clone(), || {
        execute_statement_inner(db, session, statement, registry, false)
    })
}

fn execute_statement_inner(
    db: &Arc<Database>,
    session: &mut SessionContext,
    statement: Statement,
    registry: &SessionRegistry,
    inherit_view_security: bool,
) -> Result<QueryResponse, QueryError> {
    use crate::engine::session_registry::ProcessState;
    let _http_scope = crate::engine::http::enter(db);

    if session.current_database.is_empty()
        && auth_required_privileges(session, &statement)
            .iter()
            .any(|required| match &required.scope {
                PrivilegeScope::Database { database }
                | PrivilegeScope::Table { database, .. }
                | PrivilegeScope::Column { database, .. }
                | PrivilegeScope::Routine { database, .. } => database.is_empty(),
                _ => false,
            })
    {
        return Err(crate::error::catalog::ER_NO_DB_ERROR.error("No database selected"));
    }
    if db.is_snapshot_read_only() {
        crate::backup::native::enforce_statement(&statement)?;
    }
    crate::engine::cancel::set_process_base_state(super::process_state_for_statement(&statement));

    // Publish one connection-owned budget around the outermost data/query
    // statement. Nested SELECT/CTE paths inherit it, and streaming workers
    // keep the Arc alive until their final row is released.
    if crate::engine::executor::mem_budget::current_budget().is_none()
        && statement_uses_query_memory(&statement)
    {
        if let Some(handle) = session
            .cancel_handle
            .as_ref()
            .map(|handle| handle.session_handle_arc())
        {
            let budget = crate::engine::executor::mem_budget::MemBudget::for_session_with_database(
                handle, db,
            );
            return crate::engine::executor::mem_budget::with_query_budget(budget, || {
                execute_statement_inner(db, session, statement, registry, inherit_view_security)
                    .map_err(crate::engine::executor::mem_budget::terminal_oom)
            });
        }
        let budget = crate::engine::executor::mem_budget::MemBudget::for_database(db);
        return crate::engine::executor::mem_budget::with_query_budget(budget, || {
            execute_statement_inner(db, session, statement, registry, inherit_view_security)
                .map_err(crate::engine::executor::mem_budget::terminal_oom)
        });
    }
    ai::validate(session, &statement)?;
    publish_optimizer_hint_diagnostics(&statement);
    let _backup_ddl_admission = crate::backup::ddl_admission(db, &statement)?;
    {
        let _checking =
            crate::engine::cancel::enter_process_stage(ProcessState::CheckingPermissions);
        crate::replication::enforce_replica_read_only(&statement)?;
        crate::repl::enforce_role_read_only(&statement)?;
        crate::backup::wait_for_write_gate(
            db,
            session
                .current_txn
                .or_else(|| session.xa_branch.as_ref().map(|branch| branch.txn_id)),
            &statement,
        )?;
        crate::binlog::wait_for_global_read_lock_scoped(
            &statement,
            db.global_read_lock_scope(),
            session,
        )?;
        enforce_session_write_access(db, session, &statement)?;
        enforce_account_resource_limits(session, &statement)?;
    }
    // Propagate the session's `foreign_key_checks` to the storage layer
    // for the duration of this statement (and any cascades it triggers,
    // which run synchronously on this thread). The RAII guard restores the
    // previous thread-local on return so a pooled worker thread never leaks
    // an OFF setting into later internal/background DML.
    let _fk_checks_guard = crate::storage::FkChecksGuard::new(session.foreign_key_checks);
    // `SET @@SESSION.sql_log_bin = OFF` suppresses every row/DDL event
    // produced by this statement. The flag lives on the SessionContext;
    // we mirror it into a thread-local that the binlog record_* helpers
    // consult (they don't otherwise have access to the session).
    let _sql_log_bin_guard = crate::binlog::SessionSqlLogBinGuard::new(session.sql_log_bin);
    // Make the Database + session reachable to `nextval`/`currval`/`lastval`/
    // `setval` during expression evaluation in this statement (and any nested
    // sub-statements). SAFETY: `db` and `session` outlive this call; sequence
    // functions only touch `session.seq_state` (a `RefCell`) and read
    // `current_database`, never aliasing the `&mut session` used below.
    let _seq_ctx_guard = unsafe {
        crate::engine::expr::install_sequence_eval_context(
            std::ptr::NonNull::from(&**db),
            std::ptr::NonNull::from(&*session),
        )
    };
    // Make the Database + session + registry reachable to the expression
    // engine so a user-defined stored function used in an expression can be
    // run row-by-row (`eval_stored_function`). SAFETY: all three outlive this
    // call; statements referencing such functions are pinned to the serial
    // path below, so the `&mut session` re-borrow inside the callback never
    // aliases a concurrent use.
    let _stored_fn_ctx_guard = unsafe {
        crate::engine::expr::install_stored_fn_context(
            std::ptr::NonNull::from(db),
            std::ptr::NonNull::from(&mut *session),
            std::ptr::NonNull::from(registry),
            inherit_view_security,
        )
    };
    let mut statement = statement;
    match &statement {
        Statement::Insert(stmt) => reject_system_catalog_dml(db, session, &stmt.table)?,
        Statement::LoadData(stmt) => reject_system_catalog_dml(db, session, &stmt.table)?,
        Statement::Update(stmt) => reject_system_catalog_dml(db, session, &stmt.table)?,
        Statement::Delete(stmt) => reject_system_catalog_dml(db, session, &stmt.table)?,
        _ => {}
    }
    let check_view_name = match &statement {
        Statement::Insert(insert) => Some(insert.table.clone()),
        Statement::Update(update) => Some(update.table.clone()),
        _ => None,
    };
    let mut view_security_overrides =
        authorize_view_references_and_collect_security(db, session, &statement)?;
    // Re-entered CTE execution sees the already-expanded body. Retain the
    // enclosing statement's authorized view context until that statement ends.
    if inherit_view_security {
        view_security_overrides.extend(session.view_security_overrides.iter().cloned());
    }
    let saved_view_security_overrides = std::mem::replace(
        &mut session.view_security_overrides,
        view_security_overrides,
    );
    let result = (|| {
        let mut function_resolution =
            super::routines::resolve_stored_functions(db, session, &mut statement, registry)?;
        // Resolve user references before inserting persistently bound view bodies.
        qualify_statement_tables(db, session, &mut statement)?;
        super::routines::with_view_function_binding(|| {
            expand_views(db, session, &mut statement, 0)
        })?;
        let has_fusion = select::hybrid::statement_has_fusion(&statement);
        let _fulltext_scoring = select::fulltext_context::enter_fusion(has_fusion);
        if has_fusion {
            crate::parser::rrf::validate_statement(&statement, &|name| {
                [
                    crate::parser::USER_VAR_ASSIGN_FUNCTION,
                    "NEXTVAL",
                    "SETVAL",
                    "GET_LOCK",
                    "RELEASE_LOCK",
                    "RELEASE_ALL_LOCKS",
                ]
                .iter()
                .any(|function| name.eq_ignore_ascii_case(function))
                    || (db.has_stored_functions()
                        && (name.contains('.')
                            || !crate::engine::cbo::function_cost::is_known_builtin_function(name))
                        && (crate::engine::expr::resolved_view_stored_function(name).is_some()
                            || db
                                .routine_definition(
                                    &session.current_database,
                                    crate::storage::RoutineKindSchema::Function,
                                    name,
                                )
                                .is_some()))
            })
            .map_err(|error| error.spec.error(error.message))?;
        }
        function_resolution.merge(super::routines::resolve_stored_functions(
            db,
            session,
            &mut statement,
            registry,
        )?);
        enforce_stored_function_table_restrictions(
            session,
            &statement,
            &function_resolution.restricted_tables,
        )?;
        if matches!(statement, Statement::CreateTableAsSelect { .. })
            && function_resolution.tables.values().any(|write| *write)
        {
            return Err(
                crate::error::catalog::ER_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT.error(
                    "Can't update table through a stored function used by CREATE TABLE SELECT",
                ),
            );
        }
        // A row-by-row stored function re-enters the executor against the
        // owning session, which is only safe on the serial connection thread.
        let sequence_dependent = crate::parser::collect_statement_function_names(&statement)
            .iter()
            .any(|name| crate::storage::sequence::is_sequence_function(name));
        let http_dependent = crate::engine::http::statement_has_http(&statement);
        if http_dependent {
            crate::engine::http::activate();
        }
        let _serial_guard =
            (function_resolution.needs_serial || sequence_dependent || http_dependent)
                .then(crate::planner::force_serial_scope);
        // Even a read-only stored function may execute nested DO/SELECT
        // statements. Hold their transaction finalization until the owning
        // statement boundary; otherwise an inner autocommit statement can
        // commit the outer INSERT's transaction and leave its TxnView stale.
        let has_routine_calls =
            !function_resolution.routines.is_empty() || statement_invokes_trigger(db, &statement);
        // CALL is a sequence of SQL statements and may manage transactions.
        // Only functions/triggers belong to an atomic owning SQL statement.
        let response = if has_routine_calls && !matches!(statement, Statement::Call(_)) {
            super::tx::execute_atomic_routine_statement(db, session, |db, session| {
                execute_mdl_protected_statement(
                    db,
                    session,
                    statement,
                    registry,
                    &function_resolution.routines,
                    &function_resolution.tables,
                )
            })
        } else {
            execute_mdl_protected_statement(
                db,
                session,
                statement,
                registry,
                &function_resolution.routines,
                &function_resolution.tables,
            )
        };
        if sequence_dependent {
            response.and_then(QueryResponse::materialize)
        } else if http_dependent {
            response.map(crate::engine::http::wrap_response)
        } else {
            response
        }
    })();
    session.view_security_overrides = saved_view_security_overrides;
    result.map_err(|err| {
        if err.is(crate::error::catalog::ER_VIEW_CHECK_FAILED) {
            if let Some(name) = &check_view_name {
                let (database, name) =
                    crate::storage::split_table_name(&session.current_database, name);
                return error(
                    crate::error::catalog::ER_VIEW_CHECK_FAILED,
                    format!("CHECK OPTION failed '{}.{}'", database, name),
                );
            }
        }
        err
    })
}

fn enforce_stored_function_table_restrictions(
    session: &SessionContext,
    statement: &Statement,
    restricted_tables: &std::collections::BTreeMap<(String, String), bool>,
) -> Result<(), QueryError> {
    if restricted_tables.is_empty() {
        return Ok(());
    }
    for table_use in collect_statement_table_uses(statement) {
        let (database, table) =
            crate::storage::split_table_name(&session.current_database, &table_use.table);
        let writes_same_table =
            restricted_tables
                .iter()
                .any(|((routine_database, routine_table), write)| {
                    *write
                        && routine_database.eq_ignore_ascii_case(&database)
                        && routine_table.eq_ignore_ascii_case(&table)
                });
        if writes_same_table {
            return Err(error(
                crate::error::catalog::ER_CANT_UPDATE_USED_TABLE_IN_SF_OR_TRG,
                format!(
                    "Can't update table '{table}' in stored function/trigger because it is already used by statement which invoked this stored function/trigger",
                ),
            ));
        }
    }
    Ok(())
}

fn statement_uses_query_memory(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Select(_)
            | Statement::SetOperation(_)
            | Statement::WithCte(_)
            | Statement::Explain { .. }
            | Statement::SelectIntoVars { .. }
            | Statement::SelectIntoFile { .. }
            | Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            | Statement::Call(_)
            | Statement::Do(_)
            | Statement::LoadData(_)
    )
}

fn publish_optimizer_hint_diagnostics<'q>(statement: impl Into<crate::parser::QueryRef<'q>>) {
    let statement = statement.into();
    let (hints, warnings, ignored_kind): (&[crate::parser::QueryHint], &[String], Option<&str>) =
        match statement {
            crate::parser::QueryRef::Select(select) => {
                (&select.query_hints, &select.hint_warnings, None)
            }
            crate::parser::QueryRef::Other(Statement::Insert(insert)) => {
                (&insert.query_hints, &insert.hint_warnings, Some("INSERT"))
            }
            crate::parser::QueryRef::Other(Statement::Update(update)) => {
                (&update.query_hints, &update.hint_warnings, Some("UPDATE"))
            }
            crate::parser::QueryRef::Other(Statement::Delete(delete)) => {
                (&delete.query_hints, &delete.hint_warnings, Some("DELETE"))
            }
            crate::parser::QueryRef::Other(Statement::LoadData(load)) => {
                (&load.query_hints, &load.hint_warnings, None)
            }
            crate::parser::QueryRef::Other(Statement::CreateIndex(index)) => {
                (&index.query_hints, &index.hint_warnings, None)
            }
            crate::parser::QueryRef::Other(Statement::AlterTable(alter)) => {
                (&alter.query_hints, &alter.hint_warnings, None)
            }
            crate::parser::QueryRef::WithCte(with) => {
                publish_optimizer_hint_diagnostics(&with.body);
                return;
            }
            crate::parser::QueryRef::Other(Statement::Explain { target, .. }) => {
                publish_optimizer_hint_diagnostics(target);
                return;
            }
            crate::parser::QueryRef::Other(Statement::SelectIntoVars { inner, .. })
            | crate::parser::QueryRef::Other(Statement::SelectIntoFile { inner, .. }) => {
                publish_optimizer_hint_diagnostics(inner);
                return;
            }
            _ => return,
        };
    for warning in warnings {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WARN_UNSUPPORTED_MAX_EXECUTION_TIME,
            warning.clone(),
        );
    }
    if let Some(kind) = ignored_kind {
        for hint in hints {
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::ER_WARN_UNSUPPORTED_MAX_EXECUTION_TIME,
                format!("Optimizer hint {hint:?} is accepted but ignored for {kind}"),
            );
        }
    }
}

fn index_build_options_from_hints(
    hints: &[crate::parser::QueryHint],
) -> crate::storage::IndexBuildOptions {
    let explicit = hints.iter().any(|hint| {
        matches!(
            hint,
            crate::parser::QueryHint::Parallel { .. } | crate::parser::QueryHint::NoParallel
        )
    });
    let disabled = crate::util::env::var("MYDB_PARALLEL_DISABLE")
        .ok()
        .is_some_and(|value| {
            value == "1" || value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("on")
        });
    if disabled {
        if explicit
            && hints
                .iter()
                .any(|hint| matches!(hint, crate::parser::QueryHint::Parallel { degree } if *degree != 1))
        {
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::ER_WARN_UNSUPPORTED_MAX_EXECUTION_TIME,
                "PARALLEL ignored: MYDB_PARALLEL_DISABLE is set".to_string(),
            );
        }
        return crate::storage::IndexBuildOptions::new(1, explicit);
    }
    if hints
        .iter()
        .any(|hint| matches!(hint, crate::parser::QueryHint::NoParallel))
    {
        return crate::storage::IndexBuildOptions::serial();
    }
    let environment_degree = || {
        crate::util::env::var("MYDB_PARALLEL_DEGREE")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|degree| *degree >= 1)
            .unwrap_or(2)
    };
    let degree = hints
        .iter()
        .find_map(|hint| match hint {
            crate::parser::QueryHint::Parallel { degree } => Some(*degree as usize),
            _ => None,
        })
        .filter(|degree| *degree >= 1)
        .unwrap_or_else(environment_degree);
    crate::storage::IndexBuildOptions::new(degree, explicit)
}

fn has_explicit_parallel_hint(hints: &[crate::parser::QueryHint]) -> bool {
    hints
        .iter()
        .any(|hint| matches!(hint, crate::parser::QueryHint::Parallel { .. }))
}

fn reject_parallel_hint_for_non_btree(
    hints: &[crate::parser::QueryHint],
    index_type: &str,
) -> Result<(), QueryError> {
    if has_explicit_parallel_hint(hints)
        && !index_type.eq_ignore_ascii_case("BTREE")
        && !index_type.eq_ignore_ascii_case("FULLTEXT")
    {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("PARALLEL index creation is not supported for {index_type} indexes"),
        ));
    }
    Ok(())
}

pub(super) fn reject_system_catalog_dml(
    db: &Database,
    session: &SessionContext,
    table_name: &str,
) -> Result<(), QueryError> {
    if session.has_temp_table(table_name) {
        return Ok(());
    }
    let (database, table) = crate::storage::split_table_name(&session.current_database, table_name);
    // performance_schema DML is dispatched to the PFS handlers in the
    // Insert/Update/Delete arms below: writable setup tables are mutated, and
    // every other PFS table raises ER_WRONG_PERFSCHEMA_USAGE (1683). Let it
    // through here so those handlers run.
    if database.eq_ignore_ascii_case("performance_schema") {
        return Ok(());
    }
    if crate::engine::sys_schema::is_sys_config(&database, &table) {
        return Ok(());
    }
    if db.is_base_table(&database, &table) {
        return Ok(());
    }
    if crate::storage::is_system_database(&database) {
        return Err(error(crate::error::catalog::ER_TABLEACCESS_DENIED_ERROR,
            format!(
                "direct DML against system catalog table '{table_name}' is not supported; use DDL or DCL"
            ),
        ));
    }
    Ok(())
}

fn is_reserved_system_table_name(name: &str) -> bool {
    crate::engine::information_schema::find_system_table_name(name).is_some()
}

fn pfs_command_denied(session: &SessionContext, command: &str, table: &str) -> QueryError {
    error(
        crate::error::catalog::ER_TABLEACCESS_DENIED_ERROR,
        format!(
            "{command} command denied to user '{}'@'{}' for table '{table}'",
            session.user, session.host
        ),
    )
}

/// Resolve `table_name` to a registered performance_schema table name, or
/// `None` if it is not a performance_schema table (caller proceeds normally;
/// an unregistered `performance_schema.<x>` falls through to the standard
/// "table does not exist" error).
fn pfs_dml_target(session: &SessionContext, table_name: &str) -> Option<String> {
    let (database, table) = crate::storage::split_table_name(&session.current_database, table_name);
    if database.eq_ignore_ascii_case("performance_schema")
        && crate::engine::information_schema::find_system_table("performance_schema", &table)
            .is_some()
    {
        Some(table)
    } else {
        None
    }
}

fn try_pfs_dml_update(
    session: &SessionContext,
    stmt: &crate::parser::Update,
) -> Result<Option<QueryResponse>, QueryError> {
    let Some(table) = pfs_dml_target(session, &stmt.table) else {
        return Ok(None);
    };
    match crate::engine::perf_setup::disposition(&table) {
        crate::engine::perf_setup::Disposition::Writable => {
            // performance_schema configuration tables accept scalar user
            // variables in assignments (for example restoring a value saved
            // with `SELECT ... INTO @original_setting`). Resolve those at the
            // session boundary before handing the restricted DML expression
            // subset to perf_setup.
            let assignments = stmt
                .assignments
                .iter()
                .cloned()
                .map(|mut assignment| {
                    if let crate::parser::Expr::UserVariable(name) = &assignment.value {
                        assignment.value = crate::parser::Expr::BoundValue(
                            session.user_variable(name).cloned().unwrap_or(Value::Null),
                        );
                    }
                    assignment
                })
                .collect::<Vec<_>>();
            let affected =
                crate::engine::perf_setup::update(&table, &assignments, stmt.selection.as_ref())?;
            Ok(Some(QueryResponse::OkAffected {
                affected_rows: affected,
            }))
        }
        crate::engine::perf_setup::Disposition::ReadOnly => {
            Err(pfs_command_denied(session, "UPDATE", &table))
        }
    }
}

fn try_pfs_dml_insert(
    session: &SessionContext,
    stmt: &crate::parser::Insert,
) -> Result<Option<QueryResponse>, QueryError> {
    let Some(table) = pfs_dml_target(session, &stmt.table) else {
        return Ok(None);
    };
    match crate::engine::perf_setup::disposition(&table) {
        crate::engine::perf_setup::Disposition::Writable => {
            if !matches!(table.as_str(), "setup_actors" | "setup_objects") {
                return Err(pfs_command_denied(session, "INSERT", &table));
            }
            let rows = match &stmt.source {
                crate::parser::InsertSource::Values(rows) => rows.as_slice(),
                crate::parser::InsertSource::Query(_) => {
                    return Err(error(
                        crate::error::catalog::ER_WRONG_PERFSCHEMA_USAGE,
                        "INSERT ... SELECT into performance_schema is not supported".to_string(),
                    ));
                }
            };
            let affected =
                crate::engine::perf_setup::insert(&table, stmt.columns.as_deref(), rows)?;
            Ok(Some(QueryResponse::OkAffected {
                affected_rows: affected,
            }))
        }
        crate::engine::perf_setup::Disposition::ReadOnly => {
            Err(pfs_command_denied(session, "INSERT", &table))
        }
    }
}

fn try_pfs_dml_delete(
    session: &SessionContext,
    stmt: &crate::parser::Delete,
) -> Result<Option<QueryResponse>, QueryError> {
    let Some(table) = pfs_dml_target(session, &stmt.table) else {
        return Ok(None);
    };
    match crate::engine::perf_setup::disposition(&table) {
        crate::engine::perf_setup::Disposition::Writable => {
            if !matches!(table.as_str(), "setup_actors" | "setup_objects") {
                return Err(pfs_command_denied(session, "DELETE", &table));
            }
            let affected = crate::engine::perf_setup::delete(&table, stmt.selection.as_ref())?;
            Ok(Some(QueryResponse::OkAffected {
                affected_rows: affected,
            }))
        }
        crate::engine::perf_setup::Disposition::ReadOnly => {
            Err(pfs_command_denied(session, "DELETE", &table))
        }
    }
}

fn sys_dml_target(session: &SessionContext, table_name: &str) -> Option<String> {
    let (database, table) = crate::storage::split_table_name(&session.current_database, table_name);
    crate::engine::sys_schema::is_sys_config(&database, &table).then_some(table)
}

fn ensure_sys_config_autocommit(session: &SessionContext) -> Result<(), QueryError> {
    if session.explicit_txn || !session.autocommit || session.xa_branch.is_some() {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "sys.sys_config DML inside a transaction is not supported",
        ));
    }
    Ok(())
}

fn try_sys_dml_insert(
    db: &Database,
    session: &SessionContext,
    stmt: &crate::parser::Insert,
) -> Result<Option<QueryResponse>, QueryError> {
    if sys_dml_target(session, &stmt.table).is_none() {
        return Ok(None);
    }
    ensure_sys_config_autocommit(session)?;
    let affected = crate::engine::sys_schema::insert_sys_config(db, session, stmt)?;
    Ok(Some(QueryResponse::OkAffected {
        affected_rows: affected,
    }))
}

fn try_sys_dml_update(
    db: &Database,
    session: &SessionContext,
    stmt: &crate::parser::Update,
) -> Result<Option<QueryResponse>, QueryError> {
    if sys_dml_target(session, &stmt.table).is_none() {
        return Ok(None);
    }
    ensure_sys_config_autocommit(session)?;
    let affected = crate::engine::sys_schema::update_sys_config(db, session, stmt)?;
    Ok(Some(QueryResponse::OkAffected {
        affected_rows: affected,
    }))
}

fn try_sys_dml_delete(
    db: &Database,
    session: &SessionContext,
    stmt: &crate::parser::Delete,
) -> Result<Option<QueryResponse>, QueryError> {
    if sys_dml_target(session, &stmt.table).is_none() {
        return Ok(None);
    }
    ensure_sys_config_autocommit(session)?;
    let affected = crate::engine::sys_schema::delete_sys_config(db, stmt)?;
    Ok(Some(QueryResponse::OkAffected {
        affected_rows: affected,
    }))
}

pub(super) fn select_deadline_ms(select: &Select, session: &SessionContext) -> u64 {
    select
        .query_hints
        .iter()
        .find_map(|hint| match hint {
            crate::parser::QueryHint::MaxExecutionTime { ms } => Some(*ms),
            _ => None,
        })
        .unwrap_or(session.sysvars.max_execution_time_ms)
}

fn query_deadline_ms<'q>(
    statement: impl Into<crate::parser::QueryRef<'q>>,
    session: &SessionContext,
) -> Option<u64> {
    let statement = statement.into();
    match statement {
        crate::parser::QueryRef::Select(select) => Some(select_deadline_ms(select, session)),
        crate::parser::QueryRef::WithCte(with) => query_deadline_ms(&with.body, session),
        crate::parser::QueryRef::SetOperation(set) => query_deadline_ms(&set.left, session),
        _ => None,
    }
}

/// Dispatch a `WITH ... body` statement: materialise the CTEs into a registry
/// then execute the body with that registry installed in thread-local state.
fn execute_with_cte(
    db: &Arc<Database>,
    session: &mut SessionContext,
    statement: Statement,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let Statement::WithCte(WithCte { mut ctes, body, .. }) = statement else {
        unreachable!("execute_with_cte called with non-WithCte statement")
    };
    super::cte::validate_cte_names(&ctes)?;
    // Validate the original output schemas before inlining or pruning can
    // erase duplicate columns. This only binds metadata; it executes no rows.
    super::select::describe_cte_definitions(db, session, &ctes)?;
    // CTE materialization is part of the SELECT's execution time. Start its
    // deadline before opening any CTE workers, and keep the same absolute
    // deadline when the body later re-enters ordinary SELECT dispatch.
    let _deadline = query_deadline_ms(&body, session).and_then(|ms| {
        crate::engine::cancel::install_query_deadline(std::time::Duration::from_millis(ms))
    });
    // The query hint belongs to the WITH body, not to the internal SELECTs
    // used to materialize its CTE definitions. Letting the first CTE consume
    // it stores that inner plan under the outer SQL fingerprint; a subsequent
    // execution with a different legal CTE rewrite can then pair the wrong
    // physical projection with the current AST. Hold the hint aside until the
    // body is dispatched. Nested SELECTs still consume it exactly once there.
    let body_query_hint = crate::engine::cache::take_query_hint();

    // P1-1: inline single-use, non-recursive CTEs into their reference site as
    // derived tables and drop them from the materialisation set, so the
    // derived-table planner (incl. flatten) can push projection/predicates
    // into the body. Conservative — anything ambiguous stays materialised.
    let mut body_stmt = *body;
    // Apply the same CTE rewrite sequence used by EXPLAIN: shared-reference
    // domain pushdown, single-reference group-key pushdown, inlining, column
    // pruning, and common predicate pushdown.
    super::cte::apply_pre_materialization_rewrites(
        db,
        &session.current_database,
        &mut ctes,
        &mut body_stmt,
        true,
    );
    // Install a per-query memory budget for the whole WITH execution
    // so `materialize_recursive` and the body share one budget. Mirrors
    // the pattern in `execute_select`.
    let body_fn = || -> Result<QueryResponse, QueryError> {
        let cte_map = super::cte::materialize_ctes(db, session, &ctes, &body_stmt, registry)?;
        super::cte::with_cte_registry_extended(cte_map, || match body_query_hint {
            Some(hint) => crate::engine::cache::with_query_hint(hint, || {
                execute_cte_statement(db, session, body_stmt, registry)
            }),
            None => execute_cte_statement(db, session, body_stmt, registry),
        })
    };
    if crate::engine::executor::mem_budget::current_budget().is_some() {
        body_fn()
    } else {
        let budget =
            std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::from_env());
        crate::engine::executor::mem_budget::with_query_budget(budget, body_fn)
    }
}

/// Helper used by the CTE materialiser to execute a sub-statement (anchor /
/// recursive part / non-recursive body). Re-enters the main dispatch so a
/// nested `WITH` works correctly. Earlier CTEs are visible through the active
/// thread-local registry.
pub(super) fn execute_for_cte(
    db: &Arc<Database>,
    session: &mut SessionContext,
    statement: &mut Statement,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    execute_cte_statement(db, session, statement.clone(), registry)
}

/// Only re-entry of an already authorized, expanded CTE may inherit these
/// privileges. Independent statements (including routine bodies) start fresh.
fn execute_cte_statement(
    db: &Arc<Database>,
    session: &mut SessionContext,
    statement: Statement,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    select::with_session_registry(registry.clone(), || {
        execute_statement_inner(db, session, statement, registry, true)
    })
}

const MAX_VIEW_DEPTH: usize = 32;

/// Trail of `(database, view_name)` lowercased pairs traversed on the
/// active expansion path, used for cycle detection (e.g. v1 → v2 → v1).
type ViewTrail = std::collections::HashSet<(String, String)>;

/// Reverse map a catalog `DataTypeKind` to the parser `DataType` used to
/// declare a CTAS output column. This is what lets a `VARCHAR(10)` /
/// `ENUM(...)` / `MEDIUMTEXT` / `DECIMAL(10,2)` source column reproduce
/// faithfully in the new table instead of collapsing to the wire type's
/// lossy default (e.g. every string → TEXT).
fn ctas_data_type_from_kind(
    kind: &crate::storage::schema::DataTypeKind,
) -> crate::parser::DataType {
    use crate::parser::DataType;
    use crate::storage::schema::DataTypeKind as K;
    let md = |p: &Option<u8>, s: &Option<u8>| match (p, s) {
        (Some(p), Some(s)) => Some((*p, *s)),
        _ => None,
    };
    match kind {
        K::TinyInt => DataType::TinyInt,
        K::SmallInt => DataType::SmallInt,
        K::MediumInt => DataType::MediumInt,
        K::Int => DataType::Int,
        K::BigInt => DataType::BigInt,
        K::Bool => DataType::Bool,
        K::Float { precision, scale } => DataType::Float(md(precision, scale)),
        K::Double { precision, scale } => DataType::Double(md(precision, scale)),
        K::Decimal { precision, scale } => DataType::Decimal(md(precision, scale)),
        K::Char { len } => DataType::Char(*len),
        K::VarChar { len } => DataType::VarChar(*len),
        K::Binary { len } => DataType::Binary(*len),
        K::VarBinary { len } => DataType::VarBinary(*len),
        K::Text { max_len } => DataType::Text(*max_len),
        K::Blob { max_len } => DataType::Blob(*max_len),
        K::Date => DataType::Date,
        K::Time { fsp } => DataType::Time { fsp: *fsp },
        K::DateTime { fsp } => DataType::DateTime { fsp: *fsp },
        K::Timestamp { fsp } => DataType::Timestamp { fsp: *fsp },
        K::Year => DataType::Year,
        K::Json => DataType::Json,
        K::Geometry { subtype, srid } => DataType::Geometry(*subtype, *srid),
        K::Vector { dim, datatype } => DataType::Vector(*dim, *datatype),
        K::Enum { values } => DataType::Enum(values.clone()),
        K::Set { values } => DataType::Set(values.clone()),
        K::Bit { len } => DataType::Bit(*len),
    }
}

/// Build the declared column for a CTAS output column.
///
/// For a column with a single-table catalog origin (`org_table` /
/// `org_name` populated) we re-read the real source `ColumnSchema` and
/// reproduce its type, charset/collation, UNSIGNED/ZEROFILL and NOT NULL —
/// matching MySQL, which carries those attributes into the new table but
/// drops keys, AUTO_INCREMENT and the generated-column expression (a
/// generated source materializes as a plain column holding the computed
/// value). Expression / derived columns keep the lossy but safe
/// type-derived inference.
///
/// Nullability comes from the bound result descriptor and join shape, never
/// from the presence or absence of NULL values in this execution.
fn ctas_column_def(
    db: &Arc<Database>,
    session: &SessionContext,
    col: &crate::types::Column,
    source_nullable: bool,
) -> crate::parser::ColumnDef {
    let src = &col.source;
    if !src.org_name.is_empty() && !src.org_table.is_empty() {
        let src_name = if src.schema.is_empty() {
            src.org_table.clone()
        } else {
            format!("{}.{}", src.schema, src.org_table)
        };
        let qualified = qualify_table_name(session, &src_name);
        if let Ok(table) = db.table(&qualified) {
            if let Some((_, column)) = table
                .columns
                .iter()
                .enumerate()
                .find(|(_, c)| c.name.eq_ignore_ascii_case(&src.org_name))
            {
                return crate::parser::ColumnDef {
                    name: col.name.clone(),
                    data_type: ctas_data_type_from_kind(&column.data_type),
                    unsigned: column.unsigned,
                    zerofill: column.zerofill,
                    invisible: false,
                    // The query can null-extend a catalog NOT NULL column.
                    nullable: column.nullable || source_nullable,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    default: column
                        .auto_increment
                        .then(|| crate::parser::Expr::Literal(crate::parser::Literal::Int(0))),
                    on_update: None,
                    checks: Vec::new(),
                    generated: None,
                    charset: column.charset.clone(),
                    collation: column.collation.clone(),
                };
            }
        }
    }
    type_derived_ctas_column(col, source_nullable)
}

/// Type-derived fallback used for expression / derived columns and when the
/// catalog origin can't be resolved (view, dropped table, …). UNSIGNED is
/// semantic result metadata and is therefore valid even with no catalog
/// origin (`CAST(... AS UNSIGNED)`, bitwise expressions, unsigned integer
/// arithmetic). ZEROFILL and declared temporal fsp remain catalog attributes.
/// The bound descriptor supplies expression nullability and width. Unknown
/// string widths use the full LOB domain rather than inspecting source rows.
fn type_derived_ctas_column(
    col: &crate::types::Column,
    source_nullable: bool,
) -> crate::parser::ColumnDef {
    use crate::mysql::protocol::{BINARY_FLAG, NOT_NULL_FLAG, UNSIGNED_FLAG, ZEROFILL_FLAG};
    let src = &col.source;
    let length = if matches!(col.column_type, ColumnType::VarString | ColumnType::Blob) {
        Some(src.length.unwrap_or(crate::storage::schema::LONG_LOB_MAX))
    } else {
        src.length
    };
    let has_source = !src.org_name.is_empty();
    let unsigned = src.flags & UNSIGNED_FLAG != 0;
    let zerofill = has_source && src.flags & ZEROFILL_FLAG != 0;
    // Fractional precision belongs to the bound expression descriptor, not
    // to catalog provenance. Computed NOW(6), TIME(...), DATE_ADD(...), etc.
    // must materialize with the same FSP as a direct/prepared result.
    let fsp = src.decimals.unwrap_or(0).min(6);
    // Binary expression results can use either wire string tag. Preserve
    // their binary domain instead of turning a VAR_STRING result into a
    // character column with the destination table's default charset.
    let binary_string = matches!(col.column_type, ColumnType::VarString | ColumnType::Blob)
        && src.collation_id.map_or(src.flags & BINARY_FLAG != 0, |id| {
            id == crate::charset::BINARY_COLLATION_ID
        });
    let data_type = if binary_string {
        if let Some(bytes) = length.filter(|length| *length <= u16::MAX as u32) {
            crate::parser::DataType::VarBinary(Some(bytes as usize))
        } else {
            column_type_to_data_type(ColumnType::Blob, fsp, length, src.collation_id)
        }
    } else if col.column_type == crate::types::ColumnType::Blob
        && src.flags & BINARY_FLAG == 0
        && src
            .collation_id
            .and_then(crate::charset::resolve_collation_id)
            .is_some_and(|collation| collation.charset != "binary")
    {
        // The protocol uses the BLOB family for both TEXT and BLOB once a
        // string result exceeds 512 bytes. A non-binary collation identifies
        // the character result (`GROUP_CONCAT(text_col)` is the common case);
        // materialize it as TEXT so its charset/collation remain declarable.
        text_data_type(length)
    } else if matches!(
        col.column_type,
        crate::types::ColumnType::Float | crate::types::ColumnType::Double
    ) && src.numeric_precision.is_some()
        && src.decimals.is_some()
    {
        let spec = Some((src.numeric_precision.unwrap(), src.decimals.unwrap()));
        match col.column_type {
            crate::types::ColumnType::Float => crate::parser::DataType::Float(spec),
            crate::types::ColumnType::Double => crate::parser::DataType::Double(spec),
            _ => unreachable!(),
        }
    } else if col.column_type == crate::types::ColumnType::LongLong
        && !unsigned
        && src.length.is_some_and(|length| length <= 9)
    {
        // MySQL exposes integer expressions through MYSQL_TYPE_LONGLONG but
        // materializes their bounded domain as INT when the descriptor needs
        // at most nine display digits (booleans, comparisons and small DIV
        // expressions). Keep the wire tag and storage family separate.
        crate::parser::DataType::Int
    } else if col.column_type == ColumnType::Vector {
        let (datatype, dim) = src.vector_type.unwrap_or_default();
        crate::parser::DataType::Vector(dim, datatype)
    } else {
        column_type_to_data_type(col.column_type, fsp, length, src.collation_id)
    };
    let nullable = source_nullable || src.flags & NOT_NULL_FLAG == 0;
    let default = if !nullable {
        match data_type {
            crate::parser::DataType::TinyInt
            | crate::parser::DataType::SmallInt
            | crate::parser::DataType::MediumInt
            | crate::parser::DataType::Int
            | crate::parser::DataType::BigInt
            | crate::parser::DataType::Bool
            | crate::parser::DataType::Float(_)
            | crate::parser::DataType::Double(_)
            | crate::parser::DataType::Decimal(_)
            | crate::parser::DataType::Year
            | crate::parser::DataType::Bit(_) => Some(crate::parser::Expr::Literal(if unsigned {
                crate::parser::Literal::UInt(0)
            } else {
                crate::parser::Literal::Int(0)
            })),
            // A non-null DATETIME expression materialized by CTAS receives
            // MySQL's implicit zero DATETIME default. This is metadata on the
            // generated target definition, not an evaluation of NOW() as a
            // default expression.
            crate::parser::DataType::DateTime { .. } => Some(crate::parser::Expr::Literal(
                crate::parser::Literal::String("0000-00-00 00:00:00".to_string()),
            )),
            _ => None,
        }
    } else {
        None
    };
    // Protocol metadata carries a collation id for every MySQL field,
    // including numeric and temporal fields (normally `binary`).  It is not
    // evidence that the derived SQL type accepts an explicit CHARACTER SET
    // or COLLATE clause.  Only declarable character kinds may copy it into
    // the CTAS ColumnDef; otherwise DDL correctly rejects the internally
    // generated definition as ER_COLLATION_CHARSET_MISMATCH.
    let text_collation = matches!(
        data_type,
        crate::parser::DataType::Char(_)
            | crate::parser::DataType::VarChar(_)
            | crate::parser::DataType::Text(_)
            | crate::parser::DataType::Enum(_)
            | crate::parser::DataType::Set(_)
            | crate::parser::DataType::Json
    )
    .then(|| {
        src.collation_id
            .and_then(crate::charset::resolve_collation_id)
    })
    .flatten();
    crate::parser::ColumnDef {
        name: col.name.clone(),
        data_type,
        unsigned,
        zerofill,
        invisible: false,
        nullable,
        primary_key: false,
        unique: false,
        auto_increment: false,
        default,
        on_update: None,
        checks: Vec::new(),
        generated: None,
        charset: text_collation.map(|info| info.charset.to_string()),
        collation: text_collation.map(|info| info.name.to_string()),
    }
}

fn warn_metadata_only_create_options(table: &crate::parser::CreateTable) {
    for (key, value) in &table.table_options {
        if key.eq_ignore_ascii_case("ZONE_MAP") || key.eq_ignore_ascii_case("ZONE_SIZE") {
            continue;
        }
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
            format!("table option {key}={value} is retained as metadata only"),
        );
    }
}

/// Validate constant string operands in numeric comparisons before CTAS
/// chooses an index/zone-map/vectorized scan path. Those paths are free to
/// precompute a typed constant, but they must not silently turn an invalid
/// string into zero and bypass the strict SELECT-to-table conversion policy.
///
/// This is a bind-time, schema-driven rule: no SQL text matching and no
/// per-row work. Runtime coercion remains the oracle for non-constant values.
fn validate_ctas_numeric_comparison_constants<'q>(
    db: &Arc<Database>,
    session: &SessionContext,
    query: impl Into<crate::parser::QueryRef<'q>>,
) -> Result<(), QueryError> {
    let query = query.into();
    use crate::storage::schema::DataTypeKind;

    struct Source {
        qualifiers: Vec<String>,
        table: Arc<crate::storage::schema::TableSchema>,
    }

    fn is_numeric_type(data_type: &DataTypeKind) -> bool {
        matches!(
            data_type,
            DataTypeKind::TinyInt
                | DataTypeKind::SmallInt
                | DataTypeKind::MediumInt
                | DataTypeKind::Int
                | DataTypeKind::BigInt
                | DataTypeKind::Bool
                | DataTypeKind::Float { .. }
                | DataTypeKind::Double { .. }
                | DataTypeKind::Decimal { .. }
                | DataTypeKind::Year
                | DataTypeKind::Bit { .. }
        )
    }

    fn numeric_kind(data_type: &DataTypeKind) -> &'static str {
        match data_type {
            DataTypeKind::Decimal { .. } => "DECIMAL",
            DataTypeKind::Float { .. } | DataTypeKind::Double { .. } => "DOUBLE",
            _ => "INTEGER",
        }
    }

    fn resolve_numeric_column<'a>(
        expr: &Expr,
        sources: &'a [Source],
    ) -> Option<(&'a crate::storage::schema::ColumnSchema, &'static str)> {
        let (qualifier, column) = match expr {
            Expr::Identifier(column) => (None, column.as_str()),
            Expr::CompoundIdentifier(parts) => {
                let column = parts.last()?.as_str();
                let qualifier = (parts.len() >= 2).then(|| parts[parts.len() - 2].as_str());
                (qualifier, column)
            }
            _ => return None,
        };
        let mut found = None;
        for source in sources {
            if qualifier.is_some_and(|qualifier| {
                !source
                    .qualifiers
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(qualifier))
            }) {
                continue;
            }
            let Some(column) = source
                .table
                .columns
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(column))
            else {
                continue;
            };
            if !is_numeric_type(&column.data_type) || found.is_some() {
                return None;
            }
            found = Some((column, numeric_kind(&column.data_type)));
        }
        found
    }

    fn constant_string(expr: &Expr) -> Option<String> {
        let value = crate::engine::expr::eval_const(expr).ok()?;
        match value {
            Value::String(text) | Value::Decimal(text) => Some(text),
            Value::SmallString(text) => {
                Some(unsafe { std::str::from_utf8_unchecked(&text) }.to_string())
            }
            Value::Bytes(bytes) => Some(String::from_utf8_lossy(&bytes).into_owned()),
            _ => None,
        }
    }

    fn validate_comparison(
        numeric_expr: &Expr,
        string_expr: &Expr,
        sources: &[Source],
    ) -> Result<(), QueryError> {
        let Some((_column, kind)) = resolve_numeric_column(numeric_expr, sources) else {
            return Ok(());
        };
        let Some(text) = constant_string(string_expr) else {
            return Ok(());
        };
        if crate::util::decimal::parse_lenient_f64(&text).clean {
            return Ok(());
        }
        // MySQL preserves the expression provenance here: a direct literal
        // comparison is a failed numeric comparison (1292), while a folded
        // expression such as CONCAT/COALESCE is diagnosed as an incorrect
        // value for the CTAS destination (1366).
        let spec = if matches!(string_expr, Expr::Literal(Literal::String(_))) {
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE
        } else {
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD
        };
        Err(error(
            spec,
            format!("Truncated incorrect {kind} value: '{text}'"),
        ))
    }

    fn walk(expr: &Expr, sources: &[Source]) -> Result<(), QueryError> {
        match expr {
            Expr::Binary { left, op, right } => {
                if matches!(
                    op,
                    BinaryOp::Eq
                        | BinaryOp::NullSafeEq
                        | BinaryOp::NotEq
                        | BinaryOp::Lt
                        | BinaryOp::LtEq
                        | BinaryOp::Gt
                        | BinaryOp::GtEq
                ) {
                    validate_comparison(left, right, sources)?;
                    validate_comparison(right, left, sources)?;
                }
                walk(left, sources)?;
                walk(right, sources)
            }
            Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
                walk(expr, sources)
            }
            Expr::InList { expr, list, .. } => {
                for item in list {
                    validate_comparison(expr, item, sources)?;
                    walk(item, sources)?;
                }
                walk(expr, sources)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                validate_comparison(expr, low, sources)?;
                validate_comparison(expr, high, sources)?;
                walk(expr, sources)?;
                walk(low, sources)?;
                walk(high, sources)
            }
            Expr::Like { expr, pattern, .. } => {
                walk(expr, sources)?;
                walk(pattern, sources)
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(operand) = operand {
                    walk(operand, sources)?;
                }
                for expr in conditions.iter().chain(results) {
                    walk(expr, sources)?;
                }
                if let Some(expr) = else_result {
                    walk(expr, sources)?;
                }
                Ok(())
            }
            Expr::Function { args, .. } | Expr::Tuple(args) => {
                for arg in args {
                    walk(arg, sources)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    let crate::parser::QueryRef::Select(select) = query else {
        return Ok(());
    };
    let mut refs: Vec<&crate::parser::TableRef> = select.from.iter().collect();
    refs.extend(select.joins.iter().map(|join| &join.table));
    let mut sources = Vec::with_capacity(refs.len() + usize::from(select.table.is_some()));
    for table_ref in refs {
        if table_ref.generate_series.is_some() {
            continue;
        }
        let qualified = qualify_table_name(session, &table_ref.name);
        let Ok(table) = db.table(&qualified) else {
            continue;
        };
        let mut qualifiers = vec![table_ref.name.clone()];
        if let Some(base) = table_ref.name.rsplit('.').next() {
            qualifiers.push(base.to_string());
        }
        if let Some(alias) = &table_ref.alias {
            qualifiers.push(alias.clone());
        }
        sources.push(Source { qualifiers, table });
    }
    if sources.is_empty() {
        if let Some(name) = &select.table {
            let qualified = qualify_table_name(session, name);
            if let Ok(table) = db.table(&qualified) {
                sources.push(Source {
                    qualifiers: vec![name.clone()],
                    table,
                });
            }
        }
    }
    if sources.is_empty() {
        return Ok(());
    }
    if let Some(selection) = &select.selection {
        walk(selection, &sources)?;
    }
    for join in &select.joins {
        if let Some(condition) = &join.condition {
            walk(condition, &sources)?;
        }
    }
    if let Some(having) = &select.having {
        walk(having, &sources)?;
    }
    for item in &select.projection {
        if let crate::parser::SelectItem::Expr { expr, .. } = item {
            walk(expr, &sources)?;
        }
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    pub(super) static CTAS_SOURCE_READY: std::cell::RefCell<Option<Box<dyn FnOnce(&Database)>>> =
        std::cell::RefCell::new(None);
    pub(super) static CTAS_BATCH_WRITTEN: std::cell::RefCell<Option<Box<dyn FnOnce(&Database)>>> =
        std::cell::RefCell::new(None);
}

#[cfg(test)]
fn ctas_source_ready(db: &Database) {
    let observer = CTAS_SOURCE_READY.with(|slot| slot.borrow_mut().take());
    if let Some(observer) = observer {
        observer(db);
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_create_table_as_select(
    db: &Arc<Database>,
    session: &mut SessionContext,
    target: crate::parser::CreateTable,
    explicit_definition: bool,
    conflict_policy: crate::parser::CtasConflictPolicy,
    query: crate::parser::QueryExpr,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    if target.temporary {
        let user_transaction = session.explicit_txn || !session.autocommit;
        let result = execute_temp_ddl_in_transaction(db, session, |db, session| {
            // Nested source statements must neither commit the temporary load
            // transaction nor release the owning statement's MDL tickets.
            let txn = ensure_statement_txn(db, session)?;
            let savepoint = db.statement_savepoint(txn.txn_id)?;
            let explicit_txn = session.explicit_txn;
            session.explicit_txn = true;
            let result = execute_create_temp_table_as_select(
                db,
                session,
                target,
                explicit_definition,
                conflict_policy,
                query,
                registry,
            );
            session.explicit_txn = explicit_txn;
            match result {
                Ok(response) => Ok(response),
                Err(err) => {
                    // Undo loaded rows before the temporary DDL wrapper drops
                    // their files. Otherwise a later user ROLLBACK sees undo
                    // records referring to an already removed table.
                    db.rollback_to_statement_savepoint(txn.txn_id, savepoint)?;
                    Err(err)
                }
            }
        });
        if !user_transaction {
            // The nested source used transaction-duration MDL while the
            // temporary DDL wrapper finalized its autocommit transaction.
            db.release_mdl_duration(session.connection_id as u64, MdlDuration::Transaction);
            session.advance_mdl_txn_generation();
        }
        return result;
    }
    implicit_commit_for_ddl(db, session)?;
    let qualified = qualify_persistent_table_name(session, &target.name);
    if db.relation_name_exists(&qualified) || is_reserved_system_table_name(&qualified) {
        if target.if_not_exists {
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Note,
                crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                format!("Table '{}' already exists", target.name),
            );
            return Ok(QueryResponse::Ok);
        }
        return Err(error(
            crate::error::catalog::ER_TABLE_EXISTS_ERROR,
            format!("Table '{}' already exists", target.name),
        ));
    }
    // This is a statement-owned transaction even when autocommit is disabled.
    // Keep nested SELECT/CTE/function statements from finalizing it. Their MDL
    // is transaction-duration, while the outer CTAS retains statement-duration
    // target X/source SR until catalog publication or failure cleanup finishes.
    session.explicit_txn = true;
    let result = (|| {
        ensure_statement_txn(db, session)?;
        let source = prepare_ctas_source(db, session, query, registry)?;
        let definition = source.definition(target, explicit_definition);
        let catalog_guard = crate::storage::CatalogTxnBatchGuard::new();
        let target = definition.target;
        db.create_table_with_partition(
            &qualified,
            target.columns,
            target.primary_key,
            target.indexes,
            target.checks,
            target.foreign_keys,
            target.auto_increment_start,
            target.partition,
            target.default_charset,
            target.default_collation,
            target.column_comments,
            target.table_comment,
            target.pctfree,
            target.pctused,
            target.table_options,
            target.engine,
        )?;
        let load = (|| {
            let scatter = definition
                .select_slot
                .map(|slots| {
                    db.table(&qualified).and_then(|table| {
                        CtasDeclaredScatter::new(
                            &table,
                            &slots,
                            conflict_policy == crate::parser::CtasConflictPolicy::Ignore,
                        )
                    })
                })
                .transpose()?;
            emit_copied_numeric_deprecations(definition.copied_numeric_deprecations);
            ctas_persistent_load(db, session, &qualified, source, scatter, conflict_policy)
        })();
        let (intent, affected_rows) = match load {
            Ok(intent) => intent,
            Err(err) => {
                let _ = tx::rollback_session_txn(db, session);
                let _ = db.drop_table(&qualified);
                return Err(err);
            }
        };
        let needs_flush = catalog_guard.any_flush_requested();
        drop(catalog_guard);
        if needs_flush {
            // Retain the committed-load intent on failure so recovery can
            // finish publishing the schema after restart.
            db.commit_catalog_txn_now()?;
        }
        db.remove_ctas_intent(intent.as_ref());
        Ok(QueryResponse::OkAffected { affected_rows })
    })();
    if result.is_err() {
        let _ = tx::rollback_session_txn(db, session);
    }
    result
}

fn ctas_outer_nullable_sources<'q>(
    statement: impl Into<crate::parser::QueryRef<'q>>,
) -> std::collections::HashSet<String> {
    let statement = statement.into();
    let crate::parser::QueryRef::Select(select) = statement else {
        return std::collections::HashSet::new();
    };
    let mut tables: Vec<&crate::parser::TableRef> = select.from.iter().collect();
    tables.extend(select.joins.iter().map(|join| &join.table));
    let mut nullable = std::collections::HashSet::new();
    let mut mark = |idx: usize| {
        if let Some(table) = tables.get(idx) {
            // The query alias identifies an occurrence in a self join.
            let name = table.alias.as_deref().unwrap_or(&table.name);
            nullable.insert(name.to_ascii_lowercase());
            if table.alias.is_none() {
                if let Some(base) = name.rsplit('.').next() {
                    nullable.insert(base.to_ascii_lowercase());
                }
            }
        }
    };
    for (join_idx, join) in select.joins.iter().enumerate() {
        let right = select.from.len() + join_idx;
        match join.join_type {
            JoinType::Left => mark(right),
            JoinType::Right => (0..right).for_each(&mut mark),
            JoinType::Full => (0..=right).for_each(&mut mark),
            _ => {}
        }
    }
    nullable
}

fn ctas_column_is_outer_nullable(
    column: &crate::types::Column,
    nullable_sources: &std::collections::HashSet<String>,
) -> bool {
    let source = column.source.table.to_ascii_lowercase();
    !source.is_empty()
        && (nullable_sources.contains(&source)
            || source
                .rsplit('.')
                .next()
                .is_some_and(|base| nullable_sources.contains(base)))
}

fn ctas_group_concat_metadata<'q>(
    db: &Arc<Database>,
    session: &SessionContext,
    query: impl Into<crate::parser::QueryRef<'q>>,
) -> (Vec<bool>, Vec<Option<(String, String)>>) {
    let query = query.into();
    let Some(select) = query.leftmost_select() else {
        return (Vec::new(), Vec::new());
    };
    if !select
        .projection
        .iter()
        .all(|item| matches!(item, SelectItem::Expr { .. }))
    {
        return (Vec::new(), Vec::new());
    }
    let outputs = select
        .projection
        .iter()
        .map(|item| {
            matches!(
                item,
                SelectItem::Expr {
                    expr: Expr::Function { name, .. },
                    ..
                } if name.eq_ignore_ascii_case("GROUP_CONCAT")
            )
        })
        .collect::<Vec<_>>();
    if !select.joins.is_empty() || select.from.len() != 1 {
        return (outputs, Vec::new());
    }
    let Some(table_name) = select.table.as_deref() else {
        return (outputs, Vec::new());
    };
    let qualified = qualify_table_name(session, table_name);
    let Ok(table) = db.table(&qualified) else {
        return (outputs, Vec::new());
    };
    let collations = select
        .projection
        .iter()
        .map(|item| {
            let SelectItem::Expr {
                expr: Expr::Function { name, .. },
                ..
            } = item
            else {
                return None;
            };
            if !name.eq_ignore_ascii_case("GROUP_CONCAT") {
                return None;
            }
            let SelectItem::Expr { expr, .. } = item else {
                unreachable!();
            };
            crate::engine::expr::expr_type_descriptor(expr, &table)
                .collation
                .and_then(|collation| crate::charset::resolve_collation_id(collation.collation_id))
                .map(|info| (info.charset.to_string(), info.name.to_string()))
        })
        .collect();
    (outputs, collations)
}

/// Bind-time nullability for CTAS projection expressions.  The SELECT may be
/// constant-folded before its result metadata reaches this layer, so deriving
/// nullability from the folded runtime value would incorrectly turn operators
/// such as `DIV` into NOT NULL.  Preserve the original expression domain for
/// the common zero/one-table shapes; joins keep their existing origin-aware
/// handling because a single `TableSchema` cannot describe all input slots.
fn ctas_static_projection_nullability<'q>(
    db: &Arc<Database>,
    session: &SessionContext,
    query: impl Into<crate::parser::QueryRef<'q>>,
) -> Vec<bool> {
    let query = query.into();
    let crate::parser::QueryRef::Select(select) = query else {
        return Vec::new();
    };
    if !select
        .projection
        .iter()
        .all(|item| matches!(item, SelectItem::Expr { .. }))
        || !select.joins.is_empty()
        || select.from.len() > 1
    {
        return Vec::new();
    }

    let table = select.table.as_deref().and_then(|name| {
        let qualified = qualify_table_name(session, name);
        db.table(&qualified).ok()
    });
    // A derived/view input has no directly resolvable TableSchema here. Its
    // result metadata already carries bound nullability; treating every
    // identifier as Unknown would only widen known NOT NULL columns.
    if !select.from.is_empty() && table.is_none() {
        return Vec::new();
    }
    select
        .projection
        .iter()
        .map(|item| {
            let SelectItem::Expr { expr, .. } = item else {
                unreachable!();
            };
            if let Some(table) = table.as_deref() {
                crate::engine::expr_type::infer_expr_type_in_table(expr, table).nullable
            } else {
                let no_column = |_: &Expr| None;
                let no_placeholder = |_: usize| None;
                let context = crate::engine::expr_type::TypeContext::context_free(
                    &no_column,
                    &no_placeholder,
                );
                crate::engine::expr_type::infer_expr_type(expr, &context).nullable
            }
        })
        .collect()
}

/// Owns the source exactly once. CTAS derives its schema from result metadata
/// and never drains or rewinds the cursor to inspect runtime values.
struct CtasSource {
    response: QueryResponse,
    inferred_columns: Vec<crate::parser::ColumnDef>,
    enum_source_columns: Vec<bool>,
}

struct CtasDefinition {
    target: crate::parser::CreateTable,
    select_slot: Option<Vec<usize>>,
    copied_numeric_deprecations: Vec<super::ddl_deprecation::CopiedNumericDeprecation>,
}

impl CtasSource {
    fn definition(
        &self,
        mut target: crate::parser::CreateTable,
        explicit_definition: bool,
    ) -> CtasDefinition {
        warn_metadata_only_create_options(&target);
        let mut copied_numeric_deprecations = Vec::new();
        let mut slots = Vec::with_capacity(self.inferred_columns.len());
        if !explicit_definition {
            target.columns.clear();
        }
        for column in &self.inferred_columns {
            if let Some(pos) = target
                .columns
                .iter()
                .position(|existing| existing.name.eq_ignore_ascii_case(&column.name))
                .filter(|_| explicit_definition)
            {
                slots.push(pos);
            } else {
                slots.push(target.columns.len());
                copied_numeric_deprecations
                    .extend(copied_numeric_deprecations_for_column_def(column));
                target.columns.push(column.clone());
            }
        }
        CtasDefinition {
            target,
            select_slot: explicit_definition.then_some(slots),
            copied_numeric_deprecations,
        }
    }
}

fn prepare_ctas_source(
    db: &Arc<Database>,
    session: &mut SessionContext,
    query: crate::parser::QueryExpr,
    registry: &SessionRegistry,
) -> Result<CtasSource, QueryError> {
    let outer_nullable = ctas_outer_nullable_sources(&query);
    let static_nullable = ctas_static_projection_nullability(db, session, &query);
    let (group_concat_outputs, group_concat_collations) =
        ctas_group_concat_metadata(db, session, &query);
    let vector_source = if let crate::parser::QueryExpr::Select(select) = &query {
        select
            .from
            .iter()
            .chain(select.joins.iter().map(|join| &join.table))
            .filter_map(|source| db.table(&qualify_table_name(session, &source.name)).ok())
            .any(|table| table.indexes.iter().any(|index| index.is_vector()))
    } else {
        false
    };
    let response = crate::engine::expr::with_strict_numeric_materialization(true, || {
        validate_ctas_numeric_comparison_constants(db, session, &query)?;
        // Parent sinks consume rows themselves; suppress optional response
        // materialization while retaining blocking operators inside the plan.
        let _streaming = super::select::force_streaming_response_scope();
        let response = execute_statement(db, session, query.into_statement(), registry)?;
        crate::engine::expr::reject_strict_numeric_materialization_warnings()?;
        Ok(response)
    })?;
    if vector_source {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
            "CREATE TABLE AS SELECT copies VECTOR columns and data but not vector indexes; run CREATE INDEX ... USING HNSW/IVF on the target",
        );
    }
    #[cfg(test)]
    ctas_source_ready(db);
    let columns = match &response {
        QueryResponse::ResultSet(result) => result.columns.as_slice(),
        QueryResponse::SharedResultSet(result) => result.columns.as_ref(),
        QueryResponse::StreamingResultSet(stream) => stream.columns.as_slice(),
        _ => {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "CREATE TABLE AS SELECT requires SELECT result",
            ))
        }
    };
    let enum_source_columns = ctas_enum_source_columns(columns);
    let inferred_columns = columns
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            let mut column = ctas_column_def(
                db,
                session,
                col,
                ctas_column_is_outer_nullable(col, &outer_nullable)
                    || static_nullable.get(idx).copied().unwrap_or(false)
                    || col.source.flags & crate::mysql::protocol::NOT_NULL_FLAG == 0,
            );
            if group_concat_outputs.get(idx).copied().unwrap_or(false) {
                let max_len =
                    super::select::aggregate::group_concat_max_len().min(u32::MAX as usize) as u32;
                column.data_type = if max_len > 512 {
                    text_data_type(Some(max_len))
                } else {
                    column_type_to_data_type(
                        crate::types::ColumnType::VarString,
                        0,
                        Some(max_len),
                        None,
                    )
                };
            }
            if let Some((charset, collation)) =
                group_concat_collations.get(idx).and_then(Option::as_ref)
            {
                column.charset = Some(charset.clone());
                column.collation = Some(collation.clone());
            }
            column
        })
        .collect();
    Ok(CtasSource {
        response,
        inferred_columns,
        enum_source_columns,
    })
}

/// A stored ENUM has two observably different empty values: ordinal zero
/// (invalid / empty sentinel) and a legitimately declared empty member. The
/// row codec represents them as `String("")` and `SmallString("")`
/// respectively. CTAS must preserve that distinction when the source result
/// column itself is ENUM; otherwise normal target-column coercion would map
/// both values to the declared empty member.
fn ctas_enum_source_columns(columns: &[crate::types::Column]) -> Vec<bool> {
    columns
        .iter()
        .map(|column| column.source.flags & crate::mysql::protocol::ENUM_FLAG != 0)
        .collect()
}

fn preserve_ctas_enum_zero_sentinels(rows: &mut [Vec<Value>], enum_source_columns: &[bool]) {
    for row in rows {
        for (index, value) in row.iter_mut().enumerate() {
            if enum_source_columns.get(index).copied().unwrap_or(false)
                && matches!(value, Value::String(text) if text.is_empty())
            {
                *value = Value::Int(0);
            }
        }
    }
}

/// Row count per storage batch when loading a CTAS payload. Bounds the
/// transient batch the storage layer holds at once, matching the
/// INSERT…SELECT chunked loader (`INSERT_SELECT_CHUNK_ROWS`) instead of
/// handing the whole result set to a single `insert_rows` call.
#[cfg(not(test))]
const CTAS_LOAD_CHUNK_ROWS: usize = 4096;
// See INSERT_SELECT_CHUNK_ROWS: CTAS chunk-boundary tests exercise identical
// loader semantics with a compact fixture; release builds retain 4096 rows.
#[cfg(test)]
const CTAS_LOAD_CHUNK_ROWS: usize = 64;

/// Precomputed scatter for the declared-columns CTAS path: source SELECT rows
/// are narrower than the merged `declared + appended` layout, so each row is
/// scattered into a per-row copy of the declared-default template with the
/// SELECT values placed at their mapped slots. Building this once and applying
/// it inside the chunked load loop bounds mapped rows to the current batch;
/// no second result collection or per-row default evaluation is needed.
struct CtasDeclaredScatter {
    template: Vec<Value>,
    /// `select_slot[i]` = merged-column index fed by SELECT output column `i`.
    select_slot: Vec<usize>,
}

impl CtasDeclaredScatter {
    fn new(table: &TableSchema, select_slot: &[usize], ignore: bool) -> Result<Self, QueryError> {
        let mut template: Vec<Value> = (0..table.columns.len())
            .map(|idx| crate::engine::expr::resolve_column_default_value(&table.columns[idx]))
            .collect();
        let missing = super::insert::missing_required_columns(table, select_slot);
        super::insert::enforce_missing_required_columns(table, &mut template, &missing, ignore)?;
        Ok(Self {
            template,
            select_slot: select_slot.to_vec(),
        })
    }

    fn scatter(&self, src: Vec<Value>) -> Vec<Value> {
        let mut row = self.template.clone();
        for (i, value) in src.into_iter().enumerate() {
            row[self.select_slot[i]] = value;
        }
        row
    }
}

/// Common persistent/temporary sink: source rows are consumed once in bounded
/// batches, with the same mapping, enum-copy, conflict and diagnostic policy.
fn ctas_load_rows(
    db: &Arc<Database>,
    txn: TxnView,
    qualified: &str,
    source: CtasSource,
    scatter: Option<CtasDeclaredScatter>,
    conflict_policy: crate::parser::CtasConflictPolicy,
) -> Result<u64, QueryError> {
    let mut records = 0u64;
    let mut affected = 0u64;
    crate::engine::expr::with_strict_numeric_materialization(true, || {
        super::select::drive_response_chunks(
            source.response,
            CTAS_LOAD_CHUNK_ROWS,
            |mut chunk| {
                records += chunk.len() as u64;
                preserve_ctas_enum_zero_sentinels(&mut chunk, &source.enum_source_columns);
                if let Some(scatter) = &scatter {
                    chunk = chunk.into_iter().map(|row| scatter.scatter(row)).collect();
                }
                affected +=
                    crate::storage::with_enum_field_copy_coercion(|| match conflict_policy {
                        crate::parser::CtasConflictPolicy::Error => db
                            .insert_rows(txn, qualified, chunk)
                            .map(|ids| ids.len() as u64),
                        policy => super::insert::execute_insert_with_conflict(
                            db,
                            txn,
                            qualified,
                            None,
                            chunk,
                            policy == crate::parser::CtasConflictPolicy::Ignore,
                            policy == crate::parser::CtasConflictPolicy::Replace,
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                        .map(|response| match response {
                            QueryResponse::OkAffected { affected_rows } => affected_rows,
                            _ => unreachable!("CTAS conflict sink returns affected rows"),
                        }),
                    })?;
                #[cfg(test)]
                {
                    let observer = CTAS_BATCH_WRITTEN.with(|slot| slot.borrow_mut().take());
                    if let Some(observer) = observer {
                        observer(db);
                    }
                }
                crate::engine::expr::reject_strict_numeric_materialization_warnings()
            },
        )?;
        crate::engine::expr::reject_strict_numeric_materialization_warnings()
    })?;
    super::insert::finalize_insert_info(records, true);
    Ok(affected)
}

fn ctas_persistent_load(
    db: &Arc<Database>,
    session: &mut SessionContext,
    qualified: &str,
    source: CtasSource,
    scatter: Option<CtasDeclaredScatter>,
    conflict_policy: crate::parser::CtasConflictPolicy,
) -> Result<(Option<std::path::PathBuf>, u64), QueryError> {
    let txn = ensure_statement_txn(db, session)?;
    let intent = db.write_ctas_intent(qualified, txn.txn_id)?;
    let result = (|| {
        let (database, ddl, ctx) = ctas_binlog_create(db, session, qualified)?;
        if let Some((database, ddl, ctx)) = database.zip(ddl).zip(ctx).map(|((a, b), c)| (a, b, c))
        {
            crate::binlog::record_ddl_transactional_with(txn.txn_id, &database, &ddl, &ctx)?;
        }
        let affected = ctas_load_rows(db, txn, qualified, source, scatter, conflict_policy)?;
        tx::commit_session_txn(db, session)?;
        Ok(affected)
    })();
    let affected = match result {
        Ok(affected) => affected,
        Err(err) => {
            let _ = tx::rollback_session_txn(db, session);
            // The caller drops the uncommitted target while catalog flushes are
            // still deferred. Leave no intent for an already rolled-back load.
            db.remove_ctas_intent(intent.as_ref());
            return Err(err);
        }
    };
    let _ = db.mark_ctas_intent_committed(intent.as_ref());
    Ok((intent, affected))
}

/// CTAS logs the resolved column list (not the original `AS SELECT` text,
/// which would be non-deterministic statement-based replication) followed by
/// the WRITE_ROWS stream — matching MySQL 8.0's atomic CTAS binlog shape.
fn ctas_binlog_create(
    db: &Arc<Database>,
    session: &SessionContext,
    qualified: &str,
) -> Result<
    (
        Option<String>,
        Option<String>,
        Option<crate::binlog::BinlogQueryContext>,
    ),
    QueryError,
> {
    if !crate::binlog::enabled() {
        return Ok((None, None, None));
    }
    let table = db.table(qualified)?;
    let ddl = format!(
        "{} START TRANSACTION",
        crate::engine::show::create_table_sql(&table)?
    );
    let (database, _) = crate::storage::split_table_name(&session.current_database, qualified);
    let ctx = super::binlog_ctx_from_session(session);
    Ok((Some(database), Some(ddl), Some(ctx)))
}

fn visible_table_key_and_name(session: &SessionContext, name: &str) -> (String, String) {
    let (database, table) = crate::storage::split_table_name(&session.current_database, name);
    (
        crate::storage::table_key(&database, &table),
        format!("{database}.{table}"),
    )
}

fn execute_temp_ddl_in_transaction<F>(
    db: &Arc<Database>,
    session: &mut SessionContext,
    execute: F,
) -> Result<QueryResponse, QueryError>
where
    F: FnOnce(&Arc<Database>, &mut SessionContext) -> Result<QueryResponse, QueryError>,
{
    let user_transaction = session.explicit_txn || !session.autocommit;
    let txn = ensure_temp_statement_txn(db, session)?;
    let (_txn, _snapshot_guard) = tx::atomically_guard_statement_view(db, session, txn);
    let temp_savepoint = session.temp_table_journal.len();
    let result = execute(db, session);
    let result = match result {
        Ok(response) => {
            // CREATE/DROP TEMPORARY do not implicitly commit, but the object
            // operation itself is not transactional in MySQL. Keep journal
            // entries only until the statement succeeds so a statement-level
            // error can still clean up; after success, finalize physical
            // drops and disarm rollback of creates/renames.
            let entries = session.temp_table_journal.split_off(temp_savepoint);
            for entry in entries {
                match entry {
                    crate::engine::api::TempTableJournalEntry::Create { .. } => {
                        if user_transaction {
                            session.created_temp_table_not_rollbackable = true;
                        }
                    }
                    crate::engine::api::TempTableJournalEntry::Drop { internal_key, .. } => {
                        if let Err(err) = db.drop_temp_table(&internal_key) {
                            crate::engine::api::push_statement_warning(
                                crate::error::ConditionLevel::Warning,
                                crate::error::catalog::ER_GET_ERRNO,
                                format!(
                                    "Temporary table dropped; physical cleanup failed: {}",
                                    err.message()
                                ),
                            );
                        }
                        if user_transaction {
                            session.dropped_temp_table_not_rollbackable = true;
                        }
                    }
                }
            }
            Ok(response)
        }
        Err(err) => {
            let _ = rollback_temp_journal_to(db, session, temp_savepoint);
            Err(err)
        }
    };
    finish_statement_txn(db, session, result)
}

#[allow(clippy::too_many_arguments)]
fn execute_create_temp_table_as_select(
    db: &Arc<Database>,
    session: &mut SessionContext,
    target: crate::parser::CreateTable,
    explicit_definition: bool,
    conflict_policy: crate::parser::CtasConflictPolicy,
    query: crate::parser::QueryExpr,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    let name = target.name.clone();
    let on_commit = target.on_commit.unwrap_or(TempTableOnCommit::PreserveRows);
    let (visible_key, visible_name) = visible_table_key_and_name(session, &name);
    if session.temp_tables.contains_key(&visible_key) {
        if target.if_not_exists {
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Note,
                crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                format!("Table '{name}' already exists"),
            );
            return Ok(QueryResponse::Ok);
        }
        return Err(error(
            crate::error::catalog::ER_TABLE_EXISTS_ERROR,
            format!("Table '{name}' already exists"),
        ));
    }
    let source = prepare_ctas_source(db, session, query, registry)?;
    let definition = source.definition(target, explicit_definition);
    let target = definition.target;
    let internal_key = db.create_temp_table_with_partition(
        &visible_name,
        session.connection_id,
        &session.temp_quota,
        target.columns,
        target.primary_key,
        target.indexes,
        target.checks,
        target.foreign_keys,
        target.auto_increment_start,
        target.partition,
        target.default_charset,
        target.default_collation,
        target.column_comments,
        target.table_comment,
        target.pctfree,
        target.pctused,
        target.table_options,
        target.engine,
    )?;
    session.add_temp_table(visible_key.clone(), internal_key.clone(), on_commit);
    session
        .temp_table_journal
        .push(crate::engine::api::TempTableJournalEntry::Create {
            visible_key,
            internal_key: internal_key.clone(),
        });
    let scatter = definition
        .select_slot
        .map(|slots| {
            db.table(&internal_key).and_then(|table| {
                CtasDeclaredScatter::new(
                    &table,
                    &slots,
                    conflict_policy == crate::parser::CtasConflictPolicy::Ignore,
                )
            })
        })
        .transpose()?;
    let txn = ensure_statement_txn(db, session)?;
    emit_copied_numeric_deprecations(definition.copied_numeric_deprecations);
    let affected_rows = ctas_load_rows(db, txn, &internal_key, source, scatter, conflict_policy)?;
    Ok(QueryResponse::OkAffected { affected_rows })
}

fn execute_create_table_like(
    db: &Arc<Database>,
    session: &mut SessionContext,
    name: &str,
    if_not_exists: bool,
    source: &str,
    temporary: bool,
    on_commit: Option<TempTableOnCommit>,
) -> Result<QueryResponse, QueryError> {
    if temporary {
        return execute_temp_ddl_in_transaction(db, session, |db, session| {
            execute_create_temp_table_like(
                db,
                session,
                name,
                if_not_exists,
                source,
                on_commit.unwrap_or(TempTableOnCommit::PreserveRows),
            )
        });
    }
    implicit_commit_for_ddl(db, session)?;
    let qualified = qualify_persistent_table_name(session, name);
    if db.relation_name_exists(&qualified) || is_reserved_system_table_name(&qualified) {
        if if_not_exists {
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Note,
                crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                format!("Table '{name}' already exists"),
            );
            return Ok(QueryResponse::Ok);
        }
        return Err(error(
            crate::error::catalog::ER_TABLE_EXISTS_ERROR,
            format!("Table '{name}' already exists"),
        ));
    }

    let source_qualified = qualify_table_name(session, source);
    let source_table = match db.table(&source_qualified) {
        Ok(table) => table,
        Err(err) => {
            if db
                .view_definition(&session.current_database, source)
                .is_some()
            {
                return Err(error(
                    crate::error::catalog::ER_WRONG_OBJECT,
                    format!("'{source_qualified}' is not BASE TABLE"),
                ));
            }
            return Err(err);
        }
    };
    db.create_table_like(&qualified, source_table.as_ref())?;
    emit_copied_numeric_deprecations(
        source_table
            .columns
            .iter()
            .flat_map(copied_numeric_deprecations_for_schema),
    );
    Ok(QueryResponse::Ok)
}

fn execute_create_temp_table_like(
    db: &Arc<Database>,
    session: &mut SessionContext,
    name: &str,
    if_not_exists: bool,
    source: &str,
    on_commit: TempTableOnCommit,
) -> Result<QueryResponse, QueryError> {
    let (visible_key, visible_name) = visible_table_key_and_name(session, name);
    if session.temp_tables.contains_key(&visible_key) {
        if if_not_exists {
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Note,
                crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                format!("Table '{name}' already exists"),
            );
            return Ok(QueryResponse::Ok);
        }
        return Err(error(
            crate::error::catalog::ER_TABLE_EXISTS_ERROR,
            format!("Table '{name}' already exists"),
        ));
    }
    let source_qualified = qualify_table_name(session, source);
    let source_table = db.table(&source_qualified).map_err(|err| {
        if db
            .view_definition(&session.current_database, source)
            .is_some()
            && !session.has_temp_table(source)
        {
            error(
                crate::error::catalog::ER_WRONG_OBJECT,
                format!("'{source_qualified}' is not BASE TABLE"),
            )
        } else {
            err
        }
    })?;
    let internal_key = db.create_temp_table_like(
        &visible_name,
        session.connection_id,
        &session.temp_quota,
        &source_table,
    )?;
    session.add_temp_table(visible_key.clone(), internal_key.clone(), on_commit);
    session
        .temp_table_journal
        .push(crate::engine::api::TempTableJournalEntry::Create {
            visible_key,
            internal_key,
        });
    emit_copied_numeric_deprecations(
        source_table
            .columns
            .iter()
            .flat_map(copied_numeric_deprecations_for_schema),
    );
    Ok(QueryResponse::Ok)
}

fn create_index_schema_from_parts(
    db: &Database,
    table_name: &str,
    name: Option<String>,
    columns: Vec<String>,
    column_prefix_lengths: Vec<Option<usize>>,
    column_descending: Vec<bool>,
    parser_key_expressions: Vec<Option<String>>,
    parser_key_expression_multi_valued: Vec<bool>,
    unique: bool,
    primary: bool,
    included_columns: Vec<String>,
    index_type: String,
    visible: bool,
    comment: String,
    fulltext_parser: String,
    vector_options: Option<crate::storage::vector_config::VectorIndexOptions>,
    defer_expressions: bool,
) -> Result<IndexSchema, QueryError> {
    let column_directions: Vec<crate::storage::IndexColumnDirection> = column_descending
        .iter()
        .map(|desc| {
            if *desc {
                crate::storage::IndexColumnDirection::Desc
            } else {
                crate::storage::IndexColumnDirection::Asc
            }
        })
        .collect();
    let key_expressions = if defer_expressions {
        parser_key_expressions
            .into_iter()
            .enumerate()
            .map(|(position, expression)| {
                expression.map(|expr_sql| crate::storage::IndexExpression {
                    expr_sql,
                    data_type: crate::storage::DataTypeKind::VarChar { len: None },
                    nullable: true,
                    referenced_columns: Vec::new(),
                    multi_valued: parser_key_expression_multi_valued
                        .get(position)
                        .copied()
                        .unwrap_or(false),
                    collation_id: None,
                    ast: None,
                    program: None,
                })
            })
            .collect()
    } else {
        build_index_key_expressions(
            db,
            table_name,
            &columns,
            &parser_key_expressions,
            &parser_key_expression_multi_valued,
        )?
    };
    if key_expressions
        .iter()
        .flatten()
        .any(|expr| expr.multi_valued)
    {
        if unique || primary {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "UNIQUE or PRIMARY multi-valued indexes are not supported",
            ));
        }
        if !index_type.eq_ignore_ascii_case("BTREE") {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "multi-valued indexes require BTREE index type",
            ));
        }
        if !included_columns.is_empty() {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "multi-valued indexes with INCLUDE columns are not supported",
            ));
        }
        let multi_pos = key_expressions
            .iter()
            .position(|expr| expr.as_ref().is_some_and(|expr| expr.multi_valued))
            .expect("multi-valued expression checked above");
        if column_prefix_lengths
            .get(multi_pos)
            .copied()
            .flatten()
            .is_some()
        {
            return Err(error(
                crate::error::catalog::ER_WRONG_SUB_KEY,
                "prefix length is not allowed for multi-valued index key part",
            ));
        }
        if column_directions.get(multi_pos).is_some_and(|direction| {
            !matches!(direction, crate::storage::IndexColumnDirection::Asc)
        }) {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "ASC or DESC is not allowed for a multi-valued index key part",
            ));
        }
    }
    let fulltext = index_type.eq_ignore_ascii_case("FULLTEXT").then(|| {
        // Snapshot the current tokenizer settings so the build/query sides
        // never diverge across restarts that change MYDB_FT_* env.
        let mut meta = crate::storage::FulltextMeta {
            parser: fulltext_parser,
            ..Default::default()
        };
        crate::storage::fulltext::snapshot_tokenizer_config_into(&mut meta);
        meta
    });
    let vector = crate::storage::vector_config::VectorIndexConfig::from_options(
        &index_type,
        vector_options.as_ref(),
        &comment,
    )?;
    Ok(IndexSchema {
        readable_from_scn: 0,
        index_id: 0,
        name: name.unwrap_or_else(|| columns.join("_")),
        columns,
        column_prefix_lengths,
        column_directions,
        key_expressions,
        unique,
        primary,
        included_columns,
        state: crate::storage::IndexState::Public,
        index_type,
        visible,
        comment,
        vector,
        fulltext,
    })
}

fn execute_create_temp_index(
    db: &Arc<Database>,
    session: &mut SessionContext,
    stmt: CreateIndex,
) -> Result<QueryResponse, QueryError> {
    let internal_key = session
        .resolve_temp_table_name(&stmt.table)
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("table '{}' does not exist", stmt.table),
            )
        })?;
    let CreateIndex {
        name,
        table,
        columns,
        column_prefix_lengths,
        column_descending,
        key_expressions: parser_key_expressions,
        key_expression_multi_valued: parser_key_expression_multi_valued,
        unique,
        included_columns,
        index_type,
        visible,
        comment,
        fulltext_parser,
        vector_options,
        ..
    } = stmt;
    let index = create_index_schema_from_parts(
        db,
        &internal_key,
        Some(name),
        columns,
        column_prefix_lengths,
        column_descending,
        parser_key_expressions,
        parser_key_expression_multi_valued,
        unique,
        false,
        included_columns,
        index_type,
        visible,
        comment,
        fulltext_parser,
        vector_options,
        false,
    )
    .map_err(|err| {
        if err.is(crate::error::catalog::ER_NO_SUCH_TABLE) {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{table}' doesn't exist"),
            )
        } else {
            err
        }
    })?;
    let snapshot = db.snapshot_temp_table(&internal_key)?;
    match db.add_temp_index(&internal_key, index) {
        Ok(()) => {
            db.discard_temp_table_snapshot(snapshot)?;
            Ok(QueryResponse::Ok)
        }
        Err(err) => {
            let _ = db.restore_temp_table_snapshot(snapshot);
            Err(err)
        }
    }
}

fn execute_temp_alter_table(
    db: &Arc<Database>,
    session: &mut SessionContext,
    stmt: AlterTable,
) -> Result<QueryResponse, QueryError> {
    Database::validate_alter_identifiers(&stmt.actions)?;
    if !stmt.partition_actions.is_empty() {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "ALTER TABLE partition operations on TEMPORARY tables are not supported",
        ));
    }
    validate_alter_lock(stmt.lock)?;
    let old_visible_key = session.temp_visible_key(&stmt.table);
    let temp = session
        .temp_tables
        .get(&old_visible_key)
        .cloned()
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("table '{}' does not exist", stmt.table),
            )
        })?;
    if matches!(stmt.actions.as_slice(), [AlterAction::RenameTable { .. }]) {
        let AlterAction::RenameTable { new_name } = stmt.actions.into_iter().next().unwrap() else {
            unreachable!("matched above");
        };
        let new_name = qualify_alter_rename_target(session, &stmt.table, &new_name);
        let new_visible_key = session.temp_visible_key(&new_name);
        if session.temp_tables.contains_key(&new_visible_key) {
            return Err(error(
                crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                format!("Table '{new_name}' already exists"),
            ));
        }
        let (_, visible_name) = visible_table_key_and_name(session, &new_name);
        db.rename_temp_table(&temp.internal_key, &visible_name)?;
        session.temp_tables.remove(&old_visible_key);
        session.add_temp_table(
            new_visible_key.clone(),
            temp.internal_key.clone(),
            temp.on_commit,
        );
        return Ok(QueryResponse::Ok);
    }

    let snapshot = db.snapshot_temp_table(&temp.internal_key)?;
    let result = (|| -> Result<(), QueryError> {
        for action in stmt.actions {
            validate_alter_algorithm(stmt.algorithm, &action)?;
            match action {
                AlterAction::AddColumn(column) => {
                    db.add_temp_column(&temp.internal_key, column)?;
                }
                AlterAction::DropColumn(column) => {
                    db.drop_temp_column(&temp.internal_key, &column)?;
                }
                AlterAction::MoveColumn { column, position } => {
                    db.move_temp_column_position(&temp.internal_key, &column, position)?;
                }
                AlterAction::SetColumnComment { column, comment } => {
                    db.set_temp_column_comment(&temp.internal_key, &column, &comment)?;
                }
                AlterAction::SetColumnDefault { column, default } => {
                    db.set_temp_column_default(&temp.internal_key, &column, default)?;
                }
                AlterAction::AddIndex(index) => {
                    let primary = index
                        .name
                        .as_deref()
                        .is_some_and(|name| name.eq_ignore_ascii_case("PRIMARY"));
                    let name = index.name.or_else(|| Some(index.columns.join("_")));
                    let index_schema = create_index_schema_from_parts(
                        db,
                        &temp.internal_key,
                        name,
                        index.columns,
                        index.column_prefix_lengths,
                        index.column_descending,
                        index.key_expressions,
                        index.key_expression_multi_valued,
                        index.unique,
                        primary,
                        index.included_columns,
                        index.index_type,
                        index.visible,
                        index.comment,
                        index.fulltext_parser,
                        index.vector_options,
                        false,
                    )?;
                    db.add_temp_index(&temp.internal_key, index_schema)?;
                }
                AlterAction::DropIndex(index_name) => {
                    db.drop_temp_index(&temp.internal_key, &index_name)?;
                }
                AlterAction::ModifyColumn(mut column) => {
                    let checks = std::mem::take(&mut column.checks);
                    for warning in db.modify_temp_column(
                        &temp.internal_key,
                        column,
                        session.is_strict_sql_mode(),
                    )? {
                        crate::engine::api::push_sql_warning(warning);
                    }
                    for check in checks {
                        db.add_temp_check_constraint(&temp.internal_key, check)?;
                    }
                }
                AlterAction::ChangeColumn {
                    old_name,
                    mut new_def,
                } => {
                    let checks = std::mem::take(&mut new_def.checks);
                    for warning in db.change_temp_column(
                        &temp.internal_key,
                        &old_name,
                        new_def,
                        session.is_strict_sql_mode(),
                    )? {
                        crate::engine::api::push_sql_warning(warning);
                    }
                    for check in checks {
                        db.add_temp_check_constraint(&temp.internal_key, check)?;
                    }
                }
                AlterAction::RenameColumn { old_name, new_name } => {
                    db.rename_temp_column(&temp.internal_key, &old_name, &new_name)?;
                }
                AlterAction::RenameIndex { old_name, new_name } => {
                    db.rename_temp_index(&temp.internal_key, &old_name, &new_name)?;
                }
                AlterAction::SetColumnVisibility { column, invisible } => {
                    db.mutate_temp_table_schema(&temp.internal_key, |table| {
                        let position = table.column_index(&column)?;
                        table.columns[position].invisible = invisible;
                        Ok(())
                    })?;
                }
                AlterAction::AlterIndexVisibility { name, visible } => {
                    db.mutate_temp_table_schema(&temp.internal_key, |table| {
                        let index = table
                            .indexes
                            .iter_mut()
                            .find(|index| index.name.eq_ignore_ascii_case(&name))
                            .ok_or_else(|| {
                                error(
                                    crate::error::catalog::ER_CANT_DROP_FIELD_OR_KEY,
                                    format!("index '{name}' does not exist"),
                                )
                            })?;
                        if index.primary && !visible {
                            return Err(error(
                                crate::error::catalog::ER_PK_INDEX_CANT_BE_INVISIBLE,
                                "A primary key index cannot be invisible",
                            ));
                        }
                        index.visible = visible;
                        Ok(())
                    })?;
                }
                AlterAction::AddCheck(check) => {
                    db.add_temp_check_constraint(&temp.internal_key, check)?;
                }
                AlterAction::DropCheck {
                    name, if_exists, ..
                } => {
                    db.drop_temp_check_constraint(&temp.internal_key, &name, if_exists)?;
                }
                AlterAction::AlterCheck { name, enforced } => {
                    db.alter_temp_check_constraint(&temp.internal_key, &name, enforced)?;
                }
                AlterAction::Noop { warning } => {
                    if let Some((level, code, message)) = warning {
                        let level = if level.eq_ignore_ascii_case("Note") {
                            crate::error::ConditionLevel::Note
                        } else if level.eq_ignore_ascii_case("Error") {
                            crate::error::ConditionLevel::Error
                        } else {
                            crate::error::ConditionLevel::Warning
                        };
                        crate::engine::api::push_statement_warning(
                            level,
                            *crate::error::catalog::lookup_by_code(code)
                                .expect("ALTER NOOP warning must use the catalog"),
                            message,
                        );
                    }
                }
                other => {
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        format!(
                            "ALTER TABLE action '{}' on TEMPORARY tables is not supported",
                            alter_action_label(&other)
                        ),
                    ));
                }
            }
        }
        Ok(())
    })();
    match result {
        Ok(()) => {
            db.discard_temp_table_snapshot(snapshot)?;
            Ok(QueryResponse::Ok)
        }
        Err(err) => {
            let _ = db.restore_temp_table_snapshot(snapshot);
            Err(err)
        }
    }
}

fn alter_action_label(action: &AlterAction) -> &'static str {
    match action {
        AlterAction::Rebuild => "REBUILD",
        AlterAction::Noop { .. } => "NOOP",
        AlterAction::SetColumnVisibility { .. } => "ALTER COLUMN VISIBILITY",
        AlterAction::ConvertToCharset { .. } => "CONVERT TO CHARACTER SET",
        AlterAction::AddColumn(_) => "ADD COLUMN",
        AlterAction::DropColumn(_) => "DROP COLUMN",
        AlterAction::MoveColumn { .. } => "MODIFY COLUMN ORDER",
        AlterAction::SetColumnComment { .. } => "ALTER COLUMN COMMENT",
        AlterAction::SetColumnDefault { .. } => "ALTER COLUMN DEFAULT",
        AlterAction::SetTableOptions { .. } => "TABLE OPTIONS",
        AlterAction::SetStorageProperties { .. } => "STORAGE PROPERTIES",
        AlterAction::AddIndex(_) => "ADD INDEX",
        AlterAction::DropIndex(_) => "DROP INDEX",
        AlterAction::DropPrimaryKey => "DROP PRIMARY KEY",
        AlterAction::RenameIndex { .. } => "RENAME INDEX",
        AlterAction::RenameTable { .. } => "RENAME TABLE",
        AlterAction::AlterIndexVisibility { .. } => "ALTER INDEX VISIBILITY",
        AlterAction::ModifyColumn(_) => "MODIFY COLUMN",
        AlterAction::ChangeColumn { .. } => "CHANGE COLUMN",
        AlterAction::RenameColumn { .. } => "RENAME COLUMN",
        AlterAction::AddCheck(_) => "ADD CHECK",
        AlterAction::AddForeignKey(_) => "ADD FOREIGN KEY",
        AlterAction::DropCheck { .. } => "DROP CHECK",
        AlterAction::DropForeignKey { .. } => "DROP FOREIGN KEY",
        AlterAction::AlterCheck { .. } => "ALTER CHECK",
        AlterAction::SetAutoIncrement(_) => "AUTO_INCREMENT",
    }
}

fn column_type_to_data_type(
    ct: crate::types::ColumnType,
    fsp: u8,
    observed_length: Option<u32>,
    collation_id: Option<u16>,
) -> crate::parser::DataType {
    use crate::parser::DataType;
    use crate::storage::schema::{DEFAULT_LOB_MAX, LONG_LOB_MAX, MEDIUM_LOB_MAX};
    use crate::types::ColumnType;
    let lob_max = || {
        let length = observed_length.unwrap_or(DEFAULT_LOB_MAX);
        if length <= DEFAULT_LOB_MAX {
            DEFAULT_LOB_MAX
        } else if length <= MEDIUM_LOB_MAX {
            MEDIUM_LOB_MAX
        } else {
            LONG_LOB_MAX
        }
    };
    match ct {
        ColumnType::Tiny => DataType::TinyInt,
        ColumnType::Short => DataType::SmallInt,
        ColumnType::MediumInt => DataType::MediumInt,
        ColumnType::Long => DataType::Int,
        ColumnType::LongLong => DataType::BigInt,
        ColumnType::Year => DataType::Year,
        ColumnType::Float => DataType::Float(None),
        ColumnType::Double => DataType::Double(None),
        ColumnType::Decimal { precision, scale } => DataType::Decimal(Some((precision, scale))),
        ColumnType::Date => DataType::Date,
        ColumnType::Time => DataType::Time { fsp },
        ColumnType::DateTime => DataType::DateTime { fsp },
        ColumnType::Timestamp => DataType::Timestamp { fsp },
        ColumnType::Blob => DataType::Blob(lob_max()),
        ColumnType::Json => DataType::Json,
        ColumnType::Geometry => {
            DataType::Geometry(crate::storage::schema::GeometrySubtype::Geometry, None)
        }
        ColumnType::Vector => DataType::Vector(None, crate::vector::VectorDataType::Float32),
        ColumnType::Bit { bits } => DataType::Bit(Some(bits as usize)),
        ColumnType::VarString => {
            if let Some(bytes) = observed_length {
                let max_bytes_per_character = collation_id
                    .and_then(crate::charset::resolve_collation_id)
                    .and_then(|collation| crate::charset::resolve_charset(collation.charset).ok())
                    .map(|charset| u32::from(charset.max_len))
                    .unwrap_or(1);
                let characters = bytes.saturating_add(max_bytes_per_character.saturating_sub(1))
                    / max_bytes_per_character;
                if bytes <= DEFAULT_LOB_MAX && characters <= u16::MAX as u32 {
                    DataType::VarChar(Some(characters as usize))
                } else {
                    DataType::Text(lob_max())
                }
            } else {
                DataType::Text(lob_max())
            }
        }
    }
}

fn text_data_type(observed_length: Option<u32>) -> crate::parser::DataType {
    use crate::storage::schema::{DEFAULT_LOB_MAX, LONG_LOB_MAX, MEDIUM_LOB_MAX};
    let length = observed_length.unwrap_or(DEFAULT_LOB_MAX);
    crate::parser::DataType::Text(if length <= DEFAULT_LOB_MAX {
        DEFAULT_LOB_MAX
    } else if length <= MEDIUM_LOB_MAX {
        MEDIUM_LOB_MAX
    } else {
        LONG_LOB_MAX
    })
}

// CTE handling now happens via `execute_with_cte` (which materialises results
// into a thread-local registry) rather than AST substitution.

pub(super) fn execute_statement_with_qualification(
    db: &Arc<Database>,
    session: &mut SessionContext,
    statement: Statement,
    already_qualified: bool,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    if already_qualified
        && !crate::engine::http::statement_has_http(&statement)
        && !matches!(statement, Statement::Call(_))
        // PREPARE qualifies names but leaves stored views unexpanded. Such
        // executions still need the full security and expansion lifecycle.
        && !collect_statement_table_uses(&statement).iter().any(|usage| {
            db.view_definition(&session.current_database, &usage.table).is_some()
        })
        && !super::routines::statement_references_stored_function(
            db,
            &session.current_database,
            &statement,
        )
    {
        execute_mdl_protected_statement(
            db,
            session,
            statement,
            registry,
            &BTreeSet::new(),
            &std::collections::BTreeMap::new(),
        )
    } else {
        execute_statement(db, session, statement, registry)
    }
}

/// Public re-exportable wrapper used by the event scheduler so it can
/// drive the full dispatch pipeline without exposing the inner module
/// as `pub`.
pub(super) fn dispatch_execute_statement(
    db: &Arc<Database>,
    session: &mut SessionContext,
    statement: Statement,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    execute_statement(db, session, statement, registry)
}

fn account_key(account: &AccountIdent) -> AccountKey {
    AccountKey::new(account.user.clone(), account.host.clone())
}

fn account_ident_matches_session(account: &AccountIdent, session: &SessionContext) -> bool {
    let key = show_account_key(account, session);
    key == session.authenticated_as
}

pub(super) fn show_account_key(account: &AccountIdent, session: &SessionContext) -> AccountKey {
    if account.user == "CURRENT_USER()" && account.host.is_empty() {
        session.authenticated_as.clone()
    } else {
        account_key(account)
    }
}

fn ssl_requirement(option: Option<UserSslRequirement>) -> SslRequirement {
    match option {
        None | Some(UserSslRequirement::None) => SslRequirement::None,
        Some(UserSslRequirement::Ssl) => SslRequirement::Ssl,
        Some(UserSslRequirement::X509) => SslRequirement::X509,
        Some(UserSslRequirement::Specified {
            cipher,
            issuer,
            subject,
        }) => SslRequirement::Specified {
            cipher,
            issuer,
            subject,
        },
    }
}

fn grant_scope_to_privilege_scope(session: &SessionContext, scope: &GrantScope) -> PrivilegeScope {
    match scope {
        GrantScope::Global => PrivilegeScope::Global,
        GrantScope::Database(database) => PrivilegeScope::Database {
            database: if database.is_empty() {
                session.current_database.clone()
            } else {
                database.clone()
            },
        },
        GrantScope::Table { database, table } => PrivilegeScope::Table {
            database: database
                .clone()
                .unwrap_or_else(|| session.current_database.clone()),
            table: table.clone(),
        },
        GrantScope::Routine {
            database,
            name,
            kind,
        } => PrivilegeScope::Routine {
            database: database
                .clone()
                .unwrap_or_else(|| session.current_database.clone()),
            routine: name.clone(),
            kind: kind.lower().to_string(),
        },
    }
}

fn foreign_key_requirement(
    session: &SessionContext,
    child: &str,
    fk: &crate::parser::ForeignKeyDef,
) -> RequiredPrivilege {
    let (child_database, _) = crate::storage::split_table_name(&session.current_database, child);
    let (database, table) = crate::storage::split_table_name(&child_database, &fk.referenced_table);
    RequiredPrivilege {
        privilege: Privilege::References,
        scope: PrivilegeScope::Table { database, table },
    }
}

fn auth_query_required_privileges<'q>(
    session: &SessionContext,
    query: impl Into<crate::parser::QueryRef<'q>>,
) -> Vec<RequiredPrivilege> {
    collect_statement_table_uses(query)
        .into_iter()
        .filter(|usage| {
            !usage.table.is_empty() && !reads_freely_readable_system_schema(session, &usage.table)
        })
        .map(|usage| {
            let (database, table) =
                crate::storage::split_table_name(&session.current_database, &usage.table);
            RequiredPrivilege {
                privilege: Privilege::Select,
                scope: PrivilegeScope::Table { database, table },
            }
        })
        .collect()
}

fn auth_required_privileges(
    session: &SessionContext,
    statement: &Statement,
) -> Vec<RequiredPrivilege> {
    let global = |privilege| RequiredPrivilege {
        privilege,
        scope: PrivilegeScope::Global,
    };
    let table = |privilege, name: &str| {
        let (database, table) = crate::storage::split_table_name(&session.current_database, name);
        RequiredPrivilege {
            privilege,
            scope: PrivilegeScope::Table { database, table },
        }
    };
    let database = |privilege, name: &str| RequiredPrivilege {
        privilege,
        scope: PrivilegeScope::Database {
            database: name.to_string(),
        },
    };
    let routine = |privilege, kind: RoutineKind, name: &str| {
        let (database, routine) = crate::storage::split_table_name(&session.current_database, name);
        RequiredPrivilege {
            privilege,
            scope: PrivilegeScope::Routine {
                database,
                routine,
                kind: kind.lower().to_string(),
            },
        }
    };
    match statement {
        Statement::Ai(_) => Vec::new(),
        Statement::AlterSystemReloadHttpConfig | Statement::ShowHttpStatus => {
            vec![global(Privilege::Dynamic("HTTP_ADMIN".into()))]
        }
        Statement::CreateDatabase { name, .. } => vec![database(Privilege::Create, name)],
        Statement::AlterDatabase { name, .. } => vec![database(
            Privilege::Alter,
            if name.is_empty() {
                &session.current_database
            } else {
                name
            },
        )],
        Statement::DropDatabase { name, .. } => vec![database(Privilege::Drop, name)],
        Statement::CreateTable(stmt) if stmt.temporary => {
            vec![table(Privilege::CreateTmpTable, &stmt.name)]
        }
        Statement::CreateTable(stmt) => {
            let mut out = vec![table(Privilege::Create, &stmt.name)];
            out.extend(
                stmt.foreign_keys
                    .iter()
                    .map(|fk| foreign_key_requirement(session, &stmt.name, fk)),
            );
            out
        }
        Statement::CreateTableLike {
            name,
            source,
            temporary,
            ..
        } => vec![
            table(
                if *temporary {
                    Privilege::CreateTmpTable
                } else {
                    Privilege::Create
                },
                name,
            ),
            table(Privilege::Select, source),
        ],
        Statement::CreateTableAsSelect {
            table: target,
            query,
            ..
        } => {
            // Persistent CTAS needs CREATE + INSERT; a new session temporary
            // target needs only CREATE TEMPORARY TABLES. Both need source SELECT.
            let mut out = vec![table(
                if target.temporary {
                    Privilege::CreateTmpTable
                } else {
                    Privilege::Create
                },
                &target.name,
            )];
            if !target.temporary {
                out.push(table(Privilege::Insert, &target.name));
            }
            out.extend(
                target
                    .foreign_keys
                    .iter()
                    .map(|fk| foreign_key_requirement(session, &target.name, fk)),
            );
            let mut uses = Vec::new();
            collect_statement_table_uses_scoped(
                query,
                &std::collections::HashSet::new(),
                &mut uses,
            );
            out.extend(
                uses.into_iter()
                    .filter(|table_use| !table_use.table.is_empty() && !table_use.write)
                    .filter(|table_use| {
                        !reads_freely_readable_system_schema(session, &table_use.table)
                    })
                    .map(|table_use| table(Privilege::Select, &table_use.table)),
            );
            out
        }
        Statement::LockTables(specs) => specs
            .iter()
            .flat_map(|spec| {
                [
                    table(Privilege::LockTables, &spec.table),
                    table(Privilege::Select, &spec.table),
                ]
            })
            .collect(),
        Statement::UnlockTables => Vec::new(),
        Statement::RenameTables { pairs } => pairs
            .iter()
            // MySQL: the source needs ALTER + DROP, the target needs
            // CREATE + INSERT. `table()` resolves each name against the
            // current database, so cross-db pairs gate on the right schema.
            .flat_map(|(old, new)| {
                [
                    table(Privilege::Alter, old),
                    table(Privilege::Drop, old),
                    table(Privilege::Create, new),
                    table(Privilege::Insert, new),
                ]
            })
            .collect(),
        Statement::AlterTable(stmt) => {
            let mut out = vec![table(Privilege::Alter, &stmt.table)];
            // A rename via `ALTER TABLE ... RENAME TO` demands the same
            // privileges as `RENAME TABLE`: DROP on the source plus CREATE +
            // INSERT on the target (ALTER is already required above).
            for action in &stmt.actions {
                if let AlterAction::AddForeignKey(fk) = action {
                    out.push(foreign_key_requirement(session, &stmt.table, fk));
                }
                if let AlterAction::RenameTable { new_name } = action {
                    let target = qualify_alter_rename_target(session, &stmt.table, new_name);
                    out.push(table(Privilege::Drop, &stmt.table));
                    out.push(table(Privilege::Create, &target));
                    out.push(table(Privilege::Insert, &target));
                }
            }
            out
        }
        Statement::DropTable { names, .. } => names
            .iter()
            .map(|name| table(Privilege::Drop, name))
            .collect(),
        Statement::TruncateTable { name } => vec![table(Privilege::Drop, name)],
        Statement::CreateIndex(stmt) => vec![table(Privilege::Index, &stmt.table)],
        Statement::DropIndex { table: name, .. } => vec![table(Privilege::Index, name)],
        Statement::AnalyzeTable { tables, .. }
        | Statement::RepairTable { tables }
        | Statement::OptimizeTable { tables, .. } => tables
            .iter()
            .flat_map(|name| {
                [
                    table(Privilege::Select, name),
                    table(Privilege::Insert, name),
                ]
            })
            .collect(),
        Statement::CheckTable { tables } => {
            tables.iter().map(|t| table(Privilege::Select, t)).collect()
        }
        Statement::ChecksumTable { tables, .. } => {
            tables.iter().map(|t| table(Privilege::Select, t)).collect()
        }
        Statement::CreateView { name, query, .. } | Statement::AlterView { name, query, .. } => {
            let name = name.bind(&session.current_database).catalog_key();
            let mut required = vec![table(Privilege::CreateView, &name)];
            if matches!(
                statement,
                Statement::AlterView { .. }
                    | Statement::CreateView {
                        or_replace: true,
                        ..
                    }
            ) {
                required.push(table(Privilege::Drop, &name));
            }
            required.extend(auth_query_required_privileges(session, query));
            required
        }
        Statement::CreateRoutine(def) => {
            let (database, _) =
                crate::storage::split_table_name(&session.current_database, &def.name);
            vec![RequiredPrivilege {
                privilege: Privilege::CreateRoutine,
                scope: PrivilegeScope::Database { database },
            }]
        }
        Statement::CreateTrigger(def) => vec![table(Privilege::Trigger, &def.table)],
        Statement::AlterRoutine { kind, name, .. } => {
            vec![routine(Privilege::AlterRoutine, *kind, name)]
        }
        Statement::DropRoutine { kind, name, .. } => {
            vec![routine(Privilege::AlterRoutine, *kind, name)]
        }
        Statement::Call(call) => vec![routine(
            Privilege::Execute,
            RoutineKind::Procedure,
            &call.name,
        )],
        Statement::ShowCreateRoutine { .. } => Vec::new(),
        Statement::DropTrigger { .. } | Statement::ShowCreateTrigger { .. } => Vec::new(),
        Statement::ShowTriggers { .. } => Vec::new(),
        Statement::ShowRoutineStatus { .. } => Vec::new(),
        Statement::CreateEvent(def) => {
            let (database, _) =
                crate::storage::split_table_name(&session.current_database, &def.name);
            vec![RequiredPrivilege {
                privilege: Privilege::Event,
                scope: PrivilegeScope::Database { database },
            }]
        }
        Statement::AlterEvent(stmt) => {
            let (database, _) =
                crate::storage::split_table_name(&session.current_database, &stmt.name);
            vec![RequiredPrivilege {
                privilege: Privilege::Event,
                scope: PrivilegeScope::Database { database },
            }]
        }
        Statement::DropEvent { name, .. } => {
            let (database, _) = crate::storage::split_table_name(&session.current_database, name);
            vec![RequiredPrivilege {
                privilege: Privilege::Event,
                scope: PrivilegeScope::Database { database },
            }]
        }
        Statement::ShowCreateEvent { .. } | Statement::ShowEvents { .. } => Vec::new(),
        Statement::DropView { names, .. } => names
            .iter()
            .map(|name| {
                table(
                    Privilege::Drop,
                    &name.bind(&session.current_database).catalog_key(),
                )
            })
            .collect(),
        // EXPLAIN performs the same name/column resolution as the wrapped
        // statement, so MySQL requires the wrapped DML/SELECT privileges even
        // when ANALYZE is absent and no rows are touched.
        Statement::Explain { target, .. } => auth_required_privileges(session, target),
        Statement::SelectIntoVars { inner, .. } => auth_query_required_privileges(session, inner),
        Statement::SelectIntoFile { inner, .. } => {
            let mut out = auth_query_required_privileges(session, inner);
            out.push(global(Privilege::File));
            out
        }
        Statement::WithCte(_) | Statement::SetOperation(_) => {
            collect_statement_table_uses(statement)
                .into_iter()
                .filter(|u| {
                    !u.table.is_empty() && !reads_freely_readable_system_schema(session, &u.table)
                })
                .map(|u| table(Privilege::Select, &u.table))
                .collect()
        }
        Statement::CreateSequence {
            name, temporary, ..
        } => vec![table(
            if *temporary {
                Privilege::CreateTmpTable
            } else {
                Privilege::Create
            },
            name,
        )],
        Statement::AlterSequence {
            name, rename_to, ..
        } => {
            let mut out = vec![table(Privilege::Alter, name)];
            if let Some(target) = rename_to {
                out.extend([
                    table(Privilege::Drop, name),
                    table(Privilege::Create, target),
                ]);
            }
            out
        }
        Statement::DropSequence { names, .. } => names
            .iter()
            .map(|name| table(Privilege::Drop, name))
            .collect(),
        Statement::ShowCreateSequence { name } => vec![table(Privilege::Select, name)],
        Statement::FlushPrivileges => vec![global(Privilege::Reload)],
        Statement::DeclareSqlCursor { query, .. } => auth_query_required_privileges(session, query),
        Statement::Do(exprs) => {
            let mut uses = Vec::new();
            for expr in exprs {
                collect_expr_table_uses(expr, &Default::default(), &mut uses);
            }
            uses.into_iter()
                .filter(|u| {
                    !u.table.is_empty() && !reads_freely_readable_system_schema(session, &u.table)
                })
                .map(|u| table(Privilege::Select, &u.table))
                .collect()
        }
        Statement::Insert(stmt) => {
            let mut out = vec![table(Privilege::Insert, &stmt.table)];
            if stmt.returning.is_some() {
                out.push(table(Privilege::Select, &stmt.table));
            }
            // REPLACE may delete the conflicting row before inserting, so MySQL
            // requires both INSERT and DELETE privileges on the target table.
            if stmt.replace {
                out.push(table(Privilege::Delete, &stmt.table));
            }
            if let InsertSource::Query(select) = &stmt.source {
                let mut uses = Vec::new();
                let cte_names = std::collections::HashSet::new();
                collect_statement_table_uses_scoped(select, &cte_names, &mut uses);
                out.extend(
                    uses.into_iter()
                        .filter(|table_use| {
                            !table_use.table.is_empty()
                                && !reads_freely_readable_system_schema(session, &table_use.table)
                        })
                        .map(|table_use| table(Privilege::Select, &table_use.table)),
                );
            } else if let InsertSource::Values(rows) = &stmt.source {
                // Scalar/EXISTS/IN subqueries inside VALUES are reads too.
                // Their execution materializes the subquery before DML
                // dispatch, so privilege checks must include those sources
                // at the statement boundary just like INSERT ... SELECT.
                let mut uses = Vec::new();
                for expression in rows.iter().flatten() {
                    collect_expr_table_uses(
                        expression,
                        &std::collections::HashSet::new(),
                        &mut uses,
                    );
                }
                out.extend(
                    uses.into_iter()
                        .filter(|table_use| {
                            !table_use.table.is_empty()
                                && !reads_freely_readable_system_schema(session, &table_use.table)
                        })
                        .map(|table_use| table(Privilege::Select, &table_use.table)),
                );
            }
            if let Some(assignments) = &stmt.on_duplicate_key_update {
                out.push(table(Privilege::Update, &stmt.table));
                if !odku_read_columns(stmt).is_empty() {
                    out.push(table(Privilege::Select, &stmt.table));
                }
                let mut uses = Vec::new();
                for assignment in assignments {
                    collect_expr_table_uses(
                        &assignment.value,
                        &std::collections::HashSet::new(),
                        &mut uses,
                    );
                }
                out.extend(
                    uses.into_iter()
                        .filter(|table_use| {
                            !table_use.table.is_empty()
                                && !reads_freely_readable_system_schema(session, &table_use.table)
                        })
                        .map(|table_use| table(Privilege::Select, &table_use.table)),
                );
            }
            out
        }
        Statement::Update(stmt) => {
            let wrapped = Statement::Update(stmt.clone());
            let uses = collect_statement_table_uses(&wrapped);
            let mut out = uses
                .iter()
                .into_iter()
                .filter(|table_use| {
                    table_use.write
                        || !reads_freely_readable_system_schema(session, &table_use.table)
                })
                .map(|table_use| {
                    table(
                        if table_use.write {
                            Privilege::Update
                        } else {
                            Privilege::Select
                        },
                        &table_use.table,
                    )
                })
                .collect::<Vec<_>>();
            for table_use in uses.iter().filter(|table_use| table_use.write) {
                let source = authorization_table_ref(&table_use.table, table_use.alias.as_deref());
                if !update_read_columns(stmt, &source).is_empty() {
                    out.push(table(Privilege::Select, &table_use.table));
                }
            }
            if stmt.returning.is_some() {
                out.push(table(Privilege::Select, &stmt.table));
            }
            out
        }
        Statement::Delete(stmt) => {
            let wrapped = Statement::Delete(stmt.clone());
            let uses = collect_statement_table_uses(&wrapped);
            let mut out = uses
                .iter()
                .into_iter()
                .filter(|table_use| {
                    table_use.write
                        || !reads_freely_readable_system_schema(session, &table_use.table)
                })
                .map(|table_use| {
                    table(
                        if table_use.write {
                            Privilege::Delete
                        } else {
                            Privilege::Select
                        },
                        &table_use.table,
                    )
                })
                .collect::<Vec<_>>();
            for table_use in uses.iter().filter(|table_use| table_use.write) {
                let source = authorization_table_ref(&table_use.table, table_use.alias.as_deref());
                if !delete_read_columns(stmt, &source).is_empty() {
                    out.push(table(Privilege::Select, &table_use.table));
                }
            }
            if stmt.returning.is_some() {
                out.push(table(Privilege::Select, &stmt.table));
            }
            out
        }
        Statement::Select(stmt) => {
            let mut uses = Vec::new();
            let cte_names = std::collections::HashSet::new();
            collect_select_table_uses(stmt, stmt.for_update, &cte_names, &mut uses);
            uses.into_iter()
                .filter(|table_use| !table_use.table.is_empty())
                // information_schema is readable by
                // everyone (implicit USAGE) — MySQL gates only row visibility,
                // not access, which the IS row generators enforce per object.
                .filter(|table_use| !reads_freely_readable_system_schema(session, &table_use.table))
                .map(|table_use| table(Privilege::Select, &table_use.table))
                .collect()
        }
        Statement::Describe { table: name }
        | Statement::ShowColumns { table: name, .. }
        | Statement::ShowIndex { table: name, .. }
        | Statement::ShowCreateTable { table: name } => vec![table(Privilege::Select, name)],
        Statement::LoadData(stmt) => {
            // MySQL: server-side LOAD DATA requires the FILE privilege
            // because the server reads the file off its own filesystem;
            // LOAD DATA LOCAL streams the file from the client, so FILE
            // is deliberately NOT required (only table INSERT). Gating
            // LOCAL on FILE was G7 — a spurious privilege demand that
            // rejected legitimate client-side loads.
            let mut required = vec![table(Privilege::Insert, &stmt.table)];
            if !stmt.local {
                required.push(global(Privilege::File));
            }
            if stmt.replace {
                required.push(table(Privilege::Delete, &stmt.table));
            }
            required
        }

        // MySQL requires no privilege to see your *own* threads; PROCESS only
        // widens the view to other accounts' threads. The scoping is enforced
        // at the row level in `show_processlist_result` (a non-PROCESS caller
        // sees only its own connections), so there is no blanket requirement
        // here — a hard PROCESS gate would wrongly reject the self-view.
        Statement::ShowProcesslist { .. } | Statement::ShowMydbMemorySessions => vec![],
        // Ownership and live grants must be checked against the same locked
        // target identity in kill_session; a blanket gate hides error 1095.
        Statement::Kill { .. } => vec![],
        Statement::Shutdown | Statement::Restart => vec![global(Privilege::Shutdown)],
        Statement::MydbReplicationAdmin { sql }
            if sql
                .trim()
                .trim_end_matches(';')
                .trim()
                .eq_ignore_ascii_case("START MYDB REPLICATION FORCE_RESYNC") =>
        {
            vec![
                global(Privilege::Dynamic("SYSTEM_VARIABLES_ADMIN".to_string())),
                global(Privilege::Shutdown),
            ]
        }
        Statement::AlterSystemCheckpoint
        | Statement::AlterSystemFlushDataCache
        | Statement::AlterSystemFlushSqlCache
        | Statement::MydbReplicationAdmin { .. } => vec![global(Privilege::Dynamic(
            "SYSTEM_VARIABLES_ADMIN".to_string(),
        ))],
        Statement::ReplicationAdmin { sql } => {
            if sql.trim_start().to_ascii_uppercase().starts_with("SHOW ") {
                vec![global(Privilege::ReplClient)]
            } else {
                vec![global(Privilege::Dynamic("REPLICATION_SLAVE_ADMIN".into()))]
            }
        }
        Statement::BackupAdmin { .. } => {
            vec![global(Privilege::Dynamic("BACKUP_ADMIN".to_string()))]
        }
        // P1-3: RESET PERSIST clears persisted global variable overrides —
        // parity with SET GLOBAL/PERSIST which already require this privilege.
        Statement::ResetPersist { .. } => vec![global(Privilege::Dynamic(
            "SYSTEM_VARIABLES_ADMIN".to_string(),
        ))],
        Statement::BinlogAdmin { sql } => crate::binlog::admin_privilege(sql)
            .map(global)
            .into_iter()
            .collect(),
        Statement::FlushLogs { .. } => vec![global(Privilege::Reload)],
        // INSTALL/UNINSTALL COMPONENT is admin-only. MySQL gates it on INSERT
        // to mysql system tables; mydb requires SYSTEM_VARIABLES_ADMIN (SUPER
        // implies it) as the closest admin capability.
        Statement::InstallComponent { .. } | Statement::UninstallComponent { .. } => {
            vec![global(Privilege::Dynamic(
                "SYSTEM_VARIABLES_ADMIN".to_string(),
            ))]
        }
        Statement::ShowEngineMydbStatus
        | Statement::ShowMydbMemory
        | Statement::ShowEngine { .. } => vec![global(Privilege::Process)],
        Statement::CreateTablespace { .. }
        | Statement::DropTablespace { .. }
        | Statement::AlterTablespace { .. } => vec![global(Privilege::CreateTablespace)],
        Statement::CreateResourceGroup { .. }
        | Statement::DropResourceGroup { .. }
        | Statement::AlterResourceGroup { .. } => vec![global(Privilege::Dynamic(
            "RESOURCE_GROUP_ADMIN".to_string(),
        ))],
        Statement::SetResourceGroup { .. } => vec![global(Privilege::Dynamic(
            "RESOURCE_GROUP_USER".to_string(),
        ))],
        Statement::XaRecover { .. } => {
            vec![global(Privilege::Dynamic("XA_RECOVER_ADMIN".to_string()))]
        }
        Statement::CancelDdlJob(_) => vec![global(Privilege::Dynamic(
            "SYSTEM_VARIABLES_ADMIN".to_string(),
        ))],
        Statement::FlushUserResources => vec![global(Privilege::Reload)],
        Statement::FlushHosts => vec![global(Privilege::Reload)],
        Statement::FlushStatus => vec![global(Privilege::Reload)],
        Statement::FlushTables { .. } => vec![global(Privilege::Reload)],
        Statement::FlushQueryCache => vec![global(Privilege::Reload)],
        Statement::CreateUser(_) => vec![global(Privilege::CreateUser)],
        Statement::AlterUser(stmt)
            if stmt.current_user || account_ident_matches_session(&stmt.account, session) =>
        {
            let mut required = Vec::new();
            // Password self-service must not exempt accompanying account-policy
            // changes. Check dual-password authority independently as well.
            if stmt.plugin.is_some()
                || stmt.account_locked.is_some()
                || stmt.password_expired.is_some()
                || stmt.ssl_requirement.is_some()
                || stmt.resource_options != crate::parser::ResourceOptionsPatch::default()
                || stmt.failed_login_attempts.is_some()
                || stmt.password_lock_time_days.is_some()
                || stmt.password_lifetime_days.is_some()
                || stmt.password_history.is_some()
                || stmt.password_reuse_interval_days.is_some()
                || stmt.password_require_current.is_some()
                || stmt.comment.is_some()
                || stmt.attribute_json.is_some()
                || !(stmt.password.is_some() || stmt.random_password || stmt.discard_old_password)
            {
                required.push(global(Privilege::CreateUser));
            }
            if stmt.retain_current_password || stmt.discard_old_password {
                required.push(global(Privilege::Dynamic(
                    "APPLICATION_PASSWORD_ADMIN".to_string(),
                )));
            }
            required
        }
        Statement::AlterUser(_)
        | Statement::AlterUsers(_)
        | Statement::DropUser { .. }
        | Statement::RenameUser { .. } => {
            vec![global(Privilege::CreateUser)]
        }
        Statement::ShowCreateUser { account }
            if (account.user == "CURRENT_USER()" && account.host.is_empty())
                || account_ident_matches_session(account, session) =>
        {
            Vec::new()
        }
        Statement::ShowGrantsFor { account, .. }
            if (account.user == "CURRENT_USER()" && account.host.is_empty())
                || account_ident_matches_session(account, session) =>
        {
            Vec::new()
        }
        Statement::ShowGrantsFor { .. } => vec![table(Privilege::Select, "mysql.user")],
        Statement::ShowCreateUser { .. } => vec![database(Privilege::Select, "mysql")],
        Statement::CreateRole { .. } => vec![global(Privilege::CreateRole)],
        Statement::DropRole { .. } => vec![global(Privilege::DropRole)],
        Statement::Grant(_) | Statement::Revoke(_) => Vec::new(),
        Statement::SetPassword {
            account: None,
            retain_current: true,
            ..
        } => vec![global(Privilege::Dynamic(
            "APPLICATION_PASSWORD_ADMIN".to_string(),
        ))],
        Statement::SetPassword { account: None, .. } => Vec::new(),
        Statement::SetPassword {
            account: Some(account),
            retain_current,
            ..
        } if account_ident_matches_session(account, session) => {
            if *retain_current {
                vec![global(Privilege::Dynamic(
                    "APPLICATION_PASSWORD_ADMIN".to_string(),
                ))]
            } else {
                Vec::new()
            }
        }
        Statement::SetPassword { .. } => vec![global(Privilege::CreateUser)],
        Statement::SetDefaultRole(stmt) => {
            // MySQL requires CREATE USER to set default roles for
            // *other* accounts. Setting your own default roles (among roles
            // already granted to you — enforced in execute_set_default_role)
            // needs no special privilege.
            let targets = match stmt {
                SetDefaultRole::All { to }
                | SetDefaultRole::None { to }
                | SetDefaultRole::Roles { to, .. } => to,
            };
            if targets
                .iter()
                .all(|acct| account_ident_matches_session(acct, session))
            {
                Vec::new()
            } else {
                vec![global(Privilege::CreateUser)]
            }
        }
        Statement::Set(assignments) => {
            // MySQL: writing any GLOBAL system variable (also SET PERSIST /
            // PERSIST_ONLY) requires SYSTEM_VARIABLES_ADMIN; PERSIST_ONLY
            // additionally requires PERSIST_RO_VARIABLES_ADMIN. Session and
            // user-variable (`@x`) assignments need no privilege. This is the
            // single authoritative gate — the registry-level check is only a
            // defense-in-depth backstop and no longer name-lists variables.
            let mut need_sysvar_admin = false;
            let mut need_persist_ro = false;
            for assignment in assignments {
                match set_assignment_scope_for_auth(assignment) {
                    Some(SetScope::Global) | Some(SetScope::Persist) => need_sysvar_admin = true,
                    Some(SetScope::PersistOnly) => {
                        need_sysvar_admin = true;
                        need_persist_ro = true;
                    }
                    _ => {}
                }
            }
            let mut uses = Vec::new();
            for assignment in assignments {
                collect_expr_table_uses(&assignment.value, &Default::default(), &mut uses);
            }
            let mut out: Vec<_> = uses
                .into_iter()
                .filter(|u| {
                    !u.table.is_empty() && !reads_freely_readable_system_schema(session, &u.table)
                })
                .map(|u| table(Privilege::Select, &u.table))
                .collect();
            if need_sysvar_admin {
                out.push(global(Privilege::Dynamic(
                    "SYSTEM_VARIABLES_ADMIN".to_string(),
                )));
            }
            if need_persist_ro {
                out.push(global(Privilege::Dynamic(
                    "PERSIST_RO_VARIABLES_ADMIN".to_string(),
                )));
            }
            out
        }
        Statement::SetTransactionIsolation {
            scope: Some(SetScope::Global),
            ..
        }
        | Statement::SetTransactionCharacteristics {
            scope: Some(SetScope::Global),
            ..
        } => {
            vec![global(Privilege::Dynamic(
                "SYSTEM_VARIABLES_ADMIN".to_string(),
            ))]
        }
        // Explicitly classified session operations and handler-filtered metadata.
        Statement::Begin { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. }
        | Statement::Savepoint(_)
        | Statement::RollbackToSavepoint(_)
        | Statement::ReleaseSavepoint(_)
        | Statement::Prepare { .. }
        | Statement::ExecutePrepared { .. }
        | Statement::DeallocatePrepared { .. }
        | Statement::SetAutocommit(_)
        | Statement::SetTransactionIsolation { .. }
        | Statement::ShowSequences { .. }
        | Statement::ExplainForConnection { .. }
        | Statement::UseDatabase(_)
        | Statement::ShowDatabases { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowTableStatus { .. }
        | Statement::ShowCreateDatabase { .. }
        | Statement::ShowCharset { .. }
        | Statement::ShowCollation { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowStatus { .. }
        | Statement::ShowDiagnostics { .. }
        | Statement::GetDiagnostics { .. }
        | Statement::ShowCountDiagnostics { .. }
        | Statement::ShowEngines
        | Statement::ShowPlugins
        | Statement::ShowOpenTables { .. }
        | Statement::ShowGrants
        | Statement::ShowPrivileges
        | Statement::SetRole(_)
        | Statement::SetNames { .. }
        | Statement::SetCharacterSet { .. }
        | Statement::SetTransactionCharacteristics { .. }
        | Statement::Noop { .. }
        | Statement::Unsupported { .. }
        | Statement::MultiStatement { .. }
        | Statement::XaStart { .. }
        | Statement::XaEnd { .. }
        | Statement::XaPrepare { .. }
        | Statement::XaCommit { .. }
        | Statement::XaRollback { .. }
        | Statement::FetchSqlCursor { .. }
        | Statement::CloseSqlCursor { .. } => Vec::new(),
    }
}

/// Apply the same implicit metadata access rules as I_S visibility.
fn reads_freely_readable_system_schema(session: &SessionContext, name: &str) -> bool {
    let (database, table) = crate::storage::split_table_name(&session.current_database, name);
    crate::engine::information_schema::implicitly_readable_metadata_table(&database, &table)
}

/// Effective SET scope for privilege gating. Mirrors `execute_set_statement`'s
/// scope resolution: an explicit `GLOBAL`/`PERSIST`/… qualifier OR a
/// `@@global.` / `persist.` … name prefix. Returns `None` for user variables
/// (`@x`), which never require a privilege.
fn set_assignment_scope_for_auth(assignment: &SetAssignment) -> Option<SetScope> {
    let mut scope = assignment.scope;
    let mut name = assignment.name.to_ascii_lowercase();
    if let Some(rest) = name.strip_prefix("@@") {
        name = rest.to_string();
    }
    for (prefix, prefixed) in [
        ("session.", SetScope::Session),
        ("local.", SetScope::Session),
        ("global.", SetScope::Global),
        ("persist.", SetScope::Persist),
        ("persist_only.", SetScope::PersistOnly),
    ] {
        if let Some(rest) = name.strip_prefix(prefix) {
            scope = Some(prefixed);
            name = rest.to_string();
            break;
        }
    }
    if name.starts_with('@') {
        return None;
    }
    // No qualifier and no prefix → session-scoped (Default resolves to Session
    // for settable vars); such writes need no privilege.
    scope.or(Some(SetScope::Session))
}

pub(super) fn prepared_required_privileges(
    session: &SessionContext,
    statement: &Statement,
) -> Vec<RequiredPrivilege> {
    auth_required_privileges(session, statement)
}

pub(super) fn authorize_statement(
    db: &Arc<Database>,
    session: &SessionContext,
    statement: &Statement,
) -> Result<(), QueryError> {
    let required = auth_required_privileges(session, statement);
    authorize_prepared_statement(db, session, statement, &required)
}

pub(super) fn authorize_prepared_statement(
    db: &Arc<Database>,
    session: &SessionContext,
    statement: &Statement,
    required_privileges: &[RequiredPrivilege],
) -> Result<(), QueryError> {
    authorize_prepared_statement_with_epoch(db, session, statement, required_privileges).map(|_| ())
}

pub(super) fn authorize_prepared_statement_with_epoch(
    db: &Arc<Database>,
    session: &SessionContext,
    statement: &Statement,
    required_privileges: &[RequiredPrivilege],
) -> Result<u64, QueryError> {
    db.ensure_operational()?;
    // Global SQL privileges never grant ownership of another session's table.
    // Apply this before root/internal-user privilege shortcuts and bound recipes.
    for required in required_privileges {
        if let PrivilegeScope::Table { database, .. } = &required.scope {
            if database.eq_ignore_ascii_case("__mydb_temp")
                && !required_privilege_targets_session_temp(session, required)
            {
                return Err(error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    "Temporary table is not visible in this session",
                ));
            }
        }
    }
    match statement {
        Statement::UseDatabase(name) | Statement::ShowCreateDatabase { name, .. } => {
            crate::engine::auth::require_database_access(db, session, name)?
        }
        Statement::ShowTables { database, .. }
        | Statement::ShowTableStatus { database, .. }
        | Statement::ShowSequences { database, .. } => {
            crate::engine::auth::require_database_access(
                db,
                session,
                database.as_deref().unwrap_or(&session.current_database),
            )?
        }
        _ => {}
    }
    let generation = crate::engine::auth::authorization_generation();
    let bound = if matches!(statement,Statement::Update(u) if !u.joins.is_empty())
        || matches!(statement,Statement::Delete(d) if !d.joins.is_empty())
    {
        super::auth_recipe::BoundAccessRecipe::build(db, session, statement)
    } else {
        None
    };
    let bound_required = bound.as_ref().map(|r| r.requirements());
    let required_privileges = bound_required.as_deref().unwrap_or(required_privileges);
    let explain_view_privileges = explain_dml_view_privileges(db, session, statement);
    // F8: statements with no required privileges (SELECT 1 / SHOW WARNINGS
    // / session-local SET / etc.) skip the catalog lock entirely. Returning
    // the cached session epoch is fine because no grants will be consulted.
    if required_privileges.is_empty()
        && explain_view_privileges.is_empty()
        && trigger_name_for_table_privilege(statement).is_none()
    {
        return Ok(generation);
    }
    // Borrow the auth catalog through the closure-form accessor instead
    // of cloning it. The previous `db.auth_catalog()` deep-cloned every
    // BTreeMap / Vec inside it on every COM_STMT_EXECUTE — a clear
    // contributor to the per-statement allocation churn measured in the
    // sysbench oltp_point_select profile (alongside
    // `with_session_expr_info` and `cancel_checker`). Borrowing is safe
    // because all downstream consumers
    // (`view_security_override_allows`, `account_has_privilege`,
    // `column_privilege_fallback`) take `&AuthCatalog`.
    let ctx = session.authorization_context();
    let _authorized_epoch = db.with_auth_catalog(|auth| {
        for required in required_privileges
            .iter()
            .chain(explain_view_privileges.iter())
        {
            if !matches!(required.privilege, Privilege::CreateTmpTable)
                && required_privilege_targets_session_temp(session, &required)
            {
                continue;
            }
            if view_security_override_allows(db, auth, session, statement, &required)
                || auth.account_has_privilege(&ctx, &required, false)
                || column_privilege_fallback(db, auth, &ctx, session, statement, &required)
            {
                continue;
            }
            if matches!(
                statement,
                Statement::CreateRole { .. } | Statement::DropRole { .. }
            ) && auth.account_has_privilege(
                &ctx,
                &RequiredPrivilege {
                    privilege: Privilege::CreateUser,
                    scope: PrivilegeScope::Global,
                },
                false,
            ) {
                continue;
            }
            if required.privilege == Privilege::CreateUser
                && matches!(
                    statement,
                    Statement::SetPassword { .. }
                        | Statement::AlterUser(_)
                        | Statement::AlterUsers(_)
                )
                && can_administer_passwords(auth, session)
            {
                continue;
            }
            if let Some(column) =
                super::auth_recipe::BoundAccessRecipe::build(db, session, statement)
                    .and_then(|recipe| recipe.denied_column(auth, &ctx, required))
            {
                return Err(error(
                    crate::error::catalog::ER_COLUMNACCESS_DENIED_ERROR,
                    format!(
                        "{} command denied to user '{}'@'{}' for column '{}'",
                        required.privilege.sql_name(),
                        ctx.authenticated_as.user,
                        ctx.authenticated_as.host,
                        column
                    ),
                ));
            }
            if let PrivilegeScope::Routine {
                database, routine, ..
            } = &required.scope
            {
                return Err(error(
                    crate::error::catalog::ER_PROCACCESS_DENIED_ERROR,
                    format!(
                        "{} command denied to user '{}'@'{}' for routine '{}.{}'",
                        required.privilege.sql_name().to_ascii_lowercase(),
                        ctx.authenticated_as.user,
                        ctx.authenticated_as.host,
                        database,
                        routine,
                    ),
                ));
            }
            return Err(privilege_denied(&required.privilege, &required.scope));
        }
        Ok(auth.epoch)
    })??;
    // Keep the EXPLAIN-only catalog walk and its stack frame off normal
    // SELECT/DML authorization paths, including unoptimized builds.
    if matches!(statement, Statement::Explain { .. }) {
        authorize_explain_view_dependencies(db, session, statement)?;
    }
    // Trigger-privilege check runs after the auth-catalog read lock is
    // released because `trigger_definition` itself takes the catalog
    // read lock, and `std::sync::RwLock` is not re-entrant on the same
    // thread.
    if let Some(trigger_name) = trigger_name_for_table_privilege(statement) {
        if let Some(trigger) = db.trigger_definition(&session.current_database, trigger_name) {
            let required = crate::engine::auth::RequiredPrivilege {
                privilege: Privilege::Trigger,
                scope: PrivilegeScope::Table {
                    database: trigger.table_database,
                    table: trigger.table_name,
                },
            };
            let allowed =
                db.with_auth_catalog(|auth| auth.account_has_privilege(&ctx, &required, false))?;
            if !allowed {
                return Err(privilege_denied(&required.privilege, &required.scope));
            }
        }
    }
    Ok(generation)
}

fn authorize_explain_view_dependencies(
    db: &Arc<Database>,
    session: &SessionContext,
    statement: &Statement,
) -> Result<(), QueryError> {
    let Statement::Explain { target, .. } = statement else {
        return Ok(());
    };
    let mut pending = Vec::new();
    for usage in collect_statement_table_uses(target) {
        if !session.has_temp_table(&usage.table) {
            if let Some(view) = db.view_definition(&session.current_database, &usage.table) {
                pending.push(view);
            }
        }
    }
    if pending.is_empty() {
        return Ok(());
    }
    let auth = db.auth_catalog();
    let caller = session.authorization_context();
    let mut visited = BTreeSet::new();
    let denied = || {
        error(
            crate::error::catalog::ER_VIEW_NO_EXPLAIN,
            "EXPLAIN/SHOW can not be issued; lacking privileges for underlying table",
        )
    };
    while let Some(view) = pending.pop() {
        if !visited.insert((
            view.database.to_ascii_lowercase(),
            view.name.to_ascii_lowercase(),
        )) {
            continue;
        }
        let required = RequiredPrivilege {
            privilege: Privilege::ShowView,
            scope: PrivilegeScope::Table {
                database: view.database.clone(),
                table: view.name.clone(),
            },
        };
        if !auth.account_has_privilege(&caller, &required, false) {
            return Err(denied());
        }
        for dependency in &view.dependencies {
            let required = RequiredPrivilege {
                privilege: Privilege::Select,
                scope: PrivilegeScope::Table {
                    database: dependency.database.clone(),
                    table: dependency.name.clone(),
                },
            };
            // EXPLAIN exposes the implementation of a DEFINER view. Its
            // dependencies must be authorized as the caller, never via the
            // execution-only security overrides.
            if !auth.account_has_privilege(&caller, &required, false) {
                return Err(denied());
            }
            if dependency.is_view {
                if let Some(nested) = db.view_definition(&dependency.database, &dependency.name) {
                    pending.push(nested);
                }
            }
        }
    }
    Ok(())
}

fn explain_dml_view_privileges(
    db: &Arc<Database>,
    session: &SessionContext,
    statement: &Statement,
) -> Vec<RequiredPrivilege> {
    let target = match statement {
        Statement::Explain { target, .. } => match target.as_ref() {
            Statement::WithCte(with) => with.body.as_ref(),
            other => other,
        },
        _ => return Vec::new(),
    };
    let table = match target {
        Statement::Insert(stmt) => &stmt.table,
        Statement::Update(stmt) => &stmt.table,
        Statement::Delete(stmt) => &stmt.table,
        _ => return Vec::new(),
    };
    let (database, table_name) = crate::storage::split_table_name(&session.current_database, table);
    if db.view_definition(&database, &table_name).is_none() {
        return Vec::new();
    }
    vec![RequiredPrivilege {
        privilege: Privilege::ShowView,
        scope: PrivilegeScope::Table {
            database,
            table: table_name,
        },
    }]
}

fn view_security_override_allows(
    db: &Database,
    auth: &crate::engine::auth::AuthCatalog,
    session: &SessionContext,
    statement: &Statement,
    required: &crate::engine::auth::RequiredPrivilege,
) -> bool {
    if required.privilege != Privilege::Select {
        return false;
    }
    let PrivilegeScope::Table { database, table } = &required.scope else {
        return false;
    };
    let Some(override_) = session.view_security_overrides.iter().find(|override_| {
        override_.database.eq_ignore_ascii_case(database)
            && override_.table.eq_ignore_ascii_case(table)
    }) else {
        return false;
    };
    auth.account_has_privilege(&override_.context, required, false)
        || column_privilege_fallback(db, auth, &override_.context, session, statement, required)
}

fn required_privilege_targets_session_temp(
    session: &SessionContext,
    required: &crate::engine::auth::RequiredPrivilege,
) -> bool {
    let PrivilegeScope::Table { database, table } = &required.scope else {
        return false;
    };
    let key = crate::storage::table_key(database, table);
    session.temp_tables.contains_key(&key)
        || session
            .temp_tables
            .values()
            .any(|temp| temp.internal_key.eq_ignore_ascii_case(&key))
}

fn column_privilege_fallback(
    db: &Database,
    auth: &crate::engine::auth::AuthCatalog,
    ctx: &crate::engine::auth::AuthorizationContext,
    session: &SessionContext,
    statement: &Statement,
    required: &crate::engine::auth::RequiredPrivilege,
) -> bool {
    if let PrivilegeScope::Table { database, table } = &required.scope {
        if matches!(
            statement,
            Statement::Describe { .. }
                | Statement::ShowColumns { .. }
                | Statement::ShowIndex { .. }
        ) {
            return crate::engine::information_schema::implicitly_readable_metadata_table(
                database, table,
            ) || auth.account_can_access_schema(ctx, database, Some(table));
        }
        if matches!(statement, Statement::ShowCreateTable { .. })
            && db.view_definition(database, table).is_none()
        {
            return crate::engine::information_schema::implicitly_readable_metadata_table(
                database, table,
            ) || crate::engine::auth::all_privileges_for_scope(&required.scope)
                .iter()
                .any(|privilege| {
                    auth.account_has_privilege(
                        ctx,
                        &RequiredPrivilege {
                            privilege: privilege.clone(),
                            scope: required.scope.clone(),
                        },
                        false,
                    )
                });
        }
    }
    if matches!(
        required.privilege,
        Privilege::Select | Privilege::Insert | Privilege::Update
    ) && matches!(
        statement,
        Statement::Select(_)
            | Statement::CreateView { .. }
            | Statement::AlterView { .. }
            | Statement::CreateTableAsSelect { .. }
            | Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            | Statement::WithCte(_)
            | Statement::SetOperation(_)
            | Statement::SelectIntoVars { .. }
            | Statement::SelectIntoFile { .. }
    ) {
        return super::auth_recipe::BoundAccessRecipe::build(db, session, statement)
            .is_some_and(|recipe| recipe.allows(auth, ctx, required));
    }
    if let Statement::Explain { target, .. } = statement {
        return column_privilege_fallback(db, auth, ctx, session, target, required);
    }
    let PrivilegeScope::Table { database, table } = &required.scope else {
        return false;
    };
    let columns = match (&required.privilege, statement) {
        (Privilege::Select, Statement::Select(select)) => select_column_requirements(select),
        (Privilege::Select, Statement::Insert(insert)) => {
            let columns = odku_read_columns(insert);
            (!columns.is_empty() && !columns.contains(COLUMN_SCAN_OPAQUE))
                .then(|| columns.into_iter().collect())
        }
        (Privilege::Update, Statement::Insert(insert)) => insert
            .on_duplicate_key_update
            .as_ref()
            .map(|a| a.iter().map(|a| a.column.clone()).collect()),
        (Privilege::Select, Statement::Update(update)) => update_select_column_requirements(update),
        (Privilege::Select, Statement::Delete(delete)) => delete_select_column_requirements(delete),
        (Privilege::Insert, Statement::Insert(insert)) => {
            insert.columns.clone().filter(|c| !c.is_empty())
        }
        (Privilege::Update, Statement::Update(update)) => {
            let cols: Vec<_> = update
                .assignments
                .iter()
                .map(|a| a.column.clone())
                .collect();
            (!cols.is_empty()).then_some(cols)
        }
        _ => None,
    };
    let Some(columns) = columns else {
        return false;
    };
    columns.into_iter().all(|column| {
        let required = crate::engine::auth::RequiredPrivilege {
            privilege: required.privilege.clone(),
            scope: PrivilegeScope::Column {
                database: database.clone(),
                table: table.clone(),
                column,
            },
        };
        auth.account_has_privilege(ctx, &required, false)
    }) || {
        // INSERT without an explicit column list may target every visible
        // column; keep table-level privilege as the only accepted form.
        let _ = session;
        false
    }
}

fn authorization_table_ref(name: &str, alias: Option<&str>) -> TableRef {
    TableRef {
        name: name.to_string(),
        alias: alias.map(str::to_string),
        system_time: None,
        index_hints: Vec::new(),
        partitions: Vec::new(),
        column_aliases: Vec::new(),
        generate_series: None,
    }
}

fn odku_read_columns(insert: &crate::parser::Insert) -> BTreeSet<String> {
    let table = authorization_table_ref(&insert.table, None);
    let mut columns = BTreeSet::new();
    if let Some(assignments) = &insert.on_duplicate_key_update {
        for assignment in assignments {
            collect_expr_columns(&assignment.value, &table, &mut columns);
        }
        for assignment in assignments {
            columns.retain(|c| !c.eq_ignore_ascii_case(&assignment.column));
        }
    }
    columns
}

fn update_read_columns(update: &crate::parser::Update, table: &TableRef) -> BTreeSet<String> {
    let mut columns = BTreeSet::new();
    if let Some(expr) = &update.selection {
        collect_expr_columns(expr, table, &mut columns);
    }
    for assignment in &update.assignments {
        collect_expr_columns(&assignment.value, table, &mut columns);
    }
    for order in &update.order_by {
        collect_expr_columns(&order.expr, table, &mut columns);
    }
    for join in &update.joins {
        if let Some(expr) = &join.condition {
            collect_expr_columns(expr, table, &mut columns);
        }
        columns.extend(join.using_columns.iter().cloned());
    }
    // MySQL needs SELECT only for columns read but not modified. UPDATE on
    // the assigned column also authorizes its old value (SET x=x+1).
    for assignment in &update.assignments {
        let matches = assignment.table_qualifier.as_deref().map_or(
            table.name.eq_ignore_ascii_case(&update.table),
            |qualifier| {
                table
                    .alias
                    .as_deref()
                    .is_some_and(|a| a.eq_ignore_ascii_case(qualifier))
                    || table
                        .name
                        .rsplit('.')
                        .next()
                        .is_some_and(|n| n.eq_ignore_ascii_case(qualifier))
                    || table.name.eq_ignore_ascii_case(qualifier)
            },
        );
        if matches {
            columns.retain(|c| !c.eq_ignore_ascii_case(&assignment.column));
        }
    }
    columns
}

fn delete_read_columns(delete: &crate::parser::Delete, table: &TableRef) -> BTreeSet<String> {
    let mut columns = BTreeSet::new();
    if let Some(expr) = &delete.selection {
        collect_expr_columns(expr, table, &mut columns);
    }
    for order in &delete.order_by {
        collect_expr_columns(&order.expr, table, &mut columns);
    }
    for join in &delete.joins {
        if let Some(expr) = &join.condition {
            collect_expr_columns(expr, table, &mut columns);
        }
        columns.extend(join.using_columns.iter().cloned());
    }
    columns
}

fn update_select_column_requirements(update: &crate::parser::Update) -> Option<Vec<String>> {
    if !update.joins.is_empty() {
        return None;
    }
    let columns = update_read_columns(
        update,
        &authorization_table_ref(&update.table, update.primary_alias.as_deref()),
    );
    (!columns.is_empty() && !columns.contains(COLUMN_SCAN_OPAQUE))
        .then(|| columns.into_iter().collect())
}

fn delete_select_column_requirements(delete: &crate::parser::Delete) -> Option<Vec<String>> {
    if !delete.joins.is_empty() {
        return None;
    }
    let columns = delete_read_columns(
        delete,
        &authorization_table_ref(&delete.table, delete.primary_alias.as_deref()),
    );
    (!columns.is_empty() && !columns.contains(COLUMN_SCAN_OPAQUE))
        .then(|| columns.into_iter().collect())
}

/// Sentinel column name `collect_expr_columns` inserts when it meets a node
/// (subquery / EXISTS) whose referenced columns it cannot enumerate. A NUL
/// byte can't appear in a real SQL identifier, so its presence unambiguously
/// means "column set is incomplete — do not narrow to column scope" (B4).
const COLUMN_SCAN_OPAQUE: &str = "\0__column_scan_opaque__";

fn select_column_requirements(select: &Select) -> Option<Vec<String>> {
    if select.from.len() != 1 || !select.joins.is_empty() {
        return None;
    }
    let table = &select.from[0];
    let mut columns = std::collections::BTreeSet::new();
    let mut saw_wildcard = false;
    for item in &select.projection {
        match item {
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => saw_wildcard = true,
            SelectItem::Expr { expr, .. } => collect_expr_columns(expr, table, &mut columns),
        }
    }
    if let Some(expr) = &select.selection {
        collect_expr_columns(expr, table, &mut columns);
    }
    for expr in &select.group_by {
        collect_expr_columns(expr, table, &mut columns);
    }
    if let Some(expr) = &select.having {
        collect_expr_columns(expr, table, &mut columns);
    }
    for order in &select.order_by {
        collect_expr_columns(&order.expr, table, &mut columns);
    }
    if saw_wildcard || columns.contains(COLUMN_SCAN_OPAQUE) || columns.is_empty() {
        None
    } else {
        Some(columns.into_iter().collect())
    }
}

fn collect_expr_columns(
    expr: &Expr,
    table: &TableRef,
    out: &mut std::collections::BTreeSet<String>,
) {
    crate::parser::visit::inspect_value_expr(
        expr,
        |expr| {
            match expr {
                Expr::Identifier(name) => {
                    out.insert(name.clone());
                }
                Expr::CompoundIdentifier(parts) => {
                    if let Some(column) = parts.last() {
                        let qualifier = parts.get(parts.len().saturating_sub(2));
                        if qualifier.is_none()
                            || qualifier.is_some_and(|q| {
                                table
                                    .alias
                                    .as_ref()
                                    .is_some_and(|alias| alias.eq_ignore_ascii_case(q))
                                    || table.name.ends_with(&format!(".{q}"))
                                    || table.name.eq_ignore_ascii_case(q)
                            })
                        {
                            out.insert(column.clone());
                        }
                    }
                }
                Expr::Function { name, .. } if name.eq_ignore_ascii_case("VALUES") => return false,
                Expr::InSubquery { expr, .. } | Expr::QuantifiedComparison { left: expr, .. } => {
                    collect_expr_columns(expr, table, out);
                    out.insert(COLUMN_SCAN_OPAQUE.to_string());
                    return false;
                }
                Expr::Subquery(_) | Expr::Exists { .. } => {
                    out.insert(COLUMN_SCAN_OPAQUE.to_string());
                    return false;
                }
                _ => {}
            }
            true
        },
        |_| {},
    );
}

fn trigger_name_for_table_privilege(statement: &Statement) -> Option<&str> {
    match statement {
        Statement::DropTrigger { name, .. } | Statement::ShowCreateTrigger { name } => {
            Some(name.as_str())
        }
        _ => None,
    }
}

/// INSTALL/UNINSTALL COMPONENT. mydb only implements the opt-in
/// `validate_password` component; any other component URN is rejected (no
/// silent no-op). The installation is durable and exposed by mysql.component.
fn execute_component(
    db: &Arc<Database>,
    session: &mut SessionContext,
    names: &[String],
    install: bool,
) -> Result<QueryResponse, QueryError> {
    use crate::engine::password_policy as pol;
    static COMPONENT_CHANGE: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _change = COMPONENT_CHANGE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    implicit_commit_for_ddl(db, session)?;
    for name in names {
        if !pol::is_validate_password_component(name) {
            return Err(error(
                crate::error::catalog::ER_COMPONENTS_CANT_LOAD,
                format!("Cannot load component from specified URN: '{name}'."),
            ));
        }
    }
    if names.len() != 1 {
        return Err(error(
            crate::error::catalog::ER_COMPONENTS_CANT_LOAD,
            "Duplicate validate_password component in one statement",
        ));
    }
    for name in names {
        if install {
            if db.auth_catalog().password_validation_component {
                return Err(error(
                    crate::error::catalog::ER_COMPONENTS_CANT_LOAD,
                    format!(
                        "Cannot load component from specified URN: '{name}'. (already installed)"
                    ),
                ));
            }
            db.set_password_validation_component(true)?;
        } else {
            if !db.auth_catalog().password_validation_component {
                return Err(error(
                    crate::error::catalog::ER_COMPONENTS_LOAD_CANT_REGISTER_SERVICE_IMPLEMENTATION,
                    format!(
                        "Cannot unload never-installed component with specified URN: '{name}'."
                    ),
                ));
            }
            db.set_password_validation_component(false)?;
        }
    }
    Ok(QueryResponse::Ok)
}

fn referenced_definers(db: &Database) -> BTreeMap<AccountKey, &'static str> {
    let catalog = db.catalog();
    let mut out = BTreeMap::new();
    for v in catalog.views.values() {
        out.insert(AccountKey::new(&v.definer_user, &v.definer_host), "view");
    }
    for v in catalog.routines.values() {
        out.insert(
            AccountKey::new(&v.definer_user, &v.definer_host),
            "stored routine",
        );
    }
    for v in catalog.triggers.values() {
        out.insert(AccountKey::new(&v.definer_user, &v.definer_host), "trigger");
    }
    for v in catalog.events.values() {
        out.insert(AccountKey::new(&v.definer_user, &v.definer_host), "event");
    }
    out
}
fn check_definer_account_change(
    auth: &crate::engine::auth::AuthCatalog,
    session: &SessionContext,
    references: &BTreeMap<AccountKey, &str>,
    account: &AccountKey,
    operation: &str,
) -> Result<(), QueryError> {
    let Some(kind) = references.get(account) else {
        return Ok(());
    };
    let permitted = session.internal_system_session
        || auth.account_has_privilege(
            &session.authorization_context(),
            &RequiredPrivilege {
                privilege: Privilege::Dynamic("SET_USER_ID".into()),
                scope: PrivilegeScope::Global,
            },
            false,
        );
    let message = format!(
        "Operation {operation} for {} references a definer account in a {kind}",
        account.display()
    );
    if !permitted {
        return Err(error(
            crate::error::catalog::ER_CANNOT_USER_REFERENCED_AS_DEFINER,
            message,
        ));
    }
    crate::engine::api::push_statement_warning(
        crate::error::ConditionLevel::Warning,
        crate::error::catalog::ER_USER_REFERENCED_AS_DEFINER,
        message,
    );
    Ok(())
}

fn execute_create_user(
    db: &Arc<Database>,
    session: &SessionContext,
    stmt: crate::parser::CreateUser,
) -> Result<QueryResponse, QueryError> {
    let references = referenced_definers(db);
    let mut cache_seeds = Vec::new();
    let mut created_accounts = Vec::new();
    let mut generated_passwords = Vec::new();
    db.mutate_auth_catalog(|auth| {
        let default_roles = stmt
            .default_roles
            .iter()
            .map(account_key)
            .collect::<Vec<_>>();
        for role in &default_roles {
            if !auth.accounts.get(&account_id(role)).is_some() {
                return Err(error(
                    crate::error::catalog::ER_USER_DOES_NOT_EXIST,
                    format!("Authorization ID {} does not exist", role.display()),
                ));
            }
        }
        ensure_system_account_access(auth, session, default_roles.iter())?;
        if !session.internal_system_session {
            let ctx = session.authorization_context();
            for role in &default_roles {
                if !auth.account_can_administer_role(&ctx, role) {
                    return Err(error(
                        crate::error::catalog::ER_SPECIFIC_ACCESS_DENIED_ERROR,
                        format!(
                            "Access denied; you need ROLE_ADMIN or WITH ADMIN OPTION for role {}",
                            role.display()
                        ),
                    ));
                }
            }
        }
        for spec in stmt.accounts {
            let key = account_key(&spec.account);
            validate_account_key(&key)?;
            if auth.accounts.contains_key(&account_id(&key)) {
                if stmt.if_not_exists {
                    crate::engine::api::push_statement_warning(
                        crate::error::ConditionLevel::Note,
                        crate::error::catalog::ER_USER_ALREADY_EXISTS,
                        format!("Authorization ID {} already exists.", key.display()),
                    );
                    continue;
                }
                return Err(error(
                    crate::error::catalog::ER_CANNOT_USER,
                    format!("Operation CREATE USER failed for {}", key.display()),
                ));
            }
            check_definer_account_change(auth, session, &references, &key, "CREATE USER")?;
            let plugin = spec.plugin.clone().unwrap_or_else(default_auth_plugin);
            let has_explicit_password = spec.password.is_some() || spec.random_password;
            let password = if spec.random_password {
                generate_random_password()
            } else {
                spec.password.unwrap_or_default()
            };
            if !crate::engine::auth::is_supported_auth_plugin(&plugin) {
                return Err(error(
                    crate::error::catalog::ER_PLUGIN_IS_NOT_LOADED,
                    format!("Plugin '{plugin}' is not loaded"),
                ));
            }
            if spec.password_is_pre_hashed
                && !crate::engine::auth::is_valid_authentication_string(&plugin, &password)
            {
                return Err(error(
                    crate::error::catalog::ER_PASSWORD_FORMAT,
                    format!(
                        "The password hash doesn't have the expected format for plugin '{plugin}'"
                    ),
                ));
            }
            // Stage 3: validate_password enforcement on the plaintext. Skipped
            // for `IDENTIFIED WITH ... AS '<hash>'` (pre-hashed) and no-ops
            // unless the component is installed. Rejects weak passwords (1819).
            if has_explicit_password && !spec.password_is_pre_hashed {
                crate::engine::password_policy::validate_with_installation(
                    auth.password_validation_component,
                    &password,
                    Some(&key.user),
                )?;
            }
            // G3: `IDENTIFIED WITH plugin AS '<hash>'` stores the credential
            // verbatim — used by mysqldump backups and replication mirroring
            // where the plaintext password is never available. Otherwise we
            // hash the plaintext with the plugin's `password_auth_string`.
            let auth_string = if spec.password_is_pre_hashed {
                password.clone()
            } else {
                checked_password_auth_string(&plugin, &password)?
            };
            let mut account = UserAccount::new(key.clone(), plugin.clone(), auth_string.clone());
            account.is_role = stmt.is_role;
            account.account_locked = spec.account_locked;
            account.password_expired = spec.password_expired;
            account.ssl_type = ssl_requirement(spec.ssl_requirement);
            account.max_questions = spec.resource_options.max_queries_per_hour;
            account.max_updates = spec.resource_options.max_updates_per_hour;
            account.max_connections = spec.resource_options.max_connections_per_hour;
            account.max_user_connections = spec.resource_options.max_user_connections;
            account.failed_login_attempts = spec.failed_login_attempts.unwrap_or(0);
            account.password_lock_time_days = spec.password_lock_time_days;
            account.password_lifetime_days = spec.password_lifetime_days;
            account.password_reuse_history = spec.password_history;
            account.password_reuse_time_days = spec.password_reuse_interval_days;
            account.password_require_current = spec.password_require_current;
            // B3 parity with ALTER USER: COMMENT / ATTRIBUTE merge into the
            // account's user_attributes JSON (metadata.comment for COMMENT).
            if spec.attribute_json.is_some() || spec.comment.is_some() {
                account.user_attributes_json = Some(merge_user_attributes(
                    account.user_attributes_json.as_deref(),
                    spec.attribute_json.as_deref(),
                    spec.comment.as_deref(),
                )?);
            }
            auth.accounts.insert(account_id(&key), account);
            created_accounts.push(key.clone());
            if spec.random_password {
                generated_passwords.push((key.clone(), password.clone()));
            }
            if plugin == "caching_sha2_password" && !spec.password_is_pre_hashed {
                cache_seeds.push((key.clone(), password.clone(), auth_string));
            }
            for role in &default_roles {
                let edge = RoleEdge {
                    from_role: role.clone(),
                    to_account: key.clone(),
                    admin_option: false,
                };
                auth.role_edges.push(edge.clone());
                auth.default_roles.push(edge);
            }
        }
        Ok(())
    })?;
    for key in created_accounts {
        crate::engine::auth::failed_login_clear(&key);
        crate::engine::auth::auth_cache_forget(&key);
    }
    for (account, password, credential) in cache_seeds {
        crate::engine::auth::auth_cache_remember_password(&account, &password, &credential);
    }
    if !generated_passwords.is_empty() {
        return Ok(QueryResponse::ResultSet(crate::engine::api::ResultSet {
            columns: vec![
                col("user", crate::types::ColumnType::VarString),
                col("host", crate::types::ColumnType::VarString),
                col("generated password", crate::types::ColumnType::VarString),
                col("auth_factor", crate::types::ColumnType::Tiny),
            ],
            rows: generated_passwords
                .into_iter()
                .map(|(account, password)| {
                    vec![
                        Value::String(account.user),
                        Value::String(account.host),
                        Value::String(password),
                        Value::UInt(1),
                    ]
                })
                .collect(),
        }));
    }
    Ok(QueryResponse::Ok)
}

struct AlterUserEffects {
    target_key: AccountKey,
    clear_login_failures: bool,
    credential_cache_changed: bool,
    cache_password: Option<String>,
    cache_credential: Option<String>,
}

fn execute_alter_users(
    db: &Arc<Database>,
    session: &SessionContext,
    statements: Vec<crate::parser::AlterUser>,
) -> Result<QueryResponse, QueryError> {
    // Persist all accounts atomically. Runtime caches are updated only after
    // the catalog mutation commits, so a later error cannot leak a new password.
    let mut effects = Vec::with_capacity(statements.len());
    let mut result: Option<crate::engine::api::ResultSet> = None;
    db.mutate_auth_catalog(|auth| {
        for stmt in statements {
            let (effect, generated) = apply_alter_user(auth, session, stmt)?;
            effects.push(effect);
            if let Some(mut generated) = generated {
                if let Some(rows) = result.as_mut() {
                    rows.rows.append(&mut generated.rows);
                } else {
                    result = Some(generated);
                }
            }
        }
        Ok(())
    })?;
    for AlterUserEffects {
        target_key,
        clear_login_failures,
        credential_cache_changed,
        cache_password,
        cache_credential,
    } in effects
    {
        if clear_login_failures {
            crate::engine::auth::failed_login_clear(&target_key);
        }
        if credential_cache_changed {
            crate::engine::auth::auth_cache_forget(&target_key);
            if let (Some(password), Some(credential)) =
                (cache_password.as_deref(), cache_credential.as_deref())
            {
                crate::engine::auth::auth_cache_remember_password(
                    &target_key,
                    password,
                    credential,
                );
            }
        }
    }
    Ok(result
        .map(QueryResponse::ResultSet)
        .unwrap_or(QueryResponse::Ok))
}

fn apply_alter_user(
    auth: &mut crate::engine::auth::AuthCatalog,
    session: &SessionContext,
    stmt: crate::parser::AlterUser,
) -> Result<(AlterUserEffects, Option<crate::engine::api::ResultSet>), QueryError> {
    // L4: RANDOM PASSWORD generates a 20-char ASCII password the executor
    // returns to the client. Resolve it up-front so we can both apply it
    // *and* surface it in the result set below.
    let mut generated_random_password: Option<String> = None;
    if stmt.random_password {
        generated_random_password = Some(generate_random_password());
    }
    let cache_password = generated_random_password.clone().or_else(|| {
        (!stmt.password_is_pre_hashed)
            .then(|| stmt.password.clone())
            .flatten()
    });
    let credential_cache_changed =
        cache_password.is_some() || stmt.plugin.is_some() || stmt.discard_old_password;

    let target_key = if stmt.current_user {
        session.authenticated_as.clone()
    } else {
        account_key(&stmt.account)
    };
    let target_user = target_key.user.clone();
    let target_host = target_key.host.clone();
    validate_account_key(&target_key)?;

    if stmt.replace_old_password.is_some() && target_key != session.authenticated_as {
        return Err(error(
            crate::error::catalog::ER_CURRENT_PASSWORD_NOT_REQUIRED,
            "REPLACE can be specified only when changing the current user's password",
        ));
    }
    if stmt.retain_current_password && stmt.password.is_none() && !stmt.random_password {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "RETAIN CURRENT PASSWORD requires a new password",
        ));
    }
    if stmt.retain_current_password && stmt.discard_old_password {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "RETAIN CURRENT PASSWORD and DISCARD OLD PASSWORD are mutually exclusive",
        ));
    }
    if stmt.retain_current_password && stmt.plugin.is_some() {
        return Err(error(
            crate::error::catalog::ER_PASSWORD_CANNOT_BE_RETAINED_ON_PLUGIN_CHANGE,
            "The current password cannot be retained when changing the authentication plugin",
        ));
    }

    ensure_system_account_access(auth, session, std::iter::once(&target_key))?;

    let privileged_password_change = can_administer_passwords(auth, session);
    let reset_plugin_credentials =
        stmt.plugin.is_some() && stmt.password.is_none() && !stmt.random_password;
    let mut cache_credential = None;
    let mut clear_login_failures = false;
    (|| -> Result<(), QueryError> {
        let Some(account) = auth.accounts.get_mut(&account_id(&target_key)) else {
            if stmt.if_exists {
                // L14: emit warning 3162 (`ER_USER_DOES_NOT_EXIST`) and
                // succeed. Matches MySQL 8.0.29+ ALTER USER IF EXISTS.
                crate::engine::api::push_statement_warning(
                    crate::error::ConditionLevel::Note,
                    crate::error::catalog::ER_USER_DOES_NOT_EXIST,
                    format!("Authorization ID {} does not exist.", target_key.display()),
                );
                return Ok(());
            }
            return Err(error(
                crate::error::catalog::ER_CANNOT_USER,
                format!("Operation ALTER USER failed for {}", target_key.display()),
            ));
        };
        let previous_plugin = account.plugin.clone();
        if let Some(plugin) = stmt.plugin {
            if !crate::engine::auth::is_supported_auth_plugin(&plugin) {
                return Err(error(
                    crate::error::catalog::ER_PLUGIN_IS_NOT_LOADED,
                    format!("Plugin '{plugin}' is not loaded"),
                ));
            }
            if account.plugin != plugin || reset_plugin_credentials {
                account.secondary_authentication_string = None;
            }
            account.plugin = plugin;
            if reset_plugin_credentials {
                account.authentication_string.clear();
                account.password_expired = true;
                account.password_last_changed_ms = Some(current_time_ms());
                account.failed_login_count = 0;
                account.locked_until_ms = None;
                clear_login_failures = true;
            }
        }
        if stmt.password_is_pre_hashed
            && stmt.password.as_ref().is_some_and(|password| {
                !crate::engine::auth::is_valid_authentication_string(&account.plugin, password)
            })
        {
            return Err(error(
                crate::error::catalog::ER_PASSWORD_FORMAT,
                format!(
                    "The password hash doesn't have the expected format for plugin '{}'",
                    account.plugin
                ),
            ));
        }

        // Determine the new authentication string (if any). Order:
        //   1. RANDOM PASSWORD wins outright (plaintext just generated)
        //   2. password + pre_hashed → store verbatim
        //   3. password (plaintext) → hash via plugin
        //   4. no password change → leave alone
        let new_auth_string: Option<String> = if let Some(ref plain) = generated_random_password {
            Some(checked_password_auth_string(&account.plugin, plain)?)
        } else if let Some(password) = stmt.password.clone() {
            if stmt.password_is_pre_hashed {
                Some(password)
            } else {
                // Stage 3: validate_password enforcement on the new plaintext
                // (no-op unless the component is installed).
                crate::engine::password_policy::validate_with_installation(
                    auth.password_validation_component,
                    &password,
                    Some(&account.key.user),
                )?;
                Some(checked_password_auth_string(&account.plugin, &password)?)
            }
        } else {
            None
        };

        if let Some(new_auth) = new_auth_string {
            // L2: PASSWORD REQUIRE CURRENT enforcement.
            let require_current = account
                .password_require_current
                .unwrap_or_else(crate::engine::auth::global_password_require_current);
            if require_current && !privileged_password_change {
                let Some(old_plain) = stmt.replace_old_password.as_deref() else {
                    return Err(error(crate::error::catalog::ER_MISSING_CURRENT_PASSWORD,
                        "Current password needs to be specified in the REPLACE clause in order to change it.",
                    ));
                };
                if !crate::engine::auth::verify_plaintext_auth_string(
                    &previous_plugin,
                    &account.authentication_string,
                    old_plain,
                ) {
                    return Err(error(crate::error::catalog::ER_INCORRECT_CURRENT_PASSWORD,
                        "Incorrect current password. Specify the correct password which has to be replaced.",
                    ));
                }
            } else if let Some(old_plain) = stmt.replace_old_password.as_deref() {
                // REPLACE given but not strictly required — still verify it,
                // matching MySQL 8.0 behaviour. Mismatch is fatal.
                if !crate::engine::auth::verify_plaintext_auth_string(
                    &previous_plugin,
                    &account.authentication_string,
                    old_plain,
                ) {
                    return Err(error(crate::error::catalog::ER_INCORRECT_CURRENT_PASSWORD,
                        "Incorrect current password. Specify the correct password which has to be replaced.",
                    ));
                }
            }

            // L1: history / reuse interval enforcement. password_history
            // stores the plugin-hashed auth string (sha256 of); we compare
            // candidate by hashing through the same plugin first. Skip for
            // pre-hashed credentials only when the *outgoing* and incoming
            // already match (they're indistinguishable to us).
            let reused = if stmt.password_is_pre_hashed {
                account.password_auth_string_reused(&new_auth, current_time_ms())
            } else if let Some(plain) = generated_random_password
                .as_deref()
                .or(stmt.password.as_deref())
            {
                account.plaintext_password_reused(plain, current_time_ms())
            } else {
                false
            };
            if reused {
                return Err(error(
                    crate::error::catalog::ER_CREDENTIALS_CONTRADICT_TO_HISTORY,
                    format!(
                        "Cannot use these credentials for {} because they contradict the password history policy",
                        account.key.display()
                    ),
                ));
            }

            // L3: RETAIN CURRENT PASSWORD — preserve current auth string as
            // secondary before we overwrite primary.
            if stmt.retain_current_password {
                if account.authentication_string.is_empty() {
                    return Err(error(
                        crate::error::catalog::ER_SECOND_PASSWORD_CANNOT_BE_EMPTY,
                        "The current password is empty and cannot be retained",
                    ));
                }
                account.secondary_authentication_string =
                    Some(account.authentication_string.clone());
            }
            // L1: push the *outgoing* credential onto the history before
            // overwriting. We skip the synthetic empty default when the
            // account was just freshly minted with no password.
            if !account.authentication_string.is_empty() {
                let outgoing = account.authentication_string.clone();
                account.push_password_history(&outgoing, current_time_ms());
            }
            account.authentication_string = new_auth;
            account.password_last_changed_ms = Some(current_time_ms());
            account.password_expired = false;
        }
        if stmt.discard_old_password {
            account.secondary_authentication_string = None;
        }
        if let Some(locked) = stmt.account_locked {
            account.account_locked = locked;
            // ACCOUNT UNLOCK also clears any temporary failed-login lock.
            if !locked {
                clear_login_failures = true;
            }
        }
        if let Some(expired) = stmt.password_expired {
            account.password_expired = expired;
        }
        if let Some(ssl) = stmt.ssl_requirement {
            account.ssl_type = ssl_requirement(Some(ssl));
        }
        if let Some(value) = stmt.resource_options.max_queries_per_hour {
            account.max_questions = value;
        }
        if let Some(value) = stmt.resource_options.max_updates_per_hour {
            account.max_updates = value;
        }
        if let Some(value) = stmt.resource_options.max_connections_per_hour {
            account.max_connections = value;
        }
        if let Some(value) = stmt.resource_options.max_user_connections {
            account.max_user_connections = value;
        }
        if let Some(value) = stmt.failed_login_attempts {
            account.failed_login_attempts = value;
            account.failed_login_count = 0;
            account.locked_until_ms = None;
            clear_login_failures = true;
        }
        if let Some(value) = stmt.password_lock_time_days {
            account.password_lock_time_days = Some(value);
        }
        if let Some(value) = stmt.password_lifetime_days {
            account.password_lifetime_days = match value {
                crate::parser::AccountPolicy::Default => None,
                crate::parser::AccountPolicy::Value(value) => Some(value),
            };
        }
        if let Some(value) = stmt.password_history {
            account.password_reuse_history = match value {
                crate::parser::AccountPolicy::Default => None,
                crate::parser::AccountPolicy::Value(value) => Some(value),
            };
        }
        if let Some(value) = stmt.password_reuse_interval_days {
            account.password_reuse_time_days = match value {
                crate::parser::AccountPolicy::Default => None,
                crate::parser::AccountPolicy::Value(value) => Some(value),
            };
        }
        if let Some(value) = stmt.password_require_current {
            account.password_require_current = match value {
                crate::parser::AccountPolicy::Default => None,
                crate::parser::AccountPolicy::Value(value) => Some(value),
            };
        }
        // L5: ATTRIBUTE / COMMENT — merge-patched into user_attributes_json.
        if stmt.attribute_json.is_some() || stmt.comment.is_some() {
            account.user_attributes_json = Some(merge_user_attributes(
                account.user_attributes_json.as_deref(),
                stmt.attribute_json.as_deref(),
                stmt.comment.as_deref(),
            )?);
        }
        cache_credential = Some(account.authentication_string.clone());
        Ok(())
    })()?;
    let effects = AlterUserEffects {
        target_key,
        clear_login_failures,
        credential_cache_changed,
        cache_password,
        cache_credential,
    };
    if let Some(plain) = generated_random_password {
        let row = vec![
            Value::String(target_user),
            Value::String(target_host),
            Value::String(plain),
            Value::UInt(1),
        ];
        return Ok((
            effects,
            Some(crate::engine::api::ResultSet {
                columns: vec![
                    col("user", crate::types::ColumnType::VarString),
                    col("host", crate::types::ColumnType::VarString),
                    col("generated password", crate::types::ColumnType::VarString),
                    col("auth_factor", crate::types::ColumnType::Tiny),
                ],
                rows: vec![row],
            }),
        ));
    }
    Ok((effects, None))
}

/// Generate a 20-character ASCII password matching MySQL 8.0
/// `IDENTIFIED BY RANDOM PASSWORD`. Character set is the printable subset
/// MySQL uses (no quotes / backslashes that would need escaping).
fn generate_random_password() -> String {
    use rand::{rngs::OsRng, seq::SliceRandom, RngCore};
    const LOWER: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
    const UPPER: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const DIGIT: &[u8] = b"0123456789";
    const SPECIAL: &[u8] = b"!#%&()*+,-./:;<=>?@[]^_{|}~";
    const CHARSET: &[u8] =
        b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!#%&()*+,-./:;<=>?@[]^_{|}~";
    let mut output = vec![LOWER[0], UPPER[0], DIGIT[0], SPECIAL[0]];
    for (slot, choices) in output.iter_mut().zip([LOWER, UPPER, DIGIT, SPECIAL]) {
        *slot = *choices.choose(&mut OsRng).expect("non-empty alphabet");
    }
    while output.len() < 20 {
        let mut byte = [0u8; 1];
        OsRng.fill_bytes(&mut byte);
        let ceiling = 256usize - (256usize % CHARSET.len());
        if usize::from(byte[0]) < ceiling {
            output.push(CHARSET[usize::from(byte[0]) % CHARSET.len()]);
        }
    }
    output.shuffle(&mut OsRng);
    String::from_utf8(output).expect("password alphabet is ASCII")
}

/// Rolling CRC32 over SQL-visible rows in physical scan order. Each value
/// uses its canonical display bytes, separated by 0x00; rows end in 0x01.
/// This is mydb's digest, not MySQL's engine-specific storage checksum.
/// Missing tables yield NULL without aborting the remaining table list.
fn execute_checksum_table(
    db: &Arc<Database>,
    session: &mut SessionContext,
    tables: &[String],
) -> Result<QueryResponse, QueryError> {
    use crate::storage::TxnView;
    let columns = vec![
        col("Table", crate::types::ColumnType::VarString),
        col("Checksum", crate::types::ColumnType::LongLong),
    ];
    let mut rows = Vec::with_capacity(tables.len());
    // Capture the view lazily for the first existing table and keep its undo
    // guard until the entire list is consumed. Even at READ COMMITTED, every
    // table in this statement must use the same snapshot.
    let mut scan_view: Option<(TxnView, Option<crate::storage::SnapshotGuard>)> = None;
    for tbl in tables {
        let (db_name, name) = crate::storage::split_table_name(&session.current_database, tbl);
        let qualified = format!("{db_name}.{name}");
        // Missing tables: surface a NULL row instead of erroring out.
        let resolved = qualify_table_name(session, tbl);
        if db.table(&resolved).is_err() {
            rows.push(vec![Value::String(qualified), Value::Null]);
            continue;
        }
        let txn = if let Some((txn, _guard)) = &scan_view {
            *txn
        } else {
            // CHECKSUM is a consistent read, not the current-read view used
            // by DML. Choose the view exactly like a plain SELECT: outside a
            // transaction it must not open one (a physical replica rejects
            // the BEGIN record), and RR snapshots stay pinned and protected
            // from concurrent undo purge.
            let (txn, guard) =
                if session.autocommit && !session.explicit_txn && session.current_txn.is_none() {
                    let (txn, guard) = super::tx::autocommit_read_only_view_with_guard(db, session);
                    (txn, Some(guard))
                } else if session.current_txn.is_none()
                    && session.xa_branch.is_none()
                    && session.effective_isolation() != crate::storage::IsolationLevel::Serializable
                {
                    super::tx::lazy_read_only_transaction_view_with_guard(db, session)
                } else {
                    current_transaction_read_select_view(db, session)?
                };
            scan_view = Some((txn, guard));
            txn
        };
        let mut checksum = TableChecksum::new();
        match db.scan_rows_ordered_streaming(&resolved, txn, |values| {
            checksum.update_row(values);
            Ok(())
        }) {
            Ok(()) => {
                rows.push(vec![
                    Value::String(qualified),
                    Value::Int(checksum.finalize() as i64),
                ]);
            }
            Err(err) => {
                // Scan errored mid-stream (e.g. table dropped between
                // resolve and scan). MySQL surfaces this as NULL rather
                // than aborting the multi-table command — track the
                // warning so operators can see what happened.
                crate::engine::api::push_statement_warning(
                    crate::error::ConditionLevel::Warning,
                    crate::error::catalog::ER_PARSE_ERROR,
                    format!("CHECKSUM TABLE {qualified}: {}", err.message()),
                );
                rows.push(vec![Value::String(qualified), Value::Null]);
            }
        }
    }
    Ok(QueryResponse::ResultSet(crate::engine::api::ResultSet {
        columns,
        rows,
    }))
}

#[cfg(test)]
#[path = "tests/checksum_digest.rs"]
mod checksum_digest_tests;

/// Batch CRC updates so hardware CRC implementations process long slices,
/// and reuse canonical stack formatters instead of allocating per scalar.
struct TableChecksum {
    hasher: crc32fast::Hasher,
    buffer: Vec<u8>,
}

impl TableChecksum {
    const BUFFER_BYTES: usize = 64 * 1024;

    fn new() -> Self {
        Self {
            hasher: crc32fast::Hasher::new(),
            buffer: Vec::with_capacity(Self::BUFFER_BYTES),
        }
    }

    fn append(&mut self, bytes: &[u8]) {
        if bytes.len() > Self::BUFFER_BYTES - self.buffer.len() {
            self.hasher.update(&self.buffer);
            self.buffer.clear();
        }
        if bytes.len() >= Self::BUFFER_BYTES {
            self.hasher.update(bytes);
        } else {
            self.buffer.extend_from_slice(bytes);
        }
    }

    fn update_row(&mut self, values: &[Value]) {
        let mut temporal = [0u8; 32];
        let mut decimal = [0u8; 48];
        let mut integer = itoa::Buffer::new();
        for (i, value) in values.iter().enumerate() {
            if i != 0 {
                self.append(&[0]);
            }
            match value {
                Value::Int(n) => self.append(integer.format(*n).as_bytes()),
                Value::UInt(n) => self.append(integer.format(*n).as_bytes()),
                Value::DecimalCompact(n, scale) if *scale <= 30 => self.append(
                    crate::types::write_decimal_compact_canonical(*n, *scale, &mut decimal),
                ),
                _ => {
                    if let Some(bytes) = value.canonical_cmp_bytes(&mut temporal) {
                        self.append(bytes);
                    } else {
                        // Preserve lossy UTF-8, float, wide decimal, JSON, GIS
                        // and vector display semantics through the shared path.
                        self.append(value.to_display_string().as_bytes());
                    }
                }
            }
        }
        self.append(&[1]);
    }

    fn finalize(mut self) -> u32 {
        self.hasher.update(&self.buffer);
        self.hasher.finalize()
    }
}

fn execute_table_maintenance_status(
    db: &Arc<Database>,
    session: &SessionContext,
    tables: &[String],
    op: &str,
) -> Result<QueryResponse, QueryError> {
    let columns = vec![
        col("Table", crate::types::ColumnType::VarString),
        col("Op", crate::types::ColumnType::VarString),
        col("Msg_type", crate::types::ColumnType::VarString),
        col("Msg_text", crate::types::ColumnType::VarString),
    ];
    let mut rows = Vec::with_capacity(tables.len());
    for tbl in tables {
        let (db_name, name) = crate::storage::split_table_name(&session.current_database, tbl);
        let result = (|| {
            if op == "optimize" && session.has_temp_table(tbl) {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "online OPTIMIZE TABLE does not support temporary tables",
                ));
            }
            let schema = db.table(&qualify_table_name(session, tbl))?;
            if op == "optimize" {
                db.optimize_table_online(&schema.qualified_name(), session.connection_id as u64)?;
                // Log only the targets that actually succeeded. The generic
                // DDL audit cannot distinguish per-table maintenance failures.
                let sql = format!(
                    "OPTIMIZE TABLE `{}`.`{}`",
                    db_name.replace('`', "``"),
                    name.replace('`', "``")
                );
                crate::binlog::record_ddl_autocommit_with(
                    &db_name,
                    &sql,
                    &super::binlog_ctx_from_session(session),
                )?;
            }
            Ok(schema)
        })();
        match result {
            Ok(schema) => {
                let qualified = format!("{db_name}.{name}");
                // CHECK TABLE (M6, docs/lob-management-audit-2026-07.md):
                // actually validate LOB integrity — `.lob` free-list /
                // page structure and every live row's off-page chain —
                // instead of the old unconditional "OK". Any problem is
                // reported as an `error` row (MySQL CHECK shape); a clean
                // table still returns `status = OK`.
                if op == "check" {
                    let mut problems = db.check_table_lob_integrity(&schema);
                    problems.extend(db.check_table_index_integrity(&schema));
                    if problems.is_empty() {
                        rows.push(vec![
                            Value::String(qualified),
                            Value::String(op.to_string()),
                            Value::String("status".to_string()),
                            Value::String("OK".to_string()),
                        ]);
                    } else {
                        for problem in problems {
                            rows.push(vec![
                                Value::String(qualified.clone()),
                                Value::String(op.to_string()),
                                Value::String("error".to_string()),
                                Value::String(problem),
                            ]);
                        }
                    }
                    continue;
                }
                let (msg_type, msg_text): (&str, &str) = match op {
                    "optimize" => ("status", "OK"),
                    // REPAIR: mydb has no MyISAM-style on-disk repair
                    // path; surface MySQL's standard Note row.
                    "repair" => (
                        "note",
                        "The storage engine for the table doesn't support repair",
                    ),
                    _ => ("status", "OK"),
                };
                rows.push(vec![
                    Value::String(qualified),
                    Value::String(op.to_string()),
                    Value::String(msg_type.to_string()),
                    Value::String(msg_text.to_string()),
                ]);
            }
            Err(err) => {
                if err.code() == crate::error::catalog::ER_QUERY_INTERRUPTED.code() {
                    return Err(err);
                }
                let qualified = format!("{db_name}.{name}");
                rows.push(vec![
                    Value::String(qualified),
                    Value::String(op.to_string()),
                    Value::String("Error".to_string()),
                    Value::String(err.message().to_string()),
                ]);
                rows.push(vec![
                    Value::String(format!("{db_name}.{name}")),
                    Value::String(op.to_string()),
                    Value::String("status".to_string()),
                    Value::String("Operation failed".to_string()),
                ]);
            }
        }
    }
    Ok(QueryResponse::ResultSet(crate::engine::api::ResultSet {
        columns,
        rows,
    }))
}
/// Merge ATTRIBUTE/COMMENT into the stored JSON per RFC 7396 merge-patch
/// semantics. COMMENT lives at `metadata.comment` for MySQL compatibility.
fn merge_user_attributes(
    existing: Option<&str>,
    attribute_json: Option<&str>,
    comment: Option<&str>,
) -> Result<String, QueryError> {
    let mut current: serde_json::Value = match existing {
        Some(s) => serde_json::from_str(s).unwrap_or(serde_json::json!({})),
        None => serde_json::json!({}),
    };
    if let Some(attr) = attribute_json {
        let parsed = serde_json::from_str::<serde_json::Value>(attr).map_err(|err| {
            error(
                crate::error::catalog::ER_INVALID_USER_ATTRIBUTE_JSON,
                format!("Invalid user attribute JSON: {err}"),
            )
        })?;
        if !parsed.is_object() {
            return Err(error(
                crate::error::catalog::ER_INVALID_USER_ATTRIBUTE_JSON,
                "User attribute must be a JSON object",
            ));
        }
        json_merge_patch(&mut current, &parsed);
    }
    if let Some(comment) = comment {
        let Some(object) = current.as_object_mut() else {
            return Err(error(
                crate::error::catalog::ER_INVALID_USER_ATTRIBUTE_JSON,
                "User attributes must remain a JSON object",
            ));
        };
        let metadata = object
            .entry("metadata")
            .or_insert_with(|| serde_json::json!({}));
        if let Some(obj) = metadata.as_object_mut() {
            obj.insert(
                "comment".to_string(),
                serde_json::Value::String(comment.to_string()),
            );
        }
    }
    serde_json::to_string(&current).map_err(|err| {
        error(
            crate::error::catalog::ER_INVALID_USER_ATTRIBUTE_JSON,
            format!("Invalid user attribute JSON: {err}"),
        )
    })
}

fn json_merge_patch(target: &mut serde_json::Value, patch: &serde_json::Value) {
    use serde_json::Value;
    match (target, patch) {
        (Value::Object(target_map), Value::Object(patch_map)) => {
            for (k, v) in patch_map {
                if v.is_null() {
                    target_map.remove(k);
                } else {
                    let entry = target_map.entry(k.clone()).or_insert(Value::Null);
                    json_merge_patch(entry, v);
                }
            }
        }
        (target_slot, patch_value) => {
            *target_slot = patch_value.clone();
        }
    }
}

fn execute_set_password(
    db: &Arc<Database>,
    session: &SessionContext,
    account: Option<AccountIdent>,
    mut password: String,
    random_password: bool,
    replace: Option<String>,
    retain_current: bool,
) -> Result<QueryResponse, QueryError> {
    let key = account
        .as_ref()
        .map(|account| show_account_key(account, session))
        .unwrap_or_else(|| session.authenticated_as.clone());
    if random_password {
        password = generate_random_password();
    }
    if replace.is_some() && key != session.authenticated_as {
        return Err(error(
            crate::error::catalog::ER_CURRENT_PASSWORD_NOT_REQUIRED,
            "REPLACE can be specified only when changing the current user's password",
        ));
    }
    let mut cache_credential = String::new();
    db.mutate_auth_catalog(|auth| {
        ensure_system_account_access(auth, session, std::iter::once(&key))?;
        let privileged_password_change = can_administer_passwords(auth, session);
        let Some(account) = auth.accounts.get_mut(&account_id(&key)) else {
            return Err(error(crate::error::catalog::ER_CANNOT_USER,
                format!("Operation SET PASSWORD failed for {}", key.display()),
            ));
        };
        // Stage 3: validate_password enforcement (SET PASSWORD always takes a
        // plaintext string in MySQL 8.0). No-op unless the component is on.
        crate::engine::password_policy::validate_with_installation(auth.password_validation_component, &password, Some(&key.user))?;
        // PASSWORD REQUIRE CURRENT: the account policy mandates that the
        // operator supply the old password before switching. MySQL 4047:
        // ER_CURRENT_PASSWORD_NOT_REQUIRED-style — actually 4047 is
        // "current password specified when not required" so the missing
        // case uses 4045/3892; we settle on 3892 to match MySQL 8.0.4+.
        if account
            .password_require_current
            .unwrap_or_else(crate::engine::auth::global_password_require_current)
            && !privileged_password_change
            && replace.is_none()
        {
            return Err(error(crate::error::catalog::ER_MISSING_CURRENT_PASSWORD,
                format!(
                    "Current password needs to be specified when changing the password for {} \
                     because the user account has password_require_current enabled",
                    key.display()
                ),
            ));
        }
        // If REPLACE is supplied (whether required or not), the supplied
        // old password must hash to the current authentication_string.
        if let Some(old) = replace.as_ref() {
            if !crate::engine::auth::verify_plaintext_auth_string(
                &account.plugin,
                &account.authentication_string,
                old,
            ) {
                return Err(error(crate::error::catalog::ER_INCORRECT_CURRENT_PASSWORD,
                    "Incorrect current password. Specify the correct password which has to be replaced.",
                ));
            }
        }
        // History stores hashes of plugin authentication strings, so compare
        // like-for-like. Passing plaintext here made SET PASSWORD bypass the
        // policy while ALTER USER correctly rejected the same reuse.
        let new_auth_string = checked_password_auth_string(&account.plugin, &password)?;
        if account.plaintext_password_reused(&password, current_time_ms()) {
            return Err(error(
                crate::error::catalog::ER_CREDENTIALS_CONTRADICT_TO_HISTORY,
                format!(
                    "Cannot use these credentials for {} because they contradict the password history policy",
                    account.key.display()
                ),
            ));
        }
        // Either roll the old password into the history (default), or
        // promote it to the dual-password slot (RETAIN CURRENT PASSWORD).
        if !account.authentication_string.is_empty() {
            let outgoing = account.authentication_string.clone();
            if retain_current {
                account.secondary_authentication_string = Some(outgoing);
            } else {
                account.push_password_history(&outgoing, current_time_ms());
            }
        }
        if retain_current && account.authentication_string.is_empty() {
            return Err(error(
                crate::error::catalog::ER_SECOND_PASSWORD_CANNOT_BE_EMPTY,
                "The current password is empty and cannot be retained",
            ));
        }
        account.authentication_string = new_auth_string;
        cache_credential = account.authentication_string.clone();
        account.password_last_changed_ms = Some(current_time_ms());
        account.password_expired = false;
        Ok(())
    })?;
    crate::engine::auth::auth_cache_forget(&key);
    crate::engine::auth::auth_cache_remember_password(&key, &password, &cache_credential);
    if random_password {
        return Ok(QueryResponse::ResultSet(crate::engine::api::ResultSet {
            columns: vec![
                col("user", crate::types::ColumnType::VarString),
                col("host", crate::types::ColumnType::VarString),
                col("generated password", crate::types::ColumnType::VarString),
                col("auth_factor", crate::types::ColumnType::Tiny),
            ],
            rows: vec![vec![
                Value::String(key.user),
                Value::String(key.host),
                Value::String(password),
                Value::UInt(1),
            ]],
        }));
    }
    Ok(QueryResponse::Ok)
}

fn execute_drop_accounts(
    db: &Arc<Database>,
    session: &SessionContext,
    accounts: Vec<AccountIdent>,
    if_exists: bool,
    roles_only: bool,
) -> Result<QueryResponse, QueryError> {
    let references = referenced_definers(db);
    let authority = db.auth_catalog();
    let account_keys = accounts.iter().map(account_key).collect::<Vec<_>>();
    for account in &account_keys {
        validate_account_key(account)?;
    }
    db.with_auth_catalog(|auth| ensure_system_account_access(auth, session, account_keys.iter()))??;
    db.mutate_auth_catalog(|auth| {
        if roles_only {
            let mandatory = auth.mandatory_role_keys();
            if let Some(role) = accounts
                .iter()
                .map(account_key)
                .find(|role| mandatory.contains(role))
            {
                return Err(error(
                    crate::error::catalog::ER_MANDATORY_ROLE,
                    format!(
                        "Role {} is a mandatory role and cannot be dropped",
                        role.display()
                    ),
                ));
            }
        }
        for account in accounts {
            let key = account_key(&account);
            match auth.accounts.get(&account_id(&key)) {
                Some(_) => {}
                None if if_exists => {
                    crate::engine::api::push_statement_warning(
                        crate::error::ConditionLevel::Note,
                        crate::error::catalog::ER_USER_DOES_NOT_EXIST,
                        format!("Authorization ID {} does not exist.", key.display()),
                    );
                    continue;
                }
                None => {
                    return Err(error(
                        crate::error::catalog::ER_CANNOT_USER,
                        format!("Operation DROP USER failed for {}", key.display()),
                    ));
                }
            }
            check_definer_account_change(&authority, session, &references, &key, "DROP USER")?;
            auth.accounts.remove(&account_id(&key));
            auth.grants.retain(|g| {
                g.grantee != key
                    && !matches!(&g.scope, PrivilegeScope::Proxy { user, host }
                        if user == &key.user && host.eq_ignore_ascii_case(&key.host))
            });
            auth.role_edges
                .retain(|e| e.from_role != key && e.to_account != key);
            auth.default_roles
                .retain(|e| e.from_role != key && e.to_account != key);
            auth.restrictions.retain(|r| r.grantee != key);
        }
        Ok(())
    })?;
    for key in &account_keys {
        crate::engine::auth::failed_login_clear(key);
        crate::engine::auth::clear_user_resources(key);
        crate::engine::auth::auth_cache_forget(key);
    }
    Ok(QueryResponse::Ok)
}

fn execute_grant(
    db: &Arc<Database>,
    session: &SessionContext,
    stmt: crate::parser::Grant,
) -> Result<QueryResponse, QueryError> {
    let mut stmt = stmt;
    for target in &mut stmt.targets {
        if target.user == "CURRENT_USER()" && target.host.is_empty() {
            target.user = session.authenticated_as.user.clone();
            target.host = session.authenticated_as.host.clone();
        }
    }

    if let GrantKind::Privileges { privileges, scope } = &stmt.kind {
        let resolved = grant_scope_to_privilege_scope(session, scope);
        if let PrivilegeScope::Routine {
            database,
            routine,
            kind,
        } = &resolved
        {
            let routine_kind = if kind.eq_ignore_ascii_case("function") {
                crate::storage::RoutineKindSchema::Function
            } else {
                crate::storage::RoutineKindSchema::Procedure
            };
            if db
                .routine_definition(database, routine_kind, routine)
                .is_none()
            {
                return Err(error(
                    crate::error::catalog::ER_SP_DOES_NOT_EXIST,
                    format!(
                        "{} {database}.{routine} does not exist",
                        kind.to_ascii_uppercase()
                    ),
                ));
            }
        }
        for spec in privileges.iter().filter(|spec| !spec.columns.is_empty()) {
            let PrivilegeScope::Table { database, table } = &resolved else {
                return Err(error(
                    crate::error::catalog::ER_ILLEGAL_GRANT_FOR_TABLE,
                    "Column privileges require a table scope",
                ));
            };
            let target_columns: Vec<String> = match db.table(&format!("{database}.{table}")) {
                Ok(target) => target
                    .iter_visible_columns()
                    .map(|(_, c)| c.name.clone())
                    .collect(),
                Err(err) => match db.view_definition(database, table) {
                    Some(view) => view.columns.iter().map(|c| c.name.clone()).collect(),
                    None => return Err(err),
                },
            };
            for column in &spec.columns {
                if !target_columns
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(column))
                {
                    return Err(error(
                        crate::error::catalog::ER_BAD_FIELD_ERROR,
                        format!("Unknown column '{column}' in '{database}.{table}'"),
                    ));
                }
            }
        }
    }
    let grant_as = stmt.grant_as.clone();
    db.mutate_auth_catalog(|auth| {
        let (grantor, grant_context) = resolve_grant_context(auth, session, grant_as.as_ref())?;
        let actor_is_system = session.internal_system_session
            || auth.account_has_privilege(&session.authorization_context(), &RequiredPrivilege { privilege:Privilege::Dynamic("SYSTEM_USER".into()), scope:PrivilegeScope::Global }, false);
        let target_keys: Vec<_> = stmt.targets.iter().map(account_key).collect();
        ensure_system_account_access(auth,session,target_keys.iter())?;
        match stmt.kind {
            GrantKind::Privileges { privileges, scope } => {
                let scope = grant_scope_to_privilege_scope(session, &scope);
                // All authority is evaluated against the pre-statement snapshot.
                let authority = auth.clone();
                let mut grant_positions: std::collections::HashMap<AccountKey, Vec<usize>> = stmt.targets.iter().map(|target| {
                    let key = account_key(target);
                    let positions = authority.grant_positions_for(&key);
                    (key, positions)
                }).collect();
                for target in stmt.targets {
                    let grantee = account_key(&target);
                    if !auth.accounts.contains_key(&account_id(&grantee)) {
                        return Err(error(crate::error::catalog::ER_CANT_CREATE_USER_WITH_GRANT,
                            "You are not allowed to create a user with GRANT",
                        ));
                    }
                    if authority.account_is_system_user(&grantee) && !actor_is_system {
                        return Err(system_user_access_denied());
                    }
                    for spec in &privileges {
                        if spec.name.eq_ignore_ascii_case("USAGE") && !stmt.grant_option {
                            if !authority.account_has_privilege(&grant_context,&RequiredPrivilege{privilege:Privilege::GrantOption,scope:scope.clone()},false) {
                                return Err(crate::engine::auth::access_denied(&grant_context.authenticated_as.user,&grant_context.authenticated_as.host));
                            }
                            continue;
                        }
                        for (privilege, grant_scope) in grant_entries_for_spec(spec, &scope) {
                            validate_privilege_scope(&privilege, &grant_scope)?;
                            let mut inherited_restrictions = if matches!(grant_scope, PrivilegeScope::Global) {
                                authority.delegated_restrictions(&grant_context, &privilege)
                            } else { BTreeSet::new() };
                            if matches!(grant_scope,PrivilegeScope::Global) && authority.grants_for(&grantee).any(|g| g.privilege==privilege && matches!(g.scope,PrivilegeScope::Global)) {
                                let existing=authority.global_restrictions(&BTreeSet::from([grantee.clone()]),&privilege);
                                inherited_restrictions=inherited_restrictions.intersection(&existing).cloned().collect();
                            }
                            if matches!(&privilege, Privilege::Dynamic(name) if name == "SYSTEM_USER")
                                && !actor_is_system
                            {
                                return Err(system_user_access_denied());
                            }
                            if matches!(&privilege, Privilege::Dynamic(name) if name == "SYSTEM_USER")
                                && auth.mandatory_role_keys().contains(&grantee)
                            {
                                return Err(error(
                                    crate::error::catalog::ER_CANNOT_GRANT_SYSTEM_PRIV_TO_MANDATORY_ROLE,
                                    format!(
                                        "Cannot grant SYSTEM_USER to mandatory role {}",
                                        grantee.display()
                                    ),
                                ));
                            }
                            let required = crate::engine::auth::RequiredPrivilege {
                                privilege: privilege.clone(),
                                scope: grant_scope.clone(),
                            };
                            if !authority.account_has_privilege(
                                &grant_context,
                                &required,
                                true,
                            ) {
                                return Err(privilege_denied(&required.privilege, &required.scope));
                            }
                            let positions = grant_positions.entry(grantee.clone()).or_default();
                            let static_scope_grant_option = !matches!(privilege, Privilege::Dynamic(_))
                                && positions.iter().any(|idx| {
                                    let grant = &auth.grants[*idx];
                                    scope_equal(&grant.scope,&grant_scope)
                                        && !matches!(grant.privilege, Privilege::Dynamic(_))
                                        && grant.grant_option
                                });
                            if (stmt.grant_option || matches!(privilege, Privilege::GrantOption)) && !matches!(privilege, Privilege::Dynamic(_)) {
                                for idx in positions.iter() {
                                    let grant = &mut auth.grants[*idx];
                                    if scope_equal(&grant.scope,&grant_scope) && !matches!(grant.privilege, Privilege::Dynamic(_)) {
                                        grant.grant_option = true;
                                    }
                                }
                            }
                            let effective_grant_option = stmt.grant_option || static_scope_grant_option || matches!(privilege, Privilege::GrantOption);
                            if let Some(idx) = positions.iter().copied().find(|idx| {
                                let grant = &auth.grants[*idx];
                                grant.privilege == privilege && scope_equal(&grant.scope,&grant_scope)
                            }) {
                                let existing = &mut auth.grants[idx];
                                existing.grant_option |= effective_grant_option;
                                existing.grantor = grantor.clone();
                                existing.updated_ms = crate::storage::naming::current_unix_ms();
                            } else {
                                positions.push(auth.grants.len());
                                auth.grants.push(GrantEntry {
                                    updated_ms: crate::storage::naming::current_unix_ms(),
                                    grantor: grantor.clone(),
                                    grantee: grantee.clone(),
                                    privilege: privilege.clone(),
                                    scope: grant_scope.clone(),
                                    grant_option: effective_grant_option,
                                    origin: GrantOrigin::Explicit,
                                });
                            }
                            if (stmt.grant_option || matches!(privilege,Privilege::GrantOption)) && !matches!(privilege,Privilege::Dynamic(_)) {
                                let go_scope=grant_option_level(&grant_scope);
                                if !positions.iter().any(|i| auth.grants[*i].privilege==Privilege::GrantOption && scope_equal(&auth.grants[*i].scope,&go_scope)) {
                                    positions.push(auth.grants.len());
                                    auth.grants.push(GrantEntry { updated_ms: crate::storage::naming::current_unix_ms(), grantor:grantor.clone(),grantee:grantee.clone(),privilege:Privilege::GrantOption,scope:go_scope,grant_option:true,origin:GrantOrigin::Explicit});
                                }
                            }
                            // L9: GRANT priv ON db.* removes any matching
                            // partial-revoke RestrictionEntry — explicit
                            // schema-level grant overrides the deny list.
                            if let PrivilegeScope::Database { database } = &grant_scope {
                                auth.restrictions.retain(|r| {
                                    !(r.grantee == grantee
                                        && r.privilege == privilege
                                        && r.database.eq_ignore_ascii_case(database))
                                });
                            }
                            if matches!(grant_scope, PrivilegeScope::Global) {
                                auth.restrictions.retain(|restriction| {
                                    !(restriction.grantee == grantee
                                        && restriction.privilege == privilege)
                                });
                                for database in &inherited_restrictions {
                                    auth.restrictions.push(
                                        crate::engine::auth::RestrictionEntry {
                                            grantee: grantee.clone(),
                                            privilege: privilege.clone(),
                                            database: database.clone(),
                                        },
                                    );
                                }
                            }
                        }
                    }
                }
            }
            GrantKind::Roles(roles) => {
                let mandatory=auth.mandatory_role_keys();
                let seeds:BTreeSet<_>=roles.iter().map(account_key).collect();
                let closure=auth.role_closure(&seeds);
                let authority=auth.clone();
                for target in stmt.targets {
                    let to_account = account_key(&target);
                    if to_account.user.is_empty() {
                        return Err(error(
                            crate::error::catalog::ER_CANNOT_GRANT_ROLES_TO_ANONYMOUS_USER,
                            "Cannot grant roles to an anonymous user",
                        ));
                    }
                    if !auth.accounts.contains_key(&account_id(&to_account)) {
                        return Err(error(crate::error::catalog::ER_CANT_CREATE_USER_WITH_GRANT,
                            "You are not allowed to create a user with GRANT",
                        ));
                    }
                    ensure_system_account_access(&authority,session,std::iter::once(&to_account))?;
                    if mandatory.contains(&to_account) || closure.contains(&to_account) {
                        return Err(error(crate::error::catalog::ER_ROLE_GRANTED_TO_ITSELF,format!("Granting roles to {} would create a role cycle",to_account.display())));
                    }
                    for role in &roles {
                        let role = account_key(role);
                        if !auth
                            .accounts
                            .get(&account_id(&role))
                            .is_some()
                        {
                            return Err(error(crate::error::catalog::ER_ROLE_NOT_GRANTED,
                                format!("Role {} does not exist", role.display()),
                            ));
                        }
                        if auth.account_is_system_user(&role) && !actor_is_system {
                            return Err(system_user_access_denied());
                        }
                        // B1: granting a role requires ROLE_ADMIN or holding the
                        // role WITH ADMIN OPTION. Bootstrap/system sessions are
                        // exempt (they run without a real grantee).
                        if !session.internal_system_session
                            && !authority.account_can_administer_role(
                                &session.authorization_context(),
                                &role,
                            )
                        {
                            return Err(error(crate::error::catalog::ER_UNKNOWN_AUTHID,
                                format!(
                                    "Access denied; you need (at least one of) the ROLE_ADMIN privilege(s) for this operation on role {}",
                                    role.display()
                                ),
                            ));
                        }
                        let position = auth.role_edges.iter().position(|edge| {
                            edge.from_role == role && edge.to_account == to_account
                        });
                        if let Some(i) = position {
                            auth.role_edges[i].admin_option |= stmt.grant_option;
                        } else {
                            auth.role_edges.push(RoleEdge {
                                from_role: role,
                                to_account: to_account.clone(),
                                admin_option: stmt.grant_option,
                            });
                        }
                    }
                }
            }
            GrantKind::Proxy(_) => {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "GRANT PROXY is not supported: no proxy authentication plugin is available",
                ));
            }
        }
        Ok(())
    })?;
    Ok(QueryResponse::Ok)
}

fn resolve_grant_context(
    auth: &crate::engine::auth::AuthCatalog,
    session: &SessionContext,
    grant_as: Option<&crate::parser::GrantAs>,
) -> Result<(AccountKey, crate::engine::auth::AuthorizationContext), QueryError> {
    let Some(grant_as) = grant_as else {
        let context = session.authorization_context();
        return Ok((context.authenticated_as.clone(), context));
    };
    let account = account_key(&grant_as.account);
    if !auth.accounts.contains_key(&account_id(&account)) {
        return Err(error(
            crate::error::catalog::ER_UKNOWN_AUTH_ID_OR_ACCESS_DENIED_FOR_GRANT_AS,
            format!(
                "Unknown authorization ID {} for GRANT AS",
                account.display()
            ),
        ));
    }
    if account != session.authenticated_as && !session.internal_system_session {
        let caller = session.authorization_context();
        let may_set_user = auth.account_has_privilege(
            &caller,
            &RequiredPrivilege {
                privilege: Privilege::Dynamic("SET_USER_ID".to_string()),
                scope: PrivilegeScope::Global,
            },
            false,
        );
        let caller_is_system = auth.account_has_privilege(
            &session.authorization_context(),
            &RequiredPrivilege {
                privilege: Privilege::Dynamic("SYSTEM_USER".into()),
                scope: PrivilegeScope::Global,
            },
            false,
        );
        if !may_set_user || (auth.account_is_system_user(&account) && !caller_is_system) {
            return Err(error(
                crate::error::catalog::ER_UKNOWN_AUTH_ID_OR_ACCESS_DENIED_FOR_GRANT_AS,
                format!("Access denied for GRANT AS {}", account.display()),
            ));
        }
    }

    let applicable = auth.applicable_roles(&account);
    let active_roles = match grant_as.roles.as_ref() {
        None | Some(SetRole::None) => BTreeSet::new(),
        Some(SetRole::Default) => auth.active_default_roles(&account),
        Some(SetRole::All) => applicable.clone(),
        Some(SetRole::AllExcept(excluded)) => {
            let excluded = excluded.iter().map(account_key).collect::<BTreeSet<_>>();
            applicable.difference(&excluded).cloned().collect()
        }
        Some(SetRole::Roles(roles)) => {
            let selected = roles.iter().map(account_key).collect::<BTreeSet<_>>();
            if let Some(role) = selected.iter().find(|role| !applicable.contains(*role)) {
                return Err(error(
                    crate::error::catalog::ER_ROLE_NOT_GRANTED,
                    format!(
                        "Role {} has not been granted to {}",
                        role.display(),
                        account.display()
                    ),
                ));
            }
            selected
        }
    };
    Ok((
        account.clone(),
        crate::engine::auth::AuthorizationContext {
            authenticated_as: account,
            active_roles: active_roles.into(),
            epoch: auth.epoch,
        },
    ))
}

fn grant_entries_for_spec(
    spec: &PrivilegeSpec,
    scope: &PrivilegeScope,
) -> Vec<(Privilege, PrivilegeScope)> {
    // `ALL [PRIVILEGES]` expands at GRANT time into the canonical static
    // bundle for the scope (see `auth::all_privileges_for_scope`). Storing
    // each static priv as its own row lets `privilege_implies` stay simple
    // (no ALL wildcard) and lets `SHOW GRANTS` decide *display-time* whether
    // to fold the rows back into "ALL PRIVILEGES".
    let is_all =
        spec.name.eq_ignore_ascii_case("ALL") || spec.name.eq_ignore_ascii_case("ALL PRIVILEGES");

    let target_scopes: Vec<PrivilegeScope> = if spec.columns.is_empty() {
        vec![scope.clone()]
    } else if let PrivilegeScope::Table { database, table } = scope {
        spec.columns
            .iter()
            .map(|column| PrivilegeScope::Column {
                database: database.clone(),
                table: table.clone(),
                column: column.to_ascii_lowercase(),
            })
            .collect()
    } else {
        vec![scope.clone()]
    };

    let mut out = Vec::new();
    for scope in target_scopes {
        if is_all {
            for privilege in crate::engine::auth::all_privileges_for_scope(&scope) {
                out.push((privilege.clone(), scope.clone()));
            }
            if matches!(scope, PrivilegeScope::Global) {
                out.extend(
                    crate::engine::auth::default_dynamic_privileges()
                        .iter()
                        .cloned()
                        .map(|privilege| (privilege, scope.clone())),
                );
            }
        } else {
            out.push((
                if spec.name.eq_ignore_ascii_case("USAGE") {
                    Privilege::GrantOption
                } else {
                    Privilege::parse(&spec.name)
                },
                scope,
            ));
        }
    }
    out
}

fn validate_privilege_scope(
    privilege: &Privilege,
    scope: &PrivilegeScope,
) -> Result<(), QueryError> {
    if matches!(privilege, Privilege::GrantOption)
        && !matches!(
            scope,
            PrivilegeScope::Column { .. } | PrivilegeScope::Proxy { .. }
        )
    {
        return Ok(());
    }
    if let Privilege::Dynamic(name) = privilege {
        if !crate::engine::auth::is_registered_dynamic_privilege(name) {
            return Err(error(
                crate::error::catalog::ER_SYNTAX_ERROR,
                format!("Unknown dynamic privilege '{name}'"),
            ));
        }
        if !matches!(scope, PrivilegeScope::Global) {
            return Err(error(
                crate::error::catalog::ER_ILLEGAL_PRIVILEGE_LEVEL,
                format!("Illegal privilege level specified for dynamic privilege {name}"),
            ));
        }
        return Ok(());
    }
    if crate::engine::auth::all_privileges_for_scope(scope).contains(privilege) {
        return Ok(());
    }
    Err(error(
        crate::error::catalog::ER_ILLEGAL_PRIVILEGE_LEVEL,
        format!(
            "Illegal privilege level specified for {}",
            privilege.sql_name()
        ),
    ))
}

fn execute_revoke(
    db: &Arc<Database>,
    session: &SessionContext,
    stmt: crate::parser::Revoke,
) -> Result<QueryResponse, QueryError> {
    let mut stmt = stmt;
    for target in &mut stmt.targets {
        if target.user == "CURRENT_USER()" && target.host.is_empty() {
            target.user = session.authenticated_as.user.clone();
            target.host = session.authenticated_as.host.clone();
        }
    }

    let mut valid_targets = BTreeSet::new();
    db.with_auth_catalog(|auth| {
        for target in &stmt.targets {
            let target = account_key(target);
            if auth.accounts.contains_key(&account_id(&target)) {
                valid_targets.insert(target);
            } else if stmt.if_exists || stmt.ignore_unknown_user {
                crate::engine::api::push_statement_warning(
                    crate::error::ConditionLevel::Note,
                    crate::error::catalog::ER_USER_DOES_NOT_EXIST,
                    format!("Authorization ID {} does not exist.", target.display()),
                );
            } else {
                return Err(error(
                    crate::error::catalog::ER_NONEXISTING_GRANT,
                    format!(
                        "There is no such grant defined for user {}",
                        target.display()
                    ),
                ));
            }
        }
        Ok(())
    })??;
    if valid_targets.is_empty() {
        return Ok(QueryResponse::Ok);
    }
    // P0-5: `REVOKE ALL PRIVILEGES [, GRANT OPTION] FROM <user>` (no ON scope)
    // must purge every grant + restriction row for the target accounts.
    // Requires the same authority MySQL requires: CREATE USER, or held with
    // GRANT OPTION on the same scopes the target actually has.  We follow the
    // simpler rule already used elsewhere: hold `CreateUser` globally to
    // strip an account wholesale.
    if stmt.revoke_all_from_target {
        let ctx = session.authorization_context();
        let has_create_user = db.with_auth_catalog(|auth| {
            auth.account_has_privilege(
                &ctx,
                &crate::engine::auth::RequiredPrivilege {
                    privilege: Privilege::CreateUser,
                    scope: PrivilegeScope::Global,
                },
                false,
            )
        })?;
        if !session.internal_system_session && !has_create_user {
            return Err(privilege_denied(
                &Privilege::CreateUser,
                &PrivilegeScope::Global,
            ));
        }
        db.mutate_auth_catalog(|auth| {
            let targets = valid_targets.clone();
            ensure_system_account_access(auth, session, targets.iter())?;
            auth.grants.retain(|g| !targets.contains(&g.grantee));
            auth.restrictions.retain(|r| !targets.contains(&r.grantee));
            Ok(())
        })?;
        return Ok(QueryResponse::Ok);
    }
    db.mutate_auth_catalog(|auth| {
        let actor_is_system = session.internal_system_session
            || auth.account_has_privilege(&session.authorization_context(), &RequiredPrivilege { privilege:Privilege::Dynamic("SYSTEM_USER".into()), scope:PrivilegeScope::Global }, false);
        match stmt.kind {
            GrantKind::Privileges { privileges, scope } => {
                let scope = grant_scope_to_privilege_scope(session, &scope);
                if privileges.iter().all(|p|p.name.eq_ignore_ascii_case("USAGE")) {
                    if !auth.account_has_privilege(&session.authorization_context(),&RequiredPrivilege{privilege:Privilege::GrantOption,scope:scope.clone()},false) {
                        return Err(crate::engine::auth::access_denied(&session.user,&session.host));
                    }
                    return Ok(());
                }
                let revoke_all = privileges.iter().any(|p| p.name.eq_ignore_ascii_case("ALL") || p.name.eq_ignore_ascii_case("ALL PRIVILEGES"));
                let privileges: Vec<_> = privileges
                    .iter()
                    .filter(|p|!p.name.eq_ignore_ascii_case("USAGE"))
                    .flat_map(|spec| grant_entries_for_spec(spec, &scope))
                    .collect();
                for (privilege, scope) in &privileges {
                    validate_privilege_scope(privilege, scope)?;
                }
                let targets = valid_targets.clone();
                if !actor_is_system
                    && targets
                        .iter()
                        .any(|target| auth.account_is_system_user(target))
                {
                    return Err(system_user_access_denied());
                }
                for (privilege, scope) in &privileges {
                    let required = crate::engine::auth::RequiredPrivilege {
                        privilege: privilege.clone(),
                        scope: scope.clone(),
                    };
                    if !auth.account_has_privilege(
                        &session.authorization_context(),
                        &required,
                        true,
                    ) {
                        return Err(privilege_denied(&required.privilege, &required.scope));
                    }
                }
                // L9: in partial_revokes=ON mode, a REVOKE at schema scope
                // against an account that holds the privilege globally
                // writes a `RestrictionEntry` so the global grant remains
                // intact but the targeted schema is denied. The exact-
                // schema-grant row (if any) is still removed below by the
                // normal retain pass — and the restriction is what keeps
                // the schema denied via the global fallback.
                let partial = crate::engine::auth::partial_revokes_enabled();
                for grantee in &targets {
                    for (privilege, scope) in &privileges {
                        if matches!(scope, PrivilegeScope::Global) || revoke_all {
                            continue;
                        }
                        let has_grant = auth.grants.iter().any(|g| {
                            if &g.grantee != grantee { return false; }
                            if matches!(privilege, Privilege::GrantOption) || (stmt.grant_option_only && !matches!(privilege, Privilege::Dynamic(_))) {
                                !matches!(g.privilege, Privilege::Dynamic(_))
                                    && (g.grant_option || matches!(g.privilege, Privilege::GrantOption))
                                    && crate::engine::auth::scope_equal(&crate::engine::auth::grant_option_level(&g.scope), &crate::engine::auth::grant_option_level(scope))
                            } else {
                                &g.privilege == privilege && crate::engine::auth::scope_equal(&g.scope, scope)
                            }
                        });
                        let may_restrict = partial && !stmt.grant_option_only && matches!(scope, PrivilegeScope::Database { .. })
                            && auth.grants.iter().any(|g| &g.grantee == grantee && &g.privilege == privilege && matches!(g.scope, PrivilegeScope::Global));
                        if !has_grant && !may_restrict {
                            return Err(error(crate::error::catalog::ER_NONEXISTING_GRANT, format!("There is no such grant defined for user {}", grantee.display())));
                        }
                    }
                }
                if partial {
                    for grantee in &targets {
                        for (privilege, scope) in &privileges {
                            let PrivilegeScope::Database { database } = scope else {
                                continue;
                            };
                            let has_global = auth.grants.iter().any(|g| {
                                &g.grantee == grantee
                                    && &g.privilege == privilege
                                    && matches!(g.scope, PrivilegeScope::Global)
                            });
                            let has_schema = auth.grants.iter().any(|g| &g.grantee == grantee && &g.privilege == privilege && crate::engine::auth::scope_equal(&g.scope, scope));
                            if has_global && !has_schema && !stmt.grant_option_only {
                                let already_restricted = auth.restrictions.iter().any(|r| {
                                    &r.grantee == grantee
                                        && &r.privilege == privilege
                                        && r.database.eq_ignore_ascii_case(database)
                                });
                                if !already_restricted {
                                    auth.restrictions.push(
                                        crate::engine::auth::RestrictionEntry {
                                            grantee: grantee.clone(),
                                            privilege: privilege.clone(),
                                            database: database.clone(),
                                        },
                                    );
                                }
                            }
                        }
                    }
                }
                let mut positions: Vec<_> = targets.iter().flat_map(|target| auth.grant_positions_for(target)).collect();
                positions.sort_unstable();
                positions.dedup();
                for i in positions.into_iter().rev() {
                    let grant=&auth.grants[i];
                    let revoke_scope_go=privileges.iter().any(|(p,scope)| (matches!(p,Privilege::GrantOption) || (stmt.grant_option_only && !matches!(p,Privilege::Dynamic(_))))
                        && !matches!(grant.privilege,Privilege::Dynamic(_))
                        && crate::engine::auth::scope_equal(&crate::engine::auth::grant_option_level(scope), &crate::engine::auth::grant_option_level(&grant.scope)));
                    if revoke_scope_go {
                        if matches!(grant.privilege,Privilege::GrantOption) { auth.grants.swap_remove(i); }
                        else { auth.grants[i].grant_option=false; }
                    } else if privileges.iter().any(|(p,scope)|p==&grant.privilege&&crate::engine::auth::scope_equal(scope,&grant.scope)) {
                        if stmt.grant_option_only { auth.grants[i].grant_option=false; }
                        else { auth.grants.swap_remove(i); }
                    }
                }
                if !stmt.grant_option_only {
                    // If partial revoke was NOT engaged for a particular
                    // (grantee, priv, db), we may also need to drop a matching
                    // restriction entry the user inserted earlier so the
                    // privilege state remains coherent.
                    auth.restrictions.retain(|r| {
                            !(targets.contains(&r.grantee)
                                && privileges.iter().any(|(privilege, scope)| {
                                    privilege == &r.privilege
                                        && (matches!(scope, PrivilegeScope::Global) || (!partial && matches!(scope, PrivilegeScope::Database { database } if database.eq_ignore_ascii_case(&r.database))))
                                }))
                    });
                }
            }
            GrantKind::Roles(roles) => {
                let roles: std::collections::BTreeSet<_> = roles.iter().map(account_key).collect();
                let targets = valid_targets.clone();
                if !actor_is_system
                    && targets
                        .iter()
                        .any(|target| auth.account_is_system_user(target))
                {
                    return Err(system_user_access_denied());
                }
                let mandatory = auth.mandatory_role_keys();
                if let Some(role) = roles.iter().find(|role| mandatory.contains(*role)) {
                    return Err(error(
                        crate::error::catalog::ER_MANDATORY_ROLE,
                        format!(
                            "Role {} is a mandatory role and cannot be revoked",
                            role.display()
                        ),
                    ));
                }
                // B1: revoking a role requires the same authority as granting
                // it — ROLE_ADMIN or the role WITH ADMIN OPTION. Otherwise any
                // user could strip roles from arbitrary accounts.
                if !session.internal_system_session {
                    let ctx = session.authorization_context();
                    for role in &roles {
                        if !auth.account_can_administer_role(&ctx, role) {
                            return Err(error(crate::error::catalog::ER_UNKNOWN_AUTHID,
                                format!(
                                    "Access denied; you need (at least one of) the ROLE_ADMIN privilege(s) for this operation on role {}",
                                    role.display()
                                ),
                            ));
                        }
                    }
                }
                for role in &roles {
                    for target in &targets {
                        if !auth.role_edges.iter().any(|edge| &edge.from_role == role && &edge.to_account == target) {
                            return Err(error(crate::error::catalog::ER_ROLE_NOT_GRANTED, format!("Role {} has not been granted to {}", role.display(), target.display())));
                        }
                    }
                }
                auth.role_edges
                    .retain(|e| !(roles.contains(&e.from_role) && targets.contains(&e.to_account)));
                auth.default_roles
                    .retain(|e| !(roles.contains(&e.from_role) && targets.contains(&e.to_account)));
            }
            GrantKind::Proxy(proxied) => {
                let proxied = account_key(&proxied);
                let actor_can_revoke = session.internal_system_session
                    || auth.account_has_proxy(&session.authorization_context(), &proxied, true);
                if !actor_can_revoke {
                    return Err(privilege_denied(
                        &Privilege::Dynamic("PROXY".to_string()),
                        &PrivilegeScope::Proxy {
                            user: proxied.user.clone(),
                            host: proxied.host.clone(),
                        },
                    ));
                }
                let targets = valid_targets.clone();
                auth.grants.retain(|grant| {
                    !(targets.contains(&grant.grantee)
                        && matches!(&grant.privilege, Privilege::Dynamic(name) if name == "PROXY")
                        && matches!(&grant.scope, PrivilegeScope::Proxy { user, host }
                            if user == &proxied.user && host.eq_ignore_ascii_case(&proxied.host)))
                });
            }
        }
        Ok(())
    })?;
    Ok(QueryResponse::Ok)
}

fn system_user_access_denied() -> QueryError {
    error(
        crate::error::catalog::ER_SPECIFIC_ACCESS_DENIED_ERROR,
        "Access denied; you need (at least one of) the SYSTEM_USER privilege(s) for this operation",
    )
}

fn validate_account_key(account: &AccountKey) -> Result<(), QueryError> {
    if account.user.chars().count() > 32 {
        return Err(error(
            crate::error::catalog::ER_WRONG_STRING_LENGTH,
            "String 'user name' is too long (should be no longer than 32)",
        ));
    }
    if account.host.chars().count() > 255 {
        return Err(error(
            crate::error::catalog::ER_WRONG_STRING_LENGTH,
            "String 'host name' is too long (should be no longer than 255)",
        ));
    }
    Ok(())
}

/// MySQL password administrators need not prove knowledge of the old password.
fn can_administer_passwords(
    auth: &crate::engine::auth::AuthCatalog,
    session: &SessionContext,
) -> bool {
    session.internal_system_session
        || [
            RequiredPrivilege {
                privilege: Privilege::CreateUser,
                scope: PrivilegeScope::Global,
            },
            RequiredPrivilege {
                privilege: Privilege::Update,
                scope: PrivilegeScope::Database {
                    database: "mysql".into(),
                },
            },
        ]
        .iter()
        .any(|required| {
            auth.account_has_privilege(&session.authorization_context(), required, false)
        })
}

pub(super) fn kill_session(
    db: &Database,
    registry: &SessionRegistry,
    session: &SessionContext,
    connection_id: u32,
    query_only: bool,
) -> Result<(), QueryError> {
    // Read grants before taking the registry/entry locks. Keep the checked
    // identity and target handle locked until cancellation is signalled.
    let auth = db.auth_catalog();
    registry.kill_authorized(connection_id, query_only, |target| {
        if target != &session.authenticated_as && !session.internal_system_session {
            let required = RequiredPrivilege {
                privilege: Privilege::Dynamic("CONNECTION_ADMIN".into()),
                scope: PrivilegeScope::Global,
            };
            if !auth.account_has_privilege(&session.authorization_context(), &required, false) {
                return Err(error(
                    crate::error::catalog::ER_KILL_DENIED_ERROR,
                    format!("You are not owner of thread {connection_id}"),
                ));
            }
            ensure_system_account_access(&auth, session, std::iter::once(target))?;
        }
        Ok(())
    })?;
    db.notify_lock_waiters();
    Ok(())
}

fn ensure_system_account_access<'a>(
    auth: &crate::engine::auth::AuthCatalog,
    session: &SessionContext,
    targets: impl IntoIterator<Item = &'a AccountKey>,
) -> Result<(), QueryError> {
    if session.internal_system_session
        || auth.account_has_privilege(
            &session.authorization_context(),
            &RequiredPrivilege {
                privilege: Privilege::Dynamic("SYSTEM_USER".into()),
                scope: PrivilegeScope::Global,
            },
            false,
        )
    {
        return Ok(());
    }
    if targets
        .into_iter()
        .any(|target| auth.account_is_system_user(target))
    {
        Err(system_user_access_denied())
    } else {
        Ok(())
    }
}

fn execute_set_role(
    db: &Arc<Database>,
    session: &mut SessionContext,
    stmt: SetRole,
) -> Result<QueryResponse, QueryError> {
    let auth = db.auth_catalog();
    // The set of roles this session may activate: everything granted directly
    // to the account, plus the mandatory roles (implicitly granted to every
    // account). SET ROLE may only activate roles in this set — otherwise it
    // would let a session escalate privilege by naming any existing role
    // (L17 high-severity bug fix).
    let granted_roles: std::collections::BTreeSet<crate::engine::auth::AccountKey> =
        auth.applicable_roles(&session.authenticated_as);
    let active_roles: BTreeSet<AccountKey> = match stmt {
        SetRole::All => granted_roles,
        SetRole::AllExcept(excludes) => {
            // ALL EXCEPT: the exception list is purely subtractive — it
            // is NOT an error to name a role you weren't granted (MySQL
            // 8.0 silently drops unmatched names from the EXCEPT set).
            let exclude: std::collections::BTreeSet<crate::engine::auth::AccountKey> =
                excludes.iter().map(account_key).collect();
            granted_roles.difference(&exclude).cloned().collect()
        }
        SetRole::None => Default::default(),
        SetRole::Default => auth.active_default_roles(&session.authenticated_as),
        SetRole::Roles(roles) => {
            let mut activate = std::collections::BTreeSet::new();
            for role in roles {
                let key = account_key(&role);
                if !granted_roles.contains(&key) {
                    // MySQL 8.0: ER_ROLE_NOT_GRANTED (3530).
                    return Err(error(
                        crate::error::catalog::ER_ROLE_NOT_GRANTED,
                        format!(
                            "Role {} has not been granted to {}",
                            key.display(),
                            session.authenticated_as.display()
                        ),
                    ));
                }
                activate.insert(key);
            }
            activate
        }
    };
    session.active_roles = active_roles.into();
    Ok(QueryResponse::Ok)
}

fn execute_set_default_role(
    db: &Arc<Database>,
    session: &SessionContext,
    stmt: SetDefaultRole,
) -> Result<QueryResponse, QueryError> {
    db.mutate_auth_catalog(|auth| {
        let targets = match &stmt {
            SetDefaultRole::All { to }
            | SetDefaultRole::None { to }
            | SetDefaultRole::Roles { to, .. } => to,
        };
        let keys: Vec<_> = targets.iter().map(account_key).collect();
        ensure_system_account_access(auth, session, keys.iter())?;
        if !matches!(&stmt, SetDefaultRole::None { .. }) {
            for key in &keys {
                if !auth.accounts.contains_key(&account_id(key)) {
                    return Err(error(
                        crate::error::catalog::ER_UNKNOWN_AUTHID,
                        format!("Unknown authorization ID {}", key.display()),
                    ));
                }
            }
        }
        match stmt {
            SetDefaultRole::All { to } => {
                for target in to {
                    let to_account = account_key(&target);
                    auth.default_roles.retain(|e| e.to_account != to_account);
                    // Snapshot first to release the immutable borrow before
                    // we push into auth.default_roles.
                    let new_edges: Vec<_> = auth
                        .role_edges
                        .iter()
                        .filter(|e| e.to_account == to_account)
                        .cloned()
                        .collect();
                    for edge in new_edges {
                        auth.default_roles.push(edge);
                    }
                }
            }
            SetDefaultRole::None { to } => {
                let targets: std::collections::BTreeSet<_> = to.iter().map(account_key).collect();
                auth.default_roles
                    .retain(|e| !targets.contains(&e.to_account));
            }
            SetDefaultRole::Roles { roles, to } => {
                let roles: Vec<_> = roles.iter().map(account_key).collect();
                for target in to {
                    let to_account = account_key(&target);
                    // L19: validate that every role being set as default
                    // is actually granted to the target account. Without
                    // this check, an admin could set an unrelated role as
                    // default and the user would silently activate it
                    // (with the role's privileges) on every login.
                    let granted = auth.applicable_roles(&to_account);
                    for role in &roles {
                        if !granted.contains(role) {
                            return Err(error(
                                crate::error::catalog::ER_ROLE_NOT_GRANTED,
                                format!(
                                    "Role {} has not been granted to {}",
                                    role.display(),
                                    to_account.display()
                                ),
                            ));
                        }
                    }
                    auth.default_roles.retain(|e| e.to_account != to_account);
                    for role in &roles {
                        auth.default_roles.push(RoleEdge {
                            from_role: role.clone(),
                            to_account: to_account.clone(),
                            admin_option: false,
                        });
                    }
                }
            }
        }
        Ok(())
    })?;
    Ok(QueryResponse::Ok)
}

fn execute_xa_statement(
    db: &Arc<Database>,
    session: &mut SessionContext,
    statement: Statement,
) -> Result<QueryResponse, QueryError> {
    match statement {
        Statement::XaStart { xid } => xa_start(db, session, xid).map(|_| QueryResponse::Ok),
        Statement::XaEnd { xid } => xa_end(db, session, xid).map(|_| QueryResponse::Ok),
        Statement::XaPrepare { xid } => xa_prepare(db, session, xid).map(|_| QueryResponse::Ok),
        Statement::XaCommit { xid, one_phase } => {
            xa_commit(db, session, xid, one_phase).map(|_| QueryResponse::Ok)
        }
        Statement::XaRollback { xid } => xa_rollback(db, session, xid).map(|_| QueryResponse::Ok),
        Statement::XaRecover { convert_xid } => Ok(xa_recover_result(db, convert_xid)),
        _ => unreachable!("non-XA statement reached execute_xa_statement"),
    }
}

/// `DECLARE <name> CURSOR FOR <select>` — open a session-level SQL
/// cursor. We execute the SELECT eagerly (so the cursor is OPEN as soon
/// as DECLARE returns, matching PostgreSQL semantics), then move the
/// resulting `ResultCursor` into the named-cursor registry. Subsequent
/// `FETCH N FROM <name>` statements pull rows out of that registry
/// entry; `CLOSE <name>` (or session drop) releases it.
fn execute_declare_sql_cursor(
    db: &Arc<Database>,
    session: &mut SessionContext,
    name: String,
    query: crate::parser::QueryExpr,
    sql: String,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    // Surface the user's original DECLARE SQL in `i_s.mydb_cursors.SQL`
    // rather than a Debug rendering of the parsed AST.
    let sql_for_view: Arc<str> = Arc::from(sql);
    let response = execute_statement(db, session, query.into_statement(), registry)?;
    let (columns, cursor): (Vec<Column>, Box<dyn crate::engine::api::ResultCursor>) = match response
    {
        QueryResponse::ResultSet(rs) => {
            let columns = rs.columns.clone();
            let cursor: Box<dyn crate::engine::api::ResultCursor> =
                Box::new(crate::engine::api::VecResultCursor::new(rs.rows));
            (columns, cursor)
        }
        QueryResponse::StreamingResultSet(s) => (s.columns, s.cursor),
        other => {
            return Err(error(
                crate::error::catalog::ER_PARSE_ERROR,
                format!("DECLARE CURSOR's SELECT did not yield a result set: {other:?}"),
            ))
        }
    };
    crate::engine::cursor_registry::named_open(
        session.connection_id,
        name,
        sql_for_view,
        columns,
        cursor,
    )?;
    Ok(QueryResponse::Ok)
}

/// `FETCH N FROM <name>` — pull up to `count` rows from a previously
/// declared SQL cursor. Returns them as a regular `ResultSet`; the
/// MySQL text protocol writer in the session layer encodes them like
/// any other SELECT result.
fn execute_fetch_sql_cursor(
    session: &mut SessionContext,
    name: String,
    count: u64,
) -> Result<QueryResponse, QueryError> {
    let result =
        crate::engine::cursor_registry::named_fetch(session.connection_id, &name, count.max(1))?;
    Ok(QueryResponse::ResultSet(crate::engine::api::ResultSet {
        columns: result.columns,
        rows: result.rows,
    }))
}

fn execute_close_sql_cursor(
    session: &mut SessionContext,
    name: String,
) -> Result<QueryResponse, QueryError> {
    crate::engine::cursor_registry::named_close(session.connection_id, &name)?;
    Ok(QueryResponse::Ok)
}

fn release_explicit_locks(db: &Arc<Database>, session: &mut SessionContext) {
    session.explicit_table_locks.clear();
    db.release_explicit_table_mdl(session.connection_id as u64);
}

fn collect_lock_table_dependencies(
    db: &Database,
    database: &str,
    name: &str,
    trail: &mut BTreeSet<String>,
    out: &mut Vec<String>,
) -> Result<(), QueryError> {
    let (schema, table) = crate::storage::split_table_name(database, name);
    let qualified = format!("{schema}.{table}");
    if trail.len() >= MAX_VIEW_DEPTH || !trail.insert(qualified.clone()) {
        return Err(error(
            crate::error::catalog::ER_VIEW_INVALID,
            "cyclic or excessively nested view in LOCK TABLES",
        ));
    }
    if let Some(view) = db.view_definition(&schema, &table) {
        if view.invalid {
            return Err(error(
                crate::error::catalog::ER_VIEW_INVALID,
                format!("View '{qualified}' references invalid objects"),
            ));
        }
        for dependency in &view.dependencies {
            collect_lock_table_dependencies(
                db,
                &dependency.database,
                &dependency.name,
                trail,
                out,
            )?;
        }
    } else {
        db.table(&qualified)?;
    }
    trail.remove(&qualified);
    out.push(qualified);
    Ok(())
}

fn execute_lock_tables(
    db: &Arc<Database>,
    session: &mut SessionContext,
    specs: Vec<LockTableSpec>,
) -> Result<QueryResponse, QueryError> {
    ensure_no_xa_branch_for_local_txn(session, "LOCK TABLES")?;
    implicit_commit_for_ddl(db, session)?;
    release_explicit_locks(db, session);

    let mut requests = Vec::new();
    let mut held = std::collections::HashMap::new();
    for spec in specs {
        if spec.low_priority_write {
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
                "'LOW_PRIORITY WRITE' is deprecated and is treated as WRITE",
            );
        }
        if session.resolve_temp_table_name(&spec.table).is_some() {
            continue;
        }
        let qualified = qualify_persistent_table_name(session, &spec.table);
        let mut dependencies = Vec::new();
        collect_lock_table_dependencies(
            db,
            &session.current_database,
            &qualified,
            &mut BTreeSet::new(),
            &mut dependencies,
        )?;
        for dependency in dependencies {
            let (schema, table) =
                crate::storage::split_table_name(&session.current_database, &dependency);
            let key = crate::storage::table_key(&schema, &table);
            requests.push(MdlRequest {
                key: MdlObjectKey::table(&schema, &table),
                lock_type: match spec.mode {
                    LockTableMode::Read => MdlLockType::SharedNoWrite,
                    LockTableMode::Write => MdlLockType::Exclusive,
                },
            });
            held.insert(
                key,
                SessionTableLock {
                    table: table.clone(),
                    alias: (dependency == qualified)
                        .then(|| spec.alias.clone())
                        .flatten(),
                    mode: spec.mode,
                },
            );
        }
    }
    db.acquire_mdl(
        session.connection_id as u64,
        Some(session.connection_id as u64),
        MdlDuration::Explicit,
        requests,
        "sql/lock_tables",
    )?;
    session.explicit_table_locks = held;
    Ok(QueryResponse::Ok)
}

fn execute_unlock_tables(
    db: &Arc<Database>,
    session: &mut SessionContext,
) -> Result<QueryResponse, QueryError> {
    ensure_no_xa_branch_for_local_txn(session, "UNLOCK TABLES")?;
    if session.has_explicit_table_locks() {
        implicit_commit_for_ddl(db, session)?;
    }
    release_explicit_locks(db, session);
    db.release_mdl_duration(session.connection_id as u64, MdlDuration::Explicit);
    crate::binlog::unlock_tables_for_session_scoped(session, db.global_read_lock_scope());
    Ok(QueryResponse::Ok)
}

fn execute_sql_prepare(
    db: &Arc<Database>,
    session: &mut SessionContext,
    name: String,
    source: Expr,
    registry: &SessionRegistry,
) -> Result<(), QueryError> {
    let prepare_started = std::time::Instant::now();
    let key = name.to_ascii_lowercase();
    // MySQL drops the old named statement before compiling its replacement.
    session.remove_sql_prepared_statement(&key);

    let source_value = match source {
        Expr::Literal(literal) => literal_value(&literal),
        Expr::UserVariable(name) => session.user_variable(&name).cloned().unwrap_or(Value::Null),
        _ => {
            return Err(error(
                crate::error::catalog::ER_PARSE_ERROR,
                "PREPARE source must be a string literal or user variable",
            ))
        }
    };
    if matches!(source_value, Value::Null) {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "PREPARE source may not be NULL",
        ));
    }
    let sql = crate::storage::value_to_string(&source_value);
    let _mode = crate::parser::ParserSqlModeGuard::activate(session.temporal_sql_mode_bits);
    let mut statement = parse_statement(&sql).map_err(parse_error_to_query_error)?;
    if matches!(statement, Statement::LoadData(_)) {
        return Err(error(
            crate::error::catalog::ER_UNSUPPORTED_PS,
            "LOAD DATA is not supported in prepared statements",
        ));
    }
    if let Statement::CreateView { query, .. } | Statement::AlterView { query, .. } = &statement {
        reject_unsupported_view_query(query)?;
    }
    if matches!(statement, Statement::MultiStatement { .. }) {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "PREPARE does not support multiple statements",
        ));
    }
    let mut parameter_count = 0usize;
    crate::parser::number_statement_placeholders(&mut statement, &mut parameter_count);
    // SQL PREPARE performs schema-aware constant validation once. Keep the
    // stored AST unqualified so its normal execution lifecycle is unchanged,
    // but bind a private copy to the current database/temporary-table scope
    // for diagnostics that MySQL reports at PREPARE rather than EXECUTE.
    let mut validation_statement = statement.clone();
    super::routines::with_view_function_binding(|| {
        expand_views(db, session, &mut validation_statement, 0)
    })?;
    qualify_statement_tables(db, session, &mut validation_statement)?;
    let routine_resolution = super::routines::resolve_stored_functions(
        db,
        session,
        &mut validation_statement,
        registry,
    )?;
    if let Statement::Select(select) = &validation_statement {
        crate::engine::executor::select::warn_prepared_select_temporal_constants(db, select);
    }
    let result_columns: Arc<[crate::types::Column]> =
        crate::engine::executor::select::aggregate::with_group_concat_max_len(
            session.group_concat_max_len,
            || {
                crate::engine::expr::with_session_expr_info(session, || {
                    // The new statement's placeholders must not see parameters
                    // belonging to the surrounding PREPARE command.
                    crate::engine::params::with_params(Vec::new(), || {
                        super::select::describe_statement_columns(
                            db,
                            session,
                            &validation_statement,
                        )
                    })
                })
            },
        )?
        .into();
    let result_column_bytes = result_columns.iter().fold(0usize, |total, column| {
        total
            .saturating_add(std::mem::size_of::<crate::types::Column>())
            .saturating_add(column.name.capacity())
            .saturating_add(column.source.schema.capacity())
            .saturating_add(column.source.table.capacity())
            .saturating_add(column.source.org_table.capacity())
            .saturating_add(column.source.org_name.capacity())
    });
    let retained_capacity = key
        .capacity()
        .saturating_add(sql.capacity().saturating_mul(16))
        .saturating_add(session.current_database.capacity())
        .saturating_add(result_column_bytes)
        .saturating_add(4096);
    let memory_charge = Arc::new(
        crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
            retained_capacity,
        )?,
    );
    let pfs = crate::engine::session_registry::PreparedStatementSnapshot::sql(
        name,
        Arc::from(sql.as_str()),
        session.connection_id as u64,
        retained_capacity as u64,
        prepare_started.elapsed(),
    );
    session.insert_sql_prepared_statement(
        key,
        crate::engine::api::SessionSqlPreparedStatement {
            pfs,
            original_sql: Arc::from(sql.as_str()),
            parser_sql_mode_bits: session.temporal_sql_mode_bits,
            statement: Arc::new(statement),
            database: session.current_database.clone(),
            parameter_count,
            result_columns,
            routine_result_metadata: !routine_resolution.routines.is_empty(),
            memory_charge,
        },
    )?;
    super::warn_successful_source_syntax_with_mode(&sql, session.temporal_sql_mode_bits);
    Ok(())
}

fn execute_sql_prepared(
    db: &Arc<Database>,
    session: &mut SessionContext,
    registry: &SessionRegistry,
    name: &str,
    using: &[String],
) -> Result<QueryResponse, QueryError> {
    let prepared = session
        .sql_prepared_statements
        .get(&name.to_ascii_lowercase())
        .cloned()
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_UNKNOWN_STMT_HANDLER,
                format!("Unknown prepared statement handler ({name}) given to EXECUTE"),
            )
        })?;
    if crate::engine::http::may_call(db, &prepared.original_sql) {
        crate::engine::http::activate();
        crate::engine::cbo::trace::redact_external();
    }
    let execute_started = std::time::Instant::now();
    if using.len() != prepared.parameter_count {
        session.record_sql_prepared_execute(name, execute_started.elapsed(), true);
        return Err(error(
            crate::error::catalog::ER_WRONG_ARGUMENTS,
            "Incorrect arguments to EXECUTE",
        ));
    }
    let params = using
        .iter()
        .map(|name| session.user_variable(name).cloned().unwrap_or(Value::Null))
        .collect::<Vec<_>>();
    super::warn_statement_digest_charset_conversion(&prepared.original_sql);
    let saved_database =
        std::mem::replace(&mut session.current_database, prepared.database.clone());
    // Walk the already parsed AST. Avoid rescanning the SQL text on every
    // EXECUTE; DIV is uncommon and non-DIV prepared statements stay on the
    // allocation-free path.
    let int_div_context = prepared_int_div_error_context(&prepared.statement);
    let mut statement = (*prepared.statement).clone();
    if let Statement::Call(call) = &mut statement {
        for arg in &mut call.args {
            if let Expr::Placeholder(index) = arg {
                if let Some(variable) = using.get(*index) {
                    *arg = Expr::UserVariable(variable.clone());
                }
            }
        }
    }
    let mut response = crate::engine::params::with_params(params, || {
        let execute = || execute_statement(db, session, statement, registry);
        match int_div_context {
            Some(context) => crate::engine::expr::with_integer_overflow_context(context, execute),
            None => execute(),
        }
    });
    session.current_database = saved_database;
    if let Ok(result) = &mut response {
        if !prepared.routine_result_metadata {
            apply_sql_prepared_result_columns(result, &prepared.result_columns);
        }
        super::select::cap_prepared_group_concat_limit_warnings(&prepared.statement, result);
        // PREPARE validates source syntax, but EXECUTE starts a fresh
        // diagnostics area. Re-publish lexical deprecation diagnostics for
        // the original statement so prepared and text protocols expose the
        // same successful-statement warnings.
        super::warn_successful_source_syntax_with_mode(
            &prepared.original_sql,
            prepared.parser_sql_mode_bits,
        );
    }
    session.record_sql_prepared_execute(name, execute_started.elapsed(), response.is_err());
    response
}

fn apply_sql_prepared_result_columns(
    response: &mut QueryResponse,
    prepared_columns: &[crate::types::Column],
) {
    fn replace_if_compatible(
        columns: &mut [crate::types::Column],
        prepared_columns: &[crate::types::Column],
    ) {
        if columns.len() == prepared_columns.len() {
            for (column, prepared) in columns.iter_mut().zip(prepared_columns) {
                if prepared.source.dynamic_parameter {
                    column.name.clone_from(&prepared.name);
                    continue;
                }
                // Bound arithmetic strings can resolve to exact DECIMAL at
                // EXECUTE time. Do not overwrite that precision with the
                // unbound PREPARE fallback DOUBLE descriptor.
                if matches!(column.column_type, crate::types::ColumnType::Decimal { .. })
                    && matches!(prepared.column_type, crate::types::ColumnType::Double)
                {
                    continue;
                }
                *column = prepared.clone();
            }
        }
    }

    match response {
        QueryResponse::ResultSet(result) => {
            replace_if_compatible(&mut result.columns, prepared_columns)
        }
        QueryResponse::SharedResultSet(result)
            if result.columns.len() == prepared_columns.len() =>
        {
            replace_if_compatible(Arc::make_mut(&mut result.columns), prepared_columns);
        }
        QueryResponse::SharedResultSet(_) => {}
        QueryResponse::StreamingResultSet(result) => {
            replace_if_compatible(&mut result.columns, prepared_columns)
        }
        QueryResponse::MultiResult(results) => {
            if let Some(first) = results.first_mut() {
                apply_sql_prepared_result_columns(first, prepared_columns);
            }
        }
        QueryResponse::Ok | QueryResponse::OkAffected { .. } => {}
    }
}

fn prepared_int_div_error_context<'q>(
    statement: impl Into<crate::parser::QueryRef<'q>>,
) -> Option<String> {
    let statement = statement.into();
    fn find(expr: &crate::parser::Expr) -> Option<String> {
        use crate::parser::Expr;
        match expr {
            Expr::Binary {
                left,
                op: crate::parser::BinaryOp::IntDiv,
                right,
            } => Some(format!(
                "({} DIV {})",
                crate::parser::expr_to_sql(left),
                crate::parser::expr_to_sql(right)
            )),
            Expr::Unary { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::GroupKeyRef { expr, .. } => find(expr),
            Expr::Binary { left, right, .. }
            | Expr::Like {
                expr: left,
                pattern: right,
                ..
            } => find(left).or_else(|| find(right)),
            Expr::Function { args, .. } => args.iter().find_map(find),
            _ => None,
        }
    }

    let crate::parser::QueryRef::Select(select) = statement else {
        return None;
    };
    select.projection.iter().find_map(|item| match item {
        crate::parser::SelectItem::Expr { expr, .. } => find(expr),
        _ => None,
    })
}

pub(super) fn execute_qualified_statement(
    db: &Arc<Database>,
    session: &mut SessionContext,
    mut statement: Statement,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    authorize_statement(db, session, &statement)?;
    if crate::binlog::enabled() {
        let kind = match &statement {
            Statement::Grant(g) => Some(&g.kind),
            Statement::Revoke(r) => Some(&r.kind),
            _ => None,
        };
        if session.sql_log_bin && kind.is_some_and(|kind| matches!(kind, GrantKind::Privileges { privileges, .. } if privileges.iter().any(|p| p.name.to_ascii_uppercase().starts_with("HTTP_")))) {
            return Err(crate::engine::http::error("ER_HTTP_UNSUPPORTED_CONTEXT", "HTTP privileges cannot be written to MySQL binlog"));
        }
        if kind.is_some_and(|kind|matches!(kind,GrantKind::Privileges{privileges,..} if privileges.iter().any(|p|p.name.to_ascii_uppercase().starts_with("AI_")))) {return Err(crate::engine::ai::error("ER_AI_UNSUPPORTED_CONTEXT","AI privileges cannot be written to MySQL binlog"));}
    }
    if let Statement::DropDatabase { name, .. } = &statement {
        if db.ai_catalog()?.models.keys().any(|m| {
            m.rsplit_once('.')
                .is_some_and(|(schema, _)| schema == name.to_ascii_lowercase())
        }) {
            return Err(crate::engine::ai::error(
                "ER_AI_UNSUPPORTED_CONTEXT",
                "drop schema AI models explicitly before DROP DATABASE",
            ));
        }
    }
    crate::planner::rewrite_enum_set_dml(db, &mut statement);
    if matches!(
        &statement,
        Statement::CreateUser(_)
            | Statement::AlterUser(_)
            | Statement::AlterUsers(_)
            | Statement::DropUser { .. }
            | Statement::RenameUser { .. }
            | Statement::CreateRole { .. }
            | Statement::DropRole { .. }
            | Statement::Grant(_)
            | Statement::Revoke(_)
            | Statement::SetPassword { .. }
            | Statement::SetDefaultRole(_)
    ) {
        implicit_commit_for_ddl(db, session)?;
    }
    // Resolve and register the complete temporal query graph while statement
    // MDL is held and before any branch can return a row. Leaf SELECT
    // executors see this context and therefore neither re-evaluate AS OF
    // expressions nor register snapshots independently.
    let temporal_context = super::temporal::preflight_statement(db, &statement)?;
    let _temporal_scope = temporal_context.map(|context| super::temporal::enter(Some(context)));
    match statement {
        Statement::AlterSystemReloadHttpConfig => {
            crate::engine::http::authorize(db, &session.authorization_context(), "HTTP_ADMIN")?;
            crate::engine::http::reload()?;
            Ok(QueryResponse::Ok)
        }
        Statement::ShowHttpStatus => {
            crate::engine::http::authorize(db, &session.authorization_context(), "HTTP_ADMIN")?;
            Ok(QueryResponse::ResultSet(ResultSet {
                columns: vec![
                    Column {
                        name: "Variable_name".into(),
                        column_type: ColumnType::VarString,
                        source: Default::default(),
                    },
                    Column {
                        name: "Value".into(),
                        column_type: ColumnType::LongLong,
                        source: Default::default(),
                    },
                ],
                rows: crate::engine::http::status()
                    .into_iter()
                    .map(|(k, v)| vec![Value::String(k), Value::UInt(v)])
                    .collect(),
            }))
        }
        Statement::Ai(command) => ai::admin(db, session, command, registry),
        Statement::Begin {
            read_only,
            consistent_snapshot,
        } => {
            ensure_no_xa_branch_for_local_txn(session, "BEGIN")?;
            begin_transaction_with(db, session, read_only, consistent_snapshot)?;
            Ok(QueryResponse::Ok)
        }
        Statement::Commit { chain, release } => {
            ensure_no_xa_branch_for_local_txn(session, "COMMIT")?;
            finish_transaction_with_completion(db, session, chain, release, false)?;
            Ok(QueryResponse::Ok)
        }
        Statement::Rollback { chain, release } => {
            ensure_no_xa_branch_for_local_txn(session, "ROLLBACK")?;
            finish_transaction_with_completion(db, session, chain, release, true)?;
            Ok(QueryResponse::Ok)
        }
        Statement::Savepoint(name) => {
            // M1.2 - Named user savepoints
            tx::ensure_xa_savepoint_allowed(session)?;
            if session.autocommit && !session.explicit_txn && session.current_txn.is_none() {
                return Ok(QueryResponse::Ok);
            }
            let txn = ensure_statement_txn(db, session)?;
            // Successful temporary DDL is not user-transactional. Only the
            // storage engine's DML savepoint participates in ROLLBACK TO.
            db.create_named_savepoint(txn.txn_id, name)?;
            Ok(QueryResponse::Ok)
        }
        Statement::RollbackToSavepoint(name) => {
            tx::ensure_xa_savepoint_allowed(session)?;
            let Some(txn_id) = session.current_txn else {
                return Err(error(
                    crate::error::catalog::ER_SP_DOES_NOT_EXIST,
                    format!("SAVEPOINT {} does not exist", name),
                ));
            };
            db.rollback_to_named_savepoint(txn_id, &name)?;
            Ok(QueryResponse::Ok)
        }
        Statement::ReleaseSavepoint(name) => {
            tx::ensure_xa_savepoint_allowed(session)?;
            let Some(txn_id) = session.current_txn else {
                return Err(error(
                    crate::error::catalog::ER_SP_DOES_NOT_EXIST,
                    format!("SAVEPOINT {} does not exist", name),
                ));
            };
            db.release_named_savepoint(txn_id, &name)?;
            Ok(QueryResponse::Ok)
        }
        Statement::Prepare { name, source } => {
            execute_sql_prepare(db, session, name, source, registry)?;
            Ok(QueryResponse::Ok)
        }
        Statement::ExecutePrepared { name, using } => {
            execute_sql_prepared(db, session, registry, &name, &using)
        }
        Statement::DeallocatePrepared { name } => {
            let key = name.to_ascii_lowercase();
            if !session.remove_sql_prepared_statement(&key) {
                return Err(error(
                    crate::error::catalog::ER_UNKNOWN_STMT_HANDLER,
                    format!(
                        "Unknown prepared statement handler ({name}) given to DEALLOCATE PREPARE"
                    ),
                ));
            }
            Ok(QueryResponse::Ok)
        }
        Statement::SetAutocommit(value) => {
            apply_autocommit(db, session, value)?;
            Ok(QueryResponse::Ok)
        }
        Statement::SetTransactionIsolation { scope, isolation } => {
            apply_transaction_isolation_scope(
                session,
                scope,
                parser_isolation_to_storage(isolation)?,
            )?;
            session.mark_pfs_variables_dirty();
            Ok(QueryResponse::Ok)
        }
        Statement::Set(assignments) => {
            if assignments
                .iter()
                .any(|a| crate::engine::ai::sql::expr_has_ai(&a.value))
            {
                let value = crate::engine::ai::with_calls(db.clone(), session, || {
                    crate::engine::expr::with_session_expr_info(session, || {
                        crate::engine::expr::eval_const(&assignments[0].value)
                    })
                })?;
                session.set_user_variable(assignments[0].name.trim_start_matches('@'), value);
                return Ok(QueryResponse::Ok);
            }
            if assignments
                .iter()
                .any(|a| super::prepared_dml::expr_contains_subquery(&a.value))
            {
                let statement_owned_txn =
                    session.autocommit && !session.explicit_txn && session.current_txn.is_none();
                execute_in_transaction_with_session(db, session, |db, session, _| {
                    let result = execute_set_statement(db, session, &assignments);
                    // SET may disable autocommit after reading its subqueries.
                    // That read belongs to the old autocommit statement, not
                    // to the next user transaction selected by the new value.
                    if statement_owned_txn && !session.autocommit {
                        if result.is_ok() {
                            commit_session_txn(db, session)?;
                        } else {
                            rollback_session_txn(db, session)?;
                        }
                    }
                    result
                })
            } else {
                execute_set_statement(db, session, &assignments)
            }
        }
        Statement::SetNames { charset, collation } => {
            let response =
                execute_set_names_statement(session, charset.as_deref(), collation.as_deref())?;
            session.mark_pfs_variables_dirty();
            Ok(response)
        }
        Statement::SetCharacterSet { charset } => {
            let response = execute_set_character_set_statement(session, charset.as_deref())?;
            session.mark_pfs_variables_dirty();
            Ok(response)
        }
        Statement::SetTransactionCharacteristics {
            scope,
            characteristics,
        } => {
            let response = apply_transaction_characteristics(session, scope, &characteristics)?;
            session.mark_pfs_variables_dirty();
            Ok(response)
        }
        Statement::ResetPersist { var, if_exists } => {
            execute_reset_persist(db, var.as_deref(), if_exists)
        }
        Statement::SetResourceGroup { .. } => Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "RESOURCE GROUP is not supported",
        )),
        Statement::LockTables(specs) => execute_lock_tables(db, session, specs),
        Statement::UnlockTables => execute_unlock_tables(db, session),
        Statement::AlterSystemCheckpoint => {
            db.checkpoint()?;
            Ok(QueryResponse::Ok)
        }
        Statement::AlterSystemFlushDataCache => {
            db.flush_data_cache()?;
            Ok(QueryResponse::Ok)
        }
        Statement::AlterSystemFlushSqlCache => {
            crate::engine::cache::flush_global_sql_caches();
            Ok(QueryResponse::Ok)
        }
        Statement::ReplicationAdmin { sql } => crate::replication::handle_admin_sql(&sql),
        Statement::MydbReplicationAdmin { sql } => {
            crate::repl::handle_admin_sql_with_session(&sql, session)
        }
        Statement::BackupAdmin { command } => crate::backup::sql::execute(db, &command),
        Statement::BinlogAdmin { sql } => {
            if sql
                .trim()
                .trim_end_matches(';')
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .eq_ignore_ascii_case("FLUSH TABLES WITH READ LOCK")
            {
                implicit_commit_for_ddl(db, session)?;
                db.acquire_mdl(
                    session.connection_id as u64,
                    Some(session.connection_id as u64),
                    MdlDuration::Explicit,
                    vec![MdlRequest {
                        key: MdlObjectKey::global(),
                        lock_type: MdlLockType::SharedNoWrite,
                    }],
                    "sql/flush_tables_with_read_lock",
                )?;
            }
            crate::binlog::handle_admin_sql_scoped(
                &sql,
                session,
                db.global_read_lock_scope(),
                || db.reset_binlog_wal_cutoff(),
            )
        }
        Statement::CreateDatabase {
            name,
            if_not_exists,
            default_charset,
            default_collation,
        } => {
            if name.chars().count() > 64 {
                return Err(error(
                    crate::error::catalog::ER_TOO_LONG_IDENT,
                    format!("Identifier name '{name}' is too long"),
                ));
            }
            implicit_commit_for_ddl(db, session)?;
            let (charset, collation) = if default_charset.is_none() && default_collation.is_none() {
                (
                    Some(session.sysvars.character_set_server.as_str()),
                    Some(session.sysvars.collation_server.as_str()),
                )
            } else {
                (default_charset.as_deref(), default_collation.as_deref())
            };
            db.create_database_with_options(&name, if_not_exists, charset, collation)?;
            Ok(QueryResponse::Ok)
        }
        Statement::AlterDatabase {
            name,
            default_charset,
            default_collation,
        } => {
            let name = if name.is_empty() {
                session.current_database.clone()
            } else {
                name
            };
            if name.is_empty() {
                return Err(error(
                    crate::error::catalog::ER_NO_DB_ERROR,
                    "No database selected",
                ));
            }
            implicit_commit_for_ddl(db, session)?;
            db.alter_database_defaults(
                &name,
                default_charset.as_deref(),
                default_collation.as_deref(),
            )?;
            Ok(QueryResponse::Ok)
        }
        Statement::DropDatabase { name, if_exists } => {
            implicit_commit_for_ddl(db, session)?;
            // The schema X grant is already held. Re-enumerate on every retry:
            // a CREATE/RENAME may have completed while we waited for that grant.
            let requests = drop_database_mdl_requests(db, &name);
            let started = Instant::now();
            let mut waits = LockWaitStats::default();
            loop {
                match db.acquire_cached_mdl(
                    session.connection_id as u64,
                    Some(session.connection_id as u64),
                    MdlDuration::Statement,
                    &requests,
                    "sql/drop_database",
                ) {
                    Ok(()) => break,
                    Err(err) if is_lock_wait_pending(&err) => {
                        // Keep schema X and the pending object ticket while
                        // waiting. Returning pending through statement cleanup
                        // would erase the ticket and misclassify this as a row
                        // wait (with the wrong timeout) on the outer retry.
                        wait_for_lock_signal_or_cancel(db, session, started, &mut waits)?;
                    }
                    Err(err) => return Err(err),
                }
            }
            db.drop_database(&name, if_exists)?;
            if session.current_database.eq_ignore_ascii_case(&name) {
                session.current_database.clear();
                session.mark_pfs_variables_dirty();
                // CLIENT_SESSION_TRACK: the current schema implicitly
                // moved to DEFAULT_DATABASE — tell the client so its
                // cached `currentSchema` (Connector/J, mysqlsh, …)
                // stays in sync.
                session.note_schema_change(String::new());
            }
            Ok(QueryResponse::Ok)
        }
        Statement::UseDatabase(name) => {
            let db_name = name.to_ascii_lowercase();
            if !db.database_exists(&db_name) {
                return Err(error(
                    crate::error::catalog::ER_BAD_DB_ERROR,
                    format!("unknown database '{name}'"),
                ));
            }
            session.current_database = db_name.clone();
            session.mark_pfs_variables_dirty();
            // CLIENT_SESSION_TRACK SCHEMA: queue the new schema name
            // into the next OK packet so the client mirrors it.
            session.note_schema_change(db_name);
            Ok(QueryResponse::Ok)
        }
        Statement::CreateTable(stmt) => {
            // MySQL 8.0.23+: a table must have at least one visible column.
            if !stmt.columns.is_empty() && stmt.columns.iter().all(|c| c.invisible) {
                return Err(error(
                    crate::error::catalog::ER_TABLE_MUST_HAVE_A_VISIBLE_COLUMN,
                    "A table must have at least one visible column.",
                ));
            }
            crate::storage::schema::validate_supported_table_options(&stmt.table_options)?;
            if stmt.temporary {
                let on_commit = stmt.on_commit.unwrap_or(TempTableOnCommit::PreserveRows);
                return execute_temp_ddl_in_transaction(db, session, |db, session| {
                    let (visible_key, visible_name) =
                        visible_table_key_and_name(session, &stmt.name);
                    if session.temp_tables.contains_key(&visible_key) {
                        if stmt.if_not_exists {
                            crate::engine::api::push_statement_warning(
                                crate::error::ConditionLevel::Note,
                                crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                                format!("Table '{}' already exists", stmt.name),
                            );
                            return Ok(QueryResponse::Ok);
                        }
                        return Err(crate::error::messages::table_exists(&stmt.name));
                    }
                    warn_metadata_only_create_options(&stmt);
                    let internal_key = db.create_temp_table_with_partition(
                        &visible_name,
                        session.connection_id,
                        &session.temp_quota,
                        stmt.columns,
                        stmt.primary_key,
                        stmt.indexes,
                        stmt.checks,
                        stmt.foreign_keys,
                        stmt.auto_increment_start,
                        stmt.partition,
                        stmt.default_charset,
                        stmt.default_collation,
                        stmt.column_comments,
                        stmt.table_comment,
                        stmt.pctfree,
                        stmt.pctused,
                        stmt.table_options,
                        stmt.engine,
                    )?;
                    session.add_temp_table(visible_key.clone(), internal_key.clone(), on_commit);
                    session.temp_table_journal.push(
                        crate::engine::api::TempTableJournalEntry::Create {
                            visible_key,
                            internal_key,
                        },
                    );
                    Ok(QueryResponse::Ok)
                });
            }
            implicit_commit_for_ddl(db, session)?;
            let name = qualify_persistent_table_name(session, &stmt.name);
            if db.relation_name_exists(&name) || is_reserved_system_table_name(&name) {
                if stmt.if_not_exists {
                    crate::engine::api::push_statement_warning(
                        crate::error::ConditionLevel::Note,
                        crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                        format!("Table '{}' already exists", stmt.name),
                    );
                    return Ok(QueryResponse::Ok);
                }
                return Err(crate::error::messages::table_exists(&stmt.name));
            }
            warn_metadata_only_create_options(&stmt);
            match db.create_table_with_partition(
                &name,
                stmt.columns,
                stmt.primary_key,
                stmt.indexes,
                stmt.checks,
                stmt.foreign_keys,
                stmt.auto_increment_start,
                stmt.partition,
                stmt.default_charset,
                stmt.default_collation,
                stmt.column_comments,
                stmt.table_comment,
                stmt.pctfree,
                stmt.pctused,
                stmt.table_options,
                stmt.engine,
            ) {
                Ok(()) => {}
                Err(err)
                    if stmt.if_not_exists
                        && err.is(crate::error::catalog::ER_TABLE_EXISTS_ERROR) =>
                {
                    crate::engine::api::push_statement_warning(
                        crate::error::ConditionLevel::Note,
                        crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                        format!("Table '{}' already exists", stmt.name),
                    );
                }
                Err(err) => return Err(err),
            }
            let (database, table) =
                crate::storage::split_table_name(&session.current_database, &name);
            revalidate_altered_table_views(db, session, &database, &table)?;
            Ok(QueryResponse::Ok)
        }
        Statement::CreateTableLike {
            name,
            if_not_exists,
            source,
            temporary,
            on_commit,
        } => {
            execute_create_table_like(
                db,
                session,
                &name,
                if_not_exists,
                &source,
                temporary,
                on_commit,
            )?;
            Ok(QueryResponse::Ok)
        }
        Statement::AlterTable(stmt) => {
            // MySQL 8.0 permits only LOCK=DEFAULT with an explicitly
            // requested INSTANT algorithm. A non-default LOCK is not an
            // additional promise to validate action-by-action; the pair is
            // intrinsically invalid and must fail with ER_WRONG_USAGE before
            // any catalog mutation.
            if stmt.algorithm == AlterAlgorithm::Instant && stmt.lock != AlterLock::Default {
                return Err(error(
                    crate::error::catalog::ER_WRONG_USAGE,
                    "Incorrect usage of ALGORITHM=INSTANT and LOCK=NONE/SHARED/EXCLUSIVE",
                ));
            }
            if is_session_temp_table_ref(session, &stmt.table) {
                // ALTER TABLE on a temporary table is still an implicit-commit
                // statement in MySQL; only CREATE/DROP TEMPORARY receive the
                // no-implicit-commit exception.
                implicit_commit_for_ddl(db, session)?;
                return execute_temp_alter_table(db, session, stmt);
            }
            implicit_commit_for_ddl(db, session)?;
            let index_build_options = index_build_options_from_hints(&stmt.query_hints);
            let explicit_parallel_hint = has_explicit_parallel_hint(&stmt.query_hints);
            let has_add_index = stmt
                .actions
                .iter()
                .any(|action| matches!(action, AlterAction::AddIndex(_)));
            if explicit_parallel_hint && !has_add_index {
                crate::engine::api::push_statement_warning(
                    crate::error::ConditionLevel::Warning,
                    crate::error::catalog::ER_WARN_UNSUPPORTED_MAX_EXECUTION_TIME,
                    "PARALLEL hint is accepted but ignored for ALTER TABLE without ADD INDEX"
                        .to_string(),
                );
            }
            let mut table_name = qualify_persistent_table_name(session, &stmt.table);
            if is_reserved_system_table_name(&table_name) {
                return Err(error(
                    crate::error::catalog::ER_NO_SYSTEM_TABLE_ACCESS,
                    format!("Cannot alter reserved system table '{table_name}'"),
                ));
            }
            let alter_has_zerofill = stmt.actions.iter().any(|action| match action {
                AlterAction::AddColumn(def) | AlterAction::ModifyColumn(def) => def.zerofill,
                AlterAction::ChangeColumn { new_def, .. } => new_def.zerofill,
                _ => false,
            });
            if alter_has_zerofill {
                crate::engine::api::push_statement_warning(
                    crate::error::ConditionLevel::Warning,
                    crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT,
                    "The ZEROFILL attribute is deprecated and will be removed in a \
                     future release. Use the LPAD function to zero-pad numbers, or \
                     store the formatted numbers in a CHAR column.",
                );
            }
            // Startup keeps compact catalog headers. Hydrate and pin the full
            // descriptor before any preflight/snapshot/mutation uses the map.
            // Otherwise a cold metadata-only ALTER can persist zero columns.
            let _alter_source = db.table(&table_name)?;
            let _ddl_definition = db.pin_ddl_table(&table_name)?;
            db.validate_alter_table_actions(&table_name, &stmt.actions)?;
            let alter_plan = plan_persistent_alter(db, session, &stmt)?;
            validate_alter_algorithm_for_required(stmt.algorithm, alter_plan.required)?;
            db.acquire_alter_execution_mdl(
                session.connection_id as u64,
                &table_name,
                alter_plan.mdl,
                session
                    .sysvars
                    .lock_wait_timeout
                    .map(std::time::Duration::from_secs)
                    .unwrap_or_else(crate::storage::metadata_wait_timeout),
            )?;
            let has_copy_required_action = alter_plan.physical_copy;
            // A concurrent reader loading another cold table can evict cached
            // descriptors. Keep the mutable DDL candidate resident through
            // durable publication (including the oversized-descriptor case).
            let _metadata_definition = alter_plan
                .read_compatible_metadata
                .then(|| db.pin_read_compatible_ddl_definition(&_alter_source))
                .transpose()?;
            let online_type_copy = alter_plan.online_copy;
            let constraint_only = alter_plan.constraints;

            let online_index_only = stmt.partition_actions.is_empty()
                && stmt.algorithm != AlterAlgorithm::Copy
                && matches!(stmt.lock, AlterLock::None | AlterLock::Default)
                && stmt.actions.iter().all(|a| matches!(a, AlterAction::AddIndex(i) if i.index_type.eq_ignore_ascii_case("BTREE")));
            if !has_copy_required_action
                && !online_index_only
                && stmt.actions.len() + stmt.partition_actions.len() > 1
            {
                for action in &stmt.actions {
                    if let AlterAction::AddIndex(index) = action {
                        let primary = index
                            .name
                            .as_deref()
                            .is_some_and(|name| name.eq_ignore_ascii_case("PRIMARY"));
                        if index.unique || primary {
                            let schema = create_index_schema_from_parts(
                                db,
                                &table_name,
                                index.name.clone(),
                                index.columns.clone(),
                                index.column_prefix_lengths.clone(),
                                index.column_descending.clone(),
                                index.key_expressions.clone(),
                                index.key_expression_multi_valued.clone(),
                                index.unique,
                                primary,
                                index.included_columns.clone(),
                                index.index_type.clone(),
                                index.visible,
                                index.comment.clone(),
                                index.fulltext_parser.clone(),
                                index.vector_options.clone(),
                                false,
                            )?;
                            db.validate_add_index_existing_rows(&table_name, &schema)?;
                        }
                    }
                }
            }
            // Defer dependent-view invalidation until successful publication:
            // a failed conversion must leave both source and views usable.
            let (alter_db, alter_name) =
                crate::storage::split_table_name(&session.current_database, &stmt.table);

            // Detect the homogeneous multi-AddIndex shape: when every
            // action adds an index, fuse them onto a single heap scan
            // via `add_indexes_with_async`. A single-action ALTER TABLE
            // also flows through here when it's `ADD INDEX`, so the
            // narrow-int specialisation kicks in either way.
            let all_add_index = !stmt.actions.is_empty()
                && stmt
                    .actions
                    .iter()
                    .all(|a| matches!(a, AlterAction::AddIndex(_)));
            if stmt.lock == AlterLock::None && (!all_add_index || stmt.actions.iter().any(|a| matches!(
                a, AlterAction::AddIndex(i) if !i.index_type.eq_ignore_ascii_case("BTREE")
            ))) && stmt.actions.iter().any(|a| matches!(
                a, AlterAction::AddIndex(i) if i.name.as_deref().is_some_and(|n| n.eq_ignore_ascii_case("PRIMARY"))
            )) {
                return Err(error(crate::error::catalog::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON,
                    "LOCK=NONE for ADD PRIMARY KEY requires an index-only ALTER"));
            }
            let physical_copy_requested =
                has_copy_required_action || stmt.algorithm == AlterAlgorithm::Copy;
            let mut fused_physical_copy = false;
            if physical_copy_requested && stmt.partition_actions.is_empty() {
                let mut added_indexes = Vec::new();
                for action in &stmt.actions {
                    let AlterAction::AddIndex(index) = action else {
                        continue;
                    };
                    reject_parallel_hint_for_non_btree(&stmt.query_hints, &index.index_type)?;
                    let primary = index
                        .name
                        .as_deref()
                        .is_some_and(|name| name.eq_ignore_ascii_case("PRIMARY"));
                    added_indexes.push(create_index_schema_from_parts(
                        db,
                        &table_name,
                        index.name.clone(),
                        index.columns.clone(),
                        index.column_prefix_lengths.clone(),
                        index.column_descending.clone(),
                        index.key_expressions.clone(),
                        index.key_expression_multi_valued.clone(),
                        index.unique,
                        primary,
                        index.included_columns.clone(),
                        index.index_type.clone(),
                        index.visible,
                        index.comment.clone(),
                        index.fulltext_parser.clone(),
                        index.vector_options.clone(),
                        true,
                    )?);
                }
                match db.rewrite_compound_alter_once(
                    &table_name,
                    &stmt.actions,
                    added_indexes,
                    session.is_strict_sql_mode(),
                    online_type_copy.then_some((session.connection_id as u64, stmt.lock)),
                )? {
                    Some(warnings) => {
                        for warning in warnings {
                            crate::engine::api::push_sql_warning(warning);
                        }
                        fused_physical_copy = true;
                    }
                    None if stmt.algorithm == AlterAlgorithm::Copy => {
                        return Err(error(
                            crate::error::catalog::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON,
                            "ALGORITHM=COPY is not supported for this ALTER action mix",
                        ));
                    }
                    None => {
                        return Err(error(
                            crate::error::catalog::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON,
                            "this physical ALTER action mix has no atomic execution plan",
                        ));
                    }
                }
            }
            if constraint_only {
                db.alter_constraints_online(
                    &table_name,
                    &stmt.actions,
                    session.connection_id as u64,
                    stmt.lock,
                )?;
            } else if fused_physical_copy {
                // One candidate schema, one source scan, one index fan-out and
                // one generation publish already completed above.
            } else if all_add_index {
                let mut indexes = Vec::with_capacity(stmt.actions.len());
                for action in stmt.actions {
                    let AlterAction::AddIndex(index) = action else {
                        unreachable!("filtered to AddIndex above");
                    };
                    reject_parallel_hint_for_non_btree(&stmt.query_hints, &index.index_type)?;
                    let primary = index
                        .name
                        .as_deref()
                        .is_some_and(|name| name.eq_ignore_ascii_case("PRIMARY"));
                    let name = index.name.unwrap_or_else(|| index.columns.join("_"));
                    let column_directions = index
                        .column_descending
                        .iter()
                        .map(|desc| {
                            if *desc {
                                crate::storage::IndexColumnDirection::Desc
                            } else {
                                crate::storage::IndexColumnDirection::Asc
                            }
                        })
                        .collect();
                    let key_expressions = build_index_key_expressions(
                        db,
                        &table_name,
                        &index.columns,
                        &index.key_expressions,
                        &index.key_expression_multi_valued,
                    )?;
                    let fulltext = index.index_type.eq_ignore_ascii_case("FULLTEXT").then(|| {
                        let mut meta = crate::storage::FulltextMeta {
                            parser: index.fulltext_parser.clone(),
                            ..Default::default()
                        };
                        crate::storage::fulltext::snapshot_tokenizer_config_into(&mut meta);
                        meta
                    });
                    indexes.push(IndexSchema {
                        readable_from_scn: 0,
                        index_id: 0,
                        name,
                        columns: index.columns,
                        column_prefix_lengths: index.column_prefix_lengths,
                        column_directions,
                        key_expressions,
                        unique: index.unique,
                        primary,
                        included_columns: index.included_columns,
                        state: crate::storage::IndexState::Public,
                        vector: crate::storage::vector_config::VectorIndexConfig::from_options(
                            &index.index_type,
                            index.vector_options.as_ref(),
                            &index.comment,
                        )?,
                        index_type: index.index_type,
                        visible: index.visible,
                        comment: index.comment,
                        fulltext,
                    });
                }
                db.add_indexes_for_sql(
                    &table_name,
                    indexes,
                    index_build_options,
                    session.connection_id as u64,
                    matches!(stmt.lock, AlterLock::None | AlterLock::Default)
                        && stmt.algorithm != AlterAlgorithm::Copy,
                )?;
            } else {
                // C1 (atomic DDL): when a multi-action ALTER consists solely
                // of metadata-reversible actions (no heap rewrite, no
                // index/heap/LOB file mutation, no cross-table effect), make
                // the whole statement atomic by snapshotting the schema up
                // front and restoring it if any action fails mid-loop. This
                // closes the window where e.g. `ALTER t ADD CHECK(bad1), ADD
                // CHECK(bad2)` left `bad1` committed after `bad2` failed on
                // existing data. Statements containing a physical action keep
                // the sequential path — a schema-only restore after an
                // irreversible mutation would corrupt (see
                // `alter_action_is_metadata_reversible`).
                let atomic_metadata = alter_plan.metadata_atomic;
                let rollback_snapshot = if atomic_metadata {
                    Some(db.snapshot_table_schema(&table_name)?)
                } else {
                    None
                };
                // Bug B1/B5: without deferral each per-action
                // `commit_catalog_txn` committed a separate DD generation. A
                // crash between them could leave a statement like
                // `ALTER t ADD c INT AFTER a` (which parses to
                // AddColumn + MoveColumn) with only the ADD persisted
                // and the column stuck at the end — breaking MySQL
                // 8.0 atomic-DDL semantics. Under this guard every
                // internal `commit_catalog_txn` becomes a no-op; we commit
                // once at the end (success) or not at all (failure —
                // the schema snapshot rollback restored in-memory
                // state to match on-disk state).
                let atomic_flush_guard = if atomic_metadata {
                    Some(crate::storage::CatalogTxnBatchGuard::new())
                } else {
                    None
                };
                let action_result = (|| -> Result<(), QueryError> {
                    if alter_plan.read_compatible_metadata && stmt.actions.len() == 2 {
                        if let AlterAction::MoveColumn { position, .. } = &stmt.actions[1] {
                            return db.alter_column_positioned_metadata(
                                &table_name,
                                &stmt.actions[0],
                                position,
                                session.connection_id as u64,
                            );
                        }
                    }
                    for action in stmt.actions {
                        match action {
                            AlterAction::AddColumn(column) => {
                                db.add_column(&table_name, column)?;
                            }
                            AlterAction::DropColumn(column_name) => {
                                db.drop_column(&table_name, &column_name)?;
                            }
                            AlterAction::MoveColumn { column, position } => {
                                db.move_column_position(&table_name, &column, position)?;
                            }
                            AlterAction::SetColumnComment { column, comment } => {
                                db.set_column_comment(&table_name, &column, &comment)?;
                            }
                            AlterAction::SetColumnDefault { column, default } => {
                                db.set_column_default(&table_name, &column, default)?;
                            }
                            AlterAction::SetTableOptions {
                                default_charset,
                                default_collation,
                                comment,
                                mut generic,
                            } => {
                                let engine = generic
                                    .iter()
                                    .position(|(key, _)| key.eq_ignore_ascii_case("ENGINE"))
                                    .map(|index| generic.remove(index).1);
                                db.set_table_options(
                                    &table_name,
                                    engine,
                                    default_charset,
                                    default_collation,
                                    comment,
                                    generic,
                                )?;
                            }
                            AlterAction::SetStorageProperties { pctfree, pctused } => {
                                db.set_table_storage_properties(&table_name, pctfree, pctused)?;
                            }
                            AlterAction::Rebuild => {
                                unreachable!("rebuild uses the physical COPY plan")
                            }
                            AlterAction::Noop { warning } => {
                                if let Some((level, code, message)) = warning {
                                    let level = if level.eq_ignore_ascii_case("Note") {
                                        crate::error::ConditionLevel::Note
                                    } else if level.eq_ignore_ascii_case("Error") {
                                        crate::error::ConditionLevel::Error
                                    } else {
                                        crate::error::ConditionLevel::Warning
                                    };
                                    crate::engine::api::push_statement_warning(
                                        level,
                                        *crate::error::catalog::lookup_by_code(code)
                                            .expect("ALTER NOOP warning must use the catalog"),
                                        message,
                                    );
                                }
                            }
                            AlterAction::SetColumnVisibility { column, invisible } => {
                                db.set_column_visibility(&table_name, &column, invisible)?;
                            }
                            AlterAction::ConvertToCharset { charset, collation } => {
                                for warning in db.convert_table_charset(
                                    &table_name,
                                    &charset,
                                    collation.as_deref(),
                                    session.is_strict_sql_mode(),
                                )? {
                                    crate::engine::api::push_sql_warning(warning);
                                }
                            }
                            AlterAction::AddIndex(index) => {
                                reject_parallel_hint_for_non_btree(
                                    &stmt.query_hints,
                                    &index.index_type,
                                )?;
                                let primary = index
                                    .name
                                    .as_deref()
                                    .is_some_and(|name| name.eq_ignore_ascii_case("PRIMARY"));
                                let name = index.name.unwrap_or_else(|| index.columns.join("_"));
                                let column_directions = index
                                    .column_descending
                                    .iter()
                                    .map(|desc| {
                                        if *desc {
                                            crate::storage::IndexColumnDirection::Desc
                                        } else {
                                            crate::storage::IndexColumnDirection::Asc
                                        }
                                    })
                                    .collect();
                                // Route through the online-build entry point.
                                // `MYDB_DDL_ASYNC` may select internal worker
                                // mechanics, but SQL waits until Public or a
                                // propagated failure.
                                let key_expressions = build_index_key_expressions(
                                    db,
                                    &table_name,
                                    &index.columns,
                                    &index.key_expressions,
                                    &index.key_expression_multi_valued,
                                )?;
                                let fulltext =
                                    index.index_type.eq_ignore_ascii_case("FULLTEXT").then(|| {
                                        let mut meta = crate::storage::FulltextMeta {
                                            parser: index.fulltext_parser.clone(),
                                            ..Default::default()
                                        };
                                        crate::storage::fulltext::snapshot_tokenizer_config_into(
                                            &mut meta,
                                        );
                                        meta
                                    });
                                db.add_index_with_async_options(
                                    &table_name,
                                    IndexSchema {
                                        readable_from_scn: 0,
                                        index_id: 0,
                                        name,
                                        columns: index.columns,
                                        column_prefix_lengths: index.column_prefix_lengths,
                                        column_directions,
                                        key_expressions,
                                        unique: index.unique,
                                        primary,
                                        included_columns: index.included_columns,
                                        state: crate::storage::IndexState::Public,
                                        vector: crate::storage::vector_config::VectorIndexConfig::from_options(&index.index_type, index.vector_options.as_ref(), &index.comment)?,
                        index_type: index.index_type,
                                        visible: index.visible,
                                        comment: index.comment,
                                        fulltext,
                                    },
                                    index_build_options,
                                )?;
                            }
                            AlterAction::DropIndex(index) => db.drop_index(&table_name, &index)?,
                            AlterAction::DropPrimaryKey => {
                                db.drop_primary_key(&table_name)?;
                            }
                            AlterAction::RenameIndex { old_name, new_name } => {
                                db.rename_index(&table_name, &old_name, &new_name)?;
                            }
                            AlterAction::RenameTable { new_name } => {
                                let old_db = alter_db.clone();
                                let old_table = alter_name.clone();
                                let new_qualified =
                                    qualify_alter_rename_target(session, &table_name, &new_name);
                                if is_reserved_system_table_name(&new_qualified) {
                                    return Err(crate::error::messages::table_exists(&new_name));
                                }
                                db.rename_table(&table_name, &new_qualified)?;
                                let _ = db.invalidate_views_referencing(&old_db, &old_table);
                                table_name = new_qualified;
                            }
                            AlterAction::AlterIndexVisibility { name, visible } => {
                                db.alter_index_visibility(&table_name, &name, visible)?
                            }
                            AlterAction::ModifyColumn(mut column) => {
                                // MySQL allows a redeclared CHECK inside
                                // MODIFY — apply it as an ADD CHECK after the
                                // column change, same end state as MySQL.
                                let checks = std::mem::take(&mut column.checks);
                                for warning in db.modify_column_with_policy_for_session(
                                    &table_name,
                                    column,
                                    session.is_strict_sql_mode(),
                                    stmt.lock,
                                    session.connection_id as u64,
                                )? {
                                    crate::engine::api::push_sql_warning(warning);
                                }
                                for check in checks {
                                    db.add_check_constraint(&table_name, check)?;
                                }
                            }
                            AlterAction::ChangeColumn {
                                old_name,
                                mut new_def,
                            } => {
                                let checks = std::mem::take(&mut new_def.checks);
                                for warning in db.change_column_with_policy_for_session(
                                    &table_name,
                                    &old_name,
                                    new_def,
                                    session.is_strict_sql_mode(),
                                    stmt.lock,
                                    session.connection_id as u64,
                                )? {
                                    crate::engine::api::push_sql_warning(warning);
                                }
                                for check in checks {
                                    db.add_check_constraint(&table_name, check)?;
                                }
                            }
                            AlterAction::RenameColumn { old_name, new_name } => {
                                db.rename_column(&table_name, &old_name, &new_name)?;
                            }
                            AlterAction::AddCheck(check) => {
                                db.add_check_constraint(&table_name, check)?;
                            }
                            AlterAction::AddForeignKey(fk) => {
                                db.add_foreign_key(&table_name, fk)?;
                            }
                            AlterAction::DropCheck {
                                name,
                                if_exists,
                                include_foreign_keys,
                            } => {
                                db.drop_check_constraint(
                                    &table_name,
                                    &name,
                                    if_exists,
                                    include_foreign_keys,
                                )?;
                            }
                            AlterAction::DropForeignKey { name } => {
                                db.drop_foreign_key(&table_name, &name)?;
                            }
                            AlterAction::AlterCheck { name, enforced } => {
                                db.alter_check_constraint(&table_name, &name, enforced)?;
                            }
                            AlterAction::SetAutoIncrement(value) => {
                                db.set_auto_increment(&table_name, value)?;
                            }
                        }
                    }
                    Ok(())
                })();
                if let Err(err) = action_result {
                    // Atomic rollback for the all-metadata case; a no-op
                    // (snapshot is None) for statements that took the
                    // sequential physical path, preserving prior behaviour.
                    // Under the flush-defer guard, no intermediate action
                    // reached disk, so restoring the in-memory catalog
                    // and simply dropping the guard leaves on-disk state
                    // at the pre-ALTER generation — nothing to unwind on
                    // disk.
                    if let Some(snapshot) = rollback_snapshot {
                        db.restore_table_schema_from_snapshot(&table_name, snapshot)?;
                    }
                    drop(atomic_flush_guard);
                    return Err(err);
                }
                // Success: if the batch requested any flush during the
                // deferred window, commit once — mydb 8.0-style atomic
                // DDL. Skipping the flush when no action actually asked
                // for one avoids a spurious DD generation for a batch
                // that was all Noop / warnings.
                if let Some(guard) = atomic_flush_guard {
                    let needs_flush = guard.any_flush_requested();
                    drop(guard);
                    if needs_flush {
                        if let Err(err) = db.commit_ddl_catalog_txn_recoverable() {
                            if let Some(snapshot) = rollback_snapshot {
                                db.restore_table_schema_from_snapshot(&table_name, snapshot)?;
                            }
                            return Err(err);
                        }
                    }
                }
            }
            // Run any partition actions captured by the partition
            // pre-parser. Unsupported mixed physical statements were rejected
            // by the plan before any regular action was executed.
            let mut maintenance_rows: Vec<Vec<Value>> = Vec::new();
            for action in stmt.partition_actions {
                use crate::parser_partition::PartitionAction as PA;
                // ANALYZE/CHECK/OPTIMIZE/REPAIR answer with the MySQL
                // maintenance result set (Table/Op/Msg_type/Msg_text);
                // everything else stays a plain OK.
                let op_label = match &action {
                    PA::AnalyzePartitions(_) => Some("analyze"),
                    PA::CheckPartitions(_) => Some("check"),
                    PA::OptimizePartitions(_) => Some("optimize"),
                    PA::RepairPartitions(_) => Some("repair"),
                    _ => None,
                };
                let allow_partition_dml = matches!(stmt.lock, AlterLock::Default | AlterLock::None)
                    && db.partition_action_online_supported(&table_name, &action)?;
                db.alter_partition_with_mdl(
                    &table_name,
                    action,
                    Some((
                        session.connection_id as u64,
                        session
                            .sysvars
                            .lock_wait_timeout
                            .map(std::time::Duration::from_secs)
                            .unwrap_or_else(crate::storage::metadata_wait_timeout),
                        allow_partition_dml,
                    )),
                )?;
                if let Some(op) = op_label {
                    maintenance_rows.push(vec![
                        Value::String(table_name.clone()),
                        Value::String(op.to_string()),
                        Value::String("status".to_string()),
                        Value::String("OK".to_string()),
                    ]);
                }
            }
            revalidate_altered_table_views(db, session, &alter_db, &alter_name)?;
            if !maintenance_rows.is_empty() {
                let columns = ["Table", "Op", "Msg_type", "Msg_text"]
                    .into_iter()
                    .map(|name| crate::types::Column {
                        name: name.to_string(),
                        column_type: crate::types::ColumnType::VarString,
                        source: Default::default(),
                    })
                    .collect();
                return Ok(QueryResponse::ResultSet(crate::engine::api::ResultSet {
                    columns,
                    rows: maintenance_rows,
                }));
            }
            Ok(QueryResponse::Ok)
        }
        Statement::DropTable {
            names,
            if_exists,
            temporary,
            ..
        } => {
            let temp_name_count = names
                .iter()
                .filter(|name| session.resolve_temp_table_name(name).is_some())
                .count();
            // `DROP TEMPORARY TABLE` names only temporary tables. A plain
            // `DROP TABLE` (no TEMPORARY) may name a mix: MySQL 8.0 drops the
            // temporary table when one exists with that name (temp takes
            // precedence), otherwise the base table — all in one statement.
            if temporary {
                // DROP TEMPORARY is the explicit no-implicit-commit form. The
                // successful drop itself is still not rollbackable.
                return execute_temp_ddl_in_transaction(db, session, |_, session| {
                    for name in names {
                        let visible_key = session.temp_visible_key(&name);
                        let Some(temp) = session.temp_tables.remove(&visible_key) else {
                            if if_exists {
                                // MySQL emits Note 1051 for the skipped table.
                                crate::engine::api::push_statement_warning(
                                    crate::error::ConditionLevel::Note,
                                    crate::error::catalog::ER_BAD_TABLE_ERROR,
                                    format!(
                                        "Unknown table '{}'",
                                        qualify_persistent_table_name(session, &name)
                                    ),
                                );
                                continue;
                            }
                            return Err(error(
                                crate::error::catalog::ER_BAD_TABLE_ERROR,
                                format!(
                                    "Unknown table '{}'",
                                    qualify_persistent_table_name(session, &name)
                                ),
                            ));
                        };
                        session.temp_table_journal.push(
                            crate::engine::api::TempTableJournalEntry::Drop {
                                visible_key,
                                internal_key: temp.internal_key,
                                on_commit: temp.on_commit,
                            },
                        );
                    }
                    Ok(QueryResponse::Ok)
                });
            }
            if temp_name_count == names.len() {
                // Plain DROP TABLE, even when every resolved object happens
                // to be temporary, is an implicit-commit statement.
                implicit_commit_for_ddl(db, session)?;
                for name in names {
                    let visible_key = session.temp_visible_key(&name);
                    let Some(temp) = session.temp_tables.remove(&visible_key) else {
                        if if_exists {
                            crate::engine::api::push_statement_warning(
                                crate::error::ConditionLevel::Note,
                                crate::error::catalog::ER_BAD_TABLE_ERROR,
                                format!(
                                    "Unknown table '{}'",
                                    qualify_persistent_table_name(session, &name)
                                ),
                            );
                            continue;
                        }
                        return Err(error(
                            crate::error::catalog::ER_BAD_TABLE_ERROR,
                            format!(
                                "Unknown table '{}'",
                                qualify_persistent_table_name(session, &name)
                            ),
                        ));
                    };
                    db.drop_temp_table(&temp.internal_key)?;
                }
                return Ok(QueryResponse::Ok);
            }
            // Mixed (or pure-persistent) `DROP TABLE`: split the names by whether
            // they resolve to a session temporary table. A non-TEMPORARY DROP
            // TABLE implicitly commits regardless of what it names.
            let (temp_names, persistent_names): (Vec<String>, Vec<String>) = names
                .iter()
                .cloned()
                .partition(|name| session.resolve_temp_table_name(name).is_some());
            for name in &persistent_names {
                let qualified = qualify_persistent_table_name(session, name);
                if is_reserved_system_table_name(&qualified) {
                    return Err(error(
                        crate::error::catalog::ER_NO_SYSTEM_TABLE_ACCESS,
                        format!("Cannot drop reserved system table '{qualified}'"),
                    ));
                }
            }
            implicit_commit_for_ddl(db, session)?;
            if !persistent_names.is_empty() {
                let qualified = persistent_names
                    .iter()
                    .map(|name| qualify_persistent_table_name(session, name))
                    .collect::<Vec<_>>();
                db.drop_tables(&qualified, if_exists)?;
                for name in &persistent_names {
                    let (drop_db, drop_name) =
                        crate::storage::split_table_name(&session.current_database, name);
                    let _ = db.invalidate_views_referencing(&drop_db, &drop_name);
                }
                // When this statement also names a temporary table, the whole
                // `Statement::DropTable` is classified as temp-DDL, so the
                // generic DDL binlog audit skips it. Emit a persistent-only DROP
                // so the base-table removal still replicates (MySQL logs the
                // temporary and base parts as separate events).
                if !temp_names.is_empty() && crate::binlog::enabled() {
                    let if_exists_clause = if if_exists { "IF EXISTS " } else { "" };
                    let list = persistent_names
                        .iter()
                        .map(|name| {
                            let (d, t) =
                                crate::storage::split_table_name(&session.current_database, name);
                            format!("`{d}`.`{t}`")
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    let sql = format!("DROP TABLE {if_exists_clause}{list}");
                    let ctx = super::binlog_ctx_from_session(session);
                    let _ = crate::binlog::record_ddl_autocommit_with(
                        &session.current_database,
                        &sql,
                        &ctx,
                    );
                }
            }
            if !temp_names.is_empty() {
                for name in &temp_names {
                    let visible_key = session.temp_visible_key(name);
                    if let Some(temp) = session.temp_tables.remove(&visible_key) {
                        db.drop_temp_table(&temp.internal_key)?;
                    }
                }
            }
            Ok(QueryResponse::Ok)
        }
        Statement::TruncateTable { name } => {
            if let Some(internal_key) = session.resolve_temp_table_name(&name) {
                implicit_commit_for_ddl(db, session)?;
                db.truncate_temp_table(&internal_key)?;
                return Ok(QueryResponse::Ok);
            }
            let (schema, table) =
                crate::storage::split_table_name(&session.current_database, &name);
            if schema.eq_ignore_ascii_case("performance_schema") {
                crate::engine::perf_schema::truncate(
                    &table,
                    registry,
                    &session.user,
                    &session.host,
                )?;
                return Ok(QueryResponse::Ok);
            }
            if schema.eq_ignore_ascii_case("mysql")
                && crate::engine::logging::truncate_log_table(db.data_dir(), &table)?
            {
                return Ok(QueryResponse::Ok);
            }
            if crate::engine::sys_schema::is_sys_config(&schema, &table) {
                implicit_commit_for_ddl(db, session)?;
                crate::engine::sys_schema::truncate_sys_config(db)?;
                return Ok(QueryResponse::Ok);
            }
            if crate::storage::is_system_database(&schema) {
                return Err(error(
                    crate::error::catalog::ER_SIGNAL_EXCEPTION,
                    format!("cannot TRUNCATE system table '{name}'"),
                ));
            }
            implicit_commit_for_ddl(db, session)?;
            db.truncate_table(&qualify_persistent_table_name(session, &name))?;
            Ok(QueryResponse::Ok)
        }
        Statement::CreateIndex(stmt) => {
            if !matches!(
                stmt.algorithm,
                AlterAlgorithm::Default | AlterAlgorithm::Inplace
            ) {
                return Err(error(
                    crate::error::catalog::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON,
                    "CREATE INDEX supports ALGORITHM=DEFAULT or INPLACE; use ALTER TABLE for COPY",
                ));
            }
            if matches!(stmt.lock, AlterLock::None | AlterLock::Shared)
                && !stmt.index_type.eq_ignore_ascii_case("BTREE")
            {
                return Err(error(
                    crate::error::catalog::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON,
                    "LOCK=NONE/SHARED requires a BTREE index",
                ));
            }
            if is_session_temp_table_ref(session, &stmt.table) {
                implicit_commit_for_ddl(db, session)?;
                return execute_create_temp_index(db, session, stmt);
            }
            implicit_commit_for_ddl(db, session)?;
            let CreateIndex {
                name,
                table,
                algorithm: _,
                lock,
                columns,
                column_prefix_lengths,
                column_descending,
                key_expressions: parser_key_expressions,
                key_expression_multi_valued: parser_key_expression_multi_valued,
                unique,
                included_columns,
                index_type,
                visible,
                comment,
                fulltext_parser,
                vector_options,
                query_hints,
                hint_warnings: _,
            } = stmt;
            reject_parallel_hint_for_non_btree(&query_hints, &index_type)?;
            let index_build_options = index_build_options_from_hints(&query_hints);
            // Standalone `CREATE FULLTEXT INDEX ... WITH PARSER ngram` must
            // persist the tokenizer, mirroring the inline / ALTER paths.
            let fulltext = index_type.eq_ignore_ascii_case("FULLTEXT").then(|| {
                let mut meta = crate::storage::FulltextMeta {
                    parser: fulltext_parser.clone(),
                    ..Default::default()
                };
                crate::storage::fulltext::snapshot_tokenizer_config_into(&mut meta);
                meta
            });
            let column_directions = column_descending
                .iter()
                .map(|desc| {
                    if *desc {
                        crate::storage::IndexColumnDirection::Desc
                    } else {
                        crate::storage::IndexColumnDirection::Asc
                    }
                })
                .collect();
            let qualified_table = qualify_persistent_table_name(session, &table);
            let _ddl_definition = db.pin_ddl_table(&qualified_table)?;
            let key_expressions = build_index_key_expressions(
                db,
                &qualified_table,
                &columns,
                &parser_key_expressions,
                &parser_key_expression_multi_valued,
            )?;
            db.add_indexes_for_sql(
                &qualified_table,
                vec![IndexSchema {
                    readable_from_scn: 0,
                    index_id: 0,
                    name,
                    columns,
                    column_prefix_lengths,
                    column_directions,
                    key_expressions,
                    unique,
                    primary: false,
                    included_columns,
                    state: crate::storage::IndexState::Public,
                    vector: crate::storage::vector_config::VectorIndexConfig::from_options(
                        &index_type,
                        vector_options.as_ref(),
                        &comment,
                    )?,
                    index_type,
                    visible,
                    comment,
                    fulltext,
                }],
                index_build_options,
                session.connection_id as u64,
                matches!(lock, AlterLock::None | AlterLock::Default),
            )?;
            Ok(QueryResponse::Ok)
        }
        Statement::DropIndex { name, table } => {
            if let Some(internal_key) = session.resolve_temp_table_name(&table) {
                implicit_commit_for_ddl(db, session)?;
                let snapshot = db.snapshot_temp_table(&internal_key)?;
                match db.drop_temp_index(&internal_key, &name) {
                    Ok(()) => {
                        db.discard_temp_table_snapshot(snapshot)?;
                        return Ok(QueryResponse::Ok);
                    }
                    Err(err) => {
                        let _ = db.restore_temp_table_snapshot(snapshot);
                        return Err(err);
                    }
                }
            }
            implicit_commit_for_ddl(db, session)?;
            db.drop_index(&qualify_persistent_table_name(session, &table), &name)?;
            Ok(QueryResponse::Ok)
        }
        Statement::AnalyzeTable {
            tables,
            partitions,
            histogram,
        } => {
            implicit_commit_for_ddl(db, session)?;
            if let Some(targets) = partitions {
                if tables.iter().any(|table| session.has_temp_table(table)) {
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "ANALYZE PARTITION does not support temporary tables",
                    ));
                }
                if tables.len() != 1 || histogram.is_some() {
                    return Err(error(
                        crate::error::catalog::ER_WRONG_USAGE,
                        "ANALYZE TABLE PARTITION requires exactly one table and no histogram action",
                    ));
                }
                let table = qualify_table_name(session, &tables[0]);
                let result = db.alter_partition(
                    &table,
                    crate::parser_partition::PartitionAction::AnalyzePartitions(targets),
                );
                let (msg_type, msg_text) = match result {
                    Ok(()) => ("status", "OK".to_string()),
                    Err(err) if err.code() == 1317 => return Err(err),
                    Err(err) => ("Error", err.to_string()),
                };
                let mut rows = vec![vec![
                    Value::String(table.clone()),
                    Value::String("analyze".to_string()),
                    Value::String(msg_type.to_string()),
                    Value::String(msg_text),
                ]];
                if msg_type == "Error" {
                    rows.push(vec![
                        Value::String(table),
                        Value::String("analyze".to_string()),
                        Value::String("status".to_string()),
                        Value::String("Operation failed".to_string()),
                    ]);
                }
                return Ok(QueryResponse::ResultSet(crate::engine::api::ResultSet {
                    columns: vec![
                        col("Table", ColumnType::VarString),
                        col("Op", ColumnType::VarString),
                        col("Msg_type", ColumnType::VarString),
                        col("Msg_text", ColumnType::VarString),
                    ],
                    rows,
                }));
            }
            if histogram.is_some() && tables.len() != 1 {
                return Err(error(
                    crate::error::catalog::ER_WRONG_USAGE,
                    "ANALYZE TABLE histogram operations require exactly one table",
                ));
            }
            let mut combined: Option<crate::engine::api::ResultSet> = None;
            for table in tables {
                let result = match execute_analyze_table(db, session, &table, histogram.as_ref()) {
                    Ok(QueryResponse::ResultSet(result)) => result,
                    Ok(_) => unreachable!("ANALYZE TABLE always returns a result set"),
                    Err(err) if err.code() == 1317 || histogram.is_some() => return Err(err),
                    Err(err) => {
                        let table = qualify_persistent_table_name(session, &table);
                        let mut rows = vec![vec![
                            Value::String(table.clone()),
                            Value::String("analyze".to_string()),
                            Value::String("Error".to_string()),
                            Value::String(err.to_string()),
                        ]];
                        rows.push(vec![
                            Value::String(table),
                            Value::String("analyze".to_string()),
                            Value::String("status".to_string()),
                            Value::String("Operation failed".to_string()),
                        ]);
                        crate::engine::api::ResultSet {
                            columns: vec![
                                col("Table", ColumnType::VarString),
                                col("Op", ColumnType::VarString),
                                col("Msg_type", ColumnType::VarString),
                                col("Msg_text", ColumnType::VarString),
                            ],
                            rows,
                        }
                    }
                };
                if let Some(existing) = &mut combined {
                    existing.rows.extend(result.rows);
                } else {
                    combined = Some(result);
                }
            }
            Ok(QueryResponse::ResultSet(combined.unwrap_or_else(|| {
                crate::engine::api::ResultSet {
                    columns: Vec::new(),
                    rows: Vec::new(),
                }
            })))
        }
        Statement::CheckTable { tables } => {
            // Validate LOB chains and each published physical B-tree. Report
            // structural failures in MySQL's four-column maintenance format.
            implicit_commit_for_ddl(db, session)?;
            execute_table_maintenance_status(db, session, &tables, "check")
        }
        Statement::RepairTable { tables } => {
            // REPAIR TABLE — same story as CHECK: there is nothing for an
            // MVCC engine to repair short of a replica re-clone. Surface
            // MySQL's standard InnoDB note rather than silently no-op.
            implicit_commit_for_ddl(db, session)?;
            execute_table_maintenance_status(db, session, &tables, "repair")
        }
        Statement::OptimizeTable {
            tables,
            no_write_to_binlog,
        } => {
            implicit_commit_for_ddl(db, session)?;
            let _binlog = crate::binlog::SessionSqlLogBinGuard::new(
                session.sql_log_bin && !no_write_to_binlog,
            );
            execute_table_maintenance_status(db, session, &tables, "optimize")
        }
        Statement::ChecksumTable { tables, quick } => {
            // CHECKSUM TABLE — compute a CRC32 rollup over each row's
            // wire-format serialization. Provides the SQL surface that
            // pt-table-checksum and the in-tree mydb-replica-diff tool
            // build on to compare master/replica state without dumping
            // the full data set. Returns two columns: Table / Checksum.
            // QUICK is honoured per MySQL: tables not declared with the
            // CHECKSUM=1 option may return NULL (we conservatively still
            // run the scan, matching MySQL's actual InnoDB behavior).
            let _ = quick;
            execute_checksum_table(db, session, &tables)
        }
        Statement::CreateTablespace { .. }
        | Statement::DropTablespace { .. }
        | Statement::AlterTablespace { .. } => Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "TABLESPACE is not supported",
        )),
        Statement::CreateResourceGroup { .. }
        | Statement::DropResourceGroup { .. }
        | Statement::AlterResourceGroup { .. } => Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "RESOURCE GROUP is not supported",
        )),
        Statement::CreateView {
            name,
            sql,
            query,
            or_replace,
            if_not_exists,
            algorithm,
            definer,
            security_type,
            columns,
            check_option,
        } => {
            let name = name.bind(&session.current_database).catalog_key();
            implicit_commit_for_ddl(db, session)?;
            let qualified = &name;
            if is_reserved_system_table_name(&qualified) {
                if if_not_exists {
                    crate::engine::api::push_statement_warning(
                        crate::error::ConditionLevel::Note,
                        crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                        format!("Table '{name}' already exists"),
                    );
                    return Ok(QueryResponse::Ok);
                }
                return Err(crate::error::messages::table_exists(&name));
            }
            super::routines::validate_definer_privilege(db, session, definer.as_ref())?;
            let view_options = materialize_view_options(session, algorithm, definer, security_type);
            // P3.2 — a missing DEFINER at CREATE time is only a warning (1449);
            // MySQL still creates the view. The error is raised later when the
            // view is read (`effective_view_security_account`).
            warn_if_view_definer_missing(db, &view_options)?;
            let validation = validate_view_definition(
                db,
                session,
                &name,
                view_query_from_query(query),
                &columns,
                &view_options.algorithm,
            )?;
            views::publish_view_definition(
                db,
                session,
                &name,
                &sql,
                validation,
                view_options,
                check_option,
                or_replace,
                if_not_exists,
            )
        }
        Statement::AlterView {
            name,
            sql,
            query,
            algorithm,
            definer,
            security_type,
            columns,
            check_option,
        } => {
            let name = name.bind(&session.current_database).catalog_key();
            implicit_commit_for_ddl(db, session)?;
            if is_reserved_system_table_name(&name) {
                return Err(error(
                    crate::error::catalog::ER_NO_SYSTEM_TABLE_ACCESS,
                    format!("Cannot alter reserved system view '{name}'"),
                ));
            }
            // ALTER VIEW must address an existing view; otherwise raise the
            // same 1146 the SELECT path would.
            let Some(existing_view) = db.view_definition(&session.current_database, &name) else {
                if db.is_base_table(&session.current_database, &name) {
                    return Err(error(
                        crate::error::catalog::ER_WRONG_OBJECT,
                        format!(
                            "'{}' is not VIEW",
                            qualify_persistent_table_name(session, &name)
                        ),
                    ));
                }
                return Err(error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    format!("unknown view '{}'", name),
                ));
            };
            super::routines::validate_definer_privilege(db, session, definer.as_ref())?;
            let view_options = materialize_alter_view_options(
                session,
                &existing_view,
                algorithm,
                definer,
                security_type,
            );
            // P3.2 — DEFINER absence at ALTER time degrades to warning 1449.
            warn_if_view_definer_missing(db, &view_options)?;
            let validation = validate_view_definition(
                db,
                session,
                &name,
                view_query_from_query(query),
                &columns,
                &view_options.algorithm,
            )?;
            views::publish_view_definition(
                db,
                session,
                &name,
                &sql,
                validation,
                view_options,
                check_option,
                true,
                false,
            )
        }
        Statement::DropView { names, if_exists } => {
            let names: Vec<_> = names
                .iter()
                .map(|name| name.bind(&session.current_database).catalog_key())
                .collect();
            implicit_commit_for_ddl(db, session)?;
            db.drop_views(&session.current_database, &names, if_exists)?;
            // Dropped views vanish from the catalog, so plans that expanded
            // them fail view object-dep validation on next lookup and are
            // evicted; only DML memos are cleared conservatively.
            crate::engine::cache::clear_prepared_dml_caches();
            Ok(QueryResponse::Ok)
        }
        Statement::CreateSequence {
            name,
            if_not_exists,
            temporary,
            options,
        } => {
            if options
                .owned_table
                .as_ref()
                .is_some_and(|owner| session.has_temp_table(owner))
            {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "a permanent sequence cannot be owned by a temporary table",
                ));
            }
            implicit_commit_for_ddl(db, session)?;
            if !temporary
                && is_reserved_system_table_name(&qualify_persistent_table_name(session, &name))
            {
                if if_not_exists {
                    crate::engine::api::push_statement_warning(
                        crate::error::ConditionLevel::Note,
                        crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                        format!("Table '{name}' already exists"),
                    );
                    return Ok(QueryResponse::Ok);
                }
                return Err(crate::error::messages::table_exists(&name));
            }
            db.create_sequence(
                &session.current_database,
                &name,
                if_not_exists,
                temporary,
                &options,
            )?;
            Ok(QueryResponse::Ok)
        }
        Statement::AlterSequence {
            name,
            if_exists,
            options,
            restart,
            rename_to,
        } => {
            if options
                .owned_table
                .as_ref()
                .is_some_and(|owner| session.has_temp_table(owner))
            {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "a permanent sequence cannot be owned by a temporary table",
                ));
            }
            implicit_commit_for_ddl(db, session)?;
            let restart_opt = match restart {
                None => None,
                Some(crate::parser::SequenceRestart::ToStart) => Some(None),
                Some(crate::parser::SequenceRestart::To(v)) => Some(Some(v)),
            };
            db.alter_sequence(
                &session.current_database,
                &name,
                if_exists,
                &options,
                restart_opt,
                rename_to.as_deref(),
            )?;
            Ok(QueryResponse::Ok)
        }
        Statement::DropSequence { names, if_exists } => {
            implicit_commit_for_ddl(db, session)?;
            db.drop_sequences(&session.current_database, &names, if_exists)?;
            Ok(QueryResponse::Ok)
        }
        Statement::CreateRoutine(def) => execute_create_routine(db, session, def),
        Statement::CreateTrigger(def) => execute_create_trigger(db, session, def),
        Statement::AlterRoutine {
            kind,
            name,
            characteristics,
        } => execute_alter_routine(db, session, kind, &name, characteristics),
        Statement::DropRoutine {
            kind,
            name,
            if_exists,
        } => execute_drop_routine(db, session, kind, &name, if_exists),
        Statement::DropTrigger { name, if_exists } => {
            execute_drop_trigger(db, session, &name, if_exists)
        }
        Statement::CreateEvent(def) => execute_create_event(db, session, def, registry),
        Statement::AlterEvent(stmt) => execute_alter_event(db, session, stmt),
        Statement::DropEvent { name, if_exists } => {
            execute_drop_event(db, session, &name, if_exists)
        }
        Statement::CreateTableAsSelect {
            table,
            explicit_definition,
            conflict_policy,
            query,
        } => execute_create_table_as_select(
            db,
            session,
            *table,
            explicit_definition,
            conflict_policy,
            query,
            registry,
        ),
        Statement::Insert(stmt) => {
            if let Some(resp) = try_pfs_dml_insert(session, &stmt)? {
                return Ok(resp);
            }
            if let Some(resp) = try_sys_dml_insert(db, session, &stmt)? {
                return Ok(resp);
            }
            reject_system_catalog_dml(db, session, &stmt.table)?;
            // S1 — DELAYED is deprecated since 5.6 and silently downgraded to
            // a regular INSERT on InnoDB. Mirror MySQL's diagnostic so apps
            // depending on the warning surface keep working.
            if stmt.delayed {
                crate::engine::api::push_statement_warning(
                    crate::error::ConditionLevel::Warning,
                    crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
                    "'INSERT DELAYED' is deprecated and will be removed in a future release. \
                     Please use INSERT instead.",
                );
            }
            let view_name = db
                .view_definition(&session.current_database, &stmt.table)
                .map(|view| format!("{}.{}", view.database, view.name));
            let stmt = rewrite_insert_through_view(db, session, stmt)?;
            let result = if is_session_temp_table_ref(session, &stmt.table) {
                execute_temp_in_transaction_with_session(db, session, |db, session, txn| {
                    execute_insert(db, session, txn, stmt, registry)
                })
            } else {
                execute_dml_in_transaction_with_session(db, session, |db, session, txn| {
                    execute_insert(db, session, txn, stmt, registry)
                })
            };
            result.map_err(|err| {
                if err.code() == 1364 {
                    if let Some(view) = view_name {
                        return error(crate::error::catalog::ER_NO_DEFAULT_FOR_VIEW_FIELD,
                            format!("Field of view '{view}' underlying table doesn't have a default value"));
                    }
                }
                err
            })
        }
        Statement::LoadData(stmt) => {
            reject_system_catalog_dml(db, session, &stmt.table)?;
            execute_load_data(db, session, stmt, registry)
        }
        Statement::Select(stmt) => {
            let mut stmt = stmt;
            inline_single_use_with_in_select(db, &mut stmt)?;
            // `sql_select_limit`: inject an implicit LIMIT when the user
            // didn't supply one. `u64::MAX` is the registry's sentinel
            // for "no cap" so we skip the rewrite in that common case
            // to avoid touching the plan-cache key needlessly.
            // P2-C3 SQL_CALC_FOUND_ROWS: snapshot the user-supplied
            // LIMIT/OFFSET *before* the implicit `sql_select_limit` injection
            // below, so the strip-limit pre-pass strips only the real user
            // window and reports the true pre-LIMIT total via `FOUND_ROWS()`.
            let calc_window = if stmt.sql_calc_found_rows {
                Some((stmt.limit.clone(), stmt.offset.clone()))
            } else {
                None
            };
            apply_sql_select_limit(session, &mut stmt);
            if calc_window.is_some() {
                // MySQL 8.0.17 deprecation warning for the modifier itself.
                crate::engine::api::push_statement_warning(
                    crate::error::ConditionLevel::Warning,
                    crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
                    "'SQL_CALC_FOUND_ROWS' is deprecated and will be \
                     removed in a future release. Consider using two \
                     separate queries instead",
                );
            }
            // `max_execution_time`: install a statement deadline. Zero
            // disables the timeout. Guard restores the previous slot on
            // drop so nested SELECTs (subqueries, CTEs) inherit the
            // outermost statement's deadline.
            //
            // P2-C2: `/*+ MAX_EXECUTION_TIME(N) */` hint overrides the
            // session sysvar for this statement only. Picked from the
            // hint list before the sysvar fallback. `ms=0` from the hint
            // explicitly disables the cap (matches MySQL semantics).
            let deadline_ms = select_deadline_ms(&stmt, session);
            let _deadline = crate::engine::cancel::install_query_deadline(
                std::time::Duration::from_millis(deadline_ms),
            );
            if stmt.projection.iter().any(|item| matches!(item, SelectItem::Expr { expr, .. } if crate::engine::ai::sql::expr_has_ai(expr))) { return ai::project(db, session, stmt, registry); }
            refresh_select_stats(db, &stmt)?;
            if let Some((user_limit, user_offset)) = calc_window {
                execute_select_calc_found_rows(db, session, stmt, registry, user_limit, user_offset)
            } else {
                run_select_response(db, session, stmt, registry)
            }
        }
        Statement::SetOperation(stmt) => {
            execute_set_operation_dispatch(db, session, stmt, registry)
        }
        s @ Statement::WithCte(_) => {
            // Keep the whole WITH inside the enclosing MDL scope, including
            // native materialization and parallel fallback dispatch.
            execute_with_cte(db, session, s, registry)
        }
        Statement::Explain {
            target,
            format,
            analyze,
        } => {
            // NOTE(explain-mysql80-compat A1/A2): MySQL 8.0 renders
            // `EXPLAIN ANALYZE` only as TREE and rejects explicit
            // `FORMAT=TRADITIONAL`/`FORMAT=JSON` with error 1235. mydb is a
            // deliberate *superset* here: explicit TRADITIONAL/JSON/TREE are
            // all supported and carry real execution metrics. A bare ANALYZE
            // defaults to TREE, matching the operator-oriented nature of the
            // command. DML ANALYZE is also a documented write-executing mydb
            // extension.
            let resolved_format =
                super::select::resolve_explain_format(format, analyze, session.explain_format);
            if crate::engine::ai::sql::statement_has_ai(&target) {
                return ai::explain(db, session, *target, resolved_format, analyze, registry);
            }
            if analyze {
                execute_explain(db, session, *target, resolved_format, true, registry)
            } else {
                // Plain EXPLAIN prepares and optimizes the target but does not
                // execute it.  Constant evaluation used by planning must
                // therefore not leak data-conversion warnings (for example
                // INET_ATON('bad')) into the statement diagnostics.  Preserve
                // warnings already published for the EXPLAIN text itself,
                // including optimizer-hint diagnostics.
                let explain_warnings = crate::engine::api::take_statement_warnings();
                let result =
                    execute_explain(db, session, *target, resolved_format, false, registry);
                let _planning_warnings = crate::engine::api::take_statement_warnings();
                for warning in explain_warnings {
                    crate::engine::api::push_sql_warning(warning);
                }
                result
            }
        }
        Statement::Update(stmt) => {
            if let Some(resp) = try_pfs_dml_update(session, &stmt)? {
                return Ok(resp);
            }
            if let Some(resp) = try_sys_dml_update(db, session, &stmt)? {
                return Ok(resp);
            }
            reject_system_catalog_dml(db, session, &stmt.table)?;
            let stmt = rewrite_update_through_view(db, session, stmt)?;
            if is_session_temp_table_ref(session, &stmt.table) {
                execute_temp_in_transaction_with_session(db, session, |db, session, txn| {
                    execute_update(db, session, txn, stmt, registry)
                })
            } else {
                execute_in_transaction_with_session(db, session, |db, session, txn| {
                    execute_update(db, session, txn, stmt, registry)
                })
            }
        }
        Statement::Delete(stmt) => {
            if let Some(resp) = try_pfs_dml_delete(session, &stmt)? {
                return Ok(resp);
            }
            if let Some(resp) = try_sys_dml_delete(db, session, &stmt)? {
                return Ok(resp);
            }
            reject_system_catalog_dml(db, session, &stmt.table)?;
            let stmt = rewrite_delete_through_view(db, session, stmt)?;
            if is_session_temp_table_ref(session, &stmt.table) {
                execute_temp_in_transaction_with_session(db, session, |db, session, txn| {
                    execute_delete(db, session, txn, stmt, registry)
                })
            } else {
                execute_in_transaction_with_session(db, session, |db, session, txn| {
                    execute_delete(db, session, txn, stmt, registry)
                })
            }
        }
        Statement::Describe { table } => describe_table(db, session, &table),
        Statement::ShowDatabases { filter } => show_databases(db, session, filter.as_ref()),
        Statement::ShowTables {
            database,
            full,
            filter,
        } => show_tables(
            db,
            session,
            database.as_deref().unwrap_or(&session.current_database),
            full,
            filter.as_ref(),
        ),
        Statement::ShowTableStatus { database, filter } => {
            show_table_status(db, session, database.as_deref(), filter.as_ref())
        }
        Statement::ShowColumns {
            table,
            full,
            filter,
        } => show_columns(db, session, &table, full, filter.as_ref()),
        Statement::ShowIndex { table, filter } => show_index(db, session, &table, filter.as_ref()),
        Statement::ShowCreateDatabase {
            name,
            if_not_exists,
        } => show_create_database(db, &name, if_not_exists),
        Statement::ShowCreateTable { table } => show_create_table(db, session, &table),
        Statement::ShowCreateSequence { name } => {
            crate::engine::show::show_create_sequence(db, session, &name)
        }
        Statement::ShowSequences { database, filter } => {
            crate::engine::show::show_sequences(db, session, database.as_deref(), filter.as_ref())
        }
        Statement::ShowCreateRoutine { kind, name } => {
            show_create_routine(db, session, kind, &name)
        }
        Statement::ShowRoutineStatus { kind, filter } => {
            show_routine_status(db, session, kind, filter.as_ref())
        }
        Statement::ShowCreateTrigger { name } => show_create_trigger(db, session, &name),
        Statement::ShowTriggers { database, filter } => {
            show_triggers(db, session, database.as_deref(), filter.as_ref())
        }
        Statement::ShowCreateEvent { name } => show_create_event(db, session, &name),
        Statement::ShowEvents { database, filter } => {
            show_events(db, session, database.as_deref(), filter.as_ref())
        }
        Statement::Call(call) => execute_call(db, session, call, registry),
        Statement::ShowCharset { filter } => show_charset_result(filter.as_ref()),
        Statement::ShowCollation { filter } => show_collation_result(filter.as_ref()),
        Statement::ShowVariables { scope, filter } => {
            show_variables_result(session, Some(db.as_ref()), scope, filter.as_ref())
        }
        Statement::ShowStatus { scope, filter } => show_status_result(
            session,
            Some(db.as_ref()),
            Some(registry),
            scope,
            filter.as_ref(),
        ),
        Statement::ShowDiagnostics { errors_only, limit } => {
            Ok(show_diagnostics_result(session, errors_only, limit))
        }
        Statement::GetDiagnostics {
            stacked,
            condition,
            items,
        } => super::routines::execute_get_diagnostics(session, stacked, condition.as_ref(), &items),
        Statement::ShowCountDiagnostics { errors_only } => Ok(show_count_result(
            if errors_only {
                "@@session.error_count"
            } else {
                "@@session.warning_count"
            },
            if errors_only {
                session.error_count()
            } else {
                session.warning_count()
            },
        )),
        Statement::ShowEngines => Ok(show_engines_result()),
        Statement::ShowEngineMydbStatus => Ok(show_engine_mydb_status(db)),
        Statement::ShowMydbMemory => Ok(crate::engine::memory_status::summary(db, registry)),
        Statement::ShowMydbMemorySessions => {
            let all = session.internal_system_session
                || db.with_auth_catalog(|auth| {
                    auth.account_has_privilege(
                        &session.authorization_context(),
                        &RequiredPrivilege {
                            privilege: Privilege::Process,
                            scope: PrivilegeScope::Global,
                        },
                        false,
                    )
                })?;
            Ok(crate::engine::memory_status::sessions(
                registry,
                (!all).then_some(session.connection_id),
            ))
        }
        Statement::ShowEngine { engine, kind } => show_engine_result(db, &engine, kind),
        Statement::ShowPlugins => Ok(show_plugins_result()),
        Statement::ShowOpenTables { database, filter } => {
            show_open_tables(db, session, database.as_deref(), filter.as_ref())
        }
        Statement::ShowProcesslist { full } => {
            Ok(show_processlist_result(db, session, registry, full))
        }
        Statement::ShowGrants => show_grants_result(db, session, None, &[]),
        Statement::ShowGrantsFor {
            account,
            using_roles,
        } => show_grants_result(
            db,
            session,
            Some(&show_account_key(&account, session)),
            &using_roles.iter().map(account_key).collect::<Vec<_>>(),
        ),
        Statement::ShowCreateUser { account } => {
            show_create_user_result(db, &show_account_key(&account, session))
        }
        Statement::Kill {
            target,
            connection_id,
        } => {
            crate::engine::cancel::check_cancelled()?;
            let id = u32::try_from(connection_id).map_err(|_| {
                error(
                    crate::error::catalog::ER_NO_SUCH_THREAD,
                    format!("Unknown thread id: {connection_id}"),
                )
            })?;
            kill_session(
                db,
                registry,
                session,
                id,
                matches!(target, KillTarget::Query),
            )?;
            if id == session.connection_id && matches!(target, KillTarget::Connection) {
                session.close_after_response = true;
            }
            Ok(QueryResponse::Ok)
        }
        Statement::CancelDdlJob(job_id) => {
            implicit_commit_for_ddl(db, session)?;
            db.cancel_ddl_job(job_id)?;
            Ok(QueryResponse::Ok)
        }
        Statement::Shutdown => {
            // Privilege already enforced by authorize_statement. Signal the
            // accept loop; the OK is written by the session, which then
            // self-closes on the next command-loop iteration.
            crate::mysql::shutdown::request_shutdown(
                crate::mysql::shutdown::ShutdownSource::Sql {
                    conn_id: session.connection_id,
                    user: session.user.clone(),
                },
                crate::mysql::shutdown::ShutdownIntent::Halt,
            );
            Ok(QueryResponse::Ok)
        }
        Statement::Restart => {
            // Capability gate BEFORE triggering any shutdown so we never
            // shut down without being able to come back (MySQL 3707).
            if !crate::mysql::shutdown::restart_supported() {
                return Err(error(
                    crate::error::catalog::ER_RESTART_SERVER_FAILED,
                    "Restart server failed (mydb in-process restart is unavailable on this \
                     platform, was disabled via MYDB_RESTART_DISABLE, or the executable path could not be resolved)",
                ));
            }
            crate::mysql::shutdown::request_shutdown(
                crate::mysql::shutdown::ShutdownSource::Restart {
                    conn_id: session.connection_id,
                    user: session.user.clone(),
                },
                crate::mysql::shutdown::ShutdownIntent::Restart,
            );
            Ok(QueryResponse::Ok)
        }
        Statement::ShowPrivileges => Ok(show_privileges_result()),
        Statement::CreateUser(stmt) => execute_create_user(db, session, stmt),
        Statement::AlterUser(stmt) => execute_alter_users(db, session, vec![stmt]),
        Statement::AlterUsers(stmts) => execute_alter_users(db, session, stmts),
        Statement::DropUser {
            accounts,
            if_exists,
        } => execute_drop_accounts(db, session, accounts, if_exists, false),
        Statement::RenameUser { renames } => {
            let references = referenced_definers(db);
            let runtime_renames: Vec<_> = renames
                .iter()
                .map(|(from, to)| (account_key(from), account_key(to)))
                .collect();
            db.mutate_auth_catalog(|auth| {
                let sources: std::collections::BTreeSet<_> =
                    renames.iter().map(|(from, _)| account_key(from)).collect();
                let targets: std::collections::BTreeSet<_> =
                    renames.iter().map(|(_, to)| account_key(to)).collect();
                ensure_system_account_access(auth, session, sources.iter())?;
                if sources.len() != renames.len() || targets.len() != renames.len() {
                    return Err(error(
                        crate::error::catalog::ER_CANNOT_USER,
                        "Operation RENAME USER failed: duplicate source or destination account",
                    ));
                }
                // Validate the entire rename set before changing any row.
                // In particular, never remove a source and then overwrite an
                // existing destination account.
                for (from, to) in &renames {
                    let from = account_key(from);
                    let to = account_key(to);
                    validate_account_key(&to)?;
                    check_definer_account_change(auth, session, &references, &from, "RENAME USER")?;
                    check_definer_account_change(auth, session, &references, &to, "RENAME USER")?;
                    if !auth.accounts.contains_key(&account_id(&from))
                        || auth.accounts.contains_key(&account_id(&to))
                    {
                        return Err(error(
                            crate::error::catalog::ER_CANNOT_USER,
                            format!(
                                "Operation RENAME USER failed for {} TO {}",
                                from.display(),
                                to.display()
                            ),
                        ));
                    }
                }
                for (from, to) in renames {
                    let from = account_key(&from);
                    let to = account_key(&to);
                    let Some(mut account) = auth.accounts.remove(&account_id(&from)) else {
                        return Err(error(
                            crate::error::catalog::ER_CANNOT_USER,
                            format!("Operation RENAME USER failed for {}", from.display()),
                        ));
                    };
                    account.key = to.clone();
                    auth.accounts.insert(account_id(&to), account);
                    auth.grants.for_each_mut(|grant| {
                        if grant.grantee == from {
                            grant.grantee = to.clone();
                        }
                        if let PrivilegeScope::Proxy { user, host } = &mut grant.scope {
                            if user == &from.user && host.eq_ignore_ascii_case(&from.host) {
                                *user = to.user.clone();
                                *host = to.host.clone();
                            }
                        }
                    });
                    auth.role_edges.for_each_mut(|edge| {
                        if edge.from_role == from {
                            edge.from_role = to.clone();
                        }
                        if edge.to_account == from {
                            edge.to_account = to.clone();
                        }
                    });
                    auth.default_roles.for_each_mut(|edge| {
                        if edge.from_role == from {
                            edge.from_role = to.clone();
                        }
                        if edge.to_account == from {
                            edge.to_account = to.clone();
                        }
                    });
                    auth.restrictions.for_each_mut(|restriction| {
                        if restriction.grantee == from {
                            restriction.grantee = to.clone();
                        }
                    });
                }
                Ok(())
            })?;
            for (from, to) in runtime_renames {
                crate::engine::auth::failed_login_rename(&from, &to);
                crate::engine::auth::rename_user_resources(&from, &to);
                crate::engine::auth::auth_cache_forget(&from);
                crate::engine::auth::auth_cache_forget(&to);
            }
            Ok(QueryResponse::Ok)
        }
        Statement::CreateRole {
            roles,
            if_not_exists,
        } => execute_create_user(
            db,
            session,
            crate::parser::CreateUser {
                accounts: roles
                    .into_iter()
                    .map(|account| crate::parser::UserSpec {
                        account,
                        password: None,
                        random_password: false,
                        plugin: None,
                        password_is_pre_hashed: false,
                        account_locked: true,
                        password_expired: false,
                        ssl_requirement: None,
                        resource_options: Default::default(),
                        failed_login_attempts: None,
                        password_lock_time_days: None,
                        password_lifetime_days: None,
                        password_history: None,
                        password_reuse_interval_days: None,
                        password_require_current: None,
                        comment: None,
                        attribute_json: None,
                    })
                    .collect(),
                if_not_exists,
                is_role: true,
                default_roles: Vec::new(),
            },
        ),
        Statement::DropRole { roles, if_exists } => {
            execute_drop_accounts(db, session, roles, if_exists, true)
        }
        Statement::Grant(stmt) => execute_grant(db, session, stmt),
        Statement::Revoke(stmt) => execute_revoke(db, session, stmt),
        Statement::SetRole(stmt) => execute_set_role(db, session, stmt),
        Statement::SetPassword {
            account,
            password,
            random_password,
            replace,
            retain_current,
        } => execute_set_password(
            db,
            session,
            account,
            password,
            random_password,
            replace,
            retain_current,
        ),
        Statement::SetDefaultRole(stmt) => execute_set_default_role(db, session, stmt),
        Statement::FlushPrivileges => {
            implicit_commit_for_ddl(db, session)?;
            // mydb does not read grants from `mysql.*` tables — the catalog
            // is the single source of truth — so FLUSH PRIVILEGES has no
            // "reload from table" semantics. We bump the auth epoch anyway
            // so any prepared statement re-validates next execution
            // (matches the spirit of MySQL FLUSH PRIVILEGES, post-changes).
            db.mutate_auth_catalog(|_| Ok(()))?;
            crate::engine::auth::auth_cache_clear();
            crate::engine::auth::flush_all_failed_logins();
            Ok(QueryResponse::Ok)
        }
        Statement::FlushUserResources => {
            crate::engine::auth::flush_user_resources();
            Ok(QueryResponse::Ok)
        }
        // (helpers below)
        Statement::FlushHosts => {
            implicit_commit_for_ddl(db, session)?;
            // mydb does not maintain a host-resolution cache; the
            // equivalent state in MySQL — blocked-host tracking after
            // repeated handshake failures — lives on the per-account
            // `failed_login_count` for mydb. Resetting those gives FLUSH
            // HOSTS a meaningful effect parallel to MySQL.
            crate::engine::auth::flush_all_failed_login_counts(db)?;
            Ok(QueryResponse::Ok)
        }
        Statement::FlushStatus => {
            implicit_commit_for_ddl(db, session)?;
            // MySQL 8.0: FLUSH STATUS resets a small set of process-global
            // counters (Aborted_*, Max_used_connections-style peaks). For
            // mydb we only have meaningful peaks on connection metrics, so
            // reset those; running totals (Innodb_*) are left intact —
            // same as MySQL 8.0 (where most Innodb_* counters survive).
            crate::mysql::connection_manager::reset_status_peaks();
            registry.flush_session_status();
            crate::engine::status_counters::record_flush_status();
            Ok(QueryResponse::Ok)
        }
        Statement::FlushTables {
            tables,
            with_read_lock,
            for_export,
        } => {
            implicit_commit_for_ddl(db, session)?;
            if for_export {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "FLUSH TABLES ... FOR EXPORT is not yet supported",
                ));
            }
            if with_read_lock {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "FLUSH TABLES WITH READ LOCK is not yet supported \
                     (global read lock not wired through DDL/DML interlocks); \
                     use a serializable READ-ONLY transaction or a base \
                     backup snapshot instead",
                ));
            }
            // Validate each named table exists before issuing the flush —
            // matches MySQL which raises 1146 ER_NO_SUCH_TABLE on unknown
            // names. The bare form (no list) flushes everything dirty.
            if !tables.is_empty() {
                for tbl in &tables {
                    let (db_name, table_name) =
                        crate::storage::split_table_name(&session.current_database, tbl);
                    db.table_in_database(&db_name, &table_name)?;
                }
            }
            db.flush_dirty_data()?;
            crate::engine::status_counters::record_flush_tables();
            Ok(QueryResponse::Ok)
        }
        Statement::FlushQueryCache => {
            implicit_commit_for_ddl(db, session)?;
            // Query cache was removed in MySQL 8.0. Accept the statement
            // for compatibility with init scripts that pre-date 8.0, but
            // emit the same deprecation warning MySQL 8.0 raises for
            // legacy query-cache syntax.
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
                "'FLUSH QUERY CACHE' is deprecated and will be removed in \
                 a future release; the query cache was removed in MySQL 8.0",
            );
            Ok(QueryResponse::Ok)
        }
        Statement::FlushLogs { kind } => {
            implicit_commit_for_ddl(db, session)?;
            if kind == crate::engine::logging::FlushLogKind::All {
                if let Some(manager) = crate::binlog::manager() {
                    manager.rotate()?;
                }
            }
            if let Some(service) = crate::engine::logging::active_service() {
                service.flush(kind)?;
            }
            Ok(QueryResponse::Ok)
        }
        Statement::InstallComponent { names } => execute_component(db, session, &names, true),
        Statement::UninstallComponent { names } => execute_component(db, session, &names, false),
        Statement::Noop { .. } => Ok(QueryResponse::Ok),
        Statement::Do(exprs) => execute_do_statement(db, session, exprs, registry),
        Statement::Unsupported { kind, .. } => Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("mydb parsed this SQL but does not execute it yet: {kind}"),
        )),
        Statement::ExplainForConnection {
            connection_id,
            format,
        } => execute_explain_for_connection(db, session, connection_id, format, registry),
        Statement::SelectIntoVars { inner, targets } => {
            execute_select_into_vars(db, session, inner, targets, registry)
        }
        Statement::SelectIntoFile { inner, options } => {
            execute_select_into_file(db, session, inner, options, registry)
        }
        Statement::RenameTables { pairs } => {
            // MySQL: with tables held by LOCK TABLES, RENAME TABLE is rejected
            // (ER_LOCK_OR_ACTIVE_TRANSACTION 1192) — the session must UNLOCK
            // first. Reject honestly rather than renaming under a stale lock.
            if session.has_explicit_table_locks() {
                return Err(error(crate::error::catalog::ER_LOCK_OR_ACTIVE_TRANSACTION,
                    "Can't execute the given command because you have active locked tables or an active transaction",
                ));
            }
            // RENAME TABLE is DDL — implicitly commit any pending transaction.
            implicit_commit_for_ddl(db, session)?;
            // MySQL: `RENAME TABLE` operates only on the persistent base-table
            // namespace — it never renames a TEMPORARY table (use
            // `ALTER TABLE ... RENAME TO` for those). If a source name resolves
            // to a session temp table with no shadowing base table, reject with
            // a clear pointer instead of a bare "table does not exist".
            for (old, _) in &pairs {
                let visible_key = session.temp_visible_key(old);
                if session.temp_tables.contains_key(&visible_key)
                    && !db.has_persistent_table_or_view(&session.current_database, old)
                {
                    return Err(error(crate::error::catalog::ER_NO_SUCH_TABLE,
                        format!(
                            "Table '{old}' is a TEMPORARY table; use ALTER TABLE ... RENAME TO to rename it"
                        ),
                    ));
                }
            }
            // Resolve unqualified names against the current database so
            // `RENAME TABLE t1 TO t2` works in `USE mydb`.
            let resolved: Vec<(String, String)> = pairs
                .into_iter()
                .map(|(o, n)| {
                    let (od, ot) = crate::storage::split_table_name(&session.current_database, &o);
                    let (nd, nt) = crate::storage::split_table_name(&session.current_database, &n);
                    (format!("{od}.{ot}"), format!("{nd}.{nt}"))
                })
                .collect();
            for (old, new) in &resolved {
                if is_reserved_system_table_name(old) {
                    return Err(error(
                        crate::error::catalog::ER_NO_SYSTEM_TABLE_ACCESS,
                        format!("Cannot rename reserved system table '{old}'"),
                    ));
                }
                if is_reserved_system_table_name(new) {
                    return Err(crate::error::messages::table_exists(new));
                }
            }
            db.rename_tables_atomic(&resolved)?;
            for (_, name) in &resolved {
                let (database, table) =
                    crate::storage::split_table_name(&session.current_database, name);
                revalidate_altered_table_views(db, session, &database, &table)?;
            }
            Ok(QueryResponse::Ok)
        }
        Statement::XaStart { .. }
        | Statement::XaEnd { .. }
        | Statement::XaPrepare { .. }
        | Statement::XaCommit { .. }
        | Statement::XaRollback { .. }
        | Statement::XaRecover { .. } => execute_xa_statement(db, session, statement),
        Statement::DeclareSqlCursor { name, query, sql } => {
            execute_declare_sql_cursor(db, session, name, query, sql, registry)
        }
        Statement::FetchSqlCursor { name, count } => execute_fetch_sql_cursor(session, name, count),
        Statement::CloseSqlCursor { name } => execute_close_sql_cursor(session, name),
        Statement::MultiStatement { sql, .. } => {
            // CLIENT_MULTI_STATEMENTS: one COM_QUERY carrying several
            // `;`-separated statements. Re-split via parse_statements (which
            // converts each), execute them in order against the same session,
            // and return one result per statement as a MultiResult — the
            // session layer sends each with SERVER_MORE_RESULTS_EXISTS set on
            // all but the last. A failing statement aborts the batch; the
            // effects of already-executed non-transactional statements remain,
            // matching MySQL. Nested MultiResults (e.g. a CALL among the
            // statements) are flattened so the wire layer never sees nesting.
            let statements =
                crate::parser::parse_statements(&sql).map_err(super::parse_error_to_query_error)?;
            let additional = statements.len().saturating_sub(1) as u64;
            crate::mysql::connection_manager::record_statements(additional);
            crate::engine::status_counters::add_session_counter(
                session,
                crate::engine::status_counters::SessionCounter::Questions,
                additional,
            );
            crate::engine::status_counters::add_session_counter(
                session,
                crate::engine::status_counters::SessionCounter::Queries,
                additional,
            );
            let mut results = Vec::with_capacity(statements.len());
            for statement in statements {
                if let Some(kind) =
                    crate::engine::status_counters::classify_statement_com(&statement)
                {
                    crate::engine::status_counters::record_session_com(session, kind);
                }
                match execute_statement(db, session, statement, registry)? {
                    QueryResponse::MultiResult(inner) => results.extend(inner),
                    other => results.push(other),
                }
            }
            Ok(QueryResponse::MultiResult(results))
        }
    }
}

/// Implement `EXPLAIN [FORMAT=...] FOR CONNECTION <conn_id>`. The target
/// connection's current statement (the `info` field tracked by the session
/// registry) is re-parsed, then handed to the regular EXPLAIN executor. mydb
/// produces the planner's view; it does NOT execute the target statement.
/// Requires the PROCESS privilege.
fn execute_explain_for_connection(
    db: &Arc<Database>,
    session: &mut SessionContext,
    connection_id: u32,
    format: Option<crate::parser::ExplainFormat>,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    if connection_id != session.connection_id
        && !session.internal_system_session
        && !db.auth_catalog().account_has_privilege(
            &session.authorization_context(),
            &RequiredPrivilege {
                privilege: Privilege::Process,
                scope: PrivilegeScope::Global,
            },
            false,
        )
    {
        return Err(error(
            crate::error::catalog::ER_SPECIFIC_ACCESS_DENIED_ERROR,
            "Access denied; PROCESS privilege required for EXPLAIN FOR CONNECTION",
        ));
    }
    let processes = registry.snapshot();
    let Some(target) = processes.into_iter().find(|p| p.id == connection_id) else {
        return Err(error(
            crate::error::catalog::ER_NO_SUCH_THREAD,
            format!("Unknown thread id: {connection_id}"),
        ));
    };
    let current_sql = target.info.unwrap_or_default();
    let current_sql = current_sql.trim();
    if current_sql.is_empty() {
        return Err(error(
            crate::error::catalog::ER_NO_SUCH_THREAD,
            format!("connection {connection_id} has no active statement to explain"),
        ));
    }
    let mut inner =
        crate::parser::parse_statement(current_sql).map_err(super::parse_error_to_query_error)?;
    // EXPLAIN FOR CONNECTION uses the target connection's default database
    // and only applies the PROCESS/self check above. It must not accidentally
    // demand the caller's privileges on every object referenced by the target
    // statement, and it must never execute an EXPLAIN ANALYZE found in INFO.
    let saved_database = std::mem::replace(
        &mut session.current_database,
        target.database.unwrap_or_default(),
    );
    let result = (|| {
        expand_views(db, session, &mut inner, 0)?;
        qualify_statement_tables(db, session, &mut inner)?;
        let resolved_format = format.unwrap_or(session.explain_format);
        super::select::execute_explain(db, session, inner, resolved_format, false, registry)
    })();
    session.current_database = saved_database;
    result
}

/// Implement `SELECT ... INTO @v1, @v2 [FROM ...]`. The inner statement is
/// already a regular Statement (Select / SetOperation / WithCte). We materialise
/// its result, check shape, and bind the row's columns to user variables.
/// MySQL semantics:
///   • 0 rows: variables retain their previous value (no error).
///   • 1 row: column count must match target count (error 1222 / 1241 otherwise).
///   • ≥2 rows: error 1172 ("Result consisted of more than one row").
/// MySQL `DO expr [, expr] ...` (13.2.3). Evaluate each expression for its
/// side effects (typically `RELEASE_LOCK()` / `GET_LOCK()` / `SLEEP()`),
/// discard the result, and return an OK packet — `DO` deliberately bypasses
/// the result-set protocol so a stored-program caller can ignore a value
/// that a bare SELECT would have to fetch. We synthesise a projection-only
/// `Statement::Select` (no FROM, no WHERE) and drain its rows so the side
/// effects fire exactly once per expression in declared order.
fn execute_do_statement(
    db: &Arc<Database>,
    session: &mut SessionContext,
    exprs: Vec<crate::parser::Expr>,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    use crate::parser::{Select, SelectItem, Statement};
    if exprs.iter().all(crate::engine::expr::expr_is_constant)
        && !exprs.iter().any(super::select::expr_has_toplevel_aggregate)
    {
        for expr in &exprs {
            // Preserve bind-time error precedence even on the constant DO
            // shortcut. In particular, REGEXP/GIS and numeric/GIS operands
            // must reject the structured SQL type (1210) before evaluating a
            // malformed geometry constructor (3037). These validation passes
            // may evaluate constants to establish error precedence; suppress
            // their incidental conversion warnings so a nested MAKE_SET,
            // BETWEEN or collation aggregation cannot multiply diagnostics.
            crate::engine::expr::with_suppressed_expression_warnings(|| {
                crate::engine::expr::validate_static_expr_numeric_operands(expr)
            })?;
            crate::engine::expr::with_suppressed_expression_warnings(|| {
                crate::engine::expr::validate_static_expr_collations(expr)
            })?;
            // Text-protocol MySQL deliberately diagnoses a malformed
            // constant string extent once while fixing these functions and
            // once again during evaluation. Keep that narrow bind-time
            // contract separate from the general validators above.
            crate::engine::expr::publish_do_static_string_extent_warnings(expr);
            let _ = crate::engine::expr::eval_const(expr)?;
        }
        return Ok(QueryResponse::Ok);
    }
    let projection = exprs
        .into_iter()
        .map(|expr| SelectItem::Expr { expr, alias: None })
        .collect();
    let synthetic_select = Select {
        projection,
        ..Select::default()
    };
    let response = execute_statement(db, session, Statement::Select(synthetic_select), registry)?
        .materialize()?;
    // Drain — DO discards the materialised rows on purpose. Any error
    // raised by an expression has already propagated up via `?`.
    drop(response);
    Ok(QueryResponse::Ok)
}

fn execute_select_into_vars(
    db: &Arc<Database>,
    session: &mut SessionContext,
    inner: crate::parser::QueryExpr,
    targets: Vec<String>,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    if targets.is_empty() {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "SELECT ... INTO requires at least one target variable",
        ));
    }
    let response = execute_statement(db, session, inner.into_statement(), registry)?;
    let Some(row) = take_single_into_row(response)? else {
        crate::engine::api::push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_SP_FETCH_NO_DATA,
            "No data - zero rows fetched, selected, or processed",
        );
        return Ok(QueryResponse::Ok);
    };
    if row.len() != targets.len() {
        return Err(error(crate::error::catalog::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT,
            format!(
                "The used SELECT statements have a different number of columns ({}) than INTO targets ({})",
                row.len(),
                targets.len()
            ),
        ));
    }
    for (name, value) in targets.iter().zip(row.into_iter()) {
        session.set_user_variable(name, value);
    }
    Ok(QueryResponse::Ok)
}

/// SELECT INTO needs one row or a cardinality error. Do not drain a large
/// streaming result merely to prove that it has more than one row.
pub(super) fn take_single_into_row(
    response: QueryResponse,
) -> Result<Option<Vec<Value>>, QueryError> {
    let too_many = || {
        error(
            crate::error::catalog::ER_TOO_MANY_ROWS,
            "Result consisted of more than one row",
        )
    };
    let from_rows = |rows: Vec<Vec<Value>>| {
        if rows.len() > 1 {
            Err(too_many())
        } else {
            Ok(rows.into_iter().next())
        }
    };
    match response {
        QueryResponse::ResultSet(result) => from_rows(result.rows),
        QueryResponse::SharedResultSet(result) => from_rows(result.rows),
        QueryResponse::StreamingResultSet(mut stream) => {
            let result = (|| {
                let first = stream.cursor.next_row()?;
                if first.is_some() && stream.cursor.next_row()?.is_some() {
                    return Err(too_many());
                }
                Ok(first)
            })();
            crate::engine::api::append_statement_warnings(stream.cursor.take_warnings());
            result
        }
        other => Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            format!(
                "SELECT ... INTO target was not a result set ({})",
                response_kind(&other)
            ),
        )),
    }
}

fn response_kind(response: &QueryResponse) -> &'static str {
    match response {
        QueryResponse::Ok => "Ok",
        QueryResponse::OkAffected { .. } => "OkAffected",
        QueryResponse::ResultSet(_) => "ResultSet",
        QueryResponse::SharedResultSet(_) => "SharedResultSet",
        QueryResponse::StreamingResultSet(_) => "StreamingResultSet",
        QueryResponse::MultiResult(_) => "MultiResult",
    }
}

fn map_window_name_to_vec(name: &str) -> Option<crate::engine::executor::vec::window::WindowFn> {
    use crate::engine::executor::vec::window::WindowFn;
    // Functions whose vec output is byte-identical to the legacy
    // `evaluate_window_per_partition` path. COUNT (always `Value::Int`)
    // and MIN/MAX (pass-through of the input value) match trivially.
    //
    // SUM/AVG now share the legacy `NumericAggState` kernel inside
    // `vec::window::run_window_partition`, so an exact (integer/decimal)
    // argument yields the same `DecimalCompact`/`Int` (and AVG keeps the
    // `div_precision_increment` scale via `finalize_avg_value`). The
    // sliding subtract is exact for i128 but would drift for f64, and a
    // mixed-scale argument would diverge from the legacy per-frame scale,
    // so `try_vec_window_fast_path` only routes SUM/AVG when every
    // argument value is exact and shares one scale — otherwise it falls
    // back here to the legacy path.
    Some(match name {
        "ROW_NUMBER" => WindowFn::RowNumber,
        "RANK" => WindowFn::Rank,
        "DENSE_RANK" => WindowFn::DenseRank,
        "COUNT" => WindowFn::Count,
        "MIN" => WindowFn::Min,
        "MAX" => WindowFn::Max,
        "SUM" => WindowFn::Sum,
        "AVG" => WindowFn::Avg,
        _ => return None,
    })
}

/// Evaluate a statement-constant frame offset, including bound parameters,
/// as a `usize`. Invalid shapes fall back to the validating row path.
fn const_window_offset(expr: &Expr) -> Option<usize> {
    usize::try_from(window_const_i64(expr).ok()?).ok()
}

fn map_rows_frame(frame: &WindowFrame) -> Option<crate::engine::executor::vec::window::RowsFrame> {
    use crate::engine::executor::vec::window::RowsFrame;
    // ROWS frames only — vec engine does not yet handle RANGE/GROUPS.
    if !matches!(frame.units, WindowFrameUnits::Rows) {
        return None;
    }
    if !matches!(
        frame.exclusion,
        crate::parser::WindowFrameExclusion::NoOthers
    ) {
        return None;
    }
    let preceding = match &frame.start {
        WindowFrameBound::UnboundedPreceding => None,
        WindowFrameBound::CurrentRow => Some(0usize),
        WindowFrameBound::Preceding(e) => Some(const_window_offset(e)?),
        // start = Following → empty frame in standard semantics; rare,
        // safer to fall back.
        WindowFrameBound::UnboundedFollowing => return None,
        WindowFrameBound::Following(_) => return None,
    };
    let following = match &frame.end {
        WindowFrameBound::UnboundedFollowing => None,
        WindowFrameBound::CurrentRow => Some(0usize),
        WindowFrameBound::Following(e) => Some(const_window_offset(e)?),
        WindowFrameBound::UnboundedPreceding => return None,
        WindowFrameBound::Preceding(_) => return None,
    };
    Some(RowsFrame {
        preceding,
        following,
    })
}

/// Try to evaluate a window function via the incremental vec engine.
/// Returns `Ok(Some(values))` (length = rows.len(), scattered by partition)
/// when the shape matches; `Ok(None)` otherwise so the caller continues
/// with the legacy match.
fn try_vec_window_fast_path(
    upper: &str,
    args: &[Expr],
    frame: &WindowFrame,
    order_by: &[OrderBy],
    partitions: &[Vec<usize>],
    columns: &[Column],
    rows: &[Vec<Value>],
) -> Result<Option<Vec<Value>>, QueryError> {
    let Some(win_fn) = map_window_name_to_vec(upper) else {
        return Ok(None);
    };
    let ignore_frame = matches!(
        win_fn,
        crate::engine::executor::vec::window::WindowFn::RowNumber
            | crate::engine::executor::vec::window::WindowFn::Rank
            | crate::engine::executor::vec::window::WindowFn::DenseRank
    );
    // Frame shape gate — only applies to aggregate-style functions.
    let rows_frame = if ignore_frame {
        crate::engine::executor::vec::window::RowsFrame::whole_partition()
    } else {
        match map_rows_frame(frame) {
            Some(f) => f,
            None => return Ok(None),
        }
    };
    // Argument shape gate.
    use crate::engine::executor::vec::window::WindowFn;
    let needs_col = !matches!(
        win_fn,
        WindowFn::RowNumber | WindowFn::Rank | WindowFn::DenseRank
    );
    let count_star = matches!(win_fn, WindowFn::Count)
        && (args.is_empty() || args.iter().any(|a| matches!(a, Expr::Wildcard)));
    let arg_expr: Option<&Expr> = if needs_col && !count_star {
        match args.first() {
            Some(e) if !matches!(e, Expr::Wildcard) => Some(e),
            _ => return Ok(None),
        }
    } else {
        None
    };
    let arg_pos = arg_expr.and_then(|arg| expr_column_position(arg, columns));
    let order_positions: Vec<Option<usize>> =
        if matches!(win_fn, WindowFn::Rank | WindowFn::DenseRank) && !order_by.is_empty() {
            order_by
                .iter()
                .map(|o| expr_column_position(&o.expr, columns))
                .collect()
        } else {
            Vec::new()
        };

    // Evaluate one partition. Returns `Ok(None)` to bail the *entire* vec
    // fast path to the legacy evaluator (SUM/AVG non-exact / mixed-scale
    // gate). `Ok(Some(part_out))` is the per-partition-position result,
    // scattered back by the caller via `partition[pos] -> part_out[pos]`.
    let eval_one = |partition: &[usize]| -> Result<Option<Vec<Value>>, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        let n = partition.len();
        // Column values per row in this partition. For COUNT(*) we feed
        // an `Int(1)` per row so the increment counts visible rows; the
        // sliding-window code treats only NULL as "skip".
        let column_values: Vec<Value> = if let Some(arg) = arg_expr {
            partition
                .iter()
                .map(|&row_idx| -> Result<Value, QueryError> {
                    crate::engine::cancel::check_cancelled()?;
                    Ok(if let Some(pos) = arg_pos {
                        rows[row_idx].get(pos).cloned().unwrap_or(Value::Null)
                    } else {
                        eval_expr_against_columns(arg, columns, &rows[row_idx])?
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        } else if count_star {
            vec![Value::Int(1); n]
        } else {
            // ROW_NUMBER / RANK / DENSE_RANK don't read column_values; a
            // placeholder vector of the right length keeps the API happy.
            vec![Value::Null; n]
        };
        // SUM/AVG exactness gate: route to vec only when every non-NULL
        // argument value is exact (integer/decimal) AND shares a single
        // scale. f64 sliding add/subtract is not associative (would drift
        // from legacy's fresh per-frame sum), and a mixed-scale argument
        // would leave the running `Exact` state at a higher scale than the
        // legacy per-frame recompute, producing a numerically-equal but
        // byte-different `DecimalCompact`. Either case bails to legacy.
        if matches!(win_fn, WindowFn::Sum | WindowFn::Avg) {
            let mut scale: Option<u8> = None;
            for v in &column_values {
                if matches!(v, Value::Null) {
                    continue;
                }
                match crate::engine::executor::select::aggregate::exact_decimal_parts(v) {
                    Some((_, s)) => match scale {
                        None => scale = Some(s),
                        Some(existing) if existing == s => {}
                        Some(_) => return Ok(None),
                    },
                    None => return Ok(None),
                }
            }
        }
        // Order keys per partition row, only built when RANK/DENSE_RANK
        // needs them. Empty for the others (ROW_NUMBER ignores, aggregates
        // don't use peer detection).
        let order_keys: Vec<Vec<Value>> =
            if matches!(win_fn, WindowFn::Rank | WindowFn::DenseRank) && !order_by.is_empty() {
                partition
                    .iter()
                    .map(|&row_idx| {
                        order_by
                            .iter()
                            .zip(order_positions.iter())
                            .map(|(o, pos)| -> Result<Value, QueryError> {
                                Ok(if let Some(pos) = pos {
                                    rows[row_idx].get(*pos).cloned().unwrap_or(Value::Null)
                                } else {
                                    eval_expr_against_columns(&o.expr, columns, &rows[row_idx])?
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                Vec::new()
            };
        let order_collations: Vec<Option<u16>> = order_positions
            .iter()
            .map(|position| {
                position
                    .and_then(|position| columns.get(position))
                    .and_then(|column| column.source.collation_id)
            })
            .collect();
        let part_out = crate::engine::executor::vec::window::run_window_partition_with_collations(
            win_fn,
            rows_frame,
            &column_values,
            &order_keys,
            &order_collations,
        )?;
        Ok(Some(part_out))
    };

    let mut result = vec![Value::Null; rows.len()];
    // O3 (q51): partitions are independent (disjoint row indices, all reads
    // from immutable `columns` / `rows`). Fan the eval across workers when
    // the input is large and many-partitioned. Each worker propagates the
    // session expr info (sql_mode / tz / div_precision) so `eval_expr` inside
    // a non-trivial arg expression behaves identically to the serial path.
    let degree = window_parallel_degree(rows.len(), partitions.len());
    if degree <= 1 {
        for partition in partitions.iter() {
            match eval_one(partition)? {
                Some(part_out) => {
                    for (pos, &row_idx) in partition.iter().enumerate() {
                        result[row_idx] = part_out[pos].clone();
                    }
                }
                None => return Ok(None),
            }
        }
    } else {
        let chunk = partitions.len().div_ceil(degree).max(1);
        let session_info = crate::engine::expr::current_session_expr_info();
        let session_info = &session_info;
        let eval_one = &eval_one;
        // Each worker returns its chunk's per-partition outputs in order, or a
        // bail signal (the exactness gate fired). `?`-free join: worker errors
        // and bails surface as variants we fold after the scope.
        type ChunkOut = Result<Option<Vec<Vec<Value>>>, QueryError>;
        let cancellation = crate::engine::cancel::CancellationSnapshot::capture();
        let parts: Vec<ChunkOut> = std::thread::scope(|scope| {
            let handles: Vec<_> = partitions
                .chunks(chunk)
                .map(|pc| {
                    let cancellation = cancellation.clone();
                    scope.spawn(move || -> ChunkOut {
                        let _cancel_scope = cancellation.enter();
                        crate::engine::expr::with_session_expr_info_option(session_info, || {
                            let mut outs = Vec::with_capacity(pc.len());
                            for partition in pc {
                                match eval_one(partition)? {
                                    Some(out) => outs.push(out),
                                    None => return Ok(None),
                                }
                            }
                            Ok(Some(outs))
                        })
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| {
                    h.join().unwrap_or_else(|_| {
                        Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "window eval worker panicked",
                        ))
                    })
                })
                .collect()
        });
        // Fold: any worker error propagates; any bail → whole path bails to
        // legacy (Ok(None)); otherwise scatter results back by chunk order.
        let mut chunk_outs = Vec::with_capacity(parts.len());
        for p in parts {
            match p? {
                Some(outs) => chunk_outs.push(outs),
                None => return Ok(None),
            }
        }
        let mut chunk_iter = partitions.chunks(chunk);
        for outs in chunk_outs {
            let pc = chunk_iter.next().expect("chunk count matches worker count");
            for (partition, part_out) in pc.iter().zip(outs) {
                for (pos, &row_idx) in partition.iter().enumerate() {
                    result[row_idx] = part_out[pos].clone();
                }
            }
        }
    }
    Ok(Some(result))
}

#[cfg(test)]
#[path = "tests/ctas_sink.rs"]
mod ctas_sink_tests;

#[cfg(test)]
mod window_cancellation_tests {
    use super::*;

    #[test]
    fn window_sort_cancels_inside_one_large_partition() {
        let token = crate::engine::cancel::QueryCancelToken::noop();
        let _scope = crate::engine::cancel::enter(token.clone());
        let comparisons = std::cell::Cell::new(0);
        let mut indices: Vec<usize> = (0..100_000).rev().collect();
        let error = cancellable_window_sort(&mut indices, |a, b| {
            comparisons.set(comparisons.get() + 1);
            if comparisons.get() == 2000 {
                token.cancel_local();
            }
            a.cmp(&b)
        })
        .unwrap_err();
        assert_eq!(error.code(), 1317);
        assert!(comparisons.get() < 20_000);
    }

    #[test]
    fn window_sort_matches_total_order_across_merge_boundaries() {
        for n in [0, 1, 1023, 1024, 1025, 10003] {
            let mut indices: Vec<usize> = (0..n).rev().collect();
            let compare = |a: usize, b: usize| (a % 37).cmp(&(b % 37)).then_with(|| a.cmp(&b));
            let mut expected = indices.clone();
            expected.sort_by(|a, b| compare(*a, *b));
            cancellable_window_sort(&mut indices, compare).unwrap();
            assert_eq!(indices, expected);
        }
    }

    #[test]
    fn window_evaluation_checks_cancel_before_large_frame() {
        let token = crate::engine::cancel::QueryCancelToken::noop();
        let _scope = crate::engine::cancel::enter(token.clone());
        token.cancel_local();
        let expr = Expr::WindowFunction {
            name: "COUNT".into(),
            args: vec![Expr::Wildcard],
            spec: Box::new(crate::parser::WindowSpec {
                partition_by: vec![],
                order_by: vec![],
                frame: None,
                inherits: None,
            }),
        };
        let rows = vec![vec![Value::Int(1)]; 1024];
        assert_eq!(
            evaluate_window_with_partitions(&expr, &[(0..1024).collect()], &[], &rows)
                .unwrap_err()
                .code(),
            1317
        );
        assert_eq!(
            compute_window_partitions(&[], &rows, &[], &[], None)
                .unwrap_err()
                .code(),
            1317
        );
    }
}

fn run_select_response(
    db: &Arc<Database>,
    session: &mut SessionContext,
    mut stmt: Select,
    registry: &SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    if let Some(flat) = super::select::try_flatten_locking_derived_select(db, &stmt) {
        stmt = flat;
    }
    let _row_lock_timeout = tx::select_lock_wait_timeout_guard(session, &stmt);
    if select_has_window_functions(&stmt) {
        if stmt.for_update && !stmt.joins.is_empty() {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "locking joins with window functions are not yet supported",
            ));
        }
        return execute_select_with_window(db, session, stmt, registry);
    }
    if can_use_read_only_select(session, &stmt) {
        let (txn, _snapshot_guard) = tx::autocommit_read_only_view_with_guard(db, session);
        execute_select(db, txn, stmt, registry)
    } else if can_use_lazy_read_only_transaction_select(session, &stmt) {
        // RR reuses the transaction's read view (pinned on the first
        // consistent read) so repeated SELECTs are stable across
        // intervening commits; RC takes a fresh snapshot per statement.
        let (txn, _snapshot_guard) = tx::lazy_read_only_transaction_view_with_guard(db, session);
        execute_select(db, txn, stmt, registry)
    } else {
        execute_select_in_transaction(db, session, stmt, registry)
    }
}

fn execute_select_calc_found_rows(
    db: &Arc<Database>,
    session: &mut SessionContext,
    mut stmt: Select,
    registry: &SessionRegistry,
    user_limit: Option<crate::parser::LimitValue>,
    user_offset: Option<crate::parser::LimitValue>,
) -> Result<QueryResponse, QueryError> {
    stmt.limit = None;
    stmt.offset = None;
    stmt.sql_calc_found_rows = false;
    let _streaming = super::select::force_streaming_response_scope();
    let response = run_select_response(db, session, stmt, registry)?;
    let offset = crate::engine::executor::select::limit_value_or_zero(user_offset.as_ref())?;
    let limit = crate::engine::executor::select::limit_value_or_max(user_limit.as_ref())?;
    let (columns, mut cursor): (_, Box<dyn ResultCursor>) = match response {
        QueryResponse::StreamingResultSet(rs) => (rs.columns, rs.cursor),
        QueryResponse::ResultSet(rs) => (
            rs.columns,
            Box::new(crate::engine::api::VecResultCursor::new(rs.rows)),
        ),
        QueryResponse::SharedResultSet(rs) => (
            rs.columns.to_vec(),
            Box::new(crate::engine::api::VecResultCursor::new(rs.rows)),
        ),
        other => return Ok(other),
    };
    let mut total = 0u64;
    let mut window = Vec::new();
    let drained = (|| -> Result<(), QueryError> {
        while let Some(row) = cursor.next_row()? {
            if total >= offset as u64 && window.len() < limit {
                window.push(row);
            }
            total = total.saturating_add(1);
            if total % 1024 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
        }
        Ok(())
    })();
    crate::engine::api::append_statement_warnings(cursor.take_warnings());
    drained?;
    Ok(QueryResponse::StreamingResultSet(StreamingResultSet {
        columns,
        found_rows: Some(total),
        cursor: Box::new(crate::engine::api::VecResultCursor::new(window)),
    }))
}
