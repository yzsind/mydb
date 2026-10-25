//! Statement metadata locking and dependency collection.
use super::*;

pub(super) fn execute_mdl_protected_statement(
    db: &Arc<Database>,
    session: &mut SessionContext,
    statement: Statement,
    registry: &SessionRegistry,
    routine_mdl: &BTreeSet<crate::engine::executor::routines::ResolvedRoutineMdl>,
    routine_tables: &std::collections::BTreeMap<(String, String), bool>,
) -> Result<QueryResponse, QueryError> {
    ensure_xa_statement_allowed(session, &statement)?;
    let mdl_guard = {
        let _opening = crate::engine::cancel::enter_process_stage(
            crate::engine::session_registry::ProcessState::OpeningTables,
        );
        acquire_statement_mdl_with_routines(db, session, &statement, routine_mdl, routine_tables)?
    };
    let connection_id = session.connection_id as u64;
    let mdl_scope = crate::engine::executor::stream_mdl::StatementScope::enter(db, connection_id);
    let is_call = matches!(statement, Statement::Call(_));
    if is_call {
        session.routine_mdl_depth += 1;
    }
    let result = execute_qualified_statement(db, session, statement, registry);
    if is_call {
        session.routine_mdl_depth -= 1;
    }
    crate::engine::executor::stream_mdl::finish(
        db,
        connection_id,
        mdl_guard,
        result,
        mdl_scope.outermost() && session.routine_mdl_depth == 0,
    )
}

pub(super) fn authorize_view_references_and_collect_security(
    db: &Arc<Database>,
    session: &SessionContext,
    statement: &Statement,
) -> Result<Vec<crate::engine::api::ViewSecurityOverride>, QueryError> {
    let mut views = Vec::new();
    collect_statement_view_refs(db, session, statement, &mut views)?;
    if views.is_empty() {
        return Ok(Vec::new());
    }
    // Authorize the unexpanded query before any DEFINER override exists.
    // Otherwise joining an allowed view to its private base table reuses the
    // view's privilege for the caller's independent base-table reference.
    authorize_statement(db, session, statement)?;
    let auth = db.auth_catalog();
    let ctx = session.authorization_context();
    let mut overrides = Vec::new();
    for view in views {
        let required = crate::engine::auth::RequiredPrivilege {
            privilege: Privilege::Select,
            scope: PrivilegeScope::Table {
                database: view.database.clone(),
                table: view.name.clone(),
            },
        };
        if !auth.account_has_privilege(&ctx, &required, false)
            && !column_privilege_fallback(db, &auth, &ctx, session, statement, &required)
        {
            return Err(privilege_denied(&required.privilege, &required.scope));
        }
        collect_view_definer_overrides(db, session, &view, &mut overrides, &mut ViewTrail::new())?;
    }
    Ok(overrides)
}

fn collect_statement_view_refs(
    db: &Arc<Database>,
    session: &SessionContext,
    statement: &Statement,
    out: &mut Vec<ViewSchema>,
) -> Result<(), QueryError> {
    // Share the scope-aware table-use walker with authorization/MDL. It covers
    // expression subqueries and excludes CTE/derived aliases.
    fn is_query(statement: &Statement) -> bool {
        match statement {
            Statement::Select(_) | Statement::SetOperation(_) => true,
            Statement::WithCte(with) => is_query(&with.body),
            Statement::Explain { target, .. } => is_query(target),
            Statement::SelectIntoVars { .. } | Statement::SelectIntoFile { .. } => true,
            _ => false,
        }
    }
    for usage in collect_statement_table_uses(statement) {
        if !usage.write || is_query(statement) {
            collect_table_view_ref(db, session, &usage.table, out)?;
        }
    }
    Ok(())
}

fn collect_table_view_ref(
    db: &Arc<Database>,
    session: &SessionContext,
    name: &str,
    out: &mut Vec<ViewSchema>,
) -> Result<(), QueryError> {
    if session.has_temp_table(name) {
        return Ok(());
    }
    let Some(view) = db.view_definition(&session.current_database, name) else {
        return Ok(());
    };
    if view.invalid {
        return Err(error(crate::error::catalog::ER_VIEW_INVALID,
            format!(
                "View '{}.{}' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them",
                view.database, view.name
            ),
        ));
    }
    if !out.iter().any(|v| {
        v.database.eq_ignore_ascii_case(&view.database) && v.name.eq_ignore_ascii_case(&view.name)
    }) {
        out.push(view);
    }
    Ok(())
}

pub(super) fn statement_invokes_trigger(db: &Database, statement: &Statement) -> bool {
    use crate::storage::TriggerEventSchema;
    match statement {
        Statement::Insert(insert) => {
            db.table_has_trigger_event(&insert.table, TriggerEventSchema::Insert)
        }
        Statement::Update(update) => {
            db.table_has_trigger_event(&update.table, TriggerEventSchema::Update)
        }
        Statement::Delete(delete) => {
            db.table_has_trigger_event(&delete.table, TriggerEventSchema::Delete)
        }
        _ => false,
    }
}

fn collect_view_definer_overrides(
    db: &Arc<Database>,
    invoker: &SessionContext,
    view: &ViewSchema,
    out: &mut Vec<crate::engine::api::ViewSecurityOverride>,
    trail: &mut ViewTrail,
) -> Result<(), QueryError> {
    let key = trail_key(view);
    if !trail.insert(key.clone()) {
        return Err(error(
            crate::error::catalog::ER_VIEW_RECURSIVE,
            "Recursive view reference",
        ));
    }
    let context = effective_view_security_account(db, invoker, view, None)?;
    let mut security = invoker.clone();
    security.authenticated_as = context.authenticated_as.clone();
    security.active_roles = context.active_roles.clone();
    security.current_database = view.database.clone();
    security.view_security_overrides.clear();
    let body = Statement::Select(stored_view_query(view)?);
    // Validate each view body under its own security context before flattening
    // dependencies. This also checks access to intermediate INVOKER views.
    authorize_statement(db, &security, &body).map_err(|err| {
        if view.security_type == RoutineSecuritySchema::Definer
            && (err.is(crate::error::catalog::ER_TABLEACCESS_DENIED_ERROR)
                || err.is(crate::error::catalog::ER_COLUMNACCESS_DENIED_ERROR))
        {
            error(crate::error::catalog::ER_VIEW_INVALID, format!(
                "View '{}.{}' references invalid table(s) or column(s) or definer/invoker of view lack rights to use them",
                view.database, view.name))
        } else { err }
    })?;
    for dep in &view.dependencies {
        if dep.is_view {
            if let Some(nested) = db.view_definition(&dep.database, &dep.name) {
                collect_view_definer_overrides(db, &security, &nested, out, trail)?;
            }
        } else if !out.iter().any(|existing| {
            existing.database.eq_ignore_ascii_case(&dep.database)
                && existing.table.eq_ignore_ascii_case(&dep.name)
        }) {
            out.push(crate::engine::api::ViewSecurityOverride {
                database: dep.database.clone(),
                table: dep.name.clone(),
                context: context.clone(),
            });
        }
    }
    trail.remove(&key);
    Ok(())
}

#[derive(Debug, Clone)]
pub(in crate::engine::executor) struct StatementTableUse {
    pub(in crate::engine::executor) table: String,
    pub(in crate::engine::executor) alias: Option<String>,
    pub(in crate::engine::executor) write: bool,
}

fn acquire_statement_mdl_with_routines(
    db: &Arc<Database>,
    session: &SessionContext,
    statement: &Statement,
    routine_mdl: &BTreeSet<crate::engine::executor::routines::ResolvedRoutineMdl>,
    routine_tables: &std::collections::BTreeMap<(String, String), bool>,
) -> Result<PreparedMdlGuard, QueryError> {
    if statement_skips_mdl(statement) && routine_mdl.is_empty() && routine_tables.is_empty() {
        return Ok(PreparedMdlGuard::None);
    }
    let uses = collect_statement_table_uses(statement);
    enforce_explicit_table_locks(session, statement, &uses)?;
    let mut requests = mdl_requests_for_statement(db, session, statement, &uses);
    requests.extend(routine_mdl.iter().map(|routine| MdlRequest {
        key: MdlObjectKey::named(
            if routine.kind == "procedure" {
                MdlObjectType::Procedure
            } else {
                MdlObjectType::Function
            },
            &routine.database,
            &routine.name,
        ),
        lock_type: MdlLockType::SharedRead,
    }));
    if matches!(statement, Statement::Call(_)) && !requests.is_empty() {
        // Pin procedure/function definitions for the whole CALL, even across
        // a COMMIT in its body. Each body SQL acquires its own table locks;
        // prelocking every possible procedure table would overconstrain DDL.
        if session.explicit_txn || !session.autocommit || session.xa_branch.is_some() {
            acquire_prepared_mdl_requests(db, session, &requests, false)?;
        }
        db.acquire_cached_mdl(
            session.connection_id as u64,
            Some(session.connection_id as u64),
            MdlDuration::Statement,
            &requests,
            "sql/call",
        )?;
        return Ok(PreparedMdlGuard::Statement);
    }
    for ((database, table), write) in routine_tables {
        let key = MdlObjectKey::table(database, table);
        if let Some(request) = requests.iter_mut().find(|request| request.key == key) {
            if *write && request.lock_type == MdlLockType::SharedRead {
                request.lock_type = MdlLockType::SharedWrite;
            }
        } else {
            requests.push(MdlRequest {
                key,
                lock_type: if *write {
                    MdlLockType::SharedWrite
                } else {
                    MdlLockType::SharedRead
                },
            });
        }
    }
    if requests
        .iter()
        .any(|r| r.lock_type == MdlLockType::SharedWrite)
        && !requests
            .iter()
            .any(|r| r.key.object_type == MdlObjectType::Global)
    {
        requests.push(MdlRequest {
            key: MdlObjectKey::global(),
            lock_type: MdlLockType::SharedWrite,
        });
    }
    if requests.is_empty() {
        return Ok(PreparedMdlGuard::None);
    }
    // The ordinary text dispatcher and the prepared/compiled paths have the
    // same MDL lifetime rules. Reuse the cached acquire here as well: within
    // an explicit transaction, already-held table grants are a session-local
    // memo hit; for an autocommit single-table read, SHARED_READ is granted by
    // the per-object atomic reader path. The old generic path rebuilt a
    // ticket under the single global MDL mutex for every statement and then
    // scanned every owner ticket again at statement end, even though
    // transaction-duration tickets were already held until COMMIT.
    if matches!(statement, Statement::WithCte(_))
        && !crate::engine::executor::stream_mdl::StatementScope::active(
            db,
            session.connection_id as u64,
        )
        && session.autocommit
        && !session.explicit_txn
        && session.xa_branch.is_none()
    {
        // CTE workers share this owner's tickets. Acquire the complete WITH
        // dependency set before rewriting/materialization and keep it ticketed
        // so nested dispatch cannot leave grants outside the outer lease.
        db.acquire_cached_mdl(
            session.connection_id as u64,
            Some(session.connection_id as u64),
            MdlDuration::Statement,
            &requests,
            "sql/statement",
        )?;
        return Ok(PreparedMdlGuard::Statement);
    }
    acquire_prepared_mdl_requests(db, session, &requests, is_ddl_statement(statement))
}

pub(in crate::engine::executor) fn prepared_statement_mdl_requests(
    db: &Arc<Database>,
    session: &SessionContext,
    statement: &Statement,
) -> Vec<MdlRequest> {
    if statement_skips_mdl(statement) {
        return Vec::new();
    }
    let uses = collect_statement_table_uses(statement);
    mdl_requests_for_statement(db, session, statement, &uses)
}

/// How a prepared statement's MDL was granted — tells the caller which
/// release path pairs with the acquire.
pub(in crate::engine::executor) enum PreparedMdlGuard {
    /// Nothing to release at statement end (skipped, empty, or
    /// transaction-duration tickets released at commit/rollback).
    None,
    /// Ticketed statement-duration grant — release via
    /// `release_cached_mdl_duration`.
    Statement,
    /// Lock-free fast-path SHARED_READ grant — release via
    /// `release_fast_mdl`.
    Fast(crate::storage::mdl::MdlFastGrant),
}

pub(in crate::engine::executor) fn acquire_prepared_statement_mdl(
    db: &Arc<Database>,
    session: &SessionContext,
    statement: &Statement,
    requests: &[MdlRequest],
) -> Result<PreparedMdlGuard, QueryError> {
    if statement_skips_mdl(statement) {
        return Ok(PreparedMdlGuard::None);
    }
    if session.has_explicit_table_locks() {
        let uses = collect_statement_table_uses(statement);
        enforce_explicit_table_locks(session, statement, &uses)?;
    }
    acquire_prepared_mdl_requests(db, session, requests, is_ddl_statement(statement))
}

/// SELECT-specialised prepared MDL acquire.  The compiled SELECT memo already
/// proves the statement kind and carries its immutable request recipe, so the
/// common no-LOCK-TABLES path does not need to clone a full Select AST merely
/// to rediscover that it is neither an MDL-skipping command nor DDL.
pub(in crate::engine::executor) fn acquire_prepared_select_mdl(
    db: &Arc<Database>,
    session: &SessionContext,
    select: &Select,
    requests: &[MdlRequest],
) -> Result<PreparedMdlGuard, QueryError> {
    if session.has_explicit_table_locks() {
        let statement = Statement::Select(select.clone());
        let uses = collect_statement_table_uses(&statement);
        enforce_explicit_table_locks(session, &statement, &uses)?;
    }
    acquire_prepared_mdl_requests(db, session, requests, false)
}

fn acquire_prepared_mdl_requests(
    db: &Arc<Database>,
    session: &SessionContext,
    requests: &[MdlRequest],
    is_ddl: bool,
) -> Result<PreparedMdlGuard, QueryError> {
    if requests.is_empty() {
        return Ok(PreparedMdlGuard::None);
    }
    let duration = if is_ddl {
        MdlDuration::Statement
    } else if session.explicit_txn || !session.autocommit || session.xa_branch.is_some() {
        MdlDuration::Transaction
    } else {
        MdlDuration::Statement
    };
    // Transaction-duration fast path: tickets granted earlier in this
    // transaction are held until commit/rollback, so a statement whose
    // requests were all granted for the *current* txn id can skip the
    // (global-mutex) MDL manager entirely. The memo self-invalidates when
    // the txn id or lazy transaction generation changes. Plain BEGIN /
    // SET autocommit=0 read-only transactions may never allocate a storage
    // txn id, so they use the session-local generation as the memo key.
    if duration == MdlDuration::Transaction {
        let txn_memo_key = session.current_txn.unwrap_or(session.mdl_txn_generation);
        if txn_memo_key != 0 {
            let mut memo = session.mdl_txn_grants.borrow_mut();
            let (memo_txn, granted) = &mut *memo;
            if *memo_txn != txn_memo_key {
                *memo_txn = txn_memo_key;
                granted.clear();
            }
            if requests.iter().all(|r| {
                granted
                    .iter()
                    .any(|g| g.lock_type == r.lock_type && g.key == r.key)
            }) {
                return Ok(PreparedMdlGuard::None);
            }
            db.acquire_cached_mdl(
                session.connection_id as u64,
                Some(session.connection_id as u64),
                duration,
                requests,
                "sql/statement",
            )?;
            for request in requests {
                if !granted
                    .iter()
                    .any(|g| g.lock_type == request.lock_type && g.key == request.key)
                {
                    granted.push(request.clone());
                }
            }
            return Ok(PreparedMdlGuard::None);
        }
    }
    if duration == MdlDuration::Statement {
        // Statement-duration fast path: an autocommit read's single
        // SHARED_READ request is granted lock-free (per-object atomic
        // reader count) whenever no read-blocking DDL is granted or
        // waiting on the object — the per-statement global-mutex
        // acquire/release pair measured ~5% of point-select CPU.
        if let Some(grant) = db.try_acquire_fast_mdl_cached(requests, &session.mdl_fast_cache) {
            return Ok(PreparedMdlGuard::Fast(grant));
        }
    }
    db.acquire_cached_mdl(
        session.connection_id as u64,
        Some(session.connection_id as u64),
        duration,
        requests,
        "sql/statement",
    )?;
    Ok(if duration == MdlDuration::Statement {
        PreparedMdlGuard::Statement
    } else {
        PreparedMdlGuard::None
    })
}

fn statement_skips_mdl(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::LockTables(_)
            | Statement::UnlockTables
            | Statement::Begin { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
            | Statement::Savepoint(_)
            | Statement::RollbackToSavepoint(_)
            | Statement::ReleaseSavepoint(_)
            | Statement::SetAutocommit(_)
            | Statement::SetTransactionIsolation { .. }
            | Statement::SetTransactionCharacteristics { .. }
            | Statement::Set(_)
            | Statement::SetNames { .. }
            | Statement::SetCharacterSet { .. }
            | Statement::SetResourceGroup { .. }
            | Statement::ResetPersist { .. }
            | Statement::BinlogAdmin { .. }
            | Statement::ReplicationAdmin { .. }
            | Statement::MydbReplicationAdmin { .. }
            | Statement::BackupAdmin { .. }
            | Statement::XaStart { .. }
            | Statement::XaEnd { .. }
            | Statement::XaPrepare { .. }
            | Statement::XaCommit { .. }
            | Statement::XaRollback { .. }
            | Statement::XaRecover { .. }
    )
}

fn mdl_requests_for_statement(
    db: &Database,
    session: &SessionContext,
    statement: &Statement,
    uses: &[StatementTableUse],
) -> Vec<MdlRequest> {
    let mut expanded_uses = uses.to_vec();
    add_dml_view_table_uses(db, session, statement, &mut expanded_uses);
    let mut by_table = std::collections::BTreeMap::<String, MdlLockType>::new();
    for table_use in &expanded_uses {
        let Some((_schema, _table, key)) = persistent_table_parts(session, &table_use.table) else {
            continue;
        };
        let mode = if table_use.write {
            MdlLockType::SharedWrite
        } else {
            MdlLockType::SharedRead
        };
        insert_mdl_mode(&mut by_table, key, mode);
    }
    add_fk_mdl_dependencies(db, session, statement, &expanded_uses, &mut by_table);
    add_trigger_mdl_dependencies(db, session, statement, &expanded_uses, &mut by_table);
    add_ddl_table_mdl_targets(db, session, statement, &mut by_table);
    let mut requests: Vec<MdlRequest> = by_table
        .into_iter()
        .filter_map(|(key, lock_type)| {
            let (schema, table) = split_table_key(&key)?;
            Some(MdlRequest {
                key: MdlObjectKey::table(&schema, &table),
                lock_type,
            })
        })
        .collect();
    if let Statement::DropDatabase { name, .. } = statement {
        requests.extend(drop_database_mdl_requests(db, name));
    }
    add_non_table_mdl_targets(db, session, statement, &mut requests);
    add_account_mdl_targets(session, statement, &mut requests);
    if is_ddl_statement(statement) {
        // Stabilize schema membership while DROP DATABASE enumerates objects.
        // Ordinary SELECT keeps its single-table lock-free MDL recipe.
        let schemas: BTreeSet<_> = requests
            .iter()
            .filter_map(|r| {
                (r.key.object_type != MdlObjectType::Schema
                    && r.key.object_type != MdlObjectType::Account)
                    .then(|| r.key.schema.clone())
                    .flatten()
            })
            .collect();
        requests.extend(schemas.into_iter().map(|schema| MdlRequest {
            key: MdlObjectKey::schema(&schema),
            lock_type: MdlLockType::IntentionExclusive,
        }));
    }
    if crate::replication::is_replica_write_statement(statement) {
        requests.push(MdlRequest {
            key: MdlObjectKey::global(),
            lock_type: MdlLockType::SharedWrite,
        });
    }
    requests
}

fn add_account_mdl_targets(
    session: &SessionContext,
    statement: &Statement,
    requests: &mut Vec<MdlRequest>,
) {
    let mut add = |key: AccountKey, lock_type| {
        requests.push(MdlRequest {
            key: MdlObjectKey {
                object_type: MdlObjectType::Account,
                schema: Some(key.host),
                name: Some(key.user),
            },
            lock_type,
        })
    };
    let mut definition = None;
    match statement {
        Statement::CreateUser(stmt) => {
            for spec in &stmt.accounts {
                add(account_key(&spec.account), MdlLockType::Exclusive);
            }
        }
        Statement::CreateRole { roles, .. } => {
            for role in roles {
                add(account_key(role), MdlLockType::Exclusive);
            }
        }
        Statement::DropUser { accounts, .. }
        | Statement::DropRole {
            roles: accounts, ..
        } => {
            for account in accounts {
                add(account_key(account), MdlLockType::Exclusive);
            }
        }
        Statement::RenameUser { renames } => {
            for (from, to) in renames {
                add(account_key(from), MdlLockType::Exclusive);
                add(account_key(to), MdlLockType::Exclusive);
            }
        }
        Statement::SetPassword { account, .. } => {
            add(
                account
                    .as_ref()
                    .map(|account| show_account_key(account, session))
                    .unwrap_or_else(|| session.authenticated_as.clone()),
                MdlLockType::Exclusive,
            );
        }
        Statement::CreateView { definer, .. } | Statement::AlterView { definer, .. } => {
            definition = Some(definer.as_ref())
        }
        Statement::CreateRoutine(def) => definition = Some(def.definer.as_ref()),
        Statement::CreateTrigger(def) => definition = Some(def.definer.as_ref()),
        Statement::CreateEvent(def) => definition = Some(def.definer.as_ref()),
        Statement::AlterEvent(def) => definition = Some(def.definer.as_ref()),
        _ => {}
    }
    if let Some(definer) = definition {
        add(
            definer
                .map(account_key)
                .unwrap_or_else(|| session.authenticated_as.clone()),
            MdlLockType::SharedRead,
        );
    }
}

fn add_dml_view_table_uses(
    db: &Database,
    session: &SessionContext,
    statement: &Statement,
    out: &mut Vec<StatementTableUse>,
) {
    let (target, write) = match statement {
        Statement::Insert(stmt) => (Some(stmt.table.as_str()), true),
        Statement::Update(stmt) => (Some(stmt.table.as_str()), true),
        Statement::Delete(stmt) => (Some(stmt.table.as_str()), true),
        Statement::Explain {
            target, analyze, ..
        } => {
            let target = match target.as_ref() {
                Statement::WithCte(with) => with.body.as_ref(),
                other => other,
            };
            let name = match target {
                Statement::Insert(stmt) => Some(stmt.table.as_str()),
                Statement::Update(stmt) => Some(stmt.table.as_str()),
                Statement::Delete(stmt) => Some(stmt.table.as_str()),
                _ => None,
            };
            (name, *analyze)
        }
        Statement::WithCte(with) => {
            add_dml_view_table_uses(db, session, &with.body, out);
            return;
        }
        _ => (None, false),
    };
    let Some(target) = target else { return };
    let mut pending = vec![target.to_string()];
    match statement {
        Statement::Update(s) => pending.extend(s.joins.iter().map(|j| j.table.name.clone())),
        Statement::Delete(s) => pending.extend(s.joins.iter().map(|j| j.table.name.clone())),
        _ => {}
    }
    let mut seen = std::collections::HashSet::new();
    while let Some(name) = pending.pop() {
        let (database, relation) =
            crate::storage::split_table_name(&session.current_database, &name);
        let key = crate::storage::table_key(&database, &relation);
        if !seen.insert(key) {
            continue;
        }
        let Some(view) = db.view_definition(&database, &relation) else {
            continue;
        };
        for dependency in &view.dependencies {
            let qualified = crate::storage::table_key(&dependency.database, &dependency.name);
            out.push(StatementTableUse {
                table: qualified.clone(),
                alias: None,
                write,
            });
            if dependency.is_view {
                pending.push(qualified);
            }
        }
    }
}

// catalog() supplies one atomic snapshot. Revalidated under schema X before DROP.
pub(super) fn drop_database_mdl_requests(db: &Database, database: &str) -> Vec<MdlRequest> {
    let catalog = db.catalog();
    let mut requests = Vec::new();
    let mut add = |kind, schema: &str, name: &str| {
        if schema.eq_ignore_ascii_case(database) {
            requests.push(MdlRequest {
                key: MdlObjectKey::named(kind, schema, name),
                lock_type: MdlLockType::Exclusive,
            });
        }
    };
    for table in catalog.tables.values() {
        add(MdlObjectType::Table, &table.database, &table.name);
    }
    for view in catalog.views.values() {
        add(MdlObjectType::Table, &view.database, &view.name);
    }
    for sequence in catalog.sequences.values() {
        add(MdlObjectType::Table, &sequence.database, &sequence.name);
    }
    for routine in catalog.routines.values() {
        let kind = match routine.kind {
            crate::storage::RoutineKindSchema::Procedure => MdlObjectType::Procedure,
            crate::storage::RoutineKindSchema::Function => MdlObjectType::Function,
        };
        add(kind, &routine.database, &routine.name);
    }
    for trigger in catalog.triggers.values() {
        add(MdlObjectType::Trigger, &trigger.database, &trigger.name);
    }
    for event in catalog.events.values() {
        add(MdlObjectType::Event, &event.database, &event.name);
    }
    requests
}

fn add_ddl_table_mdl_targets(
    db: &Database,
    session: &SessionContext,
    statement: &Statement,
    out: &mut std::collections::BTreeMap<String, MdlLockType>,
) {
    let mut add = |name: &str, lock_type: MdlLockType| {
        if let Some((_schema, _table, key)) = persistent_table_parts(session, name) {
            insert_mdl_mode(out, key, lock_type);
        }
    };
    match statement {
        Statement::CreateTable(stmt) if !stmt.temporary => add(&stmt.name, MdlLockType::Exclusive),
        Statement::CreateTableLike {
            name,
            source,
            temporary,
            ..
        } => {
            if !temporary {
                add(name, MdlLockType::Exclusive);
                add(source, MdlLockType::SharedRead);
            }
        }
        Statement::CreateTableAsSelect { table, query, .. } if !table.temporary => {
            add(&table.name, MdlLockType::Exclusive);
            for table_use in collect_statement_table_uses(query) {
                if let Some((_schema, _table, key)) =
                    persistent_table_parts(session, &table_use.table)
                {
                    insert_mdl_mode(out, key, MdlLockType::SharedRead);
                }
            }
        }
        Statement::AlterTable(stmt) => {
            let lock_type =
                alter_table_statement_mdl(db, session, stmt).unwrap_or(MdlLockType::Exclusive);
            add(&stmt.table, lock_type);
            for action in &stmt.partition_actions {
                if let crate::parser_partition::PartitionAction::ExchangePartition {
                    table, ..
                } = action
                {
                    let (schema, _) =
                        crate::storage::split_table_name(&session.current_database, &stmt.table);
                    let (schema, name) = crate::storage::split_table_name(&schema, table);
                    add(
                        &crate::storage::table_key(&schema, &name),
                        MdlLockType::Exclusive,
                    );
                }
            }
            for action in &stmt.actions {
                if let AlterAction::RenameTable { new_name } = action {
                    add(
                        &qualify_alter_rename_target(session, &stmt.table, new_name),
                        MdlLockType::Exclusive,
                    );
                }
            }
            if stmt.partition_actions.is_empty()
                && (Database::constraint_alter_supported(&stmt.actions)
                    || stmt.actions.iter().any(|action| {
                        matches!(
                            action,
                            AlterAction::ModifyColumn(_) | AlterAction::ChangeColumn { .. }
                        )
                    }))
            {
                let table = qualify_persistent_table_name(session, &stmt.table);
                for dependency in db.constraint_alter_dependencies(&table, &stmt.actions) {
                    add(&dependency, lock_type);
                }
            }
        }
        Statement::DropTable {
            names, temporary, ..
        } if !temporary => {
            for name in names {
                add(name, MdlLockType::Exclusive);
            }
        }
        Statement::RenameTables { pairs } => {
            for (old, new) in pairs {
                add(old, MdlLockType::Exclusive);
                add(new, MdlLockType::Exclusive);
            }
        }
        Statement::TruncateTable { name } | Statement::DropIndex { table: name, .. } => {
            add(name, MdlLockType::Exclusive)
        }
        Statement::AnalyzeTable { tables, .. } => {
            // Allow ordinary readers/writers; serialize maintenance and block DDL.
            for name in tables {
                add(name, MdlLockType::SharedUpgradable);
            }
        }
        Statement::CheckTable { tables } => {
            for tbl in tables {
                add(tbl, MdlLockType::SharedRead);
            }
        }
        Statement::ChecksumTable { tables, .. } => {
            for tbl in tables {
                add(tbl, MdlLockType::SharedRead);
            }
        }
        Statement::OptimizeTable { tables, .. } => {
            for tbl in tables {
                add(tbl, MdlLockType::SharedUpgradable);
            }
        }
        Statement::RepairTable { tables } => {
            for tbl in tables {
                add(tbl, MdlLockType::Exclusive);
            }
        }
        Statement::CreateIndex(stmt) => add(
            &stmt.table,
            match stmt.lock {
                AlterLock::Exclusive => MdlLockType::Exclusive,
                AlterLock::Shared if stmt.index_type.eq_ignore_ascii_case("BTREE") => {
                    MdlLockType::SharedUpgradable
                }
                AlterLock::Shared => MdlLockType::SharedNoWrite,
                AlterLock::None | AlterLock::Default
                    if stmt.index_type.eq_ignore_ascii_case("BTREE") =>
                {
                    MdlLockType::SharedUpgradable
                }
                AlterLock::None | AlterLock::Default => MdlLockType::SharedNoWrite,
            },
        ),
        Statement::CreateView { name, query, .. } | Statement::AlterView { name, query, .. } => {
            let name = name.bind(&session.current_database).catalog_key();
            add(&name, MdlLockType::Exclusive);
            {
                for table_use in collect_statement_table_uses(query) {
                    if let Some((_schema, _table, key)) =
                        persistent_table_parts(session, &table_use.table)
                    {
                        insert_mdl_mode(out, key, MdlLockType::SharedRead);
                    }
                }
            }
        }
        Statement::DropView { names, .. } => {
            for name in names {
                add(
                    &name.bind(&session.current_database).catalog_key(),
                    MdlLockType::Exclusive,
                );
            }
        }
        Statement::CreateTrigger(def) => add(&def.table, MdlLockType::SharedNoWrite),
        Statement::Describe { table }
        | Statement::ShowColumns { table, .. }
        | Statement::ShowIndex { table, .. }
        | Statement::ShowCreateTable { table } => add(table, MdlLockType::SharedRead),
        _ => {}
    }
}

fn add_fk_mdl_dependencies(
    db: &Database,
    session: &SessionContext,
    statement: &Statement,
    uses: &[StatementTableUse],
    out: &mut std::collections::BTreeMap<String, MdlLockType>,
) {
    let statement = match statement {
        Statement::Explain {
            target,
            analyze: true,
            ..
        } => target.as_ref(),
        other => other,
    };
    let statement = match statement {
        Statement::WithCte(with) => with.body.as_ref(),
        other => other,
    };
    // Resolve new FK declarations against the child's schema, including
    // qualified CREATE targets and mixed ALTER actions. These locks must
    // span both validation and catalog publication.
    let mut add_parent = |child: &str, fk: &crate::parser::ForeignKeyDef| {
        let (schema, _) = crate::storage::split_table_name(&session.current_database, child);
        let (schema, table) = crate::storage::split_table_name(&schema, &fk.referenced_table);
        insert_mdl_mode(
            out,
            crate::storage::table_key(&schema, &table),
            MdlLockType::SharedRead,
        );
    };
    match statement {
        Statement::CreateTable(table) if !table.temporary => {
            for fk in &table.foreign_keys {
                add_parent(&table.name, fk);
            }
        }
        Statement::CreateTableAsSelect { table, .. } if !table.temporary => {
            for fk in &table.foreign_keys {
                add_parent(&table.name, fk);
            }
        }
        Statement::AlterTable(table) => {
            for action in &table.actions {
                if let AlterAction::AddForeignKey(fk) = action {
                    add_parent(&table.table, fk);
                }
            }
        }
        _ => {}
    }
    if !matches!(
        statement,
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
    ) {
        return;
    }
    for table_use in uses.iter().filter(|table_use| table_use.write) {
        let qualified = qualify_persistent_table_name(session, &table_use.table);
        let Ok(table) = db.table(&qualified) else {
            continue;
        };
        match statement {
            Statement::Insert(_) | Statement::Update(_) => {
                for fk in &table.foreign_keys {
                    let key =
                        crate::storage::table_key(&fk.referenced_database, &fk.referenced_table);
                    insert_mdl_mode(out, key, MdlLockType::SharedRead);
                }
            }
            _ => {}
        }
        match statement {
            Statement::Update(_) | Statement::Delete(_) => {
                for inbound in &table.fk_referenced_by {
                    let mode = match statement {
                        Statement::Delete(_) if inbound.on_delete.is_blocking() => {
                            MdlLockType::SharedRead
                        }
                        Statement::Update(_) if inbound.on_update.is_blocking() => {
                            MdlLockType::SharedRead
                        }
                        _ => MdlLockType::SharedWrite,
                    };
                    insert_mdl_mode(out, inbound.child_table_key.clone(), mode);
                }
            }
            _ => {}
        }
    }
}

fn add_trigger_mdl_dependencies(
    db: &Database,
    session: &SessionContext,
    statement: &Statement,
    uses: &[StatementTableUse],
    out: &mut std::collections::BTreeMap<String, MdlLockType>,
) {
    let statement = match statement {
        Statement::Explain {
            target,
            analyze: true,
            ..
        } => target.as_ref(),
        other => other,
    };
    let statement = match statement {
        Statement::WithCte(with) => with.body.as_ref(),
        other => other,
    };
    let Some(event) = trigger_event_for_statement(statement) else {
        return;
    };
    for table_use in uses.iter().filter(|table_use| table_use.write) {
        let qualified = qualify_persistent_table_name(session, &table_use.table);
        let Ok(table) = db.table(&qualified) else {
            continue;
        };
        for timing in [TriggerTimingSchema::Before, TriggerTimingSchema::After] {
            for trigger in
                db.triggers_for_table_event(&table.qualified_name(), event.clone(), timing)
            {
                let Ok(body) = parse_routine_body_sql(&trigger.body_sql) else {
                    continue;
                };
                for table_use in collect_routine_table_uses(&body.statements) {
                    if let Some((_schema, _table, key)) = persistent_table_parts_for_database(
                        session,
                        &trigger.database,
                        &table_use.table,
                    ) {
                        let mode = if table_use.write {
                            MdlLockType::SharedWrite
                        } else {
                            MdlLockType::SharedRead
                        };
                        insert_mdl_mode(out, key, mode);
                    }
                }
            }
        }
    }
}

fn trigger_event_for_statement(statement: &Statement) -> Option<TriggerEventSchema> {
    match statement {
        Statement::Insert(_) | Statement::LoadData(_) => Some(TriggerEventSchema::Insert),
        Statement::Update(_) => Some(TriggerEventSchema::Update),
        Statement::Delete(_) => Some(TriggerEventSchema::Delete),
        _ => None,
    }
}

pub(in crate::engine::executor) fn collect_routine_table_uses(
    statements: &[RoutineStmt],
) -> Vec<StatementTableUse> {
    let mut out = Vec::new();
    for stmt in statements {
        collect_routine_stmt_table_uses(stmt, &mut out);
    }
    out
}

fn collect_routine_stmt_table_uses(stmt: &RoutineStmt, out: &mut Vec<StatementTableUse>) {
    match stmt {
        RoutineStmt::Block { statements, .. }
        | RoutineStmt::Loop { statements, .. }
        | RoutineStmt::While { statements, .. }
        | RoutineStmt::Repeat { statements, .. } => {
            for stmt in statements {
                collect_routine_stmt_table_uses(stmt, out);
            }
        }
        RoutineStmt::DeclareHandler { statement, .. } => {
            collect_routine_stmt_table_uses(statement, out)
        }
        RoutineStmt::If {
            branches,
            else_branch,
        }
        | RoutineStmt::Case {
            branches,
            else_branch,
            ..
        } => {
            for (_expr, statements) in branches {
                for stmt in statements {
                    collect_routine_stmt_table_uses(stmt, out);
                }
            }
            for stmt in else_branch {
                collect_routine_stmt_table_uses(stmt, out);
            }
        }
        RoutineStmt::DeclareCursor { select_sql, .. }
        | RoutineStmt::SelectInto { select_sql, .. } => {
            if let Ok(statement) = parse_statement(select_sql) {
                out.extend(collect_statement_table_uses(&statement));
            }
        }
        RoutineStmt::Sql(sql) => {
            if let Ok(statement) = parse_statement(sql) {
                out.extend(collect_statement_table_uses(&statement));
            }
        }
        _ => {}
    }
}

fn add_non_table_mdl_targets(
    db: &Database,
    session: &SessionContext,
    statement: &Statement,
    out: &mut Vec<MdlRequest>,
) {
    match statement {
        Statement::CreateDatabase { name, .. } | Statement::DropDatabase { name, .. } => {
            out.push(MdlRequest {
                key: MdlObjectKey::schema(name),
                lock_type: MdlLockType::Exclusive,
            });
        }
        Statement::AlterDatabase { name, .. } => out.push(MdlRequest {
            key: MdlObjectKey::schema(if name.is_empty() {
                &session.current_database
            } else {
                name
            }),
            lock_type: MdlLockType::Exclusive,
        }),
        Statement::CreateSequence { name, options, .. }
        | Statement::AlterSequence { name, options, .. } => {
            push_named_mdl(
                session,
                out,
                MdlObjectType::Table,
                name,
                MdlLockType::Exclusive,
            );
            if let Some(owner) = &options.owned_table {
                push_named_mdl(
                    session,
                    out,
                    MdlObjectType::Table,
                    owner,
                    MdlLockType::SharedRead,
                );
            }
            if let Statement::AlterSequence {
                rename_to: Some(target),
                ..
            } = statement
            {
                let (schema, _) = crate::storage::split_table_name(&session.current_database, name);
                let (schema, target) = crate::storage::split_table_name(&schema, target);
                out.push(MdlRequest {
                    key: MdlObjectKey::table(&schema, &target),
                    lock_type: MdlLockType::Exclusive,
                });
            }
        }
        Statement::DropSequence { names, .. } => {
            for name in names {
                push_named_mdl(
                    session,
                    out,
                    MdlObjectType::Table,
                    name,
                    MdlLockType::Exclusive,
                );
            }
        }
        Statement::CreateRoutine(def) => push_named_mdl(
            session,
            out,
            routine_kind_to_mdl_object_type(def.kind),
            &def.name,
            MdlLockType::Exclusive,
        ),
        Statement::AlterRoutine { kind, name, .. } | Statement::DropRoutine { kind, name, .. } => {
            push_named_mdl(
                session,
                out,
                routine_kind_to_mdl_object_type(*kind),
                name,
                MdlLockType::Exclusive,
            );
        }
        Statement::Call(call) => push_named_mdl(
            session,
            out,
            MdlObjectType::Procedure,
            &call.name,
            MdlLockType::SharedRead,
        ),
        Statement::ShowCreateRoutine { kind, name } => push_named_mdl(
            session,
            out,
            routine_kind_to_mdl_object_type(*kind),
            name,
            MdlLockType::SharedRead,
        ),
        Statement::CreateTrigger(def) => {
            push_named_mdl(
                session,
                out,
                MdlObjectType::Trigger,
                &def.name,
                MdlLockType::Exclusive,
            );
        }
        Statement::DropTrigger { name, .. } => {
            if let Some(trigger) = db.trigger_definition(&session.current_database, name) {
                out.push(MdlRequest {
                    key: MdlObjectKey::table(&trigger.table_database, &trigger.table_name),
                    lock_type: MdlLockType::SharedNoWrite,
                });
            }

            push_named_mdl(
                session,
                out,
                MdlObjectType::Trigger,
                name,
                MdlLockType::Exclusive,
            );
        }
        Statement::ShowCreateTrigger { name } => {
            push_named_mdl(
                session,
                out,
                MdlObjectType::Trigger,
                name,
                MdlLockType::SharedRead,
            );
        }
        Statement::CreateEvent(def) => {
            push_named_mdl(
                session,
                out,
                MdlObjectType::Event,
                &def.name,
                MdlLockType::Exclusive,
            );
        }
        Statement::AlterEvent(stmt) => {
            push_named_mdl(
                session,
                out,
                MdlObjectType::Event,
                &stmt.name,
                MdlLockType::Exclusive,
            );
            if let Some(rename_to) = &stmt.rename_to {
                push_named_mdl(
                    session,
                    out,
                    MdlObjectType::Event,
                    rename_to,
                    MdlLockType::Exclusive,
                );
            }
        }
        Statement::DropEvent { name, .. } => {
            push_named_mdl(
                session,
                out,
                MdlObjectType::Event,
                name,
                MdlLockType::Exclusive,
            );
        }
        Statement::ShowCreateEvent { name } => {
            push_named_mdl(
                session,
                out,
                MdlObjectType::Event,
                name,
                MdlLockType::SharedRead,
            );
        }
        _ => {}
    }
}

fn push_named_mdl(
    session: &SessionContext,
    out: &mut Vec<MdlRequest>,
    object_type: MdlObjectType,
    name: &str,
    lock_type: MdlLockType,
) {
    let (schema, name) = crate::storage::split_table_name(&session.current_database, name);
    out.push(MdlRequest {
        key: MdlObjectKey::named(object_type, &schema, &name),
        lock_type,
    });
}

fn routine_kind_to_mdl_object_type(kind: RoutineKind) -> MdlObjectType {
    match kind {
        RoutineKind::Procedure => MdlObjectType::Procedure,
        RoutineKind::Function => MdlObjectType::Function,
    }
}

fn insert_mdl_mode(
    out: &mut std::collections::BTreeMap<String, MdlLockType>,
    key: String,
    mode: MdlLockType,
) {
    out.entry(key)
        .and_modify(|existing| {
            if mode_rank(mode) > mode_rank(*existing) {
                *existing = mode;
            }
        })
        .or_insert(mode);
}

fn mode_rank(mode: MdlLockType) -> u8 {
    match mode {
        MdlLockType::IntentionExclusive => 1,
        MdlLockType::SharedRead => 2,
        MdlLockType::SharedWrite => 3,
        MdlLockType::SharedNoWrite => 4,
        MdlLockType::SharedUpgradable => 5,
        MdlLockType::SharedNoReadWrite => 6,
        MdlLockType::Exclusive => 7,
    }
}

fn persistent_table_parts(
    session: &SessionContext,
    name: &str,
) -> Option<(String, String, String)> {
    persistent_table_parts_for_database(session, &session.current_database, name)
}

fn persistent_table_parts_for_database(
    session: &SessionContext,
    current_database: &str,
    name: &str,
) -> Option<(String, String, String)> {
    if is_session_temp_table_ref(session, name) {
        return None;
    }
    let (schema, table) = crate::storage::split_table_name(current_database, name);
    if crate::storage::is_system_database(&schema) {
        return None;
    }
    let key = crate::storage::table_key(&schema, &table);
    Some((schema, table, key))
}

fn split_table_key(key: &str) -> Option<(String, String)> {
    let (schema, table) = key.split_once('.')?;
    Some((schema.to_string(), table.to_string()))
}

fn enforce_explicit_table_locks(
    session: &SessionContext,
    statement: &Statement,
    uses: &[StatementTableUse],
) -> Result<(), QueryError> {
    if !session.has_explicit_table_locks() {
        return Ok(());
    }
    if explicit_locks_reject_statement(statement) {
        return Err(error(
            crate::error::catalog::ER_LOCK_OR_ACTIVE_TRANSACTION,
            "Can't execute the given command because you have active locked tables",
        ));
    }
    for table_use in uses {
        let Some((_schema, table, key)) = persistent_table_parts(session, &table_use.table) else {
            continue;
        };
        let Some(held) = session.explicit_table_locks.get(&key) else {
            return Err(error(
                crate::error::catalog::ER_TABLE_NOT_LOCKED,
                format!("Table '{}' was not locked with LOCK TABLES", table),
            ));
        };
        match (&held.alias, &table_use.alias) {
            (Some(locked_alias), Some(alias)) if locked_alias.eq_ignore_ascii_case(alias) => {}
            (None, None) => {}
            (Some(locked_alias), _) => {
                return Err(error(
                    crate::error::catalog::ER_TABLE_NOT_LOCKED,
                    format!("Table '{}' was not locked with LOCK TABLES", locked_alias),
                ));
            }
            (None, Some(alias)) => {
                return Err(error(
                    crate::error::catalog::ER_TABLE_NOT_LOCKED,
                    format!("Table '{}' was not locked with LOCK TABLES", alias),
                ));
            }
        }
        if table_use.write && held.mode == LockTableMode::Read {
            return Err(error(
                crate::error::catalog::ER_TABLE_NOT_LOCKED_FOR_WRITE,
                format!(
                    "Table '{}' was locked with a READ lock and can't be updated",
                    held.alias.as_deref().unwrap_or(&held.table)
                ),
            ));
        }
    }
    Ok(())
}

fn explicit_locks_reject_statement(statement: &Statement) -> bool {
    // Statements MySQL rejects outright while a session holds explicit table
    // locks (ER_LOCK_OR_ACTIVE_TRANSACTION 1192). Table-scoped DDL/maintenance
    // that MySQL *permits* on a WRITE-locked table — `ALTER TABLE`,
    // `CREATE/DROP INDEX`, `ANALYZE/CHECK/CHECKSUM/REPAIR/OPTIMIZE TABLE` — is
    // deliberately absent: mysqldump wraps its per-table body in
    //   LOCK TABLES `t` WRITE; ALTER TABLE `t` DISABLE KEYS; INSERT …; …
    // and MySQL accepts each. Those statements instead flow through the
    // per-table lock check ([`enforce_explicit_table_locks`]), which requires
    // the target to be held with the appropriate lock (1100/1099 otherwise).
    matches!(
        statement,
        Statement::CreateTable(_)
            | Statement::CreateTableLike { .. }
            | Statement::CreateTableAsSelect { .. }
            | Statement::CreateView { .. }
            | Statement::AlterView { .. }
            | Statement::DropView { .. }
            | Statement::CreateRoutine(_)
            | Statement::AlterRoutine { .. }
            | Statement::DropRoutine { .. }
            | Statement::CreateTrigger(_)
            | Statement::DropTrigger { .. }
            | Statement::CreateEvent(_)
            | Statement::AlterEvent(_)
            | Statement::DropEvent { .. }
    )
}

pub(in crate::engine::executor) fn collect_statement_table_uses<'q>(
    statement: impl Into<crate::parser::QueryRef<'q>>,
) -> Vec<StatementTableUse> {
    let statement = statement.into();
    let mut out = Vec::new();
    let cte_names = std::collections::HashSet::new();
    collect_statement_table_uses_scoped(statement, &cte_names, &mut out);
    out
}

pub(super) fn collect_statement_table_uses_scoped<'q>(
    statement: impl Into<crate::parser::QueryRef<'q>>,
    cte_names: &std::collections::HashSet<String>,
    out: &mut Vec<StatementTableUse>,
) {
    let statement = statement.into();
    match statement {
        crate::parser::QueryRef::Other(Statement::Insert(stmt)) => {
            out.push(StatementTableUse {
                table: stmt.table.clone(),
                alias: None,
                write: true,
            });
            if let InsertSource::Query(select) = &stmt.source {
                collect_statement_table_uses_scoped(select, cte_names, out);
            }
            if let Some(assignments) = &stmt.on_duplicate_key_update {
                for assignment in assignments {
                    collect_expr_table_uses(&assignment.value, cte_names, out);
                }
            }
        }
        crate::parser::QueryRef::Other(Statement::LoadData(stmt)) => out.push(StatementTableUse {
            table: stmt.table.clone(),
            alias: None,
            write: true,
        }),
        crate::parser::QueryRef::Other(Statement::DropTable { names, .. }) => {
            for name in names {
                out.push(StatementTableUse {
                    table: name.clone(),
                    alias: None,
                    write: true,
                });
            }
        }
        crate::parser::QueryRef::Other(Statement::TruncateTable { name }) => {
            out.push(StatementTableUse {
                table: name.clone(),
                alias: None,
                write: true,
            })
        }
        crate::parser::QueryRef::Other(Statement::Update(stmt)) => {
            let primary_name = stmt
                .primary_alias
                .as_deref()
                .unwrap_or_else(|| stmt.table.rsplit('.').next().unwrap_or(&stmt.table));
            let primary_write = stmt.assignments.iter().any(|assignment| {
                assignment.table_qualifier.as_ref().is_none_or(|qualifier| {
                    qualifier.eq_ignore_ascii_case(primary_name)
                        || qualifier.eq_ignore_ascii_case(&stmt.table)
                })
            });
            out.push(StatementTableUse {
                table: stmt.table.clone(),
                alias: stmt.primary_alias.clone(),
                write: primary_write,
            });
            for join in &stmt.joins {
                let join_name = join.table.alias.as_deref().unwrap_or_else(|| {
                    join.table
                        .name
                        .rsplit('.')
                        .next()
                        .unwrap_or(&join.table.name)
                });
                let write = stmt.assignments.iter().any(|assignment| {
                    assignment
                        .table_qualifier
                        .as_ref()
                        .is_some_and(|qualifier| qualifier.eq_ignore_ascii_case(join_name))
                });
                if join.derived.is_none()
                    && !cte_names.contains(&join.table.name.to_ascii_lowercase())
                {
                    out.push(StatementTableUse {
                        table: join.table.name.clone(),
                        alias: join.table.alias.clone(),
                        write,
                    });
                }
                if let Some(condition) = &join.condition {
                    collect_expr_table_uses(condition, cte_names, out);
                }
            }
            for assignment in &stmt.assignments {
                collect_expr_table_uses(&assignment.value, cte_names, out);
            }
            if let Some(selection) = &stmt.selection {
                collect_expr_table_uses(selection, cte_names, out);
            }
        }
        crate::parser::QueryRef::Other(Statement::Delete(stmt)) => {
            let primary_name = stmt
                .primary_alias
                .as_deref()
                .unwrap_or_else(|| stmt.table.rsplit('.').next().unwrap_or(&stmt.table));
            let primary_write = stmt.targets.is_empty()
                || stmt
                    .targets
                    .iter()
                    .any(|target| target.eq_ignore_ascii_case(primary_name));
            out.push(StatementTableUse {
                table: stmt.table.clone(),
                alias: stmt.primary_alias.clone(),
                write: primary_write,
            });
            for join in &stmt.joins {
                let join_name = join.table.alias.as_deref().unwrap_or_else(|| {
                    join.table
                        .name
                        .rsplit('.')
                        .next()
                        .unwrap_or(&join.table.name)
                });
                let write = stmt
                    .targets
                    .iter()
                    .any(|target| target.eq_ignore_ascii_case(join_name));
                if join.derived.is_none()
                    && !cte_names.contains(&join.table.name.to_ascii_lowercase())
                {
                    out.push(StatementTableUse {
                        table: join.table.name.clone(),
                        alias: join.table.alias.clone(),
                        write,
                    });
                }
                if let Some(condition) = &join.condition {
                    collect_expr_table_uses(condition, cte_names, out);
                }
            }
            if let Some(selection) = &stmt.selection {
                collect_expr_table_uses(selection, cte_names, out);
            }
        }
        crate::parser::QueryRef::Select(select) => {
            collect_select_table_uses(select, select.for_update, cte_names, out)
        }
        crate::parser::QueryRef::Rrf(rrf) => {
            for branch in &rrf.branches {
                collect_statement_table_uses_scoped(&branch.query, cte_names, out);
            }
        }
        crate::parser::QueryRef::SetOperation(set) => {
            collect_statement_table_uses_scoped(&set.left, cte_names, out);
            collect_statement_table_uses_scoped(&set.right, cte_names, out);
        }
        crate::parser::QueryRef::WithCte(with) => {
            let mut scoped = cte_names.clone();
            for cte in with.ctes {
                if cte.is_recursive {
                    scoped.insert(cte.name.to_ascii_lowercase());
                }
                collect_statement_table_uses_scoped(&cte.query, &scoped, out);
                scoped.insert(cte.name.to_ascii_lowercase());
            }
            collect_statement_table_uses_scoped(&with.body, &scoped, out);
        }
        crate::parser::QueryRef::Other(Statement::Explain {
            target, analyze, ..
        }) => {
            let start = out.len();
            collect_statement_table_uses_scoped(target, cte_names, out);
            if !*analyze {
                for table_use in &mut out[start..] {
                    table_use.write = false;
                }
            }
        }
        crate::parser::QueryRef::Other(Statement::SelectIntoVars { inner, .. })
        | crate::parser::QueryRef::Other(Statement::SelectIntoFile { inner, .. }) => {
            collect_statement_table_uses_scoped(inner, cte_names, out)
        }
        // Table-scoped DDL / maintenance permitted under LOCK TABLES on a held
        // table (see [`explicit_locks_reject_statement`]). Register the target
        // so the per-table check enforces a lock of the right mode: the
        // mutating forms need WRITE, the read-only integrity probes accept
        // either. Not caught here (empty use list under LOCK TABLES) would let
        // them run against an unlocked table, which MySQL rejects with 1100.
        crate::parser::QueryRef::Other(Statement::AlterTable(stmt)) => {
            out.push(StatementTableUse {
                table: stmt.table.clone(),
                alias: None,
                write: true,
            });
            for action in &stmt.partition_actions {
                if let crate::parser_partition::PartitionAction::ExchangePartition {
                    table, ..
                } = action
                {
                    // An unqualified exchange target inherits the source schema.
                    let table = if let Some((schema, _)) = stmt.table.rsplit_once('.') {
                        let (schema, name) = crate::storage::split_table_name(schema, table);
                        crate::storage::table_key(&schema, &name)
                    } else {
                        table.clone()
                    };
                    out.push(StatementTableUse {
                        table,
                        alias: None,
                        write: true,
                    });
                }
            }
        }
        crate::parser::QueryRef::Other(Statement::CreateIndex(stmt)) => {
            out.push(StatementTableUse {
                table: stmt.table.clone(),
                alias: None,
                write: true,
            })
        }
        crate::parser::QueryRef::Other(Statement::DropIndex { table, .. }) => {
            out.push(StatementTableUse {
                table: table.clone(),
                alias: None,
                write: true,
            })
        }
        crate::parser::QueryRef::Other(Statement::OptimizeTable { tables, .. })
        | crate::parser::QueryRef::Other(Statement::RepairTable { tables }) => {
            for table in tables {
                out.push(StatementTableUse {
                    table: table.clone(),
                    alias: None,
                    write: true,
                });
            }
        }
        crate::parser::QueryRef::Other(Statement::AnalyzeTable { tables, .. }) => {
            for table in tables {
                out.push(StatementTableUse {
                    table: table.clone(),
                    alias: None,
                    write: false,
                });
            }
        }
        crate::parser::QueryRef::Other(Statement::CheckTable { tables })
        | crate::parser::QueryRef::Other(Statement::ChecksumTable { tables, .. }) => {
            for table in tables {
                out.push(StatementTableUse {
                    table: table.clone(),
                    alias: None,
                    write: false,
                });
            }
        }
        _ => {}
    }
}

pub(in crate::engine::executor) fn collect_select_table_uses(
    select: &Select,
    write: bool,
    cte_names: &std::collections::HashSet<String>,
    out: &mut Vec<StatementTableUse>,
) {
    let locking = write || select.for_update;
    for table in &select.from {
        // For the primary inline-derived relation the TableRef name is its
        // query-local alias, not a catalog object. Its real base-table uses
        // are collected recursively from `select.derived` below. Treating the
        // alias as a storage dependency made MDL acquisition fail with 1146
        // before a set-operation branch could be lowered to CteScan.
        if select.derived.is_none()
            && !cte_names.contains(&table.name.to_ascii_lowercase())
            && crate::engine::executor::cte::lookup_cte(&table.name).is_none()
        {
            out.push(StatementTableUse {
                table: table.name.clone(),
                alias: table.alias.clone(),
                write: locking,
            });
        }
    }
    for join in &select.joins {
        if join.derived.is_none()
            && !cte_names.contains(&join.table.name.to_ascii_lowercase())
            && crate::engine::executor::cte::lookup_cte(&join.table.name).is_none()
        {
            out.push(StatementTableUse {
                table: join.table.name.clone(),
                alias: join.table.alias.clone(),
                write: locking,
            });
        }
        if let Some(derived) = &join.derived {
            match derived {
                SubQuery::Select(s) => collect_select_table_uses(s, locking, cte_names, out),
                stmt @ (SubQuery::SetOp(_) | SubQuery::Rrf(_) | SubQuery::WithCte(_)) => {
                    collect_statement_table_uses_scoped(stmt, cte_names, out)
                }
            }
        }
        if let Some(condition) = &join.condition {
            collect_expr_table_uses(condition, cte_names, out);
        }
    }
    if let Some(derived) = &select.derived {
        match derived {
            SubQuery::Select(s) => collect_select_table_uses(s, locking, cte_names, out),
            stmt @ (SubQuery::SetOp(_) | SubQuery::Rrf(_) | SubQuery::WithCte(_)) => {
                collect_statement_table_uses_scoped(stmt, cte_names, out)
            }
        }
    }
    for item in &select.projection {
        if let SelectItem::Expr { expr, .. } = item {
            collect_expr_table_uses(expr, cte_names, out);
        }
    }
    if let Some(selection) = &select.selection {
        collect_expr_table_uses(selection, cte_names, out);
    }
    if let Some(having) = &select.having {
        collect_expr_table_uses(having, cte_names, out);
    }
    for expr in &select.group_by {
        collect_expr_table_uses(expr, cte_names, out);
    }
    for order in &select.order_by {
        collect_expr_table_uses(&order.expr, cte_names, out);
    }
    for window in &select.named_windows {
        for expr in &window.partition_by {
            collect_expr_table_uses(expr, cte_names, out);
        }
        for item in &window.order_by {
            collect_expr_table_uses(&item.expr, cte_names, out);
        }
    }
    if let Some(source) = &select.json_table {
        collect_expr_table_uses(&source.expr, cte_names, out);
    }
    if let Some(source) = &select.values {
        for row in &source.rows {
            for expr in row {
                collect_expr_table_uses(expr, cte_names, out);
            }
        }
    }
}

pub(in crate::engine::executor) fn acquire_serializable_select_table_locks(
    db: &Arc<Database>,
    session: &SessionContext,
    txn: TxnView,
    select: &Select,
) -> Result<(), QueryError> {
    if txn.txn_id == 0 {
        return Ok(());
    }
    let mut uses = Vec::new();
    let cte_names = std::collections::HashSet::new();
    collect_statement_table_uses_scoped(select, &cte_names, &mut uses);
    let mut locked = std::collections::BTreeSet::new();
    for table_use in uses {
        if table_use.write || is_session_temp_table_ref(session, &table_use.table) {
            continue;
        }
        let Ok(table) = db.table(&table_use.table) else {
            continue;
        };
        if table.system || is_system_database(&table.database) {
            continue;
        }
        if locked.insert(table.id) {
            db.acquire_table_shared(txn.txn_id, table.id)?;
        }
    }
    Ok(())
}

pub(super) fn collect_expr_table_uses(
    expr: &Expr,
    cte_names: &std::collections::HashSet<String>,
    out: &mut Vec<StatementTableUse>,
) {
    if matches!(
        expr,
        Expr::Literal(_) | Expr::BoundValue(_) | Expr::Placeholder(_)
    ) {
        return;
    }
    crate::parser::visit::inspect_value_expr(
        expr,
        |_| true,
        |subquery| collect_select_table_uses(subquery, false, cte_names, out),
    );
}
