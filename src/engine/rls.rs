//! Row security metadata and statement-bound authorization.
use crate::engine::auth::{
    AuthCatalog, AuthorizationContext, Privilege, PrivilegeScope, RequiredPrivilege,
};
use crate::parser::rls::{Command, Policy};
use crate::parser::{BinaryOp, Expr, Literal};
use crate::storage::{Database, TableSchema};
use crate::types::{QueryError, Value};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
#[path = "rls/binding.rs"]
mod column_binding;
pub(crate) use column_binding::{bind_target_columns, copied_security, materialize_policy};
#[path = "rls/catalog.rs"]
mod catalog;
pub(crate) use catalog::snapshot_conflict as catalog_snapshot_conflict;
pub(crate) use catalog::{CatalogPublication, CatalogSnapshot, CatalogStore, PolicySource};

pub(crate) fn might_require_binding(db: &Database, table: &TableSchema) -> bool {
    !table.system
        && (table.row_security.version != 0 || db.row_security_may_be_configured(table.object_id))
}

type BindingKey = (usize, u64, u64, Command, bool);
type ContextIntegerSlot = Option<(
    Value,
    crate::engine::executor::mem_budget::ControlledMemoryCharge,
)>;

pub(crate) struct BoundRowSecurity {
    pub(crate) expression: Expr,
    program: std::sync::OnceLock<crate::engine::expr_program::CompiledTableExpr>,
    pub(crate) has_subquery: bool,
    pub(crate) guarded: bool,
    sparse_columns: std::sync::OnceLock<Option<Arc<[usize]>>>,
    _memory: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}
impl BoundRowSecurity {
    /// Sparse writes admit only expressions whose evaluation is diagnostic
    /// free. A failed physical patch may enter the ordinary checked writer;
    /// retrying a volatile or warning-producing policy there would be wrong.
    pub(crate) fn sparse_columns(&self, table: &TableSchema) -> Option<Arc<[usize]>> {
        self.sparse_columns
            .get_or_init(|| {
                if self.guarded || !leakproof_for(table, &self.expression) {
                    return None;
                }
                let mut columns = std::collections::HashSet::new();
                let program = self.program.get_or_init(|| {
                    crate::engine::expr_program::CompiledTableExpr::compile_once(
                        &self.expression,
                        table,
                    )
                });
                if !program.try_collect_referenced_columns(&mut columns) {
                    return None;
                }
                let mut columns: Vec<_> = columns.into_iter().collect();
                columns.sort_unstable();
                Some(columns.into())
            })
            .clone()
    }
}

pub(crate) struct RlsExecutionContext {
    policy_access_observed: bool,
    snapshot: Arc<AuthorizationSnapshot>,
    bindings: std::collections::HashMap<BindingKey, Option<Arc<BoundRowSecurity>>>,
    context_integers: std::collections::HashMap<String, [ContextIntegerSlot; 2]>,
    expression_settings: std::collections::HashMap<
        String,
        (
            Arc<crate::engine::expr::SessionExprInfo>,
            crate::engine::executor::mem_budget::ControlledMemoryCharge,
        ),
    >,
}
pub(crate) fn policy_access_observed() -> bool {
    EXECUTION.with(|state| {
        state
            .borrow()
            .as_ref()
            .is_some_and(|state| state.policy_access_observed)
    })
}

pub(crate) fn policy_expression_info(
    encoded: &str,
    create: impl FnOnce() -> Result<Arc<crate::engine::expr::SessionExprInfo>, QueryError>,
) -> Result<Arc<crate::engine::expr::SessionExprInfo>, QueryError> {
    if let Some(info) = EXECUTION.with(|state| {
        state.borrow().as_ref().and_then(|state| {
            state
                .expression_settings
                .get(encoded)
                .map(|(info, _)| info.clone())
        })
    }) {
        return Ok(info);
    }
    let memory = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
        encoded
            .len()
            .saturating_mul(4)
            .saturating_add(std::mem::size_of::<crate::engine::expr::SessionExprInfo>())
            .saturating_add(2048),
    )?;
    let info = create()?;
    EXECUTION.with(|state| {
        if let Some(state) = state.borrow_mut().as_mut() {
            state
                .expression_settings
                .insert(encoded.to_string(), (info.clone(), memory));
        }
    });
    Ok(info)
}

pub(crate) struct AuthorizationSnapshot {
    auth: std::sync::OnceLock<(usize, Arc<AuthCatalog>, u64, Arc<CatalogSnapshot>)>,
    policy: crate::engine::auth::PolicySnapshot,
}

fn capture_catalogs(db: &Database) -> (usize, Arc<AuthCatalog>, u64, Arc<CatalogSnapshot>) {
    loop {
        let generation = crate::engine::auth::authorization_generation();
        let auth = db.auth_catalog();
        let policies = db.row_security_catalog_snapshot();
        if generation == crate::engine::auth::authorization_generation() {
            return (db as *const Database as usize, auth, generation, policies);
        }
    }
}

pub(crate) fn statement_catalog(db: &Database) -> Result<Arc<CatalogSnapshot>, QueryError> {
    EXECUTION.with(|state| {
        let state = state.borrow();
        let Some(state) = state.as_ref() else {
            return Ok(db.row_security_catalog_snapshot());
        };
        let (owner, _, _, policies) = state.snapshot.auth.get_or_init(|| capture_catalogs(db));
        if *owner != db as *const Database as usize {
            return Err(catalog_snapshot_conflict());
        }
        Ok(policies.clone())
    })
}
pub(crate) fn statement_security(
    db: &Database,
    table: &TableSchema,
) -> Result<PolicySource, QueryError> {
    statement_catalog(db)?.resolve(table, |id, version| {
        db.load_row_security_snapshot(id, version)
    })
}
pub(crate) fn statement_security_enabled(
    db: &Database,
    table: &TableSchema,
) -> Result<bool, QueryError> {
    if !might_require_binding(db, table) {
        return Ok(false);
    }
    Ok(statement_catalog(db)?.flags(table)?.1)
}
#[derive(Clone)]
pub(crate) struct EvaluationSnapshot {
    authorization: Arc<AuthorizationSnapshot>,
    ancestors: Vec<u64>,
}
pub(crate) fn evaluation_snapshot() -> Option<EvaluationSnapshot> {
    EXECUTION.with(|state| {
        state.borrow().as_ref().map(|state| EvaluationSnapshot {
            authorization: state.snapshot.clone(),
            ancestors: POLICY_STACK.with(|stack| stack.borrow().clone()),
        })
    })
}

/// One immutable catalog root, including nested identities and worker scopes.
pub(crate) fn authorization_snapshot(db: &Database) -> Arc<AuthCatalog> {
    EXECUTION.with(|state| {
        let state = state.borrow();
        let Some(state) = state.as_ref() else {
            return db.auth_catalog();
        };
        let (owner, auth, _, _) = state.snapshot.auth.get_or_init(|| capture_catalogs(db));
        // A scope cannot transfer authorization between database instances.
        if *owner == db as *const Database as usize {
            auth.clone()
        } else {
            db.auth_catalog()
        }
    })
}

/// Bind a diagnostic-free integer context value once per statement/worker.
/// This is an execution slot; shared plans and expression programs retain
/// the context read rather than a tenant-specific constant.
pub(crate) fn context_integer_slot(
    key: &str,
    unsigned: bool,
    compute: impl FnOnce() -> Option<Value>,
) -> Option<Value> {
    let cached = EXECUTION.with(|state| {
        state.borrow().as_ref().and_then(|state| {
            state
                .context_integers
                .get(key)
                .and_then(|slots| slots[usize::from(unsigned)].as_ref())
                .map(|(value, _)| value.clone())
        })
    });
    if cached.is_some() {
        return cached;
    }
    let value = compute()?;
    if EXECUTION.with(|state| state.borrow().is_some()) {
        let bytes = key.len().saturating_add(512);
        if let Some(memory) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
                bytes,
            )
        {
            EXECUTION.with(|state| {
                if let Some(state) = state.borrow_mut().as_mut() {
                    state
                        .context_integers
                        .entry(key.to_string())
                        .or_insert_with(|| [None, None])[usize::from(unsigned)] =
                        Some((value.clone(), memory));
                }
            });
        }
    }
    Some(value)
}

thread_local! {
    static EXECUTION: std::cell::RefCell<Option<RlsExecutionContext>> = const { std::cell::RefCell::new(None) };
    static TRUSTED_INTERNAL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// A Rust-only capability for physical/internal replay, never a SQL sysvar.
pub(crate) struct TrustedInternalScope(bool);
impl TrustedInternalScope {
    pub(crate) fn enter() -> Self {
        Self(TRUSTED_INTERNAL.with(|scope| scope.replace(true)))
    }
}
impl Drop for TrustedInternalScope {
    fn drop(&mut self) {
        TRUSTED_INTERNAL.with(|scope| scope.set(self.0));
    }
}

/// A connection/worker expression scope owns its own cache. Nested DEFINER
/// scopes replace it and restore it on exit; no per-row shared mutex is used.
pub(crate) struct EvaluationScope {
    previous: Option<RlsExecutionContext>,
    ancestors: Vec<u64>,
    _policy: crate::engine::auth::PolicySnapshotScope,
}
impl EvaluationScope {
    pub(crate) fn enter_statement(db: &Database) -> Self {
        if EXECUTION.with(|state| state.borrow().is_none()) {
            // Capture settings and both immutable catalogs at one stable
            // authorization generation before any user expression runs.
            let snapshot = loop {
                let generation = crate::engine::auth::authorization_generation();
                let policy = crate::engine::auth::PolicySnapshot::capture();
                let catalogs = capture_catalogs(db);
                if generation != crate::engine::auth::authorization_generation() {
                    continue;
                }
                let auth = std::sync::OnceLock::new();
                let _ = auth.set(catalogs);
                break Arc::new(AuthorizationSnapshot { auth, policy });
            };
            let captured = EvaluationSnapshot {
                authorization: snapshot,
                ancestors: POLICY_STACK.with(|stack| stack.borrow().clone()),
            };
            return Self::enter_snapshot(Some(&captured));
        }
        let scope = Self::enter();
        authorization_snapshot(db);
        scope
    }
    pub(crate) fn enter() -> Self {
        Self::enter_snapshot(None)
    }
    pub(crate) fn enter_snapshot(captured: Option<&EvaluationSnapshot>) -> Self {
        let current = EXECUTION.with(|state| state.borrow().as_ref().map(|s| s.snapshot.clone()));
        let snapshot = captured
            .map(|s| s.authorization.clone())
            .or_else(|| current.clone())
            .unwrap_or_else(|| {
                Arc::new(AuthorizationSnapshot {
                    auth: std::sync::OnceLock::new(),
                    policy: crate::engine::auth::PolicySnapshot::capture(),
                })
            });
        let policy = crate::engine::auth::PolicySnapshotScope::enter(&snapshot.policy);
        let ancestors = POLICY_STACK.with(|stack| {
            let mut stack = stack.borrow_mut();
            let previous = stack.clone();
            if let Some(captured) = captured {
                if stack.is_empty()
                    || current
                        .as_ref()
                        .is_none_or(|current| !Arc::ptr_eq(current, &captured.authorization))
                {
                    *stack = captured.ancestors.clone();
                }
            }
            previous
        });
        Self {
            previous: EXECUTION.with(|state| {
                state.borrow_mut().replace(RlsExecutionContext {
                    policy_access_observed: false,
                    snapshot,
                    bindings: std::collections::HashMap::new(),
                    context_integers: std::collections::HashMap::new(),
                    expression_settings: std::collections::HashMap::new(),
                })
            }),
            ancestors,
            _policy: policy,
        }
    }
}
impl Drop for EvaluationScope {
    fn drop(&mut self) {
        EXECUTION.with(|state| {
            let mut state = state.borrow_mut();
            if let Some(previous) = self.previous.as_mut() {
                previous.policy_access_observed |= state
                    .as_ref()
                    .is_some_and(|state| state.policy_access_observed);
            }
            *state = self.previous.take();
        });
        POLICY_STACK.with(|stack| *stack.borrow_mut() = std::mem::take(&mut self.ancestors));
    }
}

pub(crate) fn binding(
    db: &Database,
    table: &TableSchema,
    command: Command,
    check: bool,
) -> Result<Option<Arc<BoundRowSecurity>>, QueryError> {
    if !might_require_binding(db, table) {
        return Ok(None);
    }
    if TRUSTED_INTERNAL.with(|scope| scope.get()) {
        return Ok(None);
    }
    let key = (
        db as *const Database as usize,
        table.object_id,
        table.schema_version,
        command,
        check,
    );
    if let Some(cached) = EXECUTION.with(|state| {
        state
            .borrow()
            .as_ref()
            .and_then(|state| state.bindings.get(&key).cloned())
    }) {
        return Ok(cached);
    }
    let auth = authorization_snapshot(db);
    let context = crate::engine::expr::current_authorization_context();
    let result = if context
        .as_ref()
        .is_some_and(|(ctx, internal)| can_bypass(&auth, ctx, *internal))
    {
        None
    } else {
        let security = statement_security(db, table)?;
        if !security.enabled {
            EXECUTION.with(|state| {
                if let Some(state) = state.borrow_mut().as_mut() {
                    state.bindings.insert(key, None);
                }
            });
            return Ok(None);
        }
        let (ctx, _internal) =
            context.ok_or_else(|| violation("RLS table access has no authorization context"))?;
        if !crate::engine::expr::row_security_enabled() {
            return Err(violation("row_security=OFF cannot bypass row security"));
        }
        let roles = auth.effective_roles(&ctx);
        for policy in &security.policies {
            if policy.command != Command::All && policy.command != command {
                continue;
            }
            if !policy.public
                && !policy
                    .roles
                    .iter()
                    .any(|r| r == &ctx.authenticated_as || roles.contains(r))
            {
                continue;
            }
            for (id, name) in &policy.table_dependencies {
                if !db.table(name).is_ok_and(|table| table.object_id == *id) {
                    return Err(violation(
                        "Row security policy dependency changed or disappeared",
                    ));
                }
            }
        }
        let expression = predicate(&auth, &ctx, table, &security, command, check)?;
        crate::engine::executor::rls::authorize_policy_dependencies(
            db,
            &auth,
            &ctx,
            table,
            &expression,
        )?;
        let mut has_subquery = false;
        let mut stored_function = false;
        struct Subqueries<'a>(&'a mut bool, &'a mut bool);
        impl crate::parser::visit::AstVisitor for Subqueries<'_> {
            fn enter_select(&mut self, _: &crate::parser::Select) {
                *self.0 = true;
            }
            fn enter_expr(&mut self, expr: &Expr) {
                if let Expr::Function { name, .. } = expr {
                    if !crate::engine::cbo::function_cost::is_known_builtin_function(name) {
                        *self.1 = true;
                    }
                }
            }
        }
        crate::parser::visit::visit_expr(
            &mut Subqueries(&mut has_subquery, &mut stored_function),
            &expression,
        );
        // Read planning consumes the AST directly. Preserve eager write
        // binding so compilation never moves into a later locked-row check.
        let program = if command == Command::Select {
            std::sync::OnceLock::new()
        } else {
            std::sync::OnceLock::from(
                crate::engine::expr_program::CompiledTableExpr::compile_once(&expression, table),
            )
        };
        let memory = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
            std::mem::size_of::<BoundRowSecurity>()
                + crate::engine::expr_program::expr_ast_heap_bytes(&expression)
                + table.columns.len() * std::mem::size_of::<usize>()
                + 512,
        )?;
        Some(Arc::new(BoundRowSecurity {
            expression,
            program,
            has_subquery,
            guarded: has_subquery || stored_function,
            sparse_columns: std::sync::OnceLock::new(),
            _memory: memory,
        }))
    };
    EXECUTION.with(|state| {
        if let Some(state) = state.borrow_mut().as_mut() {
            state.policy_access_observed |= result.is_some();
            state.bindings.insert(key, result.clone());
        }
    });
    Ok(result)
}

pub(crate) fn row_allowed(
    db: &Database,
    txn: crate::storage::TxnView,
    table: &TableSchema,
    command: Command,
    check: bool,
    values: &[Value],
) -> Result<bool, QueryError> {
    let Some(bound) = binding(db, table, command, check)? else {
        return Ok(true);
    };
    let _policy = bound
        .guarded
        .then(|| PolicyScope::enter(table.id))
        .transpose()?;
    if bound.has_subquery {
        crate::engine::executor::rls::evaluate_subquery_policy(
            db,
            txn,
            table,
            &bound.expression,
            values,
        )
    } else {
        bound
            .program
            .get_or_init(|| {
                crate::engine::expr_program::CompiledTableExpr::compile_once(
                    &bound.expression,
                    table,
                )
            })
            .eval_bool(table, values)
    }
}

pub(crate) fn check_row(
    db: &Database,
    txn: crate::storage::TxnView,
    table: &TableSchema,
    command: Command,
    check: bool,
    values: &[Value],
) -> Result<(), QueryError> {
    if !row_allowed(db, txn, table, command, check, values)? {
        return Err(violation(format!(
            "Row violates {:?} security policy for table '{}.{}'",
            command, table.database, table.name
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RowSecurity {
    pub enabled: bool,
    pub version: u64,
    pub policies: Vec<Policy>,
}

impl RowSecurity {
    pub fn empty() -> Arc<Self> {
        static EMPTY: std::sync::OnceLock<Arc<RowSecurity>> = std::sync::OnceLock::new();
        EMPTY.get_or_init(|| Arc::new(Self::default())).clone()
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        if !self.enabled && self.version == 0 && self.policies.is_empty() {
            return 0;
        }
        std::mem::size_of::<Self>()
            + self.policies.capacity() * std::mem::size_of::<Policy>()
            + self
                .policies
                .iter()
                .map(|policy| {
                    policy.name.capacity()
                        + policy
                            .using_settings
                            .iter()
                            .chain(&policy.check_settings)
                            .map(|settings| settings.allocated_bytes())
                            .sum::<usize>()
                        + (policy.using_columns.capacity() + policy.check_columns.capacity())
                            * std::mem::size_of::<crate::parser::rls::PolicyColumnReference>()
                        + policy.roles.capacity()
                            * std::mem::size_of::<crate::engine::auth::AccountKey>()
                        + policy
                            .roles
                            .iter()
                            .map(|role| role.user.capacity() + role.host.capacity())
                            .sum::<usize>()
                        + policy
                            .using
                            .iter()
                            .chain(&policy.check)
                            .map(crate::engine::expr_program::expr_ast_heap_bytes)
                            .sum::<usize>()
                        + policy.table_dependencies.capacity()
                            * std::mem::size_of::<(u64, String)>()
                        + policy
                            .table_dependencies
                            .iter()
                            .map(|(_, name)| name.capacity())
                            .sum::<usize>()
                })
                .sum::<usize>()
    }
}

#[derive(Debug)]
struct ContextValue {
    text: Option<String>,
    _memory: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}
type ContextValues = imbl::OrdMap<String, Arc<ContextValue>>;
#[derive(Debug, Clone)]
pub(crate) struct ContextSavepoint {
    name: String,
    local: ContextValues,
    _memory: Arc<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}
#[derive(Debug, Clone)]
pub struct SessionContext {
    pub enabled: bool,
    values: ContextValues,
    local: ContextValues,
    savepoints: Vec<ContextSavepoint>,
    identity: std::cell::RefCell<Option<Arc<PlanIdentity>>>,
}
impl Default for SessionContext {
    fn default() -> Self {
        Self {
            enabled: true,
            values: ContextValues::new(),
            local: ContextValues::new(),
            savepoints: Vec::new(),
            identity: Default::default(),
        }
    }
}
impl SessionContext {
    pub(crate) fn snapshot(&self) -> ContextSnapshot {
        ContextSnapshot {
            enabled: self.enabled,
            values: self.values.clone(),
            local: self.local.clone(),
        }
    }
    pub(crate) fn set(
        &mut self,
        key: String,
        text: Option<String>,
        local: bool,
    ) -> Result<(), QueryError> {
        // Persistent roots share values and their leases with savepoints and
        // workers. Reserve before changing either visible map, including when
        // a session SET replaces an earlier LOCAL shadow.
        if text.is_none() && !local {
            self.reset(Some(&key));
            return Ok(());
        }
        let bytes = 512usize
            .saturating_add(key.capacity().saturating_mul(4))
            .saturating_add(text.as_ref().map_or(0, String::capacity));
        let memory = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
            bytes,
        )?;
        let value = Arc::new(ContextValue {
            text,
            _memory: memory,
        });
        if local {
            self.local.insert(key, value);
        } else {
            self.local.remove(&key);
            self.values.insert(key, value);
        }
        Ok(())
    }
    pub(crate) fn reset(&mut self, key: Option<&str>) {
        if let Some(key) = key {
            self.values.remove(key);
            self.local.remove(key);
        } else {
            self.values = ContextValues::new();
            self.local = ContextValues::new();
        }
    }
    pub fn value(&self, key: &str) -> Value {
        context_value(&self.values, &self.local, key)
    }
    pub fn finish_transaction(&mut self) {
        self.local = ContextValues::new();
        self.savepoints = Vec::new();
    }
    pub(crate) fn prepare_savepoint(&self, name: &str) -> Result<ContextSavepoint, QueryError> {
        let memory = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
            name.len().saturating_add(512),
        )?;
        Ok(ContextSavepoint {
            name: name.to_string(),
            local: self.local.clone(),
            _memory: Arc::new(memory),
        })
    }
    pub(crate) fn install_savepoint(&mut self, savepoint: ContextSavepoint) {
        self.savepoints
            .retain(|s| !s.name.eq_ignore_ascii_case(&savepoint.name));
        self.savepoints.push(savepoint);
    }
    pub fn rollback_to(&mut self, name: &str) {
        if let Some(index) = self
            .savepoints
            .iter()
            .rposition(|s| s.name.eq_ignore_ascii_case(name))
        {
            self.local = self.savepoints[index].local.clone();
            self.savepoints.truncate(index + 1);
        }
    }
    pub fn release(&mut self, name: &str) {
        self.savepoints
            .retain(|s| !s.name.eq_ignore_ascii_case(name));
        if self.savepoints.is_empty() {
            self.savepoints = Vec::new();
        }
    }
}
fn context_value(values: &ContextValues, local: &ContextValues, key: &str) -> Value {
    local
        .get(key)
        .or_else(|| values.get(key))
        .and_then(|entry| entry.text.as_ref())
        .map_or(Value::Null, |text| Value::String(text.clone()))
}
#[derive(Clone)]
pub(crate) struct ContextSnapshot {
    pub(crate) enabled: bool,
    values: ContextValues,
    local: ContextValues,
}
impl ContextSnapshot {
    pub(crate) fn value(&self, key: &str) -> Value {
        context_value(&self.values, &self.local, key)
    }
}

#[cfg(test)]
mod context_tests {
    use super::*;

    #[test]
    fn context_snapshots_and_savepoints_retain_and_release_governed_values() {
        use crate::engine::executor::mem_budget::controlled_memory_snapshot;
        let before = controlled_memory_snapshot().0;
        let mut session = SessionContext::default();
        session
            .set("tenant".into(), Some("session".into()), false)
            .unwrap();
        session
            .set("tenant".into(), Some("local".into()), true)
            .unwrap();
        let saved = session.prepare_savepoint("point").unwrap();
        session.install_savepoint(saved);
        let snapshot = session.snapshot();
        session.set("tenant".into(), None, true).unwrap();
        assert_eq!(session.value("tenant"), Value::Null);
        session.rollback_to("point");
        assert_eq!(session.value("tenant"), Value::String("local".into()));
        session.reset(None);
        session.finish_transaction();
        drop(session);
        assert_eq!(snapshot.value("tenant"), Value::String("local".into()));
        assert!(controlled_memory_snapshot().0 > before);
        drop(snapshot);
        assert_eq!(controlled_memory_snapshot().0, before);
    }

    #[test]
    fn rejected_context_allocation_preserves_session_and_local_shadow() {
        let mut session = SessionContext::default();
        session
            .set("tenant".into(), Some("session".into()), false)
            .unwrap();
        session
            .set("tenant".into(), Some("local".into()), true)
            .unwrap();
        crate::engine::executor::mem_budget::with_test_effective_memory_limit(1024 * 1024, || {
            assert!(session
                .set("tenant".into(), Some("x".repeat(2 * 1024 * 1024)), false)
                .is_err());
        });
        assert_eq!(session.value("tenant"), Value::String("local".into()));
        session.finish_transaction();
        assert_eq!(session.value("tenant"), Value::String("session".into()));
    }
}

thread_local! { static STRICT_OLD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
pub(crate) struct StrictOldScope(bool);
impl StrictOldScope {
    pub(crate) fn enter() -> Self {
        Self(STRICT_OLD.with(|old| old.replace(true)))
    }
}
impl Drop for StrictOldScope {
    fn drop(&mut self) {
        STRICT_OLD.with(|old| old.set(self.0));
    }
}
pub(crate) fn strict_old() -> bool {
    STRICT_OLD.with(|old| old.get())
}

thread_local! { static STATEMENT_VIEW: std::cell::RefCell<Option<(Arc<Database>, crate::storage::TxnView)>> = const { std::cell::RefCell::new(None) }; }
pub(crate) struct StatementViewScope(Option<(Arc<Database>, crate::storage::TxnView)>);
impl StatementViewScope {
    pub(crate) fn enter(db: &Arc<Database>, txn: crate::storage::TxnView) -> Self {
        Self(STATEMENT_VIEW.with(|view| view.borrow_mut().replace((db.clone(), txn))))
    }
}
impl Drop for StatementViewScope {
    fn drop(&mut self) {
        STATEMENT_VIEW.with(|view| *view.borrow_mut() = self.0.take());
    }
}
pub(crate) fn current_statement_view() -> Option<(Arc<Database>, crate::storage::TxnView)> {
    STATEMENT_VIEW.with(|view| view.borrow().clone())
}

thread_local! { static POLICY_STACK: std::cell::RefCell<Vec<u64>> = const { std::cell::RefCell::new(Vec::new()) }; }
pub(crate) struct PolicyScope;
impl PolicyScope {
    pub(crate) fn enter(id: u64) -> Result<Self, QueryError> {
        POLICY_STACK.with(|stack| {
            let mut stack = stack.borrow_mut();
            if stack.contains(&id) || stack.len() >= 64 {
                return Err(violation("Recursive row security policy"));
            }
            stack.push(id);
            Ok(Self)
        })
    }
}
impl Drop for PolicyScope {
    fn drop(&mut self) {
        POLICY_STACK.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}
pub(crate) fn policy_active() -> bool {
    POLICY_STACK.with(|stack| !stack.borrow().is_empty())
}

pub fn has_global(auth: &AuthCatalog, ctx: &AuthorizationContext, privilege: Privilege) -> bool {
    auth.account_has_privilege(
        ctx,
        &RequiredPrivilege {
            privilege,
            scope: PrivilegeScope::Global,
        },
        false,
    )
}

pub fn can_bypass(auth: &AuthCatalog, ctx: &AuthorizationContext, _internal: bool) -> bool {
    trusted_internal()
        || has_global(auth, ctx, Privilege::Super)
        || has_global(auth, ctx, Privilege::Dynamic("BYPASS_RLS".into()))
}

pub(crate) fn trusted_internal() -> bool {
    TRUSTED_INTERNAL.with(|scope| scope.get())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanIdentity {
    account: crate::engine::auth::AccountKey,
    roles: Vec<crate::engine::auth::AccountKey>,
    generation: u64,
    auth_epoch: u64,
    security_epoch: u64,
    policy: crate::engine::auth::PolicySnapshot,
    internal: bool,
    trusted: bool,
    enabled: bool,
    parameter_domains: Vec<std::mem::Discriminant<Value>>,
    expression_settings: Arc<crate::engine::expr::PolicyExpressionSettings>,
}

/// Statement-bound catalog/settings state shared by identity construction and
/// validation. Keep validation independent of the connection's last signature:
/// alternating prepared handles can retain different parameter domains.
struct PlanIdentityEnvironment {
    generation: u64,
    auth_epoch: u64,
    security_epoch: u64,
    policy: crate::engine::auth::PolicySnapshot,
    trusted: bool,
}

impl PlanIdentityEnvironment {
    fn capture(db: &Database) -> Self {
        let auth = authorization_snapshot(db);
        let generation = EXECUTION
            .with(|state| {
                state.borrow().as_ref().and_then(|s| {
                    s.snapshot
                        .auth
                        .get()
                        .map(|(_, _, generation, _)| *generation)
                })
            })
            .unwrap_or_else(crate::engine::auth::authorization_generation);
        Self {
            generation,
            auth_epoch: auth.epoch,
            security_epoch: statement_catalog(db).map_or(0, |catalog| catalog.epoch),
            policy: crate::engine::auth::PolicySnapshot::capture(),
            trusted: trusted_internal(),
        }
    }
}

impl PlanIdentity {
    pub(crate) fn matches_session(
        &self,
        db: &Database,
        session: &crate::engine::api::SessionContext,
    ) -> bool {
        self.matches_environment(session, &PlanIdentityEnvironment::capture(db))
    }

    fn matches_environment(
        &self,
        session: &crate::engine::api::SessionContext,
        environment: &PlanIdentityEnvironment,
    ) -> bool {
        self.account == session.authenticated_as
            && self.roles.iter().eq(session.active_roles.iter())
            && self.generation == environment.generation
            && self.auth_epoch == environment.auth_epoch
            && self.security_epoch == environment.security_epoch
            && self.policy == environment.policy
            && self.internal == session.internal_system_session
            && self.trusted == environment.trusted
            && self.enabled == session.rls.enabled
            && self.expression_settings.matches_session(session)
            && crate::engine::params::with_current_params(|params, _| {
                self.parameter_domains.iter().copied().eq(params
                    .unwrap_or_default()
                    .iter()
                    .map(std::mem::discriminant))
            })
    }
}

pub fn session_signature(
    db: &Database,
    session: &crate::engine::api::SessionContext,
) -> Arc<PlanIdentity> {
    let environment = PlanIdentityEnvironment::capture(db);
    let mut cached = session.rls.identity.borrow_mut();
    if let Some(identity) = cached.as_ref() {
        if identity.matches_environment(session, &environment) {
            return identity.clone();
        }
    }
    let parameter_domains = crate::engine::params::with_current_params(|params, _| {
        params
            .unwrap_or_default()
            .iter()
            .map(std::mem::discriminant)
            .collect()
    });
    let identity = Arc::new(PlanIdentity {
        account: session.authenticated_as.clone(),
        roles: session.active_roles.iter().cloned().collect(),
        generation: environment.generation,
        auth_epoch: environment.auth_epoch,
        security_epoch: environment.security_epoch,
        policy: environment.policy,
        internal: session.internal_system_session,
        trusted: environment.trusted,
        enabled: session.rls.enabled,
        parameter_domains,
        expression_settings: Arc::new(crate::engine::expr::PolicyExpressionSettings::capture(
            session,
        )),
    });
    *cached = Some(identity.clone());
    identity
}

pub fn violation(message: impl Into<String>) -> QueryError {
    crate::error::catalog::ER_ROW_SECURITY_VIOLATION.error(message)
}

pub(crate) fn metadata_rows(
    db: &Database,
    filter: impl Fn(&str, &str) -> bool,
) -> Result<Vec<Vec<Value>>, QueryError> {
    let Some((context, _internal)) = crate::engine::expr::current_authorization_context() else {
        return Err(violation(
            "Policy metadata requires an authorization context",
        ));
    };
    if !trusted_internal()
        && !has_global(
            &authorization_snapshot(db),
            &context,
            Privilege::Dynamic("ROW_SECURITY_ADMIN".into()),
        )
    {
        return Ok(Vec::new());
    }
    let keys: Vec<_> = db
        .catalog_read()?
        .tables
        .values()
        .filter(|t| t.row_security.version != 0 && filter(&t.database, &t.name))
        .map(|t| t.qualified_name())
        .collect();
    let mut rows = Vec::new();
    for key in keys {
        let table = db.table(&key)?;
        for stored in &table.row_security.policies {
            let mut policy = stored.clone();
            materialize_policy(&table, &mut policy)?;
            rows.push(vec![
                Value::String(table.database.clone()),
                Value::String(table.name.clone()),
                Value::String(policy.name.clone()),
                Value::String(format!("{:?}", policy.command).to_ascii_uppercase()),
                Value::String(
                    if policy.permissive {
                        "PERMISSIVE"
                    } else {
                        "RESTRICTIVE"
                    }
                    .into(),
                ),
                Value::String(if policy.public {
                    "PUBLIC".into()
                } else {
                    policy
                        .roles
                        .iter()
                        .map(|r| r.role_sql_name())
                        .collect::<Vec<_>>()
                        .join(", ")
                }),
                policy
                    .using
                    .as_ref()
                    .map(|e| {
                        crate::parser::try_query_expr_to_sql(e)
                            .map(Value::String)
                            .map_err(|error| violation(error.to_string()))
                    })
                    .transpose()?
                    .unwrap_or(Value::Null),
                policy
                    .check
                    .as_ref()
                    .map(|e| {
                        crate::parser::try_query_expr_to_sql(e)
                            .map(Value::String)
                            .map_err(|error| violation(error.to_string()))
                    })
                    .transpose()?
                    .unwrap_or(Value::Null),
                Value::Int(i64::from(table.row_security.enabled)),
                Value::UInt(table.row_security.version),
            ]);
        }
    }
    Ok(rows)
}

pub fn and(left: Expr, right: Expr) -> Expr {
    if matches!(&left, Expr::Literal(Literal::Bool(true))) {
        return right;
    }
    if matches!(&right, Expr::Literal(Literal::Bool(true))) {
        return left;
    }
    if matches!(&left, Expr::Literal(Literal::Bool(false))) {
        return left;
    }
    Expr::Binary {
        left: Box::new(left),
        op: BinaryOp::And,
        right: Box::new(right),
    }
}
fn or(left: Expr, right: Expr) -> Expr {
    if matches!(&left, Expr::Literal(Literal::Bool(false))) {
        return right;
    }
    if matches!(&right, Expr::Literal(Literal::Bool(false))) {
        return left;
    }
    if matches!(&left, Expr::Literal(Literal::Bool(true))) {
        return left;
    }
    Expr::Binary {
        left: Box::new(left),
        op: BinaryOp::Or,
        right: Box::new(right),
    }
}
fn boolean(value: bool) -> Expr {
    Expr::Literal(Literal::Bool(value))
}

/// Resolve a command independently; absence of a permissive policy denies.
pub fn predicate(
    auth: &AuthCatalog,
    ctx: &AuthorizationContext,
    table: &TableSchema,
    security: &RowSecurity,
    command: Command,
    check: bool,
) -> Result<Expr, QueryError> {
    let roles = auth.effective_roles(ctx);
    let mut allow = None;
    let mut restrict = None;
    for policy in &security.policies {
        if policy.command != Command::All && policy.command != command {
            continue;
        }
        if !policy.public
            && !policy
                .roles
                .iter()
                .any(|r| r == &ctx.authenticated_as || roles.contains(r))
        {
            continue;
        }
        let source = if check {
            policy.check.as_ref().or(policy.using.as_ref())
        } else {
            policy.using.as_ref()
        };
        let references = if check && policy.check.is_some() {
            &policy.check_columns
        } else {
            &policy.using_columns
        };
        let mut expression = source.cloned().unwrap_or_else(|| boolean(true));
        column_binding::materialize_target_columns(table, &mut expression, references)?;
        let settings = if check && policy.check.is_some() {
            &policy.check_settings
        } else {
            &policy.using_settings
        };
        if let Some(settings) = settings {
            struct Queries<'a>(&'a Arc<crate::engine::expr::PolicyExpressionSettings>);
            impl crate::parser::visit::AstVisitorMut for Queries<'_> {
                fn enter_select(&mut self, select: &mut crate::parser::Select) {
                    select.rls_expression_settings = Some(self.0.clone());
                }
            }
            crate::parser::visit::visit_expr_mut(&mut Queries(settings), &mut expression);
        }
        if let Some(settings) = settings
            .as_ref()
            .filter(|settings| !settings.matches_current())
        {
            expression = Expr::Function {
                name: "__MYDB_RLS_SETTINGS".into(),
                args: vec![
                    Expr::Literal(Literal::String(
                        serde_json::to_string(settings)
                            .map_err(|_| violation("Cannot encode policy expression settings"))?,
                    )),
                    expression,
                ],
                distinct: false,
            };
        }
        let target = if policy.permissive {
            &mut allow
        } else {
            &mut restrict
        };
        *target = Some(match target.take() {
            None => expression,
            Some(previous) => {
                if policy.permissive {
                    or(previous, expression)
                } else {
                    and(previous, expression)
                }
            }
        });
    }
    // Restrictive policies cannot authorize a row by themselves. Do not
    // retain their subqueries/functions behind FALSE: dependency binding
    // must not require privileges for a predicate that cannot be applied.
    let Some(allow) = allow else {
        return Ok(boolean(false));
    };
    Ok(match restrict {
        Some(restrict) => and(allow, restrict),
        None => allow,
    })
}

/// Non-leakproof user predicates stay in a short-circuit CASE lane. Typed
/// equality/range predicates can still supply index and Zone Map candidates.
pub fn protect_filter(policy: Expr, user: Option<Expr>) -> Expr {
    match user {
        None => policy,
        Some(user) if is_leakproof(&user) => and(policy, user),
        Some(user) => Expr::Case {
            operand: None,
            conditions: vec![policy].into_boxed_slice(),
            results: vec![user],
            else_result: Some(Box::new(boolean(false))),
        },
    }
}

pub fn is_leakproof(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(_))
}

/// A narrow type proof, rather than a function/operator name allowlist.
/// Parameter domains are part of the exact cached plan identity above.
fn leakproof_for(table: &TableSchema, expr: &Expr) -> bool {
    use crate::storage::DataTypeKind as T;
    let numeric_column = |expr: &Expr| {
        crate::engine::expr::expr_column_index(expr, table).is_some_and(|i| {
            matches!(
                table.columns[i].data_type,
                T::TinyInt
                    | T::SmallInt
                    | T::MediumInt
                    | T::Int
                    | T::BigInt
                    | T::Bool
                    | T::Float { .. }
                    | T::Double { .. }
                    | T::Decimal { .. }
            )
        })
    };
    let numeric_value = |expr: &Expr| match expr {
        Expr::Literal(
            Literal::Int(_)
            | Literal::UInt(_)
            | Literal::Float(_)
            | Literal::Decimal(_)
            | Literal::Bool(_)
            | Literal::Null,
        ) => true,
        Expr::BoundValue(value) => matches!(
            value,
            Value::Int(_)
                | Value::UInt(_)
                | Value::Float(_)
                | Value::Double(_)
                | Value::Decimal(_)
                | Value::DecimalCompact(..)
                | Value::Decimal128(..)
                | Value::DecimalWide(..)
                | Value::Null
        ),
        Expr::Placeholder(index) => crate::engine::params::with_current_param(*index, |value| {
            matches!(
                value,
                Some(
                    Value::Int(_)
                        | Value::UInt(_)
                        | Value::Float(_)
                        | Value::Double(_)
                        | Value::Decimal(_)
                        | Value::DecimalCompact(..)
                        | Value::Decimal128(..)
                        | Value::DecimalWide(..)
                        | Value::Null
                )
            )
        }),
        _ => false,
    };
    match expr {
        Expr::Literal(_) => true,
        Expr::Binary {
            left,
            op: BinaryOp::And | BinaryOp::Or,
            right,
        } => leakproof_for(table, left) && leakproof_for(table, right),
        Expr::Binary {
            left,
            op:
                BinaryOp::Eq
                | BinaryOp::NullSafeEq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq,
            right,
        } => {
            numeric_column(left) && (numeric_value(right) || numeric_column(right))
                || numeric_column(right) && numeric_value(left)
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            crate::engine::expr::expr_column_index(inner, table).is_some()
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            numeric_column(expr)
                && (numeric_value(low) || numeric_column(low))
                && (numeric_value(high) || numeric_column(high))
        }
        Expr::InList { expr, list, .. } => {
            numeric_column(expr)
                && list
                    .iter()
                    .all(|item| numeric_value(item) || numeric_column(item))
        }
        Expr::Unary {
            op: crate::parser::UnaryOp::Not,
            expr,
        } => leakproof_for(table, expr),
        _ => false,
    }
}

pub(crate) fn protect_table_filter(table: &TableSchema, policy: Expr, user: Option<Expr>) -> Expr {
    match user {
        Some(user) if leakproof_for(table, &user) => and(policy, user),
        user => protect_filter(policy, user),
    }
}

/// Planner calls this before access-path and aggregate selection.
pub fn scan_filter(
    db: &Database,
    table: &TableSchema,
    alias: Option<&str>,
    require_update: bool,
    user: Option<Expr>,
) -> Result<Option<Expr>, QueryError> {
    if !might_require_binding(db, table) {
        return Ok(user);
    }
    let Some(bound) = binding(db, table, Command::Select, false)? else {
        return Ok(user);
    };
    let mut guarded = bound.guarded;
    let mut policy = bound.expression.clone();
    if require_update {
        if let Some(write) = binding(db, table, Command::Update, false)? {
            guarded |= write.guarded;
            policy = and(policy, write.expression.clone());
        }
    }
    #[derive(Default)]
    struct Scope {
        names: Vec<String>,
        columns: std::collections::HashSet<String>,
        unknown_columns: bool,
    }
    struct Qualify<'a> {
        alias: &'a str,
        table: &'a TableSchema,
        db: &'a Database,
        scopes: Vec<Scope>,
    }
    impl crate::parser::visit::AstVisitorMut for Qualify<'_> {
        fn enter_select(&mut self, select: &mut crate::parser::Select) {
            let mut scope = Scope::default();
            for source in select
                .from
                .iter()
                .chain(select.joins.iter().map(|j| &j.table))
            {
                if let Some(alias) = &source.alias {
                    scope.names.push(alias.to_ascii_lowercase());
                } else {
                    scope.names.push(source.name.to_ascii_lowercase());
                    scope.names.push(
                        source
                            .name
                            .rsplit('.')
                            .next()
                            .unwrap_or(&source.name)
                            .to_ascii_lowercase(),
                    );
                }
                if let Ok(table) = self.db.table(&source.name) {
                    scope
                        .columns
                        .extend(table.columns.iter().map(|c| c.name.to_ascii_lowercase()));
                } else {
                    scope.unknown_columns = true;
                }
            }
            self.scopes.push(scope);
        }
        fn exit_select(&mut self, _: &mut crate::parser::Select) {
            self.scopes.pop();
        }
        fn enter_expr(&mut self, expr: &mut Expr) {
            match expr {
                Expr::Identifier(name)
                    if self.scopes.is_empty()
                        || self.table.column_index(name).is_ok()
                            && !self.scopes.iter().any(|s| {
                                s.unknown_columns || s.columns.contains(&name.to_ascii_lowercase())
                            }) =>
                {
                    *expr = Expr::CompoundIdentifier(vec![self.alias.to_string(), name.clone()]);
                }
                Expr::CompoundIdentifier(parts) if parts.len() == 2 || parts.len() == 3 => {
                    let qualifier = parts[..parts.len() - 1].join(".");
                    let original = qualifier.eq_ignore_ascii_case(&self.table.name)
                        || qualifier.eq_ignore_ascii_case(&self.table.qualified_name());
                    let shadowed = self
                        .scopes
                        .iter()
                        .any(|s| s.names.iter().any(|n| n.eq_ignore_ascii_case(&qualifier)));
                    if original && !shadowed {
                        *expr = Expr::CompoundIdentifier(vec![
                            self.alias.to_string(),
                            parts.last().unwrap().clone(),
                        ]);
                    }
                }
                _ => {}
            }
        }
    }
    crate::parser::visit::visit_expr_mut(
        &mut Qualify {
            alias: alias.unwrap_or(&table.name),
            table,
            db,
            scopes: Vec::new(),
        },
        &mut policy,
    );
    if guarded {
        policy = Expr::Function {
            name: "__MYDB_RLS_GUARD".into(),
            args: vec![Expr::Literal(Literal::UInt(table.id)), policy],
            distinct: false,
        };
    }
    Ok(Some(protect_table_filter(table, policy, user)))
}
