//! The central system variable registry.
//!
//! Initialised once via `OnceLock` at first access. Per-process, lock-free
//! reads after init. All lookups are case-insensitive (MySQL standard).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

use super::backend::SysVar;
use super::ctx::{ReadCtx, WriteCtx};
use super::errors::{access_denied, unknown_system_variable};
use super::meta::{Access, Scope, SetScope};
use super::persist::PersistedVariables;
use super::value::SysVarValue;
use crate::engine::api::SessionContext;
use crate::engine::auth::{Privilege, PrivilegeScope, RequiredPrivilege};
use crate::storage::Database;
use crate::types::QueryError;

/// Snapshot row used by `SHOW VARIABLES`, `information_schema.global_variables`,
/// `performance_schema.global_variables`. Returned by
/// `Registry::snapshot_for_show`.
#[derive(Debug, Clone)]
pub struct VariableRow {
    pub name: &'static str,
    pub value: String,
}

#[derive(Clone)]
pub(crate) struct VariableProvenance {
    pub source: &'static str,
    pub path: Option<String>,
    pub set_time: Option<String>,
    pub user: Option<String>,
    pub host: Option<String>,
}

pub struct Registry {
    provenance: parking_lot::RwLock<HashMap<&'static str, VariableProvenance>>,
    /// Lowercase canonical name → entry.
    by_name: HashMap<&'static str, Arc<SysVar>>,
    /// Alias name → canonical name. Aliases share the same entry.
    aliases: HashMap<&'static str, &'static str>,
    /// Stable iteration order for `SHOW VARIABLES`. Sorted by name to
    /// match MySQL's output.
    sorted_names: Vec<&'static str>,
    session_initializers: Vec<super::backend::WriteFn>,
}

impl Registry {
    fn new() -> Self {
        let mut reg = Registry {
            provenance: Default::default(),
            by_name: HashMap::new(),
            aliases: HashMap::new(),
            sorted_names: Vec::new(),
            session_initializers: Vec::new(),
        };
        super::builtin::register_all(&mut reg);
        reg.finalize();
        reg
    }

    pub(crate) fn register(&mut self, var: SysVar) {
        let name = var.def.name;
        debug_assert!(
            !self.by_name.contains_key(name),
            "duplicate sysvar registration: {name}"
        );
        for &alias in var.def.aliases {
            debug_assert!(
                !self.aliases.contains_key(alias),
                "duplicate sysvar alias: {alias}"
            );
            self.aliases.insert(alias, name);
        }
        self.by_name.insert(name, Arc::new(var));
    }

    fn finalize(&mut self) {
        self.sorted_names = self.by_name.keys().copied().collect();
        self.sorted_names.sort_unstable();
        self.session_initializers = self
            .sorted_names
            .iter()
            .filter_map(|name| self.by_name[name].inherit.clone())
            .collect();
    }

    pub(crate) fn provenance(&self, name: &str) -> Option<VariableProvenance> {
        self.provenance.read().get(name).cloned()
    }

    fn startup_provenance(&self, name: &str, source: &'static str, path: Option<String>) {
        if let Some(var) = self.get(name) {
            self.provenance.write().insert(
                var.def.name,
                VariableProvenance {
                    source,
                    path,
                    set_time: None,
                    user: None,
                    host: None,
                },
            );
        }
    }

    /// Look up a variable by its canonical or alias name (case-insensitive).
    /// Allocates only when the name needs case folding.
    pub fn get(&self, name: &str) -> Option<Arc<SysVar>> {
        self.get_entry(name).map(Arc::clone)
    }

    /// Borrow a registry entry without touching its `Arc` reference count.
    /// The registry is process-global and immutable after `OnceLock`
    /// initialization, so SELECT-side readers never need ownership. Avoiding
    /// the shared atomic increment/decrement is material under concurrent
    /// `SELECT @@var` workloads where every connection probes the same entry.
    fn get_entry(&self, name: &str) -> Option<&Arc<SysVar>> {
        // Fast path — common case is exact lowercase match.
        if let Some(var) = self.by_name.get(name) {
            return Some(var);
        }
        if let Some(canon) = self.aliases.get(name) {
            if let Some(var) = self.by_name.get(canon) {
                return Some(var);
            }
        }
        // Slow path — fold case.
        let lower = name.to_ascii_lowercase();
        if let Some(var) = self.by_name.get(lower.as_str()) {
            return Some(var);
        }
        if let Some(canon) = self.aliases.get(lower.as_str()) {
            if let Some(var) = self.by_name.get(canon) {
                return Some(var);
            }
        }
        None
    }

    /// Is the name a known canonical or alias? Cheap predicate that avoids
    /// the Arc clone of `get`.
    pub fn contains(&self, name: &str) -> bool {
        if self.by_name.contains_key(name) || self.aliases.contains_key(name) {
            return true;
        }
        let lower = name.to_ascii_lowercase();
        self.by_name.contains_key(lower.as_str()) || self.aliases.contains_key(lower.as_str())
    }

    /// Iterator over all canonical variables in name-sorted order.
    pub fn iter(&self) -> impl Iterator<Item = Arc<SysVar>> + '_ {
        self.sorted_names
            .iter()
            .filter_map(move |name| self.by_name.get(name).cloned())
    }

    /// Snapshot every variable's current value for `SHOW VARIABLES` /
    /// `information_schema.global_variables`.
    pub fn snapshot_for_show(
        &self,
        scope: Scope,
        session: Option<&SessionContext>,
        db: Option<&Database>,
    ) -> Vec<VariableRow> {
        let ctx = ReadCtx::new(session, db, scope);
        self.iter()
            .filter(|var| {
                // Hide Session-only vars in a GLOBAL view, hide Global-only
                // vars in a SESSION view? MySQL actually returns ALL vars
                // in both views — Global-only vars return their global value
                // even in `SHOW SESSION VARIABLES`. Match that.
                let _ = var; // intentionally no filter
                true
            })
            .map(|var| {
                let effective_scope = match (scope, var.scope()) {
                    // Session-scope view of a Global-only var: still return
                    // the global value (MySQL behaviour).
                    (Scope::Session, Scope::Global) => Scope::Global,
                    // Global-scope view of a Session-only var: MySQL also
                    // returns the (global default) value, which for us is
                    // whatever the backend gives without a session context.
                    (Scope::Global, Scope::Session) => Scope::Global,
                    _ => scope,
                };
                let ctx = ReadCtx::new(ctx.session, ctx.db, effective_scope);
                VariableRow {
                    name: var.def.name,
                    value: (var.read)(&ctx).to_display_string(),
                }
            })
            .collect()
    }

    /// Read a single variable as a SQL value (for `SELECT @@x`).
    pub fn read(
        &self,
        name: &str,
        scope: Scope,
        session: Option<&SessionContext>,
        db: Option<&Database>,
    ) -> Result<SysVarValue, QueryError> {
        self.read_with_explicit_session(name, scope, false, session, db)
    }

    /// SQL distinguishes implicit @@x lookup from explicit @@session.x / @@local.x.
    pub fn read_sql(
        &self,
        name: &str,
        scope: crate::parser::SystemVariableScope,
        session: Option<&SessionContext>,
        db: Option<&Database>,
    ) -> Result<SysVarValue, QueryError> {
        use crate::parser::SystemVariableScope;
        self.read_with_explicit_session(
            name,
            if scope == SystemVariableScope::Global {
                Scope::Global
            } else {
                Scope::Session
            },
            matches!(
                scope,
                SystemVariableScope::Session | SystemVariableScope::Local
            ),
            session,
            db,
        )
    }

    fn read_with_explicit_session(
        &self,
        name: &str,
        scope: Scope,
        explicit_session: bool,
        session: Option<&SessionContext>,
        db: Option<&Database>,
    ) -> Result<SysVarValue, QueryError> {
        let var = self
            .get_entry(name)
            .ok_or_else(|| unknown_system_variable(name))?;
        if scope == Scope::Global && var.scope() == Scope::Session {
            return Err(crate::error::catalog::ER_INCORRECT_GLOBAL_LOCAL_VAR
                .error(format!("Variable '{}' is a SESSION variable", var.def.name)));
        }
        if explicit_session && var.scope() == Scope::Global {
            return Err(crate::error::catalog::ER_INCORRECT_GLOBAL_LOCAL_VAR
                .error(format!("Variable '{}' is a GLOBAL variable", var.def.name)));
        }
        let effective_scope = match (scope, var.scope()) {
            (Scope::Session, Scope::Global) => Scope::Global,
            (Scope::Global, Scope::Session) => Scope::Global,
            _ => scope,
        };
        let ctx = ReadCtx::new(session, db, effective_scope);
        Ok((var.read)(&ctx))
    }

    /// Shared scope/access gate. Both preflight and application recheck it;
    /// backend and persistence checks retain their entry-specific ordering.
    fn writable_scope(
        var: &SysVar,
        set_scope: SetScope,
        session: &SessionContext,
        db: Option<&Database>,
    ) -> Result<Scope, QueryError> {
        // MySQL diagnoses a statically read-only surface before diagnosing
        // that bare SET selected the wrong scope (for example `SET version`:
        // 1238, not 1229). Scope-specific access overrides still receive the
        // effective-scope check below.
        if !set_scope.is_persist_only()
            && matches!(
                var.def.access_for(set_scope.requested_scope()),
                Access::ReadOnly
            )
        {
            return Err(super::errors::read_only_variable(var.def.name));
        }
        let effective_scope = set_scope.resolve_for(var.def.name, var.def.scope)?;
        if set_scope.is_persist_only()
            && (var.write.is_none()
                || matches!(var.def.access_for(effective_scope), Access::ReadOnly))
        {
            return Err(super::errors::read_only_variable(var.def.name));
        }
        if set_scope.is_persist() && !var.def.persistable {
            return Err(super::errors::read_only_variable(var.def.name));
        }
        // Persisting startup-only variables requires a startup application
        // backend. Reject unsupported entries above rather than silently
        // storing settings that cannot take effect on restart.
        if !matches!(set_scope, SetScope::PersistOnly) {
            var.def.check_writable(effective_scope)?;
        }
        // SuperReadWrite: the authoritative 1227 gating happens in the
        // dispatcher's `auth_required_privileges` (which produces a proper
        // MySQL error naming the privilege). This registry-level check is
        // defense-in-depth for direct-write paths — it consults the *actual*
        // SYSTEM_VARIABLES_ADMIN privilege bit (role closure + SUPER
        // implication), not a hard-coded username, so a granted non-root
        // admin passes here just as it does in the dispatcher.
        if matches!(var.def.access_for(effective_scope), Access::SuperReadWrite)
            && !account_has_sysvar_admin(session, db)
        {
            return Err(access_denied(var.def.name));
        }
        Ok(effective_scope)
    }

    /// Apply a `SET` assignment. Validates scope, access, and value type,
    /// then dispatches into the backend. For PERSIST / PERSIST_ONLY also
    /// records the assignment in `mysqld-auto.cnf` (via the Database that
    /// owns the file).
    pub fn write(
        &self,
        name: &str,
        set_scope: SetScope,
        session: &mut SessionContext,
        db: Option<&Database>,
        value: SysVarValue,
    ) -> Result<(), QueryError> {
        let var = self
            .get(name)
            .ok_or_else(|| unknown_system_variable(name))?;
        let effective_scope = Self::writable_scope(&var, set_scope, session, db)?;
        if set_scope.is_persist() {
            db.ok_or_else(super::errors::persist_requires_database)?
                .persisted_variables()
                .check_writable()
                .map_err(|e| super::errors::persist_write_failed(var.def.name, e))?;
        }
        let coerced = var.def.data_type.coerce(var.def.name, value)?;
        if let Some(validate) = &var.validate {
            validate(&ReadCtx::new(Some(session), db, effective_scope), &coerced)?;
        }
        let account = set_scope
            .is_persist()
            .then(|| session.authenticated_as.clone());
        let mut apply = || -> Result<(), QueryError> {
            if !set_scope.is_persist_only() {
                let write = var
                    .write
                    .as_ref()
                    .ok_or_else(|| super::errors::read_only_variable(var.def.name))?;
                write(
                    &mut WriteCtx::new(session, db, effective_scope),
                    coerced.clone(),
                )?;
                match effective_scope {
                    Scope::Session => session.mark_pfs_variable_dirty(var.def.name),
                    Scope::Global => {
                        self.provenance.write().insert(
                            var.def.name,
                            VariableProvenance {
                                source: "DYNAMIC",
                                path: None,
                                set_time: Some(
                                    chrono::Local::now()
                                        .format("%Y-%m-%d %H:%M:%S%.6f")
                                        .to_string(),
                                ),
                                user: Some(session.authenticated_as.user.clone()),
                                host: Some(session.authenticated_as.host.clone()),
                            },
                        );
                        super::bump_pfs_global_variable_epoch();
                    }
                    Scope::Both => unreachable!("resolved SET scope"),
                }
            }
            Ok(())
        };
        if let Some(account) = account {
            let db = db.ok_or_else(super::errors::persist_requires_database)?;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            db.persisted_variables().upsert_and_apply(
                var.def.name,
                &coerced,
                &account.user,
                &account.host,
                now_ms,
                apply,
            )?;
        } else {
            apply()?;
        }
        Ok(())
    }

    /// Validate an SQL value before the sysvar representation loses its
    /// exact numeric type (DECIMAL is otherwise represented as text).
    pub fn validate_sql_value(
        &self,
        name: &str,
        set_scope: SetScope,
        session: &SessionContext,
        db: Option<&Database>,
        value: &crate::types::Value,
    ) -> Result<SysVarValue, QueryError> {
        let var = self
            .get_entry(name)
            .ok_or_else(|| unknown_system_variable(name))?;
        if matches!(
            var.def.data_type,
            super::meta::DataType::StrictString { .. }
        ) && !matches!(
            value,
            crate::types::Value::String(_)
                | crate::types::Value::SmallString(_)
                | crate::types::Value::Bytes(_)
                | crate::types::Value::Date(_)
                | crate::types::Value::Time(_)
                | crate::types::Value::DateTime(_)
                | crate::types::Value::Timestamp(_)
                | crate::types::Value::Json(_)
                | crate::types::Value::Null
        ) {
            Self::writable_scope(var, set_scope, session, db)?;
            return Err(super::errors::wrong_type_for_var(var.def.name));
        }
        Self::validate_entry_value(
            var,
            set_scope,
            session,
            db,
            SysVarValue::from_sql_value(value),
        )
    }

    /// Validate a candidate value without touching any backend or session
    /// state. Used by `execute_set_statement`'s two-phase atomic
    /// multi-assignment: every assignment is validated first, and only if
    /// all pass do we apply them. Returns the coerced `SysVarValue` so the
    /// second pass doesn't repeat parsing.
    pub fn validate(
        &self,
        name: &str,
        set_scope: SetScope,
        session: &SessionContext,
        db: Option<&Database>,
        value: SysVarValue,
    ) -> Result<SysVarValue, QueryError> {
        let var = self
            .get_entry(name)
            .ok_or_else(|| unknown_system_variable(name))?;
        Self::validate_entry_value(var, set_scope, session, db, value)
    }

    fn validate_entry_value(
        var: &SysVar,
        set_scope: SetScope,
        session: &SessionContext,
        db: Option<&Database>,
        value: SysVarValue,
    ) -> Result<SysVarValue, QueryError> {
        let effective_scope = Self::writable_scope(var, set_scope, session, db)?;
        if !matches!(set_scope, SetScope::PersistOnly) && var.write.is_none() {
            return Err(super::errors::read_only_variable(var.def.name));
        }
        let coerced = var.def.data_type.coerce(var.def.name, value)?;
        if let Some(validate) = &var.validate {
            validate(&ReadCtx::new(Some(session), db, effective_scope), &coerced)?;
        }
        if set_scope.is_persist() {
            db.ok_or_else(super::errors::persist_requires_database)?
                .persisted_variables()
                .check_writable()
                .map_err(|e| super::errors::persist_write_failed(var.def.name, e))?;
        }
        Ok(coerced)
    }

    /// Immutable registration default, never the live global value.
    pub fn compiled_default(&self, name: &str) -> Option<SysVarValue> {
        let var = self.get_entry(name)?;
        Some(match &var.default_provider {
            Some(provider) => provider(),
            None => var.compiled_default.clone(),
        })
    }

    pub(crate) fn inherit_session_defaults(&self, session: &mut SessionContext) {
        let mut ctx = WriteCtx::new(session, None, Scope::Session);
        for inherit in &self.session_initializers {
            inherit(&mut ctx, SysVarValue::Null)
                .expect("validated global default must initialize a session");
        }
    }

    /// Distinct variable count (excludes aliases). Mostly for tests.
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }
}

/// Re-apply the contents of `mysqld-auto.cnf` to the live registry as
/// GLOBAL values. Called once during server startup, after the auth
/// catalog is loaded so SuperReadWrite gating still works (the system
/// session bypasses it anyway).
pub fn apply_persisted_on_startup(db: &Database) {
    let store: &std::sync::Arc<PersistedVariables> = db.persisted_variables();
    let entries = store.snapshot();
    if entries.is_empty() {
        return;
    }
    let mut sys = SessionContext::system_default();
    for entry in entries {
        let raw = SysVarValue::String(entry.value.clone());
        if let Err(err) = registry().write(&entry.name, SetScope::Global, &mut sys, Some(db), raw) {
            eprintln!(
                "mydb: failed to apply persisted variable '{name}' = {val:?}: {err}",
                name = entry.name,
                val = entry.value,
                err = err.message(),
            );
        } else {
            registry().startup_provenance(
                &entry.name,
                "PERSISTED",
                Some(store.file_path().display().to_string()),
            );
        }
    }
}

/// Apply validated option-file system variables before `mysqld-auto.cnf`.
/// The caller invokes this after the Database is open (some backends live on
/// Database) but before background workers or client sessions are created.
pub fn apply_option_file_on_startup(
    db: &Database,
    assignments: &[crate::startup_config::StartupSysVarAssignment],
) -> Result<(), QueryError> {
    let mut sys = SessionContext::system_default();
    for assignment in assignments {
        registry().write(
            &assignment.name,
            SetScope::Global,
            &mut sys,
            Some(db),
            assignment.value.clone(),
        )?;
        registry().startup_provenance(&assignment.name, "GLOBAL", None);
    }
    Ok(())
}

/// Real `SYSTEM_VARIABLES_ADMIN` gate. System/internal sessions bypass — they
/// run maintenance work like apply-on-startup and recovery where no user is
/// authenticated. Otherwise consult the auth catalog for the *actual*
/// privilege bit (honouring role closure and SUPER implication) rather than
/// matching a hard-coded username, so `GRANT SYSTEM_VARIABLES_ADMIN TO u`
/// takes effect and a renamed-away root loses its implicit power. When no
/// Database handle is available (rare internal call sites) grants can't be
/// consulted, so only system sessions pass.
fn account_has_sysvar_admin(session: &SessionContext, db: Option<&Database>) -> bool {
    if session.internal_system_session {
        return true;
    }
    let Some(db) = db else {
        return false;
    };
    let ctx = session.authorization_context();
    let required = RequiredPrivilege {
        privilege: Privilege::Dynamic("SYSTEM_VARIABLES_ADMIN".to_string()),
        scope: PrivilegeScope::Global,
    };
    db.with_auth_catalog(|auth| auth.account_has_privilege(&ctx, &required, false))
        .unwrap_or(false)
}

static REGISTRY: OnceLock<Registry> = OnceLock::new();

/// Process-global registry. `OnceLock` has no reset, so tests must rely
/// on side-effect-free reads rather than re-initializing the registry.
pub fn registry() -> &'static Registry {
    REGISTRY.get_or_init(Registry::new)
}
