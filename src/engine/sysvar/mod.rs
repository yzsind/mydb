//! System variable registry & dispatch — the single source of truth for
//! `SHOW VARIABLES`, `SELECT @@var`, `SET var = …`,
//! `information_schema.{global,session}_variables`, and
//! `performance_schema.{global,session,variables_by_thread}`.
//!
//! Design
//! ------
//!
//! * `SysVarDef` (in `meta.rs`) carries the declarative metadata: scope
//!   (Both/Session/Global), access (ReadWrite/ReadOnly/SuperReadWrite),
//!   data type (Bool/Int/UInt/Double/Enum/SetType/String), aliases, doc.
//! * `DataType::coerce` does MySQL-style permissive value conversion
//!   (`'ON'` → `true`, integer enum index → canonical string, etc.).
//! * `SysVar` (in `backend.rs`) pairs the def with read / write
//!   closures (`Arc<dyn Fn>`), so individual variables can hook any
//!   backend — `SessionContext` field, atomic global, storage engine
//!   knob, derived `current_database` lookup, etc.
//! * `Registry` (in `registry.rs`) is a `OnceLock`-initialised singleton
//!   built from `builtin::register_all`. Lookup is case-insensitive with
//!   an alias map (e.g. `tx_isolation` → `transaction_isolation`).
//! * MySQL-aligned error codes live in `errors.rs`.
//!
//! Call sites in the rest of the engine touch only the public re-exports
//! at the bottom of this file.

pub mod backend;
pub mod builtin;
pub mod ctx;
pub mod errors;
pub mod meta;
pub mod persist;
pub mod registry;
pub mod value;

#[cfg(test)]
mod tests;

pub use backend::SysVar;
pub use ctx::{ReadCtx, WriteCtx};
pub use errors::{
    access_denied, global_only_variable, out_of_range, read_only_variable, session_only_variable,
    unknown_system_variable, wrong_type_for_var, wrong_value_for_var,
};
pub use meta::{Access, DataType, Scope, SetScope, SysVarDef};
pub use persist::{PersistedEntry, PersistedVariables};
pub use registry::{apply_persisted_on_startup, registry, Registry, VariableRow};
#[cfg(test)]
pub use value::parse_bool;
pub use value::{format_double, parse_int_lenient, parse_uint_lenient, SysVarValue};

use crate::engine::api::SessionContext;
use crate::storage::Database;
use crate::types::QueryError;
use std::sync::atomic::{AtomicU64, Ordering};

// Sessions cache the expensive performance_schema system-variable snapshot.
// A GLOBAL write invalidates every session lazily: each connection compares
// its last-published epoch at the next command boundary.  SESSION writes use a
// per-context dirty bit instead, avoiding one full registry walk after every
// ordinary query.
static PFS_GLOBAL_VARIABLE_EPOCH: AtomicU64 = AtomicU64::new(1);

pub(crate) fn pfs_global_variable_epoch() -> u64 {
    PFS_GLOBAL_VARIABLE_EPOCH.load(Ordering::Acquire)
}

pub(crate) fn bump_pfs_global_variable_epoch() {
    PFS_GLOBAL_VARIABLE_EPOCH.fetch_add(1, Ordering::AcqRel);
}

/// Convenience: read a variable by name into a SQL-ready `Value`.
/// Used by `SELECT @@x`, `SHOW COUNT(*) WARNINGS`, etc.
pub fn read_var(
    name: &str,
    scope: Scope,
    session: Option<&SessionContext>,
    db: Option<&Database>,
) -> Result<crate::types::Value, QueryError> {
    let v = registry().read(name, scope, session, db)?;
    Ok(v.to_select_value())
}

/// Convenience: apply one `SET` assignment.
pub fn write_var(
    name: &str,
    set_scope: SetScope,
    session: &mut SessionContext,
    db: Option<&Database>,
    value: SysVarValue,
) -> Result<(), QueryError> {
    registry().write(name, set_scope, session, db, value)
}

/// Snapshot used by `SHOW VARIABLES` and the schema views.
pub fn snapshot(
    scope: Scope,
    session: Option<&SessionContext>,
    db: Option<&Database>,
) -> Vec<VariableRow> {
    registry().snapshot_for_show(scope, session, db)
}
