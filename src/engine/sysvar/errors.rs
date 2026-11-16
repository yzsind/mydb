//! MySQL-aligned error builders for the sysvar layer.
//!
//! Centralising these here keeps the dispatcher concise and ensures
//! every error code matches MySQL exactly — clients that key off the
//! error code (ORMs, replication tooling, JDBC retry policies) get the
//! right behaviour.

use crate::error::catalog::*;
use crate::types::QueryError;

pub fn unknown_system_variable(name: &str) -> QueryError {
    ER_UNKNOWN_SYSTEM_VARIABLE.error(format!("Unknown system variable '{name}'"))
}

/// MySQL 1228: tried `SET GLOBAL` on a SESSION-only variable.
pub fn session_only_variable(name: &str) -> QueryError {
    ER_LOCAL_VARIABLE.error(format!(
        "Variable '{name}' is a SESSION variable and can't be used with SET GLOBAL"
    ))
}

/// MySQL 1229: tried bare `SET` on a GLOBAL-only variable.
pub fn global_only_variable(name: &str) -> QueryError {
    ER_GLOBAL_VARIABLE.error(format!(
        "Variable '{name}' is a GLOBAL variable and should be set with SET GLOBAL"
    ))
}

/// MySQL 1231: value is the wrong type / not in the enum / out of range.
pub fn wrong_value_for_var(name: &str, value: &str) -> QueryError {
    ER_WRONG_VALUE_FOR_VAR.error(format!(
        "Variable '{name}' can't be set to the value of '{value}'"
    ))
}

/// MySQL 1232: incorrect type (e.g. string where integer was required).
pub fn wrong_type_for_var(name: &str) -> QueryError {
    ER_WRONG_TYPE_FOR_VAR.error(format!("Incorrect argument type to variable '{name}'"))
}

/// MySQL 1238: variable is read-only at runtime.
pub fn read_only_variable(name: &str) -> QueryError {
    ER_INCORRECT_GLOBAL_LOCAL_VAR.error(format!("Variable '{name}' is a read only variable"))
}

/// MySQL 1227: caller lacks the privilege needed to set this variable.
pub fn access_denied(name: &str) -> QueryError {
    ER_SPECIFIC_ACCESS_DENIED_ERROR.error(format!(
        "Access denied; you need SYSTEM_VARIABLES_ADMIN to set '{name}'"
    ))
}

/// MySQL 1231/42000 for a system-variable range violation.
pub fn out_of_range(name: &str, raw: &str) -> QueryError {
    wrong_value_for_var(name, raw)
}

/// PERSIST failed to write the on-disk file. Maps to MySQL's ER_PERSIST_*
/// family (3187 — could not save). The caller's message carries the
/// underlying io::Error context.
pub fn persist_write_failed(name: &str, detail: impl std::fmt::Display) -> QueryError {
    ER_CANT_SET_PERSISTED.error(format!("Could not persist variable '{name}': {detail}"))
}

/// PERSIST attempted without a Database — stub engine path. Returns a
/// shape MySQL would never produce (no real server lacks a data_dir), but
/// keeps the error code at 1064 so clients flag it as a syntax-like
/// rejection rather than an internal panic.
pub fn persist_requires_database() -> QueryError {
    ER_PARSE_ERROR
        .error("SET PERSIST requires a connected database; not available on the stub engine")
}
