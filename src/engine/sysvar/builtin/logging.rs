//! Logging variables — delegate reads to `crate::engine::logging::variables_snapshot`
//! and writes to `crate::engine::logging::set_global_variable`. All are
//! GLOBAL-scoped per MySQL.

use crate::engine::sysvar::backend::SysVar;
use crate::engine::sysvar::ctx::WriteCtx;
use crate::engine::sysvar::meta::{DataType, Scope, SysVarDef};
use crate::engine::sysvar::registry::Registry;
use crate::engine::sysvar::value::SysVarValue;
use crate::types::QueryError;

pub(crate) fn register(reg: &mut Registry) {
    let names: &[(&'static str, &'static str)] = &[
        ("general_log", "Enable the general query log."),
        ("general_log_file", "Path to the general query log."),
        ("slow_query_log", "Enable the slow query log."),
        ("slow_query_log_file", "Path to the slow query log."),
        ("log_output", "Where to write logs (FILE/TABLE/NONE)."),
        ("long_query_time", "Slow log threshold (seconds)."),
        (
            "min_examined_row_limit",
            "Minimum examined rows for slow log inclusion.",
        ),
        (
            "log_slow_admin_statements",
            "Include admin statements in slow log.",
        ),
        (
            "log_queries_not_using_indexes",
            "Log non-index queries to slow log.",
        ),
        ("log_timestamps", "Log timestamp format (UTC/SYSTEM)."),
        ("log_error", "Path to the error log."),
        ("log_error_verbosity", "Error log verbosity 1..3."),
        ("log_error_services", "Active log services chain."),
    ];
    for &(name, doc) in names {
        let canon: &'static str = name;
        reg.register(
            SysVar::settable(
                SysVarDef::new(canon, Scope::Global, DataType::String { max_len: None })
                    .with_doc(doc),
                move |_ctx| {
                    crate::engine::logging::variables_snapshot()
                        .into_iter()
                        .find(|(n, _)| (*n).eq_ignore_ascii_case(canon))
                        .map(|(_, v)| SysVarValue::String(v))
                        .unwrap_or(SysVarValue::String(String::new()))
                },
                move |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                    let raw = v.to_display_string();
                    let _ = crate::engine::logging::set_global_variable(canon, &raw)?;
                    Ok(())
                },
            )
            .with_default_provider(move || {
                SysVarValue::String(crate::engine::logging::compiled_variable_default(canon))
            }),
        );
    }
}
