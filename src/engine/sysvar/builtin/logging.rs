//! Logging variables backed by the active logging service and session thresholds.

use crate::engine::logging;
use crate::engine::sysvar::backend::SysVar;
use crate::engine::sysvar::ctx::WriteCtx;
use crate::engine::sysvar::meta::{Access, DataType, Scope, SelectType, SysVarDef};
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
        let mut def =
            SysVarDef::new(name, Scope::Global, DataType::String { max_len: None }).with_doc(doc);
        def = match name {
            "general_log"
            | "slow_query_log"
            | "log_slow_admin_statements"
            | "log_queries_not_using_indexes" => def.with_select_type(SelectType::Bool),
            "log_error_verbosity" => def.with_select_type(SelectType::UInt),
            _ => def,
        };
        reg.register(
            SysVar::settable(
                def,
                move |_ctx| crate::engine::logging::variable_value(name),
                move |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                    let raw = v.to_config_string();
                    let _ = crate::engine::logging::set_global_variable(name, &raw)?;
                    Ok(())
                },
            )
            .with_default_provider(move || crate::engine::logging::compiled_variable_default(name)),
        );
    }
    reg.register(
        SysVar::settable(
            SysVarDef::new(
                "long_query_time",
                Scope::Both,
                DataType::Double {
                    min: 0.0,
                    max: f64::MAX,
                },
            )
            .with_doc("Slow log threshold in seconds."),
            |ctx| match (ctx.scope, ctx.session) {
                (Scope::Session, Some(session)) => {
                    SysVarValue::Double(session.sysvars.slow_log.long_query_time_seconds)
                }
                _ => logging::variable_value("long_query_time"),
            },
            |ctx, value| {
                if ctx.is_global() {
                    logging::set_global_variable("long_query_time", &value.to_config_string())?;
                } else {
                    ctx.session
                        .sysvars
                        .slow_log
                        .set_long_query_time(value.as_f64().unwrap());
                }
                Ok(())
            },
        )
        .with_default_provider(|| logging::compiled_variable_default("long_query_time")),
    );
    reg.register(
        SysVar::settable(
            SysVarDef::new(
                "min_examined_row_limit",
                Scope::Both,
                DataType::UInt {
                    min: 0,
                    max: u64::MAX,
                },
            )
            .with_scope_access(Access::SessionAdmin, Access::ReadWrite)
            .with_doc("Minimum examined rows for slow log inclusion."),
            |ctx| match (ctx.scope, ctx.session) {
                (Scope::Session, Some(session)) => {
                    SysVarValue::UInt(session.sysvars.slow_log.min_examined_row_limit)
                }
                _ => logging::variable_value("min_examined_row_limit"),
            },
            |ctx, value| {
                if ctx.is_global() {
                    logging::set_global_variable(
                        "min_examined_row_limit",
                        &value.to_config_string(),
                    )?;
                } else {
                    ctx.session.sysvars.slow_log.min_examined_row_limit = value.as_u64().unwrap();
                }
                Ok(())
            },
        )
        .with_default_provider(|| logging::compiled_variable_default("min_examined_row_limit")),
    );
}
