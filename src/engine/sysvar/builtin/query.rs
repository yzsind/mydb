//! Query-execution variables — sql_mode, sql_safe_updates, sql_select_limit,
//! max_execution_time, group_concat_max_len, optimizer_switch, explain_format,
//! event_scheduler.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{OnceLock, RwLock};

use crate::engine::api::{
    explain_format_name, global_explain_format, parse_explain_format_name,
    set_global_explain_format,
};
use crate::engine::sysvar::backend::SysVar;
use crate::engine::sysvar::ctx::WriteCtx;
use crate::engine::sysvar::errors::wrong_value_for_var;
use crate::engine::sysvar::meta::{Access, DataType, Scope, SysVarDef};
use crate::engine::sysvar::registry::Registry;
use crate::engine::sysvar::value::SysVarValue;
use crate::types::QueryError;

static GLOBAL_SQL_SELECT_LIMIT: AtomicU64 = AtomicU64::new(u64::MAX);
static GLOBAL_SQL_MODE: OnceLock<RwLock<String>> = OnceLock::new();
static GLOBAL_SELECT_INTO_BUFFER_SIZE: AtomicU64 = AtomicU64::new(131_072);
static GLOBAL_SELECT_INTO_DISK_SYNC: AtomicBool = AtomicBool::new(false);
static GLOBAL_SELECT_INTO_DISK_SYNC_DELAY: AtomicU64 = AtomicU64::new(0);
static GLOBAL_SECURE_FILE_PRIV: OnceLock<RwLock<String>> = OnceLock::new();

fn global_sql_mode_cell() -> &'static RwLock<String> {
    GLOBAL_SQL_MODE.get_or_init(|| {
        RwLock::new("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO".to_string())
    })
}

pub(crate) fn global_sql_mode() -> String {
    global_sql_mode_cell().read().unwrap().clone()
}

pub(crate) fn global_sql_select_limit() -> u64 {
    GLOBAL_SQL_SELECT_LIMIT.load(Ordering::Relaxed)
}

pub(crate) fn global_select_into_buffer_size() -> u64 {
    GLOBAL_SELECT_INTO_BUFFER_SIZE.load(Ordering::Relaxed)
}

pub(crate) fn global_select_into_disk_sync() -> bool {
    GLOBAL_SELECT_INTO_DISK_SYNC.load(Ordering::Relaxed)
}

pub(crate) fn global_select_into_disk_sync_delay() -> u64 {
    GLOBAL_SELECT_INTO_DISK_SYNC_DELAY.load(Ordering::Relaxed)
}

pub(crate) fn global_secure_file_priv() -> String {
    GLOBAL_SECURE_FILE_PRIV
        .get_or_init(|| RwLock::new("NULL".to_string()))
        .read()
        .unwrap()
        .clone()
}

#[cfg(test)]
pub(crate) fn allow_test_server_files() {
    // Import/export fixtures explicitly opt in; production keeps the safe default.
    *GLOBAL_SECURE_FILE_PRIV
        .get_or_init(|| RwLock::new("NULL".to_string()))
        .write()
        .unwrap() = String::new();
}

#[cfg(test)]
pub(crate) fn reset_query_globals_for_mtr() {
    *global_sql_mode_cell().write().unwrap() = String::new();
    GLOBAL_SQL_SELECT_LIMIT.store(u64::MAX, Ordering::Relaxed);
    GLOBAL_SELECT_INTO_BUFFER_SIZE.store(131_072, Ordering::Relaxed);
    GLOBAL_SELECT_INTO_DISK_SYNC.store(false, Ordering::Relaxed);
    GLOBAL_SELECT_INTO_DISK_SYNC_DELAY.store(0, Ordering::Relaxed);
    *GLOBAL_SECURE_FILE_PRIV
        .get_or_init(|| RwLock::new("NULL".to_string()))
        .write()
        .unwrap() = String::new();
}

pub(crate) fn register(reg: &mut Registry) {
    // INFORMATION_SCHEMA reads storage-engine metadata on every query, without
    // a separate expiry cache. Its age is therefore already within this limit,
    // including zero; keep that cheap direct-read path for every setting.
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "information_schema_stats_expiry",
            Scope::Both,
            DataType::UInt {
                min: 0,
                max: 31_536_000,
            },
        )
        .with_doc("Maximum statistics cache age in seconds; mydb reads storage metadata directly."),
        |ctx| {
            SysVarValue::UInt(
                ctx.session
                    .map_or(86_400, |s| s.sysvars.information_schema_stats_expiry),
            )
        },
        |ctx, value| {
            ctx.session.sysvars.information_schema_stats_expiry = value.as_u64().unwrap();
            Ok(())
        },
    ));

    // MySQL compatibility RNG seeds. These are deliberately SESSION-only:
    // they expose and replace the state consumed by unseeded RAND().
    for (name, component) in [("rand_seed1", 0usize), ("rand_seed2", 1usize)] {
        reg.register(SysVar::settable(
            SysVarDef::new(
                name,
                Scope::Session,
                DataType::UInt {
                    min: 0,
                    max: 0x3FFF_FFFF,
                },
            )
            .with_doc("Connection-local seed component used by RAND()."),
            move |ctx| {
                let state = ctx.session.map(|session| session.sysvars.rand_state.lock());
                SysVarValue::UInt(
                    state
                        .as_ref()
                        .map(|state| if component == 0 { state.0 } else { state.1 })
                        .unwrap_or(0),
                )
            },
            move |ctx: &mut WriteCtx<'_>, value: SysVarValue| -> Result<(), QueryError> {
                let value = value.as_u64().unwrap_or(0);
                let mut state = ctx.session.sysvars.rand_state.lock();
                if component == 0 {
                    state.0 = value;
                } else {
                    state.1 = value;
                }
                Ok(())
            },
        ));
    }

    reg.register(SysVar::settable(
        SysVarDef::new(
            "select_into_buffer_size",
            Scope::Both,
            DataType::UInt {
                min: 8_192,
                max: 2_147_479_552,
            },
        )
        .with_access(Access::SuperReadWrite)
        .with_doc("Buffer size for SELECT INTO OUTFILE/DUMPFILE, in 4096-byte blocks."),
        |ctx| match ctx.scope {
            Scope::Global => SysVarValue::UInt(global_select_into_buffer_size()),
            Scope::Session => SysVarValue::UInt(
                ctx.session
                    .map(|s| s.sysvars.select_into_buffer_size)
                    .unwrap_or_else(global_select_into_buffer_size),
            ),
            Scope::Both => unreachable!(),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let value = v.as_u64().unwrap_or(131_072).clamp(8_192, 2_147_479_552) / 4096 * 4096;
            match ctx.scope {
                Scope::Global => GLOBAL_SELECT_INTO_BUFFER_SIZE.store(value, Ordering::Relaxed),
                Scope::Session => ctx.session.sysvars.select_into_buffer_size = value,
                Scope::Both => unreachable!(),
            }
            Ok(())
        },
    ));
    reg.register(SysVar::settable(
        SysVarDef::new("select_into_disk_sync", Scope::Both, DataType::Bool)
            .with_access(Access::SuperReadWrite)
            .with_doc("Synchronize SELECT INTO file buffers to disk while exporting."),
        |ctx| match ctx.scope {
            Scope::Global => SysVarValue::Bool(global_select_into_disk_sync()),
            Scope::Session => SysVarValue::Bool(
                ctx.session
                    .map(|s| s.sysvars.select_into_disk_sync)
                    .unwrap_or_else(global_select_into_disk_sync),
            ),
            Scope::Both => unreachable!(),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let value = v.as_bool().unwrap_or(false);
            match ctx.scope {
                Scope::Global => GLOBAL_SELECT_INTO_DISK_SYNC.store(value, Ordering::Relaxed),
                Scope::Session => ctx.session.sysvars.select_into_disk_sync = value,
                Scope::Both => unreachable!(),
            }
            Ok(())
        },
    ));
    reg.register(SysVar::settable(
        SysVarDef::new(
            "select_into_disk_sync_delay",
            Scope::Both,
            DataType::UInt {
                min: 0,
                max: 31_536_000,
            },
        )
        .with_access(Access::SuperReadWrite)
        .with_doc("Delay in milliseconds after each SELECT INTO disk sync."),
        |ctx| match ctx.scope {
            Scope::Global => SysVarValue::UInt(global_select_into_disk_sync_delay()),
            Scope::Session => SysVarValue::UInt(
                ctx.session
                    .map(|s| s.sysvars.select_into_disk_sync_delay_ms)
                    .unwrap_or_else(global_select_into_disk_sync_delay),
            ),
            Scope::Both => unreachable!(),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let value = v.as_u64().unwrap_or(0).min(31_536_000);
            match ctx.scope {
                Scope::Global => GLOBAL_SELECT_INTO_DISK_SYNC_DELAY.store(value, Ordering::Relaxed),
                Scope::Session => ctx.session.sysvars.select_into_disk_sync_delay_ms = value,
                Scope::Both => unreachable!(),
            }
            Ok(())
        },
    ));
    reg.register(SysVar::settable(
        SysVarDef::new(
            "secure_file_priv",
            Scope::Global,
            DataType::String { max_len: None },
        )
        .with_access(Access::SuperReadWrite)
        .with_doc("Directory restriction for server-side import and export; default NULL disables server file access."),
        |_ctx| SysVarValue::String(global_secure_file_priv()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            *GLOBAL_SECURE_FILE_PRIV
                .get_or_init(|| RwLock::new("NULL".to_string()))
                .write()
                .unwrap() = v.to_display_string();
            Ok(())
        },
    ));

    // sql_mode — GLOBAL supplies the default for subsequently created
    // sessions; existing sessions retain their own value.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "sql_mode",
            Scope::Both,
            DataType::SetType {
                values: &[
                    "ALLOW_INVALID_DATES",
                    "ANSI",
                    "ANSI_QUOTES",
                    "ERROR_FOR_DIVISION_BY_ZERO",
                    "HIGH_NOT_PRECEDENCE",
                    "IGNORE_SPACE",
                    "NO_AUTO_VALUE_ON_ZERO",
                    "NO_BACKSLASH_ESCAPES",
                    "NO_DIR_IN_CREATE",
                    "NO_ENGINE_SUBSTITUTION",
                    "NO_UNSIGNED_SUBTRACTION",
                    "NO_ZERO_DATE",
                    "NO_ZERO_IN_DATE",
                    "ONLY_FULL_GROUP_BY",
                    "PAD_CHAR_TO_FULL_LENGTH",
                    "PIPES_AS_CONCAT",
                    "REAL_AS_FLOAT",
                    "STRICT_ALL_TABLES",
                    "STRICT_TRANS_TABLES",
                    "TIME_TRUNCATE_FRACTIONAL",
                    "TRADITIONAL",
                ],
            },
        )
        .with_doc("SQL mode flags; comma-separated string."),
        |ctx| match ctx.scope {
            Scope::Global => SysVarValue::String(global_sql_mode()),
            _ => ctx
                .session
                .map(|s| SysVarValue::String(s.sql_mode.clone()))
                .unwrap_or_else(|| SysVarValue::String(global_sql_mode())),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let value = v.to_display_string();
            if ctx.is_global() {
                *global_sql_mode_cell().write().unwrap() = value;
            } else {
                ctx.session.sql_mode = value;
                ctx.session.temporal_sql_mode_bits =
                    crate::engine::api::parse_temporal_sql_mode_bits(&ctx.session.sql_mode)
                        | crate::engine::api::TEMPORAL_MODE_EXPLICIT;
            }
            Ok(())
        },
    ));

    // group_concat_max_len — independent global default and session limit.
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "group_concat_max_len",
            Scope::Both,
            DataType::UInt {
                min: 4,
                max: u64::MAX,
            },
        )
        .with_doc("Maximum bytes a GROUP_CONCAT result is allowed to occupy."),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::UInt(s.group_concat_max_len),
            None => SysVarValue::UInt(1024),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            ctx.session.group_concat_max_len = v.as_u64().unwrap_or(1024).max(4);
            Ok(())
        },
    ));

    // default_week_format — Both. Implicit mode for 1-arg WEEK().
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "default_week_format",
            Scope::Both,
            DataType::UInt { min: 0, max: 7 },
        )
        .with_doc("Default mode for the WEEK() function (0-7)."),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::UInt(s.sysvars.default_week_format),
            None => SysVarValue::UInt(0),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            ctx.session.sysvars.default_week_format = v.as_u64().unwrap_or(0).min(7);
            Ok(())
        },
    ));

    // sql_safe_updates — SESSION/Both. Enforced at UPDATE/DELETE dispatch.
    reg.register(SysVar::session_backed(
        SysVarDef::new("sql_safe_updates", Scope::Both, DataType::Bool)
            .with_doc("Reject UPDATE/DELETE without WHERE/LIMIT or non-key WHERE."),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::Bool(s.sysvars.sql_safe_updates),
            None => SysVarValue::Bool(false),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            ctx.session.sysvars.sql_safe_updates = v.as_bool().unwrap_or(false);
            Ok(())
        },
    ));

    // sql_select_limit — SESSION/Both. Implicit LIMIT for bare SELECTs.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "sql_select_limit",
            Scope::Both,
            DataType::UInt {
                min: 0,
                max: u64::MAX,
            },
        )
        .with_doc("Implicit LIMIT for SELECTs that omit one. DEFAULT = no limit."),
        |ctx| match ctx.scope {
            Scope::Global => SysVarValue::UInt(global_sql_select_limit()),
            Scope::Session => SysVarValue::UInt(
                ctx.session
                    .map(|s| s.sysvars.sql_select_limit)
                    .unwrap_or_else(global_sql_select_limit),
            ),
            Scope::Both => unreachable!("read scope is always resolved"),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let value = v.as_u64().unwrap_or(u64::MAX);
            match ctx.scope {
                Scope::Global => GLOBAL_SQL_SELECT_LIMIT.store(value, Ordering::Relaxed),
                Scope::Session => ctx.session.sysvars.sql_select_limit = value,
                Scope::Both => unreachable!("write scope is always resolved"),
            }
            Ok(())
        },
    ));

    // max_execution_time — SESSION/Both (ms). 0 = no limit.
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "max_execution_time",
            Scope::Both,
            DataType::UInt {
                min: 0,
                max: u64::MAX,
            },
        )
        .with_doc("Statement timeout in milliseconds (SELECT). 0 disables."),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::UInt(s.sysvars.max_execution_time_ms),
            None => SysVarValue::UInt(0),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            ctx.session.sysvars.max_execution_time_ms = v.as_u64().unwrap_or(0);
            Ok(())
        },
    ));

    // sql_quote_show_create / sql_notes / sql_warnings / sql_big_selects.
    // No GIPKs are generated by mydb. Retain the metadata preference for
    // MySQL clients without treating explicit invisible keys as generated.
    register_session_bool(
        reg,
        "show_gipk_in_create_table_and_information_schema",
        |s| s.sysvars.show_gipk_in_create_table_and_information_schema,
        |s, b| s.sysvars.show_gipk_in_create_table_and_information_schema = b,
        "GIPK metadata visibility preference; mydb does not generate invisible primary keys.",
        true,
    );
    register_session_bool(
        reg,
        "sql_quote_show_create",
        |s| s.sysvars.sql_quote_show_create,
        |s, b| s.sysvars.sql_quote_show_create = b,
        "Backtick-quote identifiers in SHOW CREATE output.",
        true,
    );
    register_session_bool(
        reg,
        "sql_notes",
        |s| s.sysvars.sql_notes,
        |s, b| s.sysvars.sql_notes = b,
        "Accumulate NOTE-level warnings.",
        true,
    );
    register_session_bool(
        reg,
        "sql_warnings",
        |s| s.sysvars.sql_warnings,
        |s, b| s.sysvars.sql_warnings = b,
        "Emit per-row warning summary on multi-row INSERT.",
        false,
    );
    register_session_bool(
        reg,
        "sql_big_selects",
        |s| s.sysvars.sql_big_selects,
        |s, b| s.sysvars.sql_big_selects = b,
        "Allow SELECTs that estimate over max_join_size rows.",
        true,
    );
    register_session_bool(
        reg,
        "sql_buffer_result",
        |s| s.sysvars.sql_buffer_result,
        |s, b| s.sysvars.sql_buffer_result = b,
        "Force SELECTs to materialise into a tmp table.",
        false,
    );
    register_session_bool(
        reg,
        "sql_auto_is_null",
        |s| s.sysvars.sql_auto_is_null,
        |s, b| s.sysvars.sql_auto_is_null = b,
        "MySQL 4.x legacy behaviour for IS NULL after AUTO_INCREMENT.",
        false,
    );
    register_session_bool(
        reg,
        "big_tables",
        |s| s.sysvars.big_tables,
        |s, b| s.sysvars.big_tables = b,
        "Prefer governed spill-backed internal temporary tables.",
        false,
    );

    // optimizer_switch — SetType-ish but we store the raw text.
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "optimizer_switch",
            Scope::Both,
            DataType::String { max_len: None },
        )
        .with_doc("Optimizer flag bitmap."),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::String(s.sysvars.optimizer_switch.clone()),
            None => SysVarValue::String(default_optimizer_switch().to_string()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let raw = v.to_display_string();
            // Merge the delta into the existing string per MySQL semantics:
            // `SET optimizer_switch = 'hash_join=off,semijoin=off'` updates
            // only those flags, leaving others untouched.
            let updated = merge_optimizer_switch(&ctx.session.sysvars.optimizer_switch, &raw)
                .map_err(|msg| wrong_value_for_var("optimizer_switch", &msg))?;
            ctx.session.sysvars.use_invisible_indexes =
                updated.contains("use_invisible_indexes=on");
            ctx.session.sysvars.optimizer_switch = updated;
            Ok(())
        },
    ));

    // optimizer_trace family.
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "optimizer_trace",
            Scope::Both,
            DataType::String { max_len: None },
        )
        .with_doc("Optimizer trace control flags."),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::String(s.sysvars.optimizer_trace.clone()),
            None => SysVarValue::String("enabled=off,one_line=off".to_string()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            ctx.session.sysvars.optimizer_trace = v.to_display_string();
            Ok(())
        },
    ));
    register_settable_uint(
        reg,
        "optimizer_trace_offset",
        Scope::Both,
        -1,
        i64::MAX,
        "Offset into trace ring buffer (kept for compat).",
    );
    register_settable_uint(
        reg,
        "optimizer_trace_limit",
        Scope::Both,
        1,
        1_000_000,
        "Trace ring buffer capacity (kept for compat).",
    );
    register_settable_uint(
        reg,
        "optimizer_trace_max_mem_size",
        Scope::Both,
        0,
        i64::MAX,
        "Maximum memory the optimizer trace may use.",
    );
    register_settable_uint(
        reg,
        "optimizer_trace_features",
        Scope::Both,
        -1,
        i64::MAX,
        "Bitmap of optimizer trace features.",
    );

    // explain_format — Both. Atomic for global; per-session field for SESSION.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "explain_format",
            Scope::Both,
            DataType::Enum {
                values: &["TRADITIONAL", "TREE", "JSON"],
            },
        )
        .with_doc("Default format for EXPLAIN."),
        |ctx| {
            let format = match (ctx.session, ctx.scope) {
                (Some(s), Scope::Session) => s.explain_format,
                _ => global_explain_format(),
            };
            SysVarValue::String(explain_format_name(format).to_string())
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let raw = v.to_display_string();
            let format = parse_explain_format_name(&raw)
                .ok_or_else(|| wrong_value_for_var("explain_format", &raw))?;
            match ctx.scope {
                Scope::Session => ctx.session.explain_format = format,
                Scope::Global => {
                    set_global_explain_format(format);
                }
                Scope::Both => unreachable!(),
            }
            Ok(())
        },
    ));

    reg.register(SysVar::settable(
        SysVarDef::new(
            "resultset_metadata",
            Scope::Session,
            DataType::Enum {
                values: &["FULL", "NONE"],
            },
        )
        .with_doc("Controls whether result-set column metadata is sent."),
        |ctx| {
            SysVarValue::String(
                if ctx
                    .session
                    .is_none_or(|session| session.resultset_metadata_full)
                {
                    "FULL"
                } else {
                    "NONE"
                }
                .to_string(),
            )
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            ctx.session.resultset_metadata_full =
                v.to_display_string().eq_ignore_ascii_case("FULL");
            Ok(())
        },
    ));

    reg.register(SysVar::settable(
        SysVarDef::new(
            "vector_result_format",
            Scope::Session,
            DataType::Enum {
                values: &["NATIVE", "TEXT"],
            },
        )
        .with_doc("VECTOR output on the text protocol; prepared binary results remain native."),
        |ctx| {
            SysVarValue::String(
                if ctx.session.is_some_and(|s| s.vector_result_format_text) {
                    "TEXT"
                } else {
                    "NATIVE"
                }
                .into(),
            )
        },
        |ctx: &mut WriteCtx<'_>, value: SysVarValue| -> Result<(), QueryError> {
            ctx.session.vector_result_format_text =
                value.to_display_string().eq_ignore_ascii_case("TEXT");
            Ok(())
        },
    ));

    // event_scheduler — Global only. Real backing via events module.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "event_scheduler",
            Scope::Global,
            DataType::Enum {
                values: &["OFF", "ON", "DISABLED"],
            },
        )
        .with_doc("Background event scheduler state.")
        // E8/G11: SET GLOBAL event_scheduler requires SYSTEM_VARIABLES_ADMIN
        // (or SUPER), matching MySQL.
        .with_access(Access::SuperReadWrite),
        |_ctx| {
            let state = crate::engine::executor::events::scheduler_state();
            SysVarValue::String(state.as_str().to_string())
        },
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let raw = v.to_display_string();
            let state =
                crate::engine::executor::events::EventSchedulerState::from_str_lenient(&raw)
                    .ok_or_else(|| wrong_value_for_var("event_scheduler", &raw))?;
            if state == crate::engine::executor::events::EventSchedulerState::Disabled {
                return Err(wrong_value_for_var("event_scheduler", &raw));
            }
            if crate::engine::executor::events::scheduler_state()
                == crate::engine::executor::events::EventSchedulerState::Disabled
            {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
                    "The server was started with MYDB_EVENT_SCHEDULER=disabled; event_scheduler cannot be changed at runtime",
                ));
            }
            crate::engine::executor::events::set_scheduler_state(state);
            Ok(())
        },
    ).with_validation(|_, value| {
        let raw = value.to_display_string();
        let state = crate::engine::executor::events::EventSchedulerState::from_str_lenient(&raw)
            .ok_or_else(|| wrong_value_for_var("event_scheduler", &raw))?;
        if state == crate::engine::executor::events::EventSchedulerState::Disabled {
            return Err(wrong_value_for_var("event_scheduler", &raw));
        }
        if crate::engine::executor::events::scheduler_state()
            == crate::engine::executor::events::EventSchedulerState::Disabled {
            return Err(crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT.error(
                "The server was started with MYDB_EVENT_SCHEDULER=disabled; event_scheduler cannot be changed at runtime"));
        }
        Ok(())
    }));

    // div_precision_increment — Both. Used by `/` to decide decimal scale.
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "div_precision_increment",
            Scope::Both,
            DataType::UInt { min: 0, max: 30 },
        )
        .with_doc("Extra decimal scale for the `/` operator."),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::UInt(s.sysvars.div_precision_increment),
            None => SysVarValue::UInt(4),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            ctx.session.sysvars.div_precision_increment = v.as_u64().unwrap_or(4).min(30);
            Ok(())
        },
    ));

    // cte_max_recursion_depth — inherited global default and session override.
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "cte_max_recursion_depth",
            Scope::Both,
            DataType::UInt {
                min: 0,
                max: u32::MAX as u64,
            },
        )
        .with_doc("Maximum recursion depth for recursive CTEs."),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::UInt(s.cte_max_recursion_depth as u64),
            None => SysVarValue::UInt(1000),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            ctx.session.cte_max_recursion_depth = v.as_u64().unwrap_or(1000) as u32;
            Ok(())
        },
    ));

    // max_sp_recursion_depth — Both. How deep a stored procedure may recurse
    // (0 = recursion disabled). Stored functions forbid recursion regardless.
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "max_sp_recursion_depth",
            Scope::Both,
            DataType::UInt { min: 0, max: 255 },
        )
        .with_doc("Number of times a stored procedure may recurse (0 disables)."),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::UInt(s.max_sp_recursion_depth as u64),
            None => SysVarValue::UInt(0),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            ctx.session.max_sp_recursion_depth = v.as_u64().unwrap_or(0).min(255) as u32;
            Ok(())
        },
    ));

    // range_optimizer_max_mem_size / range_alloc_block_size — advisory.
    register_session_uint64(
        reg,
        "range_optimizer_max_mem_size",
        |s| s.sysvars.range_optimizer_max_mem_size,
        |s, n| s.sysvars.range_optimizer_max_mem_size = n,
        "Memory cap for the range optimizer.",
        8 * 1024 * 1024,
    );
    register_session_uint64(
        reg,
        "range_alloc_block_size",
        |s| s.sysvars.range_alloc_block_size,
        |s, n| s.sysvars.range_alloc_block_size = n,
        "Range-allocator block size (advisory).",
        4096,
    );
    register_session_uint64(
        reg,
        "transaction_alloc_block_size",
        |s| s.sysvars.transaction_alloc_block_size,
        |s, n| s.sysvars.transaction_alloc_block_size = n,
        "Per-txn allocator block size (advisory).",
        8192,
    );
    register_session_uint64(
        reg,
        "transaction_prealloc_size",
        |s| s.sysvars.transaction_prealloc_size,
        |s, n| s.sysvars.transaction_prealloc_size = n,
        "Per-txn preallocation (advisory).",
        4096,
    );
    register_session_uint64(
        reg,
        "query_alloc_block_size",
        |s| s.sysvars.query_alloc_block_size,
        |s, n| s.sysvars.query_alloc_block_size = n,
        "Per-query allocator block size (advisory).",
        8192,
    );
    register_session_uint64(
        reg,
        "query_prealloc_size",
        |s| s.sysvars.query_prealloc_size,
        |s, n| s.sysvars.query_prealloc_size = n,
        "Per-query preallocation (advisory).",
        8192,
    );
    register_session_uint64(
        reg,
        "eq_range_index_dive_limit",
        |s| s.sysvars.eq_range_index_dive_limit,
        |s, n| s.sysvars.eq_range_index_dive_limit = n,
        "Switch from dive to stats above this many ranges.",
        200,
    );
    register_session_uint64(
        reg,
        "tmp_table_size",
        |s| s.sysvars.tmp_table_size,
        |s, n| s.sysvars.tmp_table_size = n,
        "Max in-memory tmp table size before spill (advisory).",
        16 * 1024 * 1024,
    );
    register_session_uint64(
        reg,
        "max_heap_table_size",
        |s| s.sysvars.max_heap_table_size,
        |s, n| s.sysvars.max_heap_table_size = n,
        "Max size for user-created MEMORY tables (advisory).",
        16 * 1024 * 1024,
    );
    register_session_uint64(
        reg,
        "sort_buffer_size",
        |s| s.sysvars.sort_buffer_size,
        |s, n| s.sysvars.sort_buffer_size = n,
        "Per-session sort buffer (advisory).",
        262_144,
    );
    register_session_uint64(
        reg,
        "join_buffer_size",
        |s| s.sysvars.join_buffer_size,
        |s, n| s.sysvars.join_buffer_size = n,
        "Per-session join buffer (advisory).",
        262_144,
    );
    register_session_uint64(
        reg,
        "read_buffer_size",
        |s| s.sysvars.read_buffer_size,
        |s, n| s.sysvars.read_buffer_size = n,
        "Per-session read buffer (advisory).",
        131_072,
    );
    register_session_uint64(
        reg,
        "read_rnd_buffer_size",
        |s| s.sysvars.read_rnd_buffer_size,
        |s, n| s.sysvars.read_rnd_buffer_size = n,
        "Per-session random-read buffer (advisory).",
        262_144,
    );
    register_session_uint64(
        reg,
        "bulk_insert_buffer_size",
        |s| s.sysvars.bulk_insert_buffer_size,
        |s, n| s.sysvars.bulk_insert_buffer_size = n,
        "Per-session bulk-insert buffer (advisory).",
        8 * 1024 * 1024,
    );

    register_storage_engine(
        reg,
        "default_storage_engine",
        false,
        "Default storage engine for new tables (mydb has one engine).",
    );
    register_storage_engine(
        reg,
        "default_tmp_storage_engine",
        true,
        "Default storage engine for tmp tables (mydb has one engine).",
    );
    reg.register(SysVar::settable(
        SysVarDef::new(
            "internal_tmp_mem_storage_engine",
            Scope::Session,
            DataType::Enum {
                values: &["TempTable", "MEMORY"],
            },
        )
        .with_doc("Storage implementation requested for internal in-memory temporary tables."),
        |ctx| {
            SysVarValue::String(
                ctx.session
                    .map(|session| session.sysvars.internal_tmp_mem_storage_engine.clone())
                    .unwrap_or_else(|| "TempTable".to_string()),
            )
        },
        |ctx: &mut WriteCtx<'_>, value: SysVarValue| -> Result<(), QueryError> {
            ctx.session.sysvars.internal_tmp_mem_storage_engine = value.to_display_string();
            Ok(())
        },
    ));
    // storage_engine is the legacy 5.5 alias for default_storage_engine.
    register_storage_engine(
        reg,
        "storage_engine",
        false,
        "Legacy alias for default_storage_engine.",
    );

    // lc_messages / lc_time_names — locale.
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "lc_messages",
            Scope::Both,
            DataType::String { max_len: Some(64) },
        )
        .with_doc("Locale for diagnostic messages."),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::String(s.sysvars.lc_messages.clone()),
            None => SysVarValue::String("en_US".to_string()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            ctx.session.sysvars.lc_messages = v.to_display_string();
            Ok(())
        },
    ));
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "lc_time_names",
            Scope::Both,
            DataType::String { max_len: Some(64) },
        )
        .with_doc("Locale for day/month names."),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::String(s.sysvars.lc_time_names.clone()),
            None => SysVarValue::String("en_US".to_string()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            ctx.session.sysvars.lc_time_names = v.to_display_string();
            Ok(())
        },
    ));
    reg.register(SysVar::const_read_only(
        SysVarDef::new(
            "lc_messages_dir",
            Scope::Global,
            DataType::String { max_len: None },
        ),
        SysVarValue::String("/usr/share/mysql/".to_string()),
    ));

    // max_join_size — Both. Advisory in mydb.
    register_settable_uint(
        reg,
        "max_join_size",
        Scope::Both,
        1,
        i64::MAX,
        "Maximum rows from a JOIN before refusal.",
    );
    register_settable_uint(
        reg,
        "max_sort_length",
        Scope::Both,
        4,
        65_535,
        "Bytes of each ORDER BY key considered for sort.",
    );
    register_settable_uint(
        reg,
        "max_seeks_for_key",
        Scope::Both,
        1,
        i64::MAX,
        "Soft upper bound on seek estimates for index lookups.",
    );
    register_settable_uint(
        reg,
        "max_length_for_sort_data",
        Scope::Both,
        4,
        8_388_608,
        "Switch from packed to row-pointer sort above this length.",
    );

    // M5.1/M5.2/M5.3 — HNSW vector-index query-time knobs. Session-level so
    // per-connection workloads can tune candidate-pool size, iterative-scan
    // policy, and brute-force fallback thresholds without restarting.
    register_session_uint64_aliased(
        reg,
        "mydb_hnsw_ef_search",
        |s| s.sysvars.mydb_hnsw_ef_search,
        |s, n| {
            s.sysvars.mydb_hnsw_ef_search = n;
            s.sysvars.mydb_hnsw_ef_search_explicit = true;
        },
        "HNSW base candidate-pool size for KNN search.",
        64,
        // pgvector dot-form name so its tooling / muscle memory works as-is.
        &["hnsw.ef_search"],
    );
    register_session_uint64(
        reg,
        "mydb_hnsw_ef_dynamic_factor",
        |s| s.sysvars.mydb_hnsw_ef_dynamic_factor,
        |s, n| s.sysvars.mydb_hnsw_ef_dynamic_factor = n,
        "HNSW dynamic ef multiplier vs LIMIT k (0 disables scaling).",
        4,
    );
    register_session_uint64_aliased(
        reg,
        "mydb_hnsw_max_scan_tuples",
        |s| s.sysvars.mydb_hnsw_max_scan_tuples,
        |s, n| s.sysvars.mydb_hnsw_max_scan_tuples = n,
        "HNSW iterative-scan upper bound on candidate pool.",
        20_000,
        &["hnsw.max_scan_tuples"],
    );
    register_session_uint64(
        reg,
        "mydb_hnsw_brute_threshold",
        |s| s.sysvars.mydb_hnsw_brute_threshold,
        |s, n| s.sysvars.mydb_hnsw_brute_threshold = n,
        "Table-rows threshold below which KNN falls back to brute scan.",
        10_000,
    );
    register_session_uint64(
        reg,
        "mydb_hnsw_brute_filter_rows",
        |s| s.sysvars.mydb_hnsw_brute_filter_rows,
        |s, n| s.sysvars.mydb_hnsw_brute_filter_rows = n,
        "Estimated filter-row count below which KNN falls back to brute scan.",
        1_000,
    );

    register_session_uint64(
        reg,
        "mydb_hnsw_rerank_mult",
        |s| s.sysvars.mydb_hnsw_rerank_mult,
        |s, n| {
            s.sysvars.mydb_hnsw_rerank_mult = n.clamp(1, 1024);
            s.sysvars.mydb_hnsw_rerank_mult_explicit = true;
        },
        "HNSW exact heap-rerank candidate multiplier vs LIMIT k.",
        4,
    );

    // IVF query-time knob: number of inverted-file cells probed per KNN
    // query (the IVF analogue of HNSW `ef_search`). pgvector-style dot-form
    // alias `ivf.nprobe` for muscle memory / tooling parity.
    register_session_uint64_aliased(
        reg,
        "mydb_ivf_nprobe",
        |s| s.sysvars.mydb_ivf_nprobe,
        |s, n| {
            s.sysvars.mydb_ivf_nprobe = n;
            s.sysvars.mydb_ivf_nprobe_explicit = true;
        },
        "IVF number of inverted lists to probe per KNN search.",
        8,
        &["ivf.nprobe"],
    );
    register_session_uint64(
        reg,
        "mydb_ivf_rerank_mult",
        |s| s.sysvars.mydb_ivf_rerank_mult,
        |s, n| {
            s.sysvars.mydb_ivf_rerank_mult = n.clamp(1, 1024);
            s.sysvars.mydb_ivf_rerank_mult_explicit = true;
        },
        "IVF exact heap-rerank candidate multiplier vs LIMIT k.",
        4,
    );
    register_session_uint64_aliased(
        reg,
        "mydb_ivf_max_scan_tuples",
        |s| s.sysvars.mydb_ivf_max_scan_tuples,
        |s, n| s.sysvars.mydb_ivf_max_scan_tuples = n,
        "IVF iterative-scan upper bound on candidate tuples.",
        20_000,
        &["ivf.max_scan_tuples"],
    );
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "mydb_ivf_iterative_scan",
            Scope::Both,
            DataType::String { max_len: Some(32) },
        )
        .with_doc("IVF iterative-scan policy: off, relaxed_order, strict_order (default).")
        .with_aliases(&["ivf.iterative_scan"]),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::String(s.sysvars.mydb_ivf_iterative_scan.clone()),
            None => SysVarValue::String("strict_order".to_string()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let raw = v.to_display_string();
            let lower = raw.trim().to_ascii_lowercase();
            match lower.as_str() {
                "off" | "relaxed_order" | "strict_order" => {
                    ctx.session.sysvars.mydb_ivf_iterative_scan = lower;
                    Ok(())
                }
                _ => Err(wrong_value_for_var("mydb_ivf_iterative_scan", &raw)),
            }
        },
    ));

    // String-valued: iterative-scan policy. pgvector users know
    // `hnsw.iterative_scan`; we expose the mydb-prefixed name and accept
    // the same three values they use ({off,relaxed_order,strict_order}).
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "mydb_hnsw_iterative_scan",
            Scope::Both,
            DataType::String { max_len: Some(32) },
        )
        .with_doc("HNSW iterative-scan policy: off, relaxed_order, strict_order (default).")
        .with_aliases(&["hnsw.iterative_scan"]),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::String(s.sysvars.mydb_hnsw_iterative_scan.clone()),
            None => SysVarValue::String("strict_order".to_string()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let raw = v.to_display_string();
            let lower = raw.trim().to_ascii_lowercase();
            match lower.as_str() {
                "off" | "relaxed_order" | "strict_order" => {
                    ctx.session.sysvars.mydb_hnsw_iterative_scan = lower;
                    Ok(())
                }
                _ => Err(wrong_value_for_var("mydb_hnsw_iterative_scan", &raw)),
            }
        },
    ));
}

fn default_optimizer_switch() -> &'static str {
    crate::engine::api::default_optimizer_switch()
}

/// Documented default value for an optimizer flag, looked up from the
/// canonical default string. `None` means the flag name is unknown —
/// MySQL rejects those with ER 1231.
fn optimizer_flag_default(key: &str) -> Option<&'static str> {
    for piece in default_optimizer_switch().split(',') {
        if let Some((k, v)) = piece.trim().split_once('=') {
            if k.trim() == key {
                return Some(if v.trim() == "on" { "on" } else { "off" });
            }
        }
    }
    None
}

/// Apply an `optimizer_switch` delta. The MySQL semantics: the new value
/// is a comma-separated list of `key=on|off|default` entries; each entry
/// updates the current setting for `key`, leaving the rest alone.
/// Unknown flag names are rejected (MySQL ER 1231), so a flag that
/// "takes" is guaranteed to exist — declared capability stays honest.
fn merge_optimizer_switch(current: &str, delta: &str) -> Result<String, String> {
    // Parse current into ordered (key, value) pairs preserving insertion order.
    let mut entries: Vec<(String, String)> = Vec::new();
    for piece in current.split(',') {
        let piece = piece.trim();
        if piece.is_empty() {
            continue;
        }
        if let Some((k, v)) = piece.split_once('=') {
            entries.push((k.trim().to_ascii_lowercase(), v.trim().to_ascii_lowercase()));
        }
    }
    for piece in delta.split(',') {
        let piece = piece.trim();
        if piece.is_empty() {
            continue;
        }
        let (k, v) = piece
            .split_once('=')
            .ok_or_else(|| format!("expected key=value, got '{piece}'"))?;
        let key = k.trim().to_ascii_lowercase();
        let val = v.trim().to_ascii_lowercase();
        if val != "on" && val != "off" && val != "default" {
            return Err(format!("invalid optimizer flag value '{val}'"));
        }
        let Some(documented_default) = optimizer_flag_default(&key) else {
            return Err(format!("unknown optimizer flag '{key}'"));
        };
        let val_to_store = if val == "default" {
            documented_default.to_string()
        } else {
            val
        };
        if let Some(slot) = entries.iter_mut().find(|(existing, _)| existing == &key) {
            slot.1 = val_to_store;
        } else {
            entries.push((key, val_to_store));
        }
    }
    Ok(entries
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(","))
}

fn register_session_bool(
    reg: &mut Registry,
    name: &'static str,
    get: fn(&crate::engine::api::SessionContext) -> bool,
    set: fn(&mut crate::engine::api::SessionContext, bool),
    doc: &'static str,
    default: bool,
) {
    reg.register(SysVar::session_backed(
        SysVarDef::new(name, Scope::Both, DataType::Bool).with_doc(doc),
        move |ctx| match ctx.session {
            Some(s) => SysVarValue::Bool(get(s)),
            None => SysVarValue::Bool(default),
        },
        move |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            set(ctx.session, v.as_bool().unwrap_or(default));
            Ok(())
        },
    ));
}

fn register_session_uint64(
    reg: &mut Registry,
    name: &'static str,
    get: fn(&crate::engine::api::SessionContext) -> u64,
    set: fn(&mut crate::engine::api::SessionContext, u64),
    doc: &'static str,
    default: u64,
) {
    register_session_uint64_aliased(reg, name, get, set, doc, default, &[]);
}

/// As [`register_session_uint64`] but with accepted alias names (e.g. the
/// pgvector dot-form `hnsw.ef_search` for `mydb_hnsw_ef_search`).
#[allow(clippy::too_many_arguments)]
fn register_session_uint64_aliased(
    reg: &mut Registry,
    name: &'static str,
    get: fn(&crate::engine::api::SessionContext) -> u64,
    set: fn(&mut crate::engine::api::SessionContext, u64),
    doc: &'static str,
    default: u64,
    aliases: &'static [&'static str],
) {
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            name,
            Scope::Both,
            DataType::UInt {
                min: 0,
                max: u64::MAX,
            },
        )
        .with_doc(doc)
        .with_aliases(aliases),
        move |ctx| match ctx.session {
            Some(s) => SysVarValue::UInt(get(s)),
            None => SysVarValue::UInt(default),
        },
        move |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            set(ctx.session, v.as_u64().unwrap_or(default));
            Ok(())
        },
    ));
}

fn register_settable_uint(
    reg: &mut Registry,
    name: &'static str,
    scope: Scope,
    _min: i64,
    _max: i64,
    doc: &'static str,
) {
    // Standalone (no backing storage) variable that round-trips. Used for
    // MySQL-compat knobs mydb honours conceptually but doesn't gate on.
    // The default is whatever MySQL exposes; we return 0 if unset because
    // these are advisory tuning knobs.
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;
    static CELLS: OnceLock<
        std::sync::Mutex<std::collections::HashMap<&'static str, &'static AtomicU64>>,
    > = OnceLock::new();
    let cells = CELLS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let cell: &'static AtomicU64 = {
        let mut g = cells.lock().expect("cells lock");
        if let Some(c) = g.get(name) {
            c
        } else {
            let leaked: &'static AtomicU64 = Box::leak(Box::new(AtomicU64::new(0)));
            g.insert(name, leaked);
            leaked
        }
    };
    reg.register(SysVar::settable(
        SysVarDef::new(
            name,
            scope,
            DataType::UInt {
                min: 0,
                max: u64::MAX,
            },
        )
        .with_doc(doc),
        move |_ctx| SysVarValue::UInt(cell.load(Ordering::Relaxed)),
        move |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            cell.store(v.as_u64().unwrap_or(0), Ordering::Relaxed);
            Ok(())
        },
    ));
}

fn register_storage_engine(
    reg: &mut Registry,
    name: &'static str,
    temporary: bool,
    doc: &'static str,
) {
    reg.register(SysVar::session_backed(
        SysVarDef::new(name, Scope::Both, DataType::String { max_len: Some(64) }).with_doc(doc),
        move |ctx| {
            SysVarValue::String(match ctx.session {
                Some(session) if temporary => session.sysvars.default_tmp_storage_engine.clone(),
                Some(session) => session.sysvars.default_storage_engine.clone(),
                None => "InnoDB".to_string(),
            })
        },
        move |ctx: &mut WriteCtx<'_>, value: SysVarValue| -> Result<(), QueryError> {
            let SysVarValue::String(engine) = value else {
                return Err(wrong_value_for_var(name, "NULL"));
            };
            if !engine.eq_ignore_ascii_case("InnoDB") && !engine.eq_ignore_ascii_case("mydb") {
                return Err(crate::error::catalog::ER_UNKNOWN_STORAGE_ENGINE
                    .error(format!("Unknown storage engine '{engine}'")));
            }
            if temporary {
                ctx.session.sysvars.default_tmp_storage_engine = engine;
            } else {
                ctx.session.sysvars.default_storage_engine = engine;
            }
            Ok(())
        },
    ));
}
