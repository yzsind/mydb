//! Transaction-control variables — autocommit, isolation, foreign_key_checks,
//! unique_checks, transaction_read_only.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::engine::api::{global_transaction_isolation, set_global_transaction_isolation};
use crate::engine::sysvar::backend::SysVar;
use crate::engine::sysvar::ctx::WriteCtx;
use crate::engine::sysvar::meta::{DataType, Scope, SysVarDef};
use crate::engine::sysvar::registry::Registry;
use crate::engine::sysvar::value::SysVarValue;
use crate::storage::IsolationLevel;
use crate::types::QueryError;

/// MySQL isolation enum values in the order MySQL accepts them.
const ISOLATION_LEVELS: &[&str] = &[
    "READ-UNCOMMITTED",
    "READ-COMMITTED",
    "REPEATABLE-READ",
    "SERIALIZABLE",
    "READ UNCOMMITTED",
    "READ COMMITTED",
    "REPEATABLE READ",
];

static AUTOMATIC_SP_PRIVILEGES: AtomicBool = AtomicBool::new(true);

pub(crate) fn automatic_sp_privileges() -> bool {
    AUTOMATIC_SP_PRIVILEGES.load(Ordering::Relaxed)
}

pub(crate) fn register(reg: &mut Registry) {
    // Both-scoped transaction defaults are inherited only by new sessions.
    reg.register(SysVar::session_backed(
        SysVarDef::new("autocommit", Scope::Both, DataType::Bool)
            .with_doc("Auto-commit each statement when not in an explicit transaction."),
        |ctx| match (ctx.session, ctx.scope) {
            (Some(s), Scope::Session) => SysVarValue::Bool(s.autocommit),
            _ => SysVarValue::Bool(true),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let b = v.as_bool().unwrap_or(true);
            ctx.session.autocommit = b;
            Ok(())
        },
    ));

    // FK enforcement is installed by the statement guard. The global
    // connection default must never write the guard's thread-local state.
    reg.register(SysVar::session_backed(
        SysVarDef::new("foreign_key_checks", Scope::Both, DataType::Bool)
            .with_doc("Enforce FK constraints during DML in this session."),
        |ctx| match (ctx.session, ctx.scope) {
            (Some(s), Scope::Session) => SysVarValue::Bool(s.foreign_key_checks),
            _ => SysVarValue::Bool(true),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let b = v.as_bool().unwrap_or(true);
            ctx.session.foreign_key_checks = b;
            Ok(())
        },
    ));

    // unique_checks — SESSION/Both. mydb honours it on the DML path.
    reg.register(SysVar::session_backed(
        SysVarDef::new("unique_checks", Scope::Both, DataType::Bool)
            .with_doc("Enforce UNIQUE constraints during DML in this session."),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::Bool(s.sysvars.unique_checks),
            None => SysVarValue::Bool(true),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            ctx.session.sysvars.unique_checks = v.as_bool().unwrap_or(true);
            Ok(())
        },
    ));

    // transaction_read_only — Both. The session value is set by
    // SET TRANSACTION READ ONLY / explicit txn declaration; SET writes
    // become the next-txn default.
    reg.register(SysVar::settable(
        SysVarDef::new("transaction_read_only", Scope::Both, DataType::Bool)
            .with_aliases(&["tx_read_only"])
            .with_doc("Default access mode for the next transaction."),
        |ctx| match (ctx.scope, ctx.session) {
            (Scope::Session, Some(s)) => SysVarValue::Bool(s.sysvars.transaction_read_only),
            _ => SysVarValue::Bool(crate::engine::api::global_transaction_read_only()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let b = v.as_bool().unwrap_or(false);
            if ctx.is_global() {
                crate::engine::api::set_global_transaction_read_only(b);
                return Ok(());
            }
            ctx.session.sysvars.transaction_read_only = b;
            if !ctx.session.in_transaction() {
                ctx.session.read_only_txn = b;
            }
            Ok(())
        },
    ));

    // sql_log_off — SESSION only; off means the general log doesn't
    // record this session's queries.
    reg.register(SysVar::settable(
        SysVarDef::new("sql_log_off", Scope::Session, DataType::Bool)
            .with_doc("Suppress this session from the general query log."),
        |ctx| match ctx.session {
            Some(s) => SysVarValue::Bool(s.sql_log_off),
            None => SysVarValue::Bool(false),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            ctx.session.sql_log_off = v.as_bool().unwrap_or(false);
            Ok(())
        },
    ));

    // transaction_isolation — Both. The session value is per session;
    // GLOBAL writes flip the atomic that new sessions inherit. Carries
    // the legacy `tx_isolation` alias.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "transaction_isolation",
            Scope::Both,
            DataType::Enum {
                values: ISOLATION_LEVELS,
            },
        )
        .with_aliases(&["tx_isolation"])
        .with_doc("Transaction isolation level."),
        |ctx| {
            let iso = match (ctx.session, ctx.scope) {
                (Some(s), Scope::Session) => s.isolation,
                _ => global_transaction_isolation(),
            };
            SysVarValue::String(iso.as_mysql_name().to_string())
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let raw = v.to_display_string();
            let iso = parse_isolation(&raw).ok_or_else(|| {
                crate::engine::sysvar::errors::wrong_value_for_var("transaction_isolation", &raw)
            })?;
            match ctx.scope {
                Scope::Session => {
                    ctx.session.isolation = iso;
                    ctx.session.next_txn_isolation = Some(iso);
                }
                Scope::Global => {
                    set_global_transaction_isolation(iso);
                }
                Scope::Both => unreachable!(),
            }
            Ok(())
        },
    ));

    // innodb_lock_wait_timeout — Both. Per-session row-lock wait timeout in
    // seconds; drives the storage-layer wait loop (M3). GLOBAL writes set the
    // default new sessions inherit. MySQL range 1..=1073741824, default 50.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "innodb_lock_wait_timeout",
            Scope::Both,
            DataType::UInt {
                min: 1,
                max: 1_073_741_824,
            },
        )
        .with_doc("Row-lock wait timeout in seconds before ER_LOCK_WAIT_TIMEOUT (1205)."),
        |ctx| match (ctx.session, ctx.scope) {
            (Some(s), Scope::Session) => SysVarValue::UInt(
                s.sysvars
                    .innodb_lock_wait_timeout
                    .unwrap_or_else(crate::engine::api::global_innodb_lock_wait_timeout),
            ),
            _ => SysVarValue::UInt(crate::engine::api::global_innodb_lock_wait_timeout()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let secs = v.as_u64().unwrap_or(50).clamp(1, 1_073_741_824);
            match ctx.scope {
                Scope::Global => crate::engine::api::set_global_innodb_lock_wait_timeout(secs),
                _ => ctx.session.sysvars.innodb_lock_wait_timeout = Some(secs),
            }
            Ok(())
        },
    ));

    // lock_wait_timeout — Both. Metadata-lock (MDL / DDL) wait timeout in
    // seconds. MySQL range 1..=31536000, default 31536000.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "lock_wait_timeout",
            Scope::Both,
            DataType::UInt {
                min: 1,
                max: 31_536_000,
            },
        )
        .with_doc("Metadata-lock wait timeout in seconds."),
        |ctx| match (ctx.session, ctx.scope) {
            (Some(s), Scope::Session) => SysVarValue::UInt(
                s.sysvars
                    .lock_wait_timeout
                    .unwrap_or_else(crate::engine::api::global_lock_wait_timeout),
            ),
            _ => SysVarValue::UInt(crate::engine::api::global_lock_wait_timeout()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let secs = v.as_u64().unwrap_or(31_536_000).clamp(1, 31_536_000);
            match ctx.scope {
                Scope::Global => crate::engine::api::set_global_lock_wait_timeout(secs),
                _ => ctx.session.sysvars.lock_wait_timeout = Some(secs),
            }
            Ok(())
        },
    ));

    // completion_type — Both. Default COMMIT/ROLLBACK completion behaviour
    // (0=NO_CHAIN, 1=CHAIN, 2=RELEASE). Drives the completion when the
    // statement carries no explicit AND [NO] CHAIN / [NO] RELEASE clause.
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "completion_type",
            Scope::Both,
            DataType::Enum {
                values: &["NO_CHAIN", "CHAIN", "RELEASE", "0", "1", "2"],
            },
        )
        .with_doc("Default transaction completion behaviour (NO_CHAIN/CHAIN/RELEASE)."),
        |ctx| {
            let ct = match ctx.session {
                Some(s) => s.sysvars.completion_type,
                None => 0,
            };
            SysVarValue::String(completion_type_name(ct).to_string())
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let raw = v.to_display_string();
            let ct = parse_completion_type(&raw).ok_or_else(|| {
                crate::engine::sysvar::errors::wrong_value_for_var("completion_type", &raw)
            })?;
            ctx.session.sysvars.completion_type = ct;
            Ok(())
        },
    ));

    // MySQL automatically grants ALTER ROUTINE and EXECUTE to a routine's
    // creator when this global variable is enabled.
    reg.register(SysVar::settable(
        SysVarDef::new("automatic_sp_privileges", Scope::Global, DataType::Bool)
            .with_doc("Auto-grant routine privileges to the creator on CREATE PROCEDURE."),
        |_ctx| SysVarValue::Bool(AUTOMATIC_SP_PRIVILEGES.load(Ordering::Relaxed)),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            AUTOMATIC_SP_PRIVILEGES.store(v.as_bool().unwrap_or(true), Ordering::Relaxed);
            Ok(())
        },
    ));
}

/// MySQL `completion_type` accepts both the numeric (0/1/2) and the named
/// (NO_CHAIN/CHAIN/RELEASE) forms, case-insensitively.
fn parse_completion_type(raw: &str) -> Option<u8> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "0" | "NO_CHAIN" => Some(0),
        "1" | "CHAIN" => Some(1),
        "2" | "RELEASE" => Some(2),
        _ => None,
    }
}

fn completion_type_name(ct: u8) -> &'static str {
    match ct {
        1 => "CHAIN",
        2 => "RELEASE",
        _ => "NO_CHAIN",
    }
}

fn parse_isolation(raw: &str) -> Option<IsolationLevel> {
    // Accept both `READ-COMMITTED` (canonical SHOW form) and the SET
    // form `READ COMMITTED` (whitespace), case-insensitive.
    let normalised = raw.trim().replace('-', " ").to_ascii_uppercase();
    match normalised.as_str() {
        "READ UNCOMMITTED" => Some(IsolationLevel::ReadUncommitted),
        "READ COMMITTED" => Some(IsolationLevel::ReadCommitted),
        "REPEATABLE READ" => Some(IsolationLevel::RepeatableRead),
        "SERIALIZABLE" => Some(IsolationLevel::Serializable),
        _ => IsolationLevel::parse_mysql_name(raw),
    }
}
