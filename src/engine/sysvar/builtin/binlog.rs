//! Binlog / GTID variables. Mostly read-only at runtime (set at server
//! startup); we expose the current values via the `binlog::variables_snapshot`
//! pipeline.

use crate::engine::sysvar::backend::SysVar;
use crate::engine::sysvar::ctx::WriteCtx;
use crate::engine::sysvar::meta::{Access, DataType, Scope, SysVarDef};
use crate::engine::sysvar::registry::Registry;
use crate::engine::sysvar::value::SysVarValue;
use crate::types::QueryError;

/// Read a binlog / GTID variable's current value out of the
/// `binlog::variables_snapshot` pipeline (single source of truth).
fn snapshot_value(canon: &str) -> SysVarValue {
    crate::binlog::variables_snapshot()
        .into_iter()
        .find(|(n, _)| (*n).eq_ignore_ascii_case(canon))
        .map(|(_, v)| SysVarValue::String(v))
        .unwrap_or(SysVarValue::String(String::new()))
}

pub(crate) fn register(reg: &mut Registry) {
    // Genuinely read-only at runtime (fixed at server startup / derived from
    // manager state): a `SET` returns the standard read-only error.
    let read_only: &[&'static str] = &[
        "log_bin",
        "binlog_checksum",
        "binlog_expire_logs_seconds",
        "server_id",
        "server_uuid",
        "gtid_mode",
        "enforce_gtid_consistency",
        "gtid_executed",
        "gtid_purged",
        "expire_logs_days",
        "max_binlog_cache_size",
        "max_binlog_stmt_cache_size",
    ];
    for name in read_only {
        let canon: &'static str = *name;
        reg.register(SysVar::settable(
            SysVarDef::new(canon, Scope::Global, DataType::String { max_len: None })
                .with_access(Access::ReadOnly)
                .with_doc("Binlog / GTID configuration (configured at server startup)."),
            move |_ctx| snapshot_value(canon),
            move |_ctx: &mut WriteCtx<'_>, _v: SysVarValue| -> Result<(), QueryError> {
                Err(crate::engine::sysvar::errors::read_only_variable(canon))
            },
        ));
    }

    // Boundary variables: mydb's value is fixed, but a `SET` to the supported
    // value is an idempotent no-op and anything else is a clear MySQL-aligned
    // error (never a silent read-only rejection or no-op). Needs
    // SYSTEM_VARIABLES_ADMIN like any GLOBAL write.
    type Validator = fn(&str) -> Result<(), QueryError>;
    let validated: &[(&'static str, Validator)] = &[
        (
            "binlog_format",
            crate::binlog::validate_binlog_format as Validator,
        ),
        ("binlog_row_image", crate::binlog::validate_binlog_row_image),
        (
            "binlog_row_metadata",
            crate::binlog::validate_binlog_row_metadata,
        ),
        (
            "binlog_row_value_options",
            crate::binlog::validate_binlog_row_value_options,
        ),
        (
            "binlog_transaction_compression",
            crate::binlog::validate_binlog_transaction_compression,
        ),
    ];
    for (name, validate) in validated {
        let canon: &'static str = *name;
        let validate: Validator = *validate;
        reg.register(SysVar::settable(
            SysVarDef::new(canon, Scope::Global, DataType::String { max_len: None })
                .with_access(Access::SuperReadWrite)
                .with_doc(
                    "Binlog format/image knob. mydb accepts only its supported value \
                     (ROW / FULL / '' / OFF) as an idempotent no-op; other values error.",
                ),
            move |_ctx| snapshot_value(canon),
            move |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                validate(&v.to_display_string())
            },
        ));
    }

    // sync_binlog — runtime-writable, real effect on the fsync cadence.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "sync_binlog",
            Scope::Global,
            DataType::UInt {
                min: 0,
                max: u64::MAX,
            },
        )
        .with_access(Access::SuperReadWrite)
        .with_doc(
            "How often the binary log is synced to disk (0 = rely on OS, N = every N \
             commit groups). Runtime-writable; env MYDB_SYNC_BINLOG is the startup default.",
        ),
        move |_ctx| snapshot_value("sync_binlog"),
        move |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let n = v
                .as_u64()
                .ok_or_else(|| crate::engine::sysvar::errors::wrong_type_for_var("sync_binlog"))?;
            crate::binlog::set_sync_binlog(n)
        },
    ));

    reg.register(SysVar::settable(
        SysVarDef::new(
            "max_binlog_size",
            Scope::Global,
            DataType::UInt {
                min: 4096,
                max: 1_073_741_824,
            },
        )
        .with_access(Access::SuperReadWrite)
        .with_doc(
            "Soft binlog rotation threshold in bytes; transactions are never split across files.",
        ),
        move |_ctx| snapshot_value("max_binlog_size"),
        move |_ctx: &mut WriteCtx<'_>, value: SysVarValue| {
            let bytes = value.as_u64().ok_or_else(|| {
                crate::engine::sysvar::errors::wrong_type_for_var("max_binlog_size")
            })?;
            crate::binlog::set_max_binlog_size(bytes)
        },
    ));

    // log_replica_updates — runtime-writable, read live on the record path.
    reg.register(SysVar::settable(
        SysVarDef::new("log_replica_updates", Scope::Global, DataType::Bool)
            .with_access(Access::SuperReadWrite)
            .with_doc(
                "Whether changes applied by the MySQL-replica applier are re-logged to this \
                 server's binary log (chained replication). Runtime-writable.",
            ),
        move |_ctx| snapshot_value("log_replica_updates"),
        move |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            crate::binlog::set_log_replica_updates(v.as_bool().unwrap_or(true))
        },
    ));

    // Semi-sync (master/source side) — real behavioural knobs wired to the
    // binlog manager. Both the 8.0.26+ `source` names and the legacy `master`
    // aliases are registered; each pair shares one setter. SuperReadWrite
    // (SYSTEM_VARIABLES_ADMIN) like any GLOBAL write.
    type BoolSetter = fn(bool) -> Result<(), QueryError>;
    let semi_sync_bools: &[(&'static str, BoolSetter)] = &[
        (
            "rpl_semi_sync_source_enabled",
            crate::binlog::set_semi_sync_source_enabled as BoolSetter,
        ),
        (
            "rpl_semi_sync_master_enabled",
            crate::binlog::set_semi_sync_source_enabled,
        ),
        (
            "rpl_semi_sync_source_wait_no_replica",
            crate::binlog::set_semi_sync_source_wait_no_replica,
        ),
        (
            "rpl_semi_sync_master_wait_no_slave",
            crate::binlog::set_semi_sync_source_wait_no_replica,
        ),
    ];
    for (name, setter) in semi_sync_bools {
        let canon: &'static str = *name;
        let setter: BoolSetter = *setter;
        reg.register(SysVar::settable(
            SysVarDef::new(canon, Scope::Global, DataType::Bool)
                .with_access(Access::SuperReadWrite)
                .with_doc("Master-side semi-sync replication toggle."),
            move |_ctx| snapshot_value(canon),
            move |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                setter(v.as_bool().unwrap_or(false))
            },
        ));
    }

    type U64Setter = fn(u64) -> Result<(), QueryError>;
    let semi_sync_uints: &[(&'static str, U64Setter)] = &[
        (
            "rpl_semi_sync_source_timeout",
            crate::binlog::set_semi_sync_source_timeout as U64Setter,
        ),
        (
            "rpl_semi_sync_master_timeout",
            crate::binlog::set_semi_sync_source_timeout,
        ),
        (
            "rpl_semi_sync_source_wait_for_replica_count",
            crate::binlog::set_semi_sync_source_wait_for_replica_count,
        ),
        (
            "rpl_semi_sync_master_wait_for_slave_count",
            crate::binlog::set_semi_sync_source_wait_for_replica_count,
        ),
    ];
    for (name, setter) in semi_sync_uints {
        let canon: &'static str = *name;
        let setter: U64Setter = *setter;
        reg.register(SysVar::settable(
            SysVarDef::new(
                canon,
                Scope::Global,
                DataType::UInt {
                    min: 0,
                    max: u64::MAX,
                },
            )
            .with_access(Access::SuperReadWrite)
            .with_doc("Master-side semi-sync replication tuning."),
            move |_ctx| snapshot_value(canon),
            move |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                let n = v
                    .as_u64()
                    .ok_or_else(|| crate::engine::sysvar::errors::wrong_type_for_var(canon))?;
                setter(n)
            },
        ));
    }

    // Receiver-side semi-sync is unavailable in the binlog client. Reject ON.
    for canon in [
        "rpl_semi_sync_replica_enabled",
        "rpl_semi_sync_slave_enabled",
    ] {
        reg.register(SysVar::settable(
            SysVarDef::new(canon, Scope::Global, DataType::Bool)
                .with_access(Access::SuperReadWrite)
                .with_doc("Replica-side semi-sync is unsupported; ON returns an explicit error."),
            move |_ctx| SysVarValue::Bool(crate::binlog::semi_sync_replica_enabled()),
            move |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                crate::binlog::set_semi_sync_replica_enabled(v.as_bool().unwrap_or(false))
            },
        ));
    }

    // sql_log_bin — Session only. SuperReadWrite (SYSTEM_VARIABLES_ADMIN).
    // mysqldump's default header is `SET @@SESSION.SQL_LOG_BIN=0`; honour
    // it so loading a dump back doesn't re-replicate every row through
    // any downstream replica.
    reg.register(SysVar::settable(
        SysVarDef::new("sql_log_bin", Scope::Session, DataType::Bool)
            .with_access(Access::SuperReadWrite)
            .with_doc(
                "Whether this session's row changes are written to the binary log. \
                 Default ON; set to OFF for bulk loads that should not propagate.",
            ),
        |ctx| match (ctx.session, ctx.scope) {
            (Some(s), crate::engine::sysvar::meta::Scope::Session) => {
                SysVarValue::Bool(s.sql_log_bin)
            }
            _ => SysVarValue::Bool(crate::engine::api::global_sql_log_bin()),
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let b = v.as_bool().unwrap_or(true);
            match ctx.scope {
                crate::engine::sysvar::meta::Scope::Session => {
                    ctx.session.sql_log_bin = b;
                }
                crate::engine::sysvar::meta::Scope::Global => {
                    crate::engine::api::set_global_sql_log_bin(b);
                }
                crate::engine::sysvar::meta::Scope::Both => unreachable!(),
            }
            Ok(())
        },
    ));
}
