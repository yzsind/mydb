//! Replication / read-only variables. `read_only` / `super_read_only` /
//! `offline_mode` actually gate writes through the shared
//! `enforce_write_access` policy.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::engine::sysvar::backend::SysVar;
use crate::engine::sysvar::ctx::WriteCtx;
use crate::engine::sysvar::meta::{Access, DataType, Scope, SysVarDef};
use crate::engine::sysvar::registry::Registry;
use crate::engine::sysvar::value::SysVarValue;
use crate::types::QueryError;

// ---------------------------------------------------------------------------
// Process-wide state.
// ---------------------------------------------------------------------------

static READ_ONLY: AtomicBool = AtomicBool::new(false);
static SUPER_READ_ONLY: AtomicBool = AtomicBool::new(false);
static OFFLINE_MODE: AtomicBool = AtomicBool::new(false);
static LOG_BIN_TRUST_FUNCTION_CREATORS: AtomicBool = AtomicBool::new(false);
static SLAVE_PARALLEL_TYPE: AtomicU64 = AtomicU64::new(0);
static SLAVE_PRESERVE_COMMIT_ORDER: AtomicBool = AtomicBool::new(true);
static SLAVE_CHECKPOINT_PERIOD: AtomicU64 = AtomicU64::new(300);
static SLAVE_CHECKPOINT_GROUP: AtomicU64 = AtomicU64::new(512);

/// Returns true if the engine is in read-only mode for non-super users.
pub fn read_only() -> bool {
    READ_ONLY.load(Ordering::Relaxed) || SUPER_READ_ONLY.load(Ordering::Relaxed)
}

/// Returns true if even super users are blocked from writing.
pub fn super_read_only() -> bool {
    SUPER_READ_ONLY.load(Ordering::Relaxed)
}

/// Returns true if the server is in offline-mode (only super users
/// admitted).
pub fn offline_mode() -> bool {
    OFFLINE_MODE.load(Ordering::Relaxed)
}

pub(crate) fn log_bin_trust_function_creators() -> bool {
    LOG_BIN_TRUST_FUNCTION_CREATORS.load(Ordering::Relaxed)
}

pub(crate) fn register(reg: &mut Registry) {
    reg.register(SysVar::settable(
        SysVarDef::new("read_only", Scope::Global, DataType::Bool)
            .with_access(Access::SuperReadWrite)
            .with_doc("Block writes from non-super users when ON."),
        |ctx| {
            SysVarValue::Bool(
                ctx.db.is_some_and(|db| db.is_snapshot_read_only())
                    || READ_ONLY.load(Ordering::Relaxed),
            )
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let on = v.as_bool().unwrap_or(false);
            if ctx.db.is_some_and(|db| db.is_snapshot_read_only()) {
                return if on {
                    Ok(())
                } else {
                    Err(crate::storage::image_io::read_only_error())
                };
            }
            READ_ONLY.store(on, Ordering::Relaxed);
            if !on {
                // Turning off read_only also turns off super_read_only
                // (MySQL semantics).
                SUPER_READ_ONLY.store(false, Ordering::Relaxed);
            }
            Ok(())
        },
    ));

    reg.register(SysVar::settable(
        SysVarDef::new("super_read_only", Scope::Global, DataType::Bool)
            .with_access(Access::SuperReadWrite)
            .with_doc("Block writes from all users (including super) when ON."),
        |ctx| {
            SysVarValue::Bool(
                ctx.db.is_some_and(|db| db.is_snapshot_read_only())
                    || SUPER_READ_ONLY.load(Ordering::Relaxed),
            )
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let on = v.as_bool().unwrap_or(false);
            if ctx.db.is_some_and(|db| db.is_snapshot_read_only()) {
                return if on {
                    Ok(())
                } else {
                    Err(crate::storage::image_io::read_only_error())
                };
            }
            SUPER_READ_ONLY.store(on, Ordering::Relaxed);
            if on {
                // Turning on super_read_only implies read_only.
                READ_ONLY.store(true, Ordering::Relaxed);
            }
            Ok(())
        },
    ));

    // L9 partial_revokes — toggles MySQL 8.0's partial revoke deny-list
    // model. When ON, `REVOKE priv ON db.* FROM u` against a holder of the
    // global GRANT writes a `RestrictionEntry` instead of rejecting.
    reg.register(SysVar::settable(
        SysVarDef::new("partial_revokes", Scope::Global, DataType::Bool)
            .with_access(Access::SuperReadWrite)
            .with_doc("Enable partial revoke (deny-list) model per MySQL 8.0.16+."),
        |_ctx| SysVarValue::Bool(crate::engine::auth::partial_revokes_enabled()),
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let enabled = v.as_bool().unwrap_or(false);
            if !enabled
                && ctx.db.is_some_and(|db| {
                    db.with_auth_catalog(|auth| !auth.restrictions.is_empty())
                        .unwrap_or(false)
                })
            {
                return Err(crate::mysql::errors::error(
                    crate::error::catalog::ER_PARTIAL_REVOKES_EXIST,
                    "The system variable partial_revokes cannot be disabled because privilege restrictions exist",
                ));
            }
            crate::engine::auth::set_partial_revokes(enabled);
            Ok(())
        },
    ));

    // activate_all_roles_on_login (MySQL 8.0) — when ON, every role granted
    // to an account (plus mandatory roles) is activated at login; when OFF
    // only default roles are. Drives `AuthCatalog::login_active_roles`.
    reg.register(SysVar::settable(
        SysVarDef::new("activate_all_roles_on_login", Scope::Global, DataType::Bool)
            .with_access(Access::SuperReadWrite)
            .with_doc("Activate all granted (and mandatory) roles at login when ON."),
        |_ctx| SysVarValue::Bool(crate::engine::auth::activate_all_roles_on_login()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            crate::engine::auth::set_activate_all_roles_on_login(v.as_bool().unwrap_or(false));
            Ok(())
        },
    ));

    // mandatory_roles (MySQL 8.0) — roles implicitly granted to every account.
    // Stored as the raw sysvar string; parsed on demand into role keys.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mandatory_roles",
            Scope::Global,
            DataType::String { max_len: None },
        )
        .with_access(Access::SuperReadWrite)
        .with_doc("Roles implicitly granted to every account (MySQL 8.0)."),
        |_ctx| SysVarValue::String(crate::engine::auth::mandatory_roles_raw()),
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let raw = v.to_display_string();
            if let Some(db) = ctx.db {
                let roles = crate::engine::auth::parse_mandatory_roles(&raw);
                let contains_system_user = db.with_auth_catalog(|auth| {
                    roles.iter().any(|role| auth.account_is_system_user(role))
                })?;
                if contains_system_user {
                    return Err(crate::mysql::errors::error(
                        crate::error::catalog::ER_DA_AUTH_ID_WITH_SYSTEM_USER_PRIV_IN_MANDATORY_ROLES,
                        "An authorization ID with SYSTEM_USER privilege cannot be listed in mandatory_roles",
                    ));
                }
            }
            crate::engine::auth::set_mandatory_roles_raw(&raw);
            Ok(())
        },
    ));

    // default_password_lifetime (MySQL 8.0) — global default password
    // lifetime in days for accounts that don't set their own; 0 = never
    // expires. Wired into login-time expiry via `password_lifetime_expired`.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "default_password_lifetime",
            Scope::Global,
            DataType::UInt {
                min: 0,
                max: u16::MAX as u64,
            },
        )
        .with_access(Access::SuperReadWrite)
        .with_doc("Global default password lifetime in days (0 = never expires)."),
        |_ctx| SysVarValue::UInt(crate::engine::auth::default_password_lifetime() as u64),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            crate::engine::auth::set_default_password_lifetime(
                v.as_u64().unwrap_or(0).min(u16::MAX as u64) as u16,
            );
            Ok(())
        },
    ));

    // password_history (MySQL 8.0) — global default count of previous passwords
    // an account may not reuse. Accounts fall back to this when they use
    // `PASSWORD HISTORY DEFAULT`. Wired into `UserAccount::password_reused`.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "password_history",
            Scope::Global,
            DataType::UInt {
                min: 0,
                max: u16::MAX as u64,
            },
        )
        .with_access(Access::SuperReadWrite)
        .with_doc("Global default number of past passwords blocked from reuse (0 = no limit)."),
        |_ctx| SysVarValue::UInt(crate::engine::auth::global_password_history() as u64),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            crate::engine::auth::set_global_password_history(
                v.as_u64().unwrap_or(0).min(u16::MAX as u64) as u16,
            );
            Ok(())
        },
    ));

    // password_reuse_interval (MySQL 8.0) — global default number of days before
    // a password may be reused. Fallback for `PASSWORD REUSE INTERVAL DEFAULT`.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "password_reuse_interval",
            Scope::Global,
            DataType::UInt {
                min: 0,
                max: u16::MAX as u64,
            },
        )
        .with_access(Access::SuperReadWrite)
        .with_doc("Global default days before a password may be reused (0 = no limit)."),
        |_ctx| SysVarValue::UInt(crate::engine::auth::global_password_reuse_interval() as u64),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            crate::engine::auth::set_global_password_reuse_interval(
                v.as_u64().unwrap_or(0).min(u16::MAX as u64) as u16,
            );
            Ok(())
        },
    ));

    // password_require_current (MySQL 8.0) — global default for whether a
    // password change must verify the current password via the REPLACE clause.
    // Fallback for `PASSWORD REQUIRE CURRENT DEFAULT`.
    reg.register(SysVar::settable(
        SysVarDef::new("password_require_current", Scope::Global, DataType::Bool)
            .with_access(Access::SuperReadWrite)
            .with_doc("Global default requiring the current password on change (OFF by default)."),
        |_ctx| SysVarValue::Bool(crate::engine::auth::global_password_require_current()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            crate::engine::auth::set_global_password_require_current(v.as_bool().unwrap_or(false));
            Ok(())
        },
    ));

    reg.register(SysVar::settable(
        SysVarDef::new("offline_mode", Scope::Global, DataType::Bool)
            .with_access(Access::SuperReadWrite)
            .with_doc("Only super users admitted when ON."),
        |_ctx| SysVarValue::Bool(OFFLINE_MODE.load(Ordering::Relaxed)),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            OFFLINE_MODE.store(v.as_bool().unwrap_or(false), Ordering::Relaxed);
            Ok(())
        },
    ));

    // mydb_replica_read_only — a mydb-specific flag wired into the
    // replication module.
    reg.register(SysVar::settable(
        SysVarDef::new("mydb_replica_read_only", Scope::Global, DataType::Bool)
            .with_doc("Reject local DDL/DML while acting as a MySQL replica."),
        |_ctx| SysVarValue::Bool(crate::replication::replica_read_only()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            crate::replication::set_replica_read_only(v.as_bool().unwrap_or(false));
            Ok(())
        },
    ));

    // slave_exec_mode / replica_exec_mode (MySQL) — STRICT (default) stops
    // the applier on a WRITE_ROWS key conflict or an absent UPDATE/DELETE
    // before-image (surfacing divergence); IDEMPOTENT skips those. Real
    // effect: wired into the row-apply path via
    // `crate::replication::apply_is_idempotent`.
    for name in &["slave_exec_mode", "replica_exec_mode"] {
        let n: &'static str = *name;
        reg.register(SysVar::settable(
            SysVarDef::new(
                n,
                Scope::Global,
                DataType::Enum {
                    values: &["STRICT", "IDEMPOTENT"],
                },
            )
            .with_access(Access::SuperReadWrite)
            .with_doc(
                "Replica row-event execution mode: STRICT stops on conflict, IDEMPOTENT skips.",
            ),
            |_ctx| SysVarValue::String(crate::replication::slave_exec_mode().to_string()),
            |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                let text = v.to_display_string().to_ascii_uppercase();
                crate::replication::set_slave_exec_mode(text == "IDEMPOTENT" || text == "1");
                Ok(())
            },
        ));
    }

    // Replication-related read-only knobs that monitoring tools probe.
    for name in &[
        "slave_skip_errors",
        "replica_skip_errors",
        "skip_name_resolve",
        "skip_show_database",
        "skip_external_locking",
        "skip_networking",
    ] {
        let n: &'static str = *name;
        reg.register(SysVar::const_read_only(
            SysVarDef::new(n, Scope::Global, DataType::Bool)
                .without_persistence()
                .with_doc("Unsupported replication / network-skip setting; read-only."),
            SysVarValue::Bool(false),
        ));
    }

    reg.register(SysVar::settable(
        SysVarDef::new(
            "log_bin_trust_function_creators",
            Scope::Global,
            DataType::Bool,
        )
        .with_doc(
            "Allow non-SUPER users to create stored functions while binary logging is enabled.",
        ),
        |_ctx| SysVarValue::Bool(LOG_BIN_TRUST_FUNCTION_CREATORS.load(Ordering::Relaxed)),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            LOG_BIN_TRUST_FUNCTION_CREATORS.store(v.as_bool().unwrap_or(false), Ordering::Relaxed);
            Ok(())
        },
    ));

    // slave / replica counter knobs — advisory.
    for name in &[
        "slave_parallel_workers",
        "replica_parallel_workers",
        "slave_pending_jobs_size_max",
        "replica_pending_jobs_size_max",
        "slave_max_allowed_packet",
        "replica_max_allowed_packet",
    ] {
        let n: &'static str = *name;
        reg.register(SysVar::const_read_only(
            SysVarDef::new(
                n,
                Scope::Global,
                DataType::UInt {
                    min: 0,
                    max: u64::MAX,
                },
            )
            .without_persistence()
            .with_doc("Replica worker tuning is unsupported; read-only."),
            SysVarValue::UInt(0),
        ));
    }

    // Accept and expose MySQL's MTS configuration knobs so standard replica
    // configuration can be loaded. These are configuration metadata only:
    // the current applier remains single-threaded.
    for name in &["slave_parallel_type", "replica_parallel_type"] {
        let n: &'static str = *name;
        reg.register(SysVar::settable(
            SysVarDef::new(
                n,
                Scope::Global,
                DataType::Enum {
                    values: &["LOGICAL_CLOCK", "DATABASE"],
                },
            )
            .without_persistence()
            .with_doc("Replica scheduling mode; accepted for configuration compatibility."),
            |_ctx| {
                SysVarValue::String(if SLAVE_PARALLEL_TYPE.load(Ordering::Relaxed) == 0 {
                    "LOGICAL_CLOCK".to_string()
                } else {
                    "DATABASE".to_string()
                })
            },
            |_ctx: &mut WriteCtx<'_>, value: SysVarValue| -> Result<(), QueryError> {
                let mode = match value {
                    SysVarValue::String(mode) => mode,
                    other => other.to_display_string(),
                };
                let encoded = match mode.to_ascii_uppercase().as_str() {
                    "LOGICAL_CLOCK" => 0,
                    "DATABASE" => 1,
                    _ => return Err(crate::engine::sysvar::errors::wrong_value_for_var(n, &mode)),
                };
                SLAVE_PARALLEL_TYPE.store(encoded, Ordering::Relaxed);
                Ok(())
            },
        ));
    }
    for name in &[
        "slave_preserve_commit_order",
        "replica_preserve_commit_order",
    ] {
        let n: &'static str = *name;
        reg.register(SysVar::settable(
            SysVarDef::new(n, Scope::Global, DataType::Bool)
                .without_persistence()
                .with_doc(
                    "Replica commit-order setting; accepted for configuration compatibility.",
                ),
            |_ctx| SysVarValue::Bool(SLAVE_PRESERVE_COMMIT_ORDER.load(Ordering::Relaxed)),
            |_ctx: &mut WriteCtx<'_>, value: SysVarValue| -> Result<(), QueryError> {
                SLAVE_PRESERVE_COMMIT_ORDER
                    .store(value.as_bool().unwrap_or(false), Ordering::Relaxed);
                Ok(())
            },
        ));
    }
    for name in &[
        "slave_checkpoint_period",
        "replica_checkpoint_period",
        "slave_checkpoint_group",
        "replica_checkpoint_group",
    ] {
        let n: &'static str = *name;
        let is_group = n.ends_with("checkpoint_group");
        reg.register(SysVar::settable(
            SysVarDef::new(
                n,
                Scope::Global,
                DataType::UInt {
                    min: 1,
                    max: u64::MAX,
                },
            )
            .without_persistence()
            .with_doc("Replica checkpoint setting; accepted for configuration compatibility."),
            move |_ctx| {
                SysVarValue::UInt(if is_group {
                    SLAVE_CHECKPOINT_GROUP.load(Ordering::Relaxed)
                } else {
                    SLAVE_CHECKPOINT_PERIOD.load(Ordering::Relaxed)
                })
            },
            move |_ctx: &mut WriteCtx<'_>, value: SysVarValue| -> Result<(), QueryError> {
                let value = value.as_u64().unwrap_or(1);
                if is_group {
                    SLAVE_CHECKPOINT_GROUP.store(value, Ordering::Relaxed);
                } else {
                    SLAVE_CHECKPOINT_PERIOD.store(value, Ordering::Relaxed);
                }
                Ok(())
            },
        ));
    }
}
