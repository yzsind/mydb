//! ORM / client probe variables — `innodb_*`, `myisam_*`, `audit_*`,
//! `keyring_*`. mydb has its own storage engine so these are not really
//! configurable; we return MySQL-default values so clients accept us as
//! MySQL 8.0.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::OnceLock;

use crate::engine::sysvar::backend::SysVar;
use crate::engine::sysvar::ctx::WriteCtx;
use crate::engine::sysvar::meta::{DataType, Scope, SysVarDef};
use crate::engine::sysvar::registry::Registry;
use crate::engine::sysvar::value::SysVarValue;
use crate::types::QueryError;

static INNODB_STRICT_MODE: AtomicBool = AtomicBool::new(true);
static MAX_PREPARED_STMT_COUNT: AtomicU64 = AtomicU64::new(16_382);
static MAX_ERROR_COUNT: AtomicU64 = AtomicU64::new(64);

pub(crate) fn max_error_count() -> u64 {
    MAX_ERROR_COUNT.load(Ordering::Relaxed)
}

pub(crate) fn global_innodb_strict_mode() -> bool {
    INNODB_STRICT_MODE.load(Ordering::Relaxed)
}

pub(crate) fn max_prepared_stmt_count() -> u64 {
    MAX_PREPARED_STMT_COUNT.load(Ordering::Relaxed)
}

pub(crate) fn register(reg: &mut Registry) {
    reg.register(SysVar::session_backed(
        SysVarDef::new(
            "max_error_count",
            Scope::Both,
            DataType::Int { min: 0, max: 65535 },
        )
        .with_doc("Maximum stored conditions; warning_count includes discarded conditions."),
        |ctx| {
            SysVarValue::UInt(match (ctx.scope, ctx.session) {
                (Scope::Session, Some(session)) => session.sysvars.max_error_count,
                _ => max_error_count(),
            })
        },
        |ctx, value| {
            let count = value.as_u64().unwrap_or(64);
            if ctx.is_global() {
                MAX_ERROR_COUNT.store(count, Ordering::Relaxed);
            } else {
                ctx.session.sysvars.max_error_count = count;
            }
            Ok(())
        },
    ));
    register_innodb_stubs(reg);
    register_misc_stubs(reg);
    register_validate_password(reg);
}

/// Real `validate_password.*` system variables backing the opt-in
/// `validate_password` component (see `engine::password_policy`). Unlike the
/// old read-only stubs these actually drive password-strength enforcement
/// once the component is installed — writes take effect immediately.
fn register_validate_password(reg: &mut Registry) {
    use crate::engine::password_policy as pol;

    // validate_password.policy — LOW / MEDIUM / STRONG.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "validate_password.policy",
            Scope::Global,
            DataType::String { max_len: None },
        )
        .with_doc("Password strength policy (LOW/MEDIUM/STRONG)."),
        |_ctx| SysVarValue::String(pol::policy_str().to_string()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            pol::set_policy(pol::parse_policy(v.as_str().unwrap_or(""))?);
            Ok(())
        },
    ));

    // Numeric knobs: (name, getter, setter).
    type Getter = fn() -> u64;
    type Setter = fn(u64);
    let uint_vars: &[(&'static str, Getter, Setter)] = &[
        ("validate_password.length", pol::length, pol::set_length),
        (
            "validate_password.mixed_case_count",
            pol::mixed_case_count,
            pol::set_mixed_case_count,
        ),
        (
            "validate_password.number_count",
            pol::number_count,
            pol::set_number_count,
        ),
        (
            "validate_password.special_char_count",
            pol::special_char_count,
            pol::set_special_char_count,
        ),
    ];
    for &(name, getter, setter) in uint_vars {
        reg.register(SysVar::settable(
            SysVarDef::new(
                name,
                Scope::Global,
                DataType::UInt {
                    min: 0,
                    max: u64::MAX,
                },
            )
            .with_doc("validate_password composition minimum."),
            move |_ctx| SysVarValue::UInt(getter()),
            move |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                setter(v.as_u64().unwrap_or(0));
                Ok(())
            },
        ));
    }

    // validate_password.check_user_name — reject password == user name.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "validate_password.check_user_name",
            Scope::Global,
            DataType::Bool,
        )
        .with_doc("Reject a password equal to the account's user name."),
        |_ctx| SysVarValue::Bool(pol::check_user_name()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            pol::set_check_user_name(v.as_bool().unwrap_or(true));
            Ok(())
        },
    ));

    // validate_password.dictionary_file — accepted but no dictionary shipped.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "validate_password.dictionary_file",
            Scope::Global,
            DataType::String { max_len: None },
        )
        .with_doc("Dictionary files are unsupported; only an empty path is accepted."),
        |_ctx| SysVarValue::String(String::new()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            if !v.to_display_string().is_empty() {
                return Err(crate::error::catalog::ER_NOT_SUPPORTED_YET
                    .error("validate_password dictionary files are not supported"));
            }
            Ok(())
        },
    ));

    // authentication_policy (8.0.27+) — validated + round-trips. mydb is
    // single-factor, so the multi-factor constraint isn't enforced; setting it
    // raises warning 1287 (accept-not-enforce) rather than silently ignoring.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "authentication_policy",
            Scope::Global,
            DataType::String { max_len: None },
        )
        .with_doc(
            "Multi-factor authentication policy (validated; not enforced — mydb is single-factor).",
        ),
        |_ctx| SysVarValue::String(pol::authentication_policy()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            pol::set_authentication_policy(&v.to_display_string())?;
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
                "'authentication_policy' is accepted and validated but its \
                 multi-factor constraints are not enforced (mydb authenticates \
                 with a single factor)",
            );
            Ok(())
        },
    ));
}

fn register_innodb_stubs(reg: &mut Registry) {
    // MySQL exposes innodb_strict_mode as a dynamic SESSION/GLOBAL boolean.
    // mydb does not use InnoDB, so retain and echo the value for client
    // compatibility while warning that it has no storage-engine effect.
    reg.register(SysVar::settable(
        SysVarDef::new("innodb_strict_mode", Scope::Both, DataType::Bool)
            .with_doc("InnoDB strict-mode compatibility value (not enforced by mydb)."),
        |ctx| match (ctx.session, ctx.scope) {
            (Some(session), Scope::Session) => {
                SysVarValue::Bool(session.sysvars.innodb_strict_mode)
            }
            _ => SysVarValue::Bool(global_innodb_strict_mode()),
        },
        |ctx: &mut WriteCtx<'_>, value: SysVarValue| -> Result<(), QueryError> {
            let enabled = value.as_bool().unwrap_or(true);
            if ctx.is_global() {
                INNODB_STRICT_MODE.store(enabled, Ordering::Relaxed);
            } else {
                ctx.session.sysvars.innodb_strict_mode = enabled;
            }
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                 crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
                "'innodb_strict_mode' is accepted for compatibility but has no effect because mydb does not use InnoDB",
            );
            Ok(())
        },
    ));

    // `(name, default_value_string)` pairs for read-only innodb stubs.
    let str_stubs: &[(&'static str, &'static str)] = &[
        ("innodb_version", "8.0.0"),
        ("innodb_default_row_format", "DYNAMIC"),
        ("innodb_flush_method", "fsync"),
        ("innodb_doublewrite", "ON"),
        ("innodb_file_per_table", "ON"),
        ("innodb_adaptive_hash_index", "ON"),
        ("innodb_print_all_deadlocks", "OFF"),
        ("innodb_stats_on_metadata", "OFF"),
        ("innodb_status_output", "OFF"),
        ("innodb_status_output_locks", "OFF"),
        ("innodb_log_compressed_pages", "ON"),
        ("innodb_undo_log_truncate", "ON"),
        ("innodb_change_buffering", "all"),
        ("innodb_redo_log_archive_dirs", ""),
        ("innodb_temp_data_file_path", "ibtmp1:12M:autoextend"),
        ("innodb_undo_directory", "./"),
        ("innodb_data_home_dir", ""),
        ("innodb_data_file_path", "ibdata1:12M:autoextend"),
        ("innodb_force_recovery", "0"),
        ("innodb_force_load_corrupted", "OFF"),
        ("innodb_rollback_on_timeout", "OFF"),
        ("innodb_table_locks", "ON"),
        ("innodb_compression_failure_threshold_pct", "5"),
        ("innodb_compression_level", "6"),
        ("innodb_log_writer_threads", "ON"),
    ];
    for &(name, value) in str_stubs {
        let v = value.to_string();
        reg.register(SysVar::const_read_only(
            SysVarDef::new(name, Scope::Global, DataType::String { max_len: None })
                .with_doc("InnoDB compat stub (mydb does not use InnoDB)."),
            SysVarValue::String(v),
        ));
    }

    // Writeable u64 stubs — round-trip but no effect.
    let u64_stubs: &[(&'static str, u64)] = &[
        ("innodb_buffer_pool_size", 134_217_728),
        ("innodb_buffer_pool_instances", 1),
        ("innodb_log_buffer_size", 16 * 1024 * 1024),
        ("innodb_log_file_size", 48 * 1024 * 1024),
        ("innodb_log_files_in_group", 2),
        ("innodb_io_capacity", 200),
        ("innodb_io_capacity_max", 2000),
        ("innodb_read_io_threads", 4),
        ("innodb_write_io_threads", 4),
        ("innodb_thread_concurrency", 0),
        ("innodb_purge_threads", 4),
        ("innodb_open_files", 4000),
        ("innodb_autoinc_lock_mode", 2),
        ("innodb_max_dirty_pages_pct", 75),
        ("innodb_max_dirty_pages_pct_lwm", 10),
        ("innodb_flush_neighbors", 0),
        ("innodb_lru_scan_depth", 1024),
        // innodb_lock_wait_timeout — real sysvar in transaction.rs (M3).
        ("innodb_old_blocks_pct", 37),
        ("innodb_old_blocks_time", 1000),
        ("innodb_random_read_ahead", 0),
        ("innodb_read_ahead_threshold", 56),
        ("innodb_sort_buffer_size", 1_048_576),
        ("innodb_spin_wait_delay", 6),
        ("innodb_sync_array_size", 1),
        ("innodb_thread_sleep_delay", 10_000),
        ("innodb_concurrency_tickets", 5000),
        ("innodb_change_buffer_max_size", 25),
        ("innodb_redo_log_capacity", 100 * 1024 * 1024),
        ("innodb_flush_log_at_trx_commit", 1),
        ("innodb_page_size", 16384),
        ("innodb_undo_tablespaces", 2),
        ("innodb_max_undo_log_size", 1024 * 1024 * 1024),
        ("innodb_purge_batch_size", 300),
        ("innodb_rollback_segments", 128),
    ];
    for &(name, default) in u64_stubs {
        let cell: &'static AtomicU64 = leak_atomic_u64(name, default);
        reg.register(SysVar::settable(
            SysVarDef::new(
                name,
                Scope::Global,
                DataType::UInt {
                    min: 0,
                    max: u64::MAX,
                },
            )
            .with_doc("InnoDB compat stub (mydb does not use InnoDB)."),
            move |_ctx| SysVarValue::UInt(cell.load(Ordering::Relaxed)),
            move |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                cell.store(v.as_u64().unwrap_or(0), Ordering::Relaxed);
                Ok(())
            },
        ));
    }
}

fn register_misc_stubs(reg: &mut Registry) {
    reg.register(SysVar::settable(
        SysVarDef::new(
            "max_prepared_stmt_count",
            Scope::Global,
            DataType::UInt {
                min: 0,
                max: 1_048_576,
            },
        )
        .with_doc("Global live server-side prepared statement limit."),
        |_ctx| SysVarValue::UInt(max_prepared_stmt_count()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            MAX_PREPARED_STMT_COUNT.store(v.as_u64().unwrap_or(0), Ordering::Relaxed);
            Ok(())
        },
    ));
    for (name, increment) in [
        ("auto_increment_increment", true),
        ("auto_increment_offset", false),
    ] {
        reg.register(SysVar::session_backed(
            SysVarDef::new(name, Scope::Both, DataType::UInt { min: 1, max: 65535 })
                .with_doc("Step or offset used by AUTO_INCREMENT value allocation."),
            move |ctx| {
                SysVarValue::UInt(
                    ctx.session
                        .map(|s| {
                            if increment {
                                s.sysvars.auto_increment_increment
                            } else {
                                s.sysvars.auto_increment_offset
                            }
                        })
                        .unwrap_or(1),
                )
            },
            move |ctx, value| {
                let value = value.as_u64().unwrap_or(1);
                if increment {
                    ctx.session.sysvars.auto_increment_increment = value;
                } else {
                    ctx.session.sysvars.auto_increment_offset = value;
                }
                Ok(())
            },
        ));
    }
    // Query cache is unsupported; reject configuration instead of a no-op.
    for (name, default) in [("query_cache_size", 0_u64)] {
        reg.register(SysVar::const_read_only(
            SysVarDef::new(
                name,
                Scope::Both,
                DataType::UInt {
                    min: 0,
                    max: u64::MAX,
                },
            )
            .without_persistence()
            .with_doc("Read-only MySQL client capability value."),
            SysVarValue::UInt(default),
        ));
    }
    reg.register(SysVar::const_read_only(
        SysVarDef::new(
            "query_cache_type",
            Scope::Both,
            DataType::String { max_len: None },
        )
        .with_doc("Query cache is not implemented; always OFF."),
        SysVarValue::String("OFF".to_string()),
    ));

    // mydb does not implement encrypted tables. Expose MySQL's default as
    // disabled for compatibility probes, but keep the variable read-only so
    // enabling an unsupported feature cannot succeed as a silent no-op.
    reg.register(SysVar::const_read_only(
        SysVarDef::new("default_table_encryption", Scope::Both, DataType::Bool)
            .with_doc("Table encryption is unsupported; always OFF."),
        SysVarValue::Bool(false),
    ));

    // Read-only "feature" strings probed by mysqldump/ORMs.
    let ro_str: &[(&'static str, &'static str)] = &[
        ("default_authentication_plugin", "caching_sha2_password"),
        // password_history / password_reuse_interval / password_require_current
        // are registered for real in `replication::register` — they drive the
        // global password-reuse and require-current policy fallback.
        ("character_sets_dir", "/usr/share/mysql/charsets/"),
        ("ft_boolean_syntax", "+ -><()~*:\"\"&|"),
        ("ft_min_word_len", "4"),
        ("ft_max_word_len", "84"),
        ("ft_query_expansion_limit", "20"),
        ("ft_stopword_file", "(built-in)"),
        ("innodb_buffer_pool_chunk_size", "134217728"),
    ];
    for &(name, value) in ro_str {
        let v = value.to_string();
        reg.register(SysVar::const_read_only(
            SysVarDef::new(name, Scope::Global, DataType::String { max_len: None })
                .with_doc("MySQL-compat stub."),
            SysVarValue::String(v),
        ));
    }

    reg.register(SysVar::settable(
        SysVarDef::new("require_secure_transport", Scope::Global, DataType::Bool),
        |_| SysVarValue::Bool(require_secure_transport()),
        |_, v| {
            REQUIRE_SECURE_TRANSPORT.store(
                v.as_bool().unwrap_or(false),
                std::sync::atomic::Ordering::Release,
            );
            Ok(())
        },
    ));

    let bool_stubs: &[(&'static str, bool)] = &[
        ("automatic_sp_privileges_compat", true),
        ("flush", false),
        ("local_infile", true),
        // activate_all_roles_on_login is registered for real in
        // `replication::register` — it drives login role activation.
        ("avoid_temporal_upgrade", false),
        ("show_create_table_skip_secondary_engine", false),
        ("show_create_table_verbosity", false),
        ("show_old_temporals", false),
        ("show_compatibility_56", false),
        ("end_markers_in_json", false),
        ("explicit_defaults_for_timestamp_compat", true),
        ("keep_files_on_create", false),
        ("large_files_support", true),
        // partial_revokes is registered for real in `replication::register`
        // — it backs the L9 RestrictionEntry flow. The stub here used to
        // expose only a static ORM-friendly value; remove from stubs to
        // avoid duplicate registration.
        ("relay_log_purge", true),
        ("relay_log_recovery", false),
        ("schema_definition_cache", false),
        ("ssl_fips_mode", false),
        ("sync_master_info", false),
        ("sync_relay_log", false),
        ("sync_relay_log_info", false),
        ("tls_ciphersuites_passes", false),
    ];
    for &(name, default) in bool_stubs {
        let cell: &'static std::sync::atomic::AtomicBool = leak_atomic_bool(name, default);
        reg.register(SysVar::settable(
            SysVarDef::new(name, Scope::Global, DataType::Bool).with_doc("MySQL-compat stub."),
            move |_ctx| SysVarValue::Bool(cell.load(std::sync::atomic::Ordering::Relaxed)),
            move |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                cell.store(
                    v.as_bool().unwrap_or(false),
                    std::sync::atomic::Ordering::Relaxed,
                );
                Ok(())
            },
        ));
    }

    let i64_stubs: &[(&'static str, i64)] = &[
        ("delayed_insert_limit", 100),
        ("delayed_insert_timeout", 300),
        ("delayed_queue_size", 1000),
        ("flush_time", 0),
        ("max_connect_errors", 100),
        ("max_delayed_threads", 20),
        ("max_relay_log_size", 0),
        // max_sp_recursion_depth is a real settable sysvar (see query.rs),
        // wired to stored-procedure recursion enforcement.
        ("max_tmp_tables", 32),
        ("max_write_lock_count", i64::MAX),
        ("metadata_locks_cache_size", 1024),
        ("metadata_locks_hash_instances", 8),
        ("preload_buffer_size", 32768),
        ("range_alloc_block_size_compat", 4096),
        ("rpl_read_size", 8192),
        ("rpl_stop_replica_timeout", 31536000),
        ("rpl_stop_slave_timeout", 31536000),
        ("slow_launch_time", 2),
        ("ssl_session_cache_timeout", 300),
        ("stored_program_cache", 256),
        ("stored_program_definition_cache", 256),
        ("table_open_cache_instances", 16),
        ("temptable_max_mmap", 1_073_741_824),
        ("temptable_max_ram", 1_073_741_824),
        ("temptable_use_mmap", 1),
        ("thread_pool_size", 16),
        ("thread_pool_oversubscribe", 3),
        ("thread_pool_high_priority_connection", 0),
        ("thread_pool_max_unused_threads", 0),
        ("thread_pool_stall_limit", 6),
        ("thread_pool_algorithm", 0),
        ("tls_session_cache_timeout", 300),
        ("updatable_views_with_limit", 1),
    ];
    for &(name, default) in i64_stubs {
        let cell: &'static AtomicI64 = leak_atomic_i64(name, default);
        reg.register(SysVar::settable(
            SysVarDef::new(
                name,
                Scope::Global,
                DataType::Int {
                    min: i64::MIN,
                    max: i64::MAX,
                },
            )
            .with_doc("MySQL-compat stub."),
            move |_ctx| SysVarValue::Int(cell.load(Ordering::Relaxed)),
            move |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                cell.store(v.as_i64().unwrap_or(0), Ordering::Relaxed);
                Ok(())
            },
        ));
    }

    // Free-form string stubs that round-trip but have no effect.
    let writable_str_stubs: &[(&'static str, &'static str)] = &[
        ("character_sets_dir_compat", "/usr/share/mysql/charsets/"),
        ("plugin_dir", "/usr/lib/mysql/plugin/"),
        ("ssl_ca", ""),
        ("ssl_cert", ""),
        ("ssl_key", ""),
        ("ssl_capath", ""),
        ("ssl_cipher", ""),
        ("ssl_crl", ""),
        ("ssl_crlpath", ""),
        ("tls_version", "TLSv1.2,TLSv1.3"),
        ("tls_ciphersuites", ""),
        ("admin_tls_version", "TLSv1.2,TLSv1.3"),
        ("admin_ssl_ca", ""),
        ("admin_ssl_cert", ""),
        ("admin_ssl_key", ""),
        ("default_collation_for_utf8mb4", "utf8mb4_0900_ai_ci"),
        ("relay_log", ""),
        ("relay_log_index", ""),
        ("relay_log_basename", ""),
        ("relay_log_info_file", "relay-log.info"),
        ("relay_log_info_repository", "TABLE"),
        ("master_info_repository", "TABLE"),
        ("source_verify_checksum", "OFF"),
        ("replica_load_tmpdir", "/tmp"),
        ("slave_load_tmpdir", "/tmp"),
        ("replica_net_timeout", "60"),
        ("slave_net_timeout", "60"),
        ("report_host", ""),
        ("report_password", ""),
        ("report_port", "3306"),
        ("report_user", ""),
        // rpl_semi_sync_* moved to real, behaviour-wired sysvars in
        // engine/sysvar/builtin/binlog.rs (phase 3 semi-sync).
        ("admin_address", ""),
        ("admin_port", "0"),
        ("bind_address", "*"),
        ("mysqlx_bind_address", "*"),
        ("mysqlx_port", "33060"),
    ];
    for &(name, default) in writable_str_stubs {
        let n: &'static str = name;
        let d: &'static str = default;
        let cell: &'static std::sync::RwLock<String> = leak_string(n, d);
        reg.register(SysVar::settable(
            SysVarDef::new(n, Scope::Global, DataType::String { max_len: None })
                .with_doc("MySQL-compat stub."),
            move |_ctx| {
                let s = cell.read().map(|s| s.clone()).unwrap_or_default();
                SysVarValue::String(s)
            },
            move |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                if let Ok(mut g) = cell.write() {
                    *g = v.to_display_string();
                }
                Ok(())
            },
        ));
    }
}

// ---------------------------------------------------------------------------
// Helpers for leaking atomic cells / RwLocks keyed by variable name.
// ---------------------------------------------------------------------------

fn leak_atomic_u64(name: &'static str, default: u64) -> &'static AtomicU64 {
    use std::collections::HashMap;
    use std::sync::Mutex;
    static CELLS: OnceLock<Mutex<HashMap<&'static str, &'static AtomicU64>>> = OnceLock::new();
    let cells = CELLS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut g = cells.lock().expect("orm_stubs u64 cells");
    if let Some(c) = g.get(name) {
        return c;
    }
    let leaked: &'static AtomicU64 = Box::leak(Box::new(AtomicU64::new(default)));
    g.insert(name, leaked);
    leaked
}

fn leak_atomic_i64(name: &'static str, default: i64) -> &'static AtomicI64 {
    use std::collections::HashMap;
    use std::sync::Mutex;
    static CELLS: OnceLock<Mutex<HashMap<&'static str, &'static AtomicI64>>> = OnceLock::new();
    let cells = CELLS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut g = cells.lock().expect("orm_stubs i64 cells");
    if let Some(c) = g.get(name) {
        return c;
    }
    let leaked: &'static AtomicI64 = Box::leak(Box::new(AtomicI64::new(default)));
    g.insert(name, leaked);
    leaked
}

fn leak_atomic_bool(name: &'static str, default: bool) -> &'static std::sync::atomic::AtomicBool {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::Mutex;
    static CELLS: OnceLock<Mutex<HashMap<&'static str, &'static AtomicBool>>> = OnceLock::new();
    let cells = CELLS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut g = cells.lock().expect("orm_stubs bool cells");
    if let Some(c) = g.get(name) {
        return c;
    }
    let leaked: &'static AtomicBool = Box::leak(Box::new(AtomicBool::new(default)));
    g.insert(name, leaked);
    leaked
}

fn leak_string(name: &'static str, default: &'static str) -> &'static std::sync::RwLock<String> {
    use std::collections::HashMap;
    use std::sync::{Mutex, RwLock};
    static CELLS: OnceLock<Mutex<HashMap<&'static str, &'static RwLock<String>>>> = OnceLock::new();
    let cells = CELLS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut g = cells.lock().expect("orm_stubs string cells");
    if let Some(c) = g.get(name) {
        return c;
    }
    let leaked: &'static RwLock<String> = Box::leak(Box::new(RwLock::new(default.to_string())));
    g.insert(name, leaked);
    leaked
}

static REQUIRE_SECURE_TRANSPORT: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
pub(crate) fn require_secure_transport() -> bool {
    REQUIRE_SECURE_TRANSPORT.load(std::sync::atomic::Ordering::Acquire)
}
