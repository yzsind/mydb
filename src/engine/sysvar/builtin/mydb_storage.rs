//! Storage-engine knobs prefixed `mydb_*`. Delegated to `Database::set_storage_variable`
//! for writes and per-getter functions for reads.

use crate::engine::sysvar::backend::SysVar;
use crate::engine::sysvar::ctx::WriteCtx;
use crate::engine::sysvar::meta::{Access, DataType, Scope, SysVarDef};
use crate::engine::sysvar::registry::Registry;
use crate::engine::sysvar::value::SysVarValue;
use crate::types::QueryError;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Once, OnceLock, RwLock};

static GLOBAL_REDO_ENABLED: AtomicBool = AtomicBool::new(true);
static REDO_ENV_INIT: Once = Once::new();
static INNODB_FT_AUX_TABLE: OnceLock<RwLock<String>> = OnceLock::new();

fn innodb_ft_aux_table_cell() -> &'static RwLock<String> {
    INNODB_FT_AUX_TABLE.get_or_init(|| RwLock::new(String::new()))
}

pub fn innodb_ft_aux_table() -> String {
    innodb_ft_aux_table_cell()
        .read()
        .map(|value| value.clone())
        .unwrap_or_default()
}

fn init_redo_from_env() {
    REDO_ENV_INIT.call_once(|| {
        let enabled = crate::util::env::var("MYDB_REDO_ENABLED")
            .ok()
            .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
                "1" | "on" | "true" | "yes" => Some(true),
                "0" | "off" | "false" | "no" => Some(false),
                _ => None,
            })
            .unwrap_or(true);
        GLOBAL_REDO_ENABLED.store(enabled, Ordering::Release);
    });
}

pub fn global_redo_enabled() -> bool {
    init_redo_from_env();
    GLOBAL_REDO_ENABLED.load(Ordering::Acquire)
}

pub fn set_global_redo_enabled(enabled: bool) {
    init_redo_from_env();
    GLOBAL_REDO_ENABLED.store(enabled, Ordering::Release);
}

pub(crate) fn register(reg: &mut Registry) {
    reg.register(SysVar::settable(
        SysVarDef::new("mydb_ai_enabled", Scope::Global, DataType::Bool)
            .with_doc("Enable external AI calls on this node; requires AI_ADMIN."),
        |_| SysVarValue::Bool(crate::engine::ai::enabled()),
        |ctx: &mut WriteCtx<'_>, value: SysVarValue| {
            let db = ctx.db.ok_or_else(|| {
                crate::engine::ai::error("ER_AI_DISABLED", "AI requires a storage database")
            })?;
            crate::engine::ai::runtime::authorize(
                db,
                &ctx.session.authorization_context(),
                "AI_ADMIN",
            )?;
            crate::engine::ai::set_enabled(value.as_bool().unwrap_or(false))
        },
    ));

    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_parallel_degree",
            Scope::Session,
            DataType::UInt { min: 1, max: 256 },
        )
        .without_persistence()
        .with_doc("Maximum executor workers available to this session."),
        |ctx| {
            SysVarValue::UInt(
                ctx.session
                    .map(|session| session.sysvars.mydb_parallel_degree)
                    .unwrap_or_else(|| u64::from(crate::planner::default_parallel_degree())),
            )
        },
        |ctx: &mut WriteCtx<'_>, value: SysVarValue| -> Result<(), QueryError> {
            ctx.session.sysvars.mydb_parallel_degree = value.as_u64().unwrap_or(1);
            Ok(())
        },
    ));

    reg.register(SysVar::settable(
        SysVarDef::new(
            "innodb_ft_aux_table",
            Scope::Global,
            DataType::String { max_len: Some(512) },
        )
        .with_access(Access::SuperReadWrite)
        .with_doc("Select db/table for INFORMATION_SCHEMA INNODB_FT_* diagnostics."),
        |_ctx| SysVarValue::String(innodb_ft_aux_table()),
        |_ctx: &mut WriteCtx<'_>, value: SysVarValue| -> Result<(), QueryError> {
            let value = match value {
                SysVarValue::String(value) => value,
                other => other.to_display_string(),
            };
            let canonical = value.trim().replace('.', "/");
            if !canonical.is_empty()
                && (canonical.split('/').count() != 2
                    || canonical.split('/').any(|part| part.trim().is_empty()))
            {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                    "Variable 'innodb_ft_aux_table' must be empty or use db/table",
                ));
            }
            *innodb_ft_aux_table_cell().write().map_err(|_| {
                crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "innodb_ft_aux_table lock poisoned",
                )
            })? = canonical;
            Ok(())
        },
    ));

    reg.register(SysVar::settable(
        SysVarDef::new("mydb_redo_enabled", Scope::Both, DataType::Bool)
            .with_access(Access::SuperReadWrite)
            .with_doc(
                "Enable full data redo. OFF uses crash-safe reduced redo for append-only bulk writes; transaction, undo, and in-place recovery WAL remains enabled.",
            ),
        |ctx| match (ctx.session, ctx.scope) {
            (Some(session), Scope::Session) => SysVarValue::Bool(session.redo_enabled),
            _ => SysVarValue::Bool(global_redo_enabled()),
        },
        |ctx: &mut WriteCtx<'_>, value: SysVarValue| -> Result<(), QueryError> {
            let enabled = value.as_bool().unwrap_or(true);
            match ctx.scope {
                Scope::Session => {
                    if ctx.session.in_transaction() {
                        return Err(crate::storage::error(
                            crate::error::catalog::ER_CANT_CHANGE_TX_CHARACTERISTICS,
                            "Transaction characteristics can't be changed while a transaction is in progress",
                        ));
                    }
                    if !enabled && crate::repl::physical_replication_active() {
                        return Err(crate::storage::error(
                            crate::error::catalog::ER_INNODB_REDO_DISABLED,
                            "mydb_redo_enabled cannot be disabled while physical replication is active",
                        ));
                    }
                    ctx.session.redo_enabled = enabled;
                }
                Scope::Global => {
                    if !enabled && crate::repl::physical_replication_active() {
                        return Err(crate::storage::error(
                            crate::error::catalog::ER_INNODB_REDO_DISABLED,
                            "mydb_redo_enabled cannot be disabled while physical replication is active",
                        ));
                    }
                    set_global_redo_enabled(enabled)
                }
                Scope::Both => unreachable!(),
            }
            Ok(())
        },
    ));

    // The mydb storage variables are read via Database methods and written
    // via `Database::set_storage_variable(name, u64)`.
    let names: &[(&'static str, fn(&crate::storage::Database) -> u64, &'static str)] = &[
        (
            "mydb_buffer_pool_pages",
            |db| db.buffer_pool_pages(),
            "Buffer pool page count.",
        ),
        (
            "mydb_undo_retention_ms",
            |db| db.undo_retention_ms(),
            "Undo retention window (ms).",
        ),
        (
            "mydb_undo_segment_bytes",
            |db| db.undo_segment_bytes(),
            "Bytes per undo segment.",
        ),
        (
            "mydb_undo_buffer_bytes",
            |db| db.undo_buffer_bytes(),
            "Undo write-buffer threshold; 0 = write-through.",
        ),
        (
            "mydb_undo_purge_interval_ms",
            |db| db.undo_purge_interval_ms(),
            "Interval between undo purge sweeps (ms).",
        ),
        (
            "mydb_undo_purge_batch_records",
            |db| db.undo_purge_batch_records(),
            "Undo records purged per sweep.",
        ),
        (
            "mydb_undo_tablespace_max_bytes",
            |db| db.undo_tablespace_max_bytes(),
            "Maximum undo tablespace size.",
        ),
        (
            "mydb_undo_mem_cache_limit",
            |db| db.undo_mem_cache_limit_bytes(),
            "Resident undo payload cache cap in bytes (0 = unbounded; evicted before-images page in from disk).",
        ),
        (
            "mydb_undo_txn_max_bytes",
            |db| db.undo_txn_max_bytes(),
            "Per-transaction undo byte cap (0 = unlimited); a runaway txn is rejected on its own growth.",
        ),
        (
            "mydb_undo_retention_guarantee",
            |db| u8::from(db.undo_retention_guarantee()) as u64,
            "1 = guarantee undo retention even against purge pressure.",
        ),
        (
            "mydb_undo_disk_cap_softfail_warn",
            |db| u8::from(db.undo_disk_cap_softfail_warn()) as u64,
            "1 = emit MySQL warning 1287 when admission soft-fails (disk cap, retention_guarantee OFF).",
        ),
        (
            "mydb_checkpoint_wal_bytes",
            |db| db.checkpoint_wal_bytes(),
            "WAL bytes between checkpoints.",
        ),
        (
            "mydb_group_commit_us",
            |db| db.group_commit_us(),
            "Group-commit leader wait window (microseconds); 0 = natural batching (fsync immediately).",
        ),
    ];

    for &(name, getter, doc) in names {
        register_storage_uint(reg, name, getter, u64::MAX, 0, 0, doc);
    }

    register_storage_uint(
        reg,
        "mydb_wal_write_buffer_bytes",
        |db| db.wal_write_buffer_bytes(),
        crate::wal::MAX_WAL_WRITE_BUFFER_BYTES,
        crate::wal::DEFAULT_WAL_WRITE_BUFFER_BYTES as u64,
        crate::wal::DEFAULT_WAL_WRITE_BUFFER_BYTES as u64,
        "WAL write-buffer threshold in bytes; 0 = write-through; default 256 KiB.",
    );

    // M3 — adaptive group-commit toggle, a 0/1 boolean (registered apart from
    // the generic u64 knobs above so `SET GLOBAL = 2` is rejected).
    register_storage_uint(
        reg, "mydb_group_commit_adaptive", |db| db.group_commit_adaptive(), 1, 0, 0,
        "1 = adapt the group-commit window to recent batch sizes (only meaningful when mydb_group_commit_us > 0).",
    );
    register_storage_uint(
        reg, "mydb_buffer_pool_small_scan_pct", |db| db.buffer_pool_small_scan_pct(), 100, 20, 20,
        "Maximum physical SQL-scan footprint as a percentage of the buffer pool that may promote after reuse (0 = disabled).",
    );

    // FK G1 — cascade depth backstop, runtime-tunable. Backed by a process
    // global (not per-Database) since the depth limit is a thread-local
    // recursion guard; `MYDB_FK_MAX_DEPTH` env remains the startup default.
    // Minimum 1 (0 would read as "not overridden").
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_fk_max_depth",
            Scope::Global,
            DataType::UInt { min: 1, max: u64::from(u32::MAX) },
        )
        .with_doc(
            "Maximum foreign-key cascade delete/update depth before ER_FK_DEPTH_EXCEEDED (runaway-recursion backstop).",
        ),
        |_ctx| SysVarValue::UInt(crate::storage::fk_max_depth_value()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            crate::storage::set_fk_max_depth(v.as_u64().unwrap_or(64));
            Ok(())
        },
    ));

    // CFG-1 - buffer-pool per-kind soft-quota percentages, 0..=100
    // (0 disables the bias). Read off the live atomic, written through
    // `set_storage_variable`; the change takes effect on the next
    // eviction sweep without a restart.
    for &(name, getter, doc) in &[
        (
            "mydb_buffer_pool_quota_heap_pct",
            (|db: &crate::storage::Database| db.buffer_pool_quota_heap_pct())
                as fn(&crate::storage::Database) -> u64,
            "Heap soft-quota % of the buffer pool (0 = disabled).",
        ),
        (
            "mydb_buffer_pool_quota_index_pct",
            |db| db.buffer_pool_quota_index_pct(),
            "Index soft-quota % of the buffer pool (0 = disabled).",
        ),
        (
            "mydb_buffer_pool_quota_lob_pct",
            |db| db.buffer_pool_quota_lob_pct(),
            "LOB soft-quota % of the buffer pool (0 = disabled).",
        ),
        (
            "mydb_buffer_pool_quota_zone_map_pct",
            |db| db.buffer_pool_quota_zone_map_pct(),
            "Zone-map page soft-quota % of the shared buffer pool (0 = disabled).",
        ),
    ] {
        register_storage_uint(reg, name, getter, 100, 0, 0, doc);
    }

    // CFG-1 - base prefetch window in pages, 0 disables read-ahead.
    // Read off the live atomic, written through `set_storage_variable`;
    // a write reseeds the adaptive effective window so it takes effect
    // on the next scan. `max` mirrors MAX_BUFFER_POOL_PREFETCH_PAGES
    // (the storage layer clamps to it, single source of truth there).
    register_storage_uint(
        reg,
        "mydb_buffer_pool_prefetch_pages",
        |db| db.buffer_pool_prefetch_pages(),
        128,
        0,
        0,
        "Base buffer-pool prefetch window in pages (0 = disabled).",
    );

    // Layered memory targets live in the mem_budget module. The two
    // Upstream knobs are `mydb_memory_target_mb` (advisory working-set/cache
    // target) and `mydb_executor_memory_target_mb` (the PGA spill watermark).
    // Hard admission always derives from effective OS/cgroup memory. Setting
    // either resets the derived query/process caches and refreshes the
    // WorkAreaManager target.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_memory_target_mb",
            Scope::Global,
            DataType::UInt {
                min: 0,
                max: u64::MAX,
            },
        )
        .with_doc(
            "Advisory working-set target (MB) for caches and spill watermarks; hard SQL/process limits are 80%/90% of effective OS/cgroup memory.",
        ),
        |_ctx| {
            SysVarValue::UInt(
                (crate::engine::executor::mem_budget::configured_memory_target_bytes()
                    .unwrap_or_else(
                        crate::engine::executor::mem_budget::memory_target_bytes,
                    )
                    / (1024 * 1024)) as u64,
            )
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let n = v.as_u64().unwrap_or(0) as usize;
            if let Some(db) = ctx.db {
                crate::engine::executor::mem_budget::validate_memory_target_mb_with_buffer(
                    n,
                    db.buffer_pool_pages(),
                )?;
            }
            crate::engine::executor::mem_budget::set_memory_target_mb(n)?;
            if let Some(db) = ctx.db {
                let _ = db.reclaim_for_memory_target(
                    crate::engine::executor::mem_budget::memory_target_bytes(),
                );
            }
            crate::engine::executor::work_area::refresh_target_from_env();
            Ok(())
        },
    ));
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_executor_memory_target_mb",
            Scope::Global,
            DataType::UInt {
                min: 0,
                max: u64::MAX,
            },
        )
        .with_doc("Executor work-area soft target (MB). Drives spill grants for Sort/HashJoin/HashAgg/Distinct."),
        |_ctx| {
            SysVarValue::UInt(
                (crate::engine::executor::mem_budget::executor_memory_target_bytes()
                    / (1024 * 1024)) as u64,
            )
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let n = v.as_u64().unwrap_or(0) as usize;
            if let Some(db) = ctx.db {
                crate::engine::executor::mem_budget::validate_executor_target_mb_with_buffer(
                    n,
                    db.buffer_pool_pages(),
                )?;
            }
            crate::engine::executor::mem_budget::set_executor_memory_target_mb(n)?;
            crate::engine::executor::work_area::refresh_target_from_env();
            Ok(())
        },
    ));

    // Executor memory limits live in the mem_budget module.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_query_mem_limit_mb",
            Scope::Global,
            DataType::UInt {
                min: 0,
                max: u64::MAX,
            },
        )
        .with_doc("Per-query work-memory soft spill watermark (MB)."),
        |_ctx| {
            SysVarValue::UInt(
                (crate::engine::executor::mem_budget::query_mem_limit_bytes() / (1024 * 1024))
                    as u64,
            )
        },
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let n = v.as_u64().unwrap_or(0) as usize;
            crate::engine::executor::mem_budget::set_query_mem_limit_mb(n)?;
            Ok(())
        },
    ));
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_process_mem_limit_mb",
            Scope::Global,
            DataType::UInt {
                min: 0,
                max: u64::MAX,
            },
        )
        .with_doc("Aggregate process work-memory soft spill watermark (MB)."),
        |_ctx| {
            SysVarValue::UInt(
                (crate::engine::executor::mem_budget::process_mem_limit_bytes() / (1024 * 1024))
                    as u64,
            )
        },
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let n = v.as_u64().unwrap_or(0) as usize;
            crate::engine::executor::mem_budget::set_process_mem_limit_mb(n)?;
            Ok(())
        },
    ));
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_idle_memory_reclaim",
            Scope::Global,
            DataType::Bool,
        )
        .with_doc(
            "Release allocator-retained pages after 250ms of SQL inactivity when the process footprint exceeds 110% of mydb_memory_target_mb.",
        ),
        |_ctx| SysVarValue::Bool(crate::engine::memory_reclaimer::enabled()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            crate::engine::memory_reclaimer::set_enabled(v.as_bool().unwrap_or(true));
            Ok(())
        },
    ));

    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_temp_disk_limit_bytes",
            Scope::Global,
            DataType::UInt {
                min: 0,
                max: u64::MAX,
            },
        )
        .with_doc("Global live spill-file limit in bytes (0 = derive from filesystem free space)."),
        |_ctx| SysVarValue::UInt(crate::engine::executor::spill_quota::global_limit_bytes()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            crate::engine::executor::spill_quota::set_global_limit_bytes(v.as_u64().unwrap_or(0));
            Ok(())
        },
    ));
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_temp_file_limit_per_query_bytes",
            Scope::Global,
            DataType::UInt {
                min: 0,
                max: u64::MAX,
            },
        )
        .with_doc("Per-query live spill-file limit in bytes (0 = unlimited)."),
        |_ctx| SysVarValue::UInt(crate::engine::executor::spill_quota::per_query_limit_bytes()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            crate::engine::executor::spill_quota::set_per_query_limit_bytes(
                v.as_u64().unwrap_or(0),
            );
            Ok(())
        },
    ));
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_temp_disk_reserve_pct",
            Scope::Global,
            DataType::UInt { min: 0, max: 90 },
        )
        .with_doc("Percentage of the spill filesystem that must remain free."),
        |_ctx| SysVarValue::UInt(crate::engine::executor::spill_quota::reserve_pct()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            crate::engine::executor::spill_quota::set_reserve_pct(v.as_u64().unwrap_or(10))
        },
    ));
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_work_area_oversubscribe_pct",
            Scope::Global,
            DataType::UInt {
                min: 100,
                max: 100_000,
            },
        )
        .with_doc("Maximum percentage of a work area's concurrency-fair memory share."),
        |_ctx| {
            SysVarValue::UInt(
                crate::engine::executor::work_area::work_area_oversubscribe_pct() as u64,
            )
        },
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            crate::engine::executor::work_area::set_work_area_oversubscribe_pct(
                v.as_u64().unwrap_or(100) as u32,
            )
        },
    ));

    // Cursor registry knobs.
    for &(name, getter, doc) in &[
        (
            "mydb_cursor_per_session_max",
            crate::engine::cursor_registry::per_session_max as fn() -> usize,
            "Per-session cursor cap.",
        ),
        (
            "mydb_cursor_global_max",
            crate::engine::cursor_registry::global_max as fn() -> usize,
            "Process-wide cursor cap.",
        ),
    ] {
        let canon: &'static str = name;
        reg.register(SysVar::settable(
            SysVarDef::new(
                canon,
                Scope::Global,
                DataType::UInt {
                    min: 0,
                    max: u64::MAX,
                },
            )
            .with_doc(doc),
            move |_ctx| SysVarValue::UInt(getter() as u64),
            move |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                let n = v.as_u64().unwrap_or(0);
                if let Some(db) = ctx.db {
                    db.set_storage_variable(canon, n)?;
                }
                Ok(())
            },
        ));
    }
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_cursor_max_idle_ms",
            Scope::Global,
            DataType::UInt {
                min: 0,
                max: u64::MAX,
            },
        )
        .with_doc("Cursor idle timeout."),
        |_ctx| SysVarValue::UInt(crate::engine::cursor_registry::max_idle_ms()),
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let n = v.as_u64().unwrap_or(0);
            if let Some(db) = ctx.db {
                db.set_storage_variable("mydb_cursor_max_idle_ms", n)?;
            }
            Ok(())
        },
    ));

    // M7 — LOB GUCs are runtime-tunable. Getters/setters delegate to the
    // storage core's `sysvar_lob_*` (atomic values, u64-encoded: boolean = 0/1,
    // compression lz4 = 1). The numeric knobs (inline_max / shrink_min_free_pages /
    // extent_min_pages / compress_min_bytes) use UInt; the boolean knobs
    // (reuse_unchanged / shrink_disable / prefetch_disable) and compression also use
    // UInt{0,1}, matching the existing mydb_* boolean style (0/1); SET values other than 0/1 are rejected by the DataType upper bound.
    //
    // (name, doc, getter, setter, min, max)
    type LobGet = fn() -> u64;
    type LobSet = fn(u64);
    let lob_knobs: &[(&'static str, &'static str, LobGet, LobSet, u64, u64)] = &[
        (
            "mydb_lob_inline_max",
            "Inline vs overflow threshold in bytes (M7: runtime-tunable).",
            crate::storage::sysvar_lob_inline_max_get,
            crate::storage::sysvar_lob_inline_max_set,
            0,
            u64::MAX,
        ),
        (
            "mydb_lob_reuse_unchanged",
            "Reuse byte-identical old LOB chains on UPDATE (0/1).",
            crate::storage::sysvar_lob_reuse_unchanged_get,
            crate::storage::sysvar_lob_reuse_unchanged_set,
            0,
            1,
        ),
        (
            "mydb_lob_shrink_disable",
            "Disable .lob tail-shrink on OPTIMIZE TABLE (0/1).",
            crate::storage::sysvar_lob_shrink_disable_get,
            crate::storage::sysvar_lob_shrink_disable_set,
            0,
            1,
        ),
        (
            "mydb_lob_shrink_min_free_pages",
            "Min trailing free pages before .lob is physically truncated.",
            crate::storage::sysvar_lob_shrink_min_free_pages_get,
            crate::storage::sysvar_lob_shrink_min_free_pages_set,
            0,
            u64::MAX,
        ),
        (
            "mydb_lob_extent_min_pages",
            "Chain page count at/above which contiguous extent allocation kicks in (0=off).",
            crate::storage::sysvar_lob_extent_min_pages_get,
            crate::storage::sysvar_lob_extent_min_pages_set,
            0,
            u64::MAX,
        ),
        (
            "mydb_lob_prefetch_disable",
            "Disable in-chain prefetch when reading large LOBs (0/1).",
            crate::storage::sysvar_lob_prefetch_disable_get,
            crate::storage::sysvar_lob_prefetch_disable_set,
            0,
            1,
        ),
        (
            "mydb_lob_compression",
            "LOB overflow compression: 0=off, 1=lz4.",
            crate::storage::sysvar_lob_compression_get,
            crate::storage::sysvar_lob_compression_set,
            0,
            1,
        ),
        (
            "mydb_lob_compress_min_bytes",
            "Minimum raw bytes before a LOB column is lz4-compressed.",
            crate::storage::sysvar_lob_compress_min_bytes_get,
            crate::storage::sysvar_lob_compress_min_bytes_set,
            0,
            u64::MAX,
        ),
    ];
    for &(name, doc, getter, setter, min, max) in lob_knobs {
        reg.register(SysVar::settable(
            SysVarDef::new(name, Scope::Global, DataType::UInt { min, max }).with_doc(doc),
            move |_ctx| SysVarValue::UInt(getter()),
            move |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                setter(v.as_u64().unwrap_or(0));
                Ok(())
            },
        ));
    }

    // WAL flush method (Phase 1/4, WAL improvement plan). Enum so users
    // can `SET GLOBAL mydb_wal_sync_method='fullfsync'`. Read off / written
    // to the process-global atomic; takes effect on the next sync. The
    // startup default is platform-derived (macOS => fsync, like
    // MySQL/PostgreSQL; Linux => fdatasync); this overrides it at runtime.
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_wal_sync_method",
            Scope::Global,
            DataType::Enum {
                values: &["fdatasync", "fsync", "fullfsync", "barrierfsync", "nosync"],
            },
        )
        .with_doc(
            "WAL flush method: fdatasync | fsync | fullfsync (macOS F_FULLFSYNC, \
             only fully power-loss-durable option there) | barrierfsync (macOS \
             F_BARRIERFSYNC: I/O ordering barrier, cheaper than fullfsync) | \
             nosync (no flush).",
        ),
        |_ctx| SysVarValue::String(crate::wal::resolved_wal_sync_method().as_str().to_string()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let raw = v.as_str().unwrap_or("");
            match crate::wal::WalSyncMethod::parse(raw) {
                Some(method) => {
                    crate::wal::set_wal_sync_method(method);
                    Ok(())
                }
                None => Err(crate::storage::error(
                    crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                    &format!(
                        "Variable 'mydb_wal_sync_method' can't be set to the value of '{raw}'"
                    ),
                )),
            }
        },
    ));

    // Replica read-only flag from the mydb-native physical replication
    // module already lives in `replication.rs`'s register fn, but we want
    // to ensure the parser sees a unique entry — it's there.

    // Background Auto-ANALYZE knobs. Read/written through the
    // `crate::engine::auto_analyze` module atomics (no `Database`
    // round-trip — the worker is a process-global background task).
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_auto_analyze_interval_s",
            Scope::Global,
            DataType::UInt {
                min: 1,
                max: u64::MAX,
            },
        )
        .with_doc("Background Auto-ANALYZE poll interval (seconds, >= 1)."),
        |_ctx| SysVarValue::UInt(crate::engine::auto_analyze::interval_s()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            crate::engine::auto_analyze::set_interval_s(v.as_u64().unwrap_or(1).max(1));
            Ok(())
        },
    ));
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_auto_analyze_ratio",
            Scope::Global,
            DataType::Double {
                min: f64::MIN_POSITIVE,
                max: f64::MAX,
            },
        )
        .with_doc(
            "Background Auto-ANALYZE staleness threshold: re-sample once \
             modified rows exceed this fraction of the table row count.",
        ),
        |_ctx| SysVarValue::Double(crate::engine::auto_analyze::ratio()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let r = v.as_f64().unwrap_or(0.0);
            if !crate::engine::auto_analyze::set_ratio(r) {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                    "Variable 'mydb_auto_analyze_ratio' requires a finite positive value",
                ));
            }
            Ok(())
        },
    ));
    reg.register(SysVar::settable(
        SysVarDef::new("mydb_auto_analyze_disable", Scope::Global, DataType::Bool)
            .with_doc("1 = disable the background Auto-ANALYZE worker."),
        |_ctx| SysVarValue::Bool(crate::engine::auto_analyze::is_disabled()),
        |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            crate::engine::auto_analyze::set_disabled(v.as_bool().unwrap_or(false));
            Ok(())
        },
    ));

    reg.register(
        SysVar::settable(
            SysVarDef::new("mydb_zone_map_enabled", Scope::Global, DataType::Bool)
                .with_doc("Enable zone-map maintenance and pruning for all eligible tables."),
            |ctx| SysVarValue::Bool(ctx.db.is_none_or(|db| db.zone_map_enabled())),
            |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                let db = require_zone_map_database(ctx)?;
                db.set_zone_map_enabled(v.as_bool().unwrap_or(true))?;
                Ok(())
            },
        )
        .with_default(SysVarValue::Bool(true)),
    );

    reg.register(SysVar::settable(
        SysVarDef::new("mydb_zone_map_maintenance", Scope::Global, DataType::Bool)
            .with_doc("Enable low-priority automatic zone-map maintenance."),
        |ctx| SysVarValue::Bool(ctx.db.map(|db| db.zone_map_maintenance()).unwrap_or(true)),
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let db = require_zone_map_database(ctx)?;
            db.set_zone_map_maintenance(v.as_bool().unwrap_or(true));
            Ok(())
        },
    ));
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_catalog_cache_mb",
            Scope::Global,
            DataType::UInt {
                min: 0,
                max: u64::MAX,
            },
        )
        .with_doc(
            "Catalog descriptor/name/auth cache target in MB; defaults to 5% of MYDB_MEMORY_TARGET capped at 256 MB.",
        ),
        |ctx| SysVarValue::UInt(ctx.db.map(|db| db.catalog_cache_mb()).unwrap_or(0)),
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            if let Some(db) = ctx.db {
                db.set_catalog_cache_mb(v.as_u64().unwrap_or(0));
            }
            Ok(())
        },
    ));
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_zone_map_maintenance_quiet_ms",
            Scope::Global,
            DataType::UInt {
                min: 0,
                max: 86_400_000,
            },
        ),
        |ctx| {
            SysVarValue::UInt(
                ctx.db
                    .map(|db| db.zone_map_maintenance_quiet_ms())
                    .unwrap_or(30_000),
            )
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let db = require_zone_map_database(ctx)?;
            db.set_zone_map_maintenance_quiet_ms(v.as_u64().unwrap_or(30_000));
            Ok(())
        },
    ));
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_zone_map_maintenance_retry_ms",
            Scope::Global,
            DataType::UInt {
                min: 0,
                max: 86_400_000,
            },
        ),
        |ctx| {
            SysVarValue::UInt(
                ctx.db
                    .map(|db| db.zone_map_maintenance_retry_ms())
                    .unwrap_or(30_000),
            )
        },
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let db = require_zone_map_database(ctx)?;
            db.set_zone_map_maintenance_retry_ms(v.as_u64().unwrap_or(30_000));
            Ok(())
        },
    ));
    reg.register(SysVar::settable(
        SysVarDef::new(
            "mydb_zone_map_rebuild_ratio",
            Scope::Global,
            DataType::Double { min: 0.0, max: 1.0 },
        )
        .with_doc("Stale-mutation fraction that schedules a zone rebuild."),
        |ctx| SysVarValue::Double(ctx.db.map(|db| db.zone_map_rebuild_ratio()).unwrap_or(0.20)),
        |ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
            let db = require_zone_map_database(ctx)?;
            if !db.set_zone_map_rebuild_ratio(v.as_f64().unwrap_or(-1.0)) {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                    "mydb_zone_map_rebuild_ratio must be between 0 and 1",
                ));
            }
            Ok(())
        },
    ));
}

fn require_zone_map_database<'a>(
    ctx: &WriteCtx<'a>,
) -> Result<&'a crate::storage::Database, QueryError> {
    ctx.db.ok_or_else(|| {
        crate::storage::error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
            "zone-map variable requires a database context",
        )
    })
}

/// Shared registration for Database-backed integer knobs. Defaults and bounds
/// stay explicit; storage continues to validate and apply the actual update.
#[allow(clippy::too_many_arguments)]
fn register_storage_uint(
    reg: &mut Registry,
    name: &'static str,
    getter: fn(&crate::storage::Database) -> u64,
    max: u64,
    read_default: u64,
    write_default: u64,
    doc: &'static str,
) {
    reg.register(SysVar::settable(
        SysVarDef::new(name, Scope::Global, DataType::UInt { min: 0, max }).with_doc(doc),
        move |ctx| SysVarValue::UInt(ctx.db.map(getter).unwrap_or(read_default)),
        move |ctx: &mut WriteCtx<'_>, value: SysVarValue| -> Result<(), QueryError> {
            if let Some(db) = ctx.db {
                db.set_storage_variable(name, value.as_u64().unwrap_or(write_default))?;
            }
            Ok(())
        },
    ));
}
