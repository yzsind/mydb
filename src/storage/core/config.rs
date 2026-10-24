impl DurabilityMode {
    fn from_env() -> Self {
        match crate::util::env::var("MYDB_DURABILITY")
            .unwrap_or_else(|_| "strict".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "relaxed" => Self::Relaxed,
            "memory" => Self::Memory,
            _ => Self::Strict,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Relaxed => "relaxed",
            Self::Memory => "memory",
        }
    }

    pub(crate) fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Strict,
            1 => Self::Relaxed,
            _ => Self::Memory,
        }
    }

    pub(crate) fn to_u8(self) -> u8 {
        match self {
            Self::Strict => 0,
            Self::Relaxed => 1,
            Self::Memory => 2,
        }
    }
}

pub(crate) fn durability_to_u8(d: DurabilityMode) -> u8 {
    d.to_u8()
}

pub(crate) fn durability_from_u8(v: u8) -> DurabilityMode {
    DurabilityMode::from_u8(v)
}

impl StartupValidationMode {
    fn from_env() -> Self {
        match crate::util::env::var("MYDB_STARTUP_VALIDATION")
            .unwrap_or_else(|_| "auto".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "full" => Self::Full,
            "off" => Self::Off,
            _ => Self::Auto,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Full => "full",
            Self::Off => "off",
        }
    }
}

impl StartupValidationPath {
    fn as_str(self) -> &'static str {
        match self {
            Self::CleanFast => "clean_fast",
            Self::Full => "full",
            Self::Off => "off",
        }
    }
}

/// Resolve buffer-pool page count.
/// Priority:
///   1. `MYDB_BUFFER_POOL_PAGES` (legacy direct override)
///   2. `MYDB_MEMORY_TARGET × MYDB_BUFFER_POOL_PCT / 100 / PAGE_SIZE`
///   3. adaptive default from the explicit or auto memory target
///
/// Floors at 1 024 pages (16 MiB) so an absurdly small target still
/// produces a runnable pool.
const DEFAULT_BUFFER_POOL_MIN_PCT: u64 = 20;
const DEFAULT_BUFFER_POOL_MAX_PCT: u64 = 60;
const DEFAULT_BUFFER_POOL_NON_BUFFER_HEADROOM_BYTES: u64 = 512 * 1024 * 1024;

/// Size the default raw-page cache without a discontinuity at 1 GiB.
///
/// Small targets retain the conservative 20% floor. As the target grows, the
/// pool uses memory above a fixed 512 MiB non-buffer envelope until reaching
/// the normal 60% ceiling. The hard governor separately reserves its rollback
/// and unmanaged slices, so this envelope is available to catalog/zone
/// metadata, protocol buffers, undo and short-lived storage scratch.
pub(crate) fn default_buffer_pool_bytes(target_bytes: u64) -> u64 {
    let minimum = target_bytes.saturating_mul(DEFAULT_BUFFER_POOL_MIN_PCT) / 100;
    let maximum = target_bytes.saturating_mul(DEFAULT_BUFFER_POOL_MAX_PCT) / 100;
    let after_headroom = target_bytes.saturating_sub(DEFAULT_BUFFER_POOL_NON_BUFFER_HEADROOM_BYTES);
    minimum.max(after_headroom.min(maximum))
}

pub fn resolve_buffer_pool_pages() -> u64 {
    if let Ok(v) = crate::util::env::var("MYDB_BUFFER_POOL_PAGES") {
        if let Ok(n) = v.parse::<u64>() {
            if n > 0 {
                return n;
            }
        }
    }
    let configured_target = crate::util::env::var("MYDB_MEMORY_TARGET")
        .ok()
        .and_then(|value| {
            crate::util::system_memory::parse_mebibyte_or_size_bytes(&value)
        })
        .unwrap_or_else(|| {
            crate::util::system_memory::default_memory_target_mb()
                .saturating_mul(1024 * 1024)
        });
    let target_bytes = crate::util::system_memory::effective_memory_limit_bytes()
        .map(|limit| configured_target.min(limit))
        .unwrap_or(configured_target);
    let explicit_pct = crate::util::env::var("MYDB_BUFFER_POOL_PCT")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0 && *n <= 100);
    let bytes = explicit_pct.map_or_else(
        || default_buffer_pool_bytes(target_bytes),
        |pct| target_bytes.saturating_mul(pct) / 100,
    );
    let pages = bytes / (crate::storage::core::PAGE_SIZE as u64);
    pages.max(1024)
}

#[cfg(test)]
mod buffer_pool_sizing_tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn adaptive_default_preserves_headroom_without_a_one_gib_cliff() {
        assert_eq!(default_buffer_pool_bytes(256 * MIB), 256 * MIB / 5);
        assert_eq!(default_buffer_pool_bytes(512 * MIB), 512 * MIB / 5);
        assert_eq!(default_buffer_pool_bytes(768 * MIB), 256 * MIB);
        assert_eq!(default_buffer_pool_bytes(1000 * MIB), 488 * MIB);
        assert_eq!(default_buffer_pool_bytes(1024 * MIB), 512 * MIB);
        assert_eq!(default_buffer_pool_bytes(1280 * MIB), 768 * MIB);
        assert_eq!(
            default_buffer_pool_bytes(12 * 1024 * MIB),
            12 * 1024 * MIB * 3 / 5
        );
    }

    #[test]
    fn adaptive_default_is_monotonic_and_continuous() {
        let mut previous = default_buffer_pool_bytes(MIB);
        for target_mib in 2..=4096 {
            let current = default_buffer_pool_bytes(target_mib * MIB);
            assert!(current >= previous, "target_mib={target_mib}");
            assert!(current - previous <= MIB, "target_mib={target_mib}");
            previous = current;
        }
    }
}

/// Resolve the per-page UPDATE reserve in bytes from env vars,
/// preferring `MYDB_PCTFREE` (percent of `PAGE_SIZE`) when set and
/// falling back to the byte-valued `MYDB_HEAP_UPDATE_RESERVE_BYTES`
/// for compatibility with the legacy knob. `MYDB_PCTFREE` is the
/// Oracle-style spelling — same semantics as InnoDB's
/// `MERGE_THRESHOLD` upside-down — and is what `ALTER TABLE …
/// PCTFREE=N` will eventually plumb through to per-table reserve.
/// Clamped to [0, MAX_HEAP_UPDATE_RESERVE_BYTES].
fn resolve_heap_update_reserve_bytes() -> usize {
    if let Some(pct) = crate::util::env::var("MYDB_PCTFREE")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
    {
        let pct = pct.min(50); // never reserve more than half a page
        let bytes = (crate::storage::core::PAGE_SIZE as u64)
            .saturating_mul(pct as u64)
            / 100;
        return (bytes as usize).min(MAX_HEAP_UPDATE_RESERVE_BYTES);
    }
    env_u64(
        "MYDB_HEAP_UPDATE_RESERVE_BYTES",
        DEFAULT_HEAP_UPDATE_RESERVE_BYTES as u64,
    )
    .min(MAX_HEAP_UPDATE_RESERVE_BYTES as u64) as usize
}

impl StorageConfig {
    pub fn from_env() -> Self {
        Self {
            durability: DurabilityMode::from_env(),
            checkpoint_interval: Duration::from_millis(env_u64(
                "MYDB_CHECKPOINT_INTERVAL_MS",
                DEFAULT_CHECKPOINT_INTERVAL_MS,
            )),
            checkpoint_wal_bytes: env_u64(
                "MYDB_CHECKPOINT_WAL_BYTES",
                DEFAULT_CHECKPOINT_WAL_BYTES,
            ),
            lock_wait: Duration::from_millis(env_u64("MYDB_LOCK_WAIT_MS", DEFAULT_LOCK_WAIT_MS)),
            group_commit_window: Duration::from_micros(env_u64(
                "MYDB_GROUP_COMMIT_US",
                DEFAULT_GROUP_COMMIT_US,
            )),
            wal_segment_bytes: env_u64("MYDB_WAL_SEGMENT_BYTES", DEFAULT_WAL_SEGMENT_BYTES),
            buffer_pool_pages: resolve_buffer_pool_pages(),
            buffer_pool_io_threads: env_u64(
                "MYDB_BUFFER_POOL_IO_THREADS",
                default_buffer_pool_io_threads() as u64,
            )
            .clamp(1, MAX_BUFFER_POOL_IO_THREADS as u64)
                as usize,
            buffer_pool_prefetch_pages: env_u64(
                "MYDB_BUFFER_POOL_PREFETCH_PAGES",
                DEFAULT_BUFFER_POOL_PREFETCH_PAGES as u64,
            )
            // N-1 - prefetch cap decoupled from flush batch size; the
            // adaptive controller can grow up to MAX_BUFFER_POOL_PREFETCH_PAGES.
            .min(MAX_BUFFER_POOL_PREFETCH_PAGES as u64)
                as usize,
            undo_retention: Duration::from_millis(env_u64(
                "MYDB_UNDO_RETENTION_MS",
                DEFAULT_UNDO_RETENTION_MS,
            )),
            heap_update_reserve_bytes: resolve_heap_update_reserve_bytes(),
            startup_validation: StartupValidationMode::from_env(),
            startup_validate_indexes: env_bool("MYDB_STARTUP_VALIDATE_INDEXES", true),
        }
    }
}

impl RuntimeStorageConfig {
    fn from_storage_config(config: &StorageConfig) -> Self {
        Self {
            checkpoint_wal_bytes: config.checkpoint_wal_bytes,
            buffer_pool_pages: config.buffer_pool_pages,
            undo_retention: config.undo_retention,
            undo_segment_bytes: env_u64(
                "MYDB_UNDO_SEGMENT_BYTES",
                crate::storage::undo::DEFAULT_UNDO_SEGMENT_BYTES,
            ),
            undo_purge_interval: Duration::from_millis(env_u64(
                "MYDB_UNDO_PURGE_INTERVAL_MS",
                crate::storage::undo::DEFAULT_UNDO_PURGE_INTERVAL_MS,
            )),
            undo_purge_batch_records: env_u64(
                "MYDB_UNDO_PURGE_BATCH_RECORDS",
                crate::storage::undo::DEFAULT_UNDO_PURGE_BATCH_RECORDS,
            )
            .max(1) as usize,
            undo_tablespace_max_bytes: env_u64("MYDB_UNDO_TABLESPACE_MAX_BYTES", 0),
            // P0/B2: soft cap on the in-memory `records` mirror. 0 = disabled
            // (opt-in, mirroring the disk tablespace cap). When the decoded
            // undo records exceed this, admission flags pressure so the
            // existing `purge_obsolete_undo()` hook fires immediately instead
            // of waiting for the periodic tick — bounding the mirror under a
            // write burst between purge cycles.
            undo_mem_soft_limit_bytes: env_u64("MYDB_UNDO_MEM_SOFT_LIMIT", 0),
            // Phase 2 (two-level mirror): cap on the *resident* undo payload
            // bytes. The bounded default prevents the retention window from
            // inflating RSS by multiple GiB under sustained writes; `0`
            // explicitly restores the historical all-resident behavior.
            // Recovery and steady state share this resolver so they enforce
            // exactly the same cap.
            undo_mem_cache_limit_bytes:
                crate::storage::undo::default_undo_mem_cache_bytes(),
            // Phase 4 — per-transaction undo byte cap. 0 = unlimited. A
            // single runaway txn is rejected on its own growth instead of
            // exhausting the shared tablespace and failing every session.
            undo_txn_max_bytes: env_u64("MYDB_UNDO_TXN_MAX_BYTES", 0),
            undo_retention_guarantee: env_bool("MYDB_UNDO_RETENTION_GUARANTEE", false),
            // Seed the group-commit knobs from the same window the static
            // `StorageConfig` resolved, and the adaptive flag from its env
            // default. Both are settable at runtime via `SET GLOBAL`.
            group_commit_us: config.group_commit_window.as_micros().min(u128::from(u64::MAX))
                as u64,
            group_commit_adaptive:
                crate::storage::txn::adaptive_group_commit_env_default(),
        }
    }
}
