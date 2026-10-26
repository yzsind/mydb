//! Per-query / per-process memory budgeting for the executor.
//!
//! Blocking operators (HashAggregate / Sort / HashJoin / Distinct) use this
//! budget as a spill watermark. Process-tracked executor charges also enter
//! the unified controlled-memory hard governor. Accounting is cooperative:
//! callers explicitly `charge` bytes when allocating large structures
//! and `release` on drop.
//!
//! The per-query limit is derived from `MYDB_MEMORY_TARGET` × executor
//! pct × query quota pct (see the "Layered memory targets" block
//! below); the legacy `MYDB_QUERY_MEM_LIMIT_MB` env is a direct
//! override. The limit is checked best-effort: it does not account
//! for every Rust allocation, only the headline operator state
//! explicitly tracked by the executor.

use crate::util::atomic_accounting::saturating_sub_usize as saturating_atomic_sub;
use crate::util::atomic_accounting::try_add_usize as try_atomic_add_bounded;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use crate::types::QueryError;

// ---------------------------------------------------------------------------
// Layered memory targets (Oracle PGA-style)
// ---------------------------------------------------------------------------
//
// Host/cgroup effective memory is the hard-limit base: ordinary governed SQL
// may use 80%, rollback/compensation may use up to 90%, and the final 10% is
// left to stacks/runtime/allocator. MYDB_MEMORY_TARGET is independent advisory
// working-set input for buffer/cache sizing and executor spill watermarks.
//
//   MYDB_BUFFER_POOL_PCT      (adaptive 20-60%, max 60) → long-lived page cache
//   MYDB_EXECUTOR_MEMORY_PCT  (default 30) → executor work areas
//
// Of the unassigned portion, 10% is a managed rollback reserve and 10% remains
// outside the managed ceiling for stacks/runtime/allocator. The split is
// advisory: explicit MYDB_BUFFER_POOL_PAGES / MYDB_EXECUTOR_MEMORY_TARGET win,
// but buffer pages still cannot consume the rollback reserve.
//
// Per-query defaults are *also* derived from the executor target via
// MYDB_QUERY_MEM_QUOTA_PCT (default 42, i.e. a single query starts spilling
// after using 42% of the executor budget; the process-wide cap and
// per-operator spill paths additionally bound aggregate memory). A handful
// of operators (Sort, vec
// HashAgg, vec Distinct) additionally register a `WorkAreaHandle`
// with the [`WorkAreaManager`] PGA arbiter, which grants tier-based
// work areas (optimal / one_pass / min) and returns `SpillHint::Spill`
// instead of 1037 when the global pool tightens. Everyone else
// (row-based HashAggregate, row-based HashJoin build + bloom, INLJ
// MRR cache, result-row materialisation, …) only consults this
// `MemBudget` umbrella; for those callers a per-query or per-process
// soft-watermark overflow asks spill-capable operators to spill. Operators
// that cannot spill use force accounting, which crosses the soft watermarks
// but still returns 1037/HY001 at the unified operational hard ceiling.

const DEFAULT_EXECUTOR_MEMORY_PCT: u32 = 30;
// A blocking analytic query may use 42% of the executor pool. This keeps two
// full-sized queries admissible concurrently with headroom while giving compact hash/sort
// layouts enough room to avoid a spill whose replay costs more than the saved
// resident bytes. The process and controlled-memory hard limits remain the
// non-bypassable ceilings; additional concurrent queries spill or fail their
// reservation normally.
const DEFAULT_QUERY_MEM_QUOTA_PCT: u32 = 42;
/// Critical governed work may consume at most this fraction of effective
/// host/cgroup memory; the remainder belongs to runtime/allocator overhead.
const MANAGED_MEMORY_PCT: usize = 90;
/// Ordinary work must leave a bounded part of the managed budget available to
/// transaction rollback. An allocation failure is recoverable only if the
/// compensating path can still encode WAL/CLR records and read undo pages.
const OPERATIONAL_MEMORY_PCT: usize = 80;
/// Performance Schema histories and summaries are optional observability
/// state. Cap them independently so lazy shard initialisation cannot consume
/// all remaining process headroom at high connection counts.
const PERFORMANCE_SCHEMA_MEMORY_PCT: usize = 5;
/// Slow-path physical admission reclaims in bounded chunks. Exact cgroup
/// samples can fall just below the limit after cache trim, then rise again
/// before the retry as parallel workers fault their already-governed pages.
/// Matching the pressure sampler's maximum 64 MiB allowance prevents that
/// race without reserving another long-lived pool.
const ADMISSION_RECLAIM_HEADROOM_BYTES: usize = 64 * 1024 * 1024;
const ADMISSION_RECLAIM_MAX_PASSES: usize = 8;
const MIN_EXECUTOR_TARGET_BYTES: usize = 16 * 1024 * 1024; // 16 MiB floor
const MIN_QUERY_LIMIT_BYTES: usize = 8 * 1024 * 1024; //  8 MiB floor
                                                      // MYDB_BUFFER_POOL_PCT default lives next to its only consumer in
                                                      // storage/core/config.rs::resolve_buffer_pool_pages.

static MEMORY_TARGET_BYTES: AtomicUsize = AtomicUsize::new(0);
static MEMORY_TARGET_EXPLICIT: AtomicBool = AtomicBool::new(false);
static EFFECTIVE_MEMORY_LIMIT_BYTES: AtomicUsize = AtomicUsize::new(0);
static EXECUTOR_MEMORY_TARGET_BYTES: AtomicUsize = AtomicUsize::new(0);
static QUERY_MEM_LIMIT_BYTES: AtomicUsize = AtomicUsize::new(0);
static PROCESS_MEM_LIMIT_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Coarse controlled-memory governor. This is intentionally independent from
/// RSS: it covers allocations the engine owns and can reclaim/spill.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlledMemoryCategory {
    BufferRaw,
    StorageScratch,
    Executor,
    #[allow(dead_code)]
    DerivedCache,
    PartitionDmlBatch,
    PartitionDdl,
    PartitionStats,
    DdlCopy,
    /// Row identities/deltas retained while an online DDL snapshot is built.
    /// The owner is the DDL job; it is released only after final replay or
    /// abort, and admission failure invalidates the DDL rather than DML.
    DdlOnlineLog,
    SystemMetadata,
    TemporalHistory,
    /// MySQL wire payloads, encoded result batches and packet staging buffers.
    /// Charged by allocation capacity and released when the connection/batch
    /// drops; unlike executor work areas this category may never spill through
    /// the query soft quota because it is part of process-resident memory.
    ProtocolTransient,
    /// Transaction-scoped row images retained until binlog commit/rollback.
    /// These are mandatory replication state, so admission failure is a clear
    /// HY001 rather than an ungoverned process allocation.
    BinlogTransient,
    /// SQL tokenizer/parser scratch and bounded parser-worker stacks. These
    /// reservations live only for the synchronous parse and are released
    /// before execution; retained prepared/plan ASTs are charged by their
    /// owning caches separately.
    ParserTransient,
    /// Native mydb WAL replication wire/TLS/base-backup/apply buffers.
    NativeReplicationTransient,
    /// Process-resident Performance Schema setup state, event rings, digest
    /// buckets and sparse summary data. Optional PFS buffers may shrink or
    /// drop samples under pressure, but they may never bypass the process
    /// memory target.
    PerformanceSchema,
}

static CONTROLLED_MEMORY_USED: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_MEMORY_PEAK: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_BUFFER_USED: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_BUFFER_RAW_LIMIT_BYTES: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_BUFFER_LIMIT_BYTES: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_BUFFER_RAW: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_STORAGE_SCRATCH: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_EXECUTOR: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_DERIVED_CACHE: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_PARTITION_DML_BATCH: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_PARTITION_DDL: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_PARTITION_STATS: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_DDL_COPY: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_DDL_ONLINE_LOG: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_SYSTEM_METADATA: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_TEMPORAL_HISTORY: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_PROTOCOL_TRANSIENT: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_BINLOG_TRANSIENT: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_PARSER_TRANSIENT: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_NATIVE_REPLICATION_TRANSIENT: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_PERFORMANCE_SCHEMA: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_REJECTIONS: AtomicU64 = AtomicU64::new(0);
static RECLAIM_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static RECLAIM_RETRY_SUCCESSES: AtomicU64 = AtomicU64::new(0);
static RECLAIM_CACHE_BYTES: AtomicU64 = AtomicU64::new(0);
static RECLAIM_BUFFER_BYTES: AtomicU64 = AtomicU64::new(0);
static RECLAIM_ALLOCATOR_BYTES: AtomicU64 = AtomicU64::new(0);
static RECLAIM_OOM_AFTER_RETRY: AtomicU64 = AtomicU64::new(0);
static RECLAIM_ADMISSION: OnceLock<Mutex<()>> = OnceLock::new();
static CONTROLLED_REJECTIONS_BY_CATEGORY: [AtomicU64; 16] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
#[allow(clippy::declare_interior_mutable_const)]
const CATEGORY_U64_ZERO: AtomicU64 = AtomicU64::new(0);
#[allow(clippy::declare_interior_mutable_const)]
const CATEGORY_USIZE_ZERO: AtomicUsize = AtomicUsize::new(0);
static CONTROLLED_CATEGORY_CURRENT_COUNT: [AtomicI64; 16] = [const { AtomicI64::new(0) }; 16];
static CONTROLLED_CATEGORY_PEAK_COUNT: [AtomicU64; 16] = [CATEGORY_U64_ZERO; 16];
static CONTROLLED_CATEGORY_PEAK_BYTES: [AtomicUsize; 16] = [CATEGORY_USIZE_ZERO; 16];

// Cumulative instrumentation is written by many independent execution and
// protocol threads. Keep each writer's categories together, without making
// every allocation transfer four process-global counter cache lines. The
// fixed directory bounds metadata; colliding writers still use atomic adds.
const CATEGORY_ACTIVITY_SHARDS: usize = 128;

struct CategoryActivity {
    alloc_count: AtomicU64,
    free_count: AtomicU64,
    alloc_bytes: AtomicU64,
    free_bytes: AtomicU64,
}

impl CategoryActivity {
    const fn new() -> Self {
        Self {
            alloc_count: AtomicU64::new(0),
            free_count: AtomicU64::new(0),
            alloc_bytes: AtomicU64::new(0),
            free_bytes: AtomicU64::new(0),
        }
    }
}

#[repr(align(128))]
struct WorkerCategoryActivity([CategoryActivity; 16]);

static CATEGORY_ACTIVITY: [WorkerCategoryActivity; CATEGORY_ACTIVITY_SHARDS] =
    [const { WorkerCategoryActivity([const { CategoryActivity::new() }; 16]) };
        CATEGORY_ACTIVITY_SHARDS];
static NEXT_CATEGORY_ACTIVITY_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static CATEGORY_ACTIVITY_SHARD: usize =
        NEXT_CATEGORY_ACTIVITY_SHARD.fetch_add(1, Ordering::Relaxed) % CATEGORY_ACTIVITY_SHARDS;
}

#[inline]
fn with_category_activity<R>(slot: usize, f: impl FnOnce(&CategoryActivity) -> R) -> R {
    CATEGORY_ACTIVITY_SHARD.with(|shard| f(&CATEGORY_ACTIVITY[*shard].0[slot]))
}

thread_local! {
    /// Synchronous storage execution stays on one OS thread, so a scoped TLS
    /// marker can let rollback use the emergency slice without weakening the
    /// process-wide hard ceiling for any other session.
    static CRITICAL_MEMORY_RESERVATION_DEPTH: std::cell::Cell<u32> = const {
        std::cell::Cell::new(0)
    };
}

thread_local! {
    static STORAGE_LATCH_ADMISSION_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Prevent synchronous database reclamation while storage latches are held.
/// Accounting and all ordinary/critical ceilings remain unchanged: an
/// allocation which cannot fit returns OOM instead of recursively acquiring
/// WAL or buffer locks through eviction. Enter before taking the latch.
#[must_use]
pub(crate) struct StorageLatchAdmissionScope {
    previous: u32,
    _thread_bound: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl StorageLatchAdmissionScope {
    pub(crate) fn enter() -> Self {
        let previous = STORAGE_LATCH_ADMISSION_DEPTH.with(|depth| {
            let previous = depth.get();
            depth.set(previous.saturating_add(1));
            previous
        });
        Self {
            previous,
            _thread_bound: std::marker::PhantomData,
        }
    }
}

impl Drop for StorageLatchAdmissionScope {
    fn drop(&mut self) {
        let _ = STORAGE_LATCH_ADMISSION_DEPTH.try_with(|depth| depth.set(self.previous));
    }
}

fn storage_latch_admission_active() -> bool {
    STORAGE_LATCH_ADMISSION_DEPTH.with(|depth| depth.get() != 0)
}

/// Run transaction compensation with access to the managed emergency slice.
/// The slice is still below `MYDB_MEMORY_TARGET`; this only crosses the
/// ordinary operational ceiling, never the managed or process ceiling.
pub(crate) fn with_critical_memory_reservations<R>(f: impl FnOnce() -> R) -> R {
    struct Reset(u32);
    impl Drop for Reset {
        fn drop(&mut self) {
            CRITICAL_MEMORY_RESERVATION_DEPTH.with(|depth| depth.set(self.0));
        }
    }

    let previous = CRITICAL_MEMORY_RESERVATION_DEPTH.with(|depth| {
        let previous = depth.get();
        depth.set(previous.saturating_add(1));
        previous
    });
    let _reset = Reset(previous);
    f()
}

pub(crate) fn critical_memory_reservations_active() -> bool {
    CRITICAL_MEMORY_RESERVATION_DEPTH.with(|depth| depth.get() != 0)
}

fn record_category_alloc(category: ControlledMemoryCategory, bytes: usize, current: usize) {
    let slot = category_slot(category);
    with_category_activity(slot, |activity| {
        activity.alloc_count.fetch_add(1, Ordering::Relaxed);
        let count = CONTROLLED_CATEGORY_CURRENT_COUNT[slot].fetch_add(1, Ordering::Relaxed) + 1;
        let count = count.max(0) as u64;
        // High watermarks only increase. Avoid a contended read-modify-write on
        // every allocation once a category is below its established peak.
        if count > CONTROLLED_CATEGORY_PEAK_COUNT[slot].load(Ordering::Relaxed) {
            CONTROLLED_CATEGORY_PEAK_COUNT[slot].fetch_max(count, Ordering::Relaxed);
        }
        activity
            .alloc_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        if current > CONTROLLED_CATEGORY_PEAK_BYTES[slot].load(Ordering::Relaxed) {
            CONTROLLED_CATEGORY_PEAK_BYTES[slot].fetch_max(current, Ordering::Relaxed);
        }
    });
}

fn record_category_free(category: ControlledMemoryCategory, bytes: usize) {
    let slot = category_slot(category);
    with_category_activity(slot, |activity| {
        activity.free_count.fetch_add(1, Ordering::Relaxed);
        CONTROLLED_CATEGORY_CURRENT_COUNT[slot].fetch_sub(1, Ordering::Relaxed);
        activity
            .free_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    });
}

fn category_counter(category: ControlledMemoryCategory) -> &'static AtomicUsize {
    match category {
        ControlledMemoryCategory::BufferRaw => &CONTROLLED_BUFFER_RAW,
        ControlledMemoryCategory::StorageScratch => &CONTROLLED_STORAGE_SCRATCH,
        ControlledMemoryCategory::Executor => &CONTROLLED_EXECUTOR,
        ControlledMemoryCategory::DerivedCache => &CONTROLLED_DERIVED_CACHE,
        ControlledMemoryCategory::PartitionDmlBatch => &CONTROLLED_PARTITION_DML_BATCH,
        ControlledMemoryCategory::PartitionDdl => &CONTROLLED_PARTITION_DDL,
        ControlledMemoryCategory::PartitionStats => &CONTROLLED_PARTITION_STATS,
        ControlledMemoryCategory::DdlCopy => &CONTROLLED_DDL_COPY,
        ControlledMemoryCategory::DdlOnlineLog => &CONTROLLED_DDL_ONLINE_LOG,
        ControlledMemoryCategory::SystemMetadata => &CONTROLLED_SYSTEM_METADATA,
        ControlledMemoryCategory::TemporalHistory => &CONTROLLED_TEMPORAL_HISTORY,
        ControlledMemoryCategory::ProtocolTransient => &CONTROLLED_PROTOCOL_TRANSIENT,
        ControlledMemoryCategory::BinlogTransient => &CONTROLLED_BINLOG_TRANSIENT,
        ControlledMemoryCategory::ParserTransient => &CONTROLLED_PARSER_TRANSIENT,
        ControlledMemoryCategory::NativeReplicationTransient => {
            &CONTROLLED_NATIVE_REPLICATION_TRANSIENT
        }
        ControlledMemoryCategory::PerformanceSchema => &CONTROLLED_PERFORMANCE_SCHEMA,
    }
}

fn category_slot(category: ControlledMemoryCategory) -> usize {
    match category {
        ControlledMemoryCategory::BufferRaw => 0,
        ControlledMemoryCategory::StorageScratch => 1,
        ControlledMemoryCategory::Executor => 2,
        ControlledMemoryCategory::DerivedCache => 3,
        ControlledMemoryCategory::PartitionDmlBatch => 4,
        ControlledMemoryCategory::PartitionDdl => 5,
        ControlledMemoryCategory::PartitionStats => 6,
        ControlledMemoryCategory::DdlCopy => 7,
        ControlledMemoryCategory::SystemMetadata => 8,
        ControlledMemoryCategory::TemporalHistory => 9,
        ControlledMemoryCategory::ProtocolTransient => 10,
        ControlledMemoryCategory::BinlogTransient => 11,
        ControlledMemoryCategory::ParserTransient => 12,
        ControlledMemoryCategory::NativeReplicationTransient => 13,
        ControlledMemoryCategory::PerformanceSchema => 14,
        ControlledMemoryCategory::DdlOnlineLog => 15,
    }
}

fn record_controlled_rejection(category: ControlledMemoryCategory) {
    CONTROLLED_REJECTIONS.fetch_add(1, Ordering::Relaxed);
    CONTROLLED_REJECTIONS_BY_CATEGORY[category_slot(category)].fetch_add(1, Ordering::Relaxed);
}

fn charge_controlled_once(
    category: ControlledMemoryCategory,
    bytes: usize,
    record_rejection: bool,
    honor_critical_scope: bool,
) -> Result<(), QueryError> {
    if bytes == 0 {
        return Ok(());
    }
    let critical = honor_critical_scope && critical_memory_reservations_active();
    // Preserve the compensation reserve in physical memory as well as in
    // logical accounting; allocator/runtime bytes can dominate the latter.
    let pressure_limit = if critical {
        managed_memory_limit_bytes()
    } else {
        operational_memory_limit_bytes()
    };
    if let Err(error) = crate::engine::memory_reclaimer::try_reserve_process_headroom(
        bytes,
        pressure_limit,
        record_rejection,
    ) {
        if record_rejection {
            record_controlled_rejection(category);
        }
        return Err(error);
    }
    let is_buffer = category == ControlledMemoryCategory::BufferRaw;
    if is_buffer {
        let limit = controlled_buffer_limit_bytes();
        if try_atomic_add_bounded(&CONTROLLED_BUFFER_USED, bytes, limit).is_none() {
            if record_rejection {
                record_controlled_rejection(category);
            }
            return Err(crate::storage::error(crate::error::catalog::ER_OUTOFMEMORY,
                format!(
                    "controlled buffer memory target exceeded: tried to allocate {} bytes for {:?}, used {} / {}",
                    bytes,
                    category,
                    CONTROLLED_BUFFER_USED.load(Ordering::Acquire),
                    limit
                ),
            ));
        }
    }
    let limit = if critical {
        managed_memory_limit_bytes()
    } else {
        operational_memory_limit_bytes()
    };
    let Some(total) = try_atomic_add_bounded(&CONTROLLED_MEMORY_USED, bytes, limit) else {
        if is_buffer {
            saturating_atomic_sub(&CONTROLLED_BUFFER_USED, bytes);
        }
        if record_rejection {
            record_controlled_rejection(category);
        }
        return Err(crate::storage::error(crate::error::catalog::ER_OUTOFMEMORY,
            format!(
                "controlled memory target exceeded: tried to allocate {} bytes for {:?}, used {} / {}",
                bytes,
                category,
                CONTROLLED_MEMORY_USED.load(Ordering::Acquire),
                limit
            ),
        ));
    };
    let category_counter = category_counter(category);
    let category_total = if category == ControlledMemoryCategory::PerformanceSchema {
        let category_limit = performance_schema_memory_limit_bytes();
        let Some(category_total) = try_atomic_add_bounded(category_counter, bytes, category_limit)
        else {
            saturating_atomic_sub(&CONTROLLED_MEMORY_USED, bytes);
            if is_buffer {
                saturating_atomic_sub(&CONTROLLED_BUFFER_USED, bytes);
            }
            if record_rejection {
                record_controlled_rejection(category);
            }
            return Err(crate::storage::error(
                crate::error::catalog::ER_OUTOFMEMORY,
                format!(
                    "performance schema memory limit exceeded: tried to allocate {} bytes, used {} / {}",
                    bytes,
                    category_counter.load(Ordering::Acquire),
                    category_limit
                ),
            ));
        };
        category_total
    } else {
        category_counter
            .fetch_add(bytes, Ordering::Relaxed)
            .saturating_add(bytes)
    };
    record_category_alloc(category, bytes, category_total);
    CONTROLLED_MEMORY_PEAK.fetch_max(total, Ordering::Relaxed);
    Ok(())
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AdmissionReclaimSnapshot {
    pub attempts: u64,
    pub retry_successes: u64,
    pub cache_bytes: u64,
    pub buffer_bytes: u64,
    pub allocator_bytes: u64,
    pub oom_after_retry: u64,
}

pub(crate) fn admission_reclaim_snapshot() -> AdmissionReclaimSnapshot {
    AdmissionReclaimSnapshot {
        attempts: RECLAIM_ATTEMPTS.load(Ordering::Relaxed),
        retry_successes: RECLAIM_RETRY_SUCCESSES.load(Ordering::Relaxed),
        cache_bytes: RECLAIM_CACHE_BYTES.load(Ordering::Relaxed),
        buffer_bytes: RECLAIM_BUFFER_BYTES.load(Ordering::Relaxed),
        allocator_bytes: RECLAIM_ALLOCATOR_BYTES.load(Ordering::Relaxed),
        oom_after_retry: RECLAIM_OOM_AFTER_RETRY.load(Ordering::Relaxed),
    }
}

pub(crate) fn current_reclaim_database() -> Option<Arc<crate::storage::Database>> {
    CURRENT_QUERY_BUDGET
        .with(|current| {
            current
                .borrow()
                .as_ref()
                .and_then(|budget| budget.reclaim_database())
        })
        .or_else(|| {
            CURRENT_RECLAIM_DATABASE
                .with(|current| current.borrow().as_ref().and_then(Weak::upgrade))
        })
}

fn charge_controlled_impl(
    category: ControlledMemoryCategory,
    bytes: usize,
    record_rejection: bool,
    honor_critical_scope: bool,
) -> Result<(), QueryError> {
    let initial = charge_controlled_once(category, bytes, false, honor_critical_scope);
    let Err(initial_error) = initial else {
        if bytes != 0 {
            reclaim_pending_clean_cache_headroom();
        }
        return initial;
    };
    let physical_pressure_failure = initial_error
        .message()
        .starts_with("process memory pressure limit exceeded");
    if !record_rejection || bytes == 0 {
        return Err(initial_error);
    }
    if physical_pressure_failure {
        // Collect on the allocating thread before it waits for the global
        // admission lock. Mimalloc cannot collect a peer's heap: parking all
        // spilling workers behind this lock otherwise strands their free
        // pages while the coordinator tries to reserve mandatory spill state.
        crate::engine::memory_reclaimer::reclaim_current_thread_allocator();
        if charge_controlled_once(category, bytes, false, honor_critical_scope).is_ok() {
            RECLAIM_RETRY_SUCCESSES.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
    }
    if storage_latch_admission_active() {
        // Retry once to preserve ordinary rejection accounting and allow a
        // peer's release. Never acquire the reclaim admission lock here.
        return charge_controlled_once(category, bytes, true, honor_critical_scope);
    }

    let admission = RECLAIM_ADMISSION.get_or_init(|| Mutex::new(()));
    let _guard = admission
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // A peer may have released memory while this caller waited.
    if charge_controlled_once(category, bytes, false, honor_critical_scope).is_ok() {
        RECLAIM_RETRY_SUCCESSES.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }

    let mut cache_reclaimed = 0u64;
    let mut buffer_reclaimed = 0u64;
    let mut allocator_reclaimed = 0u64;
    let mut physical_pressure_failure = physical_pressure_failure;
    for pass in 0..ADMISSION_RECLAIM_MAX_PASSES {
        RECLAIM_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
        let reclaimed =
            reclaim_controlled_admission_once(category, bytes, physical_pressure_failure, None)?;
        cache_reclaimed = cache_reclaimed.saturating_add(reclaimed.cache_bytes);
        buffer_reclaimed = buffer_reclaimed.saturating_add(reclaimed.buffer_bytes);
        allocator_reclaimed = allocator_reclaimed.saturating_add(reclaimed.allocator_bytes);

        let final_pass = pass + 1 == ADMISSION_RECLAIM_MAX_PASSES;
        let result = charge_controlled_once(category, bytes, final_pass, honor_critical_scope);
        match result {
            Ok(()) => {
                RECLAIM_RETRY_SUCCESSES.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            Err(error)
                if !final_pass
                    && error
                        .message()
                        .starts_with("process memory pressure limit exceeded") =>
            {
                physical_pressure_failure = true;
                if crate::engine::memory_reclaimer::pending_writeback_pressure_bytes() != 0 {
                    // Spill submits asynchronous writeback. Give those pages
                    // a bounded chance to become clean instead of exhausting
                    // every retry before the first I/O can finish. Neither
                    // the logical nor physical admission ceiling changes.
                    std::thread::sleep(std::time::Duration::from_millis(1 << pass.min(6)));
                }
            }
            Err(_error) if !final_pass => {
                // Record the stable logical/hard failure exactly once. A peer
                // release between these two calls can still make the retry
                // succeed, which is preferable to returning a stale error.
                return charge_controlled_once(category, bytes, true, honor_critical_scope)
                    .map_err(|retry_error| {
                        decorate_reclaim_error(
                            retry_error,
                            cache_reclaimed,
                            buffer_reclaimed,
                            allocator_reclaimed,
                        )
                    });
            }
            Err(error) => {
                if error.is(crate::error::catalog::ER_OUTOFMEMORY) {
                    RECLAIM_OOM_AFTER_RETRY.fetch_add(1, Ordering::Relaxed);
                }
                return Err(decorate_reclaim_error(
                    error,
                    cache_reclaimed,
                    buffer_reclaimed,
                    allocator_reclaimed,
                ));
            }
        }
    }
    unreachable!("bounded admission loop returns on every result")
}

#[derive(Clone, Copy, Debug, Default)]
struct AdmissionReclaimed {
    cache_bytes: u64,
    buffer_bytes: u64,
    allocator_bytes: u64,
}

fn reclaim_controlled_admission_once(
    category: ControlledMemoryCategory,
    bytes: usize,
    physical_pressure_failure: bool,
    database: Option<&Arc<crate::storage::Database>>,
) -> Result<AdmissionReclaimed, QueryError> {
    let before_cache = controlled_memory_snapshot().0;
    crate::engine::cache::reclaim_global_caches();
    let after_cache = controlled_memory_snapshot().0;
    let cache_reclaimed = before_cache.saturating_sub(after_cache) as u64;
    RECLAIM_CACHE_BYTES.fetch_add(cache_reclaimed, Ordering::Relaxed);

    let mut buffer_reclaimed = 0u64;
    if category != ControlledMemoryCategory::BufferRaw {
        let owned_database = database.is_none().then(current_reclaim_database).flatten();
        if let Some(database) = database.or(owned_database.as_ref()) {
            let (used, _, categories, _) = controlled_memory_snapshot();
            let floor =
                (effective_memory_limit_bytes() / 50).clamp(16 * 1024 * 1024, 256 * 1024 * 1024);
            let physical = crate::engine::memory_reclaimer::process_headroom_reclaim_bytes(
                bytes.saturating_add(ADMISSION_RECLAIM_HEADROOM_BYTES),
                operational_memory_limit_bytes(),
            );
            // Small allocations should first replace raw pages. Retiring a
            // whole navigator for an ordinary parser/scratch charge causes
            // repeated graph reconstruction between sequential queries.
            if used
                .saturating_sub(categories[0])
                .saturating_add(bytes)
                .saturating_add(floor)
                > operational_memory_limit_bytes()
                || physical > categories[0].saturating_sub(floor)
            {
                database.reclaim_vector_navigation_caches();
            }
            let before_buffer = controlled_memory_snapshot().0;
            let (_, _, categories, _) = controlled_memory_snapshot();
            let raw = categories[0];
            let non_buffer = before_buffer.saturating_sub(raw);
            let effective = effective_memory_limit_bytes();
            let min_buffer = (effective / 50)
                .clamp(16 * 1024 * 1024, 256 * 1024 * 1024)
                .min(raw);
            let desired = logical_admission_reclaim_target(
                operational_memory_limit_bytes(),
                before_buffer,
                bytes,
            );
            let physical_reclaim = crate::engine::memory_reclaimer::process_headroom_reclaim_bytes(
                bytes,
                operational_memory_limit_bytes(),
            );
            let physical_reclaim =
                admission_reclaim_bytes_with_headroom(physical_reclaim, physical_pressure_failure);
            let target = admission_reclaim_target(
                before_buffer,
                non_buffer,
                min_buffer,
                desired,
                physical_reclaim,
            );
            if let Err(error) = database.reclaim_for_query_admission(target) {
                if !error.is(crate::error::catalog::ER_WRONG_VALUE_FOR_VAR) {
                    return Err(error);
                }
            }
            let after_buffer = controlled_memory_snapshot().0;
            buffer_reclaimed = before_buffer.saturating_sub(after_buffer) as u64;
            RECLAIM_BUFFER_BYTES.fetch_add(buffer_reclaimed, Ordering::Relaxed);
        }
    }

    let allocator_before = crate::engine::memory_reclaimer::current_private_footprint_bytes();
    let _ = crate::engine::memory_reclaimer::release_allocator_cached_memory();
    reset_pressure_allowance_after_reclaim();
    let allocator_after = crate::engine::memory_reclaimer::current_private_footprint_bytes();
    let allocator_reclaimed = match (allocator_before, allocator_after) {
        (Some(before), Some(after)) => before.saturating_sub(after),
        _ => 0,
    };
    RECLAIM_ALLOCATOR_BYTES.fetch_add(allocator_reclaimed, Ordering::Relaxed);

    if physical_pressure_failure
        && crate::engine::memory_reclaimer::pending_writeback_pressure_bytes() != 0
        && crate::engine::memory_reclaimer::process_headroom_reclaim_bytes(
            bytes,
            operational_memory_limit_bytes(),
        ) != 0
    {
        // Preserve short-lived cached spills unless optional-frame and
        // allocator reclamation cannot restore the physical hard ceiling.
        super::spill_quota::request_process_writeback();
    }

    Ok(AdmissionReclaimed {
        cache_bytes: cache_reclaimed,
        buffer_bytes: buffer_reclaimed,
        allocator_bytes: allocator_reclaimed,
    })
}

fn decorate_reclaim_error(
    error: QueryError,
    cache_reclaimed: u64,
    buffer_reclaimed: u64,
    allocator_reclaimed: u64,
) -> QueryError {
    if error.is(crate::error::catalog::ER_OUTOFMEMORY) {
        let controlled = controlled_memory_snapshot().0;
        error.map_message(|message| {
            format!(
                "{message}; reclaim before 1037 released cache={} buffer={} allocator={} bytes, controlled={} / sql_hard_limit={}",
                cache_reclaimed,
                buffer_reclaimed,
                allocator_reclaimed,
                controlled,
                operational_memory_limit_bytes(),
            )
        })
    } else {
        error
    }
}

#[inline]
fn logical_admission_reclaim_target(limit: usize, used: usize, requested: usize) -> usize {
    let target = limit.saturating_sub(requested);
    if used <= target {
        return target;
    }
    // A page prefetch/cleaner can refill the exact reclaimed deficit before
    // the allocating thread retries. Retain a bounded admission window for
    // mandatory work, just as physical-pressure reclamation already does.
    // This returns cache residency; it does not increase either hard ceiling.
    target.saturating_sub(
        (limit / 32).clamp(crate::storage::PAGE_SIZE, ADMISSION_RECLAIM_HEADROOM_BYTES),
    )
}

#[inline]
fn admission_reclaim_target(
    controlled_before: usize,
    non_buffer: usize,
    min_buffer: usize,
    logical_target: usize,
    physical_reclaim: usize,
) -> usize {
    logical_target
        .min(controlled_before.saturating_sub(physical_reclaim))
        .max(non_buffer.saturating_add(min_buffer))
}

#[inline]
fn admission_reclaim_bytes_with_headroom(
    physical_reclaim: usize,
    physical_pressure_failure: bool,
) -> usize {
    if physical_pressure_failure {
        physical_reclaim.max(ADMISSION_RECLAIM_HEADROOM_BYTES)
    } else {
        physical_reclaim
    }
}

/// Reclaim optional database memory before constructing a new protocol
/// session. Connection buffers are reserved before the first command installs
/// a query budget, so the ordinary admission path has no database back-
/// reference at this point. Without this hook, a completed analytical query
/// can leave the listener unable to allocate even the initial status row.
pub(crate) fn reclaim_for_connection_admission(
    database: &Arc<crate::storage::Database>,
    requested_bytes: usize,
) -> Result<(), QueryError> {
    let pressure_limit = managed_memory_limit_bytes();
    if crate::engine::memory_reclaimer::process_headroom_reclaim_bytes(
        requested_bytes,
        pressure_limit,
    ) == 0
    {
        return Ok(());
    }

    let admission = RECLAIM_ADMISSION.get_or_init(|| Mutex::new(()));
    let _guard = admission
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    RECLAIM_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
    reclaim_controlled_admission_once(
        ControlledMemoryCategory::ProtocolTransient,
        requested_bytes,
        true,
        Some(database),
    )?;
    Ok(())
}

/// Service a sampler hint after its lock has been released, with no storage
/// latch or admission lock held. A busy peer/background worker services the
/// same hint; callers never wait behind a cache-pressure eviction.
fn reclaim_pending_clean_cache_headroom() {
    if !crate::engine::memory_reclaimer::cache_headroom_requested()
        || storage_latch_admission_active()
        || !crate::engine::memory_reclaimer::enabled()
    {
        return;
    }
    let Some(database) = current_reclaim_database() else {
        return;
    };
    let admission = RECLAIM_ADMISSION.get_or_init(|| Mutex::new(()));
    let Ok(_guard) = admission.try_lock() else {
        return;
    };
    let requested = crate::engine::memory_reclaimer::take_cache_headroom_request();
    if requested != 0 {
        let deficit = crate::engine::memory_reclaimer::cache_headroom_reclaim_bytes(requested);
        reclaim_clean_cache_headroom(&database, deficit);
    }
}

/// Preserve a useful cache floor and all pinned/dirty state. Physical cache
/// pressure is advisory: failure to find a clean victim cannot manufacture
/// a 1037 while ordinary hard admission still has room.
fn reclaim_clean_cache_headroom(database: &Arc<crate::storage::Database>, deficit: usize) {
    if deficit == 0 {
        return;
    }
    let (before, _, categories, _) = controlled_memory_snapshot();
    let raw = categories[0];
    let floor = advisory_cache_floor(
        controlled_buffer_limit_bytes(),
        effective_memory_limit_bytes(),
    )
    .min(raw);
    let reclaim = advisory_cache_reclaim_bytes(raw, floor, deficit);
    if reclaim == 0 {
        return;
    }
    RECLAIM_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
    let _ = database.reclaim_clean_for_query_admission(before.saturating_sub(reclaim));
    let released = before.saturating_sub(controlled_memory_snapshot().0);
    RECLAIM_BUFFER_BYTES.fetch_add(released as u64, Ordering::Relaxed);
    // Raw-page discard returns owned OS pages before deallocation. The old
    // physical allowance stays conservative after a release: resetting it
    // here would turn each cache eviction into another OS sample/eviction.
}

fn advisory_cache_floor(configured_buffer: usize, effective: usize) -> usize {
    // A cache hint is not an emergency: retain a fifth of the configured
    // cache for frequently reused pages. Hard admission can still reclaim
    // down to its separate, smaller floor when mandatory memory is short.
    let ceiling = (effective / 10).clamp(16 * 1024 * 1024, 1024 * 1024 * 1024);
    (configured_buffer / 5).clamp(16 * 1024 * 1024, ceiling)
}

fn advisory_cache_reclaim_bytes(raw: usize, floor: usize, deficit: usize) -> usize {
    let available = raw.saturating_sub(floor);
    // Do not run a CLOCK sweep whenever a single new page crosses the
    // retained working set. Leave a bounded refill window, matching the
    // physical sampler's maximum allowance on large caches.
    let refill = (floor / 16).clamp(crate::storage::PAGE_SIZE, 64 * 1024 * 1024);
    if available < refill {
        return 0;
    }
    deficit.min(available)
}

/// External processes and buffered I/O can grow inside the same cgroup
/// without a governed allocation. Distinguish hard non-reclaimable pressure
/// from total-residency cache hints, including while statements stay active.
pub(crate) fn spawn_memory_pressure_worker(
    database: Arc<crate::storage::Database>,
) -> crate::engine::background::BackgroundWorker {
    crate::engine::background::BackgroundWorker::spawn("mydb-memory-pressure", move |shutdown| {
        while !shutdown.wait_timeout(std::time::Duration::from_millis(50)) {
            if !crate::engine::memory_reclaimer::enabled() {
                continue;
            }
            let hard_deficit = crate::engine::memory_reclaimer::process_headroom_reclaim_bytes(
                0,
                operational_memory_limit_bytes(),
            );
            let cache_deficit = crate::engine::memory_reclaimer::cache_headroom_reclaim_bytes(0);
            if hard_deficit == 0 && cache_deficit == 0 {
                continue;
            }
            let admission = RECLAIM_ADMISSION.get_or_init(|| Mutex::new(()));
            let Ok(_guard) = admission.try_lock() else {
                continue;
            };
            if hard_deficit == 0 {
                reclaim_clean_cache_headroom(&database, cache_deficit);
                continue;
            }
            // Aim below the trigger to leave room for concurrent growth and
            // avoid trimming on every sample. This is a reclaim target, never
            // a change to configured limits or buffer-pool capacity.
            let headroom = effective_memory_limit_bytes() / 5;
            RECLAIM_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
            let _ = reclaim_controlled_admission_once(
                ControlledMemoryCategory::ProtocolTransient,
                headroom,
                true,
                Some(&database),
            );
        }
    })
}

fn reset_pressure_allowance_after_reclaim() {
    crate::engine::memory_reclaimer::reset_pressure_allowance();
}

fn charge_controlled(category: ControlledMemoryCategory, bytes: usize) -> Result<(), QueryError> {
    charge_controlled_impl(category, bytes, true, true)
}

fn release_controlled(category: ControlledMemoryCategory, bytes: usize) {
    let released = saturating_atomic_sub(category_counter(category), bytes);
    if released != 0 {
        record_category_free(category, released);
    }
    saturating_atomic_sub(&CONTROLLED_MEMORY_USED, released);
    if category == ControlledMemoryCategory::BufferRaw {
        saturating_atomic_sub(&CONTROLLED_BUFFER_USED, released);
    }
}

#[derive(Debug)]
pub struct ControlledMemoryCharge {
    category: ControlledMemoryCategory,
    bytes: usize,
}

impl ControlledMemoryCharge {
    pub fn try_new(category: ControlledMemoryCategory, bytes: usize) -> Result<Self, QueryError> {
        Self::try_new_recoverable(category, bytes).map_err(terminal_oom)
    }

    /// Reclaim before rejecting, but let the caller handle failure without
    /// latching query OOM. Derived storage may release its workspace and
    /// persist an invalidation instead; if that fails it must propagate the
    /// error. Unlike optional admission, this honors critical compensation
    /// scopes and records a rejection after exhausting bounded reclamation.
    pub(crate) fn try_new_recoverable(
        category: ControlledMemoryCategory,
        bytes: usize,
    ) -> Result<Self, QueryError> {
        charge_controlled(category, bytes)?;
        Ok(Self { category, bytes })
    }

    /// Best-effort policy admission. Capacity pressure is an expected outcome
    /// for optional caches and buffer replacement probes, so it does not
    /// increment the hard allocation-rejection counter.
    pub(crate) fn try_new_optional(
        category: ControlledMemoryCategory,
        bytes: usize,
    ) -> Option<Self> {
        // Optional state may not consume rollback's emergency slice even when
        // its caller happens to run inside a critical scope.
        charge_controlled_impl(category, bytes, false, false)
            .ok()
            .map(|()| Self { category, bytes })
    }

    /// Optional read accelerators may replace raw pages with a denser
    /// representation. Keep admission bounded and preserve the buffer floor;
    /// refusal still leaves the existing durable read path available.
    pub(crate) fn try_new_optional_with_cache_reclaim(
        category: ControlledMemoryCategory,
        bytes: usize,
        mut reclaim: impl FnMut(usize),
    ) -> Option<Self> {
        if let Some(charge) = Self::try_new_optional(category, bytes) {
            return Some(charge);
        }
        if storage_latch_admission_active() {
            return None;
        }
        // A completed bulk build can release its leases while allocator
        // arenas still retain the pages. Return those pages on their owning
        // workers before deciding that a large optional image must spill.
        if bytes >= ADMISSION_RECLAIM_HEADROOM_BYTES {
            crate::engine::memory_reclaimer::release_allocator_cached_memory();
            crate::engine::memory_reclaimer::reset_pressure_allowance();
            if let Some(charge) = Self::try_new_optional(category, bytes) {
                return Some(charge);
            }
        }
        let admission = RECLAIM_ADMISSION.get_or_init(|| Mutex::new(()));
        let _guard = admission.try_lock().ok()?;
        for _ in 0..3 {
            let (before, _, categories, _) = controlled_memory_snapshot();
            let non_buffer = before.saturating_sub(categories[0]);
            let floor =
                (effective_memory_limit_bytes() / 50).clamp(16 * 1024 * 1024, 256 * 1024 * 1024);
            let physical = crate::engine::memory_reclaimer::process_headroom_reclaim_bytes(
                bytes.saturating_add(ADMISSION_RECLAIM_HEADROOM_BYTES),
                operational_memory_limit_bytes(),
            );
            let target = operational_memory_limit_bytes()
                .saturating_sub(bytes)
                .saturating_sub(ADMISSION_RECLAIM_HEADROOM_BYTES)
                .min(before.saturating_sub(physical))
                .max(non_buffer.saturating_add(floor));
            if target >= before {
                return None;
            }
            RECLAIM_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
            reclaim(target);
            RECLAIM_BUFFER_BYTES.fetch_add(
                before.saturating_sub(controlled_memory_snapshot().0) as u64,
                Ordering::Relaxed,
            );
            if let Some(charge) = Self::try_new_optional(category, bytes) {
                return Some(charge);
            }
        }
        None
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn try_grow(&mut self, extra: usize) -> Result<(), QueryError> {
        self.try_grow_recoverable(extra).map_err(terminal_oom)
    }

    /// Derived indexes may release failed accelerator state and execute an exact scan.
    pub(crate) fn try_grow_recoverable(&mut self, extra: usize) -> Result<(), QueryError> {
        if extra == 0 {
            return Ok(());
        }
        charge_controlled(self.category, extra)?;
        self.bytes = self.bytes.saturating_add(extra);
        Ok(())
    }

    /// Transfer ownership without releasing/reacquiring global admission.
    /// A phase can lend prepaid capacity to workers even at the hard limit.
    pub(crate) fn split(&mut self, bytes: usize) -> Self {
        assert!(
            bytes <= self.bytes,
            "memory lease split exceeds reservation"
        );
        self.bytes -= bytes;
        Self {
            category: self.category,
            bytes,
        }
    }

    pub(crate) fn shrink(&mut self, bytes: usize) {
        let bytes = bytes.min(self.bytes);
        release_controlled(self.category, bytes);
        self.bytes -= bytes;
    }

    /// Move already-admitted capacity with its allocations. No temporary
    /// release/reacquisition window is introduced at the hard limit.
    pub(crate) fn absorb(&mut self, other: &mut Self) {
        assert_eq!(
            self.category, other.category,
            "memory lease categories differ"
        );
        self.bytes = self.bytes.saturating_add(other.bytes);
        other.bytes = 0;
    }

    pub(crate) fn try_grow_optional(&mut self, extra: usize) -> bool {
        if extra == 0 {
            return true;
        }
        if charge_controlled_impl(self.category, extra, false, false).is_err() {
            return false;
        }
        self.bytes = self.bytes.saturating_add(extra);
        true
    }
}

impl Drop for ControlledMemoryCharge {
    fn drop(&mut self) {
        release_controlled(self.category, self.bytes);
    }
}

pub fn controlled_memory_snapshot() -> (usize, usize, [usize; 16], u64) {
    (
        CONTROLLED_MEMORY_USED.load(Ordering::Relaxed),
        CONTROLLED_MEMORY_PEAK.load(Ordering::Relaxed),
        [
            CONTROLLED_BUFFER_RAW.load(Ordering::Relaxed),
            CONTROLLED_STORAGE_SCRATCH.load(Ordering::Relaxed),
            CONTROLLED_EXECUTOR.load(Ordering::Relaxed),
            CONTROLLED_DERIVED_CACHE.load(Ordering::Relaxed),
            CONTROLLED_PARTITION_DML_BATCH.load(Ordering::Relaxed),
            CONTROLLED_PARTITION_DDL.load(Ordering::Relaxed),
            CONTROLLED_PARTITION_STATS.load(Ordering::Relaxed),
            CONTROLLED_DDL_COPY.load(Ordering::Relaxed),
            CONTROLLED_SYSTEM_METADATA.load(Ordering::Relaxed),
            CONTROLLED_TEMPORAL_HISTORY.load(Ordering::Relaxed),
            CONTROLLED_PROTOCOL_TRANSIENT.load(Ordering::Relaxed),
            CONTROLLED_BINLOG_TRANSIENT.load(Ordering::Relaxed),
            CONTROLLED_PARSER_TRANSIENT.load(Ordering::Relaxed),
            CONTROLLED_NATIVE_REPLICATION_TRANSIENT.load(Ordering::Relaxed),
            CONTROLLED_PERFORMANCE_SCHEMA.load(Ordering::Relaxed),
            CONTROLLED_DDL_ONLINE_LOG.load(Ordering::Relaxed),
        ],
        CONTROLLED_REJECTIONS.load(Ordering::Relaxed),
    )
}

pub fn controlled_memory_rejections_by_category() -> [u64; 16] {
    std::array::from_fn(|slot| CONTROLLED_REJECTIONS_BY_CATEGORY[slot].load(Ordering::Relaxed))
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PfsMemoryCategorySnapshot {
    pub alloc_count: u64,
    pub free_count: u64,
    pub peak_count: u64,
    pub alloc_bytes: u64,
    pub free_bytes: u64,
    pub current_bytes: u64,
    pub peak_bytes: u64,
}

pub(crate) fn pfs_memory_category_snapshots() -> [PfsMemoryCategorySnapshot; 16] {
    let (_, _, current, _) = controlled_memory_snapshot();
    let mut snapshots = std::array::from_fn(|slot| PfsMemoryCategorySnapshot {
        peak_count: CONTROLLED_CATEGORY_PEAK_COUNT[slot].load(Ordering::Relaxed),
        current_bytes: current[slot] as u64,
        peak_bytes: CONTROLLED_CATEGORY_PEAK_BYTES[slot].load(Ordering::Relaxed) as u64,
        ..PfsMemoryCategorySnapshot::default()
    });
    // Like the other status fields, this is a live, non-transactional
    // snapshot. Once writers quiesce the sum is exact, including threads
    // that have exited and allocations freed on a different worker.
    for worker in &CATEGORY_ACTIVITY {
        for (snapshot, activity) in snapshots.iter_mut().zip(&worker.0) {
            snapshot.alloc_count = snapshot
                .alloc_count
                .wrapping_add(activity.alloc_count.load(Ordering::Relaxed));
            snapshot.free_count = snapshot
                .free_count
                .wrapping_add(activity.free_count.load(Ordering::Relaxed));
            snapshot.alloc_bytes = snapshot
                .alloc_bytes
                .wrapping_add(activity.alloc_bytes.load(Ordering::Relaxed));
            snapshot.free_bytes = snapshot
                .free_bytes
                .wrapping_add(activity.free_bytes.load(Ordering::Relaxed));
        }
    }
    snapshots
}

pub fn set_controlled_buffer_limit_pages(pages: u64) {
    let raw_bytes = usize::try_from(pages.saturating_mul(crate::storage::PAGE_SIZE as u64))
        .unwrap_or(usize::MAX);
    CONTROLLED_BUFFER_RAW_LIMIT_BYTES.store(raw_bytes, Ordering::Release);
    refresh_controlled_buffer_total_limit();
}

fn refresh_controlled_buffer_total_limit() {
    let raw = controlled_buffer_raw_limit_bytes();
    CONTROLLED_BUFFER_LIMIT_BYTES
        .store(raw.min(operational_memory_limit_bytes()), Ordering::Release);
}

pub(crate) fn controlled_buffer_raw_limit_bytes() -> usize {
    let configured = CONTROLLED_BUFFER_RAW_LIMIT_BYTES.load(Ordering::Acquire);
    if configured > 0 {
        configured
    } else {
        let target = memory_target_bytes();
        let explicit_pct = parse_pct_env("MYDB_BUFFER_POOL_PCT", 0) as usize;
        if explicit_pct == 0 {
            crate::storage::default_buffer_pool_bytes(target as u64).min(usize::MAX as u64) as usize
        } else {
            target.saturating_mul(explicit_pct) / 100
        }
    }
}

pub(crate) fn controlled_buffer_limit_bytes() -> usize {
    let configured = CONTROLLED_BUFFER_LIMIT_BYTES.load(Ordering::Acquire);
    if configured > 0 {
        return configured;
    }
    controlled_buffer_raw_limit_bytes().min(operational_memory_limit_bytes())
}

pub(crate) fn controlled_buffer_used_bytes() -> usize {
    CONTROLLED_BUFFER_USED.load(Ordering::Acquire)
}

pub fn validate_controlled_memory_split(buffer_pool_pages: u64) -> Result<(), QueryError> {
    validate_controlled_memory_values(
        buffer_pool_pages,
        executor_memory_target_bytes(),
        operational_memory_limit_bytes(),
    )
}

fn validate_controlled_memory_values(
    buffer_pool_pages: u64,
    _executor_bytes: usize,
    target_bytes: usize,
) -> Result<(), QueryError> {
    let raw = buffer_pool_pages.saturating_mul(crate::storage::PAGE_SIZE as u64) as u128;
    let operational = target_bytes as u128;
    if raw > operational {
        return Err(crate::storage::error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
            format!(
                "configured buffer pool ({} bytes) exceeds the operational managed-memory limit ({} bytes)",
                raw, operational
            ),
        ));
    }
    Ok(())
}

fn executor_target_for_memory_target(target_bytes: usize) -> usize {
    parse_mb_env("MYDB_EXECUTOR_MEMORY_TARGET").unwrap_or_else(|| {
        let pct = parse_pct_env("MYDB_EXECUTOR_MEMORY_PCT", DEFAULT_EXECUTOR_MEMORY_PCT) as usize;
        target_bytes
            .saturating_mul(pct)
            .saturating_div(100)
            .max(MIN_EXECUTOR_TARGET_BYTES)
    })
}

pub fn validate_memory_target_mb_with_buffer(
    mb: usize,
    buffer_pool_pages: u64,
) -> Result<(), QueryError> {
    if mb == 0 {
        return Ok(());
    }
    validate_controlled_memory_values(
        buffer_pool_pages,
        executor_target_for_memory_target(mb.saturating_mul(1024 * 1024)),
        operational_memory_limit_bytes(),
    )
}

pub fn validate_executor_target_mb_with_buffer(
    mb: usize,
    buffer_pool_pages: u64,
) -> Result<(), QueryError> {
    if mb == 0 {
        return Ok(());
    }
    validate_controlled_memory_values(
        buffer_pool_pages,
        mb.saturating_mul(1024 * 1024)
            .max(MIN_EXECUTOR_TARGET_BYTES),
        operational_memory_limit_bytes(),
    )
}

fn parse_mb_env(name: &str) -> Option<usize> {
    crate::util::env::var(name)
        .ok()
        .and_then(|value| crate::util::system_memory::parse_mebibyte_or_size_bytes(&value))
        .and_then(|bytes| usize::try_from(bytes).ok())
}

fn parse_pct_env(name: &str, default: u32) -> u32 {
    crate::util::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|n| *n > 0 && *n <= 100)
        .unwrap_or(default)
}

/// Stable hard-limit base: the lower of host RAM and cgroup memory.max.
/// Refreshed on slow-path admission and statement boundaries so runtime
/// cgroup changes tighten/relax the 80%/90% limits without filesystem I/O on
/// every charge.
pub fn effective_memory_limit_bytes() -> usize {
    let current = EFFECTIVE_MEMORY_LIMIT_BYTES.load(Ordering::Acquire);
    if current != 0 {
        return current;
    }
    refresh_effective_memory_limit_bytes()
}

#[cfg(test)]
pub(crate) fn with_test_effective_memory_limit<T>(bytes: usize, f: impl FnOnce() -> T) -> T {
    struct Restore(usize);
    impl Drop for Restore {
        fn drop(&mut self) {
            EFFECTIVE_MEMORY_LIMIT_BYTES.store(self.0, Ordering::Release);
        }
    }
    let _restore = Restore(EFFECTIVE_MEMORY_LIMIT_BYTES.swap(bytes, Ordering::AcqRel));
    f()
}

pub(crate) fn refresh_effective_memory_limit_bytes() -> usize {
    let detected = crate::util::system_memory::effective_memory_limit_bytes()
        .and_then(|bytes| usize::try_from(bytes).ok())
        .filter(|bytes| *bytes != 0)
        .unwrap_or(1024 * 1024 * 1024);
    let previous = EFFECTIVE_MEMORY_LIMIT_BYTES.swap(detected, Ordering::AcqRel);
    if previous != 0 && previous != detected {
        crate::engine::memory_reclaimer::reset_pressure_allowance();
    }
    detected
}

/// Advisory working-set target. Explicit MYDB_MEMORY_TARGET controls cache
/// sizing and spill watermarks only; hard SQL/process ceilings always derive
/// from [`effective_memory_limit_bytes`]. When unset the advisory target is
/// half the effective host/cgroup limit.
pub fn memory_target_bytes() -> usize {
    let cur = MEMORY_TARGET_BYTES.load(Ordering::Relaxed);
    if cur != 0 {
        return cur.min(effective_memory_limit_bytes());
    }
    let explicit = parse_mb_env("MYDB_MEMORY_TARGET");
    let init = explicit.unwrap_or_else(|| {
        crate::util::system_memory::default_memory_target_mb().saturating_mul(1024 * 1024) as usize
    });
    MEMORY_TARGET_EXPLICIT.store(explicit.is_some(), Ordering::Release);
    MEMORY_TARGET_BYTES
        .compare_exchange(0, init, Ordering::Relaxed, Ordering::Relaxed)
        .ok();
    MEMORY_TARGET_BYTES
        .load(Ordering::Relaxed)
        .min(effective_memory_limit_bytes())
}

pub fn configured_memory_target_bytes() -> Option<usize> {
    let _ = memory_target_bytes();
    MEMORY_TARGET_EXPLICIT
        .load(Ordering::Acquire)
        .then(|| MEMORY_TARGET_BYTES.load(Ordering::Acquire))
}

#[inline]
fn managed_limit_for_target(target: usize) -> usize {
    target.saturating_mul(MANAGED_MEMORY_PCT) / 100
}

#[inline]
fn operational_limit_for_target(target: usize) -> usize {
    target.saturating_mul(OPERATIONAL_MEMORY_PCT) / 100
}

/// Critical rollback/process hard ceiling. The remaining 10% of effective
/// host/cgroup memory belongs to stacks/runtime/allocator.
pub fn managed_memory_limit_bytes() -> usize {
    managed_limit_for_target(effective_memory_limit_bytes())
}

/// Ordinary admission ceiling. The gap to the managed ceiling is reserved for
/// rollback/compensation so a recoverable HY001 cannot become MVCC fail-stop.
pub fn operational_memory_limit_bytes() -> usize {
    operational_limit_for_target(effective_memory_limit_bytes())
}

pub(crate) fn performance_schema_memory_limit_bytes() -> usize {
    effective_memory_limit_bytes().saturating_mul(PERFORMANCE_SCHEMA_MEMORY_PCT) / 100
}

/// Executor work-area memory target. Drives [`WorkAreaManager`].
/// Lazy-initialised from (in priority order):
///
///   1. `MYDB_EXECUTOR_MEMORY_TARGET` (MiB)
///   2. `MYDB_MEMORY_TARGET × MYDB_EXECUTOR_MEMORY_PCT / 100`,
///      floored at `MIN_EXECUTOR_TARGET_BYTES` (16 MiB)
/// Tiny `MYDB_MEMORY_TARGET` values clamp up to 16 MiB.
pub fn executor_memory_target_bytes() -> usize {
    let cur = EXECUTOR_MEMORY_TARGET_BYTES.load(Ordering::Relaxed);
    if cur != 0 {
        return cur;
    }
    let init = executor_target_for_memory_target(memory_target_bytes());
    EXECUTOR_MEMORY_TARGET_BYTES
        .compare_exchange(0, init, Ordering::Relaxed, Ordering::Relaxed)
        .ok();
    EXECUTOR_MEMORY_TARGET_BYTES.load(Ordering::Relaxed)
}

/// Update the executor memory target at runtime (MiB).
pub fn set_executor_memory_target_mb(mb: usize) -> Result<(), QueryError> {
    if mb == 0 {
        return Err(crate::storage::error(
            crate::error::catalog::ER_WRONG_TYPE_FOR_VAR,
            "variable 'mydb_executor_memory_target_mb' cannot be set to 0",
        ));
    }
    let bytes = mb
        .saturating_mul(1024 * 1024)
        .max(MIN_EXECUTOR_TARGET_BYTES);
    EXECUTOR_MEMORY_TARGET_BYTES.store(bytes, Ordering::Relaxed);
    // Cascade to query / process defaults if those aren't explicitly
    // pinned via env — see `query_mem_limit_bytes`.
    QUERY_MEM_LIMIT_BYTES.store(0, Ordering::Relaxed);
    PROCESS_MEM_LIMIT_BYTES.store(0, Ordering::Relaxed);
    Ok(())
}

/// Floor used by [`crate::engine::executor::work_area::WorkAreaManager::set_target_bytes`]
/// to avoid setting a pathologically small target at runtime.
pub fn min_executor_target_bytes() -> usize {
    MIN_EXECUTOR_TARGET_BYTES
}

/// Update the advisory working-set target at runtime (MiB). Hard SQL/process
/// ceilings remain 80%/90% of effective OS/cgroup memory.
pub fn set_memory_target_mb(mb: usize) -> Result<(), QueryError> {
    if mb == 0 {
        return Err(crate::storage::error(
            crate::error::catalog::ER_WRONG_TYPE_FOR_VAR,
            "variable 'mydb_memory_target_mb' cannot be set to 0",
        ));
    }
    let bytes = mb.saturating_mul(1024 * 1024);
    MEMORY_TARGET_BYTES.store(bytes, Ordering::Relaxed);
    MEMORY_TARGET_EXPLICIT.store(true, Ordering::Release);
    crate::engine::memory_reclaimer::reset_pressure_allowance();
    refresh_controlled_buffer_total_limit();
    EXECUTOR_MEMORY_TARGET_BYTES.store(0, Ordering::Relaxed);
    QUERY_MEM_LIMIT_BYTES.store(0, Ordering::Relaxed);
    PROCESS_MEM_LIMIT_BYTES.store(0, Ordering::Relaxed);
    Ok(())
}

// (legacy `read_env_mb` removed; see `parse_mb_env` above)

/// Per-query memory budget in bytes. Resolved on first access:
///
///   1. `MYDB_QUERY_MEM_LIMIT_MB` env (legacy direct override) if set
///   2. `executor_memory_target_bytes() × MYDB_QUERY_MEM_QUOTA_PCT / 100`,
///      floored at `MIN_QUERY_LIMIT_BYTES` (8 MiB)
///
/// Default `quota_pct = 42` → a single query receives 42% of the executor
/// budget. At an explicit 1 GiB `MYDB_MEMORY_TARGET` this resolves to ~129 MiB.
/// Planning uses a stable baseline instead; this value is enforced by the
/// executor and controls runtime spill/1037 behavior.
pub fn query_mem_limit_bytes() -> usize {
    let cur = QUERY_MEM_LIMIT_BYTES.load(Ordering::Relaxed);
    if cur != 0 {
        return cur;
    }
    let init = parse_mb_env("MYDB_QUERY_MEM_LIMIT_MB").unwrap_or_else(|| {
        let pct = parse_pct_env("MYDB_QUERY_MEM_QUOTA_PCT", DEFAULT_QUERY_MEM_QUOTA_PCT) as usize;
        derived_query_mem_limit_bytes(executor_memory_target_bytes(), pct)
    });
    QUERY_MEM_LIMIT_BYTES
        .compare_exchange(0, init, Ordering::Relaxed, Ordering::Relaxed)
        .ok();
    QUERY_MEM_LIMIT_BYTES.load(Ordering::Relaxed)
}

fn derived_query_mem_limit_bytes(executor_target_bytes: usize, quota_pct: usize) -> usize {
    executor_target_bytes
        .saturating_mul(quota_pct)
        .saturating_div(100)
        .max(MIN_QUERY_LIMIT_BYTES)
}

/// Update the per-query memory budget at runtime (in MiB).
pub fn set_query_mem_limit_mb(mb: usize) -> Result<(), QueryError> {
    if mb == 0 {
        return Err(crate::storage::error(
            crate::error::catalog::ER_WRONG_TYPE_FOR_VAR,
            "variable 'mydb_query_mem_limit_mb' cannot be set to 0",
        ));
    }
    let bytes = mb.saturating_mul(1024 * 1024);
    QUERY_MEM_LIMIT_BYTES.store(bytes, Ordering::Relaxed);
    Ok(())
}

/// Process-wide spill watermark (in bytes) for all in-flight query operator
/// state combined. Crossing it asks spillable operators to spill; it is not a
/// hard allocation ceiling.
///
/// Resolved on first access from:
///
///   1. `MYDB_PROCESS_MEM_LIMIT_MB` env (legacy override) if set
///   2. `executor_memory_target_bytes()`.
///
/// Runtime-mutable through `set_process_mem_limit_mb`.
pub fn process_mem_limit_bytes() -> usize {
    let cur = PROCESS_MEM_LIMIT_BYTES.load(Ordering::Relaxed);
    if cur != 0 {
        return cur;
    }
    let init =
        parse_mb_env("MYDB_PROCESS_MEM_LIMIT_MB").unwrap_or_else(executor_memory_target_bytes);
    PROCESS_MEM_LIMIT_BYTES
        .compare_exchange(0, init, Ordering::Relaxed, Ordering::Relaxed)
        .ok();
    PROCESS_MEM_LIMIT_BYTES.load(Ordering::Relaxed)
}

/// Update the process-wide memory budget at runtime (in MiB).
pub fn set_process_mem_limit_mb(mb: usize) -> Result<(), QueryError> {
    if mb == 0 {
        return Err(crate::storage::error(
            crate::error::catalog::ER_WRONG_TYPE_FOR_VAR,
            "variable 'mydb_process_mem_limit_mb' cannot be set to 0",
        ));
    }
    let bytes = mb.saturating_mul(1024 * 1024);
    PROCESS_MEM_LIMIT_BYTES.store(bytes, Ordering::Relaxed);
    Ok(())
}

static PROCESS_QUERY_MEM_USED: AtomicUsize = AtomicUsize::new(0);
static PROCESS_QUERY_MEM_HIGH_WATER: AtomicUsize = AtomicUsize::new(0);
static QUERY_RESERVATION_REJECTIONS: AtomicU64 = AtomicU64::new(0);
static PROCESS_RESERVATION_REJECTIONS: AtomicU64 = AtomicU64::new(0);
static TERMINAL_OOM_CANCELLATIONS: AtomicU64 = AtomicU64::new(0);
static NEXT_QUERY_MEMORY_ID: AtomicU64 = AtomicU64::new(1);

/// Current total bytes charged across every active per-query budget.
pub fn process_query_mem_used() -> usize {
    PROCESS_QUERY_MEM_USED.load(Ordering::Relaxed)
}

/// Highest value `process_query_mem_used()` has reached since process
/// start. Useful for `SHOW ENGINE MYDB STATUS` to expose peak memory.
pub fn process_query_mem_high_water() -> usize {
    PROCESS_QUERY_MEM_HIGH_WATER.load(Ordering::Relaxed)
}

pub fn query_reservation_rejections() -> u64 {
    QUERY_RESERVATION_REJECTIONS.load(Ordering::Relaxed)
}

pub fn process_reservation_rejections() -> u64 {
    PROCESS_RESERVATION_REJECTIONS.load(Ordering::Relaxed)
}

pub fn terminal_oom_cancellations() -> u64 {
    TERMINAL_OOM_CANCELLATIONS.load(Ordering::Relaxed)
}

fn record_process_charge_with_reclaim(bytes: usize, reclaim: bool) -> Result<(), QueryError> {
    // Query/process limits are soft spill watermarks, but executor resident
    // memory is still ordinary database-owned memory. Reserve it from the
    // unified hard governor before publishing the query-local counters.
    if reclaim {
        charge_controlled(ControlledMemoryCategory::Executor, bytes)?;
    } else if let Err(error) =
        charge_controlled_impl(ControlledMemoryCategory::Executor, bytes, false, true)
    {
        if error
            .message()
            .starts_with("process memory pressure limit exceeded")
        {
            crate::engine::memory_reclaimer::request_spill_allocator_reclaim();
        }
        return Err(error);
    }
    let mut cur = PROCESS_QUERY_MEM_USED.load(Ordering::Acquire);
    let new_total = loop {
        let Some(next) = cur.checked_add(bytes) else {
            release_controlled(ControlledMemoryCategory::Executor, bytes);
            return Err(memory_limit_error(bytes, cur, usize::MAX));
        };
        match PROCESS_QUERY_MEM_USED.compare_exchange_weak(
            cur,
            next,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => break next,
            Err(observed) => cur = observed,
        }
    };
    // Update the high-water mark with a relaxed CAS loop. Inexact under
    // contention but cheap and good enough for monitoring.
    let mut hw = PROCESS_QUERY_MEM_HIGH_WATER.load(Ordering::Relaxed);
    while new_total > hw {
        match PROCESS_QUERY_MEM_HIGH_WATER.compare_exchange_weak(
            hw,
            new_total,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(observed) => hw = observed,
        }
    }
    Ok(())
}

fn record_process_release(bytes: usize) {
    let released = saturating_atomic_sub(&PROCESS_QUERY_MEM_USED, bytes);
    release_controlled(ControlledMemoryCategory::Executor, released);
}

#[derive(Debug)]
pub struct MemBudget {
    query_id: u64,
    used: AtomicUsize,
    peak_used: AtomicUsize,
    limit: usize,
    track_process: bool,
    /// Live temp-disk bytes this query currently holds (see
    /// `spill_quota`). Released automatically when this budget is
    /// dropped, so a finished query frees its share of the global
    /// temp-disk ceiling.
    spill_bytes: AtomicU64,
    owner: Option<Weak<crate::engine::session_registry::SessionHandle>>,
    reclaim_database: Option<Weak<crate::storage::Database>>,
    /// First non-recoverable OOM for this SQL. Every worker sharing the
    /// budget observes the same error at the next cancellation checkpoint.
    terminal_oom: OnceLock<QueryError>,
}

impl MemBudget {
    #[allow(dead_code)] // Explicit-limit constructor used by executor regression modules.
    pub fn new(limit_bytes: usize) -> Self {
        Self {
            query_id: NEXT_QUERY_MEMORY_ID.fetch_add(1, Ordering::Relaxed),
            used: AtomicUsize::new(0),
            peak_used: AtomicUsize::new(0),
            limit: limit_bytes,
            track_process: false,
            spill_bytes: AtomicU64::new(0),
            owner: None,
            reclaim_database: current_reclaim_database().map(|db| Arc::downgrade(&db)),
            terminal_oom: OnceLock::new(),
        }
    }

    /// Model a production query watermark without mutating global settings.
    #[cfg(test)]
    pub(crate) fn process_tracked_for_test(limit_bytes: usize) -> Self {
        let mut budget = Self::new(limit_bytes);
        budget.track_process = true;
        budget
    }

    /// Construct a per-query budget from `query_mem_limit_bytes()`
    /// (derived default from the executor target; the legacy
    /// `MYDB_QUERY_MEM_LIMIT_MB` env is a direct override).
    /// Charges/releases on this budget also update the process-wide
    /// counter so concurrent queries can be capped collectively.
    pub fn from_env() -> Self {
        Self {
            query_id: NEXT_QUERY_MEMORY_ID.fetch_add(1, Ordering::Relaxed),
            used: AtomicUsize::new(0),
            peak_used: AtomicUsize::new(0),
            limit: query_mem_limit_bytes(),
            track_process: true,
            spill_bytes: AtomicU64::new(0),
            owner: None,
            reclaim_database: current_reclaim_database().map(|db| Arc::downgrade(&db)),
            terminal_oom: OnceLock::new(),
        }
    }

    /// Construct and publish a process-tracked budget owned by one client
    /// connection. The weak back-reference prevents a registry/budget cycle;
    /// the last worker holding the budget clears the live session row.
    #[cfg(test)]
    pub(crate) fn for_session(
        owner: Arc<crate::engine::session_registry::SessionHandle>,
    ) -> Arc<Self> {
        let budget = Arc::new(Self {
            query_id: NEXT_QUERY_MEMORY_ID.fetch_add(1, Ordering::Relaxed),
            used: AtomicUsize::new(0),
            peak_used: AtomicUsize::new(0),
            limit: query_mem_limit_bytes(),
            track_process: true,
            spill_bytes: AtomicU64::new(0),
            owner: Some(Arc::downgrade(&owner)),
            reclaim_database: current_reclaim_database().map(|db| Arc::downgrade(&db)),
            terminal_oom: OnceLock::new(),
        });
        owner.attach_query_memory(&budget);
        budget
    }

    pub(crate) fn for_session_with_database(
        owner: Arc<crate::engine::session_registry::SessionHandle>,
        database: &Arc<crate::storage::Database>,
    ) -> Arc<Self> {
        let budget = Arc::new(Self {
            query_id: NEXT_QUERY_MEMORY_ID.fetch_add(1, Ordering::Relaxed),
            used: AtomicUsize::new(0),
            peak_used: AtomicUsize::new(0),
            limit: query_mem_limit_bytes(),
            track_process: true,
            spill_bytes: AtomicU64::new(0),
            owner: Some(Arc::downgrade(&owner)),
            reclaim_database: Some(Arc::downgrade(database)),
            terminal_oom: OnceLock::new(),
        });
        owner.attach_query_memory(&budget);
        budget
    }

    pub(crate) fn for_database(database: &Arc<crate::storage::Database>) -> Arc<Self> {
        Arc::new(Self {
            query_id: NEXT_QUERY_MEMORY_ID.fetch_add(1, Ordering::Relaxed),
            used: AtomicUsize::new(0),
            peak_used: AtomicUsize::new(0),
            limit: query_mem_limit_bytes(),
            track_process: true,
            spill_bytes: AtomicU64::new(0),
            owner: None,
            reclaim_database: Some(Arc::downgrade(database)),
            terminal_oom: OnceLock::new(),
        })
    }

    fn reclaim_database(&self) -> Option<Arc<crate::storage::Database>> {
        self.reclaim_database.as_ref().and_then(Weak::upgrade)
    }

    /// Live temp-disk bytes this query holds.
    pub fn spill_bytes(&self) -> u64 {
        self.spill_bytes.load(Ordering::Relaxed)
    }
    /// Add to the per-query temp-disk counter (called by `spill_quota`).
    #[cfg(test)]
    pub fn add_spill_bytes(&self, n: u64) {
        self.spill_bytes.fetch_add(n, Ordering::Relaxed);
    }
    /// Subtract from the per-query temp-disk counter, saturating at 0.
    pub fn sub_spill_bytes(&self, n: u64) {
        let mut cur = self.spill_bytes.load(Ordering::Acquire);
        loop {
            let next = cur - n.min(cur);
            match self.spill_bytes.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => cur = observed,
            }
        }
    }

    pub(crate) fn try_add_spill_bytes(&self, n: u64, limit: u64) -> bool {
        let mut cur = self.spill_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = cur.checked_add(n) else {
                return false;
            };
            if limit != 0 && next > limit {
                return false;
            }
            match self.spill_bytes.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => cur = observed,
            }
        }
    }

    pub fn query_id(&self) -> u64 {
        self.query_id
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Whether this is the ordinary session/query soft watermark rather than
    /// an explicitly scoped operator budget. Work-area clients may borrow
    /// beyond the former while the global executor pool has capacity, but an
    /// explicit budget must remain an authoritative spill boundary.
    pub(crate) fn tracks_process(&self) -> bool {
        self.track_process
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    pub fn peak_used(&self) -> usize {
        self.peak_used.load(Ordering::Relaxed)
    }

    pub fn remaining(&self) -> usize {
        self.limit.saturating_sub(self.used())
    }

    pub(crate) fn latch_terminal_oom(&self, error: QueryError) -> QueryError {
        debug_assert!(error.is(crate::error::catalog::ER_OUTOFMEMORY));
        if self.terminal_oom.set(error.clone()).is_ok() {
            TERMINAL_OOM_CANCELLATIONS.fetch_add(1, Ordering::Relaxed);
            error
        } else {
            self.terminal_oom.get().cloned().unwrap_or(error)
        }
    }

    pub(crate) fn terminal_oom(&self) -> Option<QueryError> {
        self.terminal_oom.get().cloned()
    }

    /// Try to charge memory against the query/process soft watermarks.
    /// Spill-capable callers use the returned error to switch algorithms.
    /// Non-spillable callers must use [`Self::force_charge`].
    pub fn charge(&self, bytes: usize) -> Result<(), QueryError> {
        match self.charge_for_spillable_operator(bytes) {
            OperatorCharge::Granted => Ok(()),
            OperatorCharge::SoftLimit(error) => {
                Err(error.unwrap_or_else(|| memory_limit_error(bytes, self.used(), self.limit)))
            }
        }
    }

    /// Reserve resident operator memory for a spill-capable operator. Query,
    /// aggregate-process, and unified-governor misses are recoverable at this
    /// point because the operator has not mutated its resident state yet.
    pub(crate) fn charge_for_spillable_operator(&self, bytes: usize) -> OperatorCharge {
        if bytes == 0 {
            return OperatorCharge::Granted;
        }
        // Both the per-query and aggregate process targets are soft spill
        // watermarks. Check them before recording the charge so a spillable
        // operator can switch algorithms without transiently inflating usage.
        if self.track_process
            && PROCESS_QUERY_MEM_USED
                .load(Ordering::Acquire)
                .checked_add(bytes)
                .is_none_or(|next| next > process_mem_limit_bytes())
        {
            PROCESS_RESERVATION_REJECTIONS.fetch_add(1, Ordering::Relaxed);
            return OperatorCharge::SoftLimit(None);
        }
        let Some(new_used) = try_atomic_add_bounded(&self.used, bytes, self.limit) else {
            QUERY_RESERVATION_REJECTIONS.fetch_add(1, Ordering::Relaxed);
            return OperatorCharge::SoftLimit(None);
        };
        if self.track_process {
            if let Err(error) = record_process_charge_with_reclaim(bytes, false) {
                saturating_atomic_sub(&self.used, bytes);
                // The operator has not mutated its resident state yet. A hard
                // governor miss is therefore still recoverable by spilling
                // already-held state and retrying with a bounded buffer.
                PROCESS_RESERVATION_REJECTIONS.fetch_add(1, Ordering::Relaxed);
                return OperatorCharge::SoftLimit(Some(error));
            }
        }
        self.peak_used.fetch_max(new_used, Ordering::Relaxed);
        OperatorCharge::Granted
    }

    /// Cross query/process soft watermarks while retaining the unified hard
    /// process target. Used for non-spillable allocations and the bounded
    /// buffer required to complete a spill pass.
    #[track_caller]
    pub fn force_charge(&self, bytes: usize) -> Result<(), QueryError> {
        let site = std::panic::Location::caller();
        self.force_charge_recoverable(bytes).map_err(|error| {
            self.latch_terminal_oom(error.map_message(|message| {
                format!(
                    "{message}; reservation site={}:{}",
                    site.file(),
                    site.line()
                )
            }))
        })
    }

    /// Preflight state which can still be discarded or spilled. It uses the
    /// same hard admission gate as required state, without cancelling sibling
    /// workers when the caller can recover from a rejected reservation.
    pub(crate) fn force_charge_recoverable(&self, bytes: usize) -> Result<(), QueryError> {
        self.force_charge_with_reclaim(bytes, true)
    }

    pub(crate) fn try_charge_spill_buffer(&self, bytes: usize) -> Result<(), QueryError> {
        self.force_charge_with_reclaim(bytes, false)
    }

    fn force_charge_with_reclaim(&self, bytes: usize, reclaim: bool) -> Result<(), QueryError> {
        if bytes == 0 {
            return Ok(());
        }
        if self.track_process {
            record_process_charge_with_reclaim(bytes, reclaim)?;
        }
        let mut cur = self.used.load(Ordering::Acquire);
        loop {
            let Some(next) = cur.checked_add(bytes) else {
                if self.track_process {
                    record_process_release(bytes);
                }
                return Err(memory_limit_error(bytes, cur, self.limit));
            };
            match self
                .used
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    self.peak_used.fetch_max(next, Ordering::Relaxed);
                    return Ok(());
                }
                Err(observed) => cur = observed,
            }
        }
    }

    pub fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let to_release = saturating_atomic_sub(&self.used, bytes);
        if self.track_process {
            record_process_release(to_release);
            crate::engine::memory_reclaimer::note_worker_memory_release(to_release);
        }
    }

    /// Returns true if the operator should consider spilling: at or
    /// above 90% of the budget.
    #[cfg(test)]
    pub fn should_spill(&self) -> bool {
        let used = self.used();
        used.saturating_mul(10) >= self.limit.saturating_mul(9)
    }
}

pub(crate) enum OperatorCharge {
    Granted,
    SoftLimit(Option<QueryError>),
}

impl Drop for MemBudget {
    fn drop(&mut self) {
        if self.track_process {
            let used = self.used.swap(0, Ordering::Relaxed);
            if used > 0 {
                record_process_release(used);
            }
        }
        if let Some(owner) = self.owner.as_ref().and_then(Weak::upgrade) {
            owner.detach_query_memory(self.query_id, self.peak_used());
        }
        if self.track_process {
            crate::engine::memory_reclaimer::notify_query_memory_released(self.peak_used());
        }
    }
}

fn memory_limit_error(want: usize, used: usize, limit: usize) -> QueryError {
    if crate::util::env::var("MYDB_DEBUG_OOM_BT").is_ok() {
        eprintln!(
            "[OOM-BT query] want={} used={} limit={}\n{}",
            want,
            used,
            limit,
            std::backtrace::Backtrace::force_capture()
        );
    }
    crate::storage::error(
        crate::error::catalog::ER_OUTOFMEMORY,
        format!(
            "query memory limit exceeded: tried to allocate {} bytes, used {} / {}",
            want, used, limit
        ),
    )
}

/// RAII guard that releases the charged bytes on drop. Used for
/// short-lived allocations where panics or early returns shouldn't
/// leak charges.
#[cfg(test)]
pub struct MemCharge<'a> {
    budget: &'a MemBudget,
    bytes: usize,
}

#[cfg(test)]
impl<'a> MemCharge<'a> {
    pub fn new(budget: &'a MemBudget, bytes: usize) -> Result<Self, QueryError> {
        budget.charge(bytes)?;
        Ok(Self { budget, bytes })
    }
}

#[cfg(test)]
impl Drop for MemCharge<'_> {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

/// Owned variant of `MemCharge` that keeps an `Arc<MemBudget>` alive.
pub struct OwnedMemCharge {
    budget: Arc<MemBudget>,
    bytes: usize,
}

impl OwnedMemCharge {
    pub fn new(budget: Arc<MemBudget>, bytes: usize) -> Result<Self, QueryError> {
        budget.charge(bytes)?;
        Ok(Self { budget, bytes })
    }

    /// Optional bounded pipeline state may overlap an already-spilling
    /// producer. Keep explicit operator quotas authoritative and check the
    /// same hard process/physical gates without latching a terminal OOM.
    pub(crate) fn new_optional_pipeline(
        budget: Arc<MemBudget>,
        bytes: usize,
    ) -> Result<Self, QueryError> {
        if budget.tracks_process() && bytes <= budget.limit() {
            budget.force_charge_recoverable(bytes)?;
        } else {
            budget.charge(bytes)?;
        }
        Ok(Self { budget, bytes })
    }

    pub fn add(&mut self, bytes: usize) -> Result<(), QueryError> {
        self.budget.charge(bytes)?;
        self.bytes = self.bytes.saturating_add(bytes);
        Ok(())
    }
}

impl Drop for OwnedMemCharge {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

#[cfg(test)]
mod optional_pipeline_tests {
    use super::*;

    #[test]
    fn recoverable_scoped_reservation_keeps_a_required_io_retry_possible() {
        let budget = Arc::new(MemBudget::new(1024));
        budget.force_charge(1).unwrap();
        let mut charge = ScopedCharge::for_budget(Some(budget.clone()));
        assert!(charge.add_force_recoverable(usize::MAX).is_err());
        assert_eq!(charge.charged(), 0);
        assert_eq!(budget.used(), 1);
        assert!(budget.terminal_oom().is_none());
        charge.add_force_recoverable(64).unwrap();
        assert_eq!(budget.used(), 65);
        drop(charge);
        assert_eq!(budget.used(), 1);
        budget.release(1);
    }

    #[test]
    fn optional_pipeline_preserves_explicit_quota_and_keeps_miss_recoverable() {
        let budget = Arc::new(MemBudget::new(256));
        let occupied = OwnedMemCharge::new(budget.clone(), 192).unwrap();
        assert!(OwnedMemCharge::new_optional_pipeline(budget.clone(), 128).is_err());
        assert!(budget.terminal_oom().is_none());
        let bitmap = OwnedMemCharge::new_optional_pipeline(budget.clone(), 32).unwrap();
        assert_eq!(budget.used(), 224);
        drop(bitmap);
        drop(occupied);
        assert_eq!(budget.used(), 0);
    }
}

// ---------------------------------------------------------------------------
// Thread-local current-query budget
// ---------------------------------------------------------------------------
//
// Operators (HashAggregate / Sort / TopK / Distinct) consult the
// thread-local `CURRENT_QUERY_BUDGET` to charge their state. The
// SELECT entry-point pushes a per-query `MemBudget` onto the slot via
// `with_query_budget`, restores the previous value on return.
//
// Defaults: if no budget is set on the thread, `try_charge` is a no-op
// — useful for tests and code paths that don't go through SELECT.

thread_local! {
    // Store the budget directly. The previous wrapper put an
    // `Arc<QueryMemoryContext>` around an `Arc<MemBudget>`, which added one
    // allocation per statement and two extra atomic reference-count updates
    // on every `current_budget()` lookup. Query budgets are already shared
    // safely through their own Arc, so the wrapper had no ownership purpose.
    static CURRENT_QUERY_BUDGET: RefCell<Option<Arc<MemBudget>>> = const { RefCell::new(None) };
}

thread_local! {
    static CURRENT_RECLAIM_DATABASE: RefCell<Option<Weak<crate::storage::Database>>> = const { RefCell::new(None) };
}

/// Database fallback for protocol allocations outside SQL execution. This is
/// thread-local and must be entered and dropped within one Future::poll.
/// It deliberately does not install a query budget or hide a SQL-owned one.
pub struct ReclaimDatabaseScope {
    previous: Option<Weak<crate::storage::Database>>,
    _not_send: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl ReclaimDatabaseScope {
    pub(crate) fn enter(database: &Arc<crate::storage::Database>) -> Self {
        Self {
            previous: CURRENT_RECLAIM_DATABASE
                .with(|current| current.replace(Some(Arc::downgrade(database)))),
            _not_send: std::marker::PhantomData,
        }
    }
}

impl Drop for ReclaimDatabaseScope {
    fn drop(&mut self) {
        CURRENT_RECLAIM_DATABASE.with(|current| *current.borrow_mut() = self.previous.take());
    }
}

struct QueryBudgetScope {
    previous: Option<Arc<MemBudget>>,
}

impl Drop for QueryBudgetScope {
    fn drop(&mut self) {
        CURRENT_QUERY_BUDGET.with(|c| *c.borrow_mut() = self.previous.take());
    }
}

pub fn with_query_budget<F, R>(budget: Arc<MemBudget>, f: F) -> R
where
    F: FnOnce() -> R,
{
    let previous = CURRENT_QUERY_BUDGET.with(|c| c.replace(Some(budget)));
    let _scope = QueryBudgetScope { previous };
    f()
}

pub fn current_budget() -> Option<Arc<MemBudget>> {
    CURRENT_QUERY_BUDGET.with(|c| c.borrow().clone())
}

/// Publish a non-recoverable allocation failure to every worker belonging to
/// the current SQL. Optional-cache misses and spill hints must not call this;
/// only an error that will be returned to the client is terminal.
pub(crate) fn terminal_oom(error: QueryError) -> QueryError {
    if !error.is(crate::error::catalog::ER_OUTOFMEMORY) {
        return error;
    }
    CURRENT_QUERY_BUDGET.with(|current| {
        current
            .borrow()
            .as_ref()
            .map(|budget| budget.latch_terminal_oom(error.clone()))
            .unwrap_or(error)
    })
}

pub(crate) fn allocation_error(message: impl Into<String>) -> QueryError {
    terminal_oom(crate::storage::error(
        crate::error::catalog::ER_OUTOFMEMORY,
        message,
    ))
}

/// Fallible capacity growth for user-sized executor vectors. Callers that
/// retain the vector must still own a capacity-based `ScopedCharge`; this
/// helper guarantees the allocator failure itself becomes a query-scoped
/// 1037 instead of reaching an infallible `reserve`/`push` abort.
pub(crate) fn try_reserve_vec_exact<T>(
    values: &mut Vec<T>,
    additional: usize,
    site: &'static str,
) -> Result<(), QueryError> {
    values
        .try_reserve_exact(additional)
        .map_err(|_| allocation_error(format!("out of memory while reserving {site}")))
}

pub(crate) fn try_vec_with_capacity<T>(
    capacity: usize,
    site: &'static str,
) -> Result<Vec<T>, QueryError> {
    let mut values = Vec::new();
    try_reserve_vec_exact(&mut values, capacity, site)?;
    Ok(values)
}

pub(crate) fn try_copy_bytes(bytes: &[u8], site: &'static str) -> Result<Vec<u8>, QueryError> {
    let mut copy = try_vec_with_capacity(bytes.len(), site)?;
    copy.extend_from_slice(bytes);
    Ok(copy)
}

pub(crate) fn try_copy_slice<T: Copy>(
    values: &[T],
    site: &'static str,
) -> Result<Vec<T>, QueryError> {
    let mut copy = try_vec_with_capacity(values.len(), site)?;
    copy.extend_from_slice(values);
    Ok(copy)
}

pub(crate) fn try_copy_string(text: &str, site: &'static str) -> Result<String, QueryError> {
    let mut copy = String::new();
    copy.try_reserve_exact(text.len())
        .map_err(|_| allocation_error(format!("out of memory while cloning {site}")))?;
    copy.push_str(text);
    Ok(copy)
}

pub(crate) fn try_clone_value(
    value: &crate::types::Value,
    site: &'static str,
) -> Result<crate::types::Value, QueryError> {
    use crate::types::Value;
    Ok(match value {
        Value::Decimal(text) => Value::Decimal(try_copy_string(text, site)?),
        Value::String(text) => Value::String(try_copy_string(text, site)?),
        Value::Bytes(bytes) => Value::Bytes(try_copy_bytes(bytes, site)?),
        Value::Json(bytes) => Value::Json(try_copy_bytes(bytes, site)?),
        Value::Geometry(bytes) => Value::Geometry(try_copy_bytes(bytes, site)?),
        Value::Vector(bytes) => Value::Vector(crate::vector::VectorValue::new(
            bytes.datatype,
            bytes.dimension,
            try_copy_bytes(bytes, site)?,
        )),
        // DecimalWide is bounded by MySQL's 65-digit precision and
        // SmallString is inline; their Clone allocations are therefore not
        // user-unbounded executor growth.
        other => other.clone(),
    })
}

/// Cheap hot-path check used by statement cancellation. The Arc is not
/// cloned; cloning the error happens only after a terminal OOM has latched.
pub(crate) fn check_terminal_oom() -> Result<(), QueryError> {
    CURRENT_QUERY_BUDGET.with(|current| {
        current
            .borrow()
            .as_ref()
            .and_then(|budget| budget.terminal_oom())
            .map_or(Ok(()), Err)
    })
}

/// Charge `bytes` against the current thread's query budget, if any.
/// Returns `Ok(())` immediately when no budget is active.
pub fn try_charge(bytes: usize) -> Result<(), QueryError> {
    if let Some(b) = current_budget() {
        b.charge(bytes).map_err(terminal_oom)?;
    }
    Ok(())
}

/// Spill-probing counterpart of [`try_charge`]. A refusal is returned to the
/// operator without poisoning the query; the caller must either spill and
/// release memory or explicitly promote the returned error to terminal.
pub(crate) fn try_charge_for_spill(bytes: usize) -> Result<(), QueryError> {
    let Some(budget) = current_budget() else {
        return Ok(());
    };
    match budget.charge_for_spillable_operator(bytes) {
        OperatorCharge::Granted => Ok(()),
        OperatorCharge::SoftLimit(error) => {
            Err(error.unwrap_or_else(|| memory_limit_error(bytes, budget.used(), budget.limit())))
        }
    }
}

/// Default per-row estimate used by operators that don't know the
/// actual row width. Conservative on the high side so the budget bites
/// before the process actually OOMs.
pub const ESTIMATED_ROW_BYTES: usize = 256;

/// Estimated in-memory footprint of one [`crate::types::Value`].
/// Includes the enum's stack size plus heap-resident capacity of any
/// variable-length variant (`String`, `Bytes`, `Json`, `Geometry`,
/// `Vector`). Fixed-width variants (`Int`, `Double`, `Date`, …) only
/// cost the stack slot. Used by P1-2 to size streaming-cursor charges
/// against the actual row payload rather than a fixed 256-byte stub.
#[inline]
pub fn estimate_value_bytes(value: &crate::types::Value) -> usize {
    use crate::engine::executor::memory_footprint::AllocatedBytes;
    std::mem::size_of::<crate::types::Value>().saturating_add(value.allocated_bytes())
}

/// Estimated in-memory footprint of a single result-set row (a slice
/// of [`crate::types::Value`]). Adds a fixed overhead for the
/// `Vec<Value>` header. Saturating arithmetic keeps a pathologically
/// large row from wrapping the accumulator.
#[inline]
pub fn estimate_row_bytes(values: &[crate::types::Value]) -> usize {
    let mut total = std::mem::size_of::<Vec<crate::types::Value>>();
    for v in values {
        total = total.saturating_add(estimate_value_bytes(v));
    }
    total
}

/// RAII guard that releases a previously-charged amount from the
/// thread-local current query budget on drop. Used at sites that have
/// no `&MemBudget` handy but want scope-bounded accounting. Callers
/// can incrementally `add` more charges; all accumulated bytes are
/// released together when the guard is dropped.
pub struct ScopedCharge {
    budget: Option<Arc<MemBudget>>,
    bytes: usize,
}

impl ScopedCharge {
    pub(crate) fn for_budget(budget: Option<Arc<MemBudget>>) -> Self {
        Self { budget, bytes: 0 }
    }
    pub(crate) fn sibling(&self) -> Self {
        Self {
            budget: self.budget.clone(),
            bytes: 0,
        }
    }
    /// Build a guard for `bytes` already charged via `try_charge`. The
    /// caller is responsible for calling `try_charge` first.
    pub fn new(bytes: usize) -> Self {
        Self {
            budget: current_budget(),
            bytes,
        }
    }

    /// Charge `bytes` against the current thread budget (strict mode)
    /// and return a guard that releases on drop. Wrapper around
    /// `try_charge_strict` for sites that want the panic-in-debug
    /// safety net.
    pub fn new_strict(bytes: usize, _site: &'static str) -> Result<Self, QueryError> {
        let budget = current_budget().unwrap_or_else(|| {
            #[cfg(debug_assertions)]
            panic!("mem_budget::ScopedCharge::new_strict at '{_site}' has no active query budget");
            #[cfg(not(debug_assertions))]
            Arc::new(MemBudget::new(usize::MAX))
        });
        budget.charge(bytes).map_err(|error| {
            error.map_message(|message| format!("{message}; reservation site={_site}"))
        })?;
        Ok(Self {
            budget: Some(budget),
            bytes,
        })
    }

    /// Charge `extra` bytes against the current thread budget and add them to
    /// the running total released by `Drop`.
    #[track_caller]
    pub fn add(&mut self, extra: usize) -> Result<(), QueryError> {
        if extra == 0 {
            return Ok(());
        }
        if let Some(budget) = &self.budget {
            let site = std::panic::Location::caller();
            budget.charge(extra).map_err(|error| {
                error.map_message(|message| {
                    format!(
                        "{message}; reservation site={}:{}",
                        site.file(),
                        site.line()
                    )
                })
            })?;
        }
        self.bytes = self.bytes.saturating_add(extra);
        Ok(())
    }

    /// Drain a mandatory producer batch even when it has crossed the query
    /// watermark. On a soft miss only a bounded amount of this consumer's
    /// state may borrow workspace; the unified hard admission still applies.
    pub(crate) fn add_with_workspace(&mut self, extra: usize) -> Result<(), QueryError> {
        match self.add(extra) {
            Ok(()) => Ok(()),
            Err(error) => {
                let cap = self.budget.as_ref().map_or(0, |budget| {
                    (budget.limit() / 8).clamp(16 * 1024, 256 * 1024)
                });
                if self.add_within(extra, cap) {
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Admit required state for a non-spillable consumer (for example TopK).
    /// Production query limits are spill watermarks: a producer may already
    /// exceed them while completing a spill pass. The consumer must still be
    /// able to drain it, subject to the unified hard governor. Explicit scoped
    /// budgets remain authoritative, including for wide payloads and huge K.
    /// A live spill producer may retain mandatory replay metadata above its
    /// watermark; allow only bounded consumer workspace while draining it.
    pub(crate) fn add_non_spillable(&mut self, extra: usize) -> Result<(), QueryError> {
        if self
            .budget
            .as_ref()
            .is_some_and(|budget| budget.tracks_process())
        {
            self.add_force(extra)
        } else {
            match self.add(extra) {
                Ok(()) => Ok(()),
                Err(error) => {
                    // Retain at most one bounded consumer work area while a
                    // spill producer still owns mandatory replay metadata.
                    let cap = self.budget.as_ref().map_or(0, |budget| {
                        if budget.spill_bytes() == 0 {
                            0
                        } else {
                            budget.limit().min(256 * 1024)
                        }
                    });
                    if self.add_within(extra, cap) {
                        Ok(())
                    } else {
                        Err(error)
                    }
                }
            }
        }
    }

    /// Transfer an already-accounted reservation from `other` into this
    /// guard without charging or releasing the underlying budget.  Both
    /// guards must belong to the same query budget.  This is used by
    /// speculative executor paths: failure drops the attempt-local guard and
    /// rolls the reservation back, while success moves it into the enclosing
    /// operator lifetime.
    pub(crate) fn absorb(&mut self, other: &mut Self) {
        if other.bytes == 0 {
            return;
        }
        let same_budget = match (&self.budget, &other.budget) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            (None, None) => true,
            _ => false,
        };
        assert!(
            same_budget,
            "cannot transfer memory charge across query budgets"
        );
        self.bytes = self.bytes.saturating_add(other.bytes);
        other.bytes = 0;
    }

    /// Compatibility spelling retained for aggregate callers while they are
    /// migrated. It obeys the same hard target as `add`; Hash Aggregate must
    /// spill or return 1037/HY001 and may never force-charge past the process
    /// target.
    pub fn add_or_fallback(&mut self, extra: usize) -> Result<(), QueryError> {
        self.add(extra)
    }

    /// Move an existing reservation with its allocation, without releasing it.
    pub(crate) fn split(&mut self, bytes: usize) -> Self {
        assert!(bytes <= self.bytes);
        self.bytes -= bytes;
        Self {
            budget: self.budget.clone(),
            bytes,
        }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.budget
            .as_ref()
            .map_or(usize::MAX, |budget| budget.remaining())
    }

    /// Total bytes this guard has charged so far.
    pub fn charged(&self) -> usize {
        self.bytes
    }

    /// Charge `extra` only if this guard's accumulated total would stay
    /// within `cap` bytes; otherwise charge nothing and return `false`,
    /// signalling the caller to abandon its in-memory build and fall back
    /// to a spilling path. The successful charge uses `add_force`, so soft
    /// watermarks do not interfere with the caller's explicit cap; the
    /// unified hard target may still make this return `false`. `cap` is an
    /// absolute ceiling on *this guard's* accumulated charge (typically the
    /// query budget's free headroom captured at operator entry), making the
    /// spill decision driven by real accumulated bytes rather than a CBO
    /// cardinality estimate.
    pub fn add_within(&mut self, extra: usize, cap: usize) -> bool {
        let Some(next) = self.bytes.checked_add(extra).filter(|next| *next <= cap) else {
            return false;
        };
        if let Some(budget) = &self.budget {
            if budget.force_charge_recoverable(extra).is_err() {
                return false;
            }
        }
        self.bytes = next;
        true
    }

    /// Charge already-mutated state past query/process soft watermarks while
    /// retaining the unified hard target.
    #[track_caller]
    pub fn add_force(&mut self, extra: usize) -> Result<(), QueryError> {
        if extra == 0 {
            return Ok(());
        }
        if let Some(b) = &self.budget {
            b.force_charge(extra)?;
        }
        self.bytes = self.bytes.saturating_add(extra);
        Ok(())
    }

    /// Preflight mandatory spill metadata while its owner can still submit
    /// pending I/O, without cancelling the query on a recoverable rejection.
    pub(crate) fn add_force_recoverable(&mut self, extra: usize) -> Result<(), QueryError> {
        if extra == 0 {
            return Ok(());
        }
        if let Some(budget) = &self.budget {
            budget.force_charge_recoverable(extra)?;
        }
        self.bytes = self.bytes.saturating_add(extra);
        Ok(())
    }

    pub(crate) fn release_bytes(&mut self, bytes: usize) {
        let released = bytes.min(self.bytes);
        if let Some(budget) = &self.budget {
            budget.release(released);
        }
        self.bytes -= released;
    }
}

impl Drop for ScopedCharge {
    fn drop(&mut self) {
        if self.bytes > 0 {
            if let Some(budget) = &self.budget {
                budget.release(self.bytes);
            }
        }
    }
}

/// Charge memory that cannot spill. It may exceed query/process soft
/// watermarks, but never the unified process target.
#[track_caller]
pub fn force_charge_scoped(bytes: usize) -> Result<ScopedCharge, QueryError> {
    let mut charge = ScopedCharge::new(0);
    if bytes > 0 {
        charge.add_force(bytes)?;
    }
    Ok(charge)
}

/// Grow a non-spillable Vec under the query's hard executor reservation.
/// `force` semantics intentionally bypass only the query/process spill
/// watermarks. Capacity is reconciled after `try_reserve_exact`, because Rust
/// allocators may grant more slots than requested.
pub(crate) fn try_reserve_vec_hard<T>(
    values: &mut Vec<T>,
    additional: usize,
    charge: &mut ScopedCharge,
    site: &'static str,
) -> Result<(), QueryError> {
    let old_capacity = values.capacity();
    let required = values
        .len()
        .checked_add(additional)
        .ok_or_else(|| allocation_error(format!("capacity overflow while reserving {site}")))?;
    if required <= old_capacity {
        return Ok(());
    }
    let item_bytes = std::mem::size_of::<T>();
    // The allocator may copy into a replacement while the complete old
    // allocation is still live. Its existing charge covers the old buffer;
    // admit the complete replacement before entering the allocator.
    let precharged = required.saturating_mul(item_bytes);
    charge.add_force(precharged)?;
    if let Err(error) = values.try_reserve_exact(additional) {
        charge.release_bytes(precharged);
        return Err(allocation_error(format!(
            "out of memory while reserving {site}: {error}"
        )));
    }
    let actual = values.capacity().saturating_mul(item_bytes);
    if actual > precharged {
        charge.add_force(actual - precharged)?;
    } else {
        charge.release_bytes(precharged - actual);
    }
    charge.release_bytes(old_capacity.saturating_mul(item_bytes));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spill_preflight_rejects_without_waiting_on_global_reclaim_and_keeps_accounting() {
        let admission = RECLAIM_ADMISSION.get_or_init(|| Mutex::new(()));
        let held = admission.lock().unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let result = record_process_charge_with_reclaim(usize::MAX, false);
            let budget = MemBudget::process_tracked_for_test(1024);
            assert!(budget.try_charge_spill_buffer(usize::MAX).is_err());
            assert_eq!(budget.used(), 0);
            sender.send(result).unwrap();
        });
        let result = receiver.recv_timeout(std::time::Duration::from_secs(1));
        drop(held);
        worker.join().unwrap();
        assert!(result
            .expect("spill admission must not wait for the reclaim owner")
            .is_err());
        let before = PROCESS_QUERY_MEM_USED.load(Ordering::Acquire);
        record_process_charge_with_reclaim(128, false).unwrap();
        assert_eq!(PROCESS_QUERY_MEM_USED.load(Ordering::Acquire), before + 128);
        record_process_release(128);
        assert_eq!(PROCESS_QUERY_MEM_USED.load(Ordering::Acquire), before);
    }

    #[test]
    fn recoverable_storage_admission_preserves_query_fallback_and_hard_limit() {
        let budget = Arc::new(MemBudget::new(1024));
        with_query_budget(budget, || {
            // The impossible request must still be rejected, including in a
            // compensation scope. Neither attempt may poison a later fallback.
            for critical in [false, true] {
                let attempt = || {
                    ControlledMemoryCharge::try_new_recoverable(
                        ControlledMemoryCategory::StorageScratch,
                        usize::MAX,
                    )
                };
                let result = if critical {
                    with_critical_memory_reservations(attempt)
                } else {
                    attempt()
                };
                assert!(result
                    .unwrap_err()
                    .is(crate::error::catalog::ER_OUTOFMEMORY));
                check_terminal_oom().unwrap();
            }
            let _fallback = ControlledMemoryCharge::try_new_recoverable(
                ControlledMemoryCategory::StorageScratch,
                1024,
            )
            .unwrap();
            check_terminal_oom().unwrap();
        });
    }

    #[test]
    fn controlled_lease_transfer_preserves_admission_until_final_owner_drops() {
        let category = ControlledMemoryCategory::StorageScratch;
        let baseline = category_counter(category).load(Ordering::Relaxed);
        let mut destination = ControlledMemoryCharge::try_new(category, 128).unwrap();
        let mut source = ControlledMemoryCharge::try_new(category, 256).unwrap();
        destination.absorb(&mut source);
        assert_eq!(destination.bytes(), 384);
        assert_eq!(source.bytes(), 0);
        drop(source);
        assert_eq!(
            category_counter(category).load(Ordering::Relaxed),
            baseline + 384
        );
        drop(destination);
        assert_eq!(category_counter(category).load(Ordering::Relaxed), baseline);
    }

    static GLOBAL_MEMORY_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn reset_controlled_test_counters() {
        CONTROLLED_MEMORY_USED.store(0, Ordering::Release);
        CONTROLLED_MEMORY_PEAK.store(0, Ordering::Release);
        CONTROLLED_BUFFER_USED.store(0, Ordering::Release);
        CONTROLLED_BUFFER_RAW_LIMIT_BYTES.store(0, Ordering::Release);
        CONTROLLED_BUFFER_LIMIT_BYTES.store(0, Ordering::Release);
        CONTROLLED_BUFFER_RAW.store(0, Ordering::Release);
        CONTROLLED_STORAGE_SCRATCH.store(0, Ordering::Release);
        CONTROLLED_EXECUTOR.store(0, Ordering::Release);
        CONTROLLED_DERIVED_CACHE.store(0, Ordering::Release);
        CONTROLLED_DDL_COPY.store(0, Ordering::Release);
        CONTROLLED_PARSER_TRANSIENT.store(0, Ordering::Release);
        CONTROLLED_PERFORMANCE_SCHEMA.store(0, Ordering::Release);
        CONTROLLED_REJECTIONS.store(0, Ordering::Release);
        RECLAIM_ATTEMPTS.store(0, Ordering::Release);
        RECLAIM_RETRY_SUCCESSES.store(0, Ordering::Release);
        RECLAIM_CACHE_BYTES.store(0, Ordering::Release);
        RECLAIM_BUFFER_BYTES.store(0, Ordering::Release);
        RECLAIM_ALLOCATOR_BYTES.store(0, Ordering::Release);
        RECLAIM_OOM_AFTER_RETRY.store(0, Ordering::Release);
        for counter in &CONTROLLED_REJECTIONS_BY_CATEGORY {
            counter.store(0, Ordering::Release);
        }
        for worker in &CATEGORY_ACTIVITY {
            for activity in &worker.0 {
                activity.alloc_count.store(0, Ordering::Release);
                activity.free_count.store(0, Ordering::Release);
                activity.alloc_bytes.store(0, Ordering::Release);
                activity.free_bytes.store(0, Ordering::Release);
            }
        }
        for counter in &CONTROLLED_CATEGORY_PEAK_COUNT {
            counter.store(0, Ordering::Release);
        }
        for counter in &CONTROLLED_CATEGORY_CURRENT_COUNT {
            counter.store(0, Ordering::Release);
        }
        for counter in &CONTROLLED_CATEGORY_PEAK_BYTES {
            counter.store(0, Ordering::Release);
        }
    }

    #[test]
    fn pfs_memory_activity_survives_worker_exit_and_shard_collisions() {
        let _guard = GLOBAL_MEMORY_TEST_LOCK.lock().unwrap();
        reset_controlled_test_counters();
        let writers = CATEGORY_ACTIVITY_SHARDS + 8;
        let barrier = std::sync::Barrier::new(writers);
        let category = ControlledMemoryCategory::PerformanceSchema;
        std::thread::scope(|scope| {
            for _ in 0..writers {
                let barrier = &barrier;
                scope.spawn(move || {
                    record_category_alloc(category, 16, 16);
                    barrier.wait();
                    record_category_free(category, 16);
                    for _ in 1..100 {
                        record_category_alloc(category, 16, 16);
                        record_category_free(category, 16);
                    }
                });
            }
        });
        let slot = category_slot(category);
        let snapshot = pfs_memory_category_snapshots()[slot];
        assert_eq!(snapshot.alloc_count, (writers * 100) as u64);
        assert_eq!(snapshot.free_count, snapshot.alloc_count);
        assert_eq!(snapshot.alloc_bytes, snapshot.alloc_count * 16);
        assert_eq!(snapshot.free_bytes, snapshot.alloc_bytes);
        assert_eq!(snapshot.peak_count, writers as u64);
        assert_eq!(
            CONTROLLED_CATEGORY_CURRENT_COUNT[slot].load(Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn pfs_memory_activity_can_initialize_on_worker_tls_teardown() {
        let _guard = GLOBAL_MEMORY_TEST_LOCK.lock().unwrap();
        reset_controlled_test_counters();
        struct ReleaseOnExit;
        impl Drop for ReleaseOnExit {
            fn drop(&mut self) {
                record_category_free(ControlledMemoryCategory::PerformanceSchema, 16);
            }
        }
        thread_local! {
            static RELEASE: std::cell::RefCell<Option<ReleaseOnExit>> = const {
                std::cell::RefCell::new(None)
            };
        }
        // Allocation and release can belong to different workers. The
        // releasing thread has never recorded statistics before its exit.
        record_category_alloc(ControlledMemoryCategory::PerformanceSchema, 16, 16);
        std::thread::spawn(|| RELEASE.with(|slot| *slot.borrow_mut() = Some(ReleaseOnExit)))
            .join()
            .unwrap();
        let snapshot = pfs_memory_category_snapshots()
            [category_slot(ControlledMemoryCategory::PerformanceSchema)];
        assert_eq!(snapshot.alloc_count, 1);
        assert_eq!(snapshot.free_count, 1);
        assert_eq!(snapshot.alloc_bytes, 16);
        assert_eq!(snapshot.free_bytes, 16);
    }

    #[test]
    fn pfs_audit_memory_count_high_water_is_not_cumulative_allocations() {
        let _guard = GLOBAL_MEMORY_TEST_LOCK.lock().unwrap();
        reset_controlled_test_counters();
        let category = ControlledMemoryCategory::PerformanceSchema;
        for _ in 0..10 {
            record_category_alloc(category, 10, 10);
            record_category_free(category, 10);
        }
        let snapshot = pfs_memory_category_snapshots()[category_slot(category)];
        assert_eq!(snapshot.alloc_count, 10);
        assert_eq!(snapshot.free_count, 10);
        assert_eq!(snapshot.peak_count, 1);
        record_category_alloc(category, 10, 10);
        record_category_alloc(category, 10, 20);
        assert_eq!(
            pfs_memory_category_snapshots()[category_slot(category)].peak_count,
            2
        );
        reset_controlled_test_counters();
    }

    #[test]
    fn streaming_budget_keeps_reclaim_database_after_entry_scope_returns() {
        let dir = tempfile::tempdir().unwrap();
        let database = Arc::new(crate::storage::Database::open(dir.path()).unwrap());
        let budget = {
            let _scope = ReclaimDatabaseScope::enter(&database);
            Arc::new(MemBudget::from_env())
        };
        assert!(current_reclaim_database().is_none());
        let expected = Arc::clone(&database);
        std::thread::spawn(move || {
            with_query_budget(budget, || {
                assert!(Arc::ptr_eq(&current_reclaim_database().unwrap(), &expected));
            })
        })
        .join()
        .unwrap();
        assert!(current_reclaim_database().is_none());
    }

    #[tokio::test]
    async fn protocol_reclaim_context_is_restored_between_polls_and_on_unwind() {
        let dir = tempfile::tempdir().unwrap();
        let database = Arc::new(crate::storage::Database::open(dir.path()).unwrap());
        assert!(current_reclaim_database().is_none());
        assert!(current_budget().is_none());
        let mut polls = 0;
        let mut work = std::pin::pin!(async {
            assert!(current_reclaim_database().is_some());
            tokio::task::yield_now().await;
            assert!(current_reclaim_database().is_some());
        });
        std::future::poll_fn(|cx| {
            assert!(current_reclaim_database().is_none());
            let result = {
                let _scope = ReclaimDatabaseScope::enter(&database);
                assert!(Arc::ptr_eq(&current_reclaim_database().unwrap(), &database));
                assert!(current_budget().is_none());
                polls += 1;
                std::future::Future::poll(work.as_mut(), cx)
            };
            assert!(current_reclaim_database().is_none());
            result
        })
        .await;
        assert!(polls >= 2);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _scope = ReclaimDatabaseScope::enter(&database);
            let _nested = ReclaimDatabaseScope::enter(&database);
            panic!("scope unwind");
        }));
        assert!(panic.is_err());
        assert!(current_reclaim_database().is_none());
    }

    #[test]
    fn memory_target_parser_accepts_mib_numbers_and_scaled_sizes() {
        let parse = crate::util::system_memory::parse_mebibyte_or_size_bytes;
        assert_eq!(parse("200"), Some(200 * 1024 * 1024));
        assert_eq!(parse("200M"), Some(200 * 1024 * 1024));
        assert_eq!(parse("1G"), Some(1024 * 1024 * 1024));
        assert_eq!(parse("0"), None);
        assert_eq!(parse("bad"), None);
    }

    #[test]
    fn default_query_quota_is_forty_two_percent_of_executor_target() {
        let executor_target = 300 * 1024 * 1024;
        assert_eq!(DEFAULT_QUERY_MEM_QUOTA_PCT, 42);
        assert_eq!(
            derived_query_mem_limit_bytes(executor_target, DEFAULT_QUERY_MEM_QUOTA_PCT as usize),
            126 * 1024 * 1024
        );
    }

    #[test]
    fn logical_admission_reclaim_survives_concurrent_page_refill() {
        let limit = 2usize * 1024 * 1024 * 1024 / 10 * 8;
        let used = limit - 3065;
        let requested = 102400;
        let target = logical_admission_reclaim_target(limit, used, requested);
        let released =
            (used - target).div_ceil(crate::storage::PAGE_SIZE) * crate::storage::PAGE_SIZE;
        // Reproduce the observed 1037: an exact-deficit reclaim lost its
        // reservation race to just one background 16 KiB page allocation.
        let exact = limit - requested;
        let exact_released =
            (used - exact).div_ceil(crate::storage::PAGE_SIZE) * crate::storage::PAGE_SIZE;
        assert!(used - exact_released + crate::storage::PAGE_SIZE + requested > limit);
        assert!(used - released + crate::storage::PAGE_SIZE + requested <= limit);
        assert_eq!(
            logical_admission_reclaim_target(limit, 0, requested),
            limit - requested
        );
        assert!(limit - requested - target <= ADMISSION_RECLAIM_HEADROOM_BYTES);
    }

    #[test]
    fn admission_reclaim_target_honors_physical_pressure_below_logical_limit() {
        let mib = 1024 * 1024;
        // The regression shape: only 6.6 GiB is logically controlled, so the
        // 8 GiB SQL target alone requested no eviction. Physical admission is
        // 700 MiB over the managed limit and must lower the target accordingly.
        assert_eq!(
            admission_reclaim_target(6_600 * mib, 600 * mib, 200 * mib, 8_000 * mib, 700 * mib,),
            5_900 * mib
        );
        // Never reclaim below the non-buffer allocation plus buffer floor.
        assert_eq!(
            admission_reclaim_target(900 * mib, 700 * mib, 100 * mib, 800 * mib, 500 * mib),
            800 * mib
        );
    }

    #[test]
    fn physical_admission_reclaim_keeps_one_sampler_window_of_headroom() {
        assert_eq!(admission_reclaim_bytes_with_headroom(0, false), 0);
        assert_eq!(
            admission_reclaim_bytes_with_headroom(1, true),
            ADMISSION_RECLAIM_HEADROOM_BYTES
        );
        assert_eq!(
            admission_reclaim_bytes_with_headroom(128 * 1024 * 1024, true),
            128 * 1024 * 1024
        );
    }

    #[test]
    fn advisory_cache_reclaim_preserves_hot_capacity_but_not_emergency_reserves() {
        let mib = 1024 * 1024;
        assert_eq!(advisory_cache_floor(4_800 * mib, 10_000 * mib), 960 * mib);
        assert_eq!(advisory_cache_floor(8_000 * mib, 10_000 * mib), 1_000 * mib);
        assert_eq!(advisory_cache_floor(64 * mib, 512 * mib), 16 * mib);
        assert_eq!(
            advisory_cache_floor(64_000 * mib, 100_000 * mib),
            1024 * mib
        );
        assert_eq!(
            advisory_cache_reclaim_bytes(1024 * mib + 64 * mib - 1, 1024 * mib, 512 * mib),
            0
        );
        assert_eq!(
            advisory_cache_reclaim_bytes(1024 * mib + 64 * mib, 1024 * mib, 512 * mib),
            64 * mib
        );
        assert_eq!(
            advisory_cache_reclaim_bytes(128 * mib, 16 * mib, 2 * mib),
            2 * mib
        );
    }

    #[test]
    fn controlled_memory_concurrent_reservations_never_cross_target() {
        let _serial = GLOBAL_MEMORY_TEST_LOCK.lock().unwrap();
        let prior_target = MEMORY_TARGET_BYTES.swap(1024 * 1024, Ordering::AcqRel);
        let prior_effective = EFFECTIVE_MEMORY_LIMIT_BYTES.swap(1024 * 1024, Ordering::AcqRel);
        reset_controlled_test_counters();
        let admitted = Arc::new(std::sync::Barrier::new(9));
        let release = Arc::new(std::sync::Barrier::new(9));
        let mut workers = Vec::new();
        for _ in 0..8 {
            let admitted = Arc::clone(&admitted);
            let release = Arc::clone(&release);
            workers.push(std::thread::spawn(move || {
                let charge = ControlledMemoryCharge::try_new(
                    ControlledMemoryCategory::DerivedCache,
                    384 * 1024,
                )
                .ok();
                admitted.wait();
                release.wait();
                charge.is_some()
            }));
        }
        admitted.wait();
        let (current, peak, categories, rejections) = controlled_memory_snapshot();
        assert!(current <= operational_memory_limit_bytes());
        assert!(peak <= operational_memory_limit_bytes());
        assert_eq!(current, categories[3]);
        assert!(rejections > 0);
        release.wait();
        assert_eq!(
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .filter(|admitted| *admitted)
                .count(),
            2
        );
        assert_eq!(controlled_memory_snapshot().0, 0);
        MEMORY_TARGET_BYTES.store(prior_target, Ordering::Release);
        EFFECTIVE_MEMORY_LIMIT_BYTES.store(prior_effective, Ordering::Release);
        reset_controlled_test_counters();
    }

    #[test]
    fn prepaid_worker_leases_transfer_at_full_quota_and_release_independently() {
        let _serial = GLOBAL_MEMORY_TEST_LOCK.lock().unwrap();
        let prior_target = MEMORY_TARGET_BYTES.swap(1024 * 1024, Ordering::AcqRel);
        let prior_effective = EFFECTIVE_MEMORY_LIMIT_BYTES.swap(1024 * 1024, Ordering::AcqRel);
        reset_controlled_test_counters();
        let limit = operational_memory_limit_bytes();
        let mut owner =
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, limit)
                .unwrap();
        let worker = owner.split(64 * 1024);
        assert_eq!(controlled_memory_snapshot().0, limit);
        assert_eq!(owner.bytes() + worker.bytes(), limit);
        drop(owner);
        assert_eq!(controlled_memory_snapshot().0, worker.bytes());
        std::thread::spawn(move || drop(worker)).join().unwrap();
        assert_eq!(controlled_memory_snapshot().0, 0);
        MEMORY_TARGET_BYTES.store(prior_target, Ordering::Release);
        EFFECTIVE_MEMORY_LIMIT_BYTES.store(prior_effective, Ordering::Release);
        reset_controlled_test_counters();
    }

    #[test]
    fn slow_admission_retries_after_peer_releases_memory() {
        let _serial = GLOBAL_MEMORY_TEST_LOCK.lock().unwrap();
        let prior_target = MEMORY_TARGET_BYTES.swap(1024 * 1024, Ordering::AcqRel);
        let prior_effective = EFFECTIVE_MEMORY_LIMIT_BYTES.swap(1024 * 1024, Ordering::AcqRel);
        reset_controlled_test_counters();

        let filler = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::SystemMetadata,
            operational_memory_limit_bytes(),
        )
        .unwrap();
        let admission = RECLAIM_ADMISSION.get_or_init(|| Mutex::new(()));
        let held = admission.lock().unwrap();
        let worker = std::thread::spawn(|| {
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::Executor, 64 * 1024)
        });
        std::thread::sleep(std::time::Duration::from_millis(10));
        drop(filler);
        drop(held);
        let charge = worker.join().unwrap().unwrap();
        assert!(admission_reclaim_snapshot().retry_successes > 0);
        drop(charge);
        assert_eq!(controlled_memory_snapshot().0, 0);

        MEMORY_TARGET_BYTES.store(prior_target, Ordering::Release);
        EFFECTIVE_MEMORY_LIMIT_BYTES.store(prior_effective, Ordering::Release);
        reset_controlled_test_counters();
    }

    #[test]
    fn configured_pool_split_must_fit_memory_target() {
        let _serial = GLOBAL_MEMORY_TEST_LOCK.lock().unwrap();
        let prior_target = MEMORY_TARGET_BYTES.swap(100 * 1024 * 1024, Ordering::AcqRel);
        let prior_effective =
            EFFECTIVE_MEMORY_LIMIT_BYTES.swap(100 * 1024 * 1024, Ordering::AcqRel);
        let prior_executor = EXECUTOR_MEMORY_TARGET_BYTES.swap(30 * 1024 * 1024, Ordering::AcqRel);
        let pages_60_mib = (60 * 1024 * 1024 / crate::storage::PAGE_SIZE) as u64;
        validate_controlled_memory_split(pages_60_mib).unwrap();
        let pages_81_mib = (81 * 1024 * 1024 / crate::storage::PAGE_SIZE) as u64;
        let error = validate_controlled_memory_split(pages_81_mib).unwrap_err();
        assert_eq!(error.code(), 1231);
        EXECUTOR_MEMORY_TARGET_BYTES.store(prior_executor, Ordering::Release);
        MEMORY_TARGET_BYTES.store(prior_target, Ordering::Release);
        EFFECTIVE_MEMORY_LIMIT_BYTES.store(prior_effective, Ordering::Release);
    }

    #[test]
    fn raw_storage_and_executor_share_the_process_quota() {
        let _serial = GLOBAL_MEMORY_TEST_LOCK.lock().unwrap();
        let prior_target =
            MEMORY_TARGET_BYTES.swap(5 * crate::storage::PAGE_SIZE, Ordering::AcqRel);
        let prior_effective =
            EFFECTIVE_MEMORY_LIMIT_BYTES.swap(5 * crate::storage::PAGE_SIZE, Ordering::AcqRel);
        let prior_raw_limit = CONTROLLED_BUFFER_RAW_LIMIT_BYTES.load(Ordering::Acquire);
        let prior_buffer_limit = CONTROLLED_BUFFER_LIMIT_BYTES.load(Ordering::Acquire);
        reset_controlled_test_counters();
        set_controlled_buffer_limit_pages(2);
        CONTROLLED_BUFFER_LIMIT_BYTES.store(3 * crate::storage::PAGE_SIZE, Ordering::Release);
        let raw1 = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::BufferRaw,
            crate::storage::PAGE_SIZE,
        )
        .unwrap();
        let raw2 = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::BufferRaw,
            crate::storage::PAGE_SIZE,
        )
        .unwrap();
        let scratch = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::StorageScratch,
            crate::storage::PAGE_SIZE,
        )
        .unwrap();
        let executor = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::Executor,
            crate::storage::PAGE_SIZE,
        )
        .unwrap();
        let total_error = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::StorageScratch,
            crate::storage::PAGE_SIZE,
        )
        .unwrap_err();
        assert_eq!(total_error.code(), 1037);
        assert_eq!(controlled_memory_snapshot().2[2], crate::storage::PAGE_SIZE);
        drop((raw1, raw2, scratch, executor));
        assert_eq!(controlled_memory_snapshot().0, 0);
        MEMORY_TARGET_BYTES.store(prior_target, Ordering::Release);
        EFFECTIVE_MEMORY_LIMIT_BYTES.store(prior_effective, Ordering::Release);
        reset_controlled_test_counters();
        CONTROLLED_BUFFER_RAW_LIMIT_BYTES.store(prior_raw_limit, Ordering::Release);
        CONTROLLED_BUFFER_LIMIT_BYTES.store(prior_buffer_limit, Ordering::Release);
    }

    #[test]
    fn storage_latch_admission_rejects_without_reclaim_and_restores_on_unwind() {
        let _serial = GLOBAL_MEMORY_TEST_LOCK.lock().unwrap();
        let prior_target = MEMORY_TARGET_BYTES.swap(10 * 1024 * 1024, Ordering::AcqRel);
        let prior_effective = EFFECTIVE_MEMORY_LIMIT_BYTES.swap(10 * 1024 * 1024, Ordering::AcqRel);
        reset_controlled_test_counters();
        let ordinary = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::SystemMetadata,
            operational_memory_limit_bytes(),
        )
        .unwrap();
        let before = controlled_memory_snapshot().0;
        let attempts = RECLAIM_ATTEMPTS.load(Ordering::Relaxed);
        {
            // This is the actual admission mutex: attempting reclaim from
            // the nested scope would deadlock, not merely change a counter.
            let _admission = RECLAIM_ADMISSION
                .get_or_init(|| Mutex::new(()))
                .lock()
                .unwrap();
            let outer = StorageLatchAdmissionScope::enter();
            assert!(std::panic::catch_unwind(|| {
                let _inner = StorageLatchAdmissionScope::enter();
                assert!(ControlledMemoryCharge::try_new(
                    ControlledMemoryCategory::StorageScratch,
                    1
                )
                .is_err());
                panic!("injected storage latch unwinding");
            })
            .is_err());
            assert!(storage_latch_admission_active());
            assert_eq!(controlled_memory_snapshot().0, before);
            assert_eq!(RECLAIM_ATTEMPTS.load(Ordering::Relaxed), attempts);
            let critical = with_critical_memory_reservations(|| {
                ControlledMemoryCharge::try_new(
                    ControlledMemoryCategory::StorageScratch,
                    1024 * 1024,
                )
            })
            .unwrap();
            assert_eq!(controlled_memory_snapshot().0, managed_memory_limit_bytes());
            assert!(with_critical_memory_reservations(|| {
                ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, 1)
            })
            .is_err());
            drop(critical);
            drop(outer);
            assert!(!storage_latch_admission_active());
        }
        assert!(
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, 1).is_err()
        );
        assert!(
            RECLAIM_ATTEMPTS.load(Ordering::Relaxed) > attempts,
            "ordinary admission must reclaim again after dropping the scope"
        );
        drop(ordinary);
        assert_eq!(controlled_memory_snapshot().0, 0);
        MEMORY_TARGET_BYTES.store(prior_target, Ordering::Release);
        EFFECTIVE_MEMORY_LIMIT_BYTES.store(prior_effective, Ordering::Release);
        reset_controlled_test_counters();
    }

    #[test]
    fn rollback_critical_scope_can_use_only_the_managed_emergency_slice() {
        let _serial = GLOBAL_MEMORY_TEST_LOCK.lock().unwrap();
        let prior_target = MEMORY_TARGET_BYTES.swap(10 * 1024 * 1024, Ordering::AcqRel);
        let prior_effective = EFFECTIVE_MEMORY_LIMIT_BYTES.swap(10 * 1024 * 1024, Ordering::AcqRel);
        reset_controlled_test_counters();

        assert_eq!(operational_memory_limit_bytes(), 8 * 1024 * 1024);
        assert_eq!(managed_memory_limit_bytes(), 9 * 1024 * 1024);
        let ordinary = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::SystemMetadata,
            operational_memory_limit_bytes(),
        )
        .unwrap();
        assert!(
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, 1,).is_err()
        );

        let critical = with_critical_memory_reservations(|| {
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, 1024 * 1024)
        })
        .unwrap();
        assert_eq!(controlled_memory_snapshot().0, managed_memory_limit_bytes());
        assert!(with_critical_memory_reservations(|| {
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, 1)
        })
        .is_err());

        drop((critical, ordinary));
        MEMORY_TARGET_BYTES.store(prior_target, Ordering::Release);
        EFFECTIVE_MEMORY_LIMIT_BYTES.store(prior_effective, Ordering::Release);
        reset_controlled_test_counters();
    }

    #[test]
    fn optional_cache_reclaim_preserves_admission_limits_and_storage_latches() {
        let _serial = GLOBAL_MEMORY_TEST_LOCK.lock().unwrap();
        let prior_target = MEMORY_TARGET_BYTES.swap(100 * 1024 * 1024, Ordering::AcqRel);
        let prior_effective =
            EFFECTIVE_MEMORY_LIMIT_BYTES.swap(100 * 1024 * 1024, Ordering::AcqRel);
        reset_controlled_test_counters();
        let prior_raw = CONTROLLED_BUFFER_RAW_LIMIT_BYTES.swap(60 * 1024 * 1024, Ordering::AcqRel);
        let prior_buffer = CONTROLLED_BUFFER_LIMIT_BYTES.swap(60 * 1024 * 1024, Ordering::AcqRel);
        let mut raw = Some(
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::BufferRaw, 50 * 1024 * 1024)
                .unwrap(),
        );
        let metadata = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::SystemMetadata,
            10 * 1024 * 1024,
        )
        .unwrap();
        {
            let _latch = StorageLatchAdmissionScope::enter();
            assert!(ControlledMemoryCharge::try_new_optional_with_cache_reclaim(
                ControlledMemoryCategory::DerivedCache,
                30 * 1024 * 1024,
                |_| panic!("reclaim under a storage latch"),
            )
            .is_none());
        }
        let mut calls = 0;
        let cache = ControlledMemoryCharge::try_new_optional_with_cache_reclaim(
            ControlledMemoryCategory::DerivedCache,
            30 * 1024 * 1024,
            |target| {
                calls += 1;
                assert!(target >= 26 * 1024 * 1024);
                drop(raw.take());
            },
        )
        .unwrap();
        assert_eq!(calls, 1);
        assert_eq!(controlled_memory_snapshot().0, 40 * 1024 * 1024);
        assert_eq!(operational_memory_limit_bytes(), 80 * 1024 * 1024);
        assert_eq!(CONTROLLED_REJECTIONS.load(Ordering::Relaxed), 0);
        drop((cache, metadata));
        assert_eq!(controlled_memory_snapshot().0, 0);
        MEMORY_TARGET_BYTES.store(prior_target, Ordering::Release);
        EFFECTIVE_MEMORY_LIMIT_BYTES.store(prior_effective, Ordering::Release);
        reset_controlled_test_counters();
        CONTROLLED_BUFFER_RAW_LIMIT_BYTES.store(prior_raw, Ordering::Release);
        CONTROLLED_BUFFER_LIMIT_BYTES.store(prior_buffer, Ordering::Release);
    }

    #[test]
    fn performance_schema_optional_state_has_an_independent_cap() {
        let _serial = GLOBAL_MEMORY_TEST_LOCK.lock().unwrap();
        let prior_target = MEMORY_TARGET_BYTES.swap(100 * 1024 * 1024, Ordering::AcqRel);
        let prior_effective =
            EFFECTIVE_MEMORY_LIMIT_BYTES.swap(100 * 1024 * 1024, Ordering::AcqRel);
        reset_controlled_test_counters();

        assert_eq!(performance_schema_memory_limit_bytes(), 5 * 1024 * 1024);
        let history = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::PerformanceSchema,
            4 * 1024 * 1024,
        )
        .unwrap();
        assert!(ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::PerformanceSchema,
            2 * 1024 * 1024,
        )
        .is_none());
        assert!(with_critical_memory_reservations(|| {
            ControlledMemoryCharge::try_new_optional(
                ControlledMemoryCategory::PerformanceSchema,
                2 * 1024 * 1024,
            )
        })
        .is_none());

        drop(history);
        MEMORY_TARGET_BYTES.store(prior_target, Ordering::Release);
        EFFECTIVE_MEMORY_LIMIT_BYTES.store(prior_effective, Ordering::Release);
        reset_controlled_test_counters();
    }

    #[test]
    fn lowering_soft_target_succeeds_without_changing_hard_limits() {
        let _serial = GLOBAL_MEMORY_TEST_LOCK.lock().unwrap();
        let prior_target = MEMORY_TARGET_BYTES.swap(64 * 1024 * 1024, Ordering::AcqRel);
        let prior_explicit = MEMORY_TARGET_EXPLICIT.swap(true, Ordering::AcqRel);
        let prior_effective = EFFECTIVE_MEMORY_LIMIT_BYTES.swap(64 * 1024 * 1024, Ordering::AcqRel);
        reset_controlled_test_counters();
        let charge = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::DerivedCache,
            2 * 1024 * 1024,
        )
        .unwrap();

        set_memory_target_mb(1).unwrap();
        assert_eq!(memory_target_bytes(), 1024 * 1024);
        assert_eq!(
            operational_memory_limit_bytes(),
            64 * 1024 * 1024 * 80 / 100
        );
        drop(charge);
        MEMORY_TARGET_BYTES.store(prior_target, Ordering::Release);
        MEMORY_TARGET_EXPLICIT.store(prior_explicit, Ordering::Release);
        EFFECTIVE_MEMORY_LIMIT_BYTES.store(prior_effective, Ordering::Release);
        reset_controlled_test_counters();
    }

    #[test]
    fn hard_limits_follow_effective_memory_not_soft_target() {
        let _serial = GLOBAL_MEMORY_TEST_LOCK.lock().unwrap();
        let prior_target = MEMORY_TARGET_BYTES.swap(1024 * 1024, Ordering::AcqRel);
        let prior_explicit = MEMORY_TARGET_EXPLICIT.swap(true, Ordering::AcqRel);
        let prior_effective = EFFECTIVE_MEMORY_LIMIT_BYTES.swap(10 * 1024 * 1024, Ordering::AcqRel);

        assert_eq!(memory_target_bytes(), 1024 * 1024);
        assert_eq!(operational_memory_limit_bytes(), 8 * 1024 * 1024);
        assert_eq!(managed_memory_limit_bytes(), 9 * 1024 * 1024);

        MEMORY_TARGET_BYTES.store(prior_target, Ordering::Release);
        MEMORY_TARGET_EXPLICIT.store(prior_explicit, Ordering::Release);
        EFFECTIVE_MEMORY_LIMIT_BYTES.store(prior_effective, Ordering::Release);
    }

    #[test]
    fn charge_and_release_roundtrip() {
        let b = MemBudget::new(1024);
        assert!(b.charge(512).is_ok());
        assert_eq!(b.used(), 512);
        b.release(256);
        assert_eq!(b.used(), 256);
    }

    #[test]
    fn ordinary_charge_observes_soft_limit() {
        let b = MemBudget::new(1024);
        b.charge(900).unwrap();
        let error = b.charge(200).unwrap_err();
        assert_eq!(error.code(), 1037);
        assert_eq!(b.used(), 900);
        b.release(900);
        assert_eq!(b.used(), 0);
    }

    #[test]
    fn spillable_charge_still_observes_soft_limit() {
        let b = MemBudget::new(1024);
        assert!(matches!(
            b.charge_for_spillable_operator(900),
            OperatorCharge::Granted
        ));
        assert!(matches!(
            b.charge_for_spillable_operator(200),
            OperatorCharge::SoftLimit(_)
        ));
        assert_eq!(b.used(), 900);
    }

    #[test]
    fn guard_releases_on_drop() {
        let b = MemBudget::new(1024);
        {
            let _g = MemCharge::new(&b, 600).unwrap();
            assert_eq!(b.used(), 600);
        }
        assert_eq!(b.used(), 0);
    }

    #[test]
    fn should_spill_at_90_percent() {
        let b = MemBudget::new(1000);
        b.charge(800).unwrap();
        assert!(!b.should_spill());
        b.charge(100).unwrap();
        assert!(b.should_spill());
    }

    #[test]
    fn add_within_charges_under_cap_and_bails_over_cap() {
        // No active query budget: add_force is a no-op on the global
        // counter, but the guard still tracks its own accumulated bytes.
        let mut g = ScopedCharge::new(0);
        assert!(g.add_within(100, 250));
        assert_eq!(g.charged(), 100);
        assert!(g.add_within(100, 250));
        assert_eq!(g.charged(), 200);
        // Third charge would exceed the cap: nothing charged, signal bail.
        assert!(!g.add_within(100, 250));
        assert_eq!(g.charged(), 200);
        // A smaller charge that still fits is admitted.
        assert!(g.add_within(50, 250));
        assert_eq!(g.charged(), 250);
    }

    #[test]
    fn add_within_never_raises_over_budget() {
        // Even with a tiny active budget, add_within force-charges within
        // its own cap and never returns an error / 1037.
        let b = Arc::new(MemBudget::new(64));
        with_query_budget(b.clone(), || {
            let mut g = ScopedCharge::new(0);
            // cap far above the 64-byte budget: still no 1037, just records.
            assert!(g.add_within(1_000, 10_000));
            assert_eq!(g.charged(), 1_000);
        });
    }

    #[test]
    fn add_within_hard_rejection_allows_spill_fallback() {
        let _serial = GLOBAL_MEMORY_TEST_LOCK.lock().unwrap();
        with_test_effective_memory_limit(1024 * 1024, || {
            let budget = Arc::new(MemBudget::process_tracked_for_test(64));
            with_query_budget(budget.clone(), || {
                let before = controlled_memory_snapshot().0;
                let mut charge = ScopedCharge::new(0);
                assert!(!charge.add_within(1024 * 1024, 2 * 1024 * 1024));
                assert_eq!(charge.charged(), 0);
                assert_eq!(budget.used(), 0);
                assert_eq!(controlled_memory_snapshot().0, before);
                assert!(budget.terminal_oom().is_none());
                crate::engine::cancel::check_cancelled().unwrap();
                assert!(charge.add_within(32 * 1024, 2 * 1024 * 1024));
            });
            assert_eq!(budget.used(), 0);
        });
    }

    #[test]
    fn track_process_flag_is_off_for_plain_new() {
        // Unit tests rely on `MemBudget::new(...)` not affecting the
        // global counter, otherwise parallel tests interfere.
        let before = process_query_mem_used();
        let b = MemBudget::new(1024);
        b.charge(512).unwrap();
        assert_eq!(process_query_mem_used(), before);
        b.release(512);
        assert_eq!(process_query_mem_used(), before);
    }

    #[test]
    fn concurrent_release_is_saturating() {
        let budget = Arc::new(MemBudget::new(4096));
        budget.charge(1024).unwrap();
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let budget = Arc::clone(&budget);
                scope.spawn(move || budget.release(1024));
            }
        });
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn query_memory_scope_restores_after_panic() {
        let outer = Arc::new(MemBudget::new(4096));
        let inner = Arc::new(MemBudget::new(4096));
        with_query_budget(Arc::clone(&outer), || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                with_query_budget(Arc::clone(&inner), || panic!("boom"));
            }));
            assert!(result.is_err());
            assert_eq!(current_budget().unwrap().query_id(), outer.query_id());
        });
        assert!(current_budget().is_none());
    }

    #[test]
    fn scoped_charge_releases_original_owner_across_contexts() {
        let first = Arc::new(MemBudget::new(4096));
        let second = Arc::new(MemBudget::new(4096));
        let charge = with_query_budget(Arc::clone(&first), || {
            ScopedCharge::new_strict(512, "owner-test").unwrap()
        });
        assert_eq!(first.used(), 512);
        with_query_budget(Arc::clone(&second), || drop(charge));
        assert_eq!(first.used(), 0);
        assert_eq!(second.used(), 0);
    }

    #[test]
    fn non_spillable_force_charge_crosses_soft_but_not_hard_target() {
        let _serial = GLOBAL_MEMORY_TEST_LOCK.lock().unwrap();
        let old_target = MEMORY_TARGET_BYTES.swap(2 * 1024 * 1024, Ordering::AcqRel);
        let old_effective = EFFECTIVE_MEMORY_LIMIT_BYTES.swap(2 * 1024 * 1024, Ordering::AcqRel);
        let old_limit = PROCESS_MEM_LIMIT_BYTES.swap(1024 * 1024, Ordering::AcqRel);
        reset_controlled_test_counters();
        PROCESS_QUERY_MEM_USED.store(0, Ordering::Release);
        PROCESS_QUERY_MEM_HIGH_WATER.store(0, Ordering::Release);
        let budget = Arc::new(MemBudget {
            query_id: NEXT_QUERY_MEMORY_ID.fetch_add(1, Ordering::Relaxed),
            used: AtomicUsize::new(0),
            peak_used: AtomicUsize::new(0),
            limit: 64,
            track_process: true,
            spill_bytes: AtomicU64::new(0),
            owner: None,
            reclaim_database: None,
            terminal_oom: OnceLock::new(),
        });
        with_query_budget(Arc::clone(&budget), || {
            let mut charge = ScopedCharge::new(0);
            charge.add_non_spillable(768 * 1024).unwrap();
            assert_eq!(process_query_mem_used(), 768 * 1024);
            assert_eq!(process_query_mem_high_water(), 768 * 1024);
            assert_eq!(controlled_memory_snapshot().0, 768 * 1024);
            assert_eq!(controlled_memory_snapshot().2[2], 768 * 1024);

            let error = charge.add_non_spillable(1024 * 1024).unwrap_err();
            assert_eq!(error.code(), 1037);
            assert_eq!(error.sql_state().as_str(), "HY001");
            assert!(error.message().contains("reclaim before 1037"));
            assert_eq!(budget.used(), 768 * 1024);
            assert_eq!(process_query_mem_used(), 768 * 1024);
            assert_eq!(controlled_memory_snapshot().0, 768 * 1024);
            assert_eq!(
                crate::engine::cancel::check_cancelled().unwrap_err().code(),
                1037
            );
        });
        budget.release(768 * 1024);
        assert_eq!(process_query_mem_used(), 0);
        assert_eq!(controlled_memory_snapshot().0, 0);
        PROCESS_MEM_LIMIT_BYTES.store(old_limit, Ordering::Release);
        MEMORY_TARGET_BYTES.store(old_target, Ordering::Release);
        EFFECTIVE_MEMORY_LIMIT_BYTES.store(old_effective, Ordering::Release);
        reset_controlled_test_counters();
    }

    #[test]
    fn fallible_vec_capacity_failure_latches_query_oom() {
        let budget = Arc::new(MemBudget::new(1024));
        with_query_budget(Arc::clone(&budget), || {
            let mut values = Vec::<u64>::new();
            let error = try_reserve_vec_exact(&mut values, usize::MAX, "test vector").unwrap_err();
            assert_eq!(error.code(), 1037);
            assert_eq!(crate::engine::cancel::check_cancelled().unwrap_err(), error);
        });
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn terminal_oom_is_shared_across_query_workers() {
        let budget = Arc::new(MemBudget::new(1024));
        let published = Arc::new(std::sync::Barrier::new(2));
        std::thread::scope(|scope| {
            let failing_budget = Arc::clone(&budget);
            let failing_barrier = Arc::clone(&published);
            let failing = scope.spawn(move || {
                with_query_budget(failing_budget, || {
                    let error = allocation_error("worker allocation failed");
                    failing_barrier.wait();
                    error
                })
            });
            let observing_budget = Arc::clone(&budget);
            let observing = scope.spawn(move || {
                with_query_budget(observing_budget, || {
                    published.wait();
                    crate::engine::cancel::check_cancelled().unwrap_err()
                })
            });
            let primary = failing.join().unwrap();
            let sibling = observing.join().unwrap();
            assert_eq!(primary, sibling);
            assert_eq!(sibling.code(), 1037);
        });
    }
}

/// Headroom retained for LOAD DATA's non-buffer working memory.
#[inline]
pub(crate) fn load_data_ancillary_headroom(operational_limit: usize) -> usize {
    (operational_limit / 20)
        .clamp(8 * 1024 * 1024, 64 * 1024 * 1024)
        .min(operational_limit / 4)
}

/// Controlled-memory target reachable by reclaiming buffer bytes above its floor.
#[inline]
pub(crate) fn load_data_reclaim_target(
    controlled: usize,
    buffer_raw: usize,
    requested: usize,
    ancillary_headroom: usize,
    min_buffer_bytes: usize,
    operational_limit: usize,
) -> Option<usize> {
    let reclaim_bytes = controlled
        .saturating_add(requested)
        .saturating_add(ancillary_headroom)
        .saturating_sub(operational_limit);
    if reclaim_bytes == 0 || reclaim_bytes > buffer_raw.saturating_sub(min_buffer_bytes) {
        return None;
    }
    Some(controlled.saturating_sub(reclaim_bytes))
}

#[cfg(test)]
mod spill_capacity_overlap_tests {
    use super::*;

    #[test]
    fn charged_vector_growth_covers_old_and_new_allocations_at_once() {
        let budget = Arc::new(MemBudget::new(1024 * 1024));
        with_query_budget(budget.clone(), || {
            let mut charge = ScopedCharge::new(0);
            charge.add_force(512 * std::mem::size_of::<u64>()).unwrap();
            let mut values = Vec::with_capacity(512);
            values.extend(0..512u64);
            let old_bytes = values.capacity() * std::mem::size_of::<u64>();
            try_reserve_vec_hard(&mut values, 1, &mut charge, "overlap regression").unwrap();
            let new_bytes = values.capacity() * std::mem::size_of::<u64>();
            assert!(
                budget.peak_used() >= old_bytes + new_bytes,
                "missing realloc overlap: peak={}, old={old_bytes}, new={new_bytes}",
                budget.peak_used()
            );
            assert_eq!(charge.charged(), new_bytes);
            assert_eq!(budget.used(), new_bytes);
            drop(values);
            drop(charge);
            assert_eq!(budget.used(), 0);
        });
    }
}
