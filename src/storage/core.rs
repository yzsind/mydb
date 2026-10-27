use crate::platform::file::{File, OpenOptions};
use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque};
use std::env;
use std::fs;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{
    AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering,
};
use std::sync::{
    Arc, Condvar, Mutex, MutexGuard, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard,
};
use std::thread;
use std::time::{Duration, Instant};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use super::recovery_context::{DatabaseOpenMode, RecoveryContext};
#[cfg(test)]
use crate::parser::DataType;
use crate::parser::{ColumnDef, Expr, Literal, QuantifiedComparisonRight, UnaryOp};
use crate::types::{QueryError, SmallStringBuf, Value};
use crate::util::cache_pad::CachePadded;
use crate::util::logging::log_message;
use crate::wal::{
    page_lsn, set_page_lsn, LogManager, WalColumnPatch, WalHeapInsert, WalIndexOp, WalIndexOpKind,
    WalPayload, WalRecord, WalRowId,
};

use super::format_versions::{HEAP_PAGE_VERSION, MVCC_ROW_VERSION};
use super::mdl::{MdlDuration, MdlLockType, MdlManager, MdlObjectKey, MdlRequest, MetadataLockRow};
use super::naming::{
    current_unix_ms, event_key, is_system_database, normalize_name, normalize_name_cow,
    normalize_table_key, routine_key, split_table_name, table_key, trigger_key,
};
use super::schema::{
    build_fk_runtime, Catalog, CheckConstraintSchema, ColumnSchema, ColumnState, DataTypeKind,
    DatabaseSchema, EventSchema, EventStatusSchema, ForeignKeySchema, InboundFk, IndexBound,
    IndexKey, IndexKeyPart, IndexSchema, IndexState, KeyPart, ParamTarget, ReferentialAction,
    RoutineKindSchema, RoutineSchema, StoredRow, TableSchema, TriggerEventSchema,
    TriggerOrderKindSchema, TriggerOrderSchema, TriggerSchema, TriggerTimingSchema, ViewSchema,
    DEFAULT_DATABASE,
};
#[cfg(test)]
use super::stats::top_value_limit;
#[cfg(test)]
use super::stats::ValueFrequency;
use super::stats::{
    analyze_sample_pages, estimate_analyzed_row_count, stats_hash_index_parts, ColumnStatsBuilder,
    ConditionalKeyDomainBuilder, DistinctStatsBuilder, IndexStats, PartitionStats, TableStats,
    ANALYZE_EXACT_ROW_LIMIT,
};
use super::temporal::TemporalHistory;
use super::txn::{
    AutoIncrementManager, AutoIncrementState, DurabilityWorker, FastHashMap,
    GroupCommitCoordinator, HeldLock, IndexRepairEpochs, IndexRepairKey, IsolationLevel,
    LockBlockers, LockManager as TxnLockManager, LockMode, LockOwnerCheck, LockWaitResource,
    RowLockState, RowWaiter, StatementSavepoint, TableLockMode, TableLockState, TransactionManager,
    TxnSlot, TxnStatus, TxnView, UndoHead,
};
use super::undo::{UndoHistoryEntry, UndoKind, UndoRecord, UndoTablespace, UndoWriteShardHandle};
use super::value::{to_f64, to_i64, to_u64, value_to_string};
use super::wait_events::{WaitEvent, WaitEventHistory, WaitEventSample};

// P0/4 — slot-reclaim retry tuning. Shared by `Database` field defs
// (txn_ops.rs::reclaim_tombstoned_slots) and the regression tests
// that reach in via `cfg(test)` accessors.
pub(crate) const SLOT_RECLAIM_MAX_ATTEMPTS: u8 = 8;
pub(crate) const SLOT_RECLAIM_QUARANTINE_CAP: usize = 4096;
pub(crate) const SLOT_RECLAIM_QUARANTINE_DRAIN_PER_RUN: usize = 256;
const LOB_RECLAIM_QUEUE_CAP: usize = 4096;
const LOB_RECLAIM_DRAIN_PER_RUN: usize = 64;
const LOB_RECLAIM_MAX_ATTEMPTS: u8 = 8;
#[cfg(test)]
static REPLICATED_TWO_OP_FAST_PATH_HITS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static REPLICATED_SORTED_INSERT_FAST_PATH_HITS: AtomicU64 = AtomicU64::new(0);

pub(crate) const PAGE_SIZE: usize = 16 * 1024;
const HEAP_PAGE_REDUCED_REDO: u16 = 1;
static INDEX_BUILD_REQUESTED_WORKERS: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_EFFECTIVE_WORKERS: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_PARALLEL_BUILDS: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_SERIAL_BUILDS: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_DEGREE_DOWNGRADES: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_SCAN_US: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_SORT_US: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_LEAF_US: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_INTERNAL_US: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_FSYNC_US: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_ROWS: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_HEAP_SCANS: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_MERGE_US: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_SPILL_WRITE_US: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_SPILL_READ_US: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_ACTIVE_WORKERS: AtomicU64 = AtomicU64::new(0);
static INDEX_BUILD_PEAK_ACTIVE_WORKERS: AtomicU64 = AtomicU64::new(0);

/// Statement-scoped worker budget for an index build.  It is resolved once
/// by the SQL layer (or by the public compatibility wrapper) and then passed
/// through every storage phase so nested sort/build code never re-reads the
/// process environment and cannot multiply an outer and inner worker pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexBuildOptions {
    pub requested_degree: usize,
    pub effective_degree: usize,
    pub explicit_hint: bool,
}

impl IndexBuildOptions {
    pub fn new(requested_degree: usize, explicit_hint: bool) -> Self {
        let requested_degree = requested_degree.max(1);
        Self {
            requested_degree,
            effective_degree: requested_degree,
            explicit_hint,
        }
    }

    pub fn from_environment() -> Self {
        let disabled = crate::util::env::var("MYDB_PARALLEL_DISABLE")
            .ok()
            .is_some_and(|value| {
                value == "1"
                    || value.eq_ignore_ascii_case("true")
                    || value.eq_ignore_ascii_case("on")
            });
        let degree = if disabled {
            1
        } else {
            crate::util::env::var("MYDB_PARALLEL_DEGREE")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|degree| *degree >= 1)
                .unwrap_or(2)
        };
        Self::new(degree, false)
    }

    pub fn serial() -> Self {
        Self::new(1, true)
    }
}

impl Default for IndexBuildOptions {
    fn default() -> Self {
        Self::from_environment()
    }
}
// Heap common header (16 B LSN + 4 B checksum + 4 B slot/free-start/
// free-end + 8 B header padding). Kept at 32 B for SIMD-friendly
// 16-B alignment of the bitmap region at offset 16; bytes 26–31 are
// reserved for future use (e.g. persisted `deleted_count` or free
// slot list head — see docs/storage-compression-opportunities.md
// optimisation E).
const PAGE_HEADER_SIZE: usize = 32;
// Slot directory entry size. Was 8 bytes pre-rowid-6bytes (offset:2 +
// len:2 + generation:2 + flags:1 + pad:1), then 6 bytes after that
// refactor (offset:2 + len:2 + flags:1 + pad:1). v10 (HEAP_PAGE_VERSION)
// drops the trailing `pad:1` filler — every slot is now offset:2 +
// len:2 + flags:1 = 5 bytes.
const SLOT_SIZE: usize = 5;
const DEFAULT_HEAP_UPDATE_RESERVE_BYTES: usize = PAGE_SIZE / 16;
pub(crate) const MAX_HEAP_UPDATE_RESERVE_BYTES: usize = PAGE_SIZE / 2;
const HEAP_PAGE_MAGIC: u32 = 0x4d48_5032;
// Bumped 8 → 9: rowid-6bytes refactor shrank both the per-slot
// directory entry (SLOT_SIZE 8 → 6) and the forwarding stub
// (9 B → 7 B). Old heap pages still validate their checksum but fail
// at decode time, matching the existing "rebuild data dir on version
// mismatch" policy.
// Bumped 9 → 10: first-wave storage compression (optimisation B
// in docs/storage-compression-opportunities.md). The trailing
// `pad:1` byte that the rowid-6bytes refactor left behind is gone;
// SLOT_SIZE 6 → 5.
// Bumped 10 → 11: DATE fixed encoding changed. The old scheme
// (`stored = days + 1`, zero-date sentinel = 0) collided at
// `days = -1` (1969-12-31), silently corrupting that single date to
// `0000-00-00`. New scheme mirrors DateTime/Timestamp: `stored = days`
// as signed LE for real dates, `signed_min_for_width(3) = -8388608`
// as the zero-date sentinel. Both the valid days range
// (-719528..2932896 for 0000-01-01..9999-12-31) and the sentinel fit
// in the 3-byte signed slot without collision.
// Bumped 11 → 12: SET column on-disk width tightened from the
// rounded 1/2/4/8 buckets to MySQL's `ceil(N/8)` (1..8 bytes).
// SET(9..16) is unchanged (2 B), but SET(17..24) shrinks 4 B → 3 B,
// SET(33..40) 8 B → 5 B, etc. Old heap pages fail to decode SET
// columns, matching the existing "rebuild data dir on version
// mismatch" policy.
// Bumped 12 → 13: DATE/DATETIME gained reserved tagged encodings for
// component-valid zero/invalid calendar values accepted by MySQL sql_mode.
// The fixed widths are unchanged, but old readers would interpret the tags
// as ordinary negative dates.
/// Flag bit on `Slot.flags`. Bit value 1.
///
/// Semantics (post rowid-6bytes refactor):
/// - `flags == 0` → slot is `ACTIVE`: holds a live row payload.
/// - `flags & ROW_FLAG_DELETED != 0` → slot is `DELETED_PENDING`: the
///   row has been deleted but is still required for MVCC visibility
///   (some snapshot with `snapshot_scn < row.deleted_scn` may still
///   read it) **or** still has dangling secondary-index entries.
/// - `flags & ROW_FLAG_FREE != 0` → slot is `FREE`: payload region
///   has been reclaimed, the slot id is safe to reuse on the next
///   INSERT (no MVCC reader can reach it and no secondary-index
///   entry points at this `(page_id, slot_id)`).
///
/// Until M2 (background slot reclaim + index purge), no slot ever
/// transitions to `FREE`; INSERT always allocates a fresh slot. This
/// is correctness-safe at the cost of slower slot turnover under
/// delete-heavy workloads.
const ROW_FLAG_DELETED: u8 = 1;
/// Flag bit on `Slot.flags`. Bit value 2. See `ROW_FLAG_DELETED`.
const ROW_FLAG_FREE: u8 = 2;
/// Row payload format version. v3 drops the per-column type tag from
/// the value section and dispatches encoding on the table's schema-
/// derived `RowCodec`: integers shrink to their declared 1/2/3/4/8
/// byte width, DATE/DATETIME/TIMESTAMP become 4/8 byte binary,
/// DECIMAL becomes packed sign+magnitude, etc. The variable MVCC
/// header (trims `last_txn_id` / `undo_ptr` when zero) and the
/// per-row NULL bitmap carry over from v2. See `core/row_codec.rs`
/// for the full layout.
// Bumped 3 → 4 when the row format gained an optional "external
// column" bitmap (off-page BLOB/TEXT). The bitmap is present only when
// the row header's codec-internal `FLAG_HAS_EXTERNAL` bit is set, so
// rows with no externalized values are byte-for-byte unchanged.
// Bumped 5 → 6: SET column stored width switched to `ceil(N/8)`
// (see HEAP_PAGE_VERSION bump 11 → 12). Existing rows with SET
// columns in the 17-24 / 33-56 member ranges are no longer
// decodable under the new width formula.
// Bumped 6 → 7: external LOB descriptors grew from 12 to 20 bytes and
// now persist an overflow allocation generation (overflow v3).
const MVCC_FLAG_DELETED: u8 = 1;
const MVCC_FLAG_ROW_PIECE: u8 = 1 << 1;
const HEAP_FORWARDING_STUB_VERSION: u8 = 0xff;
// rowid-6bytes stub layout: `[u8 version][u32 page_id][u16 slot]` = 7 B.
// Was 9 B in v6 (which carried the per-slot `generation`). Forwarding
// chains never leave a single heap file (heap is per-table), so
// `table_id` is also not stored.
const HEAP_FORWARDING_STUB_LEN: usize = 1 + 4 + 2;
/// Hard cap on forwarding-chain hops a read can traverse. Steady-state should
/// be at most 1 (original page → migrated piece), since updates to an already
/// migrated row update the piece in place rather than re-forwarding. Anything
/// greater suggests storage corruption or an unhandled relocation path.
const MAX_FORWARDING_DEPTH: usize = 8;
// A fuzzy checkpoint adds a durable WAL barrier and can briefly perturb every
// strict committer on latency-spiky storage. Ten-second checkpoints repeatedly
// re-ran while the same dirty-page redo floor pinned the WAL, causing multi-
// second stalls without reclaiming history. Keep time-based checkpoints at a
// production-scale minute and let a 4 GiB growth bound force earlier work.
const DEFAULT_CHECKPOINT_INTERVAL_MS: u64 = 60_000;
const DEFAULT_CHECKPOINT_WAL_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MIN_AUTO_CHECKPOINT_INTERVAL_MS: u64 = 5_000;
const MAX_AUTO_CHECKPOINT_INTERVAL_MS: u64 = 15_000;
const CHECKPOINT_TRIGGER_WAL: u8 = 1;
const CHECKPOINT_TRIGGER_TIME: u8 = 2;
const CHECKPOINT_TRIGGER_MANUAL: u8 = 3;
const CHECKPOINT_TRIGGER_SEQUENCE: u8 = 4;
const CHECKPOINT_KIND_FUZZY: u8 = 1;
const CHECKPOINT_KIND_SHARP: u8 = 2;
const CHECKPOINT_KIND_RESTARTPOINT: u8 = 3;
const CHECKPOINT_KIND_MEMORY: u8 = 4;
const DEFAULT_LOCK_WAIT_MS: u64 = 30_000;
/// Group-commit leader wait window. Default to natural batching: concurrent
/// appenders that arrive before/during the fsync are covered by the WAL
/// high-water mark without adding a timer delay to every autocommit. Storage
/// with an unusually expensive sync can opt into a bounded intentional wait
/// through `MYDB_GROUP_COMMIT_US` / `mydb_group_commit_us`.
const DEFAULT_GROUP_COMMIT_US: u64 = 0;
const DEFAULT_WAL_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;
// Compile-time fallback for recovery/test constructors that do not parse the
// environment. Production startup uses the host-aware governed split from
// `resolve_buffer_pool_pages`.
const DEFAULT_BUFFER_POOL_PAGES: u64 = 39_321;
/// N-1 - prefetch window default. The pre-N-1 value of 4 left SSDs
/// idle on sequential scans; 32 is the new floor and lets prefetch
/// keep ~half a megabyte of read-ahead in flight per worker.
const DEFAULT_BUFFER_POOL_PREFETCH_PAGES: usize = 32;
/// Repeated scans of physically small heaps are allowed to become a modest
/// working set after their second pass.  The percentage is runtime tunable;
/// the floor mirrors Oracle's conservative small-table heuristic.
const DEFAULT_BUFFER_POOL_SMALL_SCAN_PCT: u64 = 20;
const MIN_BUFFER_POOL_SMALL_SCAN_PAGES: u64 = 20;
/// N-1 - prefetch ceiling for the adaptive window. The Scan intent
/// can float up to this when the hit rate justifies it; capped to
/// keep queue memory bounded.
const MAX_BUFFER_POOL_PREFETCH_PAGES: usize = 128;
/// N-1 - minimum prefetch the adaptive window can shrink to. Lower
/// than this and we may as well disable prefetch altogether.
const MIN_BUFFER_POOL_PREFETCH_PAGES: usize = 1;
/// Hard ceiling on IO worker threads spawned by a single
/// BufferPool. The effective cap is clamped per-pool to
/// `shard_count` (a worker per shard is the natural maximum), so
/// raising this just lets large-shard pools schedule more IO
/// concurrency. Before M-6 this was 4 — too low for the 16-shard
/// default on a modern multi-core box.
const MAX_BUFFER_POOL_IO_THREADS: usize = 16;
/// N-2 - background page-cleaner start/stop watermarks (percent of buffer
/// pool capacity). These are *defaults*; `MYDB_BUFFER_POOL_DIRTY_HIGH_PCT`
/// and `MYDB_BUFFER_POOL_DIRTY_TARGET_PCT` override them at startup.
///
/// The cleaner is a backstop: it starts flushing once dirty crosses the high
/// water and trickles it back down to the target so a burst of dirtying does
/// not force the foreground evict path to synchronously flush a victim (or
/// the next checkpoint to flush the whole backlog at once). The companion
/// `flush_some_dirty_striped` no-op bug (a worker only flushed when its own
/// 1/stride shard stripe exceeded the *whole-pool* target — i.e. never) is
/// fixed alongside these knobs, so the cleaner now actually runs when dirty
/// exceeds the high water.
///
/// Keep the default cleaner proactive. With a constrained memory target and
/// multi-million-row OLTP updates, a 75/50 high-water policy lets dirty pages
/// sit in the 60-75% band until foreground eviction has to synchronously flush
/// victims. The 40/25 policy keeps a clean-victim supply without weakening
/// checkpoint/durability semantics.
const BUFFER_POOL_DIRTY_HIGH_PCT: u64 = 40;
const BUFFER_POOL_DIRTY_TARGET_PCT: u64 = 25;
/// Number of oldest dirty pages each IO worker retires per 25 ms tick even
/// below the count high-watermark. This advances the redo recovery floor under
/// a steady random-write workload instead of letting a small, stable dirty set
/// pin old WAL segments forever. `MYDB_BUFFER_POOL_AGE_CLEAN_PAGES_PER_TICK=0`
/// disables this maintenance path for diagnostics.
const BUFFER_POOL_AGE_CLEAN_PAGES_PER_TICK: u64 = 4;
/// B1 - the per-shard `dirty_oldest` min-heap accumulates one entry per
/// clean→dirty transition (`note_dirty`) but is only drained by the
/// age-clean pop path (`dirty <= high`) and by DROP/truncate. Under a
/// sustained write workload that keeps `dirty` above the high-water mark
/// the >high-water flush path clears `dirty_set` without touching the
/// heap, so stale entries (already-flushed pages, or old-LSN duplicates
/// of still-dirty pages) pile up without bound — a live memory leak plus
/// a WAL-retention stall (the genuinely-oldest dirty page hides behind a
/// wall of low-LSN stale entries so the redo floor never advances). We
/// bound the heap the same way `push_clock_key` bounds the CLOCK
/// VecDeque: when it grows past `2 ×` the live dirty-set size (and past a
/// small floor so a tiny working set does not thrash the rebuild), rebuild
/// it from the current dirty set so it holds exactly one fresh entry per
/// still-dirty page. Amortised O(1) per dirtying event.
const BUFFER_POOL_DIRTY_OLDEST_MIN_COMPACT: usize = 256;
/// M-6 - panic threshold; once dirty exceeds 90% of capacity the
/// background flusher pushes harder (lower target, more batches per
/// loop) so the foreground evict path doesn't have to do its own
/// WAL fsync to make room.
const BUFFER_POOL_DIRTY_PANIC_PCT: u64 = 90;
/// M-6 - panic-mode flush target. Used only when dirty > 90%; the
/// regular 50% target is too soft to clear backlog before the
/// foreground starts hitting capacity_exhausted errors.
const BUFFER_POOL_DIRTY_PANIC_TARGET_PCT: u64 = 30;
const BUFFER_POOL_FLUSH_BATCH_PAGES: usize = 32;
const BUFFER_POOL_BULK_FLUSH_RANGES: usize = 64;
/// Absolute high/target watermarks on dirty page count, applied as an
/// additional cap on top of `BUFFER_POOL_DIRTY_*_PCT`. A large buffer
/// pool (e.g. `MYDB_BUFFER_POOL_PAGES=865536`, ~14 GiB) configured for
/// a smaller dataset never crosses the 75 %-of-capacity threshold and
/// therefore never runs background flush — so the entire dirty
/// backlog (e.g. all of TPC-H sf=5 lineitem, ~310K pages) lands on the
/// foreground `flush_all` at the post-LOAD checkpoint and stretches a
/// 16-second load into 50+ seconds. Capping the high water at
/// `BUFFER_POOL_DIRTY_ABS_HIGH_PAGES` ensures background flush starts
/// as soon as the dirty backlog reaches ~768 MiB regardless of how
/// large the buffer pool is, so writes overlap with the load instead
/// of stacking up at the end. The small-BP case (`capacity * 75 / 100
/// <= ABS_HIGH`) is unchanged — `min()` picks the relative value.
const BUFFER_POOL_DIRTY_ABS_HIGH_PAGES: u64 = 49_152;
const BUFFER_POOL_DIRTY_ABS_TARGET_PAGES: u64 = 32_768;
/// P0-2 - fraction of pool capacity the absolute dirty high-water /
/// pressure-target caps scale up to once the pool is large. A fixed
/// 768 MiB cap on a multi-GiB pool keeps the cleaner in a permanent
/// high-pressure regime under write-heavy OLTP (the cap is exceeded for
/// the whole run), which defeats cold-first page selection and rewrites
/// hot pages continuously. Small pools keep the historical absolute
/// values (`min()` picks the relative arm below ~3 GiB).
const BUFFER_POOL_DIRTY_ABS_SCALE_DIV: u64 = 4;
const BUFFER_POOL_DIRTY_ABS_TARGET_SCALE_DIV: u64 = 8;
/// P0-2 - how long after a completed write-back a re-dirtied frame is
/// deferred by the background pressure cleaners, in milliseconds.
/// `MYDB_BUFFER_POOL_FLUSH_COOLDOWN_MS` overrides; 0 disables.
const BUFFER_POOL_FLUSH_COOLDOWN_MS: u64 = 1_000;
/// P5 - automatic-checkpoint preflush budget. The background checkpointer
/// uses this before taking the checkpoint mutex so the eventual checkpoint
/// has less dirty backlog to drain in one foreground burst. Keep the defaults
/// deliberately small: full correctness still comes from `flush_all`.
const CHECKPOINT_PREFLUSH_MS: u64 = 40;
const CHECKPOINT_PREFLUSH_TARGET_PCT: u64 = 25;
const CHECKPOINT_PREFLUSH_BATCH_PAGES: u64 = 256;
/// N-3 - upper bound on a single `ensure_capacity` wait loop. We
/// rely on `notify_frame_waiters` (called after every eviction, flush,
/// and pin release) for the common-case wake; this ceiling is a
/// belt-and-suspenders against any missed notify. Was 2ms before
/// N-3; the bump removes ~500 wakeups/s of pure busy waiting on
/// contended OLTP.
const BUFFER_POOL_WAIT_CEILING: Duration = Duration::from_millis(50);
/// D-2 - grace window during which a foreground evictor refuses dirty
/// victims (it raises capacity pressure and lets the IO workers flush
/// instead). After this much accumulated waiting the evictor falls back
/// to the old synchronous flush-and-evict so progress never depends on
/// worker liveness.
const BUFFER_POOL_EVICT_SYNC_GRACE: Duration = Duration::from_millis(20);
/// Above this dirty fraction, avoid an unbounded clean-only CLOCK pass.
/// A bounded clean probe still runs during the dirty-eviction grace window:
/// mixed OLTP can have plenty of clean victims even when 60% of the pool is
/// dirty. All-dirty bulk loads avoid the full clean sweep that previously
/// dominated eviction CPU.
pub(crate) const EVICT_DIRTY_PCT_FOR_PRESCAN_SKIP: u64 = 60;
// MVCC correctness is gated by the oldest active snapshot, independently of
// this best-effort post-commit history window. Retaining every finalized
// autocommit version for 60 seconds created millions of directory entries and
// forced a disruptive purge catch-up under sustained writes. Default to eager
// purge after the snapshot horizon; operators needing flashback headroom can
// opt into a non-zero retention window (and RETENTION GUARANTEE if required).
const DEFAULT_UNDO_RETENTION_MS: u64 = 0;
const DEFAULT_TXN_VISIBILITY_CACHE_LIMIT: usize = 65_536;
pub(crate) const TEMP_TABLE_ID_BASE: u64 = 1_u64 << 63;

#[inline]
pub(crate) fn is_temp_table_id(table_id: u64) -> bool {
    table_id >= TEMP_TABLE_ID_BASE
}

pub(crate) struct TempTableSnapshot {
    // Owns quota accounting and removes partial snapshots on every error path.
    _directory: crate::engine::executor::select::grace::SpillTmpDir,
    _session_charges: Vec<Arc<crate::storage::temp_quota::ByteReservation>>,
    internal_key: String,
    table: Arc<TableSchema>,
    files: Vec<TempTableFileSnapshot>,
}

#[derive(Debug, Clone)]
struct TempTableFileSnapshot {
    original: PathBuf,
    snapshot: PathBuf,
    existed: bool,
}

fn startup_progress_logging_enabled() -> bool {
    #[cfg(test)]
    {
        std::env::var("MYDB_TEST_STARTUP_LOG")
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "on" | "ON"))
            .unwrap_or(false)
    }
    #[cfg(not(test))]
    {
        true
    }
}

/// Per-thread cap on the MVCC visibility cache, configurable via
/// `MYDB_TXN_VISIBILITY_CACHE`. Default 65536 entries (~3 MiB per
/// thread). Smaller values reduce per-thread memory at the cost of
/// hit rate; larger values smooth out long scans over hot rows.
fn txn_visibility_cache_limit() -> NonZeroUsize {
    static CAP: std::sync::OnceLock<NonZeroUsize> = std::sync::OnceLock::new();
    *CAP.get_or_init(|| {
        let raw = std::env::var("MYDB_TXN_VISIBILITY_CACHE")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_TXN_VISIBILITY_CACHE_LIMIT);
        NonZeroUsize::new(raw)
            .unwrap_or_else(|| NonZeroUsize::new(DEFAULT_TXN_VISIBILITY_CACHE_LIMIT).unwrap())
    })
}
pub const LOCK_WAIT_PENDING_MESSAGE: &str = "lock wait pending";

pub(crate) fn lock_wait_pending_error() -> QueryError {
    QueryError::internal_control(
        crate::error::InternalErrorKind::LockWaitPending,
        crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
        LOCK_WAIT_PENDING_MESSAGE,
    )
}

#[inline]
pub(crate) fn is_lock_wait_pending_error(err: &QueryError) -> bool {
    err.is_internal(crate::error::InternalErrorKind::LockWaitPending)
}

static NEXT_VISIBILITY_CACHE_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// Per-thread MVCC transaction visibility cache. Backed by a real LRU
    /// (via `lru::LruCache`) so under sustained pressure we evict the
    /// least-recently-used entry instead of clearing the whole map. Cap
    /// is `txn_visibility_cache_limit()`.
    pub(crate) static TXN_VISIBILITY_CACHE: RefCell<lru::LruCache<(u64, u64), CachedTxnVisibility>> = RefCell::new(
        lru::LruCache::new(txn_visibility_cache_limit())
    );
}

/// Compact, table-scoped row locator.
///
/// 6 bytes on disk (page_id u32 + slot_id u16). Was 8 bytes before the
/// rowid-6bytes refactor; the per-slot `generation` ABA counter is
/// gone. ABA safety is now provided by a slot state machine in the
/// heap page plus a background slot-reclaim pass:
///
///   * `DELETE` marks the heap slot as `DELETED` (logical tombstone)
///     but leaves the slot occupied for MVCC visibility.
///   * A slot only transitions to a reusable `FREE` state once
///     (a) `oldest_active_snapshot_scn > row.deleted_scn`, and
///     (b) every secondary-index entry that points to
///     `(page_id, slot_id)` has been removed.
///   * `INSERT` only ever picks `FREE` slots; until that's true the
///     slot stays `DELETED` and a fresh slot is allocated.
///
/// The table identifier is **not** stored here: every heap fetch /
/// secondary-index lookup runs against a `HeapFile` / `IndexFile`
/// that already knows its owning table. For the rare paths that need
/// cross-table scoping (catalog diagnostics, cross-table debug
/// output), carry the table identity alongside the row id.
///
/// Field order is deliberate: `page_id` first lets the derived `Ord`
/// implementation sort `Vec<RowId>` by page, so index→heap fetch
/// loops enjoy sequential I/O and buffer-pool locality without any
/// extra bucketing step.
///
/// `page_id` is `u32`: 16 KiB pages × 4 G = **64 TiB per table**,
/// well above the 50 M-row tables we currently target.
/// `next_page_id` allocation is guarded against overflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RowId {
    pub page_id: u32,
    pub slot_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertRowsResult {
    pub row_ids: Vec<RowId>,
    pub storage_ids: Vec<u64>,
    pub first_insert_id: u64,
}

impl RowId {
    #[inline]
    pub const fn new(page_id: u32, slot_id: u16) -> Self {
        Self { page_id, slot_id }
    }
}

#[derive(Debug, Clone)]
struct DmlUpdate {
    row_id: RowId,
    header: RowHeader,
    old_values: Vec<Value>,
    new_values: Vec<Value>,
}

#[derive(Debug, Clone)]
struct DmlDelete {
    row_id: RowId,
    header: RowHeader,
    values: Vec<Value>,
}

#[derive(Debug, Clone, Copy)]
struct LobReclaimEntry {
    table_id: u64,
    lob: LobRef,
    attempts: u8,
}

#[derive(Debug)]
struct LobReclaimQueue {
    entries: VecDeque<LobReclaimEntry>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl LobReclaimQueue {
    fn try_new() -> Option<Self> {
        let bytes = LOB_RECLAIM_QUEUE_CAP.saturating_mul(std::mem::size_of::<LobReclaimEntry>());
        let charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
            bytes,
        )?;
        Some(Self {
            entries: VecDeque::with_capacity(LOB_RECLAIM_QUEUE_CAP),
            _charge: charge,
        })
    }
}

/// Transaction-local vector-index mutation. Derived ANN state has no MVCC undo,
/// so both deletes and replacements are published only after commit. `undo_id`
/// ties the entry to the heap undo chain, allowing savepoint rollback to prune
/// precisely the mutations whose row change was undone.
#[derive(Debug, Clone)]
pub(crate) struct PendingVectorMutation {
    pub(crate) undo_id: u64,
    pub(crate) table_id: u64,
    pub(crate) storage_id: u64,
    pub(crate) row_id: RowId,
    /// `None` is DELETE; `Some(row)` is INSERT/UPDATE-style idempotent replace.
    pub(crate) new_values: Option<Vec<Value>>,
    /// Present for UPDATE so unchanged vector columns avoid needless churn;
    /// absent for INSERT and DELETE.
    pub(crate) old_values: Option<Vec<Value>>,
}

/// One commit's superseded graph versions for one index, retired once no
/// snapshot that could still see them remains.
type VectorPurgeIdentity = (u64, u64, u64, u64, u64, u64);

#[derive(Debug)]
pub(crate) struct VectorPurge {
    index_id: u64,
    lineage: u64,
    key: (u64, u64, u64, String),
    rows: Vec<(RowId, smallvec::SmallVec<[u64; 1]>)>,
    commit_scn: u64,
}

/// Row operations committed while a lazy vector graph build is in flight.
/// `ops` stays append-only for the registration's lifetime, so concurrent
/// builders of the same key each replay the complete sequence.
type VectorPurgeIndexIdentity = (u64, u64, u64, u64, u64);

#[derive(Debug, Default)]
struct VectorPurgeQueue {
    by_time: BTreeMap<VectorPurgeIdentity, VectorPurge>,
    by_index: BTreeMap<VectorPurgeIndexIdentity, BTreeSet<u64>>,
    latest_retired: BTreeMap<(VectorPurgeIndexIdentity, RowId), u64>,
}

impl VectorPurgeQueue {
    fn new() -> Self {
        Self::default()
    }
    fn len(&self) -> usize {
        self.by_time.len()
    }
    fn is_empty(&self) -> bool {
        self.by_time.is_empty()
    }
    fn values(&self) -> impl Iterator<Item = &VectorPurge> {
        self.by_time.values()
    }
    fn contains_key(&self, key: &VectorPurgeIdentity) -> bool {
        self.by_time.contains_key(key)
    }
    fn get_mut(&mut self, key: &VectorPurgeIdentity) -> Option<&mut VectorPurge> {
        self.by_time.get_mut(key)
    }
    fn index_key(key: &VectorPurgeIdentity) -> VectorPurgeIndexIdentity {
        (key.1, key.2, key.3, key.4, key.5)
    }
    fn insert(&mut self, purge: VectorPurge) -> Option<VectorPurge> {
        let key = purge.identity();
        for (row, versions) in &purge.rows {
            if let Some(latest) = versions.iter().max() {
                self.latest_retired
                    .entry((Self::index_key(&key), *row))
                    .and_modify(|seen| *seen = (*seen).max(*latest))
                    .or_insert(*latest);
            }
        }
        self.by_index
            .entry(Self::index_key(&key))
            .or_default()
            .insert(key.0);
        self.by_time.insert(key, purge)
    }
    fn insert_new_retirements(&mut self, mut purge: VectorPurge) {
        let index = Self::index_key(&purge.identity());
        for (row, versions) in &mut purge.rows {
            if let Some(seen) = self.latest_retired.get(&(index, *row)) {
                versions.retain(|version| *version > *seen);
            }
        }
        purge.rows.retain(|(_, versions)| !versions.is_empty());
        if !purge.rows.is_empty() {
            self.insert_if_absent(purge);
        }
    }
    fn remove_completed(
        &mut self,
        key: &VectorPurgeIdentity,
        rows: &[(RowId, smallvec::SmallVec<[u64; 1]>)],
    ) {
        let index = Self::index_key(key);
        for (row, versions) in rows {
            if let Some(latest) = versions.iter().max() {
                if self
                    .latest_retired
                    .get(&(index, *row))
                    .is_some_and(|seen| seen <= latest)
                {
                    self.latest_retired.remove(&(index, *row));
                }
            }
        }
        self.remove(key);
        if self.is_empty() {
            self.latest_retired.clear();
        }
    }
    fn insert_if_absent(&mut self, purge: VectorPurge) -> bool {
        if self.contains_key(&purge.identity()) {
            return false;
        }
        self.insert(purge);
        true
    }
    fn remove(&mut self, key: &VectorPurgeIdentity) -> Option<VectorPurge> {
        let index = Self::index_key(key);
        if let Some(times) = self.by_index.get_mut(&index) {
            times.remove(&key.0);
            if times.is_empty() {
                self.by_index.remove(&index);
            }
        }
        self.by_time.remove(key)
    }
    fn ready_keys(&self, horizon: u64) -> Vec<VectorPurgeIdentity> {
        self.by_time
            .range(..=(horizon, u64::MAX, u64::MAX, u64::MAX, u64::MAX, u64::MAX))
            .map(|(key, _)| *key)
            .collect()
    }
    fn for_index(
        &self,
        index: VectorPurgeIndexIdentity,
    ) -> impl Iterator<Item = &VectorPurge> + '_ {
        self.by_index
            .get(&index)
            .into_iter()
            .flat_map(move |times| {
                times.iter().filter_map(move |time| {
                    self.by_time
                        .get(&(*time, index.0, index.1, index.2, index.3, index.4))
                })
            })
    }
}

impl VectorPurge {
    fn identity(&self) -> VectorPurgeIdentity {
        (
            self.commit_scn,
            self.key.0,
            self.key.1,
            self.key.2,
            self.index_id,
            self.lineage,
        )
    }
}

#[derive(Debug, Default)]
pub(crate) struct VectorBuildDelta {
    builders: usize,
    /// `Some` is an idempotent upsert of the committed vector, `None` a delete.
    ops: HashMap<RowId, (u64, Option<crate::vector::VectorValue>)>,
    next_seq: u64,
    failed: bool,
    charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

#[derive(Debug, Clone)]
struct HeapWrite {
    row_id: RowId,
    header: RowHeader,
    values: Vec<Value>,
    /// Per-column external mask for off-page BLOB/TEXT columns. Empty
    /// for rows with no externalized values (the common case).
    external: Vec<Option<LobRef>>,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexBatchEntry {
    kind: WalIndexOpKind,
    key: IndexKey,
    row_id: RowId,
}

#[derive(Debug, Clone)]
struct MvccStoredRow {
    row_id: RowId,
    header: RowHeader,
    values: Vec<Value>,
}

pub(crate) struct ProjectedMvccStoredRow<'a> {
    row_id: RowId,
    header: RowHeader,
    values: &'a [Value],
    /// Legacy projected-read adjustment hint. v5 layout-aware decoding
    /// maps stored columns back into the current schema and pre-fills
    /// missing columns with catalog defaults, so it returns
    /// `usize::MAX`; older self-describing paths still return a physical
    /// stored count.
    stored_count: usize,
}

/// Raw-payload row produced by
/// [`HeapFile::scan_mvcc_payloads_range_until`]. Carries everything the
/// vectorized scan bridge needs to drive
/// [`crate::storage::core::decode_row_into_sink_fast`] without
/// going through a `Vec<Value>` intermediate.
pub(crate) struct PayloadMvccStoredRow<'a> {
    pub row_id: RowId,
    pub header: RowHeader,
    /// Full slot bytes (header + body). The body offset starts at
    /// `body_offset` so the layout-id varint can be skipped by the
    /// scan loop before handing the slice to the sink decoder.
    pub payload: &'a [u8],
    /// Offset within `payload` where the row body's value section
    /// starts (i.e., past the MVCC header AND the layout-id varint).
    pub body_offset: usize,
    pub layout_id: u32,
    pub has_external: bool,
    pub plan: &'a crate::storage::core::RowDecodePlan,
    pub codec: &'a crate::storage::core::RowCodec,
}

/// One hit from [`Database::scan_rows_payloads_streaming_range`].
/// Carries either a raw payload (the hot in-place page path) or a
/// `Vec<Value>` (the deferred / partitioned / forwarded paths that
/// already materialised the row).
pub(crate) enum PayloadStreamHit<'a> {
    Payload(PayloadHitPayload<'a>),
    Values(&'a [Value]),
}

/// Inline payload context for [`PayloadStreamHit::Payload`].
pub(crate) struct PayloadHitPayload<'a> {
    pub payload: &'a [u8],
    pub codec: &'a crate::storage::core::RowCodec,
    pub plan: &'a crate::storage::core::RowDecodePlan,
    pub layout_id: u32,
    pub has_external: bool,
}

enum CachedMvccRead {
    Hit(MvccStoredRow),
    MissingPage,
    NotFound,
}

/// Cheaper sibling of [`CachedMvccRead`] that carries only the row's
/// MVCC header. Returned by the buffered fast path that index-only
/// scans use to test visibility without paying for value decoding.
pub(crate) enum CachedMvccHeaderRead {
    Hit(RowHeader),
    /// Page not in the buffer pool, or the row needs forwarding/piece
    /// reconstruction — caller must fall back to the full reader.
    MissingPage,
    /// Slot vacant or out of bounds — row is gone, treat as invisible.
    NotFound,
}

enum CachedProjectedRead {
    /// Successfully decoded projected row from the buffer-pool fast
    /// path. The trailing `usize` is the legacy default-backfill hint;
    /// v5 layout-aware rows use `usize::MAX` because the codec already
    /// applied missing-column defaults.
    Hit(RowHeader, Vec<Value>, usize),
    MissingPage,
    NotFound,
}

enum HeapPayload {
    Row(RowHeader, Vec<Value>),
    /// Projected row payload plus the legacy default-backfill hint.
    /// v5 layout-aware rows map by `layout_id` and return `usize::MAX`
    /// after filling missing columns from the catalog defaults.
    RowColumns(RowHeader, Vec<Value>, usize),
    RowPiece(RowHeader, Vec<Value>),
    /// Projected row-piece payload — same `stored_count` semantics as
    /// `RowColumns`.
    RowPieceColumns(RowHeader, Vec<Value>, usize),
    Forward(RowId),
}

#[derive(Debug, Clone, Copy)]
enum CachedTxnVisibility {
    Committed(u64),
    RolledBack,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RowHeader {
    pub(crate) flags: u8,
    pub(crate) last_txn_id: u64,
    pub(crate) undo_ptr: Option<u64>,
}

impl RowHeader {
    /// Header for a freshly-INSERTed visible row. Reserves the on-disk
    /// `undo_ptr` slot as 8 zero bytes so the first UPDATE can patch the
    /// row in place via [`try_in_place_update_delta`] instead of falling
    /// back to a full-row re-encode. The reserved slot is invisible to
    /// the visibility walker because [`read_mvcc_header`] decodes a raw
    /// zero `undo_ptr` back to `Option::None` (undo ids start at 1 and
    /// the allocator keeps them ≥ 1, so 0 is a permanent sentinel).
    /// Storage cost: +8 B per row, recouped immediately by the
    /// HeapUpdate→HeapUpdateLogical WAL shrink on the very first UPDATE.
    fn visible(txn_id: u64) -> Self {
        Self {
            flags: 0,
            last_txn_id: txn_id,
            undo_ptr: Some(0),
        }
    }

    fn is_deleted(self) -> bool {
        self.flags & MVCC_FLAG_DELETED != 0
    }
}

/// File/snapshot caches keyed by small fixed-size keys
/// (`u64` table id or `(u64, String)` index handle). ahash is 2.4-3×
/// cheaper than std SipHash-1-3 on these key shapes and every SELECT
/// touches `heap_cache` + (optionally) `index_cache` once per
/// referenced table. Hash work used to be measurable in flame
/// graphs on read-only sysbench precisely because these maps were the
/// only per-statement hashing left after `vec/hash_*` was migrated.
// File-level handle aliases. parking_lot::RwLock here serves the same
// metastable-convoy purpose as on `txns` / `undo`: the hot OLTP path
// takes one shared lock on the table's heap + index on every SELECT and
// upgrades to exclusive on every INSERT/UPDATE/DELETE. std::sync::RwLock's
// writer-priority kernel queue collapses readers into pure serialization
// the moment a single writer arrives — sysbench oltp_read_write (70% R /
// 30% W) measured ~600+ sample-cycles in `RwLock::lock_contended` on
// `lookup_index` / `range_index_bounds` alone. parking_lot's queueing
// and adaptive spinning keep readers parallel under bursty writes.
pub(crate) type HeapHandle = Arc<parking_lot::RwLock<HeapFile>>;
pub(crate) type IndexHandle = Arc<parking_lot::RwLock<IndexFile>>;

#[cfg(test)]
thread_local! {
    static INDEX_FLUSH_TEST_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
}
pub(crate) type OverflowHandle = Arc<parking_lot::RwLock<OverflowFile>>;

#[derive(Debug, Clone)]
struct ReorgStorageLocation {
    database: String,
    table_name: String,
    generation: u64,
    // Private heaps have no public catalog row. Retain their decoder across
    // idle-handle eviction until publication or rollback removes this entry.
    heap_codec: Option<Arc<RowCodec>>,
    index_ids: Option<Arc<BTreeMap<String, u64>>>,
}

type HeapCacheMap = hashbrown::HashMap<u64, HeapHandle, ahash::RandomState>;
type IndexCacheMap = hashbrown::HashMap<(u64, String), IndexHandle, ahash::RandomState>;
type OverflowCacheMap = hashbrown::HashMap<u64, OverflowHandle, ahash::RandomState>;
type IndexGenerationMap = HashMap<(u64, String), u64>;
type SpatialRtreeCacheMap = HashMap<(u64, String), Arc<SpatialRtreeCache>>;
type BufferFrameMap = hashbrown::HashMap<PageKey, Arc<Frame>, ahash::RandomState>;
/// Values are shared atomic cells so the hot paths (per-miss load, per-flush
/// generation check) read them with one atomic load under the map's read
/// lock; only first-touch inserts take the write lock. Whole-file invalidation
/// removes the map entry and writes a globally unique generation into the old
/// cell, keeping every outstanding clone coherent without ABA on reopen.
type BufferFileGenerationMap =
    hashbrown::HashMap<BufferPageFileKey, Arc<AtomicU64>, ahash::RandomState>;
type BufferFileCacheMap =
    hashbrown::HashMap<BufferPageFileKey, Arc<CachedFile>, ahash::RandomState>;
/// Sharded snapshot table — each shard sees ~1/N of the statement
/// register / drop traffic, so the hash speedup compounds across shards.
type ActiveSnapshotMap = hashbrown::HashMap<u64, ActiveSnapshot, ahash::RandomState>;

#[derive(Debug)]
struct SpatialRtreeCache {
    generation: AtomicU64,
    tree: parking_lot::RwLock<rtree::RTree>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

/// Undo directory latch. Cold segment I/O is prepared under a read guard and
/// completed after releasing it, so this lock covers metadata only. Shared
/// guards let concurrent old-version readers and diagnostics proceed together;
/// append publication, purge removal and compaction swaps take the write side.
#[derive(Debug)]
struct UndoLatch<T>(parking_lot::RwLock<T>);

impl<T> UndoLatch<T> {
    fn new(value: T) -> Self {
        Self(parking_lot::RwLock::new(value))
    }

    fn read(&self) -> parking_lot::RwLockReadGuard<'_, T> {
        self.0.read()
    }

    fn write(&self) -> parking_lot::RwLockWriteGuard<'_, T> {
        self.0.write()
    }

    fn try_read(&self) -> Option<parking_lot::RwLockReadGuard<'_, T>> {
        self.0.try_read()
    }
}

#[derive(Debug)]
pub struct Database {
    _snapshot_runtime: Option<crate::storage::image_io::SnapshotRuntime>,
    _image_repository_lease: Option<std::fs::File>,
    open_mode: DatabaseOpenMode,
    pub(crate) capture: super::capture::CaptureState,
    pub(crate) services: super::capture::DatabaseServices,
    recovery_context: RecoveryContext,
    visibility_cache_id: u64,
    next_snapshot_id: AtomicU64,
    /// Fail-closed latch for states that cannot be repaired safely online
    /// (partial rollback or a published commit whose durability barrier
    /// failed). The first error wins and every subsequent SQL statement is
    /// rejected until restart recovery reconstructs a coherent state.
    mvcc_quarantined: AtomicBool,
    mvcc_quarantine_reason: RwLock<Option<String>>,
    /// HLC wall-clock component and durable txn/SCN/time correctness source.
    commit_hlc_unix_us: Arc<AtomicU64>,
    temporal_history: TemporalHistory,
    temporal_hlc_rollback_total: AtomicU64,
    data_dir: PathBuf,
    /// Exclusive lock on `data_dir`, held for this handle's lifetime so no
    /// second `mydb` process can open the same directory concurrently (see
    /// [`crate::storage::dir_lock`]). Released when the `Database` drops.
    _dir_lock: crate::storage::dir_lock::DataDirLock,
    catalog: CatalogManager,
    /// Lock-free immutable authorization snapshot. Catalog and snapshot share
    /// the same Arc; DCL publishes a replacement only after DD/WAL commit.
    auth_snapshot: arc_swap::ArcSwap<crate::engine::auth::AuthCatalog>,
    temp_tables: RwLock<BTreeMap<String, Arc<TableSchema>>>,
    // parking_lot::RwLock: the transaction manager guards every row-lock
    // acquire/release, unique-key check, MVCC visibility lookup and commit.
    // Under high-concurrency write workloads this lock is taken millions of
    // times per second; std::sync::RwLock's contended path (park/unpark +
    // syscall) can collapse into a metastable lock convoy. parking_lot's
    // adaptive spinning keeps these short critical sections off the kernel.
    txns: parking_lot::RwLock<TransactionManager>,
    // TPC-C lock de-contention: the row/unique/table lock subsystem split out
    // of `TransactionManager` onto its own `Mutex` so row-lock acquire/release
    // no longer serialises on the single `txns` write lock with begin/commit/
    // visibility. Lock order is `txns` → `lock_manager`; see `LockManager`.
    // RwLock (not Mutex): mutating lock ops (acquire/release) take the write
    // guard, but the per-statement read-only probes — `statement_savepoint`'s
    // `lock_log_len`, the `data_locks` views, and the SHOW ENGINE stat getters —
    // take the read guard so they don't serialise against each other. Stripe
    // later only if the write side becomes the hot spot.
    lock_manager: parking_lot::RwLock<TxnLockManager>,
    mdl: MdlManager,
    // parking_lot for the same metastable-convoy reason as `txns`: every
    // row-level DML appends an undo record under this lock.
    undo: UndoLatch<UndoTablespace>,
    /// Immutable Arc-owned shard routing table shared with `UndoManager`.
    /// Keeping it outside the manager lock prevents every row write from
    /// acquiring a global read guard merely to clone one shard handle.
    undo_append_shards: Arc<[UndoWriteShardHandle]>,
    undo_resident_admission: crate::storage::undo::UndoResidentAdmission,
    undo_append_next_shard: AtomicU64,
    /// Mirrors the two runtime gates that require append admission before IO.
    /// Disabling is published before changing either gate; enabling is
    /// published after, so runtime changes cannot admit an unsafe fast append.
    undo_concurrent_append_eligible: Arc<AtomicBool>,
    // Sharded by snapshot_id to spread per-statement snapshot register/drop
    // contention across many mutexes. The hot SELECT path inserts on
    // `register_statement_snapshot` and removes on `SnapshotGuard::Drop` —
    // a single Mutex<HashMap> serialized 4-8 concurrent readers and was a
    // visible tail-latency contributor.
    active_snapshots: Box<[Mutex<ActiveSnapshotMap>]>,
    auto_increment: AutoIncrementManager,
    /// Briefly excludes AUTO_INCREMENT redo publication while checkpoint
    /// installs its coverage fence and rotates the dirty epoch.
    auto_increment_checkpoint_fence: parking_lot::RwLock<()>,
    auto_increment_dirty: Mutex<AutoIncrementDirtyQueue>,
    sequence_wal_batch: i64,
    sequence_covered_lsn: AtomicU64,
    sequence_metrics: SequenceMetrics,
    sequence_checkpoint_fence: parking_lot::RwLock<()>,
    sequence_dirty: parking_lot::Mutex<SequenceDirtyQueue>,
    sequence_dirty_generations: AtomicUsize,
    sequence_sync: parking_lot::RwLock<crate::storage::sequence::SequenceSyncConfig>,
    /// Sequence runtimes; durable after-states are checkpointed in the DD.
    sequences: crate::storage::sequence::SequenceManager,
    lock_wait_mutex: Mutex<()>,
    lock_wait_signal: Condvar,
    /// Async-runtime mirror of row-lock release progress. Inline prepared
    /// point commands subscribe only after an immediate lock conflict; a
    /// monotonically changing watch value closes the release-before-await
    /// race without parking an OS blocking-pool worker. The synchronous
    /// condvar/parker path remains the correctness oracle and fallback.
    row_lock_async_signals: Box<[tokio::sync::watch::Sender<u64>]>,
    row_lock_async_waiters: Arc<AtomicU64>,
    /// Number of threads currently parked on `lock_wait_signal` (MDL /
    /// unique-key / table-lock waiters). `notify_lock_waiters` reads this and
    /// skips the broadcast when it is 0: the overwhelmingly common
    /// contention-free case. Park-scoped (incremented only while a thread is
    /// actually blocked), so uncontended acquires never set it. A racing skip
    /// is self-healing: every waiter re-checks the real lock state on a
    /// bounded (<=20 ms) timeout, so it costs at most one slice of latency.
    lock_wait_waiters: AtomicU64,
    config: StorageConfig,
    runtime_config: RwLock<RuntimeStorageConfig>,
    buffer_pool: Arc<BufferPool>,
    /// Shared capacity floor for overlapping offline index builds.  Each
    /// builder owns one entry until its scratch reservation is released; the
    /// pool returns to its configured capacity only after the final entry
    /// leaves, so out-of-order CREATE INDEX completion cannot restore pages
    /// underneath a still-running peer.
    offline_index_buffer_leases: Mutex<OfflineIndexBuildBufferLeaseState>,
    /// Foreground bulk DDL owns large, governor-accounted scratch and must
    /// not race optional derived-data scans that refill the buffer pool.
    /// Zone-map maintenance observes this counter and cancels promptly.
    foreground_bulk_ddl_active: AtomicU64,
    /// Active statistics scans have priority over optional zone-map
    /// maintenance. This covers explicit and automatic ANALYZE so a delayed
    /// maintenance pass cannot duplicate their heap IO.
    analyze_scan_active: AtomicU64,
    // Read-mostly: written only on first table/index open or on DDL drop.
    // Hot SELECT path acquires the RwLock in shared mode for an O(1) HashMap
    // lookup; the previous `Mutex<HashMap>` serialized 4 concurrent readers
    // through the same critical section. parking_lot::RwLock here is the
    // same metastable-convoy fix as on the file-level handle locks:
    // the cache is read on every `with_*_read`/`with_*_mut` call (~100k
    // ops/sec on a 20-thread OLTP workload), so the std::sync::RwLock's
    // park/unpark cost was visible in the profile even though writes are
    // rare.
    heap_cache: parking_lot::RwLock<HeapCacheMap>,
    /// Weak per-storage fences cover only concurrent cold opens. Cache hits
    /// need no epoch lookup, and unrelated tables cannot invalidate an open.
    storage_handle_open_epochs: Mutex<HashMap<u64, std::sync::Weak<AtomicU64>>>,
    index_cache: parking_lot::RwLock<IndexCacheMap>,
    /// Mutation generations registered before the first spatial projection
    /// scan. Ordinary B-trees need no entry. Spatial R-tree cache entries are
    /// usable only when their captured generation matches.
    index_mutation_generations: parking_lot::RwLock<IndexGenerationMap>,
    /// Optional, governor-accounted R-tree projections of durable spatial
    /// B-trees. The B-tree remains the only WAL/recovery source of truth.
    spatial_rtree_cache: parking_lot::RwLock<SpatialRtreeCacheMap>,
    /// Per-table overflow (LOB) file handles, opened lazily on first
    /// externalization. Mirrors `heap_cache` / `index_cache`.
    overflow_cache: parking_lot::RwLock<OverflowCacheMap>,
    /// Conservative per-zone min/max/null summaries — derived data:
    /// absence or corruption only disables pruning.
    // Readers cover summary work; the writer durably revokes all old trust
    // before allowing writes that omit summaries. Recursive reads permit
    // maintenance/ANALYZE to reuse the normal transaction helpers.
    zone_map_gate: parking_lot::RwLock<()>,
    /// Serialize physical backup capture with background heap/LOB/undo maintenance.
    backup_maintenance_gate: parking_lot::RwLock<()>,
    zone_map_enabled: AtomicBool,
    // Also serializes WAL trust revocation with checkpoint publication.
    zone_maps: RwLock<ZoneMapsMap>,
    // Each loader shard remembers one successful OFF-mode revocation as
    // (storage id, retirement epoch). Collisions only repeat optional work.
    zone_map_load_locks: [Mutex<Option<(u64, u64)>>; 64],
    zone_map_retirement_epoch: AtomicU64,
    /// DELETE-side HNSW/IVF maintenance is commit-deferred. Applying a graph
    /// tombstone before MVCC commit made ROLLBACK permanently hide a live row
    /// from the candidate generator. Entries are keyed by transaction and
    /// pruned against the undo chain on savepoint rollback.
    pending_vector_deletes: Mutex<HashMap<u64, Vec<PendingVectorMutation>>>,
    /// Transactions between `prepare_pending_vector_commit` and the end of
    /// `apply_pending_vector_deletes`: their rows may already be visible
    /// while the shared graph lacks them, so KNN merges exactly these (plus
    /// the reader's own) pending upserts. Never held with the map above.
    vector_commits_publishing: parking_lot::Mutex<HashSet<u64>>,
    vector_recovery_active: AtomicBool,
    #[cfg(test)]
    vector_persist_failure: AtomicU64,
    vector_pending_wal: parking_lot::Mutex<BTreeMap<u64, u64>>,
    vector_copy_publications: parking_lot::Mutex<HashSet<u64>>,
    replica_vector_state: parking_lot::Mutex<ReplicaVectorState>,
    replica_vector_signal: parking_lot::Condvar,
    vector_retired_graphs: parking_lot::Mutex<HashMap<PathBuf, std::sync::Weak<VectorGraph>>>,
    vector_snapshot_dirty: parking_lot::Mutex<HashSet<u64>>,
    recovered_vector_completion: parking_lot::Mutex<()>,
    /// Number of deferred tombstones discarded by rollback/savepoint rollback.
    /// Surfaced in SHOW ENGINE MYDB STATUS.
    hnsw_tombstones_rolled_back: AtomicU64,
    /// In-memory HNSW graphs for vector (HNSW) indexes, keyed by
    /// `(table_id, partition_storage_id, normalized_index_name)`. For an
    /// unpartitioned table `partition_storage_id == table_id`, so the
    /// single-graph behaviour is preserved. For a partitioned table each
    /// partition has its own graph because `RowId`s are only unique within
    /// one partition's heap — a single mixed graph would alias rows across
    /// partitions (M5.5 design, 2026-06).
    ///
    /// Derived data, like the spatial R-tree: built lazily from a heap
    /// scan, grown incrementally on insert, and never the source of truth
    /// — the executor re-ranks every candidate by the live heap vector
    /// under MVCC. Persisted opportunistically at checkpoint (M1, 2026-06);
    /// rebuilt from heap on a missing/corrupt snapshot. Dropped on
    /// DROP INDEX / DROP TABLE / TRUNCATE.
    ///
    /// The inner `Arc<VectorGraph>` is the per-graph handle, abstracting the
    /// in-memory slab backend ([`crate::engine::vector::HnswIndex`]) and the
    /// buffer-pool paged backend (`PagedHnsw`, `.hvec`) behind one enum
    /// (selected per-process by `MYDB_VECTOR_STORAGE`, default `memory`).
    /// Either backend is internally concurrent (per-node latches / per-page
    /// latches, atomic counters) — so multiple INSERT/SEARCH threads can drive
    /// the same index without any outer RwLock. The map's own `RwLock` only
    /// protects insertions/removals of graph entries.
    vector_indexes: parking_lot::RwLock<
        HashMap<(u64, u64, u64, String), Arc<crate::engine::vector::VectorGraph>>,
    >,
    /// Committed graph maintenance for HNSW keys whose lazy build is still in
    /// flight. A build registers before its heap scan and replays these row
    /// operations before publication, so commits landing between the scan
    /// and publication are not lost. Lock order: `vector_indexes` first.
    vector_build_deltas: parking_lot::Mutex<HashMap<(u64, u64, u64, String), VectorBuildDelta>>,
    /// Registered builds; lets DML skip the registry lock when none run.
    vector_builds_in_flight: AtomicUsize,
    /// Highest SCN whose graph position or cold image may supersede older views.
    vector_snapshot_floor: AtomicU64,
    vector_build_epoch: AtomicU64,
    replicated_hnsw_table_cache: parking_lot::RwLock<HashMap<u64, Option<u64>>>,
    replicated_hnsw_dirty: parking_lot::Mutex<HashMap<u64, std::collections::HashSet<u64>>>,
    /// One owner per cold index; waiting readers share its published graph.
    vector_build_locks: parking_lot::Mutex<
        HashMap<(u64, u64, u64, String), std::sync::Weak<parking_lot::Mutex<()>>>,
    >,
    /// Graph keys whose last build failed memory admission, with the time.
    vector_build_backoff: parking_lot::Mutex<HashMap<(u64, u64, u64, String), std::time::Instant>>,
    /// Superseded HNSW node versions (DELETE / re-linked UPDATE) that some
    /// active snapshot may still need; retired once the visibility horizon
    /// passes their commit. `vector_purges_pending` mirrors the length.
    vector_purge_queue: parking_lot::Mutex<VectorPurgeQueue>,
    vector_purge_snapshot_gate: parking_lot::RwLock<()>,
    vector_purge_drain_gate: parking_lot::Mutex<()>,
    vector_purge_recovery_charge:
        parking_lot::Mutex<Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>>,
    vector_purges_pending: AtomicUsize,
    /// Lazy backing store for `SET PERSIST` / `SET PERSIST_ONLY`. Opened on
    /// first read or write, materialised against `<data_dir>/system/mysqld-auto.cnf`.
    /// A corrupt file degrades to an empty in-memory store with a warning
    /// (via `open_or_default`) so the server still boots — operator can
    /// inspect the bad file out-of-band.
    persisted_variables: OnceLock<Arc<crate::engine::sysvar::PersistedVariables>>,
    log: Arc<crate::wal::LogMutex>,
    /// Lock-free mirror of `LogManager::{next_lsn, wal_bytes}`. Lets the
    /// per-statement `Database::should_checkpoint` fast path probe WAL
    /// growth without locking `log`. Updated under the WAL mutex by
    /// `LogManager` itself; we just hold an Arc clone. See
    /// `wal::LogStateAtomics` doc for the rationale.
    log_state_atomics: Arc<crate::wal::LogStateAtomics>,
    group_commit: GroupCommitCoordinator,
    durability_worker: DurabilityWorker,
    plan_cache_hits: AtomicU64,
    plan_cache_misses: AtomicU64,
    select_parse_cache_hits: AtomicU64,
    select_compiled_plan_hits: AtomicU64,
    select_operator_compiles: AtomicU64,
    /// Prepared-handle compiled SELECT: `reuses` counts L0 memo hits;
    /// `compiles` counts shared compiled-plan construction triggered by a
    /// prepared handle.
    prepared_plan_reuses: AtomicU64,
    prepared_plan_compiles: AtomicU64,
    /// Compile-once prepared-DML fast path (INSERT/UPDATE/DELETE): `reuses`
    /// counts EXECUTEs that ran from a memoized `PreparedDmlPlan`,
    /// `compiles` counts the first-execute compilations. Mirrors
    /// `prepared_plan_*` for the write side.
    prepared_dml_reuses: AtomicU64,
    prepared_dml_compiles: AtomicU64,
    checkpoint: Mutex<CheckpointState>,
    /// Serializes whole checkpoints. Every entry point — the background
    /// checkpointer, `ALTER SYSTEM CHECKPOINT`, shutdown — runs the
    /// entire begin→flush→compact sequence under this mutex so two
    /// checkpoints can never interleave their WAL compactions.
    checkpoint_running: Mutex<()>,
    /// Physical-replica apply holds the read side for one complete WAL frame;
    /// restartpoint holds the write side while syncing files and pruning WAL.
    replica_apply_checkpoint: RwLock<()>,
    replica_applied_lsn_atomic: AtomicU64,
    replica_restartpoint_tracker: Mutex<ReplicaRestartpointTracker>,
    /// Lock-free mirror of `CheckpointState::last_checkpoint_lsn`.
    /// Updated under the `checkpoint` mutex whenever a real checkpoint
    /// completes. Probed by `should_checkpoint` to decide if WAL has
    /// advanced since the last checkpoint, without locking.
    last_checkpoint_lsn_atomic: AtomicU64,
    /// Lock-free mirror of `CheckpointState::last_checkpoint_unix_ms`.
    /// `0` means "never checkpointed yet". Used by `should_checkpoint`
    /// to decide if the time-based interval has elapsed. Armed to the
    /// startup time when the Database opens so the interval trigger
    /// works from the first process minute onward.
    last_checkpoint_unix_ms_atomic: AtomicU64,
    /// Total WAL bytes on disk right after the last checkpoint's
    /// compaction. The size trigger fires on the DELTA since then —
    /// retained history (in-flight txn undo chains, replication slots)
    /// must not re-trigger a checkpoint that cannot shrink it anyway.
    wal_bytes_after_last_checkpoint_atomic: AtomicU64,
    /// Backpressure for automatic size-triggered checkpoints. A workload
    /// that emits many first-update-after-checkpoint page images can cross
    /// `checkpoint_wal_bytes` repeatedly within a few seconds; this mirror
    /// records the last automatic checkpoint attempt (successful or failed)
    /// so the checkpointer coalesces that write burst instead of creating a
    /// checkpoint/FPI/checkpoint feedback loop. Manual checkpoints bypass it.
    last_auto_checkpoint_unix_ms_atomic: AtomicU64,
    /// Observability: completed checkpoints and the last one's duration.
    checkpoints_total_atomic: AtomicU64,
    checkpoint_failures_total_atomic: AtomicU64,
    checkpoint_preflush_failures_total_atomic: AtomicU64,
    last_checkpoint_redo_lsn_atomic: AtomicU64,
    last_checkpoint_dirty_pages_atomic: AtomicU64,
    last_checkpoint_wal_reclaimed_bytes_atomic: AtomicU64,
    checkpoint_wal_reclaimed_bytes_total_atomic: AtomicU64,
    restartpoints_total_atomic: AtomicU64,
    last_checkpoint_trigger_atomic: AtomicU8,
    last_checkpoint_kind_atomic: AtomicU8,
    last_checkpoint_snapshot_us_atomic: AtomicU64,
    last_checkpoint_flush_us_atomic: AtomicU64,
    last_checkpoint_sync_us_atomic: AtomicU64,
    last_checkpoint_compact_us_atomic: AtomicU64,
    last_checkpoint_duration_us_atomic: AtomicU64,
    checkpoint_duration_total_us_atomic: AtomicU64,
    checkpoint_duration_max_us_atomic: AtomicU64,
    last_recovery: RecoverySummary,
    // M1.4 - Lock-free txn ID/SCN allocation (Oracle-style atomic SCN counter).
    next_txn_id_atomic: AtomicU64,
    /// Read-only transactions on a physical replica must never collide with
    /// source txn ids arriving later. The high half of u64 is reserved for
    /// node-local, non-WAL replica snapshots.
    next_replica_local_txn_id: AtomicU64,
    physical_replica_mode: AtomicBool,
    next_temp_table_id: AtomicU64,
    next_scn_atomic: AtomicU64,
    // P0-1 (commit-visibility frontier): lock-free mirror of
    // `TransactionManager::commit_frontier` — the largest SCN such that every
    // commit ≤ it has finished publishing. ALL snapshots take their
    // `snapshot_scn` from this (via `visible_scn()`), never from
    // `next_scn_atomic` (the allocation counter, which runs ahead of published
    // commits). Advanced with `fetch_max` after `publish_commit` /
    // `mark_frontier_done`, so it is monotonic regardless of store interleaving.
    visible_frontier: AtomicU64,
    primary_definition_highwater: AtomicU64,
    read_definition_highwater: AtomicU64,
    // Commit acknowledgements above a publication gap sleep here. Ordinary
    // in-order publication needs no mutex acquisition.
    commit_frontier_waiters: AtomicUsize,
    commit_frontier_wait_lock: Mutex<()>,
    commit_frontier_signal: Condvar,
    // Mirrored from `TransactionManager::active_write_count`. Updated under the
    // `txns` write lock so writers serialize their mutations and the atomic is
    // always consistent with manager state. Lets the SELECT hot path probe
    // "any writer active?" without taking the txn lock.
    active_write_txns_atomic: AtomicUsize,
    // Shares the independently latched old-key directory with the transaction
    // manager. Its version's low bit is the authoritative nonempty flag.
    index_repair_state: Arc<crate::storage::txn::IndexRepairState>,
    // Shared with TransactionManager, published only on repair mutations.
    index_repair_version: Arc<AtomicU64>,
    index_repair_epochs: Arc<IndexRepairEpochs>,
    // M3.4 - Largest contiguous transaction-id prefix whose slots have been
    // safely folded away. Every id <= this value is unconditionally visible
    // to current snapshots; sparse slot collection must not jump this prefix.
    min_retained_txn_id: AtomicU64,
    // PAGE_MAX_TRX_ID visibility shortcut counters (Phase 2). Each covering
    // scan chunk that resolves visibility via a leaf's `page_max_trx_id`
    // upper bound (InnoDB-style) bumps `hits`; every chunk that had to fall
    // back to the heap-header path bumps `misses`. Surfaced through
    // `SHOW ENGINE MYDB STATUS` so operators can validate whether a
    // deployment's covering-scan traffic is actually cashing in the
    // no-`ANALYZE`-required fast path.
    pub(crate) index_page_max_trx_shortcut_hits: AtomicU64,
    pub(crate) index_page_max_trx_shortcut_misses: AtomicU64,
    // Sharded lock-free mirror of `TransactionManager::slots` for the
    // visibility-check hot path. Lets every `row_version_visible` call
    // load the writer's terminal state with an `AtomicU64::load` after a
    // shard read-lock, instead of fighting the central `txns` RwLock with
    // every concurrent row-lock acquire/release. Sysbench oltp_read_write
    // sampling showed ~30% of SELECT scan CPU stuck in
    // `parking_lot::raw_rwlock::lock_shared_slow` on the old path; this
    // mirror routes the common case around the contention entirely.
    slot_status_cache: SlotStatusCache,
    // M3.3 - Counter for "snapshot too old" diagnostic.
    snapshot_too_old_total: AtomicU64,
    temporal_query_total: AtomicU64,
    temporal_heap_fallback_total: AtomicU64,
    temporal_history_index_hit_total: AtomicU64,
    // Phase-A probe: cumulative wall-ns spent acquiring + holding the undo
    // write lock for the per-record hot-path mirror publish, plus a sample
    // count. Attributes concurrent-write serialization to the undo lock vs
    // elsewhere (TM lock / heap latch / commit). Diagnostic-only; Relaxed.
    undo_publish_lock_ns: AtomicU64,
    undo_publish_lock_samples: AtomicU64,
    // M2.4 - Wait event history (Oracle ASH-style sampling buffer).
    wait_events: Mutex<WaitEventHistory>,
    // M1.1 - Counter for read-only transactions started.
    read_only_txn_count: AtomicU64,
    // Sharded condvars for fine-grained row-lock waiter wakeups.
    pub(crate) row_lock_signals: RowLockSignals,
    // P4 — directed front-waiter wakeup registry (txn_id -> parked thread).
    pub(crate) row_lock_parkers: RowLockParkers,
    // P2 — table-level (IS/IX/S/SIX/X) lock store, sharded by table_id, split
    // out of the global `lock_manager` so the per-DML IX hot path stays off the
    // global latch. Single source of truth for table locks.
    pub(crate) table_locks_sharded: ShardedTableLocks,
    // P3 — row-lock store, sharded by (table_id,row_id), split out of the
    // global `lock_manager`. Active only when `row_lock_sharding_enabled()`
    // (MYDB_ROW_LOCK_SHARDS); otherwise the legacy single-latch path is used.
    pub(crate) row_locks_sharded: ShardedRowLocks,
    /// Number of threads currently parked in `wait_for_specific_row` on a row
    /// shard condvar. Gates the 256-shard `row_lock_signals.notify_all`
    /// thundering herd that every statement's MDL release would otherwise fire
    /// (the dominant cost of the read-only point-lookup path). Park-scoped, so
    /// it stays 0 unless real row-lock contention is in flight.
    row_lock_waiters: AtomicU64,
    commit_gc_counter: AtomicU64,
    /// Enables internal online-DDL worker/catch-up paths. The SQL connection
    /// still waits for Public, failure or cancellation; this flag must never
    /// change statement completion semantics. Initial value comes from
    /// `MYDB_DDL_ASYNC=1`; tests and runtime `set_async_ddl` may toggle it on
    /// a per-engine basis without touching the env var.
    async_ddl: AtomicBool,
    /// Active ALTER TABLE COPY rewrites. Synchronous COPY registers
    /// here while it scans/converts/rebuilds the table; future async
    /// COPY uses the same tracker so `information_schema.mydb_ddl_jobs`
    /// has one table-copy progress surface.
    alter_copy_jobs: Mutex<HashMap<u64, Arc<AlterCopyJob>>>,
    /// Bounded terminal history so a fast failure/cancellation remains
    /// observable after the issuing statement receives its error.
    alter_copy_job_history: Mutex<VecDeque<AlterCopyJobHistoryEntry>>,
    /// Partition-reorg build-then-swap (§2.1): while a repartition streams
    /// rows into freshly-allocated *shadow* storage ids that are not yet in
    /// the catalog, `table_loc_fs` needs to resolve those ids to the owning
    /// table's (database, name) so the heap/index files land at the final
    /// path. Populated for the duration of one streaming reorg and cleared
    /// once the catalog swap has published the ids (or on rollback).
    reorg_storage_locs: Mutex<HashMap<u64, ReorgStorageLocation>>,
    partition_ddl_journal_lock: Mutex<()>,
    ddl_retirement_cleanup: parking_lot::Mutex<Option<(IndexKey, RowId)>>,
    partition_cleanup_background: AtomicBool,
    /// XID ownership spans ACTIVE, IDLE, PREPARED and completion I/O.
    xa_reservations: parking_lot::Mutex<HashMap<crate::xa::Xid, u64>>,
    /// XA: every successfully prepared branch lives here, keyed by the
    /// XID. Survives session disconnect — any session can pick a
    /// PREPARED branch up via `XA COMMIT` / `XA ROLLBACK`. Rebuilt at
    /// startup from the WAL `PrepareTxn` records.
    prepared_xa: parking_lot::RwLock<HashMap<crate::xa::Xid, crate::xa::PreparedXa>>,
    /// XA counters surfaced via `SHOW ENGINE MYDB STATUS`.
    xa_prepared_active: AtomicUsize,
    xa_commits_total: AtomicU64,
    xa_rollbacks_total: AtomicU64,
    /// rowid-6bytes M2 — slot-reclaim counters surfaced via
    /// `SHOW ENGINE MYDB STATUS`.
    ///
    /// `slot_reclaim_total_atomic`: cumulative slots flipped
    /// `ROW_FLAG_DELETED → ROW_FLAG_FREE` since startup.
    /// `slot_reclaim_runs_atomic`: number of purge runs that actually
    /// found tombstones to reclaim (the same purge runs that
    /// `purge_runs` counts; a run with no Delete records purged does
    /// not bump this).
    /// `slot_reclaim_errors_atomic`: per-slot reclaim failures (heap
    /// open failed, WAL append failed, etc.). The corresponding slot
    /// stays in DELETED state — the heap remains correct, only the
    /// reclaim metric reflects the stall.
    pub(crate) slot_reclaim_total_atomic: AtomicU64,
    pub(crate) slot_reclaim_runs_atomic: AtomicU64,
    pub(crate) slot_reclaim_errors_atomic: AtomicU64,
    // P0/4 — quarantine constants. `MAX_ATTEMPTS=8` keeps a transient
    // heap-lock contender quietly retrying for ~8 purge ticks; past
    // that the failure is treated as permanent. `QUARANTINE_CAP=4096`
    // bounds the map under a pathological "DROP TABLE while undo holds
    // tombstones" scenario; `DRAIN_PER_RUN=256` keeps each reclaim
    // call's wall time roughly constant regardless of backlog.
    // Note: these live in the impl module so reclaim_tombstoned_slots
    // can name them; they are intentionally not pub.
    /// P0/4 — Slot-reclaim retry quarantine. Reclaim failures used to
    /// vanish into `slot_reclaim_errors_atomic` with no retry path; the
    /// next purge run would pick a different victim cohort and the
    /// failed (table_id, row_id) pair would never get another shot.
    /// This map holds those pairs with an attempt counter; every
    /// `reclaim_tombstoned_slots` run drains a bounded slice, retries
    /// each, removes on success or when the counter crosses
    /// `SLOT_RECLAIM_MAX_ATTEMPTS`. Bounded by
    /// `SLOT_RECLAIM_QUARANTINE_CAP` so a runaway producer can't grow
    /// it without bound.
    pub(crate) slot_reclaim_quarantine:
        parking_lot::Mutex<std::collections::HashMap<(u64, RowId), u8>>,
    /// Cumulative slots given up on after `SLOT_RECLAIM_MAX_ATTEMPTS`.
    /// Surfaced via `SHOW ENGINE MYDB STATUS` as
    /// `slot_reclaim_permanent_failures`.
    pub(crate) slot_reclaim_permanent_failures_atomic: AtomicU64,
    /// Current quarantine size, surfaced via `SHOW ENGINE MYDB STATUS`
    /// as `slot_reclaim_quarantine_size`. Lock-free read; the writer
    /// path updates it under the quarantine mutex.
    pub(crate) slot_reclaim_quarantine_size_atomic: AtomicU64,
    /// Bounded, governor-accounted retry queue for overflow chains whose
    /// purge/rollback free hit a transient I/O or latch error.
    lob_reclaim_queue: parking_lot::Mutex<Option<LobReclaimQueue>>,
    /// DML row counters + atomicity / IGNORE / LOB lifecycle metrics
    /// (M1.A2 / A3 / A4 / D1). Cheap relaxed atomics; surfaced via
    /// `SHOW ENGINE MYDB STATUS`.
    dml_stats: DmlStats,
    /// Lock-free per-table "modified rows since last ANALYZE" delta.
    /// Updated by `record_table_modifications` on the DML hot path
    /// without cloning `TableSchema`; read by `should_refresh_stats`
    /// and the planner cost estimator, which both fold the delta into
    /// the persisted `TableSchema.stats.modified_rows_since_analyze`.
    /// Cleared on `ANALYZE TABLE` / DROP TABLE / TRUNCATE.
    table_modification_deltas: parking_lot::RwLock<HashMap<u64, Arc<AtomicU64>>>,
    /// Committed signed row-count changes since the last ANALYZE. Unlike
    /// `table_modification_deltas`, UPDATE contributes zero and DELETE is
    /// negative. DML first accumulates in `pending_live_row_deltas`; commit
    /// publishes it here and rollback discards it, so estimates never expose
    /// uncommitted or rolled-back population changes.
    table_live_row_deltas: parking_lot::RwLock<HashMap<u64, Arc<AtomicI64>>>,
    pending_live_row_deltas: parking_lot::Mutex<HashMap<(u64, u64), i64>>,
    /// Monotonic per-table data-change epoch for the CTE result cache.
    /// Unlike `table_modification_deltas` (reset by `take`/`drop` to fold
    /// into persisted stats), this only ever increases — bumped on every
    /// DML row batch (`record_table_modifications_by_id`) and on TRUNCATE
    /// (`drop_table_modification_delta`, reached via `reset_table_stats`).
    /// ANALYZE deliberately does NOT bump
    /// it: ANALYZE leaves the data unchanged. A CTE cache entry snapshots
    /// `(schema_version, write_epoch)` per dependency table and stays valid
    /// exactly while those tables are untouched, so unrelated background
    /// commits (e.g. auto-analyze of other tables) never invalidate it.
    table_write_epochs: parking_lot::RwLock<HashMap<u64, Arc<AtomicU64>>>,
    /// Per-`Database` tracker used by [`Database::apply_wal_record`]: txn
    /// ids whose apply trace touched a dictionary-table heap
    /// (`tables/<dd_id>.heap`). Their matching `CommitTxn` triggers
    /// the bounded CatalogDelta apply path so the replica's in-memory catalog
    /// reflects schema changes pushed from the master. Lives outside
    /// the txn manager so the manager's hot-path locks (taken millions
    /// of times per second under OLTP) don't pay any cost on a master
    /// where this set is permanently empty.
    applier_dd_dirty_txns: parking_lot::Mutex<HashMap<u64, CatalogApplyDelta>>,
    online_index_descriptors: parking_lot::RwLock<HashMap<u64, PinnedOnlineIndexDescriptor>>,
    pending_primary_recovery: parking_lot::Mutex<Vec<u64>>,
    /// Only statistics maintenance takes these locks; DML never waits here.
    /// Weak entries let idle/dropped tables release their maintenance state.
    analyze_locks: parking_lot::Mutex<HashMap<String, std::sync::Weak<parking_lot::Mutex<()>>>>,
    #[cfg(test)]
    analyze_batch_test_hook: Mutex<Option<Arc<DdlBackfillTestHook>>>,
    #[cfg(test)]
    analyze_test_hook: Mutex<Option<Arc<DdlBackfillTestHook>>>,
    #[cfg(test)]
    ddl_backfill_test_hook: Mutex<Option<Arc<DdlBackfillTestHook>>>,
    #[cfg(test)]
    online_index_replay_test_hook: Mutex<Option<Arc<DdlBackfillTestHook>>>,
    #[cfg(test)]
    online_copy_publish_test_hook: Mutex<Option<Arc<DdlBackfillTestHook>>>,
    #[cfg(test)]
    vector_copy_committed_test_hook: Mutex<Option<Arc<DdlBackfillTestHook>>>,
    #[cfg(test)]
    vector_apply_test_hook: Mutex<Option<Arc<DdlBackfillTestHook>>>,
    #[cfg(test)]
    online_index_validation_test_hook: Mutex<Option<Arc<DdlBackfillTestHook>>>,
}

#[cfg(not(test))]
const ROW_LOCK_SIGNAL_SHARDS: usize = 256;
#[cfg(test)]
const ROW_LOCK_SIGNAL_SHARDS: usize = 64;
#[cfg(not(test))]
const ACTIVE_SNAPSHOT_SHARDS: usize = 64;
#[cfg(test)]
const ACTIVE_SNAPSHOT_SHARDS: usize = 4;

/// Process-wide DML counters. Cheap `Relaxed` atomics so the hot DML
/// path takes no contention; the SHOW ENGINE MYDB STATUS surface
/// snapshots them via `dml_stats_snapshot`.
#[derive(Debug, Default)]
pub(crate) struct DmlStats {
    /// Rows successfully inserted into a non-temp table.
    pub insert_rows: AtomicU64,
    /// Rows successfully updated via the batch / single UPDATE paths
    /// (each affected row counts once; ON DUP KEY's "+2" affordance
    /// is at the executor layer and not double-counted here).
    pub update_rows: AtomicU64,
    /// Rows successfully deleted (tombstoned). Excludes REPLACE's
    /// internal deletes; those are counted as part of the conflict
    /// path's delete bookkeeping.
    pub delete_rows: AtomicU64,
    /// Times an INSERT IGNORE / UPDATE IGNORE swallowed a constraint
    /// or data-only error and surfaced it as a SQL warning.
    pub ignore_warnings: AtomicU64,
    /// Rows whose heap slot was physically removed because their
    /// owning INSERT batch could not finalize the row-lock + undo
    /// step (A3 atomicity bail-out). Should normally be zero; a
    /// non-zero reading is a forensics signal that something failed
    /// mid-batch and the engine recovered by rolling back the
    /// already-written heap rows.
    pub rollback_inserted_rows: AtomicU64,
    /// LOB chains released because they had been allocated for a
    /// row whose write/commit later failed (A2 lifecycle reclamation).
    /// Should match the number of leaked chains the recovery path
    /// would otherwise have left behind.
    pub lob_leaks_recovered: AtomicU64,
    /// M5.C3 — UPSERT (`INSERT ... ON DUPLICATE KEY UPDATE`) rows that
    /// hit the fast path: pre-probed an existing conflict row and went
    /// straight to UPDATE, skipping the speculative INSERT attempt that
    /// would have errored 1062. Each increment corresponds to one
    /// avoided heap write + index probe + lock acquire round-trip.
    pub upsert_fast_path_hits: AtomicU64,
    /// Batch-ODKU statements/chunks that entered the batch probe path.
    pub upsert_batch_attempts: AtomicU64,
    /// Conflict-free ODKU source rows written through storage batch insert.
    pub upsert_batch_rows: AtomicU64,
    /// Source rows identified as conflict boundaries and handled in order by
    /// the legacy ODKU row path.
    pub upsert_batch_conflict_rows: AtomicU64,
    /// Rows sent to the legacy path because the batch shape was ineligible or
    /// adaptive probing stopped producing useful runs.
    pub upsert_batch_fallback_rows: AtomicU64,
    /// Speculative batch segments rolled back after a concurrent 1062 race.
    pub upsert_batch_race_rollbacks: AtomicU64,
    /// M5.C5 — FK cascade rounds that landed on a previously-visited
    /// `(table, row)` pair and short-circuited. Each increment is one
    /// avoided redundant DELETE / SET NULL / UPDATE call. A non-zero
    /// reading mostly means the catalog has a CASCADE cycle the new
    /// visited-set guard is correctly absorbing.
    pub fk_cycle_short_circuits: AtomicU64,
    /// D2 — number of LOAD DATA statements currently running. Drops back
    /// to zero when none are in flight; non-zero readings tell the
    /// operator "a bulk load is still ingesting". Each LOAD DATA bumps
    /// this on entry and decrements on exit (RAII guard).
    pub load_data_active: AtomicU64,
    /// D2 — cumulative rows ingested by every LOAD DATA in this process.
    /// Combined with `load_data_active` and elapsed wall time this lets
    /// the operator estimate throughput.
    pub load_data_rows_loaded: AtomicU64,
    /// M1 (LOB) — on a full-row UPDATE, when a column already externalized in the old version has new bytes exactly
    /// equal to the old bytes, the new version reuses the old chain instead of rewriting it. Accumulates "chains reused".
    pub lob_chains_reused: AtomicU64,
    /// M1 (LOB) — externalized write bytes saved by chain reuse (= the sum of those
    /// chains' `total_len`), i.e. the write-amplification bytes avoided — a direct measure of the optimization.
    pub lob_bytes_reused: AtomicU64,
    /// M6 (LOB) — externally allocated chains on the externalize path (all chains allocated by INSERT and
    /// by an UPDATE rewriting a new version).
    pub lob_chains_externalized: AtomicU64,
    /// M6 (LOB) — externalized bytes written on the externalize path (the sum of the
    /// newly allocated chains' bytes). Compared against `lob_bytes_reused` this yields the "share that could have been saved".
    pub lob_bytes_externalized: AtomicU64,
    /// M3/M6 (LOB) — overflow pages cumulatively returned to the OS by `shrink_to_fit`
    /// (× 16 KiB = disk bytes reclaimed).
    pub lob_pages_shrunk: AtomicU64,
    /// BUG-7 (LOB) — count of LOB chain frees that **failed** in purge/rollback (chains unrecoverable
    /// due to overflow page corruption / lock failure etc. → permanent orphans holding space). Symmetric to
    /// `lob_leaks_recovered` (successful reclamation), giving operators a "silent leak" signal
    /// instead of the error being swallowed entirely in `free_external_refs`.
    pub lob_leaks_unrecovered: AtomicU64,
}

impl DmlStats {
    pub(crate) fn record_inserts(&self, n: u64) {
        if n > 0 {
            self.insert_rows.fetch_add(n, Ordering::Relaxed);
            crate::engine::status_counters::add_active_session_counter(
                crate::engine::status_counters::SessionCounter::HandlerWrite,
                n,
            );
        }
    }
    pub(crate) fn record_updates(&self, n: u64) {
        if n > 0 {
            self.update_rows.fetch_add(n, Ordering::Relaxed);
            crate::engine::status_counters::add_active_session_counter(
                crate::engine::status_counters::SessionCounter::HandlerUpdate,
                n,
            );
        }
    }
    pub(crate) fn record_deletes(&self, n: u64) {
        if n > 0 {
            self.delete_rows.fetch_add(n, Ordering::Relaxed);
            crate::engine::status_counters::add_active_session_counter(
                crate::engine::status_counters::SessionCounter::HandlerDelete,
                n,
            );
        }
    }
    pub(crate) fn record_ignore_warning(&self) {
        self.ignore_warnings.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_rollback_inserted_rows(&self, n: u64) {
        if n > 0 {
            self.rollback_inserted_rows.fetch_add(n, Ordering::Relaxed);
        }
    }
    pub(crate) fn record_lob_leak_recovered(&self, n: u64) {
        if n > 0 {
            self.lob_leaks_recovered.fetch_add(n, Ordering::Relaxed);
        }
    }
    pub(crate) fn record_lob_leak_unrecovered(&self, n: u64) {
        if n > 0 {
            self.lob_leaks_unrecovered.fetch_add(n, Ordering::Relaxed);
        }
    }
    pub(crate) fn record_upsert_fast_path_hit(&self) {
        self.upsert_fast_path_hits.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_upsert_batch_attempt(&self) {
        self.upsert_batch_attempts.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_upsert_batch_rows(&self, n: u64) {
        if n > 0 {
            self.upsert_batch_rows.fetch_add(n, Ordering::Relaxed);
        }
    }
    pub(crate) fn record_upsert_batch_conflict_row(&self) {
        self.upsert_batch_conflict_rows
            .fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_upsert_batch_fallback_rows(&self, n: u64) {
        if n > 0 {
            self.upsert_batch_fallback_rows
                .fetch_add(n, Ordering::Relaxed);
        }
    }
    pub(crate) fn record_upsert_batch_race_rollback(&self) {
        self.upsert_batch_race_rollbacks
            .fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_fk_cycle_short_circuit(&self) {
        self.fk_cycle_short_circuits.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn load_data_enter(&self) {
        self.load_data_active.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn load_data_exit(&self) {
        // Wrapping isn't a concern (we always enter before exit), but
        // saturating_sub guards against drift if a future refactor
        // double-exits.
        loop {
            let cur = self.load_data_active.load(Ordering::Relaxed);
            let next = cur.saturating_sub(1);
            if self
                .load_data_active
                .compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }
    pub(crate) fn record_load_data_rows(&self, n: u64) {
        if n > 0 {
            self.load_data_rows_loaded.fetch_add(n, Ordering::Relaxed);
        }
    }
    /// M1 (LOB) — records one chain reuse: chain count +1; bytes saved accumulate this chain's length.
    pub(crate) fn record_lob_chain_reused(&self, bytes: u64) {
        self.lob_chains_reused.fetch_add(1, Ordering::Relaxed);
        self.lob_bytes_reused.fetch_add(bytes, Ordering::Relaxed);
    }
    /// M6 (LOB) — records one externalized chain allocation: chain count +1; bytes accumulated.
    pub(crate) fn record_lob_chain_externalized(&self, bytes: u64) {
        self.lob_chains_externalized.fetch_add(1, Ordering::Relaxed);
        self.lob_bytes_externalized
            .fetch_add(bytes, Ordering::Relaxed);
    }
    /// M3 (LOB) — records the number of pages returned to disk by one shrink.
    pub(crate) fn record_lob_pages_shrunk(&self, pages: u64) {
        if pages > 0 {
            self.lob_pages_shrunk.fetch_add(pages, Ordering::Relaxed);
        }
    }
}

#[derive(Debug)]
struct AutoIncrementDirtyQueue {
    epoch: u64,
    entries: Vec<Arc<AutoIncrementState>>,
    charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

#[derive(Debug)]
struct AutoIncrementDirtyBatch {
    entries: Vec<Arc<AutoIncrementState>>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl AutoIncrementDirtyQueue {
    fn new() -> Result<Self, QueryError> {
        Ok(Self {
            epoch: 1,
            entries: Vec::new(),
            charge: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                0,
            )?,
        })
    }

    fn enqueue(&mut self, state: Arc<AutoIncrementState>) -> Result<(), QueryError> {
        if state.queued_epoch.load(Ordering::Acquire) == self.epoch {
            return Ok(());
        }
        if self.entries.len() == self.entries.capacity() {
            let old = self.entries.capacity();
            let target = old.saturating_mul(2).max(16);
            let extra = target
                .saturating_sub(old)
                .saturating_mul(std::mem::size_of::<Arc<AutoIncrementState>>());
            self.charge.try_grow(extra)?;
            self.entries.reserve_exact(target.saturating_sub(old));
        }
        state.queued_epoch.store(self.epoch, Ordering::Release);
        self.entries.push(state);
        Ok(())
    }

    fn rotate(&mut self) -> AutoIncrementDirtyBatch {
        self.epoch = self.epoch.saturating_add(1).max(1);
        let entries = std::mem::take(&mut self.entries);
        let charge = std::mem::replace(
            &mut self.charge,
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                0,
            )
            .expect("zero-byte AUTO_INCREMENT queue reservation cannot fail"),
        );
        AutoIncrementDirtyBatch {
            entries,
            _charge: charge,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DmlStatsSnapshot {
    pub insert_rows: u64,
    pub update_rows: u64,
    pub delete_rows: u64,
    pub ignore_warnings: u64,
    pub rollback_inserted_rows: u64,
    pub lob_leaks_recovered: u64,
    /// M5.C3 — UPSERT rows that took the fast path. Surfaced via
    /// SHOW ENGINE MYDB STATUS for forensics / perf-regression tests.
    pub upsert_fast_path_hits: u64,
    pub upsert_batch_attempts: u64,
    pub upsert_batch_rows: u64,
    pub upsert_batch_conflict_rows: u64,
    pub upsert_batch_fallback_rows: u64,
    pub upsert_batch_race_rollbacks: u64,
    /// M5.C5 — count of cascade rounds the visited-set guard absorbed.
    pub fk_cycle_short_circuits: u64,
    /// D2 — current count of in-flight LOAD DATA statements.
    pub load_data_active: u64,
    /// D2 — cumulative rows ingested by LOAD DATA.
    pub load_data_rows_loaded: u64,
    /// M1 (LOB) — chains reused / bytes saved.
    pub lob_chains_reused: u64,
    pub lob_bytes_reused: u64,
    /// M6 (LOB) — externally allocated chains / bytes.
    pub lob_chains_externalized: u64,
    pub lob_bytes_externalized: u64,
    /// M3 (LOB) — overflow pages returned to disk by shrink.
    pub lob_pages_shrunk: u64,
    /// BUG-7 (LOB) — count of LOB chain frees that failed (not reclaimed) in purge/rollback.
    pub lob_leaks_unrecovered: u64,
}

/// Coerced/materialized rows shared by the batch ODKU conflict probe and the
/// final storage insert. AUTO_INCREMENT tables are deliberately excluded by
/// the executor in the first implementation, so preparation never reserves or
/// burns sequence values.
#[derive(Debug)]
pub(crate) struct PreparedInsertBatch {
    pub(crate) table_id: u64,
    pub(crate) rows: Vec<Vec<Value>>,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct DdlBackfillTestHook {
    reached: (Mutex<bool>, Condvar),
    released: (Mutex<bool>, Condvar),
}

#[derive(Debug, Clone)]
pub struct AlterCopyJobSnapshot {
    pub job_id: u64,
    pub table_id: u64,
    pub database: String,
    pub table: String,
    pub operation: &'static str,
    pub object_name: String,
    pub state: String,
    pub copied_rows: u64,
    pub total_rows: Option<u64>,
    pub replay_watermark: Option<u64>,
    pub error_message: Option<String>,
    pub cancel_allowed: bool,
}

#[derive(Debug)]
struct AlterCopyJobHistoryEntry {
    snapshot: AlterCopyJobSnapshot,
    _memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

#[derive(Debug, Clone)]
pub struct PartitionDdlJobSnapshot {
    pub job_id: u64,
    pub database: String,
    pub table: String,
    pub operation: String,
    pub state: String,
    pub copied_rows: u64,
    pub total_rows: Option<u64>,
    pub replay_watermark: Option<u64>,
    pub error_message: Option<String>,
    pub cancel_allowed: bool,
}

#[derive(Debug)]
struct AlterCopyJobProgress {
    state: String,
    copied_rows: u64,
    total_rows: Option<u64>,
    replay_watermark: Option<u64>,
    error_message: Option<String>,
}

#[derive(Debug)]
struct AlterCopyChangeLog {
    touched_rows: Vec<(u64, u64, RowId)>,
    invalidated: bool,
    spill: Option<File>,
    spilled_rows: u64,
    data_dir: PathBuf,
    memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl AlterCopyChangeLog {
    fn new(data_dir: &Path) -> Self {
        Self {
            touched_rows: Vec::new(),
            invalidated: false,
            spill: None,
            spilled_rows: 0,
            data_dir: data_dir.to_path_buf(),
            memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::DdlOnlineLog,
                0,
            )
            .expect("zero-byte online DDL reservation must succeed"),
        }
    }
}

#[derive(Debug)]
struct AlterCopyJob {
    job_id: u64,
    table_id: u64,
    database: String,
    table: String,
    operation: &'static str,
    object_name: String,
    capture_rows: bool,
    cancel_token: crate::engine::cancel::QueryCancelToken,
    progress: Mutex<AlterCopyJobProgress>,
    changes: Mutex<AlterCopyChangeLog>,
    cancel_requested: AtomicBool,
    retired_vector_graphs: Mutex<Vec<Arc<VectorGraph>>>,
    retired_files: Mutex<Vec<PathBuf>>,
}

#[cfg(test)]
impl DdlBackfillTestHook {
    fn new() -> Self {
        Self {
            reached: (Mutex::new(false), Condvar::new()),
            released: (Mutex::new(false), Condvar::new()),
        }
    }

    pub(crate) fn wait_until_reached(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let (lock, cv) = &self.reached;
        let mut reached = lock.lock().unwrap_or_else(|e| e.into_inner());
        while !*reached {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next, wait) = cv
                .wait_timeout(reached, remaining)
                .unwrap_or_else(|e| e.into_inner());
            reached = next;
            if wait.timed_out() && !*reached {
                return false;
            }
        }
        true
    }

    pub(crate) fn release(&self) {
        let (lock, cv) = &self.released;
        let mut released = lock.lock().unwrap_or_else(|e| e.into_inner());
        *released = true;
        cv.notify_all();
    }

    fn pause(&self) {
        {
            let (lock, cv) = &self.reached;
            let mut reached = lock.lock().unwrap_or_else(|e| e.into_inner());
            *reached = true;
            cv.notify_all();
        }
        let (lock, cv) = &self.released;
        let mut released = lock.lock().unwrap_or_else(|e| e.into_inner());
        while !*released {
            released = cv.wait(released).unwrap_or_else(|e| e.into_inner());
        }
    }
}

pub(crate) struct RowLockSignals {
    shards: Box<[RowLockSignalShard]>,
}

pub(crate) struct RowLockSignalShard {
    /// Monotonic counter bumped on every notify. Waiters snapshot it before
    /// they can fail to acquire the lock and only block if it has not advanced
    /// — closes the lost-wakeup race when a notifier fires between the failed
    /// lock acquire and the call to `wait`.
    counter: Mutex<u64>,
    cv: Condvar,
}

impl std::fmt::Debug for RowLockSignals {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowLockSignals")
            .field("shards", &self.shards.len())
            .finish()
    }
}

impl RowLockSignals {
    pub(crate) fn new() -> Self {
        let mut shards = Vec::with_capacity(ROW_LOCK_SIGNAL_SHARDS);
        for _ in 0..ROW_LOCK_SIGNAL_SHARDS {
            shards.push(RowLockSignalShard {
                counter: Mutex::new(0),
                cv: Condvar::new(),
            });
        }
        Self {
            shards: shards.into_boxed_slice(),
        }
    }

    fn shard_index_for(row_id: RowId) -> usize {
        let mut h = DefaultHasher::new();
        row_id.hash(&mut h);
        (h.finish() as usize) % ROW_LOCK_SIGNAL_SHARDS
    }

    /// Snapshot the shard counter for `row_id` before attempting to acquire a
    /// row lock. Pass the returned value to `wait_after` to skip the wait if
    /// any notification happened in between.
    pub(crate) fn snapshot(&self, row_id: RowId) -> u64 {
        let shard = &self.shards[Self::shard_index_for(row_id)];
        shard.counter.lock().map(|g| *g).unwrap_or(0)
    }

    /// Wait up to `timeout` on the shard for `row_id`, but only block when the
    /// shard counter is still equal to `prior`. If the counter advanced
    /// between snapshot and now, return immediately so the caller retries.
    pub(crate) fn wait_after(&self, row_id: RowId, prior: u64, timeout: Duration) -> bool {
        let shard = &self.shards[Self::shard_index_for(row_id)];
        let Ok(guard) = shard.counter.lock() else {
            return false;
        };
        if *guard != prior {
            return true;
        }
        if let Ok((_g, res)) = shard.cv.wait_timeout(guard, timeout) {
            !res.timed_out()
        } else {
            false
        }
    }

    /// Notify all shards corresponding to the given `row_ids`. Each shard is
    /// signalled at most once even when multiple ids hash to the same shard.
    pub(crate) fn notify_rows<I: IntoIterator<Item = RowId>>(&self, row_ids: I) {
        let mut bitmap = [0u64; ROW_LOCK_SIGNAL_SHARDS / 64];
        for row_id in row_ids {
            let idx = Self::shard_index_for(row_id);
            bitmap[idx / 64] |= 1u64 << (idx % 64);
        }
        for (word_idx, mut word) in bitmap.iter().copied().enumerate() {
            while word != 0 {
                let bit = word.trailing_zeros() as usize;
                word &= word - 1;
                let idx = word_idx * 64 + bit;
                let shard = &self.shards[idx];
                if let Ok(mut guard) = shard.counter.lock() {
                    *guard = guard.wrapping_add(1);
                    shard.cv.notify_all();
                }
            }
        }
    }

    /// Notify every shard. Used as a coarse-grained fallback when callers
    /// can't enumerate the released rows precisely.
    pub(crate) fn notify_all(&self) {
        for shard in self.shards.iter() {
            if let Ok(mut guard) = shard.counter.lock() {
                *guard = guard.wrapping_add(1);
                shard.cv.notify_all();
            }
        }
    }

    /// P4 coarse backstop: bump every shard generation counter without the
    /// condvar broadcast (paired with `RowLockParkers::unpark_all`).
    pub(crate) fn bump_all(&self) {
        for shard in self.shards.iter() {
            if let Ok(mut guard) = shard.counter.lock() {
                *guard = guard.wrapping_add(1);
            }
        }
    }

    /// P4 directed-wakeup backstop: bump the shard generation counters for the
    /// given rows *without* the `notify_all` condvar broadcast. Paired with
    /// `RowLockParkers::unpark` so only the row's front waiter is woken
    /// (no thundering herd), while a waiter that snapshotted the counter
    /// before this bump still skips parking (closes the lost-wakeup race; the
    /// bounded `park_timeout` re-poll is the final backstop).
    pub(crate) fn bump_rows<I: IntoIterator<Item = RowId>>(&self, row_ids: I) {
        let mut bitmap = [0u64; ROW_LOCK_SIGNAL_SHARDS / 64];
        for row_id in row_ids {
            let idx = Self::shard_index_for(row_id);
            bitmap[idx / 64] |= 1u64 << (idx % 64);
        }
        for (word_idx, mut word) in bitmap.iter().copied().enumerate() {
            while word != 0 {
                let bit = word.trailing_zeros() as usize;
                word &= word - 1;
                let idx = word_idx * 64 + bit;
                if let Ok(mut guard) = self.shards[idx].counter.lock() {
                    *guard = guard.wrapping_add(1);
                }
            }
        }
    }
}

/// P4 — directed row-lock waiter wakeup registry. Maps each blocked
/// transaction to its parked OS thread so a lock release can wake *only*
/// the released row's front waiter (`thread::unpark`) instead of the
/// 256-shard `notify_all` thundering herd, which woke every waiter on the
/// shard — they all stampeded the global `lock_manager` write latch, lost
/// the race, and re-parked (O(N²) latch traffic + the `swtch_pri` convoy
/// that dominates contended OLTP profiles). Sharded by `txn_id` so
/// register/unregister on the hot path don't serialize on one mutex.
///
/// Correctness rests on `thread::park`/`unpark` token semantics (an unpark
/// that races ahead of the park is remembered, so it is never lost) plus the
/// bounded `park_timeout` re-poll in `wait_for_specific_row`: even if a wake
/// is missed (e.g. the front waiter changed, or a compatible S-waiter run was
/// not fully woken) the parked thread re-checks within one slice.
pub(crate) struct RowLockParkers {
    shards: Box<[Mutex<HashMap<u64, thread::Thread>>]>,
    active: AtomicUsize,
}

impl std::fmt::Debug for RowLockParkers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowLockParkers")
            .field("shards", &self.shards.len())
            .finish()
    }
}

impl RowLockParkers {
    pub(crate) fn new() -> Self {
        let mut shards = Vec::with_capacity(ROW_LOCK_SIGNAL_SHARDS);
        for _ in 0..ROW_LOCK_SIGNAL_SHARDS {
            shards.push(Mutex::new(HashMap::new()));
        }
        Self {
            shards: shards.into_boxed_slice(),
            active: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn shard(&self, txn_id: u64) -> &Mutex<HashMap<u64, thread::Thread>> {
        &self.shards[(txn_id as usize) % ROW_LOCK_SIGNAL_SHARDS]
    }

    /// Record the current thread as the parker for `txn_id` while it blocks on
    /// a row lock. The returned guard unregisters on drop, so every exit path
    /// (acquired, timed out, cancelled, error) is leak-free.
    pub(crate) fn register(&self, txn_id: u64) -> RowLockParkGuard<'_> {
        if let Ok(mut map) = self.shard(txn_id).lock() {
            if map.insert(txn_id, thread::current()).is_none() {
                self.active.fetch_add(1, Ordering::SeqCst);
            }
        }
        RowLockParkGuard {
            parkers: self,
            txn_id,
        }
    }

    fn unregister(&self, txn_id: u64) {
        if let Ok(mut map) = self.shard(txn_id).lock() {
            if map.remove(&txn_id).is_some() {
                self.active.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }

    pub(crate) fn active_count(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    /// Wake exactly the thread parked for `txn_id`, if any.
    pub(crate) fn unpark(&self, txn_id: u64) {
        let handle = self
            .shard(txn_id)
            .lock()
            .ok()
            .and_then(|map| map.get(&txn_id).cloned());
        if let Some(t) = handle {
            t.unpark();
        }
    }

    /// Wake every parked row-lock waiter. Coarse fallback for releases that
    /// can't enumerate the unblocked rows (e.g. MDL/unique/table-lock paths).
    pub(crate) fn unpark_all(&self) {
        for shard in self.shards.iter() {
            if let Ok(map) = shard.lock() {
                for t in map.values() {
                    t.unpark();
                }
            }
        }
    }
}

pub(crate) struct RowLockParkGuard<'a> {
    parkers: &'a RowLockParkers,
    txn_id: u64,
}

impl Drop for RowLockParkGuard<'_> {
    fn drop(&mut self) {
        self.parkers.unregister(self.txn_id);
    }
}

/// Number of shards for the sharded table-lock store. Tables are few but
/// hot; transactions are many. 256 spreads both the per-table holder maps
/// and the per-txn membership maps so concurrent DML on distinct tables (or
/// distinct txns on one hot table) rarely collide on a shard latch.
#[cfg(not(test))]
const TABLE_LOCK_SHARDS: usize = 256;
#[cfg(test)]
const TABLE_LOCK_SHARDS: usize = 8;

/// Outcome of a sharded table-lock acquire attempt, returned to the `Database`
/// orchestration layer so it can perform the *rare* global-deadlock-graph
/// interactions (recording a wait edge on conflict, clearing a stale wait edge
/// when a previously-blocked acquire finally succeeds) without ever touching
/// the global `lock_manager` latch on the uncontended fast path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TableAcquireOutcome {
    /// Granted (fresh acquire or compatible promotion). `cleared_wait` is true
    /// when this txn had an outstanding table-lock wait that the grant just
    /// resolved, so the caller must drop its `waits_for` edge in the global
    /// deadlock graph.
    Acquired {
        cleared_wait: bool,
    },
    /// The txn already holds this table at an equal-or-stronger mode — no-op.
    AlreadyHeld,
    /// Blocked by `owner` (an incompatible holder). The caller records the
    /// wait edge in the global deadlock graph and returns the lock-wait error.
    Conflict {
        owner: u64,
    },
    SchemaChanged,
}

/// TPC-C lock de-contention (P2 "safe slice"): the table-level (IS/IX/S/SIX/X)
/// lock subsystem split out of the global `lock_manager` `RwLock` into a store
/// sharded by `table_id`. Every DML statement takes an IX (intent-exclusive)
/// lock on its target table so concurrent DDL (which takes X) is blocked; under
/// write-heavy load (e.g. TPC-C NewOrder touching ~8 tables) this used to be
/// ~8 first-touch writes plus ~15 "already-held" read-lock checks per
/// transaction, all serialising on the one global lock latch. IX locks are
/// mutually compatible, so the common case never logically conflicts — it only
/// needs a per-table membership record visible to DDL and releasable at commit.
/// Sharding that record by `table_id` lets the IX hot path run entirely on a
/// fine-grained per-shard latch, off the global lock.
///
/// Single source of truth: ALL table-lock state lives here, not in
/// `LockManager::table_locks` (which is now only a staging area for crash
/// recovery — `from_wal` rebuilds prepared-XA table locks there and
/// `Database` migrates them in at startup). Cold consumers (data_locks view,
/// XA prepare serialization, DDL conflict checks) read this store directly.
///
/// Lock order is **txn-shard → table-shard**, used identically by acquire,
/// conflict, and release, so two latches are never taken in opposing orders
/// (no AB-BA). The rare conflict / grant-after-wait paths additionally take the
/// global `lock_manager` to touch the cross-shard `waits_for` deadlock graph;
/// the uncontended grant path never does.
pub(crate) struct ShardedTableLocks {
    /// Per-table holder maps: `table_id -> {txn_id -> mode}`. Sharded by table.
    /// ahash, not std SipHash: these `u64`-keyed maps are probed on every
    /// table-lock acquire/release (i.e. every statement) — Time Profiler put
    /// SipHash in `acquire_table_lock_sharded`/`release_txn` right behind the
    /// WAL critical section as the next on-CPU hashing cost.
    table_shards:
        Box<[parking_lot::Mutex<hashbrown::HashMap<u64, TableLockState, ahash::RandomState>>]>,
    /// Per-txn membership: `txn_id -> (tables it holds, pending-wait flag)`.
    /// Sharded by txn. Drives O(held) release at commit without scanning every
    /// table shard, and carries the `waiting` flag that lets a grant-after-wait
    /// be detected without a global-lock probe on every acquire.
    txn_shards:
        Box<[parking_lot::Mutex<hashbrown::HashMap<u64, TxnTableEntry, ahash::RandomState>>]>,
}

#[derive(Debug, Default)]
struct TxnTableEntry {
    /// Tables this txn currently holds a lock on (any mode). Used to release at
    /// commit/rollback; the authoritative mode lives in the table shard.
    tables: Vec<u64>,
    /// Set when an acquire by this txn last conflicted and the caller recorded
    /// a `waits_for` edge; cleared (and reported) when a later acquire by the
    /// same txn succeeds, so the caller knows to drop that edge.
    waiting: bool,
}

impl std::fmt::Debug for ShardedTableLocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedTableLocks")
            .field("shards", &self.table_shards.len())
            .finish()
    }
}

impl ShardedTableLocks {
    fn holds_exclusive(&self, txn_id: u64, table_id: u64) -> bool {
        self.table_shard(table_id)
            .lock()
            .get(&table_id)
            .and_then(|state| state.holders.get(&txn_id))
            == Some(&TableLockMode::Exclusive)
    }

    /// Online index DDL retains object lifetime protection while permitting
    /// IX writers. Keep the ticket continuously installed across X -> IS.
    fn downgrade_exclusive_to_intent_shared(
        &self,
        txn_id: u64,
        table_id: u64,
    ) -> Result<(), QueryError> {
        self.downgrade_exclusive_to_shared_mode(txn_id, table_id, TableLockMode::IntentShared)
    }

    fn downgrade_exclusive_to_shared_mode(
        &self,
        txn_id: u64,
        table_id: u64,
        requested: TableLockMode,
    ) -> Result<(), QueryError> {
        debug_assert!(matches!(
            requested,
            TableLockMode::IntentShared | TableLockMode::Shared
        ));
        let _txn = self.txn_shard(txn_id).lock();
        let mut tables = self.table_shard(table_id).lock();
        let mode = tables
            .get_mut(&table_id)
            .and_then(|state| state.holders.get_mut(&txn_id))
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "missing online DDL table lock",
                )
            })?;
        if !matches!(*mode, TableLockMode::Exclusive | TableLockMode::Shared) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "online DDL requires a write-excluding prepare lock",
            ));
        }
        *mode = requested;
        Ok(())
    }

    pub(crate) fn new() -> Self {
        let mut table_shards = Vec::with_capacity(TABLE_LOCK_SHARDS);
        let mut txn_shards = Vec::with_capacity(TABLE_LOCK_SHARDS);
        for _ in 0..TABLE_LOCK_SHARDS {
            table_shards.push(parking_lot::Mutex::new(hashbrown::HashMap::default()));
            txn_shards.push(parking_lot::Mutex::new(hashbrown::HashMap::default()));
        }
        Self {
            table_shards: table_shards.into_boxed_slice(),
            txn_shards: txn_shards.into_boxed_slice(),
        }
    }

    #[inline]
    fn table_shard(
        &self,
        table_id: u64,
    ) -> &parking_lot::Mutex<hashbrown::HashMap<u64, TableLockState, ahash::RandomState>> {
        &self.table_shards[(table_id as usize) % TABLE_LOCK_SHARDS]
    }

    #[inline]
    fn txn_shard(
        &self,
        txn_id: u64,
    ) -> &parking_lot::Mutex<hashbrown::HashMap<u64, TxnTableEntry, ahash::RandomState>> {
        &self.txn_shards[(txn_id as usize) % TABLE_LOCK_SHARDS]
    }

    /// Attempt to acquire `mode` on `table_id` for `txn_id`. Mirrors the exact
    /// semantics of the legacy `LockManager::acquire_table_lock` (already-held
    /// no-op, compatible promotion, incompatible-holder conflict) but on the
    /// sharded latches. Never touches the global lock manager.
    pub(crate) fn try_acquire(
        &self,
        txn_id: u64,
        table_id: u64,
        mode: TableLockMode,
    ) -> TableAcquireOutcome {
        self.try_acquire_bound(txn_id, table_id, mode, None)
    }

    fn try_acquire_bound(
        &self,
        txn_id: u64,
        table_id: u64,
        mode: TableLockMode,
        version: Option<u64>,
    ) -> TableAcquireOutcome {
        if txn_id == 0 && version.is_some() {
            let tables = self.table_shard(table_id).lock();
            return if tables.get(&table_id).is_some_and(|s| {
                s.minimum_schema_version != 0
                    || s.holders.values().any(|m| *m == TableLockMode::Exclusive)
            }) {
                TableAcquireOutcome::SchemaChanged
            } else {
                TableAcquireOutcome::AlreadyHeld
            };
        }
        // Lock order: txn shard first, then table shard.
        let mut tentry_map = self.txn_shard(txn_id).lock();
        let tentry = tentry_map.entry(txn_id).or_default();
        let mut table_map = self.table_shard(table_id).lock();
        let state = table_map.entry(table_id).or_default();
        if version.is_some_and(|v| v < state.minimum_schema_version)
            || (txn_id == 0 && state.minimum_schema_version != 0)
        {
            return TableAcquireOutcome::SchemaChanged;
        }

        if let Some(existing) = state.holders.get(&txn_id).copied() {
            let mode = existing.combine(mode);
            if mode == existing {
                return TableAcquireOutcome::AlreadyHeld;
            }
            if let Some(owner) = state.holders.iter().find_map(|(owner, held)| {
                (*owner != txn_id && !TableLockMode::is_compatible(*held, mode)).then_some(*owner)
            }) {
                tentry.waiting = true;
                return TableAcquireOutcome::Conflict { owner };
            }
            state.holders.insert(txn_id, mode);
            let cleared_wait = std::mem::take(&mut tentry.waiting);
            return TableAcquireOutcome::Acquired { cleared_wait };
        }

        // Fresh acquire: conflict if any (other) holder is incompatible.
        if let Some(owner) = state.holders.iter().find_map(|(owner, held)| {
            (!TableLockMode::is_compatible(*held, mode)).then_some(*owner)
        }) {
            tentry.waiting = true;
            return TableAcquireOutcome::Conflict { owner };
        }
        state.holders.insert(txn_id, mode);
        if !tentry.tables.contains(&table_id) {
            tentry.tables.push(table_id);
        }
        let cleared_wait = std::mem::take(&mut tentry.waiting);
        TableAcquireOutcome::Acquired { cleared_wait }
    }

    /// Release every table lock held by `txn_id` (commit / rollback). Returns
    /// the freed `table_id`s so the caller can wake any table-lock waiters.
    pub(crate) fn release_txn(&self, txn_id: u64) -> Vec<u64> {
        // Lock order: drain the txn shard first, then touch table shards
        // (txn-shard → table-shard), matching `try_acquire`.
        let entry = self.txn_shard(txn_id).lock().remove(&txn_id);
        let Some(entry) = entry else {
            return Vec::new();
        };
        for &table_id in &entry.tables {
            let mut map = self.table_shard(table_id).lock();
            if let Some(state) = map.get_mut(&table_id) {
                state.holders.remove(&txn_id);
                if state.holders.is_empty() && state.minimum_schema_version == 0 {
                    map.remove(&table_id);
                }
            }
        }
        entry.tables
    }

    fn set_schema_floor(&self, table_id: u64, version: u64) {
        let mut tables = self.table_shard(table_id).lock();
        let state = tables.entry(table_id).or_default();
        state.minimum_schema_version = state.minimum_schema_version.max(version);
    }

    /// DDL guard: is any txn (other than `exclude`) holding a lock on `table_id`?
    /// Backs `check_ddl_table_lock`. `exclude` is the synthetic/own txn id to
    /// ignore (`u64::MAX` for a pure DDL probe).
    pub(crate) fn holders_excluding(
        &self,
        table_id: u64,
        exclude: u64,
    ) -> Vec<(u64, TableLockMode)> {
        self.table_shard(table_id)
            .lock()
            .get(&table_id)
            .map(|state| {
                state
                    .holders
                    .iter()
                    .filter(|(owner, _)| **owner != exclude)
                    .map(|(owner, mode)| (*owner, *mode))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The table locks held by `txn_id`, for XA PREPARE serialization. Cold
    /// path (only at XA PREPARE).
    pub(crate) fn tables_for_txn(&self, txn_id: u64) -> Vec<(u64, TableLockMode)> {
        let tables = self
            .txn_shard(txn_id)
            .lock()
            .get(&txn_id)
            .map(|entry| entry.tables.clone())
            .unwrap_or_default();
        let mut out = Vec::with_capacity(tables.len());
        for table_id in tables {
            if let Some(mode) = self
                .table_shard(table_id)
                .lock()
                .get(&table_id)
                .and_then(|state| state.holders.get(&txn_id).copied())
            {
                out.push((table_id, mode));
            }
        }
        out
    }

    /// Reinstate a recovered/prepared table lock (crash recovery migration).
    /// Single-threaded startup path.
    pub(crate) fn reinstate(&self, txn_id: u64, table_id: u64, mode: TableLockMode) {
        self.txn_shard(txn_id)
            .lock()
            .entry(txn_id)
            .or_default()
            .tables
            .push(table_id);
        self.table_shard(table_id)
            .lock()
            .entry(table_id)
            .or_default()
            .holders
            .insert(txn_id, mode);
    }

    /// Snapshot of all (table_id, txn_id, mode) holders for the
    /// `performance_schema.data_locks` view. Cold path.
    pub(crate) fn snapshot_holders(&self) -> Vec<(u64, u64, TableLockMode)> {
        let mut out = Vec::new();
        for shard in self.table_shards.iter() {
            for (table_id, state) in shard.lock().iter() {
                for (txn_id, mode) in &state.holders {
                    out.push((*table_id, *txn_id, *mode));
                }
            }
        }
        out
    }
}

/// RAII waiter-presence counter. Held for exactly the span a thread is parked
/// on a lock signal, so `notify_lock_waiters` can read the underlying counter
/// and cheaply skip waking an empty queue (its expensive 256-shard
/// `RowLockSignals::notify_all` storm in particular). `SeqCst` so a notifier's
/// load is totally ordered against the increment; the bounded-timeout waiters
/// are the backstop for any residual race.
pub(crate) struct LockWaiterGuard<'a> {
    counter: &'a AtomicU64,
}

impl<'a> LockWaiterGuard<'a> {
    #[inline]
    pub(crate) fn new(counter: &'a AtomicU64) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self { counter }
    }
}

impl Drop for LockWaiterGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct CheckpointState {
    last_checkpoint_lsn: u64,
    last_checkpoint_unix_ms: Option<u64>,
}

#[derive(Debug)]
struct ReplicaRestartpointTracker {
    dirty_paths: HashSet<String>,
    auto_increment_lsns: BTreeSet<u64>,
    sequence_lsns: BTreeMap<u64, (u64, u64)>,
    memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    /// Startup WAL recovery writes files before the live apply tracker exists.
    /// The first restartpoint therefore performs one full data-tree sync.
    needs_full_sync: bool,
}

impl ReplicaRestartpointTracker {
    fn new(needs_full_sync: bool) -> Self {
        Self {
            dirty_paths: HashSet::new(),
            auto_increment_lsns: BTreeSet::new(),
            sequence_lsns: BTreeMap::new(),
            memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                0,
            )
            .expect("zero-byte replica restartpoint reservation cannot fail"),
            needs_full_sync,
        }
    }

    fn accounted_capacity(&self) -> usize {
        self.dirty_paths
            .capacity()
            .saturating_mul(std::mem::size_of::<String>() + 1)
            .saturating_add(
                self.dirty_paths
                    .iter()
                    .map(|path| path.capacity())
                    .sum::<usize>(),
            )
            .saturating_add(self.auto_increment_lsns.len().saturating_mul(48))
            .saturating_add(self.sequence_lsns.len().saturating_mul(80))
    }

    fn reconcile_charge(&mut self) -> Result<(), QueryError> {
        let desired = self.accounted_capacity();
        let charged = self.memory_charge.bytes();
        if desired > charged {
            self.memory_charge.try_grow(desired - charged)
        } else {
            self.memory_charge.shrink(charged - desired);
            Ok(())
        }
    }

    fn register(&mut self, path: &str) -> Result<(), QueryError> {
        if self.dirty_paths.contains(path) {
            return Ok(());
        }
        // Reserve a conservative capacity envelope before either container can
        // allocate. The exact capacity is reconciled afterwards; an overly
        // generous envelope is returned immediately.
        let old_capacity = self.dirty_paths.capacity();
        let predicted_capacity = old_capacity.saturating_mul(4).saturating_add(16);
        let envelope = path.len().saturating_add(
            predicted_capacity
                .saturating_sub(old_capacity)
                .saturating_mul(std::mem::size_of::<String>() + 1),
        );
        self.memory_charge.try_grow(envelope)?;
        let mut owned = String::new();
        if owned.try_reserve_exact(path.len()).is_err() || self.dirty_paths.try_reserve(1).is_err()
        {
            self.reconcile_charge()?;
            return Err(crate::storage::error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "Out of memory tracking replica restartpoint file",
            ));
        }
        owned.push_str(path);
        self.dirty_paths.insert(owned);
        self.reconcile_charge()
    }

    fn register_sequence(&mut self, counter_id: u64, lsn: u64) -> Result<(), QueryError> {
        if let Some(entry) = self.sequence_lsns.get_mut(&counter_id) {
            entry.1 = entry.1.max(lsn);
            return Ok(());
        }
        self.memory_charge.try_grow(80)?;
        self.sequence_lsns.insert(counter_id, (lsn, lsn));
        Ok(())
    }
    fn cover_sequence_before(&mut self, covered: u64) {
        self.sequence_lsns.retain(|_, (floor, latest)| {
            if *latest < covered {
                false
            } else {
                *floor = (*floor).max(covered);
                true
            }
        });
        let _ = self.reconcile_charge();
    }
    fn sequence_floor(&self) -> Option<u64> {
        self.sequence_lsns.values().map(|v| v.0).min()
    }

    fn register_auto_increment(&mut self, lsn: u64) -> Result<(), QueryError> {
        if self.auto_increment_lsns.contains(&lsn) {
            return Ok(());
        }
        self.memory_charge.try_grow(48)?;
        self.auto_increment_lsns.insert(lsn);
        self.reconcile_charge()
    }

    fn cover_auto_increment_before(&mut self, covered_lsn: u64) {
        self.auto_increment_lsns.retain(|lsn| *lsn >= covered_lsn);
        let _ = self.reconcile_charge();
    }

    fn auto_increment_floor(&self) -> Option<u64> {
        self.auto_increment_lsns.first().copied()
    }

    fn clear_and_release(&mut self) {
        self.dirty_paths.clear();
        self.dirty_paths.shrink_to_fit();
        let desired = self.accounted_capacity();
        let charged = self.memory_charge.bytes();
        self.memory_charge.shrink(charged.saturating_sub(desired));
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BufferStats {
    pub(crate) hits: u64,
    pub(crate) misses: u64,
}

/// Query-scoped buffer accounting used by execution diagnostics. The handle
/// is propagated to parallel workers together with the executor's stats
/// collector, so unrelated database traffic cannot leak into a query's
/// EXPLAIN ANALYZE counters.
#[derive(Debug, Default)]
pub(crate) struct QueryBufferStats {
    hits: AtomicU64,
    misses: AtomicU64,
}

struct QueryIoContext {
    total: BufferStats,
}

thread_local! {
    static QUERY_BUFFER_STATS: RefCell<Option<Arc<QueryBufferStats>>> = const { RefCell::new(None) };
    static QUERY_BUFFER_STATS_PTR: std::cell::Cell<*const QueryBufferStats> = const { std::cell::Cell::new(std::ptr::null()) };
    static QUERY_IO_CONTEXT_PTR: std::cell::Cell<*mut QueryIoContext> = const { std::cell::Cell::new(std::ptr::null_mut()) };
    // Channel-backed PlanStreams do their storage work on another thread.
    // A borrowed thread-local counter cannot cross that boundary, so the
    // active stream node publishes into this Arc-backed sink. The execution
    // context captures and reinstalls it in the worker.
    static SHARED_OPERATOR_BUFFER_STATS: RefCell<Option<Arc<QueryBufferStats>>> = const { RefCell::new(None) };
    static SHARED_OPERATOR_BUFFER_STATS_PTR: std::cell::Cell<*const QueryBufferStats> = const { std::cell::Cell::new(std::ptr::null()) };
}

pub(crate) fn with_query_buffer_stats<F, R>(stats: Arc<QueryBufferStats>, f: F) -> R
where
    F: FnOnce() -> R,
{
    let ptr = Arc::as_ptr(&stats);
    if QUERY_BUFFER_STATS_PTR.with(|current| current.get() == ptr) {
        return f();
    }

    struct Restore {
        previous: Option<Arc<QueryBufferStats>>,
        previous_ptr: *const QueryBufferStats,
        previous_context_ptr: *mut QueryIoContext,
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            QUERY_BUFFER_STATS.with(|cell| {
                let current = cell.borrow_mut().take();
                let context_ptr = QUERY_IO_CONTEXT_PTR.with(|context| context.get());
                if let (Some(current), false) = (current.as_ref(), context_ptr.is_null()) {
                    let total = unsafe { &*context_ptr }.total;
                    current.hits.fetch_add(total.hits, Ordering::Relaxed);
                    current.misses.fetch_add(total.misses, Ordering::Relaxed);
                }
                *cell.borrow_mut() = self.previous.take();
                QUERY_BUFFER_STATS_PTR.with(|current| current.set(self.previous_ptr));
                QUERY_IO_CONTEXT_PTR.with(|context| context.set(self.previous_context_ptr));
            });
        }
    }

    let mut context = QueryIoContext {
        total: BufferStats::default(),
    };
    let previous = QUERY_BUFFER_STATS.with(|cell| cell.borrow_mut().replace(stats));
    let previous_ptr = QUERY_BUFFER_STATS_PTR.with(|current| current.replace(ptr));
    let previous_context_ptr =
        QUERY_IO_CONTEXT_PTR.with(|current| current.replace(std::ptr::from_mut(&mut context)));
    let _restore = Restore {
        previous,
        previous_ptr,
        previous_context_ptr,
    };
    f()
}

pub(crate) fn query_buffer_stats_counts(stats: &QueryBufferStats) -> (u64, u64) {
    (
        stats.hits.load(Ordering::Relaxed),
        stats.misses.load(Ordering::Relaxed),
    )
}

pub(crate) fn current_shared_operator_buffer_stats() -> Option<Arc<QueryBufferStats>> {
    SHARED_OPERATOR_BUFFER_STATS.with(|cell| cell.borrow().clone())
}

/// Attribute reads performed by a stream (including its asynchronously
/// spawned worker) to one ANALYZE operator. Nested scopes replace the target,
/// so a parent never double-counts reads already owned by a child node.
pub(crate) fn with_shared_operator_buffer_stats<F, R>(stats: Arc<QueryBufferStats>, f: F) -> R
where
    F: FnOnce() -> R,
{
    let ptr = Arc::as_ptr(&stats);
    if SHARED_OPERATOR_BUFFER_STATS_PTR.with(|current| current.get() == ptr) {
        return f();
    }

    struct Restore {
        previous: Option<Arc<QueryBufferStats>>,
        previous_ptr: *const QueryBufferStats,
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            SHARED_OPERATOR_BUFFER_STATS.with(|cell| {
                *cell.borrow_mut() = self.previous.take();
            });
            SHARED_OPERATOR_BUFFER_STATS_PTR.with(|current| current.set(self.previous_ptr));
        }
    }

    let previous = SHARED_OPERATOR_BUFFER_STATS.with(|cell| cell.borrow_mut().replace(stats));
    let previous_ptr = SHARED_OPERATOR_BUFFER_STATS_PTR.with(|current| current.replace(ptr));
    let _restore = Restore {
        previous,
        previous_ptr,
    };
    f()
}

pub(crate) fn with_shared_operator_buffer_stats_if_present<F, R>(
    stats: Option<Arc<QueryBufferStats>>,
    f: F,
) -> R
where
    F: FnOnce() -> R,
{
    if let Some(stats) = stats {
        with_shared_operator_buffer_stats(stats, f)
    } else {
        f()
    }
}

fn current_query_buffer_stats_counts() -> Option<(u64, u64)> {
    QUERY_BUFFER_STATS_PTR.with(|cell| {
        let stats = cell.get();
        if stats.is_null() {
            None
        } else {
            // SAFETY: `with_query_buffer_stats` owns the Arc for the whole
            // interval in which this thread-local pointer is non-null.
            let (mut hits, mut misses) = query_buffer_stats_counts(unsafe { &*stats });
            QUERY_IO_CONTEXT_PTR.with(|context| {
                let context = context.get();
                if !context.is_null() {
                    let total = unsafe { &*context }.total;
                    hits = hits.saturating_add(total.hits);
                    misses = misses.saturating_add(total.misses);
                }
            });
            Some((hits, misses))
        }
    })
}

fn record_query_buffer_hit() {
    QUERY_IO_CONTEXT_PTR.with(|cell| {
        let context = cell.get();
        if !context.is_null() {
            unsafe { (*context).total.hits = (*context).total.hits.saturating_add(1) };
        }
    });
    SHARED_OPERATOR_BUFFER_STATS_PTR.with(|cell| {
        let stats = cell.get();
        if !stats.is_null() {
            unsafe { &*stats }.hits.fetch_add(1, Ordering::Relaxed);
        }
    });
}

fn record_query_buffer_miss() {
    QUERY_IO_CONTEXT_PTR.with(|cell| {
        let context = cell.get();
        if !context.is_null() {
            unsafe { (*context).total.misses = (*context).total.misses.saturating_add(1) };
        }
    });
    SHARED_OPERATOR_BUFFER_STATS_PTR.with(|cell| {
        let stats = cell.get();
        if !stats.is_null() {
            unsafe { &*stats }.misses.fetch_add(1, Ordering::Relaxed);
        }
    });
}

#[derive(Debug, Clone, Copy)]
struct RuntimeStorageConfig {
    checkpoint_wal_bytes: u64,
    buffer_pool_pages: u64,
    undo_retention: Duration,
    undo_segment_bytes: u64,
    undo_purge_interval: Duration,
    undo_purge_batch_records: usize,
    undo_tablespace_max_bytes: u64,
    undo_mem_soft_limit_bytes: u64,
    undo_mem_cache_limit_bytes: u64,
    undo_txn_max_bytes: u64,
    undo_retention_guarantee: bool,
    /// Group-commit leader wait window, microseconds. `0` = natural batching
    /// (leader fsyncs immediately). Settable via `mydb_group_commit_us`.
    group_commit_us: u64,
    /// Adaptive-window policy toggle. Settable via `mydb_group_commit_adaptive`.
    group_commit_adaptive: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccessIntent {
    Normal,
    Scan,
    /// Sequential SQL scan over a physically small heap. The first pass has
    /// the same cold admission as `Scan`; a second logical pass promotes the
    /// frame, with a deliberately low replacement-priority ceiling.
    SmallScan,
    Prefetch,
    /// Batched non-covering lookup. A newly admitted page stays cold; a
    /// later probe batch that finds it resident promotes it to the hot set.
    Probe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanCachePolicy {
    Small,
    Bulk,
}

impl ScanCachePolicy {
    #[inline]
    fn access_intent(self) -> AccessIntent {
        match self {
            Self::Small => AccessIntent::SmallScan,
            Self::Bulk => AccessIntent::Scan,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FrameIoState {
    Loading = 0,
    Ready = 1,
    Flushing = 2,
    Evicting = 3,
    Discarded = 4,
}

impl FrameIoState {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Loading,
            1 => Self::Ready,
            2 => Self::Flushing,
            3 => Self::Evicting,
            4 => Self::Discarded,
            _ => Self::Discarded,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ActiveSnapshot {
    snapshot_scn: u64,
    started_unix_ms: u64,
}

#[derive(Debug)]
pub struct SnapshotGuard {
    db: Arc<Database>,
    snapshot_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum BufferPageFileKind {
    Heap {
        table_id: u64,
    },
    Index {
        table_id: u64,
        name: String,
    },
    Overflow {
        table_id: u64,
    },
    /// Page-based vector (HNSW) index files (`.hvec`). Kept as its own kind
    /// so graph node pages get an independent buffer-pool soft quota and are
    /// not evicted by the B-tree working set — graph-traversal locality is
    /// the whole point of the paged backend.
    Vector {
        table_id: u64,
        name: String,
    },
    /// Derived summary pages share the pool but never share a cache identity
    /// across file generations, including when a pathname is reused.
    ZoneMap {
        table_id: u64,
        file_generation: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BufferPageFileKey {
    kind: BufferPageFileKind,
    path: PathBuf,
    hash: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PageKey {
    file: Arc<BufferPageFileKey>,
    page_id: u64,
}

impl BufferPageFileKey {
    fn new(kind: BufferPageFileKind, path: PathBuf) -> Self {
        let mut hasher = DefaultHasher::new();
        kind.hash(&mut hasher);
        path.hash(&mut hasher);
        let hash = hasher.finish();
        Self { kind, path, hash }
    }
}

impl Hash for BufferPageFileKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl Hash for PageKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.file.hash);
        self.page_id.hash(state);
    }
}

#[derive(Debug)]
struct Frame {
    key: PageKey,
    raw_memory_charge: Mutex<Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>>,
    bytes: RwLock<Vec<u8>>,
    /// Number of logical SmallScan demands observed for this frame. Unlike a
    /// cache hit counter this starts at zero for Prefetch, so consuming a
    /// prefetched page during the first scan does not masquerade as reuse.
    small_scan_accesses: AtomicU8,
    pin_count: AtomicUsize,
    state: AtomicU8,
    /// Wait queue for this frame's Loading -> Ready/Discarded transition.
    /// Capacity waiters remain on BufferPool::wait_cv; keeping I/O waiters
    /// frame-local prevents one completed pread from waking every thread that
    /// happens to be loading an unrelated page.
    io_wait_lock: Mutex<()>,
    io_wait_cv: Condvar,
    io_waiters: AtomicUsize,
    generation: AtomicU64,
    dirty: AtomicBool,
    dirty_lsn: AtomicU64,
    /// ARIES dirty-page-table recLSN: first WAL LSN that dirtied this frame
    /// since its last successful write-back. `dirty_lsn` remains the latest
    /// LSN for WAL-before-data; fuzzy checkpoint recovery needs this oldest
    /// value so redo never skips an earlier unflushed update on the same page.
    first_dirty_lsn: AtomicU64,
    /// Transaction-local WAL buffering (`MYDB_WAL_TXN_BUFFER`): count of
    /// in-flight transactions that have dirtied this page but have NOT yet
    /// written its WAL record (the record sits in a thread-local buffer to be
    /// appended in one lock acquisition at commit). While this is > 0 the page
    /// must NEVER be flushed — its on-disk image would reference a change with
    /// no durable WAL behind it (WAL-ahead violation). The flush chokepoint
    /// (`flush_frame_with_wal`) and the dirty-victim eviction path both skip
    /// such frames. A refcount, not a flag, so concurrent writers to different
    /// rows of the same page each hold the page un-flushable until *their own*
    /// commit drains and decrements (a single flag would let the first
    /// committer expose a second writer's still-unlogged change). Always 0
    /// unless txn-buffering is enabled.
    deferred_pins: AtomicU32,
    dirty_epoch: AtomicU64,
    /// Raw mutations defer the 16 KiB CRC until a full page image is exposed
    /// or flushed. Resident readers validate structural bounds under the byte
    /// lock and trust the already-validated frame identity.
    checksum_dirty: AtomicBool,
    /// Optional packed numeric working image. Bytes -> packed lock order is
    /// mandatory. State: 0 absent, 1 synchronized, 2 authoritative/dirty,
    /// 3 not packable until the page is replaced or split.
    packed_leaf: parking_lot::Mutex<Option<Box<PackedNumericLeaf>>>,
    packed_leaf_state: AtomicU8,
    /// P0-1 - microseconds (since pool start) at which this frame
    /// finished loading via a prefetch. Reset when promoted out of
    /// prefetch state. 0 means "not loaded via prefetch / already
    /// promoted". Read by `record_prefetch_lead`.
    prefetch_loaded_at_us: AtomicU64,
    /// `dirty_epoch` value at which this frame's index page header
    /// (magic / version / full-page checksum) last validated cleanly.
    /// `u64::MAX` means "not yet validated". The byte-level index
    /// lookups consult this so a buffer-resident leaf page is
    /// checksummed once, not on every probe.
    index_header_validated_epoch: AtomicU64,
    /// Whether the heap image was present (not an all-zero sparse page) when
    /// `heap_header_validated_epoch` was published.  Presence is part of the
    /// validation result: rescanning all 16 KiB on every cache hit turns a
    /// hot sequential scan into an avoidable second memory-bandwidth pass.
    heap_header_present: AtomicBool,
    heap_header_validated_epoch: AtomicU64,
    ref_bit: AtomicBool,
    usage_count: AtomicU8,
    prefetch: AtomicBool,
    /// N-2 - true when this frame was first loaded for a Scan or
    /// Prefetch intent and hasn't yet been touched by a Normal
    /// request. Eviction sweeps prefer these frames so a sequential
    /// scan doesn't push the OLTP working set out of cache.
    scan_origin: AtomicBool,
    /// P0-2 - `now_us()` stamp of this frame's last completed write-back
    /// (the moment a flush cleared its dirty bit). Background pressure
    /// cleaners skip frames that were flushed and re-dirtied within the
    /// cooldown window: re-dirtying that fast identifies a member of the
    /// hot write set, and rewriting it every cleaner round is pure write
    /// amplification — it leaves via eviction or a checkpoint instead.
    /// Zero means "not flushed since load" (never deferred). Eviction,
    /// checkpoint preflush and full flushes ignore the stamp; only the two
    /// background paths consult it, and the panic watermark overrides it.
    flushed_at_us: AtomicU64,
    flush_lock: Mutex<()>,
}

#[derive(Debug, Clone)]
pub(crate) struct BufferPageDiagnostic {
    pub block_id: u64,
    pub storage_id: u64,
    pub page_number: u64,
    pub page_type: &'static str,
    pub index_name: Option<String>,
    pub fix_count: u64,
    pub newest_modification: u64,
    pub oldest_modification: u64,
    pub access_rank: u64,
    pub replacement_position: u64,
    pub page_state: &'static str,
    pub io_fix: &'static str,
    pub is_old: bool,
}

/// One governed raw page allocation that can move between an evicted frame and
/// its replacement without returning the 16 KiB block to the system allocator.
/// The charge follows the allocation, including while it is parked in a shard's
/// recycle list, so managed-resident accounting never under-reports memory.
#[derive(Debug)]
struct RawPageLease {
    bytes: Vec<u8>,
    charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

#[derive(Clone, Debug)]
struct DirtyCandidate {
    first_lsn: u64,
    key: PageKey,
}

impl PartialEq for DirtyCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.first_lsn == other.first_lsn
            && self.key.page_id == other.key.page_id
            && Arc::ptr_eq(&self.key.file, &other.key.file)
    }
}

impl Eq for DirtyCandidate {}

impl PartialOrd for DirtyCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DirtyCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_lsn = if self.first_lsn == 0 {
            u64::MAX
        } else {
            self.first_lsn
        };
        let other_lsn = if other.first_lsn == 0 {
            u64::MAX
        } else {
            other.first_lsn
        };
        self_lsn
            .cmp(&other_lsn)
            .then_with(|| self.key.file.hash.cmp(&other.key.file.hash))
            .then_with(|| self.key.page_id.cmp(&other.key.page_id))
            .then_with(|| {
                (Arc::as_ptr(&self.key.file) as usize).cmp(&(Arc::as_ptr(&other.key.file) as usize))
            })
    }
}

#[derive(Debug, Default)]
struct BufferPoolShard {
    /// M-2 follow-up — page-lookup is the hottest BufferPool path
    /// (every cached hit goes through `lookup_frame`). The map is
    /// now an `RwLock`; readers (lookup, all_frames, frames_for_file,
    /// consume_victim probe, find_clock_victim probe) take a shared
    /// guard, only insert/remove/retain paths take the exclusive
    /// guard. Pattern matches the file_cache conversion in M-2 commit
    /// `2d8c7f5` — read path no longer serialises on a global mutex.
    frames: RwLock<BufferFrameMap>,
    clock: Mutex<VecDeque<PageKey>>,
    /// Append-only weak candidates for O(1)-amortized cold recycling.
    /// Stale entries are intentionally discarded lazily by the evictor.
    cold_candidates: Mutex<VecDeque<std::sync::Weak<Frame>>>,
    /// Fixed-size raw page allocations detached from evicted frames and ready
    /// for immediate reuse by a miss admitted to this shard.
    recycled_raw_pages: Mutex<Vec<RawPageLease>>,
    hand: AtomicUsize,
    /// Independent bounded sampling cursor for pressure writeback.
    cleaner_hand: AtomicUsize,
    // Hit statistics are aggregated at observation time. Keep the hot shard
    // counter separate from its colder clock/eviction metadata, and avoid a
    // second process-wide atomic write on every resident-page access.
    hits: CachePadded<AtomicU64>,
    misses: AtomicU64,
    evictions: AtomicU64,
    /// D-1 - dirty-page registry. Every clean→dirty transition inserts
    /// the page key here; flush (epoch-checked clear), eviction, and
    /// discard remove it. Background flush / checkpoint walk this set —
    /// O(dirty) — instead of cloning every resident frame per round
    /// (O(capacity)). Entries may be stale (frame already clean or
    /// evicted): consumers re-check `frame.dirty` and drop stale keys,
    /// so false positives only cost a lookup, never correctness.
    dirty_set: Mutex<hashbrown::HashSet<PageKey, ahash::RandomState>>,
    /// Min-heap (through Reverse) of clean→dirty transitions ordered by the
    /// page's recovery LSN. Stale candidates are expected and discarded after
    /// checking the live frame epoch/state; this keeps the hot dirtying path
    /// append-only while letting low-water cleaning avoid O(dirty) collection
    /// and sorting every 25ms.
    dirty_oldest: Mutex<BinaryHeap<Reverse<DirtyCandidate>>>,
    /// Monotonic generation for clean->dirty queue insertions. Rebuilders
    /// sample this before collecting authoritative state and verify it while
    /// holding `dirty_oldest`; a changed generation means a concurrent push
    /// occurred and the rebuilt heap must not replace it.
    dirty_oldest_epoch: AtomicU64,
}

#[derive(Debug)]
struct CachedFile {
    file: File,
    #[cfg(target_os = "linux")]
    direct_read: Option<File>,
    #[cfg(target_os = "linux")]
    direct_read_disabled: AtomicBool,
    len: AtomicU64,
    /// P1 - set to true after any pwrite through this handle; consumed
    /// (swap-to-false) by `sync_cached_file_handle` so an fsync skips
    /// files that nothing has written to since the last successful sync.
    /// Initialised to `true` because we don't know the persistence state
    /// of a freshly-opened file (it may have unsynced extending writes
    /// from a prior process) — the first fsync pays the cost once, then
    /// the flag stays accurate.
    needs_sync: AtomicBool,
    /// Serialize barriers so a cleared dirty bit cannot hide an in-flight fsync.
    sync_lock: parking_lot::Mutex<()>,
    /// B3 - second-chance ("clock") bit for the bounded file-handle cache.
    /// Set on every cache hit (a cheap Relaxed store, no timestamp / clock
    /// syscall on the hot IO path) and cleared by the idle-handle sweep
    /// when the cache exceeds `MYDB_BUFFER_POOL_FILE_CACHE_MAX`. A handle
    /// is only closed once its bit is clear AND it has no pending fsync
    /// (`needs_sync == false`), so an evicted handle never drops an
    /// unsynced write obligation.
    recently_used: AtomicBool,
    /// Buffered page bytes written since kernel writeback was last started.
    unstarted_writeback_bytes: AtomicU64,
}

/// Start kernel writeback of a data file after this many buffered page bytes.
/// Without it, cleaner output stays dirty in the page cache until the inode
/// expires (`dirty_expire_centisecs`, 30 s by default) and is then written in
/// one burst that saturates the device and stalls every WAL fsync behind it.
const DATA_FILE_WRITEBACK_AFTER_BYTES: u64 = 1024 * 1024;

impl CachedFile {
    fn new(file: File, len: u64) -> Self {
        Self {
            file,
            #[cfg(target_os = "linux")]
            direct_read: None,
            #[cfg(target_os = "linux")]
            direct_read_disabled: AtomicBool::new(false),
            len: AtomicU64::new(len),
            needs_sync: AtomicBool::new(true),
            sync_lock: parking_lot::Mutex::new(()),
            recently_used: AtomicBool::new(true),
            unstarted_writeback_bytes: AtomicU64::new(0),
        }
    }

    fn len(&self) -> u64 {
        self.len.load(Ordering::Acquire)
    }

    fn refresh_len(&self) -> Result<u64, QueryError> {
        let len = self.file.metadata().map_err(io_err)?.len();
        self.len.store(len, Ordering::Release);
        Ok(len)
    }

    fn update_len_at_least(&self, len: u64) {
        let mut current = self.len.load(Ordering::Acquire);
        while current < len {
            match self
                .len
                .compare_exchange(current, len, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(next) => current = next,
            }
        }
    }

    fn mark_needs_sync(&self) {
        // Release: pwrite (the data write) happens-before this store, so any
        // observer that sees `needs_sync == true` is guaranteed to also see
        // the data in the OS cache.
        self.needs_sync.store(true, Ordering::Release);
    }

    /// Account buffered page bytes and start asynchronous writeback of the
    /// file's dirty pages once enough accumulate, keeping the device busy at
    /// a steady rate instead of in expiry bursts. Durability barriers are
    /// unchanged: this never waits for the I/O and never clears `needs_sync`.
    fn note_buffered_write(&self, bytes: usize) {
        let bytes = bytes as u64;
        let pending = self
            .unstarted_writeback_bytes
            .fetch_add(bytes, Ordering::Relaxed)
            + bytes;
        if pending < DATA_FILE_WRITEBACK_AFTER_BYTES
            || self.unstarted_writeback_bytes.swap(0, Ordering::Relaxed)
                < DATA_FILE_WRITEBACK_AFTER_BYTES
        {
            return;
        }
        // Best effort: a failed hint only leaves pages to the kernel flusher.
        let _ = crate::util::file::start_file_writeback(&self.file, 0, 0);
    }

    /// Returns true if the flag was set (i.e. caller must fsync). Reset to
    /// false atomically so concurrent writers that set it again after this
    /// point are picked up by the next sync.
    fn take_needs_sync(&self) -> bool {
        self.needs_sync.swap(false, Ordering::AcqRel)
    }
}

struct BulkFlushRange {
    file: Arc<BufferPageFileKey>,
    generation: u64,
    first: u64,
    last: u64,
}

struct BufferPool {
    capacity_pages: AtomicU64,
    /// Percentage of the buffer pool used to classify a physical SQL scan as
    /// SmallScan. Zero disables the policy. Runtime-settable.
    small_scan_pct: AtomicU64,
    /// P0-1 cache-line - high-frequency RMW from every pin/unpin
    /// (`fetch_add`/`fetch_sub` 13 sites total). Padded to its own
    /// 128-byte cache line so concurrent pin/unpin from worker threads
    /// no longer ping-pongs the line back and forth against the
    /// neighbouring `evictions`/`flushes`/`dirty_pages` counters.
    used_pages: CachePadded<AtomicU64>,
    evictions: AtomicU64,
    flushes: AtomicU64,
    /// Logical writes applied to resident page images. Unlike `flushes`, this
    /// counts buffer-pool write requests before physical page writeback.
    write_requests: AtomicU64,
    flush_errors: AtomicU64,
    background_io_errors: AtomicU64,
    /// P0-1 cache-line - 11 RMW sites driven by page dirty/clean
    /// transitions and the dirty-flusher worker. Padded for the same
    /// reason as `used_pages`.
    dirty_pages: CachePadded<AtomicU64>,
    waits: AtomicU64,
    wait_ms: AtomicU64,
    background_flushes: AtomicU64,
    /// LSN of the most recently completed checkpoint, mirrored from
    /// `Database::last_checkpoint_lsn_atomic` via
    /// `set_last_checkpoint_lsn`. The heap-side first-update-FPI
    /// predicate compares the page's `page_lsn` against this value:
    /// a page whose `page_lsn` already exceeds the checkpoint LSN has
    /// been WAL-touched since the checkpoint and does not need
    /// another FPI in this cycle. Without this mirror the FPI
    /// predicate falls back to "is the buffer-pool frame currently
    /// dirty?", which the background flusher invalidates as soon as
    /// it cleans the frame — so every subsequent dirtying in the same
    /// checkpoint window would re-emit an FPI, blowing up WAL volume
    /// on contended OLTP pages (measured -36 % tpmC on TPC-C
    /// 4w20t/1m before this gate landed).
    last_checkpoint_lsn: AtomicU64,
    /// Dependent page redo and dirty registration form one publication
    /// window. Checkpoints freeze this short window, not whole metadata scans.
    pub(crate) page_wal_publish_gate: parking_lot::RwLock<()>,
    /// Miss accounting is independent of the per-shard hit counters.
    misses: CachePadded<AtomicU64>,
    read_ios: AtomicU64,
    /// Successful page reads issued by the pool, including scan bypass reads.
    /// These may hit the OS page cache; they are not physical disk IO counts.
    kind_read_ios: [AtomicU64; BUFFER_PAGE_KIND_COUNT],
    /// Physical read operations. `read_ios` remains a page counter for
    /// compatibility; one batch may account many pages but one syscall.
    read_syscalls: AtomicU64,
    batch_read_ops: AtomicU64,
    batch_read_pages: AtomicU64,
    write_ios: AtomicU64,
    /// Physical write syscalls. `write_ios` remains a page counter; one
    /// contiguous cleaner write may account many pages but one syscall.
    write_syscalls: AtomicU64,
    prefetches: AtomicU64,
    prefetch_completed: AtomicU64,
    prefetch_hits: AtomicU64,
    prefetch_wasted: AtomicU64,
    prefetch_demand_waits: AtomicU64,
    eviction_scans: AtomicU64,
    eviction_hand: AtomicUsize,
    /// B4 - shared round-robin cursor over shards for the >high-water
    /// background flush. Every IO worker pulls its next shard to flush from
    /// this single atomic instead of statically owning `shard_idx % stride`,
    /// so a worker whose static stripe happened to be cold no longer idles
    /// while a peer drains a hot stripe alone. Fast workers claim more
    /// shards; the collect stays O(dirty-in-shard) per claim (no whole-pool
    /// rebuild).
    flush_cursor: AtomicUsize,
    shards: Vec<BufferPoolShard>,
    file_generations: RwLock<BufferFileGenerationMap>,
    /// Allocates non-repeating generation values when a file-generation map
    /// entry is removed and later recreated, preventing ABA matches with an
    /// in-flight stale frame.
    next_file_generation: AtomicU64,
    /// M-2 - swapped from `Mutex<HashMap>` to `RwLock<HashMap>` so
    /// the common case (handle already cached) goes through a read
    /// lock instead of serialising every page IO on a global mutex.
    /// Only first-touch misses take the write lock (with a
    /// double-check to avoid duplicate inserts on the race).
    file_cache: RwLock<BufferFileCacheMap>,
    log: Arc<crate::wal::LogMutex>,
    durability: DurabilityMode,
    lock_wait: Duration,
    io_threads: usize,
    /// Queued or running MRR helpers; bounded by the existing I/O degree.
    ordered_read_helpers: AtomicUsize,
    /// Configured base prefetch window — the construction-time value
    /// surfaced as `buffer_pool_prefetch_pages` in SHOW ENGINE MYDB
    /// STATUS. N-1 introduces an adaptive window on top of this; see
    /// `effective_prefetch_pages`. CFG-1 makes it runtime-settable
    /// (`SET GLOBAL mydb_buffer_pool_prefetch_pages`), hence atomic; a
    /// write also reseeds `effective_prefetch_pages` so the new base
    /// takes effect on the next scan.
    prefetch_pages: AtomicUsize,
    /// N-1 - adaptive prefetch window. Starts at `prefetch_pages`,
    /// floats inside `[MIN_BUFFER_POOL_PREFETCH_PAGES,
    /// MAX_BUFFER_POOL_PREFETCH_PAGES]` based on the EMA of recent
    /// prefetch-hit ratio. Consumers (heap/index scan) read this
    /// instead of `prefetch_pages` so the window can react to load.
    effective_prefetch_pages: AtomicUsize,
    /// EMA of prefetch_hits / prefetches in 0..=10_000 (basis points).
    /// Refreshed by `maybe_adapt_prefetch` once per window.
    prefetch_hit_ema_bp: AtomicU64,
    /// Prefetch count seen at last EMA update; the gap drives the
    /// next refresh.
    prefetch_adapt_last_prefetches: AtomicU64,
    prefetch_adapt_last_prefetch_hits: AtomicU64,
    prefetch_adapt_last_wasted: AtomicU64,
    prefetch_adapt_last_queue_drops: AtomicU64,
    /// Demand waits seen at the last adaptive update. A high hit ratio alone
    /// means existing read-ahead is useful, not that a wider window is needed.
    prefetch_adapt_last_demand_waits: AtomicU64,
    wait_lock: Mutex<()>,
    wait_cv: Condvar,
    frame_waiters: AtomicUsize,
    // P0-1 - prefetch queue records enqueue time so we can break out
    // "queue wait" (issued -> drained by IO worker) from "lead time"
    // (loaded -> first hit). The instant is encoded as microseconds
    // since `pool_started`, kept as u64 for cheap atomic transport.
    prefetch_queue: Mutex<VecDeque<(PageKey, u64)>>,
    // Bounded scheduling hints only. Dirty frames and their recovery LSNs
    // remain authoritative if a range is evicted, replaced or interrupted.
    bulk_flush_ranges: Mutex<[Option<BulkFlushRange>; BUFFER_POOL_BULK_FLUSH_RANGES]>,
    bulk_flush_cursor: AtomicUsize,
    /// Admission set spanning queued and currently executing requests.
    prefetch_queued_or_inflight: Vec<Mutex<HashSet<PageKey>>>,
    shutdown: AtomicBool,
    flusher: Mutex<Vec<thread::JoinHandle<()>>>,
    // P0-1 - per-intent read counts. The aggregate `read_ios` still
    // sums these, so existing dashboards keep working; the per-intent
    // split surfaces full-scan / prefetch IO that hides behind a high
    // global hit rate.
    read_ios_normal: AtomicU64,
    read_ios_scan: AtomicU64,
    read_ios_prefetch: AtomicU64,
    small_scan_accesses: AtomicU64,
    small_scan_cold_accesses: AtomicU64,
    small_scan_promotions: AtomicU64,
    small_scan_read_ios: AtomicU64,
    // P0-1 - prefetch backpressure / lead-time accounting.
    prefetch_queue_drops: AtomicU64,
    prefetch_duplicate_drops: AtomicU64,
    prefetch_pressure_drops: AtomicU64,
    prefetch_queue_full_drops: AtomicU64,
    prefetch_stale_drops: AtomicU64,
    prefetch_queue_wait_us_total: AtomicU64,
    prefetch_queue_drains: AtomicU64,
    prefetch_lead_us_total: AtomicU64,
    prefetch_lead_samples: AtomicU64,
    // P0-1 - cumulative time the foreground spent blocked in
    // ensure_capacity / ensure_capacity_for_insert waiting for a
    // victim. Reported in microseconds to keep millisecond-scale
    // P99s visible (the legacy `wait_ms` rounds them down to 0).
    ensure_capacity_busy_us: AtomicU64,
    // P0-1 - ns spent acquiring the file_cache lock during
    // open_cached_file. Surfaces M-2's symptom before it lands.
    file_handle_wait_ns: AtomicU64,
    // P0-1 - dirty-ratio sample histogram. Bucket i covers
    // (i*12.5 .. (i+1)*12.5)% of capacity. Background worker
    // samples once per loop iteration.
    dirty_ratio_histogram: [AtomicU64; 8],
    dirty_ratio_samples: AtomicU64,
    /// N-2 - number of resident frames whose origin is Scan or
    /// Prefetch and that have not yet been promoted via a Normal
    /// access. Decremented when the flag is cleared (promotion) or
    /// the frame is removed (eviction / discard).
    scan_origin_count: AtomicU64,
    prefetch_gate_last_scan_reads: AtomicU64,
    prefetch_gate_last_normal_reads: AtomicU64,
    prefetch_gate_mixed_reads: AtomicBool,
    /// N-2 - count of evictions that hit a scan-origin frame via
    /// the preferred-victim fast path. A high value relative to
    /// total evictions means N-2 is doing its job protecting the
    /// working set.
    scan_origin_evictions: AtomicU64,
    cold_recycle_hits: AtomicU64,
    cold_recycle_fallbacks: AtomicU64,
    raw_scan_pages: AtomicU64,
    raw_page_allocations: AtomicU64,
    raw_page_reuse_hits: AtomicU64,
    raw_page_reuse_misses: AtomicU64,
    recycled_raw_page_count: AtomicU64,
    /// P1 - count of file fsyncs that actually ran in
    /// `sync_cached_file_handle` (file had pending writes).
    file_syncs: AtomicU64,
    /// P1 - count of file fsyncs short-circuited by the needs_sync flag
    /// (file was clean since the last successful sync). Big positive
    /// gap between this and `file_syncs` means the dirty-bit fast path
    /// is paying off.
    file_sync_skipped: AtomicU64,
    /// P0 - count of `ALTER SYSTEM FLUSH DATA_CACHE` calls that took
    /// the early-out (no resident pages → no work to do).
    flush_data_cache_short_circuits: AtomicU64,
    /// Cumulative resident pages evicted and foreground microseconds spent by
    /// explicit/internal full data-cache flushes.  These are deliberately
    /// separate from checkpoint metrics: an index build can spend substantial
    /// time discarding a large clean cache without writing a dirty page.
    flush_data_cache_pages: AtomicU64,
    flush_data_cache_us: AtomicU64,
    /// D-2 - set by the foreground when an eviction pass finds no clean
    /// victim; consumed by the IO workers, which respond with a headroom
    /// flush so the foreground can evict without paying a synchronous
    /// page write + WAL fsync itself.
    capacity_pressure: AtomicBool,
    /// D-2 - foreground evictions that fell back to a synchronous dirty
    /// flush after the grace window expired (workers couldn't supply a
    /// clean victim in time) + the cumulative microseconds those
    /// synchronous flushes took. The pair is the direct measure of
    /// eviction-induced foreground latency.
    evict_sync_dirty_flushes: AtomicU64,
    evict_sync_dirty_flush_us: AtomicU64,
    /// P0-2 - dirty frames the background pressure cleaners skipped because
    /// they had been flushed and re-dirtied within the cooldown window
    /// (hot-write-set members; leaving via eviction instead of another
    /// cleaner write-back).
    cooldown_deferred_flushes: AtomicU64,
    /// D-2 - worker flush rounds triggered by capacity pressure (as
    /// opposed to the dirty-ratio watermark).
    headroom_flushes: AtomicU64,
    /// D-3 - pages written and wall-clock microseconds of the most
    /// recent full dirty flush (checkpoint / FLUSH DATA_CACHE).
    checkpoint_flush_pages: AtomicU64,
    checkpoint_flush_us: AtomicU64,
    /// P5 - pages/time spent by automatic checkpoint preflush. This work runs
    /// before the checkpoint mutex is taken and is intentionally best-effort;
    /// the following `flush_all` remains the correctness barrier.
    checkpoint_preflush_pages: AtomicU64,
    checkpoint_preflush_us: AtomicU64,
    /// D-1 - peak prefetch queue depth since process start.
    prefetch_queue_depth_peak: AtomicU64,
    /// D-4 - IO workers park on their own condvar so a foreground unpin
    /// (which must wake capacity waiters on `wait_cv`) no longer drags
    /// every IO thread out of bed at the same time.
    worker_lock: Mutex<()>,
    worker_cv: Condvar,
    /// Q-1 - resident page counts split by file kind
    /// (heap / index / overflow-LOB), maintained alongside `used_pages`.
    /// Drives the per-kind soft-quota eviction bias and the
    /// `buffer_pool_pages_{heap,index,lob}` status lines.
    /// P0-1 cache-line - per-kind resident page counts; bumped on every
    /// fix and decremented on every evict / drop. Each element gets its
    /// own cache line so heap / index / overflow IO threads don't
    /// invalidate each other's counter just because the array fits in
    /// 24 B (three packed AtomicU64s landed on the same line).
    kind_used_pages: [CachePadded<AtomicU64>; BUFFER_PAGE_KIND_COUNT],
    /// Q-1 - per-kind soft quota in percent of `capacity_pages`;
    /// 0 = unlimited. When a kind sits above its quota AND other kinds
    /// hold pages, `find_clock_victim` takes that kind's frames without
    /// the ref_bit / usage_count grace (the scan-origin treatment), so
    /// e.g. a LOB streaming flood stops evicting the index working set.
    /// Defaults: heap 0, index 0, LOB 30 — overridable via
    /// `MYDB_BUFFER_POOL_QUOTA_{HEAP,INDEX,LOB}_PCT`.
    kind_quota_pct: [AtomicU64; BUFFER_PAGE_KIND_COUNT],
    /// Q-1 - evictions taken via the over-quota preference.
    quota_evictions: AtomicU64,
    pool_started: Instant,
}

/// Number of physical page categories sharing the buffer pool.
pub(crate) const BUFFER_PAGE_KIND_COUNT: usize = 5;

impl BufferPageFileKind {
    /// Dense index for per-kind counters.
    pub(crate) fn slot(&self) -> usize {
        match self {
            BufferPageFileKind::Heap { .. } => 0,
            BufferPageFileKind::Index { .. } => 1,
            BufferPageFileKind::Overflow { .. } => 2,
            BufferPageFileKind::Vector { .. } => 3,
            BufferPageFileKind::ZoneMap { .. } => 4,
        }
    }
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferPool")
            .field(
                "capacity_pages",
                &self.capacity_pages.load(Ordering::Relaxed),
            )
            .field("used_pages", &self.used_pages.load(Ordering::Relaxed))
            .field("shards", &self.shards.len())
            .finish()
    }
}

#[derive(Debug)]
struct PinnedFrame<P: std::ops::Deref<Target = BufferPool> = Arc<BufferPool>> {
    pool: P,
    frame: Arc<Frame>,
    intent: AccessIntent,
}

#[derive(Debug, Clone, Default)]
struct BufferPoolSnapshot {
    capacity_pages: u64,
    used_pages: u64,
    dirty_pages: u64,
    freeable_pages: u64,
    evictions: u64,
    flushes: u64,
    write_requests: u64,
    flush_errors: u64,
    background_io_errors: u64,
    pinned_pages: u64,
    waits: u64,
    wait_ms: u64,
    background_flushes: u64,
    shards: u64,
    read_ios: u64,
    kind_read_ios: [u64; BUFFER_PAGE_KIND_COUNT],
    read_syscalls: u64,
    batch_read_ops: u64,
    batch_read_pages: u64,
    write_ios: u64,
    write_syscalls: u64,
    prefetches: u64,
    prefetch_hits: u64,
    prefetch_wasted: u64,
    prefetch_demand_waits: u64,
    eviction_scans: u64,
    io_threads: u64,
    prefetch_pages: u64,
    small_scan_pct: u64,
    small_scan_threshold_pages: u64,
    small_scan_accesses: u64,
    small_scan_cold_accesses: u64,
    small_scan_promotions: u64,
    small_scan_read_ios: u64,
    // P0-1
    read_ios_normal: u64,
    read_ios_scan: u64,
    read_ios_prefetch: u64,
    prefetch_queue_drops: u64,
    prefetch_duplicate_drops: u64,
    prefetch_pressure_drops: u64,
    prefetch_queue_full_drops: u64,
    prefetch_stale_drops: u64,
    raw_page_allocations: u64,
    raw_page_reuse_hits: u64,
    raw_page_reuse_misses: u64,
    recycled_raw_pages: u64,
    prefetch_queue_wait_us_total: u64,
    prefetch_queue_drains: u64,
    prefetch_lead_us_total: u64,
    prefetch_lead_samples: u64,
    ensure_capacity_busy_us: u64,
    file_handle_wait_ns: u64,
    dirty_ratio_histogram: [u64; 8],
    dirty_ratio_samples: u64,
    spill_bytes_read: u64,
    spill_bytes_written: u64,
    /// Sum across shards of `(max - min) / max * 100` rounded — 0 means
    /// perfectly balanced, 100 means one shard does everything. Computed
    /// over `(hits + misses)`.
    shard_imbalance_pct: u64,
    shard_max_hits: u64,
    shard_min_hits: u64,
    /// N-2 - share of resident frames currently flagged as
    /// scan/prefetch origin (i.e. not yet promoted into the working
    /// set). Reported as percentage of capacity.
    scan_origin_share_pct: u64,
    scan_origin_evictions: u64,
    cold_recycle_hits: u64,
    cold_recycle_fallbacks: u64,
    raw_scan_pages: u64,
    /// Q-1 - resident pages by file kind (heap / index / overflow-LOB)
    /// and the number of victim selections taken via the over-quota
    /// preference.
    kind_used_pages: [u64; BUFFER_PAGE_KIND_COUNT],
    quota_evictions: u64,
    /// P1 - actual file fsyncs run by `sync_cached_file_handle`.
    file_syncs: u64,
    /// P1 - file fsyncs short-circuited by the needs_sync fast path.
    file_sync_skipped: u64,
    /// P0 - `ALTER SYSTEM FLUSH DATA_CACHE` calls that hit the early-out.
    flush_data_cache_short_circuits: u64,
    /// Resident pages evicted and cumulative wall time spent by full
    /// data-cache flushes, including internal pressure-driven calls.
    flush_data_cache_pages: u64,
    flush_data_cache_us: u64,
    /// D-2 - foreground evictions that had to flush a dirty victim
    /// synchronously (grace window expired) + their cumulative cost.
    evict_sync_dirty_flushes: u64,
    evict_sync_dirty_flush_us: u64,
    /// P0-2 - dirty frames deferred by the background cleaners' write-back
    /// cooldown (flushed and re-dirtied within the window).
    cooldown_deferred_flushes: u64,
    /// D-2 - worker flush rounds triggered by capacity pressure.
    headroom_flushes: u64,
    /// D-3 - size + duration of the most recent full dirty flush.
    checkpoint_flush_pages: u64,
    checkpoint_flush_us: u64,
    checkpoint_preflush_pages: u64,
    checkpoint_preflush_us: u64,
    /// D-1 - peak prefetch queue depth since start.
    prefetch_queue_depth_peak: u64,
    /// D-1 - smallest dirty-page LSN currently registered (0 = none).
    /// `current LSN - oldest_dirty_lsn` is the redo distance a crash
    /// would have to replay; a stuck value means a page is pinned dirty.
    oldest_dirty_lsn: u64,
    /// PERF-5 - dirty page counts by kind, computed in the SAME single
    /// `collect_dirty_frames` pass as `oldest_dirty_lsn` so a status
    /// snapshot walks the dirty registry once instead of three times.
    dirty_heap_pages: u64,
    dirty_index_pages: u64,
    /// B1 - total entries across every shard's `dirty_oldest` min-heap.
    /// Bounded by the lazy-compaction invariant to `~2 ×` live dirty
    /// pages; a value that instead tracks the cumulative write count would
    /// mean the compaction regressed (the pre-fix unbounded-growth leak).
    dirty_oldest_len: u64,
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub durability: DurabilityMode,
    pub checkpoint_interval: Duration,
    pub checkpoint_wal_bytes: u64,
    pub lock_wait: Duration,
    pub group_commit_window: Duration,
    pub wal_segment_bytes: u64,
    pub buffer_pool_pages: u64,
    pub buffer_pool_io_threads: usize,
    pub buffer_pool_prefetch_pages: usize,
    pub undo_retention: Duration,
    pub heap_update_reserve_bytes: usize,
    pub startup_validation: StartupValidationMode,
    pub startup_validate_indexes: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupValidationMode {
    Auto,
    Full,
    Off,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartupValidationPath {
    CleanFast,
    Full,
    Off,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageStatus {
    pub durability: &'static str,
    pub current_lsn: u64,
    pub durable_lsn: u64,
    pub wal_bytes: u64,
    pub wal_logical_records: u64,
    pub wal_physical_write_calls: u64,
    pub wal_physical_write_bytes: u64,
    pub wal_page_fallback_records: u64,
    pub wal_index_batch_records: u64,
    pub wal_heap_payload_bytes: u64,
    pub wal_index_payload_bytes: u64,
    pub wal_page_payload_bytes: u64,
    pub wal_heap_fpi_payload_bytes: u64,
    pub wal_index_page_payload_bytes: u64,
    pub wal_other_page_payload_bytes: u64,
    pub reduced_redo_transactions: u64,
    pub reduced_redo_skipped_records: u64,
    pub reduced_redo_skipped_payload_bytes: u64,
    pub reduced_redo_fallback_records: u64,
    pub reduced_redo_force_flushes: u64,
    pub reduced_redo_force_flush_us: u64,
    pub wal_sync_total_us: u64,
    pub wal_sync_avg_us: u64,
    pub wal_sync_max_us: u64,
    pub wal_rotation_count: u64,
    pub wal_rotation_critical_total_us: u64,
    pub wal_rotation_critical_max_us: u64,
    pub wal_pending_sealed_sync: u64,
    pub checkpoint_lsn: u64,
    pub checkpoint_age_lsn: u64,
    pub checkpoints_total: u64,
    pub checkpoint_failures_total: u64,
    pub checkpoint_preflush_failures_total: u64,
    pub checkpoint_redo_lsn: u64,
    pub checkpoint_redo_age_lsn: u64,
    pub last_checkpoint_dirty_pages: u64,
    pub last_checkpoint_wal_reclaimed_bytes: u64,
    pub checkpoint_wal_reclaimed_bytes_total: u64,
    pub restartpoints_total: u64,
    pub replica_applied_lsn: u64,
    pub last_checkpoint_trigger: &'static str,
    pub last_checkpoint_kind: &'static str,
    pub last_checkpoint_snapshot_us: u64,
    pub last_checkpoint_flush_us: u64,
    pub last_checkpoint_sync_us: u64,
    pub last_checkpoint_compact_us: u64,
    pub last_checkpoint_duration_us: u64,
    pub checkpoint_duration_total_us: u64,
    pub checkpoint_duration_max_us: u64,
    pub buffer_hits: u64,
    pub buffer_misses: u64,
    pub buffer_hit_pct: u64,
    pub dirty_heap_pages: usize,
    pub dirty_index_pages: usize,
    pub buffer_pool_used_pages: u64,
    pub buffer_pool_dirty_pages: u64,
    pub buffer_pool_freeable_pages: u64,
    pub buffer_pool_evictions: u64,
    pub buffer_pool_flushes: u64,
    pub buffer_pool_write_requests: u64,
    pub buffer_pool_flush_errors: u64,
    pub buffer_pool_background_io_errors: u64,
    pub buffer_pool_pinned_pages: u64,
    pub buffer_pool_waits: u64,
    pub buffer_pool_wait_ms: u64,
    pub buffer_pool_background_flushes: u64,
    pub buffer_pool_shards: u64,
    pub buffer_pool_read_ios: u64,
    pub buffer_pool_read_ios_by_kind: [u64; BUFFER_PAGE_KIND_COUNT],
    pub buffer_pool_read_syscalls: u64,
    pub buffer_pool_batch_read_ops: u64,
    pub buffer_pool_batch_read_pages: u64,
    pub buffer_pool_write_ios: u64,
    pub buffer_pool_write_syscalls: u64,
    pub buffer_pool_prefetches: u64,
    pub buffer_pool_prefetch_hits: u64,
    pub buffer_pool_prefetch_wasted: u64,
    pub buffer_pool_prefetch_demand_waits: u64,
    pub buffer_pool_eviction_scans: u64,
    pub buffer_pool_io_threads: u64,
    pub buffer_pool_prefetch_pages: u64,
    pub buffer_pool_small_scan_pct: u64,
    pub buffer_pool_small_scan_threshold_pages: u64,
    pub buffer_pool_small_scan_accesses: u64,
    pub buffer_pool_small_scan_cold_accesses: u64,
    pub buffer_pool_small_scan_promotions: u64,
    pub buffer_pool_small_scan_read_ios: u64,
    pub active_transactions: usize,
    pub oldest_snapshot_scn: u64,
    pub oldest_active_txn_ms: u64,
    pub latch_waits: u64,
    pub lock_waits: u64,
    pub active_lock_waits: usize,
    pub oldest_lock_wait_ms: u64,
    pub deadlocks: u64,
    /// Configured lock wait timeout, retained for SHOW ENGINE MYDB STATUS.
    pub lock_wait_ms: u64,
    pub lock_wait_time_ms: u64,
    pub lock_wait_time_avg_ms: u64,
    pub lock_wait_time_max_ms: u64,
    pub group_commit_us: u64,
    pub group_commit_adaptive: bool,
    pub group_commit_batches: u64,
    pub group_commit_waiters: u64,
    pub avg_group_commit_size: u64,
    pub group_commit_leader_wait_total_us: u64,
    pub group_commit_follower_wait_total_us: u64,
    pub group_commit_sync_op_total_us: u64,
    pub group_commit_leader_wait_max_us: u64,
    pub group_commit_follower_wait_max_us: u64,
    pub group_commit_sync_op_max_us: u64,
    /// M1/O1 — waiters released for free by a leader's flushed high-water
    /// (fsyncs saved by the natural-batch high-water optimization).
    pub group_commit_high_water_released: u64,
    /// M4 — EMA of recent batch sizes (adaptive-window input), ×100 to stay
    /// integer (EngineStatus derives Eq). Render as `v/100 . v%100`.
    pub group_commit_recent_avg_batch_size_x100: u64,
    /// M4 — approximate p50/p99 realized batch size (from histogram buckets).
    pub group_commit_batch_p50: u64,
    pub group_commit_batch_p99: u64,
    pub durability_worker_enabled: bool,
    pub durability_queue_depth: u64,
    pub durability_queue_depth_peak: u64,
    pub durability_batches: u64,
    pub durability_requests: u64,
    pub durability_batch_max: u64,
    pub wal_syncs: u64,
    pub wal_segment_bytes: u64,
    pub buffer_pool_pages: u64,
    pub row_migrations: u64,
    pub undo_bytes: u64,
    pub undo_records: usize,
    pub undo_retention_ms: u64,
    pub temporal_earliest_available_unix_us: u64,
    pub temporal_commit_ledger_bytes: u64,
    pub temporal_history_index_bytes: u64,
    pub temporal_history_index_entries: usize,
    pub temporal_history_compactions: u64,
    pub temporal_history_last_compaction_unix_us: u64,
    pub temporal_queries: u64,
    pub temporal_history_index_hits: u64,
    pub temporal_heap_fallbacks: u64,
    pub temporal_hlc_rollbacks: u64,
    pub undo_purged_records: u64,
    pub plan_cache_hits: u64,
    pub plan_cache_misses: u64,
    pub plan_cache_hit_pct: u64,
    pub select_parse_cache_hits: u64,
    pub select_compiled_plan_hits: u64,
    pub select_operator_compiles: u64,
    pub prepared_plan_reuses: u64,
    pub prepared_plan_compiles: u64,
    pub prepared_dml_reuses: u64,
    pub prepared_dml_compiles: u64,
    pub checkpoint_interval_ms: u64,
    pub checkpoint_wal_bytes: u64,
    pub startup_validate_indexes: bool,
    pub last_checkpoint_unix_ms: Option<u64>,
    pub last_recovery_summary: String,
    // M1.5 - Extended observability fields
    pub snapshot_too_old_count: u64,
    pub read_only_txn_count: u64,
    pub min_retained_txn_id: u64,
    /// Phase 4 — PAGE_MAX_TRX_ID visibility shortcut telemetry
    /// (`doc_old/design/index-page-max-trx-2026-07.md`). Each covering
    /// scan chunk that proves visibility via a leaf's page-level
    /// upper-bound writer id bumps `hits`; every chunk that had to
    /// fall back to the heap-header path bumps `misses`. A
    /// growing hit ratio on a workload with no ANALYZE is the
    /// clearest signal that the shortcut is doing its job.
    pub index_page_max_trx_shortcut_hits: u64,
    pub index_page_max_trx_shortcut_misses: u64,
    pub undo_peak_bytes: u64,
    pub undo_peak_records: u64,
    pub undo_longest_chain: u32,
    pub undo_retention_pressure: u64,
    pub undo_segment_count: u64,
    pub undo_segment_bytes: u64,
    pub undo_disk_bytes: u64,
    pub undo_reclaimable_bytes: u64,
    pub undo_reclaimed_bytes: u64,
    pub undo_tablespace_max_bytes: u64,
    pub undo_mem_soft_limit_bytes: u64,
    /// Phase 2/4 — resident undo payload cache cap (0 = unbounded).
    pub undo_mem_cache_limit_bytes: u64,
    /// Phase 2/4 — undo payload bytes currently resident in memory.
    pub undo_resident_bytes: u64,
    /// Phase 2/4 — cumulative payload evictions to disk.
    pub undo_evicted_records: u64,
    /// Phase 4 — per-transaction undo byte cap (0 = unlimited).
    pub undo_txn_max_bytes: u64,
    /// Phase 4 — transaction currently holding the most undo bytes
    /// (the "who is blocking purge" diagnostic).
    pub undo_largest_txn_id: u64,
    pub undo_largest_txn_bytes: u64,
    pub undo_mem_pressure: u64,
    pub undo_retention_guarantee: bool,
    pub undo_purge_interval_ms: u64,
    pub undo_purge_batch_records: u64,
    pub undo_purge_runs: u64,
    pub undo_purge_errors: u64,
    pub undo_purge_last_unix_ms: u64,
    pub undo_cache_hits: u64,
    pub undo_cache_misses: u64,
    pub lock_wait_histogram: [u64; 8],
    pub row_x_lock_acquires: u64,
    pub row_s_lock_acquires: u64,
    pub lock_upgrades: u64,
    pub lock_upgrade_failures: u64,
    pub table_lock_acquires: u64,
    pub table_lock_waits: u64,
    pub chunked_release_chunks: u64,
    pub wait_event_summary: Vec<(String, u64, u64, u64)>, // (event, count, total_us, max_us)
    // M4.1/M4.2 - WAL segment + archive observability
    pub wal_segment_count: u64,
    pub wal_active_segment_id: u64,
    pub wal_archived_segment_count: u64,
    pub wal_archive_enabled: bool,
    // P0-1 - extended buffer pool observability for the buffer
    // optimization roadmap. Surfaced via SHOW ENGINE MYDB STATUS as
    // buffer_pool_<name>.
    pub buffer_pool_read_ios_normal: u64,
    pub buffer_pool_read_ios_scan: u64,
    pub buffer_pool_read_ios_prefetch: u64,
    pub buffer_pool_prefetch_queue_drops: u64,
    pub buffer_pool_prefetch_duplicate_drops: u64,
    pub buffer_pool_prefetch_pressure_drops: u64,
    pub buffer_pool_prefetch_queue_full_drops: u64,
    pub buffer_pool_prefetch_stale_drops: u64,
    pub buffer_pool_prefetch_queue_wait_avg_us: u64,
    pub buffer_pool_prefetch_lead_avg_us: u64,
    pub buffer_pool_ensure_capacity_busy_us: u64,
    pub buffer_pool_file_handle_wait_ns: u64,
    pub buffer_pool_dirty_ratio_histogram: [u64; 8],
    pub buffer_pool_dirty_ratio_samples: u64,
    pub buffer_pool_shard_imbalance_pct: u64,
    pub buffer_pool_shard_max_hits: u64,
    pub buffer_pool_shard_min_hits: u64,
    pub buffer_pool_scan_origin_share_pct: u64,
    pub buffer_pool_scan_origin_evictions: u64,
    pub buffer_pool_cold_recycle_hits: u64,
    pub buffer_pool_cold_recycle_fallbacks: u64,
    pub buffer_pool_raw_scan_pages: u64,
    pub buffer_pool_raw_page_allocations: u64,
    pub buffer_pool_raw_page_reuse_hits: u64,
    pub buffer_pool_raw_page_reuse_misses: u64,
    pub buffer_pool_recycled_raw_pages: u64,
    // Q-1 - per-kind residency + quota-driven evictions.
    pub buffer_pool_pages_heap: u64,
    pub buffer_pool_pages_index: u64,
    pub buffer_pool_pages_lob: u64,
    pub buffer_pool_pages_vector: u64,
    pub buffer_pool_pages_zone_map: u64,
    pub buffer_pool_quota_evictions: u64,
    pub buffer_pool_file_syncs: u64,
    pub buffer_pool_file_sync_skipped: u64,
    pub buffer_pool_flush_data_cache_short_circuits: u64,
    pub buffer_pool_flush_data_cache_pages: u64,
    pub buffer_pool_flush_data_cache_us: u64,
    // D-1..D-4 - dirty-page registry / eviction-decoupling metrics.
    pub buffer_pool_evict_sync_dirty_flushes: u64,
    pub buffer_pool_evict_sync_dirty_flush_us: u64,
    pub buffer_pool_cooldown_deferred_flushes: u64,
    pub buffer_pool_headroom_flushes: u64,
    pub buffer_pool_checkpoint_flush_pages: u64,
    pub buffer_pool_checkpoint_flush_us: u64,
    pub buffer_pool_checkpoint_preflush_pages: u64,
    pub buffer_pool_checkpoint_preflush_us: u64,
    pub buffer_pool_prefetch_queue_depth_peak: u64,
    pub buffer_pool_oldest_dirty_lsn: u64,
    pub bulk_index_direct_builds: u64,
    pub bulk_index_direct_pages: u64,
    pub bulk_index_direct_us: u64,
    pub bulk_index_buffered_builds: u64,
    pub bulk_index_buffered_pages: u64,
    pub bulk_index_buffered_us: u64,
    pub index_build_requested_workers: u64,
    pub index_build_effective_workers: u64,
    pub index_build_parallel_builds: u64,
    pub index_build_serial_builds: u64,
    pub index_build_degree_downgrades: u64,
    pub online_index_prepare_us: u64,
    pub online_index_replay_us: u64,
    pub online_index_validation_us: u64,
    pub online_index_publish_us: u64,
    pub online_index_barrier_wait_us: u64,
    pub online_index_tail_entries_peak: u64,
    pub online_index_tail_bytes_peak: u64,
    pub online_index_log_bytes_peak: u64,
    pub index_build_scan_us: u64,
    pub index_build_sort_us: u64,
    pub index_build_leaf_us: u64,
    pub index_build_internal_us: u64,
    pub index_build_fsync_us: u64,
    pub index_build_rows: u64,
    pub index_build_heap_scans: u64,
    pub index_build_merge_us: u64,
    pub index_build_spill_write_us: u64,
    pub index_build_spill_read_us: u64,
    pub index_build_peak_active_workers: u64,
    /// B1 - live size of the oldest-dirty min-heaps. Bounded to ~2× the
    /// dirty page count by lazy compaction; a runaway value would be the
    /// pre-fix unbounded-growth leak resurfacing.
    pub buffer_pool_dirty_oldest_len: u64,
    pub spill_bytes_written: u64,
    pub spill_bytes_read: u64,
    pub catalog_txn_commits: u64,
    pub catalog_txn_rollbacks: u64,
    pub catalog_mutated_rows: u64,
    pub catalog_epoch: u64,
    pub catalog_cache_limit_bytes: u64,
    pub catalog_cache_bytes: u64,
    pub catalog_cache_entries: u64,
    // M1.D1 - DML row + atomicity / IGNORE / LOB lifecycle counters.
    pub dml_insert_rows: u64,
    pub dml_update_rows: u64,
    pub dml_delete_rows: u64,
    pub dml_ignore_warnings: u64,
    pub dml_rollback_inserted_rows: u64,
    pub dml_lob_leaks_recovered: u64,
    /// M5.C3 — UPSERT fast-path hits.
    pub dml_upsert_fast_path_hits: u64,
    pub dml_upsert_batch_attempts: u64,
    pub dml_upsert_batch_rows: u64,
    pub dml_upsert_batch_conflict_rows: u64,
    pub dml_upsert_batch_fallback_rows: u64,
    pub dml_upsert_batch_race_rollbacks: u64,
    /// M5.C5 — FK cascade visited-set short-circuits.
    pub dml_fk_cycle_short_circuits: u64,
    /// D2 — currently-running LOAD DATA statements + cumulative rows.
    pub load_data_active: u64,
    pub load_data_rows_loaded: u64,
    /// M1/M6 (LOB) — chain reuse / externalization: chain counts and bytes.
    pub dml_lob_chains_reused: u64,
    pub dml_lob_bytes_reused: u64,
    pub dml_lob_chains_externalized: u64,
    pub dml_lob_bytes_externalized: u64,
    /// M3 (LOB) — overflow pages returned to disk by shrink.
    pub dml_lob_pages_shrunk: u64,
    /// BUG-7 (LOB) — count of LOB chain frees that failed (not reclaimed) in purge/rollback.
    pub dml_lob_leaks_unrecovered: u64,
}

/// Narrow O(1) snapshot used by MySQL `SHOW STATUS` and PFS status tables.
/// Unlike [`StorageStatus`], this never walks resident buffer frames or dirty
/// registries; every field is an atomic read or a scalar copied while holding
/// one of the existing short metadata locks.
#[derive(Debug, Clone, Default)]
pub struct MysqlStatus {
    pub buffer_hits: u64,
    pub buffer_misses: u64,
    pub buffer_pool_dirty_pages: u64,
    pub buffer_pool_pages: u64,
    pub buffer_pool_used_pages: u64,
    pub buffer_pool_waits: u64,
    pub buffer_pool_write_requests: u64,
    pub buffer_pool_read_ios: u64,
    pub buffer_pool_read_syscalls: u64,
    pub buffer_pool_write_ios: u64,
    pub buffer_pool_write_syscalls: u64,
    pub buffer_pool_evictions: u64,
    pub buffer_pool_prefetches: u64,
    pub buffer_pool_prefetch_hits: u64,
    pub lock_waits: u64,
    pub active_lock_waits: usize,
    pub lock_wait_time_ms: u64,
    pub lock_wait_time_avg_ms: u64,
    pub lock_wait_time_max_ms: u64,
    pub deadlocks: u64,
    pub table_lock_acquires: u64,
    pub table_lock_waits: u64,
    pub wal_syncs: u64,
    pub wal_logical_records: u64,
    pub wal_bytes: u64,
    pub wal_physical_write_calls: u64,
    pub wal_physical_write_bytes: u64,
    pub active_transactions: usize,
    pub undo_bytes: u64,
    pub undo_records: usize,
    pub undo_peak_bytes: u64,
    pub checkpoints_total: u64,
    pub plan_cache_hits: u64,
    pub plan_cache_misses: u64,
    pub select_parse_cache_hits: u64,
    pub select_compiled_plan_hits: u64,
    pub select_operator_compiles: u64,
    pub prepared_plan_reuses: u64,
    pub prepared_plan_compiles: u64,
    pub snapshot_too_old_count: u64,
    pub dml_insert_rows: u64,
    pub dml_update_rows: u64,
    pub dml_delete_rows: u64,
}

#[cfg(test)]
static MYSQL_STATUS_SNAPSHOT_CALLS: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn mysql_status_snapshot_calls() -> u64 {
    MYSQL_STATUS_SNAPSHOT_CALLS.load(Ordering::Relaxed)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RecoverySummary {
    scanned_records: usize,
    single_checkpoint_end_record: bool,
    redone_records: usize,
    skipped_index_redo_records: usize,
    undone_transactions: usize,
    /// XA branches that completed `PREPARE` before the crash and that
    /// recovery preserved as PREPARED (i.e. did not undo). They wait
    /// for an external `XA COMMIT` / `XA ROLLBACK`.
    prepared_recovered: usize,
    repaired_indexes: usize,
    checked_heap_files: usize,
    checked_heap_pages: usize,
    skipped_heap_files: usize,
    skipped_heap_pages: usize,
    checked_index_files: usize,
    quick_checked_index_files: usize,
    skipped_index_files: usize,
    startup_validation_mode: Option<StartupValidationMode>,
    startup_validation_path: Option<StartupValidationPath>,
    startup_validation_us: u64,
    wal_recovery_us: u64,
    total_startup_us: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct IndexValidationSummary {
    repaired_indexes: usize,
    checked_index_files: usize,
    quick_checked_index_files: usize,
    skipped_index_files: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct HeapValidationSummary {
    checked_files: usize,
    checked_pages: usize,
    skipped_files: usize,
    skipped_pages: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    Strict,
    Relaxed,
    Memory,
}

thread_local! {
    /// True while this thread applies a physical-replication WAL frame
    /// (`Database::apply_wal_record`) or any work it triggers (e.g.
    /// replica-side index rebuilds). The replica's local WAL is a
    /// byte-identical mirror of the master's stream addressed by the
    /// MASTER's LSNs — any locally appended record (index FPIs, meta
    /// page images, …) would collide with the next incoming LSN and
    /// silently fork the replica. Index/page write paths consult this
    /// flag and suppress every WAL append while it is set.
    static PHYSICAL_WAL_APPLY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Statement-scoped default installed by the SQL executor. Transactions
    /// copy the value into a txn-id keyed registry so parallel LOAD workers
    /// and later statements of an explicit transaction see one frozen mode.
    static REDO_ENABLED_SCOPE: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

/// Installs the session's redo policy for synchronous storage entry points.
/// Internal/background callers do not install a guard and therefore retain
/// full redo. The previous value is restored for nested SQL execution.
pub struct RedoScopeGuard(bool);

impl RedoScopeGuard {
    pub fn new(enabled: bool) -> Self {
        let previous = REDO_ENABLED_SCOPE.with(|scope| scope.replace(enabled));
        Self(previous)
    }
}

impl Drop for RedoScopeGuard {
    fn drop(&mut self) {
        REDO_ENABLED_SCOPE.with(|scope| scope.set(self.0));
    }
}

pub(crate) fn current_redo_enabled() -> bool {
    REDO_ENABLED_SCOPE.with(|scope| scope.get())
}

static REDUCED_REDO_TRANSACTIONS: AtomicU64 = AtomicU64::new(0);
static REDUCED_REDO_SKIPPED_RECORDS: AtomicU64 = AtomicU64::new(0);
static REDUCED_REDO_SKIPPED_PAYLOAD_BYTES: AtomicU64 = AtomicU64::new(0);
static REDUCED_REDO_FALLBACK_RECORDS: AtomicU64 = AtomicU64::new(0);
static REDUCED_REDO_FORCE_FLUSHES: AtomicU64 = AtomicU64::new(0);
static REDUCED_REDO_FORCE_FLUSH_US: AtomicU64 = AtomicU64::new(0);

pub(crate) fn physical_wal_apply_active() -> bool {
    PHYSICAL_WAL_APPLY.with(|flag| flag.get())
}

pub(crate) fn with_physical_wal_apply<T>(f: impl FnOnce() -> T) -> T {
    PHYSICAL_WAL_APPLY.with(|flag| flag.set(true));
    let result = f();
    PHYSICAL_WAL_APPLY.with(|flag| flag.set(false));
    result
}

include!("core/database/slot_cache.rs");
include!("core/database/row_lock_shard.rs");
include!("core/database/guards.rs");
include!("core/database/facade.rs");
include!("core/database/catalog_store.rs");
include!("core/database/catalog_ops.rs");
include!("core/database/ai.rs");
include!("core/database/txn_ops.rs");
include!("core/database/vector_index_paths.rs");
include!("core/database/ddl.rs");
include!("core/database/online_index.rs");
include!("core/database/online_constraints.rs");
include!("core/database/constraint_index_merge.rs");
include!("core/database/constraint_baseline.rs");
include!("core/database/online_copy.rs");
include!("core/database/partition_reorg.rs");
include!("core/database/ctas_recovery.rs");
include!("core/database/drop_table_recovery.rs");
include!("core/database/truncate_recovery.rs");
include!("core/database/rename_table_recovery.rs");
include!("core/database/dml.rs");
include!("core/database/scan.rs");
include!("core/database/read.rs");
include!("core/database/index_access.rs");
include!("core/database/constraints_dml.rs");
include!("core/database/foreign_keys.rs");
include!("core/database/index_build_encoded.rs");
include!("core/database/index_build_fulltext.rs");
include!("core/database/index_maintenance.rs");
include!("core/database/shadow_generation.rs");
include!("core/database/vector_index.rs");
include!("core/database/vector_recovery.rs");
include!("core/database/vector_replication.rs");
include!("core/database/vector_generations.rs");
include!("core/database/vector_retirements.rs");
include!("core/database/mvcc.rs");
include!("core/database/startup.rs");
include!("core/database/stats_ops.rs");
include!("core/database/io_handles.rs");
include!("core/database/sequences.rs");
include!("core/database/sequence_persistence.rs");
include!("core/database/runtime_ops.rs");
include!("core/database/lock_snapshot.rs");
include!("core/database/wal_apply.rs");
include!("core/database/replicated_index_publish.rs");

include!("core/config.rs");
include!("core/buffer.rs");
impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        let shard = (self.snapshot_id as usize) % self.db.active_snapshots.len();
        self.db.active_snapshots[shard]
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(&self.snapshot_id);
        // A finished reader may unblock superseded vector versions.
        self.db.drain_vector_purges();
    }
}

include!("core/recovery_txns.rs");
include!("core/recovery_io.rs");
include!("core/recovery.rs");
include!("core/recovery_compensation.rs");
impl Drop for Database {
    fn drop(&mut self) {
        clear_txn_wal_buffers_for_log(&self.log);
        // A plain `Database` drop is also the path tests and callers use
        // to simulate a crash. Clean shutdown goes through
        // `StorageQueryEngine::shutdown()`/`Database::checkpoint()`,
        // which flushes data after establishing the WAL checkpoint
        // contract. Flushing here can persist pages from active
        // transactions without a matching durable checkpoint.
        self.buffer_pool.shutdown_flusher();
        crate::storage::temp_quota::clear_database(&self.data_dir);
    }
}

/// Number of per-page write latches in `HeapFile::page_latches`.
/// Hashed by `page_id & (PAGE_LATCH_COUNT - 1)`. Page writes acquire
/// the matching latch so concurrent writers on different pages don't
/// step on each other's read-modify-write cycle (the prior design
/// relied on `with_heap_mut`'s file-wide RwLock for serialisation,
/// which serialised all 20 sysbench worker threads behind a single
/// writer). 256 buckets keep false-sharing rare (random workload
/// collision probability ≈ 1/256 per page).
/// Default per-page latch count. Overridable at process start via
/// `MYDB_BTREE_PAGE_LATCH_COUNT` (see [`resolved_page_latch_count`]);
/// heap and index files share the resolved value.
pub(crate) const PAGE_LATCH_COUNT: usize = 1024;
/// Upper bound on the runtime latch count. Fixes the stack size of the
/// per-batch `held_latches` dedup bitset (`MAX_PAGE_LATCH_COUNT / 64`
/// u64 words = 1 KiB) independently of the configured count, so batch
/// latch acquisition never indexes past the bitset.
const MAX_PAGE_LATCH_COUNT: usize = 8192;
const MIN_PAGE_LATCH_COUNT: usize = 64;

/// Resolve the effective per-page latch count once per process. The
/// value is rounded down to a power of two and clamped to
/// `[MIN_PAGE_LATCH_COUNT, MAX_PAGE_LATCH_COUNT]` so `count - 1` stays a
/// valid contiguous hash mask. A larger count lowers false-sharing
/// collisions between writers on unrelated pages at the cost of
/// `count * 128 B` per open heap/index file, hence the modest default.
pub(crate) fn resolved_page_latch_count() -> usize {
    use std::sync::OnceLock;
    static COUNT: OnceLock<usize> = OnceLock::new();
    *COUNT.get_or_init(|| {
        match std::env::var("MYDB_BTREE_PAGE_LATCH_COUNT")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
        {
            Some(n) if n >= MIN_PAGE_LATCH_COUNT => {
                // Largest power of two <= n, then cap at the compile-time max.
                let p2 = 1usize << (usize::BITS - 1 - n.leading_zeros()) as usize;
                p2.min(MAX_PAGE_LATCH_COUNT)
            }
            Some(_) => MIN_PAGE_LATCH_COUNT,
            None => PAGE_LATCH_COUNT,
        }
    })
}

/// Build a boxed slice of `resolved_page_latch_count()` cache-line
/// padded latches together with its `count - 1` hash mask. Shared by
/// `HeapFile` and `IndexFile` construction.
pub(crate) fn new_page_latch_array() -> (Box<[CachePadded<Mutex<()>>]>, u64) {
    let count = resolved_page_latch_count();
    let mut v: Vec<CachePadded<Mutex<()>>> = Vec::with_capacity(count);
    for _ in 0..count {
        v.push(CachePadded::new(Mutex::new(())));
    }
    (v.into_boxed_slice(), (count as u64) - 1)
}

/// Heap files are commonly tiny and numerous. Keep their per-file latch
/// footprint at 64 cache lines by default; large heaps can opt back into a
/// wider array without forcing 100k-table deployments to allocate 12.8 GiB
/// of mutex storage over a metadata scan.
pub(crate) fn new_heap_page_latch_array() -> (Box<[CachePadded<Mutex<()>>]>, u64) {
    static COUNT: OnceLock<usize> = OnceLock::new();
    let count = *COUNT.get_or_init(|| {
        crate::util::env::var("MYDB_HEAP_PAGE_LATCH_COUNT")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .map(|value| value.clamp(MIN_PAGE_LATCH_COUNT, MAX_PAGE_LATCH_COUNT))
            .map(|value| 1usize << (usize::BITS - 1 - value.leading_zeros()) as usize)
            .unwrap_or(MIN_PAGE_LATCH_COUNT)
    });
    let mut latches = Vec::with_capacity(count);
    for _ in 0..count {
        latches.push(CachePadded::new(Mutex::new(())));
    }
    (latches.into_boxed_slice(), count as u64 - 1)
}

/// Per-thread overlay for [`IndexFile`]'s "logical-WAL mode" sentinel.
///
/// Set inside `apply_single_index_op_logged` / `insert_entries_logged`
/// / `apply_batch_deferred` (etc.) before driving the B-tree mutation,
/// restored on the way out. `write_page` consults this to decide
/// whether to emit a per-page FPI or skip the WAL byte image
/// (the upcoming logical record covers the page state). Previously
/// stored as a `&mut self` field; promoted to thread-local so the hot
/// `insert_deferred` / `delete_deferred` path can take `&self` and run
/// under a shared `with_index_mut` read lock — multiple writers to the
/// same `IndexFile` no longer serialize on a per-index exclusive lock.
pub(crate) mod btree_logical_wal {
    use std::cell::{Cell, RefCell};
    thread_local! {
        static LOGICAL_WAL_LSN: Cell<Option<u64>> = const { Cell::new(None) };
        /// ALTER COPY can build several private index files first and issue
        /// their final syncs together immediately before publication.
        static BULK_SYNC_DEFERRED: Cell<bool> = const { Cell::new(false) };
        /// Deferred-index collection buffer. `Some` ⇒ the index-batch fast path
        /// running on this thread is parking its WAL record for a single
        /// batched append at commit (`MYDB_WAL_TXN_BUFFER`). Each dirtied leaf
        /// frame stamps its `PageKey` here (after taking a `deferred_pin` so the
        /// frame stays un-flushable) instead of the record being appended
        /// eagerly. The collected keys are `finalize_deferred`'d — real LSN
        /// published, pin released — the moment the parked record is appended
        /// at commit. `None` ⇒ ordinary eager index stamping.
        static DEFERRED_KEYS: RefCell<Option<Vec<super::PageKey>>> =
            const { RefCell::new(None) };
    }

    pub(crate) fn current() -> Option<u64> {
        LOGICAL_WAL_LSN.with(|c| c.get())
    }

    pub(crate) fn set(value: Option<u64>) {
        LOGICAL_WAL_LSN.with(|c| c.set(value));
    }

    pub(crate) struct LsnScope(Option<u64>);
    impl LsnScope {
        pub(crate) fn new(value: Option<u64>) -> Self {
            let old = current();
            set(value);
            Self(old)
        }
    }
    impl Drop for LsnScope {
        fn drop(&mut self) {
            set(self.0);
        }
    }

    pub(crate) fn bulk_sync_deferred() -> bool {
        BULK_SYNC_DEFERRED.with(|c| c.get())
    }

    pub(crate) fn set_bulk_sync_deferred(value: bool) {
        BULK_SYNC_DEFERRED.with(|c| c.set(value));
    }

    /// Arm deferred-index collection for the current index-batch apply.
    pub(crate) fn begin_deferred() {
        DEFERRED_KEYS.with(|c| *c.borrow_mut() = Some(Vec::new()));
    }

    /// True while an index batch is being parked (leaves get pinned + stamped,
    /// the record is appended later). Lets the fast-path guard admit an
    /// `lsn = None` apply and `write_page` take the pin-and-collect branch.
    pub(crate) fn is_deferred() -> bool {
        DEFERRED_KEYS.with(|c| c.borrow().is_some())
    }

    /// Record a leaf frame whose deferred pin must be released (and dirty_lsn
    /// raised to the real record LSN) when the parked record is appended.
    pub(super) fn push_deferred_key(key: super::PageKey) {
        DEFERRED_KEYS.with(|c| {
            if let Some(keys) = c.borrow_mut().as_mut() {
                keys.push(key);
            }
        });
    }

    /// Disarm collection and return the pinned leaf keys (empty when nothing was
    /// stamped, e.g. the fast path fell back to the eager slow path).
    pub(super) fn take_deferred() -> Vec<super::PageKey> {
        DEFERRED_KEYS.with(|c| c.borrow_mut().take().unwrap_or_default())
    }
}

#[derive(Debug)]
pub(crate) struct HeapFile {
    #[allow(dead_code)]
    path: PathBuf,
    file: File,
    /// Catalog id of the table this heap belongs to. Carried only on
    /// the file handle now — `RowId` no longer redundantly stores it
    /// (the file already knows which table it serves). Still used to
    /// build the buffer-pool file key and to scope WAL fallback paths.
    table_id: u64,
    /// Atomic durability mode — most read sites take `&self`. The DDL
    /// `discard_without_flush` path writes here (`Memory` after discard)
    /// without needing the file-wide write lock.
    durability: std::sync::atomic::AtomicU8,
    update_reserve_bytes: usize,
    /// Live page count — concurrent inserts read and CAS this. File
    /// extension serialised separately via `extend_mutex` so two
    /// allocators don't race on `set_len`.
    page_count: std::sync::atomic::AtomicU64,
    file_key: Arc<BufferPageFileKey>,
    buffer_pool: Arc<BufferPool>,
    row_migrations: std::sync::atomic::AtomicU64,
    /// Best-effort checksum memo for immutable direct-scan page images. Each
    /// slot stores the last verified on-page checksum plus one; engine writes
    /// refresh that checksum, so a changed page automatically misses.
    verified_direct_checksums: Option<VerifiedDirectHeapChecksums>,
    log: Arc<crate::wal::LogMutex>,
    /// Held during a page-extension critical section (`page_count`
    /// CAS + `file.set_len`). Page modification within already-
    /// allocated pages is serialised at finer granularity by
    /// `page_latches`.
    extend_mutex: Mutex<()>,
    /// Pages below the tail whose reclaimed slots can take new rows. Fed by
    /// slot reclaim, consumed by single-row inserts before they fall back to
    /// the tail; see [`HeapFreePages`].
    free_pages: HeapFreePages,
    /// Per-page write latches. Writers acquire the latch indexed by
    /// `page_id & page_latch_mask` for the duration of their
    /// raw-page read-modify-write cycle.
    /// Readers do not consult these — buffer-pool frame state already
    /// handles read concurrency.
    ///
    /// P0-2 cache-line - each latch is padded to its own 128-byte cache
    /// line. The unpadded layout packed ~64 latches into one line, so
    /// two writers holding numerically-adjacent latches (e.g. page 5 and
    /// page 69) were ping-ponging the same cache line — degrading the
    /// nominal 1024-way sharding to a real ~16-way. With padding the
    /// array grows from 1 KiB to 128 KiB per heap file (still tiny next
    /// to even a single overflow page) and false sharing is gone.
    page_latches: Box<[CachePadded<Mutex<()>>]>,
    /// `resolved_page_latch_count() - 1`; `page_id & page_latch_mask`
    /// selects a latch bucket. Stored per file so the count can vary by
    /// process without a recompile.
    page_latch_mask: u64,
    rel_path: String,
    /// Schema-derived encode/decode plan for this table's rows. The
    /// holder of the `HeapFile` (`Database` via `heap_handle`) is
    /// responsible for handing in a codec aligned with the current
    /// `TableSchema` and for invalidating the heap cache so a new
    /// codec is built whenever the schema changes (ADD/DROP/MODIFY
    /// column).
    codec: Arc<RowCodec>,
    /// Off-page LOB reader for this table, set by `heap_handle` when the
    /// table has a string/binary column that could be externalized.
    /// Installed as the thread-local materializer at every lock
    /// acquisition so row decode can rebuild externalized values; `None`
    /// for tables that can never externalize (the decode path then never
    /// consults it). Travels with the cached handle so the INLJ /
    /// point-read fast paths are covered without a per-call lookup.
    lob_reader: Option<Arc<dyn LobMaterializer>>,
    /// Cache eviction closes only this outer handle; BufferPool remains the
    /// owner of resident/dirty pages. Crash/drop paths keep the default true.
    discard_pages_on_drop: bool,
}

/// In-memory free-space hints for one heap: pages below the tail that hold
/// reclaimed (`ROW_FLAG_FREE`) slots. Hints are advisory: a consumer always
/// re-plans the insert under the page latch, so a stale entry costs one probe
/// and is dropped. The set is bounded; pages beyond the bound are simply not
/// advertised and their space is found again on a later reclaim.
#[derive(Debug, Default)]
struct HeapFreePages {
    len: std::sync::atomic::AtomicUsize,
    inner: parking_lot::Mutex<HeapFreePageQueue>,
}

#[derive(Debug, Default)]
struct HeapFreePageQueue {
    order: std::collections::VecDeque<u32>,
    members: crate::util::fast_hash::AhHashSet<u32>,
}

impl HeapFreePages {
    const MAX_PAGES: usize = 1 << 16;

    fn is_empty(&self) -> bool {
        self.len.load(std::sync::atomic::Ordering::Relaxed) == 0
    }

    fn offer(&self, page_id: u32) {
        let mut inner = self.inner.lock();
        if inner.order.len() >= Self::MAX_PAGES || !inner.members.insert(page_id) {
            return;
        }
        inner.order.push_back(page_id);
        self.len
            .store(inner.order.len(), std::sync::atomic::Ordering::Relaxed);
    }

    fn take(&self) -> Option<u32> {
        let mut inner = self.inner.lock();
        let page_id = inner.order.pop_front()?;
        inner.members.remove(&page_id);
        self.len
            .store(inner.order.len(), std::sync::atomic::Ordering::Relaxed);
        Some(page_id)
    }
}

#[derive(Debug)]
struct VerifiedDirectHeapChecksums {
    values: Box<[std::sync::atomic::AtomicU64]>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

include!("core/zone_map.rs");
mod zone_map_page;
include!("core/heap.rs");
include!("core/overflow.rs");
include!("core/btree.rs");
include!("core/packed_index_leaf.rs");
include!("core/vector_page_io.rs");
include!("core/vector_page.rs");
include!("core/index_key_codec.rs");
include!("core/row_codec.rs");
include!("core/constraints.rs");
include!("core/catalog_io.rs");
include!("core/bytes.rs");
include!("core/errors.rs");
// The R-tree spatial index is a proper submodule (not `include!`d) so its
// helper types stay namespaced rather than polluting `storage::core`.
#[cfg(test)]
mod auto_increment_tests;
#[cfg(test)]
mod btree_batch_tests;
pub(crate) mod rtree;
#[cfg(test)]
mod sequence_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod btree_bug_audit_tests;
#[cfg(test)]
mod btree_lookup_tests;
#[cfg(test)]
mod read_ahead_tests;

#[cfg(test)]
mod dml_audit_1015_tests;
