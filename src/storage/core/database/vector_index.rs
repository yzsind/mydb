// In-memory HNSW vector index: lazy build, incremental maintenance, search,
// optional on-disk persistence (M1, 2026-06).
//
// A vector (HNSW) index keeps a degenerate B-tree (constant key -> row_id) as a
// durable row-id registry — maintained by the generic DML path with no special
// casing — while the actual nearest-neighbour structure is the in-memory HNSW
// graph held in `Database::vector_indexes`. The graph is *derived data*, like
// the spatial R-tree: built lazily from a heap scan, grown incrementally on
// insert, and never the source of truth. Every candidate it returns is re-read
// under the query's MVCC snapshot and re-ranked by the live heap vector, so a
// validity and snapshot coverage must be checked independently of heap re-rank.
//
// M1 added an optional `.hnsw` on-disk snapshot written by the checkpoint
// path. On restart we load the snapshot, then walk the heap and INSERT only
// rows not already in the loaded graph — turning the cold-start cost from
// "full rebuild" into "load snapshot + delta scan". A missing / stale /
// corrupt file falls back to the original full-rebuild path. The header
// epilogue (`generation`, `last_persisted_lsn`) is informational; correctness
// is enforced by comparing heap vectors with live nodes, not by the LSN.
//
// NOTE: this file is `include!`d into `storage/core.rs` inside `impl Database`
// scope, so it uses `//` line comments (not `//!`) and relies on the parent
// module's imports (`Arc`, `parking_lot`, `normalize_name`, `error`, `fs`,
// `Path`, `PathBuf`, `Write`, …).

use crate::engine::vector::hnsw_paged::{
    HnswBuildStats, HnswChunkBuildStats, PagedHnsw, BUILD_NODE_METADATA_BYTES,
};
use crate::engine::vector::{
    self, CompactHnswBuilder, CompactHnswStats, HnswIndex, IvfParams, IvfSq8Index, VectorGraph,
};
use crate::storage::schema::VectorAlgo;
// `VectorIndexFile`, `NodeView`, `VecId` are defined in `core.rs` itself (via
// `include!("core/vector_page.rs")`) and are therefore already in scope here —
// importing them would collide with those local definitions.

fn hnsw_build_memory_gate() -> &'static std::sync::Mutex<()> {
    static GATE: OnceLock<std::sync::Mutex<()>> = OnceLock::new();
    GATE.get_or_init(|| std::sync::Mutex::new(()))
}

struct HnswBuildMemoryLease {
    buffer_pool: Arc<BufferPool>,
    old_capacity_pages: u64,
    charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

impl HnswBuildMemoryLease {
    fn try_acquire(buffer_pool: Arc<BufferPool>, scratch_bytes: usize) -> Option<Self> {
        use crate::engine::executor::mem_budget::{
            controlled_memory_snapshot, managed_memory_limit_bytes, memory_target_bytes,
            operational_memory_limit_bytes, ControlledMemoryCategory, ControlledMemoryCharge,
        };

        let target = memory_target_bytes();
        let min_buffer = (16 * 1024 * 1024).min(target / 4).max(PAGE_SIZE);
        if scratch_bytes.saturating_add(min_buffer) > target {
            return None;
        }
        let old_capacity_pages = buffer_pool.capacity_pages();
        let (controlled, _, categories, _) = controlled_memory_snapshot();
        let raw = categories[0];
        let non_buffer = controlled.saturating_sub(raw);
        // Keep one physical-admission sampling window available for allocator
        // fragmentation, page metadata and concurrent protocol growth. The
        // build charge describes live containers, not allocator-retained pages.
        let admission_headroom = (target / 16).min(64 * 1024 * 1024);
        let physical_reclaim = crate::engine::memory_reclaimer::process_headroom_reclaim_bytes(
            scratch_bytes.saturating_add(admission_headroom),
            managed_memory_limit_bytes(),
        );
        let desired_buffer_bytes = operational_memory_limit_bytes()
            .saturating_sub(non_buffer)
            .saturating_sub(scratch_bytes)
            .min(if physical_reclaim == 0 {
                usize::MAX
            } else {
                raw.saturating_sub(physical_reclaim)
            })
            .max(min_buffer)
            .min((old_capacity_pages as usize).saturating_mul(PAGE_SIZE));
        let desired_pages = (desired_buffer_bytes / PAGE_SIZE).max(1) as u64;
        if desired_pages < old_capacity_pages
            && buffer_pool
                .set_capacity(desired_pages, Duration::from_secs(30))
                .is_err()
        {
            return None;
        }
        if desired_pages < old_capacity_pages {
            crate::engine::memory_reclaimer::release_allocator_cached_memory();
        }
        let charge = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::StorageScratch,
            scratch_bytes,
        );
        match charge {
            Some(charge) => Some(Self {
                buffer_pool,
                old_capacity_pages,
                charge: Some(charge),
            }),
            None => {
                let _ = buffer_pool.set_capacity(old_capacity_pages, Duration::from_secs(30));
                None
            }
        }
    }

    /// Raw input/output are one-pass streams when links or training read
    /// compact spill files. Lend unused raw-page residency to their file
    /// working set; Drop restores the original buffer capacity.
    fn use_streaming_buffer(&self) -> Result<(), QueryError> {
        let target = crate::engine::executor::mem_budget::memory_target_bytes();
        let floor = (16 * 1024 * 1024).min(target / 4).max(PAGE_SIZE);
        let pages = self
            .buffer_pool
            .capacity_pages()
            .min((floor / PAGE_SIZE).max(1) as u64);
        self.buffer_pool
            .set_capacity(pages, Duration::from_secs(30))?;
        crate::engine::memory_reclaimer::release_allocator_cached_memory();
        Ok(())
    }

    fn take_charge(&mut self) -> crate::engine::executor::mem_budget::ControlledMemoryCharge {
        self.charge
            .take()
            .expect("build lease charge transferred once")
    }

    fn bytes(&self) -> usize {
        self.charge.as_ref().map_or(0, |charge| charge.bytes())
    }
}

impl Drop for HnswBuildMemoryLease {
    fn drop(&mut self) {
        // Return scratch first so restoring the buffer capacity can never make
        // the two categories overlap above MYDB_MEMORY_TARGET.
        self.charge.take();
        if let Err(err) = self
            .buffer_pool
            .set_capacity(self.old_capacity_pages, Duration::from_secs(30))
        {
            log_message(&format!(
                "hnsw: failed to restore buffer-pool capacity after build: {err:?}"
            ));
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HnswColdBuildMode {
    FullMemory,
    CompactSq8,
    ChunkedPaged,
}

/// Vector-index storage backend, selected per-process by `MYDB_VECTOR_STORAGE`.
/// **`paged` is the default** (2026-06): the graph lives in the buffer pool
/// (`.hvec`) so a vector index is demand-paged (larger-than-memory) and
/// crash-safe through the physical WAL, exactly like the heap/B-tree. Set
/// `MYDB_VECTOR_STORAGE=memory` to opt back into the in-memory slab graph +
/// `.hnsw` snapshot — faster when the whole index fits in RAM, but bounded by
/// memory and rebuilt (not WAL-recovered) on restart.
///
/// Read on every build (cheap env read); a graph already cached in
/// `vector_indexes` keeps whatever backend it was built with until it is
/// dropped/rebuilt.
fn vector_storage_paged() -> bool {
    !matches!(
        crate::util::env::var("MYDB_VECTOR_STORAGE").ok().as_deref(),
        Some("memory" | "MEMORY" | "mem" | "slab" | "hnsw")
    )
}

/// Resolve IVF_SQ8 build/query parameters: index COMMENT tokens
/// (`NLIST=`/`NPROBE=`/`KMEANS_ITERS=`/`TRAIN_SAMPLE=`) take priority, then
/// env-var build defaults (`MYDB_IVF_*`), then the built-in defaults. `nlist=0`
/// means "auto (√N)". `nprobe_default` here is only the build-time fallback;
/// the query path re-resolves nprobe from the live session GUC / hint.
fn ivf_params_from_meta(meta: &crate::storage::schema::VectorIndexMeta) -> IvfParams {
    fn env_usize(name: &str) -> Option<usize> {
        std::env::var(name)
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
    }
    let def = IvfParams::default();
    IvfParams {
        nlist: meta
            .nlist
            .map(|n| n as usize)
            .or_else(|| env_usize("MYDB_IVF_NLIST"))
            .unwrap_or(0),
        nprobe_default: meta
            .default_nprobe
            .map(|n| n as usize)
            .or_else(|| env_usize("MYDB_IVF_NPROBE").filter(|n| *n > 0))
            .unwrap_or(def.nprobe_default),
        kmeans_iters: meta
            .kmeans_iters
            .map(|n| n as usize)
            .or_else(|| env_usize("MYDB_IVF_KMEANS_ITERS").filter(|n| *n > 0))
            .unwrap_or(def.kmeans_iters),
        train_sample_mult: meta
            .train_sample_mult
            .map(|n| n as usize)
            .or_else(|| env_usize("MYDB_IVF_TRAIN_SAMPLE").filter(|n| *n > 0))
            .unwrap_or(def.train_sample_mult),
    }
}

/// Soft-cap (12.5%) on dirty nodes before the opportunistic checkpoint
/// persist gate fires. Overridden at runtime by
/// `MYDB_HNSW_PERSIST_DIRTY_RATIO`; this is the compile-time fallback used
/// when the env var is unset / malformed.
pub(crate) const DEFAULT_HNSW_PERSIST_DIRTY_RATIO: f64 = 0.10;

/// Stale-ratio threshold at which background maintenance reclaims a graph's
/// tombstones. Overridden by `MYDB_HNSW_COMPACT_RATIO`. 20% mirrors the
/// design doc's recommendation.
pub(crate) const DEFAULT_HNSW_COMPACT_RATIO: f64 = 0.20;
/// Absolute tombstone count that triggers compaction even when the ratio
/// gate hasn't fired (huge graphs with a few percent stale still have
/// millions of dead neighbours). Overridden by `MYDB_HNSW_COMPACT_ABSOLUTE`.
pub(crate) const DEFAULT_HNSW_COMPACT_ABSOLUTE: u32 = 100_000;

/// Whether `.hnsw` persistence is enabled. Reads `MYDB_HNSW_PERSIST` on every
/// call (cheap env read, runtime-toggleable from tests). Defaults to `true`.
fn hnsw_persist_enabled() -> bool {
    match std::env::var("MYDB_HNSW_PERSIST") {
        Ok(v) => !matches!(v.as_str(), "0" | "off" | "OFF" | "false" | "FALSE"),
        Err(_) => true,
    }
}

/// Dirty-ratio threshold used by `vector_graph_persist_all_dirty`. Reads
/// `MYDB_HNSW_PERSIST_DIRTY_RATIO`. A value <= 0 forces persist on every
/// checkpoint; values > 1 effectively disable opportunistic persist (the
/// gate then only fires when explicitly forced).
fn hnsw_persist_dirty_ratio() -> f64 {
    std::env::var("MYDB_HNSW_PERSIST_DIRTY_RATIO")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|v| v.is_finite())
        .unwrap_or(DEFAULT_HNSW_PERSIST_DIRTY_RATIO)
}

/// Preload mode: `eager` opens every vector index and prepares its
/// governed query accelerator before accepting user queries; `lazy` (default) defers
/// this work to the first query. Reads `MYDB_HNSW_PRELOAD`.
fn hnsw_preload_eager() -> bool {
    match std::env::var("MYDB_HNSW_PRELOAD") {
        Ok(v) => !matches!(
            v.as_str(),
            "lazy" | "LAZY" | "0" | "off" | "OFF" | "false" | "FALSE"
        ),
        Err(_) => false,
    }
}

/// Whether `CREATE INDEX ... USING HNSW` / `ALTER TABLE ADD INDEX` build the
/// in-memory graph synchronously at DDL time (so the first KNN query doesn't
/// pay the full build cost) instead of deferring to the first query. Reads
/// `MYDB_HNSW_BUILD_ON_DDL`. Defaults to `true`. Turning it off restores the
/// pure lazy-build-on-first-query behaviour.
fn hnsw_build_on_ddl_enabled() -> bool {
    match std::env::var("MYDB_HNSW_BUILD_ON_DDL") {
        Ok(v) => !matches!(v.as_str(), "0" | "off" | "OFF" | "false" | "FALSE"),
        Err(_) => true,
    }
}

/// `MYDB_HNSW_COMPACT_DISABLE=1` short-circuits the checkpoint compaction
/// pass — used by tests that want to assert tombstone accumulation in
/// isolation.
fn hnsw_compact_disabled() -> bool {
    match std::env::var("MYDB_HNSW_COMPACT_DISABLE") {
        Ok(v) => matches!(v.as_str(), "1" | "true" | "TRUE" | "on" | "ON"),
        Err(_) => false,
    }
}

/// Stale-ratio gate read from `MYDB_HNSW_COMPACT_RATIO`. <= 0 forces
/// compaction whenever any tombstone exists; > 1 effectively disables
/// the ratio gate (the absolute gate can still fire).
fn hnsw_compact_ratio() -> f64 {
    std::env::var("MYDB_HNSW_COMPACT_RATIO")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|v| v.is_finite())
        .unwrap_or(DEFAULT_HNSW_COMPACT_RATIO)
}

/// Absolute tombstone-count gate from `MYDB_HNSW_COMPACT_ABSOLUTE`.
fn hnsw_compact_absolute() -> u32 {
    std::env::var("MYDB_HNSW_COMPACT_ABSOLUTE")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(DEFAULT_HNSW_COMPACT_ABSOLUTE)
}

/// Minimum heap pages per worker on the parallel cold-build path. Spawn /
/// join overhead is amortised by the per-row HNSW insert cost (~10s of µs
/// per row at 768d), so the floor is set
/// modestly — 16 pages × ~100 rows ≈ 1600 inserts per worker.
const HNSW_BUILD_MIN_PAGES_PER_WORKER: u64 = 16;

/// Resolved minimum pages-per-worker. `MYDB_HNSW_BUILD_MIN_PAGES_PER_WORKER`
/// overrides the compile-time default; this is undocumented in the GUC
/// table (the design doc fixes the threshold) but lets tests force the
/// parallel branch on heaps that would otherwise stay serial.
fn hnsw_build_min_pages_per_worker() -> u64 {
    std::env::var("MYDB_HNSW_BUILD_MIN_PAGES_PER_WORKER")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(HNSW_BUILD_MIN_PAGES_PER_WORKER)
}

/// Hard upper bound on `MYDB_HNSW_BUILD_THREADS`. Picked so that even a
/// huge host can't oversubscribe the process — the M3 per-node latches
/// scale to tens of threads, but the design doc's win lands inside this
/// range and going wider hurts the small-table case (chunk pages divide
/// down toward zero, leaving most workers idle).
const HNSW_BUILD_MAX_THREADS: usize = 32;

/// Worker count for the parallel cold-build path. Reads
/// `MYDB_HNSW_BUILD_THREADS`; default is all available cores, clamped to
/// `[1, HNSW_BUILD_MAX_THREADS]`. CREATE INDEX is synchronous and the measured
/// 10-core optimum uses all ten cores; reserving one core stretched the 100k ×
/// 768 topology phase by ~40%. A value of 0
/// or 1 forces the single-threaded path.
///
/// The HNSW slab supports **lock-free reads** (`NodeSlab`), so the node append
/// no longer serialises workers — concurrent builders take only `structural`
/// shared and scale near-linearly. (Before that fix the fan-out was a measured
/// 3.5× *slower* than single-threaded, so the default was 1; with lock-free
/// reads a CREATE INDEX is CPU-bursty and benefits from most cores. Measured on
/// a 10-core M4, 20k vectors: 128d build 3.4 s→0.74 s, 768d 20.6 s→4.1 s —
/// beating pgvector's 0.80 s / 9.67 s respectively.)
fn hnsw_build_threads() -> usize {
    if let Ok(raw) = std::env::var("MYDB_HNSW_BUILD_THREADS") {
        if let Ok(v) = raw.parse::<usize>() {
            return v.min(HNSW_BUILD_MAX_THREADS);
        }
    }
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    cores.clamp(1, HNSW_BUILD_MAX_THREADS)
}

fn hnsw_positive_mb_env(name: &str) -> Result<Option<usize>, QueryError> {
    let Ok(raw) = std::env::var(name) else {
        return Ok(None);
    };
    let mb = raw.trim().parse::<usize>().ok().filter(|value| *value > 0);
    mb.map(|value| Some(value.saturating_mul(1024 * 1024)))
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                format!("{name} must be a positive whole number of MiB"),
            )
        })
}

fn hnsw_bulk_memory_budget_bytes() -> Result<u64, QueryError> {
    if let Some(bytes) = hnsw_positive_mb_env("MYDB_HNSW_BULK_MEMORY_MB")? {
        return Ok(bytes as u64);
    }
    // The direct transcriber owns one f32 graph, its SQ8 navigation arena and
    // adjacency scaffolding, but no second table-sized row batch. Give a cold
    // build up to 80% of the configured process target by default; an explicit
    // HNSW budget always wins. This scales down on constrained deployments and
    // avoids a fixed 256 MiB cliff on ordinary 1 GiB servers.
    Ok(
        (crate::engine::executor::mem_budget::memory_target_bytes() as u64).saturating_mul(80)
            / 100,
    )
}

fn hnsw_chunked_scratch_cap(target_bytes: usize) -> usize {
    // The ordinary buffer pool owns 60% of MYDB_MEMORY_TARGET. Let a
    // low-memory cold build borrow at most another 30%, leaving the governor's
    // remaining 10% envelope untouched. HnswBuildMemoryLease still shrinks the
    // live buffer pool first and atomically reserves the resulting scratch, so
    // this is a ceiling rather than permission to cross the managed limit.
    target_bytes / 10 * 3 + (target_bytes % 10) * 3 / 10
}

fn hnsw_preferred_cold_build_mode(
    mode: &str,
    full_bytes: usize,
    compact_bytes: usize,
    budget_bytes: usize,
    quantizable: bool,
) -> HnswColdBuildMode {
    if matches!(mode, "chunked" | "stream") {
        return HnswColdBuildMode::ChunkedPaged;
    }
    if mode == "memory" {
        return HnswColdBuildMode::FullMemory;
    }
    if full_bytes <= budget_bytes {
        HnswColdBuildMode::FullMemory
    } else if quantizable && compact_bytes <= budget_bytes {
        HnswColdBuildMode::CompactSq8
    } else {
        HnswColdBuildMode::ChunkedPaged
    }
}

/// Minimum heap pages a brute-KNN worker must own before the scan fans out
/// to a new thread. A vector page holds far fewer rows than a narrow OLTP
/// page (a 768-d row is ~3 KiB), so even a handful of pages is real work;
/// the floor only exists to keep `thread::scope` spawn/join off the
/// critical path for tiny tables. Overridden by
/// `MYDB_VECTOR_BRUTE_MIN_PAGES_PER_WORKER`.
const VECTOR_BRUTE_MIN_PAGES_PER_WORKER: u64 = 8;

fn vector_brute_min_pages_per_worker() -> u64 {
    std::env::var("MYDB_VECTOR_BRUTE_MIN_PAGES_PER_WORKER")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(VECTOR_BRUTE_MIN_PAGES_PER_WORKER)
}

/// Worker count for the parallel brute-force KNN scan. Reads
/// `MYDB_VECTOR_BRUTE_THREADS`; default `cores - 1` clamped to
/// `[1, HNSW_BUILD_MAX_THREADS]`. The global `MYDB_PARALLEL_DISABLE` escape
/// hatch forces the single-threaded path (parity with the rest of the
/// executor's parallel operators). pgvector runs a KNN seq scan
/// single-threaded by default, so fanning the scan across cores is the
/// decisive lever for the no-index path.
fn vector_brute_scan_threads() -> usize {
    if crate::util::env::var("MYDB_PARALLEL_DISABLE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        return 1;
    }
    if let Ok(raw) = std::env::var("MYDB_VECTOR_BRUTE_THREADS") {
        if let Ok(v) = raw.parse::<usize>() {
            return v.clamp(1, HNSW_BUILD_MAX_THREADS);
        }
    }
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    cores.saturating_sub(1).clamp(1, HNSW_BUILD_MAX_THREADS)
}

/// One brute-KNN candidate: an ordering distance (sqrt-free for L2) keyed to
/// the heap row it came from. NULL / dimensionless vectors map to
/// `f64::NEG_INFINITY` so they sort first under `ORDER BY distance ASC`,
/// matching MySQL's NULLs-first ordering (the row-based oracle returns
/// `DISTANCE(NULL, q) = NULL`, which sorts ahead of every real distance).
#[derive(Clone, Copy)]
struct BruteCand {
    key: f64,
    /// Partition storage id — `table.id` for unpartitioned tables. RowIds are
    /// only unique within a partition, so the tie-break (and the executor's
    /// row read) must disambiguate by `sid`.
    sid: u64,
    rid: RowId,
}

impl BruteCand {
    #[inline]
    fn rid_key(&self) -> u64 {
        ((self.rid.page_id as u64) << 16) | (self.rid.slot_id as u64)
    }
}

impl PartialEq for BruteCand {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl Eq for BruteCand {}
impl Ord for BruteCand {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Total order on (distance, row_id). `total_cmp` keeps NaN-free f32s
        // well-ordered; row_id is the deterministic tie-break so the result
        // is stable regardless of how the page range was partitioned across
        // workers.
        self.key
            .total_cmp(&other.key)
            .then_with(|| self.sid.cmp(&other.sid))
            .then_with(|| self.rid_key().cmp(&other.rid_key()))
    }
}
impl PartialOrd for BruteCand {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Bounded top-`k` nearest accumulator: a binary **max-heap** of capacity
/// `k` whose top is the current k-th nearest (the worst kept), so a closer
/// candidate evicts it in O(log k). Holds only `(f64, RowId)` — never a
/// `Value` — so the hot scan loop allocates nothing per row; the K winners
/// are re-materialised once at the end.
struct BruteTopK {
    heap: std::collections::BinaryHeap<BruteCand>,
    k: usize,
}

impl BruteTopK {
    fn new(k: usize) -> Self {
        BruteTopK {
            heap: std::collections::BinaryHeap::with_capacity(k.min(4096) + 1),
            k,
        }
    }

    #[inline]
    fn push(&mut self, key: impl Into<f64>, sid: u64, rid: RowId) {
        let key = key.into();
        let key = if key.is_nan() { f64::NEG_INFINITY } else { key };
        if self.k == 0 {
            return;
        }
        let cand = BruteCand { key, sid, rid };
        if self.heap.len() < self.k {
            self.heap.push(cand);
        } else if let Some(worst) = self.heap.peek() {
            // Max-heap top is the farthest kept; a nearer candidate replaces it.
            if cand < *worst {
                self.heap.pop();
                self.heap.push(cand);
            }
        }
    }

    fn merge(&mut self, other: BruteTopK) {
        for cand in other.heap {
            self.push(cand.key, cand.sid, cand.rid);
        }
    }

    /// Drain into `(distance, storage_id, row_id)` ascending. `storage_id`
    /// routes the executor's row read to the right partition.
    fn into_sorted(self) -> Vec<(f64, u64, RowId)> {
        let mut cands = self.heap.into_vec();
        cands.sort_unstable();
        cands.into_iter().map(|c| (c.key, c.sid, c.rid)).collect()
    }
}

impl Database {
    /// Read a `.hnsw` snapshot from disk and deserialize. Returns `Ok(None)`
    /// for a missing file (the common "first-ever query" case) and `Err` for
    /// any other IO / corruption / version-mismatch failure — the caller
    /// (`vector_graph_load_or_build`) logs the error and falls back to a
    /// full heap-scan rebuild rather than propagating to the user.
    fn vector_graph_load_from_disk(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> std::io::Result<Option<HnswIndex>> {
        let path = self.vector_index_path(table, &index.name, storage_id);
        let file = match std::fs::File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let length = usize::try_from(file.metadata()?.len())
            .map_err(|_| std::io::Error::other("HNSW snapshot exceeds address space"))?;
        let _snapshot_charge =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                length,
            )
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| std::io::Error::other("HNSW snapshot allocation failed"))?;
        file.take(length as u64).read_to_end(&mut bytes)?;
        let graph = HnswIndex::deserialize(&bytes)?;
        let meta = index.vector_meta_for_table(table);
        let column = table
            .column_index(&index.columns[0])
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let dim = match table.columns[column].data_type {
            DataTypeKind::Vector { dim, .. } => dim.map(|v| v as usize),
            _ => None,
        };
        if !graph.matches_configuration(
            meta.metric_enum(),
            meta.m as usize,
            meta.ef_construction as usize,
            dim,
        ) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HNSW snapshot parameters differ from the catalog",
            ));
        }
        Ok(Some(graph))
    }

    /// Load a snapshot, then compare every current heap vector against its
    /// live snapshot node. Only changed/new/reused rows need incremental
    /// linking. RowId alone is insufficient because UPDATE preserves it.
    ///
    /// Persistence disabled / missing file / corrupt file: behaves exactly
    /// like the pre-M1 full-rebuild path.
    ///
    /// M5.5 — `storage_id` selects which partition's heap to scan. For
    /// unpartitioned tables callers pass `table.id`. Each partition gets
    /// its own snapshot + graph (RowIds are only unique within one
    /// partition).
    fn vector_graph_load_or_build(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<VectorGraph, QueryError> {
        let column = table.column_index(&index.columns[0])?;
        if matches!(table.columns[column].data_type,DataTypeKind::Vector {datatype,..} if datatype!=crate::vector::VectorDataType::Float32)
            && index.vector_algo() == VectorAlgo::Hnsw
        {
            return self.vector_native_hnsw_load_or_build(table, index, storage_id);
        }
        if index.vector_algo() == VectorAlgo::Ivf
            || (index.vector_algo() == VectorAlgo::IvfSq8
                && matches!(table.columns[column].data_type,DataTypeKind::Vector {datatype,..} if datatype!=crate::vector::VectorDataType::Float32))
        {
            return self.vector_native_ivf_load_or_build(table, index, storage_id);
        }
        // Source HNSW page redo is not applied locally. A replica owns only
        // a revocable resident accelerator built from its committed heap view.
        if self.physical_replica_mode() && index.vector_algo() == VectorAlgo::Hnsw {
            let meta = index.vector_meta_for_table(table);
            let graph = HnswIndex::new(
                meta.metric_enum(),
                meta.m as usize,
                meta.ef_construction as usize,
            );
            let col = table.column_index(&index.columns[0])?;
            self.vector_graph_populate_from_heap(
                table,
                storage_id,
                &[col],
                &graph,
                hnsw_build_threads(),
            )?;
            return Ok(VectorGraph::Memory(graph));
        }
        let paged_backend = vector_storage_paged();
        self.ensure_vector_backend_compatible(table, index, storage_id, paged_backend)?;
        // IVF_SQ8 has its own (memory-backend) build path. It rebuilds the
        // inverted lists from the heap on every cold load — the heap is the
        // source of truth and the build is cached in `vector_indexes` for the
        // process lifetime, so there is no snapshot to load or catch up.
        if index.vector_algo() == VectorAlgo::IvfSq8 {
            return self.vector_ivf_load_or_build(table, index, storage_id);
        }
        if paged_backend {
            return Ok(VectorGraph::Paged(
                self.vector_graph_paged_load_or_build(table, index, storage_id)?,
            ));
        }
        let meta = index.vector_meta_for_table(table);
        let col_idx = table.column_index(&index.columns[0])?;
        let cols = [col_idx];

        let graph = if hnsw_persist_enabled() {
            match self.vector_graph_load_from_disk(table, index, storage_id) {
                Ok(Some(g)) => g,
                Ok(None) => HnswIndex::new(
                    meta.metric_enum(),
                    meta.m as usize,
                    meta.ef_construction as usize,
                ),
                Err(e) => {
                    log_message(&format!(
                        "hnsw: corrupt or unreadable {}/{}/{} (sid={}) — falling back to rebuild: {}",
                        table.database, table.name, index.name, storage_id, e
                    ));
                    HnswIndex::new(
                        meta.metric_enum(),
                        meta.m as usize,
                        meta.ef_construction as usize,
                    )
                }
            }
        } else {
            HnswIndex::new(
                meta.metric_enum(),
                meta.m as usize,
                meta.ef_construction as usize,
            )
        };

        // M4: parallel cold-build path. `HnswIndex::insert` is `&self` under
        // the M3 per-node latches, so workers can hammer the same graph
        // concurrently. We split the single partition's heap into
        // `[page_lo, page_hi)` ranges (one per worker) and let
        // `std::thread::scope` drive them — disjoint page ranges mean workers
        // never decode the same row twice, and `with_heap_read` is a
        // parking_lot::RwLock read so concurrent workers don't serialise on
        // the heap.
        let threads = hnsw_build_threads();
        self.vector_graph_populate_from_heap(table, storage_id, &cols, &graph, threads)?;

        Ok(VectorGraph::Memory(graph))
    }

    fn vector_native_hnsw_load_or_build(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<VectorGraph, QueryError> {
        let col = table.column_index(&index.columns[0])?;
        let DataTypeKind::Vector {
            datatype,
            dim: Some(dimension),
        } = table.columns[col].data_type
        else {
            return Err(vector::vector_err(
                "native HNSW requires a declared dimension",
            ));
        };
        let meta = index.vector_meta_for_table(table);
        let paged = vector_storage_paged() && !self.physical_replica_mode();
        let final_path = self.vector_index_paged_path(table, &index.name, storage_id);
        let mut tmp_path = final_path.clone();
        tmp_path.set_file_name(format!(
            "{}.native-build-{}.hvec",
            final_path.file_stem().unwrap_or_default().to_string_lossy(),
            self.visible_scn()
        ));
        let geometry = native_vector_geometry(datatype, dimension);
        let file = if paged {
            self.buffer_pool.discard_pages_for_path(&tmp_path)?;
            if tmp_path.exists() {
                fs::remove_file(&tmp_path).map_err(io_err)?;
            }
            let file = Arc::new(self.open_vector_index_file_at_path(
                tmp_path.clone(),
                geometry,
                meta.metric_enum().as_u8(),
                meta.m as u16,
                (meta.m * 2) as u16,
                meta.ef_construction.min(u16::MAX as u32) as u16,
            )?);
            file.invalidate_build(0)?;
            file.begin_build();
            Some(file)
        } else {
            None
        };
        let mut graph = vector::native_hnsw::NativeHnsw::new(
            datatype,
            dimension,
            meta.metric_enum(),
            meta.m as usize,
            meta.ef_construction as usize,
            file,
        )?;
        let (mut txn, _snapshot) = self.borrowed_read_only_statement_view();
        if table.row_generation_txn_id != 0 {
            txn.txn_id = table.row_generation_txn_id;
        }
        self.vector_snapshot_floor
            .fetch_max(txn.snapshot_scn, std::sync::atomic::Ordering::AcqRel);
        let built = self.scan_hnsw_build_rows(
            table,
            storage_id,
            txn,
            &[col],
            0,
            u64::MAX,
            |row, values| {
                if let Some(Value::Vector(value)) = values.first() {
                    graph.insert(row, value.clone())?;
                }
                Ok(false)
            },
        );
        if let Err(err) = built {
            graph.invalidate();
            return Err(err);
        }
        if let Some(file) = graph.file() {
            file.advance_coverage(txn.snapshot_scn);
            file.finalize_build(0)?;
        }
        if paged && self.durability_mode() != DurabilityMode::Memory {
            self.buffer_pool.discard_pages_for_path(&tmp_path)?;
            self.buffer_pool.discard_pages_for_path(&final_path)?;
            fs::rename(&tmp_path, &final_path).map_err(io_err)?;
            if let Some(parent) = final_path.parent() {
                crate::platform::fs::sync_path(parent).map_err(io_err)?;
            }
            let file = Arc::new(self.open_vector_index_file_at_path(
                final_path,
                geometry,
                meta.metric_enum().as_u8(),
                meta.m as u16,
                (meta.m * 2) as u16,
                meta.ef_construction.min(u16::MAX as u32) as u16,
            )?);
            graph.replace_file(file);
        }
        graph.prepare_navigation()?;
        Ok(VectorGraph::NativeHnsw(graph))
    }

    fn vector_native_ivf_load_or_build(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<VectorGraph, QueryError> {
        let col = table.column_index(&index.columns[0])?;
        let DataTypeKind::Vector {
            datatype,
            dim: Some(dimension),
        } = table.columns[col].data_type
        else {
            return Err(vector::vector_err(
                "native IVF requires a declared dimension",
            ));
        };
        let meta = index.vector_meta_for_table(table);
        let paged = vector_storage_paged() && !self.physical_replica_mode();
        let final_path = self.vector_index_paged_path(table, &index.name, storage_id);
        let mut tmp_path = final_path.clone();
        tmp_path.set_file_name(format!(
            "{}.native-build-{}.hvec",
            final_path.file_stem().unwrap_or_default().to_string_lossy(),
            self.visible_scn()
        ));
        let geometry = native_vector_geometry(datatype, dimension);
        let file = if paged {
            self.buffer_pool.discard_pages_for_path(&tmp_path)?;
            if tmp_path.exists() {
                fs::remove_file(&tmp_path).map_err(io_err)?;
            }
            let file = Arc::new(self.open_vector_index_file_at_path(
                tmp_path.clone(),
                geometry,
                meta.metric_enum().as_u8(),
                2,
                0,
                1,
            )?);
            file.invalidate_build(0)?;
            file.begin_build();
            Some(file)
        } else {
            None
        };
        let mut graph = vector::native_ivf::NativeIvf::new(
            datatype,
            dimension,
            meta.metric_enum(),
            ivf_params_from_meta(&meta),
            index.vector_algo() == VectorAlgo::IvfSq8,
            file,
        )?;
        let (mut txn, _snapshot) = self.borrowed_read_only_statement_view();
        if table.row_generation_txn_id != 0 {
            txn.txn_id = table.row_generation_txn_id;
        }
        self.vector_snapshot_floor
            .fetch_max(txn.snapshot_scn, std::sync::atomic::Ordering::AcqRel);
        let mut count = 0usize;
        self.scan_hnsw_build_rows(table, storage_id, txn, &[col], 0, u64::MAX, |_, values| {
            if matches!(values.first(), Some(Value::Vector(_))) {
                count += 1;
            }
            Ok(false)
        })?;
        if count > 0 {
            let params = ivf_params_from_meta(&meta);
            let nlist = if params.nlist == 0 {
                (count as f64).sqrt() as usize
            } else {
                params.nlist
            }
            .max(1)
            .min(count);
            let sample_count = nlist.saturating_mul(params.train_sample_mult).min(count);
            let sample_charge =
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    sample_count.saturating_mul(datatype.payload_len(dimension) + 96),
                )?;
            let mut ordinals = vector::ivf::training_sample(count, sample_count);
            ordinals.sort_unstable();
            let mut ordinals = ordinals.into_iter().peekable();
            let mut ordinal = 0usize;
            let mut rows = Vec::with_capacity(sample_count);
            self.scan_hnsw_build_rows(
                table,
                storage_id,
                txn,
                &[col],
                0,
                u64::MAX,
                |row, values| {
                    if let Some(Value::Vector(value)) = values.first() {
                        if ordinals.peek() == Some(&ordinal) {
                            rows.push((row, value.clone()));
                            ordinals.next();
                        }
                        ordinal += 1;
                    }
                    Ok(ordinals.peek().is_none())
                },
            )?;
            graph.initialize_from_sample(rows, nlist)?;
            drop(sample_charge);
        }
        let built = self.scan_hnsw_build_rows(
            table,
            storage_id,
            txn,
            &[col],
            0,
            u64::MAX,
            |row, values| {
                if let Some(Value::Vector(value)) = values.first() {
                    graph.insert(row, value.clone())?;
                }
                Ok(false)
            },
        );
        if let Err(err) = built {
            graph.invalidate();
            return Err(err);
        }
        if let Some(file) = graph.file() {
            file.advance_coverage(txn.snapshot_scn);
            file.finalize_build(0)?;
        }
        if paged && self.durability_mode() != DurabilityMode::Memory {
            self.buffer_pool.discard_pages_for_path(&tmp_path)?;
            self.buffer_pool.discard_pages_for_path(&final_path)?;
            fs::rename(&tmp_path, &final_path).map_err(io_err)?;
            if let Some(parent) = final_path.parent() {
                crate::platform::fs::sync_path(parent).map_err(io_err)?;
            }
            let file = Arc::new(self.open_vector_index_file_at_path(
                final_path,
                geometry,
                meta.metric_enum().as_u8(),
                2,
                0,
                1,
            )?);
            graph.replace_file(file);
        }
        Ok(VectorGraph::NativeIvf(graph))
    }

    /// IVF_SQ8 cold build: materialize every visible vector from the partition
    /// heap into a `(RowId, vector)` batch, then train the coarse quantizer and
    /// populate the SQ8 inverted lists. Build parameters come from the index
    /// COMMENT (`NLIST=`/`NPROBE=`/`KMEANS_ITERS=`/`TRAIN_SAMPLE=`) with env
    /// fallbacks. An empty heap yields an untrained (empty) index that later
    /// DML / the next cold build populates.
    fn vector_ivf_load_or_build(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<VectorGraph, QueryError> {
        if vector_storage_paged() {
            return self.vector_ivf_paged_load_or_build(table, index, storage_id);
        }
        let meta = index.vector_meta_for_table(table);
        let ivf = IvfSq8Index::new(meta.metric_enum(), ivf_params_from_meta(&meta));
        let col_idx = table.column_index(&index.columns[0])?;
        let cols = [col_idx];
        let mut rows: Vec<(RowId, Vec<f32>)> = Vec::new();
        self.with_heap_read(storage_id, |heap| {
            heap.scan_mvcc_columns_until(&cols, |row| {
                if !row.header.is_deleted() {
                    if let Some(Value::Vector(bytes)) = row.values.first() {
                        rows.push((row.row_id, vector::to_f32(bytes)));
                    }
                }
                Ok(false)
            })
        })?;
        ivf.build_from_rows(&rows)?;
        Ok(VectorGraph::Ivf(ivf))
    }

    /// Paged IVF_SQ8 build/open: open (or create) the `.ivf` file — which *is*
    /// the durable index (buffer pool + WAL) — and cold-build it from the
    /// partition heap when it holds no finalized index yet (a fresh file, or an
    /// interrupted/legacy build). A finalized file is trusted and used as-is,
    /// so it survives restart on its own with no snapshot to load. Mirrors
    /// `vector_graph_paged_load_or_build` for HNSW.
    fn vector_ivf_paged_load_or_build(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<VectorGraph, QueryError> {
        let col_idx = table.column_index(&index.columns[0])?;
        let cols = [col_idx];
        let dim = self.vector_paged_resolve_dim(table, col_idx, storage_id)?;
        let path = self.vector_index_paged_path_ext(table, &index.name, storage_id, "ivf");
        let params = ivf_params_from_meta(&index.vector_meta());
        // Opening can fail when the persisted `.ivf` disagrees with the current
        // dim/metric (a COMMENT metric change, a dim change) or its meta page is
        // corrupt. The heap is the durable record, so rather than failing every
        // KNN query with a hard 1030 (B3), discard the unusable file and reopen
        // fresh — the `!is_finalized()` branch below then rebuilds it from the
        // heap. Mirrors the HNSW memory path's log-and-rebuild.
        let paged = match self.open_ivf_index_file(table, index, storage_id, dim) {
            Ok(file) => file,
            Err(e) => {
                if self.is_snapshot_read_only() || self.physical_replica_mode() {
                    return Err(e);
                }
                log_message(&format!(
                    "ivf(paged): unusable .ivf at {} ({e:?}); discarding and rebuilding from heap",
                    path.display()
                ));
                self.buffer_pool.discard_pages_for_path(&path)?;
                let _ = fs::remove_file(&path);
                self.open_ivf_index_file(table, index, storage_id, dim)?
            }
        };
        let paged = if self.physical_replica_mode() {
            crate::engine::vector::ivf_paged::PagedIvf::new_for_replica(Arc::new(paged), params)
        } else {
            crate::engine::vector::ivf_paged::PagedIvf::new(Arc::new(paged), params)
        };
        if !paged.is_finalized() {
            if self.physical_replica_mode() {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "replicated IVF index is not finalized; wait for source replay",
                ));
            }
            log_message(format!(
                "ivf(paged): unfinalized index {}; rebuilding from heap",
                path.display()
            ));
            self.ensure_writable()?;
            drop(paged);
            self.buffer_pool.discard_pages_for_path(&path)?;
            let _ = fs::remove_file(&path);
            let file = self.open_ivf_index_file(table, index, storage_id, dim)?;
            let rebuilt = crate::engine::vector::ivf_paged::PagedIvf::new(Arc::new(file), params);
            self.vector_ivf_paged_build_from_heap(storage_id, &cols, &rebuilt)?;
            return Ok(VectorGraph::Ivf(IvfSq8Index::from_paged(rebuilt)));
        }
        self.vector_ivf_paged_build_navigator_from_heap(storage_id, &cols, &paged)?;
        Ok(VectorGraph::Ivf(IvfSq8Index::from_paged(paged)))
    }

    /// Open (creating if absent) the `.ivf` file for one paged IVF_SQ8 index,
    /// wiring it to the shared WAL + buffer pool. The on-disk payload layout
    /// (SQ8 codes vs raw f32) is fixed at create time by the metric + the SME
    /// env; a reopen adopts the file's persisted layout.
    fn open_ivf_index_file(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
        dim: u32,
    ) -> Result<crate::storage::IvfIndexFile, QueryError> {
        let meta = index.vector_meta_for_table(table);
        let metric = meta.metric_enum();
        let stores_f32 = crate::engine::vector::ivf_paged::PagedIvf::stores_f32_for(metric);
        let path = self.vector_index_paged_path_ext(table, &index.name, storage_id, "ivf");
        self.open_ivf_index_file_at_path(path, dim, metric.as_u8(), stores_f32)
    }

    fn open_ivf_index_file_at_path(
        &self,
        path: PathBuf,
        dim: u32,
        metric: u8,
        stores_f32: bool,
    ) -> Result<crate::storage::IvfIndexFile, QueryError> {
        crate::storage::IvfIndexFile::open_with_durability(
            path,
            self.log.clone(),
            self.durability_mode(),
            self.buffer_pool.clone(),
            dim,
            metric,
            stores_f32,
        )
    }

    /// Build a replacement `.ivf` beside the live file and atomically publish
    /// it.  The old finalized index remains queryable until rename succeeds.
    fn rebuild_paged_ivf_atomic(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<Arc<VectorGraph>, QueryError> {
        let col_idx = table.column_index(&index.columns[0])?;
        let dim = self.vector_paged_resolve_dim(table, col_idx, storage_id)?;
        let meta = index.vector_meta_for_table(table);
        let metric = meta.metric_enum();
        let stores_f32 = crate::engine::vector::ivf_paged::PagedIvf::stores_f32_for(metric);
        let params = ivf_params_from_meta(&meta);
        let final_path = self.vector_index_paged_path_ext(table, &index.name, storage_id, "ivf");
        let tmp_path = final_path.with_extension("ivf.optimize.tmp");
        let _ = self.buffer_pool.discard_pages_for_path(&tmp_path);
        let _ = fs::remove_file(&tmp_path);
        let replacement = crate::engine::vector::ivf_paged::PagedIvf::new(
            Arc::new(self.open_ivf_index_file_at_path(
                tmp_path.clone(),
                dim,
                metric.as_u8(),
                stores_f32,
            )?),
            params,
        );
        self.vector_ivf_paged_build_from_heap(storage_id, &[col_idx], &replacement)?;
        replacement.file().flush()?;
        if self.durability_mode() == DurabilityMode::Memory {
            // The test/in-memory durability backend has no physical pathname
            // to rename; publishing the fully-built Arc is the atomic swap.
            return Ok(Arc::new(VectorGraph::Ivf(IvfSq8Index::from_paged(
                replacement,
            ))));
        }
        let navigator = replacement.navigator();
        drop(replacement);
        self.buffer_pool.discard_pages_for_path(&tmp_path)?;
        self.buffer_pool.discard_pages_for_path(&final_path)?;
        fs::rename(&tmp_path, &final_path).map_err(|e| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!(
                    "cannot atomically publish optimized IVF index {} -> {}: {e}",
                    tmp_path.display(),
                    final_path.display()
                ),
            )
        })?;
        if let Some(parent) = final_path.parent() {
            crate::platform::fs::sync_path(parent).map_err(io_err)?;
        }
        let reopened = crate::engine::vector::ivf_paged::PagedIvf::new(
            Arc::new(self.open_ivf_index_file(table, index, storage_id, dim)?),
            params,
        );
        reopened.install_navigator(navigator);
        Ok(Arc::new(VectorGraph::Ivf(IvfSq8Index::from_paged(
            reopened,
        ))))
    }

    /// Cold-build a paged IVF index with reserved resident metadata. Small
    /// inputs keep the memory fast path; larger matrices and training samples
    /// use quota-accounted read-only mappings. The heap remains the durable
    /// record, so a scan error leaves a non-finalized, rebuildable index.
    fn vector_ivf_paged_build_from_heap(
        &self,
        storage_id: u64,
        cols: &[usize],
        paged: &crate::engine::vector::ivf_paged::PagedIvf,
    ) -> Result<(), QueryError> {
        // Count without decoding vectors before reserving all resident metadata.
        let mut count = 0usize;
        self.with_heap_read(storage_id, |heap| {
            heap.scan_mvcc_columns_until(&[], |row| {
                if !row.header.is_deleted() {
                    count += 1;
                }
                Ok(false)
            })
        })?;
        let _build_gate = hnsw_build_memory_gate()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let full_bytes = count
            .saturating_mul(paged.dim().saturating_mul(12).saturating_add(192))
            .saturating_add(16 * 1024 * 1024);
        if full_bytes <= hnsw_bulk_memory_budget_bytes()? as usize {
            if let Some(_lease) =
                HnswBuildMemoryLease::try_acquire(self.buffer_pool.clone(), full_bytes)
            {
                let mut rows = Vec::with_capacity(count);
                self.with_heap_read(storage_id, |heap| {
                    heap.scan_mvcc_columns_until(cols, |row| {
                        if !row.header.is_deleted() {
                            if let Some(Value::Vector(bytes)) = row.values.first() {
                                rows.push((row.row_id, vector::to_f32(bytes)));
                            }
                        }
                        Ok(false)
                    })
                })?;
                return paged.build_from_rows(&rows);
            }
        }
        let scratch = paged.spill_build_scratch_bytes(count);
        let lease = HnswBuildMemoryLease::try_acquire(self.buffer_pool.clone(), scratch)
            .ok_or_else(|| error(crate::error::catalog::ER_OUTOFMEMORY,
                format!("IVF spill build cannot reserve {scratch} bytes of metadata and training scratch")))?;
        lease.use_streaming_buffer()?;
        let mut spill =
            crate::engine::vector::spill_rows::VectorRowSpill::new(self.data_dir(), paged.dim())?;
        self.with_heap_read(storage_id, |heap| {
            heap.scan_mvcc_columns_until(cols, |row| {
                if !row.header.is_deleted() {
                    if let Some(Value::Vector(bytes)) = row.values.first() {
                        spill.push(
                            row.row_id,
                            &paged.prepare_build_vector(&vector::to_f32(bytes)),
                        )?;
                    }
                }
                Ok(false)
            })
        })?;
        let mapped = spill.finish()?;
        mapped.advise_random();
        let rows = mapped.rows();
        if rows.len() != count {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "IVF build row count changed during scan",
            ));
        }
        paged.build_from_spilled_rows(&rows, self.data_dir())?;
        drop(rows);
        drop(mapped);
        drop(lease);
        self.vector_ivf_paged_build_navigator_from_heap(storage_id, cols, paged)
    }

    /// Rebuild only the optional governed query navigator for an already
    /// finalized `.ivf` file (for example after process restart). The durable
    /// IVF pages are left untouched; allocation pressure merely causes the
    /// navigator to decline publication and list scanning remains available.
    fn vector_ivf_paged_build_navigator_from_heap(
        &self,
        storage_id: u64,
        cols: &[usize],
        paged: &crate::engine::vector::ivf_paged::PagedIvf,
    ) -> Result<(), QueryError> {
        if !crate::engine::vector::ivf_paged::PagedIvf::navigator_enabled() {
            return Ok(());
        }
        let scratch_bytes = paged.live_count().saturating_mul(
            std::mem::size_of::<(RowId, Vec<f32>)>()
                .saturating_add(paged.dim().saturating_mul(std::mem::size_of::<f32>())),
        );
        let Some(_scratch_lease) =
            HnswBuildMemoryLease::try_acquire(self.buffer_pool.clone(), scratch_bytes)
        else {
            log_message("ivf(paged): navigation rebuild skipped; no governed heap-scan scratch");
            return Ok(());
        };
        let mut rows: Vec<(RowId, Vec<f32>)> = Vec::with_capacity(paged.live_count());
        self.with_heap_read(storage_id, |heap| {
            heap.scan_mvcc_columns_until(cols, |row| {
                if !row.header.is_deleted() {
                    if let Some(Value::Vector(bytes)) = row.values.first() {
                        rows.push((row.row_id, vector::to_f32(bytes)));
                    }
                }
                Ok(false)
            })
        })?;
        paged.rebuild_navigator(&rows);
        Ok(())
    }

    /// Paged-backend build/open: open (or create) the `.hvec` graph file —
    /// which *is* the durable graph (buffer pool + WAL) — and, when it has no
    /// nodes yet (a freshly created index, or one created over an
    /// already-populated table), cold-build it by scanning the partition heap.
    /// A non-empty file is returned as-is: it survived restart on its own, so
    /// there is no snapshot to load or delta to catch up (unlike the memory
    /// backend's `.hnsw` path).
    fn vector_graph_paged_load_or_build(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<PagedHnsw, QueryError> {
        let col_idx = table.column_index(&index.columns[0])?;
        let cols = [col_idx];
        let dim = self.vector_paged_resolve_dim(table, col_idx, storage_id)?;
        let path = self.vector_index_paged_path(table, &index.name, storage_id);
        let file = match self.open_vector_index_file(table, index, storage_id, dim) {
            Ok(file) => file,
            Err(err)
                if err.is(crate::error::catalog::ER_GET_ERRNO) && !self.is_snapshot_read_only() =>
            {
                // A paged vector file is a derived candidate structure.  A
                // shadow-partition build can leave a short/unflushed file
                // behind after its warm graph is deliberately evicted; real
                // checksum/layout damage has the same recovery rule. Rebuild
                // from the authoritative heap instead of making DDL or the
                // first KNN query trust the corrupt artifact.
                self.buffer_pool.discard_pages_for_path(&path)?;
                let _ = fs::remove_file(&path);
                self.open_vector_index_file(table, index, storage_id, dim)?
            }
            Err(err) => return Err(err),
        };
        if self.physical_replica_mode() {
            file.disable_navigation_mapping();
        }
        let paged = PagedHnsw::new(Arc::new(file));
        // Build when the file has no finalized graph yet: either a freshly
        // created `.hvec`, or an interrupted/legacy build (non-finalized). A
        // finalized file — even an empty one, or one extended only by
        // incremental DML inserts — is trusted and used as-is.
        if !paged.is_finalized() || !paged.file().meta().query_ready {
            if self.is_snapshot_read_only() {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "paged HNSW coverage requires rebuilding; use exact heap scan",
                ));
            }
            self.ensure_writable()?;
            drop(paged);
            self.buffer_pool.discard_pages_for_path(&path)?;
            let _ = fs::remove_file(&path);
            let file = self.open_vector_index_file(table, index, storage_id, dim)?;
            let rebuilt = PagedHnsw::new(Arc::new(file));
            self.vector_graph_paged_build_from_heap(table, storage_id, &cols, &rebuilt)?;
            return Ok(rebuilt);
        }
        Ok(paged)
    }

    /// Resolve the fixed vector dimension a paged `.hvec` file must be created
    /// with: the column's declared `VECTOR(n)` dimension if present, else
    /// probe the heap for the first visible vector. A dimensionless
    /// (`VECTOR` with no length) column on an empty partition cannot be sized,
    /// so the paged backend rejects it — declare `VECTOR(n)` or build the
    /// index after loading data.
    fn vector_paged_resolve_dim(
        &self,
        table: &TableSchema,
        col_idx: usize,
        storage_id: u64,
    ) -> Result<u32, QueryError> {
        if let DataTypeKind::Vector { dim: Some(d), .. } = table.columns[col_idx].data_type {
            return Ok(d);
        }
        let cols = [col_idx];
        let mut found: Option<u32> = None;
        self.with_heap_read(storage_id, |heap| {
            heap.scan_mvcc_columns_until(&cols, |row| {
                if !row.header.is_deleted() {
                    if let Some(Value::Vector(bytes)) = row.values.first() {
                        if !bytes.is_empty() {
                            found = Some((bytes.len() / 4) as u32);
                            return Ok(true); // stop at the first vector
                        }
                    }
                }
                Ok(false)
            })
        })?;
        found.ok_or_else(|| {
            crate::engine::vector::VectorError::Unsupported(
                "paged HNSW (MYDB_VECTOR_STORAGE=paged) requires a declared VECTOR(n) \
                 dimension or a non-empty table to infer it from"
                    .to_string(),
            )
            .into_query_error()
        })
    }

    /// Open (creating if absent) the `.hvec` file for one paged HNSW index,
    /// wiring it to the shared WAL + buffer pool so its pages participate in
    /// the buffer pool's eviction and the WAL's crash recovery.
    fn open_vector_index_file(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
        dim: u32,
    ) -> Result<VectorIndexFile, QueryError> {
        let meta = index.vector_meta_for_table(table);
        let metric = meta.metric_enum().as_u8();
        let m = meta.m.clamp(2, u32::from(u16::MAX)) as u16;
        let m0 = (u32::from(m) * 2).min(u32::from(u16::MAX)) as u16;
        let ef = meta.ef_construction.clamp(1, u32::from(u16::MAX)) as u16;
        let path = self.vector_index_paged_path(table, &index.name, storage_id);
        self.open_vector_index_file_at_path(path, dim, metric, m, m0, ef)
    }

    fn open_vector_index_file_at_path(
        &self,
        path: PathBuf,
        dim: u32,
        metric: u8,
        m: u16,
        m0: u16,
        ef: u16,
    ) -> Result<VectorIndexFile, QueryError> {
        VectorIndexFile::open_with_durability(
            path,
            self.log.clone(),
            self.durability_mode(),
            self.buffer_pool.clone(),
            dim,
            metric,
            m,
            m0,
            ef,
        )
    }

    /// Cold-build a paged graph from one partition's heap. A statistics pass
    /// chooses full-memory, topology-only compact SQ8, or bounded chunked-paged
    /// construction. Every route holds a governed StorageScratch lease and
    /// keeps the file non-finalized until all topology and exact-vector writes
    /// have succeeded.
    fn vector_graph_paged_build_from_heap(
        &self,
        table: &TableSchema,
        storage_id: u64,
        cols: &[usize],
        paged: &PagedHnsw,
    ) -> Result<(), QueryError> {
        let (mut txn, _snapshot) = self.borrowed_read_only_statement_view();
        // A DDL-owned shadow heap contains the copying transaction's own
        // rows. Its catalog generation remains protected by the DDL barrier.
        if table.row_generation_txn_id != 0 {
            txn.txn_id = table.row_generation_txn_id;
        }
        self.vector_snapshot_floor
            .fetch_max(txn.snapshot_scn, std::sync::atomic::Ordering::AcqRel);

        paged.file().advance_coverage(txn.snapshot_scn);
        paged.begin_bulk_build();
        let timing = std::env::var("MYDB_HNSW_BUILD_TIMING").is_ok();
        let total_started = Instant::now();
        let threads = hnsw_build_threads();
        let (metric, m, ef_construction) = paged.memory_build_params();

        // Pass 1: exact cardinality/dimension/range. This makes every later
        // allocation and governor reservation capacity-based.
        let stats_started = Instant::now();
        let mut stats = CompactHnswStats::new(metric);
        self.scan_hnsw_build_rows(
            table,
            storage_id,
            txn,
            cols,
            0,
            u64::MAX,
            |_row_id, values| {
                if let Some(Value::Vector(bytes)) = values.first() {
                    stats.observe(&vector::to_f32(bytes));
                }

                Ok(false)
            },
        )?;
        if stats.rows() == 0 {
            paged.finalize_bulk_build()?;
            paged.record_build_stats(HnswBuildStats {
                mode: "empty",
                stats_ms: stats_started.elapsed().as_millis() as u64,
                total_ms: total_started.elapsed().as_millis() as u64,
                ..HnswBuildStats::default()
            });
            return Ok(());
        }
        if stats.dim() != paged.dim() {
            return Err(error(
                crate::error::catalog::MYDB_ER_VECTOR_DIMENSION_MISMATCH,
                format!(
                    "HNSW build observed dimension {} but index expects {}",
                    stats.dim(),
                    paged.dim()
                ),
            ));
        }
        // Persist the SQ8 calibration before any node image is written. Every
        // build route now materialises its navigation row with the exact
        // vector, so the first query never performs an index-wide arena scan.
        paged.set_navigation_quantization(stats.quantization_params())?;

        let raw_mode = std::env::var("MYDB_HNSW_BULK_MODE")
            .unwrap_or_else(|_| "auto".to_string())
            .to_ascii_lowercase();
        if !matches!(raw_mode.as_str(), "auto" | "memory" | "chunked" | "stream") {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                format!("invalid MYDB_HNSW_BULK_MODE value '{raw_mode}'"),
            ));
        }
        let budget = hnsw_bulk_memory_budget_bytes()?
            .min(crate::engine::executor::mem_budget::memory_target_bytes() as u64)
            as usize;
        let _ = hnsw_positive_mb_env("MYDB_HNSW_BUILD_CACHE_MB")?;
        let compact_bytes = stats.estimated_bytes(m, threads);
        let exact_vectors = stats
            .rows()
            .saturating_mul(stats.dim())
            .saturating_mul(std::mem::size_of::<f32>());
        let full_bytes = compact_bytes
            .saturating_add(exact_vectors)
            .saturating_add(stats.rows().saturating_mul(64));

        let _build_gate = hnsw_build_memory_gate()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut lease: Option<HnswBuildMemoryLease> = None;
        let preferred = hnsw_preferred_cold_build_mode(
            &raw_mode,
            full_bytes,
            compact_bytes,
            budget,
            stats.quantization_params().is_some(),
        );
        let mut build_mode = match preferred {
            HnswColdBuildMode::FullMemory if full_bytes > budget => {
                return Err(error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    format!(
                        "HNSW full-memory build requires {} bytes, configured build budget is {} bytes",
                        full_bytes, budget
                    ),
                ));
            }
            HnswColdBuildMode::FullMemory => {
                lease = HnswBuildMemoryLease::try_acquire(self.buffer_pool.clone(), full_bytes);
                if lease.is_some() {
                    HnswColdBuildMode::FullMemory
                } else if raw_mode == "memory" {
                    return Err(error(
                        crate::error::catalog::ER_OUTOFMEMORY,
                        "HNSW full-memory build cannot reserve memory within MYDB_MEMORY_TARGET",
                    ));
                } else if stats.quantization_params().is_some() && compact_bytes <= budget {
                    lease =
                        HnswBuildMemoryLease::try_acquire(self.buffer_pool.clone(), compact_bytes);
                    if lease.is_some() {
                        HnswColdBuildMode::CompactSq8
                    } else {
                        HnswColdBuildMode::ChunkedPaged
                    }
                } else {
                    HnswColdBuildMode::ChunkedPaged
                }
            }
            HnswColdBuildMode::CompactSq8 => {
                lease = HnswBuildMemoryLease::try_acquire(self.buffer_pool.clone(), compact_bytes);
                if lease.is_some() {
                    HnswColdBuildMode::CompactSq8
                } else {
                    HnswColdBuildMode::ChunkedPaged
                }
            }
            HnswColdBuildMode::ChunkedPaged => HnswColdBuildMode::ChunkedPaged,
        };
        if !paged.supports_direct_bulk_build() && build_mode != HnswColdBuildMode::ChunkedPaged {
            drop(lease.take());
            build_mode = HnswColdBuildMode::ChunkedPaged;
        }

        if timing {
            eprintln!(
                "[paged] build plan: mode={build_mode:?} rows={} dim={} stats={:?} reserved={} budget={}",
                stats.rows(),
                stats.dim(),
                stats_started.elapsed(),
                lease.as_ref().map_or(0, HnswBuildMemoryLease::bytes),
                budget,
            );
        }

        let mut build_stats = HnswBuildStats {
            mode: match build_mode {
                HnswColdBuildMode::FullMemory => "full-memory",
                HnswColdBuildMode::CompactSq8 => "compact-sq8",
                HnswColdBuildMode::ChunkedPaged => "chunked-paged",
            },
            rows: stats.rows() as u64,
            stats_ms: stats_started.elapsed().as_millis() as u64,
            peak_scratch_bytes: lease.as_ref().map_or(0, HnswBuildMemoryLease::bytes) as u64,
            ..HnswBuildStats::default()
        };
        match build_mode {
            HnswColdBuildMode::FullMemory => {
                let graph = HnswIndex::new(metric, m, ef_construction).with_build_reservation(
                    lease.as_mut().expect("admitted full build").take_charge(),
                    stats.rows(),
                );
                let scan_started = Instant::now();
                self.vector_graph_append_from_heap(table, txn, storage_id, cols, &graph, threads)?;
                build_stats.encode_ms = scan_started.elapsed().as_millis() as u64;
                if timing {
                    eprintln!(
                        "[paged] direct heap append: {:?} rows={}",
                        scan_started.elapsed(),
                        graph.build_node_count()
                    );
                }
                let link_started = Instant::now();
                graph.link_all(threads);
                crate::engine::cancel::check_cancelled()?;
                build_stats.link_ms = link_started.elapsed().as_millis() as u64;
                let materialize_started = Instant::now();
                paged.materialize_memory_graph(&graph)?;
                build_stats.materialize_ms = materialize_started.elapsed().as_millis() as u64;
                drop(graph);
            }
            HnswColdBuildMode::CompactSq8 => {
                let mut builder = CompactHnswBuilder::new(
                    stats.clone(),
                    m,
                    ef_construction,
                    lease
                        .as_mut()
                        .expect("admitted compact build")
                        .take_charge(),
                )
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_OUTOFMEMORY,
                        "HNSW compact build is not available for this vector distribution",
                    )
                })?;
                let encode_started = Instant::now();
                self.scan_hnsw_build_rows(
                    table,
                    storage_id,
                    txn,
                    cols,
                    0,
                    u64::MAX,
                    |row_id, values| {
                        if let Some(Value::Vector(bytes)) = values.first() {
                            let values = vector::to_f32(bytes);
                            builder.push(row_id, &values).map_err(|message| {
                                error(crate::error::catalog::ER_GET_ERRNO, message)
                            })?;
                        }

                        Ok(false)
                    },
                )?;
                build_stats.encode_ms = encode_started.elapsed().as_millis() as u64;
                let link_started = Instant::now();
                let graph = builder
                    .finish(threads)
                    .map_err(|message| error(crate::error::catalog::ER_GET_ERRNO, message))?;
                build_stats.link_ms = link_started.elapsed().as_millis() as u64;
                let materialize_started = Instant::now();
                self.vector_graph_materialize_compact_from_heap(
                    table, txn, storage_id, cols, paged, &graph,
                )?;
                build_stats.materialize_ms = materialize_started.elapsed().as_millis() as u64;
                drop(graph);
            }
            _ => {
                drop(lease.take());
                let chunked = self.vector_graph_paged_build_chunked(
                    table, txn, storage_id, cols, paged, &stats, threads,
                )?;
                build_stats.link_ms = chunked.link_ms;
                build_stats.materialize_ms = chunked.materialize_ms;
                build_stats.peak_scratch_bytes = chunked.reserved_bytes;
                build_stats.cache_hits = chunked.cache_hits;
                build_stats.cache_misses = chunked.cache_misses;
                build_stats.chunks = chunked.chunks;
            }
        }
        // The compact SQ8/CSR cache is optional, governed derived memory. Build
        // it only after releasing the larger construction reservation so
        // CREATE INDEX does not transiently double-charge both workspaces.
        drop(lease.take());
        paged.refresh_query_navigation()?;
        build_stats.total_ms = total_started.elapsed().as_millis() as u64;
        if timing {
            eprintln!(
                "[paged] build complete: mode={} rows={} stats_ms={} encode_ms={} \
                 link_ms={} materialize_ms={} total_ms={} peak_scratch_bytes={} \
                 cache_hits={} cache_misses={} chunks={}",
                build_stats.mode,
                build_stats.rows,
                build_stats.stats_ms,
                build_stats.encode_ms,
                build_stats.link_ms,
                build_stats.materialize_ms,
                build_stats.total_ms,
                build_stats.peak_scratch_bytes,
                build_stats.cache_hits,
                build_stats.cache_misses,
                build_stats.chunks,
            );
        }
        paged.record_build_stats(build_stats);
        Ok(())
    }

    fn vector_graph_materialize_compact_from_heap(
        &self,
        table: &TableSchema,
        txn: TxnView,
        storage_id: u64,
        cols: &[usize],
        paged: &PagedHnsw,
        graph: &HnswIndex,
    ) -> Result<(), QueryError> {
        type ExactRow = (RowId, Vec<f32>);
        std::thread::scope(|scope| -> Result<(), QueryError> {
            let (sender, receiver) = std::sync::mpsc::sync_channel::<ExactRow>(8);
            let scanner = scope.spawn(move || -> Result<usize, QueryError> {
                let mut sent = 0usize;
                self.scan_hnsw_build_rows(
                    table,
                    storage_id,
                    txn,
                    cols,
                    0,
                    u64::MAX,
                    |row_id, values| {
                        if let Some(Value::Vector(bytes)) = values.first() {
                            if sender.send((row_id, vector::to_f32(bytes))).is_err() {
                                return Ok(true);
                            }
                            sent += 1;
                        }

                        Ok(false)
                    },
                )?;
                Ok(sent)
            });

            let mut received = 0usize;
            let materialized =
                paged.materialize_compact_graph(graph, |id, expected_row_id, vector_out| {
                    let (row_id, vector) = receiver.recv().map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "compact HNSW exact-vector scan ended early",
                        )
                    })?;
                    if id != received || row_id != expected_row_id {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "compact HNSW heap order changed between build passes",
                        ));
                    }
                    vector_out.clear();
                    vector_out.extend_from_slice(&vector);
                    received += 1;
                    Ok(())
                });
            drop(receiver);
            let scanned = scanner.join().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "compact HNSW exact-vector scanner panicked",
                )
            })??;
            materialized?;
            if scanned != graph.build_node_count() || received != graph.build_node_count() {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "compact HNSW row count changed during build: topology={}, scanned={}, written={}",
                        graph.build_node_count(),
                        scanned,
                        received
                    ),
                ));
            }
            Ok(())
        })
    }

    fn vector_graph_paged_build_chunked(
        &self,
        table: &TableSchema,
        txn: TxnView,
        storage_id: u64,
        cols: &[usize],
        paged: &PagedHnsw,
        stats: &CompactHnswStats,
        threads: usize,
    ) -> Result<HnswChunkBuildStats, QueryError> {
        let target = crate::engine::executor::mem_budget::memory_target_bytes();
        let explicit_cache = hnsw_positive_mb_env("MYDB_HNSW_BUILD_CACHE_MB")?;
        let mut scratch_bytes = hnsw_bulk_memory_budget_bytes()?
            .min(hnsw_chunked_scratch_cap(target) as u64)
            .max((8 * 1024 * 1024) as u64) as usize;
        let lease = loop {
            if let Some(lease) =
                HnswBuildMemoryLease::try_acquire(self.buffer_pool.clone(), scratch_bytes)
            {
                break lease;
            }
            if scratch_bytes <= 8 * 1024 * 1024 {
                return Err(error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    "HNSW chunked build cannot reserve its minimum scratch within MYDB_MEMORY_TARGET",
                ));
            }
            scratch_bytes = (scratch_bytes / 2).max(8 * 1024 * 1024);
        };
        if paged.chunk_vector_bytes_per_dimension(stats.quantization_params().is_some())
            == std::mem::size_of::<f32>() + std::mem::size_of::<i8>()
        {
            lease.use_streaming_buffer()?;
        }
        // Resident metadata owned by the external adjacency sidecar.
        // Every vector is retained once in the monotonic VecId map, actual
        // level table, page hints, SQ8 norms and variable-record offset table.
        // The store preallocates these arrays to `stats.rows()`, so capacity,
        // rather than current length, is governed for the whole build lifetime.
        let sidecar_meta_bytes = stats
            .rows()
            .saturating_mul(BUILD_NODE_METADATA_BYTES)
            .saturating_add(2 * 1024 * 1024);
        let usable_bytes = scratch_bytes.checked_sub(sidecar_meta_bytes).ok_or_else(|| {
            error(
                crate::error::catalog::ER_OUTOFMEMORY,
                format!(
                    "HNSW chunked build needs {} bytes of resident sidecar metadata but its scratch reservation is {} bytes",
                    sidecar_meta_bytes, scratch_bytes
                ),
            )
        })?;
        let cache_bytes = explicit_cache
            .unwrap_or(usable_bytes * 2 / 5)
            .min(usable_bytes * 2 / 5);
        let chunk_bytes = usable_bytes.saturating_sub(cache_bytes);
        // Exact fallback retains both input rows and an f32 navigation arena.
        // SQ8 builds encode directly from input rows, with one row of cosine
        // normalization scratch; never budget only the steady-state cache.
        let bytes_per_row = stats
            .dim()
            .saturating_mul(
                paged.chunk_vector_bytes_per_dimension(stats.quantization_params().is_some()),
            )
            .saturating_add(192)
            .max(1);
        if chunk_bytes < bytes_per_row {
            return Err(error(
                crate::error::catalog::ER_OUTOFMEMORY,
                format!(
                    "HNSW chunked build needs at least {} bytes for one vector row but only {} bytes are available",
                    bytes_per_row, chunk_bytes
                ),
            ));
        }
        let chunk_rows = (chunk_bytes / bytes_per_row).max(1);
        // Demand-paged workers need a meaningful persistent cache share.
        // Spawning more workers than the cache can support only multiplies
        // duplicate hot entries and buffer-pool CLOCK contention. Keep at
        // least 4 MiB of the shared cache envelope per worker.
        let cache_limited_threads = (cache_bytes / (4 * 1024 * 1024)).max(1);
        let chunk_threads = threads.min(cache_limited_threads);
        if std::env::var("MYDB_HNSW_BUILD_TIMING").is_ok() {
            eprintln!("[paged] chunk reservation: rows={} chunk_rows={} workers={} scratch={} cache={} buffer_pages={} controlled={:?} private={:?}", stats.rows(), chunk_rows, chunk_threads, scratch_bytes, cache_bytes, self.buffer_pool.capacity_pages(), crate::engine::executor::mem_budget::controlled_memory_snapshot(), crate::engine::memory_reclaimer::current_private_footprint_bytes());
        }
        let mut session = paged.build_session(
            chunk_threads,
            cache_bytes,
            stats.quantization_params(),
            stats.rows(),
        )?;
        let mut rows: Vec<(RowId, Vec<f32>)> = Vec::with_capacity(chunk_rows);
        let mut chunks = 0usize;
        let link_started = Instant::now();
        self.scan_hnsw_build_rows(
            table,
            storage_id,
            txn,
            cols,
            0,
            u64::MAX,
            |row_id, values| {
                if let Some(Value::Vector(bytes)) = values.first() {
                    rows.push((row_id, vector::to_f32(bytes)));
                    if rows.len() >= chunk_rows {
                        paged.insert_bulk_chunk_parallel(&mut rows, chunk_threads, &mut session)?;
                        rows.clear();
                        // Workers have joined and decoded vectors are gone.
                        // Return allocator-retained pages before refilling
                        // this chunk while the statement remains active.
                        crate::engine::memory_reclaimer::reclaim_bulk_operation_pressure();
                        chunks += 1;
                    }
                }

                Ok(false)
            },
        )?;
        if !rows.is_empty() {
            paged.insert_bulk_chunk_parallel(&mut rows, chunk_threads, &mut session)?;
            rows.clear();
            crate::engine::memory_reclaimer::reclaim_bulk_operation_pressure();
            chunks += 1;
        }
        if std::env::var("MYDB_HNSW_BUILD_TIMING").is_ok() {
            eprintln!(
                "[paged] chunked build: rows={} chunks={} chunk_rows={} workers={} reserved={} cache={}",
                stats.rows(),
                chunks,
                chunk_rows,
                chunk_threads,
                lease.bytes(),
                cache_bytes
            );
        }
        let (cache_hits, cache_misses) = session.cache_stats();
        let link_ms = link_started.elapsed().as_millis() as u64;
        let materialize_started = Instant::now();
        session.release_link_scratch();
        crate::engine::memory_reclaimer::reclaim_bulk_operation_pressure();
        paged.materialize_build_adjacency(&session)?;
        paged.finalize_bulk_build()?;
        let materialize_ms = materialize_started.elapsed().as_millis() as u64;
        Ok(HnswChunkBuildStats {
            reserved_bytes: lease.bytes() as u64,
            cache_hits,
            cache_misses,
            chunks: chunks as u64,
            link_ms,
            materialize_ms,
        })
    }

    /// All build passes resolve the same committed snapshot, including undo.
    #[allow(clippy::too_many_arguments)]
    fn scan_hnsw_build_rows(
        &self,
        table: &TableSchema,
        storage_id: u64,
        txn: TxnView,
        columns: &[usize],
        lo: u64,
        hi: u64,
        mut visit: impl FnMut(RowId, &[Value]) -> Result<bool, QueryError>,
    ) -> Result<(), QueryError> {
        crate::engine::cancel::check_cancelled()?;
        let mut stopped = false;
        let mut observed = 0usize;
        self.scan_rows_columns_storage_streaming_range_with_ids_impl(
            table,
            storage_id,
            txn,
            columns,
            lo,
            hi,
            false,
            |row_id, values| {
                if !stopped {
                    if observed & 1023 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    observed += 1;
                    stopped = visit(row_id, values)?;
                }
                Ok(())
            },
        )
    }

    /// Walk one storage_id's heap (one partition, or the whole table for
    /// unpartitioned cases) and reconcile its vectors with `graph`.
    /// Dispatches between a single-threaded serial
    /// scan and a `std::thread::scope`-driven parallel page-range fan-out
    /// based on `threads` and the page count — small heaps stay serial so
    /// spawn / join overhead never dominates.
    fn vector_graph_populate_from_heap(
        &self,
        table: &TableSchema,
        storage_id: u64,
        cols: &[usize],
        graph: &HnswIndex,
        threads: usize,
    ) -> Result<(), QueryError> {
        let (mut txn, _snapshot) = self.borrowed_read_only_statement_view();
        // A DDL-owned shadow heap contains the copying transaction's own
        // rows. Its catalog generation remains protected by the DDL barrier.
        if table.row_generation_txn_id != 0 {
            txn.txn_id = table.row_generation_txn_id;
        }
        self.vector_snapshot_floor
            .fetch_max(txn.snapshot_scn, std::sync::atomic::Ordering::AcqRel);

        // Two-phase cold build vs incremental catch-up. A fresh graph (no nodes
        // yet) takes the fast path: append every row first (cheap, lock-free),
        // build the SQ8 navigation arena over the complete set, then link the
        // whole graph on the contiguous int8 codes. A graph that already has
        // nodes (snapshot loaded → catch-up) must link *new* rows incrementally
        // so `link_all` doesn't double-wire the loaded ones.
        if graph.node_count() == 0 {
            let timing = std::env::var("MYDB_HNSW_BUILD_TIMING").is_ok();
            let t_a = std::time::Instant::now();
            self.vector_graph_append_from_heap(table, txn, storage_id, cols, graph, threads)?;
            if timing {
                eprintln!("[hnsw] PhaseA append-from-heap: {:?}", t_a.elapsed());
            }
            graph.link_all(threads);
            crate::engine::cancel::check_cancelled()?;
            return Ok(());
        }

        // Catch-up: retain unchanged live nodes and re-link changed/new rows.
        self.vector_graph_insert_from_heap(table, txn, storage_id, cols, graph, threads)
    }

    /// Phase A of the cold build: append every visible vector row into `graph`
    /// as an *unlinked* node (lock-free `push`, no graph navigation). Splits the
    /// heap into disjoint `[page_lo, page_hi)` ranges across `threads` workers —
    /// disjoint ranges mean no row is decoded twice, and `with_heap_read` is a
    /// shared `RwLock` so workers don't serialise on the heap.
    fn vector_graph_append_from_heap(
        &self,
        table: &TableSchema,
        txn: TxnView,
        storage_id: u64,
        cols: &[usize],
        graph: &HnswIndex,
        threads: usize,
    ) -> Result<(), QueryError> {
        let page_count = self.with_heap_read(storage_id, |heap| heap.page_count())?;
        if page_count == 0 {
            return Ok(());
        }
        let min_pages = hnsw_build_min_pages_per_worker();
        let max_by_pages = (page_count / min_pages).max(1) as usize;
        let effective = threads.min(max_by_pages).max(1);
        if effective <= 1 {
            return self.scan_hnsw_build_rows(
                table,
                storage_id,
                txn,
                cols,
                0,
                u64::MAX,
                |row_id, values| {
                    if let Some(Value::Vector(bytes)) = values.first() {
                        graph.append_node(row_id, vector::to_f32(bytes))?;
                    }

                    Ok(false)
                },
            );
        }

        let chunk = page_count.div_ceil(effective as u64);
        std::thread::scope(|scope| -> Result<(), QueryError> {
            let mut handles = Vec::with_capacity(effective);
            let mut lo = 0u64;
            while lo < page_count {
                let hi = (lo + chunk).min(page_count);
                let cols_ref = cols;
                let graph_ref = graph;
                let h = scope.spawn(move || -> Result<(), QueryError> {
                    self.scan_hnsw_build_rows(
                        table,
                        storage_id,
                        txn,
                        cols_ref,
                        lo,
                        hi,
                        |row_id, values| {
                            if let Some(Value::Vector(bytes)) = values.first() {
                                graph_ref.append_node(row_id, vector::to_f32(bytes))?;
                            }

                            Ok(false)
                        },
                    )
                });
                handles.push(h);
                lo = hi;
            }
            for h in handles {
                h.join().map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "hnsw build worker panicked",
                    )
                })??;
            }
            Ok(())
        })
    }

    /// Compare heap vectors with snapshot nodes and re-link only changes;
    /// snapshot nodes whose row no longer exists in the heap are tombstoned.
    fn vector_graph_insert_from_heap(
        &self,
        table: &TableSchema,
        txn: TxnView,
        storage_id: u64,
        cols: &[usize],
        graph: &HnswIndex,
        threads: usize,
    ) -> Result<(), QueryError> {
        let marks = graph.begin_heap_reconcile();
        let marks = marks.as_slice();
        let page_count = self.with_heap_read(storage_id, |heap| heap.page_count())?;
        // Cap the worker count by both the configured GUC and the available
        // work — a 5-page heap with 8 configured workers would otherwise
        // spawn 5 ones with one page each, leaving most of the spawn cost
        // unamortised.
        let min_pages = hnsw_build_min_pages_per_worker();
        let max_by_pages = (page_count / min_pages).max(1) as usize;
        let effective = threads.min(max_by_pages).max(1);
        let scan_range = |lo: u64, hi: u64| -> Result<(), QueryError> {
            self.scan_hnsw_build_rows(table, storage_id, txn, cols, lo, hi, |row_id, values| {
                if let Some(Value::Vector(bytes)) = values.first() {
                    graph.reconcile_heap_vector_marked(
                        row_id,
                        vector::to_f32(bytes),
                        Some(marks),
                    )?;
                }

                Ok(false)
            })
        };
        if effective <= 1 {
            // Unbounded upper page: rows appended during the scan are still
            // visited (their nodes are new and need no mark).
            scan_range(0, u64::MAX)?;
        } else {
            let chunk = page_count.div_ceil(effective as u64);
            std::thread::scope(|scope| -> Result<(), QueryError> {
                let mut handles = Vec::with_capacity(effective);
                let mut lo = 0u64;
                while lo < page_count {
                    let hi = (lo + chunk).min(page_count);
                    let hi = if hi == page_count { u64::MAX } else { hi };
                    let scan_range = &scan_range;
                    handles.push(scope.spawn(move || scan_range(lo, hi)));
                    lo = hi;
                }
                for h in handles {
                    h.join().map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "hnsw build worker panicked",
                        )
                    })??;
                }
                Ok(())
            })?;
        }
        graph.finish_heap_reconcile(marks);
        Ok(())
    }

    /// Brute-force exact KNN over a `VECTOR(n)` column that has **no** usable
    /// secondary (HNSW) index — the no-index `ORDER BY DISTANCE(col, q)
    /// LIMIT k` path. Returns up to `k` `(ordering_distance, row_id)` pairs
    /// ascending by `(distance, row_id)`.
    ///
    /// This is the zero-copy heart of the brute path: each visible row's
    /// fixed-width vector bytes are located in place inside the heap payload
    /// (`fixed_column_byte_range_in_body`) and scored with the SIMD
    /// `ordering_distance_bytes` kernel — **no `Value::Vector` heap copy and
    /// no full-row `Vec<Value>` materialisation per row**, which differential
    /// profiling pinned as the real cost of the old row-pipeline brute scan
    /// (the distance math itself was already SIMD-fast). Only the K winners
    /// are re-read as full rows by the caller.
    ///
    /// MVCC visibility, the deferred (invisible-top-version) re-read, and
    /// partition fan-out reuse the same machinery as
    /// [`Self::scan_rows_payloads_streaming_range`]. For an unpartitioned
    /// heap the page range is split across `vector_brute_scan_threads()`
    /// workers (each with a private `BruteTopK`, merged at the end) — pgvector
    /// runs its KNN seq scan single-threaded, so this is where mydb pulls
    /// ahead.
    pub(crate) fn vector_brute_knn(
        &self,
        table: &TableSchema,
        txn: TxnView,
        col_idx: usize,
        query: &crate::vector::VectorValue,
        metric: vector::Metric,
        k: usize,
    ) -> Result<Vec<(f64, u64, RowId)>, QueryError> {
        if k == 0 || query.is_empty() {
            return Ok(Vec::new());
        }
        let cols = [col_idx];
        let storage_ids = partition_storage_ids_for_scan(table);

        // Build the work units: (storage_id, page_lo, page_hi). An
        // unpartitioned table is one heap whose pages we split across
        // workers; a partitioned table fans out one unit per partition
        // (each partition has its own page numbering, so a global page
        // range is meaningless — see `scan_rows_payloads_streaming_range`).
        let mut units: Vec<(u64, u64, u64)> = Vec::new();
        if storage_ids.len() == 1 {
            let sid = storage_ids[0];
            let page_count = self.with_heap_read(sid, |heap| heap.page_count())?;
            if page_count == 0 {
                return Ok(Vec::new());
            }
            let threads = vector_brute_scan_threads();
            let min_pages = vector_brute_min_pages_per_worker();
            let max_by_pages = (page_count / min_pages).max(1) as usize;
            let effective = threads.min(max_by_pages).max(1);
            let chunk = page_count.div_ceil(effective as u64);
            let mut lo = 0u64;
            while lo < page_count {
                let hi = (lo + chunk).min(page_count);
                units.push((sid, lo, hi));
                lo = hi;
            }
        } else {
            for sid in storage_ids {
                units.push((sid, 0, u64::MAX));
            }
        }

        // One admitted scan envelope per operator, divided across workers.
        // Reserving a full 16 MiB batch independently in every worker caused
        // large anonymous-memory bursts alongside an already warm ANN index.
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        let workers = units.len().min(vector_brute_scan_threads()).max(1);
        let mut bytes = (16 * 1024 * 1024usize).max(workers * PAGE_SIZE);
        let mut envelope = loop {
            match ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, bytes) {
                Ok(charge) => break charge,
                Err(_) if bytes > workers * PAGE_SIZE => {
                    bytes = (bytes / 2).max(workers * PAGE_SIZE)
                }
                Err(error) => return Err(error),
            }
        };
        if units.len() <= 1 {
            let mut top = BruteTopK::new(k);
            let (sid, lo, hi) = units.first().copied().unwrap_or((table.id, 0, u64::MAX));
            self.vector_brute_scan_range(
                table,
                txn,
                &cols,
                query,
                metric,
                sid,
                lo,
                hi,
                &mut envelope,
                &mut top,
            )?;
            return Ok(top.into_sorted());
        }
        let group_size = units.len().div_ceil(workers);
        let per_worker = (bytes / workers / PAGE_SIZE).max(1) * PAGE_SIZE;
        let partials: Vec<Result<BruteTopK, QueryError>> = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for group in units.chunks(group_size) {
                let mut charge = envelope.split(per_worker);
                let cols_ref = &cols;
                handles.push(scope.spawn(move || -> Result<BruteTopK, QueryError> {
                    let mut top = BruteTopK::new(k);
                    for &(sid, lo, hi) in group {
                        self.vector_brute_scan_range(
                            table,
                            txn,
                            cols_ref,
                            query,
                            metric,
                            sid,
                            lo,
                            hi,
                            &mut charge,
                            &mut top,
                        )?;
                    }
                    Ok(top)
                }));
            }
            handles
                .into_iter()
                .map(|handle| {
                    handle.join().unwrap_or_else(|_| {
                        Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "brute KNN worker panicked",
                        ))
                    })
                })
                .collect()
        });

        let mut merged = BruteTopK::new(k);
        for partial in partials {
            merged.merge(partial?);
        }
        Ok(merged.into_sorted())
    }

    /// Scan one heap page range `[page_lo, page_hi)` of `storage_id`, scoring
    /// the `col_idx` vector against `query` and feeding `(distance, row_id)`
    /// into `top`. Visibility + deferred-row handling mirror
    /// [`Self::scan_rows_payloads_streaming_range`] exactly; the only
    /// addition is the in-place distance scoring and the `row_id` we need to
    /// re-materialise winners.
    #[allow(clippy::too_many_arguments)]
    fn vector_brute_scan_range(
        &self,
        table: &TableSchema,
        txn: TxnView,
        cols: &[usize],
        query: &crate::vector::VectorValue,
        metric: vector::Metric,
        storage_id: u64,
        page_lo: u64,
        page_hi: u64,
        scan_charge: &mut crate::engine::executor::mem_budget::ControlledMemoryCharge,
        top: &mut BruteTopK,
    ) -> Result<(), QueryError> {
        let col_idx = cols[0];
        let mut vis_cache = ScanVisibilityCache::new();
        let mut deferred: Vec<RowId> = Vec::new();
        // SME is currently an exact-L2 acceleration. DOT/COSINE retain their
        // existing fused NEON kernels; default-off keeps the old path as a
        // differential oracle. Packing is bounded per worker and amortises one
        // SMSTART/SMSTOP over many candidates.
        let use_sme = query.datatype == crate::vector::VectorDataType::Float32
            && matches!(
                table.columns[col_idx].data_type,
                DataTypeKind::Vector {
                    datatype: crate::vector::VectorDataType::Float32,
                    ..
                }
            )
            && metric == vector::Metric::L2
            && vector::distance::brute_sme_enabled();
        let sme_batch_rows = std::env::var("MYDB_VECTOR_BRUTE_SME_BATCH_ROWS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(512)
            .clamp(16, 4096);
        let sme_capacity = if use_sme { sme_batch_rows } else { 0 };
        let _sme_charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
            sme_capacity
                .saturating_mul(query.len().saturating_add(std::mem::size_of::<RowId>() + 4)),
        )?;
        let mut sme_rows = Vec::<u8>::with_capacity(query.len().saturating_mul(sme_capacity));
        let mut sme_ids = Vec::<RowId>::with_capacity(sme_capacity);
        let mut sme_dist = vec![0.0f32; sme_capacity];
        let flush_sme = |rows: &mut Vec<u8>,
                         ids: &mut Vec<RowId>,
                         dist: &mut [f32],
                         top: &mut BruteTopK|
         -> Result<(), QueryError> {
            if ids.is_empty() {
                return Ok(());
            }
            let n = ids.len();
            if !vector::distance::l2_sq_sme_batch(query, rows, &mut dist[..n]) {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "SME brute-KNN batch dispatch failed",
                ));
            }
            for (i, (&rid, &d)) in ids.iter().zip(&dist[..n]).enumerate() {
                let score = if d.is_normal() {
                    d as f64
                } else {
                    vector::distance::ordering_distance_bytes(
                        query,
                        &rows[i * query.len()..(i + 1) * query.len()],
                        metric,
                    )
                };
                top.push(score, storage_id, rid);
            }
            rows.clear();
            ids.clear();
            Ok(())
        };
        self.with_heap_read(storage_id, |heap| {
            // Reuse the general coherent bulk reader: it overlays resident dirty
            // pages and reads cold gaps in bounded contiguous batches. A brute
            // scan has no reuse within the operator and must not cycle every
            // wide-vector page through shared CLOCK/prefetch admission.
            heap.scan_mvcc_payloads_direct_range_with_charge_until(
                cols,
                page_lo,
                page_hi,
                scan_charge,
                |row| {
                    let visible =
                        self.row_version_visible_cached(&mut vis_cache, row.header, txn)?;
                    if !visible {
                        deferred.push(row.row_id);
                        return Ok(false);
                    }
                    if row.header.is_deleted() {
                        return Ok(false);
                    }
                    let body = &row.payload[row.body_offset..];
                    let key = match fixed_column_byte_range_in_body(
                        body,
                        row.codec,
                        row.layout_id,
                        row.has_external,
                        col_idx,
                    )? {
                        Some(range) => {
                            let vb = &body
                                [range.offset_in_payload..range.offset_in_payload + range.width];
                            if use_sme {
                                sme_rows.extend_from_slice(vb);
                                sme_ids.push(row.row_id);
                                if sme_ids.len() == sme_batch_rows {
                                    flush_sme(&mut sme_rows, &mut sme_ids, &mut sme_dist, top)?;
                                }
                                return Ok(false);
                            }
                            {
                                let DataTypeKind::Vector {
                                    dim: Some(dimension),
                                    datatype,
                                } = table.columns[col_idx].data_type
                                else {
                                    return Err(vector::vector_err("invalid vector column layout"));
                                };
                                let v = crate::vector::VectorRef {
                                    datatype,
                                    dimension,
                                    payload: vb,
                                };
                                vector::distance::typed_ordering_distance(query, &v, metric)?
                            }
                        }
                        // Dimension drift on a VECTOR(n) column is impossible
                        // for a well-formed table (write path + planner both pin
                        // the dim); skip defensively rather than scoring garbage.
                        // NULL or dimensionless vector → NULL distance, which
                        // sorts first under ASC (NULLs-first), matching the
                        // row-based oracle.
                        None => {
                            deferred.push(row.row_id);
                            return Ok(false);
                        }
                    };
                    top.push(key, storage_id, row.row_id);
                    Ok(false)
                },
            )?;
            if use_sme {
                flush_sme(&mut sme_rows, &mut sme_ids, &mut sme_dist, top)?;
            }
            Ok(())
        })?;

        // Rows whose top version was invisible to this snapshot: resolve the
        // visible version via undo and score it. Rare on a read-mostly KNN
        // workload, so the per-row `Vec<Value>` here is not on the hot path.
        for rid in deferred {
            if let Some(values) = self.read_row_columns(table, rid, txn, cols)? {
                let key = match values.first() {
                    Some(Value::Vector(b)) if b.dimension == query.dimension => {
                        vector::distance::typed_ordering_distance(query, b, metric)?
                    }
                    Some(Value::Vector(_)) => continue,
                    _ => f64::NEG_INFINITY,
                };
                top.push(key, storage_id, rid);
            }
        }
        Ok(())
    }

    /// Get the cached HNSW graph for a vector index on a *single partition*,
    /// building it from a heap scan (or loading the on-disk snapshot +
    /// delta-scanning) on first access. The build holds only heap read locks
    /// (never the graph map lock while scanning), so it cannot deadlock
    /// against the DML insert hook, which holds heap locks and then briefly
    /// takes the map lock.
    ///
    /// `storage_id` keys per-partition graphs — pass `table.id` for
    /// unpartitioned tables.
    ///
    /// Returns a bare `Arc<HnswIndex>` — the graph is internally concurrent
    /// (M3, per-node latches), so callers don't take any outer lock to
    /// read or mutate it.
    pub(crate) fn vector_graph_get_or_build_for_partition(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<Arc<VectorGraph>, QueryError> {
        let key = (
            table.id,
            storage_id,
            table.active_generation.max(1),
            normalize_name(&index.name),
        );
        if self.physical_replica_mode()
            && index.vector_algo() == VectorAlgo::IvfSq8
            && matches!(
                table.columns[table.column_index(&index.columns[0])?].data_type,
                DataTypeKind::Vector {
                    datatype: crate::vector::VectorDataType::Float32,
                    ..
                }
            )
        {
            if !vector_storage_paged() {
                return Err(error(crate::error::catalog::ER_GET_ERRNO,
                    "memory IVF acceleration unavailable during physical replication; using exact heap search"));
            }
            if let Some(graph) = self.vector_indexes.read().get(&key) {
                return Ok(graph.clone());
            }
            // Serialize publication with redo invalidation. Otherwise a build
            // started before eviction can publish its obsolete resident state
            // after the applier has already removed the old cache entry.
            let mut map = self.vector_indexes.write();
            if let Some(graph) = map.get(&key) {
                return Ok(graph.clone());
            }
            let graph = Arc::new(self.vector_graph_load_or_build(table, index, storage_id)?);
            map.insert(key, graph.clone());
            return Ok(graph);
        }
        if let Some(graph) = self.vector_indexes.read().get(&key) {
            return Ok(graph.clone());
        }
        // A build that just failed memory admission would fail again: report
        // it immediately (callers fall back to an exact scan) instead of
        // repeating a full heap scan on every query.
        if let Some(failed_at) = self.vector_build_backoff.lock().get(&key) {
            if failed_at.elapsed() < VECTOR_BUILD_BACKOFF {
                return Err(error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    format!(
                        "vector index '{}' could not be built within the memory budget; retry deferred",
                        index.name
                    ),
                ));
            }
        }
        let built = self.vector_graph_build_and_publish(table, index, storage_id, key.clone());
        let mut backoff = self.vector_build_backoff.lock();
        match &built {
            Err(e) if e.is(crate::error::catalog::ER_OUTOFMEMORY) => {
                backoff.insert(key, std::time::Instant::now());
            }
            _ => {
                backoff.remove(&key);
            }
        }
        built
    }

    /// Build one partition graph and publish it unless another thread already
    /// did. HNSW builds register before their heap scan so DML committed
    /// before publication is replayed rather than skipped as a cold graph.
    fn vector_graph_build_and_publish(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
        key: (u64, u64, u64, String),
    ) -> Result<Arc<VectorGraph>, QueryError> {
        let build_lock = {
            let mut locks = self.vector_build_locks.lock();
            locks.retain(|_, owner| owner.strong_count() > 0);
            match locks.get(&key).and_then(std::sync::Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(parking_lot::Mutex::new(()));
                    locks.insert(key.clone(), Arc::downgrade(&lock));
                    lock
                }
            }
        };
        let _owner = loop {
            if let Some(guard) = build_lock.try_lock_for(std::time::Duration::from_millis(25)) {
                break guard;
            }
            crate::engine::cancel::check_cancelled()?;
        };
        crate::engine::cancel::check_cancelled()?;
        if let Some(existing) = self.vector_indexes.read().get(&key) {
            return Ok(existing.clone());
        }
        let registration = Some(VectorBuildRegistration::begin(self, key.clone()));
        let build_epoch = self
            .vector_build_epoch
            .load(std::sync::atomic::Ordering::Acquire);
        let graph = self.vector_graph_load_or_build(table, index, storage_id)?;
        let mut build_guard = VectorGraphBuildGuard {
            graph: &graph,
            published: false,
        };
        #[cfg(test)]
        {
            let hook = VECTOR_BUILD_BEFORE_PUBLISH_HOOK.lock().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        let mut applied = 0;
        if let Some(registration) = &registration {
            for _ in 0..8 {
                let (next, batch) = registration.ops_since(applied)?;
                if batch.is_empty() {
                    break;
                }
                applied = next;
                for (row_id, vector) in batch {
                    graph.replace_row(row_id, vector)?;
                }
                crate::engine::cancel::check_cancelled()?;
            }
        }
        let mut map = self.vector_indexes.write();
        if self
            .vector_build_epoch
            .load(std::sync::atomic::Ordering::Acquire)
            != build_epoch
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "vector build invalidated by concurrent state change",
            ));
        }
        if let Some(existing) = map.get(&key) {
            return Ok(existing.clone());
        }
        if let Some(registration) = &registration {
            let (_, batch) = registration.ops_since(applied)?;
            if batch.len() > 4096 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "vector build publication tail exceeded its work budget",
                ));
            }
            for (row_id, vector) in batch {
                graph.replace_row(row_id, vector)?;
            }
        }
        if let VectorGraph::Paged(paged) = &graph {
            paged.file().advance_coverage(self.visible_scn());
            paged.file().mark_delta_finalized(0)?;
            paged.refresh_query_navigation()?;
        }
        if let VectorGraph::NativeHnsw(native) = &graph {
            if let Some(file) = native.file() {
                file.advance_coverage(self.visible_scn());
                file.mark_delta_finalized(0)?;
            }
        }
        if let VectorGraph::NativeIvf(native) = &graph {
            if let Some(file) = native.file() {
                file.advance_coverage(self.visible_scn());
                file.mark_delta_finalized(0)?;
            }
        }
        if let VectorGraph::Memory(memory) = &graph {
            memory.set_background_quant(crate::engine::vector_maintenance::is_active(self));
        }
        build_guard.published = true;
        drop(build_guard);
        let graph = Arc::new(graph);
        map.insert(key, graph.clone());
        Ok(graph)
    }

    pub(crate) fn sync_vector_background_quant(&self) {
        let graphs = self.vector_indexes.read();
        let active = crate::engine::vector_maintenance::is_active(self);
        for graph in graphs.values() {
            if let VectorGraph::Memory(memory) = graph.as_ref() {
                memory.set_background_quant(active);
            }
        }
    }

    /// Record committed maintenance for a key whose graph is not published.
    /// Callers hold `vector_indexes` (read), which orders this against the
    /// final replay in [`Self::vector_graph_build_and_publish`].
    fn record_vector_build_delta(
        &self,
        key: &(u64, u64, u64, String),
        row_id: RowId,
        vector: Option<crate::vector::VectorValue>,
    ) {
        if self
            .vector_builds_in_flight
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            return;
        }
        if let Some(delta) = self.vector_build_deltas.lock().get_mut(key) {
            if delta.failed {
                return;
            }
            let previous = delta.ops.get(&row_id).map_or(0, |(_, value)| {
                value
                    .as_ref()
                    .map_or(0, |v| v.capacity())
                    .saturating_mul(2)
                    .saturating_add(128)
            });
            let bytes = vector
                .as_ref()
                .map_or(0, |v| v.capacity())
                .saturating_mul(2)
                .saturating_add(128);
            let increase = bytes.saturating_sub(previous);
            let limit = (crate::engine::executor::mem_budget::memory_target_bytes() / 32)
                .clamp(1024 * 1024, 32 * 1024 * 1024);
            if delta
                .charge
                .as_ref()
                .map_or(0, |c| c.bytes())
                .saturating_add(increase)
                > limit
            {
                delta.failed = true;
                return;
            }
            let admitted = match delta.charge.as_mut() {
                Some(charge) => charge.try_grow(increase).is_ok(),
                None => match crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    bytes,
                ) {
                    Ok(charge) => {
                        delta.charge = Some(charge);
                        true
                    }
                    Err(_) => false,
                },
            };
            if !admitted {
                delta.failed = true;
                return;
            }
            if previous > bytes {
                if let Some(charge) = delta.charge.as_mut() {
                    charge.shrink(previous - bytes);
                }
            }
            delta.next_seq = delta.next_seq.saturating_add(1);
            delta.ops.insert(row_id, (delta.next_seq, vector));
        }
    }

    #[cfg(test)]
    /// Convenience wrapper for the common (unpartitioned) case: builds /
    /// returns the single per-table graph keyed by `(table.id, table.id, name)`.
    /// Kept for tests and external callers that don't want to thread storage
    /// ids; the partitioned executor path goes through
    /// [`Database::vector_graph_get_or_build_for_partition`] directly.
    pub(crate) fn vector_graph_get_or_build(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
    ) -> Result<Arc<VectorGraph>, QueryError> {
        self.vector_graph_get_or_build_for_partition(table, index, table.id)
    }

    /// "CREATE INDEX takes effect immediately" hook: after an ADD INDEX
    /// finalize (heap fully backfilled, index flipped Public), eagerly build
    /// the in-memory HNSW graph for every vector index named in `index_names`
    /// so the *first* KNN query doesn't pay the full graph-build cost.
    ///
    /// Gated by `MYDB_HNSW_BUILD_ON_DDL` (default on). Best-effort: a build
    /// failure logs and leaves the graph cold — the index is already correct
    /// and usable (registry B-tree + MVCC re-rank), so the next search just
    /// lazily rebuilds it. A no-op for tables with no vector index.
    ///
    /// Empty graphs are published too: this makes a finalized-empty on-disk
    /// graph participate in subsequent incremental DML rather than becoming a
    /// trusted but permanently empty candidate source after restart.
    pub(crate) fn warm_vector_graphs_after_ddl(
        &self,
        table: &TableSchema,
        index_names: &[&str],
    ) -> Result<(), QueryError> {
        if !hnsw_build_on_ddl_enabled() {
            return Ok(());
        }
        for index_name in index_names {
            let Some(index) = table
                .indexes
                .iter()
                .find(|i| i.name.eq_ignore_ascii_case(index_name))
            else {
                continue;
            };
            if !index.is_vector() {
                continue;
            }
            for sid in table.storage_ids() {
                if let Err(e) = self.vector_graph_build_on_ddl_one(table, index, sid) {
                    let context = format!(
                        "vector index build failed for {}.{} / {} (storage_id={}): ",
                        table.database, table.name, index.name, sid
                    );
                    if e.is(crate::error::catalog::ER_OUTOFMEMORY) {
                        return Err(e.map_message(|message| format!("{context}{message}")));
                    }
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!("{context}{}", e.message()),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Build one partition's graph from the heap and publish it to the cache.
    /// An already-warm graph is left untouched (idempotent). Empty graphs must
    /// be published so future INSERTs maintain a finalized-empty disk image.
    fn vector_graph_build_on_ddl_one(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<(), QueryError> {
        let key = (
            table.id,
            storage_id,
            table.active_generation.max(1),
            normalize_name(&index.name),
        );
        if self.vector_indexes.read().contains_key(&key) {
            return Ok(()); // already warm (e.g. a concurrent query built it)
        }
        if index.vector_algo() == VectorAlgo::IvfSq8
            && vector_storage_paged()
            && matches!(
                table.columns[table.column_index(&index.columns[0])?].data_type,
                DataTypeKind::Vector {
                    datatype: crate::vector::VectorDataType::Float32,
                    ..
                }
            )
        {
            let graph = self.rebuild_paged_ivf_atomic(table, index, storage_id)?;
            self.vector_indexes.write().entry(key).or_insert(graph);
            return Ok(());
        }
        self.vector_graph_build_and_publish(table, index, storage_id, key)?;
        Ok(())
    }

    /// Current graph ordering/cold coverage is unsuitable for an older view.
    pub(crate) fn vector_snapshot_requires_exact(&self, txn: TxnView) -> bool {
        txn.historical
            || txn.snapshot_scn
                < self
                    .vector_snapshot_floor
                    .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Per-partition k-nearest-neighbour candidate search. Returns
    /// `(row_id, ordering_distance)` pairs ascending by distance — a *superset*
    /// generator; the executor re-reads each row under MVCC and re-ranks by the
    /// live heap vector before truncating to the user's LIMIT. For
    /// unpartitioned tables callers pass `table.id`; for partitioned tables
    /// the executor (`execute_vector_index_scan`) calls this once per
    /// partition storage id and merges the results.
    pub(crate) fn vector_index_search_for_partition(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
        query: &crate::vector::VectorValue,
        request: crate::engine::vector::VectorSearchRequest,
    ) -> Result<crate::engine::vector::VectorSearchResult, QueryError> {
        let graph = self.vector_graph_get_or_build_for_partition(table, index, storage_id)?;
        if !graph.is_empty() && graph.dim() != query.dimension as usize {
            return Err(error(
                crate::error::catalog::MYDB_ER_VECTOR_DIMENSION_MISMATCH,
                format!(
                    "query vector has {} dimensions but index '{}' is {}-dimensional",
                    query.len(),
                    index.name,
                    graph.dim()
                ),
            ));
        }
        if self.physical_replica_mode() {
            if let VectorGraph::Paged(paged) = graph.as_ref() {
                paged.file().disable_navigation_mapping();
            }
        }
        let result = graph.search(query, request);
        if result
            .as_ref()
            .is_err_and(|error| error.is(crate::error::catalog::ER_GET_ERRNO))
            && matches!(
                graph.as_ref(),
                VectorGraph::NativeHnsw(_) | VectorGraph::NativeIvf(_)
            )
        {
            self.evict_vector_graphs(vec![(
                table.id,
                storage_id,
                table.active_generation.max(1),
                normalize_name(&index.name),
            )]);
        }
        result
    }

    /// Dimensionless columns still obey a fixed dimension once indexed.
    /// Fixed VECTOR(n) values were already checked by column coercion.
    fn check_hnsw_index_dimensions(
        &self,
        table: &TableSchema,
        values: &[Value],
        changed: Option<&[usize]>,
    ) -> Result<(), QueryError> {
        for index in table
            .indexes
            .iter()
            .filter(|i| i.is_vector() && i.vector_algo() == VectorAlgo::Hnsw)
        {
            let col = table.column_index(&index.columns[0])?;
            if !matches!(
                table.columns[col].data_type,
                DataTypeKind::Vector { dim: None, .. }
            ) || changed.is_some_and(|cols| !cols.contains(&col))
            {
                continue;
            }
            let Some(Value::Vector(bytes)) = values.get(col) else {
                continue;
            };
            let sid = if table.is_partitioned() {
                table.route_row_to_partition(values)?.storage_id
            } else {
                table.id
            };
            let graph = self.vector_graph_get_or_build_for_partition(table, index, sid)?;
            let actual = bytes.len() / 4;
            if let VectorGraph::Memory(memory) = graph.as_ref() {
                memory.validate_insert_dimension(actual)?;
            } else if graph.dim() != actual {
                return Err(error(
                    crate::error::catalog::MYDB_ER_INVALID_VECTOR_VALUE,
                    format!(
                        "vector index '{}' dimension mismatch: expected {}, got {actual}",
                        index.name,
                        graph.dim()
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Note a freshly-inserted row to every *warm* vector graph on `table`.
    /// The single per-row maintenance entry point for the DML insert paths
    /// (`insert_row_in_table` and the bulk batch loop). A no-op for tables with
    /// no vector index and for cold graphs (the lazy build picks the row up
    /// from the heap on first search). Called pre-commit: an uncommitted /
    /// rolled-back node is invisible to readers and only costs recall, never
    /// correctness, matching the spatial-derived "graph is just a candidate
    /// generator" model.
    ///
    /// For unpartitioned tables callers pass `table.id` as `storage_id`. For
    /// partitioned tables the DML path passes the per-partition storage id
    /// so the row lands in the matching per-partition graph (M5.5).
    pub(crate) fn note_vector_inserts(
        &self,
        table: &TableSchema,
        values: &[Value],
        row_id: RowId,
        storage_id: u64,
    ) {
        let map = self.vector_indexes.read();
        let mut failed = Vec::new();
        for index in table.indexes.iter().filter(|index| index.is_vector()) {
            let key = (
                table.id,
                storage_id,
                table.active_generation.max(1),
                normalize_name(&index.name),
            );
            let Ok(col_idx) = table.column_index(&index.columns[0]) else {
                continue;
            };
            let Some(Value::Vector(bytes)) = values.get(col_idx) else {
                continue;
            };
            let Some(graph) = map.get(&key) else {
                self.record_vector_build_delta(&key, row_id, Some(bytes.clone()));
                continue;
            };
            if graph.insert(row_id, bytes.clone()).is_err() {
                failed.push(key);
            }
        }
        drop(map);
        self.evict_vector_graphs(failed);
        self.note_spatial_cache_change(table, None, Some(values), row_id, storage_id);
    }

    /// Note an UPDATE to every *warm* vector graph on `table`. mydb updates a
    /// row in place (same `RowId` + undo chain), so — unlike an append-only
    /// store — the new vector does not re-enter via the INSERT path. When the
    /// vector column actually changed, tombstone every existing node keyed
    /// by that `RowId` (the old neighbours stay on the graph until the next
    /// `compact`, but `search` already skips tombstones) and add the new
    /// vector as a fresh node. Correctness never depends on this — the
    /// executor always re-ranks by the live heap vector — but it keeps
    /// recall high for "moved-closer" updates. Skips unchanged vectors so
    /// metadata-only updates don't bloat the graph.
    pub(crate) fn note_vector_updates(
        &self,
        table: &TableSchema,
        old_values: &[Value],
        new_values: &[Value],
        row_id: RowId,
        storage_id: u64,
        retire: &VectorRetire<'_>,
    ) {
        let (update_in_place, ratio) = crate::engine::vector::update_inplace_cfg();
        let map = self.vector_indexes.read();
        let mut failed = Vec::new();
        for index in table.indexes.iter().filter(|index| index.is_vector()) {
            let key = (
                table.id,
                storage_id,
                table.active_generation.max(1),
                normalize_name(&index.name),
            );
            let Ok(col) = table.column_index(&index.columns[0]) else {
                continue;
            };
            let changed = match (old_values.get(col), new_values.get(col)) {
                (Some(old), Some(new)) => old != new,
                _ => true,
            };
            // Cold-build replay also changes positions relative to old views.
            // Publish the floor even when no shared graph exists yet.
            if changed {
                self.vector_snapshot_floor
                    .fetch_max(retire.commit_scn, std::sync::atomic::Ordering::AcqRel);
            }
            let Some(graph) = map.get(&key) else {
                if changed {
                    let vector = match new_values.get(col) {
                        Some(Value::Vector(bytes)) => Some(bytes.clone()),
                        _ => None,
                    };
                    self.record_vector_build_delta(&key, row_id, vector);
                }
                continue;
            };
            if changed {
                // Drift-adaptive fast path: if the vector barely moved, overwrite
                // the node in place (keep id + edges) and skip the re-link. Large
                // moves return false and fall through to tombstone + re-insert.
                if update_in_place
                    && matches!(new_values.get(col), Some(Value::Vector(bytes)) if graph.try_update_in_place(row_id, bytes, ratio))
                {
                    continue;
                }
                let replaced = if graph.supports_versions() {
                    // Insert the new version now; the superseded ones stay
                    // findable for snapshots that still see them.
                    graph.live_versions(row_id).and_then(|versions| {
                        if let Some(Value::Vector(bytes)) = new_values.get(col) {
                            graph.insert(row_id, bytes.clone())?;
                        }
                        retire.retire(&key, row_id, versions);
                        Ok(())
                    })
                } else {
                    graph
                        .mark_deleted_by_row_id(row_id)
                        .and_then(|_| match new_values.get(col) {
                            Some(Value::Vector(bytes)) => graph.insert(row_id, bytes.clone()),
                            _ => Ok(()),
                        })
                };
                if replaced.is_err() {
                    failed.push(key.clone());
                }
            } else {
                // A prior buggy pre-commit DELETE could have left a stale
                // tombstone even though MVCC restored the row. Metadata-only
                // UPDATE is an inexpensive self-healing opportunity: ensure a
                // live graph node exists instead of permanently no-oping.
                if !graph.has_live_row_id(row_id) {
                    if let Some(Value::Vector(bytes)) = new_values.get(col) {
                        if graph.insert(row_id, bytes.clone()).is_err() {
                            failed.push(key.clone());
                        }
                    }
                }
            }
        }
        drop(map);
        self.evict_vector_graphs(failed);
    }

    /// Note a DELETE to every *warm* vector graph on `table`. Tombstones
    /// every node keyed by `row_id`; the executor's MVCC visibility check
    /// already drops returned candidates whose heap row is invisible, so
    /// this is purely a recall optimisation — uncompacted tombstones still
    /// cost a distance computation per search even though they never
    /// surface in results. Cold graph: no-op.
    pub(crate) fn note_vector_deletes(
        &self,
        table: &TableSchema,
        row_id: RowId,
        storage_id: u64,
        retire: &VectorRetire<'_>,
    ) {
        let map = self.vector_indexes.read();
        let mut failed = Vec::new();
        for index in table.indexes.iter().filter(|index| index.is_vector()) {
            let key = (
                table.id,
                storage_id,
                table.active_generation.max(1),
                normalize_name(&index.name),
            );
            match map.get(&key) {
                Some(graph) => {
                    let retired = if graph.supports_versions() {
                        graph
                            .live_versions(row_id)
                            .map(|versions| retire.retire(&key, row_id, versions))
                    } else {
                        self.vector_snapshot_floor
                            .fetch_max(retire.commit_scn, std::sync::atomic::Ordering::AcqRel);
                        graph.mark_deleted_by_row_id(row_id).map(|_| ())
                    };
                    if retired.is_err() {
                        failed.push(key);
                    }
                }
                None => {
                    self.vector_snapshot_floor
                        .fetch_max(retire.commit_scn, std::sync::atomic::Ordering::AcqRel);
                    self.record_vector_build_delta(&key, row_id, None);
                }
            }
        }
        drop(map);
        self.evict_vector_graphs(failed);
    }

    /// Drop graphs whose incremental maintenance failed; the next access
    /// rebuilds them from the heap (a paged file was already un-finalized).
    fn evict_vector_graphs(&self, failed: Vec<(u64, u64, u64, String)>) {
        if failed.is_empty() {
            return;
        }
        let mut map = self.vector_indexes.write();
        for key in failed {
            if let Some(graph) = map.remove(&key) {
                graph.invalidate();
            }
        }
    }

    fn note_spatial_cache_change(
        &self,
        table: &TableSchema,
        old_values: Option<&[Value]>,
        new_values: Option<&[Value]>,
        row_id: RowId,
        storage_id: u64,
    ) {
        for index in table.indexes.iter().filter(|index| index.is_spatial()) {
            let old_key = old_values.and_then(|values| table.index_key(index, values).ok());
            let new_key = new_values.and_then(|values| table.index_key(index, values).ok());
            self.update_warm_spatial_cache(
                storage_id,
                &index.name,
                row_id,
                old_key.as_ref(),
                new_key.as_ref(),
            );
        }
    }

    /// The undo marker for deferred ANN maintenance, or `None` when the table
    /// has no derived vector/spatial index. Ordinary B-tree tables must not
    /// take the transaction-directory latch per mutated row just to discover
    /// that there is nothing to defer.
    pub(crate) fn deferred_ann_undo_head(
        &self,
        txn_id: u64,
        table: &TableSchema,
    ) -> Result<Option<u64>, QueryError> {
        if !table_has_deferred_ann_indexes(table) {
            return Ok(None);
        }
        self.txns_read()?.undo_head(txn_id)
    }

    /// Register DELETE-side candidate-generator maintenance for commit. The
    /// heap/index mutation is MVCC transactional; the vector graph is derived
    /// state and has no undo, so marking it before commit would survive a
    /// rollback and permanently reduce recall.
    pub(crate) fn defer_vector_delete(
        &self,
        txn_id: u64,
        undo_id: u64,
        table: &TableSchema,
        old_values: &[Value],
        row_id: RowId,
        storage_id: u64,
    ) -> Result<(), QueryError> {
        if !table_has_deferred_ann_indexes(table) {
            return Ok(());
        }
        self.pending_vector_deletes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(txn_id)
            .or_default()
            .push(PendingVectorMutation {
                undo_id,
                table_id: table.id,
                storage_id,
                row_id,
                new_values: None,
                old_values: Some(old_values.to_vec()),
            });
        Ok(())
    }

    /// An INSERT batch shares one undo marker. Ordinary B-tree tables have
    /// no derived mutations, so do not acquire the transaction latch per row
    /// just to discover that deferred maintenance has nothing to do.
    pub(crate) fn defer_vector_insert_batch(
        &self,
        txn_id: u64,
        table: &TableSchema,
        storage_id: u64,
        rows: &[Vec<Value>],
        row_ids: &[RowId],
    ) -> Result<(), QueryError> {
        debug_assert_eq!(rows.len(), row_ids.len());
        if let Some(undo_id) = self.deferred_ann_undo_head(txn_id, table)? {
            for (values, &row_id) in rows.iter().zip(row_ids) {
                self.defer_vector_update(txn_id, undo_id, table, row_id, storage_id, None, values)?;
            }
        }
        Ok(())
    }

    /// Register UPDATE-side candidate-generator maintenance for commit. Until
    /// commit the old ANN entry remains available; MVCC heap recheck preserves
    /// correctness, while rollback/savepoint/XA rollback can simply discard the
    /// pending replacement instead of trying to reconstruct a mutated graph.
    pub(crate) fn defer_vector_update(
        &self,
        txn_id: u64,
        undo_id: u64,
        table: &TableSchema,
        row_id: RowId,
        storage_id: u64,
        old_values: Option<&[Value]>,
        new_values: &[Value],
    ) -> Result<(), QueryError> {
        if !table_has_deferred_ann_indexes(table) {
            return Ok(());
        }
        self.pending_vector_deletes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(txn_id)
            .or_default()
            .push(PendingVectorMutation {
                undo_id,
                table_id: table.id,
                storage_id,
                row_id,
                new_values: Some(new_values.to_vec()),
                old_values: old_values.map(<[Value]>::to_vec),
            });
        Ok(())
    }

    /// Place rebuild markers at the commit boundary, before CommitTxn WAL.
    /// Rolled-back deferred mutations never touched IVF pages.
    pub(crate) fn prepare_pending_vector_commit(&self, txn_id: u64) -> Result<(), QueryError> {
        let affected: std::collections::HashSet<_> = self
            .pending_vector_deletes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&txn_id)
            .into_iter()
            .flatten()
            .map(|e| (e.table_id, e.storage_id))
            .collect();
        if affected.is_empty() {
            return Ok(());
        }
        // Published before the commit becomes visible, so any reader that can
        // see these rows also merges them until the graph has caught up.
        self.vector_commits_publishing.lock().insert(txn_id);
        for (table_id, storage_id) in affected {
            if let Some(table) = self.table_by_id(table_id) {
                self.begin_vector_transaction_delta(txn_id, &table, storage_id)?;
            }
        }
        Ok(())
    }

    fn begin_vector_transaction_delta(
        &self,
        txn_id: u64,
        table: &TableSchema,
        storage_id: u64,
    ) -> Result<(), QueryError> {
        for index in table.indexes.iter().filter(|index| index.is_vector()) {
            let key = (
                table.id,
                storage_id,
                table.active_generation.max(1),
                normalize_name(&index.name),
            );
            if index.vector_algo() == VectorAlgo::Hnsw
                && self.vector_build_deltas.lock().contains_key(&key)
            {
                continue;
            }
            let mut graph = self.vector_indexes.read().get(&key).cloned();
            // A cold but finalized file still represents a durable index.
            // Leaving it untouched would retain stale pages on both restart
            // and replicas. Reopen it (without a heap rebuild) before commit
            // so the ordinary delta hooks maintain it too.
            if graph.is_none()
                && index.vector_algo() == VectorAlgo::IvfSq8
                && matches!(
                    table.columns[table.column_index(&index.columns[0])?].data_type,
                    DataTypeKind::Vector {
                        datatype: crate::vector::VectorDataType::Float32,
                        ..
                    }
                )
                && vector_storage_paged()
            {
                let path = self.vector_index_paged_path_ext(table, &index.name, storage_id, "ivf");
                if path.exists() {
                    let col_idx = table.column_index(&index.columns[0])?;
                    let dim = self.vector_paged_resolve_dim(table, col_idx, storage_id)?;
                    let file = self.open_ivf_index_file(table, index, storage_id, dim)?;
                    if file.is_finalized() {
                        let paged = crate::engine::vector::ivf_paged::PagedIvf::new(
                            Arc::new(file),
                            ivf_params_from_meta(&index.vector_meta()),
                        );
                        let candidate = Arc::new(VectorGraph::Ivf(IvfSq8Index::from_paged(paged)));
                        graph = Some(
                            self.vector_indexes
                                .write()
                                .entry(key.clone())
                                .or_insert(candidate)
                                .clone(),
                        );
                    }
                }
            }
            // FLOAT32's finalized `.hvec` is trusted without a heap scan, so
            // commits must reach it while cold. Native files always rebuild
            // from heap and must not enter this legacy FLOAT32 open path.
            if graph.is_none()
                && index.vector_algo() == VectorAlgo::Hnsw
                && vector_storage_paged()
                && matches!(
                    table.columns[table.column_index(&index.columns[0])?].data_type,
                    DataTypeKind::Vector {
                        datatype: crate::vector::VectorDataType::Float32,
                        ..
                    }
                )
            {
                let path = self.vector_index_paged_path(table, &index.name, storage_id);
                if path.exists() {
                    let col_idx = table.column_index(&index.columns[0])?;
                    let dim = self.vector_paged_resolve_dim(table, col_idx, storage_id)?;
                    let file = self.open_vector_index_file(table, index, storage_id, dim)?;
                    if file.is_finalized() && file.meta().query_ready {
                        let candidate =
                            Arc::new(VectorGraph::Paged(PagedHnsw::new(Arc::new(file))));
                        graph = Some(
                            self.vector_indexes
                                .write()
                                .entry(key)
                                .or_insert(candidate)
                                .clone(),
                        );
                    }
                }
            }
            if let Some(graph) = graph {
                graph.begin_transaction_delta(txn_id)?;
            }
        }
        Ok(())
    }

    /// Apply and remove every still-live deferred DELETE for a committing txn.
    pub(crate) fn apply_pending_vector_deletes(&self, txn_id: u64, commit_scn: u64) {
        let retire = VectorRetire::new(self, commit_scn);
        let pending = self
            .pending_vector_deletes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&txn_id)
            .cloned()
            .unwrap_or_default();
        let mut affected = std::collections::HashSet::new();
        for entry in pending {
            affected.insert((entry.table_id, entry.storage_id));
            if let Some(table) = self.table_by_id(entry.table_id) {
                if let Some(new_values) = entry.new_values {
                    let old_values = entry.old_values.as_deref().unwrap_or(&[]);
                    self.note_vector_updates(
                        &table,
                        old_values,
                        &new_values,
                        entry.row_id,
                        entry.storage_id,
                        &retire,
                    );
                    self.note_spatial_cache_change(
                        &table,
                        Some(old_values),
                        Some(&new_values),
                        entry.row_id,
                        entry.storage_id,
                    );
                } else {
                    self.note_vector_deletes(&table, entry.row_id, entry.storage_id, &retire);
                    if let Some(old_values) = entry.old_values.as_deref() {
                        self.note_spatial_cache_change(
                            &table,
                            Some(old_values),
                            None,
                            entry.row_id,
                            entry.storage_id,
                        );
                    } else {
                        // DELETE callers do not retain a second copy of the
                        // before-image here. Invalidate only the affected
                        // spatial caches; the next query rebuilds from the
                        // authoritative B-tree without risking a false negative.
                        let mut caches = self.spatial_rtree_cache.write();
                        for index in table.indexes.iter().filter(|index| index.is_spatial()) {
                            caches.remove(&(entry.storage_id, normalize_name(&index.name)));
                        }
                    }
                }
            }
        }
        retire.finish();
        // This marker is deliberately last. If the process dies after the
        // heap commit but before every derived mutation lands, the pre-commit
        // transactional marker remains `finalized=false` and startup rebuilds
        // from the authoritative heap.
        let map = self.vector_indexes.read();
        for (table_id, storage_id) in affected {
            let Some(table) = self.table_by_id(table_id) else {
                continue;
            };
            for index in table.indexes.iter().filter(|index| index.is_vector()) {
                let key = (
                    table_id,
                    storage_id,
                    table.active_generation.max(1),
                    normalize_name(&index.name),
                );
                if let Some(graph) = map.get(&key) {
                    if let Err(e) = graph.finish_committed_delta(txn_id, commit_scn) {
                        log_message(format!(
                            "vector index delta finalize failed for {}: {e:?}",
                            index.name
                        ));
                    }
                }
            }
        }
        self.pending_vector_deletes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&txn_id);
        self.vector_commits_publishing.lock().remove(&txn_id);
    }

    /// The oldest snapshot SCN any current or future reader may hold: a
    /// commit with `commit_scn <= horizon` is visible to every one of them.
    fn vector_purge_horizon(&self) -> u64 {
        let (oldest, visible) = self.snapshot_visibility_horizon();
        oldest.map_or(visible, |oldest| oldest.min(visible))
    }

    /// Retire queued graph versions whose commit every snapshot now sees.
    /// Called after each commit publishes visibility and before checkpoint
    /// compaction; with nothing queued it costs one atomic load.
    pub(crate) fn drain_vector_purges(&self) {
        if self
            .vector_purges_pending
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            return;
        }
        let horizon = self.vector_purge_horizon();
        let ready: Vec<VectorPurge> = {
            let mut queue = self.vector_purge_queue.lock();
            let (ready, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut *queue)
                .into_iter()
                .partition(|purge| purge.commit_scn <= horizon);
            *queue = waiting.into();
            self.vector_purges_pending
                .store(queue.len(), std::sync::atomic::Ordering::Release);
            ready
        };
        if ready.is_empty() {
            return;
        }
        let map = self.vector_indexes.read();
        let mut failed = Vec::new();
        for purge in ready {
            // An evicted/rebuilt graph holds only current heap versions.
            let Some(graph) = map.get(&purge.key) else {
                continue;
            };
            let retired = purge.rows.iter().try_for_each(|(row_id, versions)| {
                graph.retire_versions(*row_id, versions).map(|_| ())
            });
            if retired.is_err() {
                failed.push(purge.key);
            }
        }
        drop(map);
        self.evict_vector_graphs(failed);
    }

    pub(crate) fn discard_pending_vector_deletes(&self, txn_id: u64) -> u64 {
        let entries = self
            .pending_vector_deletes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&txn_id)
            .unwrap_or_default();
        self.vector_commits_publishing.lock().remove(&txn_id);
        let discarded = entries.len() as u64;
        // A commit attempt can fail after preparing its rebuild marker but
        // before any derived pages are mutated. Rollback retires that marker
        // without finalizing over another transaction still publishing DML.
        let affected: std::collections::HashSet<_> =
            entries.iter().map(|e| (e.table_id, e.storage_id)).collect();
        let map = self.vector_indexes.read();
        for (table_id, storage_id) in affected {
            for ((tid, sid, _, _), graph) in map.iter() {
                if *tid == table_id && *sid == storage_id {
                    if let Err(e) = graph.finish_committed_delta(txn_id, self.visible_scn()) {
                        log_message(format!("vector rollback marker cleanup failed: {e:?}"));
                    }
                }
            }
        }
        drop(map);
        if discarded > 0 {
            self.hnsw_tombstones_rolled_back
                .fetch_add(discarded, std::sync::atomic::Ordering::Relaxed);
        }
        discarded
    }

    /// RowIds whose replacement may not be present in the shared ANN structure
    /// yet: the reader's own pending upserts (read-your-writes) and those of
    /// transactions whose commit is being published to the graph. Other
    /// sessions' uncommitted rows are invisible to the reader and are skipped
    /// instead of being heap-read by every concurrent KNN query.
    pub(crate) fn pending_vector_upsert_row_ids(
        &self,
        txn_id: u64,
        table_id: u64,
        storage_id: u64,
    ) -> Vec<RowId> {
        let publishing: Vec<u64> = {
            let publishing = self.vector_commits_publishing.lock();
            publishing
                .iter()
                .copied()
                .filter(|&txn| txn != txn_id)
                .collect()
        };
        let pending = self
            .pending_vector_deletes
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        std::iter::once(txn_id)
            .chain(publishing)
            .filter_map(|txn| pending.get(&txn))
            .flatten()
            .filter(|entry| {
                entry.table_id == table_id
                    && entry.storage_id == storage_id
                    && entry.new_values.is_some()
            })
            .map(|entry| entry.row_id)
            .collect()
    }

    /// Snapshot runtime stats for every *warm* vector index, aggregated across
    /// per-partition graphs and keyed by `(table_id, normalized_index_name)`.
    /// Drives `information_schema.mydb_vector_indexes` and the SHOW ENGINE HNSW
    /// section. Cold (never-queried) indexes don't appear — the view joins this
    /// against the catalog and reports them as `LOADED = 0`. Takes only the
    /// `vector_indexes` read lock; never builds a graph.
    pub(crate) fn vector_index_runtime_stats(
        &self,
    ) -> HashMap<(u64, String), vector::VectorIndexRuntimeStats> {
        let map = self.vector_indexes.read();
        let mut out: HashMap<(u64, String), vector::VectorIndexRuntimeStats> = HashMap::new();
        for ((table_id, _storage_id, _generation, name), graph) in map.iter() {
            out.entry((*table_id, name.clone()))
                .or_default()
                .accumulate(graph);
        }
        out
    }

    pub(crate) fn hnsw_tombstones_rolled_back(&self) -> u64 {
        self.hnsw_tombstones_rolled_back
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn note_replicated_hnsw_heap(&self, txn_id: u64, storage_id: u64) {
        let cached = self
            .replicated_hnsw_table_cache
            .read()
            .get(&storage_id)
            .copied();
        let owner = if let Some(cached) = cached {
            cached
        } else {
            let Some(table) = self.table_by_id(storage_id) else {
                return;
            };
            // Resident native accelerators have no index-file redo. Track
            // every vector table so heap commits also revoke those handles.
            let owner = table
                .indexes
                .iter()
                .any(|i| i.is_vector())
                .then_some(table.id);
            self.replicated_hnsw_table_cache
                .write()
                .insert(storage_id, owner);
            owner
        };
        if let Some(owner) = owner {
            self.replicated_hnsw_dirty
                .lock()
                .entry(txn_id)
                .or_default()
                .insert(owner);
        }
    }

    fn note_replicated_hnsw(&self, txn_id: u64, storage_id: u64, name: &str) {
        let Some(table) = self.table_by_id(storage_id) else {
            return;
        };
        if table.indexes.iter().any(|index| {
            index.name.eq_ignore_ascii_case(name)
                && index.is_vector()
                && index.vector_algo() == VectorAlgo::Hnsw
        }) {
            self.replicated_hnsw_dirty
                .lock()
                .entry(txn_id)
                .or_default()
                .insert(table.id);
        }
    }

    fn invalidate_replicated_hnsw_commit(&self, txn_id: u64, finish: bool) {
        let tables = {
            let mut dirty = self.replicated_hnsw_dirty.lock();
            if finish {
                dirty.remove(&txn_id)
            } else {
                dirty.get(&txn_id).cloned()
            }
        };
        let Some(tables) = tables else {
            return;
        };
        let mut graphs = self.vector_indexes.write();
        self.vector_build_epoch
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        graphs.retain(|(table, _, _, _), graph| {
            if tables.contains(table)
                && (graph.supports_versions()
                    || matches!(
                        graph.as_ref(),
                        VectorGraph::NativeHnsw(_) | VectorGraph::NativeIvf(_)
                    ))
            {
                graph.invalidate();
                false
            } else {
                true
            }
        });
    }

    /// Replica HNSW files were not physically redone. Fence every durable
    /// candidate file before the promoted node can reopen it as a primary.
    fn invalidate_promoted_hnsw_files(&self) -> Result<(), QueryError> {
        let ids: Vec<_> = self
            .catalog
            .read()
            .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "catalog lock poisoned"))?
            .tables
            .values()
            .filter(|table| !table.system)
            .map(|table| table.id)
            .collect();
        let mut directories = std::collections::HashSet::new();
        for id in ids {
            let Some(table) = self.table_by_id(id) else {
                continue;
            };
            for index in table
                .indexes
                .iter()
                .filter(|index| index.is_vector() && index.vector_algo() == VectorAlgo::Hnsw)
            {
                for storage in table.storage_ids() {
                    let path = self.vector_index_paged_path(&table, &index.name, storage);
                    if !path.exists() {
                        continue;
                    }
                    self.buffer_pool.discard_pages_for_path(&path)?;
                    match fs::remove_file(&path) {
                        Ok(()) => {
                            if let Some(parent) = path.parent() {
                                directories.insert(parent.to_path_buf());
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(io_err(error)),
                    }
                }
            }
        }
        // No local WAL is appended here: an I/O failure may still cancel the
        // role transition and let the replica resume receiving source LSNs.
        for directory in directories {
            crate::platform::fs::sync_path(directory).map_err(io_err)?;
        }
        Ok(())
    }

    /// Physical IVF redo replaces pages beneath resident directories. Retire
    /// both cached and already-cloned handles; a later query reopens finalized
    /// pages, while an incomplete delta uses the executor's exact fallback.
    pub(crate) fn invalidate_replicated_ivf_graphs(&self, path: Option<&str>) {
        if path.is_none() {
            self.replicated_hnsw_table_cache.write().clear();
            self.vector_build_epoch
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
        self.vector_indexes.write().retain(|_, graph| {
            if matches!(
                graph.as_ref(),
                VectorGraph::NativeHnsw(_) | VectorGraph::NativeIvf(_)
            ) {
                graph.invalidate();
                return false;
            }
            if path.is_none() && graph.supports_versions() {
                graph.invalidate();
                return false;
            }
            if let VectorGraph::Paged(paged) = graph.as_ref() {
                if path.is_none_or(|path| paged.file().relative_path() == path) {
                    paged.file().disable_navigation_mapping();
                }
            }
            if let VectorGraph::Ivf(IvfSq8Index::Paged(paged)) = graph.as_ref() {
                if path.is_none_or(|path| paged.file().relative_path() == path) {
                    paged.invalidate_replicated_pages();
                    false
                } else {
                    true
                }
            } else {
                !(path.is_none() && matches!(graph.as_ref(), VectorGraph::Ivf(_)))
            }
        });
    }

    /// Per-index tombstone hook. Mirrors `vector_graph_note_insert` for
    /// removals: cold graph is a no-op (the next lazy build picks up the
    /// fact that the row is gone from the heap).
    #[allow(dead_code)]
    pub(crate) fn vector_graph_mark_deleted(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        row_id: RowId,
        storage_id: u64,
    ) {
        let key = (
            table.id,
            storage_id,
            table.active_generation.max(1),
            normalize_name(&index.name),
        );
        let map = self.vector_indexes.read();
        let Some(graph) = map.get(&key) else {
            return;
        };
        if graph.mark_deleted_by_row_id(row_id).is_err() {
            drop(map);
            self.evict_vector_graphs(vec![key]);
        }
    }

    /// Incremental maintenance hook: add a freshly-inserted row's vector to a
    /// *warm* graph. A cold graph is left untouched — the eventual lazy build
    /// includes the row from the heap. Called from the DML insert path after
    /// the row's heap + registry writes. Best-effort: a malformed/missing
    /// vector value is silently skipped (the registry B-tree is the durable
    /// record; the graph is rebuildable).
    #[allow(dead_code)]
    /// Drop a single vector index's in-memory graph(s) (DROP INDEX / failed
    /// build) AND every `.hnsw` snapshot on disk that belongs to it. A
    /// partitioned table has one graph per partition keyed by storage id;
    /// all of them are removed. A no-op if no graph was built; the next
    /// search rebuilds it.
    pub(crate) fn drop_vector_graph(&self, table_id: u64, index_name: &str) {
        let normalized = normalize_name(index_name);
        self.vector_build_epoch
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.vector_indexes
            .write()
            .retain(|(tid, _, _, name), graph| {
                let keep = !(*tid == table_id && *name == normalized);
                if !keep {
                    graph.invalidate();
                }
                keep
            });
        let Some(table) = self.table_by_id(table_id) else {
            return;
        };
        // For partitioned tables there is one file per partition; we delete
        // them all (storage_id == table.id for the unpartitioned case). Both
        // the memory backend's `.hnsw` snapshot and the paged backend's
        // `.hvec` graph file are removed (an index could have been built under
        // either backend); the paged file's buffer-pool pages are evicted
        // first so nothing writes them back to a deleted file.
        for sid in table.storage_ids() {
            self.remove_partition_vector_files(&table, sid, index_name);
        }
    }

    /// Evict derived graph handles when replacing physical table storage.
    /// Callers must establish the appropriate write barrier before eviction.
    pub(crate) fn evict_table_vector_graphs(&self, table_id: u64) {
        self.vector_build_epoch
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.vector_indexes.write().retain(|(tid, _, _, _), graph| {
            let keep = *tid != table_id;
            if !keep {
                graph.invalidate();
            }
            keep
        });
    }

    /// Drop every cached vector graph for a table (DROP TABLE / TRUNCATE)
    /// AND remove every `.hnsw` snapshot / `.hvec` paged graph belonging to
    /// it. Deletes the *exact* per-index, per-storage-id files derived from
    /// the table schema.
    ///
    /// It must never scan directories for a bare `__<table_id>__` substring:
    /// table names may themselves contain `__<digits>`, so such a match would
    /// delete an unrelated table's graph whose name embeds this id — e.g.
    /// dropping id=5 matching `metrics__5__12__vidx.hvec`, or a table literally
    /// named `t__5` colliding with prefix `t__5__`. Under the current drop
    /// ordering (physical cleanup runs before the catalog mutation) the schema
    /// is still available at every call site, so we resolve precise paths.
    pub(crate) fn drop_table_vector_graphs(&self, table: &TableSchema) {
        let table_id = table.id;
        self.vector_build_epoch
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.vector_indexes.write().retain(|(tid, _, _, _), graph| {
            let keep = *tid != table_id;
            if !keep {
                graph.invalidate();
            }
            keep
        });
        for index in &table.indexes {
            if !index.is_vector() {
                continue;
            }
            for sid in table.storage_ids() {
                self.remove_partition_vector_files(table, sid, &index.name);
            }
        }
    }

    /// Background maintenance refreshes requested navigation codes and
    /// retires/repairs tombstones. Compaction has admitted, bounded delta work;
    /// searches keep using the existing graph until a replacement is ready.
    pub(crate) fn maintain_vector_indexes(&self, should_stop: impl Fn() -> bool) {
        self.drain_vector_purges();
        if self.physical_replica_mode() {
            return;
        }
        let compact_disabled = hnsw_compact_disabled();
        let ratio_gate = hnsw_compact_ratio();
        let absolute_gate = u64::from(hnsw_compact_absolute());
        let entries: Vec<((u64, u64, u64, String), Arc<VectorGraph>)> = self
            .vector_indexes
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let threads = hnsw_build_threads();
        for ((table_id, storage_id, generation, index_name), graph) in entries {
            if should_stop() {
                return;
            }
            if let VectorGraph::Memory(memory) = graph.as_ref() {
                memory.refresh_requested_quant();
            }
            if compact_disabled {
                continue;
            }
            let total = graph.node_count() as u64;
            let reclaimable = match graph.as_ref() {
                VectorGraph::Paged(paged) => paged.unrepaired_tombstones(),
                _ => u64::from(graph.stale_count()),
            };
            if reclaimable == 0 || total == 0 {
                continue;
            }
            let ratio = reclaimable as f64 / total as f64;
            if ratio < ratio_gate && reclaimable < absolute_gate {
                continue;
            }
            let Some(table) = self.table_by_id(table_id) else {
                continue;
            };
            if table.active_generation.max(1) != generation {
                continue;
            }
            let suffix = if storage_id == table_id {
                String::new()
            } else {
                format!(" (p={storage_id})")
            };
            let label = format!("{}/{}/{}{}", table.database, table.name, index_name, suffix);
            if matches!(
                graph.as_ref(),
                VectorGraph::NativeHnsw(_) | VectorGraph::NativeIvf(_)
            ) {
                let key = (table_id, storage_id, generation, index_name.clone());
                let Some(index) = table
                    .indexes
                    .iter()
                    .find(|i| i.name.eq_ignore_ascii_case(&index_name))
                else {
                    continue;
                };
                self.evict_vector_graphs(vec![key.clone()]);
                drop(graph);
                if let Err(error) =
                    self.vector_graph_build_and_publish(&table, index, storage_id, key)
                {
                    log_message(format!(
                        "native vector maintenance rebuild failed for {label}: {error}"
                    ));
                }
                continue;
            }
            let reclaimed = match graph.as_ref() {
                VectorGraph::Paged(paged) => match paged.repair_tombstones(&should_stop) {
                    Ok(repaired) => repaired,
                    Err(e) => {
                        log_message(&format!(
                            "hnsw(paged): tombstone repair failed for {label}: {e:?}"
                        ));
                        continue;
                    }
                },
                _ => u64::from(graph.compact(threads)),
            };
            if reclaimed > 0 {
                log_message(&format!(
                    "hnsw: reclaimed {reclaimed} tombstones in {label}"
                ));
            }
        }
    }

    /// Persist one warm graph's snapshot to disk atomically (write to a
    /// temp sibling, fsync, rename). Resets the graph's `dirty_count` and
    /// stamps the `last_persisted_lsn` on success.
    ///
    /// `serialize` and `dirty_count` are read separately, so concurrent
    /// inserts between the two reads can leave the serialised file with
    /// one extra node not counted by `snapshot_dirty`. That single-node
    /// drift is fine: dirty_count is a checkpoint-gate heuristic, not a
    /// safety invariant; the extra node still lands in the file and the
    /// next persist amortises the drift.
    fn vector_graph_persist_one(
        &self,
        table: &TableSchema,
        index_name: &str,
        storage_id: u64,
        graph: &Arc<VectorGraph>,
        end_lsn: u64,
    ) -> std::io::Result<()> {
        // Only the in-memory backend has a `.hnsw` snapshot to write; the
        // paged backend is already durable through the WAL, so its persist
        // hook is a no-op (and the dirty-ratio gate never selects it anyway).
        let Some(graph) = graph.as_memory() else {
            return Ok(());
        };
        let snapshot_dirty = graph.dirty_count();
        let final_path = self.vector_index_path(table, index_name, storage_id);
        if let Some(parent) = final_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp_path = final_path.with_extension("hnsw.tmp");
        {
            let f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            let mut writer = std::io::BufWriter::with_capacity(1 << 20, f);
            graph.serialize_into(&mut writer)?;
            writer.flush()?;
            writer
                .into_inner()
                .map_err(|e| e.into_error())?
                .sync_all()?;
        }
        std::fs::rename(&tmp_path, &final_path)?;
        graph.mark_persisted(end_lsn, snapshot_dirty);
        Ok(())
    }

    /// Walk every cached vector graph, persisting any whose dirty-ratio
    /// crosses the threshold. Called from the checkpoint path with
    /// `end_lsn = CheckpointEnd.lsn`. Errors are logged but never propagated:
    /// the snapshot is derived data, and a failed write is a recall hit on
    /// the next restart (full rebuild), not a correctness bug.
    ///
    /// Skips during physical-replica WAL apply: a replica's `.hnsw` file is
    /// produced locally on first query, never shipped from master, and we
    /// don't want to interleave file IO with apply-thread WAL replay.
    pub(crate) fn vector_graph_persist_all_dirty(&self, end_lsn: u64) {
        if self.physical_replica_mode() {
            return;
        }
        // Snapshot the map keys + Arc clones so we don't hold the
        // `vector_indexes` lock across file IO.
        let entries: Vec<((u64, u64, u64, String), Arc<VectorGraph>)> = self
            .vector_indexes
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for ((table_id, storage_id, _generation, index_name), graph) in &entries {
            if let Err(e) = graph.flush_paged_ivf() {
                log_message(format!(
                    "ivf(paged): checkpoint flush failed for table_id={table_id} storage_id={storage_id} index={index_name}: {e:?}"
                ));
            }
        }
        if !hnsw_persist_enabled() {
            return;
        }
        let threshold = hnsw_persist_dirty_ratio();
        for ((table_id, storage_id, generation, index_name), graph) in entries {
            let dirty = graph.dirty_count() as f64;
            let total = graph.node_count() as f64;
            // Force persist when dirty > 0 and ratio gate met, OR when the
            // graph is new (total == 0 means an empty graph — nothing to
            // persist; skip silently).
            if dirty == 0.0 || total == 0.0 {
                continue;
            }
            if dirty / total < threshold {
                continue;
            }
            let Some(table) = self.table_by_id(table_id) else {
                continue;
            };
            if table.active_generation.max(1) != generation {
                continue;
            }
            if let Err(e) =
                self.vector_graph_persist_one(&table, &index_name, storage_id, &graph, end_lsn)
            {
                let suffix = if storage_id == table_id {
                    String::new()
                } else {
                    format!(" (p={storage_id})")
                };
                log_message(&format!(
                    "hnsw: persist failed for {}/{}/{}{} (will rebuild on restart): {}",
                    table.database, table.name, index_name, suffix, e
                ));
            }
        }
    }

    /// Test helper: read `(node_count, live_count, stale_count)` for a
    /// warm graph. Returns `None` for a cold graph or when no user table
    /// of that name exists. Used by M2 tombstone / compaction tests to
    /// assert state without touching internal HnswIndex fields.
    #[cfg(test)]
    pub(crate) fn hnsw_stats_for_test(
        &self,
        table_name: &str,
        index_name: &str,
    ) -> Option<(usize, usize, u32)> {
        let table_id = self
            .catalog
            .read()
            .ok()?
            .tables
            .values()
            .find(|t| t.name.eq_ignore_ascii_case(table_name))
            .map(|t| t.id)?;
        let normalized = normalize_name(index_name);
        let map = self.vector_indexes.read();
        // Sum stats across every partition graph for this index. The
        // unpartitioned case collapses to a single (table_id, table_id, name)
        // entry, so callers see the same triple as before.
        let mut node_count = 0usize;
        let mut live = 0usize;
        let mut stale = 0u32;
        let mut hit = false;
        for ((tid, _sid, _generation, name), graph) in map.iter() {
            if *tid == table_id && *name == normalized {
                node_count += graph.node_count();
                live += graph.len();
                stale = stale.saturating_add(graph.stale_count());
                hit = true;
            }
        }
        if hit {
            Some((node_count, live, stale))
        } else {
            None
        }
    }

    /// Test helper: block until background vector-index maintenance started
    /// by a checkpoint (compaction) has finished.
    /// Test helper: adjacency entries of a warm paged graph that still point
    /// at tombstones, summed across partitions.
    #[cfg(test)]
    pub(crate) fn paged_edges_into_tombstones_for_test(&self, index_name: &str) -> u64 {
        let normalized = normalize_name(index_name);
        self.vector_indexes
            .read()
            .iter()
            .filter(|((_, _, _, name), _)| *name == normalized)
            .filter_map(|(_, graph)| match graph.as_ref() {
                VectorGraph::Paged(paged) => Some(paged.edges_into_tombstones()),
                _ => None,
            })
            .sum()
    }

    /// Test helper: total WAL bytes appended so far.
    #[cfg(test)]
    pub(crate) fn wal_bytes_for_test(&self) -> u64 {
        self.log_state_atomics.wal_bytes()
    }

    /// Test helper: run one vector maintenance round synchronously (test
    /// engines start without background workers).
    #[cfg(test)]
    pub(crate) fn run_vector_maintenance_for_test(&self) {
        self.maintain_vector_indexes(|| false);
    }

    /// Test helper: warm every Public HNSW graph synchronously regardless
    /// of `MYDB_HNSW_PRELOAD`. Wraps the eager-preload walk so unit tests
    /// can populate `vector_indexes` without relying on the fast-path
    /// bypass that turns ORDER BY DISTANCE LIMIT k into a brute-force
    /// scan (which never visits `vector_graph_get_or_build`).
    #[cfg(test)]
    pub(crate) fn vector_graph_warm_all_for_test(&self) {
        // Startup keeps lightweight catalog headers. Resolve each full
        // descriptor through the bounded catalog cache instead of relying
        // on another accelerator to have eagerly loaded all schemas.
        let ids = self
            .catalog
            .read()
            .ok()
            .map(|catalog| {
                catalog
                    .tables
                    .values()
                    .filter(|table| !table.system)
                    .map(|table| table.id)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for id in ids {
            let Some(table) = self.table_by_id(id) else {
                continue;
            };
            for index in &table.indexes {
                if !index.is_vector() {
                    continue;
                }
                if !matches!(index.state, IndexState::Public) {
                    continue;
                }
                for sid in table.storage_ids() {
                    let _ = self.vector_graph_get_or_build_for_partition(&table, index, sid);
                }
            }
        }
    }

    /// Eager preload: walk every Public HNSW index in the catalog and warm
    /// every partition's graph (load `.hnsw` if present, else rebuild from
    /// heap). Called at startup when `MYDB_HNSW_PRELOAD=eager`. Logs but
    /// tolerates per-index failures: a failed preload just falls back to
    /// lazy on first query.
    pub(crate) fn vector_graph_preload_all(&self) {
        if !hnsw_preload_eager() {
            return;
        }
        self.warm_all_vector_graphs();
    }

    /// Unconditionally warm every Public HNSW graph from the catalog (load the
    /// `.hnsw`/`.hvec` if present, else rebuild from the heap). Best-effort:
    /// per-index failures are logged and skipped (the lazy path rebuilds on
    /// first query). Used by the eager-preload startup walk and by replica
    /// promotion (M8) so a freshly-promoted master answers its first KNN
    /// without a cold-build stall.
    pub(crate) fn warm_all_vector_graphs(&self) {
        // Startup keeps lightweight catalog headers. Resolve each full
        // descriptor through the bounded catalog cache instead of relying
        // on another accelerator to have eagerly loaded all schemas.
        let ids = self
            .catalog
            .read()
            .ok()
            .map(|catalog| {
                catalog
                    .tables
                    .values()
                    .filter(|table| !table.system)
                    .map(|table| table.id)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for id in ids {
            let Some(table) = self.table_by_id(id) else {
                continue;
            };
            for index in &table.indexes {
                if !index.is_vector() {
                    continue;
                }
                if !matches!(index.state, IndexState::Public) {
                    continue;
                }
                for sid in table.storage_ids() {
                    let prepared = self
                        .vector_graph_get_or_build_for_partition(&table, index, sid)
                        .and_then(|graph| graph.prepare_query_navigation());
                    if let Err(e) = prepared {
                        log_message(&format!(
                            "hnsw: eager preload failed for {}/{}/{} (p={}): {:?}",
                            table.database, table.name, index.name, sid, e
                        ));
                    }
                }
            }
        }
    }
}

/// How long a vector graph build that failed memory admission is not retried.
const VECTOR_BUILD_BACKOFF: std::time::Duration = std::time::Duration::from_secs(10);

/// Commit-time collector for superseded graph versions. They cannot be
/// retired yet: the commit is not visible until its frontier is published,
/// so snapshots taken meanwhile still need them. [`Self::finish`] queues them
/// for [`Database::drain_vector_purges`], which runs after publication.
pub(crate) struct VectorRetire<'a> {
    db: &'a Database,
    commit_scn: u64,
    rows: std::cell::RefCell<
        HashMap<(u64, u64, u64, String), Vec<(RowId, smallvec::SmallVec<[u64; 1]>)>>,
    >,
}

impl<'a> VectorRetire<'a> {
    fn new(db: &'a Database, commit_scn: u64) -> Self {
        Self {
            db,
            commit_scn,
            rows: Default::default(),
        }
    }

    fn retire(
        &self,
        key: &(u64, u64, u64, String),
        row_id: RowId,
        versions: smallvec::SmallVec<[u64; 1]>,
    ) {
        if versions.is_empty() {
            return;
        }
        let mut rows = self.rows.borrow_mut();
        match rows.get_mut(key) {
            Some(list) => list.push((row_id, versions)),
            None => {
                rows.insert(key.clone(), vec![(row_id, versions)]);
            }
        }
    }

    fn finish(self) {
        let rows = self.rows.into_inner();
        if rows.is_empty() {
            return;
        }
        let mut queue = self.db.vector_purge_queue.lock();
        for (key, rows) in rows {
            queue.push_back(VectorPurge {
                key,
                rows,
                commit_scn: self.commit_scn,
            });
        }
        self.db
            .vector_purges_pending
            .store(queue.len(), std::sync::atomic::Ordering::Release);
    }
}

/// A completed file is not query-ready until its captured commit tail lands.
struct VectorGraphBuildGuard<'a> {
    graph: &'a VectorGraph,
    published: bool,
}
impl Drop for VectorGraphBuildGuard<'_> {
    fn drop(&mut self) {
        if !self.published {
            self.graph.invalidate();
            if let VectorGraph::Paged(paged) = self.graph {
                if let Err(err) = paged.file().invalidate_build(0) {
                    log_message(format!("HNSW failed build invalidation: {err:?}"));
                }
            }
        }
    }
}

/// Keeps a lazy HNSW build's delta registration alive until it publishes or
/// fails; the registry entry is dropped with its last builder.
struct VectorBuildRegistration<'a> {
    db: &'a Database,
    key: (u64, u64, u64, String),
}

impl<'a> VectorBuildRegistration<'a> {
    fn begin(db: &'a Database, key: (u64, u64, u64, String)) -> Self {
        db.vector_build_deltas
            .lock()
            .entry(key.clone())
            .or_default()
            .builders += 1;
        db.vector_builds_in_flight
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Self { db, key }
    }

    fn ops_since(
        &self,
        applied: u64,
    ) -> Result<(u64, Vec<(RowId, Option<crate::vector::VectorValue>)>), QueryError> {
        let deltas = self.db.vector_build_deltas.lock();
        let Some(delta) = deltas.get(&self.key) else {
            return Ok((applied, Vec::new()));
        };
        if delta.failed {
            return Err(error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "vector build delta exceeded its memory admission",
            ));
        }
        Ok((
            delta.next_seq,
            delta
                .ops
                .iter()
                .filter(|(_, (seq, _))| *seq > applied)
                .map(|(&row, (_, value))| (row, value.clone()))
                .collect(),
        ))
    }
}

impl Drop for VectorBuildRegistration<'_> {
    fn drop(&mut self) {
        let mut deltas = self.db.vector_build_deltas.lock();
        if let Some(delta) = deltas.get_mut(&self.key) {
            delta.builders -= 1;
            if delta.builders == 0 {
                deltas.remove(&self.key);
            }
        }
        self.db
            .vector_builds_in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// Test-only hook run once after a lazy vector graph build finishes and before
/// it is published, to interleave concurrent DML with the publication window.
#[cfg(test)]
pub(crate) static VECTOR_BUILD_BEFORE_PUBLISH_HOOK: parking_lot::Mutex<
    Option<Box<dyn FnOnce() + Send>>,
> = parking_lot::Mutex::new(None);

fn table_has_deferred_ann_indexes(table: &TableSchema) -> bool {
    table
        .indexes
        .iter()
        .any(|index| index.is_vector() || index.is_spatial())
}

#[cfg(test)]
mod hnsw_bulk_policy_tests {
    use super::{hnsw_chunked_scratch_cap, hnsw_preferred_cold_build_mode, HnswColdBuildMode};

    #[test]
    fn chunked_scratch_leaves_governor_headroom() {
        assert_eq!(
            hnsw_chunked_scratch_cap(1024 * 1024 * 1024),
            307 * 1024 * 1024 + 209_715
        );
        assert_eq!(hnsw_chunked_scratch_cap(10), 3);
        assert_eq!(
            hnsw_chunked_scratch_cap(usize::MAX),
            usize::MAX / 10 * 3 + 1
        );
    }

    #[test]
    fn three_level_auto_route_and_forced_modes() {
        assert_eq!(
            hnsw_preferred_cold_build_mode("auto", 80, 40, 100, true),
            HnswColdBuildMode::FullMemory
        );
        assert_eq!(
            hnsw_preferred_cold_build_mode("auto", 120, 80, 100, true),
            HnswColdBuildMode::CompactSq8
        );
        assert_eq!(
            hnsw_preferred_cold_build_mode("auto", 120, 80, 70, true),
            HnswColdBuildMode::ChunkedPaged
        );
        assert_eq!(
            hnsw_preferred_cold_build_mode("auto", 120, 80, 100, false),
            HnswColdBuildMode::ChunkedPaged
        );
        assert_eq!(
            hnsw_preferred_cold_build_mode("memory", usize::MAX, usize::MAX, 1, false),
            HnswColdBuildMode::FullMemory
        );
        for alias in ["chunked", "stream"] {
            assert_eq!(
                hnsw_preferred_cold_build_mode(alias, 1, 1, usize::MAX, true),
                HnswColdBuildMode::ChunkedPaged
            );
        }
    }
}
