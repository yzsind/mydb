//! In-memory HNSW (Hierarchical Navigable Small World) graph for approximate
//! nearest-neighbour search, with a self-describing, CRC32-checked on-disk form.
//!
//! Like the spatial R-tree (`storage/core/rtree.rs`), a vector index is
//! *derived data*: the persisted `.hnsw` file is an optimisation, and a
//! missing / stale / corrupt file is rebuilt from a heap scan at open time.
//! The graph therefore lives entirely outside the buffer-pool / WAL machinery.
//!
//! The graph is a **candidate generator**, not the source of truth. Each node
//! carries a `RowId` and a copy of its vector; [`HnswIndex::search`] returns the
//! nearest candidate `RowId`s, and the executor re-reads each row under MVCC
//! visibility and re-ranks by the *current* heap vector. Consequently:
//!   * Graph maintenance is applied at commit; the executor merges the
//!     reader's own (and in-publication) pending rows.
//!   * DELETE and re-linking UPDATE keep the superseded nodes live until no
//!     snapshot can still see them, then tombstone exactly those versions
//!     (see `Node::seq`). Searches never let tombstones occupy result slots,
//!     and the background maintenance worker compacts the graph.
//!
//! Algorithm: Malkov & Yashunin (2016), "Efficient and robust approximate
//! nearest neighbor search using Hierarchical Navigable Small World graphs."
//! Layer assignment uses a deterministic SplitMix64 PRNG seeded by node index,
//! so rebuilding from the same insertion order reproduces the same graph
//! (important for "derived, reproducible" crash recovery).
//!
//! ## Concurrency (M3, 2026-06)
//!
//! Methods take `&self` so the index can be wrapped in a bare `Arc<HnswIndex>`
//! and shared across threads without an outer write lock. Internal locking:
//!
//!   * `topology` (`parking_lot::Mutex<Topology>`) serialises updates to the
//!     entry-point / max-level pair and the append-slot allocation. Held for
//!     microseconds during INSERT's append phase and at the top of SEARCH to
//!     read a consistent entry snapshot.
//!   * `nodes` ([`NodeSlab`]) is a bucketed, append-only, **lock-free-read**
//!     store of inline `Node`s. `get(id)` is one atomic load + pointer maths
//!     (no `Arc` clone, no lock); `push` is lock-free bar a once-per-bucket
//!     allocation. A coarse `structural` `RwLock<()>` is taken **shared** for a
//!     whole search/insert (so parallel builders never block each other) and
//!     **exclusive** only by the rare ops that free or fully scan the slab.
//!   * Each `Node` carries its own neighbour `parking_lot::Mutex<Vec<Vec<u32>>>`.
//!     When wiring a bidirectional edge `(id, nb)`, we always lock the
//!     two nodes' neighbour mutexes in **ascending id order** — guaranteeing
//!     no two concurrent inserts can deadlock against each other regardless
//!     of which targets they pick.
//!   * `deleted` is an `AtomicBool` so tombstoning a row never blocks readers
//!     or other inserts.
//!   * `live`, `dirty_count`, `stale_count`, `generation`, `last_persisted_lsn`
//!     and `dim` are plain atomics — bookkeeping that doesn't need a lock.
//!
//! Compaction is build-then-swap: survivors are linked into a replacement
//! graph with the parallel cold-build path while DML that lands meanwhile is
//! logged and replayed; only the final replay and the slab swap take the
//! mutation gate and `structural` exclusively.

use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};

use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use super::distance::{
    dot_i8, dot_i8_batch4_ptrs, l2_ssd_i8, ordering_distance, prefetch_read,
    quantize_centered_into, quantize_into, Metric,
};
use crate::storage::RowId;
use crate::types::QueryError;

/// Whether SQ8 graph-navigation quantization is enabled. Reads
/// `MYDB_HNSW_QUANTIZE` on every (re)build decision (cheap, runtime-toggleable
/// from tests / benches); defaults to `true`. L2 and unit-normalized cosine
/// use SQ8 navigation; Dot retains exact navigation.
fn quant_enabled_env() -> bool {
    #[cfg(not(test))]
    {
        static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        return *ENABLED.get_or_init(quant_enabled_env_uncached);
    }
    #[cfg(test)]
    quant_enabled_env_uncached()
}

fn quant_enabled_env_uncached() -> bool {
    super::hnsw_core::quant_enabled_uncached()
}

/// Additive headroom above `m` for the **incremental** insert/UPDATE-re-link
/// path's reverse-edge soft-cap. Re-linking a vector into a mature graph wires
/// it to ~`m` neighbours, and each of those neighbours already holds `m` edges —
/// so every reverse edge overflows by one and, with an eager `m` cap, triggers a
/// full O(m²) diversity prune. That prune is the dominant UPDATE cost once SQ8
/// navigation is in play. Letting a reverse list grow to `m + slack` before
/// pruning amortises the prune across ~`slack` reverse edges (the cold build
/// uses the multiplicative `cap_mult` + a final global prune for the same end).
/// Diversity is preserved — still `prune_select` back to `m` — only the steady-
/// state degree bound rises from `m` to `m + slack`. Cached once: this is on the
/// per-insert path, so a fresh `env::var` per call would show up in the profile.
/// `MYDB_HNSW_INCR_SLACK`, default 8; `0` restores the old eager prune.
fn incremental_soft_cap_slack() -> usize {
    static SLACK: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *SLACK.get_or_init(|| {
        std::env::var("MYDB_HNSW_INCR_SLACK")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(8)
    })
}

/// Drift-adaptive in-place UPDATE: `(enabled, ratio)`. When a row's vector is
/// UPDATEd, the default path tombstones the old node and re-links the new vector
/// (a full HNSW insert — the dominant UPDATE cost). But an incremental embedding
/// refresh usually *nudges* a vector rather than teleporting it: if the move is
/// small relative to the node's nearest-neighbour distance, the existing edges
/// still point at genuine near neighbours, so we can overwrite the node's vector
/// **in place** — keeping its id and adjacency — and skip the re-link entirely.
/// Recall is preserved (edges stay valid; the executor re-ranks by the exact new
/// vector); a large move falls back to the re-link, so random/large-drift updates
/// are unaffected. L2 only (cosine/dot keep the re-link path). `ratio` is the
/// fraction of the nearest-neighbour distance the move may cover and still count
/// as "local". Cached (per-UPDATE path). `MYDB_HNSW_UPDATE_INPLACE` (default on),
/// `MYDB_HNSW_UPDATE_DRIFT_RATIO` (default 0.5).
pub(crate) fn update_inplace_cfg() -> (bool, f32) {
    static CFG: std::sync::OnceLock<(bool, f32)> = std::sync::OnceLock::new();
    *CFG.get_or_init(|| {
        let enabled = std::env::var("MYDB_HNSW_UPDATE_INPLACE")
            .map(|v| !matches!(v.as_str(), "0" | "off" | "OFF" | "false" | "FALSE"))
            .unwrap_or(true);
        let ratio = std::env::var("MYDB_HNSW_UPDATE_DRIFT_RATIO")
            .ok()
            .and_then(|s| s.parse::<f32>().ok())
            .filter(|r| *r >= 0.0 && r.is_finite())
            .unwrap_or(0.5);
        (enabled, ratio)
    })
}

/// Contiguous SQ8 (int8) quantization of the graph's vectors, used to navigate
/// the HNSW graph with 4× less memory traffic and a tight integer SIMD inner
/// loop. Derived data: rebuilt lazily from the f32 node vectors and published
/// behind [`HnswIndex::quant`]; never persisted (the `.hnsw` snapshot stores
/// the exact f32 vectors, so a load just reconstructs this on first search).
///
/// The graph *navigates* on these approximate distances; the executor then
/// re-ranks the small candidate set with exact f32 (re-read from the live heap
/// under MVCC), so recall is preserved — only the traversal is sped up.
struct QuantArena {
    update_epoch: u64,
    /// Codes in fixed `ARENA_CHUNK`-node chunks shared between arena
    /// generations: growth re-publishes the existing chunks and copies at most
    /// one partial chunk, so a refresh never clones the whole arena.
    chunks: Vec<Arc<CodeChunk>>,
    /// Nodes covered (ids `0..n`). Nodes appended after this build fall back to
    /// exact f32 distance until the next rebuild — so the arena is always safe
    /// to use even while the graph grows underneath it.
    n: usize,
    dim: usize,
    /// `scale²` — turns an integer SSD back into ~real squared-L2 units so a
    /// quantized distance and an f32-fallback distance compare correctly inside
    /// the same search heap.
    scale_sq: f32,
    /// Per-probe multiplier applied to the integer SSD to land on the **metric's
    /// ordering scale**, so arena-covered and f32-fallback nodes mix in one
    /// search heap. For L2 this is `scale_sq` (the SSD already *is* squared-L2).
    /// For Cosine the arena holds unit-normalized codes, so the SSD approximates
    /// `‖q̂−n̂‖² = 2(1−cos)`; the extra `×0.5` maps it to `1−cos`, the same value
    /// `ordering_distance(.., Cosine)` returns for un-arena'd nodes.
    dist_mul: f32,
    /// Affine params to quantize a query vector identically (`code_j =
    /// round((x_j - mids[j]) * inv_scale)`): a per-dimension centre with one
    /// shared step, so a few large-offset dimensions do not stretch the
    /// range of every other dimension.
    mids: Vec<f32>,
    inv_scale: f32,
    _charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

/// Nodes per [`CodeChunk`].
const ARENA_CHUNK: usize = 4096;

/// Row-major int8 codes for up to `ARENA_CHUNK` consecutive nodes, plus each
/// node's `‖code‖²` (`Σ code_i²`), so squared-L2 to a query is the exact
/// integer identity `‖q‖² + norm − 2·dot(q, code)` — the per-probe inner loop
/// is then a single `sdot` dot product, ≈2.5× cheaper than the
/// widen-subtract-square SSD and bit-identical to it.
struct CodeChunk {
    codes: Box<[i8]>,
    norms: Box<[i32]>,
}

/// Appends quantized rows chunk by chunk; only the open tail chunk is ever
/// mutable.
struct ArenaCodesBuilder {
    dim: usize,
    chunks: Vec<Arc<CodeChunk>>,
    codes: Vec<i8>,
    norms: Vec<i32>,
}

impl ArenaCodesBuilder {
    fn new(dim: usize) -> Self {
        Self {
            dim,
            chunks: Vec::new(),
            codes: Vec::with_capacity(ARENA_CHUNK * dim),
            norms: Vec::with_capacity(ARENA_CHUNK),
        }
    }

    /// Continue after `arena`'s rows, sharing its sealed chunks.
    fn extend(arena: &QuantArena) -> Self {
        let mut builder = Self::new(arena.dim);
        builder.chunks = arena.chunks.clone();
        if let Some(last) = builder.chunks.last() {
            if last.norms.len() < ARENA_CHUNK {
                builder.codes.extend_from_slice(&last.codes);
                builder.norms.extend_from_slice(&last.norms);
                builder.chunks.pop();
            }
        }
        builder
    }

    /// Append one row, quantized in place by `encode`.
    fn push(&mut self, encode: impl FnOnce(&mut [i8])) {
        let start = self.codes.len();
        self.codes.resize(start + self.dim, 0);
        let row = &mut self.codes[start..];
        encode(row);
        self.norms.push(super::distance::ssq_i8(row));
        if self.norms.len() == ARENA_CHUNK {
            self.seal();
        }
    }

    fn seal(&mut self) {
        let codes = std::mem::replace(&mut self.codes, Vec::with_capacity(ARENA_CHUNK * self.dim));
        let norms = std::mem::replace(&mut self.norms, Vec::with_capacity(ARENA_CHUNK));
        self.chunks.push(Arc::new(CodeChunk {
            codes: codes.into_boxed_slice(),
            norms: norms.into_boxed_slice(),
        }));
    }

    fn rows(&self) -> usize {
        self.chunks.iter().map(|c| c.norms.len()).sum::<usize>() + self.norms.len()
    }

    fn finish(mut self) -> Vec<Arc<CodeChunk>> {
        if !self.norms.is_empty() {
            self.seal();
        }
        self.chunks
    }
}

impl QuantArena {
    #[inline]
    fn codes_for(&self, id: u32) -> &[i8] {
        let id = id as usize;
        let start = (id % ARENA_CHUNK) * self.dim;
        &self.chunks[id / ARENA_CHUNK].codes[start..start + self.dim]
    }

    #[inline]
    fn norm(&self, id: u32) -> i32 {
        let id = id as usize;
        self.chunks[id / ARENA_CHUNK].norms[id % ARENA_CHUNK]
    }

    /// Quantize a query (or a node vector used as one) into `out`.
    #[inline]
    fn quantize(&self, v: &[f32], out: &mut [i8]) {
        quantize_centered_into(v, &self.mids, self.inv_scale, out);
    }

    /// Squared-L2 from a quantized query (codes + precomputed `‖q‖²`) to node
    /// `id`, in integer SSD units (the caller scales by `scale_sq`). Exactly
    /// equal to `l2_ssd_i8(qcodes, codes_for(id))` — same integer value, one
    /// `sdot` instead of the widen-subtract-square loop.
    /// Query and incremental-link probes share metric space and range checks.
    fn prepare_query(&self, metric: Metric, vector: &[f32]) -> Option<(Vec<i8>, i32)> {
        if vector.len() != self.dim {
            return None;
        }
        let mut normalized = Vec::new();
        let source = if metric == Metric::Cosine {
            normalized.extend_from_slice(vector);
            super::distance::normalize_in_place(&mut normalized);
            normalized.as_slice()
        } else {
            vector
        };
        if source
            .iter()
            .zip(&self.mids)
            .any(|(&x, &mid)| !x.is_finite() || ((x - mid) * self.inv_scale).abs() > 127.5)
        {
            return None;
        }
        let mut codes = vec![0; self.dim];
        self.quantize(source, &mut codes);
        let norm = super::distance::ssq_i8(&codes);
        Some((codes, norm))
    }

    #[inline]
    fn ssd_from_query(&self, qcodes: &[i8], qnorm: i32, id: u32) -> i32 {
        qnorm + self.norm(id) - 2 * dot_i8(qcodes, self.codes_for(id))
    }
}

/// First-pass statistics for the memory-bounded paged-HNSW cold builder.
#[derive(Clone, Debug)]
pub(crate) struct CompactHnswStats {
    metric: Metric,
    rows: usize,
    dim: usize,
    vmin: f32,
    vmax: f32,
    valid: bool,
}

impl CompactHnswStats {
    pub(crate) fn new(metric: Metric) -> Self {
        Self {
            metric,
            rows: 0,
            dim: 0,
            vmin: f32::INFINITY,
            vmax: f32::NEG_INFINITY,
            valid: metric != Metric::Dot,
        }
    }

    pub(crate) fn observe(&mut self, vector: &[f32]) {
        if vector.is_empty() {
            return;
        }
        if self.dim == 0 {
            self.dim = vector.len();
        } else if self.dim != vector.len() {
            self.valid = false;
            return;
        }
        let norm = if self.metric == Metric::Cosine {
            super::distance::norm(vector)
        } else {
            1.0
        };
        let inv_norm = if norm > 0.0 { norm.recip() } else { 1.0 };
        for &x in vector {
            let x = (x as f64 * inv_norm) as f32;
            if !x.is_finite() {
                self.valid = false;
                return;
            }
            self.vmin = self.vmin.min(x);
            self.vmax = self.vmax.max(x);
        }
        self.rows += 1;
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows
    }

    pub(crate) fn dim(&self) -> usize {
        self.dim
    }

    pub(crate) fn quantizable(&self) -> bool {
        self.valid
            && self.rows > 0
            && self.dim > 0
            && self.vmin.is_finite()
            && self.vmax.is_finite()
            && self.vmax > self.vmin
    }

    /// Global SQ8 parameters derived from the complete first-pass range.
    /// Chunked builds must reuse these for every chunk so codes retained from
    /// older chunks remain comparable with the current query/node codes.
    pub(crate) fn quantization_params(&self) -> Option<(f32, f32, f32)> {
        if !self.quantizable() {
            return None;
        }
        let mid = (self.vmax as f64 * 0.5 + self.vmin as f64 * 0.5) as f32;
        let inv_scale = 254.0 / (self.vmax - self.vmin);
        let scale_sq = inv_scale.recip().powi(2);
        let dist_mul = if self.metric == Metric::Cosine {
            scale_sq * 0.5
        } else {
            scale_sq
        };
        (mid.is_finite() && inv_scale.is_normal() && dist_mul.is_normal())
            .then_some((mid, inv_scale, dist_mul))
    }

    /// Conservative capacity envelope. Exact f32 vectors are absent.
    pub(crate) fn estimated_bytes(&self, m: usize, threads: usize) -> usize {
        let m = m.max(2);
        let m0 = m.saturating_mul(2);
        let codes = self.rows.saturating_mul(self.dim);
        let norms = self.rows.saturating_mul(std::mem::size_of::<i32>());
        // The 2× edge allowance covers deferred reverse edges and Vec growth.
        let per_node = 256usize
            .saturating_add(m0.saturating_mul(std::mem::size_of::<u32>() * 2))
            .saturating_add(m.saturating_mul(std::mem::size_of::<u32>() * 2))
            .saturating_add(m0.saturating_mul(std::mem::size_of::<f64>() * 2));
        let topology = self.rows.saturating_mul(per_node);
        let scratch = threads
            .max(1)
            .saturating_mul(self.rows.saturating_mul(std::mem::size_of::<u32>()))
            .saturating_add(threads.max(1).saturating_mul(256 * 1024));
        codes
            .saturating_add(norms)
            .saturating_add(topology)
            .saturating_add(scratch)
            .saturating_add(2 * 1024 * 1024)
    }
}

/// Ephemeral SQ8-only graph builder for paged cold builds.
pub(crate) struct CompactHnswBuilder {
    stats: CompactHnswStats,
    codes: ArenaCodesBuilder,
    mid: f32,
    inv_scale: f32,
    scale_sq: f32,
    normalized: Vec<f32>,
    // Drop the graph (and its prepaid charge) after the encoding buffers.
    graph: HnswIndex,
}

impl CompactHnswBuilder {
    pub(crate) fn new(
        stats: CompactHnswStats,
        m: usize,
        ef_construction: usize,
        charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    ) -> Option<Self> {
        if !stats.quantizable() {
            return None;
        }
        let mid = (stats.vmax + stats.vmin) * 0.5;
        let inv_scale = 254.0 / (stats.vmax - stats.vmin);
        let scale = inv_scale.recip();
        stats.rows.checked_mul(stats.dim)?;
        let rows = stats.rows;
        let dim = stats.dim;
        Some(Self {
            graph: HnswIndex::new_compact(stats.metric, m, ef_construction, dim)
                .with_build_reservation(charge, rows),
            stats,
            codes: ArenaCodesBuilder::new(dim),
            mid,
            inv_scale,
            scale_sq: scale * scale,
            normalized: Vec::with_capacity(dim),
        })
    }

    pub(crate) fn push(&mut self, row_id: RowId, vector: &[f32]) -> Result<(), &'static str> {
        if vector.len() != self.stats.dim || self.codes.rows() >= self.stats.rows {
            return Err("compact HNSW input changed between scan passes");
        }
        let src = if self.stats.metric == Metric::Cosine {
            self.normalized.clear();
            self.normalized.extend_from_slice(vector);
            super::distance::normalize_in_place(&mut self.normalized);
            self.normalized.as_slice()
        } else {
            vector
        };
        let (mid, inv_scale) = (self.mid, self.inv_scale);
        self.codes
            .push(|row| quantize_into(src, mid, inv_scale, row));
        self.graph
            .append_compact_node(row_id)
            .map_err(|_| "compact HNSW memory reservation failed")?;
        Ok(())
    }

    pub(crate) fn finish(mut self, threads: usize) -> Result<HnswIndex, &'static str> {
        if self.codes.rows() != self.stats.rows {
            return Err("compact HNSW row count changed between scan passes");
        }
        let dist_mul = if self.stats.metric == Metric::Cosine {
            self.scale_sq * 0.5
        } else {
            self.scale_sq
        };
        *self.graph.quant.write() = Some(Arc::new(QuantArena {
            _charge: None,
            update_epoch: 0,
            chunks: std::mem::replace(&mut self.codes, ArenaCodesBuilder::new(0)).finish(),
            n: self.stats.rows,
            dim: self.stats.dim,
            scale_sq: self.scale_sq,
            dist_mul,
            // The paged file persists one global centre.
            mids: vec![self.mid; self.stats.dim],
            inv_scale: self.inv_scale,
        }));
        self.graph.link_all(threads.max(1));
        Ok(self.graph)
    }
}

/// Per-search distance context. Carries the f32 query (always, for the exact
/// fallback and for un-quantized tail nodes) and, when SQ8 navigation is
/// active, a borrowed arena + the quantized query codes. Build/insert pass a
/// quant-less probe so graph construction always uses exact f32 distances
/// (build runs once; quality matters more than its speed).
struct Probe<'a> {
    query: &'a [f32],
    /// When SQ8 navigation is active: the arena, the quantized query codes, and
    /// the query's precomputed `‖q‖²` (so each probe is one `sdot` + two adds).
    quant: Option<(&'a QuantArena, &'a [i8], i32)>,
}

impl Probe<'_> {
    /// Best-effort prefetch of node `id`'s int8 code row, so the random arena
    /// fetch overlaps the distance computation of the previous neighbour. No-op
    /// when SQ8 navigation is inactive or `id` isn't arena-covered. Timing hint
    /// only — never affects which nodes are scored or the distances returned.
    #[inline(always)]
    fn prefetch(&self, id: u32) {
        if let Some((arena, _, _)) = self.quant {
            if (id as usize) < arena.n {
                let codes = arena.codes_for(id);
                // Fetch the full code before its batched dot product. Only
                // unvisited neighbours are prefetched by the layer search.
                for offset in (0..codes.len()).step_by(64) {
                    prefetch_read(codes.as_ptr().wrapping_add(offset));
                }
            }
        }
    }
}

const HNSW_MAGIC: u32 = 0x484E_5731; // "HNW1"
/// On-disk format version. v2 (M1, 2026-06) added a header epilogue
/// (`generation`, `last_persisted_lsn`) so a loaded graph can name the
/// WAL position it reflects. v1 was test-only (`#[allow(dead_code)]`); we
/// reject it on load — there is no v1 file in any production deployment.
use crate::storage::format_versions::IN_MEMORY_HNSW_VERSION as HNSW_VERSION;

/// Default max neighbours per node above layer 0.
pub(crate) const DEFAULT_M: usize = 16;
/// Default size of the dynamic candidate list during construction.
pub(crate) const DEFAULT_EF_CONSTRUCTION: usize = 200;
/// Default size of the dynamic candidate list during search (≥ k).
pub(crate) const DEFAULT_EF_SEARCH: usize = 64;

/// Minimum nodes per worker on the [`HnswIndex::link_all`] Phase-C link
/// fan-out. Below this the graph links serially — the per-link cost is large,
/// but for a tiny graph the `thread::scope` spawn/join still isn't worth it.
const HNSW_BULK_LINK_MIN_PER_THREAD: usize = 256;

/// Batch size for the atomic work-claiming loop that drives the parallel
/// Phase-C link and Phase-D prune. Workers claim `[lo, lo+BATCH)` id ranges
/// off a shared cursor instead of pre-slicing equal contiguous chunks, so fast
/// cores drain more batches than slow ones. This matters on heterogeneous CPUs
/// (e.g. Apple's 4P+6E) and on skewed per-node link cost, where equal chunks
/// leave the last worker straggling. The batch amortises the `fetch_add` over
/// enough per-node work that the atomic traffic is negligible.
const HNSW_LINK_CLAIM_BATCH: u32 = 64;

/// Run `f(id)` for every `id` in `[start, end)` across `threads` scoped
/// workers, each claiming `HNSW_LINK_CLAIM_BATCH`-sized id ranges off a shared
/// atomic cursor. Dynamic claiming (vs equal contiguous chunks) keeps fast
/// cores busy when workers finish at different rates — heterogeneous P/E cores
/// or skewed per-id cost. The set of ids processed is identical to a serial
/// loop; only the concurrent interleaving differs (and parallel build is
/// already interleaving-nondeterministic), so callers that were recall-neutral
/// under static chunking stay so. Assumes `threads >= 1` and `start < end`.
fn parallel_claim_ids(start: u32, end: u32, threads: usize, f: impl Fn(u32) + Sync) {
    let cursor = AtomicU32::new(start);
    std::thread::scope(|scope| {
        for _ in 0..threads {
            let cursor = &cursor;
            let f = &f;
            scope.spawn(move || loop {
                let lo = cursor.fetch_add(HNSW_LINK_CLAIM_BATCH, Ordering::Relaxed);
                if lo >= end {
                    break;
                }
                let hi = (lo + HNSW_LINK_CLAIM_BATCH).min(end);
                for id in lo..hi {
                    f(id);
                }
            });
        }
    });
}

/// Cold-build neighbour-list soft-cap multiplier. During [`HnswIndex::link_all`]
/// a node's reverse-edge list grows to `mult · m` before the O(m²) selection
/// heuristic prunes it back to `m`, amortising that heuristic — the profiled
/// build hotspot — across ~`(mult-1)·m` additions instead of running it on every
/// reverse edge. A final Phase-D pass restores every list to ≤ m, so the shipped
/// graph's degree is unchanged. `1` disables amortisation (eager pruning).
const HNSW_BUILD_SOFT_CAP_MULT: usize = 2;

/// Cold-build deferred-diversity link default. When on, [`HnswIndex::link_all`]
/// keeps every navigable list at ≤ m during the link fan-out via a cheap
/// drop-farthest (parking the evicted reverse edges), so navigation traverses
/// ≤ m-degree lists — about half the per-neighbour random arena fetches that
/// dominate the build — and the parked edges are merged back and diversity-pruned
/// once in Phase D. The shipped graph is the same degree-bounded structure (the
/// Phase-D candidate set is a superset of the soft-cap path's), so recall is
/// unchanged. `MYDB_HNSW_BUILD_DEFERRED=0` restores the soft-cap path.
const HNSW_BUILD_DEFERRED: bool = true;

/// Default navigable headroom above `m` for the deferred build (see
/// [`BuildAux::nav_slack`]). A small value keeps the during-build scaffolding
/// rich enough that recall has margin over the soft-cap path while still cutting
/// navigation degree roughly in half.
const HNSW_BUILD_NAV_SLACK: usize = 0;

/// Build-local scaffolding for the deferred-diversity cold build. Allocated in
/// [`HnswIndex::link_all`] and dropped before the index serves queries.
pub(crate) struct BuildAux {
    /// `pending[id]` collects `(level, neighbour)` reverse edges that the
    /// drop-farthest cap evicted from `id`'s navigable list, for the Phase-D
    /// merge that restores full diversity.
    pending: Vec<Mutex<Vec<(u8, u32)>>>,
    /// `ndist[id][level][k]` mirrors the owner→neighbour distance of
    /// `Node.neighbors[id][level][k]`, so the drop-farthest eviction is an O(m)
    /// float scan rather than an O(m) arena-fetch recomputation. By convention
    /// `ndist[id]` is only ever touched while holding `neighbors[id]`'s lock, so
    /// its own mutex is always effectively uncontended.
    ndist: Vec<Mutex<Vec<Vec<f64>>>>,
    /// Extra navigable headroom kept above `m` during the link (the list is
    /// capped at `m + nav_slack` closest, not `m`). A little headroom keeps the
    /// during-build navigation scaffolding richer — closer to the soft-cap path's
    /// — so recall has margin, at a small navigation-degree cost. Phase D still
    /// prunes every list to ≤ m, so the shipped degree is unchanged.
    nav_slack: usize,
}

#[inline]
fn pack_row(r: RowId) -> u64 {
    (u64::from(r.page_id) << 32) | u64::from(r.slot_id)
}

#[inline]
fn unpack_row(v: u64) -> RowId {
    RowId {
        page_id: (v >> 32) as u32,
        slot_id: v as u16,
    }
}

/// A `(distance, node-id)` pair ordered by distance. Used in the search heaps.
/// Distances are finite (inputs are validated finite), so `total_cmp` is a
/// total order with no NaN surprises.
type Scored = super::hnsw_core::Scored<u32>;

/// Reusable per-thread scratch for one graph traversal (a `search_layer` /
/// `greedy_descend` call chain). Hoisting these allocations out of the hot
/// loops is the single biggest build-throughput win: a 20k-node build runs
/// ~40k `search_layer` calls, and a `HashSet` + two `BinaryHeap`s allocated
/// per call dominated the profile.
///
/// `visited` is a **visit-generation array** (hnswlib's trick): `visited[id]`
/// holds the generation stamp of the traversal that last touched node `id`.
/// "Have I seen `id`?" is then a branchless `visited[id] == gen` — no hashing,
/// no allocation, and resetting between traversals is just `gen += 1`. The
/// array grows on demand (concurrent inserts can wire a neighbour id past the
/// length captured at `begin`), and a `gen` wrap clears it every 255 traversals.
struct SearchScratch {
    // Byte stamps keep random visited probes cache-resident. The occasional
    // wrap clears sequentially (one byte per node per 255 traversals).
    visited: Vec<u8>,
    gen: u8,
    frontier: BinaryHeap<Reverse<Scored>>,
    results: BinaryHeap<Scored>,
    nbr_buf: Vec<u32>,
}

impl SearchScratch {
    fn new() -> Self {
        SearchScratch {
            visited: Vec::new(),
            gen: 0,
            frontier: BinaryHeap::new(),
            results: BinaryHeap::new(),
            nbr_buf: Vec::new(),
        }
    }

    /// Start a fresh traversal: bump the generation (clearing all "visited"
    /// marks in O(1)) and empty the heaps. `node_count` pre-sizes `visited`.
    #[inline]
    fn begin(&mut self, node_count: usize) {
        if self.visited.len() < node_count {
            self.visited.resize(node_count, 0);
        }
        self.gen = self.gen.wrapping_add(1);
        if self.gen == 0 {
            // Generation wrapped — every stamp is stale; clear and restart at 1.
            self.visited.iter_mut().for_each(|v| *v = 0);
            self.gen = 1;
        }
        self.frontier.clear();
        self.results.clear();
    }

    /// Mark `id` visited; return `true` if it was newly visited this traversal.
    /// Grows `visited` if `id` is past the length captured at `begin` (a
    /// concurrent insert wired a higher-id neighbour mid-traversal).
    #[inline]
    fn visit(&mut self, id: u32) -> bool {
        let i = id as usize;
        if i >= self.visited.len() {
            self.visited.resize(i + 1, 0);
        }
        if self.visited[i] == self.gen {
            false
        } else {
            self.visited[i] = self.gen;
            true
        }
    }
}

thread_local! {
    /// Per-thread traversal scratch, reused across every insert/search on this
    /// thread (and across graphs — `begin` resets it by generation, so stale
    /// stamps from another graph never read as visited). Build worker threads
    /// and DML/insert threads both get amortised-zero allocation this way.
    static SCRATCH: RefCell<SearchScratch> = RefCell::new(SearchScratch::new());
}

/// Per-node state. Stored **inline** in the [`NodeSlab`] at a stable address
/// (segments never move once allocated), so navigation borrows `&Node`
/// lock-free — no `Arc` clone, no slab lock per distance.
struct Node {
    update_epoch: AtomicU64,
    /// Graph-unique version handle, preserved by compaction; deferred MVCC
    /// purges retire exactly the versions they captured at commit.
    seq: u64,
    row_id: RowId,
    /// Per-node vector latch lets small in-place UPDATEs proceed under the
    /// graph's shared structural guard instead of pausing every search.
    vector: RwLock<Vec<f32>>,
    /// `neighbors[l]` is the adjacency list at layer `l`, for `l` in
    /// `0..=node_level`. Each node has its own mutex so wiring at node A
    /// doesn't block wiring at node B.
    neighbors: Mutex<Vec<Vec<u32>>>,
    /// Soft-delete tombstone. Search skips these. Atomic so DELETE/UPDATE
    /// can mark without taking any other lock.
    deleted: AtomicBool,
}

/// Slots in the first slab bucket. Bucket `b` holds `SLAB_BASE << b` inline
/// `Node`s in one allocation that never moves or frees until the whole slab
/// drops, so a `&Node` borrowed from it stays valid for the slab's lifetime.
const SLAB_BASE: usize = 4096;
/// Geometric buckets cover every `u32` node id with a 21-pointer directory:
/// small graphs pay nothing for the headroom, and a bucket's allocation is
/// left uninitialised so only written slots become resident.
const SLAB_BUCKETS: usize = 21;
const SLAB_CAPACITY: usize = u32::MAX as usize;
const ROW_INDEX_SHARDS: usize = 32;

/// Bucket and in-bucket offset of slab slot `id`.
#[inline]
fn slab_slot(id: usize) -> (usize, usize) {
    let block = id / SLAB_BASE + 1;
    let bucket = (usize::BITS - 1 - block.leading_zeros()) as usize;
    (bucket, id - SLAB_BASE * ((1 << bucket) - 1))
}

#[inline]
fn slab_bucket_len(bucket: usize) -> usize {
    SLAB_BASE << bucket
}

#[inline]
fn row_index_shard(key: u64) -> usize {
    (key as usize ^ (key >> 32) as usize) & (ROW_INDEX_SHARDS - 1)
}

/// Lock-free-read, append-only node store.
///
/// `get(id)` and `len()` are lock-free (one atomic load + pointer arithmetic):
/// buckets never move, and any id reachable by a reader (via a neighbour list
/// or the entry point) was fully written and published — through the per-node
/// neighbour `Mutex` / topology `Mutex` release-acquire edge — before it could
/// be observed. `push` is lock-free except for the rare bucket allocation
/// (one `grow` mutex acquisition per bucket).
///
/// This replaces the old `RwLock<Vec<Arc<Node>>>` whose per-insert `write()`
/// serialised concurrent builders (measured: 5-worker build 3.5× *slower* than
/// single-threaded). With lock-free reads, the [`HnswIndex::structural`] lock
/// is taken once per *whole* search/insert (shared — builders don't block each
/// other) and only the rare structural ops (compact / clear / full iteration)
/// take it exclusively.
struct NodeSlab {
    dir: [std::sync::atomic::AtomicPtr<Node>; SLAB_BUCKETS],
    /// Claimed slots, which may still be under construction.
    len: AtomicUsize,
    /// Per-bucket initialization flags; published independently of topology.
    ready: [std::sync::atomic::AtomicPtr<AtomicBool>; SLAB_BUCKETS],
    published: AtomicUsize,
    /// Serialises *bucket allocation* only — not appends, not reads.
    grow: Mutex<()>,
}

// SAFETY: `Node` is `Send + Sync` (it owns only `Send + Sync` members). The
// raw bucket pointers are managed so that concurrent access is synchronised
// by the publication edges described on `NodeSlab`; the slab never hands out
// `&mut Node`.
unsafe impl Send for NodeSlab {}
unsafe impl Sync for NodeSlab {}

impl NodeSlab {
    fn new() -> Self {
        NodeSlab {
            dir: std::array::from_fn(|_| std::sync::atomic::AtomicPtr::new(std::ptr::null_mut())),
            ready: std::array::from_fn(|_| std::sync::atomic::AtomicPtr::new(std::ptr::null_mut())),
            published: AtomicUsize::new(0),
            len: AtomicUsize::new(0),
            grow: Mutex::new(()),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    /// A contiguous initialized prefix. A delayed writer leaves a hole; readers
    /// stop there and use exact distance for later, topology-published nodes.
    fn published_len(&self) -> usize {
        let start = self.published.load(Ordering::Acquire);
        let mut end = start;
        let claimed = self.len();
        while end < claimed {
            let (bucket, off) = slab_slot(end);
            let flags = self.ready[bucket].load(Ordering::Acquire);
            // SAFETY: allocated flag buckets hold `slab_bucket_len` atomics and
            // structural excludes bucket reclamation throughout this scan.
            if flags.is_null() || !unsafe { &*flags.add(off) }.load(Ordering::Acquire) {
                break;
            }
            end += 1;
        }
        if end == start {
            start // Read-only queries do not contend on a shared atomic write.
        } else {
            self.published.fetch_max(end, Ordering::AcqRel).max(end)
        }
    }

    /// Append a node, returning its id. The node is built by `make` from its
    /// freshly-assigned id (the HNSW level assignment is seeded by it). Lock-
    /// free except for the at-most-once-per-bucket allocation.
    fn push_with(&self, make: impl FnOnce(u32) -> Node) -> Result<u32, QueryError> {
        let id = self
            .len
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < SLAB_CAPACITY).then_some(current + 1)
            })
            .map_err(|_| {
                crate::storage::error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    format!("HNSW graph exceeded its {SLAB_CAPACITY} node id space"),
                )
            })?;
        let node = make(id as u32);
        let (bucket, off) = slab_slot(id);
        let mut base = self.dir[bucket].load(Ordering::Acquire);
        if base.is_null() {
            let _g = self.grow.lock();
            base = self.dir[bucket].load(Ordering::Acquire);
            if base.is_null() {
                let layout = std::alloc::Layout::array::<Node>(slab_bucket_len(bucket)).unwrap();
                // SAFETY: non-zero layout; null is checked.
                base = unsafe { std::alloc::alloc(layout) as *mut Node };
                if base.is_null() {
                    std::alloc::handle_alloc_error(layout);
                }
                let flags: Box<[AtomicBool]> = (0..slab_bucket_len(bucket))
                    .map(|_| AtomicBool::new(false))
                    .collect();
                self.ready[bucket]
                    .store(Box::into_raw(flags) as *mut AtomicBool, Ordering::Release);
                self.dir[bucket].store(base, Ordering::Release);
            }
        }
        // SAFETY: `off` is inside the bucket, the slot is uninitialised, and no
        // other thread can target the same id (unique `fetch_update`). The
        // write is published to readers via the neighbour/topology
        // release-acquire edges before `id` appears anywhere a reader can
        // reach it.
        unsafe { std::ptr::write(base.add(off), node) };
        let flags = self.ready[bucket].load(Ordering::Acquire);
        // SAFETY: flags hold the bucket's initialized atomics and live with the slab.
        unsafe { &*flags.add(off) }.store(true, Ordering::Release);
        Ok(id as u32)
    }

    /// Borrow node `id`. Lock-free.
    ///
    /// Invariant (upheld by every caller): `id` was produced by a prior `push`
    /// and reached here either through a neighbour list / entry point (so its
    /// slot is written and visible), through `published_len`'s acquire edge,
    /// or in a structural write-locked full scan. All callers also exclude
    /// concurrent `clear` with structural or exclusive ownership.
    #[inline]
    fn get(&self, id: u32) -> &Node {
        let id = id as usize;
        debug_assert!(id < self.len(), "hnsw node id {id} out of range");
        let (bucket, off) = slab_slot(id);
        let base = self.dir[bucket].load(Ordering::Acquire);
        debug_assert!(!base.is_null());
        // SAFETY: see method invariant. The bucket is allocated (id < len) and
        // never moves; the slot was written before publication.
        unsafe { &*base.add(off) }
    }

    /// Drop every node and free every bucket, resetting to empty. **Caller
    /// must hold `structural` write** (excludes all readers/appenders).
    fn clear_locked(&self) {
        self.published.store(0, Ordering::Release);
        let len = self.len.swap(0, Ordering::AcqRel);
        for bucket in 0..SLAB_BUCKETS {
            let start = SLAB_BASE * ((1 << bucket) - 1);
            if start >= len {
                break;
            }
            let base = self.dir[bucket].swap(std::ptr::null_mut(), Ordering::AcqRel);
            if base.is_null() {
                continue;
            }
            let flags = self.ready[bucket].swap(std::ptr::null_mut(), Ordering::AcqRel);
            let capacity = slab_bucket_len(bucket);
            let count = (len - start).min(capacity);
            let layout = std::alloc::Layout::array::<Node>(capacity).unwrap();
            // SAFETY: exclusive access; flagged slots of this bucket were
            // written by `push`.
            unsafe {
                for off in 0..count {
                    if (*flags.add(off)).load(Ordering::Acquire) {
                        std::ptr::drop_in_place(base.add(off));
                    }
                }
                std::alloc::dealloc(base as *mut u8, layout);
                drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                    flags, capacity,
                )));
            }
        }
    }

    /// Exchange every bucket with another unpublished/exclusively-guarded
    /// slab. Both callers must exclude append/read operations. Swapping atomic
    /// directory entries keeps the slab allocation ownership explicit: when
    /// the temporary graph drops it frees the old graph's buckets.
    fn swap_locked(&self, other: &Self) {
        let _a = self.grow.lock();
        let _b = other.grow.lock();
        for (a, b) in self.dir.iter().zip(other.dir.iter()) {
            let ap = a.swap(std::ptr::null_mut(), Ordering::AcqRel);
            let bp = b.swap(ap, Ordering::AcqRel);
            a.store(bp, Ordering::Release);
        }
        for (a, b) in self.ready.iter().zip(other.ready.iter()) {
            let ap = a.swap(std::ptr::null_mut(), Ordering::AcqRel);
            let bp = b.swap(ap, Ordering::AcqRel);
            a.store(bp, Ordering::Release);
        }
        let published = self
            .published
            .swap(other.published.load(Ordering::Acquire), Ordering::AcqRel);
        other.published.store(published, Ordering::Release);
        let a_len = self
            .len
            .swap(other.len.load(Ordering::Acquire), Ordering::AcqRel);
        other.len.store(a_len, Ordering::Release);
    }
}

impl Drop for NodeSlab {
    fn drop(&mut self) {
        // &mut self ⇒ exclusive; reuse the same teardown as `clear_locked`.
        self.clear_locked();
    }
}

/// The fields shared by every concurrent insert/search: entry point and
/// max level. A `Mutex` guards reads/writes of the *pair* so a search
/// always sees a consistent snapshot (we never publish a `max_level` that
/// outlives its `entry_point`).
struct Topology {
    entry_point: Option<u32>,
    max_level: usize,
}

/// An in-memory HNSW graph over `f32` vectors keyed by `RowId`.
pub(crate) struct HnswIndex {
    valid: AtomicBool,
    metric: Metric,
    m: usize,
    m0: usize,
    ef_construction: usize,
    /// `1 / ln(m)` — the level-assignment normalisation factor.
    ml: f64,
    /// Dimension; 0 until the first insert fixes it via CAS.
    dim: AtomicUsize,
    /// Topology consistency point — `(entry_point, max_level)`. Held briefly
    /// during INSERT's append phase and at the top of SEARCH; compaction
    /// holds it across the slab clear.
    topology: Mutex<Topology>,
    /// Lock-free-read, append-only node store (see [`NodeSlab`]).
    nodes: NodeSlab,
    /// Guards the *structure* of the slab against the rare ops that free or
    /// fully scan it. Search and insert take it **shared** for the whole
    /// operation (so concurrent builders never block each other, and a
    /// `&Node` they hold can't be freed mid-flight); `compact`'s clear,
    /// `serialize`, and the full-slab scans (`mark_deleted`, `row_ids`,
    /// `stale_ratio`) take it **exclusive**. Per-distance `get`s inside a held
    /// read guard are lock-free.
    structural: RwLock<()>,
    /// Serializes graph mutations against build-then-swap compaction without
    /// blocking searches. Incremental INSERT/DELETE/UPDATE take shared;
    /// compact takes exclusive only while snapshotting and publishing; its
    /// mutation epoch rejects replacements made stale during the unlocked build.
    mutation_gate: RwLock<()>,
    /// Live (non-deleted) node count.
    live: AtomicUsize,
    /// Monotonic counter incremented on every successful `mark_persisted`.
    /// Lets observers tell a freshly-loaded graph from one that has been
    /// rewritten in-memory.
    generation: AtomicU64,
    /// WAL LSN that the persisted snapshot reflects (the `end_lsn` of the
    /// checkpoint that wrote it). 0 means "never persisted". Used by
    /// startup catch-up to decide which heap rows post-date the snapshot.
    last_persisted_lsn: AtomicU64,
    /// Nodes inserted since the last `mark_persisted` call. Drives the
    /// dirty-ratio gate for opportunistic checkpoint persistence.
    dirty_count: AtomicU32,
    /// Tombstoned node count. Drives the stale-ratio gate for opportunistic
    /// compaction at checkpoint. Not persisted to the `.hnsw` file:
    /// deserialisation recomputes it from the node flags so the runtime stat
    /// doesn't need its own version bump.
    stale_count: AtomicU32,
    /// Published SQ8 navigation arena (L2/cosine), or `None` when quantization is
    /// disabled / not yet built / degenerate. Read-cloned (cheap `Arc`) at the
    /// top of each search; rebuilt lazily when enough new nodes have accrued.
    quant: RwLock<Option<Arc<QuantArena>>>,
    /// Serialises arena (re)builds so concurrent searches don't each rebuild.
    quant_build: Mutex<()>,
    quant_async: AtomicBool,
    quant_refresh_requested: AtomicBool,
    update_epoch: AtomicU64,
    mutation_epoch: AtomicU64,
    /// Maps `pack_row(row_id)` → every node id appended for that key. An UPDATE
    /// appends a new node and tombstones the old one, so a key can map to
    /// several ids (typically one live + a few tombstones). This turns
    /// `mark_deleted_by_row_id` from an O(node_count) slab scan (which dominated
    /// batch DELETE / UPDATE at scale — O(N·M) for N rows over an M-node graph)
    /// into an O(versions-per-row) lookup. Maintained on every `append_node`,
    /// cleared + rebuilt by `compact`, and reconstructed at deserialise. Guarded
    /// by its own `Mutex` (not `structural`) so concurrent appenders only
    /// contend on a fast hash-map insert.
    row_index: [Mutex<HashMap<u64, Vec<u32>>>; ROW_INDEX_SHARDS],
    /// Capacity reservation for the memory-resident graph. It follows the
    /// graph until the last `Arc` drops and is grown in chunks so parallel
    /// builders do not take this mutex per node.
    resident_charge: Mutex<Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>>,
    resident_reserved_nodes: AtomicUsize,
    /// Set for the duration of a compaction rebuild. Mutations then append the
    /// node ids they touch to `compaction_log`, which the rebuild replays into
    /// its replacement before publishing it, so concurrent DML never cancels
    /// or is lost by compaction.
    compacting: AtomicBool,
    compaction_log: Mutex<Option<HashSet<u32>>>,
    compaction_log_cap: AtomicUsize,
    compaction_cancelled: AtomicBool,
    /// Next node version handle (see `Node::seq`).
    next_seq: AtomicU64,
    /// Topology-only graph built from SQ8 codes (paged cold build): nodes
    /// carry no exact vectors, so edge selection must use the codes.
    codes_only: bool,
    // Temporary cold-build graphs own the caller's admitted workspace. Keep
    // this last so all graph allocations are freed before releasing admission.
    build_reservation: Option<BuildReservation>,
}

struct BuildReservation {
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    remaining_nodes: AtomicUsize,
}

impl std::fmt::Debug for HnswIndex {
    /// Compact summary — the node/neighbour vectors are large and uninteresting
    /// in a `Database` debug dump (which derives `Debug` over this field).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let topo = self.topology.lock();
        let nodes_len = self.nodes.len();
        f.debug_struct("HnswIndex")
            .field("metric", &self.metric)
            .field("dim", &self.dim.load(Ordering::Acquire))
            .field("m", &self.m)
            .field("nodes", &nodes_len)
            .field("live", &self.live.load(Ordering::Acquire))
            .field("max_level", &topo.max_level)
            .finish()
    }
}

impl HnswIndex {
    pub(crate) fn new(metric: Metric, m: usize, ef_construction: usize) -> Self {
        let m = m.max(2);
        HnswIndex {
            valid: AtomicBool::new(true),
            metric,
            m,
            m0: m * 2,
            ef_construction: ef_construction.max(m),
            ml: 1.0 / (m as f64).ln(),
            dim: AtomicUsize::new(0),
            topology: Mutex::new(Topology {
                entry_point: None,
                max_level: 0,
            }),
            nodes: NodeSlab::new(),
            structural: RwLock::new(()),
            mutation_gate: RwLock::new(()),
            live: AtomicUsize::new(0),
            generation: AtomicU64::new(0),
            last_persisted_lsn: AtomicU64::new(0),
            dirty_count: AtomicU32::new(0),
            stale_count: AtomicU32::new(0),
            quant: RwLock::new(None),
            quant_build: Mutex::new(()),
            quant_async: AtomicBool::new(false),
            quant_refresh_requested: AtomicBool::new(false),
            update_epoch: AtomicU64::new(0),
            mutation_epoch: AtomicU64::new(0),
            row_index: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            resident_charge: Mutex::new(None),
            resident_reserved_nodes: AtomicUsize::new(0),
            compacting: AtomicBool::new(false),
            compaction_log: Mutex::new(None),
            compaction_log_cap: AtomicUsize::new(0),
            compaction_cancelled: AtomicBool::new(false),
            next_seq: AtomicU64::new(0),
            codes_only: false,
            build_reservation: None,
        }
    }

    fn resident_capacity_bytes(&self, nodes: usize, dim: usize) -> usize {
        if nodes == 0 {
            return 0;
        }
        let directory = SLAB_BUCKETS
            .saturating_mul(2 * std::mem::size_of::<std::sync::atomic::AtomicPtr<Node>>());
        // Bucket allocations are uninitialised: only written slots (and the
        // eagerly zeroed ready flags of allocated buckets) become resident.
        let (bucket, _) = slab_slot(nodes - 1);
        let flags = SLAB_BASE * ((2 << bucket) - 1);
        let slab = nodes
            .div_ceil(SLAB_BASE)
            .saturating_mul(SLAB_BASE)
            .saturating_mul(std::mem::size_of::<Node>())
            .saturating_add(flags.saturating_mul(std::mem::size_of::<AtomicBool>()));
        let max_level = (-1e-12_f64.ln() * self.ml).floor() as usize;
        let neighbor_capacity = self
            .m0
            .saturating_add(max_level.saturating_mul(self.m))
            .saturating_mul(std::mem::size_of::<u32>());
        let per_node_heap = dim
            .saturating_mul(std::mem::size_of::<f32>())
            .saturating_add((max_level + 1).saturating_mul(std::mem::size_of::<Vec<u32>>()))
            .saturating_add(neighbor_capacity)
            .saturating_add(std::mem::size_of::<(u64, Vec<u32>)>());
        directory
            .saturating_add(slab)
            .saturating_add(nodes.saturating_mul(per_node_heap))
    }

    /// Transfer already-admitted construction memory, never bypass admission.
    /// These graphs are transcribed to disk and dropped before serving DML.
    pub(crate) fn with_build_reservation(
        mut self,
        charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
        rows: usize,
    ) -> Self {
        assert_eq!(self.nodes.len(), 0);
        self.build_reservation = Some(BuildReservation {
            _charge: charge,
            remaining_nodes: AtomicUsize::new(rows),
        });
        self
    }

    fn reserve_resident_node(&self, dim: usize) -> Result<(), QueryError> {
        if let Some(reservation) = &self.build_reservation {
            return reservation
                .remaining_nodes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .map(|_| ())
                .map_err(|_| {
                    crate::storage::error(
                        crate::error::catalog::ER_OUTOFMEMORY,
                        "HNSW build exceeded its reserved row capacity",
                    )
                });
        }
        const RESERVATION_CHUNK: usize = 1024;
        let required = self.nodes.len().saturating_add(1);
        if required <= self.resident_reserved_nodes.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut charge = self.resident_charge.lock();
        if required <= self.resident_reserved_nodes.load(Ordering::Acquire) {
            return Ok(());
        }
        let reserved = required.div_ceil(RESERVATION_CHUNK) * RESERVATION_CHUNK;
        let target_bytes = self.resident_capacity_bytes(reserved, dim);
        match charge.as_mut() {
            Some(existing) => {
                let extra = target_bytes.saturating_sub(existing.bytes());
                existing.try_grow(extra)?;
            }
            None => {
                *charge = Some(
                    crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                        crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
                        target_bytes,
                    )?,
                );
            }
        }
        self.resident_reserved_nodes
            .store(reserved, Ordering::Release);
        Ok(())
    }

    fn new_compact(metric: Metric, m: usize, ef_construction: usize, dim: usize) -> Self {
        let mut index = Self::new(metric, m, ef_construction);
        index.dim.store(dim, Ordering::Release);
        index.codes_only = true;
        index
    }

    /// Arena for build-time edge selection. SQ8 drives navigation, but the
    /// diversity heuristic and pruning compare node pairs exactly whenever the
    /// graph holds f32 vectors: selecting edges on doubly-quantized distances
    /// measurably degraded high-dimensional graphs (768-d cosine recall@10 at
    /// ef=64: 0.98 vs 0.9995) for a modest share of build time.
    fn selection_arena<'a>(&self, arena: Option<&'a QuantArena>) -> Option<&'a QuantArena> {
        arena.filter(|_| self.codes_only)
    }

    /// Generation counter — incremented on every `mark_persisted`. Consumed
    /// by tests and by the M7 observability layer (TBD); kept here as the
    /// authoritative reader for the atomic.
    #[allow(dead_code)]
    pub(crate) fn invalidate(&self) {
        self.valid.store(false, Ordering::Release);
    }
    pub(crate) fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }

    pub(crate) fn matches_configuration(
        &self,
        metric: Metric,
        m: usize,
        ef: usize,
        dim: Option<usize>,
    ) -> bool {
        self.metric == metric
            && self.m == m.max(2)
            && self.ef_construction == ef.max(m.max(2))
            && dim.is_none_or(|dim| self.dim() == 0 || self.dim() == dim)
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// WAL LSN that the persisted snapshot reflects (the `end_lsn` of the
    /// checkpoint that wrote it). 0 if never persisted in this process's
    /// lifetime AND the graph was not loaded from disk. Consumed by tests
    /// and the future M7 observability surface.
    #[allow(dead_code)]
    pub(crate) fn last_persisted_lsn(&self) -> u64 {
        self.last_persisted_lsn.load(Ordering::Acquire)
    }

    /// Inserts since the last `mark_persisted`. Drives the dirty-ratio gate.
    pub(crate) fn dirty_count(&self) -> u32 {
        self.dirty_count.load(Ordering::Acquire)
    }

    /// Mark the graph as persisted at WAL `lsn`. Bumps the generation,
    /// stamps the LSN, and *subtracts* `consumed_dirty` from the dirty
    /// counter — the value captured under the read lock at serialize time,
    /// which excludes any concurrent inserts that landed between the
    /// serialize and this stamp. Saturating, so a stale `consumed_dirty`
    /// can never push the counter negative.
    pub(crate) fn mark_persisted(&self, lsn: u64, consumed_dirty: u32) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.last_persisted_lsn.store(lsn, Ordering::Release);
        let mut cur = self.dirty_count.load(Ordering::Acquire);
        loop {
            let new_val = cur.saturating_sub(consumed_dirty);
            match self.dirty_count.compare_exchange_weak(
                cur,
                new_val,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Tombstoned-node count. Maintained by `mark_deleted_by_row_id` /
    /// reset by `compact`; recomputed at deserialise time from the per-node
    /// `deleted` flag.
    pub(crate) fn stale_count(&self) -> u32 {
        self.stale_count.load(Ordering::Acquire)
    }

    /// Make `row_id`'s graph entry match a committed heap vector. An identical
    /// live node keeps its topology; otherwise every node of the row is
    /// tombstoned and the vector re-linked.
    pub(crate) fn reconcile_heap_vector(
        &self,
        row_id: RowId,
        vector: Vec<f32>,
    ) -> Result<(), QueryError> {
        self.reconcile_heap_vector_marked(row_id, vector, None)
    }

    /// Start reconciling a loaded snapshot against the heap: one mark per
    /// snapshot node, set when a heap row claims it.
    pub(crate) fn begin_heap_reconcile(&self) -> Vec<AtomicBool> {
        (0..self.nodes.len())
            .map(|_| AtomicBool::new(false))
            .collect()
    }

    /// [`Self::reconcile_heap_vector`] that also records which snapshot node
    /// the heap row kept, for [`Self::finish_heap_reconcile`].
    pub(crate) fn reconcile_heap_vector_marked(
        &self,
        row_id: RowId,
        vector: Vec<f32>,
        marks: Option<&[AtomicBool]>,
    ) -> Result<(), QueryError> {
        let unchanged = {
            let _g = self.structural.read();
            let key = pack_row(row_id);
            let index = self.row_index[row_index_shard(key)].lock();
            index.get(&key).and_then(|ids| {
                ids.iter().copied().find(|&id| {
                    let node = self.nodes.get(id);
                    !node.deleted.load(Ordering::Acquire) && *node.vector.read() == vector
                })
            })
        };
        match unchanged {
            Some(id) => {
                if let Some(mark) = marks.and_then(|marks| marks.get(id as usize)) {
                    mark.store(true, Ordering::Release);
                }
            }
            None => {
                self.mark_deleted_by_row_id(row_id);
                self.insert(row_id, vector)?;
            }
        }
        Ok(())
    }

    /// Tombstone every live snapshot node no heap row claimed: rows deleted
    /// (or superseded duplicates) after the snapshot was written. Returns the
    /// number of nodes tombstoned.
    pub(crate) fn finish_heap_reconcile(&self, marks: &[AtomicBool]) -> u32 {
        let mut stale = 0;
        for (id, mark) in marks.iter().enumerate() {
            if mark.load(Ordering::Acquire) {
                continue;
            }
            let _mutation = self.mutation_gate.read();
            let _structural = self.structural.read();
            if self
                .nodes
                .get(id as u32)
                .deleted
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                stale += 1;
                self.note_compaction_touch(id as u32);
            }
        }
        if stale > 0 {
            self.mutation_epoch.fetch_add(1, Ordering::AcqRel);
            self.live.fetch_sub(stale as usize, Ordering::AcqRel);
            self.stale_count.fetch_add(stale, Ordering::AcqRel);
        }
        stale
    }

    /// Dense node count for the paged cold-build transcriber. The caller only
    /// uses this after all append/link workers have joined, so every id in
    /// `0..build_node_count()` is stable for the duration of transcription.
    pub(crate) fn build_node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Final entry point in dense in-memory node-id space.
    pub(crate) fn build_entry(&self) -> Option<(u32, usize)> {
        let _g = self.structural.read();
        let topology = self.topology.lock();
        topology
            .entry_point
            .map(|entry| (entry, topology.max_level))
    }

    /// Top layer for one finalized cold-build node. Kept separate from
    /// [`Self::copy_build_node_into`] so the paged direct writer can calculate
    /// every stable on-disk vid before it maps dense neighbour ids.
    pub(crate) fn build_node_top_layer(&self, id: usize) -> usize {
        let _g = self.structural.read();
        self.nodes
            .get(id as u32)
            .neighbors
            .lock()
            .len()
            .saturating_sub(1)
    }

    /// Copy one finalized cold-build node into caller-owned reusable buffers.
    /// This deliberately avoids exporting a second table-sized vector batch or
    /// a `Vec<HnswBuildNodeLinks>`: the paged writer needs only one node while
    /// assembling its current 16 KiB output page.
    pub(crate) fn copy_build_node_into(
        &self,
        id: usize,
        vector: &mut Vec<f32>,
        neighbors: &mut Vec<Vec<u32>>,
    ) -> RowId {
        let _g = self.structural.read();
        let node = self.nodes.get(id as u32);
        let src_vector = node.vector.read();
        vector.clear();
        vector.extend_from_slice(&src_vector);
        drop(src_vector);

        let src_neighbors = node.neighbors.lock();
        neighbors.resize_with(src_neighbors.len(), Vec::new);
        for (dst, src) in neighbors.iter_mut().zip(src_neighbors.iter()) {
            dst.clear();
            dst.extend_from_slice(src);
        }
        neighbors.truncate(src_neighbors.len());
        node.row_id
    }

    /// Copy finalized topology without reading the compact builder's empty
    /// exact-vector slot.
    pub(crate) fn copy_build_links_into(&self, id: usize, neighbors: &mut Vec<Vec<u32>>) -> RowId {
        let _g = self.structural.read();
        let node = self.nodes.get(id as u32);
        let src_neighbors = node.neighbors.lock();
        neighbors.resize_with(src_neighbors.len(), Vec::new);
        for (dst, src) in neighbors.iter_mut().zip(src_neighbors.iter()) {
            dst.clear();
            dst.extend_from_slice(src);
        }
        neighbors.truncate(src_neighbors.len());
        node.row_id
    }

    /// Tombstone every live node whose key matches `row_id`. UPDATE marks
    /// the old vector before inserting the new one; DELETE just marks.
    ///
    /// Returns the number of nodes newly marked (0 for a cold-graph
    /// no-op or a row that was already tombstoned).
    pub(crate) fn mark_deleted_by_row_id(&self, row_id: RowId) -> u32 {
        let _mutation = self.mutation_gate.read();
        // O(versions-per-row) via the `row_index` map instead of an O(node_count)
        // slab scan. `structural` is taken **shared** (not exclusive): the map's
        // node ids are all from completed `push`es, so `nodes.get` is sound under
        // a read guard, and the only op that frees the slab (`compact`'s clear)
        // takes `structural` exclusive — so it can't run concurrently. Shared
        // means concurrent searches and other row deletes don't serialise on us.
        // `deleted` is an `AtomicBool`, so the CAS itself needs no outer lock.
        let mut marked = 0u32;
        {
            // Hold `structural` shared across *both* the map read and the CAS
            // loop: it excludes `compact`'s clear (which frees the slab and
            // clears the map under `structural` exclusive), so every id we pull
            // from the map stays a valid slab slot for the whole loop.
            let _g = self.structural.read();
            let key = pack_row(row_id);
            let ids = match self.row_index[row_index_shard(key)].lock().get(&key) {
                Some(v) => v.clone(),
                None => return 0,
            };
            for &id in &ids {
                if self
                    .nodes
                    .get(id)
                    .deleted
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    marked = marked.saturating_add(1);
                    self.note_compaction_touch(id);
                }
            }
            // Tombstones retain their vectors and edges until compaction. Keep
            // the entry as a navigation anchor; result materialization filters
            // deleted rows. Deleting an entry therefore never scans the graph.
        }
        if marked > 0 {
            self.mutation_epoch.fetch_add(1, Ordering::AcqRel);
            self.live.fetch_sub(marked as usize, Ordering::AcqRel);
            self.stale_count.fetch_add(marked, Ordering::AcqRel);
        }
        marked
    }

    /// Version handles of `row_id`'s live nodes, for a deferred MVCC purge.
    pub(crate) fn live_versions(&self, row_id: RowId) -> smallvec::SmallVec<[u64; 1]> {
        let _g = self.structural.read();
        let key = pack_row(row_id);
        self.row_index[row_index_shard(key)]
            .lock()
            .get(&key)
            .map(|ids| {
                ids.iter()
                    .map(|&id| self.nodes.get(id))
                    .filter(|node| !node.deleted.load(Ordering::Acquire))
                    .map(|node| node.seq)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Tombstone the live nodes of `row_id` whose handle is listed; newer
    /// versions inserted after the handles were captured are untouched.
    pub(crate) fn retire_versions(&self, row_id: RowId, seqs: &[u64]) -> u32 {
        let _mutation = self.mutation_gate.read();
        let mut marked = 0u32;
        {
            let _g = self.structural.read();
            let key = pack_row(row_id);
            let ids = match self.row_index[row_index_shard(key)].lock().get(&key) {
                Some(v) => v.clone(),
                None => return 0,
            };
            for id in ids {
                let node = self.nodes.get(id);
                if seqs.contains(&node.seq)
                    && node
                        .deleted
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                {
                    marked += 1;
                    self.note_compaction_touch(id);
                }
            }
        }
        if marked > 0 {
            self.mutation_epoch.fetch_add(1, Ordering::AcqRel);
            self.live.fetch_sub(marked as usize, Ordering::AcqRel);
            self.stale_count.fetch_add(marked, Ordering::AcqRel);
        }
        marked
    }

    pub(crate) fn has_live_row_id(&self, row_id: RowId) -> bool {
        let _g = self.structural.read();
        let key = pack_row(row_id);
        self.row_index[row_index_shard(key)]
            .lock()
            .get(&key)
            .is_some_and(|ids| {
                ids.iter()
                    .any(|&id| !self.nodes.get(id).deleted.load(Ordering::Acquire))
            })
    }

    /// Drift-adaptive in-place vector update. For every live node keyed by
    /// `row_id`, if the new vector's move is small relative to the node's
    /// nearest-neighbour distance (`drift ≤ ratio · d_nn`), overwrite the node's
    /// f32 vector **in place** — same id, same edges — and return `true`. A move
    /// too large to keep the edges representative leaves the node untouched and
    /// returns `false`, so the caller re-links via the tombstone + insert path.
    ///
    /// Skips the dominant UPDATE cost (a full HNSW re-link) for the common case
    /// of an incremental embedding refresh. Recall is preserved: the edges still
    /// point at genuine near neighbours of the (barely moved) vector, and the
    /// executor re-ranks returned candidates by the exact, now-updated vector.
    /// L2 only — Dot/Cosine return `false` (their "distance" isn't a metric we
    /// threshold here, so they keep the exact re-link path).
    ///
    /// Takes `structural` shared and the affected nodes' vector latches. This
    /// keeps topology/slab lifetime stable while unrelated searches continue.
    pub(crate) fn try_update_in_place(
        &self,
        row_id: RowId,
        new_vector: &[f32],
        ratio: f32,
    ) -> bool {
        let _mutation = self.mutation_gate.read();
        if self.metric != Metric::L2 {
            return false;
        }
        let _g = self.structural.read();
        let key = pack_row(row_id);
        let ids = match self.row_index[row_index_shard(key)].lock().get(&key) {
            Some(v) => v.clone(),
            None => return false,
        };
        // `ordering_distance` for L2 is *squared* L2, so the `drift ≤ ratio·d_nn`
        // test in distance units becomes `drift_sq ≤ ratio²·d_nn_sq`.
        let ratio_sq = (ratio as f64).powi(2);
        let mut eligible = Vec::with_capacity(ids.len());
        for &id in &ids {
            let node = self.nodes.get(id);
            let current = node.vector.read();
            if node.deleted.load(Ordering::Acquire) || current.len() != new_vector.len() {
                continue;
            }
            let drift_sq = ordering_distance(&current, new_vector, self.metric);
            // Nearest live neighbour distance at layer 0, from the current vector.
            let mut d_nn_sq = f64::INFINITY;
            {
                let nbrs = node.neighbors.lock();
                if let Some(layer0) = nbrs.first() {
                    for &nb in layer0 {
                        let nbn = self.nodes.get(nb);
                        if nbn.deleted.load(Ordering::Acquire) {
                            continue;
                        }
                        let neighbor = nbn.vector.read();
                        let d = ordering_distance(&current, &neighbor, self.metric);
                        if d < d_nn_sq {
                            d_nn_sq = d;
                        }
                    }
                }
            }
            // No usable local scale (isolated node) or move too large → re-link.
            if !super::hnsw_core::permits_in_place_update(drift_sq, d_nn_sq, ratio_sq) {
                return false;
            }
            eligible.push(id);
        }
        if eligible.is_empty() {
            return false;
        }
        // Commit only after every live version passed the drift gate. This
        // preserves the all-or-nothing contract for duplicate row-id nodes:
        // callers may safely tombstone+relink after a false return without a
        // previously visited node having been modified.
        for id in eligible {
            let node = self.nodes.get(id);
            // Older immutable arenas must use the exact vector for this node.
            // Publish its epoch under the vector latch before the new contents.
            let mut vector = node.vector.write();
            let epoch = self.update_epoch.fetch_add(1, Ordering::AcqRel) + 1;
            node.update_epoch.store(epoch, Ordering::Release);
            vector.copy_from_slice(new_vector);
            drop(vector);
            self.mutation_epoch.fetch_add(1, Ordering::AcqRel);
            self.dirty_count.fetch_add(1, Ordering::AcqRel);
            self.note_compaction_touch(id);
        }
        true
    }

    /// Rebuild the graph keeping only non-tombstoned nodes and return the
    /// number of tombstones dropped (0 when nothing was published).
    ///
    /// Build-then-swap: survivors are appended to a fresh graph and linked
    /// with the parallel cold-build path (the same topology quality as
    /// `CREATE INDEX`), while searches and DML keep using this graph. DML that
    /// lands meanwhile is logged by node id and replayed into the replacement;
    /// only the final replay and the swap hold the mutation gate exclusively.
    ///
    /// The persistence bookkeeping (`generation`, `last_persisted_lsn`) is
    /// preserved; the swapped-in dirty counter makes the next checkpoint
    /// rewrite the snapshot.
    #[cfg(test)]
    pub(crate) fn compact(&self) -> u32 {
        self.compact_with_threads(1)
    }

    pub(crate) fn compact_with_threads(&self, threads: usize) -> u32 {
        self.compact_with_rebuild_hook(threads, || {})
    }

    fn compact_with_rebuild_hook(&self, threads: usize, after_rebuild: impl FnOnce()) -> u32 {
        if self.stale_count.load(Ordering::Acquire) == 0
            || self.compacting.swap(true, Ordering::AcqRel)
        {
            return 0;
        }
        let mut workspace_charge;
        struct Reset<'a>(&'a HnswIndex);
        impl Drop for Reset<'_> {
            fn drop(&mut self) {
                *self.0.compaction_log.lock() = None;
                self.0.compacting.store(false, Ordering::Release);
            }
        }
        let _reset = Reset(self);
        let snapshot_len = {
            let _mutation = self.mutation_gate.write();
            let count = self.nodes.len();
            let capacity = count.saturating_mul(2).saturating_add(1024);
            workspace_charge =
                match crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    capacity
                        .saturating_mul(40)
                        .saturating_add(count.saturating_mul(4)),
                ) {
                    Some(charge) => charge,
                    None => return 0,
                };
            self.compaction_log_cap.store(capacity, Ordering::Release);
            self.compaction_cancelled.store(false, Ordering::Release);
            *self.compaction_log.lock() = Some(HashSet::new());
            count
        };
        let replacement = HnswIndex::new(self.metric, self.m, self.ef_construction);
        // Old node id -> replacement id; `u32::MAX` marks a node with no live
        // counterpart (tombstoned at snapshot, or appended later and not yet
        // replayed).
        let mut remap = vec![u32::MAX; snapshot_len];
        {
            let _structural = self.structural.read();
            for (id, slot) in remap.iter_mut().enumerate() {
                let node = self.nodes.get(id as u32);
                if node.deleted.load(Ordering::Acquire) {
                    continue;
                }
                let vector = node.vector.read().clone();
                match replacement.append_node_with_seq(node.row_id, vector, node.seq) {
                    Ok(new_id) => *slot = new_id,
                    Err(_) => return 0,
                }
            }
        }
        replacement.link_all(threads);
        after_rebuild();
        let replay = |remap: &mut Vec<u32>| -> Result<usize, QueryError> {
            if self.compaction_cancelled.load(Ordering::Acquire) {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    "HNSW compaction delta exceeded its admitted capacity",
                ));
            }
            let batch: Vec<u32> = self
                .compaction_log
                .lock()
                .as_mut()
                .map(|log| log.drain().collect())
                .unwrap_or_default();
            let count = batch.len();
            for id in batch {
                self.replay_compaction_touch(&replacement, remap, id)?;
            }
            Ok(count)
        };
        // Coalesce repeated touches by id and bound catch-up rounds. Aborting
        // leaves the original graph intact when sustained writers outrun us.
        for _ in 0..8 {
            match replay(&mut remap) {
                Ok(0) => break,
                Ok(_) => {}
                Err(_) => return 0,
            }
        }
        let _mutation = self.mutation_gate.write();
        if self
            .compaction_log
            .lock()
            .as_ref()
            .is_some_and(|log| log.len() > 4096)
        {
            return 0;
        }
        if replay(&mut remap).is_err() {
            return 0;
        }
        let _ = &mut workspace_charge;
        // Tombstones made during the rebuild were replayed and survive.
        let dropped = self
            .stale_count
            .load(Ordering::Acquire)
            .saturating_sub(replacement.stale_count.load(Ordering::Acquire));
        {
            let _old_structural = self.structural.write();
            let _new_structural = replacement.structural.write();
            self.nodes.swap_locked(&replacement.nodes);
            self.update_epoch.store(
                replacement.update_epoch.load(Ordering::Acquire),
                Ordering::Release,
            );
            self.mutation_epoch.fetch_add(1, Ordering::AcqRel);
            std::mem::swap(
                &mut *self.topology.lock(),
                &mut *replacement.topology.lock(),
            );
            for shard in 0..ROW_INDEX_SHARDS {
                std::mem::swap(
                    &mut *self.row_index[shard].lock(),
                    &mut *replacement.row_index[shard].lock(),
                );
            }
            std::mem::swap(&mut *self.quant.write(), &mut *replacement.quant.write());

            let swap_atomic_usize = |a: &AtomicUsize, b: &AtomicUsize| {
                let av = a.swap(b.load(Ordering::Acquire), Ordering::AcqRel);
                b.store(av, Ordering::Release);
            };
            let swap_atomic_u32 = |a: &AtomicU32, b: &AtomicU32| {
                let av = a.swap(b.load(Ordering::Acquire), Ordering::AcqRel);
                b.store(av, Ordering::Release);
            };
            swap_atomic_usize(&self.dim, &replacement.dim);
            swap_atomic_usize(&self.live, &replacement.live);
            swap_atomic_u32(&self.dirty_count, &replacement.dirty_count);
            swap_atomic_u32(&self.stale_count, &replacement.stale_count);
            // Keep each graph's charge with the slab it now owns.
            std::mem::swap(
                &mut *self.resident_charge.lock(),
                &mut *replacement.resident_charge.lock(),
            );
            swap_atomic_usize(
                &self.resident_reserved_nodes,
                &replacement.resident_reserved_nodes,
            );
        }
        dropped
    }

    /// Bring the replacement's copy of old node `id` up to date with the
    /// live graph: link a node appended during the rebuild, tombstone one
    /// deleted since the snapshot, or copy an in-place vector update.
    fn replay_compaction_touch(
        &self,
        replacement: &HnswIndex,
        remap: &mut Vec<u32>,
        id: u32,
    ) -> Result<(), QueryError> {
        let _structural = self.structural.read();
        let node = self.nodes.get(id);
        let slot = id as usize;
        if slot >= remap.len() {
            remap.resize(slot + 1, u32::MAX);
        }
        let deleted = node.deleted.load(Ordering::Acquire);
        let target = remap[slot];
        if target == u32::MAX {
            // Tombstoned at snapshot (tombstones are never revived), or
            // appended since and not yet replayed.
            if !deleted {
                let vector = node.vector.read().clone();
                let new_id = replacement.append_node_with_seq(node.row_id, vector, node.seq)?;
                replacement.link_node(new_id);
                remap[slot] = new_id;
            }
            return Ok(());
        }
        if deleted {
            replacement.tombstone_node(target);
            return Ok(());
        }
        let current = node.vector.read();
        let copy = replacement.nodes.get(target);
        if *copy.vector.read() != *current {
            let epoch = replacement.update_epoch.fetch_add(1, Ordering::AcqRel) + 1;
            let mut vector = copy.vector.write();
            copy.update_epoch.store(epoch, Ordering::Release);
            vector.copy_from_slice(&current);
        }
        Ok(())
    }

    /// Tombstone one node by id (compaction replay).
    fn tombstone_node(&self, id: u32) {
        let _structural = self.structural.read();
        if self
            .nodes
            .get(id)
            .deleted
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            self.live.fetch_sub(1, Ordering::AcqRel);
            self.stale_count.fetch_add(1, Ordering::AcqRel);
        }
    }

    pub(crate) fn dim(&self) -> usize {
        self.dim.load(Ordering::Acquire)
    }

    /// Live (non-tombstoned) node count. Exercised by the recall tests; also
    /// the natural hook for a future `SHOW`/diagnostics surface.
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.live.load(Ordering::Acquire)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Total node count including tombstones — used by the dirty-ratio gate
    /// in the checkpoint persist path (`dirty_count / node_count` decides
    /// whether to write `.hnsw`). Also the input to a future staleness-driven
    /// rebuild trigger (design §9).
    pub(crate) fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Deterministic SplitMix64-based level assignment, seeded by node index.
    fn random_level(&self, node_id: u32) -> usize {
        super::hnsw_core::random_level(node_id as u64, self.ml, super::hnsw_paged::VEC_MAX_LAYER)
    }

    /// Borrow node `id` directly from the lock-free slab.
    #[inline]
    fn node_at(&self, id: u32) -> &Node {
        self.nodes.get(id)
    }

    #[inline]
    fn with_node_vector<T>(&self, id: u32, f: impl FnOnce(&[f32]) -> T) -> T {
        f(&self.nodes.get(id).vector.read())
    }

    #[inline]
    fn quant_covers(&self, arena: &QuantArena, id: u32) -> bool {
        (id as usize) < arena.n
            && (self.update_epoch.load(Ordering::Acquire) == arena.update_epoch
                || self.nodes.get(id).update_epoch.load(Ordering::Acquire) <= arena.update_epoch)
    }

    /// Distance from a search probe to node `id` — a lock-free slab index.
    /// Uses the SQ8 arena when active and `id` is covered, else exact f32.
    #[inline]
    fn probe_dist(&self, probe: &Probe, id: u32) -> f64 {
        if let Some((arena, qcodes, qnorm)) = probe.quant {
            if self.quant_covers(arena, id) {
                // Norm-decomposed integer SSD (one `sdot`), scaled back to ~real
                // squared-L2 so it ranks the same as (and compares with) the
                // exact f32 fallback. Bit-identical to the widen-subtract SSD.
                return arena.ssd_from_query(qcodes, qnorm, id) as f64 * (arena.dist_mul as f64);
            }
        }
        self.with_node_vector(id, |v| ordering_distance(probe.query, v, self.metric))
    }

    /// Build (or refresh, when enough new nodes have accrued) the SQ8 arena and
    /// return the published copy. Returns `None` when quantization is disabled,
    /// the metric isn't L2, the graph is empty, or the value range is
    /// degenerate. The caller must hold `structural` shared so the slab can't be
    /// freed mid-scan.
    fn ensure_quant(&self) -> Option<Arc<QuantArena>> {
        self.ensure_quant_mode(false)
    }

    #[cfg(test)]
    pub(crate) fn enable_background_quant(&self) {
        self.set_background_quant(true);
    }

    pub(crate) fn set_background_quant(&self, active: bool) {
        self.quant_async.store(active, Ordering::Release);
    }

    pub(crate) fn refresh_requested_quant(&self) {
        if self.quant_refresh_requested.swap(false, Ordering::AcqRel) {
            let _guard = self.structural.read();
            self.ensure_quant_mode(true);
        }
    }

    fn ensure_quant_mode(&self, background: bool) -> Option<Arc<QuantArena>> {
        // L2 uses the SSD identity directly; Cosine reuses it by quantizing
        // unit-normalized vectors (so the integer SSD ranks by cosine). Dot has
        // no scale-invariant normalization trick, so it stays on exact f32.
        if matches!(self.metric, Metric::Dot) || !quant_enabled_env() {
            return None;
        }
        let normalize = self.metric == Metric::Cosine;
        let dim = self.dim.load(Ordering::Acquire);
        if dim == 0 {
            return None;
        }
        // Fresh-enough fast path: arena covers all but a small fraction of nodes.
        let fresh_enough = |arena: &QuantArena, len: usize| -> bool {
            (arena.n >= len || (len - arena.n).saturating_mul(8) < len)
                && self
                    .update_epoch
                    .load(Ordering::Acquire)
                    .saturating_sub(arena.update_epoch)
                    < (len / 8).max(1) as u64
        };
        {
            let len = self.nodes.published_len();
            let cur = self.quant.read();
            if let Some(arena) = cur.as_ref() {
                if fresh_enough(arena, len) {
                    return Some(arena.clone());
                }
            }
        }
        if !background && self.quant_async.load(Ordering::Acquire) {
            self.quant_refresh_requested.store(true, Ordering::Release);
            return self.quant.read().clone();
        }
        // (Re)build under the build mutex; re-check in case a peer just built it.
        let Some(_b) = self.quant_build.try_lock() else {
            // One reader refreshes; peers retain the old codes plus exact
            // fallbacks for appended/updated nodes instead of queuing on it.
            return self.quant.read().clone();
        };
        let update_epoch = self.update_epoch.load(Ordering::Acquire);
        let len = self.nodes.published_len();
        {
            let cur = self.quant.read();
            if let Some(arena) = cur.as_ref() {
                if fresh_enough(arena, len) {
                    return Some(arena.clone());
                }
            }
        }
        if len == 0 {
            return None;
        }
        let arena_charge = if self.build_reservation.is_some() {
            None
        } else {
            let bytes = len
                .saturating_mul(dim.saturating_add(4))
                .saturating_add(ARENA_CHUNK.saturating_mul(dim.saturating_add(4)))
                .saturating_add(dim.saturating_mul(24));
            Some(
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
                    bytes,
                )?,
            )
        };
        // Materialise node `id`'s arena row into `buf` — the raw vector for L2,
        // unit-normalized for Cosine — and return its used width. A zero vector
        // (no direction) is left un-normalized; the executor's exact f32 re-rank
        // fixes any resulting nav imprecision.
        let row_of = |id: usize, buf: &mut Vec<f32>| -> usize {
            let w = self.with_node_vector(id as u32, |v| {
                let w = dim.min(v.len());
                buf.clear();
                buf.extend_from_slice(&v[..w]);
                w
            });
            if normalize {
                super::distance::normalize_in_place(buf);
            }
            w
        };
        // Growth-only fast path: retain the existing affine params and append
        // codes for newly inserted nodes. This avoids rescanning/requantizing
        // the resident prefix. Recalibrate only when a new value drifts more
        // than 2% of the step range outside its dimension's representable
        // range.
        let old_arena = { self.quant.read().as_ref().cloned() };
        if let Some(old) = old_arena {
            if old.n < len && old.dim == dim && old.update_epoch == update_epoch {
                let half_range = 127.0 / old.inv_scale;
                let margin = (2.0 * half_range * 0.02).max(f32::EPSILON);
                let reach = half_range + margin;
                let mut buf = Vec::with_capacity(dim);
                let mut drifted = false;
                for id in old.n..len {
                    let w = row_of(id, &mut buf);
                    if buf[..w]
                        .iter()
                        .zip(&old.mids)
                        .any(|(x, mid)| (x - mid).abs() > reach)
                    {
                        drifted = true;
                        break;
                    }
                }
                if !drifted {
                    let mut codes = ArenaCodesBuilder::extend(&old);
                    for id in old.n..len {
                        let w = row_of(id, &mut buf);
                        codes.push(|row| old.quantize(&buf[..w], &mut row[..w]));
                    }
                    let arena = Arc::new(QuantArena {
                        _charge: arena_charge,
                        update_epoch,
                        chunks: codes.finish(),
                        n: len,
                        dim,
                        scale_sq: old.scale_sq,
                        dist_mul: old.dist_mul,
                        mids: old.mids.clone(),
                        inv_scale: old.inv_scale,
                    });
                    *self.quant.write() = Some(arena.clone());
                    return Some(arena);
                }
            }
        }
        // Per-dimension min/max across all (possibly normalized) arena rows.
        // Each dimension is centred on its own range; the shared step covers
        // the widest dimension.
        let mut vmin = vec![f32::INFINITY; dim];
        let mut vmax = vec![f32::NEG_INFINITY; dim];
        let mut buf: Vec<f32> = Vec::with_capacity(dim);
        for id in 0..len {
            let w = row_of(id, &mut buf);
            for ((lo, hi), &x) in vmin.iter_mut().zip(vmax.iter_mut()).zip(&buf[..w]) {
                *lo = lo.min(x);
                *hi = hi.max(x);
            }
        }
        let mut widest = 0.0f32;
        let mut mids = vec![0.0f32; dim];
        for ((mid, &lo), &hi) in mids.iter_mut().zip(&vmin).zip(&vmax) {
            if lo > hi {
                continue; // dimension never observed: centre at zero
            }
            if !lo.is_finite() || !hi.is_finite() {
                return None;
            }
            *mid = (lo as f64 * 0.5 + hi as f64 * 0.5) as f32;
            widest = widest.max(hi - lo);
        }
        if !(widest > 0.0) || !widest.is_finite() {
            return None; // all-equal / non-finite → quantization is degenerate
        }
        let inv_scale = 254.0 / widest;
        let scale = 1.0 / inv_scale;
        let mut codes = ArenaCodesBuilder::new(dim);
        for id in 0..len {
            let w = row_of(id, &mut buf);
            codes.push(|row| quantize_centered_into(&buf[..w], &mids, inv_scale, &mut row[..w]));
        }
        let scale_sq = scale * scale;
        if !scale_sq.is_normal() || !inv_scale.is_normal() || mids.iter().any(|m| !m.is_finite()) {
            return None;
        }
        let arena = Arc::new(QuantArena {
            _charge: arena_charge,
            update_epoch,
            chunks: codes.finish(),
            n: len,
            dim,
            scale_sq,
            // Cosine SSD ≈ 2(1−cos); the ×0.5 lands it on the 1−cos scale the
            // f32 fallback uses, so covered/uncovered nodes mix correctly.
            dist_mul: if normalize { scale_sq * 0.5 } else { scale_sq },
            mids,
            inv_scale,
        });
        *self.quant.write() = Some(arena.clone());
        Some(arena)
    }

    /// Copy node `id`'s neighbour ids at `level` into `out` (cleared first).
    /// Holds the per-node neighbour mutex only for the `memcpy`, so the hot
    /// loops below never allocate a fresh `Vec` per visited node.
    #[inline]
    fn neighbors_into(&self, id: u32, level: usize, out: &mut Vec<u32>) {
        out.clear();

        let nbrs = self.nodes.get(id).neighbors.lock();
        if let Some(layer) = nbrs.get(level) {
            out.extend_from_slice(layer);
        }
    }

    /// Greedy single-best descent at `level`, starting from `entry`. Every
    /// node access is a lock-free slab index; the scratch neighbour-id buffer
    /// is reused across iterations.
    fn greedy_descend(
        &self,
        probe: &Probe,
        entry: u32,
        level: usize,
        scratch: &mut SearchScratch,
    ) -> u32 {
        let mut cur = entry;
        let mut cur_d = self.probe_dist(probe, cur);
        loop {
            let mut changed = false;
            // Borrow the buffer out of `scratch` so the loop can still touch
            // `self` (the buffer and the slab are disjoint).
            let mut nbr_buf = std::mem::take(&mut scratch.nbr_buf);
            self.neighbors_into(cur, level, &mut nbr_buf);
            // Stream all neighbour-code prefetches up front (see search_layer).
            for &nb in nbr_buf.iter() {
                probe.prefetch(nb);
            }
            for k in 0..nbr_buf.len() {
                let nb = nbr_buf[k];
                let d = self.probe_dist(probe, nb);
                if d < cur_d {
                    cur_d = d;
                    cur = nb;
                    changed = true;
                }
            }
            scratch.nbr_buf = nbr_buf;
            if !changed {
                break;
            }
        }
        cur
    }

    /// Best-first search confined to a single `level`. Returns up to `ef`
    /// nearest nodes, ascending by distance. Each candidate expansion holds
    /// one slab read guard while scoring all of that candidate's neighbours,
    /// reuses a neighbour-id buffer, and tracks "visited" through the scratch's
    /// generation array — so a search visiting thousands of nodes takes one
    /// lock per expansion and zero allocations per neighbour.
    ///
    /// With `live_only`, tombstones remain navigation hops (they enter the
    /// frontier) but never occupy one of the `ef` result slots, so a cluster
    /// of deleted nodes around the query cannot crowd out reachable live
    /// nodes. Construction passes `false`: tombstones stay valid link targets
    /// until compaction.
    fn search_layer(
        &self,
        probe: &Probe,
        entries: &[u32],
        ef: usize,
        level: usize,
        live_only: bool,
        scratch: &mut SearchScratch,
    ) -> Vec<Scored> {
        self.search_layer_impl::<true>(probe, entries, ef, level, live_only, scratch)
    }

    fn search_layer_impl<const BATCH: bool>(
        &self,
        probe: &Probe,
        entries: &[u32],
        ef: usize,
        level: usize,
        live_only: bool,
        scratch: &mut SearchScratch,
    ) -> Vec<Scored> {
        // Without tombstones every node is live; skip the per-admission flag
        // load so the common path is unchanged.
        let live_only = live_only && self.stale_count() > 0;
        let admits = |id: u32| !live_only || !self.nodes.get(id).deleted.load(Ordering::Acquire);
        scratch.begin(self.nodes.len());
        for &e in entries {
            if scratch.visit(e) {
                let d = self.probe_dist(probe, e);
                scratch.frontier.push(Reverse(Scored { dist: d, id: e }));
                if admits(e) {
                    scratch.results.push(Scored { dist: d, id: e });
                }
            }
        }
        while scratch.results.len() > ef {
            scratch.results.pop();
        }
        let mut nbr_buf = std::mem::take(&mut scratch.nbr_buf);
        while let Some(Reverse(c)) = scratch.frontier.pop() {
            let furthest = scratch
                .results
                .peek()
                .map(|s| s.dist)
                .unwrap_or(f64::INFINITY);
            if c.dist > furthest && scratch.results.len() >= ef {
                break;
            }
            self.neighbors_into(c.id, level, &mut nbr_buf);
            // Compact in discovery order before scoring. Visiting a node
            // depends only on its id, not on whether it enters the heap.
            // This avoids fetching already-visited codes and lets independent
            // integer dot products overlap without changing admission order.
            for &nb in &nbr_buf {
                prefetch_read(scratch.visited.as_ptr().wrapping_add(nb as usize).cast());
            }
            let mut fresh = 0;
            for i in 0..nbr_buf.len() {
                let nb = nbr_buf[i];
                if scratch.visit(nb) {
                    nbr_buf[fresh] = nb;
                    fresh += 1;
                }
            }
            nbr_buf.truncate(fresh);
            for &nb in &nbr_buf {
                probe.prefetch(nb);
            }
            for group in nbr_buf.chunks(4) {
                let distances: [f64; 4] = if BATCH && group.len() == 4 {
                    if let Some((arena, qcodes, qnorm)) = probe.quant {
                        if group.iter().all(|&id| self.quant_covers(arena, id)) {
                            let dots = dot_i8_batch4_ptrs(
                                qcodes,
                                [
                                    arena.codes_for(group[0]),
                                    arena.codes_for(group[1]),
                                    arena.codes_for(group[2]),
                                    arena.codes_for(group[3]),
                                ],
                            );
                            std::array::from_fn(|i| {
                                (qnorm + arena.norm(group[i]) - 2 * dots[i]) as f64
                                    * (arena.dist_mul as f64)
                            })
                        } else {
                            std::array::from_fn(|i| self.probe_dist(probe, group[i]))
                        }
                    } else {
                        std::array::from_fn(|i| self.probe_dist(probe, group[i]))
                    }
                } else {
                    std::array::from_fn(|i| {
                        group.get(i).map_or(0.0, |&id| self.probe_dist(probe, id))
                    })
                };
                for (&nb, d) in group.iter().zip(distances) {
                    let s = Scored { dist: d, id: nb };
                    let full = scratch.results.len() >= ef;
                    if full {
                        // Results heap is full (= ef). Peek the furthest cheaply
                        // (no sift); only a strictly closer candidate is admitted.
                        // The reject path — the common case at large ef — pays
                        // just the peek. NB: `peek_mut` sifts down on drop, so we
                        // enter it *only* when we will replace, otherwise every
                        // rejected candidate would pay a wasted sift.
                        let furthest = scratch
                            .results
                            .peek()
                            .map(|x| x.dist)
                            .unwrap_or(f64::INFINITY);
                        if d >= furthest {
                            continue;
                        }
                    }
                    scratch.frontier.push(Reverse(s));
                    if !admits(nb) {
                        continue;
                    }
                    if full {
                        // Replace the furthest in a single sift-down instead
                        // of push-then-pop (sift-up + sift-down).
                        if let Some(mut top) = scratch.results.peek_mut() {
                            *top = s;
                        }
                    } else {
                        scratch.results.push(s);
                    }
                }
            }
        }
        scratch.nbr_buf = nbr_buf;
        // Drain keeps the BinaryHeap allocation attached to thread-local
        // scratch, avoiding a fresh candidate-buffer allocation per query.
        let mut out: Vec<Scored> = scratch.results.drain().collect();
        out.sort_unstable();
        out
    }

    /// Neighbour-selection heuristic (Algorithm 4): keep a candidate only if it
    /// is closer to the query than to any already-selected neighbour, which
    /// spreads connections out and improves graph navigability. Rejected
    /// redundant edges are not refilled merely to reach the degree bound.
    /// Node-to-node distance for the build heuristic. Uses the SQ8 arena when it
    /// covers both nodes (contiguous int8 codes, 4× less traffic) and falls back
    /// to exact f32 otherwise. The arena scales its integer SSD back to ~real
    /// squared-L2, so a quantized `node_dist` and the f32 query distances stored
    /// in `Scored.dist` are on the same scale and compare correctly.
    #[inline]
    fn build_node_dist(&self, arena: Option<&QuantArena>, a: u32, b: u32) -> f64 {
        if let Some(ar) = arena {
            if self.quant_covers(ar, a) && self.quant_covers(ar, b) {
                return l2_ssd_i8(ar.codes_for(a), ar.codes_for(b)) as f64 * (ar.scale_sq as f64);
            }
        }
        let va = self.nodes.get(a).vector.read();
        let vb = self.nodes.get(b).vector.read();
        ordering_distance(&va, &vb, self.metric)
    }

    /// Returns the selected candidates (id + distance-to-query), so callers that
    /// need the edge distance — the deferred build's drop-farthest, which stores
    /// it to avoid recomputation — get it without a second pass.
    fn select_neighbors(
        &self,
        candidates: &[Scored],
        m: usize,
        arena: Option<&QuantArena>,
    ) -> Vec<Scored> {
        super::hnsw_core::select_diverse(candidates, m, |a, b| {
            Ok::<_, std::convert::Infallible>(self.build_node_dist(arena, a, b))
        })
        .unwrap_or_else(|never| match never {})
    }

    /// Re-select `node`'s adjacency at `level` down to at most `m` via the
    /// same heuristic. Returns the new neighbour list — the caller writes
    /// it back under the node's neighbour lock.
    fn prune_select(
        &self,
        node: u32,
        existing: &[u32],
        m: usize,
        arena: Option<&QuantArena>,
    ) -> Vec<u32> {
        if existing.len() <= m {
            return existing.to_vec();
        }
        let mut scored: Vec<Scored> = existing
            .iter()
            .map(|&nb| Scored {
                dist: self.build_node_dist(arena, node, nb),
                id: nb,
            })
            .collect();
        scored.sort_unstable();
        self.select_neighbors(&scored, m, arena)
            .into_iter()
            .map(|s| s.id)
            .collect()
    }

    /// Wire a bidirectional edge `(id, nb)` at `level`. The target's neighbour
    /// list is pruned back to `m` only once it exceeds `soft_cap` (≥ `m`) — so a
    /// `soft_cap` of `c·m` amortises the O(m²) heuristic across ~`(c-1)·m`
    /// additions instead of running it on every reverse edge (the profiled build
    /// hotspot). The cold build links with `soft_cap = 2·m` and then runs one
    /// final [`prune_layers_to_m`] pass so every list ends at ≤ `m`; the
    /// incremental insert path passes `soft_cap = m`, pruning eagerly as before.
    /// Locks both nodes' neighbour mutexes in ascending id order — the
    /// deadlock-freedom guarantee for parallel inserts.
    fn wire_edge(
        &self,
        id: u32,
        nb: u32,
        level: usize,
        m: usize,
        soft_cap: usize,
        arena: Option<&QuantArena>,
    ) {
        self.wire_edge_with_snapshot_hook(id, nb, level, m, soft_cap, arena, || {});
    }

    fn wire_edge_with_snapshot_hook(
        &self,
        id: u32,
        nb: u32,
        level: usize,
        m: usize,
        soft_cap: usize,
        arena: Option<&QuantArena>,
        after_snapshot: impl FnOnce(),
    ) {
        // id == nb would self-lock the same mutex twice. Defensive guard;
        // the caller filters this out, but a future refactor shouldn't be
        // able to step on the rake.
        if id == nb {
            return;
        }
        let lo = id.min(nb);
        let hi = id.max(nb);
        let lo_node = self.node_at(lo);
        let hi_node = self.node_at(hi);
        let mut lo_nbrs = lo_node.neighbors.lock();
        let mut hi_nbrs = hi_node.neighbors.lock();

        // self (id) appends nb at this level.
        let self_nbrs = if id == lo {
            &mut *lo_nbrs
        } else {
            &mut *hi_nbrs
        };
        if let Some(layer) = self_nbrs.get_mut(level) {
            layer.push(nb);
        }

        // target (nb) appends id; capture whether the layer is over-cap.
        let over_cap = {
            let target_nbrs = if nb == lo {
                &mut *lo_nbrs
            } else {
                &mut *hi_nbrs
            };
            match target_nbrs.get_mut(level) {
                Some(layer) => {
                    layer.push(id);
                    layer.len() > soft_cap
                }
                None => false,
            }
        };
        if !over_cap {
            return;
        }

        // Snapshot the target's neighbours at this layer, drop both locks,
        // compute the pruned set, then reacquire just the target's lock to
        // store it. Keeping the prune-side distance work out of the dual-lock
        // critical section keeps wire_edge's footprint short for the next
        // pair of waiters.
        let candidate_list: Vec<u32> = {
            let target_nbrs = if nb == lo { &*lo_nbrs } else { &*hi_nbrs };
            target_nbrs.get(level).cloned().unwrap_or_default()
        };
        drop(lo_nbrs);
        drop(hi_nbrs);
        after_snapshot();

        let mut candidate_list = candidate_list;
        loop {
            let pruned = self.prune_select(nb, &candidate_list, m, arena);
            let mut nbrs = self.node_at(nb).neighbors.lock();
            let Some(layer) = nbrs.get_mut(level) else {
                return;
            };
            if *layer == candidate_list {
                *layer = pruned;
                break;
            }
            if layer.len() <= soft_cap {
                break;
            }
            candidate_list.clone_from(layer);
        }
    }

    /// Deferred-diversity edge wiring (cold build). Adds the forward edge
    /// `id → nb` and the reverse `nb → id` at `level`, each carrying the (already
    /// computed, symmetric) owner→neighbour distance `dist = d(id, nb)`. When the
    /// reverse append pushes `nb`'s navigable list past `m`, the **farthest**
    /// member is evicted — found by an O(m) scan of the mirrored distance vector
    /// `aux.ndist[nb]` (plain float compares, no arena fetch, since the dominant
    /// build cost is the random code-row fetch) — and parked in `aux.pending[nb]`
    /// for the Phase-D merge. Keeping the *closest* m navigable is what preserves
    /// recall (a cheaper oldest/arbitrary eviction collapses it); reconsidering
    /// the parked edges in Phase D is what keeps the final graph's diversity.
    ///
    /// Forward and reverse take each node's own lock separately (never both at
    /// once), so there is no dual-lock ordering and far less contention than
    /// [`wire_edge`]. `aux.ndist[X]` is touched only under `neighbors[X]`'s lock,
    /// so the two stay index-aligned and `ndist`'s own lock never contends.
    fn wire_edge_deferred(
        &self,
        id: u32,
        nb: u32,
        level: usize,
        m: usize,
        dist: f64,
        aux: &BuildAux,
    ) {
        if id == nb {
            return;
        }
        // Forward: id appends nb (id is being linked now — its list holds only
        // its ≤ m forward picks at this point, so this never overflows).
        {
            let id_node = self.node_at(id);
            let mut nbrs = id_node.neighbors.lock();
            let mut nd = aux.ndist[id as usize].lock();
            if let Some(layer) = nbrs.get_mut(level) {
                layer.push(nb);
                nd[level].push(dist);
            }
        }
        // Reverse: nb appends id; if the list now exceeds m, evict nb's farthest
        // neighbour (by the mirrored distances) into the pending park.
        let dropped = {
            let nb_node = self.node_at(nb);
            let mut nbrs = nb_node.neighbors.lock();
            let mut nd = aux.ndist[nb as usize].lock();
            match nbrs.get_mut(level) {
                Some(layer) => {
                    layer.push(id);
                    let dl = &mut nd[level];
                    dl.push(dist);
                    if layer.len() > m + aux.nav_slack {
                        let mut worst_i = 0usize;
                        let mut worst_d = f64::NEG_INFINITY;
                        for (i, &d) in dl.iter().enumerate() {
                            if d > worst_d {
                                worst_d = d;
                                worst_i = i;
                            }
                        }
                        dl.swap_remove(worst_i);
                        Some(layer.swap_remove(worst_i))
                    } else {
                        None
                    }
                }
                None => None,
            }
        };
        if let Some(d) = dropped {
            aux.pending[nb as usize].lock().push((level as u8, d));
        }
    }

    /// Insert a vector keyed by `row_id`. The vector dimension must match the
    /// index dimension (the first insert fixes it); callers normalise before
    /// calling. Duplicate `row_id`s are permitted (stale tolerated).
    ///
    /// Concurrency: the node append is a lock-free [`NodeSlab::push_with`]; the
    /// descent and wiring run holding only `structural` **shared**, so many
    /// concurrent inserts (a parallel build) overlap fully instead of
    /// serialising on a slab write lock.
    pub(crate) fn insert(&self, row_id: RowId, vector: Vec<f32>) -> Result<(), QueryError> {
        let _mutation = self.mutation_gate.read();
        let id = self.append_node(row_id, vector)?;
        self.link_node(id);
        self.mutation_epoch.fetch_add(1, Ordering::AcqRel);
        self.note_compaction_touch(id);
        Ok(())
    }

    /// Record a node mutated while a compaction rebuild runs. Callers hold
    /// `mutation_gate` shared, so they either finish before the rebuild opens
    /// its log or observe `compacting` and append here.
    #[inline]
    fn note_compaction_touch(&self, id: u32) {
        if self.compacting.load(Ordering::Acquire) {
            if let Some(log) = self.compaction_log.lock().as_mut() {
                if log.contains(&id) || log.len() < self.compaction_log_cap.load(Ordering::Acquire)
                {
                    log.insert(id);
                } else {
                    self.compaction_cancelled.store(true, Ordering::Release);
                }
            }
        }
    }

    /// Reserve/check the graph's dimension before a heap mutation. CAS also
    /// makes concurrent first writes agree on one dimension in an empty graph.
    pub(crate) fn validate_insert_dimension(&self, expected_dim: usize) -> Result<(), QueryError> {
        if let Err(actual_dim) =
            self.dim
                .compare_exchange(0, expected_dim, Ordering::AcqRel, Ordering::Relaxed)
        {
            if actual_dim != expected_dim {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
                    format!(
                        "HNSW vector dimension mismatch: expected {actual_dim}, got {expected_dim}"
                    ),
                ));
            }
        }

        Ok(())
    }

    /// Phase 1 of an insert, factored out so the cold-build path can append
    /// every node first (cheap, lock-free) and only then build the SQ8 arena
    /// over the complete set before linking. Fixes `dim` on the first append,
    /// assigns the id and the HNSW level (seeded by that id), and stores the
    /// node with empty adjacency lists. Does **not** touch the entry point /
    /// topology or the live/dirty counters — that is [`link_node`]'s job.
    /// Returns the new node id.
    pub(crate) fn append_node(&self, row_id: RowId, vector: Vec<f32>) -> Result<u32, QueryError> {
        let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
        self.append_node_with_seq(row_id, vector, seq)
    }

    /// [`Self::append_node`] with a caller-chosen version handle (compaction
    /// carries each survivor's handle into the replacement graph).
    fn append_node_with_seq(
        &self,
        row_id: RowId,
        vector: Vec<f32>,
        seq: u64,
    ) -> Result<u32, QueryError> {
        let expected_dim = vector.len();
        self.validate_insert_dimension(expected_dim)?;

        self.reserve_resident_node(expected_dim)?;

        // Lock-free append. `push_with` assigns the id; we build the node — and
        // its HNSW level, seeded by that id — inside the closure. The level is
        // recoverable later as `neighbors.len() - 1`, so `link_node` needs no
        // extra state threaded through.
        let id = self.nodes.push_with(|id| {
            let level = self.random_level(id);
            Node {
                update_epoch: AtomicU64::new(0),
                seq,
                row_id,
                vector: RwLock::new(vector),
                neighbors: Mutex::new(vec![Vec::new(); level + 1]),
                deleted: AtomicBool::new(false),
            }
        })?;
        // Record the row_id → node id mapping so `mark_deleted_by_row_id` can
        // tombstone without scanning the whole slab. Fast hash insert under a
        // dedicated lock; cold-build's parallel appenders only briefly contend.
        let key = pack_row(row_id);
        self.row_index[row_index_shard(key)]
            .lock()
            .entry(key)
            .or_default()
            .push(id);
        Ok(id)
    }

    /// Append one topology-only node for [`CompactHnswBuilder`].
    fn append_compact_node(&self, row_id: RowId) -> Result<u32, QueryError> {
        self.reserve_resident_node(0)?;
        self.nodes.push_with(|id| {
            let level = self.random_level(id);
            let mut neighbors = Vec::with_capacity(level + 1);
            for layer in 0..=level {
                neighbors.push(Vec::with_capacity(if layer == 0 {
                    self.m0
                } else {
                    self.m
                }));
            }
            Node {
                update_epoch: AtomicU64::new(0),
                seq: id as u64,
                row_id,
                vector: RwLock::new(Vec::new()),
                neighbors: Mutex::new(neighbors),
                deleted: AtomicBool::new(false),
            }
        })
    }

    /// Phase 2–4 of an insert: wire an already-[`append_node`]ed node `id` into
    /// the graph. The first node linked into an empty graph becomes the entry
    /// point and returns without navigating.
    ///
    /// **SQ8 build navigation**: when the metric is L2 and the published quant
    /// arena already covers `id` (the cold-build path builds it up front over
    /// the full node set), the greedy descent, per-layer best-first search, and
    /// edge selection ([`select_neighbors`] / [`prune_select`]) all run on the
    /// contiguous int8 codes — 4× less memory traffic than the scattered 3 KiB
    /// f32 node vectors, which is the dominant cost at high dimension. The SQ8
    /// SSD is scaled back to ~squared-L2 so navigation distances and selection
    /// distances share one scale; recall tracks the exact-f32 build (verified
    /// neutral on the 8/128/768-d benchmarks). The incremental insert path
    /// (arena absent or not yet covering this fresh id) falls back to exact-f32
    /// navigation, exactly as before.
    pub(crate) fn link_node(&self, id: u32) {
        self.link_node_capped(id, 1, None);
    }

    /// [`link_node`] with an explicit neighbour-list soft-cap multiplier:
    /// reverse edges prune to `m` only past `cap_mult · m`. `cap_mult = 1` is
    /// the eager incremental-insert behaviour; the cold build passes a larger
    /// value to amortise pruning and then runs a final prune pass.
    ///
    /// **`pending`** (cold-build only, deferred-diversity mode): when `Some`,
    /// each reverse edge keeps its target's navigable list at ≤ m by a cheap
    /// **drop-farthest** (one `swap_remove`, the evicted id parked in
    /// `pending[target]`) instead of letting it grow to `cap_mult·m` and running
    /// the O(m²) diversity heuristic. Navigation therefore traverses ≤ m-degree
    /// lists for the whole link phase — about half the per-neighbour random arena
    /// fetches that dominate the build — while the union of navigable + parked
    /// edges (a superset of what the soft-cap path retains) is diversity-pruned
    /// once in the final Phase-D pass, so the shipped graph's quality is
    /// unchanged. `cap_mult` is ignored when `pending` is `Some`.
    pub(crate) fn link_node_capped(&self, id: u32, cap_mult: usize, aux: Option<&BuildAux>) {
        // Held shared for the whole link: parallel builders don't block each
        // other, but a concurrent compaction's clear (exclusive) can't free a
        // node out from under our navigation.
        let _structural = self.structural.read();

        let level = self.nodes.get(id).neighbors.lock().len().saturating_sub(1);

        // The first node becomes the entry point (no navigation). The topology
        // lock makes the "is this the first node?" check atomic across inserts.
        let (ep, max_level) = {
            let mut topo = self.topology.lock();
            match topo.entry_point {
                None => {
                    topo.entry_point = Some(id);
                    topo.max_level = level;
                    drop(topo);
                    self.live.fetch_add(1, Ordering::AcqRel);
                    self.dirty_count.fetch_add(1, Ordering::AcqRel);
                    return;
                }
                Some(ep) => (ep, topo.max_level),
            }
        };

        // Borrow our own vector for the duration of the link — the `_structural`
        // read guard keeps the slab (and thus this `&Node`) alive, and segments
        // never move, so a borrow is sound and saves a per-insert vector clone
        // (3 KiB at 768d). Navigation uses the SQ8 arena when it covers `id`
        // (cold build packs it up front); edges are still chosen by the
        // heuristic on the same ~squared-L2 scale, so recall tracks exact f32.
        let node = self.nodes.get(id);
        let node_vec = node.vector.read();
        let arena = if self.metric != Metric::Dot {
            self.quant.read().clone()
        } else {
            None
        }
        .filter(|a| a.dim == node_vec.len() || (node_vec.is_empty() && (id as usize) < a.n));
        // Quantize *this* node's vector as a query when the arena doesn't already
        // cover `id`. Cold build packs the arena over the full node set up front
        // (`id < a.n`), so it uses the stored codes with no re-quantize — the
        // original behaviour, byte-identical. The **incremental insert / UPDATE
        // re-link** path appends a fresh node past the arena prefix (`id >= a.n`),
        // where the old code fell back to scattered 3 KiB f32 fetches per probe
        // (the dominant UPDATE cost at high dim). Quantizing the new vector once —
        // exactly as `search` does for its query — lets navigation score the
        // arena-covered candidates with the cheap int8 `sdot`; the rare tail
        // candidate (another just-appended node) still falls back to f32 inside
        // `probe_dist` / `build_node_dist`, so distances stay correct.
        let stored_query = arena.as_ref().is_some_and(|a| self.quant_covers(a, id));
        let owned_q = arena
            .as_ref()
            .filter(|_| !stored_query)
            .and_then(|a| a.prepare_query(self.metric, &node_vec));
        let probe = Probe {
            query: &node_vec,
            quant: arena.as_ref().and_then(|a| {
                if stored_query {
                    Some((a.as_ref(), a.codes_for(id), a.norm(id)))
                } else {
                    owned_q
                        .as_ref()
                        .map(|(q, n)| (a.as_ref(), q.as_slice(), *n))
                }
            }),
        };

        SCRATCH.with(|cell| {
            let mut scratch = cell.borrow_mut();

            // Phase 2: greedy descent through the layers above `level`.
            let mut cur = ep;
            let mut l = max_level;
            while l > level {
                cur = self.greedy_descend(&probe, cur, l, &mut scratch);
                if l == 0 {
                    break;
                }
                l -= 1;
            }

            // Phase 3: from min(level, max_level) down to 0, find and wire
            // neighbours at each layer.
            let mut entries = vec![cur];
            let start = level.min(max_level);
            for lc in (0..=start).rev() {
                let found = self.search_layer(
                    &probe,
                    &entries,
                    self.ef_construction,
                    lc,
                    false,
                    &mut scratch,
                );
                // Edges are chosen on exact distances (see `selection_arena`);
                // re-score the SQ8-navigated pool once per layer.
                let ranked: std::borrow::Cow<'_, [Scored]> =
                    if probe.quant.is_some() && !self.codes_only {
                        let mut exact: Vec<Scored> = found
                            .iter()
                            .map(|s| Scored {
                                dist: self.with_node_vector(s.id, |v| {
                                    ordering_distance(&node_vec, v, self.metric)
                                }),
                                id: s.id,
                            })
                            .collect();
                        exact.sort_unstable();
                        std::borrow::Cow::Owned(exact)
                    } else {
                        std::borrow::Cow::Borrowed(&found)
                    };
                let m_layer = if lc == 0 { self.m0 } else { self.m };
                // Cold build keeps its `cap_mult·m` soft-cap (+ a final global
                // prune); the incremental path (`cap_mult == 1`) instead grows to
                // `m + slack` so the O(m²) diversity prune amortises across
                // several reverse edges. `max` leaves the cold-build value
                // unchanged (`2·m ≥ m + slack`).
                let soft_cap = (m_layer * cap_mult).max(m_layer + incremental_soft_cap_slack());
                // Insert M links; layer 0 reserves 2*M for later reverse links.
                let select_arena = self.selection_arena(arena.as_deref());
                let selected = self.select_neighbors(&ranked, self.m, select_arena);
                for s in &selected {
                    let nb = s.id;
                    if nb == id {
                        continue; // defensive — search_layer shouldn't surface our own id
                    }
                    match aux {
                        // Deferred mode: keep the reverse-edge target's navigable
                        // list at ≤ m by a drop-farthest (using the already-known
                        // symmetric edge distance), parking the dropped edge for
                        // the final diversity prune. Navigation then sees ≤ m-degree
                        // lists (about half the per-neighbour fetches).
                        Some(aux) => {
                            self.wire_edge_deferred(id, nb, lc, m_layer, s.dist, aux);
                        }
                        // Eager / soft-cap mode: reverse edge prunes by the
                        // diversity heuristic once the list exceeds `soft_cap`.
                        None => {
                            self.wire_edge(id, nb, lc, m_layer, soft_cap, select_arena);
                        }
                    }
                }
                entries = found.iter().map(|s| s.id).collect();
                if entries.is_empty() {
                    entries = vec![cur];
                }
            }
        });

        // Phase 4: if our level beat the captured max_level, bump topology.
        // Recheck under the lock — another concurrent insert may have already
        // raised it past ours.
        if level > max_level {
            let mut topo = self.topology.lock();
            if level > topo.max_level {
                topo.max_level = level;
                topo.entry_point = Some(id);
            }
        }

        self.live.fetch_add(1, Ordering::AcqRel);
        self.dirty_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Link every appended-but-unlinked node into the graph — the second half
    /// of a cold build. The caller has already [`append_node`]ed the complete
    /// node set (e.g. a parallel heap scan); this builds and publishes the SQ8
    /// navigation arena over that full set (Phase B) and then links node ids
    /// `0..len` (Phase C), navigating on the contiguous int8 codes.
    ///
    /// **Only valid on a graph whose nodes are all appended-but-unlinked**
    /// (a fresh `HnswIndex::new` + append pass). It links *every* id, so calling
    /// it on a graph with already-wired nodes would double-wire them — the
    /// caller must use incremental [`insert`] for the catch-up case instead.
    ///
    /// Phase C seeds the entry point with node 0 serially so the fan-out always
    /// navigates a non-empty graph, then links the rest across `threads`
    /// workers (`link_node` is `&self` under the M3 per-node latches, so
    /// concurrent linking over disjoint id ranges is safe). Small graphs link
    /// serially — `thread::scope` spawn/join would dominate.
    pub(crate) fn link_all(&self, threads: usize) {
        let len = self.nodes.len();
        if len == 0 {
            return;
        }
        let timing = std::env::var("MYDB_HNSW_BUILD_TIMING").is_ok();
        let t_b = std::time::Instant::now();
        // Phase B: build + publish the contiguous int8 arena over every node so
        // Phase C's descent runs on it. `ensure_quant` scans the slab; hold
        // `structural` shared per its contract.
        {
            let _structural = self.structural.read();
            self.ensure_quant();
        }
        if timing {
            eprintln!("[hnsw] PhaseB arena build: {:?} (n={len})", t_b.elapsed());
        }
        let t_c = std::time::Instant::now();

        // Phase C: seed the entry point with node 0, then link the rest. Reverse
        // edges grow to `HNSW_BUILD_SOFT_CAP_MULT · m` before pruning, so the
        // O(m²) heuristic runs ~once per that many additions instead of on every
        // edge (the profiled hotspot). The final Phase D pass restores ≤ m.
        let cap = std::env::var("MYDB_HNSW_BUILD_SOFT_CAP_MULT")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|v| *v >= 1)
            .unwrap_or(HNSW_BUILD_SOFT_CAP_MULT);
        // Deferred-diversity link: keep navigable lists at ≤ m via drop-farthest
        // (half the navigation fetches) and merge the parked edges back in the
        // final prune. Default on for cold build.
        let deferred = std::env::var("MYDB_HNSW_BUILD_DEFERRED")
            .ok()
            .map(|v| v != "0")
            .unwrap_or(HNSW_BUILD_DEFERRED);
        // Build-local scaffolding (deferred mode only): one park slot per node,
        // and a distance mirror sized to each node's existing (empty) layers so
        // the drop-farthest eviction is a float scan, not an arena recomputation.
        let aux: Option<BuildAux> = if deferred {
            let nav_slack = std::env::var("MYDB_HNSW_BUILD_NAV_SLACK")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(HNSW_BUILD_NAV_SLACK);
            let pending = (0..len).map(|_| Mutex::new(Vec::new())).collect();
            let ndist = (0..len as u32)
                .map(|id| {
                    let layers = self.nodes.get(id).neighbors.lock().len();
                    Mutex::new(vec![Vec::new(); layers])
                })
                .collect();
            Some(BuildAux {
                pending,
                ndist,
                nav_slack,
            })
        } else {
            None
        };
        let aux_ref = aux.as_ref();
        let link_cap = if deferred { 1 } else { cap };
        self.link_node_capped(0, link_cap, aux_ref);
        if len > 1 {
            let rest = (len - 1) as u32; // ids 1..len
            let threads = threads.max(1).min(rest as usize);
            if threads <= 1 || (rest as usize) < HNSW_BULK_LINK_MIN_PER_THREAD {
                for id in 1..len as u32 {
                    self.link_node_capped(id, link_cap, aux_ref);
                }
            } else {
                parallel_claim_ids(1, len as u32, threads, |id| {
                    self.link_node_capped(id, link_cap, aux_ref);
                });
            }
        }
        if timing {
            eprintln!("[hnsw] PhaseC link: {:?} (n={len})", t_c.elapsed());
        }

        // Phase D: restore every over-m layer to ≤ m with the diversity
        // heuristic. In deferred mode each node's parked edges are merged back
        // into its candidate set first (a superset of the soft-cap path's), so
        // the final graph quality is unchanged; with the eager soft-cap path
        // (cap = 1) lists are already ≤ m and this is skipped.
        if let Some(aux) = aux_ref {
            let t_d = std::time::Instant::now();
            self.merge_pending_and_prune(&aux.pending, threads);
            if timing {
                eprintln!("[hnsw] PhaseD merge+prune: {:?} (n={len})", t_d.elapsed());
            }
        } else if cap > 1 {
            let t_d = std::time::Instant::now();
            self.prune_layers_to_m(threads);
            if timing {
                eprintln!("[hnsw] PhaseD final prune: {:?} (n={len})", t_d.elapsed());
            }
        }
        self.repair_build_connectivity();
    }

    /// Repair only disconnected cold graphs. Ordinary connected graphs keep
    /// every selected edge. One spare slot per node bounds the repair degree.
    fn repair_build_connectivity(&self) {
        let count = self.nodes.len();
        let Some(entry) = self.topology.lock().entry_point else {
            return;
        };
        let Some(_charge) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                count.saturating_mul(5),
            )
        else {
            return;
        };
        let mut seen = vec![false; count];
        let mut stack = Vec::new();
        let visit = |root: u32, seen: &mut [bool], stack: &mut Vec<u32>| {
            seen[root as usize] = true;
            stack.push(root);
            while let Some(id) = stack.pop() {
                let neighbors = self.nodes.get(id).neighbors.lock();
                for &next in &neighbors[0] {
                    if !seen[next as usize] {
                        seen[next as usize] = true;
                        stack.push(next);
                    }
                }
            }
        };
        visit(entry, &mut seen, &mut stack);
        if seen.iter().all(|&v| v) {
            return;
        }
        let arena = self.quant.read().clone();
        for id in 0..count as u32 {
            let mut neighbors = self.nodes.get(id).neighbors.lock();
            neighbors[0] = self.prune_select(
                id,
                &neighbors[0],
                self.m0 - 1,
                self.selection_arena(arena.as_deref()),
            );
        }
        seen.fill(false);
        visit(entry, &mut seen, &mut stack);
        let mut anchor = entry;
        for id in 0..count as u32 {
            if !seen[id as usize] {
                self.nodes.get(anchor).neighbors.lock()[0].push(id);
                visit(id, &mut seen, &mut stack);
                anchor = id;
            }
        }
    }

    /// Phase D for the deferred-diversity build: for every node, merge its
    /// navigable list (≤ m closest, kept during the link) with its parked edges
    /// (reverse edges evicted by the drop-farthest cap) into one candidate set
    /// per layer, then run the diversity heuristic down to ≤ m. The merged set is
    /// a superset of what the soft-cap path would have retained, so the resulting
    /// graph is at least as good. Parallel over disjoint id ranges; each id owns
    /// its own list and park, so no cross-node locking.
    fn merge_pending_and_prune(&self, pending: &[Mutex<Vec<(u8, u32)>>], threads: usize) {
        let len = self.nodes.len();
        if len == 0 {
            return;
        }
        let arena = if self.metric != Metric::Dot {
            self.quant.read().clone()
        } else {
            None
        };
        let prune_range = |lo: u32, hi: u32| {
            for id in lo..hi {
                let ar = self.selection_arena(arena.as_deref().filter(|a| (id as usize) < a.n));
                let park = std::mem::take(&mut *pending[id as usize].lock());
                let node = self.nodes.get(id);
                let mut nbrs = node.neighbors.lock();
                for lc in 0..nbrs.len() {
                    let m_layer = if lc == 0 { self.m0 } else { self.m };
                    // Merge navigable + parked edges for this layer, deduping.
                    let mut cand = nbrs[lc].clone();
                    if !park.is_empty() {
                        for &(lvl, x) in park.iter() {
                            if lvl as usize == lc && x != id && !cand.contains(&x) {
                                cand.push(x);
                            }
                        }
                    }
                    if cand.len() > m_layer {
                        nbrs[lc] = self.prune_select(id, &cand, m_layer, ar);
                    } else {
                        nbrs[lc] = cand;
                    }
                }
            }
        };
        let threads = threads.max(1).min(len);
        if threads <= 1 || len < HNSW_BULK_LINK_MIN_PER_THREAD {
            prune_range(0, len as u32);
            return;
        }
        parallel_claim_ids(0, len as u32, threads, |id| prune_range(id, id + 1));
    }

    /// Final cold-build pass: prune every node's over-cap adjacency lists back to
    /// `m` (layer 0 to `m0`) with the same heuristic, in parallel over disjoint
    /// id ranges. Runs after Phase C has joined, so no concurrent wiring touches
    /// these lists. Navigation distances use the SQ8 arena when present.
    fn prune_layers_to_m(&self, threads: usize) {
        let len = self.nodes.len();
        if len == 0 {
            return;
        }
        let arena = if self.metric != Metric::Dot {
            self.quant.read().clone()
        } else {
            None
        };
        let prune_range = |lo: u32, hi: u32| {
            for id in lo..hi {
                let ar = self.selection_arena(arena.as_deref().filter(|a| (id as usize) < a.n));
                let node = self.nodes.get(id);
                let mut nbrs = node.neighbors.lock();
                for lc in 0..nbrs.len() {
                    let m_layer = if lc == 0 { self.m0 } else { self.m };
                    if nbrs[lc].len() > m_layer {
                        let pruned = self.prune_select(id, &nbrs[lc], m_layer, ar);
                        nbrs[lc] = pruned;
                    }
                }
            }
        };
        let threads = threads.max(1).min(len);
        if threads <= 1 || len < HNSW_BULK_LINK_MIN_PER_THREAD {
            prune_range(0, len as u32);
            return;
        }
        parallel_claim_ids(0, len as u32, threads, |id| prune_range(id, id + 1));
    }

    /// k-nearest-neighbour search. Returns up to `k` `(row_id, ordering_dist)`
    /// pairs ascending by distance, skipping tombstoned nodes. `ef_search` is
    /// the dynamic candidate-list size at layer 0 (clamped to ≥ k).
    pub(crate) fn search(&self, query: &[f32], k: usize, ef_search: usize) -> Vec<(RowId, f64)> {
        if k == 0 {
            return Vec::new();
        }
        // Shared for the whole search so a concurrent compaction can't free a
        // node we are about to index. Lock-free `get`s run inside it.
        let _structural = self.structural.read();
        // Read entry_point + max_level under the topology lock so we never
        // navigate from a max_level that doesn't match the captured entry.
        let (ep, max_level) = {
            let topo = self.topology.lock();
            match topo.entry_point {
                Some(e) => (e, topo.max_level),
                None => return Vec::new(),
            }
        };
        // Defensive: if the slab is empty, return immediately. Should not
        // happen if `entry_point` is Some, but guards against any topology /
        // slab desynchronisation under future refactors.
        if self.nodes.len() == 0 {
            return Vec::new();
        }
        let ef = ef_search.max(k).min(self.nodes.len()).max(1);
        // Build/refresh the SQ8 navigation arena (L2 only; no-op otherwise) and
        // quantize the query once. Both outlive the search closure below. The
        // `_structural` read guard above keeps the slab alive across the build.
        let arena = self.ensure_quant();
        // Quantize the query once and precompute ‖q‖² so each probe is a single
        // `sdot`. `qcodes` is full-width (`a.dim`), zero-padded past the query
        // length, matching how node codes are laid out.
        let qcodes = arena
            .as_ref()
            .and_then(|a| a.prepare_query(self.metric, query));
        let probe = Probe {
            query,
            quant: match (arena.as_ref(), qcodes.as_ref()) {
                (Some(a), Some((qc, qn))) => Some((a.as_ref(), qc.as_slice(), *qn)),
                _ => None,
            },
        };
        let score_epoch = self.update_epoch.load(Ordering::Acquire);
        let used_quant = probe.quant.is_some();
        let found = SCRATCH.with(|cell| {
            let mut scratch = cell.borrow_mut();
            // Descend the upper layers greedily (ef = 1).
            let mut cur = ep;
            let mut l = max_level;
            while l > 0 {
                cur = self.greedy_descend(&probe, cur, l, &mut scratch);
                l -= 1;
            }
            // Full search at layer 0.
            self.search_layer(&probe, &[cur], ef, 0, true, &mut scratch)
        });
        let exact_unchanged =
            !used_quant && self.update_epoch.load(Ordering::Acquire) == score_epoch;
        let mut out = Vec::with_capacity(k.min(found.len()));
        for s in found {
            let node = self.node_at(s.id);
            if node.deleted.load(Ordering::Acquire) {
                continue;
            }
            // Re-rank the returned candidates by the **exact** distance to the
            // in-memory f32 node vector, not the approximate SQ8 nav distance
            // (`s.dist`). The candidate *pool* is unchanged (the SQ8-nearest
            // `k`), so recall is identical, but the returned ordering is now
            // exact — which lets the executor read the heap row for only the
            // top-`k_final` survivors instead of re-reading and re-ranking every
            // returned candidate (`read_row` was ~19% of a low-ef query). Costs
            // `ef` in-memory f32 distances. Cheap no-op when SQ8 is
            // inactive (`s.dist` is already this exact value).
            let d = if exact_unchanged {
                s.dist
            } else {
                self.with_node_vector(s.id, |v| ordering_distance(query, v, self.metric))
            };
            out.push((node.row_id, d));
            if exact_unchanged && out.len() >= k {
                break;
            }
        }
        let cmp =
            |a: &(RowId, f64), b: &(RowId, f64)| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0));
        if out.len() > k {
            out.select_nth_unstable_by(k, cmp);
            out.truncate(k);
        }
        out.sort_unstable_by(cmp);
        out
    }

    // ---- serialization ----
    //
    // Self-describing, CRC32-checked `.hnsw` format (M1, 2026-06):
    //   [0..4]   HNSW_MAGIC u32         "HNW1"
    //   [4..6]   HNSW_VERSION u16       2 — v1 is rejected on load
    //   [6]      metric u8              0=L2, 1=Cosine, 2=Dot
    //   [7..11]  dim u32
    //   [11..15] m u32
    //   [15..19] ef_construction u32
    //   [19..27] entry_point i64        -1 = None
    //   [27..31] max_level u32
    //   [31..35] node_count u32
    //   [35..43] generation u64         monotonic per `mark_persisted`
    //   [43..51] last_persisted_lsn u64 WAL LSN this snapshot reflects
    //   [51..]   per-node records       (row_id, deleted, vector, neighbours)
    //   [..-4]   CRC32                  IEEE polynomial over everything above
    //
    // The graph is derived data: a missing / stale / corrupt file falls
    // back to a full heap-scan rebuild. The header epilogue (generation +
    // last_persisted_lsn) lets the loader run an incremental catch-up
    // instead — see `Database::vector_graph_load_or_build`.

    #[cfg(test)]
    pub(crate) fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.serialize_into(&mut out)
            .expect("writing into a Vec cannot fail");
        out
    }

    /// Stream the snapshot node by node. Only `structural` is held (shared),
    /// so searches and DML proceed throughout and no second graph-sized copy
    /// is materialised. The node set is the initialized prefix observed after
    /// the entry point; edges to nodes appended later are omitted, which keeps
    /// the file self-consistent. Per-node state is read under that node's
    /// latches; cross-node drift is harmless because a loaded snapshot is
    /// reconciled against the heap.
    pub(crate) fn serialize_into(&self, out: impl io::Write) -> io::Result<()> {
        struct CrcWriter<W> {
            inner: W,
            crc: crc32fast::Hasher,
        }
        impl<W: io::Write> CrcWriter<W> {
            fn put(&mut self, bytes: &[u8]) -> io::Result<()> {
                self.crc.update(bytes);
                self.inner.write_all(bytes)
            }
        }
        let mut w = CrcWriter {
            inner: out,
            crc: crc32fast::Hasher::new(),
        };
        let _structural = self.structural.read();
        let (entry_point, max_level) = {
            let topo = self.topology.lock();
            (topo.entry_point, topo.max_level)
        };
        let node_count = self.nodes.published_len();
        let (entry_point, max_level) = match entry_point {
            Some(entry) if (entry as usize) < node_count => (entry as i64, max_level),
            _ => (-1, 0),
        };
        let mut header = Vec::with_capacity(64);
        header.extend_from_slice(&HNSW_MAGIC.to_le_bytes());
        header.extend_from_slice(&HNSW_VERSION.to_le_bytes());
        header.push(self.metric as u8);
        header.extend_from_slice(&(self.dim.load(Ordering::Acquire) as u32).to_le_bytes());
        header.extend_from_slice(&(self.m as u32).to_le_bytes());
        header.extend_from_slice(&(self.ef_construction as u32).to_le_bytes());
        header.extend_from_slice(&entry_point.to_le_bytes());
        header.extend_from_slice(&(max_level as u32).to_le_bytes());
        header.extend_from_slice(&(node_count as u32).to_le_bytes());
        header.extend_from_slice(&self.generation.load(Ordering::Acquire).to_le_bytes());
        header.extend_from_slice(
            &self
                .last_persisted_lsn
                .load(Ordering::Acquire)
                .to_le_bytes(),
        );
        w.put(&header)?;
        let mut record = Vec::new();
        for id in 0..node_count as u32 {
            let node = self.nodes.get(id);
            record.clear();
            record.extend_from_slice(&pack_row(node.row_id).to_le_bytes());
            record.push(node.deleted.load(Ordering::Acquire) as u8);
            for v in node.vector.read().iter() {
                record.extend_from_slice(&v.to_le_bytes());
            }
            let layers = node.neighbors.lock();
            record.push(layers.len() as u8);
            for layer in layers.iter() {
                let count_at = record.len();
                record.extend_from_slice(&0u32.to_le_bytes());
                let mut kept = 0u32;
                for &nb in layer {
                    if (nb as usize) < node_count {
                        record.extend_from_slice(&nb.to_le_bytes());
                        kept += 1;
                    }
                }
                record[count_at..count_at + 4].copy_from_slice(&kept.to_le_bytes());
            }
            drop(layers);
            w.put(&record)?;
        }
        let crc = w.crc.clone().finalize();
        w.inner.write_all(&crc.to_le_bytes())
    }

    pub(crate) fn deserialize(bytes: &[u8]) -> io::Result<Self> {
        let err = || io::Error::new(io::ErrorKind::InvalidData, "corrupt hnsw file");
        if bytes.len() < 8 {
            return Err(err());
        }
        let (data, crc_bytes) = bytes.split_at(bytes.len() - 4);
        let stored_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        if crc32(data) != stored_crc {
            return Err(err());
        }
        let mut r = Cursor { buf: data, pos: 0 };
        if r.u32()? != HNSW_MAGIC {
            return Err(err());
        }
        let version = r.u16()?;
        if version != HNSW_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("hnsw file version {version} not supported (expected {HNSW_VERSION})"),
            ));
        }
        let metric = match r.u8()? {
            0 => Metric::L2,
            1 => Metric::Cosine,
            2 => Metric::Dot,
            _ => return Err(err()),
        };
        let dim = r.u32()? as usize;
        let m = r.u32()? as usize;
        let ef_construction = r.u32()? as usize;
        let entry_raw = r.i64()?;
        let max_level = r.u32()? as usize;
        if max_level > super::hnsw_paged::VEC_MAX_LAYER {
            return Err(err());
        }
        let node_count = r.u32()? as usize;
        let generation = r.u64()?;
        let last_persisted_lsn = r.u64()?;
        let minimum_record = dim.saturating_mul(4).saturating_add(14);
        if dim > super::codec::MAX_DIM
            || (node_count > 0 && dim == 0)
            || m < 2
            || ef_construction < m
            || node_count > data.len().saturating_sub(r.pos) / minimum_record
        {
            return Err(err());
        }
        let index = HnswIndex::new(metric, m, ef_construction);
        index.dim.store(dim, Ordering::Release);
        index.generation.store(generation, Ordering::Release);
        index
            .last_persisted_lsn
            .store(last_persisted_lsn, Ordering::Release);
        let mut live = 0usize;
        let mut stale = 0u32;
        for _ in 0..node_count {
            index
                .reserve_resident_node(dim)
                .map_err(|error| io::Error::other(error.to_string()))?;
            let row_id = unpack_row(r.u64()?);
            let deleted = r.u8()? != 0;
            let mut vector = Vec::with_capacity(dim);
            for _ in 0..dim {
                let value = r.f32()?;
                if !value.is_finite() {
                    return Err(err());
                }
                vector.push(value);
            }
            let level_count = r.u8()? as usize;
            if level_count == 0 || level_count > super::hnsw_paged::VEC_MAX_LAYER + 1 {
                return Err(err());
            }
            let mut neighbors = Vec::with_capacity(level_count);
            for _ in 0..level_count {
                let n = r.u32()? as usize;
                if n > node_count || n > r.buf.len().saturating_sub(r.pos) / 4 {
                    return Err(err());
                }
                let mut adj = Vec::with_capacity(n);
                for _ in 0..n {
                    let nb = r.u32()?;
                    if nb as usize >= node_count {
                        return Err(err());
                    }
                    adj.push(nb);
                }
                neighbors.push(adj);
            }
            if deleted {
                stale = stale.saturating_add(1);
            } else {
                live += 1;
            }
            let id = index
                .nodes
                .push_with(|id| Node {
                    update_epoch: AtomicU64::new(0),
                    seq: id as u64,
                    row_id,
                    vector: RwLock::new(vector),
                    neighbors: Mutex::new(neighbors),
                    deleted: AtomicBool::new(deleted),
                })
                .map_err(|e| io::Error::other(e.to_string()))?;
            let key = pack_row(row_id);
            index.row_index[row_index_shard(key)]
                .lock()
                .entry(key)
                .or_default()
                .push(id);
        }
        if r.pos != data.len()
            || entry_raw < -1
            || (entry_raw >= 0 && entry_raw as usize >= node_count)
        {
            return Err(err());
        }
        let entry_point = (entry_raw >= 0).then_some(entry_raw as u32);
        if let Some(entry) = entry_point {
            if index.nodes.get(entry).neighbors.lock().len() != max_level + 1 {
                return Err(err());
            }
        } else if live > 0 || max_level != 0 {
            return Err(err());
        }
        *index.topology.lock() = Topology {
            entry_point,
            max_level,
        };
        index.live.store(live, Ordering::Release);
        index.stale_count.store(stale, Ordering::Release);
        index.next_seq.store(node_count as u64, Ordering::Release);
        Ok(index)
    }
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(eof)?;
        let s = self.buf.get(self.pos..end).ok_or_else(eof)?;
        self.pos = end;
        Ok(s)
    }
    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> io::Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> io::Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn f32(&mut self) -> io::Result<f32> {
        Ok(f32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
}

fn eof() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "hnsw truncated")
}

// IEEE CRC32 keeps the existing .hnsw disk checksum unchanged.
use crc32fast::hash as crc32;

#[cfg(test)]
#[allow(unused_must_use)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;

    fn rid(n: u32) -> RowId {
        RowId {
            page_id: n,
            slot_id: (n % 13) as u16,
        }
    }

    /// Deterministic pseudo-random vectors (no Math.random in this env).
    fn gen_vectors(n: usize, dim: usize) -> Vec<Vec<f32>> {
        let mut out = Vec::with_capacity(n);
        let mut state = 0x1234_5678_9ABC_DEF0u64;
        for _ in 0..n {
            let mut v = Vec::with_capacity(dim);
            for _ in 0..dim {
                state = state
                    .wrapping_mul(0x5851_F42D_4C95_7F2D)
                    .wrapping_add(0x1405_7B7E_F767_814F);
                let u = (state >> 11) as f64 / (1u64 << 53) as f64;
                v.push((u as f32) * 2.0 - 1.0);
            }
            out.push(v);
        }
        out
    }

    fn brute_force(vectors: &[Vec<f32>], query: &[f32], k: usize, metric: Metric) -> Vec<usize> {
        let mut scored: Vec<(f64, usize)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (ordering_distance(query, v, metric), i))
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0));
        scored.into_iter().take(k).map(|(_, i)| i).collect()
    }

    fn searchable_copy_of_compact(
        compact: &HnswIndex,
        vectors: &[Vec<f32>],
        metric: Metric,
        m: usize,
        ef_construction: usize,
    ) -> HnswIndex {
        let searchable = HnswIndex::new(metric, m, ef_construction);
        for (i, vector) in vectors.iter().enumerate() {
            searchable.append_node(rid(i as u32), vector.clone());
        }
        for id in 0..compact.build_node_count() as u32 {
            let links = compact.nodes.get(id).neighbors.lock().clone();
            *searchable.nodes.get(id).neighbors.lock() = links;
        }
        let compact_topology = compact.topology.lock();
        let mut searchable_topology = searchable.topology.lock();
        searchable_topology.entry_point = compact_topology.entry_point;
        searchable_topology.max_level = compact_topology.max_level;
        drop(searchable_topology);
        drop(compact_topology);
        searchable
            .live
            .store(vectors.len(), std::sync::atomic::Ordering::Release);
        searchable
    }

    #[test]
    fn arena_chunks_share_sealed_rows_and_cross_chunk_boundaries() {
        let dim = 3;
        let code = |id: usize, j: usize| ((id * 7 + j) % 251) as i8;
        let arena = |chunks: Vec<Arc<CodeChunk>>, n: usize| QuantArena {
            _charge: None,
            update_epoch: 0,
            chunks,
            n,
            dim,
            scale_sq: 1.0,
            dist_mul: 1.0,
            mids: vec![0.0; dim],
            inv_scale: 1.0,
        };
        let mut builder = ArenaCodesBuilder::new(dim);
        for id in 0..ARENA_CHUNK + 5 {
            builder.push(|row| {
                row.iter_mut()
                    .enumerate()
                    .for_each(|(j, c)| *c = code(id, j))
            });
        }
        let first = arena(builder.finish(), ARENA_CHUNK + 5);
        let mut grown = ArenaCodesBuilder::extend(&first);
        for id in ARENA_CHUNK + 5..3 * ARENA_CHUNK + 1 {
            grown.push(|row| {
                row.iter_mut()
                    .enumerate()
                    .for_each(|(j, c)| *c = code(id, j))
            });
        }
        let grown = arena(grown.finish(), 3 * ARENA_CHUNK + 1);
        assert!(
            Arc::ptr_eq(&first.chunks[0], &grown.chunks[0]),
            "sealed chunk is shared"
        );
        assert_eq!(grown.chunks.len(), 4);
        for id in 0..grown.n as u32 {
            let expected: Vec<i8> = (0..dim).map(|j| code(id as usize, j)).collect();
            assert_eq!(grown.codes_for(id), expected.as_slice(), "{id}");
            assert_eq!(grown.norm(id), super::super::distance::ssq_i8(&expected));
        }
    }

    #[test]
    fn slab_buckets_tile_the_id_space_without_gaps() {
        let mut expected = (0, 0);
        for id in [
            0usize,
            1,
            4095,
            4096,
            12287,
            12288,
            1 << 24,
            SLAB_CAPACITY - 1,
        ] {
            let (bucket, off) = slab_slot(id);
            assert!(
                bucket < SLAB_BUCKETS && off < slab_bucket_len(bucket),
                "{id}"
            );
            assert_eq!(SLAB_BASE * ((1 << bucket) - 1) + off, id);
        }
        for id in 0..(SLAB_BASE * 7) {
            assert_eq!(slab_slot(id), expected, "{id}");
            expected.1 += 1;
            if expected.1 == slab_bucket_len(expected.0) {
                expected = (expected.0 + 1, 0);
            }
        }
    }

    #[test]
    fn audit_bug6_stale_prune_cannot_overwrite_a_new_nearest_edge() {
        let index = HnswIndex::new(Metric::L2, 2, 16);
        for (i, x) in [0.0, 10.0, 20.0, 1.0].into_iter().enumerate() {
            index.append_node(rid(i as u32), vec![x]).unwrap();
        }
        index.wire_edge(1, 0, 0, 1, 2, None);
        index.wire_edge_with_snapshot_hook(2, 0, 0, 1, 1, None, || {
            // T2 lands after T1 snapshots [1, 2], and keeps the nearer id=3.
            index.wire_edge(3, 0, 0, 1, 1, None);
            assert_eq!(index.nodes.get(0).neighbors.lock()[0], vec![3]);
        });
        assert_eq!(index.nodes.get(0).neighbors.lock()[0], vec![3]);
    }

    #[test]
    fn audit_bug2_old_arena_never_scores_updated_codes() {
        let index = HnswIndex::new(Metric::L2, 8, 32);
        for i in 0..64u32 {
            index.insert(rid(i), vec![i as f32, 1.0]).unwrap();
        }
        let old = index.ensure_quant().unwrap();
        assert!(index.try_update_in_place(rid(32), &[32.1, 1.0], 1.0));
        assert!(!index.quant_covers(&old, 32));
        let query = [32.1, 1.0];
        let mut codes = vec![0; 2];
        old.quantize(&query, &mut codes);
        let probe = Probe {
            query: &query,
            quant: Some((&old, &codes, super::super::distance::ssq_i8(&codes))),
        };
        assert_eq!(index.probe_dist(&probe, 32), 0.0);
        for i in 0..8u32 {
            assert!(index.try_update_in_place(rid(i), &[i as f32 + 0.1, 1.0], 1.0));
        }
        let refreshed = index.ensure_quant().unwrap();
        assert!(index.quant_covers(&refreshed, 32));
        assert!(
            !index.quant_covers(&old, 32),
            "an old reader must remain safe after publication"
        );
    }

    #[test]
    fn audit_bug5_queries_do_not_wait_for_quant_builder() {
        let index = Arc::new(HnswIndex::new(Metric::L2, 8, 32));
        for i in 0..64u32 {
            index.insert(rid(i), vec![i as f32, 1.0]).unwrap();
        }
        let old = index.ensure_quant().unwrap();
        for i in 64..96u32 {
            index.insert(rid(i), vec![i as f32, 1.0]).unwrap();
        }
        let builder = index.quant_build.lock();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                tx.send(index.search(&[80.0, 1.0], 1, 96)).unwrap();
            });
            let result = rx.recv_timeout(std::time::Duration::from_secs(2));
            drop(builder);
            assert_eq!(result.unwrap()[0].0, rid(80));
        });
        assert!(old.n < index.ensure_quant().unwrap().n);
    }

    #[test]
    fn audit_bug4_deleted_entry_remains_a_navigation_anchor() {
        let index = HnswIndex::new(Metric::L2, 8, 32);
        for i in 0..256u32 {
            index.insert(rid(i), vec![i as f32, 1.0]).unwrap();
        }
        let entry = index.build_entry().unwrap();
        let row = index.nodes.get(entry.0).row_id;
        assert_eq!(index.mark_deleted_by_row_id(row), 1);
        assert_eq!(index.build_entry(), Some(entry));
        let result = index.search(&[100.5, 1.0], 256, 256);
        assert_eq!(result.len(), 255);
        assert!(result.iter().all(|(r, _)| *r != row));
        for i in 0..256u32 {
            index.mark_deleted_by_row_id(rid(i));
        }
        assert!(index.search(&[0.0, 1.0], 10, 256).is_empty());
        index.insert(rid(500), vec![0.0, 1.0]).unwrap();
        assert_eq!(index.search(&[0.0, 1.0], 1, 257)[0].0, rid(500));
    }

    #[test]
    fn audit_bug7_maximum_level_roundtrips() {
        let index = HnswIndex::new(Metric::L2, 2, 32);
        index.insert(rid(0), vec![1.0]).unwrap();
        let max = super::super::hnsw_paged::VEC_MAX_LAYER;
        index
            .nodes
            .get(0)
            .neighbors
            .lock()
            .resize(max + 1, Vec::new());
        index.topology.lock().max_level = max;
        for id in 0..100_000 {
            assert!(index.random_level(id) <= max);
        }
        let restored = HnswIndex::deserialize(&index.serialize()).unwrap();
        assert_eq!(restored.build_entry(), Some((0, max)));
    }

    #[test]
    fn audit_bug9_compaction_replays_concurrent_dml() {
        let index = HnswIndex::new(Metric::L2, 8, 32);
        for i in 0..128u32 {
            index.insert(rid(i), vec![i as f32, 1.0]).unwrap();
        }
        index.mark_deleted_by_row_id(rid(0));
        let dropped = index.compact_with_rebuild_hook(2, || {
            // This runs after the replacement is linked. These would deadlock if
            // the rebuild still held mutation_gate exclusively, and must all
            // survive the swap instead of cancelling it.
            index.insert(rid(128), vec![128.0, 1.0]).unwrap();
            index.mark_deleted_by_row_id(rid(1));
            assert!(index.try_update_in_place(rid(64), &[64.1, 1.0], 1.0));
        });
        assert_eq!(dropped, 1, "the snapshot tombstone is dropped");
        assert_eq!(index.len(), 127);
        assert_eq!(index.stale_count(), 1, "the concurrent delete is replayed");
        assert!(index.has_live_row_id(rid(128)));
        assert!(!index.has_live_row_id(rid(1)));
        assert!(index.has_live_row_id(rid(64)));
        let found = index.search(&[64.1, 1.0], 1, 32);
        assert_eq!(found[0].0, rid(64));
        assert_eq!(found[0].1, 0.0, "the in-place update is replayed");
        assert_eq!(index.search(&[128.0, 1.0], 1, 32)[0].0, rid(128));
        assert_eq!(index.compact(), 1);
        assert_eq!(index.node_count(), 127);
    }

    #[test]
    fn insertion_fanout_uses_m_not_reverse_edge_capacity() {
        let index = HnswIndex::new(Metric::L2, 16, 64);
        for (i, vector) in gen_vectors(96, 24).into_iter().enumerate() {
            index.insert(rid(i as u32), vector).unwrap();
        }
        let mut vector = Vec::new();
        let mut neighbors = Vec::new();
        index.copy_build_node_into(95, &mut vector, &mut neighbors);
        assert!(!neighbors[0].is_empty() && neighbors[0].len() <= 16);
    }

    #[test]
    fn compact_build_transfers_one_charge_and_releases_it_on_success_or_abort() {
        use crate::engine::executor::mem_budget::{
            controlled_memory_snapshot, ControlledMemoryCategory, ControlledMemoryCharge,
        };
        let vectors = gen_vectors(128, 24);
        let mut stats = CompactHnswStats::new(Metric::L2);
        for vector in &vectors {
            stats.observe(vector);
        }
        let baseline = controlled_memory_snapshot().0;
        let bytes = stats.estimated_bytes(16, 2);
        for abort in [false, true] {
            let charge =
                ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, bytes)
                    .unwrap();
            let mut builder = CompactHnswBuilder::new(stats.clone(), 16, 64, charge).unwrap();
            for (id, vector) in vectors.iter().enumerate() {
                builder.push(rid(id as u32), vector).unwrap();
            }
            assert_eq!(
                controlled_memory_snapshot().0,
                baseline + bytes,
                "appending prepaid nodes must not charge the same topology again"
            );
            if abort {
                assert!(builder.push(rid(128), &vectors[0]).is_err());
                drop(builder);
            } else {
                let graph = builder.finish(2).unwrap();
                assert_eq!(controlled_memory_snapshot().0, baseline + bytes);
                assert!(graph.resident_charge.lock().is_none());
                drop(graph);
            }
            assert_eq!(controlled_memory_snapshot().0, baseline);
        }
    }

    #[test]
    fn prepaid_build_parallel_append_cannot_exceed_reserved_capacity() {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        let graph = HnswIndex::new(Metric::Dot, 4, 8).with_build_reservation(
            ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::StorageScratch,
                4 * 1024 * 1024,
            )
            .unwrap(),
            64,
        );
        let accepted = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let graph = &graph;
                let accepted = &accepted;
                scope.spawn(move || {
                    for row in 0..32 {
                        if graph
                            .append_node(rid(worker * 32 + row), vec![1.0, 2.0])
                            .is_ok()
                        {
                            accepted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(accepted.load(Ordering::Relaxed), 64);
        assert_eq!(graph.build_node_count(), 64);
        assert!(graph.resident_charge.lock().is_none());
    }

    #[test]
    fn compact_sq8_build_has_bounded_topology_and_recall_for_l2_and_cosine() {
        let (n, nq, dim, m, ef_construction, k) = (500, 40, 24, 16, 64, 10);
        let all = gen_vectors(n + nq, dim);
        for metric in [Metric::L2, Metric::Cosine] {
            let mut stats = CompactHnswStats::new(metric);
            for vector in &all[..n] {
                stats.observe(vector);
            }
            assert!(stats.quantizable());
            let mut builder = CompactHnswBuilder::new(
                stats.clone(),
                m,
                ef_construction,
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    stats.estimated_bytes(m, 4),
                )
                .unwrap(),
            )
            .expect("compact builder");
            for (i, vector) in all[..n].iter().enumerate() {
                builder.push(rid(i as u32), vector).unwrap();
            }
            let compact = builder.finish(4).unwrap();
            assert_eq!(compact.build_node_count(), n);
            assert!(compact.build_entry().is_some());
            for id in 0..n as u32 {
                let layers = compact.nodes.get(id).neighbors.lock();
                for (level, neighbors) in layers.iter().enumerate() {
                    assert!(
                        neighbors.len() <= if level == 0 { m * 2 } else { m },
                        "metric={metric:?} id={id} level={level} degree={}",
                        neighbors.len()
                    );
                }
            }

            let searchable =
                searchable_copy_of_compact(&compact, &all[..n], metric, m, ef_construction);
            let mut hits = 0usize;
            for query in &all[n..] {
                let truth: HashSet<usize> = brute_force(&all[..n], query, k, metric)
                    .into_iter()
                    .collect();
                hits += searchable
                    .search(query, k, 128)
                    .into_iter()
                    .filter(|(row, _)| truth.contains(&(row.page_id as usize)))
                    .count();
            }
            let recall = hits as f64 / (nq * k) as f64;
            assert!(recall >= 0.90, "metric={metric:?} recall={recall}");
        }
    }

    #[test]
    fn compact_stats_are_capacity_based_and_reject_dot_or_degenerate_ranges() {
        let mut l2 = CompactHnswStats::new(Metric::L2);
        l2.observe(&[1.0, 2.0, 3.0]);
        l2.observe(&[3.0, 2.0, 1.0]);
        assert!(l2.quantizable());
        let estimate = l2.estimated_bytes(16, 4);
        assert!(estimate >= 2 * 3 + 2 * std::mem::size_of::<i32>());

        let mut dot = CompactHnswStats::new(Metric::Dot);
        dot.observe(&[1.0, 2.0, 3.0]);
        assert!(!dot.quantizable());

        let mut flat = CompactHnswStats::new(Metric::L2);
        flat.observe(&[1.0, 1.0]);
        flat.observe(&[1.0, 1.0]);
        assert!(!flat.quantizable());
    }

    /// One-off build-time crossover bench vs the SME brute-force kNN-graph
    /// prototype (`src/bin/sme_knn_build.c`). Drives the REAL cold-build
    /// (`append_node` × N + `link_all`) so the timing is production Phase B+C+D.
    ///
    /// Run: `MYDB_HNSW_BUILD_TIMING=1 cargo test --release -p mydb \
    ///        engine::vector::hnsw::tests::build_time_crossover_bench \
    ///        -- --ignored --nocapture`
    /// Tunables: `BENCH_DIM` (768), `BENCH_THREADS` (avail cores), `BENCH_SIZES`.
    #[test]
    #[ignore]
    fn build_time_crossover_bench() {
        let dim: usize = std::env::var("BENCH_DIM")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(768);
        let threads: usize = std::env::var("BENCH_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(4)
            });
        let sizes: Vec<usize> = std::env::var("BENCH_SIZES")
            .ok()
            .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
            .unwrap_or_else(|| vec![5000, 10000, 20000, 50000]);
        let (m, ef_c, ef_search, k) = (16usize, 64usize, 128usize, 10usize);
        let nq = 200usize;
        eprintln!(
            "# mydb HNSW cold-build (link_all) dim={dim} M={m} ef_c={ef_c} ef_search={ef_search} threads={threads}"
        );
        eprintln!("{:>8} {:>12} {:>10}", "N", "build_ms", "recall@10");
        for &n in &sizes {
            let all = gen_vectors(n + nq, dim);
            let idx = HnswIndex::new(Metric::L2, m, ef_c);
            for i in 0..n {
                idx.append_node(rid(i as u32), all[i].clone());
            }
            let t = std::time::Instant::now();
            idx.link_all(threads);
            let build_ms = t.elapsed().as_secs_f64() * 1e3;

            let mut hit = 0usize;
            for qi in n..n + nq {
                let q = &all[qi];
                let truth: HashSet<usize> = brute_force(&all[..n], q, k, Metric::L2)
                    .into_iter()
                    .collect();
                for (r, _) in idx.search(q, k, ef_search) {
                    if truth.contains(&(r.page_id as usize)) {
                        hit += 1;
                    }
                }
            }
            let recall = hit as f64 / (nq * k) as f64;
            eprintln!("{:>8} {:>12.1} {:>10.4}", n, build_ms, recall);
        }
    }

    #[test]
    fn byte_visit_generations_wrap_and_grow_without_stale_hits() {
        let mut scratch = SearchScratch::new();
        for _ in 0..520 {
            scratch.begin(16);
            for id in [0, 7, 15, 127] {
                assert!(scratch.visit(id));
                assert!(!scratch.visit(id));
            }
        }
        assert_eq!(scratch.visited.len(), 128);
    }

    #[test]
    fn batched_navigation_matches_single_distance_search() {
        for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
            for dim in [3, 31, 128, 257] {
                let vectors = gen_vectors(99, dim);
                let index = HnswIndex::new(metric, 8, 32);
                for (i, v) in vectors[..96].iter().enumerate() {
                    index.append_node(rid(i as u32), v.clone()).unwrap();
                }
                index.link_all(1);
                let arena = index.ensure_quant();
                // A newly appended neighbour may be outside the captured
                // arena. Exercise a mixed four-row group and scalar tail.
                index.append_node(rid(96), vectors[96].clone()).unwrap();
                index.nodes.get(0).neighbors.lock()[0].extend([96, 1, 2, 3, 96]);
                for query in &vectors[97..] {
                    let prepared = if metric == Metric::Cosine {
                        let norm = super::super::distance::dot(query, query).sqrt();
                        query.iter().map(|x| x / norm).collect::<Vec<_>>()
                    } else {
                        query.clone()
                    };
                    let codes = arena.as_ref().map(|a| {
                        let mut codes = vec![0; dim];
                        a.quantize(&prepared, &mut codes);
                        codes
                    });
                    for quantized in [false, true] {
                        let probe = Probe {
                            query,
                            quant: if quantized {
                                arena.as_ref().zip(codes.as_ref()).map(|(a, c)| {
                                    (a.as_ref(), c.as_slice(), super::super::distance::ssq_i8(c))
                                })
                            } else {
                                None
                            },
                        };
                        for ef in [1, 17, 64, 100] {
                            let scalar = index.search_layer_impl::<false>(
                                &probe,
                                &[0],
                                ef,
                                0,
                                false,
                                &mut SearchScratch::new(),
                            );
                            let batched = index.search_layer_impl::<true>(
                                &probe,
                                &[0],
                                ef,
                                0,
                                false,
                                &mut SearchScratch::new(),
                            );
                            let identity = |rows: Vec<Scored>| {
                                rows.into_iter()
                                    .map(|s| (s.id, s.dist.to_bits()))
                                    .collect::<Vec<_>>()
                            };
                            assert_eq!(
                                identity(scalar),
                                identity(batched),
                                "metric={metric:?} dim={dim} ef={ef} quantized={quantized}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn recall_matches_brute_force_l2() {
        let dim = 16;
        let n = 200;
        let vectors = gen_vectors(n, dim);
        let index = HnswIndex::new(Metric::L2, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
        for (i, v) in vectors.iter().enumerate() {
            index.insert(rid(i as u32), v.clone());
        }
        assert_eq!(index.len(), n);
        assert_eq!(index.dim(), dim);

        let queries = gen_vectors(10, dim);
        let k = 10;
        let mut hits = 0usize;
        let mut total = 0usize;
        for q in &queries {
            let truth: HashSet<usize> = brute_force(&vectors, q, k, Metric::L2)
                .into_iter()
                .collect();
            let got = index.search(q, k, 64);
            assert!(got.len() <= k);
            // Results must be ascending by distance.
            for w in got.windows(2) {
                assert!(w[0].1 <= w[1].1);
            }
            for (rid_found, _) in &got {
                // Map row_id back to its vector index via page_id.
                if truth.contains(&(rid_found.page_id as usize)) {
                    hits += 1;
                }
            }
            total += k;
        }
        let recall = hits as f64 / total as f64;
        assert!(recall >= 0.90, "recall@{k} = {recall:.3} too low");
    }

    #[test]
    fn sq8_growth_appends_codes_until_value_range_drifts() {
        let dim = 12usize;
        let vectors = gen_vectors(64, dim);
        let index = HnswIndex::new(Metric::L2, 8, 32);
        for (i, vector) in vectors.iter().enumerate() {
            index.insert(rid(i as u32), vector.clone());
        }
        let before = index.ensure_quant().expect("SQ8 arena");
        let prefix: Vec<i8> = (0..before.n as u32)
            .flat_map(|id| before.codes_for(id).to_vec())
            .collect();
        let old_mids = before.mids.clone();
        let old_inv_scale = before.inv_scale;

        for i in 0..10u32 {
            let local: Vec<f32> = vectors[i as usize].iter().map(|v| v * 0.9).collect();
            index.insert(rid(1000 + i), local);
        }
        let grown = index.ensure_quant().expect("grown SQ8 arena");
        assert_eq!(grown.n, before.n + 10);
        assert_eq!(grown.mids, old_mids);
        assert_eq!(grown.inv_scale, old_inv_scale);
        let grown_prefix: Vec<i8> = (0..before.n as u32)
            .flat_map(|id| grown.codes_for(id).to_vec())
            .collect();
        assert_eq!(grown_prefix, prefix);

        for i in 0..12u32 {
            index.insert(rid(2000 + i), vec![10_000.0 + i as f32; dim]);
        }
        let recalibrated = index.ensure_quant().expect("recalibrated SQ8 arena");
        assert!(recalibrated.n > grown.n);
        assert_ne!(recalibrated.mids, old_mids);
    }

    #[test]
    fn recall_cosine_and_dot() {
        let dim = 24;
        let n = 150;
        let vectors = gen_vectors(n, dim);
        for metric in [Metric::Cosine, Metric::Dot] {
            let index = HnswIndex::new(metric, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
            for (i, v) in vectors.iter().enumerate() {
                index.insert(rid(i as u32), v.clone());
            }
            let queries = gen_vectors(8, dim);
            let k = 10;
            let mut hits = 0usize;
            for q in &queries {
                let truth: HashSet<usize> =
                    brute_force(&vectors, q, k, metric).into_iter().collect();
                for (rid_found, _) in index.search(q, k, 80) {
                    if truth.contains(&(rid_found.page_id as usize)) {
                        hits += 1;
                    }
                }
            }
            let recall = hits as f64 / (queries.len() * k) as f64;
            assert!(recall >= 0.85, "metric {metric:?} recall = {recall:.3}");
        }
    }

    #[test]
    fn serialize_round_trips() {
        let dim = 8;
        let vectors = gen_vectors(128, dim);
        let index = HnswIndex::new(Metric::L2, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
        for (i, v) in vectors.iter().enumerate() {
            index.insert(rid(i as u32), v.clone());
        }
        let bytes = index.serialize();
        let back = HnswIndex::deserialize(&bytes).unwrap();
        assert_eq!(back.len(), index.len());
        assert_eq!(back.dim(), dim);
        let q = &vectors[0];
        let a = index.search(q, 10, 64);
        let b = back.search(q, 10, 64);
        assert_eq!(
            a.iter().map(|(r, _)| r.page_id).collect::<Vec<_>>(),
            b.iter().map(|(r, _)| r.page_id).collect::<Vec<_>>()
        );
        // Corruption is detected.
        let mut corrupt = bytes.clone();
        let mid = corrupt.len() / 2;
        corrupt[mid] ^= 0xFF;
        assert!(HnswIndex::deserialize(&corrupt).is_err());
    }

    #[test]
    fn deserialize_rejects_out_of_range_neighbor_and_level_count() {
        let index = HnswIndex::new(Metric::L2, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
        for i in 0..8 {
            index.insert(rid(i), vec![i as f32, 0.0, 0.0, 0.0]);
        }
        let original = index.serialize();
        let rewrite_crc = |bytes: &mut Vec<u8>| {
            let data_len = bytes.len() - 4;
            let crc = crc32(&bytes[..data_len]);
            bytes[data_len..].copy_from_slice(&crc.to_le_bytes());
        };

        // Header is 51 bytes. Walk the variable-size node records and replace
        // the first real neighbour with node_count (one past the valid range).
        let mut bad_neighbor = original.clone();
        let dim = index.dim();
        let node_count = index.node_count();
        let mut pos = 51usize;
        let mut changed = false;
        for _ in 0..node_count {
            pos += 8 + 1 + dim * 4;
            let levels = bad_neighbor[pos] as usize;
            pos += 1;
            for _ in 0..levels {
                let count =
                    u32::from_le_bytes(bad_neighbor[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                if count > 0 && !changed {
                    bad_neighbor[pos..pos + 4].copy_from_slice(&(node_count as u32).to_le_bytes());
                    changed = true;
                }
                pos += count * 4;
            }
        }
        assert!(changed, "test graph must contain at least one edge");
        rewrite_crc(&mut bad_neighbor);
        assert!(HnswIndex::deserialize(&bad_neighbor).is_err());

        let mut bad_levels = original;
        let first_level_count = 51 + 8 + 1 + dim * 4;
        bad_levels[first_level_count] = 33;
        rewrite_crc(&mut bad_levels);
        assert!(HnswIndex::deserialize(&bad_levels).is_err());
    }

    #[test]
    fn append_node_rejects_dimension_mismatch_after_dimension_is_fixed() {
        let index = HnswIndex::new(Metric::L2, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
        index.append_node(rid(1), vec![1.0, 2.0, 3.0]).unwrap();
        let error = index.append_node(rid(2), vec![1.0, 2.0]).unwrap_err();
        assert_eq!(error.sql_state(), "HY000");
        assert_eq!(index.node_count(), 1);
    }

    #[test]
    fn empty_and_single() {
        let index = HnswIndex::new(Metric::L2, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
        assert!(index.search(&[1.0, 2.0], 5, 64).is_empty());
        index.insert(rid(7), vec![1.0, 2.0, 3.0]);
        let got = index.search(&[1.0, 2.0, 3.0], 5, 64);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0.page_id, 7);
    }

    #[test]
    fn mark_persisted_round_trips_through_serialize() {
        let index = HnswIndex::new(Metric::L2, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
        for i in 0..20 {
            index.insert(rid(i as u32), vec![i as f32, 0.0, 0.0, 0.0]);
        }
        assert_eq!(index.generation(), 0);
        assert_eq!(index.last_persisted_lsn(), 0);
        assert_eq!(index.dirty_count(), 20);

        // Mark a snapshot at LSN 1234, consuming all 20 dirty.
        index.mark_persisted(1234, 20);
        assert_eq!(index.generation(), 1);
        assert_eq!(index.last_persisted_lsn(), 1234);
        assert_eq!(index.dirty_count(), 0);

        // Subsequent inserts re-dirty.
        index.insert(rid(100), vec![100.0, 0.0, 0.0, 0.0]);
        assert_eq!(index.dirty_count(), 1);
        assert_eq!(index.generation(), 1);

        // Serialize → deserialize preserves the bookkeeping fields. The
        // deserialised graph starts clean (no dirty), since the file is
        // the persisted state.
        let bytes = index.serialize();
        let back = HnswIndex::deserialize(&bytes).unwrap();
        assert_eq!(back.generation(), 1);
        assert_eq!(back.last_persisted_lsn(), 1234);
        assert_eq!(back.dirty_count(), 0);
        assert_eq!(back.len(), index.len());
    }

    #[test]
    fn deserialize_rejects_old_version() {
        // Hand-craft a v1 header (magic + version=1) and assert the loader
        // refuses it. This is the "no production v1 file exists, so we
        // can require v2" contract documented in the design doc.
        let mut body = Vec::new();
        body.extend_from_slice(&HNSW_MAGIC.to_le_bytes());
        body.extend_from_slice(&1u16.to_le_bytes()); // version 1
                                                     // Pad some plausible-looking but irrelevant trailing bytes + CRC so
                                                     // the integrity check passes — we want the version check, not the
                                                     // CRC, to be the failure cause.
        body.resize(32, 0);
        let crc = crc32(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        let err = HnswIndex::deserialize(&body).unwrap_err();
        assert!(
            err.to_string().contains("not supported"),
            "expected version error, got: {err}"
        );
    }

    #[test]
    fn mark_deleted_by_row_id_tombstones_every_match() {
        let index = HnswIndex::new(Metric::L2, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
        for i in 0..10 {
            index.insert(rid(i as u32), vec![i as f32, 0.0, 0.0]);
        }
        // Push two extra nodes keyed by row_id(3) — emulates an UPDATE
        // that re-inserted the row before any tombstoning.
        index.insert(rid(3), vec![3.0, 1.0, 0.0]);
        index.insert(rid(3), vec![3.0, 2.0, 0.0]);
        assert_eq!(index.node_count(), 12);
        assert_eq!(index.len(), 12);
        assert_eq!(index.stale_count(), 0);

        let marked = index.mark_deleted_by_row_id(rid(3));
        assert_eq!(marked, 3, "all three nodes keyed by row_id(3) tombstoned");
        assert_eq!(index.len(), 9);
        assert_eq!(index.stale_count(), 3);

        // Idempotent: a second call on a fully-tombstoned key marks nothing.
        let marked_again = index.mark_deleted_by_row_id(rid(3));
        assert_eq!(marked_again, 0);
        assert_eq!(index.stale_count(), 3);

        // search must skip every tombstone.
        let hits = index.search(&[3.0, 0.0, 0.0], 5, 64);
        for (rid_found, _) in &hits {
            assert_ne!(
                rid_found.page_id, 3,
                "search must not return tombstoned row"
            );
        }
    }

    #[test]
    fn mark_deleted_works_after_compact_rebuilds_row_index() {
        // compact() reassigns node ids and clears the row_index; the re-insert
        // path must repopulate it so a subsequent DELETE still tombstones.
        let dim = 6;
        let n = 60;
        let vectors = gen_vectors(n, dim);
        let index = HnswIndex::new(Metric::L2, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
        for (i, v) in vectors.iter().enumerate() {
            index.insert(rid(i as u32), v.clone());
        }
        // Tombstone a third, then compact to reassign ids.
        for i in (0..n).step_by(3) {
            index.mark_deleted_by_row_id(rid(i as u32));
        }
        index.compact();
        // A survivor must still be reachable by row_id through the rebuilt index.
        let survivor = rid(1); // 1 % 3 != 0, so it survived
        let marked = index.mark_deleted_by_row_id(survivor);
        assert_eq!(marked, 1, "row_index rebuilt by compact's re-insert path");
        assert_eq!(
            index.mark_deleted_by_row_id(survivor),
            0,
            "idempotent after the rebuild"
        );
    }

    #[test]
    fn try_update_in_place_takes_local_moves_and_rejects_teleports() {
        let dim = 8;
        let n = 200;
        let vectors = gen_vectors(n, dim);
        let index = HnswIndex::new(Metric::L2, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
        for (i, v) in vectors.iter().enumerate() {
            index.insert(rid(i as u32), v.clone());
        }
        let nodes_before = index.node_count();
        let target = rid(7);

        // A tiny nudge is below ratio·d_nn → in place: no new node, same count,
        // the search now returns the *updated* vector for that row.
        let mut nudged = vectors[7].clone();
        nudged[0] += 1e-4;
        assert!(
            index.try_update_in_place(target, &nudged, 0.5),
            "a tiny move is taken in place"
        );
        assert_eq!(
            index.node_count(),
            nodes_before,
            "in-place keeps the same node — no append"
        );
        assert_eq!(index.stale_count(), 0, "in-place tombstones nothing");

        // A teleport (far from every existing neighbour) exceeds the threshold →
        // rejected, so the caller re-links instead.
        let far = vec![1000.0f32; dim];
        assert!(
            !index.try_update_in_place(target, &far, 0.5),
            "a large move is rejected for the re-link path"
        );
        assert_eq!(index.node_count(), nodes_before, "reject appends nothing");

        // Non-L2 metrics keep the exact re-link path (always reject).
        let cos = HnswIndex::new(Metric::Cosine, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
        for (i, v) in vectors.iter().enumerate().take(20) {
            cos.insert(rid(i as u32), v.clone());
        }
        assert!(
            !cos.try_update_in_place(rid(3), &vectors[3], 0.5),
            "non-L2 never takes the in-place path"
        );
    }

    #[test]
    fn try_update_in_place_is_all_or_nothing_for_duplicate_row_nodes() {
        let dim = 8;
        let vectors = gen_vectors(80, dim);
        let index = HnswIndex::new(Metric::L2, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
        for (i, v) in vectors.iter().enumerate() {
            index.insert(rid(i as u32), v.clone());
        }
        let target = rid(7);
        let original = vectors[7].clone();
        // A second live node for the same row is far away. The first node can
        // accept the tiny move, while this duplicate forces wholesale relink.
        index.insert(target, vec![1000.0; dim]);
        let key = pack_row(target);
        let ids = index.row_index[row_index_shard(key)].lock()[&key].clone();
        let mut nudged = original.clone();
        nudged[0] += 1e-5;
        assert!(!index.try_update_in_place(target, &nudged, 0.5));
        assert_eq!(
            index.nodes.get(ids[0]).vector.read().as_slice(),
            original.as_slice(),
            "a rejected multi-node update must not modify an earlier node"
        );
    }

    #[test]
    fn compact_drops_tombstones_and_rebuilds_topology() {
        let dim = 6;
        let n = 80;
        let vectors = gen_vectors(n, dim);
        let index = HnswIndex::new(Metric::L2, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
        for (i, v) in vectors.iter().enumerate() {
            index.insert(rid(i as u32), v.clone());
        }
        // Tombstone half the graph.
        for i in (0..n).step_by(2) {
            index.mark_deleted_by_row_id(rid(i as u32));
        }
        let nodes_before = index.node_count();
        let live_before = index.len();
        let stale_before = index.stale_count();
        assert_eq!(stale_before, (n / 2) as u32);
        assert_eq!(live_before, n - n / 2);

        let dropped = index.compact();
        assert_eq!(dropped, stale_before);
        assert_eq!(index.stale_count(), 0);
        assert_eq!(index.len(), live_before);
        assert_eq!(
            index.node_count(),
            nodes_before - stale_before as usize,
            "compact removes exactly the tombstoned nodes"
        );

        // Every survivor in the post-compact search must come from the
        // odd-index set (the even-index originals were tombstoned).
        let survivors: HashSet<usize> = (1..n).step_by(2).collect();
        let got = index.search(&vectors[1], 5, 64);
        for (rid_found, _) in &got {
            let page = rid_found.page_id as usize;
            assert!(
                survivors.contains(&page),
                "compacted search returned row {page} which is not a survivor"
            );
        }
    }

    #[test]
    fn stale_count_recomputed_from_serialized_tombstones() {
        let index = HnswIndex::new(Metric::L2, DEFAULT_M, DEFAULT_EF_CONSTRUCTION);
        for i in 0..16 {
            index.insert(rid(i as u32), vec![i as f32, 0.0, 0.0]);
        }
        for i in [2, 7, 11] {
            index.mark_deleted_by_row_id(rid(i));
        }
        assert_eq!(index.stale_count(), 3);
        let back = HnswIndex::deserialize(&index.serialize()).unwrap();
        assert_eq!(
            back.stale_count(),
            3,
            "deserialise recomputes stale_count from per-node deleted flags"
        );
        assert_eq!(back.len(), 13);
    }

    // ---- M3 concurrency tests ----

    #[test]
    fn concurrent_inserts_match_serial_recall() {
        // Eight threads insert disjoint slices of the same vector set into
        // a shared HnswIndex; recall against brute-force on the union must
        // be close to what a single-threaded build produces. Both inserts
        // and searches run without any outer write lock — exercising the
        // M3 per-node-latch path.
        let dim = 12;
        let n = 320;
        let vectors = gen_vectors(n, dim);
        let queries = gen_vectors(16, dim);
        let k = 10;

        let parallel_index = Arc::new(HnswIndex::new(
            Metric::L2,
            DEFAULT_M,
            DEFAULT_EF_CONSTRUCTION,
        ));
        let threads = 8;
        let chunk = n.div_ceil(threads);
        let mut handles = Vec::new();
        for t in 0..threads {
            let lo = t * chunk;
            let hi = ((t + 1) * chunk).min(n);
            if lo >= hi {
                break;
            }
            let idx = Arc::clone(&parallel_index);
            let slice: Vec<(usize, Vec<f32>)> = (lo..hi).map(|i| (i, vectors[i].clone())).collect();
            handles.push(std::thread::spawn(move || {
                for (i, v) in slice {
                    idx.insert(rid(i as u32), v);
                }
            }));
        }
        for h in handles {
            h.join().expect("insert thread panicked");
        }
        assert_eq!(parallel_index.len(), n);
        assert_eq!(parallel_index.dim(), dim);

        // Recall vs brute-force should stay within a few percent of the
        // single-thread baseline. Same recall floor as the serial test —
        // multi-thread insertion order is non-deterministic, so we don't
        // demand an exact match, just acceptable recall.
        let mut hits = 0usize;
        let mut total = 0usize;
        for q in &queries {
            let truth: HashSet<usize> = brute_force(&vectors, q, k, Metric::L2)
                .into_iter()
                .collect();
            for (rid_found, _) in parallel_index.search(q, k, 64) {
                if truth.contains(&(rid_found.page_id as usize)) {
                    hits += 1;
                }
            }
            total += k;
        }
        let recall = hits as f64 / total as f64;
        assert!(
            recall >= 0.85,
            "parallel-insert recall@{k} = {recall:.3} too low (threads={threads})"
        );
    }

    #[test]
    fn concurrent_insert_and_search_does_not_deadlock() {
        // One writer thread streams inserts while several reader threads
        // hammer search. The test is bounded by a wall-clock budget; if the
        // per-node latches or the topology mutex were ordered wrong, the
        // join would never return and the test runner's timeout would
        // surface it.
        let dim = 8;
        let warmup_n = 64;
        let total_n = 256;
        let vectors = gen_vectors(total_n, dim);
        let index = Arc::new(HnswIndex::new(
            Metric::L2,
            DEFAULT_M,
            DEFAULT_EF_CONSTRUCTION,
        ));
        // Warm with some nodes so search has something to return.
        for i in 0..warmup_n {
            index.insert(rid(i as u32), vectors[i].clone());
        }
        let stop = Arc::new(AtomicBool::new(false));
        let mut readers = Vec::new();
        for r in 0..4 {
            let idx = Arc::clone(&index);
            let stop = Arc::clone(&stop);
            let q = vectors[r * 3 % warmup_n].clone();
            readers.push(std::thread::spawn(move || {
                let mut iters = 0u64;
                while !stop.load(Ordering::Acquire) {
                    let _ = idx.search(&q, 10, 64);
                    iters += 1;
                }
                iters
            }));
        }
        // Writer: append the remaining nodes serially (no other writer to race with
        // for this test; the M3 contract is "search is unblocked while a writer
        // appends", not "many writers".)
        let writer_idx = Arc::clone(&index);
        let writer_vecs: Vec<(usize, Vec<f32>)> = (warmup_n..total_n)
            .map(|i| (i, vectors[i].clone()))
            .collect();
        let writer = std::thread::spawn(move || {
            for (i, v) in writer_vecs {
                writer_idx.insert(rid(i as u32), v);
            }
        });
        writer.join().expect("writer panicked");
        stop.store(true, Ordering::Release);
        for r in readers {
            let iters = r.join().expect("reader panicked");
            // Sanity check the reader actually ran; we don't care about
            // throughput, only that we didn't deadlock and that searches
            // were not refused (would return 0 iters if e.g. the topology
            // mutex was permanently held).
            assert!(iters > 0, "reader saw zero search iterations");
        }
        assert_eq!(index.len(), total_n);
    }

    #[test]
    fn concurrent_inserts_many_threads_no_deadlock() {
        // The "two inserts wire opposite-order edges" race. 8 threads each
        // insert distinct row_ids drawn from a small, dense vector space —
        // they will frequently pick overlapping neighbours, so the
        // ascending-id wire ordering is the only thing standing between us
        // and a deadlock.
        let dim = 4;
        let n = 256;
        let vectors = gen_vectors(n, dim);
        let index = Arc::new(HnswIndex::new(
            Metric::L2,
            DEFAULT_M,
            DEFAULT_EF_CONSTRUCTION,
        ));
        // Seed a single node so the topology lock is past its first-insert
        // early return; that way every thread exercises the full wire path.
        index.insert(rid(0), vectors[0].clone());
        let threads = 8;
        let mut handles = Vec::new();
        for t in 0..threads {
            let idx = Arc::clone(&index);
            let slice: Vec<(usize, Vec<f32>)> = (1..n)
                .filter(|i| i % threads == t)
                .map(|i| (i, vectors[i].clone()))
                .collect();
            handles.push(std::thread::spawn(move || {
                for (i, v) in slice {
                    idx.insert(rid(i as u32), v);
                }
            }));
        }
        for h in handles {
            h.join().expect("insert thread panicked");
        }
        assert_eq!(index.len(), n);
        // After all inserts complete, search still returns sensible results.
        let got = index.search(&vectors[0], 10, 64);
        assert!(!got.is_empty(), "search after concurrent inserts is empty");
    }

    #[test]
    fn compact_build_then_swap_never_exposes_an_empty_live_graph() {
        let index = Arc::new(HnswIndex::new(
            Metric::L2,
            DEFAULT_M,
            DEFAULT_EF_CONSTRUCTION,
        ));
        for i in 0..128u32 {
            index.insert(rid(i), vec![i as f32, 1.0, 2.0, 3.0]);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let reader_index = Arc::clone(&index);
        let reader_stop = Arc::clone(&stop);
        let reader = std::thread::spawn(move || {
            let mut searches = 0u64;
            while !reader_stop.load(Ordering::Acquire) || searches < 2_000 {
                let got = reader_index.search(&[0.0, 1.0, 2.0, 3.0], 1, 32);
                assert!(!got.is_empty(), "compact exposed an empty graph");
                searches += 1;
            }
            searches
        });
        for _ in 0..8 {
            index.compact();
        }
        stop.store(true, Ordering::Release);
        assert!(reader.join().unwrap() >= 2_000);
        assert_eq!(index.len(), 128);
    }

    #[test]
    fn concurrent_tombstones_match_serial() {
        // Two threads tombstone disjoint row_ids in parallel; the resulting
        // stale_count and live count must match the serial-DELETE answer.
        let index = Arc::new(HnswIndex::new(
            Metric::L2,
            DEFAULT_M,
            DEFAULT_EF_CONSTRUCTION,
        ));
        for i in 0..40 {
            index.insert(rid(i as u32), vec![i as f32, 0.0, 0.0]);
        }
        let a_idx = Arc::clone(&index);
        let a = std::thread::spawn(move || {
            for i in (0..20).step_by(2) {
                a_idx.mark_deleted_by_row_id(rid(i as u32));
            }
        });
        let b_idx = Arc::clone(&index);
        let b = std::thread::spawn(move || {
            for i in (20..40).step_by(2) {
                b_idx.mark_deleted_by_row_id(rid(i as u32));
            }
        });
        a.join().unwrap();
        b.join().unwrap();
        assert_eq!(index.stale_count(), 20);
        assert_eq!(index.len(), 20);
    }
}

#[cfg(test)]
mod source_audit_tests {
    use super::*;
    fn row(n: u32) -> RowId {
        RowId {
            page_id: n,
            slot_id: 0,
        }
    }
    fn node(id: u32) -> Node {
        Node {
            update_epoch: AtomicU64::new(0),
            seq: id as u64,
            row_id: row(id),
            vector: RwLock::new(vec![1.0, 2.0]),
            neighbors: Mutex::new(vec![vec![]]),
            deleted: AtomicBool::new(false),
        }
    }
    #[test]
    fn source_audit_b4_reserved_slot_is_not_published() {
        let slab = NodeSlab::new();
        let (claimed_tx, claimed_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let slab_ref = &slab;
            let writer = scope.spawn(move || {
                slab_ref
                    .push_with(|id| {
                        claimed_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        node(id)
                    })
                    .unwrap()
            });
            claimed_rx.recv().unwrap();
            // Out-of-order completion must not publish across the unfinished slot.
            assert_eq!(slab.push_with(node).unwrap(), 1);
            let prefix = slab.published_len();
            release_tx.send(()).unwrap();
            writer.join().unwrap();
            assert_eq!(prefix, 0);
        });
        assert_eq!(slab.published_len(), 2);
        assert_eq!(*slab.get(1).vector.read(), [1.0, 2.0]);
        let other = NodeSlab::new();
        slab.swap_locked(&other);
        assert_eq!(slab.published_len(), 0);
        assert_eq!(other.published_len(), 2);
        other.clear_locked();
        assert_eq!(other.published_len(), 0);
    }
    #[test]
    fn source_audit_b5_reconcile_updated_and_reused_row() {
        let graph = HnswIndex::new(Metric::L2, 4, 32);
        graph.insert(row(1), vec![1.0, 2.0]).unwrap();
        graph.reconcile_heap_vector(row(1), vec![1.0, 2.0]).unwrap();
        assert_eq!(graph.build_node_count(), 1);
        graph
            .reconcile_heap_vector(row(1), vec![100.0, 200.0])
            .unwrap();
        assert_eq!(graph.search(&[100.0, 200.0], 1, 32)[0], (row(1), 0.0));
        graph.mark_deleted_by_row_id(row(1));
        graph
            .reconcile_heap_vector(row(1), vec![-100.0, -200.0])
            .unwrap();
        assert_eq!(graph.search(&[-100.0, -200.0], 1, 32)[0], (row(1), 0.0));
        assert_eq!(graph.len(), 1);
    }
}

#[cfg(test)]
mod review_20261024 {
    use super::*;

    #[test]
    fn review_compaction_must_preserve_update_epoch() {
        let graph = HnswIndex::new(Metric::L2, 16, 200);
        for i in 0..100u32 {
            let v = if i == 0 { 0.0 } else { 10.0 + i as f32 };
            graph
                .insert(
                    RowId {
                        page_id: i,
                        slot_id: 0,
                    },
                    vec![v, 0.0],
                )
                .unwrap();
        }
        graph.search(&[0.0, 0.0], 1, 64);
        graph.mark_deleted_by_row_id(RowId {
            page_id: 99,
            slot_id: 0,
        });
        assert!(
            graph.compact_with_rebuild_hook(1, || {
                assert!(graph.try_update_in_place(
                    RowId {
                        page_id: 0,
                        slot_id: 0
                    },
                    &[2.0, 0.0],
                    0.5
                ));
            }) > 0
        );
        let _guard = graph.structural.read();
        let arena = graph.quant.read().clone().unwrap();
        let node_epoch = graph.nodes.get(0).update_epoch.load(Ordering::Acquire);
        eprintln!(
            "global_epoch={} arena_epoch={} node_epoch={}",
            graph.update_epoch.load(Ordering::Acquire),
            arena.update_epoch,
            node_epoch
        );
        assert!(
            !graph.quant_covers(&arena, 0),
            "a replayed vector update must invalidate pre-update SQ8 codes"
        );
    }
}

#[cfg(test)]
mod query_preparation_regressions {
    use super::*;

    #[test]
    fn cosine_probe_is_scale_invariant_and_l2_range_drift_uses_exact_navigation() {
        for metric in [Metric::Cosine, Metric::L2] {
            let graph = HnswIndex::new(metric, 16, 200);
            for (id, vector) in [[1.0, 0.0], [0.0, 1.0], [-1.0, 0.0], [0.0, -1.0]]
                .into_iter()
                .enumerate()
            {
                graph
                    .insert(
                        RowId {
                            page_id: id as u32,
                            slot_id: 0,
                        },
                        vector.to_vec(),
                    )
                    .unwrap();
            }
            let _guard = graph.structural.read();
            let arena = graph.ensure_quant().unwrap();
            if metric == Metric::Cosine {
                assert_eq!(
                    arena.prepare_query(metric, &[0.6, 0.8]),
                    arena.prepare_query(metric, &[6.0, 8.0])
                );
            } else {
                assert!(arena.prepare_query(metric, &[1000.0, 1000.0]).is_none());
            }
        }
    }

    #[test]
    fn disconnected_duplicate_cold_graph_is_repaired_without_exceeding_degree() {
        let graph = HnswIndex::new(Metric::L2, 16, 200);
        for id in 0..256 {
            graph
                .append_node(
                    RowId {
                        page_id: id,
                        slot_id: 0,
                    },
                    vec![1.0, 1.0],
                )
                .unwrap();
        }
        graph.link_all(4);
        assert_eq!(graph.search(&[1.0, 1.0], 256, 256).len(), 256);
        for id in 0..graph.nodes.len() as u32 {
            assert!(graph.nodes.get(id).neighbors.lock()[0].len() <= graph.m0);
        }
    }
}

#[cfg(test)]
mod asynchronous_quant_regressions {
    use super::*;
    #[test]
    fn published_queries_request_refresh_without_building_the_arena() {
        let graph = HnswIndex::new(Metric::L2, 16, 200);
        for id in 0..128 {
            graph
                .insert(
                    RowId {
                        page_id: id,
                        slot_id: 0,
                    },
                    vec![id as f32, 1.0],
                )
                .unwrap();
        }
        graph.enable_background_quant();
        assert_eq!(graph.search(&[0.0, 1.0], 1, 64)[0].0.page_id, 0);
        assert!(graph.quant.read().is_none());
        assert!(graph.quant_refresh_requested.load(Ordering::Acquire));
        graph.refresh_requested_quant();
        assert!(graph.quant.read().is_some());
        assert_eq!(graph.search(&[0.0, 1.0], 1, 64)[0].0.page_id, 0);
        graph.set_background_quant(false);
        for id in 128..160 {
            graph
                .insert(
                    RowId {
                        page_id: id,
                        slot_id: 0,
                    },
                    vec![id as f32, 1.0],
                )
                .unwrap();
        }
        graph.search(&[0.0, 1.0], 1, 64);
        assert_eq!(
            graph.quant.read().as_ref().unwrap().n,
            160,
            "stopped maintenance must restore synchronous refresh"
        );
    }
}

#[cfg(test)]
mod snapshot_validation_regressions {
    use super::*;
    #[test]
    fn valid_crc_does_not_authorize_unbounded_or_nonfinite_snapshot_fields() {
        let graph = HnswIndex::new(Metric::L2, 16, 200);
        graph
            .insert(
                RowId {
                    page_id: 1,
                    slot_id: 0,
                },
                vec![1.0, 2.0],
            )
            .unwrap();
        let snapshot = graph.serialize();
        for (offset, value) in [
            (7, u32::MAX),
            (31, u32::MAX),
            (60, f32::NAN.to_bits()),
            (69, u32::MAX),
        ] {
            let mut bytes = snapshot.clone();
            bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
            let end = bytes.len() - 4;
            let checksum = crc32(&bytes[..end]);
            bytes[end..].copy_from_slice(&checksum.to_le_bytes());
            assert!(
                HnswIndex::deserialize(&bytes).is_err(),
                "malformed field at {offset}"
            );
        }
    }
}
