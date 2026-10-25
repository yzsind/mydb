//! Page-based HNSW graph algorithm layer.
//!
//! This is the query/insert engine over the paged graph store
//! ([`crate::storage::core::VectorIndexFile`], `.hvec`). It is the
//! buffer-pool-resident successor to the in-memory slab graph in
//! [`super::hnsw`]. Persistent SQ8 navigation rows, adjacency and exact f32
//! vectors are all stored in `.hvec`; a governed optional layer-0 SQ8/CSR
//! image removes buffer-pool bookkeeping from the query hot loop. Finalized
//! files can instead use a revocable direct mapping, avoiding reconstruction
//! of that image on reopen. Mutable pages remain the correctness fallback. Correctness ultimately
//! rests on the executor re-checking
//! heap-row MVCC visibility for returned candidates — the graph is a
//! best-effort candidate generator, exactly as the in-memory version.

use crate::platform::file::{File, OpenOptions};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::hash::{BuildHasherDefault, Hasher};
use std::io::Write;
use std::num::NonZeroUsize;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{
    AtomicBool, AtomicI32, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering,
};
use std::sync::Arc;

use hashbrown::{HashMap, HashSet};
use smallvec::SmallVec;

/// Single-multiply hasher for the `u64` [`VecId`] keys used by the per-traversal
/// scratch (`visited` set, `vec_cache` map). HNSW build/search is dominated by
/// these lookups (millions of `vec_cache.get` per build); the default SipHash
/// turns out to be ~40% of cold-build CPU at 128-d. A vid is a well-distributed
/// `(page<<32 | slot)` packing, so a Fibonacci-multiply (one `mul`) gives ample
/// spread for the open-addressing table while costing a single instruction.
#[derive(Default)]
struct VidHasher(u64);
impl Hasher for VidHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write_u64(&mut self, i: u64) {
        // Fibonacci multiply concentrates entropy in the HIGH bits, but
        // hashbrown's SwissTable selects its bucket from the LOW bits. Since a
        // vid is `(page<<32 | slot)` the low 32 bits are just the slot — so a
        // bare multiply leaves the bucket index independent of the page and
        // same-slot/different-page vids collide catastrophically (measured 3×
        // build slowdown). Fold the high half back into the low with one xor
        // shift so every bucket bit depends on the whole vid.
        let z = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        self.0 = z ^ (z >> 32);
    }
    #[inline]
    fn write_u32(&mut self, i: u32) {
        let z = u64::from(i).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        self.0 = z ^ (z >> 32);
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // vids hash via `write_u64`; this byte fallback exists only for
        // completeness and is never hit on the hot path.
        let mut h = self.0;
        for &b in bytes {
            h = (h.rotate_left(5) ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
        }
        self.0 = h;
    }
}
type VidBuildHasher = BuildHasherDefault<VidHasher>;

use super::distance::{
    dot_i8, dot_i8_batch4_ptrs, ordering_distance, prefetch_read, quantize_into, Metric,
};
use super::hnsw::HnswIndex;
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::storage::{NodeId, RowId, VecId, VectorIndexFile};
use crate::types::QueryError;

/// DDL/system transaction id used for every paged-graph WAL write. The graph
/// is derived data with **no logical undo** (redo-only full-page images),
/// so its physical-redo records are never tied to a user transaction's
/// rollback — they belong to the system, exactly like B-tree DDL writes.
const VEC_DDL_TXN: u64 = 0;

/// Upper bound on a node's HNSW layer. Bounds the reserved per-node
/// neighbour footprint on disk; `random_level` essentially never exceeds
/// this for realistic cardinalities.
pub(crate) const VEC_MAX_LAYER: usize = 16;

/// Below this row count a cold build stays single-threaded: spawn/join +
/// per-thread scratch (each thread re-decodes the vectors it visits) only pays
/// off once there is real navigation work to overlap. Small builds keep the
/// serial path's single-decode resident cache and deterministic result.
const PARALLEL_BUILD_MIN_ROWS: usize = 2048;

#[derive(Clone, Debug, Default)]
pub(crate) struct HnswBuildStats {
    pub(crate) mode: &'static str,
    pub(crate) rows: u64,
    pub(crate) stats_ms: u64,
    pub(crate) encode_ms: u64,
    pub(crate) link_ms: u64,
    pub(crate) materialize_ms: u64,
    pub(crate) total_ms: u64,
    pub(crate) peak_scratch_bytes: u64,
    pub(crate) cache_hits: u64,
    pub(crate) cache_misses: u64,
    pub(crate) chunks: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct HnswChunkBuildStats {
    pub(crate) reserved_bytes: u64,
    pub(crate) cache_hits: u64,
    pub(crate) cache_misses: u64,
    pub(crate) chunks: u64,
    pub(crate) link_ms: u64,
    pub(crate) materialize_ms: u64,
}

/// Whether SQ8 graph-navigation quantization is enabled for the paged cold
/// build. Mirrors the in-memory backend's `MYDB_HNSW_QUANTIZE` toggle (default
/// on); only ever consulted for the L2 metric.
fn paged_quant_enabled() -> bool {
    #[cfg(not(test))]
    {
        static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        return *ENABLED.get_or_init(paged_quant_enabled_uncached);
    }
    #[cfg(test)]
    paged_quant_enabled_uncached()
}

fn paged_quant_enabled_uncached() -> bool {
    super::hnsw_core::quant_enabled_uncached()
}

/// Source vector for SQ8 query quantization: a unit-normalized copy for Cosine
/// (the arena codes are unit-normalized, so the query must match), or the input
/// borrowed unchanged otherwise. A zero vector has no direction → left as-is.
fn quant_src(v: &[f32], metric: Metric) -> std::borrow::Cow<'_, [f32]> {
    if metric == Metric::Cosine {
        let mut normalized = v.to_vec();
        super::distance::normalize_in_place(&mut normalized);
        return std::borrow::Cow::Owned(normalized);
    }
    std::borrow::Cow::Borrowed(v)
}

/// (distance, vid) ordered ascending by distance — same shape as the
/// in-memory `Scored`, but keyed by the paged `VecId`.
type Scored = super::hnsw_core::Scored<VecId>;

/// Optional read-optimized image of the persistent layer-0 graph. SQ8 codes
/// and compact CSR adjacency serve navigation. Exact vectors deliberately stay
/// in the governed data buffer: retaining a second f32 copy makes the optional
/// image four times larger at high dimensions and can reject an otherwise
/// useful accelerator. Final re-rank touches only `ef` vectors, while graph
/// navigation probes many more nodes, so reserving memory for the latter has a
/// much higher payoff. The durable `.hvec` path remains the
/// semantic/admission fallback. The reservation is owned by the arena and
/// follows its last `Arc` owner.
#[derive(Debug)]
struct QueryNavArena {
    epoch: u64,
    dim: usize,
    dist_mul: f32,
    vids: Vec<VecId>,
    // First dense ordinal per physical page. Sparse slots are verified and
    // fall back to binary search; the common packed layout resolves in O(1).
    page_starts: Vec<u32>,
    codes: QueryNavCodes,
    norms: Vec<i32>,
    offsets: Vec<u32>,
    neighbors: QueryNavEdges,
    query_cache: QueryNavCache,
    row_seed: parking_lot::Mutex<Option<RowLookup>>,
    _charge: ControlledMemoryCharge,
}

#[derive(Debug)]
enum QueryNavCodes {
    Resident(Vec<i8>),
    Spilled(super::spill_rows::MappedSpillBytes),
}

impl std::ops::Deref for QueryNavCodes {
    type Target = [i8];
    fn deref(&self) -> &[i8] {
        match self {
            Self::Resident(codes) => codes,
            Self::Spilled(bytes) => {
                // SAFETY: i8/u8 share layout; the immutable owner bounds this borrow.
                unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<i8>(), bytes.len()) }
            }
        }
    }
}

/// Spilled edges retain durable NodeIds; resident edges are dense ordinals.
#[derive(Debug)]
enum QueryNavEdges {
    Resident(Vec<u32>),
    Spilled(super::spill_rows::MappedSpillBytes),
}

impl std::ops::Deref for QueryNavEdges {
    type Target = [u32];
    fn deref(&self) -> &[u32] {
        match self {
            Self::Resident(edges) => edges,
            Self::Spilled(bytes) => {
                if bytes.is_empty() {
                    return &[];
                }
                // SAFETY: mmap is page aligned; the private native-endian spill
                // contains only complete u32 words and is immutable.
                unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<u32>(), bytes.len() / 4) }
            }
        }
    }
}

const QUERY_CACHE_ENTRIES: usize = 512;
const QUERY_CACHE_WAYS: usize = 8;
const QUERY_CACHE_SETS: usize = QUERY_CACHE_ENTRIES / QUERY_CACHE_WAYS;
const QUERY_CACHE_MAX_K: usize = 128;

#[derive(Debug)]
struct QueryCacheEntry {
    hash: u64,
    k: usize,
    ef: usize,
    query_bits: Box<[u32]>,
    result: Arc<[(RowId, f64)]>,
}

/// Small eight-way set-associative memo for exact repeated vector searches. It
/// is owned by the immutable navigation arena. Graph mutations invalidate
/// its epoch; vector overwrites also drop the arena and its immutable codes.
/// Full query bits are checked after the hash, making collisions performance-only. Fixed slots keep
/// admission and capacity accounting deterministic.
#[derive(Debug)]
pub(crate) struct QueryNavCache {
    sets: parking_lot::RwLock<Vec<[Option<QueryCacheEntry>; QUERY_CACHE_WAYS]>>,
}

impl QueryNavCache {
    pub(crate) fn new() -> Self {
        let mut sets: Vec<[Option<QueryCacheEntry>; QUERY_CACHE_WAYS]> =
            std::iter::repeat_with(|| std::array::from_fn(|_| None))
                .take(QUERY_CACHE_SETS)
                .collect();
        sets.shrink_to_fit();
        Self {
            sets: parking_lot::RwLock::new(sets),
        }
    }

    #[inline]
    fn hash(query: &[f32], k: usize, ef: usize) -> u64 {
        let mut hash = 0x9E37_79B9_7F4A_7C15u64
            ^ (k as u64).rotate_left(17)
            ^ (ef as u64).rotate_left(41)
            ^ (query.len() as u64).wrapping_mul(0xA24B_AED4_963E_E407);
        // The fingerprint only routes to an eight-way set; full bits below are
        // still the correctness key. Sampling caps hit cost for 768/1536-d
        // vectors, while collisions merely cause replacement/misses.
        let stride = query.len().saturating_div(32).max(1);
        for &value in query.iter().step_by(stride).take(32) {
            hash ^= u64::from(value.to_bits()).wrapping_mul(0xD6E8_FEB8_6659_FD93);
            hash = hash.rotate_left(27).wrapping_mul(0x9E37_79B1_85EB_CA87);
        }
        hash ^ (hash >> 32)
    }

    pub(crate) fn get(&self, query: &[f32], k: usize, ef: usize) -> Option<Vec<(RowId, f64)>> {
        if k > QUERY_CACHE_MAX_K {
            return None;
        }
        let hash = Self::hash(query, k, ef);
        let sets = self.sets.read();
        for entry in sets[(hash as usize) % QUERY_CACHE_SETS].iter().flatten() {
            if entry.hash == hash
                && entry.k == k
                && entry.ef == ef
                && entry.query_bits.len() == query.len()
                // SAFETY: both allocations are valid for `len * 4` bytes;
                // u32/f32 have identical size and alignment. The native
                // in-memory representation is exactly the cache key.
                && unsafe {
                    libc::memcmp(
                        entry.query_bits.as_ptr().cast(),
                        query.as_ptr().cast(),
                        query.len() * std::mem::size_of::<f32>(),
                    ) == 0
                }
            {
                return Some(entry.result.to_vec());
            }
        }
        None
    }

    pub(crate) fn insert(&self, query: &[f32], k: usize, ef: usize, result: &[(RowId, f64)]) {
        if k > QUERY_CACHE_MAX_K || result.len() > QUERY_CACHE_MAX_K {
            return;
        }
        let hash = Self::hash(query, k, ef);
        let entry = QueryCacheEntry {
            hash,
            k,
            ef,
            query_bits: query.iter().map(|value| value.to_bits()).collect(),
            result: Arc::from(result),
        };
        let mut sets = self.sets.write();
        let set = &mut sets[(hash as usize) % QUERY_CACHE_SETS];
        let slot = set
            .iter()
            .position(|current| current.is_none())
            .unwrap_or(((hash >> 32) as usize) % QUERY_CACHE_WAYS);
        set[slot] = Some(entry);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct QueryNavAllocation {
    final_bytes: usize,
    peak_bytes: usize,
    codes_bytes: usize,
    edge_capacity: usize,
    offsets_bytes: usize,
    query_cache_bytes: usize,
    row_seed_bytes: usize,
}

impl QueryNavAllocation {
    /// Cache-local quota and process-wide pressure both permit a spill
    /// transition. A failed optional resident admission must not discard a
    /// useful CSR graph when its much smaller metadata can still be admitted.
    fn try_reserve<T>(
        &self,
        limit: usize,
        mut reserve: impl FnMut(usize) -> Option<T>,
    ) -> Option<(bool, T)> {
        if self.peak_bytes <= limit {
            if let Some(charge) = reserve(self.peak_bytes) {
                return Some((false, charge));
            }
        }
        let spilled_peak = self
            .peak_bytes
            .checked_sub(self.codes_bytes)?
            .checked_sub(self.edge_capacity.checked_mul(4)?)?
            .checked_add(128 * 1024)?;
        // Tiny images gain nothing by adding two spill writer buffers.
        if spilled_peak < self.peak_bytes && spilled_peak <= limit {
            reserve(spilled_peak).map(|charge| (true, charge))
        } else {
            None
        }
    }
}

/// Capacity-based accounting for the resident navigation image and its one
/// temporary offset vector. Keep this independent of allocator state so
/// admission is deterministic for the same index shape and memory target.
fn query_navigation_allocation(
    nodes: usize,
    dim: usize,
    m0: usize,
    pages: usize,
) -> Option<QueryNavAllocation> {
    let codes_bytes = nodes.checked_mul(dim)?;
    let edge_capacity = nodes.checked_mul(m0)?;
    let offsets_bytes = nodes
        .checked_add(1)?
        .checked_mul(std::mem::size_of::<u32>())?;
    let query_cache_bytes = QUERY_CACHE_ENTRIES
        .checked_mul(
            std::mem::size_of::<Option<QueryCacheEntry>>()
                .checked_add(dim.checked_mul(std::mem::size_of::<u32>())?)?
                .checked_add(QUERY_CACHE_MAX_K.checked_mul(std::mem::size_of::<(RowId, f64)>())?)?,
        )?
        .checked_add(QUERY_CACHE_ENTRIES.checked_mul(2 * std::mem::size_of::<usize>())?)?;
    let row_seed_bytes = RowLookup::reserved_bytes(nodes)?;
    let final_bytes = codes_bytes
        .checked_add(pages.checked_mul(std::mem::size_of::<u32>())?)?
        .checked_add(nodes.checked_mul(std::mem::size_of::<i32>())?)?
        .checked_add(nodes.checked_mul(std::mem::size_of::<VecId>())?)?
        .checked_add(offsets_bytes)?
        .checked_add(edge_capacity.checked_mul(std::mem::size_of::<u32>())?)?
        .checked_add(query_cache_bytes)?
        .checked_add(row_seed_bytes)?;
    Some(QueryNavAllocation {
        final_bytes,
        peak_bytes: final_bytes.checked_add(offsets_bytes)?,
        codes_bytes,
        edge_capacity,
        offsets_bytes,
        query_cache_bytes,
        row_seed_bytes,
    })
}

impl QueryNavArena {
    #[inline]
    fn ordinal(&self, vid: VecId) -> Option<usize> {
        let page = usize::try_from(vid >> 32).ok()?;
        let &start = self.page_starts.get(page)?;
        if start != u32::MAX {
            let ordinal = (start as usize).checked_add((vid & 0xffff_ffff) as usize)?;
            if self.vids.get(ordinal) == Some(&vid) {
                return Some(ordinal);
            }
        }
        self.vids.binary_search(&vid).ok()
    }

    #[inline]
    fn distance(&self, qcodes: &[i8], qnorm: i32, ordinal: usize) -> f64 {
        let codes = self.codes_at(ordinal);
        (qnorm + self.norms[ordinal] - 2 * dot_i8(qcodes, codes)) as f64 * (self.dist_mul as f64)
    }

    #[inline]
    fn distances4(&self, qcodes: &[i8], qnorm: i32, ordinals: [usize; 4]) -> [f64; 4] {
        let rows = [
            self.codes_at(ordinals[0]),
            self.codes_at(ordinals[1]),
            self.codes_at(ordinals[2]),
            self.codes_at(ordinals[3]),
        ];
        let dots = dot_i8_batch4_ptrs(qcodes, rows);
        std::array::from_fn(|index| {
            (qnorm + self.norms[ordinals[index]] - 2 * dots[index]) as f64 * (self.dist_mul as f64)
        })
    }

    #[inline]
    fn codes_at(&self, ordinal: usize) -> &[i8] {
        let begin = ordinal * self.dim;
        &self.codes[begin..begin + self.dim]
    }

    #[inline]
    fn neighbors(&self, ordinal: usize) -> &[u32] {
        &self.neighbors[self.offsets[ordinal] as usize..self.offsets[ordinal + 1] as usize]
    }

    #[inline]
    fn prefetch_code_vector(&self, ordinal: usize) {
        let begin = ordinal * self.dim;
        if let QueryNavCodes::Spilled(mapping) = &self.codes {
            mapping.prefetch_range(begin, self.dim);
        }
        // Apple Silicon has 128-byte L1 data lines; mainstream server ARM and
        // x86 use 64. Prefetch the complete high-dimensional code, not just
        // its first line, and let the search loop keep a bounded number of
        // future candidates in flight.
        let stride = if cfg!(all(target_arch = "aarch64", target_os = "macos")) {
            128
        } else {
            64
        };
        let end = begin + self.dim;
        let mut offset = begin;
        while offset < end {
            prefetch_read(self.codes[offset..].as_ptr());
            offset += stride;
        }
    }
}

struct QueryNavScratch {
    visited: Vec<u64>,
    touched_words: Vec<usize>,
    capacity_nodes: usize,
    output_capacity: usize,
    frontier: BinaryHeap<Reverse<Scored>>,
    results: BinaryHeap<Scored>,
    unseen_neighbors: Vec<u32>,
    charge: Option<ControlledMemoryCharge>,
}

// A cache-sized bitmap is cheaper to clear in bulk than to track its first
// touched word on every node visit. Large graphs retain sparse reset.
const QUERY_NAV_DENSE_RESET_WORDS: usize = 4096 / std::mem::size_of::<u64>();

impl QueryNavScratch {
    fn new(nodes: usize, ef: usize) -> Self {
        let ef = ef.min(nodes);
        let words = nodes.saturating_add(63) / 64;
        Self {
            visited: vec![0; words],
            touched_words: Vec::with_capacity(if words > QUERY_NAV_DENSE_RESET_WORDS {
                words.min(ef.saturating_mul(32).max(64))
            } else {
                0
            }),
            capacity_nodes: nodes,
            output_capacity: ef,
            charge: None,
            frontier: BinaryHeap::with_capacity(ef.saturating_mul(2)),
            results: BinaryHeap::with_capacity(ef),
            unseen_neighbors: Vec::with_capacity(64),
        }
    }

    fn try_new(nodes: usize, ef: usize, dim: usize) -> Result<Self, QueryError> {
        let ef = ef.min(nodes);
        let words = nodes.checked_add(63).ok_or_else(query_scratch_error)? / 64;
        let touched = if words > QUERY_NAV_DENSE_RESET_WORDS {
            words.min(ef.saturating_mul(32).max(64))
        } else {
            0
        };
        let bytes = words
            .checked_mul(8)
            .and_then(|n| n.checked_add(touched.saturating_mul(8)))
            .and_then(|n| n.checked_add(ef.saturating_mul(3 * std::mem::size_of::<Scored>() + 24)))
            .and_then(|n| n.checked_add(dim.saturating_mul(8)))
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(query_scratch_error)?;
        let charge =
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::DerivedCache, bytes)?;
        let mut scratch = Self::new(nodes, ef);
        scratch.charge = Some(charge);
        Ok(scratch)
    }

    fn prepare_effort(&mut self, ef: usize) -> Result<(), QueryError> {
        let ef = ef.min(self.capacity_nodes);
        if ef > self.output_capacity {
            if let Some(charge) = self.charge.as_mut() {
                charge.try_grow((ef - self.output_capacity).saturating_mul(24))?;
            }
            self.output_capacity = ef;
        }
        Ok(())
    }

    fn reserve_unseen(&mut self, count: usize) -> Result<(), QueryError> {
        reserve_query_vec(&mut self.unseen_neighbors, count, &mut self.charge)
    }

    fn reset(&mut self) {
        if self.visited.len() <= QUERY_NAV_DENSE_RESET_WORDS {
            self.visited.fill(0);
        } else {
            for word in self.touched_words.drain(..) {
                self.visited[word] = 0;
            }
        }
        self.frontier.clear();
        self.results.clear();
        self.unseen_neighbors.clear();
    }

    #[inline]
    fn visit(&mut self, ordinal: usize) -> Result<bool, QueryError> {
        let word = ordinal >> 6;
        let mask = 1u64 << (ordinal & 63);
        let fresh = self.visited[word] & mask == 0;
        if self.visited.len() > QUERY_NAV_DENSE_RESET_WORDS && self.visited[word] == 0 {
            let needed = self.touched_words.len() + 1;
            reserve_query_vec(&mut self.touched_words, needed, &mut self.charge)?;
            self.touched_words.push(word);
        }
        self.visited[word] |= mask;
        Ok(fresh)
    }

    #[inline]
    fn consider(&mut self, scored: Scored, ef: usize) -> Result<(), QueryError> {
        if self.results.len() < ef {
            reserve_query_heap(&mut self.frontier, &mut self.charge)?;
            reserve_query_heap(&mut self.results, &mut self.charge)?;
            self.frontier.push(Reverse(scored));
            self.results.push(scored);
            return Ok(());
        }
        let furthest = self
            .results
            .peek()
            .map(|item| item.dist)
            .unwrap_or(f64::INFINITY);
        if scored.dist < furthest {
            reserve_query_heap(&mut self.frontier, &mut self.charge)?;
            if let Some(mut top) = self.results.peek_mut() {
                *top = scored;
            }
            self.frontier.push(Reverse(scored));
        }
        Ok(())
    }
}

#[cold]
fn query_scratch_error() -> QueryError {
    crate::storage::error(
        crate::error::catalog::ER_OUTOFMEMORY,
        "HNSW query scratch allocation failed",
    )
}

#[inline]
fn reserve_query_vec<T>(
    values: &mut Vec<T>,
    needed: usize,
    charge: &mut Option<ControlledMemoryCharge>,
) -> Result<(), QueryError> {
    if needed <= values.capacity() {
        return Ok(());
    }
    grow_query_vec(values, needed, charge)
}

#[cold]
#[inline(never)]
fn grow_query_vec<T>(
    values: &mut Vec<T>,
    needed: usize,
    charge: &mut Option<ControlledMemoryCharge>,
) -> Result<(), QueryError> {
    let capacity = needed.max(values.capacity().saturating_mul(2)).max(16);
    let extra = (capacity - values.capacity()).saturating_mul(std::mem::size_of::<T>());
    if let Some(charge) = charge.as_mut() {
        charge.try_grow(extra)?;
    }
    if values.try_reserve_exact(capacity - values.len()).is_err() {
        if let Some(charge) = charge.as_mut() {
            charge.shrink(extra);
        }
        return Err(query_scratch_error());
    }
    Ok(())
}

#[inline]
fn reserve_query_heap<T: Ord>(
    heap: &mut BinaryHeap<T>,
    charge: &mut Option<ControlledMemoryCharge>,
) -> Result<(), QueryError> {
    if heap.len() < heap.capacity() {
        return Ok(());
    }
    grow_query_heap(heap, charge)
}

#[cold]
#[inline(never)]
fn grow_query_heap<T: Ord>(
    heap: &mut BinaryHeap<T>,
    charge: &mut Option<ControlledMemoryCharge>,
) -> Result<(), QueryError> {
    let capacity = heap.capacity().saturating_mul(2).max(16);
    let extra = (capacity - heap.capacity()).saturating_mul(std::mem::size_of::<T>());
    if let Some(charge) = charge.as_mut() {
        charge.try_grow(extra)?;
    }
    if heap.try_reserve_exact(capacity - heap.len()).is_err() {
        if let Some(charge) = charge.as_mut() {
            charge.shrink(extra);
        }
        return Err(query_scratch_error());
    }
    Ok(())
}

thread_local! {
    static QUERY_NAV_SCRATCH: std::cell::RefCell<Option<QueryNavScratch>> = const { std::cell::RefCell::new(None) };
}

/// One global SQ8 scale for an entire chunked build. Reusing it across chunks
/// lets persistent worker caches retain compact codes for old nodes instead
/// of retaining/decoding full f32 vectors.
#[derive(Clone, Copy, Debug)]
struct BuildQuantParams {
    mid: f32,
    inv_scale: f32,
    dist_mul: f32,
}

const BUILD_ADJ_LOCKS: usize = 256;
const BUILD_ADJ_EXTENT_BYTES: u64 = 16 * 1024 * 1024;
pub(crate) const BUILD_NODE_METADATA_BYTES: usize = 2 * std::mem::size_of::<AtomicU64>()
    + std::mem::size_of::<AtomicU8>()
    + std::mem::size_of::<AtomicU32>()
    + std::mem::size_of::<AtomicI32>();

fn build_code_image_fits(rows: usize, dim: usize, m: usize, m0: usize, limit: usize) -> bool {
    let estimate = (|| {
        let codes = rows.checked_mul(dim)?;
        let base = m0.max(4).checked_mul(4)?.checked_add(8)?;
        // HNSW's geometric layer distribution adds ~1/(M-1) upper layers
        // per node. Round up, and include one spare mapped adjacency extent.
        let upper = m
            .max(2)
            .checked_mul(4)?
            .checked_add(8)?
            .div_ceil(m.max(2) - 1);
        let per_row = dim
            .checked_add(base)?
            .checked_add(upper)?
            .checked_add(BUILD_NODE_METADATA_BYTES)?;
        let image = rows
            .checked_mul(per_row)?
            .checked_add(BUILD_ADJ_EXTENT_BYTES as usize)?;
        Some(codes <= limit - limit / 4 && image <= limit - limit / 10)
    })();
    rows != 0 && dim != 0 && estimate == Some(true)
}

/// Shared file-backed extent. Access is restricted to BuildAdjStore's
/// stripe-locked copy routines; no Rust references into mutable pages escape.
struct BuildAdjExtent {
    address: *mut u8,
    _mapping: crate::platform::mapping::FileMapping,
}
// SAFETY: disjoint node records are serialized by the store's stripe locks.
// Extents never move or shrink, and their owner outlives every worker.
unsafe impl Send for BuildAdjExtent {}
unsafe impl Sync for BuildAdjExtent {}
impl BuildAdjExtent {
    fn open(file: &File, offset: u64) -> std::io::Result<Self> {
        // SAFETY: the private extent was materialized before publication;
        // stripe locks synchronize every read and write of its records.
        let mapping = unsafe {
            crate::platform::mapping::FileMapping::open(
                file,
                offset,
                BUILD_ADJ_EXTENT_BYTES as usize,
                true,
            )?
        };
        mapping.advise_random();
        Ok(Self {
            address: mapping.as_ptr(),
            _mapping: mapping,
        })
    }
}

/// Read-only view of a completed prefix of the append-only code spill.
/// The store keeps the file alive and never overwrites a published prefix.
struct BuildCodeMap {
    _mapping: crate::platform::mapping::FileMapping,
    address: *mut libc::c_void,
    len: usize,
}
// SAFETY: mapped bytes are immutable; appends only extend the file beyond
// this view. Workers own an Arc for their complete linking phase.
unsafe impl Send for BuildCodeMap {}
unsafe impl Sync for BuildCodeMap {}
impl BuildCodeMap {
    fn open(file: &File, len: usize) -> std::io::Result<Self> {
        // SAFETY: this published prefix is immutable; appended codes are
        // outside its range and every worker retains the owning Arc.
        let mapping = unsafe { crate::platform::mapping::FileMapping::open(file, 0, len, false)? };
        mapping.advise_random();
        Ok(Self {
            address: mapping.as_ptr().cast(),
            len,
            _mapping: mapping,
        })
    }

    fn copy_into(&self, offset: usize, output: &mut [i8]) -> bool {
        if offset
            .checked_add(output.len())
            .is_none_or(|end| end > self.len)
        {
            return false;
        }
        // SAFETY: checked prefix bounds and an exclusive, disjoint destination.
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.address.cast::<i8>().add(offset),
                output.as_mut_ptr(),
                output.len(),
            );
        }
        true
    }

    fn row(&self, ordinal: usize, dim: usize) -> Option<&[i8]> {
        let offset = ordinal.checked_mul(dim)?;
        if offset.checked_add(dim)? > self.len {
            return None;
        }
        // SAFETY: the immutable published prefix covers this complete row;
        // the borrow cannot outlive the worker's owning Arc.
        Some(unsafe { std::slice::from_raw_parts(self.address.cast::<i8>().add(offset), dim) })
    }
}
/// Ephemeral compact adjacency used only by chunked cold builds.
///
/// Final `.hvec` pages are deliberately wide because they include exact f32
/// vectors and fixed-capacity `u64` VecIds. Randomly updating those pages while
/// the graph is larger than the buffer pool caused the million-row build's
/// dominant `pread`/CLOCK/pwrite amplification. This store keeps only u32
/// ordinals in compact per-node records, then materializes each final node
/// once in ordinal order after linking. Records reserve only the node's actual
/// layers: almost all HNSW nodes are level 0, so reserving all
/// `VEC_MAX_LAYER + 1` layers would turn a ~136-byte record into ~1.2 KiB and
/// recreate the random-I/O working-set problem this sidecar is meant to solve.
struct BuildAdjStore {
    file: File,
    extents: parking_lot::RwLock<Vec<BuildAdjExtent>>,
    codes_mapping: parking_lot::RwLock<Option<Arc<BuildCodeMap>>>,
    codes_file: Option<File>,
    codes_path: PathBuf,
    dim: usize,
    direct_code_mapping: bool,
    disk_charge: crate::engine::executor::spill_quota::SpillCharger,
    path: PathBuf,
    vids: Box<[AtomicU64]>,
    page_starts: Box<[AtomicU32]>,
    published_nodes: AtomicUsize,
    append_lock: parking_lot::Mutex<()>,
    // Global calibration makes a code row's squared norm immutable across
    // chunks. Retain four bytes per row instead of recomputing it on every
    // worker-cache miss. This metadata is included in the build reservation.
    code_norms: Box<[AtomicI32]>,
    published_code_rows: AtomicUsize,
    levels: Box<[AtomicU8]>,
    offsets: Box<[AtomicU64]>,
    locks: Box<[parking_lot::Mutex<()>]>,
    m: usize,
    m0: usize,
    used_bytes: AtomicU64,
    allocated_bytes: AtomicU64,
}

impl BuildAdjStore {
    fn create(
        path: PathBuf,
        m: usize,
        m0: usize,
        expected_rows: usize,
        dim: usize,
    ) -> Result<Self, QueryError> {
        // A process killed before Drop leaves only this deterministic sidecar.
        // The next rebuild removes it before creating a fresh store.
        // Memory-durability tests and freshly-created schemas may not have
        // materialized the derived-index directory because the `.hvec` itself
        // has no physical backing there. The sidecar is a real file, so it
        // must establish that internal parent before using create_new.
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                crate::storage::error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "cannot create temporary HNSW adjacency directory {}: {err}",
                        parent.display()
                    ),
                )
            })?;
        }
        if let Err(err) = std::fs::remove_file(&path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "cannot remove stale temporary HNSW adjacency {}: {err}",
                        path.display()
                    ),
                ));
            }
        }
        let disk_charge = crate::engine::executor::spill_quota::charger_for_new_dir(
            path.parent().unwrap_or_else(|| Path::new(".")),
        )?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|err| {
                crate::storage::error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "cannot create temporary HNSW adjacency {}: {err}",
                        path.display()
                    ),
                )
            })?;
        let codes_path = path.with_extension("build-codes");
        let codes_file = if dim != 0 {
            let opened = (|| -> std::io::Result<File> {
                match std::fs::remove_file(&codes_path) {
                    Ok(()) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => return Err(err),
                }
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&codes_path)
            })();
            match opened {
                Ok(file) => {
                    #[cfg(target_os = "linux")]
                    {
                        use std::os::fd::AsRawFd;
                        // Advisory only: old-node code reads are sparse probes.
                        unsafe {
                            libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM);
                        }
                    }
                    Some(file)
                }
                Err(err) => {
                    let _ = std::fs::remove_file(&path);
                    return Err(crate::storage::error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!(
                            "cannot create HNSW code spill {}: {err}",
                            codes_path.display()
                        ),
                    ));
                }
            }
        } else {
            None
        };
        Ok(Self {
            file,
            extents: parking_lot::RwLock::new(Vec::new()),
            codes_mapping: parking_lot::RwLock::new(None),
            codes_file,
            codes_path,
            dim,
            // Avoid duplicating a nearly resident code image in sixteen LRU
            // caches. This is an access-policy estimate, not pinned memory:
            // all mappings remain reclaimable and the cgroup still governs
            // their physical pages. Larger images retain bounded positioned
            // reads, where fault-driven random access can thrash.
            direct_code_mapping: build_code_image_fits(
                expected_rows,
                dim,
                m,
                m0,
                crate::engine::executor::mem_budget::effective_memory_limit_bytes(),
            ),
            disk_charge,
            path,
            vids: (0..expected_rows).map(|_| AtomicU64::new(0)).collect(),
            // One hint slot per possible node page, without allocating by a
            // sparse external VecId. Uncovered pages use binary search.
            page_starts: (0..expected_rows)
                .map(|_| AtomicU32::new(u32::MAX))
                .collect(),
            published_nodes: AtomicUsize::new(0),
            append_lock: parking_lot::Mutex::new(()),
            code_norms: (0..if dim == 0 { 0 } else { expected_rows })
                .map(|_| AtomicI32::new(0))
                .collect(),
            published_code_rows: AtomicUsize::new(0),
            levels: (0..expected_rows).map(|_| AtomicU8::new(0)).collect(),
            offsets: (0..expected_rows).map(|_| AtomicU64::new(0)).collect(),
            locks: (0..BUILD_ADJ_LOCKS)
                .map(|_| parking_lot::Mutex::new(()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            m: m.max(2),
            m0: m0.max(4),
            used_bytes: AtomicU64::new(0),
            allocated_bytes: AtomicU64::new(0),
        })
    }

    fn io_error(&self, action: &str, err: std::io::Error) -> QueryError {
        crate::storage::error(
            crate::error::catalog::ER_GET_ERRNO,
            format!(
                "temporary HNSW adjacency {action} failed for {}: {err}",
                self.path.display()
            ),
        )
    }

    fn append(&self, vid: VecId, level: usize) -> Result<(), QueryError> {
        let _append = self.append_lock.lock();
        let ordinal = self.published_nodes.load(Ordering::Relaxed);
        if ordinal >= self.vids.len() {
            return Err(crate::storage::error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "HNSW build exceeded its reserved node metadata capacity",
            ));
        }
        if ordinal > 0 && self.vids[ordinal - 1].load(Ordering::Relaxed) >= vid {
            return Err(crate::storage::error(
                crate::error::catalog::ER_GET_ERRNO,
                "temporary HNSW adjacency received non-monotonic VecId",
            ));
        }
        let level = level.min(VEC_MAX_LAYER);
        let record_bytes = self.record_bytes_for_level(level) as u64;
        // The append mutex serializes node ordinals, record offsets and file
        // growth are published in one monotonic order before link workers see
        // this chunk.
        let offset = self.used_bytes.load(Ordering::Relaxed);
        let required_bytes = offset.saturating_add(record_bytes);
        let allocated_bytes = self.allocated_bytes.load(Ordering::Relaxed);
        if required_bytes > allocated_bytes {
            let grown = required_bytes
                .div_ceil(BUILD_ADJ_EXTENT_BYTES)
                .saturating_mul(BUILD_ADJ_EXTENT_BYTES);
            // Materialize backing blocks through fallible positioned writes
            // before mmap access. Sparse mmap writes can otherwise SIGBUS on
            // ENOSPC, bypassing SQL error handling. Charge the whole extent.
            let reservation = self
                .disk_charge
                .reserve_write(grown - allocated_bytes, grown - allocated_bytes)?;
            let zeros = [0u8; 64 * 1024];
            let mut at = allocated_bytes;
            while at < grown {
                self.file
                    .write_all_at(&zeros, at)
                    .map_err(|err| self.io_error("allocate", err))?;
                at += zeros.len() as u64;
            }
            drop(reservation);
            let mut extents = self.extents.write();
            while (extents.len() as u64) * BUILD_ADJ_EXTENT_BYTES < grown {
                let offset = extents.len() as u64 * BUILD_ADJ_EXTENT_BYTES;
                extents.push(
                    BuildAdjExtent::open(&self.file, offset)
                        .map_err(|err| self.io_error("map", err))?,
                );
            }
            self.allocated_bytes.store(grown, Ordering::Relaxed);
        }
        self.vids[ordinal].store(vid, Ordering::Relaxed);
        self.levels[ordinal].store(level as u8, Ordering::Relaxed);
        self.offsets[ordinal].store(offset, Ordering::Relaxed);
        if let Some(start) = usize::try_from(vid >> 32)
            .ok()
            .and_then(|page| self.page_starts.get(page))
        {
            if start.load(Ordering::Relaxed) == u32::MAX {
                if let Some(base) = ordinal
                    .checked_sub((vid & 0xffff_ffff) as usize)
                    .and_then(|base| u32::try_from(base).ok())
                {
                    start.store(base, Ordering::Relaxed);
                }
            }
        }
        self.used_bytes.store(required_bytes, Ordering::Relaxed);
        self.published_nodes.store(ordinal + 1, Ordering::Release);
        Ok(())
    }

    /// Append one chunk's already-quantized codes sequentially. Keep codes
    /// separate from adjacency so graph walks retain the compact topology,
    /// and cache misses never read/normalize the wider exact-vector pages.
    fn append_codes(&self, codes: &[i8], rows: usize) -> Result<(), QueryError> {
        let Some(file) = &self.codes_file else {
            return Ok(());
        };
        let first = self
            .published_vids()
            .len()
            .checked_sub(rows)
            .ok_or_else(|| {
                crate::storage::error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "HNSW code spill row count mismatch",
                )
            })?;
        let offset = first.saturating_mul(self.dim) as u64;
        if codes.len() != rows.saturating_mul(self.dim)
            || first != self.published_code_rows.load(Ordering::Acquire)
            || first > self.code_norms.len()
            || rows > self.code_norms.len().saturating_sub(first)
            || file
                .metadata()
                .map_err(|e| self.io_error("code stat", e))?
                .len()
                != offset
        {
            return Err(crate::storage::error(
                crate::error::catalog::ER_GET_ERRNO,
                "HNSW code spill is not a contiguous append",
            ));
        }
        let reservation = self
            .disk_charge
            .reserve_write(codes.len() as u64, codes.len() as u64)?;
        // SAFETY: i8/u8 have identical layout and alignment; this is a read-only borrow.
        let bytes = unsafe { std::slice::from_raw_parts(codes.as_ptr().cast::<u8>(), codes.len()) };
        file.write_all_at(bytes, offset)
            .map_err(|e| self.io_error("code write", e))?;
        drop(reservation);
        for (norm, row) in self.code_norms[first..first + rows]
            .iter()
            .zip(codes.chunks_exact(self.dim))
        {
            norm.store(dot_i8(row, row), Ordering::Relaxed);
        }
        self.published_code_rows
            .store(first + rows, Ordering::Release);
        // Mapping is optional: keep positioned reads as the fallback if the
        // address space cannot admit a larger view. Linking begins only after
        // the completed chunk is published here; no borrowed view escapes.
        if let Some(len) = usize::try_from(offset)
            .ok()
            .and_then(|n| n.checked_add(bytes.len()))
        {
            // Large fault-driven mappings thrash under a hard memcg limit.
            // Use them only while the whole code image fits a conservative
            // share of physical memory; oversized spills keep buffered I/O.
            let limit = crate::engine::executor::mem_budget::effective_memory_limit_bytes() / 4;
            *self.codes_mapping.write() = if self.direct_code_mapping || len <= limit {
                BuildCodeMap::open(file, len).ok().map(Arc::new)
            } else {
                None
            };
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn prefetch_codes(&self, candidates: &[VecId], scratch: &Scratch) {
        use std::os::fd::AsRawFd;
        // Mapped prefixes fit below the conservative residency ceiling.
        // Taking shared locks or entering the kernel for every edge costs
        // more than the small cached copy. The worker owns this view once per
        // chunk, so this check does not contend on the store's map lock.
        if scratch.build_codes.is_some() {
            return;
        }
        let Some(file) = self.codes_file.as_ref() else {
            return;
        };
        let vids = self.published_vids();
        let mut ranges: SmallVec<[(u64, u64); 32]> = SmallVec::new();
        for &vid in candidates {
            if scratch.vec_cache.peek(&vid).is_some()
                || scratch
                    .arena
                    .as_ref()
                    .is_some_and(|arena| arena.ord_of(vid).is_some())
            {
                continue;
            }
            if let Some(ordinal) = self.ordinal_in(vids, vid) {
                let start = ordinal as u64 * self.dim as u64;
                ranges.push((start, start + self.dim as u64));
            }
        }
        ranges.sort_unstable();
        // Merge adjacent rows. WILLNEED is an optional non-semantic hint;
        // all actual reads still check their result and propagate I/O errors.
        let mut i = 0;
        while i < ranges.len() {
            let (start, mut end) = ranges[i];
            i += 1;
            while i < ranges.len() && ranges[i].0 <= end {
                end = end.max(ranges[i].1);
                i += 1;
            }
            unsafe {
                libc::posix_fadvise(
                    file.as_raw_fd(),
                    start as libc::off_t,
                    (end - start) as libc::off_t,
                    libc::POSIX_FADV_WILLNEED,
                );
            }
        }
    }

    #[cfg(test)]
    fn read_codes(&self, vid: VecId, output: &mut Vec<i8>) -> Result<Option<i32>, QueryError> {
        self.read_codes_with_mapping(vid, output, self.codes_mapping.read().as_deref())
    }

    fn read_codes_with_mapping(
        &self,
        vid: VecId,
        output: &mut Vec<i8>,
        mapping: Option<&BuildCodeMap>,
    ) -> Result<Option<i32>, QueryError> {
        let Some(file) = &self.codes_file else {
            return Ok(None);
        };
        let Some(ordinal) = self.ordinal_in(self.published_vids(), vid) else {
            return Ok(None);
        };
        if ordinal >= self.published_code_rows.load(Ordering::Acquire) {
            return Err(self.io_error(
                "code norm read",
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "code row has not been published",
                ),
            ));
        }
        let norm = self.code_norms[ordinal].load(Ordering::Relaxed);
        output.resize(self.dim, 0);
        if mapping
            .is_some_and(|mapping| mapping.copy_into(ordinal.saturating_mul(self.dim), output))
        {
            return Ok(Some(norm));
        }
        // SAFETY: the caller owns this mutable Vec, and every i8 bit pattern is valid.
        let bytes = unsafe {
            std::slice::from_raw_parts_mut(output.as_mut_ptr().cast::<u8>(), output.len())
        };
        file.read_exact_at(bytes, ordinal.saturating_mul(self.dim) as u64)
            .map_err(|e| self.io_error("code read", e))?;
        Ok(Some(norm))
    }

    /// Metadata slots are written once, then published as a monotonic prefix.
    /// Acquire observes all slot initialization; relaxed atomic loads below
    /// need no shared-lock RMW on the navigation hot path. Fixed-size arrays
    /// never relocate while workers read them, including during later appends.
    fn published_vids(&self) -> &[AtomicU64] {
        &self.vids[..self.published_nodes.load(Ordering::Acquire)]
    }

    fn ordinal_in(&self, vids: &[AtomicU64], vid: VecId) -> Option<usize> {
        // Append-only node pages use dense slots. Verify the hint against the
        // published VecId so sparse slots, gaps and unusually large page ids
        // retain the exact binary-search fallback.
        if let Some(start) = usize::try_from(vid >> 32)
            .ok()
            .and_then(|page| self.page_starts.get(page))
        {
            let start = start.load(Ordering::Relaxed);
            if start != u32::MAX {
                if let Some(ordinal) = (start as usize).checked_add((vid & 0xffff_ffff) as usize) {
                    if vids
                        .get(ordinal)
                        .is_some_and(|value| value.load(Ordering::Relaxed) == vid)
                    {
                        return Some(ordinal);
                    }
                }
            }
        }
        vids.binary_search_by_key(&vid, |value| value.load(Ordering::Relaxed))
            .ok()
    }

    fn mapped_codes<'a>(&self, vid: VecId, mapping: &'a BuildCodeMap) -> Option<(&'a [i8], i32)> {
        let ordinal = self.ordinal_in(self.published_vids(), vid)?;
        if ordinal >= self.published_code_rows.load(Ordering::Acquire) {
            return None;
        }
        Some((
            mapping.row(ordinal, self.dim)?,
            self.code_norms[ordinal].load(Ordering::Relaxed),
        ))
    }

    fn level_cap(&self, level: usize) -> usize {
        if level == 0 {
            self.m0
        } else {
            self.m
        }
    }

    fn level_slot_base(&self, top_level: usize, level: usize) -> usize {
        top_level.saturating_add(1)
            + if level == 0 {
                0
            } else {
                self.m0 + (level - 1).saturating_mul(self.m)
            } * 4
    }

    fn record_bytes_for_level(&self, top_level: usize) -> usize {
        let counts = top_level.saturating_add(1);
        let slots = self.m0.saturating_add(top_level.saturating_mul(self.m));
        (counts.saturating_add(slots.saturating_mul(4)) + 7) & !7
    }

    fn record_meta(&self, ordinal: usize) -> Option<(u64, usize)> {
        if ordinal >= self.published_nodes.load(Ordering::Acquire) {
            return None;
        }
        let offset = self.offsets[ordinal].load(Ordering::Relaxed);
        let top_level = usize::from(self.levels[ordinal].load(Ordering::Relaxed));
        Some((offset, top_level))
    }

    // Both copy routines are called only while holding the node's stripe
    // mutex. Records can straddle extent boundaries, but never overlap.
    fn read_exact_at(&self, mut out: &mut [u8], mut offset: u64) -> Result<(), QueryError> {
        let extents = self.extents.read();
        while !out.is_empty() {
            let index = (offset / BUILD_ADJ_EXTENT_BYTES) as usize;
            let within = (offset % BUILD_ADJ_EXTENT_BYTES) as usize;
            let extent = extents.get(index).ok_or_else(|| {
                self.io_error(
                    "read",
                    std::io::Error::from(std::io::ErrorKind::UnexpectedEof),
                )
            })?;
            let len = out.len().min(BUILD_ADJ_EXTENT_BYTES as usize - within);
            // SAFETY: extent bounds checked, destination is exclusive, source
            // record cannot be written until this reader releases its stripe.
            unsafe {
                std::ptr::copy_nonoverlapping(extent.address.add(within), out.as_mut_ptr(), len);
            }
            offset += len as u64;
            out = &mut out[len..];
        }
        Ok(())
    }

    fn write_all_at(&self, mut input: &[u8], mut offset: u64) -> Result<(), QueryError> {
        let extents = self.extents.read();
        while !input.is_empty() {
            let index = (offset / BUILD_ADJ_EXTENT_BYTES) as usize;
            let within = (offset % BUILD_ADJ_EXTENT_BYTES) as usize;
            let extent = extents.get(index).ok_or_else(|| {
                self.io_error(
                    "write",
                    std::io::Error::from(std::io::ErrorKind::UnexpectedEof),
                )
            })?;
            let len = input.len().min(BUILD_ADJ_EXTENT_BYTES as usize - within);
            // SAFETY: extent bounds checked and this record's stripe excludes
            // other readers/writers. No references into the mapping exist.
            unsafe {
                std::ptr::copy_nonoverlapping(input.as_ptr(), extent.address.add(within), len);
            }
            offset += len as u64;
            input = &input[len..];
        }
        Ok(())
    }

    fn neighbors_by_ordinal(&self, ordinal: usize, level: usize) -> Result<Vec<VecId>, QueryError> {
        let Some((base, top_level)) = self.record_meta(ordinal) else {
            return Ok(Vec::new());
        };
        if level > top_level {
            return Ok(Vec::new());
        }
        let _guard = self.locks[ordinal & (BUILD_ADJ_LOCKS - 1)].lock();
        let mut count = [0u8; 1];
        self.read_exact_at(&mut count, base + level as u64)?;
        let count = usize::from(count[0]).min(self.level_cap(level));
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut raw: SmallVec<[u8; 256]> = SmallVec::new();
        raw.resize(count * 4, 0);
        self.read_exact_at(
            &mut raw,
            base + self.level_slot_base(top_level, level) as u64,
        )?;
        let vids = self.published_vids();
        let mut out = Vec::with_capacity(count);
        for bytes in raw.chunks_exact(4) {
            let neighbor = u32::from_le_bytes(bytes.try_into().unwrap()) as usize;
            if let Some(vid) = vids.get(neighbor) {
                out.push(vid.load(Ordering::Relaxed));
            }
        }
        Ok(out)
    }

    fn neighbors(&self, vid: VecId, level: usize) -> Result<Vec<VecId>, QueryError> {
        let ordinal = {
            let vids = self.published_vids();
            self.ordinal_in(vids, vid)
        };
        match ordinal {
            Some(ordinal) => self.neighbors_by_ordinal(ordinal, level),
            None => Ok(Vec::new()),
        }
    }

    fn set_neighbors(
        &self,
        vid: VecId,
        level: usize,
        neighbors: &[VecId],
    ) -> Result<(), QueryError> {
        let vids = self.published_vids();
        let Some(ordinal) = self.ordinal_in(vids, vid) else {
            return Ok(());
        };
        let Some((base, top_level)) = self.record_meta(ordinal) else {
            return Ok(());
        };
        if level > top_level {
            return Ok(());
        }
        let cap = self.level_cap(level);
        let mut raw: SmallVec<[u8; 256]> = SmallVec::with_capacity(neighbors.len().min(cap) * 4);
        for &neighbor in neighbors.iter().take(cap) {
            if let Some(neighbor_ordinal) = self.ordinal_in(vids, neighbor) {
                raw.extend_from_slice(&(neighbor_ordinal as u32).to_le_bytes());
            }
        }
        let _guard = self.locks[ordinal & (BUILD_ADJ_LOCKS - 1)].lock();
        self.write_all_at(&raw, base + self.level_slot_base(top_level, level) as u64)?;
        self.write_all_at(&[raw.len().div_ceil(4) as u8], base + level as u64)
    }

    fn push_neighbor(&self, vid: VecId, level: usize, neighbor: VecId) -> Result<bool, QueryError> {
        let vids = self.published_vids();
        let (Some(ordinal), Some(neighbor_ordinal)) =
            (self.ordinal_in(vids, vid), self.ordinal_in(vids, neighbor))
        else {
            return Ok(false);
        };
        let Some((base, top_level)) = self.record_meta(ordinal) else {
            return Ok(false);
        };
        if level > top_level {
            return Ok(false);
        }
        let _guard = self.locks[ordinal & (BUILD_ADJ_LOCKS - 1)].lock();
        let mut count = [0u8; 1];
        self.read_exact_at(&mut count, base + level as u64)?;
        let count_usize = usize::from(count[0]);
        if count_usize >= self.level_cap(level) {
            return Ok(false);
        }
        self.write_all_at(
            &(neighbor_ordinal as u32).to_le_bytes(),
            base + self.level_slot_base(top_level, level) as u64 + (count_usize * 4) as u64,
        )?;
        count[0] = count[0].saturating_add(1);
        self.write_all_at(&count, base + level as u64)?;
        Ok(true)
    }

    fn materialize(&self, target: &VectorIndexFile) -> Result<(), QueryError> {
        let count = self.published_vids().len();
        for ordinal in 0..count {
            let vid = self.vids[ordinal].load(Ordering::Relaxed);
            let top = usize::from(self.levels[ordinal].load(Ordering::Relaxed));
            let mut layers = Vec::with_capacity(top + 1);
            for level in 0..=top {
                layers.push(self.neighbors_by_ordinal(ordinal, level)?);
            }
            target.set_all_neighbors(VEC_DDL_TXN, vid, &layers)?;
        }
        Ok(())
    }
}

impl Drop for BuildAdjStore {
    fn drop(&mut self) {
        self.extents.get_mut().clear();
        self.codes_mapping.get_mut().take();
        let mut removed = true;
        for path in [&self.path, &self.codes_path] {
            if path == &self.codes_path && self.codes_file.is_none() {
                continue;
            }
            removed &= std::fs::remove_file(path)
                .map_or_else(|err| err.kind() == std::io::ErrorKind::NotFound, |_| true);
        }
        if removed {
            crate::engine::executor::spill_quota::release_dir(&self.disk_charge);
        } else {
            crate::engine::executor::spill_quota::record_cleanup_failure(
                self.disk_charge.dir_bytes(),
            );
        }
    }
}

/// Immutable, contiguous vector store shared (read-only) by every worker of a
/// cold build. Built once up front — every node's vector laid out back to back,
/// indexed by a dense ordinal via `ord` — it replaces the per-operation
/// `vec_cache` for the build: distance scoring becomes a hash → arena index
/// instead of a buffer-pool pin + decode of a randomly placed per-node `Vec`.
///
/// For the L2 metric the arena also holds an **SQ8 (int8) quantization** of
/// every vector (`codes` + per-node `norms`). The build *navigates* on these —
/// `‖q‖² + ‖c‖² − 2·q·c` via one integer `sdot` — for 4× less memory traffic
/// and a tight integer SIMD loop, exactly mirroring the in-memory backend's
/// `QuantArena`. This is the dominant cold-build lever at 128-d: exact f32
/// navigation is memory-latency bound on scattered 512-byte vectors (≈40% of
/// CPU, and it *doubles* under an 8-thread build from bandwidth contention);
/// the 128-byte int8 codes stay cache-resident and shared across cores. The
/// graph's final correctness is unaffected — the executor re-ranks search
/// candidates with exact f32 — so SQ8 only approximates the *construction*
/// heuristic, the same trade the memory backend already ships.
#[derive(Debug)]
struct BuildVectorArena {
    dim: usize,
    data: Vec<f32>,
    ord: HashMap<VecId, u32, VidBuildHasher>,
    /// SQ8 navigation state (L2 only, when enabled). `codes` is row-major int8
    /// (`ord*dim`); `norms[ord] = Σ codeᵢ²`. Empty `codes` ⇒ navigate on `data`
    /// (Dot/Cosine, or SQ8 disabled / a degenerate value range).
    codes: Vec<i8>,
    norms: Vec<i32>,
    /// Multiplier mapping the integer SSD onto the metric's ordering scale (see
    /// the memory backend's `QuantArena::dist_mul`). `scale_sq` for L2;
    /// `scale_sq × 0.5` for Cosine, whose codes are unit-normalized so the SSD
    /// approximates `2(1−cos)` and the ½ lands it on the `1−cos` scale the f32
    /// fallback (`ordering_distance(.., Cosine)`) uses for uncovered nodes.
    dist_mul: f32,
    mid: f32,
    inv_scale: f32,
}

impl BuildVectorArena {
    /// Build the arena from `data` (ordinal-major f32) + the `vid → ord` map.
    /// Quantizes to SQ8 when `metric == L2`, quantization is enabled, and the
    /// value range is non-degenerate; otherwise leaves `codes` empty (exact f32
    /// navigation).
    fn build(
        dim: usize,
        data: Vec<f32>,
        ord: HashMap<VecId, u32, VidBuildHasher>,
        metric: Metric,
        sq8: bool,
        build_quant: Option<BuildQuantParams>,
    ) -> Self {
        let n = if dim == 0 { 0 } else { data.len() / dim };
        let (mut codes, mut norms, mut mid, mut inv_scale) = (Vec::new(), Vec::new(), 0.0, 1.0);
        // SQ8 navigation: L2 directly; Cosine by quantizing unit-normalized rows
        // (the SSD then ranks by cosine). Dot has no scale-invariant trick.
        let normalize = metric == Metric::Cosine;
        let mut dist_mul = 1.0;
        if sq8 && metric != Metric::Dot && n > 0 {
            // Source rows for quantization: raw `data` for L2, a unit-normalized
            // copy for Cosine. `data` itself stays raw — it backs the exact f32
            // re-rank, and cosine ordering is scale-invariant anyway.
            let norm_data: Option<Vec<f32>> = normalize.then(|| {
                let mut nd = data.clone();
                for row in nd.chunks_mut(dim) {
                    super::distance::normalize_in_place(row);
                }
                nd
            });
            let src: &[f32] = norm_data.as_deref().unwrap_or(&data);
            let params = build_quant.or_else(|| {
                let (mut vmin, mut vmax) = (f32::INFINITY, f32::NEG_INFINITY);
                for &x in src {
                    if x < vmin {
                        vmin = x;
                    }
                    if x > vmax {
                        vmax = x;
                    }
                }
                (vmax > vmin).then(|| {
                    let mid = (vmax as f64 * 0.5 + vmin as f64 * 0.5) as f32;
                    let inv_scale = 254.0 / (vmax - vmin);
                    let scale_sq = inv_scale.recip().powi(2);
                    BuildQuantParams {
                        mid,
                        inv_scale,
                        dist_mul: if normalize { scale_sq * 0.5 } else { scale_sq },
                    }
                })
            });
            if let Some(params) = params
                .filter(|p| p.mid.is_finite() && p.inv_scale.is_normal() && p.dist_mul.is_normal())
            {
                mid = params.mid;
                inv_scale = params.inv_scale;
                dist_mul = params.dist_mul;
                codes = vec![0i8; n * dim];
                norms = vec![0i32; n];
                for id in 0..n {
                    let row = &mut codes[id * dim..id * dim + dim];
                    quantize_into(&src[id * dim..id * dim + dim], mid, inv_scale, row);
                    norms[id] = dot_i8(row, row);
                }
            }
        }
        BuildVectorArena {
            dim,
            data,
            ord,
            codes,
            norms,
            dist_mul,
            mid,
            inv_scale,
        }
    }

    /// Encode directly from the caller's bounded chunk. In particular, cosine
    /// needs only one normalized row, never another chunk-sized f32 copy.
    fn build_chunk(
        dim: usize,
        rows: &[(RowId, Vec<f32>)],
        ord: HashMap<VecId, u32, VidBuildHasher>,
        metric: Metric,
        quant: Option<BuildQuantParams>,
    ) -> Self {
        if let Some(params) = quant.filter(|_| paged_quant_enabled() && metric != Metric::Dot) {
            let mut codes = vec![0; rows.len() * dim];
            let mut norms = Vec::with_capacity(rows.len());
            for ((_, vector), code) in rows.iter().zip(codes.chunks_mut(dim)) {
                quantize_into(
                    &quant_src(vector, metric),
                    params.mid,
                    params.inv_scale,
                    code,
                );
                norms.push(dot_i8(code, code));
            }
            Self {
                dim,
                data: Vec::new(),
                ord,
                codes,
                norms,
                dist_mul: params.dist_mul,
                mid: params.mid,
                inv_scale: params.inv_scale,
            }
        } else {
            let mut data = Vec::with_capacity(rows.len() * dim);
            for (_, vector) in rows {
                data.extend_from_slice(vector);
            }
            // No global calibration: retain exact vectors for every metric.
            Self::build(dim, data, ord, metric, false, None)
        }
    }

    /// Whether SQ8 navigation is active for this arena.
    #[inline]
    fn sq8(&self) -> bool {
        !self.codes.is_empty()
    }

    /// Borrow `vid`'s exact f32 vector, or `None` if it was never loaded (a
    /// dangling edge to a node outside the build set — treated as `+inf`).
    #[inline]
    fn get(&self, vid: VecId) -> Option<&[f32]> {
        let o = *self.ord.get(&vid)? as usize * self.dim;
        Some(&self.data[o..o + self.dim])
    }

    /// `vid`'s dense ordinal, if covered.
    #[inline]
    fn ord_of(&self, vid: VecId) -> Option<usize> {
        self.ord.get(&vid).map(|&o| o as usize)
    }

    /// Int8 codes for ordinal `o`.
    #[inline]
    fn codes_at(&self, o: usize) -> &[i8] {
        &self.codes[o * self.dim..o * self.dim + self.dim]
    }

    /// SQ8 squared-L2 (in real units) from a quantized query (`qcodes`, `qnorm`)
    /// to the node at ordinal `o` — the navigation hot path.
    #[inline]
    fn ssd_from_query(&self, qcodes: &[i8], qnorm: i32, o: usize) -> f64 {
        let ssd = qnorm + self.norms[o] - 2 * dot_i8(qcodes, self.codes_at(o));
        ssd as f64 * (self.dist_mul as f64)
    }

    /// SQ8 squared-L2 (in real units) between two arena nodes `oa`, `ob`.
    #[inline]
    fn ssd_nodes(&self, oa: usize, ob: usize) -> f64 {
        let ssd =
            self.norms[oa] + self.norms[ob] - 2 * dot_i8(self.codes_at(oa), self.codes_at(ob));
        ssd as f64 * (self.dist_mul as f64)
    }
}

/// Per-traversal scratch. `visited` is a `HashSet<VecId>` because vids are
/// sparse `(page,slot)` packings — the in-memory generation array keyed by
/// dense slab ids does not translate — but it (and `vec_cache`) hash via the
/// single-multiply [`VidBuildHasher`] rather than SipHash, which is ~40% of
/// cold-build CPU at 128-d.
///
/// When `arena` is set (cold build) every vector score reads the shared,
/// contiguous [`BuildVectorArena`] — no pins, no per-thread decode, shared across
/// workers. Otherwise (incremental DML insert/search) `vec_cache` memoises each
/// node's (immutable) f32 vector for the lifetime of one operation, so a node
/// visited by `search_layer`, re-scored pairwise by
/// `select_neighbors`/`prune_select`, and re-touched by `wire_edge` is
/// page-read+decoded **at most once** instead of O(ef × m) times.
#[derive(Clone, Debug)]
enum CachedVector {
    Exact(Arc<Vec<f32>>),
    Sq8 { codes: Arc<Vec<i8>>, norm: i32 },
}

struct Scratch {
    visited: HashSet<NodeId, VidBuildHasher>,
    frontier: BinaryHeap<Reverse<Scored>>,
    results: BinaryHeap<Scored>,
    vec_cache: lru::LruCache<VecId, Option<CachedVector>, VidBuildHasher>,
    decode_buf: Vec<f32>,
    arena: Option<Arc<BuildVectorArena>>,
    // Immutable codes remain valid across append-only inserts and tombstones.
    // Adjacency and result caches still require the current topology epoch.
    navigation: Option<Arc<QueryNavArena>>,
    /// SQ8-quantized codes (+`‖·‖²`) of the *current* query vector — the node
    /// being linked, or the search query — set once per operation when the
    /// arena navigates on SQ8, so `dist_q` scores it against arena codes with a
    /// single `sdot` instead of re-quantizing per probe.
    qcodes: Vec<i8>,
    qnorm: i32,
    /// Global scale shared by every chunk of one paged cold build. When set,
    /// old-node cache entries retain SQ8 codes (about 4× the node coverage of
    /// f32) and all cached/current-chunk distances stay on one ordering scale.
    build_quant: Option<BuildQuantParams>,
    incremental_quant: Option<BuildQuantParams>,
    build_adj: Option<Arc<BuildAdjStore>>,
    build_codes: Option<Arc<BuildCodeMap>>,
    /// Maximum retained exact vectors for this scratch. Chunked builds divide
    /// one governor-reserved cache envelope across workers.
    vec_cache_limit: usize,
    cache_hits: u64,
    cache_misses: u64,
    nav_batch_order: Vec<usize>,
    nav_batch_approx: Vec<Option<f64>>,
    nav_batch_distances: Vec<f64>,
    neighbor_buf: Vec<VecId>,
}

impl Default for Scratch {
    fn default() -> Self {
        Self {
            visited: HashSet::with_hasher(VidBuildHasher::default()),
            frontier: BinaryHeap::new(),
            results: BinaryHeap::new(),
            vec_cache: lru::LruCache::with_hasher(
                NonZeroUsize::new(1).unwrap(),
                VidBuildHasher::default(),
            ),
            decode_buf: Vec::new(),
            arena: None,
            navigation: None,
            qcodes: Vec::new(),
            qnorm: 0,
            build_quant: None,
            incremental_quant: None,
            build_adj: None,
            build_codes: None,
            vec_cache_limit: 0,
            cache_hits: 0,
            cache_misses: 0,
            nav_batch_order: Vec::new(),
            nav_batch_approx: Vec::new(),
            nav_batch_distances: Vec::new(),
            neighbor_buf: Vec::new(),
        }
    }
}

impl Scratch {
    /// Reset the per-layer best-first state (frontier/results/visited) while
    /// **keeping** the vector cache/arena, shared across all layers of one
    /// operation (and across all inserts of a bulk build).
    fn clear_layer(&mut self) {
        self.visited.clear();
        self.frontier.clear();
        self.results.clear();
    }

    fn configure_cache(&mut self, limit: usize) {
        let limit = limit.max(1);
        if self.vec_cache.cap().get() != limit {
            self.vec_cache.resize(NonZeroUsize::new(limit).unwrap());
        }
    }

    fn cached_vector(&mut self, vid: VecId) -> Option<CachedVector> {
        match self.vec_cache.get(&vid) {
            Some(Some(vector)) => {
                self.cache_hits = self.cache_hits.saturating_add(1);
                Some(vector.clone())
            }
            _ => {
                self.cache_misses = self.cache_misses.saturating_add(1);
                None
            }
        }
    }

    fn cache_vector(&mut self, vid: VecId, vector: CachedVector, limit: usize) {
        self.configure_cache(limit);
        self.vec_cache.put(vid, Some(vector));
    }
}

/// Persistent worker scratch for a sequence of bounded paged-build chunks.
pub(crate) struct PagedHnswBuildSession {
    workers: Vec<Scratch>,
    cache_entries_per_worker: usize,
    build_quant: Option<BuildQuantParams>,
    build_adj: Option<Arc<BuildAdjStore>>,
    #[cfg(test)]
    force_code_reads: bool,
}

impl PagedHnswBuildSession {
    #[cfg(test)]
    pub(crate) fn use_positioned_codes_for_test(&mut self) {
        self.force_code_reads = true;
    }

    fn ensure_workers(&mut self, workers: usize) {
        while self.workers.len() < workers {
            let mut scratch = Scratch::default();
            scratch.vec_cache_limit = self.cache_entries_per_worker;
            scratch.build_quant = self.build_quant;
            scratch.build_adj.clone_from(&self.build_adj);
            self.workers.push(scratch);
        }
    }

    pub(crate) fn release_link_scratch(&mut self) {
        self.workers.clear();
    }

    pub(crate) fn cache_stats(&self) -> (u64, u64) {
        self.workers.iter().fold((0, 0), |(hits, misses), worker| {
            (
                hits.saturating_add(worker.cache_hits),
                misses.saturating_add(worker.cache_misses),
            )
        })
    }
}

/// Sorted compact reverse lookup with a bounded unsorted insertion tail.
/// Zero VecId marks a removed entry (page zero is the metadata page).
#[derive(Debug)]
struct RowLookup {
    rows: Vec<(RowId, VecId)>,
    sorted: usize,
    charge: ControlledMemoryCharge,
}
impl RowLookup {
    const TAIL: usize = 1024;
    fn reserved_bytes(nodes: usize) -> Option<usize> {
        nodes
            .checked_add(Self::TAIL)?
            .checked_mul(std::mem::size_of::<(RowId, VecId)>())
    }
    fn new(nodes: usize, charge: ControlledMemoryCharge) -> Self {
        Self {
            rows: Vec::with_capacity(nodes + Self::TAIL),
            sorted: 0,
            charge,
        }
    }
    fn seal(&mut self) {
        self.rows.retain(|(_, vid)| *vid != 0);
        self.rows.sort_unstable_by_key(|(row, _)| *row);
        self.sorted = self.rows.len();
    }
    fn range(&self, row: &RowId) -> std::ops::Range<usize> {
        let prefix = &self.rows[..self.sorted];
        let start = prefix.partition_point(|(r, _)| r < row);
        let end = start + prefix[start..].partition_point(|(r, _)| r == row);
        start..end
    }
    fn get(&self, row: &RowId) -> SmallVec<[VecId; 1]> {
        self.rows[self.range(row)]
            .iter()
            .chain(self.rows[self.sorted..].iter())
            .filter_map(|(r, vid)| (r == row && *vid != 0).then_some(*vid))
            .collect()
    }
    #[cfg(test)]
    fn remove(&mut self, row: &RowId) -> SmallVec<[VecId; 1]> {
        self.remove_if(row, |_| true)
    }
    fn remove_if(
        &mut self,
        row: &RowId,
        mut select: impl FnMut(VecId) -> bool,
    ) -> SmallVec<[VecId; 1]> {
        let range = self.range(row);
        let (prefix, tail) = self.rows.split_at_mut(self.sorted);
        prefix[range]
            .iter_mut()
            .chain(tail.iter_mut())
            .filter_map(|(r, vid)| {
                (r == row && *vid != 0 && select(*vid)).then(|| std::mem::replace(vid, 0))
            })
            .collect()
    }
    fn reserve_one(&mut self) -> Result<(), QueryError> {
        if self.rows.len() == self.rows.capacity() {
            let extra = Self::TAIL * std::mem::size_of::<(RowId, VecId)>();
            self.charge.try_grow(extra)?;
            self.rows.reserve_exact(Self::TAIL);
        }
        Ok(())
    }
    fn push(&mut self, row: RowId, vid: VecId) -> Result<(), QueryError> {
        if self.rows.len() - self.sorted >= Self::TAIL || self.rows.len() == self.rows.capacity() {
            self.seal();
        }
        let ordered =
            self.sorted == self.rows.len() && self.rows.last().is_none_or(|(last, _)| *last < row);
        self.reserve_one()?;
        self.rows.push((row, vid));
        if ordered {
            self.sorted += 1;
        }
        Ok(())
    }
}

/// Paged HNSW graph over one `.hvec` file.
#[derive(Debug)]
pub(crate) struct PagedHnsw {
    navigation_quant_enabled: bool,
    valid: AtomicBool,
    pending_commits: parking_lot::Mutex<std::collections::HashSet<u64>>,
    file: Arc<VectorIndexFile>,
    navigation_gate: Arc<parking_lot::RwLock<()>>,
    /// Cache reconstruction freezes DML topology while searches continue on
    /// the durable graph. Keep this separate from the calibration/search gate.
    mutation_gate: Arc<parking_lot::RwLock<()>>,
    reverse_latches: [parking_lot::Mutex<()>; 256],
    next_calibration: AtomicU64,
    metric: Metric,
    m: usize,
    m0: usize,
    ef_construction: usize,
    ml: f64,
    /// Lazy `row_id -> vids` reverse index for DELETE/UPDATE tombstoning.
    /// The first lookup consumes an unchanged navigation scan's compact seed,
    /// or scans node headers when no usable seed exists. Kept sorted with a
    /// bounded insertion tail and maintained on insert/delete. Correctness never depends
    /// on it — a stale entry only costs recall, since the executor re-checks
    /// heap MVCC for every candidate — so a missing entry is always safe.
    row_index: parking_lot::RwLock<Option<RowLookup>>,
    /// Live tombstone count, durably seeded from the vector-file meta page and
    /// maintained alongside each physical tombstone mutation.
    tombstones: AtomicU64,
    /// Tombstones already detached from the navigable graph by
    /// [`Self::repair_tombstones`] in this process. Repair work is gated on
    /// the remainder, so a repaired graph is not rescanned every round.
    repaired_tombstones: AtomicU64,
    /// Soft cap on the number of node vectors a single operation's scratch
    /// keeps resident. A cold build reuses one scratch across all inserts so
    /// each node's (immutable) vector is page-read + decoded **once** for the
    /// whole build instead of on every visit; this cap bounds that cache so a
    /// larger-than-memory build degrades gracefully (misses re-read the page)
    /// instead of pinning every vector in RAM. Derived from a byte budget /
    /// dim; never binds for a per-insert scratch (which holds only the working
    /// set of one insert).
    build_cache_max_entries: usize,
    /// Governed, optional layer-0 SQ8/CSR accelerator. A failed admission
    /// leaves the durable paged path fully functional. Topology mutations
    /// invalidate CSR/results by epoch while retaining immutable codes for
    /// incremental navigation. Vector overwrites drop the entire image.
    query_nav: Arc<parking_lot::RwLock<Option<Arc<QueryNavArena>>>>,
    query_nav_building: Arc<AtomicBool>,
    query_nav_rebuild_requested: Arc<AtomicBool>,
    query_nav_epoch: Arc<AtomicU64>,
    last_build: Arc<parking_lot::RwLock<Option<HnswBuildStats>>>,
}

impl PagedHnsw {
    /// Wrap an open vector index file, reading its parameters from the meta
    /// page.
    pub(crate) fn without_navigation_quantization(mut self) -> Self {
        self.navigation_quant_enabled = false;
        self
    }
    pub(crate) fn new(file: Arc<VectorIndexFile>) -> Self {
        let meta = file.meta();
        let m = (meta.m as usize).max(2);
        let metric = match meta.metric {
            1 => Metric::Cosine,
            2 => Metric::Dot,
            _ => Metric::L2,
        };
        let dim = (meta.dim as usize).max(1);
        // Byte budget for the build-time resident vector cache (default 256
        // MiB), translated to a max entry count for this index's dimension.
        let cache_mb: usize = std::env::var("MYDB_HNSW_BUILD_CACHE_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(256);
        let build_cache_max_entries =
            (cache_mb.saturating_mul(1024 * 1024) / dim.saturating_mul(4)).max(1);
        let query_nav = Arc::new(parking_lot::RwLock::new(None));
        let query_nav_building = Arc::new(AtomicBool::new(false));
        let query_nav_epoch = Arc::new(AtomicU64::new(0));
        let finalized = meta.finalized;
        let this = Self {
            navigation_quant_enabled: true,
            valid: AtomicBool::new(true),
            pending_commits: parking_lot::Mutex::new(std::collections::HashSet::new()),
            file,
            metric,
            m,
            m0: meta.m0 as usize,
            ef_construction: (meta.ef_construction as usize).max(m),
            ml: 1.0 / (m as f64).ln(),
            navigation_gate: Arc::new(parking_lot::RwLock::new(())),
            mutation_gate: Arc::new(parking_lot::RwLock::new(())),
            reverse_latches: std::array::from_fn(|_| parking_lot::Mutex::new(())),
            next_calibration: AtomicU64::new(64),
            row_index: parking_lot::RwLock::new(None),
            tombstones: AtomicU64::new(meta.tombstone_count),
            repaired_tombstones: AtomicU64::new(0),
            build_cache_max_entries,
            query_nav,
            query_nav_building,
            query_nav_rebuild_requested: Arc::new(AtomicBool::new(false)),
            query_nav_epoch,
            last_build: Arc::new(parking_lot::RwLock::new(None)),
        };
        // Reuse finalized pages directly without reconstructing a second graph.
        // Memory-only/disabled mappings retain the optional compact accelerator;
        // CREATE INDEX prepares its admitted query path before returning.
        if finalized && !this.file.prepare_navigation_mapping().unwrap_or(false) {
            this.start_query_navigation_build();
        }
        this
    }

    pub(crate) fn invalidate(&self) {
        self.valid.store(false, Ordering::Release);
        self.file.disable_navigation_mapping();
        self.invalidate_query_navigation();
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }

    pub(crate) fn begin_transaction_delta(&self, txn_id: u64) -> Result<(), QueryError> {
        let mut pending = self.pending_commits.lock();
        self.file.invalidate_build(VEC_DDL_TXN)?;
        pending.insert(txn_id);
        Ok(())
    }

    pub(crate) fn finish_committed_delta(
        &self,
        txn_id: u64,
        commit_scn: u64,
    ) -> Result<(), QueryError> {
        let mut pending = self.pending_commits.lock();
        self.file.advance_coverage(commit_scn);
        if pending.remove(&txn_id) && pending.is_empty() && self.is_valid() {
            self.file.mark_delta_finalized(VEC_DDL_TXN)?;
        }
        Ok(())
    }

    pub(crate) fn node_count(&self) -> u64 {
        self.file.node_count()
    }

    /// Fixed vector dimension of this index (from the meta page).
    #[cfg(test)]
    pub(crate) fn row_lookup_ready(&self) -> bool {
        self.row_index.read().is_some()
    }

    pub(crate) fn dim(&self) -> usize {
        self.file.dim()
    }

    pub(crate) fn supports_direct_bulk_build(&self) -> bool {
        self.file.supports_direct_bulk_build()
    }

    pub(crate) fn memory_build_params(&self) -> (Metric, usize, usize) {
        (self.metric, self.m, self.ef_construction)
    }

    pub(crate) fn set_navigation_quantization(
        &self,
        params: Option<(f32, f32, f32)>,
    ) -> Result<(), QueryError> {
        self.invalidate_query_navigation();
        self.file.set_navigation_quantization(
            (self.metric != Metric::Dot
                && (self.navigation_quant_enabled && paged_quant_enabled()))
            .then_some(params)
            .flatten(),
        )
    }

    fn ensure_navigation_quantization(&self, vector: &[f32]) -> Result<(), QueryError> {
        if self.metric == Metric::Dot || !(self.navigation_quant_enabled && paged_quant_enabled()) {
            return Ok(());
        }
        let count = self.file.node_count();
        // Sample the first 64 rows, then revisit at geometric growth points.
        // Two sequential page passes per doubling are amortized O(N * dim),
        // including when the input distribution keeps expanding.
        if count >= self.next_calibration.load(Ordering::Acquire) {
            let _gate = self.navigation_gate.write();
            let count = self.file.node_count();
            if count < self.next_calibration.load(Ordering::Acquire) {
                return Ok(());
            }
            self.next_calibration.store(
                count
                    .saturating_add(1)
                    .checked_next_power_of_two()
                    .unwrap_or(u64::MAX),
                Ordering::Release,
            );
            if self.file.navigation_quantization().is_none()
                || self.file.meta().nav_exact_nodes != 0
            {
                self.invalidate_query_navigation();
                self.file.recalibrate_navigation(vector)?;
            }
        }
        Ok(())
    }

    fn configure_persisted_query(&self, query: &[f32], scratch: &mut Scratch) {
        let Some((mid, inv_scale, _)) = self.file.navigation_quantization() else {
            scratch.qcodes.clear();
            scratch.qnorm = 0;
            return;
        };
        let src = quant_src(query, self.metric);
        if src
            .iter()
            .any(|&x| !x.is_finite() || ((x - mid) * inv_scale).abs() > 127.5)
        {
            scratch.qcodes.clear();
            scratch.qnorm = 0;
            return;
        }
        scratch.qcodes.clear();
        scratch.qcodes.resize(self.dim(), 0);
        quantize_into(&src, mid, inv_scale, &mut scratch.qcodes);
        scratch.qnorm = dot_i8(&scratch.qcodes, &scratch.qcodes);
    }

    fn query_navigation_limit_bytes() -> usize {
        std::env::var("MYDB_HNSW_QUERY_CACHE_MB")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .map(|mb| mb.saturating_mul(1024 * 1024))
            .unwrap_or_else(|| {
                crate::engine::executor::mem_budget::memory_target_bytes()
                    .saturating_div(4)
                    .min(1024 * 1024 * 1024)
            })
    }

    fn build_query_navigation_for(
        file: &VectorIndexFile,
        m0: usize,
        epoch: u64,
    ) -> Result<Option<QueryNavArena>, QueryError> {
        let meta = file.meta();
        let Some((_, _, dist_mul)) = file.navigation_quantization() else {
            return Ok(None);
        };
        if meta.node_count == 0 || meta.nav_exact_nodes != 0 {
            return Ok(None);
        }
        let nodes = usize::try_from(meta.node_count).map_err(|_| {
            crate::storage::error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "HNSW navigation accelerator node count exceeds address space",
            )
        })?;
        let dim = file.dim();
        let pages = usize::try_from(meta.node_next_page_id).map_err(|_| {
            crate::storage::error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "HNSW page directory exceeds address space",
            )
        })?;
        let allocation = query_navigation_allocation(nodes, dim, m0, pages).ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "HNSW navigation accelerator size overflows address space",
            )
        })?;
        // One temporary offset vector is retained while on-disk NodeIds are
        // translated in place to dense ordinals. It is charged for the build
        // peak and released before publication.
        let Some((spill_codes, mut charge)) =
            allocation.try_reserve(Self::query_navigation_limit_bytes(), |bytes| {
                ControlledMemoryCharge::try_new_optional(
                    ControlledMemoryCategory::DerivedCache,
                    bytes,
                )
            })
        else {
            return Ok(None);
        };
        let code_tmp = if spill_codes {
            Some(
                crate::engine::executor::select::grace::SpillTmpDir::create_kind(
                    file.spill_data_dir(),
                    "vector-nav",
                )?,
            )
        } else {
            None
        };
        let mut code_writer = code_tmp
            .as_ref()
            .map(|tmp| {
                crate::engine::executor::spill_quota::TrackedSpillWriter::create_new(
                    &tmp.path().join("codes"),
                    tmp.charger(),
                    64 * 1024,
                )
            })
            .transpose()?;

        let edge_tmp = if spill_codes {
            Some(
                crate::engine::executor::select::grace::SpillTmpDir::create_kind(
                    file.spill_data_dir(),
                    "vector-edges",
                )?,
            )
        } else {
            None
        };
        let mut edge_writer = edge_tmp
            .as_ref()
            .map(|tmp| {
                crate::engine::executor::spill_quota::TrackedSpillWriter::create_new(
                    &tmp.path().join("edges"),
                    tmp.charger(),
                    64 * 1024,
                )
            })
            .transpose()?;
        let mut row_seed = RowLookup::new(nodes, charge.split(allocation.row_seed_bytes));
        let mut edge_count = 0usize;
        let mut vids = Vec::with_capacity(nodes);
        let mut page_starts = vec![u32::MAX; pages];
        let mut codes = Vec::with_capacity(if spill_codes {
            0
        } else {
            allocation.codes_bytes
        });
        let mut norms = Vec::with_capacity(nodes);
        let mut raw_offsets = Vec::with_capacity(nodes + 1);
        let mut neighbors = Vec::with_capacity(if spill_codes {
            0
        } else {
            allocation.edge_capacity
        });
        raw_offsets.push(0u32);
        file.for_each_navigation_row(
            |vid, row_id, row_deleted, _exact, row_codes, norm, row_neighbors| {
                let page = (vid >> 32) as usize;
                if vids.len() >= nodes || page >= pages {
                    return Err(crate::storage::error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "HNSW changed while building navigation cache",
                    ));
                }
                let slot = (vid & 0xffff_ffff) as usize;
                if page_starts[page] == u32::MAX && vids.len() >= slot {
                    page_starts[page] = u32::try_from(vids.len() - slot).map_err(|_| {
                        crate::storage::error(
                            crate::error::catalog::ER_OUTOFMEMORY,
                            "HNSW page directory ordinal overflow",
                        )
                    })?;
                }
                vids.push(vid);
                if !row_deleted {
                    row_seed.rows.push((row_id, vid));
                }
                if let Some(writer) = code_writer.as_mut() {
                    // SAFETY: immutable i8/u8 byte representation has identical layout.
                    let bytes = unsafe {
                        std::slice::from_raw_parts(row_codes.as_ptr().cast::<u8>(), row_codes.len())
                    };
                    writer.write_all(bytes).map_err(|err| {
                        crate::engine::executor::spill_quota::query_error_from_io(&err)
                            .unwrap_or_else(|| {
                                crate::storage::error(
                                    crate::error::catalog::ER_GET_ERRNO,
                                    format!("HNSW navigation spill: {err}"),
                                )
                            })
                    })?;
                } else {
                    codes.extend_from_slice(row_codes);
                }
                norms.push(norm);
                if let Some(writer) = edge_writer.as_mut() {
                    // Native-endian scratch image, read only by this process.
                    let bytes = unsafe {
                        std::slice::from_raw_parts(
                            row_neighbors.as_ptr().cast::<u8>(),
                            row_neighbors.len() * 4,
                        )
                    };
                    writer.write_all(bytes).map_err(|err| {
                        crate::engine::executor::spill_quota::query_error_from_io(&err)
                            .unwrap_or_else(|| {
                                crate::storage::error(
                                    crate::error::catalog::ER_GET_ERRNO,
                                    format!("HNSW edge spill: {err}"),
                                )
                            })
                    })?;
                } else {
                    neighbors.extend_from_slice(row_neighbors);
                }
                edge_count += row_neighbors.len();
                raw_offsets.push(u32::try_from(edge_count).map_err(|_| {
                    crate::storage::error(
                        crate::error::catalog::ER_OUTOFMEMORY,
                        "HNSW navigation accelerator has more than u32::MAX edges",
                    )
                })?);
                Ok(())
            },
        )?;
        if vids.len() != nodes
            || (!spill_codes && codes.len() != allocation.codes_bytes)
            || raw_offsets.len() != nodes + 1
        {
            return Ok(None);
        }

        let mut offsets = Vec::with_capacity(nodes + 1);
        offsets.push(0u32);
        let mut write = 0usize;
        for ordinal in 0..if spill_codes { 0 } else { nodes } {
            let begin = raw_offsets[ordinal] as usize;
            let end = raw_offsets[ordinal + 1] as usize;
            for read in begin..end {
                let neighbor_vid = file.vid_from_node_id(neighbors[read]);
                if let Ok(neighbor_ordinal) = vids.binary_search(&neighbor_vid) {
                    neighbors[write] = neighbor_ordinal as u32;
                    write += 1;
                }
            }
            offsets.push(write as u32);
        }
        neighbors.truncate(write);
        let neighbors = if let Some(writer) = edge_writer {
            if writer.finish()? != (edge_count * 4) as u64 {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "HNSW edge spill length mismatch",
                ));
            }
            offsets = raw_offsets;
            let tmp = edge_tmp.expect("edge spill owner");
            let input = File::open(tmp.path().join("edges")).map_err(|err| {
                crate::storage::error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("HNSW edge spill open: {err}"),
                )
            })?;
            charge.shrink(64 * 1024);
            let mapping = super::spill_rows::MappedSpillBytes::open(input, tmp)?;
            mapping.advise_random();
            QueryNavEdges::Spilled(mapping)
        } else {
            drop(raw_offsets);
            QueryNavEdges::Resident(neighbors)
        };
        charge.shrink(allocation.offsets_bytes);
        let codes = if let Some(writer) = code_writer {
            if writer.finish()? != allocation.codes_bytes as u64 {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "HNSW navigation spill row count mismatch",
                ));
            }
            let tmp = code_tmp.expect("spill owner");
            let input = File::open(tmp.path().join("codes")).map_err(|err| {
                crate::storage::error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("HNSW navigation spill open: {err}"),
                )
            })?;
            charge.shrink(64 * 1024);
            let mapping = super::spill_rows::MappedSpillBytes::open(input, tmp)?;
            mapping.advise_random();
            QueryNavCodes::Spilled(mapping)
        } else {
            QueryNavCodes::Resident(codes)
        };
        row_seed.seal();
        Ok(Some(QueryNavArena {
            epoch,
            dim,
            dist_mul,
            vids,
            page_starts,
            codes,
            norms,
            offsets,
            neighbors,
            query_cache: QueryNavCache::new(),
            row_seed: parking_lot::Mutex::new(Some(row_seed)),
            _charge: charge,
        }))
    }

    /// Synchronously refresh after CREATE/OPTIMIZE so the first user query
    /// starts on the compact path. Admission failure is expected for an
    /// optional cache and leaves the paged implementation as the oracle.
    pub(crate) fn refresh_query_navigation(&self) -> Result<(), QueryError> {
        let _gate = self.navigation_gate.write();
        if self.metric != Metric::Dot
            && (self.navigation_quant_enabled && paged_quant_enabled())
            && (self.file.navigation_quantization().is_none()
                || self.file.meta().nav_exact_nodes != 0)
        {
            self.invalidate_query_navigation();
            self.file.recalibrate_navigation(&[])?;
        }
        if self.file.prepare_navigation_mapping()? {
            self.query_nav.write().take();
            return Ok(());
        }
        let epoch = self.query_nav_epoch.load(Ordering::Acquire);
        let arena = Self::build_query_navigation_for(&self.file, self.m0, epoch)?;
        let mut published = self.query_nav.write();
        if self.query_nav_epoch.load(Ordering::Acquire) == epoch {
            *published = arena.map(Arc::new);
        }
        Ok(())
    }

    fn start_query_navigation_build(&self) {
        if self
            .query_nav_building
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let requested = Arc::clone(&self.query_nav_rebuild_requested);
        let file = Arc::clone(&self.file);
        let navigation_gate = Arc::clone(&self.navigation_gate);
        let mutation_gate = Arc::clone(&self.mutation_gate);
        let query_nav = Arc::clone(&self.query_nav);
        let building = Arc::clone(&self.query_nav_building);
        let epoch_source = Arc::clone(&self.query_nav_epoch);
        let m0 = self.m0;
        let budget = Arc::new(crate::engine::executor::mem_budget::MemBudget::from_env());
        std::thread::spawn(move || {
            crate::engine::executor::mem_budget::with_query_budget(budget, || {
                let _gate = navigation_gate.read();
                let _mutation = mutation_gate.write();
                if file.prepare_navigation_mapping().unwrap_or(false) {
                    requested.store(false, Ordering::Release);
                    building.store(false, Ordering::Release);
                    return;
                }
                let epoch = epoch_source.load(Ordering::Acquire);
                if let Ok(Some(arena)) = Self::build_query_navigation_for(&file, m0, epoch) {
                    let mut published = query_nav.write();
                    if epoch_source.load(Ordering::Acquire) == epoch {
                        *published = Some(Arc::new(arena));
                    }
                }
                requested.store(false, Ordering::Release);
                building.store(false, Ordering::Release);
            })
        });
    }

    fn invalidate_query_navigation(&self) {
        let mut arena = self.query_nav.write();
        self.query_nav_epoch.fetch_add(1, Ordering::AcqRel);
        if arena.take().is_some() {
            self.query_nav_rebuild_requested
                .store(true, Ordering::Release);
        }
    }

    fn invalidate_query_topology(&self) {
        let _arena = self.query_nav.write();
        self.query_nav_epoch.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn record_build_stats(&self, stats: HnswBuildStats) {
        *self.last_build.write() = Some(stats);
    }

    pub(crate) fn last_build_stats(&self) -> Option<HnswBuildStats> {
        self.last_build.read().clone()
    }

    /// Transcribe a fully linked dense in-memory graph through the storage
    /// layer's sequential direct writer. Only one vector/node is copied at a
    /// time; there is no second table-sized `rows` or exported-links batch.
    pub(crate) fn materialize_memory_graph(&self, graph: &HnswIndex) -> Result<(), QueryError> {
        let timing = std::env::var("MYDB_HNSW_BUILD_TIMING").is_ok();
        let started = std::time::Instant::now();
        self.file.bulk_build_direct(
            graph.build_node_count(),
            graph.build_entry(),
            |id| graph.build_node_top_layer(id),
            |id, vector, neighbors| Ok(graph.copy_build_node_into(id, vector, neighbors)),
        )?;
        if timing {
            eprintln!(
                "[paged] direct topology materialize: {:?} rows={}",
                started.elapsed(),
                graph.build_node_count()
            );
        }
        Ok(())
    }

    /// Materialize a topology-only compact graph while the caller streams the
    /// exact heap vector for each dense id. Only a bounded channel plus the
    /// writer's reusable vector buffer is resident.
    pub(crate) fn materialize_compact_graph<F>(
        &self,
        graph: &HnswIndex,
        mut fetch_exact: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(usize, RowId, &mut Vec<f32>) -> Result<(), QueryError>,
    {
        let timing = std::env::var("MYDB_HNSW_BUILD_TIMING").is_ok();
        let started = std::time::Instant::now();
        self.file.bulk_build_direct(
            graph.build_node_count(),
            graph.build_entry(),
            |id| graph.build_node_top_layer(id),
            |id, vector, neighbors| {
                let row_id = graph.copy_build_links_into(id, neighbors);
                fetch_exact(id, row_id, vector)?;
                Ok(row_id)
            },
        )?;
        if timing {
            eprintln!(
                "[paged] compact direct materialize: {:?} rows={}",
                started.elapsed(),
                graph.build_node_count()
            );
        }
        Ok(())
    }

    /// Tombstoned node count (best-effort, in-memory). Builds the reverse
    /// index on first call if needed.
    pub(crate) fn stale_count(&self) -> u32 {
        let t = self.tombstones.load(Ordering::Acquire);
        if t != u64::MAX {
            return t.min(u32::MAX as u64) as u32;
        }
        // Not yet counted — build the reverse index (which also seeds the
        // tombstone count) and re-read.
        if self.ensure_row_index().is_ok() {
            self.tombstones.load(Ordering::Acquire).min(u32::MAX as u64) as u32
        } else {
            0
        }
    }

    /// Live (non-tombstoned) node count.
    #[allow(dead_code)] // used by VectorGraph::len (test-facing stats).
    pub(crate) fn live_count(&self) -> usize {
        (self.node_count() as usize).saturating_sub(self.stale_count() as usize)
    }

    /// Ensure the `row_id -> vids` reverse index (and tombstone count) is
    /// materialised. Idempotent; a single `for_each_node` pass.
    fn ensure_row_index(&self) -> Result<(), QueryError> {
        let mut guard = self.row_index.write();
        if guard.is_some() {
            return Ok(());
        }
        // Only an unchanged graph can seed the complete reverse lookup. A
        // topology mutation before this first lookup requires a fresh scan.
        if let Some(arena) = self.query_nav.read().as_ref() {
            if arena.epoch == self.query_nav_epoch.load(Ordering::Acquire)
                && arena.vids.len() == self.file.node_count() as usize
            {
                if let Some(seed) = arena.row_seed.lock().take() {
                    *guard = Some(seed);
                    return Ok(());
                }
            }
        }
        let nodes = self.file.node_count() as usize;
        let bytes = RowLookup::reserved_bytes(nodes).ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "HNSW reverse lookup size overflow",
            )
        })?;
        let charge =
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::DerivedCache, bytes)?;
        let mut map = RowLookup::new(nodes, charge);
        let mut tomb: u64 = 0;
        let mut scan_error = None;
        self.file.for_each_node(|vid, row_id, deleted| {
            if scan_error.is_some() {
                return;
            }
            if deleted {
                tomb += 1;
            } else if let Err(error) = map.reserve_one() {
                scan_error = Some(error);
            } else {
                map.rows.push((row_id, vid));
            }
        })?;
        if let Some(error) = scan_error {
            return Err(error);
        }
        map.seal();
        self.tombstones.store(tomb, Ordering::Release);
        *guard = Some(map);
        Ok(())
    }

    /// Record a freshly-inserted `(row_id -> vid)` mapping if the reverse
    /// index is already built (no-op otherwise — a later build picks it up
    /// from the file).
    fn note_inserted_vid(&self, row_id: RowId, vid: VecId) -> Result<(), QueryError> {
        if let Some(map) = self.row_index.write().as_mut() {
            map.push(row_id, vid)?;
        }
        Ok(())
    }

    /// Tombstone every live node keyed by `row_id`. Returns the number of
    /// nodes newly marked deleted. Best-effort recall maintenance: a failure
    /// to find/mark a node never affects correctness (MVCC recheck filters
    /// invisible rows anyway).
    pub(crate) fn mark_deleted_by_row_id(&self, row_id: RowId) -> Result<u32, QueryError> {
        self.mark_deleted_with(
            row_id,
            |_| true,
            |vid| self.file.set_tombstone(VEC_DDL_TXN, vid, true),
        )
    }

    /// Live node ids for `row_id`, used as version handles that a deferred
    /// purge later retires exactly (newer versions of the row are untouched).
    pub(crate) fn live_versions(&self, row_id: RowId) -> Result<SmallVec<[VecId; 1]>, QueryError> {
        self.ensure_row_index()?;
        Ok(self
            .row_index
            .read()
            .as_ref()
            .map(|map| map.get(&row_id))
            .unwrap_or_default())
    }

    /// Tombstone the listed live versions of `row_id`.
    pub(crate) fn retire_versions(&self, row_id: RowId, vids: &[VecId]) -> Result<u32, QueryError> {
        self.mark_deleted_with(
            row_id,
            |vid| vids.contains(&vid),
            |vid| self.file.set_tombstone(VEC_DDL_TXN, vid, true),
        )
    }

    /// Adjacency entries (any layer) that still point at a tombstone.
    #[cfg(test)]
    pub(crate) fn edges_into_tombstones(&self) -> u64 {
        let mut deleted = std::collections::HashSet::new();
        let mut live = Vec::new();
        self.file
            .for_each_node(|vid, _, is_deleted| {
                if is_deleted {
                    deleted.insert(vid);
                } else {
                    live.push(vid);
                }
            })
            .unwrap();
        let mut edges = 0;
        let mut neighbors = Vec::new();
        for vid in live {
            let mut level = 0;
            while self
                .file
                .read_neighbors_into(vid, level, &mut neighbors)
                .unwrap()
            {
                edges += neighbors.iter().filter(|nb| deleted.contains(nb)).count() as u64;
                level += 1;
            }
        }
        edges
    }

    /// Tombstones not yet detached by [`Self::repair_tombstones`].
    pub(crate) fn unrepaired_tombstones(&self) -> u64 {
        self.tombstones
            .load(Ordering::Acquire)
            .saturating_sub(self.repaired_tombstones.load(Ordering::Acquire))
    }

    /// Detach tombstoned nodes from the navigable graph in place (the paged
    /// counterpart of slab compaction, in the style of pgvector's HNSW
    /// vacuum). Every live adjacency list that references a tombstone is
    /// re-selected from its live neighbours plus the live neighbours of the
    /// removed ones; a list left nearly empty is refilled by a graph search.
    /// A tombstoned entry point is replaced by the highest live node. Each
    /// rewrite is an ordinary WAL-logged page update under the target's
    /// reverse-edge latch, so concurrent DML and searches proceed. Records
    /// stay allocated: `OPTIMIZE TABLE` reclaims file space.
    pub(crate) fn repair_tombstones(
        &self,
        should_stop: impl Fn() -> bool,
    ) -> Result<u64, QueryError> {
        let mut deleted = std::collections::HashSet::new();
        let mut live = Vec::new();
        self.file.for_each_node(|vid, _row, is_deleted| {
            if is_deleted {
                deleted.insert(vid);
            } else {
                live.push(vid);
            }
        })?;
        if deleted.is_empty() {
            return Ok(0);
        }
        let mut scratch = Scratch::default();
        scratch.vec_cache_limit = self.m0.saturating_mul(8).max(64);
        let mut neighbors = Vec::new();
        let mut detached = Vec::new();
        let mut vector = Vec::with_capacity(self.dim());
        let mut best_entry: Option<(VecId, usize)> = None;
        for &vid in &live {
            if should_stop() {
                break;
            }
            // Gates are taken per node so DML and navigation-cache rebuilds
            // (which wait for the mutation gate exclusively) interleave with
            // a long repair instead of queueing behind it.
            let _gate = self.navigation_gate.read();
            let _mutation = self.mutation_gate.read();
            let mut level = 0;
            loop {
                if !self.file.read_neighbors_into(vid, level, &mut neighbors)? {
                    break;
                }
                if best_entry.is_none_or(|(_, top)| level > top) {
                    best_entry = Some((vid, level));
                }
                if neighbors.iter().any(|nb| deleted.contains(nb)) {
                    self.repair_adjacency(
                        vid,
                        level,
                        &deleted,
                        &mut neighbors,
                        &mut detached,
                        &mut vector,
                        &mut scratch,
                    )?;
                }
                level += 1;
            }
        }
        {
            let _gate = self.navigation_gate.read();
            let _mutation = self.mutation_gate.read();
            if let (Some((entry, _)), Some((vid, level))) =
                (self.file.entry_with_level(), best_entry)
            {
                if deleted.contains(&entry) {
                    self.file.set_entry(VEC_DDL_TXN, vid, level as u32)?;
                }
            }
            self.invalidate_query_topology();
        }
        self.start_query_navigation_build();
        if should_stop() {
            return Ok(0);
        }
        let repaired = deleted.len() as u64;
        self.repaired_tombstones.store(repaired, Ordering::Release);
        Ok(repaired)
    }

    #[allow(clippy::too_many_arguments)]
    fn repair_adjacency(
        &self,
        vid: VecId,
        level: usize,
        deleted: &std::collections::HashSet<VecId>,
        neighbors: &mut Vec<VecId>,
        detached: &mut Vec<VecId>,
        vector: &mut Vec<f32>,
        scratch: &mut Scratch,
    ) -> Result<(), QueryError> {
        let cap = if level == 0 { self.m0 } else { self.m };
        // Same latch as reverse-edge wiring into `vid`: no concurrent insert
        // can add an edge between our read and the rewrite.
        let _target = self.reverse_latches[((vid >> 16) ^ vid) as usize & 255].lock();
        self.file.read_neighbors_into(vid, level, neighbors)?;
        let mut candidates: Vec<VecId> = Vec::with_capacity(cap * 2);
        for &nb in neighbors.iter() {
            if deleted.contains(&nb) {
                self.file.read_neighbors_into(nb, level, detached)?;
                candidates.extend(
                    detached
                        .iter()
                        .copied()
                        .filter(|&two_hop| two_hop != vid && !deleted.contains(&two_hop)),
                );
            } else {
                candidates.push(nb);
            }
        }
        if candidates.len() < cap / 2 {
            // Every route out of this node was deleted: re-link it by search.
            if self.file.read_vector_meta_into(vid, vector)?.is_some() {
                if let Some((entry, top)) = self.file.entry_with_level() {
                    let mut cur = entry;
                    for upper in ((level + 1)..=top as usize).rev() {
                        cur = self.greedy_descend(vector, cur, upper, scratch)?;
                    }
                    let found =
                        self.search_layer(vector, &[cur], self.ef_construction, level, scratch)?;
                    candidates.extend(
                        found
                            .iter()
                            .map(|s| s.id)
                            .filter(|&nb| nb != vid && !deleted.contains(&nb)),
                    );
                }
            }
        }
        candidates.sort_unstable();
        candidates.dedup();
        let pruned = self.prune_select(vid, &candidates, cap, scratch)?;
        self.file.set_neighbors(VEC_DDL_TXN, vid, level, &pruned)
    }

    fn mark_deleted_with(
        &self,
        row_id: RowId,
        select: impl Fn(VecId) -> bool,
        mut tombstone: impl FnMut(VecId) -> Result<bool, QueryError>,
    ) -> Result<u32, QueryError> {
        let _gate = self.navigation_gate.read();
        let _mutation = self.mutation_gate.read();
        self.ensure_row_index()?;
        self.invalidate_query_topology();
        // Keep the reverse-index write lock until all old vids are physically
        // tombstoned. A concurrent INSERT's `note_inserted_vid` then publishes
        // its new vid only after this delete finishes, so it cannot be swept
        // into the removed vector and accidentally tombstoned as an old node.
        let mut guard = self.row_index.write();
        let vids: SmallVec<[VecId; 1]> = match guard.as_mut() {
            Some(map) => map
                .get(&row_id)
                .into_iter()
                .filter(|&vid| select(vid))
                .collect(),
            None => SmallVec::new(),
        };
        let mut marked = 0u32;
        for &vid in &vids {
            // `set_tombstone` returns the previous state; only count nodes
            // that were live.
            let previous = tombstone(vid).map_err(|error| {
                // A meta-page flush may fail after the node mutation succeeded.
                self.tombstones
                    .store(self.file.meta().tombstone_count, Ordering::Release);
                error
            })?;
            if !previous {
                marked += 1;
                self.tombstones.fetch_add(1, Ordering::AcqRel);
            }
        }
        if let Some(map) = guard.as_mut() {
            map.remove_if(&row_id, |vid| vids.contains(&vid));
        }
        drop(guard);
        Ok(marked)
    }

    pub(crate) fn has_live_row_id(&self, row_id: RowId) -> Result<bool, QueryError> {
        self.ensure_row_index()?;
        Ok(self
            .row_index
            .read()
            .as_ref()
            .is_some_and(|map| !map.get(&row_id).is_empty()))
    }

    /// Drift-adaptive in-place vector UPDATE (paged mirror of
    /// [`super::hnsw::HnswIndex::try_update_in_place`]). For every live node
    /// keyed by `row_id`, if the new vector's move is small relative to the
    /// node's nearest-neighbour distance (`drift ≤ ratio · d_nn`), overwrite the
    /// node's f32 vector **in place** — same vid, same edges, same page slot, one
    /// WAL'd page write — and report `true`. A move too large to keep the edges
    /// representative leaves every node untouched and reports `false`, so the
    /// caller re-links via the tombstone + insert path.
    ///
    /// Skips the dominant UPDATE cost — a full HNSW re-link, whose navigation is
    /// f32 page-decode bound (~66% of re-link CPU at 768-d) — for the common
    /// case of an incremental embedding refresh. Recall is preserved: the edges
    /// still point at genuine near neighbours of the barely-moved vector, and the
    /// executor re-ranks returned candidates by the exact, now-updated vector.
    /// L2 only (Dot/Cosine fall back to the exact re-link path).
    ///
    /// Crash-safe and reader-safe without a structure lock: each overwrite is one
    /// WAL'd `rmw_node_page` that swaps the page atomically under the per-page
    /// latch + buffer-pool frame lock, so a concurrent search sees either the old
    /// or the new page, never a torn vector. Best-effort like every paged
    /// maintenance hook — a read/write error degrades to "not updated in place"
    /// (the caller re-links, or the next search re-checks heap MVCC anyway).
    pub(crate) fn try_update_in_place(
        &self,
        row_id: RowId,
        new_vector: &[f32],
        ratio: f32,
    ) -> Result<bool, QueryError> {
        let _gate = self.navigation_gate.read();
        let _mutation = self.mutation_gate.read();
        if self.metric != Metric::L2 || new_vector.len() != self.dim() {
            return Ok(false);
        }
        self.ensure_row_index()?;
        let vids = {
            let guard = self.row_index.read();
            let ids = guard
                .as_ref()
                .map(|map| map.get(&row_id))
                .unwrap_or_default();
            if ids.is_empty() {
                return Ok(false);
            }
            ids
        };
        // `ordering_distance` for L2 is *squared* L2, so `drift ≤ ratio·d_nn`
        // becomes `drift_sq ≤ ratio²·d_nn_sq`.
        let ratio_sq = (ratio as f64).powi(2);
        let mut old = Vec::with_capacity(self.dim());
        let mut nbrs: Vec<VecId> = Vec::new();
        let mut nbvec = Vec::with_capacity(self.dim());
        // First pass: confirm *every* node keyed by this row can move in place.
        // If any can't, fall back wholesale (the re-link path re-inserts a fresh
        // node and tombstones these), so we never half-update a multi-node row.
        for &vid in &vids {
            if !self.file.read_vector_into(vid, &mut old)? || old.len() != new_vector.len() {
                return Ok(false);
            }
            let drift_sq = ordering_distance(&old, new_vector, self.metric);
            // Nearest *live* layer-0 neighbour distance, from the current vector.
            let mut d_nn_sq = f64::INFINITY;
            self.file.read_neighbors_into(vid, 0, &mut nbrs)?;
            for &nb in &nbrs {
                match self.file.read_vector_meta_into(nb, &mut nbvec)? {
                    Some((_, deleted)) if !deleted => {
                        let d = ordering_distance(&old, &nbvec, self.metric);
                        // Any closer neighbor is enough to reject a large
                        // drift. Continuing the full neighbor scan cannot
                        // change that decision, only add random page reads.
                        if drift_sq > ratio_sq * d {
                            return Ok(false);
                        }
                        if d < d_nn_sq {
                            d_nn_sq = d;
                        }
                    }
                    _ => {}
                }
            }
            // No usable local scale (isolated node) or move too large → re-link.
            if !super::hnsw_core::permits_in_place_update(drift_sq, d_nn_sq, ratio_sq) {
                return Ok(false);
            }
        }
        // Second pass: commit the in-place overwrites (all nodes pass the gate).
        self.invalidate_query_navigation();
        let mut updated = false;
        for &vid in &vids {
            if self.file.write_vector(VEC_DDL_TXN, vid, new_vector)? {
                updated = true;
            }
        }
        Ok(updated)
    }

    /// Deterministic level for the `ordinal`-th inserted node, matching the
    /// in-memory `random_level` SplitMix64 draw, clamped to [`VEC_MAX_LAYER`].
    fn random_level(&self, ordinal: u64) -> usize {
        super::hnsw_core::random_level(ordinal, self.ml, super::hnsw_paged::VEC_MAX_LAYER)
    }

    /// Fetch `vid`'s vector as an owned copy, caching it in `scratch` when under
    /// the resident cap. `None` for a missing node (dangling edge). The owned
    /// return is only materialised on the slow path (cache miss); the hot paths
    /// below take the cached borrow directly and never reach here.
    fn read_and_maybe_cache(
        &self,
        vid: VecId,
        scratch: &mut Scratch,
    ) -> Result<Option<CachedVector>, QueryError> {
        let cache_limit = if scratch.vec_cache_limit == 0 {
            self.build_cache_max_entries
        } else {
            scratch.vec_cache_limit
        };
        scratch.configure_cache(cache_limit);
        if let Some(vector) = scratch.cached_vector(vid) {
            return Ok(Some(vector));
        }
        // Recycle the evicted payload before allocating its replacement. Small
        // per-node allocations fragmented worker arenas across spill chunks;
        // live-byte cache limits alone could not bound their physical footprint.
        let retired = if scratch.vec_cache.len() >= cache_limit {
            // Leave the LRU node in place so put() reuses that allocation too.
            let key = scratch.vec_cache.peek_lru().map(|(key, _)| *key);
            key.and_then(|key| scratch.vec_cache.peek_mut(&key).and_then(Option::take))
        } else {
            None
        };
        let cached = if scratch.incremental_quant.is_some() {
            let mut codes = match retired {
                Some(CachedVector::Sq8 { codes, .. }) => codes,
                _ => Arc::new(Vec::new()),
            };
            let output = Arc::make_mut(&mut codes);
            let norm = if let Some((arena, ordinal)) = scratch
                .navigation
                .as_deref()
                .and_then(|arena| arena.ordinal(vid).map(|ordinal| (arena, ordinal)))
            {
                output.clear();
                output.extend_from_slice(arena.codes_at(ordinal));
                Some(arena.norms[ordinal])
            } else {
                self.file.read_navigation_codes_into(vid, output)?
            };
            if let Some(norm) = norm {
                CachedVector::Sq8 { codes, norm }
            } else {
                let mut exact = Vec::with_capacity(self.dim());
                if !self.file.read_vector_into(vid, &mut exact)? {
                    return Ok(None);
                }
                CachedVector::Exact(Arc::new(exact))
            }
        } else if let Some(params) = scratch.build_quant {
            let mut codes = match retired {
                Some(CachedVector::Sq8 { codes, .. }) => codes,
                _ => Arc::new(Vec::new()),
            };
            let output = Arc::make_mut(&mut codes);
            let stored_norm = match scratch.build_adj.as_deref() {
                Some(store) => {
                    store.read_codes_with_mapping(vid, output, scratch.build_codes.as_deref())?
                }
                None => None,
            };
            if stored_norm.is_none() {
                let mut decoded = std::mem::take(&mut scratch.decode_buf);
                if !self.file.read_vector_into(vid, &mut decoded)? {
                    scratch.decode_buf = decoded;
                    return Ok(None);
                }
                if self.metric == Metric::Cosine {
                    super::distance::normalize_in_place(&mut decoded);
                }
                output.resize(self.dim(), 0);
                quantize_into(&decoded, params.mid, params.inv_scale, output);
                scratch.decode_buf = decoded;
            }
            let norm = stored_norm.unwrap_or_else(|| dot_i8(output, output));
            CachedVector::Sq8 { codes, norm }
        } else {
            let mut vector = match retired {
                Some(CachedVector::Exact(vector)) => vector,
                _ => Arc::new(Vec::new()),
            };
            if !self
                .file
                .read_vector_into(vid, Arc::make_mut(&mut vector))?
            {
                return Ok(None);
            }
            CachedVector::Exact(vector)
        };
        scratch.cache_vector(vid, cached.clone(), cache_limit);
        Ok(Some(cached))
    }

    /// Distance from `query` to `vid`, scoring against the cached vector.
    /// `INFINITY` for a missing node so it never wins selection.
    fn dist_q(&self, query: &[f32], vid: VecId, scratch: &mut Scratch) -> Result<f64, QueryError> {
        if let Some(arena) = scratch.navigation.as_deref() {
            if let Some(ordinal) = arena.ordinal(vid) {
                return Ok(arena.distance(&scratch.qcodes, scratch.qnorm, ordinal));
            }
        }
        // Cold-build fast path: the shared arena holds every node's vector.
        if let Some(arena) = scratch.arena.as_deref() {
            if arena.sq8() {
                // SQ8 navigation: score the pre-quantized query codes against
                // the target's int8 codes (one integer `sdot`). A vid outside
                // the arena's ordinal map is a node inserted *after* the arena
                // was built (query path) — fall through to a page decode, whose
                // exact f32 L² is on the same real-units scale as the SQ8 score
                // and so mixes correctly. (During a cold build every node is in
                // the build arena, so the fall-through never fires there.)
                if let Some(o) = arena.ord_of(vid) {
                    return Ok(arena.ssd_from_query(&scratch.qcodes, scratch.qnorm, o));
                }
                // sq8 + uncovered vid: fall through to the page decode below.
            } else if let Some(vector) = arena.get(vid) {
                return Ok(ordering_distance(query, vector, self.metric));
            }
            // Older chunks are outside this arena, not missing graph nodes.
            // Exact navigation must fetch them through the bounded page cache.
        }
        // A build cache hit can score the borrowed row without incrementing
        // and decrementing an Arc for every distance (billions in big graphs).
        if let Some(params) = scratch.build_quant {
            if let (Some(store), Some(mapping)) =
                (scratch.build_adj.as_deref(), scratch.build_codes.as_deref())
            {
                if let Some((codes, norm)) = store.mapped_codes(vid, mapping) {
                    return Ok(
                        (scratch.qnorm + norm - 2 * dot_i8(&scratch.qcodes, codes)) as f64
                            * (params.dist_mul as f64),
                    );
                }
            }
            if let Some(Some(CachedVector::Sq8 { codes, norm })) = scratch.vec_cache.get(&vid) {
                scratch.cache_hits = scratch.cache_hits.saturating_add(1);
                return Ok(
                    (scratch.qnorm + *norm - 2 * dot_i8(&scratch.qcodes, codes)) as f64
                        * (params.dist_mul as f64),
                );
            }
        }
        // Query/incremental path: the SQ8 row is persisted in the node page
        // and scored while that page is pinned in the shared data buffer.
        // EXACT_NAV rows deliberately return None and use the f32 fallback.
        // Chunked builds already reserve a persistent SQ8 worker cache.
        // Route old-node scores through it instead of bypassing every hit
        // with another random data-page read. Query/DML keeps its shared-page
        // navigation path and does not allocate this build cache.
        if scratch.build_quant.is_none() && !scratch.qcodes.is_empty() {
            if let Some(distance) =
                self.file
                    .navigation_distance(vid, &scratch.qcodes, scratch.qnorm)?
            {
                return Ok(distance);
            }
        }
        match self.read_and_maybe_cache(vid, scratch)? {
            Some(CachedVector::Exact(v)) => Ok(ordering_distance(query, &v, self.metric)),
            Some(CachedVector::Sq8 { codes, norm }) => {
                let Some(params) = scratch.build_quant.or(scratch.incremental_quant) else {
                    return Ok(f64::INFINITY);
                };
                let ssd = scratch.qnorm + norm - 2 * dot_i8(&scratch.qcodes, &codes);
                Ok(ssd as f64 * (params.dist_mul as f64))
            }
            None => Ok(f64::INFINITY),
        }
    }

    fn dist_q_batch(
        &self,
        query: &[f32],
        vids: &[VecId],
        scratch: &mut Scratch,
    ) -> Result<(), QueryError> {
        let mut order = std::mem::take(&mut scratch.nav_batch_order);
        let mut approx = std::mem::take(&mut scratch.nav_batch_approx);
        if scratch.arena.is_none() && scratch.navigation.is_none() && !scratch.qcodes.is_empty() {
            self.file.navigation_distances(
                vids,
                &scratch.qcodes,
                scratch.qnorm,
                &mut order,
                &mut approx,
            )?;
        } else {
            approx.clear();
            approx.resize(vids.len(), None);
        }
        let mut distances = std::mem::take(&mut scratch.nav_batch_distances);
        distances.clear();
        distances.reserve(vids.len());
        for (index, &vid) in vids.iter().enumerate() {
            distances.push(match approx[index] {
                Some(distance) => distance,
                None => self.dist_q(query, vid, scratch)?,
            });
        }
        scratch.nav_batch_order = order;
        scratch.nav_batch_approx = approx;
        scratch.nav_batch_distances = distances;
        Ok(())
    }

    /// Node-to-node distance via cached vectors (build heuristic).
    fn dist_nodes(&self, a: VecId, b: VecId, scratch: &mut Scratch) -> Result<f64, QueryError> {
        if let (Some(params), Some(store), Some(mapping)) = (
            scratch.build_quant,
            scratch.build_adj.as_deref(),
            scratch.build_codes.as_deref(),
        ) {
            if let (Some((a, na)), Some((b, nb))) = (
                store.mapped_codes(a, mapping),
                store.mapped_codes(b, mapping),
            ) {
                return Ok((na + nb - 2 * dot_i8(a, b)) as f64 * (params.dist_mul as f64));
            }
        }
        if let Some(params) = scratch.incremental_quant {
            if let Some(arena) = scratch.navigation.as_deref() {
                if let (Some(a), Some(b)) = (arena.ordinal(a), arena.ordinal(b)) {
                    return Ok((arena.norms[a] + arena.norms[b]
                        - 2 * dot_i8(arena.codes_at(a), arena.codes_at(b)))
                        as f64
                        * (params.dist_mul as f64));
                }
            }
            let va = self.read_and_maybe_cache(a, scratch)?;
            let vb = self.read_and_maybe_cache(b, scratch)?;
            return match (va, vb) {
                (
                    Some(CachedVector::Sq8 { codes: a, norm: na }),
                    Some(CachedVector::Sq8 { codes: b, norm: nb }),
                ) => Ok((na + nb - 2 * dot_i8(&a, &b)) as f64 * (params.dist_mul as f64)),
                (Some(CachedVector::Exact(a)), Some(CachedVector::Exact(b))) => {
                    Ok(ordering_distance(&a, &b, self.metric))
                }
                (Some(CachedVector::Exact(exact)), Some(CachedVector::Sq8 { .. }))
                | (Some(CachedVector::Sq8 { .. }), Some(CachedVector::Exact(exact))) => {
                    // A concurrent overwrite may make one row exact after
                    // admission. Decode both for a conservative exact fallback.
                    let mut left = Vec::with_capacity(self.dim());
                    let mut right = Vec::with_capacity(exact.len());
                    if !self.file.read_vector_into(a, &mut left)?
                        || !self.file.read_vector_into(b, &mut right)?
                    {
                        Ok(f64::INFINITY)
                    } else {
                        Ok(ordering_distance(&left, &right, self.metric))
                    }
                }
                _ => Ok(f64::INFINITY),
            };
        }
        // Do not clone the shared arena per distance: that contends on one
        // refcount cache line across all build workers. Capture only ordinals
        // and scale before mutating the worker-local cache.
        let cross = if let Some(arena) = scratch.arena.as_deref() {
            if arena.sq8() {
                let (oa, ob) = (arena.ord_of(a), arena.ord_of(b));
                if let (Some(oa), Some(ob)) = (oa, ob) {
                    return Ok(arena.ssd_nodes(oa, ob));
                }
                scratch.build_quant.map(|_| (oa, ob, arena.dist_mul))
            } else {
                if let (Some(va), Some(vb)) = (arena.get(a), arena.get(b)) {
                    return Ok(ordering_distance(va, vb, self.metric));
                }
                None
            }
        } else {
            None
        };
        if let Some((oa, ob, dist_mul)) = cross {
            match (oa, ob) {
                (Some(oa), None) => {
                    if let Some(CachedVector::Sq8 { codes, norm }) =
                        self.read_and_maybe_cache(b, scratch)?
                    {
                        let arena = scratch
                            .arena
                            .as_deref()
                            .expect("cache lookup retains build arena");
                        return Ok(
                            (arena.norms[oa] + norm - 2 * dot_i8(arena.codes_at(oa), &codes))
                                as f64
                                * (dist_mul as f64),
                        );
                    }
                }
                (None, Some(ob)) => {
                    if let Some(CachedVector::Sq8 { codes, norm }) =
                        self.read_and_maybe_cache(a, scratch)?
                    {
                        let arena = scratch
                            .arena
                            .as_deref()
                            .expect("cache lookup retains build arena");
                        return Ok(
                            (norm + arena.norms[ob] - 2 * dot_i8(&codes, arena.codes_at(ob)))
                                as f64
                                * (dist_mul as f64),
                        );
                    }
                }
                (None, None) => {
                    let va = self.read_and_maybe_cache(a, scratch)?;
                    let vb = self.read_and_maybe_cache(b, scratch)?;
                    if let (
                        Some(CachedVector::Sq8 {
                            codes: ca,
                            norm: na,
                        }),
                        Some(CachedVector::Sq8 {
                            codes: cb,
                            norm: nb,
                        }),
                    ) = (va, vb)
                    {
                        return Ok((na + nb - 2 * dot_i8(&ca, &cb)) as f64 * (dist_mul as f64));
                    }
                }
                (Some(_), Some(_)) => unreachable!("covered pair returned above"),
            }
            return Ok(f64::INFINITY);
        }
        // DML and non-quantized builds retain the exact pair-distance rule.
        let Some(CachedVector::Exact(va)) = self.read_and_maybe_cache(a, scratch)? else {
            return Ok(f64::INFINITY);
        };
        let Some(CachedVector::Exact(vb)) = self.read_and_maybe_cache(b, scratch)? else {
            return Ok(f64::INFINITY);
        };
        Ok(ordering_distance(&va, &vb, self.metric))
    }

    /// Read `vid`'s layer-`level` neighbour list (borrowed page, no clone).
    /// Empty for a missing node or a level above the node's top layer.
    fn neighbors_at_into(
        &self,
        vid: VecId,
        level: usize,
        scratch: &Scratch,
        out: &mut Vec<VecId>,
    ) -> Result<(), QueryError> {
        out.clear();
        if let Some(store) = scratch.build_adj.as_deref() {
            out.extend(store.neighbors(vid, level)?);
            return Ok(());
        }
        self.file.read_neighbors_into(vid, level, out)?;
        Ok(())
    }

    fn set_neighbors_for_build(
        &self,
        txn_id: u64,
        vid: VecId,
        level: usize,
        neighbors: &[VecId],
        scratch: &Scratch,
    ) -> Result<(), QueryError> {
        if let Some(store) = scratch.build_adj.as_deref() {
            store.set_neighbors(vid, level, neighbors)
        } else {
            self.file.set_neighbors(txn_id, vid, level, neighbors)
        }
    }

    fn push_neighbor_for_build(
        &self,
        txn_id: u64,
        vid: VecId,
        level: usize,
        neighbor: VecId,
        scratch: &Scratch,
    ) -> Result<bool, QueryError> {
        if let Some(store) = scratch.build_adj.as_deref() {
            store.push_neighbor(vid, level, neighbor)
        } else {
            self.file.push_neighbor(txn_id, vid, level, neighbor)
        }
    }

    /// Prefetch only neighbours whose distance will need a page read: current
    /// chunk nodes are covered by the arena and old hot nodes may already be
    /// in the persistent cache. The buffer pool deduplicates page ids already
    /// resident or queued.
    fn prefetch_uncached_vectors(&self, vids: &[VecId], scratch: &Scratch) {
        if let Some(store) = scratch
            .build_adj
            .as_ref()
            .filter(|store| store.codes_file.is_some())
        {
            #[cfg(target_os = "linux")]
            store.prefetch_codes(vids, scratch);
            #[cfg(not(target_os = "linux"))]
            let _ = store;
            return;
        }
        if scratch.build_quant.is_none() {
            // Query and incremental-DML navigation rows live in these same
            // node pages. Let the shared data buffer deduplicate and schedule
            // the random probes before the scoring loop reaches them.
            if let Some(arena) = scratch.navigation.as_deref() {
                let mut missing: SmallVec<[VecId; 64]> = SmallVec::new();
                for &vid in vids {
                    if let Some(ordinal) = arena.ordinal(vid) {
                        arena.prefetch_code_vector(ordinal);
                    } else {
                        missing.push(vid);
                    }
                }
                self.file.prefetch_nodes(&missing);
            } else {
                self.file.prefetch_nodes(vids);
            }
            return;
        }
        let mut missing = Vec::with_capacity(vids.len());
        for &vid in vids {
            if scratch
                .arena
                .as_ref()
                .is_some_and(|arena| arena.ord_of(vid).is_some())
                || scratch.vec_cache.peek(&vid).is_some()
            {
                continue;
            }
            missing.push(vid);
        }
        self.file.prefetch_nodes(&missing);
    }

    /// Greedy single-best descent at `level` from `entry`.
    fn greedy_descend(
        &self,
        query: &[f32],
        entry: VecId,
        level: usize,
        scratch: &mut Scratch,
    ) -> Result<VecId, QueryError> {
        let mut cur = entry;
        let mut cur_d = self.dist_q(query, cur, scratch)?;
        loop {
            let mut neighbors = std::mem::take(&mut scratch.neighbor_buf);
            self.neighbors_at_into(cur, level, scratch, &mut neighbors)?;
            self.prefetch_uncached_vectors(&neighbors, scratch);
            self.dist_q_batch(query, &neighbors, scratch)?;
            let mut changed = false;
            for (&nb, &d) in neighbors.iter().zip(scratch.nav_batch_distances.iter()) {
                if d < cur_d {
                    cur_d = d;
                    cur = nb;
                    changed = true;
                }
            }
            scratch.neighbor_buf = neighbors;
            if !changed {
                break;
            }
        }
        Ok(cur)
    }

    /// Best-first search confined to `level`, returning up to `ef` nearest
    /// nodes ascending by distance.
    fn search_layer(
        &self,
        query: &[f32],
        entries: &[VecId],
        ef: usize,
        level: usize,
        scratch: &mut Scratch,
    ) -> Result<Vec<Scored>, QueryError> {
        scratch.clear_layer();
        for &e in entries {
            if scratch.visited.insert(self.file.node_id(e)?) {
                let d = self.dist_q(query, e, scratch)?;
                scratch.frontier.push(Reverse(Scored { dist: d, id: e }));
                scratch.results.push(Scored { dist: d, id: e });
            }
        }
        while scratch.results.len() > ef {
            scratch.results.pop();
        }
        while let Some(Reverse(c)) = scratch.frontier.pop() {
            let furthest = scratch
                .results
                .peek()
                .map(|s| s.dist)
                .unwrap_or(f64::INFINITY);
            if c.dist > furthest && scratch.results.len() >= ef {
                break;
            }
            let mut neighbors = std::mem::take(&mut scratch.neighbor_buf);
            self.neighbors_at_into(c.id, level, scratch, &mut neighbors)?;
            let mut fresh = SmallVec::<[VecId; 64]>::new();
            for &nb in &neighbors {
                if scratch.visited.insert(self.file.node_id(nb)?) {
                    fresh.push(nb);
                }
            }
            scratch.neighbor_buf = neighbors;
            // Back-edges and repeated candidates never need another score.
            // Filter them before issuing random I/O hints, especially when
            // the graph's codes exceed the file-cache working set.
            self.prefetch_uncached_vectors(&fresh, scratch);
            self.dist_q_batch(query, &fresh, scratch)?;
            for (nb, &d) in fresh.into_iter().zip(scratch.nav_batch_distances.iter()) {
                let s = Scored { dist: d, id: nb };
                if scratch.results.len() < ef {
                    scratch.frontier.push(Reverse(s));
                    scratch.results.push(s);
                } else {
                    let furthest = scratch
                        .results
                        .peek()
                        .map(|x| x.dist)
                        .unwrap_or(f64::INFINITY);
                    if d < furthest {
                        if let Some(mut top) = scratch.results.peek_mut() {
                            *top = s;
                        }
                        scratch.frontier.push(Reverse(s));
                    }
                }
            }
        }
        // Drain keeps the heap allocation in query scratch for the next layer.
        let mut out: Vec<Scored> = scratch.results.drain().collect();
        out.sort_unstable();
        Ok(out)
    }

    /// Neighbour-selection heuristic (HNSW Algorithm 4): keep a candidate
    /// only if it is closer to the query than to any already-selected
    /// neighbour. Rejected redundant edges are not added back merely to
    /// fill capacity: M is an upper bound, not a required degree.
    fn select_neighbors(
        &self,
        candidates: &[Scored],
        m: usize,
        scratch: &mut Scratch,
    ) -> Result<Vec<Scored>, QueryError> {
        if let (Some(params), Some(store), Some(mapping)) = (
            scratch.build_quant,
            scratch.build_adj.as_deref(),
            scratch.build_codes.as_deref(),
        ) {
            // Resolve each immutable row once. Borrow the shared mapping;
            // retaining/copying hundreds of candidates into per-worker LRUs
            // duplicates the file cache and makes eviction dominate linking.
            let codes: Option<Vec<_>> = candidates
                .iter()
                .map(|candidate| store.mapped_codes(candidate.id, mapping))
                .collect();
            if let Some(codes) = codes {
                let selected =
                    super::hnsw_core::select_diverse_by(0..candidates.len(), m, |i, j| {
                        let (a, na) = codes[i];
                        let (b, nb) = codes[j];
                        Ok::<_, QueryError>(
                            (na + nb - 2 * dot_i8(a, b)) as f64 * (params.dist_mul as f64)
                                < candidates[i].dist,
                        )
                    })?;
                return Ok(selected.into_iter().map(|i| candidates[i]).collect());
            }
        }
        // SQ8 arena fast path: resolve each candidate's ordinal **once**, then
        // run the O(sel²) heuristic over pre-resolved ordinals (pure int8 SSD,
        // no per-pair vid→ord hashing — the dominant build cost otherwise).
        if let Some(arena) = scratch.arena.clone() {
            if arena.sq8() {
                if let Some(selected) = Self::select_neighbors_sq8(candidates, m, &arena) {
                    return Ok(selected);
                }
                // Retaining candidates after LRU eviction must stay bounded
                // even for unusually large dimensions or ef_construction.
                if scratch.build_quant.is_some()
                    && candidates
                        .len()
                        .saturating_mul(arena.dim.saturating_add(64))
                        <= 64 * 1024
                {
                    return self.select_neighbors_cross_chunk(candidates, m, &arena, scratch);
                }
            }
        }
        super::hnsw_core::select_diverse(candidates, m, |a, b| self.dist_nodes(a, b, scratch))
    }

    /// Resolve old-node cache entries once per candidate, not once per
    /// diversity pair. The current chunk is borrowed without copying codes.
    fn select_neighbors_cross_chunk(
        &self,
        candidates: &[Scored],
        m: usize,
        arena: &BuildVectorArena,
        scratch: &mut Scratch,
    ) -> Result<Vec<Scored>, QueryError> {
        enum Source {
            Current(usize),
            Older(CachedVector),
            Missing,
        }
        fn parts<'a>(source: &'a Source, arena: &'a BuildVectorArena) -> Option<(&'a [i8], i32)> {
            match source {
                Source::Current(i) => Some((arena.codes_at(*i), arena.norms[*i])),
                Source::Older(CachedVector::Sq8 { codes, norm }) => Some((codes, *norm)),
                _ => None,
            }
        }
        let mut sources = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            sources.push(if let Some(ordinal) = arena.ord_of(candidate.id) {
                Source::Current(ordinal)
            } else if let Some(vector) = self.read_and_maybe_cache(candidate.id, scratch)? {
                Source::Older(vector)
            } else {
                Source::Missing
            });
        }
        let selected = super::hnsw_core::select_diverse_by(0..candidates.len(), m, |i, r| {
            Ok::<_, QueryError>(
                match (parts(&sources[i], arena), parts(&sources[r], arena)) {
                    (Some((a, na)), Some((b, nb))) => {
                        (na + nb - 2 * dot_i8(a, b)) as f64 * (arena.dist_mul as f64)
                            < candidates[i].dist
                    }
                    _ => false,
                },
            )
        })?;
        Ok(selected.into_iter().map(|i| candidates[i]).collect())
    }

    /// SQ8 specialisation of [`Self::select_neighbors`]: each candidate's arena
    /// ordinal is resolved once into `cand_ords`, so the inner diversity test is
    /// `arena.ssd_nodes(ord, ord)` — array index + one integer `sdot`, no
    /// hashing. Candidates are already ascending by distance.
    fn select_neighbors_sq8(
        candidates: &[Scored],
        m: usize,
        arena: &BuildVectorArena,
    ) -> Option<Vec<Scored>> {
        // A chunk arena is only a prefix-local accelerator, not the graph.
        // Apply the same diversity rule to older candidates via the shared
        // global-scale cache instead of silently skipping them.
        if candidates
            .iter()
            .any(|candidate| arena.ord_of(candidate.id).is_none())
        {
            return None;
        }
        let mut selected: Vec<Scored> = Vec::with_capacity(m);
        let mut sel_ords: Vec<usize> = Vec::with_capacity(m);
        for c in candidates {
            if selected.len() >= m {
                break;
            }
            let co = arena
                .ord_of(c.id)
                .expect("candidate coverage checked above");
            let mut keep = true;
            for &ro in &sel_ords {
                if arena.ssd_nodes(co, ro) < c.dist {
                    keep = false;
                    break;
                }
            }
            if keep {
                selected.push(*c);
                sel_ords.push(co);
            }
        }
        Some(selected)
    }

    /// Re-select `node`'s adjacency at a layer down to at most `m`.
    fn prune_select(
        &self,
        node: VecId,
        existing: &[VecId],
        m: usize,
        scratch: &mut Scratch,
    ) -> Result<Vec<VecId>, QueryError> {
        if existing.len() <= m {
            return Ok(existing.to_vec());
        }
        let mut scored: Vec<Scored> = Vec::with_capacity(existing.len());
        for &nb in existing {
            scored.push(Scored {
                dist: self.dist_nodes(node, nb, scratch)?,
                id: nb,
            });
        }
        scored.sort_unstable();
        Ok(self
            .select_neighbors(&scored, m, scratch)?
            .into_iter()
            .map(|s| s.id)
            .collect())
    }

    /// Wire a bidirectional edge `(id, nb)` at `level`, pruning the target's
    /// list to `m` when its reserved capacity is reached. Because the page
    /// store reserves exactly `m`/`m0` per layer, pruning is eager (at cap)
    /// rather than the in-memory soft-cap amortisation — same ≤m invariant.
    /// Wire the reverse edge `nb -> id` at `level`: append it, and if the
    /// target's reserved capacity is full, prune its list (including the new
    /// edge) back to `m`. The forward edges `id -> *` are written in one batch
    /// by [`Self::link`] (a single page write) rather than one push each, so
    /// this only handles the scattered reverse side.
    fn add_reverse_edge(
        &self,
        txn_id: u64,
        id: VecId,
        nb: VecId,
        level: usize,
        m: usize,
        scratch: &mut Scratch,
    ) -> Result<(), QueryError> {
        if id == nb {
            return Ok(());
        }
        // Serialize the complete reverse append/prune transaction for this
        // target. Distance reads never acquire these latches, so there is no
        // nested target-latch ordering, including the disk-backed build store.
        let _reverse = self.reverse_latches[((nb >> 16) ^ nb) as usize & 255].lock();
        let appended = self.push_neighbor_for_build(txn_id, nb, level, id, scratch)?;
        if !appended {
            let mut existing = std::mem::take(&mut scratch.neighbor_buf);
            self.neighbors_at_into(nb, level, scratch, &mut existing)?;
            if existing.is_empty() {
                // Missing node / level — nothing to prune against.
                scratch.neighbor_buf = existing;
                return Ok(());
            }
            if !existing.contains(&id) {
                existing.push(id);
            }
            let pruned = self.prune_select(nb, &existing, m, scratch)?;
            self.set_neighbors_for_build(txn_id, nb, level, &pruned, scratch)?;
            scratch.neighbor_buf = existing;
        }
        Ok(())
    }

    /// Insert one vector keyed by `row_id`, using a throwaway per-insert
    /// scratch (the incremental-DML entry point).
    pub(crate) fn insert(
        &self,
        txn_id: u64,
        row_id: RowId,
        vector: &[f32],
    ) -> Result<VecId, QueryError> {
        if vector.len() != self.dim() {
            return Err(crate::storage::error(
                crate::error::catalog::MYDB_ER_INVALID_VECTOR_VALUE,
                format!(
                    "HNSW vector dimension mismatch: expected {}, got {}",
                    self.dim(),
                    vector.len()
                ),
            ));
        }
        // Insertion changes topology but never overwrites an existing vector.
        // Retain the already admitted codes; stale CSR/results are epoch-gated.
        // Consume a cold-build seed before either calibration or topology
        // invalidation can retire it; empty DML graphs maintain it from row 1.
        self.ensure_row_index()?;
        self.ensure_navigation_quantization(vector)?;
        let _gate = self.navigation_gate.read();
        let _mutation = self.mutation_gate.read();
        self.invalidate_query_topology();
        // The build-cache setting is not a per-DML allocation allowance.
        // Bound and admit decoded rows before changing the graph.
        let per_entry = self.dim().saturating_mul(4).saturating_add(128);
        let cache_entries = self
            .ef_construction
            .saturating_mul(16)
            .max(self.m0)
            .min((4 * 1024 * 1024 / per_entry).max(1));
        let traversal = self
            .ef_construction
            .saturating_mul(self.m0)
            .saturating_mul(32)
            .saturating_add(self.ef_construction.saturating_mul(64))
            .saturating_add(self.dim().saturating_mul(12));
        let _scratch_charge = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::StorageScratch,
            cache_entries
                .saturating_mul(per_entry)
                .saturating_add(traversal),
        )?;
        let mut scratch = Scratch::default();
        scratch.vec_cache_limit = cache_entries;
        self.configure_persisted_query(vector, &mut scratch);
        scratch.navigation = self
            .query_nav
            .read()
            .clone()
            .filter(|_| !scratch.qcodes.is_empty());
        let result = self.insert_with_scratch(txn_id, row_id, vector, &mut scratch);
        drop(_mutation);
        drop(_gate);
        if self.query_nav_rebuild_requested.load(Ordering::Acquire) {
            self.start_query_navigation_build();
        }
        result
    }

    /// Insert one vector reusing a caller-owned scratch. A cold build passes
    /// the **same** scratch across every insert so each node's immutable
    /// vector is page-read + decoded once for the whole build (resident in
    /// `scratch.vec_cache`, bounded by `build_cache_max_entries`) rather than
    /// re-decoded on every visit — the single biggest cold-build cost once WAL
    /// and clones are removed.
    fn insert_with_scratch(
        &self,
        txn_id: u64,
        row_id: RowId,
        vector: &[f32],
        scratch: &mut Scratch,
    ) -> Result<VecId, QueryError> {
        let ordinal = self.file.node_count();
        let level = self.random_level(ordinal);
        let id = self.file.append_node(txn_id, row_id, level, vector)?;
        // Cached and uncached incremental paths must use the same distance
        // rule. Only a wholly representable graph uses SQ8 pair scoring; an
        // out-of-calibration row keeps the exact maintenance path.
        if self.file.meta().nav_exact_nodes == 0 && !scratch.qcodes.is_empty() {
            scratch.incremental_quant =
                self.file
                    .navigation_quantization()
                    .map(|(mid, inv_scale, dist_mul)| BuildQuantParams {
                        mid,
                        inv_scale,
                        dist_mul,
                    });
        }
        self.link(txn_id, id, level, vector, scratch)?;
        self.note_inserted_vid(row_id, id)?;
        // Publish counters after the last appended node, including a partial
        // tail page. Allocation/entry changes alone can leave persisted counts
        // behind and disable the resident navigation image after restart.
        self.file.flush_meta(txn_id)?;
        Ok(id)
    }

    /// Insert using the system DDL transaction id — the entry point the
    /// backend-agnostic [`super::graph::VectorGraph`] uses, since paged-graph
    /// writes carry no logical undo and are never tied to a user transaction.
    pub(crate) fn insert_ddl(&self, row_id: RowId, vector: &[f32]) -> Result<VecId, QueryError> {
        self.insert(VEC_DDL_TXN, row_id, vector)
    }

    pub(crate) fn chunk_vector_bytes_per_dimension(&self, calibrated: bool) -> usize {
        // The caller retains the original f32 rows through linking. Only the
        // exact fallback needs a second f32 arena; SQ8 owns one byte per value.
        std::mem::size_of::<f32>()
            + if calibrated
                && (self.navigation_quant_enabled && paged_quant_enabled())
                && self.metric != Metric::Dot
            {
                1
            } else {
                4
            }
    }

    pub(crate) fn build_session(
        &self,
        threads: usize,
        total_cache_bytes: usize,
        quantization: Option<(f32, f32, f32)>,
        expected_rows: usize,
    ) -> Result<PagedHnswBuildSession, QueryError> {
        let quantization = quantization.filter(|_| {
            (self.navigation_quant_enabled && paged_quant_enabled()) && self.metric != Metric::Dot
        });
        if quantization.is_some() {
            self.file.set_navigation_quantization(quantization)?;
        }
        let threads = threads.max(1);
        let build_quant = quantization.map(|(mid, inv_scale, dist_mul)| BuildQuantParams {
            mid,
            inv_scale,
            dist_mul,
        });
        let scalar_bytes = if build_quant.is_some() {
            std::mem::size_of::<i8>()
        } else {
            std::mem::size_of::<f32>()
        };
        let per_entry = self
            .dim()
            .saturating_mul(scalar_bytes)
            .saturating_add(96)
            .max(1);
        let cache_entries_per_worker = (total_cache_bytes / threads / per_entry).max(1);
        let mut session = PagedHnswBuildSession {
            workers: Vec::with_capacity(threads),
            cache_entries_per_worker,
            build_quant,
            #[cfg(test)]
            force_code_reads: false,
            build_adj: if build_quant.is_some() {
                Some(Arc::new(BuildAdjStore::create(
                    self.file.build_adj_temp_path(),
                    self.m,
                    self.m0,
                    expected_rows,
                    self.dim(),
                )?))
            } else {
                None
            },
        };
        session.ensure_workers(threads);
        Ok(session)
    }

    pub(crate) fn materialize_build_adjacency(
        &self,
        session: &PagedHnswBuildSession,
    ) -> Result<(), QueryError> {
        if let Some(store) = session.build_adj.as_deref() {
            store.materialize(&self.file)?;
        }
        Ok(())
    }

    /// Append and link one bounded chunk. Worker scratch survives subsequent
    /// chunks, so hot old-node vectors are not decoded afresh for every row.
    /// Exact row vectors remain available until linking finishes.
    pub(crate) fn insert_bulk_chunk_parallel(
        &self,
        rows: &mut [(RowId, Vec<f32>)],
        threads: usize,
        session: &mut PagedHnswBuildSession,
    ) -> Result<(), QueryError> {
        let n = rows.len();
        if n == 0 {
            return Ok(());
        }
        let dim = self.dim();
        if std::env::var("MYDB_HNSW_BUILD_TIMING").is_ok() {
            eprintln!(
                "[paged] chunk append: rows={} nodes={} private={:?}",
                n,
                self.node_count(),
                crate::engine::memory_reclaimer::current_private_footprint_bytes()
            );
        }
        let mut ord: HashMap<VecId, u32, VidBuildHasher> =
            HashMap::with_capacity_and_hasher(n, VidBuildHasher::default());
        let mut appended: Vec<(VecId, usize, usize)> = Vec::with_capacity(n);
        for (row_idx, (rid, vector)) in rows.iter().enumerate() {
            let ordinal = self.file.node_count();
            let level = self.random_level(ordinal);
            let vid = self.file.append_node(VEC_DDL_TXN, *rid, level, vector)?;
            if let Some(store) = session.build_adj.as_deref() {
                store.append(vid, level)?;
            }
            ord.insert(vid, row_idx as u32);
            appended.push((vid, level, row_idx));
            self.note_inserted_vid(*rid, vid)?;
            if row_idx % 64 == 0 {
                self.file.reclaim_build_pressure()?;
            }
        }
        let arena = BuildVectorArena::build_chunk(dim, rows, ord, self.metric, session.build_quant);
        if let Some(store) = session.build_adj.as_deref() {
            store.append_codes(&arena.codes, n)?;
        }
        // Keep the bounded chunk's exact rows through linking: every exact
        // fallback must receive a dimension-correct query, even if SQ8 code
        // reads miss across chunk boundaries. The chunk budget includes f32.
        let rows: &[(RowId, Vec<f32>)] = rows;
        let arena = Arc::new(arena);
        if std::env::var("MYDB_HNSW_BUILD_TIMING").is_ok() {
            eprintln!(
                "[paged] chunk link: nodes={} private={:?}",
                self.node_count(),
                crate::engine::memory_reclaimer::current_private_footprint_bytes()
            );
        }

        let threads = threads.max(1).min(n);
        session.ensure_workers(threads);
        let code_mapping = session
            .build_adj
            .as_ref()
            .and_then(|store| store.codes_mapping.read().clone());
        #[cfg(test)]
        let code_mapping = code_mapping.filter(|_| !session.force_code_reads);
        for scratch in &mut session.workers {
            scratch.build_codes.clone_from(&code_mapping);
        }
        let effective = if threads == 1 || n < PARALLEL_BUILD_MIN_ROWS {
            1
        } else {
            threads
        };
        let chunk = appended.len().div_ceil(effective);
        let completed = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| -> Result<(), QueryError> {
            let mut handles = Vec::with_capacity(effective);
            for (work, scratch) in appended
                .chunks(chunk)
                .zip(session.workers[..effective].iter_mut())
            {
                let completed = &completed;
                let arena = Arc::clone(&arena);
                handles.push(scope.spawn(move || -> Result<(), QueryError> {
                    scratch.arena = Some(arena);
                    for &(vid, level, row_idx) in work {
                        self.link(VEC_DDL_TXN, vid, level, &rows[row_idx].1, scratch)?;
                        if completed.fetch_add(1, Ordering::Relaxed) % 64 == 0 {
                            self.file.reclaim_build_pressure()?;
                        }
                    }
                    scratch.arena = None;
                    Ok(())
                }));
            }
            let mut first_error = None;
            for handle in handles {
                match handle.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        first_error.get_or_insert(err);
                    }
                    Err(_) => {
                        first_error.get_or_insert(
                            crate::error::catalog::ER_UNKNOWN_ERROR.diagnostic(
                                "Unknown error",
                                "MYDB-DIAG-HNSW-WORKER-PANIC",
                                "HNSW chunked build worker panicked",
                            ),
                        );
                    }
                }
            }
            first_error.map_or(Ok(()), Err)
        })
    }

    /// Begin a cold (bulk) build from the heap: subsequent `insert_ddl` calls
    /// run under deferred-WAL mode (one WAL record per page at finalize,
    /// instead of one per edge). Pair with [`Self::finalize_bulk_build`].
    pub(crate) fn begin_bulk_build(&self) {
        self.invalidate_query_navigation();
        self.file.begin_build();
    }

    /// Finish a cold build: durably emit the per-page WAL records and mark the
    /// graph finalized. Must be called for the build to be trusted on reopen.
    pub(crate) fn finalize_bulk_build(&self) -> Result<(), QueryError> {
        self.file.finalize_build(VEC_DDL_TXN)
    }

    /// Whether the underlying file holds a finalized (fully-built) graph.
    pub(crate) fn is_finalized(&self) -> bool {
        self.file.is_finalized()
    }

    /// Wire an already-appended node `id` (top layer `level`, vector
    /// `node_vec`) into the graph.
    fn link(
        &self,
        txn_id: u64,
        id: VecId,
        level: usize,
        node_vec: &[f32],
        scratch: &mut Scratch,
    ) -> Result<(), QueryError> {
        debug_assert_eq!(node_vec.len(), self.dim());
        // Reset the per-insert best-first state; the vector cache persists
        // (it is shared across inserts during a bulk build).
        scratch.clear_layer();
        let (ep, max_level) = match self.file.claim_entry(txn_id, id, level as u32)? {
            Some((ep, ml)) if ep != id => (ep, ml as usize),
            _ => return Ok(()),
        };

        // Seed the cache with the new node's own vector (used by every pairwise
        // heuristic distance below) to avoid re-reading its page. Skipped only
        // for an in-memory cold build (it already holds every node's vector).
        let cache_limit = if scratch.vec_cache_limit == 0 {
            self.build_cache_max_entries
        } else {
            scratch.vec_cache_limit
        };
        if scratch.arena.is_none() {
            if scratch.incremental_quant.is_some() {
                self.read_and_maybe_cache(id, scratch)?.ok_or_else(|| {
                    crate::storage::error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "new HNSW node is missing during navigation setup",
                    )
                })?;
            } else {
                scratch.cache_vector(
                    id,
                    CachedVector::Exact(Arc::new(node_vec.to_vec())),
                    cache_limit,
                );
            }
        }
        // The chunk already encoded this node on the global calibration.
        // Reuse that row instead of retaining and normalizing another f32 copy.
        if let Some(arena) = scratch.arena.as_deref().filter(|arena| arena.sq8()) {
            if let Some(ordinal) = arena.ord_of(id) {
                scratch.qcodes.clear();
                scratch.qcodes.extend_from_slice(arena.codes_at(ordinal));
                scratch.qnorm = arena.norms[ordinal];
            } else {
                scratch.qcodes.resize(arena.dim, 0);
                quantize_into(
                    &quant_src(node_vec, self.metric),
                    arena.mid,
                    arena.inv_scale,
                    &mut scratch.qcodes,
                );
                scratch.qnorm = dot_i8(&scratch.qcodes, &scratch.qcodes);
            }
        }
        // Phase 2: greedy descent above `level`.
        let mut cur = ep;
        let mut l = max_level;
        while l > level {
            cur = self.greedy_descend(node_vec, cur, l, scratch)?;
            if l == 0 {
                break;
            }
            l -= 1;
        }
        // Phase 3: from min(level, max_level) down to 0, find + wire neighbours.
        let mut entries = vec![cur];
        let start = level.min(max_level);
        for lc in (0..=start).rev() {
            let found = self.search_layer(node_vec, &entries, self.ef_construction, lc, scratch)?;
            let m_layer = if lc == 0 { self.m0 } else { self.m };
            // M is insertion fanout; m0 is the layer-0 capacity for reverse
            // edges. Filling that capacity on birth doubled maintenance work.
            let selected = self.select_neighbors(&found, self.m, scratch)?;
            // Parallel linkers may already have installed reverse edges on
            // this new node. Merge under the same target latch as reverse
            // pruning, instead of overwriting those concurrent contributions.
            let fwd: Vec<VecId> = selected.iter().map(|s| s.id).filter(|&v| v != id).collect();
            if !fwd.is_empty() {
                let _target = self.reverse_latches[((id >> 16) ^ id) as usize & 255].lock();
                let mut merged = Vec::new();
                self.neighbors_at_into(id, lc, scratch, &mut merged)?;
                for &nb in &fwd {
                    if !merged.contains(&nb) {
                        merged.push(nb);
                    }
                }
                let merged = self.prune_select(id, &merged, m_layer, scratch)?;
                self.set_neighbors_for_build(txn_id, id, lc, &merged, scratch)?;
            }
            // Reverse edges: scattered across the neighbours' own pages, wired
            // (and pruned) one target at a time.
            for &nb in &fwd {
                self.add_reverse_edge(txn_id, id, nb, lc, m_layer, scratch)?;
            }
            entries = found.iter().map(|s| s.id).collect();
            if entries.is_empty() {
                entries = vec![cur];
            }
        }
        // Phase 4: raise the entry point if this node's level beats the max.
        if level > max_level {
            self.file.raise_entry(txn_id, id, level as u32)?;
        }
        Ok(())
    }

    fn search_layer_query_navigation(
        arena: &QueryNavArena,
        file: &VectorIndexFile,
        qcodes: &[i8],
        qnorm: i32,
        entry: usize,
        ef: usize,
    ) -> Result<Vec<usize>, QueryError> {
        QUERY_NAV_SCRATCH.with(|pool| {
            let mut cached = pool.borrow_mut();
            let mut scratch = match cached
                .take()
                .filter(|s| s.capacity_nodes == arena.vids.len())
            {
                Some(mut scratch) => {
                    scratch.reset();
                    scratch
                }
                None => QueryNavScratch::try_new(arena.vids.len(), ef, arena.dim)?,
            };
            scratch.prepare_effort(ef)?;
            scratch.visit(entry)?;
            let first = Scored {
                dist: arena.distance(qcodes, qnorm, entry),
                id: entry as u64,
            };
            scratch.frontier.push(Reverse(first));
            scratch.results.push(first);
            // Keep CPU lookahead small for resident images. Disk-backed probes
            // need a wider bounded I/O window to overlap random page misses.
            let prefetch_ahead = if matches!(&arena.codes, QueryNavCodes::Spilled(_))
                && arena.codes.len()
                    > crate::engine::executor::mem_budget::effective_memory_limit_bytes() / 4
            {
                32
            } else {
                8
            };
            while let Some(Reverse(candidate)) = scratch.frontier.pop() {
                let furthest = scratch
                    .results
                    .peek()
                    .map(|scored| scored.dist)
                    .unwrap_or(f64::INFINITY);
                if candidate.dist > furthest && scratch.results.len() >= ef {
                    break;
                }
                let neighbors = arena.neighbors(candidate.id as usize);
                // Mark visited first and prefetch only genuinely new neighbours.
                // The old loop prefetched every back-edge before rejecting it,
                // polluting caches without changing the candidate set.
                scratch.reserve_unseen(neighbors.len())?;
                scratch.unseen_neighbors.clear();
                for &neighbor in neighbors {
                    let ordinal = match &arena.neighbors {
                        QueryNavEdges::Resident(_) => neighbor as usize,
                        QueryNavEdges::Spilled(_) => {
                            let Some(ordinal) = arena.ordinal(file.vid_from_node_id(neighbor))
                            else {
                                continue;
                            };
                            ordinal
                        }
                    };
                    if scratch.visit(ordinal)? {
                        scratch.unseen_neighbors.push(ordinal as u32);
                    }
                }
                for &neighbor in scratch.unseen_neighbors.iter().take(prefetch_ahead) {
                    arena.prefetch_code_vector(neighbor as usize);
                }
                let mut index = 0usize;
                while index + 4 <= scratch.unseen_neighbors.len() {
                    for &future in scratch
                        .unseen_neighbors
                        .iter()
                        .skip(index + prefetch_ahead)
                        .take(4)
                    {
                        arena.prefetch_code_vector(future as usize);
                    }
                    let neighbors = [
                        scratch.unseen_neighbors[index],
                        scratch.unseen_neighbors[index + 1],
                        scratch.unseen_neighbors[index + 2],
                        scratch.unseen_neighbors[index + 3],
                    ];
                    let ordinals = neighbors.map(|neighbor| neighbor as usize);
                    let distances = arena.distances4(qcodes, qnorm, ordinals);
                    for offset in 0..4 {
                        scratch.consider(
                            Scored {
                                dist: distances[offset],
                                id: u64::from(neighbors[offset]),
                            },
                            ef,
                        )?;
                    }
                    index += 4;
                }
                while index < scratch.unseen_neighbors.len() {
                    let neighbor = scratch.unseen_neighbors[index];
                    let ordinal = neighbor as usize;
                    scratch.consider(
                        Scored {
                            dist: arena.distance(qcodes, qnorm, ordinal),
                            id: u64::from(neighbor),
                        },
                        ef,
                    )?;
                    index += 1;
                }
            }
            let mut found: Vec<Scored> = scratch.results.drain().collect();
            found.sort_unstable();
            let output = found.into_iter().map(|scored| scored.id as usize).collect();
            let limit = (crate::engine::executor::mem_budget::memory_target_bytes() / 16)
                .min(64 * 1024 * 1024);
            if scratch.charge.as_ref().is_some_and(|c| c.bytes() <= limit) {
                *cached = Some(scratch);
            }
            Ok(output)
        })
    }

    /// k-NN search. Returns up to `k` `(row_id, exact_distance)` for live
    /// (non-tombstoned) nodes, ascending by distance. MVCC visibility of the
    /// row is the caller's responsibility.
    pub(crate) fn search(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<Vec<(RowId, f64)>, QueryError> {
        let _gate = self.navigation_gate.read();
        if k == 0 {
            return Ok(Vec::new());
        }
        let (ep, max_level) = match self.file.entry_with_level() {
            Some((ep, ml)) => (ep, ml as usize),
            None => return Ok(Vec::new()),
        };
        let ef = ef_search.max(k).min(self.node_count() as usize).max(1);
        if let Some(result) = self
            .file
            .with_navigation_mapping(|mapping| mapping.search(query, k, ef))
        {
            return result;
        }
        let navigation = self.query_nav.read().clone();
        let arena = navigation.clone().filter(|arena| {
            arena.epoch == self.query_nav_epoch.load(Ordering::Acquire)
                && arena.vids.len() == self.file.node_count() as usize
                && arena.dim == query.len()
        });
        // Check the full exact-query key before allocating traversal scratch or
        // quantizing 768+ dimensions. A cache hit needs neither operation.
        if let Some(cached) = arena
            .as_deref()
            .and_then(|arena| arena.query_cache.get(query, k, ef))
        {
            return Ok(cached);
        }
        // Navigation codes are persisted with the node. Layer 0 uses the
        // optional governed SQ8/CSR image when admitted; otherwise it reads
        // those same codes and edges through the shared data buffer.
        let mut scratch = Scratch::default();
        // Exact-nav fallbacks are read through the data buffer as well. Keep
        // only one transient decoded row; visited/frontier growth is keyed by
        // compact NodeId and released with this query.
        scratch.vec_cache_limit = 1;
        self.configure_persisted_query(query, &mut scratch);
        scratch.navigation = navigation.filter(|_| !scratch.qcodes.is_empty());
        let mut cur = ep;
        let mut l = max_level;
        while l > 0 {
            cur = self.greedy_descend(query, cur, l, &mut scratch)?;
            l -= 1;
        }
        if let Some((arena, entry)) = arena
            .as_deref()
            .filter(|_| !scratch.qcodes.is_empty())
            .and_then(|arena| arena.ordinal(cur).map(|entry| (arena, entry)))
        {
            let cand = Self::search_layer_query_navigation(
                arena,
                &self.file,
                &scratch.qcodes,
                scratch.qnorm,
                entry,
                ef,
            )?;
            let candidate_vids: Vec<VecId> =
                cand.iter().map(|&ordinal| arena.vids[ordinal]).collect();
            // Navigation no longer warms the persistent node pages. Submit the
            // bounded exact re-rank set together so the data buffer can overlap
            // misses and deduplicate pages before the scoring loop reaches
            // them.
            self.file.prefetch_nodes(&candidate_vids);
            let mut out = Vec::with_capacity(cand.len());
            for vid in candidate_vids {
                let Some((row_id, deleted, distance)) =
                    self.file
                        .read_vector_meta_distance(vid, query, self.metric)?
                else {
                    continue;
                };
                if !deleted {
                    out.push((row_id, distance));
                }
            }
            let cmp = |a: &(RowId, f64), b: &(RowId, f64)| {
                a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0))
            };
            if out.len() > k {
                out.select_nth_unstable_by(k, cmp);
                out.truncate(k);
            }
            out.sort_unstable_by(cmp);
            arena.query_cache.insert(query, k, ef, &out);
            return Ok(out);
        }
        let cand: Vec<VecId> = self
            .search_layer(query, &[cur], ef, 0, &mut scratch)?
            .iter()
            .map(|s| s.id)
            .collect();
        self.file.prefetch_nodes(&cand);
        // Exact f32 re-rank. `cand` is ordered by the SQ8-approximate navigation
        // score, so the true nearest `k` need not be its first `k` entries —
        // score every candidate on its exact f32 vector (one page decode each,
        // ≈`ef` of them, versus the hundreds of nodes navigation visited) and
        // return the real top-`k`. With exact-f32 navigation `cand` is already
        // exact-ordered, but re-ranking the bounded set is cheap and keeps one
        // code path. Tombstoned nodes are skipped here (they stay navigable hops
        // above).
        let mut out = Vec::with_capacity(cand.len());
        for &vid in &cand {
            let Some((row_id, deleted, distance)) =
                self.file
                    .read_vector_meta_distance(vid, query, self.metric)?
            else {
                continue;
            };
            if deleted {
                continue;
            }
            out.push((row_id, distance));
        }
        // Return the top-`k` in ascending distance order: the executor's stable
        // re-sort breaks equal-distance ties by this order, so a deterministic
        // ascending sort here keeps LIMIT/OFFSET windowing stable.
        out.sort_by(|a, b| a.1.total_cmp(&b.1));
        out.truncate(k);
        Ok(out)
    }

    pub(crate) fn navigation_stats(&self) -> (u64, u64, u64, u64) {
        let meta = self.file.meta();
        (
            meta.node_next_page_id.saturating_sub(1),
            meta.nav_exact_nodes,
            if self.file.navigation_quantization().is_some() {
                self.node_count().saturating_sub(meta.nav_exact_nodes)
            } else {
                0
            },
            self.file.file_size_bytes(),
        )
    }

    /// Expose the underlying file to storage orchestration (atomic rebuild
    /// publish) and storage-side integration tests.
    pub(crate) fn file(&self) -> &Arc<VectorIndexFile> {
        &self.file
    }

    #[cfg(test)]
    pub(crate) fn build_distance_cache_hits_for_test(&self, query: &[f32]) -> u64 {
        let (mid, inv_scale, dist_mul) = self.file.navigation_quantization().unwrap();
        let (vid, _) = self.file.entry_with_level().unwrap();
        let mut scratch = Scratch::default();
        scratch.build_quant = Some(BuildQuantParams {
            mid,
            inv_scale,
            dist_mul,
        });
        self.configure_persisted_query(query, &mut scratch);
        self.read_and_maybe_cache(vid, &mut scratch)
            .unwrap()
            .unwrap();
        let before = scratch.cache_hits;
        assert!(self.dist_q(query, vid, &mut scratch).unwrap().is_finite());
        scratch.cache_hits - before
    }

    #[cfg(test)]
    pub(crate) fn has_reverse_seed_for_test(&self) -> bool {
        self.query_nav
            .read()
            .as_ref()
            .is_some_and(|arena| arena.row_seed.lock().is_some())
    }

    #[cfg(test)]
    pub(crate) fn has_spilled_query_navigation_for_test(&self) -> bool {
        self.query_nav
            .read()
            .as_ref()
            .is_some_and(|arena| matches!(arena.codes, QueryNavCodes::Spilled(_)))
    }

    #[cfg(test)]
    pub(crate) fn has_query_navigation_for_test(&self) -> bool {
        if self.file.with_navigation_mapping(|_| ()).is_some() {
            return true;
        }
        self.query_nav
            .read()
            .as_ref()
            .is_some_and(|arena| arena.epoch == self.query_nav_epoch.load(Ordering::Acquire))
    }

    #[cfg(test)]
    pub(crate) fn has_query_navigation_codes_for_test(&self) -> bool {
        self.query_nav.read().is_some()
    }

    #[cfg(test)]
    pub(crate) fn clear_query_navigation_for_test(&self) {
        self.query_nav.write().take();
    }
}

#[cfg(test)]
mod chunk_arena_tests {
    use super::*;

    #[test]
    fn compact_reverse_lookup_tracks_duplicates_deletes_and_tail_compaction() {
        let bytes = RowLookup::reserved_bytes(2).unwrap();
        let charge =
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::DerivedCache, bytes).unwrap();
        let mut map = RowLookup::new(2, charge);
        let row = RowId {
            page_id: 1,
            slot_id: 2,
        };
        map.push(row, 10).unwrap();
        map.seal();
        map.push(row, 20).unwrap();
        assert_eq!(map.get(&row).as_slice(), &[10, 20]);
        assert_eq!(map.remove(&row).as_slice(), &[10, 20]);
        assert!(map.get(&row).is_empty());
        for i in 0..2048 {
            map.push(row, i + 100).unwrap();
            assert_eq!(map.remove(&row).as_slice(), &[i + 100]);
        }
        map.push(row, 9999).unwrap();
        map.seal();
        assert_eq!(map.get(&row).as_slice(), &[9999]);
        assert_eq!(
            map.charge.bytes(),
            bytes,
            "update/delete churn must reuse admitted capacity"
        );
    }

    #[test]
    fn diversity_selection_does_not_refill_rejected_redundant_edges() {
        let mut ord = HashMap::<_, _, VidBuildHasher>::default();
        for (i, vid) in [10, 20, 30].into_iter().enumerate() {
            ord.insert(vid, i as u32);
        }
        let arena = BuildVectorArena::build(
            2,
            vec![1.0, 0.0, 1.1, 0.0, 0.0, 2.0],
            ord,
            Metric::L2,
            true,
            None,
        );
        let candidates = [
            Scored { id: 10, dist: 1.0 },
            Scored { id: 20, dist: 1.21 },
            Scored { id: 30, dist: 4.0 },
        ];
        let selected = PagedHnsw::select_neighbors_sq8(&candidates, 3, &arena).unwrap();
        assert_eq!(selected.iter().map(|s| s.id).collect::<Vec<_>>(), [10, 30]);
    }

    #[test]
    fn partial_chunk_must_not_skip_closer_old_candidate() {
        let mut ord = HashMap::<_, _, VidBuildHasher>::default();
        ord.insert(20, 0);
        let arena = BuildVectorArena::build(2, vec![2.0, 0.0], ord, Metric::L2, true, None);
        let candidates = [Scored { id: 10, dist: 1.0 }, Scored { id: 20, dist: 4.0 }];
        let selected = PagedHnsw::select_neighbors_sq8(&candidates, 1, &arena);
        assert!(
            selected.is_none(),
            "partial arenas must use the cross-chunk diversity heuristic"
        );
    }

    #[test]
    fn chunk_encoding_matches_full_arena_without_exact_copies() {
        let rows = vec![
            (
                RowId {
                    page_id: 1,
                    slot_id: 0,
                },
                vec![0.0, 0.0, 0.0],
            ),
            (
                RowId {
                    page_id: 2,
                    slot_id: 0,
                },
                vec![3.0, -4.0, 2.0],
            ),
            (
                RowId {
                    page_id: 3,
                    slot_id: 0,
                },
                vec![-1.0, 2.0, 0.5],
            ),
        ];
        for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
            let quant = BuildQuantParams {
                mid: 0.0,
                inv_scale: 20.0,
                dist_mul: 0.0025,
            };
            let ord = rows
                .iter()
                .enumerate()
                .map(|(i, _)| (i as VecId, i as u32))
                .collect::<HashMap<_, _, VidBuildHasher>>();
            let data = rows.iter().flat_map(|(_, v)| v.iter().copied()).collect();
            let expected = BuildVectorArena::build(3, data, ord.clone(), metric, true, Some(quant));
            let actual = BuildVectorArena::build_chunk(3, &rows, ord.clone(), metric, Some(quant));
            assert_eq!(actual.codes, expected.codes);
            assert_eq!(actual.norms, expected.norms);
            if metric != Metric::Dot {
                assert_eq!(actual.data.capacity(), 0);
                assert_eq!(actual.ssd_nodes(1, 2), expected.ssd_nodes(1, 2));
            } else {
                assert_eq!(actual.data, expected.data);
            }
            let exact = BuildVectorArena::build_chunk(3, &rows, ord, metric, None);
            assert!(!exact.sq8());
            assert_eq!(exact.get(1).unwrap(), rows[1].1.as_slice());
        }
    }
}

#[cfg(test)]
mod build_adj_tests {
    use super::BuildAdjStore;
    use std::sync::atomic::Ordering;

    #[test]
    fn shared_code_policy_accounts_for_dimension_topology_and_overflow() {
        let limit = 1024 * 1024 * 1024;
        assert!(super::build_code_image_fits(1_000_000, 768, 16, 32, limit));
        assert!(!super::build_code_image_fits(
            1_000_000, 1536, 16, 32, limit
        ));
        assert!(!super::build_code_image_fits(
            1_000_000, 768, 128, 256, limit
        ));
        assert!(!super::build_code_image_fits(
            1_000_000,
            768,
            16,
            32,
            limit / 2
        ));
        assert!(!super::build_code_image_fits(
            usize::MAX,
            usize::MAX,
            16,
            32,
            limit
        ));
        assert!(!super::build_code_image_fits(
            1,
            1,
            usize::MAX,
            usize::MAX,
            limit
        ));
        assert!(!super::build_code_image_fits(0, 768, 16, 32, limit));
        assert!(!super::build_code_image_fits(10, 0, 16, 32, limit));
    }

    #[test]
    fn ordinal_page_hints_validate_sparse_slots_and_uncovered_pages() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            BuildAdjStore::create(dir.path().join("hints.build-adj"), 16, 32, 8, 0).unwrap();
        let values = [
            1u64 << 32,
            (1 << 32) | 1,
            2 << 32,
            (2 << 32) | 3,
            1000 << 32,
        ];
        for vid in values {
            store.append(vid, 0).unwrap();
        }
        let vids = store.published_vids();
        for (ordinal, vid) in values.into_iter().enumerate() {
            assert_eq!(store.ordinal_in(vids, vid), Some(ordinal));
        }
        assert_eq!(store.ordinal_in(vids, (2 << 32) | 2), None);
        assert_eq!(store.ordinal_in(vids, 2000 << 32), None);
        assert_eq!(
            store.page_starts.len(),
            8,
            "sparse page ids must not expand the reservation"
        );
    }

    #[test]
    fn metadata_publication_keeps_complete_prefix_and_enforces_reservation() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            BuildAdjStore::create(dir.path().join("published.build-adj"), 16, 32, 256, 0).unwrap();
        let done = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                for ordinal in 0..256 {
                    store
                        .append((ordinal + 1) as u64 * 10, ordinal % 3)
                        .unwrap();
                    std::thread::yield_now();
                }
                done.store(true, Ordering::Release);
            });
            loop {
                let vids = store.published_vids();
                for (ordinal, value) in vids.iter().enumerate() {
                    let vid = value.load(Ordering::Relaxed);
                    assert_eq!(vid, (ordinal + 1) as u64 * 10);
                    assert_eq!(store.ordinal_in(vids, vid), Some(ordinal));
                    assert_eq!(store.record_meta(ordinal).unwrap().1, ordinal % 3);
                }
                if done.load(Ordering::Acquire) {
                    break;
                }
                std::thread::yield_now();
            }
        });
        assert_eq!(store.published_vids().len(), 256);
        assert!(store.append(3000, 0).is_err());
        assert_eq!(store.published_vids().len(), 256);
    }

    #[test]
    fn compact_codes_append_without_wide_vectors_and_clean_up() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            BuildAdjStore::create(dir.path().join("graph.build-adj"), 16, 32, 3, 3).unwrap();
        let codes_path = store.codes_path.clone();
        store.append(10, 0).unwrap();
        store.append(20, 1).unwrap();
        store.append_codes(&[-128, 0, 127, 1, -2, 3], 2).unwrap();
        let mut output = Vec::new();
        assert_eq!(store.read_codes(10, &mut output).unwrap(), Some(32513));
        assert_eq!(output, [-128, 0, 127]);
        let old_mapping = store.codes_mapping.read().clone().unwrap();
        store.append(30, 0).unwrap();
        assert!(store.read_codes(30, &mut output).is_err());
        assert!(store.append_codes(&[1], 1).is_err());
        store.append_codes(&[4, 5, 6], 1).unwrap();
        assert_eq!(store.read_codes(20, &mut output).unwrap(), Some(14));
        assert_eq!(output, [1, -2, 3]);
        assert_eq!(store.read_codes(30, &mut output).unwrap(), Some(77));
        assert_eq!(output, [4, 5, 6]);
        // An older worker view stays valid when the next chunk remaps the
        // store; nodes outside that view use the checked file-read fallback.
        assert_eq!(
            store
                .read_codes_with_mapping(10, &mut output, Some(&old_mapping))
                .unwrap(),
            Some(32513)
        );
        assert_eq!(
            store
                .read_codes_with_mapping(30, &mut output, Some(&old_mapping))
                .unwrap(),
            Some(77)
        );
        assert_eq!(output, [4, 5, 6]);
        drop(old_mapping);
        assert_eq!(store.read_codes(40, &mut output).unwrap(), None);
        assert_eq!(std::fs::metadata(&codes_path).unwrap().len(), 9);
        assert_eq!(store.codes_mapping.read().as_ref().unwrap().len, 9);
        // The non-mapped oracle reads the same complete append-only prefix.
        store.codes_mapping.write().take();
        assert_eq!(store.read_codes(10, &mut output).unwrap(), Some(32513));
        assert_eq!(output, [-128, 0, 127]);
        assert_eq!(
            store.disk_charge.dir_bytes(),
            super::BUILD_ADJ_EXTENT_BYTES + 9
        );
        // A cached norm must never hide a failed read of its code payload.
        store.codes_file.as_ref().unwrap().set_len(3).unwrap();
        assert!(store.read_codes(20, &mut output).is_err());
        drop(store);
        assert!(!codes_path.exists());
    }

    #[test]
    fn mapped_adjacency_crosses_extents_and_serializes_shared_node_writes() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            BuildAdjStore::create(dir.path().join("cross.build-adj"), 16, 32, 4, 0).unwrap();
        // Put a valid record across an extent boundary without allocating a
        // million-node graph. Both sides must survive subsequent extent growth.
        store
            .used_bytes
            .store(super::BUILD_ADJ_EXTENT_BYTES - 16, Ordering::Relaxed);
        store.append(10, 1).unwrap();
        for vid in [20, 30, 40] {
            store.append(vid, 0).unwrap();
        }
        std::thread::scope(|scope| {
            for vid in [20, 30, 40] {
                let store = &store;
                scope.spawn(move || assert!(store.push_neighbor(10, 0, vid).unwrap()));
            }
        });
        let mut neighbors = store.neighbors(10, 0).unwrap();
        neighbors.sort_unstable();
        assert_eq!(neighbors, [20, 30, 40]);
        store.set_neighbors(10, 1, &[30, 20]).unwrap();
        assert_eq!(store.neighbors(10, 1).unwrap(), [30, 20]);
        assert_eq!(store.extents.read().len(), 2);
    }

    #[test]
    fn sidecar_create_materializes_missing_parent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing/indexes/graph.build-adj");
        let store = BuildAdjStore::create(path.clone(), 16, 32, 1, 0).unwrap();
        assert!(path.is_file());
        drop(store);
        assert!(!path.exists());
    }

    #[test]
    fn sidecar_records_reserve_only_actual_node_layers() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            BuildAdjStore::create(dir.path().join("graph.build-adj"), 16, 32, 3, 0).unwrap();
        store.append(10, 0).unwrap();
        store.append(20, 2).unwrap();
        store.append(30, 1).unwrap();

        // level 0: align8(1 + 32*4) = 136
        // level 2: align8(3 + (32 + 2*16)*4) = 264
        // level 1: align8(2 + (32 + 1*16)*4) = 200
        assert_eq!(store.used_bytes.load(Ordering::Relaxed), 600);

        store.set_neighbors(10, 0, &[20, 30]).unwrap();
        assert_eq!(store.neighbors(10, 0).unwrap(), vec![20, 30]);
        assert!(store.push_neighbor(20, 2, 30).unwrap());
        assert_eq!(store.neighbors(20, 2).unwrap(), vec![30]);
        assert!(!store.push_neighbor(10, 1, 20).unwrap());
        assert!(store.neighbors(10, 1).unwrap().is_empty());
    }
}

#[cfg(test)]
mod query_nav_allocation_tests {
    use super::{query_navigation_allocation, QueryNavCache, QUERY_CACHE_MAX_K};
    use crate::storage::RowId;

    #[test]
    fn million_high_dimensional_rows_fit_navigation_only_budget() {
        let layout = query_navigation_allocation(1_000_000, 768, 32, 333_334).unwrap();
        assert_eq!(
            layout.final_bytes,
            912_000_004 + 333_334 * 4 + layout.query_cache_bytes + layout.row_seed_bytes
        );
        assert_eq!(layout.peak_bytes, layout.final_bytes + 4_000_004);
        assert!(layout.query_cache_bytes < 4 * 1024 * 1024);
        let (spilled, bytes) = layout.try_reserve(500 * 1024 * 1024 / 4, Some).unwrap();
        assert!(spilled);
        assert!(
            bytes < 48 * 1024 * 1024,
            "million-row metadata must fit the unchanged 500 MiB target"
        );
        assert!(layout.peak_bytes < 1024 * 1024 * 1024);

        // A duplicate exact-f32 image alone would add 3.072 GB and reject the
        // useful SQ8/CSR navigation image under the default 1 GiB cap.
        let with_exact_copy = layout
            .peak_bytes
            .checked_add(1_000_000 * 768 * std::mem::size_of::<f32>())
            .unwrap();
        assert!(with_exact_copy > 1024 * 1024 * 1024);
    }

    #[test]
    fn navigation_spills_after_global_admission_failure_and_respects_quota() {
        let layout = query_navigation_allocation(200_000, 768, 32, 70_000).unwrap();
        let spilled =
            layout.peak_bytes - layout.codes_bytes - layout.edge_capacity * 4 + 128 * 1024;
        let mut attempts = Vec::new();
        let result = layout.try_reserve(layout.peak_bytes, |bytes| {
            attempts.push(bytes);
            (bytes == spilled).then_some(bytes)
        });
        assert_eq!(result, Some((true, spilled)));
        assert_eq!(attempts, vec![layout.peak_bytes, spilled]);
        assert_eq!(
            layout.try_reserve(layout.peak_bytes, Some),
            Some((false, layout.peak_bytes))
        );
        assert_eq!(layout.try_reserve(spilled, Some), Some((true, spilled)));
        assert!(layout
            .try_reserve::<usize>(spilled - 1, |_| panic!("over quota"))
            .is_none());
        assert!(layout
            .try_reserve(layout.peak_bytes, |_| None::<usize>)
            .is_none());
    }

    #[test]
    fn navigation_layout_rejects_overflow() {
        assert!(query_navigation_allocation(usize::MAX, 768, 32, usize::MAX).is_none());
    }

    #[test]
    fn repeated_query_cache_checks_full_key() {
        let cache = QueryNavCache::new();
        let query = vec![1.0, -2.5, 3.25];
        let result = vec![(
            RowId {
                page_id: 7,
                slot_id: 9,
            },
            1.5,
        )];
        cache.insert(&query, 10, 128, &result);
        assert_eq!(cache.get(&query, 10, 128), Some(result));
        assert!(cache.get(&query, 11, 128).is_none());
        assert!(cache.get(&query, 10, 64).is_none());
        assert!(cache.get(&[1.0, -2.5, 3.5], 10, 128).is_none());
        assert!(cache
            .get(&query, QUERY_CACHE_MAX_K.saturating_add(1), 128)
            .is_none());

        let sampled = vec![0.0; 64];
        let mut same_fingerprint = sampled.clone();
        same_fingerprint[1] = 9.0; // stride=2 samples only even positions.
        assert_eq!(
            QueryNavCache::hash(&sampled, 10, 128),
            QueryNavCache::hash(&same_fingerprint, 10, 128)
        );
        cache.insert(&sampled, 10, 128, &[]);
        assert!(cache.get(&same_fingerprint, 10, 128).is_none());
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
    #[test]
    fn source_audit_b1_paged_rejects_bad_dimensions_before_mutation() {
        let (_dir, file) = crate::storage::source_audit_vector_file();
        let graph = PagedHnsw::new(file.clone());
        for vector in [vec![1.0], vec![1.0, 2.0, 3.0]] {
            assert!(graph.insert_ddl(row(1), &vector).is_err());
            assert!(file.append_node(VEC_DDL_TXN, row(1), 0, &vector).is_err());
            assert_eq!(file.node_count(), 0);
        }
    }
    #[test]
    fn source_audit_b6_recalibration_restores_navigation() {
        let (_dir, file) = crate::storage::source_audit_vector_file();
        let graph = PagedHnsw::new(file);
        for n in 0..64 {
            graph.insert_ddl(row(n), &[n as f32, 1.0]).unwrap();
        }
        graph.refresh_query_navigation().unwrap();
        assert!(graph.has_query_navigation_for_test());
        // Force exact-navigation nodes beyond the current SQ8 calibration.
        graph.insert_ddl(row(64), &[10000.0, 10000.0]).unwrap();
        graph.next_calibration.store(65, Ordering::Release);
        graph.insert_ddl(row(65), &[20000.0, 20000.0]).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !graph.has_query_navigation_for_test() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(graph.has_query_navigation_for_test());
        assert_eq!(
            graph.search(&[20000.0, 20000.0], 1, 100).unwrap()[0],
            (row(65), 0.0)
        );
    }
    #[test]
    fn source_audit_b6_queued_rebuild_uses_latest_epoch_without_blocking_search() {
        let (_dir, file) = crate::storage::source_audit_vector_file();
        let graph = PagedHnsw::new(file);
        graph.insert_ddl(row(1), &[1.0, 2.0]).unwrap();
        graph.refresh_query_navigation().unwrap();
        let mutation = graph.mutation_gate.write();
        graph.invalidate_query_navigation();
        graph.start_query_navigation_build();
        graph.invalidate_query_topology();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| tx.send(graph.search(&[1.0, 2.0], 1, 32)).unwrap());
            let found = rx.recv_timeout(std::time::Duration::from_secs(3));
            drop(mutation);
            assert_eq!(
                found.expect("cache build must not block searches").unwrap()[0],
                (row(1), 0.0)
            );
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !graph.has_query_navigation_for_test() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            graph.has_query_navigation_for_test(),
            "queued build must publish the current epoch"
        );
    }

    #[test]
    fn source_audit_b7_partial_tombstone_failure_is_retryable() {
        let (_dir, file) = crate::storage::source_audit_vector_file();
        let graph = PagedHnsw::new(file.clone());
        graph.insert_ddl(row(1), &[1.0, 2.0]).unwrap();
        graph.insert_ddl(row(1), &[2.0, 3.0]).unwrap();
        let mut calls = 0;
        let result = graph.mark_deleted_with(
            row(1),
            |_| true,
            |vid| {
                calls += 1;
                if calls == 2 {
                    return Err(crate::storage::error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "injected tombstone I/O failure",
                    ));
                }
                file.set_tombstone(VEC_DDL_TXN, vid, true)
            },
        );
        assert!(result.is_err());
        assert!(graph.has_live_row_id(row(1)).unwrap());
        assert_eq!(graph.tombstones.load(Ordering::Acquire), 1);
        assert_eq!(graph.mark_deleted_by_row_id(row(1)).unwrap(), 1);
        assert!(!graph.has_live_row_id(row(1)).unwrap());
        assert_eq!(graph.tombstones.load(Ordering::Acquire), 2);
        assert_eq!(graph.mark_deleted_by_row_id(row(1)).unwrap(), 0);
    }
}

#[cfg(test)]
mod scratch_regressions {
    use super::*;
    #[test]
    fn huge_effort_is_bounded_and_dense_and_sparse_resets_clear_previous_visits() {
        for nodes in [65, QUERY_NAV_DENSE_RESET_WORDS * 64 + 1] {
            let mut scratch = QueryNavScratch::new(nodes, usize::MAX);
            assert!(scratch.frontier.capacity() <= nodes * 2);
            for id in [0, 64, nodes - 1] {
                scratch.visit(id).unwrap();
                assert!(!scratch.visit(id).unwrap());
            }
            assert_eq!(scratch.touched_words.is_empty(), nodes == 65);
            scratch.reset();
            assert!(scratch.visit(0).unwrap());
            assert!(scratch.visit(64).unwrap());
            if nodes != 65 {
                assert!(scratch.visit(nodes - 1).unwrap());
            }
        }
    }
}
