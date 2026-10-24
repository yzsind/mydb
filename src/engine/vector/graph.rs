//! Backend-agnostic vector-graph handle.
//!
//! [`VectorGraph`] is the value stored in `Database::vector_indexes`. It hides
//! which HNSW backend a given index uses behind one enum so the maintenance
//! hooks (`note_vector_inserts`, `vector_index_search_for_partition`,
//! compaction, persistence, …) stay backend-agnostic and the call sites need
//! no per-backend branching:
//!
//!   * [`VectorGraph::Memory`] — the in-memory slab graph ([`HnswIndex`]),
//!     the long-standing default. Derived data persisted opportunistically as
//!     a `.hnsw` snapshot, rebuilt from the heap on a missing/corrupt file.
//!   * [`VectorGraph::Paged`] — the buffer-pool-resident paged graph
//!     ([`PagedHnsw`], `.hvec`). Demand-paged (larger-than-memory) and
//!     crash-safe via physical WAL; the file itself is the durable graph, so
//!     the snapshot/serialize machinery is a no-op for it.
//!
//! Selection is per-process via `MYDB_VECTOR_STORAGE` (resolved in
//! `vector_index.rs`); the default is `memory`. Both backends are pure
//! candidate generators — correctness rests on the executor re-checking heap
//! MVCC visibility for every returned row — so any best-effort divergence
//! (e.g. a paged delete that can't find its node) only ever costs recall.

use super::hnsw::HnswIndex;
use super::hnsw_paged::PagedHnsw;
use super::ivf::IvfSq8Index;
use crate::storage::RowId;
use crate::types::QueryError;
use crate::util::logging::log_message;
use smallvec::SmallVec;

/// Test-only fault injection: fail every paged HNSW insert.
#[cfg(test)]
pub(crate) static FAIL_PAGED_WRITES_FOR_TEST: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// One vector index's candidate generator, in whichever backend it was built.
#[derive(Debug)]
pub(crate) enum VectorGraph {
    Memory(HnswIndex),
    Paged(PagedHnsw),
    /// In-memory IVF_SQ8 inverted-file index ([`IvfSq8Index`]). Rebuilt from
    /// the heap on cold load (no snapshot in this backend). The third
    /// `search` argument is interpreted as **nprobe** rather than ef.
    Ivf(IvfSq8Index),
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum VectorSearchEffort {
    Hnsw { ef: usize },
    Ivf { nprobe: usize },
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct VectorSearchRequest {
    pub candidate_limit: usize,
    /// Preserve the exact re-rank pool independently from the number of rows
    /// returned to the executor. The slab backend truncates SQ8 candidates
    /// before exact scoring; the paged backend already scores the full ef.
    pub rerank_limit: usize,
    pub effort: VectorSearchEffort,
}

#[derive(Debug, Default)]
pub(crate) struct VectorSearchResult {
    pub candidates: Vec<(RowId, f32)>,
    /// Candidate distances are authoritative f32 ordering distances. With no
    /// residual filter the executor may stop after the first k MVCC-visible
    /// rows instead of heap-reading the complete rerank pool.
    pub exact_order: bool,
    /// True only when widening the backend effort cannot visit more index
    /// structure (for IVF: nprobe has reached nlist).
    pub exhausted: bool,
}

/// Runtime stats for a single vector index, aggregated across whichever
/// per-partition graphs are currently warm in `Database::vector_indexes`.
/// Powers `information_schema.mydb_vector_indexes` and the `SHOW ENGINE MYDB
/// STATUS` HNSW section. A cold (never-queried) index has no row here — the
/// view falls back to catalog-only metadata with `LOADED = 0`.
#[derive(Debug, Default, Clone)]
pub(crate) struct VectorIndexRuntimeStats {
    /// How many per-partition graphs are warm (1 for an unpartitioned table).
    pub partitions_loaded: u32,
    /// Total nodes including tombstones, summed across loaded partitions.
    pub total_nodes: u64,
    /// Live (non-tombstoned) nodes.
    pub live_nodes: u64,
    /// Tombstoned nodes.
    pub stale_nodes: u64,
    /// Runtime vector dimension (max across partitions; 0 if not yet fixed).
    pub dim: u32,
    /// Nodes inserted since the last persisted snapshot (memory backend).
    pub dirty_nodes: u64,
    /// Max snapshot generation across partitions (memory backend; 0 for paged).
    pub generation: u64,
    /// Max persisted LSN across partitions (memory backend; 0 for paged).
    pub last_persisted_lsn: u64,
    /// `"memory"`, `"paged"`, `"paged_mapped"`, `"ivf"`, `"ivf_paged"`, or
    /// `"ivf_paged_nav"` when the governed large-index navigator is active.
    pub backend: &'static str,
    /// IVF_SQ8: number of inverted lists (k-means centroids). 0 for HNSW /
    /// cold IVF.
    pub nlist: u64,
    /// IVF_SQ8: rows inserted since the last (re)train — a centroid-drift
    /// signal suggesting when to `OPTIMIZE TABLE`. 0 for HNSW.
    pub inserts_since_train: u64,
    pub list_avg_entries: u64,
    pub list_max_entries: u64,
    pub list_p95_entries: u64,
    pub finalized: bool,
    /// Paged HNSW node pages carrying persistent SQ8 navigation rows.
    pub nav_pages: u64,
    pub paged_file_size_bytes: u64,
    pub sq8_exact_fallback_nodes: u64,
    pub tombstones_on_disk: u64,
    pub sq8_covered_nodes: u64,
    pub last_build_mode: Option<&'static str>,
    pub last_build_rows: u64,
    pub last_build_stats_ms: u64,
    pub last_build_encode_ms: u64,
    pub last_build_link_ms: u64,
    pub last_build_materialize_ms: u64,
    pub last_build_total_ms: u64,
    pub last_build_peak_scratch_bytes: u64,
    pub last_build_cache_hits: u64,
    pub last_build_cache_misses: u64,
    pub last_build_chunks: u64,
}

impl VectorIndexRuntimeStats {
    /// Fold one warm partition graph into the aggregate.
    pub(crate) fn accumulate(&mut self, graph: &VectorGraph) {
        self.partitions_loaded += 1;
        self.total_nodes += graph.node_count() as u64;
        self.live_nodes += graph.len() as u64;
        self.stale_nodes += graph.stale_count() as u64;
        self.dim = self.dim.max(graph.dim() as u32);
        self.dirty_nodes += graph.dirty_count() as u64;
        self.generation = self.generation.max(graph.generation());
        self.last_persisted_lsn = self.last_persisted_lsn.max(graph.last_persisted_lsn());
        self.backend = graph.backend_name();
        if let Some(ivf) = graph.as_ivf() {
            self.nlist = self.nlist.max(ivf.nlist() as u64);
            self.inserts_since_train += ivf.inserts_since_train();
            if let Some((file_bytes, sq8_nodes, list_avg, list_max, list_p95, finalized)) =
                ivf.paged_runtime_stats()
            {
                self.paged_file_size_bytes += file_bytes;
                self.tombstones_on_disk += ivf.stale_count() as u64;
                self.sq8_covered_nodes += sq8_nodes;
                self.list_avg_entries = self.list_avg_entries.max(list_avg);
                self.list_max_entries = self.list_max_entries.max(list_max);
                self.list_p95_entries = self.list_p95_entries.max(list_p95);
                self.finalized |= finalized;
            }
        }
        if let VectorGraph::Paged(paged) = graph {
            let (nav_pages, exact_fallback, covered, file_bytes) = paged.navigation_stats();
            self.nav_pages += nav_pages;
            self.sq8_exact_fallback_nodes += exact_fallback;
            self.sq8_covered_nodes += covered;
            self.paged_file_size_bytes += file_bytes;
            self.tombstones_on_disk += paged.stale_count() as u64;
            if let Some(build) = paged.last_build_stats() {
                self.last_build_mode = Some(build.mode);
                self.last_build_rows = self.last_build_rows.saturating_add(build.rows);
                self.last_build_stats_ms = self.last_build_stats_ms.saturating_add(build.stats_ms);
                self.last_build_encode_ms =
                    self.last_build_encode_ms.saturating_add(build.encode_ms);
                self.last_build_link_ms = self.last_build_link_ms.saturating_add(build.link_ms);
                self.last_build_materialize_ms = self
                    .last_build_materialize_ms
                    .saturating_add(build.materialize_ms);
                self.last_build_total_ms = self.last_build_total_ms.saturating_add(build.total_ms);
                self.last_build_peak_scratch_bytes = self
                    .last_build_peak_scratch_bytes
                    .max(build.peak_scratch_bytes);
                self.last_build_cache_hits =
                    self.last_build_cache_hits.saturating_add(build.cache_hits);
                self.last_build_cache_misses = self
                    .last_build_cache_misses
                    .saturating_add(build.cache_misses);
                self.last_build_chunks = self.last_build_chunks.saturating_add(build.chunks);
            }
        }
    }

    /// Tombstone / total ratio in percent (0 when empty).
    pub(crate) fn stale_ratio_pct(&self) -> i64 {
        if self.total_nodes == 0 {
            0
        } else {
            ((self.stale_nodes as f64 / self.total_nodes as f64) * 100.0).round() as i64
        }
    }
}

impl VectorGraph {
    /// Prepare the optional read-optimized query image synchronously. Paged
    /// HNSW uses this during eager startup so the first user query never races
    /// the background cache build; other backends are already query-ready.
    pub(crate) fn prepare_query_navigation(&self) -> Result<(), QueryError> {
        match self {
            VectorGraph::Paged(graph) => graph.refresh_query_navigation(),
            VectorGraph::Memory(_) | VectorGraph::Ivf(_) => Ok(()),
        }
    }

    /// Borrow the in-memory backend, if this is one. Used by the snapshot
    /// persistence path, which only applies to the memory backend (the paged
    /// file is already durable via WAL).
    pub(crate) fn as_memory(&self) -> Option<&HnswIndex> {
        match self {
            VectorGraph::Memory(g) => Some(g),
            VectorGraph::Paged(_) | VectorGraph::Ivf(_) => None,
        }
    }

    /// Borrow the IVF backend, if this is one. Used by the retrain path
    /// (`OPTIMIZE TABLE`) and observability in `vector_index.rs`.
    #[allow(dead_code)] // wired by M2 retrain / M5 observability
    pub(crate) fn as_ivf(&self) -> Option<&IvfSq8Index> {
        match self {
            VectorGraph::Ivf(i) => Some(i),
            _ => None,
        }
    }

    /// Total node count, including tombstones.
    pub(crate) fn node_count(&self) -> usize {
        match self {
            VectorGraph::Memory(g) => g.node_count(),
            VectorGraph::Paged(p) => p.node_count() as usize,
            VectorGraph::Ivf(i) => i.node_count(),
        }
    }

    /// Live (non-tombstoned) node count.
    #[allow(dead_code)] // used by the test-only hnsw_stats_for_test.
    pub(crate) fn len(&self) -> usize {
        match self {
            VectorGraph::Memory(g) => g.len(),
            VectorGraph::Paged(p) => p.live_count(),
            VectorGraph::Ivf(i) => i.live_count(),
        }
    }

    /// No live nodes.
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            VectorGraph::Memory(g) => g.is_empty(),
            VectorGraph::Paged(p) => p.node_count() == 0,
            VectorGraph::Ivf(i) => i.is_empty(),
        }
    }

    /// Fixed vector dimension (0 until the first vector fixes it, for the
    /// memory backend).
    pub(crate) fn dim(&self) -> usize {
        match self {
            VectorGraph::Memory(g) => g.dim(),
            VectorGraph::Paged(p) => p.dim(),
            VectorGraph::Ivf(i) => i.dim(),
        }
    }

    /// Backend tag for `information_schema.mydb_vector_indexes` / SHOW ENGINE.
    pub(crate) fn backend_name(&self) -> &'static str {
        match self {
            VectorGraph::Memory(_) => "memory",
            VectorGraph::Paged(graph) => {
                if graph.file().with_navigation_mapping(|_| ()).is_some() {
                    "paged_mapped"
                } else {
                    "paged"
                }
            }
            // "ivf" (memory) or "ivf_paged" (buffer-pool + WAL).
            VectorGraph::Ivf(i) => i.backend_name(),
        }
    }

    /// Snapshot generation (monotonic per persist). 0 for the paged / IVF
    /// backends, which don't persist via snapshots.
    pub(crate) fn generation(&self) -> u64 {
        match self {
            VectorGraph::Memory(g) => g.generation(),
            VectorGraph::Paged(_) | VectorGraph::Ivf(_) => 0,
        }
    }

    /// LSN the persisted snapshot is caught up to. 0 for the paged backend
    /// (the `.hvec` file itself is the durable record via physical WAL) and
    /// the IVF memory backend (rebuilt from the heap, no snapshot).
    pub(crate) fn last_persisted_lsn(&self) -> u64 {
        match self {
            VectorGraph::Memory(g) => g.last_persisted_lsn(),
            VectorGraph::Paged(_) | VectorGraph::Ivf(_) => 0,
        }
    }

    /// Tombstoned node count.
    pub(crate) fn stale_count(&self) -> u32 {
        match self {
            VectorGraph::Memory(g) => g.stale_count(),
            VectorGraph::Paged(p) => p.stale_count(),
            VectorGraph::Ivf(i) => i.stale_count(),
        }
    }

    /// Nodes inserted since the last persisted snapshot. Always 0 for the
    /// paged / IVF backends (neither uses the checkpoint snapshot gate).
    pub(crate) fn dirty_count(&self) -> u32 {
        match self {
            VectorGraph::Memory(g) => g.dirty_count(),
            VectorGraph::Paged(_) | VectorGraph::Ivf(_) => 0,
        }
    }

    /// Insert one vector keyed by `row_id`. A failed paged write durably
    /// un-finalizes the `.hvec` and returns the error: callers evict the
    /// graph, so the next access rebuilds it from the heap instead of trusting
    /// a file that silently lacks a committed row.
    pub(crate) fn insert(&self, row_id: RowId, vector: Vec<f32>) -> Result<(), QueryError> {
        match self {
            VectorGraph::Memory(g) => g.insert(row_id, vector),
            VectorGraph::Paged(p) => {
                #[cfg(test)]
                if FAIL_PAGED_WRITES_FOR_TEST.load(std::sync::atomic::Ordering::Acquire) {
                    let err = crate::storage::error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "injected paged vector write failure",
                    );
                    return Err(Self::invalidate_paged(p, "insert", err));
                }
                p.insert_ddl(row_id, &vector)
                    .map(|_| ())
                    .map_err(|e| Self::invalidate_paged(p, "insert", e))
            }
            VectorGraph::Ivf(i) => {
                i.insert(row_id, &vector);
                Ok(())
            }
        }
    }

    /// Make `row_id`'s graph entry match a committed heap image: `Some` is an
    /// idempotent upsert (an identical live slab node is kept as is), `None`
    /// a delete.
    pub(crate) fn replace_row(
        &self,
        row_id: RowId,
        vector: Option<Vec<f32>>,
    ) -> Result<(), QueryError> {
        match (self, vector) {
            (VectorGraph::Memory(g), Some(vector)) => g.reconcile_heap_vector(row_id, vector),
            (_, vector) => {
                self.mark_deleted_by_row_id(row_id)?;
                vector.map_or(Ok(()), |vector| self.insert(row_id, vector))
            }
        }
    }

    pub(crate) fn begin_transaction_delta(&self, txn_id: u64) -> Result<(), QueryError> {
        match self {
            VectorGraph::Ivf(i) => i.begin_transaction_delta(txn_id),
            _ => Ok(()),
        }
    }

    pub(crate) fn finish_committed_delta(&self, txn_id: u64) -> Result<(), QueryError> {
        match self {
            VectorGraph::Ivf(i) => i.finish_committed_delta(txn_id),
            _ => Ok(()),
        }
    }

    pub(crate) fn flush_paged_ivf(&self) -> Result<(), QueryError> {
        match self {
            VectorGraph::Ivf(i) => i.flush(),
            _ => Ok(()),
        }
    }

    /// Return up to `candidate_limit` exact-ordered candidates while keeping
    /// the exploration and re-rank pools independent. The executor rechecks
    /// heap MVCC; storage errors are propagated to its explicit fallback.
    pub(crate) fn search(
        &self,
        query: &[f32],
        request: VectorSearchRequest,
    ) -> Result<VectorSearchResult, QueryError> {
        // A short result proves exhaustion only if widening `ef` cannot reach
        // more live nodes. The slab search keeps tombstones out of its result
        // pool, so it stops early only after draining its reachable frontier.
        // Paged traversal still lets tombstones fill the pool; it is exhausted
        // only once `ef` already spans every node.
        let (candidates, exhausted) = match (self, request.effort) {
            (VectorGraph::Memory(g), VectorSearchEffort::Hnsw { ef }) => {
                let mut candidates =
                    g.search(query, request.rerank_limit.max(request.candidate_limit), ef);
                candidates.truncate(request.candidate_limit);
                let exhausted = candidates.len() < request.candidate_limit;
                (candidates, exhausted)
            }
            (VectorGraph::Paged(p), VectorSearchEffort::Hnsw { ef }) => {
                let ef = ef.max(request.rerank_limit);
                let candidates = p.search(query, request.candidate_limit, ef)?;
                let exhausted = candidates.len() < request.candidate_limit
                    && ef.max(request.candidate_limit) as u64 >= p.node_count();
                (candidates, exhausted)
            }
            (VectorGraph::Ivf(i), VectorSearchEffort::Ivf { nprobe }) => {
                return i.try_search_result(query, request.candidate_limit, nprobe);
            }
            _ => {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "vector search effort does not match index algorithm",
                ));
            }
        };
        Ok(VectorSearchResult {
            candidates,
            exact_order: true,
            exhausted,
        })
    }

    /// Tombstone every node keyed by `row_id`. Returns the count newly marked.
    /// A failed paged write invalidates the file exactly like [`Self::insert`].
    pub(crate) fn mark_deleted_by_row_id(&self, row_id: RowId) -> Result<u32, QueryError> {
        match self {
            VectorGraph::Memory(g) => Ok(g.mark_deleted_by_row_id(row_id)),
            VectorGraph::Paged(p) => p
                .mark_deleted_by_row_id(row_id)
                .map_err(|e| Self::invalidate_paged(p, "delete", e)),
            VectorGraph::Ivf(i) => Ok(i.mark_deleted_by_row_id(row_id)),
        }
    }

    fn invalidate_paged(graph: &PagedHnsw, action: &str, err: QueryError) -> QueryError {
        log_message(format!(
            "hnsw(paged): {action} failed; index will be rebuilt from the heap: {err:?}"
        ));
        if let Err(meta_err) = graph.file().invalidate_build(0) {
            log_message(format!(
                "hnsw(paged): could not invalidate index after failed {action}: {meta_err:?}"
            ));
        }
        err
    }

    /// Whether superseded versions of a row may stay live until the MVCC
    /// purge horizon passes (both HNSW backends). IVF retires them at commit.
    pub(crate) fn supports_versions(&self) -> bool {
        matches!(self, VectorGraph::Memory(_) | VectorGraph::Paged(_))
    }

    /// Opaque handles of `row_id`'s live nodes (slab: node sequence; paged:
    /// vid), captured at commit and later passed to [`Self::retire_versions`].
    pub(crate) fn live_versions(&self, row_id: RowId) -> Result<SmallVec<[u64; 1]>, QueryError> {
        match self {
            VectorGraph::Memory(g) => Ok(g.live_versions(row_id)),
            VectorGraph::Paged(p) => p
                .live_versions(row_id)
                .map_err(|e| Self::invalidate_paged(p, "version lookup", e)),
            VectorGraph::Ivf(_) => Ok(SmallVec::new()),
        }
    }

    /// Tombstone exactly the captured versions of `row_id`.
    pub(crate) fn retire_versions(
        &self,
        row_id: RowId,
        versions: &[u64],
    ) -> Result<u32, QueryError> {
        match self {
            VectorGraph::Memory(g) => Ok(g.retire_versions(row_id, versions)),
            VectorGraph::Paged(p) => p
                .retire_versions(row_id, versions)
                .map_err(|e| Self::invalidate_paged(p, "purge", e)),
            VectorGraph::Ivf(i) => Ok(i.mark_deleted_by_row_id(row_id)),
        }
    }

    pub(crate) fn has_live_row_id(&self, row_id: RowId) -> bool {
        match self {
            VectorGraph::Memory(g) => g.has_live_row_id(row_id),
            VectorGraph::Paged(p) => p.has_live_row_id(row_id).unwrap_or(false),
            VectorGraph::Ivf(i) => i.has_live_row_id(row_id),
        }
    }

    /// Drift-adaptive in-place vector overwrite. When the new vector barely
    /// moved (an incremental embedding refresh), both backends overwrite the
    /// node's vector in place — keeping its id + edges — and return `true`,
    /// skipping the dominant re-link cost. A large move (or non-L2 / cold /
    /// disabled) returns `false`, so the caller falls back to the tombstone +
    /// re-insert path. A paged read/write error degrades to `false` (re-link).
    pub(crate) fn try_update_in_place(
        &self,
        row_id: RowId,
        new_vector: &[f32],
        ratio: f32,
    ) -> bool {
        match self {
            VectorGraph::Memory(g) => g.try_update_in_place(row_id, new_vector, ratio),
            VectorGraph::Paged(p) => p
                .try_update_in_place(row_id, new_vector, ratio)
                .unwrap_or_else(|e| {
                    log_message(format!(
                        "hnsw(paged): in-place update failed (re-link): {e:?}"
                    ));
                    false
                }),
            VectorGraph::Ivf(i) => i.try_update_in_place(row_id, new_vector, ratio),
        }
    }

    /// Compact away tombstones (slab and IVF). Paged graphs reclaim in place
    /// via [`PagedHnsw::repair_tombstones`] instead, so this returns 0 for them.
    pub(crate) fn compact(&self, threads: usize) -> u32 {
        match self {
            VectorGraph::Memory(g) => g.compact_with_threads(threads),
            VectorGraph::Paged(_) => 0,
            VectorGraph::Ivf(i) => i.compact(),
        }
    }
}
