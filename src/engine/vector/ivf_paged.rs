//! Paged IVF_SQ8 backend — the buffer-pool-resident, WAL-crash-safe,
//! larger-than-memory sibling of [`super::ivf::MemoryIvf`].
//!
//! [`PagedIvf`] is a thin orchestration layer over the page store
//! [`crate::storage::IvfIndexFile`] (`.ivf`): the coarse-quantizer centroids
//! and list directory live in buffer-pool pages (loaded resident — both are
//! `O(nlist)`), while the inverted lists themselves are demand-paged page
//! chains, so an index far larger than RAM can be queried by touching only the
//! `nprobe` probed lists. Every mutation is a physical WAL `PageUpdate`, so the
//! `.ivf` file *is* the durable index (no snapshot machinery, recovered by the
//! generic `redo_page_update` path).
//!
//! All the vector logic — k-means training, SQ8 quantization, centroid
//! probing, list scoring, the top-k collector — is shared with the memory
//! backend via free functions in [`super::ivf`], so the two backends quantize
//! and score identically and the memory backend stays the differential oracle.
//!
//! # Correctness contract
//! Identical to HNSW / the memory IVF: the index is a candidate *generator*.
//! `search` returns an approximate superset ordered by SQ8 (or exact-f32)
//! distance, and the executor re-reads every candidate under MVCC and re-ranks
//! by the live heap f32 vector. Quantization, centroid drift, a half-applied
//! incremental insert, or a skipped corrupt page therefore only ever cost
//! recall — never correctness.

#[cfg(test)]
use super::distance::l2_ssd_i8;
use super::distance::{self, dot_i8_batch, quantize_into, ssq_i8, Metric};
use super::hnsw::{HnswIndex, ReadOnlyHnswIndex};
use super::ivf::{
    assign_to_centroids, decode_f32_into, f32_as_bytes, ivf_sme_l2_enabled, kmeans,
    nearest_centroid, normalize, probe_centroids, quant_params_from_minmax, training_sample,
    IvfParams, TopK,
};
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::storage::{error, IvfIndexFile, RowId};
use crate::types::QueryError;
use crate::util::logging::log_message;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// The redo-only transaction id used for all physical IVF page writes (mirrors
/// the HNSW paged store's `VEC_DDL_TXN`).
const IVF_DDL_TXN: u64 = 0;
const IVF_NAV_MIN_ROWS: usize = 100_000;
const IVF_NAV_MIN_DIM: usize = 256;

#[inline]
fn ivf_navigation_enabled(rows: usize, dim: usize) -> bool {
    rows >= IVF_NAV_MIN_ROWS && dim >= IVF_NAV_MIN_DIM
}

#[inline]
fn ivf_navigation_ef(candidate_limit: usize, nprobe: usize) -> usize {
    candidate_limit.max(nprobe.saturating_mul(2).saturating_add(16))
}

/// Ensures every build exit path (including panic unwinding) leaves the file
/// out of deferred-WAL/bulk mode and visibly non-finalized.
struct IvfBuildGuard {
    file: Arc<IvfIndexFile>,
    armed: bool,
}

impl IvfBuildGuard {
    fn new(file: Arc<IvfIndexFile>) -> Self {
        Self { file, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for IvfBuildGuard {
    fn drop(&mut self) {
        if self.armed {
            self.file.abort_build();
        }
    }
}

/// A paged IVF_SQ8 index. Interior-mutable so the `VectorGraph` maintenance
/// hooks (`&self` insert/delete/update) and `&self` search share one handle.
#[derive(Debug)]
pub(crate) struct PagedIvf {
    file: Arc<IvfIndexFile>,
    metric: Metric,
    params: IvfParams,
    /// Lists store raw f32 vectors (Dot, or the opt-in SME-L2 mode) rather than
    /// SQ8 codes. Fixed by the file's persisted layout.
    stores_f32: bool,
    /// L2 + f32 lists => score with the SME batch kernel (NEON fallback).
    sme_l2: bool,
    /// `row_id -> (list_id, page_id, slot)` reverse index for DELETE/UPDATE.
    /// Finalized files prepare it before handle publication; cold builds
    /// publish it alongside their lists, then DML maintains it incrementally.
    row_index: parking_lot::RwLock<Option<HashMap<RowId, (u32, u64, u16)>>>,
    /// Serialises reverse-index publication with incremental maintenance.
    /// Without this lock an INSERT could land on an already-scanned page while
    /// `ensure_row_index` was still building its map, then be absent from the
    /// published map forever.
    maintenance_lock: parking_lot::RwLock<()>,
    /// Sticky failure marker: searches fail closed and trigger the executor's
    /// exact heap fallback until a successful rebuild replaces this instance.
    needs_rebuild: AtomicBool,
    pending_commits: parking_lot::Mutex<std::collections::HashSet<u64>>,
    /// Optional governed HNSW navigation accelerator. IVF list pages remain
    /// the durable index and correctness fallback; this derived cache only
    /// replaces the O(entries_in_nprobe * dim) candidate-generation scan.
    /// It is discarded on any maintenance/allocation failure, after which
    /// searches transparently use the ordinary IVF_SQ8 lists.
    navigator: parking_lot::RwLock<Option<Arc<IvfNavigator>>>,
}

#[derive(Debug)]
pub(crate) struct IvfNavigator {
    graph: ReadOnlyHnswIndex,
    /// Charges the SQ8 arena (`codes.capacity + norms.capacity`). HNSW topology
    /// and any exact vectors carry their existing `HnswIndex` reservation.
    _quant_charge: ControlledMemoryCharge,
}

impl PagedIvf {
    /// Wrap an open `.ivf` file, reading its parameters from the meta page.
    pub(crate) fn new(file: Arc<IvfIndexFile>, params: IvfParams) -> Self {
        Self::open(file, params, true)
    }

    pub(crate) fn new_for_replica(file: Arc<IvfIndexFile>, params: IvfParams) -> Self {
        // Replicas never mutate entries through row-id DML hooks. Avoid a
        // whole-file reverse-map scan after every physical redo invalidation.
        Self::open(file, params, false)
    }

    fn open(file: Arc<IvfIndexFile>, params: IvfParams, prepare_row_index: bool) -> Self {
        let meta = file.meta();
        let metric = match meta.metric {
            1 => Metric::Cosine,
            2 => Metric::Dot,
            _ => Metric::L2,
        };
        let stores_f32 = file.stores_f32();
        let index = PagedIvf {
            file,
            metric,
            params,
            stores_f32,
            sme_l2: metric == Metric::L2 && stores_f32,
            row_index: parking_lot::RwLock::new(None),
            maintenance_lock: parking_lot::RwLock::new(()),
            needs_rebuild: AtomicBool::new(false),
            pending_commits: parking_lot::Mutex::new(std::collections::HashSet::new()),
            navigator: parking_lot::RwLock::new(None),
        };
        // Finish reverse-map warmup before publishing the handle to DML.
        if prepare_row_index && index.is_finalized() {
            if let Err(e) = index.ensure_row_index_locked() {
                log_message(format!("ivf(paged): reverse-map warmup failed: {e:?}"));
                index.needs_rebuild.store(true, Ordering::Release);
            }
        }
        index
    }

    /// The on-disk payload layout a fresh `.ivf` file should be created with:
    /// `true` (raw f32 lists) for Dot or the opt-in SME-L2 mode, else SQ8.
    pub(crate) fn stores_f32_for(metric: Metric) -> bool {
        metric == Metric::Dot || ivf_sme_l2_enabled(metric)
    }

    pub(crate) fn file(&self) -> &Arc<IvfIndexFile> {
        &self.file
    }

    /// Clone the optional derived navigator across an atomic `.ivf` rename /
    /// reopen. The navigator owns no file handles, so its lifetime is
    /// independent of the temporary build pathname.
    pub(crate) fn navigator(&self) -> Option<Arc<IvfNavigator>> {
        self.navigator.read().clone()
    }

    pub(crate) fn install_navigator(&self, navigator: Option<Arc<IvfNavigator>>) {
        *self.navigator.write() = navigator;
    }

    pub(crate) fn has_navigator(&self) -> bool {
        self.navigator.read().is_some()
    }

    #[cfg(test)]
    pub(crate) fn install_navigator_for_test(&self, graph: Arc<HnswIndex>) {
        let charge =
            ControlledMemoryCharge::try_new_optional(ControlledMemoryCategory::DerivedCache, 0)
                .expect("zero-byte navigator test charge");
        self.install_navigator(Some(Arc::new(IvfNavigator {
            graph: ReadOnlyHnswIndex::new(Arc::try_unwrap(graph).expect("owned test navigator")),
            _quant_charge: charge,
        })));
    }

    pub(crate) fn dim(&self) -> usize {
        self.file.dim()
    }

    pub(crate) fn node_count(&self) -> usize {
        self.file.total_entries() as usize
    }

    pub(crate) fn live_count(&self) -> usize {
        self.file.live_entries() as usize
    }

    pub(crate) fn stale_count(&self) -> u32 {
        self.file.stale_entries().min(u32::MAX as u64) as u32
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.file.live_entries() == 0
    }

    pub(crate) fn nlist(&self) -> usize {
        self.file.nlist()
    }

    pub(crate) fn inserts_since_train(&self) -> u64 {
        self.file.inserts_since_train()
    }

    pub(crate) fn runtime_storage_stats(&self) -> (u64, u64, u64, u64, u64, bool) {
        let (list_avg, list_max, list_p95) = self.file.list_length_stats();
        let sq8_nodes = if self.stores_f32 {
            0
        } else {
            self.file.live_entries()
        };
        (
            self.file.estimated_file_size_bytes(),
            sq8_nodes,
            list_avg,
            list_max,
            list_p95,
            self.file.is_finalized(),
        )
    }

    /// Whether the file already holds a fully-built, durably-finalized index.
    /// A non-finalized non-empty file is an interrupted cold build and must be
    /// rebuilt from the heap upstream.
    pub(crate) fn invalidate_replicated_pages(&self) {
        self.needs_rebuild.store(true, Ordering::Release);
        self.invalidate_navigator();
    }

    pub(crate) fn is_finalized(&self) -> bool {
        self.file.is_finalized()
    }

    pub(crate) fn begin_transaction_delta(&self, txn_id: u64) -> Result<(), QueryError> {
        let mut pending = self.pending_commits.lock();
        self.file.mark_unfinalized(txn_id)?;
        pending.insert(txn_id);
        Ok(())
    }

    pub(crate) fn finish_committed_delta(&self, txn_id: u64) -> Result<(), QueryError> {
        let mut pending = self.pending_commits.lock();
        if !pending.remove(&txn_id) {
            return Ok(());
        }
        self.ensure_searchable()?;
        if pending.is_empty() {
            self.file.mark_finalized(IVF_DDL_TXN)?;
        }
        Ok(())
    }

    /// Prepare a query vector for the index's training space: unit-normalized
    /// for Cosine, raw otherwise.
    fn prep_query(&self, query: &[f32]) -> Vec<f32> {
        if self.metric == Metric::Cosine {
            normalize(query)
        } else {
            query.to_vec()
        }
    }

    /// Encode one prepared vector into a list-page payload: SQ8 codes, or its
    /// raw little-endian f32 bytes for the f32 backends.
    fn encode_payload(&self, prepped: &[f32], mid: f32, inv_scale: f32) -> Vec<u8> {
        if self.stores_f32 {
            f32_as_bytes(prepped).to_vec()
        } else {
            let mut q = vec![0i8; prepped.len()];
            quantize_into(prepped, mid, inv_scale, &mut q);
            // i8 -> u8 reinterpret (same layout).
            q.iter().map(|&c| c as u8).collect()
        }
    }

    /// Cold build (or full retrain) from a materialized `(RowId, vector)` batch:
    /// train the coarse quantizer, lay down centroids + directory, then assign
    /// and append every row. Runs inside a deferred-WAL build; a scan/build
    /// failure leaves the file non-finalized (rebuilt next open). Best-effort:
    /// on any error the index simply stays a rebuildable candidate generator.
    pub(crate) fn build_from_rows(&self, rows: &[(RowId, Vec<f32>)]) -> Result<(), QueryError> {
        let _maintenance = self.maintenance_lock.write();
        let mut build_guard = IvfBuildGuard::new(Arc::clone(&self.file));
        let result = self.build_from_rows_inner(rows);
        if result.is_ok() {
            build_guard.disarm();
            self.needs_rebuild.store(false, Ordering::Release);
            self.rebuild_navigator(rows);
        }
        result
    }

    pub(crate) fn navigator_enabled() -> bool {
        std::env::var("MYDB_IVF_NAVIGATOR")
            .is_ok_and(|v| crate::util::env::parse_bool(&v) == Some(true))
    }

    pub(crate) fn rebuild_navigator(&self, rows: &[(RowId, Vec<f32>)]) {
        if !Self::navigator_enabled() {
            return;
        }
        const NAV_M: usize = 8;
        const NAV_EF_CONSTRUCTION: usize = 64;
        if !ivf_navigation_enabled(rows.len(), self.file.dim()) {
            *self.navigator.write() = None;
            return;
        }
        let quant_bytes = rows
            .len()
            .saturating_mul(self.file.dim())
            .saturating_add(rows.len().saturating_mul(std::mem::size_of::<i32>()));
        let Some(quant_charge) = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::DerivedCache,
            quant_bytes,
        ) else {
            log_message("ivf(paged): navigation accelerator skipped: no governed SQ8 budget");
            *self.navigator.write() = None;
            return;
        };
        let navigator = HnswIndex::new(self.metric, NAV_M, NAV_EF_CONSTRUCTION);
        for (rid, vector) in rows {
            if vector.len() != self.file.dim() {
                continue;
            }
            if let Err(build_error) = navigator.append_node(*rid, vector.clone()) {
                log_message(format!(
                    "ivf(paged): navigation accelerator skipped: {build_error:?}"
                ));
                *self.navigator.write() = None;
                return;
            }
        }
        let threads = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1)
            .clamp(1, 16);
        navigator.link_all(threads);
        *self.navigator.write() = Some(Arc::new(IvfNavigator {
            graph: ReadOnlyHnswIndex::new(navigator),
            _quant_charge: quant_charge,
        }));
    }

    fn invalidate_navigator(&self) {
        self.navigator.write().take();
    }

    fn build_from_rows_inner(&self, rows: &[(RowId, Vec<f32>)]) -> Result<(), QueryError> {
        let prepared: Vec<_> = rows
            .iter()
            .map(|(rid, vector)| (*rid, self.prep_query(vector)))
            .collect();
        self.build_prepared_inner(&prepared, None)
    }

    pub(crate) fn spill_build_scratch_bytes(&self, rows: usize) -> usize {
        let nlist = if self.params.nlist > 0 {
            self.params.nlist
        } else {
            (rows as f64).sqrt().round() as usize
        }
        .clamp(1, rows.max(1));
        rows.saturating_mul(128)
            .saturating_add(nlist.saturating_mul(self.dim()).saturating_mul(16))
            .saturating_add(16 * 1024 * 1024)
    }

    pub(crate) fn prepare_build_vector(&self, vector: &[f32]) -> Vec<f32> {
        self.prep_query(vector)
    }

    pub(crate) fn build_from_spilled_rows(
        &self,
        rows: &[(RowId, &[f32])],
        data_dir: &std::path::Path,
    ) -> Result<(), QueryError> {
        let _maintenance = self.maintenance_lock.write();
        let mut guard = IvfBuildGuard::new(Arc::clone(&self.file));
        self.build_prepared_inner(rows, Some(data_dir))?;
        guard.disarm();
        self.needs_rebuild.store(false, Ordering::Release);
        Ok(())
    }

    fn build_prepared_inner<V: AsRef<[f32]> + Sync>(
        &self,
        rows: &[(RowId, V)],
        spill_dir: Option<&std::path::Path>,
    ) -> Result<(), QueryError> {
        let dim = self.file.dim();
        if dim == 0 {
            // Dimensionless empty index: mark finalized-empty so it is trusted.
            self.file.begin_build();
            return self.file.finalize_build(IVF_DDL_TXN);
        }
        let usable: Vec<&(RowId, V)> = rows
            .iter()
            .filter(|(_, v)| v.as_ref().len() == dim)
            .collect();
        let n = usable.len();
        self.file.begin_build();
        if n == 0 {
            // Trained-but-empty: still lay down a trivial quantizer so INSERT
            // has centroids to assign against.
            let centroids = vec![0.0f32; dim];
            self.file.init_trained(
                &centroids,
                1,
                self.effective_nprobe_default(),
                0.0,
                1.0,
                1.0,
            )?;
            return self.file.finalize_build(IVF_DDL_TXN);
        }

        let normalize_rows = self.metric == Metric::Cosine;
        let prepared: Vec<&[f32]> = usable.iter().map(|(_, v)| v.as_ref()).collect();

        let nlist = (if self.params.nlist > 0 {
            self.params.nlist
        } else {
            (n as f64).sqrt().round() as usize
        })
        .clamp(1, n);

        // Global SQ8 quantization params over the (normalized) training space.
        let (mut vmin, mut vmax) = (f32::INFINITY, f32::NEG_INFINITY);
        for v in &prepared {
            for &x in v.iter() {
                vmin = vmin.min(x);
                vmax = vmax.max(x);
            }
        }
        let (mid, inv_scale, dist_mul) = quant_params_from_minmax(self.metric, vmin, vmax);

        // Train the coarse quantizer on a strided sample.
        let sample_target = self
            .params
            .train_sample_mult
            .saturating_mul(nlist)
            .max(nlist);
        let sample_indices = training_sample(n, sample_target);
        // Preserve the exact sample and its order. The external route packs
        // it contiguously so each Lloyd iteration reads sequential file pages.
        let mapped_sample = if let Some(dir) = spill_dir {
            let mut spill = super::spill_rows::VectorRowSpill::new(dir, dim)?;
            for &i in &sample_indices {
                spill.push(usable[i].0, prepared[i])?;
            }
            Some(spill.finish()?)
        } else {
            None
        };
        let sample_rows = mapped_sample.as_ref().map(|sample| sample.rows());
        let sample: Vec<&[f32]> = if let Some(rows) = sample_rows.as_ref() {
            rows.iter().map(|(_, vector)| *vector).collect()
        } else {
            sample_indices.iter().map(|&i| prepared[i]).collect()
        };
        let centroids = kmeans(
            &sample,
            nlist,
            dim,
            self.params.kmeans_iters,
            normalize_rows,
        );

        drop(sample);
        drop(sample_rows);
        drop(mapped_sample);
        drop(sample_indices);

        self.file.init_trained(
            &centroids,
            nlist,
            self.effective_nprobe_default(),
            mid,
            inv_scale,
            dist_mul,
        )?;

        // Assign every row to its nearest centroid (parallel for large sets),
        // then append **list by list** so each inverted list occupies a
        // contiguous run of pages (the allocator hands out consecutive ids).
        // Contiguous lists turn the demand-paged list scan into a sequential
        // disk read and improve cache locality warm — the query only ever scans
        // whole lists. (Appending in row order would interleave every list.)
        let assign = assign_to_centroids(&prepared, &centroids, nlist, dim, self.metric);
        let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); nlist];
        for (i, &c) in assign.iter().enumerate() {
            buckets[c as usize].push(i);
        }
        for (c, bucket) in buckets.iter().enumerate() {
            for chunk in bucket.chunks(self.file.entries_per_list_page()) {
                let page_entries: Vec<(RowId, Vec<u8>)> = chunk
                    .iter()
                    .map(|&i| {
                        (
                            usable[i].0,
                            self.encode_payload(&prepared[i], mid, inv_scale),
                        )
                    })
                    .collect();
                self.file
                    .append_list_page_bulk(IVF_DDL_TXN, c, &page_entries)?;
            }
        }
        self.file.finalize_build(IVF_DDL_TXN)?;
        // Publish a complete reverse index while the just-built pages are hot.
        // Subsequent DELETE/UPDATE is O(1) and never pays a surprise first-use
        // full scan in the foreground.
        *self.row_index.write() = None;
        self.ensure_row_index_locked()?;
        Ok(())
    }

    fn effective_nprobe_default(&self) -> u32 {
        self.params.nprobe_default.max(1) as u32
    }

    /// Insert one row into a warm (trained) index. A no-op on an untrained
    /// index — the row is already in the heap and the next cold build includes
    /// it. Best-effort: a write error is logged and swallowed.
    pub(crate) fn insert(&self, rid: RowId, vector: &[f32]) {
        if let Err(e) = self.insert_inner(rid, vector) {
            self.needs_rebuild.store(true, Ordering::Release);
            log_message(format!(
                "ivf(paged): insert failed; rebuild required: {e:?}"
            ));
            return;
        }
        // Index hooks run before transaction outcome is known. A derived graph
        // cannot safely tombstone/append here because rollback would leave its
        // candidate set incomplete. Drop it and retain the durable IVF oracle;
        // the next full rebuild/OPTIMIZE republishes an accelerator.
        self.invalidate_navigator();
    }

    fn insert_inner(&self, rid: RowId, vector: &[f32]) -> Result<(), QueryError> {
        let _maintenance = self.maintenance_lock.read();
        if !self.file.is_trained() || self.file.dim() == 0 || vector.len() != self.file.dim() {
            return Ok(());
        }
        let centroids = self.file.centroids();
        let nlist = self.file.nlist();
        if nlist == 0 || centroids.len() < nlist * self.file.dim() {
            return Ok(());
        }
        let prepped = self.prep_query(vector);
        let c = nearest_centroid(&centroids, nlist, self.file.dim(), &prepped, self.metric);
        let (mid, inv_scale, _) = self.file.quant_params();
        let payload = self.encode_payload(&prepped, mid, inv_scale);
        let old_loc = self
            .row_index
            .read()
            .as_ref()
            .and_then(|map| map.get(&rid).copied());
        let (page_id, slot) = self.file.append_entry(IVF_DDL_TXN, c, &payload, rid)?;
        // Publish the new location only after the page append succeeds.  If a
        // duplicate RowId is already known, retire the old location exactly;
        // a failure leaves a harmless duplicate candidate rather than losing
        // the only live candidate.
        if let Some((_old_list, old_page, old_slot)) = old_loc {
            self.file
                .set_tombstone(IVF_DDL_TXN, old_page, old_slot, true)?;
        }
        if let Some(map) = self.row_index.write().as_mut() {
            map.insert(rid, (c as u32, page_id, slot));
        }
        Ok(())
    }

    /// Materialize the `row_id -> (list, page, slot)` reverse index (idempotent).
    #[cfg(test)]
    pub(crate) fn row_index_ready_for_test(&self) -> bool {
        self.row_index.read().is_some()
    }

    fn ensure_row_index_locked(&self) -> Result<(), QueryError> {
        if self.row_index.read().is_some() {
            return Ok(());
        }
        let mut map: HashMap<RowId, (u32, u64, u16)> = HashMap::new();
        self.file.for_each_entry(|rid, list_id, page_id, slot| {
            map.insert(rid, (list_id, page_id, slot));
        })?;
        let mut guard = self.row_index.write();
        if guard.is_none() {
            *guard = Some(map);
        }
        Ok(())
    }

    /// Tombstone the entry keyed by `row_id`. Returns 1 if newly marked.
    pub(crate) fn mark_deleted_by_row_id(&self, rid: RowId) -> u32 {
        let deleted = self.mark_deleted_inner(rid).unwrap_or_else(|e| {
            self.needs_rebuild.store(true, Ordering::Release);
            log_message(format!(
                "ivf(paged): delete failed; rebuild required: {e:?}"
            ));
            0
        });
        if deleted > 0 {
            self.invalidate_navigator();
        }
        deleted
    }

    pub(crate) fn has_live_row_id(&self, rid: RowId) -> bool {
        let _maintenance = self.maintenance_lock.write();
        if self.ensure_row_index_locked().is_err() {
            self.needs_rebuild.store(true, Ordering::Release);
            return false;
        }
        self.row_index
            .read()
            .as_ref()
            .is_some_and(|map| map.contains_key(&rid))
    }

    fn mark_deleted_inner(&self, rid: RowId) -> Result<u32, QueryError> {
        let _maintenance = self.maintenance_lock.write();
        self.ensure_row_index_locked()?;
        let loc = {
            let guard = self.row_index.read();
            guard.as_ref().and_then(|m| m.get(&rid).copied())
        };
        let Some((_list, page_id, slot)) = loc else {
            return Ok(0);
        };
        let was_deleted = self.file.set_tombstone(IVF_DDL_TXN, page_id, slot, true)?;
        // Remove the reverse entry only after the physical page mutation has
        // succeeded, so a transient I/O error remains retryable.
        if let Some(map) = self.row_index.write().as_mut() {
            if map.get(&rid).copied() == Some((_list, page_id, slot)) {
                map.remove(&rid);
            }
        }
        if was_deleted {
            Ok(0) // was already tombstoned
        } else {
            Ok(1)
        }
    }

    /// Drift-adaptive in-place update: if the new vector still assigns to the
    /// same centroid, overwrite its payload in place (keep the slot) and return
    /// `true`; otherwise return `false` so the caller falls back to tombstone +
    /// re-insert (which may move the row to a different list). `_ratio` is
    /// accepted for signature parity with HNSW; IVF uses the exact "same
    /// nearest centroid" test.
    pub(crate) fn try_update_in_place(&self, rid: RowId, new_vector: &[f32], _ratio: f32) -> bool {
        let updated = self.try_update_inner(rid, new_vector).unwrap_or_else(|e| {
            self.needs_rebuild.store(true, Ordering::Release);
            log_message(format!(
                "ivf(paged): update failed; rebuild required: {e:?}"
            ));
            false
        });
        if updated {
            self.invalidate_navigator();
        }
        updated
    }

    fn try_update_inner(&self, rid: RowId, new_vector: &[f32]) -> Result<bool, QueryError> {
        let _maintenance = self.maintenance_lock.write();
        if !self.file.is_trained() || self.file.dim() == 0 || new_vector.len() != self.file.dim() {
            return Ok(false);
        }
        self.ensure_row_index_locked()?;
        let loc = {
            let guard = self.row_index.read();
            guard.as_ref().and_then(|m| m.get(&rid).copied())
        };
        let Some((list_id, page_id, slot)) = loc else {
            return Ok(false);
        };
        let centroids = self.file.centroids();
        let nlist = self.file.nlist();
        let prepped = self.prep_query(new_vector);
        let c = nearest_centroid(&centroids, nlist, self.file.dim(), &prepped, self.metric);
        if c as u32 != list_id {
            return Ok(false); // moved cells — let the caller re-insert
        }
        let (mid, inv_scale, _) = self.file.quant_params();
        let payload = self.encode_payload(&prepped, mid, inv_scale);
        self.file
            .overwrite_entry(IVF_DDL_TXN, page_id, slot, &payload)
    }

    /// The paged backend leaves tombstones in place (search skips them);
    /// on-disk reclamation + retrain happens via `OPTIMIZE TABLE`, which
    /// discards + rebuilds the file. So compaction is a no-op here.
    pub(crate) fn compact(&self) -> u32 {
        0
    }

    /// k-NN candidate search. `effort` is nprobe, clamped to `[1, nlist]`.
    /// Returns up to `k` `(RowId, ordering_distance)` ascending — an
    /// approximate superset the executor re-ranks under MVCC. A read error
    /// degrades to an empty candidate set (never an error).
    #[cfg(test)]
    pub(crate) fn search(&self, query: &[f32], k: usize, effort: usize) -> Vec<(RowId, f64)> {
        self.try_search(query, k, effort).unwrap_or_else(|e| {
            log_message(format!("ivf(paged): search failed: {e:?}"));
            Vec::new()
        })
    }

    #[cfg(test)]
    pub(crate) fn try_search(
        &self,
        query: &[f32],
        k: usize,
        effort: usize,
    ) -> Result<Vec<(RowId, f64)>, QueryError> {
        self.ensure_searchable()?;
        self.search_inner(query, k, effort)
            .map(|(rows, _, _)| rows)
            .inspect_err(|_| self.needs_rebuild.store(true, Ordering::Release))
    }

    pub(crate) fn try_search_with_stats(
        &self,
        query: &[f32],
        k: usize,
        effort: usize,
    ) -> Result<(Vec<(RowId, f64)>, usize, bool), QueryError> {
        self.ensure_searchable()?;
        self.search_inner(query, k, effort)
            .inspect_err(|_| self.needs_rebuild.store(true, Ordering::Release))
    }

    fn ensure_searchable(&self) -> Result<(), QueryError> {
        if self.needs_rebuild.load(Ordering::Acquire) {
            Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "ivf index is marked for rebuild after an I/O or structural failure",
            ))
        } else {
            Ok(())
        }
    }

    fn search_inner(
        &self,
        query: &[f32],
        k: usize,
        effort: usize,
    ) -> Result<(Vec<(RowId, f64)>, usize, bool), QueryError> {
        let dim = self.file.dim();
        if !self.file.is_trained() || dim == 0 || query.len() != dim || k == 0 {
            return Ok((Vec::new(), 0, false));
        }
        let nlist = self.file.nlist();
        if nlist == 0 {
            return Ok((Vec::new(), 0, false));
        }
        let centroids = self.file.centroids();
        let nprobe = effort.clamp(1, nlist);
        // Full-probe is the exact-recall escape hatch: every inverted list is
        // visited and the executor re-ranks candidates with the authoritative
        // heap f32 values.  Keeping only SQ8's approximate top-k here can drop
        // a true neighbour before that re-rank, so return every live entry
        // when nprobe == nlist.  Partial-probe queries retain the bounded
        // candidate heap and their normal ANN performance.
        let candidate_limit = if nprobe == nlist {
            self.live_count().max(k)
        } else {
            k
        };

        // Experimental, opt-in navigator. It is disabled by default because
        // the server assembly path has shown severe high-dimensional recall
        // loss; a nonempty candidate pool alone does not establish quality.
        // Full-probe always retains the exhaustive IVF escape hatch.
        if nprobe < nlist {
            let navigator = { self.navigator.read().clone() };
            if let Some(navigator) = navigator {
                let nav_ef = ivf_navigation_ef(candidate_limit, nprobe);
                let candidates = navigator.graph.search(query, candidate_limit, nav_ef);
                if !candidates.is_empty() {
                    return Ok((candidates, nav_ef, true));
                }
            }
        }
        let q = self.prep_query(query);

        // 1) Coarse search: nprobe nearest centroids.
        let probed = probe_centroids(&centroids, nlist, dim, &q, self.metric, nprobe);

        #[cfg(test)]
        let bench_baseline = std::env::var_os("MYDB_IVF_BENCH_BASELINE").is_some();
        #[cfg(not(test))]
        let bench_baseline = false;

        // Cold list pages are laid out contiguously. Submit every proven
        // contiguous probed run before the first demand pin so buffer-pool I/O
        // workers can coalesce and overlap reads. Fragmented incremental-DML
        // chains simply decline this optional fast path.
        if !bench_baseline {
            self.file.prefetch_lists(&probed);
        }

        // 2) Scan the probed lists' page chains, scoring each live entry.
        let (mid, inv_scale, dist_mul) = self.file.quant_params();
        let q_codes: Vec<i8> = if self.stores_f32 {
            Vec::new()
        } else {
            let mut buf = vec![0i8; dim];
            quantize_into(&q, mid, inv_scale, &mut buf);
            buf
        };
        let qnorm = if self.stores_f32 { 0 } else { ssq_i8(&q_codes) };
        let q_bytes: Vec<u8> = if self.sme_l2 {
            f32_as_bytes(&q).to_vec()
        } else {
            Vec::new()
        };

        let mut heap = TopK::new(candidate_limit);
        let mut scratch: Vec<f32> = Vec::with_capacity(dim);
        let mut sme_out: Vec<f32> = Vec::new();
        let mut sq8_dots: Vec<i32> = Vec::new();
        let mut entries_scanned = 0usize;
        let scan_charge = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::StorageScratch,
            self.file.scan_scratch_bytes(),
        );
        let mut scan_batch = vec![0u8; scan_charge.as_ref().map_or(0, |charge| charge.bytes())];

        for &list_id in &probed {
            let head = self.file.list_head(list_id);
            if head == 0 {
                continue;
            }
            self.file
                .for_each_list_page(list_id, head, &mut scan_batch, |pref| {
                    let count = pref.count;
                    entries_scanned = entries_scanned.saturating_add(count);
                    if self.sme_l2 {
                        // One batched L2 over the page's contiguous f32 run; NEON
                        // per-row fallback when SME is unavailable / misaligned.
                        sme_out.clear();
                        sme_out.resize(count, 0.0);
                        let ok = distance::l2_sq_sme_batch(&q_bytes, pref.codes, &mut sme_out);
                        if !ok {
                            for (s, o) in sme_out.iter_mut().enumerate() {
                                decode_f32_into(&pref.codes[s * dim * 4..], dim, &mut scratch);
                                *o = distance::l2_sq(&q, &scratch);
                            }
                        }
                        for s in 0..count {
                            if !pref.tomb_at(s) {
                                heap.push(sme_out[s], pref.rid_at(s));
                            }
                        }
                    } else if self.stores_f32 {
                        // Dot: exact f32 inner-product ordering.
                        for s in 0..count {
                            if pref.tomb_at(s) {
                                continue;
                            }
                            decode_f32_into(&pref.codes[s * dim * 4..], dim, &mut scratch);
                            heap.push(
                                distance::ordering_distance(query, &scratch, Metric::Dot),
                                pref.rid_at(s),
                            );
                        }
                    } else {
                        // L2 / Cosine: int8 SSD over the SQ8 codes.
                        let codes_i8: &[i8] = unsafe {
                            std::slice::from_raw_parts(
                                pref.codes.as_ptr().cast::<i8>(),
                                pref.codes.len(),
                            )
                        };
                        if !bench_baseline {
                            sq8_dots.resize(count, 0);
                            dot_i8_batch(&q_codes, codes_i8, dim, &mut sq8_dots);
                        }
                        for s in 0..count {
                            if pref.tomb_at(s) {
                                continue;
                            }
                            #[cfg(test)]
                            let ssd = if bench_baseline {
                                l2_ssd_i8(&q_codes, &codes_i8[s * dim..s * dim + dim])
                            } else {
                                qnorm + pref.norm_at(s) - 2 * sq8_dots[s]
                            };
                            #[cfg(not(test))]
                            let ssd = qnorm + pref.norm_at(s) - 2 * sq8_dots[s];
                            heap.push(dist_mul * (ssd as f32), pref.rid_at(s));
                        }
                    }
                    Ok(())
                })?;
        }

        Ok((heap.into_sorted(), entries_scanned, false))
    }
}

#[cfg(test)]
mod navigation_tests {
    use super::*;

    #[test]
    fn navigation_threshold_avoids_small_or_low_dim_indexes() {
        assert!(!ivf_navigation_enabled(
            IVF_NAV_MIN_ROWS - 1,
            IVF_NAV_MIN_DIM
        ));
        assert!(!ivf_navigation_enabled(
            IVF_NAV_MIN_ROWS,
            IVF_NAV_MIN_DIM - 1
        ));
        assert!(ivf_navigation_enabled(IVF_NAV_MIN_ROWS, IVF_NAV_MIN_DIM));
    }

    #[test]
    fn navigation_effort_is_monotonic_and_candidate_safe() {
        assert_eq!(ivf_navigation_ef(40, 1), 40);
        assert_eq!(ivf_navigation_ef(40, 8), 40);
        assert_eq!(ivf_navigation_ef(40, 32), 80);
        assert_eq!(ivf_navigation_ef(160, 32), 160);
        assert!(ivf_navigation_ef(40, usize::MAX) >= 40);
    }
}
