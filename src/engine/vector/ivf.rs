//! In-memory IVF_SQ8 vector index — an inverted-file ANN with 8-bit scalar
//! quantization, the memory-backend sibling of [`super::hnsw::HnswIndex`].
//!
//! # Structure
//! A coarse quantizer (k-means centroids) partitions the vector space into
//! `nlist` Voronoi cells; each cell owns one inverted list of `(SQ8 code,
//! RowId)` entries. A query scores its distance to every centroid, probes the
//! `nprobe` nearest cells, and scans only those lists — the ANN speedup is the
//! `nprobe / nlist` fraction of vectors actually touched.
//!
//! # Quantization (reuses the HNSW SQ8 kernels)
//! Vectors are quantized with the same global symmetric affine map as the HNSW
//! navigation arena (`quantize_into`, [`super::distance`]): `mid =
//! (vmax+vmin)/2`, `inv_scale = 254/(vmax−vmin)`, `code = round((x−mid)·
//! inv_scale)` clamped to `[-127,127]`. `dist_mul` maps the integer SSD back
//! onto the metric's ordering scale — `scale²` for L2, `scale²·½` for Cosine
//! (whose codes are unit-normalized so the SSD approximates `2(1−cos)`).
//! `Metric::Dot` has no scale-invariant trick, so its lists keep the raw f32
//! vectors and score exact inner products (mirrors the HNSW SQ8 precedent).
//!
//! # Correctness contract (identical to HNSW)
//! The index is a candidate *generator*: `search` returns an approximate
//! superset ordered by SQ8 (or exact-f32-Dot) distance, and the executor
//! re-reads every candidate under MVCC and re-ranks by the live heap f32
//! vector before truncating to `k`. Quantization and centroid drift therefore
//! only ever cost recall, never correctness.

use super::distance::{self, dot_i8_batch, quantize_into, ssq_i8, Metric};
use crate::storage::RowId;
use parking_lot::RwLock;
use std::collections::HashMap;

/// Build/query parameters for one IVF_SQ8 index. `nlist`/`kmeans_iters`/
/// `train_sample_mult` are build-time; `nprobe_default` is the query-time
/// fallback when neither hint nor session GUC overrides it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct IvfParams {
    /// Number of inverted lists (k-means centroids). `0` = auto (`√N`, clamped).
    pub nlist: usize,
    /// Default query-time cells to probe.
    pub nprobe_default: usize,
    /// k-means iterations at build/retrain.
    pub kmeans_iters: usize,
    /// Training sample size multiplier: sample `min(N, mult·nlist)` rows.
    pub train_sample_mult: usize,
}

impl Default for IvfParams {
    fn default() -> Self {
        IvfParams {
            nlist: 0,
            nprobe_default: 8,
            kmeans_iters: 15,
            train_sample_mult: 256,
        }
    }
}

/// One inverted list (all vectors assigned to one centroid). For L2/Cosine
/// `codes` holds the row-major SQ8 codes (`len = count·dim`) and `fvecs` is
/// empty; for Dot `fvecs` holds the raw f32 vectors and `codes` is empty.
/// Deleted entries are tombstoned in `tomb` (search skips them, `compact`
/// reclaims them).
#[derive(Debug, Default, Clone)]
struct InvList {
    codes: Vec<i8>,
    norms: Vec<i32>,
    fvecs: Vec<f32>,
    rids: Vec<RowId>,
    tomb: Vec<bool>,
    live: usize,
}

impl InvList {
    #[inline]
    fn count(&self) -> usize {
        self.rids.len()
    }
}

/// Mutable index state behind the [`MemoryIvf`] lock.
#[derive(Debug, Default)]
struct IvfInner {
    revision: u64,
    /// Vector dimension, fixed by the first build (`0` until then).
    dim: usize,
    /// Centroids, row-major f32 (`nlist·dim`). For Cosine these are unit-ish
    /// (trained on normalized vectors).
    centroids: Vec<f32>,
    nlist: usize,
    // Global SQ8 quantization parameters (over the training set).
    mid: f32,
    inv_scale: f32,
    dist_mul: f32,
    lists: Vec<InvList>,
    /// `row_id → (list_index, slot)` for O(1) delete / in-place update.
    rmap: HashMap<RowId, (u32, u32)>,
    /// Total entries including tombstones.
    total: usize,
    /// Tombstoned entries.
    stale: usize,
    /// Whether a coarse quantizer has been trained (an empty index is untrained).
    trained: bool,
    /// Rows inserted since the last (re)train — a centroid-drift signal.
    inserts_since_train: u64,
}

/// Optional SME acceleration policy for the L2 list scan. `MYDB_IVF_SME`:
/// `on`/`1`/`true` or `auto` enable it when the OS advertises SME; `off`
/// (default) keeps the SQ8 int8 path. SME needs an OS-managed architectural
/// context, so hardware presence alone is not enough — we reuse the brute
/// path's `brute_sme_available()` capability probe.
///
/// When enabled (L2 only), the inverted lists store **raw f32 vectors** instead
/// of SQ8 codes, and the probed lists are scored one batch per list with the
/// proven `l2_sq_sme_batch` streaming-SVE kernel (NEON `l2_sq` is the fallback
/// and the differential oracle). This trades SQ8's 4× memory saving for
/// SME-batchable exact scoring — an opt-in knob, off by default.
pub(super) fn ivf_sme_l2_enabled(metric: Metric) -> bool {
    if metric != Metric::L2 {
        return false;
    }
    let on = match std::env::var("MYDB_IVF_SME") {
        Ok(v) => {
            v.eq_ignore_ascii_case("on")
                || v == "1"
                || v.eq_ignore_ascii_case("true")
                || v.eq_ignore_ascii_case("auto")
        }
        Err(_) => false,
    };
    on && distance::brute_sme_available()
}

/// Reinterpret an `f32` slice as its little-endian bytes for the SME kernel's
/// byte interface. Sound on the little-endian targets mydb supports (the
/// storage format is already little-endian `f32`).
#[inline]
pub(super) fn f32_as_bytes(v: &[f32]) -> &[u8] {
    // Safety: `f32` has no padding and any bit pattern is a valid `u8`; the
    // resulting slice borrows `v` for the same lifetime and is read-only.
    unsafe { std::slice::from_raw_parts(v.as_ptr().cast::<u8>(), std::mem::size_of_val(v)) }
}

/// An in-memory IVF_SQ8 index. Interior-mutable behind an `RwLock` so the
/// `VectorGraph` maintenance hooks (`&self` insert/delete/update) and the
/// `&self` search share one handle, matching `HnswIndex`.
#[derive(Debug)]
pub(crate) struct MemoryIvf {
    metric: Metric,
    params: IvfParams,
    /// SME-accelerated L2 list scan enabled (see [`ivf_sme_l2_enabled`]). Fixed
    /// at construction; determines whether lists store f32 (SME) or SQ8 codes.
    sme_l2: bool,
    inner: RwLock<IvfInner>,
}

impl MemoryIvf {
    pub(crate) fn new(metric: Metric, params: IvfParams) -> Self {
        MemoryIvf {
            metric,
            params,
            sme_l2: ivf_sme_l2_enabled(metric),
            inner: RwLock::new(IvfInner::default()),
        }
    }

    /// Whether the inverted lists store raw f32 vectors rather than SQ8 codes.
    /// True for the Dot metric (no scale-invariant SQ8 trick) and for the
    /// opt-in SME L2 mode.
    #[inline]
    fn stores_f32(&self) -> bool {
        self.metric == Metric::Dot || self.sme_l2
    }

    #[inline]
    #[allow(dead_code)] // used by M2 retrain / M5 observability
    pub(crate) fn metric(&self) -> Metric {
        self.metric
    }

    pub(crate) fn dim(&self) -> usize {
        self.inner.read().dim
    }

    /// Total entries including tombstones.
    pub(crate) fn node_count(&self) -> usize {
        self.inner.read().total
    }

    /// Live (non-tombstoned) entries.
    pub(crate) fn live_count(&self) -> usize {
        let g = self.inner.read();
        g.total - g.stale
    }

    pub(crate) fn stale_count(&self) -> u32 {
        self.inner.read().stale as u32
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.live_count() == 0
    }

    /// Number of inverted lists (0 until trained).
    #[allow(dead_code)] // surfaced by M5 observability
    pub(crate) fn nlist(&self) -> usize {
        self.inner.read().nlist
    }

    /// Rows inserted since the last (re)train — drives the OPTIMIZE / drift
    /// signal in `information_schema.mydb_vector_indexes`.
    #[allow(dead_code)] // surfaced by M5 observability
    pub(crate) fn inserts_since_train(&self) -> u64 {
        self.inner.read().inserts_since_train
    }

    /// Normalize a query vector into the space the index was trained in: a
    /// unit-length copy for Cosine, the raw vector otherwise.
    fn prep_query(&self, query: &[f32]) -> Vec<f32> {
        if self.metric == Metric::Cosine {
            normalize(query)
        } else {
            query.to_vec()
        }
    }

    /// Cold build (or full rebuild) from a materialized `(RowId, vector)` batch.
    /// Trains the coarse quantizer on a sample, then assigns every row to its
    /// nearest centroid and quantizes it into that list. Replaces any prior
    /// state. Rows whose dimension disagrees with the majority are skipped.
    pub(crate) fn build_from_rows(&self, rows: &[(RowId, Vec<f32>)]) {
        let mut g = self.inner.write();
        g.revision = g.revision.wrapping_add(1);
        let revision = g.revision;
        *g = IvfInner {
            revision,
            ..IvfInner::default()
        };
        // Determine dim from the first non-empty vector.
        let dim = rows
            .iter()
            .find(|(_, v)| !v.is_empty())
            .map(|(_, v)| v.len())
            .unwrap_or(0);
        g.dim = dim;
        if dim == 0 {
            return;
        }
        let usable: Vec<&(RowId, Vec<f32>)> = rows.iter().filter(|(_, v)| v.len() == dim).collect();
        let n = usable.len();
        if n == 0 {
            return;
        }

        // Cosine trains / assigns on unit-normalized vectors; the executor's
        // f32 re-rank is scale-invariant so the raw heap vector still governs
        // the final ordering. Materialize the prepared training vectors **once**
        // (normalize + clone per vector a single time) and reuse them for the
        // quant range, sampling, and assignment — the paged backend already does
        // this; the memory backend previously normalized each Cosine vector 2-3×
        // (quant params + sample + assign).
        let normalize_rows = self.metric == Metric::Cosine;
        let prepared: Vec<Vec<f32>> = usable
            .iter()
            .map(|(_, v)| {
                if normalize_rows {
                    normalize(v)
                } else {
                    v.clone()
                }
            })
            .collect();

        // Resolve nlist: explicit, else ~√N, clamped to [1, n].
        let nlist = (if self.params.nlist > 0 {
            self.params.nlist
        } else {
            (n as f64).sqrt().round() as usize
        })
        .clamp(1, n);
        g.nlist = nlist;

        // Global SQ8 quantization params over the (already prepared) training
        // space.
        let (mut vmin, mut vmax) = (f32::INFINITY, f32::NEG_INFINITY);
        for v in &prepared {
            for &x in v.iter() {
                vmin = vmin.min(x);
                vmax = vmax.max(x);
            }
        }
        let (mid, inv_scale, dist_mul) = quant_params_from_minmax(self.metric, vmin, vmax);
        g.mid = mid;
        g.inv_scale = inv_scale;
        g.dist_mul = dist_mul;

        // Train the coarse quantizer on a strided sample.
        let sample_target = self
            .params
            .train_sample_mult
            .saturating_mul(nlist)
            .max(nlist);
        let sample: Vec<Vec<f32>> = training_sample(n, sample_target)
            .into_iter()
            .map(|i| prepared[i].clone())
            .collect();
        let centroids = kmeans(
            &sample,
            nlist,
            dim,
            self.params.kmeans_iters,
            normalize_rows,
        );
        g.centroids = centroids;
        g.trained = true;

        // Assign every row to its nearest centroid and quantize into the list.
        let mut lists: Vec<InvList> = (0..nlist).map(|_| InvList::default()).collect();
        let mut rmap: HashMap<RowId, (u32, u32)> = HashMap::with_capacity(n);
        let mut qbuf = vec![0i8; dim];
        for i in 0..n {
            let rid = usable[i].0;
            let v = &prepared[i];
            let c = nearest_centroid(&g.centroids, nlist, dim, v, self.metric);
            let list = &mut lists[c];
            let slot = list.rids.len() as u32;
            if self.stores_f32() {
                list.fvecs.extend_from_slice(v);
            } else {
                quantize_into(v, mid, inv_scale, &mut qbuf);
                list.codes.extend_from_slice(&qbuf);
                list.norms.push(ssq_i8(&qbuf));
            }
            list.rids.push(rid);
            list.tomb.push(false);
            list.live += 1;
            rmap.insert(rid, (c as u32, slot));
        }
        g.total = n;
        g.stale = 0;
        g.lists = lists;
        g.rmap = rmap;
        g.inserts_since_train = 0;
    }

    /// Insert one row into a warm index (incremental DML). Assigns to the
    /// nearest existing centroid and appends. A no-op on an untrained index —
    /// the row is already in the heap and the next cold build includes it.
    pub(crate) fn insert(&self, rid: RowId, vector: &[f32]) {
        let mut g = self.inner.write();
        g.revision = g.revision.wrapping_add(1);
        // A vector index created on an empty table is still published into the
        // warm graph cache.  Leaving it untrained made every later INSERT a
        // permanent no-op until restart/OPTIMIZE.  Bootstrap a one-cell coarse
        // quantizer from the first vector; a later OPTIMIZE can expand it to
        // the requested nlist once there is a representative data set.
        if !g.trained {
            if vector.is_empty() {
                return;
            }
            let prepped = if self.metric == Metric::Cosine {
                normalize(vector)
            } else {
                vector.to_vec()
            };
            let (mut vmin, mut vmax) = (f32::INFINITY, f32::NEG_INFINITY);
            for &x in &prepped {
                vmin = vmin.min(x);
                vmax = vmax.max(x);
            }
            let (mid, inv_scale, dist_mul) = quant_params_from_minmax(self.metric, vmin, vmax);
            g.dim = prepped.len();
            g.centroids = prepped;
            g.nlist = 1;
            g.mid = mid;
            g.inv_scale = inv_scale;
            g.dist_mul = dist_mul;
            g.lists = vec![InvList::default()];
            g.trained = true;
        }
        if g.dim == 0 || vector.len() != g.dim {
            return;
        }
        // Overwrite semantics if the row already exists (defensive): tombstone
        // the old entry first so we never double-count a RowId.
        if let Some((li, slot)) = g.rmap.get(&rid).copied() {
            let list = &mut g.lists[li as usize];
            if !list.tomb[slot as usize] {
                list.tomb[slot as usize] = true;
                list.live -= 1;
                g.stale += 1;
            }
        }
        let prepped = if self.metric == Metric::Cosine {
            normalize(vector)
        } else {
            vector.to_vec()
        };
        let c = nearest_centroid(&g.centroids, g.nlist, g.dim, &prepped, self.metric);
        let (mid, inv_scale) = (g.mid, g.inv_scale);
        let dim = g.dim;
        let list = &mut g.lists[c];
        let slot = list.rids.len() as u32;
        if self.stores_f32() {
            list.fvecs.extend_from_slice(&prepped);
        } else {
            let mut qbuf = vec![0i8; dim];
            quantize_into(&prepped, mid, inv_scale, &mut qbuf);
            list.codes.extend_from_slice(&qbuf);
            list.norms.push(ssq_i8(&qbuf));
        }
        list.rids.push(rid);
        list.tomb.push(false);
        list.live += 1;
        g.rmap.insert(rid, (c as u32, slot));
        g.total += 1;
        g.inserts_since_train += 1;
    }

    /// Tombstone every entry keyed by `row_id`. Returns the count newly marked.
    pub(crate) fn mark_deleted_by_row_id(&self, rid: RowId) -> u32 {
        let mut g = self.inner.write();
        g.revision = g.revision.wrapping_add(1);
        let Some((li, slot)) = g.rmap.get(&rid).copied() else {
            return 0;
        };
        let list = &mut g.lists[li as usize];
        if list.tomb[slot as usize] {
            return 0;
        }
        list.tomb[slot as usize] = true;
        list.live -= 1;
        g.stale += 1;
        g.rmap.remove(&rid);
        1
    }

    pub(crate) fn has_live_row_id(&self, rid: RowId) -> bool {
        self.inner.read().rmap.contains_key(&rid)
    }

    /// Drift-adaptive in-place update: if the new vector still assigns to the
    /// same centroid, overwrite its codes/vector in place (keep the slot) and
    /// return `true`; otherwise return `false` so the caller falls back to
    /// tombstone + re-insert (which may move the row to a different list).
    /// `_ratio` is accepted for signature parity with the HNSW backend; IVF
    /// uses the exact "same nearest centroid" test instead.
    pub(crate) fn try_update_in_place(&self, rid: RowId, new_vector: &[f32], _ratio: f32) -> bool {
        let mut g = self.inner.write();
        g.revision = g.revision.wrapping_add(1);
        if !g.trained || g.dim == 0 || new_vector.len() != g.dim {
            return false;
        }
        let Some((li, slot)) = g.rmap.get(&rid).copied() else {
            return false;
        };
        let prepped = if self.metric == Metric::Cosine {
            normalize(new_vector)
        } else {
            new_vector.to_vec()
        };
        let c = nearest_centroid(&g.centroids, g.nlist, g.dim, &prepped, self.metric);
        if c as u32 != li {
            return false; // moved cells — let the caller re-insert
        }
        let (mid, inv_scale, dim) = (g.mid, g.inv_scale, g.dim);
        let s = slot as usize;
        // A previously-tombstoned slot coming back to life restores the live
        // count — adjust the index-level counter before the list mutable borrow.
        if g.lists[li as usize].tomb[s] {
            g.lists[li as usize].tomb[s] = false;
            g.lists[li as usize].live += 1;
            g.stale -= 1;
        }
        let list = &mut g.lists[li as usize];
        if self.stores_f32() {
            list.fvecs[s * dim..s * dim + dim].copy_from_slice(&prepped);
        } else {
            let mut qbuf = vec![0i8; dim];
            quantize_into(&prepped, mid, inv_scale, &mut qbuf);
            list.codes[s * dim..s * dim + dim].copy_from_slice(&qbuf);
            list.norms[s] = ssq_i8(&qbuf);
        }
        true
    }

    /// Reclaim tombstones by rebuilding every list without deleted entries.
    /// Returns the number of tombstones dropped. Keeps the same centroids
    /// (a pure garbage-collect, not a retrain).
    pub(crate) fn compact(&self) -> u32 {
        self.compact_with_observer(|| {})
    }

    fn compact_with_observer(&self, rebuilding: impl FnOnce()) -> u32 {
        let (source, dim, dropped, live, revision) = {
            let g = self.inner.read();
            if g.stale == 0 {
                return 0;
            }
            (
                g.lists.clone(),
                g.dim,
                g.stale as u32,
                g.total - g.stale,
                g.revision,
            )
        };
        rebuilding();
        let dot = self.stores_f32();
        let mut new_rmap: HashMap<RowId, (u32, u32)> = HashMap::with_capacity(live);
        let mut lists = Vec::with_capacity(source.len());
        for (li, list) in source.iter().enumerate() {
            let mut nc: Vec<i8> = Vec::new();
            let mut nn: Vec<i32> = Vec::new();
            let mut nf: Vec<f32> = Vec::new();
            let mut nr: Vec<RowId> = Vec::new();
            for s in 0..list.count() {
                if list.tomb[s] {
                    continue;
                }
                let slot = nr.len() as u32;
                if dot {
                    nf.extend_from_slice(&list.fvecs[s * dim..s * dim + dim]);
                } else {
                    nc.extend_from_slice(&list.codes[s * dim..s * dim + dim]);
                    nn.push(list.norms[s]);
                }
                new_rmap.insert(list.rids[s], (li as u32, slot));
                nr.push(list.rids[s]);
            }
            let live = nr.len();
            lists.push(InvList {
                codes: nc,
                norms: nn,
                fvecs: nf,
                tomb: vec![false; live],
                rids: nr,
                live,
            });
        }
        // Rebuild without holding the search/DML lock. If a writer changed
        // the source, discard this derived result and retry at a later
        // checkpoint rather than overwrite concurrent mutations.
        let mut g = self.inner.write();
        if g.revision != revision {
            drop(g);
            return 0;
        }
        g.revision = g.revision.wrapping_add(1);
        let old_lists = std::mem::replace(&mut g.lists, lists);
        let old_rmap = std::mem::replace(&mut g.rmap, new_rmap);
        g.total -= g.stale;
        g.stale = 0;
        drop(g);
        drop((old_lists, old_rmap));
        dropped
    }

    /// k-nearest-neighbour candidate search. `effort` is the number of cells to
    /// probe (nprobe); it is clamped to `[1, nlist]`. Returns up to `k`
    /// `(RowId, ordering_distance)` ascending — an approximate superset the
    /// executor re-ranks under MVCC.
    pub(crate) fn search(&self, query: &[f32], k: usize, effort: usize) -> Vec<(RowId, f64)> {
        let g = self.inner.read();
        if !g.trained || g.dim == 0 || query.len() != g.dim || k == 0 || g.nlist == 0 {
            return Vec::new();
        }
        let nprobe = effort.max(1).min(g.nlist);
        // With every list probed, preserve the executor's exact-recall
        // contract by handing it all live rows for authoritative f32
        // re-ranking.  SQ8 top-k ordering alone is approximate and can evict
        // a true neighbour before the executor sees it.
        let candidate_limit = if nprobe == g.nlist {
            (g.total - g.stale).max(k)
        } else {
            k
        };
        let q = self.prep_query(query);

        // 1) Coarse search: distance from the query to every centroid, take the
        //    nprobe nearest. `nlist` is small, so a full scan + partial select
        //    is cheap relative to the list scans below.
        let mut cent: Vec<(f64, usize)> = (0..g.nlist)
            .map(|c| {
                let cs = &g.centroids[c * g.dim..c * g.dim + g.dim];
                (distance::ordering_distance(&q, cs, self.metric), c)
            })
            .collect();
        let np = nprobe.min(cent.len());
        cent.select_nth_unstable_by(np - 1, |a, b| a.0.total_cmp(&b.0));
        cent.truncate(np);

        // 2) Scan the probed lists, scoring each live entry. Three modes:
        //    * SME L2 (opt-in): one batched `l2_sq_sme_batch` per list over the
        //      f32 vectors, NEON `l2_sq` as the fallback + differential oracle.
        //    * Dot: exact f32 inner-product ordering over the f32 vectors.
        //    * L2 / Cosine (default): int8 SSD over the SQ8 codes.
        let use_f32 = self.stores_f32();
        let q_codes: Vec<i8> = if use_f32 {
            Vec::new()
        } else {
            let mut buf = vec![0i8; g.dim];
            quantize_into(&q, g.mid, g.inv_scale, &mut buf);
            buf
        };
        let qnorm = if use_f32 { 0 } else { ssq_i8(&q_codes) };
        let dim = g.dim;
        let mut heap = TopK::new(candidate_limit);
        let mut sq8_dots = Vec::new();
        for &(_, c) in &cent {
            let list = &g.lists[c];
            let cnt = list.count();
            if cnt == 0 {
                continue;
            }
            if self.sme_l2 {
                // Batch the whole list (tombstoned rows are scored then skipped
                // on push — cheap relative to a scatter). `false` (SME
                // unavailable / misaligned) drops to the NEON per-row oracle.
                let mut out = vec![0f32; cnt];
                let ok = distance::l2_sq_sme_batch(
                    f32_as_bytes(&q),
                    f32_as_bytes(&list.fvecs),
                    &mut out,
                );
                if !ok {
                    for (s, o) in out.iter_mut().enumerate() {
                        *o = distance::l2_sq(&q, &list.fvecs[s * dim..s * dim + dim]);
                    }
                }
                for s in 0..cnt {
                    if !list.tomb[s] {
                        heap.push(out[s], list.rids[s]);
                    }
                }
            } else if use_f32 {
                for s in 0..cnt {
                    if list.tomb[s] {
                        continue;
                    }
                    let fv = &list.fvecs[s * dim..s * dim + dim];
                    heap.push(
                        distance::ordering_distance(query, fv, Metric::Dot),
                        list.rids[s],
                    );
                }
            } else {
                const DOT_BATCH_ROWS: usize = 256;
                for base in (0..cnt).step_by(DOT_BATCH_ROWS) {
                    let end = (base + DOT_BATCH_ROWS).min(cnt);
                    sq8_dots.resize(end - base, 0);
                    dot_i8_batch(
                        &q_codes,
                        &list.codes[base * dim..end * dim],
                        dim,
                        &mut sq8_dots,
                    );
                    for s in base..end {
                        if list.tomb[s] {
                            continue;
                        }
                        let ssd = qnorm + list.norms[s] - 2 * sq8_dots[s - base];
                        heap.push(g.dist_mul * ssd as f32, list.rids[s]);
                    }
                }
            }
        }
        heap.into_sorted()
    }
}

/// Unit-normalize a vector (zero vector stays zero).
pub(super) fn normalize(v: &[f32]) -> Vec<f32> {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        v.iter().map(|x| x / norm).collect()
    } else {
        v.to_vec()
    }
}

/// Global symmetric SQ8 params `(mid, inv_scale, dist_mul)` from the value
/// range `[vmin, vmax]` of the (optionally normalized) training space. A
/// degenerate range yields an identity-ish map that still produces a valid (if
/// coarse) ordering; the executor's f32 re-rank fixes any imprecision. Shared
/// by the memory and paged backends so both quantize identically.
pub(super) fn quant_params_from_minmax(metric: Metric, vmin: f32, vmax: f32) -> (f32, f32, f32) {
    if !(vmax > vmin) {
        return (vmin.clamp(-1.0, 1.0), 1.0, 1.0);
    }
    let mid = (vmax + vmin) * 0.5;
    let inv_scale = 254.0 / (vmax - vmin);
    let scale = 1.0 / inv_scale;
    let scale_sq = scale * scale;
    let dist_mul = if metric == Metric::Cosine {
        scale_sq * 0.5
    } else {
        scale_sq
    };
    (mid, inv_scale, dist_mul)
}

/// The `nprobe` nearest centroids to `q` under `metric`, ascending by distance.
/// A full scan of the (small) centroid set + a partial select — shared by both
/// backends' `search`.
pub(super) fn probe_centroids(
    centroids: &[f32],
    nlist: usize,
    dim: usize,
    q: &[f32],
    metric: Metric,
    nprobe: usize,
) -> Vec<usize> {
    let mut cent: Vec<(f64, usize)> = (0..nlist)
        .map(|c| {
            let cs = &centroids[c * dim..c * dim + dim];
            (distance::ordering_distance(q, cs, metric), c)
        })
        .collect();
    let np = nprobe.clamp(1, cent.len().max(1)).min(cent.len());
    if np < cent.len() {
        cent.select_nth_unstable_by(np - 1, |a, b| a.0.total_cmp(&b.0));
        cent.truncate(np);
    }
    cent.into_iter().map(|(_, c)| c).collect()
}

/// Decode `dim` little-endian f32 from `bytes` into `out` (cleared first).
pub(super) fn decode_f32_into(bytes: &[u8], dim: usize, out: &mut Vec<f32>) {
    out.clear();
    out.reserve(dim);
    out.extend(
        bytes[..dim * 4]
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]])),
    );
}

/// Index of the nearest centroid to `v` under `metric`.
pub(super) fn nearest_centroid(
    centroids: &[f32],
    nlist: usize,
    dim: usize,
    v: &[f32],
    metric: Metric,
) -> usize {
    let mut best = 0usize;
    let mut best_d = f64::INFINITY;
    let mut start = 0;
    if metric == Metric::L2 && dim >= 32 {
        while start + 4 <= nlist {
            let scores = distance::l2_sq_batch4(v, &centroids[start * dim..(start + 4) * dim], dim);
            for (offset, d) in scores.into_iter().enumerate() {
                if (d as f64) < best_d {
                    best_d = d as f64;
                    best = start + offset;
                }
            }
            start += 4;
        }
    }
    for c in start..nlist {
        let cs = &centroids[c * dim..c * dim + dim];
        let d = distance::ordering_distance(v, cs, metric);
        if d < best_d {
            best_d = d;
            best = c;
        }
    }
    best
}

/// Evenly-strided indices `0..n`, at most `target` of them (used to sample the
/// training set without allocating a shuffle; `Math.random` is unavailable in
/// this environment and a deterministic stride keeps builds reproducible).
pub(super) fn strided_sample(n: usize, target: usize) -> Vec<usize> {
    if target >= n {
        return (0..n).collect();
    }
    let step = (n as f64 / target as f64).max(1.0);
    let mut out = Vec::with_capacity(target);
    let mut acc = 0.0f64;
    while (acc as usize) < n && out.len() < target {
        out.push(acc as usize);
        acc += step;
    }
    out
}

/// Deterministic pseudo-random sample of `target` distinct indices from `0..n`,
/// for training the coarse quantizer. Unlike [`strided_sample`], the picks are
/// decorrelated from heap insertion order, so a table physically sorted by some
/// vector dimension does not yield a systematically biased training set (which
/// would place centroids poorly and depress recall). Deterministic (an LCG
/// seeded from `n`, no `Math.random`) so builds stay reproducible and the paged
/// backend remains a bit-for-bit differential oracle of the memory backend.
pub(super) fn training_sample(n: usize, target: usize) -> Vec<usize> {
    if target >= n {
        return (0..n).collect();
    }
    // SplitMix64-style LCG seeded from `n`; reject-sample distinct indices into
    // a set. `target < n` here, and training targets are modest (≈256·√N), so
    // the birthday-collision reject loop terminates quickly.
    let mut state = (n as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(1);
    let mut next = || {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    let mut seen = std::collections::HashSet::with_capacity(target);
    let mut out = Vec::with_capacity(target);
    // Bound the loop defensively so a pathological seed can't spin; fall back to
    // filling any shortfall with a strided pass.
    let mut guard = target.saturating_mul(8).max(64);
    while out.len() < target && guard > 0 {
        guard -= 1;
        let i = (next() % n as u64) as usize;
        if seen.insert(i) {
            out.push(i);
        }
    }
    if out.len() < target {
        for i in strided_sample(n, target) {
            if out.len() >= target {
                break;
            }
            if seen.insert(i) {
                out.push(i);
            }
        }
    }
    out
}

/// Lloyd's k-means. `samples` are the (already normalized, for Cosine) training
/// vectors; returns `k·dim` row-major centroids. Empty clusters are reseeded to
/// a strided sample so `nlist` lists always exist. For Cosine, centroids are
/// re-normalized each iteration (spherical k-means).
pub(super) fn kmeans<V: AsRef<[f32]> + Sync>(
    samples: &[V],
    k: usize,
    dim: usize,
    iters: usize,
    cosine: bool,
) -> Vec<f32> {
    let n = samples.len();
    let k = k.clamp(1, n.max(1));
    let mut centroids = vec![0f32; k * dim];
    // Init: strided distinct samples.
    let init = strided_sample(n, k);
    for (c, &si) in init.iter().enumerate() {
        centroids[c * dim..c * dim + dim].copy_from_slice(samples[si].as_ref());
    }
    if n == 0 {
        return centroids;
    }
    let metric = if cosine { Metric::Cosine } else { Metric::L2 };
    // Use an invalid sentinel so the first assignment pass always counts as a
    // change.  Initialising to cluster 0 can otherwise terminate after one
    // update when all initial samples happen to prefer cluster 0, even though
    // the empty-cluster reseed has just changed the centroids.
    let mut assign = vec![u32::MAX; n];
    // The assign step (each sample → nearest of `k` centroids) is the dominant
    // O(n·k·dim) cost and embarrassingly parallel + deterministic (each sample
    // independent, centroids read-only). Fan it out over the available cores
    // for large training sets; tiny sets stay serial to dodge thread overhead.
    let threads = kmeans_assign_threads(n, k, dim);
    for _ in 0..iters.max(1) {
        // Assign.
        let changed = if threads > 1 {
            kmeans_assign_parallel(samples, &centroids, k, dim, metric, &mut assign, threads)
        } else {
            let mut changed = false;
            for (i, s) in samples.iter().enumerate() {
                let c = nearest_centroid(&centroids, k, dim, s.as_ref(), metric) as u32;
                if c != assign[i] {
                    assign[i] = c;
                    changed = true;
                }
            }
            changed
        };
        // Update: mean of assigned samples.
        let mut sums = vec![0f64; k * dim];
        let mut counts = vec![0u32; k];
        for (i, s) in samples.iter().enumerate() {
            let c = assign[i] as usize;
            counts[c] += 1;
            let acc = &mut sums[c * dim..c * dim + dim];
            for (a, &x) in acc.iter_mut().zip(s.as_ref().iter()) {
                *a += x as f64;
            }
        }
        for c in 0..k {
            if counts[c] == 0 {
                // Empty cluster: reseed to a strided sample to keep nlist cells.
                let si = (c * 2654435761usize) % n;
                centroids[c * dim..c * dim + dim].copy_from_slice(samples[si].as_ref());
                continue;
            }
            let inv = 1.0 / counts[c] as f64;
            for d in 0..dim {
                centroids[c * dim + d] = (sums[c * dim + d] * inv) as f32;
            }
            if cosine {
                let cs = &mut centroids[c * dim..c * dim + dim];
                let norm: f32 = cs.iter().map(|x| x * x).sum::<f32>().sqrt();
                if norm > 0.0 {
                    for x in cs.iter_mut() {
                        *x /= norm;
                    }
                }
            }
        }
        if !changed {
            break;
        }
    }
    centroids
}

/// Worker count for the k-means assign step: 0/1 (serial) for small sets, else
/// the core count clamped by the per-thread work. `MYDB_IVF_BUILD_THREADS`
/// overrides (0/1 forces serial — the differential oracle).
fn kmeans_assign_threads(n: usize, k: usize, dim: usize) -> usize {
    if let Ok(v) = std::env::var("MYDB_IVF_BUILD_THREADS") {
        if let Ok(t) = v.parse::<usize>() {
            return t.max(1);
        }
    }
    // Only worth parallelising when the per-iteration work is substantial.
    if n < 4096 || k * dim < 4096 {
        return 1;
    }
    std::thread::available_parallelism()
        .map(|c| c.get())
        .unwrap_or(1)
        .clamp(1, 16)
}

/// Assign every vector in `samples` to its nearest centroid, in parallel for
/// large sets. The one-shot cousin of the k-means assign step, used by the
/// cold build to bucket rows into inverted lists.
pub(super) fn assign_to_centroids<V: AsRef<[f32]> + Sync>(
    samples: &[V],
    centroids: &[f32],
    k: usize,
    dim: usize,
    metric: Metric,
) -> Vec<u32> {
    let n = samples.len();
    let mut out = vec![0u32; n];
    let threads = kmeans_assign_threads(n, k, dim);
    if threads > 1 {
        kmeans_assign_parallel(samples, centroids, k, dim, metric, &mut out, threads);
    } else {
        for (i, s) in samples.iter().enumerate() {
            out[i] = nearest_centroid(centroids, k, dim, s.as_ref(), metric) as u32;
        }
    }
    out
}

/// Parallel k-means assign: split the samples into `threads` disjoint index
/// ranges, each writing its own slice of `assign`. Returns whether any
/// assignment changed. Deterministic — identical to the serial loop, only the
/// iteration is fanned out.
fn kmeans_assign_parallel<V: AsRef<[f32]> + Sync>(
    samples: &[V],
    centroids: &[f32],
    k: usize,
    dim: usize,
    metric: Metric,
    assign: &mut [u32],
    threads: usize,
) -> bool {
    let n = samples.len();
    let chunk = n.div_ceil(threads).max(1);
    let any_changed = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        let mut base = 0usize;
        for slice in assign.chunks_mut(chunk) {
            let start = base;
            base += slice.len();
            handles.push(scope.spawn(move || {
                let mut changed = false;
                for (j, a) in slice.iter_mut().enumerate() {
                    let c = nearest_centroid(centroids, k, dim, samples[start + j].as_ref(), metric)
                        as u32;
                    if c != *a {
                        *a = c;
                        changed = true;
                    }
                }
                changed
            }));
        }
        handles.into_iter().any(|h| h.join().unwrap_or(false))
    });
    any_changed
}

/// Bounded top-k min-collector: keeps the `k` smallest `(dist, rid)` seen. A
/// binary max-heap keyed on distance so the worst survivor is O(1) to evict.
pub(super) struct TopK {
    k: usize,
    // Max-heap by distance (largest at root) so we can pop the worst.
    heap: std::collections::BinaryHeap<HeapItem>,
}

struct HeapItem {
    dist: f64,
    rid: RowId,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.dist == other.dist
    }
}
impl Eq for HeapItem {}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Max-heap on distance; NaN sorts as greatest so it is evicted first.
        self.dist.total_cmp(&other.dist)
    }
}

impl TopK {
    pub(super) fn new(k: usize) -> Self {
        TopK {
            k: k.max(1),
            heap: std::collections::BinaryHeap::new(),
        }
    }

    #[inline]
    pub(super) fn push(&mut self, dist: impl Into<f64>, rid: RowId) {
        let dist = dist.into();
        if self.heap.len() < self.k {
            self.heap.push(HeapItem { dist, rid });
        } else if let Some(mut top) = self.heap.peek_mut() {
            if dist < top.dist {
                *top = HeapItem { dist, rid };
            }
        }
    }

    pub(super) fn into_sorted(self) -> Vec<(RowId, f64)> {
        let mut v: Vec<(RowId, f64)> = self.heap.into_iter().map(|it| (it.rid, it.dist)).collect();
        v.sort_by(|a, b| a.1.total_cmp(&b.1));
        v
    }
}

/// One IVF_SQ8 index in whichever backend it was built. Kept behind the same
/// enum so [`super::graph::VectorGraph::Ivf`] and its ~15 dispatch arms — plus
/// `as_ivf()` observability — stay backend-agnostic, mirroring the
/// `Memory`/`Paged` split HNSW already uses.
///
///   * [`IvfSq8Index::Memory`] — the in-memory inverted file ([`MemoryIvf`]),
///     rebuilt from the heap on cold load (no snapshot in this backend).
///   * [`IvfSq8Index::Paged`] — the buffer-pool-resident paged inverted file
///     ([`PagedIvf`], `.ivf`). Demand-paged (larger-than-memory) and crash-safe
///     via physical WAL; the file itself is the durable index.
#[derive(Debug)]
pub(crate) enum IvfSq8Index {
    Memory(MemoryIvf),
    Paged(super::ivf_paged::PagedIvf),
}

impl IvfSq8Index {
    /// Construct an in-memory IVF index (the default / oracle backend).
    pub(crate) fn new(metric: Metric, params: IvfParams) -> Self {
        IvfSq8Index::Memory(MemoryIvf::new(metric, params))
    }

    /// Wrap an open paged `.ivf` backend.
    pub(crate) fn from_paged(paged: super::ivf_paged::PagedIvf) -> Self {
        IvfSq8Index::Paged(paged)
    }

    pub(crate) fn dim(&self) -> usize {
        match self {
            IvfSq8Index::Memory(m) => m.dim(),
            IvfSq8Index::Paged(p) => p.dim(),
        }
    }

    pub(crate) fn node_count(&self) -> usize {
        match self {
            IvfSq8Index::Memory(m) => m.node_count(),
            IvfSq8Index::Paged(p) => p.node_count(),
        }
    }

    pub(crate) fn live_count(&self) -> usize {
        match self {
            IvfSq8Index::Memory(m) => m.live_count(),
            IvfSq8Index::Paged(p) => p.live_count(),
        }
    }

    pub(crate) fn stale_count(&self) -> u32 {
        match self {
            IvfSq8Index::Memory(m) => m.stale_count(),
            IvfSq8Index::Paged(p) => p.stale_count(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            IvfSq8Index::Memory(m) => m.is_empty(),
            IvfSq8Index::Paged(p) => p.is_empty(),
        }
    }

    pub(crate) fn nlist(&self) -> usize {
        match self {
            IvfSq8Index::Memory(m) => m.nlist(),
            IvfSq8Index::Paged(p) => p.nlist(),
        }
    }

    pub(crate) fn inserts_since_train(&self) -> u64 {
        match self {
            IvfSq8Index::Memory(m) => m.inserts_since_train(),
            IvfSq8Index::Paged(p) => p.inserts_since_train(),
        }
    }

    /// Backend tag for `information_schema` / SHOW ENGINE.
    pub(crate) fn backend_name(&self) -> &'static str {
        match self {
            IvfSq8Index::Memory(_) => "ivf",
            IvfSq8Index::Paged(p) if p.has_navigator() => "ivf_paged_nav",
            IvfSq8Index::Paged(_) => "ivf_paged",
        }
    }

    pub(crate) fn paged_runtime_stats(&self) -> Option<(u64, u64, u64, u64, u64, bool)> {
        match self {
            IvfSq8Index::Paged(p) => Some(p.runtime_storage_stats()),
            IvfSq8Index::Memory(_) => None,
        }
    }

    pub(crate) fn begin_transaction_delta(
        &self,
        txn_id: u64,
    ) -> Result<(), crate::types::QueryError> {
        match self {
            IvfSq8Index::Paged(p) => p.begin_transaction_delta(txn_id),
            IvfSq8Index::Memory(_) => Ok(()),
        }
    }

    pub(crate) fn finish_committed_delta(
        &self,
        txn_id: u64,
    ) -> Result<(), crate::types::QueryError> {
        match self {
            IvfSq8Index::Paged(p) => p.finish_committed_delta(txn_id),
            IvfSq8Index::Memory(_) => Ok(()),
        }
    }

    pub(crate) fn flush(&self) -> Result<(), crate::types::QueryError> {
        match self {
            IvfSq8Index::Paged(p) => p.file().flush(),
            IvfSq8Index::Memory(_) => Ok(()),
        }
    }

    pub(crate) fn build_from_rows(
        &self,
        rows: &[(RowId, Vec<f32>)],
    ) -> Result<(), crate::types::QueryError> {
        match self {
            IvfSq8Index::Memory(m) => {
                m.build_from_rows(rows);
                Ok(())
            }
            IvfSq8Index::Paged(p) => p.build_from_rows(rows),
        }
    }

    pub(crate) fn insert(&self, rid: RowId, vector: &[f32]) {
        match self {
            IvfSq8Index::Memory(m) => m.insert(rid, vector),
            IvfSq8Index::Paged(p) => p.insert(rid, vector),
        }
    }

    pub(crate) fn mark_deleted_by_row_id(&self, rid: RowId) -> u32 {
        match self {
            IvfSq8Index::Memory(m) => m.mark_deleted_by_row_id(rid),
            IvfSq8Index::Paged(p) => p.mark_deleted_by_row_id(rid),
        }
    }

    pub(crate) fn has_live_row_id(&self, rid: RowId) -> bool {
        match self {
            IvfSq8Index::Memory(m) => m.has_live_row_id(rid),
            IvfSq8Index::Paged(p) => p.has_live_row_id(rid),
        }
    }

    pub(crate) fn try_update_in_place(&self, rid: RowId, new_vector: &[f32], ratio: f32) -> bool {
        match self {
            IvfSq8Index::Memory(m) => m.try_update_in_place(rid, new_vector, ratio),
            IvfSq8Index::Paged(p) => p.try_update_in_place(rid, new_vector, ratio),
        }
    }

    pub(crate) fn compact(&self) -> u32 {
        match self {
            IvfSq8Index::Memory(m) => m.compact(),
            IvfSq8Index::Paged(p) => p.compact(),
        }
    }

    pub(crate) fn try_search_result(
        &self,
        query: &[f32],
        candidate_limit: usize,
        nprobe: usize,
    ) -> Result<super::graph::VectorSearchResult, crate::types::QueryError> {
        let nlist = self.nlist();
        let (candidates, exact_order) = match self {
            IvfSq8Index::Memory(m) => {
                let exact = m.metric() == Metric::Dot;
                (m.search(query, candidate_limit, nprobe), exact)
            }
            IvfSq8Index::Paged(p) => {
                let (rows, _entries_scanned, exact) =
                    p.try_search_with_stats(query, candidate_limit, nprobe)?;
                (rows, exact)
            }
        };
        Ok(super::graph::VectorSearchResult {
            candidates,
            exact_order,
            exhausted: nlist == 0 || nprobe.max(1) >= nlist,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_bug7_compact_does_not_overwrite_concurrent_insert() {
        let index = MemoryIvf::new(Metric::L2, IvfParams::default());
        let data = rows(100, 8, 7);
        index.build_from_rows(&data);
        index.mark_deleted_by_row_id(data[0].0);
        let rid = RowId {
            page_id: 900_000,
            slot_id: 0,
        };
        assert_eq!(
            index.compact_with_observer(|| index.insert(rid, &data[0].1)),
            0
        );
        assert!(index.has_live_row_id(rid));
        assert_eq!(index.stale_count(), 1);
        assert_eq!(index.compact(), 1);
        assert!(index.has_live_row_id(rid));
    }

    #[test]
    fn audit_bug7_search_completes_while_compactor_rebuilds() {
        let index = std::sync::Arc::new(MemoryIvf::new(Metric::L2, IvfParams::default()));
        let data = rows(100, 8, 7);
        index.build_from_rows(&data);
        index.mark_deleted_by_row_id(data[0].0);
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let compact_index = index.clone();
        let compactor = std::thread::spawn(move || {
            compact_index.compact_with_observer(|| {
                entered_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
            })
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let searcher = std::thread::spawn(move || {
            result_tx.send(index.search(&data[1].1, 10, 1)).unwrap();
        });
        let result = result_rx.recv_timeout(std::time::Duration::from_secs(2));
        // Always release the worker, including when the old exclusive-lock
        // implementation times out, so this test cannot leak a blocked thread.
        resume_tx.send(()).unwrap();
        assert_eq!(compactor.join().unwrap(), 1);
        searcher.join().unwrap();
        assert!(!result
            .expect("search blocked behind compact reconstruction")
            .is_empty());
    }

    #[test]
    fn nearest_centroid_batch_matches_single_row_oracle() {
        for dim in [1, 31, 32, 65, 768] {
            let data = rows(20, dim, 9123);
            for nlist in [1, 3, 4, 5, 17] {
                let centroids: Vec<f32> = data[..nlist]
                    .iter()
                    .flat_map(|(_, v)| v.iter().copied())
                    .collect();
                for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
                    for (_, query) in &data {
                        let mut best = 0;
                        let mut score = f64::INFINITY;
                        for c in 0..nlist {
                            let d = distance::ordering_distance(
                                query,
                                &centroids[c * dim..(c + 1) * dim],
                                metric,
                            );
                            if d < score {
                                best = c;
                                score = d;
                            }
                        }
                        assert_eq!(
                            nearest_centroid(&centroids, nlist, dim, query, metric),
                            best
                        );
                    }
                }
            }
            let tied = vec![0.0; 5 * dim];
            assert_eq!(nearest_centroid(&tied, 5, dim, &data[0].1, Metric::L2), 0);
        }
    }

    #[test]
    fn training_sample_is_distinct_and_decorrelated() {
        // C4: distinct indices, correct count, and — unlike the systematic
        // stride — not the arithmetic sequence `0, step, 2·step, …` (so a
        // heap physically ordered by a vector dimension is not sampled with a
        // fixed period). Deterministic across calls (reproducible builds).
        let n = 10_000;
        let target = 500;
        let s1 = training_sample(n, target);
        assert_eq!(s1.len(), target, "must return exactly `target` indices");
        let uniq: std::collections::HashSet<usize> = s1.iter().copied().collect();
        assert_eq!(uniq.len(), target, "indices must be distinct");
        assert!(s1.iter().all(|&i| i < n), "indices in range");
        // Reproducible.
        assert_eq!(s1, training_sample(n, target), "must be deterministic");
        // Not the strided arithmetic sequence.
        let strided = strided_sample(n, target);
        assert_ne!(s1, strided, "should not equal the systematic stride");
        // target >= n degenerates to the full set (same as strided).
        assert_eq!(training_sample(50, 100), (0..50).collect::<Vec<_>>());
    }

    fn rows(n: usize, dim: usize, seed: u64) -> Vec<(RowId, Vec<f32>)> {
        // Deterministic pseudo-random clustered vectors (no Math.random here).
        // 16 well-separated random cluster centers in R^dim + per-dim noise, so
        // clusters are not collinear (a realistic ANN workload).
        let mut s = seed;
        let mut next = || {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((s >> 33) as f32 / (1u64 << 31) as f32) - 1.0
        };
        let n_clusters = 16;
        let centers: Vec<Vec<f32>> = (0..n_clusters)
            .map(|_| (0..dim).map(|_| next() * 5.0).collect())
            .collect();
        (0..n)
            .map(|i| {
                let c = &centers[i % n_clusters];
                let v: Vec<f32> = c.iter().map(|&x| x + next() * 0.3).collect();
                (RowId::new((i + 1) as u32, 0), v)
            })
            .collect()
    }

    fn exact_knn(data: &[(RowId, Vec<f32>)], q: &[f32], k: usize, metric: Metric) -> Vec<RowId> {
        let mut scored: Vec<(f64, RowId)> = data
            .iter()
            .map(|(rid, v)| (distance::ordering_distance(q, v, metric), *rid))
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0));
        scored.into_iter().take(k).map(|(_, r)| r).collect()
    }

    fn recall_at(metric: Metric, dim: usize) -> f64 {
        let data = rows(2000, dim, 42);
        let idx = MemoryIvf::new(
            metric,
            IvfParams {
                nlist: 64,
                nprobe_default: 8,
                kmeans_iters: 15,
                train_sample_mult: 256,
            },
        );
        idx.build_from_rows(&data);
        let k = 10;
        let mut hits = 0usize;
        let mut total = 0usize;
        for qi in (0..data.len()).step_by(200) {
            let q = &data[qi].1;
            // Candidate-set recall: does the IVF candidate pool (which the
            // executor re-ranks with exact f32) contain the true top-k? This is
            // the meaningful metric for an SQ8 index whose ordering is only
            // approximate — the executor's re-rank recovers the exact order.
            let got = idx.search(q, k * 4, 32);
            let got_ids: std::collections::HashSet<RowId> =
                got.into_iter().map(|(r, _)| r).collect();
            let truth = exact_knn(&data, q, k, metric);
            for t in &truth {
                if got_ids.contains(t) {
                    hits += 1;
                }
                total += 1;
            }
        }
        hits as f64 / total as f64
    }

    #[test]
    fn ivf_l2_recall_reasonable() {
        assert!(recall_at(Metric::L2, 16) > 0.8, "L2 recall too low");
    }

    #[test]
    fn ivf_cosine_recall_reasonable() {
        assert!(recall_at(Metric::Cosine, 16) > 0.8, "Cosine recall too low");
    }

    #[test]
    fn ivf_dot_recall_reasonable() {
        assert!(recall_at(Metric::Dot, 16) > 0.8, "Dot recall too low");
    }

    #[test]
    fn ivf_full_probe_equals_brute() {
        // nprobe == nlist must scan every list → recall 1.0 vs exact for the
        // candidates it returns (before the executor's f32 re-rank).
        let data = rows(500, 8, 7);
        let idx = MemoryIvf::new(
            Metric::L2,
            IvfParams {
                nlist: 16,
                ..Default::default()
            },
        );
        idx.build_from_rows(&data);
        let q = &data[123].1;
        let got = idx.search(q, 10, 16); // nprobe = nlist
                                         // The querying row itself is its own nearest neighbour.
        assert_eq!(got.first().map(|(r, _)| *r), Some(data[123].0));
    }

    #[test]
    fn ivf_delete_then_absent() {
        let data = rows(300, 8, 3);
        let idx = MemoryIvf::new(
            Metric::L2,
            IvfParams {
                nlist: 8,
                ..Default::default()
            },
        );
        idx.build_from_rows(&data);
        let target = data[50].0;
        assert_eq!(idx.mark_deleted_by_row_id(target), 1);
        let q = &data[50].1;
        let got = idx.search(q, 20, 8);
        assert!(
            !got.iter().any(|(r, _)| *r == target),
            "deleted row resurfaced"
        );
        assert_eq!(idx.live_count(), 299);
    }

    #[test]
    fn ivf_insert_then_present() {
        let data = rows(300, 8, 9);
        let idx = MemoryIvf::new(
            Metric::L2,
            IvfParams {
                nlist: 8,
                ..Default::default()
            },
        );
        idx.build_from_rows(&data);
        let rid = RowId::new(99999, 0);
        let v = data[10].1.clone();
        idx.insert(rid, &v);
        let got = idx.search(&v, 5, 8);
        assert!(got.iter().any(|(r, _)| *r == rid), "inserted row not found");
    }

    #[test]
    fn ivf_empty_build_bootstraps_on_first_insert() {
        let idx = MemoryIvf::new(
            Metric::L2,
            IvfParams {
                nlist: 8,
                ..IvfParams::default()
            },
        );
        idx.build_from_rows(&[]);
        let target = RowId {
            page_id: 7,
            slot_id: 1,
        };
        idx.insert(target, &[1.0, 2.0, 3.0]);
        assert_eq!(idx.dim(), 3);
        assert_eq!(idx.nlist(), 1);
        assert!(idx
            .search(&[1.0, 2.0, 3.0], 1, 1)
            .iter()
            .any(|(rid, _)| *rid == target));
    }

    #[test]
    fn ivf_sme_l2_mode_is_correct() {
        // With MYDB_IVF_SME=on the L2 lists store f32 and score via the SME
        // batch kernel (or its NEON fallback where SME is unavailable). Either
        // way the scoring is exact f32, so a full-probe search must return the
        // exact top-k — a stronger guarantee than the SQ8 default. This is the
        // differential oracle: SME-on results equal exact brute force.
        crate::util::env::with_vars(&[("MYDB_IVF_SME", Some("on"))], || {
            let data = rows(600, 8, 21);
            let idx = MemoryIvf::new(
                Metric::L2,
                IvfParams {
                    nlist: 16,
                    ..Default::default()
                },
            );
            idx.build_from_rows(&data);
            // The nearest neighbour of a query equal to a stored row is that row
            // itself (distance 0) — exact regardless of reduction order. And the
            // full-probe candidate pool must contain (near-)all true neighbours.
            let mut hits = 0usize;
            let mut total = 0usize;
            for &qi in &[3usize, 100, 321, 500] {
                let q = &data[qi].1;
                let pool: std::collections::HashSet<RowId> =
                    idx.search(q, 40, 16).into_iter().map(|(r, _)| r).collect();
                assert!(
                    pool.contains(&data[qi].0),
                    "SME full-probe must find the query row"
                );
                for t in exact_knn(&data, q, 10, Metric::L2) {
                    if pool.contains(&t) {
                        hits += 1;
                    }
                    total += 1;
                }
            }
            assert!(
                hits as f64 / total as f64 > 0.9,
                "SME/f32 full-probe candidate recall too low: {hits}/{total}"
            );
        });
    }

    #[test]
    fn ivf_sme_and_sq8_agree_on_candidate_recall() {
        // SME mode and the default SQ8 mode must both surface the true
        // neighbours in their candidate pools (the executor re-ranks either).
        let data = rows(800, 12, 55);
        let run = |sme: bool| -> f64 {
            let build = || {
                let idx = MemoryIvf::new(
                    Metric::L2,
                    IvfParams {
                        nlist: 32,
                        ..Default::default()
                    },
                );
                idx.build_from_rows(&data);
                let mut hits = 0usize;
                let mut total = 0usize;
                for qi in (0..data.len()).step_by(100) {
                    let got: std::collections::HashSet<RowId> = idx
                        .search(&data[qi].1, 40, 16)
                        .into_iter()
                        .map(|(r, _)| r)
                        .collect();
                    for t in exact_knn(&data, &data[qi].1, 10, Metric::L2) {
                        if got.contains(&t) {
                            hits += 1;
                        }
                        total += 1;
                    }
                }
                hits as f64 / total as f64
            };
            if sme {
                crate::util::env::with_vars(&[("MYDB_IVF_SME", Some("on"))], build)
            } else {
                crate::util::env::with_vars(&[("MYDB_IVF_SME", Some("off"))], build)
            }
        };
        assert!(run(true) > 0.8, "SME-mode recall too low");
        assert!(run(false) > 0.8, "SQ8-mode recall too low");
    }

    #[test]
    fn ivf_compact_reclaims_tombstones() {
        let data = rows(200, 8, 11);
        let idx = MemoryIvf::new(
            Metric::L2,
            IvfParams {
                nlist: 8,
                ..Default::default()
            },
        );
        idx.build_from_rows(&data);
        for i in 0..50 {
            idx.mark_deleted_by_row_id(data[i].0);
        }
        assert_eq!(idx.stale_count(), 50);
        assert_eq!(idx.compact(), 50);
        assert_eq!(idx.stale_count(), 0);
        assert_eq!(idx.node_count(), 150);
        assert_eq!(idx.live_count(), 150);
        // Surviving rows still searchable.
        let q = &data[100].1;
        let got = idx.search(q, 5, 8);
        assert!(got.iter().any(|(r, _)| *r == data[100].0));
    }
}
