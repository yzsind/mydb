//! Native IVF lists with numeric and binary coarse quantizers.
use super::hnsw_core::Scored;
use super::{
    codec,
    distance::{typed_ordering_distance, Metric},
    ivf::{training_sample, IvfParams},
    VectorSearchRequest, VectorSearchResult,
};
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::{
    storage::{RowId, VectorIndexFile},
    types::QueryError,
    vector::{VectorDataType, VectorValue},
};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

#[derive(Debug)]
struct Entry {
    row: RowId,
    value: Option<VectorValue>,
    vid: u64,
    deleted: bool,
}
#[derive(Debug)]
struct State {
    entries: Vec<Entry>,
    rows: HashMap<RowId, usize>,
    centers: Vec<VectorValue>,
    lists: Vec<Vec<usize>>,
    list_pages: Vec<AtomicU64>,
    charge: ControlledMemoryCharge,
}
#[derive(Debug)]
pub(crate) struct NativeIvf {
    datatype: VectorDataType,
    dimension: u32,
    metric: Metric,
    params: IvfParams,
    file: Option<Arc<VectorIndexFile>>,
    state: RwLock<State>,
    valid: AtomicBool,
    pending: Mutex<HashSet<u64>>,
}
impl NativeIvf {
    pub(crate) fn new(
        datatype: VectorDataType,
        dimension: u32,
        metric: Metric,
        params: IvfParams,
        file: Option<Arc<VectorIndexFile>>,
    ) -> Result<Self, QueryError> {
        Ok(Self {
            datatype,
            dimension,
            metric,
            params,
            file,
            state: RwLock::new(State {
                entries: Vec::new(),
                rows: HashMap::new(),
                centers: Vec::new(),
                lists: vec![Vec::new()],
                list_pages: vec![AtomicU64::new(0)],
                charge: ControlledMemoryCharge::try_new(
                    ControlledMemoryCategory::StorageScratch,
                    0,
                )?,
            }),
            valid: AtomicBool::new(true),
            pending: Mutex::new(HashSet::new()),
        })
    }
    pub(crate) fn file(&self) -> Option<&Arc<VectorIndexFile>> {
        self.file.as_ref()
    }
    pub(crate) fn replace_file(&mut self, file: Arc<VectorIndexFile>) {
        self.file = Some(file);
    }
    pub(crate) fn invalidate(&self) {
        self.valid.store(false, Ordering::Release);
        if let Some(f) = &self.file {
            let _ = f.invalidate_build(0);
        }
    }
    pub(crate) fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }
    pub(crate) fn dim(&self) -> usize {
        self.dimension as usize
    }
    pub(crate) fn node_count(&self) -> usize {
        self.state.read().entries.len()
    }
    pub(crate) fn len(&self) -> usize {
        self.state.read().rows.len()
    }
    pub(crate) fn nlist(&self) -> usize {
        self.state.read().lists.len()
    }
    pub(crate) fn stale_count(&self) -> u32 {
        let state = self.state.read();
        (state.entries.len() - state.rows.len()) as u32
    }
    pub(crate) fn backend_name(&self) -> &'static str {
        if self.file.is_some() {
            "ivf_paged_native"
        } else {
            "ivf_native"
        }
    }
    pub(crate) fn begin_transaction_delta(&self, txn: u64) -> Result<(), QueryError> {
        if let Some(f) = &self.file {
            let mut pending = self.pending.lock();
            f.invalidate_build(0)?;
            pending.insert(txn);
        }
        Ok(())
    }
    pub(crate) fn finish_committed_delta(&self, txn: u64, scn: u64) -> Result<(), QueryError> {
        if let Some(f) = &self.file {
            let mut pending = self.pending.lock();
            f.advance_coverage(scn);
            if pending.remove(&txn) && pending.is_empty() && self.is_valid() {
                f.mark_delta_finalized(0)?;
            }
        }
        Ok(())
    }
    fn value(&self, state: &State, id: usize) -> Result<VectorValue, QueryError> {
        let e = &state.entries[id];
        if let Some(v) = &e.value {
            return Ok(v.clone());
        }
        self.file.as_ref().unwrap().with_native_vector(e.vid, |v| {
            Ok(VectorValue::new(
                v.datatype,
                v.dimension,
                v.payload.to_vec(),
            ))
        })
    }
    fn score(&self, state: &State, id: usize, query: &VectorValue) -> Result<f64, QueryError> {
        let e = &state.entries[id];
        if let Some(v) = &e.value {
            return typed_ordering_distance(query, v, self.metric);
        }
        self.file
            .as_ref()
            .unwrap()
            .with_native_vector(e.vid, |v| typed_ordering_distance(query, &v, self.metric))
    }
    fn nearest(
        centers: &[VectorValue],
        value: &VectorValue,
        metric: Metric,
    ) -> Result<usize, QueryError> {
        let mut best = (f64::INFINITY, 0);
        for (i, c) in centers.iter().enumerate() {
            let d = typed_ordering_distance(value, c, metric)?;
            if d < best.0 {
                best = (d, i);
            }
        }
        Ok(best.1)
    }
    pub(crate) fn insert(&self, row: RowId, value: VectorValue) -> Result<(), QueryError> {
        codec::validate_internal(&value)?;
        if self.metric == Metric::Cosine && super::distance::typed_is_zero(&value) {
            return Err(codec::vector_err("COSINE indexes reject zero vectors"));
        }
        if value.datatype != self.datatype || value.dimension != self.dimension {
            return Err(codec::vector_err("IVF datatype/dimension mismatch"));
        }
        let mut state = self.state.write();
        if let Some(&old) = state.rows.get(&row) {
            self.delete_locked(&mut state, old)?;
        }
        let list = if state.centers.is_empty() {
            0
        } else {
            Self::nearest(&state.centers, &value, self.metric)?
        };
        state
            .charge
            .try_grow(128 + if self.file.is_none() { value.len() } else { 0 })?;
        let id = state.entries.len();
        let vid = if let Some(f) = &self.file {
            f.append_native_ivf_entry(row, &value.payload, &state.list_pages[list])?
        } else {
            id as u64
        };
        let value = if self.file.is_none() {
            Some(value)
        } else {
            None
        };
        state.entries.push(Entry {
            row,
            value,
            vid,
            deleted: false,
        });
        state.rows.insert(row, id);
        state.lists[list].push(id);
        Ok(())
    }
    fn delete_locked(&self, state: &mut State, id: usize) -> Result<(), QueryError> {
        if let Some(f) = &self.file {
            f.set_tombstone(0, state.entries[id].vid, true)?;
        }
        state.entries[id].deleted = true;
        state.rows.remove(&state.entries[id].row);
        Ok(())
    }
    pub(crate) fn delete(&self, row: RowId) -> Result<u32, QueryError> {
        let mut state = self.state.write();
        if let Some(&id) = state.rows.get(&row) {
            self.delete_locked(&mut state, id)?;
            return Ok(1);
        }
        Ok(0)
    }
    pub(crate) fn has_row(&self, row: RowId) -> bool {
        self.state.read().rows.contains_key(&row)
    }
    pub(crate) fn initialize_from_sample(
        &self,
        rows: Vec<(RowId, VectorValue)>,
        nlist: usize,
    ) -> Result<(), QueryError> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut params = self.params;
        params.nlist = nlist;
        let trainer = Self::new(self.datatype, self.dimension, self.metric, params, None)?;
        for (row, value) in rows {
            trainer.insert(row, value)?;
        }
        trainer.train()?;
        let mut trained = trainer.state.write();
        let centers = std::mem::take(&mut trained.centers);
        let mut state = self.state.write();
        state
            .charge
            .try_grow(centers.iter().map(|v| v.len() + 96).sum())?;
        state.lists = (0..centers.len()).map(|_| Vec::new()).collect();
        state.list_pages = (0..centers.len()).map(|_| AtomicU64::new(0)).collect();
        state.centers = centers;
        Ok(())
    }
    fn train_centers(
        &self,
        sample: &[VectorValue],
        nlist: usize,
    ) -> Result<Vec<VectorValue>, QueryError> {
        if self.datatype != VectorDataType::Binary {
            let dim = self.dimension as usize;
            // Decode only the bounded training sample. Row entries remain native payloads.
            let _scratch = ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::StorageScratch,
                sample
                    .len()
                    .saturating_mul(dim.saturating_mul(4).saturating_add(32))
                    .saturating_add(nlist.saturating_mul(dim).saturating_mul(16)),
            )?;
            let mut dense: Vec<Vec<f32>> = sample.iter().map(codec::to_f32).collect();
            let cosine = self.metric == Metric::Cosine;
            if cosine {
                for value in &mut dense {
                    super::distance::normalize_in_place(value);
                }
            }
            // Match the established numeric IVF coarse quantizer: DOT trains with L2.
            let mut flat = super::ivf::kmeans(&dense, nlist, dim, self.params.kmeans_iters, cosine);
            if cosine {
                for center in flat.chunks_exact_mut(dim) {
                    super::distance::normalize_in_place(center);
                }
            }
            return flat
                .chunks_exact(dim)
                .enumerate()
                .map(|(i, center)| {
                    let center = if cosine && center.iter().all(|&x| x == 0.0) {
                        dense[i % dense.len()].as_slice()
                    } else {
                        center
                    };
                    if center.iter().any(|x| !x.is_finite()) {
                        return Err(codec::vector_err("non-finite IVF centroid"));
                    }
                    // Internal coarse metadata may exceed SQL FLOAT32's payload limit.
                    Ok(codec::from_f32(center))
                })
                .collect();
        }
        let mut centers = vec![sample[0].clone()];
        let mut random = 0x5eed_a913_45e2_7011u64;
        let mut weights = vec![f64::INFINITY; sample.len()];
        while centers.len() < nlist {
            let newest = centers.last().unwrap();
            let mut total = 0.0;
            for (v, weight) in sample.iter().zip(&mut weights) {
                *weight = weight.min(typed_ordering_distance(v, newest, self.metric)?.max(0.0));
                total += *weight;
            }
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            let mut threshold = (random >> 11) as f64 / (1u64 << 53) as f64 * total;
            let mut chosen = weights.len() - 1;
            for (i, w) in weights.iter().enumerate() {
                threshold -= *w;
                if threshold < 0.0 {
                    chosen = i;
                    break;
                }
            }
            if centers.iter().any(|c| c == &sample[chosen]) {
                chosen = sample
                    .iter()
                    .position(|v| !centers.contains(v))
                    .ok_or_else(|| codec::vector_err("cannot initialize IVF centers"))?;
            }
            centers.push(sample[chosen].clone());
        }
        for _ in 0..self.params.kmeans_iters {
            crate::engine::cancel::check_cancelled()?;
            let mut clusters = vec![Vec::new(); nlist];
            let mut scores = Vec::with_capacity(sample.len());
            for (i, v) in sample.iter().enumerate() {
                let center = Self::nearest(&centers, v, self.metric)?;
                clusters[center].push(i);
                scores.push(typed_ordering_distance(v, &centers[center], self.metric)?);
            }
            let mut reseeded = HashSet::new();
            let mut changed = false;
            for list in 0..nlist {
                let cluster = &clusters[list];
                if cluster.is_empty() {
                    let farthest = scores
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| !reseeded.contains(i))
                        .max_by(|(a, x), (b, y)| x.total_cmp(y).then_with(|| b.cmp(a)))
                        .map(|(i, _)| i)
                        .unwrap_or(0);
                    reseeded.insert(farthest);
                    centers[list] = sample[farthest].clone();
                    changed = true;
                    continue;
                }
                let next = if self.metric == Metric::Hamming {
                    let mut bytes = vec![0; self.datatype.payload_len(self.dimension)];
                    for bit in 0..self.dimension as usize {
                        let ones = cluster
                            .iter()
                            .filter(|&&i| sample[i].payload[bit / 8] & (1 << (bit % 8)) != 0)
                            .count();
                        if ones > cluster.len() / 2 {
                            bytes[bit / 8] |= 1 << (bit % 8);
                        }
                    }
                    VectorValue::new(self.datatype, self.dimension, bytes)
                } else {
                    let candidates = training_sample(cluster.len(), 32.min(cluster.len()));
                    let eval = training_sample(cluster.len(), 256.min(cluster.len()));
                    let cost = |c: &VectorValue| -> Result<f64, QueryError> {
                        eval.iter()
                            .map(|&j| {
                                typed_ordering_distance(c, &sample[cluster[j]], Metric::Jaccard)
                            })
                            .sum()
                    };
                    let mut best = centers[list].clone();
                    let mut best_cost = cost(&best)?;
                    for j in candidates {
                        let candidate = &sample[cluster[j]];
                        let score = cost(candidate)?;
                        if score < best_cost {
                            best = candidate.clone();
                            best_cost = score;
                        }
                    }
                    best
                };
                changed |= centers[list] != next;
                centers[list] = next;
            }
            if !changed {
                break;
            }
        }
        Ok(centers)
    }
    pub(crate) fn train(&self) -> Result<(), QueryError> {
        let mut state = self.state.write();
        let live: Vec<usize> = state
            .entries
            .iter()
            .enumerate()
            .filter_map(|(i, e)| (!e.deleted).then_some(i))
            .collect();
        if live.is_empty() {
            return Ok(());
        }
        let requested = if self.params.nlist == 0 {
            (live.len() as f64).sqrt() as usize
        } else {
            self.params.nlist
        };
        let requested = requested.max(1).min(live.len());
        let count = requested
            .saturating_mul(self.params.train_sample_mult)
            .min(live.len());
        let _scratch = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::StorageScratch,
            count
                .saturating_mul(self.datatype.payload_len(self.dimension).saturating_mul(2) + 64)
                .saturating_add(self.dimension as usize * 16),
        )?;
        let sample = training_sample(live.len(), count)
            .into_iter()
            .map(|i| self.value(&state, live[i]))
            .collect::<Result<Vec<_>, _>>()?;
        let unique: HashSet<&[u8]> = sample.iter().map(|v| v.payload.as_slice()).collect();
        let nlist = requested.min(unique.len()).max(1);
        drop(unique);
        let centers = self.train_centers(&sample, nlist)?;
        state
            .charge
            .try_grow(centers.iter().map(|v| v.len() + 64).sum())?;
        let mut lists = vec![Vec::new(); nlist];
        for id in live {
            let v = self.value(&state, id)?;
            let list = Self::nearest(&centers, &v, self.metric)?;
            lists[list].push(id);
        }
        state.list_pages = (0..lists.len()).map(|_| AtomicU64::new(0)).collect();
        state.centers = centers;
        state.lists = lists;

        Ok(())
    }
    pub(crate) fn search(
        &self,
        query: &VectorValue,
        request: VectorSearchRequest,
    ) -> Result<VectorSearchResult, QueryError> {
        if !self.is_valid() {
            return Err(codec::vector_err("native IVF requires rebuild"));
        }
        if let Some(file) = &self.file {
            file.prepare_navigation_mapping()?;
        }
        let state = self.state.read();
        let super::VectorSearchEffort::Ivf { nprobe } = request.effort else {
            return Err(codec::vector_err("invalid IVF search effort"));
        };
        let candidate_limit = request.candidate_limit.min(state.rows.len());
        if candidate_limit == 0 {
            return Ok(VectorSearchResult {
                exhausted: true,
                ..Default::default()
            });
        }
        let nprobe = nprobe.max(1).min(state.lists.len());
        let _scratch = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::StorageScratch,
            candidate_limit
                .saturating_mul(64)
                .saturating_add(self.dimension as usize)
                .saturating_add(
                    state
                        .centers
                        .len()
                        .saturating_mul(std::mem::size_of::<Scored<usize>>()),
                ),
        )?;
        let mut probes: Vec<Scored<usize>> = state
            .centers
            .iter()
            .enumerate()
            .map(|(id, c)| {
                Ok(Scored {
                    dist: typed_ordering_distance(query, c, self.metric)?,
                    id,
                })
            })
            .collect::<Result<_, QueryError>>()?;
        probes.sort_unstable();
        if probes.is_empty() {
            probes.push(Scored { dist: 0.0, id: 0 });
        }
        let mut best = BinaryHeap::new();
        best.try_reserve(candidate_limit).map_err(|_| {
            crate::storage::error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "native IVF candidate allocation failed",
            )
        })?;
        for probe in probes.into_iter().take(nprobe) {
            for &id in &state.lists[probe.id] {
                if state.entries[id].deleted {
                    continue;
                }
                let distance = self.score(&state, id, query)?;
                let score = Scored { dist: distance, id };
                if best.len() < candidate_limit || best.peek().is_some_and(|worst| score < *worst) {
                    best.push(score);
                    if best.len() > candidate_limit {
                        best.pop();
                    }
                }
            }
            crate::engine::cancel::check_cancelled()?;
        }
        let candidates = best
            .into_sorted_vec()
            .into_iter()
            .map(|s| (state.entries[s.id].row, s.dist))
            .collect();
        Ok(VectorSearchResult {
            candidates,
            exact_order: true,
            exhausted: nprobe == state.lists.len(),
        })
    }
}
