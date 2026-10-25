//! HNSW over native payloads, sharing decisions across memory and paged storage.
use super::{
    codec,
    distance::{typed_ordering_distance, Metric},
    hnsw_core::{self, Scored},
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
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet},
    hash::{BuildHasher, Hash, Hasher},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

#[derive(Debug)]
struct Node {
    exception: bool,
    row: RowId,
    group: usize,
    level: usize,
    deleted: bool,
    vid: u64,
    value: Option<VectorValue>,
    links: Vec<Vec<usize>>,
}
#[derive(Debug)]
struct NativeCsr {
    offsets: Vec<usize>,
    edges: Vec<usize>,
    _charge: ControlledMemoryCharge,
}
/// Rows at the same native coordinate share navigation, while retaining their
/// own payload, tombstone and heap identity. Hash collisions are fully checked.
#[derive(Debug)]
struct NavigationGroup {
    representative: usize,
    members: Vec<usize>,
    next_collision: Option<usize>,
}
#[derive(Debug)]
struct State {
    exceptions: usize,
    incremental_exceptions: usize,
    nodes: Vec<Node>,
    groups: Vec<NavigationGroup>,
    group_heads: HashMap<u64, usize>,
    rows: HashMap<RowId, usize>,
    by_vid: HashMap<u64, usize>,
    entry: Option<usize>,
    charge: ControlledMemoryCharge,
    csr: Option<NativeCsr>,
}
#[derive(Debug)]
pub(crate) struct NativeHnsw {
    datatype: VectorDataType,
    dimension: u32,
    metric: Metric,
    m: usize,
    ef_construction: usize,
    file: Option<Arc<VectorIndexFile>>,
    state: RwLock<State>,
    valid: AtomicBool,
    pending: Mutex<HashSet<u64>>,
    quantizer: Option<Arc<super::quantization::Quantizer>>,
    quantization_requested: bool,
    retrain: AtomicBool,
    generation: u64,
    candidates_returned: std::sync::atomic::AtomicU64,
    heap_reranks: std::sync::atomic::AtomicU64,
    exact_fallbacks: std::sync::atomic::AtomicU64,
}

impl NativeHnsw {
    pub(crate) fn new(
        datatype: VectorDataType,
        dimension: u32,
        metric: Metric,
        m: usize,
        ef_construction: usize,
        file: Option<Arc<VectorIndexFile>>,
    ) -> Result<Self, QueryError> {
        Ok(Self {
            datatype,
            dimension,
            metric,
            m,
            ef_construction,
            file,
            state: RwLock::new(State {
                exceptions: 0,
                incremental_exceptions: 0,
                nodes: Vec::new(),
                groups: Vec::new(),
                group_heads: HashMap::new(),
                rows: HashMap::new(),
                by_vid: HashMap::new(),
                entry: None,
                csr: None,
                charge: ControlledMemoryCharge::try_new_recoverable(
                    ControlledMemoryCategory::StorageScratch,
                    0,
                )?,
            }),
            valid: AtomicBool::new(true),
            pending: Mutex::new(HashSet::new()),
            quantizer: None,
            quantization_requested: false,
            retrain: AtomicBool::new(false),
            generation: 0,
            candidates_returned: std::sync::atomic::AtomicU64::new(0),
            heap_reranks: std::sync::atomic::AtomicU64::new(0),
            exact_fallbacks: std::sync::atomic::AtomicU64::new(0),
        })
    }
    pub(crate) fn set_quantizer(
        &mut self,
        requested: bool,
        quantizer: Option<Arc<super::quantization::Quantizer>>,
        generation: u64,
    ) {
        self.quantization_requested = requested;
        self.generation = if quantizer.is_some() { generation } else { 0 };
        self.quantizer = quantizer;
    }
    pub(crate) fn query_stats(&self) -> (u64, u64, u64, u64) {
        (
            self.generation,
            self.candidates_returned.load(Ordering::Relaxed),
            self.heap_reranks.load(Ordering::Relaxed),
            self.exact_fallbacks.load(Ordering::Relaxed),
        )
    }
    pub(crate) fn record_query_stats(&self, candidates: u64, heap: u64, fallback: bool) {
        if !self.quantization_requested {
            return;
        }
        self.candidates_returned
            .fetch_add(candidates, Ordering::Relaxed);
        self.heap_reranks.fetch_add(heap, Ordering::Relaxed);
        self.exact_fallbacks
            .fetch_add(u64::from(fallback), Ordering::Relaxed);
    }
    pub(crate) fn quantization_pending(&self) -> bool {
        self.quantization_requested && self.quantizer.is_none()
    }
    pub(crate) fn quantization_stats(&self) -> (usize, usize, usize) {
        let state = self.state.read();
        (
            self.quantizer.as_ref().map_or(0, |q| q.bytes()),
            self.quantizer
                .as_ref()
                .map_or(0, |q| q.record_len() * state.nodes.len()),
            state.exceptions,
        )
    }
    /// Transcribe an admitted parallel construction graph, retaining only compressed payloads.
    pub(crate) fn import_build_topology(
        &self,
        topology: &super::hnsw::HnswIndex,
    ) -> Result<(), QueryError> {
        let q = self
            .quantizer
            .as_ref()
            .ok_or_else(|| codec::vector_err("compressed builder requires a trained model"))?;
        let mut state = self.state.write();
        let mut vector = Vec::new();
        let mut links = Vec::new();
        for id in 0..topology.build_node_count() {
            crate::engine::cancel::check_cancelled()?;
            let row = topology.copy_build_node_into(id, &mut vector, &mut links);
            let value = q.store(
                &codec::from_build_f32(&vector, self.datatype)?,
                self.file.as_ref(),
            )?;
            let level = links.len().saturating_sub(1);
            state.charge.try_grow_recoverable(
                704 + if self.file.is_none() {
                    value.len() + (level + 1) * (self.m * 2 * 8 + 24)
                } else {
                    0
                },
            )?;
            let vid = if let Some(file) = &self.file {
                file.append_native_node(0, row, level, &value.payload)?
            } else {
                id as u64
            };
            let exception = value.payload[0] == 1;
            if exception {
                state.exceptions += 1;
            }
            state.nodes.push(Node {
                exception,
                row,
                group: id,
                level,
                deleted: false,
                vid,
                value: if self.file.is_none() {
                    Some(value)
                } else {
                    None
                },
                links: if self.file.is_none() {
                    vec![Vec::new(); level + 1]
                } else {
                    Vec::new()
                },
            });
            state.groups.push(NavigationGroup {
                representative: id,
                members: vec![id],
                next_collision: None,
            });
            state.rows.insert(row, id);
            state.by_vid.insert(vid, id);
        }
        for id in 0..topology.build_node_count() {
            topology.copy_build_links_into(id, &mut links);
            for (level, neighbors) in links.iter().enumerate() {
                self.set_links(
                    &mut state,
                    id,
                    level,
                    neighbors.iter().map(|&n| n as usize).collect(),
                )?;
            }
        }
        if let Some((entry, level)) = topology.build_entry() {
            state.entry = Some(entry as usize);
            if let Some(file) = &self.file {
                file.set_entry(0, state.nodes[entry as usize].vid, level as u32)?;
            }
        }
        Ok(())
    }
    pub(crate) fn replace_file(&mut self, file: Arc<VectorIndexFile>) {
        self.file = Some(file);
    }
    pub(crate) fn file(&self) -> Option<&Arc<VectorIndexFile>> {
        self.file.as_ref()
    }
    pub(crate) fn invalidate(&self) {
        self.valid.store(false, Ordering::Release);
        if let Some(file) = &self.file {
            let _ = file.invalidate_build(0);
        }
    }
    pub(crate) fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
            && (self.quantizer.is_none() || !self.retrain.load(Ordering::Acquire))
    }
    pub(crate) fn dim(&self) -> usize {
        self.dimension as usize
    }
    pub(crate) fn node_count(&self) -> usize {
        self.state.read().nodes.len()
    }
    pub(crate) fn len(&self) -> usize {
        self.state.read().rows.len()
    }
    pub(crate) fn stale_count(&self) -> u32 {
        let state = self.state.read();
        (state.nodes.len() - state.rows.len()) as u32
    }
    pub(crate) fn backend_name(&self) -> &'static str {
        if self.file.is_some() {
            "paged_native"
        } else {
            "memory_native"
        }
    }
    pub(crate) fn begin_transaction_delta(&self, txn: u64) -> Result<(), QueryError> {
        if let Some(file) = &self.file {
            let mut pending = self.pending.lock();
            file.invalidate_build(0)?;
            pending.insert(txn);
        }
        Ok(())
    }
    pub(crate) fn finish_committed_delta(&self, txn: u64, scn: u64) -> Result<(), QueryError> {
        if let Some(file) = &self.file {
            let mut pending = self.pending.lock();
            file.advance_coverage(scn);
            if pending.remove(&txn) && pending.is_empty() && self.is_valid() {
                file.mark_delta_finalized(0)?;
            }
        }
        Ok(())
    }
    pub(crate) fn prepare_navigation(&self) -> Result<(), QueryError> {
        if let Some(file) = &self.file {
            if file.prepare_navigation_mapping()? {
                if self.state.read().csr.is_some() {
                    self.state.write().csr = None;
                }
            } else {
                self.prepare_csr()?;
            }
        }
        Ok(())
    }
    fn prepare_csr(&self) -> Result<(), QueryError> {
        if self.state.read().csr.is_some() {
            return Ok(());
        }
        let mut state = self.state.write();
        if state.csr.is_some() || state.nodes.is_empty() {
            return Ok(());
        }
        let bytes = state
            .nodes
            .len()
            .saturating_mul((self.m * 2 + 1) * std::mem::size_of::<usize>())
            .saturating_add(8);
        let limit = std::env::var("MYDB_HNSW_QUERY_CACHE_MB")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .map(|mb| mb.saturating_mul(1024 * 1024))
            .unwrap_or_else(|| crate::engine::executor::mem_budget::memory_target_bytes() / 4);
        if bytes > limit {
            return Ok(());
        }
        let Some(mut charge) =
            ControlledMemoryCharge::try_new_optional(ControlledMemoryCategory::DerivedCache, bytes)
        else {
            return Ok(());
        };
        let mut offsets = Vec::new();
        let mut edges = Vec::new();
        offsets
            .try_reserve_exact(state.nodes.len() + 1)
            .map_err(|_| {
                crate::storage::error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    "native CSR offsets allocation failed",
                )
            })?;
        edges
            .try_reserve_exact(state.nodes.len().saturating_mul(self.m * 2))
            .map_err(|_| {
                crate::storage::error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    "native CSR edges allocation failed",
                )
            })?;
        offsets.push(0);
        for id in 0..state.nodes.len() {
            edges.extend(self.links(&state, id, 0)?);
            offsets.push(edges.len());
        }
        let actual =
            offsets.capacity().saturating_add(edges.capacity()) * std::mem::size_of::<usize>();
        charge.shrink(bytes.saturating_sub(actual));
        state.csr = Some(NativeCsr {
            offsets,
            edges,
            _charge: charge,
        });
        Ok(())
    }
    fn navigation_score(
        &self,
        state: &State,
        id: usize,
        query: &VectorValue,
        query_code: Option<&super::quantization::QuantQuery>,
    ) -> Result<f64, QueryError> {
        if let Some(quant) = &self.quantizer {
            let node = &state.nodes[id];
            if let Some(value) = &node.value {
                return quant.score_payload(&value.payload, self.file.as_ref(), query, query_code);
            }
            return self
                .file
                .as_ref()
                .unwrap()
                .with_native_vector(node.vid, |value| {
                    quant.score_payload(value.payload, self.file.as_ref(), query, query_code)
                });
        }
        self.score(state, id, query)
    }
    fn score(&self, state: &State, id: usize, query: &VectorValue) -> Result<f64, QueryError> {
        if self.quantizer.is_some() {
            return self.navigation_score(state, id, query, None);
        }
        let node = &state.nodes[id];
        match (&node.value, &self.file) {
            (Some(value), _) => typed_ordering_distance(query, value, self.metric),
            (_, Some(file)) => file.with_native_vector(node.vid, |value| {
                typed_ordering_distance(query, &value, self.metric)
            }),
            _ => Err(codec::vector_err("missing native HNSW payload")),
        }
    }
    fn node_probe(
        &self,
        state: &State,
        id: usize,
    ) -> Result<(VectorValue, Option<super::quantization::QuantQuery>), QueryError> {
        let Some(q) = &self.quantizer else {
            return Ok((self.value(state, id)?, None));
        };
        let node = &state.nodes[id];
        let prepared = if let Some(value) = &node.value {
            q.prepare_payload(&value.payload, self.file.as_ref())?
        } else {
            self.file
                .as_ref()
                .unwrap()
                .with_native_vector(node.vid, |value| {
                    q.prepare_payload(value.payload, self.file.as_ref())
                })?
        };
        Ok((VectorValue::default(), Some(prepared)))
    }
    fn value(&self, state: &State, id: usize) -> Result<VectorValue, QueryError> {
        let node = &state.nodes[id];
        if let Some(quant) = &self.quantizer {
            if let Some(value) = &node.value {
                return quant.value_payload(&value.payload, self.file.as_ref());
            }
            return self
                .file
                .as_ref()
                .unwrap()
                .with_native_vector(node.vid, |v| {
                    quant.value_payload(v.payload, self.file.as_ref())
                });
        }
        if let Some(value) = &node.value {
            return Ok(value.clone());
        }
        self.file
            .as_ref()
            .unwrap()
            .with_native_vector(node.vid, |v| {
                Ok(VectorValue::new(
                    v.datatype,
                    v.dimension,
                    v.payload.to_vec(),
                ))
            })
    }
    fn links(&self, state: &State, id: usize, level: usize) -> Result<Vec<usize>, QueryError> {
        let node = &state.nodes[id];
        if state.groups[node.group].representative != id || level > node.level {
            return Ok(Vec::new());
        }
        if level == 0 {
            if let Some(csr) = &state.csr {
                return Ok(csr.edges[csr.offsets[id]..csr.offsets[id + 1]].to_vec());
            }
        }

        if let Some(file) = &self.file {
            let mut out = Vec::new();
            file.read_neighbors_into(node.vid, level, &mut out)?;
            out.into_iter()
                .map(|vid| {
                    state
                        .by_vid
                        .get(&vid)
                        .copied()
                        .ok_or_else(|| codec::vector_err("native HNSW edge has no node"))
                })
                .collect()
        } else {
            Ok(node.links[level].clone())
        }
    }
    fn set_links(
        &self,
        state: &mut State,
        id: usize,
        level: usize,
        links: Vec<usize>,
    ) -> Result<(), QueryError> {
        if let Some(file) = &self.file {
            let links: Vec<u64> = links.into_iter().map(|id| state.nodes[id].vid).collect();
            file.set_neighbors(0, state.nodes[id].vid, level, &links)
        } else {
            state.nodes[id].links[level] = links;
            Ok(())
        }
    }
    fn greedy(
        &self,
        state: &State,
        query: &VectorValue,
        mut entry: usize,
        level: usize,
        query_code: Option<&super::quantization::QuantQuery>,
    ) -> Result<usize, QueryError> {
        let mut best = self.navigation_score(state, entry, query, query_code)?;
        loop {
            let mut improved = false;
            for next in self.links(state, entry, level)? {
                let d = self.navigation_score(state, next, query, query_code)?;
                if d < best {
                    best = d;
                    entry = next;
                    improved = true;
                }
            }
            if !improved {
                return Ok(entry);
            }
        }
    }
    fn layer(
        &self,
        state: &State,
        query: &VectorValue,
        entries: &[usize],
        ef: usize,
        level: usize,
        query_code: Option<&super::quantization::QuantQuery>,
    ) -> Result<Vec<Scored<usize>>, QueryError> {
        let ef = ef.max(1).min(state.nodes.len());
        let mut todo = BinaryHeap::new();
        let mut best = BinaryHeap::new();
        let mut seen = HashSet::new();
        let mut admitted = ef;
        let mut extra = ControlledMemoryCharge::try_new_recoverable(
            ControlledMemoryCategory::StorageScratch,
            0,
        )?;
        seen.try_reserve(ef).map_err(|_| {
            crate::storage::error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "native HNSW visited allocation failed",
            )
        })?;
        best.try_reserve(ef).map_err(|_| {
            crate::storage::error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "native HNSW result allocation failed",
            )
        })?;
        todo.try_reserve(ef).map_err(|_| {
            crate::storage::error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "native HNSW frontier allocation failed",
            )
        })?;
        for &id in entries {
            if seen.insert(id) {
                let score = Scored {
                    dist: self.navigation_score(state, id, query, query_code)?,
                    id,
                };
                todo.push(Reverse(score));
                best.push(score);
            }
        }
        while let Some(Reverse(current)) = todo.pop() {
            if best.len() >= ef
                && best
                    .peek()
                    .is_some_and(|worst: &Scored<usize>| current.dist > worst.dist)
            {
                break;
            }
            for id in self.links(state, current.id, level)? {
                if seen.contains(&id) {
                    continue;
                }
                if seen.len() >= admitted {
                    let next = admitted.saturating_mul(2).max(64).min(state.nodes.len());
                    extra
                        .try_grow_recoverable(next.saturating_sub(admitted).saturating_mul(128))?;
                    admitted = next;
                }
                seen.try_reserve(1).map_err(|_| {
                    crate::storage::error(
                        crate::error::catalog::ER_OUTOFMEMORY,
                        "native HNSW visited growth failed",
                    )
                })?;
                todo.try_reserve(1).map_err(|_| {
                    crate::storage::error(
                        crate::error::catalog::ER_OUTOFMEMORY,
                        "native HNSW frontier growth failed",
                    )
                })?;
                seen.insert(id);
                let score = Scored {
                    dist: self.navigation_score(state, id, query, query_code)?,
                    id,
                };
                if best.len() < ef || best.peek().is_some_and(|worst| score < *worst) {
                    todo.push(Reverse(score));
                    best.push(score);
                    if best.len() > ef {
                        best.pop();
                    }
                }
            }
            if seen.len() & 1023 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
        }
        Ok(best.into_sorted_vec())
    }
    fn select(
        &self,
        state: &State,
        candidates: &[Scored<usize>],
        maximum: usize,
    ) -> Result<Vec<usize>, QueryError> {
        let mut cached: Option<(usize, VectorValue, Option<super::quantization::QuantQuery>)> =
            None;
        let selected = hnsw_core::select_diverse_by(
            candidates.iter().copied(),
            maximum,
            |candidate, previous| {
                if cached.as_ref().is_none_or(|(id, _, _)| *id != candidate.id) {
                    let (value, prepared) = self.node_probe(state, candidate.id)?;
                    cached = Some((candidate.id, value, prepared));
                }
                Ok::<_, QueryError>(
                    self.navigation_score(
                        state,
                        previous.id,
                        &cached.as_ref().unwrap().1,
                        cached.as_ref().unwrap().2.as_ref(),
                    )? < candidate.dist,
                )
            },
        )?;
        Ok(selected.into_iter().map(|s| s.id).collect())
    }
    fn coordinate_hash(&self, state: &State, value: &VectorValue) -> u64 {
        let mut hash = state.group_heads.hasher().build_hasher();
        match self.datatype {
            VectorDataType::Float16 | VectorDataType::BFloat16 => {
                for word in value.payload.chunks_exact(2) {
                    let bits = u16::from_le_bytes(word.try_into().unwrap());
                    (if bits & 0x7fff == 0 { 0 } else { bits }).hash(&mut hash);
                }
            }
            VectorDataType::Float32 => {
                for word in value.payload.chunks_exact(4) {
                    let bits = u32::from_le_bytes(word.try_into().unwrap());
                    (if bits & 0x7fff_ffff == 0 { 0 } else { bits }).hash(&mut hash);
                }
            }
            _ => value.payload.hash(&mut hash),
        }
        hash.finish()
    }
    fn coordinate_equal(
        &self,
        state: &State,
        representative: usize,
        value: &VectorValue,
    ) -> Result<bool, QueryError> {
        let equal = |stored: &[u8]| {
            if stored == value.payload.as_slice() {
                return true;
            }
            if stored.len() != value.payload.len() {
                return false;
            }
            match self.datatype {
                VectorDataType::Float16 | VectorDataType::BFloat16 => stored
                    .chunks_exact(2)
                    .zip(value.payload.chunks_exact(2))
                    .all(|(a, b)| {
                        let a = u16::from_le_bytes(a.try_into().unwrap());
                        let b = u16::from_le_bytes(b.try_into().unwrap());
                        a == b || (a & 0x7fff == 0 && b & 0x7fff == 0)
                    }),
                VectorDataType::Float32 => stored
                    .chunks_exact(4)
                    .zip(value.payload.chunks_exact(4))
                    .all(|(a, b)| {
                        let a = u32::from_le_bytes(a.try_into().unwrap());
                        let b = u32::from_le_bytes(b.try_into().unwrap());
                        a == b || (a & 0x7fff_ffff == 0 && b & 0x7fff_ffff == 0)
                    }),
                _ => false,
            }
        };
        if let Some(stored) = &state.nodes[representative].value {
            return Ok(equal(&stored.payload));
        }
        self.file
            .as_ref()
            .unwrap()
            .with_native_vector(state.nodes[representative].vid, |v| Ok(equal(v.payload)))
    }
    fn insert_member(
        &self,
        state: &mut State,
        group: usize,
        row: RowId,
        value: VectorValue,
    ) -> Result<(), QueryError> {
        state
            .charge
            .try_grow_recoverable(512 + if self.file.is_none() { value.len() } else { 0 })?;
        state
            .nodes
            .try_reserve(1)
            .map_err(|_| codec::vector_err("native HNSW member allocation failed"))?;
        state.groups[group]
            .members
            .try_reserve(1)
            .map_err(|_| codec::vector_err("native HNSW posting allocation failed"))?;
        let id = state.nodes.len();
        let vid = if let Some(file) = &self.file {
            file.append_native_node(0, row, 0, &value.payload)?
        } else {
            id as u64
        };
        state.nodes.push(Node {
            exception: false,
            row,
            group,
            level: 0,
            deleted: false,
            vid,
            value: if self.file.is_none() {
                Some(value)
            } else {
                None
            },
            links: Vec::new(),
        });
        state.by_vid.insert(vid, id);
        state.groups[group].members.push(id);
        state.rows.insert(row, id);
        Ok(())
    }
    pub(crate) fn insert(&self, row: RowId, value: VectorValue) -> Result<(), QueryError> {
        if self.quantization_pending() {
            return Err(codec::vector_err("quantizer training is pending"));
        }
        codec::validate_internal(&value)?;
        if self.metric == Metric::Cosine && super::distance::typed_is_zero(&value) {
            return Err(codec::vector_err("COSINE indexes reject zero vectors"));
        }
        if value.datatype != self.datatype || value.dimension != self.dimension {
            return Err(codec::vector_err("native HNSW datatype/dimension mismatch"));
        }
        let mut state = self.state.write();
        state.csr = None;
        if let Some(&old) = state.rows.get(&row) {
            self.delete_locked(&mut state, old)?;
        }
        let hash = self.coordinate_hash(&state, &value);
        let mut matching = if self.quantizer.is_some() {
            None
        } else {
            state.group_heads.get(&hash).copied()
        };
        while let Some(group) = matching {
            if self.coordinate_equal(&state, state.groups[group].representative, &value)? {
                return self.insert_member(&mut state, group, row, value);
            }
            matching = state.groups[group].next_collision;
        }
        let group = state.groups.len();
        let id = state.nodes.len();
        let level = hnsw_core::random_level(
            id as u64,
            1.0 / (self.m as f64).ln(),
            super::hnsw_paged::VEC_MAX_LAYER,
        );
        let bytes = 704
            + if self.file.is_some() {
                0
            } else {
                self.quantizer
                    .as_ref()
                    .map_or(value.len(), |q| q.record_len())
                    + (level + 1) * (self.m * 2 * 8 + 24)
            };
        state.charge.try_grow_recoverable(bytes)?;
        state
            .nodes
            .try_reserve(1)
            .map_err(|_| codec::vector_err("native HNSW node allocation failed"))?;
        let prepared = self
            .quantizer
            .as_ref()
            .map(|q| q.prepare(&value))
            .transpose()?;
        let mut entry = state.entry;
        if let Some(mut current) = entry {
            for layer in ((level + 1)..=state.nodes[current].level).rev() {
                current = self.greedy(&state, &value, current, layer, prepared.as_ref())?;
            }
            entry = Some(current);
        }
        let encoded = self
            .quantizer
            .as_ref()
            .map(|q| q.store(&value, self.file.as_ref()))
            .transpose()?;
        let payload = encoded.as_ref().unwrap_or(&value);
        if self.file.is_none() && self.quantizer.is_some() {
            state.charge.try_grow_recoverable(
                payload
                    .len()
                    .saturating_sub(self.quantizer.as_ref().unwrap().record_len()),
            )?;
        }
        let vid = if let Some(file) = &self.file {
            file.append_native_node(0, row, level, &payload.payload)?
        } else {
            id as u64
        };
        let stored = if self.file.is_some() {
            None
        } else {
            Some(payload.clone())
        };
        if self.quantizer.is_some() && payload.payload.first() == Some(&1) {
            state.exceptions += 1;
            if state.nodes.len() >= self.quantizer.as_ref().unwrap().trained_rows() {
                state.incremental_exceptions += 1;
            }
        }
        state.nodes.push(Node {
            exception: self.quantizer.is_some() && payload.payload.first() == Some(&1),
            row,
            group,
            level,
            deleted: false,
            vid,
            value: stored,
            links: if self.file.is_none() {
                vec![Vec::new(); level + 1]
            } else {
                Vec::new()
            },
        });
        state
            .groups
            .try_reserve(1)
            .map_err(|_| codec::vector_err("native HNSW group allocation failed"))?;
        let mut members = Vec::new();
        members
            .try_reserve(1)
            .map_err(|_| codec::vector_err("native HNSW posting allocation failed"))?;
        members.push(id);
        let next_collision = state.group_heads.insert(hash, group);
        state.groups.push(NavigationGroup {
            representative: id,
            members,
            next_collision,
        });
        state.by_vid.insert(vid, id);
        if let Some(mut current) = entry {
            let top = level.min(state.nodes[current].level);
            for layer in (0..=top).rev() {
                let candidates = self.layer(
                    &state,
                    &value,
                    &[current],
                    self.ef_construction,
                    layer,
                    prepared.as_ref(),
                )?;
                let capacity = if layer == 0 { self.m * 2 } else { self.m };
                let chosen = self.select(&state, &candidates, capacity)?;
                self.set_links(&mut state, id, layer, chosen.clone())?;
                for neighbor in chosen {
                    let mut links = self.links(&state, neighbor, layer)?;
                    if !links.contains(&id) {
                        links.push(id);
                    }
                    if links.len() > capacity {
                        let (center, center_query) = self.node_probe(&state, neighbor)?;
                        let mut scores = links
                            .iter()
                            .map(|&id| {
                                Ok(Scored {
                                    dist: self.navigation_score(
                                        &state,
                                        id,
                                        &center,
                                        center_query.as_ref(),
                                    )?,
                                    id,
                                })
                            })
                            .collect::<Result<Vec<_>, QueryError>>()?;
                        scores.sort_unstable();
                        links = self.select(&state, &scores, capacity)?;
                    }
                    self.set_links(&mut state, neighbor, layer, links)?;
                }
                if let Some(best) = candidates.first() {
                    current = best.id;
                }
            }
        }
        if state.entry.is_none_or(|old| level > state.nodes[old].level) {
            state.entry = Some(id);
            if let Some(file) = &self.file {
                file.set_entry(0, vid, level as u32)?;
            }
        }
        state.rows.insert(row, id);
        if let Some(q) = &self.quantizer {
            let exceptions = state.incremental_exceptions;
            let limit = 64usize.max(state.rows.len().div_ceil(20));
            if (matches!(q.name(), "sq8" | "sq4") && exceptions >= limit)
                || (matches!(q.name(), "pq" | "bq")
                    && state.nodes.len() >= q.trained_rows() + 256usize.max(q.trained_rows() / 4))
            {
                self.retrain.store(true, Ordering::Release);
            }
        }
        Ok(())
    }
    fn delete_locked(&self, state: &mut State, id: usize) -> Result<(), QueryError> {
        if state.nodes[id].deleted {
            return Ok(());
        }
        if let Some(file) = &self.file {
            file.set_tombstone(0, state.nodes[id].vid, true)?;
        }
        if state.nodes[id].exception {
            state.exceptions = state.exceptions.saturating_sub(1);
        }
        state.nodes[id].deleted = true;
        state.rows.remove(&state.nodes[id].row);
        Ok(())
    }
    pub(crate) fn delete(&self, row: RowId) -> Result<u32, QueryError> {
        let mut state = self.state.write();
        let Some(&id) = state.rows.get(&row) else {
            return Ok(0);
        };
        self.delete_locked(&mut state, id)?;
        Ok(1)
    }
    pub(crate) fn has_row(&self, row: RowId) -> bool {
        self.state.read().rows.contains_key(&row)
    }
    pub(crate) fn search(
        &self,
        query: &VectorValue,
        request: VectorSearchRequest,
    ) -> Result<VectorSearchResult, QueryError> {
        if !self.is_valid() || self.quantization_pending() {
            return Err(crate::storage::error(
                crate::error::catalog::ER_GET_ERRNO,
                "native HNSW requires quantizer training or rebuild",
            ));
        }
        self.prepare_navigation()?;
        let state = self.state.read();
        let Some(mut entry) = state.entry else {
            return Ok(VectorSearchResult {
                exhausted: true,
                ..Default::default()
            });
        };
        let super::VectorSearchEffort::Hnsw { ef } = request.effort else {
            return Err(codec::vector_err("invalid native HNSW search effort"));
        };
        let ef = ef
            .max(request.rerank_limit)
            .max(request.candidate_limit)
            .min(state.groups.len());
        let _scratch = ControlledMemoryCharge::try_new_recoverable(
            ControlledMemoryCategory::StorageScratch,
            ef.saturating_mul(128)
                .saturating_add(self.dimension as usize)
                .saturating_add(
                    request
                        .candidate_limit
                        .min(state.rows.len())
                        .saturating_mul(std::mem::size_of::<(RowId, f64)>()),
                ),
        )?;
        let query_code = self
            .quantizer
            .as_ref()
            .map(|q| q.prepare(query))
            .transpose()?;
        for layer in (1..=state.nodes[entry].level).rev() {
            entry = self.greedy(&state, query, entry, layer, query_code.as_ref())?;
        }
        let pool = self.layer(&state, query, &[entry], ef, 0, query_code.as_ref())?;
        let exhausted = ef >= state.groups.len();
        let limit = request.candidate_limit.min(state.rows.len());
        let mut candidates = Vec::new();
        candidates
            .try_reserve(limit)
            .map_err(|_| codec::vector_err("native HNSW candidate allocation failed"))?;
        'groups: for score in pool {
            let group = &state.groups[state.nodes[score.id].group];
            for &member in &group.members {
                if !state.nodes[member].deleted {
                    if candidates.len() == limit {
                        break 'groups;
                    }
                    candidates.push((state.nodes[member].row, score.dist));
                }
            }
        }
        Ok(VectorSearchResult {
            candidates,
            exact_order: self.quantizer.is_none(),
            exhausted,
        })
    }
}

#[cfg(test)]
mod duplicate_regressions {
    use super::*;
    #[test]
    fn duplicate_rows_remain_reachable_after_representative_delete() {
        for datatype in [
            VectorDataType::Float16,
            VectorDataType::BFloat16,
            VectorDataType::Int8,
            VectorDataType::Binary,
        ] {
            let metrics: &[Metric] = if datatype == VectorDataType::Binary {
                &[Metric::Hamming, Metric::Jaccard]
            } else {
                &[Metric::L2, Metric::Cosine, Metric::Dot]
            };
            for &metric in metrics {
                let value = codec::parse_typed_text("[0,1]", datatype).unwrap();
                let graph = NativeHnsw::new(datatype, 2, metric, 4, 64, None).unwrap();
                for page_id in 0..512 {
                    graph
                        .insert(
                            RowId {
                                page_id,
                                slot_id: 0,
                            },
                            value.clone(),
                        )
                        .unwrap();
                }
                let request = VectorSearchRequest {
                    candidate_limit: 512,
                    rerank_limit: 512,
                    effort: super::super::VectorSearchEffort::Hnsw { ef: 512 },
                };
                assert_eq!(
                    graph.search(&value, request).unwrap().candidates.len(),
                    512,
                    "{datatype}"
                );
                graph
                    .delete(RowId {
                        page_id: 0,
                        slot_id: 0,
                    })
                    .unwrap();
                let signed_zero = codec::parse_typed_text("[-0,1]", datatype).unwrap();
                graph
                    .insert(
                        RowId {
                            page_id: 512,
                            slot_id: 0,
                        },
                        signed_zero,
                    )
                    .unwrap();
                let result = graph.search(&value, request).unwrap();
                assert_eq!(
                    result.candidates.len(),
                    512,
                    "{datatype} after delete/insert"
                );
                assert!(!result.candidates.iter().any(|(row, _)| row.page_id == 0));
                assert_eq!(
                    result
                        .candidates
                        .iter()
                        .map(|(row, _)| *row)
                        .collect::<HashSet<_>>()
                        .len(),
                    512
                );
                for node in &graph.state.read().nodes {
                    assert!(node.links.first().is_none_or(|links| links.len() <= 8));
                }
            }
        }
    }
}
