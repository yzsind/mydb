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
    complete: bool,
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
    edges: Vec<u32>,
    payloads: Option<Vec<u8>>,
    payload_stride: usize,
    _charge: ControlledMemoryCharge,
}
const NATIVE_MEMO_MAX_CANDIDATES: usize = 1024;
const NATIVE_MEMO_GATES: usize = 64;

struct NativeSearchScratch {
    visited: Vec<u64>,
    todo: BinaryHeap<Reverse<Scored<usize>>>,
    best: BinaryHeap<Scored<usize>>,
    charge: ControlledMemoryCharge,
}

thread_local! {
    static NATIVE_SEARCH_SCRATCH: std::cell::RefCell<Option<NativeSearchScratch>> = const { std::cell::RefCell::new(None) };
}

impl NativeSearchScratch {
    fn new(nodes: usize, ef: usize) -> Result<Self, QueryError> {
        let words = nodes.div_ceil(64);
        let bytes = words
            .saturating_mul(8)
            .saturating_add(ef.saturating_mul(3 * std::mem::size_of::<Scored<usize>>()));
        let charge = ControlledMemoryCharge::try_new_recoverable(
            ControlledMemoryCategory::StorageScratch,
            bytes,
        )?;
        Ok(Self {
            visited: vec![0; words],
            todo: BinaryHeap::with_capacity(ef.saturating_mul(2)),
            best: BinaryHeap::with_capacity(ef),
            charge,
        })
    }

    fn visit(&mut self, id: usize) -> Result<bool, QueryError> {
        let word = self
            .visited
            .get_mut(id / 64)
            .ok_or_else(super::persistence::corrupt)?;
        let mask = 1u64 << (id % 64);
        let fresh = *word & mask == 0;
        *word |= mask;
        Ok(fresh)
    }

    fn push(&mut self, score: Scored<usize>, ef: usize, admit: bool) -> Result<(), QueryError> {
        if self.todo.len() == self.todo.capacity() {
            let extra = self.todo.capacity().max(64);
            self.charge
                .try_grow_recoverable(extra.saturating_mul(std::mem::size_of::<Scored<usize>>()))?;
            self.todo
                .try_reserve_exact(extra)
                .map_err(|_| codec::vector_err("native HNSW frontier allocation failed"))?;
        }
        if admit && self.best.len() < ef && self.best.len() == self.best.capacity() {
            let extra = ef - self.best.capacity();
            self.charge
                .try_grow_recoverable(extra.saturating_mul(std::mem::size_of::<Scored<usize>>()))?;
            self.best
                .try_reserve_exact(extra)
                .map_err(|_| codec::vector_err("native HNSW results allocation failed"))?;
        }
        self.todo.push(Reverse(score));
        if !admit {
            return Ok(());
        }
        if self.best.len() == ef {
            self.best.pop();
        }
        self.best.push(score);
        Ok(())
    }
}

/// Reserve both full entries and one replacement. Capacity follows memory
/// headroom and vector width, independently of any query corpus or dataset.
fn native_query_memo_allocation(dimension: usize, limit: usize) -> Option<(usize, usize)> {
    let budget = (limit / 16).min(64 * 1024 * 1024);
    let entry = dimension
        .checked_mul(4)?
        .checked_add(12 + NATIVE_MEMO_MAX_CANDIDATES * 16 + 128)?;
    let entries = (budget / entry).saturating_sub(1).min(4096) / 8 * 8;
    (entries >= 8).then(|| (entries, (entries + 1) * entry))
}

#[derive(Debug)]
struct NativeQueryCache {
    cache: super::hnsw_paged::QueryNavCache,
    // Serialize cache misses by set. Peers recheck after locking, so identical
    // concurrent traversals are computed once while heap/MVCC stays per query.
    gates: [Mutex<()>; NATIVE_MEMO_GATES],
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
    query_cache: Option<NativeQueryCache>,
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
    trained_pending: std::sync::OnceLock<Arc<super::quantization::Quantizer>>,
    pending_training_gate: Mutex<()>,
    quantization_requested: bool,
    retrain: AtomicBool,
    lineage: u64,
    generation: std::sync::atomic::AtomicU64,
    persisted_lsn: std::sync::atomic::AtomicU64,
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
                query_cache: None,
                charge: ControlledMemoryCharge::try_new_recoverable(
                    ControlledMemoryCategory::StorageScratch,
                    0,
                )?,
            }),
            valid: AtomicBool::new(true),
            pending: Mutex::new(HashSet::new()),
            quantizer: None,
            trained_pending: std::sync::OnceLock::new(),
            pending_training_gate: Mutex::new(()),
            quantization_requested: false,
            retrain: AtomicBool::new(false),
            lineage: 0,
            generation: std::sync::atomic::AtomicU64::new(0),
            persisted_lsn: std::sync::atomic::AtomicU64::new(0),
            candidates_returned: std::sync::atomic::AtomicU64::new(0),
            heap_reranks: std::sync::atomic::AtomicU64::new(0),
            exact_fallbacks: std::sync::atomic::AtomicU64::new(0),
        })
    }
    fn definition_bytes(&self) -> Result<Vec<u8>, QueryError> {
        self.definition_bytes_for(self.quantizer(), self.generation.load(Ordering::Acquire))
    }
    fn definition_bytes_for(
        &self,
        model: Option<&Arc<super::quantization::Quantizer>>,
        generation: u64,
    ) -> Result<Vec<u8>, QueryError> {
        let mut out = Vec::new();
        let mut writer =
            super::persistence::Writer::new(&mut out, 3).map_err(super::persistence::io_error)?;
        writer
            .u8(self.datatype as u8)
            .map_err(super::persistence::io_error)?;
        writer
            .u32(self.dimension)
            .map_err(super::persistence::io_error)?;
        writer
            .u8(self.metric.as_u8())
            .map_err(super::persistence::io_error)?;
        writer.count(self.m).map_err(super::persistence::io_error)?;
        writer
            .count(self.ef_construction)
            .map_err(super::persistence::io_error)?;
        writer
            .u8(u8::from(self.quantization_requested))
            .map_err(super::persistence::io_error)?;
        writer
            .u64(self.lineage)
            .map_err(super::persistence::io_error)?;
        writer
            .u64(generation)
            .map_err(super::persistence::io_error)?;
        let quantizer = model
            .map(|q| q.serialize(None))
            .transpose()?
            .unwrap_or_default();
        writer
            .bytes(&quantizer)
            .map_err(super::persistence::io_error)?;
        writer.finish().map_err(super::persistence::io_error)?;
        Ok(out)
    }

    fn from_definition(
        bytes: &[u8],
        file: Option<Arc<VectorIndexFile>>,
    ) -> Result<Self, QueryError> {
        let mut reader = super::persistence::Reader::new(bytes, 3)?;
        let datatype =
            VectorDataType::from_tag(reader.u8()?).ok_or_else(super::persistence::corrupt)?;
        let dimension = reader.u32()?;
        let metric = Metric::from_u8(reader.u8()?).ok_or_else(super::persistence::corrupt)?;
        let m = reader.u32()? as usize;
        let ef = reader.u32()? as usize;
        let requested = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(super::persistence::corrupt()),
        };
        let lineage = reader.u64()?;
        let generation = reader.u64()?;
        let model = reader.bytes()?;
        if dimension == 0 || dimension > datatype.max_dimension() || m < 2 || ef < m {
            return Err(super::persistence::corrupt());
        }
        let quantizer = if model.is_empty() {
            None
        } else {
            Some(super::quantization::Quantizer::deserialize(model)?)
        };
        reader.finish()?;
        let mut graph = Self::new(datatype, dimension, metric, m, ef, file)?;
        graph.lineage = lineage;
        graph.set_quantizer(requested, quantizer, generation);
        Ok(graph)
    }

    pub(crate) fn persisted_lsn(&self) -> u64 {
        self.persisted_lsn.load(Ordering::Acquire)
    }
    pub(crate) fn mark_persisted(&self, lsn: u64) {
        self.persisted_lsn.store(lsn, Ordering::Release);
    }

    pub(crate) fn persist_definition(&self) -> Result<(), QueryError> {
        if let Some(file) = &self.file {
            file.write_native_definition(&self.definition_bytes()?)?;
        }
        Ok(())
    }

    pub(crate) fn train_pending_model(
        &self,
        config: &crate::storage::vector_config::QuantizationConfig,
        generation: u64,
        publish: impl FnOnce(&Arc<super::quantization::Quantizer>) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let _training = self.pending_training_gate.lock();
        if !self.quantization_pending() {
            return Ok(());
        }
        let minimum = match config {
            crate::storage::vector_config::QuantizationConfig::Pq { bits, .. } => 1usize << bits,
            _ => 1,
        };
        let state = self.state.read();
        if state.rows.len() < minimum {
            return Ok(());
        }
        let _scratch = ControlledMemoryCharge::try_new_recoverable(
            ControlledMemoryCategory::StorageScratch,
            self.datatype.payload_len(self.dimension).saturating_mul(3),
        )?;
        let mut trainer = super::quantization::Trainer::new(
            config.clone(),
            self.datatype,
            self.dimension,
            self.metric,
        )?;
        for (id, node) in state.nodes.iter().enumerate() {
            if !node.deleted {
                trainer.observe(&self.value(&state, id)?)?;
            }
        }
        drop(state);
        let Some(model) = trainer.finish()? else {
            return Ok(());
        };
        publish(&model)?;
        self.install_pending_model(model, generation)
    }

    pub(crate) fn install_pending_model(
        &self,
        model: Arc<super::quantization::Quantizer>,
        generation: u64,
    ) -> Result<(), QueryError> {
        let mut state = self.state.write();
        if self.quantizer().is_some() {
            if self.generation.load(Ordering::Acquire) >= generation {
                return Ok(());
            }
            return Err(super::persistence::corrupt());
        }
        if !self.quantization_requested {
            return Err(super::persistence::corrupt());
        }
        state.query_cache = None;
        state.csr = None;
        let count = state.nodes.len();
        let mut converted_charge = ControlledMemoryCharge::try_new_recoverable(
            ControlledMemoryCategory::StorageScratch,
            if self.file.is_none() {
                count.saturating_mul(24)
            } else {
                0
            },
        )?;
        let mut converted = Vec::new();
        let mut old_bytes = 0usize;
        if self.file.is_none() {
            converted
                .try_reserve_exact(count)
                .map_err(|_| codec::vector_err("pending HNSW conversion allocation failed"))?;
            for node in &state.nodes {
                let old = node
                    .value
                    .as_ref()
                    .ok_or_else(super::persistence::corrupt)?;
                let value = model.store(old, None)?;
                converted_charge.try_grow_recoverable(value.len())?;
                old_bytes = old_bytes.saturating_add(old.len());
                converted.push(value);
            }
        }
        if let Some(file) = &self.file {
            file.write_quantization_model(&model.serialize(None)?, generation)?;
            file.write_native_definition(&self.definition_bytes_for(Some(&model), generation)?)?;
        } else {
            for (node, value) in state.nodes.iter_mut().zip(converted) {
                node.exception = value.payload.first() == Some(&1);
                node.value = Some(value);
            }
            state.charge.absorb(&mut converted_charge);
            state
                .charge
                .shrink(old_bytes.saturating_add(count.saturating_mul(24)));
        }
        if self.file.is_some() {
            for node in &mut state.nodes {
                node.exception = true;
            }
        }
        state.exceptions = state
            .nodes
            .iter()
            .filter(|node| node.exception && !node.deleted)
            .count();
        state.incremental_exceptions = 0;
        self.generation.store(generation, Ordering::Release);
        self.trained_pending
            .set(model)
            .map_err(|_| super::persistence::corrupt())?;
        Ok(())
    }

    pub(crate) fn matches_quantization(
        &self,
        config: &crate::storage::vector_config::QuantizationConfig,
    ) -> bool {
        self.quantization_requested == config.enabled()
            && self.quantizer().is_none_or(|model| {
                model.matches_definition(self.datatype, self.dimension, self.metric, config)
            })
    }

    pub(crate) fn matches_definition(
        &self,
        datatype: VectorDataType,
        dimension: u32,
        metric: Metric,
        m: usize,
        ef: usize,
    ) -> bool {
        self.datatype == datatype
            && self.dimension == dimension
            && self.metric == metric
            && self.m == m
            && self.ef_construction == ef
    }

    fn reserve_paged_metadata(&self, state: &mut State, rows: usize) -> Result<(), QueryError> {
        // Prepay actual container capacities rather than a fixed kilobyte per
        // row. Hash tables have a <=7/8 load factor; 32 bytes per bucket also
        // covers control bytes and alignment for these 16-byte key/value pairs.
        let buckets = rows
            .checked_mul(8)
            .and_then(|n| n.div_ceil(7).max(8).checked_next_power_of_two())
            .ok_or_else(|| codec::vector_err("native metadata capacity overflow"))?;
        let maps = if self.quantizer().is_none() {
            3usize
        } else {
            2
        };
        let bytes = rows
            .checked_mul(std::mem::size_of::<Node>() + std::mem::size_of::<NavigationGroup>() + 32)
            .and_then(|n| {
                buckets
                    .checked_mul(32 * maps)
                    .and_then(|h| n.checked_add(h))
            })
            .ok_or_else(|| codec::vector_err("native metadata reservation overflow"))?;
        state.charge.try_grow_recoverable(bytes)?;
        let allocation_error = |_| {
            crate::storage::error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "native paged metadata allocation failed",
            )
        };
        state
            .nodes
            .try_reserve_exact(rows)
            .map_err(allocation_error)?;
        state
            .groups
            .try_reserve_exact(rows)
            .map_err(allocation_error)?;
        state.rows.try_reserve(rows).map_err(allocation_error)?;
        state.by_vid.try_reserve(rows).map_err(allocation_error)?;
        if self.quantizer().is_none() {
            state
                .group_heads
                .try_reserve(rows)
                .map_err(allocation_error)?;
        }
        Ok(())
    }

    fn restore_node(&self, state: &mut State, node: Node, prepaid: bool) -> Result<(), QueryError> {
        let id = state.nodes.len();
        if node.group > state.groups.len() || node.level > super::hnsw_paged::VEC_MAX_LAYER {
            return Err(super::persistence::corrupt());
        }
        if !prepaid {
            state.charge.try_grow_recoverable(
                1024 + node.value.as_ref().map_or(0, |v| v.len())
                    + node
                        .links
                        .iter()
                        .map(|links| links.len() * 8 + 24)
                        .sum::<usize>(),
            )?;
        }
        if node.group == state.groups.len() {
            let hash = if self.quantizer().is_none() && node.complete {
                let value = if let Some(value) = &node.value {
                    value.clone()
                } else {
                    self.file
                        .as_ref()
                        .ok_or_else(super::persistence::corrupt)?
                        .with_native_vector(node.vid, |value| {
                            if self.quantization_requested {
                                super::quantization::load_untrained(
                                    value.payload,
                                    self.file.as_ref().unwrap(),
                                    self.datatype,
                                    self.dimension,
                                )
                            } else {
                                Ok(VectorValue::new(
                                    self.datatype,
                                    self.dimension,
                                    value.payload.to_vec(),
                                ))
                            }
                        })?
                };
                Some(self.coordinate_hash(state, &value))
            } else {
                None
            };
            let next_collision = hash.and_then(|hash| state.group_heads.insert(hash, node.group));
            state.groups.push(NavigationGroup {
                representative: id,
                members: Vec::new(),
                next_collision,
            });
        }
        state.groups[node.group].members.push(id);
        if !node.deleted {
            state.rows.insert(node.row, id);
        }
        if node.exception && !node.deleted {
            state.exceptions += 1;
        }
        if state.by_vid.insert(node.vid, id).is_some() {
            return Err(super::persistence::corrupt());
        }
        state.nodes.push(node);
        Ok(())
    }

    pub(crate) fn load_paged(file: Arc<VectorIndexFile>) -> Result<Self, QueryError> {
        let graph = Self::from_definition(&file.read_native_definition()?, Some(file.clone()))?;
        let count =
            usize::try_from(file.node_count()).map_err(|_| super::persistence::corrupt())?;
        let _headers_charge = ControlledMemoryCharge::try_new_recoverable(
            ControlledMemoryCategory::StorageScratch,
            count.saturating_mul(32),
        )?;
        let mut headers = Vec::with_capacity(count);
        file.for_each_node(|vid, row, deleted| headers.push((vid, row, deleted)))?;
        if headers.len() != count {
            return Err(super::persistence::corrupt());
        }
        let mut state = graph.state.write();
        graph.reserve_paged_metadata(&mut state, count)?;
        for (vid, row, deleted) in headers {
            let (level, group, complete) = file.native_node_header(vid)?;
            if !complete && !deleted && file.is_finalized() && file.meta().query_ready {
                return Err(super::persistence::corrupt());
            }
            let exception = graph.quantizer().is_some()
                && file.with_native_vector(vid, |v| Ok(v.payload.first() == Some(&1)))?;
            graph.restore_node(
                &mut state,
                Node {
                    complete,
                    exception,
                    row,
                    group: usize::try_from(group).map_err(|_| super::persistence::corrupt())?,
                    level,
                    deleted: deleted || !complete,
                    vid,
                    value: None,
                    links: Vec::new(),
                },
                true,
            )?;
        }
        let meta = file.meta();
        if meta.entry_vid != u64::MAX {
            state.entry = Some(
                *state
                    .by_vid
                    .get(&meta.entry_vid)
                    .ok_or_else(super::persistence::corrupt)?,
            );
        } else if state.nodes.iter().any(|node| node.complete) {
            return Err(super::persistence::corrupt());
        }
        for group in &state.groups {
            for layer in 0..=state.nodes[group.representative].level {
                let _ = graph.links(&state, group.representative, layer)?;
            }
        }
        drop(state);
        Ok(graph)
    }

    pub(crate) fn serialize_into(
        &self,
        out: impl std::io::Write,
        covered_lsn: u64,
    ) -> std::io::Result<()> {
        if self.file.is_some() {
            return Err(std::io::Error::other(
                "paged HNSW does not use resident snapshots",
            ));
        }
        let state = self.state.read();
        let mut writer = super::persistence::Writer::new(out, 1)?;
        writer.bytes(&self.definition_bytes().map_err(std::io::Error::other)?)?;
        writer.u64(covered_lsn)?;
        writer.u64(state.entry.map_or(u64::MAX, |id| id as u64))?;
        writer.count(state.nodes.len())?;
        for node in &state.nodes {
            writer.row(node.row)?;
            writer.count(node.group)?;
            writer.count(node.level)?;
            writer.u8(u8::from(node.deleted))?;
            writer.u64(node.vid)?;
            writer.vector(
                node.value
                    .as_ref()
                    .ok_or_else(|| std::io::Error::other("missing resident HNSW payload"))?,
            )?;
            writer.count(node.links.len())?;
            for layer in &node.links {
                writer.count(layer.len())?;
                for id in layer {
                    writer.count(*id)?;
                }
            }
        }
        writer.finish()
    }

    pub(crate) fn deserialize(bytes: &[u8]) -> Result<Self, QueryError> {
        let mut reader = super::persistence::Reader::new(bytes, 1)?;
        let graph = Self::from_definition(reader.bytes()?, None)?;
        graph.persisted_lsn.store(reader.u64()?, Ordering::Release);
        let entry = reader.u64()?;
        let count = reader.count(35)?;
        let mut state = graph.state.write();
        for _ in 0..count {
            let row = reader.row()?;
            let group = reader.u32()? as usize;
            let level = reader.u32()? as usize;
            let deleted = match reader.u8()? {
                0 => false,
                1 => true,
                _ => return Err(super::persistence::corrupt()),
            };
            let vid = reader.u64()?;
            let value = reader.vector()?;
            if graph.quantizer().is_none() {
                codec::validate_internal(&value)?;
            }
            let exception = graph.quantizer().is_some() && value.payload.first() == Some(&1);
            let layers = reader.count(4)?;
            if layers > super::hnsw_paged::VEC_MAX_LAYER + 1 {
                return Err(super::persistence::corrupt());
            }
            let mut links = Vec::with_capacity(layers);
            for _ in 0..layers {
                let length = reader.count(4)?;
                let mut layer = Vec::with_capacity(length);
                for _ in 0..length {
                    let id = reader.u32()? as usize;
                    if id >= count {
                        return Err(super::persistence::corrupt());
                    }
                    layer.push(id);
                }
                links.push(layer);
            }
            graph.restore_node(
                &mut state,
                Node {
                    complete: true,
                    exception,
                    row,
                    group,
                    level,
                    deleted,
                    vid,
                    value: Some(value),
                    links,
                },
                false,
            )?;
        }
        if entry != u64::MAX {
            let entry = usize::try_from(entry).map_err(|_| super::persistence::corrupt())?;
            if entry >= count {
                return Err(super::persistence::corrupt());
            }
            state.entry = Some(entry);
        } else if count != 0 {
            return Err(super::persistence::corrupt());
        }
        reader.finish()?;
        drop(state);
        Ok(graph)
    }

    fn quantizer(&self) -> Option<&Arc<super::quantization::Quantizer>> {
        self.quantizer
            .as_ref()
            .or_else(|| self.trained_pending.get())
    }

    pub(crate) fn set_quantizer(
        &mut self,
        requested: bool,
        quantizer: Option<Arc<super::quantization::Quantizer>>,
        generation: u64,
    ) {
        self.quantization_requested = requested;
        self.generation.store(
            if quantizer.is_some() { generation } else { 0 },
            Ordering::Release,
        );
        self.quantizer = quantizer;
    }
    pub(crate) fn query_stats(&self) -> (u64, u64, u64, u64) {
        (
            self.generation.load(Ordering::Acquire),
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
        self.quantization_requested && self.quantizer().is_none()
    }
    pub(crate) fn quantization_stats(&self) -> (usize, usize, usize) {
        let state = self.state.read();
        (
            self.quantizer().map_or(0, |q| q.bytes()),
            self.quantizer()
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
            .quantizer()
            .ok_or_else(|| codec::vector_err("compressed builder requires a trained model"))?;
        let mut state = self.state.write();
        let mut vector = Vec::new();
        let mut links = Vec::new();
        let _encoding_scratch = ControlledMemoryCharge::try_new_recoverable(
            ControlledMemoryCategory::StorageScratch,
            if self.file.as_ref().is_some_and(|file| {
                file.supports_direct_bulk_build() && matches!(q.name(), "sq8" | "sq4")
            }) {
                self.dimension as usize * 24 + q.record_len() * 2 + 128
            } else {
                0
            },
        )?;
        let direct_vids =
            if let Some(file) = self.file.as_ref().filter(|file| {
                file.supports_direct_bulk_build() && matches!(q.name(), "sq8" | "sq4")
            }) {
                Some(file.bulk_build_native_direct(
                    topology.build_node_count(),
                    topology.build_entry(),
                    |id| topology.build_node_top_layer(id),
                    |id, payload, links| {
                        crate::engine::cancel::check_cancelled()?;
                        let row = topology.copy_build_node_into(id, &mut vector, links);
                        let value = q.store_with_reserved_scratch(
                            &codec::from_build_f32(&vector, self.datatype)?,
                            None,
                        )?;
                        // A training-set scalar payload must fit the model. Do not
                        // publish an inline exception as a persistent blob pointer.
                        if value.payload.first() != Some(&0) {
                            return Err(codec::vector_err(
                                "scalar build input changed after training",
                            ));
                        }
                        payload.clear();
                        payload.extend_from_slice(&value.payload);
                        Ok(row)
                    },
                )?)
            } else {
                None
            };
        if direct_vids.is_some() {
            let rows = topology.build_node_count();
            state
                .charge
                .try_grow_recoverable(rows.saturating_mul(704))?;
            let allocation_error = |_| {
                crate::storage::error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    "native build metadata allocation failed",
                )
            };
            state
                .nodes
                .try_reserve_exact(rows)
                .map_err(allocation_error)?;
            state
                .groups
                .try_reserve_exact(rows)
                .map_err(allocation_error)?;
            state.rows.try_reserve(rows).map_err(allocation_error)?;
            state.by_vid.try_reserve(rows).map_err(allocation_error)?;
        }
        for id in 0..topology.build_node_count() {
            crate::engine::cancel::check_cancelled()?;
            let row = if direct_vids.is_some() {
                topology.copy_build_links_into(id, &mut links)
            } else {
                topology.copy_build_node_into(id, &mut vector, &mut links)
            };
            let value = if direct_vids.is_some() {
                VectorValue::default()
            } else {
                q.store(
                    &codec::from_build_f32(&vector, self.datatype)?,
                    self.file.as_ref(),
                )?
            };
            let level = links.len().saturating_sub(1);
            if direct_vids.is_none() {
                state.charge.try_grow_recoverable(
                    704 + if self.file.is_none() {
                        value.len() + (level + 1) * (self.m * 2 * 8 + 24)
                    } else {
                        0
                    },
                )?;
            }
            let vid = if let Some(vids) = &direct_vids {
                vids[id]
            } else if let Some(file) = &self.file {
                file.append_native_node(0, row, level, &value.payload, id as u64)?
            } else {
                id as u64
            };
            let exception = value.payload.first() == Some(&1);
            if exception {
                state.exceptions += 1;
            }
            state.nodes.push(Node {
                complete: true,
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
        if direct_vids.is_none() {
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
            if let Some(file) = &self.file {
                for node in &state.nodes {
                    file.mark_node_linked(node.vid)?;
                }
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
    pub(crate) fn import_bulk_layout(
        &self,
        topology: &super::bulk_hnsw::BulkHnsw,
        vids: Vec<u64>,
    ) -> Result<(), QueryError> {
        if vids.len() != topology.len() || self.file.is_none() {
            return Err(super::persistence::corrupt());
        }
        let mut state = self.state.write();
        self.reserve_paged_metadata(&mut state, topology.len())?;
        for (id, vid) in vids.into_iter().enumerate() {
            let row = topology.row(id);
            state.nodes.push(Node {
                complete: true,
                exception: false,
                row,
                group: id,
                level: topology.level(id),
                deleted: false,
                vid,
                value: None,
                links: Vec::new(),
            });
            state.groups.push(NavigationGroup {
                representative: id,
                members: vec![id],
                next_collision: None,
            });
            state.rows.insert(row, id);
            state.by_vid.insert(vid, id);
        }
        state.entry = topology.entry().map(|(id, _)| id as usize);
        Ok(())
    }
    pub(crate) fn file(&self) -> Option<&Arc<VectorIndexFile>> {
        self.file.as_ref()
    }
    pub(crate) fn invalidate(&self) {
        self.valid.store(false, Ordering::Release);
    }
    pub(crate) fn lineage(&self) -> u64 {
        self.lineage
    }
    pub(crate) fn set_lineage(&mut self, lineage: u64) {
        self.lineage = lineage;
    }

    pub(crate) fn retraining_requested(&self) -> bool {
        self.retrain.load(Ordering::Acquire)
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
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
            file.prepare_navigation_mapping()?;
            // Dense adjacency avoids per-edge vid hashing even when payloads
            // are borrowed directly from the authoritative mapped image.
            self.prepare_csr()?;
        }
        self.prepare_query_cache();
        Ok(())
    }
    pub(crate) fn evict_query_navigation(&self) {
        if let Some(mut state) = self.state.try_write() {
            state.csr = None;
            state.query_cache = None;
        }
    }
    fn prepare_query_cache(&self) {
        if self.state.read().query_cache.is_some() {
            return;
        }
        let limit = std::env::var("MYDB_HNSW_QUERY_CACHE_MB")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .map(|mb| mb.saturating_mul(1024 * 1024))
            .unwrap_or_else(|| crate::engine::executor::mem_budget::memory_target_bytes() / 4);
        let Some((entries, bytes)) = native_query_memo_allocation(self.dimension as usize, limit)
        else {
            return;
        };
        let mut state = self.state.write();
        if state.query_cache.is_some() || state.nodes.is_empty() {
            return;
        }
        if let Some(charge) =
            ControlledMemoryCharge::try_new_optional(ControlledMemoryCategory::DerivedCache, bytes)
        {
            state.query_cache = Some(NativeQueryCache {
                cache: super::hnsw_paged::QueryNavCache::with_capacity(
                    NATIVE_MEMO_MAX_CANDIDATES,
                    entries,
                ),
                gates: std::array::from_fn(|_| Mutex::new(())),
                _charge: charge,
            });
        }
    }
    fn prepare_csr(&self) -> Result<(), QueryError> {
        if self.state.read().csr.is_some() {
            return Ok(());
        }
        let mut state = self.state.write();
        if state.csr.is_some() || state.nodes.is_empty() || state.nodes.len() > u32::MAX as usize {
            return Ok(());
        }
        let bytes = state
            .nodes
            .len()
            .saturating_mul(
                self.m
                    .saturating_mul(2)
                    .saturating_mul(std::mem::size_of::<u32>())
                    .saturating_add(std::mem::size_of::<usize>()),
            )
            .saturating_add(8);
        let limit = super::hnsw_paged::PagedHnsw::query_navigation_limit_bytes();
        let stride = self.quantizer().map_or(0, |quant| quant.record_len());
        let payload_bytes = state.nodes.len().saturating_mul(stride);
        let combined = bytes.saturating_add(payload_bytes);
        let reserve = |bytes| {
            self.file.as_ref().map_or_else(
                || {
                    ControlledMemoryCharge::try_new_optional(
                        ControlledMemoryCategory::DerivedCache,
                        bytes,
                    )
                },
                |file| file.reserve_navigation_cache(bytes),
            )
        };
        let resident = (stride != 0 && combined <= limit)
            .then(|| reserve(combined))
            .flatten();
        if std::env::var_os("MYDB_HNSW_BUILD_TIMING").is_some() {
            eprintln!("[hnsw] query navigator: nodes={} stride={stride} bytes={combined} limit={limit} payloads={}", state.nodes.len(), resident.is_some());
        }
        let (mut charge, reserve_payloads) = if let Some(charge) = resident {
            (charge, true)
        } else if bytes <= limit {
            let Some(charge) = reserve(bytes) else {
                return Ok(());
            };
            (charge, false)
        } else {
            return Ok(());
        };
        let reserved = if reserve_payloads { combined } else { bytes };
        let mut payloads = if reserve_payloads {
            let mut payloads = Vec::new();
            payloads
                .try_reserve_exact(payload_bytes)
                .map_err(|_| codec::vector_err("native HNSW payload cache allocation failed"))?;
            Some(payloads)
        } else {
            None
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
            crate::engine::cancel::check_cancelled()?;
            for edge in self.links(&state, id, 0)? {
                edges.push(u32::try_from(edge).map_err(|_| super::persistence::corrupt())?);
            }
            offsets.push(edges.len());
            if let Some(payloads) = &mut payloads {
                let node = &state.nodes[id];
                let mut append = |bytes: &[u8]| {
                    if bytes.len() != stride {
                        return Err(super::persistence::corrupt());
                    }
                    payloads.extend_from_slice(bytes);
                    Ok(())
                };
                if let Some(value) = &node.value {
                    append(&value.payload)?;
                } else {
                    self.file
                        .as_ref()
                        .ok_or_else(super::persistence::corrupt)?
                        .with_native_vector(node.vid, |value| append(value.payload))?;
                }
            }
        }
        let actual = (offsets.capacity() * std::mem::size_of::<usize>()
            + edges.capacity() * std::mem::size_of::<u32>())
        .saturating_add(payloads.as_ref().map_or(0, Vec::capacity));
        charge.shrink(reserved.saturating_sub(actual));
        state.csr = Some(NativeCsr {
            offsets,
            edges,
            payloads,
            payload_stride: stride,
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
        if let Some(quant) = self.quantizer() {
            let node = &state.nodes[id];
            if let Some(csr) = &state.csr {
                if let Some(payloads) = &csr.payloads {
                    let begin = id * csr.payload_stride;
                    return quant.score_payload(
                        &payloads[begin..begin + csr.payload_stride],
                        self.file.as_ref(),
                        query,
                        query_code,
                    );
                }
            }
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
        if self.quantizer().is_some() {
            return self.navigation_score(state, id, query, None);
        }
        let node = &state.nodes[id];
        match (&node.value, &self.file) {
            (Some(value), _) => typed_ordering_distance(query, value, self.metric),
            (_, Some(_)) if self.quantization_requested => {
                typed_ordering_distance(query, &self.value(state, id)?, self.metric)
            }
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
        let Some(q) = self.quantizer() else {
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
        if let Some(quant) = self.quantizer() {
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
                if self.quantization_requested {
                    return super::quantization::load_untrained(
                        v.payload,
                        self.file.as_ref().unwrap(),
                        self.datatype,
                        self.dimension,
                    );
                }
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
                return Ok(csr.edges[csr.offsets[id]..csr.offsets[id + 1]]
                    .iter()
                    .map(|&id| id as usize)
                    .collect());
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
            let compact = state
                .csr
                .as_ref()
                .filter(|_| level == 0)
                .map(|csr| &csr.edges[csr.offsets[entry]..csr.offsets[entry + 1]]);
            let owned = if compact.is_none() {
                self.links(state, entry, level)?
            } else {
                Vec::new()
            };
            for next in compact
                .into_iter()
                .flat_map(|edges| edges.iter().map(|&id| id as usize))
                .chain(owned)
            {
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
        self.layer_scored(
            state,
            entries,
            ef,
            level,
            self.explore_query_ties(query),
            |id| self.navigation_score(state, id, query, query_code),
        )
    }

    fn explore_query_ties(&self, query: &VectorValue) -> bool {
        matches!(self.metric, Metric::L2 | Metric::Cosine | Metric::Dot)
            && !(self.metric == Metric::Dot
                && query.dimension > 0
                && (0..query.dimension as usize).all(|index| query.element(index) == 0.0))
    }

    fn layer_scored(
        &self,
        state: &State,
        entries: &[usize],
        ef: usize,
        level: usize,
        explore_ties: bool,
        mut score_node: impl FnMut(usize) -> Result<f64, QueryError>,
    ) -> Result<Vec<Scored<usize>>, QueryError> {
        let ef = ef.max(1).min(state.nodes.len());
        if ef == 0 {
            return Ok(Vec::new());
        }
        NATIVE_SEARCH_SCRATCH.with(|pool| {
            let mut cached = pool.borrow_mut();
            let mut scratch = match cached
                .take()
                .filter(|s| s.visited.len() >= state.nodes.len().div_ceil(64))
            {
                Some(mut scratch) => {
                    scratch.visited.fill(0);
                    scratch.todo.clear();
                    scratch.best.clear();
                    scratch
                }
                None => NativeSearchScratch::new(state.nodes.len(), ef)?,
            };
            for &id in entries {
                if scratch.visit(id)? {
                    let score = Scored {
                        dist: score_node(id)?,
                        id,
                    };
                    if scratch.best.len() < ef
                        || scratch.best.peek().is_some_and(|worst| score < *worst)
                    {
                        scratch.push(score, ef, true)?;
                    } else if explore_ties
                        && level == 0
                        && (score.dist != 0.0 || self.metric == Metric::Dot)
                        && scratch
                            .best
                            .peek()
                            .is_some_and(|worst| score.dist == worst.dist)
                    {
                        scratch.push(score, ef, false)?;
                    }
                }
            }
            let mut steps = 0usize;
            while let Some(Reverse(current)) = scratch.todo.pop() {
                if scratch.best.len() >= ef
                    && scratch
                        .best
                        .peek()
                        .is_some_and(|worst: &Scored<usize>| current.dist > worst.dist)
                {
                    break;
                }
                let compact =
                    state.csr.as_ref().filter(|_| level == 0).map(|csr| {
                        &csr.edges[csr.offsets[current.id]..csr.offsets[current.id + 1]]
                    });
                let owned = if compact.is_none() {
                    self.links(state, current.id, level)?
                } else {
                    Vec::new()
                };
                for id in compact
                    .into_iter()
                    .flat_map(|edges| edges.iter().map(|&id| id as usize))
                    .chain(owned)
                {
                    if !scratch.visit(id)? {
                        continue;
                    }
                    let score = Scored {
                        dist: score_node(id)?,
                        id,
                    };
                    if scratch.best.len() < ef
                        || scratch.best.peek().is_some_and(|worst| score < *worst)
                    {
                        scratch.push(score, ef, true)?;
                    } else if explore_ties
                        && level == 0
                        && (score.dist != 0.0 || self.metric == Metric::Dot)
                        && scratch
                            .best
                            .peek()
                            .is_some_and(|worst| score.dist == worst.dist)
                    {
                        scratch.push(score, ef, false)?;
                    }
                }
                steps += 1;
                if steps & 63 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
            }
            let mut found: Vec<_> = scratch.best.drain().collect();
            found.sort_unstable();
            let limit = (crate::engine::executor::mem_budget::memory_target_bytes() / 16)
                .min(64 * 1024 * 1024);
            if scratch.charge.bytes() <= limit {
                *cached = Some(scratch);
            }
            Ok(found)
        })
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
            .with_native_vector(state.nodes[representative].vid, |v| {
                if self.quantization_requested {
                    let original = super::quantization::load_untrained(
                        v.payload,
                        self.file.as_ref().unwrap(),
                        self.datatype,
                        self.dimension,
                    )?;
                    Ok(equal(&original.payload))
                } else {
                    Ok(equal(v.payload))
                }
            })
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
            let stored = if self.quantization_requested {
                super::quantization::store_untrained(&value, Some(file))?
            } else {
                value.clone()
            };
            file.append_native_node(0, row, 0, &stored.payload, group as u64)?
        } else {
            id as u64
        };
        state.nodes.push(Node {
            complete: true,
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
        if let Some(file) = &self.file {
            file.mark_node_linked(vid)?;
        }
        state.rows.insert(row, id);
        Ok(())
    }
    pub(crate) fn insert(&self, row: RowId, value: VectorValue) -> Result<(), QueryError> {
        codec::validate_internal(&value)?;
        if self.metric == Metric::Cosine && super::distance::typed_is_zero(&value) {
            return Err(codec::vector_err("COSINE indexes reject zero vectors"));
        }
        if value.datatype != self.datatype || value.dimension != self.dimension {
            return Err(codec::vector_err("native HNSW datatype/dimension mismatch"));
        }
        let mut state = self.state.write();
        state.csr = None;
        state.query_cache = None;
        if let Some(&old) = state.rows.get(&row) {
            self.delete_locked(&mut state, old)?;
        }
        let hash = self.coordinate_hash(&state, &value);
        let mut matching = if self.quantizer().is_some() {
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
                self.quantizer().map_or(value.len(), |q| q.record_len())
                    + (level + 1) * (self.m * 2 * 8 + 24)
            };
        state.charge.try_grow_recoverable(bytes)?;
        state
            .nodes
            .try_reserve(1)
            .map_err(|_| codec::vector_err("native HNSW node allocation failed"))?;
        let prepared = self.quantizer().map(|q| q.prepare(&value)).transpose()?;
        let mut entry = state.entry;
        if let Some(mut current) = entry {
            for layer in ((level + 1)..=state.nodes[current].level).rev() {
                current = self.greedy(&state, &value, current, layer, prepared.as_ref())?;
            }
            entry = Some(current);
        }
        let encoded = if let Some(quantizer) = self.quantizer() {
            Some(quantizer.store(&value, self.file.as_ref())?)
        } else if self.quantization_requested {
            Some(super::quantization::store_untrained(
                &value,
                self.file.as_ref(),
            )?)
        } else {
            None
        };
        let payload = encoded.as_ref().unwrap_or(&value);
        if self.file.is_none() && self.quantizer().is_some() {
            state.charge.try_grow_recoverable(
                payload
                    .len()
                    .saturating_sub(self.quantizer().unwrap().record_len()),
            )?;
        }
        let vid = if let Some(file) = &self.file {
            file.append_native_node(0, row, level, &payload.payload, group as u64)?
        } else {
            id as u64
        };
        #[cfg(test)]
        if let Some(file) = &self.file {
            file.crash_point_for_test(5)?;
        }
        let stored = if self.file.is_some() {
            None
        } else {
            Some(payload.clone())
        };
        if self.quantizer().is_some() && payload.payload.first() == Some(&1) {
            state.exceptions += 1;
            if state.nodes.len() >= self.quantizer().unwrap().trained_rows() {
                state.incremental_exceptions += 1;
            }
        }
        state.nodes.push(Node {
            complete: true,
            exception: self.quantizer().is_some() && payload.payload.first() == Some(&1),
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
        if let Some(file) = &self.file {
            file.mark_node_linked(vid)?;
        }
        state.rows.insert(row, id);
        if let Some(q) = self.quantizer() {
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
        state.query_cache = None;
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
        if !self.is_valid() {
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
                .saturating_add(self.dimension as usize * 4 + 12)
                .saturating_add(
                    request
                        .candidate_limit
                        .min(state.rows.len())
                        .saturating_mul(std::mem::size_of::<(RowId, f64)>()),
                ),
        )?;
        // Cache graph candidates only. The executor still checks heap/MVCC
        // visibility and exact native distances on every statement. Holding
        // the state read lock pairs cache lifetime with DML invalidation.
        let limit = request.candidate_limit.min(state.rows.len());
        let cache_key = state.query_cache.as_ref().map(|_| query.encode());
        if let (Some(cache), Some(key)) = (&state.query_cache, &cache_key) {
            if let Some(candidates) = cache.cache.get_bytes(key, limit, ef) {
                return Ok(VectorSearchResult {
                    candidates,
                    exact_order: self.quantizer().is_none(),
                    exhausted: ef >= state.groups.len(),
                });
            }
        }
        let _compute = if limit <= NATIVE_MEMO_MAX_CANDIDATES {
            match (&state.query_cache, &cache_key) {
                (Some(cache), Some(key)) => {
                    let slot = super::hnsw_paged::QueryNavCache::hash_bytes(key, limit, ef)
                        as usize
                        % NATIVE_MEMO_GATES;
                    Some(cache.gates[slot].lock())
                }
                _ => None,
            }
        } else {
            None
        };
        if _compute.is_some() {
            if let (Some(cache), Some(key)) = (&state.query_cache, &cache_key) {
                if let Some(candidates) = cache.cache.get_bytes(key, limit, ef) {
                    return Ok(VectorSearchResult {
                        candidates,
                        exact_order: self.quantizer().is_none(),
                        exhausted: ef >= state.groups.len(),
                    });
                }
            }
        }
        let query_code = self.quantizer().map(|q| q.prepare(query)).transpose()?;
        for layer in (1..=state.nodes[entry].level).rev() {
            entry = self.greedy(&state, query, entry, layer, query_code.as_ref())?;
        }
        // A dense level-0 adjacency lets this traversal retain one mapping
        // guard, avoiding a contended shared-lock operation for every score.
        // Upper-layer traversal stays outside this guard, so it never takes
        // a recursive mapping read while a writer is waiting.
        let mapped = if state.csr.as_ref().is_some_and(|csr| csr.payloads.is_none()) {
            if let (Some(file), Some(quant)) = (&self.file, self.quantizer()) {
                file.with_navigation_mapping(|view| {
                    self.layer_scored(
                        &state,
                        &[entry],
                        ef,
                        0,
                        self.explore_query_ties(query),
                        |id| {
                            quant.score_payload(
                                view.native_payload(state.nodes[id].vid)?,
                                self.file.as_ref(),
                                query,
                                query_code.as_ref(),
                            )
                        },
                    )
                })
            } else {
                None
            }
        } else {
            None
        };
        let pool = match mapped {
            Some(result) => result?,
            None => self.layer(&state, query, &[entry], ef, 0, query_code.as_ref())?,
        };
        let exhausted = ef >= state.groups.len();
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
        if let (Some(cache), Some(key)) = (&state.query_cache, &cache_key) {
            cache.cache.insert_bytes(key, limit, ef, &candidates);
        }
        Ok(VectorSearchResult {
            candidates,
            exact_order: self.quantizer().is_none(),
            exhausted,
        })
    }
}

#[cfg(test)]
mod duplicate_regressions {
    use super::*;

    #[test]
    fn resident_payload_navigation_matches_row_scoring_and_invalidates_on_insert() {
        for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
            for config in [
                crate::storage::vector_config::QuantizationConfig::Sq4,
                crate::storage::vector_config::QuantizationConfig::Sq8,
            ] {
                let rows: Vec<_> = (0..64)
                    .map(|row| {
                        codec::parse_text(&format!(
                            "[{},1,{},-3,2,{},5]",
                            row + 1,
                            row % 7,
                            row % 13
                        ))
                        .unwrap()
                    })
                    .collect();
                let mut trainer = super::super::quantization::Trainer::new(
                    config,
                    VectorDataType::Float32,
                    7,
                    metric,
                )
                .unwrap();
                for row in &rows {
                    trainer.observe(row).unwrap();
                }
                let mut graph =
                    NativeHnsw::new(VectorDataType::Float32, 7, metric, 4, 32, None).unwrap();
                graph.set_quantizer(true, trainer.finish().unwrap(), 1);
                for (id, row) in rows.iter().enumerate() {
                    graph
                        .insert(
                            RowId {
                                page_id: id as u32,
                                slot_id: 0,
                            },
                            row.clone(),
                        )
                        .unwrap();
                }
                let request = VectorSearchRequest {
                    candidate_limit: 10,
                    rerank_limit: 32,
                    effort: super::super::VectorSearchEffort::Hnsw { ef: 32 },
                };
                let expected: Vec<_> = rows[..8]
                    .iter()
                    .map(|row| graph.search(row, request).unwrap().candidates)
                    .collect();
                graph.prepare_csr().unwrap();
                assert!(graph.state.read().csr.as_ref().unwrap().payloads.is_some());
                graph.state.write().query_cache = None;
                for (row, want) in rows[..8].iter().zip(&expected) {
                    assert_eq!(graph.search(row, request).unwrap().candidates, *want);
                }
                let deleted = expected[0][0].0;
                graph.delete(deleted).unwrap();
                assert!(!graph
                    .search(&rows[0], request)
                    .unwrap()
                    .candidates
                    .iter()
                    .any(|(row, _)| *row == deleted));
                graph
                    .insert(
                        RowId {
                            page_id: 128,
                            slot_id: 0,
                        },
                        rows[0].clone(),
                    )
                    .unwrap();
                assert!(graph.state.read().csr.is_none());
                assert!(graph.search(&rows[0], request).is_ok());
            }
        }
    }

    #[test]
    fn native_search_scratch_accounts_bitmap_and_heap_growth() {
        let mut scratch = NativeSearchScratch::new(1_000_000, 1).unwrap();
        for id in [0, 63, 64, 999_999] {
            assert!(scratch.visit(id).unwrap());
            assert!(!scratch.visit(id).unwrap());
        }
        assert!(scratch.visit(1_000_000).is_err());
        for id in 0..256 {
            scratch
                .push(
                    Scored {
                        id,
                        dist: -(id as f64),
                    },
                    16,
                    true,
                )
                .unwrap();
        }
        assert_eq!(scratch.best.len(), 16);
        assert!(
            scratch.charge.bytes()
                >= scratch.visited.capacity() * 8
                    + (scratch.todo.capacity() + scratch.best.capacity())
                        * std::mem::size_of::<Scored<usize>>()
        );
        assert!(scratch.charge.bytes() < 256 * 1024);
    }
    #[test]
    fn native_memo_capacity_stays_within_its_memory_slice() {
        for dimension in [1, 7, 768, 16383, 65532] {
            for limit in [
                1024,
                1024 * 1024,
                256 * 1024 * 1024,
                4 * 1024 * 1024 * 1024usize,
            ] {
                if let Some((entries, bytes)) = native_query_memo_allocation(dimension, limit) {
                    assert!((8..=4096).contains(&entries));
                    assert_eq!(entries % 8, 0);
                    assert!(bytes <= (limit / 16).min(64 * 1024 * 1024));
                }
            }
        }
        assert!(native_query_memo_allocation(usize::MAX, usize::MAX).is_none());
        assert!(native_query_memo_allocation(768, 0).is_none());
    }

    #[test]
    fn native_candidate_memo_invalidates_on_insert_delete_and_separates_domains() {
        let graph = NativeHnsw::new(VectorDataType::Float32, 2, Metric::L2, 4, 64, None).unwrap();
        for id in 0..16 {
            graph
                .insert(
                    RowId {
                        page_id: id,
                        slot_id: 0,
                    },
                    codec::parse_text(&format!("[{},1]", id + 1)).unwrap(),
                )
                .unwrap();
        }
        let query = codec::parse_text("[0,1]").unwrap();
        let request = VectorSearchRequest {
            candidate_limit: 1,
            rerank_limit: 1,
            effort: super::super::VectorSearchEffort::Hnsw { ef: 16 },
        };
        let before = graph.search(&query, request).unwrap().candidates;
        graph.state.write().query_cache = None;
        let barrier = std::sync::Barrier::new(16);
        std::thread::scope(|scope| {
            let peers: Vec<_> = (0..16)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        graph.search(&query, request).unwrap().candidates
                    })
                })
                .collect();
            for peer in peers {
                assert_eq!(peer.join().unwrap(), before);
            }
        });
        {
            let state = graph.state.read();
            let cache = &state.query_cache.as_ref().unwrap().cache;
            assert_eq!(
                cache.get_bytes(&query.encode(), 1, 16),
                Some(before.clone())
            );
            let other_domain = codec::parse_typed_text("[0,1]", VectorDataType::Float16).unwrap();
            assert!(cache.get_bytes(&other_domain.encode(), 1, 16).is_none());
            assert!(cache.get_bytes(&query.encode(), 2, 16).is_none());
            assert!(cache.get_bytes(&query.encode(), 1, 8).is_none());
        }
        let added = RowId {
            page_id: 99,
            slot_id: 0,
        };
        graph.insert(added, query.clone()).unwrap();
        assert!(graph.state.read().query_cache.is_none());
        assert_eq!(
            graph.search(&query, request).unwrap().candidates[0].0,
            added
        );
        graph.delete(added).unwrap();
        assert!(graph.state.read().query_cache.is_none());
        assert_eq!(graph.search(&query, request).unwrap().candidates, before);
    }

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
