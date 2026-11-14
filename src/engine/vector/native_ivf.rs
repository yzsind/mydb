//! Native IVF lists with numeric and binary coarse quantizers.
use super::centroids::{assignment_threads, PreparedCentroids, EXACT};
use super::hnsw_core::Scored;
use super::scan_codes::{self, ScanCodec, ScanQuery};
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
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};

/// Per-row scan metadata. List scans stream these in list order, so the
/// memory backend's payloads live behind one pointer to keep entries small.
#[derive(Debug)]
struct Entry {
    exception: bool,
    deleted: bool,
    /// Cached FLOAT32 cosine squared norm (`f32` bits, `0` = not yet known).
    /// Entries are immutable once appended, so the first scan may publish it.
    norm: AtomicU32,
    row: RowId,
    vid: u64,
    /// Location of the paged scan-code record ([`NO_CODE`] when absent).
    code: u64,
    /// The memory backend's stored payload and scan code.
    resident: Option<Box<Resident>>,
}

#[derive(Debug)]
struct Resident {
    value: VectorValue,
    code: Option<Box<[u8]>>,
}

/// `Entry::code` of a row without a scan code; it is always scored exactly.
const NO_CODE: u64 = u64::MAX;
/// `State::scan_words` of a deleted entry. Code locations never reach it.
const DELETED_WORD: u64 = u64::MAX - 1;

/// What phase one of a coded scan does with an entry.
enum Coded<'p> {
    Skip,
    Exact,
    Code(Result<&'p [u8], QueryError>),
}

impl Entry {
    fn new(
        exception: bool,
        row: RowId,
        value: Option<VectorValue>,
        vid: u64,
        deleted: bool,
    ) -> Self {
        Self {
            exception,
            deleted,
            norm: AtomicU32::new(0),
            row,
            vid,
            code: NO_CODE,
            resident: value.map(|value| Box::new(Resident { value, code: None })),
        }
    }

    /// The memory backend's stored payload.
    #[inline]
    fn value(&self) -> Option<&VectorValue> {
        self.resident.as_ref().map(|r| &r.value)
    }

    /// The memory backend's scan-code record.
    #[inline]
    fn code_bytes(&self) -> Option<&[u8]> {
        self.resident.as_ref().and_then(|r| r.code.as_deref())
    }

    #[cfg(test)]
    fn has_code(&self) -> bool {
        self.code != NO_CODE || self.code_bytes().is_some()
    }
}

/// Rows per batch of the parallel bulk-insert pipeline.
pub(crate) const BULK_INSERT_BATCH: usize = 16 * 1024;
#[derive(Debug)]
struct State {
    cursor_epoch: u64,
    exceptions: usize,
    incremental_exceptions: usize,
    entries: Vec<Entry>,
    rows: HashMap<RowId, usize>,
    centers: Vec<VectorValue>,
    /// Blocked ranking form of `centers` (`None` for BINARY or exceptional
    /// centroid domains, which keep exact scoring).
    prepared: Option<PreparedCentroids>,
    prepared_charge: Option<ControlledMemoryCharge>,
    /// Bumped whenever `centers` is replaced.
    centers_epoch: u64,
    lists: Vec<Vec<usize>>,
    list_pages: Vec<AtomicU64>,
    /// Per entry: its code location, [`NO_CODE`] or [`DELETED_WORD`]. Phase
    /// one of a paged coded scan reads only these words, not the entries.
    scan_words: Vec<u64>,
    /// Scan-code codec of an unquantized numeric index, fixed at training.
    codec: Option<Arc<ScanCodec>>,
    /// Current scan-code page of each list.
    code_pages: Vec<AtomicU64>,
    charge: ControlledMemoryCharge,
}

impl State {
    fn push_entry(&mut self, entry: Entry) {
        self.scan_words.push(if entry.deleted {
            DELETED_WORD
        } else {
            entry.code
        });
        self.entries.push(entry);
    }

    fn mark_deleted(&mut self, id: usize) {
        self.entries[id].deleted = true;
        self.scan_words[id] = DELETED_WORD;
    }

    fn set_code(&mut self, id: usize, location: u64) {
        self.entries[id].code = location;
        if !self.entries[id].deleted {
            self.scan_words[id] = location;
        }
    }

    /// Replace the coarse quantizer and its prepared ranking form.
    fn set_centers(
        &mut self,
        centers: Vec<VectorValue>,
        datatype: VectorDataType,
        metric: Metric,
    ) -> Result<(), QueryError> {
        let prepared = prepare_centers(&centers, datatype, metric)?;
        self.install_centers(centers, prepared);
        Ok(())
    }

    fn install_centers(&mut self, centers: Vec<VectorValue>, prepared: PreparedCenters) {
        (self.prepared, self.prepared_charge) = match prepared {
            Some((prepared, charge)) => (Some(prepared), Some(charge)),
            None => (None, None),
        };
        self.centers = centers;
        self.centers_epoch = self.centers_epoch.wrapping_add(1);
    }
}

type PreparedCenters = Option<(PreparedCentroids, ControlledMemoryCharge)>;

/// Blocked ranking form of numeric centers, charged to storage scratch.
fn prepare_centers(
    centers: &[VectorValue],
    datatype: VectorDataType,
    metric: Metric,
) -> Result<PreparedCenters, QueryError> {
    if datatype == VectorDataType::Binary || centers.is_empty() {
        return Ok(None);
    }
    let dim = centers[0].dimension as usize;
    let charge = ControlledMemoryCharge::try_new_recoverable(
        ControlledMemoryCategory::StorageScratch,
        PreparedCentroids::bytes_for(centers.len(), dim),
    )?;
    let mut flat = Vec::with_capacity(centers.len() * dim);
    for center in centers {
        if center.dimension as usize != dim {
            return Ok(None);
        }
        flat.extend(codec::to_f32(center));
    }
    Ok(PreparedCentroids::new(flat, dim, metric).map(|prepared| (prepared, charge)))
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
                cursor_epoch: 0,
                exceptions: 0,
                incremental_exceptions: 0,
                entries: Vec::new(),
                rows: HashMap::new(),
                centers: Vec::new(),
                prepared: None,
                prepared_charge: None,
                centers_epoch: 0,
                lists: vec![Vec::new()],
                list_pages: vec![AtomicU64::new(0)],
                scan_words: Vec::new(),
                codec: None,
                code_pages: vec![AtomicU64::new(0)],
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
        let state = self.state.read();
        self.definition_bytes_for(
            self.quantizer(),
            self.generation.load(Ordering::Acquire),
            &state.centers,
            state.codec.as_deref(),
        )
    }
    fn definition_bytes_for(
        &self,
        model: Option<&Arc<super::quantization::Quantizer>>,
        generation: u64,
        centers: &[VectorValue],
        codec: Option<&ScanCodec>,
    ) -> Result<Vec<u8>, QueryError> {
        let mut out = Vec::new();
        let mut writer =
            super::persistence::Writer::new(&mut out, 4).map_err(super::persistence::io_error)?;
        writer
            .u8(self.datatype as u8)
            .map_err(super::persistence::io_error)?;
        writer
            .u32(self.dimension)
            .map_err(super::persistence::io_error)?;
        writer
            .u8(self.metric.as_u8())
            .map_err(super::persistence::io_error)?;
        for value in [
            self.params.nlist,
            self.params.kmeans_iters,
            self.params.train_sample_mult,
        ] {
            writer.count(value).map_err(super::persistence::io_error)?;
        }
        writer
            .u8(u8::from(self.quantization_requested))
            .map_err(super::persistence::io_error)?;
        writer
            .u64(self.lineage)
            .map_err(super::persistence::io_error)?;
        writer
            .u64(generation)
            .map_err(super::persistence::io_error)?;
        let model = model
            .map(|q| q.serialize(None))
            .transpose()?
            .unwrap_or_default();
        writer.bytes(&model).map_err(super::persistence::io_error)?;
        writer
            .count(centers.len())
            .map_err(super::persistence::io_error)?;
        for center in centers {
            writer
                .vector(center)
                .map_err(super::persistence::io_error)?;
        }
        writer
            .bytes(&codec.map(ScanCodec::serialize).unwrap_or_default())
            .map_err(super::persistence::io_error)?;
        writer.finish().map_err(super::persistence::io_error)?;
        Ok(out)
    }

    fn from_definition(
        bytes: &[u8],
        file: Option<Arc<VectorIndexFile>>,
    ) -> Result<Self, QueryError> {
        let mut reader = super::persistence::Reader::new(bytes, 4)?;
        let datatype =
            VectorDataType::from_tag(reader.u8()?).ok_or_else(super::persistence::corrupt)?;
        let dimension = reader.u32()?;
        let metric = Metric::from_u8(reader.u8()?).ok_or_else(super::persistence::corrupt)?;
        let params = IvfParams {
            nlist: reader.u32()? as usize,
            kmeans_iters: reader.u32()? as usize,
            train_sample_mult: reader.u32()? as usize,
        };
        let requested = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(super::persistence::corrupt()),
        };
        let lineage = reader.u64()?;
        let generation = reader.u64()?;
        let model = reader.bytes()?;
        let quantizer = if model.is_empty() {
            None
        } else {
            Some(super::quantization::Quantizer::deserialize(model)?)
        };
        if dimension == 0
            || dimension > datatype.max_dimension()
            || params.kmeans_iters == 0
            || params.train_sample_mult == 0
        {
            return Err(super::persistence::corrupt());
        }
        let mut graph = Self::new(datatype, dimension, metric, params, file)?;
        graph.lineage = lineage;
        graph.set_quantizer(requested, quantizer, generation);
        let count = reader.count(9)?;
        let mut state = graph.state.write();
        let mut centers = Vec::new();
        for _ in 0..count {
            let center = reader.vector()?;
            codec::validate_internal(&center)?;
            if center.dimension != dimension
                || (center.datatype == VectorDataType::Binary)
                    != (datatype == VectorDataType::Binary)
            {
                return Err(super::persistence::corrupt());
            }
            state.charge.try_grow_recoverable(center.len() + 96)?;
            centers.push(center);
        }
        state.set_centers(centers, datatype, metric)?;
        let lists = count.max(1);
        state.lists = (0..lists).map(|_| Vec::new()).collect();
        state.list_pages = (0..lists).map(|_| AtomicU64::new(0)).collect();
        state.code_pages = (0..lists).map(|_| AtomicU64::new(0)).collect();
        let codec = reader.bytes()?;
        if !codec.is_empty() {
            let codec = ScanCodec::deserialize(codec)?;
            if !graph.codes_eligible()
                || codec.dimension() != dimension as usize
                || state.centers.is_empty()
            {
                return Err(super::persistence::corrupt());
            }
            state.charge.try_grow_recoverable(codec.allocated_bytes())?;
            state.codec = Some(Arc::new(codec));
        }
        reader.finish()?;
        drop(state);
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
        for (id, node) in state.entries.iter().enumerate() {
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
        state.cursor_epoch = state.cursor_epoch.wrapping_add(1);
        if self.quantizer().is_some() {
            if self.generation.load(Ordering::Acquire) >= generation {
                return Ok(());
            }
            return Err(super::persistence::corrupt());
        }
        if !self.quantization_requested {
            return Err(super::persistence::corrupt());
        }
        let count = state.entries.len();
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
                .map_err(|_| codec::vector_err("pending IVF conversion allocation failed"))?;
            for node in &state.entries {
                let old = node.value().ok_or_else(super::persistence::corrupt)?;
                let value = model.store(old, None)?;
                converted_charge.try_grow_recoverable(value.len())?;
                old_bytes = old_bytes.saturating_add(old.len());
                converted.push(value);
            }
        }
        if let Some(file) = &self.file {
            file.write_quantization_model(&model.serialize(None)?, generation)?;
            file.write_native_definition(&self.definition_bytes_for(
                Some(&model),
                generation,
                &state.centers,
                state.codec.as_deref(),
            )?)?;
        } else {
            for (node, value) in state.entries.iter_mut().zip(converted) {
                node.exception = value.payload.first() == Some(&1);
                node.resident = Some(Box::new(Resident { value, code: None }));
            }
            state.charge.absorb(&mut converted_charge);
            state
                .charge
                .shrink(old_bytes.saturating_add(count.saturating_mul(24)));
        }
        if self.file.is_some() {
            for node in &mut state.entries {
                node.exception = true;
            }
        }
        state.exceptions = state
            .entries
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
        params: IvfParams,
    ) -> bool {
        self.datatype == datatype
            && self.dimension == dimension
            && self.metric == metric
            && self.params.nlist == params.nlist
            && self.params.kmeans_iters == params.kmeans_iters
            && self.params.train_sample_mult == params.train_sample_mult
    }

    pub(crate) fn load_paged(file: Arc<VectorIndexFile>) -> Result<Self, QueryError> {
        let graph = Self::from_definition(&file.read_native_definition()?, Some(file.clone()))?;
        let mut headers = Vec::new();
        file.for_each_node(|vid, row, deleted| headers.push((vid, row, deleted)))?;
        let mut state = graph.state.write();
        for (vid, row, deleted) in headers {
            let (level, list, _) = file.native_node_header(vid)?;
            let list = usize::try_from(list).map_err(|_| super::persistence::corrupt())?;
            if level != 0 || list >= state.lists.len() {
                return Err(super::persistence::corrupt());
            }
            let exception = graph.quantizer().is_some()
                && file.with_native_vector(vid, |v| Ok(v.payload.first() == Some(&1)))?;
            state.charge.try_grow_recoverable(128)?;
            let id = state.entries.len();
            state.push_entry(Entry::new(exception, row, None, vid, deleted));
            state.lists[list].push(id);
            state.list_pages[list].store(vid >> 32, Ordering::Relaxed);
            if !deleted {
                state.rows.insert(row, id);
                state.exceptions += usize::from(exception);
            }
        }
        if let Some(codec) = state.codec.clone() {
            let _scratch = ControlledMemoryCharge::try_new_recoverable(
                ControlledMemoryCategory::StorageScratch,
                state.entries.len().saturating_mul(16),
            )?;
            let mut by_vid: Vec<(u64, usize)> = state
                .entries
                .iter()
                .enumerate()
                .map(|(id, entry)| (entry.vid, id))
                .collect();
            by_vid.sort_unstable();
            // A crash between an entry's append and its code's leaves the
            // entry uncoded (scored exactly); an orphan code is ignored.
            file.for_each_ivf_code(codec.record_len(), |location, data_vid, _| {
                if let Ok(position) = by_vid.binary_search_by_key(&data_vid, |(vid, _)| *vid) {
                    state.set_code(by_vid[position].1, location);
                }
            })?;
        }
        drop(state);
        graph.cluster_entries_by_list();
        Ok(graph)
    }

    pub(crate) fn serialize_into(
        &self,
        out: impl std::io::Write,
        covered_lsn: u64,
    ) -> std::io::Result<()> {
        if self.file.is_some() {
            return Err(std::io::Error::other(
                "paged IVF does not use resident snapshots",
            ));
        }
        let definition = self.definition_bytes().map_err(std::io::Error::other)?;
        let state = self.state.read();
        let mut writer = super::persistence::Writer::new(out, 2)?;
        writer.bytes(&definition)?;
        writer.u64(covered_lsn)?;
        writer.count(state.entries.len())?;
        for entry in &state.entries {
            writer.row(entry.row)?;
            writer.u64(entry.vid)?;
            writer.u8(u8::from(entry.deleted))?;
            writer.vector(
                entry
                    .value()
                    .ok_or_else(|| std::io::Error::other("missing resident IVF payload"))?,
            )?;
        }
        writer.count(state.lists.len())?;
        for list in &state.lists {
            writer.count(list.len())?;
            for id in list {
                writer.count(*id)?;
            }
        }
        writer.finish()
    }

    pub(crate) fn deserialize(bytes: &[u8]) -> Result<Self, QueryError> {
        let mut reader = super::persistence::Reader::new(bytes, 2)?;
        let graph = Self::from_definition(reader.bytes()?, None)?;
        graph.persisted_lsn.store(reader.u64()?, Ordering::Release);
        let count = reader.count(26)?;
        let mut state = graph.state.write();
        for id in 0..count {
            let row = reader.row()?;
            let vid = reader.u64()?;
            let deleted = match reader.u8()? {
                0 => false,
                1 => true,
                _ => return Err(super::persistence::corrupt()),
            };
            let value = reader.vector()?;
            if graph.quantizer().is_none() {
                codec::validate_internal(&value)?;
            }
            let exception = graph.quantizer().is_some() && value.payload.first() == Some(&1);
            state.charge.try_grow_recoverable(128 + value.len())?;
            state.push_entry(Entry::new(exception, row, Some(value), vid, deleted));
            if !deleted {
                state.rows.insert(row, id);
                state.exceptions += usize::from(exception);
            }
        }
        let lists = reader.count(4)?;
        if lists != state.lists.len() {
            return Err(super::persistence::corrupt());
        }
        let mut seen = vec![false; count];
        for list in 0..lists {
            let length = reader.count(4)?;
            for _ in 0..length {
                let id = reader.u32()? as usize;
                if id >= count || std::mem::replace(&mut seen[id], true) {
                    return Err(super::persistence::corrupt());
                }
                state.lists[list].push(id);
            }
        }
        if seen.iter().any(|seen| !seen) {
            return Err(super::persistence::corrupt());
        }
        reader.finish()?;
        if let Some(codec) = state.codec.clone() {
            let state = &mut *state;
            for entry in &mut state.entries {
                if let Some(resident) = entry.resident.as_mut() {
                    if let Some(code) = Self::encode_code(&codec, &resident.value) {
                        state.charge.try_grow_recoverable(code.len())?;
                        resident.code = Some(code);
                    }
                }
            }
        }
        drop(state);
        Ok(graph)
    }

    pub(crate) fn file(&self) -> Option<&Arc<VectorIndexFile>> {
        self.file.as_ref()
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
                .map_or(0, |q| q.record_len() * state.entries.len()),
            state.exceptions,
        )
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
        if self.retrain.load(Ordering::Acquire) {
            return true;
        }
        let state = self.state.read();
        state.centers.is_empty()
            && self.params.nlist != 1
            && state.rows.len() >= self.params.nlist.max(16)
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
    /// Whether rows keep scan codes: unquantized numeric rows whose exact
    /// payload is wide enough to gain from them.
    fn codes_eligible(&self) -> bool {
        self.quantizer.is_none()
            && !self.quantization_requested
            && scan_codes::eligible(self.datatype, self.dimension, self.metric)
    }

    /// The scan-code record of an exact row, `None` outside the code domain.
    fn encode_code(codec: &ScanCodec, value: &VectorValue) -> Option<Box<[u8]>> {
        let dense = codec::to_f32(value);
        let mut record = vec![0u8; codec.record_len()].into_boxed_slice();
        codec.encode(&dense, &mut record).then_some(record)
    }

    fn value(&self, state: &State, id: usize) -> Result<VectorValue, QueryError> {
        let e = &state.entries[id];
        if let Some(q) = self.quantizer() {
            if let Some(v) = e.value() {
                return q.value_payload(&v.payload, self.file.as_ref());
            }
            return self
                .file
                .as_ref()
                .unwrap()
                .with_native_vector(e.vid, |v| q.value_payload(v.payload, self.file.as_ref()));
        }
        if let Some(v) = e.value() {
            return Ok(v.clone());
        }
        self.file.as_ref().unwrap().with_native_vector(e.vid, |v| {
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
    fn score(
        &self,
        state: &State,
        id: usize,
        query: &VectorValue,
        prepared: Option<&super::quantization::QuantQuery>,
    ) -> Result<f64, QueryError> {
        let e = &state.entries[id];
        if let Some(q) = self.quantizer() {
            if let Some(v) = e.value() {
                return q.score_payload(&v.payload, self.file.as_ref(), query, prepared);
            }
            return self.file.as_ref().unwrap().with_native_vector(e.vid, |v| {
                q.score_payload(v.payload, self.file.as_ref(), query, prepared)
            });
        }
        if let Some(v) = e.value() {
            return typed_ordering_distance(query, v, self.metric);
        }
        if self.quantization_requested {
            return typed_ordering_distance(query, &self.value(state, id)?, self.metric);
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

    /// Inverted list of `value`: the blocked centroid ranking when the value
    /// is in its domain, else the exact typed scan over every centroid.
    fn assign_list(&self, state: &State, value: &VectorValue) -> Result<usize, QueryError> {
        if state.centers.is_empty() {
            return Ok(0);
        }
        if let Some(prepared) = &state.prepared {
            let list = if value.datatype == VectorDataType::Float32 {
                prepared
                    .row_bytes(&value.payload)
                    .and_then(|row| prepared.nearest(row))
            } else {
                let dense = codec::to_f32(value);
                prepared
                    .row_f32(&dense)
                    .and_then(|row| prepared.nearest(row))
            };
            if let Some(list) = list {
                return Ok(list);
            }
        }
        Self::nearest(&state.centers, value, self.metric)
    }

    /// Lists of many values: blocked ranking over `threads` workers, with the
    /// exact typed scan for values outside the kernel's domain. Deterministic.
    fn assign_lists(
        &self,
        centers: &[VectorValue],
        prepared: Option<&PreparedCentroids>,
        values: &[&VectorValue],
        threads: usize,
    ) -> Result<Vec<usize>, QueryError> {
        if centers.is_empty() {
            return Ok(vec![0; values.len()]);
        }
        let mut lists = vec![EXACT; values.len()];
        if let Some(prepared) = prepared {
            let decoded: Vec<Option<Vec<f32>>> = values
                .iter()
                .map(|v| (v.datatype != VectorDataType::Float32).then(|| codec::to_f32(*v)))
                .collect();
            let mut slots = Vec::with_capacity(values.len());
            let mut rows = Vec::with_capacity(values.len());
            for (i, (value, dense)) in values.iter().zip(&decoded).enumerate() {
                let row = match dense {
                    Some(dense) => prepared.row_f32(dense),
                    None => prepared.row_bytes(&value.payload),
                };
                if let Some(row) = row {
                    slots.push(i);
                    rows.push(row);
                }
            }
            let mut assigned = vec![EXACT; rows.len()];
            prepared.assign_parallel(&rows, &mut assigned, threads);
            for (slot, list) in slots.into_iter().zip(assigned) {
                lists[slot] = list;
            }
        }
        values
            .iter()
            .zip(lists)
            .map(|(value, list)| {
                if list == EXACT {
                    Self::nearest(centers, value, self.metric)
                } else {
                    Ok(list as usize)
                }
            })
            .collect()
    }

    fn check_insertable(&self, value: &VectorValue) -> Result<(), QueryError> {
        codec::validate_internal(value)?;
        if self.metric == Metric::Cosine && super::distance::typed_is_zero(value) {
            return Err(codec::vector_err("COSINE indexes reject zero vectors"));
        }
        if value.datatype != self.datatype || value.dimension != self.dimension {
            return Err(codec::vector_err("IVF datatype/dimension mismatch"));
        }
        Ok(())
    }

    pub(crate) fn insert(&self, row: RowId, value: VectorValue) -> Result<(), QueryError> {
        self.check_insertable(&value)?;
        let mut state = self.state.write();
        state.cursor_epoch = state.cursor_epoch.wrapping_add(1);
        let list = self.assign_list(&state, &value)?;
        self.insert_locked(&mut state, row, value, list)
    }

    /// Bulk insert used by index builds. Validation and list assignment run
    /// in parallel; appends then happen in batch order under one write lock,
    /// so the resulting lists and vids equal a serial `insert` loop. On an
    /// invalid value the rows before it are inserted and its error returned,
    /// as the serial loop would.
    pub(crate) fn insert_batch(&self, batch: Vec<(RowId, VectorValue)>) -> Result<(), QueryError> {
        if batch.is_empty() {
            return Ok(());
        }
        let checked = self.check_batch(&batch);
        let valid = checked
            .as_ref()
            .map_or_else(|(index, _)| *index, |_| batch.len());
        let values: Vec<&VectorValue> = batch[..valid].iter().map(|(_, v)| v).collect();
        let (lists, epoch, codec) = {
            let state = self.state.read();
            let threads = assignment_threads(values.len(), state.centers.len(), self.dim());
            (
                self.assign_lists(&state.centers, state.prepared.as_ref(), &values, threads)?,
                state.centers_epoch,
                state.codec.clone(),
            )
        };
        let mut encoded = self.encode_batch(&values);
        let _codes_scratch = codec
            .as_deref()
            .map(|codec| {
                ControlledMemoryCharge::try_new_recoverable(
                    ControlledMemoryCategory::StorageScratch,
                    values.len().saturating_mul(codec.record_len() + 32),
                )
            })
            .transpose()?;
        let mut codes = codec
            .as_deref()
            .map(|codec| self.code_batch(codec, &values))
            .unwrap_or_default();
        drop(values);
        let mut state = self.state.write();
        state.cursor_epoch = state.cursor_epoch.wrapping_add(1);
        let stale = state.centers_epoch != epoch;
        let same_codec = match (&codec, &state.codec) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            _ => false,
        };
        if !same_codec {
            codes.clear();
        }
        for (i, ((row, value), list)) in batch.into_iter().take(valid).zip(lists).enumerate() {
            // Centers retrained between the two lock scopes: reassign.
            let list = if stale {
                self.assign_list(&state, &value)?
            } else {
                list
            };
            let stored = encoded.get_mut(i).and_then(Option::take);
            let code = codes.get_mut(i).map(Option::take);
            self.insert_encoded_locked(&mut state, row, value, stored, code, list)?;
        }
        checked.map_err(|(_, err)| err)
    }

    /// Validate a batch in parallel; `Err` names the first invalid row.
    fn check_batch(&self, batch: &[(RowId, VectorValue)]) -> Result<(), (usize, QueryError)> {
        let threads = assignment_threads(batch.len(), 1 << 10, self.dim()).min(16);
        let chunk = batch.len().div_ceil(threads.max(1)).max(1);
        let failures: Vec<Option<(usize, QueryError)>> =
            crate::util::thread_spawn::scope(|scope| {
                let handles: Vec<_> = batch
                    .chunks(chunk)
                    .enumerate()
                    .map(|(part, rows)| {
                        scope.spawn(move || {
                            rows.iter().enumerate().find_map(|(i, (_, value))| {
                                self.check_insertable(value)
                                    .err()
                                    .map(|err| (part * chunk + i, err))
                            })
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| {
                        h.join().unwrap_or_else(|_| {
                            Some((0, codec::vector_err("IVF bulk validation worker panicked")))
                        })
                    })
                    .collect()
            });
        match failures
            .into_iter()
            .flatten()
            .min_by_key(|(index, _)| *index)
        {
            Some(failure) => Err(failure),
            None => Ok(()),
        }
    }

    /// Quantized records of a batch, encoded in parallel without touching the
    /// index file. Entries that are `None` (no trained model, an encoding
    /// error, or a paged exception record whose raw blob must be appended to
    /// the file) are encoded in order by the serial append, which also
    /// reports any error at its row.
    fn encode_batch(&self, values: &[&VectorValue]) -> Vec<Option<VectorValue>> {
        let Some(quantizer) = self.quantizer() else {
            return Vec::new();
        };
        let paged = self.file.is_some();
        let threads = assignment_threads(values.len(), 1 << 10, self.dim()).min(16);
        let chunk = values.len().div_ceil(threads.max(1)).max(1);
        crate::util::thread_spawn::scope(|scope| {
            let handles: Vec<_> = values
                .chunks(chunk)
                .map(|values| {
                    let len = values.len();
                    let handle = scope.spawn(move || {
                        values
                            .iter()
                            .map(|value| {
                                quantizer
                                    .store(value, None)
                                    .ok()
                                    .filter(|stored| !(paged && stored.payload.first() == Some(&1)))
                            })
                            .collect::<Vec<_>>()
                    });
                    (len, handle)
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|(len, h)| h.join().unwrap_or_else(|_| vec![None; len]))
                .collect()
        })
    }

    /// Scan-code records of a batch of exact rows, encoded in parallel.
    fn code_batch(&self, codec: &ScanCodec, values: &[&VectorValue]) -> Vec<Option<Box<[u8]>>> {
        let threads = assignment_threads(values.len(), 1 << 10, self.dim()).min(16);
        let chunk = values.len().div_ceil(threads.max(1)).max(1);
        crate::util::thread_spawn::scope(|scope| {
            let handles: Vec<_> = values
                .chunks(chunk)
                .map(|values| {
                    let len = values.len();
                    let handle = scope.spawn(move || {
                        values
                            .iter()
                            .map(|value| Self::encode_code(codec, value))
                            .collect::<Vec<_>>()
                    });
                    (len, handle)
                })
                .collect();
            // Rows of a failed worker keep no code and are scored exactly.
            handles
                .into_iter()
                .flat_map(|(len, h)| h.join().unwrap_or_else(|_| vec![None; len]))
                .collect()
        })
    }

    fn insert_locked(
        &self,
        state: &mut State,
        row: RowId,
        value: VectorValue,
        list: usize,
    ) -> Result<(), QueryError> {
        self.insert_encoded_locked(state, row, value, None, None, list)
    }

    fn insert_encoded_locked(
        &self,
        state: &mut State,
        row: RowId,
        value: VectorValue,
        stored: Option<VectorValue>,
        code: Option<Option<Box<[u8]>>>,
        list: usize,
    ) -> Result<(), QueryError> {
        if let Some(&old) = state.rows.get(&row) {
            self.delete_locked(state, old)?;
        }
        // Scan codes exist only for exact payloads (no quantizer), so the
        // row's value is the stored value.
        let code = match &state.codec {
            Some(codec) => code.unwrap_or_else(|| Self::encode_code(codec, &value)),
            None => None,
        };
        let value = if let Some(quantizer) = self.quantizer() {
            match stored {
                Some(stored) => stored,
                None => quantizer.store(&value, self.file.as_ref())?,
            }
        } else if self.quantization_requested {
            super::quantization::store_untrained(&value, self.file.as_ref())?
        } else {
            value
        };
        state
            .charge
            .try_grow_recoverable(128 + if self.file.is_none() { value.len() } else { 0 })?;
        let id = state.entries.len();
        let vid = if let Some(f) = &self.file {
            f.append_native_ivf_entry(row, &value.payload, list as u64, &state.list_pages[list])?
        } else {
            id as u64
        };
        #[cfg(test)]
        if let Some(file) = &self.file {
            file.crash_point_for_test(5)?;
        }
        let exception = self.quantizer().is_some() && value.payload.first() == Some(&1);
        let value = if self.file.is_none() {
            Some(value)
        } else {
            None
        };
        let mut entry = Entry::new(exception, row, value, vid, false);
        if let Some(code) = code {
            match &self.file {
                Some(file) => {
                    entry.code =
                        file.append_ivf_code(list as u64, vid, &code, &state.code_pages[list])?;
                }
                None => {
                    state.charge.try_grow_recoverable(code.len())?;
                    if let Some(resident) = entry.resident.as_mut() {
                        resident.code = Some(code);
                    }
                }
            }
        }
        if exception {
            state.exceptions += 1;
            if state.entries.len() >= self.quantizer().unwrap().trained_rows() {
                state.incremental_exceptions += 1;
            }
        }
        state.push_entry(entry);
        state.rows.insert(row, id);
        state.lists[list].push(id);
        if let Some(q) = self.quantizer() {
            let exceptions = state.incremental_exceptions;
            let limit = 64usize.max(state.rows.len().div_ceil(20));
            if (matches!(q.name(), "sq8" | "sq4") && exceptions >= limit)
                || (matches!(q.name(), "pq" | "bq")
                    && state.entries.len() >= q.trained_rows() + 256usize.max(q.trained_rows() / 4))
            {
                self.retrain.store(true, Ordering::Release);
            }
        }
        Ok(())
    }
    fn delete_locked(&self, state: &mut State, id: usize) -> Result<(), QueryError> {
        if let Some(f) = &self.file {
            f.set_tombstone(0, state.entries[id].vid, true)?;
        }
        if state.entries[id].exception {
            state.exceptions = state.exceptions.saturating_sub(1);
        }
        state.mark_deleted(id);
        state.rows.remove(&state.entries[id].row);
        Ok(())
    }
    pub(crate) fn delete(&self, row: RowId) -> Result<u32, QueryError> {
        let mut state = self.state.write();
        state.cursor_epoch = state.cursor_epoch.wrapping_add(1);
        if let Some(&id) = state.rows.get(&row) {
            self.delete_locked(&mut state, id)?;
            return Ok(1);
        }
        Ok(0)
    }
    pub(crate) fn has_row(&self, row: RowId) -> bool {
        self.state.read().rows.contains_key(&row)
    }
    /// Relabel entries so every inverted list occupies one contiguous id
    /// range (list order, then append order). Builds append rows in heap scan
    /// order, so without this a probe's membership, tombstone and norm checks
    /// touch a random `entries` slot per candidate. Ids are internal: vids and
    /// row mappings are rewritten together, under the write lock.
    pub(crate) fn cluster_entries_by_list(&self) {
        let mut state = self.state.write();
        let n = state.entries.len();
        if state.lists.iter().map(Vec::len).sum::<usize>() != n {
            return;
        }
        // destination[old id] = new id
        let mut destination = vec![usize::MAX; n];
        let mut next = 0usize;
        for list in &state.lists {
            for &id in list {
                if destination[id] != usize::MAX {
                    return; // an id listed twice: leave the layout unchanged
                }
                destination[id] = next;
                next += 1;
            }
        }
        state.cursor_epoch = state.cursor_epoch.wrapping_add(1);
        let state = &mut *state;
        for id in state.rows.values_mut() {
            *id = destination[*id];
        }
        let mut start = 0usize;
        for list in &mut state.lists {
            let len = list.len();
            list.clear();
            list.extend(start..start + len);
            start += len;
        }
        // Apply the permutation in place: each swap settles one entry.
        for i in 0..n {
            while destination[i] != i {
                let target = destination[i];
                state.entries.swap(i, target);
                state.scan_words.swap(i, target);
                destination.swap(i, target);
            }
        }
    }

    /// Train the coarse quantizer from a bounded training sample (the build
    /// path). Equivalent to inserting the sample into a scratch index and
    /// training it — the whole sample trains `nlist` (capped by its distinct
    /// values) centers — without the scratch index's copies or its discarded
    /// list assignment.
    pub(crate) fn initialize_from_sample(
        &self,
        rows: Vec<(RowId, VectorValue)>,
        nlist: usize,
    ) -> Result<(), QueryError> {
        if rows.is_empty() {
            return Ok(());
        }
        if let Err((_, err)) = self.check_batch(&rows) {
            return Err(err);
        }
        let mut seen = HashSet::with_capacity(rows.len());
        let sample: Vec<VectorValue> = rows
            .into_iter()
            .filter(|(row, _)| seen.insert(*row))
            .map(|(_, value)| value)
            .collect();
        drop(seen);
        let requested = nlist.max(1).min(sample.len());
        let nlist = distinct_payloads_up_to(&sample, requested).max(1);
        let centers = {
            let _scratch = ControlledMemoryCharge::try_new_recoverable(
                ControlledMemoryCategory::StorageScratch,
                (self.dimension as usize).saturating_mul(16),
            )?;
            self.train_centers(&sample, nlist)?
        };
        let codec = if self.codes_eligible() {
            ScanCodec::train(
                self.dimension as usize,
                self.metric,
                sample.iter().map(codec::to_f32),
            )
        } else {
            None
        };
        drop(sample);
        let mut state = self.state.write();
        state.cursor_epoch = state.cursor_epoch.wrapping_add(1);
        state
            .charge
            .try_grow_recoverable(centers.iter().map(|v| v.len() + 96).sum())?;
        if let Some(codec) = &codec {
            state.charge.try_grow_recoverable(codec.allocated_bytes())?;
        }
        state.lists = (0..centers.len()).map(|_| Vec::new()).collect();
        state.list_pages = (0..centers.len()).map(|_| AtomicU64::new(0)).collect();
        state.code_pages = (0..centers.len()).map(|_| AtomicU64::new(0)).collect();
        state.set_centers(centers, self.datatype, self.metric)?;
        // Existing rows (none at build time) keep no code; they are scored
        // exactly. New rows are coded from here on.
        state.codec = codec.map(Arc::new);
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
            let _scratch = ControlledMemoryCharge::try_new_recoverable(
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
    /// Retrain the coarse quantizer over the live rows and reassign every
    /// row (builds train from a bounded sample via `initialize_from_sample`).
    #[cfg(test)]
    pub(crate) fn train(&self) -> Result<(), QueryError> {
        let mut state = self.state.write();
        state.cursor_epoch = state.cursor_epoch.wrapping_add(1);
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
        let _scratch = ControlledMemoryCharge::try_new_recoverable(
            ControlledMemoryCategory::StorageScratch,
            count
                .saturating_mul(self.datatype.payload_len(self.dimension).saturating_mul(2) + 64)
                .saturating_add(self.dimension as usize * 16),
        )?;
        let sample = training_sample(live.len(), count)
            .into_iter()
            .map(|i| self.value(&state, live[i]))
            .collect::<Result<Vec<_>, _>>()?;
        let nlist = distinct_payloads_up_to(&sample, requested).max(1);
        let centers = self.train_centers(&sample, nlist)?;
        drop(sample);
        state
            .charge
            .try_grow_recoverable(centers.iter().map(|v| v.len() + 64).sum())?;
        let prepared = prepare_centers(&centers, self.datatype, self.metric)?;
        let nlist = centers.len();
        let mut lists = vec![Vec::new(); nlist];
        // Decode bounded chunks and rank them against the new centers in parallel.
        let chunk = BULK_INSERT_BATCH;
        let _chunk_scratch = ControlledMemoryCharge::try_new_recoverable(
            ControlledMemoryCategory::StorageScratch,
            chunk.min(live.len()).saturating_mul(
                self.datatype
                    .payload_len(self.dimension)
                    .saturating_mul(2)
                    .saturating_add(64),
            ),
        )?;
        for ids in live.chunks(chunk) {
            crate::engine::cancel::check_cancelled()?;
            let values = ids
                .iter()
                .map(|&id| self.value(&state, id))
                .collect::<Result<Vec<_>, _>>()?;
            let refs: Vec<&VectorValue> = values.iter().collect();
            let threads = assignment_threads(refs.len(), nlist, self.dim());
            let assigned =
                self.assign_lists(&centers, prepared.as_ref().map(|(p, _)| p), &refs, threads)?;
            for (&id, list) in ids.iter().zip(assigned) {
                lists[list].push(id);
            }
        }
        state.install_centers(centers, prepared);
        state.list_pages = (0..lists.len()).map(|_| AtomicU64::new(0)).collect();
        state.code_pages = (0..lists.len()).map(|_| AtomicU64::new(0)).collect();
        state.lists = lists;

        Ok(())
    }
    pub(crate) fn advance_cursor(
        &self,
        cursor: &mut super::cursor::VectorSearchCursor,
        effort: usize,
    ) -> Result<bool, QueryError> {
        let state = self.state.read();
        if cursor.initialized && cursor.epoch != state.cursor_epoch {
            return Ok(false);
        }
        if !cursor.initialized {
            cursor.epoch = state.cursor_epoch;
            cursor.prepared = self
                .quantizer()
                .map(|q| q.prepare(&cursor.query))
                .transpose()?;
            if let Some(prepared) = &cursor.prepared {
                cursor
                    .hnsw
                    .charge
                    .try_grow_recoverable(prepared.allocated_bytes())?;
            }
            cursor
                .hnsw
                .charge
                .try_grow_recoverable(state.centers.len().saturating_mul(48))?;
            let mut probes = state
                .centers
                .iter()
                .enumerate()
                .map(|(id, value)| {
                    Ok(Scored {
                        id,
                        dist: typed_ordering_distance(&cursor.query, value, self.metric)?,
                    })
                })
                .collect::<Result<Vec<_>, QueryError>>()?;
            probes.sort_unstable();
            cursor.probes = probes.into_iter().map(|p| p.id).collect();
            if cursor.probes.is_empty() {
                cursor.probes.push(0);
            }
            cursor.work.distances += state.centers.len() as u64;
            cursor.initialized = true;
        }
        let mut scanned = 0;
        while cursor.probe_position < effort.max(1).min(cursor.probes.len()) {
            crate::engine::cancel::check_cancelled()?;
            let list = &state.lists[cursor.probes[cursor.probe_position]];
            while cursor.probe_offset < list.len() {
                if scanned == 1024 {
                    cursor.resume_work = true;
                    return Ok(true);
                }
                let id = list[cursor.probe_offset];
                cursor.probe_offset += 1;
                scanned += 1;
                let entry = &state.entries[id];
                cursor.work.visited += 1;
                if entry.deleted
                    || cursor
                        .selection
                        .as_deref()
                        .is_some_and(|s| !s.contains(cursor.sid, entry.row))
                {
                    continue;
                }
                let distance = self.score(&state, id, &cursor.query, cursor.prepared.as_ref())?;
                cursor.work.distances += 1;
                cursor.hnsw.push_pending(entry.row, distance)?;
            }
            cursor.probe_position += 1;
            cursor.probe_offset = 0;
        }
        Ok(true)
    }
    /// Every list ordered by its centroid's distance to `query` (nearest
    /// first, ties by list id). Uses the blocked keys when the query is in
    /// their domain and the exact typed distance otherwise.
    fn rank_probes(
        &self,
        state: &State,
        query: &VectorValue,
    ) -> Result<Vec<Scored<usize>>, QueryError> {
        let mut probes = Vec::new();
        if let Some(prepared) = state.prepared.as_ref().filter(|_| {
            query.dimension == self.dimension && query.datatype != VectorDataType::Binary
        }) {
            let dense = (query.datatype != VectorDataType::Float32).then(|| codec::to_f32(query));
            let row = match &dense {
                Some(dense) => prepared.row_f32(dense),
                None => prepared.row_bytes(&query.payload),
            };
            let mut keys = Vec::new();
            if row.is_some_and(|row| prepared.keys(row, &mut keys)) {
                probes = keys
                    .into_iter()
                    .enumerate()
                    .map(|(id, key)| Scored {
                        dist: key as f64,
                        id,
                    })
                    .collect();
            }
        }
        if probes.is_empty() {
            probes = state
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
        }
        probes.sort_unstable();
        if probes.is_empty() {
            probes.push(Scored { dist: 0.0, id: 0 });
        }
        Ok(probes)
    }

    /// Score the live entries of `probes`' lists, borrowing each stored
    /// payload through `payload` (a resident value or the mapped file).
    /// FLOAT32 rows are scored eight at a time with the query's prepared
    /// reduction and cached row norms; quantized rows four at a time.
    fn scan_lists<'p>(
        &self,
        state: &'p State,
        probes: &[Scored<usize>],
        query: &VectorValue,
        prepared: Option<&super::quantization::QuantQuery>,
        keep: &mut impl FnMut(usize, f64),
        payload: impl Fn(&'p Entry) -> Result<&'p [u8], QueryError>,
    ) -> Result<(), QueryError> {
        let dense = (self.quantizer().is_none()
            && !self.quantization_requested
            && self.datatype == VectorDataType::Float32
            && query.datatype == VectorDataType::Float32
            && query.dimension == self.dimension)
            .then(|| codec::to_f32(query));
        let exact = dense
            .as_ref()
            .map(|q| super::distance::PreparedF32Distance::new(q, self.metric));
        let width = if exact.is_some() { 8 } else { 4 };
        let mut ids = [0usize; 8];
        let mut rows: [&[u8]; 8] = [&[]; 8];
        // Each live payload is located once, `2 · width` rows before it is
        // scored, and hinted into cache at that point (compact rows entirely,
        // wide rows by their head for the hardware streamer to follow).
        const RING: usize = 16;
        let lookahead = 2 * width;
        let mut ring: [(usize, &[u8]); RING] = [(0, &[]); RING];
        for probe in probes {
            let list = &state.lists[probe.id];
            let (mut cursor, mut queued, mut taken, mut filled) = (0usize, 0usize, 0usize, 0usize);
            loop {
                while queued - taken < lookahead && cursor < list.len() {
                    let id = list[cursor];
                    cursor += 1;
                    let entry = &state.entries[id];
                    if entry.deleted {
                        continue;
                    }
                    let bytes = payload(entry)?;
                    if bytes.len() <= 1024 {
                        prefetch_whole(bytes);
                    } else {
                        prefetch_payload(bytes);
                    }
                    ring[queued % RING] = (id, bytes);
                    queued += 1;
                }
                if taken == queued {
                    break;
                }
                (ids[filled], rows[filled]) = ring[taken % RING];
                taken += 1;
                filled += 1;
                if filled == width {
                    self.score_rows(
                        state,
                        &ids,
                        &rows,
                        width,
                        exact.as_ref(),
                        query,
                        prepared,
                        keep,
                    )?;
                    filled = 0;
                }
            }
            if filled > 0 {
                // Pad with the first row; lanes reduce independently.
                for j in filled..width {
                    ids[j] = ids[0];
                    rows[j] = rows[0];
                }
                self.score_rows(
                    state,
                    &ids,
                    &rows,
                    filled,
                    exact.as_ref(),
                    query,
                    prepared,
                    keep,
                )?;
            }
            crate::engine::cancel::check_cancelled()?;
        }
        Ok(())
    }

    /// SQ8 list scan: each record is located once, hinted whole a few rows
    /// before it is scored (and by its first line a code page ahead, so the
    /// hardware streamer has ramped up when the scan reaches that page), then
    /// scored on its own by the code kernel.
    #[allow(clippy::too_many_arguments)]
    fn scan_sq8<'p>(
        &self,
        state: &'p State,
        probes: &[Scored<usize>],
        query: &VectorValue,
        prepared: &super::quantization::QuantQuery,
        best: &mut TopK,
        payload: impl Fn(&'p Entry) -> Result<&'p [u8], QueryError>,
        prefetch_far: impl Fn(&'p Entry),
    ) -> Result<(), QueryError> {
        const LOOKAHEAD: usize = 4;
        const PAGE_AHEAD: usize = 24;
        const RING: usize = 8;
        let quantizer = self.quantizer().ok_or_else(super::persistence::corrupt)?;
        let mut ring: [(usize, &[u8]); RING] = [(0, &[]); RING];
        for probe in probes {
            let list = &state.lists[probe.id];
            let (mut cursor, mut queued, mut taken) = (0usize, 0usize, 0usize);
            loop {
                while queued - taken < LOOKAHEAD && cursor < list.len() {
                    if let Some(&far) = list.get(cursor + PAGE_AHEAD) {
                        prefetch_far(&state.entries[far]);
                    }
                    let id = list[cursor];
                    cursor += 1;
                    let entry = &state.entries[id];
                    if entry.deleted {
                        continue;
                    }
                    let bytes = payload(entry)?;
                    prefetch_whole(bytes);
                    ring[queued % RING] = (id, bytes);
                    queued += 1;
                }
                if taken == queued {
                    break;
                }
                let (id, bytes) = ring[taken % RING];
                taken += 1;
                let distance = match quantizer.scan_score(bytes, prepared) {
                    Some(distance) => distance,
                    None => {
                        quantizer.score_payload(bytes, self.file.as_ref(), query, Some(prepared))?
                    }
                };
                best.push(id, distance);
            }
            crate::engine::cancel::check_cancelled()?;
        }
        Ok(())
    }

    /// Exact scan of the probed lists through scan codes, in two phases.
    ///
    /// Phase one bounds every live row from its code alone and keeps the rows
    /// whose lower bound does not exceed the `limit`-th smallest upper bound.
    /// Phase two scores those rows exactly in lower-bound order and stops at
    /// the first lower bound above the current `limit`-th exact distance.
    /// Every skipped row is strictly farther than `limit` scored rows, so the
    /// result equals the exact scan's, ties included. Rows without a code are
    /// always scored.
    #[allow(clippy::too_many_arguments)]
    fn scan_coded<'p>(
        &self,
        state: &'p State,
        probes: &[Scored<usize>],
        query: &VectorValue,
        scan: &ScanQuery,
        best: &mut TopK,
        code: impl Fn(usize) -> Coded<'p>,
        prefetch: impl Fn(usize, bool),
        payload: impl Fn(&'p Entry) -> Result<&'p [u8], QueryError>,
    ) -> Result<(), QueryError> {
        // Every line of a code record is hinted four rows ahead, and the first
        // line of the record a code page ahead so the hardware streamer has
        // already ramped up when the scan crosses into that page. Measured
        // best on Ice Lake-SP against 6–16 rows, half-line density and L2-only
        // hints.
        const AHEAD: usize = 4;
        const PAGE_AHEAD: usize = 24;
        let limit = best.limit;
        let rows: usize = probes.iter().map(|p| state.lists[p.id].len()).sum();
        let _scratch = ControlledMemoryCharge::try_new_recoverable(
            ControlledMemoryCategory::StorageScratch,
            rows.saturating_add(limit)
                .saturating_mul(std::mem::size_of::<Scored<usize>>()),
        )?;
        let mut uppers: BinaryHeap<Scored<usize>> = BinaryHeap::new();
        let mut threshold = f64::INFINITY;
        let mut survivors: Vec<Scored<usize>> = Vec::new();
        for probe in probes {
            let list = &state.lists[probe.id];
            for (position, &id) in list.iter().enumerate() {
                if let Some(&ahead) = list.get(position + AHEAD) {
                    prefetch(ahead, false);
                }
                if let Some(&ahead) = list.get(position + PAGE_AHEAD) {
                    prefetch(ahead, true);
                }
                let record = match code(id) {
                    Coded::Skip => continue,
                    Coded::Exact => {
                        survivors.push(Scored {
                            dist: f64::NEG_INFINITY,
                            id,
                        });
                        continue;
                    }
                    Coded::Code(record) => record?,
                };
                let (low, high) = scan.bounds(record);
                if high < threshold {
                    uppers.push(Scored { dist: high, id });
                    if uppers.len() > limit {
                        uppers.pop();
                    }
                    if uppers.len() == limit {
                        threshold = uppers.peek().map_or(f64::INFINITY, |s| s.dist);
                    }
                }
                if low <= threshold {
                    survivors.push(Scored { dist: low, id });
                }
            }
            crate::engine::cancel::check_cancelled()?;
        }
        drop(uppers);
        survivors.retain(|s| s.dist <= threshold);
        survivors.sort_unstable();
        let dense = (self.datatype == VectorDataType::Float32
            && query.datatype == VectorDataType::Float32)
            .then(|| codec::to_f32(query));
        let exact = dense
            .as_ref()
            .map(|q| super::distance::PreparedF32Distance::new(q, self.metric));
        let mut ids = [0usize; 8];
        let mut rows: [&[u8]; 8] = [&[]; 8];
        let mut chunks = survivors.chunks(8).peekable();
        while let Some(chunk) = chunks.next() {
            if best.bar().is_some_and(|bar| chunk[0].dist > bar) {
                break;
            }
            if let Some(next) = chunks.peek() {
                for s in *next {
                    if let Ok(bytes) = payload(&state.entries[s.id]) {
                        prefetch_whole(bytes);
                    }
                }
            }
            for (j, s) in chunk.iter().enumerate() {
                ids[j] = s.id;
                rows[j] = payload(&state.entries[s.id])?;
            }
            for j in chunk.len()..8 {
                ids[j] = ids[0];
                rows[j] = rows[0];
            }
            self.score_rows(
                state,
                &ids,
                &rows,
                chunk.len(),
                exact.as_ref(),
                query,
                None,
                &mut |id, dist| best.push(id, dist),
            )?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    fn score_rows(
        &self,
        state: &State,
        ids: &[usize; 8],
        rows: &[&[u8]; 8],
        live: usize,
        exact: Option<&super::distance::PreparedF32Distance<'_>>,
        query: &VectorValue,
        prepared: Option<&super::quantization::QuantQuery>,
        keep: &mut impl FnMut(usize, f64),
    ) -> Result<(), QueryError> {
        if let Some(exact) = exact {
            let cells = ids.map(|id| &state.entries[id].norm);
            let before = cells.map(|cell| cell.load(Ordering::Relaxed));
            let mut norms = before.map(f32::from_bits);
            let scores = exact.score_batch8(*rows, &mut norms);
            for j in 0..live {
                if before[j] == 0 && norms[j].is_normal() {
                    cells[j].store(norms[j].to_bits(), Ordering::Relaxed);
                }
                keep(ids[j], scores[j]);
            }
            return Ok(());
        }
        if let Some(q) = self.quantizer() {
            let scores = q.scan_payloads4(
                [rows[0], rows[1], rows[2], rows[3]],
                self.file.as_ref(),
                query,
                prepared,
            )?;
            for j in 0..live {
                keep(ids[j], scores[j]);
            }
            return Ok(());
        }
        for j in 0..live {
            let distance = if self.quantization_requested {
                typed_ordering_distance(query, &self.value(state, ids[j])?, self.metric)?
            } else {
                let row = crate::vector::VectorRef {
                    datatype: self.datatype,
                    dimension: self.dimension,
                    payload: rows[j],
                };
                typed_ordering_distance(query, &row, self.metric)?
            };
            keep(ids[j], distance);
        }
        Ok(())
    }

    pub(crate) fn search(
        &self,
        query: &VectorValue,
        request: VectorSearchRequest,
    ) -> Result<VectorSearchResult, QueryError> {
        if !self.is_valid() {
            return Err(crate::storage::error(
                crate::error::catalog::ER_GET_ERRNO,
                "native IVF requires quantizer training or rebuild",
            ));
        }
        let mapped = match &self.file {
            Some(file) => file.prepare_navigation_mapping()?,
            None => false,
        };
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
        let _scratch = ControlledMemoryCharge::try_new_recoverable(
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
        let mut probes = self.rank_probes(&state, query)?;
        probes.truncate(nprobe);
        let prepared = self.quantizer().map(|q| q.prepare(query)).transpose()?;
        let sq8 = prepared.as_ref().is_some_and(|p| p.has_scan());
        let mut best = TopK::new(candidate_limit)?;
        // Coded scans need the scan codes of the backend in hand: resident
        // records, or the mapped file. The buffer-pool fallback scores rows.
        let coded = state
            .codec
            .as_ref()
            .filter(|_| query.dimension == self.dimension && (self.file.is_none() || mapped))
            .and_then(|codec| ScanQuery::new(codec, &codec::to_f32(query)).map(|q| (codec, q)));
        let scanned = match (&self.file, coded) {
            (None, Some((_, scan))) => Some(self.scan_coded(
                &state,
                &probes,
                query,
                &scan,
                &mut best,
                |id| {
                    let entry = &state.entries[id];
                    match entry.code_bytes() {
                        _ if entry.deleted => Coded::Skip,
                        Some(code) => Coded::Code(Ok(code)),
                        None => Coded::Exact,
                    }
                },
                |_, _| {},
                |e| {
                    e.value()
                        .map(|v| v.payload.as_slice())
                        .ok_or_else(super::persistence::corrupt)
                },
            )),
            (Some(file), Some((codec, scan))) => {
                let code_len = codec.record_len();
                file.with_navigation_mapping(|view| {
                    self.scan_coded(
                        &state,
                        &probes,
                        query,
                        &scan,
                        &mut best,
                        |id| match state.scan_words[id] {
                            DELETED_WORD => Coded::Skip,
                            NO_CODE => Coded::Exact,
                            location => Coded::Code(view.ivf_code(location, code_len)),
                        },
                        |id, first_line| {
                            let location = state.scan_words[id];
                            if location < DELETED_WORD {
                                view.prefetch_ivf_code(
                                    location,
                                    if first_line { 1 } else { code_len },
                                );
                            }
                        },
                        |e| view.native_payload(e.vid),
                    )
                })
            }
            (None, None) if sq8 => Some(self.scan_sq8(
                &state,
                &probes,
                query,
                prepared.as_ref().unwrap(),
                &mut best,
                |e| {
                    e.value()
                        .map(|v| v.payload.as_slice())
                        .ok_or_else(super::persistence::corrupt)
                },
                |_| {},
            )),
            (Some(file), None) if mapped && sq8 => file.with_navigation_mapping(|view| {
                self.scan_sq8(
                    &state,
                    &probes,
                    query,
                    prepared.as_ref().unwrap(),
                    &mut best,
                    |e| view.native_payload(e.vid),
                    |e| view.prefetch_navigation_node(e.vid),
                )
            }),
            (None, None) => Some(self.scan_lists(
                &state,
                &probes,
                query,
                prepared.as_ref(),
                &mut |id, dist| best.push(id, dist),
                |e| {
                    e.value()
                        .map(|v| v.payload.as_slice())
                        .ok_or_else(super::persistence::corrupt)
                },
            )),
            (Some(file), None) if mapped => file.with_navigation_mapping(|view| {
                self.scan_lists(
                    &state,
                    &probes,
                    query,
                    prepared.as_ref(),
                    &mut |id, dist| best.push(id, dist),
                    |e| view.native_payload(e.vid),
                )
            }),
            (Some(_), None) => None,
        };
        match scanned {
            Some(result) => result?,
            // The mapping was revoked (or never admitted): score each entry
            // through the buffer pool.
            None => {
                for probe in &probes {
                    for &id in &state.lists[probe.id] {
                        if state.entries[id].deleted {
                            continue;
                        }
                        best.push(id, self.score(&state, id, query, prepared.as_ref())?);
                    }
                    crate::engine::cancel::check_cancelled()?;
                }
            }
        }
        let candidates = best
            .best
            .into_sorted_vec()
            .into_iter()
            .map(|s| (state.entries[s.id].row, s.dist))
            .collect();
        Ok(VectorSearchResult {
            candidates,
            exact_order: self.quantizer().is_none(),
            exhausted: nprobe == state.lists.len(),
        })
    }
}

/// The `limit` nearest `(distance, id)` pairs seen so far.
struct TopK {
    best: BinaryHeap<Scored<usize>>,
    limit: usize,
}

impl TopK {
    fn new(limit: usize) -> Result<Self, QueryError> {
        let mut best = BinaryHeap::new();
        best.try_reserve(limit.saturating_add(1)).map_err(|_| {
            crate::storage::error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "native IVF candidate allocation failed",
            )
        })?;
        Ok(Self { best, limit })
    }

    #[inline]
    fn push(&mut self, id: usize, dist: f64) {
        let score = Scored { dist, id };
        if self.best.len() < self.limit || self.best.peek().is_some_and(|worst| score < *worst) {
            self.best.push(score);
            if self.best.len() > self.limit {
                self.best.pop();
            }
        }
    }

    /// The distance a row must beat once `limit` rows are held.
    #[inline]
    fn bar(&self) -> Option<f64> {
        (self.best.len() >= self.limit)
            .then(|| self.best.peek().map(|worst| worst.dist))
            .flatten()
    }
}

/// `min(limit, number of distinct payloads)`, hashing only until `limit`
/// distinct values are seen (a training sample rarely repeats a vector).
fn distinct_payloads_up_to(values: &[VectorValue], limit: usize) -> usize {
    let mut seen: HashSet<&[u8]> = HashSet::with_capacity(limit.min(values.len()));
    for value in values {
        if seen.len() >= limit {
            break;
        }
        seen.insert(value.payload.as_slice());
    }
    seen.len().min(limit)
}

/// Hint every cache line of a stored row (randomly accessed rows).
#[inline]
fn prefetch_whole(bytes: &[u8]) {
    for offset in (0..bytes.len()).step_by(64) {
        super::distance::prefetch_read(bytes[offset..].as_ptr().cast());
    }
}

/// Hint the first cache lines of a stored row; the hardware streamer follows
/// the rest of the record within its page.
#[inline]
fn prefetch_payload(bytes: &[u8]) {
    for offset in (0..bytes.len().min(512)).step_by(64) {
        super::distance::prefetch_read(bytes[offset..].as_ptr().cast());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::vector::{codec, VectorSearchEffort};

    fn clustered(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut s = seed;
        let mut next = move || {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((s >> 33) as f32 / (1u64 << 31) as f32) - 1.0
        };
        let centers: Vec<Vec<f32>> = (0..12)
            .map(|_| (0..dim).map(|_| next() * 4.0).collect())
            .collect();
        (0..n)
            .map(|i| centers[i % 12].iter().map(|&x| x + next() * 0.5).collect())
            .collect()
    }

    fn value(v: &[f32], datatype: VectorDataType) -> VectorValue {
        let value = codec::from_f32(v);
        if datatype == VectorDataType::Float32 {
            return value;
        }
        codec::coerce_value(
            crate::types::Value::Vector(value),
            datatype,
            Some(v.len() as u32),
        )
        .unwrap()
    }

    fn index(datatype: VectorDataType, dim: usize, metric: Metric, nlist: usize) -> NativeIvf {
        NativeIvf::new(
            datatype,
            dim as u32,
            metric,
            IvfParams {
                nlist,
                kmeans_iters: 8,
                train_sample_mult: 64,
            },
            None,
        )
        .unwrap()
    }

    fn rows(data: &[Vec<f32>], datatype: VectorDataType) -> Vec<(RowId, VectorValue)> {
        data.iter()
            .enumerate()
            .map(|(i, v)| (RowId::new(i as u32 + 1, 0), value(v, datatype)))
            .collect()
    }

    fn search(ivf: &NativeIvf, query: &VectorValue, k: usize, nprobe: usize) -> Vec<(RowId, f64)> {
        ivf.search(
            query,
            VectorSearchRequest {
                candidate_limit: k,
                rerank_limit: k,
                effort: VectorSearchEffort::Ivf { nprobe },
            },
        )
        .unwrap()
        .candidates
    }

    /// Scan codes only decide which rows are scored exactly: a coded index
    /// returns bit-identical candidates to the same index scanned without
    /// codes, for every metric, datatype, probe count and limit, including
    /// deleted, re-inserted, uncoded and duplicate rows.
    #[test]
    fn coded_scan_equals_uncoded_exact_scan() {
        for (datatype, metric, dim) in [
            (VectorDataType::Float32, Metric::Cosine, 96),
            (VectorDataType::Float32, Metric::L2, 33),
            (VectorDataType::Float32, Metric::Dot, 48),
            (VectorDataType::Float16, Metric::Cosine, 64),
            (VectorDataType::BFloat16, Metric::L2, 40),
        ] {
            let mut data = clustered(4000, dim, 31 + dim as u64);
            data[17] = data[16].clone(); // exact duplicate: a distance tie
            let all = rows(&data, datatype);
            let coded = index(datatype, dim, metric, 24);
            let plain = index(datatype, dim, metric, 24);
            let sample: Vec<_> = all.iter().step_by(4).cloned().collect();
            coded.initialize_from_sample(sample.clone(), 24).unwrap();
            plain.initialize_from_sample(sample, 24).unwrap();
            assert!(
                coded.state.read().codec.is_some(),
                "{datatype:?} {metric:?}"
            );
            plain.state.write().codec = None;
            // Rows inserted while the codec is withheld stay uncoded.
            let codec = coded.state.write().codec.take();
            for ivf in [&coded, &plain] {
                ivf.insert_batch(all[..300].to_vec()).unwrap();
            }
            coded.state.write().codec = codec;
            for ivf in [&coded, &plain] {
                ivf.insert_batch(all[300..].to_vec()).unwrap();
                for (row, _) in all.iter().step_by(9) {
                    ivf.delete(*row).unwrap();
                }
                for (row, v) in all.iter().step_by(27).take(40) {
                    ivf.insert(*row, v.clone()).unwrap(); // re-insert a deleted row
                }
            }
            {
                let state = coded.state.read();
                let codes = state.entries.iter().filter(|e| e.has_code()).count();
                assert!(codes > 3500 && codes < state.entries.len(), "{codes}");
                assert_eq!(state.scan_words.len(), state.entries.len());
                for (entry, &word) in state.entries.iter().zip(&state.scan_words) {
                    let expected = if entry.deleted {
                        DELETED_WORD
                    } else {
                        entry.code
                    };
                    assert_eq!(word, expected);
                }
            }
            let queries: Vec<VectorValue> = clustered(6, dim, 4242)
                .into_iter()
                .chain([data[16].clone(), data[1].clone()])
                .map(|v| value(&v, datatype))
                .collect();
            for query in &queries {
                for (k, nprobe) in [(1, 1), (10, 3), (100, 8), (500, 24)] {
                    let want = search(&plain, query, k, nprobe);
                    let got = search(&coded, query, k, nprobe);
                    assert_eq!(got.len(), want.len(), "{datatype:?} {metric:?} k={k}");
                    for (g, w) in got.iter().zip(&want) {
                        assert_eq!(g.0, w.0, "{datatype:?} {metric:?} k={k} np={nprobe}");
                        assert_eq!(g.1.to_bits(), w.1.to_bits());
                    }
                }
            }
        }
    }

    /// The parallel bulk path must build exactly the index a serial insert
    /// loop builds, and full-probe search must equal brute force.
    #[test]
    fn bulk_insert_matches_serial_inserts_and_full_probe_is_exact() {
        for (datatype, metric, dim) in [
            (VectorDataType::Float32, Metric::Cosine, 96),
            (VectorDataType::Float32, Metric::L2, 33),
            (VectorDataType::Float32, Metric::Dot, 17),
            (VectorDataType::Float16, Metric::Cosine, 40),
            (VectorDataType::BFloat16, Metric::L2, 24),
        ] {
            let data = clustered(5000, dim, dim as u64);
            let all = rows(&data, datatype);
            let serial = index(datatype, dim, metric, 20);
            let bulk = index(datatype, dim, metric, 20);
            let sample: Vec<_> = all.iter().step_by(5).cloned().collect();
            serial.initialize_from_sample(sample.clone(), 20).unwrap();
            bulk.initialize_from_sample(sample, 20).unwrap();
            for (row, v) in all.iter().cloned() {
                serial.insert(row, v).unwrap();
            }
            for chunk in all.chunks(1700) {
                bulk.insert_batch(chunk.to_vec()).unwrap();
            }
            {
                let (a, b) = (serial.state.read(), bulk.state.read());
                assert_eq!(a.lists, b.lists, "{datatype:?} {metric:?}");
                assert_eq!(a.rows, b.rows);
                assert_eq!(a.lists.len(), 20);
                // DOT assigns by inner product over L2-trained centers, so
                // large-norm centers absorb most rows; only L2/COSINE spread.
                if metric != Metric::Dot {
                    assert!(a.lists.iter().filter(|l| !l.is_empty()).count() >= 12);
                }
            }
            let query = value(&clustered(1, dim, 777)[0], datatype);
            let mut brute: Vec<(RowId, f64)> = all
                .iter()
                .map(|(row, v)| (*row, typed_ordering_distance(&query, v, metric).unwrap()))
                .collect();
            brute.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            for ivf in [&serial, &bulk] {
                let got = search(ivf, &query, 25, 20);
                assert_eq!(got.len(), 25);
                for ((row, d), (want_row, want)) in got.iter().zip(&brute) {
                    assert!((d - want).abs() <= 1e-5 * want.abs().max(1.0), "{metric:?}");
                    if row != want_row {
                        // Only an exact-distance tie may swap rows.
                        let other = brute.iter().find(|(r, _)| r == row).unwrap().1;
                        assert!((other - want).abs() <= 1e-5 * want.abs().max(1.0));
                    }
                }
                // Partial probes return a sorted subset of rows.
                let partial = search(ivf, &query, 25, 3);
                assert!(partial.windows(2).all(|w| w[0].1 <= w[1].1));
            }
        }
    }

    #[test]
    fn clustering_entries_by_list_preserves_rows_lists_and_results() {
        let dim = 24;
        let data = clustered(4000, dim, 21);
        let all = rows(&data, VectorDataType::Float32);
        let ivf = index(VectorDataType::Float32, dim, Metric::Cosine, 16);
        ivf.initialize_from_sample(all.iter().step_by(3).cloned().collect(), 16)
            .unwrap();
        ivf.insert_batch(all.clone()).unwrap();
        // A replaced row leaves a deleted entry in its old list.
        ivf.insert(all[5].0, all[900].1.clone()).unwrap();
        ivf.delete(all[17].0).unwrap();
        let queries: Vec<_> = clustered(6, dim, 99)
            .iter()
            .map(|q| value(q, VectorDataType::Float32))
            .collect();
        let before: Vec<_> = queries.iter().map(|q| search(&ivf, q, 30, 5)).collect();
        let members = |ivf: &NativeIvf| -> Vec<Vec<(RowId, bool)>> {
            let state = ivf.state.read();
            state
                .lists
                .iter()
                .map(|l| {
                    l.iter()
                        .map(|&id| (state.entries[id].row, state.entries[id].deleted))
                        .collect()
                })
                .collect()
        };
        let lists_before = members(&ivf);
        ivf.cluster_entries_by_list();
        assert_eq!(members(&ivf), lists_before);
        {
            let state = ivf.state.read();
            let mut next = 0;
            for list in &state.lists {
                assert_eq!(*list, (next..next + list.len()).collect::<Vec<_>>());
                next += list.len();
            }
            for (row, &id) in &state.rows {
                assert_eq!(state.entries[id].row, *row);
                assert!(!state.entries[id].deleted);
            }
            assert_eq!(state.rows.len(), 3999);
            // Scan words follow their entries through the permutation.
            for (entry, &word) in state.entries.iter().zip(&state.scan_words) {
                assert_eq!(
                    word,
                    if entry.deleted {
                        DELETED_WORD
                    } else {
                        entry.code
                    }
                );
            }
        }
        let after: Vec<_> = queries.iter().map(|q| search(&ivf, q, 30, 5)).collect();
        assert_eq!(before, after);
    }

    #[test]
    fn bulk_insert_encodes_quantized_records_like_serial_inserts() {
        use crate::storage::vector_config::QuantizationConfig;
        let dim = 40;
        let data = clustered(3000, dim, 13);
        let all = rows(&data, VectorDataType::Float32);
        for config in [QuantizationConfig::Sq8, QuantizationConfig::Sq4] {
            let mut trainer = super::super::quantization::Trainer::new(
                config.clone(),
                VectorDataType::Float32,
                dim as u32,
                Metric::Cosine,
            )
            .unwrap();
            for (_, v) in &all[..2000] {
                trainer.observe(v).unwrap();
            }
            let model = trainer.finish().unwrap().unwrap();
            let build = || {
                let mut ivf = index(VectorDataType::Float32, dim, Metric::Cosine, 10);
                ivf.set_quantizer(true, Some(model.clone()), 1);
                ivf.initialize_from_sample(all[..900].to_vec(), 10).unwrap();
                ivf
            };
            let (serial, bulk) = (build(), build());
            for (row, v) in all.iter().cloned() {
                serial.insert(row, v).unwrap();
            }
            bulk.insert_batch(all.clone()).unwrap();
            let (a, b) = (serial.state.read(), bulk.state.read());
            assert_eq!(a.lists, b.lists);
            assert_eq!(a.exceptions, b.exceptions);
            for (x, y) in a.entries.iter().zip(&b.entries) {
                assert_eq!(x.value(), y.value(), "{config:?}");
                assert_eq!(x.exception, y.exception);
            }
        }
    }

    #[test]
    fn bulk_insert_reports_first_invalid_row_after_inserting_its_prefix() {
        let dim = 8;
        let data = clustered(300, dim, 5);
        let mut all = rows(&data, VectorDataType::Float32);
        let ivf = index(VectorDataType::Float32, dim, Metric::Cosine, 4);
        ivf.initialize_from_sample(all[..100].to_vec(), 4).unwrap();
        all[120].1 = codec::from_f32(&[0.0; 8]);
        all[200].1 = codec::from_f32(&[1.0; 7]);
        let err = ivf.insert_batch(all).unwrap_err();
        assert!(err.to_string().contains("zero vectors"), "{err}");
        assert_eq!(ivf.len(), 120);
    }

    #[test]
    fn retrain_assigns_every_live_row_to_a_nearest_list() {
        let dim = 48;
        let data = clustered(6000, dim, 9);
        let ivf = index(VectorDataType::Float32, dim, Metric::Cosine, 0);
        ivf.insert_batch(rows(&data, VectorDataType::Float32))
            .unwrap();
        ivf.delete(RowId::new(7, 0)).unwrap();
        ivf.train().unwrap();
        let state = ivf.state.read();
        assert_eq!(state.lists.len(), 77, "auto nlist = sqrt(live)");
        let listed: usize = state.lists.iter().map(Vec::len).sum();
        assert_eq!(listed, state.rows.len());
        for (list, ids) in state.lists.iter().enumerate() {
            for &id in ids.iter().step_by(37) {
                let v = state.entries[id].value().unwrap();
                let exact: Vec<f64> = state
                    .centers
                    .iter()
                    .map(|c| typed_ordering_distance(v, c, Metric::Cosine).unwrap())
                    .collect();
                let best = exact.iter().cloned().fold(f64::INFINITY, f64::min);
                assert!(exact[list] - best <= 1e-5, "row {id} in list {list}");
            }
        }
    }

    #[test]
    fn binary_hamming_lists_keep_exact_assignment() {
        let ivf = NativeIvf::new(
            VectorDataType::Binary,
            64,
            Metric::Hamming,
            IvfParams {
                nlist: 4,
                kmeans_iters: 4,
                train_sample_mult: 32,
            },
            None,
        )
        .unwrap();
        let all: Vec<_> = (0..400u64)
            .map(|i| {
                let bits = (i % 4).wrapping_mul(0x1111_1111_1111_1111) ^ (i * 0x9E37_79B9) & 0xF;
                (
                    RowId::new(i as u32 + 1, 0),
                    VectorValue::new(VectorDataType::Binary, 64, bits.to_le_bytes().to_vec()),
                )
            })
            .collect();
        ivf.initialize_from_sample(all.clone(), 4).unwrap();
        assert!(ivf.state.read().prepared.is_none());
        ivf.insert_batch(all.clone()).unwrap();
        let state = ivf.state.read();
        for (list, ids) in state.lists.iter().enumerate() {
            for &id in ids {
                let v = state.entries[id].value().unwrap();
                assert_eq!(
                    NativeIvf::nearest(&state.centers, v, Metric::Hamming).unwrap(),
                    list
                );
            }
        }
    }
}
