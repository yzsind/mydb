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
    exception: bool,
    row: RowId,
    value: Option<VectorValue>,
    vid: u64,
    deleted: bool,
}
#[derive(Debug)]
struct State {
    exceptions: usize,
    incremental_exceptions: usize,
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
                exceptions: 0,
                incremental_exceptions: 0,
                entries: Vec::new(),
                rows: HashMap::new(),
                centers: Vec::new(),
                lists: vec![Vec::new()],
                list_pages: vec![AtomicU64::new(0)],
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
        )
    }
    fn definition_bytes_for(
        &self,
        model: Option<&Arc<super::quantization::Quantizer>>,
        generation: u64,
        centers: &[VectorValue],
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
            state.centers.push(center);
        }
        let lists = count.max(1);
        state.lists = (0..lists).map(|_| Vec::new()).collect();
        state.list_pages = (0..lists).map(|_| AtomicU64::new(0)).collect();
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
            file.write_native_definition(&self.definition_bytes_for(
                Some(&model),
                generation,
                &state.centers,
            )?)?;
        } else {
            for (node, value) in state.entries.iter_mut().zip(converted) {
                node.exception = value.payload.first() == Some(&1);
                node.value = Some(value);
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
            state.entries.push(Entry {
                exception,
                row,
                value: None,
                vid,
                deleted,
            });
            state.lists[list].push(id);
            state.list_pages[list].store(vid >> 32, Ordering::Relaxed);
            if !deleted {
                state.rows.insert(row, id);
                state.exceptions += usize::from(exception);
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
                    .value
                    .as_ref()
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
            state.entries.push(Entry {
                exception,
                row,
                value: Some(value),
                vid,
                deleted,
            });
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
    fn value(&self, state: &State, id: usize) -> Result<VectorValue, QueryError> {
        let e = &state.entries[id];
        if let Some(q) = self.quantizer() {
            if let Some(v) = &e.value {
                return q.value_payload(&v.payload, self.file.as_ref());
            }
            return self
                .file
                .as_ref()
                .unwrap()
                .with_native_vector(e.vid, |v| q.value_payload(v.payload, self.file.as_ref()));
        }
        if let Some(v) = &e.value {
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
            if let Some(v) = &e.value {
                return q.score_payload(&v.payload, self.file.as_ref(), query, prepared);
            }
            return self.file.as_ref().unwrap().with_native_vector(e.vid, |v| {
                q.score_payload(v.payload, self.file.as_ref(), query, prepared)
            });
        }
        if let Some(v) = &e.value {
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
        let value = if let Some(quantizer) = self.quantizer() {
            quantizer.store(&value, self.file.as_ref())?
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
        if exception {
            state.exceptions += 1;
            if state.entries.len() >= self.quantizer().unwrap().trained_rows() {
                state.incremental_exceptions += 1;
            }
        }
        state.entries.push(Entry {
            exception,
            row,
            value,
            vid,
            deleted: false,
        });
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
            .try_grow_recoverable(centers.iter().map(|v| v.len() + 96).sum())?;
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
        let unique: HashSet<&[u8]> = sample.iter().map(|v| v.payload.as_slice()).collect();
        let nlist = requested.min(unique.len()).max(1);
        drop(unique);
        let centers = self.train_centers(&sample, nlist)?;
        state
            .charge
            .try_grow_recoverable(centers.iter().map(|v| v.len() + 64).sum())?;
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
            return Err(crate::storage::error(
                crate::error::catalog::ER_GET_ERRNO,
                "native IVF requires quantizer training or rebuild",
            ));
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
        let prepared = self.quantizer().map(|q| q.prepare(query)).transpose()?;
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
                let distance = self.score(&state, id, query, prepared.as_ref())?;
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
            exact_order: self.quantizer().is_none(),
            exhausted: nprobe == state.lists.len(),
        })
    }
}
