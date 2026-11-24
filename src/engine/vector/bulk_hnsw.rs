//! Capacity-bounded HNSW construction, independent of persistent payloads.
//! Shared construction codes and dense adjacency avoid random edits to final
//! vector pages. Original/declared payloads are streamed only during publication.

use super::distance::{dot_i8, normalize_in_place, prefetch_read, quantize_into, Metric};
use super::hnsw_core::{random_level, select_diverse_ties, Scored};
use super::scan_codes::{ScanCodec, ScanConstructionQuery, ScanQuery};
use crate::engine::executor::select::grace::SpillTmpDir;
use crate::engine::executor::spill_quota::TrackedSpillWriter;
use crate::storage::RowId;
use crate::types::QueryError;
use parking_lot::Mutex;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::Write;
use std::path::Path;

struct CodeSpill {
    writer: Option<TrackedSpillWriter>,
    directory: Option<SpillTmpDir>,
    mapping: Option<super::spill_rows::MappedSpillBytes>,
    prefetch: Option<super::spill_rows::MappedPrefetch>,
}
impl CodeSpill {
    fn create(data_dir: &Path) -> Result<Self, QueryError> {
        let directory = SpillTmpDir::create_kind(data_dir, "vector-build")?;
        let writer = TrackedSpillWriter::create_new(
            &directory.path().join("codes"),
            directory.charger(),
            4 * 1024 * 1024,
        )?;
        Ok(Self {
            writer: Some(writer),
            directory: Some(directory),
            mapping: None,
            prefetch: None,
        })
    }

    fn seal(&mut self, expected: usize, reuse: bool) -> Result<(), QueryError> {
        if self.writer.is_none() {
            return Ok(());
        }
        if self
            .writer
            .take()
            .expect("construction spill sealed once")
            .finish()?
            != expected as u64
        {
            return Err(super::codec::vector_err(
                "HNSW construction spill length mismatch",
            ));
        }
        let directory = self.directory.take().expect("construction spill owner");
        let file =
            crate::platform::file::File::open(directory.path().join("codes")).map_err(spill_io)?;
        let mapping = super::spill_rows::MappedSpillBytes::open_with_reuse(file, directory, reuse)?;
        if mapping.len() != expected {
            return Err(super::codec::vector_err(
                "HNSW construction spill changed before mapping",
            ));
        }
        if reuse {
            mapping.advise_random();
        } else {
            mapping.advise_sequential();
        }
        self.mapping = Some(mapping);
        Ok(())
    }
}
fn spill_io(error: std::io::Error) -> QueryError {
    crate::engine::executor::spill_quota::query_error_from_io(&error).unwrap_or_else(|| {
        crate::storage::error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("HNSW construction code spill: {error}"),
        )
    })
}
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU8, AtomicUsize, Ordering};

/// Readers and writers hold the owning node's lock. Separate atomic planes
/// keep the graph Sync without unaligned atomics or interior-mutable slices.
/// After construction joins, publication only reads these immutable ids.
#[derive(Default)]
struct ConstructionEdges {
    low: Vec<AtomicU16>,
    high: Vec<AtomicU8>,
    full: Vec<AtomicU32>,
    high_bits: u8,
}

impl ConstructionEdges {
    fn bits(rows: usize) -> u8 {
        if rows <= 1 << 16 {
            16
        } else if rows <= 1 << 20 {
            20
        } else if rows <= 1 << 24 {
            24
        } else {
            32
        }
    }
    fn bytes_for(len: usize, rows: usize) -> Option<usize> {
        let low = len.checked_mul(2)?;
        match Self::bits(rows) {
            16 => Some(low),
            20 => low.checked_add(len.div_ceil(2)),
            24 => low.checked_add(len),
            _ => len.checked_mul(4),
        }
    }
    fn new(len: usize, rows: usize) -> Self {
        let bits = Self::bits(rows);
        if bits == 32 {
            return Self {
                full: (0..len).map(|_| AtomicU32::new(0)).collect(),
                ..Self::default()
            };
        }
        let high_len = match bits {
            16 => 0,
            20 => len.div_ceil(2),
            _ => len,
        };
        Self {
            low: (0..len).map(|_| AtomicU16::new(0)).collect(),
            high: (0..high_len).map(|_| AtomicU8::new(0)).collect(),
            full: Vec::new(),
            high_bits: bits - 16,
        }
    }
    fn load(&self, position: usize) -> u32 {
        if !self.full.is_empty() {
            return self.full[position].load(Ordering::Relaxed);
        }
        let high = match self.high_bits {
            4 => (self.high[position / 2].load(Ordering::Relaxed) >> (position % 2 * 4)) & 15,
            8 => self.high[position].load(Ordering::Relaxed),
            _ => 0,
        };
        u32::from(self.low[position].load(Ordering::Relaxed)) | (u32::from(high) << 16)
    }
    fn store(&self, position: usize, id: u32) {
        if !self.full.is_empty() {
            self.full[position].store(id, Ordering::Relaxed);
            return;
        }
        debug_assert!(u64::from(id) < (1u64 << (16 + self.high_bits)));
        self.low[position].store(id as u16, Ordering::Relaxed);
        match self.high_bits {
            4 => {
                let shift = position % 2 * 4;
                let mask = 15u8 << shift;
                let high = ((id >> 16) as u8) << shift;
                // Adjacent nodes can share a byte when their capacities are
                // odd. Preserve the sibling nibble atomically under its own
                // node lock; no multi-node lock ordering is introduced.
                self.high[position / 2]
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                        Some((old & !mask) | high)
                    })
                    .expect("unconditional nibble update");
            }
            8 => self.high[position].store((id >> 16) as u8, Ordering::Relaxed),
            _ => {}
        }
    }
    fn allocated_bytes(&self) -> usize {
        self.low.capacity() * 2 + self.high.capacity() + self.full.capacity() * 4
    }

    fn store_many(&self, offset: usize, ids: &[u32]) {
        if self.high_bits != 4 {
            for (position, &id) in ids.iter().enumerate() {
                self.store(offset + position, id);
            }
            return;
        }
        let mut position = 0;
        if offset % 2 != 0 && !ids.is_empty() {
            self.store(offset, ids[0]);
            position = 1;
        }
        while position + 1 < ids.len() {
            let (a, b) = (ids[position], ids[position + 1]);
            debug_assert!(a < 1 << 20 && b < 1 << 20);
            self.low[offset + position].store(a as u16, Ordering::Relaxed);
            self.low[offset + position + 1].store(b as u16, Ordering::Relaxed);
            // Both halves belong to this locked adjacency list. Boundary
            // halves still use store's RMW to protect a neighboring node.
            self.high[(offset + position) / 2].store(
                (a >> 16) as u8 | (((b >> 16) as u8) << 4),
                Ordering::Relaxed,
            );
            position += 2;
        }
        if position < ids.len() {
            self.store(offset + position, ids[position]);
        }
    }
}

pub(crate) struct BulkHnsw {
    generation: usize,
    dim: usize,
    m: usize,
    ef: usize,
    metric: Metric,
    base_fanout: usize,
    native_online: bool,
    exact_int8: bool,
    compact_links: bool,
    calibration: (f32, f32, f32),
    scan_codec: Option<ScanCodec>,
    scan_residual_limit: f32,
    code_stride: usize,
    codes: Vec<i8>,
    upper_codes: Vec<i8>,
    upper_cache_limit: usize,
    prefetch_limit: usize,
    code_spill: Option<CodeSpill>,
    exact_spill: Option<CodeSpill>,
    encoding: Vec<i8>,
    norms: Vec<i32>,
    rows: Vec<RowId>,
    levels: Vec<u8>,
    offsets: Vec<u32>,
    edges: ConstructionEdges,
    counts: Vec<AtomicU16>,
    locks: Vec<Mutex<()>>,
    entry: u32,
    expected: usize,
}

struct Scratch {
    prepared: Option<(usize, u32)>,
    query: Vec<f32>,
    scan_query: Option<ScanQuery>,
    refinement_query: Option<ScanConstructionQuery>,
    visited: Vec<u64>,
    touched: Vec<u32>,
    frontier: BinaryHeap<Reverse<Scored<u32>>>,
    results: BinaryHeap<Scored<u32>>,
    neighbors: Vec<u32>,
    expansion: Vec<u32>,
}

/// Collect freed scratch on its allocation owner before a short-lived build
/// worker exits. Otherwise successive phases can strand several heaps' free
/// slabs alongside the next phase's navigation image.
struct ConstructionWorkerReclaim;
impl Drop for ConstructionWorkerReclaim {
    fn drop(&mut self) {
        #[cfg(feature = "fast-alloc")]
        crate::engine::memory_reclaimer::reclaim_current_thread_allocator();
    }
}
impl Scratch {
    fn new(rows: usize, ef: usize, m: usize) -> Self {
        Self {
            prepared: None,
            query: Vec::new(),
            scan_query: None,
            refinement_query: None,
            visited: vec![0; rows.div_ceil(64)],
            touched: Vec::with_capacity(rows.div_ceil(64)),
            frontier: BinaryHeap::with_capacity(ef * m),
            results: BinaryHeap::with_capacity(ef + 1),
            neighbors: Vec::with_capacity(m * 4),
            expansion: Vec::with_capacity(m * 4),
        }
    }
    fn begin(&mut self) {
        for word in self.touched.drain(..) {
            self.visited[word as usize] = 0;
        }
        self.frontier.clear();
        self.results.clear();
    }
    fn visit(&mut self, id: u32) -> bool {
        let word = id as usize / 64;
        let mask = 1u64 << (id % 64);
        let bits = &mut self.visited[word];
        if *bits & mask != 0 {
            false
        } else {
            if *bits == 0 {
                self.touched.push(word as u32);
            }
            *bits |= mask;
            true
        }
    }

    fn seen(&self, id: u32) -> bool {
        self.visited[id as usize / 64] & (1u64 << (id % 64)) != 0
    }
}

impl BulkHnsw {
    /// Counts actual deterministic layers, including temporary reverse-edge headroom.
    pub(crate) fn estimated_bytes(
        rows: usize,
        dim: usize,
        m: usize,
        threads: usize,
        ef: usize,
    ) -> Option<usize> {
        Self::estimate(rows, dim, m, threads, ef, false, 0)
    }

    pub(crate) fn estimated_compact_bytes(
        rows: usize,
        dim: usize,
        m: usize,
        threads: usize,
        ef: usize,
        cache_limit: usize,
    ) -> Option<usize> {
        Self::estimate(rows, dim, m, threads, ef, true, cache_limit)
    }

    pub(crate) fn estimated_mapped_bytes(
        rows: usize,
        dim: usize,
        m: usize,
        threads: usize,
        ef: usize,
        cache_limit: usize,
    ) -> Option<usize> {
        Self::estimate(rows, dim, m, threads, ef, false, cache_limit)
    }

    fn estimate(
        rows: usize,
        dim: usize,
        m: usize,
        threads: usize,
        ef: usize,
        compact: bool,
        cache_limit: usize,
    ) -> Option<usize> {
        if rows == 0 || rows > u32::MAX as usize || m < 2 || m > 16383 {
            return None;
        }
        let ef = ef.max(m);
        let ml = 1.0 / (m as f64).ln();
        let upper = (0..rows).try_fold(0usize, |total, id| {
            total.checked_add(random_level(
                id as u64,
                ml,
                super::hnsw_paged::VEC_MAX_LAYER,
            ))
        })?;
        let layers = rows.checked_add(upper)?;
        if layers > u32::MAX as usize {
            return None;
        }
        let edges = rows
            .checked_mul(m.checked_mul(if compact { 2 } else { 3 })?)?
            .checked_add(upper.checked_mul(m.checked_mul(if compact { 1 } else { 2 })?)?)?;
        rows.checked_mul(dim.checked_add(32)?)?
            .checked_add(upper.checked_mul(dim)?.min(cache_limit))?
            .checked_add(ConstructionEdges::bytes_for(edges, rows)?)?
            .checked_add(layers.checked_mul(2)?)?
            .checked_add(
                threads.max(1).checked_mul(
                    rows.div_ceil(64)
                        .checked_mul(12)?
                        .checked_add(ef.checked_mul(m)?.checked_mul(64)?)?
                        .checked_add(1024 * 1024)?,
                )?,
            )?
            .checked_add(16 * 1024 * 1024)
    }

    /// Caller owns a reservation covering estimated_bytes for this shape.
    pub(crate) fn new(
        rows: usize,
        dim: usize,
        metric: Metric,
        m: usize,
        ef: usize,
        calibration: (f32, f32, f32),
        spill_data_dir: Option<&Path>,
    ) -> Result<Self, QueryError> {
        let code_spill = if let Some(data_dir) = spill_data_dir {
            Some(CodeSpill::create(data_dir)?)
        } else {
            None
        };
        static GENERATION: AtomicUsize = AtomicUsize::new(0);
        Ok(Self {
            generation: GENERATION.fetch_add(1, Ordering::Relaxed),
            dim,
            m,
            ef: ef.max(m),
            metric,
            calibration,
            base_fanout: m,
            native_online: false,
            exact_int8: false,
            compact_links: false,
            scan_codec: None,
            scan_residual_limit: 0.0,
            code_stride: dim,
            codes: Vec::with_capacity(if code_spill.is_some() { 0 } else { rows * dim }),
            upper_codes: Vec::new(),
            upper_cache_limit: 0,
            prefetch_limit: usize::MAX,
            code_spill,
            exact_spill: None,
            encoding: vec![0; dim],
            norms: Vec::with_capacity(rows),
            rows: Vec::with_capacity(rows),
            levels: Vec::with_capacity(rows),
            offsets: Vec::with_capacity(rows),
            edges: ConstructionEdges::default(),
            counts: Vec::new(),
            locks: Vec::new(),
            entry: 0,
            expected: rows,
        })
    }

    pub(crate) fn with_native_fanout(mut self, exact_int8: bool) -> Self {
        self.base_fanout = self.m * 2;
        self.native_online = true;
        self.exact_int8 = exact_int8;
        self
    }

    /// When codes exceed the file-cache share, trade reverse-edge slack for
    /// more navigation residency and admitted workers. Published degree caps
    /// remain 2*M / M; ordinary resident builds retain their amortized slack.
    pub(crate) fn with_compact_links(mut self, cache_limit: usize) -> Self {
        self.compact_links = true;
        self.upper_cache_limit = cache_limit;
        self
    }

    pub(crate) fn with_mapped_links(mut self, cache_limit: usize) -> Self {
        self.compact_links = false;
        self.upper_cache_limit = cache_limit;
        self
    }

    pub(crate) fn with_prefetch_limit(mut self, bytes: usize) -> Self {
        self.prefetch_limit = bytes;
        self
    }

    pub(crate) fn prefetch_bytes(&self) -> usize {
        self.code_spill
            .as_ref()
            .and_then(|spill| spill.prefetch.as_ref())
            .map_or(0, |hints| hints.bytes())
    }

    /// Native unquantized indexes group identical rows. Keep their grouped
    /// constructor when codes coincide. Hash collisions only decline the
    /// optional fast path; distinct SQL rows are never merged here.
    pub(crate) fn has_distinct_native_codes(&mut self) -> Result<bool, QueryError> {
        self.seal_codes()?;
        let bytes =
            self.rows.len().checked_mul(8).ok_or_else(|| {
                super::codec::vector_err("HNSW duplicate-check capacity overflow")
            })?;
        let Some(_charge) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                bytes,
            )
        else {
            return Ok(false);
        };
        use std::hash::{Hash, Hasher};
        let mut fingerprints = Vec::with_capacity(self.rows.len());
        for id in 0..self.rows.len() {
            if id % 256 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            let mut hash = std::collections::hash_map::DefaultHasher::new();
            self.code(id as u32).hash(&mut hash);
            fingerprints.push(hash.finish());
        }
        fingerprints.sort_unstable();
        Ok(fingerprints.windows(2).all(|pair| pair[0] != pair[1]))
    }

    /// Per-axis codes retain narrow dimensions when a global SQ8 scale would
    /// erase them. Exact rows are immutable spill data, read only for coarse
    /// residuals, zero cosine vectors, or exceptional arithmetic.
    pub(crate) fn new_scan(
        rows: usize,
        dim: usize,
        metric: Metric,
        m: usize,
        ef: usize,
        codec: ScanCodec,
        data_dir: &Path,
        spill_codes: bool,
    ) -> Result<Self, QueryError> {
        if codec.dimension() != dim || codec.metric() != metric {
            return Err(super::codec::vector_err(
                "HNSW construction codec shape changed",
            ));
        }
        if rows
            .checked_mul(dim)
            .and_then(|n| n.checked_mul(4))
            .is_none_or(|bytes| bytes > isize::MAX as usize)
        {
            return Err(super::codec::vector_err(
                "HNSW exact construction spill exceeds address space",
            ));
        }
        let mut graph = Self::new(
            rows,
            dim,
            metric,
            m,
            ef,
            (0.0, 1.0, 1.0),
            spill_codes.then_some(data_dir),
        )?;
        // Per-axis navigation retains the resident cold builder's base
        // fanout; calibrated global-code builds keep their established M.
        graph.base_fanout = m * 2;
        graph.code_stride = codec.record_len();
        graph.codes = Vec::with_capacity(if spill_codes {
            0
        } else {
            rows * graph.code_stride
        });
        graph.encoding.resize(graph.code_stride, 0);
        graph.scan_residual_limit = codec.exact_residual_limit();
        graph.scan_codec = Some(codec);
        graph.norms = Vec::new();
        graph.exact_spill = Some(CodeSpill::create(data_dir)?);
        Ok(graph)
    }
    pub(crate) fn push(&mut self, row: RowId, vector: &[f32]) -> Result<(), QueryError> {
        if vector.len() != self.dim || self.rows.len() >= self.expected {
            return Err(super::codec::vector_err(
                "HNSW construction input changed between passes",
            ));
        }
        if let Some(codec) = &self.scan_codec {
            // SAFETY: i8 and u8 have identical layout; the mutable view is
            // exclusive and all byte representations are valid.
            let output = unsafe {
                std::slice::from_raw_parts_mut(
                    self.encoding.as_mut_ptr().cast::<u8>(),
                    self.encoding.len(),
                )
            };
            if !codec.encode(vector, output) {
                output.fill(0);
                output[..4].copy_from_slice(&f32::INFINITY.to_le_bytes());
            }
            // The spill is private and consumed on this same architecture.
            // Its f32 mapping is aligned and never used as a persistent format.
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    vector.as_ptr().cast::<u8>(),
                    std::mem::size_of_val(vector),
                )
            };
            self.exact_spill
                .as_mut()
                .expect("scan exact spill")
                .writer
                .as_mut()
                .expect("unsealed exact rows")
                .write_all(bytes)
                .map_err(spill_io)?;
        } else {
            let mut normalized = Vec::new();
            let source = if self.metric == Metric::Cosine && !self.exact_int8 {
                normalized.extend_from_slice(vector);
                normalize_in_place(&mut normalized);
                normalized.as_slice()
            } else {
                vector
            };
            if self.exact_int8 {
                for (code, &value) in self.encoding.iter_mut().zip(source) {
                    *code = value as i8;
                    if f32::from(*code) != value {
                        return Err(super::codec::vector_err(
                            "HNSW lossless INT8 construction input changed",
                        ));
                    }
                }
            } else {
                quantize_into(
                    source,
                    self.calibration.0,
                    self.calibration.1,
                    &mut self.encoding,
                );
            }
            self.norms.push(dot_i8(&self.encoding, &self.encoding));
        }
        if let Some(spill) = &mut self.code_spill {
            // SAFETY: i8 and u8 have identical layouts and every bit pattern is
            // valid. The writer copies these immutable bytes before returning.
            let bytes = unsafe {
                std::slice::from_raw_parts(self.encoding.as_ptr().cast::<u8>(), self.encoding.len())
            };
            spill
                .writer
                .as_mut()
                .expect("unsealed construction codes")
                .write_all(bytes)
                .map_err(spill_io)?;
        } else {
            self.codes.extend_from_slice(&self.encoding);
        }
        self.rows.push(row);
        Ok(())
    }
    fn seal_codes(&mut self) -> Result<(), QueryError> {
        if let Some(spill) = &mut self.code_spill {
            spill.seal(self.rows.len() * self.code_stride, true)?;
        }
        if let Some(spill) = &mut self.exact_spill {
            spill.seal(self.rows.len() * self.dim * 4, self.native_online)?;
        }
        Ok(())
    }
    #[inline]
    fn code(&self, id: u32) -> &[i8] {
        if !self.upper_codes.is_empty() && self.levels[id as usize] != 0 {
            let offset = (self.offsets[id as usize] as usize - id as usize) * self.code_stride;
            if offset < self.upper_codes.len() {
                return &self.upper_codes[offset..offset + self.code_stride];
            }
        }
        let offset = id as usize * self.code_stride;
        if let Some(spill) = &self.code_spill {
            let bytes = &spill.mapping.as_ref().expect("sealed construction codes")
                [offset..offset + self.code_stride];
            // SAFETY: the completed mapping is immutable, i8 accepts every
            // byte representation, and this borrow cannot outlive the owner.
            unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<i8>(), bytes.len()) }
        } else {
            &self.codes[offset..offset + self.code_stride]
        }
    }

    #[inline]
    fn prefetch_code(&self, id: u32) {
        let cached = !self.upper_codes.is_empty()
            && self.levels[id as usize] != 0
            && (self.offsets[id as usize] as usize - id as usize)
                < self.upper_codes.len() / self.code_stride;
        if !cached {
            if let Some(spill) = &self.code_spill {
                if let (Some(mapping), Some(hints)) = (&spill.mapping, &spill.prefetch) {
                    hints.prefetch(mapping, id as usize * self.code_stride, self.code_stride);
                }
            }
        }
        prefetch_read(self.code(id).as_ptr());
    }

    fn refresh_prefetch(&self) {
        if let Some(spill) = &self.code_spill {
            if let (Some(mapping), Some(hints)) = (&spill.mapping, &spill.prefetch) {
                hints.refresh(mapping);
            }
        }
    }

    fn exact_row(&self, id: u32) -> &[f32] {
        let mapping = self
            .exact_spill
            .as_ref()
            .expect("scan exact rows")
            .mapping
            .as_ref()
            .expect("sealed exact rows");
        let start = id as usize * self.dim * 4;
        // SAFETY: push wrote exactly dim native f32 values per row, the
        // mapping is page-aligned and immutable, and id names an existing row.
        unsafe { std::slice::from_raw_parts(mapping[start..].as_ptr().cast(), self.dim) }
    }

    fn prepare_query(&self, id: u32, scratch: &mut Scratch) {
        if scratch.prepared == Some((self.generation, id)) {
            return;
        }
        scratch.prepared = Some((self.generation, id));
        scratch.scan_query = None;
        scratch.query.clear();
        if !self.native_online {
            if let Some(codec) = &self.scan_codec {
                scratch.query.extend_from_slice(self.exact_row(id));
                scratch.scan_query = ScanQuery::new(codec, &scratch.query);
            }
        }
    }

    #[inline]
    fn query_distance(&self, query: u32, other: u32, scratch: &Scratch) -> f64 {
        if self.scan_codec.is_some() && !self.native_online {
            let code = self.code(other);
            // SAFETY: immutable byte view of the completed record.
            let record = unsafe { std::slice::from_raw_parts(code.as_ptr().cast(), code.len()) };
            if let Some(prepared) = &scratch.scan_query {
                let residual = ScanCodec::record_residual(record);
                if residual.is_finite() && residual <= self.scan_residual_limit {
                    let value = prepared.estimate(record);
                    if value.is_finite() {
                        return value;
                    }
                }
            }
            return super::distance::ordering_distance(
                &scratch.query,
                self.exact_row(other),
                self.metric,
            );
        }
        self.distance(query, other)
    }
    #[inline]
    fn distance(&self, a: u32, b: u32) -> f64 {
        if let Some(codec) = &self.scan_codec {
            let record = |id| {
                let code = self.code(id);
                // SAFETY: immutable byte view, bounded by the live code owner.
                unsafe { std::slice::from_raw_parts(code.as_ptr().cast::<u8>(), code.len()) }
            };
            let (ar, br) = (record(a), record(b));
            let limit = self.scan_residual_limit;
            let (a_residual, b_residual) = (
                ScanCodec::record_residual(ar),
                ScanCodec::record_residual(br),
            );
            if a_residual.is_finite()
                && b_residual.is_finite()
                && a_residual <= limit
                && b_residual <= limit
            {
                let distance = codec.construction_distance(ar, br);
                if distance.is_finite() {
                    return distance;
                }
            }
            let mapping = self
                .exact_spill
                .as_ref()
                .expect("scan exact rows")
                .mapping
                .as_ref()
                .expect("sealed exact rows");
            let row = |id: u32| {
                let start = id as usize * self.dim * 4;
                // SAFETY: a file mapping is page-aligned, each fixed record
                // starts at a multiple of four, and push wrote native f32s.
                unsafe {
                    std::slice::from_raw_parts(mapping[start..].as_ptr().cast::<f32>(), self.dim)
                }
            };
            return super::distance::ordering_distance(row(a), row(b), self.metric);
        }
        let dot = i64::from(dot_i8(self.code(a), self.code(b)));
        let (na, nb) = (
            i64::from(self.norms[a as usize]),
            i64::from(self.norms[b as usize]),
        );
        if self.exact_int8 {
            match self.metric {
                Metric::Dot => return -(dot as f64),
                Metric::Cosine => {
                    if na == 0 || nb == 0 {
                        return 1.0;
                    }
                    return 1.0
                        - (dot as f64 / ((na as f64) * (nb as f64)).sqrt()).clamp(-1.0, 1.0);
                }
                _ => {}
            }
        }
        (na + nb - 2 * dot) as f64
    }
    fn edge_offset(&self, id: u32, layer: usize) -> usize {
        let id = id as usize;
        id * self.temporary_capacity(0)
            + (self.offsets[id] as usize - id) * self.temporary_capacity(1)
            + if layer == 0 {
                0
            } else {
                self.temporary_capacity(0) + (layer - 1) * self.temporary_capacity(1)
            }
    }
    fn temporary_capacity(&self, layer: usize) -> usize {
        if layer == 0 {
            self.m * if self.compact_links { 2 } else { 3 }
        } else {
            self.m * if self.compact_links { 1 } else { 2 }
        }
    }
    fn neighbors(&self, id: u32, layer: usize, output: &mut Vec<u32>) {
        output.clear();
        if (self.levels[id as usize] as usize) < layer {
            return;
        }
        let _guard = self.locks[id as usize].lock();
        let offset = self.edge_offset(id, layer);
        let count = self.counts[self.offsets[id as usize] as usize + layer].load(Ordering::Relaxed)
            as usize;
        output.extend((offset..offset + count).map(|position| self.edges.load(position)));
    }
    fn store_locked(&self, id: u32, layer: usize, neighbors: &[u32]) {
        let offset = self.edge_offset(id, layer);
        debug_assert!(neighbors.len() <= self.temporary_capacity(layer));
        self.edges.store_many(offset, neighbors);
        self.counts[self.offsets[id as usize] as usize + layer]
            .store(neighbors.len() as u16, Ordering::Relaxed);
    }
    fn select(&self, ranked: &[Scored<u32>], maximum: usize) -> Result<Vec<u32>, QueryError> {
        let mut comparisons = 0usize;
        let mut selected =
            select_diverse_ties(ranked, maximum, self.metric != Metric::Dot, |a, b| {
                if comparisons % 64 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                comparisons += 1;
                Ok::<_, QueryError>(self.distance(a, b))
            })?;
        // Native unquantized graphs retain only diverse edges. Their serial
        // constructor also prunes reverse lists eagerly; preserve that graph
        // geometry rather than substituting the paged builder's soft caps.
        if !self.native_online {
            super::hnsw_core::fill_pruned(&mut selected, ranked, maximum, |s| s.id);
        }
        Ok(selected.into_iter().map(|candidate| candidate.id).collect())
    }
    fn prune(&self, id: u32, neighbors: &[u32], maximum: usize) -> Result<Vec<u32>, QueryError> {
        const AHEAD: usize = 8;
        self.prefetch_code(id);
        for &other in neighbors.iter().take(AHEAD) {
            self.prefetch_code(other);
        }
        let mut ranked = Vec::with_capacity(neighbors.len());
        for (i, &other) in neighbors.iter().enumerate() {
            if let Some(&future) = neighbors.get(i + AHEAD) {
                self.prefetch_code(future);
            }
            if other != id {
                ranked.push(Scored {
                    id: other,
                    dist: self.distance(id, other),
                });
            }
        }
        ranked.sort_unstable();
        ranked.dedup_by_key(|candidate| candidate.id);
        self.select(&ranked, maximum)
    }
    fn add_edges(&self, id: u32, layer: usize, additions: &[u32]) -> Result<(), QueryError> {
        let _guard = self.locks[id as usize].lock();
        let offset = self.edge_offset(id, layer);
        let count = self.counts[self.offsets[id as usize] as usize + layer].load(Ordering::Relaxed)
            as usize;
        let mut neighbors: Vec<_> = (offset..offset + count)
            .map(|position| self.edges.load(position))
            .collect();
        let mut changed = false;
        for &addition in additions {
            if addition != id && !neighbors.contains(&addition) {
                neighbors.push(addition);
                changed = true;
            }
        }
        if !changed {
            return Ok(());
        }
        let capacity = if !self.native_online {
            self.temporary_capacity(layer)
        } else if layer == 0 {
            self.m * 2
        } else {
            self.m
        };
        if neighbors.len() > capacity {
            neighbors = self.prune(id, &neighbors, if layer == 0 { self.m * 2 } else { self.m })?;
        }
        self.store_locked(id, layer, &neighbors);
        Ok(())
    }
    fn greedy(
        &self,
        query: u32,
        mut current: u32,
        layer: usize,
        scratch: &mut Scratch,
    ) -> Result<u32, QueryError> {
        self.prepare_query(query, scratch);
        let mut best = self.query_distance(query, current, scratch);
        loop {
            crate::engine::cancel::check_cancelled()?;
            self.neighbors(current, layer, &mut scratch.neighbors);
            let before = current;
            const AHEAD: usize = 8;
            for &candidate in scratch.neighbors.iter().take(AHEAD) {
                self.prefetch_code(candidate);
            }
            for (i, &candidate) in scratch.neighbors.iter().enumerate() {
                if let Some(&future) = scratch.neighbors.get(i + AHEAD) {
                    self.prefetch_code(future);
                }
                let distance = self.query_distance(query, candidate, scratch);
                if distance < best {
                    best = distance;
                    current = candidate;
                }
            }
            if before == current {
                return Ok(current);
            }
        }
    }
    fn search(
        &self,
        query: u32,
        entry: u32,
        layer: usize,
        scratch: &mut Scratch,
    ) -> Result<Vec<Scored<u32>>, QueryError> {
        self.prepare_query(query, scratch);
        scratch.begin();
        scratch.visit(entry);
        let first = Scored {
            id: entry,
            dist: self.query_distance(query, entry, scratch),
        };
        scratch.frontier.push(Reverse(first));
        scratch.results.push(first);
        let mut steps = 0usize;
        while let Some(Reverse(candidate)) = scratch.frontier.pop() {
            if steps % 32 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            steps += 1;
            let worst = scratch.results.peek().unwrap().dist;
            if candidate.dist > worst && scratch.results.len() >= self.ef {
                break;
            }
            self.neighbors(candidate.id, layer, &mut scratch.neighbors);
            const AHEAD: usize = 8;
            for &id in scratch.neighbors.iter().take(AHEAD) {
                if !scratch.seen(id) {
                    self.prefetch_code(id);
                }
            }
            for index in 0..scratch.neighbors.len() {
                let id = scratch.neighbors[index];
                if let Some(&ahead) = scratch.neighbors.get(index + AHEAD) {
                    if !scratch.seen(ahead) {
                        self.prefetch_code(ahead);
                    }
                }
                if !scratch.visit(id) {
                    continue;
                }
                let scored = Scored {
                    id,
                    dist: self.query_distance(query, id, scratch),
                };
                if scratch.results.len() < self.ef || scored < *scratch.results.peek().unwrap() {
                    scratch.frontier.push(Reverse(scored));
                    scratch.results.push(scored);
                    if scratch.results.len() > self.ef {
                        scratch.results.pop();
                    }
                    // Discard queued nodes that are no longer among the best
                    // visited candidates. This bounds stale frontier storage
                    // even on adversarial duplicate/tie-heavy inputs.
                    if scratch.frontier.len() >= self.ef.saturating_mul(4) {
                        let worst = *scratch.results.peek().unwrap();
                        scratch.frontier.retain(|Reverse(value)| *value <= worst);
                    }
                }
            }
        }
        let mut ranked = scratch.results.drain().collect::<Vec<_>>();
        ranked.sort_unstable();
        Ok(ranked)
    }
    fn rank_for_selection(&self, id: u32, ranked: &mut [Scored<u32>]) {
        if self.scan_codec.is_some() && !self.native_online {
            // Navigation uses an original query. Diversity compares decoded
            // pairs, so re-score its pool on that same distance scale first.
            for candidate in ranked.iter_mut() {
                candidate.dist = self.distance(id, candidate.id);
            }
            ranked.sort_unstable();
        }
    }

    fn symmetrize_refinement(&self, threads: usize) -> Result<(), QueryError> {
        let cancellation = crate::engine::cancel::CancellationSnapshot::capture();
        for prune in [false, true] {
            let cursor = AtomicUsize::new(0);
            crate::util::thread_spawn::scope(|scope| -> Result<(), QueryError> {
                let mut workers = Vec::new();
                for _ in 0..threads.max(1).min(self.rows.len()) {
                    let cursor = &cursor;
                    let cancellation = &cancellation;
                    workers.push(scope.spawn(move || -> Result<(), QueryError> {
                        let _scope = cancellation.enter();
                        let _reclaim = ConstructionWorkerReclaim;
                        let mut neighbors = Vec::with_capacity(self.m * 3);
                        loop {
                            let id = cursor.fetch_add(1, Ordering::Relaxed);
                            if id >= self.rows.len() {
                                break;
                            }
                            crate::engine::cancel::check_cancelled()?;
                            self.refresh_prefetch();
                            self.neighbors(id as u32, 0, &mut neighbors);
                            if prune {
                                let selected = self.prune(id as u32, &neighbors, self.m * 2)?;
                                let _guard = self.locks[id].lock();
                                self.store_locked(id as u32, 0, &selected);
                            } else {
                                // Publish reverse proposals after all forward
                                // replacements joined, so a later replacement
                                // cannot silently erase an earlier proposal.
                                neighbors.truncate(self.m * 2);
                                for &other in &neighbors {
                                    crate::engine::cancel::check_cancelled()?;
                                    self.add_edges(other, 0, &[id as u32])?;
                                }
                            }
                        }
                        Ok(())
                    }));
                }
                let mut error = None;
                for worker in workers {
                    match worker.join() {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            error.get_or_insert(err);
                        }
                        Err(_) => {
                            error.get_or_insert_with(|| {
                                super::codec::vector_err("HNSW reverse refinement worker panicked")
                            });
                        }
                    }
                }
                error.map_or(Ok(()), Err)
            })?;
        }
        Ok(())
    }

    fn prepare_refinement_query(&self, id: u32, scratch: &mut Scratch) {
        let code = self.code(id);
        // SAFETY: immutable byte view of a complete navigation record.
        let record = unsafe { std::slice::from_raw_parts(code.as_ptr().cast(), code.len()) };
        scratch.refinement_query = self.scan_codec.as_ref().and_then(|codec| {
            (ScanCodec::record_residual(record) <= self.scan_residual_limit)
                .then(|| ScanConstructionQuery::new(codec, record))
                .flatten()
        });
    }

    #[inline]
    fn refinement_distance(&self, query: u32, other: u32, scratch: &Scratch) -> f64 {
        if let Some(prepared) = &scratch.refinement_query {
            let code = self.code(other);
            // SAFETY: immutable byte view of a complete navigation record.
            let record = unsafe { std::slice::from_raw_parts(code.as_ptr().cast(), code.len()) };
            let residual = ScanCodec::record_residual(record);
            if residual.is_finite() && residual <= self.scan_residual_limit {
                if let Some(value) = prepared.estimate(record) {
                    return value;
                }
            }
        }
        self.distance(query, other)
    }

    fn refine_neighborhoods(&self, threads: usize) -> Result<(), QueryError> {
        let started = std::time::Instant::now();
        let refinement_ef = self.ef.saturating_mul(2).min(self.rows.len());
        let cursor = AtomicUsize::new(0);
        let cancellation = crate::engine::cancel::CancellationSnapshot::capture();
        crate::util::thread_spawn::scope(|scope| -> Result<(), QueryError> {
            let mut workers = Vec::new();
            for _ in 0..threads.max(1).min(self.rows.len()) {
                let cursor = &cursor;
                let cancellation = &cancellation;
                workers.push(scope.spawn(move || -> Result<(), QueryError> {
                    let _scope = cancellation.enter();
                    let _reclaim = ConstructionWorkerReclaim;
                    let mut scratch = Scratch::new(self.rows.len(), self.ef, self.m);
                    loop {
                        let id = cursor.fetch_add(1, Ordering::Relaxed);
                        if id >= self.rows.len() {
                            break;
                        }
                        crate::engine::cancel::check_cancelled()?;
                        self.refresh_prefetch();
                        self.prepare_refinement_query(id as u32, &mut scratch);
                        scratch.begin();
                        scratch.visit(id as u32);
                        self.neighbors(id as u32, 0, &mut scratch.neighbors);
                        let original = scratch.neighbors.clone();
                        for position in 0..original.len() {
                            crate::engine::cancel::check_cancelled()?;
                            self.neighbors(original[position], 0, &mut scratch.expansion);
                            for &other in scratch.expansion.iter().take(8) {
                                self.prefetch_code(other);
                            }
                            for step in 0..scratch.expansion.len() {
                                if step % 64 == 0 {
                                    crate::engine::cancel::check_cancelled()?;
                                }
                                if let Some(&ahead) = scratch.expansion.get(step + 8) {
                                    self.prefetch_code(ahead);
                                }
                                let other = scratch.expansion[step];
                                if !scratch.visit(other) {
                                    continue;
                                }
                                let scored = Scored {
                                    id: other,
                                    dist: self.refinement_distance(id as u32, other, &scratch),
                                };
                                if scratch.results.len() < refinement_ef
                                    || scored < *scratch.results.peek().unwrap()
                                {
                                    scratch.results.push(scored);
                                    if scratch.results.len() > refinement_ef {
                                        scratch.results.pop();
                                    }
                                }
                            }
                        }
                        let mut ranked: Vec<_> = scratch.results.drain().collect();
                        // Preserve the old pool as alternatives, even when a
                        // useful long edge falls outside the new nearest ef.
                        ranked.extend(original.into_iter().map(|other| Scored {
                            id: other,
                            dist: self.distance(id as u32, other),
                        }));
                        self.rank_for_selection(id as u32, &mut ranked);
                        ranked.dedup_by_key(|candidate| candidate.id);
                        let selected = self.select(&ranked, self.m * 2)?;
                        let _guard = self.locks[id].lock();
                        self.store_locked(id as u32, 0, &selected);
                    }
                    Ok(())
                }));
            }
            let mut error = None;
            for worker in workers {
                match worker.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        error.get_or_insert(err);
                    }
                    Err(_) => {
                        error.get_or_insert_with(|| {
                            super::codec::vector_err("HNSW refinement worker panicked")
                        });
                    }
                }
            }
            error.map_or(Ok(()), Err)
        })?;
        self.symmetrize_refinement(threads)?;
        if std::env::var_os("MYDB_HNSW_BUILD_TIMING").is_some() {
            eprintln!(
                "[hnsw] topology refined: rows={} elapsed={:?}",
                self.rows.len(),
                started.elapsed()
            );
        }
        Ok(())
    }

    fn link(&self, id: u32, scratch: &mut Scratch) -> Result<(), QueryError> {
        crate::engine::cancel::check_cancelled()?;
        self.refresh_prefetch();
        if id == self.entry {
            return Ok(());
        }
        let level = self.levels[id as usize] as usize;
        let mut current = self.entry;
        for layer in (level + 1..=self.levels[self.entry as usize] as usize).rev() {
            current = self.greedy(id, current, layer, scratch)?;
        }
        for layer in (0..=level.min(self.levels[self.entry as usize] as usize)).rev() {
            let mut ranked = self.search(id, current, layer, scratch)?;
            self.rank_for_selection(id, &mut ranked);
            let selected = self.select(
                &ranked
                    .iter()
                    .copied()
                    .filter(|candidate| candidate.id != id)
                    .collect::<Vec<_>>(),
                if layer == 0 { self.base_fanout } else { self.m },
            )?;
            self.add_edges(id, layer, &selected)?;
            for &neighbor in &selected {
                self.add_edges(neighbor, layer, &[id])?;
            }
            if let Some(best) = ranked.iter().find(|candidate| candidate.id != id) {
                current = best.id;
            }
        }
        Ok(())
    }
    pub(crate) fn build(&mut self, threads: usize) -> Result<(), QueryError> {
        if self.rows.len() != self.expected {
            return Err(super::codec::vector_err(
                "HNSW construction row count changed",
            ));
        }
        self.seal_codes()?;
        if let Some(spill) = &mut self.code_spill {
            spill.prefetch = spill
                .mapping
                .as_ref()
                .and_then(|mapping| mapping.prefetcher(self.prefetch_limit));
        }
        let ml = 1.0 / (self.m as f64).ln();
        let mut layers = 0usize;
        for id in 0..self.rows.len() {
            let level = random_level(id as u64, ml, super::hnsw_paged::VEC_MAX_LAYER);
            if level > self.levels.get(self.entry as usize).copied().unwrap_or(0) as usize {
                self.entry = id as u32;
            }
            self.levels.push(level as u8);
            self.offsets.push(layers as u32);
            layers += level + 1;
        }
        if self.code_spill.is_some() && self.upper_cache_limit != 0 {
            // Upper-layer vertices are repeatedly probed across unrelated
            // insertions. Pack their immutable records rather than pinning a
            // 4 KiB file page for each scattered sub-KiB hot record. The rank
            // of upper layers is already present in offsets, so no node-sized
            // hash table or second id map is needed.
            let records = (layers - self.rows.len()).min(self.upper_cache_limit / self.code_stride);
            let mut upper = vec![0i8; records * self.code_stride];
            for id in 0..self.rows.len() {
                if id % 256 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                if self.levels[id] != 0 {
                    let offset = (self.offsets[id] as usize - id) * self.code_stride;
                    if offset >= upper.len() {
                        break;
                    }
                    upper[offset..offset + self.code_stride].copy_from_slice(self.code(id as u32));
                }
            }
            self.upper_codes = upper;
        }
        let edge_count = self.rows.len() * self.temporary_capacity(0)
            + (layers - self.rows.len()) * self.temporary_capacity(1);
        self.edges = ConstructionEdges::new(edge_count, self.rows.len());
        self.counts = (0..layers).map(|_| AtomicU16::new(0)).collect();
        self.locks = (0..self.rows.len()).map(|_| Mutex::new(())).collect();
        if self.native_online {
            // Native unquantized graphs have an online entry and eager
            // diversity pruning. Keep their insertion order and geometry;
            // compact immutable payload access supplies the speedup here.
            self.entry = 0;
            let mut scratch = Scratch::new(self.rows.len(), self.ef, self.m);
            for id in 0..self.rows.len() {
                self.link(id as u32, &mut scratch)?;
                if self.levels[id] > self.levels[self.entry as usize] {
                    self.entry = id as u32;
                }
            }
            self.repair_connectivity()?;
            return Ok(());
        }
        let cursor = AtomicUsize::new(0);
        let completed = AtomicUsize::new(0);
        let timing = std::env::var("MYDB_HNSW_BUILD_TIMING").is_ok();
        let started = std::time::Instant::now();
        let cancellation = crate::engine::cancel::CancellationSnapshot::capture();
        crate::util::thread_spawn::scope(|scope| -> Result<(), QueryError> {
            let mut handles = Vec::new();
            let graph = &*self;
            for _ in 0..threads.max(1).min(self.rows.len()) {
                let cursor = &cursor;
                let cancellation = &cancellation;
                let completed = &completed;
                handles.push(scope.spawn(move || -> Result<(), QueryError> {
                    let _scope = cancellation.enter();
                    let _reclaim = ConstructionWorkerReclaim;
                    let mut scratch = Scratch::new(graph.rows.len(), graph.ef, graph.m);
                    loop {
                        let first = cursor.fetch_add(16, Ordering::Relaxed);
                        if first >= graph.rows.len() {
                            break;
                        }
                        crate::engine::cancel::check_cancelled()?;
                        for id in first..(first + 16).min(graph.rows.len()) {
                            graph.link(id as u32, &mut scratch)?;
                        }
                        let count = (graph.rows.len() - first).min(16);
                        let before = completed.fetch_add(count, Ordering::Relaxed);
                        if timing && (before + count) / 16384 != before / 16384 {
                            eprintln!(
                                "[hnsw] topology link: rows={}/{} elapsed={:?}",
                                before + count,
                                graph.rows.len(),
                                started.elapsed()
                            );
                        }
                    }
                    Ok(())
                }));
            }
            let mut first_error = None;
            for worker in handles {
                match worker.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        first_error.get_or_insert(error);
                    }
                    Err(_) => {
                        first_error.get_or_insert_with(|| {
                            super::codec::vector_err("HNSW construction worker panicked")
                        });
                    }
                }
            }
            first_error.map_or(Ok(()), Err)
        })?;
        if timing {
            eprintln!(
                "[hnsw] topology linked: rows={} elapsed={:?}",
                self.rows.len(),
                started.elapsed()
            );
        }
        let cursor = AtomicUsize::new(0);
        crate::util::thread_spawn::scope(|scope| -> Result<(), QueryError> {
            let mut handles = Vec::new();
            for _ in 0..threads.max(1).min(self.rows.len()) {
                let graph = &*self;
                let cursor = &cursor;
                let cancellation = &cancellation;
                handles.push(scope.spawn(move || -> Result<(), QueryError> {
                    let _scope = cancellation.enter();
                    let _reclaim = ConstructionWorkerReclaim;
                    loop {
                        let id = cursor.fetch_add(1, Ordering::Relaxed);
                        if id >= graph.rows.len() {
                            break;
                        }
                        if id % 64 == 0 {
                            crate::engine::cancel::check_cancelled()?;
                        }
                        graph.refresh_prefetch();
                        for layer in 0..=graph.levels[id] as usize {
                            let mut neighbors = Vec::new();
                            graph.neighbors(id as u32, layer, &mut neighbors);
                            let maximum = if layer == 0 { graph.m * 2 } else { graph.m };
                            let selected = graph.prune(id as u32, &neighbors, maximum)?;
                            let _guard = graph.locks[id].lock();
                            graph.store_locked(id as u32, layer, &selected);
                        }
                    }
                    Ok(())
                }));
            }
            let mut first_error = None;
            for worker in handles {
                match worker.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        first_error.get_or_insert(error);
                    }
                    Err(_) => {
                        first_error.get_or_insert_with(|| {
                            super::codec::vector_err("HNSW final prune worker panicked")
                        });
                    }
                }
            }
            first_error.map_or(Ok(()), Err)
        })?;
        if timing {
            eprintln!(
                "[hnsw] topology pruned: rows={} elapsed={:?}",
                self.rows.len(),
                started.elapsed()
            );
        }
        crate::engine::cancel::check_cancelled()?;
        if self.scan_codec.is_some() {
            self.refine_neighborhoods(threads)?;
        }
        self.repair_connectivity()?;
        crate::engine::cancel::check_cancelled()?;
        Ok(())
    }
    fn repair_connectivity(&self) -> Result<(), QueryError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let started = std::time::Instant::now();
        let timing = std::env::var_os("MYDB_HNSW_BUILD_TIMING").is_some();
        // 0 = unseen, 1 = discovery-tree leaf, 2 = internal vertex.
        let mut seen = vec![0u8; self.rows.len()];
        let mut pending = Vec::with_capacity(self.rows.len());
        let mut neighbors = Vec::new();
        let visit =
            |root: u32, seen: &mut [u8], pending: &mut Vec<u32>, neighbors: &mut Vec<u32>| {
                seen[root as usize] = 1;
                pending.push(root);
                let mut visited = 0usize;
                let mut leaf = root;
                while let Some(id) = pending.pop() {
                    if visited % 64 == 0 {
                        crate::engine::cancel::check_cancelled()?;
                    }
                    visited += 1;
                    leaf = id;
                    self.neighbors(id, 0, neighbors);
                    for &other in neighbors.iter() {
                        if seen[other as usize] == 0 {
                            seen[other as usize] = 1;
                            seen[id as usize] = 2;
                            pending.push(other);
                        }
                    }
                }
                // The last popped vertex discovers no new vertex: otherwise
                // one of its children would be popped after it. Consequently
                // none of its outgoing edges belongs to this discovery tree.
                Ok::<_, QueryError>(leaf)
            };
        let mut anchor = visit(self.entry, &mut seen, &mut pending, &mut neighbors)?;
        let mut scratch = Scratch::new(self.rows.len(), self.ef, self.m);
        let mut bridges = 0usize;
        for id in 0..self.rows.len() {
            if id % 64 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            if seen[id] == 0 {
                self.refresh_prefetch();
                // Connect near the new component rather than chaining unrelated
                // components through an arbitrary leaf. Only discovery leaves
                // are eligible, so pruning cannot destroy the existing tree.
                if let Some(near) = self
                    .search(id as u32, self.entry, 0, &mut scratch)?
                    .into_iter()
                    .find(|candidate| seen[candidate.id as usize] == 1)
                {
                    anchor = near.id;
                }
                self.neighbors(anchor, 0, &mut neighbors);
                // Only this leaf needs a spare edge. Preserve every other
                // discovery-tree edge, including all previously added bridges.
                // Construction workers have joined before this phase.
                if neighbors.len() >= self.m * 2 {
                    neighbors = self.prune(anchor, &neighbors, self.m * 2 - 1)?;
                }
                neighbors.push(id as u32);
                self.store_locked(anchor, 0, &neighbors);
                seen[anchor as usize] = 2;
                anchor = visit(id as u32, &mut seen, &mut pending, &mut neighbors)?;
                bridges += 1;
            }
        }
        if timing {
            eprintln!(
                "[hnsw] bounded connectivity: bridges={bridges} elapsed={:?}",
                started.elapsed()
            );
        }
        Ok(())
    }
    pub(crate) fn release_codes(&mut self) -> usize {
        let bytes = self.codes.capacity()
            + self.upper_codes.capacity()
            + self.norms.capacity() * std::mem::size_of::<i32>()
            + self.encoding.capacity();
        self.codes = Vec::new();
        self.upper_codes = Vec::new();
        self.norms = Vec::new();
        self.code_spill.take();
        self.exact_spill.take();
        self.encoding = Vec::new();
        bytes
    }

    /// Publication has copied every edge. Retain only row/level identity for
    /// importing the native runtime, avoiding overlapping two full topologies.
    pub(crate) fn release_link_storage(&mut self) -> usize {
        let bytes = self.edges.allocated_bytes()
            + self.counts.capacity() * std::mem::size_of::<AtomicU16>()
            + self.offsets.capacity() * std::mem::size_of::<u32>()
            + self.locks.capacity() * std::mem::size_of::<Mutex<()>>()
            + self.upper_codes.capacity();
        self.edges = ConstructionEdges::default();
        self.counts = Vec::new();
        self.offsets = Vec::new();
        self.locks = Vec::new();
        self.upper_codes = Vec::new();
        bytes
    }
    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }
    pub(crate) fn row(&self, id: usize) -> RowId {
        self.rows[id]
    }
    pub(crate) fn level(&self, id: usize) -> usize {
        self.levels[id] as usize
    }
    pub(crate) fn entry(&self) -> Option<(u32, usize)> {
        (!self.rows.is_empty()).then(|| (self.entry, self.level(self.entry as usize)))
    }
    pub(crate) fn copy_links(&self, id: usize, links: &mut Vec<Vec<u32>>) {
        links.resize_with(self.level(id) + 1, Vec::new);
        links.truncate(self.level(id) + 1);
        for (layer, output) in links.iter_mut().enumerate() {
            self.neighbors(id as u32, layer, output);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linking_never_uses_itself_as_the_entry_of_an_empty_lower_layer() {
        let mut graph = graph(128, 17, false);
        graph.build(1).unwrap();
        let entry = graph.entry;
        let id = (0..graph.len())
            .find(|&id| id as u32 != entry && graph.levels[id] == 1)
            .unwrap() as u32;
        for layer in 1..=graph.levels[entry as usize] as usize {
            graph.store_locked(
                entry,
                layer,
                if layer == 1 {
                    std::slice::from_ref(&id)
                } else {
                    &[]
                },
            );
        }
        graph.store_locked(id, 1, &[entry]);
        graph.store_locked(id, 0, &[]);
        let mut scratch = Scratch::new(graph.len(), graph.ef, graph.m);
        graph.link(id, &mut scratch).unwrap();
        let mut neighbors = Vec::new();
        graph.neighbors(id, 0, &mut neighbors);
        assert!(
            !neighbors.is_empty(),
            "a zero-distance self match must not isolate the lower layer"
        );
        assert!(!neighbors.contains(&id));
    }

    #[test]
    fn construction_edges_preserve_ids_across_all_storage_boundaries() {
        let compact = ConstructionEdges::new(6, 1 << 24);
        for (position, id) in [0, 65535, 65536, 0x80ffff, 0xffffff, 1]
            .into_iter()
            .enumerate()
        {
            compact.store(position, id);
            assert_eq!(compact.load(position), id);
        }
        assert_eq!(compact.allocated_bytes(), 18);
        let full = ConstructionEdges::new(3, (1 << 24) + 1);
        for (position, id) in [0x1000000, u32::MAX, 65536].into_iter().enumerate() {
            full.store(position, id);
            assert_eq!(full.load(position), id);
        }
        assert_eq!(full.allocated_bytes(), 12);
        let narrow = ConstructionEdges::new(2, 1 << 16);
        narrow.store(0, 65535);
        assert_eq!(narrow.load(0), 65535);
        assert_eq!(narrow.allocated_bytes(), 4);
        let twenty = ConstructionEdges::new(3, 1 << 20);
        for (position, id) in [65536, 0xfffff, 0x80000].into_iter().enumerate() {
            twenty.store(position, id);
            assert_eq!(twenty.load(position), id);
        }
        assert_eq!(twenty.allocated_bytes(), 8);
    }

    #[test]
    fn shared_twenty_bit_high_bytes_preserve_concurrent_neighbor_ids() {
        let edges = ConstructionEdges::new(2, 1 << 20);
        crate::util::thread_spawn::scope(|scope| {
            for position in 0..2 {
                let edges = &edges;
                scope.spawn(move || {
                    for high in 0..20000 {
                        let id = ((high % 16) << 16) | (41 + position as u32);
                        edges.store(position, id);
                        assert_eq!(edges.load(position), id);
                    }
                });
            }
        });
    }

    #[test]
    fn packed_adjacency_batches_preserve_shared_boundary_nibbles() {
        let edges = ConstructionEdges::new(6, 1 << 20);
        crate::util::thread_spawn::scope(|scope| {
            for offset in [0usize, 3] {
                let edges = &edges;
                scope.spawn(move || {
                    for high in 0..20000 {
                        let ids = std::array::from_fn::<_, 3, _>(|i| {
                            ((high % 16) << 16) | (i as u32 + offset as u32)
                        });
                        edges.store_many(offset, &ids);
                        for (i, &id) in ids.iter().enumerate() {
                            assert_eq!(edges.load(offset + i), id);
                        }
                    }
                });
            }
        });
    }

    #[test]
    fn refinement_restores_reverse_proposals_after_forward_replacements() {
        let mut graph = graph(4, 17, false);
        graph.build(1).unwrap();
        for id in 0..4u32 {
            let next = id + 1;
            graph.store_locked(
                id,
                0,
                if id < 3 {
                    std::slice::from_ref(&next)
                } else {
                    &[]
                },
            );
        }
        graph.symmetrize_refinement(2).unwrap();
        let mut neighbors = Vec::new();
        for id in 0..4u32 {
            graph.neighbors(id, 0, &mut neighbors);
            assert!(neighbors.len() <= graph.m * 2);
            if id > 0 {
                assert!(neighbors.contains(&(id - 1)));
            }
            if id < 3 {
                assert!(neighbors.contains(&(id + 1)));
            }
        }
    }

    fn temporary_directory() -> tempfile::TempDir {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        tempfile::tempdir_in("/tmp/mydb").unwrap()
    }

    #[test]
    fn sparse_visited_words_preserve_membership_across_rounds_and_boundaries() {
        let mut scratch = Scratch::new(130, 16, 4);
        for _ in 0..300 {
            scratch.begin();
            for id in [0, 63, 64, 65, 127, 129] {
                assert!(!scratch.seen(id));
                assert!(scratch.visit(id));
                assert!(scratch.seen(id));
                assert!(!scratch.visit(id));
            }
            assert_eq!(scratch.touched.len(), 3);
            assert!(!scratch.seen(128));
        }
        scratch.begin();
        assert!(scratch.visited.iter().all(|&word| word == 0));
    }

    #[test]
    fn native_int8_construction_is_lossless_including_negative_endpoint() {
        let vectors = [
            vec![-128.0, -1.0, 0.0, 1.0, 127.0],
            vec![127.0, 3.0, -2.0, -128.0, 0.0],
        ];
        for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
            let mut graph = BulkHnsw::new(2, 5, metric, 4, 16, (0.0, 1.0, 1.0), None)
                .unwrap()
                .with_native_fanout(true);
            for (id, vector) in vectors.iter().enumerate() {
                graph
                    .push(
                        RowId {
                            page_id: id as u32,
                            slot_id: 0,
                        },
                        vector,
                    )
                    .unwrap();
            }
            assert!(graph.has_distinct_native_codes().unwrap());
            graph.build(1).unwrap();
            assert_eq!(graph.code(0)[0], -128);
            let a = super::super::codec::from_build_f32(
                &vectors[0],
                crate::vector::VectorDataType::Int8,
            )
            .unwrap();
            let b = super::super::codec::from_build_f32(
                &vectors[1],
                crate::vector::VectorDataType::Int8,
            )
            .unwrap();
            let expected = super::super::distance::typed_ordering_distance(&a, &b, metric).unwrap();
            assert!((graph.distance(0, 1) - expected).abs() <= 1e-12 * expected.abs().max(1.0));
        }
    }

    #[test]
    fn native_int8_zero_cosine_uses_the_native_finite_distance() {
        let mut graph = BulkHnsw::new(2, 3, Metric::Cosine, 4, 16, (0.0, 1.0, 1.0), None)
            .unwrap()
            .with_native_fanout(true);
        for (id, row) in [[0.0, 0.0, 0.0], [1.0, 2.0, 3.0]].iter().enumerate() {
            graph
                .push(
                    RowId {
                        page_id: id as u32,
                        slot_id: 0,
                    },
                    row,
                )
                .unwrap();
        }
        assert_eq!(graph.distance(0, 0), 1.0);
        assert_eq!(graph.distance(0, 1), 1.0);
        assert_eq!(graph.distance(1, 0), 1.0);
        graph.build(1).unwrap();
        let mut scratch = Scratch::new(2, 16, 4);
        let found = graph.search(0, graph.entry, 0, &mut scratch).unwrap();
        assert_eq!(found.len(), 2);
        assert!(found.iter().all(|candidate| candidate.dist.is_finite()));
    }

    #[test]
    fn native_code_collision_declines_ungrouped_publication() {
        let mut distinct = graph(128, 17, false).with_native_fanout(false);
        assert!(distinct.has_distinct_native_codes().unwrap());
        distinct.build(1).unwrap();
        let mut duplicates = graph(128, 17, true).with_native_fanout(false);
        assert!(!duplicates.has_distinct_native_codes().unwrap());
    }

    #[test]
    fn dot_zero_pair_distance_does_not_merge_orthogonal_directions() {
        let mut graph = BulkHnsw::new(2, 3, Metric::Dot, 4, 16, (0.0, 1.0, 1.0), None)
            .unwrap()
            .with_native_fanout(true);
        graph
            .push(
                RowId {
                    page_id: 1,
                    slot_id: 0,
                },
                &[1.0, 0.0, 0.0],
            )
            .unwrap();
        graph
            .push(
                RowId {
                    page_id: 2,
                    slot_id: 0,
                },
                &[0.0, 1.0, 0.0],
            )
            .unwrap();
        let ranked = [Scored { id: 0, dist: 0.0 }, Scored { id: 1, dist: 0.0 }];
        assert_eq!(graph.select(&ranked, 2).unwrap().len(), 2);
    }

    #[test]
    fn normal_refinement_scores_need_no_original_payload_mapping() {
        let directory = temporary_directory();
        for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
            let codec = ScanCodec::from_ranges(metric, &[-1.0; 17], &[1.0; 17]).unwrap();
            let mut graph =
                BulkHnsw::new_scan(3, 17, metric, 4, 16, codec, directory.path(), true).unwrap();
            for (id, value) in [0.123, -0.456, 0.0].into_iter().enumerate() {
                let value = if metric == Metric::Cosine && value == 0.0 {
                    0.789
                } else {
                    value
                };
                graph
                    .push(
                        RowId {
                            page_id: id as u32,
                            slot_id: 0,
                        },
                        &vec![value; 17],
                    )
                    .unwrap();
            }
            graph.seal_codes().unwrap();
            graph.exact_spill.take();
            let mut scratch = Scratch::new(3, 16, 4);
            for query in 0..3u32 {
                graph.prepare_refinement_query(query, &mut scratch);
                for other in 0..3u32 {
                    let expected = graph.distance(query, other);
                    let actual = graph.refinement_distance(query, other, &scratch);
                    assert!(actual.is_finite());
                    assert!((actual - expected).abs() <= 1e-3 * (1.0 + expected.abs()));
                }
            }
        }
    }

    #[test]
    fn scan_navigation_uses_original_query_and_reprepares_between_graphs() {
        let directory = temporary_directory();
        let mut scratch = Scratch::new(3, 16, 4);
        for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
            for shift in [0.0, 0.003] {
                let codec = ScanCodec::from_ranges(metric, &[-0.9; 3], &[0.9; 3]).unwrap();
                let query = [0.123 + shift, 0.617, -0.333];
                let near = [0.126, 0.612, -0.338];
                let outlier = [1e10, 0.0, 0.0];
                let prepared = ScanQuery::new(&codec, &query).unwrap();
                let mut graph =
                    BulkHnsw::new_scan(3, 3, metric, 4, 16, codec, directory.path(), true).unwrap();
                for (id, row) in [query, near, outlier].iter().enumerate() {
                    graph
                        .push(
                            RowId {
                                page_id: id as u32,
                                slot_id: 0,
                            },
                            row,
                        )
                        .unwrap();
                }
                graph.seal_codes().unwrap();
                graph.prepare_query(0, &mut scratch);
                let code = graph.code(1);
                // SAFETY: immutable view of the live mapped byte record.
                let record =
                    unsafe { std::slice::from_raw_parts(code.as_ptr().cast(), code.len()) };
                assert_eq!(
                    graph.query_distance(0, 1, &scratch),
                    prepared.estimate(record)
                );
                assert_eq!(
                    graph.query_distance(0, 2, &scratch),
                    super::super::distance::ordering_distance(&query, &outlier, metric)
                );
                graph.prepare_query(1, &mut scratch);
                assert_eq!(scratch.query, near);
            }
        }
    }

    #[test]
    fn scan_construction_spills_preserve_exception_distances_and_cleanup() {
        let directory = temporary_directory();
        for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
            for mapped in [false, true] {
                let dim = 33;
                let lows = vec![-0.01; dim];
                let highs = vec![0.01; dim];
                let codec = ScanCodec::from_ranges(metric, &lows, &highs).unwrap();
                let mut graph =
                    BulkHnsw::new_scan(3, dim, metric, 4, 16, codec, directory.path(), mapped)
                        .unwrap();
                let rows = [vec![0.0; dim], vec![10.0; dim], vec![0.001; dim]];
                for (id, vector) in rows.iter().enumerate() {
                    graph
                        .push(
                            RowId {
                                page_id: id as u32,
                                slot_id: 0,
                            },
                            vector,
                        )
                        .unwrap();
                }
                graph.build(1).unwrap();
                // Coarse residuals (and zero cosine vectors) must bypass the
                // compact estimate, including when only one endpoint is coarse.
                assert_eq!(
                    graph.distance(1, 2),
                    super::super::distance::ordering_distance(&rows[1], &rows[2], metric)
                );
                if metric == Metric::Cosine {
                    assert!(graph.distance(0, 2).is_nan());
                }
                graph.release_codes();
                let tmp = crate::storage::naming::system_tmp_dir(directory.path());
                assert_eq!(std::fs::read_dir(tmp).unwrap().count(), 0);
            }
        }
    }

    #[test]
    fn scan_topology_retains_anisotropic_neighborhoods_in_every_metric() {
        let directory = temporary_directory();
        let rows = 512;
        let dim = 17;
        let vectors: Vec<Vec<f32>> = (0..rows)
            .map(|id| {
                (0..dim)
                    .map(|axis| {
                        (((id * (axis + 3) * 71) % 997) as f32 / 997.0 - 0.5)
                            * [100.0, 10.0, 1.0, 0.1, 0.01][axis % 5]
                    })
                    .collect()
            })
            .collect();
        for metric in [Metric::L2, Metric::Cosine, Metric::Dot] {
            let codec = ScanCodec::train(dim, metric, &vectors).unwrap();
            let mut graph =
                BulkHnsw::new_scan(rows, dim, metric, 8, 96, codec, directory.path(), true)
                    .unwrap();
            for (id, row) in vectors.iter().enumerate() {
                graph
                    .push(
                        RowId {
                            page_id: id as u32,
                            slot_id: 0,
                        },
                        row,
                    )
                    .unwrap();
            }
            graph.build(1).unwrap();
            let mut scratch = Scratch::new(rows, 96, 8);
            let mut found = 0;
            for query in (0..rows).step_by(53) {
                let mut truth: Vec<_> = (0..rows)
                    .map(|id| Scored {
                        id: id as u32,
                        dist: super::super::distance::ordering_distance(
                            &vectors[query],
                            &vectors[id],
                            metric,
                        ),
                    })
                    .collect();
                truth.sort_unstable();
                let actual = graph
                    .search(query as u32, graph.entry, 0, &mut scratch)
                    .unwrap();
                let ids: std::collections::HashSet<_> = actual.iter().map(|v| v.id).collect();
                found += truth[..10].iter().filter(|v| ids.contains(&v.id)).count();
            }
            assert!(
                found >= 90,
                "metric={metric:?} true top-10 retained={found}/100"
            );
        }
    }

    fn graph(rows: usize, dim: usize, duplicates: bool) -> BulkHnsw {
        let mut graph = BulkHnsw::new(rows, dim, Metric::L2, 8, 96, (0.0, 1.0, 1.0), None).unwrap();
        let mut seed = 17u64;
        for id in 0..rows {
            let vector: Vec<f32> = (0..dim)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    if duplicates {
                        0.0
                    } else {
                        ((seed % 201) as i32 - 100) as f32
                    }
                })
                .collect();
            graph
                .push(
                    RowId {
                        page_id: (id / 5) as u32,
                        slot_id: (id % 5) as u16,
                    },
                    &vector,
                )
                .unwrap();
        }
        graph
    }

    #[test]
    fn connectivity_repair_only_changes_component_bridge_anchors() {
        for star in [false, true] {
            let mut graph = graph(64, 3, false);
            graph.build(1).unwrap();
            graph.entry = 0;
            for id in 0..64u32 {
                let neighbors: Vec<u32> = if star && id == 0 {
                    (1..=16).collect()
                } else if star && id < 17 {
                    vec![0]
                } else {
                    let (start, count) = if star {
                        (17, 47)
                    } else if id < 32 {
                        (0, 32)
                    } else {
                        (32, 32)
                    };
                    (1..=16)
                        .map(|step| start + (id - start + step) % count)
                        .collect()
                };
                graph.store_locked(id, 0, &neighbors);
            }
            let mut before = vec![Vec::new(); 64];
            for (id, edges) in before.iter_mut().enumerate() {
                graph.neighbors(id as u32, 0, edges);
            }
            graph.repair_connectivity().unwrap();
            let mut changed = 0;
            let closest_leaf = if star {
                Some(
                    (1..=16u32)
                        .map(|id| Scored {
                            id,
                            dist: graph.distance(17, id),
                        })
                        .min()
                        .unwrap()
                        .id,
                )
            } else {
                None
            };
            let mut seen = vec![false; 64];
            let mut pending = vec![0u32];
            let mut edges = Vec::new();
            while let Some(id) = pending.pop() {
                if std::mem::replace(&mut seen[id as usize], true) {
                    continue;
                }
                graph.neighbors(id, 0, &mut edges);
                assert!(edges.len() <= 16);
                pending.extend(edges.iter().copied());
                changed += usize::from(edges != before[id as usize]);
                if let Some(closest) = closest_leaf {
                    if edges != before[id as usize] {
                        assert_eq!(id, closest, "bridge should use the nearest existing leaf");
                    }
                }
            }
            assert!(seen.into_iter().all(|reached| reached));
            assert_eq!(changed, 1, "two components require only one bridge anchor");
            graph.neighbors(0, 0, &mut edges);
            assert_eq!(edges, before[0], "the existing discovery tree must survive");
        }
    }

    #[test]
    fn bounded_topology_preserves_rows_levels_degrees_and_recall() {
        let mut graph = graph(2048, 33, false);
        graph.build(4).unwrap();
        let mut neighbors = Vec::new();
        for id in 0..graph.len() {
            assert_eq!(
                graph.row(id),
                RowId {
                    page_id: (id / 5) as u32,
                    slot_id: (id % 5) as u16
                }
            );
            assert_eq!(
                graph.level(id),
                random_level(
                    id as u64,
                    1.0 / 8f64.ln(),
                    super::super::hnsw_paged::VEC_MAX_LAYER
                )
            );
            for level in 0..=graph.level(id) {
                graph.neighbors(id as u32, level, &mut neighbors);
                assert!(neighbors.len() <= if level == 0 { 16 } else { 8 });
                assert!(neighbors
                    .iter()
                    .all(|&other| other as usize != id && graph.level(other as usize) >= level));
                let mut unique = neighbors.clone();
                unique.sort_unstable();
                unique.dedup();
                assert_eq!(unique.len(), neighbors.len());
            }
        }
        let mut scratch = Scratch::new(graph.len(), graph.ef, graph.m);
        let mut covered = 0;
        for query in (0..graph.len()).step_by(97) {
            let mut truth: Vec<_> = (0..graph.len())
                .map(|id| Scored {
                    id: id as u32,
                    dist: graph.distance(query as u32, id as u32),
                })
                .collect();
            truth.sort_unstable();
            let actual = graph
                .search(query as u32, graph.entry, 0, &mut scratch)
                .unwrap();
            covered += truth[..10]
                .iter()
                .filter(|expected| actual[..10].contains(expected))
                .count();
        }
        assert!(covered >= 190, "top-10 matches across 22 probes: {covered}");
    }

    #[test]
    fn duplicate_codes_still_publish_every_row_in_a_connected_graph() {
        let mut graph = graph(512, 13, true);
        graph.build(4).unwrap();
        let mut seen = vec![false; graph.len()];
        let mut pending = vec![graph.entry];
        let mut neighbors = Vec::new();
        while let Some(id) = pending.pop() {
            if seen[id as usize] {
                continue;
            }
            seen[id as usize] = true;
            graph.neighbors(id, 0, &mut neighbors);
            pending.extend(neighbors.iter().copied());
        }
        assert!(seen.into_iter().all(|visited| visited));
        let mut links = Vec::new();
        for id in 0..graph.len() {
            graph.copy_links(id, &mut links);
            assert!(links[0].len() <= graph.m * 2);
        }
    }

    #[test]
    fn mapped_codes_match_resident_topology_and_release_the_spill() {
        for cache_limit in [None, Some(33), Some(usize::MAX)] {
            check_mapped_topology(cache_limit);
        }
    }

    fn check_mapped_topology(cache_limit: Option<usize>) {
        let directory = temporary_directory();
        let mut resident = graph(1024, 33, false);
        let mut mapped = BulkHnsw::new(
            1024,
            33,
            Metric::L2,
            8,
            96,
            (0.0, 1.0, 1.0),
            Some(directory.path()),
        )
        .unwrap();
        if let Some(limit) = cache_limit {
            resident = resident.with_compact_links(limit);
            mapped = mapped.with_compact_links(limit);
        }
        for id in 0..resident.len() {
            let row: Vec<f32> = resident
                .code(id as u32)
                .iter()
                .map(|&code| code as f32)
                .collect();
            mapped.push(resident.row(id), &row).unwrap();
        }
        resident.build(1).unwrap();
        mapped.build(1).unwrap();
        let mut expected = Vec::new();
        let mut actual = Vec::new();
        for id in 0..resident.len() {
            assert_eq!(
                mapped.distance(id as u32, 1023),
                resident.distance(id as u32, 1023)
            );
            resident.copy_links(id, &mut expected);
            mapped.copy_links(id, &mut actual);
            assert_eq!(actual, expected);
        }
        mapped.release_codes();
        assert_eq!(
            std::fs::read_dir(crate::storage::naming::system_tmp_dir(directory.path()))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn construction_inherits_statement_cancellation() {
        let mut graph = graph(256, 17, false);
        let _deadline =
            crate::engine::cancel::install_statement_deadline(std::time::Duration::from_millis(1));
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert_eq!(graph.build(4).unwrap_err().code(), 3024);
    }

    #[test]
    fn cancelled_scan_construction_releases_both_spill_owners() {
        let directory = temporary_directory();
        let codec = ScanCodec::from_ranges(Metric::L2, &[-1.0; 17], &[1.0; 17]).unwrap();
        let mut graph =
            BulkHnsw::new_scan(64, 17, Metric::L2, 4, 16, codec, directory.path(), true).unwrap();
        for id in 0..64 {
            graph
                .push(
                    RowId {
                        page_id: id,
                        slot_id: 0,
                    },
                    &[0.5; 17],
                )
                .unwrap();
        }
        let _deadline =
            crate::engine::cancel::install_statement_deadline(std::time::Duration::from_millis(1));
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert_eq!(graph.build(2).unwrap_err().code(), 3024);
        drop(graph);
        let tmp = crate::storage::naming::system_tmp_dir(directory.path());
        assert_eq!(std::fs::read_dir(tmp).unwrap().count(), 0);
    }

    #[test]
    fn mapped_construction_budget_counts_resident_topology_separately() {
        let rows = 1_000_000;
        let stride = 768 + super::super::scan_codes::RECORD_HEADER;
        let total = BulkHnsw::estimated_bytes(rows, stride, 16, 16, 64).unwrap();
        let resident = total - rows * stride;
        let budget = 640 * 1024 * 1024;
        assert!(total > budget);
        assert!(resident < budget);
        assert!(resident > rows * 16 * 3 * 4);
    }

    #[test]
    fn compact_reverse_storage_keeps_degrees_recall_and_reservation() {
        let rows = 2048;
        let mut graph = graph(rows, 33, false).with_compact_links(usize::MAX);
        graph.build(4).unwrap();
        let mut links = Vec::new();
        for id in 0..rows {
            graph.copy_links(id, &mut links);
            for (level, edges) in links.iter().enumerate() {
                assert!(edges.len() <= if level == 0 { 16 } else { 8 });
                assert!(edges
                    .iter()
                    .all(|&n| n as usize != id && graph.level(n as usize) >= level));
            }
        }
        let mut covered = 0;
        let mut scratch = Scratch::new(rows, graph.ef, graph.m);
        for query in (0..rows).step_by(97) {
            let mut truth: Vec<_> = (0..rows)
                .map(|id| Scored {
                    id: id as u32,
                    dist: graph.distance(query as u32, id as u32),
                })
                .collect();
            truth.sort_unstable();
            let actual = graph
                .search(query as u32, graph.entry, 0, &mut scratch)
                .unwrap();
            covered += truth[..10]
                .iter()
                .filter(|v| actual[..10].contains(v))
                .count();
        }
        assert!(
            covered >= 190,
            "compact top-10 matches across 22 probes: {covered}"
        );
        let compact = BulkHnsw::estimated_compact_bytes(rows, 33, 8, 4, 96, usize::MAX).unwrap();
        let slack = BulkHnsw::estimated_bytes(rows, 33, 8, 4, 96).unwrap();
        assert!(compact < slack);
        let entry = graph.entry();
        let row = graph.row(1);
        let level = graph.level(1);
        assert!(
            graph.release_link_storage()
                > ConstructionEdges::bytes_for(rows * 8 * 2, rows).unwrap()
        );
        assert_eq!(graph.entry(), entry);
        assert_eq!(graph.row(1), row);
        assert_eq!(graph.level(1), level);
    }

    #[test]
    fn navigation_and_diversity_observe_cancellation_inside_work_slices() {
        let mut graph = graph(256, 17, false);
        graph.build(1).unwrap();
        let mut scratch = Scratch::new(graph.len(), graph.ef, graph.m);
        let _deadline =
            crate::engine::cancel::install_statement_deadline(std::time::Duration::from_millis(1));
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert_eq!(
            graph
                .greedy(0, graph.entry, 0, &mut scratch)
                .unwrap_err()
                .code(),
            3024
        );
        assert_eq!(
            graph
                .search(0, graph.entry, 0, &mut scratch)
                .unwrap_err()
                .code(),
            3024
        );
        let ranked = [Scored { id: 0, dist: 0.0 }, Scored { id: 1, dist: 1.0 }];
        assert_eq!(graph.select(&ranked, 2).unwrap_err().code(), 3024);
        assert_eq!(graph.refine_neighborhoods(2).unwrap_err().code(), 3024);
        assert_eq!(graph.symmetrize_refinement(2).unwrap_err().code(), 3024);
    }

    #[test]
    fn reservation_covers_dense_storage_and_rejects_overflow() {
        let rows = 4096;
        let mut graph = graph(rows, 1, true);
        graph.build(1).unwrap();
        let bytes = BulkHnsw::estimated_bytes(rows, 1, 8, 1, 96).unwrap();
        let actual = graph.codes.capacity()
            + graph.rows.capacity() * std::mem::size_of::<RowId>()
            + graph.norms.capacity() * 4
            + graph.levels.capacity()
            + graph.offsets.capacity() * 4
            + graph.edges.allocated_bytes()
            + graph.counts.capacity() * 2
            + graph.locks.capacity() * std::mem::size_of::<Mutex<()>>();
        assert!(actual < bytes, "allocated={actual}, reserved={bytes}");
        assert!(BulkHnsw::estimated_bytes(usize::MAX, 768, 16, 16, 64).is_none());
    }
}
