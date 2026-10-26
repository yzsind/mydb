// Direct read-only navigation over the authoritative .hvec image. No derived
// codes/edges file, dense directory, or startup scan is required. Writers revoke
// the view before publishing a page mutation; existing readers drain first.

#[derive(Debug)]
pub(crate) struct MappedVectorNavigation {
    _mapping: crate::platform::mapping::FileMapping,
    address: *mut libc::c_void,
    len: usize,
    meta: VectorMeta,
    verified: Vec<AtomicU64>,
    cache: crate::engine::vector::hnsw_paged::QueryNavCache,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

// SAFETY: readers borrow this object under VectorIndexFile::mapped_navigation's
// read lock. Every local mutator revokes that mapping before changing any page.
// Physical replicas, whose page writer bypasses VectorIndexFile, disable it.
unsafe impl Send for MappedVectorNavigation {}
unsafe impl Sync for MappedVectorNavigation {}

struct VectorNavigationWrite<'a>(&'a VectorIndexFile);
impl Drop for VectorNavigationWrite<'_> {
    fn drop(&mut self) {
        self.0.navigation_revision.fetch_add(1, Ordering::Release);
        self.0.navigation_writers.fetch_sub(1, Ordering::Release);
    }
}

impl VectorIndexFile {
    fn navigation_write(&self) -> VectorNavigationWrite<'_> {
        self.navigation_writers.fetch_add(1, Ordering::AcqRel);
        self.navigation_revision.fetch_add(1, Ordering::AcqRel);
        // Once revoked, parallel DML only needs a shared observation. The
        // active-writer count prevents a concurrent prepare from publishing.
        let mapped = self.mapped_navigation.read().is_some();
        if mapped {
            self.mapped_navigation.write().take();
        }
        VectorNavigationWrite(self)
    }

    pub(crate) fn disable_navigation_mapping(&self) {
        self.mapping_allowed.store(false, Ordering::Release);
        self.mapped_navigation.write().take();
    }

    pub(crate) fn with_navigation_mapping<T>(
        &self,
        read: impl FnOnce(&MappedVectorNavigation) -> T,
    ) -> Option<T> {
        self.mapped_navigation.read().as_ref().map(read)
    }

    pub(crate) fn prepare_navigation_mapping(&self) -> Result<bool, QueryError> {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        if !self.mapping_allowed.load(Ordering::Acquire)
            || self.durability == DurabilityMode::Memory
            || matches!(
                std::env::var("MYDB_HNSW_MAPPED_NAVIGATION").as_deref(),
                Ok("0")
            )
        {
            return Ok(false);
        }
        if self.navigation_writers.load(Ordering::Acquire) == 0
            && self.mapped_navigation.read().is_some()
        {
            return Ok(true);
        }
        let revision = self.navigation_revision.load(Ordering::Acquire);
        let meta = self.meta();
        if !meta.finalized || meta.node_count == 0 {
            return Ok(false);
        }
        let mut view = self.mapped_navigation.write();
        // A role transition may have disabled mapping while we waited here.
        if !self.mapping_allowed.load(Ordering::Acquire) {
            return Ok(false);
        }
        if view.is_some() {
            return Ok(true);
        }
        if self.navigation_writers.load(Ordering::Acquire) != 0
            || self.navigation_revision.load(Ordering::Acquire) != revision
        {
            return Ok(false);
        }
        // Recovery and previous DML may still have dirty frames. They must reach
        // the source file before a direct reader can observe it. This is a dirty
        // frame flush, not a full index scan or a new persistent index format.
        self.flush()?;
        let file = self.file.lock();
        let Some(file) = file.as_ref() else {
            return Ok(false);
        };
        let len = usize::try_from(file.metadata().map_err(io_err)?.len()).map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "vector mapping exceeds address space",
            )
        })?;
        if len > isize::MAX as usize || len / PAGE_SIZE < meta.node_next_page_id as usize {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "truncated vector navigation file",
            ));
        }
        // An interleaved f32/SQ8 image has a wider random-I/O working set
        // than the compact navigator. Preserve that path for indexes larger
        // than the configured working-set target instead of forcing mmap.
        if len > crate::engine::executor::mem_budget::memory_target_bytes() {
            return Ok(false);
        }
        let words = (len / PAGE_SIZE).div_ceil(64);
        // A lazy checksum bitmap plus the same bounded exact-query memo used
        // by the compact path. File-backed clean pages remain OS-reclaimable.
        let bytes =
            words.saturating_mul(8).saturating_add(
                512usize.saturating_mul(
                    native_geometry(meta.dim as usize)
                        .map_or(meta.dim as usize * 4, |(datatype, dim)| {
                            datatype.payload_len(dim)
                        })
                        + 128 * 16
                        + 128,
                ),
            );
        let Some(charge) =
            ControlledMemoryCharge::try_new_optional(ControlledMemoryCategory::DerivedCache, bytes)
        else {
            return Ok(false);
        };
        // SAFETY: all local writers revoke this view before modifying the
        // authoritative file; physical replicas disable this mapping.
        let image =
            match unsafe { crate::platform::mapping::FileMapping::open(file, 0, len, false) } {
                Ok(image) => image,
                Err(_) => return Ok(false),
            };
        let mapping = MappedVectorNavigation {
            address: image.as_ptr().cast(),
            _mapping: image,
            len,
            meta,
            verified: (0..words).map(|_| AtomicU64::new(0)).collect(),
            cache: crate::engine::vector::hnsw_paged::QueryNavCache::new(),
            _charge: charge,
        };
        if self.navigation_writers.load(Ordering::Acquire) != 0
            || self.navigation_revision.load(Ordering::Acquire) != revision
        {
            return Ok(false);
        }
        *view = Some(mapping);
        Ok(true)
    }
}

impl MappedVectorNavigation {
    fn page(&self, vid: VecId) -> Result<&[u8], QueryError> {
        let page_id = vid_page(vid) as usize;
        if page_id == 0 || page_id >= self.meta.node_next_page_id as usize {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "vector navigation page out of range",
            ));
        }
        // SAFETY: bounds were checked against the mapped file length at open.
        let page = unsafe {
            std::slice::from_raw_parts(
                self.address.cast::<u8>().add(page_id * PAGE_SIZE),
                PAGE_SIZE,
            )
        };
        let word = &self.verified[page_id / 64];
        let mask = 1u64 << (page_id % 64);
        if word.load(Ordering::Relaxed) & mask == 0 {
            if read_u32(page, 0) != VEC_NODE_MAGIC
                || read_u16(page, 4) != VEC_PAGE_VERSION
                || read_u32(page, 16) != page_checksum(page, 16)
                || NODE_OFF_DIRECTORY + read_u16(page, NODE_OFF_SLOT_COUNT) as usize * 2 > PAGE_SIZE
            {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "corrupt mapped vector navigation page",
                ));
            }
            word.fetch_or(mask, Ordering::Relaxed);
        }
        Ok(page)
    }

    fn record(&self, vid: VecId) -> Result<(&[u8], usize), QueryError> {
        let page = self.page(vid)?;
        let offset = node_record_offset(page, vid_slot(vid))?;
        let level = page[offset + 1] as usize;
        if level > 16
            || offset
                + node_record_size(
                    level,
                    self.meta.dim as usize,
                    self.meta.m as usize,
                    self.meta.m0 as usize,
                )
                > PAGE_SIZE
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt mapped vector record",
            ));
        }
        Ok((page, offset))
    }

    fn neighbors(&self, vid: VecId, level: usize, out: &mut Vec<VecId>) -> Result<(), QueryError> {
        let (page, offset) = self.record(vid)?;
        out.clear();
        if page[offset] & REC_FLAG_INITIALIZED == 0 || level > page[offset + 1] as usize {
            return Ok(());
        }
        let count_off = layer_count_offset(
            offset,
            level,
            self.meta.dim as usize,
            self.meta.m as usize,
            self.meta.m0 as usize,
        );
        let count = read_u16(page, count_off) as usize;
        if count > layer_cap(level, self.meta.m as usize, self.meta.m0 as usize) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt mapped vector adjacency",
            ));
        }
        out.extend((0..count).map(|i| node_id_to_vid(read_u32(page, count_off + 2 + i * 4))));
        Ok(())
    }

    #[inline]
    fn exact_row(&self, vid: VecId, dimension: usize) -> Result<(RowId, bool, &[u8]), QueryError> {
        let (page, offset) = self.record(vid)?;
        let start = offset + 10;
        Ok((
            vec_unpack_row(read_u64(page, offset + 2)),
            page[offset] & REC_FLAG_TOMBSTONE != 0,
            &page[start..start + dimension * 4],
        ))
    }

    fn exact(
        &self,
        vid: VecId,
        query: &crate::engine::vector::distance::PreparedF32Distance<'_>,
    ) -> Result<(RowId, bool, f64), QueryError> {
        let (row, deleted, bytes) = self.exact_row(vid, query.dimension())?;
        Ok((row, deleted, query.score_bytes(bytes)))
    }

    fn exact_batch4(
        &self,
        vids: &[VecId],
        query: &crate::engine::vector::distance::PreparedF32Distance<'_>,
    ) -> Result<[(RowId, bool, f64); 4], QueryError> {
        let rows = [
            self.exact_row(vids[0], query.dimension())?,
            self.exact_row(vids[1], query.dimension())?,
            self.exact_row(vids[2], query.dimension())?,
            self.exact_row(vids[3], query.dimension())?,
        ];
        let scores = query.score_batch4(rows.map(|row| row.2));
        Ok(std::array::from_fn(|j| (rows[j].0, rows[j].1, scores[j])))
    }

    #[inline]
    fn codes(&self, vid: VecId) -> Result<Option<(&[i8], i32)>, QueryError> {
        let (page, offset) = self.record(vid)?;
        if page[offset] & REC_FLAG_INITIALIZED == 0 || page[offset] & REC_FLAG_EXACT_NAV != 0 {
            return Ok(None);
        }
        let dim = self.meta.dim as usize;
        let start = offset + 10 + dim * 4;
        let bytes = &page[start..start + dim];
        // SAFETY: i8/u8 have identical layout and the borrow is bounded by the view.
        let codes = unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<i8>(), dim) };
        Ok(Some((codes, read_u32(page, start + dim) as i32)))
    }

    #[inline]
    fn distance(
        &self,
        vid: VecId,
        query: &crate::engine::vector::distance::PreparedF32Distance<'_>,
        codes: &[i8],
        norm: i32,
    ) -> Result<f64, QueryError> {
        if !codes.is_empty() {
            if let Some((row, row_norm)) = self.codes(vid)? {
                return Ok((norm + row_norm
                    - 2 * crate::engine::vector::distance::dot_i8(codes, row))
                    as f64
                    * (self.meta.nav_dist_mul as f64));
            }
        }
        Ok(self.exact(vid, query)?.2)
    }

    fn distances(
        &self,
        vids: &[VecId],
        query: &crate::engine::vector::distance::PreparedF32Distance<'_>,
        codes: &[i8],
        norm: i32,
        out: &mut Vec<f64>,
    ) -> Result<(), QueryError> {
        use crate::engine::vector::distance::{dot_i8_batch4_ptrs, prefetch_read};
        out.clear();
        // Bring record headers into cache before dereferencing their SQ8 rows.
        for &vid in vids {
            let page = vid_page(vid) as usize;
            if page < self.len / PAGE_SIZE {
                // SAFETY: prefetch targets an address inside the live mapping.
                unsafe {
                    prefetch_read(self.address.cast::<i8>().add(page * PAGE_SIZE));
                }
            }
        }
        let mut chunks = vids.chunks_exact(4);
        for chunk in &mut chunks {
            if codes.is_empty() {
                for (_, _, score) in self.exact_batch4(chunk, query)? {
                    out.push(score);
                }
                continue;
            }
            if !codes.is_empty() {
                let rows = [
                    self.codes(chunk[0])?,
                    self.codes(chunk[1])?,
                    self.codes(chunk[2])?,
                    self.codes(chunk[3])?,
                ];
                if let [Some(a), Some(b), Some(c), Some(d)] = rows {
                    let dots = dot_i8_batch4_ptrs(codes, [a.0, b.0, c.0, d.0]);
                    for (row, dot) in [a, b, c, d].iter().zip(dots) {
                        out.push((norm + row.1 - 2 * dot) as f64 * self.meta.nav_dist_mul as f64);
                    }
                    continue;
                }
                if rows.iter().all(Option::is_none) {
                    for (_, _, score) in self.exact_batch4(chunk, query)? {
                        out.push(score);
                    }
                    continue;
                }
            }
            for &vid in chunk {
                out.push(self.distance(vid, query, codes, norm)?);
            }
        }
        for &vid in chunks.remainder() {
            out.push(self.distance(vid, query, codes, norm)?);
        }
        Ok(())
    }

    pub(crate) fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
    ) -> Result<Vec<(RowId, f64)>, QueryError> {
        use crate::engine::vector::distance::{dot_i8, quantize_into, Metric};
        use crate::engine::vector::hnsw_core::Scored;
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        if query.len() != self.meta.dim as usize {
            return Err(error(
                crate::error::catalog::MYDB_ER_VECTOR_DIMENSION_MISMATCH,
                "vector mapping query dimension mismatch",
            ));
        }
        if let Some(result) = self.cache.get(query, k, ef) {
            return Ok(result);
        }
        let metric = match self.meta.metric {
            1 => Metric::Cosine,
            2 => Metric::Dot,
            _ => Metric::L2,
        };
        let mut normalized = Vec::new();
        let src = if metric == Metric::Cosine {
            normalized.extend_from_slice(query);
            crate::engine::vector::distance::normalize_in_place(&mut normalized);
            normalized.as_slice()
        } else {
            query
        };
        let mut codes = Vec::new();
        if self.meta.nav_quant_valid
            && src.iter().all(|v| {
                v.is_finite() && ((v - self.meta.nav_mid) * self.meta.nav_inv_scale).abs() <= 127.5
            })
        {
            codes.resize(query.len(), 0);
            quantize_into(src, self.meta.nav_mid, self.meta.nav_inv_scale, &mut codes);
        }
        let norm = dot_i8(&codes, &codes);
        let prepared = crate::engine::vector::distance::PreparedF32Distance::new(query, metric);
        let distance = |vid| self.distance(vid, &prepared, &codes, norm);
        let mut current = self.meta.entry_vid;
        if current == VEC_NO_VID || k == 0 {
            return Ok(Vec::new());
        }
        let mut neighbors = Vec::with_capacity(self.meta.m0 as usize);
        for level in (1..=self.meta.max_layer as usize).rev() {
            let mut best = distance(current)?;
            loop {
                let previous = current;
                self.neighbors(current, level, &mut neighbors)?;
                for &vid in &neighbors {
                    let score = distance(vid)?;
                    if score < best {
                        best = score;
                        current = vid;
                    }
                }
                if previous == current {
                    break;
                }
            }
        }
        let slots = ((PAGE_SIZE - NODE_OFF_DIRECTORY)
            / (node_record_size(0, query.len(), self.meta.m as usize, self.meta.m0 as usize) + 2))
            .min(256)
            .max(1);
        let bits = (self.meta.node_next_page_id as usize).saturating_mul(slots);
        // Bound the bitmap for sparse/extreme layouts; normal packed pages
        // need approximately one bit per node without constructing a directory.
        let bitmap_bytes = if bits <= 16 * 1024 * 1024 * 8 {
            bits.div_ceil(64) * 8
        } else {
            0
        };
        let _bitmap_charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
            bitmap_bytes,
        )?;
        let mut bitmap = if bits <= 16 * 1024 * 1024 * 8 {
            vec![0u64; bits.div_ceil(64)]
        } else {
            Vec::new()
        };
        let mut sparse = hashbrown::HashSet::new();
        let mut visit = |vid: VecId| -> Result<bool, QueryError> {
            if bitmap.is_empty() {
                return Ok(sparse.insert(vid));
            }
            let slot = vid_slot(vid) as usize;
            let page = vid_page(vid) as usize;
            if slot >= slots || page >= self.meta.node_next_page_id as usize {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid mapped navigation edge",
                ));
            }
            let bit = page * slots + slot;
            let word = &mut bitmap[bit / 64];
            let mask = 1 << (bit % 64);
            let fresh = *word & mask == 0;
            *word |= mask;
            Ok(fresh)
        };
        let mut fresh = Vec::with_capacity(self.meta.m0 as usize);
        let mut distances = Vec::with_capacity(self.meta.m0 as usize);
        let mut frontier = BinaryHeap::new();
        let mut results = BinaryHeap::new();
        let first = Scored {
            dist: distance(current)?,
            id: current,
        };
        frontier.push(Reverse(first));
        results.push(first);
        visit(current)?;
        while let Some(Reverse(candidate)) = frontier.pop() {
            let furthest = results
                .peek()
                .map(|s: &Scored<VecId>| s.dist)
                .unwrap_or(f64::INFINITY);
            if results.len() >= ef && candidate.dist > furthest {
                break;
            }
            self.neighbors(candidate.id, 0, &mut neighbors)?;
            fresh.clear();
            for &vid in &neighbors {
                if visit(vid)? {
                    fresh.push(vid);
                }
            }
            self.distances(&fresh, &prepared, &codes, norm, &mut distances)?;
            for (&vid, &score) in fresh.iter().zip(&distances) {
                let furthest = results
                    .peek()
                    .map(|s: &Scored<VecId>| s.dist)
                    .unwrap_or(f64::INFINITY);
                if results.len() < ef || score < furthest {
                    let entry = Scored {
                        dist: score,
                        id: vid,
                    };
                    frontier.push(Reverse(entry));
                    if results.len() == ef {
                        results.pop();
                    }
                    results.push(entry);
                }
            }
        }
        let candidates = results.into_vec();
        let mut out = Vec::with_capacity(candidates.len());
        let mut chunks = candidates.chunks_exact(4);
        for chunk in &mut chunks {
            let ids = [chunk[0].id, chunk[1].id, chunk[2].id, chunk[3].id];
            for (row, deleted, distance) in self.exact_batch4(&ids, &prepared)? {
                if !deleted {
                    out.push((row, distance));
                }
            }
        }
        for candidate in chunks.remainder() {
            let (row, deleted, distance) = self.exact(candidate.id, &prepared)?;
            if !deleted {
                out.push((row, distance));
            }
        }
        out.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        out.truncate(k);
        self.cache.insert(query, k, ef, &out);
        Ok(out)
    }
}
