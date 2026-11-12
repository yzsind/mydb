// Direct read-only navigation over the authoritative .hvec image. No derived
// codes/edges file or startup scan is required. A bounded optional location
// memo is filled only by validated accesses. Writers revoke the view before
// publishing a page mutation; existing readers drain first.

#[derive(Debug)]
pub(crate) struct MappedVectorNavigation {
    _mapping: crate::platform::mapping::FileMapping,
    address: *mut libc::c_void,
    len: usize,
    meta: VectorMeta,
    source_revision: u64,
    verified: Vec<AtomicU64>,
    records: Option<MappedRecordCache>,
    norms: Option<MappedNormCache>,
    cache: crate::engine::vector::hnsw_paged::QueryNavCache,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

/// A mapping-generation-local memo of validated record locations. Zero is
/// not a valid record offset, so untouched slots need no separate bitmap.
/// Admission is optional and never scans the index or validates other rows.
#[derive(Debug)]
struct MappedRecordCache {
    slots_per_page: usize,
    offsets: Vec<std::sync::atomic::AtomicU16>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl MappedRecordCache {
    fn try_new(pages: usize, record_bytes: usize, limit: usize) -> Option<Self> {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        let slots_per_page = ((PAGE_SIZE - NODE_OFF_DIRECTORY) / record_bytes.checked_add(2)?)
            .min(usize::from(NODE_ID_SLOTS_PER_PAGE));
        if slots_per_page == 0 {
            return None;
        }
        let count = pages.checked_mul(slots_per_page)?;
        let bytes = count
            .checked_mul(std::mem::size_of::<std::sync::atomic::AtomicU16>())?
            .checked_add(std::mem::size_of::<Self>())?;
        if bytes > limit {
            return None;
        }
        // Do not reclaim caches here: preparation holds the mapping write
        // guard, and reclamation could re-enter the same index.
        let mut charge = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::DerivedCache,
            bytes,
        )?;
        let mut offsets = Vec::new();
        offsets.try_reserve_exact(count).ok()?;
        let allocated = offsets
            .capacity()
            .checked_mul(std::mem::size_of::<std::sync::atomic::AtomicU16>())?
            .checked_add(std::mem::size_of::<Self>())?;
        if allocated > bytes && (allocated > limit || !charge.try_grow_optional(allocated - bytes))
        {
            return None;
        }
        offsets.extend((0..count).map(|_| std::sync::atomic::AtomicU16::new(0)));
        Some(Self {
            slots_per_page,
            offsets,
            _charge: charge,
        })
    }

    #[inline]
    fn position(&self, vid: VecId) -> Option<usize> {
        let slot = usize::from(vid_slot(vid));
        if slot >= self.slots_per_page {
            return None;
        }
        let index = usize::try_from(vid_page(vid))
            .ok()?
            .checked_mul(self.slots_per_page)?
            .checked_add(slot)?;
        (index < self.offsets.len()).then_some(index)
    }

    #[inline]
    fn slot(&self, vid: VecId) -> Option<&std::sync::atomic::AtomicU16> {
        self.position(vid).map(|index| &self.offsets[index])
    }

    #[inline]
    fn get(&self, vid: VecId) -> Option<usize> {
        let offset = self.slot(vid)?.load(Ordering::Acquire);
        (offset != 0).then_some(usize::from(offset))
    }

    #[inline]
    fn remember(&self, vid: VecId, offset: usize) {
        debug_assert!(vid_page(vid) != 0 && offset != 0 && offset < PAGE_SIZE);
        if let Some(slot) = self.slot(vid) {
            // The complete page and record validation precedes publication.
            // Racing readers publish the same immutable record location.
            slot.store(offset as u16, Ordering::Release);
        }
    }
}

/// Optional row-derived scalars share the mapping generation, not a query
/// key. Zero means unknown; exceptional norms retain the exact wide scorer.
#[derive(Debug)]
struct MappedNormCache {
    values: Vec<AtomicU32>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl MappedNormCache {
    fn try_new(count: usize, limit: usize) -> Option<Self> {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        let bytes = count
            .checked_mul(std::mem::size_of::<AtomicU32>())?
            .checked_add(std::mem::size_of::<Self>())?;
        if bytes > limit {
            return None;
        }
        // No reclamation while the mapping publication guard is held.
        let mut charge = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::DerivedCache,
            bytes,
        )?;
        let mut values = Vec::new();
        values.try_reserve_exact(count).ok()?;
        let allocated = values
            .capacity()
            .checked_mul(std::mem::size_of::<AtomicU32>())?
            .checked_add(std::mem::size_of::<Self>())?;
        if allocated > bytes && (allocated > limit || !charge.try_grow_optional(allocated - bytes))
        {
            return None;
        }
        values.extend((0..count).map(|_| AtomicU32::new(0)));
        Some(Self {
            values,
            _charge: charge,
        })
    }
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
    /// Optional cache reclamation must not wait for a reader that may itself
    /// be requesting memory admission while holding the mapping read guard.
    pub(crate) fn evict_navigation_mapping(&self) -> bool {
        self.mapped_navigation
            .try_write()
            .is_some_and(|mut view| view.take().is_some())
    }

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
        // Grow the FLOAT32 query memo under the native path's bounded budget.
        // A larger optional memo must never prevent the mapping's admission:
        // retain the compact path's small cache when its reservation is denied.
        use crate::engine::vector::hnsw_paged::{
            QueryNavCache, QUERY_CACHE_ENTRIES, QUERY_CACHE_MAX_K,
        };
        let geometry = native_geometry(meta.dim as usize);
        let entry_bytes = geometry.map_or(meta.dim as usize * 4, |(datatype, dim)| {
            datatype.payload_len(dim)
        }) + QUERY_CACHE_MAX_K * std::mem::size_of::<(RowId, f64)>()
            + 128;
        let (entries, cache_bytes) = geometry
            .is_none()
            .then(|| {
                QueryNavCache::allocation(
                    meta.dim as usize,
                    QUERY_CACHE_MAX_K,
                    crate::engine::executor::mem_budget::memory_target_bytes() / 4,
                )
            })
            .flatten()
            .filter(|(entries, _)| *entries > QUERY_CACHE_ENTRIES)
            .unwrap_or((QUERY_CACHE_ENTRIES, (QUERY_CACHE_ENTRIES + 1) * entry_bytes));
        let reserve = |bytes| {
            ControlledMemoryCharge::try_new_optional(
                ControlledMemoryCategory::DerivedCache,
                words.saturating_mul(8).saturating_add(bytes),
            )
        };
        let reservation = reserve(cache_bytes)
            .map(|charge| (entries, charge))
            .or_else(|| {
                (entries > QUERY_CACHE_ENTRIES)
                    .then(|| reserve((QUERY_CACHE_ENTRIES + 1) * entry_bytes))
                    .flatten()
                    .map(|charge| (QUERY_CACHE_ENTRIES, charge))
            });
        let Some((cache_entries, charge)) = reservation else {
            return Ok(false);
        };
        // SAFETY: all local writers revoke this view before modifying the
        // authoritative file; physical replicas disable this mapping.
        let image =
            match unsafe { crate::platform::mapping::FileMapping::open(file, 0, len, false) } {
                Ok(image) => image,
                Err(_) => return Ok(false),
            };
        let records = MappedRecordCache::try_new(
            meta.node_next_page_id as usize,
            node_record_size(0, meta.dim as usize, meta.m as usize, meta.m0 as usize),
            (crate::engine::executor::mem_budget::memory_target_bytes() / 256)
                .min(64 * 1024 * 1024),
        );
        let norms = records
            .as_ref()
            .filter(|_| {
                cfg!(target_endian = "little")
                    && meta.metric == 1
                    && native_geometry(meta.dim as usize).is_none()
                    && meta.dim >= 64
            })
            .and_then(|records| {
                MappedNormCache::try_new(
                    records.offsets.len(),
                    (crate::engine::executor::mem_budget::memory_target_bytes() / 256)
                        .min(64 * 1024 * 1024),
                )
            });
        let mapping = MappedVectorNavigation {
            address: image.as_ptr().cast(),
            _mapping: image,
            len,
            meta,
            source_revision: revision,
            verified: (0..words).map(|_| AtomicU64::new(0)).collect(),
            records,
            norms,
            cache: QueryNavCache::with_capacity(QUERY_CACHE_MAX_K, cache_entries),
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
    pub(crate) fn source_revision(&self) -> u64 {
        self.source_revision
    }

    pub(crate) fn sq8_cursor_geometry(&self) -> Option<(usize, u64)> {
        (self.meta.nav_quant_valid
            && self.meta.nav_exact_nodes == 0
            && native_geometry(self.meta.dim as usize).is_none())
        .then_some((self.meta.dim as usize, self.meta.node_count))
    }

    /// Speculative reads must not validate an unrequested node or dereference
    /// an unchecked directory offset. Cache hits already prove these bounds;
    /// a miss only prefetches the in-file page header for ordinary validation.
    #[inline]
    pub(crate) fn prefetch_navigation_node(&self, vid: VecId) {
        use crate::engine::vector::distance::prefetch_read;
        let page = vid_page(vid) as usize;
        if page == 0 || page >= self.len / PAGE_SIZE {
            return;
        }
        // SAFETY: the file bounds above cover the complete page. A remembered
        // record belongs to this immutable mapping and was fully validated.
        unsafe {
            let address = self.address.cast::<i8>().add(page * PAGE_SIZE);
            if let Some(offset) = self.records.as_ref().and_then(|cache| cache.get(vid)) {
                prefetch_read(address.add(offset));
                if self.sq8_cursor_geometry().is_some() {
                    let dimension = self.meta.dim as usize;
                    let codes = offset + 10 + dimension * 4;
                    for position in (0..dimension).step_by(256) {
                        prefetch_read(address.add(codes + position));
                    }
                    prefetch_read(address.add(codes + dimension));
                }
            } else {
                prefetch_read(address);
            }
        }
    }

    /// Called only while VectorIndexFile retains the mapping read guard.
    pub(crate) fn navigation_node(
        &self,
        vid: VecId,
        query_codes: &[i8],
        query_norm: i32,
    ) -> Result<Option<(RowId, bool, f64)>, QueryError> {
        let (page, offset) = self.record(vid)?;
        if page[offset] & REC_FLAG_INITIALIZED == 0 {
            return Ok(None);
        }
        let Some((codes, norm)) = self.codes_in_record(page, offset) else {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "inconsistent mapped SQ8 navigation metadata",
            ));
        };
        let distance = (query_norm + norm
            - 2 * crate::engine::vector::distance::dot_i8(query_codes, codes))
            as f64
            * (self.meta.nav_dist_mul as f64);
        Ok(Some((
            vec_unpack_row(read_u64(page, offset + 2)),
            page[offset] & REC_FLAG_TOMBSTONE != 0,
            distance,
        )))
    }

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
        if let Some(offset) = self.records.as_ref().and_then(|cache| cache.get(vid)) {
            // SAFETY: this location was published only after page/slot/record
            // bounds validation on this immutable mapping. Its source cannot
            // change until all mapping readers drain and the memo is retired.
            let page = unsafe {
                std::slice::from_raw_parts(
                    self.address
                        .cast::<u8>()
                        .add(vid_page(vid) as usize * PAGE_SIZE),
                    PAGE_SIZE,
                )
            };
            return Ok((page, offset));
        }
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
        if let Some(cache) = &self.records {
            cache.remember(vid, offset);
        }
        Ok((page, offset))
    }

    /// Borrow a native payload while the caller retains the mapping read guard.
    pub(crate) fn native_payload(&self, vid: VecId) -> Result<&[u8], QueryError> {
        let (datatype, dimension) = native_geometry(self.meta.dim as usize)
            .ok_or_else(crate::engine::vector::persistence::corrupt)?;
        let (page, offset) = self.record(vid)?;
        let start = offset + 18;
        Ok(&page[start..start + datatype.payload_len(dimension)])
    }

    #[inline]
    pub(crate) fn neighbors(
        &self,
        vid: VecId,
        level: usize,
        out: &mut Vec<VecId>,
    ) -> Result<(), QueryError> {
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

    #[inline]
    fn exact_payload(&self, vid: VecId, dimension: usize) -> Result<&[u8], QueryError> {
        let (page, offset) = self.record(vid)?;
        let start = offset + 10;
        Ok(&page[start..start + dimension * 4])
    }

    fn exact_distances4(
        &self,
        vids: &[VecId],
        query: &crate::engine::vector::distance::PreparedF32Distance<'_>,
    ) -> Result<[f64; 4], QueryError> {
        // Navigation needs only distances. Row identity and tombstones are
        // still read when publishing the final exact candidate pool.
        let rows = [
            self.exact_payload(vids[0], query.dimension())?,
            self.exact_payload(vids[1], query.dimension())?,
            self.exact_payload(vids[2], query.dimension())?,
            self.exact_payload(vids[3], query.dimension())?,
        ];
        Ok(query.score_batch4(rows))
    }

    fn exact_distances8(
        &self,
        vids: &[VecId],
        query: &crate::engine::vector::distance::PreparedF32Distance<'_>,
    ) -> Result<[f64; 8], QueryError> {
        let rows = [
            self.exact_payload(vids[0], query.dimension())?,
            self.exact_payload(vids[1], query.dimension())?,
            self.exact_payload(vids[2], query.dimension())?,
            self.exact_payload(vids[3], query.dimension())?,
            self.exact_payload(vids[4], query.dimension())?,
            self.exact_payload(vids[5], query.dimension())?,
            self.exact_payload(vids[6], query.dimension())?,
            self.exact_payload(vids[7], query.dimension())?,
        ];
        let cells = std::array::from_fn::<_, 8, _>(|j| {
            self.records
                .as_ref()
                .and_then(|records| records.position(vids[j]))
                .and_then(|position| {
                    self.norms
                        .as_ref()
                        .and_then(|norms| norms.values.get(position))
                })
        });
        let before = cells.map(|cell| cell.map_or(0, |value| value.load(Ordering::Relaxed)));
        let mut norms = before.map(f32::from_bits);
        let scores = query.score_batch8(rows, &mut norms);
        for j in 0..8 {
            if before[j] == 0 && norms[j].is_normal() {
                if let Some(cell) = cells[j] {
                    cell.store(norms[j].to_bits(), Ordering::Relaxed);
                }
            }
        }
        Ok(scores)
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
        Ok(self.codes_in_record(page, offset))
    }

    #[inline]
    fn codes_in_record<'a>(&self, page: &'a [u8], offset: usize) -> Option<(&'a [i8], i32)> {
        if page[offset] & REC_FLAG_INITIALIZED == 0 || page[offset] & REC_FLAG_EXACT_NAV != 0 {
            return None;
        }
        let dim = self.meta.dim as usize;
        let start = offset + 10 + dim * 4;
        let bytes = &page[start..start + dim];
        // SAFETY: i8/u8 have identical layout and the borrow is bounded by the view.
        let codes = unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<i8>(), dim) };
        Some((codes, read_u32(page, start + dim) as i32))
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
        Ok(query.score_bytes(self.exact_payload(vid, query.dimension())?))
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
        // Exact probes need the vector prefix, not only the page header. A
        // validated location can overlap those misses with neighboring rows.
        for &vid in vids {
            let page = vid_page(vid) as usize;
            if page < self.len / PAGE_SIZE {
                // SAFETY: prefetch targets an address inside the live mapping.
                unsafe {
                    let base = self.address.cast::<i8>().add(page * PAGE_SIZE);
                    if let Some(offset) = self
                        .records
                        .as_ref()
                        .and_then(|cache| cache.get(vid))
                        .filter(|_| codes.is_empty())
                    {
                        let payload = base.add(offset + 10);
                        for position in (0..(self.meta.dim as usize * 4).min(256)).step_by(64) {
                            prefetch_read(payload.add(position));
                        }
                    } else {
                        prefetch_read(base);
                    }
                }
                if codes.is_empty() {
                    if let Some(position) =
                        self.records.as_ref().and_then(|cache| cache.position(vid))
                    {
                        if let Some(cell) = self
                            .norms
                            .as_ref()
                            .and_then(|cache| cache.values.get(position))
                        {
                            prefetch_read((cell as *const AtomicU32).cast());
                        }
                    }
                }
            }
        }
        if codes.is_empty() && self.norms.is_some() {
            let mut chunks = vids.chunks_exact(8);
            for chunk in &mut chunks {
                out.extend(self.exact_distances8(chunk, query)?);
            }
            let mut tail = chunks.remainder().chunks_exact(4);
            for chunk in &mut tail {
                out.extend(self.exact_distances4(chunk, query)?);
            }
            for &vid in tail.remainder() {
                out.push(self.distance(vid, query, codes, norm)?);
            }
            return Ok(());
        }
        let mut chunks = vids.chunks_exact(4);
        for chunk in &mut chunks {
            if codes.is_empty() {
                out.extend(self.exact_distances4(chunk, query)?);
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
                    out.extend(self.exact_distances4(chunk, query)?);
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
        let explore_ties = metric != Metric::Dot || query.iter().any(|value| *value != 0.0);
        let mut normalized = Vec::new();
        let quantized_navigation =
            self.meta.nav_quant_valid && self.meta.nav_exact_nodes < self.meta.node_count;
        let src = if quantized_navigation && metric == Metric::Cosine {
            normalized.extend_from_slice(query);
            crate::engine::vector::distance::normalize_in_place(&mut normalized);
            normalized.as_slice()
        } else {
            query
        };
        let mut codes = Vec::new();
        if quantized_navigation
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
        let live_only = self.meta.tombstone_count != 0;
        let live = |vid| -> Result<bool, QueryError> {
            if !live_only {
                return Ok(true);
            }
            let (page, offset) = self.record(vid)?;
            Ok(page[offset] & REC_FLAG_TOMBSTONE == 0)
        };
        if live(current)? {
            results.push(first);
        }
        visit(current)?;
        let mut steps = 0usize;
        while let Some(Reverse(candidate)) = frontier.pop() {
            if steps & 63 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            steps += 1;
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
                if results.len() >= ef && explore_ties && score == furthest {
                    frontier.push(Reverse(Scored {
                        dist: score,
                        id: vid,
                    }));
                } else if results.len() < ef || score < furthest {
                    let entry = Scored {
                        dist: score,
                        id: vid,
                    };
                    frontier.push(Reverse(entry));
                    if live(vid)? {
                        if results.len() == ef {
                            results.pop();
                        }
                        results.push(entry);
                    }
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
