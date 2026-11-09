const ROOT_ROUTING_MIN_READS: u8 = 8;

/// An immutable, optional internal-node image. The Frame owns the same
/// invalidation token as the IndexFile, but neither owner retains a Frame or
/// a page pin. Clearing `image` releases bytes even if the index keeps its token.
#[derive(Debug)]
struct RootRoutingCache {
    page_id: u64,
    image: arc_swap::ArcSwapOption<RootRoutingImage>,
    references: AtomicU8,
    _memory: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

#[derive(Debug)]
struct RootRoutingImage {
    bytes: Box<[u8]>,
    _memory: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl Frame {
    fn invalidate_root_routing(&self) {
        let mut slot = self.root_routing.lock();
        if let Some(cache) = slot.take() {
            cache.image.store(None);
        }
        self.has_root_routing.store(false, Ordering::Release);
    }

    fn write_bytes(&self) -> std::sync::LockResult<RwLockWriteGuard<'_, Vec<u8>>> {
        let bytes = self.bytes.write()?;
        // Invalidate before exposing writable bytes, including failed raw
        // mutations, physical replay and checksum/materialization boundaries.
        if self.has_root_routing.load(Ordering::Acquire) {
            self.invalidate_root_routing();
        }
        Ok(bytes)
    }

    fn refresh_root_routing_reference(&self) {
        if self.has_root_routing.load(Ordering::Relaxed) {
            let slot = self.root_routing.lock();
            if let Some(cache) = slot.as_ref() {
                let references = cache.references.swap(0, Ordering::Relaxed);
                if references != 0 {
                    self.ref_bit.store(true, Ordering::Relaxed);
                    let usage = self.usage_count.load(Ordering::Relaxed);
                    self.usage_count.fetch_max(
                        usage
                            .saturating_add(references)
                            .min(Self::MAX_NORMAL_USAGE_COUNT),
                        Ordering::Relaxed,
                    );
                }
            }
        }
    }
}

impl IndexFile {
    /// Callers hold the tree topology guard through descent and leaf access.
    /// Internal-node snapshots are invalidated before any raw writer proceeds;
    /// leaves always retain the ordinary pin and latch path.
    fn route_cached_root(
        &self,
        page_id: u64,
        key: &IndexKey,
        row_id: RowId,
    ) -> Result<Option<RawIndexPageRoute>, QueryError> {
        if page_id != self.root_page.load(Ordering::Acquire) {
            return Ok(None);
        }
        let Some(slot) = self.root_routing.get() else {
            return Ok(None);
        };
        let cache = slot.load();
        let Some(cache) = cache.as_ref().filter(|cache| cache.page_id == page_id) else {
            return Ok(None);
        };
        let image = cache.image.load();
        let Some(image) = image.as_ref() else {
            return Ok(None);
        };
        let references = cache.references.load(Ordering::Relaxed);
        if references < Frame::MAX_NORMAL_USAGE_COUNT {
            cache
                .references
                .fetch_max(references + 1, Ordering::Relaxed);
        }
        record_query_buffer_hit();
        self.buffer_pool.shards[self.buffer_pool.shard_index_ref(self.page_key_ref(page_id))]
            .hits
            .fetch_add(1, Ordering::Relaxed);
        route_index_page_bytes(&image.bytes, key, row_id).map(Some)
    }

    fn remember_root_routing<P: std::ops::Deref<Target = BufferPool>>(
        &self,
        pinned: &PinnedFrame<P>,
    ) -> Result<(), QueryError> {
        let frame = &pinned.frame;
        if frame.key.page_id != self.root_page.load(Ordering::Acquire)
            || !hashbrown::Equivalent::equivalent(&self.page_key_ref(frame.key.page_id), &frame.key)
        {
            return Ok(());
        }
        // Copying an internal page is worthwhile only for a reused handle.
        // Cold, capacity-churned indexes keep the allocation-free pin path.
        let reads = self.root_routing_reads.load(Ordering::Relaxed);
        if reads < ROOT_ROUTING_MIN_READS {
            self.root_routing_reads
                .fetch_max(reads + 1, Ordering::Relaxed);
            return Ok(());
        }
        let resident = frame.root_routing.lock().as_ref().cloned();
        if let Some(cache) = resident {
            self.root_routing
                .get_or_init(arc_swap::ArcSwapOption::empty)
                .store(Some(cache));
            return Ok(());
        }
        // Drop an obsolete token before admission. A refused cache allocation
        // must leave the authoritative, pinned read completely usable.
        if let Some(slot) = self.root_routing.get() {
            slot.store(None);
        }
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge, StorageLatchAdmissionScope,
        };
        let _admission = StorageLatchAdmissionScope::enter();
        let metadata_bytes = std::mem::size_of::<RootRoutingCache>()
            .saturating_add(2 * std::mem::size_of::<usize>());
        let Some(metadata) = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::SystemMetadata,
            metadata_bytes,
        ) else {
            return Ok(());
        };
        let Some(memory) = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::DerivedCache,
            PAGE_SIZE + std::mem::size_of::<RootRoutingImage>() + 2 * std::mem::size_of::<usize>(),
        ) else {
            return Ok(());
        };
        let cache = pinned.with_index_raw_bytes(|bytes| {
            let mut slot = frame.root_routing.lock();
            if !frame.is_ready() {
                return Ok(None);
            }
            if let Some(cache) = slot.as_ref() {
                return Ok(Some(Arc::clone(cache)));
            }
            if parse_raw_internal_route_page(bytes)?.is_none() {
                return Ok(None);
            }
            let image = Arc::new(RootRoutingImage {
                bytes: bytes.into(),
                _memory: memory,
            });
            let cache = Arc::new(RootRoutingCache {
                page_id: frame.key.page_id,
                image: arc_swap::ArcSwapOption::new(Some(image)),
                references: AtomicU8::new(0),
                _memory: metadata,
            });
            *slot = Some(Arc::clone(&cache));
            frame.has_root_routing.store(true, Ordering::Release);
            Ok(Some(cache))
        })?;
        // A concurrent discard may clear the payload before publication. The
        // token then remains a miss, never a stale root image or retained page.
        if let Some(cache) = cache {
            self.root_routing
                .get_or_init(arc_swap::ArcSwapOption::empty)
                .store(Some(cache));
        }
        Ok(())
    }
}
