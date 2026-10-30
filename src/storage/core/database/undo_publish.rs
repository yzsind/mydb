// Live publication and canonical maintenance use independent latches. A short
// capture swaps dependency-closed live buffers, then releases publishers
// before accounting, purge, cache I/O or compaction touches the canonical view.

use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::storage::undo::{PreparedUndoRead, UndoResidentReservation, UndoSegmentLease};

struct PublishedUndo {
    record: UndoRecord,
    reservation: UndoResidentReservation,
    segment_lease: UndoSegmentLease,
    sequence: u64,
}

struct UndoPublicationBuffer {
    records: Vec<Option<PublishedUndo>>,
    index: hashbrown::HashMap<u64, usize, ahash::RandomState>,
    map_bound: usize,
    charge: ControlledMemoryCharge,
}
impl Default for UndoPublicationBuffer {
    fn default() -> Self {
        Self {
            records: Vec::new(),
            index: Default::default(),
            map_bound: 0,
            charge: ControlledMemoryCharge::try_new(ControlledMemoryCategory::SystemMetadata, 0)
                .expect("zero-byte publication buffer reservation"),
        }
    }
}
impl UndoPublicationBuffer {
    fn insert(
        &mut self,
        ptr: u64,
        mut entry: PublishedUndo,
        sequence: &AtomicU64,
    ) -> Result<(), QueryError> {
        assert!(
            !self.index.contains_key(&ptr),
            "duplicate live undo pointer"
        );
        let map_bound = if self.index.len() == self.index.capacity() {
            (self.index.len() + 1).next_power_of_two().max(4)
                * 2
                * (std::mem::size_of::<(u64, usize)>() + 1)
        } else {
            self.map_bound
        };
        let capacity = if self.records.len() == self.records.capacity() {
            self.records.capacity().saturating_mul(2).max(4)
        } else {
            self.records.capacity()
        };
        let bytes = map_bound
            .saturating_add(capacity.saturating_mul(std::mem::size_of::<Option<PublishedUndo>>()));
        self.charge
            .try_grow(bytes.saturating_sub(self.charge.bytes()))?;
        self.index.try_reserve(1).map_err(|_| {
            error(
                crate::error::catalog::ER_OUT_OF_RESOURCES,
                "undo pointer directory allocation failed",
            )
        })?;
        if capacity > self.records.capacity() {
            self.records
                .try_reserve_exact(capacity - self.records.len())
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_OUT_OF_RESOURCES,
                        "undo publication buffer allocation failed",
                    )
                })?;
        }
        self.map_bound = map_bound;
        // Assigned under the owning shard latch: every shard's vector is sorted
        // and transaction-private append fences order a child's predecessor.
        entry.sequence = sequence.fetch_add(1, Ordering::Relaxed);
        self.index.insert(ptr, self.records.len());
        self.records.push(Some(entry));
        Ok(())
    }
    fn get(&self, ptr: u64) -> Option<&PublishedUndo> {
        self.index
            .get(&ptr)
            .and_then(|index| self.records[*index].as_ref())
    }
    fn clear(&mut self) {
        self.records.clear();
        self.index.clear();
    }
}

struct UndoPublicationShard {
    live: UndoPublicationBuffer,
    spare: Option<UndoPublicationBuffer>,
    live_bytes: u64,
    _charge: ControlledMemoryCharge,
}
struct UndoPublicationLane {
    state: parking_lot::Mutex<UndoPublicationShard>,
    count: CachePadded<std::sync::atomic::AtomicUsize>,
}

struct UndoLatch {
    inner: parking_lot::RwLock<UndoTablespace>,
    publications: Box<[UndoPublicationLane]>,
    sequence: AtomicU64,
    stage_allowed: AtomicBool,
    unrestricted_admission: AtomicBool,
    cache_limit: AtomicU64,
    disk_softfail_warn: AtomicBool,
    canonical_resident: AtomicU64,
}
impl std::fmt::Debug for UndoLatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UndoLatch")
            .field("published_count", &self.pending_count())
            .finish_non_exhaustive()
    }
}

struct UndoDirectoryWriteGuard<'a> {
    owner: &'a UndoLatch,
    guard: Option<parking_lot::RwLockWriteGuard<'a, UndoTablespace>>,
}
impl std::ops::Deref for UndoDirectoryWriteGuard<'_> {
    type Target = UndoTablespace;
    fn deref(&self) -> &Self::Target {
        self.guard.as_deref().expect("live undo write guard")
    }
}
impl std::ops::DerefMut for UndoDirectoryWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.as_deref_mut().expect("live undo write guard")
    }
}
impl Drop for UndoDirectoryWriteGuard<'_> {
    fn drop(&mut self) {
        if let Some(guard) = &self.guard {
            self.owner.refresh_policy(guard);
        }
    }
}
impl<'a> UndoDirectoryWriteGuard<'a> {
    fn into_read(mut self) -> parking_lot::RwLockReadGuard<'a, UndoTablespace> {
        self.owner
            .refresh_policy(self.guard.as_deref().expect("live undo write guard"));
        parking_lot::RwLockWriteGuard::downgrade(self.guard.take().unwrap())
    }
}

impl UndoLatch {
    fn pending_count(&self) -> usize {
        self.publications
            .iter()
            .map(|lane| lane.count.load(Ordering::Acquire))
            .sum()
    }
    fn new(value: UndoTablespace) -> Result<Self, QueryError> {
        let publications = (0..value.shard_count().max(1))
            .map(|_| {
                Ok(UndoPublicationLane {
                    state: parking_lot::Mutex::new(UndoPublicationShard {
                        live: Default::default(),
                        spare: None,
                        live_bytes: 0,
                        _charge: ControlledMemoryCharge::try_new(
                            ControlledMemoryCategory::SystemMetadata,
                            std::mem::size_of::<UndoPublicationLane>(),
                        )?,
                    }),
                    count: CachePadded::new(std::sync::atomic::AtomicUsize::new(0)),
                })
            })
            .collect::<Result<Vec<_>, QueryError>>()?
            .into_boxed_slice();
        Ok(Self {
            stage_allowed: AtomicBool::new(value.sharded_publication_eligible()),
            unrestricted_admission: AtomicBool::new(
                value.mem_soft_limit_bytes == 0 && value.tablespace_max_bytes == 0,
            ),
            cache_limit: AtomicU64::new(value.mem_cache_limit_bytes),
            disk_softfail_warn: AtomicBool::new(value.disk_cap_softfail_warn()),
            canonical_resident: AtomicU64::new(value.resident_bytes),
            inner: parking_lot::RwLock::new(value),
            publications,
            sequence: AtomicU64::new(0),
        })
    }
    fn refresh_policy(&self, value: &UndoTablespace) {
        self.canonical_resident
            .store(value.resident_bytes, Ordering::Release);
        self.cache_limit
            .store(value.mem_cache_limit_bytes, Ordering::Release);
        self.unrestricted_admission.store(
            value.mem_soft_limit_bytes == 0 && value.tablespace_max_bytes == 0,
            Ordering::Release,
        );
        self.stage_allowed
            .store(value.sharded_publication_eligible(), Ordering::Release);
        self.disk_softfail_warn
            .store(value.disk_cap_softfail_warn(), Ordering::Release);
    }
    fn shard(&self, ptr: u64) -> usize {
        crate::storage::undo::publication_shard(ptr) % self.publications.len()
    }
    fn consolidate(&self, undo: &mut UndoTablespace) {
        if self.pending_count() == 0 {
            return;
        }
        // Capture only pointer-sized buffer swaps while publishers are fenced.
        // Segment leases keep captured/live frames safe from retirement until
        // their canonical metadata is installed below.
        let mut snapshots: smallvec::SmallVec<[(UndoPublicationBuffer, usize); 16]> = {
            let mut lanes: smallvec::SmallVec<[_; 16]> = self
                .publications
                .iter()
                .map(|lane| lane.state.lock())
                .collect();
            lanes
                .iter_mut()
                .enumerate()
                .map(|(index, shard)| {
                    let replacement = shard.spare.take().unwrap_or_default();
                    let captured = std::mem::replace(&mut shard.live, replacement);
                    shard.live_bytes = 0;
                    self.publications[index].count.store(0, Ordering::Release);
                    (captured, 0)
                })
                .collect()
        };
        // A k-way merge uses the already-admitted vectors, without allocating
        // a second record array or scratch sort at a memory-pressure barrier.
        loop {
            let next = snapshots
                .iter()
                .enumerate()
                .filter_map(|(index, (buffer, position))| {
                    buffer
                        .records
                        .get(*position)
                        .and_then(Option::as_ref)
                        .map(|entry| (index, entry.sequence))
                })
                .min_by_key(|(_, sequence)| *sequence)
                .map(|(index, _)| index);
            let Some(index) = next else {
                break;
            };
            let (buffer, position) = &mut snapshots[index];
            let entry = buffer.records[*position].take().unwrap();
            *position += 1;
            undo.consolidate_published_record(entry.record, entry.reservation);
            drop(entry.segment_lease);
        }
        for (shard, (mut buffer, _)) in self.publications.iter().zip(snapshots) {
            buffer.clear();
            let mut shard = shard.state.lock();
            assert!(
                shard.spare.is_none(),
                "one canonical consolidator owns each spare buffer"
            );
            shard.spare = Some(buffer);
        }
        undo.finish_publication_barrier();
        self.refresh_policy(undo);
    }
    fn write(&self) -> UndoDirectoryWriteGuard<'_> {
        let mut guard = self.inner.write();
        self.consolidate(&mut guard);
        UndoDirectoryWriteGuard {
            owner: self,
            guard: Some(guard),
        }
    }
    fn read(&self) -> parking_lot::RwLockReadGuard<'_, UndoTablespace> {
        let guard = self.inner.read();
        if self.pending_count() == 0 {
            return guard;
        }
        drop(guard);
        self.write().into_read()
    }
    fn try_read(&self) -> Option<parking_lot::RwLockReadGuard<'_, UndoTablespace>> {
        let guard = self.inner.try_read()?;
        // Optional diagnostics/reclaim probes must never acquire live-lane
        // latches, allocate, or flush undo while their caller owns storage
        // locks. Mandatory callers use read()/write() to consolidate.
        (self.pending_count() == 0).then_some(guard)
    }
    fn insert_admission_unpressured(&self, row_count: usize) -> bool {
        if self.unrestricted_admission.load(Ordering::Acquire) {
            return true;
        }
        self.inner
            .read()
            .insert_admission_unpressured(row_count, true)
    }
    fn prepare_load_owned(&self, ptr: u64) -> Result<Option<PreparedUndoRead>, QueryError> {
        {
            if let Some(entry) = self.publications[self.shard(ptr)]
                .state
                .lock()
                .live
                .get(ptr)
            {
                return Ok(Some(PreparedUndoRead::Resident(entry.record.clone())));
            }
        }
        // A record captured by maintenance becomes visible when its canonical
        // writer finishes. Never retain a lane latch while waiting for it.
        self.inner.read().prepare_load_owned(ptr)
    }
    #[allow(clippy::too_many_arguments)]
    fn publish(
        &self,
        mut record: UndoRecord,
        bytes: Vec<u8>,
        ptr: u64,
        segment_id: u32,
        segment_end: u64,
        reservation: UndoResidentReservation,
        segment_lease: UndoSegmentLease,
    ) -> Result<(u64, Vec<u8>, bool), QueryError> {
        if !self.stage_allowed.load(Ordering::Acquire) {
            return self.write().finish_concurrent_persistent_append(
                record,
                bytes,
                ptr,
                segment_id,
                segment_end,
                reservation,
            );
        }
        record.id = ptr;
        record.chain_depth = 0; // Derived, in publication order, in canonical state.
        record.encoded_len = u32::try_from(bytes.len()).map_err(|_| {
            error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "undo record is too large",
            )
        })?;
        let resident = u64::from(record.resident_size);
        let (count, pending) = {
            let lane = &self.publications[self.shard(ptr)];
            let mut shard = lane.state.lock();
            shard.live.insert(
                ptr,
                PublishedUndo {
                    record,
                    reservation,
                    segment_lease,
                    sequence: 0,
                },
                &self.sequence,
            )?;
            shard.live_bytes = shard.live_bytes.saturating_add(resident);
            let count = shard.live.records.len();
            lane.count.store(count, Ordering::Release);
            (count, shard.live_bytes)
        };
        let limit = self.cache_limit.load(Ordering::Acquire);
        let headroom = limit.saturating_sub(self.canonical_resident.load(Ordering::Acquire));
        if count >= 256 || (limit != 0 && pending > headroom / self.publications.len() as u64) {
            // A maintenance writer already owns a coherent view. It must not
            // stall new publishers; all live payloads remain governor-admitted.
            if let Some(mut guard) = self.inner.try_write() {
                self.consolidate(&mut guard);
                self.refresh_policy(&guard);
            }
        }
        Ok((ptr, bytes, false))
    }
}

impl Database {
    #[allow(clippy::too_many_arguments)]
    fn publish_concurrent_undo_frame(
        &self,
        record: UndoRecord,
        encoded: Vec<u8>,
        ptr: u64,
        segment_id: u32,
        segment_end: u64,
        reservation: UndoResidentReservation,
        segment_lease: UndoSegmentLease,
    ) -> Result<(u64, Vec<u8>, bool), QueryError> {
        self.undo.publish(
            record,
            encoded,
            ptr,
            segment_id,
            segment_end,
            reservation,
            segment_lease,
        )
    }
}
#[cfg(test)]
mod sharded_undo_tests {
    use super::*;

    fn record(previous: Option<u64>) -> UndoRecord {
        UndoRecord {
            id: 0,
            txn_id: 1,
            created_unix_ms: 1,
            prev_undo_ptr: previous,
            kind: UndoKind::Insert,
            table_id: 1,
            row_id: RowId::new(0, 0),
            before_header: None,
            before_values: None,
            before_external: Vec::new(),
            borrowed_external: Vec::new(),
            chain_depth: 0,
            cached_size: 0,
            resident_size: 0,
            temp_reservation: None,
            resident: true,
            encoded_len: 0,
        }
    }

    #[test]
    fn pointer_reads_and_cross_shard_depth_survive_maintenance_barriers() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let manager = UndoTablespace::open_with_segments(
            &Vec::<crate::wal::WalRecord>::new(),
            dir.path(),
            1 << 20,
        )
        .unwrap();
        let shards = manager.concurrent_persistent_append_shards();
        let admission = manager.resident_admission();
        let latch = UndoLatch::new(manager).unwrap();
        let mut previous = None;
        for depth in 1..=12 {
            let mut record = record(previous);
            record.resident_size =
                crate::storage::undo::estimate_undo_resident_bytes(&record) as u32;
            let reservation = admission.reserve(&record).unwrap();
            let mut bytes = crate::storage::undo::encode_undo_record(&record).unwrap();
            let (ptr, segment, end, lease) = UndoTablespace::write_leased_persistent_frame(
                &shards[(depth - 1) % shards.len()],
                &mut bytes,
            )
            .unwrap();
            latch
                .publish(record, bytes, ptr, segment, end, reservation, lease)
                .unwrap();
            assert!(latch.pending_count() > 0);
            let loaded = latch
                .prepare_load_owned(ptr)
                .unwrap()
                .unwrap()
                .finish()
                .unwrap()
                .unwrap();
            assert_eq!(loaded.prev_undo_ptr, previous);
            // Live entries carry the same unknown diagnostic depth as a cold
            // on-disk frame. Canonical consolidation computes exact depths.
            // Alternate reads, writes, and try-reads all expose complete
            // canonical state, independent of cross-shard drain order.
            if depth % 3 == 0 {
                assert_eq!(latch.read().record_count(), depth);
                assert_eq!(latch.pending_count(), 0);
            } else if depth % 3 == 1 {
                assert_eq!(latch.write().record_count(), depth);
            } else {
                assert!(latch.try_read().is_none());
                drop(latch.read());
                assert_eq!(latch.try_read().unwrap().record_count(), depth);
            }
            assert_eq!(
                latch.read().load_owned(ptr).unwrap().chain_depth,
                depth as u32
            );
            previous = Some(ptr);
        }
        latch.write().flush().unwrap();
        drop(latch);
        let recovered = UndoTablespace::open_with_segments(
            &Vec::<crate::wal::WalRecord>::new(),
            dir.path(),
            1 << 20,
        )
        .unwrap();
        assert_eq!(recovered.record_count(), 12);
        assert!(recovered.load_owned(previous.unwrap()).is_some());
    }
    #[test]
    fn live_publication_does_not_wait_for_canonical_maintenance() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let manager = UndoTablespace::open_with_segments(
            &Vec::<crate::wal::WalRecord>::new(),
            dir.path(),
            1 << 20,
        )
        .unwrap();
        let shard = manager.concurrent_persistent_append_shards()[0].clone();
        let admission = manager.resident_admission();
        let latch = Arc::new(UndoLatch::new(manager).unwrap());
        let mut entry = record(None);
        entry.resident_size = crate::storage::undo::estimate_undo_resident_bytes(&entry) as u32;
        let reservation = admission.reserve(&entry).unwrap();
        let mut bytes = crate::storage::undo::encode_undo_record(&entry).unwrap();
        let (ptr, segment, end, lease) =
            UndoTablespace::write_leased_persistent_frame(&shard, &mut bytes).unwrap();
        let maintenance = latch.write();
        let worker_latch = Arc::clone(&latch);
        let (send, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            send.send(worker_latch.publish(entry, bytes, ptr, segment, end, reservation, lease))
                .unwrap();
        });
        let published = receive.recv_timeout(Duration::from_millis(200));
        drop(maintenance);
        worker.join().unwrap();
        published
            .expect("live publication waited for canonical maintenance")
            .unwrap();
        assert!(latch.prepare_load_owned(ptr).unwrap().is_some());
    }
    #[test]
    fn capture_orders_cross_lane_live_chains_before_deriving_depths() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let manager = UndoTablespace::open_with_segments(
            &Vec::<crate::wal::WalRecord>::new(),
            dir.path(),
            1 << 20,
        )
        .unwrap();
        let lanes = manager.concurrent_persistent_append_shards();
        let admission = manager.resident_admission();
        let latch = UndoLatch::new(manager).unwrap();
        let mut previous = None;
        let mut ids = Vec::new();
        for index in 0..64 {
            let mut entry = record(previous);
            entry.resident_size = crate::storage::undo::estimate_undo_resident_bytes(&entry) as u32;
            let reservation = admission.reserve(&entry).unwrap();
            let mut bytes = crate::storage::undo::encode_undo_record(&entry).unwrap();
            let (ptr, segment, end, lease) = UndoTablespace::write_leased_persistent_frame(
                &lanes[index % lanes.len()],
                &mut bytes,
            )
            .unwrap();
            latch
                .publish(entry, bytes, ptr, segment, end, reservation, lease)
                .unwrap();
            ids.push(ptr);
            previous = Some(ptr);
        }
        assert_eq!(latch.pending_count(), ids.len());
        let canonical = latch.read();
        for (index, ptr) in ids.iter().enumerate() {
            let loaded = canonical.load_owned(*ptr).unwrap();
            assert_eq!(loaded.chain_depth, index as u32 + 1);
            assert_eq!(
                loaded.prev_undo_ptr,
                index.checked_sub(1).map(|prev| ids[prev])
            );
        }
    }

    #[test]
    fn policy_shadows_follow_writes_and_guard_downgrade() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let manager = UndoTablespace::open_with_segments(
            &Vec::<crate::wal::WalRecord>::new(),
            dir.path(),
            1 << 20,
        )
        .unwrap();
        let latch = UndoLatch::new(manager).unwrap();
        assert!(latch.stage_allowed.load(Ordering::Acquire));
        let mut writer = latch.write();
        writer.set_txn_max_bytes(1024);
        let reader = writer.into_read();
        assert!(!latch.stage_allowed.load(Ordering::Acquire));
        drop(reader);
        {
            let mut writer = latch.write();
            writer.set_txn_max_bytes(0);
            writer.set_mem_soft_limit_bytes(1024);
        }
        assert!(!latch.stage_allowed.load(Ordering::Acquire));
        assert!(!latch.unrestricted_admission.load(Ordering::Acquire));
        {
            let mut writer = latch.write();
            writer.set_mem_soft_limit_bytes(0);
            writer.set_mem_cache_limit_bytes(2048);
        }
        assert!(latch.stage_allowed.load(Ordering::Acquire));
        assert!(latch.unrestricted_admission.load(Ordering::Acquire));
        assert_eq!(latch.cache_limit.load(Ordering::Acquire), 2048);
    }
    #[test]
    fn optional_diagnostics_do_not_reenter_live_lane_latches() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let manager = UndoTablespace::open_with_segments(
            &Vec::<crate::wal::WalRecord>::new(),
            dir.path(),
            1 << 20,
        )
        .unwrap();
        let lane = manager.concurrent_persistent_append_shards()[0].clone();
        let admission = manager.resident_admission();
        let latch = Arc::new(UndoLatch::new(manager).unwrap());
        let mut entry = record(None);
        entry.resident_size = crate::storage::undo::estimate_undo_resident_bytes(&entry) as u32;
        let reservation = admission.reserve(&entry).unwrap();
        let mut bytes = crate::storage::undo::encode_undo_record(&entry).unwrap();
        let (ptr, segment, end, lease) =
            UndoTablespace::write_leased_persistent_frame(&lane, &mut bytes).unwrap();
        latch
            .publish(entry, bytes, ptr, segment, end, reservation, lease)
            .unwrap();
        let held_lane = latch.publications[latch.shard(ptr)].state.lock();
        let worker_latch = Arc::clone(&latch);
        let (send, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            send.send(worker_latch.try_read().is_none()).unwrap();
        });
        let skipped = receive.recv_timeout(Duration::from_millis(200));
        drop(held_lane);
        worker.join().unwrap();
        assert!(skipped.expect("optional probe waited on a live publication latch"));
        assert_eq!(latch.pending_count(), 1);
        assert_eq!(latch.read().record_count(), 1);
    }
}
