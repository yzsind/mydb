// Shard-local live undo directories. The canonical manager owns maintenance,
// recovery and evicted records. A maintenance guard consolidates the live
// directories under an exclusive publication barrier; ordinary publication
// and pointer reads only share that barrier and lock their owning shard.

use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::storage::undo::{PreparedUndoRead, UndoResidentReservation};

struct PublishedUndo {
    record: UndoRecord,
    reservation: UndoResidentReservation,
}

struct UndoPublicationShard {
    records: hashbrown::HashMap<u64, PublishedUndo, ahash::RandomState>,
    charge: ControlledMemoryCharge,
}

impl UndoPublicationShard {
    fn insert(&mut self, ptr: u64, publication: PublishedUndo) -> Result<(), QueryError> {
        if self.records.len() == self.records.capacity() {
            // Charge an upper bound before growing the hash table. The record
            // payload itself is already covered by its resident reservation.
            let buckets = (self.records.len() + 1).next_power_of_two().max(4) * 2;
            let target = buckets * (std::mem::size_of::<(u64, PublishedUndo)>() + 1);
            self.charge
                .try_grow(target.saturating_sub(self.charge.bytes()))?;
            self.records.try_reserve(1).map_err(|_| {
                error(
                    crate::error::catalog::ER_OUT_OF_RESOURCES,
                    "undo publication directory allocation failed",
                )
            })?;
        }
        assert!(
            !self.records.contains_key(&ptr),
            "duplicate live undo pointer"
        );
        self.records.insert(ptr, publication);
        Ok(())
    }
}

struct UndoLatch {
    inner: parking_lot::RwLock<UndoTablespace>,
    publications: Box<[parking_lot::Mutex<UndoPublicationShard>]>,
    published_count: std::sync::atomic::AtomicUsize,
    published_bytes: AtomicU64,
}

impl std::fmt::Debug for UndoLatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UndoLatch")
            .field(
                "published_count",
                &self.published_count.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl UndoLatch {
    fn new(value: UndoTablespace) -> Result<Self, QueryError> {
        let publications = (0..value.shard_count().max(1))
            .map(|_| {
                Ok(parking_lot::Mutex::new(UndoPublicationShard {
                    records: Default::default(),
                    charge: ControlledMemoryCharge::try_new(
                        ControlledMemoryCategory::SystemMetadata,
                        std::mem::size_of::<UndoPublicationShard>()
                            + std::mem::size_of::<parking_lot::RawMutex>(),
                    )?,
                }))
            })
            .collect::<Result<Vec<_>, QueryError>>()?
            .into_boxed_slice();
        Ok(Self {
            inner: parking_lot::RwLock::new(value),
            publications,
            published_count: std::sync::atomic::AtomicUsize::new(0),
            published_bytes: AtomicU64::new(0),
        })
    }

    fn shard(&self, ptr: u64) -> usize {
        // Segment ids encode the writer lane. Compaction-lane records remain
        // canonical; only normal append lanes enter these live directories.
        crate::storage::undo::publication_shard(ptr) % self.publications.len()
    }

    fn consolidate(&self, undo: &mut UndoTablespace) {
        if self.published_count.load(Ordering::Acquire) == 0 {
            return;
        }
        // The exclusive inner guard fences every publisher and point reader.
        // Visit shards in a fixed order; never retain two shard guards.
        for shard in &self.publications {
            for (_, publication) in shard.lock().records.drain() {
                undo.consolidate_published_record(publication.record, publication.reservation);
            }
        }
        self.published_count.store(0, Ordering::Release);
        self.published_bytes.store(0, Ordering::Relaxed);
        undo.finish_publication_barrier();
    }

    fn read(&self) -> parking_lot::RwLockReadGuard<'_, UndoTablespace> {
        let guard = self.inner.read();
        if self.published_count.load(Ordering::Acquire) == 0 {
            return guard;
        }
        drop(guard);
        parking_lot::RwLockWriteGuard::downgrade(self.write())
    }

    fn write(&self) -> parking_lot::RwLockWriteGuard<'_, UndoTablespace> {
        let mut guard = self.inner.write();
        self.consolidate(&mut guard);
        guard
    }

    fn try_read(&self) -> Option<parking_lot::RwLockReadGuard<'_, UndoTablespace>> {
        let guard = self.inner.try_read()?;
        if self.published_count.load(Ordering::Acquire) == 0 {
            return Some(guard);
        }
        drop(guard);
        let mut guard = self.inner.try_write()?;
        self.consolidate(&mut guard);
        Some(parking_lot::RwLockWriteGuard::downgrade(guard))
    }

    fn insert_admission_unpressured(&self, row_count: usize) -> bool {
        // Sharded publication is disabled by either accounting policy. Policy
        // setters consolidate before changing them, so this check is exact.
        self.inner
            .read()
            .insert_admission_unpressured(row_count, true)
    }

    fn prepare_load_owned(&self, ptr: u64) -> Result<Option<PreparedUndoRead>, QueryError> {
        let undo = self.inner.read();
        if let Some(publication) = self.publications[self.shard(ptr)].lock().records.get(&ptr) {
            return Ok(Some(PreparedUndoRead::Resident(publication.record.clone())));
        }
        undo.prepare_load_owned(ptr)
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
    ) -> Result<(u64, Vec<u8>, bool), QueryError> {
        let undo = self.inner.read();
        if !undo.sharded_publication_eligible() {
            drop(undo);
            return self.write().finish_concurrent_persistent_append(
                record,
                bytes,
                ptr,
                segment_id,
                segment_end,
                reservation,
            );
        }
        // A transaction's private append guard orders predecessors before this
        // publication. Depth can therefore be copied before locking our shard,
        // without acquiring two shard locks or depending on drain order.
        record.chain_depth = record.prev_undo_ptr.map_or(1, |previous| {
            self.publications[self.shard(previous)]
                .lock()
                .records
                .get(&previous)
                .map_or_else(
                    || undo.record_chain_depth(previous),
                    |entry| entry.record.chain_depth,
                )
                .saturating_add(1)
                .max(1)
        });
        record.id = ptr;
        record.encoded_len = u32::try_from(bytes.len()).map_err(|_| {
            error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "undo record is too large",
            )
        })?;
        let resident = u64::from(record.resident_size);
        self.publications[self.shard(ptr)].lock().insert(
            ptr,
            PublishedUndo {
                record,
                reservation,
            },
        )?;
        let count = self.published_count.fetch_add(1, Ordering::Release) + 1;
        let pending = self.published_bytes.fetch_add(resident, Ordering::Relaxed) + resident;
        let consolidate = count >= self.publications.len() * 256
            || (undo.mem_cache_limit_bytes != 0
                && undo.resident_bytes.saturating_add(pending) > undo.mem_cache_limit_bytes);
        drop(undo);
        if consolidate {
            drop(self.write());
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
    ) -> Result<(u64, Vec<u8>, bool), QueryError> {
        self.undo
            .publish(record, encoded, ptr, segment_id, segment_end, reservation)
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
            let (ptr, segment, end) = UndoTablespace::write_concurrent_persistent_frame(
                &shards[(depth - 1) % shards.len()],
                &mut bytes,
            )
            .unwrap();
            latch
                .publish(record, bytes, ptr, segment, end, reservation)
                .unwrap();
            assert!(latch.published_count.load(Ordering::Acquire) > 0);
            let loaded = latch
                .prepare_load_owned(ptr)
                .unwrap()
                .unwrap()
                .finish()
                .unwrap()
                .unwrap();
            assert_eq!(loaded.prev_undo_ptr, previous);
            assert_eq!(loaded.chain_depth, depth as u32);
            // Alternate reads, writes, and try-reads all expose complete
            // canonical state, independent of cross-shard drain order.
            if depth % 3 == 0 {
                assert_eq!(latch.read().record_count(), depth);
                assert_eq!(latch.published_count.load(Ordering::Acquire), 0);
            } else if depth % 3 == 1 {
                assert_eq!(latch.write().record_count(), depth);
            } else {
                assert_eq!(latch.try_read().unwrap().record_count(), depth);
            }
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
}
