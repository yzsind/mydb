//! Fixed-size Zone Map page primitives. Page views borrow their input, so a
//! caller cannot retain decoded summaries after releasing its buffer guard.
//!
//! Common header (little endian): standard 24-byte LSN/checksum header,
//! storage id at 24, generation at 32, page id at 40, next page at 48,
//! entry count at 56, lower/upper free-space bounds at 58/60. Bytes 62..64
//! are reserved. Full keys use 20-byte slots. PackedRecords reuses bytes
//! 48..56 for base column/zone (u32 each), and uses 8-byte slots containing
//! column delta u8, kind u8, zone delta u16, payload offset/length u16 each.
//! Wider key spans retain full slots. Both forms are sorted by (column,
//! kind, zone), with checked, tightly packed payloads at the page end.

use super::*;

mod directory;
pub(super) mod mutable;
pub(super) mod summary;

pub(super) const MAGIC: u32 = 0x5a_4d_50_31; // ZMP1
const VERSION: u16 = crate::storage::format_versions::PAGED_ZONE_MAP_PAGE_VERSION;
const HEADER: usize = 64;
const SLOT: usize = 20;
const PACKED_SLOT: usize = 8;
#[cfg(test)]
const MAX_ENTRIES: usize = (PAGE_SIZE - HEADER) / SLOT;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum Kind {
    Meta = 0,
    Directory = 1,
    Bounds = 2,
    Membership = 3,
    ZoneMeta = 4,
    Overflow = 5,
    Records = 6,
    PackedRecords = 7,
}

impl TryFrom<u8> for Kind {
    type Error = QueryError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Meta),
            1 => Ok(Self::Directory),
            2 => Ok(Self::Bounds),
            3 => Ok(Self::Membership),
            4 => Ok(Self::ZoneMeta),
            5 => Ok(Self::Overflow),
            6 => Ok(Self::Records),
            7 => Ok(Self::PackedRecords),
            _ => Err(corrupt("unknown page kind")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Identity {
    pub storage: u64,
    pub generation: u64,
    pub page: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(super) struct Key {
    pub column: u32,
    pub kind: u8,
    pub zone: u64,
}

impl Key {
    fn read(bytes: &[u8], offset: usize) -> Result<Self, QueryError> {
        if bytes[offset + 5..offset + 8] != [0; 3] {
            return Err(corrupt("nonzero key reserved bytes"));
        }
        Kind::try_from(bytes[offset + 4])?;
        Ok(Self {
            column: read_u32(bytes, offset),
            kind: bytes[offset + 4],
            zone: read_u64(bytes, offset + 8),
        })
    }

    fn write(self, bytes: &mut [u8], offset: usize) {
        write_u32(bytes, offset, self.column);
        bytes[offset + 4] = self.kind;
        write_u64(bytes, offset + 8, self.zone);
    }
}

#[derive(Clone, Copy)]
struct PackedPrefix {
    column: u32,
    zone: u32,
}

fn packed_prefix(kind: Kind, keys: impl IntoIterator<Item = Key>) -> Option<PackedPrefix> {
    if kind != Kind::Records {
        return None;
    }
    let mut keys = keys.into_iter();
    let first = keys.next()?;
    let (mut low_column, mut high_column, mut low_zone, mut high_zone) =
        (first.column, first.column, first.zone, first.zone);
    for key in keys {
        low_column = low_column.min(key.column);
        high_column = high_column.max(key.column);
        low_zone = low_zone.min(key.zone);
        high_zone = high_zone.max(key.zone);
    }
    (high_column - low_column <= u8::MAX as u32
        && low_zone <= u32::MAX as u64
        && high_zone - low_zone <= u16::MAX as u64)
        .then_some(PackedPrefix {
            column: low_column,
            zone: low_zone as u32,
        })
}

fn slot_width(kind: Kind, keys: impl IntoIterator<Item = Key>) -> usize {
    if packed_prefix(kind, keys).is_some() {
        PACKED_SLOT
    } else {
        SLOT
    }
}

fn corrupt(reason: &str) -> QueryError {
    error(
        crate::error::catalog::ER_GET_ERRNO,
        format!("corrupt zone-map page: {reason}"),
    )
}

/// A validated page, including sorted keys and disjoint, tightly packed
/// payloads. Validation never follows pointers or allocates from disk lengths.
pub(super) struct View<'a> {
    bytes: &'a [u8],
    kind: Kind,
    count: usize,
}

impl<'a> View<'a> {
    pub fn open(bytes: &'a [u8], identity: Identity) -> Result<Self, QueryError> {
        if bytes.len() != PAGE_SIZE {
            return Err(corrupt("invalid page length"));
        }
        if read_u32(bytes, 0) != MAGIC || read_u16(bytes, 4) != VERSION {
            return Err(corrupt("invalid magic or version"));
        }
        let kind = Kind::try_from(bytes[6])?;
        if bytes[7] != 0 || read_u32(bytes, 20) != 0 || read_u16(bytes, 62) != 0 {
            return Err(corrupt("unknown flags or reserved bytes"));
        }
        if read_u32(bytes, 16) != page_checksum(bytes, 16) {
            return Err(corrupt("checksum mismatch"));
        }
        if identity.generation == 0
            || read_u64(bytes, 24) != identity.storage
            || read_u64(bytes, 32) != identity.generation
            || read_u64(bytes, 40) != identity.page
        {
            return Err(corrupt("owner, generation or page identity mismatch"));
        }
        let count = read_u16(bytes, 56) as usize;
        let lower = read_u16(bytes, 58) as usize;
        let upper = read_u16(bytes, 60) as usize;
        let slot = if kind == Kind::PackedRecords {
            PACKED_SLOT
        } else {
            SLOT
        };
        if count > (PAGE_SIZE - HEADER) / slot
            || lower != HEADER + count * slot
            || lower > upper
            || upper > PAGE_SIZE
        {
            return Err(corrupt("invalid free-space bounds"));
        }
        let view = Self { bytes, kind, count };
        let mut prior = None;
        let mut payload_end = PAGE_SIZE;
        for index in 0..count {
            let offset = HEADER + index * slot;
            let key = view.key_at(offset)?;
            if prior.is_some_and(|prior| prior >= key) {
                return Err(corrupt("keys are not strictly ordered"));
            }
            let start = read_u16(bytes, offset + slot - 4) as usize;
            let len = read_u16(bytes, offset + slot - 2) as usize;
            if start < upper || start.checked_add(len) != Some(payload_end) {
                return Err(corrupt("overlapping or out-of-bounds payload"));
            }
            payload_end = start;
            prior = Some(key);
        }
        if payload_end != upper {
            return Err(corrupt("unreferenced payload bytes"));
        }
        Ok(view)
    }

    fn slot_width(&self) -> usize {
        if self.kind == Kind::PackedRecords {
            PACKED_SLOT
        } else {
            SLOT
        }
    }

    fn key_at(&self, offset: usize) -> Result<Key, QueryError> {
        if self.kind != Kind::PackedRecords {
            return Key::read(self.bytes, offset);
        }
        Kind::try_from(self.bytes[offset + 1])?;
        Ok(Key {
            column: read_u32(self.bytes, 48)
                .checked_add(self.bytes[offset] as u32)
                .ok_or_else(|| corrupt("packed column overflow"))?,
            kind: self.bytes[offset + 1],
            zone: u64::from(read_u32(self.bytes, 52)) + u64::from(read_u16(self.bytes, offset + 2)),
        })
    }

    pub fn kind(&self) -> Kind {
        if self.kind == Kind::PackedRecords {
            Kind::Records
        } else {
            self.kind
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn next(&self) -> u64 {
        if self.kind == Kind::PackedRecords {
            0
        } else {
            read_u64(self.bytes, 48)
        }
    }

    pub fn entry(&self, index: usize) -> Option<(Key, &'a [u8])> {
        if index >= self.count {
            return None;
        }
        let slot = self.slot_width();
        let offset = HEADER + index * slot;
        let start = read_u16(self.bytes, offset + slot - 4) as usize;
        let len = read_u16(self.bytes, offset + slot - 2) as usize;
        Some((self.key_at(offset).ok()?, &self.bytes[start..start + len]))
    }

    /// First key >= the requested key, allowing both point probes and bounded
    /// column/zone cursors without loading a decoded directory into memory.
    pub fn lower_bound(&self, key: Key) -> usize {
        let (mut lo, mut hi) = (0, self.count);
        let packed = self.kind == Kind::PackedRecords;
        let slot = self.slot_width();
        let column_base = if packed { read_u32(self.bytes, 48) } else { 0 };
        let zone_base = if packed {
            u64::from(read_u32(self.bytes, 52))
        } else {
            0
        };
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let offset = HEADER + mid * slot;
            // The view already validated every key and payload. A search
            // needs only the key, and usually only its leading component;
            // avoid decoding/checking a payload slice at every comparison.
            let column = if packed {
                column_base + u32::from(self.bytes[offset])
            } else {
                read_u32(self.bytes, offset)
            };
            let ordering = column.cmp(&key.column).then_with(|| {
                let kind = self.bytes[offset + if packed { 1 } else { 4 }];
                kind.cmp(&key.kind).then_with(|| {
                    let zone = if packed {
                        zone_base + u64::from(read_u16(self.bytes, offset + 2))
                    } else {
                        read_u64(self.bytes, offset + 8)
                    };
                    zone.cmp(&key.zone)
                })
            });
            if ordering.is_lt() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
}

/// Encode one physical page. Callers split logical batches to fit; a value
/// larger than one page must use bounded overflow pages, never truncate.
pub(super) fn encode(
    identity: Identity,
    kind: Kind,
    next: u64,
    entries: &[(Key, &[u8])],
) -> Result<Vec<u8>, QueryError> {
    let prefix = (next == 0)
        .then(|| packed_prefix(kind, entries.iter().map(|(key, _)| *key)))
        .flatten();
    let slot = if prefix.is_some() { PACKED_SLOT } else { SLOT };
    if identity.generation == 0
        || entries.len() > (PAGE_SIZE - HEADER) / slot
        || kind == Kind::PackedRecords
    {
        return Err(corrupt("invalid generation or entry count"));
    }
    let lower = HEADER + entries.len() * slot;
    let payload_bytes = entries
        .iter()
        .try_fold(0usize, |sum, (_, bytes)| sum.checked_add(bytes.len()))
        .ok_or_else(|| corrupt("payload length overflow"))?;
    if payload_bytes > PAGE_SIZE - lower {
        return Err(corrupt("entries do not fit a page"));
    }
    let mut bytes = vec![0u8; PAGE_SIZE];
    write_u32(&mut bytes, 0, MAGIC);
    write_u16(&mut bytes, 4, VERSION);
    bytes[6] = if prefix.is_some() {
        Kind::PackedRecords as u8
    } else {
        kind as u8
    };
    write_u64(&mut bytes, 24, identity.storage);
    write_u64(&mut bytes, 32, identity.generation);
    write_u64(&mut bytes, 40, identity.page);
    if let Some(prefix) = prefix {
        write_u32(&mut bytes, 48, prefix.column);
        write_u32(&mut bytes, 52, prefix.zone);
    } else {
        write_u64(&mut bytes, 48, next);
    }
    write_u16(&mut bytes, 56, entries.len() as u16);
    write_u16(&mut bytes, 58, lower as u16);
    write_u16(&mut bytes, 60, (PAGE_SIZE - payload_bytes) as u16);
    let mut upper = PAGE_SIZE;
    let mut prior = None;
    for (index, (key, value)) in entries.iter().enumerate() {
        Kind::try_from(key.kind)?;
        if prior.is_some_and(|prior| prior >= *key) {
            return Err(corrupt("keys are not strictly ordered"));
        }
        prior = Some(*key);
        let offset = HEADER + index * slot;
        upper -= value.len();
        if let Some(prefix) = prefix {
            bytes[offset] = (key.column - prefix.column) as u8;
            bytes[offset + 1] = key.kind;
            write_u16(
                &mut bytes,
                offset + 2,
                (key.zone - u64::from(prefix.zone)) as u16,
            );
        } else {
            key.write(&mut bytes, offset);
        }
        write_u16(&mut bytes, offset + slot - 4, upper as u16);
        write_u16(&mut bytes, offset + slot - 2, value.len() as u16);
        bytes[upper..upper + value.len()].copy_from_slice(value);
    }
    refresh_page_checksum(&mut bytes);
    Ok(bytes)
}

#[derive(Clone, Copy)]
struct CostProbeState {
    file_hash: u64,
    misses: u64,
}

thread_local! {
    static COST_PROBE: std::cell::Cell<Option<CostProbeState>> = const { std::cell::Cell::new(None) };
}

/// Costing records actual cold accesses on this thread. Nested planner work
/// restores its caller's scope; the marker prevents moving a scope to a worker.
pub(in crate::storage::core) struct CostProbeScope {
    file_hash: u64,
    previous: Option<CostProbeState>,
    _thread: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl CostProbeScope {
    pub fn misses(&self) -> u64 {
        COST_PROBE.with(|slot| {
            slot.get()
                .filter(|state| state.file_hash == self.file_hash)
                .map_or(0, |state| state.misses)
        })
    }
}

impl Drop for CostProbeScope {
    fn drop(&mut self) {
        COST_PROBE.with(|slot| slot.set(self.previous));
    }
}

/// A lightweight file handle: no page images, decoded directory, or summary
/// payloads live here. Dropping the handle leaves dirty pages owned by the
/// shared pool. Callers must serialize read/modify/write and publication.
pub(super) struct PageFile {
    key: Arc<BufferPageFileKey>,
    pool: Arc<BufferPool>,
    log: Arc<crate::wal::LogMutex>,
    durability: DurabilityMode,
    storage: u64,
    generation: u64,
    relative: String,
}

impl PageFile {
    pub fn new(
        path: PathBuf,
        storage: u64,
        generation: u64,
        pool: Arc<BufferPool>,
        log: Arc<crate::wal::LogMutex>,
        durability: DurabilityMode,
    ) -> Result<Self, QueryError> {
        if generation == 0 {
            return Err(corrupt("zero file generation"));
        }
        let relative = crate::storage::naming::wal_relative_object_path(&path);
        Ok(Self {
            key: Arc::new(BufferPool::zone_map_file_key(path, storage, generation)),
            pool,
            log,
            durability,
            storage,
            generation,
            relative,
        })
    }

    pub fn cost_probe_scope(&self) -> CostProbeScope {
        let file_hash = self.key.hash;
        let previous = COST_PROBE.with(|slot| {
            slot.replace(Some(CostProbeState {
                file_hash,
                misses: 0,
            }))
        });
        CostProbeScope {
            file_hash,
            previous,
            _thread: std::marker::PhantomData,
        }
    }

    pub fn identity(&self, page: u64) -> Identity {
        Identity {
            storage: self.storage,
            generation: self.generation,
            page,
        }
    }

    fn page_key(&self, page: u64) -> Result<PageKey, QueryError> {
        // Also reject the final partial page of the u64 file-offset domain.
        page.checked_add(1)
            .and_then(|end| end.checked_mul(PAGE_SIZE as u64))
            .ok_or_else(|| corrupt("page offset overflow"))?;
        Ok(PageKey {
            file: Arc::clone(&self.key),
            page_id: page,
        })
    }

    pub fn visit<T>(
        &self,
        page: u64,
        intent: AccessIntent,
        f: impl FnOnce(View<'_>) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let key = self.page_key(page)?;
        let measuring = COST_PROBE.with(|slot| {
            slot.get()
                .is_some_and(|state| state.file_hash == self.key.hash)
        });
        // The ordinary execution path does not perform this extra lookup.
        // Count repeated misses under eviction pressure: they are real I/O,
        // unlike repeated probes of a page that is still resident.
        let cold = measuring && self.pool.lookup_frame(&key)?.is_none();
        let pinned = self.pool.pin_page_with_intent(key, intent)?;
        if cold {
            COST_PROBE.with(|slot| {
                if let Some(mut state) = slot.get().filter(|state| state.file_hash == self.key.hash)
                {
                    state.misses = state.misses.saturating_add(1);
                    slot.set(Some(state));
                }
            });
        }
        let frame = &pinned.frame;
        let result = self.pool.with_finalized_frame_bytes(frame, |bytes| {
            // The epoch is read while the raw bytes are locked. Reuse the
            // structured-page validation slot: a frame's file kind never
            // changes, so an Index reader cannot publish a ZoneMap token.
            // store_page/mark_dirty revoke the token under that same lock.
            let epoch = frame.dirty_epoch.load(Ordering::Acquire);
            let view = if frame.index_header_validated_epoch.load(Ordering::Acquire) == epoch {
                View {
                    bytes,
                    kind: Kind::try_from(bytes[6])?,
                    count: read_u16(bytes, 56) as usize,
                }
            } else {
                let started = Instant::now();
                let decoded = View::open(bytes, self.identity(page));
                ZONE_MAP_METADATA_DECODE_NS.fetch_add(
                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                    Ordering::Relaxed,
                );
                let view = decoded?;
                frame
                    .index_header_validated_epoch
                    .store(epoch, Ordering::Release);
                view
            };
            f(view)
        });
        if matches!(intent, AccessIntent::Scan | AccessIntent::SmallScan) {
            self.pool.raw_scan_pages.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Redo-only: rolled-back row changes may leave wider summaries. WAL is
    /// appended before making the new page dirty; publication/commit callers
    /// must order their record after the returned LSN. No private file writes.
    pub fn write(
        &self,
        page: u64,
        kind: Kind,
        next: u64,
        entries: &[(Key, &[u8])],
    ) -> Result<u64, QueryError> {
        Ok(self
            .write_prepared(page, kind, || {
                encode(self.identity(page), kind, next, entries).map(Some)
            })?
            .expect("encoding always returns an image"))
    }

    /// The preparation closure runs under a scratch reservation but before
    /// acquiring the publication gate or page write latch. None requests the
    /// caller's structural-update fallback without emitting WAL.
    pub(super) fn write_prepared(
        &self,
        page: u64,
        kind: Kind,
        prepare: impl FnOnce() -> Result<Option<Vec<u8>>, QueryError>,
    ) -> Result<Option<u64>, QueryError> {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        let key = self.page_key(page)?;
        // One encoding image plus WAL's owned image/compression workspace.
        // Bound this independently from the buffer pool's resident charge.
        // Writing an unready root is also the low-memory DML fallback: it
        // must reclaim evictable pages instead of using optional-cache
        // admission. Reserve before the WAL publication gate/frame latches.
        // A failed summary update can still release scratch and invalidate
        // its root, so leave terminal OOM propagation to the DML caller.
        let _scratch = ControlledMemoryCharge::try_new_recoverable(
            ControlledMemoryCategory::StorageScratch,
            PAGE_SIZE * 8,
        )?;
        let Some(image) = prepare()? else {
            return Ok(None);
        };
        let mut pending = Some(image);
        // Most resident updates have a tiny dictionary delta. Build a full
        // image only when it can materially reduce that delta or when recovery
        // needs a new anchor. Compression stays outside the global WAL mutex.
        let full_image =
            |after: &[u8]| WalPayload::full_page_image(self.relative.clone(), page, after.to_vec());
        // A checkpoint may capture an intermediate revoked tree, but may
        // never miss an appended record whose dirty frame is not yet visible.
        // Normal read admission lets a waiting checkpoint stop new writers.
        let _publication = self.pool.page_wal_publish_gate.read();
        if self.durability != DurabilityMode::Memory {
            if let Some(lsn) = self.pool.try_replace_resident_logged_page(
                &key,
                |before, anchored, validated| {
                    let mut after = pending.take().expect("replacement callback runs once");
                    let delta_eligible = kind != Kind::Meta
                        && anchored
                        && (validated || View::open(before, self.identity(page)).is_ok());
                    // A valid dirty epoch starts with an FPI, and its first-dirty
                    // LSN remains the recovery floor until writeback completes.
                    // Clean/cold pages always restart with a complete image.
                    // The prepared image has a canonical zero LSN. Redo
                    // stamps the record LSN after validating/decompressing
                    // it, just as for FPIs. Compression and CRCs therefore
                    // do not serialize unrelated writers on the WAL lock.
                    let delta = delta_eligible
                        .then(|| {
                            WalPayload::page_delta(
                                self.relative.clone(),
                                page,
                                page_lsn(before),
                                before,
                                &after,
                            )
                        })
                        .flatten();
                    let payload = match delta {
                        Some(delta) => {
                            let delta_bytes = match &delta {
                                WalPayload::PageDelta {
                                    compressed_after, ..
                                } => compressed_after.len() + 16,
                                _ => unreachable!(),
                            };
                            // Below 1/64 page, a second full-page codec
                            // pass costs more CPU than the bounded possible
                            // WAL saving. Large rewrites still compare both
                            // encodings to avoid WAL amplification.
                            if delta_bytes <= PAGE_SIZE / 64 {
                                delta
                            } else {
                                let full = full_image(&after);
                                let full_bytes = match &full {
                                    WalPayload::FullPageImage { after, .. } => after.len(),
                                    WalPayload::FullPageImageCompressed {
                                        compressed_after,
                                        ..
                                    } => compressed_after.len(),
                                    _ => unreachable!(),
                                };
                                if delta_bytes.saturating_mul(8) < full_bytes.saturating_mul(7) {
                                    delta
                                } else {
                                    full
                                }
                            }
                        }
                        None => full_image(&after),
                    };
                    let mut log = self.log.lock();
                    let lsn = log.reserve_lsn()?;
                    if page_lsn(before) >= lsn {
                        return Err(corrupt("replacement LSN does not follow its base"));
                    }
                    log.append_reserved(lsn, 0, payload)?;
                    drop(log);
                    set_page_lsn_if_formatted(&mut after, lsn);
                    Ok((lsn, after))
                },
            )? {
                return Ok(Some(lsn));
            }
        }
        let mut bytes = pending.expect("nonresident path keeps the replacement image");
        let lsn = if self.durability == DurabilityMode::Memory {
            0
        } else {
            let full = full_image(&bytes);
            let _admission =
                crate::engine::executor::mem_budget::StorageLatchAdmissionScope::enter();
            let mut log = self.log.lock();
            let lsn = log.reserve_lsn()?;
            set_page_lsn_if_formatted(&mut bytes, lsn);
            log.append_reserved(lsn, 0, full)?;
            lsn
        };
        self.pool.store_page(key, bytes, lsn)?;
        Ok(Some(lsn))
    }

    pub fn flush(&self) -> Result<(), QueryError> {
        self.pool.flush_file(&self.key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zone_map_page_revocation_reclaims_memory_before_rejecting_write() {
        use crate::engine::executor::mem_budget::{
            admission_reclaim_snapshot, controlled_memory_snapshot, operational_memory_limit_bytes,
            with_test_effective_memory_limit, ControlledMemoryCategory, ControlledMemoryCharge,
            ReclaimDatabaseScope,
        };
        fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let mut config = StorageConfig::from_env();
        config.buffer_pool_pages = 2048;
        config.durability = DurabilityMode::Strict;
        let engine =
            crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                config,
            )
            .unwrap();
        let db = engine.db_for_tests();
        let file = PageFile::new(
            dir.path().join("pressure.zm"),
            17,
            1,
            Arc::clone(&db.buffer_pool),
            Arc::clone(&db.log),
            DurabilityMode::Strict,
        )
        .unwrap();
        let store = mutable::Store::recover_unknown(file, 99, 64).unwrap();
        // More than admission's 16 MiB minimum, with real dirty, evictable
        // pages. Account for other live owners without allocating huge data.
        for page in 2..1200 {
            store
                .file
                .write(page, Kind::Bounds, 0, &[(key(page), b"value")])
                .unwrap();
        }
        let _scope = ReclaimDatabaseScope::enter(db);
        with_test_effective_memory_limit(64 * 1024 * 1024, || {
            let filler = ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::Executor,
                operational_memory_limit_bytes() - controlled_memory_snapshot().0 - PAGE_SIZE * 4,
            )
            .unwrap();
            assert!(ControlledMemoryCharge::try_new_optional(
                ControlledMemoryCategory::StorageScratch,
                PAGE_SIZE * 8,
            )
            .is_none());
            let before = admission_reclaim_snapshot();
            let lsn = store
                .invalidate()
                .expect("revocation must reclaim evictable pages");
            let after = admission_reclaim_snapshot();
            assert!(after.retry_successes > before.retry_successes);
            assert!(after.buffer_bytes > before.buffer_bytes);
            assert!(controlled_memory_snapshot().0 <= operational_memory_limit_bytes());
            drop(filler);
            store.file.flush().unwrap();
            db.buffer_pool.flush_and_evict_all().unwrap();
            let (_, ready) = directory::Root::load(&store.file, 99, 64).unwrap();
            assert!(!ready, "revocation must survive eviction and disk reload");
            LogManager::sync_durable_to(&db.log, lsn).unwrap();
            assert!(LogManager::scan(dir.path())
                .unwrap()
                .records
                .iter()
                .any(|record| record.lsn == lsn));
        });
    }

    #[test]
    fn zone_map_page_packed_keys_preserve_full_domain_and_reject_damage() {
        let entries: Vec<_> = (0..1800)
            .map(|zone| {
                (
                    Key {
                        column: 1000,
                        kind: Kind::Bounds as u8,
                        zone,
                    },
                    &[42u8][..],
                )
            })
            .collect();
        let bytes = encode(identity(), Kind::Records, 0, &entries).unwrap();
        assert_eq!(bytes[6], Kind::PackedRecords as u8);
        let view = View::open(&bytes, identity()).unwrap();
        assert_eq!(view.slot_width(), PACKED_SLOT);
        for (index, expected) in entries.iter().enumerate() {
            assert_eq!(view.entry(index), Some(*expected));
        }
        let wide = [
            (
                Key {
                    column: 0,
                    kind: Kind::Bounds as u8,
                    zone: 0,
                },
                &b"a"[..],
            ),
            (
                Key {
                    column: u32::MAX,
                    kind: Kind::Membership as u8,
                    zone: u64::MAX,
                },
                &b"b"[..],
            ),
        ];
        let full = encode(identity(), Kind::Records, 0, &wide).unwrap();
        assert_eq!(full[6], Kind::Records as u8);
        let view = View::open(&full, identity()).unwrap();
        assert_eq!(view.entry(1), Some(wide[1]));
        for mutation in 0..3 {
            let mut damaged = bytes.clone();
            match mutation {
                0 => {
                    write_u32(&mut damaged, 48, u32::MAX);
                    damaged[HEADER] = 1;
                }
                1 => write_u16(&mut damaged, HEADER + 4, 0),
                _ => write_u16(&mut damaged, HEADER + PACKED_SLOT + 2, 0),
            }
            refresh_page_checksum(&mut damaged);
            assert!(View::open(&damaged, identity()).is_err());
        }
    }

    fn identity() -> Identity {
        Identity {
            storage: 17,
            generation: 23,
            page: 5,
        }
    }

    fn key(zone: u64) -> Key {
        Key {
            column: 7,
            kind: Kind::Bounds as u8,
            zone,
        }
    }

    #[test]
    fn zone_map_page_header_golden_and_standard_lsn() {
        let mut bytes = encode(identity(), Kind::Bounds, 19, &[(key(3), b"abc")]).unwrap();
        assert_eq!(&bytes[..8], &[0x31, 0x50, 0x4d, 0x5a, 3, 0, 2, 0]);
        assert_eq!(&bytes[24..32], &17u64.to_le_bytes());
        assert_eq!(&bytes[32..40], &23u64.to_le_bytes());
        assert_eq!(&bytes[40..48], &5u64.to_le_bytes());
        assert_eq!(&bytes[56..64], &[1, 0, 84, 0, 253, 63, 0, 0]);
        set_page_lsn_if_formatted(&mut bytes, 991);
        assert_eq!(read_u64(&bytes, 8), 991);
        let view = View::open(&bytes, identity()).unwrap();
        assert_eq!(view.next(), 19);
        assert_eq!(view.kind(), Kind::Bounds);
        assert_eq!(view.entry(0), Some((key(3), b"abc".as_slice())));
    }

    #[test]
    fn zone_map_page_rejects_corruption_even_with_fresh_checksum() {
        let pristine = encode(
            identity(),
            Kind::Bounds,
            0,
            &[(key(1), b"ab"), (key(2), b"c")],
        )
        .unwrap();
        for (offset, value) in [
            (4, 255),
            (6, 255),
            (7, 1),
            (20, 1),
            (24, 0),
            (32, 0),
            (40, 0),
            (56, 255),
            (58, 0),
            (60, 0),
            (62, 1),
            (69, 1),
            (80, 0),
            (82, 255),
        ] {
            let mut bytes = pristine.clone();
            bytes[offset] = value;
            refresh_page_checksum(&mut bytes);
            assert!(View::open(&bytes, identity()).is_err(), "offset {offset}");
        }
        let mut torn = pristine.clone();
        torn[PAGE_SIZE - 1] ^= 1;
        assert!(View::open(&torn, identity()).is_err());
        for len in [0, 8, HEADER, PAGE_SIZE - 1] {
            assert!(View::open(&pristine[..len], identity()).is_err());
        }
    }

    #[test]
    fn zone_map_page_boundaries_and_search_match_independent_oracle() {
        let values: Vec<_> = (0..211u64)
            .map(|i| (key(i * 3), vec![i as u8; (i % 47) as usize]))
            .collect();
        let entries: Vec<_> = values.iter().map(|(k, v)| (*k, v.as_slice())).collect();
        let bytes = encode(identity(), Kind::Bounds, 0, &entries).unwrap();
        let view = View::open(&bytes, identity()).unwrap();
        assert_eq!(view.len(), entries.len());
        for i in 0..650 {
            let expected = entries
                .iter()
                .position(|(k, _)| *k >= key(i))
                .unwrap_or(entries.len());
            assert_eq!(view.lower_bound(key(i)), expected);
        }
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(view.entry(i), Some(*entry));
        }
        assert_eq!(view.entry(entries.len()), None);
        assert!(encode(identity(), Kind::Bounds, 0, &[(key(2), b""), (key(1), b"")]).is_err());
        assert!(encode(identity(), Kind::Bounds, 0, &[(key(1), b""), (key(1), b"")]).is_err());
        let full = vec![1; PAGE_SIZE - HEADER - SLOT];
        assert!(encode(identity(), Kind::Bounds, 0, &[(key(1), &full)]).is_ok());
        assert!(encode(
            identity(),
            Kind::Bounds,
            0,
            &[(key(1), &full), (key(2), b"")]
        )
        .is_err());
    }

    #[test]
    fn zone_map_page_search_orders_all_key_components_in_both_formats() {
        for (kind, base) in [(Kind::Records, 19), (Kind::Bounds, u64::MAX - 20)] {
            let mut entries = Vec::new();
            for column in [7, 9, 12] {
                for record_kind in [Kind::Bounds, Kind::Membership, Kind::ZoneMeta] {
                    for delta in [0, 3, 9] {
                        entries.push((
                            Key {
                                column,
                                kind: record_kind as u8,
                                zone: base + delta,
                            },
                            b"x".as_slice(),
                        ));
                    }
                }
            }
            let bytes = encode(identity(), kind, 0, &entries).unwrap();
            let view = View::open(&bytes, identity()).unwrap();
            assert_eq!(view.kind == Kind::PackedRecords, kind == Kind::Records);
            for column in 0..15 {
                for kind in 0..8 {
                    for zone in [0, base, base + 1, base + 3, base + 8, base + 9, u64::MAX] {
                        let key = Key { column, kind, zone };
                        assert_eq!(
                            view.lower_bound(key),
                            entries.partition_point(|(candidate, _)| *candidate < key)
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn zone_map_page_cache_identity_includes_generation() {
        let path = PathBuf::from("/tmp/mydb/zone-map-unified/identity.zm");
        let a = BufferPool::zone_map_file_key(path.clone(), 17, 1);
        let b = BufferPool::zone_map_file_key(path, 17, 2);
        assert_ne!(a, b);
        assert_ne!(a.hash, b.hash);
        assert_eq!(a.kind.slot(), 4);
        assert!(View::open(
            &encode(identity(), Kind::Bounds, 0, &[]).unwrap(),
            Identity {
                generation: 24,
                ..identity()
            }
        )
        .is_err());
    }

    #[test]
    fn zone_map_page_delta_reanchors_after_background_writeback() {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new(
            8,
            Arc::clone(&log),
            DurabilityMode::Strict,
            Duration::from_secs(5),
        );
        let file = PageFile::new(
            dir.path().join("delta.zm"),
            17,
            1,
            Arc::clone(&pool),
            Arc::clone(&log),
            DurabilityMode::Strict,
        )
        .unwrap();
        let key = Key {
            column: 0,
            kind: Kind::Bounds as u8,
            zone: 0,
        };
        let mut state = 0x123456789abcdefu64;
        let mut value = (0..8192)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();
        let first = file.write(1, Kind::Bounds, 0, &[(key, &value)]).unwrap();
        value[17] ^= 1;
        file.write(1, Kind::Bounds, 0, &[(key, &value)]).unwrap();
        let frame = pool
            .lookup_frame(&file.page_key(1).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(frame.first_dirty_lsn.load(Ordering::Acquire), first);
        file.flush().unwrap();
        assert!(!frame.dirty.load(Ordering::Acquire));
        assert_eq!(frame.first_dirty_lsn.load(Ordering::Acquire), 0);
        value[18] ^= 1;
        let anchor = file.write(1, Kind::Bounds, 0, &[(key, &value)]).unwrap();
        value[19] ^= 1;
        let last = file.write(1, Kind::Bounds, 0, &[(key, &value)]).unwrap();
        assert_eq!(frame.first_dirty_lsn.load(Ordering::Acquire), anchor);
        LogManager::sync_durable_to(&log, last).unwrap();
        let records = LogManager::scan(dir.path()).unwrap().records;
        assert_eq!(records.len(), 4);
        for (index, record) in records.iter().enumerate() {
            assert_eq!(
                matches!(record.payload, WalPayload::PageDelta { .. }),
                index % 2 == 1,
                "every clean-to-dirty epoch must begin with an independent FPI"
            );
        }
        file.visit(1, AccessIntent::Normal, |view| {
            assert_eq!(view.entry(0).unwrap().1, value.as_slice());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn zone_map_page_cost_probe_counts_cold_accesses_and_restores_nested_scope() {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new(
            4,
            Arc::clone(&log),
            DurabilityMode::Strict,
            Duration::from_secs(5),
        );
        let a = PageFile::new(
            dir.path().join("a.zm"),
            17,
            1,
            Arc::clone(&pool),
            Arc::clone(&log),
            DurabilityMode::Strict,
        )
        .unwrap();
        let b = PageFile::new(
            dir.path().join("b.zm"),
            18,
            1,
            Arc::clone(&pool),
            log,
            DurabilityMode::Strict,
        )
        .unwrap();
        a.write(0, Kind::Bounds, 0, &[(key(0), b"a")]).unwrap();
        a.write(1, Kind::Bounds, 0, &[(key(1), b"b")]).unwrap();
        b.write(0, Kind::Bounds, 0, &[(key(0), b"c")]).unwrap();
        pool.flush_and_evict_all().unwrap();
        let outer = a.cost_probe_scope();
        a.visit(0, AccessIntent::Normal, |_| Ok(())).unwrap();
        a.visit(0, AccessIntent::Normal, |_| Ok(())).unwrap();
        assert_eq!(outer.misses(), 1);
        {
            let inner = b.cost_probe_scope();
            b.visit(0, AccessIntent::Normal, |_| Ok(())).unwrap();
            assert_eq!(inner.misses(), 1);
        }
        assert_eq!(outer.misses(), 1);
        a.visit(1, AccessIntent::Normal, |_| Ok(())).unwrap();
        assert_eq!(outer.misses(), 2);
        b.visit(0, AccessIntent::Normal, |_| Ok(())).unwrap();
        assert_eq!(outer.misses(), 2);
        drop(outer);
        let warm = a.cost_probe_scope();
        a.visit(0, AccessIntent::Normal, |_| Ok(())).unwrap();
        assert_eq!(warm.misses(), 0);
    }

    #[test]
    fn zone_map_page_revalidates_replaced_warm_frames() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new(
            4,
            Arc::clone(&log),
            DurabilityMode::Memory,
            Duration::from_secs(5),
        );
        let file = PageFile::new(
            dir.path().join("warm.zm"),
            17,
            23,
            Arc::clone(&pool),
            log,
            DurabilityMode::Memory,
        )
        .unwrap();
        file.write(0, Kind::Bounds, 0, &[(key(0), b"a")]).unwrap();
        for _ in 0..4 {
            file.visit(0, AccessIntent::Normal, |view| {
                assert_eq!(view.entry(0).unwrap().1, b"a");
                Ok(())
            })
            .unwrap();
        }
        let page_key = file.page_key(0).unwrap();
        let frame = pool.lookup_frame(&page_key).unwrap().unwrap();
        assert_eq!(
            frame.index_header_validated_epoch.load(Ordering::Acquire),
            frame.dirty_epoch.load(Ordering::Acquire)
        );
        let mut invalid = encode(file.identity(0), Kind::Bounds, 0, &[(key(0), b"b")]).unwrap();
        write_u16(&mut invalid, 58, 0);
        refresh_page_checksum(&mut invalid);
        pool.store_page(page_key, invalid, 0).unwrap();
        assert!(file.visit(0, AccessIntent::Normal, |_| Ok(())).is_err());
        file.write(0, Kind::Bounds, 0, &[(key(0), b"c")]).unwrap();
        file.visit(0, AccessIntent::Normal, |view| {
            assert_eq!(view.entry(0).unwrap().1, b"c");
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn zone_map_page_uses_shared_eviction_and_reopens_without_decoded_cache() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new(
            4,
            Arc::clone(&log),
            DurabilityMode::Strict,
            Duration::from_secs(5),
        );
        let path = crate::storage::naming::table_object_path(dir.path(), "test", "paged", 17, "zm");
        let file = PageFile::new(
            path.clone(),
            17,
            23,
            Arc::clone(&pool),
            Arc::clone(&log),
            DurabilityMode::Strict,
        )
        .unwrap();
        let value = vec![91; 512];
        let mut previous_lsn = 0;
        for page in 0..40 {
            let lsn = file
                .write(page, Kind::Bounds, 0, &[(key(page), &value)])
                .unwrap();
            assert!(lsn > previous_lsn);
            previous_lsn = lsn;
        }
        file.flush().unwrap();
        assert!(pool.snapshot().evictions > 0);
        assert!(pool.kind_used_pages[4].load(Ordering::Relaxed) <= 4);
        pool.flush_and_evict_all().unwrap();
        assert_eq!(pool.kind_used_pages[4].load(Ordering::Relaxed), 0);
        drop(file);
        let reopened =
            PageFile::new(path, 17, 23, Arc::clone(&pool), log, DurabilityMode::Strict).unwrap();
        for page in (0..40).rev() {
            reopened
                .visit(page, AccessIntent::Scan, |view| {
                    assert_eq!(view.entry(0), Some((key(page), value.as_slice())));
                    Ok(())
                })
                .unwrap();
        }
        assert!(reopened
            .visit(u64::MAX, AccessIntent::Normal, |_| Ok(()))
            .is_err());
    }
}
