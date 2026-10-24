//! Mutable paged summary tree. Structural updates revoke the root before
//! modifying directory pages and publish it afterwards. Shrinking updates
//! exclude readers for the whole batch. Conservative, nonstructural widening
//! uses a shared root guard and leaf writer stripes; each leaf is atomic redo.

use super::directory::{Root, MAX_HEIGHT, ROOT_KEY};
use super::*;
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};

type Entries = Vec<(Key, Vec<u8>)>;

#[derive(Clone, Copy)]
enum SplitIntent {
    Balanced,
    Append,
    PrefixBoundary(usize),
}

struct Change {
    index: usize,
    inserted: bool,
    was_pending: bool,
}

#[derive(Clone, Copy)]
struct LeafTarget {
    page: u64,
    lower: Option<Key>,
    upper: Option<Key>,
}

pub(in crate::storage::core) struct StorageSnapshot {
    pub file_bytes: u64,
    pub live_bytes: u64,
    pub frame_count: u64,
    pub resident_bytes: u64,
    pub dirty_bytes: u64,
    pub charged_bytes: u64,
    pub pending: bool,
}

pub(in crate::storage::core) struct Store {
    pub(super) file: PageFile,
    state: parking_lot::RwLock<Option<Root>>,
    // Nonstructural widening keeps a shared directory guard. Serialize only
    // writers of the same leaf (or stripe), never a whole multi-leaf batch.
    leaf_writers: [parking_lot::Mutex<()>; 64],
    schema: u64,
    zone_pages: u32,
    revision: AtomicU64,
    /// Recently read records. Every foreground DML proves its widening is a
    /// no-op (the common case) from these instead of descending the paged
    /// tree through the buffer pool several times per row.
    records: RecordCache,
}

const RECORD_CACHE_SHARDS: usize = 32;
/// Bytes one shard may retain before it is emptied; bounds a store to 8 MiB.
const RECORD_CACHE_SHARD_BYTES: usize = 256 * 1024;
/// Map slot, key and `Arc` header overhead charged per cached record.
const RECORD_CACHE_ENTRY_OVERHEAD: usize = 64;

/// Bounded read cache of summary records (including proven absence),
/// charged to the unified memory governor; a denied charge simply skips
/// caching. Coherence: every writer forgets the keys it replaced after the
/// replacement is visible and before releasing the latch that serializes it.
/// Forgetting bumps the shard generation, and a reader caches what it read
/// only if the generation it sampled before reading is unchanged, so a read
/// that raced a replacement can never install the superseded value.
struct RecordCache {
    shards: Box<[CachePadded<parking_lot::Mutex<RecordShard>>]>,
}

#[derive(Default)]
struct RecordShard {
    generation: u64,
    bytes: usize,
    entries: crate::util::fast_hash::AhHashMap<Key, Option<Arc<[u8]>>>,
    charge: Option<ControlledMemoryCharge>,
}

impl Default for RecordCache {
    fn default() -> Self {
        Self {
            shards: (0..RECORD_CACHE_SHARDS)
                .map(|_| CachePadded::new(parking_lot::Mutex::new(RecordShard::default())))
                .collect(),
        }
    }
}

impl RecordCache {
    fn shard(&self, key: Key) -> &parking_lot::Mutex<RecordShard> {
        let hash = (u64::from(key.column) << 8 | u64::from(key.kind))
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ key.zone.wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
        &self.shards[(hash >> 32) as usize % RECORD_CACHE_SHARDS]
    }

    /// The cached record, or the shard generation to present to `insert`.
    fn lookup(&self, key: Key) -> Result<Option<Arc<[u8]>>, u64> {
        let shard = self.shard(key).lock();
        shard.entries.get(&key).cloned().ok_or(shard.generation)
    }

    fn insert(&self, key: Key, generation: u64, value: Option<&[u8]>) {
        let size = value.map_or(0, <[u8]>::len) + RECORD_CACHE_ENTRY_OVERHEAD;
        let mut shard = self.shard(key).lock();
        if shard.generation != generation || shard.entries.contains_key(&key) {
            return;
        }
        if shard.bytes + size > RECORD_CACHE_SHARD_BYTES {
            shard.clear();
            if size > RECORD_CACHE_SHARD_BYTES {
                return;
            }
        }
        let charged = match shard.charge.as_mut() {
            Some(charge) => charge.try_grow_optional(size),
            None => {
                shard.charge = ControlledMemoryCharge::try_new_optional(
                    ControlledMemoryCategory::DerivedCache,
                    size,
                );
                shard.charge.is_some()
            }
        };
        if !charged {
            return;
        }
        shard.bytes += size;
        shard.entries.insert(key, value.map(Arc::from));
    }

    fn forget(&self, key: Key) {
        let mut shard = self.shard(key).lock();
        shard.generation = shard.generation.wrapping_add(1);
        if let Some(value) = shard.entries.remove(&key) {
            let size = value.map_or(0, |value| value.len()) + RECORD_CACHE_ENTRY_OVERHEAD;
            shard.bytes -= size;
            if let Some(charge) = shard.charge.as_mut() {
                charge.shrink(size);
            }
        }
    }

    fn clear(&self) {
        for shard in self.shards.iter() {
            shard.lock().clear();
        }
    }
}

impl RecordShard {
    fn clear(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.entries = Default::default();
        self.bytes = 0;
        self.charge = None;
    }
}

/// Forgets a batch's keys when dropped. Declared after the state write
/// guard, it runs first on every exit path, while readers are still excluded.
struct ForgetOnDrop<'a> {
    records: &'a RecordCache,
    entries: &'a [(Key, Vec<u8>)],
}

impl Drop for ForgetOnDrop<'_> {
    fn drop(&mut self) {
        for (key, _) in self.entries {
            self.records.forget(*key);
        }
    }
}

impl Store {
    pub fn open_existing(file: PageFile, schema: u64, zone_pages: u32) -> Result<Self, QueryError> {
        let (root, ready) = Root::load(&file, schema, zone_pages)?;
        if ready {
            file.visit(root.page, AccessIntent::Normal, |view| {
                if root.height == 0 {
                    directory::validate_leaf(&view, None, None)
                } else {
                    directory::validate_node(&view, Kind::Directory, None, None)
                }
            })?;
        }
        Ok(Self {
            file,
            state: parking_lot::RwLock::new(ready.then_some(root)),
            leaf_writers: std::array::from_fn(|_| parking_lot::Mutex::new(())),
            schema,
            zone_pages,
            revision: AtomicU64::new(root.revision),
            records: RecordCache::default(),
        })
    }

    /// Revoke a missing/corrupt derived baseline without deleting or replacing
    /// an authoritative file. A new empty root cannot disprove any heap page.
    pub fn recover_unknown(
        file: PageFile,
        schema: u64,
        zone_pages: u32,
    ) -> Result<Self, QueryError> {
        let revision = file.log.lock().last_lsn();
        let store = Self {
            file,
            state: parking_lot::RwLock::new(None),
            leaf_writers: std::array::from_fn(|_| parking_lot::Mutex::new(())),
            schema,
            zone_pages,
            revision: AtomicU64::new(revision),
            records: RecordCache::default(),
        };
        store.reset_unknown()?;
        Ok(store)
    }

    pub fn root_snapshot(&self) -> Option<Root> {
        *self.state.read()
    }
    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }
    pub fn trusted(&self) -> bool {
        self.state.read().is_some()
    }
    pub fn missing_is_empty(&self) -> bool {
        self.state.read().is_some_and(|root| root.missing_is_empty)
    }

    pub fn set_missing_is_empty(&self, value: bool) -> Result<(), QueryError> {
        let mut guard = self.state.write();
        let mut root = guard.ok_or_else(|| corrupt("untrusted mutable summary root"))?;
        if root.missing_is_empty == value {
            return Ok(());
        }
        root.missing_is_empty = value;
        root.revision = self.revision.fetch_add(1, Ordering::AcqRel) + 1;
        *guard = None;
        self.publish(root, true)?;
        *guard = Some(root);
        Ok(())
    }

    pub fn flush(&self) -> Result<(), QueryError> {
        self.file.flush()
    }
    pub fn discard(&self) -> Result<(), QueryError> {
        self.file.pool.discard_file(&self.file.key)
    }

    pub fn storage_snapshot(&self) -> StorageSnapshot {
        let root = self.root_snapshot();
        let (resident, dirty) = self
            .file
            .pool
            .file_residency_stats(&self.file.key)
            .unwrap_or_default();
        let resident_bytes = resident.saturating_mul(PAGE_SIZE as u64);
        let dirty_bytes = dirty.saturating_mul(PAGE_SIZE as u64);
        let allocated = root.map_or(0, |root| root.next_page);
        let file_bytes = fs::metadata(&self.file.key.path)
            .map(|m| m.len())
            .unwrap_or(0)
            .max(allocated.saturating_mul(PAGE_SIZE as u64));
        StorageSnapshot {
            file_bytes,
            live_bytes: allocated.saturating_mul(PAGE_SIZE as u64),
            frame_count: allocated,
            resident_bytes,
            dirty_bytes,
            charged_bytes: resident_bytes,
            pending: root.is_none_or(|root| !root.missing_is_empty || root.pending_records != 0),
        }
    }
    #[cfg(test)]
    pub fn create(file: PageFile, schema: u64, zone_pages: u32) -> Result<Self, QueryError> {
        let root = directory::Builder::new(&file, schema, zone_pages)?.finish()?;
        Ok(Self {
            file,
            state: parking_lot::RwLock::new(Some(root)),
            leaf_writers: std::array::from_fn(|_| parking_lot::Mutex::new(())),
            schema,
            zone_pages,
            revision: AtomicU64::new(root.revision),
            records: RecordCache::default(),
        })
    }

    #[cfg(test)]
    pub fn open(file: PageFile, schema: u64, zone_pages: u32) -> Result<Self, QueryError> {
        let root = Root::open(&file, schema, zone_pages)?;
        // Validate the pointed root as well as metadata. Missing baselines
        // must not become trusted simply because a later root FPI survived.
        file.visit(root.page, AccessIntent::Normal, |view| {
            if root.height == 0 {
                directory::validate_leaf(&view, None, None)
            } else {
                directory::validate_node(&view, Kind::Directory, None, None)
            }
        })?;
        Ok(Self {
            file,
            state: parking_lot::RwLock::new(Some(root)),
            leaf_writers: std::array::from_fn(|_| parking_lot::Mutex::new(())),
            schema,
            zone_pages,
            revision: AtomicU64::new(root.revision),
            records: RecordCache::default(),
        })
    }

    pub fn cost_probe_scope(&self) -> CostProbeScope {
        self.file.cost_probe_scope()
    }

    pub fn visit_matches(
        &self,
        zones: impl IntoIterator<Item = u64>,
        predicate: &ZonePredicateProgram,
        safe_read_scn: u64,
        consume: impl FnMut(u64, bool, u64) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let guard = self.state.read();
        if guard.is_none() {
            return Err(corrupt("untrusted mutable summary root"));
        }
        summary::Reader::open_and_visit_matches(
            &self.file,
            self.schema,
            self.zone_pages,
            safe_read_scn,
            zones,
            predicate,
            consume,
        )
    }

    pub fn get<T>(
        &self,
        key: Key,
        f: impl FnOnce(Option<&[u8]>) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let guard = self.state.read();
        let root = guard.ok_or_else(|| corrupt("untrusted mutable summary root"))?;
        let generation = match self.records.lookup(key) {
            Ok(cached) => return f(cached.as_deref()),
            Err(generation) => generation,
        };
        let bytes = root.get(&self.file, key, |bytes| Ok(bytes.map(<[u8]>::to_vec)))?;
        self.records.insert(key, generation, bytes.as_deref());
        f(bytes.as_deref())
    }

    #[cfg(test)]
    pub fn visit_range(
        &self,
        start: Key,
        end: Key,
        f: impl FnMut(Key, &[u8]) -> Result<bool, QueryError>,
    ) -> Result<(), QueryError> {
        let guard = self.state.read();
        guard
            .ok_or_else(|| corrupt("untrusted mutable summary root"))?
            .visit_range(&self.file, start, end, f)
    }

    pub fn invalidate(&self) -> Result<u64, QueryError> {
        let mut guard = self.state.write();
        self.records.clear();
        // A previous WAL/admission failure may have cleared the runtime
        // root before its unready image reached the pool. Never treat None
        // as evidence that persistent revocation already succeeded.
        let mut root = guard.take().unwrap_or(Root {
            schema: self.schema,
            zone_pages: self.zone_pages,
            page: 1,
            height: 0,
            next_page: 2,
            records: 0,
            revision: 0,
            missing_is_empty: false,
            pending_records: 0,
        });
        root.revision = self.revision.fetch_add(1, Ordering::AcqRel) + 1;
        root.invalidate(&self.file)
    }

    pub fn reset_unknown(&self) -> Result<(), QueryError> {
        let mut guard = self.state.write();
        self.records.clear();
        // Reset is conservative: an empty directory means all heap ranges
        // are Unknown, not proof that the heap itself is empty.
        // The old root is revoked before reusing page ids. Existing readers
        // are excluded by state/data gates, and every replacement has newer
        // physical redo. Keep the file high-water, but do not append another
        // full generation on every ANALYZE.
        let next = 1u64;
        let mut root = Root {
            schema: self.schema,
            zone_pages: self.zone_pages,
            page: next,
            height: 0,
            next_page: next
                .checked_add(1)
                .ok_or_else(|| corrupt("page id overflow"))?,
            records: 0,
            revision: self.revision.fetch_add(1, Ordering::AcqRel) + 1,
            missing_is_empty: false,
            pending_records: 0,
        };
        *guard = None;
        self.publish(root, false)?;
        self.file.write(root.page, Kind::Records, 0, &[])?;
        self.publish(root, true)?;
        // Retain the new allocation high-water even for an empty map.
        root.records = 0;
        *guard = Some(root);
        Ok(())
    }

    pub fn apply(&self, entries: &[(Key, Vec<u8>)]) -> Result<(), QueryError> {
        self.apply_inner(entries, false)
    }

    /// The caller guarantees that every prefix of these replacements remains
    /// conservative for pruning (bounds widen, membership unions, or Unknown).
    /// With an unchanged directory and counters, each leaf may then be logged
    /// independently without revoking the root. Never use for shrinking rebuilds.
    pub fn apply_widening(&self, entries: &[(Key, Vec<u8>)]) -> Result<(), QueryError> {
        if self.try_patch_widening(entries)? {
            return Ok(());
        }
        self.apply_inner(entries, true)
    }

    /// Each published prefix is conservative. A shared root guard freezes
    /// directory intervals and counters while a leaf stripe protects the
    /// entire read/prepare/log/replace sequence against lost updates. Release
    /// it between leaves so unrelated zones can make progress concurrently.
    /// Structural changes retry under the existing exclusive publication path;
    /// that path rechecks already-published replacements and skips no-ops.
    fn try_patch_widening(&self, entries: &[(Key, Vec<u8>)]) -> Result<bool, QueryError> {
        if entries.is_empty() {
            return Ok(true);
        }
        for (key, bytes) in entries {
            if !matches!(key.kind, 0 | 2 | 3 | 4) || bytes.len() > PAGE_SIZE - HEADER - SLOT {
                return Err(corrupt("invalid mutable summary record"));
            }
        }
        let scratch_bytes = entries
            .len()
            .checked_mul(std::mem::size_of::<usize>() + std::mem::size_of::<Change>())
            .and_then(|bytes| bytes.checked_add(PAGE_SIZE * 24))
            .ok_or_else(|| corrupt("summary batch workspace overflow"))?;
        let _scratch = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::StorageScratch,
            scratch_bytes,
        )
        .ok_or_else(ZoneScratchLease::denied)?;
        let mut order: Vec<_> = (0..entries.len()).collect();
        order.sort_unstable_by_key(|&index| (entries[index].0, index));
        order.dedup_by(|later, earlier| {
            if entries[*later].0 == entries[*earlier].0 {
                *earlier = *later;
                true
            } else {
                false
            }
        });
        let guard = self.state.read();
        let root = guard.ok_or_else(|| corrupt("untrusted mutable summary root"))?;
        let mut changes = Vec::with_capacity(order.len());
        let mut first = 0;
        while first < order.len() {
            let target = self.leaf_for_update(root, entries[order[first]].0)?;
            let end = target.upper.map_or(order.len(), |upper| {
                first + order[first..].partition_point(|&index| entries[index].0 < upper)
            });
            if end == first {
                return Err(corrupt("invalid summary widening range"));
            }
            let _leaf = self.leaf_writers[target.page as usize % self.leaf_writers.len()].lock();
            changes.clear();
            let fits = self.file.visit(target.page, AccessIntent::Normal, |view| {
                directory::validate_leaf(&view, target.lower, target.upper)?;
                let mut growth = 0isize;
                for &index in &order[first..end] {
                    let (key, value) = &entries[index];
                    let Some((_, prior)) = view
                        .entry(view.lower_bound(*key))
                        .filter(|(candidate, _)| candidate == key)
                    else {
                        return Ok(false);
                    };
                    if prior == value.as_slice() {
                        continue;
                    }
                    let was_pending = record_pending(*key, prior);
                    if was_pending != record_pending(*key, value) {
                        return Ok(false);
                    }
                    growth += value.len() as isize - prior.len() as isize;
                    changes.push(Change {
                        index,
                        inserted: false,
                        was_pending,
                    });
                }
                let free = read_u16(view.bytes, 60)
                    .checked_sub(read_u16(view.bytes, 58))
                    .ok_or_else(|| corrupt("invalid leaf free space"))?;
                Ok(growth <= free as isize)
            })?;
            if !fits || (!changes.is_empty() && !self.patch_leaf(target, &changes, entries)?) {
                return Ok(false);
            }
            // Still under this leaf's writer stripe: the zone's next merger
            // must not prove a no-op against the replaced records.
            for change in &changes {
                self.records.forget(entries[change.index].0);
            }
            first = end;
        }
        Ok(true)
    }

    fn apply_inner(&self, entries: &[(Key, Vec<u8>)], widening: bool) -> Result<(), QueryError> {
        if entries.is_empty() {
            return Ok(());
        }
        for (key, bytes) in entries {
            if !matches!(key.kind, 0 | 2 | 3 | 4) || bytes.len() > PAGE_SIZE - HEADER - SLOT {
                return Err(corrupt("invalid mutable summary record"));
            }
        }
        // Indices borrow the caller's payloads. Sorting/deduplication never
        // clones the entire batch, and one leaf is the only decoded workspace.
        let index_bytes = entries
            .len()
            .checked_mul(std::mem::size_of::<usize>() + std::mem::size_of::<Change>())
            .and_then(|bytes| bytes.checked_add(PAGE_SIZE * 24))
            .ok_or_else(|| corrupt("summary batch workspace overflow"))?;
        let _scratch = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::StorageScratch,
            index_bytes,
        )
        .ok_or_else(ZoneScratchLease::denied)?;
        let mut order: Vec<_> = (0..entries.len()).collect();
        order.sort_unstable_by_key(|&index| (entries[index].0, index));
        // The greatest original position wins for duplicate keys, even when
        // that final value restores the old value and makes the batch neutral.
        order.dedup_by(|later, earlier| {
            if entries[*later].0 == entries[*earlier].0 {
                *earlier = *later;
                true
            } else {
                false
            }
        });
        let mut guard = self.state.write();
        let _forget = ForgetOnDrop {
            records: &self.records,
            entries,
        };
        let mut root = guard.ok_or_else(|| corrupt("untrusted mutable summary root"))?;
        let mut changes = Vec::with_capacity(order.len());
        let mut replacement_leaf = None;
        let mut replacements_fit = true;
        let mut multiple_leaves = false;
        // The sorted preflight borrows each leaf once for all of its keys.
        // Per-key get_with_hint avoided directory traversal but still pinned,
        // latched and validated the same leaf for every record in the batch.
        // The state write guard keeps these intervals stable until publication.
        let mut first = 0;
        while first < order.len() {
            let target = self.leaf_for_update(root, entries[order[first]].0)?;
            let end = target.upper.map_or(order.len(), |upper| {
                first + order[first..].partition_point(|&index| entries[index].0 < upper)
            });
            if end == first {
                return Err(corrupt("invalid summary preflight range"));
            }
            let prior_changes = changes.len();
            self.file.visit(target.page, AccessIntent::Normal, |view| {
                directory::validate_leaf(&view, target.lower, target.upper)?;
                let mut payload_growth = 0isize;
                for &index in &order[first..end] {
                    let (key, value) = &entries[index];
                    let prior = view
                        .entry(view.lower_bound(*key))
                        .filter(|(candidate, _)| candidate == key)
                        .map(|(_, bytes)| bytes);
                    if prior != Some(value.as_slice()) {
                        replacements_fit &= prior.is_some();
                        payload_growth +=
                            value.len() as isize - prior.map_or(0, <[u8]>::len) as isize;
                        changes.push(Change {
                            index,
                            inserted: prior.is_none(),
                            was_pending: prior.is_some_and(|prior| record_pending(*key, prior)),
                        });
                    }
                }
                let free = read_u16(view.bytes, 60)
                    .checked_sub(read_u16(view.bytes, 58))
                    .ok_or_else(|| corrupt("invalid leaf free space"))?;
                replacements_fit &= payload_growth <= free as isize;
                Ok(())
            })?;
            if changes.len() != prior_changes {
                if replacement_leaf.is_some() {
                    multiple_leaves = true;
                } else {
                    replacement_leaf = Some(target);
                }
            }
            first = end;
        }
        if changes.is_empty() {
            return Ok(());
        }
        let added = changes.iter().filter(|change| change.inserted).count() as u64;
        let removed_pending = changes.iter().filter(|change| change.was_pending).count() as u64;
        let added_pending = changes
            .iter()
            .filter(|change| {
                let (key, value) = &entries[change.index];
                record_pending(*key, value)
            })
            .count() as u64;
        let records = root
            .records
            .checked_add(added)
            .ok_or_else(|| corrupt("record count overflow"))?;
        let pending_records = root
            .pending_records
            .checked_sub(removed_pending)
            .and_then(|count| count.checked_add(added_pending))
            .ok_or_else(|| corrupt("pending record count mismatch"))?;

        if replacements_fit
            && records == root.records
            && pending_records == root.pending_records
            && (!multiple_leaves || widening)
        {
            // No key, separator, page allocation or root counter changes. A single
            // leaf recovers atomically through its FPI/delta. For multiple
            // leaves the caller explicitly guarantees conservative prefixes.
            // The unchanged root descriptor keeps its revision; zone mutation
            // frontiers are separate persisted records in the same batch.
            let mut first = 0;
            while first < changes.len() {
                let target = if first == 0 {
                    replacement_leaf.expect("nonempty changes have a leaf")
                } else {
                    self.leaf_for_update(root, entries[changes[first].index].0)?
                };
                let end = target.upper.map_or(changes.len(), |upper| {
                    first
                        + changes[first..].partition_point(|change| entries[change.index].0 < upper)
                });
                if end == first {
                    return Err(corrupt("invalid summary replacement range"));
                }
                if !self.patch_leaf(target, &changes[first..end], entries)? {
                    break;
                }
                first = end;
            }
            if first == changes.len() {
                return Ok(());
            }
        }

        *guard = None;
        root.revision = self.revision.fetch_add(1, Ordering::AcqRel) + 1;
        self.publish(root, false)?;
        let mut first = 0;
        while first < changes.len() {
            let target = self.leaf_for_update(root, entries[changes[first].index].0)?;
            let end = target.upper.map_or(changes.len(), |upper| {
                first + changes[first..].partition_point(|change| entries[change.index].0 < upper)
            });
            if end == first {
                return Err(corrupt("invalid summary update range"));
            }
            let applied = self.update_leaf_prefix(root, target, &changes[first..end], entries)?;
            if applied != 0 {
                first += applied;
                continue;
            }
            // Split or publish a changed lower bound for one record, then
            // re-evaluate the new leaf. The remaining records can often fill
            // that leaf in one write instead of one rewrite per insertion.
            let change = &changes[first];
            let (key, value) = &entries[change.index];
            let (page, height) = (root.page, root.height);
            let (children, inserted) = self.put_node(&mut root, page, height, *key, value)?;
            debug_assert_eq!(inserted, change.inserted);
            if children.len() > 1 {
                if root.height as usize >= MAX_HEIGHT {
                    return Err(corrupt("summary tree height limit"));
                }
                let page = allocate(&mut root)?;
                self.write_entries(page, Kind::Directory, &children)?;
                root.page = page;
                root.height += 1;
            }
            first += 1;
        }
        root.records = records;
        root.pending_records = pending_records;
        self.publish(root, true)?;
        *guard = Some(root);
        Ok(())
    }

    fn leaf_for_update(&self, root: Root, key: Key) -> Result<LeafTarget, QueryError> {
        let mut target = LeafTarget {
            page: root.page,
            lower: None,
            upper: None,
        };
        for _ in 0..root.height {
            target = self.file.visit(target.page, AccessIntent::Normal, |view| {
                directory::validate_node(&view, Kind::Directory, target.lower, target.upper)?;
                let at = view.lower_bound(key);
                let at = if view
                    .entry(at)
                    .is_some_and(|(candidate, _)| candidate == key)
                {
                    at
                } else {
                    at.saturating_sub(1)
                };
                let (first, bytes) = view
                    .entry(at)
                    .ok_or_else(|| corrupt("missing update child"))?;
                Ok(LeafTarget {
                    page: directory::decode_child(bytes, target.page, root.next_page)?,
                    lower: Some(first),
                    upper: view.entry(at + 1).map(|(key, _)| key).or(target.upper),
                })
            })?;
        }
        Ok(target)
    }

    fn patch_leaf(
        &self,
        target: LeafTarget,
        changes: &[Change],
        input: &[(Key, Vec<u8>)],
    ) -> Result<bool, QueryError> {
        // Most mutable records change payload bytes without changing size.
        // Preserve their slots and the packed key directory, copying one page
        // instead of allocating every unrelated record and repacking the leaf.
        let patched = self.file.write_prepared(target.page, Kind::Records, || {
            self.file.visit(target.page, AccessIntent::Normal, |view| {
                directory::validate_leaf(&view, target.lower, target.upper)?;
                let mut same_size = true;
                for change in changes {
                    let (key, value) = &input[change.index];
                    let at = view.lower_bound(*key);
                    let Some((_, prior)) = view.entry(at).filter(|(prior_key, _)| prior_key == key)
                    else {
                        return Ok(None);
                    };
                    same_size &= prior.len() == value.len();
                }
                if !same_size {
                    // Borrow unchanged payloads instead of allocating a Vec
                    // for every record in the leaf. The enclosing batch's
                    // scratch reservation covers this bounded reference list.
                    let mut entries = Vec::with_capacity(view.len());
                    let mut next_change = 0;
                    let mut payload_bytes = 0;
                    for index in 0..view.len() {
                        let (key, mut value) = view.entry(index).expect("validated summary slot");
                        if let Some(change) = changes.get(next_change) {
                            if input[change.index].0 == key {
                                value = &input[change.index].1;
                                next_change += 1;
                            }
                        }
                        payload_bytes += value.len();
                        entries.push((key, value));
                    }
                    if HEADER + view.len() * view.slot_width() + payload_bytes > PAGE_SIZE {
                        return Ok(None);
                    }
                    return encode(
                        self.file.identity(target.page),
                        Kind::Records,
                        view.next(),
                        &entries,
                    )
                    .map(Some);
                }
                let mut bytes = view.bytes.to_vec();
                let slot = view.slot_width();
                for change in changes {
                    let (key, value) = &input[change.index];
                    let at = view.lower_bound(*key);
                    let offset = read_u16(view.bytes, HEADER + at * slot + slot - 4) as usize;
                    bytes[offset..offset + value.len()].copy_from_slice(value);
                }
                write_u64(&mut bytes, 8, 0);
                refresh_page_checksum(&mut bytes);
                Ok(Some(bytes))
            })
        })?;
        Ok(patched.is_some())
    }

    fn update_leaf_prefix(
        &self,
        root: Root,
        target: LeafTarget,
        changes: &[Change],
        input: &[(Key, Vec<u8>)],
    ) -> Result<usize, QueryError> {
        if self.patch_leaf(target, changes, input)? {
            return Ok(changes.len());
        }
        let mut leaf = self.file.visit(target.page, AccessIntent::Normal, |view| {
            directory::validate_leaf(&view, target.lower, target.upper)?;
            Ok((0..view.len())
                .map(|index| {
                    let (key, value) = view.entry(index).expect("validated summary slot");
                    (key, value.to_vec())
                })
                .collect::<Entries>())
        })?;
        let old_first = leaf.first().map(|(key, _)| *key);
        let mut low = Key {
            column: u32::MAX,
            kind: 0,
            zone: u64::MAX,
        };
        let mut high = Key {
            column: 0,
            kind: 0,
            zone: 0,
        };
        for (key, _) in &leaf {
            low.column = low.column.min(key.column);
            low.zone = low.zone.min(key.zone);
            high.column = high.column.max(key.column);
            high.zone = high.zone.max(key.zone);
        }
        let mut payload = leaf.iter().map(|(_, value)| value.len()).sum::<usize>();
        let mut remaining_shrink = payload;
        let mut records = leaf.len();
        let mut fitting = 0;
        for (position, change) in changes.iter().enumerate() {
            let (key, value) = &input[change.index];
            let old = leaf.binary_search_by_key(key, |(key, _)| *key).ok();
            let prior_bytes = old.map_or(0, |index| leaf[index].1.len());
            payload = payload
                .checked_sub(prior_bytes)
                .and_then(|bytes| bytes.checked_add(value.len()))
                .ok_or_else(|| corrupt("summary leaf workspace overflow"))?;
            remaining_shrink = remaining_shrink.saturating_sub(prior_bytes);
            records += usize::from(old.is_none());
            low.column = low.column.min(key.column);
            low.zone = low.zone.min(key.zone);
            high.column = high.column.max(key.column);
            high.zone = high.zone.max(key.zone);
            let slot = slot_width(Kind::Records, [low, high]);
            let bytes = HEADER + records * slot + payload;
            if bytes <= PAGE_SIZE {
                fitting = position + 1;
            } else if bytes.saturating_sub(remaining_shrink) > PAGE_SIZE {
                // Even deleting every not-yet-updated old payload cannot make
                // a later prefix fit. Avoid rescanning the entire input after
                // every split of a long append-only batch.
                break;
            }
        }
        if fitting == 0 {
            return Ok(0);
        }
        for change in &changes[..fitting] {
            let (key, value) = &input[change.index];
            match leaf.binary_search_by_key(key, |(key, _)| *key) {
                Ok(index) => leaf[index].1 = value.clone(),
                Err(index) => leaf.insert(index, (*key, value.clone())),
            }
        }
        if root.height != 0 && leaf.first().map(|(key, _)| *key) != old_first {
            return Ok(0);
        }
        self.write_entries(target.page, Kind::Records, &leaf)?;
        Ok(fitting)
    }

    fn publish(&self, root: Root, ready: bool) -> Result<u64, QueryError> {
        self.file
            .write(0, Kind::Meta, 0, &[(ROOT_KEY, &root.payload(ready))])
    }

    fn write_entries(
        &self,
        page: u64,
        kind: Kind,
        entries: &[(Key, Vec<u8>)],
    ) -> Result<(), QueryError> {
        let refs: Vec<_> = entries
            .iter()
            .map(|(key, bytes)| (*key, bytes.as_slice()))
            .collect();
        self.file.write(page, kind, 0, &refs)?;
        Ok(())
    }

    fn put_node(
        &self,
        root: &mut Root,
        page: u64,
        height: u32,
        key: Key,
        value: &[u8],
    ) -> Result<(Entries, bool), QueryError> {
        // One bounded image/slot vector per tree level; never a resident
        // whole-tree directory. The external input batch has its own charge.
        let _scratch = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::StorageScratch,
            PAGE_SIZE * 24,
        )
        .ok_or_else(ZoneScratchLease::denied)?;
        let mut entries = self.file.visit(page, AccessIntent::Normal, |view| {
            if height == 0 {
                directory::validate_leaf(&view, None, None)?;
            } else {
                directory::validate_node(&view, Kind::Directory, None, None)?;
            }
            Ok((0..view.len())
                .map(|i| {
                    let (key, value) = view.entry(i).expect("validated summary slot");
                    (key, value.to_vec())
                })
                .collect::<Entries>())
        })?;
        let inserted;
        let kind;
        let mut split_intent = SplitIntent::Balanced;
        if height == 0 {
            kind = Kind::Records;
            match entries.binary_search_by_key(&key, |(key, _)| *key) {
                Ok(index) => {
                    inserted = false;
                    entries[index].1 = value.to_vec();
                }
                Err(index) => {
                    inserted = true;
                    // A prefix append in the middle of a mixed leaf must
                    // separate the whole foreign suffix. Maximizing the left
                    // fill in that case peels off old suffix records one at a
                    // time, stranding sparse pages that future inserts skip.
                    let prefix = |key: Key| Key { zone: 0, ..key };
                    if index == entries.len() {
                        split_intent = SplitIntent::Append;
                    } else if index > 0
                        && prefix(entries[index - 1].0) == prefix(key)
                        && prefix(entries[index].0) > prefix(key)
                    {
                        split_intent = SplitIntent::PrefixBoundary(index + 1);
                    }
                    entries.insert(index, (key, value.to_vec()));
                }
            }
        } else {
            kind = Kind::Directory;
            let index = match entries.binary_search_by_key(&key, |(key, _)| *key) {
                Ok(index) => index,
                Err(index) => index.saturating_sub(1),
            };
            let child = directory::decode_child(&entries[index].1, page, root.next_page)?;
            let (replacements, added) = self.put_node(root, child, height - 1, key, value)?;
            inserted = added;
            if replacements.len() == 1 && replacements[0] == entries[index] {
                return Ok((vec![(entries[0].0, page.to_le_bytes().to_vec())], inserted));
            }
            entries.splice(index..index + 1, replacements);
        }
        if encoded_size(kind, &entries) <= PAGE_SIZE {
            self.write_entries(page, kind, &entries)?;
            return Ok((vec![(entries[0].0, page.to_le_bytes().to_vec())], inserted));
        }
        let ends = split_ends(kind, &entries, split_intent)?;
        let mut children = Vec::with_capacity(ends.len());
        let mut start = 0;
        for end in ends {
            let child = if start == 0 { page } else { allocate(root)? };
            self.write_entries(child, kind, &entries[start..end])?;
            children.push((entries[start].0, child.to_le_bytes().to_vec()));
            start = end;
        }
        Ok((children, inserted))
    }
}

fn record_pending(key: Key, bytes: &[u8]) -> bool {
    match key.kind {
        0 => {
            bytes.len() != 32
                || bytes[0] != 1
                || matches!(bytes[1], 0 | 2)
                || read_u64(bytes, 16) != 0
        }
        2 => bytes.len() < 4 || bytes[0] != 1 || matches!(bytes[1], 0 | 2),
        _ => false,
    }
}

fn allocate(root: &mut Root) -> Result<u64, QueryError> {
    let page = root.next_page;
    root.next_page = page
        .checked_add(1)
        .filter(|next| next.checked_mul(PAGE_SIZE as u64).is_some())
        .ok_or_else(|| corrupt("summary allocation overflow"))?;
    Ok(page)
}

fn encoded_size(kind: Kind, entries: &[(Key, Vec<u8>)]) -> usize {
    HEADER
        + entries.len() * slot_width(kind, entries.iter().map(|(key, _)| *key))
        + entries.iter().map(|(_, value)| value.len()).sum::<usize>()
}

fn split_ends(
    kind: Kind,
    entries: &Entries,
    intent: SplitIntent,
) -> Result<Vec<usize>, QueryError> {
    if let Some(split) = split_at(kind, entries, intent) {
        return Ok(vec![split, entries.len()]);
    }
    // A large insertion/replacement between two nonempty ranges can require
    // three leaves even though every record fits and the old leaf was valid.
    // Pack consecutive ranges in linear time, accounting for key-slot widening.
    let mut ends = Vec::new();
    let mut start = 0;
    let mut payload = 0;
    let mut low = entries[0].0;
    let mut high = low;
    for (index, (key, value)) in entries.iter().enumerate() {
        low.column = low.column.min(key.column);
        low.zone = low.zone.min(key.zone);
        high.column = high.column.max(key.column);
        high.zone = high.zone.max(key.zone);
        let size =
            HEADER + (index - start + 1) * slot_width(kind, [low, high]) + payload + value.len();
        if size > PAGE_SIZE {
            if index == start {
                return Err(corrupt("summary record exceeds page capacity"));
            }
            ends.push(index);
            start = index;
            payload = 0;
            low = *key;
            high = *key;
            if HEADER + slot_width(kind, [*key]) + value.len() > PAGE_SIZE {
                return Err(corrupt("summary record exceeds page capacity"));
            }
        }
        payload += value.len();
    }
    ends.push(entries.len());
    Ok(ends)
}

fn split_at(kind: Kind, entries: &Entries, intent: SplitIntent) -> Option<usize> {
    if let SplitIntent::PrefixBoundary(after_new_key) = intent {
        // Prefer removing the unrelated suffix from the growing prefix. If
        // the prefix itself overflowed, move its newly appended key together
        // with that suffix instead. In either case the new key stays at the
        // prefix's live insertion frontier, rather than freezing old suffix
        // keys in singleton leaves. A large indivisible value may require the
        // ordinary balanced fallback below.
        for split in [after_new_key, after_new_key.saturating_sub(1)] {
            if split > 0
                && split < entries.len()
                && encoded_size(kind, &entries[..split]) <= PAGE_SIZE
                && encoded_size(kind, &entries[split..]) <= PAGE_SIZE
            {
                return Some(split);
            }
        }
    }
    // Both sides may use different key encodings. Compute the suffix
    // widths once, keeping split selection linear even for a wide key span.
    let widths = |reverse: bool| {
        let mut out = vec![SLOT; entries.len()];
        let (mut low_column, mut high_column, mut low_zone, mut high_zone) =
            (u32::MAX, 0, u64::MAX, 0);
        for step in 0..entries.len() {
            let index = if reverse {
                entries.len() - 1 - step
            } else {
                step
            };
            let key = entries[index].0;
            low_column = low_column.min(key.column);
            high_column = high_column.max(key.column);
            low_zone = low_zone.min(key.zone);
            high_zone = high_zone.max(key.zone);
            if kind == Kind::Records
                && high_column - low_column <= u8::MAX as u32
                && low_zone <= u32::MAX as u64
                && high_zone - low_zone <= u16::MAX as u64
            {
                out[index] = PACKED_SLOT;
            }
        }
        out
    };
    let prefix = widths(false);
    let suffix = widths(true);
    let total: usize = entries.iter().map(|(_, value)| value.len()).sum();
    let mut payload_left = 0usize;
    let mut best = None;
    for (index, (_, value)) in entries.iter().enumerate().take(entries.len() - 1) {
        payload_left += value.len();
        let left = payload_left + (index + 1) * prefix[index];
        let right = total - payload_left + (entries.len() - index - 1) * suffix[index + 1];
        if HEADER + left <= PAGE_SIZE && HEADER + right <= PAGE_SIZE {
            let difference = if matches!(intent, SplitIntent::Append) {
                right
            } else {
                left.abs_diff(right)
            };
            if best.is_none_or(|(_, prior)| difference < prior) {
                best = Some((index + 1, difference));
            }
        }
    }
    best.map(|(index, _)| index)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn key(zone: u64) -> Key {
        Key {
            column: 0,
            kind: Kind::Bounds as u8,
            zone,
        }
    }

    fn batch_fixture() -> (tempfile::TempDir, Store, Arc<crate::wal::LogMutex>) {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        // Packing fixtures need room for the interleaved tree. Cold reads
        // below explicitly flush and evict; the small-pool reopen regression
        // uses its own eight-page pool to retain eviction coverage.
        let pool = BufferPool::new(
            256,
            Arc::clone(&log),
            DurabilityMode::Strict,
            Duration::from_secs(5),
        );
        let file = PageFile::new(
            dir.path().join("batch.zm"),
            17,
            1,
            pool,
            Arc::clone(&log),
            DurabilityMode::Strict,
        )
        .unwrap();
        (dir, Store::create(file, 99, 64).unwrap(), log)
    }

    #[test]
    fn zone_map_page_bloom_growth_needs_three_leaves() {
        let bloom = |empty_blocks: usize| {
            let mut words = vec![u64::MAX; 1024];
            words[..empty_blocks * 8].fill(0);
            let set_bits = words.iter().map(|word| word.count_ones()).sum();
            summary::encode_membership(Some(&ZoneMembership::Bloom {
                words,
                hashes: 4,
                set_bits,
            }))
            .unwrap()
        };
        let compressed = bloom(5);
        let dense = bloom(0);
        let empty = summary::encode_membership(Some(&ZoneMembership::Exact {
            hashes: Vec::new(),
            bloom_words: 1024,
            bloom_hashes: 4,
        }))
        .unwrap();
        assert_eq!(compressed.len(), 8136);
        assert_eq!(dense.len(), 8200);
        for widening in [false, true] {
            let (_dir, store, _log) = batch_fixture();
            let keys = [10, 20, 30].map(|zone| Key {
                kind: Kind::Membership as u8,
                ..key(zone)
            });
            store
                .apply(&[
                    (keys[0], compressed.clone()),
                    (keys[1], empty.clone()),
                    (keys[2], compressed.clone()),
                ])
                .unwrap();
            assert_eq!(store.root_snapshot().unwrap().height, 0);
            let update = [(keys[1], dense.clone())];
            if widening {
                store.apply_widening(&update).unwrap();
            } else {
                store.apply(&update).unwrap();
            }
            store.flush().unwrap();
            store.file.pool.flush_and_evict_all().unwrap();
            let root = Root::open(&store.file, store.schema, store.zone_pages).unwrap();
            assert_eq!(root.height, 1);
            assert_eq!(root.records, 3);
            for (index, key) in keys.into_iter().enumerate() {
                root.get(&store.file, key, |actual| {
                    assert_eq!(
                        actual,
                        Some(if index == 1 {
                            dense.as_slice()
                        } else {
                            compressed.as_slice()
                        })
                    );
                    Ok(())
                })
                .unwrap();
            }
        }
    }

    #[test]
    fn zone_map_page_large_middle_record_needs_three_leaves() {
        for insert in [false, true] {
            for nested in [false, true] {
                let (_dir, store, _log) = batch_fixture();
                let mut expected = vec![(key(10), vec![1; 6000]), (key(30), vec![3; 6000])];
                if !insert {
                    expected.insert(1, (key(20), vec![2; 64]));
                }
                store.apply(&expected).unwrap();
                if nested {
                    store.apply(&[(key(40), vec![4; 9000])]).unwrap();
                    expected.push((key(40), vec![4; 9000]));
                    assert_eq!(store.root_snapshot().unwrap().height, 1);
                }
                let large = vec![2; 11000];
                store.apply(&[(key(20), large.clone())]).unwrap();
                if insert {
                    expected.insert(1, (key(20), large));
                } else {
                    expected[1].1 = large;
                }
                store.flush().unwrap();
                store.file.pool.flush_and_evict_all().unwrap();
                let root = Root::open(&store.file, store.schema, store.zone_pages).unwrap();
                assert_eq!(root.records, expected.len() as u64);
                let mut observed = Vec::new();
                root.visit_range(&store.file, key(0), key(50), |key, value| {
                    observed.push((key, value.to_vec()));
                    Ok(true)
                })
                .unwrap();
                assert_eq!(observed, expected);
                for (key, value) in &expected {
                    root.get(&store.file, *key, |actual| {
                        assert_eq!(actual, Some(value.as_slice()));
                        Ok(())
                    })
                    .unwrap();
                }
            }
        }
    }

    #[test]
    fn zone_map_page_same_size_patch_preserves_other_records_and_recovers() {
        let (_dir, store, _log) = batch_fixture();
        let entries: Vec<_> = (0..100).map(|id| (key(id), vec![id as u8; 64])).collect();
        store.apply(&entries).unwrap();
        let root = store.root_snapshot().unwrap();
        assert_eq!(root.height, 0);
        let before = store
            .file
            .visit(root.page, AccessIntent::Normal, |view| {
                Ok(view.bytes.to_vec())
            })
            .unwrap();
        store
            .apply(&[(key(1), vec![201; 64]), (key(77), vec![202; 64])])
            .unwrap();
        let after = store
            .file
            .visit(root.page, AccessIntent::Normal, |view| {
                Ok(view.bytes.to_vec())
            })
            .unwrap();
        let lower = read_u16(&before, 58) as usize;
        assert_eq!(
            &before[HEADER..lower],
            &after[HEADER..lower],
            "slot directory stays intact"
        );
        store.file.flush().unwrap();
        store.file.pool.flush_and_evict_all().unwrap();
        for (id, (_, original)) in entries.iter().enumerate() {
            store
                .get(key(id as u64), |actual| {
                    let expected = match id {
                        1 => vec![201; 64],
                        77 => vec![202; 64],
                        _ => original.clone(),
                    };
                    assert_eq!(actual, Some(expected.as_slice()));
                    Ok(())
                })
                .unwrap();
        }
        // A size change must fall back to repacking, keeping adjacent values.
        store.apply(&[(key(1), vec![203; 150])]).unwrap();
        store
            .get(key(77), |actual| {
                assert_eq!(actual, Some(&[202; 64][..]));
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn zone_map_page_large_batch_writes_page_prefixes_instead_of_each_record() {
        let (_dir, store, log) = batch_fixture();
        let entries = (0..8192u64)
            .map(|id| (key(id), vec![(id % 251) as u8; 96]))
            .collect::<Vec<_>>();
        let before = log.lock().last_lsn();
        store.apply(&entries).unwrap();
        let records = log.lock().last_lsn() - before;
        assert!(
            records < 2048,
            "bulk append rewrote leaves per record: {records} WAL records"
        );
        assert_eq!(store.root_snapshot().unwrap().records, entries.len() as u64);
        for (key, expected) in entries {
            store
                .get(key, |actual| {
                    assert_eq!(actual, Some(expected.as_slice()));
                    Ok(())
                })
                .unwrap();
        }
    }

    #[test]
    fn zone_map_page_packed_leaf_splits_when_distant_key_requires_wider_slots() {
        let (_dir, store, _) = batch_fixture();
        let entries: Vec<_> = (0..1500).map(|id| (key(id), vec![42])).collect();
        store.apply(&entries).unwrap();
        assert_eq!(store.root_snapshot().unwrap().height, 0);
        let distant = Key {
            column: 1000,
            kind: Kind::Bounds as u8,
            zone: u64::MAX,
        };
        store.apply(&[(distant, vec![7])]).unwrap();
        assert_eq!(store.root_snapshot().unwrap().height, 1);
        for (key, value) in entries.iter().chain(std::iter::once(&(distant, vec![7]))) {
            store
                .get(*key, |found| {
                    assert_eq!(found, Some(value.as_slice()));
                    Ok(())
                })
                .unwrap();
        }
    }

    #[test]
    fn zone_map_page_interleaved_zone_build_does_not_strand_sparse_suffixes() {
        let (_dir, store, _) = batch_fixture();
        let (zone_count, columns) = (193u64, 8u32);
        // Match the generic production builder's traversal: complete every
        // selected column of one zone, then advance to the next zone. Mutation
        // frontiers follow each bounded rebuild batch. This is deliberately
        // different from a column-sorted bulk load into an empty tree.
        for first in (0..zone_count).step_by(64) {
            let end = (first + 64).min(zone_count);
            for zone in first..end {
                let mut entries = vec![(
                    Key {
                        column: 0,
                        kind: 0,
                        zone,
                    },
                    vec![1; 32],
                )];
                for column in 0..columns {
                    entries.push((
                        Key {
                            column,
                            kind: 2,
                            zone,
                        },
                        vec![1; 20],
                    ));
                    let bytes = if column == 0 {
                        8
                    } else {
                        470 + ((zone * 37 + u64::from(column) * 17) % 110) as usize
                    };
                    entries.push((
                        Key {
                            column,
                            kind: 3,
                            zone,
                        },
                        vec![0x5a; bytes],
                    ));
                }
                store.apply(&entries).unwrap();
            }
            for zone in first..end {
                store
                    .apply(&[(
                        Key {
                            column: 0,
                            kind: 4,
                            zone,
                        },
                        vec![0; 16],
                    )])
                    .unwrap();
            }
        }
        let root = store.root_snapshot().unwrap();
        let (mut leaves, mut sparse, mut used, mut records) = (0u64, 0u64, 0u64, 0u64);
        for page in 1..root.next_page {
            store
                .file
                .visit(page, AccessIntent::Normal, |view| {
                    if view.kind() == Kind::Records {
                        leaves += 1;
                        records += view.len() as u64;
                        let size = PAGE_SIZE
                            - usize::from(read_u16(view.bytes, 60) - read_u16(view.bytes, 58));
                        used += size as u64;
                        sparse += u64::from(size < PAGE_SIZE / 10);
                    }
                    Ok(())
                })
                .unwrap();
        }
        assert_eq!(root.records, zone_count * (2 * u64::from(columns) + 2));
        assert_eq!(records, root.records);
        assert!(sparse <= u64::from(columns) * 2 + 2,
            "middle-prefix insertion stranded {sparse}/{leaves} sparse leaves; used {used} of {} bytes", leaves * PAGE_SIZE as u64);
        assert!(
            used * 10 >= leaves * PAGE_SIZE as u64 * 6,
            "interleaved append should fill at least 60%: {used}/{} bytes",
            leaves * PAGE_SIZE as u64
        );
        for zone in 0..zone_count {
            for column in 0..columns {
                let expected = if column == 0 {
                    8
                } else {
                    470 + ((zone * 37 + u64::from(column) * 17) % 110) as usize
                };
                store
                    .get(
                        Key {
                            column,
                            kind: 3,
                            zone,
                        },
                        |bytes| {
                            assert_eq!(bytes.unwrap().len(), expected);
                            Ok(())
                        },
                    )
                    .unwrap();
            }
        }
    }

    #[test]
    fn zone_map_page_append_split_packs_separate_key_prefixes() {
        let (_dir, store, _) = batch_fixture();
        let payload = vec![0x5a; 1032];
        for first in (0..240u64).step_by(30) {
            let mut batch = Vec::new();
            for column in 0..8 {
                for zone in first..first + 30 {
                    batch.push((
                        Key {
                            column,
                            kind: Kind::Membership as u8,
                            zone,
                        },
                        payload.clone(),
                    ));
                }
            }
            store.apply(&batch).unwrap();
        }
        let root = store.root_snapshot().unwrap();
        let (mut leaf_bytes, mut leaf_capacity) = (0usize, 0usize);
        for page in 1..root.next_page {
            store
                .file
                .visit(page, AccessIntent::Normal, |view| {
                    if view.kind() == Kind::Records {
                        leaf_capacity += PAGE_SIZE;
                        leaf_bytes += HEADER
                            + view.len() * view.slot_width()
                            + (0..view.len())
                                .map(|i| view.entry(i).unwrap().1.len())
                                .sum::<usize>();
                    }
                    Ok(())
                })
                .unwrap();
        }
        assert!(leaf_capacity > 0);
        assert!(
            leaf_bytes * 100 >= leaf_capacity * 90,
            "append-built leaves should be dense: {leaf_bytes}/{leaf_capacity}"
        );
        for column in 0..8 {
            for zone in 0..240 {
                store
                    .get(
                        Key {
                            column,
                            kind: Kind::Membership as u8,
                            zone,
                        },
                        |value| {
                            assert_eq!(value, Some(payload.as_slice()));
                            Ok(())
                        },
                    )
                    .unwrap();
            }
        }
    }

    #[test]
    fn zone_map_widening_does_not_exclude_directory_readers() {
        let (_dir, store, _log) = batch_fixture();
        let bounds = |min, max| {
            summary::encode_bounds(&ZoneColumnSummary {
                state: ZoneMapState::Ready,
                has_non_null: true,
                min: Some(ZoneScalar::Signed(min)),
                max: Some(ZoneScalar::Signed(max)),
                ..Default::default()
            })
            .unwrap()
        };
        let original = bounds(10, 20);
        store
            .apply(
                &(0..2000)
                    .map(|id| (key(id), original.clone()))
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        let store = Arc::new(store);
        let reader = store.state.read();
        let root = reader.unwrap();
        assert!(root.height > 0);
        let writer = Arc::clone(&store);
        let wider = bounds(0, 30);
        let updates = vec![(key(0), wider.clone()), (key(1999), wider.clone())];
        let (sent, received) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            sent.send(writer.apply_widening(&updates)).unwrap();
        });
        // Holding a directory snapshot must not stall a conservative update
        // of existing leaves. Always release it before joining on failure.
        let completed = received.recv_timeout(Duration::from_secs(2));
        drop(reader);
        thread.join().unwrap();
        completed
            .expect("widening blocked an unrelated directory reader")
            .unwrap();
        assert_eq!(
            store.root_snapshot().unwrap().payload(true),
            root.payload(true)
        );
        for id in [0, 1999] {
            store
                .get(key(id), |value| {
                    assert_eq!(value, Some(wider.as_slice()));
                    Ok(())
                })
                .unwrap();
        }
    }

    #[test]
    fn zone_map_structural_and_shrinking_updates_exclude_directory_readers() {
        for widening in [false, true] {
            let (_dir, store, _log) = batch_fixture();
            store.apply(&[(key(0), vec![10; 64])]).unwrap();
            let store = Arc::new(store);
            let reader = store.state.read();
            let writer = Arc::clone(&store);
            let (started, ready) = std::sync::mpsc::channel();
            let (sent, received) = std::sync::mpsc::channel();
            let thread = std::thread::spawn(move || {
                started.send(()).unwrap();
                let result = if widening {
                    // An insertion changes root counters, even without a split.
                    writer.apply_widening(&[(key(1), vec![20; 64])])
                } else {
                    writer.apply(&[(key(0), vec![20; 64])])
                };
                sent.send(result).unwrap();
            });
            ready.recv().unwrap();
            let completed = received.recv_timeout(Duration::from_millis(50));
            drop(reader);
            thread.join().unwrap();
            assert!(matches!(
                completed,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ));
            received.recv().unwrap().unwrap();
            store
                .get(key(u64::from(widening)), |value| {
                    assert_eq!(value, Some([20; 64].as_slice()));
                    Ok(())
                })
                .unwrap();
        }
    }

    #[test]
    fn zone_map_record_cache_follows_every_replacement() {
        let (_dir, store, _log) = batch_fixture();
        let bounds = |max| {
            summary::encode_bounds(&ZoneColumnSummary {
                state: ZoneMapState::Ready,
                has_non_null: true,
                min: Some(ZoneScalar::Signed(0)),
                max: Some(ZoneScalar::Signed(max)),
                ..Default::default()
            })
            .unwrap()
        };
        let read = |id| {
            store
                .get(key(id), |bytes| Ok(bytes.map(<[u8]>::to_vec)))
                .unwrap()
        };
        store.apply(&[(key(1), bounds(1))]).unwrap();
        // Cache both a present record and a proven absence.
        assert_eq!(read(1), Some(bounds(1)));
        assert_eq!(read(2), None);
        // A shared-guard widening patch must not leave the old bytes cached.
        store.apply_widening(&[(key(1), bounds(5))]).unwrap();
        assert_eq!(read(1), Some(bounds(5)));
        // An exclusive insert replaces the cached absence.
        store.apply(&[(key(2), bounds(7))]).unwrap();
        assert_eq!(read(2), Some(bounds(7)));
        // A shrinking (exclusive) replacement is observed as well.
        store.apply(&[(key(1), bounds(2))]).unwrap();
        assert_eq!(read(1), Some(bounds(2)));
        // Reset to an empty Unknown baseline drops every cached record.
        store.reset_unknown().unwrap();
        assert_eq!(read(1), None);
    }

    #[test]
    fn zone_map_record_cache_rejects_reads_that_raced_a_replacement() {
        let cache = RecordCache::default();
        let generation = cache.lookup(key(3)).unwrap_err();
        // A writer replaces the record between the reader's generation
        // sample and its insert: the stale read must not be installed.
        cache.forget(key(3));
        cache.insert(key(3), generation, Some(b"stale"));
        assert!(cache.lookup(key(3)).is_err());
        let generation = cache.lookup(key(3)).unwrap_err();
        cache.insert(key(3), generation, Some(b"fresh"));
        assert_eq!(
            cache.lookup(key(3)).unwrap().as_deref(),
            Some(&b"fresh"[..])
        );
    }

    #[test]
    fn zone_map_concurrent_widening_preserves_other_leaf_records() {
        let (_dir, store, _log) = batch_fixture();
        let bounds = |max| {
            summary::encode_bounds(&ZoneColumnSummary {
                state: ZoneMapState::Ready,
                has_non_null: true,
                min: Some(ZoneScalar::Signed(0)),
                max: Some(ZoneScalar::Signed(max)),
                ..Default::default()
            })
            .unwrap()
        };
        store
            .apply(&(0..8).map(|id| (key(id), bounds(0))).collect::<Vec<_>>())
            .unwrap();
        let store = Arc::new(store);
        let start = Arc::new(std::sync::Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|id| {
                let store = Arc::clone(&store);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    for max in 1..=32 {
                        store.apply_widening(&[(key(id), bounds(max))]).unwrap();
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        store.flush().unwrap();
        store.file.pool.flush_and_evict_all().unwrap();
        let root = Root::open(&store.file, store.schema, store.zone_pages).unwrap();
        for id in 0..8 {
            root.get(&store.file, key(id), |value| {
                assert_eq!(value, Some(bounds(32).as_slice()));
                Ok(())
            })
            .unwrap();
        }
    }

    #[test]
    fn zone_map_widening_releases_each_leaf_and_retries_structural_tail() {
        let (_dir, store, _log) = batch_fixture();
        let original = summary::encode_bounds(&ZoneColumnSummary {
            state: ZoneMapState::Ready,
            has_non_null: true,
            min: Some(ZoneScalar::Signed(10)),
            max: Some(ZoneScalar::Signed(20)),
            ..Default::default()
        })
        .unwrap();
        let wider = summary::encode_bounds(&ZoneColumnSummary {
            state: ZoneMapState::Ready,
            has_non_null: true,
            min: Some(ZoneScalar::Signed(0)),
            max: Some(ZoneScalar::Signed(30)),
            ..Default::default()
        })
        .unwrap();
        store
            .apply(
                &(0..2000)
                    .map(|id| (key(id), original.clone()))
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        let store = Arc::new(store);
        let root = store.root_snapshot().unwrap();
        let first_leaf = store.leaf_for_update(root, key(0)).unwrap();
        let other = (1..2000)
            .find(|&id| {
                store.leaf_for_update(root, key(id)).unwrap().page % 64 != first_leaf.page % 64
            })
            .unwrap();
        let blocked_leaf = store.leaf_writers[first_leaf.page as usize % 64].lock();
        let writer = Arc::clone(&store);
        let value = wider.clone();
        let (sent, received) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            sent.send(writer.apply_widening(&[(key(other), value)]))
                .unwrap();
        });
        let completed = received.recv_timeout(Duration::from_secs(2));
        drop(blocked_leaf);
        thread.join().unwrap();
        completed
            .expect("an unrelated leaf writer stalled")
            .unwrap();

        // The first leaf can publish before the missing tail key requires
        // exclusive structural publication. Retrying must not lose either.
        store
            .apply_widening(&[(key(0), wider.clone()), (key(2000), wider.clone())])
            .unwrap();
        assert_eq!(store.root_snapshot().unwrap().records, root.records + 1);
        store.flush().unwrap();
        store.file.pool.flush_and_evict_all().unwrap();
        let reopened = Root::open(&store.file, store.schema, store.zone_pages).unwrap();
        for id in [0, other, 2000] {
            reopened
                .get(&store.file, key(id), |bytes| {
                    assert_eq!(bytes, Some(wider.as_slice()));
                    Ok(())
                })
                .unwrap();
        }
    }

    #[test]
    fn zone_map_widening_replacements_keep_root_across_multiple_leaves() {
        let (_dir, store, log) = batch_fixture();
        let bounds = |min, max| {
            summary::encode_bounds(&ZoneColumnSummary {
                state: ZoneMapState::Ready,
                has_non_null: true,
                min: Some(ZoneScalar::Signed(min)),
                max: Some(ZoneScalar::Signed(max)),
                ..Default::default()
            })
            .unwrap()
        };
        let original = bounds(10, 20);
        let wider = bounds(0, 30);
        assert_eq!(original.len(), wider.len());
        let records: Vec<_> = (0..2000).map(|id| (key(id), original.clone())).collect();
        store.apply(&records).unwrap();
        let root = store.root_snapshot().unwrap();
        assert!(root.height > 0);
        let leaves = (1..root.next_page)
            .filter(|&page| {
                store
                    .file
                    .visit(page, AccessIntent::Normal, |view| {
                        Ok(view.kind() == Kind::Records)
                    })
                    .unwrap()
            })
            .count() as u64;
        assert!(leaves > 1);
        let updates: Vec<_> = records
            .iter()
            .map(|(key, _)| (*key, wider.clone()))
            .collect();
        let before = log.lock().last_lsn();
        store.apply_widening(&updates).unwrap();
        assert_eq!(log.lock().last_lsn() - before, leaves);
        store.flush().unwrap();
        store.file.pool.flush_and_evict_all().unwrap();
        let reopened = Root::open(&store.file, store.schema, store.zone_pages).unwrap();
        assert_eq!(reopened.payload(true), root.payload(true));
        for (key, _) in records {
            reopened
                .get(&store.file, key, |value| {
                    assert_eq!(value, Some(wider.as_slice()));
                    Ok(())
                })
                .unwrap();
        }
    }

    #[test]
    fn zone_map_single_leaf_replacement_is_one_atomic_redo() {
        check_single_leaf_atomic_replacement(64);
    }

    #[test]
    fn zone_map_resized_leaf_replacement_is_one_atomic_redo() {
        check_single_leaf_atomic_replacement(97);
    }

    fn check_single_leaf_atomic_replacement(replacement_len: usize) {
        let (_dir, store, log) = batch_fixture();
        let keys = [key(1), key(2), key(3)];
        store.apply(&keys.map(|key| (key, vec![0x41; 64]))).unwrap();
        let root = store.root_snapshot().unwrap();
        let before = log.lock().last_lsn();
        store
            .apply(&[
                (keys[0], vec![0x42; replacement_len]),
                (keys[2], vec![0x43; 64]),
            ])
            .unwrap();
        assert_eq!(
            log.lock().last_lsn() - before,
            1,
            "one atomic leaf replacement must not revoke and republish an unchanged root"
        );
        store.file.flush().unwrap();
        store.file.pool.flush_and_evict_all().unwrap();
        let reopened = Root::open(&store.file, store.schema, store.zone_pages).unwrap();
        assert_eq!(reopened.payload(true), root.payload(true));
        for (key, byte) in [(keys[0], 0x42), (keys[1], 0x41), (keys[2], 0x43)] {
            reopened
                .get(&store.file, key, |value| {
                    let len = if byte == 0x42 { replacement_len } else { 64 };
                    assert_eq!(value, Some(vec![byte; len].as_slice()));
                    Ok(())
                })
                .unwrap();
        }
    }

    #[test]
    fn zone_map_page_batch_updates_each_leaf_once_and_skips_unchanged_records() {
        let (_dir, store, log) = batch_fixture();
        let records: Vec<_> = (0..400u64)
            .map(|id| {
                (
                    Key {
                        column: (id % 8) as u32,
                        kind: Kind::Bounds as u8,
                        zone: id / 8,
                    },
                    vec![0x41; 128],
                )
            })
            .collect();
        store.apply(&records).unwrap();
        let root = store.root_snapshot().unwrap();
        assert!(root.height > 0);
        let mut leaves = 0u64;
        for page in 1..root.next_page {
            leaves += u64::from(
                store
                    .file
                    .visit(page, AccessIntent::Normal, |view| {
                        Ok(view.kind() == Kind::Records)
                    })
                    .unwrap(),
            );
        }
        assert!(leaves >= 2);
        let updates: Vec<_> = records
            .iter()
            .map(|(key, _)| (*key, vec![0x42; 128]))
            .collect();
        let (lsn, bytes) = {
            let log = log.lock();
            (log.last_lsn(), log.wal_bytes())
        };
        store.apply(&updates).unwrap();
        let (written, written_bytes) = {
            let log = log.lock();
            (log.last_lsn() - lsn, log.wal_bytes() - bytes)
        };
        assert_eq!(
            written,
            leaves + 2,
            "one physical redo record per leaf plus revoke/publish root"
        );
        assert!(
            written_bytes <= written * (PAGE_SIZE as u64 + 512),
            "WAL grows with touched pages, not records"
        );
        assert_eq!(store.root_snapshot().unwrap().records, 400);
        for (key, value) in &updates {
            store
                .get(*key, |actual| {
                    assert_eq!(actual, Some(value.as_slice()));
                    Ok(())
                })
                .unwrap();
        }
        let before = log.lock().last_lsn();
        let hits = store.file.pool.buffer_stats().hits;
        store.apply(&updates).unwrap();
        assert_eq!(
            log.lock().last_lsn(),
            before,
            "completely neutral batch has no redo"
        );
        assert!(
            store.file.pool.buffer_stats().hits - hits <= leaves * (u64::from(root.height) + 1),
            "neutral batch preflight must visit pages, not repin per record"
        );
        let mut mostly_neutral = updates.clone();
        mostly_neutral[137].1 = vec![0x43; 128];
        let before = log.lock().last_lsn();
        store.apply(&mostly_neutral).unwrap();
        assert_eq!(
            log.lock().last_lsn() - before,
            1,
            "unchanged records in other leaves are not rewritten"
        );
    }

    #[test]
    fn zone_map_page_batch_duplicate_keys_keep_last_value_and_pending_counts() {
        let (_dir, store, log) = batch_fixture();
        let meta_key = summary::key(0, Kind::ZoneMeta, 0);
        let ready = summary::encode_meta(&ZoneSummary::default()).to_vec();
        let unknown = summary::encode_meta(&ZoneSummary {
            state: Some(ZoneMapState::Unknown),
            ..ZoneSummary::default()
        })
        .to_vec();
        store.apply(&[(meta_key, ready.clone())]).unwrap();
        let before = log.lock().last_lsn();
        store
            .apply(&[(meta_key, unknown.clone()), (meta_key, ready.clone())])
            .unwrap();
        assert_eq!(
            log.lock().last_lsn(),
            before,
            "intermediate duplicate values are unobservable"
        );
        assert_eq!(store.root_snapshot().unwrap().pending_records, 0);
        store
            .apply(&[(meta_key, ready.clone()), (meta_key, unknown.clone())])
            .unwrap();
        assert_eq!(log.lock().last_lsn() - before, 3);
        assert_eq!(store.root_snapshot().unwrap().records, 1);
        assert_eq!(store.root_snapshot().unwrap().pending_records, 1);
        store
            .get(meta_key, |actual| {
                assert_eq!(actual, Some(unknown.as_slice()));
                Ok(())
            })
            .unwrap();
        store.apply(&[(meta_key, ready)]).unwrap();
        assert_eq!(store.root_snapshot().unwrap().pending_records, 0);
    }

    #[test]
    fn zone_map_page_batch_fits_final_growth_and_shrink_in_one_leaf() {
        let (_dir, store, log) = batch_fixture();
        store
            .apply(&[(key(0), vec![1; 1000]), (key(1), vec![2; 13000])])
            .unwrap();
        let before = log.lock().last_lsn();
        // Applying growth alone would split, but the atomic batch fits.
        store
            .apply(&[(key(0), vec![3; 13000]), (key(1), vec![4; 1000])])
            .unwrap();
        assert_eq!(log.lock().last_lsn() - before, 3);
        assert_eq!(store.root_snapshot().unwrap().height, 0);
        assert_eq!(store.root_snapshot().unwrap().records, 2);
        store
            .get(key(0), |value| {
                assert_eq!(value.unwrap(), vec![3; 13000]);
                Ok(())
            })
            .unwrap();
        store
            .get(key(1), |value| {
                assert_eq!(value.unwrap(), vec![4; 1000]);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn zone_map_page_batch_falls_back_for_splits_and_changed_parent_lower_bound() {
        let (_dir, store, _log) = batch_fixture();
        store
            .apply(&[(key(10), vec![10; 9000]), (key(20), vec![20; 9000])])
            .unwrap();
        assert!(store.root_snapshot().unwrap().height > 0);
        store
            .apply(&[
                (key(0), vec![1; 9000]),
                (key(5), vec![5; 9000]),
                (key(20), vec![21; 9000]),
            ])
            .unwrap();
        let root = store.root_snapshot().unwrap();
        assert_eq!(root.records, 4);
        for (id, marker) in [(0, 1), (5, 5), (10, 10), (20, 21)] {
            store
                .get(key(id), |value| {
                    assert_eq!(value.unwrap(), vec![marker; 9000]);
                    Ok(())
                })
                .unwrap();
        }
        let mut observed = Vec::new();
        store
            .visit_range(key(0), key(21), |key, _| {
                observed.push(key.zone);
                Ok(true)
            })
            .unwrap();
        assert_eq!(observed, [0, 5, 10, 20]);
    }

    #[test]
    fn zone_map_page_mutable_updates_split_and_reopen_without_decoded_cache() {
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
        let path =
            crate::storage::naming::table_object_path(dir.path(), "test", "mutable", 17, "zm");
        let file = PageFile::new(
            path.clone(),
            17,
            1,
            Arc::clone(&pool),
            Arc::clone(&log),
            DurabilityMode::Strict,
        )
        .unwrap();
        let store = Store::create(file, 99, 64).unwrap();
        let entries: Vec<_> = (0..600)
            .rev()
            .map(|id| (key(id), vec![(id % 251) as u8; 9000]))
            .collect();
        store.apply(&entries).unwrap();
        let before = log.lock().last_lsn();
        store.apply(&entries).unwrap();
        assert_eq!(
            log.lock().last_lsn(),
            before,
            "neutral summaries must not generate redo"
        );
        assert!(store.state.read().unwrap().height >= 2);
        for id in 0..600 {
            store
                .get(key(id), |value| {
                    assert_eq!(value.unwrap(), &vec![(id % 251) as u8; 9000]);
                    Ok(())
                })
                .unwrap();
        }
        let replacement = vec![7; 64];
        store.apply(&[(key(42), replacement.clone())]).unwrap();
        let pages = store.state.read().unwrap().next_page;
        for _ in 0..5 {
            store.apply(&[(key(42), vec![8; 128])]).unwrap();
            store.apply(&[(key(42), replacement.clone())]).unwrap();
        }
        assert_eq!(
            store.state.read().unwrap().next_page,
            pages,
            "ordinary updates must not append new paths"
        );
        store.file.flush().unwrap();
        pool.flush_and_evict_all().unwrap();
        drop(store);
        let file = PageFile::new(path, 17, 1, pool, log, DurabilityMode::Strict).unwrap();
        let store = Store::open(file, 99, 64).unwrap();
        store
            .get(key(42), |value| {
                assert_eq!(value, Some(replacement.as_slice()));
                Ok(())
            })
            .unwrap();
        store.invalidate().unwrap();
        assert!(store.get(key(42), |_| Ok(())).is_err());
        store.reset_unknown().unwrap();
        store
            .get(key(42), |value| {
                assert!(value.is_none());
                Ok(())
            })
            .unwrap();
    }
}
