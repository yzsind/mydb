//! Immutable policy-version roots. Current policies remain descriptor-lazy;
//! only retired versions needed by an older statement retain their payload.
use super::*;
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard, OnceLock};

#[derive(Debug)]
pub(crate) struct PolicyLease {
    security: Arc<RowSecurity>,
    _memory: ControlledMemoryCharge,
}
impl PolicyLease {
    fn new(security: Arc<RowSecurity>) -> Result<Arc<Self>, QueryError> {
        let memory = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::SystemMetadata,
            security
                .allocated_bytes()
                .saturating_add(std::mem::size_of::<Self>() + 32),
        )?;
        Ok(Arc::new(Self {
            security,
            _memory: memory,
        }))
    }
}

#[derive(Debug)]
struct Version {
    storage_id: u64,
    version: u64,
    enabled: bool,
    cached: Mutex<std::sync::Weak<PolicyLease>>,
    retired: OnceLock<Arc<PolicyLease>>,
    _memory: ControlledMemoryCharge,
}
impl Version {
    fn new(storage_id: u64, security: &RowSecurity) -> Result<Arc<Self>, QueryError> {
        if storage_id == 0 || security.version == 0 {
            return Err(violation("Invalid row security version identity"));
        }
        let memory = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::SystemMetadata,
            std::mem::size_of::<Self>() + 32,
        )?;
        Ok(Arc::new(Self {
            storage_id,
            version: security.version,
            enabled: security.enabled,
            cached: Mutex::new(std::sync::Weak::new()),
            retired: OnceLock::new(),
            _memory: memory,
        }))
    }
    // Only cold descriptor mismatches use this weak cache. Current and
    // retired versions take the lock-free branches in resolve().
    fn cached(&self) -> Option<Arc<PolicyLease>> {
        self.cached
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .upgrade()
    }
    fn cache(&self, lease: &Arc<PolicyLease>) {
        *self
            .cached
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Arc::downgrade(lease);
    }
}

pub(crate) enum PolicySource {
    Descriptor(Arc<RowSecurity>),
    Retained(Arc<PolicyLease>),
}
impl std::ops::Deref for PolicySource {
    type Target = RowSecurity;
    fn deref(&self) -> &RowSecurity {
        match self {
            Self::Descriptor(security) => security,
            Self::Retained(lease) => &lease.security,
        }
    }
}

#[derive(Debug)]
pub(crate) struct CatalogSnapshot {
    pub(crate) epoch: u64,
    entries: BTreeMap<u64, Arc<Version>>,
    _memory: ControlledMemoryCharge,
}
impl CatalogSnapshot {
    pub(crate) fn object_for_storage(&self, storage_id: u64) -> Option<u64> {
        self.entries
            .iter()
            .find_map(|(id, version)| (version.storage_id == storage_id).then_some(*id))
    }
    fn charge(entries: usize) -> Result<ControlledMemoryCharge, QueryError> {
        // B-tree nodes are cloned on publication, not on statement capture.
        // Account their key/Arc slots and conservative node fragmentation.
        ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::SystemMetadata,
            std::mem::size_of::<Self>()
                .saturating_add(64)
                .saturating_add(if entries == 0 {
                    0
                } else {
                    entries.saturating_mul(96).saturating_add(1024)
                }),
        )
    }
    pub(crate) fn flags(&self, table: &TableSchema) -> Result<(u64, bool), QueryError> {
        match self.entries.get(&table.object_id) {
            Some(version) => Ok((version.version, version.enabled)),
            None if table.row_security.version == 0 => Ok((0, false)),
            // A late-discovered configured relation may have been created or
            // enabled after capture. Never infer an unprotected old object.
            None => Err(snapshot_conflict()),
        }
    }
    pub(crate) fn resolve(
        &self,
        table: &TableSchema,
        load: impl FnOnce(u64, u64) -> Result<Arc<RowSecurity>, QueryError>,
    ) -> Result<PolicySource, QueryError> {
        let (_, enabled) = self.flags(table)?;
        if !enabled {
            return Ok(PolicySource::Descriptor(RowSecurity::empty()));
        }
        let version = &self.entries[&table.object_id];
        if table.row_security.version == version.version
            && table.row_security.enabled
            && !table.columns.is_empty()
        {
            return Ok(PolicySource::Descriptor(table.row_security.clone()));
        }
        if let Some(lease) = version.retired.get() {
            return Ok(PolicySource::Retained(lease.clone()));
        }
        if let Some(lease) = version.cached() {
            return Ok(PolicySource::Retained(lease));
        }
        let security = load(version.storage_id, version.version)?;
        if security.version != version.version || security.enabled != version.enabled {
            return Err(snapshot_conflict());
        }
        let lease = PolicyLease::new(security)?;
        version.cache(&lease);
        Ok(PolicySource::Retained(lease))
    }
    pub(crate) fn matches(&self, object_id: u64, storage_id: u64, security: &RowSecurity) -> bool {
        match self.entries.get(&object_id) {
            Some(old) => {
                old.storage_id == storage_id
                    && old.version == security.version
                    && old.enabled == security.enabled
            }
            None => security.version == 0 && !security.enabled,
        }
    }
}

pub(crate) type CatalogChange = (u64, Option<(u64, Arc<RowSecurity>)>);

#[derive(Debug)]
pub(crate) struct CatalogStore {
    current: arc_swap::ArcSwap<CatalogSnapshot>,
    writer: Mutex<()>,
    known: [std::sync::atomic::AtomicU64; 64],
    _memory: ControlledMemoryCharge,
}
impl CatalogStore {
    pub(crate) fn new<'a>(
        tables: impl Iterator<Item = &'a Arc<TableSchema>>,
    ) -> Result<Self, QueryError> {
        let memory = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::SystemMetadata,
            std::mem::size_of::<Self>(),
        )?;
        let snapshot = Self::build(tables, 1)?;
        let store = Self {
            current: arc_swap::ArcSwap::from(snapshot),
            writer: Mutex::new(()),
            known: [const { std::sync::atomic::AtomicU64::new(0) }; 64],
            _memory: memory,
        };
        for id in store.current.load().entries.keys() {
            store.mark_known(*id);
        }
        Ok(store)
    }
    fn build<'a>(
        tables: impl Iterator<Item = &'a Arc<TableSchema>>,
        epoch: u64,
    ) -> Result<Arc<CatalogSnapshot>, QueryError> {
        let mut memory = CatalogSnapshot::charge(0)?;
        let mut entries = BTreeMap::new();
        for table in
            tables.filter(|table| table.row_security.version != 0 || table.row_security.enabled)
        {
            memory.try_grow(if entries.is_empty() { 1120 } else { 96 })?;
            if entries
                .insert(
                    table.object_id,
                    Version::new(table.id, &table.row_security)?,
                )
                .is_some()
            {
                return Err(violation("Duplicate row security object identity"));
            }
        }
        Ok(Arc::new(CatalogSnapshot {
            epoch,
            entries,
            _memory: memory,
        }))
    }
    pub(crate) fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.current.load_full()
    }
    fn bits(id: u64) -> [usize; 2] {
        let mut value = id.wrapping_add(0x9e3779b97f4a7c15);
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^= value >> 31;
        [(value & 4095) as usize, ((value >> 32) & 4095) as usize]
    }
    fn mark_known(&self, id: u64) {
        for bit in Self::bits(id) {
            self.known[bit / 64].fetch_or(1u64 << (bit % 64), std::sync::atomic::Ordering::Release);
        }
    }
    /// A monotone negative proof also catches stale descriptors with a zero
    /// marker after RLS was enabled. False positives only require binding.
    pub(crate) fn may_have_security(&self, id: u64) -> bool {
        Self::bits(id).iter().all(|bit| {
            self.known[*bit / 64].load(std::sync::atomic::Ordering::Acquire) & (1u64 << (*bit % 64))
                != 0
        })
    }
    /// Recovery/bootstrap runs before serving statements. Epochs remain
    /// monotone even if a caller reloads the authoritative dictionary.
    pub(crate) fn reset<'a>(
        &self,
        tables: impl Iterator<Item = &'a Arc<TableSchema>>,
    ) -> Result<(), QueryError> {
        let _guard = self
            .writer
            .lock()
            .map_err(|_| violation("Policy publication lock poisoned"))?;
        let epoch = self
            .current
            .load()
            .epoch
            .checked_add(1)
            .ok_or_else(|| violation("Policy catalog epoch exhausted"))?;
        let snapshot = Self::build(tables, epoch)?;
        for id in snapshot.entries.keys() {
            self.mark_known(*id);
        }
        self.current.store(snapshot);
        Ok(())
    }

    /// Admission and old-version capture complete before the DD transaction.
    /// The loader reads old committed DD rows, or a replica's pinned old
    /// descriptors. It never runs a SQL policy expression under this latch.
    pub(crate) fn prepare(
        &self,
        changes: Vec<CatalogChange>,
        mut load_old: impl FnMut(u64, u64) -> Result<Arc<RowSecurity>, QueryError>,
    ) -> Result<Option<CatalogPublication<'_>>, QueryError> {
        let guard = self
            .writer
            .lock()
            .map_err(|_| violation("Policy publication lock poisoned"))?;
        let current = self.current.load_full();
        let changed: Vec<_> = changes
            .into_iter()
            .filter(|(id, new)| match new {
                Some((storage, security)) => !current.matches(*id, *storage, security),
                None => current.entries.contains_key(id),
            })
            .collect();
        if changed.is_empty() {
            return Ok(None);
        }
        let mut count = current.entries.len();
        for (id, new) in &changed {
            if new.is_some() && !current.entries.contains_key(id) {
                count += 1;
            }
            if new.is_none() && current.entries.contains_key(id) {
                count -= 1;
            }
            if let (Some(old), Some((_, security))) = (current.entries.get(id), new) {
                if security.version < old.version
                    || security.version == 0
                    || (security.version == old.version && security.enabled != old.enabled)
                {
                    return Err(violation(
                        "Row security version regressed or changed without publication",
                    ));
                }
            }
        }
        let memory = CatalogSnapshot::charge(count)?;
        let scratch = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::StorageScratch,
            changed.len().saturating_mul(256).saturating_add(128),
        )?;
        let mut next = current.entries.clone();
        let mut retired = Vec::new();
        let mut payloads = Vec::new();
        for (id, new) in changed {
            if new.is_some() {
                self.mark_known(id);
            }
            if let Some(old) = current.entries.get(&id) {
                let lease = if let Some(lease) = old.retired.get() {
                    lease.clone()
                } else if let Some(lease) = old.cached() {
                    lease
                } else if let Some((_, security)) = new.as_ref().filter(|(_, security)| {
                    security.version == old.version && security.enabled == old.enabled
                })
                {
                    // A storage relocation preserves the logical object's
                    // policy version. Its complete incoming descriptor is
                    // also the authoritative payload for the old root. On a
                    // replica the old DD primary key may already be gone.
                    PolicyLease::new(security.clone())?
                } else {
                    PolicyLease::new(load_old(old.storage_id, old.version)?)?
                };
                if lease.security.version != old.version || lease.security.enabled != old.enabled {
                    return Err(snapshot_conflict());
                }
                // The staged lease keeps the weak payload available even in
                // the DD-commit/live-descriptor publication interval.
                old.cache(&lease);
                retired.push((old.clone(), lease));
            }
            if let Some((storage, security)) = new {
                let version = Version::new(storage, &security)?;
                let lease = PolicyLease::new(security)?;
                version.cache(&lease);
                payloads.push(lease);
                next.insert(id, version);
            } else {
                next.remove(&id);
            }
        }
        let epoch = current
            .epoch
            .checked_add(1)
            .ok_or_else(|| violation("Policy catalog epoch exhausted"))?;
        Ok(Some(CatalogPublication {
            store: self,
            _guard: guard,
            _previous: current,
            next: Arc::new(CatalogSnapshot {
                epoch,
                entries: next,
                _memory: memory,
            }),
            retired,
            _payloads: payloads,
            _scratch: scratch,
        }))
    }
}

pub(crate) struct CatalogPublication<'a> {
    store: &'a CatalogStore,
    _guard: MutexGuard<'a, ()>,
    _previous: Arc<CatalogSnapshot>,
    next: Arc<CatalogSnapshot>,
    retired: Vec<(Arc<Version>, Arc<PolicyLease>)>,
    _payloads: Vec<Arc<PolicyLease>>,
    _scratch: ControlledMemoryCharge,
}
impl CatalogPublication<'_> {
    pub(crate) fn publish(self) {
        for (version, lease) in &self.retired {
            let _ = version.retired.set(lease.clone());
        }
        self.store.current.store(self.next.clone());
    }
}

pub(crate) fn snapshot_conflict() -> QueryError {
    crate::error::catalog::ER_ROW_SECURITY_SNAPSHOT_CONFLICT
        .error("Row security metadata changed after statement capture; retry the statement")
}
