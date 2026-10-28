#[derive(Debug)]
pub(super) struct CatalogManager {
    live: RwLock<Catalog>,
    /// Stable object/storage id -> normalized table key.  This is the small
    /// startup-resident object-header index; child descriptors remain lazy.
    table_keys_by_id: RwLock<HashMap<u64, String>>,
    table_headers: RwLock<HashMap<String, Arc<TableSchema>>>,
    descriptor_cache: Mutex<DescriptorCacheState>,
    dd_rows_by_object: RwLock<HashMap<(u64, u64), Vec<RowId>>>,
    epoch: AtomicU64,
    cache_limit_bytes: AtomicUsize,
    table_id_highwater: AtomicU64,
    sequence_id_highwater: AtomicU64,
    has_foreign_keys: AtomicBool,
    resident_charge: Mutex<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
    id_allocator_lock: Mutex<()>,
    online_pinned_ids: parking_lot::RwLock<HashSet<u64>>,
    ddl_pinned_ids: parking_lot::RwLock<HashMap<u64, usize>>,
}

#[cfg(test)]
mod read_compatible_catalog_tests {
    use super::*;

    #[test]
    fn read_compatible_stale_descriptor_cannot_overwrite_published_definition() {
        for pinned in [false, true] {
            let mut catalog = crate::storage::catalog::empty_catalog();
            let old = Arc::new(TableSchema::system_table(9999, "stale", Vec::new()));
            let key = old.qualified_name();
            catalog.tables.insert(key.clone(), old.clone());
            let manager = CatalogManager::new(catalog).unwrap();
            if pinned {
                manager.online_pinned_ids.write().insert(old.id);
            }
            let mut published = old.as_ref().clone();
            published.bump_schema_version();
            published.comment = "published".into();
            manager
                .write()
                .unwrap()
                .tables
                .insert(key.clone(), Arc::new(published));
            // A lazy DD read started before publication, then finished late.
            manager.install_descriptor(key.clone(), old);
            assert_eq!(
                manager.read().unwrap().tables[&key].comment,
                "published",
                "late descriptor replaced published state; pinned={pinned}"
            );
        }
    }
}

#[derive(Debug, Default)]
struct DescriptorCacheState {
    bytes: usize,
    next_generation: u64,
    entries: HashMap<String, DescriptorCacheEntry>,
    order: VecDeque<(String, u64)>,
    retired: Vec<(
        std::sync::Weak<TableSchema>,
        crate::engine::executor::mem_budget::ControlledMemoryCharge,
    )>,
}

#[derive(Debug)]
struct DescriptorCacheEntry {
    bytes: usize,
    generation: u64,
    weak: std::sync::Weak<TableSchema>,
    charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl CatalogManager {
    fn new(mut catalog: Catalog) -> Result<Self, QueryError> {
        let memory_target = crate::engine::executor::mem_budget::memory_target_bytes();
        let default_pct = if memory_target <= 1024 * 1024 * 1024 {
            2
        } else {
            5
        };
        let default_limit = memory_target
            .saturating_mul(default_pct)
            .saturating_div(100)
            .min(256 * 1024 * 1024);
        let configured = crate::util::env::var("MYDB_CATALOG_CACHE_MB")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .map(|mib| mib.saturating_mul(1024 * 1024))
            .unwrap_or(default_limit);
        let table_keys_by_id = Self::table_key_index(&catalog);
        let table_headers: HashMap<String, Arc<TableSchema>> = catalog
            .tables
            .iter()
            .map(|(key, table)| (key.clone(), Self::object_header(table)))
            .collect();
        let table_id_highwater = catalog.next_table_id;
        let sequence_id_highwater = catalog.next_sequence_id;
        let has_foreign_keys = catalog
            .tables
            .values()
            .any(|table| !table.foreign_keys.is_empty());
        // The DD loader necessarily constructs complete descriptors to
        // validate catalog invariants. Keeping all of them in `live` after
        // startup defeats the lazy descriptor cache and doubles them with
        // `table_headers`. Publish only compact headers; `Database::table`
        // reloads a descriptor from DD and installs it into the bounded cache.
        for (key, header) in &table_headers {
            catalog.tables.insert(key.clone(), Arc::clone(header));
        }
        let resident_bytes = Self::catalog_header_bytes(&catalog);
        let resident_charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
            resident_bytes,
        )?;
        Ok(Self {
            live: RwLock::new(catalog),
            table_keys_by_id: RwLock::new(table_keys_by_id),
            table_headers: RwLock::new(table_headers),
            descriptor_cache: Mutex::new(DescriptorCacheState::default()),
            dd_rows_by_object: RwLock::new(HashMap::new()),
            epoch: AtomicU64::new(1),
            cache_limit_bytes: AtomicUsize::new(configured),
            table_id_highwater: AtomicU64::new(table_id_highwater),
            sequence_id_highwater: AtomicU64::new(sequence_id_highwater),
            has_foreign_keys: AtomicBool::new(has_foreign_keys),
            resident_charge: Mutex::new(resident_charge),
            id_allocator_lock: Mutex::new(()),
            online_pinned_ids: parking_lot::RwLock::new(HashSet::new()),
            ddl_pinned_ids: parking_lot::RwLock::new(HashMap::new()),
        })
    }

    pub(super) fn object_header(table: &TableSchema) -> Arc<TableSchema> {
        let mut header = table.clone();
        // `Vec::clear`/`HashMap::clear` retain their backing allocations. A
        // seven-column descriptor therefore used to leave roughly the whole
        // descriptor resident in both `table_headers` and the live catalog
        // after descriptor-cache eviction. At 100k tables those supposedly
        // "small" headers alone exhausted a 1 GiB managed target. Replace the
        // containers so the lazy header owns zero child capacity.
        header.columns = Vec::new();
        header.primary_key = Vec::new();
        header.pending_primary_key = None;
        header.indexes = Vec::new();
        header.check_constraints = Vec::new();
        header.foreign_keys = Vec::new();
        header.fk_referenced_by = Vec::new();
        header.stats = crate::storage::TableStats::empty();
        header.row_layouts = Vec::new();
        header.column_name_map = HashMap::default();
        header.column_id_map = HashMap::new();
        header.row_codec = Arc::new(RowCodec::empty());
        Arc::new(header)
    }

    fn descriptor_bytes(table: &TableSchema) -> usize {
        std::mem::size_of::<TableSchema>()
            .saturating_add(table.database.capacity())
            .saturating_add(table.name.capacity())
            .saturating_add(table.engine.capacity())
            .saturating_add(table.charset.capacity())
            .saturating_add(table.collation.capacity())
            .saturating_add(table.comment.capacity())
            .saturating_add(table.columns.capacity().saturating_mul(768))
            .saturating_add(table.indexes.capacity().saturating_mul(1024))
            .saturating_add(table.check_constraints.capacity().saturating_mul(512))
            .saturating_add(table.foreign_keys.capacity().saturating_mul(768))
            .saturating_add(table.row_layouts.capacity().saturating_mul(256))
    }

    fn catalog_header_bytes(catalog: &Catalog) -> usize {
        let mut bytes = std::mem::size_of::<Catalog>();
        for (key, table) in &catalog.tables {
            bytes = bytes
                .saturating_add(key.capacity())
                .saturating_add(96)
                .saturating_add(Self::descriptor_bytes(&Self::object_header(table)));
            for _ in table.storage_ids() {
                bytes = bytes.saturating_add(48 + key.capacity());
            }
        }
        for (name, schema) in &catalog.databases {
            bytes = bytes
                .saturating_add(name.capacity())
                .saturating_add(schema.name.capacity())
                .saturating_add(schema.charset.capacity())
                .saturating_add(schema.collation.capacity())
                .saturating_add(96);
        }
        bytes
    }

    fn table_header_bytes(key: &str, table: &TableSchema) -> usize {
        let id_count = 1usize.saturating_add(table.storage_ids().len());
        key.len()
            .saturating_add(96)
            .saturating_add(Self::descriptor_bytes(&Self::object_header(table)))
            .saturating_add(id_count.saturating_mul(48 + key.len()))
    }

    fn reserve_table_header(&self, key: &str, table: &TableSchema) -> Result<(), QueryError> {
        if self
            .table_headers
            .read()
            .is_ok_and(|headers| headers.contains_key(key))
        {
            return Ok(());
        }
        let bytes = Self::table_header_bytes(key, table);
        let mut charge = self.resident_charge.lock().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "catalog resident charge lock poisoned",
            )
        })?;
        charge.try_grow(bytes)?;
        drop(charge);
        if let Ok(mut headers) = self.table_headers.write() {
            headers
                .entry(key.to_string())
                .or_insert_with(|| Self::object_header(table));
        }
        Ok(())
    }

    fn table_key_index(catalog: &Catalog) -> HashMap<u64, String> {
        let mut keys = HashMap::new();
        for (key, table) in &catalog.tables {
            keys.insert(table.id, key.clone());
            for storage_id in table.storage_ids() {
                keys.insert(storage_id, key.clone());
            }
        }
        keys
    }

    fn replace_live(&self, catalog: Catalog) -> Result<(), QueryError> {
        let resident_bytes = Self::catalog_header_bytes(&catalog);
        {
            let mut charge = self.resident_charge.lock().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "catalog resident charge lock poisoned",
                )
            })?;
            let charged = charge.bytes();
            if resident_bytes > charged {
                charge.try_grow(resident_bytes - charged)?;
            } else {
                charge.shrink(charged - resident_bytes);
            }
        }
        let keys = Self::table_key_index(&catalog);
        let table_id_highwater = catalog.next_table_id;
        let sequence_id_highwater = catalog.next_sequence_id;
        let has_foreign_keys = catalog
            .tables
            .values()
            .any(|table| !table.foreign_keys.is_empty());
        let headers = catalog
            .tables
            .iter()
            .map(|(key, table)| (key.clone(), Self::object_header(table)))
            .collect();
        *self
            .live
            .write()
            .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "catalog lock poisoned"))? =
            catalog;
        *self.table_keys_by_id.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "catalog id index lock poisoned",
            )
        })? = keys;
        *self.table_headers.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "catalog header lock poisoned",
            )
        })? = headers;
        *self.descriptor_cache.lock().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "catalog descriptor cache lock poisoned",
            )
        })? = DescriptorCacheState::default();
        self.table_id_highwater
            .store(table_id_highwater, Ordering::Release);
        self.sequence_id_highwater
            .store(sequence_id_highwater, Ordering::Release);
        self.has_foreign_keys
            .store(has_foreign_keys, Ordering::Release);
        Ok(())
    }

    fn table_key_by_id(&self, table_id: u64) -> Option<String> {
        self.table_keys_by_id.read().ok()?.get(&table_id).cloned()
    }

    fn dd_object_rows(&self, dd_table_id: u64, object_id: u64) -> Option<Vec<RowId>> {
        self.dd_rows_by_object
            .read()
            .ok()?
            .get(&(dd_table_id, object_id))
            .cloned()
    }

    fn publish_dd_object_rows(&self, updates: Vec<(u64, u64, Vec<RowId>)>) {
        if let Ok(mut rows) = self.dd_rows_by_object.write() {
            for (dd_table_id, object_id, row_ids) in updates {
                if row_ids.is_empty() {
                    rows.remove(&(dd_table_id, object_id));
                } else {
                    rows.insert((dd_table_id, object_id), row_ids);
                }
            }
        }
    }

    fn register_table(&self, key: &str, table: &TableSchema) {
        if !table.foreign_keys.is_empty() {
            self.has_foreign_keys.store(true, Ordering::Release);
        }
        if let Ok(mut keys) = self.table_keys_by_id.write() {
            keys.insert(table.id, key.to_string());
            for storage_id in table.storage_ids() {
                keys.insert(storage_id, key.to_string());
            }
        }
    }

    /// Release the startup-resident header and descriptor accounting only
    /// after the DROP catalog transaction has committed.
    fn forget_dropped_table(&self, key: &str, table: &TableSchema) {
        if let Ok(mut headers) = self.table_headers.write() {
            if headers.get(key).is_some_and(|header| header.id == table.id) {
                let header = headers.remove(key).expect("header checked above");
                if let Ok(mut charge) = self.resident_charge.lock() {
                    charge.shrink(Self::table_header_bytes(key, &header));
                }
            }
        }
        if let Ok(mut keys) = self.table_keys_by_id.write() {
            for id in std::iter::once(table.id).chain(table.storage_ids()) {
                if keys.get(&id).is_some_and(|mapped| mapped == key) {
                    keys.remove(&id);
                }
            }
        }
        if let Ok(mut cache) = self.descriptor_cache.lock() {
            let old_entry = cache.entries.get(key).is_some_and(|entry| {
                entry
                    .weak
                    .upgrade()
                    .is_none_or(|descriptor| descriptor.id == table.id)
            });
            if old_entry {
                if let Some(old) = cache.entries.remove(key) {
                    cache.bytes = cache.bytes.saturating_sub(old.bytes);
                    if old.weak.upgrade().is_some() {
                        cache.retired.push((old.weak, old.charge));
                    }
                }
                // The order queue contains generations, so stale entries can
                // be discarded independently of the descriptor's Arc owners.
                cache.order.retain(|(name, _)| name != key);
            }
            cache.retired.retain(|(weak, _)| weak.upgrade().is_some());
        }
    }

    fn may_have_foreign_keys(&self) -> bool {
        self.has_foreign_keys.load(Ordering::Acquire)
    }

    fn descriptor_is_pinned(&self, id: u64) -> bool {
        self.online_pinned_ids.read().contains(&id) || self.ddl_pinned_ids.read().contains_key(&id)
    }

    fn install_descriptor(&self, key: String, table: Arc<TableSchema>) {
        let bytes = Self::descriptor_bytes(&table);
        let limit = self.cache_limit_bytes();
        let Ok(mut headers) = self.table_headers.write() else {
            return;
        };
        let Ok(mut cache) = self.descriptor_cache.lock() else {
            return;
        };
        // Callers must resolve lazy descriptors outside every `live` guard.
        // This blocking publication preserves cache coherence under ordinary
        // concurrent readers; attempting to use it as an in-place read->write
        // upgrade would self-deadlock because std::sync::RwLock is not
        // upgradeable.
        let Ok(mut catalog) = self.live.write() else {
            return;
        };
        // A read-compatible DDL can publish while a cold reader decodes the
        // previous DD row outside the catalog latch. That reader may retain
        // its old immutable descriptor, but must never reinstall it over a
        // newer capture/public definition (which would lose index maintenance).
        if catalog.tables.get(&key).is_some_and(|current| {
            current.id != table.id || current.schema_version > table.schema_version
        }) {
            return;
        }
        self.register_table(&key, &table);
        // Eviction must preserve the committed identity/version, including a
        // TRUNCATE replacing a relation at the same name with a fresh ID.
        if headers.get(&key).is_none_or(|header| {
            header.id != table.id || header.schema_version != table.schema_version
        }) {
            headers.insert(key.clone(), Self::object_header(&table));
        }
        cache.retired.retain(|(weak, _)| weak.upgrade().is_some());
        if let Some(old) = cache.entries.remove(&key) {
            cache.bytes = cache.bytes.saturating_sub(old.bytes);
            if old.weak.upgrade().is_some() {
                cache.retired.push((old.weak, old.charge));
            }
        }
        // Active builds own a separately charged descriptor lease. Never
        // replace their capture definition with a lazy object header.
        if self.descriptor_is_pinned(table.id) {
            catalog.tables.insert(key, table);
            return;
        }
        while cache.bytes.saturating_add(bytes) > limit {
            let Some((victim_key, generation)) = cache.order.pop_front() else {
                break;
            };
            let Some(entry) = cache.entries.get(&victim_key) else {
                continue;
            };
            if generation != entry.generation {
                continue;
            }
            if catalog
                .tables
                .get(&victim_key)
                .is_some_and(|t| self.descriptor_is_pinned(t.id))
            {
                continue;
            }
            let victim = cache.entries.remove(&victim_key).expect("entry checked");
            cache.bytes = cache.bytes.saturating_sub(victim.bytes);
            if victim.weak.upgrade().is_some() {
                cache.retired.push((victim.weak, victim.charge));
            }
            if let Some(header) = headers.get(&victim_key).cloned() {
                catalog.tables.insert(victim_key, header);
            }
        }
        if bytes > limit || cache.bytes.saturating_add(bytes) > limit {
            if let Some(header) = headers.get(&key).cloned() {
                catalog.tables.insert(key, header);
            }
            return;
        }
        let Some(charge) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                bytes,
            )
        else {
            if let Some(header) = headers.get(&key).cloned() {
                catalog.tables.insert(key, header);
            }
            return;
        };
        cache.next_generation = cache.next_generation.wrapping_add(1).max(1);
        let generation = cache.next_generation;
        cache.entries.insert(
            key.clone(),
            DescriptorCacheEntry {
                bytes,
                generation,
                weak: Arc::downgrade(&table),
                charge,
            },
        );
        cache.order.push_back((key.clone(), generation));
        cache.bytes = cache.bytes.saturating_add(bytes);
        catalog.tables.insert(key, table);
    }

    fn publish_table_deltas(&self, object_ids: &[u64]) {
        let known_keys = self
            .table_keys_by_id
            .read()
            .map(|keys| {
                object_ids
                    .iter()
                    .map(|object_id| (*object_id, keys.get(object_id).cloned()))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();
        let changes = if let Ok(catalog) = self.live.read() {
            object_ids
                .iter()
                .map(|object_id| {
                    let known_key = known_keys.get(object_id).cloned().flatten();
                    let current = known_key
                        .as_ref()
                        .and_then(|key| catalog.tables.get(key).map(|table| (key.clone(), table)))
                        .filter(|(_, table)| table.id == *object_id)
                        .or_else(|| {
                            catalog
                                .tables
                                .iter()
                                .find(|(_, table)| table.id == *object_id)
                                .map(|(key, table)| (key.clone(), table))
                        });
                    (
                        *object_id,
                        current.map(|(key, table)| (key, Arc::clone(table))),
                    )
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let descriptors = changes
            .iter()
            .filter_map(|(_, current)| current.clone())
            .collect::<Vec<_>>();
        if let Ok(mut keys) = self.table_keys_by_id.write() {
            let mut installed = std::collections::BTreeSet::new();
            for (object_id, current) in changes {
                keys.remove(&object_id);
                if let Some((key, table)) = current {
                    if !installed.insert(key.clone()) {
                        continue;
                    }
                    keys.insert(table.id, key.clone());
                    for storage_id in table.storage_ids() {
                        keys.insert(storage_id, key.clone());
                    }
                }
            }
        }
        for (key, table) in descriptors {
            self.install_descriptor(key, table);
        }
        self.publish();
    }

    fn read(&self) -> std::sync::LockResult<RwLockReadGuard<'_, Catalog>> {
        self.live.read()
    }

    fn write(&self) -> std::sync::LockResult<RwLockWriteGuard<'_, Catalog>> {
        self.live.write()
    }

    fn publish(&self) {
        self.epoch.fetch_add(1, Ordering::Release);
    }

    fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    fn cache_limit_bytes(&self) -> usize {
        self.cache_limit_bytes.load(Ordering::Acquire)
    }

    fn set_cache_limit_bytes(&self, bytes: usize) {
        self.cache_limit_bytes.store(bytes, Ordering::Release);
    }

    fn highwater_target(next_id: u64) -> u64 {
        const BLOCK: u64 = 1024;
        next_id.saturating_add(BLOCK - 1) / BLOCK * BLOCK
    }
}

/// A primary-key selector for one bounded family of data-dictionary rows.
///
/// The v46 catalog store never rewrites unrelated objects.  Child metadata
/// tables use an object-id prefix; singleton metadata uses its complete
/// primary key.  `All` exists only for fresh-directory bootstrap and the
/// bounded auth catalog families.
#[derive(Debug, Clone)]
enum CatalogRowSelector {
    All,
    PrimaryValues(Vec<Value>),
    PrefixValues(Vec<Value>),
}

impl CatalogRowSelector {
    fn object_id(&self) -> Option<u64> {
        let values = match self {
            Self::PrimaryValues(values) | Self::PrefixValues(values) => values,
            Self::All => return None,
        };
        values.first().and_then(|value| match value {
            Value::UInt(value) => Some(*value),
            Value::Int(value) if *value >= 0 => Some(*value as u64),
            _ => None,
        })
    }
}

#[derive(Debug)]
struct CatalogRowReplacement {
    dd_table_id: u64,
    selector: CatalogRowSelector,
    rows: Vec<Vec<Value>>,
}

/// Transactional incremental catalog writer.
///
/// A transaction owns only the rows for objects named by `replacements`.
/// Heap versions, undo, primary-index maintenance and CommitTxn provide the
/// same crash atomicity as user DML; no generation marker or full catalog
/// snapshot participates in the commit.
struct CatalogTxn<'a> {
    db: &'a Database,
    sequence_changes: Vec<(Arc<SequenceSchema>, Option<Arc<SequenceSchema>>)>,
    sequence_leases: Vec<SequenceValueLease<'a>>,
    replacements: Vec<CatalogRowReplacement>,
    delta_object_ids: Vec<u64>,
    delta_invalidate_all: bool,
    #[cfg(test)]
    auth_mutation: bool,
    #[cfg(test)]
    ai_mutation: bool,
}

fn auth_json_value<T: serde::Serialize>(value: &T) -> Result<Value, QueryError> {
    serde_json::to_string(value)
        .map(Value::String)
        .map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("serialize auth DD row: {err}"),
            )
        })
}

impl<'a> CatalogTxn<'a> {
    fn new(db: &'a Database) -> Self {
        Self {
            db,
            sequence_changes: Vec::new(),
            sequence_leases: Vec::new(),
            replacements: Vec::new(),
            delta_object_ids: Vec::new(),
            delta_invalidate_all: false,
            #[cfg(test)]
            auth_mutation: false,
            #[cfg(test)]
            ai_mutation: false,
        }
    }

    #[inline(always)]
    fn mark_ai_catalog(&mut self) {
        #[cfg(test)]
        {
            self.ai_mutation = true;
        }
    }
    fn set_delta(&mut self, mut object_ids: Vec<u64>, invalidate_all: bool) {
        object_ids.sort_unstable();
        object_ids.dedup();
        self.delta_object_ids = object_ids;
        self.delta_invalidate_all = invalidate_all;
    }

    fn replace(&mut self, dd_table_id: u64, selector: CatalogRowSelector, rows: Vec<Vec<Value>>) {
        self.replacements.push(CatalogRowReplacement {
            dd_table_id,
            selector,
            rows,
        });
    }

    fn commit(self) -> Result<(), QueryError> {
        let txn = self.db.begin_txn()?;
        self.commit_in(txn, false)
    }

    fn commit_ddl(self) -> Result<(), QueryError> {
        let txn = self.db.begin_txn()?;
        self.commit_in(txn, true)
    }

    fn commit_existing(self, txn: TxnView) -> Result<(), QueryError> {
        self.commit_in(txn, true)
    }

    fn commit_in(mut self, txn: TxnView, stage_ddl_binlog: bool) -> Result<(), QueryError> {
        if stage_ddl_binlog {
            if let Some((key, value)) = crate::replication::current_ddl_receipt()? {
                self.replace(
                    crate::storage::data_dictionary::DD_DD_PROPERTIES,
                    CatalogRowSelector::PrimaryValues(vec![Value::String(key.clone())]),
                    vec![vec![Value::String(key), Value::String(value)]],
                );
            }
        }
        self.replacements
            .sort_by_key(|replacement| replacement.dd_table_id);
        // Independent table descriptors touch disjoint primary-key families
        // in every DD heap. Serialize by stable object id instead of one
        // process-wide latch; multi-object DDL acquires shards in numeric
        // order, so it cannot deadlock. Non-object singleton families (schema,
        // auth/properties) intentionally share shard 0.
        let mut lock_shards = self
            .replacements
            .iter()
            .map(|replacement| {
                replacement
                    .selector
                    .object_id()
                    .or_else(|| {
                        if replacement.dd_table_id
                            != crate::storage::data_dictionary::DD_DDL_RETIREMENTS
                        {
                            return None;
                        }
                        match &replacement.selector {
                            CatalogRowSelector::PrimaryValues(values) => match values.first() {
                                Some(Value::UInt(id)) => Some(*id),
                                _ => None,
                            },
                            _ => None,
                        }
                    })
                    .map(|id| id as usize % CATALOG_DD_COMMIT_SHARDS)
                    .unwrap_or(0)
            })
            .collect::<Vec<_>>();
        lock_shards.sort_unstable();
        lock_shards.dedup();
        if lock_shards.is_empty() {
            lock_shards.push(0);
        }
        let mut _dd_commit_guards = Vec::with_capacity(lock_shards.len());
        for shard in lock_shards {
            _dd_commit_guards.push(CATALOG_DD_COMMIT_LOCKS[shard].lock().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "catalog DD commit shard lock poisoned",
                )
            })?);
        }
        // DD row mutations are the physical implementation of the enclosing
        // DDL query. They must not appear as a second user binlog transaction;
        // statement replication already records the DDL itself.
        let _suppress_binlog = crate::binlog::SessionSqlLogBinGuard::new(false);
        let mut mutation_rows = 0u64;
        let mut locator_updates = Vec::new();
        let result = (|| -> Result<(), QueryError> {
            for replacement in &self.replacements {
                let (changed, inserted) =
                    self.db.apply_catalog_row_replacement(txn, replacement)?;
                mutation_rows = mutation_rows.saturating_add(changed as u64);
                if let Some(object_id) = replacement.selector.object_id() {
                    locator_updates.push((replacement.dd_table_id, object_id, inserted));
                }
            }
            #[cfg(test)]
            if self.auth_mutation {
                crate::engine::auth::test_faults::hit("after_dd_rows")?;
            }
            #[cfg(test)]
            if self.ai_mutation {
                crate::engine::ai::catalog_faults::hit("after_dd_rows")?;
            }
            self.db.append_wal(
                txn.txn_id,
                WalPayload::CatalogDelta {
                    object_ids: self.delta_object_ids.clone(),
                    invalidate_all: self.delta_invalidate_all,
                },
            )?;
            if stage_ddl_binlog {
                crate::binlog::stage_current_ddl_transaction(txn.txn_id)?;
            }
            self.db.commit_txn(txn.txn_id).map(|_| ()).map_err(|err| {
                log_message(&format!(
                    "CatalogTxn commit failed txn_id={} replacements={}: {err}",
                    txn.txn_id,
                    self.replacements.len()
                ));
                err
            })
        })();
        if let Err(err) = result {
            let _ = self.db.rollback_txn(txn.txn_id);
            CATALOG_TXN_ROLLBACKS.fetch_add(1, Ordering::Relaxed);
            return Err(err);
        }
        CATALOG_MUTATED_ROWS.fetch_add(mutation_rows, Ordering::Relaxed);
        CATALOG_TXN_COMMITS.fetch_add(1, Ordering::Relaxed);
        for (old, new) in &self.sequence_changes {
            match new {
                Some(def) => self.db.sequences.update_def(def.clone()),
                None => self.db.sequences.remove(old.id),
            }
        }
        self.sequence_changes.clear();
        self.db.catalog.publish_dd_object_rows(locator_updates);
        self.db.catalog.publish_table_deltas(&self.delta_object_ids);
        Ok(())
    }
}

impl Database {
    fn delete_auto_increment_metadata(&self, table_id: u64) -> Result<(), QueryError> {
        use crate::storage::data_dictionary::DD_INNODB_DYNAMIC_METADATA;
        let mut txn = CatalogTxn::new(self);
        txn.replace(
            DD_INNODB_DYNAMIC_METADATA,
            CatalogRowSelector::PrimaryValues(vec![Value::UInt(table_id)]),
            Vec::new(),
        );
        txn.set_delta(Vec::new(), false);
        txn.commit()
    }

    fn persist_auto_increment_checkpoint(
        &self,
        states: &[Arc<AutoIncrementState>],
    ) -> Result<(), QueryError> {
        if states.is_empty() || self.config.durability == DurabilityMode::Memory {
            return Ok(());
        }
        use crate::storage::data_dictionary::DD_INNODB_DYNAMIC_METADATA;
        let mut by_table = BTreeMap::<u64, (u64, u64, u64)>::new();
        for state in states {
            if state.retired.load(Ordering::Acquire) || is_temp_table_id(state.table_id) {
                continue;
            }
            let logged = *state.logged.lock();
            let candidate = (logged.version.max(1), logged.max_value, logged.lsn);
            by_table
                .entry(state.table_id)
                .and_modify(|current| {
                    if candidate.0 > current.0
                        || (candidate.0 == current.0 && candidate.1 > current.1)
                    {
                        *current = candidate;
                    }
                })
                .or_insert(candidate);
        }
        if by_table.is_empty() {
            return Ok(());
        }
        let mut txn = CatalogTxn::new(self);
        for (table_id, (version, max_value, covered_lsn)) in by_table {
            let existing = self.read_auto_increment_metadata(table_id)?;
            let chosen = match existing {
                Some(current) if current.0 > version => current,
                Some(current) if current.0 == version && current.1 > max_value => current,
                Some(current) if current.0 == version && current.1 == max_value => {
                    (version, max_value, current.2.max(covered_lsn))
                }
                _ => (version, max_value, covered_lsn),
            };
            txn.replace(
                DD_INNODB_DYNAMIC_METADATA,
                CatalogRowSelector::PrimaryValues(vec![Value::UInt(table_id)]),
                vec![vec![
                    Value::UInt(table_id),
                    Value::UInt(chosen.0),
                    Value::UInt(chosen.1),
                    Value::UInt(chosen.2),
                ]],
            );
        }
        txn.set_delta(Vec::new(), false);
        txn.commit()
    }

    pub(crate) fn commit_auth_catalog_delta(
        &self,
        before: &crate::engine::auth::AuthCatalog,
        after: &crate::engine::auth::AuthCatalog,
    ) -> Result<(), QueryError> {
        use crate::storage::data_dictionary::{
            auth_grant_key, auth_restriction_key, auth_role_edge_key, DD_DD_PROPERTIES,
            DD_GLOBAL_PRIVILEGES, DD_RESTRICTIONS, DD_ROLE_EDGES, DD_USERS,
        };

        let mut txn = CatalogTxn::new(self);
        txn.set_delta(Vec::new(), true);
        #[cfg(test)]
        {
            txn.auth_mutation = true;
        }
        for change in before.accounts.diff(&after.accounts) {
            use imbl::ordmap::DiffItem::*;
            let (old, new) = match change {
                Add(_, v) => (None, Some(v)),
                Remove(_, v) => (Some(v), None),
                Update {
                    old: (_, a),
                    new: (_, b),
                } => (Some(a), Some(b)),
            };
            let account = new.or(old).unwrap();
            let rows = new
                .map(|a| {
                    Ok(vec![vec![
                        Value::String(a.key.user.clone()),
                        Value::String(a.key.host.clone()),
                        auth_json_value(a)?,
                    ]])
                })
                .transpose()?
                .unwrap_or_default();
            txn.replace(
                DD_USERS,
                CatalogRowSelector::PrimaryValues(vec![
                    Value::String(account.key.user.clone()),
                    Value::String(account.key.host.clone()),
                ]),
                rows,
            );
        }

        // Ordinals are snapshot-local. Only inspect changed slots, then
        // coalesce by persistent stable key (swap-remove may move a survivor).
        let mut grant_delta = BTreeMap::new();
        for i in after.grants.changed_slots() {
            if let Some(old) = before.grants.get(i) {
                grant_delta
                    .entry(auth_grant_key(old))
                    .or_insert((None, None))
                    .0 = Some(old);
            }
            if let Some(new) = after.grants.get(i) {
                grant_delta
                    .entry(auth_grant_key(new))
                    .or_insert((None, None))
                    .1 = Some(new);
            }
        }
        for (key, (old, new)) in grant_delta {
            if old == new {
                continue;
            }
            let rows = new
                .map(|grant| {
                    Ok(vec![vec![
                        Value::String(key.clone()),
                        auth_json_value(grant)?,
                    ]])
                })
                .transpose()?
                .unwrap_or_default();
            txn.replace(
                DD_GLOBAL_PRIVILEGES,
                CatalogRowSelector::PrimaryValues(vec![Value::String(key)]),
                rows,
            );
        }

        let mut edge_delta = BTreeMap::new();
        for (old, new, default) in [
            (&before.role_edges, &after.role_edges, false),
            (&before.default_roles, &after.default_roles, true),
        ] {
            for i in new.changed_slots() {
                if let Some(e) = old.get(i) {
                    edge_delta
                        .entry(auth_role_edge_key(e, default))
                        .or_insert((None, None, default))
                        .0 = Some(e);
                }
                if let Some(e) = new.get(i) {
                    edge_delta
                        .entry(auth_role_edge_key(e, default))
                        .or_insert((None, None, default))
                        .1 = Some(e);
                }
            }
        }
        for (key, (old, new, default)) in edge_delta {
            if old == new {
                continue;
            }
            let rows = new
                .map(|e| {
                    Ok(vec![vec![
                        Value::String(key.clone()),
                        Value::Int(i64::from(default)),
                        auth_json_value(e)?,
                    ]])
                })
                .transpose()?
                .unwrap_or_default();
            txn.replace(
                DD_ROLE_EDGES,
                CatalogRowSelector::PrimaryValues(vec![Value::String(key)]),
                rows,
            );
        }

        let mut restriction_delta = BTreeMap::new();
        for i in after.restrictions.changed_slots() {
            if let Some(r) = before.restrictions.get(i) {
                restriction_delta
                    .entry(auth_restriction_key(r))
                    .or_insert((None, None))
                    .0 = Some(r);
            }
            if let Some(r) = after.restrictions.get(i) {
                restriction_delta
                    .entry(auth_restriction_key(r))
                    .or_insert((None, None))
                    .1 = Some(r);
            }
        }
        for (key, (old, new)) in restriction_delta {
            if old == new {
                continue;
            }
            let rows = new
                .map(|r| Ok(vec![vec![Value::String(key.clone()), auth_json_value(r)?]]))
                .transpose()?
                .unwrap_or_default();
            txn.replace(
                DD_RESTRICTIONS,
                CatalogRowSelector::PrimaryValues(vec![Value::String(key)]),
                rows,
            );
        }

        txn.replace(
            DD_DD_PROPERTIES,
            CatalogRowSelector::PrimaryValues(vec![Value::String("auth_epoch".to_string())]),
            vec![vec![
                Value::String("auth_epoch".to_string()),
                Value::String(after.epoch.to_string()),
            ]],
        );
        // Account SQL is rendered from the published password hashes by the
        // executor. Never stage its original cleartext authentication clause.
        txn.commit()
    }

    pub(crate) fn count_catalog_index_parts(
        &self,
        table_ids: &HashSet<u64>,
    ) -> Result<u64, QueryError> {
        use crate::storage::data_dictionary::DD_INDEX_COLUMN_USAGE;
        if table_ids.is_empty() {
            return Ok(0);
        }
        let txns = self.txns_read()?;
        let mut count = 0u64;
        self.with_heap_read(DD_INDEX_COLUMN_USAGE, |heap| {
            heap.scan_mvcc_until(|row| {
                if row.header.is_deleted()
                    || matches!(
                        txns.status(row.header.last_txn_id),
                        Some(TxnStatus::Active | TxnStatus::Prepared | TxnStatus::RolledBack)
                    )
                {
                    return Ok(false);
                }
                let table_id = row.values.first().and_then(|value| match value {
                    Value::UInt(value) => Some(*value),
                    Value::Int(value) if *value >= 0 => Some(*value as u64),
                    _ => None,
                });
                if table_id.is_some_and(|table_id| table_ids.contains(&table_id)) {
                    count = count.saturating_add(1);
                }
                Ok(false)
            })
        })?;
        Ok(count)
    }

    pub fn catalog_cache_mb(&self) -> u64 {
        (self.catalog.cache_limit_bytes() / (1024 * 1024)) as u64
    }

    pub fn set_catalog_cache_mb(&self, value: u64) {
        let bytes = value
            .min((usize::MAX / (1024 * 1024)) as u64)
            .saturating_mul(1024 * 1024) as usize;
        self.catalog.set_cache_limit_bytes(bytes);
    }

    fn ensure_table_id_highwater(&self, next_id: u64) -> Result<(), QueryError> {
        let _guard = self.catalog.id_allocator_lock.lock().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "catalog ID allocator lock poisoned",
            )
        })?;
        let target = CatalogManager::highwater_target(next_id);
        if target <= self.catalog.table_id_highwater.load(Ordering::Acquire) {
            return Ok(());
        }
        use crate::storage::data_dictionary::DD_DD_PROPERTIES;
        let mut txn = CatalogTxn::new(self);
        txn.replace(
            DD_DD_PROPERTIES,
            CatalogRowSelector::PrimaryValues(vec![Value::String("next_table_id".to_string())]),
            vec![vec![
                Value::String("next_table_id".to_string()),
                Value::String(target.to_string()),
            ]],
        );
        txn.set_delta(Vec::new(), false);
        txn.commit()?;
        self.catalog
            .table_id_highwater
            .store(target, Ordering::Release);
        Ok(())
    }

    fn current_next_table_id(&self) -> Result<u64, QueryError> {
        Ok(self.catalog_read()?.next_table_id)
    }

    fn ensure_sequence_id_highwater(&self, next_id: u64) -> Result<(), QueryError> {
        let _guard = self.catalog.id_allocator_lock.lock().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "catalog ID allocator lock poisoned",
            )
        })?;
        let target = CatalogManager::highwater_target(next_id);
        if target <= self.catalog.sequence_id_highwater.load(Ordering::Acquire) {
            return Ok(());
        }
        use crate::storage::data_dictionary::DD_DD_PROPERTIES;
        let mut txn = CatalogTxn::new(self);
        txn.replace(
            DD_DD_PROPERTIES,
            CatalogRowSelector::PrimaryValues(vec![Value::String("next_sequence_id".to_string())]),
            vec![vec![
                Value::String("next_sequence_id".to_string()),
                Value::String(target.to_string()),
            ]],
        );
        txn.set_delta(Vec::new(), false);
        txn.commit()?;
        self.catalog
            .sequence_id_highwater
            .store(target, Ordering::Release);
        Ok(())
    }

    fn current_next_sequence_id(&self) -> Result<u64, QueryError> {
        Ok(self.catalog_read()?.next_sequence_id)
    }

    fn note_catalog_mutation(&self, mutation: CatalogMutation) {
        let _ = self;
        CATALOG_MUTATIONS.with(|mutations| {
            mutations.borrow_mut().insert(mutation);
        });
    }

    fn note_catalog_schema(&self, name: &str) {
        self.note_catalog_mutation(CatalogMutation::Schema(normalize_name(name)));
    }

    // Version-only changes must never serialize a lazy header as a full table.
    fn note_catalog_table_version(&self, table_id: u64, version: u64) {
        self.note_catalog_mutation(CatalogMutation::TableVersion(table_id, version));
        if let Some(key) = self.catalog.table_key_by_id(table_id) {
            // Keep the eviction fallback coherent with the live version.
            if let Ok(mut headers) = self.catalog.table_headers.write() {
                if let Some(header) = headers.get_mut(&key).filter(|t| t.id == table_id) {
                    Arc::make_mut(header).schema_version = version;
                }
            }
            if let Ok(mut catalog) = self.catalog.write() {
                if let Some(table) = catalog.tables.get_mut(&key).filter(|t| t.id == table_id) {
                    Arc::make_mut(table).schema_version = version;
                }
            }
        }
    }

    fn note_catalog_table(&self, table_id: u64) {
        self.note_catalog_mutation(CatalogMutation::Table(table_id));
    }

    fn note_catalog_stats(&self, table_id: u64) {
        self.note_catalog_mutation(CatalogMutation::Stats(table_id));
    }

    fn note_catalog_table_key(&self, key: &str) -> Result<u64, QueryError> {
        let table = self
            .catalog_read()?
            .tables
            .get(key)
            .cloned()
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    format!("catalog table '{key}' disappeared before DD commit"),
                )
            })?;
        self.catalog.register_table(key, &table);
        self.note_catalog_table(table.id);
        Ok(table.id)
    }

    fn register_catalog_table_mapping(&self, key: &str) -> Result<(), QueryError> {
        let table = self
            .catalog_read()?
            .tables
            .get(key)
            .cloned()
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    format!("catalog table '{key}' disappeared before physical prepare"),
                )
            })?;
        self.catalog.reserve_table_header(key, &table)?;
        self.catalog.register_table(key, &table);
        Ok(())
    }

    fn note_catalog_view(&self, schema: &str, name: &str) {
        self.note_catalog_mutation(CatalogMutation::View {
            schema: normalize_name(schema),
            name: normalize_name(name),
        });
    }

    fn note_catalog_routine(&self, schema: &str, kind: &str, name: &str) {
        self.note_catalog_mutation(CatalogMutation::Routine {
            schema: normalize_name(schema),
            kind: kind.to_ascii_uppercase(),
            name: normalize_name(name),
        });
    }

    fn note_catalog_trigger(&self, schema: &str, name: &str) {
        self.note_catalog_mutation(CatalogMutation::Trigger {
            schema: normalize_name(schema),
            name: normalize_name(name),
        });
    }

    fn note_catalog_event(&self, schema: &str, name: &str) {
        self.note_catalog_mutation(CatalogMutation::Event {
            schema: normalize_name(schema),
            name: normalize_name(name),
        });
    }

    fn note_catalog_sequence(&self, sequence_id: u64) {
        self.note_catalog_mutation(CatalogMutation::Sequence(sequence_id));
    }

    fn note_catalog_auth(&self) {
        self.note_catalog_mutation(CatalogMutation::Auth);
    }

    fn take_catalog_mutations() -> Vec<CatalogMutation> {
        CATALOG_MUTATIONS.with(|mutations| mutations.borrow_mut().drain().collect())
    }

    fn build_catalog_txn_incremental(&self) -> Result<Option<CatalogTxn<'_>>, QueryError> {
        use crate::storage::data_dictionary::{
            dd_row_for_event, dd_row_for_routine, dd_row_for_schema, dd_row_for_sequence,
            dd_row_for_table_stats, dd_row_for_trigger, dd_row_for_view, dd_rows_for_auth,
            dd_rows_for_table, DD_CHECK_CONSTRAINTS, DD_COLUMNS, DD_DD_PROPERTIES, DD_EVENTS,
            DD_FOREIGN_KEYS, DD_FOREIGN_KEY_COLUMN_USAGE, DD_GLOBAL_PRIVILEGES, DD_INDEXES,
            DD_INDEX_COLUMN_USAGE, DD_RESTRICTIONS, DD_ROLE_EDGES, DD_ROUTINES, DD_SCHEMATA,
            DD_SEQUENCES, DD_TABLES, DD_TABLE_STATS, DD_TRIGGERS, DD_USERS, DD_VIEWS,
        };
        let mut mutations = Self::take_catalog_mutations();
        if mutations.is_empty() {
            return Ok(None);
        }
        let mut txn = CatalogTxn::new(self);
        txn.stage_sequence_dependencies(&mut mutations)?;
        let delta_object_ids = mutations
            .iter()
            .filter_map(|mutation| match mutation {
                CatalogMutation::Table(table_id)
                | CatalogMutation::TableVersion(table_id, _)
                | CatalogMutation::Stats(table_id) => Some(*table_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        let delta_invalidate_all = mutations.iter().any(|mutation| {
            !matches!(
                mutation,
                CatalogMutation::Table(_)
                    | CatalogMutation::TableVersion(_, _)
                    | CatalogMutation::Stats(_)
                    | CatalogMutation::SequenceValue(_, _)
                    | CatalogMutation::SequenceRetirement(_, _)
            )
        });
        txn.set_delta(delta_object_ids, delta_invalidate_all);
        let mut versions = BTreeMap::<u64, u64>::new();
        let mut replaced_tables = HashSet::new();
        for mutation in &mutations {
            match mutation {
                CatalogMutation::TableVersion(id, version) => {
                    versions
                        .entry(*id)
                        .and_modify(|v| *v = (*v).max(*version))
                        .or_insert(*version);
                }
                CatalogMutation::Table(id) => {
                    replaced_tables.insert(*id);
                }
                _ => {}
            }
        }
        // Read DD rows outside the live catalog guard. Coalesce deferred bumps,
        // and leave full replacements to the ordinary Table mutation below.
        for (table_id, version) in versions {
            if replaced_tables.contains(&table_id) {
                continue;
            }
            let selector = CatalogRowSelector::PrimaryValues(vec![Value::UInt(table_id)]);
            let mut rows = self.catalog_rows_for_selector(DD_TABLES, selector.clone())?;
            if rows.len() != 1 {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "version update requires an existing DD table row",
                ));
            }
            let schema = crate::storage::data_dictionary::dd_table_schema(DD_TABLES)
                .expect("compiled DD tables schema");
            rows[0][schema.column_index("schema_version")?] = Value::UInt(version);
            txn.replace(DD_TABLES, selector, rows);
        }
        let catalog = self.catalog_read()?;
        for mutation in mutations {
            match mutation {
                CatalogMutation::Schema(name) => {
                    let rows = catalog
                        .databases
                        .get(&name)
                        .map(dd_row_for_schema)
                        .into_iter()
                        .collect();
                    txn.replace(
                        DD_SCHEMATA,
                        CatalogRowSelector::PrimaryValues(vec![Value::String(name)]),
                        rows,
                    );
                }
                CatalogMutation::Table(table_id) => {
                    let table = self
                        .catalog
                        .table_key_by_id(table_id)
                        .and_then(|key| catalog.tables.get(&key))
                        .filter(|table| table.id == table_id)
                        .or_else(|| catalog.tables.values().find(|table| table.id == table_id));
                    if table.is_some_and(|table| !table.system && table.columns.is_empty()) {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "refusing to persist an incomplete table descriptor",
                        ));
                    }
                    let rows = table
                        .map(|table| dd_rows_for_table(table))
                        .unwrap_or_default();
                    txn.replace(
                        DD_TABLES,
                        CatalogRowSelector::PrimaryValues(vec![Value::UInt(table_id)]),
                        rows.tables,
                    );
                    for (dd_table_id, child_rows) in [
                        (DD_COLUMNS, rows.columns),
                        (DD_INDEXES, rows.indexes),
                        (DD_INDEX_COLUMN_USAGE, rows.index_column_usage),
                        (DD_CHECK_CONSTRAINTS, rows.check_constraints),
                        (DD_FOREIGN_KEYS, rows.foreign_keys),
                        (DD_FOREIGN_KEY_COLUMN_USAGE, rows.foreign_key_column_usage),
                        (DD_TABLE_STATS, rows.table_stats),
                    ] {
                        txn.replace(
                            dd_table_id,
                            CatalogRowSelector::PrefixValues(vec![Value::UInt(table_id)]),
                            child_rows,
                        );
                    }
                }
                CatalogMutation::TableVersion(_, _) => {}
                CatalogMutation::Stats(table_id) => {
                    let rows = self
                        .catalog
                        .table_key_by_id(table_id)
                        .and_then(|key| catalog.tables.get(&key))
                        .filter(|table| table.id == table_id)
                        .or_else(|| catalog.tables.values().find(|table| table.id == table_id))
                        .map(|table| vec![dd_row_for_table_stats(table)])
                        .unwrap_or_default();
                    txn.replace(
                        DD_TABLE_STATS,
                        CatalogRowSelector::PrimaryValues(vec![Value::UInt(table_id)]),
                        rows,
                    );
                }
                CatalogMutation::View { schema, name } => {
                    let key = table_key(&schema, &name);
                    let rows = catalog
                        .views
                        .get(&key)
                        .map(dd_row_for_view)
                        .into_iter()
                        .collect();
                    txn.replace(
                        DD_VIEWS,
                        CatalogRowSelector::PrimaryValues(vec![
                            Value::String(schema),
                            Value::String(name),
                        ]),
                        rows,
                    );
                }
                CatalogMutation::Routine { schema, kind, name } => {
                    let key = routine_key(&schema, &kind, &name);
                    let rows = catalog
                        .routines
                        .get(&key)
                        .map(|routine| dd_row_for_routine(routine))
                        .into_iter()
                        .collect();
                    txn.replace(
                        DD_ROUTINES,
                        CatalogRowSelector::PrimaryValues(vec![
                            Value::String(schema),
                            Value::String(kind),
                            Value::String(name),
                        ]),
                        rows,
                    );
                }
                CatalogMutation::Trigger { schema, name } => {
                    let key = trigger_key(&schema, &name);
                    let rows = catalog
                        .triggers
                        .get(&key)
                        .map(dd_row_for_trigger)
                        .into_iter()
                        .collect();
                    txn.replace(
                        DD_TRIGGERS,
                        CatalogRowSelector::PrimaryValues(vec![
                            Value::String(schema),
                            Value::String(name),
                        ]),
                        rows,
                    );
                }
                CatalogMutation::Event { schema, name } => {
                    let key = event_key(&schema, &name);
                    let rows = catalog
                        .events
                        .get(&key)
                        .map(dd_row_for_event)
                        .into_iter()
                        .collect();
                    txn.replace(
                        DD_EVENTS,
                        CatalogRowSelector::PrimaryValues(vec![
                            Value::String(schema),
                            Value::String(name),
                        ]),
                        rows,
                    );
                }
                CatalogMutation::Sequence(sequence_id) => {
                    let rows = catalog
                        .sequences
                        .values()
                        .find(|sequence| sequence.id == sequence_id)
                        .map(|sequence| dd_row_for_sequence(sequence))
                        .into_iter()
                        .collect();
                    txn.replace(
                        DD_SEQUENCES,
                        CatalogRowSelector::PrimaryValues(vec![Value::UInt(sequence_id)]),
                        rows,
                    );
                }
                CatalogMutation::SequenceRetirement(counter_id, sequence_id) => {
                    let through = self.log.lock().next_lsn().saturating_sub(1);
                    txn.replace(
                        crate::storage::data_dictionary::DD_SEQUENCE_RETIREMENTS,
                        CatalogRowSelector::PrimaryValues(vec![Value::UInt(counter_id)]),
                        vec![vec![
                            Value::UInt(counter_id),
                            Value::UInt(sequence_id),
                            Value::UInt(through),
                        ]],
                    );
                }
                CatalogMutation::SequenceValue(counter_id, value) => {
                    txn.replace(
                        crate::storage::data_dictionary::DD_SEQUENCE_VALUES,
                        CatalogRowSelector::PrimaryValues(vec![Value::UInt(counter_id)]),
                        value.map(|v| v.to_row()).into_iter().collect(),
                    );
                }
                CatalogMutation::Auth => {
                    let rows = dd_rows_for_auth(&catalog.auth);
                    for (dd_table_id, family) in [
                        (DD_USERS, rows.users),
                        (DD_GLOBAL_PRIVILEGES, rows.grants),
                        (DD_ROLE_EDGES, rows.role_edges),
                        (DD_RESTRICTIONS, rows.restrictions),
                    ] {
                        txn.replace(dd_table_id, CatalogRowSelector::All, family);
                    }
                    let auth_epoch = rows
                        .dd_properties
                        .into_iter()
                        .filter(|row| {
                            row.first().and_then(crate::types::value_str) == Some("auth_epoch")
                        })
                        .collect();
                    txn.replace(
                        DD_DD_PROPERTIES,
                        CatalogRowSelector::PrimaryValues(vec![Value::String(
                            "auth_epoch".to_string(),
                        )]),
                        auth_epoch,
                    );
                }
            }
        }
        drop(catalog);
        Ok(Some(txn))
    }

    fn commit_catalog_txn_incremental(&self) -> Result<(), QueryError> {
        match self.build_catalog_txn_incremental()? {
            Some(txn) => txn.commit(),
            None => Ok(()),
        }
    }

    fn commit_catalog_txn_existing(&self, txn_view: TxnView) -> Result<(), QueryError> {
        #[cfg(test)]
        if DDL_CATALOG_COMMIT_FAILURE.with(|fail| fail.replace(false)) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "injected catalog commit failure",
            ));
        }
        match self.build_catalog_txn_incremental()? {
            Some(txn) => txn.commit_existing(txn_view),
            None => self.commit_txn(txn_view.txn_id).map(|_| ()),
        }
    }

    fn dd_primary_index(table: &TableSchema) -> Result<&IndexSchema, QueryError> {
        table
            .indexes
            .iter()
            .find(|index| index.primary)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "data dictionary table mysql.{} has no primary index",
                        table.name
                    ),
                )
            })
    }

    fn catalog_selected_row_ids(
        &self,
        table: &TableSchema,
        selector: &CatalogRowSelector,
    ) -> Result<Vec<RowId>, QueryError> {
        if let Some(object_id) = selector.object_id() {
            if let Some(row_ids) = self.catalog.dd_object_rows(table.id, object_id) {
                return Ok(row_ids);
            }
        }
        match selector {
            CatalogRowSelector::All => {
                let mut row_ids = Vec::new();
                self.with_heap_read(table.id, |heap| {
                    heap.scan_mvcc_until(|row| {
                        row_ids.push(row.row_id);
                        Ok(false)
                    })
                })?;
                Ok(row_ids)
            }
            CatalogRowSelector::PrimaryValues(values) => {
                let primary = Self::dd_primary_index(table)?;
                let key = Self::dd_selector_key(table, primary, values)?;
                self.lookup_index(table, primary, key)
            }
            CatalogRowSelector::PrefixValues(values) => {
                let primary = Self::dd_primary_index(table)?;
                let key = Self::dd_selector_key(table, primary, values)?;
                self.prefix_index(table, primary, key)
            }
        }
    }

    fn catalog_rows_for_selector(
        &self,
        dd_table_id: u64,
        selector: CatalogRowSelector,
    ) -> Result<Vec<Vec<Value>>, QueryError> {
        let table =
            crate::storage::data_dictionary::dd_table_schema(dd_table_id).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("unknown data dictionary table id {dd_table_id}"),
                )
            })?;
        let row_ids = self
            .catalog_selected_row_ids(&table, &selector)
            .map_err(|err| {
                log_message(&format!(
                    "DD selector failed table_id={dd_table_id} selector={selector:?}: {err}"
                ));
                err
            })?;
        let mut rows = Vec::with_capacity(row_ids.len());
        for row_id in row_ids {
            let Some(row) = self.read_mvcc_row(dd_table_id, row_id).map_err(|err| {
                log_message(&format!(
                    "DD row read failed table_id={dd_table_id} row_id={row_id:?}: {err}"
                ));
                err
            })?
            else {
                continue;
            };
            if row.header.is_deleted() {
                continue;
            }
            rows.push(row.values);
        }
        Ok(rows)
    }

    /// Decode one table descriptor using bounded primary-index range probes.
    /// This is the CatalogManager miss path; it never scans unrelated tables.
    fn load_table_descriptor_from_dd(
        &self,
        table_id: u64,
    ) -> Result<Option<Arc<TableSchema>>, QueryError> {
        if let Some(pin) = self.online_index_descriptors.read().get(&table_id) {
            return Ok(Some(pin.schema.clone()));
        }
        let Some(base) = self.decode_table_descriptor_rows(table_id)? else {
            return Ok(None);
        };
        let mut graph = super::catalog::empty_catalog();
        let parent_key = table_key(&base.database, &base.name);
        graph.tables.insert(parent_key.clone(), base);
        for child_id in self.inbound_fk_child_ids(&graph.tables[&parent_key])? {
            if child_id == table_id {
                continue;
            }
            if let Some(child) = self.decode_table_descriptor_rows(child_id)? {
                graph
                    .tables
                    .insert(table_key(&child.database, &child.name), child);
            }
        }
        crate::storage::schema::build_fk_runtime(&mut graph);
        Ok(graph.tables.remove(&parent_key))
    }

    fn decode_table_descriptor_rows(
        &self,
        table_id: u64,
    ) -> Result<Option<Arc<TableSchema>>, QueryError> {
        self.decode_table_descriptor_rows_with_building(table_id, false)
    }

    fn decode_table_descriptor_rows_with_building(
        &self,
        table_id: u64,
        keep_building: bool,
    ) -> Result<Option<Arc<TableSchema>>, QueryError> {
        use crate::storage::data_dictionary::{
            catalog_schema_from_dd_rows, DdRowSet, DD_CHECK_CONSTRAINTS, DD_COLUMNS,
            DD_FOREIGN_KEYS, DD_FOREIGN_KEY_COLUMN_USAGE, DD_INDEXES, DD_INDEX_COLUMN_USAGE,
            DD_TABLES, DD_TABLE_STATS,
        };
        let tables = self.catalog_rows_for_selector(
            DD_TABLES,
            CatalogRowSelector::PrimaryValues(vec![Value::UInt(table_id)]),
        )?;
        if tables.is_empty() {
            return Ok(None);
        }
        let prefix = || CatalogRowSelector::PrefixValues(vec![Value::UInt(table_id)]);
        let set = DdRowSet {
            tables,
            columns: self.catalog_rows_for_selector(DD_COLUMNS, prefix())?,
            indexes: self.catalog_rows_for_selector(DD_INDEXES, prefix())?,
            index_column_usage: self.catalog_rows_for_selector(DD_INDEX_COLUMN_USAGE, prefix())?,
            check_constraints: self.catalog_rows_for_selector(DD_CHECK_CONSTRAINTS, prefix())?,
            foreign_keys: self.catalog_rows_for_selector(DD_FOREIGN_KEYS, prefix())?,
            foreign_key_column_usage: self
                .catalog_rows_for_selector(DD_FOREIGN_KEY_COLUMN_USAGE, prefix())?,
            table_stats: self.catalog_rows_for_selector(DD_TABLE_STATS, prefix())?,
            ..DdRowSet::default()
        };
        let mut decoded = catalog_schema_from_dd_rows(&set)?;
        for table in decoded.tables.values_mut() {
            if let Some(table) = Arc::get_mut(table) {
                if !keep_building {
                    table
                        .indexes
                        .retain(|index| index.state != IndexState::Building);
                    table.pending_primary_key = None;
                }
                table.build_column_index();
            }
        }
        // Compile child-column positions for this immutable descriptor. The
        // reverse relationship is resolved through DD indexes; it is not a
        // reason to materialize every parent/child table at startup.
        crate::storage::schema::build_fk_runtime(&mut decoded);
        Ok(decoded.tables.into_values().next())
    }

    fn inbound_fk_child_ids(&self, parent: &TableSchema) -> Result<Vec<u64>, QueryError> {
        use crate::storage::data_dictionary::DD_FOREIGN_KEYS;
        let table =
            crate::storage::data_dictionary::dd_table_schema(DD_FOREIGN_KEYS).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "missing mysql.foreign_keys compiled schema",
                )
            })?;
        let index = table
            .indexes
            .iter()
            .find(|index| index.name == "IDX_REFERENCED_TABLE")
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "mysql.foreign_keys reverse index is missing",
                )
            })?;
        let mut probe = vec![Value::Null; table.columns.len()];
        probe[table.column_index("referenced_schema")?] =
            Value::String(normalize_name(&parent.database));
        probe[table.column_index("referenced_table")?] =
            Value::String(normalize_name(&parent.name));
        let key = table.index_key(index, &probe)?;
        let mut child_ids = Vec::new();
        for row_id in self.lookup_index(&table, index, key)? {
            if let Some(row) = self.read_mvcc_row(DD_FOREIGN_KEYS, row_id)? {
                if !row.header.is_deleted() {
                    if let Some(id) = row.values.first().and_then(|value| match value {
                        Value::UInt(id) => Some(*id),
                        Value::Int(id) if *id >= 0 => Some(*id as u64),
                        _ => None,
                    }) {
                        child_ids.push(id);
                    }
                }
            }
        }
        child_ids.sort_unstable();
        child_ids.dedup();
        Ok(child_ids)
    }

    fn dd_selector_key(
        table: &TableSchema,
        primary: &IndexSchema,
        values: &[Value],
    ) -> Result<IndexKey, QueryError> {
        if values.is_empty() || values.len() > primary.columns.len() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "invalid data dictionary primary-key selector",
            ));
        }
        let mut row = vec![Value::Null; table.columns.len()];
        for (name, value) in primary.columns.iter().zip(values) {
            let position = table.column_index(name)?;
            row[position] = value.clone();
        }
        let mut key = table.index_key(primary, &row)?;
        key.0.truncate(values.len());
        Ok(key)
    }

    fn apply_catalog_row_replacement(
        &self,
        txn: TxnView,
        replacement: &CatalogRowReplacement,
    ) -> Result<(usize, Vec<RowId>), QueryError> {
        let table = crate::storage::data_dictionary::dd_table_schema(replacement.dd_table_id)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "unknown data dictionary table id {}",
                        replacement.dd_table_id
                    ),
                )
            })?;
        let old_row_ids = self.catalog_selected_row_ids(&table, &replacement.selector)?;
        let old_count = old_row_ids.len();
        for row_id in old_row_ids {
            let Some(old) = self.read_mvcc_row(table.id, row_id)? else {
                continue;
            };
            if old.header.is_deleted() {
                continue;
            }
            self.delete_row_at(txn, &table, table.id, row_id, &old.values)
                .map_err(|err| {
                    log_message(&format!(
                        "DD replacement delete failed table_id={} row_id={row_id:?}: {err}",
                        table.id
                    ));
                    err
                })?;
        }
        if replacement.rows.is_empty() {
            return Ok((old_count, Vec::new()));
        }
        let headers = vec![RowHeader::visible(txn.txn_id); replacement.rows.len()];
        let inserted = self
            .insert_mvcc_batch_externalized(
                txn.txn_id,
                table.id,
                &table.row_codec,
                &headers,
                &replacement.rows,
            )
            .map_err(|err| {
                log_message(&format!(
                    "DD replacement insert failed table_id={} rows={}: {err}",
                    table.id,
                    replacement.rows.len()
                ));
                err
            })?;
        if let Err(err) = (|| -> Result<(), QueryError> {
            self.push_insert_batch_undo_deferred(txn.txn_id, table.id, &inserted)?;
            self.insert_index_batches_deferred_for_rows(
                txn.txn_id,
                &table,
                &replacement.rows,
                &inserted,
            )
        })() {
            log_message(&format!(
                "DD replacement index/undo failed table_id={} rows={}: {err}",
                table.id,
                replacement.rows.len()
            ));
            self.rollback_inserted_rows(txn.txn_id, table.id, &inserted)?;
            return Err(err);
        }
        Ok((old_count.saturating_add(replacement.rows.len()), inserted))
    }

    fn bootstrap_catalog_store(&self, catalog: &Catalog) -> Result<(), QueryError> {
        use crate::storage::data_dictionary::catalog_to_dd_rows;
        let rows = catalog_to_dd_rows(catalog);
        let mut txn = CatalogTxn::new(self);
        for (dd_table_id, table_rows) in rows.per_table() {
            txn.replace(dd_table_id, CatalogRowSelector::All, table_rows.clone());
        }
        txn.set_delta(Vec::new(), true);
        txn.commit()?;
        // Provision every compiled DD B-tree even when its heap is empty.
        // Lazy creation under the first concurrent descriptor miss can make
        // readers observe an initializing tree; fixed catalog indexes are a
        // bootstrap invariant, not an optional user-index cache.
        for table in crate::storage::data_dictionary::dd_tables() {
            for index in &table.indexes {
                self.with_index_read(table.id, &index.name, |_| Ok(()))?;
            }
        }
        if self.config.durability != DurabilityMode::Memory {
            // Initialize every root before the shared WAL barrier, rather
            // than issuing a strict WAL sync for each newly opened index.
            if self.config.durability == DurabilityMode::Strict {
                let covered = self.log.lock().last_lsn();
                crate::wal::LogManager::sync_durable_to(&self.log, covered)?;
            }
            // Replica recovery skips physical index images. An empty DD heap
            // has no logical IndexBatch to repair a meta-only base file.
            for table in crate::storage::data_dictionary::dd_tables() {
                for index in &table.indexes {
                    self.with_index_read(table.id, &index.name, |file| file.flush())?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn catalog_store_stats(&self) -> (u64, u64, u64, u64, u64, u64, u64) {
        let _ = self;
        let (cache_bytes, cache_entries) = self
            .catalog
            .descriptor_cache
            .lock()
            .map(|cache| {
                let retired_bytes = cache.retired.iter().fold(0usize, |total, (_, charge)| {
                    total.saturating_add(charge.bytes())
                });
                (
                    cache.bytes.saturating_add(retired_bytes) as u64,
                    cache.entries.len().saturating_add(cache.retired.len()) as u64,
                )
            })
            .unwrap_or_default();
        (
            CATALOG_TXN_COMMITS.load(Ordering::Relaxed),
            CATALOG_TXN_ROLLBACKS.load(Ordering::Relaxed),
            CATALOG_MUTATED_ROWS.load(Ordering::Relaxed),
            self.catalog.epoch(),
            self.catalog.cache_limit_bytes() as u64,
            cache_bytes,
            cache_entries,
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CatalogMutation {
    Schema(String),
    Table(u64),
    TableVersion(u64, u64),
    Stats(u64),
    View {
        schema: String,
        name: String,
    },
    Routine {
        schema: String,
        kind: String,
        name: String,
    },
    Trigger {
        schema: String,
        name: String,
    },
    Event {
        schema: String,
        name: String,
    },
    Sequence(u64),
    SequenceValue(u64, Option<crate::storage::sequence::SequenceValue>),
    SequenceRetirement(u64, u64),
    Auth,
}

#[derive(Debug, Default)]
struct CatalogApplyDelta {
    dd_touched: bool,
    invalidate_all: bool,
    object_ids: HashSet<u64>,
    /// Recovery-only XA files share the existing apply transaction lifetime.
    has_binlog_intent: bool,
}

thread_local! {
    static CATALOG_MUTATIONS: RefCell<HashSet<CatalogMutation>> = RefCell::new(HashSet::new());
}
static CATALOG_TXN_COMMITS: AtomicU64 = AtomicU64::new(0);
static CATALOG_TXN_ROLLBACKS: AtomicU64 = AtomicU64::new(0);
static CATALOG_MUTATED_ROWS: AtomicU64 = AtomicU64::new(0);
const CATALOG_DD_COMMIT_SHARDS: usize = 256;
static CATALOG_DD_COMMIT_LOCKS: [Mutex<()>; CATALOG_DD_COMMIT_SHARDS] =
    [const { Mutex::new(()) }; CATALOG_DD_COMMIT_SHARDS];

/// A separately charged, reference-counted lease survives cache eviction and
/// nested DDL helpers. The caller must hold the object's definition MDL.
pub(crate) struct DdlTableLease<'a> {
    db: &'a Database,
    table: Arc<TableSchema>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}
impl Drop for DdlTableLease<'_> {
    fn drop(&mut self) {
        {
            let mut pins = self.db.catalog.ddl_pinned_ids.write();
            if let Some(count) = pins.get_mut(&self.table.id) {
                *count -= 1;
                if *count == 0 {
                    pins.remove(&self.table.id);
                }
            }
        }
        // Return the committed/current descriptor to the bounded cache. A
        // dropped object must never be reinstalled by its former lease.
        let key = self.db.catalog.table_key_by_id(self.table.id);
        let current = key.and_then(|key| {
            self.db
                .catalog
                .read()
                .ok()
                .and_then(|c| {
                    c.tables
                        .get(&key)
                        .filter(|t| t.id == self.table.id)
                        .cloned()
                })
                .map(|table| (key, table))
        });
        if let Some((key, table)) = current {
            self.db.catalog.install_descriptor(key, table);
        }
    }
}
impl Database {
    pub(crate) fn pin_ddl_table(&self, name: &str) -> Result<DdlTableLease<'_>, QueryError> {
        check_table_ddl_writable(&split_table_name(DEFAULT_DATABASE, name).0)?;
        loop {
            let table = self.table(name)?;
            if table.columns.is_empty() {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "incomplete DDL table descriptor",
                ));
            }
            let charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                CatalogManager::descriptor_bytes(&table),
            )?;
            let mut catalog = self.catalog_write()?;
            let key = table.qualified_name();
            let current = catalog
                .tables
                .get(&key)
                .ok_or_else(|| ddl_table_not_found(name))?;
            if current.id != table.id || current.schema_version != table.schema_version {
                continue;
            }
            *self
                .catalog
                .ddl_pinned_ids
                .write()
                .entry(table.id)
                .or_default() += 1;
            catalog.tables.insert(key, table.clone());
            return Ok(DdlTableLease {
                db: self,
                table,
                _charge: charge,
            });
        }
    }
}

impl Database {
    pub(crate) fn replication_property(&self, key: &str) -> Result<Option<String>, QueryError> {
        let table = crate::storage::data_dictionary::dd_table_schema(
            crate::storage::data_dictionary::DD_DD_PROPERTIES,
        )
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "missing DD properties schema",
            )
        })?;
        let primary = Self::dd_primary_index(&table)?;
        let key = Self::dd_selector_key(&table, primary, &[Value::String(key.to_owned())])?;
        let txn = self.begin_txn()?;
        let result = (|| {
            for id in self.lookup_index_visible(&table, primary, key, txn)? {
                if let Some(row) = self.read_row(&table, id, txn)? {
                    return Ok(row
                        .values
                        .get(1)
                        .and_then(crate::types::value_str)
                        .map(str::to_owned));
                }
            }
            Ok(None)
        })();
        self.rollback_txn(txn.txn_id)?;
        result
    }

    pub(crate) fn write_replication_property(
        &self,
        txn: TxnView,
        key: &str,
        value: Option<&str>,
    ) -> Result<(), QueryError> {
        let _binlog = crate::binlog::SessionSqlLogBinGuard::new(false);
        self.apply_catalog_row_replacement(
            txn,
            &CatalogRowReplacement {
                dd_table_id: crate::storage::data_dictionary::DD_DD_PROPERTIES,
                selector: CatalogRowSelector::PrimaryValues(vec![Value::String(key.to_owned())]),
                rows: value
                    .map(|value| {
                        vec![vec![
                            Value::String(key.to_owned()),
                            Value::String(value.to_owned()),
                        ]]
                    })
                    .unwrap_or_default(),
            },
        )
        .map(|_| ())
    }

    pub(crate) fn set_replication_property(
        &self,
        key: &str,
        value: Option<&str>,
    ) -> Result<(), QueryError> {
        let txn = self.begin_txn()?;
        if let Err(err) = self.write_replication_property(txn, key, value) {
            let _ = self.rollback_txn(txn.txn_id);
            return Err(err);
        }
        self.commit_txn(txn.txn_id).map(|_| ())
    }
}
