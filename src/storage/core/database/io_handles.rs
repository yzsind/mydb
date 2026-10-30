thread_local! {
    /// Per-thread defer counter for `Database::commit_catalog_txn`. When >0,
    /// any call to `commit_catalog_txn` records a pending flush and returns
    /// immediately; the enclosing scope commits (or drops) it. Counter
    /// (not bool) so nested guards compose safely. Sole owner is
    /// `CatalogTxnBatchGuard`.
    static CATALOG_TXN_BATCH_DEFER: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    static CATALOG_TXN_BATCH_PENDING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
thread_local! {
    static STORAGE_PATH_RESOLVED_TEST_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
}

#[cfg(test)]
fn after_storage_path_resolution_for_test() {
    let hook = STORAGE_PATH_RESOLVED_TEST_HOOK.with(|cell| cell.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

fn stable_missing_index_id(name: &str) -> u64 {
    // Defensive fallback for cleanup/recovery paths that retained only an
    // index name after removing its descriptor. Build/open paths resolve the
    // persisted index_id before reaching here.
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in name.bytes().map(|byte| byte.to_ascii_lowercase()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash | (1_u64 << 63)
}

fn object_handle_cache_max() -> usize {
    static LIMIT: OnceLock<usize> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        crate::util::env::var("MYDB_OBJECT_HANDLE_CACHE_MAX")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(512)
            .max(32)
    })
}

fn catalog_txn_batch_deferred() -> bool {
    CATALOG_TXN_BATCH_DEFER.with(|c| c.get() > 0)
}

fn note_deferred_catalog_txn_batch() {
    CATALOG_TXN_BATCH_PENDING.with(|c| c.set(true));
}

fn take_deferred_catalog_txn_batch_pending() -> bool {
    CATALOG_TXN_BATCH_PENDING.with(|c| c.replace(false))
}

/// RAII guard that turns nested `commit_catalog_txn` calls on this thread
/// into no-ops. The guard's owner is expected to explicitly call
/// `Database::commit_catalog_txn_now` at the atomic commit point when a
/// flush is required, or simply drop the guard when the enclosing
/// operation aborted and the in-memory catalog has been reverted to a
/// state equivalent to on-disk.
pub(crate) struct CatalogTxnBatchGuard {
    _priv: (),
}

impl CatalogTxnBatchGuard {
    pub(crate) fn new() -> Self {
        CATALOG_TXN_BATCH_DEFER.with(|c| c.set(c.get() + 1));
        // Reset the pending flag when this scope opens so we only see
        // requests originating in it.
        CATALOG_TXN_BATCH_PENDING.with(|c| c.set(false));
        Self { _priv: () }
    }

    /// True if any deferred `commit_catalog_txn` request has been observed
    /// since this guard was constructed. Callers use it to skip the
    /// commit-point flush when the whole batch was a no-op.
    pub(crate) fn any_flush_requested(&self) -> bool {
        CATALOG_TXN_BATCH_PENDING.with(|c| c.get())
    }
}

impl Drop for CatalogTxnBatchGuard {
    fn drop(&mut self) {
        CATALOG_TXN_BATCH_DEFER.with(|c| c.set(c.get().saturating_sub(1)));
        // Never leak a "pending" flag out of the guard's scope: an
        // outer scope's flush accounting is independent.
        if !catalog_txn_batch_deferred() {
            let _ = take_deferred_catalog_txn_batch_pending();
        }
    }
}

impl Database {
    fn storage_handle_open_epoch(&self, table_id: u64) -> Arc<AtomicU64> {
        let mut epochs = self
            .storage_handle_open_epochs
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(epoch) = epochs.get(&table_id).and_then(std::sync::Weak::upgrade) {
            return epoch;
        }
        // Dead weak entries need no persistent per-table allocation. Keep
        // cleanup amortized and bounded independently of catalog history.
        if epochs.len() >= 128 {
            epochs.retain(|_, epoch| epoch.strong_count() != 0);
        }
        let epoch = Arc::new(AtomicU64::new(0));
        epochs.insert(table_id, Arc::downgrade(&epoch));
        epoch
    }

    fn invalidate_storage_handle_open_epoch(&self, table_id: u64) {
        let mut epochs = self
            .storage_handle_open_epochs
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(epoch) = epochs.get(&table_id).and_then(std::sync::Weak::upgrade) {
            epoch.fetch_add(1, Ordering::AcqRel);
        } else {
            epochs.remove(&table_id);
        }
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Lazy accessor for the `mysqld-auto.cnf`-backed PERSIST store. The
    /// file lives under `<data_dir>/system/`; we create the directory
    /// (and the file on first write) on demand.
    pub fn persisted_variables(
        &self,
    ) -> &std::sync::Arc<crate::engine::sysvar::PersistedVariables> {
        self.persisted_variables.get_or_init(|| {
            let sys = crate::storage::naming::system_dir(&self.data_dir);
            crate::engine::sysvar::PersistedVariables::open_or_default(&sys)
        })
    }

    /// Commit the thread's bounded catalog change set to the transactional
    /// v46 data dictionary. No unrelated object is encoded or written.
    ///
    /// When the caller has activated a `CatalogTxnBatchGuard`, this
    /// records that a flush was requested and returns without touching
    /// disk. The guard's owner is expected to invoke
    /// `commit_catalog_txn_now()` at the atomic commit point (or drop the
    /// guard after a rollback that reverted the in-memory catalog to a
    /// state equivalent to what disk still holds — no flush needed).
    /// This is what makes a multi-action metadata-only ALTER TABLE
    /// atomic across crashes: intermediate mutations only exist in
    /// memory, and either the whole batch flushes once at the end or
    /// nothing at all.
    fn commit_catalog_txn(&self) -> Result<(), QueryError> {
        if catalog_txn_batch_deferred() {
            note_deferred_catalog_txn_batch();
            return Ok(());
        }
        self.commit_catalog_txn_now()
    }

    /// Commit the final user-visible DDL change set and its logical binlog
    /// event together. Preparation, statistics and ID reservations must use
    /// ordinary internal catalog transactions instead.
    pub(crate) fn commit_ddl_catalog_txn(&self) -> Result<(), QueryError> {
        if catalog_txn_batch_deferred() {
            note_deferred_catalog_txn_batch();
            return Ok(());
        }
        let result = self.commit_ddl_catalog_txn_recoverable();
        if let Err(err) = &result {
            Self::take_catalog_mutations();
            self.quarantine_mvcc(format!("DDL catalog commit failed: {}", err.message()));
        }
        result
    }

    pub(crate) fn commit_ddl_catalog_txn_recoverable(&self) -> Result<(), QueryError> {
        #[cfg(test)]
        if DDL_CATALOG_COMMIT_FAILURE.with(|fail| fail.replace(false)) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "injected catalog commit failure",
            ));
        }
        match self.build_catalog_txn_incremental()? {
            Some(txn) => txn.commit_ddl(),
            None => Ok(()),
        }
    }

    /// Flush immediately; callers without a rollback snapshot must stop serving
    /// requests if durable publication fails after changing the live catalog.
    pub(crate) fn commit_catalog_txn_now(&self) -> Result<(), QueryError> {
        let result = self.commit_catalog_txn_recoverable();
        if let Err(err) = &result {
            Self::take_catalog_mutations();
            self.quarantine_mvcc(format!("catalog commit failed: {}", err.message()));
        }
        result
    }

    /// Only for a caller that restores its catalog snapshot on every failure.
    /// Ambiguous storage commit failures still quarantine at the WAL/MVCC layer.
    pub(crate) fn commit_catalog_txn_recoverable(&self) -> Result<(), QueryError> {
        #[cfg(test)]
        if DDL_CATALOG_COMMIT_FAILURE.with(|fail| fail.replace(false)) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "injected catalog commit failure",
            ));
        }
        self.commit_catalog_txn_incremental()
    }

    /// Resolve the authoritative catalog at startup from the
    /// data-dictionary heap tables, or bootstrap a fresh data directory.
    ///
    /// A v46 dictionary contains one current MVCC row per object. A missing
    /// `catalog_version` property means the directory is fresh and is
    /// bootstrapped with one ordinary catalog transaction.
    pub(crate) fn load_or_bootstrap_catalog(&self) -> Result<(), QueryError> {
        let initialized = self
            .scan_dd_table(crate::storage::data_dictionary::DD_DD_PROPERTIES)?
            .iter()
            .any(|row| row.first().and_then(crate::types::value_str) == Some("catalog_version"));
        let migrated = if initialized {
            let headers = self.open_catalog_headers_from_store()?;
            *self.pending_primary_recovery.lock() = headers
                .tables
                .values()
                .filter(|t| t.pending_primary_key.is_some())
                .map(|t| t.id)
                .collect();
            crate::storage::naming::migrate_catalog(headers)
        } else {
            if crate::storage::control::Control::load(&self.data_dir)?
                .is_some_and(|control| control.initialized)
            {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "initialized data dictionary is missing; refusing bootstrap; {}",
                        self.catalog_property_scan_diagnostics()
                            .unwrap_or_else(|err| {
                                format!("property diagnostics failed: {}", err.message())
                            })
                    ),
                ));
            }
            // A fresh directory has no dictionary rows. Losing the version
            // property in an existing dictionary is corruption, not bootstrap.
            for table in crate::storage::data_dictionary::dd_tables() {
                if !self.scan_dd_table(table.id)?.is_empty() {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!(
                            "catalog_version is missing from a nonempty data dictionary; refusing bootstrap; {}",
                            self.catalog_property_scan_diagnostics().unwrap_or_else(|err| {
                                format!("property diagnostics failed: {}", err.message())
                            })
                        ),
                    ));
                }
            }
            self.ensure_writable()?;
            self.persist_vector_purge_ledger()?;
            crate::engine::auth::validate_password_length(
                &crate::engine::auth::default_auth_plugin(),
                &crate::util::env::var("MYDB_ROOT_PASSWORD").unwrap_or_else(|_| "root".into()),
            )?;
            let bootstrap =
                crate::storage::naming::migrate_catalog(super::catalog::empty_catalog());
            self.bootstrap_catalog_store(&bootstrap)?;
            self.catalog.replace_live(bootstrap)?;
            if self.config.durability != DurabilityMode::Memory {
                crate::storage::control::Control::finish_initialization(&self.data_dir)?;
            }
            return Ok(());
        };
        self.catalog.replace_live(migrated)?;
        if self.config.durability != DurabilityMode::Memory {
            crate::storage::control::Control::finish_initialization(&self.data_dir)?;
        }
        Ok(())
    }

    /// Open the startup-resident object headers and bounded non-table
    /// families. Column/index/constraint rows are deliberately omitted and
    /// assembled in one MVCC snapshot by `load_table_descriptor_from_dd` on
    /// first relation access.
    pub(crate) fn open_catalog_headers_from_store(&self) -> Result<Catalog, QueryError> {
        use crate::storage::data_dictionary::{
            catalog_from_dd_rows, DdRowSet, DD_DD_PROPERTIES, DD_EVENTS, DD_GLOBAL_PRIVILEGES,
            DD_RESTRICTIONS, DD_ROLE_EDGES, DD_ROUTINES, DD_SCHEMATA, DD_SEQUENCES, DD_TABLES,
            DD_TRIGGERS, DD_USERS, DD_VIEWS,
        };
        let load = |dd_id: u64| self.scan_dd_table(dd_id);
        let set = DdRowSet {
            schemata: load(DD_SCHEMATA)?,
            views: load(DD_VIEWS)?,
            routines: load(DD_ROUTINES)?,
            triggers: load(DD_TRIGGERS)?,
            events: load(DD_EVENTS)?,
            users: load(DD_USERS)?,
            grants: load(DD_GLOBAL_PRIVILEGES)?,
            role_edges: load(DD_ROLE_EDGES)?,
            dd_properties: load(DD_DD_PROPERTIES)?,
            sequences: load(DD_SEQUENCES)?,
            restrictions: load(DD_RESTRICTIONS)?,
            ..DdRowSet::default()
        };
        let mut catalog = catalog_from_dd_rows(&set)?;
        const HEADER_COLUMNS: &[usize] = &[
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 15, 16, 17, 18, 19, 20, 21, 22, 23,
        ];
        self.scan_dd_table_projected_batched(DD_TABLES, HEADER_COLUMNS, 24, 512, |rows| {
            for row in rows {
                let table = crate::storage::data_dictionary::table_header_from_dd_row(&row)?;
                catalog.tables.insert(
                    crate::storage::naming::table_key(&table.database, &table.name),
                    table,
                );
            }
            Ok(())
        })?;
        Ok(catalog)
    }

    fn scan_dd_table_projected_batched(
        &self,
        dd_id: u64,
        columns: &[usize],
        full_width: usize,
        batch_rows: usize,
        mut consume: impl FnMut(Vec<Vec<Value>>) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let batch_rows = batch_rows.clamp(1, 512);
        let mut batch = Vec::with_capacity(batch_rows);
        let txns = self.txns_read()?;
        self.with_heap_read(dd_id, |heap| {
            heap.scan_mvcc_columns_direct_until(columns, |row| {
                if row.header.is_deleted()
                    || matches!(
                        txns.status(row.header.last_txn_id),
                        Some(TxnStatus::Active | TxnStatus::Prepared | TxnStatus::RolledBack)
                    )
                {
                    return Ok(false);
                }
                let mut values = vec![Value::Null; full_width];
                for (position, value) in columns.iter().copied().zip(row.values.iter().cloned()) {
                    values[position] = value;
                }
                batch.push(values);
                if batch.len() == batch_rows {
                    consume(std::mem::take(&mut batch))?;
                    batch.reserve(batch_rows);
                }
                Ok(false)
            })
        })?;
        drop(txns);
        if !batch.is_empty() {
            consume(batch)?;
        }
        Ok(())
    }

    /// Failure-only evidence: do not hide missing catalog authority by
    /// bootstrapping over it. Record visibility and physical extent facts,
    /// without exposing property values or changing the successful open path.
    fn catalog_property_scan_diagnostics(&self) -> Result<String, QueryError> {
        let txns = self.txns_read()?;
        self.with_heap_read(crate::storage::data_dictionary::DD_DD_PROPERTIES, |heap| {
            let mut total = 0usize;
            let mut deleted = 0usize;
            let mut invisible = 0usize;
            let mut version_rows = 0usize;
            let mut version_txn = None;
            let mut version_status = None;
            heap.scan_mvcc_until(|row| {
                total += 1;
                if row.header.is_deleted() {
                    deleted += 1;
                } else if matches!(txns.status(row.header.last_txn_id),
                    Some(TxnStatus::Active | TxnStatus::Prepared | TxnStatus::RolledBack)) {
                    invisible += 1;
                }
                if row.values.first().and_then(crate::types::value_str) == Some("catalog_version") {
                    version_rows += 1;
                    version_txn = Some(row.header.last_txn_id);
                    version_status = txns.status(row.header.last_txn_id);
                }
                Ok(false)
            })?;
            Ok(format!(
                "property_rows={total}, deleted={deleted}, invisible={invisible}, version_rows={version_rows}, version_txn={version_txn:?}, version_status={version_status:?}, heap_pages={}, heap_bytes={}, next_txn_id={}",
                heap.page_count()?, heap.file.metadata().map_err(io_err)?.len(), txns.next_txn_id,
            ))
        })
    }

    /// Test-only consistency oracle. Production startup and replication must
    /// never construct this process-wide descriptor snapshot.
    #[cfg(test)]
    pub(crate) fn diagnostic_catalog_snapshot_from_store(&self) -> Result<Catalog, QueryError> {
        use crate::storage::data_dictionary::{
            catalog_from_dd_rows, DdRowSet, DD_CHECK_CONSTRAINTS, DD_COLUMNS, DD_DD_PROPERTIES,
            DD_EVENTS, DD_FOREIGN_KEYS, DD_FOREIGN_KEY_COLUMN_USAGE, DD_GLOBAL_PRIVILEGES,
            DD_INDEXES, DD_INDEX_COLUMN_USAGE, DD_RESTRICTIONS, DD_ROLE_EDGES, DD_ROUTINES,
            DD_SCHEMATA, DD_SEQUENCES, DD_TABLES, DD_TABLE_STATS, DD_TRIGGERS, DD_USERS, DD_VIEWS,
        };
        let load = |dd_id: u64| self.scan_dd_table(dd_id);
        catalog_from_dd_rows(&DdRowSet {
            schemata: load(DD_SCHEMATA)?,
            tables: load(DD_TABLES)?,
            columns: load(DD_COLUMNS)?,
            indexes: load(DD_INDEXES)?,
            index_column_usage: load(DD_INDEX_COLUMN_USAGE)?,
            check_constraints: load(DD_CHECK_CONSTRAINTS)?,
            foreign_keys: load(DD_FOREIGN_KEYS)?,
            foreign_key_column_usage: load(DD_FOREIGN_KEY_COLUMN_USAGE)?,
            views: load(DD_VIEWS)?,
            routines: load(DD_ROUTINES)?,
            triggers: load(DD_TRIGGERS)?,
            events: load(DD_EVENTS)?,
            users: load(DD_USERS)?,
            grants: load(DD_GLOBAL_PRIVILEGES)?,
            role_edges: load(DD_ROLE_EDGES)?,
            dd_properties: load(DD_DD_PROPERTIES)?,
            sequences: load(DD_SEQUENCES)?,
            restrictions: load(DD_RESTRICTIONS)?,
            table_stats: load(DD_TABLE_STATS)?,
            innodb_dynamic_metadata: Vec::new(),
        })
    }

    /// Reload non-table catalog families after a replicated schema/auth/event
    /// delta. Table descriptors are handled by object-id deltas and are never
    /// scanned here.
    fn load_non_table_families_from_store(&self) -> Result<Catalog, QueryError> {
        use crate::storage::data_dictionary::{
            catalog_from_dd_rows, DdRowSet, DD_DD_PROPERTIES, DD_EVENTS, DD_GLOBAL_PRIVILEGES,
            DD_RESTRICTIONS, DD_ROLE_EDGES, DD_ROUTINES, DD_SCHEMATA, DD_SEQUENCES, DD_TRIGGERS,
            DD_USERS, DD_VIEWS,
        };
        let set = DdRowSet {
            schemata: self.scan_dd_table(DD_SCHEMATA)?,
            views: self.scan_dd_table(DD_VIEWS)?,
            routines: self.scan_dd_table(DD_ROUTINES)?,
            triggers: self.scan_dd_table(DD_TRIGGERS)?,
            events: self.scan_dd_table(DD_EVENTS)?,
            users: self.scan_dd_table(DD_USERS)?,
            grants: self.scan_dd_table(DD_GLOBAL_PRIVILEGES)?,
            role_edges: self.scan_dd_table(DD_ROLE_EDGES)?,
            restrictions: self.scan_dd_table(DD_RESTRICTIONS)?,
            dd_properties: self.scan_dd_table(DD_DD_PROPERTIES)?,
            sequences: self.scan_dd_table(DD_SEQUENCES)?,
            ..DdRowSet::default()
        };
        catalog_from_dd_rows(&set)
    }

    /// Collect current committed rows of one dictionary heap. A missing heap
    /// opens empty during bootstrap. Active/rolled-back creators and MVCC
    /// tombstones are never catalog authority.
    fn scan_dd_table(&self, dd_id: u64) -> Result<Vec<Vec<Value>>, QueryError> {
        let mut out = Vec::new();
        let txns = self.txns_read()?;
        self.with_heap_read(dd_id, |heap| {
            heap.scan_mvcc_until(|row| {
                if row.header.is_deleted()
                    || matches!(
                        txns.status(row.header.last_txn_id),
                        Some(TxnStatus::Active | TxnStatus::Prepared | TxnStatus::RolledBack)
                    )
                {
                    return Ok(false);
                }
                out.push(row.values);
                Ok(false) // false = keep scanning
            })
        })?;
        Ok(out)
    }

    fn append_wal(&self, txn_id: u64, payload: WalPayload) -> Result<u64, QueryError> {
        self.ensure_writable()?;
        if self.config.durability == DurabilityMode::Memory {
            return Ok(0);
        }
        // Pre-encode outside the LogManager mutex so the contended
        // critical section drops to: reserve_lsn + header-byte patch
        // + pwrite + bookkeeping. CommitTxn / BeginTxn / FileCreate /
        // FileDelete all funnel through this helper, so this single
        // edit factors WAL encode work out of the lock for every
        // transaction boundary too — not just the row-level writes.
        let timed = matches!(payload, WalPayload::SequenceState { .. });
        if timed {
            let pre = crate::wal::LogManager::pre_encode_timed_record(txn_id, payload)?;
            return crate::wal::LogManager::append_timed_to(
                &self.log,
                pre,
                &self.commit_hlc_unix_us,
                Some(&self.temporal_hlc_rollback_total),
            )
            .map(|(lsn, _)| lsn);
        }
        let pre = crate::wal::LogManager::pre_encode_record(txn_id, payload)?;
        crate::wal::LogManager::append_pre_encoded_to(&self.log, pre)
    }

    fn append_commit_wal(
        &self,
        txn_id: u64,
        payload: WalPayload,
    ) -> Result<(u64, u64), QueryError> {
        self.ensure_writable()?;
        if self.config.durability == DurabilityMode::Memory {
            return Ok((0, self.next_commit_unix_us()));
        }
        let pre = crate::wal::LogManager::pre_encode_timed_record(txn_id, payload)?;
        crate::wal::LogManager::append_timed_to(
            &self.log,
            pre,
            &self.commit_hlc_unix_us,
            Some(&self.temporal_hlc_rollback_total),
        )
    }

    fn create_logged_file(
        &self,
        txn_id: u64,
        path: PathBuf,
        bytes: Vec<u8>,
    ) -> Result<(), QueryError> {
        let bytes = create_empty_heap_image(&path, bytes);
        self.append_wal(
            txn_id,
            WalPayload::FileCreate {
                path: self.relative_path(&path),
                after: bytes.clone(),
            },
        )?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(io_err)?;
        }
        fs::write(path, bytes).map_err(io_err)
    }

    fn delete_logged_file(&self, txn_id: u64, path: PathBuf) -> Result<(), QueryError> {
        let before =
            if txn_id == txn_id_for_ddl() || self.config.durability == DurabilityMode::Memory {
                Vec::new()
            } else {
                fs::read(&path).unwrap_or_default()
            };
        self.append_wal(
            txn_id,
            WalPayload::FileDelete {
                path: self.relative_path(&path),
                before,
            },
        )?;
        match fs::remove_file(&path) {
            Ok(()) => retire_page_owner_proof(&path),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                retire_page_owner_proof(&path)
            }
            Err(_) => {}
        }
        Ok(())
    }

    fn relative_path(&self, path: &Path) -> String {
        path.strip_prefix(&self.data_dir)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/")
    }

    /// Refresh the cached `HeapFile`'s `RowCodec` from the current
    /// catalog without dropping the cache entry. ALTER paths use this
    /// instead of `remove_heap_cache_entry` so dirty in-buffer-pool
    /// pages survive the schema change (the cache entry's discard
    /// path evicts pages with `discard_file`, which would lose
    /// uncommitted/unflushed row writes).
    fn refresh_heap_codec(&self, table_id: u64) -> Result<(), QueryError> {
        let Some(table) = self.table_by_id(table_id) else {
            return Ok(());
        };
        let reserve = if table.pctfree > 0 {
            (PAGE_SIZE * usize::from(table.pctfree.min(50)) / 100)
                .min(MAX_HEAP_UPDATE_RESERVE_BYTES)
        } else {
            self.config.heap_update_reserve_bytes
        };
        let mut ids = if table.id == table_id {
            table.storage_ids()
        } else {
            Vec::new()
        };
        ids.push(table_id);
        ids.sort_unstable();
        ids.dedup();
        let handles = {
            let cache = self.heap_cache.write();
            for id in &ids {
                self.invalidate_storage_handle_open_epoch(*id);
            }
            ids.iter()
                .filter_map(|id| cache.get(id).cloned())
                .collect::<Vec<_>>()
        };
        for heap in handles {
            let mut heap = heap.write();
            heap.codec = table.row_codec.clone();
            heap.update_reserve_bytes = reserve;
        }
        Ok(())
    }

    fn remove_heap_cache_entry(&self, table_id: u64) -> Result<(), QueryError> {
        let heap = {
            let mut cache = self.heap_cache.write();
            self.invalidate_storage_handle_open_epoch(table_id);
            cache.remove(&table_id)
        };
        if let Some(heap) = heap {
            heap.write().discard_without_flush();
        }
        Ok(())
    }

    /// Close the transition window for cold background opens. Call while the
    /// physical publication barrier still excludes authoritative writers.
    fn invalidate_generation_handles(&self, table: &TableSchema) -> Result<(), QueryError> {
        self.remove_heap_cache_entry(table.id)?;
        self.remove_overflow_cache_entry(table.id)?;
        for index in &table.indexes {
            self.remove_index_cache_entry(table.id, &index.name)?;
        }
        Ok(())
    }

    /// Rebind display names without flushing or discarding stable physical
    /// objects. The caller holds the table's DDL barrier. Remove all old keys
    /// before inserting new keys so rollback also handles chained renames.
    fn rebind_index_runtime_names(&self, before: &TableSchema, after: &TableSchema) {
        debug_assert_eq!(before.id, after.id);
        debug_assert_eq!(before.active_generation, after.active_generation);
        let renames = before
            .indexes
            .iter()
            .filter_map(|old| {
                after
                    .indexes
                    .iter()
                    .find(|new| new.index_id == old.index_id)
                    .filter(|new| normalize_name(&new.name) != normalize_name(&old.name))
                    .map(|new| (normalize_name(&old.name), normalize_name(&new.name)))
            })
            .collect::<Vec<_>>();
        if renames.is_empty() {
            return;
        }
        let keys = before
            .storage_ids()
            .into_iter()
            .flat_map(|id| {
                renames
                    .iter()
                    .map(move |(old, new)| ((id, old.clone()), (id, new.clone())))
            })
            .collect::<Vec<_>>();
        macro_rules! rebind {
            ($cache:expr, $keys:expr) => {{
                let mut cache = $cache.write();
                let entries = $keys
                    .iter()
                    .filter_map(|(old, new)| cache.remove(old).map(|value| (new.clone(), value)))
                    .collect::<Vec<_>>();
                cache.extend(entries);
            }};
        }
        rebind!(self.index_cache, keys);
        let graph_keys = keys
            .iter()
            .map(|((sid, old), (_, new))| {
                (
                    (
                        before.id,
                        *sid,
                        before.active_generation.max(1),
                        old.clone(),
                    ),
                    (after.id, *sid, after.active_generation.max(1), new.clone()),
                )
            })
            .collect::<Vec<_>>();
        rebind!(self.vector_indexes, graph_keys);
    }

    fn remove_index_cache_entry(&self, table_id: u64, index_name: &str) -> Result<(), QueryError> {
        let key = (table_id, normalize_name(index_name));
        let index = {
            let mut cache = self.index_cache.write();
            self.invalidate_storage_handle_open_epoch(table_id);
            cache.remove(&key)
        };
        if let Some(index) = index {
            index.write().discard_without_flush();
        }
        Ok(())
    }

    fn remove_overflow_cache_entry(&self, table_id: u64) -> Result<(), QueryError> {
        let overflow = {
            let mut cache = self.overflow_cache.write();
            self.invalidate_storage_handle_open_epoch(table_id);
            cache.remove(&table_id)
        };
        if let Some(overflow) = overflow {
            overflow.write().discard_without_flush();
        }
        Ok(())
    }

    /// Drop a table's overflow (LOB) store: evict its buffer-pool frames
    /// and delete the backing `.lob`. Safe when the file was never
    /// created (LOB-less tables): `delete_logged_file` tolerates a
    /// missing path. Used by DROP / TRUNCATE.
    fn cleanup_overflow_file(&self, txn_id: u64, table_id: u64) -> Result<(), QueryError> {
        self.remove_overflow_cache_entry(table_id)?;
        let _ = self.delete_logged_file(txn_id, self.overflow_path(table_id));
        Ok(())
    }

    /// Expose the cached heap handle to callers that want to do many
    /// reads against the same table in a tight loop (e.g. INLJ inner
    /// loop). The caller is responsible for acquiring the appropriate
    /// `read()` / `write()` lock per operation; this just skips the
    /// per-call `heap_cache` HashMap + RwLock cycle.
    ///
    /// Holding the returned `Arc` does NOT prevent DDL — `remove_heap_cache_entry`
    /// only drops the cache binding, not existing handles. Concurrent
    /// DDL on the table swaps the cached entry; any pre-existing
    /// caller-held handle becomes a stale Arc that still serves valid
    /// reads against the old file. For INLJ (which runs under a single
    /// READ COMMITTED statement snapshot) this is the desired semantic.
    pub(crate) fn cached_heap_handle(&self, table_id: u64) -> Result<HeapHandle, QueryError> {
        self.heap_handle(table_id)
    }

    /// See `cached_heap_handle`. Same rationale, applied to indexes.
    pub(crate) fn cached_index_handle(
        &self,
        table_id: u64,
        index_name: &str,
    ) -> Result<IndexHandle, QueryError> {
        self.index_handle(table_id, index_name)
    }

    fn heap_handle(&self, table_id: u64) -> Result<HeapHandle, QueryError> {
        loop {
            // Hot SELECT path: check the cache under a shared read lock first so
            // concurrent readers don't serialize on a Mutex.
            if let Some(heap) = self.heap_cache.read().get(&table_id) {
                return Ok(heap.clone());
            }
            // Double-check, then construct outside the global cache lock. Codec
            // resolution and file open can miss in DD/buffer caches; serializing
            // those operations made many-table opens scale as workers × latency.
            let (open_epoch, epoch) = {
                let cache = self.heap_cache.write();
                if let Some(heap) = cache.get(&table_id) {
                    return Ok(heap.clone());
                }
                let open_epoch = self.storage_handle_open_epoch(table_id);
                let epoch = open_epoch.load(Ordering::Acquire);
                (open_epoch, epoch)
            };
            // Resolve the row codec from the table's catalog entry so the
            // newly-opened HeapFile encodes/decodes rows in the layout the
            // current schema dictates. DDL paths that change column types
            // (ADD/DROP/MODIFY column) call `remove_heap_cache_entry` so
            // this miss path runs again and picks up the refreshed codec.
            // Data-dictionary tables are not in `catalog.tables`; their
            // codec comes from the compiled-in schema so opening a DD heap
            // never depends on an already-loaded catalog (bootstrap cycle).
            // Per-table PCTFREE override (Phase 4): if `CREATE TABLE … PCTFREE=N`
            // or `ALTER TABLE … PCTFREE=N` set a non-zero percentage, derive
            // the reserve from that. Otherwise fall back to the process-wide
            // `MYDB_PCTFREE`/`MYDB_HEAP_UPDATE_RESERVE_BYTES` resolved at
            // startup. Resolved once per heap open, baked into the HeapFile.
            let private_codec = {
                let private = self.reorg_storage_locs.lock().map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "private storage registry poisoned",
                    )
                })?;
                private
                    .get(&table_id)
                    .and_then(|location| location.heap_codec.clone())
            };
            let (codec, update_reserve_bytes) = if let Some(codec) = private_codec {
                (codec, self.config.heap_update_reserve_bytes)
            } else if let Some(dd) = crate::storage::data_dictionary::dd_table_schema(table_id) {
                (dd.row_codec.clone(), self.config.heap_update_reserve_bytes)
            } else if let Some(table) = self.table_by_id(table_id) {
                let bytes = if table.pctfree > 0 {
                    let pct = table.pctfree.min(50) as u64;
                    let reserve =
                        (crate::storage::core::PAGE_SIZE as u64).saturating_mul(pct) / 100;
                    (reserve as usize).min(crate::storage::core::MAX_HEAP_UPDATE_RESERVE_BYTES)
                } else {
                    self.config.heap_update_reserve_bytes
                };
                (table.row_codec.clone(), bytes)
            } else {
                (
                    Arc::new(RowCodec::empty()),
                    self.config.heap_update_reserve_bytes,
                )
            };
            let mut heap_file = HeapFile::open_with_buffer_pool(
                self.table_path(table_id),
                table_id,
                self.log.clone(),
                if is_temp_table_id(table_id) {
                    DurabilityMode::Memory
                } else {
                    self.config.durability
                },
                update_reserve_bytes,
                self.buffer_pool.clone(),
                codec,
            )?;
            // Setup can still fail before cache installation. A speculative
            // loser never owns shared frames, including on its error path.
            heap_file.retain_buffered_pages_on_drop();
            // Attach an off-page LOB reader for tables that can externalize
            // (any string/binary column). Lazy: the overflow handle does not
            // create a `.lob` file for tables that never store a large value.
            if codec_has_externalizable_columns(&heap_file.codec) {
                let overflow = self.overflow_handle(table_id)?;
                heap_file.lob_reader = Some(Arc::new(OverflowReader::new(overflow)));
            }
            let heap: HeapHandle = Arc::new(parking_lot::RwLock::new(heap_file));
            if let Some(heap) = self.install_heap_handle(table_id, heap, &open_epoch, epoch) {
                return Ok(heap);
            }
            crate::engine::cancel::check_cancelled()?;
        }
    }

    fn install_heap_handle(
        &self,
        table_id: u64,
        heap: HeapHandle,
        open_epoch: &AtomicU64,
        epoch: u64,
    ) -> Option<HeapHandle> {
        let mut cache = self.heap_cache.write();
        if open_epoch.load(Ordering::Acquire) != epoch {
            // Invalidation may have retired this path while open ran outside
            // the cache lock. Preserve shared frames, then resolve again.
            heap.write().retain_buffered_pages_on_drop();
            return None;
        }
        if let Some(existing) = cache.get(&table_id) {
            // Construction deliberately happens outside the cache lock. If
            // another opener won the race, both HeapFiles have the same
            // buffer-pool file key. A normal HeapFile drop discards that
            // file's frames for crash-simulation safety; doing so here would
            // also discard dirty pages written through the winning handle.
            heap.write().retain_buffered_pages_on_drop();
            return Some(existing.clone());
        }
        if cache.len() >= object_handle_cache_max() {
            let victim_id = cache
                .iter()
                .find_map(|(id, handle)| (Arc::strong_count(handle) == 1).then_some(*id));
            if let Some(victim_id) = victim_id {
                if let Some(victim) = cache.remove(&victim_id) {
                    victim.write().retain_buffered_pages_on_drop();
                }
            }
        }
        heap.write().discard_pages_on_drop = true;
        cache.insert(table_id, heap.clone());
        Some(heap)
    }

    /// DML helper used by INSERT/UPDATE/DELETE/UNDO paths. After the
    /// 2026-06-14 HeapFile-concurrency refactor, the underlying file-
    /// wide RwLock is taken in **shared** mode here — DML mutates the
    /// per-page state under per-page latches (`HeapFile::page_latch`)
    /// and the file-extension critical section is serialised by
    /// `HeapFile::extend_mutex`. Callers therefore see `&HeapFile`,
    /// not `&mut HeapFile`. The name is preserved so the audit trail
    /// at call sites (52+ historical references) stays grep-able.
    fn with_heap_mut<T>(
        &self,
        table_id: u64,
        f: impl FnOnce(&HeapFile) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        self.ensure_writable()?;
        let heap = self.heap_handle(table_id)?;
        let heap = heap.read();
        let reader = heap.lob_reader.clone();
        with_lob_materializer(reader, || f(&heap))
    }

    /// Rare DDL-only helper that needs a `&mut HeapFile`
    /// (`flush_dirty_pages`, `discard_without_flush`). Takes the
    /// file-wide write lock so the DML readers above don't observe
    /// half-discarded state.
    fn with_heap_exclusive<T>(
        &self,
        table_id: u64,
        f: impl FnOnce(&mut HeapFile) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        self.ensure_writable()?;
        let heap = self.heap_handle(table_id)?;
        let mut heap = heap.write();
        let reader = heap.lob_reader.clone();
        with_lob_materializer(reader, || f(&mut heap))
    }

    /// Test-only: probe a heap slot's raw flags (DELETED / FREE / 0)
    /// for the rowid-6bytes M2 slot-reclaim state-machine tests.
    /// Returns `None` if the slot is out-of-range. Uses `with_heap_read`
    /// — no MVCC visibility, no buffer-pool invalidation.
    #[cfg(test)]
    pub(crate) fn slot_flags_for_test(
        &self,
        table_id: u64,
        row_id: RowId,
    ) -> Result<Option<u8>, QueryError> {
        self.with_heap_read(table_id, |heap| heap.slot_flags_for_test(row_id))
    }

    /// Read-only counterpart to `with_heap_mut`. Lets MVCC scans
    /// (`scan_mvcc` / `scan_mvcc_columns_until` and friends, all
    /// `&self`) run under a shared lock so concurrent readers — and,
    /// crucially, ADD INDEX backfill — do not block writers via
    /// HeapFile's RwLock. Mutating callers (write_mvcc, page
    /// allocation) must keep using `with_heap_mut`.
    fn with_heap_read<T>(
        &self,
        table_id: u64,
        f: impl FnOnce(&HeapFile) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let heap = self.heap_handle(table_id)?;
        let heap = heap.read();
        let reader = heap.lob_reader.clone();
        with_lob_materializer(reader, || f(&heap))
    }

    fn overflow_handle(&self, table_id: u64) -> Result<OverflowHandle, QueryError> {
        loop {
            if let Some(overflow) = self.overflow_cache.read().get(&table_id) {
                return Ok(overflow.clone());
            }
            let open_epoch = self.storage_handle_open_epoch(table_id);
            let epoch = open_epoch.load(Ordering::Acquire);
            let path = self.overflow_path(table_id);
            #[cfg(test)]
            after_storage_path_resolution_for_test();
            if open_epoch.load(Ordering::Acquire) != epoch {
                crate::engine::cancel::check_cancelled()?;
                continue;
            }
            // Opening a LOB store only reads metadata and may traverse a long
            // free list. Keep that I/O outside the global cache lock.
            let opened = OverflowFile::open_with_buffer_pool(
                path,
                table_id,
                self.log.clone(),
                if is_temp_table_id(table_id) {
                    DurabilityMode::Memory
                } else {
                    self.config.durability
                },
                self.buffer_pool.clone(),
            );
            let mut cache = self.overflow_cache.write();
            if let Some(overflow) = cache.get(&table_id) {
                return Ok(overflow.clone());
            }
            if open_epoch.load(Ordering::Acquire) != epoch {
                drop(cache);
                crate::engine::cancel::check_cancelled()?;
                continue;
            }
            let overflow = Arc::new(parking_lot::RwLock::new(opened?));
            if cache.len() >= object_handle_cache_max() {
                let victim_id = cache
                    .iter()
                    .find_map(|(id, handle)| (Arc::strong_count(handle) == 1).then_some(*id));
                if let Some(victim_id) = victim_id {
                    cache.remove(&victim_id);
                }
            }
            cache.insert(table_id, overflow.clone());
            return Ok(overflow);
        }
    }

    fn with_overflow_mut<T>(
        &self,
        table_id: u64,
        f: impl FnOnce(&mut OverflowFile) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let overflow = self.overflow_handle(table_id)?;
        let mut overflow = overflow.write();
        f(&mut overflow)
    }

    fn with_overflow_read<T>(
        &self,
        table_id: u64,
        f: impl FnOnce(&OverflowFile) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let overflow = self.overflow_handle(table_id)?;
        let overflow = overflow.read();
        f(&overflow)
    }

    /// Write access to an `IndexFile`. The outer
    /// `parking_lot::RwLock<IndexFile>` is held in **exclusive** mode for
    /// every DML write — the IndexFile-internal interior mutability
    /// (`meta_state: parking_lot::Mutex`, atomic `dirty` / `dirty_meta` /
    /// `root_page`, thread-local logical-WAL sentinel) and the closure
    /// signature `&IndexFile` are kept ready for a future move to a
    /// shared lock, but for now correctness of concurrent B-tree
    /// structural mutations (split / merge / alloc paths) is preserved
    /// by serializing here. The B-tree concurrent algorithm work needed
    /// to safely move this to a shared lock is tracked separately.
    fn with_index_mut<T>(
        &self,
        table_id: u64,
        index_name: &str,
        f: impl FnOnce(&IndexFile) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        self.ensure_writable()?;
        let index = self.index_handle(table_id, index_name)?;
        let index = index.write();
        index.ensure_healthy()?;
        let result = f(&index);
        result
    }

    /// DML access to an `IndexFile`. The outer handle is held in shared
    /// mode; `IndexFile` serializes structural changes internally and lets
    /// leaf-only updates proceed through page latches.
    fn with_index_dml<T>(
        &self,
        table_id: u64,
        index_name: &str,
        f: impl FnOnce(&IndexFile) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        self.ensure_writable()?;
        let index = self.index_handle(table_id, index_name)?;
        let index = index.read();
        index.ensure_healthy()?;
        let result = f(&index);
        result
    }

    /// Phase 2: read-only access to an `IndexFile`. Multiple readers run
    /// concurrently. Callers must restrict themselves to `&IndexFile` methods
    /// (lookup/range/prefix); write paths still go through `with_index_mut`.
    fn with_index_read<T>(
        &self,
        table_id: u64,
        index_name: &str,
        f: impl FnOnce(&IndexFile) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let index = self.index_handle(table_id, index_name)?;
        let index = index.read();
        index.ensure_healthy()?;
        match f(&index) {
            Err(err) if err.is(crate::error::catalog::ER_INDEX_CORRUPT) => {
                Err(index.fence_persistent_corruption(err))
            }
            result => result,
        }
    }

    /// Leaf-range workers already own a shared handle lease. A queued DDL
    /// writer must not prevent those workers from finishing and releasing it.
    fn with_index_leaf_range_read<T>(
        &self,
        table_id: u64,
        index_name: &str,
        f: impl FnOnce(&IndexFile) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let handle = self.index_handle(table_id, index_name)?;
        let index = handle.read_recursive();
        index.ensure_healthy()?;
        f(&index)
    }

    /// Exclusive-access counterpart for the rare callers that need
    /// `&mut IndexFile` (DDL flush, discard during DROP, structural
    /// reset). Takes the outer `parking_lot::RwLock` in write mode so it
    /// excludes all concurrent `with_index_mut` / `with_index_read`
    /// callers.
    fn with_index_exclusive<T>(
        &self,
        table_id: u64,
        index_name: &str,
        f: impl FnOnce(&mut IndexFile) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        self.ensure_writable()?;
        let index = self.index_handle(table_id, index_name)?;
        let mut index = index.write();
        index.ensure_healthy()?;
        f(&mut index)
    }

    fn index_handle(&self, table_id: u64, index_name: &str) -> Result<IndexHandle, QueryError> {
        loop {
            // Hot SELECT path: shared read lock + zero-allocation borrowed
            // probe. `normalize_name` allocated a String per call just to
            // build the tuple key — visible in point-select profiles. The
            // raw-entry probe hashes `(u64, &str)`, which produces the same
            // hash as the stored `(u64, String)` key.
            let normalized = normalize_name_cow(index_name);
            {
                let cache = self.index_cache.read();
                let mut state = cache.hasher().build_hasher();
                use std::hash::{BuildHasher, Hash, Hasher};
                (table_id, normalized.as_ref()).hash(&mut state);
                let hash = state.finish();
                if let Some((_, index)) = cache.raw_entry().from_hash(hash, |(tid, name)| {
                    *tid == table_id && name.as_str() == normalized.as_ref()
                }) {
                    return Ok(index.clone());
                }
            }
            let key = (table_id, normalized.into_owned());
            // Resolve catalog/reorg identity before taking index_cache.write().
            // The fallback path may need a data-dictionary index; doing that
            // under the cache guard would recursively enter index_handle.
            let open_epoch = self.storage_handle_open_epoch(table_id);
            let epoch = open_epoch.load(Ordering::Acquire);
            let path = self.index_path(table_id, index_name);
            #[cfg(test)]
            after_storage_path_resolution_for_test();
            // Serialize the side-effecting open with cache installation. In
            // particular, a zero-length index open initializes the meta page and
            // empty root in the shared buffer pool. If speculative openers do
            // that concurrently outside this guard, a late loser can overwrite
            // pages already mutated through the installed winner. Cold opens are
            // rare; steady-state callers return from the read-locked fast path
            // above without touching this exclusive guard.
            let mut cache = self.index_cache.write();
            if let Some(index) = cache.get(&key) {
                return Ok(index.clone());
            }
            if open_epoch.load(Ordering::Acquire) != epoch {
                drop(cache);
                crate::engine::cancel::check_cancelled()?;
                continue;
            }
            let index: IndexHandle =
                Arc::new(parking_lot::RwLock::new(IndexFile::open_with_durability(
                    path,
                    self.log.clone(),
                    if is_temp_table_id(table_id) {
                        DurabilityMode::Memory
                    } else {
                        self.config.durability
                    },
                    self.buffer_pool.clone(),
                )?));
            // Reserve an idle victim without removing its binding. A miss for
            // that same index must reuse this handle until its final flush ends;
            // reopening earlier could publish stale metadata or let the old
            // handle truncate a file that the new one has already extended.
            let victim = if cache.len() >= object_handle_cache_max() {
                cache.iter().find_map(|(key, handle)| {
                    (Arc::strong_count(handle) == 1).then(|| (key.clone(), handle.clone()))
                })
            } else {
                None
            };
            cache.insert(key, index.clone());
            drop(cache);
            if let Some((key, victim)) = victim {
                self.evict_idle_index_handle(key, victim)?;
            }
            return Ok(index);
        }
    }

    fn evict_idle_index_handle(
        &self,
        key: (u64, String),
        victim: IndexHandle,
    ) -> Result<(), QueryError> {
        // Never wait for an index latch while holding the cache lock. If a
        // caller started using the reserved victim, leave it cached instead.
        let Some(mut index) = victim.try_write() else {
            return Ok(());
        };
        if Arc::strong_count(&victim) != 2 {
            return Ok(());
        }
        // Keep the binding on failure: dropping the final handle here would
        // hide the I/O error and permit a reopen from incomplete disk state.
        // Only this index is excluded during the potentially slow flush.
        index.flush()?;
        let removed = {
            let mut cache = self.index_cache.write();
            if Arc::strong_count(&victim) == 2
                && cache
                    .get(&key)
                    .is_some_and(|current| Arc::ptr_eq(current, &victim))
            {
                // No other caller can mutate it or obtain an Arc between
                // this check and removal. The completed flush is final; do
                // not run another file operation in Drop after reopening.
                index.retain_buffered_pages_on_drop();
                cache.remove(&key)
            } else {
                None
            }
        };
        drop(index);
        drop(removed);
        Ok(())
    }

    fn storage_generation(&self, table_id: u64) -> u64 {
        if crate::storage::data_dictionary::dd_table_schema(table_id).is_some()
            || is_temp_table_id(table_id)
        {
            return 1;
        }
        if let Ok(map) = self.reorg_storage_locs.lock() {
            if let Some(location) = map.get(&table_id) {
                return location.generation.max(1);
            }
        }
        if let Some(generation) = self.catalog.table_key_by_id(table_id).and_then(|key| {
            self.catalog
                .read()
                .ok()?
                .tables
                .get(&key)
                .map(|table| table.active_generation.max(1))
        }) {
            return generation;
        }
        self.table_by_id(table_id)
            .map(|table| table.active_generation.max(1))
            .unwrap_or(1)
    }

    fn table_path(&self, table_id: u64) -> PathBuf {
        if is_temp_table_id(table_id) {
            return crate::storage::naming::temp_tables_dir(&self.data_dir)
                .join(format!("{table_id}.heap"));
        }
        let generation = self.storage_generation(table_id);
        crate::storage::naming::table_object_generation_path(
            &self.data_dir,
            table_id,
            generation,
            "heap",
        )
    }

    fn overflow_path(&self, table_id: u64) -> PathBuf {
        if is_temp_table_id(table_id) {
            return crate::storage::naming::temp_tables_dir(&self.data_dir)
                .join(format!("{table_id}.lob"));
        }
        let generation = self.storage_generation(table_id);
        crate::storage::naming::table_object_generation_path(
            &self.data_dir,
            table_id,
            generation,
            "lob",
        )
    }

    /// Per-storage-object zone-map sidecar. Partition storage ids therefore
    /// get independent maps even though they share one logical TableSchema.
    pub(crate) fn zone_map_file_path(&self, table_id: u64) -> PathBuf {
        if is_temp_table_id(table_id) {
            return crate::storage::naming::temp_tables_dir(&self.data_dir)
                .join(format!("{table_id}.zm"));
        }
        crate::storage::naming::table_object_generation_path(
            &self.data_dir,
            table_id,
            self.storage_generation(table_id),
            "zm",
        )
    }

    fn read_auto_increment_metadata(
        &self,
        table_id: u64,
    ) -> Result<Option<(u64, u64, u64)>, QueryError> {
        if self.config.durability == DurabilityMode::Memory || is_temp_table_id(table_id) {
            return Ok(None);
        }
        use crate::storage::data_dictionary::DD_INNODB_DYNAMIC_METADATA;
        let rows = self.catalog_rows_for_selector(
            DD_INNODB_DYNAMIC_METADATA,
            CatalogRowSelector::PrimaryValues(vec![Value::UInt(table_id)]),
        )?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };
        let value = |idx: usize| match row.get(idx) {
            Some(Value::UInt(value)) => Some(*value),
            Some(Value::Int(value)) if *value >= 0 => Some(*value as u64),
            _ => None,
        };
        let version = value(1).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "invalid AUTO_INCREMENT metadata version",
            )
        })?;
        let max_value = value(2).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "invalid AUTO_INCREMENT metadata value",
            )
        })?;
        let covered_lsn = value(3).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "invalid AUTO_INCREMENT metadata LSN",
            )
        })?;
        Ok(Some((version.max(1), max_value, covered_lsn)))
    }

    fn read_auto_increment_max(&self, table_id: u64) -> Result<Option<u64>, QueryError> {
        Ok(self
            .read_auto_increment_metadata(table_id)?
            .map(|(_, max_value, _)| max_value))
    }

    fn auto_increment_state(
        &self,
        table_id: u64,
        initial_max: u64,
    ) -> Result<Arc<AutoIncrementState>, QueryError> {
        if let Some(state) = self.auto_increment.get(table_id) {
            return Ok(state);
        }
        let (version, max_value, covered_lsn) = self
            .read_auto_increment_metadata(table_id)?
            .unwrap_or((1, initial_max, 0));
        let state =
            self.auto_increment
                .get_or_install(table_id, version, max_value.max(initial_max))?;
        let mut logged = state.logged.lock();
        if version > logged.version || (version == logged.version && max_value > logged.max_value) {
            *logged = crate::storage::txn::AutoIncrementLoggedState {
                version,
                max_value,
                lsn: covered_lsn,
            };
        }
        drop(logged);
        Ok(state)
    }

    fn persist_auto_increment_state(
        &self,
        state: &Arc<AutoIncrementState>,
        version: u64,
        max_value: u64,
    ) -> Result<(), QueryError> {
        if self.config.durability == DurabilityMode::Memory || is_temp_table_id(state.table_id) {
            return Ok(());
        }
        let mut logged = state.logged.lock();
        if version < logged.version || (version == logged.version && max_value <= logged.max_value)
        {
            return Ok(());
        }
        let _fence = self.auto_increment_checkpoint_fence.read();
        {
            let mut dirty = self.auto_increment_dirty.lock().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "AUTO_INCREMENT dirty queue poisoned",
                )
            })?;
            dirty.enqueue(state.clone())?;
        }
        let lsn = self.append_wal(
            txn_id_for_ddl(),
            WalPayload::AutoIncrementAdvance {
                table_id: state.table_id,
                version,
                max_value,
            },
        )?;
        *logged = crate::storage::txn::AutoIncrementLoggedState {
            version,
            max_value,
            lsn,
        };
        Ok(())
    }

    fn persist_auto_increment_max(&self, table_id: u64, max_value: u64) -> Result<(), QueryError> {
        let state = self.auto_increment_state(table_id, 0)?;
        state.max_value.fetch_max(max_value, Ordering::AcqRel);
        let version = state.version.load(Ordering::Acquire);
        self.persist_auto_increment_state(&state, version, max_value)
    }

    fn reset_auto_increment(&self, table_id: u64) -> Result<(), QueryError> {
        let state = self.auto_increment_state(table_id, 0)?;
        let _slow = state.slow_path.lock();
        let version = state
            .version
            .load(Ordering::Acquire)
            .checked_add(1)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_AUTOINC_READ_FAILED,
                    "AUTO_INCREMENT version overflow",
                )
            })?;
        state.version.store(version, Ordering::Release);
        state.max_value.store(0, Ordering::Release);
        self.persist_auto_increment_state(&state, version, 0)
    }

    fn prepare_exchange_auto_increment(
        &self,
        old_table_id: u64,
        new_table_id: u64,
        minimum: u64,
    ) -> Result<(u64, u64, u64), QueryError> {
        let old = self.auto_increment_state(old_table_id, 0)?;
        let old_version = old.version.load(Ordering::Acquire);
        let max_value = old.max_value.load(Ordering::Acquire).max(minimum);
        let existing_version = self
            .read_auto_increment_metadata(new_table_id)?
            .map(|metadata| metadata.0)
            .unwrap_or(0);
        let version = old_version
            .max(existing_version)
            .checked_add(1)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_AUTOINC_READ_FAILED,
                    "AUTO_INCREMENT version overflow",
                )
            })?;
        Ok((new_table_id, version, max_value))
    }

    fn apply_exchange_auto_increment(
        &self,
        table_id: u64,
        version: u64,
        max_value: u64,
    ) -> Result<(), QueryError> {
        self.auto_increment.install(table_id, version, max_value)?;
        let state = self
            .auto_increment
            .get(table_id)
            .expect("installed AUTO_INCREMENT state");
        self.persist_auto_increment_state(&state, version, max_value)
    }

    fn spatial_index_named(&self, table_id: u64, name: &str) -> bool {
        if let Some(names) = self
            .reorg_storage_locs
            .lock()
            .ok()
            .and_then(|m| m.get(&table_id).and_then(|l| l.spatial_indexes.clone()))
        {
            return names.contains(normalize_name_cow(name).as_ref());
        }
        if let Some(table) = self.table_by_id(table_id) {
            return table
                .indexes
                .iter()
                .any(|i| i.name.eq_ignore_ascii_case(name) && i.is_spatial());
        }
        let location = self
            .reorg_storage_locs
            .lock()
            .ok()
            .and_then(|m| m.get(&table_id).cloned());
        location.is_some_and(|loc| {
            self.catalog
                .read()
                .ok()
                .and_then(|c| {
                    c.tables
                        .get(&crate::storage::naming::table_key(
                            &loc.database,
                            &loc.table_name,
                        ))
                        .cloned()
                })
                .is_some_and(|t| {
                    t.indexes
                        .iter()
                        .any(|i| i.name.eq_ignore_ascii_case(name) && i.is_spatial())
                })
        })
    }

    fn index_path(&self, table_id: u64, index_name: &str) -> PathBuf {
        if is_temp_table_id(table_id) {
            return crate::storage::naming::temp_indexes_dir(&self.data_dir).join(format!(
                "{table_id}_{}.{}",
                sanitize_name(index_name),
                if self.spatial_index_named(table_id, index_name) {
                    "rtree"
                } else {
                    "idx"
                }
            ));
        }
        let private_index_id = self.reorg_storage_locs.lock().ok().and_then(|map| {
            map.get(&table_id)
                .and_then(|location| location.index_ids.as_ref())
                .and_then(|ids| ids.get(normalize_name_cow(index_name).as_ref()).copied())
        });
        let mut index_id = private_index_id
            .or_else(|| {
                crate::storage::data_dictionary::dd_table_schema(table_id).and_then(|table| {
                    table
                        .indexes
                        .iter()
                        .find(|index| index.name.eq_ignore_ascii_case(index_name))
                        .map(|index| index.index_id)
                })
            })
            .or_else(|| {
                self.table_by_id(table_id).and_then(|table| {
                    table
                        .indexes
                        .iter()
                        .find(|index| index.name.eq_ignore_ascii_case(index_name))
                        .map(|index| index.index_id)
                })
            });
        // A private generation uses a fresh storage id that is deliberately
        // absent from the public catalog. Resolve its owning logical table
        // through the reorg map so existing indexes keep their stable IDs;
        // otherwise the initial handle would be opened under a name hash
        // while the publish path expects the persisted index_id.
        if index_id.is_none() {
            let owner = self
                .reorg_storage_locs
                .lock()
                .ok()
                .and_then(|map| map.get(&table_id).cloned());
            if let Some(location) = owner {
                index_id = self.catalog.read().ok().and_then(|catalog| {
                    catalog
                        .tables
                        .get(&crate::storage::naming::table_key(
                            &location.database,
                            &location.table_name,
                        ))
                        .and_then(|table| {
                            table
                                .indexes
                                .iter()
                                .find(|index| index.name.eq_ignore_ascii_case(index_name))
                                .map(|index| index.index_id)
                        })
                });
            }
        }
        let index_id = index_id.unwrap_or_else(|| stable_missing_index_id(index_name));
        crate::storage::naming::index_object_path_by_id(
            &self.data_dir,
            table_id,
            self.storage_generation(table_id),
            index_id,
            if self.spatial_index_named(table_id, index_name) {
                "rtree"
            } else {
                "idx"
            },
        )
    }

    fn index_path_for_table_schema(
        &self,
        table: &TableSchema,
        table_id: u64,
        index_name: &str,
    ) -> PathBuf {
        if is_temp_table_id(table_id) {
            return crate::storage::naming::temp_indexes_dir(&self.data_dir).join(format!(
                "{table_id}_{}.{}",
                sanitize_name(index_name),
                if table
                    .indexes
                    .iter()
                    .any(|index| index.name.eq_ignore_ascii_case(index_name) && index.is_spatial())
                {
                    "rtree"
                } else {
                    "idx"
                }
            ));
        }
        let index_id = table
            .indexes
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case(index_name))
            .map(|index| index.index_id)
            .unwrap_or_else(|| stable_missing_index_id(index_name));
        crate::storage::naming::index_object_path_by_id(
            &self.data_dir,
            table_id,
            table.active_generation.max(1),
            index_id,
            if table
                .indexes
                .iter()
                .any(|i| i.name.eq_ignore_ascii_case(index_name) && i.is_spatial())
            {
                "rtree"
            } else {
                "idx"
            },
        )
    }

    #[cfg(test)]
    fn dirty_index_page_count(&self) -> usize {
        self.buffer_pool.dirty_index_page_count()
    }

    fn buffer_stats(&self) -> BufferStats {
        self.buffer_pool.buffer_stats()
    }

    pub(crate) fn buffer_stats_counts(&self) -> (u64, u64) {
        if let Some(query_stats) = current_query_buffer_stats_counts() {
            return query_stats;
        }
        let stats = self.buffer_stats();
        (stats.hits, stats.misses)
    }

    fn buffer_pool_snapshot(&self) -> BufferPoolSnapshot {
        self.buffer_pool.snapshot()
    }

    pub(crate) fn buffer_page_diagnostics(&self) -> Vec<BufferPageDiagnostic> {
        self.buffer_pool.diagnostic_pages()
    }

    /// Physical page count for the heap belonging to a specific storage id.
    /// A warm handle supplies the count without I/O. For a cold durable
    /// table, fall back to the file length instead of returning 0: table
    /// metadata must not change merely because a heap has (or has not) been
    /// touched since restart.
    ///
    /// Partition-aware: `storage_id` may be a partition's id, since
    /// each partition has its own independent heap.
    pub(crate) fn heap_page_count(&self, storage_id: u64) -> u64 {
        let cache = self.heap_cache.read();
        if let Some(heap) = cache.get(&storage_id).cloned() {
            drop(cache);
            return heap.read().page_count_relaxed();
        }
        drop(cache);
        fs::metadata(self.table_path(storage_id))
            .ok()
            .map(|meta| meta.len() / PAGE_SIZE as u64)
            .unwrap_or(0)
    }

    /// Best-effort *physical* `.lob` overflow snapshot `(page_count,
    /// free_count)` for one storage id. A cold durable handle still reports
    /// the physical page count from file metadata. `free_count` is kept only
    /// in memory (and rebuilt by `OverflowFile::open`), so a cold handle
    /// conservatively reports zero reclaimable pages without opening and
    /// walking a potentially very large free list.
    ///
    /// `page_count` includes the meta page (page 0) — it is the physical
    /// `.lob` length in pages, matching the "physical page count"
    /// semantics [`Self::heap_page_count`] uses for the heap.
    pub(crate) fn lob_page_count(&self, storage_id: u64) -> (u64, u64) {
        let cache = self.overflow_cache.read();
        if let Some(overflow) = cache.get(&storage_id).cloned() {
            drop(cache);
            let overflow = overflow.read();
            return (overflow.page_count(), overflow.free_count());
        }
        drop(cache);
        let pages = fs::metadata(self.overflow_path(storage_id))
            .ok()
            .map(|meta| meta.len() / PAGE_SIZE as u64)
            .unwrap_or(0);
        (pages, 0)
    }

    /// Best-effort *physical* page count for one named index on a
    /// specific storage id. A cold durable index falls back to its file
    /// length, for the same cache-independent metadata semantics as
    /// [`Self::heap_page_count`].
    pub(crate) fn index_page_count(&self, storage_id: u64, index_name: &str) -> u64 {
        let key = (storage_id, normalize_name(index_name));
        let cache = self.index_cache.read();
        if let Some(index) = cache.get(&key).cloned() {
            drop(cache);
            return index.read().page_count();
        }
        drop(cache);
        fs::metadata(self.index_path(storage_id, index_name))
            .ok()
            .map(|meta| meta.len() / PAGE_SIZE as u64)
            .unwrap_or(0)
    }

    /// Physical bytes occupied by one public index. Ordinary indexes use
    /// the `.idx` page count above. Vector indexes have backend-specific
    /// `.hnsw` / `.hvec`  files, so account for those files rather
    /// than silently treating the index as absent from `INDEX_LENGTH`.
    pub(crate) fn index_storage_bytes(
        &self,
        table: &TableSchema,
        storage_id: u64,
        index: &IndexSchema,
    ) -> u64 {
        if !index.is_vector() {
            return self
                .index_page_count(storage_id, &index.name)
                .saturating_mul(PAGE_SIZE as u64);
        }
        let paths = match self.owned_vector_files(table, index, storage_id) {
            Ok(paths) => paths,
            Err(err) => {
                self.quarantine_mvcc(format!("vector size accounting failed: {err:?}"));
                return 0;
            }
        };
        paths
            .iter()
            .filter_map(|path| fs::metadata(path).ok())
            .map(|meta| meta.len())
            .fold(0u64, u64::saturating_add)
    }
}

#[cfg(test)]
mod catalog_bootstrap_diagnostic_tests {
    use super::*;

    #[test]
    fn missing_catalog_authority_reports_evidence_without_rebootstrapping() {
        use crate::storage::data_dictionary::DD_DD_PROPERTIES;
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let mut txn = CatalogTxn::new(&db);
        txn.replace(
            DD_DD_PROPERTIES,
            CatalogRowSelector::PrimaryValues(vec![Value::String("catalog_version".into())]),
            Vec::new(),
        );
        txn.set_delta(Vec::new(), false);
        txn.commit().unwrap();
        let before = db.scan_dd_table(DD_DD_PROPERTIES).unwrap();
        assert!(!before
            .iter()
            .any(|row| row.first().and_then(crate::types::value_str) == Some("catalog_version")));
        let err = db.load_or_bootstrap_catalog().unwrap_err();
        assert_eq!(err.code(), 1030);
        for evidence in [
            "refusing bootstrap",
            "property_rows=",
            "deleted=",
            "invisible=",
            "version_rows=",
            "heap_pages=",
            "next_txn_id=",
        ] {
            assert!(
                err.message().contains(evidence),
                "missing {evidence}: {err:?}"
            );
        }
        assert_eq!(db.scan_dd_table(DD_DD_PROPERTIES).unwrap(), before);
    }
}
