// Crash-safe object retirement: transactional DD manifests and startup recovery.
// New whole-table and index retirements commit their ownership with DDL. Local
// JSON intents remain for unpublished private objects and recovery fixtures.
//
// DROP TABLE must move a table and every physical file it owns from "present"
// to "absent" atomically. The naive "delete files, then commit the catalog"
// ordering has a crash window: files are unlinked (with an empty WAL
// before-image, so unrecoverable) but the data-dictionary generation has not
// committed yet — recovery redoes the txn-0 deletes and rolls the catalog back,
// leaving a *zombie* table (catalog says it exists, its heap/index/LOB files
// are gone).
//
// This module makes DROP crash-safe with a *commit-first* protocol backed by a
// durable intent log, mirroring `partition_reorg` / `alter_copy`:
//
//   1. Record a `DropTableIntent` listing every physical file, fsync it.
//   2. Commit the catalog change (`commit_catalog_txn`) — the single atomic point.
//   3. Unlink the files (idempotent; recovery redoes this on crash).
//   4. Remove the intent.
//
// Recovery (`recover_drop_tables`, run after the catalog is loaded and WAL
// redo/undo has replayed) drives every lingering intent to a consistent state:
//   - table still in catalog  → the drop never committed; files are intact
//                               (step 3 is post-commit). Drop the stale intent.
//   - table absent            → the drop committed; finish unlinking any file
//                               the intent still lists, then drop the intent.
// Table ids are monotonic (never reused), so `table_by_id(intent.table_id)`
// unambiguously reports whether *this* table survived the commit.

const DROP_TABLE_INTENT_DIR: &str = "ddl/drop_table";

#[cfg(test)]
static FULLTEXT_DROP_HOLD_US: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
struct FulltextDropHoldTimer(Instant);

#[cfg(test)]
impl Drop for FulltextDropHoldTimer {
    fn drop(&mut self) {
        FULLTEXT_DROP_HOLD_US.fetch_add(self.0.elapsed().as_micros() as u64, Ordering::Relaxed);
    }
}

struct CatalogRetirementWaker(Box<dyn Fn() + Send + Sync>);

impl std::fmt::Debug for CatalogRetirementWaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CatalogRetirementWaker(..)")
    }
}

fn drop_table_intent_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(DROP_TABLE_INTENT_DIR)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RetiredTableCache {
    storage_ids: Vec<u64>,
    index_names: Vec<String>,
}

impl RetiredTableCache {
    fn for_table(table: &TableSchema) -> Self {
        Self {
            storage_ids: table.storage_ids(),
            index_names: table.indexes.iter().map(|index| index.name.clone()).collect(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum RetiredFulltextPrefix {
    Table,
    Generation(u64),
    Corpus { generation: u64, corpus_id: u64 },
}

impl RetiredFulltextPrefix {
    fn covers(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Table, _) => true,
            (Self::Generation(a), Self::Generation(b) | Self::Corpus { generation: b, .. }) => {
                a == b
            }
            (
                Self::Corpus {
                    generation: a,
                    corpus_id: ca,
                },
                Self::Corpus {
                    generation: b,
                    corpus_id: cb,
                },
            ) => a == b && ca == cb,
            _ => false,
        }
    }

    fn key(&self, table_id: u64) -> IndexKey {
        let mut values = vec![KeyPart::UInt(table_id)];
        match *self {
            Self::Table => {}
            Self::Generation(generation) => values.push(KeyPart::UInt(generation)),
            Self::Corpus {
                generation,
                corpus_id,
            } => {
                values.extend([KeyPart::UInt(generation), KeyPart::UInt(corpus_id)]);
            }
        }
        IndexKey::from(values)
    }

    fn is_live(&self, table: &TableSchema) -> bool {
        match *self {
            Self::Table => true,
            Self::Generation(generation) => table.active_generation == generation,
            Self::Corpus {
                generation,
                corpus_id,
            } => {
                table.active_generation == generation
                    && table.indexes.iter().any(|index| {
                        index
                            .fulltext
                            .as_ref()
                            .is_some_and(|meta| meta.corpus_id == corpus_id)
                    })
            }
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RetiredFulltextCounters {
    prefixes: Vec<RetiredFulltextPrefix>,
    resume: Option<(IndexKey, RowId)>,
}

#[cfg(test)]
thread_local! {
    static FULLTEXT_RETIREMENT_CRASH_POINT: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct DropTableIntent {
    table_id: u64,
    /// Physical indexes use (index_id, generation >= 1). Counter-only jobs
    /// use (committing transaction_id, 0); None owns whole-table files.
    #[serde(default)]
    retired_index: Option<(u64, u64)>,
    #[serde(default)]
    had_auto_increment: bool,
    database: String,
    table_name: String,
    /// Relative-to-`data_dir` paths of the non-vector physical files to unlink
    /// (heap / lob / zone-map / btree-idx). Removing these
    /// only after the catalog commit is what makes DROP crash-safe.
    files: Vec<String>,
    /// Relative paths of paged / derived vector index files (`.hnsw` / `.hvec`). Their buffer-pool pages are discarded before the unlink so a
    /// writeback cannot resurrect the file.
    vector_files: Vec<String>,
    /// Stable index-owned directories also cover publications completed after
    /// the object was recycled. Their ownership never depends on SQL names.
    #[serde(default)]
    vector_directories: Vec<String>,
    /// Cache eviction may follow namespace publication. Stable retired ids
    /// keep cleanup independent of a replacement table's SQL name.
    #[serde(default)]
    retired_table_cache: Option<RetiredTableCache>,
    #[serde(default)]
    retired_fulltext: Option<RetiredFulltextCounters>,
}

impl Database {
    #[cfg(test)]
    pub(crate) fn fulltext_drop_hold_us_for_test(&self) -> u64 {
        FULLTEXT_DROP_HOLD_US.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn set_fulltext_retirement_crash_point_for_test(&self, point: u8) {
        FULLTEXT_RETIREMENT_CRASH_POINT.with(|slot| slot.set(point));
    }

    #[cfg(test)]
    fn crash_fulltext_retirement_for_test(&self, txn_id: u64, point: u8) -> Result<(), QueryError> {
        if FULLTEXT_RETIREMENT_CRASH_POINT.with(|slot| slot.get()) == point {
            // Persist partial DD mutations as well as the decision, if any.
            // Recovery must undo an unfinished batch and its cursor together.
            self.flush_txn_wal_buffer(txn_id, None)?;
            self.flush_dirty_data()?;
            let lsn = self.log.lock().last_lsn();
            LogManager::sync_durable_to(&self.log, lsn)?;
            std::process::exit(86);
        }
        Ok(())
    }

    pub(crate) fn set_catalog_retirement_waker(&self, wake: impl Fn() + Send + Sync + 'static) {
        *self.ddl_retirement_waker.lock() = Some(CatalogRetirementWaker(Box::new(wake)));
    }

    fn request_catalog_retirement_cleanup(&self) {
        if let Some(wake) = self.ddl_retirement_waker.lock().as_ref() {
            (wake.0)();
        }
    }

    #[cfg(test)]
    pub(crate) fn install_retirement_cleanup_test_hook(&self) -> Arc<DdlBackfillTestHook> {
        let hook = Arc::new(DdlBackfillTestHook::new());
        *self.retirement_cleanup_test_hook.lock().unwrap() = Some(hook.clone());
        hook
    }

    // Never recurse: a sibling generation/index may still belong to a live table.
    fn remove_empty_object_directories(&self, paths: impl IntoIterator<Item = PathBuf>) -> Result<(), QueryError> {
        let roots = [self.data_dir.join("objects/tables"), self.data_dir.join("objects/indexes")];
        let mut directories = BTreeSet::new();
        for path in paths {
            for root in &roots {
                if path.starts_with(root) {
                    let mut parent = path.parent();
                    while let Some(dir) = parent.filter(|dir| *dir != root.as_path()) {
                        directories.insert(dir.to_path_buf());
                        parent = dir.parent();
                    }
                }
            }
        }
        for dir in directories.iter().rev() {
            match fs::remove_dir(dir) {
                Ok(()) => {},
                Err(err) if matches!(err.kind(), std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty) => {},
                Err(err) => return Err(io_err(err)),
            }
        }
        Ok(())
    }

    fn retirement_selector(intent: &DropTableIntent) -> CatalogRowSelector {
        let (index_id, generation) = intent.retired_index.unwrap_or((0, 0));
        CatalogRowSelector::PrimaryValues(vec![
            Value::UInt(intent.table_id),
            Value::UInt(generation),
            Value::UInt(index_id),
        ])
    }

    fn retirement_is_live(&self, intent: &DropTableIntent) -> bool {
        self.table_by_id(intent.table_id).is_some_and(|table| {
            if let Some(retired) = &intent.retired_fulltext {
                return retired.prefixes.iter().any(|prefix| prefix.is_live(&table));
            }
            intent.retired_index.is_none_or(|(id, generation)| {
                table.active_generation.max(1) == generation
                    && table.indexes.iter().any(|index| index.index_id == id)
            })
        })
    }

    fn decode_retirement(values: &[Value]) -> Result<DropTableIntent, QueryError> {
        let text = values
            .get(3)
            .and_then(crate::types::value_str)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid catalog retirement payload",
                )
            })?;
        let intent: DropTableIntent = serde_json::from_str(text).map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("invalid catalog retirement: {err}"),
            )
        })?;
        if let Some(retired) = &intent.retired_fulltext {
            let valid = intent
                .retired_index
                .is_some_and(|(job, generation)| job != 0 && generation == 0)
                && intent.files.is_empty()
                && intent.vector_files.is_empty()
                && !retired.prefixes.is_empty()
                && retired.prefixes.iter().all(|prefix| match prefix {
                    RetiredFulltextPrefix::Table => true,
                    RetiredFulltextPrefix::Generation(generation) => *generation != 0,
                    RetiredFulltextPrefix::Corpus {
                        generation,
                        corpus_id,
                    } => *generation != 0 && *corpus_id != 0,
                });
            if !valid {
                return Err(fulltext_statistics_error("invalid retired counter prefix"));
            }
            let prefix = retired.prefixes.last().unwrap().key(intent.table_id);
            if retired
                .resume
                .as_ref()
                .is_some_and(|(key, _)| !key.0.starts_with(&prefix.0))
            {
                return Err(fulltext_statistics_error("invalid retired counter cursor"));
            }
        }
        let (index_id, generation) = intent.retired_index.unwrap_or((0, 0));
        if values.get(..3)
            != Some(
                [
                    Value::UInt(intent.table_id),
                    Value::UInt(generation),
                    Value::UInt(index_id),
                ]
                .as_slice(),
            )
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "catalog retirement identity mismatch",
            ));
        }
        Ok(intent)
    }

    /// The defining DD commit is the only durability barrier. Completed older
    /// retirements are removed in that same commit, after their FileDelete WAL.
    fn prepare_index_retirement(
        &self,
        intent: &DropTableIntent,
    ) -> Result<CatalogTxn<'_>, QueryError> {
        use crate::storage::data_dictionary::DD_DDL_RETIREMENTS;
        #[cfg(test)]
        if DDL_CATALOG_COMMIT_FAILURE.with(|fail| fail.replace(false)) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "injected catalog commit failure",
            ));
        }
        let mut txn = self.build_catalog_txn_incremental()?.ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "index retirement requires a catalog change",
            )
        })?;
        if let Some(_cleanup) = self.ddl_retirement_cleanup.try_lock() {
            let table =
                crate::storage::data_dictionary::dd_table_schema(DD_DDL_RETIREMENTS).unwrap();
            let previous = self.catalog_selected_row_ids(
                &table,
                &CatalogRowSelector::PrefixValues(vec![Value::UInt(intent.table_id)]),
            )?;
            if previous.len() > 4096 {
                return Err(error(
                    crate::error::catalog::ER_OUT_OF_RESOURCES,
                    "too many pending index retirements; retry after cleanup",
                ));
            }
            for row_id in previous.into_iter().take(64) {
                let Some(row) = self.read_mvcc_row(DD_DDL_RETIREMENTS, row_id)? else {
                    continue;
                };
                if row.header.is_deleted() {
                    continue;
                }
                let old = Self::decode_retirement(&row.values)?;
                if old.retired_fulltext.is_none()
                    && !self.retirement_is_live(&old)
                    && old
                        .files
                        .iter()
                        .chain(&old.vector_files)
                        .all(|path| !self.data_dir.join(path).exists())
                {
                    // Completion WAL precedes the DD durability barrier that
                    // removes the old manifest. Preparation never unlinks.
                    for path in old.files.iter().chain(&old.vector_files) {
                        self.append_wal(
                            txn_id_for_ddl(),
                            WalPayload::FileDelete {
                                path: path.clone(),
                                before: Vec::new(),
                            },
                        )?;
                    }
                    txn.replace(
                        DD_DDL_RETIREMENTS,
                        Self::retirement_selector(&old),
                        Vec::new(),
                    );
                }
            }
        }
        self.add_index_retirement(&mut txn, intent)?;
        Ok(txn)
    }

    fn stage_fulltext_generation_retirement(
        &self,
        txn: &mut CatalogTxn<'_>,
        table: &TableSchema,
        generation: u64,
    ) -> Result<(), QueryError> {
        txn.retire_fulltext_prefix(table.id, RetiredFulltextPrefix::Generation(generation));
        Ok(())
    }

    fn stage_fulltext_table_retirement(
        &self,
        txn: &mut CatalogTxn<'_>,
        table_id: u64,
    ) -> Result<(), QueryError> {
        use crate::storage::data_dictionary::DD_FULLTEXT_COUNTERS;
        // A single raw entry is a conservative existence probe, including
        // historical/private families. Ordinary tables with no such keys need
        // no additional manifest. Never enumerate a dropped table's counters.
        let prefix = RetiredFulltextPrefix::Table.key(table_id);
        let low = IndexBound {
            key: prefix.clone(),
            inclusive: true,
        };
        let exists = self.with_index_read(DD_FULLTEXT_COUNTERS, "PRIMARY", |index| {
            Ok(index
                .range_entries_chunk(Some(&low), None, None, 1)?
                .first()
                .is_some_and(|(key, _)| key.0.starts_with(&prefix.0)))
        })?;
        if exists {
            txn.retire_fulltext_prefix(table_id, RetiredFulltextPrefix::Table);
        }
        Ok(())
    }

    fn retire_fulltext_counter_batch(
        &self,
        intent: &mut DropTableIntent,
        read: TxnView,
        txn: &mut CatalogTxn<'_>,
        stop: &dyn Fn() -> bool,
    ) -> Result<bool, QueryError> {
        use crate::storage::data_dictionary::DD_FULLTEXT_COUNTERS;
        #[cfg(test)]
        {
            let hook = self.retirement_cleanup_test_hook.lock().unwrap().take();
            if let Some(hook) = hook {
                hook.pause();
            }
        }
        let retired = intent.retired_fulltext.as_mut().unwrap();
        // Bound both the raw traversal (including MVCC tombstones) and the DD
        // transaction. Persist the cursor with each batch so restart resumes
        // without rescanning retained historical keys or skipping live rows.
        let _scratch = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
            8 * 1024 * 1024,
        )?;
        let schema =
            crate::storage::data_dictionary::dd_table_schema(DD_FULLTEXT_COUNTERS).unwrap();
        let primary = Self::dd_primary_index(&schema)?;
        let prefix = retired
            .prefixes
            .last()
            .ok_or_else(|| fulltext_statistics_error("empty retired prefix list"))?
            .key(intent.table_id);
        let low = IndexBound {
            key: prefix.clone(),
            inclusive: true,
        };
        let entries = self.with_index_read(DD_FULLTEXT_COUNTERS, &primary.name, |index| {
            index.range_entries_chunk(Some(&low), None, retired.resume.as_ref(), 257)
        })?;
        for (key, row_id) in entries.iter().take(256) {
            if !key.0.starts_with(&prefix.0) {
                break;
            }
            if stop() {
                return Ok(false);
            }
            if let Some(row) = self.read_row_at(&schema, DD_FULLTEXT_COUNTERS, *row_id, read)? {
                if schema.index_key(primary, &row.values)? == *key {
                    let values = primary
                        .columns
                        .iter()
                        .map(|name| schema.column_index(name).map(|pos| row.values[pos].clone()))
                        .collect::<Result<Vec<_>, _>>()?;
                    txn.replace(
                        DD_FULLTEXT_COUNTERS,
                        CatalogRowSelector::PrimaryValues(values),
                        Vec::new(),
                    );
                }
            }
            retired.resume = Some((key.clone(), *row_id));
        }
        if entries
            .get(256)
            .is_none_or(|(key, _)| !key.0.starts_with(&prefix.0))
        {
            retired.prefixes.pop();
            retired.resume = None;
        }
        Ok(retired.prefixes.is_empty())
    }

    fn add_index_retirement(
        &self,
        txn: &mut CatalogTxn<'_>,
        intent: &DropTableIntent,
    ) -> Result<(), QueryError> {
        use crate::storage::data_dictionary::DD_DDL_RETIREMENTS;
        let (index_id, generation) = intent.retired_index.unwrap();
        let manifest = serde_json::to_string(intent).map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("serialize retirement: {err}"),
            )
        })?;
        txn.replace(
            DD_DDL_RETIREMENTS,
            Self::retirement_selector(intent),
            vec![vec![
                Value::UInt(intent.table_id),
                Value::UInt(generation),
                Value::UInt(index_id),
                Value::String(manifest),
            ]],
        );
        Ok(())
    }

    pub(crate) fn maintain_catalog_retirements_until(
        &self,
        stop: impl Fn() -> bool,
    ) -> Result<usize, QueryError> {
        use crate::storage::data_dictionary::DD_DDL_RETIREMENTS;
        let _role = self.zone_map_gate.read_recursive();
        if self.is_snapshot_read_only() {
            return Ok(0);
        }
        let Some(_maintenance) = self.backup_maintenance_gate.try_read() else {
            return Ok(0);
        };
        if self.capture.active.load(Ordering::Acquire) {
            return Ok(0);
        }
        let Some(mut cursor) = self.ddl_retirement_cleanup.try_lock() else {
            return Ok(0);
        };
        if stop() {
            return Ok(0);
        }
        let table = crate::storage::data_dictionary::dd_table_schema(DD_DDL_RETIREMENTS).unwrap();
        let entries = self.with_index_read(DD_DDL_RETIREMENTS, "PRIMARY", |index| {
            index.range_entries_chunk(None, None, cursor.as_ref(), 64)
        })?;
        if entries.is_empty() {
            *cursor = None;
            return Ok(0);
        }
        let snapshot = StorageReadSnapshot::new(self);
        // Visibility precedes strict commit I/O. Every selected manifest must
        // be durable before the corresponding old files can be removed.
        let covered = self.log.lock().last_lsn();
        if covered != 0 && self.config.durability != DurabilityMode::Memory {
            crate::wal::LogManager::sync_durable_to(&self.log, covered)?;
        }
        let mut completed = 0;
        let mut counters_pending = false;
        let mut txn = CatalogTxn::new(self);
        let mut visited = None;
        for (key, row_id) in &entries {
            if stop() {
                break;
            }
            let previous = visited.clone();
            visited = Some((key.clone(), *row_id));
            let Some(row) = self.read_row_at(&table, DD_DDL_RETIREMENTS, *row_id, snapshot.txn)?
            else {
                continue;
            };
            let primary = Self::dd_primary_index(&table)?;
            if table.index_key(primary, &row.values)? != *key {
                continue;
            }
            let mut intent = Self::decode_retirement(&row.values)?;
            if self.retirement_is_live(&intent) {
                continue;
            }
            let _disk_budget = match self.admit_disk_cleanup(Some(&intent), 0, false) {
                Ok(budget) => budget,
                Err(err) if err.is(crate::error::catalog::ER_DISK_FULL_NOWAIT) => continue,
                Err(err) => return Err(err),
            };
            if intent.retired_fulltext.is_some() {
                // Only the authority removes DD rows. Replicas consume its WAL;
                // local cleanup must never generate a divergent transaction.
                if self.physical_replica_mode() {
                    continue;
                }
                let finished = self.retire_fulltext_counter_batch(
                    &mut intent, snapshot.txn, &mut txn, &stop,
                )?;
                if finished {
                    txn.replace(DD_DDL_RETIREMENTS, Self::retirement_selector(&intent), Vec::new());
                } else {
                    self.add_index_retirement(&mut txn, &intent)?;
                    visited = previous;
                    counters_pending = true;
                }
                completed += 1;
                // One counter batch per maintenance call keeps memory, WAL,
                // shared DD latches and the cleanup mutex bounded.
                break;
            }
            let cleaned = if self.physical_replica_mode() {
                intent
                    .retired_table_cache
                    .as_ref()
                    .map_or(Ok(()), |cache| {
                        self.evict_retired_table_caches(intent.table_id, cache)
                    })
                    .and_then(|()| {
                        intent
                            .files
                            .iter()
                            .chain(&intent.vector_files)
                            .try_for_each(|path| {
                                self.buffer_pool
                                    .discard_pages_for_path(&self.data_dir.join(path))?;
                                match fs::remove_file(self.data_dir.join(path)) {
                                    Ok(()) => Ok(()),
                                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                                        Ok(())
                                    }
                                    Err(err) => Err(io_err(err)),
                                }
                            })
                    })
                    .is_ok()
            } else {
                self.purge_dropped_table_files(&intent)
            };
            if cleaned {
                completed += 1;
                if !self.physical_replica_mode() {
                    txn.replace(
                        DD_DDL_RETIREMENTS,
                        Self::retirement_selector(&intent),
                        Vec::new(),
                    );
                }
            }
        }
        if let Some(visited) = visited {
            *cursor = Some(visited);
        }
        if !txn.replacements.is_empty() {
            txn.commit()?;
        }
        drop(cursor);
        if counters_pending {
            self.request_catalog_retirement_cleanup();
        }
        Ok(completed)
    }

    fn recover_catalog_retirements(&self) -> Result<(), QueryError> {
        // Bounded startup work; remaining manifests are retried by maintenance.
        for _ in 0..16 {
            if self.maintain_catalog_retirements_until(|| false)? == 0 {
                break;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn catalog_retirement_count_for_test(&self) -> usize {
        self.scan_dd_table(crate::storage::data_dictionary::DD_DDL_RETIREMENTS)
            .unwrap()
            .len()
    }

    /// Enumerate every physical file the table owns, while the catalog still
    /// resolves its `(database, name)` (path helpers route through
    /// `table_loc_fs`, which only works before the catalog row is removed).
    fn build_drop_table_intent(&self, table: &TableSchema) -> Result<DropTableIntent, QueryError> {
        let mut files = Vec::new();
        let mut vector_files = Vec::new();
        let mut vector_directories = Vec::new();
        for sid in table.storage_ids() {
            files.push(self.relative_path(&self.table_path(sid)));
            files.push(self.relative_path(&self.overflow_path(sid)));
            files.push(self.relative_path(&self.zone_map_file_path(sid)));
            for index in &table.indexes {
                files.push(self.relative_path(&self.index_path_for_table_schema(table, sid, &index.name)));
                if index.is_vector() {
                    vector_directories.push(self.relative_path(&self.vector_index_path_for_parts(sid, table.active_generation.max(1), index.index_id, "vmanifest").with_extension("vector")));
                    vector_files.extend(self.owned_vector_files(table, index, sid)?.iter().map(|path| self.relative_path(path)));
                }
            }
        }
        Ok(DropTableIntent {
            table_id: table.id,
            retired_index: None,
            had_auto_increment: table.auto_increment_column_index().is_some(),
            database: table.database.clone(),
            table_name: table.name.clone(),
            files,
            vector_files,
            vector_directories,
            retired_table_cache: Some(RetiredTableCache::for_table(table)),
            retired_fulltext: None,
        })
    }

    /// Persist a drop intent (fsync'd). Returns `None` in memory-durability mode
    /// (nothing on disk to recover, so no intent is needed).
    fn write_drop_table_intent(
        &self,
        intent: &DropTableIntent,
    ) -> Result<Option<PathBuf>, QueryError> {
        if self.config.durability == DurabilityMode::Memory {
            return Ok(None);
        }
        let dir = drop_table_intent_dir(&self.data_dir);
        std::fs::create_dir_all(&dir).map_err(io_err)?;
        let path = dir.join(match intent.retired_index {
            Some((index_id, generation)) => {
                format!("index-{}-{generation}-{index_id}.json", intent.table_id)
            }
            None => format!("{}.json", intent.table_id),
        });
        let bytes = serde_json::to_vec(intent).map_err(|e| {
            error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                format!("serialize drop-table intent: {e}"),
            )
        })?;
        crate::util::file::write_staged_file(&path, &path.with_extension("json.tmp"), &bytes, true)
            .map_err(io_err)?;
        Ok(Some(path))
    }

    fn remove_drop_table_intent(&self, path: Option<&PathBuf>) {
        if let Some(path) = path {
            if std::fs::remove_file(path).is_ok() {
                if let Some(parent) = path.parent() {
                    let _ = crate::platform::fs::sync_path(parent);
                }
            }
        }
    }

    /// Drop the table's in-memory caches and buffer-pool frames *without*
    /// deleting any file. Run after the catalog commit and before the physical
    /// purge so a stray writeback cannot recreate a file we are about to unlink.
    fn evict_dropped_table_caches(&self, table: &TableSchema) -> Result<(), QueryError> {
        self.evict_retired_table_caches(table.id, &RetiredTableCache::for_table(table))
    }

    fn evict_retired_table_caches(
        &self,
        table_id: u64,
        cache: &RetiredTableCache,
    ) -> Result<(), QueryError> {
        // Retired readers must be invalid before their files are unlinked.
        self.evict_table_vector_graphs(table_id);
        self.vector_table_snapshot_floors.write().remove(&table_id);
        for &sid in &cache.storage_ids {
            self.remove_heap_cache_entry(sid)?;
            self.remove_overflow_cache_entry(sid)?;
            self.forget_zone_map(sid)?;
            for name in &cache.index_names {
                self.remove_index_cache_entry(sid, name)?;
            }
        }
        self.auto_increment.forget_table(table_id);
        Ok(())
    }

    /// Close stale writer bindings before releasing the publication barrier.
    /// Physical handles/files remain owned by the committed retirement row.
    fn finish_retired_table_publication(&self, table: &TableSchema) {
        for id in table.storage_ids().into_iter().chain([table.id]) {
            self.table_locks_sharded.set_schema_floor(id, u64::MAX);
        }
        if table.indexes.iter().any(IndexSchema::is_vector) {
            // Retired schema floors fence cold builders. Revoke warm handles
            // but keep their allocations/mappings in the registry until cleanup.
            for ((owner, _, _, _), graph) in self.vector_indexes.read().iter() {
                if *owner == table.id {
                    graph.revoke();
                }
            }
        }
        self.catalog
            .forget_dropped_table(&table.qualified_name(), table);
        self.table_write_epochs.write().remove(&table.id);
        self.drop_table_modification_delta_without_epoch(table.id);
    }

    /// Unlink every physical file named by the intent. Idempotent: tolerates
    /// files a prior (crashed) attempt already removed. Shared by the live drop
    /// path and crash recovery.
    ///
    /// Deletions are WAL-logged (`delete_logged_file`) rather than raw
    /// `remove_file`: the table's heap/index pages were written to the WAL by
    /// prior DML, and crash-recovery redo would re-create the file from those
    /// page records. Appending a `FileDelete` at a higher LSN makes redo net to
    /// "deleted" (recreate-then-delete). The intent log is the *additional* belt
    /// that covers the window where a `FileDelete` record is not yet durable.
    fn purge_dropped_table_files(&self, intent: &DropTableIntent) -> bool {
        self.purge_dropped_table_files_until(intent, &|| false)
    }

    fn purge_dropped_table_files_until(&self, intent: &DropTableIntent, should_stop: &dyn Fn() -> bool) -> bool {
        if should_stop() { return false; }
        #[cfg(test)]
        if let Some(hook) = self.retirement_cleanup_test_hook.lock().unwrap().take() {
            hook.pause();
        }
        if let Some(table) = self.table_by_id(intent.table_id) {
            match self.build_drop_table_intent(&table) {
                Ok(current) if intent.files.iter().chain(&intent.vector_files).any(|path|
                    current.files.iter().chain(&current.vector_files).any(|owned| owned == path)) => return false,
                Err(_) => return false,
                _ => {},
            }
        }
        // Finish any in-flight vector publication before enumerating exact
        // index-owned roots. Removed objects cannot start another publication.
        let _vector_snapshot = if intent.vector_files.is_empty() && intent.vector_directories.is_empty() {
            None
        } else {
            match self.vector_purge_snapshot_gate.try_write() { Some(guard) => Some(guard), None => return false }
        };
        let result = (|| -> Result<(), QueryError> {
            if let Some(cache) = &intent.retired_table_cache {
                self.evict_retired_table_caches(intent.table_id, cache)?;
            }
            let mut intent = intent.clone();
            let mut vector_files = intent.vector_files.iter().cloned().collect::<BTreeSet<_>>();
            for relative in &intent.vector_directories {
                if should_stop() { return Err(error(crate::error::catalog::ER_QUERY_INTERRUPTED, "retired-file cleanup interrupted; retry pending")); }
                let directory = Path::new(relative);
                if !directory.starts_with("objects/indexes") || directory.components().any(|c| !matches!(c, std::path::Component::Normal(_))) {
                    return Err(vector::persistence::corrupt());
                }
                match fs::read_dir(self.data_dir.join(directory)) {
                    Ok(entries) => {
                        for entry in entries {
                            if should_stop() {
                                return Err(error(
                                    crate::error::catalog::ER_QUERY_INTERRUPTED,
                                    "retired-file cleanup interrupted; retry pending",
                                ));
                            }
                            let entry = entry.map_err(io_err)?;
                            if entry.file_type().map_err(io_err)?.is_file() {
                                vector_files.insert(self.relative_path(&entry.path()));
                            }
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => return Err(io_err(err)),
                }
            }
            intent.vector_files = vector_files.into_iter().collect();
            #[cfg(test)]
            if DDL_CLEANUP_FAILURE.with(|fail| fail.replace(false)) {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "injected retired-file cleanup failure",
                ));
            }
            let mut max_lsn = 0;
            for rel in &intent.vector_files {
                self.buffer_pool
                    .discard_pages_for_path(&self.data_dir.join(rel))?;
            }
            for rel in intent.files.iter().chain(&intent.vector_files) {
                if should_stop() { return Err(error(crate::error::catalog::ER_QUERY_INTERRUPTED, "retired-file cleanup interrupted; retry pending")); }
                max_lsn = max_lsn.max(self.append_wal(
                    txn_id_for_ddl(),
                    WalPayload::FileDelete {
                        path: rel.clone(),
                        before: Vec::new(),
                    },
                )?);
            }
            // Make cleanup replayable before unlinking any file.
            if max_lsn > 0
                && self.config.durability != DurabilityMode::Memory
            {
                self.sync_commit_lsn(max_lsn)?;
            }
            for rel in intent.files.iter().chain(&intent.vector_files) {
                if should_stop() { return Err(error(crate::error::catalog::ER_QUERY_INTERRUPTED, "retired-file cleanup interrupted; retry pending")); }
                match std::fs::remove_file(self.data_dir.join(rel)) {
                    Ok(()) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => return Err(io_err(err)),
                }
                #[cfg(test)]
                recyclebin_crash_point("after_file_delete");
            }
            {
                let mut handles = self.vector_retired_graphs.lock();
                for rel in &intent.vector_files {
                    handles.remove(&self.data_dir.join(rel));
                }
            }
            // Includes version tombstones left after removing AUTO_INCREMENT.
            if intent.retired_index.is_none() {
                self.delete_auto_increment_metadata(intent.table_id)?;
            }
            self.remove_empty_object_directories(intent.files.iter().chain(&intent.vector_files)
                .map(|rel| self.data_dir.join(rel)).chain(intent.vector_directories.iter().map(|rel| self.data_dir.join(rel).join("__empty_owner__"))))?;
            Ok(())
        })();
        if let Err(err) = result {
            log_message(&format!(
                "DDL cleanup pending for object {}: {}",
                intent.table_id,
                err.message()
            ));
            return false;
        }
        true
    }

    /// Startup recovery for interrupted DROP TABLE statements. Must run after
    /// the catalog is loaded and WAL redo/undo has replayed so `table_by_id`
    /// reflects the committed catalog.
    pub(crate) fn recover_drop_tables(&self) -> Result<(), QueryError> {
        let dir = drop_table_intent_dir(&self.data_dir);
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(io_err(e)),
        };
        let mut paths: Vec<PathBuf> = entries
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
            .collect();
        paths.sort();
        for path in paths {
            let bytes = match std::fs::read(&path) {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            let intent: DropTableIntent = match serde_json::from_slice(&bytes) {
                Ok(intent) => intent,
                Err(err) => {
                    // A corrupt intent must not wedge startup; the table is
                    // whatever the (authoritative) catalog says. Leave the
                    // file so an operator can inspect it, and warn.
                    log_message(&format!(
                        "drop-table recovery: skipping unparseable intent {}: {err}",
                        path.display()
                    ));
                    continue;
                }
            };
            let uncommitted = self.table_by_id(intent.table_id).is_some_and(|table| {
                intent.retired_index.is_none_or(|(index_id, generation)| {
                    table.active_generation.max(1) == generation
                        && table.indexes.iter().any(|index| index.index_id == index_id)
                })
            });
            if uncommitted {
                // The drop never committed (table still present, files intact).
                log_message(&format!(
                    "drop-table recovery: rolling back interrupted DROP of {}.{} (uncommitted)",
                    intent.database, intent.table_name
                ));
                let _ = std::fs::remove_file(&path);
                continue;
            }
            // The drop committed but crashed before unlinking every file.
            log_message(&format!(
                "drop-table recovery: completing DROP of {}.{} ({} files)",
                intent.database,
                intent.table_name,
                intent.files.len() + intent.vector_files.len()
            ));
            if self.purge_dropped_table_files(&intent) {
                self.remove_drop_table_intent(Some(&path));
            }
        }
        Ok(())
    }
}
