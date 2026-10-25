// Crash-safe DROP TABLE: intent log + startup recovery.
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

fn drop_table_intent_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(DROP_TABLE_INTENT_DIR)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct DropTableIntent {
    table_id: u64,
    /// An index retirement shares the table-drop file protocol, but its
    /// commit decision is the absence of this stable index/generation pair.
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
}

impl Database {
    fn retirement_selector(intent: &DropTableIntent) -> CatalogRowSelector {
        let (index_id, generation) = intent
            .retired_index
            .expect("catalog retirement names an index");
        CatalogRowSelector::PrimaryValues(vec![
            Value::UInt(intent.table_id),
            Value::UInt(generation),
            Value::UInt(index_id),
        ])
    }

    fn retirement_is_live(&self, intent: &DropTableIntent) -> bool {
        self.table_by_id(intent.table_id).is_some_and(|table| {
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
        let (index_id, generation) = intent.retired_index.ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "catalog retirement has no index identity",
            )
        })?;
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
                if !self.retirement_is_live(&old)
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
        Ok(txn)
    }

    pub(crate) fn maintain_catalog_retirements_until(
        &self,
        stop: impl Fn() -> bool,
    ) -> Result<usize, QueryError> {
        use crate::storage::data_dictionary::DD_DDL_RETIREMENTS;
        let _role = self.zone_map_gate.read_recursive();
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
        let mut txn = CatalogTxn::new(self);
        for (key, row_id) in &entries {
            if stop() {
                break;
            }
            let Some(row) = self.read_row_at(&table, DD_DDL_RETIREMENTS, *row_id, snapshot.txn)?
            else {
                continue;
            };
            let primary = Self::dd_primary_index(&table)?;
            if table.index_key(primary, &row.values)? != *key {
                continue;
            }
            let intent = Self::decode_retirement(&row.values)?;
            if self.retirement_is_live(&intent) {
                continue;
            }
            let cleaned = if self.physical_replica_mode() {
                intent
                    .files
                    .iter()
                    .chain(&intent.vector_files)
                    .try_for_each(|path| {
                        self.buffer_pool
                            .discard_pages_for_path(&self.data_dir.join(path))?;
                        match fs::remove_file(self.data_dir.join(path)) {
                            Ok(()) => Ok(()),
                            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                            Err(err) => Err(io_err(err)),
                        }
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
        *cursor = entries.last().cloned();
        if !txn.replacements.is_empty() {
            txn.commit()?;
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
    fn build_drop_table_intent(&self, table: &TableSchema) -> DropTableIntent {
        let mut files = Vec::new();
        let mut vector_files = Vec::new();
        for sid in table.storage_ids() {
            files.push(self.relative_path(&self.table_path(sid)));
            files.push(self.relative_path(&self.overflow_path(sid)));
            files.push(self.relative_path(&self.zone_map_file_path(sid)));
            for index in &table.indexes {
                files.push(self.relative_path(&self.index_path(sid, &index.name)));
                if index.is_vector() {
                    vector_files.push(self.relative_path(&self.vector_index_path(
                        table,
                        &index.name,
                        sid,
                    )));
                    vector_files.push(self.relative_path(&self.vector_index_paged_path(
                        table,
                        &index.name,
                        sid,
                    )));
                }
            }
        }
        DropTableIntent {
            table_id: table.id,
            retired_index: None,
            had_auto_increment: table.auto_increment_column_index().is_some(),
            database: table.database.clone(),
            table_name: table.name.clone(),
            files,
            vector_files,
        }
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
        // fsync the directory so the new entry is durable before the commit.
        crate::platform::fs::sync_path(&dir).map_err(io_err)?;
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
        for sid in table.storage_ids() {
            self.remove_heap_cache_entry(sid)?;
            self.remove_overflow_cache_entry(sid)?;
            self.forget_zone_map(sid)?;
            for index in &table.indexes {
                self.remove_index_cache_entry(sid, &index.name)?;
            }
        }
        // In-memory HNSW/IVF graphs (files are removed by the purge below).
        self.vector_indexes
            .write()
            .retain(|(tid, _sid, _generation, _name), _| *tid != table.id);
        self.auto_increment.forget_table(table.id);
        Ok(())
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
        let result = (|| -> Result<(), QueryError> {
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
                && intent.retired_index.is_none()
                && self.config.durability != DurabilityMode::Memory
            {
                self.sync_commit_lsn(max_lsn)?;
            }
            for rel in intent.files.iter().chain(&intent.vector_files) {
                match std::fs::remove_file(self.data_dir.join(rel)) {
                    Ok(()) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => return Err(io_err(err)),
                }
            }
            if intent.had_auto_increment {
                self.delete_auto_increment_metadata(intent.table_id)?;
            }
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
