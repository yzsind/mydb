// Crash-safe TRUNCATE TABLE: intent log + startup recovery.
//
// TRUNCATE TABLE must move a table's data (heap, LOB overflow, indexes) from
// "old rows" to "empty" atomically. The old implementation appended raw WAL
// `FileCreate`/`FileDelete` records with no barrier and no intent, so a crash
// mid-way could leave any subset of files half-truncated (e.g. LOB deleted
// while the heap still referenced its chains — a data-corruption window).
//
// This module makes TRUNCATE crash-safe with a *commit-first* protocol backed
// by a durable intent log, mirroring `drop_table_recovery` / `partition_reorg`:
//
//   1. Record a `TruncateTableIntent` listing every storage id + index name to
//      wipe, fsync it. Also snapshots the DDL SQL + binlog context so recovery
//      can re-emit the binlog QUERY event if the crash landed after the
//      physical wipe but before `record_ddl_autocommit_with` fired.
//   2. Physical wipe (idempotent; recovery redoes on crash):
//        heap-empty FIRST → LOB cleanup → index files.
//      "heap-empty first" is the correctness ordering: emptying the heap
//      invalidates every `LobRef`, so a crash between the LOB unlink and any
//      heap update never leaves the heap referencing dead overflow chains.
//   3. Reset AUTO_INCREMENT dynamic metadata, drop derived vector graphs, reset stats
//      (and bump schema_version so plan cache re-costs against 0 rows).
//   4. `commit_catalog_txn` — the single atomic commit point for the stats reset.
//   5. Remove the intent.
//
// Recovery (`recover_truncate_tables`, run after the catalog is loaded and WAL
// redo/undo has replayed) drives every lingering intent to a consistent state:
//   - table absent from catalog  → drop the intent (the whole DDL was rolled
//                                  back by a concurrent DROP TABLE).
//   - table present              → the primary crashed mid-truncate. Re-run
//                                  the physical wipe idempotently and emit the
//                                  binlog event if the intent still asked for
//                                  one. Then drop the intent.
//
// The intent lives at `ddl/truncate/<table_id>-<lsn>.json`; the LSN suffix
// keeps back-to-back TRUNCATEs on the same table from clobbering each other's
// pending intent (recovery finishes them in filename order).

const TRUNCATE_TABLE_INTENT_DIR: &str = "ddl/truncate";

fn truncate_table_intent_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(TRUNCATE_TABLE_INTENT_DIR)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct TruncateTableIntent {
    pub(crate) table_id: u64,
    pub(crate) database: String,
    pub(crate) table_name: String,
    /// Every physical storage id (single id for unpartitioned tables; one id
    /// per leaf partition for partitioned tables). Order matches
    /// `TableSchema::storage_ids()` so recovery walks the same set.
    pub(crate) storage_ids: Vec<u64>,
    /// Non-vector index names to wipe per storage id (`.idx` files). Vector
    /// indexes are handled by `vector_index_names` below.
    pub(crate) index_names: Vec<String>,
    /// Vector index names to wipe per storage id (`.hnsw` / `.hvec` / `.ivf`).
    /// Split out because they don't have `.idx` files and their paged frames
    /// must be evicted from the buffer pool before the unlink.
    pub(crate) vector_index_names: Vec<String>,
    /// True when the table has an AUTO_INCREMENT column — recovery must reset
    /// its sidecar even if the primary crashed after wiping the data files.
    pub(crate) had_auto_increment: bool,
}

impl Database {
    #[cfg(test)]
    pub(crate) fn build_truncate_table_intent(&self, table: &TableSchema) -> TruncateTableIntent {
        let mut index_names = Vec::new();
        let mut vector_index_names = Vec::new();
        for index in &table.indexes {
            if index.is_vector() {
                vector_index_names.push(index.name.clone());
            } else {
                index_names.push(index.name.clone());
            }
        }
        TruncateTableIntent {
            table_id: table.id,
            database: table.database.clone(),
            table_name: table.name.clone(),
            storage_ids: table.storage_ids(),
            index_names,
            vector_index_names,
            had_auto_increment: table.auto_increment_column_index().is_some(),
        }
    }

    /// Persist a truncate intent (fsync'd). Returns `None` in memory-durability
    /// mode (nothing on disk to recover, so no intent is needed).
    #[cfg(test)]
    pub(crate) fn write_truncate_table_intent(
        &self,
        intent: &TruncateTableIntent,
    ) -> Result<Option<PathBuf>, QueryError> {
        if self.config.durability == DurabilityMode::Memory {
            return Ok(None);
        }
        let dir = truncate_table_intent_dir(&self.data_dir);
        std::fs::create_dir_all(&dir).map_err(io_err)?;
        let lsn = self.log.lock().next_lsn();
        let path = dir.join(format!("{}-{:016x}.json", intent.table_id, lsn));
        let bytes = serde_json::to_vec(intent).map_err(|e| {
            error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                format!("serialize truncate-table intent: {e}"),
            )
        })?;
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&path).map_err(io_err)?;
            file.write_all(&bytes).map_err(io_err)?;
            file.sync_all().map_err(io_err)?;
        }
        crate::platform::fs::sync_path(&dir).map_err(io_err)?;
        Ok(Some(path))
    }

    /// Perform the physical wipe described by `intent`. Idempotent — safe to
    /// call from both the live TRUNCATE path and startup recovery.
    ///
    /// **Ordering matters** (B1 fix): the empty heap must be written *before*
    /// the LOB is unlinked. Otherwise a crash between the LOB `FileDelete`
    /// (durable) and the heap `FileCreate` (not yet durable) leaves the heap
    /// referencing overflow chains whose backing file was already reclaimed on
    /// recovery — reads return decoded garbage or 1030. This mirrors the
    /// `truncate_partitions` fix already documented in `ddl.rs`.
    /// Startup recovery for interrupted TRUNCATE TABLE statements. Must run
    /// after the catalog is loaded and WAL redo/undo has replayed so
    /// `table_by_id` reflects the committed catalog.
    pub(crate) fn recover_truncate_tables(&self) -> Result<(), QueryError> {
        let dir = truncate_table_intent_dir(&self.data_dir);
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(io_err(e)),
        };
        let mut paths: Vec<PathBuf> = entries
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
            .collect();
        // Filename encodes `<table_id>-<lsn>` so lexical sort replays in LSN
        // order — back-to-back TRUNCATEs on the same table finish in order.
        paths.sort();
        for path in paths {
            let bytes = match std::fs::read(&path) {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            let intent: TruncateTableIntent = match serde_json::from_slice(&bytes) {
                Ok(intent) => intent,
                Err(err) => {
                    log_message(&format!(
                        "truncate-table recovery: skipping unparseable intent {}: {err}",
                        path.display()
                    ));
                    continue;
                }
            };
            // Table gone means a subsequent DROP TABLE ate it after the
            // TRUNCATE intent was written but before recovery ran — the drop
            // recovery already made those files absent, so this intent is
            // moot.
            if self.table_by_id(intent.table_id).is_none() {
                log_message(&format!(
                    "truncate-table recovery: dropping stale intent for {}.{} (table no longer present)",
                    intent.database, intent.table_name
                ));
                let _ = std::fs::remove_file(&path);
                continue;
            }
            log_message(&format!(
                "truncate-table recovery: replaying wipe for {}.{} ({} storage ids, {} indexes)",
                intent.database,
                intent.table_name,
                intent.storage_ids.len(),
                intent.index_names.len() + intent.vector_index_names.len()
            ));
            // Physical wipe is idempotent — safe to redo even if the primary
            // completed some or all of it before crashing.
            if let Err(err) = self.apply_truncate_table_wipe(&intent) {
                log_message(&format!(
                    "truncate-table recovery: wipe for {}.{} failed: {}",
                    intent.database,
                    intent.table_name,
                    err.message()
                ));
                continue;
            }
            // Vector graph files are derived; drop the cached graphs + files
            // so the next query lazily rebuilds them empty from the (now
            // empty) heap.
            if !intent.vector_index_names.is_empty() {
                if let Some(table) = self.table_by_id(intent.table_id) {
                    self.drop_table_vector_graphs(&table);
                }
            }
            // Publish a new AUTO_INCREMENT version so pre-TRUNCATE redo can
            // never be merged into the reset counter.
            if intent.had_auto_increment {
                let _ = self.reset_auto_increment(intent.table_id);
            }
            let _ = std::fs::remove_file(&path);
        }
        Ok(())
    }

    pub(crate) fn apply_truncate_table_wipe(
        &self,
        intent: &TruncateTableIntent,
    ) -> Result<(), QueryError> {
        for sid in &intent.storage_ids {
            // Drop live caches first so a stale frame can't resurrect a file
            // we're about to replace.
            self.remove_heap_cache_entry(*sid)?;
            // 1. Empty heap — overwrites in place, WAL FileCreate. Doing this
            //    FIRST invalidates every LobRef in the old rows, so the LOB
            //    cleanup below cannot leave dangling references.
            self.create_logged_file(txn_id_for_ddl(), self.table_path(*sid), Vec::new())?;
            // 2. LOB overflow (`.lob`) — WAL FileDelete + fs unlink. Missing
            //    file is tolerated (LOB-less table).
            let _ = self.cleanup_overflow_file(txn_id_for_ddl(), *sid);
            // 3. Non-vector indexes (`.idx`) — WAL FileDelete + fs unlink.
            //    We do NOT re-create empty index files: `IndexFile::open_with_
            //    durability` create-if-missing lazily builds a fresh empty
            //    tree on the next DML, and the replica converges through the
            //    same path. This also spares TRUNCATE the per-index empty-
            //    heap rescan the old `rebuild_indexes` call incurred.
            for index_name in &intent.index_names {
                self.remove_index_cache_entry(*sid, index_name)?;
                let _ =
                    self.delete_logged_file(txn_id_for_ddl(), self.index_path(*sid, index_name));
            }
        }
        Ok(())
    }
}
