// Crash-safe, memory-bounded partition reorganization (§2.1 of the
// partition MySQL-8.0 compat plan).
//
// The historical repartition path drained every row of every affected
// partition into a `Vec`, deleted the old heap files, then re-inserted
// from memory in a second transaction. A crash between the delete and
// the re-insert commit lost data permanently, and a partition larger
// than RAM could not be reorganized at all.
//
// This module replaces that with **build-then-swap**:
//
//   1. Allocate *fresh* shadow storage ids for the target partitions and
//      write a reorg journal (`ddl/partition_reorg/<table_id>-<seq>.json`,
//      fsync'd) recording the source ids, the shadow ids, and the file
//      naming needed to clean either set up later.
//   2. Stream rows out of the source partitions in page-bounded batches,
//      routing each batch through a *candidate* schema (the post-reorg
//      layout, not yet in the catalog) into the shadow storage ids. Peak
//      memory is O(batch), never O(table).
//   3. Publish the new layout with a single `commit_catalog_txn()` — the
//      durable commit point. Only *after* it succeeds are the source
//      files deleted.
//
// Recovery derives commit state from the catalog itself: if the live
// partition set already points at the shadow ids the swap committed
// (roll forward — delete the source files); otherwise it did not
// (roll back — delete the shadow files). Either way the surviving files
// hold a complete, consistent copy of the data.
//
// Test builds can set `MYDB_PARTITION_REORG_STREAMING=0` to exercise
// the legacy full-memory differential oracle; production always streams.

/// Diagnostic: the largest single in-memory batch (row count) held during
/// the most recent streaming reorg. Lets tests assert the peak-memory
/// bound is O(batch), not O(table). Best-effort / single-writer; reorgs
/// hold a per-table DDL X lock so cross-table races only blur the number,
/// never correctness.
static REORG_PEAK_BATCH_ROWS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn reorg_peak_batch_rows() -> u64 {
    REORG_PEAK_BATCH_ROWS.load(std::sync::atomic::Ordering::Relaxed)
}

fn reset_reorg_peak_batch_rows() {
    REORG_PEAK_BATCH_ROWS.store(0, std::sync::atomic::Ordering::Relaxed);
}

fn note_reorg_batch_rows(rows: usize) {
    REORG_PEAK_BATCH_ROWS.fetch_max(rows as u64, std::sync::atomic::Ordering::Relaxed);
}

/// Production always uses the crash-safe path. Only test builds retain
/// the old in-memory path as a differential oracle.
pub(crate) fn partition_reorg_streaming_enabled() -> bool {
    #[cfg(not(test))]
    {
        true
    }
    #[cfg(test)]
    {
        std::env::var("MYDB_PARTITION_REORG_STREAMING")
            .map(|v| {
                !matches!(
                    v.to_ascii_lowercase().as_str(),
                    "0" | "off" | "false" | "no"
                )
            })
            .unwrap_or(true)
    }
}

/// `MYDB_PARTITION_REORG_FORCE_RECOVER` (default off): when a reorg
/// journal cannot be parsed at startup, skip it with a warning and keep
/// opening (availability over completeness). Default hard-fails so a
/// corrupt journal is surfaced rather than silently ignored. Mirrors
/// `MYDB_WAL_FORCE_RECOVER` / `MYDB_UNDO_FORCE_RECOVER`.
fn partition_reorg_force_recover() -> bool {
    std::env::var("MYDB_PARTITION_REORG_FORCE_RECOVER")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "on" | "true" | "yes"))
        .unwrap_or(false)
}

/// Source-heap pages drained per streaming batch. Bounds peak memory to
/// roughly `batch_pages * rows_per_page` rows. Overridable via
/// `MYDB_PARTITION_REORG_BATCH_PAGES` (tests set it small to exercise the
/// multi-batch path and assert the memory bound).
fn partition_reorg_batch_pages() -> u64 {
    std::env::var("MYDB_PARTITION_REORG_BATCH_PAGES")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(256)
}

/// Test-only crash-injection point (empty in production). `before_swap`
/// stops after the shadow partitions are filled but before the catalog
/// commit (recovery must roll back); `after_swap` stops after the commit
/// but before the source files are cleaned up (recovery must roll
/// forward).
fn partition_reorg_crash_point() -> Option<String> {
    std::env::var("MYDB_PARTITION_REORG_CRASH_POINT")
        .ok()
        .filter(|s| !s.is_empty())
}

fn partition_reorg_crash_at(stage: &str) -> bool {
    let point = partition_reorg_crash_point();
    #[cfg(test)]
    if point.as_deref() == Some(format!("abort_{stage}").as_str()) {
        // Subprocess recovery tests terminate without running destructors.
        std::process::exit(86);
    }
    point.as_deref() == Some(stage)
}

/// A persisted partition-DDL intent. Tagged so one recovery scan can
/// dispatch both the row-moving reorgs and the O(1) EXCHANGE swap.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "intent")]
enum PartitionDdlIntent {
    Reorg(PartitionReorgJournal),
    Exchange(PartitionExchangeJournal),
}

/// One persisted reorg intent record. Everything recovery needs to clean
/// up either the shadow or the source file set without consulting a
/// (possibly stale) catalog for path resolution.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PartitionReorgJournal {
    job_id: u64,
    /// Logical (catalog) table id — resolvable across the commit point.
    table_id: u64,
    database: String,
    table_name: String,
    /// Human tag for diagnostics (`coalesce` / `reorganize` / …).
    op: String,
    /// Index names shared by both file sets (same logical table).
    index_names: Vec<String>,
    #[serde(default)]
    generation: u64,
    #[serde(default)]
    index_ids: std::collections::BTreeMap<String, u64>,
    /// Source partition storage ids: their rows are drained, their files
    /// deleted once the swap commits.
    source_sids: Vec<u64>,
    /// Freshly-allocated shadow storage ids: built + filled before the
    /// swap, deleted if the swap never commits.
    build_sids: Vec<u64>,
    phase: PartitionDdlPhase,
    #[serde(default)]
    cancel_requested: bool,
    #[serde(default)]
    copied_rows: u64,
    #[serde(default)]
    total_rows: Option<u64>,
    #[serde(default)]
    replay_watermark: Option<u64>,
    #[serde(default)]
    error_message: Option<String>,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
enum PartitionDdlPhase {
    Intent,
    SnapshotBuild,
    CatchUp,
    Quiesce,
    Publishing,
    Swapped,
    Cleanup,
    Done,
    Failed,
}

impl PartitionDdlPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Intent => "INTENT",
            Self::SnapshotBuild => "SNAPSHOT_BUILD",
            Self::CatchUp => "CATCH_UP",
            Self::Quiesce => "QUIESCE",
            Self::Publishing => "PUBLISHING",
            Self::Swapped => "SWAPPED",
            Self::Cleanup => "CLEANUP",
            Self::Done => "DONE",
            Self::Failed => "FAILED",
        }
    }

    fn cancellable(self) -> bool {
        matches!(
            self,
            Self::Intent | Self::SnapshotBuild | Self::CatchUp | Self::Quiesce
        )
    }
}

/// One persisted EXCHANGE PARTITION intent. The catalog storage-id swap
/// is committed by `commit_catalog_txn()`; the follow-up file renames are what
/// recovery has to finish. `renames` is an ordered list of relative
/// (from, to) moves — replaying it with "skip when the source is absent"
/// completes any partial state idempotently.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PartitionExchangeJournal {
    /// Partitioned table's logical id (resolvable across the swap).
    table_id: u64,
    /// Index of the exchanged partition within that table.
    part_idx: usize,
    /// Concrete leaf for a subpartition exchange. `None` is an ordinary
    /// non-subpartitioned partition; logical parents are never exchangeable.
    #[serde(default)]
    subpart_idx: Option<usize>,
    /// Storage id that becomes the partition's after the swap (the
    /// staging table's old id). Recovery uses it to detect whether the
    /// swap committed.
    swapped_sid: u64,
    /// Standalone table's rebound (id, version, high water). Persist it before
    /// the catalog commit so recovery also preserves unused reserved values.
    #[serde(default)]
    auto_increment: Option<(u64, u64, u64)>,
    #[serde(default)]
    partition_auto_increment: Option<(u64, u64, u64)>,
    /// Ordered relative-path renames to apply post-commit.
    renames: Vec<(String, String)>,
}

const PARTITION_REORG_DIR: &str = "ddl/partition_reorg";

fn partition_reorg_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(PARTITION_REORG_DIR)
}

struct PartitionBuildMdlGuard<'a> {
    db: &'a Database,
    owner: u64,
}

impl Drop for PartitionBuildMdlGuard<'_> {
    fn drop(&mut self) {
        self.db
            .release_mdl_duration(self.owner, MdlDuration::Transaction);
    }
}

struct PartitionCleanupMdlGuard<'a> {
    db: &'a Database,
    owner: u64,
}
impl Drop for PartitionCleanupMdlGuard<'_> {
    fn drop(&mut self) {
        self.db.release_mdl_owner(self.owner);
    }
}

type PartitionReadTxnGuard<'a> = RollbackTxnGuard<'a>;

impl Database {
    /// Call with writers excluded, before publishing a replacement layout.
    /// Reuse fresh analyzed leaf counts; count missing/stale retained leaves
    /// in bounded batches rather than falling back to the table total.
    fn refresh_retained_partition_counts(
        &self,
        table: &TableSchema,
        build_ids: &[u64],
        stats: &mut std::collections::BTreeMap<u64, crate::storage::PartitionStats>,
    ) -> Result<(), QueryError> {
        if !table.is_partitioned() {
            return Ok(());
        }
        // Retained heaps still belong to the published source generation.
        // The target schema's epoch is deliberately not visible until swap.
        let source = self.table(&table.qualified_name())?;
        let txn = self.begin_read_only_txn()?;
        let _guard = PartitionReadTxnGuard {
            db: self,
            txn_id: txn.txn_id,
        };
        let modified = self.table_modification_delta(table.id) != 0
            || source.stats.modified_rows_since_analyze != 0
            || source.stats.analyzed_at_unix_ms != table.stats.analyzed_at_unix_ms;
        let built: std::collections::HashSet<_> = build_ids.iter().copied().collect();
        let batch_pages = partition_reorg_batch_pages();
        for sid in table.storage_ids() {
            if built.contains(&sid) || (!modified && stats.contains_key(&sid)) {
                continue;
            }
            let pages = self.with_heap_read(sid, |heap| heap.page_count())?;
            let mut rows = 0u64;
            let mut lo = 0;
            while lo < pages {
                crate::engine::cancel::check_cancelled()?;
                let hi = lo.saturating_add(batch_pages).min(pages);
                rows += self
                    .scan_rows_storage_range(&source, sid, txn, lo, hi)?
                    .len() as u64;
                lo = hi;
            }
            let leaf = stats
                .entry(sid)
                .or_insert_with(crate::storage::PartitionStats::empty);
            leaf.row_count = rows;
            leaf.heap_page_count = pages;
        }
        Ok(())
    }

    /// Delete every physical file belonging to one partition storage id.
    /// Idempotent (missing files are fine); other failures retain the intent.
    /// Covers the heap
    /// and all sidecars/index files a partition can own.
    ///
    /// Discards each file's buffer-pool pages **before** unlinking (S3): the
    /// paged backends (heap/lob/`.hvec`) can hold dirty frames for a
    /// removed partition, and a background writeback after the unlink would
    /// silently resurrect the deleted file. This matches the discard-then-unlink
    /// ordering enforced by `drop_table_vector_graphs` / `drop_table_recovery`.
    fn remove_partition_storage_files(
        &self,
        _database: &str,
        _table_name: &str,
        table_id: u64,
        sid: u64,
        index_names: &[String],
        journal: &PartitionReorgJournal,
    ) -> Result<(), QueryError> {
        use crate::storage::naming::{index_object_path_by_id, table_object_generation_path};
        let table = self.table_by_id(table_id);
        let generation = if journal.generation != 0 {
            journal.generation
        } else {
            table
                .as_ref()
                .map(|t| t.active_generation.max(1))
                .unwrap_or(1)
        };
        self.remove_heap_cache_entry(sid)?;
        self.remove_overflow_cache_entry(sid)?;
        self.forget_zone_map(sid)?;
        let mut paths = ["heap", "lob", "ai", "zm"]
            .into_iter()
            .map(|ext| table_object_generation_path(&self.data_dir, sid, generation, ext))
            .collect::<Vec<_>>();
        for name in index_names {
            self.remove_index_cache_entry(sid, name)?;
            let index_id = journal
                .index_ids
                .get(name)
                .copied()
                .or_else(|| {
                    table.as_ref().and_then(|table| {
                        table
                            .indexes
                            .iter()
                            .find(|index| index.name.eq_ignore_ascii_case(name))
                            .map(|index| index.index_id)
                    })
                })
                .unwrap_or_else(|| stable_missing_index_id(name));
            for ext in ["idx"] {
                paths.push(index_object_path_by_id(
                    &self.data_dir,
                    sid,
                    generation,
                    index_id,
                    ext,
                ));
            }
        }
        for name in index_names {
            let index_id = journal.index_ids.get(name).copied().or_else(|| table.as_ref().and_then(|table|
                table.indexes.iter().find(|index| index.name.eq_ignore_ascii_case(name)).map(|index| index.index_id)))
                .unwrap_or_else(|| stable_missing_index_id(name));
            paths.extend(self.owned_vector_files_parts(sid, generation, index_id)?);
        }
        if self.durability_mode() != DurabilityMode::Memory {
            let mut last = 0;
            for path in &paths { last = self.append_wal(0, WalPayload::FileDelete { path: self.relative_path(path), before: Vec::new() })?; }
            crate::wal::LogManager::sync_durable_to(&self.log, last)?;
        }
        let mut directories = std::collections::BTreeSet::new();
        for path in paths {
            self.buffer_pool.discard_pages_for_path(&path)?;
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    if let Some(parent) = path.parent() {
                        directories.insert(parent.to_path_buf());
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!(
                            "partition cleanup failed for {}: {err}; durable intent retained",
                            path.display()
                        ),
                    ))
                }
            }
        }
        if self.config.durability != DurabilityMode::Memory {
            crate::platform::fs::sync_paths(directories).map_err(io_err)?;
        }
        Ok(())
    }
}

impl Database {
    fn reclaim_partition_target_before_reuse(
        &self,
        sid: u64,
        current: &PartitionReorgJournal,
    ) -> Result<(), QueryError> {
        // REMOVE PARTITIONING reuses the logical id as its physical heap.
        // Deferred generations at that id must be evicted before FileCreate;
        // otherwise old cached pages can be mistaken for the new target.
        if self
            .catalog_read()?
            .tables
            .values()
            .any(|table| table.storage_ids().contains(&sid))
        {
            return Err(error(
                crate::error::catalog::ER_TABLE_DEF_CHANGED,
                "partition rewrite target still belongs to a live table",
            ));
        }
        self.evict_partition_vector_graphs(current.table_id, &[sid]);
        for entry in std::fs::read_dir(partition_reorg_dir(&self.data_dir)).map_err(io_err)? {
            let path = entry.map_err(io_err)?.path();
            if !path.extension().is_some_and(|ext| ext == "json") {
                continue;
            }
            let bytes = std::fs::read(&path).map_err(io_err)?;
            let intent = serde_json::from_slice::<PartitionDdlIntent>(&bytes).map_err(|err| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("partition reuse journal: {err}"),
                )
            })?;
            let PartitionDdlIntent::Reorg(mut old) = intent else {
                continue;
            };
            if old.job_id == current.job_id
                || !(old.source_sids.contains(&sid) || old.build_sids.contains(&sid))
            {
                continue;
            }
            self.remove_partition_storage_files(
                &old.database,
                &old.table_name,
                old.table_id,
                sid,
                &old.index_names,
                &old,
            )?;
            // Reuse is a new incarnation at the same physical path. Retire
            // old journal references durably before that incarnation exists,
            // so a later cleanup/drop race cannot reclaim the new heap.
            old.source_sids.retain(|id| *id != sid);
            old.build_sids.retain(|id| *id != sid);
            self.update_partition_reorg_journal(&path, &mut old)?;
        }
        self.remove_partition_storage_files(
            &current.database,
            &current.table_name,
            current.table_id,
            sid,
            &current.index_names,
            current,
        )
    }

    fn publish_added_partitions(
        &self,
        key: &str,
        table_name: &str,
        source: &Arc<TableSchema>,
        candidate: TableSchema,
        build_sids: Vec<u64>,
        cutover: Option<(u64, Duration, bool)>,
    ) -> Result<(), QueryError> {
        let mut journal = PartitionReorgJournal {
            job_id: self.allocate_nontransaction_id(),
            table_id: source.id,
            database: source.database.clone(),
            table_name: source.name.clone(),
            op: "add_partition".into(),
            index_names: source.indexes.iter().map(|i| i.name.clone()).collect(),
            generation: source.active_generation.max(1),
            index_ids: source
                .indexes
                .iter()
                .map(|i| (i.name.clone(), i.index_id))
                .collect(),
            source_sids: Vec::new(),
            build_sids: build_sids.clone(),
            phase: PartitionDdlPhase::SnapshotBuild,
            cancel_requested: false,
            copied_rows: 0,
            total_rows: Some(0),
            replay_watermark: None,
            error_message: None,
        };
        let path = self.write_partition_reorg_journal(&journal)?;
        self.register_reorg_storage_definition(&build_sids, &candidate);
        let mut publishing = false;
        let result = (|| {
            for sid in &build_sids {
                crate::engine::cancel::check_cancelled()?;
                self.create_logged_file(txn_id_for_ddl(), self.table_path(*sid), Vec::new())?;
                self.set_cached_heap_codec(*sid, candidate.row_codec.clone())?;
                if hnsw_build_on_ddl_enabled() {
                    for index in candidate.indexes.iter().filter(|i| i.is_vector()) {
                        self.vector_graph_build_on_ddl_one(&candidate, index, *sid)?;
                    }
                }
            }
            if partition_reorg_crash_at("before_swap") {
                return Ok(());
            }
            if let Some((owner, timeout, _)) = cutover {
                self.acquire_alter_execution_mdl_checked(
                    owner,
                    table_name,
                    MdlLockType::Exclusive,
                    timeout,
                    MdlDuration::Transaction,
                    || {
                        if self.partition_reorg_cancel_requested(&path, journal.job_id) {
                            Err(error(
                                crate::error::catalog::ER_QUERY_INTERRUPTED,
                                "ADD PARTITION cancelled",
                            ))
                        } else {
                            Ok(())
                        }
                    },
                )?;
            }
            {
                let _guard = self
                    .partition_ddl_journal_lock
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                if self.partition_reorg_cancel_requested(&path, journal.job_id) {
                    return Err(error(
                        crate::error::catalog::ER_QUERY_INTERRUPTED,
                        "ADD PARTITION cancelled",
                    ));
                }
                journal.phase = PartitionDdlPhase::Publishing;
                self.update_partition_reorg_journal_locked(&path, &mut journal)?;
            }
            {
                let mut catalog = self.catalog_write()?;
                let stored = catalog.tables.get_mut(key).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_NO_SUCH_TABLE,
                        format!("Table '{table_name}' doesn't exist"),
                    )
                })?;
                publishing = true;
                let stored = Arc::make_mut(stored);
                stored.partitioning = candidate.partitioning.clone();
                for sid in &build_sids {
                    // No ANALYZE baseline exists for a newly added leaf.
                    stored.stats.partition_stats.remove(sid);
                }
                stored.bump_schema_version();
            }
            self.note_catalog_table(source.id);
            self.commit_ddl_catalog_txn().map_err(|err| {
                self.quarantine_mvcc(format!(
                    "ADD PARTITION publication failed; restart recovery required: {}",
                    err.message()
                ));
                err
            })?;
            if let Some((owner, _, _)) = cutover {
                self.release_mdl_duration(owner, MdlDuration::Transaction);
            }
            journal.phase = PartitionDdlPhase::Cleanup;
            self.update_partition_reorg_journal(&path, &mut journal)?;
            if partition_reorg_crash_at("after_swap") {
                return Ok(());
            }
            self.remove_partition_reorg_journal(&path)
        })();
        if let Err(err) = &result {
            journal.error_message = Some(err.message().to_string());
            if !publishing {
                journal.phase = PartitionDdlPhase::Failed;
                let _ = self.update_partition_reorg_journal(&path, &mut journal);
                self.evict_partition_vector_graphs(source.id, &build_sids);
                for sid in &build_sids {
                    let _ = self.remove_partition_storage_files(
                        &source.database,
                        &source.name,
                        source.id,
                        *sid,
                        &journal.index_names,
                        &journal,
                    );
                }
            }
        }
        self.unregister_reorg_storage_locs(&build_sids);
        result
    }

    fn evict_partition_vector_graphs(&self, table_id: u64, storage_ids: &[u64]) {
        let ids = storage_ids
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        self.vector_indexes
            .write()
            .retain(|(tid, sid, _, _), _| *tid != table_id || !ids.contains(sid));
    }

    pub(crate) fn partition_action_online_supported(
        &self,
        table_name: &str,
        action: &crate::parser_partition::PartitionAction,
    ) -> Result<bool, QueryError> {
        use crate::parser_partition::PartitionAction as A;
        if matches!(action, A::AddPartitions(_)) {
            return Ok(true);
        }
        let moving = matches!(
            action,
            A::RebuildPartitions(_)
                | A::OptimizePartitions(_)
                | A::CoalescePartitions(_)
                | A::ReorganizePartitions { .. }
                | A::PartitionBy(_)
                | A::AddPartitionCount(_)
                | A::RemovePartitioning
        );
        Ok(moving
            && partition_reorg_streaming_enabled()
            && self
                .table(table_name)?
                .indexes
                .iter()
                .all(|index| index.index_type.eq_ignore_ascii_case("BTREE") || index.is_vector()))
    }

    pub(crate) fn partition_action_shared_build_supported(
        &self,
        action: &crate::parser_partition::PartitionAction,
    ) -> bool {
        use crate::parser_partition::PartitionAction as A;
        match action {
            A::AddPartitions(_)
            | A::RebuildPartitions(_)
            | A::OptimizePartitions(_)
            | A::TruncatePartitions(_)
            | A::AnalyzePartitions(_)
            | A::CheckPartitions(_)
            | A::RepairPartitions(_) => true,
            A::CoalescePartitions(_)
            | A::ReorganizePartitions { .. }
            | A::PartitionBy(_)
            | A::AddPartitionCount(_)
            | A::RemovePartitioning => partition_reorg_streaming_enabled(),
            _ => false,
        }
    }

    /// Register shadow storage ids → (db, name) so path resolution works
    /// while the ids are not yet in the catalog. Cleared by
    /// [`Self::unregister_reorg_storage_locs`].
    #[cfg(test)]
    fn register_reorg_storage_locs(
        &self,
        sids: &[u64],
        database: &str,
        table_name: &str,
        generation: u64,
    ) {
        if let Ok(mut map) = self.reorg_storage_locs.lock() {
            for sid in sids {
                map.insert(
                    *sid,
                    ReorgStorageLocation {
                        database: database.to_string(),
                        table_name: table_name.to_string(),
                        generation,
                        heap_codec: None,
                        index_ids: None,
                    },
                );
            }
        }
    }

    /// Publish the private definition before opening any staged storage.
    /// Cache misses must retain new index identities as well as row codecs;
    /// the public source descriptor cannot describe newly added indexes.
    fn register_reorg_storage_definition(&self, sids: &[u64], table: &TableSchema) {
        let index_ids = Arc::new(
            table
                .indexes
                .iter()
                .map(|index| (normalize_name(&index.name), index.index_id))
                .collect::<BTreeMap<_, _>>(),
        );
        if let Ok(mut map) = self.reorg_storage_locs.lock() {
            for sid in sids {
                map.insert(
                    *sid,
                    ReorgStorageLocation {
                        database: table.database.clone(),
                        table_name: table.name.clone(),
                        generation: table.active_generation.max(1),
                        heap_codec: Some(table.row_codec.clone()),
                        index_ids: Some(index_ids.clone()),
                    },
                );
            }
        }
    }

    fn unregister_reorg_storage_locs(&self, sids: &[u64]) {
        if let Ok(mut map) = self.reorg_storage_locs.lock() {
            for sid in sids {
                map.remove(sid);
            }
        }
    }

    /// Open (if needed) the heap for `table_id` and pin `codec` onto the
    /// cached instance. Used to give shadow storage ids their real row
    /// codec while they are not yet in the catalog (see
    /// [`Self::refresh_heap_codec`], which resolves the codec from the
    /// catalog and so cannot serve a not-yet-published id).
    fn set_cached_heap_codec(
        &self,
        table_id: u64,
        codec: Arc<crate::storage::core::RowCodec>,
    ) -> Result<(), QueryError> {
        self.ensure_writable()?;
        {
            let mut private = self.reorg_storage_locs.lock().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "private storage registry poisoned",
                )
            })?;
            if let Some(location) = private.get_mut(&table_id) {
                location.heap_codec = Some(codec.clone());
            }
        }
        // Keep the exact handle alive across reader setup and codec update.
        // A cache lookup after releasing the first Arc can race eviction.
        {
            let _cache = self.heap_cache.write();
            self.invalidate_storage_handle_open_epoch(table_id);
        }
        let heap = self.heap_handle(table_id)?;
        let reader = if codec_has_externalizable_columns(&codec) {
            Some(
                Arc::new(OverflowReader::new(self.overflow_handle(table_id)?))
                    as Arc<dyn LobMaterializer>,
            )
        } else {
            None
        };
        let mut heap = heap.write();
        heap.codec = codec;
        heap.lob_reader = reader;
        Ok(())
    }

    /// Write a reorg journal and fsync it (plus its parent directory) so
    /// the intent survives a crash before the file bytes reach disk.
    fn write_partition_reorg_journal(
        &self,
        journal: &PartitionReorgJournal,
    ) -> Result<PathBuf, QueryError> {
        let name = format!("reorg-{}-{}.json", journal.table_id, journal.job_id);
        self.write_partition_ddl_intent(&name, &PartitionDdlIntent::Reorg(journal.clone()))
    }

    fn update_partition_reorg_journal(
        &self,
        path: &Path,
        journal: &mut PartitionReorgJournal,
    ) -> Result<(), QueryError> {
        let _guard = self
            .partition_ddl_journal_lock
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        self.update_partition_reorg_journal_locked(path, journal)
    }

    // Caller serializes read/modify/rename with cancellation and publication.
    fn update_partition_reorg_journal_locked(
        &self,
        path: &Path,
        journal: &mut PartitionReorgJournal,
    ) -> Result<(), QueryError> {
        if self.partition_reorg_cancel_requested(path, journal.job_id) {
            journal.cancel_requested = true;
        }
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid partition DDL journal path",
                )
            })?;
        self.write_partition_ddl_intent(file_name, &PartitionDdlIntent::Reorg(journal.clone()))?;
        Ok(())
    }

    pub(crate) fn cancel_partition_ddl_job(&self, job_id: u64) -> Result<bool, QueryError> {
        let _guard = self
            .partition_ddl_journal_lock
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = partition_reorg_dir(&self.data_dir);
        let Ok(entries) = std::fs::read_dir(dir) else {
            return Ok(false);
        };
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if !path.extension().is_some_and(|e| e == "json") {
                continue;
            }
            let Ok(bytes) = std::fs::read(&path) else {
                continue;
            };
            let Ok(PartitionDdlIntent::Reorg(mut journal)) =
                serde_json::from_slice::<PartitionDdlIntent>(&bytes)
            else {
                continue;
            };
            if journal.job_id != job_id {
                continue;
            }
            if !journal.phase.cancellable() {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!(
                        "DDL job {job_id} can no longer be cancelled in state {}",
                        journal.phase.as_str()
                    ),
                ));
            }
            journal.cancel_requested = true;
            self.update_partition_reorg_journal_locked(&path, &mut journal)?;
            return Ok(true);
        }
        Ok(false)
    }

    fn partition_reorg_cancel_requested(&self, path: &Path, job_id: u64) -> bool {
        std::fs::read(path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<PartitionDdlIntent>(&bytes).ok())
            .is_some_and(|intent| {
                matches!(intent, PartitionDdlIntent::Reorg(journal)
                    if journal.job_id == job_id && journal.cancel_requested)
            })
    }

    pub fn partition_ddl_job_snapshots(&self) -> Vec<PartitionDdlJobSnapshot> {
        let Ok(entries) = std::fs::read_dir(partition_reorg_dir(&self.data_dir)) else {
            return Vec::new();
        };
        let mut jobs = entries
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().is_some_and(|e| e == "json"))
            .filter_map(|entry| std::fs::read(entry.path()).ok())
            .filter_map(|bytes| serde_json::from_slice::<PartitionDdlIntent>(&bytes).ok())
            .filter_map(|intent| match intent {
                PartitionDdlIntent::Reorg(journal) => Some(PartitionDdlJobSnapshot {
                    job_id: journal.job_id,
                    database: journal.database,
                    table: journal.table_name,
                    operation: journal.op,
                    state: if journal.error_message.is_some() {
                        "FAILED".to_string()
                    } else if journal.cancel_requested && journal.phase.cancellable() {
                        "CANCEL_REQUESTED".to_string()
                    } else {
                        journal.phase.as_str().to_string()
                    },
                    copied_rows: journal.copied_rows,
                    total_rows: journal.total_rows,
                    replay_watermark: journal.replay_watermark,
                    error_message: journal.error_message,
                    cancel_allowed: journal.phase.cancellable(),
                }),
                PartitionDdlIntent::Exchange(_) => None,
            })
            .collect::<Vec<_>>();
        jobs.sort_by_key(|job| job.job_id);
        jobs
    }

    fn write_partition_exchange_journal(
        &self,
        journal: &PartitionExchangeJournal,
    ) -> Result<PathBuf, QueryError> {
        let name = format!("exchange-{}-{}.json", journal.table_id, journal.swapped_sid);
        self.write_partition_ddl_intent(&name, &PartitionDdlIntent::Exchange(journal.clone()))
    }

    fn write_partition_ddl_intent(
        &self,
        file_name: &str,
        intent: &PartitionDdlIntent,
    ) -> Result<PathBuf, QueryError> {
        let dir = partition_reorg_dir(&self.data_dir);
        std::fs::create_dir_all(&dir).map_err(io_err)?;
        let path = dir.join(file_name);
        let bytes = serde_json::to_vec(intent).map_err(|e| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("reorg journal encode: {e}"),
            )
        })?;
        crate::util::file::write_staged_file(
            &path,
            &path.with_extension("tmp"),
            &bytes,
            self.config.durability != DurabilityMode::Memory,
        )
        .map_err(io_err)?;
        if self.config.durability != DurabilityMode::Memory {
            crate::platform::fs::sync_path(&dir).map_err(io_err)?;
        }
        Ok(path)
    }

    fn remove_partition_reorg_journal(&self, path: &Path) -> Result<(), QueryError> {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(io_err(err)),
        }
        if self.config.durability != DurabilityMode::Memory {
            if let Some(parent) = path.parent() {
                crate::platform::fs::sync_path(parent).map_err(io_err)?;
            }
        }
        Ok(())
    }

    /// Crash-safe streaming build-then-swap core shared by every
    /// row-moving reorg. `source_sids` are drained + deleted on commit;
    /// `candidate` describes the post-reorg layout (its partition storage
    /// ids are the freshly-allocated shadow ids to fill). Installing
    /// `candidate.partitioning` into the catalog + `commit_catalog_txn()` is
    /// the single durable commit point.
    fn execute_partition_reorg_streaming(
        &self,
        key: &str,
        table_name: &str,
        table: &Arc<TableSchema>,
        op: &str,
        source_sids: Vec<u64>,
        build_sids: Vec<u64>,
        candidate: TableSchema,
        cutover: Option<(u64, Duration, bool)>,
    ) -> Result<(), QueryError> {
        reset_reorg_peak_batch_rows();
        if let Some((owner, timeout, true)) = cutover {
            let job = self.register_alter_copy_job(table);
            let mut target = candidate;
            target.bump_schema_version();
            let result = self.rewrite_partitioned_type_copy(
                table.clone(),
                target,
                true,
                job,
                Some(owner),
                Some(PartitionRewriteLayout {
                    source_ids: source_sids,
                    build_ids: build_sids,
                    operation: op.to_string(),
                    timeout,
                }),
            );
            self.unregister_alter_copy_job(table.id, result.as_ref().err());
            return result.map(|_| ());
        }
        DDL_COPY_TOTAL.fetch_add(1, Ordering::Relaxed);
        let index_names: Vec<String> = table.indexes.iter().map(|i| i.name.clone()).collect();
        let database = table.database.clone();
        let owner_name = table.name.clone();

        let mut journal = PartitionReorgJournal {
            job_id: self.allocate_nontransaction_id(),
            table_id: table.id,
            database: database.clone(),
            table_name: owner_name.clone(),
            op: op.to_string(),
            index_names: index_names.clone(),
            generation: candidate.active_generation.max(1),
            index_ids: candidate
                .indexes
                .iter()
                .map(|i| (i.name.clone(), i.index_id))
                .collect(),
            source_sids: source_sids.clone(),
            build_sids: build_sids.clone(),
            phase: PartitionDdlPhase::SnapshotBuild,
            cancel_requested: false,
            copied_rows: 0,
            total_rows: None,
            replay_watermark: None,
            error_message: None,
        };

        // Make the shadow ids path-resolvable for the whole build.
        self.register_reorg_storage_definition(&build_sids, &candidate);

        let result = self.execute_partition_reorg_streaming_inner(
            key,
            table_name,
            &candidate,
            &mut journal,
            &source_sids,
            &build_sids,
            &database,
            &owner_name,
            &index_names,
            cutover,
        );

        // On error, best-effort roll back the shadow files so a failed
        // (but non-crashed) reorg leaves no orphans. The catalog was not
        // swapped, so the source data is untouched.
        if let Err(failure) = &result {
            journal.error_message = Some(failure.message().to_string());
            let pre_swap = journal.phase.cancellable();
            if pre_swap {
                journal.phase = PartitionDdlPhase::Failed;
            }
            let path = partition_reorg_dir(&self.data_dir).join(format!(
                "reorg-{}-{}.json",
                journal.table_id, journal.job_id
            ));
            let _ = self.update_partition_reorg_journal(&path, &mut journal);
            // Once the catalog points at the shadow generation, deleting it
            // here would be data loss. Post-swap failures leave both sets to
            // startup recovery, which detects the committed generation and
            // finishes source cleanup idempotently.
            if !pre_swap {
                self.unregister_reorg_storage_locs(&build_sids);
                return result;
            }
            self.evict_partition_vector_graphs(candidate.id, &build_sids);
            for sid in &build_sids {
                let _ = self.remove_heap_cache_entry(*sid);
                for name in &index_names {
                    let _ = self.remove_index_cache_entry(*sid, name);
                }
                let _ = self.remove_partition_storage_files(
                    &database,
                    &owner_name,
                    candidate.id,
                    *sid,
                    &index_names,
                    &journal,
                );
            }
        }
        self.unregister_reorg_storage_locs(&build_sids);
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_partition_reorg_streaming_inner(
        &self,
        key: &str,
        table_name: &str,
        candidate: &TableSchema,
        journal: &mut PartitionReorgJournal,
        source_sids: &[u64],
        build_sids: &[u64],
        database: &str,
        owner_name: &str,
        index_names: &[String],
        cutover: Option<(u64, Duration, bool)>,
    ) -> Result<(), QueryError> {
        // C1: suppress row-event recording for the entire shadow-partition
        // build. Every batch below opens its own txn via `begin_txn` +
        // `insert_rows_bulk_detailed_for`, which reaches
        // `record_write_rows(table)` unconditionally. Without this guard,
        // REORGANIZE / COALESCE / REPARTITION / REMOVE PARTITIONING /
        // AddPartitionCount would emit N spurious WRITE_ROWS events under
        // the logical table id — replicas would then apply them onto rows
        // that already exist (from the source-side history) and hit 1062,
        // stopping the SQL thread. The higher-layer ALTER TABLE Query event
        // is written outside this scope and still reaches downstream, so
        // MySQL replicas replay the DDL and internally rebuild the same
        // partitioning shape.
        let _suppress_binlog = crate::binlog::SessionSqlLogBinGuard::new(false);
        // 1) Persist the intent before any file is created.
        let journal_path = self.write_partition_reorg_journal(journal)?;

        // 2) Create the (empty) shadow heap files. Index files are created
        //    lazily by the insert path. The shadow ids are not yet in the
        //    catalog, so a freshly-opened heap would resolve an empty row
        //    codec (via `table_by_id`) and decode every row as empty —
        //    pin the candidate's codec onto the cached heap up front so
        //    both writes and the scans that follow use the right shape.
        for sid in build_sids {
            if *sid == candidate.id {
                self.reclaim_partition_target_before_reuse(*sid, journal)?;
            }
            self.create_logged_file(txn_id_for_ddl(), self.table_path(*sid), Vec::new())?;
            self.remove_heap_cache_entry(*sid)?;
            self.set_cached_heap_codec(*sid, candidate.row_codec.clone())?;
        }
        // The durable initial intent already covers the complete build.

        // 3) Stream rows from the source partitions into the shadow ids
        //    via the candidate router, in page-bounded batches.
        // The rows already satisfy the source constraints. Accumulate only
        // heap/LOB data; local indexes are sorted and bulk-built once per leaf.
        let mut heap_candidate = candidate.clone();
        heap_candidate.indexes.clear();
        heap_candidate.build_column_index();
        let candidate_arc = Arc::new(heap_candidate);
        let read_txn = self.begin_read_only_txn()?;
        let read_guard = PartitionReadTxnGuard {
            db: self,
            txn_id: read_txn.txn_id,
        };
        let build_set = build_sids
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        let batch_pages = partition_reorg_batch_pages();
        let mut shadow_row_counts = std::collections::BTreeMap::<u64, u64>::new();
        let copy_sources = if journal.op == "truncate_partition" {
            &[][..]
        } else {
            source_sids
        };
        let mut last_progress_flush = Instant::now();
        let scan_timer = OnlineIndexTimer(&DDL_COPY_SCAN_US, Instant::now());
        for sid in copy_sources {
            let page_count = self.with_heap_mut(*sid, |heap| heap.page_count())?;
            let mut lo = 0u64;
            while lo < page_count {
                crate::engine::cancel::check_cancelled()?;
                if self.partition_reorg_cancel_requested(&journal_path, journal.job_id) {
                    return Err(error(
                        crate::error::catalog::ER_QUERY_INTERRUPTED,
                        format!("DDL job {} was cancelled", journal.job_id),
                    ));
                }
                let hi = (lo + batch_pages).min(page_count);
                let mut raw: Vec<MvccStoredRow> = Vec::new();
                self.with_heap_mut(*sid, |heap| {
                    heap.scan_mvcc_pages_range_until(lo, hi, |row| {
                        raw.push(row);
                        Ok(false)
                    })
                })?;
                let mut batch_charge =
                    crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                        crate::engine::executor::mem_budget::ControlledMemoryCategory::PartitionDdl,
                        raw.capacity()
                            .saturating_mul(std::mem::size_of::<MvccStoredRow>()),
                    )?;
                let mut batch: Vec<Vec<Value>> = Vec::with_capacity(raw.len());
                for row in raw {
                    if let Some(mut visible) = self.visible_row(row, read_txn)? {
                        Self::pad_full_row_to_schema(&mut visible.values, candidate);
                        batch.push(visible.values);
                    }
                }
                let batch_bytes = batch
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Vec<Value>>())
                    .saturating_add(
                        batch
                            .iter()
                            .map(|row| {
                                row.capacity()
                                    .saturating_mul(std::mem::size_of::<Value>())
                                    .saturating_add(
                                        row.iter()
                                            .map(|value| match value {
                                                Value::Decimal(value) | Value::String(value) => {
                                                    value.capacity()
                                                }
                                                Value::Bytes(value)
                                                | Value::Json(value)
                                                | Value::Geometry(value) => value.capacity(),
                                                Value::Vector(value) => value.capacity(),
                                                _ => 0,
                                            })
                                            .sum::<usize>(),
                                    )
                            })
                            .sum::<usize>(),
                    );
                batch_charge.try_grow(batch_bytes)?;
                note_reorg_batch_rows(batch.len());
                journal.copied_rows = journal.copied_rows.saturating_add(batch.len() as u64);
                if !batch.is_empty() {
                    for values in &batch {
                        let storage_id = if candidate.is_partitioned() {
                            candidate.route_row_to_partition(values)?.storage_id
                        } else {
                            candidate.id
                        };
                        if !build_set.contains(&storage_id) {
                            return Err(error(
                                crate::error::catalog::ER_REORG_OUTSIDE_RANGE,
                                "partition rewrite would write into a retained source leaf",
                            ));
                        }
                        let count = shadow_row_counts.entry(storage_id).or_default();
                        *count = count.saturating_add(1);
                    }
                    let txn = self.begin_txn()?;
                    if let Err(err) =
                        self.insert_rows_shadow_bulk_detailed_for(txn, candidate_arc.clone(), batch)
                    {
                        let _ = self.rollback_txn(txn.txn_id);
                        return Err(err);
                    }
                    self.commit_txn(txn.txn_id)?;
                }
                lo = hi;
                // Intent already contains every physical id needed for recovery.
                // Progress is advisory: avoid a durability barrier for each tiny
                // leaf while still refreshing long single-leaf builds promptly.
                if last_progress_flush.elapsed() >= Duration::from_secs(1) {
                    self.update_partition_reorg_journal(&journal_path, journal)?;
                    last_progress_flush = Instant::now();
                }
            }
        }

        drop(scan_timer);
        drop(read_guard);
        let index_read = self.begin_read_only_txn()?;
        let index_guard = PartitionReadTxnGuard {
            db: self,
            txn_id: index_read.txn_id,
        };
        self.build_partition_copy_indexes(candidate, build_sids, index_read, || {
            crate::engine::cancel::check_cancelled()?;
            if self.partition_reorg_cancel_requested(&journal_path, journal.job_id) {
                Err(error(
                    crate::error::catalog::ER_QUERY_INTERRUPTED,
                    format!("DDL job {} was cancelled", journal.job_id),
                ))
            } else {
                Ok(())
            }
        })?;
        drop(index_guard);
        // Writes are excluded by SQL's SharedNoWrite/Exclusive MDL. There
        // is no incremental replay stream, so do not fabricate a watermark.
        journal.phase = PartitionDdlPhase::CatchUp;
        journal.replay_watermark = None;

        // Build only private target graphs while readers still use the old
        // leaves. Retained leaves must not be rebuilt or evicted. A vector
        // build error remains pre-publication and can discard the target.
        if hnsw_build_on_ddl_enabled() {
            for index in candidate.indexes.iter().filter(|index| index.is_vector()) {
                for storage_id in build_sids {
                    crate::engine::cancel::check_cancelled()?;
                    self.vector_graph_build_on_ddl_one(candidate, index, *storage_id)?;
                }
            }
        }

        #[cfg(test)]
        self.pause_ddl_backfill_for_test();

        // Test-only crash window: shadow filled, catalog not yet swapped.
        if partition_reorg_crash_at("before_swap") {
            self.update_partition_reorg_journal(&journal_path, journal)?;
            return Ok(());
        }

        // 4) Commit point: publish the new layout atomically + fsync.
        if self.partition_reorg_cancel_requested(&journal_path, journal.job_id) {
            return Err(error(
                crate::error::catalog::ER_QUERY_INTERRUPTED,
                format!("DDL job {} was cancelled", journal.job_id),
            ));
        }
        journal.phase = PartitionDdlPhase::Quiesce;
        self.update_partition_reorg_journal(&journal_path, journal)?;
        if journal.cancel_requested {
            return Err(error(
                crate::error::catalog::ER_QUERY_INTERRUPTED,
                format!("DDL job {} was cancelled", journal.job_id),
            ));
        }
        // A targeted physical rewrite replaces only a subset of leaves.
        // Start from the candidate's persisted leaf statistics, remove the
        // source generations, then install statistics for the new physical
        // generations. Full repartitioning naturally removes every old leaf;
        // REBUILD PARTITION leaves unrelated partitions untouched.
        let mut shadow_stats = candidate.stats.partition_stats.clone();
        for storage_id in source_sids {
            shadow_stats.remove(storage_id);
        }
        for (build_ordinal, storage_id) in build_sids.iter().enumerate() {
            let heap_page_count = self.with_heap_read(*storage_id, |heap| heap.page_count())?;
            if candidate.is_partitioned() {
                // A physical rebuild preserves value distributions. Rebind
                // the existing sketches to the new storage generation and
                // refresh the exact row/page fields; other reorgs cannot
                // safely associate old and new leaves, so they start with
                // empty sketches until ANALYZE.
                let mut leaf_stats = if journal.op == "rebuild_partition" {
                    source_sids
                        .get(build_ordinal)
                        .and_then(|source_id| candidate.stats.partition_stats.get(source_id))
                        .cloned()
                        .unwrap_or_else(crate::storage::PartitionStats::empty)
                } else {
                    crate::storage::PartitionStats::empty()
                };
                leaf_stats.row_count = shadow_row_counts.get(storage_id).copied().unwrap_or(0);
                leaf_stats.heap_page_count = heap_page_count;
                leaf_stats.modified_rows_since_analyze = 0;
                leaf_stats.check_ok = None;
                shadow_stats.insert(*storage_id, leaf_stats);
            }
        }
        self.refresh_retained_partition_counts(candidate, build_sids, &mut shadow_stats)?;
        let (table_row_count, table_page_count) = if candidate.is_partitioned() {
            let current_ids = candidate.storage_ids();
            (
                current_ids
                    .iter()
                    .filter_map(|storage_id| shadow_stats.get(storage_id))
                    .map(|stats| stats.row_count)
                    .sum(),
                current_ids
                    .iter()
                    .filter_map(|storage_id| shadow_stats.get(storage_id))
                    .map(|stats| stats.heap_page_count)
                    .sum(),
            )
        } else {
            (
                shadow_row_counts.values().copied().sum(),
                build_sids
                    .iter()
                    .map(|storage_id| self.with_heap_read(*storage_id, |heap| heap.page_count()))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .sum(),
            )
        };
        let wait_started = Instant::now();
        if let Some((owner, timeout, _)) = cutover {
            self.acquire_alter_execution_mdl_checked(
                owner,
                table_name,
                MdlLockType::Exclusive,
                timeout,
                MdlDuration::Transaction,
                || {
                    if self.partition_reorg_cancel_requested(&journal_path, journal.job_id) {
                        Err(error(
                            crate::error::catalog::ER_QUERY_INTERRUPTED,
                            format!("DDL job {} was cancelled", journal.job_id),
                        ))
                    } else {
                        Ok(())
                    }
                },
            )?;
        }
        DDL_COPY_BARRIER_WAIT_US
            .fetch_add(wait_started.elapsed().as_micros() as u64, Ordering::Relaxed);
        let hold_timer = cutover.map(|_| OnlineCopyHoldTimer(Instant::now()));
        crate::engine::cancel::check_cancelled()?;
        {
            // Atomically close cancellation before publication. A CANCEL
            // acknowledged before this gate must abort; one after it must
            // report that the job can no longer be cancelled.
            let _guard = self
                .partition_ddl_journal_lock
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if self.partition_reorg_cancel_requested(&journal_path, journal.job_id) {
                return Err(error(
                    crate::error::catalog::ER_QUERY_INTERRUPTED,
                    format!("DDL job {} was cancelled", journal.job_id),
                ));
            }
            journal.phase = PartitionDdlPhase::Publishing;
            self.update_partition_reorg_journal_locked(&journal_path, journal)?;
        }
        self.evict_partition_vector_graphs(candidate.id, source_sids);
        {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog.tables.get_mut(key).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    format!("Table '{table_name}' doesn't exist"),
                )
            })?;
            let table_mut = Arc::make_mut(table_arc);
            table_mut.partitioning = candidate.partitioning.clone();
            table_mut.stats.partition_stats = shadow_stats;
            table_mut.stats.row_count = table_row_count;
            table_mut.stats.heap_page_count = table_page_count;
            table_mut.bump_schema_version();
        }
        self.note_catalog_table(candidate.id);
        // Publication errors can be ambiguous. Retain both generations for
        // recovery once the in-memory catalog references the target.
        journal.phase = PartitionDdlPhase::Swapped;
        self.commit_ddl_catalog_txn().map_err(|err| {
            self.quarantine_mvcc(format!(
                "partition DDL catalog publication failed; restart recovery required: {}",
                err.message()
            ));
            err
        })?;
        // The published totals include all committed DML up to the cutover.
        // Clear the old sidecar before releasing the writer barrier, otherwise
        // metadata adds those inserts/deletes a second time.
        self.drop_table_modification_delta(candidate.id);
        // SQL still holds statement SU, excluding concurrent DDL. The X
        // barrier has drained every old reader/writer and the new layout is
        // durable, so source-file cleanup can coexist with new-schema DML.
        // Explicit LOCK=EXCLUSIVE also holds statement X and remains exclusive.
        if let Some((owner, _, _)) = cutover {
            self.release_mdl_duration(owner, MdlDuration::Transaction);
        }
        drop(hold_timer);
        #[cfg(test)]
        self.pause_ddl_backfill_for_test();
        // Publishing is already non-cancellable; persist the committed
        // cleanup state once instead of fsyncing adjacent advisory phases.
        journal.phase = PartitionDdlPhase::Cleanup;
        self.update_partition_reorg_journal(&journal_path, journal)?;

        // Test-only crash window: catalog swapped, source not yet cleaned.
        if partition_reorg_crash_at("after_swap") {
            return Ok(());
        }

        // 5) Post-commit: drop the source files (idempotent cleanup).
        if self.partition_cleanup_background.load(Ordering::Acquire) {
            return Ok(());
        }
        for sid in source_sids {
            self.remove_heap_cache_entry(*sid)?;
            for name in index_names {
                let _ = self.remove_index_cache_entry(*sid, name);
            }
            self.remove_partition_storage_files(
                database,
                owner_name,
                candidate.id,
                *sid,
                index_names,
                journal,
            )?;
        }

        // 6) Intent fulfilled — drop the journal last.
        self.remove_partition_reorg_journal(&journal_path)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn partition_cleanup_background_for_tests(&self) -> bool {
        self.partition_cleanup_background.load(Ordering::Acquire)
    }

    pub(crate) fn set_partition_cleanup_background(&self, enabled: bool) {
        self.partition_cleanup_background
            .store(enabled, Ordering::Release);
    }

    pub(crate) fn maintain_partition_cleanup_until(
        &self,
        stop: impl Fn() -> bool,
    ) -> Result<usize, QueryError> {
        self.ensure_operational()?;
        let dir = partition_reorg_dir(&self.data_dir);
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(err) => return Err(io_err(err)),
        };
        let mut completed = 0;
        for entry in entries {
            if stop() || completed == 8 {
                break;
            }
            let path = entry.map_err(io_err)?.path();
            if !path.extension().is_some_and(|ext| ext == "json") {
                continue;
            }
            let bytes = std::fs::read(&path).map_err(io_err)?;
            let intent = serde_json::from_slice::<PartitionDdlIntent>(&bytes).map_err(|err| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("partition cleanup journal: {err}"),
                )
            })?;
            let PartitionDdlIntent::Reorg(journal) = intent else {
                continue;
            };
            if !matches!(
                journal.phase,
                PartitionDdlPhase::Cleanup | PartitionDdlPhase::Done | PartitionDdlPhase::Failed
            ) {
                continue;
            }
            if self.active_alter_copy_job(journal.table_id).is_some() {
                continue;
            }
            let current = self.table_by_id(journal.table_id);
            let owner = crate::storage::mdl::allocate_internal_owner()?;
            let _guard = PartitionCleanupMdlGuard { db: self, owner };
            if let Some(table) = current.as_ref() {
                if let Err(err) = self.acquire_mdl(
                    owner,
                    None,
                    MdlDuration::Statement,
                    vec![MdlRequest {
                        key: MdlObjectKey::table(&table.database, &table.name),
                        lock_type: MdlLockType::SharedUpgradable,
                    }],
                    "ddl/partition/cleanup",
                ) {
                    if is_lock_wait_pending_error(&err) {
                        continue;
                    }
                    return Err(err);
                }
                // A rename may have committed between lookup and admission.
                if self
                    .table_by_id(journal.table_id)
                    .is_none_or(|now| now.database != table.database || now.name != table.name)
                {
                    continue;
                }
            }
            self.recover_reorg_journal_until(&path, journal, &stop)?;
            completed += 1;
        }
        Ok(completed)
    }

    /// Startup recovery: reconcile every surviving reorg journal. Called
    /// after the catalog is loaded and WAL redo/undo have run, so the
    /// catalog reflects whether each swap's `commit_catalog_txn()` committed.
    pub(crate) fn recover_partition_reorgs(&self) -> Result<(), QueryError> {
        let dir = partition_reorg_dir(&self.data_dir);
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(io_err(err)),
        };
        let mut paths: Vec<PathBuf> = Vec::new();
        for entry in entries {
            let path = entry.map_err(io_err)?.path();
            if path.extension().map(|e| e == "json").unwrap_or(false) {
                paths.push(path);
            }
        }
        paths.sort();
        for path in paths {
            self.recover_one_partition_reorg(&path)?;
        }
        Ok(())
    }

    fn recover_one_partition_reorg(&self, path: &Path) -> Result<(), QueryError> {
        let bytes = std::fs::read(path).map_err(io_err)?;
        let intent: PartitionDdlIntent = match serde_json::from_slice(&bytes) {
            Ok(j) => j,
            Err(e) => {
                if partition_reorg_force_recover() {
                    log_message(format_args!(
                        "partition reorg recovery: skipping unreadable journal {} ({e}) \
                         under MYDB_PARTITION_REORG_FORCE_RECOVER",
                        path.display()
                    ));
                    let _ = std::fs::remove_file(path);
                    return Ok(());
                }
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "corrupt partition reorg journal {} ({e}); set \
                         MYDB_PARTITION_REORG_FORCE_RECOVER=1 to discard and continue",
                        path.display()
                    ),
                ));
            }
        };
        match intent {
            PartitionDdlIntent::Reorg(journal) => self.recover_reorg_journal(path, journal),
            PartitionDdlIntent::Exchange(journal) => self.recover_exchange_journal(path, journal),
        }
    }

    /// Complete a partial EXCHANGE. The storage-id swap is committed iff
    /// the partition now points at `swapped_sid`; only then are the file
    /// renames finished (replaying them idempotently). Otherwise the swap
    /// never committed and the files are untouched — just drop the journal.
    fn recover_exchange_journal(
        &self,
        path: &Path,
        journal: PartitionExchangeJournal,
    ) -> Result<(), QueryError> {
        let committed = self
            .table_by_id(journal.table_id)
            .and_then(|t| {
                t.partitioning.as_ref().and_then(|p| {
                    p.partitions.get(journal.part_idx).and_then(|part| {
                        journal
                            .subpart_idx
                            .map(|idx| part.subpartitions.get(idx).map(|sub| sub.storage_id))
                            .unwrap_or(Some(part.storage_id))
                    })
                })
            })
            .map(|sid| sid == journal.swapped_sid)
            .unwrap_or(false);
        if committed {
            if let Some((table_id, version, max_value)) = journal.partition_auto_increment {
                self.apply_exchange_auto_increment(table_id, version, max_value)?;
            }
            if let Some((table_id, version, max_value)) = journal.auto_increment {
                self.apply_exchange_auto_increment(table_id, version, max_value)?;
                self.auto_increment.forget_table(journal.swapped_sid);
                self.delete_auto_increment_metadata(journal.swapped_sid)?;
            }
            for (from, to) in &journal.renames {
                let from_abs = self.data_dir.join(from);
                let to_abs = self.data_dir.join(to);
                if from_abs.exists() {
                    if let Some(parent) = to_abs.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    let _ = std::fs::rename(&from_abs, &to_abs);
                }
            }
            self.forget_zone_map(journal.swapped_sid)?;
            if let Some(table) = self.table_by_id(journal.table_id) {
                if let Some(partitioning) = table.partitioning.as_ref() {
                    if let Some(partition) = partitioning.partitions.get(journal.part_idx) {
                        let storage_id = journal
                            .subpart_idx
                            .and_then(|idx| partition.subpartitions.get(idx))
                            .map(|sub| sub.storage_id)
                            .unwrap_or(partition.storage_id);
                        self.forget_zone_map(storage_id)?;
                    }
                }
            }
        }
        log_message(format_args!(
            "partition reorg recovery: exchange table {} part#{} — {}",
            journal.table_id,
            journal.part_idx,
            if committed {
                "committed, completed pending file renames"
            } else {
                "uncommitted, no file moves performed"
            }
        ));
        self.remove_partition_reorg_journal(path)
    }

    fn recover_reorg_journal(
        &self,
        path: &Path,
        journal: PartitionReorgJournal,
    ) -> Result<(), QueryError> {
        self.recover_reorg_journal_until(path, journal, || false)
    }

    fn recover_reorg_journal_until(
        &self,
        path: &Path,
        journal: PartitionReorgJournal,
        stop: impl Fn() -> bool,
    ) -> Result<(), QueryError> {
        // Derive commit state from the live catalog: if the shadow ids are
        // the current partition set the swap committed (roll forward),
        // otherwise it did not (roll back).
        let current_leaf_sids: Vec<u64> = match self.table_by_id(journal.table_id) {
            Some(table) => match table.partitioning.as_ref() {
                Some(part) => part.storage_ids(),
                None => vec![table.id],
            },
            // Table dropped after the journal was written — both sets are
            // orphans; delete them and move on.
            None => Vec::new(),
        };
        let committed = if journal.op == "drop_partition" {
            journal
                .source_sids
                .iter()
                .all(|sid| !current_leaf_sids.contains(sid))
        } else {
            !journal.build_sids.is_empty()
                && journal
                    .build_sids
                    .iter()
                    .all(|sid| current_leaf_sids.contains(sid))
        };

        let table_dropped = self.table_by_id(journal.table_id).is_none();
        // Committed cleanup never owns its public target files. Later DDL
        // (possibly on a table that acquired them through EXCHANGE) owns
        // their lifetime. At startup, an uncertain publication can safely
        // reconcile both sets against the recovered catalog.
        let mut victims = match journal.phase {
            PartitionDdlPhase::Cleanup | PartitionDdlPhase::Swapped | PartitionDdlPhase::Done => {
                journal.source_sids.clone()
            }
            PartitionDdlPhase::Failed => journal.build_sids.clone(),
            _ => journal
                .source_sids
                .iter()
                .chain(&journal.build_sids)
                .copied()
                .collect(),
        };
        victims.sort_unstable();
        victims.dedup();
        let live_ids = self
            .catalog_read()?
            .tables
            .values()
            .flat_map(|table| {
                table
                    .storage_ids()
                    .into_iter()
                    .map(|id| (id, table.active_generation.max(1)))
                    .collect::<Vec<_>>()
            })
            .collect::<std::collections::BTreeSet<_>>();
        for sid in victims {
            // Another DDL may already have replaced the target or moved it
            // through EXCHANGE before deferred cleanup runs. Never infer
            // liveness solely from this journal's original logical table.
            if stop() {
                return Err(error(
                    crate::error::catalog::ER_QUERY_INTERRUPTED,
                    "partition cleanup interrupted; durable intent retained",
                ));
            }
            let live = if journal.generation == 0 {
                live_ids.range((sid, 0)..=(sid, u64::MAX)).next().is_some()
            } else {
                live_ids.contains(&(sid, journal.generation))
            };
            if live {
                continue;
            }
            self.evict_partition_vector_graphs(journal.table_id, &[sid]);
            self.remove_partition_storage_files(
                &journal.database,
                &journal.table_name,
                journal.table_id,
                sid,
                &journal.index_names,
                &journal,
            )?;
        }
        log_message(format_args!(
            "partition reorg recovery: {} '{}' ({}) — {}",
            journal.op,
            journal.table_name,
            journal.table_id,
            if table_dropped {
                "table dropped, cleaned both sets"
            } else if committed {
                "committed, rolled forward (dropped source)"
            } else {
                "uncommitted, rolled back (dropped shadow)"
            }
        ));
        self.remove_partition_reorg_journal(path)
    }
}
