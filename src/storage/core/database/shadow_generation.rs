// Shared private-generation builder used by bulk operations that can publish
// a complete heap/index set atomically. The generation remains unreachable
// while rows and B-trees are built, so neither heap nor index page construction
// needs per-row WAL or undo. Publication is protected by a versioned intent.

static LOAD_SHADOW_HITS: AtomicU64 = AtomicU64::new(0);
static LOAD_SHADOW_FALLBACKS: AtomicU64 = AtomicU64::new(0);
static LOAD_SHADOW_SOURCE_BYTES: AtomicU64 = AtomicU64::new(0);
static LOAD_SHADOW_HEAP_BYTES: AtomicU64 = AtomicU64::new(0);
static LOAD_SHADOW_INDEX_BYTES: AtomicU64 = AtomicU64::new(0);
static LOAD_SHADOW_HEAP_WRITE_US: AtomicU64 = AtomicU64::new(0);
static LOAD_SHADOW_INDEX_FEED_US: AtomicU64 = AtomicU64::new(0);
static LOAD_SHADOW_INDEX_FINISH_US: AtomicU64 = AtomicU64::new(0);
static LOAD_SHADOW_SYNC_SWAP_US: AtomicU64 = AtomicU64::new(0);
static LOAD_SHADOW_PARSE_ENCODE_US: AtomicU64 = AtomicU64::new(0);
static LOAD_DIRECT_DECODE_ROWS: AtomicU64 = AtomicU64::new(0);
static LOAD_DIRECT_DECODE_FALLBACK_ROWS: AtomicU64 = AtomicU64::new(0);
static LOAD_AUTO_INCREMENT_FAST_BATCHES: AtomicU64 = AtomicU64::new(0);
static LOAD_AUTO_INCREMENT_FAST_ROWS: AtomicU64 = AtomicU64::new(0);
static LOAD_AUTO_INCREMENT_ASSIGN_BATCHES: AtomicU64 = AtomicU64::new(0);
static LOAD_AUTO_INCREMENT_ASSIGN_ROWS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BulkLoadIntent {
    version: u32,
    txn_id: u64,
    table_id: u64,
    shadow_id: u64,
    stage: BulkLoadIntentStage,
    old_to_backup: Vec<(String, String)>,
    shadow_to_live: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
enum BulkLoadIntentStage {
    Prepared,
    Swapped,
    Committed,
}

fn bulk_load_intent_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("bulk-load")
}

fn bulk_load_intent_paths(data_dir: &Path) -> Result<Vec<PathBuf>, QueryError> {
    let entries = match fs::read_dir(bulk_load_intent_dir(data_dir)) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(io_err(err)),
    };
    let mut paths = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "intent"))
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

/// Builder for one unreachable heap/index generation. Memory for the index
/// sorters is owned by `LoadDataIndexBuilders` and charged to StorageScratch;
/// append scratch is separately charged by capacity for every batch.
pub(crate) struct ShadowGenerationBuilder {
    db: Arc<Database>,
    live_table: Arc<TableSchema>,
    shadow_table: TableSchema,
    txn_id: u64,
    build_lsn: u64,
    total_rows: u64,
    index_builders: Option<LoadDataIndexBuilders>,
    published: bool,
}

#[derive(Clone)]
pub(crate) struct ShadowHeapAppender {
    db: Arc<Database>,
    shadow_id: u64,
    build_lsn: u64,
    order: Arc<(Mutex<ShadowAppendOrder>, Condvar)>,
}

struct ShadowAppendOrder {
    next_batch: usize,
    cancelled: bool,
}

pub(crate) struct PublishedShadowGeneration {
    db: Arc<Database>,
    live_table: Arc<TableSchema>,
    shadow_id: u64,
    total_rows: u64,
    intent_path: PathBuf,
    intent: BulkLoadIntent,
    swap: Option<ShadowGenerationFileSwap>,
    commit_decided: bool,
    resolved: bool,
}

/// Replay file moves after the operation-specific durable commit decision.
/// ALTER COPY also removes old live paths resurrected by earlier WAL records;
/// bulk load uses the same live paths for its new generation and must keep them.
fn recover_generation_files(
    data_dir: &Path,
    old_to_backup: &[(String, String)],
    shadow_to_live: &[(String, String)],
    committed: bool,
    remove_old_live: bool,
    invalid_path_error: &str,
) -> Result<(), QueryError> {
    let resolve = |relative: &str| -> Result<PathBuf, QueryError> {
        let relative = Path::new(relative);
        if relative.is_absolute()
            || relative
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir))
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                invalid_path_error,
            ));
        }
        Ok(data_dir.join(relative))
    };
    if committed {
        for (shadow, live) in shadow_to_live {
            let shadow = resolve(shadow)?;
            let live = resolve(live)?;
            if !live.exists() && shadow.exists() {
                fs::rename(shadow, live).map_err(io_err)?;
            }
        }
        for (old_live, backup) in old_to_backup {
            if remove_old_live {
                let _ = fs::remove_file(resolve(old_live)?);
            }
            let _ = fs::remove_file(resolve(backup)?);
        }
    } else {
        for (live, backup) in old_to_backup.iter().rev() {
            let live = resolve(live)?;
            let backup = resolve(backup)?;
            if backup.exists() {
                let _ = fs::remove_file(&live);
                fs::rename(backup, live).map_err(io_err)?;
            }
        }
        for (shadow, live_relative) in shadow_to_live {
            let shadow = resolve(shadow)?;
            let live = resolve(live_relative)?;
            let had_old_generation = old_to_backup
                .iter()
                .any(|(old_live, _)| old_live == live_relative);
            let _ = fs::remove_file(shadow);
            if !had_old_generation {
                let _ = fs::remove_file(live);
            }
        }
    }
    Ok(())
}

impl Database {
    pub(crate) fn record_load_shadow_fallback(&self) {
        LOAD_SHADOW_FALLBACKS.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn load_shadow_perf_snapshot(&self) -> [u64; 16] {
        [
            LOAD_SHADOW_HITS.load(Ordering::Relaxed),
            LOAD_SHADOW_FALLBACKS.load(Ordering::Relaxed),
            LOAD_SHADOW_SOURCE_BYTES.load(Ordering::Relaxed),
            LOAD_SHADOW_HEAP_BYTES.load(Ordering::Relaxed),
            LOAD_SHADOW_INDEX_BYTES.load(Ordering::Relaxed),
            LOAD_SHADOW_HEAP_WRITE_US.load(Ordering::Relaxed),
            LOAD_SHADOW_INDEX_FEED_US.load(Ordering::Relaxed),
            LOAD_SHADOW_INDEX_FINISH_US.load(Ordering::Relaxed),
            LOAD_SHADOW_SYNC_SWAP_US.load(Ordering::Relaxed),
            LOAD_SHADOW_PARSE_ENCODE_US.load(Ordering::Relaxed),
            LOAD_DIRECT_DECODE_ROWS.load(Ordering::Relaxed),
            LOAD_DIRECT_DECODE_FALLBACK_ROWS.load(Ordering::Relaxed),
            LOAD_AUTO_INCREMENT_FAST_BATCHES.load(Ordering::Relaxed),
            LOAD_AUTO_INCREMENT_FAST_ROWS.load(Ordering::Relaxed),
            LOAD_AUTO_INCREMENT_ASSIGN_BATCHES.load(Ordering::Relaxed),
            LOAD_AUTO_INCREMENT_ASSIGN_ROWS.load(Ordering::Relaxed),
        ]
    }

    pub(crate) fn record_load_shadow_parse_encode_us(&self, elapsed_us: u64) {
        LOAD_SHADOW_PARSE_ENCODE_US.fetch_add(elapsed_us, Ordering::Relaxed);
    }

    pub(crate) fn record_load_direct_decode_rows(&self, direct: u64, fallback: u64) {
        LOAD_DIRECT_DECODE_ROWS.fetch_add(direct, Ordering::Relaxed);
        LOAD_DIRECT_DECODE_FALLBACK_ROWS.fetch_add(fallback, Ordering::Relaxed);
    }

    pub(crate) fn record_load_auto_increment_batch(&self, rows: u64, assigned: bool) {
        if assigned {
            LOAD_AUTO_INCREMENT_ASSIGN_BATCHES.fetch_add(1, Ordering::Relaxed);
            LOAD_AUTO_INCREMENT_ASSIGN_ROWS.fetch_add(rows, Ordering::Relaxed);
        } else {
            LOAD_AUTO_INCREMENT_FAST_BATCHES.fetch_add(1, Ordering::Relaxed);
            LOAD_AUTO_INCREMENT_FAST_ROWS.fetch_add(rows, Ordering::Relaxed);
        }
    }

    pub(crate) fn table_is_physically_empty_for_shadow_load(
        &self,
        table: &TableSchema,
    ) -> Result<bool, QueryError> {
        if self.heap_page_count(table.id) != 0 {
            return Ok(false);
        }
        for index in &table.indexes {
            if !self.with_index_read(table.id, &index.name, |file| file.is_empty())? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn begin_shadow_generation(
        self: &Arc<Self>,
        txn_id: u64,
        table: Arc<TableSchema>,
    ) -> Result<ShadowGenerationBuilder, QueryError> {
        self.acquire_table_exclusive(txn_id, table.id)?;
        if !self.table_is_physically_empty_for_shadow_load(&table)? {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "shadow generation requires a physically empty table",
            ));
        }

        let shadow_id = {
            let mut catalog = self.catalog_write()?;
            let id = catalog.next_table_id;
            catalog.next_table_id = catalog.next_table_id.saturating_add(1);
            id
        };
        self.ensure_table_id_highwater(self.current_next_table_id()?)?;
        let mut shadow_table = table.as_ref().clone();
        shadow_table.id = shadow_id;
        // LOAD DATA is DML, not a schema change. Its private storage id keeps
        // the build unreachable, while publication replaces the contents of
        // the table's current physical generation. Advancing the generation
        // here would require an otherwise spurious DD mutation and leave the
        // public descriptor pointing at the empty predecessor generation.
        shadow_table.active_generation = table.active_generation.max(1);
        self.register_reorg_storage_definition(&[shadow_id], &shadow_table);
        let heap_path = self.table_path(shadow_id);
        if let Some(parent) = heap_path.parent() {
            fs::create_dir_all(parent).map_err(io_err)?;
        }
        let _ = fs::remove_file(&heap_path);
        if let Err(err) = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&heap_path)
            .map_err(io_err)
        {
            self.unregister_reorg_storage_locs(&[shadow_id]);
            return Err(err);
        }
        self.remove_heap_cache_entry(shadow_id)?;
        self.set_cached_heap_codec(shadow_id, table.row_codec.clone())?;
        self.with_heap_read(shadow_id, |heap| {
            heap.set_shadow_direct_nocache(true);
            Ok(())
        })?;
        let index_builders = match self.shadow_load_data_index_builders(&shadow_table) {
            Ok(builders) => builders,
            Err(err) => {
                let _ = self.remove_heap_cache_entry(shadow_id);
                let _ = fs::remove_file(heap_path);
                self.unregister_reorg_storage_locs(&[shadow_id]);
                return Err(err);
            }
        };
        Ok(ShadowGenerationBuilder {
            db: Arc::clone(self),
            live_table: table,
            shadow_table,
            txn_id,
            build_lsn: self.log.lock().last_lsn(),
            total_rows: 0,
            index_builders: Some(index_builders),
            published: false,
        })
    }

    fn persist_bulk_load_intent(
        &self,
        path: &Path,
        intent: &BulkLoadIntent,
    ) -> Result<(), QueryError> {
        let parent = path.parent().ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "bulk-load intent has no parent directory",
            )
        })?;
        fs::create_dir_all(parent).map_err(io_err)?;
        let tmp = path.with_extension("intent.tmp");
        let bytes = serde_json::to_vec(intent).map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("bulk-load intent encode failed: {err}"),
            )
        })?;
        crate::util::file::write_staged_file(
            path,
            &tmp,
            &bytes,
            self.config.durability != DurabilityMode::Memory,
        )
        .map_err(io_err)?;
        Ok(())
    }

    fn recover_bulk_load_intents(&self) -> Result<(), QueryError> {
        for path in bulk_load_intent_paths(&self.data_dir)? {
            let bytes = fs::read(&path).map_err(io_err)?;
            let intent: BulkLoadIntent = serde_json::from_slice(&bytes).map_err(|err| {
                error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    format!("invalid bulk-load intent {}: {err}", path.display()),
                )
            })?;
            if intent.version != 1 {
                return Err(error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    format!("unsupported bulk-load intent version {}", intent.version),
                ));
            }
            // Terminal slots may already have been GC'd from the transaction
            // manager before the intent is resolved. The MVCC slot cache is
            // the durable/recovery-populated terminal-status oracle in that
            // case; consulting only `txns.slot()` could incorrectly roll a
            // committed generation back after a crash.
            let committed = matches!(intent.stage, BulkLoadIntentStage::Committed)
                || self
                    .slot_status_cache
                    .lookup(intent.txn_id)
                    .is_some_and(|(status, _)| status == TxnStatus::Committed)
                || self
                    .txns_read()?
                    .slot(intent.txn_id)
                    .is_some_and(|slot| slot.status == TxnStatus::Committed);
            recover_generation_files(
                &self.data_dir,
                &intent.old_to_backup,
                &intent.shadow_to_live,
                committed,
                false,
                "invalid path in bulk-load intent",
            )?;
            self.finish_shadow_generation_intent(&path)?;
        }
        Ok(())
    }
}

impl ShadowGenerationBuilder {
    pub(crate) fn heap_appender(&self) -> ShadowHeapAppender {
        ShadowHeapAppender {
            db: Arc::clone(&self.db),
            shadow_id: self.shadow_table.id,
            build_lsn: self.build_lsn,
            order: Arc::new((
                Mutex::new(ShadowAppendOrder {
                    next_batch: 0,
                    cancelled: false,
                }),
                Condvar::new(),
            )),
        }
    }

    pub(crate) fn feed_prepared_batch(
        &mut self,
        index_keys: Vec<LoadDataIndexKeyBatch>,
        row_ids: &[RowId],
    ) -> Result<(), QueryError> {
        let feed_started = Instant::now();
        self.index_builders
            .as_mut()
            .expect("shadow index builders present before publish")
            .push_batch(&self.db, self.txn_id, index_keys, row_ids)?;
        LOAD_SHADOW_INDEX_FEED_US.fetch_add(
            feed_started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        self.total_rows = self.total_rows.saturating_add(row_ids.len() as u64);
        Ok(())
    }

    pub(crate) fn finish_and_publish(mut self) -> Result<PublishedShadowGeneration, QueryError> {
        let finish_started = Instant::now();
        self.index_builders
            .take()
            .expect("shadow index builders finish once")
            .finish(&self.db, self.txn_id)?;
        LOAD_SHADOW_INDEX_FINISH_US.fetch_add(
            finish_started
                .elapsed()
                .as_micros()
                .min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );

        let sync_started = Instant::now();
        self.db.with_heap_read(self.shadow_table.id, |heap| {
            heap.set_shadow_direct_nocache(false);
            heap.sync_shadow_direct()
        })?;
        for index in &self.shadow_table.indexes {
            let path = self.db.index_path_for_table_schema(
                &self.shadow_table,
                self.shadow_table.id,
                &index.name,
            );
            if self.db.config.durability != DurabilityMode::Memory {
                OpenOptions::new()
                    .read(true)
                    .open(path)
                    .and_then(|file| file.sync_all())
                    .map_err(io_err)?;
            }
        }
        let (old_to_backup, shadow_to_live) = self.db.shadow_generation_swap_paths(
            &self.live_table,
            &self.shadow_table,
            self.shadow_table.id,
            self.txn_id,
        );
        let relative_pairs = |pairs: &[(PathBuf, PathBuf)]| {
            pairs
                .iter()
                .map(|(from, to)| (self.db.relative_path(from), self.db.relative_path(to)))
                .collect::<Vec<_>>()
        };
        let intent_path = bulk_load_intent_dir(self.db.data_dir())
            .join(format!("{}-{}.intent", self.live_table.id, self.txn_id));
        let mut intent = BulkLoadIntent {
            version: 1,
            txn_id: self.txn_id,
            table_id: self.live_table.id,
            shadow_id: self.shadow_table.id,
            stage: BulkLoadIntentStage::Prepared,
            old_to_backup: relative_pairs(&old_to_backup),
            shadow_to_live: relative_pairs(&shadow_to_live),
        };
        self.db.persist_bulk_load_intent(&intent_path, &intent)?;
        let swap = self.db.swap_shadow_generation_files(
            &self.live_table,
            &self.shadow_table,
            self.shadow_table.id,
            self.txn_id,
        )?;
        intent.stage = BulkLoadIntentStage::Swapped;
        if let Err(err) = self.db.persist_bulk_load_intent(&intent_path, &intent) {
            let _ = self.db.rollback_shadow_generation_file_swap(&swap);
            return Err(err);
        }
        LOAD_SHADOW_SYNC_SWAP_US.fetch_add(
            sync_started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        LOAD_SHADOW_HEAP_BYTES.fetch_add(
            fs::metadata(self.db.table_path(self.live_table.id))
                .map(|meta| meta.len())
                .unwrap_or(0),
            Ordering::Relaxed,
        );
        LOAD_SHADOW_INDEX_BYTES.fetch_add(
            self.live_table
                .indexes
                .iter()
                .map(|index| {
                    fs::metadata(self.db.index_path_for_table_schema(
                        &self.live_table,
                        self.live_table.id,
                        &index.name,
                    ))
                    .map(|meta| meta.len())
                    .unwrap_or(0)
                })
                .sum(),
            Ordering::Relaxed,
        );
        self.db
            .record_table_modifications_by_id(self.live_table.id, self.total_rows);
        self.db.record_pending_live_row_delta(
            self.txn_id,
            self.live_table.id,
            i64::try_from(self.total_rows).unwrap_or(i64::MAX),
        );
        self.db.dml_stats.record_inserts(self.total_rows);
        self.published = true;
        Ok(PublishedShadowGeneration {
            db: Arc::clone(&self.db),
            live_table: Arc::clone(&self.live_table),
            shadow_id: self.shadow_table.id,
            total_rows: self.total_rows,
            intent_path,
            intent,
            swap: Some(swap),
            commit_decided: false,
            resolved: false,
        })
    }
}

impl ShadowHeapAppender {
    pub(crate) fn append_batch(
        &self,
        batch_idx: usize,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Vec<RowId>, QueryError> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let refs = payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let scratch = refs
            .capacity()
            .saturating_mul(std::mem::size_of::<&[u8]>())
            .saturating_add(HeapFile::shadow_payload_batch_direct_scratch_capacity(
                &refs,
            ));
        let _charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
            scratch,
        )?;
        let started = Instant::now();
        let mut prepared = HeapFile::prepare_shadow_payload_batch_direct(self.build_lsn, &refs)?;
        drop(refs);
        let first_page_id = {
            let (order, ready) = &*self.order;
            let mut state = order.lock().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "shadow append order mutex poisoned",
                )
            })?;
            while !state.cancelled && state.next_batch != batch_idx {
                state = ready.wait(state).map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "shadow append order mutex poisoned",
                    )
                })?;
            }
            if state.cancelled {
                return Err(error(
                    crate::error::catalog::ER_QUERY_INTERRUPTED,
                    "shadow append cancelled",
                ));
            }
            let first_page_id = self.db.with_heap_read(self.shadow_id, |heap| {
                heap.reserve_prepared_shadow_batch_direct(&mut prepared)
            })?;
            state.next_batch = state.next_batch.saturating_add(1);
            ready.notify_all();
            first_page_id
        };
        let row_ids = self.db.with_heap_read(self.shadow_id, |heap| {
            heap.write_reserved_shadow_batch_direct(prepared, first_page_id)
        })?;
        LOAD_SHADOW_HEAP_WRITE_US.fetch_add(
            started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        LOAD_SHADOW_SOURCE_BYTES.fetch_add(
            payloads.iter().map(|payload| payload.len() as u64).sum(),
            Ordering::Relaxed,
        );
        Ok(row_ids)
    }

    pub(crate) fn cancel(&self) {
        let (order, ready) = &*self.order;
        if let Ok(mut state) = order.lock() {
            state.cancelled = true;
            ready.notify_all();
        }
    }
}

impl Drop for ShadowGenerationBuilder {
    fn drop(&mut self) {
        if self.published {
            return;
        }
        let _ = self.db.remove_heap_cache_entry(self.shadow_table.id);
        for index in &self.shadow_table.indexes {
            let _ = self
                .db
                .remove_index_cache_entry(self.shadow_table.id, &index.name);
            let _ = fs::remove_file(self.db.index_path_for_table_schema(
                &self.shadow_table,
                self.shadow_table.id,
                &index.name,
            ));
        }
        let _ = fs::remove_file(self.db.table_path(self.shadow_table.id));
        self.db
            .unregister_reorg_storage_locs(&[self.shadow_table.id]);
    }
}

impl PublishedShadowGeneration {
    pub(crate) fn total_rows(&self) -> u64 {
        self.total_rows
    }

    /// Persist the post-commit terminal state before deleting the intent.
    /// This closes the checkpoint/slot-GC race in the same way as CTAS's
    /// `load_committed` marker: startup can roll forward even when the
    /// transaction's terminal slot is no longer retained.
    pub(crate) fn mark_committed(&mut self) -> Result<(), QueryError> {
        // Set this before the fallible marker write: once the transaction
        // commit succeeded, Drop must never restore the old generation.
        self.commit_decided = true;
        self.intent.stage = BulkLoadIntentStage::Committed;
        self.db
            .persist_bulk_load_intent(&self.intent_path, &self.intent)
    }

    pub(crate) fn finalize_committed(mut self) -> Result<(), QueryError> {
        self.resolved = true;
        if let Some(swap) = self.swap.take() {
            self.db
                .finish_shadow_generation_file_swap(&self.live_table, swap)?;
        }
        self.db.unregister_reorg_storage_locs(&[self.shadow_id]);
        self.db.finish_shadow_generation_intent(&self.intent_path)?;
        LOAD_SHADOW_HITS.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn rollback_uncommitted(mut self) {
        self.resolved = true;
        if let Some(swap) = self.swap.take() {
            if self.db.rollback_shadow_generation_file_swap(&swap).is_err() {
                self.db.unregister_reorg_storage_locs(&[self.shadow_id]);
                return;
            }
        }
        let _ = self.db.remove_heap_cache_entry(self.shadow_id);
        for index in &self.live_table.indexes {
            let _ = self
                .db
                .remove_index_cache_entry(self.shadow_id, &index.name);
            let _ = fs::remove_file(self.db.index_path_for_table_schema(
                &self.live_table,
                self.shadow_id,
                &index.name,
            ));
        }
        let _ = fs::remove_file(self.db.table_path(self.shadow_id));
        self.db.unregister_reorg_storage_locs(&[self.shadow_id]);
        let _ = self.db.finish_shadow_generation_intent(&self.intent_path);
    }
}

impl Drop for PublishedShadowGeneration {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        if self.commit_decided {
            // The committed live generation remains authoritative. Keep the
            // intent/backups for startup recovery if normal finalization was
            // interrupted after the commit decision.
            self.db.unregister_reorg_storage_locs(&[self.shadow_id]);
            return;
        }
        if let Some(swap) = self.swap.take() {
            if self.db.rollback_shadow_generation_file_swap(&swap).is_err() {
                self.db.unregister_reorg_storage_locs(&[self.shadow_id]);
                return;
            }
        }
        self.db.unregister_reorg_storage_locs(&[self.shadow_id]);
        let _ = self.db.finish_shadow_generation_intent(&self.intent_path);
    }
}

#[cfg(test)]
mod shadow_generation_tests {
    use super::*;

    fn relative(path: &str) -> String {
        path.to_string()
    }

    #[test]
    fn bulk_load_recovery_restores_old_generation_for_uncommitted_txn() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let live = dir.path().join("recovery-live.heap");
        let backup = dir.path().join("recovery-live.heap.backup");
        let shadow = dir.path().join("recovery-shadow.heap");
        fs::write(&live, b"new-generation").unwrap();
        fs::write(&backup, b"old-generation").unwrap();
        let intent_path = bulk_load_intent_dir(dir.path()).join("rollback.intent");
        let intent = BulkLoadIntent {
            version: 1,
            txn_id: u64::MAX - 1,
            table_id: 7,
            shadow_id: 8,
            stage: BulkLoadIntentStage::Swapped,
            old_to_backup: vec![(
                relative("recovery-live.heap"),
                relative("recovery-live.heap.backup"),
            )],
            shadow_to_live: vec![(
                relative("recovery-shadow.heap"),
                relative("recovery-live.heap"),
            )],
        };
        db.persist_bulk_load_intent(&intent_path, &intent).unwrap();

        db.recover_bulk_load_intents().unwrap();

        assert_eq!(fs::read(live).unwrap(), b"old-generation");
        assert!(!backup.exists());
        assert!(!shadow.exists());
        assert!(!intent_path.exists());
    }

    #[test]
    fn bulk_load_recovery_finishes_new_generation_for_committed_txn() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let txn = db.begin_dml_txn().unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        let live = dir.path().join("recovery-live.heap");
        let backup = dir.path().join("recovery-live.heap.backup");
        let shadow = dir.path().join("recovery-shadow.heap");
        fs::write(&backup, b"old-generation").unwrap();
        fs::write(&shadow, b"new-generation").unwrap();
        let intent_path = bulk_load_intent_dir(dir.path()).join("roll-forward.intent");
        let intent = BulkLoadIntent {
            version: 1,
            txn_id: txn.txn_id,
            table_id: 7,
            shadow_id: 8,
            stage: BulkLoadIntentStage::Committed,
            old_to_backup: vec![(
                relative("recovery-live.heap"),
                relative("recovery-live.heap.backup"),
            )],
            shadow_to_live: vec![(
                relative("recovery-shadow.heap"),
                relative("recovery-live.heap"),
            )],
        };
        db.persist_bulk_load_intent(&intent_path, &intent).unwrap();

        db.recover_bulk_load_intents().unwrap();

        assert_eq!(fs::read(live).unwrap(), b"new-generation");
        assert!(!backup.exists());
        assert!(!shadow.exists());
        assert!(!intent_path.exists());
    }
}
