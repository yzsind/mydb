static ONLINE_INDEX_PREPARE_US: AtomicU64 = AtomicU64::new(0);
static ONLINE_INDEX_REPLAY_US: AtomicU64 = AtomicU64::new(0);
static ONLINE_INDEX_VALIDATION_US: AtomicU64 = AtomicU64::new(0);
static ONLINE_INDEX_PUBLISH_US: AtomicU64 = AtomicU64::new(0);
static ONLINE_INDEX_BARRIER_WAIT_US: AtomicU64 = AtomicU64::new(0);
static ONLINE_INDEX_TAIL_ENTRIES_PEAK: AtomicU64 = AtomicU64::new(0);
static ONLINE_INDEX_TAIL_BYTES_PEAK: AtomicU64 = AtomicU64::new(0);
static ONLINE_INDEX_LOG_BYTES_PEAK: AtomicU64 = AtomicU64::new(0);

struct OnlineIndexTimer<'a>(&'a AtomicU64, Instant);
impl Drop for OnlineIndexTimer<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(
            self.1.elapsed().as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }
}

// Disjoint from the ordinary COPY job sequence; catalog version compatibility
// gates the interpretation of this existing DdlCommit payload.
const PRIMARY_PUBLICATION_JOB_TAG: u64 = 1 << 63;

/// The log contains physical key changes, including undo compensation. A
/// prepare barrier drains old writers before installation; a publish barrier
/// drains new writers before the final replay. Thus an unordered heap scan
/// followed by this ordered, idempotent log produces the final committed tree.
/// It is deliberately not durable: recovery discards BUILDING indexes.
#[derive(Debug)]
struct OnlineIndexBuildLog {
    state: parking_lot::Mutex<OnlineIndexBuildLogState>,
}

#[derive(Debug)]
struct OnlineIndexBuildLogState {
    entries: Vec<IndexBatchEntry>,
    charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    failed: bool,
    limit_bytes: usize,
}

impl OnlineIndexBuildLog {
    fn new() -> Self {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        Self {
            state: parking_lot::Mutex::new(OnlineIndexBuildLogState {
                entries: Vec::new(),
                charge: ControlledMemoryCharge::try_new(ControlledMemoryCategory::DdlOnlineLog, 0)
                    .expect("zero byte charge"),
                failed: false,
                limit_bytes: 64 * 1024 * 1024,
            }),
        }
    }

    fn record(&self, entries: impl IntoIterator<Item = IndexBatchEntry>) {
        let mut state = self.state.lock();
        if state.failed {
            return;
        }
        for entry in entries {
            let payload = entry
                .key
                .0
                .iter()
                .map(key_part_payload_capacity)
                .sum::<usize>()
                + if entry.key.0.spilled() {
                    entry.key.0.capacity() * std::mem::size_of::<KeyPart>()
                } else {
                    0
                };
            let growth = if state.entries.len() == state.entries.capacity() {
                state.entries.capacity().max(64) * std::mem::size_of::<IndexBatchEntry>()
            } else {
                0
            };
            let bytes = payload.saturating_add(growth);
            // Cancel the candidate, never a business write, on log exhaustion.
            if state.charge.bytes().saturating_add(bytes) > state.limit_bytes
                || !state.charge.try_grow_optional(bytes)
            {
                state.failed = true;
                return;
            }
            if growth != 0 {
                let extra = state.entries.capacity().max(64);
                if state.entries.try_reserve_exact(extra).is_err() {
                    state.failed = true;
                    return;
                }
            }
            state.entries.push(entry);
        }
    }

    fn take(&self) -> Result<OnlineIndexBuildLogState, QueryError> {
        let mut state = self.state.lock();
        ONLINE_INDEX_LOG_BYTES_PEAK.fetch_max(state.charge.bytes() as u64, Ordering::Relaxed);
        if state.failed {
            return Err(error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "online index build change log exceeded its memory budget",
            ));
        }
        let mut empty = Self::new().state.into_inner();
        std::mem::swap(&mut *state, &mut empty);
        Ok(empty)
    }
}

/// How long a non-exclusive online-index barrier waits without queueing ahead
/// of new DML before it fences new writers to let in-flight statements drain.
const ONLINE_INDEX_BARRIER_UNFENCED_GRACE: Duration = Duration::from_millis(50);

struct OnlineIndexTarget {
    table: TableSchema,
    index: IndexSchema,
    log: Arc<OnlineIndexBuildLog>,
    // Prevent cache eviction until the captured handle is replaced at publish.
    _capture: IndexHandle,
    file: Option<IndexFile>,
    validation_initialized: bool,
    null_key_groups: u64,
    duplicate_key_groups: u64,
}

#[derive(Debug)]
struct PinnedOnlineIndexDescriptor {
    schema: Arc<TableSchema>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

struct OnlineIndexDescriptorGuard<'a> {
    db: &'a Database,
    table_id: u64,
}
impl Drop for OnlineIndexDescriptorGuard<'_> {
    fn drop(&mut self) {
        self.db
            .catalog
            .online_pinned_ids
            .write()
            .remove(&self.table_id);
        self.db
            .online_index_descriptors
            .write()
            .remove(&self.table_id);
    }
}

struct OnlineIndexMdlGuard<'a> {
    db: &'a Database,
    owner: u64,
}

/// Keep raw storage writers out as well as SQL writers, until the executor
/// has either durably published the candidate or restored its schema snapshot.
struct ReadCompatibleDefinitionGuard<'a> {
    descriptor: OnlineIndexDescriptorGuard<'a>,
    _txn: OnlineIndexTxnGuard<'a>,
    key: String,
}

impl Drop for ReadCompatibleDefinitionGuard<'_> {
    fn drop(&mut self) {
        let db = self.descriptor.db;
        let current = db
            .catalog_read()
            .ok()
            .and_then(|catalog| catalog.tables.get(&self.key).cloned());
        if let Some(table) = current {
            for id in table.storage_ids().into_iter().chain([table.id]) {
                db.table_locks_sharded
                    .set_schema_floor(id, table.schema_version);
            }
        }
        // Fields drop after this body, so the version fence is installed
        // before the data S locks are released. An aborted ALTER fences only
        // the restored version, never the rejected candidate's version.
    }
}

impl Drop for OnlineIndexMdlGuard<'_> {
    fn drop(&mut self) {
        self.db
            .release_mdl_duration(self.owner, MdlDuration::Transaction);
    }
}

type OnlineIndexTxnGuard<'a> = RollbackTxnGuard<'a>;

impl Database {
    #[cfg(test)]
    pub(crate) fn insert_bound_primary_test_row(
        &self,
        txn: TxnView,
        table: &TableSchema,
        values: Vec<Value>,
    ) -> Result<RowId, QueryError> {
        self.insert_row_in_table(txn, table, values)
    }
    #[cfg(test)]
    pub(crate) fn install_online_index_validation_test_hook(&self) -> Arc<DdlBackfillTestHook> {
        let hook = Arc::new(DdlBackfillTestHook::new());
        *self.online_index_validation_test_hook.lock().unwrap() = Some(hook.clone());
        hook
    }

    #[cfg(test)]
    pub(crate) fn force_cold_online_descriptor_for_test(&self, name: &str) {
        let table = self.table(name).unwrap();
        self.catalog.write().unwrap().tables.insert(
            table.qualified_name(),
            CatalogManager::object_header(&table),
        );
    }
    pub(crate) fn primary_definition_changed_since(&self, txn: TxnView) -> bool {
        !txn.dirty_read
            && txn.snapshot_scn < self.primary_definition_highwater.load(Ordering::Acquire)
    }

    pub(crate) fn read_definition_changed_since(&self, txn: TxnView) -> bool {
        !txn.dirty_read && txn.snapshot_scn < self.read_definition_highwater.load(Ordering::Acquire)
    }

    fn check_index_read_view(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        txn: TxnView,
    ) -> Result<(), QueryError> {
        self.check_primary_read_view(table, txn)?;
        if !txn.dirty_read && txn.snapshot_scn < index.readable_from_scn {
            return Err(error(
                crate::error::catalog::ER_TABLE_DEF_CHANGED,
                "index does not cover this read snapshot; replan using a snapshot-compatible access path",
            ));
        }
        Ok(())
    }

    pub(crate) fn check_primary_read_view(
        &self,
        table: &TableSchema,
        txn: TxnView,
    ) -> Result<(), QueryError> {
        if !txn.dirty_read
            && table.row_generation_txn_id != 0
            && !self.row_version_visible(RowHeader::visible(table.row_generation_txn_id), txn)?
        {
            return Err(error(
                crate::error::catalog::ER_TABLE_DEF_CHANGED,
                "table row generation changed after this read view; retry the transaction",
            ));
        }
        if !txn.dirty_read && txn.snapshot_scn < table.primary_definition_scn {
            return Err(error(
                crate::error::catalog::ER_TABLE_DEF_CHANGED,
                "primary key definition changed after this read view; retry the transaction",
            ));
        }
        Ok(())
    }
    pub(crate) fn pin_read_compatible_ddl_definition(
        &self,
        schema: &TableSchema,
    ) -> Result<impl Drop + '_, QueryError> {
        let txn = self.begin_read_only_txn()?;
        let txn_guard = OnlineIndexTxnGuard {
            db: self,
            txn_id: txn.txn_id,
        };
        let mut ids = schema.storage_ids();
        ids.push(schema.id);
        ids.sort_unstable();
        ids.dedup();
        for id in ids {
            self.acquire_table_shared(txn.txn_id, id)?;
        }
        self.pin_online_index_descriptor(schema)?;
        Ok(ReadCompatibleDefinitionGuard {
            descriptor: OnlineIndexDescriptorGuard {
                db: self,
                table_id: schema.id,
            },
            _txn: txn_guard,
            key: schema.qualified_name(),
        })
    }

    fn pin_online_index_descriptor(&self, schema: &TableSchema) -> Result<(), QueryError> {
        let charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
            CatalogManager::descriptor_bytes(schema),
        )?;
        self.online_index_descriptors.write().insert(
            schema.id,
            PinnedOnlineIndexDescriptor {
                schema: Arc::new(schema.clone()),
                _charge: charge,
            },
        );
        self.catalog.online_pinned_ids.write().insert(schema.id);
        self.catalog_write()?.tables.insert(
            table_key(&schema.database, &schema.name),
            Arc::new(schema.clone()),
        );
        Ok(())
    }

    fn sync_online_index_descriptor(&self, table_id: u64) -> Result<(), QueryError> {
        if !self.online_index_descriptors.read().contains_key(&table_id) {
            return Ok(());
        }
        let key = self.catalog.table_key_by_id(table_id).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                "online index source disappeared",
            )
        })?;
        let schema = self
            .catalog_read()?
            .tables
            .get(&key)
            .cloned()
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    "online index source disappeared",
                )
            })?;
        self.pin_online_index_descriptor(&schema)
    }
    #[cfg(test)]
    pub(crate) fn install_online_index_replay_test_hook(&self) -> Arc<DdlBackfillTestHook> {
        let hook = Arc::new(DdlBackfillTestHook::new());
        *self.online_index_replay_test_hook.lock().unwrap() = Some(hook.clone());
        hook
    }

    pub(crate) fn add_indexes_for_sql(
        self: &Arc<Self>,
        table: &str,
        indexes: Vec<IndexSchema>,
        options: IndexBuildOptions,
        owner: u64,
        online_allowed: bool,
    ) -> Result<(), QueryError> {
        if indexes
            .iter()
            .all(|i| i.index_type.eq_ignore_ascii_case("BTREE"))
        {
            // Shared builds use the same private target and short barriers,
            // with a shared data lock during scanning. The offline builder's
            // table-X lock would silently violate LOCK=SHARED for readers.
            self.add_indexes_online(table, indexes, options, owner, online_allowed)
        } else {
            self.add_indexes_with_async_options(table, indexes, options)
        }
    }

    /// SQL calls this only after implicit commit, holding statement SU MDL.
    /// Transaction-duration MDL is therefore free for the two short barriers.
    pub(crate) fn add_indexes_online(
        &self,
        table_name: &str,
        indexes: Vec<IndexSchema>,
        options: IndexBuildOptions,
        owner: u64,
        allow_dml: bool,
    ) -> Result<(), QueryError> {
        if indexes.is_empty() {
            return Ok(());
        }
        if indexes
            .iter()
            .any(|i| !i.index_type.eq_ignore_ascii_case("BTREE"))
        {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "online index build requires BTREE indexes",
            ));
        }
        let source = self.table(table_name)?;
        if !allow_dml {
            // Retain both DDL ownership (SU) and writer exclusion (SNW) for
            // LOCK=SHARED builds. A table S lock alone lets SQL writers hold
            // SW MDL while waiting for us, creating a publication lock cycle.
            self.acquire_alter_execution_mdl(
                owner,
                table_name,
                MdlLockType::SharedNoWrite,
                self.config.lock_wait,
            )?;
        }
        // Index addition preserves heap slots/encoding, including a primary
        // key. Read-side snapshot fences hide newly established key and NOT
        // NULL facts until the reader's snapshot is covered by publication.
        let read_compatible = true;
        let barrier_mode = MdlLockType::SharedNoWrite;
        let mut lock_ids = source.storage_ids();
        lock_ids.push(source.id);
        lock_ids.sort_unstable();
        lock_ids.dedup();
        let _barrier_guard = OnlineIndexMdlGuard { db: self, owner };
        self.online_index_barrier_mode(&source, owner, false, barrier_mode)?;
        let prepare_timer = OnlineIndexTimer(&ONLINE_INDEX_PREPARE_US, Instant::now());
        let ddl_txn = self.begin_read_only_txn()?;
        let _txn_guard = OnlineIndexTxnGuard {
            db: self,
            txn_id: ddl_txn.txn_id,
        };
        let _descriptor_guard = OnlineIndexDescriptorGuard {
            db: self,
            table_id: source.id,
        };
        let mut prepared_version = source.schema_version;
        let mut names = Vec::new();
        let mut targets = Vec::new();
        let key = table_key(&source.database, &source.name);
        let mut publication_started = false;
        let subjects = indexes
            .iter()
            .map(|index| index.name.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let job = self.register_ddl_job_with_subject(&source, "INDEX", false, subjects);
        let _job_cancel_scope = crate::engine::cancel::enter(job.cancel_token.clone());
        Self::set_alter_copy_job_state(&job, "INDEX_BUILD");
        let result: Result<(), QueryError> = (|| {
            let prepared = (|| {
                self.try_online_index_data_barrier_mode(
                    ddl_txn.txn_id,
                    &lock_ids,
                    read_compatible,
                )?;
                self.pin_online_index_descriptor(&source)?;
                for index in indexes {
                    let (_, table, name) = self.prepare_index_build(
                        table_name,
                        index,
                        Some(ddl_txn.txn_id),
                        true,
                        true,
                    )?;
                    names.push(name.clone());
                    let mut index = table
                        .indexes
                        .iter()
                        .find(|i| i.name == name)
                        .unwrap()
                        .clone();
                    if table
                        .pending_primary_key
                        .as_ref()
                        .is_some_and(|p| p.capture_name == name)
                    {
                        index.primary = true;
                        index.unique = true;
                    }
                    prepared_version = table.schema_version;
                    let storage_ids = table
                        .partitioning
                        .as_ref()
                        .map(|p| p.storage_ids())
                        .unwrap_or_else(|| vec![table.id]);
                    for storage_id in storage_ids {
                        let mut leaf = table.clone();
                        leaf.id = storage_id;
                        leaf.active_generation = self.storage_generation(storage_id);
                        let capture = self.index_handle(storage_id, &name)?;
                        let log = Arc::new(OnlineIndexBuildLog::new());
                        {
                            let mut handle = capture.write();
                            handle.flush()?;
                            handle.online_build_log = Some(log.clone());
                            handle.retain_buffered_pages_on_drop();
                        }
                        targets.push(OnlineIndexTarget {
                            table: leaf,
                            index: index.clone(),
                            log,
                            _capture: capture,
                            file: None,
                            validation_initialized: false,
                            null_key_groups: 0,
                            duplicate_key_groups: 0,
                        });
                    }
                }
                Ok(())
            })();
            prepared?;
            for id in &lock_ids {
                self.table_locks_sharded
                    .set_schema_floor(*id, prepared_version);
            }
            for id in &lock_ids {
                self.table_locks_sharded
                    .downgrade_exclusive_to_shared_mode(
                        ddl_txn.txn_id,
                        *id,
                        if allow_dml {
                            TableLockMode::IntentShared
                        } else {
                            TableLockMode::Shared
                        },
                    )?;
            }
            self.notify_non_row_lock_waiters();
            self.release_mdl_duration(owner, MdlDuration::Transaction);
            drop(prepare_timer);
            #[cfg(test)]
            self.pause_ddl_backfill_for_test();

            // The snapshot keeps undo and deleted slots alive during scanning
            // and replay; writers still use their ordinary IX/row locks.
            self.build_online_index_targets(&mut targets, options)?;
            Self::set_alter_copy_job_state(&job, "CATCH_UP");
            #[cfg(test)]
            if let Some(hook) = self.online_index_replay_test_hook.lock().unwrap().clone() {
                hook.pause();
            }
            self.replay_online_index_targets(&mut targets, false)?;
            for target in &mut targets {
                Self::initialize_online_index_validation(target)?;
            }
            #[cfg(test)]
            if let Some(hook) = self
                .online_index_validation_test_hook
                .lock()
                .unwrap()
                .clone()
            {
                hook.pause();
            }
            // Continue replay outside the barrier. Only a bounded final batch
            // is admitted to the publication window.
            for attempt in 0..64 {
                crate::engine::cancel::check_cancelled()?;
                self.replay_online_index_targets(&mut targets, true)?;
                for target in &targets {
                    target.file.as_ref().unwrap().flush()?;
                }
                self.online_index_barrier_mode(&source, owner, false, barrier_mode)?;
                self.try_online_index_data_barrier_mode(
                    ddl_txn.txn_id,
                    &lock_ids,
                    read_compatible,
                )?;
                let pending: usize = targets
                    .iter()
                    .map(|t| t.log.state.lock().entries.len())
                    .sum();
                let pending_bytes: usize = targets
                    .iter()
                    .map(|t| t.log.state.lock().charge.bytes())
                    .sum();
                if pending <= 4096 && pending_bytes <= 1024 * 1024 {
                    ONLINE_INDEX_TAIL_ENTRIES_PEAK.fetch_max(pending as u64, Ordering::Relaxed);
                    ONLINE_INDEX_TAIL_BYTES_PEAK.fetch_max(pending_bytes as u64, Ordering::Relaxed);
                    break;
                }
                for id in &lock_ids {
                    self.table_locks_sharded
                        .downgrade_exclusive_to_intent_shared(ddl_txn.txn_id, *id)?;
                }
                self.notify_non_row_lock_waiters();
                self.release_mdl_duration(owner, MdlDuration::Transaction);
                if attempt == 63 {
                    return Err(error(
                        crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                        "online index build could not converge within its final-tail budget",
                    ));
                }
            }
            let publish_timer = OnlineIndexTimer(&ONLINE_INDEX_PUBLISH_US, Instant::now());
            let published = (|| {
                self.try_online_index_data_barrier_mode(
                    ddl_txn.txn_id,
                    &lock_ids,
                    read_compatible,
                )?;
                self.replay_online_index_targets(&mut targets, true)?;
                for target in &targets {
                    target.file.as_ref().unwrap().flush()?;
                }
                if targets.iter().any(|t| t.null_key_groups != 0) {
                    return Err(error(
                        crate::error::catalog::ER_INVALID_USE_OF_NULL,
                        "invalid use of NULL value for primary key",
                    ));
                }
                for target in &targets {
                    Self::finish_online_index_validation(target)?;
                }
                Self::enter_alter_copy_finalizing(&job)?;
                {
                    let current = self.table(table_name)?;
                    if current.schema_version != prepared_version
                        || current.active_generation != source.active_generation
                    {
                        return Err(error(
                            crate::error::catalog::ER_TABLE_DEF_CHANGED,
                            "online index source definition changed",
                        ));
                    }
                }
                if targets.iter().any(|t| t.index.primary) {
                    self.append_wal(
                        txn_id_for_ddl(),
                        WalPayload::DdlCommit {
                            job_id: PRIMARY_PUBLICATION_JOB_TAG | ddl_txn.txn_id,
                            retired_paths: Vec::new(),
                        },
                    )?;
                }
                for target in &mut targets {
                    let file = target.file.take().unwrap();
                    let mut cache = self.index_cache.write();
                    let name = if target.index.primary {
                        "PRIMARY"
                    } else {
                        &target.index.name
                    };
                    if target.index.primary {
                        cache.remove(&(target.table.id, normalize_name(&target.index.name)));
                    }
                    cache.insert(
                        (target.table.id, normalize_name(name)),
                        Arc::new(parking_lot::RwLock::new(file)),
                    );
                }
                // Once publication starts, an ambiguous catalog I/O error
                // must not delete the flushed tree: recovery can see Public.
                publication_started = true;
                // Drain table writers first; a commit on a different table
                // can leave a hole in the visible frontier. Use the allocation
                // highwater, not visible_scn, as a conservative coverage bound.
                let readable_from_scn = self.next_scn_atomic.load(Ordering::Acquire);
                self.read_definition_highwater
                    .fetch_max(readable_from_scn, Ordering::Release);
                {
                    let mut catalog = self.catalog_write()?;
                    let table = Arc::make_mut(catalog.tables.get_mut(&key).ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_NO_SUCH_TABLE,
                            "index source disappeared",
                        )
                    })?);
                    for index in &mut table.indexes {
                        if names.contains(&index.name) {
                            index.state = IndexState::Public;
                            index.readable_from_scn = readable_from_scn;
                        }
                    }
                    if let Some(pending) = table.pending_primary_key.take() {
                        let slot = table
                            .indexes
                            .iter_mut()
                            .find(|i| i.index_id == pending.final_index.index_id)
                            .ok_or_else(|| {
                                error(
                                    crate::error::catalog::ER_GET_ERRNO,
                                    "missing primary capture index",
                                )
                            })?;
                        *slot = pending.final_index;
                        slot.state = IndexState::Public;
                        slot.readable_from_scn = readable_from_scn;
                        table.primary_key = slot.columns.clone();
                        for id in pending.column_ids {
                            let col = table
                                .columns
                                .iter_mut()
                                .find(|c| c.column_id == id)
                                .ok_or_else(|| {
                                    error(
                                        crate::error::catalog::ER_TABLE_DEF_CHANGED,
                                        "primary column disappeared",
                                    )
                                })?;
                            col.nullable = false;
                            col.not_null_from_scn = readable_from_scn;
                        }
                        table.primary_publication_job_id =
                            PRIMARY_PUBLICATION_JOB_TAG | ddl_txn.txn_id;
                        table.primary_definition_scn = self.visible_scn();
                        self.primary_definition_highwater
                            .fetch_max(table.primary_definition_scn, Ordering::Release);
                    }
                    table.bump_schema_version();
                    table.build_column_index();
                    for id in &lock_ids {
                        self.table_locks_sharded
                            .set_schema_floor(*id, table.schema_version);
                    }
                }
                self.sync_online_index_descriptor(source.id)?;
                for id in source.storage_ids() {
                    self.refresh_heap_codec(id)?;
                }
                self.refresh_heap_codec(source.id)?;
                self.note_catalog_table(source.id);
                self.commit_ddl_catalog_txn()?;
                Ok(())
            })();
            drop(publish_timer);
            published?;
            Ok(())
        })();
        if result.is_err() && !publication_started {
            if let Ok(current) = self.table(table_name) {
                for index in &current.indexes {
                    if index.state == IndexState::Building
                        && !source
                            .indexes
                            .iter()
                            .any(|old| old.name.eq_ignore_ascii_case(&index.name))
                        && !names.contains(&index.name)
                    {
                        names.push(index.name.clone());
                    }
                }
            }
        }
        if result.is_err() && !publication_started && !names.is_empty() {
            // Cleanup is a separate correctness operation, not another part
            // of the cancelled/OOM statement. It still uses governed memory.
            let _cancel =
                crate::engine::cancel::enter(crate::engine::cancel::QueryCancelToken::noop());
            let _deadline = crate::engine::cancel::install_statement_deadline(Duration::ZERO);
            let cleanup = crate::engine::executor::mem_budget::with_query_budget(
                Arc::new(crate::engine::executor::mem_budget::MemBudget::new(
                    crate::engine::executor::mem_budget::query_mem_limit_bytes(),
                )),
                || {
                    crate::engine::executor::mem_budget::with_critical_memory_reservations(|| {
                        self.online_index_barrier_mode(&source, owner, true, barrier_mode)?;
                        loop {
                            match self.try_online_index_data_barrier_mode(
                                ddl_txn.txn_id,
                                &lock_ids,
                                read_compatible,
                            ) {
                                Ok(()) => break,
                                Err(err) if is_lock_wait_pending_error(&err) => {
                                    self.wait_for_ddl_signal(Duration::from_millis(10))?
                                }
                                Err(err) => return Err(err),
                            }
                        }
                        for target in &mut targets {
                            target.file.take();
                        }
                        self.discard_online_index_candidates(table_name, &names)
                    })
                },
            );
            if let Err(err) = cleanup {
                self.quarantine_mvcc(format!("online index cleanup failed: {}", err.message()));
                self.unregister_alter_copy_job(source.id, Some(&err));
                return Err(err);
            }
        }
        if publication_started {
            if let Err(err) = &result {
                self.quarantine_mvcc(format!(
                    "online index publication failed; recovery required: {}",
                    err.message()
                ));
            }
        }
        self.unregister_alter_copy_job(source.id, result.as_ref().err());
        result
    }

    /// The intent remains durable until private files have been unlinked.
    /// Thus a crash anywhere in cleanup repeats the operation safely.
    fn discard_online_index_candidates(
        &self,
        table_name: &str,
        names: &[String],
    ) -> Result<(), QueryError> {
        let schema = self.table(table_name)?;
        let mut storage_ids = schema.storage_ids();
        storage_ids.push(schema.id);
        storage_ids.sort_unstable();
        storage_ids.dedup();
        for index in schema
            .indexes
            .iter()
            .filter(|i| names.iter().any(|n| n.eq_ignore_ascii_case(&i.name)))
        {
            if index.state != IndexState::Building {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "refusing to discard a published index",
                ));
            }
            for id in &storage_ids {
                let path = self.index_path(*id, &index.name);
                self.remove_index_cache_entry(*id, &index.name)?;
                self.delete_logged_file(txn_id_for_ddl(), path.clone())?;
                match fs::remove_file(path) {
                    Ok(()) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => return Err(io_err(err)),
                }
            }
        }
        {
            let mut catalog = self.catalog_write()?;
            let table = Arc::make_mut(
                catalog
                    .tables
                    .get_mut(&table_key(&schema.database, &schema.name))
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_NO_SUCH_TABLE,
                            "online index source disappeared",
                        )
                    })?,
            );
            table
                .indexes
                .retain(|i| !names.iter().any(|n| n.eq_ignore_ascii_case(&i.name)));
            if table
                .pending_primary_key
                .as_ref()
                .is_some_and(|p| names.contains(&p.capture_name))
            {
                table.pending_primary_key = None;
            }
            table.bump_schema_version();
            for id in &storage_ids {
                self.table_locks_sharded
                    .set_schema_floor(*id, table.schema_version);
            }
        }
        self.sync_online_index_descriptor(schema.id)?;
        self.note_catalog_table(schema.id);
        self.commit_catalog_txn()
    }

    fn recover_pending_primary_keys(&self) -> Result<(), QueryError> {
        if self.physical_replica_mode() {
            return Ok(());
        }
        let ids = std::mem::take(&mut *self.pending_primary_recovery.lock());
        for id in ids {
            let Some(table) = self.decode_table_descriptor_rows_with_building(id, true)? else {
                continue;
            };
            let Some(pending) = table.pending_primary_key.as_ref() else {
                continue;
            };
            if !table.primary_key.is_empty() || pending.source_generation != table.active_generation
            {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "inconsistent pending primary-key definition",
                ));
            }
            let names = table
                .indexes
                .iter()
                .filter(|i| i.state == IndexState::Building)
                .map(|i| i.name.clone())
                .collect::<Vec<_>>();
            let _pin = OnlineIndexDescriptorGuard {
                db: self,
                table_id: id,
            };
            self.pin_online_index_descriptor(&table)?;
            self.discard_online_index_candidates(&table.qualified_name(), &names)?;
        }
        Ok(())
    }

    fn recover_published_primary_keys(
        &self,
        replayed_jobs: &HashSet<u64>,
    ) -> Result<(), QueryError> {
        let headers = self
            .catalog_read()?
            .tables
            .values()
            .filter(|t| t.primary_publication_job_id != 0)
            .cloned()
            .collect::<Vec<_>>();
        for header in headers {
            let replayed = (self.physical_replica_mode()
                || self.recovery_context.redo_floor().is_some())
                && replayed_jobs.contains(&header.primary_publication_job_id);
            let identities = self.catalog_rows_for_selector(
                crate::storage::data_dictionary::DD_INDEXES,
                CatalogRowSelector::PrimaryValues(vec![
                    Value::UInt(header.id),
                    Value::String("PRIMARY".into()),
                ]),
            )?;
            let Some((_, index_id)) = identities
                .first()
                .and_then(|row| crate::storage::data_dictionary::index_storage_identity(row))
            else {
                continue;
            };
            let storage_ids = header.storage_ids();
            let missing = storage_ids.iter().any(|id| {
                !crate::storage::naming::index_object_path_by_id(
                    &self.data_dir,
                    *id,
                    self.storage_generation(*id),
                    index_id,
                    "idx",
                )
                .exists()
            });
            if !replayed && !missing {
                continue;
            }
            let table = self.table(&header.qualified_name())?;
            let Some(primary) = table
                .indexes
                .iter()
                .find(|i| i.primary && i.state == IndexState::Public)
            else {
                continue;
            };
            for id in storage_ids {
                let path = self.index_path(id, &primary.name);
                // A received commit can be replayed before the applier ever
                // materialized its private bulk tree. Rebuild from committed
                // heap even when startup validation is disabled. Backup's
                // PITR may contain only the capture initialization plus later
                // DML, never the source's private bulk pages. Rebuild a replayed
                // publication during restore even if that partial file exists.
                if replayed || !path.exists() {
                    let mut leaf = table.as_ref().clone();
                    leaf.id = id;
                    leaf.active_generation = self.storage_generation(id);
                    self.rebuild_index_offline_bulk(&leaf, primary)?;
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn exhaust_online_index_log_for_test(&self, name: &str) {
        let table = self.table(name).unwrap();
        for index in table
            .indexes
            .iter()
            .filter(|i| i.state == IndexState::Building)
        {
            for id in table.storage_ids() {
                let handle = self.index_handle(id, &index.name).unwrap();
                handle
                    .read()
                    .online_build_log
                    .as_ref()
                    .unwrap()
                    .state
                    .lock()
                    .limit_bytes = 0;
            }
        }
    }

    fn try_online_index_data_barrier(&self, txn_id: u64, ids: &[u64]) -> Result<(), QueryError> {
        self.try_online_index_data_barrier_mode(txn_id, ids, false)
    }

    fn try_online_index_data_barrier_mode(
        &self,
        txn_id: u64,
        ids: &[u64],
        read_compatible: bool,
    ) -> Result<(), QueryError> {
        for (position, id) in ids.iter().enumerate() {
            let result = if read_compatible {
                self.acquire_table_shared(txn_id, *id)
            } else {
                self.acquire_table_exclusive(txn_id, *id)
            };
            if let Err(err) = result {
                for acquired in &ids[..position] {
                    self.table_locks_sharded
                        .downgrade_exclusive_to_intent_shared(txn_id, *acquired)?;
                }
                self.notify_non_row_lock_waiters();
                return Err(err);
            }
        }
        Ok(())
    }

    fn online_index_barrier(
        &self,
        table: &TableSchema,
        owner: u64,
        cleanup: bool,
    ) -> Result<(), QueryError> {
        self.online_index_barrier_mode(table, owner, cleanup, MdlLockType::Exclusive)
    }

    fn online_index_barrier_mode(
        &self,
        table: &TableSchema,
        owner: u64,
        cleanup: bool,
        mode: MdlLockType,
    ) -> Result<(), QueryError> {
        self.online_index_barrier_checked(table, owner, cleanup, mode, || Ok(()))
    }

    fn online_index_barrier_checked(
        &self,
        table: &TableSchema,
        owner: u64,
        cleanup: bool,
        mode: MdlLockType,
        check: impl Fn() -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let _timer = OnlineIndexTimer(&ONLINE_INDEX_BARRIER_WAIT_US, Instant::now());
        let started = Instant::now();
        let mut drain_started = started;
        let key = MdlObjectKey::table(&table.database, &table.name);
        loop {
            if !cleanup {
                self.ensure_operational()?;
                crate::engine::cancel::check_cancelled()?;
                check()?;
            }
            match self.acquire_mdl(
                owner,
                Some(owner),
                MdlDuration::Transaction,
                vec![MdlRequest {
                    key: key.clone(),
                    lock_type: mode,
                }],
                "ddl/online-index/barrier",
            ) {
                Ok(()) => return Ok(()),
                Err(err) if is_lock_wait_pending_error(&err) => {
                    // Exclusive COPY barriers must fence new fast readers so
                    // an uninterrupted read stream cannot starve publication.
                    // A non-exclusive barrier first tries without queueing
                    // ahead of new DML, but a continuous stream of short
                    // writers always holds SW, so after a short grace it is
                    // queued too and only in-flight statements drain.
                    // A long-lived holder cannot finish merely by fencing new
                    // work; periodically admit DML while waiting for it. Slow
                    // statement readers still drain with the ticket queued.
                    let fence = mode == MdlLockType::Exclusive
                        || started.elapsed() >= ONLINE_INDEX_BARRIER_UNFENCED_GRACE;
                    let yield_to_holder = drain_started.elapsed() >= Duration::from_millis(100)
                        && self.mdl.has_long_lived_blocker(&key, owner, mode);
                    if !fence || yield_to_holder {
                        self.mdl.clear_pending_for_owner(owner);
                        self.notify_non_row_lock_waiters();
                        drain_started = Instant::now();
                    }
                    // The caller's MDL guard clears pending tickets on exit.
                    if !cleanup && started.elapsed() >= crate::storage::txn::metadata_wait_timeout()
                    {
                        return Err(error(
                            crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                            "online index build timed out waiting for metadata lock",
                        ));
                    }
                    self.wait_for_ddl_signal(Duration::from_millis(10))?;
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn build_online_index_targets(
        &self,
        targets: &mut [OnlineIndexTarget],
        options: IndexBuildOptions,
    ) -> Result<(), QueryError> {
        INDEX_BUILD_REQUESTED_WORKERS.fetch_add(options.requested_degree as u64, Ordering::Relaxed);
        INDEX_BUILD_EFFECTIVE_WORKERS.fetch_add(options.effective_degree as u64, Ordering::Relaxed);
        if options.effective_degree > 1 {
            INDEX_BUILD_PARALLEL_BUILDS.fetch_add(1, Ordering::Relaxed);
        } else {
            INDEX_BUILD_SERIAL_BUILDS.fetch_add(1, Ordering::Relaxed);
        }
        if options.effective_degree < options.requested_degree {
            INDEX_BUILD_DEGREE_DOWNGRADES.fetch_add(1, Ordering::Relaxed);
        }
        let storage_ids = targets.iter().map(|t| t.table.id).collect::<BTreeSet<_>>();
        for storage_id in storage_ids {
            let group = targets
                .iter()
                .filter(|t| t.table.id == storage_id)
                .collect::<Vec<_>>();
            let mut table = group[0].table.clone();
            let names = group
                .iter()
                .map(|t| t.index.name.clone())
                .collect::<Vec<_>>();
            table.indexes = group
                .iter()
                .map(|t| {
                    let mut index = t.index.clone();
                    // Validate after replay: concurrent changes may resolve keys
                    // duplicated in a non-snapshot physical scan.
                    index.unique = false;
                    index.primary = false;
                    index
                })
                .collect();
            if self.config.durability != DurabilityMode::Memory
                && table.indexes.iter().all(encoded_btree_build_eligible)
            {
                let validation_indexes = group
                    .iter()
                    .filter(|target| target.index.unique)
                    .map(|target| target.index.name.clone())
                    .collect::<Vec<_>>();
                let mut validations = self.rebuild_indexes_encoded_batch(
                    &table,
                    &names,
                    options,
                    true,
                    &validation_indexes,
                )?;
                for target in targets.iter_mut().filter(|t| t.table.id == storage_id) {
                    if let Some(validation) = validations.remove(&target.index.name) {
                        target.null_key_groups = if target.index.primary {
                            validation.null_key_groups
                        } else {
                            0
                        };
                        target.duplicate_key_groups = validation.duplicate_key_groups;
                        target.validation_initialized = true;
                    }
                }
            } else {
                for index in &table.indexes {
                    let minimum = 16 * 1024 * 1024;
                    let (_lease, _charge, scratch) = reserve_offline_index_build_scratch(
                        self,
                        crate::engine::executor::mem_budget::query_mem_limit_bytes().max(minimum),
                        minimum,
                    )?;
                    let entries = self.collect_sorted_index_entries_with_degree(
                        &table,
                        index,
                        options.effective_degree,
                        scratch,
                    )?;
                    let path = self.index_path_for_table_schema(&table, storage_id, &index.name);
                    let stream: Box<dyn Iterator<Item = Result<IndexEntry, QueryError>>> =
                        match entries {
                            SortedIndexEntries::Memory(entries) => {
                                Box::new(entries.into_iter().map(Ok))
                            }
                            SortedIndexEntries::Spilled(runs) => Box::new(runs.into_merger()?),
                        };
                    let _generation =
                        btree_logical_wal::LsnScope::new(Some(self.log.lock().last_lsn()));
                    let file = IndexFile::create_rebuilt_from_sorted_stream_with_durability(
                        path,
                        stream,
                        self.log.clone(),
                        self.config.durability,
                        self.buffer_pool.clone(),
                    )?;
                    file.flush()?;
                    targets
                        .iter_mut()
                        .find(|t| t.table.id == storage_id && t.index.name == index.name)
                        .unwrap()
                        .file = Some(file);
                }
            }
        }
        for target in targets {
            if target.file.is_some() {
                continue;
            }
            target.file = Some(IndexFile::open_with_durability(
                self.index_path_for_table_schema(
                    &target.table,
                    target.table.id,
                    &target.index.name,
                ),
                self.log.clone(),
                self.config.durability,
                self.buffer_pool.clone(),
            )?);
        }
        Ok(())
    }

    fn replay_online_index_targets(
        &self,
        targets: &mut [OnlineIndexTarget],
        validate: bool,
    ) -> Result<(), QueryError> {
        let _timer = OnlineIndexTimer(&ONLINE_INDEX_REPLAY_US, Instant::now());
        for target in targets {
            // Streamed counts describe the pre-replay tree. Every replay,
            // including the first, must update them before publication.
            let validate = validate || target.validation_initialized;
            let batch = target.log.take()?;
            let file = target.file.as_ref().unwrap();
            let _validation_charge = if validate && target.index.unique {
                Some(crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    batch.charge.bytes().saturating_mul(2),
                )?)
            } else {
                None
            };
            let keys = if validate && target.index.unique {
                batch
                    .entries
                    .iter()
                    .map(|e| e.key.clone())
                    .collect::<BTreeSet<_>>()
            } else {
                BTreeSet::new()
            };
            let before = keys
                .into_iter()
                .map(|key| Self::online_index_key_state(target, &key).map(|state| (key, state)))
                .collect::<Result<Vec<_>, _>>()?;
            let mut entries = batch.entries.into_iter();
            loop {
                crate::engine::cancel::check_cancelled()?;
                let chunk = entries.by_ref().take(4096).collect::<Vec<_>>();
                if chunk.is_empty() {
                    break;
                }
                file.apply_batch_deferred(txn_id_for_ddl(), chunk)?;
            }
            for (key, old) in before {
                let new = Self::online_index_key_state(target, &key)?;
                Self::adjust_online_index_count(&mut target.null_key_groups, old.0, new.0)?;
                Self::adjust_online_index_count(&mut target.duplicate_key_groups, old.1, new.1)?;
            }
        }
        Ok(())
    }

    fn online_index_key_state(
        target: &OnlineIndexTarget,
        key: &IndexKey,
    ) -> Result<(bool, bool), QueryError> {
        let bound = IndexBound {
            key: key.clone(),
            inclusive: true,
        };
        let entries = target.file.as_ref().unwrap().range_entries_chunk(
            Some(&bound),
            Some(&bound),
            None,
            2,
        )?;
        let null = key.0.iter().any(|part| matches!(part, KeyPart::Null));
        Ok((
            target.index.primary && null && !entries.is_empty(),
            !null && entries.len() > 1,
        ))
    }

    fn adjust_online_index_count(
        count: &mut u64,
        before: bool,
        after: bool,
    ) -> Result<(), QueryError> {
        if before == after {
            return Ok(());
        }
        *count = if after {
            count.checked_add(1)
        } else {
            count.checked_sub(1)
        }
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "online index validation count is inconsistent",
            )
        })?;
        Ok(())
    }

    fn initialize_online_index_validation(
        target: &mut OnlineIndexTarget,
    ) -> Result<(), QueryError> {
        let _timer = OnlineIndexTimer(&ONLINE_INDEX_VALIDATION_US, Instant::now());
        if !target.index.unique || target.validation_initialized {
            return Ok(());
        }
        let file = target.file.as_ref().unwrap();
        let mut resume = None;
        let mut previous = None;
        let mut repeated = false;
        loop {
            crate::engine::cancel::check_cancelled()?;
            let entries = file.range_entries_chunk(None, None, resume.as_ref(), 4096)?;
            if entries.is_empty() {
                return Ok(());
            }
            for (key, _) in &entries {
                let null = key.0.iter().any(|p| matches!(p, KeyPart::Null));
                if previous.as_ref() == Some(key) {
                    if !null && !repeated {
                        target.duplicate_key_groups += 1;
                    }
                    repeated = true;
                } else {
                    if target.index.primary && null {
                        target.null_key_groups += 1;
                    }
                    repeated = false;
                }
                previous = Some(key.clone());
            }
            resume = entries.last().cloned();
        }
    }

    fn finish_online_index_validation(target: &OnlineIndexTarget) -> Result<(), QueryError> {
        if target.null_key_groups != 0 {
            return Err(error(
                crate::error::catalog::ER_INVALID_USE_OF_NULL,
                "invalid use of NULL value for primary key",
            ));
        }
        if target.duplicate_key_groups != 0 {
            return Err(error(
                crate::error::catalog::ER_DUP_ENTRY,
                format!(
                    "duplicate entry for key '{}'",
                    if target.index.primary {
                        "PRIMARY"
                    } else {
                        &target.index.name
                    }
                ),
            ));
        }
        Ok(())
    }
}
