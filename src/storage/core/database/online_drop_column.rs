// Column tombstones keep the heap generation and physical RowIds unchanged.
// Build private replacement trees at one retained source view, replay complete
// transaction-qualified row cohorts, and publish schema/files behind a short
// barrier. Projecting historical index operations directly is unsafe: deleting
// an obsolete (dropped, retained) key can erase a still-live retained key.

#[cfg(test)]
static DROP_COLUMN_LOCK_HOLD_US: AtomicU64 = AtomicU64::new(0);

struct DropColumnHoldTimer {
    #[cfg(test)]
    started: Instant,
}

impl DropColumnHoldTimer {
    fn start() -> Self {
        Self {
            #[cfg(test)]
            started: Instant::now(),
        }
    }
}

#[cfg(test)]
impl Drop for DropColumnHoldTimer {
    fn drop(&mut self) {
        DROP_COLUMN_LOCK_HOLD_US.fetch_add(
            self.started.elapsed().as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }
}

impl Database {
    #[cfg(test)]
    pub(crate) fn drop_column_hold_us_for_test(&self) -> u64 {
        DROP_COLUMN_LOCK_HOLD_US.load(Ordering::Relaxed)
    }
}

impl Database {
    pub(crate) fn online_drop_column_supported(
        &self,
        name: &str,
        column: &str,
    ) -> Result<bool, QueryError> {
        let source = self.table(name)?;
        let (_, rebuilt) = plan_index_cascade_for_drop_column(&source, column)?;
        Ok(!rebuilt.is_empty()
            && rebuilt.iter().all(|index| {
                index.index_type.eq_ignore_ascii_case("BTREE") && index.state == IndexState::Public
            }))
    }

    fn column_index_retirement(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
    ) -> Result<DropTableIntent, QueryError> {
        let mut intent = DropTableIntent {
            table_id: table.id,
            retired_index: Some((index.index_id, table.active_generation.max(1))),
            had_auto_increment: false,
            database: table.database.clone(),
            table_name: table.name.clone(),
            files: Vec::new(),
            vector_files: Vec::new(),
            vector_directories: Vec::new(),
            retired_table_cache: None,
        };
        for id in table.storage_ids() {
            let path = self.index_path_for_table_schema(table, id, &index.name);
            intent.files.push(self.relative_path(&path));
            let suffix = if index.is_spatial() { "rtree" } else { "idx" };
            for extension in [format!("{suffix}.failed"), format!("{suffix}.corrupt")] {
                intent
                    .files
                    .push(self.relative_path(&path.with_extension(extension)));
            }
            if index.is_vector() {
                intent.vector_files.extend(
                    self.owned_vector_files(table, index, id)?
                        .iter()
                        .map(|path| self.relative_path(path)),
                );
                intent.vector_directories.push(
                    self.relative_path(
                        &self
                            .vector_index_path_for_parts(
                                id,
                                table.active_generation.max(1),
                                index.index_id,
                                "vmanifest",
                            )
                            .with_extension("vector"),
                    ),
                );
            }
        }
        Ok(intent)
    }

    pub(crate) fn drop_column_online(
        &self,
        name: &str,
        column: &str,
        owner: u64,
        lock: crate::parser::AlterLock,
    ) -> Result<(), QueryError> {
        let definition = self.pin_ddl_table(name)?;
        check_table_ddl_writable(&definition.table.database)?;
        let source = {
            let catalog = self.catalog_write()?;
            self.catalog
                .online_pinned_ids
                .write()
                .insert(definition.table.id);
            catalog
                .tables
                .get(&definition.table.qualified_name())
                .cloned()
                .ok_or_else(|| ddl_table_not_found(name))?
        };
        let _descriptor = OnlineIndexDescriptorGuard {
            db: self,
            table_id: source.id,
        };
        let mut candidate = self.drop_column_candidate(&source, column)?;
        let replaced = candidate
            .indexes
            .iter()
            .filter(|index| {
                !source
                    .indexes
                    .iter()
                    .any(|old| old.index_id == index.index_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        if replaced.is_empty()
            || replaced
                .iter()
                .any(|index| !index.index_type.eq_ignore_ascii_case("BTREE"))
        {
            return Err(error(
                crate::error::catalog::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON,
                "online DROP COLUMN requires B-tree index replacements",
            ));
        }
        let obsolete = source
            .indexes
            .iter()
            .filter(|old| {
                !candidate
                    .indexes
                    .iter()
                    .any(|index| index.index_id == old.index_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        // Stable index ownership is independent of row changes. Enumerating
        // vector generations/files may perform I/O, so prepare manifests before
        // either source barrier; owned directories cover later publications.
        let retirements = obsolete
            .iter()
            .map(|index| self.column_index_retirement(&source, index))
            .collect::<Result<Vec<_>, _>>()?;
        let mut private = Vec::new();
        let mut targets = Vec::new();
        let mut job = None;
        let mut snapshot = None;
        let mut publication_started = false;
        let mut published = false;
        // This separate read-only transaction retains data locks across the
        // dictionary transaction's commit and the subsequent cache/floor switch.
        let txn = self.begin_read_only_txn()?;
        let data = OnlineIndexTxnGuard {
            db: self,
            txn_id: txn.txn_id,
        };
        let mdl = OnlineIndexMdlGuard { db: self, owner };
        let mut ids = source.storage_ids();
        ids.push(source.id);
        ids.sort_unstable();
        ids.dedup();
        let mut retired_graphs = Vec::new();
        let mut retired_handles = Vec::new();
        let mut retirement_guard = None;
        let result: Result<(), QueryError> = (|| {
            for index in &replaced {
                let intent = self.column_index_retirement(&candidate, index)?;
                let path = self.write_drop_table_intent(&intent)?;
                private.push((intent, path));
            }
            self.online_index_barrier_mode(&source, owner, false, MdlLockType::SharedNoWrite)?;
            if lock == crate::parser::AlterLock::Shared {
                // Retain SU for DDL ownership and an independent SNW ticket
                // across writable-tail barrier retries. Both end with SQL.
                self.acquire_mdl(
                    owner,
                    Some(owner),
                    MdlDuration::Statement,
                    vec![MdlRequest {
                        key: MdlObjectKey::table(&source.database, &source.name),
                        lock_type: MdlLockType::SharedNoWrite,
                    }],
                    "ddl/drop-column/shared-build",
                )?;
            }
            let prepare_hold = DropColumnHoldTimer::start();
            self.try_online_index_data_barrier_mode(txn.txn_id, &ids, true)?;
            job = Some(self.register_ddl_job_with_subject(
                &source,
                "DROP_COLUMN",
                true,
                column.to_owned(),
            ));
            let job = job.as_ref().unwrap();
            let _cancel = crate::engine::cancel::enter(job.cancel_token.clone());
            snapshot = Some(StorageReadSnapshot::new(self));
            // Source writers are drained. Include every committed source row,
            // even when a commit on another table holds back the global frontier.
            let mut watermark = TxnView {
                snapshot_scn: self
                    .next_scn_atomic
                    .load(Ordering::Acquire)
                    .saturating_sub(1),
                ..snapshot.as_ref().unwrap().txn
            };
            for id in &ids {
                self.table_locks_sharded
                    .downgrade_exclusive_to_shared_mode(
                        txn.txn_id,
                        *id,
                        if lock == crate::parser::AlterLock::Shared {
                            TableLockMode::Shared
                        } else {
                            TableLockMode::IntentShared
                        },
                    )?;
            }
            self.release_mdl_duration(owner, MdlDuration::Transaction);
            self.notify_non_row_lock_waiters();
            drop(prepare_hold);
            #[cfg(test)]
            self.pause_ddl_backfill_for_test();
            Self::set_alter_copy_job_state(job, "INDEX_BUILD");
            let columns = drop_column_index_columns(&source, &replaced)?;
            self.build_drop_column_indexes(
                &source,
                &replaced,
                &columns,
                watermark,
                &mut targets,
                job,
            )?;
            // Group handles once; replay must not visit every partition for
            // each touched row on a heavily partitioned table.
            targets.sort_unstable_by_key(|target| target.table.id);
            Self::set_alter_copy_job_state(job, "CATCH_UP");
            for attempt in 0..64 {
                Self::check_alter_copy_cancelled(job)?;
                let read = TxnView {
                    snapshot_scn: watermark.snapshot_scn.max(self.visible_scn()),
                    ..watermark
                };
                self.replay_drop_column_indexes(
                    &source,
                    &columns,
                    &mut targets,
                    job,
                    watermark,
                    read,
                    None,
                )?;
                watermark = read;
                for target in &targets {
                    target.file.as_ref().unwrap().flush()?;
                }
                #[cfg(test)]
                if let Some(hook) = self.online_copy_publish_test_hook.lock().unwrap().take() {
                    hook.pause();
                }
                self.online_index_barrier(&source, owner, false)?;
                let hold = DropColumnHoldTimer::start();
                self.try_online_index_data_barrier(txn.txn_id, &ids)?;
                // Never wait for an unrelated cleanup I/O while holding table X.
                let cleanup = if job.changes.len() <= 1024 {
                    self.ddl_retirement_cleanup.try_lock()
                } else {
                    None
                };
                let Some(_cleanup) = cleanup else {
                    for id in &ids {
                        self.table_locks_sharded
                            .downgrade_exclusive_to_shared_mode(
                                txn.txn_id,
                                *id,
                                if lock == crate::parser::AlterLock::Shared {
                                    TableLockMode::Shared
                                } else {
                                    TableLockMode::IntentShared
                                },
                            )?;
                    }
                    self.release_mdl_duration(owner, MdlDuration::Transaction);
                    self.notify_non_row_lock_waiters();
                    drop(hold);
                    if attempt == 63 {
                        return Err(error(
                            crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                            "online DROP COLUMN could not converge within its final-tail budget",
                        ));
                    }
                    self.wait_for_ddl_signal(Duration::from_millis(10))?;
                    continue;
                };
                retirement_guard = Some(_cleanup);
                // All source transactions are now committed/rolled back; this
                // highwater is a stable source view, regardless of unrelated gaps.
                let read = TxnView {
                    snapshot_scn: self
                        .next_scn_atomic
                        .load(Ordering::Acquire)
                        .saturating_sub(1),
                    ..watermark
                };
                self.replay_drop_column_indexes(
                    &source,
                    &columns,
                    &mut targets,
                    job,
                    watermark,
                    read,
                    Some(online_copy_deadline()),
                )?;
                if job.changes.len() != 0 {
                    return Err(error(
                        crate::error::catalog::ER_TABLE_DEF_CHANGED,
                        "source writes remained active at DROP COLUMN publication",
                    ));
                }
                for target in &targets {
                    if target.null_key_groups != 0 {
                        return Err(error(
                            crate::error::catalog::ER_INVALID_USE_OF_NULL,
                            "Invalid use of NULL value",
                        ));
                    }
                    if target.duplicate_key_groups != 0 {
                        return Err(error(
                            crate::error::catalog::ER_DUP_ENTRY,
                            format!("Duplicate entry in rebuilt index '{}'", target.index.name),
                        ));
                    }
                    target.file.as_ref().unwrap().flush()?;
                }
                Self::enter_alter_copy_finalizing(job)?;
                let actual = self.table(name)?;
                if actual.id != source.id
                    || actual.schema_version != source.schema_version
                    || actual.active_generation != source.active_generation
                {
                    return Err(error(
                        crate::error::catalog::ER_TABLE_DEF_CHANGED,
                        "DROP COLUMN source definition changed",
                    ));
                }
                let boundary = self.next_scn_atomic.load(Ordering::Acquire);
                for index in &mut candidate.indexes {
                    if replaced
                        .iter()
                        .any(|replacement| replacement.index_id == index.index_id)
                    {
                        index.readable_from_scn = boundary;
                    }
                }
                if candidate.primary_key != source.primary_key {
                    candidate.primary_definition_scn = boundary;
                }
                candidate.stats.analyzed_at_unix_ms = None;
                candidate.stats.modified_rows_since_analyze = candidate
                    .stats
                    .modified_rows_since_analyze
                    .saturating_add(1);
                {
                    let mut catalog = self.catalog_write()?;
                    catalog
                        .tables
                        .insert(source.qualified_name(), Arc::new(candidate.clone()));
                    build_fk_runtime(&mut catalog);
                }
                self.note_catalog_table(source.id);
                let catalog = (|| {
                    let mut catalog = self.prepare_index_retirement(&retirements[0])?;
                    for retirement in &retirements[1..] {
                        self.add_index_retirement(&mut catalog, retirement)?;
                    }
                    Ok::<_, QueryError>(catalog)
                })();
                let catalog = match catalog {
                    Ok(catalog) => catalog,
                    Err(err) => {
                        let mut catalog = self.catalog_write()?;
                        catalog
                            .tables
                            .insert(source.qualified_name(), source.clone());
                        build_fk_runtime(&mut catalog);
                        Self::take_catalog_mutations();
                        return Err(err);
                    }
                };
                publication_started = true;
                catalog.commit_ddl()?;
                #[cfg(test)]
                if let Some(hook) = self.vector_copy_committed_test_hook.lock().unwrap().take() {
                    hook.pause();
                }
                for old in &obsolete {
                    for id in source.storage_ids() {
                        // Detach by stable owner while the source is quiescent.
                        // Buffer-pool walks and spatial tree destruction follow
                        // the barrier; the cleanup guard prevents early unlink.
                        let handle = {
                            let mut cache = self.index_cache.write();
                            self.invalidate_storage_handle_open_epoch(id);
                            cache.remove(&(id, normalize_name(&old.name)))
                        };
                        if let Some(handle) = handle {
                            handle.write().retain_buffered_pages_on_drop();
                            retired_handles.push(handle);
                        }
                    }
                    if old.is_vector() {
                        let normalized = normalize_name(&old.name);
                        let mut graphs = self.vector_indexes.write();
                        let keys = graphs
                            .keys()
                            .filter(|(table, _, _, name)| {
                                *table == source.id && *name == normalized
                            })
                            .cloned()
                            .collect::<Vec<_>>();
                        for key in keys {
                            if let Some(graph) = graphs.remove(&key) {
                                graph.revoke();
                                retired_graphs.push(graph);
                            }
                        }
                        self.vector_build_epoch.fetch_add(1, Ordering::AcqRel);
                    }
                }
                for target in &mut targets {
                    self.publish_index_handle(
                        target.table.id,
                        &target.index.name,
                        Arc::new(parking_lot::RwLock::new(target.file.take().unwrap())),
                    );
                }
                self.refresh_heap_codec(source.id)?;
                for id in &ids {
                    self.table_locks_sharded
                        .set_schema_floor(*id, candidate.schema_version);
                }
                self.read_definition_highwater
                    .fetch_max(boundary, Ordering::Release);
                self.primary_definition_highwater
                    .fetch_max(candidate.primary_definition_scn, Ordering::Release);
                published = true;
                return Ok(());
            }
            unreachable!("bounded publication loop either returns or reports non-convergence")
        })();
        drop(mdl);
        drop(data);
        self.notify_non_row_lock_waiters();
        for handle in retired_handles {
            handle.write().discard_without_flush();
        }
        drop(retired_graphs);
        drop(retirement_guard);
        let mut result = result;
        if publication_started && !published {
            if let Err(err) = &result {
                self.quarantine_mvcc(format!(
                    "online DROP COLUMN publication needs recovery: {}",
                    err.message()
                ));
            }
        }
        if !publication_started && self.ensure_operational().is_ok() {
            if result.as_ref().err().is_some_and(|err| err.code() == 1062) {
                for target in &targets {
                    if let Err(err) = Self::finish_online_index_validation(target) {
                        result = Err(err);
                        break;
                    }
                }
            }
            targets.clear();
            let _cancel =
                crate::engine::cancel::enter(crate::engine::cancel::QueryCancelToken::noop());
            let cleanup =
                crate::engine::executor::mem_budget::with_critical_memory_reservations(|| {
                    for (intent, path) in &private {
                        if !self.purge_dropped_table_files(intent) {
                            return Err(error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "online DROP COLUMN private-index cleanup failed",
                            ));
                        }
                        self.remove_drop_table_intent(path.as_ref());
                    }
                    Ok::<_, QueryError>(())
                });
            if let Err(err) = cleanup {
                self.quarantine_mvcc(err.message().to_owned());
                result = Err(err);
            }
        } else if published {
            for (_, path) in &private {
                self.remove_drop_table_intent(path.as_ref());
            }
            self.request_catalog_retirement_cleanup();
        }
        if let Some(job) = job {
            self.unregister_alter_copy_job(job.table_id, result.as_ref().err());
        }
        result
    }

    fn build_drop_column_indexes(
        &self,
        source: &TableSchema,
        replacements: &[IndexSchema],
        columns: &[usize],
        read: TxnView,
        targets: &mut Vec<OnlineIndexTarget>,
        job: &Arc<AlterCopyJob>,
    ) -> Result<(), QueryError> {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        let _generation = btree_logical_wal::LsnScope::new(Some(self.log.lock().last_lsn()));
        for id in source.storage_ids() {
            let mut leaf = source.clone();
            leaf.id = id;
            leaf.partitioning = None;
            leaf.indexes = replacements
                .iter()
                .cloned()
                .map(|mut index| {
                    index.unique = false;
                    index.primary = false;
                    index
                })
                .collect();
            let mut builders = DdlCopyIndexBuilders::new(&leaf)?;
            let plans = builders.key_plans(&leaf);
            let mut values = vec![Value::Null; source.columns.len()];
            let _values = ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::DdlCopy,
                values.capacity() * std::mem::size_of::<Value>(),
            )?;
            let pages = self.with_heap_read(id, |heap| heap.page_count())?;
            for page in 0..pages {
                Self::check_alter_copy_cancelled(job)?;
                let mut rows = Vec::new();
                let mut row_ids = Vec::new();
                let mut charge =
                    ControlledMemoryCharge::try_new(ControlledMemoryCategory::DdlCopy, 0)?;
                self.scan_rows_columns_storage_streaming_range_with_ids(
                    source,
                    id,
                    read,
                    columns,
                    page,
                    page + 1,
                    |row_id, projected| {
                        for (&position, value) in columns.iter().zip(projected) {
                            values[position] = value.clone();
                        }
                        let keys = DdlCopyIndexBuilders::prepare_keys(&leaf, &plans, &mut values)?;
                        let row = DdlCopyPreparedRow {
                            source_row: row_id,
                            values: None,
                            payload: Vec::new(),
                            keys,
                        };
                        if rows.len() == rows.capacity() {
                            let additional = rows.capacity().max(8);
                            charge.try_grow(additional.saturating_mul(
                                std::mem::size_of::<DdlCopyPreparedRow>()
                                    + std::mem::size_of::<RowId>(),
                            ))?;
                            rows.reserve_exact(additional);
                            row_ids.reserve_exact(additional);
                        }
                        charge.try_grow(row.capacity_bytes())?;
                        rows.push(row);
                        row_ids.push(row_id);
                        Ok(())
                    },
                )?;
                Self::advance_alter_copy_job_copied_by(job, rows.len() as u64);
                builders.push_prepared_batch(self.data_dir(), rows, &row_ids)?;
            }
            for (name, file) in builders.finish_private(self, &leaf)? {
                let index = replacements
                    .iter()
                    .find(|index| index.name == name)
                    .unwrap()
                    .clone();
                let capture = self.index_handle(id, &name)?;
                let mut target = OnlineIndexTarget {
                    table: leaf.clone(),
                    index,
                    log: Arc::new(OnlineIndexBuildLog::new()),
                    _capture: capture,
                    file: Some(file),
                    validation_initialized: false,
                    null_key_groups: 0,
                    duplicate_key_groups: 0,
                };
                Self::initialize_online_index_validation(&mut target)?;
                target.validation_initialized = true;
                targets.push(target);
            }
        }
        Ok(())
    }

    fn replay_drop_column_indexes(
        &self,
        source: &TableSchema,
        columns: &[usize],
        targets: &mut [OnlineIndexTarget],
        job: &AlterCopyJob,
        before: TxnView,
        after: TxnView,
        deadline: Option<Instant>,
    ) -> Result<(), QueryError> {
        let ready = self.take_online_copy_ready_rows(job, after)?;
        let mut bytes = 0;
        let mut values = vec![Value::Null; source.columns.len()];
        // Delete the complete cohort first so unique-key permutations and
        // multiple changed rows cannot conflict with their old images.
        for (read, kind) in [
            (before, WalIndexOpKind::Delete),
            (after, WalIndexOpKind::Insert),
        ] {
            for cohort in ready.rows.chunk_by(|left, right| left.0 == right.0) {
                let storage_id = cohort[0].0;
                let start = targets.partition_point(|target| target.table.id < storage_id);
                let end = targets.partition_point(|target| target.table.id <= storage_id);
                let targets = &mut targets[start..end];
                if targets.is_empty() {
                    return Err(error(
                        crate::error::catalog::ER_TABLE_DEF_CHANGED,
                        "DROP COLUMN captured a row outside its source partitions",
                    ));
                }
                for batch in cohort.chunks(256) {
                    for &(id, row_id) in batch {
                        Self::check_alter_copy_cancelled(job)?;
                        check_online_copy_deadline(deadline)?;
                        if let Some(projected) =
                            self.read_row_columns_at(id, source, row_id, read, columns)?
                        {
                            check_online_copy_tail_value(&projected, deadline, &mut bytes)?;
                            for (&position, value) in columns.iter().zip(projected) {
                                values[position] = value;
                            }
                            for target in targets.iter() {
                                target.log.record(
                                    source
                                        .index_keys(&target.index, &values)?
                                        .into_iter()
                                        .map(|key| IndexBatchEntry { kind, key, row_id }),
                                );
                            }
                        }
                    }
                    self.replay_online_index_targets(targets, true)?;
                }
            }
        }
        check_online_copy_deadline(deadline)?;
        Ok(())
    }
}

fn drop_column_index_columns(
    source: &TableSchema,
    indexes: &[IndexSchema],
) -> Result<Vec<usize>, QueryError> {
    let mut columns = BTreeSet::new();
    for index in indexes {
        for (position, name) in index.columns.iter().enumerate() {
            if let Some(Some(expression)) = index.key_expressions.get(position) {
                for name in &expression.referenced_columns {
                    columns.insert(source.column_index(name)?);
                }
            } else {
                columns.insert(source.column_index(name)?);
            }
        }
        for name in &index.included_columns {
            columns.insert(source.column_index(name)?);
        }
        if let Some(predicate) = &index.predicate {
            for name in &predicate.referenced_columns {
                columns.insert(source.column_index(name)?);
            }
        }
    }
    Ok(columns.into_iter().collect())
}

#[cfg(test)]
mod online_drop_column_tests {
    use super::*;
    use crate::engine::{QueryResponse, SessionContext, StorageQueryEngine};

    #[test]
    fn drop_column_drains_source_commits_behind_an_unrelated_frontier_gap() {
        for after_snapshot in [false, true] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let engine = Arc::new(
                StorageQueryEngine::open_with_storage_config_without_background_workers(
                    dir.path(),
                    StorageConfig {
                        durability: DurabilityMode::Relaxed,
                        checkpoint_interval: Duration::from_secs(60),
                        checkpoint_wal_bytes: 64 << 20,
                        lock_wait: Duration::from_secs(1),
                        group_commit_window: Duration::ZERO,
                        wal_segment_bytes: 64 << 20,
                        buffer_pool_pages: 2048,
                        buffer_pool_io_threads: 1,
                        buffer_pool_prefetch_pages: 2,
                        undo_retention: Duration::from_secs(60),
                        heap_update_reserve_bytes: 2048,
                        startup_validation: StartupValidationMode::Auto,
                        startup_validate_indexes: true,
                    },
                )
                .unwrap(),
            );
            engine
                .execute("CREATE TABLE t(id INT PRIMARY KEY,a INT,b INT,UNIQUE ab(a,b))")
                .unwrap();
            engine.execute("INSERT INTO t VALUES(1,10,100)").unwrap();
            engine
                .execute("CREATE TABLE unrelated(id INT PRIMARY KEY,v INT)")
                .unwrap();
            engine.execute("INSERT INTO unrelated VALUES(1,0)").unwrap();
            let stalled = Arc::new(DdlBackfillTestHook::new());
            let wait = stalled.clone();
            let once = AtomicBool::new(false);
            let (allocated, allocation) = std::sync::mpsc::channel();
            commit_visibility_test_hook::install(Arc::new(move |_| {
                if !once.swap(true, Ordering::AcqRel) {
                    wait.pause();
                } else {
                    let _ = allocated.send(());
                }
            }));
            let writer = engine.clone();
            let unrelated = std::thread::spawn(move || {
                let mut session = SessionContext::default();
                session.connection_id = 29911;
                writer
                    .execute_with_session(&mut session, "UPDATE unrelated SET v=1")
                    .unwrap();
            });
            assert!(stalled.wait_until_reached(Duration::from_secs(10)));
            let frontier = engine.database().visible_scn();
            let hook = after_snapshot.then(|| engine.database().install_ddl_backfill_test_hook());
            let (send, receive) = std::sync::mpsc::channel();
            let start_ddl = || {
                let worker = engine.clone();
                let send = send.clone();
                std::thread::spawn(move || {
                    let mut session = SessionContext::default();
                    session.connection_id = 29912;
                    send.send(
                        worker
                            .execute_with_session(&mut session, "ALTER TABLE t DROP COLUMN a")
                            .map(|_| ()),
                    )
                    .unwrap();
                })
            };
            let mut ddl = after_snapshot.then(&start_ddl);
            if let Some(hook) = &hook {
                assert!(hook.wait_until_reached(Duration::from_secs(10)));
            }
            let worker = engine.clone();
            let source_writer = std::thread::spawn(move || {
                let mut session = SessionContext::default();
                session.connection_id = 29913;
                worker
                    .execute_with_session(&mut session, "UPDATE t SET b=101 WHERE id=1")
                    .unwrap();
            });
            allocation.recv_timeout(Duration::from_secs(10)).unwrap();
            if ddl.is_none() {
                ddl = Some(start_ddl());
            }
            if let Some(hook) = hook {
                hook.release();
            }
            // Commit acknowledgement retains the source data lock until the
            // global frontier closes. Both prepare and final publication must
            // drain that writer rather than silently omit its version.
            let early = receive.recv_timeout(Duration::from_millis(30));
            let still_stalled = engine.database().visible_scn() == frontier;
            stalled.release();
            unrelated.join().unwrap();
            source_writer.join().unwrap();
            ddl.unwrap().join().unwrap();
            commit_visibility_test_hook::clear();
            assert!(still_stalled, "fixture did not retain its frontier gap");
            assert!(
                early.is_err(),
                "DDL passed a source writer waiting for commit visibility"
            );
            receive
                .recv_timeout(Duration::from_secs(10))
                .unwrap()
                .unwrap();
            let QueryResponse::ResultSet(result) = engine
                .execute("SELECT id,b FROM t FORCE INDEX(ab) WHERE b=101")
                .unwrap()
                .materialize()
                .unwrap()
            else {
                panic!("rows")
            };
            assert_eq!(result.rows, vec![vec![Value::Int(1), Value::Int(101)]]);
        }
    }
}
