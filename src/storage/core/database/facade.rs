use crate::wal::WalRecordType as ReplayType;
fn collect_auto_increment_advances_source(
    records: &(impl crate::wal::WalRecordSource + ?Sized),
) -> Result<BTreeMap<u64, (u64, u64, u64)>, QueryError> {
    let mut out = BTreeMap::new();
    let mut covered_lsn = 0;
    for record in records.records().only(&[ReplayType::CheckpointEnd]) {
        if let WalPayload::CheckpointEnd {
            auto_increment_covered_lsn,
            ..
        } = record?.payload
        {
            covered_lsn = covered_lsn.max(auto_increment_covered_lsn);
        }
    }
    for record in records.records().only(&[ReplayType::AutoIncrementAdvance]) {
        let record = record?;
        if record.lsn < covered_lsn {
            continue;
        }
        if let WalPayload::AutoIncrementAdvance {
            table_id,
            version,
            max_value,
        } = &record.payload
        {
            let candidate = (*version, *max_value, record.lsn);
            out.entry(*table_id)
                .and_modify(|current: &mut (u64, u64, u64)| {
                    if candidate.0 > current.0
                        || (candidate.0 == current.0 && candidate.1 > current.1)
                    {
                        *current = candidate;
                    }
                })
                .or_insert(candidate);
        }
    }
    Ok(out)
}

#[derive(Default)]
struct PreparedRecoveryMetadata {
    prepare_scn: HashMap<u64, u64>,
    mdl: HashMap<u64, Vec<crate::wal::PreparedMdlEntry>>,
    binlog: HashMap<u64, crate::wal::PreparedBinlogRef>,
}

fn collect_prepared_metadata_source(
    records: &(impl crate::wal::WalRecordSource + ?Sized),
) -> Result<PreparedRecoveryMetadata, QueryError> {
    let mut prepared = PreparedRecoveryMetadata::default();
    // Decode potentially large PREPARE lock snapshots only once.
    for record in records.records().only(&[
        ReplayType::PrepareTxn,
        ReplayType::CommitTxn,
        ReplayType::AbortTxn,
        ReplayType::EndTxn,
    ]) {
        let record = record?;
        match &record.payload {
            WalPayload::PrepareTxn {
                prepare_scn, mdl_locks, binlog, ..
            } => {
                prepared.prepare_scn.insert(record.txn_id, *prepare_scn);
                if mdl_locks.is_empty() {
                    prepared.mdl.remove(&record.txn_id);
                } else {
                    prepared.mdl.insert(record.txn_id, mdl_locks.clone());
                }
                if let Some(intent) = binlog {
                    prepared.binlog.insert(record.txn_id, intent.clone());
                } else {
                    prepared.binlog.remove(&record.txn_id);
                }
            }
            _ => {
                prepared.prepare_scn.remove(&record.txn_id);
                prepared.mdl.remove(&record.txn_id);
                prepared.binlog.remove(&record.txn_id);
            }
        }
    }
    Ok(prepared)
}

/// Recover the transaction-id prefix whose terminal state is already folded
/// into the checkpointed data files.  A collapsed, quiescent WAL contains
/// only `CheckpointEnd`; without restoring this prefix the runtime watermark
/// stays at zero forever (there are no committed slots left for slot GC to
/// remove), disabling the B-tree `page_max_trx_id` visibility shortcut after
/// every clean restart.
///
/// A non-quiescent checkpoint can retain an older active/prepared/rolled-back
/// slot.  Such a slot must remain above the recovered prefix: index entries
/// written by it are not unconditionally visible even if a later checkpoint
/// allocated larger transaction ids.
fn recovered_committed_txn_prefix_source(
    records: &(impl crate::wal::WalRecordSource + ?Sized),
    txns: &TransactionManager,
) -> Result<u64, QueryError> {
    let mut checkpoint_prefix = 0;
    for record in records.records().only(&[ReplayType::CheckpointEnd]) {
        if let WalPayload::CheckpointEnd { next_txn_id, .. } = record?.payload {
            checkpoint_prefix = next_txn_id.saturating_sub(1);
        }
    }
    let first_non_committed = txns
        .slots
        .values()
        .filter(|slot| slot.status != TxnStatus::Committed)
        .map(|slot| slot.txn_id)
        .min();
    Ok(first_non_committed
        .map(|txn_id| checkpoint_prefix.min(txn_id.saturating_sub(1)))
        .unwrap_or(checkpoint_prefix))
}

const ALTER_COPY_JOB_DIR: &str = "ddl/alter_copy";

fn alter_copy_job_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(ALTER_COPY_JOB_DIR)
}

fn incomplete_alter_copy_job_paths(data_dir: &Path) -> Result<Vec<PathBuf>, QueryError> {
    let dir = alter_copy_job_dir(data_dir);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(io_err(err)),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let path = entry.map_err(io_err)?.path();
        // Staging files precede intent publication and all heap mutations.
        // A partially written .job.tmp is therefore safe to ignore on restart.
        if path.is_file() && path.extension().is_some_and(|extension| extension == "job") {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn reject_incomplete_alter_copy_jobs(data_dir: &Path) -> Result<(), QueryError> {
    let paths = incomplete_alter_copy_job_paths(data_dir)?
        .into_iter()
        .filter(|path| {
            fs::read(path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<AlterCopyIntent>(&bytes).ok())
                .is_none()
        })
        .collect::<Vec<_>>();
    if paths.is_empty() {
        return Ok(());
    }
    let markers = paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Err(error(crate::error::catalog::ER_UNKNOWN_ERROR,
        format!(
            "incomplete ALTER TABLE COPY job detected at startup ({markers}); \
             table files may be inconsistent, so mydb refuses to open this data directory until it is manually repaired"
        ),
    ))
}

impl Database {
    pub fn open(data_dir: impl Into<PathBuf>) -> Result<Self, QueryError> {
        Self::open_with_config(data_dir, StorageConfig::from_env())
    }

    pub(crate) fn global_read_lock_scope(&self) -> u64 {
        self.visibility_cache_id
    }

    /// Shared handle to the WAL log manager. Used by the mydb-native
    /// physical replication module (`src/repl/`) so the master-side
    /// sender can stream raw frames and the replica-side applier can
    /// append received frames via the standard `LogManager` API.
    pub fn wal_log(&self) -> std::sync::Arc<crate::wal::LogMutex> {
        std::sync::Arc::clone(&self.log)
    }

    pub fn open_with_config(
        data_dir: impl Into<PathBuf>,
        config: StorageConfig,
    ) -> Result<Self, QueryError> {
        Self::open_impl(data_dir.into(), config, RecoveryContext::default(), None)
    }

    pub fn open_image(data_dir:impl Into<PathBuf>,runtime_directory:PathBuf)->Result<Self,QueryError>{
        let data_dir=data_dir.into();
        if !crate::storage::image::is_snapshot(&data_dir)?{return Err(error(crate::error::catalog::MYDB_ER_BACKUP_FAILED,"direct image open requires a completed read-only image"));}
        let mut context=RecoveryContext::default();context.mode=DatabaseOpenMode::ReadOnlyImage;
        Self::open_impl(data_dir,StorageConfig::from_env(),context,Some(runtime_directory))
    }

    pub(crate) fn open_for_backup_preparation(
        data_dir: &Path,
        context: RecoveryContext,
    ) -> Result<Self, QueryError> {
        if context.mode != DatabaseOpenMode::SealBaseline {
            return Err(error(
                crate::error::catalog::MYDB_ER_BACKUP_FAILED,
                "baseline preparation requires a SealBaseline recovery context",
            ));
        }
        let mut config = StorageConfig::from_env();
        config.durability = DurabilityMode::Strict;
        config.startup_validation = StartupValidationMode::Full;
        config.startup_validate_indexes = true;
        Self::open_impl(data_dir.to_path_buf(), config, context, None)
    }

    pub(crate) fn open_for_archive_restore(
        data_dir: &Path,
        mut config: StorageConfig,
        context: RecoveryContext,
    ) -> Result<Self, QueryError> {
        if context.mode != DatabaseOpenMode::RestoreWorkspace {
            return Err(error(
                crate::error::catalog::MYDB_ER_BACKUP_FAILED,
                "archive restoration requires a RestoreWorkspace recovery context",
            ));
        }
        config.durability = DurabilityMode::Strict;
        Self::open_impl(data_dir.to_path_buf(), config, context, None)
    }

    fn open_impl(
        mut data_dir: PathBuf,
        mut config: StorageConfig,
        context: RecoveryContext,
        runtime_directory:Option<PathBuf>,
    ) -> Result<Self, QueryError> {
        let preparing = matches!(
            context.mode,
            DatabaseOpenMode::SealBaseline | DatabaseOpenMode::RestoreWorkspace
        );
        let startup_started = Instant::now();
        let native_snapshot = crate::storage::image::inspect(&data_dir)?;
        if let Some(image)=&native_snapshot{
            if image.state!=crate::storage::image::State::Writable||crate::startup_config::var("MYDB_UNDO_RETENTION_MS").is_err(){config.undo_retention=Duration::from_millis(image.manifest.history_retention_ms);}
        }
        let native_clock = native_snapshot
            .as_ref()
            .map_or(0, |snapshot| snapshot.manifest.end_commit_unix_us);
        let snapshot_read_only = match native_snapshot {
            Some(snapshot) if snapshot.state == crate::storage::image::State::Preparing => {
                if !preparing {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "native snapshot preparation is incomplete",
                    ));
                }
                false
            }
            Some(snapshot) => snapshot.state == crate::storage::image::State::ReadOnly,
            None => false,
        };
        if snapshot_read_only{data_dir=fs::canonicalize(&data_dir).map_err(io_err)?;}
        let image_repository_lease=if snapshot_read_only {
            if let Some(repository)=crate::storage::image_io::repository_ancestor(&data_dir){
                if context.mode!=DatabaseOpenMode::ReadOnlyImage{return Err(error(crate::error::catalog::MYDB_ER_BACKUP_FAILED,"repository bases require --backup-repository, --base and an external --runtime-dir"));}
                let file=std::fs::File::open(repository.join("gc.lock")).map_err(io_err)?;
                file.try_lock_shared().map_err(|e|error(crate::error::catalog::MYDB_ER_BACKUP_FAILED,format!("repository GC is busy: {e}")))?;Some(file)
            }else{None}
        }else{None};
        let snapshot_runtime = if snapshot_read_only {
            if config.durability == DurabilityMode::Memory {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "native snapshots cannot open in memory durability mode",
                ));
            }
            data_dir = fs::canonicalize(&data_dir).map_err(io_err)?;
            Some(if let Some(runtime)=runtime_directory.as_ref(){crate::storage::image_io::SnapshotRuntime::open_with_temp_dir(&data_dir,runtime)?}else{crate::storage::image_io::SnapshotRuntime::open(&data_dir)?})
        } else {
            None
        };
        #[cfg(test)]
        let seeded_test_directory = seed_empty_test_data_dir(&data_dir, config.durability)?;
        // The freshly cloned seed is still private fixture construction.
        // Drain each file normally, then finish its device barrier before
        // exposing the database. Existing directories retain normal recovery.
        #[cfg(test)]
        let fixture_sync = if seeded_test_directory && config.durability == DurabilityMode::Strict {
            Some(crate::platform::fs::FixtureSyncBatch::new(&data_dir).map_err(io_err)?)
        } else {
            None
        };
        if !snapshot_read_only {
            fs::create_dir_all(&data_dir).map_err(io_err)?;
            // Reject pre-v33 storage layouts BEFORE creating any new
            // directories — otherwise the `mkdir_p schemas/`/`system/`
            // calls below would set the "has_new" tell that the rejection
            // logic uses to distinguish an in-progress new layout from a
            // legacy snapshot.
            reject_legacy_storage(&data_dir)?;
            // Global service directories: WAL, undo, replication, scratch tmp.
            fs::create_dir_all(crate::storage::naming::system_wal_dir(&data_dir))
                .map_err(io_err)?;
            fs::create_dir_all(crate::storage::naming::system_undo_dir(&data_dir))
                .map_err(io_err)?;
            fs::create_dir_all(crate::storage::naming::system_replication_dir(&data_dir))
                .map_err(io_err)?;
            fs::create_dir_all(crate::storage::naming::system_tmp_dir(&data_dir))
                .map_err(io_err)?;
            // Take the exclusive data-directory lock before touching the WAL:
            // a second process (e.g. a restart racing the previous process's
            // final-checkpoint WAL collapse) must not run recovery against a
            // directory another live mydb is still mutating. Released when the
            // `Database` drops (or the process exits / re-execs).
        }
        let dir_lock = crate::storage::dir_lock::DataDirLock::acquire(&data_dir)?;
        if !snapshot_read_only {
            crate::storage::image::cleanup_abandoned_staging(&data_dir)?;
            crate::repl::base_backup::cleanup_abandoned_snapshots(&data_dir)?;
        }
        if snapshot_read_only {
            crate::storage::image::validate_open(&data_dir)?;
        }
        if !snapshot_read_only {
            // Per-database object directories root. Individual `<db>/{tables,
            // indexes}/` subdirs are created lazily by `create_database` and
            // by the first table create that lands in a db.
            fs::create_dir_all(crate::storage::naming::schemas_root(&data_dir)).map_err(io_err)?;
            fs::create_dir_all(crate::storage::naming::objects_root(&data_dir)).map_err(io_err)?;
            fs::create_dir_all(crate::storage::naming::system_catalog_dir(&data_dir))
                .map_err(io_err)?;
            // Eagerly create the system-schema dirs so dictionary heaps and
            // their indexes can be opened during bootstrap before the user
            // catalog is loaded.
            fs::create_dir_all(crate::storage::naming::schema_tables_dir(
                &data_dir, "mysql",
            ))
            .map_err(io_err)?;
            fs::create_dir_all(crate::storage::naming::schema_indexes_dir(
                &data_dir, "mysql",
            ))
            .map_err(io_err)?;
            // Drop any leftover temp-table / temp-index sidecars from a
            // previous process. (Permanent dirs are recreated above.)
            let _ = fs::remove_dir_all(crate::storage::naming::temp_tables_dir(&data_dir));
            let _ = fs::remove_dir_all(crate::storage::naming::temp_indexes_dir(&data_dir));
            // Clean up hash join spill temp directories left by an abnormally exited previous process.
            crate::engine::executor::select::grace::cleanup_orphan_tmp_dirs(&data_dir);
        }
        if data_dir.join("system/index-maintenance.failed").try_exists().map_err(io_err)? {
            return Err(error(crate::error::catalog::ER_INDEX_CORRUPT,
                "persistent vector maintenance failure requires explicit repair; recovery has preserved the failed files"));
        }
        if config.durability != DurabilityMode::Memory {
            validate_heap_file_lengths(&data_dir)?;
        }
        let capture = crate::storage::capture::CaptureState::open(&data_dir, snapshot_read_only)?;
        if capture.floor().is_some()&&config.durability==DurabilityMode::Memory{return Err(error(crate::error::catalog::MYDB_ER_BACKUP_FAILED,"a WAL-protected source cannot open in memory durability mode"));}
        if let Some(retention)=capture.protected_history_retention_ms()?{
            if config.undo_retention!=Duration::from_millis(retention){log_message(format_args!("mydb: restoring protected temporal history retention {retention}ms before WAL recovery"));}
            config.undo_retention=Duration::from_millis(retention);
        }
        let t = Instant::now();
        let recovery_result = if snapshot_read_only {
            let scan =
                LogManager::scan_replay_with_mode(&data_dir, crate::wal::WalReadMode::ReadOnly)?;
            let source = crate::storage::snapshot::SourceVisibility::from_wal(&scan.records)?;
            let image = crate::storage::image::inspect(&data_dir)?.ok_or_else(|| error(crate::error::catalog::MYDB_ER_BACKUP_FAILED, "missing image"))?;
            if source != image.seal.as_ref().ok_or_else(|| error(crate::error::catalog::MYDB_ER_BACKUP_FAILED,"missing seal"))?.visibility_state {
                return Err(error(crate::error::catalog::MYDB_ER_BACKUP_FAILED,"image transaction state mismatch"));
            }
            RecoveryResult {
                summary: RecoverySummary {
                    single_checkpoint_end_record: scan.records.len() == 1,
                    scanned_records: scan.records.len(),
                    ..Default::default()
                },
                scan: Some(scan),
                wal_modified: false,
            }
        } else {
            recover_from_wal_with_options(
                &data_dir,
                config.durability,
                config.startup_validation != StartupValidationMode::Off
                    && config.startup_validate_indexes,
                context,
            )?
        };
        let wal_recovery_us = t.elapsed().as_micros() as u64;
        reject_incomplete_alter_copy_jobs(&data_dir)?;
        let mut recovery = recovery_result.summary;
        let clean_checkpoint_startup =
            config.durability != DurabilityMode::Memory && recovery.is_clean_checkpoint_startup();
        let scan = match (recovery_result.scan, recovery_result.wal_modified) {
            (Some(scan), false) => scan,
            (stale_scan, _) => {
                // The match scrutinee otherwise retains the old payloads
                // throughout the rescan after REDO/UNDO appended WAL.
                drop(stale_scan);
                LogManager::scan_replay_with_mode(&data_dir, context.wal_read_mode())?
            }
        };
        let physical_replica = crate::repl::role::physical_replica_on_disk(&data_dir);
        let mut primary_publications = HashSet::new();
        let mut commit_hlc_wal = 0;
        for record in scan.records.records().only(&[
            ReplayType::DdlCommit,
            ReplayType::CommitTxn,
            ReplayType::SequenceState,
        ]) {
            let record = record?;
            match record.payload {
                WalPayload::DdlCommit { job_id, .. }
                    if job_id & PRIMARY_PUBLICATION_JOB_TAG != 0 =>
                {
                    primary_publications.insert(job_id);
                }
                WalPayload::SequenceState { logged_unix_us, .. } => {
                    commit_hlc_wal = commit_hlc_wal.max(logged_unix_us);
                }
                WalPayload::CommitTxn { commit_unix_us, .. } => {
                    commit_hlc_wal = commit_hlc_wal.max(commit_unix_us);
                }
                _ => {}
            }
        }
        let replica_applied_seed = if physical_replica {
            scan.next_lsn.saturating_sub(1)
        } else {
            0
        };
        // Recovery can touch undo/transaction metadata even when page redo is
        // zero (for example a loser-only tail). Conservatively make the first
        // restartpoint after every replica open sync the full data tree before
        // it is allowed to prune received WAL.
        let replica_needs_full_sync = physical_replica;
        let runtime_init_started = Instant::now();
        let auto_increment_recovery = collect_auto_increment_advances_source(&scan.records)?;
        let sequence_recovery = collect_sequence_values_source(&scan.records)?;
        let sequence_coverage = sequence_coverage_source(&scan.records)?;
        let temporal_history = TemporalHistory::open(&data_dir, config.durability, &scan.records)?;
        let commit_hlc_seed = temporal_history
            .earliest_time()
            .unwrap_or(0)
            .max(commit_hlc_wal)
            .max(native_clock)
            .max(crate::wal::read_source_clock(&data_dir)?);
        let prepared_metadata = collect_prepared_metadata_source(&scan.records)?;
        let (txns, lock_manager) = TransactionManager::from_wal_source(&scan.records)?;
        let min_retained_txn_id_seed = recovered_committed_txn_prefix_source(&scan.records, &txns)?;
        let mut retained_undo_txns = txns
            .active_undo_heads_with_txn()
            .into_iter()
            .map(|(txn_id, _)| txn_id)
            .collect::<HashSet<_>>();
        let retain_since_us = (if context.is_restore() { context.target_time_us.unwrap_or(native_clock) / 1_000 } else { current_unix_ms() })
            .saturating_sub(config.undo_retention.as_millis().min(u128::from(u64::MAX)) as u64)
            .saturating_mul(1_000);
        retained_undo_txns.extend(temporal_history.txn_ids_committed_since(retain_since_us));
        let runtime_config = RuntimeStorageConfig::from_storage_config(&config);
        // M3.1 - Use persistent undo journal when not running in Memory mode.
        let undo_started = Instant::now();
        let mut undo = if snapshot_read_only {
            UndoTablespace::open_read_only(&data_dir, runtime_config.undo_segment_bytes)?
        } else if config.durability == DurabilityMode::Memory {
            UndoTablespace::from_wal(&scan.records)?
        } else {
            if context.mode == DatabaseOpenMode::SealBaseline || (!context.is_restore() && capture.floor().is_some()) {
                UndoTablespace::open_preserving_source(
                    &scan.records,
                    &data_dir,
                    runtime_config.undo_segment_bytes,
                )?
            } else if physical_replica {
                // The source's future UndoRecord ids continue from the exact
                // segment/offset allocation captured by base backup. Ordinary
                // crash recovery is free to reclaim committed baseline undo,
                // but doing so on a physical replica resets an active segment
                // to offset 32 and makes the next replicated source frame look
                // like a multi-kilobyte hole. Preserve physical allocation;
                // the configured resident cache still evicts payloads.
                UndoTablespace::open_with_segments_for_replica(
                    &scan.records,
                    &data_dir,
                    runtime_config.undo_segment_bytes,
                )?
            } else {
                UndoTablespace::open_with_segments_for_recovery(
                    &scan.records,
                    &data_dir,
                    runtime_config.undo_segment_bytes,
                    &retained_undo_txns,
                    context.is_restore(),
                )?
            }
        };
        let undo_open_us = undo_started.elapsed().as_micros() as u64;
        undo.capture_active = Arc::clone(&capture.active);
        undo.archive_floor = Arc::clone(&capture.archive_floor);
        undo.set_tablespace_max_bytes(runtime_config.undo_tablespace_max_bytes);
        undo.set_mem_soft_limit_bytes(runtime_config.undo_mem_soft_limit_bytes);
        undo.set_mem_cache_limit_bytes(runtime_config.undo_mem_cache_limit_bytes);
        undo.set_txn_max_bytes(runtime_config.undo_txn_max_bytes);
        undo.set_retention_guarantee(runtime_config.undo_retention_guarantee);
        // Pin the WAL flush method before the first sync. Strict mode on
        // macOS resolves to F_FULLFSYNC (true power-loss durability);
        // MYDB_WAL_SYNC_METHOD / the mydb_wal_sync_method sysvar override.
        crate::wal::configure_wal_sync_method(config.durability == DurabilityMode::Strict);
        let (log, log_state_atomics) = {
            let mut log = LogManager::open_from_replay(&data_dir, &scan)?;
            log.set_physical_replica(physical_replica);
            log.set_local_recovery(context.is_restore());
            log.set_max_segment_bytes(config.wal_segment_bytes);
            // Coalesce tiny OLTP WAL frames into the configured write buffer for
            // BOTH strict and relaxed. Without it, relaxed wrote every frame
            // with its own pwrite() under the global log mutex (3-5 syscalls
            // per txn, all serialized), which inverted the expected ordering
            // — relaxed measured SLOWER than strict despite skipping fsync.
            // Relaxed's "survive process crash" contract is preserved because
            // `flush_commit_lsn` drains the buffer to the OS page cache (a
            // coalesced write, still no fsync) at each commit barrier before
            // the client is acked. Memory mode never persists, so it stays off.
            log.set_write_buffer_enabled(config.durability != DurabilityMode::Memory)?;
            if !context.is_restore()&&!snapshot_read_only {
                if let Some(floor)=capture.leases().get("archive").map(|l|l.floor).or_else(||capture.floor()){
                    let prefixes=log.pin_prefix_after(floor-1,None)?;let mut pending=0u64;
                    for prefix in &prefixes {crate::wal::visit_pinned_range(prefix,prefix.first_lsn.max(floor)-1,u64::MAX,|_,_,_,_,raw|{pending=pending.saturating_add(raw.len() as u64);Ok(())})?;}
                    capture.budget.pending.store(pending,Ordering::Release);
                }
                log.bind_wal_budget(Arc::clone(&capture.budget));
            }
            let atomics = log.state_atomics();
            (Arc::new(parking_lot::Mutex::new(log)), atomics)
        };
        // All transaction/undo/log consumers are finished. Only zone deltas
        // remain; release the general recovery directory before index builds.
        let zone_replay = scan.select(&[ReplayType::ZoneMapMergeBatch, ReplayType::VectorIndexBatch, ReplayType::VectorIndexModel, ReplayType::VectorIndexComplete, ReplayType::VectorIndexImage,
            ReplayType::CommitTxn, ReplayType::AbortTxn, ReplayType::EndTxn, ReplayType::RollbackTo])?;
        crate::engine::executor::mem_budget::validate_controlled_memory_split(
            runtime_config.buffer_pool_pages,
        )?;
        crate::engine::executor::mem_budget::set_controlled_buffer_limit_pages(
            runtime_config.buffer_pool_pages,
        );
        let buffer_pool = BufferPool::new_with_options(
            runtime_config.buffer_pool_pages,
            log.clone(),
            config.durability,
            config.lock_wait,
            config.buffer_pool_io_threads,
            config.buffer_pool_prefetch_pages,
        );
        let next_txn_id_seed = txns.next_txn_id;
        let next_scn_seed = txns.next_scn;
        let active_write_seed = txns.active_write_count;
        let index_repair_state = Arc::clone(&txns.index_repairs);
        let index_repair_version = Arc::clone(&txns.index_repair_version);
        let index_repair_epochs = Arc::clone(&txns.index_repair_epochs);
        let durability_worker =
            if !snapshot_read_only && crate::storage::txn::durability_worker_enabled() {
                DurabilityWorker::start(Arc::clone(&log))
            } else {
                DurabilityWorker::disabled()
            };
        let undo_append_shards = undo.concurrent_persistent_append_shards();
        let undo_resident_admission = undo.resident_admission();
        let undo_concurrent_append_eligible =
            undo.concurrent_persistent_append_eligibility_handle();
        let source_clock=log.lock().source_clock();
        source_clock.fetch_max(commit_hlc_seed,Ordering::Release);
        let mut db = Self {
            _snapshot_runtime: snapshot_runtime,
            _image_repository_lease: image_repository_lease,
            capture,
            services: Default::default(),
            open_mode: if snapshot_read_only {
                DatabaseOpenMode::ReadOnlyImage
            } else {
                context.mode
            },
            recovery_context: if snapshot_read_only{context.at_source_time(native_clock)}else{context},
            primary_definition_highwater: AtomicU64::new(0),
            read_definition_highwater: AtomicU64::new(0),
            visibility_cache_id: NEXT_VISIBILITY_CACHE_ID.fetch_add(1, Ordering::Relaxed),
            next_snapshot_id: AtomicU64::new(1),
            mvcc_quarantined: AtomicBool::new(false),
            mvcc_quarantine_reason: RwLock::new(None),
            commit_hlc_unix_us: source_clock,
            temporal_history,
            temporal_hlc_rollback_total: AtomicU64::new(0),
            data_dir,
            _dir_lock: dir_lock,
            // Placeholder until the authoritative catalog is loaded from the
            // data-dictionary heaps below (after recovery undo).
            catalog: CatalogManager::new(super::catalog::empty_catalog())?,
            auth_snapshot: arc_swap::ArcSwap::from_pointee(
                crate::engine::auth::AuthCatalog::empty(),
            ),
            temp_tables: RwLock::new(Default::default()),
            txns: parking_lot::RwLock::new(txns),
            unique_reservations: lock_manager.unique_reservation_filter(),
            lock_log_presence: lock_manager.lock_log_presence(),
            lock_manager: parking_lot::RwLock::new(lock_manager),
            mdl: MdlManager::default(),
            undo: UndoLatch::new(undo),
            undo_append_shards,
            undo_resident_admission,
            undo_append_next_shard: AtomicU64::new(0),
            undo_publish: UndoPublishQueue::default(),
            undo_concurrent_append_eligible,
            active_snapshots: (0..ACTIVE_SNAPSHOT_SHARDS)
                .map(|_| Mutex::new(Default::default()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            auto_increment: AutoIncrementManager::new(),
            auto_increment_checkpoint_fence: parking_lot::RwLock::new(()),
            auto_increment_dirty: Mutex::new(AutoIncrementDirtyQueue::new()?),
            sequence_wal_batch: crate::storage::sequence::sequence_wal_batch()?,
            sequence_covered_lsn: AtomicU64::new(sequence_coverage),
            sequence_metrics: SequenceMetrics::default(),
            sequence_checkpoint_fence: parking_lot::RwLock::new(()),
            sequence_dirty: parking_lot::Mutex::new(SequenceDirtyQueue::new()?),
            sequence_dirty_generations: AtomicUsize::new(0),
            sequence_sync: parking_lot::RwLock::new(
                crate::storage::sequence::SequenceSyncConfig::default(),
            ),
            sequences: crate::storage::sequence::SequenceManager::default(),
            lock_wait_mutex: Mutex::new(()),
            lock_wait_signal: Condvar::new(),
            row_lock_async_registry: Arc::new(crate::engine::api::AsyncRowLockRegistry::default()),
            lock_wait_waiters: AtomicU64::new(0),
            config: config.clone(),
            runtime_config: RwLock::new(runtime_config),
            buffer_pool,
            offline_index_buffer_leases: Mutex::new(Default::default()),
            foreground_bulk_ddl_active: AtomicU64::new(0),
            analyze_scan_active: AtomicU64::new(0),
            heap_cache: parking_lot::RwLock::new(Default::default()),
            storage_handle_open_epochs: Mutex::new(HashMap::new()),
            index_cache: parking_lot::RwLock::new(Default::default()),
            overflow_cache: parking_lot::RwLock::new(Default::default()),
            zone_map_gate: parking_lot::RwLock::new(()),
            backup_maintenance_gate: parking_lot::RwLock::new(()),
            zone_map_enabled: AtomicBool::new(true),
            zone_maps: RwLock::new(Default::default()),
            zone_map_load_locks: std::array::from_fn(|_| Mutex::new(None)),
            zone_map_retirement_epoch: AtomicU64::new(0),
            pending_vector_deletes: Mutex::new(Default::default()),
            vector_commits_publishing: parking_lot::Mutex::new(Default::default()),
            vector_recovery_active: AtomicBool::new(false),
            #[cfg(test)]
            vector_persist_failure: AtomicU64::new(0),
            vector_pending_wal: parking_lot::Mutex::new(BTreeMap::new()),
            vector_copy_publications: parking_lot::Mutex::new(HashSet::new()),
            replica_vector_state: parking_lot::Mutex::new(ReplicaVectorState::default()),
            replica_vector_signal: parking_lot::Condvar::new(),
            vector_retired_graphs: parking_lot::Mutex::new(HashMap::new()),
            vector_snapshot_dirty: parking_lot::Mutex::new(HashSet::new()),
            recovered_vector_completion: parking_lot::Mutex::new(()),
            hnsw_tombstones_rolled_back: AtomicU64::new(0),
            vector_indexes: parking_lot::RwLock::new(Default::default()),
            vector_build_deltas: parking_lot::Mutex::new(Default::default()),
            vector_builds_in_flight: AtomicUsize::new(0),
            vector_snapshot_floor: AtomicU64::new(0),
            vector_build_epoch: AtomicU64::new(0),
            replicated_hnsw_table_cache: parking_lot::RwLock::new(HashMap::new()),
            replicated_hnsw_dirty: parking_lot::Mutex::new(HashMap::new()),
            vector_build_locks: parking_lot::Mutex::new(Default::default()),
            vector_build_backoff: parking_lot::Mutex::new(Default::default()),
            vector_purge_queue: parking_lot::Mutex::new(VectorPurgeQueue::new()),
            vector_purge_snapshot_gate: parking_lot::RwLock::new(()),
            vector_purge_drain_gate: parking_lot::Mutex::new(()),
            vector_purge_recovery_charge: parking_lot::Mutex::new(None),
            vector_purges_pending: AtomicUsize::new(0),
            persisted_variables: std::sync::OnceLock::new(),
            log,
            log_state_atomics,
            group_commit: GroupCommitCoordinator::default(),
            flush_group_commit: GroupCommitCoordinator::default(),
            durability_worker,
            plan_cache_hits: AtomicU64::new(0),
            plan_cache_misses: AtomicU64::new(0),
            select_parse_cache_hits: AtomicU64::new(0),
            select_compiled_plan_hits: AtomicU64::new(0),
            select_operator_compiles: AtomicU64::new(0),
            prepared_plan_reuses: AtomicU64::new(0),
            prepared_plan_compiles: AtomicU64::new(0),
            prepared_dml_reuses: AtomicU64::new(0),
            prepared_dml_compiles: AtomicU64::new(0),
            checkpoint: Mutex::new(CheckpointState::default()),
            checkpoint_running: Mutex::new(()),
            replica_apply_checkpoint: RwLock::new(()),
            replica_applied_lsn_atomic: AtomicU64::new(replica_applied_seed),
            replica_restartpoint_tracker: Mutex::new(ReplicaRestartpointTracker::new(
                replica_needs_full_sync,
            )),
            last_checkpoint_lsn_atomic: AtomicU64::new(0),
            last_checkpoint_unix_ms_atomic: AtomicU64::new(0),
            wal_bytes_after_last_checkpoint_atomic: AtomicU64::new(0),
            last_auto_checkpoint_unix_ms_atomic: AtomicU64::new(0),
            checkpoints_total_atomic: AtomicU64::new(0),
            checkpoint_failures_total_atomic: AtomicU64::new(0),
            checkpoint_preflush_failures_total_atomic: AtomicU64::new(0),
            last_checkpoint_redo_lsn_atomic: AtomicU64::new(0),
            last_checkpoint_dirty_pages_atomic: AtomicU64::new(0),
            last_checkpoint_wal_reclaimed_bytes_atomic: AtomicU64::new(0),
            checkpoint_wal_reclaimed_bytes_total_atomic: AtomicU64::new(0),
            restartpoints_total_atomic: AtomicU64::new(0),
            last_checkpoint_trigger_atomic: AtomicU8::new(0),
            last_checkpoint_kind_atomic: AtomicU8::new(0),
            last_checkpoint_snapshot_us_atomic: AtomicU64::new(0),
            last_checkpoint_flush_us_atomic: AtomicU64::new(0),
            last_checkpoint_sync_us_atomic: AtomicU64::new(0),
            last_checkpoint_compact_us_atomic: AtomicU64::new(0),
            last_checkpoint_duration_us_atomic: AtomicU64::new(0),
            checkpoint_duration_total_us_atomic: AtomicU64::new(0),
            checkpoint_duration_max_us_atomic: AtomicU64::new(0),
            last_recovery: RecoverySummary::default(),
            next_txn_id_atomic: AtomicU64::new(next_txn_id_seed.max(1)),
            next_replica_local_txn_id: AtomicU64::new(1u64 << 63),
            physical_replica_mode: AtomicBool::new(physical_replica),
            next_temp_table_id: AtomicU64::new(TEMP_TABLE_ID_BASE),
            next_scn_atomic: AtomicU64::new(next_scn_seed.max(1)),
            // P0-1: seed the snapshot frontier to `next_scn - 1`, contiguous
            // with the allocation counter above. `txns.commit_frontier` was
            // seeded to the same value in `TransactionManager::from_wal`.
            visible_frontier: AtomicU64::new(next_scn_seed.max(1).saturating_sub(1)),
            commit_frontier_waiters: AtomicUsize::new(0),
            commit_frontier_wait_lock: Mutex::new(()),
            commit_frontier_signal: Condvar::new(),
            active_write_txns_atomic: AtomicUsize::new(active_write_seed),
            index_repair_state,
            index_repair_version,
            index_repair_epochs,
            min_retained_txn_id: AtomicU64::new(min_retained_txn_id_seed),
            index_page_max_trx_shortcut_hits: AtomicU64::new(0),
            index_page_max_trx_shortcut_misses: AtomicU64::new(0),
            slot_status_cache: SlotStatusCache::new(),
            snapshot_too_old_total: AtomicU64::new(0),
            temporal_query_total: AtomicU64::new(0),
            temporal_heap_fallback_total: AtomicU64::new(0),
            temporal_history_index_hit_total: AtomicU64::new(0),
            undo_publish_lock_ns: AtomicU64::new(0),
            undo_publish_lock_samples: AtomicU64::new(0),
            wait_events: Mutex::new(WaitEventHistory::default()),
            read_only_txn_count: AtomicU64::new(0),
            row_lock_signals: RowLockSignals::new(),
            row_lock_parkers: crate::storage::core::RowLockParkers::new(),
            table_locks_sharded: crate::storage::core::ShardedTableLocks::new(),
            row_locks_sharded: crate::storage::core::ShardedRowLocks::new(),
            row_lock_waiters: AtomicU64::new(0),
            commit_gc_counter: AtomicU64::new(0),
            // `MYDB_DDL_ASYNC=1` opts ADD INDEX into background-thread
            // backfill (see field doc in core.rs). Default false keeps
            // every existing CREATE INDEX → SELECT test working
            // unchanged. Tests that opt in flip the per-engine atomic
            // via `Database::set_async_ddl` instead of the env var so
            // they don't pollute parallel test runs.
            async_ddl: AtomicBool::new(
                std::env::var("MYDB_DDL_ASYNC")
                    .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "on" | "ON"))
                    .unwrap_or(false),
            ),
            alter_copy_jobs: Mutex::new(HashMap::new()),
            alter_copy_job_history: Mutex::new(VecDeque::with_capacity(64)),
            reorg_storage_locs: Mutex::new(HashMap::new()),
            partition_ddl_journal_lock: Mutex::new(()),
            ddl_retirement_cleanup: parking_lot::Mutex::new(None),
            partition_cleanup_background: AtomicBool::new(false),
            xa_reservations: parking_lot::Mutex::new(HashMap::new()),
            prepared_xa: parking_lot::RwLock::new(HashMap::new()),
            xa_prepared_active: AtomicUsize::new(0),
            xa_commits_total: AtomicU64::new(0),
            xa_rollbacks_total: AtomicU64::new(0),
            slot_reclaim_total_atomic: AtomicU64::new(0),
            slot_reclaim_runs_atomic: AtomicU64::new(0),
            slot_reclaim_errors_atomic: AtomicU64::new(0),
            slot_reclaim_quarantine: parking_lot::Mutex::new(HashMap::new()),
            slot_reclaim_permanent_failures_atomic: AtomicU64::new(0),
            slot_reclaim_quarantine_size_atomic: AtomicU64::new(0),
            lob_reclaim_queue: parking_lot::Mutex::new(LobReclaimQueue::try_new()),
            dml_stats: DmlStats::default(),
            table_modification_deltas: parking_lot::RwLock::new(HashMap::new()),
            table_live_row_deltas: parking_lot::RwLock::new(HashMap::new()),
            pending_live_row_deltas: (0..PENDING_LIVE_ROW_DELTA_SHARDS)
                .map(|_| parking_lot::Mutex::new(FastHashMap::default()))
                .collect(),
            table_write_epochs: parking_lot::RwLock::new(HashMap::new()),
            applier_dd_dirty_txns: parking_lot::Mutex::new(HashMap::new()),
            online_index_descriptors: parking_lot::RwLock::new(HashMap::new()),
            pending_primary_recovery: parking_lot::Mutex::new(Vec::new()),
            analyze_locks: parking_lot::Mutex::new(HashMap::new()),
            #[cfg(test)]
            analyze_batch_test_hook: Mutex::new(None),
            #[cfg(test)]
            analyze_test_hook: Mutex::new(None),
            #[cfg(test)]
            ddl_backfill_test_hook: Mutex::new(None),
            #[cfg(test)]
            create_finalize_test_hook: Mutex::new(None),
            #[cfg(test)]
            online_index_replay_test_hook: Mutex::new(None),
            #[cfg(test)]
            online_copy_publish_test_hook: Mutex::new(None),
            #[cfg(test)]
            vector_copy_committed_test_hook: Mutex::new(None),
            #[cfg(test)]
            vector_apply_test_hook: Mutex::new(None),
            #[cfg(test)]
            online_index_validation_test_hook: Mutex::new(None),
        };
        for (table_id, (version, max_value, lsn)) in auto_increment_recovery {
            db.auto_increment.install(table_id, version, max_value)?;
            if let Some(state) = db.auto_increment.get(table_id) {
                *state.logged.lock() = crate::storage::txn::AutoIncrementLoggedState {
                    version,
                    max_value,
                    lsn,
                };
            }
            if physical_replica {
                db.replica_restartpoint_tracker
                    .lock()
                    .map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "replica restartpoint tracker lock poisoned",
                        )
                    })?
                    .register_auto_increment(lsn)?;
            }
        }
        // The compiled-schema DD heaps are the sole catalog authority. v46
        // rows use ordinary transaction visibility, so active/rolled-back DD
        // writers are ignored while the committed catalog is opened. A fresh
        // directory is bootstrapped with one incremental catalog transaction.
        let runtime_init_us = runtime_init_started.elapsed().as_micros() as u64;
        let t = Instant::now();
        #[cfg(test)]
        {
            // Only a new clone of the setup script's exact seed may reuse
            // its decoded catalog. Existing directories and custom durable
            // seeds always load authoritative DD heaps and recovery markers.
            let cached_path = if !seeded_test_directory {
                None
            } else if db.config.durability == DurabilityMode::Memory {
                crate::util::env::var_os("MYDB_TEST_MEMORY_CATALOG")
            } else if clean_checkpoint_startup
                && crate::util::env::var_os("MYDB_TEST_SEED_CATALOG_SOURCE")
                    .is_some_and(|source| Some(source) == crate::util::env::var_os("MYDB_TEST_SEED_DIR"))
            {
                crate::util::env::var_os("MYDB_TEST_SEED_CATALOG")
            } else {
                None
            };
            let cached = cached_path
                .map(|path| -> Result<Catalog, QueryError> {
                    serde_json::from_slice(&fs::read(path).map_err(io_err)?)
                        .map_err(|err| io_err(std::io::Error::other(err)))
                })
                .transpose()?;
            if let Some(catalog) = cached {
                db.catalog
                    .replace_live(crate::storage::naming::migrate_catalog(catalog))?;
            } else {
                db.load_or_bootstrap_catalog()?;
            }
        }
        #[cfg(not(test))]
        db.load_or_bootstrap_catalog()?;
        if db.data_dir.join("system/index-maintenance.failed").try_exists().map_err(io_err)? {
            return Err(error(crate::error::catalog::ER_INDEX_CORRUPT, "persistent vector maintenance worker failure requires explicit repair"));
        }

        db.load_vector_purge_ledger()?;
        // No live read transaction survives restart. Historical reads still
        // need snapshot-specific descriptors, including lazily loaded ones.
        db.read_definition_highwater
            .store(db.visible_scn(), Ordering::Release);
        for table in db.catalog_read()?.tables.values() {
            db.primary_definition_highwater
                .fetch_max(table.primary_definition_scn, Ordering::Release);
            if table.primary_definition_scn != 0 {
                db.table_locks_sharded
                    .set_schema_floor(table.id, table.schema_version);
                for id in table.storage_ids() {
                    db.table_locks_sharded
                        .set_schema_floor(id, table.schema_version);
                }
            }
        }
        db.initialize_auth_snapshot()?;
        let catalog_load_us = t.elapsed().as_micros() as u64;
        // Version-2 ALTER COPY intents are build-then-swap journals.  WAL
        // recovery has already settled page images and the authoritative DD
        // catalog is now loaded, so schema_version unambiguously selects
        // rollback (old generation) or roll-forward (new generation).
        let recovered_incomplete = if !snapshot_read_only {
            db.recover_alter_copy_intents()?;
            // CTAS defers its DD generation until the row-load transaction has
            // committed. Publish any intent-carried schemas in memory first so
            // recovery undo/redo can resolve their row codecs.
            db.prepare_ctas_recovery()?;
            let recovered_incomplete = db.recover_incomplete_transactions()?;
            // Bulk-load generations are published before their transaction's
            // CommitTxn record. Resolve them only after WAL recovery has settled
            // every Active transaction, so the intent can choose rollback or
            // roll-forward from durable transaction state.
            db.recover_bulk_load_intents()?;
            db.recover_pending_primary_keys()?;
            db.finish_ctas_recovery()?;
            db.recover_published_primary_keys(&primary_publications)?;
            recovered_incomplete
        } else {
            false
        };
        db.run_startup_validation(
            &mut recovery,
            clean_checkpoint_startup && !recovered_incomplete,
        )?;
        db.initialize_auto_increment_counters()?;
        db.initialize_sequence_runtimes()?;
        if physical_replica {
            for value in sequence_recovery.values.values() {
                db.replica_restartpoint_tracker
                    .lock()
                    .map_err(|_| sequence_state_error("replica tracker poisoned"))?
                    .register_sequence(value.counter_id, value.state_lsn)?;
            }
        }
        db.recover_sequence_values(sequence_recovery)?;
        db.initialize_sequence_sync()?;
        // Reconcile any partition-reorg journals left by a crash mid
        // build-then-swap (§2.1). Must run after the catalog is loaded and
        // WAL redo/undo have settled, so commit state is derivable from the
        // live partition set.
        if !snapshot_read_only {
            db.recover_partition_reorgs()?;
            // RENAME moves name-derived physical paths before publishing the DD
            // name. Finish or reverse any interrupted move from authoritative
            // table identity before DROP/TRUNCATE cleanup runs.
            db.recover_rename_tables()?;
            // Reconcile any DROP TABLE intent logs left by a crash between the
            // catalog commit and the physical file purge. Same ordering rationale
            // as the partition-reorg pass above: `table_by_id` must reflect the
            // committed catalog.
            db.recover_drop_tables()?;
            db.recover_catalog_retirements()?;
            // Reconcile any TRUNCATE TABLE intent logs left by a crash between
            // the physical wipe and the intent removal. Runs after
            // `recover_drop_tables` so a subsequent DROP that overtook the
            // TRUNCATE has already made the table absent — recovery then
            // recognises the truncate intent as stale.
            db.recover_truncate_tables()?;
        }
        // Generic heap redo does not apply zone deltas. Load sidecars once,
        // then stream the WAL deltas against the final catalog.
        // Online physical replication applies deltas separately in wal_apply.
        // A bad sidecar remains untrusted until rebuilt from the heap.
        // Resolve the switch before admitting any derived sidecar or WAL
        // summary. Persisted globals have the highest startup priority.
        db.initialize_zone_map_enabled()?;
        let zone_replay_started = Instant::now();
        if context.continuation_base.is_none(){db.replay_zone_map_wal(&zone_replay)?;}
        if startup_progress_logging_enabled() {
            log_message(format_args!(
                "mydb recovery: zone summaries {} records in {}ms",
                zone_replay.len(),
                zone_replay_started.elapsed().as_millis()
            ));
        }
        db.replay_persistent_vector_wal(&zone_replay)?;
        if !snapshot_read_only && !db.physical_replica_mode() && db.open_mode != DatabaseOpenMode::SealBaseline {
            while db.vector_purges_pending.load(Ordering::Acquire) != 0 {
                let completed = db.drain_vector_purges();
                db.ensure_operational()?;
                if completed == 0 { return Err(vector::persistence::corrupt()); }
            }
        }
        db.restore_prepared_vector_mutations(&zone_replay)?;
        db.restore_replica_vector_completion(&zone_replay)?;
        if db.open_mode == DatabaseOpenMode::ReadWrite && !db.physical_replica_mode() { db.resume_failed_vector_jobs()?; }
        drop(zone_replay);
        // Re-publish XA branches whose `PrepareTxn` records survived the
        // crash. `TransactionManager::from_wal` already marked the
        // matching slots as PREPARED and replayed their locks; this just
        // mirrors them into `Database.prepared_xa` so XA RECOVER / XA
        // COMMIT / XA ROLLBACK can find them.
        db.rebuild_prepared_xa_from_slots(&prepared_metadata.binlog, &prepared_metadata.prepare_scn);
        db.recover_binlog_intent_files()?;
        db.restore_inflight_index_repairs()?;
        if !snapshot_read_only { db.recover_vector_completion_intents()?; }
        db.rebuild_prepared_mdl_from_wal(&prepared_metadata.mdl);
        // P2 — table locks are now owned by the sharded `table_locks_sharded`
        // store, but crash recovery (`from_wal` → `install_prepared_locks`)
        // reconstructed prepared-XA table locks into the legacy
        // `lock_manager.table_locks` / `lock_log`. Move them into the sharded
        // store (single source of truth) and strip them from the legacy
        // staging area so commit/rollback release them exactly once.
        db.migrate_recovered_table_locks();
        // Seed the lock-free slot-status cache from the recovered slot
        // map so visibility checks on pre-existing txns also take the
        // fast path. Cheap (single read of txns lock) and idempotent.
        {
            let txns = db.txns.read();
            db.slot_status_cache.seed_from_slots(
                txns.slots
                    .values()
                    .map(|slot| (slot.txn_id, slot.status, slot.commit_scn.unwrap_or(0))),
            );
        }
        recovery.wal_recovery_us = wal_recovery_us;
        recovery.total_startup_us = startup_started
            .elapsed()
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;
        if startup_progress_logging_enabled() {
            log_message(format_args!(
                "mydb startup: total={}ms, wal_recovery={}ms, catalog={}ms, validation={}ms, runtime_init={}ms (undo_open={}ms), wal_records={}, redo={}, undo={}, prepared_xa={}",
                recovery.total_startup_us / 1000,
                recovery.wal_recovery_us / 1000,
                catalog_load_us / 1000,
                recovery.startup_validation_us / 1000,
                runtime_init_us / 1000,
                undo_open_us / 1000,
                recovery.scanned_records,
                recovery.redone_records,
                recovery.undone_transactions,
                recovery.prepared_recovered,
            ));
        }
        db.last_recovery = recovery;
        // Arm the time-based checkpoint trigger. The atomic starts at 0
        // ("never checkpointed") and `should_checkpoint` requires it to
        // be non-zero, so without this seed the documented
        // `MYDB_CHECKPOINT_INTERVAL_MS` cadence would stay dead until a
        // first size-triggered or manual checkpoint.
        let armed_ms = current_unix_ms();
        db.last_checkpoint_unix_ms_atomic
            .store(armed_ms, Ordering::Release);
        if let Ok(mut checkpoint) = db.checkpoint.lock() {
            checkpoint.last_checkpoint_unix_ms = Some(armed_ms);
        }
        db.wal_bytes_after_last_checkpoint_atomic
            .store(db.log_state_atomics.wal_bytes(), Ordering::Release);
        // Publish recovered binlog commit boundaries before ANY checkpoint can
        // reclaim their WAL evidence. This also runs when binlogging is disabled
        // for this boot: existing logs must remain safe for a later re-enable.
        if !snapshot_read_only {
            crate::binlog::recover_before_checkpoint(db.data_dir())?;
        }
        // If recovery actually replayed or undid work, fold its outcome
        // into a fresh checkpoint right away: the WAL collapses (or
        // prunes down to the retention floor when XA PREPARED branches
        // survive), so an immediate second crash re-replays nothing and
        // the next startup takes the clean-fast validation path.
        // Best-effort: a failure here leaves a fully recovered, longer
        // WAL — correctness is unaffected.
        if db.config.durability != DurabilityMode::Memory
            // Backup preparation explicitly checks one final sharp checkpoint
            // after all startup reconciliation, including temporal metadata.
            && !preparing
            // Startup opens storage before replication slots are registered.
            // Retain WAL until the normal checkpoint sees their durable floors.
            && !db.data_dir().join(crate::repl::master::slot::SLOTS_DIR).exists()
            && (db.last_recovery.redone_records > 0 || db.last_recovery.undone_transactions > 0)
        {
            if let Err(err) = db.checkpoint() {
                log_message(format_args!(
                    "mydb startup: post-recovery checkpoint failed: {} ({})",
                    err.message(),
                    err.sql_state()
                ));
            }
        }
        // A positive retention window needs a durable availability baseline
        // even when this data directory was created by a run where temporal
        // tracking was disabled.  Conversely, opening with retention=0
        // invalidates an older baseline so a later re-enable cannot claim the
        // disabled interval is queryable.
        if !snapshot_read_only {
            db.initialize_temporal_baseline()?;
            // Older builds allowed persistent tables to shadow mysql's virtual
            // compatibility tables. Preserve their data under visible names
            // before accepting connections, and release the reserved names.
            let shadows = db
                .catalog()
                .tables
                .values()
                .filter(|table| {
                    !table.system
                        && table.database.eq_ignore_ascii_case("mysql")
                        && crate::engine::information_schema::find_system_table(
                            "mysql",
                            &table.name,
                        )
                        .is_some()
                })
                .map(|table| (table.name.clone(), table.id))
                .collect::<Vec<_>>();
            for (name, id) in shadows {
                let mut suffix = 0u32;
                let short_name = name.chars().take(23).collect::<String>();
                let target = loop {
                    let candidate = format!("mysql.mydb_rec_{short_name}_{id}_{suffix}");
                    if !db.relation_name_exists(&candidate) {
                        break candidate;
                    }
                    suffix = suffix.saturating_add(1);
                };
                db.rename_table(&format!("mysql.{name}"), &target)?;
                log_message(format_args!(
                    "mydb startup: renamed legacy shadow table mysql.{name} to {target}"
                ));
            }
        }
        // Eager preload (`MYDB_HNSW_PRELOAD=eager`; default is lazy) warms
        // every Public vector graph and synchronously prepares the optional
        // paged HNSW query image so the first KNN after restart / failover
        // cannot race a background cache build.
        if !snapshot_read_only {
            db.vector_graph_preload_all()?;
        }
        #[cfg(test)]
        if let Some(fixture_sync) = fixture_sync {
            fixture_sync.finish().map_err(io_err)?;
        }
        Ok(db)
    }

    #[cfg(test)]
    fn open_with_durability(
        data_dir: impl Into<PathBuf>,
        durability: DurabilityMode,
    ) -> Result<Self, QueryError> {
        Self::open_with_config(
            data_dir,
            StorageConfig {
                durability,
                checkpoint_interval: Duration::from_millis(DEFAULT_CHECKPOINT_INTERVAL_MS),
                checkpoint_wal_bytes: DEFAULT_CHECKPOINT_WAL_BYTES,
                lock_wait: Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
                group_commit_window: Duration::from_micros(DEFAULT_GROUP_COMMIT_US),
                wal_segment_bytes: DEFAULT_WAL_SEGMENT_BYTES,
                buffer_pool_pages: DEFAULT_BUFFER_POOL_PAGES,
                buffer_pool_io_threads: 1,
                buffer_pool_prefetch_pages: DEFAULT_BUFFER_POOL_PREFETCH_PAGES,
                undo_retention: Duration::from_millis(DEFAULT_UNDO_RETENTION_MS),
                heap_update_reserve_bytes: DEFAULT_HEAP_UPDATE_RESERVE_BYTES,
                startup_validation: StartupValidationMode::Full,
                startup_validate_indexes: true,
            },
        )
    }
}

#[cfg(test)]
fn seed_empty_test_data_dir(data_dir: &Path, durability: DurabilityMode) -> Result<bool, QueryError> {
    const OPT_OUT_MARKER: &str = ".mydb-test-fresh-data-dir";

    let seed = if durability == DurabilityMode::Memory {
        crate::util::env::var_os("MYDB_TEST_MEMORY_SEED_DIR")
            .or_else(|| crate::util::env::var_os("MYDB_TEST_SEED_DIR"))
    } else {
        crate::util::env::var_os("MYDB_TEST_SEED_DIR")
    };
    let Some(seed_dir) = seed.map(PathBuf::from) else {
        return Ok(false);
    };
    if crate::util::env::var_os("MYDB_TEST_DISABLE_SEED").is_some() {
        return Ok(false);
    }

    fs::create_dir_all(data_dir).map_err(io_err)?;
    let opt_out_marker = data_dir.join(OPT_OUT_MARKER);
    if opt_out_marker.exists() {
        fs::remove_file(opt_out_marker).map_err(io_err)?;
        return Ok(false);
    }
    if fs::read_dir(data_dir)
        .map_err(io_err)?
        .next()
        .transpose()
        .map_err(io_err)?
        .is_some()
    {
        return Ok(false);
    }
    #[cfg(target_os = "macos")]
    if clone_test_seed_tree(&seed_dir, data_dir)? {
        return Ok(true);
    }
    copy_test_seed_tree(&seed_dir, data_dir)?;
    Ok(true)
}

#[cfg(all(test, target_os = "macos"))]
thread_local! {
    static TEST_SEED_CLONE_TEST_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
}

#[cfg(all(test, target_os = "macos"))]
fn pause_test_seed_clone_for_test() {
    let hook = TEST_SEED_CLONE_TEST_HOOK.with(|cell| cell.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(all(test, target_os = "macos"))]
fn clone_test_seed_tree(source: &Path, destination: &Path) -> Result<bool, QueryError> {
    use std::ffi::CString;
    use std::os::raw::{c_char, c_int};
    use std::os::unix::ffi::OsStrExt;

    unsafe extern "C" {
        fn clonefile(source: *const c_char, destination: *const c_char, flags: u32) -> c_int;
    }

    let source_c = CString::new(source.as_os_str().as_bytes())
        .map_err(|error| io_err(std::io::Error::new(std::io::ErrorKind::InvalidInput, error)))?;
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let staging = tempfile::Builder::new()
        .prefix("mydb-seed-stage-")
        .tempdir_in(parent)
        .map_err(io_err)?;
    let cloned_path = staging.path().join("clone");
    let destination_c = CString::new(cloned_path.as_os_str().as_bytes())
        .map_err(|error| io_err(std::io::Error::new(std::io::ErrorKind::InvalidInput, error)))?;

    // Keep tempfile's reserved name occupied throughout cloning. Removing it
    // first allows another fixture to claim it and mixes independent seeds.
    // A same-parent rename atomically replaces the still-empty destination.
    let cloned = unsafe { clonefile(source_c.as_ptr(), destination_c.as_ptr(), 0) } == 0;
    if cloned {
        pause_test_seed_clone_for_test();
        fs::rename(&cloned_path, destination).map_err(io_err)?;
    }
    Ok(cloned)
}

#[cfg(test)]
fn copy_test_seed_tree(source: &Path, destination: &Path) -> Result<(), QueryError> {
    for entry in fs::read_dir(source).map_err(io_err)? {
        let entry = entry.map_err(io_err)?;
        let file_type = entry.file_type().map_err(io_err)?;
        let target = destination.join(entry.file_name());
        if file_type.is_dir() {
            fs::create_dir_all(&target).map_err(io_err)?;
            copy_test_seed_tree(&entry.path(), &target)?;
        } else if file_type.is_file() {
            crate::util::file::copy_fixture_file(&entry.path(), &target).map_err(io_err)?;
        }
    }
    Ok(())
}
