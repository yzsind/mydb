use crate::wal::{ReplayScan, WalRecordSource};

impl RecoverySummary {
    fn describe(&self) -> String {
        format!(
            "total_startup_us={}, wal_recovery_us={}, startup_validation_us={}, records_scanned={}, records_redone={}, index_redo_skipped={}, transactions_undone={}, transactions_prepared={}, startup_validation_mode={}, startup_validation_path={}, indexes_repaired={}, heap_files_checked={}, heap_pages_checked={}, heap_files_skipped={}, heap_pages_skipped={}, index_files_checked={}, index_files_quick_checked={}, index_files_skipped={}",
            self.total_startup_us,
            self.wal_recovery_us,
            self.startup_validation_us,
            self.scanned_records,
            self.redone_records,
            self.skipped_index_redo_records,
            self.undone_transactions,
            self.prepared_recovered,
            self.startup_validation_mode
                .map(StartupValidationMode::as_str)
                .unwrap_or("none"),
            self.startup_validation_path
                .map(StartupValidationPath::as_str)
                .unwrap_or("none"),
            self.repaired_indexes,
            self.checked_heap_files,
            self.checked_heap_pages,
            self.skipped_heap_files,
            self.skipped_heap_pages,
            self.checked_index_files,
            self.quick_checked_index_files,
            self.skipped_index_files
        )
    }

    fn is_clean_checkpoint_startup(&self) -> bool {
        self.single_checkpoint_end_record
            && self.redone_records == 0
            && self.undone_transactions == 0
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    crate::util::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn default_buffer_pool_io_threads() -> usize {
    thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(2)
        .saturating_div(2)
        .max(1)
        .min(MAX_BUFFER_POOL_IO_THREADS)
}

fn env_bool(name: &str, default: bool) -> bool {
    crate::util::env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn txn_id_for_ddl() -> u64 {
    0
}

fn reject_legacy_storage(data_dir: &Path) -> Result<(), QueryError> {
    // Legacy single-file undo tablespaces (pre-segmented).
    let undo_dir = crate::storage::naming::system_undo_dir(data_dir);
    let legacy_undo_dir = data_dir.join("undo");
    for candidate in [
        undo_dir.join("undo.tbl"),
        undo_dir.join("undo01.tbl"),
        legacy_undo_dir.join("undo.tbl"),
        legacy_undo_dir.join("undo01.tbl"),
    ] {
        if candidate.exists() {
            return Err(error(crate::error::catalog::ER_UNKNOWN_ERROR,
                "legacy undo tablespace files are incompatible with the segmented undo tablespace; use a new data directory or rebuild data",
            ));
        }
    }
    // Pre-v33 layout: top-level `redo/`, `undo/`, `tables/`, `indexes/`
    // were moved under `system/` and `schemas/`. Refuse to open such a
    // tree so a stale data dir can't be silently misinterpreted.
    for legacy in [
        data_dir.join("redo").join("redo.log"),
        data_dir.join("txn_state.json"),
    ] {
        if legacy.exists() {
            return Err(error(crate::error::catalog::ER_UNKNOWN_ERROR,
                "legacy storage files are incompatible with the current mydb development storage format; use a new data directory or rebuild data",
            ));
        }
    }
    // Pre-v33 directories (any of these existing without the new
    // `system/` or `schemas/` companions strongly suggests an old dir).
    let has_new = data_dir.join("system").exists() || data_dir.join("schemas").exists();
    for legacy in [
        data_dir.join("tables"),
        data_dir.join("indexes"),
        data_dir.join("redo"),
        data_dir.join("replication"),
    ] {
        if legacy.exists() && !has_new {
            return Err(error(crate::error::catalog::ER_UNKNOWN_ERROR,
                format!(
                    "legacy storage layout detected at {} (pre-v33 top-level `tables/` / `indexes/` / `redo/` / `replication/`); use a new data directory or rebuild data",
                    legacy.display()
                ),
            ));
        }
    }
    super::catalog::reject_legacy_catalog_json(data_dir)
}

struct RecoveryResult {
    summary: RecoverySummary,
    scan: Option<ReplayScan>,
    wal_modified: bool,
}

#[cfg(test)]
fn recover_from_wal(
    data_dir: &Path,
    durability: DurabilityMode,
) -> Result<RecoveryResult, QueryError> {
    recover_from_wal_with_options(data_dir, durability, false, RecoveryContext::default())
}

fn recover_from_wal_with_options(
    data_dir: &Path,
    durability: DurabilityMode,
    precheck_indexes: bool,
    context: RecoveryContext,
) -> Result<RecoveryResult, QueryError> {
    let mut summary = RecoverySummary::default();
    if durability == DurabilityMode::Memory {
        return Ok(RecoveryResult {
            summary,
            scan: None,
            wal_modified: false,
        });
    }
    let _recovery_io = RecoveryIoScope::enter()?;
    // M4.4 - Recovery progress reporting (Oracle-style).
    let recovery_started = Instant::now();
    if startup_progress_logging_enabled() {
        log_message("mydb recovery: scanning WAL...");
    }
    let mut scan = LogManager::scan_replay_with_mode(data_dir, context.wal_read_mode())?;
    context.validate_source_range(&scan.records)?;
    summary.scanned_records = scan.records.len();
    if startup_progress_logging_enabled() {
        log_message(format_args!(
            "mydb recovery: WAL scan complete, {} records in {}ms",
            scan.records.len(),
            recovery_started.elapsed().as_millis()
        ));
    }
    summary.single_checkpoint_end_record = scan.records.len() == 1
        && matches!(
            scan.records.record(0)?.payload,
            WalPayload::CheckpointEnd { .. }
        );
    // Payloads are disk-backed. Account for the compact per-record partition
    // and undo navigation metadata before allocating it.
    let mut navigation_charge =
        crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
            scan.records.len().saturating_mul(32).saturating_add(4096),
        )?;
    let physical_replica = crate::repl::role::physical_replica_on_disk(data_dir);
    let mut redo_cutoff = 0;
    let mut all_txns = RecoveryTxnSet::for_scan(&scan);
    let sparse_txns = all_txns.sparse();
    let mut committed = RecoveryTxnSet::for_scan(&scan);
    let mut ended = RecoveryTxnSet::for_scan(&scan);
    let mut prepared = RecoveryTxnSet::for_scan(&scan);
    for index in 0..scan.records.len() {
        let header = scan.records.header(index)?;
        let txn_id = header.txn_id;
        if txn_id != 0 && !all_txns.contains(&txn_id) {
            if sparse_txns {
                navigation_charge.try_grow(512)?;
            }
            all_txns.insert(txn_id);
        }
        use crate::wal::WalRecordType as T;
        match header.record_type {
            T::CheckpointEnd if !physical_replica => {
                if let WalPayload::CheckpointEnd { redo_lsn, .. } =
                    scan.records.record(index)?.payload
                {
                    redo_cutoff = redo_lsn;
                }
            }
            T::CommitTxn => {
                committed.insert(txn_id);
                prepared.remove(&txn_id);
            }
            T::AbortTxn => {
                prepared.remove(&txn_id);
            }
            T::EndTxn => {
                ended.insert(txn_id);
                prepared.remove(&txn_id);
            }
            T::PrepareTxn => {
                prepared.insert(txn_id);
            }
            _ => {}
        }
    }
    // PREPARED branches are durable: their heap / index / page-update
    // records must be REDOne so that a later XA COMMIT sees the data
    // (and so that the corresponding undo chain — replayed by
    // rollback_prepared — has the right "after" image to invert).
    let mut redoable = committed.clone();
    redoable.extend(prepared.iter());
    // Physical replica: the local WAL mirrors the master's stream. Redo
    // must reapply EVERY retained record regardless of commit outcome
    // (the commit may simply not have streamed yet), the master's
    // CheckpointBegin/End frames say nothing about THIS node's page
    // durability, and the undo phase must not roll back in-flight
    // master transactions (their Commit/Abort arrives with the stream;
    // a local rollback would silently fork the replica).
    if physical_replica {
        redoable.extend(all_txns.iter());
    }
    // Redo cutoff comes directly from the last completed CheckpointEnd. It is
    // the minimum of that checkpoint's Begin LSN, oldest dirty-page rec_lsn,
    // and the begin LSN of writers active at the boundary. Keeping the scalar
    // in WAL is both cheaper than a full DPT and, crucially, covers a writer
    // whose WAL append precedes Begin while page publication races the
    // checkpoint scan. Retained records below the cutoff remain available to
    // UNDO but need no REDO.
    if let Some(base) = context.continuation_base {
        redo_cutoff = base.checked_add(1).ok_or_else(|| {
            error(
                crate::error::catalog::MYDB_ER_BACKUP_FAILED,
                "source LSN overflow",
            )
        })?;
    } else if let Some(backup_floor) = context.redo_floor() {
        redo_cutoff = redo_cutoff.min(backup_floor);
    }
    let total_records = scan.records.len();
    let redo_started = Instant::now();
    // Partition redo records by target file. Heap / data / page
    // redo is self-contained file read-modify-write (page-LSN gated, no
    // shared log), so different files are fully independent and each file's
    // records, applied in LSN order by one worker, stay correct.
    //
    // Index files use one `IndexFile` per partition and append logical WAL to
    // a shared, internally locked LogManager. Records for one index stay on
    // one worker in LSN order; distinct index files are physically independent
    // and can therefore replay concurrently after the heap pass.
    let (heap_partitions, index_partitions) = plan_redo_file_partitions_with_context(
        &scan.records,
        &redoable,
        &committed,
        redo_cutoff,
        context,
    )?;
    let has_index_redo = !index_partitions.is_empty();
    let partition_count = heap_partitions.len();
    let parallel = recovery_parallel_enabled() && partition_count > 1;
    let parallelism = if parallel {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(2)
            .min(partition_count)
    } else {
        1
    };
    if startup_progress_logging_enabled() {
        log_message(format_args!(
            "mydb recovery: starting REDO phase ({} records, {} data files, parallelism={}, index_redo={})",
            total_records, partition_count, parallelism, has_index_redo
        ));
    }

    // --- Pass 1: heap / data / page redo (parallel or serial) ---
    if !parallel {
        let mut state = RedoState::with_context(None, context);
        for &rec_idx in heap_partitions.values().flatten() {
            apply_redo_record(
                data_dir,
                scan.records.record(rec_idx)?.as_ref(),
                &redoable,
                redo_cutoff,
                physical_replica,
                &mut state,
            )?;
        }
        summary.redone_records += state.redone_records;
    } else {
        // One ordered partition per file fanned across a bounded worker
        // pool; a shared atomic hands out partitions work-stealing style so
        // a few large files don't idle the rest.
        let work: Vec<&Vec<usize>> = heap_partitions.values().collect();
        let next = std::sync::atomic::AtomicUsize::new(0);
        let records = &scan.records;
        let redoable_ref = &redoable;
        let first_err: std::sync::Mutex<Option<QueryError>> = std::sync::Mutex::new(None);
        let redone_total = std::sync::atomic::AtomicUsize::new(0);

        std::thread::scope(|scope| {
            for _ in 0..parallelism {
                let next = &next;
                let work = &work;
                let first_err = &first_err;
                let redone_total = &redone_total;
                let directories = Arc::clone(&_recovery_io.directories);
                scope.spawn(move || {
                    let _recovery_io =
                        match RecoveryIoScope::enter_with_directories(directories, false) {
                            Ok(scope) => scope,
                            Err(err) => {
                                *first_err.lock().unwrap() = Some(err);
                                return;
                            }
                        };
                    let mut state = RedoState::with_context(None, context);
                    'outer: loop {
                        let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if i >= work.len() {
                            break;
                        }
                        if first_err.lock().unwrap().is_some() {
                            break;
                        }
                        for &rec_idx in work[i] {
                            if let Err(err) = records.record(rec_idx).and_then(|record| {
                                apply_redo_record(
                                    data_dir,
                                    &record,
                                    redoable_ref,
                                    redo_cutoff,
                                    physical_replica,
                                    &mut state,
                                )
                            }) {
                                *first_err.lock().unwrap() = Some(err);
                                break 'outer;
                            }
                        }
                    }
                    if let Err(err) = _recovery_io.finish() {
                        *first_err.lock().unwrap() = Some(err);
                    }
                    redone_total
                        .fetch_add(state.redone_records, std::sync::atomic::Ordering::Relaxed);
                });
            }
        });

        if let Some(err) = first_err.into_inner().unwrap() {
            return Err(err);
        }
        summary.redone_records += redone_total.load(std::sync::atomic::Ordering::Relaxed);
    }

    let native_recovery_ceiling = (!physical_replica).then_some(scan.next_lsn);
    let precheck_indexes = precheck_indexes
        && !physical_replica
        && prepared.len() == 0
        && bool_env_enabled("MYDB_RECOVERY_INDEX_REBUILD", true);
    // --- Pass 2: index redo, ordered per file and parallel across files ---
    let mut recovery_log: Option<Arc<crate::wal::LogMutex>> = None;
    if has_index_redo {
        let index_parallel = recovery_parallel_enabled() && index_partitions.len() > 1;
        if !index_parallel {
            let log = Arc::new(parking_lot::Mutex::new(LogManager::open_from_replay(
                data_dir, &scan,
            )?));
            log.lock().set_local_recovery(context.is_restore());
            let mut state = RedoState::with_context(Some(log), context);
            state.native_recovery_ceiling = native_recovery_ceiling;
            if precheck_indexes {
                for partition in index_partitions.values() {
                    precheck_recovery_index(data_dir, &scan.records, partition, &mut state)?;
                }
            }
            for &rec_idx in index_partitions.values().flatten() {
                if skip_prechecked_index_record(&scan.records, rec_idx, &mut state)? {
                    continue;
                }
                let record = scan.records.record(rec_idx)?;
                apply_redo_record(
                    data_dir,
                    &record,
                    &redoable,
                    redo_cutoff,
                    physical_replica,
                    &mut state,
                )?;
            }
            state.finish()?;
            summary.redone_records += state.redone_records;
            summary.skipped_index_redo_records += state.skipped_index_redo_records;
            recovery_log = state.recovery_log;
        } else {
            let shared_log = Arc::new(parking_lot::Mutex::new(LogManager::open_from_replay(
                data_dir, &scan,
            )?));
            shared_log.lock().set_local_recovery(context.is_restore());
            let work = index_partitions.values().collect::<Vec<_>>();
            let worker_count = std::thread::available_parallelism()
                .map(|value| value.get())
                .unwrap_or(2)
                .min(work.len());
            let next = std::sync::atomic::AtomicUsize::new(0);
            let first_err = std::sync::Mutex::new(None::<QueryError>);
            let redone_total = std::sync::atomic::AtomicUsize::new(0);
            let skipped_total = std::sync::atomic::AtomicUsize::new(0);
            let records = &scan.records;
            let redoable_ref = &redoable;
            std::thread::scope(|scope| {
                for _ in 0..worker_count {
                    let shared_log = Arc::clone(&shared_log);
                    let work = &work;
                    let next = &next;
                    let first_err = &first_err;
                    let redone_total = &redone_total;
                    let skipped_total = &skipped_total;
                    let directories = Arc::clone(&_recovery_io.directories);
                    scope.spawn(move || {
                        let _recovery_io =
                            match RecoveryIoScope::enter_with_directories(directories, false) {
                                Ok(scope) => scope,
                                Err(err) => {
                                    *first_err.lock().unwrap() = Some(err);
                                    return;
                                }
                            };
                        let mut state = RedoState::with_context(Some(shared_log), context);
                        state.native_recovery_ceiling = native_recovery_ceiling;
                        'outer: loop {
                            let work_index =
                                next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let Some(partition) = work.get(work_index) else {
                                break;
                            };
                            if first_err.lock().unwrap().is_some() {
                                break;
                            }
                            if precheck_indexes {
                                if let Err(err) = precheck_recovery_index(
                                    data_dir, records, partition, &mut state,
                                ) {
                                    *first_err.lock().unwrap() = Some(err);
                                    break 'outer;
                                }
                            }
                            for &rec_idx in partition.iter() {
                                match skip_prechecked_index_record(records, rec_idx, &mut state) {
                                    Ok(true) => continue,
                                    Ok(false) => {}
                                    Err(err) => {
                                        *first_err.lock().unwrap() = Some(err);
                                        break 'outer;
                                    }
                                }
                                if let Err(err) = records.record(rec_idx).and_then(|record| {
                                    apply_redo_record(
                                        data_dir,
                                        &record,
                                        redoable_ref,
                                        redo_cutoff,
                                        physical_replica,
                                        &mut state,
                                    )
                                }) {
                                    *first_err.lock().unwrap() = Some(err);
                                    break 'outer;
                                }
                            }
                        }
                        if let Err(err) = state.finish() {
                            *first_err.lock().unwrap() = Some(err);
                        }
                        if let Err(err) = _recovery_io.finish() {
                            *first_err.lock().unwrap() = Some(err);
                        }
                        redone_total
                            .fetch_add(state.redone_records, std::sync::atomic::Ordering::Relaxed);
                        skipped_total.fetch_add(
                            state.skipped_index_redo_records,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                    });
                }
            });
            if let Some(err) = first_err.into_inner().unwrap() {
                return Err(err);
            }
            summary.redone_records += redone_total.load(std::sync::atomic::Ordering::Relaxed);
            summary.skipped_index_redo_records +=
                skipped_total.load(std::sync::atomic::Ordering::Relaxed);
            recovery_log = Some(shared_log);
        }
    }
    let _ = redo_started;

    let losers = if physical_replica {
        // In-flight master transactions are NOT losers on a replica —
        // their Commit/Abort arrives with the resumed stream.
        HashSet::new()
    } else {
        all_txns
            .iter()
            .filter(|id| !committed.contains(id) && !ended.contains(id) && !prepared.contains(id))
            .collect::<HashSet<_>>()
    };
    summary.prepared_recovered = prepared.len();
    let wal_modified = !losers.is_empty()
        || recovery_log
            .as_ref()
            .is_some_and(|log| log.lock().next_lsn() != scan.next_lsn);
    if !losers.is_empty() {
        // M4.4 - UNDO phase progress
        if startup_progress_logging_enabled() {
            log_message(format_args!(
                "mydb recovery: starting UNDO phase ({} loser transactions)",
                losers.len()
            ));
        }

        let log = match recovery_log.take() {
            Some(log) => log,
            None => Arc::new(parking_lot::Mutex::new(LogManager::open_from_replay(
                data_dir, &scan,
            )?)),
        };
        log.lock().set_local_recovery(context.is_restore());
        set_recovery_undo_log(&log);
        let mut last_by_txn = HashMap::new();
        let pending_path = data_dir.join("system/recovery-indexes.pending");
        let mut pending_indexes: BTreeSet<String> = if pending_path.try_exists().map_err(io_err)? {
            serde_json::from_slice(&fs::read(&pending_path).map_err(io_err)?)
                .map_err(|err| error(crate::error::catalog::ER_GET_ERRNO, err.to_string()))?
        } else {
            BTreeSet::new()
        };
        let mut lsns_by_txn: HashMap<u64, Vec<u64>> = HashMap::new();
        let mut skipped_by_txn: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
        for record_index in 0..scan.records.len() {
            let header = scan.records.header(record_index)?;
            if !losers.contains(&header.txn_id) {
                continue;
            }
            last_by_txn.insert(header.txn_id, header.lsn);
            if header.record_type == crate::wal::WalRecordType::IndexBatch {
                if let WalPayload::IndexBatch { path, .. } =
                    &scan.records.record(record_index)?.payload
                {
                    pending_indexes.insert(path.clone());
                }
            }
            if header.record_type != crate::wal::WalRecordType::RollbackTo {
                continue;
            }
            let record = scan.records.record(record_index)?;
            if let WalPayload::RollbackTo {
                undo_next_lsn,
                skip_lsn_start,
                skip_lsn_end,
                ..
            } = &record.payload
            {
                if *undo_next_lsn >= record.lsn || *skip_lsn_end >= record.lsn {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "corrupt rollback WAL: non-backward recovery boundary",
                    ));
                }
                if skip_lsn_start < skip_lsn_end {
                    skipped_by_txn
                        .entry(record.txn_id)
                        .or_default()
                        .push((*skip_lsn_start, *skip_lsn_end));
                }
            }
        }
        // Index changes are logical and may already have reached disk while
        // loser heap changes are undone below. Persist repair intent BEFORE
        // any CLR/EndTxn advances the undo chain. It survives a second crash
        // and is cleared only after startup rebuilt the affected indexes.
        if !pending_indexes.is_empty() {
            let bytes = serde_json::to_vec(&pending_indexes)
                .map_err(|err| error(crate::error::catalog::ER_GET_ERRNO, err.to_string()))?;
            crate::util::file::write_staged_file(
                &pending_path,
                &pending_path.with_extension("staging"),
                &bytes,
                true,
            )
            .map_err(io_err)?;
            crate::platform::fs::sync_path(pending_path.parent().expect("system directory"))
                .map_err(io_err)?;
        }
        // Ordinary crash losers without a rolled-back savepoint need no
        // predecessor index. Keep the additional memory proportional to the
        // transactions that actually use skip intervals.
        if !skipped_by_txn.is_empty() {
            for record_index in 0..scan.records.len() {
                let record = scan.records.header(record_index)?;
                if skipped_by_txn.contains_key(&record.txn_id) {
                    lsns_by_txn
                        .entry(record.txn_id)
                        .or_default()
                        .push(record.lsn);
                }
            }
        }
        let mut undo_queue = std::collections::BinaryHeap::new();
        let mut skip_maps = HashMap::new();
        for txn_id in &losers {
            let txn_id = *txn_id;
            if let Some(lsn) = last_by_txn.get(&txn_id) {
                undo_queue.push((*lsn, txn_id));
            }
            // Buffered and eager records can interleave. Retain the saved
            // buffered prefix but skip post-savepoint eager writes. Partition
            // once above, then merge only this transaction's intervals.
            let mut ranges = skipped_by_txn.remove(&txn_id).unwrap_or_default();
            ranges.sort_unstable();
            let mut merged: Vec<(u64, u64)> = Vec::new();
            for (start, end) in ranges {
                if let Some(last) = merged.last_mut().filter(|last| start <= last.1) {
                    last.1 = last.1.max(end);
                } else {
                    merged.push((start, end));
                }
            }
            // A buffered savepoint's eager cutoff is an allocation watermark,
            // not necessarily a record owned by this transaction. Resolve the
            // predecessor once per merged interval; jumping to the cutoff
            // itself could enter another (even committed) transaction's chain.
            let mut txn_lsns = lsns_by_txn.remove(&txn_id).unwrap_or_default();
            txn_lsns.sort_unstable();
            let skipped: BTreeMap<u64, (u64, u64)> = merged
                .into_iter()
                .map(|(start, end)| {
                    let pos = txn_lsns.partition_point(|lsn| *lsn <= start);
                    let predecessor = pos.checked_sub(1).map_or(0, |pos| txn_lsns[pos]);
                    (start, (end, predecessor))
                })
                .collect();
            skip_maps.insert(txn_id, skipped);
        }
        // Full-page before images must be undone in reverse global WAL order.
        // Draining one transaction at a time can resurrect another loser's image.
        while let Some((lsn, txn_id)) = undo_queue.pop() {
            if let Some((_, &(end, predecessor))) = skip_maps[&txn_id].range(..lsn).next_back() {
                if lsn <= end {
                    if predecessor != 0 {
                        undo_queue.push((predecessor, txn_id));
                    }
                    continue;
                }
            }
            let record = scan.records.find_lsn(lsn)?.ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!(
                        "corrupt WAL undo chain: transaction {txn_id} references missing LSN {lsn}"
                    ),
                )
            })?;
            if record.txn_id != txn_id || record.prev_txn_lsn >= lsn {
                return Err(error(crate::error::catalog::ER_GET_ERRNO,
                    format!("corrupt WAL undo chain at LSN {lsn}: expected transaction {txn_id}, found {}, predecessor {}", record.txn_id, record.prev_txn_lsn)));
            }
            match &record.payload {
                WalPayload::PageUpdate {
                    path,
                    page_id,
                    before,
                    ..
                } => {
                    let mut after = before.clone();
                    let clr_lsn = log.lock().reserve_lsn()?;
                    set_page_lsn_if_formatted(&mut after, clr_lsn);
                    redo_page_update(data_dir, path, *page_id, clr_lsn, &after)?;
                    log.lock().append_reserved(
                        clr_lsn,
                        txn_id,
                        WalPayload::Clr {
                            path: path.clone(),
                            page_id: *page_id,
                            undo_next_lsn: record.prev_txn_lsn,
                            after,
                        },
                    )?;
                }
                WalPayload::PageUpdateCompressed { path, page_id, .. } => {
                    let (mut before, _) =
                        crate::wal::wal_compressed_page_update_images(&record.payload)?;
                    let clr_lsn = log.lock().reserve_lsn()?;
                    set_page_lsn_if_formatted(&mut before, clr_lsn);
                    redo_page_update(data_dir, path, *page_id, clr_lsn, &before)?;
                    let (before, _image_memory) = before.into_parts();
                    log.lock().append_reserved(
                        clr_lsn,
                        txn_id,
                        WalPayload::Clr {
                            path: path.clone(),
                            page_id: *page_id,
                            undo_next_lsn: record.prev_txn_lsn,
                            after: before,
                        },
                    )?;
                }
                WalPayload::HeapInsert {
                    path,
                    page_id,
                    slot_id,
                    ..
                } => {
                    let page_id_u64 = *page_id as u64;
                    let clr_lsn = log.lock().reserve_lsn()?;
                    redo_heap_delete(data_dir, path, clr_lsn, page_id_u64, *slot_id)?;
                    log.lock().append_reserved(
                        clr_lsn,
                        txn_id,
                        WalPayload::Clr {
                            path: path.clone(),
                            page_id: page_id_u64,
                            undo_next_lsn: record.prev_txn_lsn,
                            after: read_recovery_page(data_dir, path, page_id_u64)?,
                        },
                    )?;
                }
                WalPayload::HeapInsertBatch { path, inserts } => {
                    let mut by_page = BTreeMap::<u64, Vec<&WalHeapInsert>>::new();
                    for insert in inserts {
                        by_page
                            .entry(insert.page_id as u64)
                            .or_default()
                            .push(insert);
                    }
                    let page_count = by_page.len();
                    for (idx, (page_id, page_inserts)) in by_page.into_iter().rev().enumerate() {
                        let clr_lsn = log.lock().reserve_lsn()?;
                        redo_heap_delete_batch_page(
                            data_dir,
                            path,
                            clr_lsn,
                            page_id,
                            &page_inserts,
                        )?;
                        log.lock().append_reserved(
                            clr_lsn,
                            txn_id,
                            WalPayload::Clr {
                                path: path.clone(),
                                page_id,
                                // Intermediate CLRs revisit the ORIGINAL batch,
                                // not themselves. After an interrupted recovery,
                                // idempotent deletes finish any remaining pages.
                                // Forward links to not-yet-durable CLRs are unsafe.
                                undo_next_lsn: if idx + 1 == page_count {
                                    record.prev_txn_lsn
                                } else {
                                    record.lsn
                                },
                                after: read_recovery_page(data_dir, path, page_id)?,
                            },
                        )?;
                    }
                }
                WalPayload::HeapUpdate {
                    path,
                    page_id,
                    slot_id,
                    before,
                    ..
                }
                | WalPayload::HeapDelete {
                    path,
                    page_id,
                    slot_id,
                    before,
                    ..
                } => {
                    let page_id_u64 = *page_id as u64;
                    let clr_lsn = log.lock().reserve_lsn()?;
                    redo_heap_update(data_dir, path, clr_lsn, page_id_u64, *slot_id, before)?;
                    log.lock().append_reserved(
                        clr_lsn,
                        txn_id,
                        WalPayload::Clr {
                            path: path.clone(),
                            page_id: page_id_u64,
                            undo_next_lsn: record.prev_txn_lsn,
                            after: read_recovery_page(data_dir, path, page_id_u64)?,
                        },
                    )?;
                }
                WalPayload::HeapUpdateLogical {
                    path,
                    page_id,
                    slot_id,
                    patches,
                    undo_lsn,
                } => {
                    // Recovery undo: write each patch's `before`
                    // bytes back into the slot, then emit a CLR
                    // with the post-undo page snapshot. No catalog
                    // is loaded yet, so we cannot derive the
                    // before image from a Value-bearing undo
                    // record — that's why the WAL patch carries
                    // both before and after byte ranges.
                    let page_id_u64 = *page_id as u64;
                    let undo_patches: Vec<(usize, Vec<u8>)> = patches
                        .iter()
                        .map(|p| (p.offset as usize, p.before.clone()))
                        .collect();
                    let clr_lsn = log.lock().reserve_lsn()?;
                    redo_heap_update_logical_with_owned(
                        data_dir,
                        path,
                        clr_lsn,
                        page_id_u64,
                        *slot_id,
                        &undo_patches,
                        Some((txn_id, *undo_lsn)),
                    )?;
                    log.lock().append_reserved(
                        clr_lsn,
                        txn_id,
                        WalPayload::Clr {
                            path: path.clone(),
                            page_id: page_id_u64,
                            undo_next_lsn: record.prev_txn_lsn,
                            after: read_recovery_page(data_dir, path, page_id_u64)?,
                        },
                    )?;
                }
                WalPayload::FileCreate { path, .. } => {
                    compensate_recovery_file(
                        data_dir,
                        &log,
                        txn_id,
                        path,
                        record.prev_txn_lsn,
                        crate::wal::FileCompensation::Delete,
                    )?;
                }
                WalPayload::FileDelete { path, before } => {
                    compensate_recovery_file(
                        data_dir,
                        &log,
                        txn_id,
                        path,
                        record.prev_txn_lsn,
                        crate::wal::FileCompensation::Restore(before.clone()),
                    )?;
                }
                WalPayload::FileExtend {
                    path,
                    old_len,
                    new_len,
                } => {
                    undo_reduced_extent(data_dir, &log, &record, path, *old_len, *new_len)?;
                }
                WalPayload::FileClr { undo_next_lsn, .. }
                | WalPayload::Clr { undo_next_lsn, .. }
                | WalPayload::RollbackTo { undo_next_lsn, .. } => {
                    if *undo_next_lsn != 0 {
                        undo_queue.push((*undo_next_lsn, txn_id));
                    }
                    continue;
                }
                _ => {}
            }
            if record.prev_txn_lsn != 0 {
                undo_queue.push((record.prev_txn_lsn, txn_id));
            }
        }
        let mut end_lsn = 0;
        for txn_id in losers {
            summary.undone_transactions += 1;
            end_lsn = log.lock().append(txn_id, WalPayload::EndTxn)?;
        }
        // One barrier covers the entire compensation suffix and all endings.
        if end_lsn != 0 {
            LogManager::sync_durable_to(&log, end_lsn)?;
        }
    }
    if wal_modified {
        if let Some(log) = &recovery_log {
            log.lock().flush()?;
        }
        scan.refresh(data_dir)?;
    }
    _recovery_io.finish()?;
    Ok(RecoveryResult {
        summary,
        scan: Some(scan),
        wal_modified: false,
    })
}

type RecoveryPartitions = hashbrown::HashMap<String, Vec<usize>, ahash::RandomState>;

#[cfg(test)]
fn plan_redo_file_partitions(
    records: &(impl WalRecordSource + ?Sized),
    redoable: &RecoveryTxnSet,
    committed: &RecoveryTxnSet,
    redo_cutoff: u64,
) -> Result<(RecoveryPartitions, RecoveryPartitions), QueryError> {
    plan_redo_file_partitions_with_context(
        records,
        redoable,
        committed,
        redo_cutoff,
        RecoveryContext::default(),
    )
}

fn plan_redo_file_partitions_with_context(
    records: &(impl WalRecordSource + ?Sized),
    redoable: &RecoveryTxnSet,
    committed: &RecoveryTxnSet,
    redo_cutoff: u64,
    context: RecoveryContext,
) -> Result<(RecoveryPartitions, RecoveryPartitions), QueryError> {
    let mut heap = RecoveryPartitions::default();
    let mut indexes = RecoveryPartitions::default();
    for idx in 0..records.len() {
        let header = records.header(idx)?;
        use crate::wal::WalRecordType as T;
        let eligible = header.lsn >= redo_cutoff
            && (matches!(header.record_type, T::Clr | T::FileClr)
                || header.txn_id == 0
                || redoable.contains(&header.txn_id));
        if !eligible {
            continue;
        }
        if header.record_type == T::DdlCommit
            && (header.txn_id == 0 || committed.contains(&header.txn_id))
        {
            if let WalPayload::DdlCommit { retired_paths, .. } = &records.record(idx)?.payload {
                // These files belong to superseded physical generations, not
                // missing/corrupt live files. Their bulk-loaded base pages may
                // no longer exist, so replaying their old row deltas is invalid.
                for path in retired_paths {
                    heap.remove(path.as_str());
                    indexes.remove(path.as_str());
                }
            }
        }
        if let Some(path) = records.redo_path(idx)? {
            if context.continuation_base.is_some() && path.ends_with(".zm") {
                continue;
            }
            // A committed whole-file replacement starts a new incarnation.
            // Replaying older pages first can extend a truncated file with old
            // rows even when its new page zero already covers the reset LSN.
            // Discard that obsolete prefix in both serial and parallel redo.
            let starts_vector_image = header.record_type == T::VectorIndexImage
                && matches!(records.record(idx)?.payload, WalPayload::VectorIndexImage { offset: 0, .. });
            if starts_vector_image || matches!(header.record_type, T::FileDelete | T::FileCreate) {
                heap.remove(path.as_ref());
                indexes.remove(path.as_ref());
            }
            let partitions = if is_index_object_path(&path) {
                &mut indexes
            } else {
                &mut heap
            };
            if let Some(partition) = partitions.get_mut(path.as_ref()) {
                partition.push(idx);
            } else {
                partitions.insert(path.into_owned(), vec![idx]);
            }
        }
    }
    Ok((heap, indexes))
}

fn should_redo_record(record: &WalRecord, redoable: &RecoveryTxnSet, redo_cutoff: u64) -> bool {
    record.lsn >= redo_cutoff && (record.txn_id == 0 || redoable.contains(&record.txn_id))
}

/// Prefer one heap-derived rebuild when many logical operations would
/// repeatedly descend this index. This is independent of workload/table/key
/// type. The persisted marker makes deferred rebuilding a required recovery
/// step, including across another crash; no index update is silently lost.
fn precheck_recovery_index(
    data_dir: &Path,
    records: &(impl WalRecordSource + ?Sized),
    partition: &[usize],
    state: &mut RedoState,
) -> Result<(), QueryError> {
    if partition.len() < 64 {
        return Ok(());
    }
    let Some(relative) = records.redo_path(partition[0])? else {
        return Ok(());
    };
    let Some((_, info)) = crate::storage::naming::parse_relative_object_path(&relative) else {
        return Ok(());
    };
    if info.ext != "idx" || crate::storage::data_dictionary::is_dd_table_id(info.table_id) {
        return Ok(());
    }
    let path = data_dir.join(relative.as_ref());
    let size = match fs::metadata(&path) {
        Ok(meta) => meta.len(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(io_err(err)),
    };
    let target = size.div_ceil(PAGE_SIZE as u64).max(1).saturating_mul(64);
    let mut operations = 0u64;
    for &index in partition {
        operations = operations.saturating_add(records.index_operations(index)? as u64);
        if operations >= target {
            break;
        }
    }
    if operations < target {
        return Ok(());
    }
    IndexFile::persist_planned_rebuild_marker(&path)?;
    state.skipped_index_redo_paths.insert(relative.into_owned());
    log_message(format_args!(
        "mydb recovery: planned index rebuild for {} (at least {} logical operations, {} pages)",
        path.display(),
        operations,
        size.div_ceil(PAGE_SIZE as u64)
    ));
    Ok(())
}
fn skip_prechecked_index_record(
    records: &(impl WalRecordSource + ?Sized),
    index: usize,
    state: &mut RedoState,
) -> Result<bool, QueryError> {
    if state.skipped_index_redo_paths.is_empty()
        || records.header(index)?.record_type != crate::wal::WalRecordType::IndexBatch
    {
        return Ok(false);
    }
    if records
        .redo_path(index)?
        .is_some_and(|path| state.skipped_index_redo_paths.contains(path.as_ref()))
    {
        state.skipped_index_redo_records += 1;
        return Ok(true);
    }
    Ok(false)
}

/// Per-worker (and serial) mutable redo state. `recovery_indexes` and
/// `skipped_index_redo_paths` are keyed by file path; since each file is
/// owned by exactly one worker, they never need cross-worker sharing.
/// `recovery_log` (the re-opened LogManager that index logical-WAL redo
/// appends to) IS shared across workers via the `Arc` — set up front for
/// parallel runs, opened lazily for serial.
struct RedoState {
    context: RecoveryContext,
    recovery_log: Option<Arc<crate::wal::LogMutex>>,
    native_recovery_ceiling: Option<u64>,
    recovery_indexes: HashMap<String, IndexFile>,
    skipped_index_redo_paths: HashSet<String>,
    redone_records: usize,
    skipped_index_redo_records: usize,
}

impl RedoState {
    #[cfg(test)]
    fn new(recovery_log: Option<Arc<crate::wal::LogMutex>>) -> Self {
        Self::with_context(recovery_log, RecoveryContext::default())
    }

    fn with_context(
        recovery_log: Option<Arc<crate::wal::LogMutex>>,
        context: RecoveryContext,
    ) -> Self {
        Self {
            context,
            recovery_log,
            native_recovery_ceiling: None,
            recovery_indexes: HashMap::new(),
            skipped_index_redo_paths: HashSet::new(),
            redone_records: 0,
            skipped_index_redo_records: 0,
        }
    }

    /// Flush every index file this worker touched and clear the
    /// thread-local logical-WAL routing. Called once a worker (or the
    /// serial driver) finishes applying its records.
    fn finish(&mut self) -> Result<(), QueryError> {
        let _routing = btree_logical_wal::LsnScope::new(None);
        for index in self.recovery_indexes.values_mut() {
            index.flush()?;
        }
        Ok(())
    }
}

/// Apply one redo record to its target file. Pure per-record logic shared
/// verbatim by the serial and parallel drivers, so the two paths are
/// behaviourally identical. Returns without effect for records that fail
/// `should_redo_record` / are control records.
fn apply_redo_record(
    data_dir: &Path,
    record: &WalRecord,
    redoable: &RecoveryTxnSet,
    redo_cutoff: u64,
    physical_replica: bool,
    state: &mut RedoState,
) -> Result<(), QueryError> {
    apply_redo_record_inner(
        data_dir,
        record,
        redoable,
        redo_cutoff,
        physical_replica,
        state,
    )
    .map_err(|err| {
        let path = std::slice::from_ref(record).redo_path(0).ok().flatten();
        err.map_message(|message| {
            format!(
                "{message}; REDO lsn={} txn={} type={:?} path={}",
                record.lsn,
                record.txn_id,
                record.payload.record_type(),
                path.as_deref().unwrap_or("<control>")
            )
        })
    })
}

fn apply_redo_record_inner(
    data_dir: &Path,
    record: &WalRecord,
    redoable: &RecoveryTxnSet,
    redo_cutoff: u64,
    physical_replica: bool,
    state: &mut RedoState,
) -> Result<(), QueryError> {
    match &record.payload {
        WalPayload::VectorIndexManifest { path, bytes } if should_redo_record(record, redoable, redo_cutoff) => {
            redo_vector_manifest(data_dir, path, bytes)?;
            state.redone_records += 1;
        }
        WalPayload::VectorIndexImage { path, offset, total, bytes }
            if should_redo_record(record, redoable, redo_cutoff) => {
            redo_vector_image(data_dir, path, *offset, *total, bytes, record.lsn)?;
            state.redone_records += 1;
        }
        WalPayload::FileClr { path, action, .. } if record.lsn >= redo_cutoff => {
            if (physical_replica || state.context.continuation_base.is_some())
                && is_index_object_path(path)
                && matches!(action, crate::wal::FileCompensation::Restore(_))
            {
                state.recovery_indexes.remove(path);
                IndexFile::persist_planned_rebuild_marker(&data_dir.join(path))?;
                state.skipped_index_redo_paths.insert(path.clone());
            } else {
                apply_file_compensation(data_dir, path, action)?;
            }
            state.redone_records += 1;
        }
        WalPayload::FileCreate { path, after }
            if should_redo_record(record, redoable, redo_cutoff) =>
        {
            // Physical replica: index files replicate logically and
            // carry a LOCAL physical layout — a master image (meta
            // page / FPI at the master's page id) would corrupt the
            // local tree. Same rule as the online applier.
            if !((physical_replica || state.context.continuation_base.is_some())
                && is_index_object_path(path))
                && !existing_file_generation_covers_lsn_with_context(
                    data_dir,
                    path,
                    record.lsn,
                    state.context,
                )?
            {
                write_recovered_file(data_dir, path, after)?;
                state.redone_records += 1;
            }
        }
        WalPayload::FileDelete { path, .. }
            if should_redo_record(record, redoable, redo_cutoff) =>
        {
            // Index redo buffers page writes in an open `IndexFile` handle that
            // `state.finish()` flushes back to `path`. A DROP/rebuild emits this
            // `FileDelete` after the last `IndexBatch` for the file, so the open
            // handle must be evicted here — otherwise `finish()` would recreate
            // the file we just unlinked, leaking an orphan index file. Heap /
            // data FileDeletes have no such handle; the removes are no-ops.
            state.recovery_indexes.remove(path);
            state.skipped_index_redo_paths.remove(path);
            invalidate_recovery_page_io(data_dir, path);
            if let Err(err) = fs::remove_file(data_dir.join(path)) {
                if path.ends_with(".zm") && err.kind() != std::io::ErrorKind::NotFound {
                    return Err(io_err(err));
                }
            }
            state.redone_records += 1;
        }
        WalPayload::FileExtend { path, new_len, .. }
            if should_redo_record(record, redoable, redo_cutoff) =>
        {
            resize_recovered_file(data_dir, path, *new_len, true)?;
            state.redone_records += 1;
        }
        WalPayload::PageUpdate {
            path,
            page_id,
            after,
            ..
        } if should_redo_record(record, redoable, redo_cutoff) => {
            if !((physical_replica || state.context.continuation_base.is_some())
                && is_index_object_path(path))
            {
                redo_page_update_with_context(
                    data_dir,
                    path,
                    *page_id,
                    record.lsn,
                    after,
                    state.context,
                )?;
                state.redone_records += 1;
            }
        }
        WalPayload::PageDelta { path, .. } if should_redo_record(record, redoable, redo_cutoff) => {
            if !((physical_replica || state.context.continuation_base.is_some())
                && is_index_object_path(path))
            {
                redo_page_delta(data_dir, record)?;
                state.redone_records += 1;
            }
        }
        WalPayload::PageUpdateCompressed { path, page_id, .. }
            if should_redo_record(record, redoable, redo_cutoff) =>
        {
            if !((physical_replica || state.context.continuation_base.is_some())
                && is_index_object_path(path))
            {
                let (_, after) = crate::wal::wal_compressed_page_update_images(&record.payload)?;
                redo_page_update_with_context(
                    data_dir,
                    path,
                    *page_id,
                    record.lsn,
                    &after,
                    state.context,
                )?;
                state.redone_records += 1;
            }
        }
        WalPayload::FullPageImage {
            path,
            page_id,
            after,
        } if should_redo_record(record, redoable, redo_cutoff) => {
            if !((physical_replica || state.context.continuation_base.is_some())
                && is_index_object_path(path))
            {
                redo_page_update_with_context(
                    data_dir,
                    path,
                    *page_id,
                    record.lsn,
                    after,
                    state.context,
                )?;
                state.redone_records += 1;
            }
        }
        WalPayload::FullPageImageCompressed { path, page_id, .. }
            if should_redo_record(record, redoable, redo_cutoff) =>
        {
            if !((physical_replica || state.context.continuation_base.is_some())
                && is_index_object_path(path))
            {
                let after = crate::wal::wal_full_page_image_after(&record.payload)?;
                redo_page_update_with_context(
                    data_dir,
                    path,
                    *page_id,
                    record.lsn,
                    &after,
                    state.context,
                )?;
                state.redone_records += 1;
            }
        }
        WalPayload::HeapInsert {
            path,
            page_id,
            slot_id,
            payload,
            ..
        } if should_redo_record(record, redoable, redo_cutoff) => {
            redo_heap_insert(
                data_dir,
                path,
                record.lsn,
                *page_id as u64,
                *slot_id,
                payload,
            )?;
            state.redone_records += 1;
        }
        WalPayload::HeapInsertBatch { path, inserts }
            if should_redo_record(record, redoable, redo_cutoff) =>
        {
            redo_heap_insert_batch(data_dir, path, record.lsn, inserts)?;
            state.redone_records += 1;
        }
        WalPayload::HeapUpdate {
            path,
            page_id,
            slot_id,
            after,
            ..
        } if should_redo_record(record, redoable, redo_cutoff) => {
            redo_heap_update(data_dir, path, record.lsn, *page_id as u64, *slot_id, after)?;
            state.redone_records += 1;
        }
        WalPayload::HeapUpdateLogical {
            path,
            page_id,
            slot_id,
            patches,
            ..
        } if should_redo_record(record, redoable, redo_cutoff) => {
            redo_heap_update_logical(
                data_dir,
                path,
                record.lsn,
                *page_id as u64,
                *slot_id,
                patches,
            )?;
            state.redone_records += 1;
        }
        WalPayload::HeapDelete {
            path,
            page_id,
            slot_id,
            ..
        } if should_redo_record(record, redoable, redo_cutoff) => {
            redo_heap_delete(data_dir, path, record.lsn, *page_id as u64, *slot_id)?;
            state.redone_records += 1;
        }
        WalPayload::SlotReclaim {
            path,
            page_id,
            slot_id,
        } if should_redo_record(record, redoable, redo_cutoff) => {
            redo_slot_reclaim(data_dir, path, record.lsn, *page_id as u64, *slot_id)?;
            state.redone_records += 1;
        }
        WalPayload::IndexBatch { path, ops }
            if should_redo_record(record, redoable, redo_cutoff) =>
        {
            if state.skipped_index_redo_paths.contains(path) {
                state.skipped_index_redo_records += 1;
                return Ok(());
            }
            let log = match &state.recovery_log {
                Some(log) => log.clone(),
                None => {
                    let log = Arc::new(parking_lot::Mutex::new(LogManager::open(data_dir)?));
                    log.lock().set_local_recovery(state.context.is_restore());
                    state.recovery_log = Some(log.clone());
                    log
                }
            };
            match redo_index_batch(
                data_dir,
                log,
                &mut state.recovery_indexes,
                path,
                record.lsn,
                record.txn_id,
                ops,
                state.native_recovery_ceiling,
                state.context,
            ) {
                Ok(()) => {
                    state.redone_records += 1;
                }
                Err(err) if is_rebuildable_index_redo_error(&err) => {
                    state.skipped_index_redo_paths.insert(path.clone());
                    state.recovery_indexes.remove(path);
                    state.skipped_index_redo_records += 1;
                    log_message(format_args!(
                        "mydb recovery: skipped index redo for {} at lsn {} ({}); startup validation will rebuild it",
                        path,
                        record.lsn,
                        err.message()
                    ));
                }
                Err(err) => return Err(err),
            }
        }
        WalPayload::Clr {
            path,
            page_id,
            after,
            ..
        } if record.lsn >= redo_cutoff => {
            if !((physical_replica || state.context.continuation_base.is_some())
                && is_index_object_path(path))
            {
                redo_page_update_with_context(
                    data_dir,
                    path,
                    *page_id,
                    record.lsn,
                    after,
                    state.context,
                )?;
                state.redone_records += 1;
            }
        }
        _ => {}
    }
    Ok(())
}

/// A whole-file direct builder publishes pages that already represent every
/// logical operation up to its build watermark.  An older `FileCreate`
/// record must not truncate that newer generation during REDO.  Ordinary
/// CREATE/TRUNCATE partitions exclude all records before their last eligible
/// reset, so an already-published new generation cannot be extended by redo
/// from the old incarnation before this check.
#[cfg(test)]
fn existing_file_generation_covers_lsn(
    data_dir: &Path,
    relative: &str,
    record_lsn: u64,
) -> Result<bool, QueryError> {
    existing_file_generation_covers_lsn_with_context(
        data_dir,
        relative,
        record_lsn,
        RecoveryContext::default(),
    )
}

fn existing_file_generation_covers_lsn_with_context(
    data_dir: &Path,
    relative: &str,
    record_lsn: u64,
    context: RecoveryContext,
) -> Result<bool, QueryError> {
    // XA intent chunks are logical byte streams, not standard pages. Their
    // first bytes must never be interpreted as a page LSN that skips redo.
    if relative.starts_with("system/xa-binlog/") {
        return Ok(false);
    }
    if context.must_redo(record_lsn) {
        return Ok(false);
    }
    let path = data_dir.join(relative);
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(io_err(err)),
    };
    if file.metadata().map_err(io_err)?.len() < PAGE_SIZE as u64 {
        return Ok(false);
    }
    if relative.ends_with(".zm") {
        let mut page = vec![0; PAGE_SIZE];
        file.read_exact(&mut page).map_err(io_err)?;
        let identity = zone_map_page::Identity {
            storage: read_u64(&page, 24),
            generation: read_u64(&page, 32),
            page: 0,
        };
        return Ok(
            zone_map_page::View::open(&page, identity).is_ok() && page_lsn(&page) >= record_lsn
        );
    }
    let mut page = vec![0; PAGE_SIZE];
    file.read_exact(&mut page).map_err(io_err)?;
    Ok(recovery_page_valid(&page) && page_lsn(&page) >= record_lsn)
}

/// Whether parallel redo is enabled (`MYDB_RECOVERY_PARALLEL`, default
/// on). The serial path is always available as the kill switch and the
/// differential-test oracle.
fn recovery_parallel_enabled() -> bool {
    std::env::var_os("MYDB_RECOVERY_PARALLEL")
        .map(|v| {
            !matches!(
                v.to_string_lossy().to_ascii_lowercase().as_str(),
                "0" | "off" | "false" | "no" | ""
            )
        })
        .unwrap_or(true)
}

/// True for index object files (`schemas/<db>/indexes/….{idx,hnsw}`).
/// Shared by the recovery redo loop and the online physical-replication
/// applier: on a physical replica, index files replicate logically and
/// master page images must not be applied to them.
pub(crate) fn is_index_object_path(path: &str) -> bool {
    let Some((_db, info)) = crate::storage::naming::parse_relative_object_path(path) else {
        return false;
    };
    matches!(info.kind, crate::storage::naming::ObjectFileKind::Index) && info.ext == "idx"
}

pub(crate) fn write_recovered_file(
    data_dir: &Path,
    relative: &str,
    bytes: &[u8],
) -> Result<(), QueryError> {
    invalidate_recovery_page_io(data_dir, relative);
    let path = data_dir.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(io_err)?;
    }
    fs::write(&path, bytes).map_err(io_err)?;
    note_recovery_write(data_dir, path)?;
    Ok(())
}

fn resize_recovered_file(
    data_dir: &Path,
    relative: &str,
    target_len: u64,
    grow_only: bool,
) -> Result<(), QueryError> {
    invalidate_recovery_page_io(data_dir, relative);
    let path = data_dir.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(io_err)?;
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(io_err)?;
    let current = file.metadata().map_err(io_err)?.len();
    if !grow_only || current < target_len {
        file.set_len(target_len).map_err(io_err)?;
        note_recovery_write(data_dir, path)?;
    }
    Ok(())
}

/// Redo a dictionary-compressed standard page against its exact prior image.
/// A preceding checkpoint-protected FPI repairs torn pages before this record;
/// without that base, recovery must stop rather than inventing missing bytes.
fn redo_page_delta(data_dir: &Path, record: &WalRecord) -> Result<(), QueryError> {
    let _scratch = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
        PAGE_SIZE * 2,
    )?;
    let WalPayload::PageDelta {
        path: relative,
        page_id,
        base_lsn,
        base_crc,
        after_crc,
        compressed_after,
    } = &record.payload
    else {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "not a page delta",
        ));
    };
    let fail = |message| error(crate::error::catalog::ER_GET_ERRNO, message);
    if record.txn_id != 0
        || *base_lsn == 0
        || *base_lsn >= record.lsn
        || compressed_after.is_empty()
        || compressed_after.len() > crate::wal::WAL_PAGE_DELTA_MAX_BYTES
    {
        return Err(fail("invalid page delta scope, LSN or length"));
    }
    let validate = |bytes: &[u8]| -> Result<(), QueryError> {
        if bytes.len() != PAGE_SIZE
            || !matches!(
                read_u32(bytes, 0),
                HEAP_PAGE_MAGIC
                    | INDEX_PAGE_MAGIC
                    | INDEX_META_MAGIC
                    | OVFL_MAGIC
                    | VEC_META_MAGIC
                    | VEC_NODE_MAGIC
                    | zone_map_page::MAGIC
            )
            || read_u32(bytes, 16) != page_checksum(bytes, 16)
        {
            return Err(fail("page delta requires a valid complete standard page"));
        }
        if relative.ends_with(".zm") {
            let identity = zone_map_page::Identity {
                storage: read_u64(bytes, 24),
                generation: read_u64(bytes, 32),
                page: *page_id,
            };
            zone_map_page::View::open(bytes, identity)?;
            if crate::storage::naming::parse_relative_object_path(relative)
                .is_some_and(|(_, info)| info.table_id != identity.storage)
                || Path::new(relative)
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .and_then(|stem| stem.parse::<u64>().ok())
                    .is_some_and(|generation| generation != identity.generation)
            {
                return Err(fail("page delta identity does not match file path"));
            }
        }
        Ok(())
    };
    // Generic standard-page deltas may alternate with cached recovery IO.
    // Discard its header/length cache before changing bytes through this path.
    invalidate_recovery_page_io(data_dir, relative);
    let offset = page_id
        .checked_mul(PAGE_SIZE as u64)
        .filter(|offset| offset.checked_add(PAGE_SIZE as u64).is_some())
        .ok_or_else(|| fail("page delta offset overflow"))?;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(data_dir.join(relative))
        .map_err(io_err)?;
    if file.metadata().map_err(io_err)?.len() < offset + PAGE_SIZE as u64 {
        return Err(fail("page delta base is missing"));
    }
    let mut before = vec![0; PAGE_SIZE];
    file.seek(SeekFrom::Start(offset)).map_err(io_err)?;
    file.read_exact(&mut before).map_err(io_err)?;
    validate(&before)?;
    let current_lsn = page_lsn(&before);
    if current_lsn > record.lsn {
        return Ok(());
    }
    if current_lsn == record.lsn {
        if crc32fast::hash(&before) == *after_crc {
            return Ok(());
        }
        // A pre-encoded delta may carry the same canonical zero-LSN image
        // as an FPI. Compare its exact image checksum after normalizing only
        // the standard LSN/checksum header; payload corruption still fails.
        set_page_lsn_if_formatted(&mut before, 0);
        return if crc32fast::hash(&before) == *after_crc {
            Ok(())
        } else {
            Err(fail("page delta after-image checksum mismatch"))
        };
    }
    if current_lsn != *base_lsn || crc32fast::hash(&before) != *base_crc {
        return Err(fail(&format!("page delta base LSN or checksum mismatch: page={} expected_lsn={} actual_lsn={} expected_crc={:08x} actual_crc={:08x}", page_id, base_lsn, current_lsn, base_crc, crc32fast::hash(&before))));
    }
    let mut after = vec![0; PAGE_SIZE];
    let size = lz4_flex::block::decompress_into_with_dict(compressed_after, &mut after, &before)
        .map_err(|_| fail("corrupt page delta compression"))?;
    if size != PAGE_SIZE
        || crc32fast::hash(&after) != *after_crc
        || (page_lsn(&after) != 0 && page_lsn(&after) != record.lsn)
    {
        return Err(fail(
            "page delta after-image length, checksum or LSN mismatch",
        ));
    }
    validate(&after)?;
    if page_lsn(&after) == 0 {
        set_page_lsn_if_formatted(&mut after, record.lsn);
    }
    file.seek(SeekFrom::Start(offset)).map_err(io_err)?;
    file.write_all(&after).map_err(io_err)?;
    note_recovery_write(data_dir, data_dir.join(relative))?;
    file.flush().map_err(io_err)
}

fn redo_page_update(
    data_dir: &Path,
    relative: &str,
    page_id: u64,
    lsn: u64,
    after: &[u8],
) -> Result<(), QueryError> {
    redo_page_update_with_context(
        data_dir,
        relative,
        page_id,
        lsn,
        after,
        RecoveryContext::default(),
    )
}

fn redo_page_update_with_context(
    data_dir: &Path,
    relative: &str,
    page_id: u64,
    lsn: u64,
    after: &[u8],
    context: RecoveryContext,
) -> Result<(), QueryError> {
    // Zone Map pages are derived but their redo is physical. Validate the
    // entire image before trusting either its identity or an on-disk LSN.
    // The generic recovery header cache deliberately skips these files.
    let zone_identity = if relative.ends_with(".zm") {
        if after.len() != PAGE_SIZE {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "zone-map redo image has invalid page length",
            ));
        }
        let identity = zone_map_page::Identity {
            storage: read_u64(after, 24),
            generation: read_u64(after, 32),
            page: page_id,
        };
        zone_map_page::View::open(after, identity)?;
        if crate::storage::naming::parse_relative_object_path(relative)
            .is_some_and(|(_, info)| info.table_id != identity.storage)
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "zone-map redo owner does not match file path",
            ));
        }
        Some(identity)
    } else {
        None
    };
    if let Some(result) = with_recovery_page_io(data_dir, relative, |io| {
        io.write(page_id, lsn, after, context)
    }) {
        return result;
    }
    let path = data_dir.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(io_err)?;
    }
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(io_err)?;
    let offset = page_id
        .checked_mul(PAGE_SIZE as u64)
        .filter(|offset| offset.checked_add(PAGE_SIZE as u64).is_some())
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "recovery page offset overflow",
            )
        })?;
    let len = file.metadata().map_err(io_err)?.len();
    if len >= offset + PAGE_SIZE as u64 {
        let mut current = vec![0; PAGE_SIZE];
        file.seek(SeekFrom::Start(offset)).map_err(io_err)?;
        file.read_exact(&mut current).map_err(io_err)?;
        let valid_identity = recovery_page_valid(&current)
            && zone_identity
                .is_none_or(|identity| zone_map_page::View::open(&current, identity).is_ok());
        if valid_identity && !context.must_redo(lsn) && page_lsn(&current) >= lsn {
            return Ok(());
        }
    }
    let mut page = after.to_vec();
    page.resize(PAGE_SIZE, 0);
    set_page_lsn_if_formatted(&mut page, lsn);
    file.set_len(len.max(offset + PAGE_SIZE as u64))
        .map_err(io_err)?;
    file.seek(SeekFrom::Start(offset)).map_err(io_err)?;
    file.write_all(&page).map_err(io_err)?;
    note_recovery_write(data_dir, data_dir.join(relative))?;
    file.flush().map_err(io_err)
}

fn redo_heap_insert(
    data_dir: &Path,
    relative: &str,
    lsn: u64,
    page_id: u64,
    slot_id: u16,
    payload: &[u8],
) -> Result<(), QueryError> {
    let Some(mut page) = read_recovery_heap_page_below_lsn(data_dir, relative, page_id, lsn)?
    else {
        return Ok(());
    };
    // Skip only when the slot already holds a live row (idempotent
    // re-redo). A skipped loser below this slot leaves a FREE placeholder
    // that must still receive its committed row — see `insert_at_slot`.
    if page.read(slot_id).is_some() {
        return write_recovery_heap_page(data_dir, relative, page_id, lsn, page);
    }
    // Commit-filtered REDO skips loser inserts, so `insert` (next free
    // slot) would drift this committed row into a skipped loser's hole
    // and break the absolute slot id its index entries point at. Replay
    // at the recorded absolute slot.
    page.insert_at_slot(slot_id, payload).ok_or_else(|| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            "WAL redo heap insert does not fit",
        )
    })?;
    write_recovery_heap_page(data_dir, relative, page_id, lsn, page)
}

fn redo_heap_insert_batch(
    data_dir: &Path,
    relative: &str,
    lsn: u64,
    inserts: &[WalHeapInsert],
) -> Result<(), QueryError> {
    // `None` marks a page whose on-disk LSN already covers this record —
    // every insert targeting it is skipped instead of replayed against a
    // newer page state.
    let mut pages = BTreeMap::<u64, Option<Page>>::new();
    for insert in inserts {
        let page_id = insert.page_id as u64;
        let slot = match pages.entry(page_id) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                read_recovery_heap_page_below_lsn(data_dir, relative, page_id, lsn)?,
            ),
        };
        let Some(page) = slot.as_mut() else {
            continue;
        };
        // Skip only when the slot already holds a live row (idempotent
        // re-redo). A FREE placeholder left by a skipped loser below
        // this slot must still receive its row — see `insert_at_slot`.
        if page.read(insert.slot_id).is_some() {
            continue;
        }
        // Commit-filtered REDO skips loser inserts, so re-inserting with
        // `insert` (next free slot) would drift this committed row down
        // into a skipped loser's hole and break the absolute slot id its
        // index entries point at. Replay at the recorded absolute slot.
        page.insert_at_slot(insert.slot_id, &insert.payload)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "WAL redo heap insert batch does not fit",
                )
            })?;
    }
    for (page_id, page) in pages {
        if let Some(page) = page {
            write_recovery_heap_page(data_dir, relative, page_id, lsn, page)?;
        }
    }
    Ok(())
}

fn redo_heap_update(
    data_dir: &Path,
    relative: &str,
    lsn: u64,
    page_id: u64,
    slot_id: u16,
    payload: &[u8],
) -> Result<(), QueryError> {
    let Some(mut page) = read_recovery_heap_page_below_lsn(data_dir, relative, page_id, lsn)?
    else {
        return Ok(());
    };
    if page.read(slot_id).is_some() {
        page.update(slot_id, payload)?;
    } else {
        // Commit-filtered recovery leaves holes for skipped loser inserts.
        // Compensation must retain the absolute row identity as insert redo does.
        page.insert_at_slot(slot_id, payload).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "WAL redo heap update does not fit",
            )
        })?;
    }
    write_recovery_heap_page(data_dir, relative, page_id, lsn, page)
}

/// Redo a `WalPayload::HeapUpdateLogical` by applying its column-level
/// byte patches to the row at `slot_id`. Idempotent — running redo
/// twice writes the same bytes. If the slot is missing or stale
/// (rollback already replayed under a CLR, or a more recent page LSN
/// already covers this update), redo is a no-op. Patches outside the
/// slot length cause a hard error: that indicates either a corrupt
/// WAL or a slot/payload mismatch the writer should have caught.
fn redo_heap_update_logical(
    data_dir: &Path,
    relative: &str,
    lsn: u64,
    page_id: u64,
    slot_id: u16,
    patches: &[WalColumnPatch],
) -> Result<(), QueryError> {
    let owned_patches: Vec<(usize, Vec<u8>)> = patches
        .iter()
        .map(|p| (p.offset as usize, p.after.clone()))
        .collect();
    redo_heap_update_logical_with_owned(
        data_dir,
        relative,
        lsn,
        page_id,
        slot_id,
        &owned_patches,
        None,
    )
}

/// Internal patch-applier shared between the redo arm
/// (`patches[i].after`) and the recovery-undo arm (`patches[i].before`
/// of the same record, packaged into owned `(offset, bytes)` pairs
/// before calling this).
fn redo_heap_update_logical_with_owned(
    data_dir: &Path,
    relative: &str,
    lsn: u64,
    page_id: u64,
    slot_id: u16,
    patches: &[(usize, Vec<u8>)],
    undo_identity: Option<(u64, Option<u64>)>,
) -> Result<(), QueryError> {
    let Some(mut page) = read_recovery_heap_page_below_lsn(data_dir, relative, page_id, lsn)?
    else {
        return Ok(());
    };
    if page.read(slot_id).is_none() {
        return write_recovery_heap_page(data_dir, relative, page_id, lsn, page);
    }
    if let Some((txn_id, undo_ptr)) = undo_identity {
        let (header, _, _) = peek_mvcc_header(page.read(slot_id).unwrap()).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "corrupt logical undo row header",
            )
        })?;
        // Runtime rollback restores the predecessor's MVCC identity in the
        // same WAL record as its row bytes. A crash may precede the batch
        // progress marker: never apply the old offsets to that restored row.
        // Earlier unflushed versions are skipped until their own undo record
        // is reached. Matching identities still validate every patch range.
        if header.last_txn_id != txn_id || header.undo_ptr != undo_ptr {
            return write_recovery_heap_page(data_dir, relative, page_id, lsn, page);
        }
    }
    let _before_image = page.try_patch_columns(slot_id, patches)?;
    write_recovery_heap_page(data_dir, relative, page_id, lsn, page)
}

fn redo_heap_delete(
    data_dir: &Path,
    relative: &str,
    lsn: u64,
    page_id: u64,
    slot_id: u16,
) -> Result<(), QueryError> {
    let Some(mut page) = read_recovery_heap_page_below_lsn(data_dir, relative, page_id, lsn)?
    else {
        return Ok(());
    };
    if page.read(slot_id).is_some() {
        page.delete(slot_id)?;
    }
    write_recovery_heap_page(data_dir, relative, page_id, lsn, page)
}

/// rowid-6bytes M2 — recovery counterpart of
/// `HeapFile::reclaim_slot_logged`. Reads the page, flips the slot
/// `ROW_FLAG_DELETED → ROW_FLAG_FREE`, writes the page back at `lsn`.
/// Idempotent: a slot in ACTIVE or FREE state is left untouched, so
/// replaying the same record twice (recovery → online apply) is safe.
/// Missing pages / out-of-range slot ids are treated as no-ops because
/// the heap file may have been truncated since this record was logged.
fn redo_slot_reclaim(
    data_dir: &Path,
    relative: &str,
    lsn: u64,
    page_id: u64,
    slot_id: u16,
) -> Result<(), QueryError> {
    let Some(mut page) = read_recovery_heap_page_below_lsn(data_dir, relative, page_id, lsn)?
    else {
        return Ok(());
    };
    if page.try_reclaim_deleted_for_recovery(slot_id) {
        write_recovery_heap_page(data_dir, relative, page_id, lsn, page)
    } else {
        Ok(())
    }
}

fn redo_heap_delete_batch_page(
    data_dir: &Path,
    relative: &str,
    lsn: u64,
    page_id: u64,
    inserts: &[&WalHeapInsert],
) -> Result<(), QueryError> {
    let Some(mut page) = read_recovery_heap_page_below_lsn(data_dir, relative, page_id, lsn)?
    else {
        return Ok(());
    };
    for insert in inserts.iter().rev() {
        if page.read(insert.slot_id).is_some() {
            page.delete(insert.slot_id)?;
        }
    }
    write_recovery_heap_page(data_dir, relative, page_id, lsn, page)
}

/// Read a heap page for redo, returning `None` when the on-disk page
/// already carries `page_lsn >= lsn` — i.e. it was flushed with this
/// record's effect (or a later one) applied. Mutating first and relying
/// on the write-side LSN guard in `redo_page_update` is not enough:
/// replaying an old record against a newer page state can fail hard
/// before reaching the write (e.g. a heap insert whose slot was later
/// reclaimed and reused resolves to a different slot id).
fn read_recovery_heap_page_below_lsn(
    data_dir: &Path,
    relative: &str,
    page_id: u64,
    lsn: u64,
) -> Result<Option<Page>, QueryError> {
    // A physiological record cannot be replayed on a newer copied page:
    // slots may have been reclaimed or reused. Forced full-page images rebuild
    // the page at their own LSN, after which ordinary redo naturally resumes.
    if let Some(result) = with_recovery_page_io(data_dir, relative, |io| io.lsn(page_id)) {
        if result? >= lsn {
            return Ok(None);
        }
    }
    let buf = read_recovery_page(data_dir, relative, page_id)?;
    if recovery_page_valid(&buf) && page_lsn(&buf) >= lsn {
        return Ok(None);
    }
    Page::decode(&buf).map(Some)
}

fn read_recovery_page(
    data_dir: &Path,
    relative: &str,
    page_id: u64,
) -> Result<Vec<u8>, QueryError> {
    if let Some(result) = with_recovery_page_io(data_dir, relative, |io| io.read(page_id)) {
        return result;
    }
    let path = data_dir.join(relative);
    let offset = page_id * PAGE_SIZE as u64;
    if !path.exists() {
        return Ok(vec![0; PAGE_SIZE]);
    }
    let mut file = OpenOptions::new().read(true).open(path).map_err(io_err)?;
    if file.metadata().map_err(io_err)?.len() < offset + PAGE_SIZE as u64 {
        return Ok(vec![0; PAGE_SIZE]);
    }
    let mut buf = vec![0; PAGE_SIZE];
    file.seek(SeekFrom::Start(offset)).map_err(io_err)?;
    file.read_exact(&mut buf).map_err(io_err)?;
    Ok(buf)
}

fn write_recovery_heap_page(
    data_dir: &Path,
    relative: &str,
    page_id: u64,
    lsn: u64,
    page: Page,
) -> Result<(), QueryError> {
    let mut buf = page.encode();
    set_page_lsn(&mut buf, lsn);
    refresh_page_checksum(&mut buf);
    redo_page_update(data_dir, relative, page_id, lsn, &buf)
}

fn redo_index_batch(
    data_dir: &Path,
    log: Arc<crate::wal::LogMutex>,
    indexes: &mut HashMap<String, IndexFile>,
    relative: &str,
    lsn: u64,
    txn_id: u64,
    ops: &[WalIndexOp],
    native_recovery_ceiling: Option<u64>,
    context: RecoveryContext,
) -> Result<(), QueryError> {
    let _routing = btree_logical_wal::LsnScope::new(Some(lsn));
    let index = match indexes.entry(relative.to_string()) {
        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::hash_map::Entry::Vacant(entry) => entry.insert(
            IndexFile::open_for_recovery(data_dir.join(relative), log, lsn)?,
        ),
    };
    let mut entries = Vec::with_capacity(ops.len());
    for op in ops {
        entries.push(IndexBatchEntry {
            kind: op.kind,
            key: decode_wal_index_key(&op.key)?,
            row_id: wal_row_id_to_row_id(op.row_id),
        });
    }
    if let Some(ceiling) = native_recovery_ceiling.filter(|_| !context.must_redo(lsn)) {
        if index.recovery_batch_already_persisted(&entries, lsn, ceiling)? {
            return Ok(());
        }
    }
    // Shared with online physical replication: empty all-insert batches build
    // bottom-up, later strictly increasing chunks append at the right edge,
    // and mixed/unsorted batches retain the per-op correctness oracle.
    index.apply_replicated_batch(txn_id, entries, lsn)?;
    Ok(())
}

fn is_rebuildable_index_redo_error(err: &QueryError) -> bool {
    // WAL payload/key decoding is validated before the B-tree apply call.
    // An engine error returned by that apply path is therefore an index
    // storage failure and may be rebuilt from heap/WAL state.
    err.is(crate::error::catalog::ER_GET_ERRNO)
}

fn decode_wal_index_key(bytes: &[u8]) -> Result<IndexKey, QueryError> {
    let mut offset = 0;
    let key = decode_index_key_from(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt WAL index key payload",
        ));
    }
    Ok(key)
}

fn wal_row_id_to_row_id(row_id: WalRowId) -> RowId {
    RowId {
        page_id: row_id.page_id,
        slot_id: row_id.slot_id,
    }
}

#[cfg(test)]
mod undo_audit_recovery_tests {
    use super::*;

    #[test]
    fn audit1022_heap_update_replays_at_absolute_slot_after_skipped_inserts() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = "objects/tables/1/1.heap";
        write_recovery_heap_page(dir.path(), path, 0, 1, Page::new()).unwrap();
        redo_heap_update(dir.path(), path, 2, 0, 6, b"committed").unwrap();
        redo_heap_update(dir.path(), path, 2, 0, 6, b"committed").unwrap();
        let page = Page::decode(&read_recovery_page(dir.path(), path, 0).unwrap()).unwrap();
        assert_eq!(page.read(6), Some(b"committed".as_slice()));
        for slot in 0..6 {
            assert!(page.read(slot).is_none());
        }
    }

    #[test]
    fn ddl_retirement_redo_requires_commit_and_preserves_live_generation() {
        let old = "objects/tables/1/3.heap";
        let live = "objects/tables/1/4.heap";
        let update = |lsn, path: &str| WalRecord {
            source_time_us: 0,
            lsn,
            txn_id: 0,
            prev_txn_lsn: 0,
            payload: WalPayload::HeapUpdate {
                path: path.into(),
                page_id: 0,
                slot_id: 2,
                before: vec![],
                after: vec![],
                undo_lsn: None,
            },
        };
        let records = vec![
            update(10, old),
            WalRecord {
                source_time_us: 0,
                lsn: 11,
                txn_id: 7,
                prev_txn_lsn: 0,
                payload: WalPayload::DdlCommit {
                    job_id: 1,
                    retired_paths: vec![old.into()],
                },
            },
            update(12, live),
        ];
        // A prepared or partially received replica transaction may be redoable
        // but cannot retire the old generation until CommitTxn is durable.
        for committed in [
            RecoveryTxnSet::from_iter([]),
            RecoveryTxnSet::from_iter([7]),
        ] {
            let (heap, _) =
                plan_redo_file_partitions(&records, &RecoveryTxnSet::from_iter([7]), &committed, 0)
                    .unwrap();
            assert_eq!(heap.contains_key(old), !committed.contains(&7));
            assert_eq!(heap[live], vec![2]);
        }
        // New records for a reused path remain visible; the fence removes only
        // the superseded prefix, not all future incarnations of that pathname.
        let mut reused = records;
        reused.push(update(13, old));
        let (heap, _) = plan_redo_file_partitions(
            &reused,
            &RecoveryTxnSet::from_iter([7]),
            &RecoveryTxnSet::from_iter([7]),
            0,
        )
        .unwrap();
        assert_eq!(heap[old], vec![3]);
    }

    #[test]
    fn undo_audit_redo_deletion_discards_only_obsolete_file_incarnation() {
        let path = "objects/tables/1/1.heap";
        let record = |lsn, txn_id, payload| WalRecord {
            source_time_us: 0,
            lsn,
            txn_id,
            prev_txn_lsn: 0,
            payload,
        };
        let mut records = vec![
            record(
                10,
                0,
                WalPayload::HeapUpdate {
                    path: path.into(),
                    page_id: 0,
                    slot_id: 1,
                    before: vec![],
                    after: vec![],
                    undo_lsn: None,
                },
            ),
            record(
                11,
                7,
                WalPayload::FileDelete {
                    path: path.into(),
                    before: vec![],
                },
            ),
            record(
                12,
                8,
                WalPayload::FileCreate {
                    path: path.into(),
                    after: vec![],
                },
            ),
        ];
        let (heap, _) = plan_redo_file_partitions(
            &records,
            &RecoveryTxnSet::from_iter([7, 8]),
            &RecoveryTxnSet::from_iter([7, 8]),
            9,
        )
        .unwrap();
        assert_eq!(
            heap[path],
            vec![2],
            "a replacement supersedes the preceding deletion"
        );
        let (heap, _) = plan_redo_file_partitions(
            &records,
            &RecoveryTxnSet::from_iter([8]),
            &RecoveryTxnSet::from_iter([8]),
            9,
        )
        .unwrap();
        assert_eq!(
            heap[path],
            vec![2],
            "a committed replacement supersedes old-file compensation"
        );
        let (heap, _) = plan_redo_file_partitions(
            &records,
            &RecoveryTxnSet::from_iter([7, 8]),
            &RecoveryTxnSet::from_iter([7, 8]),
            12,
        )
        .unwrap();
        assert_eq!(
            heap[path],
            vec![2],
            "a deletion below cutoff is not a new redo boundary"
        );
        records.pop();
        let (heap, _) = plan_redo_file_partitions(
            &records[..1],
            &RecoveryTxnSet::from_iter([]),
            &RecoveryTxnSet::from_iter([]),
            9,
        )
        .unwrap();
        assert_eq!(
            heap[path],
            vec![0],
            "live-file rollback compensation must remain redoable"
        );
    }
}

#[cfg(test)]
mod zone_map_physical_redo_tests {
    use super::*;

    #[test]
    fn zone_map_page_delta_zero_lsn_image_replays_and_checks_idempotent_payload() {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let relative = "objects/tables/17/23.zm";
        let path = dir.path().join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let identity = zone_map_page::Identity {
            storage: 17,
            generation: 23,
            page: 0,
        };
        let key = zone_map_page::Key {
            column: 1,
            kind: zone_map_page::Kind::Bounds as u8,
            zone: 0,
        };
        let image = |lsn, value: &[u8]| {
            let mut bytes =
                zone_map_page::encode(identity, zone_map_page::Kind::Records, 0, &[(key, value)])
                    .unwrap();
            set_page_lsn_if_formatted(&mut bytes, lsn);
            bytes
        };
        let mut before = image(10, b"old");
        fs::write(&path, &before).unwrap();
        // A normalized delta can follow another normalized delta: dictionary
        // CRC/LSN must refer to the stamped resident image, not its encoding.
        for lsn in [20, 30] {
            let encoded = image(0, &[lsn as u8; 256]);
            let record = WalRecord {
                source_time_us: 0,
                lsn,
                txn_id: 0,
                prev_txn_lsn: 0,
                payload: WalPayload::page_delta(
                    relative.into(),
                    0,
                    page_lsn(&before),
                    &before,
                    &encoded,
                )
                .unwrap(),
            };
            redo_page_delta(dir.path(), &record).unwrap();
            let expected = image(lsn, &[lsn as u8; 256]);
            assert_eq!(fs::read(&path).unwrap(), expected);
            redo_page_delta(dir.path(), &record).unwrap();
            assert_eq!(fs::read(&path).unwrap(), expected);
            // Even a structurally valid page with the same LSN must not hide
            // a different payload behind the idempotent-replay shortcut.
            let wrong = image(lsn, &[99; 256]);
            fs::write(&path, &wrong).unwrap();
            assert!(redo_page_delta(dir.path(), &record).is_err());
            assert_eq!(fs::read(&path).unwrap(), wrong);
            fs::write(&path, &expected).unwrap();
            before = expected;
        }
    }

    #[test]
    fn zone_map_page_delta_database_restart_recovers_torn_page() {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let path = dir.path().join("objects/tables/17/23.zm");
        let expected = {
            let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
            let file = zone_map_page::PageFile::new(
                path.clone(),
                17,
                23,
                Arc::clone(&db.buffer_pool),
                Arc::clone(&db.log),
                DurabilityMode::Strict,
            )
            .unwrap();
            let key = zone_map_page::Key {
                column: 1,
                kind: zone_map_page::Kind::Bounds as u8,
                zone: 0,
            };
            let mut state = 0x123456789abcdefu64;
            let mut value = (0..8192)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state as u8
                })
                .collect::<Vec<_>>();
            let first = file
                .write(0, zone_map_page::Kind::Bounds, 0, &[(key, &value)])
                .unwrap();
            value[100] ^= 1;
            let last = file
                .write(0, zone_map_page::Kind::Bounds, 0, &[(key, &value)])
                .unwrap();
            LogManager::sync_durable_to(&db.log, last).unwrap();
            let records = LogManager::scan(dir.path()).unwrap().records;
            let first_record = records.iter().find(|record| record.lsn == first).unwrap();
            let image = crate::wal::wal_full_page_image_after(&first_record.payload).unwrap();
            assert_eq!(
                page_lsn(&image),
                0,
                "FPI compression precedes WAL reservation"
            );
            assert!(records.iter().any(|record| record.lsn == last
                && matches!(record.payload, WalPayload::PageDelta { .. })));
            file.flush().unwrap();
            fs::read(&path).unwrap()
        };
        let mut torn = expected.clone();
        torn[1000] ^= 1;
        fs::write(&path, torn).unwrap();
        let _reopened = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        assert_eq!(fs::read(&path).unwrap(), expected);
    }

    #[test]
    fn zone_map_page_delta_requires_exact_base_and_recovers_torn_fpi_chain() {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let relative = "objects/tables/17/23.zm";
        let path = dir.path().join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let identity = zone_map_page::Identity {
            storage: 17,
            generation: 23,
            page: 0,
        };
        let key = zone_map_page::Key {
            column: 1,
            kind: zone_map_page::Kind::Bounds as u8,
            zone: 0,
        };
        let image = |lsn, value: &[u8]| {
            let mut bytes =
                zone_map_page::encode(identity, zone_map_page::Kind::Records, 0, &[(key, value)])
                    .unwrap();
            set_page_lsn_if_formatted(&mut bytes, lsn);
            bytes
        };
        let before = image(10, b"before");
        let after = image(20, b"after with a different length");
        let final_image = image(30, b"final");
        let delta = |lsn, base_lsn, before: &[u8], after: &[u8]| WalRecord {
            source_time_us: 0,
            lsn,
            txn_id: 0,
            prev_txn_lsn: 0,
            payload: WalPayload::page_delta(relative.into(), 0, base_lsn, before, after).unwrap(),
        };
        let first = delta(20, 10, &before, &after);
        let second = delta(30, 20, &after, &final_image);
        assert!(
            redo_page_delta(dir.path(), &first).is_err(),
            "missing baseline must fail"
        );
        fs::write(&path, &before).unwrap();
        redo_page_delta(dir.path(), &first).unwrap();
        assert_eq!(fs::read(&path).unwrap(), after);
        redo_page_delta(dir.path(), &first).unwrap();
        redo_page_delta(dir.path(), &second).unwrap();
        redo_page_delta(dir.path(), &first).unwrap();
        assert_eq!(fs::read(&path).unwrap(), final_image);
        let mut wrong = before.clone();
        set_page_lsn_if_formatted(&mut wrong, 11);
        fs::write(&path, &wrong).unwrap();
        assert!(redo_page_delta(dir.path(), &first).is_err());
        assert_eq!(fs::read(&path).unwrap(), wrong);
        fs::write(&path, &before).unwrap();
        let mut bad = first.clone();
        if let WalPayload::PageDelta { after_crc, .. } = &mut bad.payload {
            *after_crc ^= 1;
        }
        assert!(redo_page_delta(dir.path(), &bad).is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
        let mut short = first.clone();
        if let WalPayload::PageDelta {
            compressed_after, ..
        } = &mut short.payload
        {
            *compressed_after = lz4_flex::block::compress_with_dict(&after[..100], &before);
        }
        assert!(redo_page_delta(dir.path(), &short).is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
        let mut wrong_generation = before.clone();
        write_u64(&mut wrong_generation, 32, 24);
        refresh_page_checksum(&mut wrong_generation);
        fs::write(&path, &wrong_generation).unwrap();
        assert!(redo_page_delta(dir.path(), &first).is_err());
        assert_eq!(fs::read(&path).unwrap(), wrong_generation);
        let mut torn = final_image.clone();
        torn[200] ^= 1;
        fs::write(&path, torn).unwrap();
        assert!(
            redo_page_delta(dir.path(), &second).is_err(),
            "torn base cannot be patched"
        );
        let _scope = RecoveryIoScope::enter().unwrap();
        redo_page_update(dir.path(), relative, 0, 10, &before).unwrap();
        redo_page_delta(dir.path(), &first).unwrap();
        redo_page_delta(dir.path(), &second).unwrap();
        assert_eq!(fs::read(&path).unwrap(), final_image);
    }

    #[test]
    fn restore_redo_does_not_change_another_database_recovery() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let relative = "objects/tables/17/23.zm";
        let path = dir.path().join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let identity = zone_map_page::Identity {
            storage: 17,
            generation: 23,
            page: 0,
        };
        let mut page = zone_map_page::encode(identity, zone_map_page::Kind::Meta, 0, &[]).unwrap();
        set_page_lsn_if_formatted(&mut page, 999);
        fs::write(&path, page).unwrap();
        let restore = RecoveryContext::restore(10, 100).unwrap();
        assert!(!existing_file_generation_covers_lsn_with_context(
            dir.path(),
            relative,
            77,
            restore
        )
        .unwrap());
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    assert!(
                        existing_file_generation_covers_lsn(dir.path(), relative, 77).unwrap(),
                        "an unrelated database must retain its normal page-LSN semantics"
                    );
                })
                .join()
                .unwrap();
        });
    }

    #[test]
    fn zone_map_redo_repairs_corrupt_high_lsn_and_wrong_generation() {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let relative = "objects/tables/17/23.zm";
        let path = dir.path().join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let identity = zone_map_page::Identity {
            storage: 17,
            generation: 23,
            page: 0,
        };
        let image = zone_map_page::encode(identity, zone_map_page::Kind::Meta, 0, &[]).unwrap();
        for wrong_generation in [false, true] {
            let mut current = image.clone();
            set_page_lsn_if_formatted(&mut current, 999);
            if wrong_generation {
                write_u64(&mut current, 32, 22);
                refresh_page_checksum(&mut current);
            } else {
                current[100] ^= 1;
            }
            fs::write(&path, current).unwrap();
            // Exercise startup's header cache as well as the online path.
            let _scope = RecoveryIoScope::enter().unwrap();
            redo_page_update(dir.path(), relative, 0, 77, &image).unwrap();
            let recovered = fs::read(&path).unwrap();
            zone_map_page::View::open(&recovered, identity).unwrap();
            assert_eq!(page_lsn(&recovered), 77);
            redo_page_update(dir.path(), relative, 0, 76, &image).unwrap();
            assert_eq!(fs::read(&path).unwrap(), recovered);
        }
    }

    #[test]
    fn zone_map_redo_online_apply_retires_cached_dirty_pages() {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Memory).unwrap();
        let relative = "objects/tables/17/23.zm";
        let path = dir.path().join(relative);
        let file = zone_map_page::PageFile::new(
            path.clone(),
            17,
            23,
            Arc::clone(&db.buffer_pool),
            Arc::clone(&db.log),
            DurabilityMode::Memory,
        )
        .unwrap();
        let key = zone_map_page::Key {
            column: 1,
            kind: zone_map_page::Kind::Bounds as u8,
            zone: 0,
        };
        file.write(0, zone_map_page::Kind::Bounds, 0, &[(key, b"old")])
            .unwrap();
        assert_eq!(
            file.visit(0, AccessIntent::Normal, |view| Ok(view
                .entry(0)
                .unwrap()
                .1
                .to_vec()))
                .unwrap(),
            b"old"
        );
        let image = zone_map_page::encode(
            file.identity(0),
            zone_map_page::Kind::Bounds,
            0,
            &[(key, b"new")],
        )
        .unwrap();
        let record = WalRecord {
            source_time_us: 0,
            lsn: 777,
            txn_id: 0,
            prev_txn_lsn: 0,
            payload: WalPayload::FullPageImage {
                path: relative.to_owned(),
                page_id: 0,
                after: image,
            },
        };
        db.apply_wal_record(&record).unwrap();
        file.flush().unwrap();
        assert_eq!(
            file.visit(0, AccessIntent::Normal, |view| Ok(view
                .entry(0)
                .unwrap()
                .1
                .to_vec()))
                .unwrap(),
            b"new"
        );
        assert_eq!(page_lsn(&fs::read(&path).unwrap()), 777);
        let before = fs::read(&path).unwrap();
        let mut after = zone_map_page::encode(
            file.identity(0),
            zone_map_page::Kind::Bounds,
            0,
            &[(key, b"delta-new")],
        )
        .unwrap();
        set_page_lsn_if_formatted(&mut after, 778);
        let delta = WalRecord {
            source_time_us: 0,
            lsn: 778,
            txn_id: 0,
            prev_txn_lsn: 0,
            payload: WalPayload::page_delta(relative.into(), 0, 777, &before, &after).unwrap(),
        };
        db.apply_wal_record(&delta).unwrap();
        db.apply_wal_record(&delta).unwrap();
        assert_eq!(
            file.visit(0, AccessIntent::Normal, |view| Ok(view
                .entry(0)
                .unwrap()
                .1
                .to_vec()))
                .unwrap(),
            b"delta-new"
        );
        assert_eq!(fs::read(&path).unwrap(), after);
    }

    #[test]
    fn zone_map_redo_retires_cached_root_handle_before_publication() {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let db =
            Arc::new(Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap());
        let relative = "objects/tables/17/23.zm";
        let path = dir.path().join(relative);
        let file = zone_map_page::PageFile::new(
            path.clone(),
            17,
            23,
            Arc::clone(&db.buffer_pool),
            Arc::clone(&db.log),
            DurabilityMode::Strict,
        )
        .unwrap();
        let store = Arc::new(zone_map_page::mutable::Store::create(file, 123, 8).unwrap());
        store.flush().unwrap();
        let map = Arc::new(ZoneMap::new(17, 8, 123, store, true));
        db.zone_maps.write().unwrap().insert(17, Arc::clone(&map));
        let mut image = fs::read(&path).unwrap();
        image.truncate(PAGE_SIZE);
        let reading = map.data_gate.read().unwrap();
        let applying = Arc::clone(&db);
        let worker = std::thread::spawn(move || {
            applying.apply_wal_record(&WalRecord {
                source_time_us: 0,
                lsn: 777,
                txn_id: 0,
                prev_txn_lsn: 0,
                payload: WalPayload::FullPageImage {
                    path: relative.to_owned(),
                    page_id: 0,
                    after: image,
                },
            })
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !map.retired.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(map.retired.load(Ordering::Acquire));
        assert!(map
            .may_match_zone(0, &ZonePredicateProgram::Never, u64::MAX)
            .is_err());
        assert_ne!(page_lsn(&fs::read(&path).unwrap()), 777);
        drop(reading);
        worker.join().unwrap().unwrap();
        assert!(!db.zone_maps.read().unwrap().contains_key(&17));
        assert!(map.read_zone(0, None).is_err());
        let file = zone_map_page::PageFile::new(
            path,
            17,
            23,
            Arc::clone(&db.buffer_pool),
            Arc::clone(&db.log),
            DurabilityMode::Strict,
        )
        .unwrap();
        assert!(zone_map_page::mutable::Store::open_existing(file, 123, 8)
            .unwrap()
            .trusted());
    }

    #[test]
    fn zone_map_backup_capture_flushes_leaf_after_ready_root_writeback() {
        check_zone_map_backup_leaf_capture(b"new-long", true);
    }

    #[test]
    fn zone_map_backup_capture_flushes_atomic_leaf_with_unchanged_root() {
        check_zone_map_backup_leaf_capture(b"new", false);
    }

    fn check_zone_map_backup_leaf_capture(new_value: &[u8], root_dirty: bool) {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Strict).unwrap();
        let path = dir.path().join("objects/tables/17/23.zm");
        let file = zone_map_page::PageFile::new(
            path.clone(),
            17,
            23,
            Arc::clone(&db.buffer_pool),
            Arc::clone(&db.log),
            DurabilityMode::Strict,
        )
        .unwrap();
        let store = zone_map_page::mutable::Store::create(file, 123, 8).unwrap();
        let key = zone_map_page::Key {
            column: 1,
            kind: zone_map_page::Kind::Bounds as u8,
            zone: 0,
        };
        store.apply(&[(key, b"old".to_vec())]).unwrap();
        store.flush().unwrap();
        let mut updates = vec![(key, new_value.to_vec())];
        if root_dirty {
            // A new record changes root metadata even when both payloads fit
            // in the existing leaf; replacement-only updates keep it clean.
            updates.push((zone_map_page::Key { zone: 1, ..key }, b"extra".to_vec()));
        }
        store.apply(&updates).unwrap();
        let file_key = Arc::new(BufferPool::zone_map_file_key(path.clone(), 17, 23));
        let root_frame = db
            .buffer_pool
            .lookup_frame(&PageKey {
                file: file_key,
                page_id: 0,
            })
            .unwrap()
            .unwrap();
        assert_eq!(db.buffer_pool.flush_frame(&root_frame).unwrap(), root_dirty);
        let leaf = store.root_snapshot().unwrap().page;
        let disk_value = || {
            let bytes = fs::read(&path).unwrap();
            let first = leaf as usize * PAGE_SIZE;
            let view = zone_map_page::View::open(
                &bytes[first..first + PAGE_SIZE],
                zone_map_page::Identity {
                    storage: 17,
                    generation: 23,
                    page: leaf,
                },
            )
            .unwrap();
            view.entry(view.lower_bound(key)).unwrap().1.to_vec()
        };
        assert_eq!(
            disk_value(),
            b"old",
            "ready-root writeback alone is not a complete baseline"
        );
        {
            let _capture = db.zone_map_backup_capture_guard().unwrap();
            assert_eq!(
                disk_value(),
                new_value,
                "backup capture flushes the complete frozen page image"
            );
        }
    }

    #[test]
    fn zone_map_redo_rejects_invalid_images_before_creating_file() {
        fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let relative = "objects/tables/17/23.zm";
        let identity = zone_map_page::Identity {
            storage: 17,
            generation: 23,
            page: 0,
        };
        let image = zone_map_page::encode(identity, zone_map_page::Kind::Meta, 0, &[]).unwrap();
        assert!(redo_page_update(dir.path(), relative, 0, 77, &image[..100]).is_err());
        assert!(redo_page_update(dir.path(), relative, 1, 77, &image).is_err());
        assert!(redo_page_update(dir.path(), "objects/tables/99/23.zm", 0, 77, &image).is_err());
        let mut damaged = image;
        damaged[100] ^= 1;
        assert!(redo_page_update(dir.path(), relative, 0, 77, &damaged).is_err());
        assert!(!dir.path().join(relative).exists());
    }
}

#[cfg(test)]
mod round2_recovery_tests {
    use super::*;

    #[test]
    fn round2_interleaved_loser_page_images_undo_in_wal_order() {
        for _ in 0..12 {
            let dir = tempfile::tempdir().unwrap();
            let path = "round2-page.raw";
            let mut log = LogManager::open(dir.path()).unwrap();
            log.append(11, WalPayload::BeginTxn { start_scn: 1 })
                .unwrap();
            log.append(22, WalPayload::BeginTxn { start_scn: 1 })
                .unwrap();
            let mut image = vec![0; PAGE_SIZE];
            for (txn, value) in [(11, 1), (22, 2), (11, 3), (22, 4)] {
                let before = image.clone();
                image[100] = value;
                log.append(
                    txn,
                    WalPayload::PageUpdate {
                        path: path.into(),
                        page_id: 0,
                        before,
                        after: image.clone(),
                        undo_lsn: None,
                    },
                )
                .unwrap();
            }
            log.flush().unwrap();
            fs::write(dir.path().join(path), image).unwrap();
            drop(log);
            let result = recover_from_wal(dir.path(), DurabilityMode::Strict).unwrap();
            assert_eq!(result.summary.undone_transactions, 2);
            assert_eq!(fs::read(dir.path().join(path)).unwrap()[100], 0);
            recover_from_wal(dir.path(), DurabilityMode::Strict).unwrap();
            assert_eq!(fs::read(dir.path().join(path)).unwrap()[100], 0);
        }
    }
}

/// Publish an immutable source image without re-stamping any embedded page LSN.
/// The durable receipt prevents an old CREATE image from replacing later redo.
pub(crate) fn redo_vector_image(
    data_dir: &Path, relative: &str, offset: u64, total: u64, bytes: &[u8], lsn: u64,
) -> Result<(), QueryError> {
    let path = Path::new(relative);
    if path.is_absolute() || path.components().any(|part| !matches!(part, std::path::Component::Normal(_)))
        || !relative.starts_with("objects/indexes/")
        || !matches!(path.extension().and_then(|ext| ext.to_str()), Some("hvec" | "hnsw"))
        || bytes.is_empty() || bytes.len() > 1024 * 1024
        || offset.checked_add(bytes.len() as u64).is_none_or(|end| end > total) {
        return Err(error(crate::error::catalog::ER_INDEX_CORRUPT, "invalid vector image publication"));
    }
    let path = data_dir.join(path);
    let receipt = path.with_extension(format!("{}.vpub", path.extension().unwrap().to_string_lossy()));
    match fs::read(&receipt) {
        Ok(bytes) => {
            if bytes.len() != 12 || crc32fast::hash(&bytes[..8]) != u32::from_le_bytes(bytes[8..].try_into().unwrap()) {
                return Err(error(crate::error::catalog::ER_INDEX_CORRUPT, "invalid vector publication receipt"));
            }
            if u64::from_le_bytes(bytes[..8].try_into().unwrap()) >= lsn { return Ok(()); }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(io_err(err)),
    }
    let parent = path.parent().unwrap();
    fs::create_dir_all(parent).map_err(io_err)?;
    let incoming = path.with_extension(format!("{}.incoming", path.extension().unwrap().to_string_lossy()));
    let mut file = fs::OpenOptions::new().write(true).create(true).truncate(offset == 0).open(&incoming).map_err(io_err)?;
    if file.metadata().map_err(io_err)?.len() < offset {
        return Err(error(crate::error::catalog::ER_INDEX_CORRUPT, "vector image chunk has no contiguous predecessor"));
    }
    file.seek(SeekFrom::Start(offset)).map_err(io_err)?;
    file.write_all(bytes).map_err(io_err)?;
    if offset + bytes.len() as u64 == total {
        file.set_len(total).map_err(io_err)?;
        file.sync_all().map_err(io_err)?;
        drop(file);
        invalidate_recovery_page_io(data_dir, relative);
        fs::rename(&incoming, &path).map_err(io_err)?;
        persist_vector_image_receipt(&path, lsn)?;
    }
    Ok(())
}

pub(crate) fn persist_vector_image_receipt(path: &Path, lsn: u64) -> Result<(), QueryError> {
    let receipt = path.with_extension(format!("{}.vpub", path.extension().unwrap().to_string_lossy()));
    let tmp = receipt.with_extension("vpub.tmp");
    let mut bytes = lsn.to_le_bytes().to_vec();
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    let mut file = fs::File::create(&tmp).map_err(io_err)?;
    file.write_all(&bytes).map_err(io_err)?;
    file.sync_all().map_err(io_err)?;
    fs::rename(&tmp, &receipt).map_err(io_err)?;
    crate::platform::fs::sync_path(path.parent().unwrap()).map_err(io_err)
}

pub(crate) fn redo_vector_manifest(data_dir: &Path, relative: &str, bytes: &[u8]) -> Result<(), QueryError> {
    let path = Path::new(relative);
    if !relative.starts_with("objects/indexes/") || path.extension().and_then(|e| e.to_str()) != Some("vmanifest")
        || path.components().any(|part| !matches!(part, std::path::Component::Normal(_))) {
        return Err(vector::persistence::corrupt());
    }
    let mut reader = vector::persistence::Reader::new(bytes, 7)?;
    let (storage_id, generation, index_id) = (reader.u64()?, reader.u64()?, reader.u64()?);
    let incoming = VectorGenerationManifest::decode(bytes, storage_id, generation, index_id)?;
    let path = data_dir.join(path);
    match fs::read(&path) {
        Ok(current) => {
            let current = VectorGenerationManifest::decode(&current, storage_id, generation, index_id)?;
            if current.source_lsn >= incoming.source_lsn { return Ok(()); }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {},
        Err(err) => return Err(io_err(err)),
    }
    fs::create_dir_all(path.parent().unwrap()).map_err(io_err)?;
    let tmp = path.with_extension("vmanifest.incoming");
    let mut file = fs::File::create(&tmp).map_err(io_err)?;
    file.write_all(bytes).map_err(io_err)?;
    file.sync_all().map_err(io_err)?;
    fs::rename(tmp, &path).map_err(io_err)?;
    crate::platform::fs::sync_path(path.parent().unwrap()).map_err(io_err)
}
