impl Database {
    /// Reconstruct old-key visibility for recovered in-flight writers. Physical
    /// indexes contain the current heap keys, while pre-commit readers need
    /// the committed versions reachable through undo, including deleted rows.
    fn restore_inflight_index_repairs(&self) -> Result<(), QueryError> {
        let physical_replica = self.physical_replica_mode();
        let heads = self
            .txns_read()?
            .slots
            .values()
            .filter(|slot| {
                slot.status == TxnStatus::Prepared
                    || (physical_replica && slot.status == TxnStatus::Active)
            })
            .map(|slot| (slot.txn_id, slot.undo_head.clone()))
            .collect::<Vec<_>>();
        if heads.is_empty() {
            return Ok(());
        }
        let snapshot = StorageReadSnapshot::new(self);
        for (txn_id, head) in heads {
            let mut ptr = head.get();
            let mut seen = HashSet::new();
            let mut walk = UndoChainWalk::default();
            while let Some(id) = ptr {
                walk.visit(id)?;
                let record = self
                    .load_undo_owned(id)?
                    .ok_or_else(|| undo_corruption(id, "prepared index repair undo is missing"))?;
                if record.txn_id != txn_id {
                    return Err(undo_corruption(
                        id,
                        "prepared index repair transaction mismatch",
                    ));
                }
                ptr = record.prev_undo_ptr;
                if record.before_header.is_none() || !seen.insert((record.table_id, record.row_id))
                {
                    continue;
                }
                let Some(current) = self.read_mvcc_row(record.table_id, record.row_id)? else {
                    continue;
                };
                let Some(visible) = self.visible_row(current, snapshot.txn)? else {
                    continue;
                };
                let table = self
                    .table_by_id(record.table_id)
                    .ok_or_else(|| undo_corruption(id, "prepared index repair table is missing"))?;
                let mut entries = Vec::new();
                for index in &table.indexes {
                    for key in table.index_keys(index, &visible.values)? {
                        entries.push((
                            IndexRepairKey {
                                table_id: table.id,
                                index_name: normalize_name(&index.name),
                                key,
                            },
                            record.row_id,
                            !index.is_fulltext() && !index.has_multi_valued_parts(),
                        ));
                    }
                }
                self.index_repair_state.register(
                    txn_id,
                    entries,
                    &head,
                    &self.index_repair_epochs,
                    &self.index_repair_version,
                )?;
            }
        }
        Ok(())
    }

    fn initialize_auto_increment_counters(&self) -> Result<(), QueryError> {
        // Merge the small post-checkpoint WAL overlay with its DD baseline.
        // Tables with no retained AUTO_INCREMENT redo remain fully lazy.
        for state in self.auto_increment.states() {
            let Some(table) = self.table_by_id(state.table_id) else {
                self.auto_increment.forget_table(state.table_id);
                continue;
            };
            if table.auto_increment_column_index().is_none() {
                self.auto_increment.forget_table(state.table_id);
                continue;
            }
            if let Some((version, max_value, covered_lsn)) =
                self.read_auto_increment_metadata(state.table_id)?
            {
                self.auto_increment
                    .install(state.table_id, version, max_value)?;
                let mut logged = state.logged.lock();
                if version > logged.version
                    || (version == logged.version && max_value > logged.max_value)
                {
                    *logged = crate::storage::txn::AutoIncrementLoggedState {
                        version,
                        max_value,
                        lsn: covered_lsn,
                    };
                }
            }
        }
        Ok(())
    }

    fn recover_incomplete_transactions(&mut self) -> Result<bool, QueryError> {
        // A replica's Active slots belong to the source. Their terminal WAL
        // records arrive after reconnect; local undo would fork the data and
        // append local LSNs into the source-owned WAL stream.
        if self.physical_replica_mode() {
            return Ok(false);
        }
        let active = self.txns_read()?.active_txn_ids();
        if active.is_empty() {
            return Ok(false);
        }
        for txn_id in active {
            let touched_tables = self.rollback_txn_changes(txn_id)?;
            let mut txns = self.txns_write()?;
            txns.rollback(txn_id)?;
            self.active_write_txns_atomic
                .store(txns.active_write_count, Ordering::Release);
            drop(txns);
            self.rebuild_touched_indexes(&touched_tables)?;
        }
        Ok(true)
    }

    fn run_startup_validation(
        &self,
        recovery: &mut RecoverySummary,
        clean_checkpoint_startup: bool,
    ) -> Result<(), QueryError> {
        let started = Instant::now();
        let pending_path = self.data_dir.join("system/recovery-indexes.pending");
        let pending_repair = pending_path.try_exists().map_err(io_err)?;
        if pending_repair {
            let paths: BTreeSet<String> =
                serde_json::from_slice(&fs::read(&pending_path).map_err(io_err)?)
                    .map_err(|err| error(crate::error::catalog::ER_GET_ERRNO, err.to_string()))?;
            for relative in paths {
                let path = self.data_dir.join(relative);
                // A rolled-back CREATE/DROP may have removed the generation.
                if path.try_exists().map_err(io_err)? {
                    IndexFile::persist_rebuild_marker(&path)?;
                }
            }
        }
        // Dictionary heaps are authoritative too. The ordinary index pass
        // below only visits user tables, but later startup steps already use
        // indexed DD property/sequence probes. Repair fenced dictionary trees
        // before those probes, including after a previously interrupted repair.
        recovery.repaired_indexes += self.rebuild_failed_dictionary_indexes()?;
        recovery.startup_validation_mode = Some(self.config.startup_validation);
        let path = match self.config.startup_validation {
            StartupValidationMode::Off => StartupValidationPath::Off,
            StartupValidationMode::Full => StartupValidationPath::Full,
            StartupValidationMode::Auto if clean_checkpoint_startup => {
                StartupValidationPath::CleanFast
            }
            StartupValidationMode::Auto => StartupValidationPath::Full,
        };
        recovery.startup_validation_path = Some(path);
        let force_index_rebuild = recovery.skipped_index_redo_records > 0 || pending_repair;

        match path {
            StartupValidationPath::Off => {
                if force_index_rebuild {
                    let indexes = self.validate_or_rebuild_indexes(IndexValidation::Quick)?;
                    recovery.repaired_indexes += indexes.repaired_indexes;
                    recovery.quick_checked_index_files += indexes.quick_checked_index_files;
                    recovery.checked_index_files += indexes.checked_index_files;
                }
                let heap = self.skipped_heap_files_fast();
                recovery.skipped_heap_files += heap.skipped_files;
                recovery.skipped_heap_pages += heap.skipped_pages;
                recovery.skipped_index_files += self.index_file_count_fast();
            }
            StartupValidationPath::CleanFast => {
                // A clean checkpoint already proves page durability. Validate
                // DD roots on the critical path and defer user object shape
                // checks until first open/background scrub. Only an explicit
                // recovery signal that index redo was skipped forces a
                // synchronous rebuild.
                if force_index_rebuild {
                    let index_validation =
                        self.validate_or_rebuild_indexes(IndexValidation::Full)?;
                    recovery.repaired_indexes += index_validation.repaired_indexes;
                    recovery.quick_checked_index_files +=
                        index_validation.quick_checked_index_files;
                    recovery.checked_index_files += index_validation.checked_index_files;
                } else if self.config.startup_validate_indexes
                    && self
                        .catalog_read()
                        .map(|catalog| catalog.tables.len() <= 4_096)
                        .unwrap_or(false)
                {
                    let index_validation =
                        self.validate_or_rebuild_indexes(IndexValidation::Quick)?;
                    recovery.repaired_indexes += index_validation.repaired_indexes;
                    recovery.quick_checked_index_files +=
                        index_validation.quick_checked_index_files;
                    recovery.checked_index_files += index_validation.checked_index_files;
                } else {
                    // A clean checkpoint already proves every user page was
                    // durable. Descriptor decode and index shape validation
                    // are deferred to first open/background scrub; counting
                    // files must likewise avoid 100k DD descriptor probes.
                    recovery.skipped_index_files += self.index_file_count_fast();
                }
            }
            StartupValidationPath::Full => {
                let heap = self.validate_heap_files()?;
                recovery.checked_heap_files += heap.checked_files;
                recovery.checked_heap_pages += heap.checked_pages;
                if self.config.startup_validate_indexes || force_index_rebuild {
                    let index_validation =
                        self.validate_or_rebuild_indexes(IndexValidation::Full)?;
                    recovery.repaired_indexes += index_validation.repaired_indexes;
                    recovery.checked_index_files += index_validation.checked_index_files;
                    recovery.quick_checked_index_files +=
                        index_validation.quick_checked_index_files;
                } else {
                    recovery.skipped_index_files += self.index_file_count()?;
                }
            }
        }
        if pending_repair {
            fs::remove_file(&pending_path).map_err(io_err)?;
            crate::platform::fs::sync_path(pending_path.parent().expect("system directory"))
                .map_err(io_err)?;
        }
        recovery.startup_validation_us =
            started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        Ok(())
    }

    fn rebuild_failed_dictionary_indexes(&self) -> Result<usize, QueryError> {
        if self.config.durability == DurabilityMode::Memory {
            return Ok(0);
        }
        let mut repaired = 0;
        for table in crate::storage::data_dictionary::dd_tables() {
            let mut markers = Vec::new();
            for index in &table.indexes {
                let marker = self
                    .index_path(table.id, &index.name)
                    .with_extension("idx.failed");
                if marker.try_exists().map_err(io_err)? {
                    markers.push(marker);
                }
            }
            if markers.is_empty() {
                continue;
            }
            self.ensure_writable()?;
            self.rebuild_indexes(table)?;
            // Keep the fences until every replacement tree is published.
            // A crash before removal simply repeats the idempotent rebuild.
            for marker in markers {
                fs::remove_file(&marker).map_err(io_err)?;
                crate::platform::fs::sync_path(marker.parent().expect("index directory"))
                    .map_err(io_err)?;
            }
            repaired += table.indexes.len();
        }
        Ok(repaired)
    }

    fn validate_or_rebuild_indexes(
        &self,
        validation: IndexValidation,
    ) -> Result<IndexValidationSummary, QueryError> {
        let mut summary = IndexValidationSummary::default();
        // File validation needs only object headers and persisted index IDs.
        // Loading complete descriptors here performs column/FK/statistics
        // probes for every table, defeating the lazy catalog on clean startup.
        let tables = self
            .catalog_read()?
            .tables
            .values()
            .filter(|table| !table.system)
            .cloned()
            .collect::<Vec<_>>();
        let table_positions = tables
            .iter()
            .enumerate()
            .map(|(position, table)| (table.id, position))
            .collect::<HashMap<_, _>>();
        // Indexes are local to each physical partition leaf.  Base backup
        // deliberately omits derived index files, so startup must validate
        // (and, when absent, rebuild) every `(storage_id, index)` pair rather
        // than the logical table id alone.
        let mut tasks: Vec<(usize, u64, PathBuf)> = Vec::new();
        let mut index_counts = vec![0usize; tables.len()];
        for row in self.catalog_rows_for_selector(
            crate::storage::data_dictionary::DD_INDEXES,
            CatalogRowSelector::All,
        )? {
            let Some((table_id, index_id)) =
                crate::storage::data_dictionary::index_storage_identity(&row)
            else {
                continue;
            };
            let Some(&position) = table_positions.get(&table_id) else {
                continue;
            };
            let table = &tables[position];
            for storage_id in table.storage_ids() {
                tasks.push((
                    position,
                    storage_id,
                    crate::storage::naming::index_object_path_by_id(
                        &self.data_dir,
                        storage_id,
                        table.active_generation.max(1),
                        index_id,
                        "idx",
                    ),
                ));
                index_counts[position] += 1;
            }
        }
        let failed_markers = tasks
            .iter()
            .filter_map(|(position, storage_id, path)| {
                let marker = path.with_extension("idx.failed");
                marker.exists().then_some((*position, *storage_id, marker))
            })
            .collect::<Vec<_>>();
        let workers = thread::available_parallelism()
            .map(NonZeroUsize::get)
            .unwrap_or(2)
            .min(8)
            .max(1);
        let mut needs_rebuild = vec![BTreeSet::<u64>::new(); tables.len()];
        if tasks.len() <= 1 || workers == 1 {
            for (table_idx, storage_id, path) in tasks {
                if needs_rebuild[table_idx].contains(&storage_id) {
                    continue;
                }
                if validation.validate(path).is_err() {
                    needs_rebuild[table_idx].insert(storage_id);
                }
            }
        } else {
            let next = AtomicUsize::new(0);
            let tasks_ref = &tasks;
            let validation_copy = validation;
            let results = thread::scope(|scope| {
                let mut handles = Vec::with_capacity(workers);
                for _ in 0..workers {
                    let next = &next;
                    handles.push(scope.spawn(move || {
                        let mut local: Vec<(usize, u64)> = Vec::new();
                        loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            if i >= tasks_ref.len() {
                                break;
                            }
                            let (table_idx, storage_id, path) = &tasks_ref[i];
                            let bad = validation_copy.validate(path.clone()).is_err();
                            if bad {
                                local.push((*table_idx, *storage_id));
                            }
                        }
                        local
                    }));
                }
                let mut all = Vec::new();
                for handle in handles {
                    if let Ok(result) = handle.join() {
                        all.extend(result);
                    }
                }
                all
            });
            for (table_idx, storage_id) in results {
                needs_rebuild[table_idx].insert(storage_id);
            }
        }
        for (idx, table) in tables.iter().enumerate() {
            let physical_index_count = index_counts[idx];
            if !needs_rebuild[idx].is_empty() {
                self.ensure_writable()?;
                let storage_ids = needs_rebuild[idx].iter().copied().collect::<Vec<_>>();
                let table = self
                    .load_table_descriptor_from_dd(table.id)?
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "index repair table is missing from data dictionary",
                        )
                    })?;
                if table.is_partitioned() {
                    self.rebuild_indexes_for_partition_storage_ids(&table, &storage_ids)?;
                } else {
                    self.rebuild_indexes(&table)?;
                }
                for (position, storage_id, marker) in &failed_markers {
                    if *position == idx && storage_ids.contains(storage_id) {
                        fs::remove_file(marker).map_err(io_err)?;
                    }
                }
                let repaired = storage_ids.len().saturating_mul(table.indexes.len());
                summary.repaired_indexes += repaired;
                summary.checked_index_files += repaired;
                match validation {
                    IndexValidation::Full => {
                        summary.checked_index_files += physical_index_count.saturating_sub(repaired)
                    }
                    IndexValidation::Quick => {
                        summary.quick_checked_index_files +=
                            physical_index_count.saturating_sub(repaired)
                    }
                }
            } else {
                match validation {
                    IndexValidation::Full => summary.checked_index_files += physical_index_count,
                    IndexValidation::Quick => {
                        summary.quick_checked_index_files += physical_index_count
                    }
                }
            }
        }
        Ok(summary)
    }

    fn skipped_heap_files_fast(&self) -> HeapValidationSummary {
        let mut summary = HeapValidationSummary::default();
        let root = self.data_dir.join("objects/tables");
        let Ok(entries) = std::fs::read_dir(root) else {
            return summary;
        };
        for table_dir in entries.filter_map(Result::ok) {
            let Ok(files) = std::fs::read_dir(table_dir.path()) else {
                continue;
            };
            for file in files.filter_map(Result::ok) {
                let path = file.path();
                if !path.extension().is_some_and(|ext| ext == "heap") {
                    continue;
                }
                summary.skipped_files += 1;
                summary.skipped_pages += file
                    .metadata()
                    .map(|meta| meta.len().div_ceil(PAGE_SIZE as u64) as usize)
                    .unwrap_or(0);
            }
        }
        summary
    }

    fn validate_heap_files(&self) -> Result<HeapValidationSummary, QueryError> {
        if self.config.durability == DurabilityMode::Memory {
            return Ok(HeapValidationSummary::default());
        }
        let max_lsn = self.log.lock().next_lsn().saturating_sub(1);
        let mut tasks = Vec::new();
        for table in self.user_table_snapshots()? {
            for storage_id in table.storage_ids() {
                tasks.push((self.table_path(storage_id), storage_id));
            }
        }
        let workers = thread::available_parallelism()
            .map_or(1, NonZeroUsize::get)
            .min(8)
            .min(tasks.len())
            .max(1);
        let next = AtomicUsize::new(0);
        let counts = thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..workers {
                let tasks = &tasks;
                let next = &next;
                handles.push(scope.spawn(move || {
                    let mut pages = 0;
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some((path, storage_id)) = tasks.get(index) else {
                            break;
                        };
                        pages += HeapFile::validate(path.clone(), *storage_id, max_lsn)?;
                    }
                    Ok::<_, QueryError>(pages)
                }));
            }
            let mut pages = 0;
            for handle in handles {
                pages += handle.join().map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "heap validation worker panicked",
                    )
                })??;
            }
            Ok::<_, QueryError>(pages)
        })?;
        let summary = HeapValidationSummary {
            checked_files: tasks.len(),
            checked_pages: counts,
            ..Default::default()
        };
        Ok(summary)
    }

    fn index_file_count(&self) -> Result<usize, QueryError> {
        Ok(self
            .user_table_snapshots()?
            .iter()
            .map(|table| {
                table
                    .storage_ids()
                    .len()
                    .saturating_mul(table.indexes.len())
            })
            .sum())
    }

    fn index_file_count_fast(&self) -> usize {
        let mut count = 0usize;
        let mut pending = vec![self.data_dir.join("objects/indexes")];
        while let Some(path) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(path) else {
                continue;
            };
            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().is_some_and(|ext| ext == "idx") {
                    count = count.saturating_add(1);
                }
            }
        }
        count
    }

    pub fn check_consistency(&self) -> Result<(), QueryError> {
        let txn = TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted);
        for table in self.user_table_snapshots()? {
            let rows = self.scan_rows(&table.qualified_name(), txn)?;
            let mut live_by_row_id = BTreeMap::new();
            for row in rows {
                live_by_row_id.insert(row.row_id, row.values);
            }
            for index in &table.indexes {
                let mut expected = BTreeMap::<IndexKey, Vec<RowId>>::new();
                for (row_id, values) in &live_by_row_id {
                    for key in table.index_keys(index, values)? {
                        expected.entry(key).or_default().push(*row_id);
                    }
                }
                for (key, mut expected_rows) in expected {
                    expected_rows.sort();
                    let mut actual_rows = self.lookup_index(&table, index, key.clone())?;
                    actual_rows.sort();
                    if actual_rows != expected_rows {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            format!(
                                "storage consistency check failed for index '{}': key {:?}",
                                index.name, key
                            ),
                        ));
                    }
                    for row_id in actual_rows {
                        let Some(row) = self.read_row(&table, row_id, txn)? else {
                            return Err(error(
                                crate::error::catalog::ER_GET_ERRNO,
                                format!(
                                    "storage consistency check failed for index '{}': dangling row",
                                    index.name
                                ),
                            ));
                        };
                        if !table.index_keys(index, &row.values)?.contains(&key) {
                            return Err(error(
                                crate::error::catalog::ER_GET_ERRNO,
                                format!(
                                    "storage consistency check failed for index '{}': wrong key",
                                    index.name
                                ),
                            ));
                        }
                    }
                }
                let entries =
                    self.with_index_mut(table.id, &index.name, |index_file| index_file.entries())?;
                for (key, row_id) in entries {
                    let Some(values) = live_by_row_id.get(&row_id) else {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            format!(
                                "storage consistency check failed for index '{}': dangling row",
                                index.name
                            ),
                        ));
                    };
                    if !table.index_keys(index, values)?.contains(&key) {
                        return Err(error(
                            crate::error::catalog::ER_GET_ERRNO,
                            format!(
                                "storage consistency check failed for index '{}': wrong key",
                                index.name
                            ),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn prepare_snapshot_indexes(&self) -> Result<(), QueryError> {
        for table in self.user_table_snapshots()? {
            self.rebuild_indexes(&table)?;
            self.rebuild_zone_map_for_table(&table)?;
            self.drop_table_vector_graphs(&table);
            for index in &table.indexes {
                if index.is_vector() {
                    for storage_id in table.storage_ids() {
                        self.vector_graph_build_on_ddl_one(&table, index, storage_id)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn rebuild_touched_indexes(&self, table_ids: &HashSet<u64>) -> Result<(), QueryError> {
        if table_ids.is_empty() {
            return Ok(());
        }
        for table in self.user_table_snapshots()? {
            if table_ids.contains(&table.id) {
                self.rebuild_indexes(&table)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum IndexValidation {
    Full,
    Quick,
}

impl IndexValidation {
    fn validate(self, path: PathBuf) -> Result<(), QueryError> {
        match self {
            Self::Full => IndexFile::validate(path),
            Self::Quick => IndexFile::quick_validate(path),
        }
    }
}

/// Check shape before REDO can extend a torn file with zeroes and make its
/// partial last page look complete. No payload reads or catalog materialization.
fn validate_heap_file_lengths(data_dir: &Path) -> Result<(), QueryError> {
    let root = data_dir.join("objects/tables");
    let directories = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(io_err(err)),
    };
    for directory in directories {
        let directory = directory.map_err(io_err)?;
        if !directory.file_type().map_err(io_err)?.is_dir() {
            continue;
        }
        for entry in fs::read_dir(directory.path()).map_err(io_err)? {
            let entry = entry.map_err(io_err)?;
            if entry.path().extension().is_some_and(|ext| ext == "heap") {
                let len = entry.metadata().map_err(io_err)?.len();
                if len % PAGE_SIZE as u64 != 0 {
                    return Err(error(crate::error::catalog::ER_GET_ERRNO,
                        format!("corrupt heap file length: '{}' has {len} bytes (page size {PAGE_SIZE}); refusing recovery of a torn page", entry.path().display())));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
static VECTOR_COMPLETION_FAILURE: AtomicBool = AtomicBool::new(false);

impl Database {
    /// A zero-length, identity-named intent survives a decision until the
    /// affected derived graphs and a checkpoint are durable. No undo payloads
    /// or vectors are copied into this recovery journal.
    fn prepare_vector_completion(
        &self,
        prepared: &crate::xa::PreparedXa,
    ) -> Result<Vec<(u64, PathBuf)>, QueryError> {
        if !prepared.recovered {
            return Ok(Vec::new());
        }
        let ids = self
            .undo_read()?
            .records
            .values()
            .filter(|record| record.txn_id == prepared.txn_id)
            .map(|record| record.table_id)
            .collect::<HashSet<_>>();
        let tables = ids
            .into_iter()
            .filter_map(|id| self.table_by_id(id))
            .filter(|table| table.indexes.iter().any(|index| index.is_vector()))
            .map(|table| table.id)
            .collect::<std::collections::BTreeSet<_>>();
        if tables.is_empty() {
            return Ok(Vec::new());
        }
        let dir = self.data_dir.join("system/vector-completion");
        fs::create_dir_all(&dir).map_err(io_err)?;
        crate::platform::fs::sync_path(dir.parent().unwrap()).map_err(io_err)?;
        let mut intents = Vec::new();
        for id in tables {
            let path = dir.join(format!("{}-{id}.pending", prepared.txn_id));
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .or_else(|err| {
                    if err.kind() == std::io::ErrorKind::AlreadyExists {
                        fs::OpenOptions::new().write(true).open(&path)
                    } else {
                        Err(err)
                    }
                })
                .map_err(io_err)?
                .sync_all()
                .map_err(io_err)?;
            intents.push((id, path));
        }
        crate::platform::fs::sync_path(&dir).map_err(io_err)?;
        Ok(intents)
    }

    fn finish_vector_completion(&self, intents: &[(u64, PathBuf)]) -> Result<(), QueryError> {
        if intents.is_empty() {
            return Ok(());
        }
        #[cfg(test)]
        if VECTOR_COMPLETION_FAILURE.swap(false, Ordering::SeqCst) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "injected XA vector completion interruption",
            ));
        }
        let ids = intents.iter().map(|(id, _)| *id).collect::<HashSet<_>>();
        for id in ids {
            let Some(table) = self.table_by_id(id) else {
                continue;
            };
            self.evict_table_vector_graphs(table.id);
            for index in table.indexes.iter().filter(|index| index.is_vector()) {
                for sid in table.storage_ids() {
                    for ext in ["hnsw", "hvec"] {
                        let path = self.vector_index_paged_path_ext(&table, &index.name, sid, ext);
                        self.buffer_pool.discard_pages_for_path(&path)?;
                        match fs::remove_file(&path) {
                            Ok(()) => {}
                            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                            Err(err) => return Err(io_err(err)),
                        }
                    }
                    self.vector_graph_build_on_ddl_one(&table, index, sid)?;
                    let key = (
                        table.id,
                        sid,
                        table.active_generation.max(1),
                        normalize_name(&index.name),
                    );
                    let graph = self
                        .vector_indexes
                        .read()
                        .get(&key)
                        .cloned()
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "rebuilt vector graph is missing",
                            )
                        })?;
                    graph.flush_paged_ivf()?;
                    self.vector_graph_persist_one(
                        &table,
                        &index.name,
                        sid,
                        &graph,
                        self.log.lock().next_lsn().saturating_sub(1),
                    )
                    .map_err(io_err)?;
                }
            }
        }
        self.checkpoint()?;
        // Checkpoint persists paged graphs; sync directory entries as well
        // before removing the durable intent, including slab snapshot renames.
        for (id, _) in intents {
            if let Some(table) = self.table_by_id(*id) {
                for sid in table.storage_ids() {
                    for index in table.indexes.iter().filter(|index| index.is_vector()) {
                        let path =
                            self.vector_index_paged_path_ext(&table, &index.name, sid, "hvec");
                        if let Some(parent) = path.parent() {
                            crate::platform::fs::sync_path(parent).map_err(io_err)?;
                        }
                    }
                }
            }
        }
        for (_, path) in intents {
            fs::remove_file(path).map_err(io_err)?;
        }
        crate::platform::fs::sync_path(&self.data_dir.join("system/vector-completion"))
            .map_err(io_err)
    }

    fn recover_vector_completion_intents(&self) -> Result<(), QueryError> {
        let dir = self.data_dir.join("system/vector-completion");
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(io_err(err)),
        };
        let mut intents = Vec::new();
        for entry in entries {
            let entry = entry.map_err(io_err)?;
            let name = entry.file_name();
            let id = name
                .to_str()
                .and_then(|name| name.strip_suffix(".pending"))
                .and_then(|name| name.split_once('-'))
                .and_then(|(txn, table)| {
                    Some((txn.parse::<u64>().ok()?, table.parse::<u64>().ok()?))
                })
                .filter(|(txn, table)| *txn > 0 && *table > 0)
                .map(|(_, table)| table);
            if id.is_none()
                || !entry.file_type().map_err(io_err)?.is_file()
                || entry.metadata().map_err(io_err)?.len() != 0
            {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid XA vector completion intent",
                ));
            }
            intents.push((id.unwrap(), entry.path()));
        }
        self.finish_vector_completion(&intents)
    }

    #[cfg(test)]
    pub(crate) fn test_interrupt_vector_completion(&self) {
        VECTOR_COMPLETION_FAILURE.store(true, Ordering::SeqCst);
    }
}
