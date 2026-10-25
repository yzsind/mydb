// A committed source transaction keeps its real MVCC decision while vector
// searches wait for the source's explicit maintenance completion boundary.
#[derive(Debug, Default)]
struct ReplicaVectorState {
    transactions: BTreeMap<u64, ReplicaVectorTxn>,
    readers: HashMap<u64, usize>,
    physical_writes: BTreeSet<u64>,
    images: BTreeMap<String, u64>,
}
#[derive(Debug, Default)]
struct ReplicaVectorTxn {
    records: Vec<WalRecord>,
    tables: BTreeSet<u64>,
    completing: bool,
    charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}
struct VectorReadLease<'a> {
    db: &'a Database,
    table: u64,
}
impl Drop for VectorReadLease<'_> {
    fn drop(&mut self) {
        let mut state = self.db.replica_vector_state.lock();
        if let Some(count) = state.readers.get_mut(&self.table) {
            *count -= 1;
            if *count == 0 {
                state.readers.remove(&self.table);
            }
        }
        self.db.replica_vector_signal.notify_all();
    }
}
impl Database {
    fn acquire_vector_read(&self, table: u64) -> Result<Option<VectorReadLease<'_>>, QueryError> {
        let mut state = self.replica_vector_state.lock();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while state.physical_writes.contains(&table)
            || state
                .transactions
                .values()
                .any(|txn| txn.completing && txn.tables.contains(&table))
        {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero()
                || self
                    .replica_vector_signal
                    .wait_for(&mut state, remaining)
                    .timed_out()
            {
                return Err(error(
                    crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                    "vector index is waiting for its durable source maintenance completion",
                ));
            }
        }
        *state.readers.entry(table).or_default() += 1;
        Ok(Some(VectorReadLease { db: self, table }))
    }

    fn note_replica_vector_record(&self, record: &WalRecord) -> Result<(), QueryError> {
        let (parent, tables, bytes) = match &record.payload {
            WalPayload::VectorIndexBatch { bytes } => (
                record.txn_id,
                VectorRecoveryMutation::decode_batch(bytes)?
                    .into_iter()
                    .map(|entry| entry.table_id)
                    .collect::<BTreeSet<_>>(),
                bytes.len(),
            ),
            WalPayload::VectorIndexModel { bytes } => {
                let mut reader = vector::persistence::Reader::new(bytes, 6)?;
                (reader.u64()?, BTreeSet::from([reader.u64()?]), bytes.len())
            }
            _ => return Err(vector::persistence::corrupt()),
        };
        if parent == 0 {
            return Err(vector::persistence::corrupt());
        }
        let mut state = self.replica_vector_state.lock();
        let txn = state.transactions.entry(parent).or_default();
        let bytes = bytes
            .checked_add(256)
            .ok_or_else(vector::persistence::corrupt)?;
        if let Some(charge) = txn.charge.as_mut() {
            charge.try_grow_recoverable(bytes)?;
        } else {
            txn.charge = Some(
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_recoverable(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    bytes,
                )?,
            );
        }
        txn.tables.extend(tables);
        txn.records.push(record.clone());
        self.vector_pending_wal
            .lock()
            .entry(parent)
            .or_insert(record.lsn);
        Ok(())
    }

    fn begin_replica_vector_commit(&self, txn_id: u64) -> Result<(), QueryError> {
        let mut state = self.replica_vector_state.lock();
        let Some(txn) = state.transactions.get_mut(&txn_id) else {
            return Ok(());
        };
        txn.completing = true;
        let tables = txn.tables.clone();
        while tables
            .iter()
            .any(|table| state.readers.get(table).is_some_and(|count| *count != 0))
        {
            self.replica_vector_signal.wait(&mut state);
        }
        Ok(())
    }

    fn complete_replica_vector_transaction(&self, txn_id: u64, scn: u64) -> Result<(), QueryError> {
        // Move records out while preserving the read fence on every error.
        let records = {
            let mut state = self.replica_vector_state.lock();
            let Some(txn) = state.transactions.get_mut(&txn_id) else {
                return Ok(());
            };
            if !txn.completing {
                return Err(vector::persistence::corrupt());
            }
            std::mem::take(&mut txn.records)
        };
        let result = (|| -> Result<(), QueryError> {
            // Paged directories reflect the source's completed physical pages.
            // Resident structures apply the same durable logical changes.
            let mut reloaded = HashSet::new();
            for record in &records {
                match &record.payload {
                    WalPayload::VectorIndexBatch { bytes } => {
                        for mutation in VectorRecoveryMutation::decode_batch(bytes)? {
                            let Some(table) = self.table_by_id_checked(mutation.table_id)? else {
                                continue;
                            };
                            if table.active_generation.max(1) != mutation.generation
                                || !table.storage_ids().contains(&mutation.storage_id)
                            {
                                continue;
                            }
                            let Some(index) = table
                                .indexes
                                .iter()
                                .find(|index| index.index_id == mutation.index_id)
                            else {
                                continue;
                            };
                            let key = (
                                table.id,
                                mutation.storage_id,
                                mutation.generation,
                                normalize_name(&index.name),
                            );
                            if vector_storage_paged() && reloaded.insert(key.clone()) {
                                self.vector_indexes.write().remove(&key);
                            }
                            let graph = self.vector_graph_get_or_build_for_partition(
                                &table,
                                index,
                                mutation.storage_id,
                            )?;
                            if !graph.is_paged_storage() {
                                graph.replace_row(mutation.row_id, mutation.value)?;
                                self.note_vector_snapshot_change(&graph);
                            }
                        }
                    }
                    WalPayload::VectorIndexModel { bytes } if !vector_storage_paged() => {
                        self.replay_vector_model(bytes)?
                    }
                    WalPayload::VectorIndexModel { .. } => {}
                    _ => return Err(vector::persistence::corrupt()),
                }
            }
            self.vector_snapshot_floor.fetch_max(scn, Ordering::AcqRel);
            Ok(())
        })();
        if let Err(err) = result {
            if let Some(txn) = self
                .replica_vector_state
                .lock()
                .transactions
                .get_mut(&txn_id)
            {
                txn.records = records;
            }
            return Err(err);
        }
        self.replica_vector_state
            .lock()
            .transactions
            .remove(&txn_id);
        self.vector_pending_wal.lock().remove(&txn_id);
        self.replica_vector_signal.notify_all();
        Ok(())
    }
}

impl Database {
    fn restore_replica_vector_completion(
        &self,
        records: &(impl crate::wal::WalRecordSource + ?Sized),
    ) -> Result<(), QueryError> {
        if !self.physical_replica_mode() {
            return Ok(());
        }
        for record in records.records().only(&[
            crate::wal::WalRecordType::VectorIndexBatch,
            crate::wal::WalRecordType::VectorIndexModel,
            crate::wal::WalRecordType::CommitTxn,
            crate::wal::WalRecordType::VectorIndexComplete,
            crate::wal::WalRecordType::VectorIndexImage,
            crate::wal::WalRecordType::AbortTxn,
        ]) {
            let record = record?;
            match &record.payload {
                WalPayload::VectorIndexImage {
                    path,
                    offset,
                    total,
                    bytes,
                } => {
                    if *offset == 0 {
                        self.replica_vector_state
                            .lock()
                            .images
                            .insert(path.clone(), record.lsn);
                    }
                    if offset.checked_add(bytes.len() as u64) == Some(*total) {
                        self.replica_vector_state.lock().images.remove(path);
                    }
                }
                WalPayload::VectorIndexBatch { .. } | WalPayload::VectorIndexModel { .. } => {
                    self.note_replica_vector_record(&record)?
                }
                WalPayload::CommitTxn { .. } => self.begin_replica_vector_commit(record.txn_id)?,
                WalPayload::VectorIndexComplete { txn_id, .. } => {
                    self.replica_vector_state.lock().transactions.remove(txn_id);
                    self.vector_pending_wal.lock().remove(txn_id);
                }
                WalPayload::AbortTxn => {
                    self.replica_vector_state
                        .lock()
                        .transactions
                        .remove(&record.txn_id);
                    self.vector_pending_wal.lock().remove(&record.txn_id);
                }
                _ => {}
            }
        }
        Ok(())
    }
}

struct VectorPhysicalRedoLease<'a> {
    db: &'a Database,
    tables: BTreeSet<u64>,
}
impl Drop for VectorPhysicalRedoLease<'_> {
    fn drop(&mut self) {
        let mut state = self.db.replica_vector_state.lock();
        for table in &self.tables {
            state.physical_writes.remove(table);
        }
        self.db.replica_vector_signal.notify_all();
    }
}
impl Database {
    fn begin_vector_physical_redo(
        &self,
        relative: &str,
    ) -> Result<Option<VectorPhysicalRedoLease<'_>>, QueryError> {
        let path = self.data_dir.join(relative);
        let mut keys = Vec::new();
        let mut tables = BTreeSet::new();
        {
            let graphs = self.vector_indexes.read();
            for (key, graph) in graphs.iter() {
                let file = match graph.as_ref() {
                    VectorGraph::Paged(g) => Some(g.file()),
                    VectorGraph::NativeHnsw(g) => g.file(),
                    VectorGraph::NativeIvf(g) => g.file(),
                    _ => None,
                };
                if file.is_some_and(|file| file.persistent_path() == path) {
                    tables.insert(key.0);
                    keys.push(key.clone());
                }
            }
        }
        if let Some((_, info)) = crate::storage::naming::parse_relative_object_path(relative) {
            if let Some(table) = self.table_by_id_checked(info.table_id)? {
                tables.insert(table.id);
                if info.ext == "vmanifest" {
                    if let Some(index) = table
                        .indexes
                        .iter()
                        .find(|index| Some(index.index_id) == info.part.parse::<u64>().ok())
                    {
                        keys.push((
                            table.id,
                            info.table_id,
                            table.active_generation.max(1),
                            normalize_name(&index.name),
                        ));
                    }
                }
            }
        }
        if tables.is_empty() {
            return Ok(None);
        }
        let mut state = self.replica_vector_state.lock();
        state.physical_writes.extend(tables.iter().copied());
        while tables
            .iter()
            .any(|table| state.readers.get(table).is_some_and(|count| *count != 0))
        {
            self.replica_vector_signal.wait(&mut state);
        }
        drop(state);
        let lease = VectorPhysicalRedoLease { db: self, tables };
        self.buffer_pool.discard_pages_for_path(&path)?;
        let mut graphs = self.vector_indexes.write();
        for key in keys {
            graphs.remove(&key);
        }
        Ok(Some(lease))
    }
}

impl Database {
    fn finish_promoted_vector_journal(&self) -> Result<(), QueryError> {
        let scan = crate::wal::LogManager::scan(&self.data_dir)?;
        self.replay_persistent_vector_wal(&scan.records)?;
        self.restore_prepared_vector_mutations(&scan.records)?;
        let mut state = self.replica_vector_state.lock();
        let completed = state
            .transactions
            .keys()
            .copied()
            .filter(|id| self.txns.read().status(*id) != Some(TxnStatus::Prepared))
            .collect::<Vec<_>>();
        for txn_id in completed {
            let commit_scn = self
                .txns
                .read()
                .slot(txn_id)
                .filter(|slot| slot.status == TxnStatus::Committed)
                .and_then(|slot| slot.commit_scn);
            if let Some(commit_scn) = commit_scn {
                self.append_wal(0, WalPayload::VectorIndexComplete { txn_id, commit_scn })?;
            }
            state.transactions.remove(&txn_id);
            self.vector_pending_wal.lock().remove(&txn_id);
        }
        drop(state);
        self.finish_promoted_vector_images()?;
        self.resume_failed_vector_jobs()?;
        self.replica_vector_signal.notify_all();
        Ok(())
    }
}

impl Database {
    fn finish_promoted_vector_images(&self) -> Result<(), QueryError> {
        let images = self
            .replica_vector_state
            .lock()
            .images
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        if images.is_empty() {
            return Ok(());
        }
        let mut active = HashSet::new();
        for table in self.user_table_snapshots()? {
            for index in table
                .indexes
                .iter()
                .filter(|index| index.is_vector() && index.state == IndexState::Public)
            {
                for sid in table.storage_ids() {
                    let path = if vector_storage_paged() {
                        self.active_native_vector_path(&table, index, sid)?.0
                    } else {
                        self.active_vector_snapshot_path(&table, index, sid)?.0
                    };
                    active.insert(path);
                }
            }
        }
        let mut retiring = Vec::new();
        for relative in &images {
            let path = self.data_dir.join(relative);
            if active.contains(&path) {
                return Err(error(
                    crate::error::catalog::ER_INDEX_CORRUPT,
                    "promotion blocked: an active vector structure has an incomplete source image",
                ));
            }
            let incoming = path.with_extension(format!(
                "{}.incoming",
                path.extension().unwrap().to_string_lossy()
            ));
            retiring.extend([path, incoming]);
        }
        let mut last = 0;
        for path in &retiring {
            last = self.append_wal(
                0,
                WalPayload::FileDelete {
                    path: self.relative_path(path),
                    before: Vec::new(),
                },
            )?;
        }
        crate::wal::LogManager::sync_durable_to(&self.log, last)?;
        for path in retiring {
            self.buffer_pool.discard_pages_for_path(&path)?;
            match fs::remove_file(&path) {
                Ok(()) => crate::platform::fs::sync_path(path.parent().unwrap()).map_err(io_err)?,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(io_err(err)),
            }
        }
        self.replica_vector_state.lock().images.clear();
        Ok(())
    }
}
