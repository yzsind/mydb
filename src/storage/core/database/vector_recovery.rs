// Durable logical completion records for vector index maintenance.
// Included into storage/core.rs.

struct VectorRecoveryMutation {
    table_id: u64,
    storage_id: u64,
    generation: u64,
    index_id: u64,
    row_id: RowId,
    value: Option<crate::vector::VectorValue>,
}

impl VectorRecoveryMutation {
    fn encode_batch(entries: &[Self]) -> Result<Vec<u8>, QueryError> {
        let mut bytes = Vec::new();
        let mut writer = vector::persistence::Writer::new(&mut bytes, 5).map_err(io_err)?;
        writer.count(entries.len()).map_err(io_err)?;
        for entry in entries {
            for id in [
                entry.table_id,
                entry.storage_id,
                entry.generation,
                entry.index_id,
            ] {
                writer.u64(id).map_err(io_err)?;
            }
            writer.row(entry.row_id).map_err(io_err)?;
            writer.u8(u8::from(entry.value.is_some())).map_err(io_err)?;
            if let Some(value) = &entry.value {
                writer.vector(value).map_err(io_err)?;
            }
        }
        writer.finish().map_err(io_err)?;
        Ok(bytes)
    }

    fn decode_batch(bytes: &[u8]) -> Result<Vec<Self>, QueryError> {
        let mut reader = vector::persistence::Reader::new(bytes, 5)?;
        let count = reader.count(41)?;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let table_id = reader.u64()?;
            let storage_id = reader.u64()?;
            let generation = reader.u64()?;
            let index_id = reader.u64()?;
            let row_id = reader.row()?;
            let value = match reader.u8()? {
                0 => None,
                1 => {
                    let value = reader.vector()?;
                    vector::validate_internal(&value)?;
                    Some(value)
                }
                _ => return Err(vector::persistence::corrupt()),
            };
            if table_id == 0 || storage_id == 0 || generation == 0 || index_id == 0 {
                return Err(vector::persistence::corrupt());
            }
            entries.push(Self {
                table_id,
                storage_id,
                generation,
                index_id,
                row_id,
                value,
            });
        }
        reader.finish()?;
        Ok(entries)
    }
}

impl Database {
    fn log_pending_vector_recovery(&self, txn_id: u64) -> Result<(), QueryError> {
        if self.durability_mode() == DurabilityMode::Memory {
            return Ok(());
        }
        if self.txns.read().status(txn_id) == Some(TxnStatus::Prepared)
            && self.vector_pending_wal.lock().contains_key(&txn_id)
        {
            return Ok(());
        }
        let pending = self
            .pending_vector_deletes
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .get(&txn_id)
            .cloned()
            .unwrap_or_default();
        let mut final_rows = BTreeMap::new();
        for mutation in pending {
            final_rows.insert(
                (mutation.table_id, mutation.storage_id, mutation.row_id),
                mutation.new_values,
            );
        }
        let mut batch = Vec::new();
        let mut size = 0usize;
        let flush = |batch: &mut Vec<VectorRecoveryMutation>| -> Result<(), QueryError> {
            if !batch.is_empty() {
                let bytes = VectorRecoveryMutation::encode_batch(batch)?;
                let floor = self.log_state_atomics.next_lsn();
                self.vector_pending_wal
                    .lock()
                    .entry(txn_id)
                    .or_insert(floor);
                self.append_wal(txn_id, WalPayload::VectorIndexBatch { bytes })?;
                batch.clear();
            }
            Ok(())
        };
        for ((table_id, storage_id, row_id), values) in final_rows {
            let table = self
                .table_by_id(table_id)
                .ok_or_else(vector::persistence::corrupt)?;
            for index in table.indexes.iter().filter(|index| index.is_vector()) {
                let col = table.column_index(&index.columns[0])?;
                let value = match values.as_ref().and_then(|values| values.get(col)) {
                    Some(Value::Vector(value)) => Some(value.clone()),
                    Some(Value::Null) | None => None,
                    _ => return Err(vector::persistence::corrupt()),
                };
                let bytes = value.as_ref().map_or(64, |value| value.len() + 64);
                if size.saturating_add(bytes) > 1024 * 1024 {
                    flush(&mut batch)?;
                    size = 0;
                }
                batch.push(VectorRecoveryMutation {
                    table_id,
                    storage_id,
                    generation: table.active_generation.max(1),
                    index_id: index.index_id,
                    row_id,
                    value,
                });
                size += bytes;
            }
        }
        flush(&mut batch)
    }

    fn finish_pending_vector_training(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
        graph: &VectorGraph,
        txn_id: u64,
        generation: u64,
    ) -> Result<(), QueryError> {
        let Some(config) = index
            .vector
            .as_ref()
            .filter(|config| config.quantization.enabled())
        else {
            return Ok(());
        };
        let lineage = match graph {
            VectorGraph::NativeHnsw(native) => native.lineage(),
            VectorGraph::NativeIvf(native) => native.lineage(),
            _ => 0,
        };
        let publish = |model: &Arc<vector::quantization::Quantizer>| -> Result<(), QueryError> {
            if self.durability_mode() == DurabilityMode::Memory {
                return Ok(());
            }
            let mut bytes = Vec::new();
            let mut writer = vector::persistence::Writer::new(&mut bytes, 6).map_err(io_err)?;
            for value in [
                txn_id,
                table.id,
                storage_id,
                table.active_generation.max(1),
                index.index_id,
                lineage,
                generation,
            ] {
                writer.u64(value).map_err(io_err)?;
            }
            writer.bytes(&model.serialize(None)?).map_err(io_err)?;
            writer.finish().map_err(io_err)?;
            self.append_wal(0, WalPayload::VectorIndexModel { bytes })?;
            Ok(())
        };
        match graph {
            VectorGraph::NativeHnsw(native) => {
                native.train_pending_model(&config.quantization, generation, publish)
            }
            VectorGraph::NativeIvf(native) => {
                native.train_pending_model(&config.quantization, generation, publish)
            }
            _ => Ok(()),
        }
    }

    fn replay_vector_model(&self, bytes: &[u8]) -> Result<(), QueryError> {
        let mut reader = vector::persistence::Reader::new(bytes, 6)?;
        let _parent_txn = reader.u64()?;
        let table_id = reader.u64()?;
        let storage_id = reader.u64()?;
        let generation = reader.u64()?;
        let index_id = reader.u64()?;
        let lineage = reader.u64()?;
        let model_generation = reader.u64()?;
        let model = reader.bytes()?;
        reader.finish()?;
        let Some(table) = self.table_by_id_checked(table_id)? else {
            return Ok(());
        };
        if table.active_generation.max(1) != generation
            || !table.storage_ids().contains(&storage_id)
        {
            return Ok(());
        }
        let Some(index) = table
            .indexes
            .iter()
            .find(|index| index.index_id == index_id)
        else {
            return Ok(());
        };
        if (self.physical_replica_mode() || self.open_mode == DatabaseOpenMode::SealBaseline)
            && vector_storage_paged()
        {
            return Ok(());
        }
        let model = vector::quantization::Quantizer::deserialize(model)?;
        let graph = self.vector_graph_get_or_build_for_partition(&table, index, storage_id)?;
        let result = match graph.as_ref() {
            VectorGraph::NativeHnsw(native) if native.lineage() == lineage => {
                native.install_pending_model(model, model_generation)
            }
            VectorGraph::NativeIvf(native) if native.lineage() == lineage => {
                native.install_pending_model(model, model_generation)
            }
            VectorGraph::NativeHnsw(_) | VectorGraph::NativeIvf(_) => Ok(()),
            _ => Err(vector::persistence::corrupt()),
        };
        result?;
        self.note_vector_snapshot_change(&graph);
        Ok(())
    }

    fn replay_persistent_vector_wal(
        &self,
        records: &(impl crate::wal::WalRecordSource + ?Sized),
    ) -> Result<(), QueryError> {
        if self.durability_mode() == DurabilityMode::Memory || self.is_snapshot_read_only() {
            return Ok(());
        }
        let mut committed = HashMap::new();
        for record in records
            .records()
            .only(&[crate::wal::WalRecordType::CommitTxn])
        {
            let record = record?;
            if let WalPayload::CommitTxn { commit_scn, .. } = record.payload {
                committed.insert(record.txn_id, (commit_scn, record.lsn));
            }
        }
        self.vector_recovery_active.store(true, Ordering::Release);
        let result = (|| -> Result<(), QueryError> {
            for record in records.records().only(&[
                crate::wal::WalRecordType::VectorIndexBatch,
                crate::wal::WalRecordType::VectorIndexModel,
            ]) {
                let record = record?;
                if let WalPayload::VectorIndexModel { bytes } = &record.payload {
                    self.replay_vector_model(bytes)?;
                    continue;
                }
                let decision = committed.get(&record.txn_id).copied();
                // A loser may have persisted its begin-delta marker before
                // the commit decision. Open and validate paged objects before
                // clearing that marker, without applying the loser's rows.
                if decision.is_none() && !vector_storage_paged() {
                    continue;
                }
                let WalPayload::VectorIndexBatch { bytes } = &record.payload else {
                    return Err(vector::persistence::corrupt());
                };
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
                    let graph = self.vector_graph_get_or_build_for_partition(
                        &table,
                        index,
                        mutation.storage_id,
                    )?;
                    let Some((scn, decision_lsn)) = decision else {
                        continue;
                    };
                    let covered = match graph.as_ref() {
                        VectorGraph::Memory(memory) => memory.last_persisted_lsn() >= decision_lsn,
                        VectorGraph::NativeHnsw(native) => native
                            .file()
                            .map_or(native.persisted_lsn() >= decision_lsn, |file| {
                                file.is_finalized() && file.meta().coverage_scn >= scn
                            }),
                        VectorGraph::NativeIvf(native) => native
                            .file()
                            .map_or(native.persisted_lsn() >= decision_lsn, |file| {
                                file.is_finalized() && file.meta().coverage_scn >= scn
                            }),
                        VectorGraph::Paged(paged) => {
                            paged.file().is_finalized() && paged.file().meta().coverage_scn >= scn
                        }
                    };
                    if covered {
                        continue;
                    }
                    if (self.physical_replica_mode()
                        || self.open_mode == DatabaseOpenMode::SealBaseline)
                        && graph.is_paged_storage()
                    {
                        continue;
                    }
                    graph.replace_row(mutation.row_id, mutation.value)?;
                    self.note_vector_snapshot_change(&graph);
                }
            }
            if self.physical_replica_mode() || self.open_mode == DatabaseOpenMode::SealBaseline {
                return Ok(());
            }
            // Retirement-only crashes may have no retained mutation batch.
            // Their cold paged files still need the controlled counter pass.
            while self.vector_purges_pending.load(Ordering::Acquire) != 0 {
                let completed = self.drain_vector_purges();
                self.ensure_operational()?;
                if completed == 0 {
                    return Err(vector::persistence::corrupt());
                }
            }
            let graphs = self.vector_indexes.read();
            for graph in graphs.values() {
                match graph.as_ref() {
                    VectorGraph::Paged(paged) => {
                        paged.file().advance_coverage(self.visible_scn());
                        paged.file().mark_delta_finalized(0)?;
                    }
                    VectorGraph::NativeHnsw(native) => {
                        if let Some(file) = native.file() {
                            file.advance_coverage(self.visible_scn());
                            file.mark_delta_finalized(0)?;
                        }
                    }
                    VectorGraph::NativeIvf(native) => {
                        if let Some(file) = native.file() {
                            file.advance_coverage(self.visible_scn());
                            file.mark_delta_finalized(0)?;
                        }
                    }
                    _ => {}
                }
            }
            Ok(())
        })();
        self.vector_recovery_active.store(false, Ordering::Release);
        result
    }
}

#[cfg(test)]
static VECTOR_CRASH_PHASE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
impl Database {
    pub(crate) fn set_vector_crash_phase_for_test(&self, phase: u64) {
        VECTOR_CRASH_PHASE.store(phase, Ordering::Release);
    }
    fn test_vector_crash_point(&self, phase: u64, lsn: u64) -> Result<(), QueryError> {
        if VECTOR_CRASH_PHASE.load(Ordering::Acquire) == phase {
            crate::wal::LogManager::sync_durable_to(&self.log, lsn)?;
            std::process::exit(86);
        }
        Ok(())
    }
}

impl Database {
    fn restore_prepared_vector_mutations(
        &self,
        records: &(impl crate::wal::WalRecordSource + ?Sized),
    ) -> Result<(), QueryError> {
        if self.physical_replica_mode() {
            return Ok(());
        }
        let mut rows = BTreeMap::new();
        for record in records
            .records()
            .only(&[crate::wal::WalRecordType::VectorIndexBatch])
        {
            let record = record?;
            if self.txns.read().status(record.txn_id) != Some(TxnStatus::Prepared) {
                continue;
            }
            let WalPayload::VectorIndexBatch { bytes } = &record.payload else {
                return Err(vector::persistence::corrupt());
            };
            self.vector_pending_wal
                .lock()
                .entry(record.txn_id)
                .or_insert(record.lsn);
            for mutation in VectorRecoveryMutation::decode_batch(bytes)? {
                let table = self
                    .table_by_id_checked(mutation.table_id)?
                    .ok_or_else(vector::persistence::corrupt)?;
                if table.active_generation.max(1) != mutation.generation {
                    return Err(vector::persistence::corrupt());
                }
                let index = table
                    .indexes
                    .iter()
                    .find(|index| index.index_id == mutation.index_id)
                    .ok_or_else(vector::persistence::corrupt)?;
                let values = rows
                    .entry((
                        record.txn_id,
                        table.id,
                        mutation.storage_id,
                        mutation.row_id,
                    ))
                    .or_insert_with(|| vec![Value::Null; table.columns.len()]);
                values[table.column_index(&index.columns[0])?] =
                    mutation.value.map_or(Value::Null, Value::Vector);
            }
        }
        let mut pending = self
            .pending_vector_deletes
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        for ((txn_id, table_id, storage_id, row_id), values) in rows {
            pending
                .entry(txn_id)
                .or_default()
                .push(PendingVectorMutation {
                    undo_id: 0,
                    table_id,
                    storage_id,
                    row_id,
                    new_values: Some(values),
                    old_values: None,
                });
        }
        Ok(())
    }
}

impl Database {
    /// COPY tails use the final logical owner and the new table generation.
    /// Their parent DDL decision makes the prepared immutable image current.
    fn log_private_copy_vector_mutation(
        &self,
        target: &TableSchema,
        txn_id: u64,
        storage_id: u64,
        row_id: RowId,
        values: Option<&[Value]>,
    ) -> Result<(), QueryError> {
        if self.durability_mode() == DurabilityMode::Memory
            || !target.indexes.iter().any(|index| index.is_vector())
        {
            return Ok(());
        }
        let published = self
            .table_by_key(&table_key(&target.database, &target.name))
            .ok_or_else(vector::persistence::corrupt)?;
        let final_storage = if !target.is_partitioned() && published.id != target.id {
            published.id
        } else {
            storage_id
        };
        let entries = target
            .indexes
            .iter()
            .filter(|index| index.is_vector())
            .map(|index| {
                let col = target.column_index(&index.columns[0])?;
                let value = match values.and_then(|values| values.get(col)) {
                    Some(Value::Vector(value)) => Some(value.clone()),
                    _ => None,
                };
                Ok(VectorRecoveryMutation {
                    table_id: published.id,
                    storage_id: final_storage,
                    generation: target.active_generation.max(1),
                    index_id: index.index_id,
                    row_id,
                    value,
                })
            })
            .collect::<Result<Vec<_>, QueryError>>()?;
        let bytes = VectorRecoveryMutation::encode_batch(&entries)?;
        if published.id != target.id {
            self.vector_copy_publications.lock().insert(txn_id);
        }
        self.vector_pending_wal
            .lock()
            .entry(txn_id)
            .or_insert(self.log_state_atomics.next_lsn());
        self.append_wal(txn_id, WalPayload::VectorIndexBatch { bytes })?;
        Ok(())
    }
}

#[cfg(test)]
impl Database {
    pub(crate) fn inject_vector_persist_failure_for_test(&self, stage: u64) {
        self.vector_persist_failure.store(stage, Ordering::Release);
    }
    fn vector_persist_failpoint(&self, stage: u64) -> Result<(), QueryError> {
        if self
            .vector_persist_failure
            .compare_exchange(stage, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("injected vector persistence failure stage {stage}"),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
impl Database {
    pub(crate) fn install_vector_copy_commit_test_hook(&self) -> Arc<DdlBackfillTestHook> {
        let hook = Arc::new(DdlBackfillTestHook::new());
        *self.vector_copy_committed_test_hook.lock().unwrap() = Some(hook.clone());
        hook
    }
}

#[cfg(test)]
impl Database {
    pub(crate) fn install_vector_apply_test_hook(&self) -> Arc<DdlBackfillTestHook> {
        let hook = Arc::new(DdlBackfillTestHook::new());
        *self.vector_apply_test_hook.lock().unwrap() = Some(hook.clone());
        hook
    }
}

impl Database {
    fn vector_pending_publication_floor(&self) -> Option<u64> {
        let floors = self.vector_pending_wal.lock();
        let publishing = self.vector_commits_publishing.lock();
        floors
            .iter()
            .filter(|(txn, _)| publishing.contains(txn))
            .map(|(_, floor)| *floor)
            .min()
    }
}

#[cfg(test)]
impl Database {
    pub(crate) fn pending_vector_retirement_versions_for_test(&self) -> usize {
        self.vector_purge_queue
            .lock()
            .values()
            .map(|purge| {
                purge
                    .rows
                    .iter()
                    .map(|(_, versions)| versions.len())
                    .sum::<usize>()
            })
            .sum()
    }
}
