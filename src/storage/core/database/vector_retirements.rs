// Checkpoint-owned retirement obligations are part of the persistent index state.

impl Database {
    fn persist_vector_purge_ledger(&self) -> Result<(), QueryError> {
        if self.durability_mode() == DurabilityMode::Memory || self.is_snapshot_read_only() {
            return Ok(());
        }
        let path = self.data_dir.join("system/vector-purges.v1");
        // The mandatory empty root is already durable after bootstrap. Verify
        // its complete small record instead of replacing it on every clean CP.
        if self.vector_purge_queue.lock().is_empty() && path.try_exists().map_err(io_err)? {
            let file = fs::File::open(&path).map_err(io_err)?;
            if file.metadata().map_err(io_err)?.len()
                <= vector::persistence::EMPTY_COUNT_FILE_LEN as u64
            {
                let mut bytes = Vec::new();
                file.take(vector::persistence::EMPTY_COUNT_FILE_LEN as u64 + 1)
                    .read_to_end(&mut bytes)
                    .map_err(io_err)?;
                let validated = (|| -> Result<(), QueryError> {
                    let mut reader = vector::persistence::Reader::new(&bytes, 9)?;
                    if reader.count(52)? != 0 {
                        return Err(vector::persistence::corrupt());
                    }
                    reader.finish()
                })();
                if let Err(err) = validated {
                    self.quarantine_mvcc(format!(
                        "persistent vector retirement root is corrupt: {err:?}"
                    ));
                    return Err(err);
                }
                return Ok(());
            }
        }
        let tmp = path.with_extension("v1.tmp");
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)
            .map_err(io_err)?;
        let mut out = std::io::BufWriter::new(file);
        {
            let queue = self.vector_purge_queue.lock();
            let mut writer = vector::persistence::Writer::new(&mut out, 9).map_err(io_err)?;
            writer.count(queue.len()).map_err(io_err)?;
            for purge in queue.values() {
                for id in [
                    purge.key.0,
                    purge.key.1,
                    purge.key.2,
                    purge.index_id,
                    purge.lineage,
                    purge.commit_scn,
                ] {
                    writer.u64(id).map_err(io_err)?;
                }
                writer.count(purge.rows.len()).map_err(io_err)?;
                for (row, versions) in &purge.rows {
                    writer.row(*row).map_err(io_err)?;
                    writer.count(versions.len()).map_err(io_err)?;
                    for seq in versions {
                        writer.u64(*seq).map_err(io_err)?;
                    }
                }
            }
            writer.finish().map_err(io_err)?;
        }
        out.flush().map_err(io_err)?;
        let file = out.into_inner().map_err(|err| io_err(err.into_error()))?;
        crate::platform::fs::sync_standard_file_at(&file, &tmp).map_err(io_err)?;
        rename_owned_file(&tmp, &path).map_err(io_err)?;
        crate::platform::fs::sync_path(path.parent().expect("system directory")).map_err(io_err)
    }

    fn load_vector_purge_ledger(&self) -> Result<(), QueryError> {
        if self.durability_mode() == DurabilityMode::Memory {
            return Ok(());
        }
        let path = self.data_dir.join("system/vector-purges.v1");
        let length = usize::try_from(fs::metadata(&path).map_err(io_err)?.len())
            .map_err(|_| vector::persistence::corrupt())?;
        let _input =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_recoverable(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                length,
            )?;
        let bytes = fs::read(path).map_err(io_err)?;
        let mut reader = vector::persistence::Reader::new(&bytes, 9)?;
        let count = reader.count(52)?;
        let mut charge =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_recoverable(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                0,
            )?;
        let mut queue = VectorPurgeQueue::new();
        for _ in 0..count {
            let table_id = reader.u64()?;
            let storage_id = reader.u64()?;
            let generation = reader.u64()?;
            let index_id = reader.u64()?;
            let lineage = reader.u64()?;
            let commit_scn = reader.u64()?;
            let rows_count = reader.count(12)?;
            charge.try_grow_recoverable(rows_count.saturating_mul(192).saturating_add(512))?;
            let mut rows = Vec::new();
            rows.try_reserve_exact(rows_count).map_err(|_| {
                error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    "vector retirement allocation failed",
                )
            })?;
            for _ in 0..rows_count {
                let row = reader.row()?;
                let count = reader.count(8)?;
                charge.try_grow_recoverable(count.saturating_mul(16))?;
                let mut versions = smallvec::SmallVec::new();
                versions.try_reserve_exact(count).map_err(|_| {
                    error(
                        crate::error::catalog::ER_OUTOFMEMORY,
                        "vector retirement versions allocation failed",
                    )
                })?;
                for _ in 0..count {
                    versions.push(reader.u64()?);
                }
                rows.push((row, versions));
            }
            let Some(table) = self.table_by_id_checked(table_id)? else {
                continue;
            };
            if table.active_generation.max(1) != generation
                || !table.storage_ids().contains(&storage_id)
            {
                continue;
            }
            let Some(index) = table
                .indexes
                .iter()
                .find(|index| index.index_id == index_id)
            else {
                continue;
            };
            let purge = VectorPurge {
                key: (
                    table_id,
                    storage_id,
                    generation,
                    normalize_name(&index.name),
                ),
                index_id,
                lineage,
                rows,
                commit_scn,
            };
            if queue.insert(purge).is_some() {
                return Err(vector::persistence::corrupt());
            }
        }
        reader.finish()?;
        let count = queue.len();
        *self.vector_purge_queue.lock() = queue;
        *self.vector_purge_recovery_charge.lock() = Some(charge);
        self.vector_purges_pending.store(count, Ordering::Release);
        Ok(())
    }
    fn apply_loaded_vector_retirements(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
        graph: &VectorGraph,
    ) -> Result<(), QueryError> {
        if !self.physical_replica_mode()
            && !self.is_snapshot_read_only()
            && self.open_mode != DatabaseOpenMode::SealBaseline
        {
            return Ok(());
        }
        let queue = self.vector_purge_queue.lock();
        for purge in queue
            .for_index((
                table.id,
                storage_id,
                table.active_generation.max(1),
                index.index_id,
                graph.identity_lineage(),
            ))
            .filter(|purge| purge.commit_scn <= self.visible_scn())
        {
            for (row, versions) in &purge.rows {
                match graph {
                    VectorGraph::Memory(memory) => {
                        memory.retire_versions(*row, versions);
                    }
                    VectorGraph::Paged(paged) => paged.install_retirement_mask(*row, versions)?,
                    _ => {}
                }
            }
        }
        Ok(())
    }
}

struct IndexRetirementSnapshot {
    bytes: Vec<u8>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl Database {
    fn index_retirement_snapshot(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
        lineage: u64,
    ) -> Result<IndexRetirementSnapshot, QueryError> {
        let queue = self.vector_purge_queue.lock();
        let identity = (
            table.id,
            storage_id,
            table.active_generation.max(1),
            index.index_id,
            lineage,
        );
        let size = queue.for_index(identity).fold(25usize, |size, purge| {
            size.saturating_add(12).saturating_add(
                purge
                    .rows
                    .iter()
                    .map(|(_, versions)| 12 + versions.len() * 8)
                    .sum::<usize>(),
            )
        });
        let charge =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_recoverable(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                size,
            )?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(size).map_err(|_| {
            error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "vector retirement snapshot allocation failed",
            )
        })?;
        let mut writer = vector::persistence::Writer::new(&mut bytes, 10).map_err(io_err)?;
        writer.u64(lineage).map_err(io_err)?;
        writer
            .count(queue.for_index(identity).count())
            .map_err(io_err)?;
        for purge in queue.for_index(identity) {
            writer.u64(purge.commit_scn).map_err(io_err)?;
            writer.count(purge.rows.len()).map_err(io_err)?;
            for (row, versions) in &purge.rows {
                writer.row(*row).map_err(io_err)?;
                writer.count(versions.len()).map_err(io_err)?;
                for seq in versions {
                    writer.u64(*seq).map_err(io_err)?;
                }
            }
        }
        writer.finish().map_err(io_err)?;
        Ok(IndexRetirementSnapshot {
            bytes,
            _charge: charge,
        })
    }

    fn merge_index_retirement_snapshot(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
        lineage: u64,
        bytes: &[u8],
    ) -> Result<(), QueryError> {
        let mut reader = vector::persistence::Reader::new(bytes, 10)?;
        if reader.u64()? != lineage {
            return Err(vector::persistence::corrupt());
        }
        let count = reader.count(12)?;
        let mut charge =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_recoverable(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                0,
            )?;
        let mut decoded = Vec::new();
        let mut maximum_scn = 0;
        for _ in 0..count {
            let commit_scn = reader.u64()?;
            let rows_count = reader.count(12)?;
            if commit_scn == 0 {
                return Err(vector::persistence::corrupt());
            }
            maximum_scn = maximum_scn.max(commit_scn);
            let mut estimate = 512 + rows_count.saturating_mul(192);
            charge.try_grow_recoverable(estimate)?;
            let mut rows = Vec::new();
            rows.try_reserve_exact(rows_count).map_err(|_| {
                error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    "vector retirement decode allocation failed",
                )
            })?;
            for _ in 0..rows_count {
                let row = reader.row()?;
                let seq_count = reader.count(8)?;
                charge.try_grow_recoverable(seq_count.saturating_mul(16))?;
                estimate = estimate.saturating_add(seq_count.saturating_mul(16));
                let mut versions = smallvec::SmallVec::new();
                versions.try_reserve_exact(seq_count).map_err(|_| {
                    error(
                        crate::error::catalog::ER_OUTOFMEMORY,
                        "vector retirement versions allocation failed",
                    )
                })?;
                for _ in 0..seq_count {
                    versions.push(reader.u64()?);
                }
                rows.push((row, versions));
            }
            decoded.push((
                VectorPurge {
                    key: (
                        table.id,
                        storage_id,
                        table.active_generation.max(1),
                        normalize_name(&index.name),
                    ),
                    index_id: index.index_id,
                    lineage,
                    rows,
                    commit_scn,
                },
                estimate,
            ));
        }
        reader.finish()?;
        let mut queue = self.vector_purge_queue.lock();
        // A replica's source image is authoritative for this lineage. Keeping
        // obligations absent from its replacement snapshot leaks every past
        // retirement, since replicas must never purge source-owned pages.
        let removed = if self.physical_replica_mode() || self.is_snapshot_read_only() {
            queue.remove_index_snapshot((
                table.id,
                storage_id,
                table.active_generation.max(1),
                index.index_id,
                lineage,
            ))
        } else {
            0
        };
        let mut accepted = 0usize;
        for (purge, estimate) in decoded {
            if queue.insert_if_absent(purge) {
                accepted = accepted.saturating_add(estimate);
            }
        }
        let mut retained = charge.split(accepted);
        let mut holder = self.vector_purge_recovery_charge.lock();
        if let Some(holder) = holder.as_mut() {
            holder.shrink(removed);
            holder.absorb(&mut retained);
        } else {
            *holder = Some(retained);
        }
        self.vector_purges_pending
            .store(queue.len(), Ordering::Release);
        if self.physical_replica_mode() || self.is_snapshot_read_only() {
            self.vector_snapshot_floor
                .fetch_max(maximum_scn, Ordering::AcqRel);
        }
        Ok(())
    }

    fn persist_paged_retirement_snapshot(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
        graph: &VectorGraph,
    ) -> Result<(), QueryError> {
        if self.durability_mode() == DurabilityMode::Memory {
            return Ok(());
        }
        if let VectorGraph::Paged(paged) = graph {
            let snapshot =
                self.index_retirement_snapshot(table, index, storage_id, graph.identity_lineage())?;
            paged.file().write_native_definition(&snapshot.bytes)?;
            paged.file().flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
impl Database {
    pub(crate) fn install_vector_retirement_snapshot_for_test(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        lineage: u64,
        scn: Option<u64>,
    ) -> Result<(), QueryError> {
        let mut bytes = Vec::new();
        let mut writer = vector::persistence::Writer::new(&mut bytes, 10).map_err(io_err)?;
        writer.u64(lineage).map_err(io_err)?;
        writer.count(usize::from(scn.is_some())).map_err(io_err)?;
        if let Some(scn) = scn {
            writer.u64(scn).map_err(io_err)?;
            writer.count(1).map_err(io_err)?;
            writer
                .row(RowId {
                    page_id: 1,
                    slot_id: 0,
                })
                .map_err(io_err)?;
            writer.count(1).map_err(io_err)?;
            writer.u64(scn).map_err(io_err)?;
        }
        writer.finish().map_err(io_err)?;
        self.merge_index_retirement_snapshot(table, index, table.id, lineage, &bytes)
    }
}
