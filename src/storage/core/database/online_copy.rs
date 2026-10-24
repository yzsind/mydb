static DDL_COPY_BARRIER_WAIT_US: AtomicU64 = AtomicU64::new(0);
static DDL_COPY_LOCK_HOLD_US: AtomicU64 = AtomicU64::new(0);
static DDL_COPY_LOCK_HOLD_MAX_US: AtomicU64 = AtomicU64::new(0);
static DDL_COPY_FINAL_TAIL_ROWS: AtomicU64 = AtomicU64::new(0);

struct OnlineCopyHoldTimer(Instant);
impl Drop for OnlineCopyHoldTimer {
    fn drop(&mut self) {
        let us = self.0.elapsed().as_micros().min(u64::MAX as u128) as u64;
        DDL_COPY_LOCK_HOLD_US.fetch_add(us, Ordering::Relaxed);
        DDL_COPY_LOCK_HOLD_MAX_US.fetch_max(us, Ordering::Relaxed);
    }
}

// Online physical ALTER uses immutable read views and a private generation.
// The log records transaction-qualified dirty rows, not speculative row images:
// once a transaction is visible at a gap-free read watermark we materialize its
// final state there. This also handles savepoint rollback and key permutations.
// The initial snapshot pins undo and prevents source RowId reuse until publish.
struct OnlineCopyRows {
    rows: BTreeMap<RowId, crate::storage::DmlRowRef>,
    spill: Option<File>,
    spill_path: PathBuf,
    reserved_rows: usize,
    charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl OnlineCopyRows {
    fn new(data_dir: &Path, job_id: u64) -> Result<Self, QueryError> {
        Ok(Self {
            rows: BTreeMap::new(),
            spill: None,
            reserved_rows: 0,
            spill_path: data_dir.join(format!(".alter-copy-map-{job_id}")),
            charge: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::DdlCopy,
                0,
            )?,
        })
    }
    fn spill(&mut self) -> Result<(), QueryError> {
        if self.spill.is_some() {
            return Ok(());
        }
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&self.spill_path)
            .map_err(io_err)?;
        // This is rebuildable scratch state, never part of the commit protocol.
        crate::platform::file::make_sparse(&file).map_err(io_err)?;
        // Unlink immediately: cancellation/crash cannot leak a large mapping.
        fs::remove_file(&self.spill_path).map_err(io_err)?;
        self.spill = Some(file);
        let rows = std::mem::take(&mut self.rows);
        for (source, shadow) in rows {
            self.write_disk(source, Some(shadow))?;
        }
        self.charge.shrink(self.charge.bytes());
        self.reserved_rows = 0;
        Ok(())
    }
    fn offset(source: RowId) -> u64 {
        // Sparse fixed slots support every physical RowId without requiring a PK.
        ((u64::from(source.page_id) << 16) | u64::from(source.slot_id)) * 16
    }
    fn write_disk(
        &self,
        source: RowId,
        shadow: Option<crate::storage::DmlRowRef>,
    ) -> Result<(), QueryError> {
        #[cfg(unix)]
        use std::os::unix::fs::FileExt;
        let mut bytes = [0; 16];
        if let Some(row) = shadow {
            bytes[..8].copy_from_slice(&row.storage_id.to_le_bytes());
            let value = ((u64::from(row.row_id.page_id) << 16) | u64::from(row.row_id.slot_id)) + 1;
            bytes[8..].copy_from_slice(&value.to_le_bytes());
        }
        self.spill
            .as_ref()
            .expect("spilled mapping")
            .write_all_at(&bytes, Self::offset(source))
            .map_err(io_err)
    }
    fn insert(
        &mut self,
        source: RowId,
        shadow: crate::storage::DmlRowRef,
    ) -> Result<(), QueryError> {
        if self.spill.is_some() {
            return self.write_disk(source, Some(shadow));
        }
        if !self.rows.contains_key(&source) {
            if self.reserved_rows > 0 {
                self.reserved_rows -= 1;
            } else if self.charge.bytes() >= 8 * 1024 * 1024 || !self.charge.try_grow_optional(80) {
                self.spill()?;
                return self.write_disk(source, Some(shadow));
            }
        }
        self.rows.insert(source, shadow);
        Ok(())
    }
    fn prepare_final_tail(&mut self) -> Result<(), QueryError> {
        if self.spill.is_some() || self.reserved_rows >= 1024 {
            return Ok(());
        }
        let extra = 1024 - self.reserved_rows;
        if self.charge.bytes().saturating_add(extra * 80) > 8 * 1024 * 1024
            || !self.charge.try_grow_optional(extra * 80)
        {
            return self.spill();
        }
        self.reserved_rows += extra;
        Ok(())
    }

    fn remove(&mut self, source: &RowId) -> Result<Option<crate::storage::DmlRowRef>, QueryError> {
        if let Some(file) = &self.spill {
            #[cfg(unix)]
            use std::os::unix::fs::FileExt;
            let mut bytes = [0; 16];
            match file.read_exact_at(&mut bytes, Self::offset(*source)) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(err) => return Err(io_err(err)),
            }
            let storage_id = u64::from_le_bytes(bytes[..8].try_into().unwrap());
            let value = u64::from_le_bytes(bytes[8..].try_into().unwrap());
            if value == 0 {
                return Ok(None);
            }
            self.write_disk(*source, None)?;
            let value = value - 1;
            return Ok(Some(crate::storage::DmlRowRef {
                storage_id,
                row_id: RowId {
                    page_id: (value >> 16) as u32,
                    slot_id: value as u16,
                },
            }));
        }
        let removed = self.rows.remove(source);
        if removed.is_some() {
            self.reserved_rows += 1;
        }
        Ok(removed)
    }
}

impl Database {
    #[cfg(test)]
    pub(crate) fn install_online_copy_publish_test_hook(&self) -> Arc<DdlBackfillTestHook> {
        let hook = Arc::new(DdlBackfillTestHook::new());
        *self.online_copy_publish_test_hook.lock().unwrap() = Some(hook.clone());
        hook
    }

    /// One capability decision shared by MDL selection and execution. Explicit
    /// unsupported LOCK=NONE requests must never fall through to offline COPY.
    pub(crate) fn online_type_copy_supported(
        &self,
        table_name: &str,
        actions: &[crate::parser::AlterAction],
        strict: bool,
        lock: crate::parser::AlterLock,
        copy_requested: bool,
    ) -> bool {
        if (!strict && lock != crate::parser::AlterLock::Shared)
            || !ddl_copy_streaming_enabled()
            || (self.config.durability == DurabilityMode::Memory
                && lock != crate::parser::AlterLock::Shared)
        {
            return false;
        }
        use crate::parser::AlterAction as A;
        if actions.is_empty()
            || !actions.iter().all(|a| match a {
                A::AddColumn(_) | A::ModifyColumn(_) | A::ChangeColumn { .. } => true,
                A::AddIndex(i) => i.index_type.eq_ignore_ascii_case("BTREE"),
                A::Rebuild
                | A::DropColumn(_)
                | A::DropIndex(_)
                | A::MoveColumn { .. }
                | A::ConvertToCharset { .. } => true,
                _ => false,
            })
        {
            return false;
        }
        let Ok(source) = self.table(table_name) else {
            return false;
        };
        // Shared mode captures no concurrent writes, so derived special indexes
        // only need the existing bulk builder. Writable replay stays BTREE/vector.
        (source
            .indexes
            .iter()
            .all(|index| index.index_type.eq_ignore_ascii_case("BTREE") || index.is_vector())
            || (lock == crate::parser::AlterLock::Shared && !source.is_partitioned()))
            && (copy_requested
                || actions.iter().any(|action| {
                    self.alter_action_requires_copy(table_name, action)
                        .unwrap_or(false)
                }))
    }

    fn run_online_type_copy(
        &self,
        table_name: &str,
        source: Arc<TableSchema>,
        mut target: TableSchema,
        strict: bool,
        owner: u64,
        lock: crate::parser::AlterLock,
    ) -> Result<Vec<crate::engine::api::SqlWarning>, QueryError> {
        let barrier = OnlineIndexMdlGuard { db: self, owner };
        // Serialize the pin with statistics publication. Background ANALYZE
        // must not revise the descriptor during COPY, including barrier waits.
        let _descriptor_guard = {
            let _catalog = self.catalog_write()?;
            self.catalog.online_pinned_ids.write().insert(source.id);
            OnlineIndexDescriptorGuard {
                db: self,
                table_id: source.id,
            }
        };
        self.online_index_barrier(&source, owner, false)?;
        let actual = self.table(table_name)?;
        if !same_online_copy_definition(&actual, &source)? {
            return Err(error(
                crate::error::catalog::ER_TABLE_DEF_CHANGED,
                "table changed while preparing online ALTER",
            ));
        }
        // Statistics may have changed after planning but before the pin.
        // Preserve a strictly newer publication revision for plan invalidation.
        target.schema_version = target
            .schema_version
            .max(actual.schema_version.saturating_add(1));
        let source = actual;
        let job = self.register_alter_copy_job(&source);
        target.reset_row_layouts_for_rewrite();
        target.build_column_index();
        let result = if source.is_partitioned() {
            self.rewrite_partitioned_type_copy(
                source.clone(),
                target,
                strict,
                job,
                Some(owner),
                None,
            )
        } else {
            self.rewrite_table_for_alter_copy_streaming_direct(
                table_name,
                source.clone(),
                target,
                strict,
                Some(job),
                Some((owner, lock)),
            )
        };
        drop(barrier);
        self.unregister_alter_copy_job(source.id, result.as_ref().err());
        result
    }

    fn take_online_copy_ready_rows(
        &self,
        job: &AlterCopyJob,
        snapshot: TxnView,
    ) -> Result<OnlineCopyReadyRows, QueryError> {
        // Rotate the log under its latch; classification and I/O run outside
        // it so business writers are never held behind an entire replay pass.
        let batch = {
            let mut changes = job.changes.lock().unwrap_or_else(|p| p.into_inner());
            std::mem::replace(&mut *changes, AlterCopyChangeLog::new(self.data_dir()))
        };
        if batch.invalidated {
            return Err(error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "online ALTER change log exceeded its budget or could not be written",
            ));
        }
        let charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::DdlOnlineLog,
            batch
                .len()
                .saturating_mul(std::mem::size_of::<(u64, RowId)>()),
        )?;
        let mut ready = Vec::with_capacity(batch.len());
        batch.visit(|(txn_id, storage_id, row)| {
            let status = self.slot_status_cache.lookup(txn_id).or_else(|| {
                self.txns
                    .read()
                    .slot(txn_id)
                    .map(|slot| (slot.status, slot.commit_scn.unwrap_or(0)))
            });
            if matches!(status, Some((TxnStatus::RolledBack, _))) {
                return Ok(());
            }
            if self.row_version_visible(RowHeader::visible(txn_id), snapshot)? {
                ready.push((storage_id, row));
            } else {
                let mut changes = job.changes.lock().unwrap_or_else(|p| p.into_inner());
                changes.record((txn_id, storage_id, row))?;
            }
            Ok(())
        })?;
        ready.sort_unstable();
        ready.dedup();
        Ok(OnlineCopyReadyRows {
            rows: ready,
            _charge: charge,
        })
    }

    fn replay_online_copy_rows(
        &self,
        source: &TableSchema,
        shadow: &TableSchema,
        ddl_txn: TxnView,
        job: &Arc<AlterCopyJob>,
        mapping: &mut OnlineCopyRows,
        strict: bool,
        warnings: &mut Vec<crate::engine::api::SqlWarning>,
        deadline: Option<Instant>,
    ) -> Result<usize, QueryError> {
        let snapshot = StorageReadSnapshot::new(self);
        let read = snapshot.txn;
        let rows = self.take_online_copy_ready_rows(job, read)?;
        let mut tail_bytes = 0usize;
        let count = rows.rows.len();
        // Remove the complete cohort before adding its replacement keys. A
        // committed transaction can swap unique values between multiple rows.
        for (_, row) in &rows.rows {
            Self::check_alter_copy_cancelled(job)?;
            check_online_copy_deadline(deadline)?;
            if let Some(old_ref) = mapping.remove(row)? {
                self.delete_online_copy_value(shadow, ddl_txn, old_ref)?;
            }
        }
        for (storage_id, source_id) in rows.rows {
            Self::check_alter_copy_cancelled(job)?;
            check_online_copy_deadline(deadline)?;
            let Some(row) = self.read_mvcc_row(storage_id, source_id)? else {
                continue;
            };
            let Some(mut row) = self.visible_row(row, read)? else {
                continue;
            };
            Self::pad_full_row_to_schema(&mut row.values, source);
            let values = self.prepare_alter_copy_values(shadow, row.values, strict, warnings)?;
            tail_bytes = tail_bytes.saturating_add(
                crate::engine::executor::mem_budget::estimate_row_bytes(&values),
            );
            if deadline.is_some() && tail_bytes > 4 * 1024 * 1024 {
                return Err(error(
                    crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                    "online ALTER final tail exceeded 4 MiB; retry the ALTER",
                ));
            }
            let location = self.insert_online_copy_value(shadow, ddl_txn, values)?;
            mapping.insert(source_id, location)?;
        }
        Self::set_alter_copy_job_replay_watermark(job, read.snapshot_scn);
        Ok(count)
    }

    fn insert_online_copy_batch(
        &self,
        txn_id: u64,
        storage_id: u64,
        codec: &RowCodec,
        headers: &[RowHeader],
        rows: &[Vec<Value>],
    ) -> Result<Vec<RowId>, QueryError> {
        let external = rows
            .iter()
            .map(|values| self.externalize_row_lobs(storage_id, codec, values))
            .collect::<Result<Vec<_>, _>>()?;
        self.with_heap_read(storage_id, |heap| {
            heap.insert_mvcc_batch(txn_id, headers, rows, &external)
        })
    }

    fn flush_online_copy_shadow(&self, shadow: &TableSchema) -> Result<(), QueryError> {
        self.flush_online_copy_vectors(shadow, shadow.id)?;
        // Only copy immutable flush handles under the heap latch. Holding its
        // read guard throughout writeback would block ordinary heap writers.
        let heap = self.heap_handle(shadow.id)?;
        let (heap_key, heap_file, durability) = {
            let heap = heap.read();
            (
                heap.file_key.clone(),
                heap.file.try_clone().map_err(io_err)?,
                heap.durability(),
            )
        };
        if durability != DurabilityMode::Memory {
            self.buffer_pool.flush_file(&heap_key)?;
            if durability == DurabilityMode::Strict {
                heap_file.sync_data().map_err(io_err)?;
            }
        }
        for index in &shadow.indexes {
            self.with_index_read(shadow.id, &index.name, |file| file.flush())?;
        }
        let lob = self.overflow_path(shadow.id);
        if lob.exists() {
            let overflow = self.overflow_handle(shadow.id)?;
            let (key, file) = {
                let overflow = overflow.read();
                (
                    overflow.file_key.clone(),
                    overflow
                        .file
                        .as_ref()
                        .map(File::try_clone)
                        .transpose()
                        .map_err(io_err)?,
                )
            };
            self.buffer_pool.flush_file(&key)?;
            if self.config.durability == DurabilityMode::Strict {
                if let Some(file) = file {
                    file.sync_all().map_err(io_err)?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod online_copy_mapping_tests {
    use super::*;

    #[test]
    fn spilled_online_copy_map_preserves_absence_replacement_and_large_row_ids() {
        let dir = tempfile::tempdir().unwrap();
        let mut map = OnlineCopyRows::new(dir.path(), 1).unwrap();
        let rows = [
            RowId::new(0, 0),
            RowId::new(4, 123),
            RowId::new(65537, 65535),
        ];
        for (i, row) in rows.iter().enumerate() {
            map.insert(
                *row,
                crate::storage::DmlRowRef {
                    storage_id: 5,
                    row_id: RowId::new(10 + i as u32, 2),
                },
            )
            .unwrap();
        }
        map.spill().unwrap();
        assert_eq!(map.remove(&RowId::new(3, 10)).unwrap(), None);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(
                map.remove(row).unwrap(),
                Some(crate::storage::DmlRowRef {
                    storage_id: 5,
                    row_id: RowId::new(10 + i as u32, 2)
                })
            );
            assert_eq!(map.remove(row).unwrap(), None);
            map.insert(
                *row,
                crate::storage::DmlRowRef {
                    storage_id: 6,
                    row_id: RowId::new(20, 3),
                },
            )
            .unwrap();
            assert_eq!(
                map.remove(row).unwrap(),
                Some(crate::storage::DmlRowRef {
                    storage_id: 6,
                    row_id: RowId::new(20, 3)
                })
            );
        }
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}

impl Database {
    fn delete_online_copy_value(
        &self,
        target: &TableSchema,
        txn: TxnView,
        location: crate::storage::DmlRowRef,
    ) -> Result<(), QueryError> {
        let old = self
            .read_mvcc_row(location.storage_id, location.row_id)?
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "online ALTER shadow row is missing",
                )
            })?;
        for index in &target.indexes {
            for key in target.index_keys(index, &old.values)? {
                self.with_index_dml(location.storage_id, &index.name, |file| {
                    file.delete_deferred(txn.txn_id, key, location.row_id)
                })?;
            }
        }
        for index in target.indexes.iter().filter(|index| index.is_vector()) {
            self.vector_graph_mark_deleted(target, index, location.row_id, location.storage_id);
        }
        let refs = self.read_row_external_refs(location.storage_id, location.row_id)?;
        self.with_heap_read(location.storage_id, |heap| {
            heap.delete(txn.txn_id, location.row_id)
        })?;
        self.free_external_refs(location.storage_id, &refs);
        Ok(())
    }

    fn insert_online_copy_value(
        &self,
        target: &TableSchema,
        txn: TxnView,
        values: Vec<Value>,
    ) -> Result<crate::storage::DmlRowRef, QueryError> {
        let storage_id = if target.is_partitioned() {
            target.route_row_to_partition(&values)?.storage_id
        } else {
            target.id
        };
        let mut keys = Vec::new();
        for index in &target.indexes {
            for key in target.index_keys(index, &values)? {
                if index.unique
                    && (index.primary || !key.0.iter().any(|part| matches!(part, KeyPart::Null)))
                {
                    if self
                        .with_index_read(storage_id, &index.name, |file| file.get_unique_one(&key))?
                        .is_some()
                    {
                        return Err(error(
                            crate::error::catalog::ER_DUP_ENTRY,
                            format!("duplicate entry for key '{}'", index.name),
                        ));
                    }
                }
                keys.push((&index.name, key));
            }
        }
        let vector_values = target
            .indexes
            .iter()
            .filter(|index| index.is_vector())
            .map(|index| {
                let pos = target.column_index(&index.columns[0])?;
                Ok((index.name.clone(), values.get(pos).cloned()))
            })
            .collect::<Result<Vec<_>, QueryError>>()?;
        let row_id = self.insert_online_copy_batch(
            txn.txn_id,
            storage_id,
            &target.row_codec,
            &[RowHeader::visible(txn.txn_id)],
            &[values],
        )?[0];
        for (name, key) in keys {
            self.with_index_dml(storage_id, name, |file| {
                file.insert_deferred(txn.txn_id, key, row_id)
            })?;
        }
        for (name, value) in vector_values {
            if let Some(Value::Vector(bytes)) = value {
                let graph = self
                    .vector_indexes
                    .read()
                    .get(&(
                        target.id,
                        storage_id,
                        target.active_generation.max(1),
                        normalize_name(&name),
                    ))
                    .cloned();
                if let Some(graph) = graph {
                    let vector = vector::to_f32(&bytes);
                    match graph.as_ref() {
                        VectorGraph::NativeHnsw(g) => g.insert(row_id, bytes)?,
                        VectorGraph::NativeIvf(g) => g.insert(row_id, bytes)?,
                        VectorGraph::Memory(g) => {
                            g.insert(row_id, vector)?;
                        }
                        VectorGraph::Paged(g) => {
                            g.insert_ddl(row_id, &vector)?;
                        }
                    }
                }
            }
        }
        Ok(crate::storage::DmlRowRef { storage_id, row_id })
    }

    fn replay_partitioned_type_copy(
        &self,
        source: &TableSchema,
        target: &TableSchema,
        txn: TxnView,
        job: &Arc<AlterCopyJob>,
        maps: &mut BTreeMap<u64, OnlineCopyRows>,
        counts: &mut BTreeMap<u64, u64>,
        retained: &mut RetainedPartitionCounts,
        strict: bool,
        warnings: &mut Vec<crate::engine::api::SqlWarning>,
        deadline: Option<Instant>,
    ) -> Result<std::collections::BTreeSet<u64>, QueryError> {
        let _timer = OnlineIndexTimer(&DDL_COPY_REPLAY_US, Instant::now());
        let snapshot = StorageReadSnapshot::new(self);
        let read = snapshot.txn;
        let rows = self.take_online_copy_ready_rows(job, read)?;
        let mut tail_bytes = 0usize;
        let mut dirty = std::collections::BTreeSet::new();
        for (storage_id, row_id) in &rows.rows {
            Self::check_alter_copy_cancelled(job)?;
            check_online_copy_deadline(deadline)?;
            let Some(map) = maps.get_mut(storage_id) else {
                continue;
            };
            if let Some(old) = map.remove(row_id)? {
                self.delete_online_copy_value(target, txn, old)?;
                dirty.insert(old.storage_id);
                *counts.entry(old.storage_id).or_default() -= 1;
            }
        }
        for (storage_id, row_id) in rows.rows {
            if !maps.contains_key(&storage_id) {
                if let Some(stats) = retained.stats.get_mut(&storage_id) {
                    Self::check_alter_copy_cancelled(job)?;
                    check_online_copy_deadline(deadline)?;
                    let heap = self.heap_handle(storage_id)?;
                    let before = self.is_row_id_visible_with_handle(
                        &heap,
                        source,
                        row_id,
                        retained.watermark,
                    )?;
                    let after = self.is_row_id_visible_with_handle(&heap, source, row_id, read)?;
                    stats.row_count = stats
                        .row_count
                        .checked_add_signed(i64::from(after) - i64::from(before))
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "retained partition row count overflow",
                            )
                        })?;
                }
                continue;
            }
            Self::check_alter_copy_cancelled(job)?;
            check_online_copy_deadline(deadline)?;
            let Some(row) = self.read_mvcc_row(storage_id, row_id)? else {
                continue;
            };
            let Some(mut row) = self.visible_row(row, read)? else {
                continue;
            };
            Self::pad_full_row_to_schema(&mut row.values, source);
            let values = self.prepare_alter_copy_values(target, row.values, strict, warnings)?;
            tail_bytes = tail_bytes.saturating_add(
                crate::engine::executor::mem_budget::estimate_row_bytes(&values),
            );
            if deadline.is_some() && tail_bytes > 4 * 1024 * 1024 {
                return Err(error(
                    crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                    "online ALTER final tail exceeded 4 MiB; retry the ALTER",
                ));
            }
            let target_id = if target.is_partitioned() {
                target.route_row_to_partition(&values)?.storage_id
            } else {
                target.id
            };
            if !counts.contains_key(&target_id) {
                return Err(error(
                    crate::error::catalog::ER_REORG_OUTSIDE_RANGE,
                    "online partition rewrite would write into a retained leaf",
                ));
            }
            let location = self.insert_online_copy_value(target, txn, values)?;
            dirty.insert(location.storage_id);
            maps.get_mut(&storage_id)
                .unwrap()
                .insert(row_id, location)?;
            *counts.entry(location.storage_id).or_default() += 1;
        }
        Self::set_alter_copy_job_replay_watermark(job, read.snapshot_scn);
        retained.watermark = read;
        Ok(dirty)
    }

    /// Establish exact retained-leaf counts outside the publication barrier.
    /// The job captures all source leaves, so subsequent replay adjusts only
    /// changed row identities at a common MVCC watermark without a rescan.
    fn snapshot_retained_partition_counts(
        &self,
        source: &TableSchema,
        target: &TableSchema,
        build_ids: &[u64],
        snapshot: TxnView,
        job: &AlterCopyJob,
    ) -> Result<RetainedPartitionCounts, QueryError> {
        let mut result = RetainedPartitionCounts {
            stats: BTreeMap::new(),
            watermark: snapshot,
        };
        if !target.is_partitioned() {
            return Ok(result);
        }
        let built = build_ids.iter().copied().collect::<BTreeSet<_>>();
        for id in target
            .storage_ids()
            .into_iter()
            .filter(|id| !built.contains(id))
        {
            let pages = self.with_heap_read(id, |heap| heap.page_count())?;
            let mut stats = target
                .stats
                .partition_stats
                .get(&id)
                .cloned()
                .unwrap_or_else(crate::storage::PartitionStats::empty);
            stats.row_count = 0;
            stats.heap_page_count = pages;
            for start in (0..pages).step_by(32) {
                Self::check_alter_copy_cancelled(job)?;
                crate::engine::cancel::check_cancelled()?;
                self.scan_rows_columns_storage_streaming_range(
                    source,
                    id,
                    snapshot,
                    &[],
                    start,
                    (start + 32).min(pages),
                    |_| {
                        stats.row_count += 1;
                        Ok(())
                    },
                )?;
            }
            result.stats.insert(id, stats);
        }
        Ok(result)
    }

    fn build_partition_copy_indexes(
        &self,
        target: &TableSchema,
        build_ids: &[u64],
        txn: TxnView,
        check: impl Fn() -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let _timer = OnlineIndexTimer(&DDL_COPY_INDEX_US, Instant::now());
        let _build_timer = OnlineIndexTimer(&DDL_COPY_INDEX_BUILD_US, Instant::now());
        if target.indexes.is_empty() {
            return Ok(());
        }
        for id in build_ids {
            let mut leaf = target.clone();
            leaf.id = *id;
            leaf.partitioning = None;
            let mut builders = DdlCopyIndexBuilders::new(&leaf)?;
            let plans = builders.key_plans(&leaf);
            let pages = self.with_heap_read(*id, |heap| heap.page_count())?;
            for page in 0..pages {
                check()?;
                let rows = self.scan_rows_storage_range(&leaf, *id, txn, page, page + 1)?;
                let mut ids = Vec::with_capacity(rows.len());
                let mut batch = Vec::with_capacity(rows.len());
                for mut row in rows {
                    ids.push(row.row_id);
                    let keys = DdlCopyIndexBuilders::prepare_keys(&leaf, &plans, &mut row.values)?;
                    batch.push(DdlCopyPreparedRow {
                        source_row: row.row_id,
                        values: None,
                        payload: Vec::new(),
                        keys,
                    });
                }
                builders.push_prepared_batch(self.data_dir(), batch, &ids)?;
            }
            builders.finish_partition(self, &leaf)?;
        }
        Ok(())
    }

    fn rewrite_partitioned_type_copy(
        &self,
        source: Arc<TableSchema>,
        mut target: TableSchema,
        strict: bool,
        job: Arc<AlterCopyJob>,
        owner: Option<u64>,
        rewrite: Option<PartitionRewriteLayout>,
    ) -> Result<Vec<crate::engine::api::SqlWarning>, QueryError> {
        DDL_COPY_TOTAL.fetch_add(1, Ordering::Relaxed);
        let _job_cancel_scope = crate::engine::cancel::enter(job.cancel_token.clone());
        let _binlog = crate::binlog::SessionSqlLogBinGuard::new(false);
        // A private writer transaction alone does not pin RC undo. Register
        // the read horizon atomically with purge before choosing its snapshot.
        let is_reorg = rewrite.is_some();
        let mut _source_snapshot = if is_reorg {
            None
        } else {
            Some(StorageReadSnapshot::new(self))
        };
        let txn = self.begin_txn()?;
        let _txn = OnlineCopyTxnGuard {
            db: self,
            txn_id: txn.txn_id,
        };
        target.row_generation_txn_id = txn.txn_id;
        let source_ids = rewrite
            .as_ref()
            .map(|r| r.source_ids.clone())
            .unwrap_or_else(|| source.storage_ids());
        let mut lock_ids = source_ids.clone();
        lock_ids.push(source.id);
        lock_ids.sort_unstable();
        lock_ids.dedup();
        if !is_reorg {
            self.try_online_index_data_barrier(txn.txn_id, &lock_ids)?;
        }
        let build_ids = if let Some(layout) = &rewrite {
            layout.build_ids.clone()
        } else {
            let build_ids = {
                let mut catalog = self.catalog_write()?;
                (0..source_ids.len())
                    .map(|_| {
                        let id = catalog.next_table_id;
                        catalog.next_table_id += 1;
                        id
                    })
                    .collect::<Vec<_>>()
            };
            self.ensure_table_id_highwater(self.current_next_table_id()?)?;
            let remap = source_ids
                .iter()
                .copied()
                .zip(build_ids.iter().copied())
                .collect::<BTreeMap<_, _>>();
            for partition in &mut target.partitioning.as_mut().unwrap().partitions {
                if partition.subpartitions.is_empty() {
                    partition.storage_id = remap[&partition.storage_id];
                } else {
                    for sub in &mut partition.subpartitions {
                        sub.storage_id = remap[&sub.storage_id];
                    }
                }
            }
            build_ids
        };
        target.build_column_index();
        let names = target
            .indexes
            .iter()
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();
        let mut journal = PartitionReorgJournal {
            job_id: job.job_id,
            table_id: source.id,
            database: source.database.clone(),
            table_name: source.name.clone(),
            op: rewrite
                .as_ref()
                .map(|r| r.operation.clone())
                .unwrap_or_else(|| "online_type_copy".into()),
            index_names: names.clone(),
            generation: target.active_generation.max(1),
            index_ids: target
                .indexes
                .iter()
                .map(|i| (i.name.clone(), i.index_id))
                .collect(),
            source_sids: source_ids.clone(),
            build_sids: build_ids.clone(),
            phase: PartitionDdlPhase::SnapshotBuild,
            cancel_requested: false,
            copied_rows: 0,
            total_rows: None,
            replay_watermark: None,
            error_message: None,
        };
        self.register_reorg_storage_definition(&build_ids, &target);
        let journal_path = match self.write_partition_reorg_journal(&journal) {
            Ok(path) => path,
            Err(err) => {
                self.unregister_reorg_storage_locs(&build_ids);
                return Err(err);
            }
        };
        let mut committed = false;
        let mut preserve_intent = false;
        let result = (|| {
            validate_row_fits_page(&target)?;
            for id in &build_ids {
                if *id == source.id && is_reorg {
                    self.reclaim_partition_target_before_reuse(*id, &journal)?;
                }
                self.create_logged_file(txn.txn_id, self.table_path(*id), Vec::new())?;
                self.set_cached_heap_codec(*id, target.row_codec.clone())?;
            }
            // File creation and codec/LOB initialization need only SU: the
            // targets are private. Drain writers and choose the copy snapshot
            // afterwards, so preparation I/O does not hold X and transactions
            // that wrote before capture registration are included in the scan.
            let mut source_read = txn;
            let mut prepare_hold = None;
            if let Some(layout) = &rewrite {
                let owner = owner.ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "online partition rewrite requires a SQL metadata lock owner",
                    )
                })?;
                let started = Instant::now();
                self.acquire_alter_execution_mdl_checked(
                    owner,
                    &source.qualified_name(),
                    MdlLockType::Exclusive,
                    layout.timeout,
                    MdlDuration::Transaction,
                    || Self::check_alter_copy_cancelled(&job),
                )?;
                DDL_COPY_BARRIER_WAIT_US
                    .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
                prepare_hold = Some(OnlineCopyHoldTimer(Instant::now()));
                self.try_online_index_data_barrier(txn.txn_id, &lock_ids)?;
                let pin = StorageReadSnapshot::new(self);
                source_read = pin.txn;
                _source_snapshot = Some(pin);
            }
            if let Some(owner) = owner {
                for id in &lock_ids {
                    self.table_locks_sharded
                        .downgrade_exclusive_to_intent_shared(txn.txn_id, *id)?;
                }
                self.release_mdl_duration(owner, MdlDuration::Transaction);
                self.notify_non_row_lock_waiters();
            }
            drop(prepare_hold);
            let mut retained = self.snapshot_retained_partition_counts(
                &source,
                &target,
                &build_ids,
                source_read,
                &job,
            )?;
            let mut maps = BTreeMap::new();
            for id in &source_ids {
                let mut map = OnlineCopyRows::new(self.data_dir(), job.job_id)?;
                map.spill_path = self
                    .data_dir
                    .join(format!(".alter-copy-map-{}-{id}", job.job_id));
                maps.insert(*id, map);
            }
            let mut warnings = Vec::new();
            let mut counts = build_ids
                .iter()
                .map(|id| (*id, 0))
                .collect::<BTreeMap<u64, u64>>();
            #[cfg(test)]
            self.pause_ddl_backfill_for_test();
            let scan_timer = OnlineIndexTimer(&DDL_COPY_SCAN_US, Instant::now());
            for source_id in &source_ids {
                let pages = self.with_heap_read(*source_id, |heap| heap.page_count())?;
                DDL_COPY_SOURCE_BYTES
                    .fetch_add(pages.saturating_mul(PAGE_SIZE as u64), Ordering::Relaxed);
                // One physical page bounds the external-value materialization cohort.
                for page in 0..pages {
                    Self::check_alter_copy_cancelled(&job)?;
                    let rows = self.scan_rows_storage_range(
                        &source,
                        *source_id,
                        source_read,
                        page,
                        page + 1,
                    )?;
                    if is_reorg {
                        note_reorg_batch_rows(rows.len());
                    }
                    let mut buckets = BTreeMap::<u64, Vec<(RowId, Vec<Value>)>>::new();
                    for row in rows {
                        let values = self.prepare_alter_copy_values(
                            &target,
                            row.values,
                            strict,
                            &mut warnings,
                        )?;
                        let target_id = if target.is_partitioned() {
                            target.route_row_to_partition(&values)?.storage_id
                        } else {
                            target.id
                        };
                        if !counts.contains_key(&target_id) {
                            return Err(error(
                                crate::error::catalog::ER_REORG_OUTSIDE_RANGE,
                                "online partition rewrite would write into a retained leaf",
                            ));
                        }
                        buckets
                            .entry(target_id)
                            .or_default()
                            .push((row.row_id, values));
                    }
                    for (target_id, rows) in buckets {
                        let source_rows = rows.iter().map(|r| r.0).collect::<Vec<_>>();
                        let values = rows.into_iter().map(|r| r.1).collect::<Vec<_>>();
                        let _charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                            crate::engine::executor::mem_budget::ControlledMemoryCategory::DdlCopy,
                            values.iter().map(|v|crate::engine::executor::mem_budget::estimate_row_bytes(v)).sum(),
                        )?;
                        let headers = vec![RowHeader::visible(txn.txn_id); values.len()];
                        let inserted = self.insert_online_copy_batch(
                            txn.txn_id,
                            target_id,
                            &target.row_codec,
                            &headers,
                            &values,
                        )?;
                        if owner.is_some() {
                            for (source_row, row_id) in source_rows.into_iter().zip(inserted) {
                                maps.get_mut(source_id).unwrap().insert(
                                    source_row,
                                    crate::storage::DmlRowRef {
                                        storage_id: target_id,
                                        row_id,
                                    },
                                )?;
                            }
                        }
                        *counts.entry(target_id).or_default() += values.len() as u64;
                        Self::advance_alter_copy_job_copied_by(&job, values.len() as u64);
                    }
                }
            }
            drop(scan_timer);
            Self::set_alter_copy_job_state(&job, "INDEX_BUILD");
            self.build_partition_copy_indexes(&target, &build_ids, txn, || {
                Self::check_alter_copy_cancelled(&job)
            })?;
            for id in &build_ids {
                self.build_online_copy_vectors(&target, *id)?;
            }
            Self::set_alter_copy_job_state(&job, "CATCH_UP");
            let mut _held = None;
            if let Some(owner) = owner {
                for attempt in 0..32 {
                    self.replay_partitioned_type_copy(
                        &source,
                        &target,
                        txn,
                        &job,
                        &mut maps,
                        &mut counts,
                        &mut retained,
                        strict,
                        &mut warnings,
                        None,
                    )?;
                    for id in source_ids.iter().chain(&build_ids) {
                        let mut leaf = target.clone();
                        leaf.id = *id;
                        self.flush_online_copy_shadow(&leaf)?;
                        self.flush_online_copy_vectors(&target, *id)?;
                    }
                    for map in maps.values_mut() {
                        map.prepare_final_tail()?;
                    }
                    #[cfg(test)]
                    if is_reorg && attempt == 0 {
                        self.pause_ddl_backfill_for_test();
                    }
                    let started = Instant::now();
                    if let Some(layout) = &rewrite {
                        self.acquire_alter_execution_mdl_checked(
                            owner,
                            &source.qualified_name(),
                            MdlLockType::Exclusive,
                            layout.timeout,
                            MdlDuration::Transaction,
                            || Self::check_alter_copy_cancelled(&job),
                        )?;
                    } else {
                        self.online_index_barrier_checked(
                            &source,
                            owner,
                            false,
                            MdlLockType::Exclusive,
                            || Self::check_alter_copy_cancelled(&job),
                        )?;
                    }
                    DDL_COPY_BARRIER_WAIT_US
                        .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
                    let held = OnlineCopyHoldTimer(Instant::now());
                    self.try_online_index_data_barrier(txn.txn_id, &lock_ids)?;
                    let tail = job.changes.lock().unwrap_or_else(|e| e.into_inner()).len();
                    if tail <= 1024 {
                        DDL_COPY_FINAL_TAIL_ROWS.fetch_add(tail as u64, Ordering::Relaxed);
                        let dirty_targets = self.replay_partitioned_type_copy(
                            &source,
                            &target,
                            txn,
                            &job,
                            &mut maps,
                            &mut counts,
                            &mut retained,
                            strict,
                            &mut warnings,
                            Some(online_copy_deadline()),
                        )?;
                        // Every target was flushed before taking X. Only
                        // tail-touched leaves require a second flush here.
                        for id in &dirty_targets {
                            let mut leaf = target.clone();
                            leaf.id = *id;
                            self.flush_online_copy_shadow(&leaf)?;
                            self.flush_online_copy_vectors(&target, *id)?;
                        }
                        _held = Some(held);
                        break;
                    }
                    for id in &lock_ids {
                        self.table_locks_sharded
                            .downgrade_exclusive_to_intent_shared(txn.txn_id, *id)?;
                    }
                    self.release_mdl_duration(owner, MdlDuration::Transaction);
                    self.notify_non_row_lock_waiters();
                    if attempt == 31 {
                        return Err(error(
                            crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                            "online partition ALTER could not converge",
                        ));
                    }
                }
            } else {
                for id in &build_ids {
                    let mut leaf = target.clone();
                    leaf.id = *id;
                    self.flush_online_copy_shadow(&leaf)?;
                    self.flush_online_copy_vectors(&target, *id)?;
                }
            }
            if is_reorg && partition_reorg_crash_at("before_swap") {
                preserve_intent = true;
                return Ok(warnings);
            }
            Self::enter_alter_copy_finalizing(&job)?;
            if is_reorg {
                let live = self.table(&source.qualified_name())?;
                target.stats = live.stats.clone();
                for id in &source_ids {
                    target.stats.partition_stats.remove(id);
                }
                journal.phase = PartitionDdlPhase::Publishing;
                self.update_partition_reorg_journal(&journal_path, &mut journal)?;
            } else {
                target.stats.partition_stats.clear();
            }
            for id in &build_ids {
                let mut stats = crate::storage::PartitionStats::empty();
                stats.row_count = counts.get(id).copied().unwrap_or(0);
                stats.heap_page_count = self.with_heap_read(*id, |heap| heap.page_count())?;
                if target.is_partitioned() {
                    target.stats.partition_stats.insert(*id, stats);
                }
            }
            if target.is_partitioned() {
                let mut stats = std::mem::take(&mut target.stats.partition_stats);
                for (id, mut retained_stats) in retained.stats {
                    retained_stats.heap_page_count =
                        self.with_heap_read(id, |heap| heap.page_count())?;
                    stats.insert(id, retained_stats);
                }
                target.stats.row_count = stats.values().map(|s| s.row_count).sum();
                target.stats.partition_stats = stats;
            } else {
                target.stats.row_count = counts.values().sum();
            }
            target.stats.heap_page_count = if target.is_partitioned() {
                target
                    .stats
                    .partition_stats
                    .values()
                    .map(|s| s.heap_page_count)
                    .sum()
            } else {
                self.with_heap_read(target.id, |heap| heap.page_count())?
            };
            let key = table_key(&source.database, &source.name);
            self.catalog_write()?
                .tables
                .insert(key.clone(), Arc::new(target.clone()));
            self.note_catalog_table(source.id);
            if let Err(err) = self.commit_catalog_txn_existing(txn) {
                self.quarantine_mvcc(format!(
                    "online partition publication failed; restart recovery required: {}",
                    err.message()
                ));
                preserve_intent = true;
                return Err(err);
            }
            self.drop_table_modification_delta(source.id);
            committed = true;
            if is_reorg && partition_reorg_crash_at("after_swap") {
                preserve_intent = true;
            }
            self.primary_definition_highwater
                .fetch_max(self.visible_scn(), Ordering::Release);
            self.table_locks_sharded
                .set_schema_floor(source.id, target.schema_version);
            for id in &build_ids {
                self.table_locks_sharded
                    .set_schema_floor(*id, target.schema_version);
            }
            if let Err(err) = self.mark_table_stats_stale(&key, target.stats.row_count.max(1)) {
                log_message(format!(
                    "committed partition DDL statistics refresh pending: {}",
                    err.message()
                ));
            }
            Ok(warnings)
        })();
        if committed {
            let graphs = self.vector_indexes.read();
            job.retired_vector_graphs
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .extend(
                    graphs
                        .iter()
                        .filter(|((tid, sid, _, _), _)| {
                            *tid == source.id && source_ids.contains(sid)
                        })
                        .map(|(_, graph)| graph.clone()),
                );
        }
        if !committed {
            let _ = self.rollback_txn(txn.txn_id);
        }
        // Release the barriers before reclaiming obsolete private/source files.
        if let Some(owner) = owner {
            self.release_mdl_duration(owner, MdlDuration::Transaction);
        }
        if preserve_intent {
            self.unregister_reorg_storage_locs(&build_ids);
            return result;
        }
        journal.phase = if committed {
            PartitionDdlPhase::Cleanup
        } else {
            PartitionDdlPhase::Failed
        };
        journal.error_message = result.as_ref().err().map(|err| err.message().to_string());
        if let Err(err) = self.update_partition_reorg_journal(&journal_path, &mut journal) {
            self.unregister_reorg_storage_locs(&build_ids);
            if committed {
                log_message(format!(
                    "committed partition DDL cleanup journal pending: {}",
                    err.message()
                ));
                return result;
            }
            return Err(err);
        }
        if committed && self.partition_cleanup_background.load(Ordering::Acquire) {
            self.unregister_reorg_storage_locs(&build_ids);
            return result;
        }
        let victims = if committed { &source_ids } else { &build_ids };
        self.vector_indexes
            .write()
            .retain(|(tid, sid, _, _), _| *tid != source.id || !victims.contains(sid));
        let mut cleanup_error = None;
        for id in victims {
            let _ = self.remove_heap_cache_entry(*id);
            let _ = self.remove_overflow_cache_entry(*id);
            for name in &names {
                let _ = self.remove_index_cache_entry(*id, name);
            }
            if let Err(err) = self.remove_partition_storage_files(
                &source.database,
                &source.name,
                source.id,
                *id,
                &names,
                &journal,
            ) {
                cleanup_error.get_or_insert(err);
            }
        }
        self.unregister_reorg_storage_locs(&build_ids);
        if cleanup_error.is_none() {
            if let Err(err) = self.remove_partition_reorg_journal(&journal_path) {
                cleanup_error = Some(err);
            }
        }
        if committed {
            if let Some(err) = cleanup_error {
                log_message(format!(
                    "committed partition DDL cleanup pending: {}",
                    err.message()
                ));
            }
            return result;
        }
        match (result, cleanup_error) {
            (Err(err), _) | (Ok(_), Some(err)) => Err(err),
            (Ok(warnings), None) => Ok(warnings),
        }
    }
}

struct OnlineCopyReadyRows {
    rows: Vec<(u64, RowId)>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

struct RetainedPartitionCounts {
    stats: BTreeMap<u64, crate::storage::PartitionStats>,
    watermark: TxnView,
}

fn online_copy_scratch(data_dir: &Path) -> Result<File, QueryError> {
    let seq = NEXT_INDEX_SORT_TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let path = data_dir.join(format!(".alter-copy-log-{}-{seq}", std::process::id()));
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(io_err)?;
    fs::remove_file(path).map_err(io_err)?;
    Ok(file)
}

impl AlterCopyChangeLog {
    fn len(&self) -> usize {
        (self.spilled_rows as usize).saturating_add(self.touched_rows.len())
    }

    fn encode(entry: (u64, u64, RowId)) -> [u8; 24] {
        let mut bytes = [0; 24];
        bytes[..8].copy_from_slice(&entry.0.to_le_bytes());
        bytes[8..16].copy_from_slice(&entry.1.to_le_bytes());
        bytes[16..20].copy_from_slice(&entry.2.page_id.to_le_bytes());
        bytes[20..22].copy_from_slice(&entry.2.slot_id.to_le_bytes());
        bytes
    }

    fn record(&mut self, entry: (u64, u64, RowId)) -> Result<(), QueryError> {
        #[cfg(unix)]
        use std::os::unix::fs::FileExt;
        if self.invalidated || self.len() >= 64 * 1024 * 1024 / 24 {
            return Err(error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "online ALTER change log reached its 64 MiB limit",
            ));
        }
        if self.touched_rows.len() == self.touched_rows.capacity() {
            let extra = self.touched_rows.capacity().max(64);
            if self.touched_rows.capacity() < 4096
                && self
                    .memory_charge
                    .try_grow_optional(extra * std::mem::size_of::<(u64, u64, RowId)>())
            {
                self.touched_rows.try_reserve_exact(extra).map_err(|_| {
                    error(
                        crate::error::catalog::ER_OUTOFMEMORY,
                        "online ALTER log allocation failed",
                    )
                })?;
            } else {
                if self.spill.is_none() {
                    self.spill = Some(online_copy_scratch(&self.data_dir)?);
                }
                let file = self.spill.as_ref().unwrap();
                for chunk in self.touched_rows.chunks(128) {
                    let mut bytes = [0; 24 * 128];
                    for (i, entry) in chunk.iter().enumerate() {
                        bytes[i * 24..(i + 1) * 24].copy_from_slice(&Self::encode(*entry));
                    }
                    file.write_all_at(&bytes[..chunk.len() * 24], self.spilled_rows * 24)
                        .map_err(io_err)?;
                    self.spilled_rows += chunk.len() as u64;
                }
                self.touched_rows.clear();
                if self.touched_rows.capacity() == 0 {
                    file.write_all_at(&Self::encode(entry), self.spilled_rows * 24)
                        .map_err(io_err)?;
                    self.spilled_rows += 1;
                    return Ok(());
                }
            }
        }
        self.touched_rows.push(entry);
        Ok(())
    }

    fn visit(
        &self,
        mut f: impl FnMut((u64, u64, RowId)) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        #[cfg(unix)]
        use std::os::unix::fs::FileExt;
        if let Some(file) = &self.spill {
            for offset in (0..self.spilled_rows).step_by(128) {
                let count = (self.spilled_rows - offset).min(128) as usize;
                let mut bytes = [0; 24 * 128];
                file.read_exact_at(&mut bytes[..count * 24], offset * 24)
                    .map_err(io_err)?;
                for b in bytes[..count * 24].chunks_exact(24) {
                    f((
                        u64::from_le_bytes(b[..8].try_into().unwrap()),
                        u64::from_le_bytes(b[8..16].try_into().unwrap()),
                        RowId::new(
                            u32::from_le_bytes(b[16..20].try_into().unwrap()),
                            u16::from_le_bytes(b[20..22].try_into().unwrap()),
                        ),
                    ))?;
                }
            }
        }
        for entry in &self.touched_rows {
            f(*entry)?;
        }
        Ok(())
    }
}

type OnlineCopyTxnGuard<'a> = RollbackTxnGuard<'a, true>;

impl Database {
    fn build_online_copy_vectors(
        &self,
        table: &TableSchema,
        storage_id: u64,
    ) -> Result<(), QueryError> {
        for index in table.indexes.iter().filter(|index| index.is_vector()) {
            self.vector_graph_build_on_ddl_one(table, index, storage_id)?;
        }
        Ok(())
    }
    fn flush_online_copy_vectors(
        &self,
        table: &TableSchema,
        storage_id: u64,
    ) -> Result<(), QueryError> {
        for index in table.indexes.iter().filter(|index| index.is_vector()) {
            let graph = self
                .vector_indexes
                .read()
                .get(&(
                    table.id,
                    storage_id,
                    table.active_generation.max(1),
                    normalize_name(&index.name),
                ))
                .cloned();
            if let Some(graph) = graph {
                match graph.as_ref() {
                    VectorGraph::Paged(g) => g.file().flush()?,
                    VectorGraph::NativeHnsw(g) => {
                        if let Some(file) = g.file() {
                            file.flush()?;
                        }
                    }
                    VectorGraph::NativeIvf(g) => {
                        if let Some(file) = g.file() {
                            file.flush()?;
                        }
                    }
                    VectorGraph::Memory(_) => {}
                }
            }
        }
        Ok(())
    }
}

fn online_copy_deadline() -> Instant {
    let millis = std::env::var("MYDB_DDL_COPY_MAX_FINAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1000)
        .min(60000);
    Instant::now() + Duration::from_millis(millis)
}
fn check_online_copy_deadline(deadline: Option<Instant>) -> Result<(), QueryError> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(error(
            crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
            "online ALTER final tail exceeded its time budget; retry the ALTER",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod online_copy_log_tests {
    use super::*;
    #[test]
    fn disk_log_preserves_transaction_and_partition_identity() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = AlterCopyChangeLog::new(dir.path());
        for i in 0..20000u32 {
            log.record((
                i as u64 / 5,
                10 + i as u64 % 4,
                RowId::new(i / 30, (i % 30) as u16),
            ))
            .unwrap();
        }
        assert!(log.spilled_rows > 0);
        assert_eq!(log.len(), 20000);
        let mut n = 0u32;
        log.visit(|entry| {
            assert_eq!(
                entry,
                (
                    n as u64 / 5,
                    10 + n as u64 % 4,
                    RowId::new(n / 30, (n % 30) as u16)
                )
            );
            n += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(n, 20000);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}

#[cfg(test)]
mod online_copy_tail_reservation_tests {
    use super::*;
    #[test]
    fn final_tail_growth_is_reserved_before_the_barrier() {
        let dir = tempfile::tempdir().unwrap();
        let mut map = OnlineCopyRows::new(dir.path(), 2).unwrap();
        let location = crate::storage::DmlRowRef {
            storage_id: 10,
            row_id: RowId::new(0, 1),
        };
        for i in 0..104000 {
            map.insert(RowId::new(i / 512, (i % 512) as u16), location)
                .unwrap();
        }
        assert!(map.spill.is_none());
        map.prepare_final_tail().unwrap();
        assert!(
            map.spill.is_some(),
            "spill must happen before acquiring the final barrier"
        );
        for i in 104000..105024 {
            map.insert(RowId::new(i / 512, (i % 512) as u16), location)
                .unwrap();
        }
        assert_eq!(
            map.remove(&RowId::new(105023 / 512, (105023 % 512) as u16))
                .unwrap(),
            Some(location)
        );
    }
}

struct PartitionRewriteLayout {
    source_ids: Vec<u64>,
    build_ids: Vec<u64>,
    operation: String,
    timeout: Duration,
}

/// Statistics share the plan-cache revision with DDL, but are not a structural
/// change. Compare all persisted definition fields on the uncommon mismatch
/// path so new schema fields cannot accidentally bypass this safety check.
fn same_online_copy_definition(a: &TableSchema, b: &TableSchema) -> Result<bool, QueryError> {
    if a.id != b.id {
        return Ok(false);
    }
    if a.schema_version == b.schema_version {
        return Ok(true);
    }
    let normalize = |table: &TableSchema| -> Result<serde_json::Value, QueryError> {
        let mut value = serde_json::to_value(table).map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("cannot compare online ALTER definition: {err}"),
            )
        })?;
        let fields = value.as_object_mut().expect("table schema is an object");
        fields.remove("stats");
        fields.remove("schema_version");
        Ok(value)
    };
    Ok(normalize(a)? == normalize(b)?)
}

#[cfg(test)]
mod table_ddl_audit_storage_tests {
    use super::*;

    fn fixture() -> (tempfile::TempDir, crate::engine::StorageQueryEngine) {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::Builder::new()
            .prefix("ddl-audit-1012-storage-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let mut config = StorageConfig::from_env();
        config.durability = DurabilityMode::Relaxed;
        config.undo_retention = Duration::ZERO;
        let engine =
            crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                config,
            )
            .unwrap();
        (dir, engine)
    }

    #[test]
    fn table_ddl_audit_copy_accepts_stats_revision_but_rejects_structural_change() {
        let (_dir, engine) = fixture();
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
        let db = engine.database();
        let source = db.table("test.t").unwrap();
        assert!(db.refresh_table_stats("test.t", false).unwrap());
        let actual = db.table("test.t").unwrap();
        assert!(actual.schema_version > source.schema_version);
        let mut target = source.as_ref().clone();
        target.bump_schema_version();
        db.run_online_type_copy(
            "test.t",
            source.clone(),
            target,
            true,
            9301,
            crate::parser::AlterLock::None,
        )
        .unwrap();
        assert!(db.table("test.t").unwrap().schema_version > actual.schema_version);
        assert!(!db.catalog.online_pinned_ids.read().contains(&source.id));
        assert!(db.refresh_table_stats("test.t", false).unwrap());
        let source = db.table("test.t").unwrap();
        engine.execute("ALTER TABLE t ADD c INT").unwrap();
        let mut target = source.as_ref().clone();
        target.bump_schema_version();
        assert_eq!(
            db.run_online_type_copy(
                "test.t",
                source.clone(),
                target,
                true,
                9302,
                crate::parser::AlterLock::None
            )
            .unwrap_err()
            .code(),
            1412
        );
        assert!(!db.catalog.online_pinned_ids.read().contains(&source.id));
    }

    #[test]
    fn table_ddl_audit_check_detects_corrupt_secondary_partition_page() {
        #[cfg(unix)]
        use std::os::unix::fs::FileExt;
        let (_dir, engine) = fixture();
        engine.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,KEY vi(v)) PARTITION BY HASH(id) PARTITIONS 2").unwrap();
        engine.execute("INSERT INTO t VALUES(1,10),(2,20)").unwrap();
        let db = engine.database();
        let schema = db.table("test.t").unwrap();
        let check = || {
            let crate::engine::QueryResponse::ResultSet(result) = engine
                .execute("CHECK TABLE t")
                .unwrap()
                .materialize()
                .unwrap()
            else {
                panic!("expected rows")
            };
            result.rows
        };
        assert_eq!(check()[0][2], Value::String("status".into()));
        db.flush_dirty_data().unwrap();
        let sid = schema.storage_ids()[1];
        let path = db.index_path(sid, "vi");
        db.remove_index_cache_entry(sid, "vi").unwrap();
        let file = OpenOptions::new().write(true).open(path).unwrap();
        file.write_all_at(b"BAD!", PAGE_SIZE as u64).unwrap();
        file.sync_all().unwrap();
        let result = check();
        assert!(
            result
                .iter()
                .any(|row| row[2] == Value::String("error".into())
                    && row[3].to_display_string().contains("vi")),
            "{result:?}"
        );
        assert!(
            !result
                .iter()
                .any(|row| row[3] == Value::String("OK".into())),
            "{result:?}"
        );
    }

    #[test]
    fn table_ddl_audit_projected_physical_hole_does_not_replace_decoded_value() {
        let (_dir, engine) = fixture();
        for sql in [
            "CREATE TABLE t(id INT PRIMARY KEY,a INT,b INT)",
            "ALTER TABLE t DROP a",
            "ALTER TABLE t ADD c INT DEFAULT 9",
        ] {
            engine.execute(sql).unwrap();
        }
        let schema = engine.database().table("test.t").unwrap();
        let c = schema.columns.iter().position(|c| c.name == "c").unwrap();
        let plan = ProjectedReadPlan::new(&[c], &schema);
        let mut values = [Value::Int(99)];
        plan.apply(&mut values, &[c], 3, &schema);
        assert_eq!(values, [Value::Int(99)]);
        let mut values = [Value::Null];
        plan.apply(&mut values, &[c], 3, &schema);
        assert_eq!(values, [Value::Null]);
    }
}
