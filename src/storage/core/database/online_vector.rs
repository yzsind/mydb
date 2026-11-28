// Private ANN candidates share the online registry's source barriers. The
// initial image is logged outside writer exclusion; subsequent replacements
// and Public catalog rows belong to one DDL transaction. Restart discards
// undecided Building intents and replays only decided candidate journals.
struct OnlineVectorWalGuard<'a> {
    db: &'a Database,
    txn_id: u64,
}
impl<'a> OnlineVectorWalGuard<'a> {
    fn begin(db: &'a Database, txn_id: u64) -> Self {
        db.vector_ddl_publications.lock().insert(txn_id);
        db.vector_pending_wal
            .lock()
            .insert(txn_id, db.log_state_atomics.next_lsn());
        Self { db, txn_id }
    }
}
impl Drop for OnlineVectorWalGuard<'_> {
    fn drop(&mut self) {
        self.db.vector_ddl_publications.lock().remove(&self.txn_id);
        self.db.vector_pending_wal.lock().remove(&self.txn_id);
    }
}

struct PrivateVectorIndexBuild<'a> {
    db: &'a Database,
    table: Arc<TableSchema>,
    index: IndexSchema,
    storage_id: u64,
    txn_id: u64,
    build_epoch: u64,
    registration: VectorBuildRegistration<'a>,
    graph: Option<Arc<VectorGraph>>,
    published: bool,
    dirty: bool,
    pages_dirty: bool,
}
impl<'a> PrivateVectorIndexBuild<'a> {
    fn new(
        db: &'a Database,
        table: Arc<TableSchema>,
        index: IndexSchema,
        storage_id: u64,
        txn_id: u64,
    ) -> Self {
        let registration = VectorBuildRegistration::begin(
            db,
            (
                table.id,
                storage_id,
                table.active_generation.max(1),
                normalize_name(&index.name),
            ),
        );
        Self {
            db,
            table,
            index,
            storage_id,
            txn_id,
            build_epoch: db.vector_build_epoch.load(Ordering::Acquire),
            registration,
            graph: None,
            published: false,
            dirty: false,
            pages_dirty: true,
        }
    }

    fn build(&mut self) -> Result<(), QueryError> {
        let graph =
            self.db
                .vector_graph_load_or_build(&self.table, &self.index, self.storage_id, true)?;
        self.graph = Some(Arc::new(graph));
        // Persist the base before journaling replacements. Resident recovery
        // starts here; paged replacements also have ordinary checked page WAL.
        self.flush()?;
        let graph = self.graph.as_ref().unwrap();
        self.db.persist_paged_retirement_snapshot(
            &self.table,
            &self.index,
            self.storage_id,
            graph,
        )?;
        let covered = self.db.log_state_atomics.next_lsn().saturating_sub(1);
        let prepared = self
            .db
            .vector_graph_persist_one(
                &self.table,
                &self.index.name,
                self.storage_id,
                graph,
                covered,
                false,
            )
            .map_err(io_err)?;
        self.db.publish_native_vector_generation(
            &self.table,
            &self.index,
            self.storage_id,
            graph,
            None,
            prepared,
        )?;
        graph.prepare_query_navigation()?;
        Ok(())
    }

    fn pending(&self) -> Result<(usize, usize), QueryError> {
        let deltas = self.db.vector_build_deltas.lock();
        let Some(delta) = deltas.get(&self.registration.key) else {
            return Ok((0, 0));
        };
        if delta.failed {
            return Err(error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "vector build delta exceeded its memory admission",
            ));
        }
        // The private consumer takes whole batches. Only the current tail
        // remains here, so publication admission is constant-time.
        Ok((
            delta.ops.len(),
            delta.charge.as_ref().map_or(0, |charge| charge.bytes()),
        ))
    }

    fn replay(&mut self) -> Result<(), QueryError> {
        let batch = self.registration.take_private()?;
        if !batch.ops.is_empty() {
            self.dirty = true;
            self.pages_dirty = true;
        }
        let graph = self.graph.as_ref().unwrap();
        let mut entries = Vec::new();
        let mut size = 0usize;
        let cosine =
            self.index.vector_meta_for_table(&self.table).metric_enum() == vector::Metric::Cosine;
        for (&row_id, (_, value)) in &batch.ops {
            crate::engine::cancel::check_cancelled()?;
            if cosine && value.as_ref().is_some_and(vector::distance::typed_is_zero) {
                return Err(vector::vector_err(
                    "COSINE indexes reject existing zero vectors",
                ));
            }
            let bytes = value.as_ref().map_or(64, |v| v.len().saturating_add(64));
            if size.saturating_add(bytes) > 1024 * 1024 {
                self.log_batch(&mut entries)?;
                size = 0;
            }
            entries.push(VectorRecoveryMutation {
                table_id: self.table.id,
                storage_id: self.storage_id,
                generation: self.table.active_generation.max(1),
                index_id: self.index.index_id,
                row_id,
                value: value.clone(),
            });
            size = size.saturating_add(bytes);
            graph.replace_row(row_id, value.clone())?;
        }
        self.log_batch(&mut entries)?;
        Ok(())
    }

    fn log_batch(&self, entries: &mut Vec<VectorRecoveryMutation>) -> Result<(), QueryError> {
        if !entries.is_empty() && self.db.durability_mode() != DurabilityMode::Memory {
            let bytes = VectorRecoveryMutation::encode_batch(entries)?;
            self.db
                .append_wal(self.txn_id, WalPayload::VectorIndexBatch { bytes })?;
        }
        entries.clear();
        Ok(())
    }

    fn flush(&mut self) -> Result<(), QueryError> {
        let graph = self.graph.as_ref().unwrap();
        self.db.finish_pending_vector_training(
            &self.table,
            &self.index,
            self.storage_id,
            graph,
            self.txn_id,
            self.db
                .next_scn_atomic
                .load(Ordering::Acquire)
                .saturating_sub(1),
        )?;
        let refresh = self.pages_dirty && self.dirty;
        self.flush_pages()?;
        if refresh {
            self.graph.as_ref().unwrap().prepare_query_navigation()?;
        }
        Ok(())
    }

    fn flush_pages(&mut self) -> Result<(), QueryError> {
        if !self.pages_dirty {
            return Ok(());
        }
        let graph = self.graph.as_ref().unwrap();
        graph.flush_paged_ivf()?;
        match graph.as_ref() {
            VectorGraph::Paged(paged) => {
                paged.file().advance_coverage(self.db.visible_scn());
                paged.file().mark_delta_finalized(0)?;
                paged.file().flush()?;
            }
            VectorGraph::NativeHnsw(native) => {
                if let Some(file) = native.file() {
                    file.advance_coverage(self.db.visible_scn());
                    file.mark_delta_finalized(0)?;
                    file.flush()?;
                }
            }
            VectorGraph::NativeIvf(native) => {
                if let Some(file) = native.file() {
                    file.advance_coverage(self.db.visible_scn());
                    file.mark_delta_finalized(0)?;
                    file.flush()?;
                }
            }
            _ => {}
        }
        self.pages_dirty = false;
        Ok(())
    }

    fn needs_model_training(&self) -> Result<bool, QueryError> {
        let graph = self.graph.as_ref().unwrap();
        let pending = match graph.as_ref() {
            VectorGraph::NativeHnsw(native) => native.quantization_pending(),
            VectorGraph::NativeIvf(native) => native.quantization_pending(),
            _ => false,
        };
        if !pending {
            return Ok(false);
        }
        let minimum = match &self.index.vector.as_ref().unwrap().quantization {
            crate::storage::vector_config::QuantizationConfig::Pq { bits, .. } => 1usize << bits,
            _ => 1,
        };
        let deltas = self.db.vector_build_deltas.lock();
        let delta = deltas
            .get(&self.registration.key)
            .ok_or_else(vector::persistence::corrupt)?;
        if delta.ops.is_empty() {
            return Ok(false);
        }
        let mut rows = graph.len();
        for (&row_id, (_, value)) in &delta.ops {
            match (graph.has_live_row_id(row_id)?, value.is_some()) {
                (true, false) => rows = rows.saturating_sub(1),
                (false, true) => rows = rows.saturating_add(1),
                _ => {}
            }
        }
        Ok(rows >= minimum)
    }

    fn publish(
        &mut self,
        map: &mut HashMap<(u64, u64, u64, String), Arc<VectorGraph>>,
    ) -> Result<(), QueryError> {
        let graph = self.graph.as_ref().unwrap();
        self.db.ensure_operational()?;
        if self.db.vector_build_epoch.load(Ordering::Acquire) != self.build_epoch
            || self.db.table_locks_sharded.is_retired(self.table.id)
        {
            return Err(error(
                crate::error::catalog::ER_TABLE_DEF_CHANGED,
                "vector build invalidated by concurrent state change",
            ));
        }
        graph.ensure_valid()?;
        // The caller holds the graph map. Worker registration/revocation
        // therefore cannot miss this handle when synchronizing capabilities.
        if let VectorGraph::Memory(memory) = graph.as_ref() {
            memory.set_background_quant(crate::engine::vector_maintenance::is_active(self.db));
        }
        if self.dirty {
            self.db.note_vector_snapshot_change(graph);
        }
        self.db
            .vector_table_snapshot_floor(self.table.id)
            .fetch_max(
                self.db.next_scn_atomic.load(Ordering::Acquire),
                Ordering::AcqRel,
            );
        map.insert(self.registration.key.clone(), graph.clone());
        self.published = true;
        Ok(())
    }
}
impl Drop for PrivateVectorIndexBuild<'_> {
    fn drop(&mut self) {
        if !self.published {
            if let Some(graph) = &self.graph {
                graph.invalidate();
            }
        }
    }
}

#[cfg(test)]
impl Database {
    pub(crate) fn exhaust_vector_build_log_for_test(&self) {
        for delta in self.vector_build_deltas.lock().values_mut() {
            delta.failed = true;
        }
    }
}

impl VectorBuildRegistration<'_> {
    fn take_private(&self) -> Result<VectorBuildDelta, QueryError> {
        let mut deltas = self.db.vector_build_deltas.lock();
        let delta = deltas
            .get_mut(&self.key)
            .ok_or_else(vector::persistence::corrupt)?;
        if delta.builders != 1 {
            return Err(vector::persistence::corrupt());
        }
        if delta.failed {
            return Err(error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "vector build delta exceeded its memory admission",
            ));
        }
        let batch = std::mem::take(delta);
        delta.builders = batch.builders;
        delta.next_seq = batch.next_seq;
        Ok(batch)
    }
}

impl Database {
    fn vector_registry_requires_cosine_validation(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
    ) -> bool {
        if !index.is_vector()
            || index.vector_meta_for_table(table).metric_enum() != vector::Metric::Cosine
        {
            return false;
        }
        let handle = self
            .index_cache
            .read()
            .get(&(table.id, normalize_name(&index.name)))
            .cloned();
        // A writable candidate validates its visible cohort and decided tail.
        !handle.is_some_and(|handle| handle.read().online_build_log.is_some())
    }
}

#[cfg(test)]
static ONLINE_VECTOR_MODEL_RETRIES: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
impl Database {
    pub(crate) fn vector_model_retries_for_test(&self) -> u64 {
        ONLINE_VECTOR_MODEL_RETRIES.load(Ordering::Relaxed)
    }
}
