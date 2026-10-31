// Sequence after-states deliberately bypass user undo. Only their checkpoint
// materialization uses ordinary, atomic DD transactions.

fn sequence_state_error(message: impl Into<String>) -> QueryError {
    error(crate::error::catalog::ER_GET_ERRNO, message)
}

#[cfg(test)]
thread_local! {
    static SEQUENCE_CHECKPOINT_TEST_HOOK: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
}

#[derive(Debug, Default)]
struct SequenceMetrics {
    wal_records: AtomicU64,
    checkpoint_rows: AtomicU64,
    local_sync_us: AtomicU64,
    remote_sync_us: AtomicU64,
    remote_failures: AtomicU64,
}

#[derive(Debug)]
struct SequenceDirtyQueue {
    epoch: u64,
    entries: Vec<Arc<SequenceRuntime>>,
    charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl SequenceDirtyQueue {
    fn new() -> Result<Self, QueryError> {
        Ok(Self {
            epoch: 1,
            entries: Vec::new(),
            charge: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                0,
            )?,
        })
    }

    fn enqueue(&mut self, state: Arc<SequenceRuntime>) -> Result<(), QueryError> {
        if state.queued_epoch.load(Ordering::Acquire) == self.epoch {
            return Ok(());
        }
        if self.entries.len() == self.entries.capacity() {
            let old = self.entries.capacity();
            let target = old.saturating_mul(2).max(16);
            let extra = (target - old) * std::mem::size_of::<Arc<SequenceRuntime>>();
            self.charge.try_grow(extra)?;
            if self.entries.try_reserve_exact(target - old).is_err() {
                self.charge.shrink(extra);
                return Err(error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    "sequence dirty queue allocation failed",
                ));
            }
        }
        state.queued_epoch.store(self.epoch, Ordering::Release);
        self.entries.push(state);
        Ok(())
    }

    fn snapshot(&self) -> Result<Self, QueryError> {
        let mut snapshot = Self::new()?;
        snapshot
            .charge
            .try_grow(self.entries.len() * std::mem::size_of::<Arc<SequenceRuntime>>())?;
        snapshot
            .entries
            .try_reserve_exact(self.entries.len())
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_OUTOFMEMORY,
                    "sequence checkpoint snapshot allocation failed",
                )
            })?;
        snapshot.entries.extend(self.entries.iter().cloned());
        Ok(snapshot)
    }
}

impl Drop for CatalogTxn<'_> {
    fn drop(&mut self) {
        if self.sequence_changes.is_empty() {
            return;
        }
        if let Ok(mut catalog) = self.db.catalog_write() {
            for (old, _) in &self.sequence_changes {
                catalog
                    .sequences
                    .insert(table_key(&old.database, &old.name), old.clone());
            }
        } else {
            self.db
                .quarantine_mvcc("sequence dependency rollback failed");
        }
    }
}

impl CatalogTxn<'_> {
    fn stage_sequence_dependencies(
        &mut self,
        mutations: &mut Vec<CatalogMutation>,
    ) -> Result<(), QueryError> {
        let tables: Vec<_> = mutations
            .iter()
            .filter_map(|m| match m {
                CatalogMutation::Table(id) => Some(*id),
                _ => None,
            })
            .collect();
        if tables.is_empty() || !self.db.sequences.has_owned_sequences() {
            return Ok(());
        }
        let mut objects = Vec::new();
        for id in tables {
            let live = self
                .db
                .catalog_read()?
                .tables
                .values()
                .find(|table| table.id == id)
                .map(|table| table.object_id);
            let object = match live {
                Some(id) => Some(id),
                None => self
                    .db
                    .catalog_rows_for_selector(
                        crate::storage::data_dictionary::DD_TABLES,
                        CatalogRowSelector::PrimaryValues(vec![Value::UInt(id)]),
                    )?
                    .first()
                    .map(|row| {
                        crate::storage::data_dictionary::table_header_from_dd_row(row)
                            .map(|table| table.object_id)
                    })
                    .transpose()?,
            };
            if let Some(id) = object {
                objects.push(id);
            }
        }
        let tables = objects;
        for def in self.db.sequences.owned_definitions(&tables) {
            let owner = crate::storage::mdl::allocate_internal_owner()?;
            let lease = SequenceValueLease {
                db: self.db,
                owner: Some(owner),
                fast: None,
            };
            self.db.acquire_mdl(
                owner,
                None,
                MdlDuration::Statement,
                vec![MdlRequest {
                    key: MdlObjectKey::table(&def.database, &def.name),
                    lock_type: MdlLockType::Exclusive,
                }],
                "sql/sequence-dependency",
            )?;
            let current = self
                .db
                .catalog_read()?
                .sequences
                .get(&table_key(&def.database, &def.name))
                .cloned();
            let Some(current) = current else {
                continue;
            };
            if !current
                .owned_table_id
                .is_some_and(|id| tables.contains(&id))
            {
                continue;
            }
            let table = self
                .db
                .catalog_read()?
                .tables
                .values()
                .find(|table| Some(table.object_id) == current.owned_table_id)
                .cloned();
            let column = table.as_ref().and_then(|table| {
                table.columns.iter().find(|column| {
                    Some(column.column_id) == current.owned_column_id
                        && column.state != ColumnState::Dropped
                })
            });
            let replacement = match (&table, column) {
                (Some(table), Some(column)) => {
                    if !table.database.eq_ignore_ascii_case(&current.database) {
                        return Err(sequence_state_error("moving a table with owned sequences to another database is not supported"));
                    }
                    if current.owned_table.as_deref() == Some(table.name.as_str())
                        && current.owned_column.as_deref() == Some(column.name.as_str())
                    {
                        continue;
                    }
                    let mut candidate = current.as_ref().clone();
                    candidate.owned_table = Some(table.name.clone());
                    candidate.owned_column = Some(column.name.clone());
                    Some(Arc::new(candidate))
                }
                _ => None,
            };
            self.sequence_changes
                .push((current.clone(), replacement.clone()));
            self.sequence_leases.push(lease);
            let key = table_key(&current.database, &current.name);
            match replacement {
                Some(def) => {
                    self.db.catalog_write()?.sequences.insert(key, def);
                }
                None => {
                    self.db.catalog_write()?.sequences.remove(&key);
                    mutations.push(CatalogMutation::SequenceValue(current.counter_id, None));
                    mutations.push(CatalogMutation::SequenceRetirement(
                        current.counter_id,
                        current.id,
                    ));
                }
            }
            mutations.push(CatalogMutation::Sequence(current.id));
        }
        Ok(())
    }
}

impl Database {
    pub(crate) fn sequence_status_pairs(&self) -> [(&'static str, u64); 6] {
        [
            (
                "Mydb_sequence_wal_records",
                self.sequence_metrics.wal_records.load(Ordering::Relaxed),
            ),
            (
                "Mydb_sequence_checkpoint_rows",
                self.sequence_metrics
                    .checkpoint_rows
                    .load(Ordering::Relaxed),
            ),
            (
                "Mydb_sequence_local_sync_us",
                self.sequence_metrics.local_sync_us.load(Ordering::Relaxed),
            ),
            (
                "Mydb_sequence_remote_sync_us",
                self.sequence_metrics.remote_sync_us.load(Ordering::Relaxed),
            ),
            (
                "Mydb_sequence_remote_failures",
                self.sequence_metrics
                    .remote_failures
                    .load(Ordering::Relaxed),
            ),
            (
                "Mydb_sequence_dirty_generations",
                self.sequence_dirty.lock().entries.len() as u64,
            ),
        ]
    }

    pub(crate) fn sequence_definition_by_id(
        &self,
        id: u64,
    ) -> Result<Option<Arc<SequenceSchema>>, QueryError> {
        Ok(self.sequences.get(id).map(|runtime| runtime.definition()))
    }

    pub(crate) fn acquire_sequence_write_lease(
        &self,
    ) -> Result<SequenceWriteLease<'_>, QueryError> {
        self.sequences.active_writes.fetch_add(1, Ordering::SeqCst);
        let lease = SequenceWriteLease(self);
        self.ensure_operational()?;
        self.ensure_writable()?;
        if self.physical_replica_mode() {
            return Err(error(
                crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
                "physical replica sequences are read-only",
            ));
        }
        crate::repl::ensure_sequence_write_allowed()?;
        self.capture.ensure_write()?;
        Ok(lease)
    }

    pub(crate) fn sequence_diagnostic_row(
        &self,
        def: &Arc<SequenceSchema>,
    ) -> Result<Vec<Value>, QueryError> {
        let rt = self.ensure_sequence_runtime(def)?;
        let (allocated, pending) = rt.status();
        let logged = *rt.logged.lock();
        let checkpointed = self
            .read_sequence_value(def.counter_id)?
            .map(|v| v.state_lsn)
            .unwrap_or(0);
        let sync = self.sequence_sync.read();
        Ok(vec![
            Value::String(def.database.clone()),
            Value::String(def.name.clone()),
            Value::String(def.as_type.as_str().into()),
            Value::Int(def.increment),
            Value::Int(def.min_value),
            Value::Int(def.max_value),
            Value::Int(def.cache),
            Value::Int(i64::from(def.cycle)),
            Value::UInt(def.counter_id),
            if self.physical_replica_mode() {
                Value::Null
            } else {
                allocated.map(Value::Int).unwrap_or(Value::Null)
            },
            Value::Int(logged.resume_value),
            Value::Int(i64::from(logged.is_called)),
            Value::UInt(logged.state_lsn),
            Value::UInt(checkpointed),
            Value::UInt(logged.state_lsn.min(self.log.lock().durable_lsn())),
            Value::String(
                if sync.remote_apply {
                    "remote_apply"
                } else {
                    "local"
                }
                .into(),
            ),
            Value::Int(i64::from(pending)),
            Value::UInt(sync.epoch),
        ])
    }

    fn note_catalog_sequence_value(&self, counter_id: u64, value: Option<SequenceValue>) {
        self.note_catalog_mutation(CatalogMutation::SequenceValue(counter_id, value));
    }

    fn persist_sequence_checkpoint(
        &self,
        states: &[Arc<SequenceRuntime>],
    ) -> Result<(), QueryError> {
        use crate::storage::data_dictionary::DD_SEQUENCE_VALUES;
        if states.is_empty() || self.config.durability == DurabilityMode::Memory {
            return Ok(());
        }
        // Resolve identities before taking object leases; never acquire an MDL
        // or perform catalog I/O while the publication fence is held.
        for chunk in states.chunks(256) {
            let mut leases = Vec::new();
            let mut txn = CatalogTxn::new(self);
            for rt in chunk {
                loop {
                    if rt.retired.load(Ordering::Acquire) {
                        break;
                    }
                    crate::engine::cancel::check_cancelled()?;
                    let def = rt.definition();
                    let lease = self.acquire_sequence_value_lease(&def.database, &def.name, 0)?;
                    let active = self
                        .catalog_read()?
                        .sequences
                        .get(&table_key(&def.database, &def.name))
                        .cloned();
                    if rt.retired.load(Ordering::Acquire) {
                        break;
                    }
                    if active
                        .as_ref()
                        .is_none_or(|d| d.counter_id != rt.counter_id)
                    {
                        drop(lease);
                        let current = rt.definition();
                        if current.name != def.name || current.database != def.database {
                            continue;
                        }
                        return Err(sequence_state_error(
                            "sequence definition changed during checkpoint; retry required",
                        ));
                    }
                    let value = *rt.logged.lock();
                    if value.state_lsn != 0
                        && !self
                            .read_sequence_value(value.counter_id)?
                            .is_some_and(|v| v.state_lsn > value.state_lsn)
                    {
                        txn.replace(
                            DD_SEQUENCE_VALUES,
                            CatalogRowSelector::PrimaryValues(vec![Value::UInt(value.counter_id)]),
                            vec![value.to_row()],
                        );
                    }
                    leases.push(lease);
                    break;
                }
            }
            if !txn.replacements.is_empty() {
                let count = txn.replacements.len();
                txn.commit()?;
                self.sequence_metrics
                    .checkpoint_rows
                    .fetch_add(count as u64, Ordering::Relaxed);
            }
            drop(leases);
        }
        Ok(())
    }

    fn sequence_is_retired(&self, value: SequenceValue) -> Result<bool, QueryError> {
        let rows = self.catalog_rows_for_selector(
            crate::storage::data_dictionary::DD_SEQUENCE_RETIREMENTS,
            CatalogRowSelector::PrimaryValues(vec![Value::UInt(value.counter_id)]),
        )?;
        let Some(row) = rows.first() else {
            return Ok(false);
        };
        let unsigned = |index| match row.get(index) {
            Some(Value::UInt(v)) => Some(*v),
            Some(Value::Int(v)) if *v >= 0 => Some(*v as u64),
            _ => None,
        };
        if rows.len() != 1
            || row.len() != 3
            || unsigned(0) != Some(value.counter_id)
            || unsigned(1) != Some(value.sequence_id)
        {
            return Err(sequence_state_error("invalid sequence retirement proof"));
        }
        Ok(unsigned(2).is_some_and(|lsn| lsn >= value.state_lsn))
    }

    fn purge_sequence_retirements(&self) -> Result<(), QueryError> {
        let covered = self.sequence_covered_lsn.load(Ordering::Acquire);
        if covered == 0 {
            return Ok(());
        }
        let id = crate::storage::data_dictionary::DD_SEQUENCE_RETIREMENTS;
        let bytes =
            self.with_heap_read(id, |heap| Ok(heap.file.metadata().map_err(io_err)?.len()))?;
        let _charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
            usize::try_from(bytes)
                .unwrap_or(usize::MAX)
                .saturating_mul(4),
        )?;
        let rows = self.scan_dd_table(id)?;
        for chunk in rows.chunks(256) {
            let mut txn = CatalogTxn::new(self);
            for row in chunk {
                let unsigned = |index| match row.get(index) {
                    Some(Value::UInt(v)) => Some(*v),
                    Some(Value::Int(v)) if *v >= 0 => Some(*v as u64),
                    _ => None,
                };
                let counter = unsigned(0)
                    .ok_or_else(|| sequence_state_error("invalid retired sequence identity"))?;
                let lsn = unsigned(2)
                    .ok_or_else(|| sequence_state_error("invalid sequence retirement LSN"))?;
                if row.len() != 3 || counter == 0 {
                    return Err(sequence_state_error("invalid sequence retirement row"));
                }
                if lsn < covered {
                    txn.replace(
                        id,
                        CatalogRowSelector::PrimaryValues(vec![Value::UInt(counter)]),
                        Vec::new(),
                    );
                }
            }
            if !txn.replacements.is_empty() {
                txn.commit()?;
            }
        }
        Ok(())
    }

    fn recover_sequence_values(&self, values: RecoveredSequenceValues) -> Result<(), QueryError> {
        for value in values.values.into_values() {
            let rt = self.sequences.get(value.sequence_id);
            if rt
                .as_ref()
                .is_none_or(|rt| rt.counter_id != value.counter_id)
            {
                if self.sequence_is_retired(value)? {
                    continue;
                }
                return Err(sequence_state_error(
                    "unexplained sequence generation in recovery WAL",
                ));
            }
            let rt = rt.expect("validated sequence runtime");
            rt.apply_value(value)?;
            self.sequence_dirty.lock().enqueue(rt)?;
        }
        self.sequence_dirty_generations
            .store(self.sequence_dirty.lock().entries.len(), Ordering::Release);
        Ok(())
    }

    fn apply_sequence_value(&self, value: SequenceValue) -> Result<(), QueryError> {
        let rt = self.sequences.get(value.sequence_id);
        if rt
            .as_ref()
            .is_none_or(|rt| rt.counter_id != value.counter_id)
        {
            if self.sequence_is_retired(value)? {
                return Ok(());
            }
            return Err(sequence_state_error(
                "sequence WAL references an unexplained generation",
            ));
        }
        let rt = rt.expect("validated sequence runtime");
        {
            let mut dirty = self.sequence_dirty.lock();
            dirty.enqueue(rt.clone())?;
            self.sequence_dirty_generations
                .store(dirty.entries.len(), Ordering::Release);
        }
        rt.apply_value(value)
    }

    pub(crate) fn prepare_sequence_promotion(&self) -> Result<(), QueryError> {
        self.initialize_sequence_runtimes()?;
        self.sequences.reset_for_promotion();
        self.sequences.role_epoch.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub(crate) fn sequence_role_epoch(&self) -> u64 {
        self.sequences.role_epoch.load(Ordering::Acquire)
    }

    pub(crate) fn sequence_sync_config(&self) -> crate::storage::sequence::SequenceSyncConfig {
        self.sequence_sync.read().clone()
    }

    pub(crate) fn store_sequence_sync_config(
        &self,
        config: crate::storage::sequence::SequenceSyncConfig,
    ) -> Result<u64, QueryError> {
        use crate::storage::data_dictionary::DD_DD_PROPERTIES;
        config.validate()?;
        self.ensure_writable()?;
        if config.remote_apply && self.config.durability == DurabilityMode::Memory {
            return Err(sequence_state_error(
                "remote_apply sequences require disk durability",
            ));
        }
        let mut txn = CatalogTxn::new(self);
        txn.set_delta(Vec::new(), true);
        txn.replace(
            DD_DD_PROPERTIES,
            CatalogRowSelector::PrimaryValues(vec![Value::String("sequence_sync_config".into())]),
            vec![vec![
                Value::String("sequence_sync_config".into()),
                Value::String(
                    serde_json::to_string(&config)
                        .map_err(|e| sequence_state_error(e.to_string()))?,
                ),
            ]],
        );
        txn.commit()?;
        *self.sequence_sync.write() = config;
        let target = self.log.lock().next_lsn().saturating_sub(1);
        LogManager::sync_durable_to(&self.log, target)?;
        Ok(target)
    }

    fn initialize_sequence_sync(&self) -> Result<(), QueryError> {
        use crate::storage::data_dictionary::DD_DD_PROPERTIES;
        use crate::storage::sequence::SequenceSyncConfig;
        let requested = SequenceSyncConfig::from_env()?;
        let rows = self.catalog_rows_for_selector(
            DD_DD_PROPERTIES,
            CatalogRowSelector::PrimaryValues(vec![Value::String("sequence_sync_config".into())]),
        )?;
        let missing = rows.is_empty();
        let config = if let Some(row) = rows.first() {
            let json = row
                .get(1)
                .and_then(crate::types::value_str)
                .ok_or_else(|| sequence_state_error("invalid sequence sync catalog property"))?;
            let stored: SequenceSyncConfig = serde_json::from_str(json)
                .map_err(|e| sequence_state_error(format!("invalid sequence sync config: {e}")))?;
            if let Some(requested) = requested {
                if (requested.remote_apply != stored.remote_apply
                    || requested.replicas != stored.replicas)
                    && !self.physical_replica_mode()
                    && !self.is_snapshot_read_only()
                {
                    return Err(sequence_state_error("sequence sync environment differs from catalog; remove stale bootstrap settings or change it while master writes are fenced"));
                }
            }
            stored
        } else {
            requested.unwrap_or_default()
        };
        config.validate()?;
        if config.remote_apply && self.config.durability == DurabilityMode::Memory {
            return Err(sequence_state_error(
                "remote_apply sequences require disk durability",
            ));
        }
        if config.remote_apply && !self.physical_replica_mode() && !self.is_snapshot_read_only() {
            let role = crate::repl::role::load(&self.data_dir)
                .map_err(io_err)?
                .map(|s| s.role)
                .or_else(|| {
                    crate::util::env::var("MYDB_ROLE")
                        .ok()
                        .and_then(|s| crate::repl::role::Role::parse(&s))
                });
            if role != Some(crate::repl::role::Role::Master) {
                return Err(sequence_state_error(
                    "remote_apply sequences require a native master role",
                ));
            }
        }
        if missing && !self.physical_replica_mode() && !self.is_snapshot_read_only() {
            self.store_sequence_sync_config(config)?;
        } else {
            *self.sequence_sync.write() = config;
        }
        Ok(())
    }

    fn wait_sequence_remote(&self, lsn: u64) -> Result<(), QueryError> {
        let config = self.sequence_sync.read().clone();
        if config.remote_apply {
            crate::repl::wait_for_sequence_durable(&config.replicas, lsn)?;
        }
        Ok(())
    }

    fn resolve_sequence_ownership(&self, def: &mut SequenceSchema) -> Result<(), QueryError> {
        if def.owned_table.is_some() != def.owned_column.is_some() {
            return Err(sequence_state_error(
                "OWNED BY requires both a table and a column",
            ));
        }
        let (Some(table), Some(column)) = (&def.owned_table, &def.owned_column) else {
            def.owned_table_id = None;
            def.owned_column_id = None;
            return Ok(());
        };
        let (database, table) = split_table_name(&def.database, table);
        if !database.eq_ignore_ascii_case(&def.database) {
            return Err(sequence_state_error(
                "OWNED BY must reference a table in the same database",
            ));
        }
        let table = self.table(&format!("{database}.{table}"))?;
        let column = table
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(column) && c.state != ColumnState::Dropped)
            .ok_or_else(|| sequence_state_error("OWNED BY references an unknown column"))?;
        def.owned_table_id = Some(table.object_id);
        def.owned_column_id = Some(column.column_id);
        Ok(())
    }
}

pub(crate) struct SequenceWriteLease<'a>(&'a Database);
impl Drop for SequenceWriteLease<'_> {
    fn drop(&mut self) {
        self.0
            .sequences
            .active_writes
            .fetch_sub(1, Ordering::SeqCst);
    }
}

fn sequence_coverage_source(
    records: &(impl crate::wal::WalRecordSource + ?Sized),
) -> Result<u64, QueryError> {
    let mut covered = 0;
    for record in records
        .records()
        .only(&[crate::wal::WalRecordType::CheckpointEnd])
    {
        if let WalPayload::CheckpointEnd {
            sequence_covered_lsn,
            ..
        } = record?.payload
        {
            covered = sequence_covered_lsn;
        }
    }
    Ok(covered)
}

struct RecoveredSequenceValues {
    values: BTreeMap<u64, SequenceValue>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

fn collect_sequence_values_source(
    records: &(impl crate::wal::WalRecordSource + ?Sized),
) -> Result<RecoveredSequenceValues, QueryError> {
    let mut charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
        0,
    )?;
    let covered = sequence_coverage_source(records)?;
    let mut values = BTreeMap::new();
    for record in records
        .records()
        .only(&[crate::wal::WalRecordType::SequenceState])
    {
        let record = record?;
        if record.lsn < covered {
            continue;
        }
        if let WalPayload::SequenceState {
            sequence_id,
            counter_id,
            resume_value,
            is_called,
            ..
        } = record.payload
        {
            if !values.contains_key(&counter_id) {
                charge.try_grow(160)?;
            }
            values.insert(
                counter_id,
                SequenceValue {
                    sequence_id,
                    counter_id,
                    resume_value,
                    is_called,
                    state_lsn: record.lsn,
                },
            );
        }
    }
    Ok(RecoveredSequenceValues {
        values,
        _charge: charge,
    })
}
