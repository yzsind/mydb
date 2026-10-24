// Catalog-backed sequences: definitions and initial states commit atomically;
// nontransactional WAL reservations are merged into the DD at checkpoints.
use crate::storage::sequence::{
    build_sequence_schema, err_seq_exists, err_unknown_seq, SeqAlloc, SeqOptions, SequenceRuntime,
    SequenceSchema, SequenceValue,
};

impl Database {
    fn read_sequence_value(&self, counter_id: u64) -> Result<Option<SequenceValue>, QueryError> {
        use crate::storage::data_dictionary::DD_SEQUENCE_VALUES;
        let rows = self.catalog_rows_for_selector(
            DD_SEQUENCE_VALUES,
            CatalogRowSelector::PrimaryValues(vec![Value::UInt(counter_id)]),
        )?;
        if rows.len() > 1 {
            return Err(sequence_state_error("duplicate sequence state row"));
        }
        rows.into_iter()
            .next()
            .map(|row| SequenceValue::from_row(&row))
            .transpose()
    }

    fn persist_sequence_value(
        &self,
        rt: &Arc<SequenceRuntime>,
        value: i64,
        is_called: bool,
    ) -> Result<(), QueryError> {
        if self.config.durability == DurabilityMode::Memory {
            let mut logged = rt.logged.lock();
            logged.resume_value = value;
            logged.is_called = is_called;
            return Ok(());
        }
        let state = {
            let _fence = self.sequence_checkpoint_fence.read();
            let mut dirty = self.sequence_dirty.lock();
            dirty.enqueue(rt.clone())?;
            self.sequence_dirty_generations
                .store(dirty.entries.len(), Ordering::Release);
            let mut logged = rt.logged.lock();
            let lsn = self.append_wal(
                0,
                WalPayload::SequenceState {
                    sequence_id: logged.sequence_id,
                    counter_id: rt.counter_id,
                    resume_value: value,
                    is_called,
                    logged_unix_us: 0,
                },
            )?;
            self.sequence_metrics
                .wal_records
                .fetch_add(1, Ordering::Relaxed);
            *logged = SequenceValue {
                resume_value: value,
                is_called,
                state_lsn: lsn,
                ..*logged
            };
            *logged
        };
        let started = Instant::now();
        let local = LogManager::sync_durable_to(&self.log, state.state_lsn);
        self.sequence_metrics
            .local_sync_us
            .fetch_add(elapsed_us(started), Ordering::Relaxed);
        local?;
        let started = Instant::now();
        let remote = self.wait_sequence_remote(state.state_lsn);
        self.sequence_metrics
            .remote_sync_us
            .fetch_add(elapsed_us(started), Ordering::Relaxed);
        if remote.is_err() {
            self.sequence_metrics
                .remote_failures
                .fetch_add(1, Ordering::Relaxed);
        }
        remote
    }

    fn ensure_sequence_runtime(
        &self,
        def: &Arc<SequenceSchema>,
    ) -> Result<Arc<SequenceRuntime>, QueryError> {
        if let Some(rt) = self.sequences.get(def.id) {
            if rt.counter_id == def.counter_id {
                return Ok(rt);
            }
        }
        let state = self.read_sequence_value(def.counter_id)?.ok_or_else(|| {
            sequence_state_error(format!(
                "missing state for sequence {} generation {}",
                def.id, def.counter_id
            ))
        })?;
        if state.sequence_id != def.id
            || state.resume_value < def.min_value
            || state.resume_value > def.max_value
        {
            return Err(sequence_state_error(
                "sequence state does not match its definition",
            ));
        }
        let rt = self.sequences.install(
            def.clone(),
            state.resume_value,
            state.resume_value,
            state.is_called,
        )?;
        if state.state_lsn != 0 {
            rt.apply_value(state)?;
        }
        Ok(rt)
    }

    pub(crate) fn initialize_sequence_runtimes(&self) -> Result<(), QueryError> {
        let defs: Vec<_> = self.catalog_read()?.sequences.values().cloned().collect();
        let valid: HashSet<_> = defs.iter().map(|def| def.id).collect();
        self.sequences.retain(&valid);
        for def in defs {
            self.ensure_sequence_runtime(&def)?;
            self.sequences.update_def(def);
        }
        Ok(())
    }

    // -- name resolution / introspection --

    /// Resolve a (possibly schema-qualified) sequence name to its definition.
    pub fn resolve_sequence(
        &self,
        current_db: &str,
        name: &str,
    ) -> Result<Arc<SequenceSchema>, QueryError> {
        let (database, seq_name) = split_table_name(current_db, name);
        let key = table_key(&database, &seq_name);
        let catalog = self.catalog_read()?;
        catalog
            .sequences
            .get(&key)
            .cloned()
            .ok_or_else(|| err_unknown_seq(&seq_name))
    }

    /// All sequence definitions (for `SHOW SEQUENCES` / `pg_sequences`).
    pub fn list_sequences(&self) -> Result<Vec<Arc<SequenceSchema>>, QueryError> {
        let catalog = self.catalog_read()?;
        Ok(catalog.sequences.values().cloned().collect())
    }

    // -- nextval / setval bridges (session CACHE lives in the engine) --

    /// Reserve up to `want` values from a sequence (the engine hands them out
    /// from a per-session CACHE block). The recovery reservation advances lazily.
    pub fn sequence_alloc(
        &self,
        def: &Arc<SequenceSchema>,
        want: i64,
    ) -> Result<SeqAlloc, QueryError> {
        self.ensure_writable()?;
        let rt = self.ensure_sequence_runtime(def)?;
        let batch = self.sequence_wal_batch;
        rt.alloc(want, batch, |durable, is_called| {
            self.persist_sequence_value(&rt, durable, is_called)
        })
    }

    /// `setval(seq, value, is_called)` — non-transactional.
    pub fn sequence_setval(
        &self,
        def: &Arc<SequenceSchema>,
        value: i64,
        is_called: bool,
    ) -> Result<(), QueryError> {
        self.ensure_writable()?;
        let rt = self.ensure_sequence_runtime(def)?;
        rt.setval(value, is_called, |durable, called| {
            self.persist_sequence_value(&rt, durable, called)
        })
    }

    // -- DDL --

    pub fn create_sequence(
        &self,
        current_db: &str,
        name: &str,
        if_not_exists: bool,
        temporary: bool,
        options: &SeqOptions,
    ) -> Result<(), QueryError> {
        if temporary {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "CREATE TEMPORARY SEQUENCE is not yet supported",
            ));
        }
        let (database, seq_name) = split_table_name(current_db, name);
        let key = table_key(&database, &seq_name);
        check_table_ddl_writable(&database)?;
        validate_mysql_identifier(&seq_name)?;

        let mut def_arc = {
            let mut catalog = self.catalog_write()?;
            if !catalog.databases.contains_key(&database) {
                return Err(error(
                    crate::error::catalog::ER_BAD_DB_ERROR,
                    format!("unknown database '{database}'"),
                ));
            }
            if catalog.sequences.contains_key(&key) {
                if if_not_exists {
                    return Ok(());
                }
                return Err(err_seq_exists(&seq_name));
            }
            if catalog.tables.contains_key(&key) || catalog.views.contains_key(&key) {
                return Err(error(
                    crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                    format!("Table '{seq_name}' already exists"),
                ));
            }
            let now = current_unix_ms();
            let id = catalog.next_sequence_id;
            // `build_*` validates; only commit the id bump on success.
            let def = build_sequence_schema(id, &database, &seq_name, None, options, now, now)?;
            catalog.next_sequence_id = id
                .checked_add(1)
                .ok_or_else(|| sequence_state_error("sequence identity exhausted"))?;
            Arc::new(def)
        };

        self.ensure_sequence_id_highwater(self.current_next_sequence_id()?)?;
        self.resolve_sequence_ownership(Arc::make_mut(&mut def_arc))?;
        let prepared = self.sequences.prepare(
            def_arc.clone(),
            def_arc.start_value,
            def_arc.start_value,
            false,
        )?;
        self.note_catalog_sequence_value(
            def_arc.counter_id,
            Some(SequenceValue {
                sequence_id: def_arc.id,
                counter_id: def_arc.counter_id,
                resume_value: def_arc.start_value,
                is_called: false,
                state_lsn: 0,
            }),
        );
        self.catalog_write()?
            .sequences
            .insert(key.clone(), def_arc.clone());
        self.note_catalog_sequence(def_arc.id);
        if let Err(err) = self.commit_ddl_catalog_txn_recoverable() {
            self.catalog_write()?.sequences.remove(&key);
            Self::take_catalog_mutations();
            return Err(err);
        }
        self.sequences.publish_prepared(prepared);
        Ok(())
    }

    pub fn alter_sequence(
        &self,
        current_db: &str,
        name: &str,
        if_exists: bool,
        options: &SeqOptions,
        restart: Option<Option<i64>>,
        rename_to: Option<&str>,
    ) -> Result<(), QueryError> {
        let (database, seq_name) = split_table_name(current_db, name);
        let key = table_key(&database, &seq_name);
        check_table_ddl_writable(&database)?;
        let new_name = rename_to
            .map(|target| {
                let (target_db, target_name) = split_table_name(&database, target);
                if !target_db.eq_ignore_ascii_case(&database) {
                    return Err(error(
                        crate::error::catalog::ER_FORBID_SCHEMA_CHANGE,
                        "ALTER SEQUENCE RENAME does not support moving to another database",
                    ));
                }
                validate_mysql_identifier(&target_name)?;
                Ok(target_name)
            })
            .transpose()?;
        let existing = match self.catalog_read()?.sequences.get(&key).cloned() {
            Some(def) => def,
            None if if_exists => return Ok(()),
            None => return Err(err_unknown_seq(&seq_name)),
        };
        let mut candidate = build_sequence_schema(
            existing.id,
            &database,
            &seq_name,
            Some(&existing),
            options,
            existing.created_ms,
            current_unix_ms(),
        )?;
        let restart_value = restart.map(|value| value.unwrap_or(candidate.start_value));
        if let Some(value) = restart_value {
            if value < candidate.min_value || value > candidate.max_value {
                return Err(error(
                    crate::error::catalog::MYDB_ER_INVALID_SEQUENCE,
                    format!("restart value {value} is out of bounds for sequence '{seq_name}'"),
                ));
            }
        }
        if let Some(name) = new_name {
            candidate.name = name;
        }
        let new_key = table_key(&database, &candidate.name);
        {
            let catalog = self.catalog_read()?;
            if new_key != key
                && (catalog.sequences.contains_key(&new_key)
                    || catalog.tables.contains_key(&new_key)
                    || catalog.views.contains_key(&new_key))
            {
                return Err(err_seq_exists(&candidate.name));
            }
        }
        self.resolve_sequence_ownership(&mut candidate)?;
        let changes_generator = options.as_type.is_some()
            || options.increment.is_some()
            || options.min_value != crate::storage::sequence::BoundOpt::Unset
            || options.max_value != crate::storage::sequence::BoundOpt::Unset
            || options.cache.is_some()
            || options.cycle.is_some();
        let counter_reset = if let Some(value) = restart_value {
            Some((value, false))
        } else if changes_generator {
            let (value, called) = self.ensure_sequence_runtime(&existing)?.ddl_value();
            if value < candidate.min_value || value > candidate.max_value {
                return Err(sequence_state_error(
                    "sequence current allocation is outside the new bounds",
                ));
            }
            Some((value, called))
        } else {
            None
        };
        // A restart publishes a new physical counter identity. Old session CACHE
        // blocks cannot hand out values from before this DDL, and the private
        // state is unreachable until the dictionary transaction commits.
        if counter_reset.is_some() {
            {
                let mut catalog = self.catalog_write()?;
                candidate.counter_id = catalog.next_sequence_id;
                catalog.next_sequence_id = catalog
                    .next_sequence_id
                    .checked_add(1)
                    .ok_or_else(|| sequence_state_error("sequence identity exhausted"))?;
            }
            self.ensure_sequence_id_highwater(self.current_next_sequence_id()?)?;
        }
        let candidate = Arc::new(candidate);
        let prepared = counter_reset
            .map(|(value, called)| {
                self.sequences
                    .prepare(candidate.clone(), value, value, called)
            })
            .transpose()?;
        if let Some((value, called)) = counter_reset {
            self.note_catalog_sequence_value(
                candidate.counter_id,
                Some(SequenceValue {
                    sequence_id: candidate.id,
                    counter_id: candidate.counter_id,
                    resume_value: value,
                    is_called: called,
                    state_lsn: 0,
                }),
            );
            self.note_catalog_sequence_value(existing.counter_id, None);
            self.note_catalog_mutation(CatalogMutation::SequenceRetirement(
                existing.counter_id,
                existing.id,
            ));
        }

        {
            let mut catalog = self.catalog_write()?;
            catalog.sequences.remove(&key);
            catalog.sequences.insert(new_key.clone(), candidate.clone());
        }
        self.note_catalog_sequence(existing.id);
        self.note_catalog_sequence(candidate.id);
        if let Err(err) = self.commit_ddl_catalog_txn_recoverable() {
            let mut catalog = self.catalog_write()?;
            catalog.sequences.remove(&new_key);
            catalog.sequences.insert(key, existing);
            Self::take_catalog_mutations();
            return Err(err);
        }
        if let Some(prepared) = prepared {
            self.sequences.publish_prepared(prepared);
        } else {
            // Stable object paths make a logical rename entirely metadata-only.
            self.sequences.update_def(candidate);
        }
        Ok(())
    }

    pub fn drop_sequences(
        &self,
        current_db: &str,
        names: &[String],
        if_exists: bool,
    ) -> Result<(), QueryError> {
        let mut to_drop: Vec<(String, u64, u64, String, String)> = Vec::new();
        let mut originals = Vec::new();
        {
            let catalog = self.catalog_read()?;
            for name in names {
                let (database, seq_name) = split_table_name(current_db, name);
                let key = table_key(&database, &seq_name);
                match catalog.sequences.get(&key) {
                    Some(def) => {
                        originals.push((key.clone(), def.clone()));
                        to_drop.push((key, def.id, def.counter_id, database, seq_name));
                    }
                    None => {
                        if !if_exists {
                            return Err(err_unknown_seq(&seq_name));
                        }
                    }
                }
            }
        }
        if to_drop.is_empty() {
            return Ok(());
        }
        {
            let mut catalog = self.catalog_write()?;
            for (key, _, _, _, _) in &to_drop {
                catalog.sequences.remove(key);
            }
        }
        for (_, id, counter_id, _, _) in &to_drop {
            self.note_catalog_sequence(*id);
            self.note_catalog_sequence_value(*counter_id, None);
            self.note_catalog_mutation(CatalogMutation::SequenceRetirement(*counter_id, *id));
        }
        if let Err(err) = self.commit_ddl_catalog_txn_recoverable() {
            self.catalog_write()?.sequences.extend(originals);
            Self::take_catalog_mutations();
            return Err(err);
        }
        for (_, id, _, _, _) in &to_drop {
            self.sequences.remove(*id);
        }
        Ok(())
    }
}

struct SequenceValueLease<'a> {
    db: &'a Database,
    owner: Option<u64>,
    fast: Option<crate::storage::mdl::MdlFastGrant>,
}
impl Drop for SequenceValueLease<'_> {
    fn drop(&mut self) {
        if let Some(grant) = self.fast.take() {
            self.db.release_fast_mdl(grant);
        }
        if let Some(owner) = self.owner {
            self.db.release_mdl_owner(owner);
        }
    }
}
impl Database {
    /// Sequence values are nontransactional. Protect each allocation (including
    /// a session cache hit), rather than retaining a lock until user COMMIT.
    pub(crate) fn acquire_sequence_value_lease(
        &self,
        database: &str,
        name: &str,
        connection: u64,
    ) -> Result<impl Drop + '_, QueryError> {
        let (database, name) = split_table_name(database, name);
        let requests = vec![MdlRequest {
            key: MdlObjectKey::table(&database, &name),
            lock_type: MdlLockType::SharedRead,
        }];
        if let Some(grant) = self.try_acquire_fast_mdl(&requests) {
            return Ok(SequenceValueLease {
                db: self,
                owner: None,
                fast: Some(grant),
            });
        }
        let owner = crate::storage::mdl::allocate_internal_owner()?;
        let lease = SequenceValueLease {
            db: self,
            owner: Some(owner),
            fast: None,
        };
        self.acquire_mdl(
            owner,
            Some(connection),
            MdlDuration::Statement,
            requests,
            "sql/sequence-value",
        )?;
        Ok(lease)
    }
}
