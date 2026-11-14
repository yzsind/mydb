// Recycled objects remain ordinary physical roots. Only namespace ownership
// changes; dirty pages and committed undo retain their normal lifetimes.
struct PreparedRecycle<'a> {
    _namespace: RecycleMdlGuard<'a>,
    _memory: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    entry: crate::recyclebin::Entry,
    table: Arc<TableSchema>,
}

struct RecycleDatabaseUndo {
    schema: DatabaseSchema,
    views: BTreeMap<String, ViewSchema>,
    routines: BTreeMap<String, Arc<RoutineSchema>>,
    triggers: BTreeMap<String, TriggerSchema>,
    events: BTreeMap<String, EventSchema>,
    sequences: BTreeMap<String, Arc<SequenceSchema>>,
}

impl Database {
    fn snapshot_recycle_database(&self, name: &str) -> Result<RecycleDatabaseUndo, QueryError> {
        let catalog = self.catalog_read()?;
        Ok(RecycleDatabaseUndo {
            schema: catalog
                .databases
                .get(name)
                .cloned()
                .ok_or_else(|| crate::recyclebin::failure("drop database disappeared"))?,
            views: catalog
                .views
                .iter()
                .filter(|(_, v)| v.database == name)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            routines: catalog
                .routines
                .iter()
                .filter(|(_, v)| v.database == name)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            triggers: catalog
                .triggers
                .iter()
                .filter(|(_, v)| v.database == name)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            events: catalog
                .events
                .iter()
                .filter(|(_, v)| v.database == name)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            sequences: catalog
                .sequences
                .iter()
                .filter(|(_, v)| v.database == name)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        })
    }

    fn stage_table_retirement(&self, intent: &DropTableIntent) -> Result<(), QueryError> {
        let json =
            serde_json::to_string(intent).map_err(|e| crate::recyclebin::failure(e.to_string()))?;
        self.note_catalog_mutation(CatalogMutation::Retirement(intent.table_id, json));
        Ok(())
    }

    pub fn recyclebin_variable(&self, name: &str) -> u64 {
        let c = self.runtime_config_read().recyclebin;
        match name {
            "mydb_recyclebin" => c.enabled as u64,
            "mydb_recyclebin_retention_seconds" => c.retention_seconds,
            "mydb_recyclebin_min_free_pct" => c.min_free_pct,
            "mydb_recyclebin_target_free_pct" => c.target_free_pct,
            "mydb_recyclebin_gc_interval_ms" => c.gc_interval_ms,
            _ => 0,
        }
    }

    pub fn set_recyclebin_variable(&self, name: &str, value: u64) -> Result<(), QueryError> {
        let mut runtime = self.runtime_config_write()?;
        let mut c = runtime.recyclebin;
        match name {
            "mydb_recyclebin" if value <= 1 => c.enabled = value != 0,
            "mydb_recyclebin_retention_seconds" if (1..=31_536_000).contains(&value) => {
                c.retention_seconds = value
            }
            "mydb_recyclebin_min_free_pct" if value < c.target_free_pct => c.min_free_pct = value,
            "mydb_recyclebin_target_free_pct" if value > c.min_free_pct && value <= 90 => {
                c.target_free_pct = value
            }
            "mydb_recyclebin_gc_interval_ms" if (1000..=3_600_000).contains(&value) => {
                c.gc_interval_ms = value
            }
            _ => {
                return Err(crate::recyclebin::failure(format!(
                    "invalid value for {name}"
                )))
            }
        }
        runtime.recyclebin = c;
        Ok(())
    }

    fn recyclebin_enabled(&self) -> bool {
        crate::recyclebin::session_policy().unwrap_or(self.runtime_config_read().recyclebin.enabled)
    }

    fn note_recycle_entry(
        &self,
        id: u64,
        entry: Option<&crate::recyclebin::Entry>,
    ) -> Result<(), QueryError> {
        let json = entry
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| crate::recyclebin::failure(e.to_string()))?;
        self.note_catalog_mutation(CatalogMutation::Recycle(id, json));
        Ok(())
    }

    fn prepare_recycle(
        &self,
        source: &TableSchema,
        operation: &str,
        txn_id: u64,
    ) -> Result<PreparedRecycle<'_>, QueryError> {
        let committed = self.catalog_rows_for_selector(
            crate::storage::data_dictionary::DD_TABLES,
            CatalogRowSelector::PrimaryValues(vec![Value::UInt(source.id)]),
        )?;
        let published = committed
            .first()
            .map(|row| crate::storage::data_dictionary::table_header_from_dd_row(row))
            .transpose()?;
        if !published.is_some_and(|table| {
            table.object_id == source.object_id
                && table.active_generation == source.active_generation
                && table.qualified_name() == source.qualified_name()
        }) {
            return Err(error(
                crate::error::catalog::ER_TABLE_DEF_CHANGED,
                "cannot recycle an unpublished table definition; retry DDL",
            ));
        }
        let mut memory = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
            CatalogManager::descriptor_bytes(source),
        )?;
        let manifest = self.build_drop_table_intent(source)?;
        memory.try_grow(
            manifest
                .files
                .iter()
                .chain(&manifest.vector_files)
                .map(|p| p.capacity().saturating_mul(3))
                .sum(),
        )?;
        let id = self.catalog_write()?.allocate_table_id()?;
        self.ensure_table_id_highwater(self.current_next_table_id()?)?;
        let namespace = self.recyclebin_exclusive_mdl(&format!("r_{id}"), None, false)?;
        let mut table = source.clone();
        table.database = crate::recyclebin::DATABASE.into();
        table.name = format!("r_{id}");
        table.object_id = id;
        table.row_generation_txn_id = txn_id;
        table.primary_definition_scn = self.visible_scn().saturating_add(1);
        table.bump_schema_version();
        let mut omitted = Vec::new();
        let check_names = table
            .check_constraints
            .iter()
            .map(|c| c.name.clone())
            .collect();
        for (ordinal, constraint) in table.check_constraints.iter_mut().enumerate() {
            constraint.name = format!("rc_{id}_{ordinal}");
        }
        for fk in &table.foreign_keys {
            omitted.push(format!("FOREIGN KEY {}", fk.name));
        }
        for policy in &table.row_security.policies {
            omitted.push(format!("ROW SECURITY {}", policy.name));
        }
        for trigger in self.catalog_read()?.triggers.values() {
            if trigger
                .table_database
                .eq_ignore_ascii_case(&source.database)
                && trigger.table_name.eq_ignore_ascii_case(&source.name)
            {
                omitted.push(format!("TRIGGER {}", trigger.name));
            }
        }
        for sequence in self.sequences_owned_by_tables(&[source.object_id]) {
            omitted.push(format!(
                "OWNED SEQUENCE {}.{}",
                sequence.database, sequence.name
            ));
        }
        table.foreign_keys.clear();
        table.fk_referenced_by.clear();
        table.row_security = crate::engine::rls::RowSecurity::empty();
        table.build_column_index();
        let now = current_unix_ms();
        let entry = crate::recyclebin::Entry {
            id,
            table_id: source.id,
            origin_database: source.database.clone(),
            origin_table: source.name.clone(),
            operation: operation.into(),
            operation_id: txn_id,
            recycled_ms: now,
            expires_ms: now.saturating_add(
                self.runtime_config_read()
                    .recyclebin
                    .retention_seconds
                    .saturating_mul(1000),
            ),
            data_bytes: manifest
                .files
                .iter()
                .chain(&manifest.vector_files)
                .filter_map(|p| fs::metadata(self.data_dir.join(p)).ok())
                .fold(0u64, |n, m| n.saturating_add(m.len())),
            purging: false,
            omitted,
            check_names,
            manifest: serde_json::to_string(&manifest)
                .map_err(|e| crate::recyclebin::failure(e.to_string()))?,
            last_error: String::new(),
            purge_reason: String::new(),
        };
        #[cfg(test)]
        recyclebin_crash_point("after_prepare");
        Ok(PreparedRecycle {
            _namespace: namespace,
            _memory: memory,
            entry,
            table: Arc::new(table),
        })
    }

    fn install_recycle(&self, prepared: &PreparedRecycle<'_>) -> Result<(), QueryError> {
        let key = prepared.table.qualified_name();
        self.catalog_write()?
            .tables
            .insert(key.clone(), prepared.table.clone());
        self.catalog.register_table(&key, &prepared.table);
        self.primary_definition_highwater
            .fetch_max(prepared.table.primary_definition_scn, Ordering::Release);
        for id in prepared
            .table
            .storage_ids()
            .into_iter()
            .chain([prepared.table.id])
        {
            self.table_locks_sharded
                .set_schema_floor(id, prepared.table.schema_version);
        }
        self.note_catalog_table(prepared.table.id);
        self.note_recycle_entry(prepared.entry.id, Some(&prepared.entry))
    }

    fn commit_recycle_catalog(&self, txn: TxnView) -> Result<(), QueryError> {
        #[cfg(test)]
        recyclebin_crash_point("before_commit");
        let result = self.commit_catalog_txn_existing(txn);
        if result.is_ok() {
            let committed = self
                .txns
                .read()
                .slot(txn.txn_id)
                .and_then(|slot| slot.commit_scn)
                .unwrap_or_else(|| self.visible_scn());
            self.primary_definition_highwater
                .fetch_max(committed, Ordering::Release);
            #[cfg(test)]
            recyclebin_crash_point("after_commit");
        }
        if result.is_err() {
            Self::take_catalog_mutations();
            if self.ensure_operational().is_ok() {
                if let Err(err) = self.rollback_txn(txn.txn_id) {
                    self.quarantine_mvcc(format!(
                        "recycle-bin catalog rollback failed: {}",
                        err.message()
                    ));
                }
            }
        }
        result
    }

    fn rollback_recycle_binding(
        &self,
        original: &Arc<TableSchema>,
        displaced: &str,
    ) -> Result<(), QueryError> {
        let key = original.qualified_name();
        {
            let mut catalog = self.catalog_write()?;
            if displaced != key {
                catalog.tables.remove(displaced);
            }
            catalog.tables.insert(key.clone(), original.clone());
        }
        self.catalog.register_table(&key, original);
        if displaced != key {
            self.catalog.forget_dropped_table(displaced, original);
        }
        // The DDL still owns its namespace/data locks. Restore the exact
        // pre-publication floor only after the failed transaction rolled back.
        for id in original.storage_ids().into_iter().chain([original.id]) {
            let mut shard = self.table_locks_sharded.table_shard(id).lock();
            shard.entry(id).or_default().minimum_schema_version = original.schema_version;
        }
        self.catalog.publish_table_deltas(&[original.id]);
        Ok(())
    }

    pub fn recyclebin_entries(&self) -> Result<Vec<crate::recyclebin::Entry>, QueryError> {
        let id = crate::storage::data_dictionary::DD_RECYCLEBIN;
        let schema = crate::storage::data_dictionary::dd_table_schema(id).unwrap();
        let snapshot = StorageReadSnapshot::new(self);
        let pages = self.with_heap_read(id, |heap| heap.page_count())?;
        let mut entries = Vec::new();
        // DD replacements retire physical index keys before commit. A heap
        // MVCC scan keeps the previous committed rows visible until that
        // decision, including while a replacement subsequently rolls back.
        for start in (0..pages).step_by(64) {
            self.scan_rows_columns_storage_streaming_range(
                &schema,
                id,
                snapshot.txn,
                &[0, 1, 2],
                start,
                start.saturating_add(64).min(pages),
                |row| {
                    entries.push(crate::recyclebin::Entry::decode(row)?);
                    Ok(())
                },
            )?;
        }
        Ok(entries)
    }

    pub fn recyclebin_entry(&self, name: &str) -> Result<crate::recyclebin::Entry, QueryError> {
        let normalized = normalize_name(name);
        let name = normalized.as_str();
        let id = name
            .strip_prefix("r_")
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| crate::recyclebin::failure("invalid recycle-bin object name"))?;
        let dd_id = crate::storage::data_dictionary::DD_RECYCLEBIN;
        let schema = crate::storage::data_dictionary::dd_table_schema(dd_id).unwrap();
        let primary = Self::dd_primary_index(&schema)?;
        let key = Self::dd_selector_key(&schema, primary, &[Value::UInt(id)])?;
        let snapshot = StorageReadSnapshot::new(self);
        // Include old-key repair candidates while a DD replacement has
        // removed its old B-tree entry but has not committed yet.
        for row_id in self.lookup_index_visible(&schema, primary, key, snapshot.txn)? {
            if let Some(row) = self.read_row_at(&schema, dd_id, row_id, snapshot.txn)? {
                let entry = crate::recyclebin::Entry::decode(&row.values)?;
                if entry.id == id {
                    return Ok(entry);
                }
            }
        }
        Err(crate::recyclebin::failure(format!(
            "recycle-bin object '{name}' does not exist"
        )))
    }

    fn recycle_drop_tables(&self, names: &[String], if_exists: bool) -> Result<(), QueryError> {
        let _serial = self.recyclebin_catalog_lock(false)?;
        let txn = self.begin_txn()?;
        let _txn_guard = RollbackTxnGuard::<true> {
            db: self,
            txn_id: txn.txn_id,
        };
        let mut sources = Vec::new();
        let mut keys = HashSet::new();
        for name in names {
            let (database, table_name) = split_table_name(DEFAULT_DATABASE, name);
            check_table_ddl_writable(&database)?;
            let key = table_key(&database, &table_name);
            if !keys.insert(key.clone()) {
                return Err(error(
                    crate::error::catalog::ER_NONUNIQ_TABLE,
                    "duplicate DROP TABLE target",
                ));
            }
            let Some(table) = self.table_by_key(&key) else {
                if !if_exists {
                    return Err(error(
                        crate::error::catalog::ER_BAD_TABLE_ERROR,
                        format!("Unknown table '{name}'"),
                    ));
                }
                crate::engine::api::push_statement_warning(
                    crate::error::ConditionLevel::Note,
                    crate::error::catalog::ER_BAD_TABLE_ERROR,
                    format!("Unknown table '{name}'"),
                );
                continue;
            };
            self.check_ddl_table_lock(table.id)?;
            sources.push((key, table));
        }
        if fk_checks_enabled() {
            for (_, table) in &sources {
                if table
                    .fk_referenced_by
                    .iter()
                    .any(|f| f.child_table_id != table.id && !keys.contains(&f.child_table_key))
                {
                    return Err(error(
                        crate::error::catalog::ER_FK_CANNOT_DROP_PARENT,
                        "cannot drop a table referenced by a foreign key",
                    ));
                }
            }
        }
        let mut lock_ids = sources
            .iter()
            .flat_map(|(_, t)| t.storage_ids().into_iter().chain([t.id]))
            .collect::<Vec<_>>();
        lock_ids.sort_unstable();
        lock_ids.dedup();
        for id in lock_ids {
            self.acquire_table_exclusive(txn.txn_id, id)?;
        }
        let prepared = sources
            .iter()
            .map(|(_, t)| self.prepare_recycle(t, "DROP TABLE", txn.txn_id))
            .collect::<Result<Vec<_>, _>>()?;
        let removed_triggers = self
            .catalog_read()?
            .triggers
            .iter()
            .filter(|(_, trigger)| {
                sources.iter().any(|(_, table)| {
                    trigger.table_database.eq_ignore_ascii_case(&table.database)
                        && trigger.table_name.eq_ignore_ascii_case(&table.name)
                })
            })
            .map(|(key, trigger)| (key.clone(), trigger.clone()))
            .collect::<Vec<_>>();
        {
            let mut catalog = self.catalog_write()?;
            for (key, table) in &sources {
                catalog.tables.remove(key);
                catalog.triggers.retain(|_, trigger| {
                    let keep = !trigger.table_database.eq_ignore_ascii_case(&table.database)
                        || !trigger.table_name.eq_ignore_ascii_case(&table.name);
                    if !keep {
                        self.note_catalog_trigger(&trigger.database, &trigger.name);
                    }
                    keep
                });
            }
            if sources.iter().any(|(_, t)| !t.foreign_keys.is_empty()) {
                build_fk_runtime(&mut catalog);
            }
        }
        for p in &prepared {
            self.install_recycle(p)?;
        }
        if let Err(err) = self.commit_recycle_catalog(txn) {
            for ((_, table), prepared) in sources.iter().zip(&prepared) {
                self.rollback_recycle_binding(table, &prepared.table.qualified_name())?;
            }
            let mut catalog = self.catalog_write()?;
            catalog.triggers.extend(removed_triggers);
            if sources.iter().any(|(_, t)| !t.foreign_keys.is_empty()) {
                build_fk_runtime(&mut catalog);
            }
            return Err(err);
        }
        for (key, table) in &sources {
            self.catalog.forget_dropped_table(key, table);
            self.table_write_epochs.write().remove(&table.id);
        }
        Ok(())
    }

    pub fn restore_recycled_table(
        &self,
        name: &str,
        target: Option<&str>,
    ) -> Result<Vec<String>, QueryError> {
        self.restore_recycled_table_logged(name, target, None)
    }

    pub(crate) fn restore_recycled_table_logged(
        &self,
        name: &str,
        target: Option<&str>,
        binlog: Option<&crate::binlog::BinlogQueryContext>,
    ) -> Result<Vec<String>, QueryError> {
        let default_target = self
            .recyclebin_entry(name)
            .map(|e| table_key(&e.origin_database, &e.origin_table))?;
        let target = Some(target.unwrap_or(&default_target));
        let _mdl = self.recyclebin_exclusive_mdl(name, target, false)?;
        let _serial = self.recyclebin_catalog_lock(false)?;
        let entry = self.recyclebin_entry(name)?;
        if entry.purging {
            return Err(crate::recyclebin::failure(
                "recycle-bin object is being purged",
            ));
        }
        let origin = table_key(&entry.origin_database, &entry.origin_table);
        let target = target.unwrap_or(&origin);
        let (database, table_name) = split_table_name(DEFAULT_DATABASE, target);
        check_table_ddl_writable(&database)?;
        validate_mysql_identifier(&table_name)?;
        if !self.catalog_read()?.databases.contains_key(&database) {
            return Err(error(
                crate::error::catalog::ER_BAD_DB_ERROR,
                "restore target database does not exist",
            ));
        }
        let key = table_key(&database, &table_name);
        if self.relation_name_exists(&key) {
            return Err(error(
                crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                "restore target already exists",
            ));
        }
        let archived_key = table_key(crate::recyclebin::DATABASE, name);
        let old = self.table(&archived_key)?;
        self.check_ddl_table_lock(old.id)?;
        let storage_txn = self.begin_read_only_txn()?;
        let _storage_guard = OnlineIndexTxnGuard {
            db: self,
            txn_id: storage_txn.txn_id,
        };
        let mut storage_ids = old.storage_ids();
        storage_ids.push(old.id);
        storage_ids.sort_unstable();
        storage_ids.dedup();
        for id in storage_ids {
            self.acquire_table_exclusive(storage_txn.txn_id, id)?;
        }
        let txn = self.begin_txn()?;
        let _guard = RollbackTxnGuard::<true> {
            db: self,
            txn_id: txn.txn_id,
        };
        let mut restored = old.as_ref().clone();
        restored.database = database;
        restored.name = table_name;
        for (constraint, name) in restored
            .check_constraints
            .iter_mut()
            .zip(&entry.check_names)
        {
            constraint.name = name.clone();
        }
        {
            let catalog = self.catalog_read()?;
            let names = check_constraint_names_in_schema(&catalog, &restored.database, None);
            ensure_check_constraint_names_available(&names, &restored.check_constraints)?;
        }
        self.validate_recycle_restore_dependencies(&restored)?;
        restored.object_id = self.catalog_write()?.allocate_table_id()?;
        self.ensure_table_id_highwater(self.current_next_table_id()?)?;
        restored.row_generation_txn_id = txn.txn_id;
        restored.primary_definition_scn = self.visible_scn().saturating_add(1);
        restored.bump_schema_version();
        let restored = self.bind_restored_inbound_foreign_keys(restored)?;
        if let Some(ctx) = binlog {
            if restored.engine.eq_ignore_ascii_case("mydb")
                || restored.pctfree != 0
                || restored.pctused != 0
                || restored
                    .columns
                    .iter()
                    .any(|c| matches!(c.data_type, DataTypeKind::Vector { .. }))
                || restored.indexes.iter().any(|i| i.is_vector())
            {
                return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET, "RESTORE definition cannot be represented in MySQL 8.0 logical binlog; use native replication or sql_log_bin=OFF"));
            }
            let ddl = format!(
                "{} START TRANSACTION",
                crate::engine::show::create_table_sql_with_auto_increment(
                    &restored,
                    self.auto_increment_next_value(&old)?
                )?
            );
            crate::binlog::record_ddl_transactional_with(
                txn.txn_id,
                &restored.database,
                &ddl,
                ctx,
            )?;
            crate::binlog::begin_bulk_transaction_spool(txn.txn_id, &self.data_dir, ctx.thread_id)?;
            let mut rows = Vec::new();
            let mut bytes = 0;
            self.scan_rows_ordered_streaming(&archived_key, txn, |values| {
                crate::engine::cancel::check_cancelled()?;
                crate::binlog::validate_row_domain(&restored, values)?;
                bytes += values.iter().map(Value::heap_capacity).sum::<usize>();
                rows.push(values.to_vec());
                if rows.len() >= 256 || bytes >= 4 * 1024 * 1024 {
                    crate::binlog::record_write_rows(
                        txn.txn_id,
                        &restored,
                        std::mem::take(&mut rows),
                    )?;
                    bytes = 0;
                }
                Ok(())
            })?;
            crate::binlog::record_write_rows(txn.txn_id, &restored, rows)?;
        }
        {
            let mut catalog = self.catalog_write()?;
            catalog.tables.remove(&archived_key);
            catalog.tables.insert(key.clone(), restored.clone());
        }
        self.catalog.register_table(&key, &restored);
        self.primary_definition_highwater
            .fetch_max(restored.primary_definition_scn, Ordering::Release);
        for id in restored.storage_ids().into_iter().chain([restored.id]) {
            self.table_locks_sharded
                .set_schema_floor(id, restored.schema_version);
        }
        self.note_catalog_table(restored.id);
        self.note_recycle_entry(entry.id, None)?;
        if let Err(err) = self.commit_recycle_catalog(txn) {
            self.rollback_recycle_binding(&old, &key)?;
            return Err(err);
        }
        self.catalog.forget_dropped_table(&archived_key, &old);
        Ok(entry.omitted)
    }

    fn bind_restored_inbound_foreign_keys(
        &self,
        table: TableSchema,
    ) -> Result<Arc<TableSchema>, QueryError> {
        let children = self.inbound_fk_child_ids(&table)?;
        if children.is_empty() {
            return Ok(Arc::new(table));
        }
        let key = table.qualified_name();
        let mut graph = super::catalog::empty_catalog();
        graph.tables.insert(key.clone(), Arc::new(table));
        for child_id in children {
            if child_id == graph.tables[&key].id {
                continue;
            }
            if let Some(child) = self.decode_table_descriptor_rows(child_id)? {
                graph.tables.insert(child.qualified_name(), child);
            }
        }
        let action = |action| match action {
            ReferentialAction::Restrict => crate::parser::ReferentialActionDef::Restrict,
            ReferentialAction::Cascade => crate::parser::ReferentialActionDef::Cascade,
            ReferentialAction::SetNull => crate::parser::ReferentialActionDef::SetNull,
            ReferentialAction::NoAction => crate::parser::ReferentialActionDef::NoAction,
        };
        for child in graph.tables.values() {
            let mut names = BTreeSet::new();
            let mut generation = 1;
            let mut indexes = child
                .indexes
                .iter()
                .map(|index| normalize_name(&index.name))
                .collect();
            for fk in &child.foreign_keys {
                if table_key(&fk.referenced_database, &fk.referenced_table) != key {
                    continue;
                }
                // Reuse ordinary FK definition validation without adding a
                // constraint or scanning existing child rows.
                let definition = crate::parser::ForeignKeyDef {
                    name: Some(fk.name.clone()),
                    columns: fk.columns.clone(),
                    referenced_table: key.clone(),
                    referenced_columns: fk.referenced_columns.clone(),
                    on_delete: Some(action(fk.on_delete)),
                    on_update: Some(action(fk.on_update)),
                };
                let (_, new_index) = fk_build_one(
                    &graph,
                    child,
                    &[],
                    definition,
                    &mut names,
                    &mut generation,
                    &mut indexes,
                )?;
                if new_index.is_some() {
                    return Err(error(
                        crate::error::catalog::ER_FK_NO_INDEX_CHILD,
                        "existing foreign key has no usable child index",
                    ));
                }
            }
        }
        build_fk_runtime(&mut graph);
        Ok(graph
            .tables
            .remove(&key)
            .expect("restored parent retained in FK graph"))
    }

    fn validate_recycle_restore_dependencies(&self, table: &TableSchema) -> Result<(), QueryError> {
        struct Check<'a> {
            db: &'a Database,
            database: &'a str,
            failure: Option<QueryError>,
        }
        impl crate::parser::visit::AstVisitor for Check<'_> {
            fn enter_expr(&mut self, expr: &crate::parser::Expr) {
                if self.failure.is_some() {
                    return;
                }
                if let crate::parser::Expr::Function { name, args, .. } = expr {
                    if crate::storage::sequence::is_sequence_function(name) {
                        match args.first() {
                            Some(crate::parser::Expr::Literal(crate::parser::Literal::String(
                                sequence,
                            ))) => {
                                if let Err(err) = self.db.resolve_sequence(self.database, sequence)
                                {
                                    self.failure = Some(err);
                                }
                            }
                            _ => {
                                self.failure = Some(crate::recyclebin::failure(
                                    "RESTORE requires a statically resolvable sequence dependency",
                                ))
                            }
                        }
                    } else if !crate::engine::cbo::function_cost::is_known_builtin_function(name)
                        && self
                            .db
                            .routine_definition(self.database, RoutineKindSchema::Function, name)
                            .is_none()
                    {
                        self.failure = Some(crate::recyclebin::failure(format!(
                            "RESTORE has missing function dependency '{name}'"
                        )));
                    }
                }
            }
        }
        let expressions = table
            .columns
            .iter()
            .flat_map(|c| [&c.default_expr, &c.generated_expr, &c.on_update_expr])
            .filter_map(|e| e.as_deref());
        for expression in expressions {
            let statement = crate::parser::parse_statement(&format!("SELECT {expression}"))
                .map_err(|e| crate::recyclebin::failure(e.to_string()))?;
            let mut check = Check {
                db: self,
                database: &table.database,
                failure: None,
            };
            crate::parser::visit::visit_statement(&mut check, &statement);
            if let Some(err) = check.failure {
                return Err(err);
            }
        }
        Ok(())
    }

    pub fn purge_recycled_table(&self, name: &str) -> Result<(), QueryError> {
        self.purge_recycled_table_inner(name, false)
    }

    pub fn purge_recyclebin_all(&self) -> Result<(), QueryError> {
        // Bound both memory and the set of candidate IDs. New recycling must
        // not make a long-running administrative statement chase the tail.
        let upper = IndexBound {
            key: IndexKey::from(vec![KeyPart::UInt(self.current_next_table_id()?)]),
            inclusive: false,
        };
        let mut cursor = None;
        loop {
            crate::engine::cancel::check_cancelled()?;
            let entries = {
                // Freeze the committed catalog only while selecting this
                // bounded batch. Release it before acquiring per-entry MDL.
                let _serial = self.recyclebin_catalog_lock(false)?;
                let snapshot = StorageReadSnapshot::new(self);
                self.recyclebin_cleanup_candidates(
                    "PRIMARY",
                    &mut cursor,
                    snapshot.txn,
                    false,
                    Some(&upper),
                )?
            };
            for entry in entries {
                crate::engine::cancel::check_cancelled()?;
                if !entry.purging {
                    self.purge_recycled_table(&entry.name())?;
                }
            }
            if cursor.is_none() {
                return Ok(());
            }
        }
    }

    fn purge_recycled_table_inner(&self, name: &str, background: bool) -> Result<(), QueryError> {
        // Unpublished IDs cannot be purge targets. Taking their namespace
        // lease first could invert DROP's catalog -> new-name lock order.
        self.recyclebin_entry(name)?;
        let _mdl = self.recyclebin_exclusive_mdl(name, None, background)?;
        let _serial = self.recyclebin_catalog_lock(background)?;
        let mut entry = self.recyclebin_entry(name)?;
        if entry.purging {
            return Ok(());
        }
        let key = table_key(crate::recyclebin::DATABASE, name);
        let table = self.table(&key)?;
        self.check_ddl_table_lock(table.id)?;
        let txn = self.begin_txn()?;
        let _guard = RollbackTxnGuard::<true> {
            db: self,
            txn_id: txn.txn_id,
        };
        for id in table.storage_ids().into_iter().chain([table.id]) {
            self.acquire_table_exclusive(txn.txn_id, id)?;
        }
        for id in table.storage_ids().into_iter().chain([table.id]) {
            self.table_locks_sharded.set_schema_floor(id, u64::MAX);
        }
        entry.purging = true;
        entry.purge_reason = if !background {
            "MANUAL"
        } else if entry.expires_ms <= current_unix_ms() {
            "EXPIRED"
        } else {
            "SPACE_PRESSURE"
        }
        .into();
        self.catalog_write()?.tables.remove(&key);
        self.note_catalog_table(table.id);
        self.note_recycle_entry(entry.id, Some(&entry))?;
        if let Err(err) = self.commit_recycle_catalog(txn) {
            self.rollback_recycle_binding(&table, &key)?;
            return Err(err);
        }
        let covered = self.log.lock().last_lsn();
        if covered != 0 && self.config.durability != DurabilityMode::Memory {
            crate::wal::LogManager::sync_durable_to(&self.log, covered)?;
        }
        let _ = self.evict_dropped_table_caches(&table);
        self.catalog.forget_dropped_table(&key, &table);
        Ok(())
    }

    fn recyclebin_catalog_lock(
        &self,
        background: bool,
    ) -> Result<parking_lot::MutexGuard<'_, ()>, QueryError> {
        let timeout = if background {
            Duration::ZERO
        } else {
            crate::recyclebin::mdl_timeout(self.config.lock_wait)
        };
        let started = Instant::now();
        loop {
            crate::engine::cancel::check_cancelled()?;
            self.ensure_operational()?;
            if let Some(guard) = self.recyclebin_ddl.try_lock() {
                return Ok(guard);
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(error(
                    crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                    "recycle-bin operation timed out waiting for catalog lock",
                ));
            }
            if let Some(guard) = self
                .recyclebin_ddl
                .try_lock_for(remaining.min(Duration::from_millis(10)))
            {
                // A timeout or cancellation can arrive while the mutex waits.
                crate::engine::cancel::check_cancelled()?;
                self.ensure_operational()?;
                return Ok(guard);
            }
        }
    }

    fn recyclebin_exclusive_mdl(
        &self,
        name: &str,
        target: Option<&str>,
        background: bool,
    ) -> Result<RecycleMdlGuard<'_>, QueryError> {
        let owner = crate::storage::mdl::allocate_internal_owner()?;
        let guard = RecycleMdlGuard { db: self, owner };
        let mut names = vec![table_key(crate::recyclebin::DATABASE, name)];
        if let Some(target) = target {
            names.push(target.to_owned());
        }
        let mut requests = Vec::new();
        for name in names {
            let (database, table) = split_table_name(DEFAULT_DATABASE, &name);
            requests.push(MdlRequest {
                key: MdlObjectKey::schema(&database),
                lock_type: MdlLockType::IntentionExclusive,
            });
            requests.push(MdlRequest {
                key: MdlObjectKey::table(&database, &table),
                lock_type: MdlLockType::Exclusive,
            });
        }
        let timeout = if background {
            Duration::ZERO
        } else {
            crate::recyclebin::mdl_timeout(self.config.lock_wait)
        };
        let started = Instant::now();
        loop {
            crate::engine::cancel::check_cancelled()?;
            self.ensure_operational()?;
            match self.acquire_mdl(
                owner,
                Some(owner),
                MdlDuration::Statement,
                requests.clone(),
                "ddl/recycle-bin",
            ) {
                Ok(()) => break,
                Err(err) if is_lock_wait_pending_error(&err) => {
                    if started.elapsed() >= timeout {
                        self.mdl.clear_pending_for_owner(owner);
                        return Err(error(
                            crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                            "recycle-bin operation timed out waiting for metadata lock",
                        ));
                    }
                    self.wait_for_ddl_signal(timeout.saturating_sub(started.elapsed()))?;
                }
                Err(err) => return Err(err),
            }
        }
        Ok(guard)
    }

    pub fn recyclebin_status(&self) -> [u64; 3] {
        [
            self.recyclebin_pressure.load(Ordering::Relaxed) as u64,
            self.recyclebin_pressure_purges.load(Ordering::Relaxed),
            self.recyclebin_cleanup_failures.load(Ordering::Relaxed),
        ]
    }

    #[cfg(test)]
    pub(crate) fn set_recyclebin_test_free_percent(&self, value: u64) {
        self.recyclebin_test_free_pct
            .store(value, Ordering::Relaxed);
    }

    fn recyclebin_free_percent(&self) -> Option<u64> {
        #[cfg(test)]
        {
            let value = self.recyclebin_test_free_pct.load(Ordering::Relaxed);
            if value != u64::MAX {
                return Some(value);
            }
        }
        crate::engine::executor::spill_quota::available_percent(&self.data_dir)
    }

    pub fn maintain_recyclebin(&self) -> Result<usize, QueryError> {
        self.maintain_recyclebin_until(&|| false)
    }

    pub(crate) fn maintain_recyclebin_until(
        &self,
        should_stop: &dyn Fn() -> bool,
    ) -> Result<usize, QueryError> {
        if should_stop() {
            return Ok(0);
        }
        if self.is_snapshot_read_only() || self.physical_replica_mode() {
            return Ok(0);
        }
        let _role = self.zone_map_gate.read_recursive();
        if self.physical_replica_mode() {
            return Ok(0);
        }
        let Some(_maintenance) = self.backup_maintenance_gate.try_read() else {
            return Ok(0);
        };
        if self.capture.active.load(Ordering::Acquire) {
            return Ok(0);
        }
        let Some(mut cursor) = self.recyclebin_scan.try_lock() else {
            return Ok(0);
        };
        let config = self.runtime_config_read().recyclebin;
        let free = self.recyclebin_free_percent();
        let pressure = config.min_free_pct != 0
            && free.is_some_and(|pct| {
                pct < if self.recyclebin_pressure.load(Ordering::Relaxed) {
                    config.target_free_pct
                } else {
                    config.min_free_pct
                }
            });
        self.recyclebin_pressure.store(pressure, Ordering::Relaxed);
        let dd_id = crate::storage::data_dictionary::DD_RECYCLEBIN;
        let snapshot = StorageReadSnapshot::new(self);
        let mut entries = self.recyclebin_cleanup_candidates(
            "IDX_EXPIRES",
            &mut cursor,
            snapshot.txn,
            true,
            None,
        )?;
        let mut oldest = self.recyclebin_oldest_scan.lock();
        if pressure && entries.len() < 64 {
            let mut selected = entries.iter().map(|entry| entry.id).collect::<HashSet<_>>();
            for entry in self.recyclebin_cleanup_candidates(
                "PRIMARY",
                &mut oldest,
                snapshot.txn,
                false,
                None,
            )? {
                if entries.len() == 64 {
                    break;
                }
                if selected.insert(entry.id) {
                    entries.push(entry);
                }
            }
        } else if !pressure {
            *oldest = None;
        }
        let mut completed = 0;
        for mut entry in entries {
            if should_stop() {
                break;
            }
            crate::engine::cancel::check_cancelled()?;
            let mut pressure_decision = false;
            let expired = entry.expires_ms <= current_unix_ms();
            if !entry.purging {
                if !expired && !pressure {
                    *cursor = None;
                    return Ok(completed);
                }
                if pressure
                    && !expired
                    && self
                        .recyclebin_free_percent()
                        .is_some_and(|p| p >= config.target_free_pct)
                {
                    *oldest = None;
                    return Ok(completed);
                }
                if let Err(err) = self.purge_recycled_table_inner(&entry.name(), true) {
                    if err.is(crate::error::catalog::ER_LOCK_WAIT_TIMEOUT) {
                        continue;
                    }
                    return Err(err);
                }
                if !expired {
                    pressure_decision = true;
                }
                entry.purging = true;
            }
            let _serial = match self.recyclebin_catalog_lock(true) {
                Ok(guard) => guard,
                Err(err) if err.is(crate::error::catalog::ER_LOCK_WAIT_TIMEOUT) => continue,
                Err(err) => return Err(err),
            };
            // A durable PURGING decision is the only authority to unlink.
            let current = self.recyclebin_entry(&entry.name())?;
            if !current.purging || current.table_id != entry.table_id {
                continue;
            }
            entry = current;
            if pressure_decision && entry.purge_reason == "SPACE_PRESSURE" {
                self.recyclebin_pressure_purges
                    .fetch_add(1, Ordering::Relaxed);
            }
            let intent: DropTableIntent = serde_json::from_str(&entry.manifest)
                .map_err(|e| crate::recyclebin::failure(e.to_string()))?;
            if intent.table_id != entry.table_id || self.table_by_id(entry.table_id).is_some() {
                return Err(crate::recyclebin::failure(
                    "recycle-bin physical ownership mismatch",
                ));
            }
            let covered = self.log.lock().last_lsn();
            if covered != 0 && self.config.durability != DurabilityMode::Memory {
                crate::wal::LogManager::sync_durable_to(&self.log, covered)?;
            }
            for path in intent.files.iter().chain(&intent.vector_files) {
                let relative = Path::new(path);
                if !relative.starts_with("objects")
                    || relative
                        .components()
                        .any(|c| !matches!(c, std::path::Component::Normal(_)))
                {
                    return Err(crate::recyclebin::failure(
                        "invalid recycle-bin physical path",
                    ));
                }
                self.buffer_pool
                    .discard_pages_for_path(&self.data_dir.join(relative))?;
            }
            if self.purge_dropped_table_files_until(&intent, should_stop) {
                #[cfg(test)]
                recyclebin_crash_point("before_cleanup_commit");
                let mut txn = CatalogTxn::new(self);
                txn.replace(
                    dd_id,
                    CatalogRowSelector::PrimaryValues(vec![Value::UInt(entry.id)]),
                    Vec::new(),
                );
                txn.commit()?;
                completed += 1;
            } else {
                self.recyclebin_cleanup_failures
                    .fetch_add(1, Ordering::Relaxed);
                entry.last_error = "Physical cleanup failed; retry pending".into();
                let mut txn = CatalogTxn::new(self);
                txn.replace(
                    dd_id,
                    CatalogRowSelector::PrimaryValues(vec![Value::UInt(entry.id)]),
                    vec![entry.row()?],
                );
                txn.commit()?;
            }
        }
        Ok(completed)
    }

    fn recyclebin_cleanup_candidates(
        &self,
        index_name: &str,
        cursor: &mut Option<(IndexKey, RowId)>,
        snapshot: TxnView,
        only_due: bool,
        upper: Option<&IndexBound>,
    ) -> Result<Vec<crate::recyclebin::Entry>, QueryError> {
        let id = crate::storage::data_dictionary::DD_RECYCLEBIN;
        let rows = self.with_index_read(id, index_name, |index| {
            index.range_entries_chunk(None, upper, cursor.as_ref(), 64)
        })?;
        if rows.is_empty() {
            *cursor = None;
            return Ok(Vec::new());
        }
        let schema = crate::storage::data_dictionary::dd_table_schema(id).unwrap();
        let index = schema
            .indexes
            .iter()
            .find(|index| index.name == index_name)
            .unwrap();
        let mut result = Vec::new();
        for (key, row_id) in &rows {
            let Some(row) = self.read_row_at(&schema, id, *row_id, snapshot)? else {
                continue;
            };
            if schema.index_key(index, &row.values)? != *key {
                continue;
            }
            let entry = crate::recyclebin::Entry::decode(&row.values)?;
            if only_due && !entry.purging && entry.expires_ms > current_unix_ms() {
                *cursor = None;
                return Ok(result);
            }
            result.push(entry);
        }
        *cursor = rows.last().cloned();
        Ok(result)
    }
}

struct RecycleMdlGuard<'a> {
    db: &'a Database,
    owner: u64,
}
impl Drop for RecycleMdlGuard<'_> {
    fn drop(&mut self) {
        self.db.mdl.release_owner(self.owner);
    }
}

#[cfg(test)]
fn recyclebin_crash_point(point: &str) {
    if std::env::var("MYDB_RECYCLEBIN_TEST_CRASH").ok().as_deref() == Some(point) {
        std::process::exit(86);
    }
}
