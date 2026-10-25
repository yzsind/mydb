impl Database {
    pub fn catalog(&self) -> Catalog {
        self.catalog
            .read()
            .expect("catalog read lock poisoned")
            .clone()
    }

    /// Validate cached-plan dependencies against the live catalog. Each dep
    /// carries `(table_key, table_id, schema_version)`; the key resolves the
    /// table in one `BTreeMap` lookup (O(deps)), and both the id and version
    /// must match so a DROP + CREATE that reuses the name (resetting the
    /// version to 1) is not mistaken for the original table.
    pub fn table_versions_match(&self, deps: &[crate::engine::cache::PlanDep]) -> bool {
        let Ok(catalog) = self.catalog.read() else {
            return false;
        };
        deps.iter().all(|(table_key, table_id, expected_version)| {
            catalog
                .tables
                .get(table_key.as_ref())
                .map(|table| table.id == *table_id && table.schema_version == *expected_version)
                .unwrap_or(false)
        })
    }

    /// Validate cached-plan view dependencies against the live catalog. Each
    /// dep is `(view_key, schema_version)`; the view must still exist with the
    /// same version. A missing view (DROP VIEW) or a bumped version
    /// (ALTER / CREATE OR REPLACE VIEW) fails, evicting plans that expanded it.
    pub fn view_versions_match(&self, object_deps: &[crate::engine::cache::ObjectDep]) -> bool {
        if object_deps.is_empty() {
            return true;
        }
        let Ok(catalog) = self.catalog.read() else {
            return false;
        };
        object_deps
            .iter()
            .all(|(view_key, expected_version, expected_identity)| {
                catalog
                    .views
                    .get(view_key.as_ref())
                    .map(|view| {
                        view.schema_version == *expected_version
                            && view.plan_cache_identity() == *expected_identity
                    })
                    .unwrap_or(false)
            })
    }

    /// Changes only at committed catalog publication (including replica
    /// application), before the DDL publication barrier is released.
    pub(crate) fn catalog_publication_epoch(&self) -> u64 {
        self.catalog.epoch()
    }

    pub fn table_key_identity_version_matches(
        &self,
        key: &str,
        expected_id: u64,
        expected_version: u64,
    ) -> bool {
        self.catalog
            .read()
            .map(|catalog| {
                catalog
                    .tables
                    .get(key)
                    .map(|table| {
                        table.id == expected_id && table.schema_version == expected_version
                    })
                    .unwrap_or(false)
            })
            .unwrap_or(false)
    }

    pub fn database_exists(&self, name: &str) -> bool {
        self.catalog
            .read()
            .map(|catalog| catalog.databases.contains_key(&normalize_name(name)))
            .unwrap_or(false)
    }

    /// Default character set declared on the named database, or `None`
    /// when the database is missing. Used by the `character_set_database`
    /// system variable's read closure.
    pub fn schema_charset(&self, name: &str) -> Option<String> {
        let key = normalize_name(name);
        self.catalog
            .read()
            .ok()
            .and_then(|catalog| catalog.databases.get(&key).map(|s| s.charset.clone()))
    }

    /// Default collation declared on the named database, or `None` when
    /// the database is missing.
    pub fn schema_collation(&self, name: &str) -> Option<String> {
        let key = normalize_name(name);
        self.catalog
            .read()
            .ok()
            .and_then(|catalog| catalog.databases.get(&key).map(|s| s.collation.clone()))
    }

    #[cfg(test)]
    pub fn create_database(&self, name: &str, if_not_exists: bool) -> Result<(), QueryError> {
        self.create_database_with_options(name, if_not_exists, None, None)
    }

    pub fn create_database_with_options(
        &self,
        name: &str,
        if_not_exists: bool,
        default_charset: Option<&str>,
        default_collation: Option<&str>,
    ) -> Result<(), QueryError> {
        let key = normalize_name(name);
        let collation_info = default_collation
            .map(crate::charset::resolve_collation)
            .transpose()?;
        let charset_name = match default_charset {
            Some(charset) => crate::charset::resolve_charset(charset)?.name,
            None => collation_info
                .map(|collation| collation.charset)
                .unwrap_or(crate::charset::DEFAULT_CHARSET),
        };
        let collation_name = match collation_info {
            Some(collation) => collation.name,
            None => crate::charset::default_collation_for_charset(charset_name)?,
        };
        crate::charset::validate_charset_collation_pair(charset_name, collation_name)?;
        {
            let mut catalog = self.catalog_write()?;
            if catalog.databases.contains_key(&key) {
                if if_not_exists {
                    return Ok(());
                }
                return Err(error(
                    crate::error::catalog::ER_DB_CREATE_EXISTS,
                    format!("can't create database '{name}'; database exists"),
                ));
            }
            catalog.databases.insert(
                key.clone(),
                DatabaseSchema {
                    name: key.clone(),
                    system: false,
                    charset: charset_name.to_string(),
                    collation: collation_name.to_string(),
                    created_ms: current_unix_ms(),
                    updated_ms: current_unix_ms(),
                },
            );
        }
        // Create the on-disk schema directory eagerly so subsequent
        // table creation can drop heap/index files in without an extra
        // mkdir on the hot path. Best-effort: a transient mkdir failure
        // here surfaces as a path error at the first CREATE TABLE.
        let _ = std::fs::create_dir_all(crate::storage::naming::schema_tables_dir(
            &self.data_dir,
            &key,
        ));
        let _ = std::fs::create_dir_all(crate::storage::naming::schema_indexes_dir(
            &self.data_dir,
            &key,
        ));
        self.note_catalog_schema(&key);
        self.commit_ddl_catalog_txn()
    }

    pub(crate) fn alter_database_defaults(
        &self,
        name: &str,
        charset: Option<&str>,
        collation: Option<&str>,
    ) -> Result<(), QueryError> {
        let key = normalize_name(name);
        check_table_ddl_writable(&key)?;
        validate_mysql_identifier(name)?;
        let before = self
            .catalog_read()?
            .databases
            .get(&key)
            .cloned()
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_BAD_DB_ERROR,
                    format!("unknown database '{name}'"),
                )
            })?;
        let (charset, collation) = resolve_table_charset_collation(
            charset,
            collation,
            &before.charset,
            &before.collation,
        )?;
        let mut candidate = before.clone();
        candidate.charset = charset;
        candidate.collation = collation;
        candidate.updated_ms = current_unix_ms();
        self.catalog_write()?
            .databases
            .insert(key.clone(), candidate);
        self.note_catalog_schema(&key);
        if let Err(err) = self.commit_ddl_catalog_txn_recoverable() {
            self.catalog_write()?.databases.insert(key, before);
            Self::take_catalog_mutations();
            return Err(err);
        }
        Ok(())
    }

    pub fn drop_database(&self, name: &str, if_exists: bool) -> Result<(), QueryError> {
        let key = normalize_name(name);
        if is_system_database(&key) {
            return Err(error(
                crate::error::catalog::ER_DBACCESS_DENIED_ERROR,
                format!("access denied for database '{name}'"),
            ));
        }
        {
            let catalog = self.catalog_read()?;
            if !catalog.databases.contains_key(&key) {
                if if_exists {
                    return Ok(());
                }
                return Err(error(
                    crate::error::catalog::ER_DB_DROP_EXISTS,
                    format!("can't drop database '{name}'; database doesn't exist"),
                ));
            }
        }
        let table_names = self
            .catalog_read()?
            .tables
            .values()
            .filter(|table| normalize_name(&table.database) == key)
            .map(|table| table.qualified_name())
            .collect::<Vec<_>>();
        let tables = table_names
            .iter()
            .map(|name| self.table(name))
            .collect::<Result<Vec<_>, _>>()?;
        let drop_set = table_names.iter().cloned().collect::<HashSet<_>>();
        let mut dropped = Vec::new();
        for table in &tables {
            self.check_ddl_table_lock(table.id)?;
            if fk_checks_enabled()
                && table.fk_referenced_by.iter().any(|fk| {
                    fk.child_table_id != table.id && !drop_set.contains(&fk.child_table_key)
                })
            {
                return Err(error(
                    crate::error::catalog::ER_FK_CANNOT_DROP_PARENT,
                    format!(
                        "cannot drop database '{name}': table '{}' has external references",
                        table.name
                    ),
                ));
            }
            let intent = self.build_drop_table_intent(table)?;
            let path = self.write_drop_table_intent(&intent)?;
            dropped.push((table.clone(), intent, path));
        }
        let sequences = self
            .catalog_read()?
            .sequences
            .values()
            .filter(|sequence| normalize_name(&sequence.database) == key)
            .cloned()
            .collect::<Vec<_>>();
        let (events, views, routines, triggers) = {
            let catalog = self.catalog_read()?;
            (
                catalog
                    .events
                    .values()
                    .filter(|event| normalize_name(&event.database) == key)
                    .map(|event| (event.database.clone(), event.name.clone()))
                    .collect::<Vec<_>>(),
                catalog
                    .views
                    .values()
                    .filter(|view| normalize_name(&view.database) == key)
                    .map(|view| (view.database.clone(), view.name.clone()))
                    .collect::<Vec<_>>(),
                catalog
                    .routines
                    .values()
                    .filter(|routine| normalize_name(&routine.database) == key)
                    .map(|routine| {
                        (
                            routine.database.clone(),
                            routine.kind.as_str().to_string(),
                            routine.name.clone(),
                        )
                    })
                    .collect::<Vec<_>>(),
                catalog
                    .triggers
                    .values()
                    .filter(|trigger| normalize_name(&trigger.database) == key)
                    .map(|trigger| (trigger.database.clone(), trigger.name.clone()))
                    .collect::<Vec<_>>(),
            )
        };
        let mut invalidated_views = Vec::new();
        {
            let mut catalog = self.catalog_write()?;
            let mut invalidated = std::collections::HashSet::new();
            loop {
                let mut changed = false;
                for view in catalog.views.values_mut() {
                    let view_key = (normalize_name(&view.database), normalize_name(&view.name));
                    if view_key.0 == key || invalidated.contains(&view_key) {
                        continue;
                    }
                    if view.dependencies.iter().any(|dep| {
                        normalize_name(&dep.database) == key
                            || invalidated.contains(&(
                                normalize_name(&dep.database),
                                normalize_name(&dep.name),
                            ))
                    }) {
                        view.invalid = true;
                        invalidated.insert(view_key);
                        invalidated_views.push((view.database.clone(), view.name.clone()));
                        changed = true;
                    }
                }
                if !changed {
                    break;
                }
            }
            // Cascade: scheduled events belong to the schema, so dropping
            // the database drops them too (matching MySQL). Leaving them
            // behind would orphan the rows and let the background
            // scheduler keep firing bodies against a database that no
            // longer exists.
            catalog
                .events
                .retain(|_, event| normalize_name(&event.database) != key);
            // Views, stored routines (procedures + functions) and triggers
            // are pure catalog objects (no physical heap files), so
            // `remove_dir_all` below can't reach them — they must be
            // cascaded here too, or they leak into information_schema and
            // block re-creating same-named objects in a rebuilt database
            // (1050/1304). Matches MySQL's "drop the whole schema" behavior.
            catalog
                .views
                .retain(|_, view| normalize_name(&view.database) != key);
            catalog
                .routines
                .retain(|_, routine| normalize_name(&routine.database) != key);
            catalog
                .triggers
                .retain(|_, trigger| normalize_name(&trigger.database) != key);
            for name in &table_names {
                catalog.tables.remove(name);
            }
            catalog
                .sequences
                .retain(|_, sequence| normalize_name(&sequence.database) != key);
            if tables.iter().any(|table| !table.foreign_keys.is_empty()) {
                build_fk_runtime(&mut catalog);
            }
            catalog.databases.remove(&key);
        }
        self.note_catalog_schema(&key);
        for (schema, name) in events {
            self.note_catalog_event(&schema, &name);
        }
        for (schema, name) in views.into_iter().chain(invalidated_views) {
            self.note_catalog_view(&schema, &name);
        }
        for (schema, kind, name) in routines {
            self.note_catalog_routine(&schema, &kind, &name);
        }
        for (schema, name) in triggers {
            self.note_catalog_trigger(&schema, &name);
        }
        for table in &tables {
            self.note_catalog_table(table.id);
        }
        for sequence in &sequences {
            self.note_catalog_sequence(sequence.id);
            self.note_catalog_sequence_value(sequence.counter_id, None);
            self.note_catalog_mutation(CatalogMutation::SequenceRetirement(
                sequence.counter_id,
                sequence.id,
            ));
        }
        // One dictionary transaction is the commit point for the entire set.
        self.commit_ddl_catalog_txn()?;
        for (table, intent, path) in dropped {
            if self.evict_dropped_table_caches(&table).is_ok()
                && self.purge_dropped_table_files(&intent)
            {
                self.remove_drop_table_intent(path.as_ref());
            }
            self.catalog
                .forget_dropped_table(&table.qualified_name(), &table);
            self.table_write_epochs.write().remove(&table.id);
            self.drop_table_modification_delta_without_epoch(table.id);
        }
        for sequence in sequences {
            self.sequences.remove(sequence.id);
        }
        let _ = std::fs::remove_dir_all(crate::storage::naming::schema_dir(&self.data_dir, &key));
        Ok(())
    }

    pub fn table(&self, name: &str) -> Result<Arc<TableSchema>, QueryError> {
        let key = normalize_table_key(DEFAULT_DATABASE, name);
        self.table_by_key(&key).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{name}' doesn't exist"),
            )
        })
    }

    /// Whether a table-like catalog object already owns this qualified name.
    /// CREATE TABLE shares its namespace with views and mydb sequences, so
    /// executor-side IF NOT EXISTS probes must not look at `tables` alone.
    pub fn relation_name_exists(&self, name: &str) -> bool {
        let key = normalize_table_key(DEFAULT_DATABASE, name);
        self.catalog.read().is_ok_and(|catalog| {
            catalog.tables.contains_key(&key)
                || catalog.views.contains_key(&key)
                || catalog.sequences.contains_key(&key)
        })
    }

    pub(crate) fn storage_key_for_table(&self, table: &TableSchema) -> String {
        if is_temp_table_id(table.id) {
            if let Ok(temp_tables) = self.temp_tables.read() {
                if let Some((key, _)) = temp_tables
                    .iter()
                    .find(|(_, candidate)| candidate.id == table.id)
                {
                    return key.clone();
                }
            }
        }
        table.qualified_name()
    }

    #[allow(dead_code)]
    pub fn table_in_database(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Arc<TableSchema>, QueryError> {
        let key = table_key(database, table);
        self.table_by_key(&key).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{database}.{table}' doesn't exist"),
            )
        })
    }

    pub fn is_base_table(&self, current_database: &str, name: &str) -> bool {
        let (database, table) = split_table_name(current_database, name);
        let key = table_key(&database, &table);
        self.table_by_key(&key).is_some()
    }

    /// True if `name` resolves to a *persistent* base table or view in the
    /// catalog (ignoring session/temporary tables). Used by `RENAME TABLE`,
    /// which operates only on the persistent-object namespace, to tell a
    /// temporary-table-only name apart from a real base table.
    pub fn has_persistent_table_or_view(&self, current_database: &str, name: &str) -> bool {
        let (database, table) = split_table_name(current_database, name);
        let key = table_key(&database, &table);
        self.catalog
            .read()
            .map(|catalog| catalog.tables.contains_key(&key) || catalog.views.contains_key(&key))
            .unwrap_or(false)
    }

    /// Catalog-only row-count estimate (never scans the heap). Returns the
    /// recorded `stats.row_count` for an existing base/temp table, or `None`
    /// when the name does not resolve to a base table or has no analyzed
    /// statistics yet. AST-level rewrites (e.g. the CTE inline-vs-materialise
    /// cost gate) use this to stay O(1): unlike [`Database::table_row_count`]
    /// it does not fall back to a full MVCC scan, so an unanalysed table reads
    /// as "unknown" rather than triggering an expensive count.
    pub fn catalog_row_count_estimate(&self, current_database: &str, name: &str) -> Option<u64> {
        let (database, table) = split_table_name(current_database, name);
        let key = table_key(&database, &table);
        let table = self.table_by_key(&key)?;
        (table.stats.row_count > 0).then_some(table.stats.row_count)
    }

    fn table_by_key(&self, key: &str) -> Option<Arc<TableSchema>> {
        if let Ok(temp_tables) = self.temp_tables.read() {
            if let Some(table) = temp_tables.get(key) {
                return Some(Arc::clone(table));
            }
        }
        let header = self
            .catalog
            .read()
            .ok()
            .and_then(|catalog| catalog.tables.get(key).map(Arc::clone));
        if let Some(table) = header {
            // Startup keeps only object headers.  A user table always has at
            // least one column, so an empty column vector is an unambiguous
            // lazy-descriptor marker (compiled DD tables are handled by id).
            if !table.columns.is_empty() || table.system {
                return Some(crate::storage::read_schema::for_read(
                    table,
                    self.read_definition_highwater.load(Ordering::Acquire),
                ));
            }
            match self.load_table_descriptor_from_dd(table.id) {
                Ok(Some(descriptor)) => {
                    self.catalog
                        .install_descriptor(key.to_string(), Arc::clone(&descriptor));
                    return Some(crate::storage::read_schema::for_read(
                        descriptor,
                        self.read_definition_highwater.load(Ordering::Acquire),
                    ));
                }
                Ok(None) => {
                    log_message(&format!(
                        "catalog descriptor miss for object_id={}: DD row not found",
                        table.id
                    ));
                }
                Err(err) => {
                    log_message(&format!(
                        "catalog descriptor decode failed for object_id={}: {err}",
                        table.id
                    ));
                }
            }
        }
        // v46 startup keeps every object header/name resident. A miss here is
        // therefore a real miss; probing a lazily-created DD B-tree would
        // perform physical initialization on a read path (and made temporary
        // table name probes emit WAL). Replication installs headers from its
        // CatalogDelta before publishing the commit.
        None
    }

    /// Resolve an important object without converting descriptor I/O errors to absence.
    fn table_by_id_checked(&self, table_id: u64) -> Result<Option<Arc<TableSchema>>, QueryError> {
        if let Some(table) = crate::storage::data_dictionary::dd_table_schema(table_id) {
            return Ok(Some(table));
        }
        if is_temp_table_id(table_id) {
            return Ok(self.temp_tables.read().map_err(|_| vector::persistence::corrupt())?
                .values().find(|table| table.id == table_id).cloned());
        }
        let header = self.catalog.read().map_err(|_| vector::persistence::corrupt())?.tables.values()
            .find(|table| table.id == table_id || table.storage_ids().contains(&table_id)).cloned();
        let Some(header) = header else { return Ok(None); };
        if !header.columns.is_empty() || header.system { return Ok(Some(header)); }
        let table = self.load_table_descriptor_from_dd(header.id)?.ok_or_else(vector::persistence::corrupt)?;
        self.catalog.install_descriptor(table_key(&table.database, &table.name), table.clone());
        Ok(Some(table))
    }

    fn table_by_id(&self, table_id: u64) -> Option<Arc<TableSchema>> {
        if let Some(table) = crate::storage::data_dictionary::dd_table_schema(table_id) {
            return Some(table);
        }
        if is_temp_table_id(table_id) {
            return self
                .temp_tables
                .read()
                .ok()?
                .values()
                .find(|table| table.id == table_id)
                .map(Arc::clone);
        }
        if let Some(key) = self.catalog.table_key_by_id(table_id) {
            let header = self.catalog.read().ok()?.tables.get(&key).map(Arc::clone)?;
            let still_owns_id = header.id == table_id
                || header
                    .partitioning
                    .as_ref()
                    .is_some_and(|partitioning| partitioning.owns_storage_id(table_id));
            if still_owns_id {
                if !header.columns.is_empty() || header.system {
                    return Some(header);
                }
                let table = self
                    .load_table_descriptor_from_dd(header.id)
                    .ok()
                    .flatten()?;
                self.catalog.install_descriptor(key, Arc::clone(&table));
                return Some(table);
            }
            // An in-memory DDL may have switched storage ownership before its
            // durable CatalogTxn publishes the delta. Repair the small ID
            // index from the affected live descriptors instead of trusting a
            // stale pre-swap mapping.
            let current =
                self.catalog
                    .read()
                    .ok()?
                    .tables
                    .iter()
                    .find_map(|(current_key, table)| {
                        (table.id == table_id
                            || table
                                .partitioning
                                .as_ref()
                                .is_some_and(|p| p.owns_storage_id(table_id)))
                        .then(|| (current_key.clone(), Arc::clone(table)))
                    });
            if let Some((current_key, table)) = current {
                self.catalog.register_table(&current_key, &table);
                return Some(table);
            }
        }
        // A newly prepared id has no published reverse-map entry yet. This
        // bounded slow path is used only inside DDL before CatalogTxn commit.
        if let Some((current_key, table)) =
            self.catalog
                .read()
                .ok()?
                .tables
                .iter()
                .find_map(|(current_key, table)| {
                    (table.id == table_id
                        || table
                            .partitioning
                            .as_ref()
                            .is_some_and(|p| p.owns_storage_id(table_id)))
                    .then(|| (current_key.clone(), Arc::clone(table)))
                })
        {
            self.catalog.register_table(&current_key, &table);
            return Some(table);
        }
        let table = self
            .load_table_descriptor_from_dd(table_id)
            .ok()
            .flatten()?;
        let key = table_key(&table.database, &table.name);
        self.catalog.install_descriptor(key, Arc::clone(&table));
        Some(table)
    }

    pub fn user_table_snapshots(&self) -> Result<Vec<TableSchema>, QueryError> {
        let headers = self
            .catalog_read()?
            .tables
            .values()
            .filter(|table| !table.system)
            .cloned()
            .collect::<Vec<_>>();
        let mut tables = Vec::with_capacity(headers.len());
        for header in headers {
            if !header.columns.is_empty() {
                tables.push((*header).clone());
            } else if let Some(table) = self.load_table_descriptor_from_dd(header.id)? {
                tables.push((*table).clone());
            }
        }
        Ok(tables)
    }

    pub fn create_view(
        &self,
        current_database: &str,
        name: &str,
        sql: &str,
        or_replace: bool,
        if_not_exists: bool,
        columns: Vec<crate::storage::schema::ViewColumnDef>,
        dependencies: Vec<crate::storage::schema::ViewDependency>,
        routine_dependencies: Vec<crate::storage::schema::ViewRoutineDependency>,
        algorithm: String,
        definer_user: String,
        definer_host: String,
        security_type: crate::storage::schema::RoutineSecuritySchema,
        check_option: Option<String>,
        is_updatable: bool,
        character_set_client: String,
        collation_connection: String,
        definition_normalized: String,
        query: crate::storage::schema::ViewQuery,
    ) -> Result<(), QueryError> {
        let (database, view_name) = split_table_name(current_database, name);
        let key = table_key(&database, &view_name);
        let mut next_version: u64 = 1;
        {
            let mut catalog = self.catalog_write()?;
            check_table_ddl_writable(&database)?;
            validate_mysql_identifier(&view_name)?;
            if !catalog.databases.contains_key(&database) {
                return Err(error(
                    crate::error::catalog::ER_BAD_DB_ERROR,
                    format!("unknown database '{database}'"),
                ));
            }
            if catalog.tables.contains_key(&key) || catalog.sequences.contains_key(&key) {
                return Err(error(
                    crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                    format!("relation '{name}' already exists; cannot create view with same name"),
                ));
            }
            if let Some(existing) = catalog.views.get(&key) {
                if or_replace {
                    next_version = existing.schema_version.saturating_add(1);
                    catalog.views.remove(&key);
                } else if if_not_exists {
                    crate::engine::api::push_statement_warning(
                        crate::error::ConditionLevel::Note,
                        crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                        format!("Table '{name}' already exists"),
                    );
                    return Ok(());
                } else {
                    return Err(error(
                        crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                        format!("view '{name}' already exists"),
                    ));
                }
            }
            catalog.views.insert(
                key,
                ViewSchema {
                    database: database.clone(),
                    name: view_name.clone(),
                    sql: sql.to_string(),
                    algorithm,
                    definer_user,
                    definer_host,
                    security_type,
                    columns,
                    dependencies,
                    routine_dependencies,
                    invalid: false,
                    schema_version: next_version,
                    check_option,
                    is_updatable,
                    character_set_client,
                    collation_connection,
                    definition_normalized,
                    query: Some(query),
                },
            );
        }
        self.note_catalog_view(&database, &view_name);
        self.commit_ddl_catalog_txn()
    }

    /// Mark every view that references `(database, target_name)` as invalid.
    /// Called by DROP TABLE / DROP VIEW / ALTER TABLE so dependent views fail
    /// loudly with 1356 on next access rather than silently producing wrong
    /// results. Returns the names of views that were invalidated for caller
    /// logging / warnings.
    pub fn invalidate_views_referencing(
        &self,
        database: &str,
        target_name: &str,
    ) -> Result<Vec<String>, QueryError> {
        let mut invalidated = Vec::new();
        let database_key = crate::storage::naming::normalize_name(database);
        let target_key = crate::storage::naming::normalize_name(target_name);
        {
            let mut catalog = self.catalog_write()?;
            let mut targets = std::collections::HashSet::from([(database_key, target_key)]);
            loop {
                let mut changed = false;
                for view in catalog.views.values_mut() {
                    let key = (normalize_name(&view.database), normalize_name(&view.name));
                    if targets.contains(&key) {
                        continue;
                    }
                    if view.dependencies.iter().any(|dep| {
                        targets
                            .contains(&(normalize_name(&dep.database), normalize_name(&dep.name)))
                    }) {
                        view.invalid = true;
                        targets.insert(key);
                        invalidated.push(format!("{}.{}", view.database, view.name));
                        changed = true;
                    }
                }
                if !changed {
                    break;
                }
            }
        }
        if !invalidated.is_empty() {
            for name in &invalidated {
                let (schema, view) = split_table_name(database, name);
                self.note_catalog_view(&schema, &view);
            }
            self.commit_ddl_catalog_txn()?;
        }
        Ok(invalidated)
    }

    pub fn drop_view(
        &self,
        current_database: &str,
        name: &str,
        if_exists: bool,
    ) -> Result<(), QueryError> {
        self.drop_views(current_database, &[name.to_owned()], if_exists)
    }

    /// Validate the complete set before mutating it, then persist removals and
    /// transitive invalidations together in one catalog transaction.
    pub fn drop_views(
        &self,
        current_database: &str,
        names: &[String],
        if_exists: bool,
    ) -> Result<(), QueryError> {
        let mut changed = Vec::new();
        {
            let mut catalog = self.catalog_write()?;
            let mut targets = std::collections::HashSet::new();
            for name in names {
                let (database, view_name) = split_table_name(current_database, name);
                let key = table_key(&database, &view_name);
                if catalog.tables.contains_key(&key) || catalog.sequences.contains_key(&key) {
                    return Err(error(
                        crate::error::catalog::ER_WRONG_OBJECT,
                        format!("'{database}.{view_name}' is not VIEW"),
                    ));
                }
                if !catalog.views.contains_key(&key) && !if_exists {
                    return Err(error(
                        crate::error::catalog::ER_BAD_TABLE_ERROR,
                        format!("unknown view '{name}'"),
                    ));
                }
                if !targets.insert((normalize_name(&database), normalize_name(&view_name))) {
                    return Err(error(
                        crate::error::catalog::ER_NONUNIQ_TABLE,
                        format!("Not unique table/alias: '{view_name}'"),
                    ));
                }
            }
            for (database, name) in &targets {
                if catalog.views.remove(&table_key(database, name)).is_some() {
                    changed.push((database.clone(), name.clone()));
                }
            }
            loop {
                let mut added = false;
                for view in catalog.views.values_mut() {
                    let key = (normalize_name(&view.database), normalize_name(&view.name));
                    if !targets.contains(&key)
                        && view.dependencies.iter().any(|dep| {
                            targets.contains(&(
                                normalize_name(&dep.database),
                                normalize_name(&dep.name),
                            ))
                        })
                    {
                        view.invalid = true;
                        targets.insert(key);
                        changed.push((view.database.clone(), view.name.clone()));
                        added = true;
                    }
                }
                if !added {
                    break;
                }
            }
        }
        for (database, name) in &changed {
            self.note_catalog_view(database, name);
        }
        if !changed.is_empty() {
            self.commit_ddl_catalog_txn()?;
        }
        Ok(())
    }

    pub fn view_definition(&self, current_database: &str, name: &str) -> Option<ViewSchema> {
        let (database, view_name) = split_table_name(current_database, name);
        let key = table_key(&database, &view_name);
        self.catalog.read().ok()?.views.get(&key).cloned()
    }

    pub fn create_routine(
        &self,
        current_database: &str,
        mut routine: RoutineSchema,
        or_replace: bool,
        if_not_exists: bool,
        creator: Option<&crate::engine::auth::AccountKey>,
        automatic_privileges: bool,
    ) -> Result<bool, QueryError> {
        let (database, name) = split_table_name(current_database, &routine.name);
        let key = routine_key(&database, routine.kind.lower(), &name);
        let routine_kind = routine.kind.lower().to_string();
        let now = current_unix_ms();
        let mut next_version = 1;
        {
            let mut catalog = self.catalog_write()?;
            if !catalog.databases.contains_key(&database) {
                return Err(error(
                    crate::error::catalog::ER_BAD_DB_ERROR,
                    format!("unknown database '{database}'"),
                ));
            }
            if let Some(existing) = catalog.routines.get(&key) {
                if !or_replace {
                    if if_not_exists {
                        return Ok(false);
                    }
                    return Err(error(
                        crate::error::catalog::ER_SP_ALREADY_EXISTS,
                        format!("{} {} already exists", routine.kind.as_str(), routine.name),
                    ));
                }
                next_version = existing.schema_version.saturating_add(1).max(1);
            }
            routine.database = database.clone();
            routine.name = name.clone();
            routine.schema_version = next_version;
            if routine.created_ms == 0 {
                routine.created_ms = now;
            }
            routine.updated_ms = now;
            catalog.routines.insert(key, Arc::new(routine));
            if automatic_privileges {
                if let Some(creator) = creator {
                    let store_charge =
                        crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                            crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                            catalog.auth.clone_capacity_bytes(),
                        )?;
                    let auth = Arc::make_mut(&mut catalog.auth);
                    auth.install_store_charge(store_charge);
                    let scope = crate::engine::auth::PrivilegeScope::Routine {
                        database: database.clone(),
                        routine: name.clone(),
                        kind: routine_kind.clone(),
                    };
                    let ctx = crate::engine::auth::AuthorizationContext {
                        authenticated_as: creator.clone(),
                        active_roles: Default::default(),
                        epoch: auth.epoch,
                    };
                    let mut changed = false;
                    for privilege in [
                        crate::engine::auth::Privilege::AlterRoutine,
                        crate::engine::auth::Privilege::Execute,
                    ] {
                        let required = crate::engine::auth::RequiredPrivilege {
                            privilege: privilege.clone(),
                            scope: scope.clone(),
                        };
                        if !auth.account_has_privilege(&ctx, &required, false) {
                            auth.grants.push(crate::engine::auth::GrantEntry {
                                updated_ms: crate::storage::naming::current_unix_ms(),
                                grantor: creator.clone(),
                                grantee: creator.clone(),
                                privilege,
                                scope: scope.clone(),
                                grant_option: false,
                                origin: crate::engine::auth::GrantOrigin::AutomaticRoutine,
                            });
                            changed = true;
                        }
                    }
                    if changed {
                        auth.rebuild_runtime_indexes();
                        auth.bump_epoch();
                    }
                }
            }
        }
        self.note_catalog_routine(&database, &routine_kind, &name);
        if automatic_privileges && creator.is_some() {
            self.note_catalog_auth();
        }
        self.commit_ddl_catalog_txn()?;
        if automatic_privileges && creator.is_some() {
            self.publish_auth_snapshot()?;
        }
        Ok(true)
    }

    pub fn alter_routine(
        &self,
        current_database: &str,
        kind: RoutineKindSchema,
        name: &str,
        mutate: impl FnOnce(&mut RoutineSchema),
    ) -> Result<(), QueryError> {
        let (database, routine_name) = split_table_name(current_database, name);
        let key = routine_key(&database, kind.lower(), &routine_name);
        {
            let mut catalog = self.catalog_write()?;
            let Some(routine) = catalog.routines.get_mut(&key) else {
                return Err(error(
                    crate::error::catalog::ER_SP_DOES_NOT_EXIST,
                    format!("{} {} does not exist", kind.as_str(), name),
                ));
            };
            let routine = Arc::make_mut(routine);
            mutate(routine);
            routine.bump_schema_version();
            routine.updated_ms = current_unix_ms();
        }
        self.note_catalog_routine(&database, kind.as_str(), &routine_name);
        self.commit_ddl_catalog_txn()
    }

    pub fn drop_routine(
        &self,
        current_database: &str,
        kind: RoutineKindSchema,
        name: &str,
        if_exists: bool,
    ) -> Result<bool, QueryError> {
        let (database, routine_name) = split_table_name(current_database, name);
        let key = routine_key(&database, kind.lower(), &routine_name);
        let mut invalidated_views = Vec::new();
        {
            let mut catalog = self.catalog_write()?;
            if catalog.routines.remove(&key).is_none() {
                if if_exists {
                    return Ok(false);
                }
                return Err(error(
                    crate::error::catalog::ER_SP_DOES_NOT_EXIST,
                    format!("{} {} does not exist", kind.as_str(), name),
                ));
            }
            let store_charge =
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                    catalog.auth.clone_capacity_bytes(),
                )?;
            let auth = Arc::make_mut(&mut catalog.auth);
            auth.install_store_charge(store_charge);
            let before = auth.grants.len();
            auth.grants.retain(|grant| {
                grant.origin != crate::engine::auth::GrantOrigin::AutomaticRoutine
                    || !matches!(
                        &grant.scope,
                        crate::engine::auth::PrivilegeScope::Routine {
                            database: grant_database,
                            routine,
                            kind: grant_kind,
                        } if grant_database.eq_ignore_ascii_case(&database)
                            && routine.eq_ignore_ascii_case(&routine_name)
                            && grant_kind.eq_ignore_ascii_case(kind.lower())
                    )
            });
            if auth.grants.len() != before {
                auth.rebuild_runtime_indexes();
                auth.bump_epoch();
            }
            for view in catalog.views.values_mut() {
                if view.invalid {
                    continue;
                }
                let depends = view.routine_dependencies.iter().any(|dep| {
                    dep.database.eq_ignore_ascii_case(&database)
                        && dep.name.eq_ignore_ascii_case(&routine_name)
                });
                if depends {
                    view.invalid = true;
                    invalidated_views.push((view.database.clone(), view.name.clone()));
                }
            }
        }
        self.note_catalog_routine(&database, kind.as_str(), &routine_name);
        self.note_catalog_auth();
        for (schema, view) in invalidated_views {
            self.note_catalog_view(&schema, &view);
        }
        self.commit_ddl_catalog_txn()?;
        self.publish_auth_snapshot()?;
        Ok(true)
    }

    pub fn routine_definition(
        &self,
        current_database: &str,
        kind: RoutineKindSchema,
        name: &str,
    ) -> Option<Arc<RoutineSchema>> {
        let (database, routine_name) = split_table_name(current_database, name);
        let key = routine_key(&database, kind.lower(), &routine_name);
        self.catalog.read().ok()?.routines.get(&key).cloned()
    }

    /// Fast negative gate for the stored-function AST rewrite. Most database
    /// instances define no SQL functions at all; in that case walking every
    /// literal in a bulk INSERT (twice, around view expansion) cannot possibly
    /// change the statement.
    pub fn has_stored_functions(&self) -> bool {
        self.catalog.read().is_ok_and(|catalog| {
            catalog
                .routines
                .values()
                .any(|routine| routine.kind == RoutineKindSchema::Function)
        })
    }

    pub fn routines_in_database(&self, database: &str) -> Vec<RoutineSchema> {
        let database_key = normalize_name(database);
        self.catalog
            .read()
            .ok()
            .map(|catalog| {
                catalog
                    .routines
                    .values()
                    .filter(|routine| normalize_name(&routine.database) == database_key)
                    .map(|routine| routine.as_ref().clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn create_trigger(
        &self,
        current_database: &str,
        mut trigger: TriggerSchema,
        if_not_exists: bool,
        order: Option<TriggerOrderSchema>,
    ) -> Result<bool, QueryError> {
        let (database, name) = split_table_name(current_database, &trigger.name);
        let (table_database, table_name) = split_table_name(current_database, &trigger.table_name);
        let key = trigger_key(&database, &name);
        let target_key = table_key(&table_database, &table_name);
        let now = current_unix_ms();
        let target_descriptor = self
            .table_by_key(&target_key)
            .map(|table| (table.id, table.schema_version));
        let mut changed_triggers = Vec::new();
        let target_table_id;
        let target_version;
        {
            let mut catalog = self.catalog_write()?;
            if !catalog.databases.contains_key(&database) {
                return Err(error(
                    crate::error::catalog::ER_BAD_DB_ERROR,
                    format!("unknown database '{database}'"),
                ));
            }
            // A2 — a trigger target must be a base table. Views and temporary
            // tables get MySQL's dedicated error codes instead of the generic
            // "table doesn't exist" (1146).
            if catalog.views.contains_key(&target_key) {
                return Err(error(
                    crate::error::catalog::ER_WRONG_OBJECT,
                    format!("'{}.{}' is not BASE TABLE", table_database, table_name),
                ));
            }
            if self
                .temp_tables
                .read()
                .map(|temp| temp.contains_key(&target_key))
                .unwrap_or(false)
            {
                return Err(error(
                    crate::error::catalog::ER_TRG_ON_VIEW_OR_TEMP_TABLE,
                    "Triggers cannot be created on a temporary table",
                ));
            }
            if !catalog.tables.contains_key(&target_key) {
                return Err(error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    format!("table '{}.{}' does not exist", table_database, table_name),
                ));
            }
            if database != table_database {
                return Err(error(
                    crate::error::catalog::ER_TRG_IN_WRONG_SCHEMA,
                    "Trigger in wrong schema",
                ));
            }
            if let Some(existing) = catalog.triggers.get(&key) {
                if if_not_exists {
                    if existing.table_database != table_database
                        || existing.table_name != table_name
                    {
                        return Err(error(
                            crate::error::catalog::ER_IF_NOT_EXISTS_UNSUPPORTED_TRG_EXISTS_ON_DIFFERENT_TABLE,
                            "Trigger already exists on a different table",
                        ));
                    }
                    return Ok(false);
                }
                return Err(error(
                    crate::error::catalog::ER_TRG_ALREADY_EXISTS,
                    "Trigger already exists",
                ));
            }

            trigger.database = database;
            trigger.name = name;
            trigger.table_database = table_database;
            trigger.table_name = table_name;
            trigger.schema_version = 1;
            if trigger.created_ms == 0 {
                trigger.created_ms = now;
            }
            // B5 — DATABASE_COLLATION is the collation of the schema the
            // trigger lives in (== its table's schema), not the session
            // connection collation.
            if let Some(db_schema) = catalog.databases.get(&trigger.database) {
                trigger.database_collation = db_schema.collation.clone();
            }

            let mut ordered = trigger_peer_keys(&catalog, &trigger);
            let insert_at = match order {
                Some(order) => {
                    let (ref_db, ref_name) =
                        split_table_name(&trigger.database, &order.other_trigger);
                    let ref_key = trigger_key(&ref_db, &ref_name);
                    let pos = ordered
                        .iter()
                        .position(|candidate| candidate == &ref_key)
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_REFERENCED_TRG_DOES_NOT_EXIST,
                                format!("Referenced trigger '{}' for the given action time and event type does not exist", order.other_trigger),
                            )
                        })?;
                    match order.kind {
                        TriggerOrderKindSchema::Follows => pos + 1,
                        TriggerOrderKindSchema::Precedes => pos,
                    }
                }
                None => ordered.len(),
            };
            ordered.insert(insert_at, key.clone());
            for (idx, peer_key) in ordered.iter().enumerate() {
                let action_order = idx as u32 + 1;
                if peer_key == &key {
                    trigger.action_order = action_order;
                } else if let Some(peer) = catalog.triggers.get_mut(peer_key) {
                    peer.action_order = action_order;
                    changed_triggers.push((peer.database.clone(), peer.name.clone()));
                }
            }
            changed_triggers.push((trigger.database.clone(), trigger.name.clone()));
            catalog.triggers.insert(key, trigger);
            if let Some(table_arc) = catalog.tables.get_mut(&target_key) {
                // A zero-size cache may leave only a lazy header in live.
                // Keep an owned version independently of descriptor eviction.
                let version = target_descriptor
                    .filter(|(id, _)| *id == table_arc.id)
                    .map_or(table_arc.schema_version, |(_, version)| version)
                    .max(table_arc.schema_version)
                    .saturating_add(1);
                Arc::make_mut(table_arc).schema_version = version;
                target_table_id = table_arc.id;
                target_version = version;
            } else {
                return Err(error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    "trigger target disappeared during catalog commit",
                ));
            }
        }
        for (schema, trigger) in changed_triggers {
            self.note_catalog_trigger(&schema, &trigger);
        }
        self.note_catalog_table_version(target_table_id, target_version);
        self.commit_ddl_catalog_txn()?;
        Ok(true)
    }

    pub fn drop_trigger(
        &self,
        current_database: &str,
        name: &str,
        if_exists: bool,
    ) -> Result<bool, QueryError> {
        let (database, trigger_name) = split_table_name(current_database, name);
        let key = trigger_key(&database, &trigger_name);
        let target_descriptor = self
            .trigger_definition(current_database, name)
            .and_then(|trigger| {
                self.table_by_key(&table_key(&trigger.table_database, &trigger.table_name))
            })
            .map(|table| (table.id, table.schema_version));
        let mut changed_triggers = Vec::new();
        let target_table_id;
        let target_version;
        {
            let mut catalog = self.catalog_write()?;
            let Some(removed) = catalog.triggers.remove(&key) else {
                if if_exists {
                    return Ok(false);
                }
                return Err(error(
                    crate::error::catalog::ER_TRG_DOES_NOT_EXIST,
                    format!("Trigger '{name}' does not exist"),
                ));
            };
            let ordered = trigger_peer_keys(&catalog, &removed);
            for (idx, peer_key) in ordered.iter().enumerate() {
                if let Some(peer) = catalog.triggers.get_mut(peer_key) {
                    peer.action_order = idx as u32 + 1;
                    changed_triggers.push((peer.database.clone(), peer.name.clone()));
                }
            }
            let target_key = table_key(&removed.table_database, &removed.table_name);
            if let Some(table_arc) = catalog.tables.get_mut(&target_key) {
                // A zero-size cache may leave only a lazy header in live.
                // Keep an owned version independently of descriptor eviction.
                let version = target_descriptor
                    .filter(|(id, _)| *id == table_arc.id)
                    .map_or(table_arc.schema_version, |(_, version)| version)
                    .max(table_arc.schema_version)
                    .saturating_add(1);
                Arc::make_mut(table_arc).schema_version = version;
                target_table_id = table_arc.id;
                target_version = version;
            } else {
                return Err(error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    "trigger target disappeared during catalog commit",
                ));
            }
        }
        self.note_catalog_trigger(&database, &trigger_name);
        for (schema, trigger) in changed_triggers {
            self.note_catalog_trigger(&schema, &trigger);
        }
        self.note_catalog_table_version(target_table_id, target_version);
        self.commit_ddl_catalog_txn()?;
        Ok(true)
    }

    pub fn trigger_definition(&self, current_database: &str, name: &str) -> Option<TriggerSchema> {
        let (database, trigger_name) = split_table_name(current_database, name);
        let key = trigger_key(&database, &trigger_name);
        self.catalog.read().ok()?.triggers.get(&key).cloned()
    }

    pub fn triggers_in_database(&self, database: &str) -> Vec<TriggerSchema> {
        let database_key = normalize_name(database);
        let mut triggers = self
            .catalog
            .read()
            .ok()
            .map(|catalog| {
                catalog
                    .triggers
                    .values()
                    .filter(|trigger| normalize_name(&trigger.database) == database_key)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        sort_triggers(&mut triggers);
        triggers
    }

    pub fn triggers_for_table_event(
        &self,
        table_name: &str,
        event: TriggerEventSchema,
        timing: TriggerTimingSchema,
    ) -> Vec<TriggerSchema> {
        let Ok(catalog) = self.catalog.read() else {
            return Vec::new();
        };
        // Take the read guard *before* building the lookup keys: every
        // DML statement probes this, and the overwhelmingly common
        // no-triggers case must not pay the `split_table_name` /
        // `normalize_name` string allocations just to scan an empty map.
        if catalog.triggers.is_empty() {
            return Vec::new();
        }
        let (database, table_name) = split_table_name(DEFAULT_DATABASE, table_name);
        let database_key = normalize_name(&database);
        let table_key_name = normalize_name(&table_name);
        let mut triggers = catalog
            .triggers
            .values()
            .filter(|trigger| {
                normalize_name(&trigger.table_database) == database_key
                    && normalize_name(&trigger.table_name) == table_key_name
                    && trigger.event == event
                    && trigger.timing == timing
            })
            .cloned()
            .collect::<Vec<_>>();
        drop(catalog);
        sort_triggers(&mut triggers);
        triggers
    }

    pub fn table_has_trigger_event(&self, table_name: &str, event: TriggerEventSchema) -> bool {
        let Ok(catalog) = self.catalog.read() else {
            return false;
        };
        // Same empty-map early exit as `triggers_for_table_event`.
        if catalog.triggers.is_empty() {
            return false;
        }
        let (database, table_name) = split_table_name(DEFAULT_DATABASE, table_name);
        let database_key = normalize_name(&database);
        let table_key_name = normalize_name(&table_name);
        catalog.triggers.values().any(|trigger| {
            normalize_name(&trigger.table_database) == database_key
                && normalize_name(&trigger.table_name) == table_key_name
                && trigger.event == event
        })
    }

    pub fn create_event(
        &self,
        current_database: &str,
        mut event: EventSchema,
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<bool, QueryError> {
        // The caller (event_schema_from_def) has already resolved the
        // schema from a possibly-qualified `db.name`, storing it in
        // `event.database`. Re-splitting against `current_database` would
        // discard that and mislocate a `CREATE EVENT other_db.evt` into
        // the current schema, so honour `event.database` as the default.
        let default_db = if event.database.is_empty() {
            current_database
        } else {
            event.database.as_str()
        };
        let (database, name) = split_table_name(default_db, &event.name);
        let key = event_key(&database, &name);
        let now = current_unix_ms();
        {
            let mut catalog = self.catalog_write()?;
            if !catalog.databases.contains_key(&database) {
                return Err(error(
                    crate::error::catalog::ER_BAD_DB_ERROR,
                    format!("unknown database '{database}'"),
                ));
            }
            event.database_collation = catalog.databases[&database].collation.clone();
            if let Some(existing) = catalog.events.get(&key) {
                if or_replace {
                    event.schema_version = existing.schema_version.saturating_add(1).max(1);
                    event.created_ms = existing.created_ms;
                } else if if_not_exists {
                    return Ok(false);
                } else {
                    // MySQL error 1537: Event '%s' already exists.
                    return Err(error(
                        crate::error::catalog::ER_EVENT_ALREADY_EXISTS,
                        format!("Event '{}' already exists", event.name),
                    ));
                }
            } else {
                event.schema_version = 1;
            }
            event.database = database.clone();
            event.name = name.clone();
            if event.created_ms == 0 {
                event.created_ms = now;
            }
            event.last_altered_ms = now;
            catalog.events.insert(key, event);
        }
        self.note_catalog_event(&database, &name);
        self.commit_ddl_catalog_txn()?;
        Ok(true)
    }

    pub fn alter_event(
        &self,
        current_database: &str,
        name: &str,
        rename: Option<String>,
        mutate: impl FnOnce(&mut EventSchema),
    ) -> Result<EventSchema, QueryError> {
        let (database, event_name) = split_table_name(current_database, name);
        let key = event_key(&database, &event_name);
        let now = current_unix_ms();
        let resolved = {
            let mut catalog = self.catalog_write()?;
            let mut event = catalog.events.get(&key).cloned().ok_or_else(|| {
                error(
                    crate::error::catalog::ER_EVENT_DOES_NOT_EXIST,
                    format!("Unknown event '{}'", name),
                )
            })?;
            mutate(&mut event);
            event.last_altered_ms = now;
            event.bump_schema_version();
            let final_key = if let Some(new_name) = rename {
                let (new_db, new_name) = split_table_name(current_database, &new_name);
                if !catalog.databases.contains_key(&new_db) {
                    return Err(error(
                        crate::error::catalog::ER_BAD_DB_ERROR,
                        format!("unknown database '{new_db}'"),
                    ));
                }
                let new_key = event_key(&new_db, &new_name);
                if new_key != key && catalog.events.contains_key(&new_key) {
                    return Err(error(
                        crate::error::catalog::ER_EVENT_ALREADY_EXISTS,
                        format!("Event '{}' already exists", new_name),
                    ));
                }
                event.database = new_db;
                event.name = new_name;
                new_key
            } else {
                key.clone()
            };
            catalog.events.remove(&key);
            catalog.events.insert(final_key.clone(), event.clone());
            event
        };
        self.note_catalog_event(&database, &event_name);
        self.note_catalog_event(&resolved.database, &resolved.name);
        self.commit_ddl_catalog_txn()?;
        Ok(resolved)
    }

    pub fn drop_event(
        &self,
        current_database: &str,
        name: &str,
        if_exists: bool,
    ) -> Result<bool, QueryError> {
        let (database, event_name) = split_table_name(current_database, name);
        let key = event_key(&database, &event_name);
        {
            let mut catalog = self.catalog_write()?;
            if catalog.events.remove(&key).is_none() {
                if if_exists {
                    return Ok(false);
                }
                return Err(error(
                    crate::error::catalog::ER_EVENT_DOES_NOT_EXIST,
                    format!("Unknown event '{}'", name),
                ));
            }
        }
        self.note_catalog_event(&database, &event_name);
        self.commit_ddl_catalog_txn()?;
        Ok(true)
    }

    pub fn event_definition(&self, current_database: &str, name: &str) -> Option<EventSchema> {
        let (database, event_name) = split_table_name(current_database, name);
        let key = event_key(&database, &event_name);
        self.catalog.read().ok()?.events.get(&key).cloned()
    }

    pub fn events_in_database(&self, database: &str) -> Vec<EventSchema> {
        let database_key = normalize_name(database);
        let mut events = self
            .catalog
            .read()
            .ok()
            .map(|catalog| {
                catalog
                    .events
                    .values()
                    .filter(|event| normalize_name(&event.database) == database_key)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        events.sort_by(|a, b| normalize_name(&a.name).cmp(&normalize_name(&b.name)));
        events
    }

    pub fn all_events(&self) -> Vec<EventSchema> {
        let mut events = self
            .catalog
            .read()
            .ok()
            .map(|catalog| catalog.events.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        events.sort_by(|a, b| {
            normalize_name(&a.database)
                .cmp(&normalize_name(&b.database))
                .then_with(|| normalize_name(&a.name).cmp(&normalize_name(&b.name)))
        });
        events
    }

    /// Persist a `last_executed_ms` watermark advanced by the event
    /// scheduler. Runtime state does not change the definition version used
    /// in scheduler claim keys; workers re-read the catalog. Returns the updated row.
    pub fn record_event_run(
        &self,
        database: &str,
        name: &str,
        expected_version: u64,
        executed_ms: u64,
    ) -> Result<Option<EventSchema>, QueryError> {
        let key = event_key(database, name);
        let snapshot = {
            let mut catalog = self.catalog_write()?;
            let Some(event) = catalog.events.get_mut(&key) else {
                return Ok(None);
            };
            if event.schema_version != expected_version {
                return Ok(None);
            }
            event.last_executed_ms = event.last_executed_ms.max(executed_ms);
            Some(event.clone())
        };
        self.note_catalog_event(database, name);
        self.commit_ddl_catalog_txn()?;
        Ok(snapshot)
    }

    #[cfg(test)]
    /// Set the event status (used by the scheduler when an event
    /// exhausts its ENDS window with ON COMPLETION PRESERVE). Like the run
    /// watermark, this runtime update preserves the definition version.
    pub fn set_event_status(
        &self,
        database: &str,
        name: &str,
        status: EventStatusSchema,
    ) -> Result<Option<EventSchema>, QueryError> {
        let key = event_key(database, name);
        let snapshot = {
            let mut catalog = self.catalog_write()?;
            let Some(event) = catalog.events.get_mut(&key) else {
                return Ok(None);
            };
            event.status = status;
            event.last_altered_ms = current_unix_ms();
            Some(event.clone())
        };
        self.note_catalog_event(database, name);
        self.commit_ddl_catalog_txn()?;
        Ok(snapshot)
    }

    /// Complete only the definition observed by the scheduler. The version
    /// check and mutation share the catalog lock with ALTER EVENT.
    pub fn complete_event_if_version(&self, expected: &EventSchema) -> Result<bool, QueryError> {
        let key = event_key(&expected.database, &expected.name);
        {
            let mut catalog = self.catalog_write()?;
            let Some(event) = catalog.events.get_mut(&key) else {
                return Ok(false);
            };
            if event.schema_version != expected.schema_version {
                return Ok(false);
            }
            if event.on_completion == crate::storage::EventOnCompletionSchema::Preserve {
                if event.status != EventStatusSchema::Enabled {
                    return Ok(false);
                }
                event.status = EventStatusSchema::Disabled;
                event.last_altered_ms = current_unix_ms();
            } else {
                catalog.events.remove(&key);
            }
        }
        self.note_catalog_event(&expected.database, &expected.name);
        self.commit_ddl_catalog_txn()?;
        Ok(true)
    }

    pub fn auth_catalog(&self) -> std::sync::Arc<crate::engine::auth::AuthCatalog> {
        self.auth_snapshot.load_full()
    }

    pub fn auth_epoch(&self) -> Result<u64, QueryError> {
        Ok(self.auth_snapshot.load().epoch)
    }

    pub fn with_auth_catalog<R>(
        &self,
        f: impl FnOnce(&crate::engine::auth::AuthCatalog) -> R,
    ) -> Result<R, QueryError> {
        let snapshot = self.auth_snapshot.load();
        Ok(f(&snapshot))
    }

    pub(crate) fn publish_auth_snapshot(&self) -> Result<(), QueryError> {
        let auth = {
            let mut catalog = self.catalog_write()?;
            if !catalog.auth.store_is_charged() {
                let charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                    catalog.auth.clone_capacity_bytes(),
                )?;
                std::sync::Arc::make_mut(&mut catalog.auth).install_store_charge(charge);
            }
            catalog.auth.clone()
        };
        self.auth_snapshot.store(auth);
        crate::engine::auth::invalidate_authorization_cache();
        Ok(())
    }

    pub(crate) fn initialize_auth_snapshot(&self) -> Result<(), QueryError> {
        let auth = {
            let mut catalog = self.catalog_write()?;
            let auth = std::sync::Arc::make_mut(&mut catalog.auth);
            let charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                auth.clone_capacity_bytes(),
            )?;
            auth.install_store_charge(charge);
            catalog.auth.clone()
        };
        self.auth_snapshot.store(auth);
        crate::engine::auth::invalidate_authorization_cache();
        Ok(())
    }

    pub fn mutate_auth_catalog<F>(&self, f: F) -> Result<(), QueryError>
    where
        F: FnOnce(&mut crate::engine::auth::AuthCatalog) -> Result<(), QueryError>,
    {
        static AUTH_MUTATION_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> =
            std::sync::OnceLock::new();
        let _auth_mutation = AUTH_MUTATION_LOCK
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "auth mutation lock poisoned",
                )
            })?;
        let store_bytes = self.with_auth_catalog(|auth| auth.clone_capacity_bytes())?;
        let mut store_charge = Some(
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                store_bytes,
            )?,
        );
        let _delta_scratch_charge =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                store_bytes,
            )?;
        // Candidates stay private until DD/WAL commit succeeds. Concurrent
        // DDL must not serialize an uncommitted auth candidate from Catalog.
        let snapshot = self.auth_catalog();
        let mut candidate = (*snapshot).clone();
        candidate.grants.begin_mutation();
        candidate.role_edges.begin_mutation();
        candidate.default_roles.begin_mutation();
        candidate.restrictions.begin_mutation();
        f(&mut candidate)?;
        crate::engine::cancel::check_cancelled()?;
        candidate.grants.reconcile_bytes(&snapshot.grants);
        candidate.role_edges.reconcile_bytes(&snapshot.role_edges);
        candidate
            .default_roles
            .reconcile_bytes(&snapshot.default_roles);
        candidate
            .restrictions
            .reconcile_bytes(&snapshot.restrictions);
        candidate.refresh_runtime_indexes(Some(&snapshot));
        for mandatory in candidate.mandatory_role_keys() {
            if candidate.account_is_system_user(&mandatory) {
                return Err(error(crate::error::catalog::ER_CANNOT_GRANT_SYSTEM_PRIV_TO_MANDATORY_ROLE,
                    format!("Mandatory role {} cannot acquire SYSTEM_USER directly or through another role",mandatory.display())));
            }
        }
        // Account for growth before publishing or writing durable DD rows.
        // A rejected admission leaves the old authority intact.
        let candidate_bytes = candidate.clone_capacity_bytes();
        let lease = store_charge.as_mut().expect("reserved auth store charge");
        if candidate_bytes > lease.bytes() {
            lease.try_grow(candidate_bytes - lease.bytes())?;
        } else {
            lease.shrink(lease.bytes() - candidate_bytes);
        }
        candidate.bump_epoch();
        candidate.install_store_charge(store_charge.take().expect("reserved auth store charge"));
        let candidate = std::sync::Arc::new(candidate);
        #[cfg(test)]
        crate::engine::auth::test_faults::hit("before_dd")?;
        crate::engine::cancel::check_cancelled()?;
        self.commit_auth_catalog_delta(&snapshot, &candidate)?;
        #[cfg(test)]
        crate::engine::auth::test_faults::hit("after_durable")?;
        match self.catalog_write() {
            Ok(mut catalog) => catalog.auth = candidate.clone(),
            Err(err) => {
                // The commit is durable; never pretend it rolled back. Publish
                // the matching authority and fail-stop all further execution.
                self.auth_snapshot.store(candidate);
                crate::engine::auth::invalidate_authorization_cache();
                self.quarantine_mvcc("auth catalog publication failed after durable commit");
                return Err(err);
            }
        }
        self.auth_snapshot.store(candidate);
        crate::engine::auth::invalidate_authorization_cache();
        #[cfg(test)]
        crate::engine::auth::test_faults::hit("after_publish")?;
        Ok(())
    }

    pub fn is_system_table_name(&self, current_database: &str, name: &str) -> bool {
        let (database, _) = split_table_name(current_database, name);
        is_system_database(&database)
    }

    #[allow(dead_code)]
    pub fn table_required(
        &self,
        current_database: &str,
        name: &str,
    ) -> Result<Arc<TableSchema>, QueryError> {
        let (database, table) = split_table_name(current_database, name);
        self.table_in_database(&database, &table)
    }
}

fn trigger_peer_keys(catalog: &Catalog, trigger: &TriggerSchema) -> Vec<String> {
    let table_database = normalize_name(&trigger.table_database);
    let table_name = normalize_name(&trigger.table_name);
    let mut keys = catalog
        .triggers
        .iter()
        .filter(|(_, existing)| {
            normalize_name(&existing.table_database) == table_database
                && normalize_name(&existing.table_name) == table_name
                && existing.timing == trigger.timing
                && existing.event == trigger.event
        })
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    keys.sort_by_key(|key| {
        catalog
            .triggers
            .get(key)
            .map(|trigger| trigger.action_order)
            .unwrap_or(u32::MAX)
    });
    keys
}

fn sort_triggers(triggers: &mut [TriggerSchema]) {
    triggers.sort_by(|left, right| {
        (
            normalize_name(&left.database),
            normalize_name(&left.table_name),
            left.event.lower(),
            left.timing.lower(),
            left.action_order,
            normalize_name(&left.name),
        )
            .cmp(&(
                normalize_name(&right.database),
                normalize_name(&right.table_name),
                right.event.lower(),
                right.timing.lower(),
                right.action_order,
                normalize_name(&right.name),
            ))
    });
}
