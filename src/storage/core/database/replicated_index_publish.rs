const REPLICATED_CATALOG_MDL_OWNER: u64 = u64::MAX;

struct ReplicatedPrivateDefinitionsGuard<'a> {
    db: &'a Database,
    storage_ids: Vec<u64>,
}

impl Drop for ReplicatedPrivateDefinitionsGuard<'_> {
    fn drop(&mut self) {
        self.db.unregister_reorg_storage_locs(&self.storage_ids);
    }
}

impl Database {
    /// Close old cache owners while MDL still protects the old definition.
    /// Once the new catalog is visible, retirement maintenance may unlink
    /// these immutable paths, so no later handle drop may flush into them.
    fn retire_replicated_index_handles(
        &self,
        tables: &[(u64, Option<Arc<TableSchema>>)],
        old_tables: &[Arc<TableSchema>],
    ) -> Result<(), QueryError> {
        for old in old_tables {
            let new = tables
                .iter()
                .find(|(id, _)| *id == old.id)
                .and_then(|(_, table)| table.as_ref());
            for index in &old.indexes {
                for sid in old.storage_ids() {
                    let retained = new.is_some_and(|table| {
                        table.active_generation == old.active_generation
                            && table.storage_ids().contains(&sid)
                            && table
                                .indexes
                                .iter()
                                .any(|candidate| candidate.index_id == index.index_id)
                    });
                    if retained {
                        continue;
                    }
                    self.remove_index_cache_entry(sid, &index.name)?;
                    if index.is_vector() {
                        self.vector_indexes.write().retain(
                            |(table, storage, generation, name), _| {
                                !(*table == old.id
                                    && *storage == sid
                                    && *generation == old.active_generation.max(1)
                                    && *name == normalize_name(&index.name))
                            },
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// DD row visibility and derived-index materialization share one barrier.
    /// A client may observe only the complete old or complete new definition.
    fn lock_replicated_catalog(
        &self,
        requests: Vec<MdlRequest>,
    ) -> Result<OnlineIndexMdlGuard<'_>, QueryError> {
        let guard = OnlineIndexMdlGuard {
            db: self,
            owner: REPLICATED_CATALOG_MDL_OWNER,
        };
        let started = Instant::now();
        loop {
            match self.acquire_mdl(
                REPLICATED_CATALOG_MDL_OWNER,
                None,
                MdlDuration::Transaction,
                requests.clone(),
                "repl/catalog-publish",
            ) {
                Ok(()) => return Ok(guard),
                Err(err)
                    if is_lock_wait_pending_error(&err)
                        && started.elapsed() < self.config.lock_wait =>
                {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn materialize_replicated_indexes(
        &self,
        tables: &[(u64, Option<Arc<TableSchema>>)],
        old_states: &HashMap<(u64, u64), (IndexState, String)>,
        old_tables: &[Arc<TableSchema>],
    ) -> Result<(), QueryError> {
        let _definitions = ReplicatedPrivateDefinitionsGuard {
            db: self,
            storage_ids: tables
                .iter()
                .filter_map(|(_, table)| table.as_ref())
                .flat_map(|table| table.storage_ids())
                .collect(),
        };
        // ADD PRIMARY KEY can prebuild before the public descriptor changes.
        // A cold handle must still resolve every candidate index identity.
        for (_, table) in tables {
            if let Some(table) = table {
                self.register_reorg_storage_definition(&table.storage_ids(), table);
            }
        }
        for (_, table) in tables {
            let Some(table) = table else {
                continue;
            };
            let old_table = old_tables.iter().find(|old| old.id == table.id);
            let old_storage = old_table
                .map(|old| old.storage_ids().into_iter().collect::<HashSet<_>>())
                .unwrap_or_default();
            for id in table.storage_ids() {
                self.set_cached_heap_codec(id, table.row_codec.clone())?;
                for index in table
                    .indexes
                    .iter()
                    .filter(|i| i.state == IndexState::Public)
                {
                    let old = old_states.get(&(table.id, index.index_id));
                    if let Some((IndexState::Public, old_name)) = old.filter(|_| {
                        old_storage.contains(&id)
                            && old_table
                                .is_some_and(|old| old.active_generation == table.active_generation)
                    }) {
                        if !old_name.eq_ignore_ascii_case(&index.name) {
                            let mut cache = self.index_cache.write();
                            if let Some(handle) = cache.remove(&(id, normalize_name(old_name))) {
                                cache.insert((id, normalize_name(&index.name)), handle);
                            }
                        }
                        continue;
                    }
                    let mut leaf = table.as_ref().clone();
                    leaf.id = id;
                    leaf.active_generation = self.storage_generation(id);
                    let path = self.index_path_for_table_schema(&leaf, id, &index.name);
                    if self.config.durability == DurabilityMode::Memory {
                        let minimum = 16 * 1024 * 1024;
                        let (_lease, _charge, scratch) =
                            reserve_offline_index_build_scratch(self, minimum, minimum)?;
                        let entries = self
                            .collect_sorted_index_entries_with_degree(&leaf, index, 1, scratch)?;
                        let stream: Box<dyn Iterator<Item = Result<IndexEntry, QueryError>>> =
                            match entries {
                                SortedIndexEntries::Memory(entries) => {
                                    Box::new(entries.into_iter().map(Ok))
                                }
                                SortedIndexEntries::Spilled(runs) => Box::new(runs.into_merger()?),
                            };
                        let stream: Box<dyn Iterator<Item = Result<IndexEntry, QueryError>>> =
                            if index.unique {
                                Box::new(UniqueCheckedIndexEntries::new(stream, index))
                            } else {
                                stream
                            };
                        let file = IndexFile::create_rebuilt_from_sorted_stream_with_durability(
                            path,
                            stream,
                            self.log.clone(),
                            self.config.durability,
                            self.buffer_pool.clone(),
                        )?;
                        file.flush()?;
                        self.index_cache.write().insert(
                            (id, normalize_name(&index.name)),
                            Arc::new(parking_lot::RwLock::new(file)),
                        );
                    } else {
                        self.rebuild_index_offline_bulk(&leaf, index)?;
                    }
                }
            }
        }
        Ok(())
    }
}
