// Native vector file generations keep readers on their original immutable identity.

struct VectorGenerationManifest {
    storage_id: u64,
    generation: u64,
    index_id: u64,
    physical_storage_id: u64,
    active: u64,
    resident: bool,
    lineage: u64,
    source_lsn: u64,
    retired: Vec<(u64, u64)>,
}

impl VectorGenerationManifest {
    fn encode(&self) -> Result<Vec<u8>, QueryError> {
        let mut bytes = Vec::new();
        let mut writer = vector::persistence::Writer::new(&mut bytes, 7).map_err(io_err)?;
        for id in [
            self.storage_id,
            self.generation,
            self.index_id,
            self.physical_storage_id,
            self.active,
        ] {
            writer.u64(id).map_err(io_err)?;
        }
        writer.u8(u8::from(self.resident)).map_err(io_err)?;
        writer.u64(self.lineage).map_err(io_err)?;
        writer.u64(self.source_lsn).map_err(io_err)?;
        writer.count(self.retired.len()).map_err(io_err)?;
        for (owner, id) in &self.retired {
            writer.u64(*owner).map_err(io_err)?;
            writer.u64(*id).map_err(io_err)?;
        }
        writer.finish().map_err(io_err)?;
        Ok(bytes)
    }
    fn decode(
        bytes: &[u8],
        storage_id: u64,
        generation: u64,
        index_id: u64,
    ) -> Result<Self, QueryError> {
        let mut reader = vector::persistence::Reader::new(bytes, 7)?;
        if (reader.u64()?, reader.u64()?, reader.u64()?) != (storage_id, generation, index_id) {
            return Err(vector::persistence::corrupt());
        }
        let physical_storage_id = reader.u64()?;
        let active = reader.u64()?;
        let resident = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(vector::persistence::corrupt()),
        };
        let lineage = reader.u64()?;
        let source_lsn = reader.u64()?;
        let count = reader.count(16)?;
        if physical_storage_id == 0 || active == 0 || count > 64 {
            return Err(vector::persistence::corrupt());
        }
        let mut retired = Vec::with_capacity(count);
        let mut seen = BTreeSet::new();
        for _ in 0..count {
            let owner = reader.u64()?;
            let id = reader.u64()?;
            if owner == 0
                || id == 0
                || (owner, id) == (physical_storage_id, active)
                || !seen.insert((owner, id))
            {
                return Err(vector::persistence::corrupt());
            }
            retired.push((owner, id));
        }
        reader.finish()?;
        Ok(Self {
            storage_id,
            generation,
            index_id,
            physical_storage_id,
            active,
            resident,
            lineage,
            source_lsn,
            retired,
        })
    }
}

impl Database {
    fn vector_generation_path(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
        physical: u64,
    ) -> PathBuf {
        self.vector_index_paged_path(table, &index.name, storage_id)
            .parent()
            .expect("index directory")
            .join(format!("{}.vector", index.index_id))
            .join(format!("{physical}.hvec"))
    }
    fn vector_manifest_path(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> PathBuf {
        self.vector_index_paged_path(table, &index.name, storage_id)
            .with_extension("vmanifest")
    }
    fn read_vector_manifest(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<Option<VectorGenerationManifest>, QueryError> {
        self.read_vector_manifest_parts(storage_id, table.active_generation.max(1), index.index_id)
    }
    fn read_vector_manifest_parts(
        &self,
        storage_id: u64,
        generation: u64,
        index_id: u64,
    ) -> Result<Option<VectorGenerationManifest>, QueryError> {
        let path = self.vector_index_path_for_parts(storage_id, generation, index_id, "vmanifest");
        let file = match fs::File::open(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_err(err)),
        };
        if file.metadata().map_err(io_err)?.len() > 2048 {
            return Err(vector::persistence::corrupt());
        }
        let mut bytes = Vec::new();
        file.take(2049).read_to_end(&mut bytes).map_err(io_err)?;
        Ok(Some(VectorGenerationManifest::decode(
            &bytes, storage_id, generation, index_id,
        )?))
    }
    fn active_native_vector_path(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<(PathBuf, u64), QueryError> {
        let manifest = self
            .read_vector_manifest(table, index, storage_id)?
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_INDEX_CORRUPT,
                    format!("missing native vector manifest for {}", index.name),
                )
            })?;
        if manifest.resident {
            return Err(vector::persistence::corrupt());
        }
        Ok((
            self.vector_generation_path(
                table,
                index,
                manifest.physical_storage_id,
                manifest.active,
            ),
            manifest.physical_storage_id,
        ))
    }
    fn allocate_vector_physical_id(&self) -> u64 {
        if self.physical_replica_mode()
            || matches!(
                self.open_mode,
                DatabaseOpenMode::SealBaseline
                    | DatabaseOpenMode::RestoreWorkspace
                    | DatabaseOpenMode::ReadOnlyImage
            )
        {
            self.next_replica_local_txn_id
                .fetch_add(1, Ordering::AcqRel)
        } else {
            self.allocate_nontransaction_id()
        }
    }
    fn allocate_native_vector_path(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<PathBuf, QueryError> {
        if self.durability_mode() == DurabilityMode::Memory {
            return Ok(self.vector_generation_path(
                table,
                index,
                storage_id,
                self.allocate_vector_physical_id(),
            ));
        }
        for _ in 0..1024 {
            let physical = self.allocate_vector_physical_id();
            let path = self.vector_generation_path(table, index, storage_id, physical);
            let resident = path.with_extension("hnsw");
            let paged_receipt = path.with_extension("hvec.vpub");
            let resident_receipt = path.with_extension("hnsw.vpub");
            if !path.try_exists().map_err(io_err)?
                && !resident.try_exists().map_err(io_err)?
                && !paged_receipt.try_exists().map_err(io_err)?
                && !resident_receipt.try_exists().map_err(io_err)?
            {
                let parent = path.parent().expect("vector generation directory");
                fs::create_dir_all(parent).map_err(io_err)?;
                crate::platform::fs::sync_path(parent.parent().expect("index directory"))
                    .map_err(io_err)?;
                return Ok(path);
            }
        }
        Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "cannot allocate a fresh native vector generation",
        ))
    }
    fn write_vector_manifest(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        manifest: &VectorGenerationManifest,
    ) -> Result<(), QueryError> {
        let path = self.vector_manifest_path(table, index, manifest.storage_id);
        let bytes = manifest.encode()?;
        let tmp = path.with_extension("vmanifest.tmp");
        {
            let mut file = fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp)
                .map_err(io_err)?;
            file.write_all(&bytes).map_err(io_err)?;
            file.sync_all().map_err(io_err)?;
        }
        fs::rename(&tmp, &path).map_err(io_err)?;
        #[cfg(test)]
        self.vector_persist_failpoint(5)?;
        crate::platform::fs::sync_path(path.parent().expect("index directory")).map_err(io_err)
    }
    fn active_vector_snapshot_path(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<(PathBuf, u64), QueryError> {
        let manifest = self
            .read_vector_manifest(table, index, storage_id)?
            .ok_or_else(vector::persistence::corrupt)?;
        if !manifest.resident {
            return Err(vector::persistence::corrupt());
        }
        Ok((
            self.vector_generation_path(
                table,
                index,
                manifest.physical_storage_id,
                manifest.active,
            )
            .with_extension("hnsw"),
            manifest.physical_storage_id,
        ))
    }
    fn publish_vector_source_manifest(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        manifest: &VectorGenerationManifest,
    ) -> Result<(), QueryError> {
        let path = self.vector_manifest_path(table, index, manifest.storage_id);
        let lsn = self.append_wal(
            0,
            WalPayload::VectorIndexManifest {
                path: self.relative_path(&path),
                bytes: manifest.encode()?,
            },
        )?;
        crate::wal::LogManager::sync_durable_to(&self.log, lsn)?;
        self.write_vector_manifest(table, index, manifest)
    }

    fn log_vector_initial_image(&self, path: &Path) -> Result<u64, QueryError> {
        let mut file = fs::File::open(path).map_err(io_err)?;
        let total = file.metadata().map_err(io_err)?.len();
        if total == 0 {
            return Err(vector::persistence::corrupt());
        }
        let _charge =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_recoverable(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                2 * 1024 * 1024 + 1024,
            )?;
        let mut bytes = vec![0; 1024 * 1024];
        let mut offset = 0;
        let mut last = 0;
        while offset < total {
            let count = (total - offset).min(bytes.len() as u64) as usize;
            file.read_exact(&mut bytes[..count]).map_err(io_err)?;
            last = self.append_wal(
                0,
                WalPayload::VectorIndexImage {
                    path: self.relative_path(path),
                    offset,
                    total,
                    bytes: bytes[..count].to_vec(),
                },
            )?;
            offset += count as u64;
        }
        crate::wal::LogManager::sync_durable_to(&self.log, last)?;
        persist_vector_image_receipt(path, last)?;
        Ok(last)
    }

    fn publish_native_vector_generation(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
        graph: &VectorGraph,
        old: Option<&Arc<VectorGraph>>,
        prepared: Option<VectorGenerationManifest>,
    ) -> Result<(), QueryError> {
        if self.durability_mode() == DurabilityMode::Memory {
            return Ok(());
        }
        let file = match graph {
            VectorGraph::NativeHnsw(native) => native.file(),
            VectorGraph::NativeIvf(native) => native.file(),
            VectorGraph::Paged(paged) => Some(paged.file()),
            _ => None,
        };
        let Some(file) = file else {
            let mut manifest = prepared.ok_or_else(vector::persistence::corrupt)?;
            let path = self
                .vector_generation_path(table, index, manifest.physical_storage_id, manifest.active)
                .with_extension("hnsw");
            manifest.source_lsn = self.log_vector_initial_image(&path)?;
            self.publish_vector_source_manifest(table, index, &manifest)
                .map_err(|err| {
                    self.vector_maintenance_error(
                        table,
                        index,
                        storage_id,
                        "resident generation publication",
                        err,
                    )
                })?;
            self.cleanup_native_vector_generations(table, index, storage_id)?;
            return Ok(());
        };
        let path = file.persistent_path();
        let physical = path
            .file_stem()
            .and_then(|value| value.to_str())
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(vector::persistence::corrupt)?;
        let mut retired = Vec::new();
        if let Some(manifest) = self.read_vector_manifest(table, index, storage_id)? {
            retired = manifest.retired;
            if manifest.active != physical {
                retired.push((manifest.physical_storage_id, manifest.active));
            }
        }
        if retired.len() > 64 {
            return Err(error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "native vector retirement backlog exceeded its bound",
            ));
        }
        file.flush()?;
        let source_lsn = self.log_vector_initial_image(path)?;
        crate::platform::fs::sync_path(path.parent().expect("vector generation directory"))
            .map_err(io_err)?;
        self.publish_vector_source_manifest(
            table,
            index,
            &VectorGenerationManifest {
                storage_id,
                generation: table.active_generation.max(1),
                index_id: index.index_id,
                physical_storage_id: file.meta().storage_id,
                active: physical,
                resident: false,
                lineage: graph.identity_lineage(),
                source_lsn,
                retired,
            },
        )
        .map_err(|err| {
            self.vector_maintenance_error(table, index, storage_id, "generation publication", err)
        })?;
        if let Some(old) = old {
            let old_file = match old.as_ref() {
                VectorGraph::NativeHnsw(native) => native.file(),
                VectorGraph::NativeIvf(native) => native.file(),
                VectorGraph::Paged(paged) => Some(paged.file()),
                _ => None,
            };
            if let Some(old_file) = old_file {
                self.vector_retired_graphs.lock().insert(
                    old_file.persistent_path().to_path_buf(),
                    Arc::downgrade(old),
                );
            }
        }
        Ok(())
    }
    fn cleanup_native_vector_generations(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<(), QueryError> {
        if self.is_snapshot_read_only() {
            return Ok(());
        }
        let Some(mut manifest) = self.read_vector_manifest(table, index, storage_id)? else {
            return Ok(());
        };
        if self.physical_replica_mode() && !manifest.resident {
            return Ok(());
        }
        let mut waiting = Vec::new();
        let mut changed = false;
        for (owner, physical) in &manifest.retired {
            let mut path = self.vector_generation_path(table, index, *owner, *physical);
            if manifest.resident {
                path.set_extension("hnsw");
            }
            if self
                .vector_retired_graphs
                .lock()
                .get(&path)
                .is_some_and(|graph| graph.strong_count() != 0)
            {
                waiting.push((*owner, *physical));
                continue;
            }
            if !manifest.resident {
                let lsn = self.append_wal(
                    0,
                    WalPayload::FileDelete {
                        path: self.relative_path(&path),
                        before: Vec::new(),
                    },
                )?;
                crate::wal::LogManager::sync_durable_to(&self.log, lsn)?;
            }
            self.buffer_pool.discard_pages_for_path(&path)?;
            match fs::remove_file(&path) {
                Ok(()) => {
                    crate::platform::fs::sync_path(
                        path.parent().expect("vector generation directory"),
                    )
                    .map_err(io_err)?;
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(io_err(err)),
            }
            self.vector_retired_graphs.lock().remove(&path);
            changed = true;
        }
        // Resident files have no outstanding physical page redo. Keep the
        // completed retirement entries until the next publication prunes them,
        // avoiding a second durable manifest replacement on every checkpoint.
        if changed && !manifest.resident {
            manifest.retired = waiting;
            self.write_vector_manifest(table, index, &manifest)?;
        }
        Ok(())
    }
}

impl Database {
    fn publish_copied_vector_owners(
        &self,
        table: &TableSchema,
        shadow_id: u64,
        target_id: u64,
        _graphs: &[(String, Arc<VectorGraph>)],
    ) -> Result<(), QueryError> {
        if self.durability_mode() == DurabilityMode::Memory {
            return Ok(());
        }
        for index in table.indexes.iter().filter(|index| index.is_vector()) {
            let mut manifest = self
                .read_vector_manifest(table, index, shadow_id)?
                .ok_or_else(vector::persistence::corrupt)?;
            manifest.storage_id = target_id;
            self.publish_vector_source_manifest(table, index, &manifest)?;
        }
        Ok(())
    }

    fn record_vector_maintenance_job_error(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
        err: QueryError,
    ) -> QueryError {
        if self.durability_mode() == DurabilityMode::Memory {
            return err;
        }
        let path = self
            .vector_index_paged_path(table, &index.name, storage_id)
            .with_extension("vmaintenance");
        let message = err.message().chars().take(16384).collect::<String>();
        let persist = (|| -> Result<(), QueryError> {
            if self.vector_job_failed(table, index, storage_id)? {
                return Ok(());
            }
            let bytes = serde_json::to_vec(
                &serde_json::json!({ "state": "FAILED", "storage_id": storage_id,
                "generation": table.active_generation.max(1), "index_id": index.index_id,
                "failed_unix_ms": current_unix_ms(), "error": message }),
            )
            .map_err(|_| vector::persistence::corrupt())?;
            let root = self.data_dir.join("system/vector-maintenance.jobs");
            let root_missing = !root.try_exists().map_err(io_err)?;
            if root_missing {
                self.append_wal(
                    0,
                    WalPayload::FileCreate {
                        path: self.relative_path(&root),
                        after: Vec::new(),
                    },
                )?;
            }
            let lsn = self.append_wal(
                0,
                WalPayload::FileCreate {
                    path: self.relative_path(&path),
                    after: bytes.clone(),
                },
            )?;
            crate::wal::LogManager::sync_durable_to(&self.log, lsn)?;
            if root_missing {
                write_recovered_file(&self.data_dir, &self.relative_path(&root), &[])?;
            }
            let tmp = path.with_extension("vmaintenance.tmp");
            let mut file = fs::File::create(&tmp).map_err(io_err)?;
            file.write_all(&bytes).map_err(io_err)?;
            file.sync_all().map_err(io_err)?;
            fs::rename(tmp, &path).map_err(io_err)?;
            crate::platform::fs::sync_path(path.parent().expect("index directory")).map_err(io_err)
        })();
        match persist {
            Ok(()) => err,
            Err(persist_err) => {
                self.quarantine_mvcc(format!(
                    "could not persist vector maintenance failure: {persist_err:?}"
                ));
                err.map_message(|message| {
                    format!(
                        "{message}; could not persist failed maintenance status: {}",
                        persist_err.message()
                    )
                })
            }
        }
    }
    fn clear_vector_maintenance_job_error(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<(), QueryError> {
        let path = self
            .vector_index_paged_path(table, &index.name, storage_id)
            .with_extension("vmaintenance");
        if !path.try_exists().map_err(io_err)? {
            return Ok(());
        }
        let lsn = self.append_wal(
            0,
            WalPayload::FileDelete {
                path: self.relative_path(&path),
                before: Vec::new(),
            },
        )?;
        crate::wal::LogManager::sync_durable_to(&self.log, lsn)?;
        match fs::remove_file(&path) {
            Ok(()) => crate::platform::fs::sync_path(path.parent().expect("index directory"))
                .map_err(io_err),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(io_err(err)),
        }
    }
    pub(crate) fn fail_vector_maintenance_worker(&self) {
        let message = "vector index maintenance panicked; database is fail-stopped";
        self.quarantine_mvcc(message);
        if self.durability_mode() == DurabilityMode::Memory {
            return;
        }
        let path = self.data_dir.join("system/index-maintenance.failed");
        let result = (|| -> std::io::Result<()> {
            let mut file = match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => file,
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
                Err(err) => return Err(err),
            };
            file.write_all(message.as_bytes())?;
            file.sync_all()?;
            crate::platform::fs::sync_path(path.parent().expect("system directory"))
        })();
        if let Err(err) = result {
            log_message(format!("failed to persist vector worker failure: {err}"));
        }
    }
}

impl Database {
    fn resume_failed_vector_jobs(&self) -> Result<(), QueryError> {
        if !self
            .data_dir
            .join("system/vector-maintenance.jobs")
            .try_exists()
            .map_err(io_err)?
        {
            return Ok(());
        }
        let mut directories = vec![self.data_dir.join("objects/indexes")];
        while let Some(directory) = directories.pop() {
            let entries = match fs::read_dir(directory) {
                Ok(entries) => entries,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(io_err(err)),
            };
            for entry in entries {
                let entry = entry.map_err(io_err)?;
                let kind = entry.file_type().map_err(io_err)?;
                if kind.is_dir() {
                    directories.push(entry.path());
                    continue;
                }
                if !kind.is_file()
                    || entry.path().extension().and_then(|ext| ext.to_str()) != Some("vmaintenance")
                {
                    continue;
                }
                let path = entry.path();
                let relative = self.relative_path(&path);
                let (_, info) = crate::storage::naming::parse_relative_object_path(&relative)
                    .ok_or_else(vector::persistence::corrupt)?;
                let Some(table) = self.table_by_id_checked(info.table_id)? else {
                    continue;
                };
                let generation = relative
                    .split('/')
                    .nth(3)
                    .and_then(|value| value.parse::<u64>().ok())
                    .ok_or_else(vector::persistence::corrupt)?;
                if generation != table.active_generation.max(1) {
                    continue;
                }
                let Some(index) = table.indexes.iter().find(|index| {
                    index.is_vector() && Some(index.index_id) == info.part.parse().ok()
                }) else {
                    continue;
                };
                if self.vector_job_failed(&table, index, info.table_id)? {
                    self.vector_graph_get_or_build_for_partition(&table, index, info.table_id)?;
                }
            }
        }
        Ok(())
    }
}

fn vector_persistence_io(err: std::io::Error) -> QueryError {
    if let Some(error) = err
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<QueryError>())
    {
        return error.clone();
    }
    if err.kind() == std::io::ErrorKind::InvalidData {
        error(
            crate::error::catalog::ER_INDEX_CORRUPT,
            format!("corrupt persistent vector snapshot: {err}"),
        )
    } else {
        io_err(err)
    }
}

impl Database {
    fn vector_job_failed(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<bool, QueryError> {
        let path = self
            .vector_index_paged_path(table, &index.name, storage_id)
            .with_extension("vmaintenance");
        let file = match fs::File::open(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(io_err(err)),
        };
        if file.metadata().map_err(io_err)?.len() > 128 * 1024 {
            return Err(vector::persistence::corrupt());
        }
        let mut bytes = Vec::new();
        file.take(128 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(io_err)?;
        let record: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|_| vector::persistence::corrupt())?;
        if record["state"].as_str() != Some("FAILED")
            || record["storage_id"].as_u64() != Some(storage_id)
            || record["generation"].as_u64() != Some(table.active_generation.max(1))
            || record["index_id"].as_u64() != Some(index.index_id)
            || record["error"].as_str().is_none_or(str::is_empty)
        {
            return Err(vector::persistence::corrupt());
        }
        Ok(true)
    }
}
