// Single source of truth for vector-index file naming and lifecycle.
// Included into `storage/core.rs`.

impl Database {
    /// Refuse to silently reinterpret an existing vector index through a
    /// different storage backend. Vector backend files are format-specific;
    /// changing `MYDB_VECTOR_STORAGE` requires an explicit DROP+CREATE so an
    /// operator cannot accidentally orphan a large graph and trigger a cold
    /// rebuild under a new format.
    fn ensure_vector_backend_compatible(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
        paged: bool,
    ) -> Result<(), QueryError> {
        let manifest = self.read_vector_manifest(table, index, storage_id)?;
        let conflict = manifest.is_some_and(|manifest| manifest.resident == paged);
        if conflict {
            return Err(crate::storage::error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!(
                    "vector index `{}` was created with the {} backend; DROP and CREATE the index before using the {} backend",
                    index.name,
                    if paged { "memory" } else { "paged" },
                    if paged { "paged" } else { "memory" }
                ),
            ));
        }
        Ok(())
    }

    fn vector_index_path_for_parts(
        &self,
        storage_id: u64,
        generation: u64,
        index_id: u64,
        ext: &str,
    ) -> PathBuf {
        crate::storage::naming::index_object_path_by_id(
            &self.data_dir,
            storage_id,
            generation,
            index_id,
            ext,
        )
    }

    fn vector_index_path(&self, table: &TableSchema, index_name: &str, storage_id: u64) -> PathBuf {
        let index_id = table
            .indexes
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case(index_name))
            .map(|index| index.index_id)
            .unwrap_or_else(|| stable_missing_index_id(index_name));
        self.vector_index_path_for_parts(
            storage_id,
            table.active_generation.max(1),
            index_id,
            "hnsw",
        )
    }

    fn vector_index_paged_path(
        &self,
        table: &TableSchema,
        index_name: &str,
        storage_id: u64,
    ) -> PathBuf {
        self.vector_index_paged_path_ext(table, index_name, storage_id, "hvec")
    }

    fn vector_index_paged_path_ext(
        &self,
        table: &TableSchema,
        index_name: &str,
        storage_id: u64,
        ext: &str,
    ) -> PathBuf {
        let index_id = table
            .indexes
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case(index_name))
            .map(|index| index.index_id)
            .unwrap_or_else(|| stable_missing_index_id(index_name));
        self.vector_index_path_for_parts(storage_id, table.active_generation.max(1), index_id, ext)
    }

    fn vector_file_paths(
        &self,
        table: &TableSchema,
        index_name: &str,
        storage_id: u64,
    ) -> [PathBuf; 3] {
        [
            self.vector_index_path(table, index_name, storage_id),
            self.vector_index_paged_path(table, index_name, storage_id),
            self.vector_index_paged_path(table, index_name, storage_id)
                .with_extension("vmanifest"),
        ]
    }

    fn owned_vector_files(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        storage_id: u64,
    ) -> Result<Vec<PathBuf>, QueryError> {
        self.owned_vector_files_parts(storage_id, table.active_generation.max(1), index.index_id)
    }

    fn owned_vector_files_parts(
        &self,
        storage_id: u64,
        generation: u64,
        index_id: u64,
    ) -> Result<Vec<PathBuf>, QueryError> {
        let mut paths = ["hnsw", "hvec", "vmanifest"]
            .map(|ext| self.vector_index_path_for_parts(storage_id, generation, index_id, ext))
            .into_iter()
            .collect::<BTreeSet<_>>();
        if let Some(manifest) = self.read_vector_manifest_parts(storage_id, generation, index_id)? {
            for (owner, physical) in
                std::iter::once((manifest.physical_storage_id, manifest.active))
                    .chain(manifest.retired)
            {
                let mut path = self
                    .vector_index_path_for_parts(owner, generation, index_id, "hvec")
                    .parent()
                    .unwrap()
                    .join(format!("{index_id}.vector"))
                    .join(format!("{physical}.hvec"));
                if manifest.resident {
                    path.set_extension("hnsw");
                }
                paths.insert(path.with_extension(format!(
                    "{}.vpub",
                    path.extension().unwrap().to_string_lossy()
                )));
                paths.insert(path);
            }
        }
        // Include interrupted private publications owned by this exact index.
        let directory = self
            .vector_index_path_for_parts(storage_id, generation, index_id, "hvec")
            .parent()
            .unwrap()
            .join(format!("{index_id}.vector"));
        match fs::read_dir(directory) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry.map_err(io_err)?;
                    if !entry.file_type().map_err(io_err)?.is_file() {
                        return Err(vector::persistence::corrupt());
                    }
                    paths.insert(entry.path());
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(io_err(err)),
        }
        paths.insert(self.vector_index_path_for_parts(
            storage_id,
            generation,
            index_id,
            "vector.failed",
        ));
        paths.insert(self.vector_index_path_for_parts(
            storage_id,
            generation,
            index_id,
            "vmaintenance",
        ));
        Ok(paths.into_iter().collect())
    }

    /// Remove all vector backend files for one index/partition. Buffer-pool
    /// frames are discarded first so delayed writeback cannot resurrect them.
    fn remove_partition_vector_files(
        &self,
        table: &TableSchema,
        storage_id: u64,
        index_name: &str,
    ) -> Result<(), QueryError> {
        let Some(index) = table
            .indexes
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case(index_name))
        else {
            return Ok(());
        };
        let paths = self.owned_vector_files(table, index, storage_id)?;
        let mut last = 0;
        if self.durability_mode() != DurabilityMode::Memory && !is_temp_table_id(storage_id) {
            for path in &paths {
                last = self.append_wal(
                    0,
                    WalPayload::FileDelete {
                        path: self.relative_path(path),
                        before: Vec::new(),
                    },
                )?;
            }
            crate::wal::LogManager::sync_durable_to(&self.log, last)?;
        }
        for path in paths {
            self.buffer_pool.discard_pages_for_path(&path)?;
            match fs::remove_file(&path) {
                Ok(()) => crate::platform::fs::sync_path(path.parent().unwrap()).map_err(io_err)?,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(io_err(err)),
            }
        }
        Ok(())
    }
}
