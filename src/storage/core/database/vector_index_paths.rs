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
        let memory = self.vector_index_path(table, &index.name, storage_id);
        let paged_path = match index.vector_algo() {
            VectorAlgo::Hnsw | VectorAlgo::Ivf => {
                self.vector_index_paged_path(table, &index.name, storage_id)
            }
            VectorAlgo::IvfSq8 => {
                self.vector_index_paged_path_ext(table, &index.name, storage_id, "ivf")
            }
        };
        let conflict = if paged {
            // Memory IVF currently has no durable sidecar, so there is no
            // conflicting artifact to detect in that direction.
            index.vector_algo() == VectorAlgo::Hnsw && memory.exists()
        } else {
            paged_path.exists()
        };
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
            self.vector_index_paged_path_ext(table, index_name, storage_id, "ivf"),
        ]
    }

    /// Remove all vector backend files for one index/partition. Buffer-pool
    /// frames are discarded first so delayed writeback cannot resurrect them.
    fn remove_partition_vector_files(
        &self,
        table: &TableSchema,
        storage_id: u64,
        index_name: &str,
    ) {
        self.remove_partition_vector_files_for_parts(
            &table.database,
            &table.name,
            table.id,
            storage_id,
            index_name,
        );
    }

    fn remove_partition_vector_files_for_parts(
        &self,
        database: &str,
        table_name: &str,
        table_id: u64,
        storage_id: u64,
        index_name: &str,
    ) {
        let table = self.table_by_id(table_id);
        let generation = table
            .as_ref()
            .map(|table| table.active_generation.max(1))
            .unwrap_or(1);
        let index_id = table
            .and_then(|table| {
                table
                    .indexes
                    .iter()
                    .find(|index| index.name.eq_ignore_ascii_case(index_name))
                    .map(|index| index.index_id)
            })
            .unwrap_or_else(|| stable_missing_index_id(index_name));
        for ext in ["hnsw", "hvec", "ivf"] {
            let _ = (database, table_name);
            let path = self.vector_index_path_for_parts(storage_id, generation, index_id, ext);
            let _ = self.buffer_pool.discard_pages_for_path(&path);
            let _ = fs::remove_file(path);
        }
    }
}
