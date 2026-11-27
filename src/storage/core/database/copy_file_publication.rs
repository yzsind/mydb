// COPY bindings are prepared while the next physical generation is unreachable.
// Its private WAL history is complete before FilePublish/CommitTxn, so recovery
// does not depend on a source-local hard link or an operation intent file.
struct CopyPublicationGuard<'a> {
    db: &'a Database,
    job: u64,
    files: BTreeMap<PathBuf, File>,
    pending: bool,
    index_publication_txn: Option<u64>,
    charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}
impl<'a> CopyPublicationGuard<'a> {
    fn new(db: &'a Database, job: u64) -> Result<Self, QueryError> {
        let charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::DdlCopy,
            0,
        )?;
        db.copy_publication_wal
            .lock()
            .insert(job, db.log_state_atomics.next_lsn());
        Ok(Self {
            db,
            job,
            files: BTreeMap::new(),
            pending: false,
            index_publication_txn: None,
            charge,
        })
    }
    fn retain_file(&mut self, path: &Path) -> Result<(), QueryError> {
        if self.files.contains_key(path) {
            return Ok(());
        }
        match File::open(path) {
            Ok(file) => {
                self.charge.try_grow(512 + path.as_os_str().len())?;
                self.files.insert(path.to_owned(), file);
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(io_err(err)),
        }
        Ok(())
    }
    fn retain_storage(&mut self, table: &TableSchema, storage: u64) -> Result<(), QueryError> {
        let db = self.db;
        self.retain_file(&db.table_path(storage))?;
        self.retain_file(&db.overflow_path(storage))?;
        for index in &table.indexes {
            self.retain_file(&db.index_path(storage, &index.name))?;
            if index.is_vector() {
                for path in db.owned_vector_files(table, index, storage)? {
                    self.retain_file(&path)?;
                }
            }
        }
        Ok(())
    }
    fn sync(&mut self) -> Result<(), QueryError> {
        if !self.pending {
            return Ok(());
        }
        self.db.ensure_operational()?;
        #[cfg(test)]
        if self.db.copy_sync_failure.swap(false, Ordering::AcqRel) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "injected COPY deferred sync failure",
            ));
        }
        #[cfg(target_os = "macos")]
        let mut batch = crate::platform::fs::DeviceSyncBatch::new();
        for (path, file) in &self.files {
            #[cfg(target_os = "macos")]
            batch
                .sync_file(file.try_clone().map_err(io_err)?, path)
                .map_err(io_err)?;
            #[cfg(not(target_os = "macos"))]
            crate::platform::fs::sync_file_at(file, path).map_err(io_err)?;
        }
        #[cfg(target_os = "macos")]
        batch.finish().map_err(io_err)?;
        if let Some(txn_id) = self.index_publication_txn {
            let lsn = self
                .db
                .append_wal(0, WalPayload::FilePublishComplete { txn_id })?;
            crate::wal::LogManager::sync_durable_to(&self.db.log, lsn)?;
        }
        self.pending = false;
        Ok(())
    }
}
impl Drop for CopyPublicationGuard<'_> {
    fn drop(&mut self) {
        if !self.pending {
            self.db.copy_publication_wal.lock().remove(&self.job);
        } else {
            // The files can already be Public. Never advance checkpoint past
            // unsynced data after an I/O failure; restart replays the retained WAL.
            self.db
                .quarantine_mvcc("COPY deferred file sync requires recovery");
        }
    }
}

struct PreparedCopyFiles {
    swap: ShadowGenerationFileSwap,
    expected: Vec<(PathBuf, PathBuf)>,
}
impl PreparedCopyFiles {
    fn new(db: &Database, old: &TableSchema, new: &TableSchema, shadow: u64, job: u64) -> Self {
        let (old_to_backup, expected) = db.shadow_generation_swap_paths(old, new, shadow, job);
        Self {
            swap: ShadowGenerationFileSwap {
                in_place_schema: None,
                old_to_backup,
                shadow_to_live: Vec::new(),
            },
            expected,
        }
    }
    fn has_unbound(&self) -> Result<bool, QueryError> {
        for (source, _) in &self.expected {
            if !self
                .swap
                .shadow_to_live
                .iter()
                .any(|(bound, _)| bound == source)
                && source.try_exists().map_err(io_err)?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
    fn prepare(
        &mut self,
        db: &Database,
        guard: &mut CopyPublicationGuard<'_>,
    ) -> Result<(), QueryError> {
        for (source, target) in &self.expected {
            if self
                .swap
                .shadow_to_live
                .iter()
                .any(|(bound, _)| bound == source)
            {
                continue;
            }
            if !source.try_exists().map_err(io_err)? {
                continue;
            }
            if target.try_exists().map_err(io_err)? {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "COPY target generation already exists",
                ));
            }
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).map_err(io_err)?;
            }
            hard_link_owned_file(source, target).map_err(io_err)?;
            self.swap
                .shadow_to_live
                .push((source.clone(), target.clone()));
        }
        db.sync_shadow_generation_directories(&self.swap)?;
        for (old, _) in &self.swap.old_to_backup {
            guard.retain_file(old)?;
        }
        for (source, _) in &self.swap.shadow_to_live {
            guard.retain_file(source)?;
        }
        Ok(())
    }
    fn log(&self, db: &Database, txn: u64) -> Result<(), QueryError> {
        for (source, target) in &self.swap.shadow_to_live {
            if source.extension().is_some_and(|ext| ext == "vmanifest") {
                continue;
            }
            let source = db.relative_path(source);
            let target = db.relative_path(target);
            crate::wal::validate_file_publication(&source, &target)?;
            db.append_wal(txn, WalPayload::FilePublish { source, target })?;
        }
        Ok(())
    }
}

impl Database {
    fn log_copy_file_bindings(
        &self,
        txn: u64,
        files: &PreparedCopyFiles,
    ) -> Result<(), QueryError> {
        files.log(self, txn)
    }
    fn flush_copy_pages_without_sync(&self, table: &TableSchema) -> Result<(), QueryError> {
        self.flush_copy_storage_pages_without_sync(table, table.id)
    }
    fn flush_copy_storage_pages_without_sync(
        &self,
        table: &TableSchema,
        storage: u64,
    ) -> Result<(), QueryError> {
        let heap = self.heap_handle(storage)?;
        self.buffer_pool.flush_file_pages(&heap.read().file_key)?;
        for index in &table.indexes {
            self.with_index_read(storage, &index.name, |file| file.flush_without_sync())?;
        }
        if self.overflow_path(storage).exists() {
            let overflow = self.overflow_handle(storage)?;
            self.buffer_pool
                .flush_file_pages(&overflow.read().file_key)?;
        }
        for index in table.indexes.iter().filter(|index| index.is_vector()) {
            let graph = self
                .vector_indexes
                .read()
                .get(&(
                    table.id,
                    storage,
                    table.active_generation.max(1),
                    normalize_name(&index.name),
                ))
                .cloned();
            if let Some(graph) = graph {
                let file = match graph.as_ref() {
                    VectorGraph::Paged(g) => Some(g.file()),
                    VectorGraph::NativeHnsw(g) => g.file(),
                    VectorGraph::NativeIvf(g) => g.file(),
                    _ => None,
                };
                if let Some(file) = file {
                    file.flush_meta(0)?;
                    file.flush_without_sync()?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
impl Database {
    pub(crate) fn install_copy_file_stage_test_hook(&self, phase: u8) -> Arc<DdlBackfillTestHook> {
        let hook = Arc::new(DdlBackfillTestHook::new());
        *self.copy_file_stage_test_hook.lock().unwrap() = Some((phase, hook.clone()));
        hook
    }
    pub(crate) fn fail_copy_deferred_sync_for_test(&self) {
        self.copy_sync_failure.store(true, Ordering::Release);
    }
    pub(crate) fn copy_publication_floor_for_test(&self) -> Option<u64> {
        self.copy_publication_wal.lock().values().copied().min()
    }
    fn copy_file_test_point(&self, phase: u8) -> Result<(), QueryError> {
        let hook = {
            let mut hooks = self.copy_file_stage_test_hook.lock().unwrap();
            if hooks.as_ref().is_some_and(|(p, _)| *p == phase) {
                hooks.take().map(|(_, hook)| hook)
            } else {
                None
            }
        };
        if let Some(hook) = hook {
            hook.pause();
        }
        if std::env::var("MYDB_COPY_PUBLISH_CRASH")
            .ok()
            .and_then(|v| v.parse::<u8>().ok())
            == Some(phase)
        {
            crate::wal::LogManager::sync_durable_to(
                &self.log,
                self.log_state_atomics.next_lsn().saturating_sub(1),
            )?;
            std::process::exit(86);
        }
        Ok(())
    }
}

#[cfg(test)]
mod copy_file_recovery_tests {
    use super::*;
    #[test]
    fn copy_file_publication_missing_file_undo_has_no_directory_sync_obligation() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let io = RecoveryIoScope::enter().unwrap();
        apply_file_compensation(
            dir.path(),
            "objects/indexes/9/2/1.idx",
            &crate::wal::FileCompensation::Delete,
        )
        .unwrap();
        io.finish().unwrap();
        assert!(!dir.path().join("objects").exists());
    }
}
