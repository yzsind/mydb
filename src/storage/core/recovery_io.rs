// Startup-only, bounded page coalescing and verified LSN metadata. Each file
// partition has one owner; online replication never enables this cache.
thread_local! {
    static RECOVERY_PAGE_IO: RefCell<Option<RecoveryPageIo>> = const { RefCell::new(None) };
    static RECOVERY_DIRTY_FILES: RefCell<Option<RecoveryDirtyFiles>> = const { RefCell::new(None) };
}

struct RecoveryDirectorySync {
    paths: BTreeSet<PathBuf>,
    charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    #[cfg(test)]
    fail_next: bool,
}

impl RecoveryDirectorySync {
    fn add(&mut self, paths: BTreeSet<PathBuf>) -> Result<(), QueryError> {
        for path in paths {
            if self.paths.contains(&path) {
                continue;
            }
            if self.paths.len() >= 64 {
                self.sync()?;
            }
            self.charge.try_grow(512 + path.capacity())?;
            self.paths.insert(path);
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<(), QueryError> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "injected recovery directory sync failure",
            ));
        }
        let mut levels: BTreeMap<usize, Vec<&PathBuf>> = BTreeMap::new();
        for path in &self.paths {
            levels
                .entry(path.components().count())
                .or_default()
                .push(path);
        }
        // Every data file is synchronized before its parents are registered.
        // Preserve child-before-parent ordering while coalescing shared roots.
        for paths in levels.into_values().rev() {
            let workers = std::thread::available_parallelism()
                .map_or(1, |n| n.get())
                .min(8)
                .min(paths.len());
            if workers <= 1 {
                for path in paths {
                    crate::platform::fs::sync_path(path).map_err(io_err)?;
                }
                continue;
            }
            let next = std::sync::atomic::AtomicUsize::new(0);
            std::thread::scope(|scope| -> Result<(), QueryError> {
                let mut handles = Vec::with_capacity(workers);
                for _ in 0..workers {
                    let paths = &paths;
                    let next = &next;
                    handles.push(scope.spawn(move || -> Result<(), QueryError> {
                        while let Some(path) = paths.get(next.fetch_add(1, Ordering::Relaxed)) {
                            crate::platform::fs::sync_path(path).map_err(io_err)?;
                        }
                        Ok(())
                    }));
                }
                for handle in handles {
                    handle.join().map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "directory sync worker panicked",
                        )
                    })??;
                }
                Ok(())
            })?;
        }
        self.paths.clear();
        self.charge.shrink(self.charge.bytes());
        Ok(())
    }
}

struct RecoveryDirtyFiles {
    paths: BTreeSet<PathBuf>,
    charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    root: Option<PathBuf>,
    directories: Arc<parking_lot::Mutex<RecoveryDirectorySync>>,
}
impl RecoveryDirtyFiles {
    fn sync(&mut self) -> Result<(), QueryError> {
        let mut directories = BTreeSet::new();
        for path in &self.paths {
            match File::open(path) {
                Ok(file) => file.sync_data().map_err(io_err)?,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(io_err(err)),
            }
            let mut parent = path.parent();
            while let Some(dir) =
                parent.filter(|dir| self.root.as_ref().is_some_and(|root| dir.starts_with(root)))
            {
                directories.insert(dir.to_path_buf());
                parent = dir.parent();
            }
        }
        let _admission = crate::engine::executor::mem_budget::StorageLatchAdmissionScope::enter();
        self.directories.lock().add(directories)?;
        self.paths.clear();
        self.root = None;
        let bytes = self.charge.bytes();
        self.charge.shrink(bytes);
        Ok(())
    }
}
struct RecoveryIoScope {
    previous_pages: Option<RecoveryPageIo>,
    previous_dirty: Option<RecoveryDirtyFiles>,
    directories: Arc<parking_lot::Mutex<RecoveryDirectorySync>>,
    coordinator: bool,
}
impl RecoveryIoScope {
    fn enter() -> Result<Self, QueryError> {
        Self::enter_with_directories(
            Arc::new(parking_lot::Mutex::new(RecoveryDirectorySync {
                paths: BTreeSet::new(),
                charge: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                    0,
                )?,
                #[cfg(test)]
                fail_next: false,
            })),
            true,
        )
    }

    fn enter_with_directories(
        directories: Arc<parking_lot::Mutex<RecoveryDirectorySync>>,
        coordinator: bool,
    ) -> Result<Self, QueryError> {
        let dirty = RecoveryDirtyFiles {
            paths: BTreeSet::new(),
            root: None,
            directories: Arc::clone(&directories),
            charge: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                0,
            )?,
        };
        let previous_dirty = RECOVERY_DIRTY_FILES.with(|slot| slot.replace(Some(dirty)));
        let Some(charge) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
                16 * 1024 * 1024,
            )
        else {
            return Ok(Self {
                previous_pages: RECOVERY_PAGE_IO.with(|slot| slot.replace(None)),
                previous_dirty,
                directories,
                coordinator,
            });
        };
        let state = RecoveryPageIo {
            root: PathBuf::new(),
            relative: String::new(),
            file: None,
            len: 0,
            lsns: HashMap::new(),
            cached_page: None,
            cached_dirty_lsn: None,
            undo_log: None,
            _charge: charge,
        };
        Ok(Self {
            previous_pages: RECOVERY_PAGE_IO.with(|slot| slot.replace(Some(state))),
            previous_dirty,
            directories,
            coordinator,
        })
    }
    fn finish(&self) -> Result<(), QueryError> {
        RECOVERY_PAGE_IO.with(|slot| {
            if let Some(io) = slot.borrow_mut().as_mut() {
                io.flush_cached_page()?;
            }
            Ok::<_, QueryError>(())
        })?;
        RECOVERY_DIRTY_FILES.with(|slot| {
            if let Some(dirty) = slot.borrow_mut().as_mut() {
                dirty.sync()?;
            }
            Ok::<_, QueryError>(())
        })?;
        if self.coordinator {
            self.directories.lock().sync()?;
        }
        Ok(())
    }
}

fn note_recovery_write(root: &Path, path: PathBuf) -> Result<(), QueryError> {
    RECOVERY_DIRTY_FILES.with(|slot| {
        if let Some(dirty) = slot.borrow_mut().as_mut() {
            if dirty.paths.contains(&path) {
                return Ok(());
            }
            // Bound registry and sync scratch independently of recovery size.
            if dirty.paths.len() >= 64 || dirty.root.as_ref().is_some_and(|prior| prior != root) {
                dirty.sync()?;
            }
            let mut bytes = 512 + path.capacity();
            let mut parent = path.parent();
            while let Some(dir) = parent.filter(|dir| dir.starts_with(root)) {
                bytes += 512 + dir.as_os_str().len();
                parent = dir.parent();
            }
            if dirty.root.is_none() {
                bytes += root.as_os_str().len();
            }
            dirty.charge.try_grow(bytes)?;
            if dirty.root.is_none() {
                dirty.root = Some(root.to_path_buf());
            }
            dirty.paths.insert(path);
        }
        Ok(())
    })
}

/// An unverified page header is never a redo skip oracle.
fn recovery_page_valid(bytes: &[u8]) -> bool {
    bytes.len() == PAGE_SIZE
        && matches!(
            read_u32(bytes, 0),
            HEAP_PAGE_MAGIC
                | INDEX_PAGE_MAGIC
                | spatial_index::MAGIC
                | INDEX_META_MAGIC
                | OVFL_MAGIC
                | VEC_META_MAGIC
                | VEC_NODE_MAGIC
                | zone_map_page::MAGIC
        )
        && read_u32(bytes, 16) == page_checksum(bytes, 16)
}

impl Drop for RecoveryIoScope {
    fn drop(&mut self) {
        RECOVERY_PAGE_IO.with(|slot| {
            slot.replace(self.previous_pages.take());
        });
        RECOVERY_DIRTY_FILES.with(|slot| {
            slot.replace(self.previous_dirty.take());
        });
    }
}
struct RecoveryPageIo {
    root: PathBuf,
    relative: String,
    file: Option<File>,
    len: u64,
    lsns: HashMap<u64, u64>,
    cached_page: Option<(u64, Vec<u8>)>,
    cached_dirty_lsn: Option<u64>,
    undo_log: Option<Arc<crate::wal::LogMutex>>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}
impl RecoveryPageIo {
    fn open(&mut self, root: &Path, relative: &str) -> Result<(), QueryError> {
        if self.root == root && self.relative == relative {
            return Ok(());
        }
        self.flush_cached_page()?;
        self.root = root.to_owned();
        self.relative = relative.to_owned();
        self.lsns.clear();
        self.cached_page = None;
        self.len = 0;
        self.file = match OpenOptions::new()
            .read(true)
            .write(true)
            .open(root.join(relative))
        {
            Ok(file) => {
                self.len = file.metadata().map_err(io_err)?.len();
                Some(file)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(io_err(err)),
        };
        Ok(())
    }
    fn remember(&mut self, page_id: u64, lsn: u64) {
        // Bound a large-file scan as well as a random OLTP working set. Retain
        // capacity after clearing; the reservation covers the hash allocation.
        if self.lsns.len() >= 262144 {
            self.lsns.clear();
        }
        self.lsns.insert(page_id, lsn);
    }
    fn lsn(&mut self, page_id: u64) -> Result<u64, QueryError> {
        if let Some(lsn) = self.lsns.get(&page_id) {
            return Ok(*lsn);
        }
        let bytes = self.read(page_id)?;
        Ok(if recovery_page_valid(&bytes) {
            page_lsn(&bytes)
        } else {
            0
        })
    }
    fn read(&mut self, page_id: u64) -> Result<Vec<u8>, QueryError> {
        if let Some((cached_id, bytes)) = &self.cached_page {
            if *cached_id == page_id {
                return Ok(bytes.clone());
            }
        }
        self.flush_cached_page()?;
        let offset = page_id.checked_mul(PAGE_SIZE as u64).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "recovery page offset overflow",
            )
        })?;
        let mut bytes = vec![0; PAGE_SIZE];
        if self.len.saturating_sub(offset) >= PAGE_SIZE as u64 {
            read_exact_at_counted(
                self.file.as_ref().expect("nonempty recovery file"),
                &mut bytes,
                offset,
            )
            .map_err(io_err)?;
        }
        self.remember(
            page_id,
            if recovery_page_valid(&bytes) {
                page_lsn(&bytes)
            } else {
                0
            },
        );
        self.cached_page = Some((page_id, bytes.clone()));
        Ok(bytes)
    }

    fn write(
        &mut self,
        page_id: u64,
        lsn: u64,
        after: &[u8],
        context: RecoveryContext,
    ) -> Result<(), QueryError> {
        if !context.must_redo(lsn) && self.lsn(page_id)? >= lsn {
            return Ok(());
        }
        if self.file.is_none() {
            let path = self.root.join(&self.relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(io_err)?;
            }
            self.file = Some(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(path)
                    .map_err(io_err)?,
            );
        }
        let mut page = after.to_vec();
        page.resize(PAGE_SIZE, 0);
        set_page_lsn_if_formatted(&mut page, lsn);
        if self
            .cached_page
            .as_ref()
            .is_some_and(|(id, _)| *id != page_id)
        {
            self.flush_cached_page()?;
        }
        let offset = page_id * PAGE_SIZE as u64;
        self.len = self.len.max(offset + PAGE_SIZE as u64);
        self.remember(
            page_id,
            if recovery_page_valid(&page) {
                page_lsn(&page)
            } else {
                0
            },
        );
        self.cached_page = Some((page_id, page));
        self.cached_dirty_lsn = Some(lsn);
        Ok(())
    }
    fn flush_cached_page(&mut self) -> Result<(), QueryError> {
        let Some(lsn) = self.cached_dirty_lsn else {
            return Ok(());
        };
        // Original redo is durable already. Recovery-generated undo images
        // additionally require the compensation WAL barrier before writeback.
        if let Some(log) = &self.undo_log {
            LogManager::sync_durable_to(log, lsn)?;
        }
        let (page_id, bytes) = self.cached_page.as_ref().expect("dirty recovery page");
        write_all_at(
            self.file.as_ref().expect("dirty recovery file"),
            bytes,
            *page_id * PAGE_SIZE as u64,
        )
        .map_err(io_err)?;
        note_recovery_write(&self.root, self.root.join(&self.relative))?;
        self.cached_dirty_lsn = None;
        Ok(())
    }
}
fn with_recovery_page_io<T>(
    root: &Path,
    relative: &str,
    work: impl FnOnce(&mut RecoveryPageIo) -> Result<T, QueryError>,
) -> Option<Result<T, QueryError>> {
    // IndexFile also writes these files through its own cache. Never cache
    // their headers here, or a physical/logical redo mix would see stale LSNs.
    // Zone Map redo must validate the complete page and generation before
    // trusting its LSN; a cached 16-byte header cannot establish this.
    if is_index_object_path(relative) || relative.ends_with(".zm") || relative.ends_with(".hvec") {
        return None;
    }
    RECOVERY_PAGE_IO.with(|slot| {
        let mut slot = slot.borrow_mut();
        let state = slot.as_mut()?;
        Some(state.open(root, relative).and_then(|_| work(state)))
    })
}
fn invalidate_recovery_page_io(root: &Path, relative: &str) {
    RECOVERY_PAGE_IO.with(|slot| {
        if let Some(state) = slot.borrow_mut().as_mut() {
            if state.root == root && state.relative == relative {
                state.relative.clear();
                state.file = None;
                state.len = 0;
                state.lsns.clear();
                state.cached_page = None;
                state.cached_dirty_lsn = None;
            }
        }
    });
}

#[cfg(test)]
mod recovery_io_tests {
    use super::*;

    #[test]
    fn recovery_workers_share_a_checked_directory_durability_barrier() {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        fs::create_dir_all(root.path().join("objects")).unwrap();
        let coordinator = RecoveryIoScope::enter().unwrap();
        std::thread::scope(|scope| {
            for index in 0..2 {
                let root = root.path();
                let directories = Arc::clone(&coordinator.directories);
                scope.spawn(move || {
                    let worker =
                        RecoveryIoScope::enter_with_directories(directories, false).unwrap();
                    let path = root.join(format!("objects/{index}.heap"));
                    fs::write(&path, [index as u8]).unwrap();
                    note_recovery_write(root, path).unwrap();
                    worker.finish().unwrap();
                });
            }
        });
        // Both files are synchronized; their shared parents are retained
        // exactly once until the coordinator's checked completion boundary.
        assert_eq!(coordinator.directories.lock().paths.len(), 2);
        coordinator.directories.lock().fail_next = true;
        assert!(coordinator.finish().is_err());
        assert_eq!(coordinator.directories.lock().paths.len(), 2);
        coordinator.finish().unwrap();
        assert!(coordinator.directories.lock().paths.is_empty());
        for index in 0..2 {
            assert_eq!(
                fs::read(root.path().join(format!("objects/{index}.heap"))).unwrap(),
                [index as u8]
            );
        }
    }
    #[test]
    fn index_redo_failure_restores_its_callers_logical_wal_routing() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let _caller = btree_logical_wal::LsnScope::new(Some(123));
        let mut indexes = HashMap::new();
        let ops = [WalIndexOp {
            kind: crate::wal::WalIndexOpKind::Insert,
            key: Vec::new(),
            row_id: crate::wal::WalRowId {
                page_id: 0,
                slot_id: 0,
            },
        }];
        assert!(redo_index_batch(
            dir.path(),
            log,
            &mut indexes,
            "objects/indexes/999/1/1.idx",
            77,
            1,
            &ops,
            None,
            RecoveryContext::restore(10, 100).unwrap()
        )
        .is_err());
        assert_eq!(btree_logical_wal::current(), Some(123));
        let mut state = RedoState::new(None);
        state.finish().unwrap();
        assert_eq!(btree_logical_wal::current(), Some(123));
    }

    #[test]
    fn explicit_restore_rules_reach_parallel_cached_page_writers() {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let relative = "objects/tables/101/1.heap";
        let page = Page::new().encode();
        for name in ["restore", "crash"] {
            let dir = root.path().join(name);
            fs::create_dir_all(dir.join("objects/tables/101")).unwrap();
            let mut newer = page.clone();
            set_page_lsn(&mut newer, 999);
            refresh_page_checksum(&mut newer);
            fs::write(dir.join(relative), newer).unwrap();
        }
        std::thread::scope(|scope| {
            for (name, context, expected) in [
                ("restore", RecoveryContext::restore(10, 100).unwrap(), 77),
                ("crash", RecoveryContext::default(), 999),
            ] {
                let dir = root.path().join(name);
                let page = &page;
                scope.spawn(move || {
                    let io = RecoveryIoScope::enter().unwrap();
                    redo_page_update_with_context(&dir, relative, 0, 77, page, context).unwrap();
                    io.finish().unwrap();
                    assert_eq!(page_lsn(&fs::read(dir.join(relative)).unwrap()), expected);
                });
            }
        });
    }

    #[test]
    fn replica_uncataloged_index_batches_reopen_with_valid_meta() {
        // A physical replica applies IndexBatch records for an online-built
        // candidate index before its catalog row exists: every record opens
        // the file with a fresh recovery handle, applies, flushes and drops,
        // so the next record must find a valid meta page on disk.
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let relative = "objects/indexes/999/1/3.idx";
        for (lsn, batch) in [(100u64, 0i64), (200, 1), (300, 2)] {
            // Insert 64 keys and delete every fourth again in the same
            // batch: a mixed batch takes the per-operation path, which never
            // splits or rewrites the meta page for this little data.
            let ops = (0..64i64)
                .map(|i| (i, false))
                .chain((3..64i64).step_by(4).map(|i| (i, true)))
                .map(|(i, delete)| {
                    let mut key = Vec::new();
                    encode_index_key_into(
                        &IndexKey::from(vec![KeyPart::Int(batch * 1000 + i)]),
                        &mut key,
                    )
                    .unwrap();
                    WalIndexOp {
                        kind: if delete {
                            crate::wal::WalIndexOpKind::Delete
                        } else {
                            crate::wal::WalIndexOpKind::Insert
                        },
                        key,
                        row_id: crate::wal::WalRowId {
                            page_id: (batch * 1000 + i) as u32,
                            slot_id: 0,
                        },
                    }
                })
                .collect::<Vec<_>>();
            let mut indexes = HashMap::new();
            redo_index_batch(
                dir.path(),
                Arc::clone(&log),
                &mut indexes,
                relative,
                lsn,
                0,
                &ops,
                None,
                RecoveryContext::default(),
            )
            .unwrap_or_else(|err| panic!("batch at lsn {lsn}: {err}"));
            let _routing = btree_logical_wal::LsnScope::new(None);
            for index in indexes.values_mut() {
                index.flush().unwrap();
            }
        }
        let reopened =
            IndexFile::open_for_recovery(dir.path().join(relative), Arc::clone(&log), 400).unwrap();
        assert_eq!(
            reopened.range_entries(None, None, false).unwrap().len(),
            3 * 48
        );
    }

    #[test]
    fn recovery_index_cost_route_requires_rebuild_before_skipping_logical_redo() {
        let dir = tempfile::tempdir().unwrap();
        let relative = "objects/indexes/999/1/1.idx";
        let path = dir.path().join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, vec![0u8; PAGE_SIZE]).unwrap();
        let mut key = Vec::new();
        encode_index_key_into(&IndexKey::from(vec![KeyPart::Int(1)]), &mut key).unwrap();
        let records = (0..128u64)
            .map(|lsn| WalRecord {
                source_time_us: 0,
                lsn: lsn + 1,
                prev_txn_lsn: 0,
                txn_id: 1,
                payload: WalPayload::IndexBatch {
                    path: relative.into(),
                    ops: vec![crate::wal::WalIndexOp {
                        kind: crate::wal::WalIndexOpKind::Insert,
                        key: key.clone(),
                        row_id: crate::wal::WalRowId {
                            page_id: 0,
                            slot_id: 0,
                        },
                    }],
                },
            })
            .collect::<Vec<_>>();
        let mut state = RedoState::new(None);
        precheck_recovery_index(
            dir.path(),
            &records,
            &(0..records.len()).collect::<Vec<_>>(),
            &mut state,
        )
        .unwrap();
        assert!(path.with_extension("idx.failed").exists());
        assert!(skip_prechecked_index_record(&records, 0, &mut state).unwrap());
        assert!(IndexFile::check_rebuild_marker(&path).is_err());
        fs::remove_file(path.with_extension("idx.failed")).unwrap();
        fs::remove_file(&path).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let healthy = IndexFile::open(path.clone(), log).unwrap();
        healthy.flush().unwrap();
        let mut healthy_state = RedoState::new(None);
        precheck_recovery_index(
            dir.path(),
            &records,
            &(0..records.len()).collect::<Vec<_>>(),
            &mut healthy_state,
        )
        .unwrap();
        assert!(healthy_state.skipped_index_redo_paths.contains(relative));
        assert!(path.with_extension("idx.failed").exists());
        fs::remove_file(path.with_extension("idx.failed")).unwrap();
        let mut small_state = RedoState::new(None);
        precheck_recovery_index(dir.path(), &records, &[0], &mut small_state).unwrap();
        assert!(small_state.skipped_index_redo_paths.is_empty());
        assert!(!path.with_extension("idx.failed").exists());
    }

    #[test]
    fn recovery_lsn_cache_tracks_writes_and_invalidates_file_generations() {
        let dir = tempfile::tempdir().unwrap();
        let relative = "objects/tables/999/1.heap";
        let mut page = Page::new().encode();
        set_page_lsn(&mut page, 20);
        refresh_page_checksum(&mut page);
        write_recovered_file(dir.path(), relative, &page).unwrap();
        let _scope = RecoveryIoScope::enter().unwrap();
        assert!(
            read_recovery_heap_page_below_lsn(dir.path(), relative, 0, 10)
                .unwrap()
                .is_none()
        );
        resize_recovered_file(dir.path(), relative, 0, false).unwrap();
        set_page_lsn(&mut page, 5);
        refresh_page_checksum(&mut page);
        write_recovered_file(dir.path(), relative, &page).unwrap();
        assert!(
            read_recovery_heap_page_below_lsn(dir.path(), relative, 0, 10)
                .unwrap()
                .is_some()
        );
        redo_page_update(dir.path(), relative, 0, 30, &page).unwrap();
        assert!(
            read_recovery_heap_page_below_lsn(dir.path(), relative, 0, 25)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            page_lsn(&read_recovery_page(dir.path(), relative, 0).unwrap()),
            30
        );
        _scope.finish().unwrap();
        assert_eq!(page_lsn(&fs::read(dir.path().join(relative)).unwrap()), 30);
        let deletion = WalRecord {
            source_time_us: 0,
            lsn: 40,
            prev_txn_lsn: 0,
            txn_id: 0,
            payload: WalPayload::FileDelete {
                path: relative.into(),
                before: vec![],
            },
        };
        apply_redo_record(
            dir.path(),
            &deletion,
            &RecoveryTxnSet::from_iter([]),
            0,
            false,
            &mut RedoState::new(None),
        )
        .unwrap();
        write_recovered_file(dir.path(), relative, &page).unwrap();
        assert!(
            read_recovery_heap_page_below_lsn(dir.path(), relative, 0, 10)
                .unwrap()
                .is_some()
        );
    }
}

#[cfg(test)]
mod recovery_index_allocation_tests {
    use super::*;
    #[test]
    fn recovery_index_allocator_does_not_reuse_persisted_page_extent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("allocator.idx");
        let mut file = File::create(&path).unwrap();
        let entry = |n| IndexEntry {
            key: IndexKey::from(vec![KeyPart::Int(n)]),
            row_id: RowId {
                page_id: n as u32,
                slot_id: 0,
            },
        };
        let meta = IndexMeta {
            root_page: 1,
            next_page_id: 2,
            free_page_head: None,
        };
        write_index_page(&mut file, 0, &encode_index_meta(&meta)).unwrap();
        write_index_page(
            &mut file,
            1,
            &encode_index_tree_page(&IndexPage::Internal(IndexInternalPage::new_generic(
                vec![entry(2)],
                vec![2, 3],
            )))
            .unwrap(),
        )
        .unwrap();
        write_index_page(
            &mut file,
            2,
            &encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new(
                vec![entry(1)],
                Some(3),
            )))
            .unwrap(),
        )
        .unwrap();
        write_index_page(
            &mut file,
            3,
            &encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new(vec![entry(2)], None)))
                .unwrap(),
        )
        .unwrap();
        drop(file);
        IndexFile::validate(path.clone()).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let index = IndexFile::open_for_recovery(path.clone(), log, 100).unwrap();
        assert!(
            index.reserve_page_id().unwrap() >= 4,
            "recovery must not allocate over an existing reachable leaf"
        );
        assert_eq!(
            index
                .get(&IndexKey::from(vec![KeyPart::Int(1)]), false)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            index
                .get(&IndexKey::from(vec![KeyPart::Int(2)]), false)
                .unwrap()
                .len(),
            1
        );
    }
}

#[cfg(test)]
mod recovery_index_lsn_tests {
    use super::*;
    #[test]
    fn native_index_batch_does_not_skip_a_synthesized_empty_root() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let index = IndexFile::open_for_recovery(dir.path().join("new.idx"), log, 10).unwrap();
        let key = IndexKey::from(vec![KeyPart::Int(1)]);
        let row = RowId {
            page_id: 1,
            slot_id: 0,
        };
        let batch = vec![IndexBatchEntry {
            kind: WalIndexOpKind::Insert,
            key: key.clone(),
            row_id: row,
        }];
        assert!(!index
            .recovery_batch_already_persisted(&batch, 10, 30)
            .unwrap());
        index.apply_replicated_batch(1, batch, 10).unwrap();
        assert_eq!(index.get(&key, false).unwrap(), vec![row]);
    }

    #[test]
    fn native_index_batch_uses_durable_lsn_not_replay_stamps() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("native.idx");
        let mut file = File::create(&path).unwrap();
        let key = |n| IndexKey::from(vec![KeyPart::Int(n)]);
        let row = |n| RowId {
            page_id: n,
            slot_id: 0,
        };
        let meta = IndexMeta {
            root_page: 1,
            next_page_id: 2,
            free_page_head: None,
        };
        write_index_page(&mut file, 0, &encode_index_meta(&meta)).unwrap();
        let mut leaf = encode_index_tree_page(&IndexPage::Leaf(IndexLeafPage::new(
            vec![IndexEntry {
                key: key(2),
                row_id: row(2),
            }],
            None,
        )))
        .unwrap();
        set_page_lsn_if_formatted(&mut leaf, 20);
        write_index_page(&mut file, 1, &leaf).unwrap();
        drop(file);
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let index = IndexFile::open_for_recovery(path, log, 10).unwrap();
        let batch = vec![IndexBatchEntry {
            kind: WalIndexOpKind::Insert,
            key: key(1),
            row_id: row(1),
        }];
        assert!(index
            .recovery_batch_already_persisted(&batch, 10, 30)
            .unwrap());
        assert!(!index
            .recovery_batch_already_persisted(&batch, 25, 30)
            .unwrap());
        assert!(!index
            .recovery_batch_already_persisted(&batch, 10, 20)
            .unwrap());
        let batch = vec![
            IndexBatchEntry {
                kind: WalIndexOpKind::Insert,
                key: key(3),
                row_id: row(3),
            },
            IndexBatchEntry {
                kind: WalIndexOpKind::Insert,
                key: key(4),
                row_id: row(4),
            },
        ];
        assert!(!index
            .recovery_batch_already_persisted(&batch, 25, 30)
            .unwrap());
        index.apply_replicated_batch(1, batch, 25).unwrap();
        assert_eq!(index.get(&key(3), false).unwrap(), vec![row(3)]);
        assert_eq!(index.get(&key(4), false).unwrap(), vec![row(4)]);
    }
}

fn set_recovery_undo_log(log: &Arc<crate::wal::LogMutex>) {
    RECOVERY_PAGE_IO.with(|slot| {
        if let Some(io) = slot.borrow_mut().as_mut() {
            io.undo_log = Some(Arc::clone(log));
        }
    });
}
