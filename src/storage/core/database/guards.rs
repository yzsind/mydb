impl Database {
    pub(crate) fn catalog_read(&self) -> Result<RwLockReadGuard<'_, Catalog>, QueryError> {
        self.catalog.read().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "catalog read lock poisoned",
            )
        })
    }

    fn catalog_write(&self) -> Result<RwLockWriteGuard<'_, Catalog>, QueryError> {
        self.catalog.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "catalog write lock poisoned",
            )
        })
    }

    fn txns_read(
        &self,
    ) -> Result<parking_lot::RwLockReadGuard<'_, TransactionManager>, QueryError> {
        self.ensure_operational()?;
        Ok(self.txns.read())
    }

    fn txns_write(
        &self,
    ) -> Result<parking_lot::RwLockWriteGuard<'_, TransactionManager>, QueryError> {
        self.ensure_operational()?;
        Ok(self.txns.write())
    }

    /// Write guard for the row/unique/table lock subsystem (`LockManager`),
    /// split out of `txns` so lock acquire/release runs off the `txns` write
    /// lock. Lock order: `txns` → `lock_manager`; never take `txns` while
    /// holding this. Use for any mutation (acquire/release/savepoint-restore).
    #[inline]
    fn lock_manager(&self) -> parking_lot::RwLockWriteGuard<'_, TxnLockManager> {
        self.lock_manager.write()
    }

    /// Read guard for lock-subsystem probes that don't mutate it
    /// (`statement_savepoint`'s `lock_log_len`, the `data_locks` snapshots, the
    /// SHOW ENGINE stat getters). Shared so per-statement savepoints across
    /// concurrent sessions don't serialise on the lock subsystem.
    #[inline]
    fn lock_manager_read(&self) -> parking_lot::RwLockReadGuard<'_, TxnLockManager> {
        self.lock_manager.read()
    }

    /// Lock-free transaction-status probe for the lock subsystem (it must not
    /// reach into `txns`). Returns a closure over `slot_status_cache` suitable
    /// for `LockManager::acquire_unique_key_lock`.
    #[inline]
    fn lock_owner_status(&self) -> impl Fn(u64) -> Option<TxnStatus> + '_ {
        move |txn_id| {
            self.slot_status_cache
                .lookup(txn_id)
                .map(|(status, _)| status)
        }
    }

    fn undo_read(&self) -> Result<parking_lot::RwLockReadGuard<'_, UndoTablespace>, QueryError> {
        Ok(self.undo.read())
    }

    fn undo_lock(&self) -> Result<UndoDirectoryWriteGuard<'_>, QueryError> {
        Ok(self.undo.write())
    }

    /// Resolve undo metadata under the short global latch, then perform any
    /// segment page-in after releasing it. This keeps a cold historical read
    /// from blocking foreground undo publication for the duration of disk I/O.
    fn load_undo_owned(
        &self,
        undo_ptr: u64,
    ) -> Result<Option<crate::storage::undo::UndoRecord>, QueryError> {
        let prepared = self.undo.prepare_load_owned(undo_ptr)?;
        match prepared {
            Some(prepared) => prepared.finish(),
            None => Ok(None),
        }
    }

    fn checkpoint_lock(&self) -> Result<MutexGuard<'_, CheckpointState>, QueryError> {
        self.checkpoint.lock().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "checkpoint lock poisoned",
            )
        })
    }

    fn runtime_config_read(&self) -> RuntimeStorageConfig {
        self.runtime_config
            .read()
            .map(|config| *config)
            .unwrap_or_else(|_| RuntimeStorageConfig::from_storage_config(&self.config))
    }

    fn runtime_config_write(
        &self,
    ) -> Result<RwLockWriteGuard<'_, RuntimeStorageConfig>, QueryError> {
        self.runtime_config.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "runtime config lock poisoned",
            )
        })
    }
}

/// Borrowed counterpart of SnapshotGuard for storage APIs accepting &Database.
/// Register while selecting the frontier, under the same shard latch as purge.
struct StorageReadSnapshot<'a> {
    db: &'a Database,
    key: SnapshotKey,
    txn: TxnView,
}

impl<'a> StorageReadSnapshot<'a> {
    fn new(db: &'a Database) -> Self {
        Self::with_isolation(db, IsolationLevel::RepeatableRead)
    }

    fn with_isolation(db: &'a Database, isolation: IsolationLevel) -> Self {
        let (key, snapshot_scn) = db.pin_snapshot(None);
        Self {
            db,
            key,
            txn: TxnView {
                snapshot_registered: true,
                ..TxnView::read_only(snapshot_scn, isolation)
            },
        }
    }
}

impl Drop for StorageReadSnapshot<'_> {
    fn drop(&mut self) {
        self.db.unpin_snapshot(self.key);
    }
}

/// Roll back an owned internal transaction on scope exit. COPY only rolls back
/// a still-active transaction after publication; read/index guards always try.
struct RollbackTxnGuard<'a, const ACTIVE_ONLY: bool = false> {
    db: &'a Database,
    txn_id: u64,
}

impl<const ACTIVE_ONLY: bool> Drop for RollbackTxnGuard<'_, ACTIVE_ONLY> {
    fn drop(&mut self) {
        let rollback = !ACTIVE_ONLY
            || self
                .db
                .txns
                .read()
                .slot(self.txn_id)
                .is_some_and(|slot| slot.status == TxnStatus::Active);
        if rollback {
            let _ = self.db.rollback_txn(self.txn_id);
        }
    }
}
