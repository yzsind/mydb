static DDL_EXCHANGE_HOLD_US: AtomicU64 = AtomicU64::new(0);

struct ExchangeHoldTimer(Instant);
impl Drop for ExchangeHoldTimer {
    fn drop(&mut self) {
        DDL_EXCHANGE_HOLD_US.fetch_add(
            self.0.elapsed().as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }
}

struct ExchangePreparation<'a> {
    _hold: ExchangeHoldTimer,
    _data: OnlineIndexTxnGuard<'a>,
    _mdl: Option<OnlineIndexMdlGuard<'a>>,
    counts: (u64, u64, u64),
    publication_txn: TxnView,
    _publication: OnlineIndexTxnGuard<'a>,
}

struct ExchangePublicationGuard<'a> {
    db: &'a Database,
    finished: bool,
}
impl ExchangePublicationGuard<'_> {
    fn finish(mut self) {
        self.finished = true;
    }
}
impl Drop for ExchangePublicationGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.db
                .quarantine_mvcc("EXCHANGE publication requires restart recovery");
        }
    }
}

impl Database {
    #[cfg(test)]
    fn exchange_test_point(&self, phase: u8) -> Result<(), QueryError> {
        if std::env::var("MYDB_EXCHANGE_FAIL_STAGE").ok().as_deref() == Some(&phase.to_string()) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "injected EXCHANGE publication failure",
            ));
        }
        if std::env::var("MYDB_EXCHANGE_CRASH_STAGE").ok().as_deref() == Some(&phase.to_string()) {
            crate::wal::LogManager::sync_durable_to(
                &self.log,
                self.log_state_atomics.next_lsn().saturating_sub(1),
            )?;
            std::process::exit(86);
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn exchange_hold_us_for_test(&self) -> u64 {
        DDL_EXCHANGE_HOLD_US.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn heap_dirty_pages_for_test(&self, table: &str) -> u64 {
        let table = self.table(table).unwrap();
        let heap = self.heap_handle(table.id).unwrap();
        let key = heap.read().file_key.clone();
        self.buffer_pool.file_residency_stats(&key).unwrap().1
    }

    fn exchange_online_supported(&self, table: &TableSchema, other: &TableSchema) -> bool {
        table.indexes.iter().chain(&other.indexes).all(|index| {
            index.state == IndexState::Public
                && (index.index_type.eq_ignore_ascii_case("BTREE")
                    || index.is_spatial()
                    || index.is_fulltext())
        })
    }

    fn exchange_write_stamp(&self, table: &TableSchema, other: &TableSchema) -> (u64, u64) {
        (
            self.table_write_epoch(table.id),
            self.table_write_epoch(other.id),
        )
    }

    fn exchange_mdl_barrier(
        &self,
        table: &TableSchema,
        other: &TableSchema,
        owner: u64,
        timeout: Duration,
        mode: MdlLockType,
        job: &AlterCopyJob,
    ) -> Result<(), QueryError> {
        let requests = [table, other]
            .into_iter()
            .map(|t| MdlRequest {
                key: MdlObjectKey::table(&t.database, &t.name),
                lock_type: mode,
            })
            .collect::<Vec<_>>();
        let started = Instant::now();
        let result = (|| loop {
            Self::check_alter_copy_cancelled(job)?;
            crate::engine::cancel::check_cancelled()?;
            self.ensure_operational()?;
            match self.acquire_mdl(
                owner,
                Some(owner),
                MdlDuration::Transaction,
                requests.clone(),
                "ddl/exchange/publication",
            ) {
                Ok(()) => return Ok(()),
                Err(err) if is_lock_wait_pending_error(&err) => {
                    if started.elapsed() >= timeout {
                        return Err(error(
                            crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                            "EXCHANGE timed out waiting for metadata locks",
                        ));
                    }
                    self.wait_for_ddl_signal(timeout.saturating_sub(started.elapsed()))?;
                }
                Err(err) => return Err(err),
            }
        })();
        if result.is_err() {
            self.mdl.clear_pending_for_owner(owner);
        }
        result
    }

    fn exchange_read_barrier(
        &self,
        owner: u64,
        ids: &[u64],
        timeout: Duration,
        job: &AlterCopyJob,
    ) -> Result<(), QueryError> {
        let started = Instant::now();
        loop {
            Self::check_alter_copy_cancelled(job)?;
            crate::engine::cancel::check_cancelled()?;
            self.ensure_operational()?;
            let mut acquired = true;
            for id in ids {
                match self
                    .table_locks_sharded
                    .try_acquire(owner, *id, TableLockMode::Shared)
                {
                    TableAcquireOutcome::Acquired { .. } | TableAcquireOutcome::AlreadyHeld => {}
                    _ => {
                        acquired = false;
                        break;
                    }
                }
            }
            if acquired {
                return Ok(());
            }
            self.table_locks_sharded.release_txn(owner);
            self.notify_non_row_lock_waiters();
            if started.elapsed() >= timeout {
                return Err(error(
                    crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                    "EXCHANGE timed out aligning its read snapshot",
                ));
            }
            self.wait_for_ddl_signal(timeout.saturating_sub(started.elapsed()))?;
        }
    }

    fn prepare_exchange<'a>(
        &'a self,
        table: &TableSchema,
        other: &TableSchema,
        storage: u64,
        part_index: usize,
        validation: bool,
        cutover: Option<(u64, Duration, bool)>,
        job: &AlterCopyJob,
    ) -> Result<ExchangePreparation<'a>, QueryError> {
        let txn = self.begin_read_only_txn()?;
        let data = OnlineIndexTxnGuard {
            db: self,
            txn_id: txn.txn_id,
        };
        let mdl = cutover.map(|(owner, _, _)| OnlineIndexMdlGuard { db: self, owner });
        let mut ids = vec![table.id, storage, other.id];
        ids.sort_unstable();
        ids.dedup();
        let online = cutover.is_some_and(|(_, _, enabled)| enabled);
        let part = table
            .partitioning
            .as_ref()
            .expect("EXCHANGE partition definition");
        let mut columns = HashSet::new();
        if validation {
            for name in part
                .columns
                .iter()
                .chain(part.subpartitioning.iter().flat_map(|p| p.columns.iter()))
            {
                columns.insert(table.column_index(name)?);
            }
            for program in [&part.expr_program, &part.sub_expr_program]
                .into_iter()
                .flatten()
            {
                if !program.try_collect_referenced_columns(&mut columns) {
                    columns.extend(0..table.columns.len());
                }
            }
            // A missing compiled expression uses the router's AST fallback.
            if columns.is_empty()
                || (!part.expr_sql.is_empty() && part.expr_program.is_none())
                || (part.sub_expr_ast.is_some() && part.sub_expr_program.is_none())
            {
                columns.extend(0..table.columns.len());
            }
        }
        let mut columns = columns.into_iter().collect::<Vec<_>>();
        columns.sort_unstable();
        for _attempt in 0..8 {
            let snapshot;
            let stamp;
            let mut held = None;
            if online {
                let (_, timeout, _) = cutover.unwrap();
                self.exchange_read_barrier(txn.txn_id, &ids, timeout, job)?;
                snapshot = StorageReadSnapshot::new(self);
                stamp = self.exchange_write_stamp(table, other);
                self.table_locks_sharded.release_txn(txn.txn_id);
                self.notify_non_row_lock_waiters();
            } else {
                if let Some((owner, timeout, _)) = cutover {
                    self.exchange_mdl_barrier(
                        table,
                        other,
                        owner,
                        timeout,
                        MdlLockType::SharedNoWrite,
                        job,
                    )?;
                    self.exchange_read_barrier(txn.txn_id, &ids, timeout, job)?;
                } else {
                    held = Some(ExchangeHoldTimer(Instant::now()));
                    self.try_online_index_data_barrier(txn.txn_id, &ids)?;
                }
                snapshot = StorageReadSnapshot::new(self);
                stamp = self.exchange_write_stamp(table, other);
            }
            #[cfg(test)]
            self.pause_ddl_backfill_for_test();
            let mut incoming = 0u64;
            let mut projected = vec![Value::Null; table.columns.len()];
            let checked = self.scan_rows_columns_storage_streaming_range(
                other,
                other.id,
                snapshot.txn,
                &columns,
                0,
                u64::MAX,
                |row| {
                    Self::check_alter_copy_cancelled(job)?;
                    crate::engine::cancel::check_cancelled()?;
                    incoming += 1;
                    if validation {
                        for (pos, value) in columns.iter().zip(row) {
                            projected[*pos] = value.clone();
                        }
                        let routed = table.route_row_to_partition(&projected)?;
                        if routed.index != part_index || routed.storage_id != storage {
                            return Err(error(
                                crate::error::catalog::ER_ROW_DOES_NOT_MATCH_PARTITION,
                                "EXCHANGE PARTITION: found row not matching the partition's bounds",
                            ));
                        }
                    }
                    Ok(())
                },
            );
            let mut outgoing = 0u64;
            self.scan_rows_columns_storage_streaming_range(
                table,
                storage,
                snapshot.txn,
                &[],
                0,
                u64::MAX,
                |_| {
                    outgoing += 1;
                    Ok(())
                },
            )?;
            if online {
                #[cfg(test)]
                if _attempt == 0 {
                    if let Some(hook) = self.online_copy_publish_test_hook.lock().unwrap().take() {
                        hook.pause();
                    }
                }
            }
            // Future counter namespaces are unreachable from either live
            // participant. Build them outside writer exclusion in the same
            // transaction that will publish the ownership swap. A changed
            // source epoch rolls this entire private attempt back.
            let publication_txn = self.begin_txn()?;
            let publication = OnlineIndexTxnGuard {
                db: self,
                txn_id: publication_txn.txn_id,
            };
            if checked.is_ok() && table.indexes.iter().any(IndexSchema::is_fulltext) {
                let view = TxnView {
                    snapshot_scn: snapshot.txn.snapshot_scn,
                    snapshot_registered: true,
                    ..publication_txn
                };
                let mut next_other = other.clone();
                next_other.id = storage;
                self.transfer_fulltext_statistics(view, other, other.id, table, other.id)?;
                self.transfer_fulltext_statistics(view, table, storage, &next_other, storage)?;
                self.flush_txn_wal_buffer(publication_txn.txn_id, None)?;
                if self.config.durability != DurabilityMode::Memory {
                    crate::wal::LogManager::sync_durable_to(
                        &self.log,
                        self.log_state_atomics.next_lsn().saturating_sub(1),
                    )?;
                }
            }
            if let Some((owner, timeout, _)) = cutover {
                self.exchange_mdl_barrier(
                    table,
                    other,
                    owner,
                    timeout,
                    MdlLockType::Exclusive,
                    job,
                )?;
                held = Some(ExchangeHoldTimer(Instant::now()));
                self.try_online_index_data_barrier(txn.txn_id, &ids)?;
                if online && self.exchange_write_stamp(table, other) != stamp {
                    self.table_locks_sharded.release_txn(txn.txn_id);
                    self.release_mdl_duration(owner, MdlDuration::Transaction);
                    self.notify_non_row_lock_waiters();
                    drop(held);
                    continue;
                }
            }
            checked?;
            Self::enter_alter_copy_finalizing(job)?;
            let total = table
                .stats
                .row_count
                .saturating_add_signed(self.table_live_row_delta(table.id));
            return Ok(ExchangePreparation {
                _hold: held.unwrap(),
                _data: data,
                _mdl: mdl,
                counts: (outgoing, incoming, total),
                publication_txn,
                _publication: publication,
            });
        }
        Err(error(
            crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
            "EXCHANGE could not obtain a stable validation snapshot within its retry budget",
        ))
    }
}
