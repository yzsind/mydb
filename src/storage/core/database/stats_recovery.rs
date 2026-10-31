/// Root row layout is shared by live progress capture and logical counter replay.
fn statistics_root_fold(
    row: &[Value],
    txn: u64,
) -> Result<Option<(u64, u64, i64, u64, bool)>, QueryError> {
    let number = |i| match row.get(i) {
        Some(Value::UInt(n)) => Ok(*n),
        Some(Value::Int(n)) if *n >= 0 => Ok(*n as u64),
        _ => Err(crate::storage::stats_codec::corrupt()),
    };
    if number(1)? != 0 {
        return Ok(None);
    }
    let reset = number(14)? == txn && txn != 0;
    if !reset && (txn == 0 || number(13)? != txn) {
        return Ok(None);
    }
    let baseline = match row.get(10) {
        Some(Value::Int(n)) => *n,
        _ => return Err(crate::storage::stats_codec::corrupt()),
    };
    Ok(Some((number(0)?, number(9)?, baseline, number(11)?, reset)))
}

/// Capture only at an applier/restartpoint boundary, with durable WAL covering
/// every applied record. The progress file owns the matching coverage watermark.
impl Database {
    pub(crate) fn replica_statistics_snapshot(
        &self,
        lsn: u64,
    ) -> Result<crate::repl::replica::progress::StatisticsSnapshot, QueryError> {
        let _apply = self.replica_apply_checkpoint.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "replica statistics snapshot lock poisoned",
            )
        })?;
        self.replica_statistics_snapshot_locked(lsn)
    }

    fn replica_statistics_snapshot_locked(
        &self,
        lsn: u64,
    ) -> Result<crate::repl::replica::progress::StatisticsSnapshot, QueryError> {
        use crate::repl::replica::progress::StatisticsSnapshot;
        if lsn != self.replica_applied_lsn_atomic.load(Ordering::Acquire)
            || lsn > self.log.lock().durable_lsn()
        {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "statistics snapshot is not at the durable applied frontier",
            ));
        }
        let live = self.table_live_row_deltas.read();
        let modified = self.table_modification_deltas.read();
        let ids: BTreeSet<_> = live.keys().chain(modified.keys()).copied().collect();
        let mut tables = Vec::with_capacity(ids.len());
        let known = self.catalog.table_keys_by_id.read().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "catalog identity lock poisoned",
            )
        })?;
        for id in ids {
            // Progress must not fault cold descriptors and their histograms into cache.
            if known.contains_key(&id) {
                tables.push((
                    id,
                    live.get(&id).map_or(0, |n| n.load(Ordering::Relaxed)),
                    modified.get(&id).map_or(0, |n| n.load(Ordering::Relaxed)),
                ));
            }
        }
        drop(known);
        let mut pending = Vec::new();
        for shard in &self.pending_live_row_deltas {
            for (&txn, values) in shard.lock().iter() {
                let mut values = values.iter().map(|(&id, &n)| (id, n)).collect::<Vec<_>>();
                values.sort_unstable();
                pending.push((txn, values));
            }
        }
        pending.sort_by_key(|e| e.0);
        drop(live);
        drop(modified);
        let active: HashSet<_> = self.applier_dd_dirty_txns.lock().keys().copied().collect();
        let mut folds = BTreeMap::<u64, Vec<(u64, u64, i64, u64, bool)>>::new();
        if !active.is_empty() {
            let id = crate::storage::data_dictionary::DD_TABLE_STATS;
            self.with_heap_read(id, |heap| {
                heap.scan_mvcc_until(|row| {
                    if !row.header.is_deleted() && active.contains(&row.header.last_txn_id) {
                        if let Some((table, revision, baseline, changes, reset)) =
                            statistics_root_fold(&row.values, row.header.last_txn_id)?
                        {
                            folds
                                .entry(row.header.last_txn_id)
                                .or_default()
                                .push((table, revision, baseline, changes, reset));
                        }
                    }
                    Ok(false)
                })
            })?;
        }
        Ok(StatisticsSnapshot {
            covered_lsn: lsn,
            tables,
            pending,
            folds: folds.into_iter().collect(),
        })
    }

    /// Physical REDO has already restored pages. Rebuild only the statistical
    /// sidecars from a progress snapshot and its retained logical WAL tail.
    fn recover_replica_statistics(&self) -> Result<(), QueryError> {
        let progress =
            crate::repl::replica::progress::load_progress(&self.data_dir).map_err(io_err)?;
        let snapshot = progress
            .and_then(|p| p.statistics)
            .map(Arc::unwrap_or_clone)
            .unwrap_or_default();
        if snapshot.covered_lsn > self.replica_applied_lsn_atomic.load(Ordering::Acquire) {
            return Err(crate::storage::stats_codec::corrupt());
        }
        let mut live = BTreeMap::<u64, i64>::new();
        let mut modified = BTreeMap::<u64, u64>::new();
        for (id, population, changes) in snapshot.tables {
            if live.insert(id, population).is_some() {
                return Err(crate::storage::stats_codec::corrupt());
            }
            modified.insert(id, changes);
        }
        let mut pending: HashMap<u64, BTreeMap<u64, i64>> = snapshot
            .pending
            .into_iter()
            .map(|(txn, rows)| (txn, rows.into_iter().collect()))
            .collect();
        let mut folds: HashMap<u64, BTreeMap<u64, (u64, i64, u64, bool)>> = snapshot
            .folds
            .into_iter()
            .map(|(txn, rows)| {
                (
                    txn,
                    rows.into_iter()
                        .map(|(id, rev, pop, changes, reset)| (id, (rev, pop, changes, reset)))
                        .collect(),
                )
            })
            .collect();
        let owners: HashMap<u64, u64> = self
            .catalog_read()?
            .tables
            .values()
            .filter(|t| !t.system)
            .flat_map(|t| {
                t.storage_ids()
                    .into_iter()
                    .chain(std::iter::once(t.id))
                    .map(move |id| (id, t.id))
            })
            .collect();
        fn visit(
            txn: u64,
            payload: &WalPayload,
            owners: &HashMap<u64, u64>,
            live: &mut BTreeMap<u64, i64>,
            modified: &mut BTreeMap<u64, u64>,
            pending: &mut HashMap<u64, BTreeMap<u64, i64>>,
            folds: &mut HashMap<u64, BTreeMap<u64, (u64, i64, u64, bool)>>,
        ) -> Result<(), QueryError> {
            if let WalPayload::TxnBatch { records } = payload {
                for payload in records {
                    visit(txn, payload, owners, live, modified, pending, folds)?;
                }
                return Ok(());
            }
            let mut population = |path: &str, changes: u64, delta: i64| {
                if txn == 0 {
                    return;
                }
                let Some((_, object)) = crate::storage::naming::parse_relative_object_path(path)
                else {
                    return;
                };
                let Some(&id) = owners.get(&object.table_id) else {
                    return;
                };
                *modified.entry(id).or_default() = modified
                    .get(&id)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(changes);
                let n = pending.entry(txn).or_default().entry(id).or_default();
                *n = n.saturating_add(delta);
            };
            let root = |path: &str,
                        bytes: &[u8]|
             -> Result<Option<(u64, u64, i64, u64, bool)>, QueryError> {
                let Some((_, object)) = crate::storage::naming::parse_relative_object_path(path)
                else {
                    return Ok(None);
                };
                if object.table_id != crate::storage::data_dictionary::DD_TABLE_STATS {
                    return Ok(None);
                }
                let table = crate::storage::data_dictionary::dd_table_schema(object.table_id)
                    .ok_or_else(crate::storage::stats_codec::corrupt)?;
                let (_, row) = decode_mvcc_row(bytes, &table.row_codec)?;
                statistics_root_fold(&row, txn)
            };
            match payload {
                WalPayload::HeapInsert { path, payload, .. } => {
                    if let Some((id, revision, baseline, changes, reset)) = root(path, payload)? {
                        folds
                            .entry(txn)
                            .or_default()
                            .insert(id, (revision, baseline, changes, reset));
                    } else if heap_payload_counts_as_logical_row(payload) {
                        population(path, 1, 1);
                    }
                }
                WalPayload::HeapInsertBatch { path, inserts } => {
                    let mut logical = 0;
                    for row in inserts {
                        if let Some((id, revision, baseline, changes, reset)) =
                            root(path, &row.payload)?
                        {
                            folds
                                .entry(txn)
                                .or_default()
                                .insert(id, (revision, baseline, changes, reset));
                        } else if heap_payload_counts_as_logical_row(&row.payload) {
                            logical += 1;
                        }
                    }
                    if logical > 0 {
                        population(path, logical, logical as i64);
                    }
                }
                WalPayload::HeapUpdate {
                    path,
                    before,
                    after,
                    ..
                } => {
                    let before = heap_payload_live_logical_state(before);
                    let after = heap_payload_live_logical_state(after);
                    if before.is_some() || after.is_some() {
                        population(
                            path,
                            1,
                            i64::from(after.unwrap_or(false)) - i64::from(before.unwrap_or(false)),
                        );
                    }
                }
                WalPayload::HeapUpdateLogical { path, patches, .. } => {
                    let delta = patches
                        .iter()
                        .find_map(|p| {
                            let offset = 1usize.checked_sub(p.offset as usize)?;
                            Some(
                                i64::from(*p.after.get(offset)? & MVCC_FLAG_DELETED == 0)
                                    - i64::from(*p.before.get(offset)? & MVCC_FLAG_DELETED == 0),
                            )
                        })
                        .unwrap_or(0);
                    population(path, 1, delta);
                }
                WalPayload::CommitTxn { .. } => {
                    for (id, delta) in pending.remove(&txn).unwrap_or_default() {
                        let n = live.entry(id).or_default();
                        *n = n.saturating_add(delta);
                    }
                    for (id, (_revision, baseline, changes, reset)) in
                        folds.remove(&txn).unwrap_or_default()
                    {
                        if reset {
                            live.remove(&id);
                            modified.remove(&id);
                        } else {
                            let n = live.entry(id).or_default();
                            *n = n.saturating_sub(baseline);
                            let n = modified.entry(id).or_default();
                            *n = n.saturating_sub(changes);
                        }
                    }
                }
                WalPayload::AbortTxn | WalPayload::EndTxn => {
                    pending.remove(&txn);
                    folds.remove(&txn);
                }
                _ => {}
            }
            Ok(())
        }
        if snapshot.covered_lsn < self.replica_applied_lsn_atomic.load(Ordering::Acquire) {
            let scan = LogManager::scan_replay_with_mode(
                &self.data_dir,
                self.recovery_context.wal_read_mode(),
            )?;
            for record in scan.records.records() {
                let record = record?;
                if record.lsn <= snapshot.covered_lsn {
                    continue;
                }
                visit(
                    record.txn_id,
                    &record.payload,
                    &owners,
                    &mut live,
                    &mut modified,
                    &mut pending,
                    &mut folds,
                )?;
            }
        }
        for (id, n) in live {
            if owners.contains_key(&id) {
                self.table_live_row_deltas
                    .write()
                    .insert(id, Arc::new(AtomicI64::new(n)));
            }
        }
        for (id, n) in modified {
            if owners.contains_key(&id) {
                self.table_modification_deltas
                    .write()
                    .insert(id, Arc::new(AtomicU64::new(n)));
            }
        }
        for (txn, rows) in pending {
            if self
                .txns
                .read()
                .status(txn)
                .is_some_and(|s| s.is_in_flight())
            {
                for (id, n) in rows {
                    self.record_pending_live_row_delta(txn, id, n);
                }
            }
        }
        Ok(())
    }
}
