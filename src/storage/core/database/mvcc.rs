/// Direct-mapped per-scan visibility memo (see
/// `Database::row_version_visible_cached`). Stack-allocated by each scan
/// loop; `u64::MAX` keys mark empty slots (no real txn id can collide).
pub(crate) struct ScanVisibilityCache {
    keys: [u64; SCAN_VIS_CACHE_SLOTS],
    vals: [bool; SCAN_VIS_CACHE_SLOTS],
}

const SCAN_VIS_CACHE_SLOTS: usize = 32;
/// Bound compensation metadata and the amount of work between durable progress
/// records. An individual undo record remains indivisible (including its LOB
/// ownership transfer); a large insert record is applied in bounded row chunks.
const ROLLBACK_BATCH_RECORDS: usize = 4096;
const ROLLBACK_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// Brent's cycle detector: constant space, no limit on a valid chain's length,
/// and one pointer comparison per hop. Physical undo addresses need not be
/// monotonic because append shards and compaction allocate independently.
#[derive(Default)]
struct UndoChainWalk {
    anchor: Option<u64>,
    power: usize,
    distance: usize,
}

impl UndoChainWalk {
    #[inline]
    fn visit(&mut self, ptr: u64) -> Result<(), QueryError> {
        if self.anchor == Some(ptr) {
            return Err(undo_corruption(ptr, "cycle detected in undo chain"));
        }
        if self.anchor.is_none() || self.distance == self.power {
            self.anchor = Some(ptr);
            self.power = self.power.saturating_mul(2).max(1);
            self.distance = 0;
        }
        self.distance += 1;
        Ok(())
    }
}

struct TxnUndoHeadCache {
    log: std::sync::Weak<crate::wal::LogMutex>,
    txn_id: u64,
    head: UndoHead,
    /// An eagerly registered writer may append its first undo without a
    /// directory read. A head reset invalidates this empty-head proof.
    eager_writer_generation: Option<u64>,
    /// Persistent DML begins with immutable read-write/WAL-backed attributes.
    persistent_dml: bool,
}

thread_local! {
    // One stable transaction handle per executing thread. The weak log
    // identity cannot alias a later database allocated at the same address,
    // and the slot-status mirror rejects finished transactions before reuse.
    static TXN_UNDO_HEAD_CACHE: RefCell<Option<TxnUndoHeadCache>> = const { RefCell::new(None) };
}

#[cfg(test)]
thread_local! {
    static ROLLBACK_BATCH_RECORD_LIMIT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(ROLLBACK_BATCH_RECORDS) };
    static ROLLBACK_PROGRESS_TEST_HOOK: RefCell<Option<Box<dyn FnMut(u64, Option<u64>, bool)>>> =
        const { RefCell::new(None) };
    static ROLLBACK_RECORD_TEST_HOOK: RefCell<Option<Box<dyn FnMut()>>> =
        const { RefCell::new(None) };
}

#[cold]
fn undo_corruption(undo_ptr: u64, detail: impl std::fmt::Display) -> QueryError {
    error(
        crate::error::catalog::ER_GET_ERRNO,
        format!("corrupt undo record {undo_ptr}: {detail}"),
    )
}

/// Whether to time the hot-path undo publish lock (`MYDB_UNDO_PUBLISH_LOCK_PROBE`).
/// Off by default so production pays no `Instant::now()` per undo record; the
/// concurrent-write probe sets it to attribute write serialization.
fn undo_publish_lock_probe_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        matches!(
            std::env::var("MYDB_UNDO_PUBLISH_LOCK_PROBE")
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "1" | "on" | "true" | "yes"
        )
    })
}

/// Whether to compute the undo record size estimate off-lock in `push_undo`
/// (default) vs. leaving it inside the locked mirror publish. Differential
/// oracle: `MYDB_UNDO_HOIST_SIZE=0` restores the in-lock computation so the
/// critical-section reduction can be A/B'd against the old path.
fn undo_hoist_size_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        !matches!(
            std::env::var("MYDB_UNDO_HOIST_SIZE")
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "off" | "false" | "no"
        )
    })
}

impl ScanVisibilityCache {
    pub(crate) fn new() -> Self {
        ScanVisibilityCache {
            keys: [u64::MAX; SCAN_VIS_CACHE_SLOTS],
            vals: [false; SCAN_VIS_CACHE_SLOTS],
        }
    }
}

thread_local! {
    static EXCLUSIVE_INSERT_LOCK_SCOPE: std::cell::Cell<Option<(usize, u64, u64)>> = const { std::cell::Cell::new(None) };
}

struct ExclusiveInsertLockScope(Option<(usize, u64, u64)>);

impl Drop for ExclusiveInsertLockScope {
    fn drop(&mut self) {
        EXCLUSIVE_INSERT_LOCK_SCOPE.with(|scope| scope.set(self.0));
    }
}

impl Database {
    fn visible_row(
        &self,
        row: MvccStoredRow,
        txn: TxnView,
    ) -> Result<Option<StoredRow>, QueryError> {
        let Some((header, values)) = self.visible_version(row.header, row.values, txn)? else {
            return Ok(None);
        };
        if header.is_deleted() {
            return Ok(None);
        }
        Ok(Some(StoredRow {
            row_id: row.row_id,
            values,
        }))
    }

    /// ALTER COPY variant that also reports whether the returned values are
    /// the current heap payload (and can therefore be byte-transcoded).
    /// The ordinary committed-row path performs the visibility probe once;
    /// only the rare undo lookup falls back through `visible_version`.
    fn visible_row_with_current_payload(
        &self,
        row: MvccStoredRow,
        txn: TxnView,
    ) -> Result<(Option<StoredRow>, bool), QueryError> {
        if self.row_version_visible(row.header, txn)? {
            if row.header.is_deleted() {
                return Ok((None, false));
            }
            return Ok((
                Some(StoredRow {
                    row_id: row.row_id,
                    values: row.values,
                }),
                true,
            ));
        }
        let Some((header, values)) = self.visible_version(row.header, row.values, txn)? else {
            return Ok((None, false));
        };
        if header.is_deleted() {
            return Ok((None, false));
        }
        Ok((
            Some(StoredRow {
                row_id: row.row_id,
                values,
            }),
            false,
        ))
    }

    /// Rebuild externalized columns from their chain descriptors into
    /// the slots they occupy as `Value::Null` placeholders. Used when an
    /// old row version is reconstructed from undo for an older snapshot.
    fn materialize_external_into(
        &self,
        table_id: u64,
        values: &mut [Value],
        refs: &[(usize, LobRef)],
    ) -> Result<(), QueryError> {
        if refs.is_empty() {
            return Ok(());
        }
        let codec = self
            .table_by_id(table_id)
            .map(|t| t.row_codec.clone())
            .or_else(|| {
                crate::storage::data_dictionary::dd_table_schema(table_id)
                    .map(|dd| dd.row_codec.clone())
            });
        for (col, lob) in refs {
            if *col >= values.len() {
                return Err(undo_corruption(
                    lob.first_page_id,
                    format!(
                        "external column {} exceeds reconstructed row width {}",
                        col,
                        values.len()
                    ),
                ));
            }
            let bytes = self.with_overflow_read(table_id, |ovf| ovf.read_chain(*lob))?;
            let kind = codec
                .as_ref()
                .and_then(|c| c.columns.get(*col))
                .map(|c| &c.kind);
            let value = match kind {
                Some(ColumnKind::VarString) => {
                    Value::String(String::from_utf8(bytes).map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "invalid utf8 in externalized TEXT",
                        )
                    })?)
                }
                Some(ColumnKind::Json) => Value::Json(bytes),
                Some(ColumnKind::Geometry) => Value::Geometry(bytes),
                Some(ColumnKind::Vector) => Value::Vector(bytes),
                _ => Value::Bytes(bytes),
            };
            values[*col] = value;
        }
        Ok(())
    }

    /// Borrow ordinary rows; copy only when an indexed external value needs
    /// materialization. Unindexed large columns never trigger overflow I/O.
    fn rollback_index_values<'a>(
        &self,
        table: &TableSchema,
        table_id: u64,
        values: &'a [Value],
        refs: &[(usize, LobRef)],
    ) -> Result<std::borrow::Cow<'a, [Value]>, QueryError> {
        let mut values = std::borrow::Cow::Borrowed(values);
        for descriptor in refs {
            let needed = table.indexes.iter().any(|index| {
                index
                    .columns
                    .iter()
                    .chain(index.included_columns.iter())
                    .any(|name| table.column_index(name).ok() == Some(descriptor.0))
                    || index.key_expressions.iter().flatten().any(|expr| {
                        expr.referenced_columns.is_empty()
                            || expr
                                .referenced_columns
                                .iter()
                                .any(|name| table.column_index(name).ok() == Some(descriptor.0))
                    })
            });
            if needed {
                self.materialize_external_into(
                    table_id,
                    values.to_mut(),
                    std::slice::from_ref(descriptor),
                )?;
            }
        }
        Ok(values)
    }

    fn enqueue_lob_reclaim(&self, entry: LobReclaimEntry) {
        let mut queue = self.lob_reclaim_queue.lock();
        let Some(queue) = queue.as_mut() else {
            self.dml_stats.record_lob_leak_unrecovered(1);
            return;
        };
        if queue.entries.iter().any(|pending| {
            pending.table_id == entry.table_id
                && pending.lob.first_page_id == entry.lob.first_page_id
                && pending.lob.generation == entry.lob.generation
        }) {
            return;
        }
        if queue.entries.len() == LOB_RECLAIM_QUEUE_CAP {
            self.dml_stats.record_lob_leak_unrecovered(1);
            return;
        }
        queue.entries.push_back(entry);
    }

    pub(crate) fn retry_lob_reclaims(&self) {
        for _ in 0..LOB_RECLAIM_DRAIN_PER_RUN {
            let entry = {
                let mut queue = self.lob_reclaim_queue.lock();
                queue.as_mut().and_then(|queue| queue.entries.pop_front())
            };
            let Some(mut entry) = entry else {
                break;
            };
            if self
                .with_overflow_mut(entry.table_id, |ovf| ovf.free_chain(entry.lob))
                .is_ok()
            {
                continue;
            }
            entry.attempts = entry.attempts.saturating_add(1);
            if entry.attempts >= LOB_RECLAIM_MAX_ATTEMPTS {
                self.dml_stats.record_lob_leak_unrecovered(1);
            } else {
                self.enqueue_lob_reclaim(entry);
            }
        }
    }

    /// Free the overflow chains referenced by `refs` (used by purge and
    /// rollback). A transient failure is queued for bounded retry; rollback
    /// correctness never depends on space reclamation succeeding inline.
    fn free_external_refs(&self, table_id: u64, refs: &[(usize, LobRef)]) {
        for (_, lob) in refs {
            if self
                .with_overflow_mut(table_id, |ovf| ovf.free_chain(*lob))
                .is_err()
            {
                self.enqueue_lob_reclaim(LobReclaimEntry {
                    table_id,
                    lob: *lob,
                    attempts: 1,
                });
            }
        }
    }

    /// M1 (LOB) — rebuild the external column mask when rollback restores an old version.
    ///
    /// `before_external` is the combined owned + borrowed descriptor set still held
    /// by the old row after rollback; matching `values` slots hold Null placeholders.
    /// Those chains are fed to
    /// `externalize_row_lobs_with_reuse` as "kept" entries: kept chains stay as-is (no
    /// reallocation); unexpected over-threshold inline columns get fresh chains. The mask feeds `write_mvcc_ext`,
    /// restoring the old version while honoring the one-page constraint. With no reuse it degrades to
    /// `external_mask_from(before_external)` (the externalize path allocates nothing new for
    /// all-Null values, keeping only forced chains), equivalent to the old behavior.
    fn rebuild_external_for_rollback(
        &self,
        table_id: u64,
        before_external: &[(usize, LobRef)],
        values: &[Value],
    ) -> Result<Vec<Option<LobRef>>, QueryError> {
        let codec = self
            .table_by_id(table_id)
            .map(|t| t.row_codec.clone())
            .or_else(|| {
                crate::storage::data_dictionary::dd_table_schema(table_id)
                    .map(|dd| dd.row_codec.clone())
            });
        let Some(codec) = codec else {
            // No codec (theoretically unreachable) — fall back to the old behavior, at least keeping the chains.
            return Ok(external_mask_from(before_external, values.len()));
        };
        let keep_mask = external_mask_from(before_external, values.len());
        // Rollback restore: any newly-allocated chain (for an unexpected
        // oversized inline column) belongs to the restored
        // row; failure of the restore itself surfaces up the rollback,
        // which is not a leak vector here — discard the alloc list.
        let (mask, _newly_allocated) =
            self.externalize_row_lobs_with_reuse(table_id, &codec, values, &keep_mask)?;
        Ok(mask)
    }

    fn visible_version(
        &self,
        header: RowHeader,
        values: Vec<Value>,
        txn: TxnView,
    ) -> Result<Option<(RowHeader, Vec<Value>)>, QueryError> {
        if self.row_version_visible(header, txn)? {
            return Ok(Some((header, values)));
        }
        let Some(mut undo_ptr) = header.undo_ptr else {
            return Ok(None);
        };
        let mut current_header = header;
        // `working` carries the row values as we walk back through the
        // undo chain. For `UndoKind::Update` / `::Delete` each step
        // replaces `working` with the record's full `before_values`.
        // For `UndoKind::UpdateDelta` we mutate `working` in place,
        // substituting just the columns the record patched — every
        // other column stays at whatever the *more recent* version had.
        //
        // `working` and each undo record are owned so the global undo
        // directory latch never spans segment-file I/O. Resident hits clone
        // the record under a shared latch; cold hits capture a stable file +
        // offset under that latch, release it, then decode off-lock. The cost
        // is one record clone on the old-snapshot path (never the hot
        // `row_version_visible`-at-top path), in exchange for allowing undo
        // append/purge metadata updates to proceed during cold reads.
        let mut working: Vec<Value> = values;
        // A full undo image stores off-page values as NULL placeholders.
        // Keep its descriptors alongside `working` while walking through
        // subsequent delta records, otherwise a delta-visible version would
        // accidentally return NULL for every unchanged LOB column.
        let mut working_external: Vec<(usize, LobRef)> = Vec::new();
        let mut walk = UndoChainWalk::default();
        loop {
            walk.visit(undo_ptr)?;
            // Phase 1: page-fault undo records. `load_undo_owned` returns a
            // resident clone or reads a captured segment location without
            // holding the directory latch. Only a record that is *physically gone*
            // (segment unlinked by purge / offset past the tail) yields
            // `None` here, and only then is the snapshot genuinely too
            // old — a record merely evicted from memory still reads back.
            let record = match self.load_undo_owned(undo_ptr)? {
                Some(record) => record,
                None => {
                    self.snapshot_too_old_total.fetch_add(1, Ordering::Relaxed);
                    let retention_ms = self
                        .runtime_config_read()
                        .undo_retention
                        .as_millis()
                        .min(u128::from(u64::MAX)) as u64;
                    return Err(error(crate::error::catalog::MYDB_ER_SNAPSHOT_TOO_OLD,
                        format!(
                            "snapshot too old: undo record {} required for consistent read has been purged \
                             (snapshot_scn={}, last_txn_id={}). \
                             Hint: increase mydb_undo_retention_ms (current={}ms) or shorten the long-running query.",
                            undo_ptr, txn.snapshot_scn, current_header.last_txn_id, retention_ms
                        ),
                    ));
                }
            };
            // Compaction keeps stale heap/header pointers valid through the
            // forwarding table, so the decoded record's relocated id may
            // legitimately differ from the logical pointer followed here.
            let record_table_id = record.table_id;
            let before_header = record.before_header;
            let before_values = record.before_values;
            let mut before_external = record.before_external;
            before_external.extend(record.borrowed_external);
            match record.kind {
                UndoKind::Insert | UndoKind::InsertBatch { .. } => return Ok(None),
                UndoKind::Update | UndoKind::Delete => {
                    let before_header = before_header
                        .ok_or_else(|| undo_corruption(undo_ptr, "missing before_header"))?;
                    let before_values = before_values
                        .ok_or_else(|| undo_corruption(undo_ptr, "missing before_values"))?;
                    if self.row_version_visible(before_header, txn)? {
                        let mut before_values = before_values;
                        // The old version's externalized columns are NULL
                        // placeholders in `before_values`; rebuild them
                        // from the chain descriptors before returning.
                        self.materialize_external_into(
                            record_table_id,
                            &mut before_values,
                            &before_external,
                        )?;
                        return Ok(Some((before_header, before_values)));
                    }
                    // The full before-image becomes the working version
                    // as we step back. UpdateDelta records that follow
                    // are applied on top of it.
                    working = before_values;
                    working_external = before_external;
                    current_header = before_header;
                    let Some(next_ptr) = before_header.undo_ptr else {
                        return Ok(None);
                    };
                    undo_ptr = next_ptr;
                }
                UndoKind::UpdateDelta { before_cols } => {
                    let before_header = before_header
                        .ok_or_else(|| undo_corruption(undo_ptr, "missing before_header"))?;
                    let before_values = before_values
                        .ok_or_else(|| undo_corruption(undo_ptr, "missing before_values"))?;
                    if before_cols.len() != before_values.len() {
                        return Err(undo_corruption(
                            undo_ptr,
                            format!(
                                "delta column/value length mismatch: {} != {}",
                                before_cols.len(),
                                before_values.len()
                            ),
                        ));
                    }
                    // Reverse-apply the column patches: positions in
                    // `before_cols[i]` get the matching `before_values[i]`.
                    for (col, value) in before_cols.into_iter().zip(before_values) {
                        let col = col as usize;
                        if col >= working.len() {
                            return Err(undo_corruption(
                                undo_ptr,
                                format!("delta column {col} exceeds row width {}", working.len()),
                            ));
                        }
                        working[col] = value;
                    }
                    if self.row_version_visible(before_header, txn)? {
                        let mut result = std::mem::take(&mut working);
                        let external = std::mem::take(&mut working_external);
                        self.materialize_external_into(record_table_id, &mut result, &external)?;
                        return Ok(Some((before_header, result)));
                    }
                    current_header = before_header;
                    let Some(next_ptr) = before_header.undo_ptr else {
                        return Ok(None);
                    };
                    undo_ptr = next_ptr;
                }
            }
        }
    }

    fn row_version_visible(&self, header: RowHeader, txn: TxnView) -> Result<bool, QueryError> {
        if header.last_txn_id == txn.txn_id {
            return Ok(true);
        }
        if txn.historical {
            if let Some((status, commit_scn)) = self.slot_status_cache.lookup(header.last_txn_id) {
                return Ok(matches!(status, TxnStatus::Committed)
                    && commit_scn != 0
                    && commit_scn <= txn.snapshot_scn);
            }
            if let Some((status, commit_scn)) = self
                .txns_read()?
                .slot(header.last_txn_id)
                .map(|slot| (slot.status, slot.commit_scn))
            {
                return Ok(matches!(status, TxnStatus::Committed)
                    && commit_scn.is_some_and(|scn| scn <= txn.snapshot_scn));
            }
            let Some(commit) = self.temporal_history.txn_commit(header.last_txn_id) else {
                if txn.historical_baseline_txn_id != 0
                    && header.last_txn_id < txn.historical_baseline_txn_id
                    && txn.snapshot_scn >= txn.historical_baseline_scn
                {
                    return Ok(true);
                }
                self.snapshot_too_old_total.fetch_add(1, Ordering::Relaxed);
                return Err(error(
                    crate::error::catalog::MYDB_ER_SYSTEM_TIME_TOO_OLD,
                    format!(
                        "historical visibility for transaction {} is no longer available in the commit ledger",
                        header.last_txn_id,
                    ),
                ));
            };
            return Ok(commit.commit_scn <= txn.snapshot_scn);
        }
        // FAST PATH 0: `min_retained_txn_id` proves that every writer id at or
        // below it has no retained slot (committed and visible to every live
        // snapshot, or a declared read-only transaction that owns no heap
        // header). Its slot-status mirror entries are removed before the
        // watermark is published, so checking it first is equivalent to a
        // mirror miss followed by FAST PATH 2, without taking a shard lock and
        // probing a hash map for every row last written by an old transaction.
        let watermark = self.min_retained_txn_id.load(Ordering::Acquire);
        if watermark > 0 && header.last_txn_id <= watermark {
            return Ok(true);
        }
        // FAST PATH: probe the lock-free atomic slot-status mirror. A hit
        // gives us the writer's terminal state without touching the
        // central `txns` RwLock, which under sysbench oltp_read_write was
        // the dominant SELECT bottleneck (~30% of CPU stuck in
        // `parking_lot::raw_rwlock::lock_shared_slow`).
        if let Some((status, commit_scn)) = self.slot_status_cache.lookup(header.last_txn_id) {
            if txn.dirty_read {
                // READ UNCOMMITTED sees Active / Prepared / Committed
                // alike; only RolledBack stays invisible.
                return Ok(!matches!(status, TxnStatus::RolledBack));
            }
            return Ok(match status {
                TxnStatus::Active | TxnStatus::Prepared => false,
                TxnStatus::RolledBack => false,
                TxnStatus::Committed => commit_scn <= txn.snapshot_scn,
            });
        }
        let cache_key = (self.visibility_cache_id, header.last_txn_id);
        if txn.dirty_read {
            // READ UNCOMMITTED sees every status except RolledBack. The
            // final states (Committed / RolledBack / GC'd slot) are shared
            // with the snapshot path via the same cache; Active/Prepared
            // stay uncached because their outcome can still flip.
            if let Some(cached) =
                TXN_VISIBILITY_CACHE.with(|cache| cache.borrow_mut().get(&cache_key).copied())
            {
                return Ok(matches!(cached, CachedTxnVisibility::Committed(_)));
            }
            let status = self
                .txns_read()?
                .slot(header.last_txn_id)
                .map(|slot| (slot.status, slot.commit_scn));
            return Ok(match status {
                Some((TxnStatus::RolledBack, _)) => {
                    Self::cache_txn_visibility(cache_key, CachedTxnVisibility::RolledBack);
                    false
                }
                Some((TxnStatus::Committed, commit_scn)) => {
                    Self::cache_txn_visibility(
                        cache_key,
                        CachedTxnVisibility::Committed(commit_scn.unwrap_or(0)),
                    );
                    true
                }
                Some((TxnStatus::Active | TxnStatus::Prepared, _)) => true,
                None => {
                    // Slot GC'd ⇒ old committed txn (see the snapshot path
                    // below for the proof); cacheable forever.
                    Self::cache_txn_visibility(cache_key, CachedTxnVisibility::Committed(0));
                    true
                }
            });
        }
        if let Some(cached) =
            TXN_VISIBILITY_CACHE.with(|cache| cache.borrow_mut().get(&cache_key).copied())
        {
            return Ok(match cached {
                CachedTxnVisibility::Committed(commit_scn) => commit_scn <= txn.snapshot_scn,
                CachedTxnVisibility::RolledBack => false,
            });
        }
        let Some((status, commit_scn)) = self
            .txns_read()?
            .slot(header.last_txn_id)
            .map(|slot| (slot.status, slot.commit_scn))
        else {
            // M3.4 - The slot was GC'd. A missing slot is unambiguously an
            // old, committed transaction: rolled-back txns rewrite their row
            // versions on rollback and are GC'd separately, so a still-visible
            // row version can never resolve through a purged rolled-back slot.
            // A committed txn is only eligible for slot GC once its commit SCN
            // sits below every live snapshot, so it is visible to all current
            // readers. Cache `Committed(0)` unconditionally — txn ids are
            // monotonic and never reused, so the entry stays valid forever.
            // (The previous `watermark > 0` gate left this uncached whenever
            // `min_retained_txn_id` had not advanced, forcing a contended
            // `txns_read()` RwLock acquisition on every row of every scan.)
            Self::cache_txn_visibility(cache_key, CachedTxnVisibility::Committed(0));
            return Ok(true);
        };
        match status {
            // A PREPARED writer's row versions are not yet final, so they
            // remain invisible to other transactions exactly like an
            // ACTIVE writer's. The cache is intentionally NOT poisoned
            // here — once the PREPARED branch commits/rolls back this
            // txn's visibility outcome flips, and we want subsequent
            // visibility checks to re-read the slot.
            TxnStatus::Active | TxnStatus::Prepared => Ok(false),
            TxnStatus::RolledBack => {
                Self::cache_txn_visibility(cache_key, CachedTxnVisibility::RolledBack);
                Ok(false)
            }
            TxnStatus::Committed => {
                let commit_scn = commit_scn.unwrap_or(0);
                Self::cache_txn_visibility(cache_key, CachedTxnVisibility::Committed(commit_scn));
                Ok(commit_scn <= txn.snapshot_scn)
            }
        }
    }

    fn cache_txn_visibility(key: (u64, u64), visibility: CachedTxnVisibility) {
        TXN_VISIBILITY_CACHE.with(|cache| {
            // LruCache::put returns the displaced entry, evicting LRU
            // when at capacity — no manual size check needed.
            cache.borrow_mut().put(key, visibility);
        });
    }

    /// P1-2 — per-scan visibility memo wrapping [`Self::row_version_visible`].
    /// A scan's `snapshot_scn` is fixed for its lifetime, so the boolean
    /// outcome per writer txn is stable and cacheable — INCLUDING
    /// Active/Prepared: a writer that commits mid-scan is assigned
    /// `commit_scn >= next_scn > snapshot_scn` and stays invisible to this
    /// snapshot, and Committed/RolledBack/GC'd outcomes never change. The
    /// direct-mapped probe replaces the per-row thread-local RefCell + LRU
    /// hash on the hot path (the TLS cache remains the cross-statement
    /// second tier inside `row_version_visible`). Dirty reads bypass the
    /// memo: READ UNCOMMITTED tracks live writer state per row.
    #[inline]
    fn row_version_visible_cached(
        &self,
        cache: &mut ScanVisibilityCache,
        header: RowHeader,
        txn: TxnView,
    ) -> Result<bool, QueryError> {
        if txn.dirty_read {
            return self.row_version_visible(header, txn);
        }
        let writer = header.last_txn_id;
        let idx = (writer as usize) & (SCAN_VIS_CACHE_SLOTS - 1);
        if cache.keys[idx] == writer {
            return Ok(cache.vals[idx]);
        }
        let visible = self.row_version_visible(header, txn)?;
        cache.keys[idx] = writer;
        cache.vals[idx] = visible;
        Ok(visible)
    }

    fn ensure_write_allowed(
        &self,
        txn: TxnView,
        table_id: u64,
        row: &MvccStoredRow,
    ) -> Result<(), QueryError> {
        if row.header.last_txn_id == txn.txn_id {
            return Ok(());
        }
        self.lock_row_nonblocking(txn.txn_id, table_id, row.row_id)
    }

    /// M1.1 - Reject any DML attempt in a read-only transaction.
    pub(crate) fn check_dml_allowed(&self, txn: TxnView) -> Result<(), QueryError> {
        if txn.read_only {
            return Err(error(
                crate::error::catalog::ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION,
                "cannot execute statement in a READ ONLY transaction",
            ));
        }
        Ok(())
    }

    pub(crate) fn check_dml_allowed_for_table(
        &self,
        txn: TxnView,
        _table_id: u64,
    ) -> Result<(), QueryError> {
        self.check_dml_allowed(txn)?;
        Ok(())
    }

    /// P2 — acquire a table-level lock through the sharded `ShardedTableLocks`
    /// store. The uncontended grant (the common IX-on-DML case) never touches
    /// the global `lock_manager`; only a conflict (record wait edge) or a
    /// grant that resolves a prior wait (drop the stale edge) dips into the
    /// cross-shard deadlock graph. Mirrors the legacy
    /// `LockManager::acquire_table_lock` semantics exactly.
    pub(crate) fn acquire_table_lock_sharded(
        &self,
        txn_id: u64,
        table_id: u64,
        mode: TableLockMode,
    ) -> Result<(), QueryError> {
        self.handle_table_lock_outcome(
            txn_id,
            table_id,
            self.table_locks_sharded.try_acquire(txn_id, table_id, mode),
        )
    }

    fn handle_table_lock_outcome(
        &self,
        txn_id: u64,
        table_id: u64,
        outcome: TableAcquireOutcome,
    ) -> Result<(), QueryError> {
        match outcome {
            TableAcquireOutcome::SchemaChanged => Err(error(
                crate::error::catalog::ER_TABLE_DEF_CHANGED,
                "table definition changed; rebind the table and retry the transaction",
            )),
            TableAcquireOutcome::AlreadyHeld => Ok(()),
            TableAcquireOutcome::Acquired { cleared_wait } => {
                if cleared_wait {
                    // We had a pending table-lock wait edge that this grant
                    // resolved — drop it from the deadlock graph.
                    self.lock_manager().clear_wait(txn_id);
                }
                Ok(())
            }
            TableAcquireOutcome::Conflict { owner } => self
                .lock_manager()
                .record_table_lock_wait(txn_id, owner, table_id),
        }
    }

    fn acquire_table_write_binding(
        &self,
        txn_id: u64,
        table: &TableSchema,
    ) -> Result<(), QueryError> {
        self.handle_table_lock_outcome(
            txn_id,
            table.id,
            self.table_locks_sharded.try_acquire_bound(
                txn_id,
                table.id,
                TableLockMode::IntentExclusive,
                Some(table.schema_version),
            ),
        )
    }

    /// Acquire a table-level shared data lock. SERIALIZABLE plain SELECTs
    /// use this coarse lock to block concurrent DML on the same table without
    /// modeling next-key/gap locks.
    pub(crate) fn acquire_table_shared(
        &self,
        txn_id: u64,
        table_id: u64,
    ) -> Result<(), QueryError> {
        if txn_id == 0 {
            return Ok(());
        }
        self.acquire_table_lock_sharded(txn_id, table_id, TableLockMode::Shared)
    }

    /// M2.2 - Acquire X (exclusive) lock at table level for DDL.
    #[allow(dead_code)]
    pub(crate) fn acquire_table_exclusive(
        &self,
        txn_id: u64,
        table_id: u64,
    ) -> Result<(), QueryError> {
        self.acquire_table_lock_sharded(txn_id, table_id, TableLockMode::Exclusive)
    }

    /// Execute inserts under an existing table X lock without retaining a
    /// redundant row/unique lock for every inserted row. The scope is local to
    /// this thread, database, transaction and table; ordinary DML and nested
    /// writes to other tables retain their normal locking. The caller must
    /// retain X through commit/rollback and must not downgrade it in the scope.
    pub(crate) fn with_exclusive_insert_locks<T>(
        &self,
        txn_id: u64,
        table_id: u64,
        execute: impl FnOnce() -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        if !self.table_locks_sharded.holds_exclusive(txn_id, table_id) {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "exclusive insert scope requires a table X lock",
            ));
        }
        let key = (self as *const Self as usize, txn_id, table_id);
        let _scope = ExclusiveInsertLockScope(
            EXCLUSIVE_INSERT_LOCK_SCOPE.with(|scope| scope.replace(Some(key))),
        );
        execute()
    }

    fn insert_locks_covered_by_table(&self, txn_id: u64, table_id: u64) -> bool {
        EXCLUSIVE_INSERT_LOCK_SCOPE
            .with(|scope| scope.get() == Some((self as *const Self as usize, txn_id, table_id)))
    }

    /// M2.2 - Verify that DDL can proceed: no active transaction holds any
    /// table-level lock on the target. Returns lock wait error if blocked.
    /// Uses a synthetic DDL txn id (`u64::MAX`) for visibility tracking.
    pub(crate) fn check_ddl_table_lock(&self, table_id: u64) -> Result<(), QueryError> {
        let mut holders = self
            .table_locks_sharded
            .holders_excluding(table_id, u64::MAX);
        if !holders.is_empty() {
            // Only the DDL conflict path pays for transaction-state lookup.
            // Transaction IDs are never reused. Keep synthetic/system owners
            // and both Active and Prepared branches; a finalized/missing slot
            // cannot legitimately retain a transactional table lock.
            let txns = self.txns_read()?;
            holders.retain(|(id, _)| {
                if *id == 0 || txns.slot(*id).is_some_and(|s| s.status.is_in_flight()) {
                    true
                } else {
                    self.table_locks_sharded.release_txn(*id);
                    false
                }
            });
        }
        if !holders.is_empty() {
            let owners: Vec<String> = holders
                .iter()
                .map(|(id, mode)| format!("txn {} ({})", id, mode.as_str()))
                .collect();
            return Err(error(
                crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                format!(
                    "DDL lock wait timeout: table {} held by {}",
                    table_id,
                    owners.join(", ")
                ),
            ));
        }
        Ok(())
    }

    /// P2 recovery migration — move table locks reconstructed into the legacy
    /// in-manager store (prepared-XA branches replayed by `from_wal`) into the
    /// sharded store, then strip them from the legacy staging area.
    pub(crate) fn migrate_recovered_table_locks(&self) {
        let drained = self.lock_manager().drain_table_locks();
        for (table_id, txn_id, mode) in drained {
            self.table_locks_sharded.reinstate(txn_id, table_id, mode);
        }
        // P3 — likewise migrate recovered row locks into the sharded store
        // (only when sharding is enabled; otherwise they stay in lock_manager).
        if row_lock_sharding_enabled() {
            let rows = self.lock_manager().drain_row_locks();
            for (txn_id, held) in rows {
                self.row_locks_sharded.reinstate(txn_id, held);
            }
        }
    }

    fn check_dml_write_allowed(
        &self,
        txn: TxnView,
        table_id: u64,
        row: &MvccStoredRow,
    ) -> Result<(), QueryError> {
        let status = if row.header.last_txn_id != txn.txn_id {
            self.slot_status_cache
                .lookup(row.header.last_txn_id)
                .map(|(s, _)| s)
                .or_else(|| {
                    self.txns_read()
                        .ok()
                        .and_then(|guard| guard.status(row.header.last_txn_id))
                })
        } else {
            None
        };
        if row.header.last_txn_id != txn.txn_id
            && matches!(status, Some(TxnStatus::Active) | Some(TxnStatus::Prepared))
        {
            // Enqueue ourselves on the row's wait list (deadlock detection)
            // and yield via the lock-wait pending error so the caller's retry
            // loop re-reads the row at the latest committed version after the
            // conflicting writer finishes. Re-reading is required to avoid
            // lost updates because callers compute new values from
            // `row.values`, which would be stale after blocking in place.
            self.check_row_lock_nonblocking(txn.txn_id, table_id, row.row_id)?;
            return Err(crate::storage::core::lock_wait_pending_error());
        }
        Ok(())
    }

    /// `push_undo` carrying the old version's externalized-column
    /// descriptors (UPDATE / DELETE of a row with off-page columns).
    /// Pass an empty `before_external` for rows with no off-page columns.
    fn push_undo_ext(
        &self,
        txn_id: u64,
        kind: UndoKind,
        table_id: u64,
        row_id: RowId,
        before_header: Option<RowHeader>,
        before_values: Option<Vec<Value>>,
        before_external: Vec<(usize, LobRef)>,
    ) -> Result<u64, QueryError> {
        self.push_undo_with_flush(
            txn_id,
            kind,
            table_id,
            row_id,
            before_header,
            before_values,
            before_external,
            Vec::new(),
        )
    }

    fn push_undo_ext_borrowed(
        &self,
        txn_id: u64,
        kind: UndoKind,
        table_id: u64,
        row_id: RowId,
        before_header: Option<RowHeader>,
        before_values: Option<Vec<Value>>,
        before_external: Vec<(usize, LobRef)>,
        borrowed_external: Vec<(usize, LobRef)>,
    ) -> Result<u64, QueryError> {
        self.push_undo_with_flush(
            txn_id,
            kind,
            table_id,
            row_id,
            before_header,
            before_values,
            before_external,
            borrowed_external,
        )
    }

    fn push_undo_deferred(
        &self,
        txn_id: u64,
        kind: UndoKind,
        table_id: u64,
        row_id: RowId,
        before_header: Option<RowHeader>,
        before_values: Option<Vec<Value>>,
    ) -> Result<u64, QueryError> {
        self.push_undo_with_flush(
            txn_id,
            kind,
            table_id,
            row_id,
            before_header,
            before_values,
            Vec::new(),
            Vec::new(),
        )
    }

    fn push_insert_batch_undo_deferred(
        &self,
        txn_id: u64,
        table_id: u64,
        row_ids: &[RowId],
    ) -> Result<Option<u64>, QueryError> {
        let result = match row_ids {
            [] => Ok(None),
            [row_id] => self
                .push_undo_deferred(txn_id, UndoKind::Insert, table_id, *row_id, None, None)
                .map(Some),
            [first, ..] => self
                .push_undo_deferred(
                    txn_id,
                    UndoKind::InsertBatch {
                        row_ids: row_ids.to_vec(),
                    },
                    table_id,
                    *first,
                    None,
                    None,
                )
                .map(Some),
        };
        if result.is_err() {
            self.rollback_inserted_rows(txn_id, table_id, row_ids)?;
        }
        result
    }

    fn preflight_insert_undo_admission(
        &self,
        table_id: u64,
        row_count: usize,
    ) -> Result<(), QueryError> {
        if row_count == 0
            || is_temp_table_id(table_id)
            || self.config.durability == DurabilityMode::Memory
        {
            return Ok(());
        }
        // The common admission check changes no state. Publication rechecks
        // the limits under the write latch, just as after the old preflight.
        if self
            .undo
            .read()
            .insert_admission_unpressured(row_count, true)
        {
            return Ok(());
        }
        // P2/12 — capture retention_pressure_count before/after admission
        // so we can tell whether THIS call crossed the disk cap (the soft
        // path). The warning is only useful if it lands in the
        // currently-executing statement's warning list, so we emit it
        // here rather than from inside the undo manager.
        let (pressure, disk_cap_crossed, warn_enabled) = {
            let mut undo = self.undo_lock()?;
            let pressure_before = undo.retention_pressure_count();
            let pressure = undo.check_insert_admission(row_count, true)?;
            let crossed = undo.retention_pressure_count() > pressure_before;
            let warn = undo.disk_cap_softfail_warn();
            if crossed && warn {
                undo.disk_cap_softfail_warns_emitted =
                    undo.disk_cap_softfail_warns_emitted.saturating_add(1);
            }
            (pressure, crossed, warn)
        };
        if disk_cap_crossed && warn_enabled {
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::MYDB_WARN_UNDO_CAPACITY,
                format!(
                    "undo tablespace approaching cap; \
                     admission triggered soft-fail purge (table_id={table_id}, rows={row_count})"
                ),
            );
        }
        if pressure {
            self.purge_obsolete_undo();
        }
        Ok(())
    }

    /// Phase-A probe: accumulate the wall time (wait + hold) of one
    /// hot-path undo publish lock acquisition. `started` is `Some` only when
    /// `MYDB_UNDO_PUBLISH_LOCK_PROBE` is set (captured immediately before
    /// `undo_lock()`); the caller drops the guard before calling this, so the
    /// sample spans contention wait + critical section. A no-op otherwise.
    #[inline]
    fn record_undo_publish_lock(&self, started: Option<std::time::Instant>) {
        let Some(started) = started else { return };
        let ns = started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        self.undo_publish_lock_ns.fetch_add(ns, Ordering::Relaxed);
        self.undo_publish_lock_samples
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Cumulative (ns, samples) for the undo publish lock — read by
    /// SHOW STATUS / the concurrent-write probe to attribute serialization.
    pub fn undo_publish_lock_stats(&self) -> (u64, u64) {
        (
            self.undo_publish_lock_ns.load(Ordering::Relaxed),
            self.undo_publish_lock_samples.load(Ordering::Relaxed),
        )
    }

    fn cache_started_dml_txn(&self, txn_id: u64, head: UndoHead) {
        let generation = head.reset_generation();
        TXN_UNDO_HEAD_CACHE.with(|cache| {
            *cache.borrow_mut() = Some(TxnUndoHeadCache {
                log: Arc::downgrade(&self.log),
                txn_id,
                head,
                eager_writer_generation: Some(generation),
                persistent_dml: true,
            });
        });
    }

    /// These immutable attributes are known for transactions created by
    /// begin_dml_txn. XA state remains a live probe, never a cached attribute.
    fn cached_persistent_dml_prepared(&self, txn_id: u64) -> Option<bool> {
        let known = TXN_UNDO_HEAD_CACHE.with(|cache| {
            cache.borrow().as_ref().is_some_and(|cached| {
                cached.txn_id == txn_id
                    && cached.persistent_dml
                    && std::ptr::eq(cached.log.as_ptr(), Arc::as_ptr(&self.log))
            })
        });
        if !known {
            return None;
        }
        self.slot_status_cache
            .lookup(txn_id)
            .and_then(|(status, _)| {
                status
                    .is_in_flight()
                    .then_some(status == TxnStatus::Prepared)
            })
    }

    /// Statements sharing a transaction are serialized by their caller.
    /// The stable head also mirrors its repair-log watermark, so a successful
    /// statement needs no global transaction-directory lease for its snapshot.
    fn undo_head_for_statement(&self, txn_id: u64) -> Result<UndoHead, QueryError> {
        self.ensure_operational()?;
        let cached = TXN_UNDO_HEAD_CACHE.with(|cache| {
            let cache = cache.borrow();
            let cached = cache.as_ref()?;
            (cached.txn_id == txn_id && std::ptr::eq(cached.log.as_ptr(), Arc::as_ptr(&self.log)))
                .then(|| cached.head.clone())
        });
        if let Some(head) = cached {
            if self
                .slot_status_cache
                .lookup(txn_id)
                .is_some_and(|(status, _)| status == TxnStatus::Active)
            {
                return Ok(head);
            }
        }
        let head = self.txns_read()?.active_slot(txn_id)?.undo_head.clone();
        TXN_UNDO_HEAD_CACHE.with(|cache| {
            *cache.borrow_mut() = Some(TxnUndoHeadCache {
                log: Arc::downgrade(&self.log),
                txn_id,
                head: head.clone(),
                eager_writer_generation: None,
                persistent_dml: false,
            });
        });
        Ok(head)
    }

    fn undo_head_for_append(&self, txn_id: u64) -> Result<(UndoHead, bool), QueryError> {
        self.ensure_operational()?;
        let cached = TXN_UNDO_HEAD_CACHE.with(|cache| {
            let cache = cache.borrow();
            let cached = cache.as_ref()?;
            (cached.txn_id == txn_id
                && std::ptr::eq(cached.log.as_ptr(), Arc::as_ptr(&self.log))
                && (cached.head.get().is_some()
                    || cached.eager_writer_generation == Some(cached.head.reset_generation())))
            .then(|| cached.head.clone())
        });
        if let Some(head) = cached {
            if self
                .slot_status_cache
                .lookup(txn_id)
                .is_some_and(|(status, _)| status.is_in_flight())
            {
                // Nonempty chains were already marked as writers. Empty
                // eager writers retain that proof only until the head resets.
                return Ok((head, true));
            }
        }
        let (head, writer) = {
            let txns = self.txns_read()?;
            let head = txns.undo_head_handle(txn_id)?;
            let writer = txns.slot(txn_id).is_some_and(|slot| slot.is_writer);
            (head, writer)
        };
        TXN_UNDO_HEAD_CACHE.with(|cache| {
            *cache.borrow_mut() = Some(TxnUndoHeadCache {
                log: Arc::downgrade(&self.log),
                txn_id,
                head: head.clone(),
                eager_writer_generation: None,
                persistent_dml: false,
            });
        });
        Ok((head, writer))
    }

    fn push_undo_with_flush(
        &self,
        txn_id: u64,
        kind: UndoKind,
        table_id: u64,
        row_id: RowId,
        before_header: Option<RowHeader>,
        before_values: Option<Vec<Value>>,
        before_external: Vec<(usize, LobRef)>,
        borrowed_external: Vec<(usize, LobRef)>,
    ) -> Result<u64, QueryError> {
        // Reuse a live handle, including eagerly registered empty writers;
        // otherwise fetch it under the txns read lock. Appends serialize only
        // against writers sharing this transaction, then publish the head via
        // CAS. Only a transaction not yet marked as a writer takes the txns
        // write lock — `mark_writer` must flag the txn
        // BEFORE its first undo/WAL append so checkpoint truncation
        // (`min_in_flight_wal_lsn`) can never miss an in-flight writer.
        //
        // DML statements that go through `begin_dml_txn` flag the writer
        // eagerly under the same TM.write that allocated the slot, so the
        // first undo here finds `is_writer` already set and avoids a
        // second TM.write acquisition. Read the flag under the same
        // read-lock that fetches the undo head so the check is free.
        let (head, already_writer) = self.undo_head_for_append(txn_id)?;
        // Parallel INSERT ... SELECT workers share one TxnView. Hold the
        // transaction-private latch across the whole append so two chunks
        // cannot persist sibling records with the same prev_undo_ptr and make
        // the second head publication fail (or, worse, fork the undo chain).
        // Different transactions use different latches and stay concurrent.
        let _append_guard = head.lock_append();
        let prev = head.get();
        if prev.is_none() && !already_writer {
            let mut txns = self.txns_write()?;
            self.capture.ensure_write()?;
            txns.mark_writer(txn_id)?;
            self.active_write_txns_atomic
                .store(txns.active_write_count, Ordering::SeqCst);
            // Freeze also inspects writer state under the transaction lock.
        }
        let mut record = UndoRecord {
            id: 0,
            txn_id,
            created_unix_ms: current_unix_ms(),
            kind,
            table_id,
            row_id,
            prev_undo_ptr: prev,
            before_header,
            before_values,
            before_external,
            borrowed_external,
            chain_depth: 0,
            cached_size: 0,
            resident_size: 0,
            temp_reservation: None,
            resident: true,
            // Set to the exact frame length by the undo append path.
            encoded_len: 0,
        };
        // Compute the size estimate here, OUTSIDE the undo lock. The mirror
        // publish (`insert_loaded_record`, under the undo write lock) skips
        // its own estimation when `cached_size` is already set, so the
        // O(columns) walk over `before_values` no longer runs inside the
        // serialized critical section. Identical value, just computed off-lock.
        if undo_hoist_size_enabled() {
            record.cached_size =
                u32::try_from(crate::storage::undo::estimate_undo_record_bytes(&record))
                    .unwrap_or(u32::MAX);
        }
        let temp_table = is_temp_table_id(table_id);
        if temp_table {
            record.temp_reservation = crate::storage::temp_quota::reserve_undo(
                &self.data_dir,
                table_id,
                crate::storage::undo::estimate_undo_resident_bytes(&record),
            )?;
        }
        // Admission and directory publication need the same capacity-based
        // estimate. Compute it once before either shared accounting latch.
        // Leave oversized records uncached rather than narrowing admission.
        record.resident_size =
            u32::try_from(crate::storage::undo::estimate_undo_resident_bytes(&record)).unwrap_or(0);
        let persistent = !temp_table && self.config.durability != DurabilityMode::Memory;
        // Encode (and CRC, inside write_frame) cost scales with row width;
        // do the encoding outside the global undo lock and let the lock
        // cover only ptr allocation + id patch + frame IO.
        let mut encoded = crate::storage::undo::encode_undo_record(&record)?;
        let (undo_id, bytes, pressure) = {
            let concurrent_shard =
                if persistent && self.undo_concurrent_append_eligible.load(Ordering::Acquire) {
                    let shard_count = self.undo_append_shards.len();
                    if shard_count == 0 {
                        None
                    } else {
                        let shard_idx = self.undo_append_next_shard.fetch_add(1, Ordering::Relaxed)
                            as usize
                            % shard_count;
                        Some(std::sync::Arc::clone(&self.undo_append_shards[shard_idx]))
                    }
                } else {
                    None
                };
            let concurrent_frame = if let Some(shard) = concurrent_shard {
                // Admission owns the manager's stable shared charge, so it
                // needs no undo-directory latch. Publication still takes the
                // exclusive latch and rechecks the runtime admission policies.
                let reservation = self.undo_resident_admission.reserve(&record)?;
                let frame = crate::storage::undo::UndoManager::write_concurrent_persistent_frame(
                    &shard,
                    &mut encoded,
                )?;
                Some((frame, reservation))
            } else {
                None
            };
            // Diagnostic timing is env-gated so the default hot path pays no
            // `Instant::now()` — the probe measured the undo publish lock at
            // only ~2–7% of wall time, so it stays off in production.
            let lock_started = undo_publish_lock_probe_enabled().then(std::time::Instant::now);
            if let Some(((ptr, segment_id, segment_end), reservation)) = concurrent_frame {
                let mut undo = self.undo_lock()?;
                let out = undo.finish_concurrent_persistent_append(
                    record,
                    encoded,
                    ptr,
                    segment_id,
                    segment_end,
                    reservation,
                )?;
                drop(undo);
                self.record_undo_publish_lock(lock_started);
                out
            } else {
                let mut undo = self.undo_lock()?;
                let out = undo.append_record_preencoded(record, encoded, persistent)?;
                drop(undo);
                self.record_undo_publish_lock(lock_started);
                out
            }
        };
        if persistent {
            let payload = WalPayload::UndoRecord { undo_id, bytes };
            // A streamed full-page image can include another transaction's
            // deferred row header. Publish its before-image first so a WAL
            // reader never receives a header whose undo is still private.
            // Standalone transactions keep undo/data commit batching.
            if txn_wal_buffering_active(&self.log, txn_id)
                && !(self.capture.redo_required() || crate::repl::physical_replication_active())
            {
                if push_txn_wal_record(&self.log, txn_id, payload, Vec::new()).is_err() {
                    if let Ok(mut undo) = self.undo_lock() {
                        let _ = undo.remove_record(undo_id);
                    }
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "transaction WAL buffer disappeared while staging undo",
                    ));
                }
            } else if let Err(err) = self.append_wal(txn_id, payload) {
                if let Ok(mut undo) = self.undo_lock() {
                    let _ = undo.remove_record(undo_id);
                }
                return Err(err);
            }
        }
        self.publish_undo_head(&head, prev, undo_id)?;
        // Immediate pressure purge can compact an active undo head. Release
        // this transaction's non-reentrant append fence first; compaction
        // reacquires the same fence while remapping active heads.
        drop(_append_guard);
        if pressure {
            self.purge_obsolete_undo();
        }
        Ok(undo_id)
    }

    fn publish_undo_head(
        &self,
        head: &UndoHead,
        prev: Option<u64>,
        undo_id: u64,
    ) -> Result<(), QueryError> {
        if head.compare_exchange(prev, Some(undo_id)) {
            return Ok(());
        }
        // The append fence makes this an invariant violation. Stop further
        // writes before cleanup; never let a transaction commit with a WAL
        // undo head that differs from its runtime chain.
        self.quarantine_mvcc("transaction undo head changed while appending undo".to_string());
        self.undo_lock()?.discard_unpublished_record(undo_id)?;
        Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "transaction undo head changed while appending undo",
        ))
    }

    fn rollback_txn_changes(&self, txn_id: u64) -> Result<HashSet<u64>, QueryError> {
        self.rollback_txn_changes_until(txn_id, None, 0, (0, 0))
    }

    fn rollback_txn_changes_until(
        &self,
        txn_id: u64,
        stop_at: Option<u64>,
        undo_next_lsn: u64,
        undo_skip: (u64, u64),
    ) -> Result<HashSet<u64>, QueryError> {
        #[cfg(test)]
        let batch_record_limit = ROLLBACK_BATCH_RECORD_LIMIT.with(|limit| limit.get());
        #[cfg(not(test))]
        let batch_record_limit = ROLLBACK_BATCH_RECORDS;
        let mut undo_ptr = self.txns_read()?.undo_head(txn_id)?;
        let mut touched_tables = HashSet::new();
        let mut compensated = Vec::new();
        let mut walk = UndoChainWalk::default();
        let mut batch_records = 0usize;
        let mut batch_bytes = 0usize;
        // Logical undo order does not identify a physical WAL prefix: bulk
        // heap writes and parallel workers can publish their WAL after several
        // undo records. Intermediate progress must retain the original
        // physical recovery cursor. Only the final marker may jump to the
        // caller's savepoint boundary (or zero for a complete rollback).
        // Compensation is logged as maintenance (transaction zero), so this
        // cursor can be captured lazily before the first progress marker.
        // Small rollbacks need no additional acquisition of the WAL latch.
        let mut recovery_cursor = None;
        while let Some(ptr) = undo_ptr {
            if let Some(stop) = stop_at {
                if ptr == stop || {
                    let undo = self.undo.read();
                    undo.canonical_ptr(ptr) == undo.canonical_ptr(stop)
                } {
                    break;
                }
            }
            walk.visit(ptr)?;
            let Some(record) = self.load_undo_owned(ptr)? else {
                return Err(error(
                    crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                    format!(
                        "rollback incomplete for transaction {txn_id}: undo record {ptr} is missing"
                    ),
                ));
            };
            if record.txn_id != txn_id {
                return Err(undo_corruption(
                    ptr,
                    format!(
                        "rollback expected txn {txn_id}, decoded txn={}",
                        record.txn_id
                    ),
                ));
            }
            batch_records += 1;
            batch_bytes = batch_bytes.saturating_add(if record.cached_size != 0 {
                record.cached_size as usize
            } else {
                crate::storage::undo::estimate_undo_record_bytes(&record)
                    .try_into()
                    .unwrap_or(usize::MAX)
            });
            touched_tables.insert(record.table_id);
            let owns_reclaimable =
                !record.before_external.is_empty() || matches!(record.kind, UndoKind::Delete);
            match record.kind {
                UndoKind::Insert => {
                    self.rollback_inserted_rows(record.txn_id, record.table_id, &[record.row_id])?;
                }
                UndoKind::InsertBatch { row_ids } => {
                    for rows in row_ids.rchunks(batch_record_limit) {
                        self.rollback_inserted_rows(record.txn_id, record.table_id, rows)?;
                    }
                }
                UndoKind::Update => {
                    let (Some(header), Some(values)) = (record.before_header, record.before_values)
                    else {
                        return Err(error(crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                            format!("rollback incomplete for transaction {txn_id}: update undo record {ptr} has no before-image")));
                    };
                    let Some((current_values, current_external)) =
                        self.read_row_collecting_external(record.table_id, record.row_id)?
                    else {
                        return Err(error(crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                            format!("rollback incomplete for transaction {txn_id}: updated row {:?} is missing", record.row_id)));
                    };
                    let mut old_external = record.before_external;
                    old_external.extend(record.borrowed_external);
                    if let Some(table) = self.table_by_id(record.table_id) {
                        let current_keys = self.rollback_index_values(
                            &table,
                            record.table_id,
                            &current_values,
                            &current_external,
                        )?;
                        let old_keys = self.rollback_index_values(
                            &table,
                            record.table_id,
                            &values,
                            &old_external,
                        )?;
                        self.update_changed_indexes_compensating(
                            record.table_id,
                            &table,
                            current_keys.as_ref(),
                            old_keys.as_ref(),
                            record.row_id,
                        )?;
                    }
                    let old_pages = old_external
                        .iter()
                        .map(|(_, r)| (r.first_page_id, r.generation))
                        .collect::<HashSet<_>>();
                    let to_free = current_external
                        .into_iter()
                        .filter(|(_, r)| !old_pages.contains(&(r.first_page_id, r.generation)))
                        .collect::<Vec<_>>();
                    let mask = self.rebuild_external_for_rollback(
                        record.table_id,
                        &old_external,
                        &values,
                    )?;
                    self.with_heap_mut(record.table_id, |heap| {
                        heap.write_mvcc_ext(txn_id_for_ddl(), record.row_id, header, &values, &mask)
                    })?;
                    self.free_external_refs(record.table_id, &to_free);
                }
                UndoKind::Delete => {
                    let (Some(header), Some(values)) = (record.before_header, record.before_values)
                    else {
                        return Err(error(
                            crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                            format!(
                                "rollback incomplete for transaction {txn_id}: delete undo record {ptr} has no before-image"
                            ),
                        ));
                    };
                    let mut old_external = record.before_external;
                    old_external.extend(record.borrowed_external);
                    if let Some(table) = self.table_by_id(record.table_id) {
                        let index_values = self.rollback_index_values(
                            &table,
                            record.table_id,
                            &values,
                            &old_external,
                        )?;
                        self.insert_indexes_deferred_at(
                            txn_id_for_ddl(),
                            record.table_id,
                            &table,
                            index_values.as_ref(),
                            record.row_id,
                        )?;
                    }
                    let mask = external_mask_from(&old_external, values.len());
                    self.with_heap_mut(record.table_id, |heap| {
                        heap.write_mvcc_ext(txn_id_for_ddl(), record.row_id, header, &values, &mask)
                    })?;
                }
                UndoKind::UpdateDelta { before_cols } => {
                    // Reconstruct the old row by reading the current
                    // heap version and splicing in the before-values
                    // for the patched columns. The full-row write
                    // through `write_mvcc_ext` keeps the rollback path
                    // identical in shape to the existing Update arm —
                    // no special "in-place patch via undo" path is
                    // needed for rollback correctness.
                    let (Some(header), Some(before_values)) =
                        (record.before_header, record.before_values)
                    else {
                        return Err(error(
                            crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                            format!(
                                "rollback incomplete for transaction {txn_id}: delta undo record {ptr} has no before-image"
                            ),
                        ));
                    };
                    if before_cols.len() != before_values.len() {
                        return Err(error(
                                crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                                format!(
                                    "rollback incomplete for transaction {txn_id}: delta undo record {ptr} has {} columns but {} values",
                                    before_cols.len(),
                                    before_values.len(),
                                ),
                            ));
                    }
                    let Some((current_values, current_external)) =
                        self.read_row_collecting_external(record.table_id, record.row_id)?
                    else {
                        return Err(error(crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                            format!("rollback incomplete for transaction {txn_id}: row {:?} for delta undo record {ptr} is missing", record.row_id)));
                    };
                    let mut reverted = current_values.clone();
                    for i in 0..before_cols.len() {
                        let col = before_cols[i] as usize;
                        if col >= reverted.len() {
                            return Err(error(
                                    crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                                    format!(
                                        "rollback incomplete for transaction {txn_id}: column {col} in delta undo record {ptr} is out of range"
                                    ),
                                ));
                        }
                        reverted[col] = before_values[i].clone();
                    }
                    let mut old_external = record.before_external;
                    old_external.extend(record.borrowed_external);
                    // Delta undo owns only the changed columns. Untouched
                    // external values are represented by NULL in the collected
                    // row, and their descriptors remain on the current row.
                    // Retain those chains when restoring the old version;
                    // otherwise rollback writes NULL and frees a live LOB.
                    for &(column, reference) in &current_external {
                        if !before_cols
                            .iter()
                            .any(|&changed| usize::from(changed) == column)
                            && !old_external
                                .iter()
                                .any(|(old_column, _)| *old_column == column)
                        {
                            old_external.push((column, reference));
                        }
                    }
                    if let Some(table) = self.table_by_id(record.table_id) {
                        let current_keys = self.rollback_index_values(
                            &table,
                            record.table_id,
                            &current_values,
                            &current_external,
                        )?;
                        let reverted_keys = self.rollback_index_values(
                            &table,
                            record.table_id,
                            &reverted,
                            &old_external,
                        )?;
                        self.update_changed_indexes_compensating(
                            record.table_id,
                            &table,
                            current_keys.as_ref(),
                            reverted_keys.as_ref(),
                            record.row_id,
                        )?;
                    }
                    let old_pages = old_external
                        .iter()
                        .map(|(_, r)| (r.first_page_id, r.generation))
                        .collect::<HashSet<_>>();
                    let to_free = current_external
                        .into_iter()
                        .filter(|(_, r)| !old_pages.contains(&(r.first_page_id, r.generation)))
                        .collect::<Vec<_>>();
                    let mask = external_mask_from(&old_external, reverted.len());
                    self.with_heap_mut(record.table_id, |heap| {
                        heap.write_mvcc_ext(
                            txn_id_for_ddl(),
                            record.row_id,
                            header,
                            &reverted,
                            &mask,
                        )
                    })?;
                    self.free_external_refs(record.table_id, &to_free);
                }
            }
            if owns_reclaimable {
                compensated.push(self.undo.read().wal_identity(record.id));
            }
            undo_ptr = record.prev_undo_ptr;
            #[cfg(test)]
            ROLLBACK_RECORD_TEST_HOOK.with(|hook| {
                if let Some(hook) = hook.borrow_mut().as_mut() {
                    hook();
                }
            });
            if (batch_records >= batch_record_limit || batch_bytes >= ROLLBACK_BATCH_BYTES)
                && undo_ptr.is_some()
                && !self.undo_boundary_matches(undo_ptr, stop_at)
            {
                self.persist_rollback_progress(
                    txn_id,
                    undo_ptr,
                    *recovery_cursor
                        .get_or_insert_with(|| self.log.lock().last_txn_lsn(txn_id)),
                    (0, 0),
                    &compensated,
                    true,
                )?;
                compensated.clear();
                batch_records = 0;
                batch_bytes = 0;
            }
        }
        if stop_at.is_some() && undo_ptr.is_none() {
            return Err(undo_corruption(
                stop_at.unwrap(),
                "savepoint boundary missing from transaction chain",
            ));
        }
        if batch_records != 0 {
            self.persist_rollback_progress(
                txn_id,
                stop_at,
                undo_next_lsn,
                undo_skip,
                &compensated,
                false,
            )?;
        }
        Ok(touched_tables)
    }

    fn undo_boundary_matches(&self, head: Option<u64>, stop: Option<u64>) -> bool {
        head == stop
            || match (head, stop) {
                (Some(head), Some(stop)) => {
                    let undo = self.undo.read();
                    undo.canonical_ptr(head) == undo.canonical_ptr(stop)
                }
                _ => false,
            }
    }

    fn persist_rollback_progress(
        &self,
        txn_id: u64,
        undo_head: Option<u64>,
        undo_next_lsn: u64,
        undo_skip: (u64, u64),
        compensated: &[u64],
        intermediate: bool,
    ) -> Result<(), QueryError> {
        let undo_head = undo_head.map(|ptr| self.undo.read().wal_identity(ptr));
        let lsn = self.append_wal(
            txn_id,
            WalPayload::RollbackTo {
                undo_head,
                undo_next_lsn,
                skip_lsn_start: undo_skip.0,
                skip_lsn_end: undo_skip.1,
                compensated: compensated.to_vec(),
            },
        )?;
        // Persist each intermediate batch before starting another one. Small
        // rollbacks keep the existing final commit barrier, except when LOB /
        // delete ownership must be durable before its manifest is published.
        if lsn != 0 && (intermediate || !compensated.is_empty()) {
            crate::wal::LogManager::sync_durable_to(&self.log, lsn)?;
        }
        if !compensated.is_empty() {
            self.undo.write().mark_compensated(compensated);
        }
        #[cfg(test)]
        ROLLBACK_PROGRESS_TEST_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().as_mut() {
                hook(txn_id, undo_head, intermediate);
            }
        });
        Ok(())
    }

    fn rollback_inserted_rows(
        &self,
        txn_id: u64,
        table_id: u64,
        row_ids: &[RowId],
    ) -> Result<(), QueryError> {
        self.rollback_inserted_rows_inner(table_id, row_ids)
            .map_err(|err| {
                self.quarantine_mvcc(format!(
                    "insert compensation failed for transaction {txn_id}: {}",
                    err.message()
                ));
                err
            })
    }

    fn rollback_inserted_rows_inner(
        &self,
        table_id: u64,
        row_ids: &[RowId],
    ) -> Result<(), QueryError> {
        let table = self.table_by_id(table_id);
        let mut index_deletes = table.as_ref().map(|table| {
            table
                .indexes
                .iter()
                .map(|index| (index.name.clone(), Vec::new()))
                .collect::<BTreeMap<_, Vec<IndexBatchEntry>>>()
        });
        let target_row_ids = row_ids.iter().copied().collect::<HashSet<_>>();
        let mut scan_fallback_indexes = HashSet::new();
        let mut chains_reclaimed: u64 = 0;
        for row_id in row_ids.iter().rev().copied() {
            // One collector read yields both the row's (non-LOB) values
            // for index-delete keys and the chains the row owns — without
            // fetching any LOB bytes. The inserted row is the sole owner
            // of its chains, so they are freed here.
            let collected = self.read_row_collecting_external(table_id, row_id)?;
            if let Some((values, refs)) = &collected {
                if let Some(table) = table.as_ref() {
                    let values = self.rollback_index_values(table, table_id, values, refs)?;
                    if let Some(index_deletes) = index_deletes.as_mut() {
                        for index in &table.indexes {
                            let entries = index_deletes
                                .get_mut(&index.name)
                                .expect("index delete batch should exist");
                            match table.index_keys(index, &values) {
                                Ok(keys) if !scan_fallback_indexes.contains(&index.name) => {
                                    for key in keys {
                                        entries.push(IndexBatchEntry {
                                            kind: WalIndexOpKind::Delete,
                                            key,
                                            row_id,
                                        });
                                    }
                                }
                                Ok(_) => {}
                                Err(_) => {
                                    // The forward INSERT may have failed while
                                    // evaluating this functional/MVI key. Do
                                    // not repeat the same invalid expression
                                    // during compensation. A physical scan is
                                    // authoritative and remains cheap because
                                    // it is used only on the error path.
                                    entries.clear();
                                    scan_fallback_indexes.insert(index.name.clone());
                                }
                            }
                        }
                    }
                }
                // Surface every chain we reclaim through the DML stats —
                // this is the M1.A2 "engine recovered the chain instead
                // of leaking it" signal. Callers (A3 atomicity bail-out,
                // INSERT undo append failure) trigger this path.
                chains_reclaimed = chains_reclaimed.saturating_add(refs.len() as u64);
            }
            if let Some((_, refs)) = &collected {
                self.with_heap_mut(table_id, |heap| heap.delete(txn_id_for_ddl(), row_id))?;
                self.free_external_refs(table_id, refs);
            } else if let Some(table) = &table {
                // Recovery or a prior cleanup may already have removed the
                // heap row. Still remove any residual index candidates.
                scan_fallback_indexes.extend(table.indexes.iter().map(|index| index.name.clone()));
            }
        }
        self.dml_stats.record_lob_leak_recovered(chains_reclaimed);
        if let (Some(table), Some(mut index_deletes)) = (table.as_ref(), index_deletes) {
            for index in &table.indexes {
                if !scan_fallback_indexes.contains(&index.name) {
                    continue;
                }
                let physical =
                    self.with_index_read(table_id, &index.name, |index_file| index_file.entries())?;
                let entries = index_deletes
                    .get_mut(&index.name)
                    .expect("index delete batch should exist");
                entries.extend(
                    physical
                        .into_iter()
                        .filter(|(_, row_id)| target_row_ids.contains(row_id))
                        .map(|(key, row_id)| IndexBatchEntry {
                            kind: WalIndexOpKind::Delete,
                            key,
                            row_id,
                        }),
                );
            }
            self.apply_index_batches_deferred_at(txn_id_for_ddl(), table_id, table, index_deletes)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod rollback_batch_tests {
    use super::*;
    include!("../rollback_batch_tests.rs");
}

#[cfg(test)]
mod undo_audit_tests {
    use super::*;

    #[test]
    fn exclusive_insert_scope_bounds_locks_and_excludes_other_writers() {
        std::fs::create_dir_all("/tmp/mydb/exclusive-inserts").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb/exclusive-inserts").unwrap();
        let db = Database::open(dir.path()).unwrap();
        let txn = db.begin_txn().unwrap();
        let peer = db.begin_txn().unwrap();
        assert!(db
            .with_exclusive_insert_locks(txn.txn_id, 42, || Ok(()))
            .is_err());
        db.acquire_table_exclusive(txn.txn_id, 42).unwrap();
        crate::util::env::with_vars(&[("MYDB_MAX_LOCKS", Some("1"))], || {
            db.with_exclusive_insert_locks(txn.txn_id, 42, || {
                for page_id in 0..4096 {
                    db.lock_rows_nonblocking(
                        txn.txn_id,
                        42,
                        [RowId {
                            page_id,
                            slot_id: 0,
                        }],
                    )?;
                    assert!(db.insert_locks_covered_by_table(txn.txn_id, 42));
                    assert!(!db.insert_locks_covered_by_table(peer.txn_id, 42));
                    assert!(!db.insert_locks_covered_by_table(txn.txn_id, 43));
                }
                assert_eq!(db.row_locks_sharded.approx_lock_count(), 0);
                assert!(db.lock_manager_read().locks.is_empty());
                assert!(db.lock_manager_read().unique_locks.is_empty());
                assert!(db
                    .acquire_table_lock_sharded(peer.txn_id, 42, TableLockMode::IntentExclusive)
                    .is_err());
                Ok(())
            })
            .unwrap();
        });
        assert!(!db.insert_locks_covered_by_table(txn.txn_id, 42));
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: Result<(), QueryError> =
                db.with_exclusive_insert_locks(txn.txn_id, 42, || panic!("scope cleanup"));
        }));
        assert!(panic.is_err());
        assert!(!db.insert_locks_covered_by_table(txn.txn_id, 42));
        db.rollback_txn(peer.txn_id).unwrap();
        db.rollback_txn(txn.txn_id).unwrap();
        let next = db.begin_txn().unwrap();
        db.acquire_table_exclusive(next.txn_id, 42).unwrap();
        db.rollback_txn(next.txn_id).unwrap();
    }

    #[test]
    fn undo_audit_head_conflict_discards_and_quarantines() {
        std::fs::create_dir_all("/tmp/mydb/undo-fix").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb/undo-fix").unwrap();
        let db = Database::open(dir.path()).unwrap();
        let txn = db.begin_txn().unwrap();
        let head = db
            .txns_read()
            .unwrap()
            .undo_head_handle(txn.txn_id)
            .unwrap();
        head.set(Some(42));
        assert!(db.publish_undo_head(&head, None, 43).is_err());
        assert_eq!(head.get(), Some(42));
        assert!(db.mvcc_quarantined.load(Ordering::Acquire));
    }

    #[test]
    fn undo_audit_ddl_reconciles_only_stale_transaction_locks() {
        std::fs::create_dir_all("/tmp/mydb/undo-fix").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb/undo-fix").unwrap();
        let db = Database::open(dir.path()).unwrap();
        let txn = db.begin_txn().unwrap();
        let id = txn.txn_id;
        db.table_locks_sharded
            .reinstate(id, 42, TableLockMode::IntentExclusive);
        assert!(db.check_ddl_table_lock(42).is_err());
        db.txns_write().unwrap().active_slot_mut(id).unwrap().status = TxnStatus::Prepared;
        assert!(db.check_ddl_table_lock(42).is_err());
        db.txns_write().unwrap().rollback(id).unwrap();
        db.check_ddl_table_lock(42).unwrap();
        assert!(db
            .table_locks_sharded
            .holders_excluding(42, u64::MAX)
            .is_empty());
        db.table_locks_sharded
            .reinstate(id + 100, 42, TableLockMode::IntentExclusive);
        db.check_ddl_table_lock(42).unwrap();
        db.table_locks_sharded
            .reinstate(0, 42, TableLockMode::Exclusive);
        assert!(db.check_ddl_table_lock(42).is_err());
    }
}
