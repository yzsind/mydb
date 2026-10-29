// Online WAL apply for the mydb-native physical-replication replica.
//
// `Database::apply_wal_record` is invoked by the replica's
// `crate::repl::replica::applier::Applier` immediately after the frame
// has been written into the local WAL via `LogManager::append_reserved`.
// The local WAL is now byte-identical to the master's, but nothing in
// the live `Database` (buffer pool, txn manager, MVCC SCN counter,
// catalog) has been mutated, so a local SELECT would still return stale
// data until a restart-recovery cycle. This pass closes that gap by
// replaying each frame against the running engine so the replica is
// queryable online.
//
// Strategy:
//
// 1. Heap mutations use a frame-scoped redo file cache: every heap file
//    in a physical WAL frame is opened once, and each page update performs
//    one read-modify-write with one final checksum. Generic page images still
//    reuse `recovery::redo_page_update`. Both paths set page LSNs and are
//    idempotent (an `lsn` ≤ existing page LSN is dropped). After each redo
//    we invalidate the affected live buffer-pool page so a concurrent SELECT
//    cannot serve a stale frame.
//
// 2. Index batches reuse the live `IndexFile` from
//    `Database::index_cache` when the catalog already knows the
//    `(table_id, index_name)` matching the WAL `path`. The
//    `logical_wal_lsn` guard suppresses re-emission of WAL during
//    `insert_internal` / `delete_internal`. For paths that the catalog
//    does not yet know about (e.g. an ADD INDEX whose catalog row
//    hasn't yet hit `CommitTxn`), we fall back to the recovery-style
//    `IndexFile::open_for_recovery` + replay path used by startup
//    recovery, and invalidate the live cache afterwards if a stale
//    handle exists.
//
// 3. Transaction control records mutate `TransactionManager` slots in
//    lock-step with what the master did: `BeginTxn` registers an
//    `Active` slot, `CommitTxn` flips the slot to
//    `Committed { commit_scn }`, `EndTxn` to `RolledBack`, and
//    `PrepareTxn` to `Prepared`. On `CommitTxn` we `fetch_max` the
//    master's `commit_scn` into `next_scn_atomic`, so any local
//    snapshot taken after the apply sees rows the master committed at
//    or before that SCN.
//
// 4. Data-dictionary writes are detected (any path `tables/<id>.heap`
//    with `id ≥ DD_TABLE_ID_BASE`). Their txn_id is recorded in
//    `applier_dd_dirty_txns`; on the matching `CommitTxn`, CatalogDelta
//    refreshes only affected table ids or the bounded non-table families.
//
// 5. Checkpoint records are replica-local. File lifecycle records and undo
//    records are applied online as well: promotion needs the undo directory to
//    physically compensate any source transaction abandoned at the role flip.
//
// The whole apply runs inside `with_replication_applier_pub`, so any
// sub-path that consults
// `crate::replication::replication_applier_active` (e.g. the role-based
// read-only guard) knows to let it through.

const MAX_ONLINE_REDO_FILES_PER_CHUNK: usize = 64;

#[derive(Default)]
struct OnlineHeapRedoFiles {
    files: HashMap<String, OnlineHeapRedoFile>,
}

/// Scratch state shared by every physical WAL frame in one receiver chunk.
///
/// The receiver caps chunks at `MYDB_REPL_CHUNK_BYTES` (1 MiB by default), and
/// the heap-file cache is independently bounded at 64 entries. The context is
/// dropped after the chunk, so it neither retains file descriptors nor adds a
/// long-lived resident-memory category to the database.
#[derive(Default)]
pub(crate) struct OnlineWalApplyContext {
    heap_redo: OnlineHeapRedoFiles,
}

struct OnlineHeapRedoFile {
    file: File,
    len: u64,
    owner: [u8; 16],
}

impl OnlineHeapRedoFiles {
    /// Mutate one heap page with a single read/write file handle. The cache is
    /// scoped to one physical WAL frame, so a `TxnBatch` reuses handles across
    /// all of its logical records without adding long-lived resident memory.
    fn mutate_page(
        &mut self,
        data_dir: &Path,
        relative: &str,
        page_id: u64,
        lsn: u64,
        mutate: impl FnOnce(&mut Page) -> Result<bool, QueryError>,
    ) -> Result<(), QueryError> {
        if !self.files.contains_key(relative) {
            // A pathological cross-schema chunk must not retain an unbounded
            // number of descriptors. Clearing is deliberately simple: the
            // common OLTP case has only a handful of heap paths, while a
            // 64-file chunk still amortizes thousands of per-frame opens.
            if self.files.len() >= MAX_ONLINE_REDO_FILES_PER_CHUNK {
                self.files.clear();
            }
            let path = data_dir.join(relative);
            let open = || {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&path)
            };
            let file = match open() {
                Ok(file) => file,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    if let Some(parent) = path.parent() {
                        fs::create_dir_all(parent).map_err(io_err)?;
                    }
                    open().map_err(io_err)?
                }
                Err(err) => return Err(io_err(err)),
            };
            let len = file.metadata().map_err(io_err)?.len();
            self.files.insert(
                relative.to_string(),
                OnlineHeapRedoFile {
                    file,
                    len,
                    owner: page_owner_result(&path)?,
                },
            );
        }
        let entry = self
            .files
            .get_mut(relative)
            .expect("online heap redo file inserted above");

        let offset = page_id.checked_mul(PAGE_SIZE as u64).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "WAL redo heap page offset overflow",
            )
        })?;
        let end = offset.checked_add(PAGE_SIZE as u64).ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "WAL redo heap page end overflow",
            )
        })?;
        let mut bytes = vec![0; PAGE_SIZE];
        if entry.len >= end {
            entry.file.seek(SeekFrom::Start(offset)).map_err(io_err)?;
            entry.file.read_exact(&mut bytes).map_err(io_err)?;
            validate_page_owner(&bytes, &entry.owner, page_id)?;
            if page_lsn(&bytes) >= lsn && recovery_page_valid(&bytes) {
                return Ok(());
            }
        }

        let mut page = Page::decode(&bytes)?;
        if !mutate(&mut page)? {
            return Ok(());
        }
        page.set_lsn(lsn);
        let mut bytes = page.encode_into_owned();
        stamp_page_owner(&mut bytes, &entry.owner, page_id);
        refresh_page_checksum(&mut bytes);
        if entry.len < end {
            entry.file.set_len(end).map_err(io_err)?;
            entry.len = end;
        }
        entry.file.seek(SeekFrom::Start(offset)).map_err(io_err)?;
        entry.file.write_all(&bytes).map_err(io_err)?;
        Ok(())
    }
}

impl Database {
    pub(crate) fn flush_replicated_undo_pending(&self) -> Result<(), QueryError> {
        self.undo.write().drain_replicated_pending_frames()
    }

    #[cfg(test)]
    /// Apply a single WAL record received from the master. See module
    /// docs for the design.
    ///
    /// Returns `Ok(())` for checkpoint records the replica deliberately
    /// ignores. Failures bubble up to the receiver
    /// loop so a corrupted master stream surfaces as a hard error
    /// instead of silent divergence.
    pub fn apply_wal_record(&self, record: &WalRecord) -> Result<(), QueryError> {
        let mut context = OnlineWalApplyContext::default();
        self.apply_wal_record_with_context(record, &mut context)
    }

    /// Apply one record while reusing bounded per-chunk redo scratch state.
    ///
    /// Production physical replication keeps one context for the whole
    /// network chunk. The public single-record wrapper above remains the
    /// correctness oracle for recovery tests and non-streaming callers.
    pub(crate) fn apply_wal_record_with_context(
        &self,
        record: &WalRecord,
        context: &mut OnlineWalApplyContext,
    ) -> Result<(), QueryError> {
        let _apply = self.replica_apply_checkpoint.read().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "replica apply/restartpoint lock poisoned",
            )
        })?;
        let result = crate::replication::with_replication_applier_pub(|| {
            with_physical_wal_apply(|| self.apply_wal_record_inner(record, &mut context.heap_redo))
        });
        if result.is_ok() {
            self.replica_applied_lsn_atomic
                .fetch_max(record.lsn, Ordering::Release);
        }
        result
    }

    fn apply_wal_record_inner(
        &self,
        record: &WalRecord,
        heap_redo: &mut OnlineHeapRedoFiles,
    ) -> Result<(), QueryError> {
        let lsn = record.lsn;
        let txn_id = record.txn_id;
        let _vector_write = match replica_payload_path(&record.payload) {
            Some(path) if path.ends_with(".hvec") || path.ends_with(".vmanifest") => {
                self.begin_vector_physical_redo(path)?
            }
            _ => None,
        };
        let _zone_operation = replica_payload_path(&record.payload)
            .filter(|path| path.ends_with(".zm"))
            .map(|_| self.zone_map_gate.read_recursive());
        let _zone_reload = match replica_payload_path(&record.payload) {
            Some(path) if path.ends_with(".zm") => self.retire_replayed_zone_map(path)?,
            _ => None,
        };
        if let Some(path) =
            replica_payload_path(&record.payload).filter(|path| path.ends_with(".zm"))
        {
            // Retire dirty/pinned frames and wait for any old flush before
            // overwriting disk. Invalidating only after redo allows an old
            // writeback to land after the new image and erase that redo.
            self.buffer_pool
                .discard_pages_for_path(&self.data_dir.join(path))?;
        }
        if txn_id != 0 && replica_payload_is_write(&record.payload) {
            let undo_head = match &record.payload {
                WalPayload::UndoRecord { undo_id, .. } => Some(*undo_id),
                _ => None,
            };
            self.mark_applied_txn_writer(txn_id, undo_head);
        }
        if let Some(path) = replica_payload_path(&record.payload) {
            if replica_payload_is_write(&record.payload) {
                if let Some(storage) = Self::parse_heap_table_id(path) {
                    self.note_replicated_hnsw_heap(txn_id, storage);
                }
            }
            let mut tracker = self.replica_restartpoint_tracker.lock().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "replica restartpoint tracker lock poisoned",
                )
            })?;
            tracker.register(path)?;
        }
        match &record.payload {
            WalPayload::TxnBatch { records } => {
                let first_logical_lsn = lsn
                    .checked_add(1)
                    .and_then(|end| end.checked_sub(records.len() as u64))
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "invalid WAL batch LSN span",
                        )
                    })?;
                let mut logical_prev_lsn = record.prev_txn_lsn;
                for (idx, payload) in records.iter().enumerate() {
                    let logical_lsn = first_logical_lsn + idx as u64;
                    self.apply_wal_record_inner(
                        &WalRecord {
                            source_time_us: record.source_time_us,
                            lsn: logical_lsn,
                            prev_txn_lsn: logical_prev_lsn,
                            txn_id,
                            payload: payload.clone(),
                        },
                        heap_redo,
                    )?;
                    logical_prev_lsn = logical_lsn;
                }
            }
            WalPayload::ZoneMapMergeBatch { bytes } => {
                // After-only set enlargement: replay is idempotent and is
                // deliberately independent of transaction outcome.
                self.apply_zone_map_wal(bytes)?;
            }
            WalPayload::SpatialIndexUndo { .. } | WalPayload::SpatialIndexClr { .. } => {}
            WalPayload::SpatialPageBatch { path, bytes } => {
                self.apply_spatial_page_batch(path, bytes, lsn)?;
            }
            WalPayload::VectorIndexManifest { path, bytes } => {
                redo_vector_manifest(&self.data_dir, path, bytes)?
            }
            WalPayload::VectorIndexImage {
                path,
                offset,
                total,
                bytes,
            } => {
                if *offset == 0 {
                    self.replica_vector_state
                        .lock()
                        .images
                        .insert(path.clone(), lsn);
                }
                redo_vector_image(&self.data_dir, path, *offset, *total, bytes, lsn)?;
                if offset.checked_add(bytes.len() as u64) == Some(*total) {
                    self.replica_vector_state.lock().images.remove(path);
                }
                self.invalidate_buffer_pool_path(path)?;
            }
            WalPayload::VectorIndexBatch { .. } | WalPayload::VectorIndexModel { .. } => {
                self.note_replica_vector_record(record)?
            }
            WalPayload::VectorIndexComplete { txn_id, commit_scn } => {
                self.complete_replica_vector_transaction(*txn_id, *commit_scn)?
            }
            WalPayload::HistoryIndexBatch { bytes } => {
                // Kept invisible until the matching CommitTxn installs the
                // commit-SCN ledger entry. Abort/loser batches are harmless.
                self.temporal_history
                    .reserve_commit_capacity(record.txn_id, bytes.len())?;
                self.temporal_history
                    .note_history_index_batch(txn_id, bytes)?;
            }
            WalPayload::CatalogDelta {
                object_ids,
                invalidate_all,
            } => {
                let mut deltas = self.applier_dd_dirty_txns.lock();
                let delta = deltas.entry(txn_id).or_default();
                delta.invalidate_all |= *invalidate_all;
                delta.object_ids.extend(object_ids.iter().copied());
            }
            WalPayload::SequenceState {
                sequence_id,
                counter_id,
                resume_value,
                is_called,
                logged_unix_us,
            } => {
                self.commit_hlc_unix_us
                    .fetch_max(*logged_unix_us, Ordering::AcqRel);
                self.replica_restartpoint_tracker
                    .lock()
                    .map_err(|_| sequence_state_error("replica tracker poisoned"))?
                    .register_sequence(*counter_id, lsn)?;
                self.apply_sequence_value(SequenceValue {
                    sequence_id: *sequence_id,
                    counter_id: *counter_id,
                    resume_value: *resume_value,
                    is_called: *is_called,
                    state_lsn: lsn,
                })?;
            }
            WalPayload::AutoIncrementAdvance {
                table_id,
                version,
                max_value,
            } => {
                self.replica_restartpoint_tracker
                    .lock()
                    .map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "replica restartpoint tracker lock poisoned",
                        )
                    })?
                    .register_auto_increment(lsn)?;
                self.auto_increment
                    .install(*table_id, *version, *max_value)?;
                if let Some(state) = self.auto_increment.get(*table_id) {
                    let mut logged = state.logged.lock();
                    if *version > logged.version
                        || (*version == logged.version && *max_value >= logged.max_value)
                    {
                        *logged = crate::storage::txn::AutoIncrementLoggedState {
                            version: *version,
                            max_value: *max_value,
                            lsn,
                        };
                    }
                }
            }
            WalPayload::HeapInsert {
                path,
                page_id,
                slot_id,
                payload,
                ..
            } => {
                heap_redo.mutate_page(&self.data_dir, path, *page_id as u64, lsn, |page| {
                    if page.read(*slot_id).is_none() {
                        page.insert_at_slot(*slot_id, payload).ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "WAL redo heap insert does not fit",
                            )
                        })?;
                    }
                    Ok(true)
                })?;
                self.note_dd_touch_if_needed(path, txn_id);
                self.discard_heap_page_only(path, *page_id as u64)?;
                self.refresh_heap_handle_if_grown(path, *page_id as u64);
                if crate::storage::core::heap_payload_counts_as_logical_row(payload) {
                    self.note_replicated_table_mutation(txn_id, path, 1, 1);
                }
            }
            WalPayload::DdlCommit { .. } => {
                // CommitTxn is the visibility/durability authority. This
                // marker lets recovery and diagnostics identify the DDL
                // transaction without introducing a second state change.
            }
            WalPayload::HeapInsertBatch { path, inserts } => {
                let mut by_page = BTreeMap::<u64, Vec<&WalHeapInsert>>::new();
                for insert in inserts {
                    by_page
                        .entry(insert.page_id as u64)
                        .or_default()
                        .push(insert);
                }
                for (page_id, page_inserts) in by_page {
                    heap_redo.mutate_page(&self.data_dir, path, page_id, lsn, |page| {
                        for insert in page_inserts {
                            if page.read(insert.slot_id).is_none() {
                                page.insert_at_slot(insert.slot_id, &insert.payload)
                                    .ok_or_else(|| {
                                        error(
                                            crate::error::catalog::ER_GET_ERRNO,
                                            "WAL redo heap insert batch does not fit",
                                        )
                                    })?;
                            }
                        }
                        Ok(true)
                    })?;
                }
                self.note_dd_touch_if_needed(path, txn_id);
                let mut max_page = 0u64;
                for insert in inserts {
                    let page = insert.page_id as u64;
                    if page > max_page {
                        max_page = page;
                    }
                    self.discard_heap_page_only(path, page)?;
                }
                self.refresh_heap_handle_if_grown(path, max_page);
                let logical_rows = inserts
                    .iter()
                    .filter(|insert| {
                        crate::storage::core::heap_payload_counts_as_logical_row(&insert.payload)
                    })
                    .count() as u64;
                self.note_replicated_table_mutation(
                    txn_id,
                    path,
                    logical_rows,
                    logical_rows.min(i64::MAX as u64) as i64,
                );
            }
            WalPayload::HeapUpdate {
                path,
                page_id,
                slot_id,
                before,
                after,
                ..
            } => {
                heap_redo.mutate_page(&self.data_dir, path, *page_id as u64, lsn, |page| {
                    if page.read(*slot_id).is_some() {
                        page.update(*slot_id, after)?;
                    } else {
                        let actual_slot = page.insert(after).ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "WAL redo heap update does not fit",
                            )
                        })?;
                        if actual_slot != *slot_id {
                            return Err(error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "WAL redo heap update slot mismatch",
                            ));
                        }
                    }
                    Ok(true)
                })?;
                self.note_dd_touch_if_needed(path, txn_id);
                self.discard_heap_page_only(path, *page_id as u64)?;
                self.refresh_heap_handle_if_grown(path, *page_id as u64);
                let before_live = crate::storage::core::heap_payload_live_logical_state(before);
                let after_live = crate::storage::core::heap_payload_live_logical_state(after);
                if before_live.is_some() || after_live.is_some() {
                    let live_delta = i64::from(after_live.unwrap_or(false))
                        - i64::from(before_live.unwrap_or(false));
                    self.note_replicated_table_mutation(txn_id, path, 1, live_delta);
                }
            }
            WalPayload::HeapUpdateLogical {
                path,
                page_id,
                slot_id,
                patches,
                ..
            } => {
                let owned_patches: Vec<(usize, Vec<u8>)> = patches
                    .iter()
                    .map(|patch| (patch.offset as usize, patch.after.clone()))
                    .collect();
                heap_redo.mutate_page(&self.data_dir, path, *page_id as u64, lsn, |page| {
                    if page.read(*slot_id).is_some() {
                        let _ = page.try_patch_columns(*slot_id, &owned_patches)?;
                    }
                    Ok(true)
                })?;
                self.note_dd_touch_if_needed(path, txn_id);
                self.discard_heap_page_only(path, *page_id as u64)?;
                self.refresh_heap_handle_if_grown(path, *page_id as u64);
                self.note_replicated_table_mutation(txn_id, path, 1, 0);
            }
            WalPayload::HeapDelete {
                path,
                page_id,
                slot_id,
                before,
                ..
            } => {
                heap_redo.mutate_page(&self.data_dir, path, *page_id as u64, lsn, |page| {
                    if page.read(*slot_id).is_some() {
                        page.delete(*slot_id)?;
                    }
                    Ok(true)
                })?;
                self.note_dd_touch_if_needed(path, txn_id);
                self.discard_heap_page_only(path, *page_id as u64)?;
                self.refresh_heap_handle_if_grown(path, *page_id as u64);
                let _ = before;
            }
            WalPayload::SlotReclaim {
                path,
                page_id,
                slot_id,
            } => {
                // rowid-6bytes M2 — apply the slot reclaim on this
                // engine's heap. Idempotent: a slot in ACTIVE or
                // FREE state is left untouched. Errors here mean the
                // heap file is missing or the page lock is poisoned
                // — the same conditions cause any other apply path
                // to fail, so propagate.
                heap_redo.mutate_page(
                    &self.data_dir,
                    path,
                    *page_id as u64,
                    lsn,
                    |page| {
                        if let Some(snapshot_scn) = self.oldest_active_snapshot_scn() {
                            if let Some((header, _, _)) = page
                                .read(*slot_id)
                                .and_then(crate::storage::core::peek_mvcc_header)
                            {
                                // The source can purge a tombstone before a
                                // replica-local reader has released its older
                                // view. Keep the physical row/undo anchor until
                                // that delete is visible to every local view.
                                // Fresh ordinary views register while sampling
                                // the already-published commit frontier, so a
                                // view registered after this check cannot need
                                // a pre-delete version absent from this sample.
                                let reader = TxnView::read_only(
                                    snapshot_scn,
                                    IsolationLevel::RepeatableRead,
                                );
                                if header.is_deleted() && !self.row_version_visible(header, reader)? {
                                    return Err(error(
                                        crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                                        format!(
                                            "physical replication replay blocked by active snapshot {snapshot_scn}: \
                                             slot reclaim at LSN {lsn} for {path} page {page_id} slot {slot_id}; \
                                             replay resumes after older readers finish",
                                        ),
                                    ));
                                }
                            }
                        }
                        Ok(page.try_reclaim_deleted_for_recovery(*slot_id))
                    },
                )?;
                self.note_dd_touch_if_needed(path, txn_id);
                self.discard_heap_page_only(path, *page_id as u64)?;
                self.refresh_heap_handle_if_grown(path, *page_id as u64);
            }
            WalPayload::PageUpdate {
                path,
                page_id,
                after,
                ..
            }
            | WalPayload::FullPageImage {
                path,
                page_id,
                after,
            }
            | WalPayload::Clr {
                path,
                page_id,
                after,
                ..
            } => {
                // Index files are replicated LOGICALLY (IndexBatch
                // covers every DML index op; DDL builds are rebuilt
                // from the local heap on catalog reload). The replica's
                // tree therefore has its OWN physical layout — the
                // master's page ids mean nothing here, and raw-writing
                // a master FPI/CLR image at that page id corrupts the
                // local tree ("corrupt index: invalid page magic").
                // Heap files stay physically identical, so their
                // page images apply as-is.
                if Self::path_is_index_file(path) {
                    return Ok(());
                }
                redo_page_update(&self.data_dir, path, *page_id, lsn, after)?;
                self.note_dd_touch_if_needed(path, txn_id);
                self.invalidate_buffer_pool_path(path)?;
            }
            WalPayload::FullPageImageCompressed { path, page_id, .. } => {
                if Self::path_is_index_file(path) {
                    return Ok(());
                }
                let after = crate::wal::wal_full_page_image_after(&record.payload)?;
                redo_page_update(&self.data_dir, path, *page_id, lsn, &after)?;
                self.note_dd_touch_if_needed(path, txn_id);
                self.invalidate_buffer_pool_path(path)?;
            }
            WalPayload::PageDelta { path, .. } => {
                if Self::path_is_index_file(path) {
                    return Ok(());
                }
                redo_page_delta(&self.data_dir, record)?;
                self.note_dd_touch_if_needed(path, txn_id);
                self.invalidate_buffer_pool_path(path)?;
            }
            WalPayload::PageUpdateCompressed { path, page_id, .. } => {
                if Self::path_is_index_file(path) {
                    return Ok(());
                }
                let (_, after) = crate::wal::wal_compressed_page_update_images(&record.payload)?;
                redo_page_update(&self.data_dir, path, *page_id, lsn, &after)?;
                self.note_dd_touch_if_needed(path, txn_id);
                self.invalidate_buffer_pool_path(path)?;
            }
            WalPayload::IndexBatch { path, ops } => {
                self.apply_index_batch_record(path, lsn, txn_id, ops)?;
            }
            WalPayload::IndexInsert { path, key, row_id } => {
                let ops = vec![WalIndexOp {
                    kind: WalIndexOpKind::Insert,
                    key: key.clone(),
                    row_id: *row_id,
                }];
                self.apply_index_batch_record(path, lsn, txn_id, &ops)?;
            }
            WalPayload::IndexDelete { path, key, row_id } => {
                let ops = vec![WalIndexOp {
                    kind: WalIndexOpKind::Delete,
                    key: key.clone(),
                    row_id: *row_id,
                }];
                self.apply_index_batch_record(path, lsn, txn_id, &ops)?;
            }
            WalPayload::BeginTxn { start_scn } => {
                self.apply_begin_txn(txn_id, *start_scn, lsn);
            }
            WalPayload::CommitTxn {
                commit_scn,
                commit_unix_us,
                ..
            } => {
                let schema_changed = self
                    .applier_dd_dirty_txns
                    .lock()
                    .get(&txn_id)
                    .is_some_and(|delta| delta.dd_touched);
                self.vector_snapshot_floor
                    .fetch_max(*commit_scn, Ordering::AcqRel);
                self.begin_replica_vector_commit(txn_id)?;
                self.apply_commit_txn(txn_id, *commit_scn)?;
                if schema_changed {
                    self.replicated_hnsw_table_cache.write().clear();
                }
                self.commit_hlc_unix_us
                    .fetch_max(*commit_unix_us, Ordering::AcqRel);
                if *commit_unix_us != 0 && self.temporal_history.has_history_index_batch(txn_id) {
                    self.note_temporal_commit(txn_id, *commit_scn, *commit_unix_us);
                    self.persist_temporal_commit(txn_id, *commit_scn, *commit_unix_us)?;
                    self.temporal_history
                        .persist_noted_history_index_batch(txn_id)?;
                }
            }
            WalPayload::AbortTxn => {
                // The source can stop between compensation batches. Keep its
                // remaining undo pinned and eligible for promotion rollback
                // until EndTxn confirms physical compensation is complete.
                self.apply_abort_txn(txn_id, false);
                self.temporal_history
                    .discard_uncommitted_history_index_batch(txn_id);
            }
            WalPayload::EndTxn => {
                self.replicated_hnsw_dirty.lock().remove(&txn_id);
                if self
                    .txns
                    .read()
                    .status(txn_id)
                    .is_some_and(|status| status.is_in_flight())
                {
                    self.apply_abort_txn(txn_id, true);
                }
                let delta = self.applier_dd_dirty_txns.lock().remove(&txn_id);
                if delta.is_some_and(|delta| delta.has_binlog_intent) {
                    crate::binlog::prepared::cleanup(&self.data_dir, txn_id);
                }
            }
            WalPayload::PrepareTxn {
                xid,
                prepare_scn,
                locks: _,
                mdl_locks: _,
                binlog: _,
            } => {
                self.apply_prepare_txn(txn_id, xid.clone(), *prepare_scn, lsn);
            }
            WalPayload::FileClr { path, action, .. } => {
                heap_redo.files.remove(path);
                if Self::path_is_index_file(path)
                    && matches!(action, crate::wal::FileCompensation::Restore(_))
                {
                    // Replica B-trees have a local physical layout. Rebuild
                    // from the authoritative heap, never install source pages.
                    if let Some((table_id, _)) = self.parse_index_table_and_name(path) {
                        if let Some(table) = self.table_by_id(table_id) {
                            self.rebuild_indexes(&table)?;
                        }
                    }
                } else {
                    apply_file_compensation(&self.data_dir, path, action)?;
                }
                self.invalidate_buffer_pool_path(path)?;
            }
            WalPayload::FileCreate { path, after } => {
                // The initial image only exists in this record — the
                // lazy `create(true)` in the redo helpers would leave a
                // zeroed first page and later opens would fail. Index
                // files are the exception: they replicate logically
                // (see PageUpdate above) and are materialized by the
                // catalog-reload repair, so a master meta image must
                // never clobber the locally-built tree.
                if Self::path_is_index_file(path) {
                    return Ok(());
                }
                heap_redo.files.remove(path);
                write_recovered_file(&self.data_dir, path, after)?;
                if path == "system/undo/undo.manifest" {
                    self.undo_lock()?.apply_source_manifest(after)?;
                }
                if path.starts_with("system/xa-binlog/") {
                    self.applier_dd_dirty_txns
                        .lock()
                        .entry(txn_id)
                        .or_default()
                        .has_binlog_intent = true;
                }
                self.note_dd_touch_if_needed(path, txn_id);
                self.invalidate_buffer_pool_path(path)?;
            }
            WalPayload::FileDelete { path, .. } => {
                heap_redo.files.remove(path);
                if let Err(err) = std::fs::remove_file(self.data_dir.join(path)) {
                    if err.kind() != std::io::ErrorKind::NotFound {
                        return Err(io_err(err));
                    }
                }
                self.invalidate_buffer_pool_path(path)?;
            }
            WalPayload::FileExtend { path, new_len, .. } => {
                let target = self.data_dir.join(path);
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent).map_err(io_err)?;
                }
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(target)
                    .map_err(io_err)?;
                if file.metadata().map_err(io_err)?.len() < *new_len {
                    file.set_len(*new_len).map_err(io_err)?;
                }
                self.invalidate_buffer_pool_path(path)?;
            }
            WalPayload::UndoRecord { undo_id, bytes } => {
                self.undo
                    .write()
                    .apply_replicated_wal_record(*undo_id, bytes)?;
            }
            WalPayload::RollbackTo {
                undo_head,
                compensated,
                ..
            } => {
                self.undo.write().mark_compensated(compensated);
                if let Some(slot) = self.txns.write().slots.get_mut(&txn_id) {
                    slot.undo_head.set(*undo_head);
                }
            }
            WalPayload::CheckpointBegin => {}
            WalPayload::CheckpointEnd {
                auto_increment_covered_lsn,
                sequence_covered_lsn,
                gc_safe_txn_frontier,
                next_txn_id,
                ..
            } => {
                let mut txns = self.txns.write();
                txns.registered_txn_frontier =
                    txns.registered_txn_frontier.max(*gc_safe_txn_frontier);
                txns.next_txn_id = txns.next_txn_id.max(*next_txn_id);
                drop(txns);
                self.next_txn_id_atomic
                    .fetch_max(*next_txn_id, Ordering::AcqRel);
                self.replica_restartpoint_tracker
                    .lock()
                    .map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "replica restartpoint tracker lock poisoned",
                        )
                    })?
                    .cover_auto_increment_before(*auto_increment_covered_lsn);
                self.replica_restartpoint_tracker
                    .lock()
                    .map_err(|_| sequence_state_error("replica tracker poisoned"))?
                    .cover_sequence_before(*sequence_covered_lsn);
                self.sequence_covered_lsn
                    .store(*sequence_covered_lsn, Ordering::Release);
            }
        }
        Ok(())
    }

    /// True if a heap path like `schemas/<db>/tables/<name>__<id>__data.heap`
    /// references a dictionary table (one whose `id` lives in the reserved
    /// `DD_TABLE_ID_BASE..` range).
    fn path_targets_dd_heap(path: &str) -> bool {
        let Some((_db, info)) = crate::storage::naming::parse_relative_object_path(path) else {
            return false;
        };
        matches!(info.kind, crate::storage::naming::ObjectFileKind::Table)
            && info.part == "data"
            && info.ext == "heap"
            && crate::storage::data_dictionary::is_dd_table_id(info.table_id)
    }

    /// B-tree and vector indexes are rebuilt locally from committed heap data.
    fn path_is_index_file(path: &str) -> bool {
        is_index_object_path(path)
    }

    fn note_dd_touch_if_needed(&self, path: &str, txn_id: u64) {
        if txn_id == 0 {
            return;
        }
        if Self::parse_heap_table_id(path)
            == Some(crate::storage::data_dictionary::DD_DDL_RETIREMENTS)
        {
            return;
        }
        if Self::path_targets_dd_heap(path) {
            self.applier_dd_dirty_txns
                .lock()
                .entry(txn_id)
                .or_default()
                .dd_touched = true;
        }
    }

    /// Per-page pool invalidation for the hot heap apply path. The
    /// redo helpers rewrote exactly `page_id` on disk; dropping just
    /// that page is O(1), where the whole-file scan in
    /// `invalidate_buffer_pool_path` walks every pool shard and was the
    /// dominant cost of replica apply under TPC-C.
    fn discard_heap_page_only(&self, relative: &str, page_id: u64) -> Result<(), QueryError> {
        let Some(table_id) = Self::parse_heap_table_id(relative) else {
            // Not a recognizable heap object — fall back to the
            // whole-file invalidation for safety.
            return self.invalidate_buffer_pool_path(relative);
        };
        let abs = self.data_dir.join(relative);
        let key = PageKey {
            file: Arc::new(BufferPool::heap_file_key(abs, table_id)),
            page_id,
        };
        self.buffer_pool.discard_page(&key)?;
        Ok(())
    }

    /// Drop the cached `HeapFile` handle when the redo touched a page
    /// at or beyond the handle's cached `page_count` — the file grew on
    /// disk and the stale handle would treat the new pages as
    /// nonexistent (SELECT misses freshly replicated rows). Page
    /// growth is rare relative to in-page updates, so the hot path
    /// stays O(1).
    fn refresh_heap_handle_if_grown(&self, relative: &str, page_id: u64) {
        let Some(table_id) = Self::parse_heap_table_id(relative) else {
            return;
        };
        let stale = self
            .heap_cache
            .read()
            .get(&table_id)
            .cloned()
            .map(|handle| handle.read().page_count().unwrap_or(0) <= page_id)
            .unwrap_or(false);
        if stale {
            let _ = self.remove_heap_cache_entry(table_id);
        }
    }

    fn invalidate_buffer_pool_path(&self, relative: &str) -> Result<(), QueryError> {
        let abs = self.data_dir.join(relative);
        self.buffer_pool.discard_pages_for_path(&abs)?;
        // The heap_cache / index_cache may hold an open `HeapFile` /
        // `IndexFile` whose own buffer-pool view of these pages is
        // separate. Drop the cached handle so the next access through
        // `cached_heap_handle` / `cached_index_handle` reopens it.
        // We don't know the table_id from the path alone for indexes,
        // so we scan both caches.
        if let Some(table_id) = Self::parse_heap_table_id(relative) {
            self.remove_heap_cache_entry(table_id)?;
        }
        // LOB allocation metadata is cached separately from buffer pages.
        // Retire both owners so promotion cannot allocate from a stale high
        // water mark and truncate already replicated chains.
        if let Some((_, info)) = crate::storage::naming::parse_relative_object_path(relative) {
            if info.ext == "lob" {
                self.remove_overflow_cache_entry(info.table_id)?;
                self.remove_heap_cache_entry(info.table_id)?;
            }
        }
        if let Some((table_id, index_name)) = self.parse_index_table_and_name(relative) {
            self.remove_index_cache_entry(table_id, &index_name)?;
        }
        Ok(())
    }

    fn parse_heap_table_id(relative: &str) -> Option<u64> {
        let (_db, info) = crate::storage::naming::parse_relative_object_path(relative)?;
        match info.kind {
            crate::storage::naming::ObjectFileKind::Table
                if info.part == "data" && info.ext == "heap" =>
            {
                Some(info.table_id)
            }
            _ => None,
        }
    }

    /// Resolve the logical index name carried indirectly by a stable
    /// `objects/indexes/<storage_id>/<generation>/<index_id>.<ext>` path.
    /// Legacy name-bearing paths remain readable only for defensive WAL
    /// diagnostics; the current format always takes the numeric branch.
    fn parse_index_table_and_name(&self, relative: &str) -> Option<(u64, String)> {
        let (_db, info) = crate::storage::naming::parse_relative_object_path(relative)?;
        if !matches!(info.kind, crate::storage::naming::ObjectFileKind::Index) {
            return None;
        }
        if info.ext != "rtree" && info.ext != "idx" && info.ext != "hnsw" && info.ext != "hvec" {
            return None;
        }
        // Startup keeps compact catalog headers without index definitions.
        // Resolve through the bounded descriptor cache by physical storage ID;
        // a numeric index ID must never be reinterpreted as a SQL index name.
        let table = self.table_by_id(info.table_id)?;
        let index = if let Ok(index_id) = info.part.parse::<u64>() {
            table
                .indexes
                .iter()
                .find(|index| index.index_id == index_id)
        } else {
            table
                .indexes
                .iter()
                .find(|index| crate::storage::naming::sanitize_fs_name(&index.name) == info.part)
        }?;
        Some((info.table_id, index.name.clone()))
    }

    fn apply_begin_txn(&self, txn_id: u64, start_scn: u64, lsn: u64) {
        if txn_id == 0 {
            return;
        }
        let frontier = {
            let mut txns = self.txns.write();
            txns.next_txn_id = txns.next_txn_id.max(txn_id + 1);
            txns.next_scn = txns.next_scn.max(start_scn + 1);
            txns.slots.entry(txn_id).or_insert(TxnSlot {
                txn_id,
                status: TxnStatus::Active,
                start_scn,
                started_unix_ms: current_unix_ms(),
                commit_scn: None,
                undo_head: UndoHead::default(),
                is_writer: false,
                // BEGIN does not prove READ ONLY: a later record can turn
                // this source transaction into a writer. Never let a page
                // visibility watermark cross it merely because no write has
                // arrived yet.
                read_only: false,
                wal_logged: true,
                xa_xid: None,
                begin_lsn: lsn,
            });
            // A base backup already contains all effects visible at the
            // source transaction's start snapshot. Fresh replicas therefore
            // must not wait for pre-snapshot SCNs that will never be streamed.
            txns.seed_commit_frontier(start_scn)
        };
        self.slot_status_cache.insert_active(txn_id);
        self.publish_visible_frontier(frontier);
        self.next_txn_id_atomic
            .fetch_max(txn_id + 1, Ordering::AcqRel);
        self.next_scn_atomic
            .fetch_max(start_scn + 1, Ordering::AcqRel);
    }

    fn mark_applied_txn_writer(&self, txn_id: u64, undo_head: Option<u64>) {
        let mut txns = self.txns.write();
        let newly_writer = {
            let Some(slot) = txns.slots.get_mut(&txn_id) else {
                return;
            };
            if !slot.status.is_in_flight() {
                return;
            }
            let newly_writer = !slot.is_writer;
            if newly_writer {
                slot.is_writer = true;
                slot.read_only = false;
            }
            if let Some(undo_head) = undo_head {
                // WAL is applied in logical record order. Each UndoRecord is the
                // newest source head at this point, so promotion rollback can use
                // the normal chain walker rather than merely flipping slot state.
                slot.undo_head.set(Some(undo_head));
            }
            newly_writer
        };
        if newly_writer {
            txns.active_write_count = txns.active_write_count.saturating_add(1);
        }
    }

    fn apply_commit_txn(&self, txn_id: u64, commit_scn: u64) -> Result<(), QueryError> {
        let ddl_ids = self
            .applier_dd_dirty_txns
            .lock()
            .get(&txn_id)
            .filter(|d| d.dd_touched)
            .map(|d| d.object_ids.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        let old_tables = ddl_ids
            .iter()
            .filter_map(|id| self.table_by_id(*id))
            .collect::<Vec<_>>();
        let requests: Vec<_> = old_tables
            .iter()
            .map(|t| MdlRequest {
                key: MdlObjectKey::table(&t.database, &t.name),
                lock_type: MdlLockType::Exclusive,
            })
            .collect();
        let _ddl_mdl = if requests.is_empty() {
            None
        } else {
            Some(self.lock_replicated_catalog(requests)?)
        };
        let old_index_states = old_tables
            .iter()
            .flat_map(|t| {
                t.indexes
                    .iter()
                    .map(move |i| ((t.id, i.index_id), (i.state, i.name.clone())))
            })
            .collect::<HashMap<_, _>>();
        if txn_id != 0 {
            let frontier = {
                let mut txns = self.txns.write();
                txns.next_txn_id = txns.next_txn_id.max(txn_id + 1);
                txns.next_scn = txns.next_scn.max(commit_scn + 1);
                let was_writer = if let Some(slot) = txns.slots.get_mut(&txn_id) {
                    let was_writer = slot.is_writer;
                    slot.status = TxnStatus::Committed;
                    slot.commit_scn = Some(commit_scn);
                    was_writer
                } else {
                    txns.slots.insert(
                        txn_id,
                        TxnSlot {
                            txn_id,
                            status: TxnStatus::Committed,
                            start_scn: commit_scn.saturating_sub(1),
                            started_unix_ms: 0,
                            commit_scn: Some(commit_scn),
                            undo_head: UndoHead::default(),
                            is_writer: false,
                            read_only: false,
                            wal_logged: true,
                            xa_xid: None,
                            begin_lsn: 0,
                        },
                    );
                    false
                };
                if was_writer {
                    txns.active_write_count = txns.active_write_count.saturating_sub(1);
                }
                // Mirror the terminal state inside the directory critical
                // section so lock-free status probes never lag the slot.
                self.slot_status_cache.mark_committed(txn_id, commit_scn);
                // P0-1: replicas/redo apply in LSN order, which can differ from
                // commit_scn order, so the frontier must be gap-aware here too.
                txns.mark_frontier_done(commit_scn)
            };
            self.publish_visible_frontier(frontier);
        }
        self.next_txn_id_atomic
            .fetch_max(txn_id.saturating_add(1), Ordering::AcqRel);
        self.next_scn_atomic
            .fetch_max(commit_scn + 1, Ordering::AcqRel);
        self.publish_pending_live_row_deltas(txn_id);
        // If this committing txn touched any data-dictionary table, the
        // replica's in-memory catalog is now stale (the on-disk DD
        // heaps reflect the new schema, but `self.catalog` is the
        // previous version). Apply the bounded CatalogDelta so subsequent SELECTs
        // (and apply paths that need to resolve a new path → table_id)
        // see the post-commit world.
        let delta = self.applier_dd_dirty_txns.lock().remove(&txn_id);
        if delta.as_ref().is_some_and(|delta| delta.has_binlog_intent) {
            crate::binlog::prepared::cleanup(&self.data_dir, txn_id);
        }
        if let Some(delta) = delta.filter(|delta| delta.dd_touched) {
            if !delta.object_ids.is_empty() {
                let object_ids = delta.object_ids.into_iter().collect::<Vec<_>>();
                let mut loaded = Vec::with_capacity(object_ids.len());
                for object_id in object_ids.iter().copied() {
                    match self.load_table_descriptor_from_dd(object_id) {
                        Ok(table) => loaded.push((object_id, table)),
                        Err(err) => {
                            crate::util::logging::log_message(format_args!(
                                "mydb-repl applier: catalog delta object_id={object_id} failed: {}",
                                err.message()
                            ));
                            self.quarantine_mvcc(format!(
                                "replicated catalog decode failed: {}",
                                err.message()
                            ));
                            return Err(err);
                        }
                    }
                }
                let requests: Vec<_> = loaded
                    .iter()
                    .filter_map(|(_, t)| t.as_ref())
                    .map(|t| MdlRequest {
                        key: MdlObjectKey::table(&t.database, &t.name),
                        lock_type: MdlLockType::Exclusive,
                    })
                    .collect();
                let _new_names = if requests.is_empty() {
                    None
                } else {
                    Some(self.lock_replicated_catalog(requests)?)
                };
                let prebuild = loaded.iter().filter_map(|(_, t)| t.as_ref()).all(|t| {
                    t.primary_publication_job_id != 0
                        && old_tables.iter().any(|old| {
                            old.id == t.id && old.active_generation == t.active_generation
                        })
                        && t.indexes.iter().any(|i| {
                            i.primary
                                && !matches!(
                                    old_index_states.get(&(t.id, i.index_id)),
                                    Some((IndexState::Public, _))
                                )
                        })
                });
                if prebuild {
                    if let Err(err) =
                        self.materialize_replicated_indexes(&loaded, &old_index_states, &old_tables)
                    {
                        self.quarantine_mvcc(format!(
                            "replicated index materialization failed: {}",
                            err.message()
                        ));
                        return Err(err);
                    }
                }
                if let Err(err) = self.retire_replicated_index_handles(&loaded, &old_tables) {
                    self.quarantine_mvcc(format!(
                        "replicated index retirement failed: {}",
                        err.message()
                    ));
                    return Err(err);
                }
                {
                    let mut catalog = self.catalog.write().map_err(|_| {
                        self.quarantine_mvcc("replicated catalog lock poisoned");
                        error(crate::error::catalog::ER_GET_ERRNO, "catalog lock poisoned")
                    })?;
                    for (object_id, table) in &loaded {
                        catalog
                            .tables
                            .retain(|_, existing| existing.id != *object_id);
                        if let Some(table) = table {
                            // Promotion allocates from the live catalog. Physical
                            // partition IDs share the same namespace as table IDs.
                            let max_id = table
                                .storage_ids()
                                .into_iter()
                                .chain(std::iter::once(table.id))
                                .max()
                                .unwrap_or(table.id);
                            catalog.next_table_id =
                                catalog.next_table_id.max(max_id.saturating_add(1));
                            for column in &table.columns {
                                self.read_definition_highwater
                                    .fetch_max(column.not_null_from_scn, Ordering::Release);
                            }
                            for index in &table.indexes {
                                self.read_definition_highwater
                                    .fetch_max(index.readable_from_scn, Ordering::Release);
                            }
                            self.primary_definition_highwater
                                .fetch_max(table.primary_definition_scn, Ordering::Release);
                            if table.primary_definition_scn != 0 {
                                self.table_locks_sharded
                                    .set_schema_floor(table.id, table.schema_version);
                                for id in table.storage_ids() {
                                    self.table_locks_sharded
                                        .set_schema_floor(id, table.schema_version);
                                }
                            }
                            catalog.tables.insert(table.qualified_name(), table.clone());
                        }
                    }
                }
                // CatalogManager keeps a compact id→key index and bounded
                // descriptor/header caches outside the live Catalog value.
                // WAL apply must publish those structures as well; updating
                // only `live.tables` leaves a freshly replicated CREATE
                // invisible to normal table lookup until restart.
                self.catalog.publish_table_deltas(&object_ids);
                for (_, table) in &loaded {
                    let Some(table) = table else { continue };
                    let Some(previous) = old_tables.iter().find(|old| old.id == table.id) else {
                        continue;
                    };
                    if previous.stats.analyzed_at_unix_ms != table.stats.analyzed_at_unix_ms {
                        // The DD row includes the baselines folded by the
                        // source ANALYZE. Replay only those subtractions;
                        // DML committed after its snapshot stays visible.
                        if let Some(counter) = self.table_live_row_deltas.read().get(&table.id) {
                            let baseline = table.stats.last_analyze_live_baseline;
                            let _ = counter.fetch_update(
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                                |current| {
                                    let aligned = if baseline >= 0 {
                                        baseline.min(current.max(0))
                                    } else {
                                        baseline.max(current.min(0))
                                    };
                                    Some(current.saturating_sub(aligned))
                                },
                            );
                        }
                        if let Some(counter) = self.table_modification_deltas.read().get(&table.id)
                        {
                            let baseline = table.stats.last_analyze_modification_baseline;
                            let _ = counter.fetch_update(
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                                |current| Some(current.saturating_sub(baseline)),
                            );
                        }
                    }
                }
                if !prebuild {
                    if let Err(err) =
                        self.materialize_replicated_indexes(&loaded, &old_index_states, &old_tables)
                    {
                        self.quarantine_mvcc(format!(
                            "replicated index materialization failed: {}",
                            err.message()
                        ));
                        return Err(err);
                    }
                }
                if !delta.invalidate_all {
                    return Ok(());
                }
            }
            match self.load_non_table_families_from_store() {
                Ok(loaded) => {
                    {
                        let mut catalog = self.catalog.write().map_err(|_| {
                            self.quarantine_mvcc("replicated catalog lock poisoned");
                            error(crate::error::catalog::ER_GET_ERRNO, "catalog lock poisoned")
                        })?;
                        catalog.next_table_id = catalog.next_table_id.max(loaded.next_table_id);
                        catalog.next_sequence_id =
                            catalog.next_sequence_id.max(loaded.next_sequence_id);
                        catalog.databases = loaded.databases;
                        catalog.views = loaded.views;
                        catalog.routines = loaded.routines;
                        catalog.triggers = loaded.triggers;
                        catalog.events = loaded.events;
                        catalog.sequences = loaded.sequences;
                        catalog.auth = loaded.auth;
                    }
                    self.initialize_sequence_runtimes()?;
                    self.initialize_sequence_sync()?;
                    self.publish_auth_snapshot().map_err(|err| {
                        self.quarantine_mvcc(format!(
                            "replicated auth publication failed: {}",
                            err.message()
                        ));
                        err
                    })?;
                }
                Err(err) => {
                    crate::util::logging::log_message(format_args!(
                        "mydb-repl applier: catalog reload after commit_scn={commit_scn} failed: {}",
                        err.message()
                    ));
                    self.quarantine_mvcc(format!(
                        "replicated catalog reload failed: {}",
                        err.message()
                    ));
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    fn apply_abort_txn(&self, txn_id: u64, complete: bool) {
        if txn_id == 0 {
            return;
        }
        {
            let mut txns = self.txns.write();
            let status = if complete {
                TxnStatus::RolledBack
            } else {
                TxnStatus::Active
            };
            let was_writer = if let Some(slot) = txns.slots.get_mut(&txn_id) {
                // Replaying an old intent must never resurrect a terminal
                // transaction. A prepared abort becomes an active loser.
                if !complete && !slot.status.is_in_flight() {
                    return;
                }
                let was_writer = slot.is_writer && slot.status.is_in_flight();
                slot.status = status;
                was_writer
            } else {
                txns.slots.insert(
                    txn_id,
                    TxnSlot {
                        txn_id,
                        status,
                        start_scn: 0,
                        started_unix_ms: 0,
                        commit_scn: None,
                        undo_head: UndoHead::default(),
                        is_writer: false,
                        read_only: false,
                        wal_logged: true,
                        xa_xid: None,
                        begin_lsn: 0,
                    },
                );
                false
            };
            if complete && was_writer {
                txns.active_write_count = txns.active_write_count.saturating_sub(1);
            }
            if complete {
                self.slot_status_cache.mark_rolled_back(txn_id);
            } else {
                self.slot_status_cache.insert_active(txn_id);
            }
        }
        self.discard_pending_live_row_deltas(txn_id);
        let delta = self.applier_dd_dirty_txns.lock().remove(&txn_id);
        if delta.is_some_and(|delta| delta.has_binlog_intent) {
            crate::binlog::prepared::cleanup(&self.data_dir, txn_id);
        }
    }

    fn note_replicated_table_mutation(
        &self,
        txn_id: u64,
        path: &str,
        modified_rows: u64,
        live_delta: i64,
    ) {
        if txn_id == 0 || (modified_rows == 0 && live_delta == 0) {
            return;
        }
        let Some((_database, object)) = crate::storage::naming::parse_relative_object_path(path)
        else {
            return;
        };
        if object.kind != crate::storage::naming::ObjectFileKind::Table {
            return;
        }
        let storage_id = object.table_id;
        let table_id = self.catalog.read().ok().and_then(|catalog| {
            catalog
                .tables
                .values()
                .find(|table| {
                    !table.system
                        && (table.id == storage_id || table.storage_ids().contains(&storage_id))
                })
                .map(|table| table.id)
        });
        let Some(table_id) = table_id else {
            return;
        };
        self.record_table_modifications_by_id(table_id, modified_rows);
        self.record_pending_live_row_delta(txn_id, table_id, live_delta);
    }

    fn apply_prepare_txn(&self, txn_id: u64, xid: crate::xa::Xid, prepare_scn: u64, lsn: u64) {
        if txn_id == 0 {
            return;
        }
        {
            let mut txns = self.txns.write();
            txns.next_txn_id = txns.next_txn_id.max(txn_id + 1);
            txns.next_scn = txns.next_scn.max(prepare_scn + 1);
            let slot = txns.slots.entry(txn_id).or_insert(TxnSlot {
                txn_id,
                status: TxnStatus::Active,
                start_scn: prepare_scn.saturating_sub(1),
                started_unix_ms: current_unix_ms(),
                commit_scn: None,
                undo_head: UndoHead::default(),
                is_writer: false,
                read_only: false,
                wal_logged: true,
                xa_xid: None,
                begin_lsn: lsn,
            });
            if slot.status == TxnStatus::Active {
                slot.status = TxnStatus::Prepared;
                slot.xa_xid = Some(xid);
            }
            // P0-1: prepare_scn is drawn from the allocation counter, so it must
            // be resolved on the frontier (the PREPARED txn stays invisible via
            // its status) or the contiguous frontier stalls.
            let frontier = txns.mark_frontier_done(prepare_scn);
            self.publish_visible_frontier(frontier);
        }
        self.slot_status_cache.mark_prepared(txn_id);
        self.next_txn_id_atomic
            .fetch_max(txn_id + 1, Ordering::AcqRel);
        self.next_scn_atomic
            .fetch_max(prepare_scn + 1, Ordering::AcqRel);
    }

    fn apply_spatial_page_batch(
        &self,
        path: &str,
        bytes: &[u8],
        lsn: u64,
    ) -> Result<(), QueryError> {
        if let Some((txn_id, key, row_id)) = spatial_index::deleted_key(bytes)? {
            if let Some((storage_id, name)) = self.parse_index_table_and_name(path) {
                let head = self
                    .txns
                    .read()
                    .slot(txn_id)
                    .filter(|s| s.status.is_in_flight())
                    .map(|s| s.undo_head.clone());
                if let Some(head) = head {
                    self.index_repair_state.register(
                        txn_id,
                        [(
                            IndexRepairKey {
                                table_id: storage_id,
                                index_name: normalize_name(&name),
                                key,
                            },
                            row_id,
                            true,
                        )],
                        &head,
                        &self.index_repair_epochs,
                        &self.index_repair_version,
                    )?;
                }
            }
        }
        let abs = self.data_dir.join(path);
        let cache = self.index_cache.write();
        let handle = cache.values().find(|h| h.read().path == abs).cloned();
        if let Some(handle) = handle {
            let index = handle.read();
            index
                .spatial
                .as_ref()
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_INDEX_CORRUPT,
                        "invalid spatial replay target",
                    )
                })?
                .replay(&self.data_dir, bytes, lsn)?;
        } else {
            spatial_index::redo(&self.data_dir, path, bytes, lsn)?;
            self.buffer_pool.discard_pages_for_path(&abs)?;
        }
        Ok(())
    }

    fn apply_index_batch_record(
        &self,
        path: &str,
        lsn: u64,
        txn_id: u64,
        ops: &[WalIndexOp],
    ) -> Result<(), QueryError> {
        // Fast path: catalog already knows this (table_id, index_name).
        // Apply via the live IndexFile so concurrent SELECTs see the
        // changes immediately.
        if let Some((table_id, index_name)) = self.parse_index_table_and_name(path) {
            self.note_replicated_hnsw(txn_id, table_id, &index_name);
            let entries = ops
                .iter()
                .map(|op| {
                    Ok(IndexBatchEntry {
                        kind: op.kind,
                        key: decode_wal_index_key(&op.key)?,
                        row_id: wal_row_id_to_row_id(op.row_id),
                    })
                })
                .collect::<Result<Vec<_>, QueryError>>()?;
            // Removing a source key before COMMIT must not hide its old
            // committed version from local snapshots. Use the same governed
            // repair registry as ordinary DML, before changing the B-tree.
            // Pure INSERT batches keep their existing lock/registration path.
            if txn_id != 0
                && entries
                    .iter()
                    .any(|entry| entry.kind == WalIndexOpKind::Delete)
            {
                let head = self
                    .txns
                    .read()
                    .slot(txn_id)
                    .filter(|slot| slot.status.is_in_flight())
                    .map(|slot| slot.undo_head.clone());
                if let Some(head) = head {
                    let table = self.table_by_id(table_id);
                    let owner = table.as_ref().map_or(table_id, |table| table.id);
                    let single_key = table
                        .as_ref()
                        .and_then(|table| {
                            table
                                .indexes
                                .iter()
                                .find(|index| index.name.eq_ignore_ascii_case(&index_name))
                        })
                        .is_none_or(|index| {
                            !index.is_fulltext() && !index.has_multi_valued_parts()
                        });
                    self.index_repair_state.register(
                        txn_id,
                        entries
                            .iter()
                            .filter(|entry| entry.kind == WalIndexOpKind::Delete)
                            .map(|entry| {
                                (
                                    IndexRepairKey {
                                        table_id: owner,
                                        index_name: normalize_name(&index_name),
                                        key: entry.key.clone(),
                                    },
                                    entry.row_id,
                                    single_key,
                                )
                            }),
                        &head,
                        &self.index_repair_epochs,
                        &self.index_repair_version,
                    )?;
                }
            }
            let handle = self
                .cached_index_handle(table_id, &index_name)
                .map_err(|err| {
                    err.map_message(|message| {
                        format!("apply index open {path} (lsn {lsn}): {message}")
                    })
                })?;
            let index = handle.write();
            index.apply_replicated_batch(txn_id, entries, lsn)?;
            // No flush here: index pages stay dirty in the shared
            // buffer pool (master PageUpdate/FileCreate images are
            // skipped for index files, so nothing discards them), and
            // a per-batch flush would F_FULLFSYNC the file every few
            // frames — measured as the dominant cost of replica apply.
            // Durability comes from the local WAL (IndexBatch replay
            // on crash recovery) plus the restartpoint page flush.
            return Ok(());
        }
        // Slow path: catalog hasn't been reloaded yet (e.g. a CREATE
        // INDEX whose DD row commit is still pending). Reuse the
        // recovery helper which writes raw bytes to disk; invalidate
        // the buffer pool for that file so any subsequent live read
        // refreshes from disk.
        let log = std::sync::Arc::clone(&self.log);
        let mut recovery_indexes: std::collections::HashMap<String, IndexFile> =
            std::collections::HashMap::new();
        redo_index_batch(
            &self.data_dir,
            log,
            &mut recovery_indexes,
            path,
            lsn,
            txn_id,
            ops,
        )?;
        let _routing = btree_logical_wal::LsnScope::new(None);
        for index in recovery_indexes.values_mut() {
            index.flush()?;
        }
        self.invalidate_buffer_pool_path(path)?;
        Ok(())
    }
}

fn replica_payload_is_write(payload: &WalPayload) -> bool {
    !matches!(
        payload,
        WalPayload::BeginTxn { .. }
            | WalPayload::CommitTxn { .. }
            | WalPayload::AbortTxn
            | WalPayload::EndTxn
            | WalPayload::CheckpointBegin
            | WalPayload::CheckpointEnd { .. }
    )
}

fn replica_payload_path(payload: &WalPayload) -> Option<&str> {
    match payload {
        WalPayload::VectorIndexImage { path, .. }
        | WalPayload::VectorIndexManifest { path, .. }
        | WalPayload::PageUpdate { path, .. }
        | WalPayload::PageDelta { path, .. }
        | WalPayload::PageUpdateCompressed { path, .. }
        | WalPayload::FullPageImage { path, .. }
        | WalPayload::FullPageImageCompressed { path, .. }
        | WalPayload::FileClr { path, .. }
        | WalPayload::FileCreate { path, .. }
        | WalPayload::FileDelete { path, .. }
        | WalPayload::FileExtend { path, .. }
        | WalPayload::Clr { path, .. }
        | WalPayload::HeapInsert { path, .. }
        | WalPayload::HeapInsertBatch { path, .. }
        | WalPayload::HeapUpdate { path, .. }
        | WalPayload::HeapUpdateLogical { path, .. }
        | WalPayload::HeapDelete { path, .. }
        | WalPayload::SlotReclaim { path, .. }
        | WalPayload::IndexInsert { path, .. }
        | WalPayload::IndexDelete { path, .. }
        | WalPayload::IndexBatch { path, .. } => Some(path.as_str()),
        _ => None,
    }
}
