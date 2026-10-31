//! Replica WAL applier. Takes raw WAL frame bytes received from the
//! master and:
//!   1. writes them to the local WAL via `LogManager::append_reserved`,
//!      preserving the master's LSN/txn_id structure so the local
//!      segments end up byte-identical to the master's (modulo the
//!      segment-level file headers, which embed the local instance's
//!      `current_unix_ms`);
//!   2. calls `Database::apply_wal_record` so the live storage layer
//!      (buffer pool, transaction manager, MVCC SCN counter, catalog)
//!      reflects each frame as soon as it lands — i.e. a `SELECT`
//!      issued on the replica returns the master's freshly committed
//!      rows without waiting for restart-recovery.
//!
//! These two steps are paired: durability comes from the local WAL
//! write; visibility for online queries comes from
//! `apply_wal_record`. The applier maintains a small in-memory
//! bookkeeping of committed/aborted transactions used by the
//! receiver's ack pacing.

use crate::repl::now_ms;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::repl::replica::progress::{load_progress, save_progress, ReplicaProgress};
use crate::storage::{error, Database, OnlineWalApplyContext};
use crate::types::QueryError;

fn replica_poison_error(detail: impl Into<String>) -> QueryError {
    let detail = detail.into();
    QueryError::internal_control(
        crate::error::InternalErrorKind::ReplicaPoisonFrame,
        crate::error::catalog::ER_UNKNOWN_ERROR,
        "Unknown error",
    )
    .with_diagnostic("MYDB-DIAG-REPL-POISON", detail)
}
pub(crate) fn overlap_read_error(err: crate::wal::WalReaderError) -> QueryError {
    match err {
        crate::wal::WalReaderError::Io(err) => err,
        other => replica_poison_error(format!("unverifiable WAL overlap: {other:?}")),
    }
}

use crate::wal::{decode_frame_for_replication, WalPayload, WalReadChunk, WalRecord};

/// Per-batch outcome reported back to the receiver, used to decide
/// when to send a `ReplicaAck`.
#[derive(Debug, Clone, Copy)]
pub struct ApplyOutcome {
    pub frames_applied: u64,
    pub last_lsn_applied: u64,
    pub committed_txns: u64,
}

#[derive(Debug)]
pub struct Applier {
    data_dir: std::path::PathBuf,
    log: Arc<crate::wal::LogMutex>,
    /// Optional live `Database` whose buffer pool / txn manager /
    /// catalog get mutated by `apply_wal_record`. The constructor used
    /// by integration tests for raw WAL-byte fidelity (no online apply)
    /// leaves this `None`; production callers supply the engine's
    /// `Arc<Database>` so a `SELECT` on the replica sees the master's
    /// commits without restart.
    db: Option<Arc<Database>>,
    progress: ReplicaProgress,
    /// Set of txn ids we've seen a CommitTxn for during this run.
    committed: HashMap<u64, u64>,
    /// Next LSN to append into the local WAL. Frames below this are
    /// already in the local WAL from this process (possibly not yet
    /// fsynced) — re-appending them after a reconnect would create
    /// duplicate LSNs. Initialized from the LogManager at construction.
    local_wal_next: u64,
    /// Highest LSN successfully projected into the live database this run.
    /// This is intentionally independent from the local WAL end: append may
    /// succeed and online apply may fail.
    applied_lsn: u64,
    /// Group-fsync pacing: fsync the local WAL (and advance the
    /// durable progress/ack watermark) at most every few milliseconds
    /// during catch-up instead of once per chunk. A per-chunk fsync
    /// capped apply throughput at ~2.3k frames/s under TPC-C.
    last_sync_at: std::time::Instant,
    sync_interval: std::time::Duration,
    /// Pending master identity for the next progress save.
    pending_master: Option<(String, u64)>,
    /// Last LSN written to `replica_progress.json`. The in-memory durable
    /// watermark may run ahead between metadata checkpoints; restart resumes
    /// from the local durable WAL end, so this does not weaken WAL durability.
    persisted_lsn: u64,
    last_progress_persist_at: std::time::Instant,
}

/// Group-fsync window for the applier. Apply keeps running ahead of
/// the durable watermark; crash recovery re-fetches anything past it.
const APPLIER_SYNC_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);
/// `replica_progress.json` uses two full syncs (file + parent directory).
/// Keep that crash-safe protocol, but checkpoint it independently of the
/// 50 ms WAL/ACK cadence. The durable local WAL is the restart authority.
const PROGRESS_PERSIST_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// Upper bound on the in-memory committed-txn tracking map before it is
/// cleared to avoid unbounded growth (C14). Far above any realistic
/// count of concurrently-open transactions.
const MAX_TRACKED_COMMITTED_TXNS: usize = 262_144;

impl Applier {
    /// Legacy constructor used by the WAL-fidelity integration tests
    /// (no live `Database`). New callers prefer
    /// [`Applier::with_database`].
    pub fn new(
        data_dir: impl AsRef<Path>,
        log: Arc<crate::wal::LogMutex>,
        initial_progress: ReplicaProgress,
    ) -> Self {
        let data_dir = data_dir.as_ref().to_path_buf();
        let local_wal_next = log.lock().next_lsn();
        let persisted_lsn = load_progress(&data_dir)
            .ok()
            .flatten()
            .map(|progress| progress.durable_applied_lsn)
            .unwrap_or(initial_progress.durable_applied_lsn);
        Self {
            data_dir,
            log,
            db: None,
            applied_lsn: initial_progress
                .applied_lsn
                .max(initial_progress.durable_applied_lsn),
            progress: initial_progress,
            committed: HashMap::new(),
            local_wal_next,
            last_sync_at: std::time::Instant::now(),
            sync_interval: APPLIER_SYNC_INTERVAL,
            pending_master: None,
            persisted_lsn,
            last_progress_persist_at: std::time::Instant::now(),
        }
    }

    /// Construct an applier that also drives `Database::apply_wal_record`
    /// after each WAL frame is durable on the replica, so a local
    /// `SELECT` reflects master commits without waiting for a restart.
    pub fn with_database(
        data_dir: impl AsRef<Path>,
        log: Arc<crate::wal::LogMutex>,
        db: Arc<Database>,
        mut initial_progress: ReplicaProgress,
    ) -> Self {
        let data_dir = data_dir.as_ref().to_path_buf();
        let local_wal_next = log.lock().next_lsn();
        let persisted_lsn = load_progress(&data_dir)
            .ok()
            .flatten()
            .map(|progress| progress.durable_applied_lsn)
            .unwrap_or(initial_progress.durable_applied_lsn);
        // A reconnect can follow a partially applied chunk whose progress
        // file still describes an earlier durable prefix. The live database
        // already published every successful record up to this watermark;
        // replaying its BeginTxn records would temporarily hide committed
        // rows through the slot-status cache. Failed records never advance
        // this watermark. Keep durable_applied_lsn unchanged: it remains the
        // durable ACK/restart truth, independently of online visibility.
        let applied_lsn = initial_progress
            .applied_lsn
            .max(initial_progress.durable_applied_lsn)
            .max(db.replica_applied_lsn());
        initial_progress.applied_lsn = applied_lsn;
        Self {
            data_dir,
            log,
            db: Some(db),
            applied_lsn,
            progress: initial_progress,
            committed: HashMap::new(),
            local_wal_next,
            last_sync_at: std::time::Instant::now(),
            sync_interval: APPLIER_SYNC_INTERVAL,
            pending_master: None,
            persisted_lsn,
            last_progress_persist_at: std::time::Instant::now(),
        }
    }

    pub fn last_applied_lsn(&self) -> u64 {
        self.progress.durable_applied_lsn
    }

    pub fn progress(&self) -> ReplicaProgress {
        self.progress.clone()
    }

    pub fn set_sync_interval(&mut self, interval: std::time::Duration) {
        self.sync_interval = interval.max(std::time::Duration::from_millis(1));
    }

    #[allow(clippy::too_many_arguments)]
    pub fn note_source_status(
        &mut self,
        current_lsn: u64,
        durable_lsn: u64,
        timeline_uuid: String,
        generation: u64,
        frozen: bool,
        fence_lsn: u64,
        fence_token_hash: String,
    ) {
        self.progress.source_current_lsn = current_lsn;
        self.progress.source_durable_lsn = durable_lsn;
        self.progress.timeline_uuid = timeline_uuid;
        self.progress.source_generation = generation;
        self.progress.source_frozen = frozen;
        self.progress.source_fence_lsn = fence_lsn;
        self.progress.source_fence_token_hash = fence_token_hash;
        self.progress.last_update_ms = now_ms();
    }

    /// Apply one chunk of WAL frames. Returns the per-batch outcome.
    /// Frames whose LSN is `<= last_applied_lsn` are skipped (replay
    /// after reconnect is idempotent because LSNs are monotonic).
    pub fn apply_chunk(
        &mut self,
        chunk: &WalReadChunk,
        master_uuid: &str,
        master_epoch: u64,
    ) -> Result<ApplyOutcome, QueryError> {
        // Pass 1: decode every frame past our applied watermark. While
        // decoding, cross-check the master-declared chunk metadata
        // (frame_count / start_lsn / end_lsn) against what the bytes
        // actually contain (C10). Frame CRCs already catch bit-flips and
        // the frame CRC in `decode_frame_for_replication` catches torn
        // payloads, but a master-side framing bug (miscounted frames,
        // mismatched end_lsn) would otherwise slip through — this is the
        // defense-in-depth layer that turns such a bug into a loud error
        // instead of a silent divergence.
        let mut decoded: Vec<(crate::wal::ReplicationFrame, usize)> = Vec::new();
        let mut overlap_reader = None;
        let mut cursor = 0usize;
        let mut total_frames = 0u32;
        let mut first_all_lsn = 0u64;
        let mut last_all_lsn = 0u64;
        let mut previous_logical_end = None;
        while cursor < chunk.bytes.len() {
            let frame_offset = cursor;
            let frame = decode_frame_for_replication(&chunk.bytes[cursor..])
                .map_err(|err| replica_poison_error(format!("apply decode: {}", err.message())))?;
            cursor += frame.frame_len;
            let logical_start_lsn = match &frame.payload {
                WalPayload::TxnBatch { records } => frame
                    .header
                    .lsn
                    .checked_add(1)
                    .and_then(|end| end.checked_sub(records.len() as u64))
                    .ok_or_else(|| replica_poison_error("invalid WAL batch LSN span"))?,
                _ => frame.header.lsn,
            };
            // A new frame cannot partially overlap a different local framing.
            if logical_start_lsn < self.local_wal_next && frame.header.lsn >= self.local_wal_next {
                return Err(replica_poison_error(
                    "conflicting WAL frame across the local prefix",
                ));
            }
            // Verify replayed history before either skipping or projecting it.
            if frame.header.lsn < self.local_wal_next {
                if overlap_reader.is_none() {
                    overlap_reader = Some(
                        crate::wal::WalReader::open(&self.log, logical_start_lsn)
                            .map_err(overlap_read_error)?,
                    );
                }
                let local = overlap_reader
                    .as_mut()
                    .unwrap()
                    .next_chunk(frame.header.lsn, frame.frame_len)
                    .map_err(overlap_read_error)?
                    .ok_or_else(|| replica_poison_error("missing local WAL overlap"))?;
                if local.bytes != chunk.bytes[frame_offset..cursor] {
                    return Err(replica_poison_error(
                        "conflicting local WAL at the same LSN; rebuild replica",
                    ));
                }
            }
            if total_frames == 0 {
                first_all_lsn = logical_start_lsn;
            }
            last_all_lsn = frame.header.lsn;
            total_frames += 1;
            if frame.header.lsn <= self.applied_lsn {
                continue;
            }
            // Master LSNs are dense — a gap means frames were silently
            // skipped somewhere (network framing, filter bug). Fail
            // loudly instead of diverging quietly.
            if let Some(prev_end) = previous_logical_end {
                if logical_start_lsn != prev_end + 1 {
                    return Err(replica_poison_error(format!(
                        "WAL stream gap: frame logical start {} follows {}",
                        logical_start_lsn, prev_end
                    )));
                }
            } else if self.applied_lsn != 0 && logical_start_lsn > self.applied_lsn + 1 {
                return Err(replica_poison_error(format!(
                    "WAL stream gap: chunk starts at {} but applied watermark is {}",
                    logical_start_lsn, self.applied_lsn
                )));
            }
            previous_logical_end = Some(frame.header.lsn);
            decoded.push((frame, frame_offset));
        }
        // Cross-check declared metadata vs decoded reality (only when the
        // master populated it — `frame_count == 0` is a synthetic/empty
        // chunk from a raw-bytes test).
        if chunk.frame_count != 0 {
            if total_frames != chunk.frame_count {
                return Err(replica_poison_error(format!(
                    "WAL chunk frame_count mismatch: declared {} but decoded {}",
                    chunk.frame_count, total_frames
                )));
            }
            if first_all_lsn != chunk.start_lsn || last_all_lsn != chunk.end_lsn {
                return Err(replica_poison_error(format!(
                    "WAL chunk LSN range mismatch: declared [{}, {}] but decoded [{}, {}]",
                    chunk.start_lsn, chunk.end_lsn, first_all_lsn, last_all_lsn
                )));
            }
        }
        if decoded.is_empty() {
            return Ok(ApplyOutcome {
                frames_applied: 0,
                last_lsn_applied: self.progress.durable_applied_lsn,
                committed_txns: 0,
            });
        }

        // Pass 2: append the whole batch under ONE WAL lock. Frames
        // already in the local WAL from this process (reconnect replay
        // past the durable watermark) are skipped to avoid duplicate
        // LSNs. Online apply writes physical pages directly, so the whole
        // chunk must be durable before any of its pages can reach the OS.
        // One barrier per chunk preserves batching without violating WAL
        // before data. Log-only consumers may retain the paced sync policy.
        let urgent_commit = self.sync_interval <= std::time::Duration::from_millis(2)
            && decoded
                .iter()
                .any(|(frame, _)| payload_contains_commit(&frame.payload));
        let sync_target = {
            let mut log = self.log.lock();
            for (frame, offset) in &decoded {
                if frame.header.lsn < self.local_wal_next {
                    continue;
                }
                // One clone per frame (down from two): append consumes
                // an owned payload, the online-apply pass below reuses
                // the decoded original.
                log.append_replication_frame(
                    &chunk.bytes[*offset..*offset + frame.frame_len],
                    frame,
                )?;
                self.local_wal_next = frame.header.lsn + 1;
            }
            decoded
                .last()
                .map(|(frame, _)| frame.header.lsn)
                .unwrap_or(0)
        };
        self.progress.received_lsn = self.progress.received_lsn.max(chunk.end_lsn);
        // Fsync outside the WAL mutex. Online database apply shares one
        // barrier across the complete chunk; log-only consumers can pace it.
        if sync_target != 0
            && (self.db.is_some()
                || urgent_commit
                || self.last_sync_at.elapsed() >= self.sync_interval)
        {
            crate::wal::LogManager::sync_durable_to(&self.log, sync_target)?;
            self.last_sync_at = std::time::Instant::now();
        }

        // Pass 3: project each record into the live Database (buffer
        // pool, txn manager, MVCC SCN, catalog). Crash between pass 2
        // and here is safe: restart recovery replays the local WAL.
        let mut frames = 0u64;
        let mut last_lsn = self.applied_lsn;
        let mut committed = 0u64;
        // One bounded context per network chunk lets heap redo reuse file
        // descriptors across physical frames. It is intentionally not stored
        // on `Applier`: reconnects/chunk boundaries release every descriptor.
        let mut apply_context = OnlineWalApplyContext::default();
        for (frame, _) in decoded {
            if let Some(db) = &self.db {
                let record = WalRecord {
                    source_time_us: frame.header.source_time_us,
                    lsn: frame.header.lsn,
                    prev_txn_lsn: frame.header.prev_txn_lsn,
                    txn_id: frame.header.txn_id,
                    payload: frame.payload,
                };
                db.apply_wal_record_with_context(&record, &mut apply_context)
                    .map_err(|err| {
                        QueryError::internal_control(
                            crate::error::InternalErrorKind::ReplicaApplyFailure,
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            format!(
                                "replica apply failed at LSN {}: {}",
                                record.lsn,
                                err.message()
                            ),
                        )
                    })?;
                last_lsn = record.lsn;
                self.applied_lsn = self.applied_lsn.max(record.lsn);
                self.progress.applied_lsn = self.applied_lsn;
                frames += 1;
                self.track_txn_state_payload(record.txn_id, &record.payload);
                if payload_contains_commit(&record.payload) {
                    committed += 1;
                }
            } else {
                last_lsn = frame.header.lsn;
                self.applied_lsn = self.applied_lsn.max(frame.header.lsn);
                self.progress.applied_lsn = self.applied_lsn;
                frames += 1;
                self.track_txn_state_payload(frame.header.txn_id, &frame.payload);
                if payload_contains_commit(&frame.payload) {
                    committed += 1;
                }
            }
        }

        self.applied_lsn = self.applied_lsn.max(last_lsn);
        self.progress.applied_lsn = self.applied_lsn;
        if let Some(db) = &self.db {
            // Persist the largest contiguous physical undo prefixes once per
            // network chunk. This amortizes file open/flush work across many
            // source transactions while preserving the WAL-backed reorder
            // invariant.
            db.flush_replicated_undo_pending()?;
            // Online recovery does not travel through the normal foreground
            // commit path, so drive slot GC and replica-safe resident undo
            // eviction at every bounded network chunk. Without this, a fast
            // catch-up can materialize hundreds of MiB of committed undo
            // before the periodic worker gets a chance to reclaim it.
            db.gc_committed_txn_slots();
            const MAX_PURGE_BATCHES_PER_CHUNK: usize = 8;
            for _ in 0..MAX_PURGE_BATCHES_PER_CHUNK {
                if !db.purge_obsolete_undo() {
                    break;
                }
            }
        }
        self.pending_master = Some((master_uuid.to_string(), master_epoch));
        self.advance_durable_progress(false)?;

        Ok(ApplyOutcome {
            frames_applied: frames,
            last_lsn_applied: self.progress.durable_applied_lsn,
            committed_txns: committed,
        })
    }

    /// Move the durable progress watermark up to
    /// `min(applied, local WAL durable_lsn)` and persist it when it
    /// advanced. Ack/semi-sync semantics: the master may only be told
    /// about LSNs that are fsynced in our local WAL.
    fn advance_durable_progress(&mut self, force_persist: bool) -> Result<(), QueryError> {
        let durable = self
            .log
            .lock()
            .durable_lsn()
            .min(self.progress.received_lsn);
        self.progress.durable_lsn = self.progress.durable_lsn.max(durable);
        let watermark = self.applied_lsn.min(durable);
        if watermark > self.progress.durable_applied_lsn {
            self.progress.durable_applied_lsn = watermark;
            if let Some((uuid, epoch)) = self.pending_master.as_ref() {
                self.progress.last_master_uuid.clone_from(uuid);
                self.progress.last_master_epoch = *epoch;
            }
            self.progress.last_update_ms = now_ms();
        }
        if self.progress.durable_applied_lsn <= self.persisted_lsn {
            return Ok(());
        }
        if !force_persist && self.last_progress_persist_at.elapsed() < PROGRESS_PERSIST_INTERVAL {
            return Ok(());
        }
        if let Some(db) = &self.db {
            if self.applied_lsn > self.progress.durable_applied_lsn {
                crate::wal::LogManager::sync_durable_to(&self.log, self.applied_lsn)?;
                self.progress.durable_lsn = self.progress.durable_lsn.max(self.applied_lsn);
                self.progress.durable_applied_lsn = self.applied_lsn;
            }
            self.progress.statistics =
                Some(Arc::new(db.replica_statistics_snapshot(self.applied_lsn)?));
        }
        save_progress(&self.data_dir, &self.progress).map_err(|err| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("progress persist: {err}"),
            )
        })?;
        self.persisted_lsn = self.progress.durable_applied_lsn;
        self.last_progress_persist_at = std::time::Instant::now();
        Ok(())
    }

    /// Force-fsync any appended-but-unsynced WAL tail and advance the
    /// durable progress. Called by the receiver on its ack/heartbeat
    /// ticks so the watermark catches up once the stream goes idle.
    pub fn flush_pending(&mut self) -> Result<(), QueryError> {
        if self.applied_lsn > self.progress.durable_applied_lsn {
            let sync_lsn = self.applied_lsn.min(self.local_wal_next.saturating_sub(1));
            crate::wal::LogManager::sync_durable_to(&self.log, sync_lsn)?;
            self.last_sync_at = std::time::Instant::now();
        }
        // Also checkpoint an already-durable in-memory watermark once its
        // metadata interval expires, even if the stream went idle meanwhile.
        self.advance_durable_progress(false)
    }

    /// Force both the local WAL and its progress metadata to stable storage.
    /// Used at orderly shutdown so STOP/START does not replay the throttled
    /// metadata window.
    pub fn flush_and_persist(&mut self) -> Result<(), QueryError> {
        if self.applied_lsn > self.progress.durable_applied_lsn {
            let sync_lsn = self.applied_lsn.min(self.local_wal_next.saturating_sub(1));
            crate::wal::LogManager::sync_durable_to(&self.log, sync_lsn)?;
            self.last_sync_at = std::time::Instant::now();
        }
        self.advance_durable_progress(true)
    }

    fn track_txn_state_payload(&mut self, txn_id: u64, payload: &WalPayload) {
        match payload {
            WalPayload::TxnBatch { records } => {
                for record in records {
                    self.track_txn_state_payload(txn_id, record);
                }
            }
            WalPayload::CommitTxn { commit_scn, .. } => {
                // Bound the map: it's cleared by AbortTxn/EndTxn, but a
                // master stream that (by design or bug) never emits an
                // EndTxn for some committed txns would otherwise grow it
                // without limit on a long-running replica (C14). The map
                // is advisory (ack pacing / tests), so on overflow we
                // drop the bookkeeping rather than leak memory.
                if self.committed.len() >= MAX_TRACKED_COMMITTED_TXNS {
                    crate::util::logging::log_message(format_args!(
                        "mydb-repl replica: committed-txn tracking exceeded {} entries; \
                         clearing (advisory bookkeeping only)",
                        MAX_TRACKED_COMMITTED_TXNS
                    ));
                    self.committed.clear();
                }
                self.committed.insert(txn_id, *commit_scn);
            }
            WalPayload::AbortTxn | WalPayload::EndTxn => {
                self.committed.remove(&txn_id);
            }
            _ => {}
        }
    }
}

fn payload_contains_commit(payload: &WalPayload) -> bool {
    match payload {
        WalPayload::CommitTxn { .. } => true,
        WalPayload::TxnBatch { records } => records
            .iter()
            .any(|record| matches!(record, WalPayload::CommitTxn { .. })),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{LogManager, WalHeapInsert, WalPayload, WalReadChunk, WalReader};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn build_chunk(dir: &TempDir) -> (Arc<crate::wal::LogMutex>, WalReadChunk) {
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        {
            let mut g = log.lock();
            for _ in 0..3 {
                g.append(
                    1,
                    WalPayload::HeapInsertBatch {
                        path: "schemas/test/tables/t__1__data.heap".into(),
                        inserts: vec![WalHeapInsert {
                            page_id: 1,
                            slot_id: 0,
                            payload: vec![0x11; 32],
                            undo_lsn: None,
                        }],
                    },
                )
                .unwrap();
            }
            g.flush().unwrap();
        }
        let start = log.lock().min_available_lsn();
        let mut reader = WalReader::open(&log, start).unwrap();
        let safe = log.lock().next_lsn().saturating_sub(1);
        let chunk = reader
            .next_chunk(safe, 1 << 20)
            .unwrap()
            .expect("a chunk with all 3 frames");
        assert_eq!(chunk.frame_count, 3);
        (log, chunk)
    }

    #[test]
    fn reconnect_preserves_live_applied_prefix_without_skipping_failed_record() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let source_dir = tempfile::Builder::new()
            .prefix("repl-source-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let target_dir = tempfile::Builder::new()
            .prefix("repl-target-")
            .tempdir_in("/tmp/mydb")
            .unwrap();
        let db = Arc::new(Database::open(target_dir.path()).unwrap());
        db.set_physical_replica_mode(true).unwrap();
        let target_log = db.wal_log();
        let baseline_lsn = target_log.lock().last_lsn();
        let source_log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(source_dir.path()).unwrap(),
        ));
        let (begin_lsn, committed_lsn) = {
            let mut log = source_log.lock();
            let begin = log
                .append_reserved(baseline_lsn + 1, 100, WalPayload::BeginTxn { start_scn: 0 })
                .unwrap();
            let commit = log
                .append(
                    100,
                    WalPayload::CommitTxn {
                        commit_scn: 1,
                        commit_unix_us: 1,
                        binlog: None,
                    },
                )
                .unwrap();
            log.flush().unwrap();
            (begin, commit)
        };
        let mut applier = Applier::with_database(
            target_dir.path(),
            Arc::clone(&target_log),
            Arc::clone(&db),
            ReplicaProgress {
                received_lsn: baseline_lsn,
                durable_lsn: baseline_lsn,
                applied_lsn: baseline_lsn,
                durable_applied_lsn: baseline_lsn,
                ..ReplicaProgress::default()
            },
        );
        applier.sync_interval = std::time::Duration::from_secs(3600);
        target_log.lock().set_write_buffer_enabled(true).unwrap();
        let chunk = WalReader::open(&source_log, begin_lsn)
            .unwrap()
            .next_chunk(committed_lsn, 1 << 20)
            .unwrap()
            .unwrap();
        applier.apply_chunk(&chunk, "master", 1).unwrap();
        assert!(
            target_log.lock().durable_lsn() >= committed_lsn,
            "online apply must persist WAL even inside the paced-sync interval"
        );
        assert_eq!(db.replica_applied_lsn(), committed_lsn);
        assert!(!db.is_txn_active(100));

        std::fs::create_dir(target_dir.path().join("not-a-heap-file")).unwrap();
        let failed_lsn = {
            let mut log = source_log.lock();
            let lsn = log
                .append(
                    0,
                    WalPayload::SlotReclaim {
                        path: "not-a-heap-file".into(),
                        page_id: 0,
                        slot_id: 0,
                    },
                )
                .unwrap();
            log.flush().unwrap();
            lsn
        };
        let failed = WalReader::open(&source_log, failed_lsn)
            .unwrap()
            .next_chunk(failed_lsn, 1 << 20)
            .unwrap()
            .unwrap();
        let err = applier.apply_chunk(&failed, "master", 1).unwrap_err();
        assert!(err.is_internal(crate::error::InternalErrorKind::ReplicaApplyFailure));
        let mut reconciled = ReplicaProgress {
            last_master_uuid: "master".into(),
            ..ReplicaProgress::default()
        };
        crate::repl::replica::progress::reconcile_applied(&mut reconciled, &db, &target_log);
        assert_eq!(reconciled.durable_applied_lsn, committed_lsn);
        assert!(target_log.lock().durable_lsn() >= failed_lsn);
        assert_eq!(db.replica_applied_lsn(), committed_lsn);
        let mut retry = Applier::with_database(
            target_dir.path(),
            target_log,
            Arc::clone(&db),
            ReplicaProgress::default(),
        );
        assert_eq!(retry.progress().applied_lsn, committed_lsn);
        assert_eq!(
            retry.progress().durable_applied_lsn,
            0,
            "online watermark must not fabricate durable ACK progress"
        );
        let duplicate_begin = WalReader::open(&source_log, begin_lsn)
            .unwrap()
            .next_chunk(begin_lsn, 1 << 20)
            .unwrap()
            .unwrap();
        assert_eq!(
            retry
                .apply_chunk(&duplicate_begin, "master", 1)
                .unwrap()
                .frames_applied,
            0,
            "an already applied BEGIN must not replace committed visibility with Active"
        );
        assert!(!db.is_txn_active(100));
        assert!(
            retry.apply_chunk(&failed, "master", 1).is_err(),
            "the received but failed record must still be retried"
        );
        assert_eq!(db.replica_applied_lsn(), committed_lsn);
    }

    #[test]
    fn release_audit_overlap_io_is_retryable() {
        let err = error(crate::error::catalog::ER_GET_ERRNO, "injected ENOSPC");
        let mapped = overlap_read_error(crate::wal::WalReaderError::Io(err));
        assert!(!mapped.is_internal(crate::error::InternalErrorKind::ReplicaPoisonFrame));
        assert_eq!(mapped.code(), crate::error::catalog::ER_GET_ERRNO.code());
    }

    #[test]
    fn valid_chunk_applies_and_advances() {
        let dir = TempDir::new().unwrap();
        let (log, chunk) = build_chunk(&dir);
        // A second applier over the SAME log: frames are already durable
        // so pass-2 append skips them; we only exercise decode + metadata
        // check + watermark advance.
        let mut applier = Applier::new(dir.path(), Arc::clone(&log), ReplicaProgress::default());
        let out = applier.apply_chunk(&chunk, "m", 1).unwrap();
        // All 3 frames decoded + passed the metadata cross-check.
        assert_eq!(out.frames_applied, 3);
        // After forcing the group-fsync, the durable watermark reaches
        // the chunk end.
        applier.flush_pending().unwrap();
        assert_eq!(applier.last_applied_lsn(), chunk.end_lsn);
    }

    #[test]
    fn progress_metadata_is_throttled_but_forced_on_shutdown() {
        let dir = TempDir::new().unwrap();
        let (log, chunk) = build_chunk(&dir);
        let mut applier = Applier::new(dir.path(), log, ReplicaProgress::default());

        applier.apply_chunk(&chunk, "master-a", 1).unwrap();
        applier.flush_pending().unwrap();
        assert_eq!(applier.last_applied_lsn(), chunk.end_lsn);
        assert!(
            load_progress(dir.path()).unwrap().is_none(),
            "the hot apply path must not fsync progress metadata per chunk"
        );

        applier.flush_and_persist().unwrap();
        let persisted = load_progress(dir.path()).unwrap().unwrap();
        assert_eq!(persisted.durable_applied_lsn, chunk.end_lsn);
        assert_eq!(persisted.last_master_uuid, "master-a");
    }

    #[test]
    fn chunk_frame_count_mismatch_is_rejected() {
        let dir = TempDir::new().unwrap();
        let (log, mut chunk) = build_chunk(&dir);
        chunk.frame_count = 2; // lies: bytes actually hold 3 frames
        let mut applier = Applier::new(dir.path(), log, ReplicaProgress::default());
        let err = applier.apply_chunk(&chunk, "m", 1).unwrap_err();
        assert_eq!(err.diagnostic_id(), Some("MYDB-DIAG-REPL-POISON"));
        assert!(
            err.diagnostic_detail()
                .is_some_and(|detail| detail.contains("frame_count mismatch")),
            "got: {err:?}"
        );
    }

    #[test]
    fn chunk_lsn_range_mismatch_is_rejected() {
        let dir = TempDir::new().unwrap();
        let (log, mut chunk) = build_chunk(&dir);
        chunk.end_lsn += 100; // lies about the last LSN
        let mut applier = Applier::new(dir.path(), log, ReplicaProgress::default());
        let err = applier.apply_chunk(&chunk, "m", 1).unwrap_err();
        assert_eq!(err.diagnostic_id(), Some("MYDB-DIAG-REPL-POISON"));
        assert!(
            err.diagnostic_detail()
                .is_some_and(|detail| detail.contains("LSN range mismatch")),
            "got: {err:?}"
        );
    }
}

#[cfg(test)]
mod crash_consistency {
    use super::*;
    use crate::wal::{LogManager, WalReader};
    #[test]
    fn reconnect_rejects_same_lsn_different_payload() {
        let old_dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let new_dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let old = Arc::new(parking_lot::Mutex::new(
            LogManager::open(old_dir.path()).unwrap(),
        ));
        let new = Arc::new(parking_lot::Mutex::new(
            LogManager::open(new_dir.path()).unwrap(),
        ));
        old.lock()
            .append(1, WalPayload::BeginTxn { start_scn: 10 })
            .unwrap();
        old.lock().flush().unwrap();
        new.lock()
            .append(1, WalPayload::BeginTxn { start_scn: 20 })
            .unwrap();
        new.lock().flush().unwrap();
        let chunk = WalReader::open(&new, 1)
            .unwrap()
            .next_chunk(1, 4096)
            .unwrap()
            .unwrap();
        let mut applier = Applier::new(old_dir.path(), old.clone(), ReplicaProgress::default());
        let result = applier.apply_chunk(&chunk, "source", 1);
        applier.flush_and_persist().unwrap();
        let local = WalReader::open(&old, 1)
            .unwrap()
            .next_chunk(1, 4096)
            .unwrap()
            .unwrap();
        assert_ne!(local.bytes, chunk.bytes);
        assert!(
            result.is_err(),
            "divergent existing WAL was accepted and acked at {}",
            applier.last_applied_lsn()
        );
    }
}

#[cfg(test)]
mod frame_overlap_tests {
    use super::*;
    use crate::wal::{LogManager, WalReader};
    #[test]
    fn partially_overlapping_batch_is_rejected_before_append() {
        let source_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let target_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let source = Arc::new(parking_lot::Mutex::new(
            LogManager::open(source_dir.path()).unwrap(),
        ));
        let target = Arc::new(parking_lot::Mutex::new(
            LogManager::open(target_dir.path()).unwrap(),
        ));
        target
            .lock()
            .append(1, WalPayload::BeginTxn { start_scn: 1 })
            .unwrap();
        target.lock().flush().unwrap();
        source
            .lock()
            .append_txn_batch(
                1,
                vec![
                    WalPayload::BeginTxn { start_scn: 1 },
                    WalPayload::CommitTxn {
                        commit_scn: 2,
                        commit_unix_us: 1,
                        binlog: None,
                    },
                ],
            )
            .unwrap();
        source.lock().flush().unwrap();
        let chunk = WalReader::open(&source, 1)
            .unwrap()
            .next_chunk(2, 4096)
            .unwrap()
            .unwrap();
        let mut applier = Applier::new(
            target_dir.path(),
            Arc::clone(&target),
            ReplicaProgress::default(),
        );
        assert!(applier.apply_chunk(&chunk, "source", 1).is_err());
        assert_eq!(target.lock().last_lsn(), 1);
        assert_eq!(applier.last_applied_lsn(), 0);
    }
}
