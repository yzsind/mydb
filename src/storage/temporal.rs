use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

use serde::{Deserialize, Serialize};

use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::types::QueryError;

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_LEDGER_PERSIST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(crate) fn fail_next_ledger_persist_for_test() {
    FAIL_NEXT_LEDGER_PERSIST.with(|fail| fail.set(true));
}
use crate::wal::{WalPayload, WalRecordType};

use super::txn::IndexRepairKey;
use super::{error, DurabilityMode, IndexBound, IndexKey, RowId, TxnView};

const LEDGER_MAGIC: &[u8; 8] = b"MYDBCTL1";
const LEDGER_VERSION: u32 = 1;
const HEADER_LEN: usize = 16;
const RECORD_LEN: usize = 32;
/// `commit_unix_us` never reaches the high bit for representable SQL
/// timestamps.  Use it to distinguish the single availability-baseline
/// record from ordinary commit records without growing the fixed-width
/// ledger format.
const BASELINE_TIME_MARKER: u64 = 1 << 63;
const HISTORY_MAGIC: &[u8; 8] = b"MYDBHIX1";
const HISTORY_VERSION: u32 = 1;
const HISTORY_FRAME_HEADER_LEN: usize = 16;

fn index_bounds_are_empty(low: Option<&IndexBound>, high: Option<&IndexBound>) -> bool {
    low.zip(high).is_some_and(|(low, high)| {
        low.key > high.key || (low.key == high.key && (!low.inclusive || !high.inclusive))
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct HistoryIndexWireEntry {
    table_id: u64,
    index_name: String,
    key: IndexKey,
    row_id: RowId,
}

#[derive(Debug, Clone, Copy)]
struct HistoryIndexVersion {
    row_id: RowId,
    commit_scn: u64,
}

#[derive(Debug, Default)]
struct HistoryIndexState {
    batches: HashMap<u64, Vec<HistoryIndexWireEntry>>,
    by_key: BTreeMap<IndexRepairKey, Vec<HistoryIndexVersion>>,
    persisted_txns: HashSet<u64>,
    indexed_txns: HashSet<u64>,
}

#[derive(Debug, Default)]
struct TemporalPersistRegistry {
    pending_lsns: Mutex<std::collections::BTreeSet<u64>>,
    changed: Condvar,
}

#[derive(Debug)]
pub(crate) struct TemporalPersistToken {
    registry: Arc<TemporalPersistRegistry>,
    lsn: Option<u64>,
}

impl TemporalPersistToken {
    pub(crate) fn complete(mut self) {
        self.release();
    }

    fn release(&mut self) {
        let Some(lsn) = self.lsn.take() else {
            return;
        };
        let mut pending = self
            .registry
            .pending_lsns
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        pending.remove(&lsn);
        drop(pending);
        self.registry.changed.notify_all();
    }
}

impl Drop for TemporalPersistToken {
    fn drop(&mut self) {
        self.release();
    }
}

impl HistoryIndexState {
    fn rebuild(&mut self, timeline: &TimelineState) {
        self.by_key.clear();
        self.indexed_txns.clear();
        for (&txn_id, batch) in &self.batches {
            let Some(commit) = timeline.by_txn.get(&txn_id) else {
                continue;
            };
            for entry in batch {
                self.by_key
                    .entry(IndexRepairKey {
                        table_id: entry.table_id,
                        index_name: entry.index_name.clone(),
                        key: entry.key.clone(),
                    })
                    .or_default()
                    .push(HistoryIndexVersion {
                        row_id: entry.row_id,
                        commit_scn: commit.commit_scn,
                    });
            }
            self.indexed_txns.insert(txn_id);
        }
    }

    fn publish_txn(&mut self, txn_id: u64, commit_scn: u64) {
        if !self.indexed_txns.insert(txn_id) {
            return;
        }
        let Some(batch) = self.batches.get(&txn_id) else {
            self.indexed_txns.remove(&txn_id);
            return;
        };
        for entry in batch {
            self.by_key
                .entry(IndexRepairKey {
                    table_id: entry.table_id,
                    index_name: entry.index_name.clone(),
                    key: entry.key.clone(),
                })
                .or_default()
                .push(HistoryIndexVersion {
                    row_id: entry.row_id,
                    commit_scn,
                });
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommitLedgerEntry {
    pub txn_id: u64,
    pub commit_scn: u64,
    pub commit_unix_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TemporalBaseline {
    /// Transactions below this id may predate temporal tracking.  Once their
    /// slots have been reclaimed, a snapshot at/after `snapshot_scn` can still
    /// safely treat their surviving heap versions as committed.
    pub first_tracked_txn_id: u64,
    pub snapshot_scn: u64,
    pub available_unix_us: u64,
}

#[derive(Debug, Default)]
struct TimelineState {
    by_scn: Vec<CommitLedgerEntry>,
    by_txn: HashMap<u64, CommitLedgerEntry>,
    persisted_txns: HashSet<u64>,
    /// Conservative governor reservations made by commits in this process.
    /// Startup entries are covered by the initial capacity charge instead.
    reservation_bytes: HashMap<u64, usize>,
    baseline: Option<TemporalBaseline>,
}

/// Durable commit ledger and SCN timeline. The fixed-width file is a startup
/// correctness source after transaction slots and old WAL segments have been
/// reclaimed; WAL tail records repair a crash between CommitTxn durability and
/// the ledger append.
#[derive(Debug)]
pub(crate) struct TemporalHistory {
    path: PathBuf,
    history_path: PathBuf,
    durability: DurabilityMode,
    file: Mutex<Option<File>>,
    history_file: Mutex<Option<File>>,
    file_needs_sync: AtomicBool,
    history_needs_sync: AtomicBool,
    #[cfg(all(test, target_os = "macos"))]
    sync_fixture_epoch: AtomicU64,
    state: RwLock<TimelineState>,
    history: RwLock<HistoryIndexState>,
    memory_charge: Mutex<ControlledMemoryCharge>,
    persist_registry: Arc<TemporalPersistRegistry>,
    /// Until the durable old-key sidecar has been reconstructed, historical
    /// index access must conservatively fall back to the heap. A fresh
    /// process can safely use the transaction-manager repair map; a reopen
    /// with existing commits cannot prove that map is complete.
    history_index_degraded: AtomicBool,
    history_compactions: AtomicU64,
    history_last_compaction_unix_us: AtomicU64,
}

impl TemporalHistory {
    pub(crate) fn open(
        data_dir: &Path,
        durability: DurabilityMode,
        wal: &(impl crate::wal::WalRecordSource + ?Sized),
    ) -> Result<Self, QueryError> {
        let read_only = crate::storage::image_io::immutable_path(data_dir);
        let path = data_dir.join("system/commit-ledger.v1");
        let (mut state, mut ledger_changed) = if durability == DurabilityMode::Memory {
            (TimelineState::default(), false)
        } else {
            load_ledger(&path)?
        };
        let mut temporal_wal_txns = HashSet::new();
        for record in wal
            .records()
            .only(&[WalRecordType::HistoryIndexBatch, WalRecordType::TxnBatch])
        {
            let record = record?;
            collect_wal_history_txns(record.txn_id, &record.payload, &mut temporal_wal_txns);
        }
        for record in wal
            .records()
            .only(&[WalRecordType::CommitTxn, WalRecordType::TxnBatch])
        {
            let record = record?;
            if temporal_wal_txns.contains(&record.txn_id) {
                ledger_changed |= collect_wal_commit(record.txn_id, &record.payload, &mut state);
            }
        }
        state.by_scn.sort_unstable_by_key(|entry| entry.commit_scn);
        let loaded_entries = state.by_scn.len();
        state.by_scn.dedup_by_key(|entry| entry.commit_scn);
        ledger_changed |= state.by_scn.len() != loaded_entries;

        let history_path = data_dir.join("system/history-index.v1");
        let (mut history, mut history_changed) = if durability == DurabilityMode::Memory {
            (HistoryIndexState::default(), false)
        } else {
            load_history_index(&history_path)
                .unwrap_or_else(|_| (HistoryIndexState::default(), true))
        };
        for record in wal
            .records()
            .only(&[WalRecordType::HistoryIndexBatch, WalRecordType::TxnBatch])
        {
            let record = record?;
            history_changed |=
                collect_wal_history_batch(record.txn_id, &record.payload, &state, &mut history);
        }
        history.rebuild(&state);
        let history_index_degraded = durability != DurabilityMode::Memory
            && state.by_txn.iter().any(|(txn_id, commit)| {
                let covered_by_baseline = state.baseline.is_some_and(|baseline| {
                    *txn_id < baseline.first_tracked_txn_id
                        && commit.commit_scn <= baseline.snapshot_scn
                });
                !covered_by_baseline && !history.batches.contains_key(txn_id)
            });
        let mut reused_paths = Vec::new();
        let file = if read_only || durability == DurabilityMode::Memory {
            None
        } else {
            if ledger_changed {
                rewrite_ledger(&path, state.baseline, &state.by_scn, durability)?;
            } else {
                reused_paths.push(path.as_path());
            }
            state.persisted_txns = state.by_txn.keys().copied().collect();
            Some(open_append(&path)?)
        };
        let history_file =
            if read_only || durability == DurabilityMode::Memory || history_index_degraded {
                None
            } else {
                if history_changed {
                    rewrite_history_index(&history_path, &history.batches, durability)?;
                } else {
                    reused_paths.push(history_path.as_path());
                }
                history.persisted_txns = history.batches.keys().copied().collect();
                Some(open_append(&history_path)?)
            };
        // Valid, unchanged sidecars need no replacement. Still drain their
        // existing bytes before startup can eventually release WAL coverage;
        // a prior process may have appended them without completing a sync.
        if durability == DurabilityMode::Strict && !reused_paths.is_empty() {
            crate::platform::fs::sync_paths(
                reused_paths
                    .into_iter()
                    .chain(std::iter::once(path.parent().unwrap())),
            )
            .map_err(|err| ledger_io_error(&path, err))?;
        }
        let resident_bytes = temporal_capacity_bytes(&state, &history);
        let memory_charge = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::TemporalHistory,
            resident_bytes,
        )?;
        let file_needs_sync = file.is_some();
        let history_needs_sync = history_file.is_some();
        Ok(Self {
            path,
            history_path,
            durability,
            file: Mutex::new(file),
            history_file: Mutex::new(history_file),
            file_needs_sync: AtomicBool::new(file_needs_sync),
            history_needs_sync: AtomicBool::new(history_needs_sync),
            #[cfg(all(test, target_os = "macos"))]
            sync_fixture_epoch: AtomicU64::new(u64::MAX),
            state: RwLock::new(state),
            history: RwLock::new(history),
            memory_charge: Mutex::new(memory_charge),
            persist_registry: Arc::new(TemporalPersistRegistry::default()),
            history_index_degraded: AtomicBool::new(history_index_degraded),
            history_compactions: AtomicU64::new(0),
            history_last_compaction_unix_us: AtomicU64::new(0),
        })
    }

    /// Serialize only WAL-append registration with checkpoint's cutoff
    /// capture. The returned token remains live through sidecar persistence;
    /// commits still fsync/group-commit concurrently.
    pub(crate) fn track_commit_wal<T>(
        &self,
        append: impl FnOnce() -> Result<(u64, T), QueryError>,
    ) -> Result<(u64, T, TemporalPersistToken), QueryError> {
        let mut pending = self
            .persist_registry
            .pending_lsns
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let (lsn, value) = append()?;
        pending.insert(lsn);
        drop(pending);
        Ok((
            lsn,
            value,
            TemporalPersistToken {
                registry: Arc::clone(&self.persist_registry),
                lsn: Some(lsn),
            },
        ))
    }

    /// Append CheckpointBegin under the same short registry latch, wait only
    /// for commits whose WAL precedes that cutoff, then fsync both temporal
    /// correctness files. Newer commits do not delay the checkpoint and their
    /// WAL remains above the retention floor.
    pub(crate) fn checkpoint_barrier(
        &self,
        append_begin: impl FnOnce() -> Result<u64, QueryError>,
    ) -> Result<u64, QueryError> {
        let mut pending = self
            .persist_registry
            .pending_lsns
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let begin_lsn = append_begin()?;
        while pending.iter().next().is_some_and(|lsn| *lsn < begin_lsn) {
            pending = self
                .persist_registry
                .changed
                .wait(pending)
                .unwrap_or_else(|poison| poison.into_inner());
        }
        drop(pending);
        self.sync_files()?;
        Ok(begin_lsn)
    }

    fn sync_files(&self) -> Result<(), QueryError> {
        if self.durability == DurabilityMode::Memory {
            return Ok(());
        }
        #[cfg(target_os = "macos")]
        {
            // Keep both identities stable through the shared device barrier.
            // Compaction never holds its history-file guard while acquiring
            // the ledger-file guard, and appenders use only one file guard.
            let history = self
                .history_file
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let ledger = self
                .file
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            #[cfg(test)]
            let epoch = crate::platform::fs::fixture_sync_epoch();
            #[cfg(test)]
            let force =
                epoch.is_none_or(|value| self.sync_fixture_epoch.load(Ordering::Acquire) != value);
            #[cfg(not(test))]
            let force = false;
            let sync_ledger =
                ledger.is_some() && (force || self.file_needs_sync.load(Ordering::Acquire));
            let sync_history =
                history.is_some() && (force || self.history_needs_sync.load(Ordering::Acquire));
            let mut batch = crate::platform::fs::DeviceSyncBatch::new();
            if let Some(file) = ledger.as_ref().filter(|_| sync_ledger) {
                let file = file
                    .try_clone()
                    .map_err(|err| ledger_io_error(&self.path, err))?;
                batch
                    .sync_file(file, &self.path)
                    .map_err(|err| ledger_io_error(&self.path, err))?;
            }
            if let Some(file) = history.as_ref().filter(|_| sync_history) {
                let file = file
                    .try_clone()
                    .map_err(|err| history_io_error(&self.history_path, err))?;
                batch
                    .sync_file(file, &self.history_path)
                    .map_err(|err| history_io_error(&self.history_path, err))?;
            }
            batch
                .finish()
                .map_err(|err| ledger_io_error(&self.path, err))?;
            // Both file guards exclude append/replacement through the full
            // barrier and dirty-bit clear. Errors retain every pending bit.
            if sync_ledger {
                self.file_needs_sync.store(false, Ordering::Release);
            }
            if sync_history {
                self.history_needs_sync.store(false, Ordering::Release);
            }
            #[cfg(test)]
            self.sync_fixture_epoch
                .store(epoch.unwrap_or(u64::MAX), Ordering::Release);
        }
        #[cfg(not(target_os = "macos"))]
        {
            if let Some(file) = self
                .file
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .as_mut()
                .filter(|_| self.file_needs_sync.load(Ordering::Acquire))
            {
                crate::platform::fs::sync_standard_data_at(file, &self.path)
                    .map_err(|err| ledger_io_error(&self.path, err))?;
                self.file_needs_sync.store(false, Ordering::Release);
            }
            if let Some(file) = self
                .history_file
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .as_mut()
                .filter(|_| self.history_needs_sync.load(Ordering::Acquire))
            {
                crate::platform::fs::sync_standard_data_at(file, &self.history_path)
                    .map_err(|err| history_io_error(&self.history_path, err))?;
                self.history_needs_sync.store(false, Ordering::Release);
            }
        }
        Ok(())
    }

    pub(crate) fn checkpoint_sync(&self) -> Result<(), QueryError> {
        self.sync_files()
    }

    /// Reserve before a commit can allocate ledger/timeline nodes or decode
    /// its old-key batch. The per-transaction estimate is deliberately
    /// conservative and represents container capacity, not current length.
    pub(crate) fn reserve_commit_capacity(
        &self,
        txn_id: u64,
        history_payload_bytes: usize,
    ) -> Result<(), QueryError> {
        let bytes = 160usize.saturating_add(history_payload_bytes.saturating_mul(4));
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        if state.reservation_bytes.contains_key(&txn_id) {
            return Ok(());
        }
        self.memory_charge
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .try_grow(bytes)?;
        state.reservation_bytes.insert(txn_id, bytes);
        Ok(())
    }

    pub(crate) fn encode_history_index_batch(
        entries: Vec<(IndexRepairKey, RowId)>,
    ) -> Result<Vec<u8>, QueryError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let entries = entries
            .into_iter()
            .map(|(key, row_id)| HistoryIndexWireEntry {
                table_id: key.table_id,
                index_name: key.index_name,
                key: key.key,
                row_id,
            })
            .collect::<Vec<_>>();
        serde_json::to_vec(&entries).map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("history index batch encode failed: {err}"),
            )
        })
    }

    pub(crate) fn note_history_index_batch(
        &self,
        txn_id: u64,
        bytes: &[u8],
    ) -> Result<(), QueryError> {
        let entries = decode_history_batch(bytes)?;
        let commit_scn = self
            .state
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .by_txn
            .get(&txn_id)
            .map(|commit| commit.commit_scn);
        let mut history = self
            .history
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        if history.batches.contains_key(&txn_id) {
            return Ok(());
        }
        history.batches.insert(txn_id, entries);
        if let Some(commit_scn) = commit_scn {
            history.publish_txn(txn_id, commit_scn);
        }
        Ok(())
    }

    pub(crate) fn persist_history_index_batch(
        &self,
        txn_id: u64,
        bytes: &[u8],
    ) -> Result<(), QueryError> {
        self.note_history_index_batch(txn_id, bytes)?;
        if self.durability == DurabilityMode::Memory {
            return Ok(());
        }
        let already_persisted = self
            .history
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .persisted_txns
            .contains(&txn_id);
        if already_persisted {
            return Ok(());
        }
        let frame = encode_history_frame(txn_id, bytes)?;
        let mut history_file = self
            .history_file
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        {
            let Some(file) = history_file.as_mut() else {
                self.history_index_degraded.store(true, Ordering::Release);
                return Ok(());
            };
            self.history_needs_sync.store(true, Ordering::Release);
            if let Err(err) = file.write_all(&frame) {
                self.history_index_degraded.store(true, Ordering::Release);
                return Err(history_io_error(&self.history_path, err));
            }
        }
        drop(history_file);
        self.history
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
            .persisted_txns
            .insert(txn_id);
        Ok(())
    }

    pub(crate) fn compact_history_index(
        &self,
        oldest_snapshot_scn: Option<u64>,
        ledger_baseline: Option<TemporalBaseline>,
        clear_ledger: bool,
    ) -> Result<(), QueryError> {
        let history_changed = {
            let mut file = self
                .history_file
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let timeline = self
                .state
                .read()
                .unwrap_or_else(|poison| poison.into_inner());
            let mut history = self
                .history
                .write()
                .unwrap_or_else(|poison| poison.into_inner());
            let before = history.batches.len();
            history.batches.retain(|txn_id, _| {
                if clear_ledger {
                    return false;
                }
                !oldest_snapshot_scn.is_some_and(|oldest| {
                    timeline
                        .by_txn
                        .get(txn_id)
                        .is_some_and(|commit| commit.commit_scn <= oldest)
                })
            });
            let changed = history.batches.len() != before;
            if changed {
                history.batches.shrink_to_fit();
                history.rebuild(&timeline);
                history.persisted_txns = history.batches.keys().copied().collect();
                history.persisted_txns.shrink_to_fit();
                history.indexed_txns.shrink_to_fit();
                if self.durability != DurabilityMode::Memory && !self.history_index_degraded() {
                    self.history_needs_sync.store(true, Ordering::Release);
                    *file = None;
                    if let Err(err) =
                        rewrite_history_index(&self.history_path, &history.batches, self.durability)
                    {
                        self.history_index_degraded.store(true, Ordering::Release);
                        return Err(err);
                    }
                    *file = Some(open_append(&self.history_path)?);
                }
            }
            changed
        };

        let mut released_reservations = 0usize;
        let ledger_changed = if clear_ledger || ledger_baseline.is_some() {
            let mut file = self
                .file
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let mut timeline = self
                .state
                .write()
                .unwrap_or_else(|poison| poison.into_inner());
            let old_len = timeline.by_scn.len();
            let old_baseline = timeline.baseline;
            if clear_ledger {
                timeline.by_scn.clear();
                timeline.by_txn.clear();
                timeline.persisted_txns.clear();
                released_reservations = timeline.reservation_bytes.drain().map(|(_, b)| b).sum();
                timeline.baseline = None;
            } else if let Some(mut baseline) = ledger_baseline {
                // A transaction allocated before the cutoff may commit after
                // it. Keep the baseline below every such retained transaction
                // id so missing ledger records can never classify it as old.
                if let Some(first_newer_txn) = timeline
                    .by_scn
                    .iter()
                    .filter(|entry| entry.commit_scn > baseline.snapshot_scn)
                    .map(|entry| entry.txn_id)
                    .min()
                {
                    baseline.first_tracked_txn_id =
                        baseline.first_tracked_txn_id.min(first_newer_txn);
                }
                let advances = old_baseline.is_none_or(|old| {
                    baseline.snapshot_scn >= old.snapshot_scn
                        && baseline.available_unix_us >= old.available_unix_us
                        && baseline.first_tracked_txn_id >= old.first_tracked_txn_id
                });
                if advances {
                    let mut removed_txns = Vec::new();
                    timeline.by_scn.retain(|entry| {
                        let remove = entry.commit_scn <= baseline.snapshot_scn
                            && entry.txn_id < baseline.first_tracked_txn_id;
                        if remove {
                            removed_txns.push(entry.txn_id);
                        }
                        !remove
                    });
                    for txn_id in removed_txns {
                        timeline.by_txn.remove(&txn_id);
                        timeline.persisted_txns.remove(&txn_id);
                        released_reservations = released_reservations.saturating_add(
                            timeline.reservation_bytes.remove(&txn_id).unwrap_or(0),
                        );
                    }
                    timeline.baseline = Some(baseline);
                }
            }
            let changed = old_len != timeline.by_scn.len() || old_baseline != timeline.baseline;
            if changed {
                timeline.by_scn.shrink_to_fit();
                timeline.by_txn.shrink_to_fit();
                timeline.persisted_txns.shrink_to_fit();
                timeline.reservation_bytes.shrink_to_fit();
                if self.durability != DurabilityMode::Memory {
                    self.file_needs_sync.store(true, Ordering::Release);
                    *file = None;
                    if let Err(err) = rewrite_ledger(
                        &self.path,
                        timeline.baseline,
                        &timeline.by_scn,
                        self.durability,
                    ) {
                        *file = open_append(&self.path).ok();
                        return Err(err);
                    }
                    *file = Some(open_append(&self.path)?);
                }
                timeline.persisted_txns = timeline.by_txn.keys().copied().collect();
            }
            changed
        } else {
            false
        };

        if released_reservations != 0 {
            self.memory_charge
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .shrink(released_reservations);
        }
        if !history_changed && !ledger_changed {
            return Ok(());
        }
        self.history_compactions.fetch_add(1, Ordering::Relaxed);
        self.history_last_compaction_unix_us.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros()
                .min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        Ok(())
    }

    pub(crate) fn persist_noted_history_index_batch(&self, txn_id: u64) -> Result<(), QueryError> {
        let payload = {
            let history = self
                .history
                .read()
                .unwrap_or_else(|poison| poison.into_inner());
            let Some(batch) = history.batches.get(&txn_id) else {
                return Ok(());
            };
            serde_json::to_vec(batch).map_err(|err| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("history index batch encode failed: {err}"),
                )
            })?
        };
        self.persist_history_index_batch(txn_id, &payload)
    }

    pub(crate) fn discard_uncommitted_history_index_batch(&self, txn_id: u64) {
        let mut timeline = self
            .state
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        let committed = timeline.by_txn.contains_key(&txn_id);
        if committed {
            return;
        }
        let reserved = timeline.reservation_bytes.remove(&txn_id).unwrap_or(0);
        let mut history = self
            .history
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        if history.batches.remove(&txn_id).is_some() {
            history.rebuild(&timeline);
        }
        drop(history);
        drop(timeline);
        if reserved != 0 {
            self.memory_charge
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .shrink(reserved);
        }
    }

    pub(crate) fn has_history_index_batch(&self, txn_id: u64) -> bool {
        self.history
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .batches
            .contains_key(&txn_id)
    }

    pub(crate) fn history_index_entries(
        &self,
        txn: TxnView,
        table_id: u64,
        index_name: &str,
        mut key_matches: impl FnMut(&IndexKey) -> bool,
    ) -> Vec<(IndexKey, RowId)> {
        if self.history_index_degraded() {
            return Vec::new();
        }
        let index_name = index_name.to_ascii_lowercase();
        let start = IndexRepairKey {
            table_id,
            index_name: index_name.clone(),
            key: IndexKey::from(Vec::new()),
        };
        let history = self
            .history
            .read()
            .unwrap_or_else(|poison| poison.into_inner());
        history
            .by_key
            .range(start..)
            .take_while(|(key, _)| key.table_id == table_id && key.index_name == index_name)
            .filter(|(key, _)| key_matches(&key.key))
            .flat_map(|(key, versions)| {
                versions.iter().filter_map(|version| {
                    (version.commit_scn > txn.snapshot_scn)
                        .then(|| (key.key.clone(), version.row_id))
                })
            })
            .collect()
    }

    /// Exact-key counterpart of `history_index_entries`. Keep this separate
    /// from the predicate-based range helper so equality probes retain the
    /// logarithmic lookup promised by the history sidecar.
    pub(crate) fn history_index_entries_exact(
        &self,
        txn: TxnView,
        table_id: u64,
        index_name: &str,
        key: &IndexKey,
    ) -> Vec<(IndexKey, RowId)> {
        if self.history_index_degraded() {
            return Vec::new();
        }
        let repair_key = IndexRepairKey {
            table_id,
            index_name: index_name.to_ascii_lowercase(),
            key: key.clone(),
        };
        let history = self
            .history
            .read()
            .unwrap_or_else(|poison| poison.into_inner());
        history
            .by_key
            .get(&repair_key)
            .into_iter()
            .flatten()
            .filter_map(|version| {
                (version.commit_scn > txn.snapshot_scn).then(|| (key.clone(), version.row_id))
            })
            .collect()
    }

    pub(crate) fn history_index_entries_bounds(
        &self,
        txn: TxnView,
        table_id: u64,
        index_name: &str,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
    ) -> Vec<(IndexKey, RowId)> {
        self.collect_history_index_bounds(txn, table_id, index_name, low, high, |key, row_id| {
            (key.clone(), row_id)
        })
    }

    pub(crate) fn history_index_row_ids_bounds(
        &self,
        txn: TxnView,
        table_id: u64,
        index_name: &str,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
    ) -> Vec<RowId> {
        self.collect_history_index_bounds(txn, table_id, index_name, low, high, |_, row_id| row_id)
    }

    /// Project while holding the history read lock; do not materialize keys
    /// for callers that need only RowIds. The mapper is statically dispatched.
    fn collect_history_index_bounds<T>(
        &self,
        txn: TxnView,
        table_id: u64,
        index_name: &str,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
        project: impl Fn(&IndexKey, RowId) -> T,
    ) -> Vec<T> {
        if self.history_index_degraded() || index_bounds_are_empty(low, high) {
            return Vec::new();
        }
        let index_name = index_name.to_ascii_lowercase();
        let composite = |key: IndexKey| IndexRepairKey {
            table_id,
            index_name: index_name.clone(),
            key,
        };
        let low_bound = match low {
            Some(bound) if bound.inclusive => {
                std::ops::Bound::Included(composite(bound.key.clone()))
            }
            Some(bound) => std::ops::Bound::Excluded(composite(bound.key.clone())),
            None => std::ops::Bound::Included(composite(IndexKey::from(Vec::new()))),
        };
        let high_bound = match high {
            Some(bound) if bound.inclusive => {
                std::ops::Bound::Included(composite(bound.key.clone()))
            }
            Some(bound) => std::ops::Bound::Excluded(composite(bound.key.clone())),
            None => std::ops::Bound::Unbounded,
        };
        let history = self
            .history
            .read()
            .unwrap_or_else(|poison| poison.into_inner());
        history
            .by_key
            .range((low_bound, high_bound))
            .take_while(|(key, _)| key.table_id == table_id && key.index_name == index_name)
            .flat_map(|(key, versions)| {
                versions.iter().filter_map(|version| {
                    (version.commit_scn > txn.snapshot_scn)
                        .then(|| project(&key.key, version.row_id))
                })
            })
            .collect()
    }

    pub(crate) fn note_commit(&self, entry: CommitLedgerEntry) {
        {
            let mut state = self
                .state
                .write()
                .unwrap_or_else(|poison| poison.into_inner());
            if !state.by_txn.contains_key(&entry.txn_id) {
                state.by_txn.insert(entry.txn_id, entry);
                match state
                    .by_scn
                    .binary_search_by_key(&entry.commit_scn, |candidate| candidate.commit_scn)
                {
                    Ok(_) => {}
                    Err(index) => state.by_scn.insert(index, entry),
                }
            }
        }
        self.history
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
            .publish_txn(entry.txn_id, entry.commit_scn);
    }

    pub(crate) fn persist_commit(&self, entry: CommitLedgerEntry) -> Result<(), QueryError> {
        self.note_commit(entry);
        if self.durability == DurabilityMode::Memory {
            return Ok(());
        }
        #[cfg(test)]
        if FAIL_NEXT_LEDGER_PERSIST.with(|fail| fail.replace(false)) {
            return Err(ledger_error(
                &self.path,
                "injected post-durable ledger persistence failure",
            ));
        }
        {
            let state = self
                .state
                .read()
                .unwrap_or_else(|poison| poison.into_inner());
            if state.persisted_txns.contains(&entry.txn_id) {
                return Ok(());
            }
        }
        let bytes = encode_record(entry);
        let mut file = self
            .file
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let file = file
            .as_mut()
            .ok_or_else(|| ledger_error(&self.path, "ledger file is closed"))?;
        self.file_needs_sync.store(true, Ordering::Release);
        file.write_all(&bytes)
            .map_err(|err| ledger_io_error(&self.path, err))?;
        self.state
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
            .persisted_txns
            .insert(entry.txn_id);
        Ok(())
    }

    pub(crate) fn baseline(&self) -> Option<TemporalBaseline> {
        self.state
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .baseline
    }

    /// Replace the historical availability baseline atomically.  The caller
    /// holds the transaction-manager write latch and has verified that no
    /// writer is active, so every earlier committed heap version is covered by
    /// `snapshot_scn` and every later writer will be tracked normally.
    pub(crate) fn establish_baseline(&self, baseline: TemporalBaseline) -> Result<(), QueryError> {
        if self.durability == DurabilityMode::Memory {
            self.state
                .write()
                .unwrap_or_else(|poison| poison.into_inner())
                .baseline = Some(baseline);
            return Ok(());
        }

        let mut file = self
            .file
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let entries = self
            .state
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .by_scn
            .clone();
        self.file_needs_sync.store(true, Ordering::Release);
        *file = None;
        if let Err(err) = rewrite_ledger(&self.path, Some(baseline), &entries, self.durability) {
            *file = open_append(&self.path).ok();
            return Err(err);
        }
        *file = Some(open_append(&self.path)?);
        self.state
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
            .baseline = Some(baseline);
        Ok(())
    }

    pub(crate) fn clear_baseline(&self) -> Result<(), QueryError> {
        if self.baseline().is_none() {
            return Ok(());
        }
        if self.durability == DurabilityMode::Memory {
            self.state
                .write()
                .unwrap_or_else(|poison| poison.into_inner())
                .baseline = None;
            return Ok(());
        }
        let mut file = self
            .file
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let entries = self
            .state
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .by_scn
            .clone();
        self.file_needs_sync.store(true, Ordering::Release);
        *file = None;
        if let Err(err) = rewrite_ledger(&self.path, None, &entries, self.durability) {
            *file = open_append(&self.path).ok();
            return Err(err);
        }
        *file = Some(open_append(&self.path)?);
        self.state
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
            .baseline = None;
        Ok(())
    }

    /// Map wall-clock time to the largest complete SCN prefix. Running-max
    /// commit time is evaluated in SCN order, so out-of-order concurrent
    /// commit publication can never expose a prefix with a missing lower SCN.
    pub(crate) fn scn_as_of(&self, target_unix_us: u64, visible_scn: u64) -> Option<u64> {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poison| poison.into_inner());
        let baseline = state
            .baseline
            .filter(|baseline| target_unix_us >= baseline.available_unix_us);
        let mut running_max_time = baseline.map_or(0, |baseline| baseline.available_unix_us);
        let mut result = baseline.map(|baseline| baseline.snapshot_scn.min(visible_scn));
        for entry in &state.by_scn {
            if entry.commit_scn > visible_scn {
                break;
            }
            if baseline.is_some_and(|baseline| entry.commit_scn <= baseline.snapshot_scn) {
                continue;
            }
            running_max_time = running_max_time.max(entry.commit_unix_us);
            if running_max_time <= target_unix_us {
                result = Some(entry.commit_scn);
            } else {
                break;
            }
        }
        result
    }

    pub(crate) fn txn_commit(&self, txn_id: u64) -> Option<CommitLedgerEntry> {
        self.state
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .by_txn
            .get(&txn_id)
            .copied()
    }

    pub(crate) fn txn_ids_committed_since(&self, unix_us: u64) -> HashSet<u64> {
        self.state
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .by_scn
            .iter()
            .filter(|entry| entry.commit_unix_us >= unix_us)
            .map(|entry| entry.txn_id)
            .collect()
    }

    pub(crate) fn txn_ids_committed_since_for(
        &self,
        unix_us: u64,
        creator_ids: &[u64],
    ) -> HashSet<u64> {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poison| poison.into_inner());
        creator_ids
            .iter()
            .copied()
            .filter(|txn_id| {
                state
                    .by_txn
                    .get(txn_id)
                    .is_some_and(|entry| entry.commit_unix_us >= unix_us)
            })
            .collect()
    }

    pub(crate) fn earliest_time(&self) -> Option<u64> {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poison| poison.into_inner());
        state
            .baseline
            .map(|baseline| baseline.available_unix_us)
            .or_else(|| state.by_scn.first().map(|entry| entry.commit_unix_us))
    }

    pub(crate) fn bytes(&self) -> u64 {
        fs::metadata(&self.path).map(|meta| meta.len()).unwrap_or(0)
    }

    pub(crate) fn history_index_bytes(&self) -> u64 {
        fs::metadata(&self.history_path)
            .map(|meta| meta.len())
            .unwrap_or(0)
    }

    pub(crate) fn history_index_entry_count(&self) -> usize {
        self.history
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .by_key
            .values()
            .map(Vec::len)
            .sum()
    }

    pub(crate) fn history_index_stats(&self, table_id: u64, index_name: &str) -> (usize, usize) {
        let index_name = index_name.to_ascii_lowercase();
        let start = IndexRepairKey {
            table_id,
            index_name: index_name.clone(),
            key: IndexKey::from(Vec::new()),
        };
        let history = self
            .history
            .read()
            .unwrap_or_else(|poison| poison.into_inner());
        history
            .by_key
            .range(start..)
            .take_while(|(key, _)| key.table_id == table_id && key.index_name == index_name)
            .fold((0usize, 0usize), |(entries, bytes), (key, versions)| {
                let key_bytes = serde_json::to_vec(&key.key).map_or(0, |encoded| encoded.len());
                (
                    entries.saturating_add(versions.len()),
                    bytes.saturating_add(
                        key_bytes
                            .saturating_add(key.index_name.capacity())
                            .saturating_add(
                                versions
                                    .capacity()
                                    .saturating_mul(std::mem::size_of::<HistoryIndexVersion>()),
                            ),
                    ),
                )
            })
    }

    pub(crate) fn history_compactions(&self) -> u64 {
        self.history_compactions.load(Ordering::Relaxed)
    }

    pub(crate) fn history_last_compaction_unix_us(&self) -> u64 {
        self.history_last_compaction_unix_us.load(Ordering::Relaxed)
    }

    pub(crate) fn history_index_degraded(&self) -> bool {
        self.history_index_degraded.load(Ordering::Acquire)
    }
}

fn decode_history_batch(bytes: &[u8]) -> Result<Vec<HistoryIndexWireEntry>, QueryError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_slice(bytes).map_err(|err| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("history index batch decode failed: {err}"),
        )
    })
}

fn temporal_capacity_bytes(timeline: &TimelineState, history: &HistoryIndexState) -> usize {
    let timeline_bytes = timeline
        .by_scn
        .capacity()
        .saturating_mul(std::mem::size_of::<CommitLedgerEntry>())
        .saturating_add(
            timeline
                .by_txn
                .capacity()
                .saturating_mul(std::mem::size_of::<(u64, CommitLedgerEntry)>() + 24),
        )
        .saturating_add(
            timeline
                .persisted_txns
                .capacity()
                .saturating_mul(std::mem::size_of::<u64>() + 16),
        );
    let batch_bytes = history.batches.iter().fold(0usize, |total, (_, batch)| {
        total
            .saturating_add(
                std::mem::size_of::<u64>() + std::mem::size_of::<Vec<HistoryIndexWireEntry>>() + 24,
            )
            .saturating_add(
                batch
                    .capacity()
                    .saturating_mul(std::mem::size_of::<HistoryIndexWireEntry>()),
            )
            .saturating_add(batch.iter().fold(0usize, |entry_total, entry| {
                entry_total
                    .saturating_add(entry.index_name.capacity())
                    .saturating_add(
                        serde_json::to_vec(&entry.key).map_or(0, |encoded| encoded.capacity()),
                    )
            }))
    });
    let key_bytes = history
        .by_key
        .iter()
        .fold(0usize, |total, (key, versions)| {
            total
                .saturating_add(std::mem::size_of::<IndexRepairKey>() + 48)
                .saturating_add(key.index_name.capacity())
                .saturating_add(
                    serde_json::to_vec(&key.key).map_or(0, |encoded| encoded.capacity()),
                )
                .saturating_add(
                    versions
                        .capacity()
                        .saturating_mul(std::mem::size_of::<HistoryIndexVersion>()),
                )
        });
    timeline_bytes
        .saturating_add(
            history
                .batches
                .capacity()
                .saturating_mul(std::mem::size_of::<u64>() + 24),
        )
        .saturating_add(
            history
                .persisted_txns
                .capacity()
                .saturating_mul(std::mem::size_of::<u64>() + 16),
        )
        .saturating_add(batch_bytes)
        .saturating_add(key_bytes)
}

fn collect_wal_commit(txn_id: u64, payload: &WalPayload, state: &mut TimelineState) -> bool {
    match payload {
        WalPayload::CommitTxn {
            commit_scn,
            commit_unix_us,
            ..
        } if *commit_unix_us != 0 => {
            let entry = CommitLedgerEntry {
                txn_id,
                commit_scn: *commit_scn,
                commit_unix_us: *commit_unix_us,
            };
            if state.by_txn.get(&txn_id) == Some(&entry) {
                return false;
            }
            state.by_txn.insert(txn_id, entry);
            state.by_scn.push(entry);
            true
        }
        WalPayload::TxnBatch { records } => {
            let mut changed = false;
            for payload in records {
                changed |= collect_wal_commit(txn_id, payload, state);
            }
            changed
        }
        _ => false,
    }
}

fn collect_wal_history_batch(
    txn_id: u64,
    payload: &WalPayload,
    timeline: &TimelineState,
    history: &mut HistoryIndexState,
) -> bool {
    match payload {
        WalPayload::HistoryIndexBatch { bytes } if timeline.by_txn.contains_key(&txn_id) => {
            if let Ok(entries) = decode_history_batch(bytes) {
                if history.batches.get(&txn_id) != Some(&entries) {
                    history.batches.insert(txn_id, entries);
                    return true;
                }
            }
            false
        }
        WalPayload::TxnBatch { records } => {
            let mut changed = false;
            for payload in records {
                changed |= collect_wal_history_batch(txn_id, payload, timeline, history);
            }
            changed
        }
        _ => false,
    }
}

fn collect_wal_history_txns(txn_id: u64, payload: &WalPayload, txns: &mut HashSet<u64>) {
    match payload {
        WalPayload::HistoryIndexBatch { .. } => {
            txns.insert(txn_id);
        }
        WalPayload::TxnBatch { records } => {
            for payload in records {
                collect_wal_history_txns(txn_id, payload, txns);
            }
        }
        _ => {}
    }
}

fn open_append(path: &Path) -> Result<File, QueryError> {
    OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(|err| ledger_io_error(path, err))
}

fn rewrite_ledger(
    path: &Path,
    baseline: Option<TemporalBaseline>,
    entries: &[CommitLedgerEntry],
    durability: DurabilityMode,
) -> Result<Option<File>, QueryError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| ledger_io_error(path, err))?;
    }
    let temp = path.with_extension("v1.tmp");
    let file = File::create(&temp).map_err(|err| ledger_io_error(&temp, err))?;
    let mut file = BufWriter::with_capacity(64 * 1024, file);
    file.write_all(LEDGER_MAGIC)
        .map_err(|err| ledger_io_error(&temp, err))?;
    file.write_all(&LEDGER_VERSION.to_le_bytes())
        .map_err(|err| ledger_io_error(&temp, err))?;
    file.write_all(&0u32.to_le_bytes())
        .map_err(|err| ledger_io_error(&temp, err))?;
    if let Some(baseline) = baseline {
        file.write_all(&encode_baseline_record(baseline))
            .map_err(|err| ledger_io_error(&temp, err))?;
    }
    for entry in entries {
        file.write_all(&encode_record(*entry))
            .map_err(|err| ledger_io_error(&temp, err))?;
    }
    file.flush().map_err(|err| ledger_io_error(&temp, err))?;
    if durability == DurabilityMode::Strict {
        crate::platform::fs::sync_standard_file_at(file.get_ref(), &temp)
            .map_err(|err| ledger_io_error(&temp, err))?;
    }
    drop(file);
    fs::rename(&temp, path).map_err(|err| ledger_io_error(path, err))?;
    Ok(None)
}

fn encode_history_frame(txn_id: u64, payload: &[u8]) -> Result<Vec<u8>, QueryError> {
    let len = u32::try_from(payload.len()).map_err(|_| {
        error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "history index transaction batch is too large",
        )
    })?;
    let mut frame = Vec::with_capacity(HISTORY_FRAME_HEADER_LEN + payload.len());
    frame.extend_from_slice(&txn_id.to_le_bytes());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn rewrite_history_index(
    path: &Path,
    batches: &HashMap<u64, Vec<HistoryIndexWireEntry>>,
    durability: DurabilityMode,
) -> Result<(), QueryError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| history_io_error(path, err))?;
    }
    let temp = path.with_extension("v1.tmp");
    let file = File::create(&temp).map_err(|err| history_io_error(&temp, err))?;
    let mut file = BufWriter::with_capacity(64 * 1024, file);
    file.write_all(HISTORY_MAGIC)
        .map_err(|err| history_io_error(&temp, err))?;
    file.write_all(&HISTORY_VERSION.to_le_bytes())
        .map_err(|err| history_io_error(&temp, err))?;
    file.write_all(&0u32.to_le_bytes())
        .map_err(|err| history_io_error(&temp, err))?;
    let mut txn_ids = batches.keys().copied().collect::<Vec<_>>();
    txn_ids.sort_unstable();
    for txn_id in txn_ids {
        let payload = serde_json::to_vec(&batches[&txn_id]).map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("history index batch encode failed: {err}"),
            )
        })?;
        file.write_all(&encode_history_frame(txn_id, &payload)?)
            .map_err(|err| history_io_error(&temp, err))?;
    }
    file.flush().map_err(|err| history_io_error(&temp, err))?;
    match durability {
        DurabilityMode::Strict => crate::platform::fs::sync_standard_file_at(file.get_ref(), &temp),
        DurabilityMode::Relaxed | DurabilityMode::Memory => file.flush(),
    }
    .map_err(|err| history_io_error(&temp, err))?;
    drop(file);
    fs::rename(&temp, path).map_err(|err| history_io_error(path, err))?;
    Ok(())
}

fn load_history_index(path: &Path) -> Result<(HistoryIndexState, bool), QueryError> {
    let mut bytes = Vec::new();
    match File::open(path) {
        Ok(mut file) => file
            .read_to_end(&mut bytes)
            .map_err(|err| history_io_error(path, err))?,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok((HistoryIndexState::default(), true))
        }
        Err(err) => return Err(history_io_error(path, err)),
    };
    if bytes.len() < HEADER_LEN
        || &bytes[..8] != HISTORY_MAGIC
        || u32::from_le_bytes(bytes[8..12].try_into().unwrap()) != HISTORY_VERSION
    {
        return Err(history_error(path, "invalid history-index header"));
    }
    let mut state = HistoryIndexState::default();
    let mut changed = false;
    let mut offset = HEADER_LEN;
    while offset < bytes.len() {
        if bytes.len() - offset < HISTORY_FRAME_HEADER_LEN {
            return Err(history_error(path, "truncated history-index frame header"));
        }
        let txn_id = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        let len = u32::from_le_bytes(bytes[offset + 8..offset + 12].try_into().unwrap()) as usize;
        let expected = u32::from_le_bytes(bytes[offset + 12..offset + 16].try_into().unwrap());
        offset += HISTORY_FRAME_HEADER_LEN;
        let end = offset
            .checked_add(len)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| history_error(path, "truncated history-index frame payload"))?;
        let payload = &bytes[offset..end];
        if crc32fast::hash(payload) != expected {
            return Err(history_error(path, "history-index frame checksum mismatch"));
        }
        changed |= state
            .batches
            .insert(txn_id, decode_history_batch(payload)?)
            .is_some();
        state.persisted_txns.insert(txn_id);
        offset = end;
    }
    Ok((state, changed))
}

fn load_ledger(path: &Path) -> Result<(TimelineState, bool), QueryError> {
    let mut bytes = Vec::new();
    match File::open(path) {
        Ok(mut file) => file
            .read_to_end(&mut bytes)
            .map_err(|err| ledger_io_error(path, err))?,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok((TimelineState::default(), true))
        }
        Err(err) => return Err(ledger_io_error(path, err)),
    };
    if bytes.len() < HEADER_LEN
        || &bytes[..8] != LEDGER_MAGIC
        || u32::from_le_bytes(bytes[8..12].try_into().unwrap()) != LEDGER_VERSION
        || (bytes.len() - HEADER_LEN) % RECORD_LEN != 0
    {
        return Err(ledger_error(
            path,
            "invalid header or truncated fixed-width record",
        ));
    }
    let mut state = TimelineState::default();
    for chunk in bytes[HEADER_LEN..].chunks_exact(RECORD_LEN) {
        let expected = u32::from_le_bytes(chunk[24..28].try_into().unwrap());
        if crc32fast::hash(&chunk[..24]) != expected {
            return Err(ledger_error(path, "record checksum mismatch"));
        }
        let entry = CommitLedgerEntry {
            txn_id: u64::from_le_bytes(chunk[0..8].try_into().unwrap()),
            commit_scn: u64::from_le_bytes(chunk[8..16].try_into().unwrap()),
            commit_unix_us: u64::from_le_bytes(chunk[16..24].try_into().unwrap()),
        };
        if entry.commit_unix_us & BASELINE_TIME_MARKER != 0 {
            if state.baseline.is_some() {
                return Err(ledger_error(path, "duplicate temporal baseline"));
            }
            state.baseline = Some(TemporalBaseline {
                first_tracked_txn_id: entry.txn_id,
                snapshot_scn: entry.commit_scn,
                available_unix_us: entry.commit_unix_us & !BASELINE_TIME_MARKER,
            });
            continue;
        }
        if state.by_txn.insert(entry.txn_id, entry).is_some() {
            return Err(ledger_error(path, "duplicate transaction id"));
        }
        state.by_scn.push(entry);
        state.persisted_txns.insert(entry.txn_id);
    }
    Ok((state, false))
}

fn encode_record(entry: CommitLedgerEntry) -> [u8; RECORD_LEN] {
    let mut bytes = [0u8; RECORD_LEN];
    bytes[0..8].copy_from_slice(&entry.txn_id.to_le_bytes());
    bytes[8..16].copy_from_slice(&entry.commit_scn.to_le_bytes());
    bytes[16..24].copy_from_slice(&entry.commit_unix_us.to_le_bytes());
    let checksum = crc32fast::hash(&bytes[..24]);
    bytes[24..28].copy_from_slice(&checksum.to_le_bytes());
    bytes
}

fn encode_baseline_record(baseline: TemporalBaseline) -> [u8; RECORD_LEN] {
    encode_record(CommitLedgerEntry {
        txn_id: baseline.first_tracked_txn_id,
        commit_scn: baseline.snapshot_scn,
        commit_unix_us: baseline.available_unix_us | BASELINE_TIME_MARKER,
    })
}

fn ledger_io_error(path: &Path, err: std::io::Error) -> QueryError {
    ledger_error(path, format!("{err}"))
}

fn ledger_error(path: &Path, detail: impl std::fmt::Display) -> QueryError {
    error(
        crate::error::catalog::ER_GET_ERRNO,
        format!(
            "commit ledger '{}' is corrupt or unavailable: {detail}",
            path.display()
        ),
    )
}

fn history_io_error(path: &Path, err: std::io::Error) -> QueryError {
    history_error(path, err)
}

fn history_error(path: &Path, detail: impl std::fmt::Display) -> QueryError {
    error(
        crate::error::catalog::ER_GET_ERRNO,
        format!(
            "history index '{}' is corrupt or unavailable: {detail}",
            path.display()
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_tracks_sidecar_writes_and_retries_failed_barriers() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let history = TemporalHistory::open(dir.path(), DurabilityMode::Strict, &[]).unwrap();
        assert!(history.file_needs_sync.load(Ordering::Acquire));
        assert!(history.history_needs_sync.load(Ordering::Acquire));
        history.checkpoint_sync().unwrap();
        assert!(!history.file_needs_sync.load(Ordering::Acquire));
        assert!(!history.history_needs_sync.load(Ordering::Acquire));
        #[cfg(target_os = "macos")]
        {
            // A clean checkpoint must not issue file IO. Swap in a handle
            // whose fsync fails, without marking any application write.
            let good = history
                .file
                .lock()
                .unwrap()
                .replace(File::open("/dev/null").unwrap());
            history.checkpoint_sync().unwrap();
            *history.file.lock().unwrap() = good;
        }
        history.persist_history_index_batch(1, b"[]").unwrap();
        history
            .persist_commit(CommitLedgerEntry {
                txn_id: 1,
                commit_scn: 1,
                commit_unix_us: 100,
            })
            .unwrap();
        assert!(history.file_needs_sync.load(Ordering::Acquire));
        assert!(history.history_needs_sync.load(Ordering::Acquire));
        #[cfg(target_os = "macos")]
        {
            let good = history
                .file
                .lock()
                .unwrap()
                .replace(File::open("/dev/null").unwrap());
            assert!(history.checkpoint_sync().is_err());
            assert!(history.file_needs_sync.load(Ordering::Acquire));
            assert!(history.history_needs_sync.load(Ordering::Acquire));
            *history.file.lock().unwrap() = good;
        }
        history.checkpoint_sync().unwrap();
        assert!(!history.file_needs_sync.load(Ordering::Acquire));
        assert!(!history.history_needs_sync.load(Ordering::Acquire));
        #[cfg(target_os = "macos")]
        {
            let aborted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _batch = crate::platform::fs::FixtureSyncBatch::new(dir.path()).unwrap();
                history.checkpoint_sync().unwrap();
                panic!("abort private temporal fixture construction");
            }));
            assert!(aborted.is_err());
            let good = history
                .file
                .lock()
                .unwrap()
                .replace(File::open("/dev/null").unwrap());
            assert!(
                history.checkpoint_sync().is_err(),
                "a deferred barrier cannot certify real IO"
            );
            *history.file.lock().unwrap() = good;
            history.checkpoint_sync().unwrap();
        }
        drop(history);
        let reopened = TemporalHistory::open(dir.path(), DurabilityMode::Strict, &[]).unwrap();
        assert_eq!(reopened.txn_commit(1).unwrap().commit_scn, 1);
        assert_eq!(reopened.history.read().unwrap().batches.len(), 1);
        assert!(!reopened.history_index_degraded());
    }

    #[test]
    fn reopen_reuses_valid_temporal_files_and_repairs_wal_tail() {
        fn record(txn_id: u64) -> crate::wal::WalRecord {
            let batch = vec![HistoryIndexWireEntry {
                table_id: 17,
                index_name: "old_key".into(),
                key: IndexKey::from(vec![crate::storage::KeyPart::String(format!(
                    "key-{txn_id}"
                ))]),
                row_id: RowId {
                    page_id: 1,
                    slot_id: txn_id as u16,
                },
            }];
            crate::wal::WalRecord {
                source_time_us: txn_id * 100,
                lsn: txn_id,
                prev_txn_lsn: 0,
                txn_id,
                payload: WalPayload::TxnBatch {
                    records: vec![
                        WalPayload::HistoryIndexBatch {
                            bytes: serde_json::to_vec(&batch).unwrap(),
                        },
                        WalPayload::CommitTxn {
                            commit_scn: txn_id,
                            commit_unix_us: txn_id * 100,
                            binlog: None,
                        },
                    ],
                },
            }
        }
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut wal = vec![record(1), record(2)];
        let history =
            TemporalHistory::open(dir.path(), DurabilityMode::Strict, wal.as_slice()).unwrap();
        history
            .establish_baseline(TemporalBaseline {
                first_tracked_txn_id: 1,
                snapshot_scn: 0,
                available_unix_us: 1,
            })
            .unwrap();
        history.checkpoint_sync().unwrap();
        drop(history);
        let ledger = dir.path().join("system/commit-ledger.v1");
        let index = dir.path().join("system/history-index.v1");
        let original = [fs::read(&ledger).unwrap(), fs::read(&index).unwrap()];
        #[cfg(unix)]
        let originals = [File::open(&ledger).unwrap(), File::open(&index).unwrap()];
        for _ in 0..2 {
            let reopened =
                TemporalHistory::open(dir.path(), DurabilityMode::Strict, wal.as_slice()).unwrap();
            assert_eq!(reopened.txn_commit(2).unwrap().commit_unix_us, 200);
            assert_eq!(reopened.baseline().unwrap().available_unix_us, 1);
            assert!(!reopened.history_index_degraded());
            for (i, path) in [&ledger, &index].into_iter().enumerate() {
                assert_eq!(fs::read(path).unwrap(), original[i]);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    assert_eq!(
                        fs::metadata(path).unwrap().ino(),
                        originals[i].metadata().unwrap().ino(),
                        "unchanged temporal sidecar was replaced: {}",
                        path.display()
                    );
                }
            }
        }
        // The next committed WAL batch is absent from both sidecars.
        wal.push(record(3));
        let repaired =
            TemporalHistory::open(dir.path(), DurabilityMode::Strict, wal.as_slice()).unwrap();
        assert_eq!(repaired.txn_commit(3).unwrap().commit_scn, 3);
        assert_eq!(repaired.history.read().unwrap().batches.len(), 3);
        assert!(!repaired.history_index_degraded());
        drop(repaired);
        let reopened = TemporalHistory::open(dir.path(), DurabilityMode::Strict, &[]).unwrap();
        assert_eq!(reopened.txn_commit(3).unwrap().commit_scn, 3);
        assert_eq!(reopened.history.read().unwrap().batches.len(), 3);
        #[cfg(target_os = "macos")]
        {
            // A failed batched sync must propagate and leave retry possible.
            let original = reopened
                .file
                .lock()
                .unwrap()
                .replace(File::open("/dev/null").unwrap());
            assert!(reopened.checkpoint_sync().is_err());
            *reopened.file.lock().unwrap() = original;
            reopened.checkpoint_sync().unwrap();
        }
        drop(reopened);
        // A damaged history sidecar must still be rebuilt from WAL.
        fs::write(&index, b"torn").unwrap();
        let repaired =
            TemporalHistory::open(dir.path(), DurabilityMode::Strict, wal.as_slice()).unwrap();
        assert_eq!(repaired.history.read().unwrap().batches.len(), 3);
        assert!(!repaired.history_index_degraded());
        drop(repaired);
        // Corruption of the authoritative ledger remains fail-closed.
        fs::write(&ledger, b"torn").unwrap();
        assert!(TemporalHistory::open(dir.path(), DurabilityMode::Strict, wal.as_slice()).is_err());
    }

    #[test]
    fn history_range_projections_preserve_bounds_scope_and_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let history = TemporalHistory::open(dir.path(), DurabilityMode::Memory, &[]).unwrap();
        let keys = [
            vec![super::super::KeyPart::Int(1)],
            vec![
                super::super::KeyPart::Int(2),
                super::super::KeyPart::String("two".into()),
            ],
            vec![super::super::KeyPart::Int(3)],
        ]
        .map(IndexKey::from);
        {
            let mut state = history.history.write().unwrap();
            for table_id in [1, 2] {
                for index_name in ["a", "idx", "z"] {
                    for (i, key) in keys.iter().enumerate() {
                        state.by_key.insert(
                            IndexRepairKey {
                                table_id,
                                index_name: index_name.into(),
                                key: key.clone(),
                            },
                            [5, 10, 15]
                                .map(|commit_scn| HistoryIndexVersion {
                                    row_id: RowId {
                                        page_id: i as u32,
                                        slot_id: commit_scn as u16,
                                    },
                                    commit_scn,
                                })
                                .to_vec(),
                        );
                    }
                }
            }
        }
        let mut bounds = vec![None];
        for key in &keys {
            for inclusive in [false, true] {
                bounds.push(Some(IndexBound {
                    key: key.clone(),
                    inclusive,
                }));
            }
        }
        for snapshot in [0, 5, 10, 15] {
            let txn = TxnView::read_only(snapshot, super::super::IsolationLevel::ReadCommitted);
            for low in &bounds {
                for high in &bounds {
                    let mut expected = Vec::new();
                    for (i, key) in keys.iter().enumerate() {
                        let in_bounds = low
                            .as_ref()
                            .is_none_or(|b| key > &b.key || (b.inclusive && key == &b.key))
                            && high
                                .as_ref()
                                .is_none_or(|b| key < &b.key || (b.inclusive && key == &b.key));
                        for scn in [5, 10, 15] {
                            if in_bounds && scn > snapshot {
                                expected.push((
                                    key.clone(),
                                    RowId {
                                        page_id: i as u32,
                                        slot_id: scn as u16,
                                    },
                                ));
                            }
                        }
                    }
                    assert_eq!(
                        history.history_index_entries_bounds(
                            txn,
                            1,
                            "IDX",
                            low.as_ref(),
                            high.as_ref()
                        ),
                        expected
                    );
                    assert_eq!(
                        history.history_index_row_ids_bounds(
                            txn,
                            1,
                            "IDX",
                            low.as_ref(),
                            high.as_ref()
                        ),
                        expected.iter().map(|(_, row)| *row).collect::<Vec<_>>()
                    );
                }
            }
        }
        history
            .history_index_degraded
            .store(true, Ordering::Release);
        let txn = TxnView::read_only(0, super::super::IsolationLevel::ReadCommitted);
        assert!(history
            .history_index_entries_bounds(txn, 1, "idx", None, None)
            .is_empty());
        assert!(history
            .history_index_row_ids_bounds(txn, 1, "idx", None, None)
            .is_empty());
    }

    #[test]
    fn compaction_prunes_ledger_batches_and_reservations() {
        let dir = tempfile::tempdir().unwrap();
        let history = TemporalHistory::open(dir.path(), DurabilityMode::Relaxed, &[]).unwrap();
        history
            .establish_baseline(TemporalBaseline {
                first_tracked_txn_id: 1,
                snapshot_scn: 0,
                available_unix_us: 100,
            })
            .unwrap();

        for txn_id in 1..=8 {
            history.reserve_commit_capacity(txn_id, 2).unwrap();
            history.persist_history_index_batch(txn_id, b"[]").unwrap();
            history
                .persist_commit(CommitLedgerEntry {
                    txn_id,
                    commit_scn: txn_id,
                    commit_unix_us: txn_id * 100,
                })
                .unwrap();
        }
        let charged_before = history
            .memory_charge
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .bytes();

        history
            .compact_history_index(
                Some(4),
                Some(TemporalBaseline {
                    first_tracked_txn_id: 5,
                    snapshot_scn: 4,
                    available_unix_us: 400,
                }),
                false,
            )
            .unwrap();

        assert!(history.txn_commit(1).is_none());
        assert_eq!(history.txn_commit(5).unwrap().commit_scn, 5);
        assert_eq!(history.scn_as_of(450, 8), Some(4));
        assert_eq!(
            history
                .history
                .read()
                .unwrap_or_else(|poison| poison.into_inner())
                .batches
                .len(),
            4
        );
        let charged_after = history
            .memory_charge
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .bytes();
        assert!(charged_after < charged_before);
        drop(history);

        let reopened = TemporalHistory::open(dir.path(), DurabilityMode::Relaxed, &[]).unwrap();
        assert!(reopened.txn_commit(1).is_none());
        assert_eq!(reopened.txn_commit(8).unwrap().commit_scn, 8);
        assert_eq!(reopened.scn_as_of(450, 8), Some(4));
        assert_eq!(
            reopened
                .history
                .read()
                .unwrap_or_else(|poison| poison.into_inner())
                .batches
                .len(),
            4
        );
    }

    #[test]
    fn compaction_can_clear_disabled_timeline() {
        let dir = tempfile::tempdir().unwrap();
        let history = TemporalHistory::open(dir.path(), DurabilityMode::Memory, &[]).unwrap();
        history.reserve_commit_capacity(7, 0).unwrap();
        history.note_commit(CommitLedgerEntry {
            txn_id: 7,
            commit_scn: 3,
            commit_unix_us: 300,
        });

        history.compact_history_index(None, None, true).unwrap();

        assert!(history.txn_commit(7).is_none());
        assert!(history.earliest_time().is_none());
    }
}
