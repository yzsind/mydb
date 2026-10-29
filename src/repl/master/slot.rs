//! Replication slots: one slot per registered replica, persisted as
//! `<data-dir>/replication/slots/<uuid>.slot`. Each slot tracks the
//! highest LSN the replica has acknowledged, the last time we heard
//! from it, and its status (active / abandoned / invalidated).
//!
//! The slot file uses the same write-tmp + rename atomicity pattern as
//! `role.json`. A `SlotRegistry` holds the in-memory state and
//! discovers existing slot files on startup.

use std::collections::HashMap;
use std::fs::{self};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::storage::error;
use crate::types::QueryError;

pub const SLOTS_DIR: &str = "system/replication/slots";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SlotStatus {
    /// Replica is connected (or recently was) — slot blocks WAL pruning.
    Active,
    /// Replica hasn't been seen for longer than the slot timeout. Slot
    /// no longer participates in retention; if the replica reconnects
    /// before its WAL position is pruned, status flips back to Active.
    Abandoned,
    /// Replica fell too far behind: the WAL it needs has been
    /// force-pruned to respect `max_retained_bytes`. The replica must
    /// do a base backup to recover.
    Invalidated,
}

impl SlotStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            SlotStatus::Active => "active",
            SlotStatus::Abandoned => "abandoned",
            SlotStatus::Invalidated => "invalidated",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlotState {
    pub replica_uuid: String,
    pub acked_lsn: u64,
    /// Actual ACK on this connection, separate from a seeded retention floor.
    /// Never persist it as evidence that a future connection is durable.
    #[serde(skip)]
    pub(crate) confirmed_acked_lsn: u64,
    pub last_seen_ms: u64,
    pub first_registered_ms: u64,
    pub status: SlotStatus,
    /// Live authenticated sender connection. This is deliberately separate
    /// from retention status: a disconnected slot may continue pinning WAL but
    /// must leave the semi-sync quorum immediately.
    #[serde(default)]
    pub connected: bool,
    /// In-flight base backup owns a retention lease until streaming starts.
    #[serde(skip)]
    pub seeding: bool,
}

impl SlotState {
    pub fn new(replica_uuid: String, now_ms: u64) -> Self {
        Self {
            replica_uuid,
            acked_lsn: 0,
            confirmed_acked_lsn: 0,
            last_seen_ms: now_ms,
            first_registered_ms: now_ms,
            status: SlotStatus::Active,
            connected: true,
            seeding: false,
        }
    }
}

/// Handle to a single slot's state. Shared between the sender task that
/// owns the wire connection and the retention path. Updates are
/// happening under a `parking_lot::Mutex`; persistence is via the
/// owning [`SlotRegistry::persist`].
#[derive(Clone)]
pub struct SlotHandle {
    pub uuid: String,
    pub(crate) state: Arc<Mutex<SlotState>>,
    /// Shared with [`SlotRegistry`] — every `record_ack` call notifies
    /// the registry-wide condvar so a commit waiting for semi-sync ack
    /// gets woken instead of polling.
    pub(crate) ack_signal: Arc<(StdMutex<u64>, Condvar)>,
}

impl SlotHandle {
    pub fn snapshot(&self) -> SlotState {
        self.state.lock().clone()
    }

    #[cfg(test)]
    pub fn acked_lsn(&self) -> u64 {
        self.state.lock().acked_lsn
    }

    pub fn status(&self) -> SlotStatus {
        self.state.lock().status
    }

    /// Seed the retention floor for a freshly (re)connected replica.
    /// A brand-new slot has `acked_lsn == 0`, which — while the replica
    /// catches up or streams its base backup — would pin the master's
    /// WAL retention floor to LSN 0 and can get the slot mis-classified
    /// as the worst lagger under the byte cap (spurious invalidation →
    /// re-seed loop). The replica has, by definition, already got
    /// everything below its requested start LSN, so seeding the floor
    /// there is safe. Only ever raises the floor (never regresses a
    /// real ack) and does not touch `last_seen_ms`/status.
    pub fn ensure_floor(&self, floor_lsn: u64) {
        let mut s = self.state.lock();
        if floor_lsn > s.acked_lsn {
            s.acked_lsn = floor_lsn;
        }
    }

    /// Re-establish retention only after the sender has validated a resume
    /// position or accepted a new base backup. An arbitrary ACK must never
    /// resurrect a slot whose required WAL has already been discarded.
    pub fn activate_at(&self, floor_lsn: u64) {
        let mut state = self.state.lock();
        state.acked_lsn = floor_lsn;
        state.confirmed_acked_lsn = 0;
        state.status = SlotStatus::Active;
    }

    /// Update from a replica ack. Only advances `acked_lsn` forward;
    /// always advances `last_seen_ms`. Returns the new acked_lsn.
    /// Notifies waiting semi-sync committers via the registry condvar.
    pub fn record_ack(&self, acked_lsn: u64, now_ms: u64) -> u64 {
        let new = {
            let mut s = self.state.lock();
            if acked_lsn > s.acked_lsn {
                s.acked_lsn = acked_lsn;
            }
            s.confirmed_acked_lsn = s.confirmed_acked_lsn.max(acked_lsn);
            s.last_seen_ms = now_ms;
            s.connected = true;
            s.seeding = false;
            if s.status == SlotStatus::Abandoned {
                s.status = SlotStatus::Active;
            }
            s.acked_lsn
        };
        let (lock, cvar) = &*self.ack_signal;
        let mut counter = lock.lock().unwrap();
        *counter = counter.saturating_add(1);
        cvar.notify_all();
        new
    }
}

impl std::fmt::Debug for SlotHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotHandle")
            .field("uuid", &self.uuid)
            .field("state", &*self.state.lock())
            .finish()
    }
}

/// In-memory registry of per-replica slots, backed by `<data-dir>/replication/slots/`.
#[derive(Debug)]
pub struct SlotRegistry {
    dir: PathBuf,
    slots: HashMap<String, SlotHandle>,
    ack_signal: Arc<(StdMutex<u64>, Condvar)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemiSyncOutcome {
    /// At least `wait_count` slots had `acked_lsn >= target_lsn`.
    Acknowledged,
    /// Waited up to `timeout` without reaching the threshold; commit
    /// proceeds anyway (degraded to async-equivalent).
    TimedOut,
    /// No active slots — no point waiting; commit proceeds.
    NoActiveSlots,
    /// Fewer registered slots than `wait_count` — the threshold is
    /// unreachable no matter how long we wait, so we degrade to async
    /// immediately instead of burning the full timeout on every commit.
    /// Distinct from `NoActiveSlots` (zero replicas) and `TimedOut`
    /// (replicas present but slow), so `SHOW MYDB MASTER STATUS` can
    /// report "running degraded" honestly.
    Degraded,
}

/// A point-in-time, lock-free snapshot of the slots needed to wait for a
/// semi-sync ack WITHOUT holding the `SlotRegistry` (parking_lot) mutex.
///
/// Before this, the commit-path hook held `manager.slots.lock()` for the
/// entire ack wait (see `after_local_commit_durable`), which serialized
/// every concurrent committer through one lock during the wait window —
/// semi-sync throughput collapsed to a single in-flight commit. The
/// waiter clones the per-slot state `Arc`s and the shared `ack_signal`
/// under a brief registry lock, then the caller releases the registry
/// lock and blocks on the condvar here. New slots that connect mid-wait
/// aren't in this snapshot — correct, since they weren't present to ack
/// the commit in question; existing slots still wake us via the shared
/// `ack_signal`.
pub struct SemiSyncWaiter {
    ack_signal: Arc<(StdMutex<u64>, Condvar)>,
    slots: Vec<Arc<Mutex<SlotState>>>,
}

impl SemiSyncWaiter {
    fn active_slot_count(&self) -> u32 {
        self.slots
            .iter()
            .filter(|s| {
                let s = s.lock();
                s.status == SlotStatus::Active && s.connected
            })
            .count() as u32
    }

    /// Same contract as [`SlotRegistry::wait_for_ack`], but holds no
    /// registry lock while waiting.
    pub fn wait_for_ack(
        &self,
        target_lsn: u64,
        wait_count: u32,
        timeout: Duration,
    ) -> SemiSyncOutcome {
        if wait_count == 0 {
            return SemiSyncOutcome::Acknowledged;
        }
        // Threshold unreachable regardless of waiting: fewer slots exist
        // than we require acks from. Degrade immediately.
        let eligible = self.active_slot_count();
        if eligible < wait_count {
            if eligible == 0 {
                return SemiSyncOutcome::NoActiveSlots;
            }
            return SemiSyncOutcome::Degraded;
        }
        if self.active_slot_count() == 0 {
            return SemiSyncOutcome::NoActiveSlots;
        }
        let deadline = Instant::now() + timeout;
        let (lock, cvar) = &*self.ack_signal;
        let mut counter = lock.lock().unwrap();
        loop {
            drop(counter);
            let met = self
                .slots
                .iter()
                .filter(|s| {
                    let s = s.lock();
                    s.status == SlotStatus::Active
                        && s.connected
                        && s.confirmed_acked_lsn >= target_lsn
                })
                .count() as u32;
            if met >= wait_count {
                return SemiSyncOutcome::Acknowledged;
            }
            let now = Instant::now();
            if now >= deadline {
                return SemiSyncOutcome::TimedOut;
            }
            if crate::engine::cancel::check_cancelled().is_err() {
                return SemiSyncOutcome::Degraded;
            }
            let remaining = (deadline - now).min(Duration::from_millis(50));
            counter = lock.lock().unwrap();
            let (new_counter, _wait_res) = cvar.wait_timeout(counter, remaining).unwrap();
            counter = new_counter;
        }
    }
}

impl SlotRegistry {
    /// Open the registry under `data_dir`, discovering any existing
    /// slot files.
    pub fn open(data_dir: impl Into<PathBuf>) -> Result<Self, QueryError> {
        let dir: PathBuf = data_dir.into().join(SLOTS_DIR);
        fs::create_dir_all(&dir).map_err(io_to_query)?;
        let mut slots = HashMap::new();
        for entry in fs::read_dir(&dir).map_err(io_to_query)? {
            let entry = entry.map_err(io_to_query)?;
            let name = entry.file_name();
            let name_str = match name.to_str() {
                Some(s) => s,
                None => continue,
            };
            // Clean stray .tmp half-writes from a crashed rename first
            // (these end with `.slot.tmp`, so check before the `.slot`
            // suffix filter).
            if name_str.ends_with(".tmp") {
                let _ = fs::remove_file(entry.path());
                continue;
            }
            if !name_str.ends_with(".slot") {
                continue;
            }
            let bytes = fs::read(entry.path()).map_err(io_to_query)?;
            let mut state: SlotState = match serde_json::from_slice(&bytes) {
                Ok(s) => s,
                Err(e) => {
                    // A single corrupt slot must not block the whole
                    // master's replication subsystem from starting.
                    // Quarantine it (rename to `.corrupt`) and carry on;
                    // the affected replica re-registers on reconnect (or
                    // re-seeds via base backup if its WAL was pruned).
                    let corrupt = entry.path().with_extension("slot.corrupt");
                    let _ = fs::rename(entry.path(), &corrupt);
                    crate::util::logging::log_message(format_args!(
                        "mydb-repl master: slot file {name_str} is corrupt ({e}); \
                         quarantined to {} and skipped",
                        corrupt.display()
                    ));
                    continue;
                }
            };
            // A process restart has no live TCP senders, regardless of what a
            // crash-stale slot file recorded.
            state.connected = false;
            let uuid = state.replica_uuid.clone();
            // ack_signal will be re-bound below once the registry has its own.
            let placeholder_signal = Arc::new((StdMutex::new(0u64), Condvar::new()));
            slots.insert(
                uuid.clone(),
                SlotHandle {
                    uuid,
                    state: Arc::new(Mutex::new(state)),
                    ack_signal: placeholder_signal,
                },
            );
        }
        let ack_signal = Arc::new((StdMutex::new(0u64), Condvar::new()));
        // Re-bind existing handles to the registry's ack_signal so all
        // newly created and recovered handles signal through the same
        // condvar.
        let slots = slots
            .into_iter()
            .map(|(uuid, h)| {
                (
                    uuid,
                    SlotHandle {
                        uuid: h.uuid,
                        state: h.state,
                        ack_signal: Arc::clone(&ack_signal),
                    },
                )
            })
            .collect();
        Ok(Self {
            dir,
            slots,
            ack_signal,
        })
    }

    /// Look up a slot by replica UUID without creating one.
    pub fn get(&self, replica_uuid: &str) -> Option<SlotHandle> {
        self.slots.get(replica_uuid).cloned()
    }

    /// Get an existing slot or create a fresh one.
    pub fn create_or_get(&mut self, replica_uuid: &str, now_ms: u64) -> SlotHandle {
        if let Some(h) = self.slots.get(replica_uuid) {
            let mut state = h.state.lock();
            state.connected = true;
            state.confirmed_acked_lsn = 0;
            state.last_seen_ms = now_ms;
            if state.status == SlotStatus::Abandoned {
                state.status = SlotStatus::Active;
            }
            drop(state);
            return h.clone();
        }
        let state = SlotState::new(replica_uuid.to_string(), now_ms);
        let handle = SlotHandle {
            uuid: replica_uuid.to_string(),
            state: Arc::new(Mutex::new(state)),
            ack_signal: Arc::clone(&self.ack_signal),
        };
        self.slots.insert(replica_uuid.to_string(), handle.clone());
        handle
    }

    /// Pin WAL from `floor_lsn` for a node that is expected to connect later,
    /// such as the former source after a planned switchover. The slot is
    /// disconnected, so it never counts toward the semi-sync quorum, and the
    /// ordinary retention policy (slot timeout, byte cap) still releases it.
    pub fn reserve_disconnected(
        &mut self,
        replica_uuid: &str,
        floor_lsn: u64,
        now_ms: u64,
    ) -> SlotHandle {
        let handle = self.create_or_get(replica_uuid, now_ms);
        {
            let mut state = handle.state.lock();
            state.acked_lsn = floor_lsn;
            state.confirmed_acked_lsn = 0;
            state.status = SlotStatus::Active;
            state.connected = false;
        }
        handle
    }

    /// Build a lock-free [`SemiSyncWaiter`] snapshot. Call this under a
    /// brief registry lock, release the registry lock, then wait on the
    /// returned handle — so concurrent committers don't serialize on the
    /// registry mutex during the ack wait.
    pub(crate) fn named_sequence_waiter(&self, names: &[String]) -> Option<SemiSyncWaiter> {
        let slots = names
            .iter()
            .map(|name| self.slots.get(name).map(|h| Arc::clone(&h.state)))
            .collect::<Option<Vec<_>>>()?;
        Some(SemiSyncWaiter {
            ack_signal: Arc::clone(&self.ack_signal),
            slots,
        })
    }

    pub fn semi_sync_waiter(&self) -> SemiSyncWaiter {
        SemiSyncWaiter {
            ack_signal: Arc::clone(&self.ack_signal),
            slots: self.slots.values().map(|h| Arc::clone(&h.state)).collect(),
        }
    }

    #[cfg(test)]
    /// Block the caller until at least `wait_count` active slots have
    /// `acked_lsn >= target_lsn` or `timeout` elapses. Designed for the
    /// master-side commit-path semi-sync hook; cheap when there are no
    /// active slots (returns `NoActiveSlots` immediately).
    ///
    /// NOTE: this holds `&self` for the whole wait. The commit-path hook
    /// must instead go through [`SlotRegistry::semi_sync_waiter`] so it
    /// doesn't pin the registry lock; this method is kept for tests and
    /// non-hot callers.
    pub fn wait_for_ack(
        &self,
        target_lsn: u64,
        wait_count: u32,
        timeout: Duration,
    ) -> SemiSyncOutcome {
        self.semi_sync_waiter()
            .wait_for_ack(target_lsn, wait_count, timeout)
    }

    /// Persist a slot's state to disk atomically.
    pub fn persist(&self, replica_uuid: &str) -> Result<(), QueryError> {
        let handle = match self.slots.get(replica_uuid) {
            Some(h) => h,
            None => return Ok(()),
        };
        let snap = handle.snapshot();
        let path = slot_path(&self.dir, replica_uuid);
        let tmp = path.with_extension("slot.tmp");
        let bytes = serde_json::to_vec_pretty(&snap).map_err(|e| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("slot persist: {e}"),
            )
        })?;
        // Crash-safe atomic write: fsync tmp + parent dir so a recovered
        // master reads a real acked_lsn, not a stale/empty slot.
        crate::repl::durable::atomic_write_sync(&path, &tmp, &bytes).map_err(io_to_query)?;
        Ok(())
    }

    #[cfg(test)]
    /// Persist every slot. Use sparingly; per-slot persistence via
    /// `persist(uuid)` is preferred for hot-path updates.
    pub fn persist_all(&self) -> Result<(), QueryError> {
        for uuid in self.slots.keys() {
            self.persist(uuid)?;
        }
        Ok(())
    }

    /// Remove a slot and delete its on-disk file. Returns true if the
    /// slot existed.
    pub fn drop_slot(&mut self, replica_uuid: &str) -> Result<bool, QueryError> {
        if let Some(_handle) = self.slots.remove(replica_uuid) {
            let path = slot_path(&self.dir, replica_uuid);
            if path.exists() {
                fs::remove_file(&path).map_err(io_to_query)?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Mark a slot as abandoned (no longer contributing to retention).
    /// Returns the previous status, or None if the slot doesn't exist.
    pub fn mark_abandoned(&self, replica_uuid: &str) -> Option<SlotStatus> {
        let handle = self.slots.get(replica_uuid)?;
        let mut s = handle.state.lock();
        let prev = s.status;
        if s.status != SlotStatus::Invalidated {
            s.status = SlotStatus::Abandoned;
        }
        Some(prev)
    }

    /// Mark a slot as invalidated (WAL it needed has been force-pruned).
    pub fn mark_invalidated(&self, replica_uuid: &str) -> Option<SlotStatus> {
        let handle = self.slots.get(replica_uuid)?;
        let mut s = handle.state.lock();
        let prev = s.status;
        s.status = SlotStatus::Invalidated;
        Some(prev)
    }

    #[cfg(test)]
    pub fn mark_disconnected(&self, replica_uuid: &str) -> bool {
        let Some(handle) = self.slots.get(replica_uuid) else {
            return false;
        };
        handle.state.lock().connected = false;
        let (lock, cvar) = &*self.ack_signal;
        let mut counter = lock.lock().unwrap();
        *counter = counter.saturating_add(1);
        cvar.notify_all();
        true
    }

    /// Snapshot of all slot states (sorted by uuid for stability).
    pub fn snapshot(&self) -> Vec<SlotState> {
        let mut out: Vec<SlotState> = self.slots.values().map(|h| h.snapshot()).collect();
        out.sort_by(|a, b| a.replica_uuid.cmp(&b.replica_uuid));
        out
    }

    #[cfg(test)]
    /// Lowest acked_lsn across all slots considered for retention. If
    /// `only_active` is true, abandoned/invalidated slots are excluded.
    /// Returns `None` when no qualifying slot exists.
    pub fn min_acked_lsn(&self, only_active: bool) -> Option<u64> {
        let mut min: Option<u64> = None;
        for handle in self.slots.values() {
            let snap = handle.snapshot();
            if only_active && snap.status != SlotStatus::Active {
                continue;
            }
            min = Some(min.map_or(snap.acked_lsn, |m| m.min(snap.acked_lsn)));
        }
        min
    }

    #[cfg(test)]
    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }
}

fn slot_path(dir: &Path, replica_uuid: &str) -> PathBuf {
    dir.join(format!("{replica_uuid}.slot"))
}

fn io_to_query(err: std::io::Error) -> QueryError {
    error(
        crate::error::catalog::ER_GET_ERRNO,
        format!("slot io: {err}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_reg(dir: &TempDir) -> SlotRegistry {
        SlotRegistry::open(dir.path()).unwrap()
    }

    #[test]
    fn create_persist_reload_round_trips_slot_state() {
        let dir = TempDir::new().unwrap();
        let mut reg = open_reg(&dir);
        let handle = reg.create_or_get("replica-A", 1000);
        handle.record_ack(500, 1500);
        reg.persist("replica-A").unwrap();
        drop(reg);

        let reg2 = open_reg(&dir);
        let h2 = reg2.get("replica-A").expect("slot present");
        let snap = h2.snapshot();
        assert_eq!(snap.acked_lsn, 500);
        assert_eq!(snap.last_seen_ms, 1500);
        assert_eq!(snap.first_registered_ms, 1000);
        assert_eq!(snap.status, SlotStatus::Active);
    }

    #[test]
    fn reserved_former_source_slot_pins_fork_without_joining_quorum() {
        let dir = TempDir::new().unwrap();
        let mut reg = open_reg(&dir);
        reg.reserve_disconnected("former-source", 112, 1_000);
        reg.persist("former-source").unwrap();
        drop(reg);
        let reg = open_reg(&dir);
        let snap = reg
            .get("former-source")
            .expect("reserved slot persists")
            .snapshot();
        assert_eq!(snap.acked_lsn, 112);
        assert_eq!(snap.status, SlotStatus::Active);
        assert!(!snap.connected, "a reserved slot is not a semi-sync voter");
        let policy = crate::repl::master::RetentionPolicy {
            max_retained_bytes: u64::MAX,
            slot_timeout_ms: 300_000,
        };
        let pinned = policy.decide(200, &reg.snapshot(), &[], 2_000);
        assert_eq!(pinned.min_required_lsn, 112);
        let released = policy.decide(200, &reg.snapshot(), &[], 1_000 + 300_000);
        assert_eq!(
            released.abandon_slot_uuids,
            vec!["former-source".to_string()]
        );
    }

    #[test]
    fn record_ack_never_regresses_lsn_but_always_advances_last_seen() {
        let mut reg = open_reg(&TempDir::new().unwrap());
        let h = reg.create_or_get("r1", 100);
        h.record_ack(200, 200);
        h.record_ack(150, 300); // regression — ignored
        let s = h.snapshot();
        assert_eq!(s.acked_lsn, 200);
        assert_eq!(s.last_seen_ms, 300);
    }

    #[test]
    fn min_acked_lsn_picks_smallest_active_only_when_requested() {
        let mut reg = open_reg(&TempDir::new().unwrap());
        let a = reg.create_or_get("a", 0);
        let b = reg.create_or_get("b", 0);
        let c = reg.create_or_get("c", 0);
        a.record_ack(1000, 1);
        b.record_ack(500, 1);
        c.record_ack(200, 1);
        reg.mark_abandoned("c");
        assert_eq!(reg.min_acked_lsn(false), Some(200), "all slots considered");
        assert_eq!(reg.min_acked_lsn(true), Some(500), "abandoned excluded");
        reg.mark_invalidated("b");
        assert_eq!(reg.min_acked_lsn(true), Some(1000));
        assert_eq!(reg.min_acked_lsn(false), Some(200));
    }

    #[test]
    fn drop_slot_removes_state_and_file() {
        let dir = TempDir::new().unwrap();
        let mut reg = open_reg(&dir);
        reg.create_or_get("x", 0);
        reg.persist("x").unwrap();
        assert!(slot_path(&dir.path().join(SLOTS_DIR), "x").exists());
        assert!(reg.drop_slot("x").unwrap());
        assert!(!slot_path(&dir.path().join(SLOTS_DIR), "x").exists());
        assert!(reg.get("x").is_none());
        // dropping non-existent is no-op.
        assert!(!reg.drop_slot("x").unwrap());
    }

    #[test]
    fn abandoned_slot_reactivates_on_next_ack() {
        let mut reg = open_reg(&TempDir::new().unwrap());
        let h = reg.create_or_get("r", 0);
        h.record_ack(100, 1);
        reg.mark_abandoned("r");
        assert_eq!(h.status(), SlotStatus::Abandoned);
        h.record_ack(200, 2);
        assert_eq!(h.status(), SlotStatus::Active);
    }

    #[test]
    fn audit_reseed_retention_floor_does_not_forge_semi_sync_ack() {
        let dir = TempDir::new().unwrap();
        let mut reg = open_reg(&dir);
        let slot = reg.create_or_get("r", 1);
        slot.record_ack(10, 2);
        reg.mark_invalidated("r");
        slot.activate_at(100);
        assert_eq!(
            reg.wait_for_ack(100, 1, Duration::ZERO),
            SemiSyncOutcome::TimedOut
        );
        slot.record_ack(100, 3);
        assert_eq!(
            reg.wait_for_ack(100, 1, Duration::ZERO),
            SemiSyncOutcome::Acknowledged
        );
    }

    #[test]
    fn audit_validated_resume_reactivates_and_persists_slot() {
        let dir = TempDir::new().unwrap();
        let mut reg = open_reg(&dir);
        let slot = reg.create_or_get("r", 1);
        slot.record_ack(10, 2);
        reg.mark_invalidated("r");
        slot.record_ack(20, 3);
        assert_eq!(slot.status(), SlotStatus::Invalidated);
        slot.activate_at(100);
        reg.persist("r").unwrap();
        assert_eq!(slot.status(), SlotStatus::Active);
        assert_eq!(slot.acked_lsn(), 100);
        let reopened = open_reg(&dir);
        assert_eq!(reopened.get("r").unwrap().status(), SlotStatus::Active);
    }

    #[test]
    fn invalidated_slot_stays_invalidated_even_on_ack() {
        let mut reg = open_reg(&TempDir::new().unwrap());
        let h = reg.create_or_get("r", 0);
        h.record_ack(100, 1);
        reg.mark_invalidated("r");
        // record_ack would normally re-activate; but invalidated must stay
        // invalidated (replica needs base backup before any future progress
        // is meaningful). The Active->Abandoned->Active transition does
        // re-activate; Invalidated->Active should not.
        assert_eq!(h.status(), SlotStatus::Invalidated);
        // record_ack currently re-activates from Abandoned only; verify it
        // does NOT re-activate from Invalidated.
        let mut s = h.state.lock();
        s.status = SlotStatus::Invalidated;
        drop(s);
        h.record_ack(200, 3);
        assert_eq!(h.status(), SlotStatus::Invalidated);
    }

    #[test]
    fn registry_discovers_existing_slot_files_on_open() {
        let dir = TempDir::new().unwrap();
        let mut reg = open_reg(&dir);
        reg.create_or_get("alpha", 0);
        reg.create_or_get("beta", 0);
        reg.persist_all().unwrap();
        drop(reg);

        let reg2 = open_reg(&dir);
        assert_eq!(reg2.slot_count(), 2);
        let mut uuids: Vec<_> = reg2
            .snapshot()
            .into_iter()
            .map(|s| s.replica_uuid)
            .collect();
        uuids.sort();
        assert_eq!(uuids, vec!["alpha".to_string(), "beta".to_string()]);
    }

    #[test]
    fn corrupt_slot_file_is_quarantined_and_skipped_on_open() {
        // A single corrupt slot must NOT block the master's whole
        // replication subsystem from starting (C11). It is quarantined
        // to `.slot.corrupt` and skipped; a good slot alongside it still
        // loads.
        let dir = TempDir::new().unwrap();
        let slots_dir = dir.path().join(SLOTS_DIR);
        fs::create_dir_all(&slots_dir).unwrap();
        fs::write(slots_dir.join("bad.slot"), b"not json").unwrap();
        // Also write a valid slot to confirm it survives.
        {
            let mut reg = SlotRegistry::open(dir.path()).unwrap();
            reg.create_or_get("good", 1);
            reg.persist("good").unwrap();
        }
        let reg = SlotRegistry::open(dir.path()).expect("open must tolerate corrupt slot");
        assert!(reg.get("good").is_some(), "valid slot must load");
        assert!(
            !slots_dir.join("bad.slot").exists(),
            "corrupt slot must be renamed away"
        );
        assert!(
            slots_dir.join("bad.slot.corrupt").exists(),
            "corrupt slot must be quarantined to .slot.corrupt"
        );
    }

    #[test]
    fn wait_for_ack_returns_immediately_when_no_active_slots() {
        let reg = open_reg(&TempDir::new().unwrap());
        let out = reg.wait_for_ack(100, 1, Duration::from_secs(5));
        assert_eq!(out, SemiSyncOutcome::NoActiveSlots);
    }

    #[test]
    fn wait_for_ack_satisfied_immediately_when_already_acked() {
        let mut reg = open_reg(&TempDir::new().unwrap());
        let h = reg.create_or_get("r", 0);
        h.record_ack(500, 0);
        let out = reg.wait_for_ack(400, 1, Duration::from_secs(5));
        assert_eq!(out, SemiSyncOutcome::Acknowledged);
    }

    #[test]
    fn wait_for_ack_times_out_when_replica_never_acks() {
        let mut reg = open_reg(&TempDir::new().unwrap());
        let _ = reg.create_or_get("r", 0);
        let out = reg.wait_for_ack(500, 1, Duration::from_millis(150));
        assert_eq!(out, SemiSyncOutcome::TimedOut);
    }

    #[test]
    fn disconnected_slot_retains_wal_but_leaves_semi_sync_quorum() {
        let dir = TempDir::new().unwrap();
        let mut reg = open_reg(&dir);
        let h = reg.create_or_get("r", 1);
        h.record_ack(100, 2);
        assert!(reg.mark_disconnected("r"));
        assert_eq!(h.status(), SlotStatus::Active, "retention remains active");
        assert_eq!(
            reg.wait_for_ack(101, 1, Duration::from_secs(30)),
            SemiSyncOutcome::NoActiveSlots
        );
    }

    #[test]
    fn ensure_floor_raises_but_never_regresses_acked() {
        let mut reg = open_reg(&TempDir::new().unwrap());
        let h = reg.create_or_get("r", 0);
        assert_eq!(h.acked_lsn(), 0);
        h.ensure_floor(500);
        assert_eq!(h.acked_lsn(), 500, "floor seeds a fresh slot");
        h.record_ack(700, 1);
        h.ensure_floor(600); // lower than real ack — must not regress
        assert_eq!(h.acked_lsn(), 700, "ensure_floor never lowers a real ack");
    }

    #[test]
    fn wait_for_ack_degrades_fast_when_fewer_slots_than_wait_count() {
        // Only one replica registered, but semi-sync wants 2 acks: the
        // threshold is unreachable, so we degrade IMMEDIATELY instead of
        // burning the full timeout on every commit (C8).
        let mut reg = open_reg(&TempDir::new().unwrap());
        let h = reg.create_or_get("r", 0);
        h.record_ack(1000, 1);
        let started = Instant::now();
        let out = reg.wait_for_ack(500, 2, Duration::from_secs(30));
        assert_eq!(out, SemiSyncOutcome::Degraded);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "must not wait out the timeout"
        );
    }

    #[test]
    fn semi_sync_waiter_observes_acks_without_registry_lock() {
        // The waiter is a detached snapshot: it must still be woken by an
        // ack delivered through the shared signal, proving the commit
        // hook doesn't need to hold the registry lock while waiting (Pf1).
        let mut reg = open_reg(&TempDir::new().unwrap());
        let h = reg.create_or_get("r", 0);
        let waiter = reg.semi_sync_waiter();
        let waker = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            h.record_ack(500, 0);
        });
        let out = waiter.wait_for_ack(500, 1, Duration::from_secs(2));
        waker.join().unwrap();
        assert_eq!(out, SemiSyncOutcome::Acknowledged);
    }

    #[test]
    fn wait_for_ack_wakes_when_replica_acks_during_wait() {
        let mut reg = open_reg(&TempDir::new().unwrap());
        let h = reg.create_or_get("r", 0);
        let reg = Arc::new(reg);
        let reg2 = Arc::clone(&reg);
        let waker = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            h.record_ack(500, 0);
            // keep the registry handle alive
            let _ = reg2;
        });
        let out = reg.wait_for_ack(500, 1, Duration::from_secs(2));
        waker.join().unwrap();
        assert_eq!(out, SemiSyncOutcome::Acknowledged);
    }

    #[test]
    fn stray_tmp_file_is_cleaned_up_on_open() {
        let dir = TempDir::new().unwrap();
        let slots_dir = dir.path().join(SLOTS_DIR);
        fs::create_dir_all(&slots_dir).unwrap();
        fs::write(slots_dir.join("a.slot.tmp"), b"{}").unwrap();
        let _ = SlotRegistry::open(dir.path()).unwrap();
        assert!(!slots_dir.join("a.slot.tmp").exists());
    }
}
