// P3 — sharded row-lock subsystem (TPC-C lock de-contention).
//
// MEASURED MOTIVATION (2026-06-28, `sample` on the P2 build at 4w20t):
// the dominant contended latch is the global `lock_manager` RwLock, and
// within it the ROW-lock map. `acquire_row_lock_blocking` was the #1
// exclusive-lock-slow caller (1486 samples) and `statement_savepoint`'s
// `lock_log_len` read was the #1 shared-lock-slow caller (2333), both
// stalling on the single `lock_manager` latch. `txns` (commit) was only 89.
//
// DESIGN. The row-lock state is split out of the monolithic `LockManager`
// into two sharded stores so that concurrent row-lock acquires on different
// rows run on independent fine-grained latches instead of serialising on one
// global write lock:
//
//   * `row_shards` — keyed by (table_id, row_id) hash. Holds `locks`
//     (row -> owners+mode) and `row_waiters` (row -> FIFO queue). Concurrent
//     acquires of *different* rows land on different shards => parallel.
//   * `txn_shards` — keyed by txn_id. Holds each txn's ordered row
//     `lock_log` (for release + savepoint), a `waiting` flag, and the row it
//     is currently waiting on (`waiting_on`). A txn's log is written almost
//     exclusively by its own thread, so this latch is near-uncontended; it
//     also makes `lock_log_len` (the savepoint watermark) readable without
//     the global lock_manager latch.
//
// The cross-shard deadlock graph (`waits_for`) and unique-key locks stay in
// the global `LockManager`, touched ONLY on the rare conflict / grant-after-
// wait paths. The uncontended acquire path touches only the row shard and the
// txn shard — never the global lock_manager. A lock-free-ish per-txn
// `waiting` flag gates the global `waits_for` access so a free-row acquire
// (the common case, which never waited) skips the global latch entirely.
//
// LATCH ORDER. To avoid AB-BA, latches are NEVER nested across the two row
// shards: every operation holds at most one row shard at a time, releasing it
// before touching another shard / the txn shard / the global lock_manager.
// The conflict path migrates a stale waiter off its previous row shard in a
// separate, non-nested critical section.
//
// SAFETY / CORRECTNESS. Behaviour is byte-for-byte the legacy algorithm
// (`LockManager::acquire_row_lock_with_mode` + `try_acquire_waited_row` +
// `record_resource_wait` + `unlock_txn` + `restore_savepoint_locks`), only
// relocated. The whole subsystem is gated behind `MYDB_ROW_LOCK_SHARDS`
// (default off => the legacy single-latch path is used unchanged); when off,
// none of these structures are consulted.

/// Number of row shards and txn shards. Power of two so the mask is cheap.
/// Tunable via `MYDB_ROW_LOCK_SHARDS` (clamped to a power of two).
#[cfg(not(test))]
const ROW_LOCK_SHARD_DEFAULT: usize = 64;
#[cfg(test)]
const ROW_LOCK_SHARD_DEFAULT: usize = 8;

// ahash for the same reason as `storage::txn::FastHashMap`: internal
// keys, hashed on every acquire/release — SipHash showed up in TPC-C
// profiles.
#[derive(Debug, Default)]
pub(crate) struct RowShard {
    pub(crate) locks: FastHashMap<(u64, RowId), RowLockState>,
    pub(crate) row_waiters: FastHashMap<(u64, RowId), VecDeque<RowWaiter>>,
}

#[derive(Debug, Default)]
pub(crate) struct TxnRowState {
    /// Ordered row locks held by this txn (Row / RowMode / Rows), for release
    /// and savepoint truncation. Mirrors the row entries the legacy
    /// `lock_log` carried; unique-key entries stay in the global lock_manager.
    pub(crate) log: Vec<HeldLock>,
    /// True while this txn has an outstanding `waits_for` edge in the global
    /// deadlock graph; gates the global latch on the success path.
    pub(crate) waiting: bool,
    /// The row this txn is currently blocked on (for cross-shard waiter
    /// migration when it switches the row it waits on).
    pub(crate) waiting_on: Option<(u64, RowId)>,
}

/// Outcome of a sharded row-lock acquire attempt. The `Database` layer
/// performs the (rare) global deadlock-graph interaction based on this.
#[derive(Debug, Clone)]
pub(crate) enum RowAcquireOutcome {
    /// Granted (fresh / self-holds / compatible share / upgrade). If
    /// `cleared_wait`, this grant resolved a prior wait — drop the global edge.
    Acquired { cleared_wait: bool },
    /// Blocked by `owner`. The waiter is already enqueued on the row shard and
    /// `waiting_on` updated (stale prior waiter migrated off its shard). The
    /// caller records the `waits_for` edge + runs deadlock detection.
    Conflict { owners: LockBlockers },
}

pub(crate) struct ShardedRowLocks {
    row_shards: Box<[parking_lot::Mutex<RowShard>]>,
    txn_shards: Box<[parking_lot::Mutex<FastHashMap<u64, TxnRowState>>]>,
    mask: usize,
}

impl std::fmt::Debug for ShardedRowLocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedRowLocks")
            .field("shards", &self.row_shards.len())
            .finish()
    }
}

impl ShardedRowLocks {
    pub(crate) fn new() -> Self {
        let n = row_lock_shard_count();
        let mut row_shards = Vec::with_capacity(n);
        let mut txn_shards = Vec::with_capacity(n);
        for _ in 0..n {
            row_shards.push(parking_lot::Mutex::new(RowShard::default()));
            txn_shards.push(parking_lot::Mutex::new(FastHashMap::default()));
        }
        Self {
            row_shards: row_shards.into_boxed_slice(),
            txn_shards: txn_shards.into_boxed_slice(),
            mask: n - 1,
        }
    }

    #[inline]
    fn row_shard(&self, key: (u64, RowId)) -> &parking_lot::Mutex<RowShard> {
        // Mix table and row identity. RowId is (page_id, slot_id, generation);
        // fold them with the table id so colliding physical ids across tables
        // land in different shards.
        let r = &key.1;
        let mut h = key.0.wrapping_mul(0x9E3779B97F4A7C15);
        h ^= (r.page_id as u64).wrapping_mul(0xD1B54A32D192ED03);
        h ^= (r.slot_id as u64).wrapping_mul(0x94D049BB133111EB);
        h ^= h >> 29;
        &self.row_shards[(h as usize) & self.mask]
    }

    #[inline]
    fn txn_shard(&self, txn_id: u64) -> &parking_lot::Mutex<FastHashMap<u64, TxnRowState>> {
        let key = txn_id ^ (txn_id >> 33);
        &self.txn_shards[(key as usize) & self.mask]
    }

    /// Compatibility check against the current owners of `key`, mirroring
    /// `LockManager::lock_owner_compat` plus the FIFO-fairness rule: a free row
    /// with a non-empty waiter queue whose front is not us is a Conflict so we
    /// queue behind the existing waiters instead of jumping them.
    fn compat(shard: &RowShard, key: (u64, RowId), txn_id: u64, mode: LockMode) -> LockOwnerCheck {
        match shard.locks.get(&key) {
            None => {
                // Free — but respect a non-empty FIFO queue we're not fronting.
                if let Some(q) = shard.row_waiters.get(&key) {
                    if let Some(front) = q.front() {
                        if front.txn_id != txn_id {
                            return LockOwnerCheck::Conflict(smallvec::smallvec![front.txn_id]);
                        }
                    }
                }
                LockOwnerCheck::Free
            }
            Some(state) => {
                if state.holds(txn_id) {
                    return LockOwnerCheck::SelfHolds;
                }
                if mode == LockMode::Shared && state.mode == LockMode::Shared {
                    // Still must not jump a queued exclusive waiter.
                    if let Some(q) = shard.row_waiters.get(&key) {
                        if let Some(front) = q.front() {
                            if front.txn_id != txn_id {
                                return LockOwnerCheck::Conflict(smallvec::smallvec![front.txn_id]);
                            }
                        }
                    }
                    return LockOwnerCheck::CanShare;
                }
                let owners = state.owners.blockers_except(txn_id);
                debug_assert!(!owners.is_empty());
                LockOwnerCheck::Conflict(owners)
            }
        }
    }

    /// Read-only: would `txn_id` *still* conflict trying to acquire `mode` on
    /// `(table_id,row_id)` right now (including the FIFO-fairness rule — a free
    /// row we are not fronting still counts as blocked)? A blocked waiter uses
    /// this to decide whether to park: park only while its *exact* row is still
    /// blocked. The previous guard compared a per-*shard* signal counter, which
    /// advanced on every release of any co-located row, so a waiter on a hot
    /// row (TPC-C district/warehouse) skipped parking and busy-retried through
    /// the global lock-manager latch thousands of times per acquire. This
    /// per-row check eliminates those futile wakeups while still catching the
    /// genuine "row freed in the tiny window before we parked" race (returns
    /// false → caller retries immediately). Takes only the row shard latch.
    pub(crate) fn still_blocked(
        &self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
        mode: LockMode,
    ) -> bool {
        let key = (table_id, row_id);
        let shard = self.row_shard(key).lock();
        matches!(
            Self::compat(&shard, key, txn_id, mode),
            LockOwnerCheck::Conflict(_)
        )
    }

    /// Pop `txn_id` from the front of `key`'s waiter queue if it is the front
    /// (it just got granted); GC the queue when empty.
    fn pop_front_waiter(shard: &mut RowShard, key: (u64, RowId), txn_id: u64) {
        let mut remove = false;
        if let Some(q) = shard.row_waiters.get_mut(&key) {
            if q.front().map(|w| w.txn_id) == Some(txn_id) {
                q.pop_front();
            } else {
                q.retain(|w| w.txn_id != txn_id);
            }
            remove = q.is_empty();
        }
        if remove {
            shard.row_waiters.remove(&key);
        }
    }

    fn enqueue_waiter(shard: &mut RowShard, key: (u64, RowId), txn_id: u64, mode: LockMode) {
        let q = shard.row_waiters.entry(key).or_default();
        if !q.iter().any(|w| w.txn_id == txn_id) {
            q.push_back(RowWaiter { txn_id, mode });
        }
    }

    fn dequeue_waiter(shard: &mut RowShard, key: (u64, RowId), txn_id: u64) {
        let mut remove = false;
        if let Some(q) = shard.row_waiters.get_mut(&key) {
            q.retain(|w| w.txn_id != txn_id);
            remove = q.is_empty();
        }
        if remove {
            shard.row_waiters.remove(&key);
        }
    }

    /// Try to acquire `mode` on `(table_id,row_id)` for `txn_id`. `record_log`
    /// controls whether a granted fresh lock is appended to the txn's row log
    /// (false for the check-only probe). Mirrors
    /// `LockManager::acquire_row_lock_with_mode`.
    pub(crate) fn try_acquire(
        &self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
        mode: LockMode,
        record_log: bool,
    ) -> RowAcquireOutcome {
        self.try_acquire_inner(txn_id, table_id, row_id, mode, record_log, true)
    }

    /// Immediate acquire used by an async-worker scheduling probe. A conflict
    /// is reported without joining the row FIFO or publishing a deadlock-graph
    /// edge; the caller replays the statement on the blocking pool, whose
    /// ordinary acquire performs both operations. A successful grant is a
    /// normal transaction-duration lock and is logged exactly once.
    pub(crate) fn try_acquire_without_wait(
        &self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
        mode: LockMode,
        record_log: bool,
    ) -> RowAcquireOutcome {
        self.try_acquire_inner(txn_id, table_id, row_id, mode, record_log, false)
    }

    fn try_acquire_inner(
        &self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
        mode: LockMode,
        record_log: bool,
        enqueue_on_conflict: bool,
    ) -> RowAcquireOutcome {
        let key = (table_id, row_id);
        // ---- row shard critical section ----
        let mut granted: Option<HeldLock> = None;
        let conflict_owners;
        {
            let mut shard = self.row_shard(key).lock();
            match Self::compat(&shard, key, txn_id, mode) {
                LockOwnerCheck::SelfHolds => {
                    Self::pop_front_waiter(&mut shard, key, txn_id);
                    // Possible S->X upgrade.
                    if mode == LockMode::Exclusive {
                        if let Some(state) = shard.locks.get_mut(&key) {
                            if state.mode == LockMode::Shared && state.sole_owner(txn_id) {
                                state.mode = LockMode::Exclusive;
                            } else if state.mode == LockMode::Shared {
                                let owners = state.owners.blockers_except(txn_id);
                                drop(shard);
                                return if enqueue_on_conflict {
                                    self.into_conflict(txn_id, key, owners, mode)
                                } else {
                                    RowAcquireOutcome::Conflict { owners }
                                };
                            }
                        }
                    }
                    drop(shard);
                    return RowAcquireOutcome::Acquired {
                        cleared_wait: self.clear_waiting(txn_id, Some(key)),
                    };
                }
                LockOwnerCheck::CanShare => {
                    if record_log {
                        match shard.locks.get_mut(&key) {
                            Some(state) => {
                                if !state.holds(txn_id) {
                                    state.owners.add(txn_id);
                                }
                            }
                            None => {
                                shard.locks.insert(key, RowLockState::new(mode, txn_id));
                            }
                        }
                        Self::pop_front_waiter(&mut shard, key, txn_id);
                        granted = Some(HeldLock::RowMode(table_id, row_id, mode));
                    } else {
                        Self::pop_front_waiter(&mut shard, key, txn_id);
                    }
                }
                LockOwnerCheck::Free => {
                    if record_log {
                        shard.locks.insert(key, RowLockState::new(mode, txn_id));
                        Self::pop_front_waiter(&mut shard, key, txn_id);
                        granted = Some(HeldLock::RowMode(table_id, row_id, mode));
                    } else {
                        Self::pop_front_waiter(&mut shard, key, txn_id);
                    }
                }
                LockOwnerCheck::Conflict(owners) => {
                    if enqueue_on_conflict {
                        Self::enqueue_waiter(&mut shard, key, txn_id, mode);
                        conflict_owners = owners;
                        drop(shard);
                        return self.into_conflict(txn_id, key, conflict_owners, mode);
                    }
                    return RowAcquireOutcome::Conflict { owners };
                }
            }
        }
        // ---- granted: update txn shard (row log + clear wait) ----
        let cleared = self.finish_grant(txn_id, granted, key);
        RowAcquireOutcome::Acquired {
            cleared_wait: cleared,
        }
    }

    /// Shared tail for a granted acquire: append the row-log entry (if any) and
    /// clear+report this txn's pending-wait flag, all under the txn shard.
    fn finish_grant(&self, txn_id: u64, granted: Option<HeldLock>, key: (u64, RowId)) -> bool {
        let (stale, cleared) = {
            let mut map = self.txn_shard(txn_id).lock();
            let entry = map.entry(txn_id).or_default();
            if let Some(held) = granted {
                entry.log.push(held);
            }
            (entry.waiting_on.take(), std::mem::take(&mut entry.waiting))
        };
        self.dequeue_stale_wait(txn_id, stale, Some(key));
        cleared
    }

    /// Clear+report the pending-wait flag without recording a lock (used by the
    /// self-holds path).
    fn clear_waiting(&self, txn_id: u64, key: Option<(u64, RowId)>) -> bool {
        let (stale, cleared) = {
            let mut map = self.txn_shard(txn_id).lock();
            match map.get_mut(&txn_id) {
                Some(entry) => (entry.waiting_on.take(), std::mem::take(&mut entry.waiting)),
                None => (None, false),
            }
        };
        self.dequeue_stale_wait(txn_id, stale, key);
        cleared
    }

    /// Leave the FIFO of a row this txn stopped waiting for. `waiting_on` is
    /// the only record of that queue entry: once it is cleared, neither
    /// `clear_wait` nor `release_txn` can find the entry, and a finished txn
    /// left at the queue front blocks every later locker of a free row until
    /// its lock wait times out.
    fn dequeue_stale_wait(
        &self,
        txn_id: u64,
        stale: Option<(u64, RowId)>,
        granted: Option<(u64, RowId)>,
    ) {
        if let Some(stale) = stale.filter(|stale| Some(*stale) != granted) {
            let mut shard = self.row_shard(stale).lock();
            Self::dequeue_waiter(&mut shard, stale, txn_id);
        }
    }

    /// Conflict bookkeeping: mark the txn waiting on `key`, migrate any stale
    /// waiter off the previous row it was waiting on (different shard, taken in
    /// a separate non-nested critical section), and return the Conflict.
    fn into_conflict(
        &self,
        txn_id: u64,
        key: (u64, RowId),
        owners: LockBlockers,
        _mode: LockMode,
    ) -> RowAcquireOutcome {
        let prev = {
            let mut map = self.txn_shard(txn_id).lock();
            let entry = map.entry(txn_id).or_default();
            entry.waiting = true;
            let prev = entry.waiting_on;
            entry.waiting_on = Some(key);
            prev
        };
        if let Some(prev_key) = prev {
            if prev_key != key {
                let mut shard = self.row_shard(prev_key).lock();
                Self::dequeue_waiter(&mut shard, prev_key, txn_id);
            }
        }
        RowAcquireOutcome::Conflict { owners }
    }

    /// Collect the FIFO-front waiter of each released row, for P4 directed
    /// wakeup. Mirrors `LockManager::collect_front_row_waiters`.
    pub(crate) fn collect_front_row_waiters(&self, rows: &[(u64, RowId)], out: &mut Vec<u64>) {
        for &key in rows {
            let shard = self.row_shard(key).lock();
            if let Some(q) = shard.row_waiters.get(&key) {
                if let Some(front) = q.front() {
                    out.push(front.txn_id);
                }
            }
        }
    }

    /// Current row-lock-log length for `txn_id` — the savepoint watermark.
    /// Read-only, lock-free of the global lock_manager.
    pub(crate) fn row_log_len(&self, txn_id: u64) -> usize {
        self.txn_shard(txn_id)
            .lock()
            .get(&txn_id)
            .map(|e| e.log.len())
            .unwrap_or(0)
    }


    /// Total number of row-lock + waiter entries held by a single txn batch
    /// pre-size hint; not exact across shards (only used as a reserve hint).
    pub(crate) fn approx_lock_count(&self) -> usize {
        self.row_shards
            .iter()
            .map(|s| s.lock().locks.len())
            .sum()
    }

    /// Release every row lock held by `txn_id` (commit / rollback). Returns the
    /// freed `(table_id, row_id)` pairs so the caller can wake waiters.
    pub(crate) fn release_txn(&self, txn_id: u64) -> Vec<(u64, RowId)> {
        let entry = self.txn_shard(txn_id).lock().remove(&txn_id);
        let Some(entry) = entry else {
            return Vec::new();
        };
        // If the txn was mid-wait (e.g. killed/rolled back while blocked),
        // drop its dead waiter so it doesn't stall the row's FIFO queue.
        if let Some(key) = entry.waiting_on {
            let mut shard = self.row_shard(key).lock();
            Self::dequeue_waiter(&mut shard, key, txn_id);
        }
        let mut freed = Vec::new();
        for held in entry.log {
            self.release_held(txn_id, &held, &mut freed);
        }
        freed
    }

    /// Clear a txn's pending wait: drop its waiter from whatever row it was
    /// blocked on and clear the per-txn waiting flag. Pairs with the global
    /// `LockManager::clear_wait` (which drops the `waits_for` edge). Used by
    /// the blocking-loop timeout/cancel and savepoint-rollback cleanup so a
    /// dead waiter never stalls the FIFO queue.
    pub(crate) fn clear_wait(&self, txn_id: u64) {
        self.clear_wait_with_change(txn_id);
    }

    pub(crate) fn clear_wait_with_change(&self, txn_id: u64) -> bool {
        let waiting_on = {
            let mut map = self.txn_shard(txn_id).lock();
            match map.get_mut(&txn_id) {
                Some(e) => {
                    e.waiting = false;
                    e.waiting_on.take()
                }
                None => None,
            }
        };
        if let Some(key) = waiting_on {
            let mut shard = self.row_shard(key).lock();
            Self::dequeue_waiter(&mut shard, key, txn_id);
        }
        waiting_on.is_some()
    }

    pub(crate) fn blocked_waiting_row(&self, txn_id: u64) -> Option<(u64, RowId)> {
        let key = self
            .txn_shard(txn_id)
            .lock()
            .get(&txn_id)
            .and_then(|entry| entry.waiting.then_some(entry.waiting_on).flatten())?;
        let shard = self.row_shard(key).lock();
        let held_by_other = shard
            .locks
            .get(&key)
            .is_some_and(|state| !state.holds(txn_id));
        let queued_behind_other = shard
            .row_waiters
            .get(&key)
            .and_then(|queue| queue.front())
            .is_some_and(|front| front.txn_id != txn_id);
        (held_by_other || queued_behind_other).then_some(key)
    }

    /// Restore to a savepoint: release row locks logged after `row_log_len`,
    /// returning the freed rows. Mirrors the row half of
    /// `restore_savepoint_locks`.
    pub(crate) fn restore_savepoint(&self, txn_id: u64, row_log_len: usize) -> Vec<(u64, RowId)> {
        let released: Vec<HeldLock> = {
            let mut map = self.txn_shard(txn_id).lock();
            match map.get_mut(&txn_id) {
                Some(entry) if entry.log.len() > row_log_len => {
                    entry.log.split_off(row_log_len)
                }
                _ => Vec::new(),
            }
        };
        let mut freed = Vec::new();
        for held in released.into_iter().rev() {
            self.release_held(txn_id, &held, &mut freed);
        }
        freed
    }

    fn release_held(&self, txn_id: u64, held: &HeldLock, freed: &mut Vec<(u64, RowId)>) {
        match held {
            HeldLock::Row(table_id, row_id) | HeldLock::RowMode(table_id, row_id, _) => {
                self.release_row_owner(*table_id, *row_id, txn_id);
                freed.push((*table_id, *row_id));
            }
            HeldLock::Rows(table_id, rows) => {
                for row_id in rows {
                    self.release_row_owner(*table_id, *row_id, txn_id);
                    freed.push((*table_id, *row_id));
                }
            }
            // Unique / Table entries are never in the sharded row log.
            HeldLock::Unique(_) | HeldLock::UniqueRange(_) | HeldLock::Table(_, _) => {}
        }
    }

    fn release_row_owner(&self, table_id: u64, row_id: RowId, txn_id: u64) {
        let key = (table_id, row_id);
        let mut shard = self.row_shard(key).lock();
        let mut remove = false;
        if let Some(state) = shard.locks.get_mut(&key) {
            if state.owners.remove(txn_id) {
                remove = true;
            } else if state.mode == LockMode::Exclusive {
                state.mode = LockMode::Shared;
            }
        }
        if remove {
            shard.locks.remove(&key);
        }
    }

    /// Batch fast path: acquire `mode` on every row that is immediately
    /// grantable (free + un-queued, or already-share-compatible) under one
    /// pass, recording them as a single `Rows` log entry; return the rows that
    /// must fall through to per-row blocking. Mirrors
    /// `LockManager::lock_rows_batch_nonblocking`.
    pub(crate) fn lock_rows_batch_nonblocking(
        &self,
        txn_id: u64,
        table_id: u64,
        row_ids: &[RowId],
        mode: LockMode,
    ) -> Vec<RowId> {
        let mut acquired: Vec<RowId> = Vec::with_capacity(row_ids.len());
        let mut deferred: Vec<RowId> = Vec::new();
        for &row_id in row_ids {
            let key = (table_id, row_id);
            let mut shard = self.row_shard(key).lock();
            // Never jump a non-empty queue.
            if shard
                .row_waiters
                .get(&key)
                .is_some_and(|q| !q.is_empty())
            {
                deferred.push(row_id);
                continue;
            }
            match Self::compat(&shard, key, txn_id, mode) {
                LockOwnerCheck::Free => {
                    shard.locks.insert(key, RowLockState::new(mode, txn_id));
                    acquired.push(row_id);
                }
                LockOwnerCheck::CanShare => {
                    if let Some(state) = shard.locks.get_mut(&key) {
                        if !state.holds(txn_id) {
                            state.owners.add(txn_id);
                        }
                    } else {
                        shard.locks.insert(key, RowLockState::new(mode, txn_id));
                    }
                    acquired.push(row_id);
                }
                LockOwnerCheck::SelfHolds | LockOwnerCheck::Conflict(_) => deferred.push(row_id),
            }
        }
        if !acquired.is_empty() {
            let stale = {
                let mut map = self.txn_shard(txn_id).lock();
                let entry = map.entry(txn_id).or_default();
                entry.waiting = false;
                entry.log.push(HeldLock::Rows(table_id, acquired));
                entry.waiting_on.take()
            };
            self.dequeue_stale_wait(txn_id, stale, None);
        }
        deferred
    }

    /// Snapshot all granted row locks for the `data_locks` view.
    pub(crate) fn snapshot_locks(&self) -> Vec<((u64, RowId), LockMode, Vec<u64>)> {
        let mut out = Vec::new();
        for shard in self.row_shards.iter() {
            let s = shard.lock();
            for (key, state) in s.locks.iter() {
                out.push((*key, state.mode, state.owners.as_slice().to_vec()));
            }
        }
        out
    }

    /// Reinstate a recovered/prepared row lock (XA crash-recovery migration).
    pub(crate) fn reinstate(&self, txn_id: u64, held: HeldLock) {
        match &held {
            HeldLock::Row(table_id, row_id) | HeldLock::RowMode(table_id, row_id, _) => {
                let mode = if let HeldLock::RowMode(_, _, m) = &held {
                    *m
                } else {
                    LockMode::Exclusive
                };
                let key = (*table_id, *row_id);
                self.row_shard(key)
                    .lock()
                    .locks
                    .entry(key)
                    .and_modify(|state| {
                        state.owners.add(txn_id);
                        if mode == LockMode::Exclusive {
                            state.mode = mode;
                        }
                    })
                    .or_insert_with(|| RowLockState::new(mode, txn_id));
            }
            HeldLock::Rows(table_id, rows) => {
                for row_id in rows {
                    let key = (*table_id, *row_id);
                    self.row_shard(key)
                        .lock()
                        .locks
                        .entry(key)
                        .and_modify(|state| {
                            state.owners.add(txn_id);
                            state.mode = LockMode::Exclusive;
                        })
                        .or_insert_with(|| RowLockState::new(LockMode::Exclusive, txn_id));
                }
            }
            HeldLock::Unique(_) | HeldLock::UniqueRange(_) | HeldLock::Table(_, _) => return,
        }
        self.txn_shard(txn_id)
            .lock()
            .entry(txn_id)
            .or_default()
            .log
            .push(held);
    }

    /// Export effective modes, not acquisition-log modes: batches may be
    /// shared and a later S -> X upgrade does not append another log entry.
    pub(crate) fn prepared_rows_for_txn(&self, txn_id: u64) -> Vec<(u64, RowId, LockMode)> {
        let log = self
            .txn_shard(txn_id)
            .lock()
            .get(&txn_id)
            .map(|entry| entry.log.clone())
            .unwrap_or_default();
        let mut seen = HashSet::new();
        let mut rows = Vec::new();
        let mut visit = |table_id, row_id| {
            if !seen.insert((table_id, row_id)) {
                return;
            }
            let shard = self.row_shard((table_id, row_id)).lock();
            if let Some(state) = shard
                .locks
                .get(&(table_id, row_id))
                .filter(|s| s.holds(txn_id))
            {
                rows.push((table_id, row_id, state.mode));
            }
        };
        for held in log {
            match held {
                HeldLock::Row(table, row) | HeldLock::RowMode(table, row, _) => visit(table, row),
                HeldLock::Rows(table, rows) => {
                    for row in rows {
                        visit(table, row);
                    }
                }
                _ => {}
            }
        }
        rows
    }

    pub(crate) fn holds_row(&self, txn_id: u64, table_id: u64, row_id: RowId) -> bool {
        self.row_shard((table_id, row_id))
            .lock()
            .locks
            .get(&(table_id, row_id))
            .is_some_and(|state| state.holds(txn_id))
    }

    /// Release an exact row lock only when it is the most recent lock logged
    /// by this transaction. Point DML uses this after a lock wait reveals that
    /// the candidate disappeared or no longer matches; no mutation occurred,
    /// so retaining the newly acquired lock until transaction end would only
    /// convoy the next waiter. A lock inherited from an earlier statement is
    /// never at risk because callers first record `holds_row`.
    pub(crate) fn release_last_exact_row(
        &self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
    ) -> bool {
        let popped = {
            let mut map = self.txn_shard(txn_id).lock();
            let Some(entry) = map.get_mut(&txn_id) else {
                return false;
            };
            let exact = matches!(
                entry.log.last(),
                Some(HeldLock::Row(t, r) | HeldLock::RowMode(t, r, _))
                    if *t == table_id && *r == row_id
            );
            exact.then(|| entry.log.pop()).flatten()
        };
        if popped.is_none() {
            return false;
        }
        self.release_row_owner(table_id, row_id, txn_id);
        true
    }
}

/// Parsed `MYDB_ROW_LOCK_SHARDS` shard count (power-of-two, >=1). 0/unset/"off"
/// disables sharding entirely (legacy path). Default when enabled-without-count
/// is `ROW_LOCK_SHARD_DEFAULT`.
fn row_lock_shard_count() -> usize {
    let raw = match std::env::var("MYDB_ROW_LOCK_SHARDS") {
        Ok(v) => v,
        Err(_) => return ROW_LOCK_SHARD_DEFAULT,
    };
    let n: usize = raw.trim().parse().unwrap_or(ROW_LOCK_SHARD_DEFAULT);
    let n = n.max(1);
    // round up to power of two
    n.next_power_of_two()
}

impl Database {
    pub(crate) fn txn_holds_row_lock(&self, txn_id: u64, table_id: u64, row_id: RowId) -> bool {
        if row_lock_sharding_enabled() {
            self.row_locks_sharded
                .holds_row(txn_id, table_id, row_id)
        } else {
            self.lock_manager_read()
                .row_lock_held_by_txn(txn_id, table_id, row_id)
        }
    }

    pub(crate) fn release_new_point_row_lock(
        &self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
    ) {
        let released = if row_lock_sharding_enabled() {
            self.row_locks_sharded
                .release_last_exact_row(txn_id, table_id, row_id)
        } else {
            self.lock_manager()
                .release_last_exact_row(txn_id, table_id, row_id)
        };
        if released {
            self.notify_lock_waiters_for_rows(std::iter::once((table_id, row_id)));
        }
    }

    /// Try one row-lock acquire without creating waiter state. This is the
    /// scheduling probe used while synchronous execution is still on an async
    /// runtime worker. On conflict the same statement is replayed on the
    /// blocking pool and follows the full FIFO/deadlock-detection path there.
    pub(crate) fn try_row_lock_without_wait(
        &self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
        mode: LockMode,
    ) -> Result<bool, QueryError> {
        if row_lock_sharding_enabled() {
            return match self.row_locks_sharded.try_acquire_without_wait(
                txn_id, table_id, row_id, mode, true,
            ) {
                RowAcquireOutcome::Acquired { cleared_wait } => {
                    debug_assert!(!cleared_wait, "an immediate probe must not own waiter state");
                    Ok(true)
                }
                RowAcquireOutcome::Conflict { .. } => Ok(false),
            };
        }

        self.lock_manager()
            .try_acquire_row_lock_without_wait(txn_id, table_id, row_id, mode)
    }

    /// Acquire `mode` on `(table_id,row_id)` via the sharded store, performing
    /// the (rare) global deadlock-graph interaction. Non-blocking: returns
    /// `Ok(())` on grant, or 1205 (pending) / 1213 (deadlock) on conflict —
    /// exactly like the legacy `LockManager::acquire_row_lock_with_mode`, so
    /// the blocking retry loops are unchanged. `record_log=false` is the
    /// check-only probe (`insert_if_free=false`).
    pub(crate) fn row_lock_acquire_sharded(
        &self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
        mode: LockMode,
        record_log: bool,
    ) -> Result<(), QueryError> {
        match self
            .row_locks_sharded
            .try_acquire(txn_id, table_id, row_id, mode, record_log)
        {
            RowAcquireOutcome::Acquired { cleared_wait } => {
                if cleared_wait {
                    // This grant resolved a prior wait — drop the global edge.
                    self.lock_manager().clear_wait(txn_id);
                }
                Ok(())
            }
            RowAcquireOutcome::Conflict { owners } => self
                .lock_manager()
                .record_row_lock_wait(txn_id, owners, table_id, row_id, mode),
        }
    }

    /// Count the row locks currently held by `txn_id`, reading whichever store
    /// is active. Test/diagnostic helper (the row lock lives in the sharded
    /// store under `MYDB_ROW_LOCK_SHARDS`, in `lock_manager` otherwise).
    #[cfg(test)]
    pub(crate) fn count_row_locks_held_by(&self, txn_id: u64) -> usize {
        if row_lock_sharding_enabled() {
            self.row_locks_sharded
                .snapshot_locks()
                .iter()
                .filter(|(_, _, owners)| owners.contains(&txn_id))
                .count()
        } else {
            self.lock_manager_read()
                .locks
                .values()
                .filter(|s| s.holds(txn_id))
                .count()
        }
    }

    /// Non-blocking exclusive row lock (`insert_if_free=true`) — branches on
    /// the sharding flag. Mirrors `LockManager::lock_row`.
    pub(crate) fn lock_row_nonblocking(
        &self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
    ) -> Result<(), QueryError> {
        if self.insert_locks_covered_by_table(txn_id, table_id) {
            return Ok(());
        }
        if row_lock_sharding_enabled() {
            self.row_lock_acquire_sharded(txn_id, table_id, row_id, LockMode::Exclusive, true)
        } else {
            self.lock_manager().lock_row(txn_id, table_id, row_id)
        }
    }

    /// Non-blocking exclusive row-lock *probe* (`insert_if_free=false`) — used
    /// to enqueue on the row's wait list for deadlock detection without taking
    /// the lock. Mirrors `LockManager::check_row_lock`.
    pub(crate) fn check_row_lock_nonblocking(
        &self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
    ) -> Result<(), QueryError> {
        if row_lock_sharding_enabled() {
            self.row_lock_acquire_sharded(txn_id, table_id, row_id, LockMode::Exclusive, false)
        } else {
            self.lock_manager().check_row_lock(txn_id, table_id, row_id)
        }
    }

    /// Non-blocking exclusive batch lock for caller-owned / freshly-inserted
    /// rows (no contention expected). Mirrors `LockManager::lock_rows`: a
    /// conflict propagates as 1205. Branches on the sharding flag.
    pub(crate) fn lock_rows_nonblocking(
        &self,
        txn_id: u64,
        table_id: u64,
        row_ids: impl IntoIterator<Item = RowId>,
    ) -> Result<(), QueryError> {
        if self.insert_locks_covered_by_table(txn_id, table_id) {
            return Ok(());
        }
        if !row_lock_sharding_enabled() {
            return self.lock_manager().lock_rows(txn_id, table_id, row_ids);
        }
        let rows: Vec<RowId> = row_ids.into_iter().collect();
        let deferred = self.row_lock_batch_sharded(txn_id, table_id, &rows, LockMode::Exclusive)?;
        for row_id in deferred {
            self.row_lock_acquire_sharded(txn_id, table_id, row_id, LockMode::Exclusive, true)?;
        }
        Ok(())
    }

    /// Sharded batch fast path for `lock_rows_blocking_mode`: returns the rows
    /// that must fall through to per-row blocking.
    pub(crate) fn row_lock_batch_sharded(
        &self,
        txn_id: u64,
        table_id: u64,
        row_ids: &[RowId],
        mode: LockMode,
    ) -> Result<Vec<RowId>, QueryError> {
        // Honour MYDB_MAX_LOCKS if set (default 0 = unlimited). Count row locks
        // (sharded) plus unique-key locks (still global) to match the legacy
        // `self.locks.len() + self.unique_locks.len()` semantics. Release
        // builds cache the parse once per process — the env cannot change
        // under a running server and the `getenv` lock showed up on TPC-C
        // profiles at this per-DML-batch call rate. Test builds keep the
        // fresh read through the env shim so `util::env::with_vars`
        // scoped overrides are honoured.
        #[cfg(not(test))]
        let max = {
            static MAX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
            *MAX.get_or_init(|| {
                crate::util::env::var("MYDB_MAX_LOCKS")
                    .ok()
                    .and_then(|v| v.trim().parse::<usize>().ok())
                    .unwrap_or(0)
            })
        };
        #[cfg(test)]
        let max = crate::util::env::var("MYDB_MAX_LOCKS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(0);
        if max > 0 {
            let current = self
                .row_locks_sharded
                .approx_lock_count()
                .saturating_add(self.lock_manager_read().unique_key_lock_count());
            if current.saturating_add(row_ids.len()) > max {
                return Err(error(crate::error::catalog::ER_LOCK_TABLE_FULL,
                    "The total number of locks exceeds the lock table size",
                ));
            }
        }
        Ok(self
            .row_locks_sharded
            .lock_rows_batch_nonblocking(txn_id, table_id, row_ids, mode))
    }
}

/// Whether the sharded row-lock path is enabled. Default ON: high-concurrency
/// OLTP workloads take millions of row locks per minute, and keeping all row
/// locks behind the legacy single latch turns otherwise independent rows into
/// a shared serialization point. `MYDB_ROW_LOCK_SHARDS=<n>` (n>=1) tunes the
/// shard count; `0`/`off`/`false`/`no` force the legacy single-latch path.
pub(crate) fn row_lock_sharding_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("MYDB_ROW_LOCK_SHARDS") {
        Ok(v) => {
            let t = v.trim().to_ascii_lowercase();
            !(t.is_empty() || t == "0" || t == "off" || t == "false" || t == "no")
        }
        Err(_) => true,
    })
}

#[cfg(test)]
mod audit_reinstate_tests {
    use super::*;
    #[test]
    fn transaction_audit_prepared_rows_preserve_batch_shared_and_upgraded_modes() {
        let locks = ShardedRowLocks::new();
        let first = RowId::new(1, 1);
        let second = RowId::new(1, 2);
        assert!(locks
            .lock_rows_batch_nonblocking(1, 9, &[first, second], LockMode::Shared)
            .is_empty());
        assert_eq!(locks.prepared_rows_for_txn(1).len(), 2);
        assert!(locks
            .prepared_rows_for_txn(1)
            .iter()
            .all(|r| r.2 == LockMode::Shared));
        assert!(matches!(
            locks.try_acquire_without_wait(1, 9, first, LockMode::Exclusive, true),
            RowAcquireOutcome::Acquired { .. }
        ));
        let exported = locks.prepared_rows_for_txn(1);
        assert!(exported.contains(&(9, first, LockMode::Exclusive)));
        assert!(exported.contains(&(9, second, LockMode::Shared)));
    }

    #[test]
    fn waiter_that_moves_on_leaves_the_row_queue_it_was_waiting_in() {
        // Txn 2 queues behind txn 1 on `busy`, then gets other rows through
        // the batch path and finishes. Its stale queue entry must not stay at
        // the front of `busy`'s FIFO and block txn 3 after txn 1 is gone.
        let locks = ShardedRowLocks::new();
        let busy = RowId::new(1, 1);
        let other = RowId::new(1, 2);
        assert!(matches!(
            locks.try_acquire(1, 9, busy, LockMode::Exclusive, true),
            RowAcquireOutcome::Acquired { .. }
        ));
        assert!(matches!(
            locks.try_acquire(2, 9, busy, LockMode::Exclusive, true),
            RowAcquireOutcome::Conflict { .. }
        ));
        assert!(locks
            .lock_rows_batch_nonblocking(2, 9, &[other], LockMode::Exclusive)
            .is_empty());
        locks.release_txn(2);
        locks.release_txn(1);
        assert!(matches!(
            locks.try_acquire(3, 9, busy, LockMode::Exclusive, true),
            RowAcquireOutcome::Acquired { .. }
        ));
    }

    #[test]
    fn audit_reinstate_retains_all_shared_owners() {
        let locks = ShardedRowLocks::new();
        let row = RowId::new(1, 1);
        for txn in [1, 2] {
            locks.reinstate(txn, HeldLock::RowMode(9, row, LockMode::Shared));
        }
        let snapshot = locks.snapshot_locks();
        assert_eq!(snapshot.len(), 1);
        assert!(snapshot[0].2.contains(&1));
        assert!(snapshot[0].2.contains(&2));
        assert!(matches!(locks.try_acquire_without_wait(3, 9, row, LockMode::Exclusive, true), RowAcquireOutcome::Conflict { .. }));
    }
}
