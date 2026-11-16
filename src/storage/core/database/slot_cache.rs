// Sharded lock-free cache mirroring `TransactionManager::slots` for the
// visibility-check hot path. The per-row visibility probe in
// `Database::row_version_visible` used to bottleneck on `txns.read()`
// because every reader fights the same coarse `RwLock<TransactionManager>`
// against begin/commit/rollback writers and against row-lock acquisitions
// (sysbench oltp_read_write samples show ~30% of SELECT scan CPU stuck in
// `parking_lot::raw_rwlock::lock_shared_slow`). This structure lets the
// reader load the slot's terminal state with a single `AtomicU64::load`
// after a brief shard read-lock.
//
// Bit layout of the packed `AtomicU64`:
//   bits 0..3  : status discriminant (Active=0, Prepared=1, Committed=2,
//                RolledBack=3)
//   bits 4..63 : commit SCN (60 bits — far above the 2^52 SCN budget of
//                any realistic workload)
//
// `parking_lot::RwLock<HashMap<u64, AtomicU64>>` per shard keeps per-shard
// contention at ~1/SHARDS of the previous central lock while avoiding any
// extra crate dependency. Lookups load the atomic while holding the shard's
// read guard; this also avoids an Arc refcount increment/decrement per MVCC
// visibility check. Updates only need the same shared guard because they
// mutate the atomic, while insert/remove still take the exclusive guard.

#[cfg(not(test))]
const SLOT_CACHE_SHARDS: usize = 64;
#[cfg(test)]
const SLOT_CACHE_SHARDS: usize = 4;
const SLOT_CACHE_SHARD_MASK: u64 = (SLOT_CACHE_SHARDS as u64) - 1;

const SLOT_STATUS_ACTIVE: u64 = 0;
const SLOT_STATUS_PREPARED: u64 = 1;
const SLOT_STATUS_COMMITTED: u64 = 2;
const SLOT_STATUS_ROLLED_BACK: u64 = 3;

#[inline]
fn pack_slot_state(status: TxnStatus, commit_scn: u64) -> u64 {
    debug_assert!(commit_scn <= (u64::MAX >> 4));
    let s = match status {
        TxnStatus::Active => SLOT_STATUS_ACTIVE,
        TxnStatus::Prepared => SLOT_STATUS_PREPARED,
        TxnStatus::Committed => SLOT_STATUS_COMMITTED,
        TxnStatus::RolledBack => SLOT_STATUS_ROLLED_BACK,
    };
    (s & 0xF) | (commit_scn << 4)
}

#[inline]
fn unpack_slot_state(packed: u64) -> (TxnStatus, u64) {
    let status = match packed & 0xF {
        SLOT_STATUS_ACTIVE => TxnStatus::Active,
        SLOT_STATUS_PREPARED => TxnStatus::Prepared,
        SLOT_STATUS_COMMITTED => TxnStatus::Committed,
        SLOT_STATUS_ROLLED_BACK => TxnStatus::RolledBack,
        _ => TxnStatus::Active,
    };
    (status, packed >> 4)
}

/// P0-3 cache-line - each shard is ~49 B (parking_lot RwLock 1 B +
/// std HashMap header 48 B), so the unpadded layout fits two shards
/// into one 64-byte cache line and four into one 128-byte line.
/// A visibility check on txn_id X read-locks shard X's RwLock; a
/// concurrent commit on txn_id Y bumps the RwLock state of shard Y;
/// when both shards share a cache line the two CPUs ping-pong it.
/// 128-byte alignment gives each shard its own line on both x86 and
/// Apple Silicon.
#[derive(Debug, Default)]
#[repr(align(128))]
pub(crate) struct SlotStatusShard {
    map: parking_lot::RwLock<HashMap<u64, AtomicU64, ahash::RandomState>>,
}

#[derive(Debug)]
pub(crate) struct SlotStatusCache {
    shards: Box<[SlotStatusShard]>,
}

impl SlotStatusCache {
    pub(crate) fn new() -> Self {
        let mut shards = Vec::with_capacity(SLOT_CACHE_SHARDS);
        for _ in 0..SLOT_CACHE_SHARDS {
            shards.push(SlotStatusShard::default());
        }
        Self {
            shards: shards.into_boxed_slice(),
        }
    }

    #[inline]
    fn shard(&self, txn_id: u64) -> &SlotStatusShard {
        // Mix the high bits down so monotonic txn ids land in different
        // shards rather than streaming through a single one.
        let key = txn_id ^ (txn_id >> 33);
        &self.shards[(key & SLOT_CACHE_SHARD_MASK) as usize]
    }

    /// Publish an Active slot for `txn_id`. Replaces any prior entry —
    /// safe because txn ids are monotonic, so a re-insertion only happens
    /// when a slot was GC'd and the id reused (never in normal operation).
    pub(crate) fn insert_active(&self, txn_id: u64) {
        let packed = pack_slot_state(TxnStatus::Active, 0);
        let shard = self.shard(txn_id);
        let mut map = shard.map.write();
        match map.get(&txn_id) {
            Some(existing) => existing.store(packed, Ordering::Release),
            None => {
                map.insert(txn_id, AtomicU64::new(packed));
            }
        }
    }

    /// Atomically transition `txn_id` to `Prepared`. No-op if the slot is
    /// not in the cache (caller falls back to the slow path on lookup).
    pub(crate) fn mark_prepared(&self, txn_id: u64) {
        self.update_status(txn_id, TxnStatus::Prepared, 0);
    }

    /// Atomically transition `txn_id` to `Committed` with the given SCN.
    pub(crate) fn mark_committed(&self, txn_id: u64, commit_scn: u64) {
        self.update_status(txn_id, TxnStatus::Committed, commit_scn);
    }

    /// Atomically transition `txn_id` to `RolledBack`.
    pub(crate) fn mark_rolled_back(&self, txn_id: u64) {
        self.update_status(txn_id, TxnStatus::RolledBack, 0);
    }

    fn update_status(&self, txn_id: u64, status: TxnStatus, commit_scn: u64) {
        // Overflow is an optional-cache miss, never a truncated visibility
        // timestamp. The transaction slot remains the authoritative source.
        if commit_scn > (u64::MAX >> 4) {
            self.remove(txn_id);
            return;
        }
        let packed = pack_slot_state(status, commit_scn);
        let shard = self.shard(txn_id);
        // Read-lock is enough: we only mutate the per-slot AtomicU64, not
        // the map shape. If the entry isn't there yet (slot rebuilt from
        // WAL recovery and never re-published), promote to a write-lock
        // insert so visibility checks see the terminal state immediately.
        {
            let map = shard.map.read();
            if let Some(existing) = map.get(&txn_id) {
                existing.store(packed, Ordering::Release);
                return;
            }
        }
        let mut map = shard.map.write();
        match map.get(&txn_id) {
            Some(existing) => existing.store(packed, Ordering::Release),
            None => {
                map.insert(txn_id, AtomicU64::new(packed));
            }
        }
    }

    /// Drop the slot from the cache (called on GC). Subsequent lookups
    /// return `None`, which the visibility check interprets as "GC'd ⇒
    /// committed and visible to any current snapshot" via
    /// `min_retained_txn_id`.
    pub(crate) fn remove(&self, txn_id: u64) {
        let shard = self.shard(txn_id);
        let mut map = shard.map.write();
        map.remove(&txn_id);
    }

    /// Sharded visibility hot-path probe. Returns `Some` with the current
    /// `(status, commit_scn)` if the slot is present; `None` if the slot was
    /// never published or was GC'd. The atomic is loaded under the shard read
    /// guard, so a concurrent remove cannot invalidate the reference and no
    /// per-probe Arc refcount traffic is needed.
    #[inline]
    pub(crate) fn lookup(&self, txn_id: u64) -> Option<(TxnStatus, u64)> {
        let shard = self.shard(txn_id);
        let map = shard.map.read();
        map.get(&txn_id)
            .map(|atomic| unpack_slot_state(atomic.load(Ordering::Acquire)))
    }

    /// Repopulate the cache from a fully-populated slot map. Called once
    /// at startup after WAL recovery reconstructs `TransactionManager`.
    pub(crate) fn seed_from_slots<I>(&self, iter: I)
    where
        I: IntoIterator<Item = (u64, TxnStatus, u64)>,
    {
        for (txn_id, status, commit_scn) in iter {
            self.update_status(txn_id, status, commit_scn);
        }
    }

}

impl Default for SlotStatusCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod slot_cache_tests {
    use super::*;

    #[test]
    fn lookup_observes_atomic_status_transitions_and_remove() {
        let cache = SlotStatusCache::new();
        assert_eq!(cache.lookup(41), None);

        cache.insert_active(41);
        assert_eq!(cache.lookup(41), Some((TxnStatus::Active, 0)));

        cache.mark_prepared(41);
        assert_eq!(cache.lookup(41), Some((TxnStatus::Prepared, 0)));

        cache.mark_committed(41, 99);
        assert_eq!(cache.lookup(41), Some((TxnStatus::Committed, 99)));

        cache.remove(41);
        assert_eq!(cache.lookup(41), None);
    }
}

#[cfg(test)]
mod cache_audit_tests {
    use super::*;

    #[test]
    fn scn_overflow_falls_back_instead_of_truncating() {
        let cache = SlotStatusCache::new();
        cache.insert_active(7);
        cache.mark_committed(7, (1 << 60) - 1);
        assert_eq!(cache.lookup(7), Some((TxnStatus::Committed, (1 << 60) - 1)));
        cache.mark_committed(7, 1 << 60);
        assert_eq!(cache.lookup(7), None);
        cache.seed_from_slots([(8, TxnStatus::Committed, u64::MAX)]);
        assert_eq!(cache.lookup(8), None);
    }
}
