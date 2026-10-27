use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::{BuildHasher, Hash, Hasher};

/// Hot lock-table maps use ahash instead of the default SipHash —
/// profiling showed SipHash at ~1.7% of TPC-C CPU, dominated by
/// `LockManager::acquire_unique_key_lock` hashing a `(table_id,
/// index_name, IndexKey)` per INSERT/unique-check. DoS-resistant
/// hashing buys nothing here: the keys are internal identifiers, never
/// attacker-chosen strings hashed across a trust boundary.
pub(crate) type FastHashMap<K, V> = HashMap<K, V, ahash::RandomState>;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::types::QueryError;
use crate::wal::{PreparedLockEntry, WalPayload, WalRecordType};

use super::core::{error, RowId};
use super::naming::{current_unix_ms, normalize_name};
use super::schema::{IndexBound, IndexKey};
use super::undo::{UndoCreatorState, UndoRecord};
use crate::util::cache_pad::CachePadded;

/// Internal-only condition used by the inline-DML fast path: a lock
/// acquisition inside a statement running in "nonblocking locks" mode
/// would have parked the OS thread. The session layer catches this identity,
/// rolls the statement back (the normal statement-savepoint error path
/// already did), and re-runs the statement on the blocking pool where
/// parking is safe. Never sent to a client: the only producer is the
/// wait-gate below, and the only consumer converts it into a retry.
static INLINE_LOCK_WOULD_BLOCK_RETRIES: AtomicU64 = AtomicU64::new(0);
static INLINE_LOCK_WOULD_BLOCK_LOG_SUPPRESSED: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
thread_local! {
    static INDUCE_DURABILITY_SYNC_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(crate) fn with_durability_sync_failure<T>(f: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            INDUCE_DURABILITY_SYNC_FAILURE.with(|flag| flag.set(self.0));
        }
    }
    let prior = INDUCE_DURABILITY_SYNC_FAILURE.with(|flag| flag.replace(true));
    let _reset = Reset(prior);
    f()
}

#[cfg(test)]
pub(crate) fn fail_durability_sync_if_induced() -> Result<(), QueryError> {
    if INDUCE_DURABILITY_SYNC_FAILURE.with(std::cell::Cell::get) {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "induced WAL durability failure",
        ));
    }
    Ok(())
}

thread_local! {
    /// When set, the row/unique-key lock wait points return
    /// an internal would-block condition instead of parking the thread. Installed
    /// by the session layer around inline (tokio-worker) execution of
    /// prepared DML inside explicit transactions — a tokio worker must
    /// never park on a lock condvar, or all workers can end up blocked on
    /// locks whose holders need a worker to progress (distributed
    /// deadlock). Thread-local (not task-local) is correct here: the
    /// guarded execution is fully synchronous, and the flag is cleared by
    /// the RAII guard before the closure returns to the async world.
    static LOCK_WAITS_NONBLOCKING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };

    /// Exact row that caused the most recent inline would-block result on
    /// this worker. The session consumes it synchronously before its first
    /// await and subscribes to the matching async release-signal shard.
    static NONBLOCKING_ROW_WAIT_TARGET: std::cell::Cell<Option<NonblockingRowWaitTarget>> = const { std::cell::Cell::new(None) };

    /// Per-statement row-lock wait timeout override, in seconds, installed by
    /// the executor from the session's `innodb_lock_wait_timeout`. `None`
    /// means "use the database-level `config.lock_wait` default". Thread-local
    /// is correct for the same reason as [`LOCK_WAITS_NONBLOCKING`]: a
    /// statement (including its blocking lock wait) runs synchronously on one
    /// thread, and the RAII guard restores the previous value on exit.
    static LOCK_WAIT_TIMEOUT_SECS: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    static METADATA_WAIT_TIMEOUT_SECS: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

pub(crate) struct MetadataWaitTimeoutGuard(Option<u64>);

impl MetadataWaitTimeoutGuard {
    pub(crate) fn new(seconds: Option<u64>) -> Self {
        Self(METADATA_WAIT_TIMEOUT_SECS.with(|cell| cell.replace(seconds)))
    }
}

impl Drop for MetadataWaitTimeoutGuard {
    fn drop(&mut self) {
        METADATA_WAIT_TIMEOUT_SECS.with(|cell| cell.set(self.0));
    }
}

pub(crate) fn metadata_wait_timeout() -> Duration {
    Duration::from_secs(
        METADATA_WAIT_TIMEOUT_SECS
            .with(|cell| cell.get())
            .unwrap_or_else(crate::engine::api::global_lock_wait_timeout),
    )
}

/// RAII guard installing a per-statement row-lock wait timeout (seconds).
/// `None` means "no session override — fall back to the database-level
/// `config.lock_wait` default". Restores the previous value on drop, so
/// nesting (e.g. stored routines) is safe.
pub struct LockWaitTimeoutGuard(Option<u64>);

impl LockWaitTimeoutGuard {
    pub fn new(timeout_secs: Option<u64>) -> Self {
        let prev = LOCK_WAIT_TIMEOUT_SECS.with(|cell| cell.replace(timeout_secs));
        LockWaitTimeoutGuard(prev)
    }
}

impl Drop for LockWaitTimeoutGuard {
    fn drop(&mut self) {
        LOCK_WAIT_TIMEOUT_SECS.with(|cell| cell.set(self.0));
    }
}

/// The current thread's row-lock wait timeout override, if the executor
/// installed one for this statement.
pub(crate) fn lock_wait_timeout_override() -> Option<Duration> {
    LOCK_WAIT_TIMEOUT_SECS
        .with(|cell| cell.get())
        .map(Duration::from_secs)
}

/// Run `f` with lock waits in nonblocking mode (see
/// [`LOCK_WAITS_NONBLOCKING`]). Restores the previous mode on exit, so
/// nesting is safe.
pub fn with_nonblocking_lock_waits<R>(f: impl FnOnce() -> R) -> R {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            LOCK_WAITS_NONBLOCKING.with(|cell| cell.set(self.0));
        }
    }
    NONBLOCKING_ROW_WAIT_TARGET.with(|target| target.set(None));
    let prev = LOCK_WAITS_NONBLOCKING.with(|cell| cell.replace(true));
    let _reset = Reset(prev);
    f()
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct NonblockingRowWaitTarget {
    pub(crate) table_id: u64,
    pub(crate) row_id: RowId,
    pub(crate) observed_generation: u64,
}

pub(crate) fn record_nonblocking_row_wait_target(
    table_id: u64,
    row_id: RowId,
    observed_generation: u64,
) {
    NONBLOCKING_ROW_WAIT_TARGET.with(|target| {
        target.set(Some(NonblockingRowWaitTarget {
            table_id,
            row_id,
            observed_generation,
        }))
    });
}

pub(crate) fn take_nonblocking_row_wait_target() -> Option<NonblockingRowWaitTarget> {
    NONBLOCKING_ROW_WAIT_TARGET.with(std::cell::Cell::take)
}

/// True when the current thread runs a statement whose lock waits must
/// not park (inline execution on a tokio worker).
pub(crate) fn lock_waits_nonblocking() -> bool {
    LOCK_WAITS_NONBLOCKING.with(|cell| cell.get())
}

/// The error the wait gate returns in nonblocking mode.
pub(crate) fn lock_would_block_error() -> QueryError {
    QueryError::internal_control(
        crate::error::InternalErrorKind::LockWouldBlock,
        crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
        "lock wait would block inline execution (internal, retried on the blocking pool)",
    )
}

/// True iff `err` is the internal would-block marker.
pub fn is_lock_would_block_error(err: &QueryError) -> bool {
    err.is_internal(crate::error::InternalErrorKind::LockWouldBlock)
}

pub fn record_inline_lock_would_block_retry() {
    INLINE_LOCK_WOULD_BLOCK_RETRIES.fetch_add(1, Ordering::Relaxed);
}

pub fn record_inline_lock_would_block_log_suppressed() {
    INLINE_LOCK_WOULD_BLOCK_LOG_SUPPRESSED.fetch_add(1, Ordering::Relaxed);
}

pub fn inline_lock_would_block_retry_count() -> u64 {
    INLINE_LOCK_WOULD_BLOCK_RETRIES.load(Ordering::Relaxed)
}

pub fn inline_lock_would_block_log_suppressed_count() -> u64 {
    INLINE_LOCK_WOULD_BLOCK_LOG_SUPPRESSED.load(Ordering::Relaxed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// READ UNCOMMITTED — plain consistent reads may observe the newest
    /// heap version even when another transaction has not committed it.
    ReadUncommitted,
    ReadCommitted,
    /// REPEATABLE READ — MySQL's default. Plain (consistent) reads within a
    /// transaction observe a single snapshot pinned at the first consistent
    /// read, so repeated SELECTs are stable across intervening commits.
    /// Current reads (UPDATE / DELETE / SELECT ... FOR UPDATE) still read the
    /// latest committed version and lock. Gap / next-key locking (full
    /// phantom prevention on range scans) is a later phase.
    RepeatableRead,
    /// SERIALIZABLE — implemented by promoting transactional plain SELECTs
    /// to table-level shared locking reads in the executor. No next-key/gap
    /// locks are modeled.
    Serializable,
}

impl IsolationLevel {
    pub fn as_mysql_name(self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "READ-UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ-COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE-READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
        }
    }

    pub fn parse_mysql_name(value: &str) -> Option<Self> {
        match value.trim().replace('-', " ").to_ascii_uppercase().as_str() {
            "READ UNCOMMITTED" => Some(IsolationLevel::ReadUncommitted),
            "READ COMMITTED" => Some(IsolationLevel::ReadCommitted),
            "REPEATABLE READ" => Some(IsolationLevel::RepeatableRead),
            "SERIALIZABLE" => Some(IsolationLevel::Serializable),
            _ => None,
        }
    }

    pub fn uses_repeatable_snapshot(self) -> bool {
        matches!(self, IsolationLevel::RepeatableRead)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TxnView {
    pub txn_id: u64,
    pub snapshot_scn: u64,
    pub isolation: IsolationLevel,
    pub read_only: bool,
    pub dirty_read: bool,
    /// Historical snapshots have stricter visibility rules than ordinary
    /// statement snapshots: missing txn slots must consult the durable commit
    /// ledger and current-snapshot-only page shortcuts are disabled.
    pub historical: bool,
    /// Availability baseline installed when temporal retention is enabled.
    /// Missing ledger entries below `historical_baseline_txn_id` are known to
    /// have committed no later than `historical_baseline_scn`.
    pub historical_baseline_scn: u64,
    pub historical_baseline_txn_id: u64,
    /// Owning session's connection id (0 = no session context). Retained in
    /// the DML view for protocol/session accounting; AUTO_INCREMENT no longer
    /// keeps connection-owned reservation tails.
    pub connection_id: u32,
    /// True when the statement/transaction context already owns an active
    /// snapshot registration for `snapshot_scn`. Nested executor entries reuse
    /// that registration instead of inserting the same SCN a second time.
    pub(crate) snapshot_registered: bool,
}

impl TxnView {
    pub fn read_only(snapshot_scn: u64, isolation: IsolationLevel) -> Self {
        Self {
            txn_id: 0,
            snapshot_scn,
            isolation,
            read_only: true,
            dirty_read: matches!(isolation, IsolationLevel::ReadUncommitted),
            historical: false,
            historical_baseline_scn: 0,
            historical_baseline_txn_id: 0,
            connection_id: 0,
            snapshot_registered: false,
        }
    }

    pub fn historical(snapshot_scn: u64) -> Self {
        Self {
            txn_id: 0,
            snapshot_scn,
            isolation: IsolationLevel::ReadCommitted,
            read_only: true,
            dirty_read: false,
            historical: true,
            historical_baseline_scn: 0,
            historical_baseline_txn_id: 0,
            connection_id: 0,
            snapshot_registered: false,
        }
    }

    pub(crate) fn with_historical_baseline(
        mut self,
        baseline_scn: u64,
        first_tracked_txn_id: u64,
    ) -> Self {
        self.historical_baseline_scn = baseline_scn;
        self.historical_baseline_txn_id = first_tracked_txn_id;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum TxnStatus {
    Active,
    /// XA branch that has successfully completed PREPARE and is awaiting an
    /// out-of-band XA COMMIT / XA ROLLBACK. Semantically still "running" for
    /// MVCC visibility, lock conflict, and GC purposes — it just isn't
    /// owned by any session anymore.
    Prepared,
    Committed,
    RolledBack,
}

impl TxnStatus {
    /// `true` if the slot is still holding locks and undo state — i.e.
    /// `Active` or `Prepared`. Most visibility / GC checks group these
    /// two together because PREPARED writes are not yet final.
    pub(crate) fn is_in_flight(self) -> bool {
        matches!(self, TxnStatus::Active | TxnStatus::Prepared)
    }
}

/// The transaction's private undo-stack head.
///
/// `0` is the `None` sentinel — undo ptrs can never be 0 (persistent ptrs
/// start past the 32-byte segment header, memory-mode ids start at 1).
/// Cloning shares both the published atomic and the per-transaction append
/// latch. A handle obtained under the txns lock therefore stays live after
/// that lock is released. Appends to different transactions remain fully
/// concurrent, while writers sharing one transaction serialize the complete
/// read-prev / persist-record / publish-head sequence so the undo chain cannot
/// fork (parallel INSERT ... SELECT workers are one such caller).
#[derive(Debug, Default)]
struct UndoHeadInner {
    value: AtomicU64,
    reset_generation: AtomicU64,
    append_latch: parking_lot::Mutex<()>,
    index_repair_log_len: AtomicUsize,
}

#[derive(Debug, Default)]
pub(crate) struct UndoHead(Arc<UndoHeadInner>);

impl UndoHead {
    /// Invalidate facts cached before a savepoint restore or undo remap.
    pub(crate) fn reset_generation(&self) -> u64 {
        self.0.reset_generation.load(Ordering::Acquire)
    }

    pub(crate) fn index_repair_log_len(&self) -> usize {
        self.0.index_repair_log_len.load(Ordering::Acquire)
    }

    fn set_index_repair_log_len(&self, len: usize) {
        self.0.index_repair_log_len.store(len, Ordering::Release);
    }

    pub(crate) fn get(&self) -> Option<u64> {
        match self.0.value.load(Ordering::Acquire) {
            0 => None,
            v => Some(v),
        }
    }

    pub(crate) fn set(&self, value: Option<u64>) {
        self.0.reset_generation.fetch_add(1, Ordering::Release);
        self.0.value.store(value.unwrap_or(0), Ordering::Release);
    }

    /// Serialize undo-record linkage for callers that share this transaction.
    /// The guard must cover reading the previous head, persisting/staging the
    /// new record, and publishing the new head.
    pub(crate) fn lock_append(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.0.append_latch.lock()
    }

    /// Publish `new` iff the head is still `prev`. Mirrors the old
    /// "undo head changed while appending undo" guard.
    pub(crate) fn compare_exchange(&self, prev: Option<u64>, new: Option<u64>) -> bool {
        self.0
            .value
            .compare_exchange(
                prev.unwrap_or(0),
                new.unwrap_or(0),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

impl Clone for UndoHead {
    fn clone(&self) -> Self {
        UndoHead(Arc::clone(&self.0))
    }
}

impl From<Option<u64>> for UndoHead {
    fn from(value: Option<u64>) -> Self {
        UndoHead(Arc::new(UndoHeadInner {
            value: AtomicU64::new(value.unwrap_or(0)),
            reset_generation: AtomicU64::new(0),
            append_latch: parking_lot::Mutex::new(()),
            index_repair_log_len: AtomicUsize::new(0),
        }))
    }
}

impl Serialize for UndoHead {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.get().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for UndoHead {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Option::<u64>::deserialize(deserializer).map(UndoHead::from)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TxnSlot {
    pub(crate) txn_id: u64,
    pub(crate) status: TxnStatus,
    pub(crate) start_scn: u64,
    #[serde(default)]
    pub(crate) started_unix_ms: u64,
    pub(crate) commit_scn: Option<u64>,
    pub(crate) undo_head: UndoHead,
    /// Lock-protected writer flag, kept in sync with `active_write_count`.
    /// Set by `mark_writer` BEFORE the transaction's first undo/WAL append
    /// so checkpoint truncation (`min_in_flight_wal_lsn`) can never miss an
    /// in-flight writer whose head publication is still racing.
    #[serde(default)]
    pub(crate) is_writer: bool,
    #[serde(default)]
    pub(crate) read_only: bool,
    #[serde(default = "default_wal_logged")]
    pub(crate) wal_logged: bool,
    /// Set when the slot has successfully completed `XA PREPARE`. Survives
    /// crashes via the WAL `PrepareTxn` record. `None` for ordinary
    /// (non-XA) transactions.
    #[serde(default)]
    pub(crate) xa_xid: Option<crate::xa::Xid>,
    /// Next WAL LSN observed when the transaction began (or the first
    /// record's LSN for slots rebuilt from a WAL scan). Lower bound on
    /// every WAL record this transaction can have written: checkpoint
    /// WAL truncation must retain records at or above the minimum of
    /// this value across in-flight write transactions so a crash can
    /// still undo them. `0` means "unknown" and conservatively pins
    /// the retention floor.
    #[serde(default)]
    pub(crate) begin_lsn: u64,
}

fn default_wal_logged() -> bool {
    true
}

#[derive(Debug)]
pub(crate) struct WalSavepointBoundary {
    /// Upper bound on eager records allocated before the savepoint. For a
    /// buffered txn this may include other transactions' records; it is a
    /// skip-interval boundary, never a transaction-chain backlink.
    pub(crate) eager_lsn: u64,
    pub(crate) prefix_lsn: std::sync::atomic::AtomicU64,
    pub(crate) last_lsn: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Clone)]
pub struct StatementSavepoint {
    pub(crate) wal_boundary: Option<std::sync::Arc<WalSavepointBoundary>>,
    pub(crate) undo_head: Option<u64>,
    pub(crate) lock_log_len: usize,
    /// P3 — sharded row-lock-log watermark. Captured/used only when row-lock
    /// sharding is enabled (rows live in `ShardedRowLocks`, not the legacy
    /// `lock_log`); 0 in the legacy path.
    pub(crate) row_log_len: usize,
    pub(crate) index_repair_log_len: usize,
    pub(crate) binlog_len: usize,
    /// Transaction-local signed row-count deltas at this savepoint. Owned by
    /// `Database` but carried here so statement and named-savepoint rollback
    /// restore CBO population tracking together with the undo chain.
    pub(crate) live_row_deltas: HashMap<u64, i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct IndexRepairKey {
    pub(crate) table_id: u64,
    pub(crate) index_name: String,
    pub(crate) key: IndexKey,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexRepairEntry {
    pub(crate) txn_id: u64,
    pub(crate) row_id: RowId,
    pub(crate) seq: u64,
    /// Writer that created this key: the transaction that retired the row's
    /// previous key in the same single-key index, or 0 when purged. Multi-key
    /// indexes use 0 conservatively and certify candidates against heap MVCC.
    pub(crate) creator: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexRepairLogEntry {
    key: IndexRepairKey,
    seq: u64,
}

const INDEX_REPAIR_SCOPES: usize = 128;

#[derive(Debug, Default)]
struct IndexRepairEpoch {
    version: AtomicU64,
    keys: AtomicUsize,
    /// Advanced before a rolled-back writer's retired keys leave the
    /// registry. Its compensation already restored their physical entries,
    /// so a reader that sees this move across one physical read re-reads
    /// that span: the read may predate the restore while its refresh missed
    /// the removed key.
    rollbacks: AtomicU64,
}

/// Bounded, conservative index-local emptiness proofs. Hash collisions only
/// force an extra repair lookup; they never hide a repair. Both registration
/// and removal advance the version, including empty -> nonempty -> empty.
/// Registry shards mutate these concurrently, so every change is an atomic
/// read-modify-write: the key count moves first, then the version is
/// recomputed from the count it observes.
#[derive(Debug)]
pub(crate) struct IndexRepairEpochs([IndexRepairEpoch; INDEX_REPAIR_SCOPES]);

impl Default for IndexRepairEpochs {
    fn default() -> Self {
        Self(std::array::from_fn(|_| IndexRepairEpoch::default()))
    }
}

impl IndexRepairEpochs {
    fn scope(table_id: u64, index_name: &str) -> usize {
        let mut hash = table_id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        for byte in index_name.bytes() {
            hash = (hash ^ u64::from(byte.to_ascii_lowercase())).wrapping_mul(0x100_0000_01b3);
        }
        hash ^= hash >> 32;
        hash as usize & (INDEX_REPAIR_SCOPES - 1)
    }

    pub(crate) fn version(&self, table_id: u64, index_name: &str) -> &AtomicU64 {
        &self.0[Self::scope(table_id, index_name)].version
    }

    /// Whether the scope of `(table_id, index_name)` holds any retired key.
    /// A colliding index can make this conservatively true, never false
    /// while a key of this index is registered.
    pub(crate) fn has_keys(&self, table_id: u64, index_name: &str) -> bool {
        self.version(table_id, index_name).load(Ordering::Acquire) & 1 != 0
    }

    /// See [`IndexRepairEpoch::rollbacks`].
    pub(crate) fn rollback_epoch(&self, table_id: u64, index_name: &str) -> u64 {
        self.0[Self::scope(table_id, index_name)]
            .rollbacks
            .load(Ordering::Acquire)
    }

    fn note_rollback(&self, key: &IndexRepairKey) {
        self.0[Self::scope(key.table_id, &key.index_name)]
            .rollbacks
            .fetch_add(1, Ordering::AcqRel);
    }

    fn publish(&self, key: &IndexRepairKey, key_delta: i8) {
        let epoch = &self.0[Self::scope(key.table_id, &key.index_name)];
        match key_delta {
            1 => {
                epoch.keys.fetch_add(1, Ordering::AcqRel);
            }
            -1 => {
                let previous = epoch.keys.fetch_sub(1, Ordering::AcqRel);
                assert!(previous > 0, "index repair scope underflow");
            }
            _ => {}
        }
        advance_repair_version(&epoch.version, &epoch.keys);
    }
}

/// Move `version` to a new even generation whose bit 0 says whether `keys`
/// is non-zero. Every count change is followed by its own successful update,
/// which re-reads the count after a failed exchange, so the last published
/// bit always reflects the final count.
fn advance_repair_version(version: &AtomicU64, keys: &AtomicUsize) {
    let mut current = version.load(Ordering::Relaxed);
    loop {
        let nonempty = keys.load(Ordering::Acquire) != 0;
        let next = (current.wrapping_add(2) & !1) | u64::from(nonempty);
        match version.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NamedSavepoint {
    pub(crate) name: String,
    pub(crate) savepoint: StatementSavepoint,
}

const INDEX_REPAIR_ENTRY_SHARDS: usize = 64;
const INDEX_REPAIR_LOG_SHARDS: usize = 64;

type IndexRepairEntryMap = BTreeMap<IndexRepairKey, Vec<IndexRepairEntry>>;
/// Registered retirements of one row in one index, `(seq, writer)` in
/// registration order.
type IndexRepairRowMap = HashMap<(u64, String, RowId), smallvec::SmallVec<[(u64, u64); 2]>>;

/// Old-key visibility repairs. A writer publishes here before removing a
/// physical index entry; slot publication remains separate.
///
/// Every point read, range read and old-key-retiring write of an index with
/// retired keys consults this registry, so it is partitioned instead of
/// guarded by one latch: retired keys live in `entry_shards` chosen by key
/// hash, and each writer's ordered registration log lives in `log_shards`
/// chosen by transaction id. Exact-key readers touch one entry shard; range
/// readers visit every shard under short read leases and merge.
///
/// Lock order: transaction directory -> log shard -> entry shard. Readers
/// never take a log shard, and no path holds two entry shards at once.
#[derive(Debug)]
pub(crate) struct IndexRepairState {
    entry_shards: Box<[CachePadded<parking_lot::RwLock<IndexRepairEntryMap>>]>,
    log_shards: Box<[CachePadded<parking_lot::Mutex<HashMap<u64, Vec<IndexRepairLogEntry>>>>]>,
    /// Retirement chains by row, sharded by row id. A row's writers are
    /// serialized by its row lock, so the chain is in version order and the
    /// previous retirer of a row is the creator of the key retired next.
    row_shards: Box<[CachePadded<parking_lot::Mutex<IndexRepairRowMap>>]>,
    /// Distinct retired keys across all entry shards; drives the global
    /// version's non-empty bit.
    keys: AtomicUsize,
    /// Writers that currently own a non-empty registration log.
    writers: AtomicUsize,
    next_seq: AtomicU64,
}

impl Default for IndexRepairState {
    fn default() -> Self {
        Self {
            entry_shards: (0..INDEX_REPAIR_ENTRY_SHARDS)
                .map(|_| CachePadded::new(parking_lot::RwLock::new(BTreeMap::new())))
                .collect(),
            log_shards: (0..INDEX_REPAIR_LOG_SHARDS)
                .map(|_| CachePadded::new(parking_lot::Mutex::new(HashMap::new())))
                .collect(),
            row_shards: (0..INDEX_REPAIR_ENTRY_SHARDS)
                .map(|_| CachePadded::new(parking_lot::Mutex::new(HashMap::new())))
                .collect(),
            keys: AtomicUsize::new(0),
            writers: AtomicUsize::new(0),
            next_seq: AtomicU64::new(0),
        }
    }
}

impl IndexRepairState {
    fn entry_shard_of(key: &IndexRepairKey) -> usize {
        use std::hash::{BuildHasher, Hash, Hasher};
        static HASHER: std::sync::OnceLock<ahash::RandomState> = std::sync::OnceLock::new();
        let mut hasher = HASHER.get_or_init(ahash::RandomState::new).build_hasher();
        key.hash(&mut hasher);
        hasher.finish() as usize & (INDEX_REPAIR_ENTRY_SHARDS - 1)
    }

    fn entry_shard(&self, key: &IndexRepairKey) -> &parking_lot::RwLock<IndexRepairEntryMap> {
        &self.entry_shards[Self::entry_shard_of(key)]
    }

    fn row_shard(&self, row_id: RowId) -> &parking_lot::Mutex<IndexRepairRowMap> {
        let hash = (u64::from(row_id.page_id) << 16 | u64::from(row_id.slot_id))
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            >> 32;
        &self.row_shards[hash as usize & (INDEX_REPAIR_ENTRY_SHARDS - 1)]
    }

    /// Append `(seq, txn_id)` to the row's retirement chain and return the
    /// previous retirer, i.e. the creator of a single-key row version.
    fn push_row_retirement(
        &self,
        key: &IndexRepairKey,
        row_id: RowId,
        seq: u64,
        txn_id: u64,
    ) -> u64 {
        let mut rows = self.row_shard(row_id).lock();
        let chain = rows
            .entry((key.table_id, key.index_name.clone(), row_id))
            .or_default();
        let creator = chain.last().map_or(0, |&(_, writer)| writer);
        chain.push((seq, txn_id));
        creator
    }

    fn remove_row_retirement(&self, key: &IndexRepairKey, row_id: RowId, seq: u64) {
        let mut rows = self.row_shard(row_id).lock();
        let row_key = (key.table_id, key.index_name.clone(), row_id);
        if let Some(chain) = rows.get_mut(&row_key) {
            chain.retain(|entry| entry.0 != seq);
            if chain.is_empty() {
                rows.remove(&row_key);
            }
        }
    }

    fn log_shard(
        &self,
        txn_id: u64,
    ) -> &parking_lot::Mutex<HashMap<u64, Vec<IndexRepairLogEntry>>> {
        let hash = txn_id.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32;
        &self.log_shards[hash as usize & (INDEX_REPAIR_LOG_SHARDS - 1)]
    }

    /// Whether any writer still owns registrations awaiting purge.
    pub(crate) fn has_writers(&self) -> bool {
        self.writers.load(Ordering::Acquire) != 0
    }

    /// The third tuple field identifies single-key indexes. For FULLTEXT/MVI,
    /// one mutation can retain some keys while retiring others, so the last
    /// row writer does not identify every key's creator. Keep those candidates
    /// conservatively and certify their keys against the MVCC-visible row.
    pub(crate) fn register(
        &self,
        txn_id: u64,
        repairs: impl IntoIterator<Item = (IndexRepairKey, RowId, bool)>,
        head: &UndoHead,
        epochs: &IndexRepairEpochs,
        version: &AtomicU64,
    ) -> Result<(), QueryError> {
        let mut repairs = repairs.into_iter().peekable();
        if repairs.peek().is_none() {
            return Ok(());
        }
        let mut logs = self.log_shard(txn_id).lock();
        let log = logs.entry(txn_id).or_default();
        let was_empty = log.is_empty();
        for (key, row_id, single_key) in repairs {
            let mut shard = self.entry_shard(&key).write();
            let entries = shard.entry(key.clone()).or_default();
            let new_key = entries.is_empty();
            if !log.is_empty()
                && entries
                    .iter()
                    .any(|entry| entry.txn_id == txn_id && entry.row_id == row_id)
            {
                continue;
            }
            let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
            let creator = if single_key {
                self.push_row_retirement(&key, row_id, seq, txn_id)
            } else {
                0
            };
            entries.push(IndexRepairEntry {
                txn_id,
                row_id,
                seq,
                creator,
            });
            if new_key {
                self.keys.fetch_add(1, Ordering::AcqRel);
            }
            // Publish while the shard lease is held so an optimistic reader
            // that misses this entry is guaranteed to see the version move.
            epochs.publish(&key, i8::from(new_key));
            advance_repair_version(version, &self.keys);
            drop(shard);
            log.push(IndexRepairLogEntry { key, seq });
        }
        if was_empty && !log.is_empty() {
            self.writers.fetch_add(1, Ordering::AcqRel);
        }
        head.set_index_repair_log_len(log.len());
        if log.is_empty() {
            logs.remove(&txn_id);
        }
        Ok(())
    }

    /// Drop `txn_id`'s registrations beyond `len`. `rollback` marks a
    /// rolled-back writer (statement, savepoint or transaction rollback);
    /// purge of finished writers passes false.
    pub(crate) fn truncate(
        &self,
        txn_id: u64,
        len: usize,
        head: Option<&UndoHead>,
        epochs: &IndexRepairEpochs,
        version: &AtomicU64,
        rollback: bool,
    ) {
        let mut logs = self.log_shard(txn_id).lock();
        let Some(log) = logs.get_mut(&txn_id) else {
            return;
        };
        let removed = log.split_off(len.min(log.len()));
        if let Some(head) = head {
            head.set_index_repair_log_len(log.len());
        }
        if log.is_empty() {
            logs.remove(&txn_id);
            self.writers.fetch_sub(1, Ordering::AcqRel);
        }
        for entry in removed.into_iter().rev() {
            if rollback {
                epochs.note_rollback(&entry.key);
            }
            let mut shard = self.entry_shard(&entry.key).write();
            let remove_key = if let Some(entries) = shard.get_mut(&entry.key) {
                entries.retain(|repair| {
                    let keep = repair.txn_id != txn_id || repair.seq != entry.seq;
                    if !keep {
                        self.remove_row_retirement(&entry.key, repair.row_id, repair.seq);
                    }
                    keep
                });
                entries.is_empty()
            } else {
                false
            };
            if remove_key {
                shard.remove(&entry.key);
                self.keys.fetch_sub(1, Ordering::AcqRel);
            }
            epochs.publish(&entry.key, if remove_key { -1 } else { 0 });
            advance_repair_version(version, &self.keys);
        }
    }

    /// Clear every repair of finished `writers` (from
    /// `TransactionManager::obsolete_index_repair_writers`). A finished
    /// writer registers nothing more, so this needs only the registry
    /// shards, each held for one key at a time so index readers and
    /// registering writers interleave with a large backlog.
    pub(crate) fn purge_finished(
        &self,
        writers: &[(u64, Option<UndoHead>)],
        epochs: &IndexRepairEpochs,
        version: &AtomicU64,
    ) {
        for (txn_id, head) in writers {
            self.truncate(*txn_id, 0, head.as_ref(), epochs, version, false);
        }
    }

    /// Transaction ids that own registrations.
    pub(crate) fn writer_ids(&self) -> Vec<u64> {
        let mut ids = Vec::new();
        for shard in self.log_shards.iter() {
            ids.extend(shard.lock().keys().copied());
        }
        ids
    }

    /// Retired keys and row ids registered by `txn_id`, in registration order.
    pub(crate) fn writer_entries(&self, txn_id: u64) -> Vec<(IndexRepairKey, RowId)> {
        let logs = self.log_shard(txn_id).lock();
        let Some(log) = logs.get(&txn_id) else {
            return Vec::new();
        };
        let mut batch = Vec::with_capacity(log.len());
        for logged in log {
            let shard = self.entry_shard(&logged.key).read();
            if let Some(entry) = shard
                .get(&logged.key)
                .and_then(|entries| entries.iter().find(|entry| entry.seq == logged.seq))
            {
                batch.push((logged.key.clone(), entry.row_id));
            }
        }
        batch
    }

    /// Retired keys of one table index within `[low, high]` that satisfy
    /// `key_matches`, with their writers, in key order. Resolving each
    /// writer's state is left to the caller so no shard lease is ever held
    /// across the transaction directory.
    pub(crate) fn candidates(
        &self,
        table_id: u64,
        index_name: &str,
        low: Option<&IndexBound>,
        high: Option<&IndexBound>,
        mut key_matches: impl FnMut(&IndexKey) -> bool,
    ) -> Vec<IndexRepairCandidate> {
        let index_name = normalize_name(index_name);
        let start = IndexRepairKey {
            table_id,
            index_name,
            key: low.map_or_else(|| IndexKey::from(Vec::new()), |bound| bound.key.clone()),
        };
        let mut out = Vec::new();
        for shard in self.entry_shards.iter() {
            let shard = shard.read();
            for (key, entries) in shard.range(&start..).take_while(|(key, _)| {
                key.table_id == table_id
                    && key.index_name == start.index_name
                    && super::core::index_key_in_bounds(&key.key, None, high)
            }) {
                if !super::core::index_key_in_bounds(&key.key, low, None) || !key_matches(&key.key)
                {
                    continue;
                }
                out.extend(entries.iter().map(|entry| IndexRepairCandidate {
                    key: key.key.clone(),
                    row_id: entry.row_id,
                    writer: entry.txn_id,
                    creator: entry.creator,
                }));
            }
        }
        if out.len() > 1 {
            out.sort_by(|a, b| a.key.cmp(&b.key));
        }
        out
    }

    /// Retired entries for one exact logical index key.
    pub(crate) fn exact_candidates(
        &self,
        table_id: u64,
        index_name: &str,
        key: &IndexKey,
    ) -> Vec<IndexRepairCandidate> {
        let repair_key = IndexRepairKey {
            table_id,
            index_name: normalize_name(index_name),
            key: key.clone(),
        };
        self.exact_candidates_for(&repair_key, |_| true)
    }

    /// Entries of exactly `repair_key` whose writer passes `include`.
    pub(crate) fn exact_candidates_for(
        &self,
        repair_key: &IndexRepairKey,
        include: impl FnMut(u64) -> bool,
    ) -> Vec<IndexRepairCandidate> {
        Self::collect_exact(&self.entry_shard(repair_key).read(), repair_key, include)
    }

    /// Run `f` while registration of `repair_key` is excluded, then snapshot
    /// that key's entries under the same lease. The key's shard is held
    /// shared, so no writer can retire this key (and then remove its
    /// physical index entry) until both are done.
    pub(crate) fn with_key_registration_excluded<T>(
        &self,
        repair_key: &IndexRepairKey,
        include: impl FnMut(u64) -> bool,
        f: impl FnOnce() -> T,
    ) -> (T, Vec<IndexRepairCandidate>) {
        let shard = self.entry_shard(repair_key).read();
        let out = f();
        let candidates = Self::collect_exact(&shard, repair_key, include);
        (out, candidates)
    }

    fn collect_exact(
        shard: &IndexRepairEntryMap,
        repair_key: &IndexRepairKey,
        mut include: impl FnMut(u64) -> bool,
    ) -> Vec<IndexRepairCandidate> {
        shard
            .get(repair_key)
            .into_iter()
            .flatten()
            .filter(|entry| include(entry.txn_id))
            .map(|entry| IndexRepairCandidate {
                key: repair_key.key.clone(),
                row_id: entry.row_id,
                writer: entry.txn_id,
                creator: entry.creator,
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn entry_count(&self, key: &IndexRepairKey) -> usize {
        self.entry_shard(key).read().get(key).map_or(0, Vec::len)
    }

    #[cfg(test)]
    pub(crate) fn log_len(&self, txn_id: u64) -> usize {
        self.log_shard(txn_id)
            .lock()
            .get(&txn_id)
            .map_or(0, Vec::len)
    }
}

/// One retired index key still registered for visibility repair.
#[derive(Debug, Clone)]
pub(crate) struct IndexRepairCandidate {
    pub(crate) key: IndexKey,
    pub(crate) row_id: RowId,
    pub(crate) writer: u64,
    /// See [`IndexRepairEntry::creator`].
    pub(crate) creator: u64,
}

/// Whether the key a repair entry retired was ever part of the row version
/// `txn` sees: its creator (the previous retirer of the same row, 0 once
/// purged) must be visible. A row moved twice after the snapshot leaves a
/// retired key created by an invisible writer; merging it as well would
/// return the row once per retirement.
pub(crate) fn repair_key_created_visible_to(
    txn: TxnView,
    creator: u64,
    state: Option<(TxnStatus, Option<u64>)>,
    durable_commit_scn: impl FnOnce(u64) -> Option<u64>,
) -> bool {
    if creator == 0 || creator == txn.txn_id {
        return true;
    }
    match state {
        None => durable_commit_scn(creator).is_none_or(|scn| scn <= txn.snapshot_scn),
        Some((TxnStatus::Active | TxnStatus::Prepared | TxnStatus::RolledBack, _)) => false,
        Some((TxnStatus::Committed, commit_scn)) => {
            commit_scn.is_some_and(|commit_scn| commit_scn <= txn.snapshot_scn)
        }
    }
}

/// Whether `txn` must still merge a retired key written by `writer` into its
/// index reads. `state` is the writer's `(status, commit_scn)`, or `None` once
/// its slot has been collected, in which case only the durable commit ledger
/// can still place the commit after this snapshot. In-flight writers (PREPARED
/// included: it may still commit) keep their repair entries live.
pub(crate) fn repair_entry_live_for(
    txn: TxnView,
    writer: u64,
    state: Option<(TxnStatus, Option<u64>)>,
    durable_commit_scn: impl FnOnce(u64) -> Option<u64>,
) -> bool {
    writer != txn.txn_id
        && match state {
            None => durable_commit_scn(writer).is_some_and(|scn| scn > txn.snapshot_scn),
            Some((TxnStatus::Active | TxnStatus::Prepared, _)) => true,
            Some((TxnStatus::Committed, commit_scn)) => {
                commit_scn.is_some_and(|commit_scn| commit_scn > txn.snapshot_scn)
            }
            Some((TxnStatus::RolledBack, _)) => false,
        }
}

#[derive(Debug, Default)]
pub(crate) struct TransactionManager {
    pub(crate) next_txn_id: u64,
    pub(crate) next_scn: u64,
    /// P0-1 (commit-visibility frontier): the largest SCN `f` such that every
    /// `commit_scn ≤ f` has finished publishing (its slot is already
    /// `Committed`). This is the only value a snapshot may safely use as its
    /// `snapshot_scn`: reading it guarantees no `commit_scn ≤ f` is still
    /// mid-commit, so visibility can never flip under a fixed snapshot.
    /// Snapshots read the lock-free `Database::visible_frontier` mirror of it;
    /// this field is the authoritative copy advanced under the txns write lock.
    pub(crate) commit_frontier: u64,
    /// Commit SCNs that published *out of order* — ahead of
    /// `commit_frontier + 1` — and are buffered until the gap (an earlier,
    /// still-unpublished commit) fills in. Bounded by the out-of-order publish
    /// window (≈ number of concurrent writers).
    pub(crate) published_ahead: std::collections::BTreeSet<u64>,
    pub(crate) active_count: usize,
    pub(crate) active_write_count: usize,
    pub(crate) slots: BTreeMap<u64, TxnSlot>,
    /// Oldest in-flight transaction that is allowed to write. Since ids are
    /// monotonic, advancing this cursor when its owner finishes is amortized
    /// O(total slots) and explicit READ ONLY transactions never pin it.
    pub(crate) oldest_write_capable_txn_id: Option<u64>,
    /// Start key for the next bounded terminal-slot GC pass.
    pub(crate) slot_gc_cursor: u64,
    /// Only this continuously registered prefix may be crossed by slot GC.
    pub(crate) registered_txn_frontier: u64,
    registered_txn_ranges: BTreeMap<u64, u64>,
    pub(crate) index_repairs: Arc<IndexRepairState>,
    /// Versioned emptiness proof for optimistic physical index probes. Bit 0
    /// means repairs exist; the remaining bits change on every mutation, so
    /// empty -> nonempty -> empty cannot masquerade as an unchanged directory.
    /// Writers publish under the transaction latch before changing the index.
    pub(crate) index_repair_version: Arc<AtomicU64>,
    pub(crate) index_repair_epochs: Arc<IndexRepairEpochs>,
    pub(crate) named_savepoints: HashMap<u64, Vec<NamedSavepoint>>,
}

/// Most point-lock sets cover only a few tables. Keep their exact counters
/// inline; larger sets retain hash lookup rather than an unbounded scan.
#[derive(Debug, Default)]
struct UniquePointTables {
    inline: SmallVec<[(u64, usize); 4]>,
    spill: Option<FastHashMap<u64, usize>>,
}

impl UniquePointTables {
    fn contains_key(&self, table: &u64) -> bool {
        self.spill.as_ref().map_or_else(
            || self.inline.iter().any(|(id, _)| id == table),
            |tables| tables.contains_key(table),
        )
    }

    fn increment(&mut self, table: u64) {
        if let Some(tables) = &mut self.spill {
            *tables.entry(table).or_default() += 1;
        } else if let Some((_, count)) = self.inline.iter_mut().find(|(id, _)| *id == table) {
            *count += 1;
        } else if self.inline.len() < 4 {
            self.inline.push((table, 1));
        } else {
            let mut tables: FastHashMap<u64, usize> = self.inline.drain(..).collect();
            tables.insert(table, 1);
            self.spill = Some(tables);
        }
    }

    fn decrement(&mut self, table: u64) {
        if let Some(tables) = &mut self.spill {
            if let Some(count) = tables.get_mut(&table) {
                *count -= 1;
                if *count == 0 {
                    tables.remove(&table);
                }
            }
            if tables.is_empty() {
                self.spill = None;
            }
        } else if let Some(position) = self.inline.iter().position(|(id, _)| *id == table) {
            self.inline[position].1 -= 1;
            if self.inline[position].1 == 0 {
                self.inline.swap_remove(position);
            }
        }
    }
}

/// TPC-C lock de-contention: the row/unique/table lock subsystem split out
/// of `TransactionManager` so it lives behind its own `Mutex` instead of
/// sharing the single global `txns: RwLock<TransactionManager>` with the
/// slot / SCN / commit-frontier machinery. Under write-heavy load every
/// `begin`/`commit`/visibility operation and every row-lock acquire used to
/// serialise on that one RwLock; moving the (tiny, ~150 ns) lock critical
/// sections onto a separate `Mutex` lets row-lock traffic run concurrently
/// with begin/commit. The lock algorithm itself is unchanged — only where
/// the state lives and which latch guards it.
///
/// Lock ordering: global order is `txns` → `lock_manager`. Methods that need
/// both (commit/rollback/savepoint-restore) take `txns` first (or, preferably,
/// drop it before taking `lock_manager`). The lock subsystem never reaches
/// back into `txns`; the one place that needs a transaction's status
/// (`acquire_unique_key_lock`) takes a lock-free status closure from the
/// caller (`Database::slot_status_cache`).
#[derive(Debug, Default)]
pub(crate) struct LockManager {
    // Row-lock + waiter maps are keyed by `(table_id, RowId)` because
    // the new 8-byte `RowId` no longer carries `table_id`. Two
    // concurrent INSERTs into different tables can otherwise land on
    // colliding (page_id, slot_id, generation) values — keying the
    // map by just `RowId` would treat them as the same lockable row
    // and route them through the wait-for-graph, which under load
    // looks like a spurious deadlock.
    pub(crate) locks: FastHashMap<(u64, RowId), RowLockState>,
    pub(crate) row_waiters: FastHashMap<(u64, RowId), VecDeque<RowWaiter>>,
    pub(crate) unique_locks: UniqueLockMap,
    unique_point_tables: UniquePointTables,
    unique_ranges: UniqueRangeMap,
    unique_range_keys: usize,
    pub(crate) table_locks: FastHashMap<u64, TableLockState>,
    pub(crate) lock_log: FastHashMap<u64, Vec<HeldLock>>,
    pub(crate) waits_for: FastHashMap<u64, LockWait>,
    pub(crate) lock_wait_count: u64,
    pub(crate) lock_wait_time_ms: u64,
    pub(crate) lock_wait_time_max_ms: u64,
    pub(crate) deadlock_count: u64,
    // M2.5 - Lock wait histogram
    pub(crate) lock_wait_histogram: [u64; 8],
    pub(crate) row_x_lock_acquires: u64,
    pub(crate) row_s_lock_acquires: u64,
    pub(crate) lock_upgrades: u64,
    pub(crate) lock_upgrade_failures: u64,
    pub(crate) table_lock_acquires: u64,
    pub(crate) table_lock_waits: u64,
    // M2.3 - Released lock count for chunked-release diagnostics
    pub(crate) chunked_release_chunks: u64,
}

pub(crate) struct LockRelease {
    pub(crate) rows: Vec<(u64, RowId)>,
    pub(crate) affected_waiters: Vec<u64>,
}

/// M2.1 - Lock mode for row locks (Oracle TX enqueue style: shared/exclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockMode {
    Shared,
    Exclusive,
}

impl LockMode {
    #[cfg(test)]
    pub fn is_compatible(self, other: LockMode) -> bool {
        matches!((self, other), (LockMode::Shared, LockMode::Shared))
    }

    pub fn as_str(self) -> &'static str {
        match self {
            LockMode::Shared => "S",
            LockMode::Exclusive => "X",
        }
    }
}

/// Row-lock owner set. Inlines the single-owner case — the norm for
/// exclusive locks — to avoid one heap allocation per locked row. A bulk
/// `UPDATE` / `DELETE` / `SELECT … FOR UPDATE` over N rows would otherwise
/// perform N tiny `Vec` allocations (and N frees at commit). Only shared
/// locks with two or more concurrent holders spill to `Many`.
///
/// Invariant: `Many` always holds ≥ 2 owners. Constructors and `remove`
/// shrink `Many` back to `One` so a single owner never keeps a heap `Vec`.
#[derive(Debug, Clone)]
pub(crate) enum LockOwner {
    One(u64),
    Many(Vec<u64>),
}

impl LockOwner {
    #[inline]
    pub(crate) fn contains(&self, txn_id: u64) -> bool {
        match self {
            LockOwner::One(o) => *o == txn_id,
            LockOwner::Many(v) => v.iter().any(|o| *o == txn_id),
        }
    }

    /// Borrow the owners as a slice for zero-allocation iteration. `One`
    /// borrows the inline value via `slice::from_ref`.
    #[inline]
    pub(crate) fn as_slice(&self) -> &[u64] {
        match self {
            LockOwner::One(o) => std::slice::from_ref(o),
            LockOwner::Many(v) => v.as_slice(),
        }
    }

    /// Add a shared-lock co-owner. `One` promotes to `Many` on the second
    /// distinct owner; duplicates are ignored.
    pub(crate) fn add(&mut self, txn_id: u64) {
        match self {
            LockOwner::One(o) => {
                if *o != txn_id {
                    *self = LockOwner::Many(vec![*o, txn_id]);
                }
            }
            LockOwner::Many(v) => {
                if !v.iter().any(|o| *o == txn_id) {
                    v.push(txn_id);
                }
            }
        }
    }

    /// Remove an owner. Returns `true` when no owners remain (the caller
    /// then drops the whole lock entry). Shrinks `Many` → `One` when exactly
    /// one owner is left so we never retain a heap `Vec` for a single owner.
    pub(crate) fn remove(&mut self, txn_id: u64) -> bool {
        match self {
            LockOwner::One(o) => *o == txn_id,
            LockOwner::Many(v) => {
                v.retain(|o| *o != txn_id);
                match v.len() {
                    0 => true,
                    1 => {
                        *self = LockOwner::One(v[0]);
                        false
                    }
                    _ => false,
                }
            }
        }
    }

    /// Collect every owner other than `txn_id`. An S -> X upgrade must wait
    /// for all other shared holders, and every edge is required for complete
    /// deadlock detection.
    #[inline]
    pub(crate) fn blockers_except(&self, txn_id: u64) -> LockBlockers {
        self.as_slice()
            .iter()
            .copied()
            .filter(|owner| *owner != txn_id)
            .collect()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RowLockState {
    pub(crate) mode: LockMode,
    pub(crate) owners: LockOwner,
}

impl RowLockState {
    pub(crate) fn new(mode: LockMode, owner: u64) -> Self {
        Self {
            mode,
            owners: LockOwner::One(owner),
        }
    }

    pub(crate) fn holds(&self, txn_id: u64) -> bool {
        self.owners.contains(txn_id)
    }

    pub(crate) fn sole_owner(&self, txn_id: u64) -> bool {
        matches!(&self.owners, LockOwner::One(o) if *o == txn_id)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RowWaiter {
    pub(crate) txn_id: u64,
    pub(crate) mode: LockMode,
}

/// M2.2 - Table-level intent locks (Oracle TM enqueue style).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableLockMode {
    IntentShared,         // IS
    IntentExclusive,      // IX
    Shared,               // S
    ShareIntentExclusive, // SIX
    Exclusive,            // X
}

impl TableLockMode {
    /// Least mode holding both permissions. S and IX are incomparable.
    #[inline]
    pub(crate) fn combine(self, requested: Self) -> Self {
        use TableLockMode::*;
        match (self, requested) {
            (Exclusive, _) | (_, Exclusive) => Exclusive,
            (ShareIntentExclusive, _) | (_, ShareIntentExclusive) => ShareIntentExclusive,
            (Shared, IntentExclusive) | (IntentExclusive, Shared) => ShareIntentExclusive,
            (IntentShared, mode) | (mode, IntentShared) => mode,
            (mode, _) => mode,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            TableLockMode::IntentShared => "IS",
            TableLockMode::IntentExclusive => "IX",
            TableLockMode::Shared => "S",
            TableLockMode::ShareIntentExclusive => "SIX",
            TableLockMode::Exclusive => "X",
        }
    }

    /// Compatibility matrix between an existing holder mode and a requested mode.
    pub fn is_compatible(held: TableLockMode, requested: TableLockMode) -> bool {
        use TableLockMode::*;
        match (held, requested) {
            (Exclusive, _) | (_, Exclusive) => false,
            (IntentShared, _) => true,
            (_, IntentShared) => true,
            (IntentExclusive, IntentExclusive) => true,
            (IntentExclusive, _) | (_, IntentExclusive) => false,
            (Shared, Shared) => true,
            (Shared, _) | (_, Shared) => false,
            (ShareIntentExclusive, ShareIntentExclusive) => false,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TableLockState {
    pub(crate) holders: HashMap<u64, TableLockMode>,
    pub(crate) minimum_schema_version: u64,
}

#[derive(Debug, Default)]
pub(crate) struct GroupCommitCoordinator {
    pub(crate) state: parking_lot::Mutex<GroupCommitState>,
    /// B1 fix — a *separate* condvar the leader alone parks on while growing
    /// its batch inside the wait window. Followers arriving during the window
    /// signal `leader_ready` (never `ready`) so the wakeup is guaranteed to
    /// reach the leader. Previously both classes shared `ready`, so a
    /// follower's `notify_one` could land on another parked follower and the
    /// leader slept out its whole window — turning the window into pure dead
    /// time under moderate concurrency.
    pub(crate) leader_ready: parking_lot::Condvar,
    /// Lock-free mirror of `GroupCommitState::durable_lsn`, published (Release)
    /// under the `state` lock every time the watermark advances. A committer
    /// reads it (Acquire) BEFORE touching the `state` mutex: if its LSN is
    /// already durable (a concurrent leader's fsync + high-water release
    /// covered it) it returns without acquiring the mutex or parking. This is
    /// the same durability predicate as the locked `lsn <= state.durable_lsn`
    /// check — only the read is hoisted out of the critical section. Under
    /// `oltp_update_index` most followers are covered by the leader's flushed
    /// high-water by the time they reach here, so this drains the bulk of the
    /// single-`state`-mutex traffic and the condvar park/wake the profile
    /// showed as the top cost.
    pub(crate) durable_lsn_pub: AtomicU64,
    /// Lock-free mirror of `GroupCommitState::syncing`. A committer whose LSN
    /// is not yet durable spins on the durable watermark ONLY while a sync is
    /// in flight (this flag is set) — a leader will publish its durability
    /// shortly, so a bounded busy-wait is cheaper than a futex park/wake round
    /// trip for a ~30µs fsync. When no sync is in flight the committer falls
    /// straight through to lead one itself, so the spin never delays an fsync.
    pub(crate) sync_in_flight: AtomicBool,
}

#[derive(Debug)]
enum DurabilityMessage {
    Sync {
        lsn: u64,
        window: Duration,
        target: u64,
        complete: Arc<DurabilityCompletion>,
    },
    Shutdown,
}

#[derive(Debug, Default)]
struct DurabilityCompletion {
    result: Mutex<Option<Result<(), QueryError>>>,
    ready: Condvar,
}

/// Optional single-owner WAL durability executor. Committers enqueue their
/// target LSN and park on a private one-shot channel; the worker drains all
/// targets that arrived together and performs one fsync to their high-water
/// mark. The direct [`GroupCommitCoordinator`] path is the default: its
/// lock-free durable-watermark fast path avoids the per-commit channel,
/// completion mutex/condvar and completion-pool mutex round trips. The worker
/// remains available through `MYDB_DURABILITY_WORKER=1` for differential
/// testing and workloads where caller election is unusually contentious.
#[derive(Debug)]
pub(crate) struct DurabilityWorker {
    sender: mpsc::Sender<DurabilityMessage>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
    queue_depth: Arc<AtomicU64>,
    queue_depth_peak: Arc<AtomicU64>,
    batches: Arc<AtomicU64>,
    requests: Arc<AtomicU64>,
    batch_max: Arc<AtomicU64>,
    completion_pool: Mutex<Vec<Arc<DurabilityCompletion>>>,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DurabilityWorkerStats {
    pub(crate) queue_depth: u64,
    pub(crate) queue_depth_peak: u64,
    pub(crate) batches: u64,
    pub(crate) requests: u64,
    pub(crate) batch_max: u64,
}

impl DurabilityWorker {
    pub(crate) fn disabled() -> Self {
        let (sender, receiver) = mpsc::channel();
        drop(receiver);
        Self {
            sender,
            handle: Mutex::new(None),
            queue_depth: Arc::new(AtomicU64::new(0)),
            queue_depth_peak: Arc::new(AtomicU64::new(0)),
            batches: Arc::new(AtomicU64::new(0)),
            requests: Arc::new(AtomicU64::new(0)),
            batch_max: Arc::new(AtomicU64::new(0)),
            completion_pool: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn start(log: Arc<crate::wal::LogMutex>) -> Self {
        let (sender, receiver) = mpsc::channel();
        let queue_depth = Arc::new(AtomicU64::new(0));
        let queue_depth_peak = Arc::new(AtomicU64::new(0));
        let batches = Arc::new(AtomicU64::new(0));
        let requests = Arc::new(AtomicU64::new(0));
        let batch_max = Arc::new(AtomicU64::new(0));
        let worker_depth = Arc::clone(&queue_depth);
        let worker_batches = Arc::clone(&batches);
        let worker_requests = Arc::clone(&requests);
        let worker_batch_max = Arc::clone(&batch_max);
        let handle = thread::Builder::new()
            .name("mydb-wal-durable".to_string())
            .spawn(move || {
                let mut shutting_down = false;
                while !shutting_down {
                    let first = match receiver.recv() {
                        Ok(DurabilityMessage::Sync {
                            lsn,
                            window,
                            target,
                            complete,
                        }) => (lsn, window, target.max(1), complete),
                        Ok(DurabilityMessage::Shutdown) | Err(_) => break,
                    };
                    worker_depth.fetch_sub(1, Ordering::AcqRel);
                    let mut target_lsn = first.0;
                    let batch_target = first.2;
                    // The normal target is capped at eight. Keep the whole
                    // batch inline so a strict OLTP workload does not perform
                    // one allocator round trip per fsync batch (well over
                    // 100k allocations/minute at 20 concurrent committers).
                    // Larger operator-configured targets spill through
                    // SmallVec's ordinary, correctness-equivalent fallback.
                    let mut completions: smallvec::SmallVec<[Arc<DurabilityCompletion>; 8]> =
                        smallvec::smallvec![first.3];
                    let deadline = Instant::now() + first.1;
                    loop {
                        if completions.len() as u64 >= batch_target {
                            break;
                        }
                        let next = match receiver.try_recv() {
                            Ok(message) => Some(message),
                            Err(mpsc::TryRecvError::Disconnected) => {
                                shutting_down = true;
                                None
                            }
                            Err(mpsc::TryRecvError::Empty) if Instant::now() < deadline => {
                                match receiver.recv_timeout(
                                    deadline.saturating_duration_since(Instant::now()),
                                ) {
                                    Ok(message) => Some(message),
                                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                                        shutting_down = true;
                                        None
                                    }
                                    Err(mpsc::RecvTimeoutError::Timeout) => None,
                                }
                            }
                            Err(mpsc::TryRecvError::Empty) => None,
                        };
                        match next {
                            Some(DurabilityMessage::Sync { lsn, complete, .. }) => {
                                worker_depth.fetch_sub(1, Ordering::AcqRel);
                                target_lsn = target_lsn.max(lsn);
                                completions.push(complete);
                            }
                            Some(DurabilityMessage::Shutdown) => {
                                shutting_down = true;
                                break;
                            }
                            None => break,
                        }
                    }
                    let batch_size = completions.len() as u64;
                    worker_batches.fetch_add(1, Ordering::Relaxed);
                    worker_requests.fetch_add(batch_size, Ordering::Relaxed);
                    worker_batch_max.fetch_max(batch_size, Ordering::Relaxed);
                    let result = crate::wal::LogManager::sync_durable_to(&log, target_lsn);
                    for complete in completions {
                        if let Ok(mut slot) = complete.result.lock() {
                            *slot = Some(result.clone());
                            complete.ready.notify_one();
                        }
                    }
                }

                // Shutdown is graceful: requests already queued before the
                // shutdown marker are drained in batches. If every sender was
                // dropped unexpectedly, fail any remaining waiters explicitly.
                while let Ok(message) = receiver.try_recv() {
                    if let DurabilityMessage::Sync { complete, .. } = message {
                        worker_depth.fetch_sub(1, Ordering::AcqRel);
                        if let Ok(mut slot) = complete.result.lock() {
                            *slot = Some(Err(error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "durability worker stopped before WAL sync",
                            )));
                            complete.ready.notify_one();
                        }
                    }
                }
            })
            .expect("failed to start WAL durability worker");
        Self {
            sender,
            handle: Mutex::new(Some(handle)),
            queue_depth,
            queue_depth_peak,
            batches,
            requests,
            batch_max,
            completion_pool: Mutex::new(Vec::new()),
        }
    }

    #[inline]
    pub(crate) fn sync_lsn(
        &self,
        lsn: u64,
        window: Duration,
        target: u64,
    ) -> Result<(), QueryError> {
        let complete = self
            .completion_pool
            .lock()
            .ok()
            .and_then(|mut pool| pool.pop())
            .unwrap_or_else(|| Arc::new(DurabilityCompletion::default()));
        if let Ok(mut slot) = complete.result.lock() {
            *slot = None;
        }
        let depth = self.queue_depth.fetch_add(1, Ordering::AcqRel) + 1;
        self.queue_depth_peak.fetch_max(depth, Ordering::Relaxed);
        if self
            .sender
            .send(DurabilityMessage::Sync {
                lsn,
                window,
                target: target.max(1),
                complete: Arc::clone(&complete),
            })
            .is_err()
        {
            self.queue_depth.fetch_sub(1, Ordering::AcqRel);
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "WAL durability worker is stopped",
            ));
        }
        let result = {
            let mut slot = complete.result.lock().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "durability completion lock poisoned",
                )
            })?;
            while slot.is_none() {
                slot = complete.ready.wait(slot).map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "durability completion lock poisoned",
                    )
                })?;
            }
            slot.take().unwrap()
        };
        if let Ok(mut pool) = self.completion_pool.lock() {
            pool.push(complete);
        }
        result
    }

    pub(crate) fn stats(&self) -> DurabilityWorkerStats {
        DurabilityWorkerStats {
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            queue_depth_peak: self.queue_depth_peak.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            requests: self.requests.load(Ordering::Relaxed),
            batch_max: self.batch_max.load(Ordering::Relaxed),
        }
    }
}

impl Drop for DurabilityWorker {
    fn drop(&mut self) {
        let _ = self.sender.send(DurabilityMessage::Shutdown);
        if let Ok(handle) = self.handle.get_mut() {
            if let Some(handle) = handle.take() {
                let _ = handle.join();
            }
        }
    }
}

pub(crate) fn durability_worker_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("MYDB_DURABILITY_WORKER")
            .map(|value| !matches!(value.trim(), "0" | "off" | "false" | "no"))
            .unwrap_or(false)
    })
}

/// Bounded spin budget a not-yet-durable committer burns on the lock-free
/// durable watermark while a sync is in flight, before falling back to the
/// locked park. Default `0` (spin disabled): busy-spinning steals CPU from
/// co-located clients and from the leader/other server threads, which measured
/// as a net throughput *loss* when the sysbench driver shares the host. On a
/// dedicated server (driver on a separate machine, spare cores) a small budget
/// can avoid the futex round trip for a ~30µs fsync — set
/// `MYDB_GROUP_COMMIT_SPIN=<iterations>` to opt in. The lock-free durable-LSN
/// fast path (a single atomic read before the `state` mutex) is always on and
/// independent of this budget.
fn group_commit_spin_budget() -> u32 {
    static BUDGET: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::env::var("MYDB_GROUP_COMMIT_SPIN")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .unwrap_or(0)
    })
}

pub(crate) fn group_commit_target_max() -> u64 {
    static TARGET: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *TARGET.get_or_init(|| {
        std::env::var("MYDB_GROUP_COMMIT_TARGET_MAX")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(8)
    })
}

/// Derive a bounded group-commit target from an active-writer snapshot taken
/// while the current transaction is still active. Freezing the snapshot before
/// publish avoids the target collapsing when several committers all leave the
/// active set before the first one reaches the durability barrier.
pub(crate) fn group_commit_target(active_writers: usize, target_max: u64) -> u64 {
    (active_writers as u64).clamp(1, target_max.max(1))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncOutcome {
    Synced,
    NoOp,
}

#[derive(Debug)]
struct CommitWaitSignal {
    ready: AtomicBool,
    thread: thread::Thread,
}

impl CommitWaitSignal {
    fn notify(&self) {
        self.ready.store(true, Ordering::Release);
        self.thread.unpark();
    }

    fn wait(&self) {
        while !self.ready.load(Ordering::Acquire) {
            thread::park();
        }
    }
}

thread_local! {
    // A synchronous executor can have only one parked commit at a time.
    // Reuse its private wake token instead of allocating one per transaction.
    static COMMIT_WAIT_SIGNAL: Arc<CommitWaitSignal> = Arc::new(CommitWaitSignal {
        ready: AtomicBool::new(false),
        thread: thread::current(),
    });
}

#[derive(Debug)]
struct CommitWaiter {
    lsn: u64,
    pending: bool,
    signal: Arc<CommitWaitSignal>,
}

#[derive(Debug, Default)]
pub(crate) struct GroupCommitState {
    parked: SmallVec<[CommitWaiter; 32]>,
    syncing: bool,
    sync_target_lsn: u64,
    sync_waiters: u64,
    pending_max_lsn: u64,
    pending_waiters: u64,
    durable_lsn: u64,
    batches: u64,
    waiters: u64,
    total_batch_size: u64,
    leader_wait_total_us: u64,
    follower_wait_total_us: u64,
    sync_op_total_us: u64,
    leader_wait_max_us: u64,
    follower_wait_max_us: u64,
    sync_op_max_us: u64,
    /// M5/B8 — Exponential moving average of recent batch size. Used by
    /// the adaptive group-commit window to cap over-large configured
    /// waits when batches are fragile. Updated after every successful
    /// sync via a 0.25-alpha EMA. Zero before the first batch falls
    /// back to the configured window so cold-start behaviour matches
    /// the static path.
    recent_avg_batch_size: f64,
    /// M1/O1 — committers released *solely* by a leader's flushed high-water:
    /// their LSN exceeded the most recent batch's snapshot target (so natural
    /// batching would NOT have covered them) yet was ≤ the flushed high-water.
    /// This isolates the fsyncs the high-water optimization saves *beyond* what
    /// `pending_max_lsn` snapshotting already coalesces. In practice this is
    /// near-zero for autocommit OLTP (the append→register gap is µs vs a
    /// ms-scale fsync, so committers are almost always registered — and thus in
    /// the snapshot — before a leader forms its batch); it grows only when
    /// committers append well ahead of registering.
    high_water_released: u64,
    /// Snapshot target LSN of the most recent completed batch. Used to decide
    /// whether a released waiter was covered by the snapshot (normal batching)
    /// or only by the flushed high-water (`high_water_released`).
    last_batch_sync_lsn: u64,
    /// M4/O2 — batch-size histogram buckets: [1, 2, 3-4, 5-8, 9-16, 17+].
    /// Cheap approximation of the batch-size distribution so operators can
    /// read a p50/p99 without a full histogram.
    batch_size_buckets: [u64; 6],
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct GroupCommitStats {
    pub batches: u64,
    /// Participants in realized batches. Already-durable requests that return
    /// before queue registration do not contribute to this count.
    pub waiters: u64,
    pub total_batch_size: u64,
    pub leader_wait_total_us: u64,
    pub follower_wait_total_us: u64,
    pub sync_op_total_us: u64,
    pub leader_wait_max_us: u64,
    pub follower_wait_max_us: u64,
    pub sync_op_max_us: u64,
    /// Snapshot of the EMA the adaptive window reads. Exposed via
    /// `SHOW ENGINE MYDB STATUS` for the perf-debug case where the
    /// raw `avg_group_commit_size` rounds to 1 but the EMA can be
    /// 0.5 / 0.75 / 1.4 etc. and the adaptive policy depends on the
    /// exact bucket.
    pub recent_avg_batch_size: f64,
    /// M1/O1 — waiters released for free by a leader's flushed high-water
    /// (fsyncs saved). See `GroupCommitState::high_water_released`.
    /// This counts registered waiters, not lock-free already-durable returns.
    pub high_water_released: u64,
    /// M4/O2 — batch-size histogram buckets [1, 2, 3-4, 5-8, 9-16, 17+].
    pub batch_size_buckets: [u64; 6],
}

impl GroupCommitStats {
    pub(crate) fn combined(mut self, other: Self) -> Self {
        let total_batches = self.batches.saturating_add(other.batches);
        if total_batches != 0 {
            self.recent_avg_batch_size = (self.recent_avg_batch_size * self.batches as f64
                + other.recent_avg_batch_size * other.batches as f64)
                / total_batches as f64;
        }
        self.batches = total_batches;
        self.waiters = self.waiters.saturating_add(other.waiters);
        self.total_batch_size = self.total_batch_size.saturating_add(other.total_batch_size);
        self.leader_wait_total_us = self
            .leader_wait_total_us
            .saturating_add(other.leader_wait_total_us);
        self.follower_wait_total_us = self
            .follower_wait_total_us
            .saturating_add(other.follower_wait_total_us);
        self.sync_op_total_us = self.sync_op_total_us.saturating_add(other.sync_op_total_us);
        self.high_water_released = self
            .high_water_released
            .saturating_add(other.high_water_released);
        self.leader_wait_max_us = self.leader_wait_max_us.max(other.leader_wait_max_us);
        self.follower_wait_max_us = self.follower_wait_max_us.max(other.follower_wait_max_us);
        self.sync_op_max_us = self.sync_op_max_us.max(other.sync_op_max_us);
        for (count, other_count) in self
            .batch_size_buckets
            .iter_mut()
            .zip(other.batch_size_buckets)
        {
            *count = count.saturating_add(other_count);
        }
        self
    }
}

/// M5/B8 — Compute the wait-window the group-commit leader should use,
/// given the configured static value and the EMA of recent batch sizes.
///
/// Three-tier behaviour driven by the recent batch-size EMA:
///
/// * `recent_avg <= 0.0` (cold start) → return `configured` unchanged so
///   the very first batch has a chance to grow before the EMA reacts.
/// * `0 < recent_avg < 1.5` (batching is fragile) → return a small
///   latency cap, `min(configured, 100us)`. Collapsing all the way to
///   zero made moderate concurrency self-reinforcing: once a few small
///   batches pulled the EMA below the threshold, leaders stopped giving
///   near-arriving committers even a short chance to join and strict
///   throughput bounced between good and bad runs.
/// * `recent_avg >= 1.5` (batching is forming) → return `configured`
///   so a brief wait can grow the next batch without exceeding the
///   operator's configured latency budget.
///
/// Note about the prior zero-collapse: it helped very low concurrency
/// avoid dead-wait with a 500us default, but it was too coarse for mixed
/// OLTP commit arrivals. A bounded 100us floor preserves the low-latency
/// behaviour while avoiding the cliff.
fn adaptive_window_for_state_enabled(
    configured: Duration,
    recent_avg: f64,
    adaptive_enabled: bool,
) -> Duration {
    if !adaptive_enabled || configured.is_zero() {
        return configured;
    }
    if recent_avg <= 0.0 {
        return configured;
    }
    if recent_avg < 1.5 {
        return configured.min(Duration::from_micros(100));
    }
    configured
}

/// Startup default for the adaptive-window policy. Runtime state lives in
/// `RuntimeStorageConfig::group_commit_adaptive` (settable via
/// `SET GLOBAL mydb_group_commit_adaptive`); this only seeds it from the
/// environment at open time.
pub(crate) fn adaptive_group_commit_env_default() -> bool {
    crate::util::env::var("MYDB_GROUP_COMMIT_ADAPTIVE")
        .map(|value| {
            !matches!(
                value.to_ascii_lowercase().as_str(),
                "0" | "off" | "false" | "no" | ""
            )
        })
        .unwrap_or(false)
}

/// Differential-oracle switch for the M1 high-water release. Default ON.
/// `MYDB_GC_HIGH_WATER=0` (or `off`/`false`/`no`) makes the coordinator advance
/// `durable_lsn` only to the batch snapshot LSN — the pre-M1 behavior — so a
/// benchmark can A/B the fsync savings. Process-level (cached), test/bench aid.
fn high_water_release_enabled() -> bool {
    static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        !matches!(
            crate::util::env::var("MYDB_GC_HIGH_WATER")
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "off" | "false" | "no"
        )
    })
}

fn group_commit_stall_attribution_enabled() -> bool {
    static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        [
            "MYDB_GROUP_COMMIT_STALL_ATTRIBUTION",
            "MYDB_STALL_ATTRIBUTION",
        ]
        .iter()
        .any(|name| {
            crate::util::env::var(name)
                .map(|value| {
                    matches!(
                        value.to_ascii_lowercase().as_str(),
                        "1" | "on" | "true" | "yes"
                    )
                })
                .unwrap_or(false)
        })
    })
}

/// Maximum number of row + unique-key locks the manager will hold across all
/// transactions before refusing further bulk acquisitions. `0` (the default)
/// means unlimited. Read fresh per batch — not cached in a `OnceLock` — so it
/// honours runtime `MYDB_MAX_LOCKS` changes and test overrides; the lookup is
/// one env read per *batch* (never per row), negligible next to locking N rows.
fn max_locks() -> usize {
    crate::util::env::var("MYDB_MAX_LOCKS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

impl GroupCommitCoordinator {
    /// Remove covered followers and hand leadership to one uncovered request.
    /// Signals are delivered after dropping the coordinator mutex, so kernel
    /// wakeups do not extend its critical section. Coverage is the published
    /// durability watermark; an uncovered waiter never receives success.
    fn completed_waiters(
        state: &mut GroupCommitState,
        snapshot_lsn: u64,
    ) -> SmallVec<[Arc<CommitWaitSignal>; 32]> {
        let mut ready = SmallVec::new();
        let mut late_pending = 0u64;
        let mut high_water_released = 0u64;
        state.parked.retain(|waiter| {
            if waiter.lsn > state.durable_lsn {
                return true;
            }
            if waiter.pending {
                late_pending += u64::from(waiter.lsn > snapshot_lsn);
                high_water_released += u64::from(waiter.lsn > state.last_batch_sync_lsn);
            }
            ready.push(Arc::clone(&waiter.signal));
            false
        });
        state.pending_waiters = state.pending_waiters.saturating_sub(late_pending);
        state.high_water_released = state
            .high_water_released
            .saturating_add(high_water_released);
        if !state.syncing && !state.parked.is_empty() {
            // Hand off the next barrier before the completed cohort.
            ready.insert(0, state.parked.remove(0).signal);
        }
        ready
    }

    pub(crate) fn sync_lsn(
        &self,
        lsn: u64,
        window: Duration,
        target_waiters: u64,
        adaptive_enabled: bool,
        log: &Arc<crate::wal::LogMutex>,
    ) -> Result<(), QueryError> {
        self.sync_lsn_with(lsn, window, target_waiters, adaptive_enabled, |sync_lsn| {
            let (high_water, synced) =
                crate::wal::LogManager::sync_durable_high_water_to(log, sync_lsn)?;
            Ok((
                if synced {
                    SyncOutcome::Synced
                } else {
                    SyncOutcome::NoOp
                },
                high_water,
            ))
        })
    }

    /// Closure-based variant used internally and by tests. The closure receives
    /// the LSN to sync and returns `(outcome, durable_high_water)` — the
    /// high-water is the largest LSN the sync actually made durable (≥ the
    /// requested LSN), which the coordinator uses to release natural-batch
    /// stragglers for free. On Err the leader rolls back batch state so
    /// retrying waiters can re-form a batch without losing accounting.
    pub(crate) fn sync_lsn_with<F>(
        &self,
        lsn: u64,
        window: Duration,
        target_waiters: u64,
        adaptive_enabled: bool,
        sync_op: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(u64) -> Result<(SyncOutcome, u64), QueryError>,
    {
        self.sync_lsn_with_attribution(
            lsn,
            window,
            target_waiters,
            adaptive_enabled,
            group_commit_stall_attribution_enabled(),
            sync_op,
        )
    }

    fn sync_lsn_with_attribution<F>(
        &self,
        lsn: u64,
        window: Duration,
        target_waiters: u64,
        adaptive_enabled: bool,
        stall_attribution: bool,
        mut sync_op: F,
    ) -> Result<(), QueryError>
    where
        F: FnMut(u64) -> Result<(SyncOutcome, u64), QueryError>,
    {
        // Lock-free fast path + bounded spin. Most committers under a
        // write-heavy OLTP load are made durable by a concurrent leader's fsync
        // (and its high-water release) before they ever need to lead or park.
        // Serve them without touching the `state` mutex: read the published
        // durable watermark; if already covered, return. Otherwise, while a
        // sync is in flight, spin on the watermark for a bounded budget — a
        // leader will publish our durability within roughly one fsync (~30µs),
        // which is cheaper to spin-wait than to park (a futex round trip plus a
        // re-collision on the single `state` mutex under the leader's
        // `notify_all`). If no sync is in flight, fall through immediately to
        // lead one — never spin away an fsync's worth of latency.
        if lsn <= self.durable_lsn_pub.load(Ordering::Acquire) {
            return Ok(());
        }
        let spin_budget = group_commit_spin_budget();
        if spin_budget > 0 {
            let mut spins = 0u32;
            while self.sync_in_flight.load(Ordering::Acquire) {
                if lsn <= self.durable_lsn_pub.load(Ordering::Acquire) {
                    return Ok(());
                }
                if spins >= spin_budget {
                    break;
                }
                spins += 1;
                std::hint::spin_loop();
            }
        }
        let mut registered = false;
        // B1/O1 — track whether this waiter registered as a *pending* waiter
        // (its LSN was beyond the in-flight sync target, so absent the
        // high-water optimization it would have had to lead its own fsync).
        // Used to attribute a free release to the high-water counter.
        let mut registered_as_pending = false;
        let mut state = self.state.lock();
        loop {
            if lsn <= state.durable_lsn {
                if registered_as_pending {
                    // Released without leading a sync of our own. Attribute the
                    // save to the high-water optimization ONLY if the most
                    // recent snapshot did not already cover us — i.e. our LSN
                    // exceeded that batch's target but was ≤ the flushed
                    // high-water. Otherwise this is ordinary snapshot batching.
                    // Withdraw our pending-waiter contribution so the next
                    // leader doesn't count us as a phantom (we never joined a
                    // real batch). Safe for correctness: durability rides on
                    // `pending_max_lsn` (a max, never decremented), not on this
                    // count, which only feeds batch-size stats and the leader's
                    // target-wait threshold. `saturating_sub` tolerates the
                    // race where a leader already consumed our increment.
                    let snapshot_lsn = state.sync_target_lsn.max(state.last_batch_sync_lsn);
                    if lsn > snapshot_lsn {
                        state.high_water_released = state.high_water_released.saturating_add(1);
                        state.pending_waiters = state.pending_waiters.saturating_sub(1);
                    }
                }
                return Ok(());
            }
            if state.syncing {
                if !registered {
                    if state.sync_target_lsn != 0 && lsn <= state.sync_target_lsn {
                        state.sync_waiters = state.sync_waiters.saturating_add(1);
                    } else {
                        state.pending_max_lsn = state.pending_max_lsn.max(lsn);
                        state.pending_waiters = state.pending_waiters.saturating_add(1);
                        registered_as_pending = true;
                    }
                    registered = true;
                    if state.sync_target_lsn == 0 {
                        // Leader is growing its batch inside the wait window;
                        // wake it (and only it) so it re-checks the target.
                        self.leader_ready.notify_one();
                    }
                }
                let signal = COMMIT_WAIT_SIGNAL.with(Arc::clone);
                signal.ready.store(false, Ordering::Release);
                state.parked.push(CommitWaiter {
                    lsn,
                    pending: registered_as_pending,
                    signal: Arc::clone(&signal),
                });
                let wait_started = stall_attribution.then(Instant::now);
                drop(state);
                signal.wait();
                let covered = lsn <= self.durable_lsn_pub.load(Ordering::Acquire);
                if covered && !stall_attribution {
                    return Ok(());
                }
                state = self.state.lock();
                if let Some(wait_started) = wait_started {
                    let waited_us =
                        wait_started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                    state.follower_wait_total_us =
                        state.follower_wait_total_us.saturating_add(waited_us);
                    state.follower_wait_max_us = state.follower_wait_max_us.max(waited_us);
                }
                if covered {
                    return Ok(());
                }
                continue;
            }

            // A previously parked follower is already included in the pending
            // count. A new leader contributes itself exactly once.
            if !registered {
                state.pending_max_lsn = state.pending_max_lsn.max(lsn);
                state.pending_waiters = state.pending_waiters.saturating_add(1);
            }
            state.syncing = true;
            // Publish leadership so spinning committers (above) know a sync is
            // in flight and a leader will make them durable shortly.
            self.sync_in_flight.store(true, Ordering::Release);
            // M5/B8 — adapt the wait window to recent batch sizes. The
            // first batch (avg=0) uses the configured window unchanged;
            // subsequent low-batch periods cap oversized windows at a
            // short latency budget without exceeding the configured value.
            let effective_window = adaptive_window_for_state_enabled(
                window,
                state.recent_avg_batch_size,
                adaptive_enabled,
            );
            let target_waiters = target_waiters.max(1);
            if !effective_window.is_zero() {
                let deadline = Instant::now() + effective_window;
                while state.pending_waiters < target_waiters {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    let wait_started = stall_attribution.then(Instant::now);
                    let timeout = self.leader_ready.wait_for(&mut state, remaining);
                    if let Some(wait_started) = wait_started {
                        let waited_us =
                            wait_started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                        state.leader_wait_total_us =
                            state.leader_wait_total_us.saturating_add(waited_us);
                        state.leader_wait_max_us = state.leader_wait_max_us.max(waited_us);
                    }
                    if timeout.timed_out() {
                        break;
                    }
                }
            }

            let sync_lsn = state.pending_max_lsn;
            let batch_size = state.pending_waiters.max(1);
            state.sync_target_lsn = sync_lsn;
            state.pending_max_lsn = 0;
            state.pending_waiters = 0;
            drop(state);

            let sync_started = stall_attribution.then(Instant::now);
            let sync_result = sync_op(sync_lsn);
            let sync_us = sync_started
                .map(|started| started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64);

            match sync_result {
                Ok((_outcome, high_water)) => {
                    state = self.state.lock();
                    if let Some(sync_us) = sync_us {
                        state.sync_op_total_us = state.sync_op_total_us.saturating_add(sync_us);
                        state.sync_op_max_us = state.sync_op_max_us.max(sync_us);
                    }
                    let covered_waiters = state.sync_waiters;
                    // M1 — advance to the flushed high-water (≥ sync_lsn), not
                    // just the snapshot target. Any committer that appended
                    // during this sync and is now parked as a pending waiter
                    // with LSN ≤ high_water is released for free on the
                    // `notify_all` below, without paying its own fsync. The
                    // oracle switch collapses to the snapshot LSN for A/B.
                    let released_lsn = if high_water_release_enabled() {
                        high_water.max(sync_lsn)
                    } else {
                        sync_lsn
                    };
                    state.durable_lsn = state.durable_lsn.max(released_lsn);
                    // Publish the advanced watermark BEFORE clearing the
                    // in-flight flag / broadcasting, so spinning committers see
                    // their durability on the lock-free path and skip both the
                    // `state` mutex and the condvar wake.
                    self.durable_lsn_pub
                        .fetch_max(state.durable_lsn, Ordering::Release);
                    // Record this batch's snapshot target so waiters released
                    // afterwards can tell snapshot coverage from high-water-only
                    // coverage (see `high_water_released`).
                    state.last_batch_sync_lsn = state.last_batch_sync_lsn.max(sync_lsn);
                    state.batches = state.batches.saturating_add(1);
                    let realized_batch = batch_size.saturating_add(covered_waiters);
                    state.waiters = state.waiters.saturating_add(realized_batch);
                    state.total_batch_size = state.total_batch_size.saturating_add(realized_batch);
                    let bucket = match realized_batch {
                        0 | 1 => 0,
                        2 => 1,
                        3..=4 => 2,
                        5..=8 => 3,
                        9..=16 => 4,
                        _ => 5,
                    };
                    state.batch_size_buckets[bucket] =
                        state.batch_size_buckets[bucket].saturating_add(1);
                    // EMA update: 25 % weight on the new observation, 75 %
                    // on history. Tuned so a single low-batch run doesn't
                    // collapse the window all the way to its floor.
                    let new_observation = realized_batch as f64;
                    state.recent_avg_batch_size = if state.recent_avg_batch_size <= 0.0 {
                        new_observation
                    } else {
                        0.25 * new_observation + 0.75 * state.recent_avg_batch_size
                    };
                    state.syncing = false;
                    self.sync_in_flight.store(false, Ordering::Release);
                    let ready = Self::completed_waiters(&mut state, sync_lsn);
                    state.sync_target_lsn = 0;
                    state.sync_waiters = 0;
                    drop(state);
                    for signal in ready {
                        signal.notify();
                    }
                    return Ok(());
                }
                Err(err) => {
                    state = self.state.lock();
                    if let Some(sync_us) = sync_us {
                        state.sync_op_total_us = state.sync_op_total_us.saturating_add(sync_us);
                        state.sync_op_max_us = state.sync_op_max_us.max(sync_us);
                    }
                    // On sync failure, fold sync_waiters back into pending so
                    // their contribution to the next batch's target_waiters
                    // threshold is preserved. Without this, late waiters
                    // registered as sync_waiters get dropped from the count
                    // when they re-loop with `registered=true`, biasing the
                    // next leader's window timing.
                    let recovered_waiters = state.sync_waiters;
                    state.pending_max_lsn = state.pending_max_lsn.max(sync_lsn);
                    state.pending_waiters = state
                        .pending_waiters
                        .saturating_add(batch_size)
                        .saturating_add(recovered_waiters);
                    state.sync_waiters = 0;
                    state.syncing = false;
                    self.sync_in_flight.store(false, Ordering::Release);
                    state.sync_target_lsn = 0;
                    let ready = Self::completed_waiters(&mut state, 0);
                    drop(state);
                    for signal in ready {
                        signal.notify();
                    }
                    return Err(err);
                }
            }
        }
    }

    pub(crate) fn stats(&self) -> GroupCommitStats {
        let state = self.state.lock();
        GroupCommitStats {
            batches: state.batches,
            waiters: state.waiters,
            total_batch_size: state.total_batch_size,
            leader_wait_total_us: state.leader_wait_total_us,
            follower_wait_total_us: state.follower_wait_total_us,
            sync_op_total_us: state.sync_op_total_us,
            leader_wait_max_us: state.leader_wait_max_us,
            follower_wait_max_us: state.follower_wait_max_us,
            sync_op_max_us: state.sync_op_max_us,
            recent_avg_batch_size: state.recent_avg_batch_size,
            high_water_released: state.high_water_released,
            batch_size_buckets: state.batch_size_buckets,
        }
    }

    pub(crate) fn mark_durable(&self, lsn: u64) {
        let mut state = self.state.lock();
        state.durable_lsn = state.durable_lsn.max(lsn);
        self.durable_lsn_pub
            .fetch_max(state.durable_lsn, Ordering::Release);
        let snapshot_lsn = state.sync_target_lsn;
        let ready = Self::completed_waiters(&mut state, snapshot_lsn);
        self.leader_ready.notify_all();
        drop(state);
        for signal in ready {
            signal.notify();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct UniqueKeyLock {
    table_id: u64,
    index_name: Arc<str>,
    key: IndexKey,
}

type UniqueLockMap = hashbrown::HashMap<Arc<UniqueKeyLock>, u64, ahash::RandomState>;

type UniqueRangeMap = hashbrown::HashMap<
    (u64, Arc<str>, bool),
    BTreeMap<i128, (Arc<UniqueKeyRangeLock>, u64)>,
    ahash::RandomState,
>;

#[derive(Debug, Clone)]
pub(crate) struct UniqueKeyRangeLock {
    pub(crate) table_id: u64,
    pub(crate) index_name: Arc<str>,
    unsigned: bool,
    first: i128,
    last: i128,
}

impl UniqueKeyRangeLock {
    pub(crate) fn keys(&self) -> impl Iterator<Item = IndexKey> + '_ {
        (self.first..=self.last).map(|value| {
            IndexKey::from(vec![if self.unsigned {
                super::schema::KeyPart::UInt(value as u64)
            } else {
                super::schema::KeyPart::Int(value as i64)
            }])
        })
    }

    pub(crate) fn len(&self) -> usize {
        (self.last - self.first + 1) as usize
    }
}

#[derive(Hash)]
struct UniqueRangeScope<'a>(u64, &'a str, bool);

impl hashbrown::Equivalent<(u64, Arc<str>, bool)> for UniqueRangeScope<'_> {
    fn equivalent(&self, other: &(u64, Arc<str>, bool)) -> bool {
        self.0 == other.0 && self.1 == other.1.as_ref() && self.2 == other.2
    }
}

fn unique_range_owner(
    ranges: &UniqueRangeMap,
    table: u64,
    name: &str,
    key: &IndexKey,
) -> Option<u64> {
    let (unsigned, value) = match key.0.as_slice() {
        [super::schema::KeyPart::Int(value)] => (false, i128::from(*value)),
        [super::schema::KeyPart::UInt(value)] => (true, i128::from(*value)),
        _ => return None,
    };
    let entries = ranges.get(&UniqueRangeScope(table, name, unsigned))?;
    let (_, (range, owner)) = entries.range(..=value).next_back()?;
    (value <= range.last).then_some(*owner)
}

/// Borrow the normalized identity for non-mutating uniqueness probes. Hash
/// fields in the same order as the owned key; no String/IndexKey clone is needed.
#[derive(Hash)]
struct UniqueKeyProbe<'a> {
    table_id: u64,
    index_name: &'a str,
    key: &'a IndexKey,
}

impl hashbrown::Equivalent<Arc<UniqueKeyLock>> for UniqueKeyProbe<'_> {
    fn equivalent(&self, other: &Arc<UniqueKeyLock>) -> bool {
        self.table_id == other.table_id
            && self.index_name == other.index_name.as_ref()
            && self.key == &other.key
    }
}

impl UniqueKeyLock {
    pub(crate) fn new(table_id: u64, index_name: String, key: IndexKey) -> Self {
        Self {
            table_id,
            index_name: index_name.into(),
            key,
        }
    }

    pub(crate) fn key(&self) -> &IndexKey {
        &self.key
    }

    pub(crate) fn table_id(&self) -> u64 {
        self.table_id
    }

    pub(crate) fn index_name(&self) -> &str {
        &self.index_name
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum HeldLock {
    /// `(table_id, row_id)` — row locks are scoped per-table since
    /// the 8-byte `RowId` no longer carries `table_id`.
    Row(u64, RowId), // legacy: implicit Exclusive
    RowMode(u64, RowId, LockMode), // M2.1: row lock with explicit mode
    Rows(u64, Vec<RowId>),
    // Share the key with the lookup table and keep every log entry compact.
    Unique(Arc<UniqueKeyLock>),
    UniqueRange(Arc<UniqueKeyRangeLock>),
    Table(u64, TableLockMode), // M2.2: table-level lock
}

pub(crate) enum LockOwnerCheck {
    Free,
    SelfHolds,
    CanShare,
    Conflict(LockBlockers),
}

pub(crate) type LockBlockers = SmallVec<[u64; 2]>;

const AUTO_INCREMENT_SHARDS: usize = 64;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct AutoIncrementLoggedState {
    pub(crate) version: u64,
    pub(crate) max_value: u64,
    pub(crate) lsn: u64,
}

#[derive(Debug)]
pub(crate) struct AutoIncrementState {
    pub(crate) table_id: u64,
    pub(crate) version: AtomicU64,
    pub(crate) max_value: AtomicU64,
    pub(crate) logged: parking_lot::Mutex<AutoIncrementLoggedState>,
    pub(crate) slow_path: parking_lot::Mutex<()>,
    pub(crate) queued_epoch: AtomicU64,
    pub(crate) retired: AtomicBool,
    _memory: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl AutoIncrementState {
    fn try_new(table_id: u64, version: u64, max_value: u64) -> Result<Self, QueryError> {
        // Charge the Arc allocation, state, and one hash-table entry. The
        // charge lives inside the Arc and is released by the final owner.
        let bytes =
            std::mem::size_of::<Self>().saturating_add(std::mem::size_of::<(u64, Arc<Self>)>() * 2);
        let memory = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
            bytes,
        )?;
        Ok(Self {
            table_id,
            version: AtomicU64::new(version.max(1)),
            max_value: AtomicU64::new(max_value),
            logged: parking_lot::Mutex::new(AutoIncrementLoggedState::default()),
            slow_path: parking_lot::Mutex::new(()),
            queued_epoch: AtomicU64::new(0),
            retired: AtomicBool::new(false),
            _memory: memory,
        })
    }
}

#[derive(Debug)]
pub(crate) struct AutoIncrementManager {
    shards: Box<[parking_lot::RwLock<FastHashMap<u64, Arc<AutoIncrementState>>>]>,
    initializations: parking_lot::Mutex<FastHashMap<u64, Arc<AutoIncrementInitializationLock>>>,
}

#[derive(Debug)]
struct AutoIncrementInitializationLock {
    mutex: parking_lot::Mutex<()>,
    _memory: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

/// A cold initializer owns a table-local gate without holding the registry
/// lock across catalog/index I/O. Waiting callers retain the same gate; the
/// last caller removes it, including on error or panic, so failed opens do
/// not leave an unbounded registry behind.
pub(crate) struct AutoIncrementInitialization<'a> {
    manager: &'a AutoIncrementManager,
    table_id: u64,
    gate: Option<Arc<AutoIncrementInitializationLock>>,
}

impl AutoIncrementInitialization<'_> {
    pub(crate) fn lock(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.gate
            .as_ref()
            .expect("live initialization guard")
            .mutex
            .lock()
    }
}

impl Drop for AutoIncrementInitialization<'_> {
    fn drop(&mut self) {
        let mut initializations = self.manager.initializations.lock();
        // Release our reference under the registry lock. Otherwise two
        // concurrent destructors can both count the other's reference and
        // leave an unused gate in the map after both fields are dropped.
        drop(self.gate.take());
        if initializations
            .get(&self.table_id)
            .is_some_and(|gate| Arc::strong_count(gate) == 1)
        {
            initializations.remove(&self.table_id);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LockWaitResource {
    /// `(table_id, row_id)` — `RowId` alone is not unique across
    /// tables under the new 8-byte layout.
    Row(u64, RowId),
    Unique(UniqueKeyLock),
    Table(u64),
}

#[derive(Debug, Clone)]
pub(crate) struct LockWait {
    /// Every transaction that currently blocks this resource request. Row
    /// locks can have multiple shared owners; retaining only the first owner
    /// makes the wait graph lose real edges and can miss a deadlock.
    pub(crate) owners: LockBlockers,
    pub(crate) resource: LockWaitResource,
    pub(crate) started_unix_ms: u64,
    #[allow(dead_code)]
    pub(crate) mode: Option<LockMode>,
}

impl AutoIncrementManager {
    pub(crate) fn new() -> Self {
        Self {
            shards: (0..AUTO_INCREMENT_SHARDS)
                .map(|_| parking_lot::RwLock::new(FastHashMap::default()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            initializations: parking_lot::Mutex::new(FastHashMap::default()),
        }
    }

    pub(crate) fn initialization(
        &self,
        table_id: u64,
    ) -> Result<AutoIncrementInitialization<'_>, QueryError> {
        let mut initializations = self.initializations.lock();
        let gate = match initializations.entry(table_id) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.get().clone(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let memory = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                    std::mem::size_of::<AutoIncrementInitializationLock>()
                        + std::mem::size_of::<(u64, Arc<AutoIncrementInitializationLock>)>() * 2,
                )?;
                entry
                    .insert(Arc::new(AutoIncrementInitializationLock {
                        mutex: parking_lot::Mutex::new(()),
                        _memory: memory,
                    }))
                    .clone()
            }
        };
        Ok(AutoIncrementInitialization {
            manager: self,
            table_id,
            gate: Some(gate),
        })
    }

    #[inline]
    fn shard(&self, table_id: u64) -> usize {
        table_id as usize & (AUTO_INCREMENT_SHARDS - 1)
    }

    pub(crate) fn get(&self, table_id: u64) -> Option<Arc<AutoIncrementState>> {
        self.shards[self.shard(table_id)]
            .read()
            .get(&table_id)
            .cloned()
    }

    pub(crate) fn get_or_install(
        &self,
        table_id: u64,
        version: u64,
        max_value: u64,
    ) -> Result<Arc<AutoIncrementState>, QueryError> {
        if let Some(state) = self.get(table_id) {
            return Ok(state);
        }
        let state = Arc::new(AutoIncrementState::try_new(table_id, version, max_value)?);
        let mut shard = self.shards[self.shard(table_id)].write();
        Ok(shard
            .entry(table_id)
            .or_insert_with(|| state.clone())
            .clone())
    }

    pub(crate) fn cached_max(&self, table_id: u64) -> Option<u64> {
        self.get(table_id)
            .map(|state| state.max_value.load(Ordering::Acquire))
    }

    pub(crate) fn states(&self) -> Vec<Arc<AutoIncrementState>> {
        self.shards
            .iter()
            .flat_map(|shard| shard.read().values().cloned().collect::<Vec<_>>())
            .collect()
    }

    pub(crate) fn install(
        &self,
        table_id: u64,
        version: u64,
        max_value: u64,
    ) -> Result<(), QueryError> {
        let state = self.get_or_install(table_id, version, max_value)?;
        let _guard = state.slow_path.lock();
        let current_version = state.version.load(Ordering::Acquire);
        if version > current_version {
            state.version.store(version, Ordering::Release);
            state.max_value.store(max_value, Ordering::Release);
        } else if version == current_version {
            state.max_value.fetch_max(max_value, Ordering::AcqRel);
        }
        state.retired.store(false, Ordering::Release);
        Ok(())
    }

    pub(crate) fn forget_table(&self, table_id: u64) {
        if let Some(state) = self.shards[self.shard(table_id)].write().remove(&table_id) {
            state.retired.store(true, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod auto_increment_initialization_tests {
    use super::*;

    #[test]
    fn auto_increment_initialization_gates_are_released_after_concurrent_use() {
        let manager = AutoIncrementManager::new();
        let barrier = std::sync::Barrier::new(8);
        let active = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let (manager, barrier, active) = (&manager, &barrier, &active);
                scope.spawn(move || {
                    barrier.wait();
                    for _ in 0..64 {
                        let initialization = manager.initialization(1).unwrap();
                        let _guard = initialization.lock();
                        assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
                        std::thread::yield_now();
                        assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
                    }
                });
            }
        });
        assert!(manager.initializations.lock().is_empty());
    }

    #[test]
    fn auto_increment_initialization_does_not_block_other_tables_in_the_shard() {
        let manager = AutoIncrementManager::new();
        let first = manager.initialization(1).unwrap();
        let guard = first.lock();
        std::thread::scope(|scope| {
            let (send, receive) = std::sync::mpsc::channel();
            let manager = &manager;
            let worker = scope.spawn(move || {
                let second = manager
                    .initialization(1 + AUTO_INCREMENT_SHARDS as u64)
                    .unwrap();
                let _guard = second.lock();
                send.send(()).unwrap();
            });
            let result = receive.recv_timeout(Duration::from_secs(5));
            drop(guard);
            worker.join().unwrap();
            result.expect("unrelated tables must initialize independently");
        });
        drop(first);
        assert!(manager.initializations.lock().is_empty());
    }
}

impl TransactionManager {
    pub(crate) fn from_wal_source(
        records: &(impl crate::wal::WalRecordSource + ?Sized),
    ) -> Result<(Self, LockManager), QueryError> {
        let mut manager = Self::default();
        let mut lock_manager = LockManager::default();
        manager.next_txn_id = 1;
        manager.next_scn = 1;
        // Seeded to `next_scn - 1` at the end of replay (see below): after
        // recovery every durably-committed txn has a reconstructed `Committed`
        // slot, so the whole `[.., next_scn-1]` range is fully published.
        let mut replay_slots = hashbrown::HashMap::<_, _, ahash::RandomState>::default();
        for index in 0..records.len() {
            let header = records.header(index)?;
            if header.record_type == WalRecordType::UndoRecord {
                let undo = records.undo_record(index)?.expect("undo header");
                let undo_id = &undo.undo_id;
                let bytes = undo.bytes.as_ref();
                manager.next_txn_id = manager.next_txn_id.max(header.txn_id + 1);
                if let Ok(undo) =
                    crate::storage::undo::decode_undo_record_metadata(bytes).or_else(|_| {
                        serde_json::from_slice::<UndoRecord>(bytes).map_err(|e| {
                            crate::error::catalog::ER_GET_ERRNO.error(format!("undo decode: {}", e))
                        })
                    })
                {
                    replay_slots
                        .entry(header.txn_id)
                        .or_insert(TxnSlot {
                            txn_id: header.txn_id,
                            status: TxnStatus::Active,
                            start_scn: 0,
                            started_unix_ms: current_unix_ms(),
                            commit_scn: None,
                            undo_head: UndoHead::default(),
                            is_writer: false,
                            read_only: false,
                            wal_logged: true,
                            xa_xid: None,
                            begin_lsn: header.lsn,
                        })
                        .undo_head
                        .set(Some((*undo_id).max(undo.id)));
                }
                continue;
            }
            if !matches!(
                header.record_type,
                WalRecordType::CheckpointEnd
                    | WalRecordType::BeginTxn
                    | WalRecordType::CommitTxn
                    | WalRecordType::AbortTxn
                    | WalRecordType::EndTxn
                    | WalRecordType::RollbackTo
                    | WalRecordType::PrepareTxn
            ) {
                continue;
            }
            let record = records.record(index)?;
            match &record.payload {
                WalPayload::CheckpointEnd {
                    next_txn_id,
                    next_scn,
                    ..
                } => {
                    manager.next_txn_id = manager.next_txn_id.max(*next_txn_id);
                    manager.next_scn = manager.next_scn.max(*next_scn);
                }
                WalPayload::BeginTxn { start_scn } => {
                    manager.next_txn_id = manager.next_txn_id.max(record.txn_id + 1);
                    manager.next_scn = manager.next_scn.max(start_scn + 1);
                    replay_slots.entry(record.txn_id).or_insert(TxnSlot {
                        txn_id: record.txn_id,
                        status: TxnStatus::Active,
                        start_scn: *start_scn,
                        started_unix_ms: current_unix_ms(),
                        commit_scn: None,
                        undo_head: UndoHead::default(),
                        is_writer: false,
                        read_only: false,
                        wal_logged: true,
                        xa_xid: None,
                        begin_lsn: record.lsn,
                    });
                }
                WalPayload::CommitTxn { commit_scn, .. } => {
                    manager.next_txn_id = manager.next_txn_id.max(record.txn_id + 1);
                    manager.next_scn = manager.next_scn.max(commit_scn + 1);
                    replay_slots
                        .entry(record.txn_id)
                        .and_modify(|slot| {
                            slot.status = TxnStatus::Committed;
                            slot.commit_scn = Some(*commit_scn);
                        })
                        .or_insert(TxnSlot {
                            txn_id: record.txn_id,
                            status: TxnStatus::Committed,
                            start_scn: commit_scn.saturating_sub(1),
                            started_unix_ms: 0,
                            commit_scn: Some(*commit_scn),
                            undo_head: UndoHead::default(),
                            is_writer: false,
                            read_only: false,
                            wal_logged: true,
                            xa_xid: None,
                            begin_lsn: 0,
                        });
                }
                WalPayload::AbortTxn => {
                    // Abort is durable intent, not completed compensation.
                    // Physical replicas can restart at a rollback batch and
                    // must retain the remaining chain for catch-up/promotion.
                    // Native recovery appends EndTxn after finishing UNDO.
                    replay_slots
                        .entry(record.txn_id)
                        .and_modify(|slot| {
                            if slot.status.is_in_flight() {
                                slot.status = TxnStatus::Active;
                            }
                        })
                        .or_insert(TxnSlot {
                            txn_id: record.txn_id,
                            status: TxnStatus::Active,
                            start_scn: 0,
                            started_unix_ms: 0,
                            commit_scn: None,
                            undo_head: UndoHead::default(),
                            is_writer: false,
                            read_only: false,
                            wal_logged: true,
                            xa_xid: None,
                            begin_lsn: 0,
                        });
                }
                WalPayload::EndTxn => {
                    if let Some(slot) = replay_slots.get_mut(&record.txn_id) {
                        if slot.status.is_in_flight() {
                            slot.status = TxnStatus::RolledBack;
                            slot.undo_head.set(None);
                        }
                    }
                }
                WalPayload::RollbackTo { undo_head, .. } => {
                    if let Some(slot) = replay_slots.get_mut(&record.txn_id) {
                        slot.undo_head.set(*undo_head);
                    }
                }
                WalPayload::PrepareTxn {
                    xid,
                    prepare_scn,
                    locks,
                    ..
                } => {
                    manager.next_txn_id = manager.next_txn_id.max(record.txn_id + 1);
                    manager.next_scn = manager.next_scn.max(prepare_scn + 1);
                    let slot = replay_slots.entry(record.txn_id).or_insert(TxnSlot {
                        txn_id: record.txn_id,
                        status: TxnStatus::Active,
                        start_scn: 0,
                        started_unix_ms: current_unix_ms(),
                        commit_scn: None,
                        undo_head: UndoHead::default(),
                        is_writer: false,
                        read_only: false,
                        wal_logged: true,
                        xa_xid: None,
                        begin_lsn: record.lsn,
                    });
                    // WAL replay is ordered: a subsequent commit, abort
                    // intent or end overrides this preparation. Never turn
                    // a terminal slot back into a prepared branch.
                    if slot.status == TxnStatus::Active {
                        slot.status = TxnStatus::Prepared;
                    }
                    slot.xa_xid = Some(xid.clone());
                    if slot.status == TxnStatus::Prepared {
                        LockManager::install_prepared_locks(
                            &mut lock_manager.locks,
                            &mut lock_manager.unique_locks,
                            &mut lock_manager.unique_point_tables,
                            &mut lock_manager.table_locks,
                            &mut lock_manager.lock_log,
                            record.txn_id,
                            locks,
                        )?;
                    }
                }
                _ => {}
            }
            // Only unresolved PREPARED branches retain their lock snapshot.
            // Resolve it while replaying the terminal record, before the
            // startup migration moves table/row locks into sharded stores.
            if matches!(
                header.record_type,
                WalRecordType::CommitTxn | WalRecordType::AbortTxn | WalRecordType::EndTxn
            ) && lock_manager.lock_log.contains_key(&record.txn_id)
            {
                lock_manager.unlock_txn(record.txn_id);
            }
        }
        manager.slots = replay_slots.into_iter().collect();
        manager.active_count = manager
            .slots
            .values()
            .filter(|slot| slot.status.is_in_flight())
            .count();
        manager.oldest_write_capable_txn_id = manager
            .slots
            .values()
            .filter(|slot| slot.status.is_in_flight() && !slot.read_only)
            .map(|slot| slot.txn_id)
            .min();
        manager.slot_gc_cursor = manager.slots.keys().next().copied().unwrap_or(0);
        // Rebuild the writer flags from the replayed undo heads, then the
        // write count from the flags (the single source the runtime
        // bookkeeping uses from here on).
        for slot in manager.slots.values_mut() {
            slot.is_writer = slot.undo_head.get().is_some() || slot.status == TxnStatus::Prepared;
        }
        manager.active_write_count = manager
            .slots
            .values()
            .filter(|slot| slot.status.is_in_flight() && slot.is_writer)
            .count();
        // P0-1: the whole recovered SCN range is fully published. Any recovered
        // Active/Prepared slot stays invisible via its status, not via the
        // frontier, so seeding to `next_scn - 1` is correct and contiguous with
        // the allocation counter (`Database::next_scn_atomic` is seeded to the
        // same `next_scn`).
        manager.commit_frontier = manager.next_scn.saturating_sub(1);
        manager.registered_txn_frontier = manager.next_txn_id.saturating_sub(1);
        Ok((manager, lock_manager))
    }
}

impl LockManager {
    pub(crate) fn unique_key_lock_count(&self) -> usize {
        self.unique_locks.len() + self.unique_range_keys
    }

    pub(crate) fn unique_range_locks(&self) -> impl Iterator<Item = (&UniqueKeyRangeLock, u64)> {
        self.unique_ranges.values().flat_map(|entries| {
            entries
                .values()
                .map(|(range, owner)| (range.as_ref(), *owner))
        })
    }

    pub(crate) fn restore_prepared_locks(
        &mut self,
        txn_id: u64,
        entries: &[PreparedLockEntry],
    ) -> Result<(), QueryError> {
        // A failed promotion can retry before its runtime registry is
        // published. Replace any partially installed legacy snapshot.
        self.unlock_txn(txn_id);
        Self::install_prepared_locks(
            &mut self.locks,
            &mut self.unique_locks,
            &mut self.unique_point_tables,
            &mut self.table_locks,
            &mut self.lock_log,
            txn_id,
            entries,
        )
    }

    pub(crate) fn row_lock_held_by_txn(&self, txn_id: u64, table_id: u64, row_id: RowId) -> bool {
        Self::row_lock_held_by(&self.locks, table_id, row_id, txn_id)
    }

    pub(crate) fn release_last_exact_row(
        &mut self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
    ) -> bool {
        let exact = self.lock_log.get(&txn_id).is_some_and(|log| {
            matches!(
                log.last(),
                Some(HeldLock::Row(t, r) | HeldLock::RowMode(t, r, _))
                    if *t == table_id && *r == row_id
            )
        });
        if !exact {
            return false;
        }
        let held = self
            .lock_log
            .get_mut(&txn_id)
            .and_then(Vec::pop)
            .expect("exact lock checked above");
        Self::release_held_lock(
            &mut self.locks,
            &mut self.unique_locks,
            &mut self.unique_point_tables,
            &mut self.unique_ranges,
            &mut self.unique_range_keys,
            &mut self.table_locks,
            txn_id,
            held,
        );
        self.clear_unblocked_waits_for_owner(txn_id);
        true
    }

    /// Replay the lock snapshot of a PREPARED branch (read from a
    /// `WalPayload::PrepareTxn` record) into the in-memory lock manager.
    /// Re-installs row, unique-key and table locks under `txn_id` and
    /// re-emits the matching `HeldLock` entries so the standard commit /
    /// rollback path releases them when the branch eventually completes.
    fn install_prepared_locks(
        locks: &mut FastHashMap<(u64, RowId), RowLockState>,
        unique_locks: &mut UniqueLockMap,
        point_tables: &mut UniquePointTables,
        table_locks: &mut FastHashMap<u64, TableLockState>,
        lock_log: &mut FastHashMap<u64, Vec<HeldLock>>,
        txn_id: u64,
        entries: &[PreparedLockEntry],
    ) -> Result<(), QueryError> {
        let log = lock_log.entry(txn_id).or_default();
        for entry in entries {
            match entry {
                PreparedLockEntry::Row {
                    table_id,
                    page_id,
                    slot_id,
                    mode,
                } => {
                    let row_id = RowId {
                        page_id: *page_id,
                        slot_id: *slot_id,
                    };
                    let lock_mode = if *mode == 0 {
                        LockMode::Shared
                    } else {
                        LockMode::Exclusive
                    };
                    let key = (*table_id, row_id);
                    locks
                        .entry(key)
                        .and_modify(|state| {
                            state.owners.add(txn_id);
                            if lock_mode == LockMode::Exclusive {
                                state.mode = LockMode::Exclusive;
                            }
                        })
                        .or_insert_with(|| RowLockState::new(lock_mode, txn_id));
                    log.push(HeldLock::RowMode(*table_id, row_id, lock_mode));
                }
                PreparedLockEntry::UniqueKey {
                    table_id,
                    index_name,
                    key_bytes,
                } => {
                    let key = serde_json::from_slice::<IndexKey>(key_bytes).map_err(|err| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            format!("invalid prepared unique lock: {err}"),
                        )
                    })?;
                    let lock = Arc::new(UniqueKeyLock::new(*table_id, index_name.clone(), key));
                    if unique_locks.insert(lock.clone(), txn_id).is_none() {
                        point_tables.increment(*table_id);
                    }
                    log.push(HeldLock::Unique(lock));
                }
                PreparedLockEntry::Table { table_id, mode } => {
                    let tl_mode = match mode {
                        0 => TableLockMode::IntentShared,
                        1 => TableLockMode::IntentExclusive,
                        2 => TableLockMode::Shared,
                        3 => TableLockMode::ShareIntentExclusive,
                        _ => TableLockMode::Exclusive,
                    };
                    table_locks
                        .entry(*table_id)
                        .or_default()
                        .holders
                        .insert(txn_id, tl_mode);
                    log.push(HeldLock::Table(*table_id, tl_mode));
                }
            }
        }
        Ok(())
    }
}

impl TransactionManager {
    fn advance_oldest_write_capable_after_finish(&mut self, txn_id: u64) {
        if self.oldest_write_capable_txn_id != Some(txn_id) {
            return;
        }
        self.oldest_write_capable_txn_id = self
            .slots
            .range(txn_id.saturating_add(1)..)
            .find_map(|(id, slot)| (slot.status.is_in_flight() && !slot.read_only).then_some(*id));
    }

    pub(crate) fn close_registration_history(&mut self) {
        self.registered_txn_frontier = self.next_txn_id.saturating_sub(1);
        self.registered_txn_ranges.clear();
    }

    /// Consume an allocator ID for an operation that never owns row versions
    /// or undo. It has no transaction slot, but must close its registration
    /// gap just like a registered transaction. Do not infer anything about
    /// other IDs: a lower writer may still be between allocation and begin.
    pub(crate) fn note_nontransaction_id(&mut self, id: u64) {
        self.note_registered_txn(id);
        if id < (1u64 << 63) {
            self.next_txn_id = self.next_txn_id.max(id.saturating_add(1));
        }
    }

    fn note_registered_txn(&mut self, id: u64) {
        if id >= (1u64 << 63) || id <= self.registered_txn_frontier {
            return;
        }
        if id == self.registered_txn_frontier + 1 {
            self.registered_txn_frontier = id;
        } else {
            let mut start = id;
            let mut end = id;
            if let Some((&lo, &hi)) = self.registered_txn_ranges.range(..=id).next_back() {
                if hi >= id {
                    return;
                }
                if hi + 1 == id {
                    start = lo;
                    self.registered_txn_ranges.remove(&lo);
                }
            }
            if let Some(hi) = self.registered_txn_ranges.remove(&(id + 1)) {
                end = hi;
            }
            self.registered_txn_ranges.insert(start, end);
        }
        while let Some(end) = self
            .registered_txn_ranges
            .remove(&(self.registered_txn_frontier + 1))
        {
            self.registered_txn_frontier = end;
        }
    }

    pub(crate) fn begin_with_id(
        &mut self,
        txn_id: u64,
        start_scn: u64,
        read_only: bool,
        wal_logged: bool,
        begin_lsn: u64,
    ) -> Result<TxnView, QueryError> {
        Ok(self.register_new_slot(TxnSlot {
            txn_id,
            status: TxnStatus::Active,
            start_scn,
            started_unix_ms: current_unix_ms(),
            commit_scn: None,
            undo_head: UndoHead::default(),
            is_writer: false,
            read_only,
            wal_logged,
            xa_xid: None,
            begin_lsn,
        }))
    }

    /// Register a fully prepared slot. DML constructs its private head and
    /// timestamp before acquiring the directory latch, then publishes the
    /// writer flag in the same critical section as slot registration.
    pub(crate) fn register_new_slot(&mut self, slot: TxnSlot) -> TxnView {
        let txn_id = slot.txn_id;
        let start_scn = slot.start_scn;
        let read_only = slot.read_only;
        self.note_registered_txn(txn_id);
        if txn_id < (1u64 << 63) {
            self.next_txn_id = self.next_txn_id.max(txn_id.saturating_add(1));
        }
        self.next_scn = self.next_scn.max(start_scn.saturating_add(1));
        self.active_write_count += usize::from(slot.is_writer);
        self.slots.insert(txn_id, slot);
        self.active_count += 1;
        if !read_only {
            self.oldest_write_capable_txn_id = Some(
                self.oldest_write_capable_txn_id
                    .map_or(txn_id, |oldest| oldest.min(txn_id)),
            );
        }
        TxnView {
            txn_id,
            snapshot_scn: start_scn,
            isolation: IsolationLevel::ReadCommitted,
            read_only,
            dirty_read: false,
            historical: false,
            historical_baseline_scn: 0,
            historical_baseline_txn_id: 0,
            connection_id: 0,
            snapshot_registered: false,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn mark_wal_logged(&mut self, txn_id: u64) -> Result<(), QueryError> {
        let slot = self.active_slot_mut(txn_id)?;
        slot.wal_logged = true;
        Ok(())
    }

    /// Promote an ACTIVE transaction to PREPARED for XA two-phase commit.
    /// Locks, undo head, active counters, and the undo chain are
    /// intentionally untouched — the PREPARED branch is still in-flight
    /// from every other observer's point of view; only the session
    /// ownership has been detached. The caller (storage layer) writes the
    /// `PrepareTxn` WAL record before flipping the in-memory status.
    pub(crate) fn prepare(&mut self, txn_id: u64, xid: crate::xa::Xid) -> Result<(), QueryError> {
        let slot = self.active_slot_mut(txn_id)?;
        slot.status = TxnStatus::Prepared;
        slot.xa_xid = Some(xid);
        Ok(())
    }

    pub(crate) fn publish_commit_read_only(&mut self, txn_id: u64) -> Result<(), QueryError> {
        // Accept PREPARED here too so XA COMMIT (one-phase or two-phase)
        // works against a read-only PREPARED branch — read-only XA
        // branches are rare but legal.
        let slot = self.in_flight_slot_mut(txn_id)?;
        slot.status = TxnStatus::Committed;
        slot.commit_scn = Some(0);
        let was_writer = slot.is_writer;
        self.active_count = self.active_count.saturating_sub(1);
        if was_writer {
            self.active_write_count = self.active_write_count.saturating_sub(1);
        }
        self.advance_oldest_write_capable_after_finish(txn_id);
        // Read-only txns hold no locks; the caller still drains the lock
        // manager (`unlock_txn`) defensively to clear any stray waiters.
        self.named_savepoints.remove(&txn_id);
        Ok(())
    }

    pub(crate) fn publish_commit(
        &mut self,
        txn_id: u64,
        commit_scn: u64,
    ) -> Result<u64, QueryError> {
        // Accept either ACTIVE or PREPARED; XA COMMIT after PREPARE
        // funnels through this method so the lock-release / index-repair
        // cleanup runs the same way as a regular commit.
        let slot = self.in_flight_slot_mut(txn_id)?;
        slot.status = TxnStatus::Committed;
        slot.commit_scn = Some(commit_scn);
        let was_writer = slot.is_writer;
        self.active_count = self.active_count.saturating_sub(1);
        if was_writer {
            self.active_write_count = self.active_write_count.saturating_sub(1);
        }
        self.advance_oldest_write_capable_after_finish(txn_id);
        // `next_scn` stays as recovery/checkpoint bookkeeping only. The
        // snapshot watermark is now the gap-aware `commit_frontier`, advanced
        // here — in the SAME critical section that flips the slot to Committed
        // — so no snapshot can ever observe `commit_frontier >= commit_scn`
        // while this slot is still Active (the P0-1 commit-visibility fix).
        self.next_scn = self.next_scn.max(commit_scn.saturating_add(1));
        let frontier = self.mark_frontier_done(commit_scn);
        // Committed old-key entries double as the in-memory temporal-history
        // index and are retired by `purge_obsolete_index_repairs` at the
        // oldest of the active-snapshot and configured history horizons.
        self.named_savepoints.remove(&txn_id);
        // Row/unique/table lock release (`unlock_txn`) now lives on the
        // separately-latched `LockManager`; the caller (`commit_txn`) drops the
        // `txns` write lock and then drains it, preserving the early-lock-release
        // ordering while taking the two latches sequentially (`txns` then
        // `lock_manager`).
        Ok(frontier)
    }

    /// P0-1: mark `scn` as fully resolved for the commit-visibility frontier
    /// and return the new frontier. "Resolved" means the SCN will never again
    /// transition a transaction into visibility at that number — either it just
    /// published (commit), or it is an XA `prepare_scn` / a voided commit whose
    /// transaction is invisible by slot status anyway. Every SCN drawn from the
    /// allocation counter MUST be marked exactly once, or the contiguous
    /// frontier stalls. Advances over any buffered out-of-order completions.
    pub(crate) fn mark_frontier_done(&mut self, scn: u64) -> u64 {
        if scn == self.commit_frontier.saturating_add(1) {
            self.commit_frontier = scn;
            while self
                .published_ahead
                .remove(&self.commit_frontier.saturating_add(1))
            {
                self.commit_frontier += 1;
            }
        } else if scn > self.commit_frontier.saturating_add(1) {
            // A later commit published before an earlier one — buffer it until
            // the gap fills.
            self.published_ahead.insert(scn);
        }
        // scn <= commit_frontier: already accounted for (recovery seed or a
        // double-mark) — leave the frontier untouched.
        self.commit_frontier
    }

    /// Seed the resolved frontier from a source snapshot carried by replicated
    /// WAL. A streamed `BeginTxn.start_scn` means every SCN at or below that
    /// value was already published before the transaction began. On a fresh
    /// physical replica those earlier transactions can be present only in the
    /// base backup, so waiting for their WAL records would stall visibility
    /// forever.
    pub(crate) fn seed_commit_frontier(&mut self, resolved_scn: u64) -> u64 {
        if resolved_scn > self.commit_frontier {
            self.commit_frontier = resolved_scn;
            self.published_ahead
                .retain(|scn| *scn > self.commit_frontier);
            while self
                .published_ahead
                .remove(&self.commit_frontier.saturating_add(1))
            {
                self.commit_frontier += 1;
            }
        }
        self.commit_frontier
    }

    /// Slot-side rollback bookkeeping. Row/unique/table lock release lives on
    /// the separately-latched `LockManager`; the caller drops the `txns` write
    /// lock and then calls `LockManager::unlock_txn` (lock order
    /// `txns` → `lock_manager`).
    pub(crate) fn rollback(&mut self, txn_id: u64) -> Result<(), QueryError> {
        let slot = self.slots.get_mut(&txn_id).ok_or_else(|| {
            error(
                crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                "transaction does not exist",
            )
        })?;
        // Either ACTIVE or PREPARED counts toward the in-flight accounting.
        let was_in_flight = slot.status.is_in_flight();
        let was_writer = was_in_flight && slot.is_writer;
        if was_in_flight {
            self.active_count = self.active_count.saturating_sub(1);
        }
        if was_writer {
            self.active_write_count = self.active_write_count.saturating_sub(1);
        }
        slot.status = TxnStatus::RolledBack;
        self.advance_oldest_write_capable_after_finish(txn_id);
        self.clear_index_repairs(txn_id);
        self.named_savepoints.remove(&txn_id);
        Ok(())
    }

    /// Undo a BEGIN that failed before its WAL record was accepted. Such a
    /// transaction has no durable identity and no row changes, so retaining a
    /// RolledBack slot would only pin GC/checkpoint bookkeeping.
    pub(crate) fn discard_failed_begin(&mut self, txn_id: u64) {
        let Some(slot) = self.slots.remove(&txn_id) else {
            return;
        };
        if slot.status.is_in_flight() {
            self.active_count = self.active_count.saturating_sub(1);
            if slot.is_writer {
                self.active_write_count = self.active_write_count.saturating_sub(1);
            }
        }
        self.advance_oldest_write_capable_after_finish(txn_id);
        self.clear_index_repairs(txn_id);
        self.named_savepoints.remove(&txn_id);
    }

    /// A replica cannot prune a streamed BEGIN merely because its first
    /// write has not arrived yet. Later records still need that identity.
    pub(crate) fn min_replica_in_flight_wal_lsn(&self) -> Option<u64> {
        self.slots
            .values()
            .filter(|slot| slot.status.is_in_flight() && slot.wal_logged)
            .map(|slot| slot.begin_lsn)
            .min()
    }

    /// Lowest WAL `begin_lsn` across in-flight write transactions
    /// (Active or Prepared with undo work). Checkpoint WAL truncation
    /// must retain records at or above this LSN so a crash can still
    /// undo these transactions. `None` when no such transaction exists.
    ///
    /// Safe against the "first write races the checkpoint" window:
    /// `is_writer` is set by `mark_writer` under the txns write lock
    /// BEFORE the transaction's first undo/WAL append, so by the time any
    /// of its WAL records can carry an LSN below the checkpoint's begin
    /// LSN, this filter already counts the transaction. (Stronger than
    /// the previous `undo_head`-based check, which published AFTER the
    /// append.)
    pub(crate) fn min_in_flight_wal_lsn(&self) -> Option<u64> {
        self.slots
            .values()
            .filter(|slot| {
                slot.status.is_in_flight() && (slot.is_writer || slot.status == TxnStatus::Prepared)
            })
            .map(|slot| slot.begin_lsn)
            .min()
    }

    pub(crate) fn status(&self, txn_id: u64) -> Option<TxnStatus> {
        self.slots.get(&txn_id).map(|slot| slot.status)
    }

    pub(crate) fn slot(&self, txn_id: u64) -> Option<&TxnSlot> {
        self.slots.get(&txn_id)
    }

    pub(crate) fn undo_head(&self, txn_id: u64) -> Result<Option<u64>, QueryError> {
        // Accept PREPARED here so XA ROLLBACK against a PREPARED branch
        // can probe its undo chain just like an ACTIVE branch.
        let slot = self.slots.get(&txn_id).ok_or_else(|| {
            error(
                crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                "transaction does not exist",
            )
        })?;
        if !slot.status.is_in_flight() {
            return Err(error(
                crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                "transaction is not active",
            ));
        }
        Ok(slot.undo_head.get())
    }

    /// Handle to the transaction's undo head for lock-free publication
    /// (`push_undo_with_flush`). Same in-flight gating as `undo_head`.
    pub(crate) fn undo_head_handle(&self, txn_id: u64) -> Result<UndoHead, QueryError> {
        let slot = self.slots.get(&txn_id).ok_or_else(|| {
            error(
                crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                "transaction does not exist",
            )
        })?;
        if !slot.status.is_in_flight() {
            return Err(error(
                crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                "transaction is not active",
            ));
        }
        Ok(slot.undo_head.clone())
    }

    /// Flag the transaction as a writer BEFORE its first undo/WAL append.
    /// Idempotent; keeps `active_write_count` consistent with `is_writer`.
    /// Called once per transaction (the undo-head CAS path skips it when
    /// the head is already non-empty), so the txns write lock here is not
    /// a per-row cost.
    pub(crate) fn mark_writer(&mut self, txn_id: u64) -> Result<(), QueryError> {
        let slot = self.active_slot_mut(txn_id)?;
        if !slot.is_writer {
            slot.is_writer = true;
            self.active_write_count += 1;
        }
        Ok(())
    }

    /// `lock_log_len` is captured from the (separately-latched) `LockManager`
    /// by the caller and threaded in so the slot snapshot stays consistent with
    /// the lock-log length without this method reaching across the lock split.
    #[cfg(test)]
    pub(crate) fn statement_savepoint(
        &self,
        txn_id: u64,
        lock_log_len: usize,
        row_log_len: usize,
    ) -> Result<StatementSavepoint, QueryError> {
        Ok(StatementSavepoint {
            wal_boundary: None,
            undo_head: self.active_slot(txn_id)?.undo_head.get(),
            lock_log_len,
            row_log_len,
            index_repair_log_len: self.active_slot(txn_id)?.undo_head.index_repair_log_len(),
            binlog_len: crate::binlog::txn_len(txn_id),
            live_row_deltas: HashMap::new(),
        })
    }

    /// Whether restoring the slot and index-repair log would change anything.
    /// The caller serializes statements of this transaction and must still
    /// compensate data, binlog and locks independently. In particular, an
    /// eager writer with no undo yet must restore its writer flag.
    pub(crate) fn statement_savepoint_matches(
        &self,
        txn_id: u64,
        savepoint: &StatementSavepoint,
    ) -> Result<bool, QueryError> {
        let slot = self.active_slot(txn_id)?;
        Ok(slot.undo_head.get() == savepoint.undo_head
            && slot.is_writer == savepoint.undo_head.is_some()
            && slot.undo_head.index_repair_log_len() == savepoint.index_repair_log_len)
    }

    /// Slot-side savepoint restore. The lock-log truncation + row-lock release
    /// runs on the `LockManager` (`restore_savepoint_locks`); the caller takes
    /// `txns` first, then `lock_manager`.
    pub(crate) fn restore_statement_savepoint(
        &mut self,
        txn_id: u64,
        savepoint: &StatementSavepoint,
    ) -> Result<(), QueryError> {
        let slot = self.active_slot_mut(txn_id)?;
        let was_some = slot.is_writer;
        let now_some = savepoint.undo_head.is_some();
        slot.undo_head.set(savepoint.undo_head);
        slot.is_writer = now_some;
        match (was_some, now_some) {
            (false, true) => self.active_write_count += 1,
            (true, false) => {
                self.active_write_count = self.active_write_count.saturating_sub(1);
            }
            _ => {}
        }
        self.truncate_index_repairs(txn_id, savepoint.index_repair_log_len);
        Ok(())
    }
}

impl LockManager {
    /// Compress an exact contiguous integer set without reserving any gaps.
    /// Point reservations on this table take precedence: decline rather
    /// than scan the point table while holding its latch. The caller then uses the
    /// ordinary per-key path, which also checks these ranges.
    pub(crate) fn try_lock_integer_range(
        &mut self,
        txn_id: u64,
        table_id: u64,
        index_name: &str,
        unsigned: bool,
        first: i128,
        last: i128,
    ) -> Result<bool, QueryError> {
        if self.unique_point_tables.contains_key(&table_id) || first > last {
            return Ok(false);
        }
        let valid = if unsigned {
            first >= 0 && last <= i128::from(u64::MAX)
        } else {
            first >= i128::from(i64::MIN) && last <= i128::from(i64::MAX)
        };
        if !valid {
            return Ok(false);
        }
        let Ok(count) = usize::try_from(last - first + 1) else {
            return Ok(false);
        };
        let name: Arc<str> = normalize_name(index_name).into();
        let scope = (table_id, Arc::clone(&name), unsigned);
        if self.unique_ranges.get(&scope).is_some_and(|entries| {
            entries
                .range(..=last)
                .next_back()
                .is_some_and(|(_, (range, _))| range.last >= first)
        }) {
            return Ok(false);
        }
        self.check_lock_capacity(count)?;
        let entries = self.unique_ranges.entry(scope).or_default();
        let range = Arc::new(UniqueKeyRangeLock {
            table_id,
            index_name: name,
            unsigned,
            first,
            last,
        });
        entries.insert(first, (Arc::clone(&range), txn_id));
        self.unique_range_keys += count;
        self.lock_log
            .entry(txn_id)
            .or_default()
            .push(HeldLock::UniqueRange(range));
        Ok(true)
    }

    fn row_lock_held_by(
        locks: &FastHashMap<(u64, RowId), RowLockState>,
        table_id: u64,
        row_id: RowId,
        txn_id: u64,
    ) -> bool {
        locks
            .get(&(table_id, row_id))
            .map(|state| state.holds(txn_id))
            .unwrap_or(false)
    }

    fn release_held_lock(
        locks: &mut FastHashMap<(u64, RowId), RowLockState>,
        unique_locks: &mut UniqueLockMap,
        point_tables: &mut UniquePointTables,
        unique_ranges: &mut UniqueRangeMap,
        unique_range_keys: &mut usize,
        table_locks: &mut FastHashMap<u64, TableLockState>,
        txn_id: u64,
        lock: HeldLock,
    ) {
        match lock {
            HeldLock::Row(table_id, row_id) | HeldLock::RowMode(table_id, row_id, _) => {
                Self::release_row_owner(locks, table_id, row_id, txn_id);
            }
            HeldLock::Rows(table_id, row_ids) => {
                for row_id in row_ids {
                    Self::release_row_owner(locks, table_id, row_id, txn_id);
                }
            }
            HeldLock::Unique(lock) => {
                if let hashbrown::hash_map::RawEntryMut::Occupied(entry) =
                    unique_locks.raw_entry_mut().from_key(lock.as_ref())
                {
                    if *entry.get() == txn_id {
                        entry.remove();
                        Self::release_unique_point_count(point_tables, lock.table_id);
                    }
                }
            }
            HeldLock::UniqueRange(range) => {
                let scope = (
                    range.table_id,
                    Arc::clone(&range.index_name),
                    range.unsigned,
                );
                if let Some(entries) = unique_ranges.get_mut(&scope) {
                    if entries
                        .get(&range.first)
                        .is_some_and(|(_, owner)| *owner == txn_id)
                    {
                        entries.remove(&range.first);
                        *unique_range_keys = unique_range_keys.saturating_sub(range.len());
                    }
                    if entries.is_empty() {
                        unique_ranges.remove(&scope);
                    }
                }
            }
            HeldLock::Table(table_id, _) => {
                if let Some(state) = table_locks.get_mut(&table_id) {
                    state.holders.remove(&txn_id);
                    if state.holders.is_empty() {
                        table_locks.remove(&table_id);
                    }
                }
            }
        }
    }

    fn release_unique_point_count(tables: &mut UniquePointTables, table_id: u64) {
        tables.decrement(table_id);
    }

    fn release_row_owner(
        locks: &mut FastHashMap<(u64, RowId), RowLockState>,
        table_id: u64,
        row_id: RowId,
        txn_id: u64,
    ) {
        let key = (table_id, row_id);
        let mut remove = false;
        if let Some(state) = locks.get_mut(&key) {
            if state.owners.remove(txn_id) {
                remove = true;
            } else if state.mode == LockMode::Exclusive {
                // Exclusive lock: at most one owner; switch the remaining
                // holder to S if we somehow weren't the sole owner.
                state.mode = LockMode::Shared;
            }
        }
        if remove {
            locks.remove(&key);
        }
    }
}

impl TransactionManager {
    pub(crate) fn history_index_batch(&self, txn_id: u64) -> Vec<(IndexRepairKey, RowId)> {
        self.index_repairs.writer_entries(txn_id)
    }

    #[cfg(test)]
    pub(crate) fn register_index_repairs<I>(
        &mut self,
        txn_id: u64,
        keys: I,
        row_id: RowId,
    ) -> Result<(), QueryError>
    where
        I: IntoIterator<Item = IndexRepairKey>,
    {
        self.register_index_repair_entries(txn_id, keys.into_iter().map(|key| (key, row_id)))
    }

    #[cfg(test)]
    pub(crate) fn register_index_repair_entries<I>(
        &mut self,
        txn_id: u64,
        repairs: I,
    ) -> Result<(), QueryError>
    where
        I: IntoIterator<Item = (IndexRepairKey, RowId)>,
    {
        let head = self.active_slot(txn_id)?.undo_head.clone();
        self.index_repairs.register(
            txn_id,
            repairs.into_iter().map(|(key, row_id)| (key, row_id, true)),
            &head,
            &self.index_repair_epochs,
            &self.index_repair_version,
        )
    }

    fn repair_writer_state(&self, writer: u64) -> Option<(TxnStatus, Option<u64>)> {
        self.slot(writer).map(|slot| (slot.status, slot.commit_scn))
    }

    pub(crate) fn index_visibility_repair_row_ids(
        &self,
        txn: TxnView,
        table_id: u64,
        index_name: &str,
        key_matches: impl FnMut(&IndexKey) -> bool,
        mut durable_commit_scn: impl FnMut(u64) -> Option<u64>,
    ) -> Vec<RowId> {
        self.index_repairs
            .candidates(table_id, index_name, None, None, key_matches)
            .into_iter()
            .filter(|candidate| {
                repair_entry_live_for(
                    txn,
                    candidate.writer,
                    self.repair_writer_state(candidate.writer),
                    &mut durable_commit_scn,
                )
            })
            .map(|candidate| candidate.row_id)
            .collect()
    }

    pub(crate) fn live_repair_entries(
        &self,
        txn: TxnView,
        candidates: Vec<IndexRepairCandidate>,
        mut durable_commit_scn: impl FnMut(u64) -> Option<u64>,
    ) -> Vec<(IndexKey, RowId)> {
        candidates
            .into_iter()
            .filter(|candidate| {
                repair_entry_live_for(
                    txn,
                    candidate.writer,
                    self.repair_writer_state(candidate.writer),
                    &mut durable_commit_scn,
                ) && repair_key_created_visible_to(
                    txn,
                    candidate.creator,
                    self.repair_writer_state(candidate.creator),
                    &mut durable_commit_scn,
                )
            })
            .map(|candidate| (candidate.key, candidate.row_id))
            .collect()
    }

    pub(crate) fn truncate_index_repairs(&mut self, txn_id: u64, len: usize) {
        let head = self.slots.get(&txn_id).map(|slot| slot.undo_head.clone());
        self.index_repairs.truncate(
            txn_id,
            len,
            head.as_ref(),
            &self.index_repair_epochs,
            &self.index_repair_version,
            true,
        );
    }

    pub(crate) fn clear_index_repairs(&mut self, txn_id: u64) {
        self.truncate_index_repairs(txn_id, 0);
    }

    /// Writers whose index repairs no current or future snapshot can need,
    /// with their undo heads when their slots are still retained. Each
    /// verdict rests on a terminal outcome (or a collected slot), so it stays
    /// valid after the transaction directory is released: callers clear these
    /// with `IndexRepairState::purge_finished` under the registry shards alone
    /// instead of excluding every BEGIN/COMMIT for the whole pass.
    pub(crate) fn obsolete_index_repair_writers(
        &self,
        oldest_snapshot_scn: Option<u64>,
    ) -> Vec<(u64, Option<UndoHead>)> {
        self.index_repairs
            .writer_ids()
            .into_iter()
            .filter(|txn_id| {
                self.slots.get(txn_id).is_none_or(|slot| match slot.status {
                    // PREPARED writers may still commit; their index
                    // repairs must be preserved until they leave the
                    // PREPARED state.
                    TxnStatus::Active | TxnStatus::Prepared => false,
                    TxnStatus::RolledBack => true,
                    TxnStatus::Committed => match (slot.commit_scn, oldest_snapshot_scn) {
                        (Some(commit_scn), Some(oldest)) => commit_scn <= oldest,
                        (Some(_), None) => true,
                        (None, _) => false,
                    },
                })
            })
            .map(|txn_id| {
                let head = self.slots.get(&txn_id).map(|slot| slot.undo_head.clone());
                (txn_id, head)
            })
            .collect()
    }
}

impl LockManager {
    pub(crate) fn lock_row(
        &mut self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
    ) -> Result<(), QueryError> {
        self.acquire_row_lock(txn_id, table_id, row_id, true)
    }

    pub(crate) fn check_row_lock(
        &mut self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
    ) -> Result<(), QueryError> {
        self.acquire_row_lock(txn_id, table_id, row_id, false)
    }

    /// Lock-table capacity guard. `MYDB_MAX_LOCKS` caps the total number of
    /// row + unique-key locks held across all transactions; `0` (the default)
    /// means unlimited. A bulk acquisition that would exceed the cap fails with
    /// MySQL 1206 rather than growing the in-memory lock table until the
    /// process runs out of memory. Checked once per batch.
    pub(crate) fn check_lock_capacity(&self, additional: usize) -> Result<(), QueryError> {
        let max = max_locks();
        if max == 0 {
            return Ok(());
        }
        let current = self.locks.len() + self.unique_locks.len() + self.unique_range_keys;
        if current.saturating_add(additional) > max {
            return Err(error(
                crate::error::catalog::ER_LOCK_TABLE_FULL,
                "The total number of locks exceeds the lock table size",
            ));
        }
        Ok(())
    }

    pub(crate) fn lock_rows(
        &mut self,
        txn_id: u64,
        table_id: u64,
        row_ids: impl IntoIterator<Item = RowId>,
    ) -> Result<(), QueryError> {
        let row_ids = row_ids.into_iter();
        let hint = row_ids.size_hint().0;
        self.check_lock_capacity(hint)?;
        // Pre-size the lock table once for the whole batch so a bulk
        // UPDATE / DELETE / INSERT of N rows doesn't trigger a cascade of
        // incremental HashMap rehashes while inserting N entries.
        if hint > 1 {
            self.locks.reserve(hint);
        }
        let mut acquired = Vec::with_capacity(hint);
        for row_id in row_ids {
            match self.acquire_row_lock_without_log(txn_id, table_id, row_id) {
                Ok(true) => acquired.push(row_id),
                Ok(false) => {}
                Err(err) => {
                    self.log_acquired_row_locks(txn_id, table_id, acquired);
                    return Err(err);
                }
            }
        }
        self.log_acquired_row_locks(txn_id, table_id, acquired);
        Ok(())
    }

    /// Try to acquire `mode` locks on a batch of rows under a single
    /// lock-table latch — the fast path for `SELECT … FOR UPDATE / FOR
    /// SHARE`. Rows that are free with no queued waiters (or, for shared
    /// mode, already share-locked) are taken immediately and recorded as one
    /// `HeldLock::Rows` entry. Rows that conflict, need an S→X upgrade, or
    /// already have waiters queued (FIFO fairness) are returned so the caller
    /// can block on them one at a time. This collapses the common
    /// no-contention case from N global-latch acquisitions down to one.
    pub(crate) fn lock_rows_batch_nonblocking(
        &mut self,
        txn_id: u64,
        table_id: u64,
        row_ids: &[RowId],
        mode: LockMode,
    ) -> Vec<RowId> {
        self.locks.reserve(row_ids.len());
        let mut acquired: Vec<RowId> = Vec::with_capacity(row_ids.len());
        let mut deferred: Vec<RowId> = Vec::new();
        for &row_id in row_ids {
            let key = (table_id, row_id);
            // Never jump a non-empty wait queue: defer any row someone is
            // already waiting on so the per-row path enqueues us in order.
            if self
                .row_waiters
                .get(&key)
                .is_some_and(|queue| !queue.is_empty())
            {
                deferred.push(row_id);
                continue;
            }
            match self.lock_owner_compat(key, txn_id, mode) {
                LockOwnerCheck::Free => {
                    self.locks.insert(key, RowLockState::new(mode, txn_id));
                    acquired.push(row_id);
                    match mode {
                        LockMode::Shared => {
                            self.row_s_lock_acquires = self.row_s_lock_acquires.saturating_add(1)
                        }
                        LockMode::Exclusive => {
                            self.row_x_lock_acquires = self.row_x_lock_acquires.saturating_add(1)
                        }
                    }
                }
                LockOwnerCheck::CanShare => {
                    // Shared request on a row already S-locked by others.
                    if let Some(state) = self.locks.get_mut(&key) {
                        if !state.holds(txn_id) {
                            state.owners.add(txn_id);
                        }
                    }
                    acquired.push(row_id);
                    self.row_s_lock_acquires = self.row_s_lock_acquires.saturating_add(1);
                }
                // SelfHolds may need an S→X upgrade; Conflict must wait. Both
                // route to the per-row blocking path (which records its own
                // lock-log entry only when it actually acquires).
                LockOwnerCheck::SelfHolds | LockOwnerCheck::Conflict(_) => deferred.push(row_id),
            }
        }
        if !acquired.is_empty() {
            self.waits_for.remove(&txn_id);
            self.lock_log
                .entry(txn_id)
                .or_default()
                .push(HeldLock::Rows(table_id, acquired));
        }
        deferred
    }

    pub(crate) fn log_acquired_row_locks(
        &mut self,
        txn_id: u64,
        table_id: u64,
        acquired: Vec<RowId>,
    ) {
        match acquired.as_slice() {
            [] => {}
            [row_id] => self
                .lock_log
                .entry(txn_id)
                .or_default()
                .push(HeldLock::Row(table_id, *row_id)),
            _ => self
                .lock_log
                .entry(txn_id)
                .or_default()
                .push(HeldLock::Rows(table_id, acquired)),
        }
    }

    /// A shared-latch probe for the common check-only path. Stale entries
    /// need no eager removal: insertion and transaction cleanup still own that
    /// mutation. A conflict falls back to the exclusive wait/deadlock path.
    pub(crate) fn unique_key_uncontended(
        &self,
        txn_id: u64,
        table_id: u64,
        index_name: &str,
        key: &IndexKey,
        status_of: &impl Fn(u64) -> Option<TxnStatus>,
    ) -> bool {
        let name = super::naming::normalize_name_cow(index_name);
        if let Some(owner) = unique_range_owner(&self.unique_ranges, table_id, &name, key) {
            if owner != txn_id
                && matches!(
                    status_of(owner),
                    Some(TxnStatus::Active | TxnStatus::Prepared)
                )
            {
                return false;
            }
        }
        let probe = UniqueKeyProbe {
            table_id,
            index_name: &name,
            key,
        };
        match self.unique_locks.get(&probe).copied() {
            Some(owner) if owner != txn_id => !matches!(
                status_of(owner),
                Some(TxnStatus::Active | TxnStatus::Prepared)
            ),
            _ => true,
        }
    }

    pub(crate) fn check_unique_key_lock(
        &mut self,
        txn_id: u64,
        table_id: u64,
        index_name: &str,
        key: &IndexKey,
        status_of: &impl Fn(u64) -> Option<TxnStatus>,
    ) -> Result<(), QueryError> {
        self.acquire_unique_key_lock(txn_id, table_id, index_name, key, false, status_of)
    }

    pub(crate) fn lock_unique_keys<'a>(
        &mut self,
        txn_id: u64,
        locks: impl IntoIterator<Item = (u64, &'a str, IndexKey)>,
        status_of: &impl Fn(u64) -> Option<TxnStatus>,
    ) -> Result<(), QueryError> {
        let mut locks = locks.into_iter().peekable();
        if locks.peek().is_none() {
            return Ok(());
        }
        let count = locks.size_hint().0;
        if locks.size_hint().1 == Some(1) {
            let (table_id, index_name, key) = locks.next().expect("one unique lock");
            return self.acquire_owned_unique_key_lock(
                txn_id,
                UniqueKeyLock {
                    table_id,
                    index_name: normalize_name(index_name).into(),
                    key,
                },
                true,
                status_of,
            );
        }
        if count > 1 {
            self.unique_locks.reserve(count);
        }
        let log = self.lock_log.entry(txn_id).or_default();
        log.reserve(count);
        // Reuse consecutive index identities and move already-built keys.
        // The lock table and rollback log share each immutable name.
        let mut previous_name: Option<(&str, Arc<str>)> = None;
        for (table_id, index_name, key) in locks {
            let name = match &previous_name {
                Some((previous, normalized)) if *previous == index_name => normalized.clone(),
                _ => {
                    let normalized: Arc<str> = normalize_name(index_name).into();
                    previous_name = Some((index_name, normalized.clone()));
                    normalized
                }
            };
            let mut lock = Some(UniqueKeyLock {
                table_id,
                index_name: name,
                key,
            });
            let key = lock.as_ref().unwrap();
            if let Some(owner) =
                unique_range_owner(&self.unique_ranges, key.table_id, &key.index_name, &key.key)
            {
                if owner == txn_id && !self.unique_locks.contains_key(key) {
                    continue;
                }
                if owner != txn_id
                    && matches!(
                        status_of(owner),
                        Some(TxnStatus::Active | TxnStatus::Prepared)
                    )
                {
                    return self.record_unique_key_wait(txn_id, owner, lock.unwrap());
                }
            }
            match Self::reserve_unique_key_lock(
                &mut self.unique_locks,
                &mut self.unique_point_tables,
                txn_id,
                &mut lock,
                true,
                status_of,
            ) {
                Ok(Some(lock)) => log.push(HeldLock::Unique(lock)),
                Ok(None) => {}
                Err(owner) => {
                    return self.record_unique_key_wait(
                        txn_id,
                        owner,
                        lock.expect("blocked key retained"),
                    )
                }
            }
        }
        Ok(())
    }

    /// `status_of` is the lock-free transaction-status probe (backed by
    /// `Database::slot_status_cache`). The lock subsystem lives behind its own
    /// `Mutex`, decoupled from the `txns` `RwLock`, so it must not reach back
    /// into the slot map (that would invert the `txns` → `lock_manager` lock
    /// order). Passing the owner's status in via a lock-free closure preserves
    /// the original "owner still Active/Prepared ⇒ wait; otherwise steal the
    /// stale reservation" semantics without taking the `txns` latch.
    pub(crate) fn acquire_unique_key_lock(
        &mut self,
        txn_id: u64,
        table_id: u64,
        index_name: &str,
        key: &IndexKey,
        insert_if_free: bool,
        status_of: &impl Fn(u64) -> Option<TxnStatus>,
    ) -> Result<(), QueryError> {
        if !insert_if_free
            && self.unique_key_uncontended(txn_id, table_id, index_name, key, status_of)
        {
            return Ok(());
        }
        let lock = UniqueKeyLock {
            table_id,
            index_name: normalize_name(index_name).into(),
            key: key.clone(),
        };
        self.acquire_owned_unique_key_lock(txn_id, lock, insert_if_free, status_of)
    }

    fn acquire_owned_unique_key_lock(
        &mut self,
        txn_id: u64,
        lock: UniqueKeyLock,
        insert_if_free: bool,
        status_of: &impl Fn(u64) -> Option<TxnStatus>,
    ) -> Result<(), QueryError> {
        if let Some(owner) = unique_range_owner(
            &self.unique_ranges,
            lock.table_id,
            &lock.index_name,
            &lock.key,
        ) {
            if owner == txn_id && !self.unique_locks.contains_key(&lock) {
                return Ok(());
            }
            if owner != txn_id
                && matches!(
                    status_of(owner),
                    Some(TxnStatus::Active | TxnStatus::Prepared)
                )
            {
                return self.record_unique_key_wait(txn_id, owner, lock);
            }
        }
        let mut lock = Some(lock);
        match Self::reserve_unique_key_lock(
            &mut self.unique_locks,
            &mut self.unique_point_tables,
            txn_id,
            &mut lock,
            insert_if_free,
            status_of,
        ) {
            Ok(Some(lock)) => {
                self.lock_log
                    .entry(txn_id)
                    .or_default()
                    .push(HeldLock::Unique(lock));
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(owner) => {
                self.record_unique_key_wait(txn_id, owner, lock.expect("blocked key retained"))
            }
        }
    }

    /// Probe and reserve with one hash and one table search. Return newly
    /// acquired identities to the caller so a batch can append to its lock
    /// log without looking up the transaction again for every key.
    #[inline]
    fn reserve_unique_key_lock(
        unique_locks: &mut UniqueLockMap,
        point_tables: &mut UniquePointTables,
        txn_id: u64,
        lock: &mut Option<UniqueKeyLock>,
        insert_if_free: bool,
        status_of: &impl Fn(u64) -> Option<TxnStatus>,
    ) -> Result<Option<Arc<UniqueKeyLock>>, u64> {
        let key = lock.as_ref().expect("pending unique key");
        // Match the map's ordinary key hashing exactly; precomputed insertion
        // must remain compatible with borrowed probes and table growth.
        let mut hasher = unique_locks.hasher().build_hasher();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        match unique_locks
            .raw_entry_mut()
            .from_key_hashed_nocheck(hash, key)
        {
            hashbrown::hash_map::RawEntryMut::Occupied(mut entry) => {
                let owner = *entry.get();
                if owner == txn_id {
                    return Ok(None);
                }
                if matches!(
                    status_of(owner),
                    Some(TxnStatus::Active | TxnStatus::Prepared)
                ) {
                    return Err(owner);
                }
                if insert_if_free {
                    // The old owner's rollback log may retain this immutable
                    // identity. Its release still checks the current owner,
                    // so taking over a stale reservation can reuse it safely.
                    *entry.get_mut() = txn_id;
                    Ok(Some(Arc::clone(entry.key())))
                } else {
                    entry.remove();
                    Self::release_unique_point_count(point_tables, key.table_id);
                    Ok(None)
                }
            }
            hashbrown::hash_map::RawEntryMut::Vacant(entry) => {
                if insert_if_free {
                    let lock = Arc::new(lock.take().expect("new unique key"));
                    point_tables.increment(lock.table_id);
                    entry.insert_hashed_nocheck(hash, Arc::clone(&lock), txn_id);
                    Ok(Some(lock))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub(crate) fn acquire_row_lock_without_log(
        &mut self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
    ) -> Result<bool, QueryError> {
        let key = (table_id, row_id);
        match self.lock_owner_compat(key, txn_id, LockMode::Exclusive) {
            LockOwnerCheck::SelfHolds => {
                self.waits_for.remove(&txn_id);
                self.dequeue_row_waiter(txn_id, table_id, row_id);
                Ok(false)
            }
            LockOwnerCheck::Conflict(owners) => self
                .record_lock_wait(txn_id, owners, table_id, row_id, LockMode::Exclusive)
                .map(|()| false),
            LockOwnerCheck::CanShare => {
                // For exclusive request this branch is unreachable; treat as conflict.
                let owners = self
                    .locks
                    .get(&key)
                    .map(|state| state.owners.blockers_except(txn_id))
                    .unwrap_or_else(|| smallvec::smallvec![txn_id]);
                self.record_lock_wait(txn_id, owners, table_id, row_id, LockMode::Exclusive)
                    .map(|()| false)
            }
            LockOwnerCheck::Free => {
                self.try_acquire_waited_row(txn_id, table_id, row_id, LockMode::Exclusive)?;
                self.waits_for.remove(&txn_id);
                self.locks
                    .insert(key, RowLockState::new(LockMode::Exclusive, txn_id));
                self.row_x_lock_acquires = self.row_x_lock_acquires.saturating_add(1);
                Ok(true)
            }
        }
    }

    pub(crate) fn acquire_row_lock(
        &mut self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
        insert_if_free: bool,
    ) -> Result<(), QueryError> {
        self.acquire_row_lock_with_mode(
            txn_id,
            table_id,
            row_id,
            LockMode::Exclusive,
            insert_if_free,
        )
    }

    /// M2.1 - Acquire a row lock in the requested mode (S/X).
    pub(crate) fn acquire_row_lock_with_mode(
        &mut self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
        mode: LockMode,
        insert_if_free: bool,
    ) -> Result<(), QueryError> {
        let key = (table_id, row_id);
        match self.lock_owner_compat(key, txn_id, mode) {
            LockOwnerCheck::SelfHolds => {
                self.waits_for.remove(&txn_id);
                self.dequeue_row_waiter(txn_id, table_id, row_id);
                // Possibly upgrading S -> X.
                if mode == LockMode::Exclusive {
                    if let Some(state) = self.locks.get_mut(&key) {
                        if state.mode == LockMode::Shared && state.sole_owner(txn_id) {
                            state.mode = LockMode::Exclusive;
                            self.lock_upgrades = self.lock_upgrades.saturating_add(1);
                        } else if state.mode == LockMode::Shared {
                            // Cannot upgrade with multiple S holders without waiting.
                            self.lock_upgrade_failures =
                                self.lock_upgrade_failures.saturating_add(1);
                            let owners = state.owners.blockers_except(txn_id);
                            return self.record_lock_wait(txn_id, owners, table_id, row_id, mode);
                        }
                    }
                }
                Ok(())
            }
            LockOwnerCheck::CanShare => {
                self.try_acquire_waited_row(txn_id, table_id, row_id, mode)?;
                self.waits_for.remove(&txn_id);
                if insert_if_free {
                    if let Some(state) = self.locks.get_mut(&key) {
                        state.owners.add(txn_id);
                    } else {
                        self.locks.insert(key, RowLockState::new(mode, txn_id));
                    }
                    self.lock_log
                        .entry(txn_id)
                        .or_default()
                        .push(HeldLock::RowMode(table_id, row_id, mode));
                    self.row_s_lock_acquires = self.row_s_lock_acquires.saturating_add(1);
                }
                Ok(())
            }
            LockOwnerCheck::Conflict(owners) => {
                self.record_lock_wait(txn_id, owners, table_id, row_id, mode)
            }
            LockOwnerCheck::Free => {
                self.try_acquire_waited_row(txn_id, table_id, row_id, mode)?;
                self.waits_for.remove(&txn_id);
                if insert_if_free {
                    self.locks.insert(key, RowLockState::new(mode, txn_id));
                    self.lock_log
                        .entry(txn_id)
                        .or_default()
                        .push(HeldLock::RowMode(table_id, row_id, mode));
                    match mode {
                        LockMode::Shared => {
                            self.row_s_lock_acquires = self.row_s_lock_acquires.saturating_add(1)
                        }
                        LockMode::Exclusive => {
                            self.row_x_lock_acquires = self.row_x_lock_acquires.saturating_add(1)
                        }
                    }
                }
                Ok(())
            }
        }
    }

    /// Acquire immediately without joining the FIFO wait queue or publishing
    /// a waits-for edge. Used only by the protocol's nonblocking scheduling
    /// probe; a conflict is replayed through the normal blocking acquire.
    pub(crate) fn try_acquire_row_lock_without_wait(
        &mut self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
        mode: LockMode,
    ) -> Result<bool, QueryError> {
        let key = (table_id, row_id);
        if self
            .row_waiters
            .get(&key)
            .and_then(|queue| queue.front())
            .is_some_and(|front| front.txn_id != txn_id)
            && !self
                .locks
                .get(&key)
                .is_some_and(|state| state.holds(txn_id))
        {
            return Ok(false);
        }
        match self.lock_owner_compat(key, txn_id, mode) {
            LockOwnerCheck::SelfHolds => {
                if mode == LockMode::Exclusive {
                    if let Some(state) = self.locks.get_mut(&key) {
                        if state.mode == LockMode::Shared && state.sole_owner(txn_id) {
                            state.mode = LockMode::Exclusive;
                            self.lock_upgrades = self.lock_upgrades.saturating_add(1);
                        } else if state.mode == LockMode::Shared {
                            return Ok(false);
                        }
                    }
                }
                Ok(true)
            }
            LockOwnerCheck::CanShare => {
                if let Some(state) = self.locks.get_mut(&key) {
                    state.owners.add(txn_id);
                } else {
                    self.locks.insert(key, RowLockState::new(mode, txn_id));
                }
                self.lock_log
                    .entry(txn_id)
                    .or_default()
                    .push(HeldLock::RowMode(table_id, row_id, mode));
                self.row_s_lock_acquires = self.row_s_lock_acquires.saturating_add(1);
                Ok(true)
            }
            LockOwnerCheck::Free => {
                self.locks.insert(key, RowLockState::new(mode, txn_id));
                self.lock_log
                    .entry(txn_id)
                    .or_default()
                    .push(HeldLock::RowMode(table_id, row_id, mode));
                match mode {
                    LockMode::Shared => {
                        self.row_s_lock_acquires = self.row_s_lock_acquires.saturating_add(1)
                    }
                    LockMode::Exclusive => {
                        self.row_x_lock_acquires = self.row_x_lock_acquires.saturating_add(1)
                    }
                }
                Ok(true)
            }
            LockOwnerCheck::Conflict(_) => Ok(false),
        }
    }

    fn lock_owner_compat(&self, key: (u64, RowId), txn_id: u64, mode: LockMode) -> LockOwnerCheck {
        let Some(state) = self.locks.get(&key) else {
            return LockOwnerCheck::Free;
        };
        if state.holds(txn_id) {
            return LockOwnerCheck::SelfHolds;
        }
        if mode == LockMode::Shared && state.mode == LockMode::Shared {
            return LockOwnerCheck::CanShare;
        }
        let owners = state.owners.blockers_except(txn_id);
        debug_assert!(!owners.is_empty());
        LockOwnerCheck::Conflict(owners)
    }

    pub(crate) fn record_lock_wait(
        &mut self,
        txn_id: u64,
        owners: LockBlockers,
        table_id: u64,
        row_id: RowId,
        mode: LockMode,
    ) -> Result<(), QueryError> {
        self.enqueue_row_waiter(txn_id, table_id, row_id, mode);
        self.record_resource_wait(
            txn_id,
            owners,
            LockWaitResource::Row(table_id, row_id),
            Some(mode),
        )
    }

    /// P2 — record a table-lock wait edge in the cross-shard deadlock graph on
    /// behalf of the sharded `ShardedTableLocks` store (whose conflict path
    /// can't reach `waits_for` itself). Returns the lock-wait-pending (1205) or
    /// deadlock (1213) error exactly as the legacy in-manager table-lock
    /// acquire did. Rare path: only when an incompatible holder blocks a
    /// (DDL X / SERIALIZABLE S / promotion) table-lock request.
    pub(crate) fn record_table_lock_wait(
        &mut self,
        txn_id: u64,
        owner: u64,
        table_id: u64,
    ) -> Result<(), QueryError> {
        self.table_lock_waits = self.table_lock_waits.saturating_add(1);
        self.record_resource_wait(
            txn_id,
            smallvec::smallvec![owner],
            LockWaitResource::Table(table_id),
            None,
        )
    }

    /// P3 — record a row-lock wait edge in the cross-shard deadlock graph on
    /// behalf of the sharded `ShardedRowLocks` store (the waiter is already
    /// enqueued on its row shard; this only updates `waits_for` + runs deadlock
    /// detection). Returns 1205 (pending) or 1213 (deadlock). In sharded mode
    /// the global `row_waiters` map is empty, so `record_resource_wait`'s
    /// previous-resource dequeue is a harmless no-op (the shard handled it).
    pub(crate) fn record_row_lock_wait(
        &mut self,
        txn_id: u64,
        owners: LockBlockers,
        table_id: u64,
        row_id: RowId,
        mode: LockMode,
    ) -> Result<(), QueryError> {
        self.record_resource_wait(
            txn_id,
            owners,
            LockWaitResource::Row(table_id, row_id),
            Some(mode),
        )
    }

    fn record_unique_key_wait(
        &mut self,
        txn_id: u64,
        owner: u64,
        lock: UniqueKeyLock,
    ) -> Result<(), QueryError> {
        self.record_resource_wait(
            txn_id,
            smallvec::smallvec![owner],
            LockWaitResource::Unique(lock),
            None,
        )
    }

    fn record_resource_wait(
        &mut self,
        txn_id: u64,
        owners: LockBlockers,
        resource: LockWaitResource,
        mode: Option<LockMode>,
    ) -> Result<(), QueryError> {
        debug_assert!(!owners.is_empty());
        let existing_wait = self
            .waits_for
            .get(&txn_id)
            .is_some_and(|wait| wait.owners == owners && wait.resource == resource);
        if !existing_wait {
            if let Some(previous) = self
                .waits_for
                .get(&txn_id)
                .map(|wait| wait.resource.clone())
            {
                if previous != resource {
                    self.remove_waiter_for_resource(txn_id, &previous);
                }
            }
            self.lock_wait_count = self.lock_wait_count.saturating_add(1);
            self.waits_for.insert(
                txn_id,
                LockWait {
                    owners: owners.clone(),
                    resource: resource.clone(),
                    started_unix_ms: current_unix_ms(),
                    mode,
                },
            );
        }
        if self.detect_deadlock(txn_id, &owners) {
            self.deadlock_count = self.deadlock_count.saturating_add(1);
            self.waits_for.remove(&txn_id);
            self.remove_waiter_for_resource(txn_id, &resource);
            return Err(error(
                crate::error::catalog::ER_LOCK_DEADLOCK,
                "deadlock found when trying to get lock; try restarting transaction",
            ));
        }
        Err(crate::storage::core::lock_wait_pending_error())
    }

    pub(crate) fn enqueue_row_waiter(
        &mut self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
        mode: LockMode,
    ) {
        let key = (table_id, row_id);
        if let Some(wait) = self.waits_for.get(&txn_id) {
            if let LockWaitResource::Row(wait_table_id, wait_row_id) = wait.resource {
                if (wait_table_id, wait_row_id) != key {
                    self.dequeue_row_waiter(txn_id, wait_table_id, wait_row_id);
                }
            }
        }
        let queue = self.row_waiters.entry(key).or_default();
        if !queue.iter().any(|waiter| waiter.txn_id == txn_id) {
            queue.push_back(RowWaiter { txn_id, mode });
        }
    }

    /// P4 — collect the front (FIFO-next) waiter of each given row, for
    /// directed wakeup. Only the front waiter can acquire next (enforced by
    /// `try_acquire_waited_row`), so waking just these avoids the herd.
    pub(crate) fn collect_front_row_waiters(&self, rows: &[(u64, RowId)], out: &mut Vec<u64>) {
        for key in rows {
            if let Some(queue) = self.row_waiters.get(key) {
                if let Some(front) = queue.front() {
                    out.push(front.txn_id);
                }
            }
        }
    }

    pub(crate) fn dequeue_row_waiter(&mut self, txn_id: u64, table_id: u64, row_id: RowId) {
        let key = (table_id, row_id);
        let mut remove_queue = false;
        if let Some(queue) = self.row_waiters.get_mut(&key) {
            queue.retain(|waiter| waiter.txn_id != txn_id);
            remove_queue = queue.is_empty();
        }
        if remove_queue {
            self.row_waiters.remove(&key);
        }
    }

    pub(crate) fn try_acquire_waited_row(
        &mut self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
        mode: LockMode,
    ) -> Result<(), QueryError> {
        let key = (table_id, row_id);
        let mut remove_queue = false;
        let mut blocked_by = None;
        if let Some(queue) = self.row_waiters.get_mut(&key) {
            match queue.front().copied() {
                Some(waiter) if waiter.txn_id == txn_id => {
                    queue.pop_front();
                    remove_queue = queue.is_empty();
                }
                Some(waiter) => {
                    if !queue.iter().any(|waiter| waiter.txn_id == txn_id) {
                        queue.push_back(RowWaiter { txn_id, mode });
                    }
                    blocked_by = Some(waiter.txn_id);
                }
                None => remove_queue = true,
            }
        }
        if remove_queue {
            self.row_waiters.remove(&key);
        }
        if let Some(owner) = blocked_by {
            return self.record_resource_wait(
                txn_id,
                smallvec::smallvec![owner],
                LockWaitResource::Row(table_id, row_id),
                Some(mode),
            );
        }
        Ok(())
    }

    pub(crate) fn clear_wait(&mut self, txn_id: u64) {
        self.clear_wait_with_change(txn_id);
    }

    /// Report queue/graph changes so no-op statement compensation need not
    /// wake unrelated transactions. A removed FIFO entry can enable its
    /// successor even when this transaction released no held locks.
    pub(crate) fn clear_wait_with_change(&mut self, txn_id: u64) -> bool {
        let mut changed = false;
        if let Some(wait) = self.waits_for.remove(&txn_id) {
            changed = true;
            self.remove_waiter_for_resource(txn_id, &wait.resource);
            if matches!(wait.resource, LockWaitResource::Row(_, _)) {
                return true;
            }
        }
        self.dequeue_all_row_waits(txn_id) || changed
    }

    fn remove_waiter_for_resource(&mut self, txn_id: u64, resource: &LockWaitResource) {
        if let LockWaitResource::Row(table_id, row_id) = resource {
            self.dequeue_row_waiter(txn_id, *table_id, *row_id);
        }
    }

    fn dequeue_all_row_waits(&mut self, txn_id: u64) -> bool {
        let keys = self
            .row_waiters
            .iter()
            .filter_map(|(key, queue)| {
                queue
                    .iter()
                    .any(|waiter| waiter.txn_id == txn_id)
                    .then_some(*key)
            })
            .collect::<Vec<_>>();
        let changed = !keys.is_empty();
        for (table_id, row_id) in keys {
            self.dequeue_row_waiter(txn_id, table_id, row_id);
        }
        changed
    }

    pub(crate) fn detect_deadlock(&self, waiting_txn: u64, owners: &[u64]) -> bool {
        // Most waits have one blocker and stay entirely inline. Exclusive
        // requests over a shared row can have several blockers, so walk the
        // complete graph instead of treating it as a functional graph.
        let mut pending = SmallVec::<[u64; 8]>::from_slice(owners);
        let mut seen = SmallVec::<[u64; 16]>::new();
        while let Some(owner) = pending.pop() {
            if owner == waiting_txn {
                return true;
            }
            if seen.contains(&owner) {
                continue;
            }
            seen.push(owner);
            if let Some(wait) = self.waits_for.get(&owner) {
                pending.extend(wait.owners.iter().copied());
            }
        }
        false
    }

    pub(crate) fn lock_wait_diagnostics(&self, limit: usize, active_count: usize) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            "active_txns={} row_locks={} unique_locks={} table_locks={} row_wait_queues={} waits_for={}",
            active_count,
            self.locks.len(),
            self.unique_key_lock_count(),
            self.table_locks.len(),
            self.row_waiters.len(),
            self.waits_for.len()
        ));
        for (waiter, wait) in self.waits_for.iter().take(limit) {
            let owner_wait = wait
                .owners
                .first()
                .and_then(|owner| self.waits_for.get(owner))
                .map(|owner_wait| {
                    format!(
                        " owner_waits_for txns={:?} resource={}",
                        owner_wait.owners,
                        Self::format_wait_resource(&owner_wait.resource)
                    )
                })
                .unwrap_or_default();
            lines.push(format!(
                "waiter txn={} waits_for txns={:?} resource={} blocked={}{}",
                waiter,
                wait.owners,
                Self::format_wait_resource(&wait.resource),
                self.lock_wait_still_blocked(*waiter, wait),
                owner_wait
            ));
        }
        for (row_id, queue) in self.row_waiters.iter().take(limit) {
            let owners = self
                .locks
                .get(row_id)
                .map(|state| {
                    format!(
                        "lock_mode={} owners={:?}",
                        state.mode.as_str(),
                        state.owners.as_slice()
                    )
                })
                .unwrap_or_else(|| "lock_free".to_string());
            let waiters = queue
                .iter()
                .map(|waiter| format!("{}:{}", waiter.txn_id, waiter.mode.as_str()))
                .collect::<Vec<_>>()
                .join(",");
            lines.push(format!(
                "row_queue row={:?} {} waiters=[{}]",
                row_id, owners, waiters
            ));
        }
        lines.join(" | ")
    }

    fn format_wait_resource(resource: &LockWaitResource) -> String {
        match resource {
            LockWaitResource::Row(table_id, row_id) => {
                format!("row(table={table_id}, {row_id:?})")
            }
            LockWaitResource::Unique(lock) => format!(
                "unique(table={}, index={}, key={:?})",
                lock.table_id, lock.index_name, lock.key
            ),
            LockWaitResource::Table(table_id) => format!("table({table_id})"),
        }
    }
}

impl TransactionManager {
    /// Only ACTIVE slots — explicitly excludes PREPARED. Used by the
    /// startup path that rolls back stale in-memory transactions that
    /// never reached COMMIT/ABORT/END in the WAL; PREPARED branches are
    /// fully durable and must NOT be auto-rolled-back at startup —
    /// they wait for an external XA COMMIT / XA ROLLBACK.
    pub(crate) fn active_txn_ids(&self) -> Vec<u64> {
        self.slots
            .values()
            .filter(|slot| slot.status == TxnStatus::Active)
            .map(|slot| slot.txn_id)
            .collect()
    }

    pub(crate) fn active_txn_count(&self) -> usize {
        self.active_count
    }

    /// Returns `(txn_id, head)` so the
    /// keep-set walker in `plan_purge_victims` can cache per-txn state
    /// across purge runs. Each active txn's undo chain — the linear
    /// list reachable via `prev_undo_ptr` — is private to that txn:
    /// `push_undo_with_flush` sets `prev_undo_ptr` to the txn's prior
    /// head, never another txn's. So caching by `txn_id` is safe and
    /// independent walks never overlap.
    pub(crate) fn active_undo_heads_with_txn(&self) -> Vec<(u64, u64)> {
        self.slots
            .values()
            .filter(|slot| slot.status.is_in_flight())
            .filter_map(|slot| slot.undo_head.get().map(|head| (slot.txn_id, head)))
            .collect()
    }

    pub(crate) fn undo_creator_states_for(
        &self,
        creator_ids: &[u64],
    ) -> BTreeMap<u64, UndoCreatorState> {
        creator_ids
            .iter()
            .filter_map(|txn_id| self.slots.get(txn_id))
            .map(|slot| {
                let state = match slot.status {
                    TxnStatus::Active => UndoCreatorState::Active,
                    TxnStatus::Prepared => UndoCreatorState::Prepared,
                    TxnStatus::Committed => {
                        UndoCreatorState::Committed(slot.commit_scn.unwrap_or(0))
                    }
                    TxnStatus::RolledBack => UndoCreatorState::RolledBack,
                };
                (slot.txn_id, state)
            })
            .collect()
    }
}

impl LockManager {
    pub(crate) fn active_lock_waits(&self) -> usize {
        self.waits_for
            .iter()
            .filter(|(waiter, wait)| self.lock_wait_still_blocked(**waiter, wait))
            .count()
    }

    pub(crate) fn oldest_lock_wait_ms(&self) -> u64 {
        let now = current_unix_ms();
        self.waits_for
            .iter()
            .filter(|(waiter, wait)| self.lock_wait_still_blocked(**waiter, wait))
            .map(|(_, wait)| now.saturating_sub(wait.started_unix_ms))
            .max()
            .unwrap_or(0)
    }

    pub(crate) fn blocked_row_wait(&self, txn_id: u64) -> Option<(u64, RowId)> {
        let wait = self.waits_for.get(&txn_id)?;
        if !self.lock_wait_still_blocked(txn_id, wait) {
            return None;
        }
        match wait.resource {
            LockWaitResource::Row(table_id, row_id) => Some((table_id, row_id)),
            LockWaitResource::Unique(_) | LockWaitResource::Table(_) => None,
        }
    }

    fn lock_wait_still_blocked(&self, waiter: u64, wait: &LockWait) -> bool {
        wait.owners
            .iter()
            .any(|owner| self.lock_wait_owner_still_blocks(waiter, wait, *owner))
    }

    fn lock_wait_owner_still_blocks(&self, waiter: u64, wait: &LockWait, owner: u64) -> bool {
        match &wait.resource {
            LockWaitResource::Row(table_id, row_id) => Self::row_lock_held_by(
                &self.locks,
                *table_id,
                *row_id,
                owner,
            ) || self
                .row_waiters
                .get(&(*table_id, *row_id))
                .is_some_and(|queue| {
                    let owner_pos = queue
                        .iter()
                        .position(|row_waiter| row_waiter.txn_id == owner);
                    let waiter_pos = queue
                        .iter()
                        .position(|row_waiter| row_waiter.txn_id == waiter);
                    matches!((owner_pos, waiter_pos), (Some(owner), Some(waiter)) if owner < waiter)
                }),
            LockWaitResource::Unique(lock) => {
                self.unique_locks.get(lock).copied() == Some(owner)
                    || unique_range_owner(
                        &self.unique_ranges,
                        lock.table_id,
                        &lock.index_name,
                        &lock.key,
                    ) == Some(owner)
            }
            LockWaitResource::Table(table_id) => self
                .table_locks
                .get(table_id)
                .is_some_and(|state| state.holders.contains_key(&owner)),
        }
    }

    fn clear_unblocked_waits_for_owner(&mut self, owner: u64) {
        let released_edges = self
            .waits_for
            .iter()
            .filter_map(|(waiter, wait)| {
                (wait.owners.contains(&owner)
                    && !self.lock_wait_owner_still_blocks(*waiter, wait, owner))
                .then_some(*waiter)
            })
            .collect::<Vec<_>>();
        for waiter in released_edges {
            let remove_wait = self.waits_for.get_mut(&waiter).is_some_and(|wait| {
                wait.owners.retain(|candidate| *candidate != owner);
                wait.owners.is_empty()
            });
            if remove_wait {
                self.waits_for.remove(&waiter);
            }
        }
    }

    /// Current length of `txn_id`'s ordered held-lock log — captured into a
    /// `StatementSavepoint` so a later rollback-to-savepoint can release
    /// exactly the locks acquired after the savepoint.
    pub(crate) fn lock_log_len(&self, txn_id: u64) -> usize {
        self.lock_log.get(&txn_id).map(Vec::len).unwrap_or(0)
    }

    /// Lock-side half of `restore_statement_savepoint`: truncate the held-lock
    /// log back to `lock_log_len` and release every lock acquired since,
    /// returning the freed `(table_id, row_id)` pairs so the caller can wake
    /// row waiters. Paired with `TransactionManager::restore_statement_savepoint`
    /// (slot side); the caller takes `txns` first, then `lock_manager`.
    pub(crate) fn restore_savepoint_locks(
        &mut self,
        txn_id: u64,
        lock_log_len: usize,
    ) -> Vec<(u64, RowId)> {
        let mut released_rows: Vec<(u64, RowId)> = Vec::new();
        if let Some(log) = self.lock_log.get_mut(&txn_id) {
            let released = log.split_off(lock_log_len.min(log.len()));
            for lock in released.into_iter().rev() {
                Self::collect_released_rows(&lock, &mut released_rows);
                Self::release_held_lock(
                    &mut self.locks,
                    &mut self.unique_locks,
                    &mut self.unique_point_tables,
                    &mut self.unique_ranges,
                    &mut self.unique_range_keys,
                    &mut self.table_locks,
                    txn_id,
                    lock,
                );
            }
        }
        self.clear_unblocked_waits_for_owner(txn_id);
        released_rows
    }
}

impl TransactionManager {
    pub(crate) fn oldest_active_txn_ms(&self) -> u64 {
        let now = current_unix_ms();
        self.slots
            .values()
            .filter(|slot| slot.status.is_in_flight())
            .map(|slot| now.saturating_sub(slot.started_unix_ms))
            .max()
            .unwrap_or(0)
    }

    pub(crate) fn active_slot(&self, txn_id: u64) -> Result<&TxnSlot, QueryError> {
        let slot = self.slots.get(&txn_id).ok_or_else(|| {
            error(
                crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                "transaction does not exist",
            )
        })?;
        if slot.status != TxnStatus::Active {
            return Err(error(
                crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                "transaction is not active",
            ));
        }
        Ok(slot)
    }

    pub(crate) fn active_slot_mut(&mut self, txn_id: u64) -> Result<&mut TxnSlot, QueryError> {
        let slot = self.slots.get_mut(&txn_id).ok_or_else(|| {
            error(
                crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                "transaction does not exist",
            )
        })?;
        if slot.status != TxnStatus::Active {
            return Err(error(
                crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                "transaction is not active",
            ));
        }
        Ok(slot)
    }

    /// Like [`active_slot_mut`] but accepts either ACTIVE or PREPARED. Used
    /// by the publish-commit / rollback path so an XA branch's PREPARED →
    /// COMMITTED transition can run through the same code as the standard
    /// ACTIVE → COMMITTED transition.
    pub(crate) fn in_flight_slot_mut(&mut self, txn_id: u64) -> Result<&mut TxnSlot, QueryError> {
        let slot = self.slots.get_mut(&txn_id).ok_or_else(|| {
            error(
                crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                "transaction does not exist",
            )
        })?;
        if !slot.status.is_in_flight() {
            return Err(error(
                crate::error::catalog::ER_WARNING_NOT_COMPLETE_ROLLBACK,
                "transaction is not active",
            ));
        }
        Ok(slot)
    }
}

impl LockManager {
    pub(crate) fn unlock_txn(&mut self, txn_id: u64) -> LockRelease {
        self.clear_wait(txn_id);
        let mut released_rows: Vec<(u64, RowId)> = Vec::new();
        // The caller holds the same write latch for this entire operation.
        // Staging each group in another Vec cannot let waiters progress and
        // allocated 1024 entries even for a tiny transaction (and again for
        // the empty final group). Consume the owned log directly, retaining
        // reverse acquisition order and the existing diagnostic group count.
        if let Some(locks) = self.lock_log.remove(&txn_id) {
            const CHUNK_SIZE: usize = 1024;
            self.chunked_release_chunks = self
                .chunked_release_chunks
                .saturating_add(locks.len().div_ceil(CHUNK_SIZE) as u64);
            for lock in locks.into_iter().rev() {
                Self::collect_released_rows(&lock, &mut released_rows);
                Self::release_held_lock(
                    &mut self.locks,
                    &mut self.unique_locks,
                    &mut self.unique_point_tables,
                    &mut self.unique_ranges,
                    &mut self.unique_range_keys,
                    &mut self.table_locks,
                    txn_id,
                    lock,
                );
            }
        }
        // Also release any table locks recorded directly without log (defensive).
        let table_ids: Vec<u64> = self
            .table_locks
            .iter()
            .filter(|(_, state)| state.holders.contains_key(&txn_id))
            .map(|(id, _)| *id)
            .collect();
        for table_id in table_ids {
            if let Some(state) = self.table_locks.get_mut(&table_id) {
                state.holders.remove(&txn_id);
                if state.holders.is_empty() {
                    self.table_locks.remove(&table_id);
                }
            }
        }
        let affected_waiters = self
            .waits_for
            .iter()
            .filter_map(|(waiter, wait)| wait.owners.contains(&txn_id).then_some(*waiter))
            .collect::<Vec<_>>();
        for &waiter in &affected_waiters {
            let remove_wait = self.waits_for.get_mut(&waiter).is_some_and(|wait| {
                wait.owners.retain(|owner| *owner != txn_id);
                wait.owners.is_empty()
            });
            if remove_wait {
                self.waits_for.remove(&waiter);
            }
        }
        LockRelease {
            rows: released_rows,
            affected_waiters,
        }
    }

    fn collect_released_rows(lock: &HeldLock, out: &mut Vec<(u64, RowId)>) {
        match lock {
            HeldLock::Row(table_id, row_id) | HeldLock::RowMode(table_id, row_id, _) => {
                out.push((*table_id, *row_id))
            }
            HeldLock::Rows(table_id, rows) => out.extend(rows.iter().map(|rid| (*table_id, *rid))),
            HeldLock::Unique(_) | HeldLock::UniqueRange(_) | HeldLock::Table(_, _) => {}
        }
    }
}

impl TransactionManager {
    // ====== M1.2 Named Savepoints ======

    pub(crate) fn create_named_savepoint(
        &mut self,
        txn_id: u64,
        name: String,
        savepoint: StatementSavepoint,
    ) -> Result<(), QueryError> {
        let name = name.to_lowercase();
        self.active_slot(txn_id)?;
        let entry = self.named_savepoints.entry(txn_id).or_default();
        // Same-name savepoint replaces previous (MySQL semantics).
        if let Some(idx) = entry.iter().position(|sp| sp.name == name) {
            entry.remove(idx);
        }
        entry.push(NamedSavepoint { name, savepoint });
        Ok(())
    }

    pub(crate) fn lookup_named_savepoint(
        &self,
        txn_id: u64,
        name: &str,
    ) -> Result<StatementSavepoint, QueryError> {
        let name = name.to_lowercase();
        self.named_savepoints
            .get(&txn_id)
            .and_then(|list| list.iter().rev().find(|sp| sp.name == name))
            .map(|sp| sp.savepoint.clone())
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_SP_DOES_NOT_EXIST,
                    format!("SAVEPOINT {} does not exist", name),
                )
            })
    }

    pub(crate) fn release_named_savepoint(
        &mut self,
        txn_id: u64,
        name: &str,
    ) -> Result<(), QueryError> {
        let name = name.to_lowercase();
        let entry = self.named_savepoints.get_mut(&txn_id).ok_or_else(|| {
            error(
                crate::error::catalog::ER_SP_DOES_NOT_EXIST,
                format!("SAVEPOINT {} does not exist", name),
            )
        })?;
        // RELEASE forgets only the named boundary; later savepoints survive.
        if let Some(idx) = entry.iter().rposition(|sp| sp.name == name) {
            entry.remove(idx);
            Ok(())
        } else {
            Err(error(
                crate::error::catalog::ER_SP_DOES_NOT_EXIST,
                format!("SAVEPOINT {} does not exist", name),
            ))
        }
    }

    pub(crate) fn truncate_named_savepoints_after(&mut self, txn_id: u64, savepoint_name: &str) {
        let savepoint_name = savepoint_name.to_lowercase();
        if let Some(entry) = self.named_savepoints.get_mut(&txn_id) {
            if let Some(idx) = entry.iter().rposition(|sp| sp.name == savepoint_name) {
                entry.truncate(idx + 1);
            }
        }
    }

    // ====== M2.2 Table-level intent locks (Oracle TM enqueue) ======
}

impl LockManager {
    // ====== M2.5 Lock wait histogram ======

    pub(crate) fn record_lock_wait_duration(&mut self, duration_ms: u64) {
        self.lock_wait_time_ms = self.lock_wait_time_ms.saturating_add(duration_ms);
        self.lock_wait_time_max_ms = self.lock_wait_time_max_ms.max(duration_ms);
        let bucket = match duration_ms {
            0 => 0,
            1..=9 => 1,
            10..=99 => 2,
            100..=999 => 3,
            1000..=4999 => 4,
            5000..=9999 => 5,
            10000..=29999 => 6,
            _ => 7,
        };
        self.lock_wait_histogram[bucket] = self.lock_wait_histogram[bucket].saturating_add(1);
    }

    pub(crate) fn lock_wait_histogram(&self) -> [u64; 8] {
        self.lock_wait_histogram
    }

    /// P2 recovery migration: drain the legacy in-manager table-lock state
    /// (reconstructed by `from_wal::install_prepared_locks` for prepared-XA
    /// branches) so the caller can reinstate it into the sharded
    /// `ShardedTableLocks` store. Also strips `HeldLock::Table` entries from
    /// `lock_log` so the legacy unlock/serialize paths no longer touch table
    /// locks (the sharded store owns them now). Returns `(table_id, txn_id,
    /// mode)` triples. Single-threaded startup path only.
    pub(crate) fn drain_table_locks(&mut self) -> Vec<(u64, u64, TableLockMode)> {
        let mut out = Vec::new();
        for (table_id, state) in self.table_locks.drain() {
            for (txn_id, mode) in state.holders {
                out.push((table_id, txn_id, mode));
            }
        }
        for log in self.lock_log.values_mut() {
            log.retain(|held| !matches!(held, HeldLock::Table(_, _)));
        }
        self.lock_log.retain(|_, log| !log.is_empty());
        out
    }

    /// P3 recovery migration: drain the legacy in-manager ROW-lock state
    /// (reconstructed by `from_wal`) so the caller can reinstate it into the
    /// sharded `ShardedRowLocks` store. Strips Row/RowMode/Rows entries from
    /// `lock_log` (Unique entries stay) and clears the `locks` map. Returns
    /// `(txn_id, HeldLock)` row entries. Single-threaded startup path only.
    pub(crate) fn drain_row_locks(&mut self) -> Vec<(u64, HeldLock)> {
        let mut out = Vec::new();
        for (txn_id, log) in self.lock_log.iter_mut() {
            let mut keep = Vec::with_capacity(log.len());
            for held in log.drain(..) {
                match held {
                    HeldLock::Row(_, _) | HeldLock::RowMode(_, _, _) | HeldLock::Rows(_, _) => {
                        out.push((*txn_id, held));
                    }
                    other => keep.push(other),
                }
            }
            *log = keep;
        }
        self.lock_log.retain(|_, log| !log.is_empty());
        self.locks.clear();
        out
    }
}

#[cfg(test)]
mod group_commit_tests {
    use super::*;
    use std::cell::RefCell;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn make_err() -> QueryError {
        error(crate::error::catalog::ER_GET_ERRNO, "induced sync failure")
    }

    #[test]
    fn index_repair_scopes_preserve_collisions_and_partial_rollback() {
        let mut txns = TransactionManager::default();
        let first = IndexRepairKey {
            table_id: 1,
            index_name: "primary".into(),
            key: IndexKey::from(vec![crate::storage::KeyPart::Int(7)]),
        };
        let scope = IndexRepairEpochs::scope(1, "primary");
        assert_eq!(scope, IndexRepairEpochs::scope(1, "PRIMARY"));
        let collision = (2..10_000)
            .find(|&id| IndexRepairEpochs::scope(id, "other") == scope)
            .unwrap();
        let second = IndexRepairKey {
            table_id: collision,
            index_name: "other".into(),
            key: first.key.clone(),
        };
        let unrelated = (2..10_000)
            .find(|&id| IndexRepairEpochs::scope(id, "other") != scope)
            .unwrap();
        let epochs = Arc::clone(&txns.index_repair_epochs);
        let version = epochs.version(1, "PRIMARY");
        let before = version.load(Ordering::Acquire);
        for id in 1..=3 {
            txns.begin_with_id(id, 0, false, true, 1).unwrap();
            let key = if id == 3 { &second } else { &first };
            txns.register_index_repairs(id, [key.clone()], RowId::new(1, id as u16))
                .unwrap();
            assert_eq!(version.load(Ordering::Acquire) & 1, 1);
            assert_eq!(
                epochs.version(unrelated, "other").load(Ordering::Acquire),
                0
            );
        }
        // Removing one owner must retain the same key's proof, and removing
        // that key must retain a different index which hashes to this scope.
        txns.clear_index_repairs(1);
        assert_eq!(version.load(Ordering::Acquire) & 1, 1);
        txns.clear_index_repairs(2);
        assert_eq!(version.load(Ordering::Acquire) & 1, 1);
        txns.clear_index_repairs(3);
        let after = version.load(Ordering::Acquire);
        assert_eq!(after & 1, 0);
        assert_ne!(
            before, after,
            "empty-directory ABA must invalidate the probe"
        );
        txns.clear_index_repairs(3);
        assert_eq!(after, version.load(Ordering::Acquire));
    }

    #[test]
    fn failed_begin_discard_restores_transaction_counters() {
        let mut txns = TransactionManager::default();
        txns.begin_with_id(1, 0, false, true, 1).unwrap();
        txns.mark_writer(1).unwrap();
        assert_eq!(txns.active_count, 1);
        assert_eq!(txns.active_write_count, 1);
        txns.discard_failed_begin(1);
        assert_eq!(txns.active_count, 0);
        assert_eq!(txns.active_write_count, 0);
        assert!(txns.slot(1).is_none());
    }

    #[test]
    fn index_repair_version_detects_empty_directory_aba() {
        let mut txns = TransactionManager::default();
        let version = Arc::clone(&txns.index_repair_version);
        let before = version.load(Ordering::Acquire);
        assert_eq!(before & 1, 0);
        let key = IndexRepairKey {
            table_id: 1,
            index_name: "primary".into(),
            key: IndexKey::from(vec![crate::storage::KeyPart::Int(7)]),
        };
        for id in [1, 2] {
            txns.begin_with_id(id, 0, false, true, 1).unwrap();
            txns.register_index_repairs(id, [key.clone()], RowId::new(1, id as u16))
                .unwrap();
            assert_eq!(version.load(Ordering::Acquire) & 1, 1);
        }
        txns.clear_index_repairs(1);
        assert_eq!(version.load(Ordering::Acquire) & 1, 1);
        txns.clear_index_repairs(2);
        let after = version.load(Ordering::Acquire);
        assert_eq!(after & 1, 0);
        assert_ne!(
            before, after,
            "register + rollback must invalidate an optimistic probe"
        );
        txns.clear_index_repairs(2);
        assert_eq!(after, version.load(Ordering::Acquire));
    }

    #[test]
    fn index_repair_key_created_after_the_snapshot_is_not_live() {
        // One row moved twice after the snapshot: 10 -> 20 by txn 1, then
        // 20 -> 30 by txn 2. Only 10 belongs to the snapshot's version.
        let mut txns = TransactionManager::default();
        let key = |value| IndexRepairKey {
            table_id: 1,
            index_name: "ik".into(),
            key: IndexKey::from(vec![crate::storage::KeyPart::Int(value)]),
        };
        let row = RowId::new(3, 4);
        txns.begin_with_id(1, 0, false, true, 1).unwrap();
        txns.register_index_repairs(1, [key(10)], row).unwrap();
        txns.publish_commit(1, 6).unwrap();
        txns.begin_with_id(2, 0, false, true, 1).unwrap();
        txns.register_index_repairs(2, [key(20)], row).unwrap();
        let live = |txns: &TransactionManager, snapshot_scn| {
            let candidates = txns.index_repairs.candidates(1, "ik", None, None, |_| true);
            txns.live_repair_entries(
                TxnView::read_only(snapshot_scn, IsolationLevel::ReadCommitted),
                candidates,
                |_| None,
            )
        };
        assert_eq!(live(&txns, 5), vec![(key(10).key, row)]);
        assert_eq!(live(&txns, 6), vec![(key(20).key, row)]);
        txns.clear_index_repairs(2);
        assert!(live(&txns, 6).is_empty());
    }

    #[test]
    fn index_repair_multi_key_origins_remain_conservative_across_partial_updates() {
        let mut txns = TransactionManager::default();
        let row = RowId::new(3, 4);
        let key = |value| IndexRepairKey {
            table_id: 1,
            index_name: "multi".into(),
            key: IndexKey::from(vec![crate::storage::KeyPart::Int(value)]),
        };
        for writer in [1, 2] {
            txns.begin_with_id(writer, 0, false, true, 1).unwrap();
            let head = txns.slot(writer).unwrap().undo_head.clone();
            // Keys retained by the first update can be retired by a later
            // writer without having been created by that earlier writer.
            txns.index_repairs
                .register(
                    writer,
                    [key(writer as i64 * 10), key(writer as i64 * 10 + 1)]
                        .into_iter()
                        .map(|key| (key, row, false)),
                    &head,
                    &txns.index_repair_epochs,
                    &txns.index_repair_version,
                )
                .unwrap();
            txns.publish_commit(writer, writer + 5).unwrap();
        }
        let candidates = txns
            .index_repairs
            .candidates(1, "multi", None, None, |_| true);
        assert_eq!(candidates.len(), 4);
        assert!(candidates.iter().all(|candidate| candidate.creator == 0));
        txns.truncate_index_repairs(2, 1);
        assert_eq!(
            txns.index_repairs
                .candidates(1, "multi", None, None, |_| true)
                .len(),
            3
        );
        txns.clear_index_repairs(2);
        assert_eq!(
            txns.index_repairs
                .candidates(1, "multi", None, None, |_| true)
                .len(),
            2
        );
    }

    #[test]
    fn index_repair_rollback_epoch_moves_on_rollback_not_on_purge() {
        let mut txns = TransactionManager::default();
        let epochs = Arc::clone(&txns.index_repair_epochs);
        let key = IndexRepairKey {
            table_id: 1,
            index_name: "ik".into(),
            key: IndexKey::from(vec![crate::storage::KeyPart::Int(7)]),
        };
        let epoch = || epochs.rollback_epoch(1, "ik");
        txns.begin_with_id(1, 0, false, true, 1).unwrap();
        txns.register_index_repairs(1, [key.clone()], RowId::new(1, 1))
            .unwrap();
        let before = epoch();
        txns.rollback(1).unwrap();
        assert_ne!(before, epoch(), "rollback removal must move the epoch");

        txns.begin_with_id(2, 0, false, true, 1).unwrap();
        txns.register_index_repairs(2, [key], RowId::new(1, 2))
            .unwrap();
        txns.publish_commit(2, 9).unwrap();
        let before = epoch();
        let writers = txns.obsolete_index_repair_writers(None);
        txns.index_repairs.purge_finished(
            &writers,
            &txns.index_repair_epochs,
            &txns.index_repair_version,
        );
        assert_eq!(txns.index_repairs.log_len(2), 0);
        assert_eq!(
            before,
            epoch(),
            "purge of a committed writer is not a rollback"
        );
    }

    #[test]
    fn index_repair_first_registration_and_savepoint_dedup() {
        let mut txns = TransactionManager::default();
        let key = IndexRepairKey {
            table_id: 1,
            index_name: "secondary".into(),
            key: IndexKey::from(vec![crate::storage::KeyPart::Int(7)]),
        };
        for id in 1..=100 {
            txns.begin_with_id(id, 0, false, true, 1).unwrap();
            txns.register_index_repairs(id, [key.clone()], RowId::new(1, 1))
                .unwrap();
        }
        assert_eq!(txns.index_repairs.entry_count(&key), 100);
        txns.register_index_repairs(100, [key.clone()], RowId::new(1, 1))
            .unwrap();
        assert_eq!(txns.index_repairs.entry_count(&key), 100);
        txns.register_index_repairs(100, [key.clone()], RowId::new(1, 2))
            .unwrap();
        assert_eq!(txns.index_repairs.entry_count(&key), 101);
        txns.truncate_index_repairs(100, 0);
        assert_eq!(txns.index_repairs.entry_count(&key), 99);
        txns.register_index_repairs(100, [key.clone()], RowId::new(1, 1))
            .unwrap();
        assert_eq!(txns.index_repairs.entry_count(&key), 100);
        assert_eq!(txns.index_repairs.log_len(100), 1);
    }

    #[test]
    fn savepoint_match_checks_undo_writer_and_index_repairs() {
        let mut txns = TransactionManager::default();
        txns.begin_with_id(1, 0, false, true, 1).unwrap();
        let before = txns.statement_savepoint(1, 0, 0).unwrap();
        assert!(txns.statement_savepoint_matches(1, &before).unwrap());
        txns.mark_writer(1).unwrap();
        assert!(!txns.statement_savepoint_matches(1, &before).unwrap());
        txns.restore_statement_savepoint(1, &before).unwrap();
        assert_eq!(txns.active_write_count, 0);
        assert!(txns.statement_savepoint_matches(1, &before).unwrap());

        txns.mark_writer(1).unwrap();
        txns.active_slot(1).unwrap().undo_head.set(Some(10));
        let after = txns.statement_savepoint(1, 0, 0).unwrap();
        assert!(txns.statement_savepoint_matches(1, &after).unwrap());
        txns.active_slot(1).unwrap().undo_head.set(Some(11));
        assert!(!txns.statement_savepoint_matches(1, &after).unwrap());
        txns.restore_statement_savepoint(1, &after).unwrap();
        let key = IndexRepairKey {
            table_id: 1,
            index_name: "secondary".into(),
            key: IndexKey::from(vec![crate::storage::KeyPart::Int(7)]),
        };
        txns.register_index_repairs(1, [key.clone()], RowId::new(1, 0))
            .unwrap();
        assert!(!txns.statement_savepoint_matches(1, &after).unwrap());
        txns.restore_statement_savepoint(1, &after).unwrap();
        assert!(txns.statement_savepoint_matches(1, &after).unwrap());
        assert_eq!(txns.index_repairs.entry_count(&key), 0);
        assert_eq!(txns.active_write_count, 1);
    }

    #[test]
    fn deadlock_detection_traverses_every_shared_lock_owner() {
        let mut manager = LockManager::default();
        manager.waits_for.insert(
            2,
            LockWait {
                owners: smallvec::smallvec![3],
                resource: LockWaitResource::Table(20),
                started_unix_ms: 0,
                mode: None,
            },
        );

        // Transaction 3 is about to wait for shared owners [1, 2]. The first
        // owner is not part of a cycle, while the second waits for 3. A graph
        // that records/traverses only owner 1 misses the real 2 -> 3 -> 2
        // deadlock.
        assert!(manager.detect_deadlock(3, &[1, 2]));
        assert!(!manager.detect_deadlock(4, &[1, 2]));

        // Releasing one shared holder removes only that graph edge. Dropping
        // the complete wait here would temporarily hide the still-real 3 -> 2
        // dependency; retaining owner 1 would allow a later false cycle.
        manager.waits_for.insert(
            3,
            LockWait {
                owners: smallvec::smallvec![1, 2],
                resource: LockWaitResource::Row(30, RowId::new(1, 1)),
                started_unix_ms: 0,
                mode: Some(LockMode::Exclusive),
            },
        );
        manager.unlock_txn(1);
        assert_eq!(manager.waits_for[&3].owners.as_slice(), &[2]);
    }

    #[test]
    fn unique_integer_ranges_preserve_point_conflicts_and_release() {
        use crate::storage::KeyPart;
        for (unsigned, first, last) in [
            (false, -10, 10),
            (true, i128::from(u64::MAX) - 20, i128::from(u64::MAX)),
        ] {
            for status in [TxnStatus::Active, TxnStatus::Prepared] {
                let mut manager = LockManager::default();
                assert!(manager
                    .try_lock_integer_range(1, 9, "Uk", unsigned, first, last)
                    .unwrap());
                assert_eq!(manager.unique_key_lock_count(), 21);
                assert_eq!(manager.lock_log_len(1), 1);
                let key = |n: i128| {
                    IndexKey::from(vec![if unsigned {
                        KeyPart::UInt(n as u64)
                    } else {
                        KeyPart::Int(n as i64)
                    }])
                };
                for n in [first, first + 10, last] {
                    assert!(!manager.unique_key_uncontended(2, 9, "UK", &key(n), &|_| Some(status)));
                    assert!(manager.unique_key_uncontended(1, 9, "uk", &key(n), &|_| Some(status)));
                    manager
                        .acquire_unique_key_lock(1, 9, "uk", &key(n), true, &|_| Some(status))
                        .unwrap();
                    assert_eq!(manager.lock_log_len(1), 1);
                }
                assert!(manager.unique_key_uncontended(2, 10, "uk", &key(first), &|_| Some(status)));
                assert!(
                    manager.unique_key_uncontended(2, 9, "other", &key(first), &|_| Some(status))
                );
                let error = manager
                    .acquire_unique_key_lock(2, 9, "uk", &key(first), true, &|_| Some(status))
                    .unwrap_err();
                assert!(crate::storage::core::is_lock_wait_pending_error(&error));
                assert_eq!(manager.unlock_txn(1).affected_waiters, vec![2]);
                assert_eq!(manager.unique_key_lock_count(), 0);
                assert!(manager.unique_ranges.is_empty());
                manager
                    .acquire_unique_key_lock(2, 9, "uk", &key(first), true, &|_| Some(status))
                    .unwrap();
            }
        }
    }

    #[test]
    fn unique_integer_ranges_preserve_savepoints_deadlocks_and_point_fallback() {
        use crate::storage::KeyPart;
        let key = |n| IndexKey::from(vec![KeyPart::Int(n)]);
        let active = |_| Some(TxnStatus::Active);
        let mut manager = LockManager::default();
        assert!(manager
            .try_lock_integer_range(1, 9, "uk", false, 1, 10)
            .unwrap());
        let saved = manager.lock_log_len(1);
        assert!(manager
            .try_lock_integer_range(1, 9, "uk", false, 20, 30)
            .unwrap());
        manager.restore_savepoint_locks(1, saved);
        assert_eq!(manager.unique_key_lock_count(), 10);
        assert!(manager.unique_key_uncontended(2, 9, "uk", &key(20), &active));
        assert!(manager
            .try_lock_integer_range(2, 9, "uk", false, 20, 30)
            .unwrap());
        assert!(manager
            .acquire_unique_key_lock(1, 9, "uk", &key(20), true, &active)
            .is_err());
        let deadlock = manager
            .acquire_unique_key_lock(2, 9, "uk", &key(1), true, &active)
            .unwrap_err();
        assert!(deadlock.is(crate::error::catalog::ER_LOCK_DEADLOCK));
        manager.unlock_txn(1);
        manager.unlock_txn(2);
        manager
            .acquire_unique_key_lock(3, 9, "uk", &key(5), true, &active)
            .unwrap();
        assert!(
            manager
                .try_lock_integer_range(4, 10, "uk", false, 1, 10)
                .unwrap(),
            "point reservations on another table must not disable compression"
        );
        assert!(!manager
            .try_lock_integer_range(4, 9, "uk", false, 1, 10)
            .unwrap());
        assert!(manager
            .acquire_unique_key_lock(4, 9, "uk", &key(5), true, &active)
            .is_err());
    }

    #[test]
    fn unique_point_table_counters_spill_and_return_to_inline() {
        let mut tables = UniquePointTables::default();
        for table in 0..12 {
            tables.increment(table);
            tables.increment(table);
        }
        assert!(tables.spill.is_some());
        for table in (0..12).rev() {
            tables.decrement(table);
            assert!(tables.contains_key(&table));
            tables.decrement(table);
            assert!(!tables.contains_key(&table));
        }
        assert!(tables.spill.is_none());
        tables.increment(99);
        assert!(tables.spill.is_none());
        assert!(tables.contains_key(&99));
        tables.decrement(99);
        assert!(!tables.contains_key(&99));
    }

    #[test]
    fn unique_integer_range_overlap_falls_back_before_capacity_check() {
        let mut manager = LockManager::default();
        crate::util::env::with_vars(&[("MYDB_MAX_LOCKS", Some("10"))], || {
            assert!(manager
                .try_lock_integer_range(1, 9, "uk", false, 1, 10)
                .unwrap());
            assert!(!manager
                .try_lock_integer_range(1, 9, "uk", false, 1, 10)
                .unwrap());
            assert!(manager
                .try_lock_integer_range(2, 9, "uk", false, 11, 20)
                .unwrap_err()
                .is(crate::error::catalog::ER_LOCK_TABLE_FULL));
            assert_eq!(manager.unique_key_lock_count(), 10);
        });
    }

    #[test]
    fn unique_batch_stale_takeover_survives_previous_owner_release() {
        use crate::storage::KeyPart;
        let keys = vec![
            IndexKey::from(vec![KeyPart::Int(-7)]),
            IndexKey::from(vec![KeyPart::UInt(u64::MAX)]),
            IndexKey::from(vec![KeyPart::String("字🙂".into()), KeyPart::Int(9)]),
            IndexKey::from(vec![KeyPart::Bytes(vec![0, 255])]),
        ];
        for state in [
            Some(TxnStatus::Committed),
            Some(TxnStatus::RolledBack),
            None,
        ] {
            let mut manager = LockManager::default();
            let batch = || keys.iter().cloned().map(|key| (9, "Uk", key));
            manager
                .lock_unique_keys(1, batch(), &|_| Some(TxnStatus::Active))
                .unwrap();
            manager.lock_unique_keys(2, batch(), &|_| state).unwrap();
            assert_eq!(manager.unique_locks.len(), keys.len());
            assert_eq!(manager.lock_log_len(2), keys.len());
            // A retry by the current owner must not append duplicate locks.
            manager
                .lock_unique_keys(2, batch(), &|_| Some(TxnStatus::Active))
                .unwrap();
            assert_eq!(manager.lock_log_len(2), keys.len());
            manager.unlock_txn(1);
            assert_eq!(manager.unique_locks.len(), keys.len());
            for key in &keys {
                assert!(!manager
                    .unique_key_uncontended(3, 9, "UK", key, &|_| Some(TxnStatus::Prepared)));
            }
            manager.unlock_txn(2);
            assert!(manager.unique_locks.is_empty());
        }
    }

    #[test]
    fn unique_batch_conflict_preserves_prefix_and_savepoint_lock_ownership() {
        use crate::storage::KeyPart;
        let key = |n| IndexKey::from(vec![KeyPart::Int(n)]);
        for owner_status in [TxnStatus::Active, TxnStatus::Prepared] {
            let mut manager = LockManager::default();
            let status = |_| Some(owner_status);
            manager
                .acquire_unique_key_lock(1, 9, "uk", &key(4), true, &status)
                .unwrap();
            manager
                .lock_unique_keys(2, [(9, "uk", key(1))], &status)
                .unwrap();
            let savepoint = manager.lock_log_len(2);
            // Distinct table/index identities remain independent, and the
            // first blocked key must leave the acquired prefix rollbackable.
            let batch = [
                (10, "uk", key(4)),
                (9, "other", key(4)),
                (9, "UK", key(3)),
                (9, "uk", key(4)),
                (9, "uk", key(5)),
            ];
            let err = manager.lock_unique_keys(2, batch, &status).unwrap_err();
            assert!(crate::storage::core::is_lock_wait_pending_error(&err));
            assert_eq!(manager.lock_log_len(2), savepoint + 3);
            assert_eq!(manager.unique_locks.len(), 5);
            assert!(manager.unique_key_uncontended(3, 9, "uk", &key(5), &status));
            manager.restore_savepoint_locks(2, savepoint);
            assert_eq!(manager.unique_locks.len(), 2);
            assert!(!manager.unique_key_uncontended(3, 9, "uk", &key(1), &status));
            assert!(!manager.unique_key_uncontended(3, 9, "uk", &key(4), &status));
            assert!(manager.unique_key_uncontended(3, 9, "uk", &key(3), &status));
            manager.unlock_txn(2);
            manager.unlock_txn(1);
            assert!(manager.unique_locks.is_empty());
        }
        let mut manager = LockManager::default();
        manager
            .lock_unique_keys(1, [], &|_| Some(TxnStatus::Active))
            .unwrap();
        assert!(manager.lock_log.is_empty());
    }

    #[test]
    fn unique_lock_release_preserves_waiters_across_group_boundaries() {
        for count in [0usize, 1, 1024, 1025] {
            let mut manager = LockManager::default();
            let active = |_txn_id| Some(TxnStatus::Active);
            for value in 0..count {
                let key = IndexKey::from(vec![crate::storage::KeyPart::Int(value as i64)]);
                manager
                    .acquire_unique_key_lock(1, 9, "uk", &key, true, &active)
                    .unwrap();
            }
            if count != 0 {
                let key = IndexKey::from(vec![crate::storage::KeyPart::Int(count as i64 - 1)]);
                let pending = manager
                    .check_unique_key_lock(2, 9, "uk", &key, &active)
                    .unwrap_err();
                assert!(crate::storage::core::is_lock_wait_pending_error(&pending));
            }
            let release = manager.unlock_txn(1);
            assert!(manager.unique_locks.is_empty());
            assert_eq!(manager.lock_log_len(1), 0);
            assert!(release.rows.is_empty());
            assert_eq!(
                release.affected_waiters,
                if count == 0 { vec![] } else { vec![2] }
            );
            assert_eq!(manager.chunked_release_chunks, count.div_ceil(1024) as u64);
        }
    }

    #[test]
    fn borrowed_unique_probe_preserves_typed_keys_owners_and_name_normalization() {
        use crate::storage::KeyPart;
        let parts = vec![
            KeyPart::Null,
            KeyPart::Int(-7),
            KeyPart::UInt(u64::MAX),
            KeyPart::Float32(123),
            KeyPart::Float64(456),
            KeyPart::Decimal {
                value: -12345,
                scale: 2,
            },
            KeyPart::DecimalWide {
                value: num_bigint::BigInt::from(123),
                scale: 4,
            },
            KeyPart::Bytes(vec![0, 255]),
            KeyPart::String("例子".into()),
            KeyPart::CollatedString {
                collation_id: 45,
                value: "A".into(),
            },
            KeyPart::Date(17),
            KeyPart::Time(-123),
            KeyPart::DateTime(123456),
            KeyPart::Year(120),
        ];
        for key in parts
            .iter()
            .cloned()
            .map(|part| IndexKey::from(vec![part]))
            .chain(std::iter::once(IndexKey::from(parts.clone())))
        {
            let mut manager = LockManager::default();
            let active = |_| Some(TxnStatus::Active);
            assert!(manager.unique_key_uncontended(2, 9, "Uk", &key, &active));
            manager
                .acquire_unique_key_lock(1, 9, "Uk", &key, true, &active)
                .unwrap();
            assert!(manager.unique_key_uncontended(1, 9, "UK", &key, &active));
            assert!(!manager.unique_key_uncontended(2, 9, "uk", &key, &active));
            assert!(
                !manager.unique_key_uncontended(2, 9, "UK", &key, &|_| Some(TxnStatus::Prepared))
            );
            assert!(manager.unique_key_uncontended(2, 10, "uk", &key, &active));
            assert!(manager.unique_key_uncontended(2, 9, "other", &key, &active));
            for status in [
                Some(TxnStatus::Committed),
                Some(TxnStatus::RolledBack),
                None,
            ] {
                assert!(manager.unique_key_uncontended(2, 9, "UK", &key, &|_| status));
            }
            assert!(manager.waits_for.is_empty());
            assert_eq!(manager.lock_log_len(1), 1);
            assert_eq!(manager.unique_locks.len(), 1);
            assert!(manager
                .check_unique_key_lock(2, 9, "uk", &key, &active)
                .is_err());
            assert_eq!(manager.unlock_txn(1).affected_waiters, vec![2]);
        }
    }

    #[test]
    fn unique_key_owner_release_reports_exact_waiter() {
        let mut manager = LockManager::default();
        let key = IndexKey::from(vec![crate::storage::KeyPart::Int(7)]);
        let active = |_txn_id| Some(TxnStatus::Active);
        manager
            .acquire_unique_key_lock(1, 9, "uk", &key, true, &active)
            .unwrap();
        let pending = manager
            .acquire_unique_key_lock(2, 9, "uk", &key, false, &active)
            .unwrap_err();
        assert!(crate::storage::core::is_lock_wait_pending_error(&pending));

        let release = manager.unlock_txn(1);
        assert!(release.rows.is_empty());
        assert_eq!(release.affected_waiters, vec![2]);
    }

    #[test]
    fn pre_publish_target_preserves_active_committers() {
        assert_eq!(group_commit_target(0, 8), 1);
        assert_eq!(group_commit_target(1, 8), 1);
        assert_eq!(group_commit_target(2, 8), 2);
        assert_eq!(group_commit_target(3, 8), 3);
        assert_eq!(group_commit_target(20, 8), 8);
    }

    /// When the leader's sync errors out, late waiters that piggybacked on the
    /// failed batch (sync_waiters) must remain accounted for so the next
    /// batch's pending_waiters reflects them. Otherwise the next leader's
    /// window collapses early and fairness/throughput degrade.
    #[test]
    fn error_path_preserves_sync_waiter_accounting() {
        let coord = Arc::new(GroupCommitCoordinator::default());
        let attempts = Arc::new(std::sync::Mutex::new(0u32));
        // Block the leader's first sync_op until the follower has registered
        // as sync_waiter, so the recovery path is exercised deterministically.
        let follower_registered =
            Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));

        let coord_leader = coord.clone();
        let attempts_leader = attempts.clone();
        let registered_for_leader = follower_registered.clone();
        let leader = thread::spawn(move || {
            coord_leader.sync_lsn_with(10, Duration::ZERO, 1, false, |lsn| {
                let mut a = attempts_leader.lock().unwrap();
                *a += 1;
                let attempt = *a;
                drop(a);
                if attempt == 1 {
                    let (lock, cv) = &*registered_for_leader;
                    let mut done = lock.lock().unwrap();
                    while !*done {
                        let (next, _) = cv.wait_timeout(done, Duration::from_secs(2)).unwrap();
                        done = next;
                    }
                    Err(make_err())
                } else {
                    Ok((SyncOutcome::Synced, lsn))
                }
            })
        });

        // Wait until the leader is mid-sync (sync_target_lsn=10).
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            {
                let st = coord.state.lock();
                if st.syncing && st.sync_target_lsn == 10 {
                    break;
                }
            }
            if Instant::now() >= deadline {
                panic!("leader never entered syncing state");
            }
            thread::sleep(Duration::from_micros(100));
        }

        // Follower registers as sync_waiter (lsn <= sync_target_lsn).
        let coord_follower = coord.clone();
        let follower = thread::spawn(move || {
            coord_follower.sync_lsn_with(5, Duration::ZERO, 1, false, |lsn| {
                Ok((SyncOutcome::Synced, lsn))
            })
        });

        // Wait until follower is registered as sync_waiter, then unblock leader.
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            {
                let st = coord.state.lock();
                if st.sync_waiters >= 1 {
                    break;
                }
            }
            if Instant::now() >= deadline {
                panic!("follower never registered as sync_waiter");
            }
            thread::sleep(Duration::from_micros(100));
        }
        {
            let (lock, cv) = &*follower_registered;
            *lock.lock().unwrap() = true;
            cv.notify_all();
        }

        let leader_result = leader.join().unwrap();
        let follower_result = follower.join().unwrap();

        // Leader returns the induced error; the follower picks up leadership
        // and completes the second batch successfully.
        assert!(leader_result.is_err(), "leader returns the induced error");
        assert!(
            follower_result.is_ok(),
            "follower must complete: {:?}",
            follower_result
        );
        // Leader called sync_op once (errored). Follower has its own sync_op
        // and called it once successfully.
        assert_eq!(*attempts.lock().unwrap(), 1, "leader's sync_op runs once");

        let st = coord.state.lock();
        assert!(st.durable_lsn >= 5, "durable_lsn={}", st.durable_lsn);
        assert!(st.batches >= 1, "expected at least one successful batch");
        // The fix folds sync_waiters back into pending_waiters on error, so
        // the follower's resulting batch should record both itself and the
        // recovered leader's pending_waiter contribution. Without the fix
        // (sync_waiters dropped), waiters would be 1; with the fix, >= 2.
        assert!(
            st.waiters >= 2,
            "recovered waiter must be credited; waiters={}",
            st.waiters
        );
    }

    /// Sanity: success path advances durable_lsn and tallies stats.
    #[test]
    fn success_path_advances_durable_lsn() {
        let coord = GroupCommitCoordinator::default();
        let calls = RefCell::new(0u32);
        coord
            .sync_lsn_with(7, Duration::ZERO, 1, false, |lsn| {
                *calls.borrow_mut() += 1;
                Ok((SyncOutcome::Synced, lsn))
            })
            .unwrap();
        assert_eq!(*calls.borrow(), 1);
        let st = coord.state.lock();
        assert_eq!(st.durable_lsn, 7);
        assert_eq!(st.batches, 1);
        assert_eq!(st.waiters, 1);
    }

    #[test]
    fn durable_follower_returns_without_reacquiring_coordinator_mutex() {
        let coord = Arc::new(GroupCommitCoordinator::default());
        let (release, wait) = std::sync::mpsc::channel();
        let leader_coord = Arc::clone(&coord);
        let leader = thread::spawn(move || {
            leader_coord.sync_lsn_with(1, Duration::ZERO, 1, false, |_| {
                wait.recv().unwrap();
                Ok((SyncOutcome::Synced, 2))
            })
        });
        let deadline = Instant::now() + Duration::from_secs(3);
        while coord.state.lock().sync_target_lsn != 1 {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        let (done, completed) = std::sync::mpsc::channel();
        let follower_coord = Arc::clone(&coord);
        let follower = thread::spawn(move || {
            let result = follower_coord.sync_lsn_with_attribution(
                2,
                Duration::ZERO,
                1,
                false,
                false,
                |_| panic!("the published watermark already covers this follower"),
            );
            done.send(result).unwrap();
        });
        while coord.state.lock().parked.len() != 1 {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        // An independent durability barrier may cover this waiter while the
        // leader is still in flight. Keep the coordinator locked during its
        // wake to verify that returning success uses the atomic watermark.
        let mut state = coord.state.lock();
        state.durable_lsn = 2;
        coord.durable_lsn_pub.store(2, Ordering::Release);
        let ready = GroupCommitCoordinator::completed_waiters(&mut state, 1);
        for signal in ready {
            signal.notify();
        }
        let result = completed.recv_timeout(Duration::from_secs(2));
        drop(state);
        release.send(()).unwrap();
        leader.join().unwrap().unwrap();
        follower.join().unwrap();
        result
            .expect("a durable follower must not queue at the coordinator mutex")
            .unwrap();
    }

    #[test]
    fn failed_sync_hands_leadership_to_all_queued_followers() {
        let coord = Arc::new(GroupCommitCoordinator::default());
        let (release, wait) = std::sync::mpsc::channel();
        let leader_coord = Arc::clone(&coord);
        let leader = thread::spawn(move || {
            leader_coord.sync_lsn_with(1, Duration::ZERO, 1, false, |_| {
                wait.recv().unwrap();
                Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "induced sync failure",
                ))
            })
        });
        let deadline = Instant::now() + Duration::from_secs(3);
        while coord.state.lock().sync_target_lsn != 1 {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        let (done, completed) = std::sync::mpsc::channel();
        let mut followers = Vec::new();
        for lsn in 2..=9 {
            let coord = Arc::clone(&coord);
            let done = done.clone();
            followers.push(thread::spawn(move || {
                let result = coord.sync_lsn_with(lsn, Duration::ZERO, 1, false, |_| {
                    Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "induced sync failure",
                    ))
                });
                done.send(result.is_err()).unwrap();
            }));
        }
        while coord.state.lock().parked.len() != 8 {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        release.send(()).unwrap();
        assert!(leader.join().unwrap().is_err());
        for _ in 0..8 {
            assert!(completed.recv_timeout(Duration::from_secs(3)).unwrap());
        }
        for follower in followers {
            follower.join().unwrap();
        }
        assert!(coord.state.lock().parked.is_empty());
    }

    #[test]
    fn group_commit_completion_queue_handles_repeated_concurrent_followers() {
        let coord = Arc::new(GroupCommitCoordinator::default());
        let appended = Arc::new(AtomicU64::new(0));
        let durable = Arc::new(AtomicU64::new(0));
        let start = Arc::new(std::sync::Barrier::new(40));
        let (completed, receiver) = std::sync::mpsc::channel();
        let mut workers = Vec::new();
        for _ in 0..40 {
            let coord = coord.clone();
            let appended = appended.clone();
            let durable = durable.clone();
            let start = start.clone();
            let completed = completed.clone();
            workers.push(thread::spawn(move || {
                start.wait();
                for _ in 0..32 {
                    let lsn = appended.fetch_add(1, Ordering::AcqRel) + 1;
                    coord
                        .sync_lsn_with(lsn, Duration::ZERO, 4, false, |_| {
                            let high_water = appended.load(Ordering::Acquire);
                            thread::sleep(Duration::from_micros(50));
                            durable.fetch_max(high_water, Ordering::Release);
                            Ok((SyncOutcome::Synced, high_water))
                        })
                        .unwrap();
                    assert!(durable.load(Ordering::Acquire) >= lsn);
                }
                completed.send(()).unwrap();
            }));
        }
        for _ in 0..40 {
            receiver.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(durable.load(Ordering::Acquire), 1280);
    }

    #[test]
    fn stall_attribution_can_stay_off_on_hot_path() {
        let coord = GroupCommitCoordinator::default();
        coord
            .sync_lsn_with_attribution(7, Duration::from_millis(1), 2, false, false, |lsn| {
                thread::sleep(Duration::from_millis(1));
                Ok((SyncOutcome::Synced, lsn))
            })
            .unwrap();
        let st = coord.state.lock();
        assert_eq!(st.leader_wait_total_us, 0);
        assert_eq!(st.follower_wait_total_us, 0);
        assert_eq!(st.sync_op_total_us, 0);
        assert_eq!(st.leader_wait_max_us, 0);
        assert_eq!(st.follower_wait_max_us, 0);
        assert_eq!(st.sync_op_max_us, 0);
    }

    #[test]
    fn stall_attribution_records_wait_and_sync_time_when_enabled() {
        let coord = GroupCommitCoordinator::default();
        coord
            .sync_lsn_with_attribution(7, Duration::from_millis(1), 2, false, true, |lsn| {
                thread::sleep(Duration::from_millis(1));
                Ok((SyncOutcome::Synced, lsn))
            })
            .unwrap();
        let st = coord.state.lock();
        assert!(st.leader_wait_total_us > 0, "{st:?}");
        assert!(st.sync_op_total_us > 0, "{st:?}");
        assert!(st.leader_wait_max_us > 0, "{st:?}");
        assert!(st.sync_op_max_us > 0, "{st:?}");
    }

    /// B8 — pure-function check for `adaptive_window_for_state`. Locks in
    /// the curve so a future tweak that breaks the lo/hi clamp shows up
    /// as a unit-test failure instead of a silent regression.
    ///
    /// Policy (current):
    /// * cold start (avg == 0) → configured (let first batch grow).
    /// * 0 < avg < 1.5 (batching is fragile) → min(configured, 100us).
    /// * avg >= 1.5 → configured.
    #[test]
    fn adaptive_window_clamps_low_and_high() {
        let cfg = Duration::from_micros(500);
        // Cold start (avg = 0) returns the configured window untouched —
        // matches the static-path behaviour the optimizer depends on.
        assert_eq!(adaptive_window_for_state_enabled(cfg, 0.0, true), cfg);
        // EMA has converged to ~1 (e.g. fragile batching). Keep a tiny
        // window so moderate concurrency does not fall into a zero-wait
        // feedback loop, but cap it well below larger configured waits.
        assert_eq!(
            adaptive_window_for_state_enabled(cfg, 0.5, true),
            Duration::from_micros(100)
        );
        assert_eq!(
            adaptive_window_for_state_enabled(cfg, 1.0, true),
            Duration::from_micros(100)
        );
        assert_eq!(
            adaptive_window_for_state_enabled(cfg, 1.49, true),
            Duration::from_micros(100)
        );
        assert_eq!(
            adaptive_window_for_state_enabled(Duration::from_micros(50), 1.0, true),
            Duration::from_micros(50)
        );
        // Above the low-batch threshold: batching is at least starting
        // to form, so keep the configured window so a brief wait can
        // grow it further.
        assert_eq!(adaptive_window_for_state_enabled(cfg, 1.5, true), cfg);
        assert_eq!(adaptive_window_for_state_enabled(cfg, 2.0, true), cfg);
        // High EMA no longer expands beyond the configured latency budget.
        assert_eq!(adaptive_window_for_state_enabled(cfg, 4.0, true), cfg);
        assert_eq!(adaptive_window_for_state_enabled(cfg, 8.0, true), cfg);
        assert_eq!(adaptive_window_for_state_enabled(cfg, 32.0, true), cfg);
        // Default/off path preserves the configured static window.
        assert_eq!(adaptive_window_for_state_enabled(cfg, 1.0, false), cfg);
        // Zero-configured window stays zero (no group commit).
        let off = adaptive_window_for_state_enabled(Duration::ZERO, 10.0, true);
        assert_eq!(off, Duration::ZERO);
    }

    /// B8 — EMA tracks recent batch sizes across successful syncs. After
    /// three single-waiter batches the EMA should be near 1; after three
    /// 8-waiter batches the EMA should be > 4. Confirms the feedback
    /// signal the adaptive window relies on actually moves with workload.
    #[test]
    fn ema_tracks_recent_batch_size() {
        let coord = Arc::new(GroupCommitCoordinator::default());
        // Three single-writer batches.
        for lsn in 1..=3 {
            coord
                .sync_lsn_with(lsn, Duration::ZERO, 1, false, |l| {
                    Ok((SyncOutcome::Synced, l))
                })
                .unwrap();
        }
        let avg_after_low = coord.state.lock().recent_avg_batch_size;
        assert!(
            avg_after_low > 0.0 && avg_after_low <= 1.0,
            "EMA after single-waiter runs = {avg_after_low}"
        );
        // Three batches with the leader carrying covered_waiters via direct
        // state mutation (no real concurrency in the unit test). We
        // pre-bump pending_waiters so the leader's batch_size reflects a
        // bunched run.
        for lsn in 4..=6 {
            {
                let mut st = coord.state.lock();
                st.pending_waiters = 8;
                st.pending_max_lsn = lsn;
            }
            coord
                .sync_lsn_with(lsn, Duration::ZERO, 1, false, |l| {
                    Ok((SyncOutcome::Synced, l))
                })
                .unwrap();
        }
        let avg_after_high = coord.state.lock().recent_avg_batch_size;
        assert!(
            avg_after_high > avg_after_low,
            "EMA must rise with bigger batches ({avg_after_low} -> {avg_after_high})"
        );
    }

    /// M1 — a leader that reports a flushed high-water beyond its snapshot LSN
    /// releases a straggler (which appended during the sync) for free: the
    /// straggler returns durable WITHOUT running its own sync_op, and the
    /// `high_water_released` counter records the saved fsync.
    #[test]
    fn high_water_releases_straggler_without_extra_sync() {
        let coord = Arc::new(GroupCommitCoordinator::default());
        // Leader syncs snapshot LSN 10 but reports high-water 20 (as a real
        // fsync of the whole WAL file would). Block its sync_op until a
        // straggler with LSN 15 (> 10, ≤ 20) has registered as pending.
        let coord_leader = coord.clone();
        let leader = thread::spawn(move || {
            coord_leader.sync_lsn_with(10, Duration::ZERO, 1, false, |_sync_lsn| {
                let deadline = Instant::now() + Duration::from_secs(2);
                loop {
                    if coord_leader.state.lock().pending_waiters >= 1 {
                        break;
                    }
                    assert!(Instant::now() < deadline, "straggler never registered");
                    thread::sleep(Duration::from_micros(50));
                }
                // Flushed high-water covers the straggler.
                Ok((SyncOutcome::Synced, 20))
            })
        });

        // Ensure the LSN-10 thread is the established leader (mid-sync,
        // sync_target=10) before the straggler enters — otherwise the
        // straggler could win leadership and the test would be racy.
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            {
                let st = coord.state.lock();
                if st.syncing && st.sync_target_lsn == 10 {
                    break;
                }
            }
            assert!(Instant::now() < deadline, "leader never established");
            thread::sleep(Duration::from_micros(50));
        }

        // Straggler: its closure must NEVER run — it is released by the
        // leader's high-water. Count invocations to prove it.
        let straggler_syncs = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let straggler_syncs_c = straggler_syncs.clone();
        let coord_straggler = coord.clone();
        let straggler = thread::spawn(move || {
            coord_straggler.sync_lsn_with(15, Duration::ZERO, 1, false, move |lsn| {
                straggler_syncs_c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok((SyncOutcome::Synced, lsn))
            })
        });

        leader.join().unwrap().unwrap();
        straggler.join().unwrap().unwrap();

        assert_eq!(
            straggler_syncs.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "straggler must be released for free, not run its own sync"
        );
        let st = coord.state.lock();
        assert!(st.durable_lsn >= 20, "durable_lsn={}", st.durable_lsn);
        assert!(
            st.high_water_released >= 1,
            "high_water_released={}",
            st.high_water_released
        );
        // Exactly one real sync happened (the leader's).
        assert_eq!(st.batches, 1, "batches={}", st.batches);
    }

    /// B1 — with a non-zero window and `target=3`, the leader must wake and
    /// fsync *promptly* once enough waiters arrive, instead of sleeping out
    /// the whole (here: multi-second) window. Regression for the shared-condvar
    /// lost-wakeup where a follower's notify could land on another parked
    /// follower and the leader missed it. Two followers are used so at least
    /// one is already parked when the last arrives — the exact bug shape.
    #[test]
    fn leader_wakes_promptly_when_target_met() {
        let coord = Arc::new(GroupCommitCoordinator::default());
        let window = Duration::from_secs(3);
        let started = Instant::now();

        let coord_leader = coord.clone();
        let leader = thread::spawn(move || {
            coord_leader
                .sync_lsn_with(10, window, 3, false, |lsn| Ok((SyncOutcome::Synced, lsn)))
                .unwrap();
        });

        // Wait until the leader is parked in its window (syncing, target not
        // yet snapshotted).
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            {
                let st = coord.state.lock();
                if st.syncing && st.sync_target_lsn == 0 && st.pending_waiters >= 1 {
                    break;
                }
            }
            assert!(Instant::now() < deadline, "leader never entered window");
            thread::sleep(Duration::from_micros(100));
        }

        // Two followers join the in-progress window, pushing pending to 3.
        let followers: Vec<_> = [11u64, 12u64]
            .into_iter()
            .map(|lsn| {
                let c = coord.clone();
                thread::spawn(move || {
                    c.sync_lsn_with(lsn, window, 3, false, |l| Ok((SyncOutcome::Synced, l)))
                        .unwrap();
                })
            })
            .collect();

        leader.join().unwrap();
        for f in followers {
            f.join().unwrap();
        }

        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "leader should batch promptly once target met, took {elapsed:?} (window={window:?})"
        );
        let st = coord.state.lock();
        assert!(st.batches >= 1);
        assert!(st.waiters >= 3, "waiters={}", st.waiters);
    }

    #[test]
    fn durability_worker_batches_private_completions_and_persists_high_water() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = crate::wal::LogManager::open(dir.path()).unwrap();
        let mut lsns = Vec::new();
        for txn_id in 1..=8 {
            lsns.push(
                log.append(
                    txn_id,
                    WalPayload::CommitTxn {
                        commit_scn: txn_id,
                        commit_unix_us: txn_id,
                        binlog: None,
                    },
                )
                .unwrap(),
            );
        }
        let log = Arc::new(parking_lot::Mutex::new(log));
        let worker = Arc::new(DurabilityWorker::start(Arc::clone(&log)));
        let barrier = Arc::new(std::sync::Barrier::new(lsns.len()));
        let handles = lsns
            .iter()
            .copied()
            .map(|lsn| {
                let worker = Arc::clone(&worker);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    worker.sync_lsn(lsn, Duration::from_millis(10), 8)
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap().unwrap();
        }
        let stats = worker.stats();
        assert_eq!(stats.requests, 8, "{stats:?}");
        assert!(stats.batches < 8, "{stats:?}");
        assert!(stats.batch_max > 1, "{stats:?}");
        assert!(log.lock().durable_lsn() >= *lsns.last().unwrap());
    }

    #[test]
    fn durability_worker_single_writer_skips_batch_window() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = crate::wal::LogManager::open(dir.path()).unwrap();
        let lsn = log
            .append(
                1,
                WalPayload::CommitTxn {
                    commit_scn: 1,
                    commit_unix_us: 1,
                    binlog: None,
                },
            )
            .unwrap();
        let log = Arc::new(parking_lot::Mutex::new(log));
        let worker = DurabilityWorker::start(Arc::clone(&log));
        let started = Instant::now();
        worker.sync_lsn(lsn, Duration::from_millis(200), 1).unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "single writer waited out the group-commit window"
        );
        assert!(log.lock().durable_lsn() >= lsn);
    }
}

#[cfg(test)]
mod mvcc_registration_tests {
    use super::*;

    #[test]
    fn transaction_audit_immediate_reentry_does_not_wait_for_its_own_waiter() {
        let mut locks = LockManager::default();
        let row = RowId::new(1, 1);
        assert!(locks
            .try_acquire_row_lock_without_wait(1, 7, row, LockMode::Exclusive)
            .unwrap());
        locks.enqueue_row_waiter(2, 7, row, LockMode::Exclusive);
        assert!(locks
            .try_acquire_row_lock_without_wait(1, 7, row, LockMode::Exclusive)
            .unwrap());
        assert!(!locks
            .try_acquire_row_lock_without_wait(3, 7, row, LockMode::Shared)
            .unwrap());
        assert!(!locks.waits_for.contains_key(&1));
    }

    #[test]
    fn nontransaction_registration_preserves_pending_writer_gap() {
        let mut txns = TransactionManager::default();
        // Writer 1 has allocated its ID but has not registered yet. An
        // internal owner and a later writer must not prove it committed.
        txns.note_nontransaction_id(2);
        txns.begin_with_id(3, 0, false, false, 0).unwrap();
        assert_eq!(txns.registered_txn_frontier, 0);
        assert_eq!(txns.next_txn_id, 4);
        assert!(!txns.slots.contains_key(&2));
        txns.begin_with_id(1, 0, false, false, 0).unwrap();
        assert_eq!(txns.registered_txn_frontier, 3);
        assert_eq!(txns.slots[&1].status, TxnStatus::Active);
    }

    #[test]
    fn nontransaction_replica_owner_preserves_source_allocator() {
        let mut txns = TransactionManager::default();
        txns.note_nontransaction_id(1);
        txns.note_nontransaction_id((1u64 << 63) + 7);
        assert_eq!(txns.next_txn_id, 2);
        assert_eq!(txns.registered_txn_frontier, 1);
        assert!(txns.registered_txn_ranges.is_empty());
        assert!(txns.slots.is_empty());
    }

    #[test]
    fn mvcc_registration_intervals_stay_bounded_behind_long_hole() {
        let mut txns = TransactionManager::default();
        txns.note_registered_txn(1);
        for id in 3..100_003 {
            txns.note_registered_txn(id);
        }
        assert_eq!(txns.registered_txn_frontier, 1);
        assert_eq!(txns.registered_txn_ranges.len(), 1);
        txns.note_registered_txn(2);
        assert_eq!(txns.registered_txn_frontier, 100_002);
        assert!(txns.registered_txn_ranges.is_empty());
    }

    #[test]
    fn mvcc_registration_reverse_order_duplicates_and_replica_ids() {
        let mut txns = TransactionManager::default();
        for id in (2..100).rev() {
            txns.note_registered_txn(id);
        }
        txns.note_registered_txn(50);
        txns.note_registered_txn(1u64 << 63);
        assert_eq!(txns.registered_txn_ranges.len(), 1);
        assert_eq!(txns.registered_txn_frontier, 0);
        txns.note_registered_txn(1);
        assert_eq!(txns.registered_txn_frontier, 99);
        assert!(txns.registered_txn_ranges.is_empty());
    }
}
