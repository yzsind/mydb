/// Transaction-local WAL buffering (`MYDB_WAL_TXN_BUFFER`, default ON).
///
/// When active, a write transaction's per-row WAL records (heap page updates)
/// are pre-encoded and parked here instead of being appended under the global
/// WAL mutex immediately. They are all appended in a SINGLE lock acquisition at
/// commit (together with the CommitTxn record), collapsing the per-transaction
/// `self.log.lock()` count. The pages they describe are held un-flushable in
/// the buffer pool via `deferred_pins` until that commit-time append assigns
/// their real LSNs (see `Frame::deferred_pins`). A crash before commit appends
/// none of these buffered data records and flushes none of their pages; the
/// eager BeginTxn record is harmless and recovery sees an empty loser.
///
/// Each entry is `(pre-encoded record, frames it dirtied)`. The frames are
/// `finalize_deferred`'d (real LSN published, pin released) right after their
/// record is appended.
type TxnWalEntry = (WalPayload, Vec<PageKey>);

/// Only the lock log can change during a compiled projection-only point read.
/// Transaction undo/binlog/index state is materialized only on an error path.
/// Statements sharing a transaction must be serialized by the caller.
pub(crate) enum LockingReadLockWatermark {
    Sharded(usize),
    Legacy(usize),
}

const ASYNC_ROW_LOCK_SIGNAL_SHARDS: usize = 64;

/// Undo records one purge lock round plans and removes. Rounds keep each
/// undo-lock hold in the tens of microseconds, so DML undo publication never
/// queues behind a whole purge batch. Measured under concurrent TPC-C, 256
/// records held the write latch ~160 us on average (up to ~3 ms) and the
/// planning read latch ~130 us, stalling every publisher meanwhile.
const UNDO_PURGE_ROUND_RECORDS: usize = 64;

#[inline]
fn async_row_lock_signal_shard(table_id: u64, row_id: RowId) -> usize {
    let mut hash = table_id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    hash ^= (row_id.page_id as u64).wrapping_mul(0xD1B5_4A32_D192_ED03);
    hash ^= (row_id.slot_id as u64).wrapping_mul(0x94D0_49BB_1331_11EB);
    hash ^= hash >> 29;
    hash as usize & (ASYNC_ROW_LOCK_SIGNAL_SHARDS - 1)
}

/// A commit whose MVCC state and WAL record have been published, but whose
/// durability barrier and post-durable hooks are intentionally deferred.
///
/// MySQL row replication is a single ordered applier. If every source
/// transaction waits for its own fsync before the next one is even applied,
/// the normal concurrent group-commit coordinator can never form a batch.
/// The replica prepares commits strictly in source order, then makes the
/// highest LSN durable once and finishes every covered commit in that same
/// order. Ordinary client commits still use `commit_txn`, which prepares and
/// finishes a one-element batch.
#[must_use = "a published commit must be passed to Database::finish_deferred_commits"]
pub(crate) struct DeferredCommit {
    pub(crate) logical_apply_lease:
        Option<parking_lot::ArcRwLockReadGuard<parking_lot::RawRwLock, ()>>,
    _backup_feedback: Option<crate::storage::capture::CommitSample>,
    txn_id: u64,
    commit_scn: u64,
    commit_unix_us: u64,
    temporal_enabled: bool,
    history_index_batch: Vec<u8>,
    temporal_persist: Option<super::temporal::TemporalPersistToken>,
    commit_lsn: Option<u64>,
    durability_lsn: Option<u64>,
    index_error: Option<QueryError>,
    commit_group_target: u64,
    binlog_commit: Option<crate::binlog::BinlogCommit>,
    binlog_semi_sync_target: Option<(String, u64)>,
    finished: bool,
}

impl Drop for DeferredCommit {
    fn drop(&mut self) {
        if !self.finished {
            // This type is crate-private and has only the ordinary commit and
            // relay group-commit callers. Keep a production diagnostic in
            // addition to `must_use`: silently dropping the token would omit
            // post-durable accounting and replication hooks.
            eprintln!(
                "mydb invariant violation: deferred commit for transaction {} was dropped unfinished",
                self.txn_id
            );
        }
    }
}

/// Approximate percentile of the realized group-commit batch size from the
/// coarse histogram buckets `[1, 2, 3-4, 5-8, 9-16, 17+]`. Returns the
/// representative value of the bucket the `p`-th percentile falls into — a
/// cheap p50/p99 for `SHOW ENGINE MYDB STATUS` without keeping a full
/// histogram. Empty history → 0.
fn batch_size_percentile(buckets: &[u64; 6], p: u64) -> u64 {
    const REPRESENTATIVE: [u64; 6] = [1, 2, 4, 8, 16, 32];
    let total: u64 = buckets.iter().copied().sum();
    if total == 0 {
        return 0;
    }
    // Rank of the target observation (1-based), ceil(p/100 * total).
    let target = ((p.min(100).saturating_mul(total)) + 99) / 100;
    let target = target.max(1);
    let mut cumulative = 0u64;
    for (idx, &count) in buckets.iter().enumerate() {
        cumulative = cumulative.saturating_add(count);
        if cumulative >= target {
            return REPRESENTATIVE[idx];
        }
    }
    REPRESENTATIVE[5]
}

#[inline]
fn elapsed_us(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
}

fn checkpoint_trigger_name(trigger: u8) -> &'static str {
    match trigger {
        CHECKPOINT_TRIGGER_WAL => "wal",
        CHECKPOINT_TRIGGER_TIME => "time",
        CHECKPOINT_TRIGGER_MANUAL => "manual",
        CHECKPOINT_TRIGGER_SEQUENCE => "sequence",
        _ => "none",
    }
}

fn checkpoint_kind_name(kind: u8) -> &'static str {
    match kind {
        CHECKPOINT_KIND_FUZZY => "fuzzy",
        CHECKPOINT_KIND_SHARP => "sharp",
        CHECKPOINT_KIND_RESTARTPOINT => "restartpoint",
        CHECKPOINT_KIND_MEMORY => "memory",
        _ => "none",
    }
}

fn sync_replica_data_tree(path: &Path, wal_dir: &Path) -> Result<(), QueryError> {
    if path == wal_dir {
        return Ok(());
    }
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(io_err(err)),
    };
    for entry in entries {
        let entry = entry.map_err(io_err)?;
        let child = entry.path();
        let ty = entry.file_type().map_err(io_err)?;
        if ty.is_dir() {
            sync_replica_data_tree(&child, wal_dir)?;
        } else if ty.is_file() {
            crate::platform::fs::sync_path(&child).map_err(io_err)?;
        }
    }
    crate::platform::fs::sync_path(path).map_err(io_err)
}

/// A connection's statements are dispatched through the blocking pool and are
/// not pinned to one OS thread. Transaction WAL state must therefore be keyed
/// by transaction identity, never stored in `thread_local!`.
///
/// The WAL Arc address scopes transaction ids to one open database. The entry
/// is removed at commit/rollback, before that Arc can be dropped and its
/// address reused. Sharding keeps independent writers off one global mutex.
const TXN_WAL_BUFFER_SHARDS: usize = 64;
#[cfg(test)]
thread_local! {
    static FAIL_TXN_WAL_BUFFER_FLUSH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
type TxnWalBufferKey = (usize, u64);
#[derive(Default)]
struct TxnWalBuffer {
    entries: Vec<TxnWalEntry>,
    boundaries: Vec<(
        usize,
        std::sync::Weak<crate::storage::txn::WalSavepointBoundary>,
    )>,
}
type TxnWalBufferShard = parking_lot::Mutex<HashMap<TxnWalBufferKey, TxnWalBuffer>>;

type ReducedRedoTxnShard = parking_lot::Mutex<HashMap<TxnWalBufferKey, HashSet<String>>>;

fn reduced_redo_txn_shards() -> &'static [ReducedRedoTxnShard] {
    static SHARDS: OnceLock<Box<[ReducedRedoTxnShard]>> = OnceLock::new();
    SHARDS.get_or_init(|| {
        (0..TXN_WAL_BUFFER_SHARDS)
            .map(|_| parking_lot::Mutex::new(HashMap::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice()
    })
}

#[inline]
fn reduced_redo_txn_shard(key: TxnWalBufferKey) -> &'static ReducedRedoTxnShard {
    let mixed = (key.0 as u64).rotate_left(17) ^ key.1;
    &reduced_redo_txn_shards()[mixed as usize & (TXN_WAL_BUFFER_SHARDS - 1)]
}

pub(crate) fn txn_reduced_redo_active(log: &Arc<crate::wal::LogMutex>, txn_id: u64) -> bool {
    let key = txn_wal_buffer_key(log, txn_id);
    reduced_redo_txn_shard(key).lock().contains_key(&key)
}

pub(crate) fn note_reduced_redo_file(
    log: &Arc<crate::wal::LogMutex>,
    txn_id: u64,
    relative_path: String,
) {
    let key = txn_wal_buffer_key(log, txn_id);
    if let Some(files) = reduced_redo_txn_shard(key).lock().get_mut(&key) {
        files.insert(relative_path);
    }
}

fn reduced_redo_files(log: &Arc<crate::wal::LogMutex>, txn_id: u64) -> Vec<String> {
    let key = txn_wal_buffer_key(log, txn_id);
    reduced_redo_txn_shard(key)
        .lock()
        .get(&key)
        .map(|files| files.iter().cloned().collect())
        .unwrap_or_default()
}

fn begin_txn_redo_policy(log: &Arc<crate::wal::LogMutex>, txn_id: u64) {
    if !current_redo_enabled() {
        let key = txn_wal_buffer_key(log, txn_id);
        if reduced_redo_txn_shard(key)
            .lock()
            .insert(key, HashSet::new())
            .is_none()
        {
            REDUCED_REDO_TRANSACTIONS.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn end_txn_redo_policy(log: &Arc<crate::wal::LogMutex>, txn_id: u64) {
    let key = txn_wal_buffer_key(log, txn_id);
    reduced_redo_txn_shard(key).lock().remove(&key);
}

fn txn_wal_buffer_shards() -> &'static [TxnWalBufferShard] {
    static SHARDS: OnceLock<Box<[TxnWalBufferShard]>> = OnceLock::new();
    SHARDS.get_or_init(|| {
        (0..TXN_WAL_BUFFER_SHARDS)
            .map(|_| parking_lot::Mutex::new(HashMap::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice()
    })
}

#[inline]
fn txn_wal_buffer_key(log: &Arc<crate::wal::LogMutex>, txn_id: u64) -> TxnWalBufferKey {
    (Arc::as_ptr(log) as usize, txn_id)
}

#[inline]
fn txn_wal_buffer_shard(key: TxnWalBufferKey) -> &'static TxnWalBufferShard {
    let mixed = (key.0 as u64).rotate_left(17) ^ key.1;
    &txn_wal_buffer_shards()[mixed as usize & (TXN_WAL_BUFFER_SHARDS - 1)]
}

/// Cap on buffered records before a mid-transaction eager flush fires, bounding
/// how many frames a single transaction can hold deferred-pinned (and thus
/// un-evictable) at once. Picked well above the 1-record autocommit hot path.
const TXN_WAL_BUFFER_FLUSH_THRESHOLD: usize = 256;

fn txn_wal_buffer_feature_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        !matches!(
            std::env::var("MYDB_WAL_TXN_BUFFER")
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "off" | "false" | "no"
        )
    })
}

/// Whether the index-batch WAL record participates in transaction WAL
/// buffering (park + deferred-pin the leaf, batched append at commit). Default
/// ON when buffering is active; `MYDB_WAL_TXN_BUFFER_INDEX=0` keeps the index
/// record on the eager path so the index deferral can be A/B'd against
/// heap-only buffering as a differential oracle.
pub(crate) fn txn_wal_buffer_index_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        !matches!(
            std::env::var("MYDB_WAL_TXN_BUFFER_INDEX")
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "off" | "false" | "no"
        )
    })
}

/// P4 — directed row-lock waiter wakeup. When enabled (default), a lock
/// release wakes only the released row's front waiter (`thread::unpark`)
/// instead of the 256-shard `notify_all` thundering herd. Set
/// `MYDB_ROW_LOCK_DIRECT_WAKE=0` to fall back to the legacy condvar broadcast
/// (A/B + safety escape hatch). Correctness is independent of the choice: the
/// bounded `park_timeout`/`wait_timeout` re-poll in `wait_for_specific_row`
/// self-heals any missed wake within one slice.
pub(crate) fn row_lock_direct_wake_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        !matches!(
            std::env::var("MYDB_ROW_LOCK_DIRECT_WAKE")
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "off" | "false" | "no"
        )
    })
}

fn txn_wal_buffering_active(log: &Arc<crate::wal::LogMutex>, txn_id: u64) -> bool {
    let key = txn_wal_buffer_key(log, txn_id);
    txn_wal_buffer_shard(key).lock().contains_key(&key)
}

fn begin_txn_wal_buffering(log: &Arc<crate::wal::LogMutex>, txn_id: u64) {
    let key = txn_wal_buffer_key(log, txn_id);
    let previous = txn_wal_buffer_shard(key)
        .lock()
        .insert(key, TxnWalBuffer::default());
    debug_assert!(
        previous.is_none(),
        "transaction WAL buffer must not be armed twice"
    );
}

/// Park a logical record + the frames it dirtied. Encoding is deferred until
/// the commit tail is known, allowing the whole tail to receive one frame CRC.
/// Returns the owned payload/keys when buffering disappeared after the caller's
/// eligibility probe, allowing it to fall back to an eager WAL append without
/// cloning hot-path payloads.
fn push_txn_wal_record(
    log: &Arc<crate::wal::LogMutex>,
    txn_id: u64,
    payload: WalPayload,
    keys: Vec<PageKey>,
) -> Result<(), (WalPayload, Vec<PageKey>)> {
    let key = txn_wal_buffer_key(log, txn_id);
    let mut shard = txn_wal_buffer_shard(key).lock();
    let Some(buf) = shard.get_mut(&key) else {
        return Err((payload, keys));
    };
    buf.entries.push((payload, keys));
    Ok(())
}

fn txn_wal_buffer_len(log: &Arc<crate::wal::LogMutex>, txn_id: u64) -> usize {
    let key = txn_wal_buffer_key(log, txn_id);
    txn_wal_buffer_shard(key)
        .lock()
        .get(&key)
        .map_or(0, |buf| buf.entries.len())
}

fn txn_wal_has_deferred_pages(log: &Arc<crate::wal::LogMutex>, txn_id: u64) -> bool {
    let key = txn_wal_buffer_key(log, txn_id);
    txn_wal_buffer_shard(key)
        .lock()
        .get(&key)
        .is_some_and(|buffer| buffer.entries.iter().any(|(_, keys)| !keys.is_empty()))
}

/// Take the currently-buffered entries, leaving buffering active with an empty
/// buffer (mid-transaction flush). Returns empty when inactive.
fn take_txn_wal_entries(log: &Arc<crate::wal::LogMutex>, txn_id: u64) -> TxnWalBuffer {
    let key = txn_wal_buffer_key(log, txn_id);
    txn_wal_buffer_shard(key)
        .lock()
        .get_mut(&key)
        .map(std::mem::take)
        .unwrap_or_default()
}

/// End buffering for this transaction (commit/rollback complete).
fn end_txn_wal_buffering(log: &Arc<crate::wal::LogMutex>, txn_id: u64) {
    let key = txn_wal_buffer_key(log, txn_id);
    txn_wal_buffer_shard(key).lock().remove(&key);
}

fn clear_txn_wal_buffers_for_log(log: &Arc<crate::wal::LogMutex>) {
    let log_id = Arc::as_ptr(log) as usize;
    for shard in txn_wal_buffer_shards() {
        let mut shard = shard.lock();
        shard.retain(|(entry_log_id, _), _| *entry_log_id != log_id);
        if shard.is_empty() {
            shard.shrink_to_fit();
        }
    }
    for shard in reduced_redo_txn_shards() {
        shard
            .lock()
            .retain(|(entry_log_id, _), _| *entry_log_id != log_id);
    }
}

/// Phase 5 — undo compaction policy.
/// * `Off`   — never compact (the purge tick only drops fully-dead segments).
/// * `Auto`  — DEFAULT. Compact only when fragmentation is significant
///             (lots of dead space pinned in partially-live segments), so
///             steady OLTP pays nothing while long-transaction workloads
///             that strand dead space get reclaimed.
/// * `On`    — compact whenever anything is reclaimable (aggressive).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UndoCompactionMode {
    Off,
    Auto,
    On,
}

/// Phase 5 — read the compaction policy from the environment.
/// `MYDB_UNDO_COMPACTION = off | auto | on` (default `auto`); the legacy
/// `MYDB_UNDO_COMPACTION_ENABLED=1` still maps to `on` for compatibility.
fn compaction_mode() -> UndoCompactionMode {
    if let Ok(v) = std::env::var("MYDB_UNDO_COMPACTION") {
        let t = v.trim();
        if t.eq_ignore_ascii_case("off") || t == "0" {
            return UndoCompactionMode::Off;
        }
        if t.eq_ignore_ascii_case("on") || t == "1" {
            return UndoCompactionMode::On;
        }
        if t.eq_ignore_ascii_case("auto") {
            return UndoCompactionMode::Auto;
        }
    }
    if let Ok(v) = std::env::var("MYDB_UNDO_COMPACTION_ENABLED") {
        let t = v.trim();
        if t == "1" || t.eq_ignore_ascii_case("true") {
            return UndoCompactionMode::On;
        }
        if t == "0" || t.eq_ignore_ascii_case("false") {
            return UndoCompactionMode::Off;
        }
    }
    UndoCompactionMode::Auto
}

/// P1/10 — pick the compaction execution strategy. `true` (default) uses
/// build-then-swap (rewrite off-lock, swap under a microsecond write-lock
/// hold); `MYDB_UNDO_COMPACTION_BUILD_SWAP=0` reverts to the legacy
/// in-lock rewrite as a differential oracle.
fn undo_compaction_build_swap() -> bool {
    match std::env::var("MYDB_UNDO_COMPACTION_BUILD_SWAP") {
        Ok(v) => {
            let t = v.trim();
            !(t == "0" || t.eq_ignore_ascii_case("off") || t.eq_ignore_ascii_case("false"))
        }
        Err(_) => true,
    }
}

/// Test-only hook fired in `commit_txn` immediately AFTER a writer
/// allocates its `commit_scn` (the global allocation clock
/// `next_scn_atomic` has already been bumped) but BEFORE the commit is
/// published to the slot status / snapshot watermark (`publish_commit`).
///
/// This is exactly the "allocate-before-publish" window behind the MVCC
/// commit-visibility atomicity bug: a concurrent reader can take a
/// snapshot whose `snapshot_scn >= commit_scn` while the writer's slot is
/// still `Active`. The regression test installs a hook that parks the
/// committer here so a reader thread can deterministically observe whether
/// it gets a torn / non-repeatable view across the publish point.
///
/// Compiled out entirely in non-test builds (zero cost on the commit hot
/// path — not even an atomic load).
#[cfg(test)]
pub(crate) mod commit_visibility_test_hook {
    use std::sync::{Arc, Mutex, OnceLock};

    type Hook = Arc<dyn Fn(u64) + Send + Sync>;

    fn cell() -> &'static Mutex<Option<Hook>> {
        static HOOK: OnceLock<Mutex<Option<Hook>>> = OnceLock::new();
        HOOK.get_or_init(|| Mutex::new(None))
    }

    pub(crate) fn install(hook: Hook) {
        *cell().lock().unwrap() = Some(hook);
    }

    pub(crate) fn clear() {
        *cell().lock().unwrap() = None;
    }

    pub(crate) fn fire(commit_scn: u64) {
        // Clone the Arc out from under the lock so a blocking hook does not
        // hold the registry mutex while parked.
        let hook = cell().lock().unwrap().clone();
        if let Some(hook) = hook {
            hook(commit_scn);
        }
    }
}

/// Test-only hook fired after undo compaction has installed a relocation but
/// before it remaps the affected active heads. A concurrent append to one of
/// those transactions must stay blocked until the head has been remapped and
/// the fence is released.
#[cfg(test)]
pub(crate) mod undo_compaction_head_fence_test_hook {
    use std::sync::{Arc, Mutex, OnceLock};

    type Hook = Arc<dyn Fn() + Send + Sync>;

    fn cell() -> &'static Mutex<Option<Hook>> {
        static HOOK: OnceLock<Mutex<Option<Hook>>> = OnceLock::new();
        HOOK.get_or_init(|| Mutex::new(None))
    }

    pub(crate) fn install(hook: Hook) {
        *cell().lock().unwrap() = Some(hook);
    }

    pub(crate) fn clear() {
        *cell().lock().unwrap() = None;
    }

    pub(crate) fn fire() {
        let hook = cell().lock().unwrap().clone();
        if let Some(hook) = hook {
            hook();
        }
    }
}

/// P0-1: RAII guard that guarantees every allocated `commit_scn` /
/// `prepare_scn` is eventually marked done on the commit-visibility frontier,
/// even on the error paths between allocation and publish. If the SCN is
/// resolved normally (commit publishes, prepare promotes) the caller calls
/// [`ScnFrontierGuard::disarm`]; otherwise `Drop` marks it done (a "void"
/// completion — the transaction is invisible by slot status, but the frontier
/// must still be allowed to advance past this number or it stalls forever).
///
/// Drop must NOT run while the caller holds the txns write lock (it acquires
/// it). All commit/prepare error paths bail BEFORE taking that lock, so this
/// holds.
struct ScnFrontierGuard<'a> {
    db: &'a Database,
    scn: u64,
    armed: bool,
}

impl<'a> ScnFrontierGuard<'a> {
    fn new(db: &'a Database, scn: u64) -> Self {
        Self {
            db,
            scn,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ScnFrontierGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.db.complete_commit_frontier(self.scn);
        }
    }
}

impl Database {
    #[cfg(test)]
    pub(crate) fn buffered_txn_wal_records_for_test(&self, txn_id: u64) -> usize {
        txn_wal_buffer_len(&self.log, txn_id)
    }

    pub fn physical_replica_mode(&self) -> bool {
        self.physical_replica_mode.load(Ordering::Acquire)
    }

    pub fn set_physical_replica_mode(&self, enabled: bool) -> Result<(), QueryError> {
        // Rebuilds hold the read side through page allocation/publication.
        // Drain them before changing roles so no local Zone Map writer can
        // keep allocating pages after the physical replica role is visible.
        let _zone_map_role_transition = self.zone_map_gate.write();
        let role_changed = self.physical_replica_mode() != enabled;
        if role_changed {
            self.sequences.role_epoch.fetch_add(1, Ordering::AcqRel);
        }
        if enabled && role_changed {
            let covered = self.log_state_atomics.next_lsn().saturating_sub(1)
                .min(self.vector_pending_publication_floor().unwrap_or(u64::MAX).saturating_sub(1));
            self.vector_graph_persist_all_dirty(covered)?;
        }
        self.log.lock().set_physical_replica(enabled);
        if enabled {
            // Publish the receiving role before draining vector readers. A
            // graph opened concurrently with invalidation must observe this
            // flag and cannot begin a new direct-mapped read after the drain.
            self.physical_replica_mode.store(true, Ordering::Release);
        }
        if role_changed {
            self.invalidate_replicated_vector_graphs();
        }
        if !enabled {
            // Hold registration serialization before opening the write role:
            // no newly allocated local ID may be mistaken for a source hole.
            let mut txns = self.txns.write();
            if self.physical_replica_mode.swap(false, Ordering::AcqRel) {
                txns.close_registration_history();
            }
        }
        if !enabled && role_changed { self.finish_promoted_vector_journal()?; }
        Ok(())
    }

    pub fn replica_applied_lsn(&self) -> u64 {
        self.replica_applied_lsn_atomic.load(Ordering::Acquire)
    }

    pub fn has_pending_replicated_undo(&self) -> bool {
        self.undo.read().has_replicated_pending_frames()
    }

    pub fn persist_replicated_undo_for_promotion(&self) -> Result<(), QueryError> {
        self.undo.write().persist_replicated_frames_for_promotion()
    }

    /// Online apply retains PREPARED slots without installing writable XA
    /// runtime state. Publish missing branches before the promotion opens
    /// writes, using the same lock snapshots and metadata as startup recovery.
    pub fn restore_replicated_xa_for_promotion(&self) -> Result<(), QueryError> {
        let missing = {
            let txns = self.txns.read();
            let prepared = self.prepared_xa.read();
            let mut missing = HashMap::new();
            for slot in txns
                .slots
                .values()
                .filter(|slot| slot.status == TxnStatus::Prepared)
            {
                let xid = slot.xa_xid.as_ref().ok_or_else(|| {
                    error(
                        crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED,
                        "promotion refused: prepared XA slot has no XID",
                    )
                })?;
                if let Some(branch) = prepared.get(xid) {
                    if branch.txn_id != slot.txn_id {
                        return Err(error(
                            crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED,
                            "promotion refused: prepared XA registry owner mismatch",
                        ));
                    }
                } else {
                    missing.insert(slot.txn_id, xid.clone());
                }
            }
            missing
        };
        if missing.is_empty() {
            return Ok(());
        }
        let scan = LogManager::scan(&self.data_dir)?;
        let mut snapshots = HashMap::new();
        for record in scan
            .records
            .records()
            .only(&[crate::wal::WalRecordType::PrepareTxn])
        {
            let record = record?;
            if let WalPayload::PrepareTxn { xid, .. } = &record.payload {
                if missing.get(&record.txn_id) == Some(xid) {
                    snapshots.insert(record.txn_id, record);
                }
            }
        }
        if snapshots.len() != missing.len() {
            return Err(error(
                crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED,
                "promotion refused: prepared XA branch is missing its PREPARE WAL snapshot",
            ));
        }
        for (txn_id, record) in snapshots {
            let WalPayload::PrepareTxn {
                xid,
                prepare_scn,
                locks,
                mdl_locks,
                binlog,
            } = &record.payload
            else {
                unreachable!()
            };
            self.reserve_xa(xid, txn_id)?;
            self.lock_manager().restore_prepared_locks(txn_id, locks)?;
            self.migrate_recovered_table_locks();
            self.mdl.install_prepared(txn_id, mdl_locks);
            let started_unix_ms = self.txns.read().slots[&txn_id].started_unix_ms;
            self.prepared_xa.write().insert(
                xid.clone(),
                crate::xa::PreparedXa {
                    xid: xid.clone(),
                    txn_id,
                    prepare_scn: *prepare_scn,
                    started_unix_ms,
                    binlog: binlog.clone(),
                    recovered: true,
                },
            );
            self.xa_prepared_active.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn allocate_read_txn_id(&self) -> u64 {
        if self.physical_replica_mode() {
            self.next_replica_local_txn_id
                .fetch_add(1, Ordering::AcqRel)
        } else {
            self.next_txn_id_atomic.fetch_add(1, Ordering::AcqRel)
        }
    }

    /// Allocate a unique owner/job ID that can never appear in a row header
    /// or undo record. Register its consumption so transaction GC does not
    /// wait forever for a slot that this operation intentionally never creates.
    fn allocate_nontransaction_id(&self) -> u64 {
        // Match replica role changes' registration serialization. Replica
        // local owners use the high-half namespace without advancing the
        // source transaction allocator or its registration frontier.
        let mut txns = self.txns.write();
        let id = self.allocate_read_txn_id();
        txns.note_nontransaction_id(id);
        id
    }

    pub(crate) fn is_active_write_transaction(&self, txn_id: u64) -> Result<bool, QueryError> {
        Ok(self
            .txns_read()?
            .slots
            .get(&txn_id)
            .is_some_and(|slot| slot.status == TxnStatus::Active && slot.is_writer))
    }

    pub fn open_mode(&self) -> DatabaseOpenMode {
        self.open_mode
    }

    pub fn is_snapshot_read_only(&self) -> bool {
        self.open_mode == DatabaseOpenMode::ReadOnlyImage
    }

    pub(crate) fn ensure_writable(&self) -> Result<(), QueryError> {
        if self.is_snapshot_read_only() {
            return Err(crate::storage::image_io::read_only_error());
        }
        Ok(())
    }

    pub(crate) fn ensure_operational(&self) -> Result<(), QueryError> {
        if !self.mvcc_quarantined.load(Ordering::Acquire) {
            return Ok(());
        }
        let reason = self
            .mvcc_quarantine_reason
            .read()
            .ok()
            .and_then(|reason| reason.clone())
            .unwrap_or_else(|| "MVCC state requires restart recovery".to_string());
        Err(error(
            crate::error::catalog::ER_SERVER_SHUTDOWN,
            format!("database is fail-stopped: {reason}"),
        ))
    }

    fn quarantine_mvcc(&self, reason: impl Into<String>) {
        let reason = reason.into();
        if self
            .mvcc_quarantined
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            if let Ok(mut stored) = self.mvcc_quarantine_reason.write() {
                *stored = Some(reason.clone());
            }
            crate::util::logging::log_message(format_args!("mydb MVCC fail-stop: {reason}"));
        }
    }

    /// P0-1: the snapshot watermark — the largest SCN such that every commit
    /// ≤ it has finished publishing. Lock-free. This is the ONLY value any
    /// snapshot may use as its `snapshot_scn`.
    #[inline]
    pub(crate) fn visible_scn(&self) -> u64 {
        self.visible_frontier.load(Ordering::Acquire)
    }

    fn publish_visible_frontier(&self, frontier: u64) {
        // SeqCst pairs with waiter registration and its predicate recheck:
        // either the waiter observes publication or the publisher observes
        // the waiter. The mutex bridges the recheck-to-sleep interval.
        let previous = self.visible_frontier.fetch_max(frontier, Ordering::SeqCst);
        if frontier > previous && self.commit_frontier_waiters.load(Ordering::SeqCst) != 0 {
            let _guard = self
                .commit_frontier_wait_lock
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            self.commit_frontier_signal.notify_all();
        }
    }

    fn wait_for_commit_visibility(&self, commit_scn: u64) {
        if self.visible_scn() >= commit_scn {
            return;
        }
        let mut guard = self
            .commit_frontier_wait_lock
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        self.commit_frontier_waiters.fetch_add(1, Ordering::SeqCst);
        while self.visible_frontier.load(Ordering::SeqCst) < commit_scn {
            guard = self
                .commit_frontier_signal
                .wait(guard)
                .unwrap_or_else(|e| e.into_inner());
        }
        self.commit_frontier_waiters.fetch_sub(1, Ordering::SeqCst);
    }

    /// With COMMIT_GATE held, drain the allocated visibility prefix before
    /// pairing a consistent read view with the published binlog status.
    pub(crate) fn fence_binlog_snapshot_visibility(&self) -> Result<(), QueryError> {
        self.ensure_operational()?;
        let target = self
            .next_scn_atomic
            .load(Ordering::Acquire)
            .saturating_sub(1);
        self.wait_for_commit_visibility(target);
        self.ensure_operational()
    }

    /// P0-1: mark `scn` done on the frontier under the txns write lock and
    /// publish the new frontier to the lock-free mirror. Used by the
    /// `ScnFrontierGuard` void path and by XA prepare. Monotonic via
    /// `fetch_max`.
    pub(crate) fn complete_commit_frontier(&self, scn: u64) {
        let frontier = {
            let mut txns = self.txns.write();
            txns.mark_frontier_done(scn)
        };
        self.publish_visible_frontier(frontier);
    }
}

impl Database {
    pub fn begin_txn(&self) -> Result<TxnView, QueryError> {
        if self.is_snapshot_read_only() {
            return self.begin_read_only_txn();
        }
        if !current_redo_enabled()
            && (self.capture.redo_required() || crate::repl::physical_replication_active())
        {
            return Err(error(
                crate::error::catalog::ER_INNODB_REDO_DISABLED,
                "mydb_redo_enabled=OFF is incompatible with physical replication",
            ));
        }
        // M1.4 - Lock-free txn ID/SCN allocation using atomic counters.
        // Falls back to manager-internal counter if atomic seed has not been
        // bumped (preserves single source of truth on recovery).
        let txn_id = self.next_txn_id_atomic.fetch_add(1, Ordering::AcqRel);
        // P0-1: snapshot from the published frontier, never the allocation
        // counter (which runs ahead of published commits).
        let start_scn = self.visible_scn();
        let begin_lsn = self.log_state_atomics.next_lsn();
        let txn = self
            .txns_write()?
            .begin_with_id(txn_id, start_scn, false, true, begin_lsn)?;
        self.slot_status_cache.insert_active(txn.txn_id);
        if let Err(err) = self.append_wal(
            txn.txn_id,
            WalPayload::BeginTxn {
                start_scn: txn.snapshot_scn,
            },
        ) {
            self.txns_write()?.discard_failed_begin(txn.txn_id);
            self.slot_status_cache.remove(txn.txn_id);
            return Err(err);
        }
        begin_txn_redo_policy(&self.log, txn.txn_id);
        Ok(txn)
    }

    /// Begin a DML transaction. Identical to [`begin_txn`] except that the
    /// slot is flagged as a writer under the SAME txns write-lock acquisition
    /// (instead of lazily on the first `push_undo_with_flush`). The lazy
    /// path turned a single TM.write into two for every autocommit INSERT /
    /// UPDATE / DELETE statement, which dominated TransactionManager lock
    /// contention under bulk-loader workloads (sysbench prepare profile:
    /// 8.24% begin_txn + 2.44% push_undo_with_flush mark_writer waits).
    /// Callers that aren't certain the statement will issue an undo append
    /// — read-only transactions, lazy-started SELECTs — must keep using
    /// `begin_txn`: an eager writer flag here would inflate
    /// `active_write_count`, which feeds the group-commit batch target
    /// and the in-flight-writer watermark.
    pub fn begin_dml_txn(&self) -> Result<TxnView, QueryError> {
        self.ensure_writable()?;
        if !current_redo_enabled()
            && (self.capture.redo_required() || crate::repl::physical_replication_active())
        {
            return Err(error(
                crate::error::catalog::ER_INNODB_REDO_DISABLED,
                "mydb_redo_enabled=OFF is incompatible with physical replication",
            ));
        }
        let txn_id = self.next_txn_id_atomic.fetch_add(1, Ordering::AcqRel);
        // P0-1: snapshot from the published frontier, never the allocation
        // counter (which runs ahead of published commits).
        let start_scn = self.visible_scn();
        let begin_lsn = self.log_state_atomics.next_lsn();
        let undo_head = UndoHead::default();
        let slot = TxnSlot {
            txn_id,
            status: TxnStatus::Active,
            start_scn,
            started_unix_ms: current_unix_ms(),
            commit_scn: None,
            undo_head: undo_head.clone(),
            is_writer: true,
            read_only: false,
            wal_logged: true,
            xa_xid: None,
            begin_lsn,
        };
        let txn = {
            let mut txns = self.txns_write()?;
            let view = txns.register_new_slot(slot);
            self.active_write_txns_atomic
                .store(txns.active_write_count, Ordering::SeqCst);
            if let Err(err) = self.capture.ensure_write() {
                txns.discard_failed_begin(txn_id);
                self.active_write_txns_atomic
                    .store(txns.active_write_count, Ordering::SeqCst);
                return Err(err);
            }
            view
        };
        self.slot_status_cache.insert_active(txn.txn_id);
        if let Err(err) = self.append_wal(
            txn.txn_id,
            WalPayload::BeginTxn {
                start_scn: txn.snapshot_scn,
            },
        ) {
            let mut txns = self.txns_write()?;
            txns.discard_failed_begin(txn.txn_id);
            self.active_write_txns_atomic
                .store(txns.active_write_count, Ordering::Release);
            drop(txns);
            self.slot_status_cache.remove(txn.txn_id);
            return Err(err);
        }
        begin_txn_redo_policy(&self.log, txn.txn_id);
        if txn_wal_buffer_feature_enabled() && self.config.durability != DurabilityMode::Memory {
            begin_txn_wal_buffering(&self.log, txn.txn_id);
        }
        self.cache_started_dml_txn(txn.txn_id, undo_head);
        Ok(txn)
    }

    /// A transaction which began with a read/savepoint can later write.
    /// Arm the same bounded WAL buffer used by write-first transactions,
    /// retaining every earlier buffered record when this is called again.
    pub(crate) fn enable_txn_wal_buffering_for_write(&self, txn_id: u64) -> Result<(), QueryError> {
        if self.config.durability == DurabilityMode::Memory || !txn_wal_buffer_feature_enabled() {
            return Ok(());
        }
        let eligible = {
            let txns = self.txns_read()?;
            let slot = txns.active_slot(txn_id)?;
            slot.wal_logged && !slot.read_only
        };
        if eligible {
            let key = txn_wal_buffer_key(&self.log, txn_id);
            txn_wal_buffer_shard(key).lock().entry(key).or_default();
        }
        Ok(())
    }

    /// M1.1 - Begin a read-only transaction (Oracle SET TRANSACTION READ ONLY).
    /// Read-only txns:
    /// - allocate a txn_id and snapshot SCN
    /// - skip WAL BeginTxn/EndTxn records (no durability work)
    /// - reject DML at the executor layer (error 1792)
    /// The executor registers a purge guard atomically with the first read
    /// view (or eagerly for WITH CONSISTENT SNAPSHOT).
    pub fn begin_read_only_txn(&self) -> Result<TxnView, QueryError> {
        let txn_id = self.allocate_read_txn_id();
        // P0-1: snapshot from the published frontier.
        let start_scn = self.visible_scn();
        let begin_lsn = self.log_state_atomics.next_lsn();
        let txn = self
            .txns_write()?
            .begin_with_id(txn_id, start_scn, true, false, begin_lsn)?;
        self.slot_status_cache.insert_active(txn.txn_id);
        self.read_only_txn_count.fetch_add(1, Ordering::Relaxed);
        Ok(txn)
    }

    pub fn begin_temp_txn(&self) -> Result<TxnView, QueryError> {
        let txn_id = self.allocate_read_txn_id();
        // P0-1: snapshot from the published frontier, never the allocation
        // counter (which runs ahead of published commits).
        let start_scn = self.visible_scn();
        let begin_lsn = self.log_state_atomics.next_lsn();
        let view = self
            .txns_write()?
            .begin_with_id(txn_id, start_scn, false, false, begin_lsn)?;
        self.slot_status_cache.insert_active(view.txn_id);
        Ok(view)
    }

    pub fn snapshot_too_old_count(&self) -> u64 {
        self.snapshot_too_old_total.load(Ordering::Relaxed)
    }

    /// M2.4 - Record a wait event sample.
    pub fn record_wait_event(
        &self,
        txn_id: u64,
        event: WaitEvent,
        started_unix_us: u64,
        duration_us: u64,
        detail: impl Into<String>,
    ) {
        if let Ok(mut history) = self.wait_events.lock() {
            history.record(WaitEventSample {
                txn_id,
                event,
                started_unix_us,
                duration_us,
                detail: detail.into(),
            });
        }
    }

    #[cfg(test)]
    pub fn statement_view(
        &self,
        txn_id: u64,
        isolation: IsolationLevel,
    ) -> Result<TxnView, QueryError> {
        let txns = self.txns_read()?;
        // P0-1: write-txn statement snapshot comes from the published frontier,
        // not `current_scn()` (next_scn), which can run ahead of published
        // commits and out of publish order.
        let snapshot_scn = self.visible_scn();
        let read_only = txns.slot(txn_id).is_some_and(|slot| slot.read_only);
        Ok(TxnView {
            txn_id,
            snapshot_scn,
            isolation,
            read_only,
            dirty_read: false,
            historical: false,
            historical_baseline_scn: 0,
            historical_baseline_txn_id: 0,
            connection_id: 0,
            snapshot_registered: false,
        })
    }

    /// Lock-free `statement_view` for a session-tracked transaction. The
    /// caller supplies an optional transaction-wide snapshot. Write
    /// transactions and READ COMMITTED read-only transactions take the
    /// statement snapshot from the published commit frontier (`visible_scn`),
    /// which is the gap-aware snapshot watermark (P0-1) — never the raw
    /// allocation counter, which runs ahead of published commits.
    pub fn session_statement_view(
        &self,
        txn_id: u64,
        isolation: IsolationLevel,
        read_only_snapshot: Option<u64>,
    ) -> TxnView {
        let (snapshot_scn, read_only) = match read_only_snapshot {
            Some(frozen_scn) => (frozen_scn, true),
            None => (self.fresh_snapshot_horizon(), false),
        };
        TxnView {
            txn_id,
            snapshot_scn,
            isolation,
            read_only,
            dirty_read: false,
            historical: false,
            historical_baseline_scn: 0,
            historical_baseline_txn_id: 0,
            connection_id: 0,
            // A supplied frozen snapshot is owned by the session's
            // transaction-wide guard.
            snapshot_registered: read_only_snapshot.is_some(),
        }
    }

    fn fresh_snapshot_horizon(&self) -> u64 {
        self.visible_scn()
    }

    pub fn register_statement_snapshot(self: &Arc<Self>, snapshot_scn: u64) -> SnapshotGuard {
        let snapshot_id = self.next_snapshot_id.fetch_add(1, Ordering::Relaxed);
        let shard = (snapshot_id as usize) % self.active_snapshots.len();
        let mut snapshots = self.active_snapshots[shard]
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        snapshots.insert(
            snapshot_id,
            ActiveSnapshot {
                snapshot_scn,
                started_unix_ms: current_unix_ms(),
            },
        );
        SnapshotGuard {
            db: Arc::clone(self),
            snapshot_id,
        }
    }

    /// Register a set of historical snapshots atomically with respect to
    /// purge's all-shard horizon sampling. Lock order matches purge (ascending
    /// shard index), so it observes either the complete set or none of it.
    pub(crate) fn register_statement_snapshots_atomic(
        self: &Arc<Self>,
        snapshot_scns: &[u64],
    ) -> Vec<SnapshotGuard> {
        let mut shards = self
            .active_snapshots
            .iter()
            .map(|shard| shard.lock().unwrap_or_else(|poison| poison.into_inner()))
            .collect::<Vec<_>>();
        let mut guards = Vec::with_capacity(snapshot_scns.len());
        for &snapshot_scn in snapshot_scns {
            let snapshot_id = self.next_snapshot_id.fetch_add(1, Ordering::Relaxed);
            let shard = (snapshot_id as usize) % self.active_snapshots.len();
            if let Some(snapshots) = shards.get_mut(shard) {
                snapshots.insert(
                    snapshot_id,
                    ActiveSnapshot {
                        snapshot_scn,
                        started_unix_ms: current_unix_ms(),
                    },
                );
            }
            guards.push(SnapshotGuard {
                db: Arc::clone(self),
                snapshot_id,
            });
        }
        guards
    }

    /// Create and register a read-only statement view as one operation with
    /// respect to the selected active-snapshot shard. Purge holds every shard
    /// while computing its reader/visibility horizon. Therefore it either
    /// observes this registration, or finishes sampling the horizon before
    /// the fresh `visible_scn` is chosen; it cannot purge between choosing the
    /// SCN and publishing the reader.
    pub fn read_only_statement_view_with_guard(
        self: &Arc<Self>,
        isolation: IsolationLevel,
        snapshot_scn: Option<u64>,
    ) -> (TxnView, SnapshotGuard) {
        let snapshot_id = self.next_snapshot_id.fetch_add(1, Ordering::Relaxed);
        let shard = (snapshot_id as usize) % self.active_snapshots.len();
        let chosen_scn;
        let mut snapshots = self.active_snapshots[shard]
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        chosen_scn = snapshot_scn.unwrap_or_else(|| self.fresh_snapshot_horizon());
        snapshots.insert(
            snapshot_id,
            ActiveSnapshot {
                snapshot_scn: chosen_scn,
                started_unix_ms: current_unix_ms(),
            },
        );
        (
            TxnView {
                snapshot_registered: true,
                ..TxnView::read_only(chosen_scn, isolation)
            },
            SnapshotGuard {
                db: Arc::clone(self),
                snapshot_id,
            },
        )
    }

    /// Transaction-scoped twin of [`Self::read_only_statement_view_with_guard`].
    /// The snapshot is selected while holding its destination snapshot shard,
    /// so purge cannot run between observing the commit frontier and publishing
    /// the reader. `snapshot_scn` is supplied only for an already-pinned RR
    /// view; READ COMMITTED callers pass `None` for a fresh statement horizon.
    pub fn session_statement_view_with_guard(
        self: &Arc<Self>,
        txn_id: u64,
        isolation: IsolationLevel,
        snapshot_scn: Option<u64>,
        read_only: bool,
    ) -> (TxnView, SnapshotGuard) {
        let snapshot_id = self.next_snapshot_id.fetch_add(1, Ordering::Relaxed);
        let shard = (snapshot_id as usize) % self.active_snapshots.len();
        let chosen_scn;
        let mut snapshots = self.active_snapshots[shard]
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        chosen_scn = snapshot_scn.unwrap_or_else(|| self.fresh_snapshot_horizon());
        snapshots.insert(
            snapshot_id,
            ActiveSnapshot {
                snapshot_scn: chosen_scn,
                started_unix_ms: current_unix_ms(),
            },
        );
        (
            TxnView {
                txn_id,
                snapshot_scn: chosen_scn,
                isolation,
                read_only,
                dirty_read: false,
                historical: false,
                historical_baseline_scn: 0,
                historical_baseline_txn_id: 0,
                connection_id: 0,
                snapshot_registered: true,
            },
            SnapshotGuard {
                db: Arc::clone(self),
                snapshot_id,
            },
        )
    }

    /// Internal synchronous scans borrow the database instead of owning an
    /// Arc. Select and register their frontier under the same snapshot shard
    /// lock as ordinary statement guards, closing the race with undo purge.
    fn borrowed_read_only_statement_view(&self) -> (TxnView, impl Drop + '_) {
        struct Guard<'a> {
            db: &'a Database,
            snapshot_id: u64,
        }
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                let shard = self.snapshot_id as usize % self.db.active_snapshots.len();
                self.db.active_snapshots[shard]
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .remove(&self.snapshot_id);
            }
        }
        let snapshot_id = self.next_snapshot_id.fetch_add(1, Ordering::Relaxed);
        let shard = snapshot_id as usize % self.active_snapshots.len();
        let mut snapshots = self.active_snapshots[shard]
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let snapshot_scn = self.fresh_snapshot_horizon();
        snapshots.insert(
            snapshot_id,
            ActiveSnapshot {
                snapshot_scn,
                started_unix_ms: current_unix_ms(),
            },
        );
        (
            TxnView {
                snapshot_registered: true,
                ..TxnView::read_only(snapshot_scn, IsolationLevel::ReadCommitted)
            },
            Guard {
                db: self,
                snapshot_id,
            },
        )
    }

    pub fn read_only_statement_view(
        &self,
        isolation: IsolationLevel,
        snapshot_scn: Option<u64>,
    ) -> TxnView {
        // Hot path: non-blocking. P0-1 — the snapshot comes from the published
        // commit frontier (`visible_scn`), the gap-aware watermark advanced
        // only AFTER a commit's slot is published. The previous code read the
        // raw allocation counter `next_scn_atomic`, whose comment ("bumped
        // under the txns write lock at commit time") was incorrect: it is
        // bumped lock-free at the START of `commit_txn`, ahead of publish — the
        // root of the commit-visibility atomicity bug.
        match snapshot_scn {
            Some(snapshot_scn) => TxnView::read_only(snapshot_scn, isolation),
            None => TxnView::read_only(self.fresh_snapshot_horizon(), isolation),
        }
    }

    pub fn lock_wait_timeout(&self) -> Duration {
        // A per-statement session override (`innodb_lock_wait_timeout`) wins
        // over the database-level default when the executor installed one.
        crate::storage::txn::lock_wait_timeout_override().unwrap_or(self.config.lock_wait)
    }

    pub fn wait_for_lock_signal(&self, started: Instant) -> Result<(), QueryError> {
        if crate::storage::txn::lock_waits_nonblocking() {
            return Err(crate::storage::txn::lock_would_block_error());
        }
        let lock_wait = self.lock_wait_timeout();
        let elapsed = started.elapsed();
        if elapsed >= lock_wait {
            // M2.5 - Record histogram bucket for the timed-out wait.
            let elapsed_ms = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
            {
                let active = self.txns.read().active_txn_count();
                let mut lm = self.lock_manager();
                lm.record_lock_wait_duration(elapsed_ms);
                if std::env::var_os("MYDB_LOCK_DIAGNOSTICS").is_some() {
                    log_message(format_args!(
                        "[lock-timeout] elapsed_ms={} {}",
                        elapsed_ms,
                        lm.lock_wait_diagnostics(16, active)
                    ));
                }
            }
            // M2.4 - Record wait event sample.
            self.record_wait_event(
                0,
                WaitEvent::RowLockContention,
                current_unix_ms().saturating_mul(1000),
                elapsed.as_micros().min(u128::from(u64::MAX)) as u64,
                "lock_wait_timeout",
            );
            return Err(error(
                crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                "lock wait timeout exceeded",
            ));
        }
        let remaining = lock_wait.saturating_sub(elapsed);
        // Register as a parked waiter only for the span of the actual block, so
        // uncontended paths keep `lock_wait_waiters` at 0 and the gate in
        // `notify_lock_waiters` stays effective.
        let _waiter = LockWaiterGuard::new(&self.lock_wait_waiters);
        let guard = self.lock_wait_mutex.lock().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "lock wait signal lock poisoned",
            )
        })?;
        drop(
            self.lock_wait_signal
                .wait_timeout(guard, remaining.min(Duration::from_millis(10)))
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "lock wait signal lock poisoned",
                    )
                })?,
        );
        Ok(())
    }

    pub fn wait_for_lock_signal_for_txn(
        &self,
        txn_id: Option<u64>,
        started: Instant,
    ) -> Result<(), QueryError> {
        let Some(txn_id) = txn_id else {
            return self.wait_for_lock_signal(started);
        };
        let blocked_row = if row_lock_sharding_enabled() {
            self.row_locks_sharded.blocked_waiting_row(txn_id)
        } else {
            self.lock_manager_read().blocked_row_wait(txn_id)
        };
        let Some((table_id, row_id)) = blocked_row else {
            return self.wait_for_lock_signal(started);
        };
        let signal = self.row_lock_signals.snapshot(row_id);
        let still_blocked = if row_lock_sharding_enabled() {
            self.row_locks_sharded.blocked_waiting_row(txn_id)
        } else {
            self.lock_manager_read().blocked_row_wait(txn_id)
        }
        .is_some();
        if !still_blocked {
            return Ok(());
        }
        // This entry point has already confirmed the txn is blocked and does not
        // track the requested mode; `Exclusive` is the conservative choice for
        // the per-row park guard (park whenever any other owner conflicts).
        self.wait_for_specific_row(
            txn_id,
            table_id,
            row_id,
            LockMode::Exclusive,
            signal,
            started,
            self.lock_wait_timeout(),
        )
    }

    pub fn notify_lock_waiters(&self) {
        // Skip the wake entirely when nobody is parked. The row-lock branch is
        // a 256-shard mutex+condvar storm (`RowLockSignals::notify_all`) that
        // every statement's MDL release used to fire unconditionally: pure
        // overhead, and a cross-thread serialization point, on any workload
        // without live lock contention (e.g. point lookups). The bounded
        // (<=20-50 ms) timeout in every `wait_for_*` loop makes a racing skip
        // self-healing, so the gate cannot hang a waiter.
        if self.lock_wait_waiters.load(Ordering::SeqCst) != 0 {
            self.lock_wait_signal.notify_all();
        }
        let parked_row_waiters = self.row_lock_waiters.load(Ordering::SeqCst) != 0;
        let registered_row_parkers = self.row_lock_parkers.active_count() != 0;
        if parked_row_waiters || registered_row_parkers {
            if row_lock_direct_wake_enabled() {
                // Park-based waiters don't listen on the shard condvar; this
                // coarse path can't enumerate unblocked rows, so wake them all
                // (cheap — only currently-parked waiters) and let each re-check.
                self.row_lock_signals.bump_all();
                if parked_row_waiters {
                    self.row_lock_parkers.unpark_all();
                }
            } else {
                self.row_lock_signals.notify_all();
            }
        }
    }

    fn wake_released_lock_waiters(
        &self,
        release: crate::storage::txn::LockRelease,
    ) -> Vec<(u64, RowId)> {
        if row_lock_direct_wake_enabled() {
            for waiter in release.affected_waiters {
                self.row_lock_parkers.unpark(waiter);
            }
        }
        release.rows
    }

    /// Wake waiters that use the global lock signal (MDL, unique-key and
    /// table-lock retry loops) without disturbing row waiters, which have
    /// their own row-specific directed wakeup path.
    ///
    /// In particular, statement-duration MDL is released after virtually
    /// every prepared statement. Broadcasting that release to row parkers
    /// makes an unrelated hot-row waiter repeatedly wake, fail the same row
    /// lock check and park again until its real owner commits.
    pub(crate) fn notify_non_row_lock_waiters(&self) {
        if self.lock_wait_waiters.load(Ordering::SeqCst) != 0 {
            self.lock_wait_signal.notify_all();
        }
    }

    /// DDL waits share the metadata/table release signal, without charging a
    /// row-lock timeout or inheriting the row-lock statement timeout. The
    /// caller owns its deadline/cancellation policy (cleanup is noncancelable).
    /// A bounded wait closes the race between a failed acquire and subscribing.
    fn wait_for_ddl_signal(&self, remaining: Duration) -> Result<(), QueryError> {
        let _waiter = LockWaiterGuard::new(&self.lock_wait_waiters);
        let guard = self.lock_wait_mutex.lock().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "DDL wait signal lock poisoned",
            )
        })?;
        drop(
            self.lock_wait_signal
                .wait_timeout(guard, remaining.min(Duration::from_millis(10)))
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "DDL wait signal lock poisoned",
                    )
                })?,
        );
        Ok(())
    }

    pub fn acquire_mdl(
        &self,
        owner_id: u64,
        owner_thread_id: Option<u64>,
        duration: MdlDuration,
        requests: Vec<MdlRequest>,
        source: &str,
    ) -> Result<(), QueryError> {
        self.mdl
            .acquire_many(owner_id, owner_thread_id, duration, requests, source)
    }

    pub fn acquire_cached_mdl(
        &self,
        owner_id: u64,
        owner_thread_id: Option<u64>,
        duration: MdlDuration,
        requests: &[MdlRequest],
        source: &str,
    ) -> Result<(), QueryError> {
        self.mdl
            .acquire_cached(owner_id, owner_thread_id, duration, requests, source)
    }

    /// Lock-free statement-duration SHARED_READ MDL acquisition (single
    /// request). `None` means "take the ticketed slow path"; a grant is
    /// released via [`Self::release_fast_mdl`] (or plain drop on unwind).
    pub fn try_acquire_fast_mdl(
        &self,
        requests: &[MdlRequest],
    ) -> Option<crate::storage::mdl::MdlFastGrant> {
        self.mdl.try_acquire_statement_fast(requests)
    }

    pub(crate) fn try_acquire_fast_mdl_cached(
        &self,
        requests: &[MdlRequest],
        cache: &std::cell::RefCell<crate::storage::mdl::MdlFastCache>,
    ) -> Option<crate::storage::mdl::MdlFastGrant> {
        self.mdl
            .try_acquire_statement_fast_cached(requests, &mut cache.borrow_mut())
    }

    /// Release a fast-path MDL grant. Wakes the bounded-timeout MDL
    /// retry loops only when a read-blocking acquire is parked on the
    /// object — the common uncontended release is two atomic ops.
    pub fn release_fast_mdl(&self, grant: crate::storage::mdl::MdlFastGrant) {
        if grant.release() {
            self.notify_non_row_lock_waiters();
        }
    }

    pub fn release_mdl_duration(&self, owner_id: u64, duration: MdlDuration) {
        if self.mdl.release_duration(owner_id, duration) {
            self.notify_non_row_lock_waiters();
        }
    }

    pub fn release_cached_mdl_duration(
        &self,
        owner_id: u64,
        duration: MdlDuration,
        requests: &[MdlRequest],
    ) {
        if self.mdl.release_cached(owner_id, duration, requests) {
            self.notify_non_row_lock_waiters();
        }
    }

    pub(crate) fn retain_streaming_fast_mdl(
        &self,
        grant: &crate::storage::mdl::MdlFastGrant,
        owner: u64,
        connection: u64,
    ) -> bool {
        self.mdl.retain_fast_read(grant, owner, connection)
    }

    pub(crate) fn retain_streaming_transaction_mdl(&self, owner: u64, cursor_owner: u64) -> bool {
        self.mdl.retain_transaction_reads(owner, cursor_owner)
    }

    pub fn transfer_mdl_duration_to_owner(
        &self,
        from_owner_id: u64,
        from_duration: MdlDuration,
        to_owner_id: u64,
        to_owner_thread_id: Option<u64>,
        to_duration: MdlDuration,
        source: &str,
    ) -> bool {
        self.mdl.transfer_duration_to_owner(
            from_owner_id,
            from_duration,
            to_owner_id,
            to_owner_thread_id,
            to_duration,
            source,
        )
    }

    pub fn release_mdl_owner(&self, owner_id: u64) {
        if self.mdl.release_owner(owner_id) {
            self.notify_non_row_lock_waiters();
        }
    }

    pub fn release_prepared_mdl(&self, txn_id: u64) {
        if self.mdl.release_prepared(txn_id) {
            self.notify_non_row_lock_waiters();
        }
    }

    pub fn transfer_transaction_mdl_to_prepared(&self, owner_id: u64, txn_id: u64) {
        self.mdl.transfer_transaction_to_prepared(owner_id, txn_id);
    }

    pub fn clear_mdl_waits_for_owner(&self, owner_id: u64) {
        if self.mdl.clear_pending_for_owner(owner_id) {
            self.notify_non_row_lock_waiters();
        }
    }

    pub fn release_explicit_table_mdl(&self, owner_id: u64) {
        if self.mdl.release_explicit_table_locks(owner_id) {
            self.notify_non_row_lock_waiters();
        }
    }

    pub fn metadata_lock_snapshot(&self) -> Vec<MetadataLockRow> {
        self.mdl.snapshot()
    }

    /// Notify only the shards that own the released row ids — avoids the
    /// global thundering herd that `notify_lock_waiters` triggers.
    /// `released` here is `(table_id, row_id)` pairs because the row
    /// lock map is keyed per-table. Only the `row_id` part feeds the
    /// shard hash though — that's still unique enough to spread the
    /// notifications across shards.
    #[allow(clippy::needless_pass_by_value)]
    pub fn notify_lock_waiters_for_rows<I: IntoIterator<Item = (u64, RowId)>>(&self, rows: I) {
        // Wake unique-key/table-lock waiters via the legacy signal (those
        // don't sit on a row shard), but only when some thread is parked.
        if self.lock_wait_waiters.load(Ordering::SeqCst) != 0 {
            self.lock_wait_signal.notify_all();
        }
        let async_waiters = self.row_lock_async_waiters.load(Ordering::Acquire) != 0;
        if row_lock_direct_wake_enabled() {
            let parked_row_waiters = self.row_lock_waiters.load(Ordering::SeqCst) != 0;
            let direct_waiters = parked_row_waiters || self.row_lock_parkers.active_count() != 0;
            if !direct_waiters && !async_waiters {
                return;
            }
            let rows: Vec<(u64, RowId)> = rows.into_iter().collect();
            if rows.is_empty() {
                return;
            }
            if async_waiters {
                self.notify_async_row_lock_shards(&rows);
            }
            if !direct_waiters {
                return;
            }
            // P4: wake only each released row's FIFO-front waiter (the sole
            // txn that can acquire next) instead of the whole shard. Bump the
            // counters first so a waiter that snapshotted before this skips
            // parking; then `unpark` the precise fronts. Do this even when
            // `row_lock_waiters == 0`: a waiter registers its thread before it
            // increments that parked-waiter counter, and a release in that
            // small window must still bump the row signal or the waiter can
            // park for the whole fallback slice while the row is already free.
            if !parked_row_waiters {
                self.row_lock_signals
                    .bump_rows(rows.iter().map(|(_table_id, rid)| *rid));
                return;
            }
            let mut fronts: Vec<u64> = Vec::new();
            if row_lock_sharding_enabled() {
                self.row_locks_sharded
                    .collect_front_row_waiters(&rows, &mut fronts);
            } else {
                self.lock_manager_read()
                    .collect_front_row_waiters(&rows, &mut fronts);
            }
            self.row_lock_signals
                .bump_rows(rows.iter().map(|(_table_id, rid)| *rid));
            for txn in fronts {
                self.row_lock_parkers.unpark(txn);
            }
        } else {
            let row_waiters = self.row_lock_waiters.load(Ordering::SeqCst) != 0;
            if !row_waiters && !async_waiters {
                return;
            }
            let rows: Vec<(u64, RowId)> = rows.into_iter().collect();
            if rows.is_empty() {
                return;
            }
            if async_waiters {
                self.notify_async_row_lock_shards(&rows);
            }
            if !row_waiters {
                return;
            }
            self.row_lock_signals
                .notify_rows(rows.iter().map(|(_table_id, rid)| *rid));
        }
    }

    fn notify_async_row_lock_shards(&self, rows: &[(u64, RowId)]) {
        let mut notified = [false; ASYNC_ROW_LOCK_SIGNAL_SHARDS];
        for &(table_id, row_id) in rows {
            let shard = async_row_lock_signal_shard(table_id, row_id);
            if notified[shard] {
                continue;
            }
            let signal = &self.row_lock_async_signals[shard];
            if signal.receiver_count() != 0 {
                signal.send_modify(|generation| *generation = generation.wrapping_add(1));
                notified[shard] = true;
            }
        }
    }

    pub(crate) fn subscribe_row_lock_async_signal(
        &self,
        target: crate::storage::NonblockingRowWaitTarget,
    ) -> crate::engine::api::AsyncRowLockSignal {
        let receiver = self.row_lock_async_signals
            [async_row_lock_signal_shard(target.table_id, target.row_id)]
        .subscribe();
        crate::engine::api::AsyncRowLockSignal::new(
            receiver,
            target.observed_generation,
            Arc::clone(&self.row_lock_async_waiters),
        )
    }

    /// Try to acquire an exclusive row lock; if blocked, wait inline on the
    /// row's shard condvar until either the lock is granted or the configured
    /// wait timeout elapses. Avoids unwinding the entire statement on every
    /// retry, which is the dominant cost under hot-row contention (e.g.,
    /// TPC-C district/warehouse rows).
    pub fn acquire_row_lock_blocking(
        &self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
        mode: LockMode,
    ) -> Result<(), QueryError> {
        if crate::storage::txn::lock_waits_nonblocking() {
            let signal_shard = async_row_lock_signal_shard(table_id, row_id);
            let observed_generation = *self.row_lock_async_signals[signal_shard].borrow();
            if self.try_row_lock_without_wait(txn_id, table_id, row_id, mode)? {
                return Ok(());
            }
            crate::storage::txn::record_nonblocking_row_wait_target(
                table_id,
                row_id,
                observed_generation,
            );
            return Err(crate::storage::txn::lock_would_block_error());
        }
        let started = Instant::now();
        let timeout = self.lock_wait_timeout();
        // P4: register this thread so a lock release can `unpark` exactly this
        // waiter (the guard unregisters on every exit path). No-op cost when
        // direct wake is disabled — we simply never consult the registry.
        let _park_guard =
            row_lock_direct_wake_enabled().then(|| self.row_lock_parkers.register(txn_id));
        loop {
            crate::engine::cancel::check_cancelled()?;
            // Snapshot the shard counter BEFORE the (possibly failing)
            // acquire so `wait_after` can detect a notification that fires
            // between the failed acquire and the call to wait. Without this,
            // a notify can be lost and the waiter sleeps a full slice with
            // the lock already free.
            let signal = self.row_lock_signals.snapshot(row_id);
            let acquire_result = if row_lock_sharding_enabled() {
                self.row_lock_acquire_sharded(txn_id, table_id, row_id, mode, true)
            } else {
                self.lock_manager()
                    .acquire_row_lock_with_mode(txn_id, table_id, row_id, mode, true)
            };
            match acquire_result {
                Ok(()) => return Ok(()),
                Err(err) if is_lock_wait_pending_error(&err) => {
                    // Re-enqueued in `record_lock_wait`; fall through to wait.
                }
                Err(err) => {
                    self.clear_blocking_lock_wait(txn_id);
                    return Err(err);
                }
            }
            if let Err(err) =
                self.wait_for_specific_row(txn_id, table_id, row_id, mode, signal, started, timeout)
            {
                self.clear_blocking_lock_wait(txn_id);
                return Err(err);
            }
            if let Err(err) = crate::engine::cancel::check_cancelled() {
                self.clear_blocking_lock_wait(txn_id);
                return Err(err);
            }
        }
    }

    /// Acquire X locks on a batch of rows belonging to the same table,
    /// blocking on each in order.
    pub fn lock_rows_blocking(
        &self,
        txn_id: u64,
        table_id: u64,
        row_ids: Vec<RowId>,
    ) -> Result<(), QueryError> {
        self.lock_rows_blocking_mode(txn_id, table_id, row_ids, LockMode::Exclusive)
    }

    /// Batch row-lock acquisition with a fast/slow split: one latch grabs
    /// every free, un-queued row at once (recorded as a single batch
    /// lock-log entry); only conflicting rows or S→X upgrades fall through to
    /// per-row blocking. Backs `SELECT … FOR UPDATE` (X) and `FOR SHARE` (S).
    pub fn lock_rows_blocking_mode(
        &self,
        txn_id: u64,
        table_id: u64,
        row_ids: Vec<RowId>,
        mode: LockMode,
    ) -> Result<(), QueryError> {
        if row_ids.is_empty() {
            return Ok(());
        }
        let deferred = if row_lock_sharding_enabled() {
            self.row_lock_batch_sharded(txn_id, table_id, &row_ids, mode)?
        } else {
            let mut lm = self.lock_manager();
            lm.check_lock_capacity(row_ids.len())?;
            lm.lock_rows_batch_nonblocking(txn_id, table_id, &row_ids, mode)
        };
        for row_id in deferred {
            self.acquire_row_lock_blocking(txn_id, table_id, row_id, mode)?;
        }
        Ok(())
    }

    /// Non-blocking point probe for another transaction's reservation of a
    /// unique key. A table without any reservation skips the lock-manager
    /// latch; otherwise the exact latched check decides.
    pub(crate) fn check_unique_key_reservation(
        &self,
        txn_id: u64,
        table_id: u64,
        index_name: &str,
        key: &IndexKey,
    ) -> Result<(), QueryError> {
        if !self.unique_reservations.may_have_reservations(table_id) {
            return Ok(());
        }
        self.lock_manager().check_unique_key_lock(
            txn_id,
            table_id,
            index_name,
            key,
            &self.lock_owner_status(),
        )
    }

    pub fn check_unique_key_lock_blocking(
        &self,
        txn_id: u64,
        table_id: u64,
        index_name: &str,
        key: &IndexKey,
    ) -> Result<bool, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        // No reservation on the table: nothing to wait for, and no latch.
        if !self.unique_reservations.may_have_reservations(table_id) {
            return Ok(false);
        }
        if self.lock_manager_read().unique_key_uncontended(
            txn_id,
            table_id,
            index_name,
            key,
            &self.lock_owner_status(),
        ) {
            return Ok(false);
        }
        let started = Instant::now();
        let timeout = self.lock_wait_timeout();
        let _park_guard =
            row_lock_direct_wake_enabled().then(|| self.row_lock_parkers.register(txn_id));
        let mut waited = false;
        loop {
            crate::engine::cancel::check_cancelled()?;
            let acquire_result = self.lock_manager().acquire_unique_key_lock(
                txn_id,
                table_id,
                index_name,
                key,
                false,
                &self.lock_owner_status(),
            );
            match acquire_result {
                Ok(()) => return Ok(waited),
                Err(err) if is_lock_wait_pending_error(&err) => {
                    waited = true;
                }
                Err(err) => {
                    self.clear_blocking_lock_wait(txn_id);
                    return Err(err);
                }
            }
            if let Err(err) = self.wait_for_global_signal(started, timeout) {
                self.clear_blocking_lock_wait(txn_id);
                return Err(err);
            }
        }
    }

    /// Revalidate a missing unique-index candidate while preventing another
    /// writer from reserving/removing that key between the ownership check
    /// and the probe. Only the rare miss path holds the lock directory across
    /// an index read; ordinary hits keep the existing row-lock/reprobe path.
    fn recheck_unique_index_miss<T>(
        &self,
        txn_id: u64,
        table_id: u64,
        index_name: &str,
        key: &IndexKey,
        probe: impl FnOnce() -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        {
            // A shared guard also prevents a new key reservation throughout
            // the index recheck, without excluding unrelated check-only reads.
            let locks = self.lock_manager_read();
            if locks.unique_key_uncontended(
                txn_id,
                table_id,
                index_name,
                key,
                &self.lock_owner_status(),
            ) {
                return probe();
            }
        }
        let started = Instant::now();
        let timeout = self.lock_wait_timeout();
        let _park_guard =
            row_lock_direct_wake_enabled().then(|| self.row_lock_parkers.register(txn_id));
        loop {
            crate::engine::cancel::check_cancelled()?;
            let mut locks = self.lock_manager();
            match locks.check_unique_key_lock(
                txn_id,
                table_id,
                index_name,
                key,
                &self.lock_owner_status(),
            ) {
                Ok(()) => {
                    let result = probe();
                    drop(locks);
                    return result;
                }
                Err(err) => {
                    drop(locks);
                    if !is_lock_wait_pending_error(&err) {
                        self.clear_blocking_lock_wait(txn_id);
                        return Err(err);
                    }
                }
            }
            if let Err(err) = self.wait_for_global_signal(started, timeout) {
                self.clear_blocking_lock_wait(txn_id);
                return Err(err);
            }
        }
    }

    fn clear_blocking_lock_wait(&self, txn_id: u64) {
        if row_lock_sharding_enabled() {
            self.row_locks_sharded.clear_wait(txn_id);
        }
        self.lock_manager().clear_wait(txn_id);
        self.notify_lock_waiters();
    }

    fn wait_for_specific_row(
        &self,
        txn_id: u64,
        table_id: u64,
        row_id: RowId,
        mode: LockMode,
        prior_signal: u64,
        started: Instant,
        timeout: Duration,
    ) -> Result<(), QueryError> {
        if crate::storage::txn::lock_waits_nonblocking() {
            return Err(crate::storage::txn::lock_would_block_error());
        }
        let elapsed = started.elapsed();
        crate::engine::cancel::check_cancelled()?;
        if elapsed >= timeout {
            return Err(self.lock_wait_timeout_error(elapsed));
        }
        let remaining = timeout.saturating_sub(elapsed);
        // Cap each wait so the deadlock detector (run on the next acquire
        // attempt) can fire even when notifications are missed.
        let max_slice = Duration::from_millis(50);
        let wait_started = Instant::now();
        {
            let _waiter = LockWaiterGuard::new(&self.row_lock_waiters);
            let slice = remaining.min(max_slice);
            if row_lock_direct_wake_enabled() {
                // P4: park this specific thread. A release for our row `unpark`s
                // us directly (std token-based, so a wake that races ahead of
                // the park is not lost); the slice is the self-healing backstop.
                //
                // Park only while our *exact* row is still blocked. The old
                // guard compared a per-shard signal counter that advanced on
                // every release of any co-located row, so a waiter on a hot row
                // (TPC-C district/warehouse) skipped parking and busy-retried
                // through the global lock-manager latch thousands of times per
                // acquire — a self-inflicted wakeup storm that dominated CPU and
                // serialized every committer under 100-terminal contention. The
                // precise per-row check parks in that common case yet still
                // catches the genuine "row freed in the window" race (returns
                // false → we skip the park and retry immediately).
                let still_blocked = if row_lock_sharding_enabled() {
                    self.row_locks_sharded
                        .still_blocked(txn_id, table_id, row_id, mode)
                } else {
                    self.row_lock_signals.snapshot(row_id) == prior_signal
                };
                if still_blocked {
                    std::thread::park_timeout(slice);
                }
            } else {
                self.row_lock_signals
                    .wait_after(row_id, prior_signal, slice);
            }
        }
        crate::engine::perf_events::record_lock_wait_sample(wait_started.elapsed());
        crate::engine::cancel::check_cancelled()
    }

    fn wait_for_global_signal(
        &self,
        started: Instant,
        timeout: Duration,
    ) -> Result<(), QueryError> {
        if crate::storage::txn::lock_waits_nonblocking() {
            return Err(crate::storage::txn::lock_would_block_error());
        }
        let elapsed = started.elapsed();
        crate::engine::cancel::check_cancelled()?;
        if elapsed >= timeout {
            return Err(self.lock_wait_timeout_error(elapsed));
        }
        let remaining = timeout.saturating_sub(elapsed);
        let _waiter = LockWaiterGuard::new(&self.lock_wait_waiters);
        let wait_started = Instant::now();
        let slice = remaining.min(Duration::from_millis(20));
        if row_lock_direct_wake_enabled() {
            std::thread::park_timeout(slice);
        } else {
            let guard = self.lock_wait_mutex.lock().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "lock wait signal lock poisoned",
                )
            })?;
            drop(
                self.lock_wait_signal
                    .wait_timeout(guard, slice)
                    .map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "lock wait signal lock poisoned",
                        )
                    })?,
            );
        }
        crate::engine::perf_events::record_lock_wait_sample(wait_started.elapsed());
        crate::engine::cancel::check_cancelled()
    }

    fn lock_wait_timeout_error(&self, elapsed: Duration) -> QueryError {
        let elapsed_ms = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
        {
            let active = self.txns.read().active_txn_count();
            let mut lm = self.lock_manager();
            lm.record_lock_wait_duration(elapsed_ms);
            if std::env::var_os("MYDB_LOCK_DIAGNOSTICS").is_some() {
                log_message(format_args!(
                    "[lock-timeout] elapsed_ms={} {}",
                    elapsed_ms,
                    lm.lock_wait_diagnostics(16, active)
                ));
            }
        }
        self.record_wait_event(
            0,
            WaitEvent::RowLockContention,
            current_unix_ms().saturating_mul(1000),
            elapsed.as_micros().min(u128::from(u64::MAX)) as u64,
            "lock_wait_timeout",
        );
        error(
            crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
            "lock wait timeout exceeded",
        )
    }

    pub fn buffer_pool_pages(&self) -> u64 {
        self.runtime_config_read().buffer_pool_pages
    }

    // CFG-1 - per-kind buffer-pool soft-quota percentages, runtime
    // readable/settable (slot 0 = heap, 1 = index, 2 = LOB).
    pub fn buffer_pool_quota_heap_pct(&self) -> u64 {
        self.buffer_pool.kind_quota_pct(0)
    }

    pub fn buffer_pool_quota_index_pct(&self) -> u64 {
        self.buffer_pool.kind_quota_pct(1)
    }

    pub fn buffer_pool_quota_lob_pct(&self) -> u64 {
        self.buffer_pool.kind_quota_pct(2)
    }

    pub fn buffer_pool_quota_zone_map_pct(&self) -> u64 {
        self.buffer_pool.kind_quota_pct(4)
    }

    pub fn set_buffer_pool_quota_pct(&self, slot: usize, pct: u64) -> Result<(), QueryError> {
        self.buffer_pool.set_kind_quota_pct(slot, pct);
        Ok(())
    }

    // CFG-1 - configured base prefetch window (pages), runtime
    // readable/settable. 0 disables read-ahead.
    pub fn buffer_pool_prefetch_pages(&self) -> u64 {
        self.buffer_pool.prefetch_base_pages() as u64
    }

    pub fn set_buffer_pool_prefetch_pages(&self, pages: u64) -> Result<(), QueryError> {
        self.buffer_pool.set_prefetch_base_pages(pages as usize);
        Ok(())
    }

    pub fn buffer_pool_small_scan_pct(&self) -> u64 {
        self.buffer_pool.small_scan_pct()
    }

    pub fn set_buffer_pool_small_scan_pct(&self, pct: u64) -> Result<(), QueryError> {
        self.buffer_pool.set_small_scan_pct(pct);
        Ok(())
    }

    pub fn checkpoint_wal_bytes(&self) -> u64 {
        self.runtime_config_read().checkpoint_wal_bytes
    }

    pub fn undo_retention_ms(&self) -> u64 {
        self.runtime_config_read()
            .undo_retention
            .as_millis()
            .min(u128::from(u64::MAX)) as u64
    }

    pub(crate) fn record_temporal_query(&self) {
        self.temporal_query_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_temporal_heap_fallback(&self) {
        self.temporal_heap_fallback_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_temporal_history_index_hit(&self) {
        self.temporal_history_index_hit_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn historical_view_as_of(
        &self,
        target_unix_us: u64,
        statement_start_unix_us: u64,
    ) -> Result<TxnView, QueryError> {
        let statement_start_unix_us = if self.is_snapshot_read_only() {
            self.recovery_context
                .target_time_us
                .unwrap_or(statement_start_unix_us)
        } else {
            statement_start_unix_us
        };
        let visible_scn = self.visible_scn();
        let baseline = self.temporal_history.baseline();
        let with_baseline = |view: TxnView| {
            baseline.map_or(view, |baseline| {
                view.with_historical_baseline(baseline.snapshot_scn, baseline.first_tracked_txn_id)
            })
        };
        // A future target is defined as the latest committed prefix visible at
        // statement start. It must not wait for, or include, later commits.
        if target_unix_us >= statement_start_unix_us {
            return Ok(with_baseline(TxnView::historical(visible_scn)));
        }
        let retention_us = self.undo_retention_ms().saturating_mul(1_000);
        let retention_start = statement_start_unix_us.saturating_sub(retention_us);
        let earliest_allowed = baseline
            .map(|baseline| retention_start.max(baseline.available_unix_us))
            .unwrap_or(retention_start);
        if retention_us == 0 || target_unix_us < earliest_allowed {
            self.snapshot_too_old_total.fetch_add(1, Ordering::Relaxed);
            return Err(error(
                crate::error::catalog::MYDB_ER_SYSTEM_TIME_TOO_OLD,
                format!(
                    "FOR SYSTEM_TIME AS OF target {target_unix_us}us is older than the available history window (earliest {earliest_allowed}us, mydb_undo_retention_ms={})",
                    self.undo_retention_ms(),
                ),
            ));
        }
        let snapshot_scn = self
            .temporal_history
            .scn_as_of(target_unix_us, visible_scn)
            .ok_or_else(|| {
                self.snapshot_too_old_total.fetch_add(1, Ordering::Relaxed);
                error(
                    crate::error::catalog::MYDB_ER_SYSTEM_TIME_TOO_OLD,
                    format!(
                        "FOR SYSTEM_TIME AS OF target {target_unix_us}us is older than the available commit timeline"
                    ),
                )
            })?;
        Ok(with_baseline(TxnView::historical(snapshot_scn)))
    }

    pub fn undo_segment_bytes(&self) -> u64 {
        self.runtime_config_read().undo_segment_bytes
    }

    /// P1/9 — runtime read of the undo write-buffer threshold (sysvar
    /// `mydb_undo_buffer_bytes`). Lives in the UndoManager since it is
    /// applied to its `buffer_limit` field directly.
    pub fn undo_buffer_bytes(&self) -> u64 {
        self.undo
            .try_read()
            .map(|undo| undo.buffer_limit())
            .unwrap_or(0)
    }

    /// P2/12 — disk-cap soft-fail warning toggle (sysvar
    /// `mydb_undo_disk_cap_softfail_warn`).
    pub fn undo_disk_cap_softfail_warn(&self) -> bool {
        self.undo
            .try_read()
            .map(|undo| undo.disk_cap_softfail_warn())
            .unwrap_or(false)
    }

    pub fn set_undo_disk_cap_softfail_warn(&self, enabled: bool) -> Result<(), QueryError> {
        let mut undo = self.undo_lock()?;
        undo.set_disk_cap_softfail_warn(enabled);
        Ok(())
    }

    /// P2/12 — cumulative count of warnings emitted by the soft-fail
    /// path. Used by SHOW ENGINE MYDB STATUS.
    pub fn undo_disk_cap_softfail_warns_emitted(&self) -> u64 {
        self.undo
            .try_read()
            .map(|undo| undo.disk_cap_softfail_warns_emitted)
            .unwrap_or(0)
    }

    /// P1/6 — number of rollback-segment write shards. Surfaced via
    /// `SHOW ENGINE MYDB STATUS` so operators can confirm
    /// `MYDB_UNDO_WRITE_SHARDS` took effect.
    pub fn undo_shard_count(&self) -> u64 {
        self.undo
            .try_read()
            .map(|undo| undo.shard_count() as u64)
            .unwrap_or(0)
    }

    /// P2/13 — surface the live undo mirror as a flat record list for
    /// `information_schema.mydb_undo_history`. Each entry exposes the
    /// fields the view aggregates by `(txn_id, table_id)`. Returns
    /// `None` if the undo lock is unavailable (avoid blocking the
    /// catalog query when a writer is mid-append).
    pub fn collect_undo_history(&self) -> Option<Vec<UndoHistoryEntry>> {
        let undo = self.undo.try_read()?;
        let mut out = Vec::with_capacity(undo.records.len());
        for record in undo.records.values() {
            out.push(UndoHistoryEntry {
                txn_id: record.txn_id,
                table_id: record.table_id,
                bytes: u64::from(record.cached_size),
                chain_depth: record.chain_depth,
                created_unix_ms: record.created_unix_ms,
            });
        }
        Some(out)
    }

    /// P2/14 — snapshot of the undo I/O latency counters surfaced via
    /// `SHOW ENGINE MYDB STATUS`. Returns
    /// `(write_frame_samples, write_frame_ns_total, write_frame_p50_ns,
    ///   write_frame_p99_ns, flush_samples, flush_ns_total, flush_p50_ns,
    ///   flush_p99_ns, scan_total_ns)`. All values are monotonic since
    /// process start; `_p50/_p99` are log-bucket approximations (factor
    /// of 10).
    pub fn undo_io_latency_snapshot(&self) -> (u64, u64, u64, u64, u64, u64, u64, u64, u64) {
        let Some(undo) = self.undo.try_read() else {
            return (0, 0, 0, 0, 0, 0, 0, 0, 0);
        };
        (
            undo.write_frame_latency_samples(),
            undo.write_frame_latency_total_ns(),
            undo.write_frame_latency_percentile(50),
            undo.write_frame_latency_percentile(99),
            undo.flush_latency_samples(),
            undo.flush_latency_total_ns(),
            undo.flush_latency_percentile(50),
            undo.flush_latency_percentile(99),
            undo.scan_total_ns.load(Ordering::Relaxed),
        )
    }

    pub fn undo_purge_interval_ms(&self) -> u64 {
        self.runtime_config_read()
            .undo_purge_interval
            .as_millis()
            .min(u128::from(u64::MAX)) as u64
    }

    pub fn undo_purge_batch_records(&self) -> u64 {
        self.runtime_config_read().undo_purge_batch_records as u64
    }

    pub fn undo_tablespace_max_bytes(&self) -> u64 {
        self.runtime_config_read().undo_tablespace_max_bytes
    }

    pub fn undo_retention_guarantee(&self) -> bool {
        self.runtime_config_read().undo_retention_guarantee
    }

    pub fn set_buffer_pool_pages(&self, pages: u64) -> Result<(), QueryError> {
        static RESIZE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        if pages == 0 {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                "variable 'mydb_buffer_pool_pages' cannot be set to 0",
            ));
        }
        let _resize_guard = RESIZE_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "buffer pool resize lock poisoned",
                )
            })?;
        let old_pages = self.buffer_pool_pages();
        crate::engine::executor::mem_budget::validate_controlled_memory_split(pages)?;
        crate::engine::executor::mem_budget::set_controlled_buffer_limit_pages(pages);
        // BUG-3 - bound the resize wait. A shrink blocked by pinned pages
        // should fail promptly (the operator can retry) rather than idle
        // the connection for the full lock_wait (default 30s). Evictable
        // pages are still reclaimed without waiting; this cap only bounds
        // the idle wait when no clean victim can be found, while staying
        // long enough to outlast a transient pin from a concurrent query.
        let resize_wait = self.config.lock_wait.min(Duration::from_secs(2));
        if let Err(err) = set_configured_buffer_pool_capacity(self, pages, resize_wait) {
            crate::engine::executor::mem_budget::set_controlled_buffer_limit_pages(old_pages);
            return Err(err);
        }
        let buffer_limit = crate::engine::executor::mem_budget::controlled_buffer_limit_bytes();
        if let Err(err) = self
            .buffer_pool
            .reclaim_for_buffer_limit(buffer_limit, resize_wait)
        {
            crate::engine::executor::mem_budget::set_controlled_buffer_limit_pages(old_pages);
            let _ = set_configured_buffer_pool_capacity(self, old_pages, resize_wait);
            return Err(err);
        }
        match self.runtime_config_write() {
            Ok(mut config) => {
                config.buffer_pool_pages = pages;
                Ok(())
            }
            Err(err) => {
                let _ = set_configured_buffer_pool_capacity(self, old_pages, resize_wait);
                crate::engine::executor::mem_budget::set_controlled_buffer_limit_pages(old_pages);
                Err(err)
            }
        }
    }

    pub(crate) fn reclaim_for_memory_target(&self, target_bytes: usize) -> Result<(), QueryError> {
        let timeout = self.config.lock_wait.min(Duration::from_secs(2));
        self.reclaim_zone_maps_for_memory_target(target_bytes);
        self.buffer_pool
            .reclaim_for_memory_target(target_bytes, timeout)
    }

    /// Bounded query-admission reclaim. It never changes configured buffer
    /// capacity: only optional zone maps, recycled raw pages and evictable
    /// resident frames are released. Pinned frames are skipped and the caller
    /// retains the original 1037 if the target cannot be reached in time.
    pub(crate) fn reclaim_for_query_admission(
        &self,
        target_bytes: usize,
    ) -> Result<(), QueryError> {
        self.reclaim_zone_maps_for_memory_target(target_bytes);
        self.buffer_pool
            .reclaim_for_memory_target(target_bytes, Duration::from_millis(100))
    }

    /// Physical-pressure hints release only optional clean state. Dirty or
    /// pinned pages remain owned by their existing WAL/writeback protocol.
    pub(crate) fn reclaim_clean_for_query_admission(
        &self,
        target_bytes: usize,
    ) -> Result<(), QueryError> {
        // Lightweight descriptors do not pin their raw pages. Dropping them
        // here saves little physical memory but repeatedly rebuilds metadata
        // during index probes. Hard admission still reclaims those handles.
        self.buffer_pool
            .reclaim_clean_for_memory_target(target_bytes, Duration::from_millis(10))
    }

    pub fn set_checkpoint_wal_bytes(&self, bytes: u64) -> Result<(), QueryError> {
        if bytes == 0 {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                "variable 'mydb_checkpoint_wal_bytes' cannot be set to 0",
            ));
        }
        self.runtime_config_write()?.checkpoint_wal_bytes = bytes;
        Ok(())
    }

    pub fn set_undo_retention_ms(&self, ms: u64) -> Result<(), QueryError> {
        let _admission = crate::storage::capture::Admission::enter(self)?;
        // Serialize concurrent SET GLOBAL operations and keep the published
        // runtime value unchanged until the matching durable baseline update
        // succeeds.
        let mut runtime = self.runtime_config_write()?;
        let old_ms = runtime.undo_retention.as_millis().min(u128::from(u64::MAX)) as u64;
        if old_ms != ms && self.capture.redo_required() {
            return Err(error(
                crate::error::catalog::MYDB_ER_BACKUP_FAILED,
                "stop WAL protection before changing temporal history retention",
            ));
        }
        if old_ms > 0 && ms == 0 {
            self.temporal_history.clear_baseline()?;
            runtime.undo_retention = Duration::ZERO;
            return Ok(());
        }
        if old_ms == 0 && ms > 0 {
            // Freeze begin/commit publication while installing the baseline.
            // Refuse an ambiguous transition instead of guessing visibility
            // for a writer that started while history tracking was disabled.
            let txns = self.txns_write()?;
            if txns.active_write_count != 0 {
                return Err(error(
                    crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                    "cannot enable mydb_undo_retention_ms while write transactions are active; retry after they finish",
                ));
            }
            let snapshot_scn = txns.commit_frontier;
            let first_tracked_txn_id = self.next_txn_id_atomic.load(Ordering::Acquire);
            let available_unix_us = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros()
                .min(u128::from(u64::MAX)) as u64;
            self.temporal_history
                .establish_baseline(super::temporal::TemporalBaseline {
                    first_tracked_txn_id,
                    snapshot_scn,
                    available_unix_us,
                })?;
            self.publish_visible_frontier(snapshot_scn);
            runtime.undo_retention = Duration::from_millis(ms);
            drop(txns);
            return Ok(());
        }
        runtime.undo_retention = Duration::from_millis(ms);
        Ok(())
    }

    pub(crate) fn initialize_temporal_baseline(&self) -> Result<(), QueryError> {
        if self.undo_retention_ms() == 0 {
            return self.temporal_history.clear_baseline();
        }
        if self.temporal_history.baseline().is_some() {
            return Ok(());
        }
        let txns = self.txns_write()?;
        let baseline = super::temporal::TemporalBaseline {
            first_tracked_txn_id: self.next_txn_id_atomic.load(Ordering::Acquire),
            snapshot_scn: txns.commit_frontier,
            available_unix_us: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros()
                .min(u128::from(u64::MAX)) as u64,
        };
        self.temporal_history.establish_baseline(baseline)?;
        self.publish_visible_frontier(baseline.snapshot_scn);
        Ok(())
    }

    pub fn wal_write_buffer_bytes(&self) -> u64 {
        self.log.lock().write_buffer_bytes()
    }

    pub fn set_wal_write_buffer_bytes(&self, bytes: u64) -> Result<(), QueryError> {
        self.log.lock().set_write_buffer_bytes(bytes)
    }

    /// Runtime group-commit leader wait window, microseconds. `0` disables the
    /// window (natural batching). No upper clamp beyond `u64`; operators are
    /// trusted to pick a latency budget.
    pub fn group_commit_us(&self) -> u64 {
        self.runtime_config_read().group_commit_us
    }

    pub fn set_group_commit_us(&self, us: u64) -> Result<(), QueryError> {
        self.runtime_config_write()?.group_commit_us = us;
        Ok(())
    }

    pub fn group_commit_adaptive(&self) -> u64 {
        u64::from(self.runtime_config_read().group_commit_adaptive)
    }

    pub fn set_group_commit_adaptive(&self, on: bool) -> Result<(), QueryError> {
        self.runtime_config_write()?.group_commit_adaptive = on;
        Ok(())
    }

    pub fn set_undo_segment_bytes(&self, bytes: u64) -> Result<(), QueryError> {
        {
            let mut undo = self.undo_lock()?;
            undo.set_segment_bytes(bytes)?;
        }
        self.runtime_config_write()?.undo_segment_bytes = bytes;
        Ok(())
    }

    /// P1/9 — set the undo write-buffer threshold at runtime. Value of
    /// `0` switches the manager to write-through (the pre-buffer path);
    /// any other value enables buffered append with that flush cap.
    pub fn set_undo_buffer_bytes(&self, bytes: u64) -> Result<(), QueryError> {
        let mut undo = self.undo_lock()?;
        undo.set_buffer_limit(bytes)
    }

    pub fn set_undo_purge_interval_ms(&self, ms: u64) -> Result<(), QueryError> {
        self.runtime_config_write()?.undo_purge_interval = Duration::from_millis(ms);
        Ok(())
    }

    pub fn set_undo_purge_batch_records(&self, records: u64) -> Result<(), QueryError> {
        if records == 0 {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                "variable 'mydb_undo_purge_batch_records' cannot be set to 0",
            ));
        }
        self.runtime_config_write()?.undo_purge_batch_records = records as usize;
        Ok(())
    }

    /// M3.2 - Toggle undo retention guarantee mode.
    pub fn set_undo_retention_guarantee(&self, enabled: bool) -> Result<(), QueryError> {
        if enabled {
            self.undo_concurrent_append_eligible
                .store(false, Ordering::Release);
        }
        self.undo_lock()?.set_retention_guarantee(enabled);
        self.runtime_config_write()?.undo_retention_guarantee = enabled;
        if !enabled && self.runtime_config_read().undo_txn_max_bytes == 0 {
            self.undo_concurrent_append_eligible
                .store(true, Ordering::Release);
        }
        Ok(())
    }

    /// M3.2 - Set max undo tablespace size in bytes.
    pub fn set_undo_tablespace_max_bytes(&self, bytes: u64) -> Result<(), QueryError> {
        self.undo_lock()?.set_tablespace_max_bytes(bytes);
        self.runtime_config_write()?.undo_tablespace_max_bytes = bytes;
        Ok(())
    }

    /// P0/B2 - Set the in-memory undo `records` soft cap in bytes (0 =
    /// disabled). Over-cap fires an immediate `purge_obsolete_undo()`.
    pub fn set_undo_mem_soft_limit_bytes(&self, bytes: u64) -> Result<(), QueryError> {
        self.undo_lock()?.set_mem_soft_limit_bytes(bytes);
        self.runtime_config_write()?.undo_mem_soft_limit_bytes = bytes;
        Ok(())
    }

    /// Phase 2 — set the resident undo payload cache cap in bytes (0 =
    /// unbounded / no eviction). Lowering it evicts immediately; a
    /// consistent read pages evicted before-images back in from disk.
    pub fn set_undo_mem_cache_limit_bytes(&self, bytes: u64) -> Result<(), QueryError> {
        self.undo_lock()?.set_mem_cache_limit_bytes(bytes);
        self.runtime_config_write()?.undo_mem_cache_limit_bytes = bytes;
        Ok(())
    }

    pub fn undo_mem_cache_limit_bytes(&self) -> u64 {
        self.runtime_config_read().undo_mem_cache_limit_bytes
    }

    /// Phase 4 — per-transaction undo byte cap (0 = unlimited).
    pub fn set_undo_txn_max_bytes(&self, bytes: u64) -> Result<(), QueryError> {
        if bytes != 0 {
            self.undo_concurrent_append_eligible
                .store(false, Ordering::Release);
        }
        self.undo_lock()?.set_txn_max_bytes(bytes);
        self.runtime_config_write()?.undo_txn_max_bytes = bytes;
        if bytes == 0 && !self.runtime_config_read().undo_retention_guarantee {
            self.undo_concurrent_append_eligible
                .store(true, Ordering::Release);
        }
        Ok(())
    }

    pub fn undo_txn_max_bytes(&self) -> u64 {
        self.runtime_config_read().undo_txn_max_bytes
    }

    pub fn record_plan_cache_hit(&self) {
        self.plan_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_plan_cache_miss(&self) {
        self.plan_cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_select_parse_cache_hit(&self) {
        self.select_parse_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_select_compiled_plan_hit(&self) {
        self.select_compiled_plan_hits
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_select_operator_compile(&self) {
        self.select_operator_compiles
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_prepared_plan_reuse(&self) {
        self.prepared_plan_reuses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_prepared_plan_compile(&self) {
        self.prepared_plan_compiles.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_prepared_dml_reuse(&self) {
        self.prepared_dml_reuses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_prepared_dml_compile(&self) {
        self.prepared_dml_compiles.fetch_add(1, Ordering::Relaxed);
    }

    /// Configured durability mode. Used by the physical-replication
    /// sender to decide whether shipped WAL must be capped at the
    /// durable (fsynced) LSN.
    pub fn durability_mode(&self) -> DurabilityMode {
        self.config.durability
    }

    /// Physical-replica promotion: compensate every transaction abandoned by
    /// the old source before accepting writes. Online WAL apply retains the
    /// replicated undo records, so use the normal rollback funnel rather than
    /// merely flipping slot status; the latter leaves heap/index mutations in
    /// place and they can become visible after undo/slot GC.
    pub fn rollback_all_active_replication_txns(&self) -> Result<usize, QueryError> {
        let active_ids: Vec<u64> = self
            .txns
            .read()
            .slots
            .values()
            .filter(|slot| slot.status == TxnStatus::Active)
            .map(|slot| slot.txn_id)
            .collect();
        if !active_ids.is_empty() {
            self.validate_replication_rollback_coverage(&active_ids)?;
        }
        crate::wal::with_promotion_wal(|| {
            for txn_id in active_ids.iter().copied() {
                self.rollback_txn(txn_id)?;
            }
            Ok(active_ids.len())
        })
    }

    fn validate_replication_rollback_coverage(&self, active_ids: &[u64]) -> Result<(), QueryError> {
        let mut charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::StorageScratch,
            active_ids.len().saturating_mul(64).saturating_add(4096),
        )?;
        let active = active_ids.iter().copied().collect::<HashSet<_>>();
        let mut reachable = HashSet::new();
        let mut inserted = HashSet::new();
        for &txn_id in active_ids {
            let mut ptr = self.txns_read()?.undo_head(txn_id)?;
            while let Some(id) = ptr {
                charge.try_grow(128)?;
                let canonical = self.undo.read().canonical_ptr(id);
                if !reachable.insert((txn_id, canonical)) {
                    return Err(undo_corruption(id, "cycle in replicated rollback chain"));
                }
                let record = self.load_undo_owned(id)?.ok_or_else(|| {
                    error(
                        crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED,
                        format!(
                            "promotion refused: transaction {txn_id} is missing logical undo {id}"
                        ),
                    )
                })?;
                if record.txn_id != txn_id {
                    return Err(undo_corruption(
                        id,
                        "replicated rollback transaction mismatch",
                    ));
                }
                match &record.kind {
                    UndoKind::Insert => {
                        charge.try_grow(128)?;
                        inserted.insert((txn_id, record.table_id, record.row_id));
                    }
                    UndoKind::InsertBatch { row_ids } => {
                        charge.try_grow(row_ids.len().saturating_mul(128))?;
                        inserted.extend(row_ids.iter().map(|row| (txn_id, record.table_id, *row)));
                    }
                    _ => {}
                }
                ptr = record.prev_undo_ptr;
            }
        }
        // A source FPI can carry a newer in-flight row than the logical undo
        // stream has delivered. Physical gap relocation alone cannot make
        // that row safe to compensate: prove each current version and each
        // same-transaction before-header is covered by its rollback chain.
        let ids = self
            .catalog_read()?
            .tables
            .values()
            .flat_map(|table| table.storage_ids())
            .collect::<HashSet<_>>();
        for storage_id in ids {
            self.with_heap_read(storage_id, |heap| heap.scan_mvcc_columns_until(&[], |row| {
                let txn_id = row.header.last_txn_id;
                if !active.contains(&txn_id) { return Ok(false); }
                let mut header = row.header;
                let mut hops = 0;
                loop {
                    let Some(id) = header.undo_ptr else {
                        // INSERT rows have no historical predecessor. Their
                        // exact RowId must still be owned by a received undo.
                        if inserted.contains(&(txn_id,storage_id,row.row_id)) { break; }
                        return Err(error(crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED,
                            format!("promotion refused: storage {storage_id} has an uncovered row from transaction {txn_id}")));
                    };
                    let canonical = self.undo.read().canonical_ptr(id);
                    if !reachable.contains(&(txn_id,canonical)) {
                        return Err(error(crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED,
                            format!("promotion refused: storage {storage_id} transaction {txn_id} row references unreceived logical undo {id}")));
                    }
                    hops += 1;
                    if hops > reachable.len() {
                        return Err(undo_corruption(id, "cycle in replicated row before-header chain"));
                    }
                    let record = self.load_undo_owned(id)?.ok_or_else(|| undo_corruption(id,"replicated row before-image is missing"))?;
                    let Some(before) = record.before_header.filter(|before| before.last_txn_id == txn_id) else { break; };
                    header = before;
                }
                Ok(false)
            }))?;
        }
        Ok(())
    }

    pub fn commit_txn(&self, txn_id: u64) -> Result<u64, QueryError> {
        let mut deferred = self.commit_txn_deferred(txn_id)?;
        let commit_scn = deferred.commit_scn;
        self.finish_deferred_commits(&mut [&mut deferred])?;
        Ok(commit_scn)
    }

    /// Publish one transaction in commit order without waiting for its WAL
    /// durability barrier. Callers must promptly pass the returned token to
    /// `finish_deferred_commits`; dropping it would leave post-durable hooks
    /// unfinished.
    pub(crate) fn commit_txn_deferred(&self, txn_id: u64) -> Result<DeferredCommit, QueryError> {
        // Handler_commit: every commit — read-only, autocommit, explicit, or
        // fast-path — funnels through here, matching MySQL's internal commit
        // counter (which also counts read-only commits).
        crate::engine::status_counters::record_handler_commit();
        // M1.1 - Read-only commit: no WAL, no lock release queue, just slot cleanup.
        self.ensure_operational()?;
        let (read_only, wal_logged, prepared) =
            if let Some(prepared) = self.cached_persistent_dml_prepared(txn_id) {
                (false, true, prepared)
            } else {
                self.txns_read()?
                    .slot(txn_id)
                    .map(|slot| {
                        (
                            slot.read_only,
                            slot.wal_logged,
                            slot.status == TxnStatus::Prepared,
                        )
                    })
                    .unwrap_or((false, true, false))
            };
        if !prepared && (read_only || !wal_logged) {
            let mut txns = self.txns_write()?;
            txns.publish_commit_read_only(txn_id)?;
            self.slot_status_cache.mark_committed(txn_id, 0);
            self.active_write_txns_atomic
                .store(txns.active_write_count, Ordering::Release);
            drop(txns);
            // Lock release moved onto the separate `lock_manager` latch (order
            // txns → lock_manager). Read-only txns hold no locks; this just
            // clears any stray waiter bookkeeping defensively.
            let lock_release = self.lock_manager().unlock_txn(txn_id);
            let mut released = self.wake_released_lock_waiters(lock_release);
            if row_lock_sharding_enabled() {
                released.extend(self.row_locks_sharded.release_txn(txn_id));
            }
            self.notify_lock_waiters_for_rows(released);
            self.publish_pending_live_row_deltas(txn_id);
            self.maybe_gc_after_commit();
            end_txn_redo_policy(&self.log, txn_id);
            self.table_locks_sharded.release_txn(txn_id);
            return Ok(DeferredCommit {
                logical_apply_lease: None,
                _backup_feedback: None,
                txn_id,
                commit_scn: 0,
                commit_unix_us: 0,
                temporal_enabled: false,
                history_index_batch: Vec::new(),
                temporal_persist: None,
                commit_lsn: None,
                durability_lsn: None,
                index_error: None,
                commit_group_target: 1,
                binlog_commit: None,
                binlog_semi_sync_target: None,
                finished: false,
            });
        }
        let backup_feedback = self.capture.commit_sample();
        let has_vector_pending = self.prepare_pending_vector_commit(txn_id)?;
        // Reduced redo omits row images only for pages that were newly
        // allocated by this transaction. Force those data pages (plus their
        // file lengths) before the CommitTxn can become visible/durable. WAL
        // protected in-place/index pages may remain dirty and recover via
        // their ordinary records; flushing them here is conservative but
        // keeps one simple data-before-commit fence for every storage kind.
        if txn_reduced_redo_active(&self.log, txn_id)
            && (self.capture.redo_required() || crate::repl::physical_replication_active())
        {
            return Err(error(
                crate::error::catalog::ER_INNODB_REDO_DISABLED,
                "cannot commit a reduced-redo transaction after physical replication is enabled",
            ));
        }
        if txn_reduced_redo_active(&self.log, txn_id)
            && self.config.durability != DurabilityMode::Memory
        {
            let started = Instant::now();
            let files = reduced_redo_files(&self.log, txn_id);
            let data_dir = &self.data_dir;
            std::thread::scope(|scope| -> Result<(), QueryError> {
                let handles = files
                    .into_iter()
                    .map(|relative| {
                        scope.spawn(move || {
                            File::open(data_dir.join(relative))
                                .and_then(|file| file.sync_data())
                                .map_err(io_err)
                        })
                    })
                    .collect::<Vec<_>>();
                for handle in handles {
                    handle.join().map_err(|_| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            "reduced-redo data sync worker panicked",
                        )
                    })??;
                }
                Ok(())
            })?;
            self.undo.write().flush()?;
            REDUCED_REDO_FORCE_FLUSHES.fetch_add(1, Ordering::Relaxed);
            REDUCED_REDO_FORCE_FLUSH_US.fetch_add(elapsed_us(started), Ordering::Relaxed);
        }
        // Freeze the batching target while this transaction is still present
        // in the active-writer count. Once commit publication starts, several
        // peers may all leave that count before any one reaches fsync; deriving
        // the target at the durability barrier would then collapse a real
        // two-writer workload back to target=1.
        let commit_group_target = self.current_group_commit_target();
        let temporal_enabled = self.undo_retention_ms() > 0;
        let history_index_batch = if temporal_enabled {
            let batch = super::temporal::TemporalHistory::encode_history_index_batch(
                self.txns_read()?.history_index_batch(txn_id),
            )?;
            self.temporal_history
                .reserve_commit_capacity(txn_id, batch.len())?;
            batch
        } else {
            Vec::new()
        };
        // M1.4 - Atomic SCN allocation for commit_scn.
        let binlog_commit_gate = crate::binlog::enabled().then(crate::binlog::commit_gate);
        let commit_scn = self.next_scn_atomic.fetch_add(1, Ordering::AcqRel);
        // Test-only: open the allocate-before-publish visibility window so the
        // MVCC commit-visibility regression test can race a reader here. No-op
        // (compiled out) in release builds.
        #[cfg(test)]
        commit_visibility_test_hook::fire(commit_scn);
        // P0-1: from here until publish, `commit_scn` is allocated but not yet
        // on the frontier. If any `?` below bails before publish, this guard's
        // Drop marks it done so the contiguous frontier can still advance.
        let mut frontier_guard = ScnFrontierGuard::new(self, commit_scn);
        let binlog_commit = crate::binlog::stage_txn_commit(txn_id, commit_scn)?;
        // Capture the binlog end position for the optional MySQL semi-sync wait
        // below, before `publish_txn_commit` consumes the commit.
        let binlog_semi_sync_target = binlog_commit
            .as_ref()
            .map(|commit| (commit.wal_ref().file.clone(), commit.wal_ref().end_pos));
        let commit_payload = WalPayload::CommitTxn {
            commit_scn,
            commit_unix_us: 0,
            binlog: binlog_commit
                .as_ref()
                .map(|commit| commit.wal_ref().clone()),
        };
        // Zone Map pages append redo-only FPI records synchronously during
        // DML. The CommitTxn/TxnBatch LSN is allocated below, after those
        // appends, so the normal commit durability barrier covers their WAL
        // prefix without a second transaction-local summary envelope.
        if txn_wal_buffering_active(&self.log, txn_id) {
            if temporal_enabled {
                if let Err((payload, _)) = push_txn_wal_record(
                    &self.log,
                    txn_id,
                    WalPayload::HistoryIndexBatch {
                        bytes: history_index_batch.clone(),
                    },
                    Vec::new(),
                ) {
                    self.append_wal(txn_id, payload)?;
                }
            }
        } else {
            if temporal_enabled {
                self.append_wal(
                    txn_id,
                    WalPayload::HistoryIndexBatch {
                        bytes: history_index_batch.clone(),
                    },
                )?;
            }
        }
        // COMMIT_GATE establishes binlog/SCN append order only. Never hold it
        // across either durable syscall: staged peers must be able to join the
        // binlog fsync batch and then the WAL group-commit batch.
        drop(binlog_commit_gate);
        crate::binlog::sync_staged_txn_commit(binlog_commit.as_ref())?;

        // Preserve binlog order at the WAL boundary with a cheap process-local
        // turn. The turn covers only append work, not fsync. This keeps
        // recovery's WalBinlogRef sequence deterministic while allowing every
        // committer to queue at the shared durability worker concurrently.
        let mut commit_unix_us = 0;
        let (commit_lsn, temporal_persist) =
            crate::binlog::with_wal_commit_order(binlog_commit.as_ref(), || {
                let append = || {
                    // Txn-buffering: append every parked heap record AND the
                    // CommitTxn record in ONE WAL-mutex acquisition, then release
                    // deferred pins. The pre-encode runs outside the lock.
                    if txn_wal_buffering_active(&self.log, txn_id) {
                        let (lsn, time) = self
                            .flush_txn_wal_buffer(txn_id, Some(commit_payload))?
                            .unwrap_or((0, 0));
                        commit_unix_us = time;
                        end_txn_wal_buffering(&self.log, txn_id);
                        Ok(lsn)
                    } else {
                        let (lsn, time) = self.append_commit_wal(txn_id, commit_payload)?;
                        commit_unix_us = time;
                        Ok(lsn)
                    }
                };
                if temporal_enabled {
                    self.temporal_history
                        .track_commit_wal(|| append().map(|lsn| (lsn, ())))
                        .map(|(lsn, (), token)| (lsn, Some(token)))
                } else {
                    append().map(|lsn| (lsn, None))
                }
            })?;
        #[cfg(test)]
        if has_vector_pending { self.test_vector_crash_point(1, commit_lsn)?; }
        // Publish and release row locks before the group durability barrier.
        // Strict durability guarantees COMMIT success only after WAL fsync;
        // a concurrent reader can observe an in-progress commit before that
        // response. This is also InnoDB's commit-in-memory contract. A later
        // writing observer receives a higher WAL LSN and cannot become durable
        // before the transaction it observed. Keeping the fsync/group window
        // outside row locks preserves throughput on contended rows.
        let mut txns = self.txns_write()?;
        let new_frontier = txns.publish_commit(txn_id, commit_scn)?;
        self.slot_status_cache.mark_committed(txn_id, commit_scn);
        self.active_write_txns_atomic
            .store(txns.active_write_count, Ordering::Release);
        drop(txns);
        if temporal_enabled {
            self.note_temporal_commit(txn_id, commit_scn, commit_unix_us);
            // `history_index_batch` was encoded by this process immediately
            // above, so decode failure is an internal invariant violation.
            // Publication has already made the slot Committed: from this point
            // onward the commit path must be infallible and must still release
            // locks / reach its durability barrier. Fail-stop the database and
            // let recovery rebuild the sidecar from WAL instead of returning a
            // half-published error.
            if let Err(err) = self
                .temporal_history
                .note_history_index_batch(txn_id, &history_index_batch)
            {
                self.quarantine_mvcc(format!(
                    "committed transaction {txn_id} could not publish its temporal history batch: {}",
                    err.message()
                ));
            }
        }
        // The durable decision precedes redo-only vector maintenance. Preserve
        // errors until lock and frontier cleanup is complete, and include all
        // required maintenance WAL in the response durability barrier.
        let mut index_error = self.apply_pending_vector_deletes(txn_id, commit_scn).err();
        // Early lock release, now on the separate `lock_manager` latch: drop the
        // `txns` write lock first (order txns → lock_manager), then release this
        // txn's row/unique locks — still BEFORE the durability fsync below.
        // Keep its table lock until finish_deferred_commits closes durability
        // and visibility. Ordinary writer IX locks remain mutually compatible;
        // retaining the existing entry needs no extra commit-path latch.

        let lock_release = self.lock_manager().unlock_txn(txn_id);
        let mut released = self.wake_released_lock_waiters(lock_release);
        if row_lock_sharding_enabled() {
            released.extend(self.row_locks_sharded.release_txn(txn_id));
        }
        // P0-1: publish the new frontier to the lock-free mirror — AFTER the
        // slot is Committed (above), so no snapshot can observe the frontier
        // covering this commit while its slot is still Active. The commit is now
        // on the frontier; disarm the void guard.
        self.publish_visible_frontier(new_frontier);
        frontier_guard.disarm();
        // Superseded vector versions queued by this (or an earlier) commit
        // become retirable once the frontier covers them.
        self.drain_vector_purges();
        if has_vector_pending && index_error.is_none() {
            index_error = self.ensure_operational().err();
        }
        let durability_lsn = if has_vector_pending {
            commit_lsn.max(self.log.lock().next_lsn().saturating_sub(1))
        } else {
            commit_lsn
        };
        self.notify_lock_waiters_for_rows(released);
        Ok(DeferredCommit {
            logical_apply_lease: None,
            _backup_feedback: backup_feedback,
            txn_id,
            commit_scn,
            commit_unix_us,
            temporal_enabled,
            history_index_batch,
            temporal_persist,
            commit_lsn: Some(commit_lsn),
            durability_lsn: Some(durability_lsn),
            index_error,
            commit_group_target,
            binlog_commit,
            binlog_semi_sync_target,
            finished: false,
        })
    }

    /// Make a source-ordered set of previously prepared commits durable with
    /// one high-water barrier, then publish their post-durable effects in the
    /// same order. A durability barrier for the largest LSN covers every
    /// earlier commit because WAL append and fsync watermarks are monotonic.
    pub(crate) fn finish_deferred_commits(
        &self,
        commits: &mut [&mut DeferredCommit],
    ) -> Result<(), QueryError> {
        let highest = commits.iter().filter_map(|commit| commit.durability_lsn).max();
        if let Some(highest_lsn) = highest {
            let target = commits
                .iter()
                .map(|commit| commit.commit_group_target)
                .max()
                .unwrap_or(1);
            #[cfg(test)]
            let induced = crate::storage::txn::fail_durability_sync_if_induced();
            #[cfg(not(test))]
            let induced: Result<(), QueryError> = Ok(());
            let durable = induced.and_then(|()| {
                if self.config.durability == DurabilityMode::Strict
                    || commits.iter().any(|commit| commit.binlog_commit.is_some())
                {
                    self.sync_commit_lsn_with_target(highest_lsn, target)
                } else if self.config.durability == DurabilityMode::Relaxed {
                    self.flush_commit_lsn(highest_lsn)
                } else {
                    Ok(())
                }
            });
            if let Err(err) = durable {
                self.quarantine_mvcc(format!(
                    "commit WAL durability barrier at LSN {highest_lsn} failed after MVCC publication: {}",
                    err.message()
                ));
                for commit in commits.iter_mut() {
                    commit.finished = true;
                    self.table_locks_sharded.release_txn(commit.txn_id);
                }
                self.notify_non_row_lock_waiters();
                return Err(err);
            }
        }

        for commit in commits.iter_mut() {
            let Some(_commit_lsn) = commit.commit_lsn else {
                commit.finished = true;
                continue;
            };
            // Dump consumers must not observe a transaction until its WAL
            // commit is durable. Tokens are finished in source commit order.
            if let Err(err) = crate::binlog::publish_txn_commit(commit.binlog_commit.take()) {
                self.quarantine_mvcc(format!(
                    "binlog publication failed after WAL commit: {}",
                    err.message()
                ));
                for commit in commits.iter_mut() {
                    commit.finished = true;
                    self.table_locks_sharded.release_txn(commit.txn_id);
                }
                self.notify_non_row_lock_waiters();
                return Err(err);
            }

            if commit.temporal_enabled {
                let ledger = self.persist_temporal_commit(
                    commit.txn_id,
                    commit.commit_scn,
                    commit.commit_unix_us,
                );
                let history = self
                    .temporal_history
                    .persist_history_index_batch(commit.txn_id, &commit.history_index_batch);
                if let Err(err) = ledger.and(history) {
                    // The primary WAL commit is already durable and externally
                    // visible. Returning an error here would report a rollback
                    // outcome for a transaction that recovery must commit, and
                    // the early return used to skip every cleanup below. Keep
                    // the successful commit outcome, finish all bookkeeping,
                    // and fail-stop subsequent work until restart reconstructs
                    // the temporal sidecars from retained WAL.
                    self.quarantine_mvcc(format!(
                        "durable transaction {} could not persist temporal metadata: {}",
                        commit.txn_id,
                        err.message()
                    ));
                }
            }
            if let Some(token) = commit.temporal_persist.take() {
                token.complete();
            }
            self.publish_pending_live_row_deltas(commit.txn_id);
            self.maybe_gc_after_commit();
            end_txn_redo_policy(&self.log, commit.txn_id);
            commit.finished = true;
        }
        // WAL durability alone does not make an out-of-order commit visible
        // to a new RC statement. Do not acknowledge until the contiguous
        // snapshot frontier covers every write in this batch. Publication
        // and lock release stay nonblocking so earlier committers can finish.
        if let Some(scn) = commits.iter().map(|commit| commit.commit_scn).max() {
            self.wait_for_commit_visibility(scn);
        }
        for commit in commits.iter() {
            self.table_locks_sharded.release_txn(commit.txn_id);
        }
        self.notify_non_row_lock_waiters();
        for commit in commits.iter() {
            if let Some(commit_lsn) = commit.commit_lsn {
                crate::repl::after_local_commit_durable(commit_lsn);
            }
            if let Some((ref file, end_pos)) = commit.binlog_semi_sync_target {
                crate::binlog::semi_sync_wait_after_commit(file, end_pos);
            }
        }
        if let Some((txn_id, err)) = commits.iter_mut().find_map(|commit| {
            commit.index_error.take().map(|err| (commit.txn_id, err))
        }) {
            return Err(err.map_message(|message| format!(
                "transaction {txn_id} has a committed WAL decision but index maintenance failed; database is fail-stopped: {message}"
            )));
        }
        Ok(())
    }

    fn next_commit_unix_us(&self) -> u64 {
        let real = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;
        let mut observed = self.commit_hlc_unix_us.load(Ordering::Acquire);
        loop {
            let next = real.max(observed.saturating_add(1));
            match self.commit_hlc_unix_us.compare_exchange_weak(
                observed,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if real <= observed {
                        self.temporal_hlc_rollback_total
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return next;
                }
                Err(actual) => observed = actual,
            }
        }
    }

    /// The caller holds wal_log() while selecting the immutable prefix.
    pub(crate) fn backup_time_fence(&self) -> u64 {
        crate::wal::advance_ordered_time(&self.commit_hlc_unix_us)
    }

    fn note_temporal_commit(&self, txn_id: u64, commit_scn: u64, commit_unix_us: u64) {
        self.temporal_history
            .note_commit(super::temporal::CommitLedgerEntry {
                txn_id,
                commit_scn,
                commit_unix_us,
            });
    }

    fn persist_temporal_commit(
        &self,
        txn_id: u64,
        commit_scn: u64,
        commit_unix_us: u64,
    ) -> Result<(), QueryError> {
        self.temporal_history
            .persist_commit(super::temporal::CommitLedgerEntry {
                txn_id,
                commit_scn,
                commit_unix_us,
            })
    }

    fn maybe_gc_after_commit(&self) {
        let n = self.commit_gc_counter.fetch_add(1, Ordering::Relaxed);
        // The engine's maintenance worker runs GC off the commit path. Keep a
        // coarse fallback for direct Database users/tests so slots remain
        // bounded even when no StorageQueryEngine owns the database.
        if n % 4096 == 0 {
            self.advance_min_retained_txn_id();
        }
        if n % 1024 == 0 {
            self.purge_obsolete_index_repairs();
        }
    }

    /// Background transaction-slot maintenance entry. Delayed collection is
    /// conservative for MVCC correctness; the rotating bounded scan keeps
    /// autocommit writers from paying for the retained slot population.
    pub(crate) fn gc_committed_txn_slots(&self) {
        self.advance_min_retained_txn_id();
        self.purge_obsolete_index_repairs();
    }

    /// M3.4 - Advance the minimum retained txn_id watermark, allowing the
    /// transaction manager to drop slots whose visibility outcome can be
    /// inferred from the watermark alone.
    fn advance_min_retained_txn_id(&self) {
        // Scan only a bounded rotating window to decide whether there is any
        // GC-eligible slot. Skipping the write lock and the all-shards
        // `oldest_active_snapshot_scn` scan when nothing is collectable keeps
        // maintenance latency stable under long retained histories.
        const SLOT_GC_SCAN_LIMIT: usize = 8192;
        // Scan a rotating bounded window. This keeps maintenance latency
        // independent of total retained history while every slot is still
        // revisited across successive passes.
        let (oldest_active, terminal_candidates, rolled_back_candidates, next_cursor) = {
            let txns = self.txns.read();
            let mut terminal = Vec::new();
            let mut rolled_back = Vec::new();
            let mut last_seen = None;
            let mut scanned = 0usize;
            let start = txns.slot_gc_cursor;
            for (id, slot) in txns.slots.range(start..) {
                if scanned >= SLOT_GC_SCAN_LIMIT {
                    break;
                }
                scanned += 1;
                last_seen = Some(*id);
                if !slot.status.is_in_flight() {
                    terminal.push(*id);
                    if slot.status == TxnStatus::RolledBack {
                        rolled_back.push(*id);
                    }
                }
            }
            if scanned < SLOT_GC_SCAN_LIMIT && start != 0 {
                for (id, slot) in txns.slots.range(..start) {
                    if scanned >= SLOT_GC_SCAN_LIMIT {
                        break;
                    }
                    scanned += 1;
                    last_seen = Some(*id);
                    if !slot.status.is_in_flight() {
                        terminal.push(*id);
                        if slot.status == TxnStatus::RolledBack {
                            rolled_back.push(*id);
                        }
                    }
                }
            }
            let next = last_seen.map_or(0, |id| id.saturating_add(1));
            (
                txns.oldest_write_capable_txn_id,
                terminal,
                rolled_back,
                next,
            )
        };
        if terminal_candidates.is_empty() {
            self.txns.write().slot_gc_cursor = next_cursor;
            return;
        }
        // Even with no registered reader, the published commit frontier is
        // an implicit lower bound for the next snapshot. Commits can publish
        // out of SCN order: a later slot may already be `Committed` while
        // `visible_scn` is still held behind an earlier in-flight commit. Keep
        // that slot so a snapshot taken right now can still classify the row
        // version as newer and invisible. Once the gap closes, `visible_scn`
        // advances and the next GC pass may collect it.
        let (oldest_registered_snapshot_scn, visible_scn) = self.snapshot_visibility_horizon();
        let oldest_snapshot_scn = Some(
            oldest_registered_snapshot_scn
                // Slot GC uses a strict `commit_scn < horizon` test below.
                // With no reader, `visible_scn + 1` therefore collects every
                // fully published commit (including equality) while retaining
                // any out-of-order commit still above the frontier.
                .unwrap_or_else(|| visible_scn.saturating_add(1)),
        );
        // BUG-1 (docs/lob-management-audit-2026-07.md) — a rolled-back slot
        // must survive GC until its undo records drain, so `apply_purge` can
        // still read its `RolledBack` creator state and skip freeing the
        // before-image chains rollback handed back to the live row. Only the
        // rolled-back candidates gathered above need this check, so probe the
        // undo tablespace for exactly those (rare) ids instead of materializing
        // the whole retained-writer set. The old `txn_ids_with_undo()` built a
        // HashSet sized to every writer retained under undo retention (tens to
        // hundreds of thousands at OLTP rates) on every call, and held the undo
        // read lock the entire time — blocking every concurrent undo append
        // (the write-path hot lock) ~160×/s. Read the undo lock BEFORE the txns
        // write lock (never nested) so there is no lock-order inversion with the
        // purge path. `drained_rolled_back` holds the candidates confirmed to
        // have no un-purged undo; a slot that rolls back after pass 1 is simply
        // not in the set and is conservatively retained until the next cycle.
        let drained_rolled_back: std::collections::HashSet<u64> =
            if rolled_back_candidates.is_empty() {
                std::collections::HashSet::new()
            } else {
                let undo = self.undo.read();
                rolled_back_candidates
                    .iter()
                    .copied()
                    .filter(|id| !undo.has_undo_for_txn(*id))
                    .collect()
            };
        {
            let mut txns = self.txns.write();
            txns.slot_gc_cursor = next_cursor;
            let watermark = oldest_active.unwrap_or(txns.next_txn_id);
            // Drop slots that are committed/rolled back AND whose txn_id is below
            // any active txn AND whose commit_scn is below the oldest snapshot.
            let to_remove: Vec<u64> = terminal_candidates
                .iter()
                .copied()
                .filter(|id| *id < watermark)
                .filter(|id| {
                    let Some(slot) = txns.slots.get(id) else {
                        return false;
                    };
                    match slot.status {
                        // Neither ACTIVE nor PREPARED is GC-eligible — the
                        // PREPARED branch still owns its undo chain and
                        // locks until XA COMMIT / XA ROLLBACK.
                        TxnStatus::Active | TxnStatus::Prepared => false,
                        TxnStatus::Committed => slot
                            .commit_scn
                            .map(|scn| {
                                oldest_snapshot_scn
                                    .map(|min_scn| scn < min_scn)
                                    .unwrap_or(true)
                            })
                            .unwrap_or(false),
                        // BUG-1 — retain a rolled-back slot until its undo
                        // records are all purged; only then is dropping it (and
                        // letting its creator state degrade to `None`) safe. Only
                        // slots we explicitly confirmed drained above are eligible;
                        // any that rolled back after pass 1 wait for the next cycle.
                        TxnStatus::RolledBack => drained_rolled_back.contains(&slot.txn_id),
                    }
                })
                .collect();
            for id in &to_remove {
                txns.slots.remove(id);
            }
            // `min_retained_txn_id` is a proof about every transaction id in
            // a prefix, not merely the largest id removed in this pass.
            // Commit-SCN eligibility is intentionally allowed to be sparse
            // (a higher txn id may commit before a lower one), so advance only
            // across registered ids for which no retained slot remains. A
            // missing id above registered_txn_frontier may still be between
            // atomic allocation and slot registration, and is not safe.
            let prior = self.min_retained_txn_id.load(Ordering::Relaxed);
            let mut new_min = prior;
            let last_allocated = txns.registered_txn_frontier;
            while new_min < last_allocated {
                let next = new_min.saturating_add(1);
                if let Some(slot) = txns.slots.get(&next) {
                    if slot.status.is_in_flight() && slot.read_only {
                        // A declared read-only transaction can never own a
                        // heap header, so retaining its session snapshot does
                        // not invalidate this txn-id prefix proof.
                        new_min = next;
                        continue;
                    }
                    break;
                }
                new_min = next;
            }
            // Drop the txns write-lock before touching the slot-status
            // cache so visibility readers that hit the cache during GC
            // don't contend with the lock release.
            drop(txns);
            for id in &to_remove {
                self.slot_status_cache.remove(*id);
            }
            if new_min > prior {
                self.min_retained_txn_id.store(new_min, Ordering::Release);
            }
        }
    }

    fn sync_commit_lsn(&self, lsn: u64) -> Result<(), QueryError> {
        let target = self.current_group_commit_target();
        self.sync_commit_lsn_with_target(lsn, target)
    }

    fn current_group_commit_target(&self) -> u64 {
        let active_txns = self.concurrent_active_txn_count();
        // Cap target_waiters at a small runtime-tunable value: with N active writers the leader
        // previously waited for all N before fsync. With N=20 (sysbench
        // oltp_read_write) only 1-2 writers are in the commit phase at
        // once (the rest are in SELECTs / UPDATEs), so target=20 always
        // timed out the deadline and the batch never grew past 1.92. After
        // WAL append coalescing, fsync is cheap relative to the wait itself;
        // profiling `oltp_update_index` originally showed the realized batch
        // converging around 4; after zero-window yield coalescing, a target of
        // 8 is worth testing because it can reduce strict fsync count without
        // adding timer sleep. `MYDB_GROUP_COMMIT_TARGET_MAX` is the oracle.
        //
        // Lower bound is 1 (not 2): with a single active writer (e.g.
        // sysbench single-thread oltp_read_write) there is never a
        // second waiter to batch with, so a target of 2 forced every
        // commit to wait out the full `group_commit_window` (default
        // 500us) on a condvar before fsyncing — that wait dominated
        // the commit-path CPU profile (~49% of on-CPU). Letting the
        // leader fsync immediately when it is alone preserves multi-
        // writer batching (target still grows with active_txns) while
        // removing the single-writer dead time.
        let target_max = crate::storage::txn::group_commit_target_max();
        crate::storage::txn::group_commit_target(active_txns, target_max)
    }

    fn sync_commit_lsn_with_target(&self, lsn: u64, target: u64) -> Result<(), QueryError> {
        let (window, adaptive) = self.group_commit_window_and_adaptive();
        if crate::storage::txn::durability_worker_enabled() {
            return self.durability_worker.sync_lsn(lsn, window, target);
        }
        self.group_commit
            .sync_lsn(lsn, window, target, adaptive, &self.log)
    }

    /// Current group-commit leader wait window and adaptive flag. Read from
    /// the runtime config (settable via `SET GLOBAL mydb_group_commit_us` /
    /// `mydb_group_commit_adaptive`) so operators can tune batching without a
    /// restart. One RwLock read per *batch* (not per row), negligible next to
    /// the fsync it gates.
    fn group_commit_window_and_adaptive(&self) -> (std::time::Duration, bool) {
        let cfg = self.runtime_config_read();
        (
            std::time::Duration::from_micros(cfg.group_commit_us),
            cfg.group_commit_adaptive,
        )
    }

    /// Relaxed-durability commit barrier. Like `sync_commit_lsn`, but the
    /// per-batch `sync_op` is a WAL write-buffer flush (drain the pending
    /// bytes to the OS page cache, NO fsync) instead of an fsync — that is
    /// exactly relaxed's contract: survive a process crash, not a power loss.
    ///
    /// The WAL write buffer is enabled for relaxed (see `facade.rs`), so
    /// appends only `memcpy` frames into the 256 KiB buffer under the log
    /// mutex — cheap. A separate coordinator uses the same batching algorithm
    /// as strict without sharing its fsync watermark. N committers do one coalesced
    /// `write()` instead of one-per-frame. That is what makes relaxed faster
    /// than strict again: identical batching, minus the fsync.
    ///
    /// History: an earlier version kept the write buffer OFF for relaxed and
    /// wrote every frame with its own `pwrite()` under the log mutex, then
    /// made this barrier a no-op `mark_durable`. The per-frame syscalls (3-5
    /// per txn, all serialized on the global log mutex) dominated and inverted
    /// the ordering — relaxed measured ~37k tps against strict's ~56k on
    /// sysbench oltp_update_index (20 threads, 1M rows). Coalescing via the
    /// write buffer + this batched flush removes that floor.
    fn flush_commit_lsn(&self, lsn: u64) -> Result<(), QueryError> {
        let target = self.current_group_commit_target();
        let (window, adaptive) = self.group_commit_window_and_adaptive();
        self.flush_group_commit
            .sync_lsn_with(lsn, window, target, adaptive, |sync_lsn| {
                // `prepare_sync_to` drains `pending_write_bytes` to the fd
                // (a coalesced write into the OS page cache) and returns the
                // flushed high-water LSN. We deliberately do NOT call
                // `finish_sync_to`: that would advance the WAL's *fsync*-level
                // `durable_lsn`, causing a later checkpoint / semi-sync
                // `prepare_sync_to` to short-circuit and skip the real fsync.
                // Leaving it untouched keeps the fsync watermark honest while
                // the coordinator advances its own release watermark to
                // `high_water` (below), unblocking this batch's waiters.
                let mut guard = self.log.lock();
                match guard.prepare_sync_to(sync_lsn)? {
                    Some((_files, high_water)) => {
                        Ok((crate::storage::txn::SyncOutcome::Synced, high_water))
                    }
                    None => Ok((crate::storage::txn::SyncOutcome::NoOp, guard.durable_lsn())),
                }
            })
    }

    /// Append all parked transaction-local WAL records (and `final_record`, if
    /// given) in a SINGLE WAL-mutex acquisition, then publish each dirtied
    /// frame's real LSN and release its deferred pin. Returns the LSN assigned
    /// to `final_record`. BeginTxn is appended eagerly when the transaction
    /// starts, so WAL order is begin → parked records → final_record even when
    /// different statements run on different blocking-pool workers.
    fn flush_txn_wal_buffer(
        &self,
        txn_id: u64,
        final_record: Option<WalPayload>,
    ) -> Result<Option<(u64, u64)>, QueryError> {
        let TxnWalBuffer {
            entries,
            boundaries,
        } = take_txn_wal_entries(&self.log, txn_id);
        if entries.is_empty() && boundaries.is_empty() && final_record.is_none() {
            return Ok(None);
        }
        // An append failure invalidates the in-memory dirty images whose
        // redo was still parked. Never turn their deferred pins into ordinary
        // flushable pages with LSN 0. Fail-stop the database, detach those
        // frames without writeback, and let restart WAL recovery rebuild a
        // coherent pool from the durable prefix.
        let pinned_keys: Vec<PageKey> = entries
            .iter()
            .flat_map(|(_, keys)| keys.iter().cloned())
            .collect();
        let result = (|| -> Result<Option<(u64, u64)>, QueryError> {
            #[cfg(test)]
            if FAIL_TXN_WAL_BUFFER_FLUSH.with(|fail| fail.replace(false)) {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "injected transaction WAL flush failure",
                ));
            }
            let mut finalize: Vec<(PageKey, u64)> = Vec::with_capacity(entries.len());
            // Ordinary OLTP tails are encoded before acquiring the globally
            // contended WAL mutex. Oversized bulk transactions are different:
            // retaining every multi-megabyte encoded prefix frame until one final
            // lock acquisition multiplies peak memory and page faults. Stream
            // those prefix frames through the transaction-owned WAL scratch one
            // at a time; the bounded final tail keeps the common one-lock path.
            let final_lsn = match final_record {
                Some(commit) => {
                    // A TxnBatch is a commit-ending physical frame capped at 257
                    // logical records. Drain any oversized prefix as ordinary
                    // frames, then keep the largest legal suffix plus CommitTxn
                    // atomic in the final batch. A multi-row write can add several
                    // heap/index records between threshold probes, so the nominal
                    // 256-entry buffer can legitimately exceed the batch cap at
                    // commit.
                    let max_tail_entries = crate::wal::MAX_TXN_BATCH_RECORDS.saturating_sub(1);
                    let prefix_len = entries.len().saturating_sub(max_tail_entries);
                    let mut tail_records =
                        Vec::with_capacity(entries.len().saturating_sub(prefix_len) + 1);
                    let mut tail_keys = Vec::new();
                    for (idx, (payload, keys)) in entries.into_iter().enumerate() {
                        if idx < prefix_len {
                            let pre = crate::wal::LogManager::pre_encode_record(txn_id, payload)?;
                            let lsn =
                                crate::wal::LogManager::append_pre_encoded_to(&self.log, pre)?;
                            for key in keys {
                                finalize.push((key, lsn));
                            }
                        } else {
                            tail_records.push(payload);
                            tail_keys.extend(keys);
                        }
                    }
                    tail_records.push(commit);
                    let tail = crate::wal::LogManager::pre_encode_timed_record(
                        txn_id,
                        WalPayload::TxnBatch {
                            records: tail_records,
                        },
                    )?;
                    let (final_lsn, time) = crate::wal::LogManager::append_timed_to(
                        &self.log,
                        tail,
                        &self.commit_hlc_unix_us,
                        Some(&self.temporal_hlc_rollback_total),
                    )?;
                    for key in tail_keys {
                        finalize.push((key, final_lsn));
                    }
                    Some((final_lsn, time))
                }
                None => {
                    let mut encoded = Vec::with_capacity(entries.len());
                    for (payload, keys) in entries {
                        encoded.push((
                            crate::wal::LogManager::pre_encode_record(txn_id, payload)?,
                            keys,
                        ));
                    }
                    let mut log = self.log.lock();
                    let prefix_lsn = log.last_txn_lsn(txn_id);
                    let mut boundaries = boundaries.into_iter().peekable();
                    // `encoded` was declared before `log`: its payloads and
                    // buffers are destroyed after the WAL guard on all exits.
                    for (position, (pre, keys)) in encoded.iter_mut().enumerate() {
                        while boundaries.peek().is_some_and(|(at, _)| *at == position) {
                            if let Some(cursor) = boundaries.next().unwrap().1.upgrade() {
                                cursor
                                    .prefix_lsn
                                    .store(prefix_lsn.max(cursor.eager_lsn), Ordering::Relaxed);
                                cursor
                                    .last_lsn
                                    .store(log.last_txn_lsn(txn_id), Ordering::Release);
                            }
                        }
                        let lsn = log.append_pre_encoded_ref(pre)?;
                        for key in keys.drain(..) {
                            finalize.push((key, lsn));
                        }
                    }
                    for (_, cursor) in boundaries {
                        if let Some(cursor) = cursor.upgrade() {
                            cursor
                                .prefix_lsn
                                .store(prefix_lsn.max(cursor.eager_lsn), Ordering::Relaxed);
                            cursor
                                .last_lsn
                                .store(log.last_txn_lsn(txn_id), Ordering::Release);
                        }
                    }
                    None
                }
            };
            // Publish real LSNs and release deferred pins OUTSIDE the WAL mutex.
            for (key, lsn) in finalize {
                self.buffer_pool.finalize_deferred(&key, lsn);
            }
            Ok(final_lsn)
        })();
        match result {
            Ok(lsn) => Ok(lsn),
            Err(err) => {
                self.quarantine_mvcc(format!(
                    "transaction {txn_id} WAL append failed: {}; restart recovery required",
                    err.message()
                ));
                end_txn_wal_buffering(&self.log, txn_id);
                for key in pinned_keys {
                    if let Err(discard_err) = self.buffer_pool.discard_page(&key) {
                        crate::util::logging::log_message(format_args!(
                            "WAL failure page discard failed: {}",
                            discard_err.message()
                        ));
                    }
                }
                Err(err)
            }
        }
    }

    /// Drain the txn-buffer mid-transaction once it crosses the pin-bounding
    /// threshold, so a large transaction never holds an unbounded number of
    /// frames deferred-pinned (and thus un-evictable) at once.
    fn maybe_flush_txn_wal_buffer(&self, txn_id: u64) -> Result<(), QueryError> {
        if txn_wal_buffer_len(&self.log, txn_id) >= TXN_WAL_BUFFER_FLUSH_THRESHOLD {
            self.flush_txn_wal_buffer(txn_id, None)?;
        }
        Ok(())
    }

    fn publish_wal_before_eager_heap_write(&self, txn_id: u64) -> Result<(), QueryError> {
        if self.config.durability != DurabilityMode::Memory
            && txn_wal_has_deferred_pages(&self.log, txn_id)
        {
            self.flush_txn_wal_buffer(txn_id, None)?;
        }
        Ok(())
    }

    fn concurrent_active_txn_count(&self) -> usize {
        self.active_write_txns_atomic.load(Ordering::Acquire).max(1)
    }

    fn sync_checkpoint_undo(&self) -> Result<(), QueryError> {
        let shards = self.undo.read().checkpoint_write_shards();
        for shard in shards.iter() {
            crate::storage::undo::UndoWriteShard::checkpoint_sync(shard)?;
        }
        self.undo.write().checkpoint_sync_manifest()
    }

    pub fn flush_dirty_data(&self) -> Result<(), QueryError> {
        self.buffer_pool.flush_all()?;
        let durable_lsn = {
            let mut log = self.log.lock();
            log.flush()?;
            log.durable_lsn()
        };
        self.group_commit.mark_durable(durable_lsn);
        Ok(())
    }

    pub fn flush_data_cache(&self) -> Result<(), QueryError> {
        self.buffer_pool.flush_and_evict_all()
    }

    /// Durability barrier for the rollback compensation path.
    ///
    /// Earlier this took every touched index's handle RwLock in **write**
    /// mode and synchronously flushed all of that index's dirty pages
    /// (`with_index_exclusive(flush())`). That serialised every concurrent
    /// `with_index_dml` (read) navigator on the hottest indexes: under
    /// contended write workloads with a steady rollback rate (TPC-C issues
    /// ~1% NewOrder rollbacks) a single rolling-back transaction would stall
    /// all 20 terminals' inserts/lookups on `order_line` / `stock` for the
    /// duration of the flush. Profiling showed it as the dominant
    /// `lock_shared_slow` park on the index handle, and removing it lifts
    /// 4w/20t strict tpmC by ~50%.
    ///
    /// The per-index flush is redundant for crash durability: the rollback
    /// compensation writes (index deletes/reinserts via
    /// `apply_index_batches_deferred` / `insert_batch_deferred`, and the heap
    /// restores) are themselves WAL-logged, and the `EndTxn` record the
    /// caller appends + `sync_commit_lsn` right after this returns persists
    /// them in WAL order. On crash, recovery either redoes those logged
    /// records or re-runs the aborted transaction's undo chain — neither
    /// path depends on the dirty index pages having reached disk here. So we
    /// only advance the durable LSN (helps the group-commit coordinator) and
    /// leave page write-back to the background flusher / next checkpoint,
    /// exactly like the normal commit path.
    fn flush_touched_indexes(&self, table_ids: &HashSet<u64>) -> Result<(), QueryError> {
        if table_ids.is_empty() {
            return Ok(());
        }
        let durable_lsn = {
            let mut log = self.log.lock();
            log.flush()?;
            log.durable_lsn()
        };
        self.group_commit.mark_durable(durable_lsn);
        Ok(())
    }

    /// Run a full checkpoint: flush all dirty data, write the
    /// CheckpointBegin/End marker pair, and truncate the WAL down to the
    /// retention floor. Safe to run concurrently with writers — the
    /// floor keeps every record that an in-flight transaction's undo
    /// chain (or a physical-replication slot) still needs, and the
    /// full-collapse fast path only fires when the WAL tail is provably
    /// clean (verified under the log mutex inside the compaction).
    /// Whole-checkpoint exclusivity comes from `checkpoint_running`;
    /// this entry blocks until any in-progress checkpoint finishes.
    pub fn checkpoint(&self) -> Result<(), QueryError> {
        self.ensure_writable()?;
        let _running = self.checkpoint_running.lock().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "checkpoint-running lock poisoned",
            )
        })?;
        let result = self.checkpoint_locked(true, CHECKPOINT_TRIGGER_MANUAL);
        if result.is_ok() {
            self.last_auto_checkpoint_unix_ms_atomic
                .store(current_unix_ms(), Ordering::Release);
        } else {
            self.checkpoint_failures_total_atomic
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Final sharp checkpoint after every client/background writer has been
    /// drained. A single `flush_all` pass snapshots the dirty registry before
    /// it writes; pages carrying a deferred WAL pin (or a last racing
    /// redirty) remain dirty and force that checkpoint to retain their old
    /// redo floor. Under a long OLTP run this can leave tens of gigabytes of
    /// WAL behind an otherwise clean shutdown, and startup's recovery scan
    /// then exceeds `MYDB_MEMORY_TARGET` before it can reach CheckpointEnd.
    ///
    /// Normal/manual checkpoints remain one-pass and writer-safe. Shutdown is
    /// already quiesced, so retry a bounded number of sharp passes until the
    /// post-flush dirty snapshot is empty. Each successful pass monotonically
    /// removes the previous pass's residual set; failure to converge is an
    /// explicit shutdown error rather than claiming a clean restart point.
    pub(crate) fn checkpoint_for_shutdown(&self) -> Result<(), QueryError> {
        if self.is_snapshot_read_only() {
            return Ok(());
        }
        const MAX_SHUTDOWN_CHECKPOINT_PASSES: usize = 8;
        for _ in 0..MAX_SHUTDOWN_CHECKPOINT_PASSES {
            self.checkpoint()?;
            if self
                .last_checkpoint_dirty_pages_atomic
                .load(Ordering::Acquire)
                == 0
            {
                return Ok(());
            }
        }
        Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            format!(
                "shutdown checkpoint did not converge after {MAX_SHUTDOWN_CHECKPOINT_PASSES} passes; {} dirty buffer page(s) remain",
                self.last_checkpoint_dirty_pages_atomic.load(Ordering::Acquire)
            ),
        ))
    }

    /// Pre-create the next WAL segment off the commit critical path. Driven by
    /// the background checkpointer poll. Segment rotation would otherwise do a
    /// synchronous file create + `sync_all` while holding the WAL mutex,
    /// stalling every concurrent committer for the duration — on storage with
    /// occasional multi-second fsync latency (cloud disks, APFS copy-on-write)
    /// that surfaces as a global commit stall. With the next segment prepared
    /// ahead of time, rotation is only a cheap metadata rename.
    pub fn ensure_next_wal_segment_prepared(&self) -> Result<(), QueryError> {
        if self.config.durability == DurabilityMode::Memory {
            return Ok(());
        }
        crate::wal::LogManager::ensure_next_segment(&self.log)
    }

    /// Opportunistic trigger used by the background checkpointer: runs a
    /// checkpoint only when a trigger condition fired AND no other
    /// checkpoint is currently in progress.
    pub fn maybe_checkpoint(&self) -> Result<(), QueryError> {
        let Some(trigger) = self.should_checkpoint() else {
            return Ok(());
        };
        let Ok(_running) = self.checkpoint_running.try_lock() else {
            return Ok(());
        };
        let attempted_ms = current_unix_ms();
        if let Err(err) = self.buffer_pool.checkpoint_preflush() {
            self.checkpoint_preflush_failures_total_atomic
                .fetch_add(1, Ordering::Relaxed);
            crate::util::logging::log_message(format_args!(
                "background checkpoint preflush failed: {} ({})",
                err.message(),
                err.sql_state()
            ));
        }
        let result = self.checkpoint_locked(false, trigger);
        self.last_auto_checkpoint_unix_ms_atomic
            .store(attempted_ms, Ordering::Release);
        if result.is_err() {
            self.checkpoint_failures_total_atomic
                .fetch_add(1, Ordering::Relaxed);
            // Backoff: re-arm both triggers so a persistent failure
            // (e.g. disk full) retries on the normal cadence instead of
            // every poll tick. Only the trigger atomics move — the
            // observable CheckpointState keeps the last SUCCESSFUL
            // checkpoint.
            self.last_checkpoint_unix_ms_atomic
                .store(current_unix_ms(), Ordering::Release);
            self.wal_bytes_after_last_checkpoint_atomic
                .store(self.log_state_atomics.wal_bytes(), Ordering::Release);
            if let Err(err) = &result {
                crate::util::logging::log_message(format_args!(
                    "background checkpoint failed: trigger={} error={} ({})",
                    checkpoint_trigger_name(trigger),
                    err.message(),
                    err.sql_state()
                ));
            }
        }
        result
    }

    fn checkpoint_locked(&self, sharp: bool, trigger: u8) -> Result<(), QueryError> {
        let started = Instant::now();
        let wal_bytes_before = self.log_state_atomics.wal_bytes();
        let mut snapshot_us = 0_u64;
        let mut flush_us = 0_u64;
        let mut sync_us = 0_u64;
        let mut compact_us = 0_u64;
        if self.config.durability == DurabilityMode::Memory {
            // Even in memory mode, keep binlog metadata internally
            // consistent for an orderly restart. Storage durability remains
            // intentionally weaker according to the selected mode.
            crate::binlog::checkpoint_manifest()?;
            let now_ms = current_unix_ms();
            let mut checkpoint = self.checkpoint_lock()?;
            checkpoint.last_checkpoint_lsn = 0;
            checkpoint.last_checkpoint_unix_ms = Some(now_ms);
            drop(checkpoint);
            self.last_checkpoint_lsn_atomic.store(0, Ordering::Release);
            self.last_checkpoint_unix_ms_atomic
                .store(now_ms, Ordering::Release);
            self.group_commit.mark_durable(0);
            self.record_checkpoint_success(
                started,
                trigger,
                CHECKPOINT_KIND_MEMORY,
                0,
                0,
                wal_bytes_before,
                self.log_state_atomics.wal_bytes(),
                snapshot_us,
                flush_us,
                sync_us,
                compact_us,
            );
            return Ok(());
        }
        // Physical replica: the local WAL is a byte-identical mirror of
        // the master's stream, addressed by the MASTER's LSNs. Appending
        // local CheckpointBegin/End records here would allocate LSNs the
        // master is about to use for real records — duplicate LSNs in
        // the local WAL, a wrong recovery cutoff, and silent divergence.
        // Instead run a restartpoint: flush pages, then prune WAL below
        // the durable applied position without writing anything.
        if self.physical_replica_mode() {
            crate::binlog::checkpoint_manifest()?;
            return self.replica_restartpoint(started, trigger);
        }
        // Run maintenance that can append WAL BEFORE CheckpointBegin. Its
        // page effects are then flushed by this checkpoint and its records
        // sit below the recovery cutoff. In particular, undo purge can turn
        // tombstones into reusable heap slots via logged SlotReclaim writes;
        // running it after WAL compaction would leave a fresh recovery tail
        // behind an otherwise clean shutdown checkpoint.
        if self.open_mode != DatabaseOpenMode::SealBaseline {
            self.purge_sequence_retirements()?;
            self.purge_obsolete_undo();
        }
        // v46 catalog rows are ordinary MVCC versions.  Their tombstones are
        // reclaimed by the same slot/undo lifecycle as other transactional
        // heaps; checkpoint never serializes or garbage-collects logical
        // catalog generations.
        let (begin_lsn, auto_increment_dirty, sequence_dirty, vector_redo_floor, vector_publish_floor) = {
            let _sequence_fence = self.sequence_checkpoint_fence.write();
            let _auto_increment_fence = self.auto_increment_checkpoint_fence.write();
            let begin_lsn = self.temporal_history.checkpoint_barrier(|| {
                self.append_wal(txn_id_for_ddl(), WalPayload::CheckpointBegin)
            })?;
            let vector_redo_floor = self.vector_pending_wal.lock().values().copied().min();
            let vector_publish_floor = self.vector_pending_publication_floor();
            let dirty = self
                .auto_increment_dirty
                .lock()
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "AUTO_INCREMENT dirty queue poisoned",
                    )
                })?
                .rotate();
            let sequences = self.sequence_dirty.lock().snapshot()?;
            (begin_lsn, dirty, sequences, vector_redo_floor, vector_publish_floor)
        };
        if let Err(err) = self
            .persist_sequence_checkpoint(&sequence_dirty.entries)
            .and_then(|_| self.persist_auto_increment_checkpoint(&auto_increment_dirty.entries))
        {
            if let Ok(mut dirty) = self.auto_increment_dirty.lock() {
                for state in auto_increment_dirty.entries {
                    let _ = dirty.enqueue(state);
                }
            }
            return Err(err);
        }
        // Metadata pages have no transaction WAL floor. Drain only their
        // WAL-append -> resident-dirty publication windows, not whole ANALYZE
        // scans or queued DML. A checkpoint between tree updates may persist
        // an invalid root, which safely reopens as Unknown after a crash.
        // Keep this after catalog persistence, which can itself publish pages.
        // Begin may precede this drain; no recovery floor advances until End.
        let page_checkpoint = self.buffer_pool.page_wal_publish_gate.write();
        // Capture the in-flight writer floor immediately after Begin. A
        // writer can have appended WAL below `begin_lsn` while its buffer-pool
        // image is still being published. If it commits before the later
        // dirty-page scan, the page alone cannot prove that older WAL is
        // needed. Folding this floor into CheckpointEnd closes that window.
        // Writers that start after Begin can only append at/above begin_lsn.
        let (next_txn_id_snap, next_scn_snap, txn_floor) = {
            let txns = self.txns_read()?;
            (
                txns.next_txn_id,
                txns.next_scn,
                txns.min_in_flight_wal_lsn(),
            )
        };
        // Binlog transaction manifests are not rewritten on every commit:
        // doing so would add a metadata fsync to the hot path. Establish a
        // durable published-prefix snapshot after CheckpointBegin instead.
        // Every earlier binlogged commit is covered by the manifest; every
        // later commit carries a WalBinlogRef at/after begin_lsn and therefore
        // survives the WAL retention floor below. Without this barrier, a
        // checkpoint could compact all transaction references while leaving a
        // DDL-era manifest position, causing restart to truncate valid,
        // committed binlog bytes back to that stale position.
        crate::binlog::checkpoint_manifest()?;
        // Use the unlocked-fsync entry: the actual `fsync` system call
        // runs outside the WAL mutex so concurrent committers keep
        // appending while the checkpoint's begin record is being made
        // durable.
        // A fuzzy checkpoint does not make data pages durable here; its Begin
        // record only brackets the scalar dirty-page snapshot. If the process
        // crashes before CheckpointEnd is durable, recovery ignores the
        // incomplete checkpoint, so forcing Begin separately is unnecessary.
        // Let the End barrier persist Begin + redo floor atomically in WAL order and
        // avoid an extra global fsync stall on every automatic checkpoint.
        // Sharp/manual checkpoints keep the Begin barrier before flushing data
        // pages as the explicit operator durability boundary.
        if sharp {
            let phase_started = Instant::now();
            crate::wal::LogManager::sync_durable_to(&self.log, begin_lsn)?;
            sync_us = sync_us.saturating_add(elapsed_us(phase_started));
        }
        // Manual checkpoint / shutdown remains sharp so operators can force a
        // fully compact WAL and a clean startup point. Automatic checkpoints
        // are fuzzy: they snapshot the oldest dirty rec_lsn instead of forcing the
        // whole buffer pool through a multi-second write burst.
        if sharp {
            let phase_started = Instant::now();
            // Page batches enforce WAL-before-data. End supplies the final
            // WAL barrier; defer data-file sync until after the dirty snapshot.
            self.buffer_pool.flush_checkpoint_pages()?;
            flush_us = elapsed_us(phase_started);
        }
        // A sharp checkpoint is still allowed to run with live writers. Its
        // flush walks a bounded snapshot, so pages dirtied concurrently may
        // remain afterwards. Always take the scalar post-flush snapshot and
        // make the recovery floor cover them instead of assuming empty.
        let phase_started = Instant::now();
        let (oldest_dirty_lsn, dirty_page_count) = self.buffer_pool.checkpoint_dirty_snapshot()?;
        snapshot_us = elapsed_us(phase_started);
        // The gate only has to cover the snapshot: every publication admitted
        // after this point appends its WAL above `begin_lsn`, which already
        // bounds the redo floor. Holding it across the data-file sync, the End
        // fsync and WAL reclamation below stalled every zone-map publishing
        // DML for the whole (I/O-bound) remainder of the checkpoint.
        drop(page_checkpoint);
        // Sync after the snapshot: every page excluded as clean must be durable.
        // Pages written later were included in the snapshot's conservative floor.
        // This also covers relaxed-mode fuzzy checkpoints after background writes.
        let phase_started = Instant::now();
        self.buffer_pool.sync_cached_files()?;
        flush_us = flush_us.saturating_add(elapsed_us(phase_started));
        let mut redo_floor = begin_lsn
            .min(vector_redo_floor.unwrap_or(u64::MAX))
            .min(txn_floor.unwrap_or(u64::MAX))
            .min(oldest_dirty_lsn.unwrap_or(u64::MAX));
        let dirty_pages_empty = dirty_page_count == 0;
        // Zone-map pages participate in the same dirty snapshot and WAL
        // floor as heap/index pages; no private sidecar publication follows.
        // Drain the undo segment tail buffer before WAL truncation: a
        // buffered undo frame's only durable twin is its WAL UndoRecord,
        // and the compaction below may drop records with `lsn < floor`.
        // After this flush the segment file itself carries every frame.
        if self.undo_retention_ms() > 0 {
            self.sync_checkpoint_undo()?;
        } else {
            // No historical reads survive restart. Active/Prepared undo remains
            // protected by txn_floor in WAL; live snapshots keep the file copy.
            // Enabling history after this check establishes a new baseline:
            // its later writes are at/above Begin and cannot be reclaimed here.
            // Keep pending-sync tracking for a future enabled checkpoint.
            self.undo.write().flush()?;
        }
        // Complete every required vector publication before CheckpointEnd and
        // WAL reclamation. A failed index flush must fail this checkpoint.
        let vector_covered_lsn = if matches!(self.open_mode, DatabaseOpenMode::SealBaseline | DatabaseOpenMode::RestoreWorkspace) {
            self.recovery_context.source_end().ok_or_else(vector::persistence::corrupt)?
        } else { begin_lsn };
        let vector_covered_lsn = vector_covered_lsn.min(vector_publish_floor.unwrap_or(u64::MAX).saturating_sub(1));
        self.vector_graph_persist_all_dirty(vector_covered_lsn)?;
        redo_floor = redo_floor.min(self.vector_pending_wal.lock().values().copied().min().unwrap_or(u64::MAX));
        self.ensure_operational()?;
        // Re-read the watermarks immediately before appending so a
        // concurrent BeginTxn/CommitTxn that raced past the snapshot
        // above (`txns_read` released its lock before `append_wal`
        // takes the WAL mutex) cannot leave CheckpointEnd carrying a
        // smaller next_txn_id / next_scn than the values atomically
        // visible to writers. Recovery's `TransactionManager::from_wal`
        // already folds in per-record maxes for non-collapse paths,
        // but the collapse fast-path drops the post-CheckpointEnd
        // tail and relies on CheckpointEnd alone for these
        // watermarks — so the fence has to be airtight before append.
        let next_scn = next_scn_snap.max(self.next_scn_atomic.load(Ordering::Acquire));
        let next_txn_id = next_txn_id_snap.max({
            let txns = self.txns_read()?;
            txns.next_txn_id
        });
        // The source GC proof also excludes registered transactions whose
        // BEGIN has not reached WAL yet; a registration-only proof would
        // let the replica cross those still-unseen active writers.
        let gc_safe_txn_frontier = self.min_retained_txn_id.load(Ordering::Acquire);
        zone_map_failpoint(5).map_err(io_err)?;
        #[cfg(test)]
        SEQUENCE_CHECKPOINT_TEST_HOOK.with(|hook| {
            if let Some(callback) = hook.borrow_mut().take() {
                callback();
            }
        });
        let end_lsn = self.append_wal(
            txn_id_for_ddl(),
            WalPayload::CheckpointEnd {
                redo_lsn: redo_floor,
                next_txn_id,
                next_scn,
                auto_increment_covered_lsn: begin_lsn,
                sequence_covered_lsn: begin_lsn,
                gc_safe_txn_frontier,
            },
        )?;
        let phase_started = Instant::now();
        crate::wal::LogManager::sync_durable_to(&self.log, end_lsn)?;
        sync_us = sync_us.saturating_add(elapsed_us(phase_started));
        self.sequence_covered_lsn
            .store(begin_lsn, Ordering::Release);
        self.sequence_dirty.lock().entries.retain(|rt| {
            if rt.logged.lock().state_lsn >= begin_lsn && !rt.retired.load(Ordering::Acquire) {
                true
            } else {
                rt.queued_epoch.store(0, Ordering::Release);
                false
            }
        });
        self.sequence_dirty_generations
            .store(self.sequence_dirty.lock().entries.len(), Ordering::Release);
        zone_map_failpoint(6).map_err(io_err)?;
        // Replication slots may pin WAL history below the checkpoint.
        let slot_floor = {
            // Sealed + active: the byte-cap maths must see the active
            // segment's already-written bytes, or a lagging slot can
            // pin far more than `max_retained_bytes` before tripping.
            let sealed_segments = self.log.lock().segment_snapshot();
            crate::repl::wal_retention_floor(end_lsn, &sealed_segments, current_unix_ms())
        };
        let slot_floor = match (
            slot_floor,
            if matches!(
                self.open_mode,
                DatabaseOpenMode::SealBaseline | DatabaseOpenMode::RestoreWorkspace
            ) {
                None
            } else {
                self.capture.floor()
            },
        ) {
            (Some(replica), Some(backup)) => Some(replica.min(backup)),
            (Some(replica), None) => Some(replica),
            (None, Some(backup)) => Some(backup),
            (None, None) => None,
        };
        // Retention floor: WAL records with `lsn >= floor` must survive.
        //  - `begin_lsn` caps the floor whenever anything could have been
        //    appended during the checkpoint window (those records may
        //    have unflushed page effects);
        //  - `txn_floor` keeps in-flight write transactions' undo chains
        //    (incl. XA PREPARED branches) replayable after a crash;
        //  - `slot_floor` keeps history a physical replica still streams.
        // Only a fully quiesced checkpoint (empty begin→end window, no
        // in-flight writers, no lagging slots) requests the collapse; the
        // compaction re-verifies the tail is still clean under the log
        // mutex before collapsing, so a racing append falls back to
        // prune-only instead of being destroyed.
        let quiesced = txn_floor.is_none()
            && vector_redo_floor.is_none()
            && slot_floor.is_none()
            && dirty_pages_empty
            && end_lsn == begin_lsn.saturating_add(1);
        let floor = if quiesced {
            end_lsn
        } else {
            redo_floor.min(slot_floor.unwrap_or(u64::MAX))
        };
        let phase_started = Instant::now();
        // Preserve the HLC before its last CommitTxn can be reclaimed.
        crate::wal::persist_source_clock(
            &self.data_dir,
            self.commit_hlc_unix_us.load(Ordering::Acquire),
        )?;
        if let Err(err) = crate::wal::LogManager::reclaim_checkpoint(
            &self.log,
            end_lsn,
            next_txn_id,
            next_scn,
            floor,
            sharp,
            gc_safe_txn_frontier,
        ) {
            crate::util::logging::log_message(format_args!(
                "WARNING: checkpoint {} is durable, but WAL cleanup failed (will retry): {}",
                end_lsn,
                err.message()
            ));
        }
        compact_us = elapsed_us(phase_started);
        self.group_commit.mark_durable(end_lsn);
        let now_ms = current_unix_ms();
        let wal_bytes_now = self.log_state_atomics.wal_bytes();
        let mut checkpoint = self.checkpoint_lock()?;
        checkpoint.last_checkpoint_lsn = end_lsn;
        checkpoint.last_checkpoint_unix_ms = Some(now_ms);
        drop(checkpoint);
        // Mirror to the atomics so `should_checkpoint` can skip the slow
        // path without touching the checkpoint mutex.
        self.last_checkpoint_lsn_atomic
            .store(end_lsn, Ordering::Release);
        self.last_checkpoint_unix_ms_atomic
            .store(now_ms, Ordering::Release);
        self.wal_bytes_after_last_checkpoint_atomic
            .store(wal_bytes_now, Ordering::Release);
        // Publish the checkpoint LSN to the buffer pool so the
        // heap-side first-update FPI gate can skip pages already
        // WAL-touched in this checkpoint window.
        self.buffer_pool.set_last_checkpoint_lsn(redo_floor);
        self.record_checkpoint_success(
            started,
            trigger,
            if sharp {
                CHECKPOINT_KIND_SHARP
            } else {
                CHECKPOINT_KIND_FUZZY
            },
            redo_floor,
            dirty_page_count,
            wal_bytes_before,
            wal_bytes_now,
            snapshot_us,
            flush_us,
            sync_us,
            compact_us,
        );
        Ok(())
    }

    /// Replica-side restartpoint (PostgreSQL terminology): make every
    /// applied WAL effect durable in the data files, then prune local
    /// WAL segments that recovery no longer needs. Never appends to the
    /// WAL — the local WAL carries master LSNs only.
    pub(crate) fn replica_restartpoint(
        &self,
        started: Instant,
        trigger: u8,
    ) -> Result<(), QueryError> {
        let wal_bytes_before = self.log_state_atomics.wal_bytes();
        let _apply_barrier = self.replica_apply_checkpoint.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "replica apply/restartpoint lock poisoned",
            )
        })?;
        let applied_lsn = self.replica_applied_lsn_atomic.load(Ordering::Acquire);
        let wal_durable_lsn = self.log.lock().durable_lsn();
        let persisted = crate::repl::replica::progress::load_progress(&self.data_dir)
            .map_err(io_err)?.map(|p| p.durable_applied_lsn).unwrap_or(0);
        let mut restart_lsn = applied_lsn.min(wal_durable_lsn).min(persisted);
        // Keep every streamed in-flight BEGIN, including transactions whose
        // first write has not arrived yet. Data-file durability alone cannot
        // reconstruct their MVCC identity after restart.
        if let Some(floor) = self.txns_read()?.min_replica_in_flight_wal_lsn() {
            restart_lsn = restart_lsn.min(floor);
        }
        if let Some(auto_increment_floor) = self
            .replica_restartpoint_tracker
            .lock()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "replica restartpoint tracker lock poisoned",
                )
            })?
            .auto_increment_floor()
        {
            restart_lsn = restart_lsn.min(auto_increment_floor);
        }
        if let Some(floor) = self
            .replica_restartpoint_tracker
            .lock()
            .map_err(|_| sequence_state_error("replica tracker poisoned"))?
            .sequence_floor()
        {
            restart_lsn = restart_lsn.min(floor);
        }
        if let Some(floor) = self.replica_vector_state.lock().images.values().copied().min() {
            restart_lsn = restart_lsn.min(floor);
        }
        if restart_lsn == 0 {
            return Ok(());
        }
        // Out-of-order source undo offsets are safely buffered and backed by
        // local durable WAL. Do not prune that WAL until the missing physical
        // prefix arrives and every buffered frame lands in the undo segment.
        if self.has_pending_replicated_undo() {
            return Ok(());
        }
        if let Some(floor) = self.vector_pending_wal.lock().values().copied().min() {
            restart_lsn = restart_lsn.min(floor);
        }
        let completing_floor = self.replica_vector_state.lock().transactions.values()
            .filter(|txn| txn.completing).flat_map(|txn| txn.records.first().map(|record| record.lsn)).min();
        let vector_covered = applied_lsn.min(completing_floor.unwrap_or(u64::MAX).saturating_sub(1));
        self.vector_graph_persist_all_dirty(vector_covered)?;
        let phase_started = Instant::now();
        let (_, dirty_page_count) = self.buffer_pool.checkpoint_dirty_snapshot()?;
        let snapshot_us = elapsed_us(phase_started);
        // Everything the applier wrote (redo_* file writes + buffer-pool
        // resident pages) must be durable before WAL below the applied
        // watermark can go away. The write-side barrier keeps the watermark
        // and dirty-file registry stable through this sequence.
        let phase_started = Instant::now();
        self.flush_dirty_data()?;
        let flush_us = elapsed_us(phase_started);
        let phase_started = Instant::now();
        self.sync_checkpoint_undo()?;
        self.temporal_history.checkpoint_sync()?;
        self.sync_replica_restartpoint_files()?;
        let sync_us = elapsed_us(phase_started);
        // Data files now cover every record below `restart_lsn`; sealed
        // segments entirely below it are dead weight. The active segment
        // and any segment straddling the watermark stay.
        let phase_started = Instant::now();
        crate::wal::persist_source_clock(
            &self.data_dir,
            self.commit_hlc_unix_us.load(Ordering::Acquire),
        )?;
        if let Err(err) = crate::wal::LogManager::prune_checkpoint_segments(&self.log, restart_lsn)
        {
            crate::util::logging::log_message(format_args!(
                "WARNING: restartpoint {} is durable, but WAL cleanup failed (will retry): {}",
                restart_lsn,
                err.message()
            ));
        }
        let compact_us = elapsed_us(phase_started);
        self.group_commit.mark_durable(wal_durable_lsn);
        let now_ms = current_unix_ms();
        let wal_bytes_now = self.log_state_atomics.wal_bytes();
        let mut checkpoint = self.checkpoint_lock()?;
        checkpoint.last_checkpoint_lsn = restart_lsn;
        checkpoint.last_checkpoint_unix_ms = Some(now_ms);
        drop(checkpoint);
        self.last_checkpoint_lsn_atomic
            .store(restart_lsn, Ordering::Release);
        self.last_checkpoint_unix_ms_atomic
            .store(now_ms, Ordering::Release);
        self.wal_bytes_after_last_checkpoint_atomic
            .store(wal_bytes_now, Ordering::Release);
        self.restartpoints_total_atomic
            .fetch_add(1, Ordering::Relaxed);
        self.record_checkpoint_success(
            started,
            trigger,
            CHECKPOINT_KIND_RESTARTPOINT,
            restart_lsn,
            dirty_page_count,
            wal_bytes_before,
            wal_bytes_now,
            snapshot_us,
            flush_us,
            sync_us,
            compact_us,
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn record_checkpoint_success(
        &self,
        started: Instant,
        trigger: u8,
        kind: u8,
        redo_lsn: u64,
        dirty_pages: u64,
        wal_bytes_before: u64,
        wal_bytes_after: u64,
        snapshot_us: u64,
        flush_us: u64,
        sync_us: u64,
        compact_us: u64,
    ) {
        let reclaimed = wal_bytes_before.saturating_sub(wal_bytes_after);
        self.last_checkpoint_trigger_atomic
            .store(trigger, Ordering::Release);
        self.last_checkpoint_kind_atomic
            .store(kind, Ordering::Release);
        self.last_checkpoint_redo_lsn_atomic
            .store(redo_lsn, Ordering::Release);
        self.last_checkpoint_dirty_pages_atomic
            .store(dirty_pages, Ordering::Release);
        self.last_checkpoint_wal_reclaimed_bytes_atomic
            .store(reclaimed, Ordering::Release);
        self.checkpoint_wal_reclaimed_bytes_total_atomic
            .fetch_add(reclaimed, Ordering::Relaxed);
        self.last_checkpoint_snapshot_us_atomic
            .store(snapshot_us, Ordering::Release);
        self.last_checkpoint_flush_us_atomic
            .store(flush_us, Ordering::Release);
        self.last_checkpoint_sync_us_atomic
            .store(sync_us, Ordering::Release);
        self.last_checkpoint_compact_us_atomic
            .store(compact_us, Ordering::Release);
        self.checkpoints_total_atomic
            .fetch_add(1, Ordering::Relaxed);
        let duration_us = elapsed_us(started);
        self.last_checkpoint_duration_us_atomic
            .store(duration_us, Ordering::Release);
        self.checkpoint_duration_total_us_atomic
            .fetch_add(duration_us, Ordering::Relaxed);
        self.checkpoint_duration_max_us_atomic
            .fetch_max(duration_us, Ordering::Relaxed);
    }

    fn sync_replica_restartpoint_files(&self) -> Result<(), QueryError> {
        let mut tracker = self.replica_restartpoint_tracker.lock().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "replica restartpoint tracker lock poisoned",
            )
        })?;
        if tracker.needs_full_sync {
            sync_replica_data_tree(
                &self.data_dir,
                &crate::storage::naming::system_wal_dir(&self.data_dir),
            )?;
        } else {
            for relative in &tracker.dirty_paths {
                let path = self.data_dir.join(relative);
                match crate::platform::fs::sync_path(&path) {
                    Ok(()) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => return Err(io_err(err)),
                }
                if let Some(parent) = path.parent() {
                    crate::platform::fs::sync_path(parent).map_err(io_err)?;
                }
            }
        }
        tracker.needs_full_sync = false;
        tracker.clear_and_release();
        Ok(())
    }

    fn should_checkpoint(&self) -> Option<u8> {
        if self.config.durability == DurabilityMode::Memory {
            return None;
        }
        // Probed by the background checkpointer once per poll tick, so
        // it must stay lock-free: atomics only.
        let current_lsn = self.log_state_atomics.next_lsn();
        let last_checkpoint_lsn = self.last_checkpoint_lsn_atomic.load(Ordering::Acquire);
        // `LogManager::next_lsn()` is the next allocatable LSN, not the
        // most recently written one. Immediately after a checkpoint compacts
        // to LSN N, next_lsn is N + 1; with no new WAL since then there is
        // nothing to checkpoint.
        if current_lsn <= last_checkpoint_lsn.saturating_add(1) {
            return None;
        }
        // Size trigger fires on WAL growth SINCE the last checkpoint.
        // Comparing the absolute size would re-trigger immediately when
        // retained history (in-flight txn undo chains, replication
        // slots) keeps the WAL above the threshold even though another
        // checkpoint cannot shrink it.
        let wal_threshold = self.runtime_config_read().checkpoint_wal_bytes;
        let wal_bytes = self.log_state_atomics.wal_bytes();
        let baseline = self
            .wal_bytes_after_last_checkpoint_atomic
            .load(Ordering::Acquire);
        let size_triggered = wal_bytes.saturating_sub(baseline) >= wal_threshold;
        let last_unix_ms = self.last_checkpoint_unix_ms_atomic.load(Ordering::Acquire);
        let interval = self
            .config
            .checkpoint_interval
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        // The unix-ms atomic is armed to the startup time when the
        // Database opens, so the interval trigger works from the first
        // process minute onward (it used to stay dead until a first
        // size-triggered checkpoint seeded it).
        let now_ms = current_unix_ms();
        let time_since_checkpoint = now_ms.saturating_sub(last_unix_ms);
        let time_triggered = last_unix_ms != 0 && time_since_checkpoint >= interval;
        let min_auto_interval = interval.saturating_div(4).clamp(
            MIN_AUTO_CHECKPOINT_INTERVAL_MS.min(interval),
            MAX_AUTO_CHECKPOINT_INTERVAL_MS,
        );
        let last_auto_ms = self
            .last_auto_checkpoint_unix_ms_atomic
            .load(Ordering::Acquire);
        let size_triggered = size_triggered
            && (last_auto_ms == 0 || now_ms.saturating_sub(last_auto_ms) >= min_auto_interval);
        // In-flight write transactions no longer block checkpoints: the
        // retention floor inside `checkpoint_locked` keeps every WAL
        // record their undo chains need, so checkpointing under load is
        // safe — it just cannot collapse the whole log.
        if size_triggered {
            Some(CHECKPOINT_TRIGGER_WAL)
        } else if !self.physical_replica_mode()
            && self.sequence_dirty_generations.load(Ordering::Acquire) >= 4096
            && (last_auto_ms == 0 || now_ms.saturating_sub(last_auto_ms) >= min_auto_interval)
        {
            Some(CHECKPOINT_TRIGGER_SEQUENCE)
        } else if time_triggered {
            Some(CHECKPOINT_TRIGGER_TIME)
        } else {
            None
        }
    }

    /// Called inside the binlog commit gate before RESET reuses filenames.
    pub(crate) fn reset_binlog_wal_cutoff(&self) -> Result<u64, QueryError> {
        LogManager::sync_allocation_boundary(&self.log)
    }

    #[cfg(test)]
    /// Lock-free snapshot of the WAL state atomics. Lets tests probe
    /// the WAL byte/LSN watermark without going through `storage_status`
    /// (which also touches the buffer pool / checkpoint locks).
    pub fn log_state_atomics(&self) -> Arc<crate::wal::LogStateAtomics> {
        Arc::clone(&self.log_state_atomics)
    }

    pub fn mysql_status(&self) -> MysqlStatus {
        #[cfg(test)]
        MYSQL_STATUS_SNAPSHOT_CALLS.fetch_add(1, Ordering::Relaxed);
        let (wal_bytes, wal_syncs, wal_stats) = {
            let log = self.log.lock();
            (log.wal_bytes(), log.sync_count(), log.stats())
        };
        let (undo_bytes, undo_records, undo_peak_bytes) = {
            let undo = self.undo.read();
            (undo.bytes(), undo.record_count(), undo.peak_bytes())
        };
        let active_transactions = self.txns.read().active_txn_count();
        let (
            lock_waits,
            active_lock_waits,
            lock_wait_time_ms,
            lock_wait_time_max_ms,
            deadlocks,
            table_lock_acquires,
            table_lock_waits,
        ) = {
            let locks = self.lock_manager_read();
            (
                locks.lock_wait_count,
                locks.active_lock_waits(),
                locks.lock_wait_time_ms,
                locks.lock_wait_time_max_ms,
                locks.deadlock_count,
                locks.table_lock_acquires,
                locks.table_lock_waits,
            )
        };
        let lock_wait_time_avg_ms = if lock_waits == 0 {
            0
        } else {
            lock_wait_time_ms / lock_waits
        };
        let pool = &self.buffer_pool;
        MysqlStatus {
            buffer_hits: pool.buffer_stats().hits,
            buffer_misses: pool.misses.load(Ordering::Relaxed),
            buffer_pool_dirty_pages: pool.dirty_pages.load(Ordering::Relaxed),
            buffer_pool_pages: pool.capacity_pages.load(Ordering::Relaxed),
            buffer_pool_used_pages: pool.used_pages.load(Ordering::Relaxed),
            buffer_pool_waits: pool.waits.load(Ordering::Relaxed),
            buffer_pool_write_requests: pool.write_requests.load(Ordering::Relaxed),
            buffer_pool_read_ios: pool.read_ios.load(Ordering::Relaxed),
            buffer_pool_read_syscalls: pool.read_syscalls.load(Ordering::Relaxed),
            buffer_pool_write_ios: pool.write_ios.load(Ordering::Relaxed),
            buffer_pool_write_syscalls: pool.write_syscalls.load(Ordering::Relaxed),
            buffer_pool_evictions: pool.evictions.load(Ordering::Relaxed),
            buffer_pool_prefetches: pool.prefetches.load(Ordering::Relaxed),
            buffer_pool_prefetch_hits: pool.prefetch_hits.load(Ordering::Relaxed),
            lock_waits,
            active_lock_waits,
            lock_wait_time_ms,
            lock_wait_time_avg_ms,
            lock_wait_time_max_ms,
            deadlocks,
            table_lock_acquires,
            table_lock_waits,
            wal_syncs,
            wal_logical_records: wal_stats.logical_records,
            wal_bytes,
            wal_physical_write_calls: wal_stats.physical_write_calls,
            wal_physical_write_bytes: wal_stats.physical_write_bytes,
            active_transactions,
            undo_bytes,
            undo_records,
            undo_peak_bytes,
            checkpoints_total: self.checkpoints_total_atomic.load(Ordering::Relaxed),
            plan_cache_hits: self.plan_cache_hits.load(Ordering::Relaxed),
            plan_cache_misses: self.plan_cache_misses.load(Ordering::Relaxed),
            select_parse_cache_hits: self.select_parse_cache_hits.load(Ordering::Relaxed),
            select_compiled_plan_hits: self.select_compiled_plan_hits.load(Ordering::Relaxed),
            select_operator_compiles: self.select_operator_compiles.load(Ordering::Relaxed),
            prepared_plan_reuses: self.prepared_plan_reuses.load(Ordering::Relaxed),
            prepared_plan_compiles: self.prepared_plan_compiles.load(Ordering::Relaxed),
            snapshot_too_old_count: self.snapshot_too_old_total.load(Ordering::Relaxed),
            dml_insert_rows: self.dml_stats.insert_rows.load(Ordering::Relaxed),
            dml_update_rows: self.dml_stats.update_rows.load(Ordering::Relaxed),
            dml_delete_rows: self.dml_stats.delete_rows.load(Ordering::Relaxed),
        }
    }

    pub fn storage_status(&self) -> StorageStatus {
        let (
            current_lsn,
            durable_lsn,
            wal_bytes,
            wal_syncs,
            wal_stats,
            wal_segment_count,
            wal_active_segment_id,
            wal_archived_segment_count,
            wal_archive_enabled,
            wal_pending_sealed_sync,
        ) = {
            let log = self.log.lock();
            (
                log.next_lsn(),
                log.durable_lsn(),
                log.wal_bytes(),
                log.sync_count(),
                log.stats(),
                log.segment_count() as u64,
                log.active_segment_id(),
                log.archived_segment_count() as u64,
                log.archive.enabled,
                log.pending_sealed_sync_count() as u64,
            )
        };
        let wal_sync_avg_us = if wal_syncs == 0 {
            0
        } else {
            wal_stats.sync_total_us / wal_syncs
        };
        let checkpoint = self
            .checkpoint
            .lock()
            .map(|checkpoint| *checkpoint)
            .unwrap_or_default();
        let buffer_stats = self.buffer_stats();
        let buffer_accesses = buffer_stats.hits.saturating_add(buffer_stats.misses);
        let buffer_hit_pct = if buffer_accesses == 0 {
            0
        } else {
            buffer_stats.hits.saturating_mul(100) / buffer_accesses
        };
        let group_commit_stats = self
            .group_commit
            .stats()
            .combined(self.flush_group_commit.stats());
        let durability_worker_enabled = crate::storage::txn::durability_worker_enabled();
        let durability_stats = self.durability_worker.stats();
        let effective_batches = if durability_worker_enabled {
            durability_stats.batches
        } else {
            group_commit_stats.batches
        };
        let effective_requests = if durability_worker_enabled {
            durability_stats.requests
        } else {
            group_commit_stats.total_batch_size
        };
        let avg_group_commit_size = if effective_batches == 0 {
            0
        } else {
            effective_requests / effective_batches
        };
        let (
            undo_bytes,
            undo_records,
            undo_purged_records,
            undo_peak_bytes,
            undo_peak_records,
            undo_longest_chain,
            undo_retention_pressure,
            undo_mem_pressure,
            undo_segment_count,
            undo_disk_bytes,
            undo_reclaimable_bytes,
            undo_reclaimed_bytes,
            undo_purge_runs,
            undo_purge_errors,
            undo_purge_last_unix_ms,
            undo_cache_hits,
            undo_cache_misses,
            undo_resident_bytes,
            undo_mem_cache_limit_bytes,
            undo_evicted_records,
            undo_txn_max_bytes,
            undo_largest_txn_id,
            undo_largest_txn_bytes,
        ) = {
            let undo = self.undo.read();
            let (largest_txn_id, largest_txn_bytes) = undo.largest_txn();
            (
                undo.bytes(),
                undo.record_count(),
                undo.purged_records,
                undo.peak_bytes(),
                undo.peak_records(),
                undo.longest_chain_depth(),
                undo.retention_pressure_count(),
                undo.mem_pressure_count,
                undo.segment_count(),
                undo.disk_bytes,
                undo.reclaimable_bytes(),
                undo.reclaimed_bytes,
                undo.purge_runs,
                undo.purge_errors,
                undo.purge_last_unix_ms,
                undo.cache_hits(),
                undo.cache_misses(),
                undo.resident_bytes(),
                undo.mem_cache_limit_bytes(),
                undo.evicted_records(),
                undo.txn_max_bytes(),
                largest_txn_id,
                largest_txn_bytes,
            )
        };
        let runtime_config = self.runtime_config_read();
        let buffer_pool = self.buffer_pool_snapshot();
        let snapshot_stats = self.snapshot_stats();
        let row_migrations = self.row_migration_count();
        let plan_cache_hits = self.plan_cache_hits.load(Ordering::Relaxed);
        let plan_cache_misses = self.plan_cache_misses.load(Ordering::Relaxed);
        let plan_cache_accesses = plan_cache_hits.saturating_add(plan_cache_misses);
        let plan_cache_hit_pct = if plan_cache_accesses == 0 {
            0
        } else {
            plan_cache_hits.saturating_mul(100) / plan_cache_accesses
        };
        let select_parse_cache_hits = self.select_parse_cache_hits.load(Ordering::Relaxed);
        let select_compiled_plan_hits = self.select_compiled_plan_hits.load(Ordering::Relaxed);
        let select_operator_compiles = self.select_operator_compiles.load(Ordering::Relaxed);
        let prepared_plan_reuses = self.prepared_plan_reuses.load(Ordering::Relaxed);
        let prepared_plan_compiles = self.prepared_plan_compiles.load(Ordering::Relaxed);
        let prepared_dml_reuses = self.prepared_dml_reuses.load(Ordering::Relaxed);
        let prepared_dml_compiles = self.prepared_dml_compiles.load(Ordering::Relaxed);
        let (active_txns, oldest_active_txn_ms) = {
            let txns = self.txns.read();
            (txns.active_txn_count(), txns.oldest_active_txn_ms())
        };
        let txn_stats = {
            let lm = self.lock_manager_read();
            (
                active_txns,
                oldest_active_txn_ms,
                lm.lock_wait_count,
                lm.active_lock_waits(),
                lm.oldest_lock_wait_ms(),
                lm.deadlock_count,
                lm.lock_wait_time_ms,
                lm.lock_wait_time_max_ms,
            )
        };
        let (
            lock_wait_histogram,
            row_x_lock_acquires,
            row_s_lock_acquires,
            lock_upgrades,
            lock_upgrade_failures,
            table_lock_acquires,
            table_lock_waits,
            chunked_release_chunks,
        ) = {
            let lm = self.lock_manager_read();
            (
                lm.lock_wait_histogram(),
                lm.row_x_lock_acquires,
                lm.row_s_lock_acquires,
                lm.lock_upgrades,
                lm.lock_upgrade_failures,
                lm.table_lock_acquires,
                lm.table_lock_waits,
                lm.chunked_release_chunks,
            )
        };
        let wait_event_summary = self
            .wait_events
            .lock()
            .map(|history| {
                history
                    .aggregates()
                    .into_iter()
                    .map(|(event, agg)| {
                        (
                            event.as_str().to_string(),
                            agg.count,
                            agg.total_us,
                            agg.max_us,
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let bulk_index = bulk_index_build_status();
        let (
            catalog_txn_commits,
            catalog_txn_rollbacks,
            catalog_mutated_rows,
            catalog_epoch,
            catalog_cache_limit_bytes,
            catalog_cache_bytes,
            catalog_cache_entries,
        ) = self.catalog_store_stats();
        let dml = self.dml_stats_snapshot();
        StorageStatus {
            durability: self.config.durability.as_str(),
            current_lsn,
            durable_lsn,
            wal_bytes,
            wal_logical_records: wal_stats.logical_records,
            wal_physical_write_calls: wal_stats.physical_write_calls,
            wal_physical_write_bytes: wal_stats.physical_write_bytes,
            wal_page_fallback_records: wal_stats.page_fallback_records,
            wal_index_batch_records: wal_stats.index_batch_records,
            wal_heap_payload_bytes: wal_stats.heap_payload_bytes,
            wal_index_payload_bytes: wal_stats.index_payload_bytes,
            wal_page_payload_bytes: wal_stats.page_payload_bytes,
            wal_heap_fpi_payload_bytes: wal_stats.heap_fpi_payload_bytes,
            wal_index_page_payload_bytes: wal_stats.index_page_payload_bytes,
            wal_other_page_payload_bytes: wal_stats.other_page_payload_bytes,
            reduced_redo_transactions: REDUCED_REDO_TRANSACTIONS.load(Ordering::Relaxed),
            reduced_redo_skipped_records: REDUCED_REDO_SKIPPED_RECORDS.load(Ordering::Relaxed),
            reduced_redo_skipped_payload_bytes: REDUCED_REDO_SKIPPED_PAYLOAD_BYTES
                .load(Ordering::Relaxed),
            reduced_redo_fallback_records: REDUCED_REDO_FALLBACK_RECORDS.load(Ordering::Relaxed),
            reduced_redo_force_flushes: REDUCED_REDO_FORCE_FLUSHES.load(Ordering::Relaxed),
            reduced_redo_force_flush_us: REDUCED_REDO_FORCE_FLUSH_US.load(Ordering::Relaxed),
            wal_sync_total_us: wal_stats.sync_total_us,
            wal_sync_avg_us,
            wal_sync_max_us: wal_stats.sync_max_us,
            wal_rotation_count: wal_stats.rotation_count,
            wal_rotation_critical_total_us: wal_stats.rotation_critical_total_us,
            wal_rotation_critical_max_us: wal_stats.rotation_critical_max_us,
            wal_pending_sealed_sync,
            checkpoint_lsn: checkpoint.last_checkpoint_lsn,
            checkpoint_age_lsn: current_lsn.saturating_sub(checkpoint.last_checkpoint_lsn),
            checkpoints_total: self.checkpoints_total_atomic.load(Ordering::Relaxed),
            checkpoint_failures_total: self
                .checkpoint_failures_total_atomic
                .load(Ordering::Relaxed),
            checkpoint_preflush_failures_total: self
                .checkpoint_preflush_failures_total_atomic
                .load(Ordering::Relaxed),
            checkpoint_redo_lsn: self.last_checkpoint_redo_lsn_atomic.load(Ordering::Acquire),
            checkpoint_redo_age_lsn: current_lsn
                .saturating_sub(self.last_checkpoint_redo_lsn_atomic.load(Ordering::Acquire)),
            last_checkpoint_dirty_pages: self
                .last_checkpoint_dirty_pages_atomic
                .load(Ordering::Relaxed),
            last_checkpoint_wal_reclaimed_bytes: self
                .last_checkpoint_wal_reclaimed_bytes_atomic
                .load(Ordering::Relaxed),
            checkpoint_wal_reclaimed_bytes_total: self
                .checkpoint_wal_reclaimed_bytes_total_atomic
                .load(Ordering::Relaxed),
            restartpoints_total: self.restartpoints_total_atomic.load(Ordering::Relaxed),
            replica_applied_lsn: self.replica_applied_lsn_atomic.load(Ordering::Acquire),
            last_checkpoint_trigger: checkpoint_trigger_name(
                self.last_checkpoint_trigger_atomic.load(Ordering::Acquire),
            ),
            last_checkpoint_kind: checkpoint_kind_name(
                self.last_checkpoint_kind_atomic.load(Ordering::Acquire),
            ),
            last_checkpoint_snapshot_us: self
                .last_checkpoint_snapshot_us_atomic
                .load(Ordering::Relaxed),
            last_checkpoint_flush_us: self.last_checkpoint_flush_us_atomic.load(Ordering::Relaxed),
            last_checkpoint_sync_us: self.last_checkpoint_sync_us_atomic.load(Ordering::Relaxed),
            last_checkpoint_compact_us: self
                .last_checkpoint_compact_us_atomic
                .load(Ordering::Relaxed),
            last_checkpoint_duration_us: self
                .last_checkpoint_duration_us_atomic
                .load(Ordering::Relaxed),
            checkpoint_duration_total_us: self
                .checkpoint_duration_total_us_atomic
                .load(Ordering::Relaxed),
            checkpoint_duration_max_us: self
                .checkpoint_duration_max_us_atomic
                .load(Ordering::Relaxed),
            buffer_hits: buffer_stats.hits,
            buffer_misses: buffer_stats.misses,
            buffer_hit_pct,
            dirty_heap_pages: buffer_pool.dirty_heap_pages as usize,
            dirty_index_pages: buffer_pool.dirty_index_pages as usize,
            buffer_pool_used_pages: buffer_pool.used_pages,
            buffer_pool_dirty_pages: buffer_pool.dirty_pages,
            buffer_pool_freeable_pages: buffer_pool.freeable_pages,
            buffer_pool_evictions: buffer_pool.evictions,
            buffer_pool_flushes: buffer_pool.flushes,
            buffer_pool_write_requests: buffer_pool.write_requests,
            buffer_pool_flush_errors: buffer_pool.flush_errors,
            buffer_pool_background_io_errors: buffer_pool.background_io_errors,
            buffer_pool_pinned_pages: buffer_pool.pinned_pages,
            buffer_pool_waits: buffer_pool.waits,
            buffer_pool_wait_ms: buffer_pool.wait_ms,
            buffer_pool_background_flushes: buffer_pool.background_flushes,
            buffer_pool_shards: buffer_pool.shards,
            buffer_pool_read_ios: buffer_pool.read_ios,
            buffer_pool_read_ios_by_kind: buffer_pool.kind_read_ios,
            buffer_pool_read_syscalls: buffer_pool.read_syscalls,
            buffer_pool_batch_read_ops: buffer_pool.batch_read_ops,
            buffer_pool_batch_read_pages: buffer_pool.batch_read_pages,
            buffer_pool_write_ios: buffer_pool.write_ios,
            buffer_pool_write_syscalls: buffer_pool.write_syscalls,
            buffer_pool_prefetches: buffer_pool.prefetches,
            buffer_pool_prefetch_hits: buffer_pool.prefetch_hits,
            buffer_pool_prefetch_wasted: buffer_pool.prefetch_wasted,
            buffer_pool_prefetch_demand_waits: buffer_pool.prefetch_demand_waits,
            buffer_pool_eviction_scans: buffer_pool.eviction_scans,
            buffer_pool_io_threads: buffer_pool.io_threads,
            buffer_pool_prefetch_pages: buffer_pool.prefetch_pages,
            buffer_pool_small_scan_pct: buffer_pool.small_scan_pct,
            buffer_pool_small_scan_threshold_pages: buffer_pool.small_scan_threshold_pages,
            buffer_pool_small_scan_accesses: buffer_pool.small_scan_accesses,
            buffer_pool_small_scan_cold_accesses: buffer_pool.small_scan_cold_accesses,
            buffer_pool_small_scan_promotions: buffer_pool.small_scan_promotions,
            buffer_pool_small_scan_read_ios: buffer_pool.small_scan_read_ios,
            active_transactions: txn_stats.0,
            oldest_snapshot_scn: snapshot_stats.0,
            oldest_active_txn_ms: txn_stats.1,
            latch_waits: 0,
            lock_waits: txn_stats.2,
            active_lock_waits: txn_stats.3,
            oldest_lock_wait_ms: txn_stats.4,
            deadlocks: txn_stats.5,
            lock_wait_ms: self.config.lock_wait.as_millis().min(u128::from(u64::MAX)) as u64,
            lock_wait_time_ms: txn_stats.6,
            lock_wait_time_avg_ms: if txn_stats.2 == 0 {
                0
            } else {
                txn_stats.6 / txn_stats.2
            },
            lock_wait_time_max_ms: txn_stats.7,
            // Reflect the *runtime* window (settable via SET GLOBAL), not the
            // startup default.
            group_commit_us: runtime_config.group_commit_us,
            group_commit_adaptive: runtime_config.group_commit_adaptive,
            group_commit_batches: effective_batches,
            group_commit_waiters: effective_requests,
            avg_group_commit_size,
            group_commit_leader_wait_total_us: group_commit_stats.leader_wait_total_us,
            group_commit_follower_wait_total_us: group_commit_stats.follower_wait_total_us,
            group_commit_sync_op_total_us: group_commit_stats.sync_op_total_us,
            group_commit_leader_wait_max_us: group_commit_stats.leader_wait_max_us,
            group_commit_follower_wait_max_us: group_commit_stats.follower_wait_max_us,
            group_commit_sync_op_max_us: group_commit_stats.sync_op_max_us,
            group_commit_high_water_released: group_commit_stats.high_water_released,
            group_commit_recent_avg_batch_size_x100: (group_commit_stats
                .recent_avg_batch_size
                .max(0.0)
                * 100.0)
                .round()
                .min(u64::MAX as f64) as u64,
            group_commit_batch_p50: batch_size_percentile(
                &group_commit_stats.batch_size_buckets,
                50,
            ),
            group_commit_batch_p99: batch_size_percentile(
                &group_commit_stats.batch_size_buckets,
                99,
            ),
            durability_worker_enabled,
            durability_queue_depth: durability_stats.queue_depth,
            durability_queue_depth_peak: durability_stats.queue_depth_peak,
            durability_batches: durability_stats.batches,
            durability_requests: durability_stats.requests,
            durability_batch_max: durability_stats.batch_max,
            wal_syncs,
            wal_segment_bytes: self.config.wal_segment_bytes,
            buffer_pool_pages: buffer_pool.capacity_pages,
            row_migrations,
            undo_bytes,
            undo_records,
            undo_retention_ms: runtime_config
                .undo_retention
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
            temporal_earliest_available_unix_us: self.temporal_history.earliest_time().unwrap_or(0),
            temporal_commit_ledger_bytes: self.temporal_history.bytes(),
            temporal_history_index_bytes: self.temporal_history.history_index_bytes(),
            temporal_history_index_entries: self.temporal_history.history_index_entry_count(),
            temporal_history_compactions: self.temporal_history.history_compactions(),
            temporal_history_last_compaction_unix_us: self
                .temporal_history
                .history_last_compaction_unix_us(),
            temporal_queries: self.temporal_query_total.load(Ordering::Relaxed),
            temporal_history_index_hits: self
                .temporal_history_index_hit_total
                .load(Ordering::Relaxed),
            temporal_heap_fallbacks: self.temporal_heap_fallback_total.load(Ordering::Relaxed),
            temporal_hlc_rollbacks: self.temporal_hlc_rollback_total.load(Ordering::Relaxed),
            undo_purged_records,
            plan_cache_hits,
            plan_cache_misses,
            plan_cache_hit_pct,
            select_parse_cache_hits,
            select_compiled_plan_hits,
            select_operator_compiles,
            prepared_plan_reuses,
            prepared_plan_compiles,
            prepared_dml_reuses,
            prepared_dml_compiles,
            checkpoint_interval_ms: self
                .config
                .checkpoint_interval
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
            checkpoint_wal_bytes: runtime_config.checkpoint_wal_bytes,
            startup_validate_indexes: self.config.startup_validate_indexes,
            last_checkpoint_unix_ms: checkpoint.last_checkpoint_unix_ms,
            last_recovery_summary: self.last_recovery.describe(),
            // M1.5 - Extended fields
            snapshot_too_old_count: self.snapshot_too_old_total.load(Ordering::Relaxed),
            read_only_txn_count: self.read_only_txn_count.load(Ordering::Relaxed),
            min_retained_txn_id: self.min_retained_txn_id.load(Ordering::Relaxed),
            index_page_max_trx_shortcut_hits: self
                .index_page_max_trx_shortcut_hits
                .load(Ordering::Relaxed),
            index_page_max_trx_shortcut_misses: self
                .index_page_max_trx_shortcut_misses
                .load(Ordering::Relaxed),
            undo_peak_bytes,
            undo_peak_records,
            undo_longest_chain,
            undo_retention_pressure,
            undo_segment_count,
            undo_segment_bytes: runtime_config.undo_segment_bytes,
            undo_disk_bytes,
            undo_reclaimable_bytes,
            undo_reclaimed_bytes,
            undo_tablespace_max_bytes: runtime_config.undo_tablespace_max_bytes,
            undo_mem_soft_limit_bytes: runtime_config.undo_mem_soft_limit_bytes,
            undo_mem_cache_limit_bytes,
            undo_resident_bytes,
            undo_evicted_records,
            undo_txn_max_bytes,
            undo_largest_txn_id,
            undo_largest_txn_bytes,
            undo_mem_pressure,
            undo_retention_guarantee: runtime_config.undo_retention_guarantee,
            undo_purge_interval_ms: runtime_config
                .undo_purge_interval
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
            undo_purge_batch_records: runtime_config.undo_purge_batch_records as u64,
            undo_purge_runs,
            undo_purge_errors,
            undo_purge_last_unix_ms,
            undo_cache_hits,
            undo_cache_misses,
            lock_wait_histogram,
            row_x_lock_acquires,
            row_s_lock_acquires,
            lock_upgrades,
            lock_upgrade_failures,
            table_lock_acquires,
            table_lock_waits,
            chunked_release_chunks,
            wait_event_summary,
            wal_segment_count,
            wal_active_segment_id,
            wal_archived_segment_count,
            wal_archive_enabled,
            buffer_pool_read_ios_normal: buffer_pool.read_ios_normal,
            buffer_pool_read_ios_scan: buffer_pool.read_ios_scan,
            buffer_pool_read_ios_prefetch: buffer_pool.read_ios_prefetch,
            buffer_pool_prefetch_queue_drops: buffer_pool.prefetch_queue_drops,
            buffer_pool_prefetch_duplicate_drops: buffer_pool.prefetch_duplicate_drops,
            buffer_pool_prefetch_pressure_drops: buffer_pool.prefetch_pressure_drops,
            buffer_pool_prefetch_queue_full_drops: buffer_pool.prefetch_queue_full_drops,
            buffer_pool_prefetch_stale_drops: buffer_pool.prefetch_stale_drops,
            buffer_pool_prefetch_queue_wait_avg_us: if buffer_pool.prefetch_queue_drains == 0 {
                0
            } else {
                buffer_pool.prefetch_queue_wait_us_total / buffer_pool.prefetch_queue_drains
            },
            buffer_pool_prefetch_lead_avg_us: if buffer_pool.prefetch_lead_samples == 0 {
                0
            } else {
                buffer_pool.prefetch_lead_us_total / buffer_pool.prefetch_lead_samples
            },
            buffer_pool_ensure_capacity_busy_us: buffer_pool.ensure_capacity_busy_us,
            buffer_pool_file_handle_wait_ns: buffer_pool.file_handle_wait_ns,
            buffer_pool_dirty_ratio_histogram: buffer_pool.dirty_ratio_histogram,
            buffer_pool_dirty_ratio_samples: buffer_pool.dirty_ratio_samples,
            buffer_pool_shard_imbalance_pct: buffer_pool.shard_imbalance_pct,
            buffer_pool_shard_max_hits: buffer_pool.shard_max_hits,
            buffer_pool_shard_min_hits: buffer_pool.shard_min_hits,
            buffer_pool_scan_origin_share_pct: buffer_pool.scan_origin_share_pct,
            buffer_pool_scan_origin_evictions: buffer_pool.scan_origin_evictions,
            buffer_pool_cold_recycle_hits: buffer_pool.cold_recycle_hits,
            buffer_pool_cold_recycle_fallbacks: buffer_pool.cold_recycle_fallbacks,
            buffer_pool_raw_scan_pages: buffer_pool.raw_scan_pages,
            buffer_pool_raw_page_allocations: buffer_pool.raw_page_allocations,
            buffer_pool_raw_page_reuse_hits: buffer_pool.raw_page_reuse_hits,
            buffer_pool_raw_page_reuse_misses: buffer_pool.raw_page_reuse_misses,
            buffer_pool_recycled_raw_pages: buffer_pool.recycled_raw_pages,
            buffer_pool_pages_heap: buffer_pool.kind_used_pages[0],
            buffer_pool_pages_index: buffer_pool.kind_used_pages[1],
            buffer_pool_pages_lob: buffer_pool.kind_used_pages[2],
            buffer_pool_pages_vector: buffer_pool.kind_used_pages[3],
            buffer_pool_pages_zone_map: buffer_pool.kind_used_pages[4],
            buffer_pool_quota_evictions: buffer_pool.quota_evictions,
            buffer_pool_file_syncs: buffer_pool.file_syncs,
            buffer_pool_file_sync_skipped: buffer_pool.file_sync_skipped,
            buffer_pool_flush_data_cache_short_circuits: buffer_pool
                .flush_data_cache_short_circuits,
            buffer_pool_flush_data_cache_pages: buffer_pool.flush_data_cache_pages,
            buffer_pool_flush_data_cache_us: buffer_pool.flush_data_cache_us,
            buffer_pool_evict_sync_dirty_flushes: buffer_pool.evict_sync_dirty_flushes,
            buffer_pool_evict_sync_dirty_flush_us: buffer_pool.evict_sync_dirty_flush_us,
            buffer_pool_cooldown_deferred_flushes: buffer_pool.cooldown_deferred_flushes,
            buffer_pool_headroom_flushes: buffer_pool.headroom_flushes,
            buffer_pool_checkpoint_flush_pages: buffer_pool.checkpoint_flush_pages,
            buffer_pool_checkpoint_flush_us: buffer_pool.checkpoint_flush_us,
            buffer_pool_checkpoint_preflush_pages: buffer_pool.checkpoint_preflush_pages,
            buffer_pool_checkpoint_preflush_us: buffer_pool.checkpoint_preflush_us,
            buffer_pool_prefetch_queue_depth_peak: buffer_pool.prefetch_queue_depth_peak,
            buffer_pool_oldest_dirty_lsn: buffer_pool.oldest_dirty_lsn,
            bulk_index_direct_builds: bulk_index.direct_builds,
            bulk_index_direct_pages: bulk_index.direct_pages,
            bulk_index_direct_us: bulk_index.direct_us,
            bulk_index_buffered_builds: bulk_index.buffered_builds,
            bulk_index_buffered_pages: bulk_index.buffered_pages,
            bulk_index_buffered_us: bulk_index.buffered_us,
            index_build_requested_workers: INDEX_BUILD_REQUESTED_WORKERS.load(Ordering::Relaxed),
            index_build_effective_workers: INDEX_BUILD_EFFECTIVE_WORKERS.load(Ordering::Relaxed),
            index_build_parallel_builds: INDEX_BUILD_PARALLEL_BUILDS.load(Ordering::Relaxed),
            index_build_serial_builds: INDEX_BUILD_SERIAL_BUILDS.load(Ordering::Relaxed),
            index_build_degree_downgrades: INDEX_BUILD_DEGREE_DOWNGRADES.load(Ordering::Relaxed),
            online_index_prepare_us: ONLINE_INDEX_PREPARE_US.load(Ordering::Relaxed),
            online_index_replay_us: ONLINE_INDEX_REPLAY_US.load(Ordering::Relaxed),
            online_index_validation_us: ONLINE_INDEX_VALIDATION_US.load(Ordering::Relaxed),
            online_index_publish_us: ONLINE_INDEX_PUBLISH_US.load(Ordering::Relaxed),
            online_index_barrier_wait_us: ONLINE_INDEX_BARRIER_WAIT_US.load(Ordering::Relaxed),
            online_index_tail_entries_peak: ONLINE_INDEX_TAIL_ENTRIES_PEAK.load(Ordering::Relaxed),
            online_index_tail_bytes_peak: ONLINE_INDEX_TAIL_BYTES_PEAK.load(Ordering::Relaxed),
            online_index_log_bytes_peak: ONLINE_INDEX_LOG_BYTES_PEAK.load(Ordering::Relaxed),
            index_build_scan_us: INDEX_BUILD_SCAN_US.load(Ordering::Relaxed),
            index_build_sort_us: INDEX_BUILD_SORT_US.load(Ordering::Relaxed),
            index_build_leaf_us: INDEX_BUILD_LEAF_US.load(Ordering::Relaxed),
            index_build_internal_us: INDEX_BUILD_INTERNAL_US.load(Ordering::Relaxed),
            index_build_fsync_us: INDEX_BUILD_FSYNC_US.load(Ordering::Relaxed),
            index_build_rows: INDEX_BUILD_ROWS.load(Ordering::Relaxed),
            index_build_heap_scans: INDEX_BUILD_HEAP_SCANS.load(Ordering::Relaxed),
            index_build_merge_us: INDEX_BUILD_MERGE_US.load(Ordering::Relaxed),
            index_build_spill_write_us: INDEX_BUILD_SPILL_WRITE_US.load(Ordering::Relaxed),
            index_build_spill_read_us: INDEX_BUILD_SPILL_READ_US.load(Ordering::Relaxed),
            index_build_peak_active_workers: INDEX_BUILD_PEAK_ACTIVE_WORKERS
                .load(Ordering::Relaxed),
            buffer_pool_dirty_oldest_len: buffer_pool.dirty_oldest_len,
            spill_bytes_written: buffer_pool.spill_bytes_written,
            spill_bytes_read: buffer_pool.spill_bytes_read,
            catalog_txn_commits,
            catalog_txn_rollbacks,
            catalog_mutated_rows,
            catalog_epoch,
            catalog_cache_limit_bytes,
            catalog_cache_bytes,
            catalog_cache_entries,
            dml_insert_rows: dml.insert_rows,
            dml_update_rows: dml.update_rows,
            dml_delete_rows: dml.delete_rows,
            dml_ignore_warnings: dml.ignore_warnings,
            dml_rollback_inserted_rows: dml.rollback_inserted_rows,
            dml_lob_leaks_recovered: dml.lob_leaks_recovered,
            dml_upsert_fast_path_hits: dml.upsert_fast_path_hits,
            dml_upsert_batch_attempts: dml.upsert_batch_attempts,
            dml_upsert_batch_rows: dml.upsert_batch_rows,
            dml_upsert_batch_conflict_rows: dml.upsert_batch_conflict_rows,
            dml_upsert_batch_fallback_rows: dml.upsert_batch_fallback_rows,
            dml_upsert_batch_race_rollbacks: dml.upsert_batch_race_rollbacks,
            dml_fk_cycle_short_circuits: dml.fk_cycle_short_circuits,
            load_data_active: self.dml_stats.load_data_active.load(Ordering::Relaxed),
            load_data_rows_loaded: self.dml_stats.load_data_rows_loaded.load(Ordering::Relaxed),
            dml_lob_chains_reused: dml.lob_chains_reused,
            dml_lob_bytes_reused: dml.lob_bytes_reused,
            dml_lob_chains_externalized: dml.lob_chains_externalized,
            dml_lob_bytes_externalized: dml.lob_bytes_externalized,
            dml_lob_pages_shrunk: dml.lob_pages_shrunk,
            dml_lob_leaks_unrecovered: dml.lob_leaks_unrecovered,
        }
    }

    /// Borrow the live DML counters (engine-internal: used by the
    /// executor's IGNORE path to bump `dml_ignore_warnings` directly).
    pub(crate) fn dml_stats(&self) -> &DmlStats {
        &self.dml_stats
    }

    /// Snapshot the DML counters. Test-only / engine-internal: SHOW
    /// ENGINE MYDB STATUS exposes the same numbers via `storage_status`.
    pub fn dml_stats_snapshot(&self) -> DmlStatsSnapshot {
        DmlStatsSnapshot {
            insert_rows: self.dml_stats.insert_rows.load(Ordering::Relaxed),
            update_rows: self.dml_stats.update_rows.load(Ordering::Relaxed),
            delete_rows: self.dml_stats.delete_rows.load(Ordering::Relaxed),
            ignore_warnings: self.dml_stats.ignore_warnings.load(Ordering::Relaxed),
            rollback_inserted_rows: self
                .dml_stats
                .rollback_inserted_rows
                .load(Ordering::Relaxed),
            lob_leaks_recovered: self.dml_stats.lob_leaks_recovered.load(Ordering::Relaxed),
            upsert_fast_path_hits: self.dml_stats.upsert_fast_path_hits.load(Ordering::Relaxed),
            upsert_batch_attempts: self.dml_stats.upsert_batch_attempts.load(Ordering::Relaxed),
            upsert_batch_rows: self.dml_stats.upsert_batch_rows.load(Ordering::Relaxed),
            upsert_batch_conflict_rows: self
                .dml_stats
                .upsert_batch_conflict_rows
                .load(Ordering::Relaxed),
            upsert_batch_fallback_rows: self
                .dml_stats
                .upsert_batch_fallback_rows
                .load(Ordering::Relaxed),
            upsert_batch_race_rollbacks: self
                .dml_stats
                .upsert_batch_race_rollbacks
                .load(Ordering::Relaxed),
            fk_cycle_short_circuits: self
                .dml_stats
                .fk_cycle_short_circuits
                .load(Ordering::Relaxed),
            load_data_active: self.dml_stats.load_data_active.load(Ordering::Relaxed),
            load_data_rows_loaded: self.dml_stats.load_data_rows_loaded.load(Ordering::Relaxed),
            lob_chains_reused: self.dml_stats.lob_chains_reused.load(Ordering::Relaxed),
            lob_bytes_reused: self.dml_stats.lob_bytes_reused.load(Ordering::Relaxed),
            lob_chains_externalized: self
                .dml_stats
                .lob_chains_externalized
                .load(Ordering::Relaxed),
            lob_bytes_externalized: self
                .dml_stats
                .lob_bytes_externalized
                .load(Ordering::Relaxed),
            lob_pages_shrunk: self.dml_stats.lob_pages_shrunk.load(Ordering::Relaxed),
            lob_leaks_unrecovered: self.dml_stats.lob_leaks_unrecovered.load(Ordering::Relaxed),
        }
    }

    /// Run one purge batch. Returns `true` when the batch filled to
    /// `undo_purge_batch_records` — a signal that MORE eligible undo is
    /// still pending (we are *behind*). `false` means the batch came back
    /// partial/empty: everything currently eligible has been reclaimed
    /// (we are *caught up*).
    ///
    /// The background worker uses the return value to (a) drain a burst by
    /// looping while `true`, and (b) run segment compaction ONLY when
    /// caught up. Compaction holds the global undo write lock for the whole
    /// segment rewrite; running it while behind stalls every foreground
    /// row-write (which appends its undo under that same lock) yet cannot
    /// shrink a backlog that is still growing — so it is gated off the
    /// write-load hot path here.
    pub(crate) fn purge_obsolete_undo(&self) -> bool {
        let maintenance = self.backup_maintenance_gate.read();
        if self.capture.active.load(Ordering::Acquire) {
            return false;
        }
        if self.is_snapshot_read_only() {
            return false;
        }
        let physical_replica = self.physical_replica_mode();
        self.retry_lob_reclaims();
        let creator_ids = self.undo.read().creator_txn_ids();
        let (active_txn_heads, creator_states) = {
            let txns = self.txns.read();
            (
                txns.active_undo_heads_with_txn(),
                txns.undo_creator_states_for(&creator_ids),
            )
        };
        // A creator absent from `creator_states` is purge-safe only when
        // transaction-slot GC has positively classified its id as committed.
        // New transactions can append undo after the state snapshot above and
        // before the undo scan below; treating every absent creator as an old
        // committed transaction races those foreground writers.
        let committed_creator_watermark = self.min_retained_txn_id.load(Ordering::Acquire);
        let runtime = self.runtime_config_read();
        let batch_limit = runtime.undo_purge_batch_records.max(1);
        let retention_ms = runtime.undo_retention.as_millis().min(u128::from(u64::MAX)) as u64;
        let retain_since = current_unix_ms().saturating_sub(retention_ms);
        // No active reader does not mean "all committed undo is obsolete":
        // `visible_scn` can lag a published out-of-order commit. The next
        // statement would take that lower snapshot and need the committed
        // writer's before-image, so treat the frontier as an implicit active
        // snapshot until its commit gap closes.
        let (oldest_registered_snapshot_scn, visible_scn) = self.snapshot_visibility_horizon();
        let oldest_snapshot_scn = Some(
            oldest_registered_snapshot_scn
                // A registered reader at the same frontier can have been
                // pinned immediately before an equal-SCN commit completed
                // publication. Keep equality in that case. Without a real
                // reader, preserve the ordinary `commit_scn > visible_scn`
                // purge boundary so tombstones reclaim promptly.
                .map(|scn| scn.saturating_sub(1))
                .unwrap_or(visible_scn),
        );
        // Two-phase purge: victim selection runs under the SHARED undo lock,
        // so old-version readers remain concurrent; only removal takes the
        // write side. Eligibility is monotone, so selected victims stay valid
        // across lock releases. Every DML publishes its undo under the write
        // side, so the batch is planned and applied in short rounds: one
        // round per batch made each foreground writer that arrived during a
        // pass wait out the whole scan and removal (~1 ms per 4096 records).
        let recent_committed_txns = if retention_ms == 0 {
            // Time travel is disabled, so a commit timestamp at the same
            // clock tick must not accidentally retain its undo payload.
            HashSet::new()
        } else {
            self.temporal_history
                .txn_ids_committed_since_for(retain_since.saturating_mul(1_000), &creator_ids)
        };
        let mut resume_unix_ms = 0;
        let mut purged = 0usize;
        let mut outcome = crate::storage::undo::PurgeOutcome::default();
        while purged < batch_limit {
            let round_limit = (batch_limit - purged).min(UNDO_PURGE_ROUND_RECORDS);
            let victims = self.undo.read().plan_purge_victims_with_recent(
                &active_txn_heads,
                retain_since,
                oldest_snapshot_scn,
                &creator_states,
                committed_creator_watermark,
                &recent_committed_txns,
                &mut resume_unix_ms,
                round_limit,
            );
            if victims.is_empty() {
                break;
            }
            purged += victims.len();
            let mut undo = self.undo.write();
            if physical_replica {
                // Drop only resident directory entries. Source undo pointers
                // are physical identities: never unlink/compact segments or
                // reclaim heap/LOB state independently on a replica.
                undo.evict_replica_records(&victims);
            } else {
                outcome.absorb(undo.apply_purge_victims(&victims, &creator_states));
            }
            drop(undo);
            if victims.len() < round_limit {
                break;
            }
        }
        self.undo.write().record_purge_run();
        // A full batch ⇒ more eligible undo may still be pending ⇒ behind.
        let was_full = purged >= batch_limit;
        if !physical_replica {
            // Free the off-page chains owned by purged versions and the
            // tombstoned slots ready for reclaim after releasing the undo
            // lock (`free_chain` takes the overflow lock, `with_heap_mut`
            // takes the per-heap lock; never nest either under the undo
            // lock). A purged version is the last reference to its chains,
            // so this reclaims the space. Dead segment files are likewise
            // unlinked after the lock drops.
            crate::storage::undo::UndoTablespace::unlink_segments(&outcome.removable_segments);
            for (table_id, refs) in &outcome.freed_chains {
                self.free_external_refs(*table_id, refs);
            }
            self.reclaim_tombstoned_slots(&outcome.reclaim_targets);
        }
        // Compaction acquires its own maintenance guard; do not recursively
        // acquire a read lock while a backup writer could be waiting.
        drop(maintenance);
        // Phase 5 — adaptive compaction, gated to the caught-up path.
        if !was_full && !physical_replica {
            self.maybe_compact_undo_when_caught_up(retention_ms);
        }
        was_full
    }

    /// Segment compaction, only reached from the *caught-up* purge path.
    ///
    /// `compact_segments` holds the global undo write lock for the whole
    /// segment rewrite (~hundreds of ms for a 64 MiB segment), which every
    /// foreground row-write stalls behind (they append their undo under the
    /// same lock). So compaction must fire ONLY when it genuinely pays off,
    /// never on the ordinary draining frontier.
    ///
    /// `apply_purge`+`detach_empty_segments` already reclaim fully-dead
    /// segments for free. The only thing left for compaction is space
    /// stranded behind *pinned* live records — a long-running transaction
    /// or a long read snapshot that stops normal purge from draining old
    /// segments. When purge keeps up (the steady OLTP case) the oldest
    /// undo record hovers around the retention window and segments drain
    /// wholesale, so there is nothing worth compacting.
    ///
    /// `Auto` therefore gates on a cheap stranding signal — the oldest
    /// record being far (>2×) past the retention window — and caps the
    /// rewrite at ONE segment per tick so even then the lock hold stays
    /// bounded (residual fragmentation clears incrementally across ticks).
    /// `On` (explicit opt-in) stays aggressive; `Off` never compacts.
    fn maybe_compact_undo_when_caught_up(&self, retention_ms: u64) {
        match compaction_mode() {
            UndoCompactionMode::Off => {}
            UndoCompactionMode::On => {
                let reclaimable = self
                    .undo
                    .try_read()
                    .map(|undo| undo.reclaimable_bytes())
                    .unwrap_or(0);
                if reclaimable > 0 {
                    let _ = self.compact_undo_segments(30, 4);
                }
            }
            UndoCompactionMode::Auto => {
                // Stranding gate: only when the oldest undo record is
                // >2× retention old (pinned, un-drainable by purge).
                let stranded = {
                    let now = current_unix_ms();
                    let threshold = retention_ms.saturating_mul(2);
                    self.undo
                        .try_read()
                        .and_then(|undo| undo.oldest_created_unix_ms())
                        .is_some_and(|oldest| now.saturating_sub(oldest) > threshold)
                };
                if !stranded {
                    return;
                }
                let has_candidates = self
                    .undo
                    .try_read()
                    .map(|undo| !undo.compaction_candidates(30, 1).is_empty())
                    .unwrap_or(false);
                if has_candidates {
                    let _ = self.compact_undo_segments(30, 1);
                }
            }
        }
    }

    /// P1/10 — drive one compaction pass. Returns the number of bytes
    /// freed (sum of dropped segment sizes). `min_live_ratio_pct`
    /// selects candidates with at most that fraction of their bytes
    /// still pinned by live records; `batch_limit` caps work per pass.
    /// Default policy: 30% live ratio, 4 segments per tick.
    ///
    /// Safety: before the pointer swap, fences any active transaction whose
    /// head will move using the same private append latch as foreground DML.
    /// The fence remains held through the undo rewrite/swap and atomic head
    /// remap, so an append cannot link against the retired pointer.
    pub fn compact_undo_segments(
        &self,
        min_live_ratio_pct: u32,
        batch_limit: usize,
    ) -> Result<u64, QueryError> {
        let _maintenance = self.backup_maintenance_gate.read();
        if self.capture.active.load(Ordering::Acquire) {
            return Ok(0);
        }
        if undo_compaction_build_swap()
            || self.capture.floor().is_some()
            || crate::repl::physical_replication_active()
        {
            self.compact_undo_segments_build_swap(min_live_ratio_pct, batch_limit)
        } else {
            self.compact_undo_segments_in_lock(min_live_ratio_pct, batch_limit)
        }
    }

    /// P1/10 build-then-swap compaction (default). The expensive segment
    /// rewrite — reading before-images, re-encoding, writing + fsyncing
    /// the replacement file — runs with NO undo lock held; only the
    /// pointer/map swap takes the write lock, for microseconds. This is
    /// what keeps the "stranded segment" compaction path from stalling
    /// every foreground row-write (each appends its undo under that same
    /// lock) for the hundreds of ms a 64 MiB rewrite used to take.
    fn compact_undo_segments_build_swap(
        &self,
        min_live_ratio_pct: u32,
        batch_limit: usize,
    ) -> Result<u64, QueryError> {
        // Phase 1 — plan + snapshot under the SHARED lock.
        let plan = {
            let undo = self.undo.read();
            undo.plan_compaction(min_live_ratio_pct, batch_limit)
        };
        let Some(plan) = plan else {
            return Ok(0);
        };
        // Phase 2 — build the replacement segment file(s) OFF the lock.
        let built = crate::storage::undo::UndoManager::build_compaction_segments(&plan)?;
        if built.is_empty() {
            return Ok(0);
        }
        // Snapshot active UndoHead handles before the swap (see below).
        let mut head_handles: Vec<(u64, crate::storage::txn::UndoHead)> = {
            let txns = self.txns_read()?;
            txns.active_undo_heads_with_txn()
                .into_iter()
                .filter(|(_, head)| plan.relocates_ptr(*head))
                .filter_map(|(txn_id, _)| txns.undo_head_handle(txn_id).ok().map(|h| (txn_id, h)))
                .collect()
        };
        // Lock in stable order so two manual/background compaction attempts
        // cannot deadlock while fencing more than one active transaction.
        head_handles.sort_unstable_by_key(|(txn_id, _)| *txn_id);
        // `push_undo_with_flush` takes this same per-transaction latch before
        // it reads prev_undo_ptr and holds it through persistence + head CAS.
        // Fence every head that the plan moves BEFORE the undo-map swap, and
        // retain the guards through remapping. Otherwise an append can read
        // the old head, wait for the swap, persist a child of the now-relocated
        // record, and then fail its CAS when the compactor publishes the new
        // head — the production error observed under concurrent TPC-C.
        let _head_fences: Vec<_> = head_handles
            .iter()
            .map(|(_, head)| head.lock_append())
            .collect();
        // Phase 3 — install atomically under the WRITE lock (short).
        let (bytes_freed, remap) = {
            let mut undo = self.undo_lock()?;
            undo.commit_compaction_logged(plan, built, |records| {
                let lsn = self.log.lock().append_txn_batch(0, records)?;
                crate::wal::LogManager::sync_durable_to(&self.log, lsn)
            })?
        };
        if remap.is_empty() {
            // Drift aborted the swap (a planned record was purged/rolled
            // back mid-build); nothing changed, retry next tick.
            return Ok(0);
        }
        #[cfg(test)]
        undo_compaction_head_fence_test_hook::fire();
        self.remap_active_undo_heads(&head_handles, &remap);
        Ok(bytes_freed)
    }

    /// Legacy in-lock compaction — the whole segment rewrite runs under
    /// the undo write lock. Kept as a differential oracle behind
    /// `MYDB_UNDO_COMPACTION_BUILD_SWAP=0`.
    fn compact_undo_segments_in_lock(
        &self,
        min_live_ratio_pct: u32,
        batch_limit: usize,
    ) -> Result<u64, QueryError> {
        // Candidate selection runs under the SHARED lock — read-only.
        let candidates = {
            let undo = self.undo.read();
            undo.compaction_candidates(min_live_ratio_pct, batch_limit)
        };
        if candidates.is_empty() {
            return Ok(0);
        }
        // Capture every active transaction's UndoHead handle BEFORE we
        // mutate so we can atomically remap heads once the rewrite is
        // done. `UndoHead` is `Arc<AtomicU64>`, so the handle stays
        // valid even after the TM lock drops.
        let mut head_handles: Vec<(u64, crate::storage::txn::UndoHead)> = {
            let txns = self.txns_read()?;
            txns.active_undo_heads_with_txn()
                .into_iter()
                .filter(|(_, head)| {
                    candidates.contains(&crate::storage::undo::undo_ptr_segment(*head))
                })
                .filter_map(|(txn_id, _)| txns.undo_head_handle(txn_id).ok().map(|h| (txn_id, h)))
                .collect()
        };
        head_handles.sort_unstable_by_key(|(txn_id, _)| *txn_id);
        let _head_fences: Vec<_> = head_handles
            .iter()
            .map(|(_, head)| head.lock_append())
            .collect();
        let (bytes_freed, remap) = {
            let mut undo = self.undo_lock()?;
            undo.compact_segments(&candidates)?
        };
        if remap.is_empty() {
            return Ok(0);
        }
        #[cfg(test)]
        undo_compaction_head_fence_test_hook::fire();
        self.remap_active_undo_heads(&head_handles, &remap);
        Ok(bytes_freed)
    }

    /// Remap active transaction undo heads after a compaction moved records.
    /// The caller holds each selected head's append latch across both the undo
    /// swap and this CAS, so a foreground append cannot publish between them.
    /// CAS remains defensive against transaction completion/savepoint restore.
    fn remap_active_undo_heads(
        &self,
        head_handles: &[(u64, crate::storage::txn::UndoHead)],
        remap: &[(u64, u64)],
    ) {
        use std::collections::HashMap;
        let map: HashMap<u64, u64> = remap.iter().copied().collect();
        for (_txn_id, head) in head_handles {
            if let Some(current) = head.get() {
                if let Some(new) = map.get(&current) {
                    head.compare_exchange(Some(current), Some(*new));
                }
            }
        }
    }

    /// rowid-6bytes M2: after `purge_except_chains` drops the Delete
    /// undo records, flip the matching heap slots `ROW_FLAG_DELETED →
    /// ROW_FLAG_FREE` so the slot ids become reusable. Each transition
    /// emits a `WalPayload::SlotReclaim` record (idempotent on
    /// recovery). Errors during reclaim never abort the purge run —
    /// the heap stays correct (slot just stays in DELETED state) and
    /// the failed `(table_id, row_id)` lands in the per-database
    /// quarantine for retry on the next call. After
    /// `SLOT_RECLAIM_MAX_ATTEMPTS` the pair is dropped and counted as
    /// a permanent failure (table dropped, persistent heap corruption,
    /// etc.) so the quarantine stays bounded.
    fn reclaim_tombstoned_slots(&self, targets: &[(u64, RowId)]) {
        let mut user_targets = Vec::with_capacity(targets.len());
        for (table_id, row_id) in targets {
            if crate::storage::data_dictionary::dd_table_schema(*table_id).is_some() {
                // DD vacuum has its own observability surface; do not pollute
                // user heap slot-reclaim counters or snapshot tests.
                let _ = self.with_heap_mut(*table_id, |heap| heap.reclaim_slot_logged(*row_id));
            } else {
                user_targets.push((*table_id, *row_id));
            }
        }
        self.reclaim_tombstoned_slots_inner(&user_targets, |table_id, row_id| {
            self.with_heap_mut(table_id, |heap| heap.reclaim_slot_logged(row_id))
        });
    }

    /// Inner state machine — split out so tests can inject a synthetic
    /// `attempt` closure. The shape mirrors `reclaim_tombstoned_slots`'s
    /// three phases (process new targets → drain retry queue → commit
    /// fresh failures). Production callers use the wrapper above.
    fn reclaim_tombstoned_slots_inner<F>(&self, targets: &[(u64, RowId)], mut attempt: F)
    where
        F: FnMut(u64, RowId) -> Result<bool, QueryError>,
    {
        let quarantine_initially_empty = self.slot_reclaim_quarantine.lock().is_empty();
        if targets.is_empty() && quarantine_initially_empty {
            return;
        }
        let mut reclaimed: u64 = 0;
        let mut errors: u64 = 0;
        let mut new_failures: Vec<(u64, RowId)> = Vec::new();
        // Phase 1: process newly produced targets. Failures bubble into
        // the quarantine; successes (and idempotent no-ops) drop any
        // matching prior entry.
        for (table_id, row_id) in targets {
            match attempt(*table_id, *row_id) {
                Ok(true) => {
                    reclaimed = reclaimed.saturating_add(1);
                    self.slot_reclaim_drop_from_quarantine((*table_id, *row_id));
                }
                Ok(false) => {
                    self.slot_reclaim_drop_from_quarantine((*table_id, *row_id));
                }
                Err(_) => {
                    errors = errors.saturating_add(1);
                    new_failures.push((*table_id, *row_id));
                }
            }
        }
        // Phase 2: drain a bounded slice of existing quarantine entries
        // and retry each. Success removes the entry; another failure
        // bumps the attempt counter; entries past
        // `SLOT_RECLAIM_MAX_ATTEMPTS` are counted as permanent failures
        // and evicted so they can never starve the queue.
        let drained: Vec<((u64, RowId), u8)> = {
            let q = self.slot_reclaim_quarantine.lock();
            q.iter()
                .take(SLOT_RECLAIM_QUARANTINE_DRAIN_PER_RUN)
                .map(|(k, v)| (*k, *v))
                .collect()
        };
        let mut permanent_failures: u64 = 0;
        for ((table_id, row_id), prev_attempts) in drained {
            match attempt(table_id, row_id) {
                Ok(true) => {
                    reclaimed = reclaimed.saturating_add(1);
                    self.slot_reclaim_drop_from_quarantine((table_id, row_id));
                }
                Ok(false) => {
                    self.slot_reclaim_drop_from_quarantine((table_id, row_id));
                }
                Err(_) => {
                    let bumped = prev_attempts.saturating_add(1);
                    if bumped >= SLOT_RECLAIM_MAX_ATTEMPTS {
                        permanent_failures = permanent_failures.saturating_add(1);
                        self.slot_reclaim_drop_from_quarantine((table_id, row_id));
                    } else {
                        let mut q = self.slot_reclaim_quarantine.lock();
                        q.insert((table_id, row_id), bumped);
                        let size = q.len() as u64;
                        drop(q);
                        self.slot_reclaim_quarantine_size_atomic
                            .store(size, Ordering::Relaxed);
                    }
                }
            }
        }
        // Phase 3: commit the new-failure cohort. Honour
        // `SLOT_RECLAIM_QUARANTINE_CAP` — once the queue is full, drop
        // fresh losers rather than starving entries already grinding
        // through their attempt budget (those would otherwise rotate
        // out of the bounded `take` window indefinitely).
        if !new_failures.is_empty() {
            let mut q = self.slot_reclaim_quarantine.lock();
            for key in new_failures {
                if q.len() >= SLOT_RECLAIM_QUARANTINE_CAP && !q.contains_key(&key) {
                    permanent_failures = permanent_failures.saturating_add(1);
                    continue;
                }
                let entry = q.entry(key).or_insert(0);
                *entry = entry.saturating_add(1);
            }
            let size = q.len() as u64;
            drop(q);
            self.slot_reclaim_quarantine_size_atomic
                .store(size, Ordering::Relaxed);
        }
        if reclaimed > 0 {
            self.slot_reclaim_total_atomic
                .fetch_add(reclaimed, Ordering::Relaxed);
        }
        // Counted runs include retry-only runs, preserving the
        // existing meaning of "a run that touched the reclaim path".
        self.slot_reclaim_runs_atomic
            .fetch_add(1, Ordering::Relaxed);
        if errors > 0 {
            self.slot_reclaim_errors_atomic
                .fetch_add(errors, Ordering::Relaxed);
        }
        if permanent_failures > 0 {
            self.slot_reclaim_permanent_failures_atomic
                .fetch_add(permanent_failures, Ordering::Relaxed);
        }
    }

    fn slot_reclaim_drop_from_quarantine(&self, key: (u64, RowId)) {
        let mut q = self.slot_reclaim_quarantine.lock();
        if q.remove(&key).is_some() {
            let size = q.len() as u64;
            drop(q);
            self.slot_reclaim_quarantine_size_atomic
                .store(size, Ordering::Relaxed);
        }
    }

    fn purge_obsolete_index_repairs(&self) {
        // Read-mostly fast path: take a read lock first to check if any
        // repair entries exist at all. The vast majority of commits in an
        // OLTP workload have no index repairs to purge; iterating all
        // active-snapshot shards and re-acquiring the txns write lock for
        // every such commit is pure overhead. Only fall through to the
        // write path when there is actual work to do.
        let has_live_repairs = self.index_repair_state.has_writers();
        let (active_oldest, visible_scn) = self.snapshot_visibility_horizon();
        let retention_ms = self.undo_retention_ms();
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;
        let (history_oldest, cutoff_unix_us) = if retention_ms == 0 {
            (None, None)
        } else {
            let cutoff = now_us.saturating_sub(retention_ms.saturating_mul(1_000));
            (
                self.temporal_history
                    .scn_as_of(cutoff, self.visible_frontier.load(Ordering::Acquire))
                    .or_else(|| self.temporal_history.baseline().map(|b| b.snapshot_scn)),
                Some(cutoff),
            )
        };
        let oldest_snapshot_scn = match (active_oldest, history_oldest) {
            (Some(active), Some(history)) => Some(active.min(history)),
            (Some(active), None) => Some(active),
            (None, Some(history)) => Some(history),
            (None, None) => None,
        };
        // The next reader still needs versions behind an unpublished commit gap.
        let oldest_snapshot_scn =
            Some(oldest_snapshot_scn.map_or(visible_scn, |scn| scn.min(visible_scn)));
        if has_live_repairs {
            // Decide under the shared directory lock, then clear under the
            // registry latch alone: an exclusive directory lock here would
            // stall every concurrent BEGIN/COMMIT for the whole pass.
            let writers = self
                .txns
                .read()
                .obsolete_index_repair_writers(oldest_snapshot_scn);
            self.index_repair_state.purge_finished(
                &writers,
                &self.index_repair_epochs,
                &self.index_repair_version,
            );
        }
        // Ledger pruning advances its availability baseline only when there
        // are no active readers. Existing TxnView values carry the baseline
        // captured at their start and therefore cannot observe a newer one.
        let safe_first_tracked_txn_id = {
            let txns = self.txns.read();
            txns.slots
                .values()
                .filter(|slot| slot.status.is_in_flight())
                .map(|slot| slot.txn_id)
                .min()
                .unwrap_or_else(|| self.next_txn_id_atomic.load(Ordering::Acquire))
        };
        let ledger_baseline = if active_oldest.is_none() && retention_ms != 0 {
            history_oldest
                .zip(cutoff_unix_us)
                .map(
                    |(snapshot_scn, available_unix_us)| super::temporal::TemporalBaseline {
                        first_tracked_txn_id: safe_first_tracked_txn_id,
                        snapshot_scn,
                        available_unix_us,
                    },
                )
        } else {
            None
        };
        let clear_ledger = active_oldest.is_none() && retention_ms == 0;
        // The sidecar is an accelerator: compaction failure degrades future
        // history plans to the heap oracle and must never fail a commit.
        let _ = self.temporal_history.compact_history_index(
            oldest_snapshot_scn,
            ledger_baseline,
            clear_ledger,
        );
    }

    pub(crate) fn oldest_active_snapshot_scn(&self) -> Option<u64> {
        let mut min: Option<u64> = None;
        for shard in self.active_snapshots.iter() {
            let snapshots = shard.lock().unwrap_or_else(|poison| poison.into_inner());
            for snapshot in snapshots.values() {
                min = Some(match min {
                    Some(current) => current.min(snapshot.snapshot_scn),
                    None => snapshot.snapshot_scn,
                });
            }
        }
        min
    }

    /// Sample the oldest registered reader and the published visibility
    /// frontier while holding every snapshot shard. A fresh statement locks
    /// its chosen shard while reading `visible_scn` and registering itself, so
    /// it must fall wholly before or wholly after this horizon sample.
    fn snapshot_visibility_horizon(&self) -> (Option<u64>, u64) {
        let mut locked_shards = Vec::with_capacity(self.active_snapshots.len());
        for shard in self.active_snapshots.iter() {
            locked_shards.push(shard.lock().unwrap_or_else(|poison| poison.into_inner()));
        }
        let oldest_snapshot_scn = locked_shards
            .iter()
            .flat_map(|snapshots| snapshots.values())
            .map(|snapshot| snapshot.snapshot_scn)
            .min();
        let visible_scn = self.visible_scn();
        (oldest_snapshot_scn, visible_scn)
    }

    /// P2/16 — count of active read snapshots across all shards. Used
    /// by the PROMOTE TO MASTER guard. Existing readers are refused because
    /// promotion rolls back abandoned source transactions and establishes a
    /// new role boundary; no pre-promote view may cross that transition.
    pub fn active_snapshot_count(&self) -> usize {
        self.active_snapshots
            .iter()
            .map(|shard| {
                shard
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .len()
            })
            .sum()
    }

    pub fn rollback_txn(&self, txn_id: u64) -> Result<(), QueryError> {
        let result = crate::engine::executor::mem_budget::with_critical_memory_reservations(|| {
            self.rollback_txn_inner(txn_id)
        });
        match result {
            Ok(()) => Ok(()),
            Err(original) => {
                // Once physical compensation has started it is impossible to
                // prove which heap/index/LOB mutations were restored. Marking
                // the slot RolledBack and releasing locks would allow slot GC
                // to reinterpret a surviving current version as committed.
                // Fail-stop instead; restart recovery replays the durable undo
                // chain while the in-memory slot and locks remain intact.
                self.quarantine_mvcc(format!(
                    "rollback of transaction {txn_id} did not complete: {}",
                    original.message()
                ));
                Err(original)
            }
        }
    }

    fn rollback_txn_inner(&self, txn_id: u64) -> Result<(), QueryError> {
        // Handler_rollback: single funnel for every rollback path.
        crate::engine::status_counters::record_handler_rollback();
        self.discard_pending_live_row_deltas(txn_id);
        // Zone Map page redo is conservative and redo-only: rollback leaves
        // its widened bounds in place, with no private WAL buffer to discard.
        // Txn-buffering: append any parked forward records (and release their
        // frames' deferred pins) BEFORE rolling back, so the transaction is
        // exactly equivalent to an eagerly-logged one from here on — the
        // existing AbortTxn / undo (CLR) / EndTxn path is unchanged.
        if txn_wal_buffering_active(&self.log, txn_id) {
            self.flush_txn_wal_buffer(txn_id, None)?;
            end_txn_wal_buffering(&self.log, txn_id);
        }
        crate::binlog::rollback_txn(txn_id);
        self.discard_pending_vector_deletes(txn_id)?;
        let (has_undo, prepared) = {
            let txns = self.txns_read()?;
            (
                txns.undo_head(txn_id)?.is_some(),
                txns.slot(txn_id)
                    .is_some_and(|slot| slot.status == TxnStatus::Prepared),
            )
        };
        if !has_undo {
            let (wal_logged, prepared) = self
                .txns_read()?
                .slot(txn_id)
                .map(|slot| (slot.wal_logged, slot.status == TxnStatus::Prepared))
                .unwrap_or((false, false));
            if wal_logged || prepared {
                // A replicated BEGIN remains write-capable until its terminal
                // record arrives, even when this source transaction did no DML.
                let end_lsn = self.append_wal(txn_id, WalPayload::EndTxn)?;
                // PREPARE is durable even without undo. Persist its rollback
                // decision before acknowledging or releasing locks, otherwise
                // crash recovery can resurrect an already completed branch.
                if prepared {
                    match self.config.durability {
                        DurabilityMode::Strict => self.sync_commit_lsn(end_lsn)?,
                        DurabilityMode::Relaxed => self.flush_commit_lsn(end_lsn)?,
                        DurabilityMode::Memory => {}
                    }
                }
            }
            let mut txns = self.txns_write()?;
            txns.rollback(txn_id)?;
            self.slot_status_cache.mark_rolled_back(txn_id);
            self.active_write_txns_atomic
                .store(txns.active_write_count, Ordering::Release);
            drop(txns);
            self.table_locks_sharded.release_txn(txn_id);
            let lock_release = self.lock_manager().unlock_txn(txn_id);
            let mut released = self.wake_released_lock_waiters(lock_release);
            if row_lock_sharding_enabled() {
                released.extend(self.row_locks_sharded.release_txn(txn_id));
            }
            self.notify_lock_waiters_for_rows(released);
            end_txn_redo_policy(&self.log, txn_id);
            return Ok(());
        }
        self.append_wal(txn_id, WalPayload::AbortTxn)?;
        let touched_tables = self.rollback_txn_changes(txn_id)?;
        let mut txns = self.txns_write()?;
        txns.rollback(txn_id)?;
        self.slot_status_cache.mark_rolled_back(txn_id);
        self.active_write_txns_atomic
            .store(txns.active_write_count, Ordering::Release);
        drop(txns);
        self.table_locks_sharded.release_txn(txn_id);
        let lock_release = self.lock_manager().unlock_txn(txn_id);
        let mut released = self.wake_released_lock_waiters(lock_release);
        if row_lock_sharding_enabled() {
            released.extend(self.row_locks_sharded.release_txn(txn_id));
        }
        self.notify_lock_waiters_for_rows(released);
        self.flush_touched_indexes(&touched_tables)?;
        let end_lsn = self.append_wal(txn_id, WalPayload::EndTxn)?;
        match self.config.durability {
            DurabilityMode::Strict => self.sync_commit_lsn(end_lsn)?,
            DurabilityMode::Relaxed if prepared => self.flush_commit_lsn(end_lsn)?,
            _ => {}
        }
        end_txn_redo_policy(&self.log, txn_id);
        Ok(())
    }

    /// Capture before executing the compiled exact locking read. It cannot
    /// invoke routines, mutate rows, or acquire unique-key locks. Avoid the
    /// unrelated transaction/global unique-lock latches on successful reads.
    pub(crate) fn locking_read_lock_watermark(&self, txn_id: u64) -> LockingReadLockWatermark {
        if row_lock_sharding_enabled() {
            LockingReadLockWatermark::Sharded(self.row_locks_sharded.row_log_len(txn_id))
        } else {
            LockingReadLockWatermark::Legacy(self.lock_manager_read().lock_log_len(txn_id))
        }
    }

    /// The read left undo, binlog, index repairs and live-row deltas unchanged.
    /// Snapshot them now for the existing compensation path, replacing only
    /// the lock boundary captured before the read acquired its candidate.
    pub(crate) fn materialize_locking_read_savepoint(
        &self,
        txn_id: u64,
        watermark: LockingReadLockWatermark,
    ) -> Result<StatementSavepoint, QueryError> {
        let mut savepoint = self.read_only_statement_savepoint(txn_id)?;
        match watermark {
            LockingReadLockWatermark::Sharded(len) => savepoint.row_log_len = len,
            LockingReadLockWatermark::Legacy(len) => savepoint.lock_log_len = len,
        }
        Ok(savepoint)
    }

    /// The compiled locking point read cannot append WAL; retain lock and undo watermarks.
    pub(crate) fn read_only_statement_savepoint(
        &self,
        txn_id: u64,
    ) -> Result<StatementSavepoint, QueryError> {
        // Most statements start in a transaction without unique/range
        // reservations; prove that lock-free instead of taking the latch.
        let lock_log_len = if self.lock_log_presence.may_have_log(txn_id) {
            self.lock_manager_read().lock_log_len(txn_id)
        } else {
            0
        };
        let row_log_len = if row_lock_sharding_enabled() {
            self.row_locks_sharded.row_log_len(txn_id)
        } else {
            0
        };
        let head = self.undo_head_for_statement(txn_id)?;
        let mut savepoint = StatementSavepoint {
            wal_boundary: None,
            undo_head: head.get(),
            lock_log_len,
            row_log_len,
            index_repair_log_len: head.index_repair_log_len(),
            binlog_len: crate::binlog::txn_len(txn_id),
            live_row_deltas: HashMap::new(),
        };
        savepoint.live_row_deltas = self.pending_live_row_deltas_snapshot(txn_id);
        Ok(savepoint)
    }

    pub fn statement_savepoint(&self, txn_id: u64) -> Result<StatementSavepoint, QueryError> {
        let mut savepoint = self.read_only_statement_savepoint(txn_id)?;
        if self.config.durability != DurabilityMode::Memory {
            let key = txn_wal_buffer_key(&self.log, txn_id);
            let mut shard = txn_wal_buffer_shard(key).lock();
            if let Some(buf) = shard.get_mut(&key) {
                // Statements of one transaction are serialized by the caller.
                // Every completed eager append has published its allocated LSN;
                // every subsequent eager append will reserve a greater one.
                // Thus the global allocation watermark separates this txn's
                // pre/post-savepoint eager records without taking the WAL
                // mutex. Other txns may advance it past our own last record:
                // flush must clamp prefix_lsn to this lower bound to represent
                // an empty skip interval, while resolving last_lsn to an actual
                // record in this transaction's chain as before.
                let eager_lsn = self.log_state_atomics.next_lsn().saturating_sub(1);
                let cursor = Arc::new(crate::storage::txn::WalSavepointBoundary {
                    eager_lsn,
                    prefix_lsn: AtomicU64::new(eager_lsn),
                    last_lsn: AtomicU64::new(u64::MAX),
                });
                // Finished statement boundaries are normally at the tail;
                // discard them amortized O(1), without scanning named saves.
                while buf
                    .boundaries
                    .last()
                    .is_some_and(|(_, boundary)| boundary.strong_count() == 0)
                {
                    buf.boundaries.pop();
                }
                buf.boundaries
                    .push((buf.entries.len(), Arc::downgrade(&cursor)));
                savepoint.wal_boundary = Some(cursor);
            } else {
                drop(shard);
                // Without a buffer there is no later boundary-resolution pass:
                // recovery needs the exact transaction backlink, not a global
                // allocation watermark that may belong to another transaction.
                let eager_lsn = self.log.lock().last_txn_lsn(txn_id);
                savepoint.wal_boundary =
                    Some(Arc::new(crate::storage::txn::WalSavepointBoundary {
                        eager_lsn,
                        prefix_lsn: AtomicU64::new(eager_lsn),
                        last_lsn: AtomicU64::new(eager_lsn),
                    }));
            }
        }
        Ok(savepoint)
    }

    /// M1.2 - Create a named user savepoint within an active transaction.
    pub fn create_named_savepoint(&self, txn_id: u64, name: String) -> Result<(), QueryError> {
        let savepoint = self.statement_savepoint(txn_id)?;
        self.txns_write()?
            .create_named_savepoint(txn_id, name, savepoint)
    }

    /// Preserve WAL batching for a savepoint with no data changes.
    fn rollback_savepoint_changes(
        &self,
        txn_id: u64,
        savepoint: &StatementSavepoint,
    ) -> Result<(), QueryError> {
        let head = self.txns_read()?.undo_head(txn_id)?;
        let unchanged = head == savepoint.undo_head
            || match (head, savepoint.undo_head) {
                (Some(head), Some(stop)) => {
                    let undo = self.undo.read();
                    undo.canonical_ptr(head) == undo.canonical_ptr(stop)
                }
                _ => false,
            };
        if unchanged {
            return Ok(());
        }
        self.flush_txn_wal_buffer(txn_id, None)?;
        let boundary = savepoint
            .wal_boundary
            .as_ref()
            .map_or(0, |cursor| cursor.last_lsn.load(Ordering::Acquire));
        if boundary == u64::MAX {
            return Err(undo_corruption(0, "unresolved savepoint WAL boundary"));
        }
        let skip = savepoint.wal_boundary.as_ref().map_or((0, 0), |cursor| {
            (cursor.eager_lsn, cursor.prefix_lsn.load(Ordering::Acquire))
        });
        self.rollback_txn_changes_until(txn_id, savepoint.undo_head, boundary, skip)?;
        Ok(())
    }

    /// M1.2 - Rollback to a named savepoint, undoing changes after it.
    pub fn rollback_to_named_savepoint(&self, txn_id: u64, name: &str) -> Result<(), QueryError> {
        let savepoint = self.txns_read()?.lookup_named_savepoint(txn_id, name)?;
        let result = (|| {
            self.rollback_savepoint_changes(txn_id, &savepoint)?;
            self.restore_pending_live_row_deltas(txn_id, &savepoint.live_row_deltas);
            crate::binlog::truncate_txn(txn_id, savepoint.binlog_len);
            let mut txns = self.txns_write()?;
            txns.restore_statement_savepoint(txn_id, &savepoint)?;
            txns.truncate_named_savepoints_after(txn_id, name);
            self.active_write_txns_atomic
                .store(txns.active_write_count, Ordering::Release);
            drop(txns);
            self.prune_pending_vector_deletes_to_undo_chain(txn_id)?;
            // MySQL/InnoDB semantics (M4): `ROLLBACK TO SAVEPOINT` undoes data
            // changes and deletes later savepoints, but does NOT release the row
            // locks acquired after the savepoint — they are held until the
            // transaction ends. (Rows *inserted* after the savepoint are undone by
            // `rollback_txn_changes_until` above, which releases their implicit
            // locks along with the row versions, matching InnoDB.) The legacy
            // early-release path is kept behind `MYDB_SAVEPOINT_RELEASE_LOCKS` as a
            // differential oracle.
            if savepoint_release_locks_on_rollback() {
                let mut released = self
                    .lock_manager()
                    .restore_savepoint_locks(txn_id, savepoint.lock_log_len);
                if row_lock_sharding_enabled() {
                    released.extend(
                        self.row_locks_sharded
                            .restore_savepoint(txn_id, savepoint.row_log_len),
                    );
                }
                self.notify_lock_waiters_for_rows(released);
            }
            Ok(())
        })();
        result.map_err(|err: QueryError| {
            self.quarantine_mvcc(format!(
                "savepoint compensation failed for transaction {txn_id}: {}",
                err.message()
            ));
            err
        })
    }

    /// Release only the named savepoint.
    pub fn release_named_savepoint(&self, txn_id: u64, name: &str) -> Result<(), QueryError> {
        self.txns_write()?.release_named_savepoint(txn_id, name)
    }

    pub fn rollback_to_statement_savepoint(
        &self,
        txn_id: u64,
        savepoint: StatementSavepoint,
    ) -> Result<(), QueryError> {
        self.rollback_to_statement_savepoint_inner(txn_id, savepoint, true)
    }

    pub fn rollback_to_statement_savepoint_preserving_wait(
        &self,
        txn_id: u64,
        savepoint: StatementSavepoint,
    ) -> Result<(), QueryError> {
        self.rollback_to_statement_savepoint_inner(txn_id, savepoint, false)
    }

    fn rollback_to_statement_savepoint_inner(
        &self,
        txn_id: u64,
        savepoint: StatementSavepoint,
        clear_wait: bool,
    ) -> Result<(), QueryError> {
        self.txns_read()?.undo_head(txn_id)?;
        let result = (|| {
            self.rollback_savepoint_changes(txn_id, &savepoint)?;
            self.restore_pending_live_row_deltas(txn_id, &savepoint.live_row_deltas);
            crate::binlog::truncate_txn(txn_id, savepoint.binlog_len);
            let lock_log_len = savepoint.lock_log_len;
            let metadata_unchanged = self
                .txns_read()?
                .statement_savepoint_matches(txn_id, &savepoint)?;
            if !metadata_unchanged {
                let mut txns = self.txns_write()?;
                txns.restore_statement_savepoint(txn_id, &savepoint)?;
                self.active_write_txns_atomic
                    .store(txns.active_write_count, Ordering::Release);
            }
            self.prune_pending_vector_deletes_to_undo_chain(txn_id)?;
            // Lock order txns → lock_manager: drop `txns`, then release post-savepoint
            // locks and (optionally) clear this txn's own wait under the lock latch.
            let mut lm = self.lock_manager();
            // Unique/table locks do not appear in `released`, but releasing
            // them still needs the existing non-row waiter notification.
            let mut lock_state_changed = lm.lock_log_len(txn_id) > lock_log_len;
            let mut released = lm.restore_savepoint_locks(txn_id, lock_log_len);
            if clear_wait {
                lock_state_changed |= lm.clear_wait_with_change(txn_id);
            }
            drop(lm);
            if row_lock_sharding_enabled() {
                if clear_wait {
                    lock_state_changed |= self.row_locks_sharded.clear_wait_with_change(txn_id);
                }
                released.extend(
                    self.row_locks_sharded
                        .restore_savepoint(txn_id, savepoint.row_log_len),
                );
            }
            if released.is_empty() {
                // An inline LockWouldBlock probe commonly changed neither
                // held locks nor wait queues. Broadcasting that no-op wakes
                // unrelated row waiters only to make them park again. Keep
                // notifications for real releases and cancelled FIFO entries.
                if clear_wait && lock_state_changed {
                    self.notify_lock_waiters();
                }
            } else {
                self.notify_lock_waiters_for_rows(released);
            }
            Ok(())
        })();
        result.map_err(|err: QueryError| {
            self.quarantine_mvcc(format!(
                "statement compensation failed for transaction {txn_id}: {}",
                err.message()
            ));
            err
        })
    }

    /// Retain only deferred vector deletes whose undo records remain reachable
    /// from the transaction's current head after a savepoint rollback.
    fn prune_pending_vector_deletes_to_undo_chain(&self, txn_id: u64) -> Result<(), QueryError> {
        if !self
            .pending_vector_deletes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&txn_id)
        {
            return Ok(());
        }
        let mut reachable = std::collections::HashSet::new();
        let mut ptr = self.txns_read()?.undo_head(txn_id)?;
        while let Some(id) = ptr {
            // Deferred mutations keep their original WAL identity while
            // active heads may have moved through several compactions.
            let identity = self.undo.read().wal_identity(id);
            if !reachable.insert(identity) {
                return Err(undo_corruption(
                    id,
                    "cycle in deferred-index transaction chain",
                ));
            }
            let record = self
                .load_undo_owned(id)?
                .ok_or_else(|| undo_corruption(id, "missing deferred-index transaction record"))?;
            ptr = record.prev_undo_ptr;
        }
        let mut pending = self
            .pending_vector_deletes
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut discarded = 0u64;
        if let Some(entries) = pending.get_mut(&txn_id) {
            let before = entries.len();
            entries.retain(|entry| reachable.contains(&entry.undo_id));
            discarded = before.saturating_sub(entries.len()) as u64;
            if entries.is_empty() {
                pending.remove(&txn_id);
            }
        }
        drop(pending);
        if discarded > 0 {
            self.hnsw_tombstones_rolled_back
                .fetch_add(discarded, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn clear_txn_wait(&self, txn_id: u64) -> Result<(), QueryError> {
        if row_lock_sharding_enabled() {
            self.row_locks_sharded.clear_wait(txn_id);
        }
        self.lock_manager().clear_wait(txn_id);
        self.notify_lock_waiters();
        Ok(())
    }

    pub fn is_txn_active(&self, txn_id: u64) -> bool {
        // PREPARED is still "in flight" from the storage layer's
        // perspective — it owns undo, locks, and a slot. Callers (in
        // particular the XA commit-prepared / rollback-prepared path
        // and `commit_session_txn` after a re-attach) rely on this
        // returning true for either state.
        if let Some((status, _)) = self.slot_status_cache.lookup(txn_id) {
            return status.is_in_flight();
        }
        // A miss covers the begin window before the mirror is published and
        // GC'd slots; only the authoritative directory can tell them apart.
        self.txns
            .read()
            .status(txn_id)
            .is_some_and(|s| s.is_in_flight())
    }

    pub fn has_active_write_txns(&self) -> bool {
        self.active_write_txns_atomic.load(Ordering::Acquire) > 0
            || self.sequences.active_writes.load(Ordering::SeqCst) > 0
    }

    pub(crate) fn backup_maintenance_capture_guard(
        &self,
    ) -> Result<
        (
            std::sync::MutexGuard<'_, ()>,
            parking_lot::RwLockWriteGuard<'_, ()>,
        ),
        QueryError,
    > {
        // Follow checkpoint -> maintenance ordering: checkpoints themselves
        // purge undo before flushing ledger/catalog/page state.
        let checkpoint = self.checkpoint_running.lock().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "checkpoint lock poisoned",
            )
        })?;
        Ok((checkpoint, self.backup_maintenance_gate.write()))
    }

    pub fn has_active_backup_conflicts(&self) -> bool {
        self.has_active_write_txns()
            || self.foreground_bulk_ddl_active.load(Ordering::Acquire) != 0
            || self.analyze_scan_active.load(Ordering::Acquire) != 0
    }

    /// Prepared XA branches are stable, WAL-pinned state. Their decisions
    /// take a separate admission lease before modifying the prepared map.
    pub(crate) fn has_active_archive_backup_conflicts(&self) -> bool {
        let txns = self.txns.read();
        let writers = txns.active_write_count;
        let prepared = self.prepared_xa.read().len();
        writers > prepared
            || !self.vector_commits_publishing.lock().is_empty()
            || self.sequences.active_writes.load(Ordering::SeqCst) != 0
            || self.foreground_bulk_ddl_active.load(Ordering::Acquire) != 0
            || self.analyze_scan_active.load(Ordering::Acquire) != 0
    }

    // ───────────────── XA / distributed transactions ─────────────────

    /// Reserve the XID from START until successful completion, including IDLE.
    /// This latch is never held across transaction I/O.
    pub fn reserve_xa(&self, xid: &crate::xa::Xid, txn_id: u64) -> Result<(), QueryError> {
        let mut reservations = self.xa_reservations.lock();
        if let Some(owner) = reservations.get(xid) {
            if *owner != txn_id {
                return Err(error(
                    crate::error::catalog::ER_XAER_DUPID,
                    "XAER_DUPID: an XA branch with the same XID already exists",
                ));
            }
        }
        reservations.insert(xid.clone(), txn_id);
        Ok(())
    }

    pub fn release_xa(&self, xid: &crate::xa::Xid, txn_id: u64) {
        let mut reservations = self.xa_reservations.lock();
        if reservations.get(xid) == Some(&txn_id) {
            reservations.remove(xid);
            drop(reservations);
            if crate::binlog::enabled() {
                crate::binlog::prepared::cleanup(&self.data_dir, txn_id);
            }
        }
    }

    pub fn prepare_xa(
        &self,
        txn_id: u64,
        owner_id: u64,
        xid: crate::xa::Xid,
    ) -> Result<u64, QueryError> {
        let _admission = crate::storage::capture::Admission::for_active_writer(self, txn_id)?;
        if txn_reduced_redo_active(&self.log, txn_id) {
            return Err(error(
                crate::error::catalog::ER_INNODB_REDO_DISABLED,
                "XA PREPARE requires full redo; rollback this branch and enable mydb_redo_enabled",
            ));
        }
        if self.prepared_xa.read().contains_key(&xid) {
            return Err(error(
                crate::error::catalog::ER_XAER_DUPID,
                "XAER_DUPID: an XA branch with the same XID is already PREPARED",
            ));
        }
        self.reserve_xa(&xid, txn_id)?;
        // Pin WAL before appending PrepareTxn, including empty/read-only
        // branches. Checkpoint must not collapse while PREPARE is in progress.
        {
            let mut txns = self.txns_write()?;
            txns.mark_writer(txn_id)?;
            txns.mark_wal_logged(txn_id)?;
        }
        // Snapshot the held locks under the txns read lock so we can
        // ship them in the PrepareTxn WAL record without holding the
        // lock during the fsync. The txn is ACTIVE up to and through
        // the WAL append; only the txns_write step below flips it to
        // PREPARED, so MVCC visibility / conflict detection stays
        // consistent.
        let mut locks = collect_prepared_locks(&self.lock_manager(), txn_id)?;
        // P3 — in sharded mode the row locks live in `row_locks_sharded`, not
        // `lock_log`; append them so XA recovery reinstates them.
        if row_lock_sharding_enabled() {
            for (table_id, row_id, mode) in self.row_locks_sharded.prepared_rows_for_txn(txn_id) {
                locks.push(crate::wal::PreparedLockEntry::Row {
                    table_id,
                    page_id: row_id.page_id,
                    slot_id: row_id.slot_id,
                    mode: u8::from(mode == LockMode::Exclusive),
                });
            }
        }
        // P2 — table locks live in the sharded store now, not in `lock_log`;
        // append them so the PrepareTxn WAL record reinstates the prepared
        // branch's IX/S/X table locks on crash recovery.
        for (table_id, mode) in self.table_locks_sharded.tables_for_txn(txn_id) {
            locks.push(crate::wal::PreparedLockEntry::Table {
                table_id,
                mode: match mode {
                    crate::storage::txn::TableLockMode::IntentShared => 0,
                    crate::storage::txn::TableLockMode::IntentExclusive => 1,
                    crate::storage::txn::TableLockMode::Shared => 2,
                    crate::storage::txn::TableLockMode::ShareIntentExclusive => 3,
                    crate::storage::txn::TableLockMode::Exclusive => 4,
                },
            });
        }
        // Drain the ordinary transaction WAL buffer before PrepareTxn.
        // Zone Map FPI records were already appended synchronously by DML;
        // allocating PrepareTxn below places them in the same durable prefix.
        if txn_wal_buffering_active(&self.log, txn_id) {
            self.flush_txn_wal_buffer(txn_id, None)?;
            end_txn_wal_buffering(&self.log, txn_id);
        }
        // The drained WAL prefix includes every UndoRecord, just as for an
        // ordinary durable commit. PREPARE's barrier below protects both undo
        // and row redo. Checkpoints synchronize the undo files before pruning
        // that prefix; forcing every shard and rewriting the undo manifest
        // here only serializes unrelated writers behind global filesystem I/O.
        let binlog = crate::binlog::prepared::prepare(txn_id, |path, bytes| {
            // PREPARED pins this WAL prefix until completion. Live commits
            // still own their pending operations; only recovery needs to
            // materialize intent files. Avoid a redundant filesystem write
            // and allocation for every prepared chunk on the source.
            self.append_wal(txn_id, WalPayload::FileCreate { path, after: bytes })?;
            Ok(())
        })?;
        let mdl_locks = self.mdl.prepared_entries_for_owner(owner_id);
        let prepare_scn = self.next_scn_atomic.fetch_add(1, Ordering::AcqRel);
        // P0-1: a PREPARED txn is invisible by slot status, so `prepare_scn`
        // never gates visibility — but it IS drawn from the allocation counter,
        // so it must be marked done on the frontier (at promote below, or via
        // this guard's Drop if a `?` bails first) or the frontier stalls.
        let mut frontier_guard = ScnFrontierGuard::new(self, prepare_scn);
        self.log_pending_vector_recovery(txn_id)?;
        let prepare_lsn = self.append_wal(
            txn_id,
            WalPayload::PrepareTxn {
                xid: xid.clone(),
                prepare_scn,
                locks,
                mdl_locks,
                binlog: binlog.clone(),
            },
        )?;
        match self.config.durability {
            DurabilityMode::Strict => self.sync_commit_lsn(prepare_lsn)?,
            DurabilityMode::Relaxed => self.flush_commit_lsn(prepare_lsn)?,
            DurabilityMode::Memory => {}
        }
        // Promote the in-memory slot. Doing this AFTER the fsync means a
        // crash between WAL append and slot promotion is recoverable —
        // recovery sees the PrepareTxn record and reconstructs the
        // PREPARED slot.
        let frontier = {
            let mut txns = self.txns_write()?;
            txns.prepare(txn_id, xid.clone())?;
            // Resolve prepare_scn on the frontier in the same critical section
            // as the PREPARED promotion.
            txns.mark_frontier_done(prepare_scn)
        };
        self.publish_visible_frontier(frontier);
        frontier_guard.disarm();
        self.slot_status_cache.mark_prepared(txn_id);
        let started_unix_ms = crate::storage::naming::current_unix_ms();
        self.prepared_xa.write().insert(
            xid.clone(),
            crate::xa::PreparedXa {
                xid,
                txn_id,
                prepare_scn,
                started_unix_ms,
                binlog,
                recovered: false,
            },
        );
        self.xa_prepared_active.fetch_add(1, Ordering::Relaxed);
        Ok(prepare_scn)
    }

    pub fn commit_prepared(&self, xid: &crate::xa::Xid) -> Result<u64, QueryError> {
        self.ensure_writable()?;
        let _completion = self.recovered_vector_completion.lock();
        let _admission = crate::storage::capture::Admission::control(self)?;
        let prepared = {
            let mut map = self.prepared_xa.write();
            map.remove(xid).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_XAER_NOTA,
                    "XAER_NOTA: no such PREPARED XA branch",
                )
            })?
        };
        // Reuse the standard commit path — slot is PREPARED, which
        // `in_flight_slot_mut` accepts, so undo cleanup / lock release /
        // group-commit flow all run exactly like a regular commit.
        let txn_id = prepared.txn_id;
        let result: Result<u64, QueryError> = (|| {
            let intents = self.prepare_vector_completion(&prepared)?;
            if let Some(intent) = &prepared.binlog {
                crate::binlog::prepared::restore(&self.data_dir, txn_id, intent)?;
            }
            let scn = self.commit_txn(txn_id)?;
            self.finish_vector_completion(&intents)?;
            Ok(scn)
        })();
        let commit_scn = match result {
            Ok(scn) => scn,
            Err(err) => {
                if self.txns.read().status(txn_id) == Some(TxnStatus::Prepared) {
                    self.prepared_xa.write().insert(xid.clone(), prepared);
                } else {
                    self.quarantine_mvcc(format!(
                        "XA completion failed after publication: {}",
                        err.message()
                    ));
                }
                return Err(err);
            }
        };
        self.release_xa(xid, txn_id);
        self.release_prepared_mdl(txn_id);
        self.xa_prepared_active.fetch_sub(1, Ordering::Relaxed);
        self.xa_commits_total.fetch_add(1, Ordering::Relaxed);
        Ok(commit_scn)
    }

    pub fn rollback_prepared(&self, xid: &crate::xa::Xid) -> Result<(), QueryError> {
        self.ensure_writable()?;
        let _completion = self.recovered_vector_completion.lock();
        let _admission = crate::storage::capture::Admission::control(self)?;
        let prepared = {
            let mut map = self.prepared_xa.write();
            map.remove(xid).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_XAER_NOTA,
                    "XAER_NOTA: no such PREPARED XA branch",
                )
            })?
        };
        let txn_id = prepared.txn_id;
        let result = (|| {
            let intents = self.prepare_vector_completion(&prepared)?;
            self.rollback_txn(txn_id)?;
            self.finish_vector_completion(&intents)
        })();
        if let Err(err) = result {
            if self.txns.read().status(txn_id) == Some(TxnStatus::Prepared) {
                self.prepared_xa.write().insert(xid.clone(), prepared);
            } else {
                self.quarantine_mvcc(format!("XA rollback completion failed: {}", err.message()));
            }
            return Err(err);
        }
        self.release_xa(xid, txn_id);
        self.release_prepared_mdl(txn_id);
        self.xa_prepared_active.fetch_sub(1, Ordering::Relaxed);
        self.xa_rollbacks_total.fetch_add(1, Ordering::Relaxed);
        if prepared.binlog.is_some() && !crate::binlog::enabled() {
            crate::binlog::prepared::cleanup(&self.data_dir, txn_id);
        }
        Ok(())
    }

    pub fn prepared_xa_contains(&self, xid: &crate::xa::Xid) -> bool {
        self.prepared_xa.read().contains_key(xid)
    }

    pub fn list_prepared_xa(&self) -> Vec<crate::xa::PreparedXa> {
        let mut all: Vec<_> = self.prepared_xa.read().values().cloned().collect();
        all.sort_by_key(|p| p.prepare_scn);
        all
    }

    /// Counters for `SHOW ENGINE MYDB STATUS`.
    pub fn xa_status_counters(&self) -> (usize, u64, u64) {
        (
            self.xa_prepared_active.load(Ordering::Relaxed),
            self.xa_commits_total.load(Ordering::Relaxed),
            self.xa_rollbacks_total.load(Ordering::Relaxed),
        )
    }

    /// rowid-6bytes M2 — slot-reclaim counters surfaced via
    /// `SHOW ENGINE MYDB STATUS`. Returns
    /// `(slot_reclaim_total, slot_reclaim_runs, slot_reclaim_errors)`.
    pub fn slot_reclaim_status_counters(&self) -> (u64, u64, u64) {
        (
            self.slot_reclaim_total_atomic.load(Ordering::Relaxed),
            self.slot_reclaim_runs_atomic.load(Ordering::Relaxed),
            self.slot_reclaim_errors_atomic.load(Ordering::Relaxed),
        )
    }

    /// P0/4 — extended slot-reclaim counters: `(quarantine_size,
    /// permanent_failures)`. Surfaced via `SHOW ENGINE MYDB STATUS`
    /// next to the legacy triple so operators can tell "retry queue
    /// has work to do" from "retries have been giving up".
    pub fn slot_reclaim_quarantine_counters(&self) -> (u64, u64) {
        (
            self.slot_reclaim_quarantine_size_atomic
                .load(Ordering::Relaxed),
            self.slot_reclaim_permanent_failures_atomic
                .load(Ordering::Relaxed),
        )
    }

    /// Test hook: drive the slot-reclaim path directly with a synthetic
    /// target list. Each target is fed to `outcome`, whose return value
    /// stands in for `reclaim_slot_logged`. The drained quarantine
    /// entries are fed the same closure — keep that in mind when
    /// asserting state across multiple calls.
    #[cfg(test)]
    pub(crate) fn trigger_slot_reclaim_for_test<F>(&self, targets: &[(u64, RowId)], outcome: F)
    where
        F: FnMut(u64, RowId) -> Result<bool, QueryError>,
    {
        self.reclaim_tombstoned_slots_inner(targets, outcome);
    }

    /// Test hook: snapshot the quarantine map.
    #[cfg(test)]
    pub(crate) fn slot_reclaim_quarantine_snapshot_for_test(&self) -> Vec<((u64, RowId), u8)> {
        self.slot_reclaim_quarantine
            .lock()
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect()
    }

    /// Engine-startup hook: after recovery has reconstructed every
    /// `TxnSlot`, register PREPARED slots in `prepared_xa` so subsequent
    /// `XA RECOVER` / `XA COMMIT` / `XA ROLLBACK` find them.
    pub(crate) fn rebuild_prepared_xa_from_slots(
        &self,
        binlogs: &HashMap<u64, crate::wal::PreparedBinlogRef>,
        prepare_scns: &HashMap<u64, u64>,
    ) {
        let txns = self.txns.read();
        let mut map = self.prepared_xa.write();
        map.clear();
        let mut count = 0usize;
        for slot in txns.slots.values() {
            if slot.status != TxnStatus::Prepared {
                continue;
            }
            let Some(xid) = slot.xa_xid.clone() else {
                continue;
            };
            self.xa_reservations.lock().insert(xid.clone(), slot.txn_id);
            map.insert(
                xid.clone(),
                crate::xa::PreparedXa {
                    xid,
                    txn_id: slot.txn_id,
                    prepare_scn: prepare_scns
                        .get(&slot.txn_id)
                        .copied()
                        .unwrap_or_else(|| slot.start_scn.max(slot.commit_scn.unwrap_or(0))),
                    started_unix_ms: slot.started_unix_ms,
                    binlog: binlogs.get(&slot.txn_id).cloned(),
                    recovered: true,
                },
            );
            count += 1;
        }
        self.xa_prepared_active.store(count, Ordering::Relaxed);
    }

    fn recover_binlog_intent_files(&self) -> Result<(), QueryError> {
        if self.is_snapshot_read_only() {
            return Ok(());
        }
        // Replicas can retain ACTIVE source transactions whose PREPARE has
        // not arrived yet. Restore cleanup ownership for partial intents too.
        let live = self
            .txns
            .read()
            .slots
            .values()
            .filter(|slot| slot.status.is_in_flight())
            .map(|slot| slot.txn_id)
            .collect();
        let retained = crate::binlog::prepared::cleanup_finished(&self.data_dir, &live)?;
        if self.physical_replica_mode() {
            let mut deltas = self.applier_dd_dirty_txns.lock();
            for txn_id in retained {
                deltas.entry(txn_id).or_default().has_binlog_intent = true;
            }
        }
        Ok(())
    }

    pub(crate) fn rebuild_prepared_mdl_from_wal(
        &self,
        prepared_mdl: &std::collections::HashMap<u64, Vec<crate::wal::PreparedMdlEntry>>,
    ) {
        let txns = self.txns.read();
        for slot in txns.slots.values() {
            if slot.status != TxnStatus::Prepared {
                continue;
            }
            let Some(entries) = prepared_mdl.get(&slot.txn_id) else {
                continue;
            };
            self.mdl.install_prepared(slot.txn_id, entries);
        }
    }
}

/// Differential-oracle switch for `ROLLBACK TO SAVEPOINT` lock handling.
/// Default `false` = MySQL/InnoDB semantics (keep row locks acquired after the
/// savepoint until the transaction ends). Set `MYDB_SAVEPOINT_RELEASE_LOCKS=1`
/// to restore the legacy early-release behaviour for A/B comparison.
fn savepoint_release_locks_on_rollback() -> bool {
    match std::env::var("MYDB_SAVEPOINT_RELEASE_LOCKS") {
        Ok(v) => {
            let t = v.trim().to_ascii_lowercase();
            !(t.is_empty() || t == "0" || t == "off" || t == "false" || t == "no")
        }
        Err(_) => false,
    }
}

/// Serialize a transaction's currently-held locks into the compact
/// `PreparedLockEntry` form used by the `PrepareTxn` WAL record. Reads
/// the lock_log under the caller's lock-manager latch.
fn collect_prepared_locks(
    lock_manager: &crate::storage::txn::LockManager,
    txn_id: u64,
) -> Result<Vec<crate::wal::PreparedLockEntry>, QueryError> {
    use crate::storage::txn::{HeldLock, LockMode, TableLockMode};
    use crate::wal::PreparedLockEntry;
    let Some(log) = lock_manager.lock_log.get(&txn_id) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(log.len());
    let mut rows = HashSet::new();
    for held in log {
        match held {
            HeldLock::Row(table_id, row_id) | HeldLock::RowMode(table_id, row_id, _) => {
                rows.insert((*table_id, *row_id));
            }
            HeldLock::Rows(table_id, row_ids) => {
                rows.extend(row_ids.iter().map(|row| (*table_id, *row)));
            }
            HeldLock::Unique(lock) => {
                let key_bytes = serde_json::to_vec(lock.key()).map_err(|err| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!("cannot encode prepared unique lock: {err}"),
                    )
                })?;
                out.push(PreparedLockEntry::UniqueKey {
                    table_id: lock.table_id(),
                    index_name: lock.index_name().to_string(),
                    key_bytes,
                });
            }
            HeldLock::UniqueRange(range) => {
                for key in range.keys() {
                    let key_bytes = serde_json::to_vec(&key).map_err(|err| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            format!("cannot encode prepared unique range: {err}"),
                        )
                    })?;
                    out.push(PreparedLockEntry::UniqueKey {
                        table_id: range.table_id,
                        index_name: range.index_name.to_string(),
                        key_bytes,
                    });
                }
            }
            HeldLock::Table(table_id, mode) => out.push(PreparedLockEntry::Table {
                table_id: *table_id,
                mode: match mode {
                    TableLockMode::IntentShared => 0,
                    TableLockMode::IntentExclusive => 1,
                    TableLockMode::Shared => 2,
                    TableLockMode::ShareIntentExclusive => 3,
                    TableLockMode::Exclusive => 4,
                },
            }),
        }
    }
    for (table_id, row_id) in rows {
        if let Some(state) = lock_manager
            .locks
            .get(&(table_id, row_id))
            .filter(|s| s.holds(txn_id))
        {
            out.push(PreparedLockEntry::Row {
                table_id,
                page_id: row_id.page_id,
                slot_id: row_id.slot_id,
                mode: u8::from(state.mode == LockMode::Exclusive),
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod transaction_audit_tests {
    use super::*;

    #[test]
    fn txn_audit_relaxed_flush_cannot_satisfy_strict_barrier() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let db = Database::open_with_durability(dir.path(), DurabilityMode::Relaxed).unwrap();
        db.buffer_pool.shutdown_flusher();
        let txn = db.begin_txn().unwrap();
        let lsn = db.log.lock().append(txn.txn_id, WalPayload::EndTxn).unwrap();
        db.flush_commit_lsn(lsn).unwrap();
        assert!(db.log.lock().durable_lsn() < lsn);
        db.sync_commit_lsn(lsn).unwrap();
        assert!(
            db.log.lock().durable_lsn() >= lsn,
            "strict barrier must fsync a previously flushed LSN"
        );
    }
}
