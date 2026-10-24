use crate::util::fast_hash::fnv1a_64;
use smallvec::SmallVec;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use crate::types::QueryError;
use crate::wal::PreparedMdlEntry;

use super::naming::normalize_name;
use crate::util::coarse_clock::coarse_unix_ms;

const PREPARED_OWNER_TAG: u64 = 1_u64 << 63;
// Connection owners fit in u32; prepared-XA owners occupy the high half.
// Internal cursor/maintenance leases use the disjoint interval between them.
static NEXT_INTERNAL_OWNER: AtomicU64 = AtomicU64::new(1u64 << 32);
pub(crate) fn allocate_internal_owner() -> Result<u64, QueryError> {
    NEXT_INTERNAL_OWNER
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |owner| {
            owner
                .checked_add(1)
                .filter(|next| *next < PREPARED_OWNER_TAG)
        })
        .map_err(|_| {
            super::error(
                crate::error::catalog::ER_OUT_OF_RESOURCES,
                "metadata lock internal owner ids exhausted",
            )
        })
}

type MdlObjectMap = hashbrown::HashMap<MdlObjectKey, ObjectLockState, ahash::RandomState>;

pub fn prepared_owner_id(txn_id: u64) -> u64 {
    PREPARED_OWNER_TAG | txn_id
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum MdlObjectType {
    Global,
    Schema,
    Table,
    Function,
    Procedure,
    Trigger,
    Event,
    Commit,
    Account,
}

impl MdlObjectType {
    pub fn as_str(self) -> &'static str {
        match self {
            MdlObjectType::Global => "GLOBAL",
            MdlObjectType::Schema => "SCHEMA",
            MdlObjectType::Table => "TABLE",
            MdlObjectType::Function => "FUNCTION",
            MdlObjectType::Procedure => "PROCEDURE",
            MdlObjectType::Trigger => "TRIGGER",
            MdlObjectType::Event => "EVENT",
            MdlObjectType::Commit => "COMMIT",
            MdlObjectType::Account => "ACL CACHE",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MdlObjectKey {
    pub object_type: MdlObjectType,
    pub schema: Option<String>,
    pub name: Option<String>,
}

impl MdlObjectKey {
    pub fn global() -> Self {
        Self {
            object_type: MdlObjectType::Global,
            schema: None,
            name: None,
        }
    }

    pub fn table(schema: &str, name: &str) -> Self {
        Self {
            object_type: MdlObjectType::Table,
            schema: Some(normalize_name(schema)),
            name: Some(normalize_name(name)),
        }
    }

    pub fn schema(schema: &str) -> Self {
        Self {
            object_type: MdlObjectType::Schema,
            schema: Some(normalize_name(schema)),
            name: None,
        }
    }

    pub fn named(object_type: MdlObjectType, schema: &str, name: &str) -> Self {
        Self {
            object_type,
            schema: Some(normalize_name(schema)),
            name: Some(normalize_name(name)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MdlLockType {
    SharedRead,
    SharedWrite,
    SharedNoWrite,
    SharedUpgradable,
    SharedNoReadWrite,
    Exclusive,
    IntentionExclusive,
}

impl MdlLockType {
    pub fn as_str(self) -> &'static str {
        match self {
            MdlLockType::SharedRead => "SHARED_READ",
            MdlLockType::SharedWrite => "SHARED_WRITE",
            MdlLockType::SharedNoWrite => "SHARED_NO_WRITE",
            MdlLockType::SharedUpgradable => "SHARED_UPGRADABLE",
            MdlLockType::SharedNoReadWrite => "SHARED_NO_READ_WRITE",
            MdlLockType::Exclusive => "EXCLUSIVE",
            MdlLockType::IntentionExclusive => "INTENTION_EXCLUSIVE",
        }
    }

    fn rank(self) -> u8 {
        match self {
            MdlLockType::IntentionExclusive => 1,
            MdlLockType::SharedRead => 2,
            MdlLockType::SharedWrite => 3,
            MdlLockType::SharedNoWrite => 4,
            MdlLockType::SharedUpgradable => 5,
            MdlLockType::SharedNoReadWrite => 6,
            MdlLockType::Exclusive => 7,
        }
    }

    /// Lock modes form a partial order. In particular SNW and SU (and
    /// SNW and SW) cannot replace one another: their conflict sets differ.
    /// Keep incomparable tickets rather than silently dropping protection.
    fn covers(self, other: Self) -> bool {
        use MdlLockType::*;
        [
            SharedRead,
            SharedWrite,
            SharedNoWrite,
            SharedUpgradable,
            SharedNoReadWrite,
            Exclusive,
            IntentionExclusive,
        ]
        .into_iter()
        .all(|probe| compatible(other, probe) || !compatible(self, probe))
    }

    fn is_writer_like(self) -> bool {
        matches!(
            self,
            MdlLockType::SharedWrite
                | MdlLockType::SharedNoWrite
                | MdlLockType::SharedUpgradable
                | MdlLockType::SharedNoReadWrite
                | MdlLockType::Exclusive
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MdlDuration {
    Statement,
    Transaction,
    Explicit,
}

impl MdlDuration {
    pub fn as_str(self) -> &'static str {
        match self {
            MdlDuration::Statement => "STATEMENT",
            MdlDuration::Transaction => "TRANSACTION",
            MdlDuration::Explicit => "EXPLICIT",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MdlStatus {
    Granted,
    Pending,
}

impl MdlStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            MdlStatus::Granted => "GRANTED",
            MdlStatus::Pending => "PENDING",
        }
    }
}

#[derive(Debug, Clone)]
pub struct MdlRequest {
    pub key: MdlObjectKey,
    pub lock_type: MdlLockType,
}

#[derive(Debug, Clone)]
pub struct MetadataLockRow {
    pub object_type: &'static str,
    pub object_schema: Option<String>,
    pub object_name: Option<String>,
    pub column_name: Option<String>,
    pub object_instance: u64,
    pub lock_type: &'static str,
    pub lock_duration: &'static str,
    pub lock_status: &'static str,
    pub source: String,
    pub owner_thread_id: Option<u64>,
    pub owner_event_id: Option<u64>,
}

#[derive(Debug, Clone)]
struct MdlTicket {
    id: u64,
    owner_id: u64,
    owner_thread_id: Option<u64>,
    key: MdlObjectKey,
    lock_type: MdlLockType,
    duration: MdlDuration,
    status: MdlStatus,
    source: String,
    granted_unix_ms: u64,
}

#[derive(Debug, Default)]
struct ObjectLockState {
    granted: Vec<MdlTicket>,
    waiting: Vec<MdlTicket>,
}

#[derive(Debug, Default)]
struct MdlState {
    next_ticket_id: u64,
    objects: MdlObjectMap,
}

/// Per-object counters for compatible statement-duration shared grants.
///
/// All participation is SeqCst: a shared acquire publishes its counter before
/// checking that mode's ticket/provisional blockers. An incompatible ticket
/// publishes intent before checking the corresponding counters and retains
/// its blocker mask while granted or queued. One side always observes the
/// other, including SHARED_NO_WRITE, which excludes writes but permits reads.
#[derive(Debug, Default)]
pub struct MdlFastSlot {
    /// In-flight statement-duration SHARED_READ grants taken on the
    /// lock-free path. Never represented as tickets.
    readers: AtomicI64,
    writers: AtomicI64,
    intentions: AtomicI64,
    /// Ticket-derived bit mask for the compatible shared modes, recomputed
    /// under the state mutex after every relevant ticket mutation.
    ticket_blockers: AtomicU64,
    /// Per-mode in-flight incompatible acquisition intents. They close the
    /// window before an incompatible ticket reaches the table.
    provisional_blockers: [AtomicU64; 3],
}

const FAST_MDL_TYPES: [MdlLockType; 3] = [
    MdlLockType::SharedRead,
    MdlLockType::SharedWrite,
    MdlLockType::IntentionExclusive,
];

fn fast_type_index(lock_type: MdlLockType) -> Option<usize> {
    FAST_MDL_TYPES.iter().position(|&kind| kind == lock_type)
}

fn fast_blocker_mask(lock_type: MdlLockType) -> u64 {
    FAST_MDL_TYPES
        .iter()
        .enumerate()
        .fold(0, |mask, (index, &kind)| {
            mask | (u64::from(!compatible(lock_type, kind)) << index)
        })
}

impl MdlFastSlot {
    fn count(&self, kind: MdlLockType) -> &AtomicI64 {
        match kind {
            MdlLockType::SharedRead => &self.readers,
            MdlLockType::SharedWrite => &self.writers,
            MdlLockType::IntentionExclusive => &self.intentions,
            _ => unreachable!("only compatible shared modes use fast grants"),
        }
    }

    fn blocked(&self, kind: MdlLockType) -> bool {
        let index = fast_type_index(kind).expect("fast lock type");
        self.ticket_blockers.load(Ordering::SeqCst) & (1 << index) != 0
            || self.provisional_blockers[index].load(Ordering::SeqCst) != 0
    }

    pub fn has_blockers(&self) -> bool {
        self.ticket_blockers.load(Ordering::SeqCst) != 0
            || self
                .provisional_blockers
                .iter()
                .any(|count| count.load(Ordering::SeqCst) != 0)
    }

    fn compatible_with(&self, requested: MdlLockType) -> bool {
        FAST_MDL_TYPES.iter().all(|&kind| {
            compatible(kind, requested) || self.count(kind).load(Ordering::SeqCst) == 0
        })
    }
}

/// RAII batch of compatible shared metadata grants.
/// Dropping it releases every object count even on error/unwind paths;
/// [`Database::release_fast_mdl`] wraps the drop with a waiter wake when
/// a blocker arrived while the statement ran.
#[derive(Debug)]
pub struct MdlFastGrant {
    slots: SmallVec<[(Arc<MdlFastSlot>, MdlLockType); 2]>,
}

impl MdlFastGrant {
    pub fn slot(&self) -> Arc<MdlFastSlot> {
        Arc::clone(&self.slots[0].0)
    }

    /// Release every object before checking whether a waiting incompatible
    /// ticket needs a wake. Emptying the grant prevents its Drop from releasing
    /// the same counters twice.
    pub fn release(mut self) -> bool {
        let slots = std::mem::take(&mut self.slots);
        for (slot, kind) in &slots {
            slot.count(*kind).fetch_sub(1, Ordering::SeqCst);
        }
        slots.iter().any(|(slot, kind)| slot.blocked(*kind))
    }
}

impl Drop for MdlFastGrant {
    fn drop(&mut self) {
        for (slot, kind) in &self.slots {
            slot.count(*kind).fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// `MYDB_MDL_FAST=0` disables the lock-free reader path (differential
/// oracle: every statement then takes the ticketed slow path).
fn mdl_fast_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("MYDB_MDL_FAST")
            .map(|v| v != "0")
            .unwrap_or(true)
    })
}

fn blocks_fast_grants(lock_type: MdlLockType) -> bool {
    fast_blocker_mask(lock_type) != 0
}

fn count_fast_blockers(object: &ObjectLockState) -> u64 {
    object
        .granted
        .iter()
        .chain(object.waiting.iter())
        .filter(|ticket| blocks_fast_grants(ticket.lock_type))
        .count() as u64
}

type FastSlotMap = hashbrown::HashMap<MdlObjectKey, Arc<MdlFastSlot>, ahash::RandomState>;

#[derive(Debug, Default)]
pub struct MdlManager {
    identity: Arc<()>,
    state: parking_lot::Mutex<MdlState>,
    /// Persistent per-object fast-path slots. Entries are never removed
    /// (bounded by the number of distinct MDL objects ever touched), so
    /// a slot Arc cached by a reader can never go stale against a
    /// recreated object entry in `state.objects`.
    fast: parking_lot::RwLock<FastSlotMap>,
}

#[derive(Debug)]
struct CachedFastSlot {
    key: MdlObjectKey,
    slot: Arc<MdlFastSlot>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

/// A bounded cache of slot identities, never of granted locks. The weak
/// manager identity prevents reuse across database instances or recreation.
#[derive(Debug, Default)]
pub(crate) struct MdlFastCache {
    identity: std::sync::Weak<()>,
    slots: SmallVec<[CachedFastSlot; 8]>,
}

impl Clone for MdlFastCache {
    fn clone(&self) -> Self {
        // Cloning session execution state must not duplicate uncharged key
        // allocations or carry optional slot derivatives to worker clones.
        Self::default()
    }
}

impl MdlFastCache {
    fn slot(&mut self, manager: &MdlManager, key: &MdlObjectKey) -> Arc<MdlFastSlot> {
        if !std::ptr::eq(self.identity.as_ptr(), Arc::as_ptr(&manager.identity)) {
            self.slots.clear();
            self.identity = Arc::downgrade(&manager.identity);
        }
        if let Some(cached) = self.slots.iter().find(|cached| &cached.key == key) {
            return Arc::clone(&cached.slot);
        }
        let slot = manager.fast_slot(key);
        if self.slots.len() < 8 {
            let bytes = key.schema.as_ref().map_or(0, String::len)
                + key.name.as_ref().map_or(0, String::len);
            if let Some(charge) =
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                    bytes,
                )
            {
                self.slots.push(CachedFastSlot {
                    key: key.clone(),
                    slot: Arc::clone(&slot),
                    _charge: charge,
                });
            }
        }
        slot
    }
}

impl MdlManager {
    /// Lock-free acquisition of a single statement-duration SHARED_READ
    /// metadata lock. Returns `None` (caller falls back to the ticketed
    /// slow path) whenever the request shape is not the hot one or any
    /// read-blocking ticket/intent exists on the object.
    #[cfg(test)]
    pub fn try_acquire_statement_read_fast(&self, requests: &[MdlRequest]) -> Option<MdlFastGrant> {
        if requests.len() != 1 || requests[0].lock_type != MdlLockType::SharedRead {
            return None;
        }
        self.try_acquire_statement_fast(requests)
    }

    /// Compatible shared grants publish before checking ticketed/provisional
    /// blockers. Any rejected object rolls the entire batch back by RAII;
    /// callers then use the sorted ticket path without retaining partial grants.
    pub fn try_acquire_statement_fast(&self, requests: &[MdlRequest]) -> Option<MdlFastGrant> {
        self.try_acquire_statement_fast_with(requests, |key| self.fast_slot(key))
    }

    pub(crate) fn try_acquire_statement_fast_cached(
        &self,
        requests: &[MdlRequest],
        cache: &mut MdlFastCache,
    ) -> Option<MdlFastGrant> {
        self.try_acquire_statement_fast_with(requests, |key| cache.slot(self, key))
    }

    fn try_acquire_statement_fast_with(
        &self,
        requests: &[MdlRequest],
        mut slot_for: impl FnMut(&MdlObjectKey) -> Arc<MdlFastSlot>,
    ) -> Option<MdlFastGrant> {
        if !mdl_fast_enabled()
            || requests.is_empty()
            || requests
                .iter()
                .any(|request| fast_type_index(request.lock_type).is_none())
        {
            return None;
        }
        let mut grant = MdlFastGrant {
            slots: SmallVec::new(),
        };
        for request in requests {
            let slot = slot_for(&request.key);
            slot.count(request.lock_type).fetch_add(1, Ordering::SeqCst);
            grant.slots.push((slot, request.lock_type));
            if grant
                .slots
                .last()
                .expect("published grant")
                .0
                .blocked(request.lock_type)
            {
                return None;
            }
        }
        Some(grant)
    }

    fn fast_slot(&self, key: &MdlObjectKey) -> Arc<MdlFastSlot> {
        if let Some(slot) = self.fast.read().get(key) {
            return Arc::clone(slot);
        }
        Arc::clone(self.fast.write().entry(key.clone()).or_default())
    }

    fn fast_slot_if_present(&self, key: &MdlObjectKey) -> Option<Arc<MdlFastSlot>> {
        self.fast.read().get(key).cloned()
    }

    /// Republish the ticket-derived blocker count for `key` after a
    /// ticket mutation. Must run under the `state` mutex (callers all
    /// hold it). Creates the slot when blockers exist so a first-time
    /// fast reader can observe them; skips slot creation otherwise.
    fn refresh_fast_blockers(&self, key: &MdlObjectKey, object: Option<&ObjectLockState>) {
        let mask = object
            .map(|object| {
                object
                    .granted
                    .iter()
                    .chain(&object.waiting)
                    .fold(0, |mask, ticket| mask | fast_blocker_mask(ticket.lock_type))
            })
            .unwrap_or(0);
        if mask == 0 {
            if let Some(slot) = self.fast_slot_if_present(key) {
                slot.ticket_blockers.store(0, Ordering::SeqCst);
            }
        } else {
            self.fast_slot(key)
                .ticket_blockers
                .store(mask, Ordering::SeqCst);
        }
    }

    /// Recompute blocker counts for every object whose ticket set was
    /// touched by a bulk release. `keys` may contain objects that were
    /// pruned from the table — those republish as zero.
    fn refresh_fast_blockers_for_keys(&self, state: &MdlState, keys: &[MdlObjectKey]) {
        for key in keys {
            self.refresh_fast_blockers(key, state.objects.get(key));
        }
    }

    pub fn acquire_many(
        &self,
        owner_id: u64,
        owner_thread_id: Option<u64>,
        duration: MdlDuration,
        mut requests: Vec<MdlRequest>,
        source: &str,
    ) -> Result<(), QueryError> {
        if requests.is_empty() {
            return Ok(());
        }
        requests.sort_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then(a.lock_type.rank().cmp(&b.lock_type.rank()))
        });
        requests.dedup_by(|a, b| a.key == b.key && a.lock_type == b.lock_type);

        let mut state = self.state.lock();
        let mut created_grants = Vec::new();
        let mut promoted_grants = Vec::new();
        // Keys of read-blocking tickets granted in this batch: the abort
        // path removes those tickets again, so their objects' fast-path
        // blocker counts must be republished afterwards. Tracked lazily —
        // read-blocking grants are DDL-rare.
        let mut created_blocker_keys: Vec<MdlObjectKey> = Vec::new();
        for request in requests {
            let blocker_key = blocks_fast_grants(request.lock_type).then(|| request.key.clone());
            match self.acquire_one(
                &mut state,
                owner_id,
                owner_thread_id,
                duration,
                request,
                source,
            ) {
                AcquireOne::Granted(id) => {
                    created_grants.push(id);
                    if let Some(key) = blocker_key {
                        created_blocker_keys.push(key);
                    }
                }
                AcquireOne::Promoted(previous) => {
                    promoted_grants.push(previous);
                    if let Some(key) = blocker_key {
                        created_blocker_keys.push(key);
                    }
                }
                AcquireOne::AlreadyHeld => {}
                AcquireOne::Pending => {
                    remove_granted_tickets(&mut state, owner_id, &created_grants);
                    for previous in promoted_grants.into_iter().rev() {
                        restore_granted_ticket(&mut state, *previous);
                    }
                    self.refresh_fast_blockers_for_keys(&state, &created_blocker_keys);
                    return self.pending_or_deadlock(&mut state, owner_id);
                }
            }
        }
        Ok(())
    }

    pub fn acquire_cached(
        &self,
        owner_id: u64,
        owner_thread_id: Option<u64>,
        duration: MdlDuration,
        requests: &[MdlRequest],
        source: &str,
    ) -> Result<(), QueryError> {
        if requests.len() != 1 {
            return self.acquire_many(
                owner_id,
                owner_thread_id,
                duration,
                requests.to_vec(),
                source,
            );
        }
        let mut state = self.state.lock();
        match self.acquire_one(
            &mut state,
            owner_id,
            owner_thread_id,
            duration,
            requests[0].clone(),
            source,
        ) {
            AcquireOne::Granted(_) | AcquireOne::AlreadyHeld | AcquireOne::Promoted(_) => Ok(()),
            AcquireOne::Pending => self.pending_or_deadlock(&mut state, owner_id),
        }
    }

    pub fn release_explicit_table_locks(&self, owner_id: u64) -> bool {
        let mut state = self.state.lock();
        let (released, changed) = release_where(&mut state, |ticket| {
            ticket.owner_id == owner_id
                && ticket.duration == MdlDuration::Explicit
                && ticket.key.object_type != MdlObjectType::Global
        });
        self.refresh_fast_blockers_for_keys(&state, &changed);
        released
    }

    fn pending_or_deadlock(&self, state: &mut MdlState, owner_id: u64) -> Result<(), QueryError> {
        // Build edges only on contention, under the same mutex as ticket
        // mutation. Match can_grant, including writer preference and holder
        // reentry, so compatible queued readers do not create false cycles.
        let mut edges = std::collections::HashMap::<u64, Vec<u64>>::new();
        for object in state.objects.values() {
            for (pos, waiter) in object.waiting.iter().enumerate() {
                let blockers = edges.entry(waiter.owner_id).or_default();
                blockers.extend(
                    object
                        .granted
                        .iter()
                        .filter(|held| {
                            held.owner_id != waiter.owner_id
                                && !compatible(held.lock_type, waiter.lock_type)
                        })
                        .map(|held| held.owner_id),
                );
                blockers.extend(
                    object.waiting[..pos]
                        .iter()
                        .filter(|earlier| {
                            waiting_ticket_blocks_owner(
                                object,
                                earlier,
                                waiter.owner_id,
                                waiter.lock_type,
                            )
                        })
                        .map(|earlier| earlier.owner_id),
                );
            }
        }
        let mut seen = std::collections::HashSet::new();
        let mut pending = edges.get(&owner_id).cloned().unwrap_or_default();
        while let Some(owner) = pending.pop() {
            if owner == owner_id {
                let (_, changed) = release_where(state, |ticket| {
                    ticket.owner_id == owner_id && ticket.status == MdlStatus::Pending
                });
                self.refresh_fast_blockers_for_keys(state, &changed);
                return Err(crate::error::catalog::ER_LOCK_DEADLOCK.error(
                    "Deadlock found when trying to get metadata lock; try restarting transaction",
                ));
            }
            if seen.insert(owner) {
                if let Some(blockers) = edges.get(&owner) {
                    pending.extend(blockers);
                }
            }
        }
        Err(crate::storage::core::lock_wait_pending_error())
    }

    /// Ticket acquisition with the fast-path Dekker protocol wrapped
    /// around read-blocking lock types: publish `provisional_blockers`
    /// before the grant decision (which then requires `readers == 0`),
    /// republish the ticket-derived count before withdrawing the
    /// provisional intent — the lock-free reader check never observes a
    /// window where an EXCLUSIVE / SHARED_NO_READ_WRITE acquire is
    /// invisible.
    fn acquire_one(
        &self,
        state: &mut MdlState,
        owner_id: u64,
        owner_thread_id: Option<u64>,
        duration: MdlDuration,
        request: MdlRequest,
        source: &str,
    ) -> AcquireOne {
        if blocks_fast_grants(request.lock_type) {
            let key = request.key.clone();
            let slot = self.fast_slot(&key);
            for (index, &kind) in FAST_MDL_TYPES.iter().enumerate() {
                if !compatible(request.lock_type, kind) {
                    slot.provisional_blockers[index].fetch_add(1, Ordering::SeqCst);
                }
            }
            let lock_type = request.lock_type;
            let result = acquire_one_ticketed(
                state,
                owner_id,
                owner_thread_id,
                duration,
                request,
                source,
                Some(&slot),
            );
            self.refresh_fast_blockers(&key, state.objects.get(&key));
            for (index, &kind) in FAST_MDL_TYPES.iter().enumerate() {
                if !compatible(lock_type, kind) {
                    slot.provisional_blockers[index].fetch_sub(1, Ordering::SeqCst);
                }
            }
            result
        } else {
            acquire_one_ticketed(
                state,
                owner_id,
                owner_thread_id,
                duration,
                request,
                source,
                None,
            )
        }
    }

    pub fn release_duration(&self, owner_id: u64, duration: MdlDuration) -> bool {
        let mut state = self.state.lock();
        let (released, changed_blockers) = release_where(&mut state, |ticket| {
            ticket.owner_id == owner_id && ticket.duration == duration
        });
        self.refresh_fast_blockers_for_keys(&state, &changed_blockers);
        released
    }

    pub fn release_cached(
        &self,
        owner_id: u64,
        duration: MdlDuration,
        requests: &[MdlRequest],
    ) -> bool {
        // Cached recipes enumerate every object acquired by this statement.
        // Global write protection makes even a point UPDATE a two-object
        // recipe; releasing it must not scan every table in the database.
        let mut state = self.state.lock();
        let mut released = false;
        for request in requests {
            let key = &request.key;
            let mut remove_object = false;
            let mut blockers_changed = false;
            if let Some(object) = state.objects.get_mut(key) {
                let before = object.granted.len() + object.waiting.len();
                let blockers_before = count_fast_blockers(object);
                object
                    .granted
                    .retain(|ticket| ticket.owner_id != owner_id || ticket.duration != duration);
                object
                    .waiting
                    .retain(|ticket| ticket.owner_id != owner_id || ticket.duration != duration);
                remove_object = object.granted.is_empty() && object.waiting.is_empty();
                blockers_changed =
                    blockers_before != 0 && count_fast_blockers(object) != blockers_before;
                released |= before != object.granted.len() + object.waiting.len();
            }
            if remove_object {
                state.objects.remove(key);
            }
            if blockers_changed {
                self.refresh_fast_blockers(key, state.objects.get(key));
            }
        }
        released
    }

    pub fn release_owner(&self, owner_id: u64) -> bool {
        let mut state = self.state.lock();
        let (released, changed_blockers) =
            release_where(&mut state, |ticket| ticket.owner_id == owner_id);
        self.refresh_fast_blockers_for_keys(&state, &changed_blockers);
        released
    }

    pub fn release_prepared(&self, txn_id: u64) -> bool {
        self.release_owner(prepared_owner_id(txn_id))
    }

    pub(crate) fn retain_fast_read(&self, grant: &MdlFastGrant, owner: u64, thread: u64) -> bool {
        // Anonymous grants exclude incompatible tickets until all owned
        // cursor tickets are installed. This includes multi-object results.
        let mut state = self.state.lock();
        let fast = self.fast.read();
        let mut keys = SmallVec::<[(MdlObjectKey, MdlLockType); 2]>::new();
        for (slot, kind) in &grant.slots {
            let Some(key) = fast
                .iter()
                .find_map(|(key, candidate)| Arc::ptr_eq(candidate, slot).then(|| key.clone()))
            else {
                return false;
            };
            keys.push((key, *kind));
        }
        drop(fast);
        for (key, lock_type) in keys {
            state.next_ticket_id = state.next_ticket_id.saturating_add(1);
            let id = state.next_ticket_id;
            state
                .objects
                .entry(key.clone())
                .or_default()
                .granted
                .push(MdlTicket {
                    id,
                    owner_id: owner,
                    owner_thread_id: Some(thread),
                    key,
                    lock_type,
                    duration: MdlDuration::Statement,
                    status: MdlStatus::Granted,
                    source: "sql/streaming-result".into(),
                    granted_unix_ms: coarse_unix_ms(),
                });
        }
        true
    }

    pub(crate) fn retain_transaction_reads(&self, owner: u64, cursor_owner: u64) -> bool {
        let mut state = self.state.lock();
        let copies = state
            .objects
            .values()
            .flat_map(|object| object.granted.iter())
            .filter(|ticket| {
                ticket.owner_id == owner && ticket.duration == MdlDuration::Transaction
            })
            .cloned()
            .collect::<Vec<_>>();
        let retained = !copies.is_empty();
        for mut ticket in copies {
            state.next_ticket_id = state.next_ticket_id.saturating_add(1);
            ticket.id = state.next_ticket_id;
            ticket.owner_id = cursor_owner;
            ticket.duration = MdlDuration::Statement;
            ticket.lock_type = if ticket.lock_type == MdlLockType::IntentionExclusive {
                MdlLockType::IntentionExclusive
            } else {
                MdlLockType::SharedRead
            };
            ticket.source = "sql/streaming-result".to_string();
            state
                .objects
                .entry(ticket.key.clone())
                .or_default()
                .granted
                .push(ticket);
        }
        retained
    }

    pub fn transfer_duration_to_owner(
        &self,
        from_owner_id: u64,
        from_duration: MdlDuration,
        to_owner_id: u64,
        to_owner_thread_id: Option<u64>,
        to_duration: MdlDuration,
        source: &str,
    ) -> bool {
        let mut state = self.state.lock();
        let mut transferred = false;
        for object in state.objects.values_mut() {
            for ticket in object
                .granted
                .iter_mut()
                .chain(object.waiting.iter_mut())
                .filter(|ticket| {
                    ticket.owner_id == from_owner_id && ticket.duration == from_duration
                })
            {
                ticket.owner_id = to_owner_id;
                ticket.owner_thread_id = to_owner_thread_id;
                ticket.duration = to_duration;
                ticket.source = source.to_string();
                transferred = true;
            }
        }
        transferred
    }

    pub fn transfer_transaction_to_prepared(&self, owner_id: u64, txn_id: u64) {
        let mut state = self.state.lock();
        let prepared = prepared_owner_id(txn_id);
        for object in state.objects.values_mut() {
            for ticket in object
                .granted
                .iter_mut()
                .chain(object.waiting.iter_mut())
                .filter(|ticket| {
                    ticket.owner_id == owner_id && ticket.duration == MdlDuration::Transaction
                })
            {
                ticket.owner_id = prepared;
            }
        }
    }

    /// Long-lived transactions and explicit locks cannot be drained by fencing
    /// new statements. Online maintenance may yield its barrier to new DML
    /// while waiting for these holders, without yielding to statement readers.
    pub(crate) fn has_long_lived_blocker(
        &self,
        key: &MdlObjectKey,
        owner_id: u64,
        requested: MdlLockType,
    ) -> bool {
        self.state.lock().objects.get(key).is_some_and(|object| {
            object.granted.iter().any(|ticket| {
                ticket.owner_id != owner_id
                    && ticket.duration != MdlDuration::Statement
                    && !compatible(ticket.lock_type, requested)
            })
        })
    }

    pub fn clear_pending_for_owner(&self, owner_id: u64) -> bool {
        let mut state = self.state.lock();
        let (released, changed_blockers) = release_where(&mut state, |ticket| {
            ticket.owner_id == owner_id && ticket.status == MdlStatus::Pending
        });
        self.refresh_fast_blockers_for_keys(&state, &changed_blockers);
        released
    }

    pub fn snapshot(&self) -> Vec<MetadataLockRow> {
        let state = self.state.lock();
        let mut rows = Vec::new();
        for object in state.objects.values() {
            for ticket in object.granted.iter().chain(object.waiting.iter()) {
                let instance_seed = format!(
                    "{}:{}:{:?}:{:?}:{:?}",
                    ticket.id,
                    ticket.owner_id,
                    ticket.key.object_type,
                    ticket.key.schema,
                    ticket.key.name
                );
                rows.push(MetadataLockRow {
                    object_type: ticket.key.object_type.as_str(),
                    object_schema: ticket.key.schema.clone(),
                    object_name: ticket.key.name.clone(),
                    column_name: None,
                    object_instance: fnv1a_64(&instance_seed),
                    lock_type: ticket.lock_type.as_str(),
                    lock_duration: ticket.duration.as_str(),
                    lock_status: ticket.status.as_str(),
                    source: ticket.source.clone(),
                    owner_thread_id: ticket.owner_thread_id,
                    owner_event_id: None,
                });
            }
        }
        // Anonymous shared grants have no tickets; synthesize one
        // row per in-flight reader so `performance_schema.metadata_locks`
        // still reflects them (owner unattributed — the lock-free grant
        // records no per-owner state by design).
        let fast = self.fast.read();
        for (key, slot) in fast.iter() {
            for &kind in &FAST_MDL_TYPES {
                let readers = slot.count(kind).load(Ordering::SeqCst).max(0);
                for _ in 0..readers {
                    rows.push(MetadataLockRow {
                        object_type: key.object_type.as_str(),
                        object_schema: key.schema.clone(),
                        object_name: key.name.clone(),
                        column_name: None,
                        object_instance: 0,
                        lock_type: kind.as_str(),
                        lock_duration: MdlDuration::Statement.as_str(),
                        lock_status: MdlStatus::Granted.as_str(),
                        source: "sql/statement".to_string(),
                        owner_thread_id: None,
                        owner_event_id: None,
                    });
                }
            }
        }
        rows
    }

    pub fn prepared_entries_for_owner(&self, owner_id: u64) -> Vec<PreparedMdlEntry> {
        let state = self.state.lock();
        let mut entries = Vec::new();
        for object in state.objects.values() {
            for ticket in &object.granted {
                if ticket.owner_id == owner_id && ticket.duration == MdlDuration::Transaction {
                    entries.push(PreparedMdlEntry {
                        object_type: encode_object_type(ticket.key.object_type),
                        schema: ticket.key.schema.clone(),
                        name: ticket.key.name.clone(),
                        lock_type: encode_lock_type(ticket.lock_type),
                    });
                }
            }
        }
        entries
    }

    pub fn install_prepared(&self, txn_id: u64, entries: &[PreparedMdlEntry]) -> bool {
        if entries.is_empty() {
            return false;
        }
        let mut state = self.state.lock();
        let owner_id = prepared_owner_id(txn_id);
        let mut installed = false;
        for entry in entries {
            let Some(object_type) = decode_object_type(entry.object_type) else {
                continue;
            };
            let Some(lock_type) = decode_lock_type(entry.lock_type) else {
                continue;
            };
            let key = MdlObjectKey {
                object_type,
                schema: entry.schema.as_deref().map(normalize_name),
                name: entry.name.as_deref().map(normalize_name),
            };
            if state
                .objects
                .entry(key.clone())
                .or_default()
                .granted
                .iter()
                .any(|ticket| ticket.owner_id == owner_id && ticket.key == key)
            {
                continue;
            }
            state.next_ticket_id = state.next_ticket_id.saturating_add(1);
            let id = state.next_ticket_id;
            state
                .objects
                .entry(key.clone())
                .or_default()
                .granted
                .push(MdlTicket {
                    id,
                    owner_id,
                    owner_thread_id: None,
                    key: key.clone(),
                    lock_type,
                    duration: MdlDuration::Transaction,
                    status: MdlStatus::Granted,
                    source: "xa/recovery".to_string(),
                    granted_unix_ms: coarse_unix_ms(),
                });
            if blocks_fast_grants(lock_type) {
                self.refresh_fast_blockers(&key, state.objects.get(&key));
            }
            installed = true;
        }
        installed
    }
}

fn encode_object_type(object_type: MdlObjectType) -> u8 {
    match object_type {
        MdlObjectType::Global => 0,
        MdlObjectType::Schema => 1,
        MdlObjectType::Table => 2,
        MdlObjectType::Function => 3,
        MdlObjectType::Procedure => 4,
        MdlObjectType::Trigger => 5,
        MdlObjectType::Event => 6,
        MdlObjectType::Commit => 7,
        MdlObjectType::Account => 8,
    }
}

fn decode_object_type(value: u8) -> Option<MdlObjectType> {
    Some(match value {
        0 => MdlObjectType::Global,
        1 => MdlObjectType::Schema,
        2 => MdlObjectType::Table,
        3 => MdlObjectType::Function,
        4 => MdlObjectType::Procedure,
        5 => MdlObjectType::Trigger,
        6 => MdlObjectType::Event,
        7 => MdlObjectType::Commit,
        8 => MdlObjectType::Account,
        _ => return None,
    })
}

fn encode_lock_type(lock_type: MdlLockType) -> u8 {
    match lock_type {
        MdlLockType::SharedRead => 0,
        MdlLockType::SharedWrite => 1,
        MdlLockType::SharedNoWrite => 2,
        MdlLockType::SharedUpgradable => 3,
        MdlLockType::SharedNoReadWrite => 4,
        MdlLockType::Exclusive => 5,
        MdlLockType::IntentionExclusive => 6,
    }
}

fn decode_lock_type(value: u8) -> Option<MdlLockType> {
    Some(match value {
        0 => MdlLockType::SharedRead,
        1 => MdlLockType::SharedWrite,
        2 => MdlLockType::SharedNoWrite,
        3 => MdlLockType::SharedUpgradable,
        4 => MdlLockType::SharedNoReadWrite,
        5 => MdlLockType::Exclusive,
        6 => MdlLockType::IntentionExclusive,
        _ => return None,
    })
}

enum AcquireOne {
    Granted(u64),
    AlreadyHeld,
    Promoted(Box<MdlTicket>),
    Pending,
}

fn acquire_one_ticketed(
    state: &mut MdlState,
    owner_id: u64,
    owner_thread_id: Option<u64>,
    duration: MdlDuration,
    request: MdlRequest,
    source: &str,
    fast_slot: Option<&MdlFastSlot>,
) -> AcquireOne {
    // Hot path (a concurrent shared lock already exists on this object,
    // e.g. every sysbench SELECT on the same table): the object entry is
    // present, so look it up without cloning the key. `entry(key.clone())`
    // would allocate the schema+name strings on *every* acquire even
    // though the clone is only needed on first insert — under the global
    // state mutex that clone is pure per-statement malloc churn.
    let object = if state.objects.contains_key(&request.key) {
        state
            .objects
            .get_mut(&request.key)
            .expect("present after contains_key")
    } else {
        state.objects.entry(request.key.clone()).or_default()
    };
    if let Some(pos) = object.granted.iter().position(|ticket| {
        ticket.owner_id == owner_id
            && ticket.duration == duration
            && (ticket.lock_type.covers(request.lock_type)
                || request.lock_type.covers(ticket.lock_type))
    }) {
        let existing_lock_type = object.granted[pos].lock_type;
        if existing_lock_type.covers(request.lock_type) {
            return AcquireOne::AlreadyHeld;
        }
        if fast_slot.is_none_or(|slot| slot.compatible_with(request.lock_type))
            && object
                .granted
                .iter()
                .filter(|ticket| ticket.owner_id != owner_id)
                .all(|ticket| compatible(ticket.lock_type, request.lock_type))
        {
            let previous = Box::new(object.granted[pos].clone());
            object.granted[pos].lock_type = request.lock_type;
            object.granted[pos].source = source.to_string();
            // A prior attempt of this same upgrade may have parked a
            // waiting ticket (blocked then, grantable now via the
            // promote path). Withdraw it — a leaked PENDING ticket
            // would fence readers forever.
            object.waiting.retain(|ticket| {
                ticket.owner_id != owner_id
                    || ticket.duration != duration
                    || ticket.lock_type != request.lock_type
            });
            return AcquireOne::Promoted(previous);
        }
    }

    if let Some(pos) = object.waiting.iter().position(|ticket| {
        ticket.owner_id == owner_id
            && ticket.duration == duration
            && ticket.lock_type == request.lock_type
    }) {
        if fast_slot.is_none_or(|slot| slot.compatible_with(request.lock_type))
            && can_grant(object, owner_id, request.lock_type, pos)
        {
            let mut ticket = object.waiting.remove(pos);
            ticket.status = MdlStatus::Granted;
            ticket.granted_unix_ms = coarse_unix_ms();
            let id = ticket.id;
            object.granted.push(ticket);
            return AcquireOne::Granted(id);
        }
        return AcquireOne::Pending;
    }

    if fast_slot.is_none_or(|slot| slot.compatible_with(request.lock_type))
        && can_grant_new(object, owner_id, request.lock_type)
    {
        state.next_ticket_id = state.next_ticket_id.saturating_add(1);
        let id = state.next_ticket_id;
        object.granted.push(MdlTicket {
            id,
            owner_id,
            owner_thread_id,
            key: request.key,
            lock_type: request.lock_type,
            duration,
            status: MdlStatus::Granted,
            source: source.to_string(),
            granted_unix_ms: coarse_unix_ms(),
        });
        return AcquireOne::Granted(id);
    }

    state.next_ticket_id = state.next_ticket_id.saturating_add(1);
    object.waiting.push(MdlTicket {
        id: state.next_ticket_id,
        owner_id,
        owner_thread_id,
        key: request.key,
        lock_type: request.lock_type,
        duration,
        status: MdlStatus::Pending,
        source: source.to_string(),
        granted_unix_ms: 0,
    });
    AcquireOne::Pending
}

// Existing holders must be able to upgrade/reacquire before queued requests,
// just like the same-duration promotion path above. Otherwise an owner can
// wait on its own waiter (including indirectly: SU owner -> queued X -> queued
// writer -> owner's X barrier). Only granted locks and fast readers may block
// that owner. Ordinary newcomers still retain writer preference.
fn waiting_ticket_blocks_owner(
    object: &ObjectLockState,
    waiting: &MdlTicket,
    owner_id: u64,
    requested: MdlLockType,
) -> bool {
    waiting.owner_id != owner_id
        && waiting.lock_type.is_writer_like()
        && !compatible(waiting.lock_type, requested)
        && !object.granted.iter().any(|held| {
            held.owner_id == owner_id
                || (held.owner_id > u32::MAX as u64
                    && held.owner_id < PREPARED_OWNER_TAG
                    && held.owner_thread_id == Some(owner_id))
        })
}

fn can_grant_new(object: &ObjectLockState, owner_id: u64, requested: MdlLockType) -> bool {
    object
        .granted
        .iter()
        .filter(|ticket| ticket.owner_id != owner_id)
        .all(|ticket| compatible(ticket.lock_type, requested))
        && object
            .waiting
            .iter()
            .all(|ticket| !waiting_ticket_blocks_owner(object, ticket, owner_id, requested))
}

fn can_grant(
    object: &ObjectLockState,
    owner_id: u64,
    requested: MdlLockType,
    waiting_pos: usize,
) -> bool {
    object
        .granted
        .iter()
        .filter(|ticket| ticket.owner_id != owner_id)
        .all(|ticket| compatible(ticket.lock_type, requested))
        && object
            .waiting
            .iter()
            .take(waiting_pos)
            .all(|ticket| !waiting_ticket_blocks_owner(object, ticket, owner_id, requested))
}

fn compatible(held: MdlLockType, requested: MdlLockType) -> bool {
    use MdlLockType::*;
    if held == Exclusive || requested == Exclusive {
        return false;
    }
    if held == SharedNoReadWrite || requested == SharedNoReadWrite {
        return false;
    }
    if held == IntentionExclusive || requested == IntentionExclusive {
        return true;
    }
    if held == SharedNoWrite || requested == SharedNoWrite {
        return matches!(
            (held, requested),
            (SharedNoWrite, SharedRead)
                | (SharedRead, SharedNoWrite)
                | (SharedNoWrite, SharedNoWrite)
        );
    }
    if held == SharedUpgradable || requested == SharedUpgradable {
        return matches!(
            (held, requested),
            (SharedUpgradable, SharedRead)
                | (SharedRead, SharedUpgradable)
                | (SharedUpgradable, SharedWrite)
                | (SharedWrite, SharedUpgradable)
        );
    }
    true
}

fn remove_granted_tickets(state: &mut MdlState, owner_id: u64, ids: &[u64]) {
    if ids.is_empty() {
        return;
    }
    for object in state.objects.values_mut() {
        object
            .granted
            .retain(|ticket| ticket.owner_id != owner_id || !ids.contains(&ticket.id));
    }
    prune_empty_objects(state);
}

fn restore_granted_ticket(state: &mut MdlState, previous: MdlTicket) {
    if let Some(ticket) = state.objects.get_mut(&previous.key).and_then(|object| {
        object
            .granted
            .iter_mut()
            .find(|ticket| ticket.id == previous.id && ticket.owner_id == previous.owner_id)
    }) {
        *ticket = previous;
    }
}

fn release_where<F>(state: &mut MdlState, mut predicate: F) -> (bool, Vec<MdlObjectKey>)
where
    F: FnMut(&MdlTicket) -> bool,
{
    let mut released = false;
    // Objects whose read-blocking ticket count changed: the caller must
    // republish their fast-path blocker counts after the prune. Key
    // clones only happen when an EXCLUSIVE / SHARED_NO_READ_WRITE ticket
    // was actually removed — DDL-rare, never on the DML/read hot path.
    let mut changed_blockers = Vec::new();
    for (key, object) in state.objects.iter_mut() {
        let before = object.granted.len() + object.waiting.len();
        let blockers_before = count_fast_blockers(object);
        object.granted.retain(|ticket| !predicate(ticket));
        object.waiting.retain(|ticket| !predicate(ticket));
        released |= before != object.granted.len() + object.waiting.len();
        if blockers_before != 0 && count_fast_blockers(object) != blockers_before {
            changed_blockers.push(key.clone());
        }
    }
    prune_empty_objects(state);
    (released, changed_blockers)
}

fn prune_empty_objects(state: &mut MdlState) {
    state
        .objects
        .retain(|_, object| !object.granted.is_empty() || !object.waiting.is_empty());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(lock_type: MdlLockType) -> MdlRequest {
        MdlRequest {
            key: MdlObjectKey::table("test", "t"),
            lock_type,
        }
    }

    #[test]
    fn fast_slot_cache_cannot_bypass_another_database_managers_blockers() {
        let left = MdlManager::default();
        let right = MdlManager::default();
        let mut cache = MdlFastCache::default();
        let grant = right
            .try_acquire_statement_fast_cached(&[req(MdlLockType::SharedWrite)], &mut cache)
            .unwrap();
        drop(grant);
        left.acquire_many(
            1,
            Some(1),
            MdlDuration::Statement,
            vec![req(MdlLockType::Exclusive)],
            "ddl",
        )
        .unwrap();
        assert!(left
            .try_acquire_statement_fast_cached(&[req(MdlLockType::SharedWrite)], &mut cache)
            .is_none());
        left.release_owner(1);
        assert!(left
            .try_acquire_statement_fast_cached(&[req(MdlLockType::SharedWrite)], &mut cache)
            .is_some());
    }

    #[test]
    fn fast_shared_modes_preserve_every_ticket_compatibility_pair() {
        for &held in &FAST_MDL_TYPES {
            for requested in [
                MdlLockType::SharedRead,
                MdlLockType::SharedWrite,
                MdlLockType::SharedNoWrite,
                MdlLockType::SharedUpgradable,
                MdlLockType::SharedNoReadWrite,
                MdlLockType::Exclusive,
                MdlLockType::IntentionExclusive,
            ] {
                let mdl = MdlManager::default();
                let grant = mdl.try_acquire_statement_fast(&[req(held)]).unwrap();
                let result = mdl.acquire_many(
                    2,
                    Some(2),
                    MdlDuration::Statement,
                    vec![req(requested)],
                    "test",
                );
                assert_eq!(
                    result.is_ok(),
                    compatible(held, requested),
                    "fast {held:?}, ticket {requested:?}"
                );
                drop(grant);
                if result.is_err() {
                    mdl.acquire_many(
                        2,
                        Some(2),
                        MdlDuration::Statement,
                        vec![req(requested)],
                        "retry",
                    )
                    .unwrap();
                }
            }
        }
    }

    #[test]
    fn failed_fast_batch_and_unwind_release_every_acquired_object() {
        let mdl = MdlManager::default();
        let a = MdlRequest {
            key: MdlObjectKey::table("test", "a"),
            lock_type: MdlLockType::SharedWrite,
        };
        let b = MdlRequest {
            key: MdlObjectKey::table("test", "b"),
            lock_type: MdlLockType::SharedWrite,
        };
        let x = MdlRequest {
            lock_type: MdlLockType::Exclusive,
            ..b.clone()
        };
        mdl.acquire_many(1, Some(1), MdlDuration::Statement, vec![x], "ddl")
            .unwrap();
        assert!(mdl
            .try_acquire_statement_fast(&[a.clone(), b.clone()])
            .is_none());
        mdl.acquire_many(
            2,
            Some(2),
            MdlDuration::Statement,
            vec![MdlRequest {
                lock_type: MdlLockType::Exclusive,
                ..a.clone()
            }],
            "ddl",
        )
        .unwrap();
        mdl.release_owner(1);
        mdl.release_owner(2);
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _grant = mdl
                .try_acquire_statement_fast(&[a.clone(), b.clone()])
                .unwrap();
            panic!("exercise metadata grant unwinding");
        }));
        assert!(unwind.is_err());
        mdl.acquire_many(
            3,
            Some(3),
            MdlDuration::Statement,
            vec![
                MdlRequest {
                    lock_type: MdlLockType::Exclusive,
                    ..a
                },
                MdlRequest {
                    lock_type: MdlLockType::Exclusive,
                    ..b
                },
            ],
            "ddl",
        )
        .unwrap();
    }

    #[test]
    fn streaming_fast_batch_retains_all_objects_until_cursor_release() {
        let mdl = MdlManager::default();
        let requests = [
            MdlRequest {
                key: MdlObjectKey::table("test", "a"),
                lock_type: MdlLockType::SharedRead,
            },
            MdlRequest {
                key: MdlObjectKey::table("test", "b"),
                lock_type: MdlLockType::SharedWrite,
            },
        ];
        let grant = mdl.try_acquire_statement_fast(&requests).unwrap();
        let cursor_owner = 1u64 << 32;
        assert!(mdl.retain_fast_read(&grant, cursor_owner, 7));
        drop(grant);
        for request in &requests {
            let exclusive = MdlRequest {
                lock_type: MdlLockType::Exclusive,
                ..request.clone()
            };
            assert!(mdl
                .acquire_many(9, Some(9), MdlDuration::Statement, vec![exclusive], "ddl")
                .is_err());
        }
        mdl.release_owner(cursor_owner);
        mdl.acquire_many(
            9,
            Some(9),
            MdlDuration::Statement,
            requests
                .iter()
                .map(|request| MdlRequest {
                    lock_type: MdlLockType::Exclusive,
                    ..request.clone()
                })
                .collect(),
            "ddl",
        )
        .unwrap();
    }

    #[test]
    fn cross_duration_exclusive_reacquire_does_not_wait_for_its_own_waiter() {
        let mdl = MdlManager::default();
        mdl.acquire_many(
            1,
            Some(1),
            MdlDuration::Statement,
            vec![req(MdlLockType::Exclusive)],
            "ddl",
        )
        .unwrap();
        assert!(mdl
            .acquire_many(
                2,
                Some(2),
                MdlDuration::Statement,
                vec![req(MdlLockType::SharedWrite)],
                "writer"
            )
            .is_err());
        mdl.acquire_many(
            1,
            Some(1),
            MdlDuration::Transaction,
            vec![req(MdlLockType::Exclusive)],
            "barrier",
        )
        .unwrap();
        mdl.release_duration(1, MdlDuration::Transaction);
        assert!(mdl
            .acquire_many(
                2,
                Some(2),
                MdlDuration::Statement,
                vec![req(MdlLockType::SharedWrite)],
                "writer"
            )
            .is_err());
        mdl.release_duration(1, MdlDuration::Statement);
        mdl.acquire_many(
            2,
            Some(2),
            MdlDuration::Statement,
            vec![req(MdlLockType::SharedWrite)],
            "writer",
        )
        .unwrap();
    }

    #[test]
    fn cross_duration_shared_upgrade_still_waits_for_other_readers() {
        let mdl = MdlManager::default();
        mdl.acquire_many(
            1,
            Some(1),
            MdlDuration::Statement,
            vec![req(MdlLockType::SharedNoWrite)],
            "ddl",
        )
        .unwrap();
        assert!(mdl
            .acquire_many(
                2,
                Some(2),
                MdlDuration::Statement,
                vec![req(MdlLockType::SharedWrite)],
                "writer"
            )
            .is_err());
        mdl.acquire_many(
            3,
            Some(3),
            MdlDuration::Statement,
            vec![req(MdlLockType::SharedRead)],
            "reader",
        )
        .unwrap();
        assert!(mdl
            .acquire_many(
                1,
                Some(1),
                MdlDuration::Transaction,
                vec![req(MdlLockType::Exclusive)],
                "barrier"
            )
            .is_err());
        mdl.release_owner(3);
        mdl.acquire_many(
            1,
            Some(1),
            MdlDuration::Transaction,
            vec![req(MdlLockType::Exclusive)],
            "barrier",
        )
        .unwrap();
        mdl.release_duration(1, MdlDuration::Transaction);
        assert!(mdl
            .acquire_many(
                2,
                Some(2),
                MdlDuration::Statement,
                vec![req(MdlLockType::SharedWrite)],
                "writer"
            )
            .is_err());
        mdl.release_owner(1);
        mdl.acquire_many(
            2,
            Some(2),
            MdlDuration::Statement,
            vec![req(MdlLockType::SharedWrite)],
            "writer",
        )
        .unwrap();
    }

    #[test]
    fn cross_duration_upgrade_bypasses_indirect_waiter_cycle() {
        let mdl = MdlManager::default();
        mdl.acquire_many(
            1,
            Some(1),
            MdlDuration::Statement,
            vec![req(MdlLockType::SharedUpgradable)],
            "online ddl",
        )
        .unwrap();
        assert!(mdl
            .acquire_many(
                2,
                Some(2),
                MdlDuration::Statement,
                vec![req(MdlLockType::Exclusive)],
                "other ddl"
            )
            .is_err());
        assert!(mdl
            .acquire_many(
                3,
                Some(3),
                MdlDuration::Statement,
                vec![req(MdlLockType::SharedWrite)],
                "writer"
            )
            .is_err());
        mdl.acquire_many(
            1,
            Some(1),
            MdlDuration::Transaction,
            vec![req(MdlLockType::Exclusive)],
            "publish",
        )
        .unwrap();
        mdl.release_owner(1);
        // Newcomers cannot bypass the queued DDL.
        assert!(mdl
            .acquire_many(
                4,
                Some(4),
                MdlDuration::Statement,
                vec![req(MdlLockType::SharedWrite)],
                "new writer"
            )
            .is_err());
        mdl.acquire_many(
            2,
            Some(2),
            MdlDuration::Statement,
            vec![req(MdlLockType::Exclusive)],
            "other ddl",
        )
        .unwrap();
        mdl.release_owner(2);
        mdl.acquire_many(
            3,
            Some(3),
            MdlDuration::Statement,
            vec![req(MdlLockType::SharedWrite)],
            "writer",
        )
        .unwrap();
    }

    #[test]
    fn read_compatible_mdl_join_preserves_both_conflict_sets() {
        use MdlLockType::*;
        let modes = [
            SharedRead,
            SharedWrite,
            SharedNoWrite,
            SharedUpgradable,
            SharedNoReadWrite,
            Exclusive,
            IntentionExclusive,
        ];
        for held in modes {
            for requested in modes {
                for probe in modes {
                    let mdl = MdlManager::default();
                    for mode in [held, requested] {
                        mdl.acquire_many(
                            1,
                            Some(1),
                            MdlDuration::Statement,
                            vec![req(mode)],
                            "same-owner union",
                        )
                        .unwrap();
                    }
                    let granted = mdl
                        .acquire_many(
                            2,
                            Some(2),
                            MdlDuration::Statement,
                            vec![req(probe)],
                            "probe",
                        )
                        .is_ok();
                    assert_eq!(
                        granted,
                        compatible(held, probe) && compatible(requested, probe),
                        "held={held:?} requested={requested:?} probe={probe:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn failed_multi_object_acquire_restores_existing_upgrade() {
        let mdl = MdlManager::default();
        let a = MdlObjectKey::table("test", "a");
        let b = MdlObjectKey::table("test", "b");
        mdl.acquire_many(
            1,
            Some(1),
            MdlDuration::Explicit,
            vec![MdlRequest {
                key: a.clone(),
                lock_type: MdlLockType::SharedUpgradable,
            }],
            "original",
        )
        .unwrap();
        mdl.acquire_many(
            2,
            Some(2),
            MdlDuration::Transaction,
            vec![MdlRequest {
                key: b.clone(),
                lock_type: MdlLockType::SharedWrite,
            }],
            "writer",
        )
        .unwrap();
        assert!(mdl
            .acquire_many(
                1,
                Some(1),
                MdlDuration::Explicit,
                vec![
                    MdlRequest {
                        key: a.clone(),
                        lock_type: MdlLockType::Exclusive
                    },
                    MdlRequest {
                        key: b,
                        lock_type: MdlLockType::Exclusive
                    },
                ],
                "upgrade"
            )
            .is_err());
        let rows = mdl.snapshot();
        let original = rows
            .iter()
            .find(|row| row.object_name.as_deref() == Some("a"))
            .unwrap();
        assert_eq!(original.lock_type, "SHARED_UPGRADABLE");
        assert_eq!(original.source, "original");
        assert!(mdl
            .try_acquire_statement_read_fast(&[MdlRequest {
                key: a,
                lock_type: MdlLockType::SharedRead,
            }])
            .is_some());
    }

    #[test]
    fn fast_read_grant_and_release() {
        let mdl = MdlManager::default();
        let grant = mdl
            .try_acquire_statement_read_fast(&[req(MdlLockType::SharedRead)])
            .expect("uncontended fast read grant");
        // Visible in the introspection snapshot as a synthesized row.
        assert_eq!(
            mdl.snapshot()
                .iter()
                .filter(|row| row.lock_type == "SHARED_READ" && row.lock_status == "GRANTED")
                .count(),
            1
        );
        drop(grant);
        assert!(mdl.snapshot().is_empty());
    }

    #[test]
    fn fast_read_declines_multi_request_and_non_read() {
        let mdl = MdlManager::default();
        assert!(mdl
            .try_acquire_statement_read_fast(&[req(MdlLockType::SharedWrite)])
            .is_none());
        assert!(mdl
            .try_acquire_statement_read_fast(&[
                req(MdlLockType::SharedRead),
                MdlRequest {
                    key: MdlObjectKey::table("test", "t2"),
                    lock_type: MdlLockType::SharedRead,
                },
            ])
            .is_none());
    }

    #[test]
    fn exclusive_blocks_fast_read_until_released() {
        let mdl = MdlManager::default();
        mdl.acquire_many(
            1,
            Some(1),
            MdlDuration::Explicit,
            vec![req(MdlLockType::Exclusive)],
            "test",
        )
        .unwrap();
        assert!(mdl
            .try_acquire_statement_read_fast(&[req(MdlLockType::SharedRead)])
            .is_none());
        mdl.release_owner(1);
        assert!(mdl
            .try_acquire_statement_read_fast(&[req(MdlLockType::SharedRead)])
            .is_some());
    }

    #[test]
    fn waiting_exclusive_blocks_fast_read() {
        let mdl = MdlManager::default();
        // Owner 1 holds SHARED_READ via the ticketed path, so owner 2's
        // EXCLUSIVE parks as waiting.
        mdl.acquire_many(
            1,
            Some(1),
            MdlDuration::Explicit,
            vec![req(MdlLockType::SharedRead)],
            "test",
        )
        .unwrap();
        assert!(mdl
            .acquire_many(
                2,
                Some(2),
                MdlDuration::Explicit,
                vec![req(MdlLockType::Exclusive)],
                "test",
            )
            .is_err());
        // The parked EXCLUSIVE must fence new fast readers.
        assert!(mdl
            .try_acquire_statement_read_fast(&[req(MdlLockType::SharedRead)])
            .is_none());
        // Withdrawing the waiter reopens the fast path.
        mdl.clear_pending_for_owner(2);
        assert!(mdl
            .try_acquire_statement_read_fast(&[req(MdlLockType::SharedRead)])
            .is_some());
    }

    #[test]
    fn fast_reader_blocks_exclusive_grant() {
        let mdl = MdlManager::default();
        let grant = mdl
            .try_acquire_statement_read_fast(&[req(MdlLockType::SharedRead)])
            .expect("fast read grant");
        assert!(mdl
            .acquire_many(
                1,
                Some(1),
                MdlDuration::Explicit,
                vec![req(MdlLockType::Exclusive)],
                "test",
            )
            .is_err());
        drop(grant);
        // Retry after the reader released (the parked waiting ticket is
        // promoted on the next acquire call).
        mdl.acquire_many(
            1,
            Some(1),
            MdlDuration::Explicit,
            vec![req(MdlLockType::Exclusive)],
            "test",
        )
        .unwrap();
    }

    #[test]
    fn fast_reader_blocks_exclusive_upgrade() {
        let mdl = MdlManager::default();
        // Owner 1 holds SHARED_UPGRADABLE and wants to promote to
        // EXCLUSIVE while a fast reader is in flight — must stay pending.
        mdl.acquire_many(
            1,
            Some(1),
            MdlDuration::Explicit,
            vec![req(MdlLockType::SharedUpgradable)],
            "test",
        )
        .unwrap();
        let grant = mdl
            .try_acquire_statement_read_fast(&[req(MdlLockType::SharedRead)])
            .expect("fast read grant");
        assert!(mdl
            .acquire_many(
                1,
                Some(1),
                MdlDuration::Explicit,
                vec![req(MdlLockType::Exclusive)],
                "test",
            )
            .is_err());
        drop(grant);
        mdl.acquire_many(
            1,
            Some(1),
            MdlDuration::Explicit,
            vec![req(MdlLockType::Exclusive)],
            "test",
        )
        .unwrap();
    }

    #[test]
    fn read_lock_blocks_write_but_allows_read() {
        let mdl = MdlManager::default();
        mdl.acquire_many(
            1,
            Some(1),
            MdlDuration::Explicit,
            vec![req(MdlLockType::SharedNoWrite)],
            "test",
        )
        .unwrap();
        mdl.acquire_many(
            2,
            Some(2),
            MdlDuration::Statement,
            vec![req(MdlLockType::SharedRead)],
            "test",
        )
        .unwrap();
        assert!(mdl
            .acquire_many(
                3,
                Some(3),
                MdlDuration::Statement,
                vec![req(MdlLockType::SharedWrite)],
                "test",
            )
            .is_err());
        assert_eq!(
            mdl.snapshot()
                .into_iter()
                .filter(|row| row.lock_status == "PENDING")
                .count(),
            1
        );
    }

    #[test]
    fn write_lock_blocks_every_other_session() {
        let mdl = MdlManager::default();
        mdl.acquire_many(
            1,
            Some(1),
            MdlDuration::Explicit,
            vec![req(MdlLockType::Exclusive)],
            "test",
        )
        .unwrap();
        assert!(mdl
            .acquire_many(
                2,
                Some(2),
                MdlDuration::Statement,
                vec![req(MdlLockType::SharedRead)],
                "test",
            )
            .is_err());
    }

    #[test]
    fn release_grants_pending_waiter_on_retry() {
        let mdl = MdlManager::default();
        mdl.acquire_many(
            1,
            Some(1),
            MdlDuration::Explicit,
            vec![req(MdlLockType::Exclusive)],
            "test",
        )
        .unwrap();
        assert!(mdl
            .acquire_many(
                2,
                Some(2),
                MdlDuration::Statement,
                vec![req(MdlLockType::SharedRead)],
                "test",
            )
            .is_err());
        mdl.release_duration(1, MdlDuration::Explicit);
        mdl.acquire_many(
            2,
            Some(2),
            MdlDuration::Statement,
            vec![req(MdlLockType::SharedRead)],
            "test",
        )
        .unwrap();
    }
    #[test]
    fn deadlock_detection_includes_queued_writer_edges() {
        let mdl = MdlManager::default();
        let acquire = |owner, name, mode| {
            mdl.acquire_cached(
                owner,
                Some(owner),
                MdlDuration::Transaction,
                &[MdlRequest {
                    key: MdlObjectKey::table("test", name),
                    lock_type: mode,
                }],
                "test",
            )
        };
        acquire(1, "a", MdlLockType::SharedRead).unwrap();
        acquire(3, "b", MdlLockType::SharedRead).unwrap();
        assert!(crate::storage::is_lock_wait_pending_error(
            &acquire(2, "a", MdlLockType::Exclusive).unwrap_err()
        ));
        assert!(crate::storage::is_lock_wait_pending_error(
            &acquire(1, "b", MdlLockType::Exclusive).unwrap_err()
        ));
        // 3 waits for queued writer 2, which waits for 1, which waits for 3.
        assert_eq!(
            acquire(3, "a", MdlLockType::SharedRead).unwrap_err().code(),
            1213
        );
        assert!(!mdl
            .snapshot()
            .iter()
            .any(|r| r.owner_thread_id == Some(3) && r.lock_status == "PENDING"));
        mdl.release_owner(3);
        acquire(1, "b", MdlLockType::Exclusive).unwrap();
        mdl.release_owner(1);
        acquire(2, "a", MdlLockType::Exclusive).unwrap();
    }
    #[test]
    fn cached_multi_object_release_preserves_unrelated_grants() {
        let mdl = MdlManager::default();
        let requests = vec![
            MdlRequest {
                key: MdlObjectKey::global(),
                lock_type: MdlLockType::SharedWrite,
            },
            req(MdlLockType::SharedWrite),
        ];
        mdl.acquire_many(1, Some(1), MdlDuration::Statement, requests.clone(), "test")
            .unwrap();
        mdl.acquire_many(
            1,
            Some(1),
            MdlDuration::Transaction,
            vec![req(MdlLockType::SharedRead)],
            "test",
        )
        .unwrap();
        mdl.release_cached(1, MdlDuration::Statement, &requests);
        let snapshot = mdl.snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].lock_duration, "TRANSACTION");
        assert!(!mdl.release_cached(1, MdlDuration::Statement, &requests));
    }
}
