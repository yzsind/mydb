//! Demand-driven executor memory arbitration.
//!
//! Work areas register with a small floor, then grow against current pool
//! pressure and the fair share of active consumers. Cardinality and row-width
//! estimates do not control reservations. Operators charge actual resident
//! bytes and spill when their handle requests it; release returns both usage
//! and surplus reservations to the shared pool.

use crate::util::atomic_accounting::saturating_sub_usize as atomic_saturating_sub;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use crate::engine::executor::mem_budget;

/// Absolute floor for any work area's reservation, independent of the
/// (possibly wildly wrong) cardinality estimate. A registration never
/// grabs more than this until the operator *actually* touches the
/// bytes via `try_charge`.
const MIN_OP_FLOOR_BYTES: usize = 64 * 1024;
static OVERSUBSCRIBE_PCT_CONFIG: AtomicUsize = AtomicUsize::new(0);

/// `MYDB_WORK_AREA_OVERSUBSCRIBE_PCT` — how much of the executor pool a
/// single work area may grow into, expressed as a percentage of its
/// concurrency-fair share (`target / demanding_handles`). 100 = strict
/// fair share (the sum of all shares equals the pool); >100 lets a lone
/// or under-subscribed operator borrow ahead of its share while the
/// global hard cap (`allocated > target`) still forces a spill once the
/// pool is genuinely full. The startup value is cached and may later be
/// replaced through the GLOBAL sysvar setter.
fn oversubscribe_pct() -> u32 {
    let cur = OVERSUBSCRIBE_PCT_CONFIG.load(Ordering::Acquire);
    if cur != 0 {
        return cur as u32;
    }
    let value = crate::util::env::var("MYDB_WORK_AREA_OVERSUBSCRIBE_PCT")
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
        .filter(|v| *v >= 100 && *v <= 100_000)
        .unwrap_or(100);
    let _ = OVERSUBSCRIBE_PCT_CONFIG.compare_exchange(
        0,
        value as usize,
        Ordering::AcqRel,
        Ordering::Acquire,
    );
    OVERSUBSCRIBE_PCT_CONFIG.load(Ordering::Acquire) as u32
}

pub fn work_area_oversubscribe_pct() -> u32 {
    oversubscribe_pct()
}

pub fn set_work_area_oversubscribe_pct(value: u32) -> Result<(), crate::types::QueryError> {
    if !(100..=100_000).contains(&value) {
        return Err(crate::storage::error(
            crate::error::catalog::ER_WRONG_TYPE_FOR_VAR,
            "variable 'mydb_work_area_oversubscribe_pct' must be between 100 and 100000",
        ));
    }
    OVERSUBSCRIBE_PCT_CONFIG.store(value as usize, Ordering::Release);
    Ok(())
}

/// Which tier the arbiter chose for a handle. Used by spec-aware
/// operators (e.g. Sort) to decide algorithmic shape: Optimal → keep
/// fully in-memory; OnePass → reserve grace buffer; Min → multi-pass
/// spill from the first row.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum GrantTier {
    Optimal,
    OnePass,
    Min,
}

/// Reply from `try_charge`. The operator is expected to:
/// - on `Continue`: keep accumulating in-memory state
/// - on `Spill`:    write the largest partition / run to disk,
///                  then call `release(freed_bytes)`
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SpillHint {
    Continue,
    Spill,
}

/// Global manager. One per process. Holds the executor memory
/// target, the running allocation total, and aggregated counters for
/// the advisor view.
pub struct WorkAreaManager {
    target_bytes: AtomicUsize,
    allocated_bytes: AtomicUsize,
    high_water_bytes: AtomicUsize,
    used_bytes: AtomicUsize,
    high_water_used_bytes: AtomicUsize,
    active_handles: AtomicU64,
    /// Registered handles that currently own at least one charged byte.
    /// Plan construction may register several downstream work areas long
    /// before they consume input; counting those idle handles in the fair
    /// share forces the only running operator to spill into otherwise unused
    /// memory. A handle enters/leaves this set on its first charge/last
    /// release, so true concurrent consumers still divide the pool fairly.
    demanding_handles: AtomicU64,
    optimal_grants: AtomicU64,
    one_pass_grants: AtomicU64,
    min_grants: AtomicU64,
    spill_requests: AtomicU64,
    spill_estimated_bytes: AtomicU64,
    force_overflow_bytes: AtomicU64,
    force_overflow_events: AtomicU64,
}

impl WorkAreaManager {
    fn new(target_bytes: usize) -> Self {
        Self {
            target_bytes: AtomicUsize::new(target_bytes),
            allocated_bytes: AtomicUsize::new(0),
            high_water_bytes: AtomicUsize::new(0),
            used_bytes: AtomicUsize::new(0),
            high_water_used_bytes: AtomicUsize::new(0),
            active_handles: AtomicU64::new(0),
            demanding_handles: AtomicU64::new(0),
            optimal_grants: AtomicU64::new(0),
            one_pass_grants: AtomicU64::new(0),
            min_grants: AtomicU64::new(0),
            spill_requests: AtomicU64::new(0),
            spill_estimated_bytes: AtomicU64::new(0),
            force_overflow_bytes: AtomicU64::new(0),
            force_overflow_events: AtomicU64::new(0),
        }
    }

    /// Snapshot for the advisor view.
    pub fn snapshot(&self) -> WorkAreaSnapshot {
        WorkAreaSnapshot {
            target_bytes: self.target_bytes(),
            allocated_bytes: self.allocated_bytes(),
            high_water_bytes: self.high_water_bytes(),
            used_bytes: self.used_bytes(),
            high_water_used_bytes: self.high_water_used_bytes(),
            active_handles: self.active_handles(),
            optimal_grants: self.optimal_grants(),
            one_pass_grants: self.one_pass_grants(),
            min_grants: self.min_grants(),
            spill_requests: self.spill_requests(),
            spill_estimated_bytes: self.spill_estimated_bytes(),
            force_overflow_bytes: self.force_overflow_bytes(),
            force_overflow_events: self.force_overflow_events(),
        }
    }

    pub fn target_bytes(&self) -> usize {
        self.target_bytes.load(Ordering::Relaxed)
    }
    pub fn set_target_bytes(&self, bytes: usize) {
        self.target_bytes.store(
            bytes.max(crate::engine::executor::mem_budget::min_executor_target_bytes()),
            Ordering::Relaxed,
        );
    }
    pub fn allocated_bytes(&self) -> usize {
        self.allocated_bytes.load(Ordering::Relaxed)
    }
    pub fn high_water_bytes(&self) -> usize {
        self.high_water_bytes.load(Ordering::Relaxed)
    }
    pub fn used_bytes(&self) -> usize {
        self.used_bytes.load(Ordering::Relaxed)
    }
    pub fn high_water_used_bytes(&self) -> usize {
        self.high_water_used_bytes.load(Ordering::Relaxed)
    }
    pub fn active_handles(&self) -> u64 {
        self.active_handles.load(Ordering::Relaxed)
    }
    pub fn demanding_handles(&self) -> u64 {
        self.demanding_handles.load(Ordering::Relaxed)
    }
    pub fn optimal_grants(&self) -> u64 {
        self.optimal_grants.load(Ordering::Relaxed)
    }
    pub fn one_pass_grants(&self) -> u64 {
        self.one_pass_grants.load(Ordering::Relaxed)
    }
    pub fn min_grants(&self) -> u64 {
        self.min_grants.load(Ordering::Relaxed)
    }
    pub fn spill_requests(&self) -> u64 {
        self.spill_requests.load(Ordering::Relaxed)
    }
    pub fn spill_estimated_bytes(&self) -> u64 {
        self.spill_estimated_bytes.load(Ordering::Relaxed)
    }
    pub fn force_overflow_bytes(&self) -> u64 {
        self.force_overflow_bytes.load(Ordering::Relaxed)
    }
    pub fn force_overflow_events(&self) -> u64 {
        self.force_overflow_events.load(Ordering::Relaxed)
    }

    /// Pick the tier to grant based on current pool pressure.
    fn select_tier(&self) -> GrantTier {
        let target = self.target_bytes();
        if target == 0 {
            return GrantTier::Optimal;
        }
        let alloc = self.allocated_bytes();
        // Water as bps (per 10 000) to avoid float in hot path.
        let water_bps = (alloc.saturating_mul(10_000) / target) as u64;
        // Threshold ladder — chosen so a query starting against an
        // empty pool gets Optimal, a small handful of concurrent ops
        // each get a graceful one-pass grace pass, and the last few
        // arrivals quietly fall back to multi-pass spill.
        //
        // The 90 % bound here and `MemBudget::should_spill`'s 90 % are
        // independent counters (PGA pool vs per-query budget); do not
        // assume they fire at the same time.
        if water_bps < 7_000 {
            GrantTier::Optimal
        } else if water_bps < 9_000 {
            GrantTier::OnePass
        } else {
            GrantTier::Min
        }
    }

    /// Concurrency-fair upper bound a single work area may grow into,
    /// in bytes. This is the estimate-INDEPENDENT replacement for
    /// an optimizer estimate as a growth cap: it is derived purely from
    /// the live executor target and the number of active work areas,
    /// never from CBO cardinality. A lone operator gets (nearly) the
    /// whole pool; as concurrency rises every operator's share shrinks
    /// and the ones that already grew past the new share start spilling
    /// on their next `try_charge`.
    ///
    /// `target / max(1, demanding) × oversubscribe%`. Registered but
    /// still-idle downstream operators do not dilute a running operator's
    /// share. The global hard cap
    /// (`allocated > target`) remains the real ceiling; this only
    /// shapes how the pool is divided among contending operators.
    pub fn fair_share_bytes(&self) -> usize {
        let target = self.target_bytes();
        if target == 0 {
            return usize::MAX;
        }
        let demanding = self.demanding_handles().max(1) as usize;
        let base = target / demanding;
        let scaled = (base as u128 * oversubscribe_pct() as u128 / 100) as usize;
        // Never below the absolute floor, never above the whole pool —
        // a single work area must not be told it may use more than the
        // entire executor target.
        scaled.max(MIN_OP_FLOOR_BYTES).min(target)
    }

    fn try_add_allocated(&self, delta: usize) -> bool {
        if delta == 0 {
            return true;
        }
        let target = self.target_bytes();
        let mut cur = self.allocated_bytes.load(Ordering::Acquire);
        let new_total = loop {
            let Some(next) = cur.checked_add(delta) else {
                return false;
            };
            if target != 0 && next > target {
                return false;
            }
            match self.allocated_bytes.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break next,
                Err(observed) => cur = observed,
            }
        };
        let mut hw = self.high_water_bytes.load(Ordering::Relaxed);
        while new_total > hw {
            match self.high_water_bytes.compare_exchange_weak(
                hw,
                new_total,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => hw = observed,
            }
        }
        true
    }

    fn sub_allocated(&self, delta: usize) {
        atomic_saturating_sub(&self.allocated_bytes, delta);
    }

    fn add_used(&self, delta: usize) {
        let new_total = atomic_saturating_add(&self.used_bytes, delta);
        let mut hw = self.high_water_used_bytes.load(Ordering::Relaxed);
        while new_total > hw {
            match self.high_water_used_bytes.compare_exchange_weak(
                hw,
                new_total,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => hw = observed,
            }
        }
    }

    fn sub_used(&self, delta: usize) {
        atomic_saturating_sub(&self.used_bytes, delta);
    }
}

fn atomic_saturating_add(counter: &AtomicUsize, delta: usize) -> usize {
    let mut cur = counter.load(Ordering::Acquire);
    loop {
        let next = cur.saturating_add(delta);
        match counter.compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return next,
            Err(observed) => cur = observed,
        }
    }
}

/// Snapshot of manager state, suitable for emitting through
/// `SHOW ENGINE MYDB STATUS` and `information_schema.mydb_memory_advisor`.
#[derive(Clone, Debug, Default)]
pub struct WorkAreaSnapshot {
    pub target_bytes: usize,
    pub allocated_bytes: usize,
    pub high_water_bytes: usize,
    pub used_bytes: usize,
    pub high_water_used_bytes: usize,
    pub active_handles: u64,
    pub optimal_grants: u64,
    pub one_pass_grants: u64,
    pub min_grants: u64,
    pub spill_requests: u64,
    pub spill_estimated_bytes: u64,
    pub force_overflow_bytes: u64,
    pub force_overflow_events: u64,
}

static GLOBAL: OnceLock<Arc<WorkAreaManager>> = OnceLock::new();

/// Global singleton handle. Initialised lazily on first access from
/// `mem_budget::executor_memory_target_bytes()`.
pub fn global_manager() -> &'static Arc<WorkAreaManager> {
    GLOBAL.get_or_init(|| {
        Arc::new(WorkAreaManager::new(
            mem_budget::executor_memory_target_bytes(),
        ))
    })
}

/// Re-sync the manager's target from `mem_budget::executor_memory_target_bytes()`.
/// Called after `SET GLOBAL mydb_executor_memory_target_mb = ...`.
pub fn refresh_target_from_env() {
    let m = global_manager();
    m.set_target_bytes(mem_budget::executor_memory_target_bytes());
}

/// Public snapshot accessor — used by advisor view and SHOW ENGINE.
pub fn snapshot() -> WorkAreaSnapshot {
    global_manager().snapshot()
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

/// Per-operator reservation token. Created via [`WorkAreaHandle::register`]
/// or [`WorkAreaHandle::register_with`]; dropped when the operator
/// finishes, returning its grant to the pool.
pub struct WorkAreaHandle {
    mutation: parking_lot::Mutex<()>,
    manager: Arc<WorkAreaManager>,
    /// Bytes currently reserved on the manager's behalf. Starts at
    /// runtime floor, grows with actual usage and shrinks after a spill.
    reserved: AtomicUsize,
    /// Bytes the operator currently holds in-memory (running total
    /// across `try_charge` minus `release`).
    used: AtomicUsize,
    /// Set by the arbiter when the operator should dump its in-memory
    /// state. Cleared by `release()` after the spill.
    spill_requested: AtomicBool,
    /// A dependent blocking consumer may borrow unused global executor
    /// headroom while draining its producer. Once enabled, subsequent batch
    /// charges grow directly against the hard pool instead of repeatedly
    /// tripping the fair-share watermark and inflating spill diagnostics.
    pipeline_borrowing: AtomicBool,
    demanding: AtomicBool,
    released: AtomicBool,
}

impl WorkAreaHandle {
    /// Register a new work area against the global manager.
    pub fn register() -> Self {
        Self::register_with(global_manager().clone())
    }

    /// Test/internal entry: register against an explicit manager.
    pub fn register_with(manager: Arc<WorkAreaManager>) -> Self {
        // Count this work area as active up front so `fair_share_bytes`
        // (used both here and in `try_charge`) divides the pool by the
        // true contender count, self included.
        manager.active_handles.fetch_add(1, Ordering::Relaxed);
        let tier = manager.select_tier();
        // A registration is permission to grow, not touched memory. Reserve
        // only the runtime floor so idle downstream operators cannot pre-grab
        // the pool before they consume their first row.
        let floor = MIN_OP_FLOOR_BYTES.min(manager.fair_share_bytes());
        let reserved = if manager.try_add_allocated(floor) {
            floor
        } else {
            0
        };
        let initially_spilling = reserved == 0;
        if initially_spilling {
            manager.spill_requests.fetch_add(1, Ordering::Relaxed);
        }
        match tier {
            GrantTier::Optimal => {
                manager.optimal_grants.fetch_add(1, Ordering::Relaxed);
            }
            GrantTier::OnePass => {
                manager.one_pass_grants.fetch_add(1, Ordering::Relaxed);
            }
            GrantTier::Min => {
                manager.min_grants.fetch_add(1, Ordering::Relaxed);
            }
        }
        Self {
            mutation: parking_lot::Mutex::new(()),
            manager,
            reserved: AtomicUsize::new(reserved),
            used: AtomicUsize::new(0),
            spill_requested: AtomicBool::new(initially_spilling),
            pipeline_borrowing: AtomicBool::new(false),
            demanding: AtomicBool::new(false),
            released: AtomicBool::new(false),
        }
    }

    #[allow(dead_code)]
    pub fn reserved_bytes(&self) -> usize {
        self.reserved.load(Ordering::Relaxed)
    }
    pub fn used_bytes(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
    pub fn must_spill(&self) -> bool {
        if self.spill_requested.load(Ordering::Relaxed) {
            return true;
        }
        let used = self.used.load(Ordering::Acquire);
        let target = self.manager.target_bytes();
        let target_shrank_below_pool = target > 0 && self.manager.allocated_bytes() > target;
        let fair_share_shrank = !self.pipeline_borrowing.load(Ordering::Acquire)
            && used > self.manager.fair_share_bytes();
        if target_shrank_below_pool || fair_share_shrank {
            if !self.spill_requested.swap(true, Ordering::AcqRel) {
                self.manager.spill_requests.fetch_add(1, Ordering::Relaxed);
            }
            return true;
        }
        false
    }
    pub fn manager(&self) -> &Arc<WorkAreaManager> {
        &self.manager
    }

    /// Try to charge `bytes` toward this work area. Returns a
    /// [`SpillHint`] — the operator is responsible for honouring
    /// `Spill` (write largest partition / run, then `release` the
    /// freed bytes). The call never fails: when the pool is tight
    /// the bytes are still tracked and added to
    /// `force_overflow_bytes` for diagnostics, but the operator must
    /// then spill on the very next iteration to bring usage back
    /// under control.
    pub fn try_charge(&self, bytes: usize) -> SpillHint {
        if bytes == 0 {
            return if self.must_spill() {
                SpillHint::Spill
            } else {
                SpillHint::Continue
            };
        }
        let _mutation = self.mutation.lock();
        if !self.demanding.swap(true, Ordering::AcqRel) {
            self.manager
                .demanding_handles
                .fetch_add(1, Ordering::AcqRel);
        }
        let new_used = atomic_saturating_add(&self.used, bytes);
        self.manager.add_used(bytes);
        // Maybe grow the reservation to cover the new usage. The growth
        // ceiling is the operator's *dynamic fair share* of the pool —
        // NOT `spec.optimal_bytes`. This is the heart of estimate
        // independence: if CBO under-estimated and `optimal` is tiny,
        // the operator can still grow into whatever the pool can
        // currently spare; if CBO over-estimated, we never reserved the
        // slack in the first place. The fair share is recomputed each
        // call so a newly-arrived concurrent operator instantly tightens
        // everyone's ceiling.
        let grow_cap = if self.pipeline_borrowing.load(Ordering::Acquire) {
            let target = self.manager.target_bytes();
            if target == 0 {
                usize::MAX
            } else {
                target
            }
        } else {
            self.manager.fair_share_bytes().max(MIN_OP_FLOOR_BYTES)
        };
        let mut reserved = self.reserved.load(Ordering::Relaxed);
        while new_used > reserved && reserved < grow_cap {
            let want = new_used.min(grow_cap);
            let delta = want.saturating_sub(reserved);
            if !self.manager.try_add_allocated(delta) {
                break; // pool full — fall through to spill request
            }
            match self.reserved.compare_exchange(
                reserved,
                want,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => {
                    self.manager.sub_allocated(delta);
                    reserved = observed;
                }
            }
        }

        let reserved_now = self.reserved.load(Ordering::Relaxed);
        let target = self.manager.target_bytes();
        let allocated = self.manager.allocated_bytes();
        // Strict gates: use ">" not ">=" so a charge that exactly
        // saturates the local grant (or the global target) is still a
        // legitimate Continue — the operator asked for that much and
        // we promised it. Spill kicks in only when an additional
        // charge would push past the boundary.
        let global_high = target > 0 && allocated > target;
        let over_reserved = new_used > reserved_now;

        if over_reserved {
            // Couldn't grow into the pool; track overflow so the
            // advisor can flag chronic under-grants.
            let overflow = (new_used - reserved_now) as u64;
            self.manager
                .force_overflow_bytes
                .fetch_add(overflow, Ordering::Relaxed);
            self.manager
                .force_overflow_events
                .fetch_add(1, Ordering::Relaxed);
        }

        if over_reserved || global_high {
            if !self.spill_requested.swap(true, Ordering::Relaxed) {
                self.manager.spill_requests.fetch_add(1, Ordering::Relaxed);
            }
            return SpillHint::Spill;
        }
        SpillHint::Continue
    }

    /// Let a blocking consumer borrow otherwise idle executor headroom while
    /// it drains a dependent producer.
    ///
    /// The ordinary fair-share cap is appropriate for independent work areas,
    /// but a parent build and its producer overlap by construction: spilling
    /// the complete parent because that overlap is a few batches larger than
    /// `target / demanding_handles` wastes the idle remainder of the same
    /// executor pool. This specialized retry may grow past the fair share, but
    /// `try_add_allocated` still enforces the unchanged global hard target.
    pub fn try_borrow_for_pipeline_drain(&self) -> SpillHint {
        let _mutation = self.mutation.lock();
        let used = self.used.load(Ordering::Acquire);
        let mut reserved = self.reserved.load(Ordering::Relaxed);
        while used > reserved {
            let delta = used - reserved;
            if !self.manager.try_add_allocated(delta) {
                return SpillHint::Spill;
            }
            match self.reserved.compare_exchange(
                reserved,
                used,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.pipeline_borrowing.store(true, Ordering::Release);
                    self.spill_requested.store(false, Ordering::Release);
                    return SpillHint::Continue;
                }
                Err(observed) => {
                    self.manager.sub_allocated(delta);
                    reserved = observed;
                }
            }
        }
        self.pipeline_borrowing.store(true, Ordering::Release);
        self.spill_requested.store(false, Ordering::Release);
        SpillHint::Continue
    }

    /// Release resident memory without attributing ordinary shrink/drop to
    /// disk I/O. Concurrent workers must retain the remaining reservation.
    pub fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let _mutation = self.mutation.lock();
        let actual = atomic_saturating_sub(&self.used, bytes);
        self.manager.sub_used(actual);
        let remaining = self.used.load(Ordering::Acquire);
        if remaining == 0 && self.demanding.swap(false, Ordering::AcqRel) {
            self.manager
                .demanding_handles
                .fetch_sub(1, Ordering::AcqRel);
        }
        // Shrink the reservation back toward the registration floor so
        // the bytes a spill just freed are returned to the pool for
        // other operators. The floor mirrors `register_with`: the
        // partition-derived `min_bytes`, never the cardinality-derived
        // tier baseline.
        let baseline = MIN_OP_FLOOR_BYTES
            .max(remaining)
            .min(self.reserved.load(Ordering::Relaxed));
        let mut cur = self.reserved.load(Ordering::Relaxed);
        loop {
            if cur <= baseline {
                break;
            }
            match self.reserved.compare_exchange(
                cur,
                baseline,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.manager.sub_allocated(cur - baseline);
                    break;
                }
                Err(observed) => cur = observed,
            }
        }
        let target = self.manager.target_bytes();
        let needs_spill = remaining > self.reserved_bytes()
            || (target > 0 && self.manager.allocated_bytes() > target)
            || (!self.pipeline_borrowing.load(Ordering::Acquire)
                && remaining > self.manager.fair_share_bytes());
        self.spill_requested.store(needs_spill, Ordering::Release);
        if remaining == 0 {
            self.pipeline_borrowing.store(false, Ordering::Release);
        }
    }

    pub(crate) fn record_spill(&self, freed_bytes: usize) {
        self.manager
            .spill_estimated_bytes
            .fetch_add(freed_bytes as u64, Ordering::Relaxed);
    }
}

impl Drop for WorkAreaHandle {
    fn drop(&mut self) {
        if self.released.swap(true, Ordering::Relaxed) {
            return;
        }
        let reserved = self.reserved.load(Ordering::Relaxed);
        self.manager.sub_allocated(reserved);
        let used = self.used.load(Ordering::Relaxed);
        if used > 0 {
            self.manager.sub_used(used);
        }
        if self.demanding.swap(false, Ordering::AcqRel) {
            self.manager
                .demanding_handles
                .fetch_sub(1, Ordering::AcqRel);
        }
        self.manager.active_handles.fetch_sub(1, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_manager(target: usize) -> Arc<WorkAreaManager> {
        Arc::new(WorkAreaManager::new(target))
    }

    #[test]
    fn tier_selection_by_water() {
        let m = fresh_manager(1000 * 1024);

        // Empty pool → optimal.
        assert_eq!(m.select_tier(), GrantTier::Optimal);
        // Pre-fill 80 % → one_pass.
        assert!(m.try_add_allocated(800 * 1024));
        assert_eq!(m.select_tier(), GrantTier::OnePass);
        // Pre-fill 95 % → min.
        assert!(m.try_add_allocated(150 * 1024));
        assert_eq!(m.select_tier(), GrantTier::Min);
    }

    #[test]
    fn registration_reserves_only_the_runtime_floor() {
        // Registration keeps the rest of the pool available to concurrent
        // operators until this handle charges actual resident bytes.
        let m = fresh_manager(4 * 1024 * 1024);

        {
            let h = WorkAreaHandle::register_with(m.clone());
            assert_eq!(h.reserved_bytes(), MIN_OP_FLOOR_BYTES); // floor only
            assert_eq!(m.allocated_bytes(), MIN_OP_FLOOR_BYTES);
            assert_eq!(m.active_handles(), 1);
        }
        assert_eq!(m.allocated_bytes(), 0);
        assert_eq!(m.active_handles(), 0);
    }

    #[test]
    fn grows_on_demand_into_free_pool() {
        // A handle needing 3 MiB can grow into an otherwise empty 8 MiB pool.
        let m = fresh_manager(8 * 1024 * 1024);

        let h = WorkAreaHandle::register_with(m.clone());
        assert_eq!(h.try_charge(3 * 1024 * 1024), SpillHint::Continue);
        assert!(!h.must_spill());
        // Growth follows actual demand rather than a fixed initial grant.
        assert!(h.reserved_bytes() >= 3 * 1024 * 1024);
    }

    #[test]
    fn charge_requests_spill_once_grant_is_exceeded() {
        let m = fresh_manager(2 * 1024 * 1024);

        let h = WorkAreaHandle::register_with(m.clone());
        // Filling the optimal reservation exactly is fine — that's
        // what we promised the operator.
        let hint = h.try_charge(1024 * 1024);
        assert_eq!(hint, SpillHint::Continue);
        assert!(!h.must_spill());
        // One byte over the local grant *and* the pool can no longer
        // grow it — must signal spill so the operator sheds memory.
        assert!(m.try_add_allocated(1024 * 1024)); // saturate the global pool
        let hint = h.try_charge(1);
        assert_eq!(hint, SpillHint::Spill);
        assert!(h.must_spill());
        // Release simulates a spill freeing memory.
        h.release(800 * 1024);
        assert!(!h.must_spill());
    }

    #[test]
    fn dependent_pipeline_can_borrow_idle_global_headroom() {
        let target = 6 * 1024 * 1024;
        let m = fresh_manager(target);

        let producer = WorkAreaHandle::register_with(m.clone());
        assert_eq!(producer.try_charge(2 * 1024 * 1024), SpillHint::Continue);
        let parent = WorkAreaHandle::register_with(m.clone());
        assert_eq!(parent.try_charge(2_900 * 1024), SpillHint::Continue);
        assert_eq!(parent.try_charge(200 * 1024), SpillHint::Spill);
        assert_eq!(parent.try_borrow_for_pipeline_drain(), SpillHint::Continue);
        assert!(m.allocated_bytes() <= target);
        assert!(parent.reserved_bytes() >= parent.used_bytes());
        drop(producer);
    }

    #[test]
    fn over_reserved_charge_records_overflow_but_does_not_fail() {
        // With a *saturated* pool the operator cannot grow, so a charge
        // beyond its floor must signal Spill and record the overflow —
        // never panic / 1037.
        let m = fresh_manager(2 * 1024 * 1024);

        let h = WorkAreaHandle::register_with(m.clone());
        assert!(m.try_add_allocated(2 * 1024 * 1024 - h.reserved_bytes()));
        let hint = h.try_charge(1024 * 1024);
        assert_eq!(hint, SpillHint::Spill);
        assert!(m.force_overflow_bytes() > 0);
    }

    #[test]
    fn small_target_still_grants_min_floor() {
        // Even with a tiny target, the arbiter never grants 0.
        let m = fresh_manager(64 * 1024);

        let h = WorkAreaHandle::register_with(m.clone());
        assert!(h.reserved_bytes() >= 64 * 1024);
    }

    #[test]
    fn fair_share_counts_consumers_not_idle_registrations() {
        // Registration alone does not consume memory. The first real
        // consumer may use the whole pool; once a second handle also charges,
        // each handle's fair share roughly halves.
        let target = 8 * 1024 * 1024;
        let m = fresh_manager(target);

        assert_eq!(m.fair_share_bytes(), target); // no handles → whole pool
        let h1 = WorkAreaHandle::register_with(m.clone());
        let h2 = WorkAreaHandle::register_with(m.clone());
        assert_eq!(m.fair_share_bytes(), target); // both idle → whole pool
        assert_eq!(h1.try_charge(64 * 1024), SpillHint::Continue);
        assert_eq!(m.fair_share_bytes(), target); // 1 consumer → whole pool
        assert_eq!(h2.try_charge(64 * 1024), SpillHint::Continue);
        // 2 consumers → ~half each.
        assert_eq!(m.fair_share_bytes(), target / 2);
        h2.release(64 * 1024);
        assert_eq!(m.fair_share_bytes(), target);
    }

    #[test]
    fn concurrent_growth_respects_global_cap() {
        // Two operators each try to grow large in a pool that can only
        // hold one. The pool's hard cap (allocated <= target) must hold:
        // their combined reservation never exceeds the target, and the
        // loser is told to spill.
        let target = 4 * 1024 * 1024;
        let m = fresh_manager(target);

        let h1 = WorkAreaHandle::register_with(m.clone());
        let h2 = WorkAreaHandle::register_with(m.clone());
        // Each may grow only to its fair share (~2 MiB) before spilling.
        let _ = h1.try_charge(3 * 1024 * 1024);
        let _ = h2.try_charge(3 * 1024 * 1024);
        assert!(
            m.allocated_bytes() <= target,
            "allocated {} exceeded target {}",
            m.allocated_bytes(),
            target
        );
        // At least one of them was pushed to spill.
        assert!(h1.must_spill() || h2.must_spill());
    }

    #[test]
    fn concurrent_reservation_is_atomic() {
        let target = 1024 * 1024;
        let manager = fresh_manager(target);
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let wins = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|scope| {
            for _ in 0..2 {
                let manager = Arc::clone(&manager);
                let barrier = Arc::clone(&barrier);
                let wins = Arc::clone(&wins);
                scope.spawn(move || {
                    barrier.wait();
                    if manager.try_add_allocated(800 * 1024) {
                        wins.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
            barrier.wait();
        });
        assert_eq!(wins.load(Ordering::Relaxed), 1);
        assert!(manager.allocated_bytes() <= target);
        manager.sub_allocated(target);
        assert_eq!(manager.allocated_bytes(), 0);
    }

    #[test]
    fn existing_handle_observes_runtime_target_shrink() {
        let manager = fresh_manager(32 * 1024 * 1024);

        let handle = WorkAreaHandle::register_with(Arc::clone(&manager));
        assert_eq!(handle.try_charge(20 * 1024 * 1024), SpillHint::Continue);
        manager.set_target_bytes(16 * 1024 * 1024);
        assert!(handle.must_spill());
        let newcomer = WorkAreaHandle::register_with(Arc::clone(&manager));
        assert!(newcomer.must_spill());
    }

    #[test]
    fn idle_registration_does_not_starve_a_later_operator() {
        // An idle first registration reserves only the runtime floor,
        // leaving the second handle room to grow into its fair share.
        let target = 8 * 1024 * 1024;
        let m = fresh_manager(target);

        let h1 = WorkAreaHandle::register_with(m.clone());
        assert_eq!(h1.reserved_bytes(), MIN_OP_FLOOR_BYTES);

        let h2 = WorkAreaHandle::register_with(m.clone());
        // h2 can still grow to ~2 MiB — the pool wasn't pre-grabbed.
        assert_eq!(h2.try_charge(2 * 1024 * 1024), SpillHint::Continue);
    }
}

#[cfg(test)]
mod spill_contract_regressions {
    use super::*;

    #[test]
    fn partial_release_retains_live_reservation() {
        let manager = Arc::new(WorkAreaManager::new(8 * 1024 * 1024));
        let area = WorkAreaHandle::register_with(manager.clone());
        assert_eq!(area.try_charge(1024 * 1024), SpillHint::Continue);
        area.release(256 * 1024);
        assert_eq!(area.used_bytes(), 768 * 1024);
        assert_eq!(area.reserved_bytes(), 768 * 1024);
        assert_eq!(manager.allocated_bytes(), 768 * 1024);
    }

    #[test]
    fn ordinary_release_does_not_record_spill() {
        let manager = Arc::new(WorkAreaManager::new(8 * 1024 * 1024));
        let area = WorkAreaHandle::register_with(manager.clone());
        assert_eq!(area.try_charge(1024 * 1024), SpillHint::Continue);
        area.release(1024 * 1024);
        assert_eq!(manager.spill_estimated_bytes(), 0);
    }
}
