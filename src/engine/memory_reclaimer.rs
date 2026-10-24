//! Idle-time allocator pressure relief.
//!
//! Rust containers release their allocations when the last query/cursor owner
//! drops, but a general-purpose allocator may retain the now-free pages. This
//! module coalesces statement-completion notifications and performs allocator
//! relief on one background thread, never on the client response path.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};

thread_local! {
    static SPILL_ALLOCATOR_RECLAIM: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(crate) fn request_spill_allocator_reclaim() {
    SPILL_ALLOCATOR_RECLAIM.with(|requested| requested.set(true));
}

/// Spillable admission can release its own state before requesting allocator
/// relief. Collect on that state's owning thread, after its grant shrinks,
/// without taking the process-wide reclaim lock or waiting on sibling workers.
pub(crate) fn reclaim_spill_allocator_if_requested() {
    if SPILL_ALLOCATOR_RECLAIM.with(|requested| requested.replace(false)) {
        reclaim_current_thread_allocator();
    }
}

/// Return free pages on their heap's owner before it can block behind a
/// coordinator. Collection never acquires database/cache admission locks.
pub(crate) fn reclaim_current_thread_allocator() {
    allocator_pressure_relief();
    reset_pressure_allowance();
}
use std::time::{Duration, Instant};

const IDLE_DELAY: Duration = Duration::from_millis(250);
const RETRY_DELAYS: [Duration; 2] = [Duration::from_millis(750), Duration::from_secs(2)];
const TRIGGER_PCT: u64 = 110;
const MIN_HEADROOM_RECLAIM_PEAK_BYTES: usize = 8 * 1024 * 1024;
/// A statement this long is likely analytical/bulk work whose temporary
/// containers can leave allocator arenas resident even after every governed
/// owner has dropped. Arm the existing next-statement footprint check so a
/// sequential client regains the executor headroom before starting more work.
const LONG_STATEMENT_RECLAIM_THRESHOLD: Duration = Duration::from_millis(50);
/// Re-sample OS pressure after at most this much newly admitted resident
/// memory. This bounds drift from file-cache/runtime growth without putting a
/// `/proc` read on every container push.
#[cfg(not(test))]
const MAX_PRESSURE_ALLOWANCE_BYTES: usize = 64 * 1024 * 1024;

static ACTIVE_STATEMENTS: AtomicUsize = AtomicUsize::new(0);
static ENABLED: AtomicBool = AtomicBool::new(true);
static ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static SUCCESSES: AtomicU64 = AtomicU64::new(0);
static SKIPPED_ACTIVE: AtomicU64 = AtomicU64::new(0);
static UNSUPPORTED: AtomicU64 = AtomicU64::new(0);
static LAST_BEFORE: AtomicU64 = AtomicU64::new(0);
static LAST_AFTER: AtomicU64 = AtomicU64::new(0);
static LAST_RELEASED: AtomicU64 = AtomicU64::new(0);
static LAST_DURATION_US: AtomicU64 = AtomicU64::new(0);
/// A recently completed query owned enough work-area memory that allocator
/// retention could consume the headroom promised to the next query.  The
/// next idle -> active transition checks the real process footprint before
/// starting execution. Keeping the reasons in a small atomic hint avoids reading
/// `/proc/self/smaps_rollup` on ordinary OLTP statements.
const RECLAIM_LONG_STATEMENT: u8 = 1;
const RECLAIM_MEMORY_PEAK: u8 = 2;
const TIME_ONLY_RECLAIM_INTERVAL: Duration = Duration::from_secs(2);
const TIME_ONLY_RECLAIM_GROWTH: u64 = 64 * 1024 * 1024;
static HEADROOM_RECLAIM_PENDING: AtomicU8 = AtomicU8::new(0);
static LAST_STATEMENT_RECLAIM: Mutex<Option<(Instant, u64)>> = Mutex::new(None);
#[cfg(not(test))]
static PRESSURE_ALLOWANCE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PHYSICAL_PRESSURE_REJECTIONS: AtomicU64 = AtomicU64::new(0);
#[cfg(not(test))]
static PRESSURE_SAMPLE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static CACHE_HEADROOM_REQUESTED: AtomicUsize = AtomicUsize::new(0);

struct WakeState {
    generation: u64,
    last_activity: Instant,
    wake_on_activity: bool,
}

impl WakeState {
    fn new() -> Self {
        Self {
            generation: 0,
            last_activity: Instant::now(),
            wake_on_activity: false,
        }
    }

    fn record_activity(&mut self) -> bool {
        self.generation = self.generation.wrapping_add(1);
        self.last_activity = Instant::now();
        self.wake_on_activity
    }
}

struct Reclaimer {
    state: Mutex<WakeState>,
    wake: Condvar,
}

static RECLAIMER: OnceLock<Reclaimer> = OnceLock::new();
static WORKER_STARTED: OnceLock<()> = OnceLock::new();
static CONFIG_INITIALIZED: OnceLock<()> = OnceLock::new();
static WORKER_RECLAIM_EPOCH: AtomicU64 = AtomicU64::new(0);
static WORKER_RECLAIM_COMPLETED: AtomicU64 = AtomicU64::new(0);

thread_local! {
    // mi_collect visits the calling thread's heap. A background collector
    // cannot retire free pages still owned by an idle blocking worker.
    static WORKER_RECLAIM_PENDING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static WORKER_RECLAIM_SEEN: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

pub(crate) fn note_worker_memory_release(bytes: usize) {
    if bytes >= MIN_HEADROOM_RECLAIM_PEAK_BYTES {
        WORKER_RECLAIM_PENDING.with(|pending| pending.set(true));
    }
}

/// Identify persistent pool workers to the allocator. Otherwise freeing a
/// short-lived worker's partial states can adopt its abandoned pages into a
/// coordinator that then parks indefinitely with mostly empty pages.
pub(crate) fn mark_allocator_thread_pool_worker() {
    #[cfg(feature = "fast-alloc")]
    {
        unsafe extern "C" {
            fn mi_thread_set_in_threadpool();
        }
        // SAFETY: this public mimalloc API affects only the calling thread's
        // allocator lifecycle; it changes neither limits nor thread counts.
        unsafe { mi_thread_set_in_threadpool() };
    }
}

/// Collect on the allocation owner's thread at a completed phase/task
/// boundary. Ordinary small tasks perform no OS sampling or allocator walk.
/// Active peers retain their heaps; all admission ceilings remain unchanged.
pub(crate) fn reclaim_worker_memory_after_phase() {
    let epoch = WORKER_RECLAIM_EPOCH.load(Ordering::Acquire);
    let requested = WORKER_RECLAIM_SEEN.with(|seen| seen.replace(epoch) != epoch);
    if requested {
        // A coordinator can free allocations originally made by this worker
        // after it has parked. Collect remote frees on the owner before it
        // parks again; live allocations and admission limits are untouched.
        allocator_pressure_relief();
        WORKER_RECLAIM_COMPLETED.fetch_add(1, Ordering::Release);
    }
    if !WORKER_RECLAIM_PENDING.with(|pending| pending.replace(false)) || !enabled() {
        return;
    }
    let Some(current) = current_nonreclaimable_pressure_bytes() else {
        return;
    };
    let ceiling = statement_memory_ceiling(
        crate::engine::executor::mem_budget::memory_target_bytes() as u64,
        crate::engine::executor::mem_budget::operational_memory_limit_bytes() as u64,
        RESERVED_RUNTIME_CAPACITY.load(Ordering::Acquire),
    );
    if should_reclaim_statement_headroom(
        current,
        ceiling,
        crate::engine::executor::mem_budget::executor_memory_target_bytes() as u64,
    ) {
        reclaim_now();
        reset_pressure_allowance();
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ReclaimerSnapshot {
    pub active_statements: u64,
    pub attempts: u64,
    pub successes: u64,
    pub skipped_active: u64,
    pub unsupported: u64,
    pub last_before_bytes: u64,
    pub last_after_bytes: u64,
    pub last_released_bytes: u64,
    pub last_duration_us: u64,
}

fn global() -> &'static Reclaimer {
    RECLAIMER.get_or_init(|| Reclaimer {
        state: Mutex::new(WakeState::new()),
        wake: Condvar::new(),
    })
}

fn initialize_config() {
    CONFIG_INITIALIZED.get_or_init(|| {
        let enabled = crate::util::env::var("MYDB_IDLE_MEMORY_RECLAIM")
            .ok()
            .map(|value| {
                !matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "0" | "off" | "false" | "no"
                )
            })
            .unwrap_or(true);
        ENABLED.store(enabled, Ordering::Release);
    });
}

fn ensure_worker() {
    initialize_config();
    WORKER_STARTED.get_or_init(|| {
        let _ = std::thread::Builder::new()
            .name("mydb-memory-reclaimer".to_string())
            .spawn(worker_loop);
    });
}

fn notify() {
    ensure_worker();
    let reclaimer = global();
    if let Ok(mut state) = reclaimer.state.lock() {
        if state.record_activity() {
            reclaimer.wake.notify_one();
        }
    }
}

/// Covers execution plus result encoding/socket write. It is intentionally
/// process-global: allocator relief only runs after every client command is
/// out of its latency-sensitive section.
pub(crate) struct StatementActivityGuard {
    started: Instant,
}

impl StatementActivityGuard {
    pub(crate) fn enter() -> Self {
        ensure_worker();
        let previous = ACTIVE_STATEMENTS.fetch_add(1, Ordering::AcqRel);
        // Only the idle -> active transition can invalidate a pending reclaim
        // attempt. Once one statement is already active, the worker has
        // either observed activity or will observe the non-zero counter
        // before reclaiming. Avoiding a global mutex + condvar notification
        // for every additional concurrent statement is important on
        // prepared-statement OLTP paths.
        if should_notify_activity_start(previous) {
            #[cfg(not(test))]
            refresh_memory_limit_for_statement_start();
            reclaim_statement_headroom_if_requested();
            notify();
        }
        Self {
            started: Instant::now(),
        }
    }
}

#[inline]
fn should_notify_activity_start(previous_active: usize) -> bool {
    previous_active == 0
}

/// Minimum spacing between host/cgroup limit re-reads on the statement-start
/// path. A low-concurrency client makes an idle -> active transition before
/// nearly every statement, and each refresh reads `/proc/meminfo` plus the
/// cgroup limit files. Limits change only through operator action, so a
/// one-second staleness bound keeps resizes visible without per-statement I/O.
#[cfg(not(test))]
const STATEMENT_LIMIT_REFRESH_INTERVAL_MS: u64 = 1_000;

#[cfg(not(test))]
fn refresh_memory_limit_for_statement_start() {
    static LAST_REFRESH_MS: AtomicU64 = AtomicU64::new(u64::MAX);
    let now = crate::util::coarse_clock::coarse_elapsed_ms();
    let last = LAST_REFRESH_MS.load(Ordering::Relaxed);
    if last != u64::MAX && now.saturating_sub(last) < STATEMENT_LIMIT_REFRESH_INTERVAL_MS {
        return;
    }
    if LAST_REFRESH_MS
        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        crate::engine::executor::mem_budget::refresh_effective_memory_limit_bytes();
    }
}

impl Drop for StatementActivityGuard {
    fn drop(&mut self) {
        if should_request_reclaim_after_statement(self.started.elapsed()) {
            HEADROOM_RECLAIM_PENDING.fetch_or(RECLAIM_LONG_STATEMENT, Ordering::Release);
        }
        let prior = ACTIVE_STATEMENTS.fetch_sub(1, Ordering::AcqRel);
        if prior <= 1 {
            notify();
        }
    }
}

#[inline]
fn should_request_reclaim_after_statement(elapsed: Duration) -> bool {
    elapsed >= LONG_STATEMENT_RECLAIM_THRESHOLD
}

/// Fallback completion signal for embedded execution and for server-side
/// cursors whose last worker/budget owner can outlive COM_STMT_EXECUTE.
///
/// A substantial peak also arms a one-shot statement-start check. Sequential
/// analytical clients commonly leave less than the ordinary 250ms idle
/// window between queries, so allocator arenas from several already-finished
/// work areas can otherwise accumulate until the next large operator is
/// killed by the kernel despite all live memory being within its governor.
pub(crate) fn notify_query_memory_released(query_peak_bytes: usize) {
    if should_request_headroom_reclaim(
        query_peak_bytes,
        crate::engine::executor::mem_budget::executor_memory_target_bytes(),
    ) {
        HEADROOM_RECLAIM_PENDING.fetch_or(RECLAIM_MEMORY_PEAK, Ordering::Release);
    }
    // Protocol activity already signals its final idle boundary. Only
    // embedded execution or a cursor/budget released after that boundary
    // needs another notification. Keep the headroom hint above unconditional.
    if ACTIVE_STATEMENTS.load(Ordering::Acquire) == 0 {
        notify();
    }
}

#[inline]
fn should_request_headroom_reclaim(query_peak_bytes: usize, executor_target: usize) -> bool {
    let threshold = (executor_target / 32).max(MIN_HEADROOM_RECLAIM_PEAK_BYTES);
    query_peak_bytes >= threshold
}

#[inline]
fn statement_headroom_floor(memory_target: u64, executor_target: u64) -> u64 {
    memory_target.saturating_sub(executor_target)
}

fn statement_memory_ceiling(target: u64, operational_limit: u64, reserved_runtime: u64) -> u64 {
    target.min(pressure_limit_after_runtime_reservation(
        operational_limit,
        reserved_runtime,
    ))
}

#[inline]
fn should_reclaim_statement_headroom(
    current: u64,
    memory_target: u64,
    executor_target: u64,
) -> bool {
    memory_target != 0 && current > statement_headroom_floor(memory_target, executor_target)
}

/// Reclaim allocator-cached pages before the first statement in a new active
/// interval when a previous memory-heavy query may have consumed the next
/// query's executor headroom. Mimalloc collects each heap on its owning
/// worker; collecting only the protocol/coordinator heap leaves remote frees
/// resident across consecutive statements.
fn reclaim_statement_headroom_if_requested() {
    if !enabled() {
        return;
    }
    let reasons = HEADROOM_RECLAIM_PENDING.swap(0, Ordering::AcqRel);
    if reasons == 0 {
        return;
    }
    // Clean file cache can be reclaimed by the kernel and cannot be freed by
    // malloc_trim. Including it here forced allocator-wide trims between
    // scan-heavy statements even when anonymous memory left full headroom.
    let Some(current) = current_nonreclaimable_pressure_bytes() else {
        return;
    };
    // Use the same physical ceiling as allocation admission. In a small
    // cgroup, reserved worker stacks can make it substantially lower than
    // the advisory target; that unavailable capacity cannot be promised to
    // the next statement's executor work area.
    let memory_target = statement_memory_ceiling(
        crate::engine::executor::mem_budget::memory_target_bytes() as u64,
        crate::engine::executor::mem_budget::operational_memory_limit_bytes() as u64,
        RESERVED_RUNTIME_CAPACITY.load(Ordering::Acquire),
    );
    let executor_target =
        crate::engine::executor::mem_budget::executor_memory_target_bytes() as u64;
    if should_reclaim_statement_headroom(current, memory_target, executor_target) {
        let previous = LAST_STATEMENT_RECLAIM
            .lock()
            .ok()
            .and_then(|last| last.map(|(at, pressure)| (at.elapsed(), pressure)));
        if coalesce_time_only_reclaim(reasons, current, memory_target, previous) {
            return;
        }
        let released = reclaim_using(release_allocator_cached_memory);
        if let Ok(mut last) = LAST_STATEMENT_RECLAIM.lock() {
            *last = Some((Instant::now(), current.saturating_sub(released)));
        }
    }
}

/// Latency alone does not prove allocator retention. Under stable pressure,
/// repeatedly trimming after an I/O/lock stall can itself produce more long
/// statements. Large work-area releases and real footprint growth bypass this
/// debounce; the physical/logical admission governors are unchanged.
fn coalesce_time_only_reclaim(
    reasons: u8,
    current: u64,
    target: u64,
    previous: Option<(Duration, u64)>,
) -> bool {
    reasons & RECLAIM_MEMORY_PEAK == 0
        && target != 0
        && current < target
        && previous.is_some_and(|(elapsed, pressure)| {
            elapsed < TIME_ONLY_RECLAIM_INTERVAL
                && current.saturating_sub(pressure) < TIME_ONLY_RECLAIM_GROWTH
        })
}

pub(crate) fn set_enabled(enabled: bool) {
    initialize_config();
    ENABLED.store(enabled, Ordering::Release);
    notify();
}

pub(crate) fn enabled() -> bool {
    initialize_config();
    ENABLED.load(Ordering::Acquire)
}

pub(crate) fn snapshot() -> ReclaimerSnapshot {
    ReclaimerSnapshot {
        active_statements: ACTIVE_STATEMENTS.load(Ordering::Relaxed) as u64,
        attempts: ATTEMPTS.load(Ordering::Relaxed),
        successes: SUCCESSES.load(Ordering::Relaxed),
        skipped_active: SKIPPED_ACTIVE.load(Ordering::Relaxed),
        unsupported: UNSUPPORTED.load(Ordering::Relaxed),
        last_before_bytes: LAST_BEFORE.load(Ordering::Relaxed),
        last_after_bytes: LAST_AFTER.load(Ordering::Relaxed),
        last_released_bytes: LAST_RELEASED.load(Ordering::Relaxed),
        last_duration_us: LAST_DURATION_US.load(Ordering::Relaxed),
    }
}

pub(crate) fn physical_pressure_rejections() -> u64 {
    PHYSICAL_PRESSURE_REJECTIONS.load(Ordering::Relaxed)
}

/// Dirty spill pages become reclaimable after the already-submitted writes
/// complete. Only physical-admission retries consult this cold-path sample.
pub(crate) fn pending_writeback_pressure_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        let path = current_linux_cgroup_v2_path()
            .or_else(current_linux_cgroup_v1_path)
            .map(|path| path.join("memory.stat"));
        if let Some(stat) = path.and_then(|path| std::fs::read_to_string(path).ok()) {
            return parse_writeback_pressure_bytes(&stat);
        }
    }
    0
}

#[cfg(any(test, target_os = "linux"))]
fn parse_writeback_pressure_bytes(stat: &str) -> u64 {
    let value = |wanted: &str| {
        stat.lines().find_map(|line| {
            let mut fields = line.split_whitespace();
            (fields.next()? == wanted)
                .then(|| fields.next()?.parse::<u64>().ok())
                .flatten()
        })
    };
    value("file_dirty")
        .or_else(|| value("total_dirty"))
        .or_else(|| value("dirty"))
        .unwrap_or(0)
        .saturating_add(
            value("file_writeback")
                .or_else(|| value("total_writeback"))
                .or_else(|| value("writeback"))
                .unwrap_or(0),
        )
}

// Native stacks are reserved before workers can be admitted. Until those
// stacks are touched, RSS cannot see them, so physical admission must also
// retain their promised capacity instead of lending it to later allocations.
static RESERVED_RUNTIME_CAPACITY: AtomicU64 = AtomicU64::new(0);

pub(crate) fn reserve_runtime_capacity(bytes: usize) {
    RESERVED_RUNTIME_CAPACITY.fetch_add(bytes as u64, Ordering::AcqRel);
    reset_pressure_allowance();
}

fn pressure_limit_after_runtime_reservation(limit: u64, reserved: u64) -> u64 {
    limit.saturating_sub(reserved)
}

pub(crate) fn reset_pressure_allowance() {
    #[cfg(not(test))]
    PRESSURE_ALLOWANCE_BYTES.store(0, Ordering::Release);
}

#[cfg(not(test))]
fn try_take_pressure_allowance(bytes: usize) -> bool {
    let mut current = PRESSURE_ALLOWANCE_BYTES.load(Ordering::Acquire);
    loop {
        if current < bytes {
            return false;
        }
        match PRESSURE_ALLOWANCE_BYTES.compare_exchange_weak(
            current,
            current - bytes,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return true,
            Err(observed) => current = observed,
        }
    }
}

/// Admit a forthcoming governed allocation against the real process/cgroup
/// footprint. The logical governor remains the authority for ownership and
/// release; this conservative allowance exists to cover allocator-retained
/// pages and other resident memory that logical live-byte accounting cannot
/// see. Released logical reservations intentionally do not replenish the
/// allowance because the allocator may retain their pages.
#[cfg(not(test))]
pub(crate) fn try_reserve_process_headroom(
    bytes: usize,
    pressure_limit_bytes: usize,
    record_rejection: bool,
) -> Result<(), crate::types::QueryError> {
    if bytes == 0 || try_take_pressure_allowance(bytes) {
        return Ok(());
    }
    let sample_lock = PRESSURE_SAMPLE_LOCK.get_or_init(|| Mutex::new(()));
    let _sample = sample_lock.lock().map_err(|_| {
        crate::storage::error(
            crate::error::catalog::ER_OUTOFMEMORY,
            "memory pressure sampler is unavailable",
        )
    })?;
    if try_take_pressure_allowance(bytes) {
        return Ok(());
    }

    let effective = effective_pressure_limit_bytes(pressure_limit_bytes);
    let used = current_nonreclaimable_pressure_bytes().unwrap_or(0);
    if cache_headroom_reclaim_bytes(bytes) != 0 {
        // The caller services this hint only after leaving the sampler and
        // outside storage latches. Optional eviction must never recurse into
        // admission while holding the physical sampling lock.
        CACHE_HEADROOM_REQUESTED.fetch_max(bytes.max(1), Ordering::Release);
    }
    let allowance = pressure_allowance_after_admission(used, bytes as u64, effective);
    let Some(remaining) = allowance else {
        if record_rejection {
            PHYSICAL_PRESSURE_REJECTIONS.fetch_add(1, Ordering::Relaxed);
        }
        return Err(crate::storage::error(
            crate::error::catalog::ER_OUTOFMEMORY,
            format!(
                "process memory pressure limit exceeded: tried to allocate {} bytes, used {} / {}{}",
                bytes, used, effective, pressure_error_breakdown()
            ),
        ));
    };

    // All callers share this fast-path allowance. A compensation allocation
    // may use the higher ceiling, but must never lend its emergency headroom
    // to a subsequent ordinary allocation on another thread.
    let ordinary_limit = effective_pressure_limit_bytes(
        crate::engine::executor::mem_budget::operational_memory_limit_bytes(),
    );
    let shared = shared_pressure_allowance(remaining, effective, ordinary_limit);
    PRESSURE_ALLOWANCE_BYTES.store(
        shared.min(MAX_PRESSURE_ALLOWANCE_BYTES as u64) as usize,
        Ordering::Release,
    );
    Ok(())
}

#[cfg(not(test))]
fn effective_pressure_limit_bytes(pressure_limit_bytes: usize) -> u64 {
    let refreshed = crate::engine::executor::mem_budget::refresh_effective_memory_limit_bytes();
    let configured = pressure_limit_bytes.min(refreshed) as u64;
    let limit = crate::util::system_memory::effective_memory_limit_bytes()
        .map(|system| system.min(configured))
        .unwrap_or(configured);
    pressure_limit_after_runtime_reservation(
        limit,
        RESERVED_RUNTIME_CAPACITY.load(Ordering::Acquire),
    )
}

/// Bytes of real, non-reclaimable process pressure that must be returned
/// before a governed allocation can fit below `pressure_limit_bytes`.
///
/// Logical controlled-memory usage can already be below its SQL ceiling while
/// allocator arenas and non-reclaimable kernel state keep the cgroup above the
/// managed ceiling. Admission reclaim uses this physical deficit to evict optional
/// buffer frames instead of incorrectly choosing a no-op logical target.
#[cfg(not(test))]
pub(crate) fn process_headroom_reclaim_bytes(
    requested_bytes: usize,
    pressure_limit_bytes: usize,
) -> usize {
    let effective = effective_pressure_limit_bytes(pressure_limit_bytes);
    let used = current_nonreclaimable_pressure_bytes().unwrap_or(0);
    headroom_reclaim_bytes_for_values(used, requested_bytes as u64, effective)
        .min(usize::MAX as u64) as usize
}

#[cfg(test)]
pub(crate) fn process_headroom_reclaim_bytes(
    _requested_bytes: usize,
    _pressure_limit_bytes: usize,
) -> usize {
    0
}

#[inline]
fn headroom_reclaim_bytes_for_values(used: u64, requested: u64, limit: u64) -> u64 {
    used.saturating_add(requested).saturating_sub(limit)
}

#[inline]
fn shared_pressure_allowance(remaining: u64, admitted_limit: u64, ordinary_limit: u64) -> u64 {
    remaining.saturating_sub(admitted_limit.saturating_sub(ordinary_limit))
}

#[inline]
fn pressure_allowance_after_admission(used: u64, requested: u64, limit: u64) -> Option<u64> {
    used.checked_add(requested)
        .filter(|next| *next <= limit)
        .map(|next| limit - next)
}

#[cfg(test)]
pub(crate) fn try_reserve_process_headroom(
    _bytes: usize,
    _pressure_limit_bytes: usize,
    _record_rejection: bool,
) -> Result<(), crate::types::QueryError> {
    Ok(())
}

fn worker_loop() {
    let reclaimer = global();
    let mut seen_generation = 0u64;
    loop {
        let Ok(mut state) = reclaimer.state.lock() else {
            return;
        };
        while state.generation == seen_generation {
            state.wake_on_activity = true;
            let Ok(next) = reclaimer.wake.wait(state) else {
                return;
            };
            state = next;
            state.wake_on_activity = false;
        }
        // Once a timer is armed, producers only advance its deadline. Waking
        // an OS thread twice per short SQL statement defeats the debounce.
        // Rechecking the latest deadline at expiry preserves the same quiet
        // period, including the final completion of a sustained SQL burst.
        let Some(state) = wait_for_quiet_period(reclaimer, state, IDLE_DELAY) else {
            return;
        };
        seen_generation = state.generation;
        drop(state);
        if !enabled() {
            continue;
        }
        if ACTIVE_STATEMENTS.load(Ordering::Acquire) != 0 {
            SKIPPED_ACTIVE.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        try_reclaim();
        for delay in RETRY_DELAYS {
            let Ok(mut state) = reclaimer.state.lock() else {
                return;
            };
            if state.generation != seen_generation {
                break;
            }
            let generation = state.generation;
            state.wake_on_activity = true;
            let Ok((mut state, timeout)) = reclaimer.wake.wait_timeout(state, delay) else {
                return;
            };
            state.wake_on_activity = false;
            let changed = !timeout.timed_out() || state.generation != generation;
            drop(state);
            if changed || ACTIVE_STATEMENTS.load(Ordering::Acquire) != 0 || !enabled() {
                break;
            }
            if !above_trigger() {
                break;
            }
            try_reclaim();
        }
    }
}

fn wait_for_quiet_period<'a>(
    reclaimer: &'a Reclaimer,
    mut state: std::sync::MutexGuard<'a, WakeState>,
    quiet: Duration,
) -> Option<std::sync::MutexGuard<'a, WakeState>> {
    state.wake_on_activity = false;
    loop {
        let remaining = quiet.saturating_sub(state.last_activity.elapsed());
        if remaining.is_zero() {
            return Some(state);
        }
        state = reclaimer.wake.wait_timeout(state, remaining).ok()?.0;
    }
}

fn above_trigger() -> bool {
    let Some(current) = current_pressure_footprint_bytes() else {
        return false;
    };
    let target = crate::engine::executor::mem_budget::memory_target_bytes() as u64;
    current > target.saturating_mul(TRIGGER_PCT) / 100
}

/// Footprint used only for the pressure decision. On Linux the cgroup hard
/// limit counts resident file pages and kernel memory as well as allocator
/// arenas; looking only at smaps Private_* can therefore decide that 6.5 GiB
/// is safe while `memory.current` is already 9.7 GiB of a 10 GiB memcg. The
/// allocator-release accounting itself remains private-footprint based.
fn current_pressure_footprint_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    if let Some(current) = current_linux_cgroup_memory_bytes() {
        return Some(current);
    }
    current_private_footprint_bytes()
}

/// Pressure that cannot be expected to disappear through ordinary kernel
/// clean-page reclaim. cgroup `memory.current` is the OOM accounting source,
/// while clean filesystem pages are reclaimable whether currently active or
/// inactive. Counting active clean pages as pinned made a scan-heavy SF=10
/// workload reject allocations with 1037 while the cgroup reported no OOM and
/// held several GiB of clean page cache. Anonymous allocator arenas, shmem,
/// dirty pages and pages under writeback remain included.
fn current_nonreclaimable_pressure_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    if let Some(current) = current_linux_cgroup_memory_bytes() {
        return Some(nonreclaimable_pressure_bytes(
            current,
            current_linux_cgroup_reclaimable_bytes().unwrap_or(0),
        ));
    }
    current_private_footprint_bytes()
}

#[cfg(not(test))]
fn pressure_error_breakdown() -> String {
    #[cfg(target_os = "linux")]
    {
        let path = current_linux_cgroup_v2_path()
            .map(|path| path.join("memory.stat"))
            .or_else(|| current_linux_cgroup_v1_path().map(|path| path.join("memory.stat")));
        if let Some(stat) = path.and_then(|path| std::fs::read_to_string(path).ok()) {
            let mut output = String::from("; cgroup");
            for wanted in [
                "anon",
                "file_dirty",
                "file_writeback",
                "shmem",
                "kernel",
                "slab_reclaimable",
            ] {
                if let Some(value) = stat.lines().find_map(|line| {
                    let (name, value) = line.split_once(' ')?;
                    (name == wanted).then_some(value.trim())
                }) {
                    output.push_str(&format!(" {wanted}={value}"));
                }
            }
            return output;
        }
    }
    String::new()
}

#[inline]
#[cfg(any(test, target_os = "linux"))]
fn nonreclaimable_pressure_bytes(current: u64, reclaimable: u64) -> u64 {
    current.saturating_sub(reclaimable)
}

pub(crate) fn cache_headroom_requested() -> bool {
    CACHE_HEADROOM_REQUESTED.load(Ordering::Acquire) != 0
}

pub(crate) fn take_cache_headroom_request() -> usize {
    CACHE_HEADROOM_REQUESTED.swap(0, Ordering::AcqRel)
}

/// Clean file pages are reclaimable, but not guaranteed to disappear before
/// the next anonymous-page fault. Reclaim optional internal copies while the
/// *total* cgroup footprint approaches its hard limit. This is an eviction
/// hint, not a second hard limit or a reason to reject a valid allocation.
pub(crate) fn cache_headroom_reclaim_bytes(requested: usize) -> usize {
    #[cfg(all(target_os = "linux", not(test)))]
    if let (Some(current), Some(limit)) = crate::util::system_memory::linux_cgroup_memory() {
        let effective = crate::engine::executor::mem_budget::effective_memory_limit_bytes() as u64;
        let deficit = cache_headroom_for_limits(current, requested as u64, limit, effective);
        let allocation_window = (limit / 100).clamp(16 * 1024, 64 * 1024 * 1024);
        if deficit != 0
            && (requested as u64 >= allocation_window
                || file_cache_pressure_active(limit, current, requested as u64))
        {
            return deficit.min(usize::MAX as u64) as usize;
        }
    }
    let _ = requested;
    0
}

/// Full but healthy caches are not pressure. Net refaults during reclaim
/// identify competition with the kernel working set; raw-buffer growth is
/// credited because those pages are moving into the engine cache instead.
#[cfg(all(target_os = "linux", not(test)))]
#[derive(Clone, Copy)]
struct CachedKernelPressure {
    sampled_ms: u64,
    limit: u64,
    refault: Option<u64>,
    reclaim: Option<u64>,
    physical_nonraw: u64,
}

#[cfg(any(test, target_os = "linux"))]
fn pressure_sample_fresh(sampled: u64, now: u64, sampled_limit: u64, limit: u64) -> bool {
    now >= sampled && now - sampled < 10 && sampled_limit == limit
}

#[cfg(all(target_os = "linux", not(test)))]
fn file_cache_pressure_active(limit: u64, current: u64, requested: u64) -> bool {
    static HISTORY: Mutex<CachePressureHistory> = Mutex::new(CachePressureHistory::new());
    static PAGE_BYTES: OnceLock<u64> = OnceLock::new();
    // Serialize observations, including their counter reads: concurrent
    // samplers must not mistake an older snapshot for a controller reset.
    let mut history = HISTORY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let page_bytes = *PAGE_BYTES.get_or_init(|| {
        // SAFETY: sysconf reads a scalar process property.
        u64::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).unwrap_or(4096)
    });
    let now = crate::util::coarse_clock::coarse_elapsed_ms();
    let sample = match history
        .cached_kernel
        .filter(|s| pressure_sample_fresh(s.sampled_ms, now, s.limit, limit))
    {
        Some(sample) => sample,
        None => {
            let Some(path) = current_linux_cgroup_v2_path().or_else(current_linux_cgroup_v1_path)
            else {
                return false;
            };
            let Ok(stat) = std::fs::read_to_string(path.join("memory.stat")) else {
                return true;
            };
            let value = |text: &str, key: &str| {
                text.lines().find_map(|line| {
                    let (name, number) = line.split_once(' ')?;
                    (name == key)
                        .then(|| number.trim().parse::<u64>().ok())
                        .flatten()
                })
            };
            let refault = value(&stat, "workingset_refault_file")
                .or_else(|| value(&stat, "total_workingset_refault_file"))
                .or_else(|| value(&stat, "workingset_refault"));
            let reclaim = std::fs::read_to_string(path.join("memory.events"))
                .ok()
                .and_then(|events| value(&events, "max"))
                .or_else(|| read_cgroup_u64(&path.join("memory.failcnt")));
            let raw = crate::engine::executor::mem_budget::controlled_buffer_used_bytes() as u64;
            let sample = CachedKernelPressure {
                sampled_ms: now,
                limit,
                refault: refault.map(|r| r.saturating_mul(page_bytes)),
                reclaim,
                physical_nonraw: current
                    .saturating_sub(parse_cgroup_reclaimable_bytes(&stat).unwrap_or(0))
                    .saturating_sub(raw),
            };
            history.cached_kernel = Some(sample);
            sample
        }
    };
    let (Some(refault), Some(reclaim)) = (sample.refault, sample.reclaim) else {
        // Older controllers may not expose the counters. Preserve bounded
        // conservative relief rather than silently disabling protection.
        return true;
    };
    history.threshold_bytes = (limit / 100).clamp(page_bytes, 64 * 1024 * 1024);
    let (controlled, _, categories, _) =
        crate::engine::executor::mem_budget::controlled_memory_snapshot();
    let raw = categories[0] as u64;
    // Only additional OS signals are cached. Logical ownership/forecast is
    // read on every allocation sample, and hard admission remains fresh.
    let essential = (
        sample.physical_nonraw,
        nonraw_pressure_bytes(0, raw, controlled as u64, requested).1,
    );
    history.observe_pressure(now, refault, reclaim, raw, essential)
}

#[cfg(any(test, target_os = "linux"))]
fn nonraw_pressure_bytes(physical: u64, raw: u64, controlled: u64, requested: u64) -> (u64, u64) {
    // Logical ownership sees growing executor structures before their new
    // pages fault. Physical pressure also catches allocator/runtime growth
    // after logical owners have released their buffers. Raw-cache migration
    // is excluded from both, not from ordinary hard reservation accounting.
    (
        physical.saturating_sub(raw),
        controlled.saturating_sub(raw).saturating_add(requested),
    )
}

#[cfg(any(test, target_os = "linux"))]
struct CachePressureHistory {
    #[cfg(all(target_os = "linux", not(test)))]
    cached_kernel: Option<CachedKernelPressure>,
    previous: Option<(u64, u64, u64, (u64, u64))>,
    last_sample_ms: u64,
    window_start_ms: u64,
    net_refault_bytes: i128,
    physical_growth_bytes: i128,
    logical_growth_bytes: i128,
    reclaimed: bool,
    active_until_ms: u64,
    threshold_bytes: u64,
}

#[cfg(any(test, target_os = "linux"))]
impl CachePressureHistory {
    const fn new() -> Self {
        Self {
            #[cfg(all(target_os = "linux", not(test)))]
            cached_kernel: None,
            previous: None,
            last_sample_ms: 0,
            window_start_ms: 0,
            net_refault_bytes: 0,
            physical_growth_bytes: 0,
            logical_growth_bytes: 0,
            reclaimed: false,
            active_until_ms: 0,
            threshold_bytes: 64 * 1024 * 1024,
        }
    }

    #[cfg(test)]
    fn observe(&mut self, now: u64, refault: u64, reclaim: u64, raw: u64) -> bool {
        self.observe_pressure(now, refault, reclaim, raw, (0, 0))
    }

    fn observe_pressure(
        &mut self,
        now: u64,
        refault: u64,
        reclaim: u64,
        raw: u64,
        essential: (u64, u64),
    ) -> bool {
        let previous = self.previous.replace((refault, reclaim, raw, essential));
        let last_sample = self.last_sample_ms;
        self.last_sample_ms = now;
        let Some((old_refault, old_reclaim, old_raw, old_essential)) = previous else {
            self.window_start_ms = now;
            return false;
        };
        if refault < old_refault || reclaim < old_reclaim || now.saturating_sub(last_sample) > 250 {
            *self = Self {
                threshold_bytes: self.threshold_bytes,
                ..Self::new()
            };
            self.previous = Some((refault, reclaim, raw, essential));
            self.last_sample_ms = now;
            self.window_start_ms = now;
            return false;
        }
        if now.saturating_sub(self.window_start_ms) >= 100 {
            self.window_start_ms = now;
            self.net_refault_bytes = 0;
            self.physical_growth_bytes = 0;
            self.logical_growth_bytes = 0;
            self.reclaimed = false;
        }
        self.net_refault_bytes = self
            .net_refault_bytes
            .saturating_add(refault.saturating_sub(old_refault) as i128)
            .saturating_sub(raw.saturating_sub(old_raw) as i128);
        self.reclaimed |= reclaim > old_reclaim;
        self.physical_growth_bytes = self
            .physical_growth_bytes
            .saturating_add(essential.0 as i128 - old_essential.0 as i128);
        self.logical_growth_bytes = self
            .logical_growth_bytes
            .saturating_add(essential.1 as i128 - old_essential.1 as i128);
        let refault_pressure =
            self.reclaimed && self.net_refault_bytes >= self.threshold_bytes as i128;
        let allocation_pressure = self.physical_growth_bytes.max(self.logical_growth_bytes)
            >= self.threshold_bytes as i128;
        if refault_pressure || allocation_pressure {
            self.active_until_ms = now.saturating_add(250);
            if refault_pressure {
                self.net_refault_bytes = 0;
                self.reclaimed = false;
            }
            if allocation_pressure {
                self.physical_growth_bytes = 0;
                self.logical_growth_bytes = 0;
            }
        }
        now < self.active_until_ms
    }
}

#[cfg(any(test, target_os = "linux"))]
fn cache_headroom_for_limits(current: u64, requested: u64, reported: u64, effective: u64) -> u64 {
    // v1 represents "unlimited" with a huge numeric sentinel. A non-binding
    // cap above physical memory must not turn unrelated host file cache into
    // a reason to shrink this process's otherwise healthy working set.
    if reported > effective {
        return 0;
    }
    cache_headroom_for_values(current, requested, reported)
}

#[cfg(any(test, target_os = "linux"))]
fn cache_headroom_for_values(current: u64, requested: u64, limit: u64) -> u64 {
    if limit == 0 {
        return 0;
    }
    let next = current.saturating_add(requested);
    let trigger = limit - limit / 10;
    if next <= trigger {
        return 0;
    }
    // Separate high/low watermarks prevent repeated eviction after one page
    // crosses the trigger. The existing hard and compensation ceilings stay
    // authoritative, including when every remaining frame is pinned/dirty.
    next.saturating_sub(limit - limit / 20 * 3)
}

#[cfg(test)]
#[test]
fn cache_pressure_has_total_residency_hysteresis_without_a_new_hard_limit() {
    let gib = 1024 * 1024 * 1024;
    assert_eq!(cache_headroom_for_values(8 * gib, 0, 10 * gib), 0);
    assert_eq!(cache_headroom_for_values(9 * gib, 0, 10 * gib), 0);
    assert_eq!(
        cache_headroom_for_values(10 * gib, 0, 10 * gib),
        3 * gib / 2
    );
    assert_eq!(
        cache_headroom_for_values(8 * gib, 2 * gib, 10 * gib),
        3 * gib / 2
    );
    assert_eq!(cache_headroom_for_values(u64::MAX, 1, 0), 0);
    assert_eq!(
        cache_headroom_for_values(u64::MAX, u64::MAX, 100),
        u64::MAX - 85
    );
    assert_eq!(cache_headroom_for_limits(99, 0, u64::MAX, 100), 0);
    assert_eq!(cache_headroom_for_limits(99, 0, 200, 100), 0);
    assert_eq!(cache_headroom_for_limits(99, 0, 100, 100), 14);
    assert_eq!(cache_headroom_for_limits(19, 0, 20, 100), 2);
    assert!(pressure_sample_fresh(1, 10, 100, 100));
    assert!(!pressure_sample_fresh(1, 11, 100, 100));
    assert!(!pressure_sample_fresh(10, 9, 100, 100));
    assert!(!pressure_sample_fresh(10, 10, 100, 90));
}

#[cfg(test)]
#[test]
fn cache_pressure_credits_working_set_migration_and_accumulates_net_refaults() {
    let mib = 1024 * 1024;
    let mut h = CachePressureHistory::new();
    assert!(!h.observe(0, 0, 0, 0));
    assert!(!h.observe(10, 128 * mib, 1, 256 * mib));
    assert!(!h.observe(20, 256 * mib, 2, 512 * mib));
    assert!(!h.observe(110, 256 * mib, 2, 512 * mib));
    assert!(!h.observe(120, 288 * mib, 3, 512 * mib));
    assert!(h.observe(130, 320 * mib, 4, 512 * mib));
    assert!(h.observe(200, 320 * mib, 4, 512 * mib));
    assert!(!h.observe(390, 320 * mib, 4, 512 * mib));
}

#[cfg(test)]
#[test]
fn cache_pressure_requires_reclaim_and_does_not_replay_old_controller_history() {
    let mib = 1024 * 1024;
    let mut h = CachePressureHistory::new();
    assert!(!h.observe(0, 0, 0, 0));
    assert!(!h.observe(10, 128 * mib, 0, 0));
    assert!(!h.observe(1_000, 1024 * mib, 20, 0));
    assert!(h.observe(1_010, 1088 * mib, 21, 0));
    assert!(!h.observe(1_020, 0, 0, 0));
    assert!(!h.observe(1_030, 32 * mib, 1, 0));
    let mut small = CachePressureHistory::new();
    small.threshold_bytes = mib;
    assert!(!small.observe(0, 0, 0, 0));
    assert!(small.observe(10, 2 * mib, 1, 0));
}

#[cfg(test)]
#[test]
fn cache_pressure_preflights_compulsory_growth_but_credits_raw_cache_migration() {
    let mib = 1024 * 1024;
    assert_eq!(
        nonraw_pressure_bytes(200 * mib, 100 * mib, 200 * mib, mib),
        (100 * mib, 101 * mib)
    );
    assert_eq!(
        nonraw_pressure_bytes(328 * mib, 228 * mib, 328 * mib, mib),
        (100 * mib, 101 * mib)
    );
    assert_eq!(
        nonraw_pressure_bytes(392 * mib, 228 * mib, 392 * mib, mib),
        (164 * mib, 165 * mib)
    );
    let mut h = CachePressureHistory::new();
    assert!(!h.observe_pressure(0, 0, 0, 0, (500 * mib, 100 * mib)));
    assert!(!h.observe_pressure(50, 0, 0, 128 * mib, (500 * mib, 100 * mib)));
    assert!(h.observe_pressure(60, 0, 0, 128 * mib, (500 * mib, 164 * mib)));
    assert!(h.observe_pressure(200, 0, 0, 128 * mib, (500 * mib, 164 * mib)));
    assert!(!h.observe_pressure(320, 0, 0, 128 * mib, (500 * mib, 164 * mib)));
}

/// Whether admitting a forthcoming bulk allocation would consume the
/// configured resident-memory headroom.
///
/// On Linux this uses the containing cgroup's `memory.current`, so clean file
/// pages and allocator-retained arenas are considered alongside live engine
/// allocations. Callers use the result only to reclaim optional state (for
/// example, clean buffer-pool frames) before acquiring their governed work
/// reservation; it must not alter a physical query plan.
pub(crate) fn bulk_allocation_pressure_reclaim_bytes(
    requested_bytes: usize,
    limit_pct: u64,
) -> usize {
    let Some(current) = current_pressure_footprint_bytes() else {
        return 0;
    };
    let target = crate::engine::executor::mem_budget::memory_target_bytes() as u64;
    bulk_allocation_pressure_reclaim_bytes_for_values(
        current,
        requested_bytes.min(u64::MAX as usize) as u64,
        target,
        limit_pct,
    )
    .min(usize::MAX as u64) as usize
}

#[inline]
fn bulk_allocation_pressure_reclaim_bytes_for_values(
    current: u64,
    requested: u64,
    target: u64,
    limit_pct: u64,
) -> u64 {
    if target == 0 || limit_pct == 0 {
        return 0;
    }
    current
        .saturating_add(requested)
        .saturating_sub(target.saturating_mul(limit_pct.min(100)) / 100)
}

#[cfg(target_os = "linux")]
fn current_linux_cgroup_memory_bytes() -> Option<u64> {
    if let Some(path) = current_linux_cgroup_v2_path() {
        if let Some(value) = read_cgroup_u64(&path.join("memory.current")) {
            return Some(value);
        }
    }
    read_cgroup_u64(&current_linux_cgroup_v1_path()?.join("memory.usage_in_bytes"))
}

#[cfg(target_os = "linux")]
fn current_linux_cgroup_reclaimable_bytes() -> Option<u64> {
    let stat = current_linux_cgroup_v2_path()
        .map(|path| path.join("memory.stat"))
        .or_else(|| current_linux_cgroup_v1_path().map(|path| path.join("memory.stat")))?;
    let text = std::fs::read_to_string(stat).ok()?;
    parse_cgroup_reclaimable_bytes(&text)
}

#[cfg(any(test, target_os = "linux"))]
fn parse_cgroup_reclaimable_bytes(text: &str) -> Option<u64> {
    let value = |wanted: &str| {
        text.lines().find_map(|line| {
            let mut fields = line.split_whitespace();
            (fields.next()? == wanted)
                .then(|| fields.next()?.parse::<u64>().ok())
                .flatten()
        })
    };
    if let Some(file) = value("file") {
        // Slab reclaimable is kernel-owned inode/dentry cache, separate from
        // file pages. Counting it as pinned executor memory makes a sequence
        // of spill-heavy queries fail even after their allocations are freed.
        // Dirty/writeback pages, shmem, unreclaimable slab and anonymous pages
        // still count against the same physical-admission ceiling.
        return Some(
            reclaimable_file_bytes(
                file,
                value("shmem").unwrap_or(0),
                value("file_dirty").unwrap_or(0),
                value("file_writeback").unwrap_or(0),
            )
            .saturating_add(value("slab_reclaimable").unwrap_or(0)),
        );
    }
    for key in ["total_inactive_file", "inactive_file"] {
        if let Some(inactive) = value(key) {
            return Some(
                inactive.saturating_add(
                    value("total_slab_reclaimable")
                        .or_else(|| value("slab_reclaimable"))
                        .unwrap_or(0),
                ),
            );
        }
    }
    None
}

#[inline]
#[cfg(any(test, target_os = "linux"))]
fn reclaimable_file_bytes(file: u64, shmem: u64, dirty: u64, writeback: u64) -> u64 {
    file.saturating_sub(shmem)
        .saturating_sub(dirty)
        .saturating_sub(writeback)
}

#[cfg(target_os = "linux")]
fn current_linux_cgroup_v2_path() -> Option<std::path::PathBuf> {
    let relative = std::fs::read_to_string("/proc/self/cgroup")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("0::").map(str::to_owned))?;
    Some(std::path::Path::new("/sys/fs/cgroup").join(relative.trim_start_matches('/')))
}

#[cfg(target_os = "linux")]
fn current_linux_cgroup_v1_path() -> Option<std::path::PathBuf> {
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let relative = cgroup.lines().find_map(|line| {
        let mut fields = line.splitn(3, ':');
        let _id = fields.next()?;
        let controllers = fields.next()?;
        let path = fields.next()?;
        controllers
            .split(',')
            .any(|controller| controller == "memory")
            .then_some(path)
    })?;
    Some(std::path::Path::new("/sys/fs/cgroup/memory").join(relative.trim_start_matches('/')))
}

#[cfg(target_os = "linux")]
fn read_cgroup_u64(path: &std::path::Path) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn try_reclaim() {
    if !above_trigger() || ACTIVE_STATEMENTS.load(Ordering::Acquire) != 0 {
        return;
    }
    reclaim_using(release_allocator_cached_memory);
}

fn reclaim_now() -> u64 {
    reclaim_using(allocator_pressure_relief)
}

fn reclaim_using(reclaim: fn() -> bool) -> u64 {
    let before = current_private_footprint_bytes().unwrap_or(0);
    let started = Instant::now();
    ATTEMPTS.fetch_add(1, Ordering::Relaxed);
    if !reclaim() {
        UNSUPPORTED.fetch_add(1, Ordering::Relaxed);
    }
    let after = current_private_footprint_bytes().unwrap_or(before);
    let released = before.saturating_sub(after);
    if released > 0 {
        SUCCESSES.fetch_add(1, Ordering::Relaxed);
    }
    LAST_BEFORE.store(before, Ordering::Relaxed);
    LAST_AFTER.store(after, Ordering::Relaxed);
    LAST_RELEASED.store(released, Ordering::Relaxed);
    LAST_DURATION_US.store(
        started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
        Ordering::Relaxed,
    );
    released
}

/// Return allocator-cached pages after a bulk statement has released its
/// governed work buffers but before the next statement can immediately start.
///
/// The ordinary background worker deliberately waits for 250 ms of process
/// idle time. Consecutive multi-minute `LOAD DATA` statements never provide
/// that gap, so glibc arenas can retain several statements' freed sort/run
/// buffers even though the hard governor has already released their charges.
/// Check the real process footprint once at that bulk-statement boundary and
/// pay the allocator-wide trim only after it has crossed the configured hard
/// target. `malloc_trim` / `mi_collect` are process-wide and thread-safe.
pub(crate) fn reclaim_bulk_statement_boundary() {
    let Some(current) = current_private_footprint_bytes() else {
        return;
    };
    let target = crate::engine::executor::mem_budget::memory_target_bytes() as u64;
    if should_reclaim_bulk_statement_boundary(current, target) {
        reclaim_now();
    }
}

#[inline]
fn should_reclaim_bulk_statement_boundary(current: u64, target: u64) -> bool {
    target != 0 && current > target
}

/// Synchronously return allocator-cached free pages to the OS after a caller
/// has deliberately released a large governed allocation (for example, HNSW
/// shrinking the buffer pool before acquiring a build lease).
pub(crate) fn release_allocator_cached_memory() -> bool {
    let supported = allocator_pressure_relief();
    #[cfg(feature = "fast-alloc")]
    {
        let completed = WORKER_RECLAIM_COMPLETED.load(Ordering::Acquire);
        WORKER_RECLAIM_EPOCH.fetch_add(1, Ordering::AcqRel);
        let workers = crate::util::blocking_pool::wake_for_allocator_reclaim();
        // Only the physical-pressure slow path waits. A busy peer may hold
        // the resource this caller needs, so acknowledgements are bounded.
        let deadline = Instant::now() + Duration::from_millis(5);
        while WORKER_RECLAIM_COMPLETED
            .load(Ordering::Acquire)
            .wrapping_sub(completed)
            < workers as u64
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_micros(50));
        }
    }
    supported
}

/// Reclaim allocator-retained pages at a safe boundary inside one long bulk
/// operation.
///
/// The ordinary worker cannot run while the statement activity guard is held.
/// Incremental index backfill, however, has explicit chunk boundaries after
/// all page locks and temporary decoded-page oracles have been released. A
/// low-frequency call here prevents thousands of freed arenas from building
/// up behind one multi-second statement. The cgroup check keeps the normal
/// path syscall-only; allocator collection runs only above the managed 85%
/// limit.
pub(crate) fn reclaim_bulk_operation_pressure() {
    let Some(current) = current_pressure_footprint_bytes() else {
        return;
    };
    let target = crate::engine::executor::mem_budget::memory_target_bytes() as u64;
    if bulk_operation_exceeds_pressure_limit(current, target) {
        reclaim_now();
    }
}

#[inline]
fn bulk_operation_exceeds_pressure_limit(current: u64, target: u64) -> bool {
    target != 0 && current > target.saturating_mul(85) / 100
}

#[cfg(feature = "fast-alloc")]
fn allocator_pressure_relief() -> bool {
    // SAFETY: mimalloc collects the calling thread's heap and shared arenas.
    // It does not collect live pages owned by other workers. The linked
    // global allocator owns the heaps being collected.
    unsafe { libmimalloc_sys::mi_collect(true) };
    true
}

#[cfg(all(not(feature = "fast-alloc"), target_os = "macos"))]
fn allocator_pressure_relief() -> bool {
    unsafe extern "C" {
        fn malloc_default_zone() -> *mut libc::c_void;
        fn malloc_get_all_zones(
            task: libc::c_uint,
            reader: *const libc::c_void,
            addresses: *mut *mut usize,
            count: *mut libc::c_uint,
        ) -> libc::c_int;
        fn malloc_zone_pressure_relief(zone: *mut libc::c_void, goal: libc::size_t)
            -> libc::size_t;
    }
    // SAFETY: for the current task, malloc_get_all_zones may use a null memory
    // reader and returns an allocator-owned array of registered zone addresses.
    // Recent macOS versions return a DefaultMallocZone wrapper from
    // malloc_default_zone; walking the registered zones reaches the underlying
    // scalable zone whose pressure-relief callback owns the large-object cache.
    unsafe {
        let mut addresses = std::ptr::null_mut();
        let mut count = 0;
        let listed = malloc_get_all_zones(0, std::ptr::null(), &mut addresses, &mut count) == 0;
        if listed && !addresses.is_null() {
            for address in std::slice::from_raw_parts(addresses, count as usize) {
                malloc_zone_pressure_relief(*address as *mut libc::c_void, 0);
            }
        } else {
            let zone = malloc_default_zone();
            if !zone.is_null() {
                malloc_zone_pressure_relief(zone, 0);
            }
        }
        malloc_zone_pressure_relief(std::ptr::null_mut(), 0);
    }
    true
}

#[cfg(all(not(feature = "fast-alloc"), target_os = "linux", target_env = "gnu"))]
fn allocator_pressure_relief() -> bool {
    // SAFETY: glibc malloc_trim is process-wide and thread-safe.
    unsafe {
        libc::malloc_trim(0);
    }
    true
}

#[cfg(not(any(
    feature = "fast-alloc",
    target_os = "macos",
    all(target_os = "linux", target_env = "gnu")
)))]
fn allocator_pressure_relief() -> bool {
    false
}

#[cfg(target_os = "macos")]
pub(crate) fn current_private_footprint_bytes() -> Option<u64> {
    unsafe {
        let mut info = std::mem::MaybeUninit::<libc::rusage_info_v0>::uninit();
        let result = libc::proc_pid_rusage(
            libc::getpid(),
            libc::RUSAGE_INFO_V0,
            info.as_mut_ptr().cast(),
        );
        (result == 0).then(|| info.assume_init().ri_phys_footprint)
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn current_private_footprint_bytes() -> Option<u64> {
    if let Ok(text) = std::fs::read_to_string("/proc/self/smaps_rollup") {
        let mut private_kib = 0u64;
        for line in text.lines() {
            let Some((name, rest)) = line.split_once(':') else {
                continue;
            };
            if matches!(name, "Private_Clean" | "Private_Dirty" | "Private_Hugetlb") {
                private_kib = private_kib.saturating_add(
                    rest.split_whitespace()
                        .next()
                        .and_then(|value| value.parse::<u64>().ok())
                        .unwrap_or(0),
                );
            }
        }
        if private_kib != 0 {
            return Some(private_kib.saturating_mul(1024));
        }
    }
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (page_size > 0)
        .then(|| std::fs::read_to_string("/proc/self/statm").ok())
        .flatten()
        .and_then(|line| line.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|pages| pages.saturating_mul(page_size as u64))
}

#[cfg(windows)]
pub(crate) fn current_private_footprint_bytes() -> Option<u64> {
    crate::platform::memory::process_memory().map(|p| p.private_bytes)
}

#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
pub(crate) fn current_private_footprint_bytes() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writeback_pressure_includes_only_pending_io_for_both_cgroup_versions() {
        assert_eq!(parse_writeback_pressure_bytes("anon 1000\nfile 9000\n"), 0);
        assert_eq!(
            parse_writeback_pressure_bytes("file_dirty 4096\nfile_writeback 8192\n"),
            12288
        );
        assert_eq!(
            parse_writeback_pressure_bytes(
                "dirty 1\nwriteback 2\ntotal_dirty 4096\ntotal_writeback 8192\n"
            ),
            12288
        );
    }

    #[cfg(feature = "fast-alloc")]
    #[test]
    fn allocator_admission_reclaim_reaches_parked_heap_owners() {
        crate::util::blocking_pool::global()
            .spawn_joinable(|| {})
            .join()
            .unwrap();
        let before = WORKER_RECLAIM_COMPLETED.load(Ordering::Acquire);
        assert!(release_allocator_cached_memory());
        let deadline = Instant::now() + Duration::from_secs(1);
        while WORKER_RECLAIM_COMPLETED.load(Ordering::Acquire) == before
            && Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        assert!(WORKER_RECLAIM_COMPLETED.load(Ordering::Acquire) > before);
    }

    #[cfg(all(feature = "fast-alloc", any(target_os = "linux", target_os = "macos")))]
    #[test]
    fn statement_headroom_reclaim_reaches_parked_heap_owners() {
        use crate::engine::executor::mem_budget;

        crate::util::blocking_pool::global()
            .spawn_joinable(|| {})
            .join()
            .unwrap();
        let previous_target = mem_budget::memory_target_bytes();
        let previous_enabled = enabled();
        // A tiny advisory target deterministically requests collection; no
        // allocations are admitted against a modified physical hard limit.
        mem_budget::set_memory_target_mb(1).unwrap();
        set_enabled(true);
        let before = WORKER_RECLAIM_EPOCH.load(Ordering::Acquire);
        HEADROOM_RECLAIM_PENDING.store(RECLAIM_MEMORY_PEAK, Ordering::Release);
        reclaim_statement_headroom_if_requested();
        let after = WORKER_RECLAIM_EPOCH.load(Ordering::Acquire);
        mem_budget::set_memory_target_mb(previous_target / (1024 * 1024)).unwrap();
        set_enabled(previous_enabled);
        assert!(
            after > before,
            "statement reclaim must reach the allocating workers"
        );
    }

    #[test]
    fn worker_reclaim_hint_is_local_and_ignores_small_releases() {
        WORKER_RECLAIM_PENDING.with(|pending| pending.set(false));
        note_worker_memory_release(MIN_HEADROOM_RECLAIM_PEAK_BYTES - 1);
        assert!(!WORKER_RECLAIM_PENDING.with(|pending| pending.get()));
        std::thread::spawn(|| {
            note_worker_memory_release(MIN_HEADROOM_RECLAIM_PEAK_BYTES);
            assert!(WORKER_RECLAIM_PENDING.with(|pending| pending.replace(false)));
        })
        .join()
        .unwrap();
        assert!(!WORKER_RECLAIM_PENDING.with(|pending| pending.get()));
    }

    #[test]
    fn quiet_timer_observes_final_activity_without_per_statement_wakeups() {
        let reclaimer = std::sync::Arc::new(Reclaimer {
            state: Mutex::new(WakeState::new()),
            wake: Condvar::new(),
        });
        let quiet = Duration::from_secs(1);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = {
            let reclaimer = reclaimer.clone();
            std::thread::spawn(move || {
                let state = reclaimer.state.lock().unwrap();
                started_tx.send(()).unwrap();
                let state = wait_for_quiet_period(&reclaimer, state, quiet).unwrap();
                done_tx
                    .send((state.generation, state.last_activity.elapsed()))
                    .unwrap();
            })
        };
        started_rx.recv().unwrap();
        // Taking the mutex waits until the worker has armed its timer and
        // released the guard. Keep the complete burst atomic; a scheduling
        // delay must not let the worker observe a partially recorded burst.
        // The one-second quiet period leaves headroom under loaded nextest runs.
        {
            let mut state = reclaimer.state.lock().unwrap();
            for _ in 0..64 {
                assert!(!state.record_activity());
            }
        }
        // No condvar notifications were sent during the burst. The original
        // timer still must discover the new deadline and the final event.
        let (generation, elapsed) = done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(generation, 64);
        assert!(elapsed >= quiet);
        worker.join().unwrap();
    }

    #[test]
    fn protocol_activity_coalesces_budget_release_but_embedded_release_notifies() {
        let guard = StatementActivityGuard::enter();
        let before = global().state.lock().unwrap().generation;
        notify_query_memory_released(0);
        assert_eq!(global().state.lock().unwrap().generation, before);
        drop(guard);
        let completed = global().state.lock().unwrap().generation;
        assert_ne!(completed, before);
        notify_query_memory_released(0);
        assert_ne!(global().state.lock().unwrap().generation, completed);
    }

    #[test]
    fn statement_guard_tracks_activity() {
        let before = ACTIVE_STATEMENTS.load(Ordering::Acquire);
        {
            let _guard = StatementActivityGuard::enter();
            assert_eq!(ACTIVE_STATEMENTS.load(Ordering::Acquire), before + 1);
        }
        assert_eq!(ACTIVE_STATEMENTS.load(Ordering::Acquire), before);
    }

    #[test]
    fn activity_start_only_notifies_on_idle_to_active_transition() {
        assert!(should_notify_activity_start(0));
        assert!(!should_notify_activity_start(1));
        assert!(!should_notify_activity_start(64));
    }

    #[test]
    fn substantial_query_peak_arms_headroom_check() {
        let executor = 512 * 1024 * 1024;
        assert!(!should_request_headroom_reclaim(15 * 1024 * 1024, executor));
        assert!(should_request_headroom_reclaim(16 * 1024 * 1024, executor));
        assert!(should_request_headroom_reclaim(512 * 1024 * 1024, executor));
    }

    #[test]
    fn long_statement_arms_allocator_headroom_check() {
        assert!(!should_request_reclaim_after_statement(
            LONG_STATEMENT_RECLAIM_THRESHOLD - Duration::from_millis(1)
        ));
        assert!(should_request_reclaim_after_statement(
            LONG_STATEMENT_RECLAIM_THRESHOLD
        ));
    }

    #[test]
    fn statement_headroom_preserves_the_full_executor_target() {
        let gib = 1024 * 1024 * 1024u64;
        assert_eq!(statement_headroom_floor(40 * gib, 12 * gib), 28 * gib);
        assert!(!should_reclaim_statement_headroom(
            28 * gib,
            40 * gib,
            12 * gib
        ));
        assert!(should_reclaim_statement_headroom(
            28 * gib + 1,
            40 * gib,
            12 * gib
        ));
        assert!(!should_reclaim_statement_headroom(gib, 0, 12 * gib));
    }

    #[test]
    fn bulk_statement_boundary_reclaims_only_above_hard_target() {
        assert!(!should_reclaim_bulk_statement_boundary(0, 1024));
        assert!(!should_reclaim_bulk_statement_boundary(1024, 1024));
        assert!(should_reclaim_bulk_statement_boundary(1025, 1024));
        assert!(!should_reclaim_bulk_statement_boundary(1025, 0));
    }

    #[test]
    fn bulk_allocation_pressure_preserves_requested_headroom() {
        let gib = 1024 * 1024 * 1024u64;
        assert_eq!(
            bulk_allocation_pressure_reclaim_bytes_for_values(7 * gib, gib, 10 * gib, 85,),
            0
        );
        assert_eq!(
            bulk_allocation_pressure_reclaim_bytes_for_values(8 * gib, gib, 10 * gib, 85,),
            gib / 2
        );
        assert_eq!(
            bulk_allocation_pressure_reclaim_bytes_for_values(8 * gib, gib, 0, 85,),
            0
        );
    }

    #[test]
    fn clean_cache_does_not_consume_allocator_statement_headroom() {
        let clean = reclaimable_file_bytes(400, 0, 0, 0);
        assert!(!should_reclaim_statement_headroom(
            nonreclaimable_pressure_bytes(720, clean),
            500,
            150,
        ));
        // Dirty/writeback/shared pages are not treated as allocator headroom.
        let clean = reclaimable_file_bytes(400, 20, 40, 20);
        assert!(should_reclaim_statement_headroom(
            nonreclaimable_pressure_bytes(720, clean),
            500,
            150,
        ));
    }

    #[test]
    fn cgroup_clean_file_pages_are_treated_as_reclaimable() {
        assert_eq!(reclaimable_file_bytes(4_000, 500, 200, 100), 3_200);
        assert_eq!(nonreclaimable_pressure_bytes(10_000, 3_200), 6_800);
        assert_eq!(nonreclaimable_pressure_bytes(1_000, 2_000), 0);
        assert_eq!(reclaimable_file_bytes(100, 200, 0, 0), 0);
    }

    #[test]
    fn cgroup_pressure_excludes_reclaimable_slab_and_keeps_pinned_pages() {
        assert_eq!(parse_cgroup_reclaimable_bytes(
            "file 1000\nshmem 100\nfile_dirty 200\nfile_writeback 100\nslab_reclaimable 150\nslab_unreclaimable 250\nanon 9000\n"
        ), Some(750));
        assert_eq!(
            parse_cgroup_reclaimable_bytes("total_inactive_file 400\ntotal_slab_reclaimable 100\n"),
            Some(500)
        );
        assert_eq!(parse_cgroup_reclaimable_bytes("anon 9000\n"), None);
    }

    #[test]
    fn physical_pressure_admission_preserves_requested_headroom() {
        assert_eq!(
            pressure_allowance_after_admission(700, 200, 1_000),
            Some(100)
        );
        assert_eq!(pressure_allowance_after_admission(900, 101, 1_000), None);
        assert_eq!(
            pressure_allowance_after_admission(u64::MAX, 1, u64::MAX),
            None
        );
    }

    #[test]
    fn compensation_cannot_lend_emergency_headroom_to_ordinary_allocations() {
        let ordinary = 800;
        let emergency = 900;
        assert_eq!(pressure_allowance_after_admission(810, 10, ordinary), None);
        let remaining = pressure_allowance_after_admission(810, 10, emergency).unwrap();
        assert_eq!(remaining, 80);
        assert_eq!(shared_pressure_allowance(remaining, emergency, ordinary), 0);
        assert_eq!(shared_pressure_allowance(150, emergency, ordinary), 50);
        assert_eq!(shared_pressure_allowance(150, ordinary, ordinary), 150);
        assert_eq!(pressure_allowance_after_admission(900, 1, emergency), None);
    }

    #[test]
    fn physical_reclaim_reports_only_the_admission_deficit() {
        assert_eq!(headroom_reclaim_bytes_for_values(700, 200, 1_000), 0);
        assert_eq!(headroom_reclaim_bytes_for_values(900, 101, 1_000), 1);
        assert_eq!(headroom_reclaim_bytes_for_values(1_100, 64, 1_000), 164);
    }

    #[test]
    fn bulk_operation_pressure_uses_managed_limit() {
        assert!(!bulk_operation_exceeds_pressure_limit(850, 1000));
        assert!(bulk_operation_exceeds_pressure_limit(851, 1000));
        assert!(!bulk_operation_exceeds_pressure_limit(851, 0));
    }
}

#[cfg(test)]
#[test]
fn reserved_native_stacks_leave_physical_headroom_before_they_are_touched() {
    assert_eq!(pressure_limit_after_runtime_reservation(1932, 168), 1764);
    assert_eq!(pressure_limit_after_runtime_reservation(100, 168), 0);
    assert_eq!(pressure_limit_after_runtime_reservation(1932, 0), 1932);
}

#[test]
fn statement_reclaim_preserves_executor_headroom_below_physical_ceiling() {
    let mib = 1024 * 1024;
    let target = 900 * mib;
    let executor = 270 * mib;
    let operational = 1024 * mib * 80 / 100;
    let stacks = 185_991_168;
    let ceiling = statement_memory_ceiling(target, operational, stacks);
    assert_eq!(ceiling, 673_002_291);
    assert!(!should_reclaim_statement_headroom(
        500 * mib,
        target,
        executor
    ));
    assert!(should_reclaim_statement_headroom(
        500 * mib,
        ceiling,
        executor
    ));
    assert!(!should_reclaim_statement_headroom(
        300 * mib,
        ceiling,
        executor
    ));
    assert_eq!(
        statement_memory_ceiling(100 * mib, operational, stacks),
        100 * mib
    );
}

#[cfg(test)]
#[test]
fn time_only_reclaim_is_coalesced_without_weakening_peak_or_pressure_checks() {
    let target = 8 * 1024 * 1024 * 1024;
    let pressure = 6 * 1024 * 1024 * 1024;
    let previous = Some((Duration::from_millis(100), pressure));
    assert!(coalesce_time_only_reclaim(
        RECLAIM_LONG_STATEMENT,
        pressure,
        target,
        previous
    ));
    assert!(!coalesce_time_only_reclaim(
        RECLAIM_LONG_STATEMENT | RECLAIM_MEMORY_PEAK,
        pressure,
        target,
        previous
    ));
    assert!(!coalesce_time_only_reclaim(
        RECLAIM_LONG_STATEMENT,
        target,
        target,
        previous
    ));
    assert!(!coalesce_time_only_reclaim(
        RECLAIM_LONG_STATEMENT,
        pressure + TIME_ONLY_RECLAIM_GROWTH,
        target,
        previous
    ));
    assert!(!coalesce_time_only_reclaim(
        RECLAIM_LONG_STATEMENT,
        pressure,
        target,
        Some((TIME_ONLY_RECLAIM_INTERVAL, pressure))
    ));
    assert!(!coalesce_time_only_reclaim(
        RECLAIM_LONG_STATEMENT,
        pressure,
        target,
        None
    ));
}
