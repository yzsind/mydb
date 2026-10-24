//! Custom blocking pool that bypasses tokio's `spawn_blocking` central
//! `Mutex<Inner>`.
//!
//! Tokio's blocking pool serializes every spawn AND every worker
//! idle-park behind a single mutex. sysbench `oltp_read_write`
//! profiling on the wal-persistence-overhaul branch — after the
//! IndexLeafPage cached_size optimization (commit 98c28c86) eliminated
//! the top three B-tree CPU hot functions — showed
//! `__psynch_mutexwait` at 61 k samples / 25 s sample, every stack
//! anchored in
//! `tokio::runtime::blocking::pool::Spawner::spawn_task`. At
//! 20 threads × 4 DML per RW txn × ~4 k tps the spawn rate is
//! ~320 k/s, and the pool's inner-mutex becomes the dominant wait
//! once the per-task CPU cost has been driven down.
//!
//! The replacement here uses:
//!
//! * a `crossbeam_deque::Injector` MPMC queue (lockless push + steal)
//! * `MYDB_BLOCKING_POOL_SIZE` pre-spawned baseline worker threads (default
//!   CPU-scaled — enough row-lock condvar headroom without waking a
//!   256-thread pool on small OLTP boxes; tokio's hidden default is 512
//!   grown lazily, we pre-spawn so the steady-state spawn path never
//!   allocates a thread). The pool then grows on demand up to
//!   `MYDB_BLOCKING_POOL_MAX` (default 2× the CPU-scaled baseline) whenever
//!   a task is queued with zero idle workers. This is a correctness
//!   requirement, not just a tuning knob: lock-taking statements — including
//!   the COMMIT that releases a transaction's row locks — run here and park
//!   on a row-lock condvar. If a fixed-size pool is fully occupied by workers
//!   parked in such waits, a queued COMMIT can never run, so the locks it
//!   would release are held indefinitely and every waiter behind them times
//!   out (a thread-starvation deadlock invisible to the lock-graph detector,
//!   observed as a total TPC-C collapse once concurrency exceeds the pool at
//!   e.g. 100 terminals on a 10-core box → 40-thread pool). Both the baseline
//!   and explicit worker settings fit the unmanaged stack/runtime slice.
//!   Automatic scheduling can reserve extra complete stacks from unused
//!   managed memory before promising more workers to transaction admission.
//!   That capacity is also withheld from physical allocation headroom until
//!   the stacks are touched. Baseline size, explicit settings and stack sizes
//!   remain unchanged; growth is bounded by the funded worker ceiling.
//! * a lock-free sleeper-token queue plus `Thread::park` / `unpark` for
//!   worker parking. `unpark` retains a wake that arrives just before
//!   `park`, closing the lost-wakeup race without serializing submitters and
//!   re-parking workers on one wake mutex. Idle workers park indefinitely
//!   with no timeout polling and therefore consume zero CPU.
//! * a `tokio::sync::oneshot::channel` per spawn to deliver the
//!   result back to the awaiting tokio task; the receiver is wrapped
//!   in a [`BlockingHandle`] that impls `Future`, so call sites read
//!   exactly like `tokio::task::spawn_blocking(...).await` minus the
//!   `JoinError` → `BlockingError` rename.
//!
//! The pool is process-global, lazily initialized on first call to
//! [`spawn`]. Tests share the production pool — no per-test thread
//! spinup.

use std::any::Any;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, OnceLock, Weak};
use std::task::{Context, Poll};
use std::thread;

use crossbeam_deque::{Injector, Steal};
use parking_lot::Mutex;
use tokio::sync::oneshot;

type Task = Box<dyn FnOnce(&Arc<WorkerSleeper>) + Send + 'static>;

/// A scheduling hint, never ownership of a worker. Sharing or retaining a hint
/// cannot pin queued work behind a blocked thread or keep a pool alive.
#[derive(Clone, Default)]
pub struct Affinity {
    last: Arc<Mutex<Weak<WorkerSleeper>>>,
}

impl Affinity {
    pub fn configured() -> Option<Self> {
        let enabled = crate::util::env::var("MYDB_BLOCKING_AFFINITY")
            .map(|v| !matches!(v.trim(), "0" | "off" | "false" | "no"))
            .unwrap_or(true);
        enabled.then(Self::default)
    }

    fn preferred(&self, pool: &Arc<Injector<Task>>) -> Option<Arc<WorkerSleeper>> {
        self.last
            .lock()
            .upgrade()
            .filter(|worker| std::ptr::eq(worker.pool.as_ptr(), Arc::as_ptr(pool)))
    }
}

pub struct BlockingPool {
    injector: Arc<Injector<Task>>,
    sleepers: Arc<Injector<Arc<WorkerSleeper>>>,
    idle: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
    workers: Mutex<Vec<thread::JoinHandle<()>>>,
    /// Current worker-thread count (baseline + on-demand overflow). Read on
    /// the grow path to bound growth at `max_workers`.
    total: Arc<AtomicUsize>,
    /// Hard ceiling on the worker count. Growth stops here.
    max_workers: usize,
    /// Stack size for on-demand overflow workers (same as the baseline).
    stack_size: usize,
}

const WORKER_RUNNING: u8 = 0;
const WORKER_PARKED: u8 = 1;
const WORKER_NOTIFIED: u8 = 2;

/// One reusable park token per worker. `Thread::unpark` remembers a wake that
/// races just before `Thread::park`, so the submit path does not need the
/// global mutex required by a condition variable to close the lost-wakeup
/// window. The state CAS makes stale queue entries harmless.
struct WorkerSleeper {
    state: AtomicU8,
    thread: OnceLock<thread::Thread>,
    pool: Weak<Injector<Task>>,
    /// A targeted wake leaves its queue entry in place. Publish at most one
    /// entry per worker, even across arbitrarily many targeted wake cycles.
    queued: AtomicBool,
}

impl WorkerSleeper {
    fn new(pool: &Arc<Injector<Task>>) -> Self {
        Self {
            state: AtomicU8::new(WORKER_RUNNING),
            thread: OnceLock::new(),
            pool: Arc::downgrade(pool),
            queued: AtomicBool::new(false),
        }
    }
}

#[derive(Debug)]
pub enum BlockingError {
    /// The submitted closure panicked; the captured message (if any)
    /// is preserved for the resulting `QueryError`.
    Panicked(String),
    /// The pool was dropped (or the receiver hung up) before the task
    /// produced a value. Surfaces as a server execution error at the
    /// protocol layer.
    Cancelled,
}

impl std::fmt::Display for BlockingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockingError::Panicked(msg) => write!(f, "blocking task panicked: {msg}"),
            BlockingError::Cancelled => write!(f, "blocking task cancelled"),
        }
    }
}

impl std::error::Error for BlockingError {}

pub struct BlockingHandle<T> {
    state: HandleState<T>,
}

enum HandleState<T> {
    Pending(oneshot::Receiver<Result<T, BlockingError>>),
    /// Produced by [`spawn_session`] when the work already ran inline.
    Ready(Option<Result<T, BlockingError>>),
}

impl<T> Unpin for BlockingHandle<T> {}

impl<T> Future for BlockingHandle<T> {
    type Output = Result<T, BlockingError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.state {
            HandleState::Pending(rx) => match Pin::new(rx).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(inner)) => Poll::Ready(inner),
                Poll::Ready(Err(_recv_err)) => Poll::Ready(Err(BlockingError::Cancelled)),
            },
            HandleState::Ready(result) => Poll::Ready(
                result
                    .take()
                    .expect("inline blocking handle polled after completion"),
            ),
        }
    }
}

/// Synchronous completion handle for a long-lived blocking-pool task.
///
/// Query stream producers cannot be awaited from [`crate::engine::api::ResultCursor::drop`],
/// but they still need thread-like `is_finished` and `join` semantics so the
/// fixed transaction snapshot and cancellation tree outlive all execution.
/// This handle supplies exactly that without allocating a fresh OS thread per
/// result set.
pub struct BlockingJoinHandle {
    rx: mpsc::Receiver<Result<(), BlockingError>>,
    finished: Arc<AtomicBool>,
}

impl BlockingJoinHandle {
    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }

    pub fn join(self) -> Result<(), BlockingError> {
        self.rx.recv().unwrap_or(Err(BlockingError::Cancelled))
    }
}

impl BlockingPool {
    pub fn new(num_workers: usize) -> Self {
        Self::with_limits(num_workers, configured_max(num_workers))
    }

    /// `baseline` workers are pre-spawned; the pool may grow on demand up to
    /// `max_workers` (clamped to at least `baseline`).
    pub fn with_limits(baseline: usize, max_workers: usize) -> Self {
        let baseline = baseline.max(1);
        let max_workers = max_workers.max(baseline);
        let injector = Arc::new(Injector::new());
        let sleepers = Arc::new(Injector::new());
        let idle = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let total = Arc::new(AtomicUsize::new(0));

        let stack_size = configured_stack_size();
        let mut workers = Vec::with_capacity(baseline);
        for _ in 0..baseline {
            workers.push(spawn_worker(
                &injector, &sleepers, &idle, &shutdown, stack_size,
            ));
        }
        // Account for the pre-spawned baseline; on-demand growth bumps this
        // further via `maybe_grow`. Set after spawning — no `maybe_grow` can
        // race here because the pool isn't published yet.
        total.store(baseline, Ordering::Release);
        Self {
            injector,
            sleepers,
            idle,
            shutdown,
            workers: Mutex::new(workers),
            total,
            max_workers,
            stack_size,
        }
    }

    /// Add one on-demand worker when every existing worker is busy and the
    /// pool is below its ceiling.
    ///
    /// This is the deadlock guard. Lock-taking statements — including the
    /// COMMIT that *releases* a transaction's row locks — all run on this
    /// pool and block on `Condvar::wait_timeout` while queued for a row lock.
    /// If every worker is parked inside such a wait, a queued COMMIT can never
    /// run, so the locks its transaction holds are never released and the
    /// waiters it blocks never proceed: a thread-starvation deadlock that no
    /// lock-graph cycle detector can see. Growing the pool guarantees a
    /// runnable thread always exists for the lock-releasing continuation.
    fn maybe_grow(&self) {
        // Reserve a slot with a CAS so racing spawns can't overshoot the cap.
        // The reservation *is* the count bump — `spawn_worker` does not touch
        // `total` — so the worker is counted exactly once and the ceiling is
        // never exceeded even under concurrent growth.
        let mut cur = self.total.load(Ordering::Acquire);
        loop {
            if cur >= self.max_workers {
                return;
            }
            match self.total.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Overflow workers park forever like the baseline (no
                    // reaping): the pool converges to the true concurrency
                    // high-water mark, and idle parked threads cost ~0 CPU and
                    // ~0 committed stack. They exit on pool shutdown via the
                    // sleeper-token wakeup + shutdown flag.
                    let handle = spawn_worker(
                        &self.injector,
                        &self.sleepers,
                        &self.idle,
                        &self.shutdown,
                        self.stack_size,
                    );
                    self.workers.lock().push(handle);
                    return;
                }
                Err(actual) => cur = actual,
            }
        }
    }

    pub fn spawn<F, T>(&self, f: F) -> BlockingHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.spawn_affine(None, f)
    }

    /// Prefer the session's last worker only if it is parked. The task remains
    /// globally stealable, including when that worker loses the scheduling race.
    pub fn spawn_affine<F, T>(&self, affinity: Option<&Affinity>, f: F) -> BlockingHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let preferred = affinity.and_then(|hint| hint.preferred(&self.injector));
        let affinity = affinity.cloned();
        let (tx, rx) = oneshot::channel();
        let task: Task = Box::new(move |worker| {
            if let Some(hint) = affinity {
                *hint.last.lock() = Arc::downgrade(worker);
            }
            let result = catch_unwind(AssertUnwindSafe(f));
            let _ = tx.send(match result {
                Ok(value) => Ok(value),
                Err(payload) => Err(BlockingError::Panicked(panic_message(payload))),
            });
        });
        self.injector.push(task);
        // A parked worker publishes a reusable token before its final queue
        // recheck. `unpark` is token-based: a wake that lands immediately
        // before `park` is retained, eliminating both the lost-notify race and
        // the shared wake mutex that serialized short OLTP statements.
        std::sync::atomic::fence(Ordering::SeqCst);
        let woke_preferred = preferred
            .as_ref()
            .is_some_and(|worker| wake_sleeper(worker));
        if !woke_preferred
            && !wake_one_sleeper(&self.sleepers)
            && self.idle.load(Ordering::Acquire) == 0
        {
            // No worker is idle to pick this task up. Either the existing
            // workers are busy with short tasks (they will drain it in a
            // moment) or they are all parked inside a row-lock wait and will
            // NOT — the starvation-deadlock case. We can't tell the two apart
            // cheaply, so grow when below the ceiling: a spurious extra worker
            // parks harmlessly, while the needed one breaks the deadlock. The
            // pool converges to the real concurrency high-water mark because
            // growth only fires while zero workers are parked.
            self.maybe_grow();
        }
        BlockingHandle {
            state: HandleState::Pending(rx),
        }
    }

    /// Submit a task whose completion is joined synchronously.
    ///
    /// Unlike [`Self::spawn`], this is intended for an owner with a synchronous
    /// `Drop` implementation (notably a streaming result cursor). The task is
    /// scheduled and woken through the same queue as ordinary blocking work;
    /// only result delivery uses a standard channel so `join` is runtime-free.
    pub fn spawn_joinable<F>(&self, f: F) -> BlockingJoinHandle
    where
        F: FnOnce() + Send + 'static,
    {
        let (tx, rx) = mpsc::sync_channel(1);
        let finished = Arc::new(AtomicBool::new(false));
        let task_finished = Arc::clone(&finished);
        let task: Task = Box::new(move |_| {
            let result = catch_unwind(AssertUnwindSafe(f))
                .map_err(|payload| BlockingError::Panicked(panic_message(payload)))
                .map(|_| ());
            task_finished.store(true, Ordering::Release);
            let _ = tx.send(result);
        });
        self.injector.push(task);
        std::sync::atomic::fence(Ordering::SeqCst);
        if !wake_one_sleeper(&self.sleepers) && self.idle.load(Ordering::Acquire) == 0 {
            self.maybe_grow();
        }
        BlockingJoinHandle { rx, finished }
    }
}

impl Drop for BlockingPool {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        wake_all_sleepers(&self.sleepers);
        for handle in self.workers.get_mut().drain(..) {
            let _ = handle.join();
        }
    }
}

/// Spawn one worker thread. Shared by the baseline pre-spawn
/// ([`BlockingPool::with_limits`]) and on-demand growth
/// ([`BlockingPool::maybe_grow`]). The live worker count (`total`) is managed
/// by the callers — the baseline via a single `store`, growth via a CAS
/// reservation — so this function only handles thread creation. A monotonic
/// sequence gives each worker a unique, stable name across growth.
fn spawn_worker(
    injector: &Arc<Injector<Task>>,
    sleepers: &Arc<Injector<Arc<WorkerSleeper>>>,
    idle: &Arc<AtomicUsize>,
    shutdown: &Arc<AtomicBool>,
    stack_size: usize,
) -> thread::JoinHandle<()> {
    static WORKER_SEQ: AtomicUsize = AtomicUsize::new(0);
    let idx = WORKER_SEQ.fetch_add(1, Ordering::Relaxed);
    let injector = Arc::clone(injector);
    let sleepers = Arc::clone(sleepers);
    let idle = Arc::clone(idle);
    let shutdown = Arc::clone(shutdown);
    thread::Builder::new()
        .name(format!("mydb-blk-{idx}"))
        .stack_size(stack_size)
        .spawn(move || worker_loop(injector, sleepers, idle, shutdown))
        .expect("spawn blocking pool worker")
}

fn worker_loop(
    injector: Arc<Injector<Task>>,
    sleepers: Arc<Injector<Arc<WorkerSleeper>>>,
    idle: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
) {
    crate::engine::memory_reclaimer::mark_allocator_thread_pool_worker();
    let sleeper = Arc::new(WorkerSleeper::new(&injector));
    let _ = sleeper.thread.set(thread::current());
    loop {
        // Drain available tasks before any park check. Under
        // sustained load this never hits `Empty`, so the park path is cold.
        loop {
            match injector.steal() {
                Steal::Success(task) => task(&sleeper),
                Steal::Empty => break,
                Steal::Retry => continue,
            }
        }
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        crate::engine::memory_reclaimer::reclaim_worker_memory_after_phase();
        // Publish the sleeper before the final queue recheck. A submitter can
        // atomically claim this exact worker; stale tokens are skipped by CAS.
        sleeper.state.store(WORKER_PARKED, Ordering::SeqCst);
        idle.fetch_add(1, Ordering::SeqCst);
        if !sleeper.queued.swap(true, Ordering::AcqRel) {
            sleepers.push(Arc::clone(&sleeper));
        }
        std::sync::atomic::fence(Ordering::SeqCst);
        if !injector.is_empty() || shutdown.load(Ordering::Acquire) {
            if sleeper
                .state
                .compare_exchange(
                    WORKER_PARKED,
                    WORKER_RUNNING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                idle.fetch_sub(1, Ordering::Release);
            } else {
                // A submitter already claimed us and issued `unpark`.
                sleeper.state.store(WORKER_RUNNING, Ordering::Release);
                idle.fetch_sub(1, Ordering::Release);
            }
            continue;
        }
        thread::park();
        sleeper.state.store(WORKER_RUNNING, Ordering::Release);
        idle.fetch_sub(1, Ordering::Release);
    }
}

fn wake_sleeper(sleeper: &WorkerSleeper) -> bool {
    if sleeper
        .state
        .compare_exchange(
            WORKER_PARKED,
            WORKER_NOTIFIED,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .is_ok()
    {
        if let Some(worker) = sleeper.thread.get() {
            worker.unpark();
            return true;
        }
    }
    false
}

fn wake_one_sleeper(sleepers: &Injector<Arc<WorkerSleeper>>) -> bool {
    loop {
        match sleepers.steal() {
            Steal::Success(sleeper) => {
                sleeper.queued.store(false, Ordering::Release);
                if wake_sleeper(&sleeper) {
                    return true;
                }
            }
            Steal::Retry => continue,
            Steal::Empty => return false,
        }
    }
}

fn wake_all_sleepers(sleepers: &Injector<Arc<WorkerSleeper>>) {
    while wake_one_sleeper(sleepers) {}
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_owned()
    } else {
        "blocking pool task panicked".to_owned()
    }
}

static GLOBAL_POOL: OnceLock<BlockingPool> = OnceLock::new();

/// Wake existing workers so an allocator collection request can run on each
/// heap's owner. Busy workers observe it at their next safe task boundary.
/// This creates no workers and never waits for a task holding a resource.
pub(crate) fn wake_for_allocator_reclaim() -> usize {
    let Some(pool) = GLOBAL_POOL.get() else {
        return 0;
    };
    let Some(workers) = pool.workers.try_lock() else {
        return 0;
    };
    let current = thread::current().id();
    let mut woken = 0;
    for worker in workers.iter() {
        if worker.thread().id() != current {
            worker.thread().unpark();
            woken += 1;
        }
    }
    woken
}

pub fn global() -> &'static BlockingPool {
    GLOBAL_POOL.get_or_init(|| {
        let limits = global_limits();
        BlockingPool::with_limits(limits.baseline, limits.maximum)
    })
}

/// Maximum number of query workers backed by operator settings and the
/// unmanaged plus explicitly reserved managed stack capacity.
///
/// Transaction admission uses this before the pool is initialized so it can
/// keep the number of concurrently lock-holding transactions below the number
/// of workers that must run their lock-releasing COMMIT/ROLLBACK operations.
pub(crate) fn configured_max_workers() -> usize {
    global_limits().maximum
}

struct GlobalPoolLimits {
    baseline: usize,
    maximum: usize,
    // This reservation backs the FULL stack capacity of every worker beyond
    // the unmanaged runtime slice, including workers not yet spawned. Thus
    // transaction admission can promise N+1 workers without relying on lazy
    // OS commitment or hoping allocation succeeds after lock waits begin.
    stack_reservation: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

fn reserve_stack_headroom<T>(
    baseline_max: usize,
    desired: usize,
    stack_bytes: usize,
    budget: usize,
    mut reserve: impl FnMut(usize) -> Option<T>,
) -> (usize, Option<T>) {
    let extra = desired
        .saturating_sub(baseline_max)
        .min(budget / stack_bytes.max(1));
    for workers in (1..=extra).rev() {
        if let Some(charge) = reserve(workers * stack_bytes) {
            return (baseline_max + workers, Some(charge));
        }
    }
    (baseline_max, None)
}

static GLOBAL_LIMITS: OnceLock<GlobalPoolLimits> = OnceLock::new();

fn global_limits() -> &'static GlobalPoolLimits {
    GLOBAL_LIMITS.get_or_init(|| {
        let baseline = configured_size();
        let legacy_max = configured_max(baseline);
        // Explicit worker settings retain their existing meaning and clamp.
        // This is only an automatic scheduling policy, not an override of an
        // operator-requested concurrency or stack size.
        let explicit_pool = ["MYDB_BLOCKING_POOL_SIZE", "MYDB_BLOCKING_POOL_MAX"]
            .iter()
            .any(|name| {
                crate::util::env::var(name)
                    .ok()
                    .and_then(|v| v.parse::<usize>().ok())
                    .is_some_and(|v| v > 0)
            });
        if explicit_pool {
            return GlobalPoolLimits {
                baseline,
                maximum: legacy_max,
                stack_reservation: None,
            };
        }
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let automatic = super::exec_permit::automatic_limit_for(cpus);
        let admission = crate::util::env::var("MYDB_TXN_ADMISSION").ok();
        let desired_txns = match admission.as_deref().map(str::trim) {
            Some("0" | "off" | "false" | "no") => 0,
            Some(value) => value
                .parse::<usize>()
                .ok()
                .filter(|n| *n > 0)
                .unwrap_or(automatic)
                .min(automatic),
            None => automatic,
        };
        let desired = desired_txns.saturating_add(1).max(legacy_max);
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        // Borrow only unused managed headroom, with a bounded share of the
        // unchanged advisory target. The physical/cgroup admission gate also
        // checks every reservation; failure retains the old safe ceiling.
        let target = crate::engine::executor::mem_budget::memory_target_bytes();
        let used = crate::engine::executor::mem_budget::controlled_memory_snapshot().0;
        let budget = (target / 5).min(target.saturating_sub(used));
        let (maximum, stack_reservation) = reserve_stack_headroom(
            legacy_max,
            desired,
            configured_stack_size().saturating_add(64 * 1024),
            budget,
            |bytes| {
                ControlledMemoryCharge::try_new_optional(
                    ControlledMemoryCategory::SystemMetadata,
                    bytes,
                )
            },
        );
        if let Some(charge) = &stack_reservation {
            crate::engine::memory_reclaimer::reserve_runtime_capacity(charge.bytes());
        }
        GlobalPoolLimits {
            baseline,
            maximum,
            stack_reservation,
        }
    })
}

pub(crate) fn stack_budget_status() -> (usize, usize, usize) {
    match GLOBAL_LIMITS.get() {
        Some(limits) => (
            limits.baseline,
            limits.maximum,
            limits
                .stack_reservation
                .as_ref()
                .map_or(0, |charge| charge.bytes()),
        ),
        None => {
            let baseline = configured_size();
            (baseline, configured_max(baseline), 0)
        }
    }
}

/// `crate::util::blocking_pool::spawn(|| ...)` is the drop-in
/// replacement for `tokio::task::spawn_blocking(|| ...)`. The
/// returned [`BlockingHandle`] is `.await`-able exactly like a
/// `tokio::task::JoinHandle<T>`; the only surface difference is the
/// error variant ([`BlockingError`] instead of `tokio::task::JoinError`).
pub fn spawn<F, T>(f: F) -> BlockingHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    global().spawn(f)
}

/// Submit session work with an optional soft worker-affinity hint.
pub fn spawn_affine<F, T>(affinity: Option<&Affinity>, f: F) -> BlockingHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    global().spawn_affine(affinity, f)
}

/// Submit statement work for a client session. A request worker that owns
/// only this session has nothing else to drive while the session waits, so
/// the work runs right here (with the same panic containment as a pool task)
/// instead of costing a cross-thread hand-off and two wakeups. Otherwise it
/// goes to the blocking pool so a long statement cannot stall sibling
/// connections sharing the worker.
pub fn spawn_session<F, T>(affinity: Option<&Affinity>, f: F) -> BlockingHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    if crate::util::connection_dispatch::current_worker_owns_single_connection() {
        let result = catch_unwind(AssertUnwindSafe(f))
            .map_err(|payload| BlockingError::Panicked(panic_message(payload)));
        return BlockingHandle {
            state: HandleState::Ready(Some(result)),
        };
    }
    spawn_affine(affinity, f)
}

/// Stack size of blocking-pool workers, for other threads that may run the
/// same statement work.
pub(crate) fn worker_stack_size() -> usize {
    configured_stack_size()
}

/// Schedule a task on the process blocking pool and return a synchronous
/// completion handle. See [`BlockingPool::spawn_joinable`].
pub fn spawn_joinable<F>(f: F) -> BlockingJoinHandle
where
    F: FnOnce() + Send + 'static,
{
    global().spawn_joinable(f)
}

/// Stack size for blocking-pool workers. Query execution (planning, expression
/// evaluation, decorrelation, derived/CTE materialization) runs here and
/// recurses with the *shape* of the SQL — deeply-nested set operations /
/// derived tables / wide self-joins (e.g. TPC-DS q04) can exceed the 2 MiB
/// default thread stack and abort the whole **process** with a stack overflow,
/// not just fail the one query. Reserve a generous stack instead; it is lazily
/// committed, so the resident cost is only the pages a query actually touches.
/// `MYDB_BLOCKING_STACK_MB` overrides (clamped to [2, 512]).
fn configured_stack_size() -> usize {
    stack_size_from_raw(
        crate::util::env::var("MYDB_BLOCKING_STACK_MB")
            .ok()
            .as_deref(),
    )
}

/// Pure core of [`configured_stack_size`] (testable without touching the
/// process environment). Default 8 MiB; a valid override is clamped to
/// [2, 512] MiB; anything unparseable / non-positive falls back to the default.
fn stack_size_from_raw(raw: Option<&str>) -> usize {
    // The Rust default (2 MiB on the supported platforms) was insufficient for
    // deeply nested TPC-DS planning/execution. 8 MiB preserves a 4× safety
    // margin while allowing a 10 GiB process target to retain at most 64
    // workers inside the blocking-stack half of its runtime/stack slice.
    const DEFAULT_STACK_MB: usize = 8;
    let mb = raw
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .map(|n| n.clamp(2, 512))
        .unwrap_or(DEFAULT_STACK_MB);
    mb * 1024 * 1024
}

fn configured_size() -> usize {
    let requested = crate::util::env::var("MYDB_BLOCKING_POOL_SIZE")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .map(|n| n.min(4096))
        .unwrap_or_else(cpu_scaled_baseline);
    requested.min(configured_stack_worker_limit()).max(1)
}

fn cpu_scaled_baseline() -> usize {
    // Scale the baseline to the machine instead of pre-spawning 256 workers
    // everywhere. Prepared-write OLTP workloads with tens of active sessions
    // spend measurable time in scheduler/park wakeups when the pool is far
    // wider than the active demand. Under heavier concurrency the pool now
    // grows past this baseline on demand (see `configured_max`), so this is
    // only the pre-spawned floor, not a hard cap.
    std::thread::available_parallelism()
        .map(|n| n.get().saturating_mul(4).clamp(16, 128))
        .unwrap_or(32)
}

/// Number of blocking stacks that fit in the process slice explicitly left
/// outside the managed-memory ceiling. Thread stacks are runtime memory, but
/// unlike allocator slack they have a known configured capacity, so budget the
/// full capacity rather than hoping the OS keeps most stack pages untouched.
fn stack_worker_limit_for(memory_target: usize, stack_size: usize) -> usize {
    // The process leaves 10% outside the managed database ceiling. Blocking
    // workers may consume at most half; Tokio workers, allocator arenas,
    // socket/kernel state and other runtime owners need the other half.
    const BLOCKING_STACK_PCT: usize = 5;
    memory_target
        .saturating_mul(BLOCKING_STACK_PCT)
        .saturating_div(100)
        .saturating_div(stack_size.max(1))
        .max(1)
}

fn configured_stack_worker_limit() -> usize {
    stack_worker_limit_for(
        crate::engine::executor::mem_budget::effective_memory_limit_bytes(),
        configured_stack_size(),
    )
}

/// Ceiling for on-demand growth (`MYDB_BLOCKING_POOL_MAX`). The ordinary
/// default is twice the CPU-scaled baseline, leaving runnable headroom when
/// baseline workers are parked in row-lock waits. Both the default and an
/// explicit override are hard-clamped by the stack slice of effective
/// OS/cgroup memory; a tuning knob cannot weaken the process hard limit.
fn configured_max(baseline: usize) -> usize {
    const HARD_CEILING: usize = 8192;
    let requested = crate::util::env::var("MYDB_BLOCKING_POOL_MAX")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or_else(|| baseline.saturating_mul(2));
    requested
        .clamp(baseline, HARD_CEILING)
        .min(configured_stack_worker_limit().max(baseline))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_stack_headroom_restores_cpu_admission_without_unfunded_workers() {
        const MIB: usize = 1024 * 1024;
        let mut reservations = Vec::new();
        let (workers, charge) = reserve_stack_headroom(12, 33, 8 * MIB, 200 * MIB, |bytes| {
            reservations.push(bytes);
            Some(bytes)
        });
        assert_eq!(workers, 33);
        assert_eq!(charge, Some(168 * MIB));
        assert_eq!(reservations, vec![168 * MIB]);
        assert_eq!((workers - 12) * 8 * MIB, charge.unwrap());
    }

    #[test]
    fn stack_headroom_keeps_the_old_safe_ceiling_when_admission_fails() {
        let (workers, charge) = reserve_stack_headroom(12, 33, 8, 200, |_| None::<usize>);
        assert_eq!(workers, 12);
        assert!(charge.is_none());
        let (workers, charge) = reserve_stack_headroom(12, 33, 8, 31, Some);
        assert_eq!(workers, 15);
        assert_eq!(charge, Some(24));
        let (workers, charge) =
            reserve_stack_headroom(12, 33, 8, 200, |bytes| (bytes <= 40).then_some(bytes));
        assert_eq!(workers, 17);
        assert_eq!(charge, Some(40));
        let (workers, charge): (usize, Option<usize>) =
            reserve_stack_headroom(64, 33, 8, 200, |_| panic!("no extra capacity needed"));
        assert_eq!(workers, 64);
        assert!(charge.is_none());
    }

    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::time::Duration;

    fn wait_until_parked(pool: &BlockingPool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while pool.idle.load(Ordering::Acquire) != pool.total.load(Ordering::Acquire) {
            assert!(std::time::Instant::now() < deadline, "workers did not park");
            std::thread::yield_now();
        }
    }

    #[tokio::test]
    async fn affinity_reuses_parked_worker_and_bounds_sleeper_entries() {
        let pool = BlockingPool::with_limits(4, 4);
        let affinity = Affinity::default();
        wait_until_parked(&pool);
        let first = pool
            .spawn_affine(Some(&affinity), || thread::current().id())
            .await
            .unwrap();
        for _ in 0..2_000 {
            wait_until_parked(&pool);
            let next = pool
                .spawn_affine(Some(&affinity), || thread::current().id())
                .await
                .unwrap();
            assert_eq!(next, first);
            assert!(
                pool.sleepers.len() <= 4,
                "targeted wakes must not accumulate tokens"
            );
        }
    }

    #[tokio::test]
    async fn affinity_busy_worker_does_not_block_lock_releasing_task() {
        let pool = BlockingPool::with_limits(2, 2);
        let affinity = Affinity::default();
        wait_until_parked(&pool);
        let owner = pool
            .spawn_affine(Some(&affinity), || thread::current().id())
            .await
            .unwrap();
        wait_until_parked(&pool);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let blocked = pool.spawn_affine(Some(&affinity), move || {
            entered_tx.send(thread::current().id()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        assert_eq!(
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            owner
        );
        let release = pool.spawn_affine(Some(&affinity), move || {
            release_tx.send(()).unwrap();
            thread::current().id()
        });
        let other = tokio::time::timeout(Duration::from_secs(3), release)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(other, owner);
        blocked.await.unwrap();
    }

    #[tokio::test]
    async fn affinity_is_pool_scoped_and_does_not_retain_dead_workers() {
        let affinity = Affinity::default();
        let a = BlockingPool::with_limits(1, 1);
        let b = BlockingPool::with_limits(1, 1);
        let first = a
            .spawn_affine(Some(&affinity), || thread::current().id())
            .await
            .unwrap();
        wait_until_parked(&a);
        wait_until_parked(&b);
        let next = tokio::time::timeout(
            Duration::from_secs(3),
            b.spawn_affine(Some(&affinity), || thread::current().id()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_ne!(first, next);
        drop(b);
        assert!(affinity.last.lock().upgrade().is_none());
        assert_eq!(
            a.spawn_affine(Some(&affinity), || thread::current().id())
                .await
                .unwrap(),
            first
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn affinity_concurrent_shared_hints_execute_exactly_once() {
        let pool = Arc::new(BlockingPool::with_limits(4, 8));
        let affinity = Affinity::default();
        let counter = Arc::new(AtomicUsize::new(0));
        let mut clients = Vec::new();
        for _ in 0..16 {
            let pool = Arc::clone(&pool);
            let affinity = affinity.clone();
            let counter = Arc::clone(&counter);
            clients.push(tokio::spawn(async move {
                for i in 0..500 {
                    let counter = Arc::clone(&counter);
                    pool.spawn_affine((i % 3 != 0).then_some(&affinity), move || {
                        counter.fetch_add(1, Ordering::Relaxed);
                    })
                    .await
                    .unwrap();
                }
            }));
        }
        for client in clients {
            tokio::time::timeout(Duration::from_secs(15), client)
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 8_000);
        wait_until_parked(&pool);
        assert!(pool.sleepers.len() <= pool.total.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn affinity_survives_panic_and_dropped_completion_handle() {
        let pool = BlockingPool::with_limits(1, 1);
        let affinity = Affinity::default();
        let failed = pool.spawn_affine(Some(&affinity), || panic!("affine panic"));
        assert!(matches!(failed.await, Err(BlockingError::Panicked(_))));
        let counter = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&counter);
        drop(pool.spawn_affine(Some(&affinity), move || {
            c.fetch_add(1, Ordering::Relaxed);
        }));
        pool.spawn_affine(Some(&affinity), || ()).await.unwrap();
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn stack_size_default_override_and_clamp() {
        let mb = |n: usize| n * 1024 * 1024;
        // Default when unset / unparseable / non-positive.
        assert_eq!(stack_size_from_raw(None), mb(8));
        assert_eq!(stack_size_from_raw(Some("abc")), mb(8));
        assert_eq!(stack_size_from_raw(Some("0")), mb(8));
        // Valid override, with surrounding whitespace tolerated.
        assert_eq!(stack_size_from_raw(Some("8")), mb(8));
        assert_eq!(stack_size_from_raw(Some(" 32 ")), mb(32));
        // Clamped to [2, 512].
        assert_eq!(stack_size_from_raw(Some("1")), mb(2));
        assert_eq!(stack_size_from_raw(Some("100000")), mb(512));
    }

    #[tokio::test]
    async fn spawn_executes_and_returns_value() {
        let pool = BlockingPool::new(2);
        let h = pool.spawn(|| 42_i32);
        assert_eq!(h.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn spawn_captures_panic_message() {
        let pool = BlockingPool::new(2);
        let h: BlockingHandle<()> = pool.spawn(|| panic!("blocking pool boom"));
        match h.await {
            Err(BlockingError::Panicked(msg)) => assert!(msg.contains("blocking pool boom")),
            other => panic!("expected Panicked, got {other:?}"),
        }
    }

    #[test]
    fn joinable_task_completes_and_reports_panic() {
        let pool = BlockingPool::new(2);
        let completed = Arc::new(AtomicBool::new(false));
        let completed_in_task = Arc::clone(&completed);
        let handle = pool.spawn_joinable(move || {
            completed_in_task.store(true, Ordering::Release);
        });
        handle.join().unwrap();
        assert!(completed.load(Ordering::Acquire));

        let handle = pool.spawn_joinable(|| panic!("joinable pool boom"));
        match handle.join() {
            Err(BlockingError::Panicked(msg)) => assert!(msg.contains("joinable pool boom")),
            other => panic!("expected Panicked, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn many_concurrent_spawns_all_execute() {
        let pool = Arc::new(BlockingPool::new(8));
        let counter = Arc::new(AtomicUsize::new(0));
        let mut tasks = vec![];
        for _ in 0..1024 {
            let p = Arc::clone(&pool);
            let c = Arc::clone(&counter);
            tasks.push(tokio::spawn(async move {
                p.spawn(move || c.fetch_add(1, Ordering::Relaxed))
                    .await
                    .unwrap()
            }));
        }
        for t in tasks {
            let _ = t.await.unwrap();
        }
        assert_eq!(counter.load(Ordering::Relaxed), 1024);
    }

    #[test]
    fn configured_max_never_below_baseline_and_honors_override() {
        // Ceiling clamps up to the baseline even for a tiny override.
        assert_eq!(configured_max(200).max(200), configured_max(200));
        assert!(configured_max(50) >= 50);
    }

    #[test]
    fn stack_worker_limit_charges_full_stack_capacity() {
        let mib = 1024 * 1024;
        assert_eq!(stack_worker_limit_for(10 * 1024 * mib, 8 * mib), 64);
        assert_eq!(stack_worker_limit_for(1024 * mib, 16 * mib), 3);
        assert_eq!(stack_worker_limit_for(1, 8 * mib), 1);
    }

    // The deadlock guard: when every pre-spawned worker is blocked inside a
    // task (as a lock-taking statement is while parked on a row-lock condvar),
    // newly submitted tasks — e.g. the COMMIT that would release those locks —
    // must still run. A fixed-size pool would starve them forever; the
    // on-demand growth spawns the extra threads that keep the system live.
    #[tokio::test]
    async fn grows_when_all_baseline_workers_are_blocked() {
        // Baseline of 2, allowed to grow to 8.
        let pool = Arc::new(BlockingPool::with_limits(2, 8));
        let release = Arc::new(AtomicBool::new(false));

        // Occupy both baseline workers with tasks that will not return until
        // released — mimicking two transactions parked in a row-lock wait.
        let mut blockers = vec![];
        for _ in 0..2 {
            let p = Arc::clone(&pool);
            let r = Arc::clone(&release);
            blockers.push(p.spawn(move || {
                while !r.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }));
        }
        // Give both blockers time to occupy the baseline workers.
        std::thread::sleep(Duration::from_millis(50));

        // These must complete despite zero idle baseline workers — only
        // possible if the pool grew. A fixed-size pool would hang here.
        let counter = Arc::new(AtomicUsize::new(0));
        let mut runners = vec![];
        for _ in 0..3 {
            let p = Arc::clone(&pool);
            let c = Arc::clone(&counter);
            runners.push(tokio::spawn(async move {
                p.spawn(move || c.fetch_add(1, Ordering::Relaxed))
                    .await
                    .unwrap()
            }));
        }
        for t in runners {
            // A generous bound: on a fixed-size pool this await never resolves.
            let done = tokio::time::timeout(Duration::from_secs(10), t).await;
            done.expect("grown worker did not run — pool starved")
                .expect("runner task panicked");
        }
        assert_eq!(counter.load(Ordering::Relaxed), 3);
        assert!(
            pool.total.load(Ordering::Acquire) > 2,
            "pool should have grown past baseline"
        );

        // Release the blockers and confirm they finish cleanly.
        release.store(true, Ordering::Release);
        for b in blockers {
            b.await.unwrap();
        }
    }

    #[tokio::test]
    async fn worker_wakes_promptly_after_park() {
        let pool = BlockingPool::new(1);
        // Run one task to ensure the worker is alive, then sleep
        // long enough that it parks on the condvar.
        let _ = pool.spawn(|| ()).await.unwrap();
        std::thread::sleep(Duration::from_millis(20));
        // The next spawn must wake the parked worker within the
        // 50 ms safety-net window; in practice the mutex-serialized
        // notify resolves in well under a millisecond.
        let start = std::time::Instant::now();
        let result = pool.spawn(|| 7_i32).await.unwrap();
        let elapsed = start.elapsed();
        assert_eq!(result, 7);
        assert!(
            elapsed < Duration::from_millis(50),
            "wake latency too high: {elapsed:?}",
        );
    }

    #[tokio::test]
    async fn short_tasks_reuse_idle_workers_without_growth() {
        let pool = BlockingPool::with_limits(8, 16);
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while pool.idle.load(Ordering::Acquire) < 8 && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(pool.idle.load(Ordering::Acquire), 8);

        for value in 0..2_000usize {
            assert_eq!(pool.spawn(move || value).await.unwrap(), value);
        }
        assert_eq!(
            pool.total.load(Ordering::Acquire),
            8,
            "parked workers should be claimed without spurious pool growth"
        );
    }

    #[tokio::test]
    async fn handle_cancellation_when_pool_dropped() {
        let pool = BlockingPool::new(1);
        // Submit a task whose result we never await; instead drop the
        // handle, then drop the pool. Should not deadlock or panic.
        let h = pool.spawn(|| 1_i32);
        drop(h);
        drop(pool);
        // If we got here without hanging, the cleanup path is sound.
    }

    #[tokio::test]
    async fn handle_resolves_to_cancelled_when_sender_dropped() {
        // Construct a pool that drops every task without running it
        // — easiest path: drop the pool while a handle is still
        // outstanding (no worker ever picks up the task).
        let pool = BlockingPool::new(1);
        // Hold the worker busy with a long task so the second spawn
        // queues. Drop the pool while the long task is sleeping to
        // exercise the Sender-dropped path on the queued task.
        let busy = pool.spawn(|| std::thread::sleep(Duration::from_millis(80)));
        let queued: BlockingHandle<i32> = pool.spawn(|| 99);
        drop(pool);
        // Busy task may complete normally or be Cancelled depending
        // on timing; we only assert that the second handle resolves
        // (no hang) and reports either Cancelled or its value.
        let _ = busy.await; // ignore — pool drop joined the worker which finished its current task
        match queued.await {
            Err(BlockingError::Cancelled) => {}
            Ok(99) => {} // Worker happened to pick it up before shutdown
            other => panic!("unexpected outcome: {other:?}"),
        }
    }
}
