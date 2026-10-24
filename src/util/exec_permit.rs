//! Transaction admission control.
//!
//! At high connection counts the server runs one in-flight lock-taking statement
//! per connection, so e.g. ~100 TPC-C terminals put ~100 concurrently *open*
//! transactions onto a ~10-core box. Each holds its hot-row X locks (the TPC-C
//! district/warehouse rows, locked from `SELECT ... FOR UPDATE` through commit)
//! for the whole transaction, and the OS deschedules those lock-holding threads
//! under the CPU oversubscription, inflating hold times and collapsing
//! throughput once load is past the saturation knee. Measured on this repo
//! (10 warehouses): 20T = 81.5k tpmC, 40T = 74.6k, 100T = 57.1k — throughput
//! *falls* as terminals rise, because more concurrent transactions means more
//! contention on the same handful of hot rows, not more useful parallelism.
//!
//! This gate bounds the number of concurrently *open, lock-holding* transactions
//! to `MYDB_TXN_ADMISSION`. A session takes an admission permit when it runs its
//! first lock-taking statement and holds it until the transaction commits or
//! rolls back; further transactions wait (async — without occupying a worker
//! thread or holding any lock) for a permit to free. At any instant only ~N
//! transactions contend on the hot rows, reproducing the lower-concurrency
//! operating point where throughput peaks, while every client stays connected.
//!
//! Deadlock-free: a waiting (unadmitted) transaction holds no locks, so it can
//! never block an admitted one; every admitted transaction can always run to
//! commit (statement execution is scheduled by the blocking pool, not gated
//! here) and then releases its permit. The configured/automatic limit is also
//! clamped below the blocking-pool worker ceiling, leaving a worker available
//! for the COMMIT/ROLLBACK operations that release locks. A one-worker pool
//! admits exactly one transaction: it cannot contend with another admitted
//! transaction, so the sole worker remains able to reach that transaction's
//! commit. When `MYDB_TXN_ADMISSION` is unset the requested limit is CPU-scaled
//! (4× logical CPUs, with a minimum of 8).
//! `MYDB_TXN_ADMISSION=0` remains an explicit diagnostic escape hatch; a
//! positive value supplies an operator-requested concurrency point subject to
//! the same worker-safety clamp.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// A permit representing one admitted (open, lock-holding) transaction. Dropping
/// it — on commit/rollback or when the session is dropped — frees the slot.
pub struct AdmissionPermit {
    _normal: tokio::sync::OwnedSemaphorePermit,
}

static PARALLEL_READ_PRESSURE: AtomicUsize = AtomicUsize::new(0);
static PARALLEL_READ_PRESSURE_UNTIL_MS: AtomicU64 = AtomicU64::new(0);
static PARALLEL_READ_PRESSURE_ENTRIES: AtomicU64 = AtomicU64::new(0);
static PARALLEL_READ_PRESSURE_SUPPRESSED_RW_TXN: AtomicU64 = AtomicU64::new(0);
static TXN_ADMISSION_WAITS: AtomicU64 = AtomicU64::new(0);
static TXN_ADMISSION_WAIT_NS: AtomicU64 = AtomicU64::new(0);
static MIXED_ADMISSION_WAITS: AtomicU64 = AtomicU64::new(0);
static MIXED_ADMISSION_WAIT_NS: AtomicU64 = AtomicU64::new(0);
static PARALLEL_READ_GATE_WAITS: AtomicU64 = AtomicU64::new(0);
static PARALLEL_READ_GATE_WAIT_NS: AtomicU64 = AtomicU64::new(0);

// Keep a confirmed analytical connection's pressure lease alive across short
// gaps between its consecutive operators/statements. 250 ms was too short for
// mixed JDBC workloads: writers burst into the gaps and evicted the next AP
// scan's cache working set. A two-second lease is still bounded, is renewed
// only by read-only parallel work, and never changes physical-plan topology.
const PARALLEL_READ_PRESSURE_GRACE_MS: u64 = 2_000;
const MIXED_ADMISSION_SHAPING_MS: u64 = 2;

#[cfg(test)]
pub(crate) fn parallel_read_pressure_grace_ms_for_test() -> u64 {
    PARALLEL_READ_PRESSURE_GRACE_MS
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecPermitStatus {
    pub txn_admission_waits: u64,
    pub txn_admission_wait_us: u64,
    pub mixed_admission_waits: u64,
    pub mixed_admission_wait_us: u64,
    pub parallel_read_pressure_entries: u64,
    pub parallel_read_pressure_suppressed_rw_txn: u64,
    pub parallel_read_pressure_active: usize,
    pub parallel_read_gate_waits: u64,
    pub parallel_read_gate_wait_us: u64,
}

pub fn status_snapshot() -> ExecPermitStatus {
    ExecPermitStatus {
        txn_admission_waits: TXN_ADMISSION_WAITS.load(Ordering::Relaxed),
        txn_admission_wait_us: TXN_ADMISSION_WAIT_NS.load(Ordering::Relaxed) / 1_000,
        mixed_admission_waits: MIXED_ADMISSION_WAITS.load(Ordering::Relaxed),
        mixed_admission_wait_us: MIXED_ADMISSION_WAIT_NS.load(Ordering::Relaxed) / 1_000,
        parallel_read_pressure_entries: PARALLEL_READ_PRESSURE_ENTRIES.load(Ordering::Relaxed),
        parallel_read_pressure_suppressed_rw_txn: PARALLEL_READ_PRESSURE_SUPPRESSED_RW_TXN
            .load(Ordering::Relaxed),
        parallel_read_pressure_active: PARALLEL_READ_PRESSURE.load(Ordering::Acquire),
        parallel_read_gate_waits: PARALLEL_READ_GATE_WAITS.load(Ordering::Relaxed),
        parallel_read_gate_wait_us: PARALLEL_READ_GATE_WAIT_NS.load(Ordering::Relaxed) / 1_000,
    }
}

fn saturating_elapsed_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

/// Decide whether a statement's parallel read represents cross-session
/// analytical pressure. The inputs are captured once at statement start, so
/// this scheduling classification cannot make a physical plan depend on live
/// load.
///
/// A read-only transaction is analytical even when explicitly opened. A
/// read-write transaction is deliberately excluded: its SELECT may be followed
/// by DML in the same transaction, and treating that SELECT as external AP
/// pressure would make the transaction throttle itself and every peer writer.
pub(crate) fn parallel_read_pressure_eligible(
    read_only_txn: bool,
    write_intent_seen: bool,
) -> bool {
    read_only_txn || !write_intent_seen
}

fn monotonic_millis() -> u64 {
    static EPOCH: OnceLock<std::time::Instant> = OnceLock::new();
    EPOCH
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn parallel_read_pressure_active() -> bool {
    PARALLEL_READ_PRESSURE.load(Ordering::Acquire) > 0
        || monotonic_millis() < PARALLEL_READ_PRESSURE_UNTIL_MS.load(Ordering::Acquire)
}

/// Lifetime marker for a CPU-intensive parallel read. Existing admitted
/// transactions are never suspended; only new lock-taking transactions are
/// serialized while at least one marker is live, so commits keep progressing
/// and pure transactional workloads retain the normal CPU-scaled limit.
pub struct ParallelReadPressureGuard {
    gate: Arc<ParallelReadGate>,
    query_id: u64,
}

struct ParallelReadGate {
    // Count live scan leases by statement, not by operator or worker thread.
    // An admitted scan can synchronously evaluate a subquery, and sibling
    // CTE/UNION pipelines can depend on one another. None may wait for a
    // permit held by the same statement. MemBudget's query_id is inherited
    // by all those paths and is never a reusable connection/thread ID.
    active: std::sync::Mutex<std::collections::HashMap<u64, usize>>,
    available: std::sync::Condvar,
    limit: usize,
}

fn parallel_read_gate() -> &'static Arc<ParallelReadGate> {
    static GATE: OnceLock<Arc<ParallelReadGate>> = OnceLock::new();
    GATE.get_or_init(|| {
        let cpus = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(4);
        let degree = crate::util::env::var("MYDB_PARALLEL_DEGREE")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(2);
        Arc::new(ParallelReadGate {
            active: std::sync::Mutex::new(std::collections::HashMap::new()),
            available: std::sync::Condvar::new(),
            // Bound independent analytical statements. Physical topology
            // and the planned degree of every scan remain unchanged; nested
            // and sibling operators share their statement's admission.
            limit: cpus.div_ceil(degree).max(1),
        })
    })
}

impl Drop for ParallelReadPressureGuard {
    fn drop(&mut self) {
        if PARALLEL_READ_PRESSURE.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Avoid opening all waiting writers in the short gaps between
            // consecutive analytical pipelines/clients. The grace period is
            // scheduling state only; it cannot affect a physical plan.
            PARALLEL_READ_PRESSURE_UNTIL_MS.store(
                monotonic_millis().saturating_add(PARALLEL_READ_PRESSURE_GRACE_MS),
                Ordering::Release,
            );
        }
        let mut active = self
            .gate
            .active
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let leases = active.get_mut(&self.query_id).expect("live scan lease");
        *leases -= 1;
        if *leases == 0 {
            active.remove(&self.query_id);
            // Several workers for one waiting statement may be asleep.
            // After admission all of them are eligible to share its slot.
            self.gate.available.notify_all();
        }
    }
}

pub fn enter_parallel_read_pressure(
    query_id: u64,
    eligible: bool,
    check_cancelled: impl Fn() -> Result<(), crate::types::QueryError>,
) -> Result<Option<ParallelReadPressureGuard>, crate::types::QueryError> {
    if !eligible {
        PARALLEL_READ_PRESSURE_SUPPRESSED_RW_TXN.fetch_add(1, Ordering::Relaxed);
        return Ok(None);
    }
    parallel_read_gate()
        .enter(query_id, check_cancelled)
        .map(Some)
}

/// Include interrupted waits in telemetry instead of recording only the
/// waits that eventually acquired a permit.
struct ParallelReadWait(Instant);

impl Drop for ParallelReadWait {
    fn drop(&mut self) {
        PARALLEL_READ_GATE_WAITS.fetch_add(1, Ordering::Relaxed);
        PARALLEL_READ_GATE_WAIT_NS.fetch_add(saturating_elapsed_ns(self.0), Ordering::Relaxed);
    }
}

impl ParallelReadGate {
    fn enter(
        self: &Arc<Self>,
        query_id: u64,
        check_cancelled: impl Fn() -> Result<(), crate::types::QueryError>,
    ) -> Result<ParallelReadPressureGuard, crate::types::QueryError> {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut wait = None;
        loop {
            check_cancelled()?;
            if active.contains_key(&query_id) || active.len() < self.limit {
                break;
            }
            wait.get_or_insert_with(|| ParallelReadWait(Instant::now()));
            // KILL, deadlines, cursor-local cancellation and terminal OOM do not
            // notify this gate. Bound the sleep so each is observed even if all
            // admitted statements remain active indefinitely.
            active = self
                .available
                .wait_timeout(active, Duration::from_millis(20))
                .unwrap_or_else(|poison| poison.into_inner())
                .0;
        }
        let leases = active.entry(query_id).or_default();
        *leases += 1;
        if *leases == 1 {
            // Wake siblings already waiting for this newly admitted statement.
            self.available.notify_all();
        }
        drop(active);
        PARALLEL_READ_PRESSURE_ENTRIES.fetch_add(1, Ordering::Relaxed);
        PARALLEL_READ_PRESSURE.fetch_add(1, Ordering::AcqRel);
        Ok(ParallelReadPressureGuard {
            gate: Arc::clone(self),
            query_id,
        })
    }
}

fn mixed_workload_semaphore() -> &'static Arc<Semaphore> {
    static SEM: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SEM.get_or_init(|| Arc::new(Semaphore::new(mixed_workload_limit())))
}

fn mixed_workload_limit_for(cpus: usize, parallel_degree: usize) -> usize {
    if cpus <= 2 {
        return 1;
    }
    cpus.saturating_sub(parallel_degree.max(1)).clamp(2, 16)
}

fn mixed_workload_limit() -> usize {
    let cpus = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(4);
    let parallel_degree = crate::util::env::var("MYDB_PARALLEL_DEGREE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(2);
    mixed_workload_limit_for(cpus, parallel_degree)
}

pub(super) fn automatic_limit_for(cpus: usize) -> usize {
    cpus.saturating_mul(4).max(8)
}

fn automatic_limit() -> usize {
    automatic_limit_for(
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(4),
    )
}

fn configured_limit(raw: Option<&str>, automatic: usize, blocking_workers: usize) -> usize {
    let requested = match raw.map(str::trim) {
        None | Some("") => automatic,
        Some("0" | "off" | "false" | "no") => return 0,
        Some(value) => value
            .parse::<usize>()
            .ok()
            .filter(|n| *n > 0)
            .map(|n| n.min(4096))
            .unwrap_or(automatic),
    };
    // N lock-holding transactions can park N workers. Keep one worker for
    // the commit/rollback that releases those locks. At a one-worker ceiling,
    // serial admission is itself sufficient to prevent inter-transaction row
    // lock waits, so admit one rather than reducing the semaphore to zero.
    let worker_safe_limit = blocking_workers.saturating_sub(1).max(1);
    requested.min(worker_safe_limit)
}

/// The process-wide admission semaphore, or `None` when unconfigured or
/// explicitly disabled with `MYDB_TXN_ADMISSION=0`.
pub fn global() -> Option<&'static Arc<Semaphore>> {
    static SEM: OnceLock<Option<Arc<Semaphore>>> = OnceLock::new();
    SEM.get_or_init(|| {
        let raw = crate::util::env::var("MYDB_TXN_ADMISSION").ok();
        let n = configured_limit(
            raw.as_deref(),
            automatic_limit(),
            crate::util::blocking_pool::configured_max_workers(),
        );
        if n == 0 {
            None
        } else {
            Some(Arc::new(Semaphore::new(n)))
        }
    })
    .as_ref()
}

/// Acquire normal admission and, while a parallel read is live, the single
/// mixed-workload write slot. This runs before the statement takes any lock.
/// A pressure transition racing the final check can let at most one extra
/// transaction through; it cannot create a lock-holding waiter.
pub async fn acquire() -> Option<AdmissionPermit> {
    let normal_sem = Arc::clone(global()?);
    let normal = match Arc::clone(&normal_sem).try_acquire_owned() {
        Ok(permit) => permit,
        Err(tokio::sync::TryAcquireError::NoPermits) => {
            let started = Instant::now();
            let permit = normal_sem.acquire_owned().await.ok()?;
            TXN_ADMISSION_WAITS.fetch_add(1, Ordering::Relaxed);
            TXN_ADMISSION_WAIT_NS.fetch_add(saturating_elapsed_ns(started), Ordering::Relaxed);
            permit
        }
        Err(tokio::sync::TryAcquireError::Closed) => return None,
    };
    if parallel_read_pressure_active() {
        // Entering this branch always imposes the shaping delay, so it
        // is a real admission wait even when the single slot is immediately
        // available. Normal admission above reads the clock only on actual
        // semaphore contention; this colder mixed path may time the complete
        // queue + shaping interval directly.
        let started = Instant::now();
        let mixed_sem = Arc::clone(mixed_workload_semaphore());
        let permit = match Arc::clone(&mixed_sem).try_acquire_owned() {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => mixed_sem.acquire_owned().await.ok()?,
            Err(tokio::sync::TryAcquireError::Closed) => return None,
        };
        // Shape the one admitted writer rather than releasing a burst at each
        // scan boundary. This wait is async and occurs before any row/metadata
        // lock is acquired, so it consumes neither a blocking worker nor a
        // database resource. Pure TP workloads never enter this branch.
        tokio::time::sleep(std::time::Duration::from_millis(MIXED_ADMISSION_SHAPING_MS)).await;
        MIXED_ADMISSION_WAITS.fetch_add(1, Ordering::Relaxed);
        MIXED_ADMISSION_WAIT_NS.fetch_add(saturating_elapsed_ns(started), Ordering::Relaxed);
        // This semaphore shapes transaction *starts*. The normal admission
        // permit below remains transaction-scoped and continues to bound all
        // open lock holders until COMMIT/ROLLBACK. Holding the mixed permit as
        // well made a long analytical transaction monopolize one CPU slice
        // for its complete lifetime, turning a 2 ms start shaper into an
        // accidental low concurrency ceiling.
        drop(permit);
    }
    Some(AdmissionPermit { _normal: normal })
}

#[cfg(test)]
mod tests {
    use super::{
        automatic_limit, automatic_limit_for, configured_limit, mixed_workload_limit_for,
        parallel_read_pressure_eligible,
    };

    fn enter_pressure(eligible: bool) -> Option<super::ParallelReadPressureGuard> {
        let budget = crate::engine::executor::mem_budget::current_budget().unwrap_or_else(|| {
            std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::new(0))
        });
        super::enter_parallel_read_pressure(
            budget.query_id(),
            eligible,
            crate::engine::cancel::check_cancelled,
        )
        .unwrap()
    }

    #[test]
    fn parallel_read_nested_query_reuses_admission_across_threads() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        use std::sync::{mpsc, Arc};
        use std::time::Duration;

        let budget = Arc::new(MemBudget::new(1024));
        // Model sibling CTE scans holding all slots while an IN/scalar
        // subquery on another worker opens a scan for the same statement.
        let holders = with_query_budget(budget.clone(), || {
            (0..super::parallel_read_gate().limit)
                .map(|_| enter_pressure(true).unwrap())
                .collect::<Vec<_>>()
        });
        let (tx, rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            with_query_budget(budget, || {
                let nested = enter_pressure(true).unwrap();
                tx.send(()).unwrap();
                drop(nested);
            });
        });
        let completed = rx.recv_timeout(Duration::from_secs(1)).is_ok();
        // Release before asserting so the broken implementation can unwind
        // without leaking a permanently blocked test worker.
        drop(holders);
        worker.join().unwrap();
        assert!(
            completed,
            "nested scans must not wait for their own statement's permits"
        );
        assert_eq!(super::status_snapshot().parallel_read_pressure_active, 0);
    }

    fn single_query_gate() -> std::sync::Arc<super::ParallelReadGate> {
        std::sync::Arc::new(super::ParallelReadGate {
            active: std::sync::Mutex::new(std::collections::HashMap::new()),
            available: std::sync::Condvar::new(),
            limit: 1,
        })
    }

    #[test]
    fn parallel_read_gate_keeps_peer_waiting_until_last_scan_releases() {
        use std::sync::mpsc;
        use std::time::Duration;
        let gate = single_query_gate();
        let outer = gate.enter(1, || Ok(())).unwrap();
        let nested = gate.enter(1, || Ok(())).unwrap();
        let (tx, rx) = mpsc::channel();
        let peer_gate = gate.clone();
        let peer = std::thread::spawn(move || {
            let permit = peer_gate.enter(2, || Ok(())).unwrap();
            tx.send(()).unwrap();
            drop(permit);
        });
        let blocked_with_both = rx.recv_timeout(Duration::from_millis(50)).is_err();
        // Release out of acquisition order, as early cursor Drop can do.
        drop(outer);
        let blocked_with_nested = rx.recv_timeout(Duration::from_millis(50)).is_err();
        drop(nested);
        let resumed = rx.recv_timeout(Duration::from_secs(1)).is_ok();
        peer.join().unwrap();
        assert!(blocked_with_both && blocked_with_nested && resumed);
        assert!(gate.active.lock().unwrap().is_empty());
    }

    #[test]
    fn parallel_read_gate_wait_observes_kill_local_cancel_and_deadline() {
        use crate::engine::cancel::{CancellationSnapshot, QueryCancelToken};
        use crate::engine::session_registry::SessionRegistry;
        use std::sync::{mpsc, Arc};
        use std::time::Duration;

        for mode in ["query", "connection", "local", "deadline"] {
            let gate = single_query_gate();
            let holder = gate.enter(1, || Ok(())).unwrap();
            let registry = SessionRegistry::new();
            let handle = registry.register(42, "root".into(), "local".into(), "test".into());
            let _scope = crate::engine::cancel::enter(QueryCancelToken::from_session(handle));
            let _deadline =
                crate::engine::cancel::install_statement_deadline(if mode == "deadline" {
                    Duration::from_millis(40)
                } else {
                    Duration::ZERO
                });
            let cancel = CancellationSnapshot::capture().fork_local();
            let worker_cancel = cancel.clone();
            let worker_gate = Arc::clone(&gate);
            let (started_tx, started_rx) = mpsc::channel();
            let (tx, rx) = mpsc::channel();
            let worker = std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                let result = worker_gate.enter(2, || worker_cancel.check());
                tx.send(result.err().map(|error| error.code())).unwrap();
            });
            started_rx.recv().unwrap();
            match mode {
                "query" => {
                    registry.kill_query(42).unwrap();
                }
                "connection" => {
                    registry.kill_connection(42).unwrap();
                }
                "local" => cancel.cancel_local(),
                _ => {}
            }
            let result = rx.recv_timeout(Duration::from_secs(1));
            drop(holder);
            worker.join().unwrap();
            assert_eq!(
                result.unwrap(),
                Some(if mode == "deadline" { 3024 } else { 1317 }),
                "{mode}"
            );
            assert!(gate.active.lock().unwrap().is_empty());
        }
    }

    #[test]
    fn parallel_read_gate_wait_observes_terminal_oom_and_records_wait() {
        use crate::engine::executor::mem_budget::MemBudget;
        use std::sync::{mpsc, Arc};
        use std::time::Duration;
        let gate = single_query_gate();
        let holder = gate.enter(1, || Ok(())).unwrap();
        let budget = Arc::new(MemBudget::new(1));
        let worker_budget = Arc::clone(&budget);
        let worker_gate = gate.clone();
        let (tx, rx) = mpsc::channel();
        let before = super::status_snapshot();
        let worker = std::thread::spawn(move || {
            let result = worker_gate.enter(2, || worker_budget.terminal_oom().map_or(Ok(()), Err));
            tx.send(result.err().map(|error| error.code())).unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
        budget.latch_terminal_oom(crate::error::catalog::ER_OUTOFMEMORY.error("test OOM"));
        let result = rx.recv_timeout(Duration::from_secs(1));
        drop(holder);
        worker.join().unwrap();
        assert_eq!(result.unwrap(), Some(1037));
        let after = super::status_snapshot();
        assert!(after.parallel_read_gate_waits > before.parallel_read_gate_waits);
        assert!(after.parallel_read_gate_wait_us > before.parallel_read_gate_wait_us);
        assert!(gate.active.lock().unwrap().is_empty());
    }

    #[test]
    fn parallel_read_gate_unwind_releases_shared_leases() {
        let gate = single_query_gate();
        let result = std::panic::catch_unwind(|| {
            let _outer = gate.enter(1, || Ok(())).unwrap();
            let _inner = gate.enter(1, || Ok(())).unwrap();
            panic!("operator panicked");
        });
        assert!(result.is_err());
        assert!(gate.active.lock().unwrap().is_empty());
        drop(gate.enter(2, || Ok(())).unwrap());
    }

    #[test]
    fn automatic_limit_explicit_override_and_disable_are_parsed_safely() {
        let automatic = 48;
        let workers = 65;
        assert_eq!(configured_limit(None, automatic, workers), automatic);
        assert_eq!(configured_limit(Some(""), automatic, workers), automatic);
        assert_eq!(configured_limit(Some("64"), automatic, workers), 64);
        assert_eq!(configured_limit(Some("5000"), automatic, workers), 64);
        for value in ["0", "off", "false", "no"] {
            assert_eq!(configured_limit(Some(value), automatic, workers), 0);
        }
        assert_eq!(
            configured_limit(Some("invalid"), automatic, workers),
            automatic
        );
        assert!(automatic_limit() >= 8);
    }

    #[test]
    fn admission_leaves_a_lock_release_worker_or_serializes_one_worker_pool() {
        assert_eq!(configured_limit(None, 32, 1), 1);
        assert_eq!(configured_limit(None, 32, 2), 1);
        assert_eq!(configured_limit(None, 32, 8), 7);
        assert_eq!(configured_limit(Some("64"), 32, 8), 7);
        assert_eq!(configured_limit(Some("0"), 32, 1), 0);
    }

    #[test]
    fn automatic_limit_is_cpu_scaled_and_saturates_safely() {
        assert_eq!(automatic_limit_for(0), 8);
        assert_eq!(automatic_limit_for(1), 8);
        assert_eq!(automatic_limit_for(10), 40);
        assert_eq!(automatic_limit_for(16), 64);
        assert_eq!(automatic_limit_for(32), 128);
        assert_eq!(automatic_limit_for(1024), 4096);
        assert_eq!(automatic_limit_for(usize::MAX), usize::MAX);
        assert_eq!(configured_limit(None, automatic_limit_for(16), 64), 63);
        assert_eq!(configured_limit(None, automatic_limit_for(16), 256), 64);
    }

    #[test]
    fn mixed_workload_capacity_reserves_parallel_read_cores() {
        assert_eq!(mixed_workload_limit_for(1, 4), 1);
        assert_eq!(mixed_workload_limit_for(2, 1), 1);
        assert_eq!(mixed_workload_limit_for(4, 2), 2);
        assert_eq!(mixed_workload_limit_for(10, 4), 6);
        assert_eq!(mixed_workload_limit_for(32, 4), 16);
    }

    #[test]
    fn parallel_read_pressure_distinguishes_analytics_from_read_write_transactions() {
        // A connection with no write history is analytical even when its JDBC
        // pool configured autocommit=0 globally.
        assert!(parallel_read_pressure_eligible(false, false));
        // Sticky write intent classifies later parallel reads as part of a
        // read-write workload.
        assert!(!parallel_read_pressure_eligible(false, true));
        // An explicitly read-only transaction overrides older connection
        // history and remains analytical by declaration.
        assert!(parallel_read_pressure_eligible(true, true));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn analytical_pressure_shapes_starts_without_owning_transaction_lifetime() {
        let before = super::status_snapshot();
        let pressure = enter_pressure(true).expect("eligible pressure guard");
        let limit = super::mixed_workload_limit();
        let mut admitted = Vec::with_capacity(limit);
        for _ in 0..limit {
            admitted.push(super::acquire().await.expect("normal admission enabled"));
        }
        // Every returned permit still owns normal transaction admission, but
        // its mixed start-shaping lease has already been released. A further
        // writer therefore needs only its own bounded shaping delay; it must
        // not wait for one of the earlier transactions to commit.
        let second = tokio::time::timeout(std::time::Duration::from_secs(1), super::acquire())
            .await
            .expect("mixed start shaper must not retain a transaction lease")
            .expect("normal admission enabled");
        drop(second);
        drop(admitted);
        drop(pressure);

        let after_analytics = super::status_snapshot();
        assert!(
            after_analytics.mixed_admission_waits >= before.mixed_admission_waits.saturating_add(2)
        );
        assert_eq!(after_analytics.parallel_read_pressure_active, 0);

        // Let the deliberate post-AP grace expire, then prove a read-write
        // transaction's parallel read neither creates a guard nor sends the
        // next writer through mixed admission.
        tokio::time::sleep(std::time::Duration::from_millis(
            super::parallel_read_pressure_grace_ms_for_test() + 10,
        ))
        .await;
        assert!(enter_pressure(false).is_none());
        let before_rw = super::status_snapshot();
        let permit = super::acquire().await.expect("normal admission enabled");
        drop(permit);
        let after_rw = super::status_snapshot();
        assert_eq!(
            after_rw.mixed_admission_waits, before_rw.mixed_admission_waits,
            "suppressed read-write pressure must not shape a peer writer"
        );
        assert!(
            after_rw.parallel_read_pressure_suppressed_rw_txn
                > before.parallel_read_pressure_suppressed_rw_txn
        );
    }
}
