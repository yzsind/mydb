//! Statement-level cancellation.
//!
//! Owns the data path for `KILL [CONNECTION|QUERY] N` and
//! `max_execution_time` deadlines. Replaces the older thread-local
//! borrowed-pointer design with a cross-thread-shareable
//! [`QueryCancelToken`] so parallel workers, the vectorized scheduler,
//! and any future detached worker (async DDL backfill, event scheduler)
//! see the same cancellation signal as the main statement thread.
//!
//! Design choices, in case future agents wonder why:
//!
//! - `QueryCancelToken` is `Clone` and backed by `Arc<Inner>`. Cloning is a
//!   single relaxed atomic increment; `MYDB_PARALLEL_DEGREE` ≤ 4, so the
//!   total clone traffic per query is trivial.
//! - The hot path is `token.check()`: one Relaxed bool load (sticky
//!   short-circuit) + one Instant compare + two Acquire u64 loads against
//!   the live `SessionHandle`. Zero allocation, zero borrow checks once
//!   the sticky bit flips.
//! - Worker threads pull a clone before `thread::scope` and call
//!   [`enter`] in the worker body — that just replaces a thread-local
//!   `Option<QueryCancelToken>`, no atomic-refcount traffic on the
//!   check path.
//! - The legacy `Arc<dyn Fn() -> Option<QueryError>>` fallback slot is
//!   gone — it had no remaining writer and only added a `RefCell::borrow`
//!   to every hot-path check.

use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::engine::session_registry::{ProcessStageGuard, ProcessState, SessionHandle};
use crate::types::QueryError;

/// Amortize deadline/TLS checks across bounded scalar input batches.
/// Side effects and long-running kernels also keep their own checkpoints.
pub(crate) const SCALAR_ROW_CANCEL_MASK: usize = 255;

/// Process-wide KILL telemetry. Read by `SHOW ENGINE MYDB STATUS`.
static KILLED_QUERIES: AtomicU64 = AtomicU64::new(0);
static KILLED_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
static KILL_OBSERVED: AtomicU64 = AtomicU64::new(0);

pub fn record_killed_query() {
    KILLED_QUERIES.fetch_add(1, Ordering::Relaxed);
}

pub fn record_killed_connection() {
    KILLED_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
}

fn record_kill_observed() {
    KILL_OBSERVED.fetch_add(1, Ordering::Relaxed);
}

#[derive(Debug, Clone, Copy, Default)]
pub struct KillStats {
    pub killed_queries: u64,
    pub killed_connections: u64,
    pub kill_observed: u64,
}

pub fn snapshot_kill_stats() -> KillStats {
    KillStats {
        killed_queries: KILLED_QUERIES.load(Ordering::Relaxed),
        killed_connections: KILLED_CONNECTIONS.load(Ordering::Relaxed),
        kill_observed: KILL_OBSERVED.load(Ordering::Relaxed),
    }
}

pub fn interrupted_error() -> QueryError {
    crate::error::catalog::ER_QUERY_INTERRUPTED.error("Query execution was interrupted")
}

pub fn execution_time_exceeded_error() -> QueryError {
    crate::error::catalog::ER_QUERY_TIMEOUT
        .error("Query execution was interrupted, maximum statement execution time exceeded")
}

/// Shared state behind a [`QueryCancelToken`]. The single `Arc` is the
/// unit of sharing across all worker threads cooperating on one
/// statement: cloning a token just bumps this Arc's refcount.
struct Inner {
    base_query_generation: u64,
    base_connection_generation: u64,
    session: Option<Arc<SessionHandle>>,
    /// Parent cancellation scope for cursor-owned workers. Cancelling the
    /// child stops its worker tree without poisoning the statement token;
    /// KILL/deadline state still propagates from parent to child.
    parent: Option<QueryCancelToken>,
    /// Latches `true` the first time `check()` reports cancelled. Once
    /// latched, every subsequent `check()` short-circuits on a single
    /// relaxed bool load — no atomic Acquire loads against the session.
    sticky: AtomicBool,
}

/// Statement-level cancel handle. Cheap to clone; worker threads take a
/// clone before spawning and call [`enter`] in the worker body so
/// [`check_cancelled`] reports the same KILL signal as the main thread.
#[derive(Clone)]
pub struct QueryCancelToken {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for QueryCancelToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryCancelToken")
            .field("has_session", &self.inner.session.is_some())
            .field("sticky", &self.inner.sticky.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl QueryCancelToken {
    /// Build a token bound to `session` with cancellation generations
    /// snapshotted at construction time. Any KILL issued *after* this
    /// call will be observed.
    pub fn from_session(session: Arc<SessionHandle>) -> Self {
        let base_query_generation = session.query_cancel_generation();
        let base_connection_generation = session.connection_kill_generation();
        Self {
            inner: Arc::new(Inner {
                base_query_generation,
                base_connection_generation,
                session: Some(session),
                parent: None,
                sticky: AtomicBool::new(false),
            }),
        }
    }

    /// Whether this token can serve as the fresh statement token for a
    /// session whose current generations are (`query_generation`,
    /// `connection_generation`) and whose registry handle is `session`.
    /// True only when the token was minted from the same handle with the
    /// same baselines and has not latched — i.e. re-minting would produce
    /// a semantically identical token. Lets the per-command
    /// `refresh_cancel_generations` skip one `Arc<Inner>` alloc/free per
    /// statement in the no-KILL steady state.
    pub fn is_reusable_for(
        &self,
        session: &Arc<SessionHandle>,
        query_generation: u64,
        connection_generation: u64,
    ) -> bool {
        let inner = &*self.inner;
        inner.base_query_generation == query_generation
            && inner.base_connection_generation == connection_generation
            && !inner.sticky.load(Ordering::Relaxed)
            && inner
                .session
                .as_ref()
                .is_some_and(|held| Arc::ptr_eq(held, session))
    }

    /// A token that never reports cancelled. Used for tests, internal
    /// background jobs that have no owning session, and the initial
    /// `Default` of `SessionContext` before the session is bound.
    pub fn noop() -> Self {
        Self {
            inner: Arc::new(Inner {
                base_query_generation: 0,
                base_connection_generation: 0,
                session: None,
                parent: None,
                sticky: AtomicBool::new(false),
            }),
        }
    }

    /// Create an independently cancellable child scope. The child observes
    /// its parent's KILL/local cancellation, while cancelling the child does
    /// not change the parent statement token.
    pub fn fork_local(&self) -> Self {
        Self {
            inner: Arc::new(Inner {
                base_query_generation: 0,
                base_connection_generation: 0,
                session: None,
                parent: Some(self.clone()),
                sticky: AtomicBool::new(false),
            }),
        }
    }

    /// Cancel work that shares this token without recording a user-issued
    /// `KILL`. Streaming cursors use this when their consumer disappears so
    /// detached producers stop at the next cancellation point.
    pub fn cancel_local(&self) {
        self.inner.sticky.store(true, Ordering::Release);
    }

    /// Check whether the owning session has been KILLed since this
    /// token was minted. Latches on first hit.
    #[inline]
    pub fn check(&self) -> Result<(), QueryError> {
        let inner = &*self.inner;
        if inner.sticky.load(Ordering::Relaxed) {
            return Err(interrupted_error());
        }
        if let Some(parent) = &inner.parent {
            if let Err(err) = parent.check() {
                inner.sticky.store(true, Ordering::Relaxed);
                return Err(err);
            }
        }
        if let Some(handle) = &inner.session {
            if handle.is_connection_killed_since(inner.base_connection_generation)
                || handle.is_query_cancelled_since(inner.base_query_generation)
            {
                inner.sticky.store(true, Ordering::Relaxed);
                record_kill_observed();
                return Err(interrupted_error());
            }
        }
        Ok(())
    }
}

thread_local! {
    /// Currently active token on this thread. Replaced by [`enter`] for
    /// the duration of its `CancelScope`. Worker threads start empty
    /// and must `enter` a token they cloned from the main thread.
    static CURRENT_TOKEN: RefCell<Option<QueryCancelToken>> = const { RefCell::new(None) };

    /// `max_execution_time` deadline for the current statement (or any
    /// outer statement on this thread). Lives separately from the token
    /// so a nested subquery's deadline guard can restore the outer
    /// deadline on Drop without touching the token.
    static STATEMENT_DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
    // Distinguish an outer query with an explicitly disabled deadline from
    // a thread that has not entered a query deadline scope yet.
    static STATEMENT_DEADLINE_INSTALLED: Cell<bool> = const { Cell::new(false) };
}

/// The single entry point operators call from anywhere in the synchronous
/// executor stack. Returns immediately when no token / deadline is in
/// scope — the cost on the no-cancellation hot path is one RefCell
/// borrow and one `Cell::get`.
#[inline]
pub fn check_cancelled() -> Result<(), QueryError> {
    // A terminal memory failure is the statement's primary error. Check it
    // before KILL/deadline state so sibling workers cannot overwrite 1037
    // with the generic 1317 interruption while unwinding.
    crate::engine::executor::mem_budget::check_terminal_oom()?;
    if let Some(deadline) = STATEMENT_DEADLINE.with(|c| c.get()) {
        if Instant::now() >= deadline {
            return Err(execution_time_exceeded_error());
        }
    }
    CURRENT_TOKEN.with(|cell| match cell.borrow().as_ref() {
        Some(token) => token.check(),
        None => Ok(()),
    })
}

/// Clone the current thread's token, if any. Worker threads call this
/// before spawning so each child can `enter` its own clone.
pub fn current_token() -> Option<QueryCancelToken> {
    CURRENT_TOKEN.with(|cell| cell.borrow().clone())
}

/// Enter an observable MySQL process-list phase for the current statement.
/// Parallel workers inherit the same session through their cancellation
/// token, so their overlapping phases are merged by fixed counters.
pub(crate) fn enter_process_stage(state: ProcessState) -> Option<ProcessStageGuard> {
    CURRENT_TOKEN.with(|cell| {
        cell.borrow()
            .as_ref()
            .and_then(|token| token.inner.session.as_ref())
            .map(|handle| handle.enter_stage(state))
    })
}

pub(crate) fn set_process_base_state(state: ProcessState) {
    CURRENT_TOKEN.with(|cell| {
        if let Some(handle) = cell
            .borrow()
            .as_ref()
            .and_then(|token| token.inner.session.as_ref())
        {
            handle.set_base_state(state);
        }
    });
}

/// Cancellation state inherited by a worker spawned for the current
/// statement. The deadline is absolute, so moving execution to another
/// thread never restarts `max_execution_time`.
#[derive(Clone, Debug)]
pub struct CancellationSnapshot {
    token: QueryCancelToken,
    deadline: Option<Instant>,
    deadline_installed: bool,
}

impl CancellationSnapshot {
    pub fn capture() -> Self {
        Self {
            token: current_token().unwrap_or_else(QueryCancelToken::noop),
            deadline: STATEMENT_DEADLINE.with(Cell::get),
            deadline_installed: STATEMENT_DEADLINE_INSTALLED.with(Cell::get),
        }
    }

    /// Install this statement's cancellation state on the current thread.
    pub fn enter(&self) -> CancellationSnapshotScope {
        let token_scope = enter(self.token.clone());
        let previous_deadline = STATEMENT_DEADLINE.with(|cell| cell.replace(self.deadline));
        let previous_deadline_installed =
            STATEMENT_DEADLINE_INSTALLED.with(|cell| cell.replace(self.deadline_installed));
        CancellationSnapshotScope {
            token_scope: Some(token_scope),
            previous_deadline,
            previous_deadline_installed,
        }
    }

    /// Derive a cursor/worker-owned cancellation scope from this statement.
    /// The absolute deadline and external KILL ancestry are preserved.
    pub fn fork_local(&self) -> Self {
        Self {
            token: self.token.fork_local(),
            deadline: self.deadline,
            deadline_installed: self.deadline_installed,
        }
    }

    /// Stop workers owned by a cursor whose consumer has gone away.
    pub fn cancel_local(&self) {
        self.token.cancel_local();
    }

    /// Cancel unfinished work, close its consumer channel, then join the worker.
    /// Closing first releases a producer blocked on a full bounded channel.
    pub(crate) fn finish_channel_worker<T>(
        &self,
        receiver: &mut Option<std::sync::mpsc::Receiver<T>>,
        worker: &mut Option<std::thread::JoinHandle<()>>,
    ) {
        if worker.as_ref().is_some_and(|worker| !worker.is_finished()) {
            self.cancel_local();
        }
        drop(receiver.take());
        if let Some(worker) = worker.take() {
            let _ = worker.join();
        }
    }

    /// Check captured cancellation state without relying on the caller's
    /// thread-locals. Cursors call this before returning buffered rows.
    pub fn check(&self) -> Result<(), QueryError> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(execution_time_exceeded_error());
        }
        self.token.check()
    }
}

pub struct CancellationSnapshotScope {
    token_scope: Option<CancelScope>,
    previous_deadline: Option<Instant>,
    previous_deadline_installed: bool,
}

impl Drop for CancellationSnapshotScope {
    fn drop(&mut self) {
        STATEMENT_DEADLINE.with(|cell| cell.set(self.previous_deadline));
        STATEMENT_DEADLINE_INSTALLED.with(|cell| cell.set(self.previous_deadline_installed));
        drop(self.token_scope.take());
    }
}

/// Install `token` as the current thread's cancel context. The returned
/// guard restores the previous value on Drop.
pub fn enter(token: QueryCancelToken) -> CancelScope {
    let prev = CURRENT_TOKEN.with(|cell| cell.replace(Some(token)));
    CancelScope { prev }
}

pub struct CancelScope {
    prev: Option<QueryCancelToken>,
}

impl Drop for CancelScope {
    fn drop(&mut self) {
        CURRENT_TOKEN.with(|cell| {
            *cell.borrow_mut() = self.prev.take();
        });
    }
}

/// Install a statement deadline of `dur` from now. `dur == 0` clears
/// the deadline. The returned RAII guard restores the prior deadline
/// (typically `None`, or an outer statement's deadline for nested
/// subqueries) on Drop.
pub fn install_statement_deadline(dur: Duration) -> StatementDeadlineGuard {
    let deadline = if dur.is_zero() {
        None
    } else {
        Some(Instant::now() + dur)
    };
    let prev = STATEMENT_DEADLINE.with(|c| c.replace(deadline));
    let prev_installed = STATEMENT_DEADLINE_INSTALLED.with(|c| c.replace(true));
    StatementDeadlineGuard {
        prev,
        prev_installed,
    }
}

/// Only the outer query chooses a deadline. Nested SELECT/CTE dispatch must
/// neither restart its clock nor re-enable a cap disabled by an outer hint.
pub fn install_query_deadline(dur: Duration) -> Option<StatementDeadlineGuard> {
    if STATEMENT_DEADLINE_INSTALLED.with(Cell::get) {
        None
    } else {
        Some(install_statement_deadline(dur))
    }
}

pub struct StatementDeadlineGuard {
    prev: Option<Instant>,
    prev_installed: bool,
}

impl Drop for StatementDeadlineGuard {
    fn drop(&mut self) {
        STATEMENT_DEADLINE.with(|c| c.set(self.prev));
        STATEMENT_DEADLINE_INSTALLED.with(|c| c.set(self.prev_installed));
    }
}

/// Compatibility wrapper: install the session's [`QueryCancelToken`]
/// (already present on `session`) onto this thread. The session-bound
/// token is built once at the top-level statement entry in
/// `mysql::session`; nested `execute_with_session` calls (subqueries,
/// stored programs, triggers) reuse the same token rather than
/// re-snapshotting generations — that way a KILL that fires *after*
/// the statement starts but *before* the nested call begins is still
/// observed.
pub fn install_session_cancel_context(session: &crate::engine::api::SessionContext) -> CancelScope {
    let token = session
        .current_cancel_token()
        .cloned()
        .unwrap_or_else(QueryCancelToken::noop);
    enter(token)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::session_registry::SessionRegistry;

    #[test]
    fn noop_token_never_reports_cancelled() {
        let token = QueryCancelToken::noop();
        assert!(token.check().is_ok());
        // sticky stays false
        assert!(token.check().is_ok());
    }

    #[test]
    fn session_token_observes_kill_query() {
        let registry = SessionRegistry::new();
        let handle = registry.register(42, "root".into(), "localhost".into(), "test".into());
        let token = QueryCancelToken::from_session(Arc::clone(&handle));
        assert!(token.check().is_ok());
        registry.kill_query(42).unwrap();
        let err = token.check().unwrap_err();
        assert_eq!(err.code(), 1317);
        // sticky: subsequent calls still fail
        assert!(token.check().is_err());
    }

    #[test]
    fn session_token_observes_kill_connection() {
        let registry = SessionRegistry::new();
        let handle = registry.register(43, "root".into(), "localhost".into(), "test".into());
        let token = QueryCancelToken::from_session(Arc::clone(&handle));
        registry.kill_connection(43).unwrap();
        assert!(token.check().is_err());
    }

    #[test]
    fn enter_scope_restores_previous_token() {
        let outer = QueryCancelToken::noop();
        let _outer_scope = enter(outer.clone());
        {
            let inner = QueryCancelToken::noop();
            let _inner_scope = enter(inner);
            assert!(current_token().is_some());
        }
        assert!(current_token().is_some());
    }

    #[test]
    fn cloned_token_shares_sticky_bit() {
        let registry = SessionRegistry::new();
        let _handle = registry.register(44, "root".into(), "localhost".into(), "test".into());
        let a = QueryCancelToken::from_session(Arc::clone(&_handle));
        let b = a.clone();
        registry.kill_query(44).unwrap();
        assert!(a.check().is_err());
        // The sticky bit set via `a` is also observed via `b` because
        // both share the same Inner.
        assert!(b.check().is_err());
    }

    #[test]
    fn statement_deadline_fires() {
        let _g = install_statement_deadline(Duration::from_millis(1));
        std::thread::sleep(Duration::from_millis(20));
        let err = check_cancelled().unwrap_err();
        assert_eq!(err.code(), 3024);
    }

    #[test]
    fn nested_query_deadlines_preserve_absolute_and_disabled_scopes_across_workers() {
        for duration in [Duration::ZERO, Duration::from_secs(1)] {
            let outer = install_query_deadline(duration).expect("outer query owns deadline");
            let deadline = STATEMENT_DEADLINE.with(Cell::get);
            let snapshot = CancellationSnapshot::capture();
            std::thread::spawn(move || {
                let _scope = snapshot.enter();
                for nested in [
                    Duration::ZERO,
                    Duration::from_millis(1),
                    Duration::from_secs(10),
                ] {
                    assert!(install_query_deadline(nested).is_none());
                    assert_eq!(STATEMENT_DEADLINE.with(Cell::get), deadline);
                }
            })
            .join()
            .unwrap();
            drop(outer);
            assert!(!STATEMENT_DEADLINE_INSTALLED.with(Cell::get));
            assert!(STATEMENT_DEADLINE.with(Cell::get).is_none());
        }
    }

    #[test]
    fn cancellation_snapshot_keeps_absolute_deadline_across_threads() {
        let _g = install_statement_deadline(Duration::from_millis(10));
        let snapshot = CancellationSnapshot::capture();
        std::thread::sleep(Duration::from_millis(20));
        let code = std::thread::spawn(move || {
            let _scope = snapshot.enter();
            check_cancelled().unwrap_err().code()
        })
        .join()
        .unwrap();
        assert_eq!(code, 3024);
    }

    #[test]
    fn local_snapshot_cancel_stops_worker_without_kill_telemetry() {
        let before = snapshot_kill_stats();
        let snapshot = CancellationSnapshot::capture();
        let worker_snapshot = snapshot.clone();
        snapshot.cancel_local();
        let code = std::thread::spawn(move || {
            let _scope = worker_snapshot.enter();
            check_cancelled().unwrap_err().code()
        })
        .join()
        .unwrap();
        assert_eq!(code, 1317);
        let after = snapshot_kill_stats();
        assert_eq!(after.kill_observed, before.kill_observed);
    }

    #[test]
    fn child_cancel_does_not_poison_parent_statement() {
        let parent = CancellationSnapshot::capture();
        let child = parent.fork_local();
        child.cancel_local();
        assert_eq!(child.check().unwrap_err().code(), 1317);
        assert!(parent.check().is_ok());
    }

    /// Regression: the P0 audit finding was that parallel `thread::scope`
    /// workers had an empty thread-local cancel context, so the
    /// `check_cancelled()` calls inside their inner loops became no-ops.
    /// This test wires a token across a `thread::scope` worker exactly the
    /// way the parallel-scan operators do and verifies that a KILL issued
    /// on the main thread is observed by the worker via
    /// `crate::engine::cancel::check_cancelled()`.
    #[test]
    fn worker_inheriting_token_observes_kill() {
        let registry = SessionRegistry::new();
        let handle = registry.register(101, "root".into(), "localhost".into(), "test".into());
        let token = QueryCancelToken::from_session(Arc::clone(&handle));
        let started = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let observed: std::sync::Mutex<Option<u16>> = std::sync::Mutex::new(None);
        let observed_ref = &observed;
        let started_ref = &started;
        std::thread::scope(|s| {
            let token = token.clone();
            s.spawn(move || {
                let _scope = enter(token);
                started_ref.store(true, Ordering::Release);
                let deadline = std::time::Instant::now() + Duration::from_secs(2);
                loop {
                    if let Err(e) = check_cancelled() {
                        *observed_ref.lock().unwrap() = Some(e.code());
                        return;
                    }
                    if std::time::Instant::now() >= deadline {
                        *observed_ref.lock().unwrap() = Some(u16::MAX);
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            });
            while !started.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(1));
            }
            std::thread::sleep(Duration::from_millis(20));
            registry.kill_query(101).unwrap();
        });
        assert_eq!(*observed.lock().unwrap(), Some(1317));
    }

    /// Companion to the above: `worker_inheriting_token` must NOT trip on
    /// a KILL targeted at *another* connection.
    #[test]
    fn worker_inheriting_token_ignores_other_session_kill() {
        let registry = SessionRegistry::new();
        let h1 = registry.register(201, "root".into(), "localhost".into(), "test".into());
        let _h2 = registry.register(202, "root".into(), "localhost".into(), "test".into());
        let token = QueryCancelToken::from_session(Arc::clone(&h1));

        let started = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed: std::sync::Mutex<Option<u16>> = std::sync::Mutex::new(None);
        let observed_ref = &observed;
        let started_ref = &started;
        std::thread::scope(|s| {
            let token = token.clone();
            s.spawn(move || {
                let _scope = enter(token);
                started_ref.store(true, Ordering::Release);
                let deadline = std::time::Instant::now() + Duration::from_millis(200);
                while std::time::Instant::now() < deadline {
                    if let Err(e) = check_cancelled() {
                        *observed_ref.lock().unwrap() = Some(e.code());
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
                *observed_ref.lock().unwrap() = Some(0);
            });
            while !started.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(1));
            }
            registry.kill_query(202).unwrap();
        });
        assert_eq!(*observed.lock().unwrap(), Some(0));
    }

    #[test]
    fn kill_stats_counter_increments() {
        let before = snapshot_kill_stats();
        let registry = SessionRegistry::new();
        let _h = registry.register(301, "root".into(), "localhost".into(), "test".into());
        registry.kill_query(301).unwrap();
        registry.kill_query(301).unwrap();
        registry.kill_connection(301).unwrap();
        let after = snapshot_kill_stats();
        assert_eq!(after.killed_queries - before.killed_queries, 2);
        assert_eq!(after.killed_connections - before.killed_connections, 1);
    }
}
