//! Receiver task lifecycle for the mydb-native physical-replication
//! replica.
//!
//! `START MYDB REPLICATION` / `STOP MYDB REPLICATION` / boot-time
//! auto-start all funnel through this module. The runtime is owned by
//! the application (`#[tokio::main]` in `src/main.rs`), but the SQL
//! handlers run inside `spawn_blocking`, so we can't call
//! `tokio::spawn` from there directly. Instead the manager captures a
//! [`tokio::runtime::Handle`] at process boot (via
//! [`set_runtime_handle`]) and the lifecycle uses
//! [`Handle::spawn`] to attach a long-running receiver task.
//!
//! Each running receiver pair owns one [`tokio::sync::Notify`]
//! (shutdown signal) and the receiver loop's `JoinHandle`. Starting
//! while already running is a no-op; stopping while already stopped is
//! a no-op. Both are idempotent so admin scripts can replay
//! `START`/`STOP` without surprises.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex as PlMutex;
use tokio::runtime::Handle;
use tokio::sync::Notify;

use crate::repl::config::ReplConfig;
use crate::repl::replica::{Receiver, ReceiverEvent, ReplicaProgress};
use crate::storage::{error, Database};
use crate::types::QueryError;
use crate::util::logging::log_message;

/// State shared between the SQL admin handlers and the running
/// receiver task. Held inside an `Arc<PlMutex<...>>` on
/// [`crate::repl::MydbReplManager`].
#[derive(Default)]
pub struct ReceiverLifecycle {
    /// The shutdown signal handed to the running outer loop. Notifying
    /// it asks the receiver loop to exit at the next safe point.
    shutdown: Option<Arc<Notify>>,
    /// Sticky shutdown flag paired with `shutdown`. The `Notify` alone
    /// is not enough: it does not buffer between waiters, so a
    /// `notify_waiters` that fires while no one is currently awaiting
    /// `.notified()` is lost. The outer reconnect loop re-creates a
    /// fresh `notified()` future on each iteration; without this flag
    /// a shutdown notified while the inner `run_once` was running
    /// (consumed by *its* notified future) would not stop the next
    /// iteration. The flag is set inside `stop_and_wait` /
    /// `stop` before `notify_waiters` so the loop's next "should I
    /// keep going?" check sees `true`.
    shutdown_flag: Option<Arc<AtomicBool>>,
    /// Join handle for the outer loop. Awaited (with timeout) inside
    /// `stop_blocking` so the next `start` can't race against a
    /// shutdown in flight.
    join: Option<tokio::task::JoinHandle<()>>,
}

impl ReceiverLifecycle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_running(&self) -> bool {
        self.shutdown.is_some()
            && self
                .join
                .as_ref()
                .map(|join| !join.is_finished())
                .unwrap_or(false)
    }

    /// Async stop — notifies shutdown and awaits the task's join with
    /// a timeout. Used by tests that already run on the tokio runtime
    /// and need to deterministically wait for the receiver loop to
    /// finish before asserting "replica saw no new rows after STOP".
    pub async fn stop_and_wait(&mut self) {
        let Some(shutdown) = self.shutdown.take() else {
            self.shutdown_flag = None;
            self.join = None;
            return;
        };
        if let Some(flag) = self.shutdown_flag.take() {
            flag.store(true, Ordering::Release);
        }
        let Some(join) = self.join.take() else {
            return;
        };
        shutdown.notify_waiters();
        let _ = tokio::time::timeout(STOP_TIMEOUT, join).await;
    }

    /// Spin up the receiver loop. The actual receiver task is spawned
    /// via the supplied runtime `handle`. The outer loop wraps
    /// `Receiver::run_once` with a reconnect backoff so a transient
    /// master-side disconnect (or fork-detection rejection) does not
    /// require operator intervention.
    pub fn start(
        &mut self,
        handle: Handle,
        db: Arc<Database>,
        log: Arc<crate::wal::LogMutex>,
        data_dir: std::path::PathBuf,
        master_endpoint: String,
        replica_uuid: String,
        config: ReplConfig,
        initial_progress: ReplicaProgress,
        status: Arc<PlMutex<String>>,
        live_progress: Arc<PlMutex<ReplicaProgress>>,
        role_state: Arc<std::sync::Mutex<crate::repl::role::RoleState>>,
    ) -> Result<(), QueryError> {
        if self.shutdown.is_some() {
            if self.is_running() {
                return Ok(());
            }
            // A prior receiver exited naturally (resync/poison/fatal error).
            // Drop its completed handle and permit an explicit restart.
            self.shutdown = None;
            self.shutdown_flag = None;
            self.join = None;
        }
        let (host, port) = master_endpoint.rsplit_once(':').ok_or_else(|| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("master_endpoint '{master_endpoint}' must be 'host:port'"),
            )
        })?;
        let port: u16 = port.parse().map_err(|_| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("invalid master port in '{master_endpoint}'"),
            )
        })?;
        let host = host.to_string();
        let shutdown = Arc::new(Notify::new());
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_for_task = Arc::clone(&shutdown);
        let shutdown_flag_for_task = Arc::clone(&shutdown_flag);
        let backoff = config.reconnect_backoff;
        let cfg_for_task = config.clone();
        let join = handle.spawn(async move {
            run_reconnect_loop(
                shutdown_for_task,
                shutdown_flag_for_task,
                db,
                log,
                data_dir,
                host,
                port,
                replica_uuid,
                cfg_for_task,
                initial_progress,
                backoff,
                status,
                live_progress,
                role_state,
            )
            .await;
        });
        self.shutdown = Some(shutdown);
        self.shutdown_flag = Some(shutdown_flag);
        self.join = Some(join);
        Ok(())
    }

    /// Notify the receiver loop, then wait briefly for it to join.
    ///
    /// Two callers exist, and they need different waiting strategies:
    ///
    /// * Production: dispatched from inside `spawn_blocking`, on a
    ///   blocking worker thread that is NOT a runtime worker. Sync
    ///   waiting (`std::sync::mpsc::recv_timeout`) is fine there
    ///   because the runtime's main worker threads remain free to
    ///   drive the detached await task.
    ///
    /// * Tests: dispatched from inside `#[tokio::test]`, on the
    ///   runtime's main worker thread. Sync waiting there would block
    ///   the very thread that needs to drive the spawned task —
    ///   classic deadlock. So we just notify, detach the await, and
    ///   return; the caller can poll `is_running()` if it needs to
    ///   observe completion (or just trust that the next `start` will
    ///   be idempotent until the prior task drains).
    pub fn stop(&mut self, handle: Handle) -> Result<(), QueryError> {
        let Some(shutdown) = self.shutdown.take() else {
            self.shutdown_flag = None;
            self.join = None;
            return Ok(());
        };
        if let Some(flag) = self.shutdown_flag.take() {
            flag.store(true, Ordering::Release);
        }
        let Some(join) = self.join.take() else {
            return Ok(());
        };
        shutdown.notify_waiters();
        let on_runtime = Handle::try_current().is_ok();
        if on_runtime {
            // Don't block — driving the spawn requires the very
            // thread we're on. Detach and rely on the receiver's
            // cooperative shutdown.
            handle.spawn(async move {
                let _ = tokio::time::timeout(STOP_TIMEOUT, join).await;
            });
        } else {
            // Blocking-worker path. Park the thread on a sync channel
            // until the detached await reports the join completed.
            let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);
            handle.spawn(async move {
                let _ = tokio::time::timeout(STOP_TIMEOUT, join).await;
                let _ = tx.send(());
            });
            let _ = rx.recv_timeout(STOP_TIMEOUT + Duration::from_secs(1));
        }
        Ok(())
    }
}

const STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// Outer reconnect loop. Each iteration:
/// 1. Tries `Receiver::run_once`. On success (clean shutdown / master
///    closed) we keep looping unless `shutdown` was notified.
/// 2. On a `ResyncRequired` event we surface a log message and exit
///    the loop — Item 3's streaming base backup handles automatic
///    re-base-backup; until then the operator must intervene.
/// 3. On any other error we sleep `backoff` then reconnect.
async fn run_reconnect_loop(
    shutdown: Arc<Notify>,
    shutdown_flag: Arc<AtomicBool>,
    db: Arc<Database>,
    log: Arc<crate::wal::LogMutex>,
    data_dir: std::path::PathBuf,
    host: String,
    port: u16,
    replica_uuid: String,
    config: ReplConfig,
    mut initial_progress: ReplicaProgress,
    backoff: Duration,
    status: Arc<PlMutex<String>>,
    live_progress: Arc<PlMutex<ReplicaProgress>>,
    role_state: Arc<std::sync::Mutex<crate::repl::role::RoleState>>,
) {
    let mut resync_required = false;
    // Poison-frame circuit breaker (C5): a deterministic apply error
    // (decode failure, WAL gap, chunk-metadata mismatch) recurs on every
    // reconnect because we re-request the same LSN. Without a breaker the
    // receiver spins "connect → same chunk → fail → backoff → reconnect"
    // forever. Count consecutive deterministic failures that make no
    // forward progress; past the threshold, flag a resync and stop so the
    // next boot re-seeds instead of looping. Transient failures
    // (connect/timeout/read) don't count — they reset on any progress.
    let mut retry_delay = backoff;
    let mut poison_failures = 0u32;
    let mut last_applied_seen = initial_progress.durable_applied_lsn;
    const MAX_POISON_FAILURES: u32 = 5;
    loop {
        if shutdown_flag.load(Ordering::Acquire) {
            return;
        }
        // Try to load the latest progress from disk so a restart of
        // the receiver after STOP/START doesn't replay frames that
        // a previous run already persisted as applied.
        if let Ok(Some(loaded)) = crate::repl::replica::progress::load_progress(&data_dir) {
            if loaded.durable_applied_lsn > initial_progress.durable_applied_lsn {
                initial_progress = loaded;
            }
        }
        let receiver = Receiver {
            master_host: host.clone(),
            master_port: port,
            replica_uuid: replica_uuid.clone(),
            config: config.clone(),
            data_dir: data_dir.clone(),
            log: Arc::clone(&log),
            db: Some(Arc::clone(&db)),
            initial_progress: initial_progress.clone(),
            shutdown_flag: Some(Arc::clone(&shutdown_flag)),
        };
        let inner_shutdown = Arc::clone(&shutdown);
        let mut last_event: Option<ReceiverEvent> = None;
        let status_cb = Arc::clone(&status);
        let outcome = receiver
            .run_once(inner_shutdown, |ev| {
                match &ev {
                    ReceiverEvent::Connected {
                        master_uuid,
                        transport,
                        ..
                    } => {
                        *status_cb.lock() =
                            format!("connected to master {master_uuid} via {transport}");
                    }
                    ReceiverEvent::Disconnected { reason } => {
                        *status_cb.lock() = format!("disconnected: {reason}");
                    }
                    ReceiverEvent::ResyncRequired { reason, .. } => {
                        *status_cb.lock() = format!("resync required: {reason}");
                    }
                    ReceiverEvent::Progress(progress) => {
                        *live_progress.lock() = progress.clone();
                        if !progress.timeline_uuid.is_empty() {
                            if let Ok(mut role) = role_state.lock() {
                                if role.timeline_uuid != progress.timeline_uuid
                                    || role.cluster_uuid != progress.cluster_uuid
                                {
                                    let previous = (!role.timeline_uuid.is_empty())
                                        .then(|| role.timeline_uuid.clone());
                                    role.parent_timeline_uuid = previous;
                                    role.timeline_uuid.clone_from(&progress.timeline_uuid);
                                    role.cluster_uuid.clone_from(&progress.cluster_uuid);
                                    role.generation =
                                        role.generation.max(progress.source_generation);
                                    if let Err(err) = crate::repl::role::save(&data_dir, &role) {
                                        *status_cb.lock() = format!(
                                            "error: persist followed timeline metadata: {err}"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                if matches!(ev, ReceiverEvent::ResyncRequired { .. }) {
                    last_event = Some(ev);
                }
            })
            .await;
        if let Some(ReceiverEvent::ResyncRequired { reason, .. }) = last_event {
            log_message(format_args!(
                "mydb-repl receiver: resync required ({reason}); flagged for base-backup re-seed on next restart"
            ));
            // Persist the flag so the next process start re-seeds via
            // `repl::maybe_bootstrap_replica` without operator surgery.
            if let Err(err) =
                crate::repl::replica::progress::write_resync_marker(&data_dir, &reason)
            {
                log_message(format_args!(
                    "mydb-repl receiver: failed to write resync marker: {err}"
                ));
            }
            resync_required = true;
        }
        // Circuit-breaker bookkeeping: did the last run make durable
        // forward progress? If so, any prior poison streak is cleared.
        let current_applied = crate::repl::replica::progress::load_progress(&data_dir)
            .ok()
            .flatten()
            .map(|p| p.durable_applied_lsn)
            .unwrap_or(last_applied_seen);
        if current_applied > last_applied_seen {
            last_applied_seen = current_applied;
            poison_failures = 0;
            retry_delay = backoff;
        }
        if let Err(err) = &outcome {
            let deterministic =
                err.is_internal(crate::error::InternalErrorKind::ReplicaPoisonFrame);
            if deterministic && current_applied <= last_applied_seen {
                poison_failures = poison_failures.saturating_add(1);
                if poison_failures >= MAX_POISON_FAILURES {
                    log_message(format_args!(
                        "mydb-repl receiver: {poison_failures} consecutive deterministic apply \
                         failures at LSN {last_applied_seen} (last: {}); tripping circuit breaker \
                         and flagging resync for re-seed on next restart",
                        err.message()
                    ));
                    let _ = crate::repl::replica::progress::write_resync_marker(
                        &data_dir,
                        &format!("poison frame at LSN {last_applied_seen}: {}", err.message()),
                    );
                    *status.lock() = format!("stopped: poison frame at LSN {last_applied_seen}");
                    return;
                }
            }
        }
        if let Err(err) = outcome {
            *status.lock() = format!("error: {}", err.message());
            log_message(format_args!(
                "mydb-repl receiver: run_once failed: {}",
                err.message()
            ));
        }
        // Quick path: did someone call STOP MYDB REPLICATION?
        // The Notify alone is unreliable across loop iterations
        // (notify_waiters does not buffer), so consult the sticky
        // flag.
        if shutdown_flag.load(Ordering::Acquire) {
            return;
        }
        if resync_required {
            // Without item-3 base-backup support, looping is wasteful
            // — the master will reject every reconnect for the same
            // reason. Exit gracefully; START MYDB REPLICATION restarts
            // us after the operator clears state.
            return;
        }
        // Reconnect backoff. Bail early if shutdown fires while we
        // sleep so STOP MYDB REPLICATION is snappy.
        tokio::select! {
            _ = shutdown.notified() => return,
            _ = tokio::time::sleep(retry_delay) => {}
        }
        retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(60));
        if shutdown_flag.load(Ordering::Acquire) {
            return;
        }
    }
}

// Generate a stable replica UUID. Stored in the role state, but we
// also need a fallback for cases where the role state isn't fully
// initialized (early boot tests, etc.).
