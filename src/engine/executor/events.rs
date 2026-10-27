//! Event scheduler executor.
//!
//! MySQL 8.0 compatible `CREATE EVENT` / `ALTER EVENT` / `DROP EVENT`
//! DDL, plus a background scheduler thread that fires due events.
//!
//! Lifecycle
//! ---------
//! * DDL handlers in this module translate `parser::EventDef` /
//!   `parser::AlterEventStmt` into [`EventSchema`] and persist via
//!   [`Database::create_event`] / [`Database::alter_event`] /
//!   [`Database::drop_event`].
//! * A single [`EventSchedulerHandle`] is bound to each
//!   `StorageQueryEngine` at open time; it spawns a tokio task on a
//!   per-engine current-thread runtime so test code without an ambient
//!   reactor still gets event firing. The task scans the catalog,
//!   picks the next due event, sleeps until then, and re-executes the
//!   body through the regular query path.
//!
//! Concurrency
//! -----------
//! The scheduler does not hold any locks while sleeping; on every wake it
//! re-reads the catalog. DDL on events bumps the scheduler's `Notify` so an
//! `ALTER EVENT … DISABLE` issued mid-sleep wakes the worker. Due bodies are
//! claimed by `(database, name, schema_version, due_time)` and handed to a
//! small bounded worker pool. Queue jobs carry a unified-governor reservation,
//! so a burst of events cannot grow resident memory without bound.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{NaiveDateTime, TimeZone, Utc};
use tokio::sync::Notify;

use crate::engine::api::{QueryResponse, SessionContext};
use crate::engine::auth::AccountKey;
use crate::parser::{
    parse_expr_fragment, parse_statements, AlterEventStmt, EventDef, EventOnCompletion,
    EventSchedule, EventStatus,
};
use crate::storage::{
    error, parse_datetime_to_micros, split_table_name, value_to_string, Database,
    EventIntervalSchema, EventOnCompletionSchema, EventScheduleSchema, EventSchema,
    EventStatusSchema,
};
use crate::types::{QueryError, Value};

/// Global value of `@@global.event_scheduler`.  Mirrors MySQL: ON
/// (default) runs scheduled events, OFF parks the worker, DISABLED
/// permanently disables it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSchedulerState {
    On = 1,
    Off = 0,
    Disabled = 2,
}

impl EventSchedulerState {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventSchedulerState::On => "ON",
            EventSchedulerState::Off => "OFF",
            EventSchedulerState::Disabled => "DISABLED",
        }
    }

    pub fn from_str_lenient(raw: &str) -> Option<EventSchedulerState> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "ON" | "1" | "TRUE" | "YES" => Some(EventSchedulerState::On),
            "OFF" | "0" | "FALSE" | "NO" => Some(EventSchedulerState::Off),
            "DISABLED" => Some(EventSchedulerState::Disabled),
            _ => None,
        }
    }
}

/// Process-global event scheduler switch.  Sized as `AtomicU8` so the
/// hot path (`maybe_fire`) does not touch a Mutex.
static SCHEDULER_STATE: AtomicU8 = AtomicU8::new(EventSchedulerState::On as u8);

pub fn scheduler_state() -> EventSchedulerState {
    match SCHEDULER_STATE.load(Ordering::Relaxed) {
        0 => EventSchedulerState::Off,
        2 => EventSchedulerState::Disabled,
        _ => EventSchedulerState::On,
    }
}

pub fn set_scheduler_state(state: EventSchedulerState) {
    SCHEDULER_STATE.store(state as u8, Ordering::Relaxed);
    notify_all_schedulers();
}

/// G10: apply the start-time `MYDB_EVENT_SCHEDULER` setting
/// (`on`/`off`/`disabled`). Unlike the runtime `SET GLOBAL
/// event_scheduler` path, `DISABLED` is accepted here because it is a
/// server-startup-only knob (matching MySQL's `--event-scheduler`).
/// Called once before any scheduler worker is spawned, so it stores the
/// state directly without waking non-existent workers.
pub fn apply_startup_state_from_env() {
    if let Ok(raw) = std::env::var("MYDB_EVENT_SCHEDULER") {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return;
        }
        match EventSchedulerState::from_str_lenient(trimmed) {
            Some(state) => SCHEDULER_STATE.store(state as u8, Ordering::Relaxed),
            None => crate::util::logging::log_message(format!(
                "event scheduler: ignoring invalid MYDB_EVENT_SCHEDULER='{raw}' (expected on/off/disabled)"
            )),
        }
    }
}

/// Registry of live event-scheduler handles so global state changes
/// (e.g. `SET GLOBAL event_scheduler = OFF`) wake every active worker.
static REGISTRY: OnceLock<Mutex<Vec<Weak<EventSchedulerInner>>>> = OnceLock::new();

fn registry() -> &'static Mutex<Vec<Weak<EventSchedulerInner>>> {
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

fn register_scheduler(handle: &Arc<EventSchedulerInner>) {
    let mut lock = registry().lock().unwrap_or_else(|e| e.into_inner());
    lock.retain(|weak| weak.strong_count() > 0);
    lock.push(Arc::downgrade(handle));
}

/// Wake the sole worker attached to one scheduler.
///
/// `notify_one` intentionally retains a permit when the worker has not
/// registered its wait yet.  Shutdown, DDL, and global-state changes can all
/// race with the worker between its state check and `notified().await`.
fn wake_scheduler(notify: &Notify) {
    notify.notify_one();
}

fn notify_all_schedulers() {
    let lock = registry().lock().unwrap_or_else(|e| e.into_inner());
    for weak in lock.iter() {
        if let Some(handle) = weak.upgrade() {
            wake_scheduler(&handle.notify);
        }
    }
}

/// Per-engine scheduler.  Held inside `StorageQueryEngine` so dropping
/// the engine stops the worker.
pub struct EventSchedulerHandle {
    inner: Arc<EventSchedulerInner>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

struct EventSchedulerInner {
    db: Arc<Database>,
    registry: Arc<crate::engine::SessionRegistry>,
    notify: Notify,
    shutdown: AtomicBool,
    /// Most recent fire time per `(database, name, schema_version)`,
    /// guarding against re-running an event that has just executed
    /// when the worker is woken mid-cycle.
    fired: Mutex<HashMap<(String, String, u64), u64>>,
    claims: Mutex<HashSet<EventClaim>>,
    active_processes: Mutex<HashSet<u32>>,
}

type EventClaim = (String, String, u64, u64);

struct EventJob {
    event: EventSchema,
    fire_ms: u64,
    claim: EventClaim,
    _memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

struct EventWorkerPool {
    sender: Option<SyncSender<EventJob>>,
    workers: Vec<std::thread::JoinHandle<()>>,
    inner: Arc<EventSchedulerInner>,
}

impl EventWorkerPool {
    fn spawn(inner: Arc<EventSchedulerInner>) -> Self {
        let worker_count = event_worker_count();
        let (sender, receiver) = sync_channel(worker_count.saturating_mul(2).max(1));
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let receiver = Arc::clone(&receiver);
            let worker_inner = Arc::clone(&inner);
            workers.push(
                std::thread::Builder::new()
                    .name(format!("mydb-event-worker-{index}"))
                    .spawn(move || event_worker_loop(worker_inner, receiver))
                    .expect("failed to spawn event scheduler worker"),
            );
        }
        Self {
            sender: Some(sender),
            workers,
            inner,
        }
    }

    fn try_enqueue(&self, event: &EventSchema, fire_ms: u64) -> bool {
        let claim = (
            event.database.clone(),
            event.name.clone(),
            event.schema_version,
            fire_ms,
        );
        {
            let mut claims = self
                .inner
                .claims
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if !claims.insert(claim.clone()) {
                return true;
            }
        }
        let Some(memory_charge) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                event_job_capacity_bytes(event),
            )
        else {
            self.release_claim(&claim);
            return false;
        };
        let job = EventJob {
            event: event.clone(),
            fire_ms,
            claim: claim.clone(),
            _memory_charge: memory_charge,
        };
        let Some(sender) = &self.sender else {
            self.release_claim(&claim);
            return false;
        };
        match sender.try_send(job) {
            Ok(()) => true,
            Err(TrySendError::Full(job)) | Err(TrySendError::Disconnected(job)) => {
                self.release_claim(&job.claim);
                false
            }
        }
    }

    fn release_claim(&self, claim: &EventClaim) {
        self.inner
            .claims
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(claim);
    }

    fn shutdown(mut self) {
        let active = self
            .inner
            .active_processes
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .copied()
            .collect::<Vec<_>>();
        for process_id in active {
            self.inner.registry.cancel_background_query(process_id);
        }
        self.sender.take();
        for worker in self.workers.drain(..) {
            // Never detach a writer before the final checkpoint. Repeat
            // cancellation to cover workers registering after the first scan.
            while !worker.is_finished() {
                for process_id in self
                    .inner
                    .active_processes
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .iter()
                {
                    self.inner.registry.cancel_background_query(*process_id);
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            if worker.join().is_err() {
                crate::util::logging::log_message(
                    "event scheduler: worker panicked during shutdown",
                );
            }
        }
    }
}

fn event_worker_count() -> usize {
    crate::util::env::var("MYDB_EVENT_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|count| *count > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|count| count.get().min(4))
                .unwrap_or(2)
        })
        .clamp(1, 32)
}

fn event_job_capacity_bytes(event: &EventSchema) -> usize {
    let schedule_bytes = match &event.schedule {
        EventScheduleSchema::OneShot { .. } => 0,
        EventScheduleSchema::Recurring { interval, .. } => interval
            .value
            .capacity()
            .saturating_add(interval.unit.capacity()),
    };
    std::mem::size_of::<EventJob>()
        .saturating_add(schedule_bytes)
        // `EventJob` owns both the cloned EventSchema and the claim key.
        .saturating_add(event.database.capacity().saturating_mul(2))
        .saturating_add(event.name.capacity().saturating_mul(2))
        .saturating_add(event.definer_user.capacity())
        .saturating_add(event.definer_host.capacity())
        .saturating_add(event.comment.capacity())
        .saturating_add(event.body_sql.capacity())
        .saturating_add(event.create_sql.capacity())
        .saturating_add(event.sql_mode.capacity())
        .saturating_add(event.time_zone.capacity())
        .saturating_add(event.character_set_client.capacity())
        .saturating_add(event.collation_connection.capacity())
        .saturating_add(event.database_collation.capacity())
}

fn event_worker_loop(inner: Arc<EventSchedulerInner>, receiver: Arc<Mutex<Receiver<EventJob>>>) {
    loop {
        let job = receiver
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .recv();
        let Ok(job) = job else {
            return;
        };
        let panicked = run_event_claim(&inner.claims, &inner.notify, &job.claim, || {
            if !inner.shutdown.load(Ordering::Acquire) {
                fire_event(&inner, &job.event, job.fire_ms);
            }
        });
        if panicked {
            crate::util::logging::log_message(format_args!(
                "event scheduler: worker recovered from panic while firing {}.{}",
                job.event.database, job.event.name
            ));
        }
    }
}

fn run_event_claim(
    claims: &Mutex<HashSet<EventClaim>>,
    notify: &Notify,
    claim: &EventClaim,
    run: impl FnOnce(),
) -> bool {
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run)).is_err();
    claims
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .remove(claim);
    wake_scheduler(notify);
    panicked
}

#[cfg(test)]
#[test]
fn event_claim_is_released_after_worker_panic() {
    let claims = Mutex::new(HashSet::new());
    let notify = Notify::new();
    let claim: EventClaim = ("test".into(), "event".into(), 1, 123);
    claims.lock().unwrap().insert(claim.clone());
    assert!(run_event_claim(&claims, &notify, &claim, || panic!(
        "injected event panic"
    )));
    assert!(!claims.lock().unwrap().contains(&claim));
    claims.lock().unwrap().insert(claim.clone());
    assert!(!run_event_claim(&claims, &notify, &claim, || {}));
    assert!(!claims.lock().unwrap().contains(&claim));
}

impl EventSchedulerHandle {
    pub fn spawn(db: Arc<Database>, registry: Arc<crate::engine::SessionRegistry>) -> Self {
        let inner = Arc::new(EventSchedulerInner {
            db,
            registry,
            notify: Notify::new(),
            shutdown: AtomicBool::new(false),
            fired: Mutex::new(HashMap::new()),
            claims: Mutex::new(HashSet::new()),
            active_processes: Mutex::new(HashSet::new()),
        });
        register_scheduler(&inner);

        let worker = Arc::clone(&inner);
        let worker = std::thread::Builder::new()
            .name("mydb-event-scheduler".to_string())
            .spawn(move || run_scheduler(worker))
            .expect("failed to spawn event scheduler thread");
        EventSchedulerHandle {
            inner,
            worker: Mutex::new(Some(worker)),
        }
    }

    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
        wake_scheduler(&self.inner.notify);
        if let Some(worker) = self.worker.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = worker.join();
        }
        registry()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .retain(|weak| !std::ptr::eq(weak.as_ptr(), Arc::as_ptr(&self.inner)));
    }
}

impl Drop for EventSchedulerHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run_scheduler(inner: Arc<EventSchedulerInner>) {
    let _process = inner.registry.register_background(
        "event_scheduler".to_string(),
        "localhost".to_string(),
        None,
        "Daemon",
        crate::engine::session_registry::ProcessState::WaitingForNextActivation,
        None,
    );
    // Each scheduler owns a single-threaded tokio runtime so we get
    // `Notify` + `sleep_until` without forcing the broader server onto
    // a multi-thread reactor.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build event scheduler runtime");
    let workers = EventWorkerPool::spawn(Arc::clone(&inner));
    runtime.block_on(scheduler_loop(inner, &workers));
    workers.shutdown();
}

async fn scheduler_loop(inner: Arc<EventSchedulerInner>, workers: &EventWorkerPool) {
    let mut last_fired_prune_ms = 0_u64;
    loop {
        if inner.shutdown.load(Ordering::Acquire) {
            return;
        }
        if scheduler_state() != EventSchedulerState::On {
            // Sleep until DDL or state change wakes us.
            inner.notify.notified().await;
            continue;
        }
        // G9: on a physical replica the master's event bodies already
        // arrive through replication, so re-firing them here would
        // double-apply the writes. Park (polling for a PROMOTE that flips
        // the role) instead of firing. `DISABLE ON SLAVE` events are
        // handled independently via their non-Enabled status below.
        if inner.db.physical_replica_mode() {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                _ = inner.notify.notified() => {}
            }
            continue;
        }
        let now = current_unix_ms();
        if now.saturating_sub(last_fired_prune_ms) >= 60_000 {
            let cutoff = now.saturating_sub(10 * 60 * 1000);
            inner
                .fired
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .retain(|_, fire_ms| *fire_ms >= cutoff);
            last_fired_prune_ms = now;
        }
        let events = inner.db.all_events();
        let mut next_fire_ms: Option<u64> = None;
        let mut sleep_for_no_events = true;
        for event in events {
            if event.status != EventStatusSchema::Enabled {
                continue;
            }
            sleep_for_no_events = false;
            let next_due = match next_due_ms(&event, now) {
                Some(next) => next,
                None => {
                    // Event has expired — drop or DISABLE per
                    // ON COMPLETION clause.
                    finalize_completed_event(&inner, &event);
                    continue;
                }
            };
            if next_due <= now {
                // Don't re-fire the same `(name, version, due-time)` in
                // a tight loop if the catalog write hasn't settled.
                let key = (
                    event.database.clone(),
                    event.name.clone(),
                    event.schema_version,
                );
                {
                    let fired = inner.fired.lock().unwrap_or_else(|e| e.into_inner());
                    if fired.get(&key).copied() == Some(next_due) {
                        // Already serviced this tick; sleep to the
                        // following occurrence rather than re-firing.
                        if let Some(future) = following_due_ms(&event, next_due) {
                            next_fire_ms = Some(next_fire_ms.map_or(future, |m| m.min(future)));
                        }
                        continue;
                    }
                }
                if workers.try_enqueue(&event, next_due) {
                    let mut fired = inner.fired.lock().unwrap_or_else(|e| e.into_inner());
                    fired.insert(key, next_due);
                    // Trim stale fired entries to avoid unbounded
                    // growth.  Keys >10 minutes old can never affect
                    // dedup against a fresh schedule.
                    let cutoff = now.saturating_sub(10 * 60 * 1000);
                    fired.retain(|_, ts| *ts >= cutoff);
                } else {
                    // Bounded queue or memory pressure: retry this due tick
                    // shortly instead of skipping it and jumping to the next
                    // recurrence.
                    let retry = now.saturating_add(10);
                    next_fire_ms = Some(next_fire_ms.map_or(retry, |m| m.min(retry)));
                    continue;
                }
                // Fold the *next* occurrence into the sleep target. Without
                // this a lone recurring event leaves `next_fire_ms` unset
                // and drops into the 5s idle fallback below, so it would
                // fire on a 5s floor regardless of its interval (an
                // `EVERY 1 SECOND` event firing only every 5s).
                if let Some(future) = following_due_ms(&event, next_due) {
                    next_fire_ms = Some(next_fire_ms.map_or(future, |m| m.min(future)));
                }
                continue;
            }
            next_fire_ms = Some(next_fire_ms.map_or(next_due, |m| m.min(next_due)));
        }

        if let Some(next) = next_fire_ms {
            let wait_ms = next.saturating_sub(now).min(60_000);
            let sleep = tokio::time::sleep(Duration::from_millis(wait_ms.max(1)));
            tokio::select! {
                _ = sleep => {}
                _ = inner.notify.notified() => {}
            }
        } else if sleep_for_no_events {
            // Without another enqueue there is no producer to prune the last
            // fired keys. Wake at most once per minute until they expire.
            if inner
                .fired
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .is_empty()
            {
                inner.notify.notified().await;
            } else {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                    _ = inner.notify.notified() => {}
                }
            }
        } else {
            // Events exist but none have a future due time
            // (e.g. expired with PRESERVE).  Sleep modestly so we
            // pick up status flips without busy-spinning.
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                _ = inner.notify.notified() => {}
            }
        }
    }
}

fn fire_event(inner: &Arc<EventSchedulerInner>, event: &EventSchema, fire_ms: u64) {
    if let Err(err) = run_event_body_with_tracking(
        &inner.db,
        &inner.registry,
        event,
        Some(&inner.active_processes),
    ) {
        crate::util::logging::log_message(format!(
            "event scheduler: event '{}.{}' failed: {}",
            event.database,
            event.name,
            if crate::engine::http::may_call(&inner.db, &event.body_sql) {
                "[redacted HTTP event error]"
            } else {
                err.message()
            }
        ));
    }
    // A worker may finish after ALTER. Only its claimed definition may
    // receive the watermark or completion action.
    if let Ok(Some(recorded)) =
        inner
            .db
            .record_event_run(&event.database, &event.name, event.schema_version, fire_ms)
    {
        if next_due_ms(&recorded, fire_ms.saturating_add(1)).is_none() {
            finalize_completed_event(inner, &recorded);
        }
    }
}

struct ActiveEventProcessGuard<'a> {
    active: &'a Mutex<HashSet<u32>>,
    process_id: u32,
}

impl Drop for ActiveEventProcessGuard<'_> {
    fn drop(&mut self) {
        self.active
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(&self.process_id);
    }
}

fn run_event_body_with_tracking(
    db: &Arc<Database>,
    registry: &crate::engine::SessionRegistry,
    event: &EventSchema,
    active_processes: Option<&Mutex<HashSet<u32>>>,
) -> Result<(), QueryError> {
    // G7: the body runs under the definer's account, and that account
    // must exist (MySQL skips the firing and logs an error when the
    // definer was dropped). Verifying up front mirrors MySQL's
    // "definer does not exist" behaviour instead of silently running
    // body statements with no grants.
    let definer_exists = db
        .with_auth_catalog(|auth| {
            auth.accounts
                .contains_key(&crate::engine::auth::account_id(&AccountKey::new(
                    &event.definer_user,
                    &event.definer_host,
                )))
        })
        .unwrap_or(false);
    if !definer_exists {
        return Err(error(
            crate::error::catalog::ER_NO_SUCH_USER,
            format!(
                "The user specified as a definer ('{}'@'{}') does not exist",
                event.definer_user, event.definer_host
            ),
        ));
    }

    let http_dependent = crate::engine::http::may_call(db, &event.body_sql);
    let _http_scope = crate::engine::http::enter(db);
    if http_dependent {
        crate::engine::http::activate();
    }
    let _http_serial = http_dependent.then(crate::planner::force_serial_scope);
    let mut session = SessionContext::default();
    let process = registry.register_background(
        "event_scheduler".to_string(),
        "localhost".to_string(),
        Some(event.database.clone()),
        "Query",
        crate::engine::session_registry::ProcessState::Executing,
        Some(crate::engine::logging::redact_sql_for_log(&event.body_sql)),
    );
    let _active_process = process.as_ref().and_then(|process| {
        let active = active_processes?;
        active
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(process.id());
        Some(ActiveEventProcessGuard {
            active,
            process_id: process.id(),
        })
    });
    if let Some(process) = &process {
        session.connection_id = process.id();
        session.set_cancel_handle(Some(process.handle().cancel_handle()));
    }
    session.current_database = event.database.clone();
    session.user = event.definer_user.clone();
    session.host = event.definer_host.clone();
    session.authenticated_as = AccountKey::new(&event.definer_user, &event.definer_host);
    session.login_user = event.definer_user.clone();
    // G7: load the definer's default roles so privilege checks on the
    // body match what the definer would see at login.
    session.active_roles = db
        .with_auth_catalog(|auth| auth.login_active_roles(&session.authenticated_as))
        .unwrap_or_default()
        .into();
    session.auth_epoch = db.auth_epoch().unwrap_or(0);
    // G1: the body executes under the sql_mode captured at CREATE time,
    // matching stored-routine semantics. Refresh the cached temporal /
    // numeric mode bits so strict-mode behaviour follows the snapshot.
    session.sql_mode = event.sql_mode.clone();
    session.temporal_sql_mode_bits =
        crate::engine::api::parse_temporal_sql_mode_bits(&event.sql_mode);
    session.character_set_client = event.character_set_client.clone();
    session.collation_connection = event.collation_connection.clone();
    // Run the body under the event's recorded time zone so NOW() / date
    // functions inside it observe the same zone the schedule was written
    // against.
    if !event.time_zone.is_empty() {
        session.sysvars.time_zone = event.time_zone.clone();
    }

    // Keep the ordinary SQL path lightweight. Stored-program statements
    // (BEGIN blocks, IF, WHILE, ...) use the compiled routine executor.
    let _cancel_scope = crate::engine::cancel::install_session_cancel_context(&session);
    let session_ptr = std::ptr::NonNull::from(&session);
    let execution = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: the event owns this pinned session on one worker. Every
        // result is drained before the scope ends; nested security/session
        // changes must be visible to expression evaluation on that worker.
        unsafe {
            crate::engine::expr::with_session_expr_info_raw(session_ptr, || {
                if let Ok(statements) = parse_statements(&event.body_sql) {
                    for statement in statements {
                        crate::mysql::connection_manager::record_internal_query();
                        super::authorize_statement(db, &session, &statement)?;
                        super::dispatch_execute_statement(db, &mut session, statement, registry)?
                            .materialize()?;
                    }
                    Ok(())
                } else {
                    super::routines::execute_event_body(db, &mut session, event, registry)
                }
            })
        }
    }));
    // The background session is not a protocol Session and therefore has no
    // connection Drop guard. Release its transaction and locks on success,
    // error, and panic before the worker processes another event.
    let txn_cleanup = super::tx::rollback_session_txn(db, &mut session);
    db.release_mdl_owner(session.connection_id as u64);
    crate::engine::expr::release_all_user_locks(session.connection_id);
    let temp_cleanup = super::tx::close_temp_tables(db, &mut session);
    match execution {
        Ok(result) => {
            txn_cleanup?;
            temp_cleanup?;
            result
        }
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

/// Parse and validate stored-program structure without resolving body tables.
fn validate_event_body(body_sql: &str) -> Result<(), QueryError> {
    super::routines::validate_event_body(body_sql)
}

fn parse_err(err: crate::parser::ParseError) -> QueryError {
    err.spec.error(format!("{err}"))
}

fn finalize_completed_event(inner: &Arc<EventSchedulerInner>, event: &EventSchema) {
    let _ = inner.db.complete_event_if_version(event);
}

/// DDL handler for `CREATE EVENT`.
pub(super) fn execute_create_event(
    db: &Arc<Database>,
    session: &mut SessionContext,
    def: EventDef,
    _registry: &crate::engine::SessionRegistry,
) -> Result<QueryResponse, QueryError> {
    super::routines::validate_definer_privilege(db, session, def.definer.as_ref())?;
    super::implicit_commit_for_ddl(db, session)?;
    let statement_second = current_unix_ms() / 1000;
    let staged = event_schema_from_def(session, def)?;
    let StagedEvent {
        event,
        or_replace,
        if_not_exists,
    } = staged;
    super::routines::warn_missing_definer(
        db,
        &AccountKey::new(&event.definer_user, &event.definer_host),
    )?;
    if let Some(existing) = db.event_definition(&event.database, &event.name) {
        if !or_replace && !if_not_exists {
            return Err(error(
                crate::error::catalog::ER_EVENT_ALREADY_EXISTS,
                format!("Event '{}' already exists", event.name),
            ));
        }
        if if_not_exists && !or_replace {
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Note,
                crate::error::catalog::ER_EVENT_ALREADY_EXISTS,
                format!("Event '{}' already exists", existing.name),
            );
            return Ok(QueryResponse::Ok);
        }
    }

    // Expired one-shot definitions never execute user DML during CREATE.
    // Compare at schedule precision so CURRENT_TIMESTAMP remains schedulable.
    if let EventScheduleSchema::OneShot { execute_at_ms } = &event.schedule {
        if *execute_at_ms / 1000 < statement_second {
            crate::engine::api::push_statement_warning(
                crate::error::ConditionLevel::Note,
                crate::error::catalog::ER_EVENT_CANNOT_CREATE_IN_THE_PAST,
                match event.on_completion {
                    EventOnCompletionSchema::NotPreserve => "Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. The event was dropped immediately after creation",
                    EventOnCompletionSchema::Preserve => "Event execution time is in the past. Event has been disabled",
                }.to_string(),
            );
            match event.on_completion {
                EventOnCompletionSchema::NotPreserve => {
                    // Validate the target even when no object will be retained.
                    if !db.database_exists(&event.database) {
                        return Err(error(
                            crate::error::catalog::ER_BAD_DB_ERROR,
                            format!("unknown database '{}'", event.database),
                        ));
                    }
                    if or_replace {
                        db.drop_event(&event.database, &event.name, true)?;
                    }
                    return Ok(QueryResponse::Ok);
                }
                EventOnCompletionSchema::Preserve => {
                    // Persist as DISABLED, never executed.
                    let mut disabled = event;
                    disabled.status = EventStatusSchema::Disabled;
                    db.create_event(
                        &session.current_database,
                        disabled,
                        or_replace,
                        if_not_exists,
                    )?;
                    notify_all_schedulers();
                    return Ok(QueryResponse::Ok);
                }
            }
        }
    }

    db.create_event(&session.current_database, event, or_replace, if_not_exists)?;
    // Events are invisible to the query planner — no SQL-cache flush needed.
    notify_all_schedulers();
    Ok(QueryResponse::Ok)
}

struct StagedEvent {
    event: EventSchema,
    or_replace: bool,
    if_not_exists: bool,
}

fn event_schema_from_def(
    session: &SessionContext,
    def: EventDef,
) -> Result<StagedEvent, QueryError> {
    let (database, name) = split_table_name(&session.current_database, &def.name);
    // G8: validate the body syntactically at CREATE time. MySQL only
    // performs a parse-level check (not semantic), raising 1064 on a
    // malformed body before the event is stored.
    validate_event_body(&def.body_sql)?;
    let definer = def.definer.unwrap_or_else(|| crate::parser::AccountIdent {
        user: session.authenticated_as.user.clone(),
        host: session.authenticated_as.host.clone(),
    });
    // G2: capture the creating session's time zone. Schedule literals are
    // interpreted in this zone and stored as UTC ms.
    let time_zone = session.sysvars.time_zone.clone();
    let schedule = resolve_schedule(&def.schedule, current_unix_ms(), &time_zone)?;
    let event = EventSchema {
        database,
        name,
        definer_user: definer.user,
        definer_host: definer.host,
        schedule,
        on_completion: def
            .on_completion
            .map(on_completion_schema)
            .unwrap_or(EventOnCompletionSchema::NotPreserve),
        status: def
            .status
            .map(status_schema)
            .unwrap_or(EventStatusSchema::Enabled),
        comment: def.comment.unwrap_or_default(),
        body_sql: def.body_sql,
        create_sql: def.create_sql,
        created_ms: 0,
        last_altered_ms: 0,
        last_executed_ms: 0,
        schema_version: 0,
        sql_mode: session.sql_mode.clone(),
        time_zone,
        character_set_client: session.character_set_client.clone(),
        collation_connection: session.collation_connection.clone(),
        // Filled from the target database under the CREATE catalog lock.
        database_collation: String::new(),
    };
    Ok(StagedEvent {
        event,
        or_replace: def.or_replace,
        if_not_exists: def.if_not_exists,
    })
}

pub(super) fn execute_alter_event(
    db: &Arc<Database>,
    session: &mut SessionContext,
    stmt: AlterEventStmt,
) -> Result<QueryResponse, QueryError> {
    super::routines::validate_definer_privilege(db, session, stmt.definer.as_ref())?;
    let definer = stmt
        .definer
        .as_ref()
        .map(|d| AccountKey::new(&d.user, &d.host))
        .unwrap_or_else(|| session.authenticated_as.clone());
    super::routines::warn_missing_definer(db, &definer)?;
    super::implicit_commit_for_ddl(db, session)?;
    // G8: validate a replaced body syntactically before persisting.
    if let Some(body_sql) = stmt.body_sql.as_ref() {
        validate_event_body(body_sql)?;
    }
    let rename_to = stmt.rename_to.clone();
    let now = current_unix_ms();
    let session_tz = session.sysvars.time_zone.clone();
    let new_schedule = stmt
        .schedule
        .as_ref()
        .map(|schedule| resolve_schedule(schedule, now, &session_tz))
        .transpose()?;
    db.alter_event(&session.current_database, &stmt.name, rename_to, |event| {
        if let Some(schedule) = new_schedule {
            event.schedule = schedule;
            // G2: a re-specified schedule re-anchors the event's time
            // zone to the current session (its literals were just
            // interpreted in that zone).
            event.time_zone = session_tz.clone();
            // G12: changing ON SCHEDULE resets the execution baseline so
            // next_due is computed cleanly from the new schedule rather
            // than back-filling against the old last_executed watermark.
            event.last_executed_ms = 0;
        }
        if let Some(oc) = stmt.on_completion {
            event.on_completion = on_completion_schema(oc);
        }
        if let Some(status) = stmt.status {
            event.status = status_schema(status);
        }
        if let Some(comment) = stmt.comment {
            event.comment = comment;
        }
        if let Some(body_sql) = stmt.body_sql {
            event.body_sql = body_sql;
        }
        // ALTER EVENT without DEFINER adopts the executing account. Keeping
        // the previous owner would let EVENT privilege rewrite a root-owned
        // event and run its replacement body with root's authority.
        let definer = stmt.definer.unwrap_or_else(|| crate::parser::AccountIdent {
            user: session.authenticated_as.user.clone(),
            host: session.authenticated_as.host.clone(),
        });
        event.definer_user = definer.user;
        event.definer_host = definer.host;
        if !stmt.create_sql.is_empty() {
            event.create_sql = stmt.create_sql.clone();
        }
    })?;
    // Events are invisible to the query planner — no SQL-cache flush needed.
    notify_all_schedulers();
    Ok(QueryResponse::Ok)
}

pub(super) fn execute_drop_event(
    db: &Arc<Database>,
    session: &mut SessionContext,
    name: &str,
    if_exists: bool,
) -> Result<QueryResponse, QueryError> {
    super::implicit_commit_for_ddl(db, session)?;
    db.drop_event(&session.current_database, name, if_exists)?;
    // Events are invisible to the query planner — no SQL-cache flush needed.
    notify_all_schedulers();
    Ok(QueryResponse::Ok)
}

pub fn on_completion_schema(oc: EventOnCompletion) -> EventOnCompletionSchema {
    match oc {
        EventOnCompletion::NotPreserve => EventOnCompletionSchema::NotPreserve,
        EventOnCompletion::Preserve => EventOnCompletionSchema::Preserve,
    }
}

pub fn status_schema(status: EventStatus) -> EventStatusSchema {
    match status {
        EventStatus::Enabled => EventStatusSchema::Enabled,
        EventStatus::Disabled => EventStatusSchema::Disabled,
        EventStatus::DisabledOnSlave => EventStatusSchema::DisabledOnSlave,
    }
}

/// G9: the running server's id, surfaced as `ORIGINATOR` on event
/// metadata. On a single server this matches `@@server_id` (MySQL fills
/// ORIGINATOR with the creating server's id).
pub fn current_server_id() -> u32 {
    crate::binlog::server_id()
}

pub fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn resolve_schedule(
    schedule: &EventSchedule,
    now_ms: u64,
    time_zone: &str,
) -> Result<EventScheduleSchema, QueryError> {
    match schedule {
        EventSchedule::At { at_expr } => {
            // G3: an AT timestamp in the past is NOT an error in MySQL.
            // The decision (run once / disable + note 1588) is made by
            // the CREATE EVENT handler once it knows the ON COMPLETION
            // clause, so we just materialise the requested instant here.
            let execute_at = eval_timestamp_expr(at_expr, time_zone)?;
            let _ = now_ms;
            Ok(EventScheduleSchema::OneShot {
                execute_at_ms: execute_at,
            })
        }
        EventSchedule::Every {
            interval_value,
            interval_unit,
            starts_expr,
            ends_expr,
        } => {
            // Validate interval (numeric value(s), recognized unit).
            let _ = parse_interval_parts(interval_value, interval_unit)?;
            let starts_ms = match starts_expr {
                Some(expr) => eval_timestamp_expr(expr, time_zone)?,
                None => now_ms,
            };
            let ends_ms = match ends_expr {
                Some(expr) => Some(eval_timestamp_expr(expr, time_zone)?),
                None => None,
            };
            if let Some(ends) = ends_ms {
                if ends <= starts_ms {
                    return Err(error(
                        crate::error::catalog::ER_EVENT_INTERVAL_NOT_POSITIVE_OR_TOO_BIG,
                        "ENDS must be strictly after STARTS",
                    ));
                }
            }
            Ok(EventScheduleSchema::Recurring {
                interval: EventIntervalSchema {
                    value: interval_value.clone(),
                    unit: interval_unit.clone(),
                },
                starts_ms,
                ends_ms,
            })
        }
    }
}

fn eval_timestamp_expr(expr_sql: &str, time_zone: &str) -> Result<u64, QueryError> {
    let expr = parse_expr_fragment(expr_sql).map_err(parse_err)?;
    let value = crate::engine::expr::eval_const_runtime(&expr)?;
    // Schedule expressions must be dates, not ambiguous numeric epochs.
    let local_ms = value_to_unix_ms(value)?;
    Ok(local_ms_to_utc_ms(local_ms, time_zone))
}

/// Convert a wall-clock instant (ms since epoch *as if the local time
/// were UTC*) in `time_zone` into the true UTC instant. `SYSTEM` /
/// `LOCAL` / empty follow the host offset (matching `@@time_zone =
/// 'SYSTEM'`); a named IANA zone uses its full DST history. Unknown
/// zones fall back to treating the input as already-UTC.
fn local_ms_to_utc_ms(local_ms: u64, time_zone: &str) -> u64 {
    let local_secs = (local_ms / 1000) as i64;
    let sub_ms = (local_ms % 1000) as i64;
    let tz = time_zone.trim();
    let utc_secs =
        if tz.is_empty() || tz.eq_ignore_ascii_case("SYSTEM") || tz.eq_ignore_ascii_case("LOCAL") {
            use chrono::{Local, Offset};
            let off = Local::now().offset().fix().local_minus_utc() as i64;
            local_secs - off
        } else if tz.eq_ignore_ascii_case("UTC") || tz.eq_ignore_ascii_case("GMT") {
            local_secs
        } else if let Some(off) = parse_fixed_offset_secs(tz) {
            local_secs - off
        } else {
            match crate::engine::tz::find_named_zone(tz) {
                Some(zone) => zone.utc_from_local(local_secs),
                None => local_secs,
            }
        };
    let total = utc_secs * 1000 + sub_ms;
    if total < 0 {
        0
    } else {
        total as u64
    }
}

/// Parse a `±HH:MM` fixed-offset time-zone literal into seconds east of
/// UTC, or `None` if it is not in that form.
fn parse_fixed_offset_secs(tz: &str) -> Option<i64> {
    let bytes = tz.as_bytes();
    let sign = match bytes.first() {
        Some(b'+') => 1i64,
        Some(b'-') => -1i64,
        _ => return None,
    };
    let body = &tz[1..];
    let (hh, mm) = body.split_once(':')?;
    let h: i64 = hh.parse().ok()?;
    let m: i64 = mm.parse().ok()?;
    if h <= 14 && m <= 59 {
        Some(sign * (h * 3600 + m * 60))
    } else {
        None
    }
}

fn value_to_unix_ms(value: Value) -> Result<u64, QueryError> {
    match value {
        Value::Int(_) | Value::UInt(_) | Value::Float(_) | Value::Double(_)
        | Value::Decimal(_) | Value::DecimalCompact(..) | Value::Decimal128(..)
        | Value::DecimalWide(_) => Err(error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
            "numeric event timestamps are not supported; use FROM_UNIXTIME() or a datetime expression",
        )),
        Value::Null => Err(error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
            "event timestamp evaluated to NULL",
        )),
        Value::DateTime(micros) | Value::Timestamp(micros) => {
            if micros < 0 {
                Err(error(
                    crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
                    "event timestamp must be non-negative",
                ))
            } else {
                Ok((micros / 1000) as u64)
            }
        }
        Value::Date(days) => {
            let secs = days as i64 * 86_400;
            if secs < 0 {
                Err(error(
                    crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
                    "event timestamp must be non-negative",
                ))
            } else {
                Ok((secs * 1000) as u64)
            }
        }
        other => {
            let text = value_to_string(&other);
            let micros = parse_datetime_to_micros(text.trim())?;
            if micros < 0 {
                Err(error(
                    crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
                    "event timestamp must be non-negative",
                ))
            } else {
                Ok((micros / 1000) as u64)
            }
        }
    }
}

/// A normalised interval, split into the calendar-bearing component
/// (`months`) and the fixed-length component (`days` + `ms`). MySQL's
/// 11 compound units and every simple unit map onto this shape:
/// MONTH/QUARTER/YEAR/YEAR_MONTH carry months; DAY and the DAY_* /
/// HOUR_* / MINUTE_* / SECOND_* compounds carry days + sub-day ms;
/// MICROSECOND rounds up to the 1 ms scheduler floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IntervalParts {
    months: i64,
    days: i64,
    ms: i64,
}

impl IntervalParts {
    fn is_zero(&self) -> bool {
        self.months == 0 && self.days == 0 && self.ms == 0
    }
}

/// Parse `value` (raw expr text from `EVERY <value> <unit>`) under the
/// given `unit`, yielding normalised [`IntervalParts`]. Simple units take
/// a single integer; compound units take a delimited string such as
/// `'1-6'` (YEAR_MONTH) or `'1 12:30:45'` (DAY_SECOND), matching MySQL's
/// `INTERVAL` literal grammar.
fn parse_interval_parts(value: &str, unit: &str) -> Result<IntervalParts, QueryError> {
    let unit = unit.trim().to_ascii_uppercase();
    let value = value.trim();
    // Compound units carry multiple components packed into one literal.
    let nums = parse_interval_components(value, &unit)?;
    let bad = || {
        error(
            crate::error::catalog::ER_PARSE_ERROR,
            format!("EVERY value '{value}' is not valid for unit {unit}"),
        )
    };
    let n = |i: usize| -> i64 { nums.get(i).copied().unwrap_or(0) };
    let single = || -> Result<i64, QueryError> {
        if nums.len() != 1 {
            return Err(bad());
        }
        Ok(nums[0])
    };
    let expect = |count: usize| -> Result<(), QueryError> {
        if nums.len() == count {
            Ok(())
        } else {
            Err(bad())
        }
    };
    const MS: i64 = 1;
    const SEC: i64 = 1_000;
    const MIN: i64 = 60 * 1_000;
    const HOUR: i64 = 3_600 * 1_000;
    let parts = match unit.as_str() {
        "MICROSECOND" => {
            let us = single()?;
            // Sub-millisecond floor: the scheduler wakes on a 1 ms grid.
            let ms = (us + 999) / 1_000;
            if us > 0 && ms == 0 {
                IntervalParts {
                    months: 0,
                    days: 0,
                    ms: 1,
                }
            } else {
                IntervalParts {
                    months: 0,
                    days: 0,
                    ms: ms * MS,
                }
            }
        }
        "SECOND" => IntervalParts {
            months: 0,
            days: 0,
            ms: single()? * SEC,
        },
        "MINUTE" => IntervalParts {
            months: 0,
            days: 0,
            ms: single()? * MIN,
        },
        "HOUR" => IntervalParts {
            months: 0,
            days: 0,
            ms: single()? * HOUR,
        },
        "DAY" => IntervalParts {
            months: 0,
            days: single()?,
            ms: 0,
        },
        "WEEK" => IntervalParts {
            months: 0,
            days: single()? * 7,
            ms: 0,
        },
        "MONTH" => IntervalParts {
            months: single()?,
            days: 0,
            ms: 0,
        },
        "QUARTER" => IntervalParts {
            months: single()? * 3,
            days: 0,
            ms: 0,
        },
        "YEAR" => IntervalParts {
            months: single()? * 12,
            days: 0,
            ms: 0,
        },
        "YEAR_MONTH" => {
            expect(2)?;
            IntervalParts {
                months: n(0) * 12 + n(1),
                days: 0,
                ms: 0,
            }
        }
        "DAY_HOUR" => {
            expect(2)?;
            IntervalParts {
                months: 0,
                days: n(0),
                ms: n(1) * HOUR,
            }
        }
        "DAY_MINUTE" => {
            expect(3)?;
            IntervalParts {
                months: 0,
                days: n(0),
                ms: n(1) * HOUR + n(2) * MIN,
            }
        }
        "DAY_SECOND" => {
            expect(4)?;
            IntervalParts {
                months: 0,
                days: n(0),
                ms: n(1) * HOUR + n(2) * MIN + n(3) * SEC,
            }
        }
        "DAY_MICROSECOND" => {
            // d h:m:s.us — 5 components
            expect(5)?;
            let us_ms = (n(4) + 999) / 1_000;
            IntervalParts {
                months: 0,
                days: n(0),
                ms: n(1) * HOUR + n(2) * MIN + n(3) * SEC + us_ms.max(if n(4) > 0 { 1 } else { 0 }),
            }
        }
        "HOUR_MINUTE" => {
            expect(2)?;
            IntervalParts {
                months: 0,
                days: 0,
                ms: n(0) * HOUR + n(1) * MIN,
            }
        }
        "HOUR_SECOND" => {
            expect(3)?;
            IntervalParts {
                months: 0,
                days: 0,
                ms: n(0) * HOUR + n(1) * MIN + n(2) * SEC,
            }
        }
        "HOUR_MICROSECOND" => {
            expect(4)?;
            let us_ms = (n(3) + 999) / 1_000;
            IntervalParts {
                months: 0,
                days: 0,
                ms: n(0) * HOUR + n(1) * MIN + n(2) * SEC + us_ms.max(if n(3) > 0 { 1 } else { 0 }),
            }
        }
        "MINUTE_SECOND" => {
            expect(2)?;
            IntervalParts {
                months: 0,
                days: 0,
                ms: n(0) * MIN + n(1) * SEC,
            }
        }
        "MINUTE_MICROSECOND" => {
            expect(3)?;
            let us_ms = (n(2) + 999) / 1_000;
            IntervalParts {
                months: 0,
                days: 0,
                ms: n(0) * MIN + n(1) * SEC + us_ms.max(if n(2) > 0 { 1 } else { 0 }),
            }
        }
        "SECOND_MICROSECOND" => {
            expect(2)?;
            let us_ms = (n(1) + 999) / 1_000;
            IntervalParts {
                months: 0,
                days: 0,
                ms: n(0) * SEC + us_ms.max(if n(1) > 0 { 1 } else { 0 }),
            }
        }
        other => {
            return Err(error(
                crate::error::catalog::ER_EVENT_INTERVAL_NOT_POSITIVE_OR_TOO_BIG,
                format!("unsupported EVERY unit '{other}'"),
            ));
        }
    };
    if parts.months < 0 || parts.days < 0 || parts.ms < 0 || parts.is_zero() {
        return Err(error(
            crate::error::catalog::ER_EVENT_INTERVAL_NOT_POSITIVE_OR_TOO_BIG,
            "EVERY interval must be strictly positive",
        ));
    }
    Ok(parts)
}

/// Split a compound-interval literal into its integer components.
/// Accepts the MySQL delimiters: `-` (YEAR_MONTH), space (day vs time),
/// `:` (time fields) and `.` (fractional seconds). A bare integer (no
/// delimiter) yields a single component.
fn parse_interval_components(value: &str, unit: &str) -> Result<Vec<i64>, QueryError> {
    // Strip a surrounding quote pair if present (the parser keeps the
    // raw expr text, so `'1:30'` arrives quoted).
    let trimmed = value.trim();
    let unquoted = trimmed
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .or_else(|| trimmed.strip_prefix('"').and_then(|s| s.strip_suffix('"')))
        .unwrap_or(trimmed)
        .trim();
    if unquoted.is_empty() {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "EVERY value is empty",
        ));
    }
    if unquoted.starts_with('-') || (unit != "YEAR_MONTH" && unquoted.contains('-')) {
        return Err(error(
            crate::error::catalog::ER_EVENT_INTERVAL_NOT_POSITIVE_OR_TOO_BIG,
            "EVERY interval must be strictly positive",
        ));
    }
    let mut out = Vec::new();
    for token in unquoted
        .split_whitespace()
        .flat_map(|part| part.split(['-', ':', '.']))
    {
        if token.is_empty() {
            return Err(error(
                crate::error::catalog::ER_EVENT_INTERVAL_NOT_POSITIVE_OR_TOO_BIG,
                "EVERY interval has an empty or signed component",
            ));
        }
        let n: i64 = token.trim().parse().map_err(|_| {
            error(
                crate::error::catalog::ER_PARSE_ERROR,
                format!("EVERY value '{value}' is not an integer"),
            )
        })?;
        // Bound components before multiplying units; otherwise a valid i64
        // literal can overflow in debug builds or wrap in release builds.
        if !(0..=i32::MAX as i64).contains(&n) {
            return Err(error(
                crate::error::catalog::ER_EVENT_INTERVAL_NOT_POSITIVE_OR_TOO_BIG,
                "EVERY interval component must be non-negative and at most 2147483647",
            ));
        }
        out.push(n);
    }
    if out.is_empty() {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "EVERY value has no components",
        ));
    }
    Ok(out)
}

/// Fixed-length duration of `interval` in ms, or `None` when it carries a
/// calendar (month-bearing) component that must step through the real
/// calendar via [`add_interval`].
fn fixed_interval_ms(interval: &EventIntervalSchema) -> Option<u64> {
    let parts = parse_interval_parts(&interval.value, &interval.unit).ok()?;
    if parts.months != 0 {
        return None;
    }
    let total = parts.days.saturating_mul(86_400 * 1_000) + parts.ms;
    if total <= 0 {
        None
    } else {
        Some(total as u64)
    }
}

/// Calendar-correct addition of `interval` to a UTC-ms instant, stepping
/// in the event's `time_zone` calendar so month/day boundaries and DST
/// transitions land on the intended local wall-clock. Month arithmetic
/// clamps to the last valid day (`2026-01-31 + 1 MONTH = 2026-02-28`),
/// matching MySQL / `TIMESTAMPADD`. Returns `None` on overflow or an
/// uninterpretable interval.
fn add_interval_parts(base_ms: u64, parts: IntervalParts, time_zone: &str) -> Option<u64> {
    // Work in local wall-clock so calendar fields (months, days) advance
    // on the event's own calendar; convert back to UTC at the end.
    let local_ms = utc_ms_to_local_ms(base_ms, time_zone);
    let secs = (local_ms / 1000) as i64;
    let nanos = ((local_ms % 1000) as u32) * 1_000_000;
    let dt = Utc.timestamp_opt(secs, nanos).single()?;
    let mut naive = dt.naive_utc();
    if parts.months != 0 {
        naive = add_months(naive, parts.months)?;
    }
    if parts.days != 0 {
        naive = naive.checked_add_signed(chrono::Duration::days(parts.days))?;
    }
    if parts.ms != 0 {
        naive = naive.checked_add_signed(chrono::Duration::milliseconds(parts.ms))?;
    }
    let local_out = naive.and_utc().timestamp_millis();
    if local_out < 0 {
        return None;
    }
    Some(local_ms_to_utc_ms(local_out as u64, time_zone))
}

/// Inverse of [`local_ms_to_utc_ms`]: render a true UTC instant as the
/// wall-clock ms in `time_zone` (i.e. seconds-since-epoch as if the local
/// time were UTC). Used so calendar stepping reasons in local time.
fn utc_ms_to_local_ms(utc_ms: u64, time_zone: &str) -> u64 {
    let utc_secs = (utc_ms / 1000) as i64;
    let sub_ms = (utc_ms % 1000) as i64;
    let tz = time_zone.trim();
    let local_secs =
        if tz.is_empty() || tz.eq_ignore_ascii_case("SYSTEM") || tz.eq_ignore_ascii_case("LOCAL") {
            use chrono::{Local, Offset};
            let off = Local::now().offset().fix().local_minus_utc() as i64;
            utc_secs + off
        } else if tz.eq_ignore_ascii_case("UTC") || tz.eq_ignore_ascii_case("GMT") {
            utc_secs
        } else if let Some(off) = parse_fixed_offset_secs(tz) {
            utc_secs + off
        } else {
            match crate::engine::tz::find_named_zone(tz) {
                Some(zone) => utc_secs + zone.offset_at_utc(utc_secs) as i64,
                None => utc_secs,
            }
        };
    let total = local_secs * 1000 + sub_ms;
    if total < 0 {
        0
    } else {
        total as u64
    }
}

/// Add `months` calendar months to `dt`, clamping the day-of-month to the
/// target month's length (Jan 31 + 1 month → Feb 28/29).
fn add_months(dt: NaiveDateTime, months: i64) -> Option<NaiveDateTime> {
    use chrono::{Datelike, NaiveDate};
    let total = dt.year() as i64 * 12 + (dt.month0() as i64) + months;
    let year = (total.div_euclid(12)) as i32;
    let month0 = total.rem_euclid(12) as u32;
    let month = month0 + 1;
    let last_day = days_in_month(year, month);
    let day = dt.day().min(last_day);
    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    Some(date.and_time(dt.time()))
}

fn days_in_month(year: i32, month: u32) -> u32 {
    use chrono::NaiveDate;
    let (ny, nm) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let first_next = NaiveDate::from_ymd_opt(ny, nm, 1);
    let first_this = NaiveDate::from_ymd_opt(year, month, 1);
    match (first_next, first_this) {
        (Some(a), Some(b)) => (a - b).num_days() as u32,
        _ => 31,
    }
}

/// Compute the next due timestamp (UTC ms) for `event`, taking
/// `last_executed_ms` into account.  Returns `None` if the event has
/// expired (one-shot already fired, or recurring window past `ENDS`).
///
/// "Next due" means the earliest scheduled instant that the scheduler
/// has not yet acted on.  When the scheduler wakes at `from_ms` and
/// `next_due <= from_ms`, it should fire immediately.
///
/// G5 (catch-up): when the scheduler has been parked (server down /
/// `event_scheduler=OFF`) across several periods, MySQL does **not**
/// back-fill every missed instant. It fires at most once for the
/// overdue window and then resumes on the next future aligned point.
/// We model this by skipping the aligned tick forward to the most
/// recent occurrence that is `<= from_ms` (so it still fires once),
/// rather than returning the very first un-serviced tick.
pub fn next_due_ms(event: &EventSchema, from_ms: u64) -> Option<u64> {
    match &event.schedule {
        EventScheduleSchema::OneShot { execute_at_ms } => {
            if event.last_executed_ms >= *execute_at_ms {
                None
            } else {
                Some(*execute_at_ms)
            }
        }
        EventScheduleSchema::Recurring {
            interval,
            starts_ms,
            ends_ms,
        } => {
            // The first un-serviced aligned tick strictly after the most
            // recent run (or `starts_ms` if never run).
            let mut next = if event.last_executed_ms < *starts_ms {
                *starts_ms
            } else {
                interval_step_after(
                    *starts_ms,
                    event.last_executed_ms,
                    interval,
                    &event.time_zone,
                )?
            };
            // G5: if more than one whole period is already overdue, skip
            // the intervening (missed) occurrences and align to the most
            // recent tick at or before `from_ms`. The single remaining
            // overdue tick still fires immediately.
            if next < from_ms {
                if let Some(aligned) =
                    align_forward(*starts_ms, interval, from_ms, &event.time_zone)
                {
                    if aligned > next {
                        next = aligned;
                    }
                }
            }
            if let Some(ends) = ends_ms {
                if next > *ends {
                    return None;
                }
            }
            Some(next)
        }
    }
}

/// The next scheduled instant strictly after `serviced_ms`, assuming the
/// event just fired at `serviced_ms`. Returns `None` when there is no
/// further occurrence (a one-shot that just ran, or a recurring event
/// whose next tick would fall past `ENDS`). Used by the scheduler loop to
/// pick a correct sleep target right after firing, instead of falling
/// into the coarse idle fallback.
fn following_due_ms(event: &EventSchema, serviced_ms: u64) -> Option<u64> {
    let mut advanced = event.clone();
    advanced.last_executed_ms = serviced_ms;
    next_due_ms(&advanced, serviced_ms.saturating_add(1))
}

/// First aligned occurrence of `interval` strictly greater than
/// `after_ms`, anchored at `starts_ms`. Calendar units step through the
/// real calendar; fixed-length units use direct multiplication. Returns
/// `None` if the interval cannot be interpreted.
fn interval_step_after(
    starts_ms: u64,
    after_ms: u64,
    interval: &EventIntervalSchema,
    time_zone: &str,
) -> Option<u64> {
    let mut tick = starts_ms;
    if tick > after_ms {
        return Some(tick);
    }
    if let Some(step_ms) = fixed_interval_ms(interval) {
        let step_ms = step_ms.max(1);
        let elapsed = after_ms.saturating_sub(starts_ms);
        let count = elapsed / step_ms + 1;
        return Some(starts_ms.saturating_add(count.saturating_mul(step_ms)));
    }
    // Calendar units: compute each occurrence from STARTS + n*interval
    // rather than chaining from the previous occurrence. This preserves
    // the original day-of-month anchor across clamp points:
    // Jan 31 + 1 MONTH = Feb 28, and the following tick is Mar 31.
    let mut occurrence = 1u64;
    while tick <= after_ms {
        let stepped = add_interval_occurrence(starts_ms, interval, occurrence, time_zone)?;
        if stepped <= tick {
            // Defensive: avoid an infinite loop on a degenerate step.
            return Some(tick.saturating_add(1));
        }
        tick = stepped;
        occurrence = occurrence.saturating_add(1);
        if occurrence > 10_000_000 {
            return Some(tick);
        }
    }
    Some(tick)
}

/// Most recent aligned occurrence `<= now_ms` (or the first future one
/// if `now_ms < starts_ms`). Used by the catch-up logic to skip missed
/// occurrences without back-filling each one.
fn align_forward(
    starts_ms: u64,
    interval: &EventIntervalSchema,
    now_ms: u64,
    time_zone: &str,
) -> Option<u64> {
    if now_ms < starts_ms {
        return Some(starts_ms);
    }
    if let Some(step_ms) = fixed_interval_ms(interval) {
        let step_ms = step_ms.max(1);
        let elapsed = now_ms.saturating_sub(starts_ms);
        let count = elapsed / step_ms;
        return Some(starts_ms.saturating_add(count.saturating_mul(step_ms)));
    }
    // Calendar units: walk forward keeping the last tick that is <= now,
    // with each occurrence anchored back to STARTS.
    let mut tick = starts_ms;
    let mut occurrence = 1u64;
    loop {
        let stepped = add_interval_occurrence(starts_ms, interval, occurrence, time_zone)?;
        if stepped > now_ms || stepped <= tick {
            return Some(tick);
        }
        tick = stepped;
        occurrence = occurrence.saturating_add(1);
        if occurrence > 10_000_000 {
            return Some(tick);
        }
    }
}

fn add_interval_occurrence(
    starts_ms: u64,
    interval: &EventIntervalSchema,
    occurrence: u64,
    time_zone: &str,
) -> Option<u64> {
    let parts = parse_interval_parts(&interval.value, &interval.unit).ok()?;
    let mut multiplied = parts;
    let n = i64::try_from(occurrence).ok()?;
    multiplied.months = multiplied.months.checked_mul(n)?;
    multiplied.days = multiplied.days.checked_mul(n)?;
    multiplied.ms = multiplied.ms.checked_mul(n)?;
    add_interval_parts(starts_ms, multiplied, time_zone)
}

/// Format a UTC-ms schedule timestamp as the `"YYYY-MM-DD HH:MM:SS"`
/// wall-clock string in `time_zone` — the form MySQL shows on
/// information_schema.events / SHOW EVENTS / SHOW CREATE EVENT (which
/// display schedule times in the event's own zone).
pub fn format_ms_in_tz(ms: u64, time_zone: &str) -> String {
    if ms == 0 {
        return String::new();
    }
    let local_ms = utc_ms_to_local_ms(ms, time_zone);
    format_ms_as_datetime(local_ms)
}

/// Convert a unix-ms timestamp into the `"YYYY-MM-DD HH:MM:SS"` form
/// MySQL uses on information_schema.events / SHOW EVENTS.
pub fn format_ms_as_datetime(ms: u64) -> String {
    if ms == 0 {
        return String::new();
    }
    let secs = (ms / 1000) as i64;
    let nanos = ((ms % 1000) as u32) * 1_000_000;
    let dt = Utc
        .timestamp_opt(secs, nanos)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).unwrap());
    dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    #[test]
    fn scheduler_wakeup_is_retained_until_worker_waits() {
        let notify = Notify::new();

        // Reproduce the shutdown race deterministically: the notification
        // lands before the worker registers `notified().await`.
        wake_scheduler(&notify);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_millis(100), notify.notified())
                .await
                .expect("scheduler wakeup was lost before the waiter registered");
        });
    }
}

#[cfg(test)]
#[test]
fn shutdown_waits_for_event_worker_past_old_detach_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let engine = super::StorageQueryEngine::open(dir.path()).unwrap();
    let scheduler = EventSchedulerHandle::spawn(
        Arc::clone(&engine.db),
        Arc::new(crate::engine::SessionRegistry::new()),
    );
    scheduler.shutdown();
    let (release, blocked) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        blocked.recv().unwrap();
    });
    let pool = EventWorkerPool {
        sender: None,
        workers: vec![worker],
        inner: Arc::clone(&scheduler.inner),
    };
    let (done, completed) = std::sync::mpsc::channel();
    let shutdown = std::thread::spawn(move || {
        pool.shutdown();
        done.send(()).unwrap();
    });
    let prematurely_finished = completed.recv_timeout(Duration::from_millis(2200)).is_ok();
    release.send(()).unwrap();
    shutdown.join().unwrap();
    assert!(
        !prematurely_finished,
        "shutdown detached a live event writer"
    );
}
