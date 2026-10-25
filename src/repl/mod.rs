//! mydb-native physical replication (mydb → mydb).
//!
//! Separate from [`crate::replication`] (which is the MySQL→mydb logical
//! binlog receiver). The two coexist: `SHOW REPLICA STATUS` remains the
//! MySQL-compatible logical replica status, while `SHOW MYDB REPLICA
//! STATUS` reflects this physical-WAL replica.
//!
//! Architecture (full):
//!
//! ```text
//!   Master                          Replica
//!   ------                          -------
//!   WAL append --> notify ---->  Receiver --> Applier (recovery replay)
//!                              ^                      |
//!                              | ReplicaAck           v
//!                              +-------- progress.json
//!
//!   per-replica Slot (acked_lsn) ↔ WAL retention min
//! ```
//!
//! Phase 0 lands: module skeleton, role file, wire protocol, env config,
//! SQL admin gateway with stub handlers. Subsequent phases fill in the
//! WAL reader, slots, sender/receiver, applier, observability, and
//! switchover.

pub mod base_backup;
pub mod config;
pub mod durable;
pub mod lifecycle;
pub mod master;
pub mod protocol;
pub mod replica;
pub mod role;
pub mod tls;

#[cfg(test)]
mod tests_integration;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use parking_lot::Mutex as PlMutex;
use tokio::runtime::Handle;

use crate::engine::{QueryResponse, ResultSet, StorageQueryEngine};
use crate::parser::Statement;
use crate::storage::error;
use crate::types::{Column, ColumnType, QueryError, Value};

pub use lifecycle::ReceiverLifecycle;

pub use config::ReplConfig;
pub use master::{SlotRegistry, SlotStatus};
pub use replica::ReplicaProgress;
pub use role::{Role, RoleState};

/// Wall-clock milliseconds used by replication wire messages and metadata.
/// Keep full clock resolution; these timestamps are not coarse-clock samples.
#[inline]
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

/// Transport security failures share one classification and diagnostic.
/// Ordinary I/O failures retain the mapping owned by each caller.
fn transport_security_error(detail: &str) -> Option<QueryError> {
    let lower = detail.to_ascii_lowercase();
    if lower.contains("certificate")
        || lower.contains("unknownca")
        || lower.contains("unknown issuer")
        || lower.contains("tls")
    {
        Some(error(
            crate::error::catalog::MYDB_ER_REPLICATION_SECURITY,
            format!("native replication TLS authentication failed: {detail}"),
        ))
    } else {
        None
    }
}

static GLOBAL: OnceLock<Arc<MydbReplManager>> = OnceLock::new();

/// Process-wide manager for mydb-native physical replication.
///
/// Holds the data directory, the engine handle, the loaded role state,
/// and the resolved configuration. Subsystem handles (sender service,
/// receiver, slot registry) live as `Option`s on this struct and are
/// started/stopped by the role transitions.
pub struct MydbReplManager {
    pub(crate) data_dir: PathBuf,
    pub(crate) engine: Arc<StorageQueryEngine>,
    pub(crate) role_state: Arc<Mutex<RoleState>>,
    pub(crate) config: ReplConfig,
    /// Master-side replication slots — populated on master role, but the
    /// registry exists in every role so admin commands like `DROP MYDB
    /// REPLICA` can inspect/remove stale slot files.
    pub(crate) slots: Arc<PlMutex<SlotRegistry>>,
    /// Phase 7 metrics: count of commits that waited (acknowledged) vs
    /// degraded to async by the semi-sync timeout.
    pub(crate) semi_sync_acked_commits: AtomicU64,
    pub(crate) semi_sync_timeout_commits: AtomicU64,
    /// Commits that ran semi-sync degraded: either no replica was
    /// connected, or fewer replicas exist than `wait_count` (threshold
    /// unreachable). Kept separate from `acked` so operators aren't
    /// misled into thinking a replica confirmed durability when none
    /// did (C8).
    pub(crate) semi_sync_degraded_commits: AtomicU64,
    /// Phase 8: `STOP MASTER WRITES` toggles this flag. When set, new
    /// write statements at this master are rejected with MySQL error
    /// 1290; existing transactions are allowed to drain.
    pub(crate) master_writes_frozen: AtomicBool,
    /// Independent ownership: RESUME MASTER WRITES cannot release a backup's
    /// capture fence, and finishing a backup cannot release an admin fence.
    pub(crate) backup_capture_frozen: AtomicBool,
    /// Tokio runtime handle captured at `init_global` time. The SQL
    /// admin handlers run inside `spawn_blocking`, so they cannot
    /// directly `tokio::spawn`; they use this handle to attach the
    /// receiver task to the application's runtime instead.
    pub(crate) runtime_handle: PlMutex<Option<Handle>>,
    /// Receiver-task lifecycle wrapper. Holds the optional
    /// `Notify` + `JoinHandle` pair while the receiver is running.
    pub(crate) receiver_lifecycle: Arc<PlMutex<ReceiverLifecycle>>,
    /// Master-side WAL listener (`MYDB_REPL_PORT`). Populated when this
    /// node runs as master; PROMOTE starts it, DEMOTE stops it.
    pub(crate) master_service: Arc<PlMutex<Option<master::MasterService>>>,
    /// Latest receiver status line for SHOW MYDB REPLICA STATUS
    /// (connected / last error / resync reason).
    pub(crate) receiver_status: Arc<PlMutex<String>>,
    pub(crate) receiver_progress: Arc<PlMutex<ReplicaProgress>>,
}

/// Initialize the global manager. Idempotent; the first call wins and
/// subsequent calls return the same `Arc`. The role file is read (and
/// created if missing) under `<data-dir>/replication/role.json`, with
/// `MYDB_ROLE` taken as an override.
pub fn init_global(
    data_dir: impl Into<PathBuf>,
    engine: Arc<StorageQueryEngine>,
) -> Result<Arc<MydbReplManager>, QueryError> {
    let data_dir: PathBuf = data_dir.into();
    let initial_state = role::load_or_init(&data_dir).map_err(|err| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("mydb-repl: failed to load role.json: {err}"),
        )
    })?;
    let config = ReplConfig::from_env();
    let initially_frozen = initial_state.write_fence.is_some();
    let initial_replica_progress = replica::load_progress(&data_dir)
        .ok()
        .flatten()
        .unwrap_or_default();
    let slots = Arc::new(PlMutex::new(SlotRegistry::open(&data_dir).map_err(
        |err| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("mydb-repl: failed to open slots dir: {}", err.message()),
            )
        },
    )?));
    let manager = GLOBAL.get_or_init(|| {
        // Capture the running tokio runtime handle so SQL admin
        // commands fired later from inside `spawn_blocking` can
        // attach background receiver tasks to it.
        let runtime_handle = Handle::try_current().ok();
        Arc::new(MydbReplManager {
            data_dir,
            engine,
            role_state: Arc::new(Mutex::new(initial_state)),
            config,
            slots,
            semi_sync_acked_commits: AtomicU64::new(0),
            semi_sync_timeout_commits: AtomicU64::new(0),
            semi_sync_degraded_commits: AtomicU64::new(0),
            master_writes_frozen: AtomicBool::new(initially_frozen),
            backup_capture_frozen: AtomicBool::new(false),
            runtime_handle: PlMutex::new(runtime_handle),
            receiver_lifecycle: Arc::new(PlMutex::new(ReceiverLifecycle::new())),
            master_service: Arc::new(PlMutex::new(None)),
            receiver_status: Arc::new(PlMutex::new(String::new())),
            receiver_progress: Arc::new(PlMutex::new(initial_replica_progress)),
        })
    });
    manager.finish_pending_promotion()?;
    // Auto-start the role-appropriate subsystem. Replicas spin up the
    // receiver; masters bind the WAL listener so replicas can connect
    // (previously only tests ever started the master service).
    if manager.config.auto_start {
        match manager.role() {
            Role::Replica => {
                if manager
                    .role_state
                    .lock()
                    .map(|s| s.master_endpoint.is_some())
                    .unwrap_or(false)
                {
                    manager.start_receiver_if_needed()?;
                } else {
                    crate::util::logging::log_message("mydb-repl: replica is unconfigured; waiting for CHANGE MYDB MASTER TO (writes remain disabled)");
                }
            }
            Role::Master => {
                manager.start_master_service_if_needed()?;
            }
            Role::Standalone => {}
        }
    }
    Ok(Arc::clone(manager))
}

/// True for either side of mydb's physical WAL-streaming replication.
/// Reduced data redo cannot be streamed because the omitted heap payloads do
/// not exist in the WAL consumed by a physical replica.
pub fn physical_replication_active() -> bool {
    global_manager()
        .map(|manager| manager.role() != Role::Standalone)
        .unwrap_or(false)
}

/// Replica bootstrap, run BEFORE the engine opens its `Database`:
/// when this node is a replica whose data directory is fresh (or whose
/// previous run flagged `resync_required`), pull a streaming base
/// backup from the configured master so the engine opens on a
/// consistent snapshot and WAL streaming can resume from `backup_lsn+1`.
pub async fn maybe_bootstrap_replica(data_dir: &std::path::Path) -> Result<(), QueryError> {
    // Resolve a crash between staged subtree renames before deciding whether
    // the directory is fresh. Otherwise a partially-installed `schemas/`
    // makes `fresh=false` and can bypass the retry entirely.
    base_backup::recover_interrupted_install(data_dir)?;
    if !role::physical_replica_on_disk(data_dir) {
        return Ok(());
    }
    let config = ReplConfig::from_env();
    // Persist the replica role + endpoint now so the engine's recovery
    // path (and a later `init_global`) see a consistent role file.
    let state = role::load_or_init(data_dir).map_err(|err| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("mydb-repl bootstrap: role.json: {err}"),
        )
    })?;
    let endpoint_from_state = state.master_endpoint.clone();
    let (host, port) = match (&config.master_host, endpoint_from_state) {
        (_, Some(ep)) => match ep.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), p.parse().unwrap_or(config.master_port)),
            None => (ep, config.master_port),
        },
        (Some(h), None) => (h.clone(), config.master_port),
        (None, None) => return Ok(()), // not configured yet; CHANGE MYDB MASTER TO later
    };
    if state.master_endpoint.is_none() {
        let mut updated = state.clone();
        updated.master_endpoint = Some(format!("{host}:{port}"));
        role::save(data_dir, &updated).map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("persist bootstrap endpoint: {err}"),
            )
        })?;
    }
    let resync_reason = replica::progress::resync_marker_reason(data_dir);
    let fresh = !crate::storage::naming::schemas_root(data_dir).exists();
    if !fresh && resync_reason.is_none() {
        return Ok(());
    }
    if let Some(reason) = &resync_reason {
        crate::util::logging::log_message(format_args!(
            "mydb-repl bootstrap: re-seeding from master {host}:{port} (resync required: {reason})"
        ));
    } else {
        crate::util::logging::log_message(format_args!(
            "mydb-repl bootstrap: fresh data dir; seeding base backup from master {host}:{port}"
        ));
    }
    // Keep the engine closed until data and progress form a complete seed.
    // A persistent marker also covers crashes after installing a fresh seed.
    replica::progress::write_resync_marker(
        data_dir,
        resync_reason
            .as_deref()
            .unwrap_or("initial base backup required"),
    )
    .map_err(|err| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("persist bootstrap marker: {err}"),
        )
    })?;
    let mut delay = config.reconnect_backoff.clamp(
        std::time::Duration::from_millis(100),
        std::time::Duration::from_secs(30),
    );
    let backup_lsn = loop {
        match base_backup::perform_base_backup_into(&host, port, &state.node_uuid, data_dir).await {
            Ok(lsn) => break lsn,
            Err(err) => {
                crate::util::logging::log_message(format_args!(
                    "mydb-repl bootstrap: {}; retry in {:?} (SQL unavailable until seed completes)",
                    err.message(),
                    delay
                ));
                tokio::time::sleep(delay).await;
                delay = delay
                    .saturating_mul(2)
                    .min(std::time::Duration::from_secs(30));
            }
        }
    };
    replica::progress::clear_resync_marker(data_dir);
    crate::util::logging::log_message(format_args!(
        "mydb-repl bootstrap: base backup complete at LSN {backup_lsn}"
    ));
    Ok(())
}

/// Get the global manager if it has been initialized.
pub fn global_manager() -> Option<Arc<MydbReplManager>> {
    GLOBAL.get().map(Arc::clone)
}

/// Returns true for the small set of admin commands handled by this
/// module — they all use the `MYDB` keyword to disambiguate from the
/// MySQL-compatible commands in [`crate::replication`].
pub fn is_mydb_repl_admin_sql(sql: &str) -> bool {
    // Head guard — only these keywords ever begin a mydb-native replication
    // admin command. Reject the rest without allocating a full-SQL uppercase
    // copy.
    let (head_buf, head_len) = crate::parser::extract_first_keyword(sql);
    if !matches!(
        &head_buf[..head_len],
        b"SHOW"
            | b"CHANGE"
            | b"START"
            | b"STOP"
            | b"RESET"
            | b"PROMOTE"
            | b"DEMOTE"
            | b"RESUME"
            | b"DROP"
    ) {
        return false;
    }
    let upper = sql.trim().trim_end_matches(';').trim().to_ascii_uppercase();
    upper == "SHOW MYDB MASTER STATUS"
        || upper == "SHOW MYDB REPLICA STATUS"
        || upper == "SHOW MYDB REPLICAS"
        || upper.starts_with("CHANGE MYDB SEQUENCE SYNC TO")
        || upper.starts_with("CHANGE MYDB MASTER TO")
        || upper == "START MYDB REPLICATION"
        || upper == "START MYDB REPLICATION FORCE_RESYNC"
        || upper == "STOP MYDB REPLICATION"
        || upper == "RESET MYDB REPLICATION"
        || upper.starts_with("PROMOTE TO MASTER")
        || upper.starts_with("DEMOTE TO REPLICA")
        || upper.starts_with("STOP MASTER WRITES")
        || upper == "RESUME MASTER WRITES"
        || upper.starts_with("DROP MYDB REPLICA")
}

#[cfg(test)]
/// Phase 0 entry point: parses the SQL command and dispatches to the
/// corresponding subsystem handler. Subsystems not yet implemented
/// return a `NotImplemented`-style error so the wiring is in place and
/// observable.
pub fn handle_admin_sql(sql: &str) -> Result<QueryResponse, QueryError> {
    handle_admin_sql_with_session(sql, &crate::engine::SessionContext::default())
}

pub fn handle_admin_sql_with_session(
    sql: &str,
    session: &crate::engine::SessionContext,
) -> Result<QueryResponse, QueryError> {
    let manager = global_manager().ok_or_else(|| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "mydb-repl manager is not initialized in this mydb process",
        )
    })?;
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();

    if upper == "SHOW MYDB MASTER STATUS" {
        return manager.show_master_status();
    }
    if upper == "SHOW MYDB REPLICA STATUS" {
        return manager.show_replica_status();
    }
    if upper == "SHOW MYDB REPLICAS" {
        return manager.show_replicas();
    }
    if upper.starts_with("CHANGE MYDB SEQUENCE SYNC TO") {
        return manager.handle_change_sequence_sync(trimmed);
    }
    if upper.starts_with("CHANGE MYDB MASTER TO") {
        return manager.handle_change_master(trimmed);
    }
    if upper == "START MYDB REPLICATION FORCE_RESYNC" {
        return manager.handle_force_resync(session);
    }
    if upper == "START MYDB REPLICATION" {
        return manager.handle_start_replication();
    }
    if upper == "STOP MYDB REPLICATION" {
        return manager.handle_stop_replication();
    }
    if upper == "RESET MYDB REPLICATION" {
        return manager.handle_reset_replication();
    }
    if upper.starts_with("PROMOTE TO MASTER") {
        return manager.handle_promote(trimmed, session);
    }
    if upper.starts_with("DEMOTE TO REPLICA") {
        return manager.handle_demote(trimmed);
    }
    if upper.starts_with("STOP MASTER WRITES") {
        return manager.handle_stop_master_writes(trimmed, session);
    }
    if upper == "RESUME MASTER WRITES" {
        return manager.handle_resume_master_writes();
    }
    if upper.starts_with("DROP MYDB REPLICA") {
        return manager.handle_drop_replica(trimmed);
    }
    Err(error(
        crate::error::catalog::ER_NOT_SUPPORTED_YET,
        format!("unsupported mydb-repl command: {trimmed}"),
    ))
}

/// Physical replay does not generate a logical binlog. Refuse mixed
/// cascades instead of advertising a healthy stream with missing changes.
pub(crate) fn ensure_logical_source_supported() -> Result<(), QueryError> {
    if global_manager().is_some_and(|manager| manager.role() == Role::Replica) {
        return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "a physical replica cannot serve a MySQL binlog downstream; use native physical replication or a logical replication source"));
    }
    Ok(())
}

/// Enforce role-based read-only at the dispatch entry point. When this
/// node is in `Role::Replica`, any write statement (DDL, DML, DCL,
/// `SELECT ... FOR UPDATE`) is rejected with MySQL error 1290. Also
/// fires when the node is a master with `STOP MASTER WRITES` in effect
/// — used by the manual-switchover flow to drain writes before
/// promoting a replica. The applier task uses
/// [`crate::replication::replication_applier_active`] (the same
/// thread-local) to bypass this check.
pub fn enforce_role_read_only(statement: &Statement) -> Result<(), QueryError> {
    let manager = global_manager();
    let role = manager
        .as_ref()
        .map(|m| m.role())
        .unwrap_or(Role::Standalone);
    if let Some(mgr) = &manager {
        if role == Role::Master
            && (mgr.master_writes_frozen.load(Ordering::Acquire)
                || mgr.backup_capture_frozen.load(Ordering::Acquire))
            && !crate::replication::replication_applier_active()
            && is_role_write_statement(statement)
        {
            return Err(error(crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
                "master writes are frozen (STOP MASTER WRITES); RESUME MASTER WRITES or PROMOTE a replica",
            ));
        }
    }
    enforce_read_only_for_role(role, statement)
}

/// Testable variant: accepts the role directly so unit tests can drive
/// every role without going through the process-global `OnceLock`.
pub fn enforce_read_only_for_role(role: Role, statement: &Statement) -> Result<(), QueryError> {
    if role != Role::Replica {
        return Ok(());
    }
    if crate::replication::replication_applier_active() {
        return Ok(());
    }
    if !is_role_write_statement(statement) {
        return Ok(());
    }
    Err(error(crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
        "this mydb instance is a replica (role=replica) and is read-only; promote to master before issuing writes",
    ))
}

pub(crate) fn ensure_sequence_write_allowed() -> Result<(), QueryError> {
    if let Some(manager) = global_manager() {
        if manager.role() == Role::Replica
            || manager.master_writes_frozen.load(Ordering::SeqCst)
            || manager.backup_capture_frozen.load(Ordering::SeqCst)
        {
            return Err(error(
                crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
                "sequence writes are frozen or this node is a replica",
            ));
        }
    }
    Ok(())
}

pub(crate) fn wait_for_sequence_durable(replicas: &[String], lsn: u64) -> Result<(), QueryError> {
    let manager = global_manager().ok_or_else(|| {
        error(
            crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
            "remote_apply requires native replication",
        )
    })?;
    if manager.role() != Role::Master {
        return Err(error(
            crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
            "remote_apply requires a native master",
        ));
    }
    let local = manager.role_snapshot().node_uuid;
    let required: Vec<_> = replicas
        .iter()
        .filter(|id| **id != local)
        .cloned()
        .collect();
    if required.is_empty() {
        return Err(error(
            crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
            "remote_apply has no remote failover candidate; configure a replacement replica",
        ));
    }
    let waiter = manager
        .slots
        .lock()
        .named_sequence_waiter(&required)
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                "required sequence replica is missing; reservation may already be durable locally",
            )
        })?;
    let deadline = std::time::Instant::now() + manager.config.semi_sync_timeout;
    loop {
        crate::engine::cancel::check_cancelled()?;
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let outcome = waiter.wait_for_ack(
            lsn,
            required.len() as u32,
            remaining.min(std::time::Duration::from_millis(50)),
        );
        if outcome == master::slot::SemiSyncOutcome::Acknowledged {
            return Ok(());
        }
        if remaining.is_zero() || outcome != master::slot::SemiSyncOutcome::TimedOut {
            return Err(error(crate::error::catalog::ER_LOCK_WAIT_TIMEOUT, format!("sequence remote_apply not confirmed at LSN {lsn}: {outcome:?}; state may already be durable locally")));
        }
    }
}

/// Phase 7 entry point invoked by the storage layer's commit path
/// after the WAL is locally durable. When the node is a master in
/// `semi_sync` mode, blocks until enough replicas have acked the
/// `commit_lsn` or the configured timeout elapses. Records a metric
/// either way; never propagates an error (semi-sync is best-effort).
pub fn after_local_commit_durable(commit_lsn: u64) {
    let Some(manager) = global_manager() else {
        return;
    };
    if manager.role() != Role::Master {
        return;
    }
    if manager.config.mode != config::ReplMode::SemiSync {
        return;
    }
    // Snapshot the slots under a brief registry lock, then RELEASE it
    // before blocking on the ack. Holding `manager.slots.lock()` across
    // the wait serialized every concurrent committer through one lock
    // during the wait window, collapsing semi-sync throughput to a
    // single in-flight commit (Pf1).
    let waiter = { manager.slots.lock().semi_sync_waiter() };
    let outcome = waiter.wait_for_ack(
        commit_lsn,
        manager.config.semi_sync_wait_count,
        manager.config.semi_sync_timeout,
    );
    match outcome {
        master::slot::SemiSyncOutcome::Acknowledged => {
            manager
                .semi_sync_acked_commits
                .fetch_add(1, Ordering::Relaxed);
        }
        master::slot::SemiSyncOutcome::NoActiveSlots | master::slot::SemiSyncOutcome::Degraded => {
            manager
                .semi_sync_degraded_commits
                .fetch_add(1, Ordering::Relaxed);
        }
        master::slot::SemiSyncOutcome::TimedOut => {
            manager
                .semi_sync_timeout_commits
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Checkpoint hook: the lowest WAL LSN that physical-replication slots
/// still need, or `None` when nothing below `checkpoint_lsn` is pinned.
/// Applies the retention policy side effects while deciding — slots
/// silent past the timeout are marked Abandoned (stop pinning), and
/// slots whose lag would exceed `max_retained_bytes` are Invalidated
/// (their replicas must re-seed via base backup).
pub fn wal_retention_floor(
    checkpoint_lsn: u64,
    sealed_segments: &[crate::wal::WalSegmentInfo],
    now_ms: u64,
) -> Option<u64> {
    let manager = global_manager()?;
    // A promoting replica already owns the reserved fork slot while its
    // promotion checkpoints run, before the role itself becomes Master.
    let role = manager.role_snapshot();
    if role.role != Role::Master && !role.promotion_pending {
        return None;
    }
    let policy = master::RetentionPolicy {
        max_retained_bytes: manager.config.max_retained_bytes,
        slot_timeout_ms: manager
            .config
            .slot_timeout
            .as_millis()
            .min(u128::from(u64::MAX)) as u64,
    };
    let slots = manager.slots.lock();
    let decision = policy.decide(checkpoint_lsn, &slots.snapshot(), sealed_segments, now_ms);
    for uuid in &decision.abandon_slot_uuids {
        if slots.mark_abandoned(uuid).is_some() {
            let _ = slots.persist(uuid);
            crate::util::logging::log_message(format_args!(
                "mydb-repl master: slot {uuid} timed out; marked abandoned (stops pinning WAL retention)"
            ));
        }
    }
    for uuid in &decision.invalidate_slot_uuids {
        if slots.mark_invalidated(uuid).is_some() {
            let _ = slots.persist(uuid);
            crate::util::logging::log_message(format_args!(
                "mydb-repl master: slot {uuid} exceeded max retained WAL bytes; invalidated (replica must re-seed)"
            ));
        }
    }
    (decision.min_required_lsn < checkpoint_lsn).then_some(decision.min_required_lsn)
}

/// True for statements that the replica role must reject.
pub fn is_role_write_statement(statement: &Statement) -> bool {
    crate::statement_effects::is_replica_forbidden(statement)
}

impl MydbReplManager {
    pub fn role(&self) -> Role {
        self.role_state.lock().unwrap().role
    }

    pub fn role_snapshot(&self) -> RoleState {
        self.role_state.lock().unwrap().clone()
    }

    /// Persist a new role state, bumping the epoch when the role flag
    /// changes. Used by PROMOTE/DEMOTE handlers in phase 8 — exposed
    /// here so tests can flip the role without going through SQL.
    pub fn set_role(&self, role: Role, master_endpoint: Option<String>) -> Result<(), QueryError> {
        let mut state = self.role_state.lock().unwrap();
        if state.role != role {
            state.role = role;
            state.epoch = state.epoch.saturating_add(1);
            match role {
                Role::Master => {
                    state.promoted_at_ms = now_ms();
                    state.master_endpoint = None;
                }
                Role::Replica => {
                    state.demoted_at_ms = now_ms();
                    state.master_endpoint = master_endpoint;
                }
                Role::Standalone => {
                    state.master_endpoint = None;
                }
            }
        } else if role == Role::Replica && state.master_endpoint != master_endpoint {
            state.master_endpoint = master_endpoint;
        }
        role::save(&self.data_dir, &state).map_err(|err| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("mydb-repl: failed to persist role.json: {err}"),
            )
        })?;
        self.engine
            .database_for_replication()
            .set_physical_replica_mode(role == Role::Replica)?;
        Ok(())
    }

    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    pub fn config(&self) -> &ReplConfig {
        &self.config
    }

    pub fn engine(&self) -> &Arc<StorageQueryEngine> {
        &self.engine
    }

    fn show_master_status(&self) -> Result<QueryResponse, QueryError> {
        let state = self.role_snapshot();
        let log = self.engine.database_for_replication().wal_log();
        let (
            current_lsn,
            durable_lsn,
            min_available_lsn,
            segment_count,
            active_segment_id,
            wal_bytes,
        ) = {
            let g = log.lock();
            (
                g.next_lsn().saturating_sub(1),
                g.durable_lsn(),
                g.min_available_lsn(),
                g.segment_count() as u64,
                g.active_segment_id(),
                g.wal_bytes(),
            )
        };
        let slot_snap = self.slots.lock().snapshot();
        let active_slots = slot_snap
            .iter()
            .filter(|s| s.status == SlotStatus::Active)
            .count() as u64;
        let min_slot_lsn = slot_snap
            .iter()
            .filter(|s| s.status == SlotStatus::Active)
            .map(|s| s.acked_lsn)
            .min()
            .map(Value::UInt)
            .unwrap_or(Value::Null);
        let columns = vec![
            mk_col("Node_UUID", ColumnType::VarString),
            mk_col("Cluster_UUID", ColumnType::VarString),
            mk_col("Timeline_UUID", ColumnType::VarString),
            mk_col("Role", ColumnType::VarString),
            mk_col("Epoch", ColumnType::LongLong),
            mk_col("Current_LSN", ColumnType::LongLong),
            mk_col("Durable_LSN", ColumnType::LongLong),
            mk_col("Min_Available_LSN", ColumnType::LongLong),
            mk_col("Active_Segment_Id", ColumnType::LongLong),
            mk_col("Segment_Count", ColumnType::LongLong),
            mk_col("WAL_Bytes", ColumnType::LongLong),
            mk_col("Active_Slot_Count", ColumnType::LongLong),
            mk_col("Invalidated_Slot_Count", ColumnType::LongLong),
            mk_col("Abandoned_Slot_Count", ColumnType::LongLong),
            mk_col("Min_Slot_Acked_LSN", ColumnType::LongLong),
            mk_col("Replication_Mode", ColumnType::VarString),
            mk_col("Bind_Port", ColumnType::LongLong),
            mk_col("Listener_Running", ColumnType::VarString),
            mk_col("Connected_Senders", ColumnType::LongLong),
            mk_col("Semi_Sync_Acked_Commits", ColumnType::LongLong),
            mk_col("Semi_Sync_Timeout_Commits", ColumnType::LongLong),
            mk_col("Semi_Sync_Degraded_Commits", ColumnType::LongLong),
            mk_col("Writes_Frozen", ColumnType::VarString),
        ];
        let (listener_running, connected_senders) = {
            let g = self.master_service.lock();
            match g.as_ref() {
                Some(svc) => ("yes", svc.tracker.snapshot().count),
                None => ("no", 0),
            }
        };
        let row = vec![
            Value::String(state.node_uuid),
            Value::String(state.cluster_uuid),
            Value::String(state.timeline_uuid),
            Value::String(state.role.as_str().to_string()),
            Value::UInt(state.epoch),
            Value::UInt(current_lsn),
            Value::UInt(durable_lsn),
            Value::UInt(min_available_lsn),
            Value::UInt(active_segment_id),
            Value::UInt(segment_count),
            Value::UInt(wal_bytes),
            Value::UInt(active_slots),
            Value::UInt(
                slot_snap
                    .iter()
                    .filter(|s| s.status == SlotStatus::Invalidated)
                    .count() as u64,
            ),
            Value::UInt(
                slot_snap
                    .iter()
                    .filter(|s| s.status == SlotStatus::Abandoned)
                    .count() as u64,
            ),
            min_slot_lsn,
            Value::String(self.config.mode.as_str().to_string()),
            Value::UInt(self.config.bind_port as u64),
            Value::String(listener_running.to_string()),
            Value::UInt(connected_senders),
            Value::UInt(self.semi_sync_acked_commits.load(Ordering::Relaxed)),
            Value::UInt(self.semi_sync_timeout_commits.load(Ordering::Relaxed)),
            Value::UInt(self.semi_sync_degraded_commits.load(Ordering::Relaxed)),
            Value::String(
                if self.master_writes_frozen.load(Ordering::Acquire) {
                    "yes"
                } else {
                    "no"
                }
                .to_string(),
            ),
        ];
        Ok(QueryResponse::ResultSet(ResultSet {
            columns,
            rows: vec![row],
        }))
    }

    fn show_replica_status(&self) -> Result<QueryResponse, QueryError> {
        let state = self.role_snapshot();
        let disk_progress = replica::load_progress(&self.data_dir)
            .map_err(|err| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("mydb-repl: load progress: {err}"),
                )
            })?
            .unwrap_or_default();
        let progress = if self.receiver_lifecycle.lock().is_running() {
            self.receiver_progress.lock().clone()
        } else {
            disk_progress
        };
        let log = self.engine.database_for_replication().wal_log();
        let local_next_lsn = log.lock().next_lsn();
        let columns = vec![
            mk_col("Node_UUID", ColumnType::VarString),
            mk_col("Role", ColumnType::VarString),
            mk_col("Epoch", ColumnType::LongLong),
            mk_col("Master_Endpoint", ColumnType::VarString),
            mk_col("Last_Master_UUID", ColumnType::VarString),
            mk_col("Last_Master_Epoch", ColumnType::LongLong),
            mk_col("Received_LSN", ColumnType::LongLong),
            mk_col("Durable_LSN", ColumnType::LongLong),
            mk_col("Applied_LSN", ColumnType::LongLong),
            mk_col("Durable_Applied_LSN", ColumnType::LongLong),
            mk_col("Local_Next_LSN", ColumnType::LongLong),
            mk_col("Receive_Apply_Gap_LSN", ColumnType::LongLong),
            mk_col("Last_Update_Ms", ColumnType::LongLong),
            mk_col("Receiver_Running", ColumnType::VarString),
            mk_col("Resync_Required", ColumnType::VarString),
            mk_col("Last_Status", ColumnType::VarString),
        ];
        let lag = progress
            .received_lsn
            .saturating_sub(progress.durable_applied_lsn);
        let row = vec![
            Value::String(state.node_uuid),
            Value::String(state.role.as_str().to_string()),
            Value::UInt(state.epoch),
            state
                .master_endpoint
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
            if progress.last_master_uuid.is_empty() {
                Value::Null
            } else {
                Value::String(progress.last_master_uuid.clone())
            },
            Value::UInt(progress.last_master_epoch),
            Value::UInt(progress.received_lsn),
            Value::UInt(progress.durable_lsn),
            Value::UInt(progress.applied_lsn),
            Value::UInt(progress.durable_applied_lsn),
            Value::UInt(local_next_lsn),
            Value::UInt(lag),
            Value::UInt(progress.last_update_ms),
            Value::String(
                if self.receiver_lifecycle.lock().is_running() {
                    "yes"
                } else {
                    "no"
                }
                .to_string(),
            ),
            replica::progress::resync_marker_reason(&self.data_dir)
                .map(Value::String)
                .unwrap_or(Value::Null),
            {
                let s = self.receiver_status.lock().clone();
                if s.is_empty() {
                    Value::Null
                } else {
                    Value::String(s)
                }
            },
        ];
        Ok(QueryResponse::ResultSet(ResultSet {
            columns,
            rows: vec![row],
        }))
    }

    fn show_replicas(&self) -> Result<QueryResponse, QueryError> {
        let current = self
            .engine
            .database_for_replication()
            .wal_log()
            .lock()
            .next_lsn()
            .saturating_sub(1);
        let rows = self
            .slots
            .lock()
            .snapshot()
            .into_iter()
            .map(|slot| {
                vec![
                    Value::String(slot.replica_uuid),
                    Value::String(if slot.connected { "yes" } else { "no" }.to_string()),
                    Value::String(slot.status.as_str().to_string()),
                    Value::UInt(slot.acked_lsn),
                    Value::UInt(current.saturating_sub(slot.acked_lsn)),
                    Value::UInt(slot.last_seen_ms),
                    Value::UInt(slot.first_registered_ms),
                ]
            })
            .collect();
        Ok(QueryResponse::ResultSet(ResultSet {
            columns: vec![
                mk_col("Replica_UUID", ColumnType::VarString),
                mk_col("Connected", ColumnType::VarString),
                mk_col("Retention_State", ColumnType::VarString),
                mk_col("Replica_Acked_LSN", ColumnType::LongLong),
                mk_col("Lag_LSN", ColumnType::LongLong),
                mk_col("Last_Seen_Ms", ColumnType::LongLong),
                mk_col("First_Registered_Ms", ColumnType::LongLong),
            ],
            rows,
        }))
    }

    fn handle_change_master(&self, sql: &str) -> Result<QueryResponse, QueryError> {
        // Serialize with START through role/endpoint persistence.
        let lifecycle = self.receiver_lifecycle.lock();
        if lifecycle.is_running() {
            return Err(error(
                crate::error::catalog::ER_REPLICA_CHANNEL_MUST_STOP,
                "CHANGE MYDB MASTER TO requires STOP MYDB REPLICATION first",
            ));
        }
        let opts = parse_change_master(sql)?;
        let endpoint = match (opts.host, opts.port) {
            (Some(h), Some(p)) => Some(format!("{h}:{p}")),
            (Some(h), None) => Some(format!("{h}:{}", self.config.master_port)),
            (None, Some(_)) => {
                return Err(error(
                    crate::error::catalog::ER_PARSE_ERROR,
                    "CHANGE MYDB MASTER TO: PORT requires HOST",
                ))
            }
            (None, None) => {
                return Err(error(
                    crate::error::catalog::ER_PARSE_ERROR,
                    "CHANGE MYDB MASTER TO: HOST is required",
                ))
            }
        };
        let mut state = self.role_state.lock().unwrap();
        if state.role == Role::Master {
            return Err(error(
                crate::error::catalog::ER_ILLEGAL_HA,
                "CHANGE MYDB MASTER TO on a master requires DEMOTE TO REPLICA",
            ));
        }
        let mut next = state.clone();
        if next.role == Role::Standalone || next.master_endpoint != endpoint {
            // A standalone may have accepted local writes. Its existing pages
            // cannot safely be continued with another source's physical WAL.
            replica::progress::write_resync_marker(
                &self.data_dir,
                "master configuration changed; base backup required",
            )
            .map_err(|err| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("persist resync marker: {err}"),
                )
            })?;
            next.role = Role::Replica;
            next.epoch = next.epoch.saturating_add(1);
        }
        next.master_endpoint = endpoint;
        role::save(&self.data_dir, &next).map_err(|err| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("mydb-repl: failed to persist role.json: {err}"),
            )
        })?;
        *state = next;
        self.engine
            .database_for_replication()
            .set_physical_replica_mode(true)?;
        Ok(QueryResponse::Ok)
    }

    fn handle_force_resync(
        &self,
        session: &crate::engine::SessionContext,
    ) -> Result<QueryResponse, QueryError> {
        let state = self.role_state.lock().unwrap();
        if state.role != Role::Replica || state.master_endpoint.is_none() {
            return Err(error(
                crate::error::catalog::MYDB_ER_REPLICATION_STATE,
                "FORCE_RESYNC requires role=replica and a configured master",
            ));
        }
        if !crate::mysql::shutdown::restart_supported() {
            return Err(error(crate::error::catalog::ER_RESTART_SERVER_FAILED,
                "FORCE_RESYNC requires in-process RESTART support; enable RESTART or restart manually"));
        }
        replica::progress::write_resync_marker(&self.data_dir, "explicit FORCE_RESYNC requested")
            .map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("persist resync marker: {err}"),
            )
        })?;
        crate::mysql::shutdown::use_persisted_role_on_restart();
        crate::mysql::shutdown::request_shutdown(
            crate::mysql::shutdown::ShutdownSource::Restart {
                conn_id: session.connection_id,
                user: session.user.clone(),
            },
            crate::mysql::shutdown::ShutdownIntent::Restart,
        );
        Ok(QueryResponse::Ok)
    }

    fn handle_start_replication(&self) -> Result<QueryResponse, QueryError> {
        self.start_receiver_if_needed()?;
        Ok(QueryResponse::Ok)
    }

    fn handle_stop_replication(&self) -> Result<QueryResponse, QueryError> {
        self.stop_receiver_if_running()?;
        Ok(QueryResponse::Ok)
    }

    /// Spawn the receiver loop if one isn't already running. Public so
    /// the boot-time auto-start path and the (future) PROMOTE/DEMOTE
    /// flow can share the wiring.
    pub fn start_receiver_if_needed(&self) -> Result<(), QueryError> {
        let runtime_handle = self.runtime_handle.lock().clone().ok_or_else(|| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "START MYDB REPLICATION: no tokio runtime handle was captured at init_global",
            )
        })?;
        let mut lifecycle = self.receiver_lifecycle.lock();
        if lifecycle.is_running() {
            return Ok(());
        }
        let role_state = self.role_state.lock().unwrap().clone();
        if role_state.role != Role::Replica {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "START MYDB REPLICATION: this node is not in role=replica",
            ));
        }
        if let Some(reason) = replica::progress::resync_marker_reason(&self.data_dir) {
            return Err(error(
                crate::error::catalog::MYDB_ER_REPLICATION_STATE,
                format!(
                    "START MYDB REPLICATION refused: resync required ({reason}); run START MYDB REPLICATION FORCE_RESYNC (disconnects sessions for a controlled restart)"
                ),
            ));
        }
        let master_endpoint = role_state.master_endpoint.clone().ok_or_else(|| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "START MYDB REPLICATION: no master_endpoint configured (CHANGE MYDB MASTER TO ...)",
            )
        })?;
        let master_host = master_endpoint
            .rsplit_once(':')
            .map(|(host, _)| host.trim_matches(['[', ']']))
            .ok_or_else(|| {
                error(
                    crate::error::catalog::MYDB_ER_REPLICATION_STATE,
                    format!("invalid master endpoint '{master_endpoint}'"),
                )
            })?;
        crate::repl::tls::validate_endpoint(&self.config, master_host).map_err(|err| {
            error(
                crate::error::catalog::MYDB_ER_REPLICATION_SECURITY,
                format!("native replication transport configuration: {err}"),
            )
        })?;
        let db = self.engine.database_for_replication().clone();
        let log = db.wal_log();
        let mut initial_progress = replica::load_progress(&self.data_dir)
            .map_err(|err| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("load replica progress: {err}"),
                )
            })?
            .unwrap_or_default();
        // Startup recovery is the one authority that may reconcile a
        // throttled progress file with the durable local WAL: it has actually
        // redone the frames into Database state. Never perform this inference
        // inside Receiver::run_once after an online apply failure.
        let recovered_applied = db.replica_applied_lsn();
        let recovered_durable = recovered_applied.min(log.lock().durable_lsn());
        if !initial_progress.last_master_uuid.is_empty()
            && recovered_durable > initial_progress.durable_applied_lsn
        {
            initial_progress.received_lsn = initial_progress.received_lsn.max(recovered_durable);
            initial_progress.durable_lsn = initial_progress.durable_lsn.max(recovered_durable);
            initial_progress.applied_lsn = initial_progress.applied_lsn.max(recovered_applied);
            initial_progress.durable_applied_lsn = recovered_durable;
            initial_progress.last_update_ms = now_ms();
            replica::progress::save_progress(&self.data_dir, &initial_progress).map_err(|err| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("persist recovered replica progress: {err}"),
                )
            })?;
        }
        *self.receiver_progress.lock() = initial_progress.clone();
        lifecycle.start(
            runtime_handle,
            db,
            log,
            self.data_dir.clone(),
            master_endpoint,
            role_state.node_uuid.clone(),
            self.config.clone(),
            initial_progress,
            Arc::clone(&self.receiver_status),
            Arc::clone(&self.receiver_progress),
            Arc::clone(&self.role_state),
        )
    }

    /// Bind the master-side WAL listener if it isn't already running.
    /// Used by boot-time auto-start (role=master) and PROMOTE.
    pub fn start_master_service_if_needed(&self) -> Result<(), QueryError> {
        let runtime_handle = self.runtime_handle.lock().clone().ok_or_else(|| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "mydb-repl: no tokio runtime handle was captured at init_global",
            )
        })?;
        let mut service_guard = self.master_service.lock();
        if service_guard.is_some() {
            return Ok(());
        }
        let state = self.role_snapshot();
        let db = self.engine.database_for_replication().clone();
        crate::repl::tls::validate_endpoint(&self.config, &self.config.bind_host).map_err(
            |err| {
                error(
                    crate::error::catalog::MYDB_ER_REPLICATION_SECURITY,
                    format!("native replication transport configuration: {err}"),
                )
            },
        )?;
        let init = master::MasterServiceInit {
            bind_host: self.config.bind_host.clone(),
            bind_port: self.config.bind_port,
            config: self.config.clone(),
            master_uuid: role::uuid_string_to_bytes(&state.node_uuid).unwrap_or([0u8; 16]),
            epoch: state.epoch,
            server_version: format!("mydb/{}", env!("CARGO_PKG_VERSION")),
            log: db.wal_log(),
            slots: Arc::clone(&self.slots),
            data_dir: Some(self.data_dir.clone()),
            db: Some(db),
            role_state: Some(Arc::clone(&self.role_state)),
        };
        let bind = format!("{}:{}", init.bind_host, init.bind_port);
        let std_listener = std::net::TcpListener::bind(&bind).map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("mydb-repl bind {bind}: {err}"),
            )
        })?;
        std_listener.set_nonblocking(true).map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("mydb-repl set_nonblocking {bind}: {err}"),
            )
        })?;
        let _runtime = runtime_handle.enter();
        let listener = tokio::net::TcpListener::from_std(std_listener).map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("mydb-repl adopt listener {bind}: {err}"),
            )
        })?;
        let svc = master::start_bound_master_service(init, listener)?;
        crate::util::logging::log_message(format_args!(
            "mydb-repl master: WAL listener bound on {}",
            svc.bind_addr()
        ));
        *service_guard = Some(svc);
        Ok(())
    }

    /// Stop the master-side WAL listener if running (DEMOTE path).
    pub fn stop_master_service_if_running(&self) -> Result<(), QueryError> {
        if let Some(svc) = self.master_service.lock().take() {
            if let Some(handle) = self.runtime_handle.lock().clone() {
                if Handle::try_current().is_ok() {
                    handle.spawn(async move { svc.shutdown().await });
                } else {
                    let (tx, rx) = std::sync::mpsc::sync_channel(1);
                    handle.spawn(async move {
                        svc.shutdown().await;
                        let _ = tx.send(());
                    });
                    rx.recv_timeout(std::time::Duration::from_secs(11))
                        .map_err(|_| {
                            error(
                                crate::error::catalog::ER_STOP_REPLICA_IO_THREAD_TIMEOUT,
                                "timed out stopping mydb replication senders",
                            )
                        })?;
                }
            }
        }
        Ok(())
    }

    /// Notify the receiver loop's shutdown signal and wait briefly
    /// for the task to finish.
    pub fn stop_receiver_if_running(&self) -> Result<(), QueryError> {
        let runtime_handle = self.runtime_handle.lock().clone().ok_or_else(|| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "STOP MYDB REPLICATION: no tokio runtime handle was captured",
            )
        })?;
        let mut lifecycle = self.receiver_lifecycle.lock();
        lifecycle.stop(runtime_handle)
    }

    /// Quiesce every native-replication task before the storage engine's final
    /// checkpoint. Unlike the SQL STOP wrappers this is async and deterministic:
    /// no receiver/applier or sender may retain a Database handle on return.
    pub async fn shutdown_all(&self) {
        let receiver = {
            let mut guard = self.receiver_lifecycle.lock();
            std::mem::replace(&mut *guard, ReceiverLifecycle::new())
        };
        let mut receiver = receiver;
        receiver.stop_and_wait().await;

        let service = self.master_service.lock().take();
        if let Some(service) = service {
            service.shutdown().await;
        }
    }

    #[cfg(test)]
    pub(crate) fn is_receiver_running(&self) -> bool {
        self.receiver_lifecycle.lock().is_running()
    }

    /// Test-only: explicitly install a runtime handle. Tests build the
    /// manager outside a `#[tokio::main]` block so `Handle::try_current`
    /// returns `None`; they call this from inside their `#[tokio::test]`
    /// after constructing the manager.
    #[cfg(test)]
    pub(crate) fn install_runtime_handle_for_tests(&self, handle: Handle) {
        *self.runtime_handle.lock() = Some(handle);
    }

    /// Deterministic STOP for tests running on the tokio runtime.
    /// Notifies the receiver's shutdown signal and awaits the task's
    /// join so the test can confidently assert "no further frames will
    /// be applied past this point". The sync `handle_stop_replication`
    /// path can't `.await` here so it detaches instead.
    #[cfg(test)]
    pub(crate) async fn stop_receiver_and_wait(&self) {
        let snapshot = {
            let mut g = self.receiver_lifecycle.lock();
            (
                g.is_running(),
                std::mem::replace(&mut *g, ReceiverLifecycle::new()),
            )
        };
        let (was_running, mut lifecycle) = snapshot;
        if !was_running {
            return;
        }
        lifecycle.stop_and_wait().await;
    }
    fn handle_reset_replication(&self) -> Result<QueryResponse, QueryError> {
        // Clear the replica's progress file + force role back to
        // standalone. Useful for "wipe and start over" after manual
        // base backup.
        if self.role() == Role::Master {
            return Err(error(
                crate::error::catalog::ER_ILLEGAL_HA,
                "RESET MYDB REPLICATION is not valid while role=master",
            ));
        }
        self.stop_receiver_if_running()?;
        let path = replica::progress::progress_path(&self.data_dir);
        if let Err(err) = std::fs::remove_file(&path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("RESET MYDB REPLICATION: remove {}: {err}", path.display()),
                ));
            }
        }
        replica::progress::clear_resync_marker(&self.data_dir);
        let mut state = self.role_state.lock().unwrap();
        state.role = Role::Standalone;
        state.master_endpoint = None;
        state.epoch = state.epoch.saturating_add(1);
        role::save(&self.data_dir, &state).map_err(|err| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("mydb-repl: persist role.json: {err}"),
            )
        })?;
        self.engine
            .database_for_replication()
            .set_physical_replica_mode(false)?;
        Ok(QueryResponse::Ok)
    }
    fn handle_change_sequence_sync(&self, sql: &str) -> Result<QueryResponse, QueryError> {
        if self.role() != Role::Master
            || !self.master_writes_frozen.load(Ordering::Acquire)
            || self.role_snapshot().promotion_pending
        {
            return Err(error(crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED,
                "sequence sync configuration requires a completed STOP MASTER WRITES on a native master"));
        }
        let db = self.engine.database_for_replication();
        if db.has_active_backup_conflicts() {
            return Err(error(
                crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED,
                "sequence sync change requires all writers to drain",
            ));
        }
        let body = sql
            .get("CHANGE MYDB SEQUENCE SYNC TO".len()..)
            .unwrap_or("")
            .trim();
        if !body.starts_with("'") || !body.ends_with("'") || body.len() < 2 {
            return Err(error(
                crate::error::catalog::ER_PARSE_ERROR,
                "sequence sync configuration must be a quoted JSON object",
            ));
        }
        let config: crate::storage::sequence::SequenceSyncConfig =
            serde_json::from_str(&unquote(body)).map_err(|e| {
                error(
                    crate::error::catalog::ER_PARSE_ERROR,
                    format!("invalid sequence sync configuration: {e}"),
                )
            })?;
        config.validate()?;
        let previous = db.sequence_sync_config();
        if config.epoch
            != previous.epoch.checked_add(1).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "sequence sync epoch exhausted",
                )
            })?
        {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                "sequence sync configuration requires exactly the next epoch",
            ));
        }
        if config.replicas.contains(&self.role_snapshot().node_uuid) {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                "sequence sync candidates must be remote nodes",
            ));
        }
        if config.remote_apply {
            let target = db.wal_log().lock().durable_lsn();
            wait_for_sequence_durable(&config.replicas, target)?;
        }
        let target = db.store_sequence_sync_config(config.clone())?;
        if config.remote_apply {
            wait_for_sequence_durable(&config.replicas, target)?;
        }
        Ok(QueryResponse::Ok)
    }

    fn handle_promote(
        &self,
        sql: &str,
        session: &crate::engine::SessionContext,
    ) -> Result<QueryResponse, QueryError> {
        let (sql, expected_sequence_epoch) = split_sequence_promotion_epoch(sql)?;
        let mode = parse_promote(sql)?;
        let role = self.role();
        if role == Role::Master {
            let config = self
                .engine
                .database_for_replication()
                .sequence_sync_config();
            validate_sequence_promotion_epoch(&config, expected_sequence_epoch)?;
            self.finish_pending_promotion()?;
            // Idempotent, but make sure the listener is up.
            let _ = self.start_master_service_if_needed();
            return Ok(QueryResponse::Ok);
        }
        let promote_lsn = match (role, mode) {
            (Role::Replica, PromoteMode::Bare) => {
                return Err(error(
                    crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED,
                    "replica promotion requires AT LSN ... FENCE_TOKEN=... WAIT ... or FORCE FENCED",
                ));
            }
            (Role::Replica, PromoteMode::Planned { lsn, token, wait }) => {
                let deadline = std::time::Instant::now() + wait;
                loop {
                    session.check_cancelled()?;
                    let progress = self.receiver_progress.lock().clone();
                    let fresh = now_ms().saturating_sub(progress.last_update_ms)
                        <= self.config.heartbeat.saturating_mul(6).as_millis() as u64;
                    let token_matches =
                        role::fence_token_hash(&token) == progress.source_fence_token_hash;
                    if progress.source_frozen
                        && progress.source_fence_lsn == lsn
                        && token_matches
                        && fresh
                        && progress.durable_applied_lsn >= lsn
                    {
                        break;
                    }
                    if std::time::Instant::now() >= deadline {
                        return Err(error(
                            crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED,
                            format!(
                                "planned promotion preconditions not met: target_lsn={lsn}, durable_applied={}, source_frozen={}, fence_lsn={}, token_match={}, heartbeat_fresh={fresh}, pending_replicated_undo={}",
                                progress.durable_applied_lsn,
                                progress.source_frozen,
                                progress.source_fence_lsn,
                                token_matches,
                                self.engine.database_for_replication().has_pending_replicated_undo()
                            ),
                        ));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                lsn
            }
            (Role::Replica, PromoteMode::ForceFenced) => {
                let lsn = self.receiver_progress.lock().durable_applied_lsn;
                crate::util::logging::log_message(format_args!(
                    "mydb-repl FORCE FENCED promotion at durable_applied_lsn={lsn}; data loss beyond this point is possible"
                ));
                lsn
            }
            (Role::Standalone, PromoteMode::Bare) => self
                .engine
                .database_for_replication()
                .wal_log()
                .lock()
                .durable_lsn(),
            (Role::Standalone, _) => {
                return Err(error(
                    crate::error::catalog::MYDB_ER_REPLICATION_STATE,
                    "standalone promotion accepts only bare PROMOTE TO MASTER",
                ));
            }
            (Role::Master, _) => unreachable!("master returned above"),
        };
        // A role change is an MVCC boundary. Refuse it while a local snapshot
        // is active rather than letting a reader span source-stream shutdown,
        // abandoned-transaction rollback and the new writable epoch.
        let snapshot_count = self
            .engine
            .database_for_replication()
            .active_snapshot_count();
        if snapshot_count > 0 {
            return Err(error(crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED,
                format!(
                    "PROMOTE TO MASTER refused: {snapshot_count} active read snapshot(s) \
                     would lose access to undo history at the role flip. \
                     Close pre-promote client connections (or wait for their reads to finish) and retry."
                ),
            ));
        }
        // Stop consuming the old master's stream first.
        self.stop_receiver_if_running()?;
        let db = self.engine.database_for_replication();
        // Complete every validated received frame before fixing the fork.
        // A frame that still cannot be applied blocks promotion explicitly.
        let tail = db.wal_log().lock().next_lsn().saturating_sub(1);
        crate::wal::LogManager::sync_durable_to(&db.wal_log(), tail)?;
        if role == Role::Replica && tail > db.replica_applied_lsn() {
            let scan = crate::wal::LogManager::scan(&self.data_dir)?;
            let mut context = crate::storage::OnlineWalApplyContext::default();
            for record in &scan.records {
                if record.lsn > db.replica_applied_lsn() {
                    db.apply_wal_record_with_context(record, &mut context)?;
                }
            }
        }
        let config = db.sequence_sync_config();
        validate_sequence_promotion_epoch(&config, expected_sequence_epoch)?;
        if config.remote_apply && !config.replicas.contains(&self.role_snapshot().node_uuid) {
            return Err(error(
                crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED,
                "promotion candidate is not in the persisted sequence sync configuration",
            ));
        }
        let promote_lsn = promote_lsn.max(db.replica_applied_lsn());
        self.master_writes_frozen.store(true, Ordering::SeqCst);
        {
            let mut state = self.role_state.lock().unwrap();
            state.promotion_pending = true;
            state.promotion_target_lsn = Some(promote_lsn);
            state.write_fence = Some(role::WriteFence {
                token: role::generate_uuid_string(),
                fence_lsn: promote_lsn,
                created_at_ms: now_ms(),
            });
            role::save(&self.data_dir, &state).map_err(|e| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("persist promotion intent: {e}"),
                )
            })?;
        }
        self.finish_pending_promotion()?;
        self.start_master_service_if_needed()?;
        // M8 — eager-warm HNSW graphs on the freshly-promoted master so the
        // first KNN after failover doesn't pay a cold-build stall. The graph is
        // derived data (heap-as-truth), so this is best-effort: a failure just
        // falls back to the lazy first-query build. Done after the role flip so
        // it builds from the now-stable post-promote catalog/heap.
        self.engine
            .database_for_replication()
            .warm_all_vector_graphs()?;
        Ok(QueryResponse::Ok)
    }
    fn finish_pending_promotion(&self) -> Result<(), QueryError> {
        let before = self.role_snapshot();
        if !before.promotion_pending {
            return Ok(());
        }
        self.master_writes_frozen.store(true, Ordering::SeqCst);
        let target = before.promotion_target_lsn.ok_or_else(|| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "promotion intent has no target LSN",
            )
        })?;
        let db = self.engine.database_for_replication();
        if before.role != Role::Master {
            self.reserve_former_source_slot(target)?;
            db.persist_replicated_undo_for_promotion()?;
            db.rollback_all_active_replication_txns()?;
            db.restore_replicated_xa_for_promotion()?;
            db.prepare_sequence_promotion()?;
            db.checkpoint()?;
            let mut state = self.role_state.lock().unwrap();
            state.promote_to_master_at(target);
            state.promotion_pending = true;
            state.promotion_target_lsn = Some(target);
            state.write_fence = before.write_fence.clone();
            role::save(&self.data_dir, &state).map_err(|e| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("persist promotion fork: {e}"),
                )
            })?;
        }
        // Local catalog materialization may allocate LSNs only after the fork
        // has a durable owner, and before any client can write on that owner.
        db.set_physical_replica_mode(false)?;
        db.prepare_sequence_promotion()?;
        db.warm_all_vector_graphs()?;
        db.checkpoint()?;
        {
            let mut state = self.role_state.lock().unwrap();
            state.promotion_pending = false;
            state.promotion_target_lsn = None;
            state.write_fence = None;
            role::save(&self.data_dir, &state).map_err(|e| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("finish promotion: {e}"),
                )
            })?;
        }
        self.master_writes_frozen.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// After a switchover the former source is demoted and resumes from
    /// `fork_lsn + 1` on the new timeline. The promotion checkpoints would
    /// otherwise collapse that WAL suffix before any slot exists, forcing a
    /// full re-seed of a node that never diverged.
    fn reserve_former_source_slot(&self, fork_lsn: u64) -> Result<(), QueryError> {
        let former_source = self.receiver_progress.lock().last_master_uuid.clone();
        if former_source.is_empty() || former_source == self.role_snapshot().node_uuid {
            return Ok(());
        }
        let mut slots = self.slots.lock();
        slots.reserve_disconnected(&former_source, fork_lsn, now_ms());
        slots.persist(&former_source)
    }

    fn handle_demote(&self, sql: &str) -> Result<QueryResponse, QueryError> {
        let opts = parse_demote(sql)?;
        let endpoint = format!("{}:{}", opts.host, opts.port);
        let before = self.role_snapshot();
        if before.role != Role::Master || before.write_fence.is_none() {
            return Err(error(
                crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED,
                "DEMOTE TO REPLICA requires a frozen master (STOP MASTER WRITES first)",
            ));
        }
        let db = self.engine.database_for_replication();
        let durable_lsn = db.wal_log().lock().durable_lsn();
        let fence_lsn = before
            .write_fence
            .as_ref()
            .map(|f| f.fence_lsn)
            .unwrap_or(0);
        if durable_lsn > fence_lsn {
            return Err(error(
                crate::error::catalog::MYDB_ER_REPLICATION_DIVERGED,
                format!(
                    "cannot demote: durable LSN {durable_lsn} advanced past fence LSN {fence_lsn}"
                ),
            ));
        }
        // Freeze writes immediately so nothing new lands between here and
        // the role flip (C7). Symmetric with promote's write handling;
        // once role becomes Replica the frozen flag is moot (the replica
        // read-only gate dominates), and a later promote clears it.
        self.master_writes_frozen.store(true, Ordering::SeqCst);
        self.stop_master_service_if_running()?;
        {
            let mut state = self.role_state.lock().unwrap();
            state.demote_to_replica(endpoint);
            role::save(&self.data_dir, &state).map_err(|err| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("mydb-repl: persist role.json: {err}"),
                )
            })?;
        }
        self.engine
            .database_for_replication()
            .set_physical_replica_mode(true)?;
        let progress = ReplicaProgress {
            received_lsn: durable_lsn,
            durable_lsn,
            applied_lsn: durable_lsn,
            durable_applied_lsn: durable_lsn,
            last_master_uuid: String::new(),
            last_master_epoch: 0,
            cluster_uuid: before.cluster_uuid,
            timeline_uuid: before.timeline_uuid,
            source_current_lsn: durable_lsn,
            source_durable_lsn: durable_lsn,
            source_generation: before.generation,
            source_frozen: false,
            source_fence_lsn: 0,
            source_fence_token_hash: String::new(),
            last_update_ms: now_ms(),
        };
        replica::progress::save_progress(&self.data_dir, &progress).map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("DEMOTE TO REPLICA: save follow progress: {err}"),
            )
        })?;
        *self.receiver_progress.lock() = progress;
        replica::progress::clear_resync_marker(&self.data_dir);
        self.start_receiver_if_needed()?;
        Ok(QueryResponse::Ok)
    }
    fn handle_stop_master_writes(
        &self,
        sql: &str,
        session: &crate::engine::SessionContext,
    ) -> Result<QueryResponse, QueryError> {
        if self.role() != Role::Master {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "STOP MASTER WRITES is only valid on a master",
            ));
        }
        let wait = parse_stop_master_writes(sql)?;
        let token = role::generate_uuid_string();
        let provisional_lsn = self
            .engine
            .database_for_replication()
            .wal_log()
            .lock()
            .next_lsn()
            .saturating_sub(1);
        self.master_writes_frozen.store(true, Ordering::SeqCst);
        {
            let mut state = self.role_state.lock().unwrap();
            state.write_fence = Some(role::WriteFence {
                token: token.clone(),
                fence_lsn: provisional_lsn,
                created_at_ms: now_ms(),
            });
            if let Err(err) = role::save(&self.data_dir, &state) {
                self.master_writes_frozen.store(false, Ordering::SeqCst);
                state.write_fence = None;
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("STOP MASTER WRITES: persist fence: {err}"),
                ));
            }
        }
        let deadline = std::time::Instant::now() + wait;
        let db = self.engine.database_for_replication();
        while db.has_active_backup_conflicts() {
            session.check_cancelled()?;
            if std::time::Instant::now() >= deadline {
                return Err(error(
                    crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED,
                    "STOP MASTER WRITES timed out draining active writers; master remains frozen",
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        db.checkpoint()?;
        let fence_lsn = db.wal_log().lock().next_lsn().saturating_sub(1);
        let timeline = {
            let mut state = self.role_state.lock().unwrap();
            if let Some(fence) = state.write_fence.as_mut() {
                fence.fence_lsn = fence_lsn;
            }
            let timeline = state.timeline_uuid.clone();
            role::save(&self.data_dir, &state).map_err(|err| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("STOP MASTER WRITES: persist fence LSN: {err}"),
                )
            })?;
            timeline
        };
        Ok(QueryResponse::ResultSet(ResultSet {
            columns: vec![
                mk_col("Fence_LSN", ColumnType::LongLong),
                mk_col("Timeline_ID", ColumnType::VarString),
                mk_col("Fence_Token", ColumnType::VarString),
            ],
            rows: vec![vec![
                Value::UInt(fence_lsn),
                Value::String(timeline),
                Value::String(token),
            ]],
        }))
    }
    fn handle_resume_master_writes(&self) -> Result<QueryResponse, QueryError> {
        if self.role_snapshot().promotion_pending {
            return Err(error(
                crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED,
                "complete the pending promotion before resuming writes",
            ));
        }
        let db = self.engine.database_for_replication();
        let sync = db.sequence_sync_config();
        if sync.remote_apply {
            let target = db.wal_log().lock().durable_lsn();
            wait_for_sequence_durable(&sync.replicas, target)?;
        }
        if self.role() != Role::Master {
            return Err(error(
                crate::error::catalog::ER_ILLEGAL_HA,
                "RESUME MASTER WRITES is only valid on a master",
            ));
        }
        {
            let mut state = self.role_state.lock().unwrap();
            state.write_fence = None;
            state.generation = state.generation.saturating_add(1);
            role::save(&self.data_dir, &state).map_err(|err| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("RESUME MASTER WRITES: persist fence removal: {err}"),
                )
            })?;
        }
        self.master_writes_frozen.store(false, Ordering::SeqCst);
        Ok(QueryResponse::Ok)
    }
    fn handle_drop_replica(&self, sql: &str) -> Result<QueryResponse, QueryError> {
        let uuid = parse_drop_replica(sql)?;
        let mut g = self.slots.lock();
        if g.get(&uuid)
            .map(|slot| slot.snapshot().connected)
            .unwrap_or(false)
        {
            return Err(error(
                crate::error::catalog::ER_REPLICA_CHANNEL_MUST_STOP,
                format!("DROP MYDB REPLICA: replica '{uuid}' is connected"),
            ));
        }
        let existed = g.drop_slot(&uuid)?;
        if !existed {
            return Err(error(
                crate::error::catalog::ER_ILLEGAL_HA,
                format!("DROP MYDB REPLICA: no slot for '{uuid}'"),
            ));
        }
        Ok(QueryResponse::Ok)
    }
}

fn mk_col(name: &str, column_type: ColumnType) -> Column {
    Column {
        source: Default::default(),
        name: name.to_string(),
        column_type,
    }
}

#[derive(Debug, Default)]
struct ChangeMasterOpts {
    host: Option<String>,
    port: Option<u16>,
}

fn parse_change_master(sql: &str) -> Result<ChangeMasterOpts, QueryError> {
    // Strip the leading `CHANGE MYDB MASTER TO` prefix.
    let upper = sql.to_ascii_uppercase();
    let pos = upper.find("CHANGE MYDB MASTER TO").ok_or_else(|| {
        error(
            crate::error::catalog::ER_PARSE_ERROR,
            "expected CHANGE MYDB MASTER TO",
        )
    })?;
    let body = sql[pos + "CHANGE MYDB MASTER TO".len()..].trim();
    let mut opts = ChangeMasterOpts::default();
    for raw in split_top_level_commas(body) {
        let (key, value) = raw.split_once('=').ok_or_else(|| {
            error(
                crate::error::catalog::ER_PARSE_ERROR,
                format!("bad CHANGE MYDB MASTER TO clause: '{raw}'"),
            )
        })?;
        let value = unquote(value.trim());
        match key.trim().to_ascii_uppercase().as_str() {
            "HOST" => opts.host = Some(value),
            "PORT" => {
                opts.port = Some(value.parse().map_err(|_| {
                    error(
                        crate::error::catalog::ER_PARSE_ERROR,
                        format!("invalid PORT '{value}'"),
                    )
                })?)
            }
            other => {
                return Err(error(
                    crate::error::catalog::ER_PARSE_ERROR,
                    format!("unsupported CHANGE MYDB MASTER TO option '{other}'"),
                ))
            }
        }
    }
    Ok(opts)
}

#[derive(Debug)]
struct DemoteOpts {
    host: String,
    port: u16,
}

#[derive(Debug)]
enum PromoteMode {
    Bare,
    Planned {
        lsn: u64,
        token: String,
        wait: std::time::Duration,
    },
    ForceFenced,
}

fn split_sequence_promotion_epoch(sql: &str) -> Result<(&str, Option<u64>), QueryError> {
    const PREFIX: &str = "SEQUENCE_SYNC_EPOCH=";
    let sql = sql.trim();
    let token = sql.split_whitespace().last().unwrap_or("");
    if !token
        .get(..PREFIX.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(PREFIX))
    {
        return Ok((sql, None));
    }
    let epoch = token[PREFIX.len()..]
        .parse::<u64>()
        .ok()
        .filter(|value| *value != 0)
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_PARSE_ERROR,
                "SEQUENCE_SYNC_EPOCH requires a positive integer",
            )
        })?;
    Ok((sql[..sql.len() - token.len()].trim_end(), Some(epoch)))
}

fn validate_sequence_promotion_epoch(
    config: &crate::storage::sequence::SequenceSyncConfig,
    expected: Option<u64>,
) -> Result<(), QueryError> {
    if (config.remote_apply || expected.is_some()) && expected != Some(config.epoch) {
        return Err(error(crate::error::catalog::MYDB_ER_ROLE_CHANGE_BLOCKED,
            format!("promotion requires the operator's current SEQUENCE_SYNC_EPOCH={}, received {expected:?}", config.epoch)));
    }
    Ok(())
}

fn parse_promote(sql: &str) -> Result<PromoteMode, QueryError> {
    let body = sql["PROMOTE TO MASTER".len()..].trim();
    if body.is_empty() {
        return Ok(PromoteMode::Bare);
    }
    if body.eq_ignore_ascii_case("FORCE FENCED") {
        return Ok(PromoteMode::ForceFenced);
    }
    let parts = body.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 6
        || !parts[0].eq_ignore_ascii_case("AT")
        || !parts[1].eq_ignore_ascii_case("LSN")
        || !parts[4].eq_ignore_ascii_case("WAIT")
    {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "expected PROMOTE TO MASTER AT LSN <n> FENCE_TOKEN='<token>' WAIT <seconds> or FORCE FENCED",
        ));
    }
    let lsn = parts[2].parse::<u64>().map_err(|_| {
        error(
            crate::error::catalog::ER_PARSE_ERROR,
            "invalid promotion LSN",
        )
    })?;
    let (key, raw_token) = parts[3].split_once('=').ok_or_else(|| {
        error(
            crate::error::catalog::ER_PARSE_ERROR,
            "FENCE_TOKEN requires '='",
        )
    })?;
    if !key.eq_ignore_ascii_case("FENCE_TOKEN") {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "expected FENCE_TOKEN",
        ));
    }
    let token = unquote(raw_token);
    if role::uuid_string_to_bytes(&token).is_none() {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "FENCE_TOKEN must be the UUID returned by STOP MASTER WRITES",
        ));
    }
    let wait_seconds = parts[5].parse::<u64>().map_err(|_| {
        error(
            crate::error::catalog::ER_PARSE_ERROR,
            "invalid promotion WAIT seconds",
        )
    })?;
    if wait_seconds == 0 || wait_seconds > 3600 {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "promotion WAIT seconds must be between 1 and 3600",
        ));
    }
    Ok(PromoteMode::Planned {
        lsn,
        token,
        wait: std::time::Duration::from_secs(wait_seconds),
    })
}

fn parse_stop_master_writes(sql: &str) -> Result<std::time::Duration, QueryError> {
    let body = sql["STOP MASTER WRITES".len()..].trim();
    if body.is_empty() {
        return Ok(std::time::Duration::from_secs(30));
    }
    let mut parts = body.split_whitespace();
    if !parts
        .next()
        .is_some_and(|part| part.eq_ignore_ascii_case("WAIT"))
    {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "STOP MASTER WRITES accepts only WAIT <seconds>",
        ));
    }
    let seconds = parts
        .next()
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_PARSE_ERROR,
                "WAIT requires seconds",
            )
        })?
        .parse::<u64>()
        .map_err(|_| {
            error(
                crate::error::catalog::ER_PARSE_ERROR,
                "invalid WAIT seconds",
            )
        })?;
    if parts.next().is_some() || seconds == 0 || seconds > 3600 {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "WAIT seconds must be between 1 and 3600",
        ));
    }
    Ok(std::time::Duration::from_secs(seconds))
}

fn parse_demote(sql: &str) -> Result<DemoteOpts, QueryError> {
    let upper = sql.to_ascii_uppercase();
    let pos = upper.find("WITH MASTER").ok_or_else(|| {
        error(
            crate::error::catalog::ER_PARSE_ERROR,
            "DEMOTE TO REPLICA WITH MASTER='host:port' required",
        )
    })?;
    let body = sql[pos + "WITH MASTER".len()..].trim();
    let body = body.strip_prefix('=').unwrap_or(body).trim();
    let value = unquote(body);
    let (host, port) = value.rsplit_once(':').ok_or_else(|| {
        error(
            crate::error::catalog::ER_PARSE_ERROR,
            "MASTER must be 'host:port'",
        )
    })?;
    let port: u16 = port.parse().map_err(|_| {
        error(
            crate::error::catalog::ER_PARSE_ERROR,
            format!("invalid port '{port}'"),
        )
    })?;
    Ok(DemoteOpts {
        host: host.to_string(),
        port,
    })
}

fn parse_drop_replica(sql: &str) -> Result<String, QueryError> {
    let upper = sql.to_ascii_uppercase();
    let pos = upper.find("DROP MYDB REPLICA").ok_or_else(|| {
        error(
            crate::error::catalog::ER_PARSE_ERROR,
            "DROP MYDB REPLICA 'uuid'",
        )
    })?;
    let body = sql[pos + "DROP MYDB REPLICA".len()..].trim();
    let value = unquote(body);
    if value.is_empty() {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "DROP MYDB REPLICA requires a uuid",
        ));
    }
    Ok(value)
}

fn split_top_level_commas(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut quote_char = '\0';
    for c in s.chars() {
        match c {
            '\'' | '"' if !in_quotes => {
                in_quotes = true;
                quote_char = c;
                current.push(c);
            }
            c if in_quotes && c == quote_char => {
                in_quotes = false;
                current.push(c);
            }
            ',' if !in_quotes => {
                parts.push(std::mem::take(&mut current));
            }
            _ => current.push(c),
        }
    }
    if !current.trim().is_empty() {
        parts.push(current);
    }
    parts.into_iter().map(|s| s.trim().to_string()).collect()
}

fn unquote(s: &str) -> String {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        if s.len() >= 2 {
            return s[1..s.len() - 1].to_string();
        }
    }
    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_statement;

    #[test]
    fn sequence_promotion_requires_the_current_configuration_epoch() {
        let config = crate::storage::sequence::SequenceSyncConfig {
            remote_apply: true,
            replicas: vec![role::generate_uuid_string()],
            epoch: 7,
        };
        assert!(validate_sequence_promotion_epoch(&config, None).is_err());
        assert!(validate_sequence_promotion_epoch(&config, Some(6)).is_err());
        assert!(validate_sequence_promotion_epoch(&config, Some(7)).is_ok());
        let (sql, epoch) =
            split_sequence_promotion_epoch("PROMOTE TO MASTER FORCE FENCED SEQUENCE_SYNC_EPOCH=7")
                .unwrap();
        assert!(matches!(
            parse_promote(sql).unwrap(),
            PromoteMode::ForceFenced
        ));
        assert_eq!(epoch, Some(7));
        assert!(split_sequence_promotion_epoch(
            "PROMOTE TO MASTER FORCE FENCED SEQUENCE_SYNC_EPOCH=0"
        )
        .is_err());
    }

    fn parse(sql: &str) -> Statement {
        parse_statement(sql).unwrap_or_else(|err| panic!("parse '{sql}' failed: {err:?}"))
    }

    #[test]
    fn standalone_role_allows_everything() {
        for sql in [
            "SELECT 1",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a=1",
            "DELETE FROM t",
            "CREATE TABLE t (a INT)",
        ] {
            assert!(
                enforce_read_only_for_role(Role::Standalone, &parse(sql)).is_ok(),
                "standalone must allow: {sql}"
            );
        }
    }

    #[test]
    fn master_role_allows_everything() {
        for sql in [
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a=1",
            "DROP TABLE t",
        ] {
            assert!(
                enforce_read_only_for_role(Role::Master, &parse(sql)).is_ok(),
                "master must allow: {sql}"
            );
        }
    }

    #[test]
    fn replica_role_allows_reads() {
        for sql in [
            "SELECT 1",
            "SELECT * FROM t",
            "SHOW TABLES",
            "SHOW VARIABLES",
            "EXPLAIN SELECT * FROM t",
        ] {
            assert!(
                enforce_read_only_for_role(Role::Replica, &parse(sql)).is_ok(),
                "replica must allow read: {sql}"
            );
        }
    }

    #[test]
    fn replica_role_rejects_writes_with_error_1290() {
        for sql in [
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a=1",
            "DELETE FROM t",
            "CREATE TABLE t (a INT)",
            "DROP TABLE t",
            "ALTER TABLE t ADD COLUMN b INT",
            "TRUNCATE TABLE t",
            "GRANT SELECT ON *.* TO 'u'@'%'",
            // P0-7 regression — previously silently allowed:
            "RENAME TABLE t TO t2",
            "OPTIMIZE TABLE t",
            "REPAIR TABLE t",
            "CREATE SEQUENCE s1",
            "ALTER SEQUENCE s1 INCREMENT BY 5",
            "DROP SEQUENCE s1",
            "CREATE TABLESPACE ts1 ADD DATAFILE 'x'",
            "DROP TABLESPACE ts1",
        ] {
            let err = enforce_read_only_for_role(Role::Replica, &parse(sql))
                .expect_err(&format!("replica must reject: {sql}"));
            assert_eq!(err.code(), 1290, "wrong code for '{sql}': {err:?}");
        }
    }

    #[test]
    fn replica_role_rejects_select_for_update() {
        let err = enforce_read_only_for_role(
            Role::Replica,
            &parse("SELECT * FROM t WHERE a=1 FOR UPDATE"),
        )
        .expect_err("FOR UPDATE on replica must fail");
        assert_eq!(err.code(), 1290);
    }

    #[test]
    fn parse_change_master_extracts_host_port() {
        let opts = parse_change_master("CHANGE MYDB MASTER TO HOST='10.0.0.1', PORT=3317").unwrap();
        assert_eq!(opts.host.as_deref(), Some("10.0.0.1"));
        assert_eq!(opts.port, Some(3317));
    }

    #[test]
    fn parse_change_master_rejects_port_without_host() {
        let err = parse_change_master("CHANGE MYDB MASTER TO PORT=3317")
            .expect("opts ok")
            // the per-key parse succeeds; HOST required check happens in handler
            ;
        assert!(err.host.is_none());
        assert_eq!(err.port, Some(3317));
    }

    #[test]
    fn parse_demote_requires_host_colon_port() {
        let opts = parse_demote("DEMOTE TO REPLICA WITH MASTER='10.1.2.3:3320'").unwrap();
        assert_eq!(opts.host, "10.1.2.3");
        assert_eq!(opts.port, 3320);
        let err = parse_demote("DEMOTE TO REPLICA WITH MASTER='nohost'").unwrap_err();
        assert_eq!(err.code(), 1064);
    }

    #[test]
    fn parse_promote_requires_explicit_fence_contract_on_replica_forms() {
        assert!(matches!(
            parse_promote("PROMOTE TO MASTER FORCE FENCED").unwrap(),
            PromoteMode::ForceFenced
        ));
        let token = "123e4567-e89b-42d3-a456-426614174000";
        match parse_promote(&format!(
            "PROMOTE TO MASTER AT LSN 42 FENCE_TOKEN='{token}' WAIT 30"
        ))
        .unwrap()
        {
            PromoteMode::Planned {
                lsn,
                token: got,
                wait,
            } => {
                assert_eq!(lsn, 42);
                assert_eq!(got, token);
                assert_eq!(wait, std::time::Duration::from_secs(30));
            }
            other => panic!("unexpected promote mode: {other:?}"),
        }
        assert!(parse_promote("PROMOTE TO MASTER SOMEDAY").is_err());
    }

    #[test]
    fn parse_drop_replica_extracts_uuid() {
        let uuid = parse_drop_replica("DROP MYDB REPLICA 'abc-123'").unwrap();
        assert_eq!(uuid, "abc-123");
        let err = parse_drop_replica("DROP MYDB REPLICA").unwrap_err();
        assert_eq!(err.code(), 1064);
    }

    #[test]
    fn split_top_level_commas_handles_quoted_values() {
        let parts = split_top_level_commas("HOST='a, b', PORT=3316");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "HOST='a, b'");
        assert_eq!(parts[1], "PORT=3316");
    }

    #[test]
    fn admin_sql_detection_matches_command_set() {
        assert!(is_mydb_repl_admin_sql("SHOW MYDB MASTER STATUS"));
        assert!(is_mydb_repl_admin_sql("  show mydb replica status ;"));
        assert!(is_mydb_repl_admin_sql(
            "CHANGE MYDB MASTER TO HOST='h', PORT=3316"
        ));
        assert!(is_mydb_repl_admin_sql("START MYDB REPLICATION"));
        assert!(is_mydb_repl_admin_sql("STOP MYDB REPLICATION"));
        assert!(is_mydb_repl_admin_sql("RESET MYDB REPLICATION"));
        assert!(is_mydb_repl_admin_sql("PROMOTE TO MASTER"));
        assert!(is_mydb_repl_admin_sql(
            "DEMOTE TO REPLICA WITH MASTER='127.0.0.1:3316'"
        ));
        assert!(is_mydb_repl_admin_sql("STOP MASTER WRITES"));
        assert!(is_mydb_repl_admin_sql("RESUME MASTER WRITES"));
        assert!(is_mydb_repl_admin_sql("DROP MYDB REPLICA 'uuid'"));
        assert!(is_mydb_repl_admin_sql("SHOW MYDB REPLICAS"));
        assert!(!is_mydb_repl_admin_sql("SHOW MYDB MASTER STATUS EXTRA"));
        assert!(!is_mydb_repl_admin_sql("START MYDB REPLICATION EXTRA"));

        // not ours
        assert!(!is_mydb_repl_admin_sql("SHOW REPLICA STATUS"));
        assert!(!is_mydb_repl_admin_sql("START REPLICA"));
        assert!(!is_mydb_repl_admin_sql(
            "CHANGE REPLICATION SOURCE TO X='Y'"
        ));
        assert!(!is_mydb_repl_admin_sql("SELECT 1"));
    }
}
