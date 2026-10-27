use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;

use crate::engine::status_counters::SessionStatusValues;
use crate::storage::error;
use crate::types::{QueryError, Value};

/// connection-id → session map. ahash is ~2.8× cheaper than std
/// SipHash-1-3 on the `u32` key, and KILL / SHOW PROCESSLIST / register
/// / unregister all go through this map (the hot per-command update
/// bypasses it via `Arc<SessionHandle>`, so the remaining traffic is
/// connection-establishment heavy in BenchmarkSQL/sysbench).
type SessionMap = hashbrown::HashMap<u32, Arc<SessionHandle>, ahash::RandomState>;

/// Per-connection registry. Hot-path callers (the session command
/// loop) hold an `Arc<SessionHandle>` directly, so updating "what
/// command is this connection running" no longer takes the global
/// hashmap lock — only the per-entry mutex. This matters for
/// multi-connection sysbench/BenchmarkSQL where every COM_QUERY /
/// COM_STMT_EXECUTE used to contend on the same `Mutex<HashMap>`.
#[derive(Clone)]
pub struct SessionRegistry {
    sessions: Arc<RwLock<SessionMap>>,
    next_thread_id: Arc<AtomicU32>,
    reserved_thread_ids: Arc<Mutex<hashbrown::HashSet<u32, ahash::RandomState>>>,
    retired_status: Arc<crate::engine::status_counters::RetiredSessionStatus>,
    retired_status_groups: Arc<Mutex<RetiredStatusGroups>>,
    retired_connections: Arc<Mutex<RetiredConnectionGroups>>,
}

/// MySQL process-list thread class. Background rows are deliberately kept in
/// the same registry so every PROCESSLIST-compatible surface observes one
/// coherent snapshot and thread ids cannot collide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessKind {
    Foreground,
    Background,
}

/// States exposed by MySQL 8.0's process-list interfaces. Keep these values
/// static: state changes happen on command/operator boundaries and must not
/// allocate or contend on the global session map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum ProcessState {
    Starting,
    CheckingPermissions,
    OpeningTables,
    Optimizing,
    Preparing,
    Executing,
    Update,
    Updating,
    SortingForOrder,
    SortingForGroup,
    RemovingDuplicates,
    UserSleep,
    UserLock,
    CreatingTable,
    CreatingIndex,
    AlteringTable,
    CopyToTmpTable,
    Analyzing,
    CheckingTable,
    WaitingTableMetadataLock,
    WaitingSchemaMetadataLock,
    WaitingEventMetadataLock,
    WaitingFunctionMetadataLock,
    WaitingProcedureMetadataLock,
    WaitingTriggerMetadataLock,
    WaitingGlobalMetadataLock,
    WaitingOnCond,
    RollingBack,
    SendingToClient,
    QueryEnd,
    Killed,
    WaitingForNextActivation,
    SendingBinlogEventToReplica,
    WaitingForBinlogUpdates,
    ConnectingToSource,
    RequestingBinlogDump,
    WaitingForSourceEvent,
    QueueingSourceEvent,
    ReconnectingAfterSourceFailure,
    ReadingRelayLog,
    WaitingForRelayLog,
    WaitingForSourceDelay,
}

impl ProcessState {
    const COUNT: usize = ProcessState::WaitingForSourceDelay as usize + 1;
    const ALL: [ProcessState; Self::COUNT] = [
        Self::Starting,
        Self::CheckingPermissions,
        Self::OpeningTables,
        Self::Optimizing,
        Self::Preparing,
        Self::Executing,
        Self::Update,
        Self::Updating,
        Self::SortingForOrder,
        Self::SortingForGroup,
        Self::RemovingDuplicates,
        Self::UserSleep,
        Self::UserLock,
        Self::CreatingTable,
        Self::CreatingIndex,
        Self::AlteringTable,
        Self::CopyToTmpTable,
        Self::Analyzing,
        Self::CheckingTable,
        Self::WaitingTableMetadataLock,
        Self::WaitingSchemaMetadataLock,
        Self::WaitingEventMetadataLock,
        Self::WaitingFunctionMetadataLock,
        Self::WaitingProcedureMetadataLock,
        Self::WaitingTriggerMetadataLock,
        Self::WaitingGlobalMetadataLock,
        Self::WaitingOnCond,
        Self::RollingBack,
        Self::SendingToClient,
        Self::QueryEnd,
        Self::Killed,
        Self::WaitingForNextActivation,
        Self::SendingBinlogEventToReplica,
        Self::WaitingForBinlogUpdates,
        Self::ConnectingToSource,
        Self::RequestingBinlogDump,
        Self::WaitingForSourceEvent,
        Self::QueueingSourceEvent,
        Self::ReconnectingAfterSourceFailure,
        Self::ReadingRelayLog,
        Self::WaitingForRelayLog,
        Self::WaitingForSourceDelay,
    ];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::CheckingPermissions => "checking permissions",
            Self::OpeningTables => "Opening tables",
            Self::Optimizing => "optimizing",
            Self::Preparing => "preparing",
            Self::Executing => "executing",
            Self::Update => "update",
            Self::Updating => "updating",
            Self::SortingForOrder => "Sorting for order",
            Self::SortingForGroup => "Sorting for group",
            Self::RemovingDuplicates => "Removing duplicates",
            Self::UserSleep => "User sleep",
            Self::UserLock => "User lock",
            Self::CreatingTable => "creating table",
            Self::CreatingIndex => "Creating index",
            Self::AlteringTable => "altering table",
            Self::CopyToTmpTable => "copy to tmp table",
            Self::Analyzing => "Analyzing",
            Self::CheckingTable => "Checking table",
            Self::WaitingTableMetadataLock => "Waiting for table metadata lock",
            Self::WaitingSchemaMetadataLock => "Waiting for schema metadata lock",
            Self::WaitingEventMetadataLock => "Waiting for event metadata lock",
            Self::WaitingFunctionMetadataLock => "Waiting for stored function metadata lock",
            Self::WaitingProcedureMetadataLock => "Waiting for stored procedure metadata lock",
            Self::WaitingTriggerMetadataLock => "Waiting for trigger metadata lock",
            Self::WaitingGlobalMetadataLock => "Waiting for global read lock",
            Self::WaitingOnCond => "Waiting on cond",
            Self::RollingBack => "Rolling back",
            Self::SendingToClient => "Sending to client",
            Self::QueryEnd => "query end",
            Self::Killed => "Killed",
            Self::WaitingForNextActivation => "Waiting for next activation",
            Self::SendingBinlogEventToReplica => "Sending binlog event to replica",
            Self::WaitingForBinlogUpdates => {
                "Source has sent all binlog to replica; waiting for more updates"
            }
            Self::ConnectingToSource => "Connecting to source",
            Self::RequestingBinlogDump => "Requesting binlog dump",
            Self::WaitingForSourceEvent => "Waiting for source to send event",
            Self::QueueingSourceEvent => "Queueing source event to relay log",
            Self::ReconnectingAfterSourceFailure => {
                "Waiting to reconnect after a failed source event read"
            }
            Self::ReadingRelayLog => "Reading event from the relay log",
            Self::WaitingForRelayLog => "Replica has read all relay log; waiting for more updates",
            Self::WaitingForSourceDelay => {
                "Waiting until SOURCE_DELAY seconds after source executed event"
            }
        }
    }

    /// Activity phases can overlap (parallel workers) or nest. The most
    /// specific phase wins, independently of entry/exit order.
    const fn priority(self) -> u8 {
        match self {
            Self::Killed => u8::MAX,
            Self::WaitingTableMetadataLock
            | Self::WaitingSchemaMetadataLock
            | Self::WaitingEventMetadataLock
            | Self::WaitingFunctionMetadataLock
            | Self::WaitingProcedureMetadataLock
            | Self::WaitingTriggerMetadataLock
            | Self::WaitingGlobalMetadataLock
            | Self::WaitingOnCond
            | Self::UserSleep
            | Self::UserLock => 220,
            Self::SortingForOrder
            | Self::SortingForGroup
            | Self::RemovingDuplicates
            | Self::CopyToTmpTable => 200,
            Self::SendingToClient => 190,
            Self::CheckingPermissions | Self::OpeningTables | Self::Optimizing => 180,
            Self::Executing
            | Self::Update
            | Self::Updating
            | Self::CreatingTable
            | Self::CreatingIndex
            | Self::AlteringTable
            | Self::Analyzing
            | Self::CheckingTable
            | Self::RollingBack
            | Self::QueryEnd => 150,
            Self::Preparing => 100,
            Self::Starting => 10,
            _ => 140,
        }
    }
}

/// Shared state for a single connection. Returned from
/// [`SessionRegistry::register`] and stored on the session so the
/// hot path bypasses the global hashmap.
pub struct SessionHandle {
    kill_flag: Arc<AtomicBool>,
    connection_kill_generation: AtomicU64,
    query_cancel_generation: AtomicU64,
    /// Storage transaction id this connection currently owns, or 0 when it has
    /// no open transaction. A per-handle relaxed atomic (contention-free, like
    /// the kill generations), so the per-transaction update never touches the
    /// global session map. Read by `snapshot()` to join
    /// `performance_schema.data_locks` rows back to the owning thread.
    current_txn: AtomicU64,
    /// First consistent read of a lazy transaction, zero while inactive.
    lazy_trx_started_ms: AtomicU64,
    session_memory_peak: AtomicU64,
    protocol_prepared_memory_bytes: AtomicU64,
    sql_prepared_memory_bytes: AtomicU64,
    cte_memory_bytes: AtomicU64,
    status: Arc<crate::engine::status_counters::SessionStatus>,
    notify: Notify,
    entry: Mutex<SessionEntry>,
    _background_memory_charge: Option<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

#[derive(Clone)]
pub struct SessionCancelHandle {
    handle: Arc<SessionHandle>,
}

impl std::fmt::Debug for SessionCancelHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionCancelHandle")
            .finish_non_exhaustive()
    }
}

enum ProcessSqlText {
    Owned(String),
    Shared(Arc<str>),
}

impl ProcessSqlText {
    fn as_str(&self) -> &str {
        match self {
            Self::Owned(text) => text,
            Self::Shared(text) => text,
        }
    }
}

struct SessionEntry {
    authenticated_as: crate::engine::auth::AccountKey,
    user: String,
    host: String,
    database: Option<String>,
    /// `&'static str` so the busy/idle transitions on the hot path
    /// don't allocate. The set of values is fixed: "Sleep", "Query",
    /// "Execute", "Prepare", "Init DB".
    command: &'static str,
    started_at: Instant,
    info: Option<ProcessSqlText>,
    kind: ProcessKind,
    base_state: Option<ProcessState>,
    effective_state: Option<ProcessState>,
    active_stages: [u16; ProcessState::COUNT],
    active_stage_total: u32,
    killed: bool,
    /// Replication SQL threads use MySQL's special TIME semantics: seconds
    /// since the source event most recently applied, rather than state age.
    time_reference_unix_secs: Option<u64>,
    /// CLIENT_CONNECT_ATTRS sent at handshake; surfaced via
    /// performance_schema.session_connect_attrs.
    connection_attrs: std::collections::BTreeMap<String, String>,
    session_variables: Vec<(String, String)>,
    user_variables: Vec<(String, Value)>,
    pfs_autocommit: bool,
    pfs_isolation: String,
    prepared_statements: Vec<PreparedStatementSnapshot>,
    // Keep the identity alongside the weak reference: detaching a finishing
    // query must not temporarily own (and possibly destroy) its replacement.
    active_query_memory: Option<(u64, Weak<crate::engine::executor::mem_budget::MemBudget>)>,
}

#[derive(Debug, Clone)]
pub struct ProcessInfo {
    pub id: u32,
    pub user: String,
    pub host: String,
    pub database: Option<String>,
    pub command: String,
    pub time_secs: u64,
    pub state: Option<String>,
    pub info: Option<String>,
    pub connection_attrs: std::collections::BTreeMap<String, String>,
    pub session_variables: Vec<(String, String)>,
    pub user_variables: Vec<(String, Value)>,
    pub autocommit: bool,
    pub isolation: String,
    pub(crate) prepared_statements: Vec<PreparedStatementSnapshot>,
    /// Storage transaction id this connection currently owns, if any. Joins
    /// `performance_schema.data_locks.ENGINE_TRANSACTION_ID` back to a
    /// `THREAD_ID`.
    pub current_txn: Option<u64>,
    pub lazy_trx_started_ms: u64,
    pub query_id: Option<u64>,
    pub exec_current_bytes: u64,
    pub query_peak_bytes: Option<u64>,
    pub session_peak_bytes: u64,
    pub query_limit_bytes: Option<u64>,
    pub spill_bytes: Option<u64>,
    pub prepared_cache_bytes: u64,
    pub cte_cache_bytes: u64,
    pub retained_cache_bytes: u64,
    pub(crate) kind: ProcessKind,
}

#[derive(Debug, Clone)]
pub(crate) struct ThreadStatusSnapshot {
    pub thread_id: u32,
    pub status: SessionStatusValues,
}

#[derive(Debug, Clone)]
pub(crate) struct StatusGroupSnapshot {
    pub user: Option<String>,
    pub host: Option<String>,
    pub status: SessionStatusValues,
}

#[derive(Debug, Clone)]
pub(crate) struct RetiredConnectionSnapshot {
    pub user: String,
    pub host: String,
    pub total_connections: u64,
    pub max_controlled_memory: u64,
    pub max_total_memory: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedStatementSnapshot {
    pub statement_name: Option<String>,
    pub object_instance: u64,
    pub statement_id: u64,
    pub sql_text: Arc<str>,
    pub owner_thread_id: u64,
    pub timer_prepare: u64,
    pub count_execute: u64,
    pub sum_timer_execute: u64,
    pub min_timer_execute: u64,
    pub max_timer_execute: u64,
    pub sum_errors: u64,
    pub memory_bytes: u64,
}

impl PreparedStatementSnapshot {
    pub(crate) fn sql(
        name: String,
        sql_text: Arc<str>,
        thread_id: u64,
        memory_bytes: u64,
        elapsed: std::time::Duration,
    ) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1u64 << 63);
        let statement_id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            statement_name: Some(name),
            object_instance: statement_id,
            statement_id,
            sql_text,
            owner_thread_id: thread_id,
            timer_prepare: u64::try_from(elapsed.as_nanos().saturating_mul(1_000))
                .unwrap_or(u64::MAX),
            count_execute: 0,
            sum_timer_execute: 0,
            min_timer_execute: 0,
            max_timer_execute: 0,
            sum_errors: 0,
            memory_bytes,
        }
    }
}

const MAX_RETIRED_STATUS_GROUPS: usize = 4096;

#[derive(Debug, Clone, Copy, Default)]
struct RetiredConnectionStat {
    total_connections: u64,
    max_controlled_memory: u64,
    max_total_memory: u64,
}

#[derive(Debug)]
struct RetiredConnectionGroups {
    accounts: std::collections::BTreeMap<(String, String), RetiredConnectionStat>,
    charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl Default for RetiredConnectionGroups {
    fn default() -> Self {
        Self {
            accounts: Default::default(),
            charge: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::PerformanceSchema,
                0,
            )
            .expect("zero-byte PFS connection reservation cannot fail"),
        }
    }
}

impl RetiredConnectionGroups {
    fn merge(&mut self, user: &str, host: &str, controlled: u64, total: u64) {
        let key = (user.to_string(), host.to_string());
        if let Some(stat) = self.accounts.get_mut(&key) {
            stat.total_connections = stat.total_connections.saturating_add(1);
            stat.max_controlled_memory = stat.max_controlled_memory.max(controlled);
            stat.max_total_memory = stat.max_total_memory.max(total);
            return;
        }
        if self.accounts.len() >= MAX_RETIRED_STATUS_GROUPS {
            return;
        }
        let bytes = std::mem::size_of::<((String, String), RetiredConnectionStat)>()
            .saturating_add(key.0.capacity())
            .saturating_add(key.1.capacity());
        if !self.charge.try_grow_optional(bytes) {
            return;
        }
        self.accounts.insert(
            key,
            RetiredConnectionStat {
                total_connections: 1,
                max_controlled_memory: controlled,
                max_total_memory: total,
            },
        );
    }
}

#[derive(Debug)]
struct RetiredStatusGroups {
    accounts: std::collections::BTreeMap<(String, String), SessionStatusValues>,
    hosts: std::collections::BTreeMap<String, SessionStatusValues>,
    users: std::collections::BTreeMap<String, SessionStatusValues>,
    charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl Default for RetiredStatusGroups {
    fn default() -> Self {
        Self {
            accounts: Default::default(),
            hosts: Default::default(),
            users: Default::default(),
            charge: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                0,
            )
            .expect("zero-byte status aggregation reservation cannot fail"),
        }
    }
}

impl RetiredStatusGroups {
    fn merge(&mut self, user: &str, host: &str, status: &SessionStatusValues) {
        let account = (user.to_string(), host.to_string());
        if let Some(existing) = self.accounts.get_mut(&account) {
            existing.add_values(status);
        } else if self.accounts.len() < MAX_RETIRED_STATUS_GROUPS
            && self.reserve_entry(
                std::mem::size_of::<((String, String), SessionStatusValues)>()
                    .saturating_add(account.0.capacity())
                    .saturating_add(account.1.capacity()),
            )
        {
            self.accounts.insert(account, status.clone());
        } else {
            crate::engine::status_counters::record_status_aggregation_dropped();
        }

        if let Some(existing) = self.hosts.get_mut(host) {
            existing.add_values(status);
        } else if self.hosts.len() < MAX_RETIRED_STATUS_GROUPS
            && self.reserve_entry(
                std::mem::size_of::<(String, SessionStatusValues)>().saturating_add(host.len()),
            )
        {
            self.hosts.insert(host.to_string(), status.clone());
        } else {
            crate::engine::status_counters::record_status_aggregation_dropped();
        }

        if let Some(existing) = self.users.get_mut(user) {
            existing.add_values(status);
        } else if self.users.len() < MAX_RETIRED_STATUS_GROUPS
            && self.reserve_entry(
                std::mem::size_of::<(String, SessionStatusValues)>().saturating_add(user.len()),
            )
        {
            self.users.insert(user.to_string(), status.clone());
        } else {
            crate::engine::status_counters::record_status_aggregation_dropped();
        }
    }

    fn reserve_entry(&mut self, bytes: usize) -> bool {
        self.charge.try_grow_optional(bytes)
    }

    fn reset_values(&mut self) {
        for value in self.accounts.values_mut() {
            *value = Default::default();
        }
        for value in self.hosts.values_mut() {
            *value = Default::default();
        }
        for value in self.users.values_mut() {
            *value = Default::default();
        }
    }
}

fn normalize_status_host(host: &str) -> String {
    if let Ok(addr) = host.parse::<std::net::SocketAddr>() {
        return addr.ip().to_string();
    }
    if let Some(port) = host.strip_prefix("localhost:") {
        if port.bytes().all(|byte| byte.is_ascii_digit()) {
            return "localhost".to_string();
        }
    }
    host.to_string()
}

/// Restores the prior effective state when a nested or concurrent activity
/// phase ends. The fixed counter array makes out-of-order worker completion
/// safe without allocating a dynamic stage stack.
pub(crate) struct ProcessStageGuard {
    handle: Arc<SessionHandle>,
    state: ProcessState,
}

pub(crate) struct BackgroundProcessGuard {
    sessions: Arc<RwLock<SessionMap>>,
    id: u32,
    handle: Arc<SessionHandle>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(SessionMap::default())),
            next_thread_id: Arc::new(AtomicU32::new(1)),
            reserved_thread_ids: Arc::new(Mutex::new(Default::default())),
            retired_status: Arc::new(Default::default()),
            retired_status_groups: Arc::new(Mutex::new(Default::default())),
            retired_connections: Arc::new(Mutex::new(Default::default())),
        }
    }

    pub fn allocate_thread_id(&self) -> u32 {
        let sessions = self.sessions.read();
        let mut reserved = self.reserved_thread_ids.lock();
        loop {
            let id = self.next_thread_id.fetch_add(1, Ordering::Relaxed);
            if id != 0 && !sessions.contains_key(&id) && reserved.insert(id) {
                return id;
            }
        }
    }

    #[cfg(test)]
    pub fn register(
        &self,
        connection_id: u32,
        user: String,
        host: String,
        database: String,
    ) -> Arc<SessionHandle> {
        self.register_with_status(
            connection_id,
            user,
            host,
            database,
            crate::engine::status_counters::SessionStatus::new_untracked(),
        )
    }

    #[cfg(test)]
    pub(crate) fn register_with_status(
        &self,
        connection_id: u32,
        user: String,
        host: String,
        database: String,
        status: Arc<crate::engine::status_counters::SessionStatus>,
    ) -> Arc<SessionHandle> {
        let account = crate::engine::auth::AccountKey::new(&user, normalize_status_host(&host));
        self.register_authenticated(connection_id, user, host, database, status, account)
    }

    pub(crate) fn register_authenticated(
        &self,
        connection_id: u32,
        user: String,
        host: String,
        database: String,
        status: Arc<crate::engine::status_counters::SessionStatus>,
        account: crate::engine::auth::AccountKey,
    ) -> Arc<SessionHandle> {
        self.register_with_kind(
            connection_id,
            user,
            host,
            database,
            "Sleep",
            None,
            None,
            ProcessKind::Foreground,
            account,
            None,
            status,
        )
    }

    fn register_with_kind(
        &self,
        connection_id: u32,
        user: String,
        host: String,
        database: String,
        command: &'static str,
        state: Option<ProcessState>,
        info: Option<String>,
        kind: ProcessKind,
        authenticated_as: crate::engine::auth::AccountKey,
        background_memory_charge: Option<
            crate::engine::executor::mem_budget::ControlledMemoryCharge,
        >,
        status: Arc<crate::engine::status_counters::SessionStatus>,
    ) -> Arc<SessionHandle> {
        let kill_flag = Arc::new(AtomicBool::new(false));
        let entry = SessionEntry {
            authenticated_as,
            user,
            host,
            database: (!database.is_empty()).then_some(database),
            command,
            started_at: Instant::now(),
            info: info.map(ProcessSqlText::Owned),
            kind,
            base_state: state,
            effective_state: state,
            active_stages: [0; ProcessState::COUNT],
            active_stage_total: 0,
            killed: false,
            time_reference_unix_secs: None,
            connection_attrs: std::collections::BTreeMap::new(),
            session_variables: Vec::new(),
            user_variables: Vec::new(),
            pfs_autocommit: true,
            pfs_isolation: "REPEATABLE READ".to_string(),
            prepared_statements: Vec::new(),
            active_query_memory: None,
        };
        let handle = Arc::new(SessionHandle {
            kill_flag,
            connection_kill_generation: AtomicU64::new(0),
            query_cancel_generation: AtomicU64::new(0),
            current_txn: AtomicU64::new(0),
            lazy_trx_started_ms: AtomicU64::new(0),
            session_memory_peak: AtomicU64::new(0),
            protocol_prepared_memory_bytes: AtomicU64::new(0),
            sql_prepared_memory_bytes: AtomicU64::new(0),
            cte_memory_bytes: AtomicU64::new(0),
            status,
            notify: Notify::new(),
            entry: Mutex::new(entry),
            _background_memory_charge: background_memory_charge,
        });
        self.sessions
            .write()
            .insert(connection_id, Arc::clone(&handle));
        handle
    }

    pub(crate) fn register_background(
        &self,
        user: String,
        host: String,
        database: Option<String>,
        command: &'static str,
        state: ProcessState,
        info: Option<String>,
    ) -> Option<BackgroundProcessGuard> {
        let reserved_bytes = std::mem::size_of::<SessionHandle>()
            .saturating_add(std::mem::size_of::<SessionEntry>())
            .saturating_add(std::mem::size_of::<(u32, Arc<SessionHandle>)>())
            .saturating_add(user.capacity())
            .saturating_add(host.capacity())
            .saturating_add(database.as_ref().map_or(0, String::capacity))
            .saturating_add(info.as_ref().map_or(0, String::capacity));
        let Some(memory_charge) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                reserved_bytes,
            )
        else {
            crate::util::logging::log_message(format_args!(
                "processlist: skipped optional background row for {user}: SystemMetadata budget exhausted"
            ));
            return None;
        };
        let status = match crate::engine::status_counters::SessionStatus::try_new_governed() {
            Ok(status) => status,
            Err(_) => {
                crate::util::logging::log_message(format_args!(
                    "processlist: skipped optional background row for {user}: status budget exhausted"
                ));
                return None;
            }
        };
        let id = self.allocate_thread_id();
        let handle = self.register_with_kind(
            id,
            user,
            host,
            database.unwrap_or_default(),
            command,
            Some(state),
            info,
            ProcessKind::Background,
            crate::engine::auth::AccountKey::new("", ""),
            Some(memory_charge),
            status,
        );
        Some(BackgroundProcessGuard {
            sessions: Arc::clone(&self.sessions),
            id,
            handle,
        })
    }

    pub fn unregister(&self, connection_id: u32) {
        crate::engine::cbo::trace::clear_connection(connection_id);
        let mut sessions = self.sessions.write();
        self.reserved_thread_ids.lock().remove(&connection_id);
        if let Some(handle) = sessions.remove(&connection_id) {
            let status = handle.status.snapshot();
            let entry = handle.entry.lock();
            if entry.kind == ProcessKind::Foreground {
                self.retired_status.merge(&handle.status);
                let host = normalize_status_host(&entry.host);
                self.retired_status_groups
                    .lock()
                    .merge(&entry.user, &host, &status);
                let controlled = handle.session_memory_peak.load(Ordering::Relaxed);
                let retained = handle
                    .protocol_prepared_memory_bytes
                    .load(Ordering::Relaxed)
                    .saturating_add(handle.sql_prepared_memory_bytes.load(Ordering::Relaxed))
                    .saturating_add(handle.cte_memory_bytes.load(Ordering::Relaxed));
                self.retired_connections.lock().merge(
                    &entry.user,
                    &host,
                    controlled,
                    controlled.saturating_add(retained),
                );
            }
            crate::engine::perf_events::unregister_thread(connection_id as u64);
            crate::engine::perf_errors::unregister_thread(connection_id as u64);
        }
    }

    /// Consistent lifecycle snapshot: the registry read lock prevents a
    /// connection from being merged into `retired_status` and removed while
    /// it is also included in the active-session sum.
    pub(crate) fn global_session_status(&self) -> SessionStatusValues {
        let sessions = self.sessions.read();
        let mut values = self.retired_status.snapshot();
        for handle in sessions.values() {
            if handle.entry.lock().kind == ProcessKind::Foreground {
                values.add_snapshot(&handle.status);
            }
        }
        values
    }

    pub(crate) fn running_foreground_count(&self) -> u64 {
        self.sessions
            .read()
            .values()
            .filter(|handle| {
                let entry = handle.entry.lock();
                entry.kind == ProcessKind::Foreground && entry.command != "Sleep"
            })
            .count() as u64
    }

    /// MySQL FLUSH STATUS rolls every active session into the global base and
    /// then zeros its session counters. Holding the write lock makes the
    /// merge/reset atomic with respect to unregister and global snapshots.
    pub(crate) fn flush_session_status(&self) {
        let sessions = self.sessions.write();
        for handle in sessions.values() {
            if handle.entry.lock().kind == ProcessKind::Foreground {
                self.retired_status.merge(&handle.status);
                handle.status.reset();
            }
        }
        self.retired_status_groups.lock().reset_values();
    }

    pub(crate) fn truncate_thread_status(&self) {
        let sessions = self.sessions.write();
        let mut groups = self.retired_status_groups.lock();
        for handle in sessions.values() {
            let entry = handle.entry.lock();
            if entry.kind != ProcessKind::Foreground {
                continue;
            }
            let status = handle.status.snapshot();
            self.retired_status.merge(&handle.status);
            groups.merge(&entry.user, &normalize_status_host(&entry.host), &status);
            handle.status.reset();
        }
    }

    pub(crate) fn reset_account_status(&self) {
        for value in self.retired_status_groups.lock().accounts.values_mut() {
            *value = Default::default();
        }
    }

    pub(crate) fn reset_host_status(&self) {
        for value in self.retired_status_groups.lock().hosts.values_mut() {
            *value = Default::default();
        }
    }

    pub(crate) fn reset_user_status(&self) {
        for value in self.retired_status_groups.lock().users.values_mut() {
            *value = Default::default();
        }
    }

    pub(crate) fn thread_status_snapshots(&self) -> Vec<ThreadStatusSnapshot> {
        let sessions = self.sessions.read();
        sessions
            .iter()
            .filter_map(|(&thread_id, handle)| {
                let entry = handle.entry.lock();
                (entry.kind == ProcessKind::Foreground).then(|| ThreadStatusSnapshot {
                    thread_id,
                    status: handle.status.snapshot(),
                })
            })
            .collect()
    }

    pub(crate) fn account_status_snapshots(&self) -> Vec<StatusGroupSnapshot> {
        self.status_group_snapshots(
            |retired| &retired.accounts,
            |entry| (entry.user.clone(), normalize_status_host(&entry.host)),
            |(user, host), status| StatusGroupSnapshot {
                user: Some(user),
                host: Some(host),
                status,
            },
        )
    }

    pub(crate) fn host_status_snapshots(&self) -> Vec<StatusGroupSnapshot> {
        self.status_group_snapshots(
            |retired| &retired.hosts,
            |entry| normalize_status_host(&entry.host),
            |host, status| StatusGroupSnapshot {
                user: None,
                host: Some(host),
                status,
            },
        )
    }

    pub(crate) fn user_status_snapshots(&self) -> Vec<StatusGroupSnapshot> {
        self.status_group_snapshots(
            |retired| &retired.users,
            |entry| entry.user.clone(),
            |user, status| StatusGroupSnapshot {
                user: Some(user),
                host: None,
                status,
            },
        )
    }

    fn status_group_snapshots<K: Clone + Ord>(
        &self,
        retired: impl FnOnce(
            &RetiredStatusGroups,
        ) -> &std::collections::BTreeMap<K, SessionStatusValues>,
        key: impl Fn(&SessionEntry) -> K,
        row: impl Fn(K, SessionStatusValues) -> StatusGroupSnapshot,
    ) -> Vec<StatusGroupSnapshot> {
        // Match unregister/flush/truncate lock order, and keep the lifecycle
        // stable across both halves of the snapshot. Otherwise a connection
        // could retire after cloning the base but before reading live counters.
        let sessions = self.sessions.read();
        let mut groups = retired(&self.retired_status_groups.lock()).clone();
        #[cfg(test)]
        status_tests::after_retired_snapshot();
        for handle in sessions.values() {
            let entry = handle.entry.lock();
            if entry.kind == ProcessKind::Foreground {
                groups
                    .entry(key(&entry))
                    .or_default()
                    .add_snapshot(&handle.status);
            }
        }
        drop(sessions);
        groups
            .into_iter()
            .map(|(key, status)| row(key, status))
            .collect()
    }

    pub(crate) fn retired_connection_snapshots(&self) -> Vec<RetiredConnectionSnapshot> {
        self.retired_connections
            .lock()
            .accounts
            .iter()
            .map(|((user, host), stat)| RetiredConnectionSnapshot {
                user: user.clone(),
                host: host.clone(),
                total_connections: stat.total_connections,
                max_controlled_memory: stat.max_controlled_memory,
                max_total_memory: stat.max_total_memory,
            })
            .collect()
    }

    /// Authorize and cancel the same live identity, including COM_CHANGE_USER races.
    pub(crate) fn kill_authorized(
        &self,
        connection_id: u32,
        query_only: bool,
        authorize: impl FnOnce(&crate::engine::auth::AccountKey) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let sessions = self.sessions.read();
        let handle = sessions.get(&connection_id).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_THREAD,
                format!("Unknown thread id: {connection_id}"),
            )
        })?;
        let entry = handle.entry.lock();
        authorize(&entry.authenticated_as)?;
        if entry.kind != ProcessKind::Foreground {
            return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "KILL is not supported for background service threads; use the service's stop command"));
        }
        if query_only {
            handle.cancel_query();
            crate::engine::cancel::record_killed_query();
        } else {
            handle.kill_connection();
            crate::engine::cancel::record_killed_connection();
        }
        drop(entry);
        handle.mark_killed_state();
        Ok(())
    }

    #[cfg(test)]
    pub fn kill(&self, connection_id: u32) -> Result<(), QueryError> {
        self.kill_connection(connection_id)
    }

    #[cfg(test)]
    pub fn kill_connection(&self, connection_id: u32) -> Result<(), QueryError> {
        let sessions = self.sessions.read();
        match sessions.get(&connection_id) {
            Some(handle) if handle.entry.lock().kind == ProcessKind::Foreground => {
                handle.kill_connection();
                handle.mark_killed_state();
                crate::engine::cancel::record_killed_connection();
                Ok(())
            }
            Some(_) => Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "KILL is not supported for background service threads; use the service's stop command")),
            None => Err(error(
                crate::error::catalog::ER_NO_SUCH_THREAD,
                format!("Unknown thread id: {connection_id}"),
            )),
        }
    }

    /// Force every registered connection closed. Used by the graceful
    /// shutdown path when the drain window expires with connections still
    /// open: sets each session's kill flag and wakes any blocked read so
    /// the connection task returns promptly instead of pinning a slot
    /// until its idle timeout.
    pub fn kill_all(&self) {
        let sessions = self.sessions.read();
        for handle in sessions
            .values()
            .filter(|handle| handle.entry.lock().kind == ProcessKind::Foreground)
        {
            handle.kill_connection();
            handle.mark_killed_state();
            crate::engine::cancel::record_killed_connection();
        }
    }

    #[cfg(test)]
    pub fn kill_query(&self, connection_id: u32) -> Result<(), QueryError> {
        let sessions = self.sessions.read();
        match sessions.get(&connection_id) {
            Some(handle) if handle.entry.lock().kind == ProcessKind::Foreground => {
                handle.cancel_query();
                handle.mark_killed_state();
                crate::engine::cancel::record_killed_query();
                Ok(())
            }
            Some(_) => Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "KILL is not supported for background service threads; use the service's stop command")),
            None => Err(error(
                crate::error::catalog::ER_NO_SUCH_THREAD,
                format!("Unknown thread id: {connection_id}"),
            )),
        }
    }

    pub(crate) fn cancel_background_query(&self, connection_id: u32) {
        let sessions = self.sessions.read();
        if let Some(handle) = sessions.get(&connection_id) {
            if handle.entry.lock().kind == ProcessKind::Background {
                handle.cancel_query();
                handle.mark_killed_state();
            }
        }
    }

    pub fn snapshot(&self) -> Vec<ProcessInfo> {
        let sessions = self.sessions.read();
        let now = Instant::now();
        let now_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        sessions
            .iter()
            .map(|(&id, handle)| {
                let current_txn = match handle.current_txn.load(Ordering::Relaxed) {
                    0 => None,
                    txn => Some(txn),
                };
                // Declare this before the guard so it is dropped AFTER the
                // guard, including during unwinding. A concurrent query can
                // release its final owner after our upgrade; MemBudget::drop
                // then calls detach_query_memory on this same entry.
                let active_budget;
                let entry = handle.entry.lock();
                active_budget = entry
                    .active_query_memory
                    .as_ref()
                    .and_then(|(_, budget)| budget.upgrade());
                #[cfg(test)]
                tests::snapshot_budget_upgraded();
                let query_id = active_budget.as_ref().map(|budget| budget.query_id());
                let exec_current_bytes = active_budget
                    .as_ref()
                    .map(|budget| budget.used() as u64)
                    .unwrap_or(0);
                let query_peak_bytes = active_budget
                    .as_ref()
                    .map(|budget| budget.peak_used() as u64);
                let query_limit_bytes = active_budget.as_ref().map(|budget| budget.limit() as u64);
                let spill_bytes = active_budget.as_ref().map(|budget| budget.spill_bytes());
                let prepared_cache_bytes = handle
                    .protocol_prepared_memory_bytes
                    .load(Ordering::Relaxed)
                    .saturating_add(handle.sql_prepared_memory_bytes.load(Ordering::Relaxed));
                let cte_cache_bytes = handle.cte_memory_bytes.load(Ordering::Relaxed);
                let retained_cache_bytes = prepared_cache_bytes.saturating_add(cte_cache_bytes);
                let session_peak_bytes = handle
                    .session_memory_peak
                    .load(Ordering::Relaxed)
                    .max(query_peak_bytes.unwrap_or(0));
                ProcessInfo {
                    id,
                    user: entry.user.clone(),
                    host: entry.host.clone(),
                    database: entry.database.clone(),
                    command: entry.command.to_string(),
                    time_secs: entry
                        .time_reference_unix_secs
                        .map(|reference| now_unix_secs.saturating_sub(reference))
                        .unwrap_or_else(|| now.duration_since(entry.started_at).as_secs()),
                    state: entry
                        .effective_state
                        .map(|state| state.as_str().to_string()),
                    info: entry.info.as_ref().map(|text| text.as_str().to_owned()),
                    connection_attrs: entry.connection_attrs.clone(),
                    session_variables: entry.session_variables.clone(),
                    user_variables: entry.user_variables.clone(),
                    autocommit: entry.pfs_autocommit,
                    isolation: entry.pfs_isolation.clone(),
                    prepared_statements: entry.prepared_statements.clone(),
                    current_txn,
                    lazy_trx_started_ms: handle.lazy_trx_started_ms.load(Ordering::Relaxed),
                    query_id,
                    exec_current_bytes,
                    query_peak_bytes,
                    session_peak_bytes,
                    query_limit_bytes,
                    spill_bytes,
                    prepared_cache_bytes,
                    cte_cache_bytes,
                    retained_cache_bytes,
                    kind: entry.kind,
                }
            })
            .collect()
    }
}

impl SessionHandle {
    /// A lost transport must cancel owned work without counting a SQL KILL.
    pub(crate) fn peer_disconnected(&self) {
        self.kill_connection();
    }

    pub fn kill_flag(&self) -> &Arc<AtomicBool> {
        &self.kill_flag
    }

    pub fn cancel_handle(self: &Arc<Self>) -> SessionCancelHandle {
        SessionCancelHandle {
            handle: Arc::clone(self),
        }
    }

    pub fn connection_kill_generation(&self) -> u64 {
        self.connection_kill_generation.load(Ordering::Acquire)
    }

    pub fn query_cancel_generation(&self) -> u64 {
        self.query_cancel_generation.load(Ordering::Acquire)
    }

    pub fn is_connection_killed_since(&self, generation: u64) -> bool {
        self.connection_kill_generation() != generation
    }

    pub fn is_query_cancelled_since(&self, generation: u64) -> bool {
        self.query_cancel_generation() != generation || self.kill_flag.load(Ordering::Acquire)
    }

    pub async fn notified(&self) {
        if self.kill_flag.load(Ordering::Acquire) {
            return;
        }
        self.notify.notified().await;
    }

    fn kill_connection(&self) {
        self.kill_flag.store(true, Ordering::Release);
        self.connection_kill_generation
            .fetch_add(1, Ordering::AcqRel);
        self.query_cancel_generation.fetch_add(1, Ordering::AcqRel);
        self.notify.notify_waiters();
        // Broadcast to current I/O waiters and retain a permit for the gap
        // before the next command read registers. Keep registration off the
        // ordinary command path; only connection KILL is sticky.
        self.notify.notify_one();
    }

    fn cancel_query(&self) {
        self.query_cancel_generation.fetch_add(1, Ordering::AcqRel);
        // Also kick waiters so any blocking I/O `select!` on the session
        // (e.g. a long streaming result write that's hit TCP
        // backpressure) sees the kill request right away. Cheap: at most
        // one tokio::Notify wake per KILL QUERY call, not per row.
        self.notify.notify_waiters();
    }

    /// Tag the session's `STATE` column for `SHOW PROCESSLIST` /
    /// `information_schema.processlist`. Called from the registry when
    /// `KILL` lands; cleared on the next `mark_busy` / `mark_idle`.
    pub fn mark_killed_state(&self) {
        let mut entry = self.entry.lock();
        entry.killed = true;
        Self::recompute_effective_state(&mut entry);
    }

    /// Hot-path transition into a busy state. The session calls this
    /// once per COM_QUERY / COM_STMT_EXECUTE / COM_STMT_PREPARE /
    /// COM_INIT_DB. Avoids allocating for the common case where
    /// `database` matches the cached value.
    pub fn mark_busy(&self, command: &'static str, info: Option<String>, database: &str) {
        let mut entry = self.entry.lock();
        entry.command = command;
        entry.info = info.map(|sql| {
            ProcessSqlText::Owned(
                if crate::engine::ai::sql::sql_may_have_ai(&sql)
                    || crate::engine::http::sensitive(&sql)
                {
                    crate::engine::http::redaction_marker(&sql, false).to_owned()
                } else {
                    sql
                },
            )
        });
        entry.killed = false;
        entry.active_stages.fill(0);
        entry.active_stage_total = 0;
        entry.base_state = Some(ProcessState::Starting);
        entry.effective_state = Some(ProcessState::Starting);
        // Coarse clock: PROCESSLIST's TIME column has second
        // resolution, and these two transitions run once per command —
        // two real clock reads per statement were visible in
        // point-select profiles.
        entry.started_at = crate::util::coarse_clock::coarse_instant();
        Self::update_database(&mut entry.database, database);
    }

    /// Hot-path transition back to idle. Called once per command at
    /// the bottom of the session command loop.
    pub fn mark_idle(&self, database: &str) {
        let mut entry = self.entry.lock();
        entry.command = "Sleep";
        entry.info = None;
        entry.killed = false;
        entry.active_stages.fill(0);
        entry.active_stage_total = 0;
        entry.base_state = None;
        entry.effective_state = None;
        entry.started_at = crate::util::coarse_clock::coarse_instant();
        Self::update_database(&mut entry.database, database);
    }

    pub(crate) fn set_base_state(&self, state: ProcessState) {
        let mut entry = self.entry.lock();
        entry.base_state = Some(state);
        if entry.killed {
            return;
        }
        if entry.active_stage_total == 0 {
            Self::set_effective_state(&mut entry, Some(state));
        } else {
            Self::recompute_effective_state(&mut entry);
        }
    }

    pub(crate) fn set_process_info(&self, info: Option<String>) {
        self.entry.lock().info = info.map(|sql| {
            ProcessSqlText::Owned(
                if crate::engine::ai::sql::sql_may_have_ai(&sql)
                    || crate::engine::http::sensitive(&sql)
                {
                    crate::engine::http::redaction_marker(&sql, false).to_owned()
                } else {
                    sql
                },
            )
        });
    }

    /// The prepared handle owns and charges the immutable SQL text. This
    /// short-lived process-list reference disappears at the idle transition.
    pub(crate) fn set_process_info_shared(&self, info: Arc<str>) {
        self.entry.lock().info = Some(ProcessSqlText::Shared(
            if crate::engine::ai::sql::sql_may_have_ai(&info)
                || crate::engine::http::sensitive(&info)
            {
                Arc::from(crate::engine::http::redaction_marker(&info, false))
            } else {
                info
            },
        ));
    }

    pub(crate) fn set_time_reference_unix_secs(&self, reference: Option<u64>) {
        self.entry.lock().time_reference_unix_secs = reference;
    }

    pub(crate) fn enter_stage(self: &Arc<Self>, state: ProcessState) -> ProcessStageGuard {
        let mut entry = self.entry.lock();
        let count = &mut entry.active_stages[state as usize];
        let first = *count == 0;
        *count = count.saturating_add(1);
        entry.active_stage_total = entry.active_stage_total.saturating_add(1);
        if first && !entry.killed {
            let current_priority = entry
                .effective_state
                .map(ProcessState::priority)
                .unwrap_or(0);
            if state.priority() > current_priority {
                Self::set_effective_state(&mut entry, Some(state));
            } else if entry
                .effective_state
                .is_some_and(|current| current.priority() == state.priority() && current != state)
            {
                // Equal-priority ties retain the stable ProcessState::ALL
                // ordering used by the full oracle.
                Self::recompute_effective_state(&mut entry);
            }
        }
        drop(entry);
        ProcessStageGuard {
            handle: Arc::clone(self),
            state,
        }
    }

    fn leave_stage(&self, state: ProcessState) {
        let mut entry = self.entry.lock();
        let index = state as usize;
        let was_active = entry.active_stages[index] != 0;
        entry.active_stages[index] = entry.active_stages[index].saturating_sub(1);
        let remaining = entry.active_stages[index];
        if was_active {
            entry.active_stage_total = entry.active_stage_total.saturating_sub(1);
        }
        if !was_active || remaining != 0 || entry.killed {
            return;
        }
        let base_keeps_state = entry
            .base_state
            .is_some_and(|base| base.priority() >= state.priority());
        if entry.effective_state == Some(state) && !base_keeps_state {
            Self::recompute_effective_state(&mut entry);
        }
    }

    fn recompute_effective_state(entry: &mut SessionEntry) {
        let next = if entry.killed {
            Some(ProcessState::Killed)
        } else {
            let active = entry
                .active_stages
                .iter()
                .enumerate()
                .filter(|(_, count)| **count != 0)
                .map(|(index, _)| ProcessState::ALL[index])
                .max_by_key(|state| state.priority());
            match (active, entry.base_state) {
                (Some(active), Some(base)) if base.priority() >= active.priority() => Some(base),
                (Some(active), _) => Some(active),
                (None, base) => base,
            }
        };
        Self::set_effective_state(entry, next);
    }

    fn set_effective_state(entry: &mut SessionEntry, next: Option<ProcessState>) {
        if next != entry.effective_state {
            entry.effective_state = next;
            entry.started_at = crate::util::coarse_clock::coarse_instant();
        }
    }

    fn update_database(current: &mut Option<String>, database: &str) {
        if database.is_empty() {
            *current = None;
        } else if current.as_deref() != Some(database) {
            *current = Some(database.to_string());
        }
    }

    /// Store the CLIENT_CONNECT_ATTRS the client sent at handshake. Called
    /// once after `register`. Surfaced via performance_schema connection
    /// attribute tables.
    pub fn set_connection_attrs(&self, attrs: std::collections::BTreeMap<String, String>) {
        self.entry.lock().connection_attrs = attrs;
    }

    /// Publish session-owned variable state for lock-bounded PFS snapshots.
    /// Writers replace one compact vector at command boundaries; readers never
    /// invent another session's values from the global defaults.
    pub fn publish_pfs_variables(
        &self,
        session_variables: Vec<(String, String)>,
        mut user_variables: Vec<(String, Value)>,
        autocommit: bool,
        isolation: String,
    ) {
        user_variables.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let mut entry = self.entry.lock();
        entry.session_variables = session_variables;
        entry.user_variables = user_variables;
        entry.pfs_autocommit = autocommit;
        entry.pfs_isolation = isolation;
    }

    /// Replace one canonical session-variable value after a single-variable
    /// `SET`.  The vector is name-sorted by the full snapshot builder, so a
    /// binary search avoids rebuilding every dynamic sysvar backend just to
    /// publish one changed value.  Missing entries are inserted defensively;
    /// this also keeps the method correct if a future registry snapshot hides
    /// a scope-specific variable.
    pub fn publish_pfs_variable(&self, name: &'static str, value: String) {
        let mut entry = self.entry.lock();
        match entry
            .session_variables
            .binary_search_by(|(candidate, _)| candidate.as_str().cmp(name))
        {
            Ok(index) => entry.session_variables[index].1 = value,
            Err(index) => entry
                .session_variables
                .insert(index, (name.to_string(), value)),
        }
    }

    pub(crate) fn publish_pfs_prepared(&self, mut prepared: Vec<PreparedStatementSnapshot>) {
        for row in &mut prepared {
            redact_ai_prepared(row);
        }
        let mut entry = self.entry.lock();
        entry
            .prepared_statements
            .retain(|row| row.statement_name.is_some());
        entry.prepared_statements.extend(prepared);
    }

    pub(crate) fn publish_pfs_prepared_statement(&self, mut prepared: PreparedStatementSnapshot) {
        redact_ai_prepared(&mut prepared);
        let mut entry = self.entry.lock();
        if let Some(current) = entry.prepared_statements.iter_mut().find(|current| {
            current.statement_id == prepared.statement_id
                && current.statement_name.is_some() == prepared.statement_name.is_some()
        }) {
            *current = prepared;
        } else {
            entry.prepared_statements.push(prepared);
        }
    }

    pub(crate) fn publish_pfs_sql_prepared(&self, mut prepared: Vec<PreparedStatementSnapshot>) {
        for row in &mut prepared {
            redact_ai_prepared(row);
        }
        let mut entry = self.entry.lock();
        entry
            .prepared_statements
            .retain(|row| row.statement_name.is_none());
        entry.prepared_statements.extend(prepared);
    }

    pub fn set_identity(
        &self,
        user: String,
        database: String,
        account: crate::engine::auth::AccountKey,
    ) {
        let mut entry = self.entry.lock();
        entry.user = user;
        entry.authenticated_as = account;
        entry.database = (!database.is_empty()).then_some(database);
        entry.command = "Sleep";
        entry.info = None;
        entry.killed = false;
        entry.active_stages.fill(0);
        entry.active_stage_total = 0;
        entry.base_state = None;
        entry.effective_state = None;
        entry.active_query_memory = None;
        entry.started_at = Instant::now();
        self.session_memory_peak.store(0, Ordering::Relaxed);
        self.protocol_prepared_memory_bytes
            .store(0, Ordering::Relaxed);
        self.sql_prepared_memory_bytes.store(0, Ordering::Relaxed);
        self.cte_memory_bytes.store(0, Ordering::Relaxed);
    }

    /// Record the storage transaction id this connection now owns (or `None`
    /// when it commits / rolls back). A single relaxed atomic store — no lock,
    /// no contention with other connections.
    pub fn set_current_txn(&self, txn: Option<u64>) {
        self.current_txn.store(txn.unwrap_or(0), Ordering::Relaxed);
    }

    /// Observe lazy transactions without allocating a storage transaction slot.
    /// Only the first consistent read needs to consult the clock.
    pub(crate) fn note_lazy_transaction(&self) {
        if self.lazy_trx_started_ms.load(Ordering::Relaxed) == 0 {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .max(1) as u64;
            self.lazy_trx_started_ms.store(now, Ordering::Relaxed);
        }
    }

    pub(crate) fn clear_lazy_transaction(&self) {
        self.lazy_trx_started_ms.store(0, Ordering::Relaxed);
    }

    pub(crate) fn attach_query_memory(
        &self,
        budget: &Arc<crate::engine::executor::mem_budget::MemBudget>,
    ) {
        self.entry.lock().active_query_memory = Some((budget.query_id(), Arc::downgrade(budget)));
    }

    pub(crate) fn detach_query_memory(&self, query_id: u64, peak: usize) {
        self.session_memory_peak
            .fetch_max(peak as u64, Ordering::Relaxed);
        let mut entry = self.entry.lock();
        let matches = entry
            .active_query_memory
            .as_ref()
            .is_some_and(|(active_id, _)| *active_id == query_id);
        if matches {
            entry.active_query_memory = None;
        }
    }

    pub(crate) fn set_protocol_prepared_memory_bytes(&self, bytes: u64) {
        self.protocol_prepared_memory_bytes
            .store(bytes, Ordering::Relaxed);
    }

    pub(crate) fn set_sql_prepared_memory_bytes(&self, bytes: u64) {
        self.sql_prepared_memory_bytes
            .store(bytes, Ordering::Relaxed);
    }

    pub(crate) fn set_cte_memory_bytes(&self, bytes: u64) {
        self.cte_memory_bytes.store(bytes, Ordering::Relaxed);
    }
}

impl Drop for ProcessStageGuard {
    fn drop(&mut self) {
        self.handle.leave_stage(self.state);
    }
}

impl BackgroundProcessGuard {
    pub(crate) fn id(&self) -> u32 {
        self.id
    }

    pub(crate) fn handle(&self) -> &Arc<SessionHandle> {
        &self.handle
    }
}

impl Drop for BackgroundProcessGuard {
    fn drop(&mut self) {
        self.sessions.write().remove(&self.id);
    }
}

impl SessionCancelHandle {
    /// Clone the underlying `Arc<SessionHandle>`. Used by
    /// `QueryCancelToken::from_session` to take a long-lived reference
    /// the token can share across worker threads.
    pub fn session_handle_arc(&self) -> Arc<SessionHandle> {
        Arc::clone(&self.handle)
    }

    /// Borrow the underlying `Arc<SessionHandle>` without bumping the
    /// refcount. Used by the per-command token-reuse check.
    pub fn session_handle_ref(&self) -> &Arc<SessionHandle> {
        &self.handle
    }

    pub fn connection_kill_generation(&self) -> u64 {
        self.handle.connection_kill_generation()
    }

    pub fn query_cancel_generation(&self) -> u64 {
        self.handle.query_cancel_generation()
    }

    #[cfg(test)]
    pub fn is_connection_killed_since(&self, generation: u64) -> bool {
        self.handle.is_connection_killed_since(generation)
    }

    #[cfg(test)]
    pub fn is_query_cancelled_since(&self, generation: u64) -> bool {
        self.handle.is_query_cancelled_since(generation)
    }

    pub fn set_current_txn(&self, txn: Option<u64>) {
        self.handle.set_current_txn(txn);
    }

    pub(crate) fn set_sql_prepared_memory_bytes(&self, bytes: u64) {
        self.handle.set_sql_prepared_memory_bytes(bytes);
    }

    pub(crate) fn set_cte_memory_bytes(&self, bytes: u64) {
        self.handle.set_cte_memory_bytes(bytes);
    }
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::time::Duration;

    thread_local! {
        static SNAPSHOT_AFTER_UPGRADE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
            const { std::cell::RefCell::new(None) };
    }

    pub(super) fn snapshot_budget_upgraded() {
        let hook = SNAPSHOT_AFTER_UPGRADE.with(|hook| hook.borrow_mut().take());
        if let Some(hook) = hook {
            hook();
        }
    }

    #[test]
    fn snapshot_can_release_the_last_query_budget_without_reentering_entry_lock() {
        let registry = SessionRegistry::new();
        let owner = registry.register(7, "root".into(), "localhost".into(), "test".into());
        let budget = crate::engine::executor::mem_budget::MemBudget::for_session(owner);
        let query_id = budget.query_id();
        let worker_registry = registry.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            SNAPSHOT_AFTER_UPGRADE.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || drop(budget)));
            });
            tx.send(worker_registry.snapshot()).unwrap();
        });
        let snapshot = rx.recv_timeout(Duration::from_secs(2)).expect(
            "snapshot destroyed the last query budget while holding the session entry lock",
        );
        worker.join().unwrap();
        assert_eq!(snapshot[0].query_id, Some(query_id));
        assert_eq!(registry.snapshot()[0].query_id, None);
    }

    #[test]
    fn finishing_old_budget_does_not_detach_replacement_query() {
        let registry = SessionRegistry::new();
        let owner = registry.register(7, "root".into(), "localhost".into(), "test".into());
        let old = crate::engine::executor::mem_budget::MemBudget::for_session(owner.clone());
        let current = crate::engine::executor::mem_budget::MemBudget::for_session(owner);
        let current_id = current.query_id();
        drop(old);
        assert_eq!(registry.snapshot()[0].query_id, Some(current_id));
        drop(current);
        assert_eq!(registry.snapshot()[0].query_id, None);
    }

    fn state(registry: &SessionRegistry, id: u32) -> Option<String> {
        registry
            .snapshot()
            .into_iter()
            .find(|process| process.id == id)
            .and_then(|process| process.state)
    }

    #[test]
    fn idle_columns_are_null_and_busy_starts_in_starting() {
        let registry = SessionRegistry::new();
        let handle = registry.register(7, "root".into(), "localhost:33060".into(), "".into());
        let idle = registry.snapshot().pop().unwrap();
        assert_eq!(idle.database, None);
        assert_eq!(idle.state, None);
        assert_eq!(idle.info, None);
        assert_eq!(idle.host, "localhost:33060");

        handle.mark_busy("Query", Some("SELECT 1".into()), "test");
        let busy = registry.snapshot().pop().unwrap();
        assert_eq!(busy.database.as_deref(), Some("test"));
        assert_eq!(busy.state.as_deref(), Some("starting"));
        assert_eq!(busy.info.as_deref(), Some("SELECT 1"));
    }

    #[test]
    fn nested_stages_restore_and_same_state_keeps_time_origin() {
        let registry = SessionRegistry::new();
        let handle = registry.register(7, "root".into(), "localhost".into(), "test".into());
        handle.mark_busy("Query", Some("SELECT 1".into()), "test");
        handle.set_base_state(ProcessState::Executing);
        handle.entry.lock().started_at = Instant::now() - Duration::from_secs(5);
        handle.set_base_state(ProcessState::Executing);
        assert!(registry.snapshot()[0].time_secs >= 5);

        let sorting = handle.enter_stage(ProcessState::SortingForOrder);
        assert_eq!(state(&registry, 7).as_deref(), Some("Sorting for order"));
        let sleeping = handle.enter_stage(ProcessState::UserSleep);
        assert_eq!(state(&registry, 7).as_deref(), Some("User sleep"));
        drop(sleeping);
        assert_eq!(state(&registry, 7).as_deref(), Some("Sorting for order"));
        drop(sorting);
        assert_eq!(state(&registry, 7).as_deref(), Some("executing"));
    }

    #[test]
    fn copy_stage_overrides_and_restores_alter_state() {
        let registry = SessionRegistry::new();
        let handle = registry.register(7, "root".into(), "localhost".into(), "test".into());
        handle.mark_busy(
            "Query",
            Some("ALTER TABLE t ADD COLUMN c INT".into()),
            "test",
        );
        handle.set_base_state(ProcessState::AlteringTable);

        let copying = handle.enter_stage(ProcessState::CopyToTmpTable);
        assert_eq!(state(&registry, 7).as_deref(), Some("copy to tmp table"));
        drop(copying);
        assert_eq!(state(&registry, 7).as_deref(), Some("altering table"));
    }

    #[test]
    fn operator_stage_overrides_stream_send_then_restores_it() {
        let registry = SessionRegistry::new();
        let handle = registry.register(7, "root".into(), "localhost".into(), "test".into());
        handle.mark_busy(
            "Query",
            Some("SELECT k, COUNT(*) FROM t GROUP BY k".into()),
            "test",
        );
        handle.set_base_state(ProcessState::Executing);

        let sending = handle.enter_stage(ProcessState::SendingToClient);
        assert_eq!(state(&registry, 7).as_deref(), Some("Sending to client"));
        let grouping = handle.enter_stage(ProcessState::SortingForGroup);
        assert_eq!(state(&registry, 7).as_deref(), Some("Sorting for group"));
        drop(grouping);
        assert_eq!(state(&registry, 7).as_deref(), Some("Sending to client"));
        drop(sending);
        assert_eq!(state(&registry, 7).as_deref(), Some("executing"));
    }

    #[test]
    fn equal_priority_stages_keep_oracle_tie_order_and_base_wins_ties() {
        let registry = SessionRegistry::new();
        let handle = registry.register(7, "root".into(), "localhost".into(), "test".into());
        handle.mark_busy("Query", None, "test");
        handle.set_base_state(ProcessState::Preparing);

        let executing = handle.enter_stage(ProcessState::Executing);
        assert_eq!(state(&registry, 7).as_deref(), Some("executing"));
        let updating = handle.enter_stage(ProcessState::Updating);
        assert_eq!(state(&registry, 7).as_deref(), Some("updating"));
        drop(updating);
        assert_eq!(state(&registry, 7).as_deref(), Some("executing"));
        drop(executing);
        assert_eq!(state(&registry, 7).as_deref(), Some("preparing"));

        handle.set_base_state(ProcessState::Executing);
        let update = handle.enter_stage(ProcessState::Update);
        assert_eq!(state(&registry, 7).as_deref(), Some("executing"));
        drop(update);
        assert_eq!(state(&registry, 7).as_deref(), Some("executing"));
    }

    #[test]
    fn concurrent_same_stage_uses_counter_not_stack_order() {
        let registry = SessionRegistry::new();
        let handle = registry.register(7, "root".into(), "localhost".into(), "test".into());
        handle.mark_busy("Query", None, "test");
        handle.set_base_state(ProcessState::Executing);
        let entered = Arc::new(Barrier::new(3));
        let leave = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let handle = Arc::clone(&handle);
            let entered = Arc::clone(&entered);
            let leave = Arc::clone(&leave);
            workers.push(std::thread::spawn(move || {
                let _stage = handle.enter_stage(ProcessState::SortingForOrder);
                entered.wait();
                leave.wait();
            }));
        }
        entered.wait();
        assert_eq!(state(&registry, 7).as_deref(), Some("Sorting for order"));
        leave.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(state(&registry, 7).as_deref(), Some("executing"));
    }

    #[test]
    fn killed_cannot_be_overwritten_by_late_stage_restore() {
        let registry = SessionRegistry::new();
        let handle = registry.register(7, "root".into(), "localhost".into(), "test".into());
        handle.mark_busy("Query", None, "test");
        handle.set_base_state(ProcessState::Executing);
        let stage = handle.enter_stage(ProcessState::SortingForOrder);
        handle.mark_killed_state();
        drop(stage);
        assert_eq!(state(&registry, 7).as_deref(), Some("Killed"));
        handle.mark_idle("test");
        assert_eq!(state(&registry, 7), None);
    }

    #[test]
    fn reserved_thread_ids_survive_wrap_before_registration() {
        let registry = SessionRegistry::new();
        let first = registry.allocate_thread_id();
        registry.next_thread_id.store(first, Ordering::Relaxed);
        assert_ne!(registry.allocate_thread_id(), first);
        registry.unregister(first);
        registry.next_thread_id.store(first, Ordering::Relaxed);
        assert_eq!(registry.allocate_thread_id(), first);
    }

    #[tokio::test]
    async fn connection_kill_before_wait_is_not_lost() {
        let registry = SessionRegistry::new();
        let handle = registry.register(7, "root".into(), "localhost".into(), "test".into());
        handle.kill_connection();
        // Covers a kill between the flag check and polling the future.
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            handle.notify.notified(),
        )
        .await
        .unwrap();
        tokio::time::timeout(std::time::Duration::from_millis(100), handle.notified())
            .await
            .unwrap();
    }

    #[test]
    fn background_ids_are_unique_and_guard_unregisters() {
        let registry = SessionRegistry::new();
        let foreground_id = registry.allocate_thread_id();
        registry.register(
            foreground_id,
            "root".into(),
            "localhost:33060".into(),
            "".into(),
        );
        let background = registry
            .register_background(
                "event_scheduler".into(),
                "localhost".into(),
                None,
                "Daemon",
                ProcessState::WaitingForNextActivation,
                None,
            )
            .expect("test background observation should fit memory budget");
        let background_id = background.id();
        assert_ne!(foreground_id, background_id);
        assert_eq!(registry.snapshot().len(), 2);
        assert!(background.handle._background_memory_charge.is_some());
        drop(background);
        assert_eq!(registry.snapshot().len(), 1);
    }

    #[test]
    fn kill_unknown_thread_returns_mysql_error() {
        let registry = SessionRegistry::new();
        let err = registry.kill_connection(7).unwrap_err();
        assert_eq!(err.code(), 1094);
        assert_eq!(err.sql_state(), "HY000");
    }

    #[test]
    fn query_and_connection_kill_generations_are_distinct() {
        let registry = SessionRegistry::new();
        let handle = registry.register(7, "root".into(), "localhost".into(), "test".into());
        let connection_generation = handle.connection_kill_generation();
        let query_generation = handle.query_cancel_generation();

        registry.kill_query(7).unwrap();
        assert!(!handle.is_connection_killed_since(connection_generation));
        assert!(handle.is_query_cancelled_since(query_generation));

        let query_generation = handle.query_cancel_generation();
        registry.kill_connection(7).unwrap();
        assert!(handle.is_connection_killed_since(connection_generation));
        assert!(handle.is_query_cancelled_since(query_generation));
        assert!(handle.kill_flag().load(Ordering::Acquire));
    }
}

#[cfg(test)]
#[path = "session_registry/status_tests.rs"]
mod status_tests;

fn redact_ai_prepared(row: &mut PreparedStatementSnapshot) {
    if crate::engine::ai::sql::sql_may_have_ai(&row.sql_text)
        || crate::engine::http::sensitive(&row.sql_text)
    {
        row.sql_text = Arc::from(crate::engine::http::redaction_marker(&row.sql_text, false));
    }
}
