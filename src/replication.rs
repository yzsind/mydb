use crate::gtid::{
    append_gtid, format_gtid_interval_map, gtid_set_contains, parse_gtid_interval_map,
};
#[cfg(test)]
use crate::replication_sql::extract_quoted_string;
use crate::replication_sql::{
    parse_show_relaylog_events_clause, show_relaylog_events_columns, ReplicationShow,
};
pub use crate::replication_types::{
    ReplThreadError, ReplicationChannelConfig, ReplicationSslMode, ReplicationStatus,
};
use std::cell::Cell;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::NaiveDate;
use mysql::prelude::Queryable;
use mysql_common::binlog::{consts::BinlogVersion, EventStreamReader};
use serde::{Deserialize, Serialize};

use crate::engine::{QueryEngine, QueryResponse, ResultSet, SessionContext, StorageQueryEngine};
use crate::parser::Statement;
use crate::storage::{
    error, value_to_string, DataTypeKind, Database, DeferredCommit, RowId, TableSchema, TxnView,
};
use crate::types::{QueryError, Value};
use crate::util::logging::log_message;

const DEFAULT_CHANNEL: &str = "default";
const DEFAULT_REPL_PORT: u16 = 3306;
const DEFAULT_REPL_SERVER_ID: u32 = 9001;
const DEFAULT_CONNECT_RETRY_MS: u64 = 1000;
const DEFAULT_RELAY_SYNC_TXNS: u64 = 32;
const DEFAULT_RELAY_SYNC_INTERVAL_MS: u64 = 100;
const DEFAULT_PROGRESS_SYNC_TXNS: u64 = 64;
const DEFAULT_PROGRESS_SYNC_INTERVAL_MS: u64 = 100;
const DEFAULT_APPLY_BATCH_TXNS: u64 = 16;
const DEFAULT_APPLY_BATCH_BYTES: u64 = 4 * 1024 * 1024;

static GLOBAL_MANAGER: OnceLock<Arc<ReplicationManager>> = OnceLock::new();
include!("replication_state.rs");
include!("replication_apply_state.rs");

thread_local! {
    /// Set for the duration of crash-recovery relay replay so apply is
    /// forced idempotent even when the configured mode is STRICT.
    static RECOVERY_IDEMPOTENT: Cell<bool> = const { Cell::new(false) };
}

/// True when apply should skip (rather than error on) idempotent
/// re-application: either `slave_exec_mode=IDEMPOTENT` or we are inside
/// crash-recovery relay replay.
pub(crate) fn apply_is_idempotent() -> bool {
    SLAVE_EXEC_MODE.load(Ordering::Relaxed) == 1 || RECOVERY_IDEMPOTENT.with(|c| c.get())
}

fn with_recovery_idempotent<T>(f: impl FnOnce() -> T) -> T {
    crate::util::scoped_cell::with_value(&RECOVERY_IDEMPOTENT, true, f)
}

impl Default for ReplicationChannelConfig {
    fn default() -> Self {
        Self {
            channel: DEFAULT_CHANNEL.to_string(),
            source_host: String::new(),
            source_port: DEFAULT_REPL_PORT,
            source_user: "root".to_string(),
            source_password: String::new(),
            server_id: DEFAULT_REPL_SERVER_ID,
            source_log_file: String::new(),
            source_log_pos: 4,
            auto_position: false,
            connect_retry_ms: DEFAULT_CONNECT_RETRY_MS,
            do_databases: Vec::new(),
            ignore_databases: Vec::new(),
            do_tables: Vec::new(),
            ignore_tables: Vec::new(),
            wild_do_tables: Vec::new(),
            wild_ignore_tables: Vec::new(),
            ssl_mode: ReplicationSslMode::default(),
            ssl_ca: None,
            ssl_cert: None,
            ssl_key: None,
            ssl_cipher: None,
            source_delay_secs: 0,
        }
    }
}

impl ReplicationChannelConfig {
    pub fn from_env() -> Option<Self> {
        let source_host = crate::util::env::var("MYDB_REPL_SOURCE_HOST").ok()?;
        let mut cfg = Self {
            source_host,
            source_port: env_parse("MYDB_REPL_SOURCE_PORT", DEFAULT_REPL_PORT),
            source_user: crate::util::env::var("MYDB_REPL_USER")
                .unwrap_or_else(|_| "root".to_string()),
            source_password: crate::util::env::var("MYDB_REPL_PASSWORD").unwrap_or_default(),
            server_id: env_parse("MYDB_REPL_SERVER_ID", DEFAULT_REPL_SERVER_ID),
            source_log_file: crate::util::env::var("MYDB_REPL_SOURCE_LOG_FILE").unwrap_or_default(),
            source_log_pos: env_parse("MYDB_REPL_SOURCE_LOG_POS", 4),
            auto_position: env_bool("MYDB_REPL_AUTO_POSITION"),
            connect_retry_ms: env_parse("MYDB_REPL_CONNECT_RETRY_MS", DEFAULT_CONNECT_RETRY_MS),
            channel: crate::util::env::var("MYDB_REPL_CHANNEL")
                .unwrap_or_else(|_| DEFAULT_CHANNEL.to_string()),
            ..Self::default()
        };
        cfg.do_databases = env_csv("MYDB_REPL_DO_DB");
        cfg.ignore_databases = env_csv("MYDB_REPL_IGNORE_DB");
        if let Ok(mode) = crate::util::env::var("MYDB_REPL_SSL_MODE") {
            if let Some(m) = ReplicationSslMode::from_label(&mode) {
                cfg.ssl_mode = m;
            }
        }
        cfg.ssl_ca = crate::util::env::var("MYDB_REPL_SSL_CA")
            .ok()
            .filter(|s| !s.is_empty());
        cfg.ssl_cert = crate::util::env::var("MYDB_REPL_SSL_CERT")
            .ok()
            .filter(|s| !s.is_empty());
        cfg.ssl_key = crate::util::env::var("MYDB_REPL_SSL_KEY")
            .ok()
            .filter(|s| !s.is_empty());
        cfg.ssl_cipher = crate::util::env::var("MYDB_REPL_SSL_CIPHER")
            .ok()
            .filter(|s| !s.is_empty());
        Some(cfg)
    }

    fn is_configured(&self) -> bool {
        !self.source_host.is_empty()
    }

    fn source_label(&self) -> String {
        format!("{}:{}", self.source_host, self.source_port)
    }
}

/// A replication thread error captured with its MySQL error code and the wall
/// clock time it occurred, so `SHOW REPLICA STATUS` can surface a real
/// `Last_*_Errno` / `Last_*_Error_Timestamp` instead of a hardcoded `0`.
impl ReplThreadError {
    /// Format the timestamp the way MySQL renders `Last_*_Error_Timestamp`
    /// (`YYMMDD HH:MM:SS`). Empty string when never set.
    fn timestamp_text(&self) -> String {
        use chrono::{TimeZone, Utc};
        Utc.timestamp_opt(self.at_unix_secs as i64, 0)
            .single()
            .map(|dt| dt.format("%y%m%d %H:%M:%S").to_string())
            .unwrap_or_default()
    }
}

impl ReplicationStatus {
    fn from_config(cfg: &ReplicationChannelConfig, data_dir: &Path) -> Self {
        let relay = relay_log_path(data_dir, &cfg.channel);
        Self {
            channel: cfg.channel.clone(),
            configured: cfg.is_configured(),
            running: false,
            io_state: "stopped".to_string(),
            sql_state: "stopped".to_string(),
            last_error: None,
            source_host: cfg.source_host.clone(),
            source_port: cfg.source_port,
            source_log_file: cfg.source_log_file.clone(),
            source_log_pos: cfg.source_log_pos,
            relay_log_file: relay.to_string_lossy().into_owned(),
            relay_log_pos: 0,
            retrieved_gtid_set: String::new(),
            executed_gtid_set: String::new(),
            received_events: 0,
            applied_events: 0,
            applied_transactions: 0,
            last_event_timestamp: 0,
            seconds_behind_source: None,
            sql_delay_until: None,
            binlog_checksum_failures: 0,
            filtered_events: 0,
            last_io_error: None,
            last_sql_error: None,
            sql_running: false,
            exec_source_log_file: cfg.source_log_file.clone(),
            exec_source_log_pos: cfg.source_log_pos,
            source_server_id: 0,
            source_uuid: String::new(),
            count_received_heartbeats: 0,
            last_heartbeat_at_unix_secs: 0,
        }
    }

    /// MySQL 8.0 `SHOW REPLICA STATUS` column layout — 60 fields in the
    /// documented order. Columns mydb doesn't yet track ship as NULL /
    /// empty string / 0 (the same shape MySQL uses for unconfigured
    /// channels) so orchestrator / mha / mysqldump consume the result
    /// without re-keying by ordinal.
    ///
    /// Build the 60-column row using both the live `ReplicationStatus`
    /// and the channel's `ReplicationChannelConfig` (source user / port
    /// / connect-retry / auto-position / source-delay / SSL fields all
    /// live on the config, not the status).
    fn row(&self, cfg: &ReplicationChannelConfig) -> Vec<Value> {
        let yes_no = |b: bool| Value::String(if b { "Yes" } else { "No" }.to_string());
        // MySQL splits IO-thread vs SQL-thread errors. mydb classifies each
        // failure by the phase it occurred in (connect/read/parse = IO,
        // apply = SQL) and surfaces the real MySQL error code + timestamp.
        let io_errno = self
            .last_io_error
            .as_ref()
            .map(|e| e.code as u64)
            .unwrap_or(0);
        let io_error_msg = self
            .last_io_error
            .as_ref()
            .map(|e| e.message.clone())
            .unwrap_or_default();
        let io_error_ts = self
            .last_io_error
            .as_ref()
            .map(|e| e.timestamp_text())
            .unwrap_or_default();
        let sql_errno = self
            .last_sql_error
            .as_ref()
            .map(|e| e.code as u64)
            .unwrap_or(0);
        let sql_error_msg = self
            .last_sql_error
            .as_ref()
            .map(|e| e.message.clone())
            .unwrap_or_default();
        let sql_error_ts = self
            .last_sql_error
            .as_ref()
            .map(|e| e.timestamp_text())
            .unwrap_or_default();
        // `Last_Errno`/`Last_Error` (cols 19/20) mirror the SQL thread in
        // MySQL, falling back to the most recent error of either kind.
        let (last_errno, last_err) = if self.last_sql_error.is_some() {
            (sql_errno, sql_error_msg.clone())
        } else {
            (io_errno, io_error_msg.clone())
        };
        // env-driven filter rules are global (not per-channel) in mydb; pull
        // them so SHOW REPLICA STATUS reflects what the IO thread actually
        // applies, instead of always reporting empty filters.
        // Filter columns must reflect the *effective* configuration
        // (`cfg.*`, which is what `filter_allows_*` actually consult), not the
        // process env — a `CHANGE ... REPLICATE_DO_DB=(...)` set through SQL
        // was previously invisible here, and table/wildcard filters were never
        // shown at all.
        let csv = |v: &[String]| Value::String(v.join(","));
        // `Relay_Log_Space` = on-disk size of the channel's relay log. mydb's
        // relay log is a single JSON-Lines file per channel, so its byte length
        // is the total space consumed by retrieved-but-unpurged events.
        let relay_log_space = std::fs::metadata(&self.relay_log_file)
            .map(|m| m.len())
            .unwrap_or(0);
        vec![
            Value::String(self.io_state.clone()),    // 1  Replica_IO_State
            Value::String(self.source_host.clone()), // 2  Source_Host
            Value::String(cfg.source_user.clone()),  // 3  Source_User
            Value::UInt(self.source_port as u64),    // 4  Source_Port
            Value::UInt((cfg.connect_retry_ms / 1000).max(1)), // 5  Connect_Retry (s)
            Value::String(self.source_log_file.clone()), // 6  Source_Log_File
            Value::UInt(self.source_log_pos),        // 7  Read_Source_Log_Pos
            Value::String(self.relay_log_file.clone()), // 8  Relay_Log_File
            Value::UInt(self.relay_log_pos),         // 9  Relay_Log_Pos
            Value::String(self.exec_source_log_file.clone()), // 10 Relay_Source_Log_File
            yes_no(self.running),                    // 11 Replica_IO_Running
            yes_no(self.sql_running),                // 12 Replica_SQL_Running
            csv(&cfg.do_databases),                  // 13 Replicate_Do_DB
            csv(&cfg.ignore_databases),              // 14 Replicate_Ignore_DB
            csv(&cfg.do_tables),                     // 15 Replicate_Do_Table
            csv(&cfg.ignore_tables),                 // 16 Replicate_Ignore_Table
            csv(&cfg.wild_do_tables),                // 17 Replicate_Wild_Do_Table
            csv(&cfg.wild_ignore_tables),            // 18 Replicate_Wild_Ignore_Table
            Value::UInt(last_errno),                 // 19 Last_Errno
            Value::String(last_err.clone()),         // 20 Last_Error
            Value::UInt(0),                          // 21 Skip_Counter
            Value::UInt(self.exec_source_log_pos),   // 22 Exec_Source_Log_Pos
            Value::UInt(relay_log_space),            // 23 Relay_Log_Space
            Value::String("None".to_string()),       // 24 Until_Condition
            Value::String(String::new()),            // 25 Until_Log_File
            Value::UInt(0),                          // 26 Until_Log_Pos
            // SSL columns reflect the channel's TLS configuration instead of
            // a hard-coded "No"/empty, so tooling that checks
            // Source_SSL_Allowed sees the real state.
            yes_no(cfg.ssl_mode.requires_tls()), // 27 Source_SSL_Allowed
            Value::String(cfg.ssl_ca.clone().unwrap_or_default()), // 28 Source_SSL_CA_File
            Value::String(String::new()),        // 29 Source_SSL_CA_Path
            Value::String(cfg.ssl_cert.clone().unwrap_or_default()), // 30 Source_SSL_Cert
            Value::String(String::new()),        // 31 Source_SSL_Cipher
            Value::String(cfg.ssl_key.clone().unwrap_or_default()), // 32 Source_SSL_Key
            // MySQL reports NULL while the SQL (applier) thread is not running
            // — there is no meaningful "behind" measure without an applier.
            if self.sql_running {
                if self.running
                    && !self.source_log_file.is_empty()
                    && self.exec_source_log_file == self.source_log_file
                    && self.exec_source_log_pos >= self.source_log_pos
                {
                    Value::UInt(0)
                } else {
                    self.seconds_behind_source
                        .map(Value::UInt)
                        .unwrap_or(Value::Null)
                }
            } else {
                Value::Null
            }, // 33 Seconds_Behind_Source
            yes_no(matches!(
                cfg.ssl_mode,
                ReplicationSslMode::VerifyCa | ReplicationSslMode::VerifyIdentity
            )), // 34 Source_SSL_Verify_Server_Cert
            Value::UInt(io_errno),                     // 35 Last_IO_Errno
            Value::String(io_error_msg),               // 36 Last_IO_Error
            Value::UInt(sql_errno),                    // 37 Last_SQL_Errno
            Value::String(sql_error_msg),              // 38 Last_SQL_Error
            Value::String(String::new()),              // 39 Replicate_Ignore_Server_Ids
            Value::UInt(self.source_server_id as u64), // 40 Source_Server_Id
            Value::String(self.source_uuid.clone()),   // 41 Source_UUID
            Value::String(String::new()),              // 42 Source_Info_File
            Value::UInt(cfg.source_delay_secs),        // 43 SQL_Delay
            self.sql_delay_until
                .map(|until| Value::UInt(until.saturating_sub(unix_secs())))
                .unwrap_or(Value::Null), // 44 SQL_Remaining_Delay
            Value::String(self.sql_state.clone()),     // 45 Replica_SQL_Running_State
            Value::UInt(86400),                        // 46 Source_Retry_Count
            Value::String(String::new()),              // 47 Source_Bind
            Value::String(io_error_ts),                // 48 Last_IO_Error_Timestamp
            Value::String(sql_error_ts),               // 49 Last_SQL_Error_Timestamp
            Value::String(String::new()),              // 50 Source_SSL_Crl
            Value::String(String::new()),              // 51 Source_SSL_Crlpath
            Value::String(self.retrieved_gtid_set.clone()), // 52 Retrieved_Gtid_Set
            Value::String(self.executed_gtid_set.clone()), // 53 Executed_Gtid_Set
            Value::UInt(if cfg.auto_position { 1 } else { 0 }), // 54 Auto_Position
            Value::String(String::new()),              // 55 Replicate_Rewrite_DB
            Value::String(self.channel.clone()),       // 56 Channel_Name
            Value::String(String::new()),              // 57 Source_TLS_Version
            Value::String(String::new()),              // 58 Source_public_key_path
            Value::UInt(0),                            // 59 Get_Source_public_key
            Value::String(String::new()),              // 60 Network_Namespace
        ]
    }
}

/// 60-column layout for `SHOW REPLICA STATUS` — kept in a const slice so
/// the columns() helper and any external metadata views project the
/// exact same order.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedChannel {
    config: ReplicationChannelConfig,
    status: ReplicationStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RelayRecord {
    source_file: String,
    source_pos: u64,
    gtid: Option<String>,
    op: RelayOp,
}

/// Session-scoped state extracted from the source-side QUERY_EVENT
/// `status_vars` block. Replayed on the replica before executing the DDL
/// so semantics (sql_mode, FK/UNIQUE checks, charset, time_zone) match the
/// source's effective session at the time the statement ran.
///
/// Fields default to "leave session alone" so older relay records that
/// predate this field deserialize without semantic shift.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct DdlSessionState {
    #[serde(default)]
    sql_mode: u64,
    #[serde(default = "ddl_state_default_bool_true")]
    foreign_key_checks: bool,
    #[serde(default = "ddl_state_default_bool_true")]
    unique_checks: bool,
    #[serde(default)]
    time_zone: Option<String>,
    #[serde(default)]
    charset_client_id: Option<u16>,
    #[serde(default)]
    collation_connection_id: Option<u16>,
    #[serde(default)]
    collation_server_id: Option<u16>,
    #[serde(default = "ddl_state_default_bool_true")]
    explicit_defaults_for_timestamp: bool,
}

fn ddl_state_default_bool_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RelayOp {
    Noop,
    // Source bytes are interpreted only by the SQL thread, after preceding
    // DDL has applied. IO must not consult the replica's mutable catalog.
    RawRows {
        event_type: u8,
        fde: Vec<u8>,
        table_map: Vec<u8>,
        rows: Vec<u8>,
    },
    TxnCommit {
        event_count: u64,
        /// Source COMMIT/XID event timestamp (unix secs), used SQL-side to
        /// compute `Seconds_Behind_Source` from the *applied* transaction.
        /// `#[serde(default)]` keeps older relay logs (which lack it) loadable.
        #[serde(default)]
        commit_ts: u64,
    },
    Ddl {
        #[serde(default)]
        source_ts: u64,
        database: String,
        sql: String,
        #[serde(default)]
        session: DdlSessionState,
        /// True when this QUERY_EVENT is part of an atomic CTAS transaction.
        #[serde(default)]
        transactional: bool,
    },
    XaPrepare {
        xid: crate::xa::Xid,
        source_ts: u64,
    },
    XaFinish {
        xid: crate::xa::Xid,
        commit: bool,
        source_ts: u64,
    },
    WriteRows {
        database: String,
        table: String,
        rows: Vec<Vec<Value>>,
    },
    UpdateRows {
        database: String,
        table: String,
        rows: Vec<RowUpdate>,
    },
    DeleteRows {
        database: String,
        table: String,
        rows: Vec<Vec<Value>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RowUpdate {
    before: Vec<Value>,
    after: Vec<Value>,
}

#[derive(Debug, Default)]
struct TxnBuffer {
    gtid: Option<String>,
    ops: Vec<RelayOp>,
    event_count: u64,
    /// Timestamp (unix secs) of the most recent source event buffered into
    /// this transaction; becomes the commit marker's `commit_ts`.
    last_ts: u64,
    active: bool,
    xa_xid: Option<crate::xa::Xid>,
    xa_ended: bool,
}

struct PendingRelayCommit {
    commit: Option<DeferredCommit>,
    relay_offset: u64,
    source_file: String,
    source_pos: u64,
    gtid: Option<String>,
    event_count: u64,
    commit_ts: u64,
}

struct ChannelRuntime {
    config: Mutex<ReplicationChannelConfig>,
    status: Arc<Mutex<ReplicationStatus>>,
    /// Master stop: set by `STOP REPLICA` (no thread qualifier). Both threads
    /// check it; per-thread `io_stop`/`sql_stop` handle
    /// `STOP REPLICA IO_THREAD|SQL_THREAD`.
    stop: Arc<AtomicBool>,
    /// Per-thread stop flags for `STOP REPLICA {IO|SQL}_THREAD`. The IO thread
    /// exits on `stop || io_stop`; the SQL thread on `stop || sql_stop`. An
    /// apply error sets `sql_stop` so the SQL thread halts while the IO thread
    /// keeps buffering to the relay log (MySQL behaviour).
    io_stop: Arc<AtomicBool>,
    sql_stop: Arc<AtomicBool>,
    io_join: Mutex<Option<JoinHandle<()>>>,
    sql_join: Mutex<Option<JoinHandle<()>>>,
    /// Optional `START REPLICA UNTIL` stop condition installed by the
    /// admin. The SQL thread consults this after each transaction commit
    /// (DDL or XID) and stops itself when the condition fires.
    until: Mutex<Option<UntilCondition>>,
    flush_policy: ReplicationFlushPolicy,
    /// Cached relay-log writer. We keep the file open across events to
    /// avoid the open/close syscall pair per record (TPC-C TM = ~32k
    /// events/s would otherwise pay 64k syscalls/s). Reset on STOP / RESET
    /// REPLICA so the next start observes the latest tail.
    relay_writer: Mutex<Option<std::io::BufWriter<std::fs::File>>>,
    /// Relay durability is group-committed. Row records and commit markers
    /// may reach the page cache immediately, but SQL apply is released only
    /// after this state is fsynced and published through `relay_gate`.
    relay_sync: Mutex<RelaySyncState>,
    /// IO→SQL handoff gate. The IO thread advances `durable_offset` to the
    /// relay-log byte length it has fsynced (at a bounded group boundary)
    /// and notifies `relay_cond`; the SQL thread applies relay records up to
    /// that offset, then waits. `io_generation` bumps whenever the IO thread
    /// reconnects/resets the relay so the SQL reader can re-seek.
    relay_gate: Mutex<RelayGate>,
    relay_cond: Condvar,
    /// Last IO progress known to be covered by a relay-log fsync. Persisted
    /// channel metadata must never publish the live read position past this
    /// point, otherwise a crash can resume after relay bytes that never made
    /// it to durable storage.
    durable_io_progress: Mutex<DurableIoProgress>,
    /// Serialises and rate-limits the expensive channel JSON
    /// create/serialize/fsync/rename sequence shared by IO and SQL threads.
    progress_persist: Mutex<ProgressPersistState>,
    /// Last source binlog `(file, pos)` the IO thread committed to the relay
    /// at a *transaction boundary*. Distinct from the per-event
    /// `status.source_log_pos` (which advances mid-transaction to drive
    /// `Read_Source_Log_Pos`): a non-GTID reconnect must resume from the last
    /// committed boundary, never a mid-transaction offset, or the source
    /// re-streams from inside a transaction whose TABLE_MAP the fresh
    /// connection never saw → "missing TABLE_MAP id" (1235) in a loop.
    io_committed_pos: Mutex<Option<(String, u64)>>,
}

/// Which replication thread(s) a `START`/`STOP REPLICA` targets. `Both` is the
/// unqualified form; `Io`/`Sql` correspond to `IO_THREAD` / `SQL_THREAD`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplThreadSel {
    Both,
    Io,
    Sql,
}

/// Shared IO→SQL coordination state guarded by `ChannelRuntime::relay_gate`.
#[derive(Debug, Default, Clone, Copy)]
struct RelayGate {
    /// Relay-log byte length the IO thread has durably written (fsynced).
    durable_offset: u64,
    /// Bumped when the relay log is reset (RESET REPLICA) so a parked SQL
    /// reader knows to re-open from the start.
    io_generation: u64,
}

#[derive(Debug, Clone, Copy)]
struct ReplicationFlushPolicy {
    relay_sync_txns: u64,
    relay_sync_interval: Duration,
    progress_sync_txns: u64,
    progress_sync_interval: Duration,
    apply_batch_txns: u64,
    apply_batch_bytes: u64,
}

impl Default for ReplicationFlushPolicy {
    fn default() -> Self {
        Self {
            relay_sync_txns: DEFAULT_RELAY_SYNC_TXNS,
            relay_sync_interval: Duration::from_millis(DEFAULT_RELAY_SYNC_INTERVAL_MS),
            progress_sync_txns: DEFAULT_PROGRESS_SYNC_TXNS,
            progress_sync_interval: Duration::from_millis(DEFAULT_PROGRESS_SYNC_INTERVAL_MS),
            apply_batch_txns: DEFAULT_APPLY_BATCH_TXNS,
            apply_batch_bytes: DEFAULT_APPLY_BATCH_BYTES,
        }
    }
}

impl ReplicationFlushPolicy {
    fn from_env() -> Self {
        Self {
            relay_sync_txns: env_parse("MYDB_REPL_RELAY_SYNC_TXNS", DEFAULT_RELAY_SYNC_TXNS).max(1),
            relay_sync_interval: Duration::from_millis(
                env_parse(
                    "MYDB_REPL_RELAY_SYNC_INTERVAL_MS",
                    DEFAULT_RELAY_SYNC_INTERVAL_MS,
                )
                .max(1),
            ),
            progress_sync_txns: env_parse(
                "MYDB_REPL_PROGRESS_SYNC_TXNS",
                DEFAULT_PROGRESS_SYNC_TXNS,
            )
            .max(1),
            progress_sync_interval: Duration::from_millis(
                env_parse(
                    "MYDB_REPL_PROGRESS_SYNC_INTERVAL_MS",
                    DEFAULT_PROGRESS_SYNC_INTERVAL_MS,
                )
                .max(1),
            ),
            apply_batch_txns: env_parse("MYDB_REPL_APPLY_BATCH_TXNS", DEFAULT_APPLY_BATCH_TXNS)
                .max(1),
            apply_batch_bytes: env_parse("MYDB_REPL_APPLY_BATCH_BYTES", DEFAULT_APPLY_BATCH_BYTES)
                .max(1),
        }
    }
}

#[derive(Debug)]
struct RelaySyncState {
    last_sync: Instant,
    pending_boundaries: u64,
    latest_source_file: String,
    latest_source_pos: u64,
    /// Byte offset of the last complete transaction, excluding any following rows.
    latest_boundary_offset: u64,
    pending_gtids: Vec<String>,
}

impl Default for RelaySyncState {
    fn default() -> Self {
        Self {
            last_sync: Instant::now(),
            pending_boundaries: 0,
            latest_source_file: String::new(),
            latest_source_pos: 4,
            latest_boundary_offset: 0,
            pending_gtids: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct DurableIoProgress {
    source_file: String,
    source_pos: u64,
    retrieved_gtid_set: String,
}

impl DurableIoProgress {
    fn from_status(status: &ReplicationStatus) -> Self {
        Self {
            source_file: status.source_log_file.clone(),
            source_pos: status.source_log_pos,
            retrieved_gtid_set: status.retrieved_gtid_set.clone(),
        }
    }
}

#[derive(Debug)]
struct ProgressPersistState {
    last_sync: Instant,
    pending_updates: u64,
}

impl Default for ProgressPersistState {
    fn default() -> Self {
        Self {
            last_sync: Instant::now(),
            pending_updates: 0,
        }
    }
}

/// Conditions admissible after `START REPLICA UNTIL`. Each variant captures
/// a different MySQL clause; the apply loop checks them after every
/// transaction commit. `Position` and `SqlBeforeGtids` halt **before** the
/// next event past the boundary; `SqlAfterGtids` halts once every named GTID
/// has been applied.
#[derive(Debug, Clone)]
pub enum UntilCondition {
    Position { file: String, pos: u64 },
    SqlBeforeGtids(String),
    SqlAfterGtids(String),
    SqlAfterMtsGaps,
}

pub struct ReplicationManager {
    data_dir: PathBuf,
    engine: Arc<StorageQueryEngine>,
    channels: Mutex<HashMap<String, Arc<ChannelRuntime>>>,
}

pub fn init_global(
    data_dir: impl Into<PathBuf>,
    engine: Arc<StorageQueryEngine>,
) -> Arc<ReplicationManager> {
    let data_dir = data_dir.into();
    let manager = GLOBAL_MANAGER.get_or_init(|| {
        Arc::new(ReplicationManager {
            data_dir,
            engine,
            channels: Mutex::new(HashMap::new()),
        })
    });
    Arc::clone(manager)
}

pub fn global_manager() -> Option<Arc<ReplicationManager>> {
    GLOBAL_MANAGER.get().map(Arc::clone)
}

pub fn enforce_replica_read_only(statement: &Statement) -> Result<(), QueryError> {
    if !replica_read_only()
        || replication_applier_active()
        || !is_replica_write_statement(statement)
    {
        return Ok(());
    }
    Err(error(crate::error::catalog::ER_OPTION_PREVENTS_STATEMENT,
        "mydb is running with mydb_replica_read_only=ON; stop replication or SET GLOBAL mydb_replica_read_only=0 before local writes",
    ))
}

pub fn maybe_start_from_env(manager: &Arc<ReplicationManager>) -> Result<(), QueryError> {
    // Restore channels configured via SQL in a prior run so they are visible
    // to SHOW REPLICA STATUS and (unless skip_replica_start) resume applying —
    // MySQL's skip_replica_start=OFF default. Environment configuration only
    // bootstraps channels that have never been persisted or explicitly removed.
    manager.restore_persisted_channels();

    let mut started = false;
    if let Some(cfg) = ReplicationChannelConfig::from_env() {
        if channel_tombstone(&manager.data_dir, &cfg.channel).exists()
            || manager
                .channels
                .lock()
                .map(|c| c.contains_key(&cfg.channel))
                .unwrap_or(false)
        {
            return Ok(());
        }
        set_replica_read_only(env_bool_default("MYDB_REPLICA_READ_ONLY", true));
        manager.change_source(cfg.clone())?;
        if env_bool("MYDB_REPL_ENABLE") || env_bool("MYDB_REPL_START") {
            manager.start_channel(&cfg.channel)?;
            started = true;
        }
    }
    if !started && env_bool("MYDB_REPL_START") {
        manager.start_channel(DEFAULT_CHANNEL)?;
    }
    Ok(())
}

/// Startup-only equivalent of MySQL `skip_replica_start`: when set, persisted
/// channels are loaded into memory (so they report in SHOW REPLICA STATUS) but
/// not auto-started. Default OFF (auto-resume), matching MySQL.
fn skip_replica_start() -> bool {
    env_bool("MYDB_SKIP_REPLICA_START") || env_bool("MYDB_REPL_NO_AUTOSTART")
}

impl ReplicationManager {
    /// Scan the replication directory for persisted `<channel>.json` metadata
    /// and load each channel into memory (so it reports in SHOW REPLICA
    /// STATUS after a restart) and, unless `skip_replica_start`, resume its
    /// threads. Best-effort: a corrupt/partial metadata file is logged and
    /// skipped rather than aborting startup.
    pub fn restore_persisted_channels(&self) {
        let dir = replication_dir(&self.data_dir).join("channels");
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => return, // no replication dir yet — nothing to restore
        };
        let skip_start = skip_replica_start();
        for entry in entries.flatten() {
            let path = entry.path();
            // Only `<channel>.json` (skip relay logs, temp files, etc.).
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let Some(channel) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            if channel_tombstone(&self.data_dir, channel).exists() {
                continue;
            }
            let channel = channel.to_string();
            // Already loaded (e.g. env-config)? Skip.
            if self
                .channels
                .lock()
                .map(|c| c.contains_key(&channel))
                .unwrap_or(false)
            {
                continue;
            }
            let runtime = match self.ensure_channel(&channel) {
                Ok(runtime) => runtime,
                Err(err) => {
                    log_message(&format!(
                        "replication: skipping unloadable channel '{channel}': {}",
                        err.message()
                    ));
                    continue;
                }
            };
            let configured = runtime
                .config
                .lock()
                .map(|c| c.is_configured())
                .unwrap_or(false);
            if configured && !skip_start {
                if env_bool_default("MYDB_REPLICA_READ_ONLY", true) {
                    set_replica_read_only(true);
                }
                if let Err(err) = self.start_channel(&channel) {
                    log_message(&format!(
                        "replication: failed to auto-start channel '{channel}': {}",
                        err.message()
                    ));
                }
            }
        }
    }

    /// The current configuration for `channel` to use as the merge base of an
    /// incremental `CHANGE REPLICATION SOURCE TO`: the live in-memory config
    /// if the channel exists, else the persisted metadata, else a fresh
    /// default carrying the channel name.
    pub fn base_config_for_change(&self, channel: &str) -> ReplicationChannelConfig {
        if let Ok(channels) = self.channels.lock() {
            if let Some(runtime) = channels.get(channel) {
                if let Ok(cfg) = runtime.config.lock() {
                    return cfg.clone();
                }
            }
        }
        if let Ok(persisted) = self.load_channel(channel) {
            if persisted.config.is_configured() {
                return persisted.config;
            }
        }
        ReplicationChannelConfig {
            channel: channel.to_string(),
            ..ReplicationChannelConfig::default()
        }
    }

    pub fn change_source(&self, cfg: ReplicationChannelConfig) -> Result<(), QueryError> {
        fs::create_dir_all(replication_dir(&self.data_dir))
            .map_err(|err| io_error("create replication directory", err))?;
        if cfg.is_configured() && env_bool_default("MYDB_REPLICA_READ_ONLY", true) {
            set_replica_read_only(true);
        }
        let channel = cfg.channel.clone();
        let runtime = {
            let mut channels = self
                .channels
                .lock()
                .map_err(|_| poisoned_lock("replication manager lock poisoned"))?;
            channels
                .entry(channel.clone())
                .or_insert_with(|| {
                    Arc::new(ChannelRuntime {
                        status: Arc::new(Mutex::new(ReplicationStatus::from_config(
                            &cfg,
                            &self.data_dir,
                        ))),
                        config: Mutex::new(cfg.clone()),
                        stop: Arc::new(AtomicBool::new(false)),
                        io_stop: Arc::new(AtomicBool::new(false)),
                        sql_stop: Arc::new(AtomicBool::new(false)),
                        io_join: Mutex::new(None),
                        sql_join: Mutex::new(None),
                        until: Mutex::new(None),
                        flush_policy: ReplicationFlushPolicy::from_env(),
                        relay_writer: Mutex::new(None),
                        relay_sync: Mutex::new(RelaySyncState::default()),
                        relay_gate: Mutex::new(RelayGate::default()),
                        relay_cond: Condvar::new(),
                        durable_io_progress: Mutex::new(DurableIoProgress::from_status(
                            &ReplicationStatus::from_config(&cfg, &self.data_dir),
                        )),
                        progress_persist: Mutex::new(ProgressPersistState::default()),
                        io_committed_pos: Mutex::new(None),
                    })
                })
                .clone()
        };
        // MySQL 8.0.27+ semantics: CHANGE REPLICATION SOURCE requires the
        // affected replication threads to be stopped. Applying a new host /
        // user / auto_position / SSL config while the IO thread runs on an
        // already-open connection is a classic silent-config-drift trap
        // (monitoring shows the new config; the running thread stays on the
        // old one until it happens to reconnect). Report `ER_REPLICA_CHANNEL_MUST_STOP`
        // (3081) so orchestrator/HA scripts notice and issue STOP first.
        let io_running = runtime
            .io_join
            .lock()
            .map(|slot| slot.as_ref().is_some_and(|h| !h.is_finished()))
            .unwrap_or(false);
        let sql_running = runtime
            .sql_join
            .lock()
            .map(|slot| slot.as_ref().is_some_and(|h| !h.is_finished()))
            .unwrap_or(false);
        if io_running || sql_running {
            let mut parts = Vec::with_capacity(2);
            if io_running {
                parts.push("IO");
            }
            if sql_running {
                parts.push("SQL");
            }
            return Err(error(crate::error::catalog::ER_REPLICA_CHANNEL_MUST_STOP,
                format!(
                    "This operation cannot be performed with a running replication thread ({}); run STOP REPLICA first (channel '{}')",
                    parts.join("/"),
                    channel
                ),
            ));
        }
        // Reset old relay bytes with their offsets. Keeping the file while
        // resetting status to offset zero would replay transactions from the
        // previous source after CHANGE SOURCE.
        self.reset_channel(&channel)?;
        {
            let mut current = runtime
                .config
                .lock()
                .map_err(|_| poisoned_lock("replication config lock poisoned"))?;
            *current = cfg.clone();
        }
        {
            let mut status = runtime
                .status
                .lock()
                .map_err(|_| poisoned_lock("replication status lock poisoned"))?;
            let executed = status.executed_gtid_set.clone();
            let retrieved = status.retrieved_gtid_set.clone();
            *status = ReplicationStatus::from_config(&cfg, &self.data_dir);
            status.executed_gtid_set = executed;
            status.retrieved_gtid_set = retrieved;
        }
        if let (Ok(status), Ok(mut durable)) =
            (runtime.status.lock(), runtime.durable_io_progress.lock())
        {
            *durable = DurableIoProgress::from_status(&status);
        }
        if let Ok(mut committed) = runtime.io_committed_pos.lock() {
            *committed = None;
        }
        if let Ok(mut relay_sync) = runtime.relay_sync.lock() {
            *relay_sync = RelaySyncState::default();
        }
        if let Ok(mut progress) = runtime.progress_persist.lock() {
            *progress = ProgressPersistState::default();
        }
        self.persist_runtime(&runtime)?;
        let tombstone = channel_tombstone(&self.data_dir, &channel);
        match fs::remove_file(&tombstone) {
            Ok(()) => {
                if let Some(parent) = tombstone.parent() {
                    crate::platform::fs::sync_path(parent)
                        .map_err(|err| io_error("sync removed channel tombstone", err))?;
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(io_error("remove channel tombstone", err)),
        }
        Ok(())
    }

    pub fn start_channel(&self, channel: &str) -> Result<(), QueryError> {
        self.start_channel_with_until(channel, None)
    }

    pub fn start_channel_with_until(
        &self,
        channel: &str,
        until: Option<UntilCondition>,
    ) -> Result<(), QueryError> {
        self.start_channel_threads(channel, until, ReplThreadSel::Both)
    }

    /// Start the IO and/or SQL replication threads for a channel.
    /// `START REPLICA` starts both; `START REPLICA IO_THREAD|SQL_THREAD` starts
    /// one. The IO thread ingests the source binlog into the relay log; the SQL
    /// thread applies the relay log. They run independently.
    pub fn start_channel_threads(
        &self,
        channel: &str,
        until: Option<UntilCondition>,
        sel: ReplThreadSel,
    ) -> Result<(), QueryError> {
        let runtime = self.ensure_channel(channel)?;
        // A timed-out STOP retains its handle. Do not claim START succeeded
        // or reset shared state while that previous generation is stopping.
        for (selected, stopped, slot) in [
            (
                matches!(sel, ReplThreadSel::Both | ReplThreadSel::Io),
                &runtime.io_stop,
                &runtime.io_join,
            ),
            (
                matches!(sel, ReplThreadSel::Both | ReplThreadSel::Sql),
                &runtime.sql_stop,
                &runtime.sql_join,
            ),
        ] {
            if selected
                && stopped.load(Ordering::Acquire)
                && slot
                    .lock()
                    .map_err(|_| poisoned_lock("replication thread lock poisoned"))?
                    .as_ref()
                    .is_some_and(|worker| !worker.is_finished())
            {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "replication thread is still stopping; retry START REPLICA after it exits",
                ));
            }
        }
        // Install the UNTIL condition before the SQL thread spins up; it reads
        // it on each commit and self-stops when matched.
        {
            let mut slot = runtime
                .until
                .lock()
                .map_err(|_| poisoned_lock("replication until lock poisoned"))?;
            *slot = until;
        }
        let cfg = runtime
            .config
            .lock()
            .map_err(|_| poisoned_lock("replication config lock poisoned"))?
            .clone();
        if !cfg.is_configured() {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "replication source is not configured; use CHANGE REPLICATION SOURCE TO first",
            ));
        }
        // Any START clears the master stop; individual threads are then gated
        // by their own io_stop/sql_stop flags.
        runtime.stop.store(false, Ordering::Release);
        if let Ok(mut status) = runtime.status.lock() {
            // Clear both the legacy string error and the structured per-phase
            // errors so `SHOW REPLICA STATUS` doesn't keep reporting the
            // failure the operator has just fixed. `Last_IO_Errno` /
            // `Last_SQL_Errno` should ONLY reflect errors from the current
            // run.
            status.last_error = None;
            status.last_io_error = None;
            status.last_sql_error = None;
        }
        // Crash-recovery relay hygiene: drop any uncommitted/torn tail past
        // the last transaction boundary so the IO thread appends cleanly and
        // the SQL thread never splices a stale partial transaction with a
        // re-fetched one. Seed the durable gate to the truncated length so
        // the SQL thread doesn't busy-spin.
        //
        // **Safety guard**: truncation opens the relay file exclusively; if an
        // IO thread is already alive it holds an append-mode `BufWriter`
        // whose offset is at the *pre-truncation* end-of-file. A truncate
        // now would silently overwrite already-fsynced events on the next
        // flush and confuse the SQL thread's `applied vs durable` invariant.
        // Only run recovery when *both* thread slots are absent or finished
        // (i.e. this is a cold-start of the affected threads). If IO/SQL are
        // still running, skip recovery — the existing threads already own
        // the state.
        let io_running = runtime
            .io_join
            .lock()
            .map(|slot| slot.as_ref().is_some_and(|h| !h.is_finished()))
            .unwrap_or(false);
        let sql_running = runtime
            .sql_join
            .lock()
            .map(|slot| slot.as_ref().is_some_and(|h| !h.is_finished()))
            .unwrap_or(false);
        if !io_running && !sql_running {
            let relay_path = relay_log_path(&self.data_dir, channel);
            match recover_relay_boundary(&relay_path) {
                Ok((committed, boundary)) => {
                    if let Some(boundary) = boundary {
                        *runtime
                            .io_committed_pos
                            .lock()
                            .map_err(|_| poisoned_lock("replication position lock poisoned"))? =
                            Some((boundary.source_file.clone(), boundary.source_pos));
                        let mut progress = runtime.durable_io_progress.lock().map_err(|_| {
                            poisoned_lock("replication durable progress lock poisoned")
                        })?;
                        progress.source_file = boundary.source_file.clone();
                        progress.source_pos = boundary.source_pos;
                        let mut status = runtime
                            .status
                            .lock()
                            .map_err(|_| poisoned_lock("replication status lock poisoned"))?;
                        status.source_log_file = boundary.source_file;
                        status.source_log_pos = boundary.source_pos;
                    }
                    if let Ok(mut gate) = runtime.relay_gate.lock() {
                        gate.durable_offset = committed;
                    }
                }
                Err(err) => {
                    log_message(&format!(
                        "replication: relay recovery scan failed for channel '{channel}': {}",
                        err.message()
                    ));
                    return Err(err);
                }
            }
        }
        if matches!(sel, ReplThreadSel::Both | ReplThreadSel::Io) {
            self.spawn_io_thread(&runtime);
        }
        if matches!(sel, ReplThreadSel::Both | ReplThreadSel::Sql) {
            self.spawn_sql_thread(&runtime);
        }
        Ok(())
    }

    fn spawn_io_thread(&self, runtime: &Arc<ChannelRuntime>) {
        let mut slot = match runtime.io_join.lock() {
            Ok(slot) => slot,
            Err(_) => return,
        };
        if slot.as_ref().is_some_and(|handle| !handle.is_finished()) {
            return;
        }
        runtime.io_stop.store(false, Ordering::Release);
        if let Ok(mut status) = runtime.status.lock() {
            status.running = true;
            status.io_state = "connecting".to_string();
        }
        let runner = ChannelRunner::new(
            self.data_dir.clone(),
            Arc::clone(&self.engine),
            Arc::clone(runtime),
        );
        *slot = Some(thread::spawn(move || runner.run_io()));
    }

    fn spawn_sql_thread(&self, runtime: &Arc<ChannelRuntime>) {
        let mut slot = match runtime.sql_join.lock() {
            Ok(slot) => slot,
            Err(_) => return,
        };
        if slot.as_ref().is_some_and(|handle| !handle.is_finished()) {
            return;
        }
        runtime.sql_stop.store(false, Ordering::Release);
        if let Ok(mut status) = runtime.status.lock() {
            status.sql_running = true;
            status.sql_state = "waiting for relay log".to_string();
        }
        let runner = ChannelRunner::new(
            self.data_dir.clone(),
            Arc::clone(&self.engine),
            Arc::clone(runtime),
        );
        *slot = Some(thread::spawn(move || runner.run_sql()));
    }

    pub fn stop_channel(&self, channel: &str) -> Result<(), QueryError> {
        self.stop_channel_threads(channel, ReplThreadSel::Both)
    }

    /// Stop the IO and/or SQL replication threads for a channel. `STOP REPLICA`
    /// stops both; `STOP REPLICA IO_THREAD|SQL_THREAD` stops one, leaving the
    /// other running (e.g. stop SQL to pause apply while IO keeps buffering).
    pub fn stop_channel_threads(
        &self,
        channel: &str,
        sel: ReplThreadSel,
    ) -> Result<(), QueryError> {
        self.stop_channel_threads_inner(channel, sel, false)
    }

    fn stop_channel_threads_inner(
        &self,
        channel: &str,
        sel: ReplThreadSel,
        shutdown: bool,
    ) -> Result<(), QueryError> {
        let runtime = self.ensure_channel(channel)?;
        let stop_io = matches!(sel, ReplThreadSel::Both | ReplThreadSel::Io);
        let stop_sql = matches!(sel, ReplThreadSel::Both | ReplThreadSel::Sql);
        if matches!(sel, ReplThreadSel::Both) {
            runtime.stop.store(true, Ordering::Release);
        }
        if stop_io {
            runtime.io_stop.store(true, Ordering::Release);
        }
        if stop_sql {
            runtime.sql_stop.store(true, Ordering::Release);
        }
        // Wake the SQL thread if it is parked in relay_cond.wait_timeout.
        runtime.relay_cond.notify_all();
        // IO has a transport timeout; SQL must finish its current atomic transaction.
        // Retain any timed-out IO handle so START cannot spawn a second writer.
        if stop_io {
            {
                let mut slot = runtime
                    .io_join
                    .lock()
                    .map_err(|_| poisoned_lock("replication thread lock poisoned"))?;
                join_thread_with_timeout(
                    &mut slot,
                    if shutdown {
                        None
                    } else {
                        Some(Duration::from_secs(45))
                    },
                )
                .map_err(|message| error(crate::error::catalog::ER_GET_ERRNO, message))?;
            }
            // Flush + drop the cached relay BufWriter so the next start picks up
            // the current tail; the IO thread owns it, so only do this once IO
            // has joined.
            if let Ok(mut writer) = runtime.relay_writer.lock() {
                if let Some(mut w) = writer.take() {
                    let _ = w.flush();
                    let _ = w.get_mut().sync_data();
                }
            }
            if let Ok(mut status) = runtime.status.lock() {
                status.running = false;
                status.io_state = "stopped".to_string();
            }
        }
        if stop_sql {
            {
                let mut slot = runtime
                    .sql_join
                    .lock()
                    .map_err(|_| poisoned_lock("replication thread lock poisoned"))?;
                join_thread_with_timeout(&mut slot, None)
                    .map_err(|message| error(crate::error::catalog::ER_GET_ERRNO, message))?;
            }
            if let Ok(mut status) = runtime.status.lock() {
                status.sql_running = false;
                status.sql_state = "stopped".to_string();
            }
        }
        self.persist_runtime(&runtime)
    }

    pub fn reset_channel(&self, channel: &str) -> Result<(), QueryError> {
        self.stop_channel(channel)?;
        if relay_reclaim_path(&self.data_dir, channel).exists() {
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "interrupted relay reclaim; restart server to recover before resetting channel",
            ));
        }
        let runtime = self.ensure_channel(channel)?;
        let branch_scope = replication_property_key("xa-channel", channel, b"").into_bytes();
        if self
            .engine
            .database_for_replication()
            .list_prepared_xa()
            .iter()
            .any(|p| p.xid.format_id == REPLICA_XA_FORMAT_ID && p.xid.bqual == branch_scope)
        {
            return Err(xa_relay_error("cannot reset a channel with prepared source XA transactions; complete them on the source and let the SQL thread apply the outcome first"));
        }
        self.engine
            .database_for_replication()
            .set_replication_property(&replication_property_key("ddl", channel, b""), None)?;
        {
            let cfg = runtime
                .config
                .lock()
                .map_err(|_| poisoned_lock("replication config lock poisoned"))?
                .clone();
            let mut status = runtime
                .status
                .lock()
                .map_err(|_| poisoned_lock("replication status lock poisoned"))?;
            // MySQL 8.0 `RESET REPLICA` (no ALL) semantics: reset relay logs,
            // received position, `Retrieved_Gtid_Set`, applied/received
            // counters, and last errors — but **preserve** `@@gtid_executed`
            // (aka `Executed_Gtid_Set` in `SHOW REPLICA STATUS`). Only
            // `RESET REPLICA ALL` or `RESET MASTER` clears the executed GTID
            // history. Erasing it here was the source of a re-connect (with
            // `AUTO_POSITION=1`) requesting the *entire* source binlog from
            // GTID 1 and flooding the applier with 1062 duplicate-key errors
            // for rows we had already applied.
            let executed_gtid = status.executed_gtid_set.clone();
            *status = ReplicationStatus::from_config(&cfg, &self.data_dir);
            status.executed_gtid_set = executed_gtid;
        }
        if let (Ok(status), Ok(mut durable)) =
            (runtime.status.lock(), runtime.durable_io_progress.lock())
        {
            *durable = DurableIoProgress::from_status(&status);
        }
        if let Ok(mut committed) = runtime.io_committed_pos.lock() {
            *committed = None;
        }
        if let Ok(mut relay_sync) = runtime.relay_sync.lock() {
            *relay_sync = RelaySyncState::default();
        }
        if let Ok(mut progress) = runtime.progress_persist.lock() {
            *progress = ProgressPersistState::default();
        }
        if let Ok(mut writer) = runtime.relay_writer.lock() {
            *writer = None;
        }
        // Reset the durable gate to match the truncated relay log. Without
        // this the gate kept the *old* high `durable_offset` while the new
        // relay file restarts at 0, so `run_sql`'s `applied < durable` stayed
        // true forever and the SQL thread spun a core at 100%. Bump
        // `io_generation` so any (re)started parked reader re-seeks from the
        // start. `stop_channel` above already joined both threads, so no live
        // reader observes a torn transition.
        if let Ok(mut gate) = runtime.relay_gate.lock() {
            gate.durable_offset = 0;
            gate.io_generation = gate.io_generation.wrapping_add(1);
        }
        runtime.relay_cond.notify_all();
        let _ = fs::remove_file(relay_log_path(&self.data_dir, channel));
        self.persist_runtime(&runtime)
    }

    /// `RESET REPLICA ALL`: like `reset_channel`, but also discards the
    /// stored connection configuration — the persisted `<channel>.json`
    /// metadata file and the in-memory channel entry — so the channel is
    /// forgotten entirely (MySQL semantics). After this, `SHOW REPLICA
    /// STATUS` no longer lists the channel.
    pub fn reset_channel_all(&self, channel: &str) -> Result<(), QueryError> {
        self.reset_channel(channel)?;
        let tombstone = channel_tombstone(&self.data_dir, channel);
        crate::repl::durable::atomic_write_sync(
            &tombstone,
            &tombstone.with_extension("disabled.tmp"),
            b"RESET REPLICA ALL\n",
        )
        .map_err(|err| io_error("persist removed replication channel", err))?;
        // Drop the persisted metadata and relay log.
        let _ = fs::remove_file(channel_path(&self.data_dir, channel));
        let _ = fs::remove_file(relay_log_path(&self.data_dir, channel));
        // Drop the in-memory channel so it is no longer reported (keyed by the
        // raw channel name, matching `ensure_channel`'s insert).
        if let Ok(mut channels) = self.channels.lock() {
            channels.remove(channel);
        }
        Ok(())
    }

    pub fn show_status(&self, channel: Option<&str>) -> Result<QueryResponse, QueryError> {
        self.show_status_with_header(channel, false)
    }

    fn show_status_with_header(
        &self,
        channel: Option<&str>,
        legacy: bool,
    ) -> Result<QueryResponse, QueryError> {
        let channels = self
            .channels
            .lock()
            .map_err(|_| poisoned_lock("replication manager lock poisoned"))?;
        let mut rows = Vec::new();
        for (name, runtime) in channels.iter() {
            if channel.is_some_and(|want| want != name) {
                continue;
            }
            let cfg = runtime
                .config
                .lock()
                .map_err(|_| poisoned_lock("replication config lock poisoned"))?
                .clone();
            let status = runtime
                .status
                .lock()
                .map_err(|_| poisoned_lock("replication status lock poisoned"))?
                .clone();
            rows.push(status.row(&cfg));
        }
        Ok(QueryResponse::ResultSet(ResultSet {
            columns: ReplicationStatus::columns(legacy),
            rows,
        }))
    }

    /// Join all replication writers before the final checkpoint. A failed
    /// join is explicit and prevents RESTART; it must never masquerade as a
    /// quiescent channel or discard the only handle to a live applier.
    pub fn stop_all_channels(&self) -> Result<(), QueryError> {
        let names: Vec<String> = self
            .channels
            .lock()
            .map_err(|_| poisoned_lock("replication channels lock poisoned"))?
            .keys()
            .cloned()
            .collect();
        let mut failure = None;
        for name in names {
            if let Err(err) = self.stop_channel_threads_inner(&name, ReplThreadSel::Both, true) {
                log_message(&format!(
                    "replication: stop channel '{name}' during shutdown failed: {}",
                    err.message()
                ));
                failure.get_or_insert(err);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    /// Snapshot of every known channel for performance_schema.replication_*
    /// projection. Each entry is the live `(config, status)` pair under a
    /// single lock acquisition, so the view reflects a consistent moment.
    pub fn snapshot_all_channels(&self) -> Vec<(ReplicationChannelConfig, ReplicationStatus)> {
        let Ok(channels) = self.channels.lock() else {
            return Vec::new();
        };
        let mut out = Vec::with_capacity(channels.len());
        for runtime in channels.values() {
            let Ok(cfg) = runtime.config.lock() else {
                continue;
            };
            let Ok(status) = runtime.status.lock() else {
                continue;
            };
            out.push((cfg.clone(), status.clone()));
        }
        out
    }

    fn ensure_channel(&self, channel: &str) -> Result<Arc<ChannelRuntime>, QueryError> {
        if let Some(runtime) = self
            .channels
            .lock()
            .map_err(|_| poisoned_lock("replication manager lock poisoned"))?
            .get(channel)
            .cloned()
        {
            return Ok(runtime);
        }
        let mut persisted = self.load_channel(channel)?;
        // Thread running-state is a runtime fact, not a persisted one. A crash
        // while replicating leaves `running`/`sql_running` = true on disk;
        // clearing them on load stops `SHOW REPLICA STATUS` from reporting
        // Yes/Yes for threads that are not actually running yet (P2-8).
        persisted.status.running = false;
        persisted.status.sql_running = false;
        persisted.status.io_state = "Not started".to_string();
        persisted.status.sql_state = "Not started".to_string();
        let durable_io_progress = DurableIoProgress::from_status(&persisted.status);
        let runtime = Arc::new(ChannelRuntime {
            config: Mutex::new(persisted.config),
            status: Arc::new(Mutex::new(persisted.status)),
            stop: Arc::new(AtomicBool::new(false)),
            io_stop: Arc::new(AtomicBool::new(false)),
            sql_stop: Arc::new(AtomicBool::new(false)),
            io_join: Mutex::new(None),
            sql_join: Mutex::new(None),
            until: Mutex::new(None),
            flush_policy: ReplicationFlushPolicy::from_env(),
            relay_writer: Mutex::new(None),
            relay_sync: Mutex::new(RelaySyncState::default()),
            relay_gate: Mutex::new(RelayGate::default()),
            relay_cond: Condvar::new(),
            durable_io_progress: Mutex::new(durable_io_progress),
            progress_persist: Mutex::new(ProgressPersistState::default()),
            io_committed_pos: Mutex::new(None),
        });
        self.channels
            .lock()
            .map_err(|_| poisoned_lock("replication manager lock poisoned"))?
            .insert(channel.to_string(), Arc::clone(&runtime));
        Ok(runtime)
    }

    fn load_channel(&self, channel: &str) -> Result<PersistedChannel, QueryError> {
        recover_relay_reclaim(&self.data_dir, channel)?;
        let path = channel_path(&self.data_dir, channel);
        let bytes = fs::read(&path).map_err(|err| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("replication channel '{channel}' is not configured: {err}"),
            )
        })?;
        serde_json::from_slice(&bytes).map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!(
                    "corrupt replication channel metadata '{}': {err}",
                    path.display()
                ),
            )
        })
    }

    fn persist_runtime(&self, runtime: &ChannelRuntime) -> Result<(), QueryError> {
        let cfg = runtime
            .config
            .lock()
            .map_err(|_| poisoned_lock("replication config lock poisoned"))?
            .clone();
        let mut status = runtime
            .status
            .lock()
            .map_err(|_| poisoned_lock("replication status lock poisoned"))?
            .clone();
        // `SHOW REPLICA STATUS` exposes the live read position, which can be
        // ahead of the last relay fsync. The on-disk restart point must be
        // capped at progress whose relay bytes are known durable.
        let durable = runtime
            .durable_io_progress
            .lock()
            .map_err(|_| poisoned_lock("replication durable progress lock poisoned"))?
            .clone();
        status.source_log_file = durable.source_file;
        status.source_log_pos = durable.source_pos;
        status.retrieved_gtid_set = durable.retrieved_gtid_set;
        persist_channel(
            &self.data_dir,
            &PersistedChannel {
                config: cfg,
                status,
            },
        )
    }
}

struct ChannelRunner {
    data_dir: PathBuf,
    engine: Arc<StorageQueryEngine>,
    runtime: Arc<ChannelRuntime>,
    process_handle: Option<Arc<crate::engine::session_registry::SessionHandle>>,
}

impl ChannelRunner {
    fn new(
        data_dir: PathBuf,
        engine: Arc<StorageQueryEngine>,
        runtime: Arc<ChannelRuntime>,
    ) -> Self {
        Self {
            data_dir,
            engine,
            runtime,
            process_handle: None,
        }
    }

    /// True when the IO thread should exit (`STOP REPLICA` or `STOP REPLICA
    /// IO_THREAD`).
    fn io_should_stop(&self) -> bool {
        self.runtime.stop.load(Ordering::Acquire) || self.runtime.io_stop.load(Ordering::Acquire)
    }

    /// True when the SQL thread should exit (`STOP REPLICA` or `STOP REPLICA
    /// SQL_THREAD`, or an apply error self-stopped it).
    fn sql_should_stop(&self) -> bool {
        self.runtime.stop.load(Ordering::Acquire) || self.runtime.sql_stop.load(Ordering::Acquire)
    }

    /// IO thread: connect to the source, read the binlog stream, and write
    /// events into the relay log. It never applies — the SQL thread tails the
    /// relay log. On error it retries (keeps the channel pulling), so a
    /// downstream apply failure does not stop replication ingest.
    fn run_io(mut self) {
        let process = self.engine.session_registry().register_background(
            "system user".to_string(),
            "localhost".to_string(),
            None,
            "Connect",
            crate::engine::session_registry::ProcessState::ConnectingToSource,
            None,
        );
        self.process_handle = process.as_ref().map(|process| Arc::clone(process.handle()));
        let cfg = match self.runtime.config.lock() {
            Ok(cfg) => cfg.clone(),
            Err(_) => return,
        };
        log_message(format_args!(
            "[replication:{}] IO thread starting from {}",
            cfg.channel,
            cfg.source_label()
        ));
        loop {
            if self.io_should_stop() {
                let _ = self.sync_pending_relay(true);
                self.mark_io_stopped("stopped");
                return;
            }
            let _ = take_io_error_fatal(); // clear any stale flag before the attempt
            let result = self.run_once(&cfg);
            if let Err(err) = self.finish_io_attempt() {
                self.set_io_error(&err);
                self.runtime.io_stop.store(true, Ordering::Release);
                self.mark_io_stopped("relay cleanup failed; restart required");
                return;
            }
            match result {
                Ok(()) => {
                    if self.io_should_stop() {
                        self.mark_io_stopped("stopped");
                        return;
                    }
                }
                Err(err) => {
                    self.set_io_error(&err);
                    if io_error_is_fatal(&err, take_io_error_fatal()) {
                        // Deterministic poison-pill: reconnecting to the same
                        // source position would re-hit it forever. Stop the IO
                        // thread with the error recorded (MySQL halts the IO
                        // thread on a checksum failure / fatal precondition),
                        // rather than spinning. STOP REPLICA + a fixed source
                        // then START REPLICA resumes.
                        self.runtime.io_stop.store(true, Ordering::Release);
                        self.mark_io_stopped("stopped on fatal error");
                        return;
                    }
                    self.set_process_state(
                        crate::engine::session_registry::ProcessState::ReconnectingAfterSourceFailure,
                    );
                    let delay = Duration::from_millis(cfg.connect_retry_ms.max(100));
                    thread::sleep(delay);
                }
            }
        }
    }

    /// Seal complete transactions, then discard the interrupted transaction before
    /// accepting bytes from a fresh dump. SQL never reads beyond durable_offset.
    fn finish_io_attempt(&self) -> Result<(), QueryError> {
        self.sync_pending_relay(true)?;
        let mut slot = self
            .runtime
            .relay_writer
            .lock()
            .map_err(|_| relay_lock_error())?;
        if let Some(writer) = slot.as_mut() {
            writer
                .flush()
                .map_err(|e| io_error("flush interrupted relay", e))?;
            let durable = self
                .runtime
                .relay_gate
                .lock()
                .map_err(|_| relay_lock_error())?
                .durable_offset;
            let file = writer.get_mut();
            if file
                .metadata()
                .map_err(|e| io_error("stat interrupted relay", e))?
                .len()
                > durable
            {
                #[cfg(windows)]
                {
                    // Append-only Windows handles lack FILE_WRITE_DATA, which
                    // SetEndOfFile requires. Keep atomic append semantics on
                    // the writer and use a separate writable description for
                    // truncation under the existing relay gate/writer latch.
                    crate::platform::fs::truncate_and_sync(file, durable)
                        .map_err(|e| io_error("sync interrupted relay", e))?;
                }
                #[cfg(not(windows))]
                {
                    file.set_len(durable)
                        .map_err(|e| io_error("truncate interrupted relay", e))?;
                    file.sync_data()
                        .map_err(|e| io_error("sync interrupted relay", e))?;
                }
            }
        }
        let progress = self
            .runtime
            .durable_io_progress
            .lock()
            .map_err(|_| relay_lock_error())?
            .clone();
        *self
            .runtime
            .io_committed_pos
            .lock()
            .map_err(|_| relay_lock_error())? =
            Some((progress.source_file.clone(), progress.source_pos));
        self.update_status(|status| {
            status.source_log_file = progress.source_file;
            status.source_log_pos = progress.source_pos;
            status.retrieved_gtid_set = progress.retrieved_gtid_set;
        });
        Ok(())
    }

    /// SQL thread: tail the relay log and apply committed transactions. It
    /// starts by draining any relay records left on disk from a prior run
    /// (crash recovery), then blocks on `relay_cond` waiting for the IO thread
    /// to advance the durable offset. An apply error self-stops only the SQL
    /// thread (`sql_stop`), leaving the IO thread buffering — matching MySQL.
    fn run_sql(mut self) {
        let process = self.engine.session_registry().register_background(
            "system user".to_string(),
            "localhost".to_string(),
            None,
            "Connect",
            crate::engine::session_registry::ProcessState::ReadingRelayLog,
            None,
        );
        self.process_handle = process.as_ref().map(|process| Arc::clone(process.handle()));
        self.update_status(|status| {
            status.sql_running = true;
            status.sql_state = "recovering relay log".to_string();
        });
        // Crash recovery: drain relay records left on disk from a prior run
        // before tailing live IO writes. An error here stops the SQL thread.
        if let Err(err) = self.replay_relay_tail() {
            self.set_sql_error(&err);
            self.runtime.sql_stop.store(true, Ordering::Release);
            self.mark_sql_stopped("stopped on error");
            return;
        }
        self.update_status(|status| {
            status.sql_state = "waiting for relay log".to_string();
        });
        self.set_process_state(crate::engine::session_registry::ProcessState::WaitingForRelayLog);
        loop {
            if self.sql_should_stop() {
                self.mark_sql_stopped("stopped");
                return;
            }
            let (durable, generation) = {
                let gate = match self.runtime.relay_gate.lock() {
                    Ok(gate) => gate,
                    Err(_) => return,
                };
                (gate.durable_offset, gate.io_generation)
            };
            let applied = self.status_snapshot().relay_log_pos;
            if applied < durable {
                self.set_process_state(
                    crate::engine::session_registry::ProcessState::ReadingRelayLog,
                );
                if let Err(err) = self.sql_apply_available(durable) {
                    // Halt the SQL thread but keep the IO thread running.
                    self.set_sql_error(&err);
                    self.runtime.sql_stop.store(true, Ordering::Release);
                    self.mark_sql_stopped("stopped on error");
                    return;
                }
                if let Err(err) = self.reclaim_applied_relay(16 * 1024 * 1024) {
                    self.runtime.stop.store(true, Ordering::Release);
                    self.set_sql_error(&err);
                    self.mark_sql_stopped("relay reclaim failed; restart required");
                    return;
                }
                if self.sql_caught_up_should_stop() {
                    return;
                }
            } else {
                self.set_process_state(
                    crate::engine::session_registry::ProcessState::WaitingForRelayLog,
                );
                self.sql_wait_for_relay(generation);
            }
        }
    }

    // Reclaim an applied prefix at a durable transaction boundary. Copy only
    // when at least half is consumed, amortizing copy cost under sustained lag.
    // A durable intent makes replacement + metadata reset restartable. IO is
    // excluded through the entire operation; no post-intent bytes may append.
    fn reclaim_applied_relay(&self, minimum_bytes: u64) -> Result<(), QueryError> {
        if self.status_snapshot().relay_log_pos < minimum_bytes {
            return Ok(());
        }
        let mut writer = self
            .runtime
            .relay_writer
            .lock()
            .map_err(|_| relay_lock_error())?;
        let _progress = self
            .runtime
            .progress_persist
            .lock()
            .map_err(|_| relay_lock_error())?;
        let cfg = self
            .runtime
            .config
            .lock()
            .map_err(|_| relay_lock_error())?
            .clone();
        let Some(w) = writer.as_mut() else {
            return Ok(());
        };
        w.flush()
            .map_err(|e| io_error("flush relay for reclaim", e))?;
        let len = w
            .get_ref()
            .metadata()
            .map_err(|e| io_error("stat relay", e))?
            .len();
        let mut status = self.status_snapshot();
        let consumed = status.relay_log_pos;
        if consumed == 0
            || consumed > len
            || consumed < len / 2
            || consumed
                > self
                    .runtime
                    .relay_gate
                    .lock()
                    .map_err(|_| relay_lock_error())?
                    .durable_offset
        {
            return Ok(());
        }
        let durable = self
            .runtime
            .durable_io_progress
            .lock()
            .map_err(|_| relay_lock_error())?
            .clone();
        status.source_log_file = durable.source_file;
        status.source_log_pos = durable.source_pos;
        status.retrieved_gtid_set = durable.retrieved_gtid_set;
        status.relay_log_pos = 0;
        let persisted = PersistedChannel {
            config: cfg.clone(),
            status: status.clone(),
        };
        let marker = relay_reclaim_path(&self.data_dir, &cfg.channel);
        let retained = marker.with_extension("retained");
        let mut source = File::open(relay_log_path(&self.data_dir, &cfg.channel))
            .map_err(|e| io_error("open relay prefix", e))?;
        source
            .seek(SeekFrom::Start(consumed))
            .map_err(|e| io_error("seek relay prefix", e))?;
        let mut tail = File::create(&retained).map_err(|e| io_error("create retained relay", e))?;
        std::io::copy(&mut source, &mut tail)
            .and_then(|_| tail.sync_all())
            .map_err(|e| io_error("sync retained relay", e))?;
        sync_replication_directory(&self.data_dir)?;
        let tmp = marker.with_extension("tmp");
        let bytes = serde_json::to_vec(&persisted).map_err(|e| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("serialize relay reclaim: {e}"),
            )
        })?;
        let mut intent = File::create(&tmp).map_err(|e| io_error("create relay reclaim", e))?;
        intent
            .write_all(&bytes)
            .and_then(|_| intent.sync_all())
            .map_err(|e| io_error("sync relay reclaim", e))?;
        fs::rename(&tmp, &marker).map_err(|e| io_error("install relay reclaim", e))?;
        sync_replication_directory(&self.data_dir)?;
        // From here an error requires restart; the durable intent prevents IO
        // from accepting more bytes until recovery completes.
        if let Err(err) = recover_relay_reclaim(&self.data_dir, &cfg.channel) {
            self.runtime.stop.store(true, Ordering::Release);
            return Err(err);
        }
        // Keep a writer for retained pending transactions: heartbeat fsync and
        // reconnect cleanup must still see them even if no new event arrives.
        let retained_file = OpenOptions::new()
            .append(true)
            .open(relay_log_path(&self.data_dir, &cfg.channel))
            .map_err(|e| io_error("reopen retained relay", e))?;
        *writer = Some(std::io::BufWriter::with_capacity(64 * 1024, retained_file));
        self.runtime
            .status
            .lock()
            .map_err(|_| relay_lock_error())?
            .relay_log_pos = 0;
        let mut gate = self
            .runtime
            .relay_gate
            .lock()
            .map_err(|_| relay_lock_error())?;
        gate.durable_offset -= consumed;
        let mut sync = self
            .runtime
            .relay_sync
            .lock()
            .map_err(|_| relay_lock_error())?;
        sync.latest_boundary_offset = sync.latest_boundary_offset.saturating_sub(consumed);
        gate.io_generation = gate.io_generation.wrapping_add(1);
        Ok(())
    }

    /// Park the SQL thread until the IO thread advances the durable relay
    /// offset (or a stop is requested / the relay generation changes). Uses a
    /// short timeout so a stop flag set without a notify is still observed.
    fn sql_wait_for_relay(&self, seen_generation: u64) {
        let Ok(gate) = self.runtime.relay_gate.lock() else {
            return;
        };
        let applied = self.status_snapshot().relay_log_pos;
        if gate.durable_offset > applied
            || gate.io_generation != seen_generation
            || self.sql_should_stop()
        {
            return;
        }
        let _ = self
            .runtime
            .relay_cond
            .wait_timeout(gate, Duration::from_millis(200));
    }

    /// If a `START REPLICA UNTIL` condition fired mid-apply, the applier set
    /// the master `stop`; surface it as a clean SQL-thread stop.
    fn sql_caught_up_should_stop(&self) -> bool {
        if self.runtime.stop.load(Ordering::Acquire) {
            self.mark_sql_stopped("stopped");
            true
        } else {
            false
        }
    }

    /// Apply every durably-written relay transaction in `[relay_log_pos,
    /// durable)`. `relay_log_pos` is a byte offset into the relay log, so we
    /// seek straight to it — no rescan — and `durable` always lands on a
    /// TxnCommit/DDL record boundary, so this reads a whole number of
    /// transactions. Row records buffer until their commit marker.
    fn sql_apply_available(&self, durable: u64) -> Result<(), QueryError> {
        let path = relay_log_path(&self.data_dir, &self.status_snapshot().channel);
        if !path.exists() {
            return Ok(());
        }
        let applied = self.status_snapshot().relay_log_pos;
        if applied >= durable {
            return Ok(());
        }
        let mut file = File::open(&path).map_err(|err| io_error("open relay log", err))?;
        file.seek(SeekFrom::Start(applied))
            .map_err(|err| io_error("seek relay log", err))?;
        let mut reader = BufReader::new(file);
        let mut offset = applied;
        let mut txn_records: Vec<RelayRecord> = Vec::new();
        let mut pending_commits: Vec<PendingRelayCommit> = Vec::new();
        let mut pending_bytes = 0u64;
        let mut txn_bytes = 0u64;
        let mut buf = Vec::new();
        while offset < durable && !self.sql_should_stop() {
            buf.clear();
            let read = match reader.read_until(b'\n', &mut buf) {
                Ok(read) => read,
                Err(err) => {
                    // Commits prepared from earlier records are already
                    // visible. Finish their durability/progress before
                    // surfacing an error from a later relay record.
                    self.flush_pending_relay_commits(
                        &path,
                        &mut pending_commits,
                        &mut pending_bytes,
                    )?;
                    return Err(io_error("read relay log", err));
                }
            };
            if read == 0 {
                break;
            }
            let next_offset = offset.saturating_add(read as u64);
            // Never process a record beyond the durable boundary (a TxnCommit's
            // bytes may be on disk before the IO thread bumps `durable`).
            if next_offset > durable {
                break;
            }
            // A record with no trailing newline is a partial/torn write (crash
            // mid-append). Stop here; the source re-sends from the last durable
            // commit on reconnect.
            if !matches!(buf.last(), Some(b'\n')) {
                break;
            }
            while matches!(buf.last(), Some(b'\n' | b'\r')) {
                buf.pop();
            }
            if buf.is_empty() {
                offset = next_offset;
                continue;
            }
            txn_bytes = txn_bytes.saturating_add(read as u64);
            let record: RelayRecord = match serde_json::from_slice(&buf) {
                Ok(record) => record,
                Err(err) => {
                    self.flush_pending_relay_commits(
                        &path,
                        &mut pending_commits,
                        &mut pending_bytes,
                    )?;
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!(
                            "corrupt relay log '{}' at byte {offset}: {err}",
                            path.display()
                        ),
                    ));
                }
            };
            let boundary = matches!(
                &record.op,
                RelayOp::TxnCommit { .. }
                    | RelayOp::XaPrepare { .. }
                    | RelayOp::XaFinish { .. }
                    | RelayOp::Ddl {
                        transactional: false,
                        ..
                    }
            );
            self.dispatch_relay_record(
                record,
                &path,
                next_offset,
                &mut txn_records,
                &mut pending_commits,
                &mut pending_bytes,
                txn_bytes,
            )?;
            if boundary {
                txn_bytes = 0;
            }
            offset = next_offset;
        }
        // The durable relay window may contain fewer transactions than the
        // configured batch cap. Never leave prepared commits pending while
        // parking for the next IO handoff or honoring STOP REPLICA.
        self.flush_pending_relay_commits(&path, &mut pending_commits, &mut pending_bytes)?;
        Ok(())
    }

    /// Handle one relay record read by the SQL thread: buffer row records into
    /// `txn_records`, and on a TxnCommit/DDL boundary apply the buffered
    /// transaction, advancing the applied relay offset, exec position,
    /// executed GTID set and counters. Enforces `START REPLICA UNTIL` and
    /// `SOURCE_DELAY` at commit time (SQL-thread semantics).
    fn dispatch_relay_record(
        &self,
        record: RelayRecord,
        path: &Path,
        next_offset: u64,
        txn_records: &mut Vec<RelayRecord>,
        pending_commits: &mut Vec<PendingRelayCommit>,
        pending_bytes: &mut u64,
        txn_bytes: u64,
    ) -> Result<(), QueryError> {
        match &record.op {
            RelayOp::XaPrepare { xid, source_ts } | RelayOp::XaFinish { xid, source_ts, .. } => {
                let gtid = record
                    .gtid
                    .clone()
                    .or_else(|| txn_records.iter().find_map(|r| r.gtid.clone()));
                self.flush_pending_relay_commits(path, pending_commits, pending_bytes)?;
                if self.until_should_stop_before(gtid.as_deref()) {
                    self.stop_on_until();
                    return Ok(());
                }
                if !self.enforce_source_delay(*source_ts) {
                    return Ok(());
                }
                let duplicate = gtid.as_ref().is_some_and(|g| {
                    gtid_set_contains(&self.status_snapshot().executed_gtid_set, g)
                });
                let event_count = txn_records.len() as u64 + 1;
                if !duplicate {
                    match &record.op {
                        RelayOp::XaPrepare { .. } => {
                            self.apply_xa_prepare(xid, txn_records, &record)?
                        }
                        RelayOp::XaFinish { commit, .. } => {
                            if !txn_records.is_empty() {
                                return Err(xa_relay_error(
                                    "XA completion contains unexpected row records",
                                ));
                            }
                            self.apply_xa_finish(xid, *commit)?;
                        }
                        _ => unreachable!(),
                    }
                }
                txn_records.clear();
                if let Some(g) = &gtid {
                    self.update_status(|status| {
                        status.executed_gtid_set = append_gtid(&status.executed_gtid_set, g)
                    });
                }
                self.record_applied_boundary(
                    path,
                    next_offset,
                    &record.source_file,
                    record.source_pos,
                    event_count,
                    *source_ts,
                    true,
                )?;
                // Only retire recovery evidence after the cursor is durable.
                if matches!(&record.op, RelayOp::XaFinish { .. }) {
                    self.cleanup_xa_receipts(xid)?;
                }
                if self.until_should_stop_after(
                    &record.source_file,
                    record.source_pos,
                    gtid.as_deref(),
                ) {
                    self.stop_on_until();
                }
            }
            RelayOp::TxnCommit {
                event_count,
                commit_ts,
            } => {
                let gtid = record
                    .gtid
                    .clone()
                    .or_else(|| txn_records.iter().find_map(|r| r.gtid.clone()));
                // Pre-commit UNTIL (SqlBeforeGtids) stops before applying.
                if self.until_should_stop_before(gtid.as_deref()) {
                    self.flush_pending_relay_commits(path, pending_commits, pending_bytes)?;
                    self.stop_on_until();
                    return Ok(());
                }
                if self.source_delay_enabled() {
                    self.flush_pending_relay_commits(path, pending_commits, pending_bytes)?;
                }
                if !self.enforce_source_delay(*commit_ts) {
                    return Ok(());
                }
                let ops = txn_records.drain(..).map(|r| r.op).collect::<Vec<_>>();
                // Row events have before/after images and crash-recovery replay
                // is idempotent. DDL is not generally idempotent (for example,
                // replaying CREATE TABLE), so its applied relay position must
                // remain a synchronous restart barrier.
                let contains_ddl = ops.iter().any(|op| matches!(op, RelayOp::Ddl { .. }));
                let count = *event_count;
                if contains_ddl {
                    self.flush_pending_relay_commits(path, pending_commits, pending_bytes)?;
                    self.apply_records_at_boundary(ops, gtid.clone(), &record)?;
                    self.record_applied_boundary(
                        path,
                        next_offset,
                        &record.source_file,
                        record.source_pos,
                        count,
                        *commit_ts,
                        true,
                    )?;
                } else {
                    let duplicate_in_batch = gtid.as_ref().is_some_and(|candidate| {
                        pending_commits
                            .iter()
                            .any(|item| item.gtid.as_ref() == Some(candidate))
                    });
                    let commit_result = if duplicate_in_batch {
                        self.update_status(|status| {
                            status.sql_state = "skipped duplicate GTID".to_string()
                        });
                        Ok(None)
                    } else {
                        self.prepare_row_records(ops, gtid.as_deref())
                    };
                    let commit = match commit_result {
                        Ok(commit) => commit,
                        Err(err) => {
                            // Earlier prepared commits are already visible in
                            // MVCC. Make them durable and publish their relay
                            // progress before stopping on this transaction.
                            self.flush_pending_relay_commits(path, pending_commits, pending_bytes)?;
                            return Err(err);
                        }
                    };
                    pending_commits.push(PendingRelayCommit {
                        commit,
                        relay_offset: next_offset,
                        source_file: record.source_file.clone(),
                        source_pos: record.source_pos,
                        gtid: gtid.clone(),
                        event_count: count,
                        commit_ts: *commit_ts,
                    });
                    *pending_bytes = pending_bytes.saturating_add(txn_bytes);
                    if pending_commits.len() as u64 >= self.runtime.flush_policy.apply_batch_txns
                        || *pending_bytes >= self.runtime.flush_policy.apply_batch_bytes
                    {
                        self.flush_pending_relay_commits(path, pending_commits, pending_bytes)?;
                    }
                }
                if self.until_should_stop_after(
                    &record.source_file,
                    record.source_pos,
                    gtid.as_deref(),
                ) {
                    self.flush_pending_relay_commits(path, pending_commits, pending_bytes)?;
                    self.stop_on_until();
                }
            }
            RelayOp::Ddl {
                transactional: true,
                ..
            } => txn_records.push(record),
            RelayOp::Ddl { source_ts, .. } => {
                if !txn_records.is_empty() {
                    self.flush_pending_relay_commits(path, pending_commits, pending_bytes)?;
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "mixed DDL and row events in one relay transaction is not supported",
                    ));
                }
                let gtid = record.gtid.clone();
                if self.until_should_stop_before(gtid.as_deref()) {
                    self.flush_pending_relay_commits(path, pending_commits, pending_bytes)?;
                    self.stop_on_until();
                    return Ok(());
                }
                self.flush_pending_relay_commits(path, pending_commits, pending_bytes)?;
                if !self.enforce_source_delay(*source_ts) {
                    return Ok(());
                }
                self.apply_records_at_boundary(vec![record.op.clone()], gtid.clone(), &record)?;
                self.record_applied_boundary(
                    path,
                    next_offset,
                    &record.source_file,
                    record.source_pos,
                    1,
                    *source_ts,
                    true,
                )?;
                if self.until_should_stop_after(
                    &record.source_file,
                    record.source_pos,
                    gtid.as_deref(),
                ) {
                    self.stop_on_until();
                }
            }
            RelayOp::Noop
            | RelayOp::RawRows { .. }
            | RelayOp::WriteRows { .. }
            | RelayOp::UpdateRows { .. }
            | RelayOp::DeleteRows { .. } => txn_records.push(record),
        }
        Ok(())
    }

    fn source_delay_enabled(&self) -> bool {
        self.runtime
            .config
            .lock()
            .map(|cfg| cfg.source_delay_secs != 0)
            .unwrap_or(false)
    }

    /// Apply one row-only source transaction and publish its local commit in
    /// source order, deferring only the durability barrier and post-durable
    /// hooks so several independent commits can share one WAL fsync.
    fn prepare_row_records(
        &self,
        ops: Vec<RelayOp>,
        gtid: Option<&str>,
    ) -> Result<Option<DeferredCommit>, QueryError> {
        self.update_status(|status| status.sql_state = "applying relay transaction".to_string());
        if gtid.is_some_and(|value| {
            gtid_set_contains(&self.status_snapshot().executed_gtid_set, value)
        }) {
            self.update_status(|status| status.sql_state = "skipped duplicate GTID".to_string());
            return Ok(None);
        }
        debug_assert!(!ops.iter().any(|op| matches!(op, RelayOp::Ddl { .. })));
        let ops = ops
            .into_iter()
            .map(|op| self.decode_relay_rows(op))
            .collect::<Result<Vec<_>, _>>()?;
        let db = self.engine.database_for_replication();
        let logical_gate = crate::storage::capture::logical_apply_gate(db)?;
        let logical_apply_lease = logical_gate.read_arc();
        let txn = db.begin_txn()?;
        let result = with_replication_applier(|| apply_ops_to_storage(&self.engine, db, txn, &ops));
        match result {
            Ok(()) => db.commit_txn_deferred(txn.txn_id).map(|mut commit| {
                commit.logical_apply_lease = Some(logical_apply_lease);
                Some(commit)
            }),
            Err(err) => {
                let _ = db.rollback_txn(txn.txn_id);
                Err(err)
            }
        }
    }

    fn flush_pending_relay_commits(
        &self,
        path: &Path,
        pending: &mut Vec<PendingRelayCommit>,
        pending_bytes: &mut u64,
    ) -> Result<(), QueryError> {
        if pending.is_empty() {
            *pending_bytes = 0;
            return Ok(());
        }
        let mut commits = pending
            .iter_mut()
            .filter_map(|item| item.commit.as_mut())
            .collect::<Vec<_>>();
        self.engine
            .database_for_replication()
            .finish_deferred_commits(&mut commits)?;
        for item in pending.drain(..) {
            if let Some(gtid) = &item.gtid {
                self.update_status(|status| {
                    status.executed_gtid_set = append_gtid(&status.executed_gtid_set, gtid);
                });
            }
            self.record_applied_boundary(
                path,
                item.relay_offset,
                &item.source_file,
                item.source_pos,
                item.event_count,
                item.commit_ts,
                false,
            )?;
        }
        *pending_bytes = 0;
        Ok(())
    }

    fn record_applied_boundary(
        &self,
        path: &Path,
        relay_offset: u64,
        source_file: &str,
        source_pos: u64,
        event_count: u64,
        commit_ts: u64,
        force_persist: bool,
    ) -> Result<(), QueryError> {
        self.advance_applied(
            path,
            relay_offset,
            source_file,
            source_pos,
            None,
            force_persist,
        )?;
        self.update_status(|status| {
            status.applied_transactions = status.applied_transactions.saturating_add(1);
            status.applied_events = status.applied_events.saturating_add(event_count);
            if commit_ts != 0 {
                status.seconds_behind_source = seconds_behind(commit_ts);
            }
        });
        if commit_ts != 0 {
            if let Some(handle) = &self.process_handle {
                handle.set_time_reference_unix_secs(Some(commit_ts));
            }
        }
        Ok(())
    }

    /// Advance the SQL-thread durable progress after applying a transaction:
    /// the applied relay offset, the executed source position and retrieved
    /// GTID, persisted so a restart resumes here.
    fn advance_applied(
        &self,
        path: &Path,
        relay_offset: u64,
        source_file: &str,
        source_pos: u64,
        _gtid: Option<String>,
        force_persist: bool,
    ) -> Result<(), QueryError> {
        let previous = self.status_snapshot();
        self.update_status(|status| {
            status.relay_log_file = path.to_string_lossy().into_owned();
            status.relay_log_pos = relay_offset;
            status.exec_source_log_file = source_file.to_string();
            status.exec_source_log_pos = source_pos;
            status.sql_state = "idle".to_string();
        });
        // executed_gtid_set was advanced inside apply_records. Ordinary row
        // transactions may group metadata writes because recovery replay is
        // idempotent. DDL must publish its restart point immediately because
        // replaying arbitrary DDL is not generally idempotent.
        if force_persist {
            if let Err(err) = self.persist_runtime_force() {
                self.update_status(|status| {
                    status.relay_log_pos = previous.relay_log_pos;
                    status.exec_source_log_file = previous.exec_source_log_file;
                    status.exec_source_log_pos = previous.exec_source_log_pos;
                });
                return Err(err);
            }
        } else {
            self.persist_runtime_maybe();
        }
        Ok(())
    }

    /// IO thread: after durably writing a commit/DDL boundary to the relay log,
    /// advance the read position (`Read_Source_Log_Pos`) and persist. The
    /// retrieved GTID set was already advanced when the GTID event arrived.
    fn persist_io_progress(&self, file: &str, pos: u64, _gtid: Option<String>) {
        self.update_status(|status| {
            status.source_log_file = file.to_string();
            status.source_log_pos = pos;
        });
        // Record the committed transaction boundary for non-GTID reconnect.
        if let Ok(mut committed) = self.runtime.io_committed_pos.lock() {
            *committed = Some((file.to_string(), pos));
        }
        self.persist_runtime_maybe();
    }

    /// IO thread commit boundary (XID or `COMMIT` query). Durably writes a
    /// TxnCommit marker to the relay log — always, even for an empty
    /// transaction, so the SQL thread advances the executed GTID set for
    /// GTID-only / fully-filtered transactions — then advances IO read
    /// progress. The SQL thread picks it up via the durable-offset gate.
    fn io_commit_txn(&self, file: &str, pos: u64, txn: &mut TxnBuffer) -> Result<(), QueryError> {
        let gtid = txn.gtid.take();
        self.append_relay(
            file,
            pos,
            gtid.clone(),
            &RelayOp::TxnCommit {
                event_count: txn.event_count,
                commit_ts: txn.last_ts,
            },
        )?;
        self.persist_io_progress(file, pos, gtid);
        txn.event_count = 0;
        txn.ops.clear();
        txn.active = false;
        txn.xa_xid = None;
        txn.xa_ended = false;
        Ok(())
    }

    fn persist_runtime_maybe(&self) {
        let Ok(mut progress) = self.runtime.progress_persist.lock() else {
            return;
        };
        progress.pending_updates = progress.pending_updates.saturating_add(1);
        if progress.pending_updates < self.runtime.flush_policy.progress_sync_txns
            && progress.last_sync.elapsed() < self.runtime.flush_policy.progress_sync_interval
        {
            return;
        }
        let manager = ReplicationManager {
            data_dir: self.data_dir.clone(),
            engine: Arc::clone(&self.engine),
            channels: Mutex::new(HashMap::new()),
        };
        if manager.persist_runtime(&self.runtime).is_ok() {
            progress.pending_updates = 0;
            progress.last_sync = Instant::now();
        }
    }

    fn persist_runtime_force(&self) -> Result<(), QueryError> {
        let mut progress = self
            .runtime
            .progress_persist
            .lock()
            .map_err(|_| relay_lock_error())?;
        let manager = ReplicationManager {
            data_dir: self.data_dir.clone(),
            engine: Arc::clone(&self.engine),
            channels: Mutex::new(HashMap::new()),
        };
        manager.persist_runtime(&self.runtime)?;
        progress.pending_updates = 0;
        progress.last_sync = Instant::now();
        Ok(())
    }

    /// Common `START REPLICA UNTIL` self-stop: set the master stop so both
    /// threads wind down after the matching transaction.
    fn stop_on_until(&self) {
        self.update_status(|status| {
            status.running = false;
            status.io_state = "stopped on UNTIL condition".to_string();
            status.sql_state = "stopped on UNTIL condition".to_string();
        });
        self.runtime.stop.store(true, Ordering::Release);
    }

    fn run_once(&self, cfg: &ReplicationChannelConfig) -> Result<(), QueryError> {
        self.set_process_state(crate::engine::session_registry::ProcessState::ConnectingToSource);
        self.update_status(|status| {
            status.running = true;
            status.io_state = format!("connecting to {}", cfg.source_label());
        });
        // Read/keepalive timeouts protect the IO thread from silent TCP
        // half-open (NAT idle, firewall DROP, source crash without RST). The
        // source binlog dump requests a sub-second heartbeat below, so any
        // multi-second silence is real trouble — 30s read_timeout remains a
        // generous transport-failure bound. Without this, `stream.next()` would block
        // until the OS TCP keepalive (Linux default ~2h) and `SHOW REPLICA
        // STATUS` would keep reporting IO=Yes indefinitely.
        let mut opts = mysql::OptsBuilder::new()
            .ip_or_hostname(Some(cfg.source_host.clone()))
            .tcp_port(cfg.source_port)
            .user(Some(cfg.source_user.clone()))
            .pass(Some(cfg.source_password.clone()))
            .prefer_socket(false)
            .read_timeout(Some(std::time::Duration::from_secs(30)))
            .tcp_keepalive_time_ms(Some(10_000));
        if !cfg.ssl_mode.requires_tls() && require_secure_transport_for_replication() {
            mark_io_error_fatal();
            return Err(error(
                crate::error::catalog::ER_SECURE_TRANSPORT_REQUIRED,
                "MYDB_REPL_REQUIRE_SECURE_TRANSPORT is on but SOURCE_SSL is DISABLED",
            ));
        }
        if let Some(ssl) = build_ssl_opts(cfg)? {
            opts = opts.ssl_opts(Some(ssl));
        }
        let mut conn = mysql::Conn::new(opts).map_err(|err| {
            error(
                crate::error::catalog::CR_CONN_HOST_ERROR,
                format!("connect replication source failed: {err}"),
            )
        })?;

        // get_binlog_stream sends COM_REGISTER_SLAVE automatically. Set both
        // identity aliases before that registration (MySQL 8 and legacy sources).
        let uuid = crate::binlog::config_snapshot().server_uuid;
        conn.exec_drop("SET @replica_uuid = ?, @slave_uuid = ?", (&uuid, &uuid))
            .map_err(|err| {
                error(
                    crate::error::catalog::CR_SERVER_LOST,
                    format!("register replica UUID failed: {err}"),
                )
            })?;

        let _ = conn.query_drop("SET @master_binlog_checksum= @@global.binlog_checksum");
        let _ = conn.query_drop("SET @source_binlog_checksum= @@global.binlog_checksum");
        let heartbeat_ns = self
            .runtime
            .flush_policy
            .relay_sync_interval
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        let _ = conn.query_drop(format!(
            "SET @master_heartbeat_period= {}",
            heartbeat_ns.max(1)
        ));

        // Fail fast on a non-ROW source. mydb only replays row-based events;
        // a STATEMENT/MIXED source emits INTVAR/RAND/USER_VAR and bare
        // QUERY_EVENT DML that we cannot apply. Detecting it here yields one
        // clear error at connect time instead of an opaque mid-stream 1235
        // once the first such event arrives.
        let fmt = conn
            .query_first::<String, _>("SELECT @@global.binlog_format")
            .map_err(|err| {
                mark_io_error_fatal();
                error(
                    crate::error::catalog::CR_SERVER_LOST,
                    format!("cannot verify replication source binlog_format: {err}"),
                )
            })?;
        require_replication_source_setting("binlog_format", fmt.as_deref(), "ROW")?;

        // mydb replays FULL row images only (see design boundary: no
        // MINIMAL/NOBLOB). A MINIMAL/NOBLOB source omits unchanged columns from
        // the after-image and drops the before-image to key columns, which our
        // FULL-image apply path cannot reconstruct. Reject at connect time with
        // a clear root cause rather than corrupting rows mid-stream.
        let image = conn
            .query_first::<String, _>("SELECT @@global.binlog_row_image")
            .map_err(|err| {
                mark_io_error_fatal();
                error(
                    crate::error::catalog::CR_SERVER_LOST,
                    format!("cannot verify replication source binlog_row_image: {err}"),
                )
            })?;
        require_replication_source_setting("binlog_row_image", image.as_deref(), "FULL")?;

        // PARTIAL_JSON is accepted: update-row conversion combines JSON_DIFF
        // operations with the FULL before-image before writing the relay op.

        // Capture the source's identity so `SHOW REPLICA STATUS` reports the
        // real `Source_Server_Id` / `Source_UUID` instead of 0 / empty. Both
        // are best-effort: a source that refuses the read still replicates.
        let probed_server_id = conn
            .query_first::<u32, _>("SELECT @@global.server_id")
            .ok()
            .flatten();
        let probed_uuid = conn
            .query_first::<String, _>("SELECT @@global.server_uuid")
            .ok()
            .flatten();
        if probed_server_id.is_some() || probed_uuid.is_some() {
            self.update_status(|status| {
                if let Some(id) = probed_server_id {
                    status.source_server_id = id;
                }
                if let Some(uuid) = probed_uuid.clone() {
                    status.source_uuid = uuid;
                }
            });
        }

        let (file, pos) = self.start_position(cfg, &mut conn)?;
        {
            let mut durable = self
                .runtime
                .durable_io_progress
                .lock()
                .map_err(|_| relay_lock_error())?;
            if durable.source_file.is_empty() {
                durable.source_file = file.clone();
                durable.source_pos = pos;
            }
        }
        self.set_process_state(crate::engine::session_registry::ProcessState::RequestingBinlogDump);
        self.update_status(|status| {
            status.io_state = "requesting binlog stream".to_string();
            status.source_log_file = file.clone();
            status.source_log_pos = pos;
        });
        self.set_process_state(
            crate::engine::session_registry::ProcessState::WaitingForSourceEvent,
        );
        let request = self.build_binlog_request(cfg, &file, pos)?;
        let mut stream = conn.get_binlog_stream(request).map_err(|err| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("request binlog stream failed: {err}"),
            )
        })?;

        let mut table_maps: HashMap<u64, mysql::binlog::events::TableMapEvent<'static>> =
            HashMap::new();
        let mut txn = TxnBuffer::default();
        let mut current_file = file;
        self.update_status(|status| {
            status.io_state = "reading source binlog".to_string();
            status.sql_state = "waiting for transaction".to_string();
        });

        // Check both the master stop and the IO-thread stop so
        // `STOP REPLICA IO_THREAD` breaks the read loop on the next event.
        while !self.io_should_stop() {
            let Some(event_result) = stream.next() else {
                return Ok(());
            };
            let event = event_result.map_err(|err| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("read binlog event from source failed: {err}"),
                )
            })?;
            let header = event.header();
            let next_pos = header.log_pos() as u64;
            let event_ts = header.timestamp() as u64;
            let checksum_ok = verify_binlog_event_crc32(&event);
            self.update_status(|status| {
                status.received_events = status.received_events.saturating_add(1);
                status.source_log_file = current_file.clone();
                if next_pos != 0 {
                    status.source_log_pos = next_pos;
                }
                if event_ts != 0 {
                    status.last_event_timestamp = event_ts;
                }
                // Seconds_Behind_Source is computed SQL-side at commit (see
                // dispatch_relay_record) so it reflects *applied* lag, not IO
                // receive progress — otherwise a dead applier with a live IO
                // thread would keep reporting ~0.
                if !checksum_ok {
                    status.binlog_checksum_failures =
                        status.binlog_checksum_failures.saturating_add(1);
                }
            });
            if !checksum_ok {
                return Err(error(
                    crate::error::catalog::ER_BINLOG_FATAL_ERROR,
                    format!(
                        "binlog event CRC32 mismatch at {current_file}:{next_pos}; aborting channel"
                    ),
                ));
            }

            txn.last_ts = event_ts;
            self.set_process_state(
                crate::engine::session_registry::ProcessState::QueueingSourceEvent,
            );
            self.handle_source_event(
                &event,
                &mut current_file,
                next_pos,
                &mut table_maps,
                &mut txn,
            )?;
            self.set_process_state(
                crate::engine::session_registry::ProcessState::WaitingForSourceEvent,
            );
        }
        Ok(())
    }

    fn handle_source_event(
        &self,
        event: &mysql::binlog::events::Event,
        current_file: &mut String,
        progress_pos: u64,
        table_maps: &mut HashMap<u64, mysql::binlog::events::TableMapEvent<'static>>,
        txn: &mut TxnBuffer,
    ) -> Result<(), QueryError> {
        let Some(data) = event.read_data().map_err(|err| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("parse binlog event failed at {current_file}:{progress_pos}: {err}"),
            )
        })?
        else {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("unsupported unknown binlog event at {current_file}:{progress_pos}"),
            ));
        };

        use mysql::binlog::events::EventData;
        match data {
            EventData::FormatDescriptionEvent(_) | EventData::PreviousGtidsEvent(_) => {}
            EventData::RotateEvent(ev) => {
                if !ev.is_fake() {
                    *current_file = clean_binlog_file_name(ev.name_raw());
                    self.update_status(|status| {
                        status.source_log_file = current_file.clone();
                        status.source_log_pos = ev.position();
                    });
                }
            }
            EventData::HeartbeatEvent => {
                // A low-rate source may not fill a relay group. The requested
                // heartbeat closes that tail within the configured interval
                // and also gives throttled channel metadata a periodic flush
                // opportunity.
                self.sync_pending_relay(false)?;
                self.persist_runtime_maybe();
                // Heartbeat events do not advance any position — they only
                // prove the source-to-replica IO pipe is alive. Track count
                // and last-seen so `SHOW REPLICA STATUS` /
                // `performance_schema.replication_connection_status` reflect
                // reality (was blackholed indefinitely before).
                let now = unix_secs();
                self.update_status(|status| {
                    status.count_received_heartbeats =
                        status.count_received_heartbeats.saturating_add(1);
                    status.last_heartbeat_at_unix_secs = now;
                });
            }
            EventData::TableMapEvent(ev) => {
                table_maps.insert(ev.table_id(), ev.into_owned());
            }
            EventData::GtidEvent(ev) => {
                let gtid = format_gtid(ev.sid(), ev.gno());
                txn.gtid = Some(gtid.clone());
                self.update_status(|status| {
                    status.retrieved_gtid_set = append_gtid(&status.retrieved_gtid_set, &gtid)
                });
            }
            EventData::AnonymousGtidEvent(_) => {
                txn.gtid = None;
            }
            EventData::QueryEvent(ev) => {
                let session = ddl_session_state_from_query_event(&ev);
                let sql = match decode_query_event_sql(&ev, &session) {
                    Ok(sql) => sql.trim().to_string(),
                    Err(err) => {
                        mark_io_error_fatal();
                        return Err(err);
                    }
                };
                if sql
                    .split_ascii_whitespace()
                    .next()
                    .is_some_and(|word| word.eq_ignore_ascii_case("XA"))
                {
                    self.handle_xa_query(&sql, current_file, progress_pos, txn)?;
                    return Ok(());
                }
                if sql.eq_ignore_ascii_case("BEGIN") {
                    txn.ops.clear();
                    txn.event_count = 0;
                    txn.active = true;
                    return Ok(());
                }
                if sql.eq_ignore_ascii_case("COMMIT") {
                    self.io_commit_txn(current_file, progress_pos, txn)?;
                    return Ok(());
                }
                if is_session_query(&sql) {
                    return Ok(());
                }
                if let Err(err) = validate_row_query_event(&sql, session.sql_mode) {
                    mark_io_error_fatal();
                    return Err(err);
                }
                let db = ev.schema().into_owned();
                let op = RelayOp::Ddl {
                    source_ts: txn.last_ts,
                    database: db,
                    sql,
                    session,
                    transactional: txn.active,
                };
                if txn.active {
                    self.append_relay_uncommitted(
                        current_file,
                        progress_pos,
                        txn.gtid.clone(),
                        &op,
                    )?;
                    txn.event_count = txn.event_count.saturating_add(1);
                    return Ok(());
                }
                // IO thread: durably append the DDL record and hand it to the
                // SQL thread — it applies. Advance IO read progress + retrieved
                // GTID; the SQL thread advances exec progress + executed GTID.
                self.append_relay(current_file, progress_pos, txn.gtid.clone(), &op)?;
                self.persist_io_progress(current_file, progress_pos, txn.gtid.take());
            }
            EventData::XaPrepareLogEvent(bytes) => {
                let (one_phase, xid) = decode_xa_prepare_event(&bytes)?;
                if txn.xa_xid.as_ref() != Some(&xid) || !txn.xa_ended {
                    mark_io_error_fatal();
                    return Err(xa_relay_error(
                        "XA_PREPARE_LOG_EVENT does not match XA START/END",
                    ));
                }
                if one_phase {
                    self.io_commit_txn(current_file, progress_pos, txn)?;
                } else {
                    self.append_relay(
                        current_file,
                        progress_pos,
                        txn.gtid.clone(),
                        &RelayOp::XaPrepare {
                            xid,
                            source_ts: txn.last_ts,
                        },
                    )?;
                    self.persist_io_progress(current_file, progress_pos, txn.gtid.take());
                    *txn = TxnBuffer::default();
                }
            }
            EventData::XidEvent(_) => {
                self.io_commit_txn(current_file, progress_pos, txn)?;
            }
            EventData::RowsEvent(rows) => {
                let table_id = rows.table_id();
                let Some(table_map) = table_maps.get(&table_id) else {
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        format!("rows event references missing TABLE_MAP id {table_id}"),
                    ));
                };
                use mysql_common::proto::MySerialize;
                let mut fde = Vec::new();
                let mut map_bytes = Vec::new();
                let mut row_bytes = Vec::new();
                event.fde().serialize(&mut fde);
                table_map.serialize(&mut map_bytes);
                rows.serialize(&mut row_bytes);
                let op = RelayOp::RawRows {
                    event_type: event.header().event_type_raw(),
                    fde,
                    table_map: map_bytes,
                    rows: row_bytes,
                };
                // IO thread only buffers the row record into the relay log; the
                // SQL thread reads it back and applies at the commit marker.
                self.append_relay(current_file, progress_pos, txn.gtid.clone(), &op)?;
                txn.event_count = txn.event_count.saturating_add(1);
            }
            EventData::StopEvent | EventData::IgnorableEvent(_) | EventData::RowsQueryEvent(_) => {}
            EventData::TransactionPayloadEvent(ev) => {
                let mut payload = ev.decompressed().map_err(|err| {
                    error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        format!("decompress transaction payload failed: {err}"),
                    )
                })?;
                let mut payload_reader = EventStreamReader::new(BinlogVersion::Version4);
                loop {
                    let Some(inner_event) = payload_reader
                        .read_decompressed(&mut payload)
                        .map_err(|err| {
                            error(
                                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                                format!("read compressed transaction payload failed: {err}"),
                            )
                        })?
                    else {
                        break;
                    };
                    let header = inner_event.header();
                    txn.last_ts = header.timestamp() as u64;
                    self.update_status(|status| {
                        status.received_events = status.received_events.saturating_add(1);
                        if header.timestamp() != 0 {
                            status.last_event_timestamp = header.timestamp() as u64;
                        }
                    });
                    self.handle_source_event(
                        &inner_event,
                        current_file,
                        progress_pos,
                        table_maps,
                        txn,
                    )?;
                }
            }
            other => {
                let (kind, hint) = match &other {
                    // These three only appear in STATEMENT/MIXED binlogs. The
                    // connect-time binlog_format probe should have rejected the
                    // source already; if we still see one, spell out the cause.
                    EventData::IntvarEvent(_) => (
                        "INTVAR_EVENT",
                        "; source is not emitting pure ROW format (set binlog_format=ROW)",
                    ),
                    EventData::RandEvent(_) => (
                        "RAND_EVENT",
                        "; source is not emitting pure ROW format (set binlog_format=ROW)",
                    ),
                    EventData::UserVarEvent(_) => (
                        "USER_VAR_EVENT",
                        "; source is not emitting pure ROW format (set binlog_format=ROW)",
                    ),
                    EventData::IncidentEvent(_) => (
                        "INCIDENT_EVENT",
                        "; the source reported a logging incident (possible event loss)",
                    ),
                    EventData::LoadEvent(_) | EventData::NewLoadEvent(_) => (
                        "LOAD_EVENT",
                        "; legacy LOAD DATA logging is unsupported (use binlog_format=ROW)",
                    ),
                    EventData::ExecuteLoadQueryEvent(_) | EventData::BeginLoadQueryEvent(_) => (
                        "LOAD_QUERY_EVENT",
                        "; legacy LOAD DATA logging is unsupported (use binlog_format=ROW)",
                    ),
                    _ => ("binlog event", ""),
                };
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("{kind} is not supported by mydb replication{hint}"),
                ));
            }
        }
        Ok(())
    }

    fn start_position(
        &self,
        cfg: &ReplicationChannelConfig,
        conn: &mut mysql::Conn,
    ) -> Result<(String, u64), QueryError> {
        // Prefer the last *committed* transaction boundary for a reconnect:
        // resuming from the per-event `status.source_log_pos` (which advances
        // mid-transaction) would ask the source to re-stream from inside a
        // transaction, and the fresh connection's empty TABLE_MAP cache would
        // then reject the first RowsEvent with 1235 in a tight loop.
        if let Ok(committed) = self.runtime.io_committed_pos.lock() {
            if let Some((file, pos)) = committed.as_ref() {
                if !file.is_empty() {
                    return Ok((file.clone(), (*pos).max(4)));
                }
            }
        }
        let status = self.status_snapshot();
        if !status.source_log_file.is_empty() {
            return Ok((status.source_log_file, status.source_log_pos.max(4)));
        }
        if !cfg.source_log_file.is_empty() {
            return Ok((cfg.source_log_file.clone(), cfg.source_log_pos.max(4)));
        }
        let row = conn
            .query_first::<mysql::Row, _>("SHOW BINARY LOG STATUS")
            .or_else(|_| conn.query_first::<mysql::Row, _>("SHOW MASTER STATUS"))
            .map_err(|err| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("cannot discover source binary log status: {err}"),
                )
            })?
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "source did not return binary log status; is log_bin enabled?",
                )
            })?;
        let file = row.get::<String, _>(0).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "source binary log status has no file column",
            )
        })?;
        let pos = row.get::<u64, _>(1).unwrap_or(4).max(4);
        Ok((file, pos))
    }

    fn build_binlog_request(
        &self,
        cfg: &ReplicationChannelConfig,
        file: &str,
        pos: u64,
    ) -> Result<mysql::BinlogRequest<'static>, QueryError> {
        if cfg.auto_position {
            let status = self.status_snapshot();
            let durable = self
                .runtime
                .durable_io_progress
                .lock()
                .map_err(|_| relay_lock_error())?;
            let received = format!(
                "{},{}",
                status.executed_gtid_set, durable.retrieved_gtid_set
            );
            let sids = parse_gtid_set(&format_gtid_interval_map(&parse_gtid_interval_map(
                &received,
            )))?;
            Ok(mysql::BinlogRequest::new(cfg.server_id)
                .with_use_gtid(true)
                .with_filename(Vec::<u8>::new())
                .with_pos(4_u64)
                .with_sids(sids))
        } else {
            Ok(mysql::BinlogRequest::new(cfg.server_id)
                .with_filename(file.as_bytes().to_vec())
                .with_pos(pos))
        }
    }

    fn decode_relay_rows(&self, op: RelayOp) -> Result<RelayOp, QueryError> {
        let RelayOp::RawRows {
            event_type,
            fde,
            table_map,
            rows,
        } = op
        else {
            return Ok(op);
        };
        use mysql_common::{
            binlog::{events::*, BinlogCtx, BinlogEvent},
            io::ParseBuf,
            proto::MyDeserialize,
        };
        let decode = || -> std::io::Result<_> {
            let default_fde = FormatDescriptionEvent::new(BinlogVersion::Version4);
            let fde = FormatDescriptionEvent::deserialize(
                BinlogCtx::new(
                    fde.len(),
                    &default_fde,
                    FormatDescriptionEvent::EVENT_TYPE as u8,
                ),
                &mut ParseBuf(&fde),
            )?;
            let map = TableMapEvent::deserialize(
                BinlogCtx::new(table_map.len(), &fde, TableMapEvent::EVENT_TYPE as u8),
                &mut ParseBuf(&table_map),
            )?;
            let ctx = BinlogCtx::new(rows.len(), &fde, event_type);
            let mut buf = ParseBuf(&rows);
            let rows = match event_type {
                23 => {
                    RowsEventData::WriteRowsEventV1(WriteRowsEventV1::deserialize(ctx, &mut buf)?)
                }
                24 => {
                    RowsEventData::UpdateRowsEventV1(UpdateRowsEventV1::deserialize(ctx, &mut buf)?)
                }
                25 => {
                    RowsEventData::DeleteRowsEventV1(DeleteRowsEventV1::deserialize(ctx, &mut buf)?)
                }
                30 => RowsEventData::WriteRowsEvent(WriteRowsEvent::deserialize(ctx, &mut buf)?),
                31 => RowsEventData::UpdateRowsEvent(UpdateRowsEvent::deserialize(ctx, &mut buf)?),
                32 => RowsEventData::DeleteRowsEvent(DeleteRowsEvent::deserialize(ctx, &mut buf)?),
                39 => RowsEventData::PartialUpdateRowsEvent(PartialUpdateRowsEvent::deserialize(
                    ctx, &mut buf,
                )?),
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "invalid relay rows event type",
                    ))
                }
            };
            Ok((rows.into_owned(), map.into_owned()))
        };
        let (rows, map) = decode().map_err(|err| io_error("decode relay rows", err))?;
        self.convert_rows_event(rows, &map)
    }

    fn convert_rows_event(
        &self,
        rows: mysql::binlog::events::RowsEventData<'_>,
        table_map: &mysql::binlog::events::TableMapEvent<'_>,
    ) -> Result<RelayOp, QueryError> {
        use mysql::binlog::events::RowsEventData;
        let database = table_map.database_name().into_owned();
        let table = table_map.table_name().into_owned();
        // Filter first — checking the catalog for a virtual `mysql.time_zone`
        // table when the operator already excludes the schema would just
        // surface a false "table not found".
        if !self.table_allowed(&database, &table) {
            self.update_status(|status| {
                status.filtered_events = status.filtered_events.saturating_add(1);
            });
            return Ok(RelayOp::Noop);
        }
        let full_name = crate::storage::table_key(&database, &table);
        let mut target = self.engine.database_for_replication().table(&full_name)?;
        // FULL TABLE_MAP metadata carries source collation IDs. Decode using
        // those even when the destination intentionally has another charset.
        // MINIMAL metadata requires matching schemas; strict decoding below
        // rejects invalid bytes rather than replacing them with U+FFFD.
        let meta = mysql_common::binlog::events::OptionalMetaExtractor::new(
            table_map.iter_optional_meta(),
        )
        .map_err(|err| io_error("decode source column metadata", err))?;
        let mut charsets = meta.iter_charset();
        let mut enum_charsets = meta.iter_enum_and_set_charset();
        let source_columns = target
            .iter_visible_columns()
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        for (index, target_index) in source_columns
            .into_iter()
            .take(table_map.columns_count() as usize)
            .enumerate()
        {
            let ty = table_map.get_column_type(index).map_err(|err| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("invalid source column type: {err}"),
                )
            })?;
            let id = match ty {
                Some(ty) if ty.is_character_type() => charsets.next(),
                Some(ty) if ty.is_enum_or_set_type() => enum_charsets.next(),
                _ => None,
            };
            if let Some(id) = id {
                let id = id.map_err(|err| io_error("decode source charset", err))?;
                let charset = crate::charset::resolve_collation_id_checked(id)?.charset;
                Arc::make_mut(&mut target).columns[target_index].charset = Some(charset.to_owned());
            }
        }
        // TABLE_MAP includes all live columns (including virtual/invisible
        // columns), ordered by their logical source position. Heap tombstones
        // and physical target slots must never change the source wire layout.
        let column_indices = target
            .iter_visible_columns()
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        let col_count = column_indices.len();
        if table_map.columns_count() as usize != col_count {
            return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!(
                    "schema mismatch for replicated table {database}.{table}: source declares {} columns, target has {}",
                    table_map.columns_count(), col_count,
                ),
            ));
        }
        if let Some(bitmap) = rows.columns_before_image() {
            if bitmap.count_ones() != col_count {
                return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("binlog_row_image=FULL is required for {database}.{table}; before-image bitmap has {} of {col_count} bits set", bitmap.count_ones()),
                ));
            }
        }
        if let Some(bitmap) = rows.columns_after_image() {
            if bitmap.count_ones() != col_count {
                return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("binlog_row_image=FULL is required for {database}.{table}; after-image bitmap has {} of {col_count} bits set", bitmap.count_ones()),
                ));
            }
        }

        let mut writes = Vec::new();
        let mut updates = Vec::new();
        let mut deletes = Vec::new();
        for row_result in rows.rows(table_map) {
            let (before, after) = row_result.map_err(|err| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("decode rows event for {database}.{table} failed: {err}"),
                )
            })?;
            match &rows {
                RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => {
                    let row = after.ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            "write rows event has no after image",
                        )
                    })?;
                    writes.push(convert_binlog_row(&target, &column_indices, row, None)?);
                }
                RowsEventData::UpdateRowsEvent(_)
                | RowsEventData::UpdateRowsEventV1(_)
                | RowsEventData::PartialUpdateRowsEvent(_) => {
                    let before = before.ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            "update rows event has no before image",
                        )
                    })?;
                    let after = after.ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            "update rows event has no after image",
                        )
                    })?;
                    let before = convert_binlog_row(&target, &column_indices, before, None)?;
                    let after = convert_binlog_row(&target, &column_indices, after, Some(&before))?;
                    updates.push(RowUpdate { before, after });
                }
                RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => {
                    let row = before.ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            "delete rows event has no before image",
                        )
                    })?;
                    deletes.push(convert_binlog_row(&target, &column_indices, row, None)?);
                }
            }
        }
        Ok(match rows {
            RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => {
                RelayOp::WriteRows {
                    database,
                    table,
                    rows: writes,
                }
            }
            RowsEventData::UpdateRowsEvent(_)
            | RowsEventData::UpdateRowsEventV1(_)
            | RowsEventData::PartialUpdateRowsEvent(_) => RelayOp::UpdateRows {
                database,
                table,
                rows: updates,
            },
            RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => {
                RelayOp::DeleteRows {
                    database,
                    table,
                    rows: deletes,
                }
            }
        })
    }

    fn table_allowed(&self, database: &str, table: &str) -> bool {
        let cfg = self
            .runtime
            .config
            .lock()
            .map(|cfg| cfg.clone())
            .unwrap_or_default();
        filter_allows_table(&cfg, database, table)
    }

    /// Delay the transaction being applied, independently of later IO events.
    /// Return false on STOP so the caller leaves its relay boundary unapplied.
    fn enforce_source_delay(&self, commit_ts: u64) -> bool {
        let delay = self
            .runtime
            .config
            .lock()
            .map(|cfg| cfg.source_delay_secs)
            .unwrap_or(0);
        if delay == 0 || commit_ts == 0 {
            return !self.sql_should_stop();
        }
        let earliest_apply = commit_ts.saturating_add(delay);
        while unix_secs() < earliest_apply && !self.sql_should_stop() {
            self.set_process_state(
                crate::engine::session_registry::ProcessState::WaitingForSourceDelay,
            );
            self.update_status(|status| {
                status.sql_delay_until = Some(earliest_apply);
                status.sql_state = "waiting for SOURCE_DELAY".to_string();
                status.seconds_behind_source = seconds_behind(commit_ts);
            });
            // Recheck wall time and cancellation without holding channel locks.
            std::thread::sleep(Duration::from_millis(50));
        }
        self.update_status(|status| status.sql_delay_until = None);
        self.set_process_state(crate::engine::session_registry::ProcessState::ReadingRelayLog);
        !self.sql_should_stop()
    }

    fn until_should_stop_before(&self, gtid: Option<&str>) -> bool {
        let Some(until) = self.runtime.until.lock().ok().and_then(|s| s.clone()) else {
            return false;
        };
        match until {
            UntilCondition::SqlBeforeGtids(set) => {
                if let Some(g) = gtid {
                    gtid_set_contains(&set, g)
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    fn until_should_stop_after(&self, file: &str, pos: u64, gtid: Option<&str>) -> bool {
        let Some(until) = self.runtime.until.lock().ok().and_then(|s| s.clone()) else {
            return false;
        };
        match until {
            UntilCondition::Position { file: f, pos: p } => file == f && pos >= p,
            UntilCondition::SqlAfterGtids(set) => {
                let executed = self.status_snapshot().executed_gtid_set;
                crate::binlog::gtid_subset(&set, &executed)
            }
            UntilCondition::SqlAfterMtsGaps => {
                // mydb has a single applier, so "no gaps" is always true on
                // the first transaction we observe.
                gtid.is_some()
            }
            _ => false,
        }
    }

    fn append_relay(
        &self,
        file: &str,
        pos: u64,
        gtid: Option<String>,
        op: &RelayOp,
    ) -> Result<(), QueryError> {
        self.append_relay_with_sync(file, pos, gtid, op, None)
    }

    fn append_relay_uncommitted(
        &self,
        file: &str,
        pos: u64,
        gtid: Option<String>,
        op: &RelayOp,
    ) -> Result<(), QueryError> {
        self.append_relay_with_sync(file, pos, gtid, op, Some(false))
    }

    fn append_relay_with_sync(
        &self,
        file: &str,
        pos: u64,
        gtid: Option<String>,
        op: &RelayOp,
        force_sync_override: Option<bool>,
    ) -> Result<(), QueryError> {
        let path = relay_log_path(&self.data_dir, &self.status_snapshot().channel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| io_error("create relay directory", err))?;
        }
        let record = RelayRecord {
            source_file: file.to_string(),
            source_pos: pos,
            gtid,
            op: op.clone(),
        };
        // Transaction boundaries are group-fsynced. DDL remains an immediate
        // barrier; transactional DDL is an intra-transaction record and is
        // covered by its later TxnCommit. A crash can lose only the
        // not-yet-published group: persisted source progress is capped by
        // `durable_io_progress`, so that group is fetched again.
        let durable_boundary = force_sync_override != Some(false)
            && matches!(
                &record.op,
                RelayOp::TxnCommit { .. }
                    | RelayOp::Ddl { .. }
                    | RelayOp::XaPrepare { .. }
                    | RelayOp::XaFinish { .. }
            );
        let ddl_barrier = force_sync_override != Some(false)
            && matches!(
                &record.op,
                RelayOp::Ddl { .. } | RelayOp::XaPrepare { .. } | RelayOp::XaFinish { .. }
            );
        let mut slot = self
            .runtime
            .relay_writer
            .lock()
            .map_err(|_| poisoned_lock("relay writer lock poisoned"))?;
        if relay_reclaim_path(
            &self.data_dir,
            &self
                .runtime
                .config
                .lock()
                .map_err(|_| relay_lock_error())?
                .channel,
        )
        .exists()
        {
            self.runtime.stop.store(true, Ordering::Release);
            return Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "interrupted relay reclaim; restart replication before appending",
            ));
        }
        if slot.is_none() {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(|err| io_error("open relay log", err))?;
            *slot = Some(std::io::BufWriter::with_capacity(64 * 1024, file));
        }
        let writer = slot.as_mut().unwrap();
        serde_json::to_writer(&mut *writer, &record).map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("write relay log JSON failed: {err}"),
            )
        })?;
        writer
            .write_all(b"\n")
            .map_err(|err| io_error("write relay log", err))?;
        let mut durable_batch = None;
        if durable_boundary {
            let mut sync = self
                .runtime
                .relay_sync
                .lock()
                .map_err(|_| poisoned_lock("relay sync state lock poisoned"))?;
            sync.pending_boundaries = sync.pending_boundaries.saturating_add(1);
            sync.latest_source_file = record.source_file.clone();
            sync.latest_source_pos = record.source_pos;
            sync.latest_boundary_offset = writer
                .get_ref()
                .metadata()
                .map_err(|e| io_error("stat relay boundary", e))?
                .len()
                + writer.buffer().len() as u64;
            if let Some(gtid) = &record.gtid {
                sync.pending_gtids.push(gtid.clone());
            }
            let sync_now = ddl_barrier
                || sync.pending_boundaries >= self.runtime.flush_policy.relay_sync_txns
                || sync.last_sync.elapsed() >= self.runtime.flush_policy.relay_sync_interval;
            if sync_now {
                writer
                    .flush()
                    .map_err(|err| io_error("flush relay log", err))?;
                let f = writer.get_mut();
                f.sync_data()
                    .map_err(|err| io_error("fsync relay log", err))?;
                let relay_pos = sync.latest_boundary_offset;
                durable_batch = Some((
                    relay_pos,
                    std::mem::take(&mut sync.latest_source_file),
                    sync.latest_source_pos,
                    std::mem::take(&mut sync.pending_gtids),
                ));
                sync.pending_boundaries = 0;
                sync.last_sync = Instant::now();
            }
        }
        self.update_status(|status| {
            status.relay_log_file = path.to_string_lossy().into_owned();
        });
        if let Some((relay_pos, source_file, source_pos, gtids)) = durable_batch {
            self.publish_relay_durable(relay_pos, source_file, source_pos, gtids);
        }
        drop(slot);
        Ok(())
    }

    /// Flush a partially filled relay group. Heartbeats call this with
    /// `force=false`, giving a bounded idle-tail delay; stop/reconnect paths
    /// force it so no accepted relay transaction is left only in BufWriter.
    fn sync_pending_relay(&self, force: bool) -> Result<bool, QueryError> {
        let mut slot = self
            .runtime
            .relay_writer
            .lock()
            .map_err(|_| poisoned_lock("relay writer lock poisoned"))?;
        let Some(writer) = slot.as_mut() else {
            return Ok(false);
        };
        let mut sync = self
            .runtime
            .relay_sync
            .lock()
            .map_err(|_| poisoned_lock("relay sync state lock poisoned"))?;
        if sync.pending_boundaries == 0
            || (!force && sync.last_sync.elapsed() < self.runtime.flush_policy.relay_sync_interval)
        {
            return Ok(false);
        }
        writer
            .flush()
            .map_err(|err| io_error("flush relay log", err))?;
        let f = writer.get_mut();
        f.sync_data()
            .map_err(|err| io_error("fsync relay log", err))?;
        let relay_pos = sync.latest_boundary_offset;
        let source_file = std::mem::take(&mut sync.latest_source_file);
        let source_pos = sync.latest_source_pos;
        let gtids = std::mem::take(&mut sync.pending_gtids);
        sync.pending_boundaries = 0;
        sync.last_sync = Instant::now();
        drop(sync);
        self.publish_relay_durable(relay_pos, source_file, source_pos, gtids);
        drop(slot);
        Ok(true)
    }

    fn publish_relay_durable(
        &self,
        relay_pos: u64,
        source_file: String,
        source_pos: u64,
        gtids: Vec<String>,
    ) {
        if let Ok(mut durable) = self.runtime.durable_io_progress.lock() {
            durable.source_file = source_file;
            durable.source_pos = source_pos;
            for gtid in gtids {
                durable.retrieved_gtid_set = append_gtid(&durable.retrieved_gtid_set, &gtid);
            }
        }
        if let Ok(mut gate) = self.runtime.relay_gate.lock() {
            if relay_pos > gate.durable_offset {
                gate.durable_offset = relay_pos;
            }
        }
        self.runtime.relay_cond.notify_all();
    }

    fn apply_records(&self, ops: Vec<RelayOp>, gtid: Option<String>) -> Result<(), QueryError> {
        let logical_gate =
            crate::storage::capture::logical_apply_gate(self.engine.database_for_replication())?;
        let _logical_apply = logical_gate.read_arc();
        // Called only from the SQL thread; a failure propagates up to
        // `set_sql_error` and stops the SQL thread while IO keeps buffering.
        self.update_status(|status| status.sql_state = "applying relay transaction".to_string());
        if let Some(gtid) = &gtid {
            if gtid_set_contains(&self.status_snapshot().executed_gtid_set, gtid) {
                self.update_status(|status| {
                    status.sql_state = "skipped duplicate GTID".to_string()
                });
                return Ok(());
            }
        }
        if ops.iter().any(|op| matches!(op, RelayOp::Ddl { .. })) {
            let has_rows = ops.iter().any(|op| {
                matches!(
                    op,
                    RelayOp::RawRows { .. }
                        | RelayOp::WriteRows { .. }
                        | RelayOp::UpdateRows { .. }
                        | RelayOp::DeleteRows { .. }
                )
            });
            // MySQL 8.0.21+ writes atomic `CREATE TABLE … AS SELECT` as a
            // single transaction containing one CREATE Query event followed
            // by the WRITE_ROWS for the projected rows (and a terminating
            // XID). We accept exactly that shape: one DDL, DDL comes first
            // in the non-Noop op sequence, and the DDL is a CREATE. Anything
            // else with mixed DDL+rows is still refused so we don't paper
            // over genuine source misconfiguration.
            let ddl_count = ops
                .iter()
                .filter(|op| matches!(op, RelayOp::Ddl { .. }))
                .count();
            let first_meaningful_is_ddl_create = ops
                .iter()
                .find(|op| !matches!(op, RelayOp::Noop | RelayOp::TxnCommit { .. }))
                .map(|op| match op {
                    RelayOp::Ddl { sql, .. } => sql
                        .trim_start()
                        .trim_start_matches(|c: char| c == '/' || c == '*' || c == '!')
                        .split_ascii_whitespace()
                        .next()
                        .map(|w| w.eq_ignore_ascii_case("CREATE"))
                        .unwrap_or(false),
                    _ => false,
                })
                .unwrap_or(false);
            let atomic_ctas = has_rows && ddl_count == 1 && first_meaningful_is_ddl_create;
            if !atomic_ctas
                && ops.iter().any(|op| {
                    !matches!(
                        op,
                        RelayOp::Ddl { .. } | RelayOp::Noop | RelayOp::TxnCommit { .. }
                    )
                })
            {
                return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "mixed DDL and row events in one replication transaction is not supported (except atomic CTAS)",
                ));
            }
            let cfg = self
                .runtime
                .config
                .lock()
                .map(|c| c.clone())
                .unwrap_or_default();
            for (ordinal, op) in ops.iter().enumerate() {
                let RelayOp::Ddl {
                    database,
                    sql,
                    session: state,
                    ..
                } = op
                else {
                    continue;
                };
                // User-management DDL targets the global auth catalog,
                // which mydb owns per-instance; never replicate it.
                if is_user_management_ddl(sql) {
                    self.update_status(|status| {
                        status.filtered_events = status.filtered_events.saturating_add(1);
                    });
                    continue;
                }
                // Also protect replay of relay records written by an earlier
                // receiver, before QUERY_EVENT admission was validated.
                validate_row_query_event(sql, state.sql_mode)?;
                // Replication filter: schema-level first; if we can identify
                // the target table, also honor the table-level rules.
                if !filter_allows_database(&cfg, database) {
                    self.update_status(|status| {
                        status.filtered_events = status.filtered_events.saturating_add(1);
                    });
                    continue;
                }
                let targets = extract_ddl_target_tables(sql, database);
                if !targets.is_empty() {
                    // db-level was already checked against the *default* schema
                    // above; DDL must apply only the table-level rules to the
                    // resolved targets (MySQL semantics — see
                    // `filter_allows_table_rules`). Multi-target DDL
                    // (`DROP TABLE a, b`, `RENAME TABLE a TO x, b TO y`) is
                    // atomic on the source: if any target of a single
                    // statement resolves to a mix of allowed and denied under
                    // our filters, we cannot faithfully replay half of it —
                    // silently applying would leak deny-listed schema, silently
                    // dropping would abandon allow-listed schema. We refuse
                    // the statement (1235) so the operator notices the
                    // config-scope mismatch and either widens the filter or
                    // splits the source-side migration.
                    let allowed = targets
                        .iter()
                        .filter(|(db, tbl)| filter_allows_table_rules(&cfg, db, tbl))
                        .count();
                    let denied = targets.len() - allowed;
                    if allowed == 0 && denied > 0 {
                        self.update_status(|status| {
                            status.filtered_events = status.filtered_events.saturating_add(1);
                        });
                        continue;
                    }
                    if allowed > 0 && denied > 0 {
                        return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            format!(
                                "multi-target DDL `{sql}` has {allowed} filter-allowed and {denied} filter-denied target(s); refuse to apply partially",
                            ),
                        ));
                    }
                }
                reject_temporary_statement_replication(sql)?;
                let mut session = SessionContext::default();
                if !database.is_empty() {
                    session.current_database = crate::storage::normalize_name(database);
                }
                apply_ddl_session_state(&mut session, state);
                // MySQL 8.0.21+ writes atomic `CREATE TABLE … AS SELECT` as
                // a single Query event whose SQL text has a trailing
                // ` START TRANSACTION` marker (that marker plus the row
                // events are how the source-side atomicity is expressed on
                // the wire). mydb's parser doesn't recognise the marker;
                // strip it here before executing the CREATE so the DDL is
                // accepted verbatim, then fall through to the row-events
                // (see atomic_ctas path above).
                let sql_owned;
                let sql_to_run: &str = if let Some(stripped) = strip_ctas_start_txn(sql) {
                    sql_owned = stripped;
                    &sql_owned
                } else {
                    sql
                };
                // The receipt is written with the final catalog mutation,
                // so a stale channel cursor cannot replay non-idempotent DDL.
                DDL_RECEIPT.with(|slot| {
                    if let Some((_, receipt)) = slot.borrow_mut().as_mut() {
                        receipt.ordinal = ordinal;
                    }
                });
                if let Some((key, expected)) = current_ddl_receipt()? {
                    if let Some(value) = self
                        .engine
                        .database_for_replication()
                        .replication_property(&key)?
                    {
                        let applied: DdlReceipt = serde_json::from_str(&value).map_err(|err| {
                            error(
                                crate::error::catalog::ER_GET_ERRNO,
                                format!("invalid replication DDL receipt: {err}"),
                            )
                        })?;
                        let expected: DdlReceipt =
                            serde_json::from_str(&expected).map_err(|err| {
                                error(
                                    crate::error::catalog::ER_GET_ERRNO,
                                    format!("invalid replication DDL context: {err}"),
                                )
                            })?;
                        if applied.boundary == expected.boundary && applied.ordinal >= ordinal {
                            continue;
                        }
                    }
                }
                self.set_process_info(Some(sql.to_string()));
                let result = with_replication_applier(|| {
                    QueryEngine::execute(self.engine.as_ref(), &mut session, sql_to_run)
                });
                let cleanup = QueryEngine::close_session(self.engine.as_ref(), &mut session);
                self.set_process_info(None);
                result?;
                cleanup?;
            }
            if !atomic_ctas {
                if let Some(gtid) = &gtid {
                    self.update_status(|status| {
                        status.executed_gtid_set = append_gtid(&status.executed_gtid_set, gtid);
                    });
                }
                self.update_status(|status| status.sql_state = "idle".to_string());
                return Ok(());
            }
            // Atomic CTAS: DDL is done, fall through to apply the row events
            // in a real storage transaction below.
        }
        let ops = ops
            .into_iter()
            .map(|op| self.decode_relay_rows(op))
            .collect::<Result<Vec<_>, _>>()?;
        let db = self.engine.database_for_replication();
        let txn = db.begin_txn()?;
        let result = with_replication_applier(|| apply_ops_to_storage(&self.engine, db, txn, &ops));
        match result {
            Ok(()) => {
                db.commit_txn(txn.txn_id)?;
                if let Some(gtid) = &gtid {
                    self.update_status(|status| {
                        status.executed_gtid_set = append_gtid(&status.executed_gtid_set, gtid);
                    });
                }
                self.update_status(|status| status.sql_state = "idle".to_string());
                Ok(())
            }
            Err(err) => {
                let _ = db.rollback_txn(txn.txn_id);
                Err(err)
            }
        }
    }

    /// One-shot crash-recovery drain: apply any relay records already on disk
    /// beyond the persisted applied offset. Run once by the SQL thread at
    /// startup before it begins tailing live IO writes. Reuses the same
    /// dispatch path as live apply; seeds the durable gate so the live loop
    /// also sees these bytes.
    fn replay_relay_tail(&self) -> Result<(), QueryError> {
        let path = relay_log_path(&self.data_dir, &self.status_snapshot().channel);
        if !path.exists() {
            return Ok(());
        }
        let len = self
            .runtime
            .relay_gate
            .lock()
            .map_err(|_| relay_lock_error())?
            .durable_offset;
        // Crash-recovery replay may re-apply a transaction that committed to
        // storage but whose progress was not yet persisted (or an anonymous
        // transaction with no GTID to dedup). Force idempotent apply so such
        // a re-apply is a silent skip instead of a STRICT 1062/1032 that
        // would wedge recovery.
        with_recovery_idempotent(|| self.sql_apply_available(len))
    }

    fn thread_error(err: &QueryError) -> ReplThreadError {
        ReplThreadError {
            code: err.code(),
            message: err.message().to_string(),
            at_unix_secs: unix_secs(),
        }
    }

    /// IO-thread error (connect / read stream / parse / relay write). Records
    /// `Last_IO_Error*`; the IO thread retries after this so ingest continues.
    fn set_io_error(&self, err: &QueryError) {
        log_message(format_args!(
            "[replication:{}] IO error {} {}",
            self.status_snapshot().channel,
            err.code(),
            err.message()
        ));
        let thread_error = Self::thread_error(err);
        self.update_status(|status| {
            status.last_error = Some(err.message().to_string());
            status.io_state = "retrying after error".to_string();
            status.last_io_error = Some(thread_error);
        });
    }

    /// SQL-thread error (applying a transaction). Records `Last_SQL_Error*`;
    /// the caller sets `sql_stop` so the SQL thread halts while the IO thread
    /// keeps buffering.
    fn set_sql_error(&self, err: &QueryError) {
        log_message(format_args!(
            "[replication:{}] SQL error {} {}",
            self.status_snapshot().channel,
            err.code(),
            err.message()
        ));
        let thread_error = Self::thread_error(err);
        self.update_status(|status| {
            status.last_error = Some(err.message().to_string());
            status.sql_state = "stopped on error".to_string();
            status.last_sql_error = Some(thread_error);
        });
    }

    fn mark_io_stopped(&self, state: &str) {
        self.update_status(|status| {
            status.running = false;
            status.io_state = state.to_string();
        });
    }

    fn mark_sql_stopped(&self, state: &str) {
        self.update_status(|status| {
            status.sql_running = false;
            status.sql_state = state.to_string();
        });
    }

    fn update_status(&self, f: impl FnOnce(&mut ReplicationStatus)) {
        if let Ok(mut status) = self.runtime.status.lock() {
            f(&mut status);
        }
    }

    fn set_process_state(&self, state: crate::engine::session_registry::ProcessState) {
        if let Some(handle) = &self.process_handle {
            handle.set_base_state(state);
        }
    }

    fn set_process_info(&self, info: Option<String>) {
        if let Some(handle) = &self.process_handle {
            handle.set_process_info(info);
        }
    }

    fn status_snapshot(&self) -> ReplicationStatus {
        self.runtime
            .status
            .lock()
            .map(|status| status.clone())
            .unwrap_or_else(|_| {
                ReplicationStatus::from_config(&ReplicationChannelConfig::default(), &self.data_dir)
            })
    }
}

fn apply_ops_to_storage(
    _engine: &Arc<StorageQueryEngine>,
    db: &Arc<Database>,
    txn: TxnView,
    ops: &[RelayOp],
) -> Result<(), QueryError> {
    for op in ops {
        match op {
            RelayOp::RawRows { .. } => {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "undecoded relay rows reached storage",
                ))
            }
            RelayOp::Noop | RelayOp::TxnCommit { .. } | RelayOp::Ddl { .. } => {}
            RelayOp::XaPrepare { .. } | RelayOp::XaFinish { .. } => {
                return Err(xa_relay_error("XA boundary reached ordinary row apply"))
            }
            RelayOp::WriteRows {
                database,
                table,
                rows,
            } => apply_write_rows(db, txn, database, table, rows)?,
            RelayOp::UpdateRows {
                database,
                table,
                rows,
            } => apply_update_rows(db, txn, database, table, rows)?,
            RelayOp::DeleteRows {
                database,
                table,
                rows,
            } => apply_delete_rows(db, txn, database, table, rows)?,
        }
    }
    Ok(())
}

fn relay_lock_error() -> QueryError {
    error(
        crate::error::catalog::ER_GET_ERRNO,
        "replication relay lock poisoned",
    )
}

fn apply_write_rows(
    db: &Arc<Database>,
    txn: TxnView,
    database: &str,
    table: &str,
    rows: &[Vec<Value>],
) -> Result<(), QueryError> {
    let table_name = crate::storage::table_key(database, table);
    match db.insert_rows_detailed(txn, &table_name, rows.to_vec()) {
        Ok(_) => Ok(()),
        // Only explicit IDEMPOTENT mode and crash recovery may skip conflicts.
        // Retry rows individually so new rows in a mixed replay batch survive.
        Err(err) if err.is(crate::error::catalog::ER_DUP_ENTRY) && apply_is_idempotent() => {
            for row in rows {
                match db.insert_row(txn, &table_name, row.clone()) {
                    Ok(_) => {}
                    Err(e) if e.is(crate::error::catalog::ER_DUP_ENTRY) => {
                        continue;
                    }
                    Err(e) => {
                        return Err(e.map_message(|message| {
                            format!(
                                "{message} (replicating WRITE_ROWS to {database}.{table}: {row:?})"
                            )
                        }))
                    }
                }
            }
            Ok(())
        }
        Err(err) => Err(err.map_message(|message| {
            format!("{message} (replicating to {database}.{table}: {rows:?})")
        })),
    }
}

fn apply_update_rows(
    db: &Arc<Database>,
    txn: TxnView,
    database: &str,
    table: &str,
    rows: &[RowUpdate],
) -> Result<(), QueryError> {
    let table_name = crate::storage::table_key(database, table);
    let schema = db.table(&table_name)?;
    // HASH_SCAN for keyless tables: build a one-shot row index so each
    // before-image is an O(1) lookup instead of a full-table rescan.
    let mut keyless = keyless_matcher(db, txn, &schema)?;
    for update in rows {
        let before_match = match keyless.as_mut() {
            Some(m) => m.take_match(&update.before),
            None => find_matching_row(db, txn, &schema, &update.before)?,
        };
        if let Some(row_id) = before_match {
            let after = update.after.clone();
            db.update_row_by_id(txn, &schema, row_id, move |_| Ok(after.clone()))?;
        } else {
            if apply_is_idempotent() {
                continue;
            }
            return Err(error(
                crate::error::catalog::ER_KEY_NOT_FOUND,
                format!(
                    "Could not execute Update_rows event on table {database}.{table}: \
                     could not find the row to update (slave_exec_mode=STRICT)"
                ),
            ));
        }
    }
    Ok(())
}

/// Build a `KeylessRowIndex` when the table has no usable unique index and the
/// HASH_SCAN optimisation is enabled (`MYDB_REPL_HASH_MATCH`, default on; `0`
/// keeps the per-row full-scan `find_matching_row` path as a differential
/// oracle). Keyed tables return `None` — their index lookup is already fast.
fn keyless_matcher(
    db: &Arc<Database>,
    txn: TxnView,
    schema: &TableSchema,
) -> Result<Option<KeylessRowIndex>, QueryError> {
    if has_usable_unique_index(schema) || !env_bool_default("MYDB_REPL_HASH_MATCH", true) {
        return Ok(None);
    }
    Ok(Some(KeylessRowIndex::build(db, txn, schema)?))
}

fn apply_delete_rows(
    db: &Arc<Database>,
    txn: TxnView,
    database: &str,
    table: &str,
    rows: &[Vec<Value>],
) -> Result<(), QueryError> {
    let table_name = crate::storage::table_key(database, table);
    let schema = db.table(&table_name)?;
    let mut keyless = keyless_matcher(db, txn, &schema)?;
    for row in rows {
        let matched = match keyless.as_mut() {
            Some(m) => m.take_match(row),
            None => find_matching_row(db, txn, &schema, row)?,
        };
        if let Some(row_id) = matched {
            db.delete_row_by_id(txn, &schema, row_id)?;
        } else if apply_is_idempotent() {
            // Row already absent (idempotent re-apply) or
            // slave_exec_mode=IDEMPOTENT / crash recovery — skip silently.
            continue;
        } else {
            // MySQL's default slave_exec_mode=STRICT stops the applier with
            // HA_ERR_KEY_NOT_FOUND (1032) when a DELETE_ROWS before-image is
            // absent, so data divergence is surfaced rather than masked. This
            // now matches apply_update_rows (previously DELETE skipped
            // silently — an asymmetry that hid drift).
            return Err(error(
                crate::error::catalog::ER_KEY_NOT_FOUND,
                format!(
                    "Could not execute Delete_rows event on table {database}.{table}: \
                     could not find the row to delete (slave_exec_mode=STRICT)"
                ),
            ));
        }
    }
    Ok(())
}

fn find_matching_row(
    db: &Arc<Database>,
    txn: TxnView,
    table: &TableSchema,
    image: &[Value],
) -> Result<Option<RowId>, QueryError> {
    for index in table
        .indexes
        .iter()
        .filter(|index| index.primary || index.unique)
    {
        if index.key_expressions.iter().any(Option::is_some) {
            continue;
        }
        let key = table.index_key(index, image)?;
        let ids = db.lookup_index_visible(table, index, key, txn)?;
        // A PRIMARY KEY — or a UNIQUE index whose columns are all NOT NULL —
        // is an authoritative row identity: in row-based replication the
        // before-image's key columns *are* the row identity, so a matched key
        // locates exactly the target row (MySQL selects the PK, else the first
        // all-NOT-NULL unique key, as the event's search key and does not
        // re-compare non-key columns). A full-row string comparison here is
        // both unnecessary and fragile — types whose stored form does not
        // round-trip byte-identically with the binlog image (JSON key
        // ordering/whitespace, DOUBLE/DECIMAL formatting, ...) would
        // spuriously fail to match and raise 1032 (B6). A *nullable* unique
        // key is not a guaranteed identity (duplicated across NULLs), so it
        // keeps MySQL's HASH_SCAN full-row verification.
        let authoritative = index.primary || index_columns_all_not_null(table, index);
        for row_id in ids {
            if let Some(row) = db.read_row(table, row_id, txn)? {
                if authoritative || row_values_match(table, &row.values, image) {
                    return Ok(Some(row.row_id));
                }
            }
        }
        if authoritative {
            return Ok(None);
        }
    }
    let rows = db.scan_rows(&table.qualified_name(), txn)?;
    Ok(rows
        .into_iter()
        .find(|row| row_values_match(table, &row.values, image))
        .map(|row| row.row_id))
}

fn row_values_match(table: &TableSchema, left: &[Value], right: &[Value]) -> bool {
    left.len() == right.len()
        && replicated_image_indices(table)
            .all(|idx| value_to_string(&left[idx]) == value_to_string(&right[idx]))
}

/// Only source-authoritative values identify a FULL row image. Tombstones
/// and replica-recomputed virtual values are placeholders in decoded images.
fn replicated_image_indices(table: &TableSchema) -> impl Iterator<Item = usize> + '_ {
    table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, col)| {
            col.state != crate::storage::schema::ColumnState::Dropped
                && (col.generated_expr.is_none() || col.generated_stored)
        })
        .map(|(idx, _)| idx)
}

/// True when every column referenced by `index` is declared NOT NULL — the
/// condition under which MySQL treats a UNIQUE key as a guaranteed row
/// identity (a nullable unique key can duplicate across NULLs, so it is not).
fn index_columns_all_not_null(
    table: &TableSchema,
    index: &crate::storage::schema::IndexSchema,
) -> bool {
    index.columns.iter().all(|col| {
        table
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(col))
            .is_some_and(|c| !c.nullable)
    })
}

/// A table can locate a row-based-replication before-image by index only when
/// it has a PRIMARY or UNIQUE index that is not expression-based.
fn has_usable_unique_index(table: &TableSchema) -> bool {
    table.indexes.iter().any(|index| {
        (index.primary || index.unique) && !index.key_expressions.iter().any(Option::is_some)
    })
}

/// PF3 — HASH_SCAN for tables with no usable unique index. Without one, the
/// per-row `find_matching_row` fallback materialises the whole table for
/// *every* image in the event: O(N·M) with two string allocations per column
/// per comparison. This scans the table once, hashing each row by the same
/// normalised per-column string form `row_values_match` uses, so each of the
/// event's M images is then an O(1) lookup — O(N+M) overall (MySQL's
/// `HASH_SCAN`). Matched row-ids are consumed so duplicate keyless rows and
/// repeated images in one event map to distinct physical rows.
struct KeylessRowIndex {
    indices: Vec<usize>,
    map: std::collections::HashMap<Vec<String>, Vec<RowId>>,
}

impl KeylessRowIndex {
    fn build(db: &Arc<Database>, txn: TxnView, table: &TableSchema) -> Result<Self, QueryError> {
        let rows = db.scan_rows(&table.qualified_name(), txn)?;
        let indices = replicated_image_indices(table).collect::<Vec<_>>();
        let mut map: HashMap<Vec<String>, Vec<RowId>> = HashMap::with_capacity(rows.len());
        for row in rows {
            map.entry(Self::image_key(&indices, &row.values))
                .or_default()
                .push(row.row_id);
        }
        Ok(Self { indices, map })
    }

    fn image_key(indices: &[usize], image: &[Value]) -> Vec<String> {
        indices
            .iter()
            .map(|&idx| value_to_string(&image[idx]))
            .collect()
    }

    fn take_match(&mut self, image: &[Value]) -> Option<RowId> {
        let key = Self::image_key(&self.indices, image);
        let ids = self.map.get_mut(&key)?;
        let id = ids.pop();
        if ids.is_empty() {
            self.map.remove(&key);
        }
        id
    }
}

fn clean_binlog_file_name(raw: &[u8]) -> String {
    let mut end = raw.len();
    if raw.len() > 4 && raw[raw.len() - 4..].iter().any(|b| !binlog_file_byte(*b)) {
        end -= 4;
    }
    end = raw[..end]
        .iter()
        .position(|b| !binlog_file_byte(*b))
        .unwrap_or(end);
    String::from_utf8_lossy(&raw[..end]).into_owned()
}

fn binlog_file_byte(byte: u8) -> bool {
    matches!(
        byte,
        b'0'..=b'9'
            | b'a'..=b'z'
            | b'A'..=b'Z'
            | b'.'
            | b'_'
            | b'-'
            | b'/'
            | b'\\'
            | b':'
    )
}

/// Decode row-image strings strictly. FULL metadata supplies the source
/// charset; MINIMAL metadata requires matching source/destination schemas.
/// Invalid bytes must stop SQL apply, never become replacement characters.
fn decode_replicated_string(
    bytes: &[u8],
    target_charset: Option<&str>,
) -> Result<Value, QueryError> {
    let cs = target_charset
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("utf8mb4");
    let text = if matches!(
        cs.to_ascii_lowercase().as_str(),
        "utf8mb4" | "utf8mb3" | "utf8" | "binary" | "ascii"
    ) {
        std::str::from_utf8(bytes).map(str::to_owned).map_err(|_| error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("replicated string is not valid {cs}; source/replica charset mismatch; align schemas or provide source charset metadata"),
        ))?
    } else {
        crate::charset::decode_bytes(cs, bytes).map_err(|err| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("cannot decode replicated string as {cs}: {err}"),
            )
        })?
    };
    Ok(Value::String(text))
}

fn convert_binlog_row(
    table: &TableSchema,
    column_indices: &[usize],
    row: mysql::binlog::row::BinlogRow,
    before_image: Option<&[Value]>,
) -> Result<Vec<Value>, QueryError> {
    if row.len() != column_indices.len() {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!(
                "binlog row image for {} has {} columns, target has {}",
                table.qualified_name(),
                row.len(),
                column_indices.len(),
            ),
        ));
    }
    // MySQL's row events (FULL mode) carry one wire slot per column of the
    // TABLE_MAP — including VIRTUAL generated columns, whose slot the source
    // fills with the (server-computed) value. We keep VIRTUAL columns as
    // `Value::Null` and let `materialize_generated_columns` recompute them
    // on the replica: this makes the applier resilient to a virtual
    // expression differing between source and replica (e.g. after a
    // catch-up rebuild) rather than trusting whatever bytes happened to be
    // in the source's per-thread cache. STORED generated columns *do*
    // pass through, and the constraints layer's replica-authoritative path
    // preserves them (see A6).
    let mut out = vec![Value::Null; table.columns.len()];
    for (wire_idx, &idx) in column_indices.iter().enumerate() {
        let column = &table.columns[idx];
        if column.generated_expr.is_some() && !column.generated_stored {
            continue;
        }
        let value = row.as_ref(wire_idx).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "binlog row value was already taken",
            )
        })?;
        let converted = binlog_value_to_mydb(
            value,
            &column.data_type,
            column.unsigned,
            column.charset.as_deref(),
            before_image.and_then(|row| row.get(idx)),
        )
        .map_err(|err| {
            err.map_message(|message| {
                format!(
                    "{message} (column `{}` type {:?} unsigned={} raw={:?})",
                    column.name, column.data_type, column.unsigned, value
                )
            })
        })?;
        out[idx] = converted;
    }
    Ok(out)
}

fn binlog_value_to_mydb(
    value: &mysql::binlog::value::BinlogValue<'_>,
    target: &DataTypeKind,
    unsigned: bool,
    target_charset: Option<&str>,
    before_value: Option<&Value>,
) -> Result<Value, QueryError> {
    match value {
        mysql::binlog::value::BinlogValue::Value(value) => {
            mysql_value_to_mydb(value, target, unsigned, target_charset)
        }
        mysql::binlog::value::BinlogValue::Jsonb(json) => decode_jsonb_to_mydb(json),
        mysql::binlog::value::BinlogValue::JsonDiff(diffs) => {
            if !matches!(target, DataTypeKind::Json) {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "PARTIAL_JSON value was received for a non-JSON target column",
                ));
            }
            let base = before_value.ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "PARTIAL_JSON update has no full before-image",
                )
            })?;
            let mut decoded = Vec::with_capacity(diffs.len());
            for diff in diffs {
                let value = diff
                    .value()
                    .map(|value| decode_jsonb_node(value, 1))
                    .transpose()?;
                decoded.push((diff.operation() as u8, diff.path_str().into_owned(), value));
            }
            crate::engine::expr::apply_replication_json_diffs(base, &decoded)
        }
    }
}

/// Decode directly into native JSONB so rich scalar tags and DECIMAL
/// precision survive both full row images and PARTIAL_JSON replacements.
fn decode_jsonb_to_mydb(
    json: &mysql_common::binlog::jsonb::Value<'_>,
) -> Result<Value, QueryError> {
    Ok(Value::Json(decode_jsonb_node(json, 1)?))
}

fn decode_jsonb_node(
    value: &mysql_common::binlog::jsonb::Value<'_>,
    depth: usize,
) -> Result<Vec<u8>, QueryError> {
    use crate::engine::json::{build_array, build_object, scalar_from_value, JsonType};
    use mysql_common::binlog::jsonb::Value as J;
    use mysql_common::constants::ColumnType as C;
    if depth > crate::engine::json::MAX_JSON_DEPTH {
        return Err(crate::engine::json::json_too_deep());
    }
    let malformed = || {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "malformed MySQL binary JSON value",
        )
    };
    let scalar = match value {
        J::Null => Value::Null,
        J::Bool(v) => {
            return Ok(crate::engine::json::from_serde(&serde_json::Value::Bool(
                *v,
            )))
        }
        J::I16(v) => Value::Int(*v as i64),
        J::I32(v) => Value::Int(*v as i64),
        J::I64(v) => Value::Int(*v),
        J::U16(v) => Value::UInt(*v as u64),
        J::U32(v) => Value::UInt(*v as u64),
        J::U64(v) => Value::UInt(*v),
        J::F64(v) if v.is_finite() => Value::Double(*v),
        J::F64(_) => return Err(malformed()),
        J::String(v) => Value::String(
            std::str::from_utf8(v.str_raw())
                .map_err(|_| malformed())?
                .to_owned(),
        ),
        J::SmallArray(arr) => {
            let nodes = arr
                .iter()
                .map(|item| decode_jsonb_node(&item.map_err(|_| malformed())?, depth + 1))
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(build_array(&nodes));
        }
        J::LargeArray(arr) => {
            let nodes = arr
                .iter()
                .map(|item| decode_jsonb_node(&item.map_err(|_| malformed())?, depth + 1))
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(build_array(&nodes));
        }
        J::SmallObject(obj) => {
            let pairs = obj
                .iter()
                .map(|item| {
                    let (key, value) = item.map_err(|_| malformed())?;
                    Ok((
                        std::str::from_utf8(key.value_raw())
                            .map_err(|_| malformed())?
                            .to_owned(),
                        decode_jsonb_node(&value, depth + 1)?,
                    ))
                })
                .collect::<Result<Vec<_>, QueryError>>()?;
            return Ok(build_object(&pairs));
        }
        J::LargeObject(obj) => {
            let pairs = obj
                .iter()
                .map(|item| {
                    let (key, value) = item.map_err(|_| malformed())?;
                    Ok((
                        std::str::from_utf8(key.value_raw())
                            .map_err(|_| malformed())?
                            .to_owned(),
                        decode_jsonb_node(&value, depth + 1)?,
                    ))
                })
                .collect::<Result<Vec<_>, QueryError>>()?;
            return Ok(build_object(&pairs));
        }
        J::Opaque(op) => {
            let mut rendered = String::new();
            render_jsonb_opaque(op, &mut rendered).map_err(|_| malformed())?;
            if op.value_type() == C::MYSQL_TYPE_NEWDECIMAL {
                Value::Decimal(rendered)
            } else {
                let text: String = serde_json::from_str(&rendered).map_err(|_| malformed())?;
                let ty = match op.value_type() {
                    C::MYSQL_TYPE_DATE => Some(JsonType::Date),
                    C::MYSQL_TYPE_TIME => Some(JsonType::Time),
                    C::MYSQL_TYPE_DATETIME => Some(JsonType::Datetime),
                    C::MYSQL_TYPE_TIMESTAMP => Some(JsonType::Timestamp),
                    _ => None,
                };
                if let Some(ty) = ty {
                    return crate::engine::json::typed_text_scalar(ty, &text);
                }
                return Ok(crate::engine::json::opaque_scalar(
                    op.value_type() as u8,
                    op.data_raw(),
                ));
            }
        }
    };
    Ok(scalar_from_value(&scalar))
}

fn render_jsonb_opaque(
    op: &mysql_common::binlog::jsonb::OpaqueValue<'_>,
    out: &mut String,
) -> std::fmt::Result {
    use mysql_common::constants::ColumnType;
    use std::fmt::Write;
    let bytes = op.data_raw();
    match op.value_type() {
        ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            // JSON uses precision/scale followed by binary DECIMAL. Validate
            // each base-10 group before invoking the vendor decoder, which
            // assumes trusted group ranges and precision >= scale.
            use mysql_common::binlog::decimal::{decimal_bin_size, Decimal, DIG_TO_BYTES};
            if bytes.len() < 3 {
                return Err(std::fmt::Error);
            }
            let (precision, scale) = (bytes[0] as usize, bytes[1] as usize);
            // Json_decimal stores decimal_t::intg + frac, including leading
            // group padding. SQL DECIMAL(65,30) can therefore carry 66 here.
            if precision == 0
                || precision > 81
                || scale > precision
                || (precision - scale).div_ceil(9) + scale.div_ceil(9) > 9
                || bytes.len() != 2 + decimal_bin_size(precision, scale)
            {
                return Err(std::fmt::Error);
            }
            let mut payload = bytes[2..].to_vec();
            let mask = if payload[0] & 0x80 != 0 { 0 } else { 0xff };
            payload[0] ^= 0x80;
            let intg = precision - scale;
            let groups = std::iter::once(intg % 9)
                .chain(std::iter::repeat_n(9, intg / 9 + scale / 9))
                .chain(std::iter::once(scale % 9));
            let mut offset = 0;
            for digits in groups.filter(|digits| *digits != 0) {
                let len = DIG_TO_BYTES[digits] as usize;
                let group = payload[offset..offset + len]
                    .iter()
                    .fold(0_u32, |v, b| (v << 8) | (b ^ mask) as u32);
                if group >= 10_u32.pow(digits as u32) {
                    return Err(std::fmt::Error);
                }
                offset += len;
            }
            let decimal = Decimal::read_packed(bytes, false).map_err(|_| std::fmt::Error)?;
            write!(out, "{decimal}")
        }
        ColumnType::MYSQL_TYPE_DATE
        | ColumnType::MYSQL_TYPE_TIME
        | ColumnType::MYSQL_TYPE_DATETIME
        | ColumnType::MYSQL_TYPE_TIMESTAMP => {
            // Json_datetime::to_packed stores TIME_to_longlong_packed as a
            // signed eight-byte little-endian integer, without disk offsets.
            let packed = i64::from_le_bytes(bytes.try_into().map_err(|_| std::fmt::Error)?);
            let negative = packed < 0;
            let value = packed.unsigned_abs();
            let fraction = value & 0xffffff;
            let int_part = value >> 24;
            let is_time = op.value_type() == ColumnType::MYSQL_TYPE_TIME;
            let is_date = op.value_type() == ColumnType::MYSQL_TYPE_DATE;
            let hms = if is_time {
                int_part
            } else {
                int_part & 0x1ffff
            };
            let (hour, minute, second) = (hms >> 12, (hms >> 6) & 63, hms & 63);
            if fraction > 999999
                || minute > 59
                || second > 59
                || hour > if is_time { 838 } else { 23 }
                || (!is_time && negative)
            {
                return Err(std::fmt::Error);
            }
            let mut text = String::new();
            if is_time {
                if negative {
                    text.push('-');
                }
            } else {
                let ymd = int_part >> 17;
                let ym = ymd >> 5;
                let (year, month, day) = (ym / 13, ym % 13, ymd & 31);
                if year > 9999 || (is_date && (hms != 0 || fraction != 0)) {
                    return Err(std::fmt::Error);
                }
                write!(text, "{year:04}-{month:02}-{day:02}")?;
                if is_date {
                    return render_json_string(&text, out);
                }
                text.push(' ');
            }
            write!(text, "{hour:02}:{minute:02}:{second:02}.{fraction:06}")?;
            render_json_string(&text, out)
        }
        _ => {
            // MySQL renders other opaque types as tagged, padded base64,
            // wrapping at 76 characters. This is the source JSON text value.
            let text = crate::engine::json::opaque_text(op.value_type() as u8, bytes);
            render_json_string(&text, out)
        }
    }
}

fn render_json_string(s: &str, out: &mut String) -> std::fmt::Result {
    use std::fmt::Write;
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x08' => out.push_str("\\b"),
            '\x0C' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => write!(out, "\\u{:04x}", c as u32)?,
            c => out.push(c),
        }
    }
    out.push('"');
    Ok(())
}

/// Parse a TIMESTAMP2 binlog text form (`"<unix_secs>[.<frac>]"`) into
/// mydb's absolute `Value::Timestamp` microsecond count. Returns `None` for shapes
/// we don't recognise so the caller can fall back to surfacing the raw
/// text (avoiding silent corruption on malformed input).
fn timestamp_text_to_datetime(s: &str) -> Option<i64> {
    let s = s.trim();
    let (secs_str, micros) = match s.split_once('.') {
        Some((secs, frac)) => {
            let mut padded = String::from(frac);
            if padded.len() < 6 {
                padded.push_str(&"000000"[..6 - padded.len()]);
            } else if padded.len() > 6 {
                padded.truncate(6);
            }
            (secs, padded.parse::<u32>().ok()?)
        }
        None => (s, 0u32),
    };
    let secs: i64 = secs_str.parse().ok()?;
    if secs == 0 && micros == 0 {
        return Some(crate::types::ZERO_DATETIME);
    }
    Some(secs.checked_mul(1_000_000)?.checked_add(micros as i64)?)
}

/// Render a SET bit-mask as the comma-separated member-name string mydb
/// stores in `Value::String`. Members at unset bits are skipped; an empty
/// mask renders as the empty string.
fn set_mask_to_csv(mask: u64, values: &[String]) -> String {
    let mut out = String::new();
    for (i, name) in values.iter().enumerate() {
        if mask & (1u64 << i) != 0 {
            if !out.is_empty() {
                out.push(',');
            }
            out.push_str(name);
        }
    }
    out
}

/// Mask for re-interpreting a sign-extended `Int(v)` from mysql_common as
/// the source UNSIGNED column's true u64 value. Returns `None` for
/// non-integer targets so callers fall through to the plain `Int(v)` path.
fn unsigned_mask_for(target: &DataTypeKind) -> Option<u64> {
    match target {
        DataTypeKind::TinyInt => Some(0xFF),
        DataTypeKind::SmallInt => Some(0xFFFF),
        DataTypeKind::MediumInt => Some(0xFF_FFFF),
        DataTypeKind::Int => Some(0xFFFF_FFFF),
        DataTypeKind::BigInt => Some(u64::MAX),
        _ => None,
    }
}

/// `(max_signed_value, width_bits)` for SIGNED integer columns whose byte
/// width does not map to a native primitive (notably MEDIUMINT's 3-byte
/// slot). mysql_common reads such slots as the raw unsigned bit pattern
/// — callers manually sign-extend when the value exceeds the signed max.
fn signed_extent_for(target: &DataTypeKind) -> Option<(u64, u32)> {
    match target {
        // 1-byte TINYINT signed: rarely needed (mysql_common gives i8) but
        // included for defensiveness.
        DataTypeKind::TinyInt => Some((0x7F, 8)),
        // 2-byte SMALLINT signed: similar.
        DataTypeKind::SmallInt => Some((0x7FFF, 16)),
        // 3-byte MEDIUMINT signed: this is the one that actually arrives
        // wrong without sign extension.
        DataTypeKind::MediumInt => Some((0x7F_FFFF, 24)),
        // 4-byte INT signed: defensive.
        DataTypeKind::Int => Some((0x7FFF_FFFF, 32)),
        // BIGINT signed: mysql_common decodes as i64; no sign-extension
        // needed (and 1<<64 would overflow). Excluded.
        _ => None,
    }
}

fn mysql_value_to_mydb(
    value: &mysql::Value,
    target: &DataTypeKind,
    unsigned: bool,
    target_charset: Option<&str>,
) -> Result<Value, QueryError> {
    // mysql_common decodes integer binlog values without the column's
    // signed/unsigned flag (the unsigned bit lives only in the extended
    // TABLE_MAP metadata mysql_common does not surface). When the source
    // column is UNSIGNED but the decoder produced a signed `Int(...)`,
    // re-interpret the two's-complement bit pattern *at the column's
    // declared byte width* — naive `v as u64` would extend `-1` to
    // `u64::MAX` instead of the correct `0xFF`/`0xFFFF`/...
    Ok(match value {
        mysql::Value::NULL => Value::Null,
        mysql::Value::Int(v) => {
            if unsigned {
                if let Some(mask) = unsigned_mask_for(target) {
                    Value::UInt((*v as u64) & mask)
                } else {
                    Value::Int(*v)
                }
            } else if let Some((max_signed, width_bits)) = signed_extent_for(target) {
                // mysql_common decodes the unsigned-bit-pattern of the
                // column's byte slot without sign-extension on widths
                // it has no native primitive for (notably MEDIUMINT's
                // 3-byte slot, which gets zero-extended to u32). When
                // the high bit was set, reconstruct the negative i64.
                let raw = *v;
                if raw >= 0 && raw as u64 > max_signed {
                    Value::Int(raw - (1i64 << width_bits))
                } else {
                    Value::Int(raw)
                }
            } else {
                Value::Int(*v)
            }
        }
        mysql::Value::UInt(v) => {
            if !unsigned {
                if let Some((max_signed, width_bits)) = signed_extent_for(target) {
                    let raw = *v;
                    if raw > max_signed {
                        Value::Int((raw as i128 - (1i128 << width_bits)) as i64)
                    } else {
                        Value::Int(raw as i64)
                    }
                } else {
                    Value::Int(*v as i64)
                }
            } else {
                Value::UInt(*v)
            }
        }
        mysql::Value::Float(v) => Value::Float(*v),
        mysql::Value::Double(v) => Value::Double(*v),
        mysql::Value::Bytes(bytes) => match target {
            DataTypeKind::Blob { .. }
            | DataTypeKind::Binary { .. }
            | DataTypeKind::VarBinary { .. } => Value::Bytes(bytes.clone()),
            // SET column: source ROW image is the raw little-endian bit
            // mask (1–8 bytes depending on `set_max_len`). Translate
            // bit positions into the comma-separated member list mydb
            // stores so `coerce_value`'s `Set` arm accepts it without a
            // round-trip through MySQL-style text parsing.
            DataTypeKind::Set { values } => {
                let mut mask: u64 = 0;
                for (i, b) in bytes.iter().take(8).enumerate() {
                    mask |= (*b as u64) << (i * 8);
                }
                Value::String(set_mask_to_csv(mask, values))
            }
            // TIMESTAMP: mysql_common surfaces TIMESTAMP2 as the text
            // form `"<unix_secs>[.<frac>]"` (an artefact of its single
            // pre-typed value enum). Convert to mydb's `DateTime` so the
            // storage-layer `Timestamp` arm accepts it directly.
            DataTypeKind::Timestamp { .. } => {
                let s = String::from_utf8_lossy(bytes);
                timestamp_text_to_datetime(&s)
                    .map(Value::Timestamp)
                    .unwrap_or_else(|| Value::String(s.into_owned()))
            }
            // YEAR: arrives as `"YYYY"`. `mysql_common` decodes the on-disk
            // raw byte (0..=255) as `(1900 + y).to_string()`, so the zero-year
            // (`YEAR "0000"`, raw byte = 0) shows up here as the *string*
            // `"1900"` — NOT `"0"`. We must map both `"0"` and `"1900"` to
            // `Value::Year(0)` (the mydb zero-year sentinel), only years
            // strictly in `1901..=2155` become non-sentinel `Year(y-1900)`,
            // and anything outside the representable range keeps the raw
            // string so `coerce_value` can raise the right 1264/1265 warning
            // rather than silently clamping to `2155`.
            DataTypeKind::Year => {
                let s = String::from_utf8_lossy(bytes);
                s.trim()
                    .parse::<i32>()
                    .ok()
                    .and_then(|y| {
                        if y == 0 || y == 1900 {
                            Some(Value::Year(0))
                        } else if (1901..=2155).contains(&y) {
                            Some(Value::Year((y - 1900) as u8))
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| Value::String(s.into_owned()))
            }
            // GEOMETRY: the row image carries MySQL's on-disk representation
            // (`SRID(4 LE) + WKB`), which is byte-identical to mydb's
            // `Value::Geometry` internal form. Preserve the raw bytes — the
            // `_` arm's `from_utf8_lossy` would replace any non-UTF-8 byte
            // (e.g. an IEEE-754 coordinate byte) with U+FFFD and corrupt the
            // geometry (observed: `POINT(1 2)` round-tripping to garbage).
            DataTypeKind::Geometry { .. } => Value::Geometry(bytes.clone()),
            // BIT(M): the row image carries the value as `ceil(M/8)`
            // big-endian bytes. Hand the raw bytes to `coerce_value`'s Bit
            // arm (which reads them big-endian into an integer) — the `_`
            // arm's `from_utf8_lossy` would corrupt any non-ASCII byte
            // (`BIT(8)=0x35` silently became the ASCII digit `"5"` → 5, and
            // high-bit values like `0x81` became U+FFFD and broke apply).
            DataTypeKind::Bit { .. } => Value::Bytes(bytes.clone()),
            // VECTOR(n): mydb producers use the versioned portable envelope;
            // legacy/raw producers remain accepted for backward compatibility.
            DataTypeKind::Vector { dim, datatype } => {
                Value::Vector(crate::engine::vector::codec::coerce_value(
                    Value::Bytes(bytes.clone()),
                    *datatype,
                    *dim,
                )?)
            }
            _ => decode_replicated_string(bytes, target_charset)?,
        },
        mysql::Value::Date(y, m, d, hh, mm, ss, us) => {
            if *y == 0 && *m == 0 && *d == 0 {
                match target {
                    DataTypeKind::Date => Value::Date(crate::types::ZERO_DATE),
                    DataTypeKind::Timestamp { .. } => Value::Timestamp(crate::types::ZERO_DATETIME),
                    _ => Value::DateTime(crate::types::ZERO_DATETIME),
                }
            } else if matches!(target, DataTypeKind::Date) {
                let date =
                    NaiveDate::from_ymd_opt(*y as i32, *m as u32, *d as u32).ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            "invalid DATE in binlog row",
                        )
                    })?;
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                Value::Date(date.signed_duration_since(epoch).num_days() as i32)
            } else {
                let date =
                    NaiveDate::from_ymd_opt(*y as i32, *m as u32, *d as u32).ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            "invalid DATETIME date in binlog row",
                        )
                    })?;
                let dt = date
                    .and_hms_micro_opt(*hh as u32, *mm as u32, *ss as u32, *us)
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_NOT_SUPPORTED_YET,
                            "invalid DATETIME time in binlog row",
                        )
                    })?;
                if matches!(target, DataTypeKind::Timestamp { .. }) {
                    Value::Timestamp(dt.and_utc().timestamp_micros())
                } else {
                    Value::DateTime(dt.and_utc().timestamp_micros())
                }
            }
        }
        mysql::Value::Time(neg, days, hours, minutes, seconds, micros) => {
            let total = ((((*days as i64 * 24 + *hours as i64) * 60 + *minutes as i64) * 60
                + *seconds as i64)
                * 1_000_000)
                + *micros as i64;
            Value::Time(if *neg { -total } else { total })
        }
    })
}

/// Emit MySQL 8.0's 1287 deprecation warning for the legacy `SLAVE`/`MASTER`
/// replication keywords (the modern spellings are `REPLICA`/`REPLICATION
/// SOURCE`). `SHOW SLAVE STATUS` is warned separately at its own call site.
fn warn_deprecated_replica_syntax(upper: &str) {
    let (old, new) = if upper.starts_with("START SLAVE") {
        ("START SLAVE", "START REPLICA")
    } else if upper.starts_with("STOP SLAVE") {
        ("STOP SLAVE", "STOP REPLICA")
    } else if upper.starts_with("RESET SLAVE") {
        ("RESET SLAVE", "RESET REPLICA")
    } else if upper.starts_with("CHANGE MASTER TO") {
        ("CHANGE MASTER TO", "CHANGE REPLICATION SOURCE TO")
    } else {
        return;
    };
    crate::engine::api::push_statement_warning(
        crate::error::ConditionLevel::Warning,
        crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
        format!(
            "'{old}' is deprecated and will be removed in a future release. \
             Please use {new} instead"
        ),
    );
}

pub fn handle_admin_sql(sql: &str) -> Result<QueryResponse, QueryError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    match ReplicationShow::classify(trimmed) {
        Some(kind @ ReplicationShow::Status { legacy }) => {
            kind.warn_if_legacy();
            let channel =
                parse_channel_only(trimmed, &["SHOW REPLICA STATUS", "SHOW SLAVE STATUS"])?;
            return match global_manager() {
                Some(manager) => manager.show_status_with_header(channel.as_deref(), legacy),
                None => Ok(QueryResponse::ResultSet(ResultSet {
                    columns: kind.columns(),
                    rows: Vec::new(),
                })),
            };
        }
        Some(ReplicationShow::RelaylogEvents) => return show_relaylog_events(trimmed),
        None => {}
    }
    let upper = trimmed.to_ascii_uppercase();
    warn_deprecated_replica_syntax(&upper);
    let manager = global_manager().ok_or_else(|| {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "replication manager is not initialized in this mydb process",
        )
    })?;
    if upper.starts_with("START REPLICA") || upper.starts_with("START SLAVE") {
        let (channel, until, sel) = parse_start_replica(trimmed)?;
        let channel = channel.unwrap_or_else(|| DEFAULT_CHANNEL.to_string());
        // UNTIL applies to the SQL thread; it is meaningless with IO_THREAD.
        if until.is_some() && sel == ReplThreadSel::Io {
            return Err(error(
                crate::error::catalog::ER_CANT_USE_OPTION_HERE,
                "START REPLICA UNTIL is not allowed with the IO_THREAD option",
            ));
        }
        manager.start_channel_threads(&channel, until, sel)?;
        return Ok(QueryResponse::Ok);
    }
    if upper.starts_with("STOP REPLICA") || upper.starts_with("STOP SLAVE") {
        let (channel, sel) = parse_stop_replica(trimmed, &["STOP REPLICA", "STOP SLAVE"])?;
        let channel = channel.unwrap_or_else(|| DEFAULT_CHANNEL.to_string());
        manager.stop_channel_threads(&channel, sel)?;
        return Ok(QueryResponse::Ok);
    }
    if upper.starts_with("RESET REPLICA") || upper.starts_with("RESET SLAVE") {
        // `RESET REPLICA [ALL] [FOR CHANNEL 'x']`. The `ALL` variant also
        // clears the stored connection configuration (MySQL). Strip the `ALL`
        // token before delegating to `parse_channel_only`, which only knows
        // the prefix + optional FOR CHANNEL grammar.
        let prefix = if upper.starts_with("RESET SLAVE") {
            "RESET SLAVE"
        } else {
            "RESET REPLICA"
        };
        let rest = trimmed[prefix.len()..].trim_start();
        let is_all = {
            let up = rest.to_ascii_uppercase();
            up == "ALL" || up.starts_with("ALL ") || up.starts_with("ALL\t")
        };
        let normalized = if is_all {
            format!("{prefix} {}", rest[3..].trim_start())
        } else {
            trimmed.to_string()
        };
        let channel = parse_channel_only(normalized.trim(), &["RESET REPLICA", "RESET SLAVE"])?
            .unwrap_or_else(|| DEFAULT_CHANNEL.to_string());
        if is_all {
            manager.reset_channel_all(&channel)?;
        } else {
            manager.reset_channel(&channel)?;
        }
        return Ok(QueryResponse::Ok);
    }
    if upper.starts_with("CHANGE REPLICATION SOURCE TO") || upper.starts_with("CHANGE MASTER TO") {
        // MySQL applies CHANGE incrementally: clauses not present keep their
        // existing values. Start from the channel's current config (in-memory,
        // else persisted, else default) rather than a blank default — a
        // `CHANGE ... SOURCE_DELAY=60` must not wipe host/user/password/etc.
        let channel = parse_change_source_channel(trimmed);
        let base = manager.base_config_for_change(&channel);
        let cfg = parse_change_source_onto(trimmed, base)?;
        manager.change_source(cfg)?;
        return Ok(QueryResponse::Ok);
    }
    Err(error(
        crate::error::catalog::ER_NOT_SUPPORTED_YET,
        format!("unsupported replication command: {trimmed}"),
    ))
}

/// Parse the modifiers on
/// `START REPLICA [IO_THREAD|SQL_THREAD] [UNTIL ...] [FOR CHANNEL '...']`.
/// Returns `(channel?, until_condition?, thread_selector)`. A bare
/// `IO_THREAD`/`SQL_THREAD` (optionally with an `S`, MySQL accepts both
/// `SQL_THREAD` and the historical spellings) selects one thread.
fn parse_start_replica(
    sql: &str,
) -> Result<(Option<String>, Option<UntilCondition>, ReplThreadSel), QueryError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    let head_len = if upper.starts_with("START REPLICA") {
        "START REPLICA".len()
    } else if upper.starts_with("START SLAVE") {
        "START SLAVE".len()
    } else {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "expected START REPLICA",
        ));
    };
    let mut tail = trimmed[head_len..].trim();
    // Optional IO_THREAD / SQL_THREAD selector (a comma-separated pair means
    // both). Consume it from the head of the tail.
    let (sel, consumed) = parse_thread_selector(tail);
    tail = tail[consumed..].trim();
    // Optional UNTIL clause
    let mut until: Option<UntilCondition> = None;
    let upper_tail = tail.to_ascii_uppercase();
    if let Some(rest) = upper_tail.strip_prefix("UNTIL ") {
        let pos = tail.len() - rest.len();
        let until_body_full = tail[pos..].trim();
        // Find the next FOR CHANNEL boundary (if any) so we don't slurp it
        // into the UNTIL body.
        let upper_body = until_body_full.to_ascii_uppercase();
        let (until_body, after_body) = if let Some(pos) = upper_body.rfind(" FOR CHANNEL ") {
            (until_body_full[..pos].trim(), until_body_full[pos..].trim())
        } else {
            (until_body_full, "")
        };
        until = Some(parse_until_clause(until_body)?);
        tail = after_body;
    }
    // FOR CHANNEL '<name>'
    let channel = if !tail.is_empty() {
        let upper_tail = tail.to_ascii_uppercase();
        if let Some(rest) = upper_tail.strip_prefix("FOR CHANNEL ") {
            let pos = tail.len() - rest.len();
            Some(unquote_sql_value(tail[pos..].trim()))
        } else {
            return Err(error(
                crate::error::catalog::ER_PARSE_ERROR,
                format!("unexpected tail in START REPLICA: '{tail}'"),
            ));
        }
    } else {
        None
    };
    Ok((channel, until, sel))
}

/// Parse a leading `IO_THREAD`/`SQL_THREAD` selector (either or both,
/// comma-separated) from the head of a START/STOP REPLICA tail. Returns the
/// selector and the number of bytes consumed (0 if none present → `Both`).
fn parse_thread_selector(tail: &str) -> (ReplThreadSel, usize) {
    let upper = tail.to_ascii_uppercase();
    let has_io = upper.starts_with("IO_THREAD");
    let has_sql = upper.starts_with("SQL_THREAD");
    if !has_io && !has_sql {
        return (ReplThreadSel::Both, 0);
    }
    // First token.
    let first_len = if has_io {
        "IO_THREAD".len()
    } else {
        "SQL_THREAD".len()
    };
    let mut consumed = first_len;
    let mut sel = if has_io {
        ReplThreadSel::Io
    } else {
        ReplThreadSel::Sql
    };
    // Optional ", <other>_THREAD".
    let rest = tail[consumed..].trim_start();
    let lead_ws = tail[consumed..].len() - rest.len();
    if let Some(after_comma) = rest.strip_prefix(',') {
        let after = after_comma.trim_start();
        let after_upper = after.to_ascii_uppercase();
        let inner_ws = after_comma.len() - after.len();
        if after_upper.starts_with("SQL_THREAD") && sel == ReplThreadSel::Io {
            sel = ReplThreadSel::Both;
            consumed += lead_ws + 1 + inner_ws + "SQL_THREAD".len();
        } else if after_upper.starts_with("IO_THREAD") && sel == ReplThreadSel::Sql {
            sel = ReplThreadSel::Both;
            consumed += lead_ws + 1 + inner_ws + "IO_THREAD".len();
        }
    }
    (sel, consumed)
}

/// Parse `STOP REPLICA [IO_THREAD|SQL_THREAD] [FOR CHANNEL '...']`, returning
/// `(channel?, thread_selector)`.
fn parse_stop_replica(
    sql: &str,
    prefixes: &[&str],
) -> Result<(Option<String>, ReplThreadSel), QueryError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    let Some(prefix) = prefixes.iter().find(|p| upper.starts_with(**p)) else {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "expected STOP REPLICA",
        ));
    };
    let tail = trimmed[prefix.len()..].trim();
    let (sel, consumed) = parse_thread_selector(tail);
    let rest = tail[consumed..].trim();
    let channel = if rest.is_empty() {
        None
    } else {
        let rest_upper = rest.to_ascii_uppercase();
        if let Some(after) = rest_upper.strip_prefix("FOR CHANNEL ") {
            let pos = rest.len() - after.len();
            Some(unquote_sql_value(rest[pos..].trim()))
        } else {
            return Err(error(
                crate::error::catalog::ER_PARSE_ERROR,
                format!("unexpected tail in STOP REPLICA: '{rest}'"),
            ));
        }
    };
    Ok((channel, sel))
}

fn parse_until_clause(body: &str) -> Result<UntilCondition, QueryError> {
    let upper = body.to_ascii_uppercase();
    if upper.trim() == "SQL_AFTER_MTS_GAPS" {
        return Ok(UntilCondition::SqlAfterMtsGaps);
    }
    // Otherwise SOURCE_LOG_FILE=... / SOURCE_LOG_POS=... or SQL_BEFORE_GTIDS / SQL_AFTER_GTIDS.
    let mut file = None::<String>;
    let mut pos = None::<u64>;
    let mut before_gtids = None::<String>;
    let mut after_gtids = None::<String>;
    for part in split_top_level_commas(body) {
        let Some((raw_key, raw_value)) = part.split_once('=') else {
            return Err(error(
                crate::error::catalog::ER_PARSE_ERROR,
                format!("bad UNTIL option '{part}' (expected KEY=VALUE)"),
            ));
        };
        let key = raw_key.trim().to_ascii_uppercase();
        let value = unquote_sql_value(raw_value.trim());
        match key.as_str() {
            "SOURCE_LOG_FILE" | "MASTER_LOG_FILE" => file = Some(value),
            "SOURCE_LOG_POS" | "MASTER_LOG_POS" => {
                pos = Some(value.parse().map_err(|_| {
                    error(
                        crate::error::catalog::ER_PARSE_ERROR,
                        format!("invalid UNTIL pos '{value}'"),
                    )
                })?);
            }
            "SQL_BEFORE_GTIDS" => before_gtids = Some(value),
            "SQL_AFTER_GTIDS" => after_gtids = Some(value),
            other => {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("UNTIL option {other} is not supported by mydb"),
                ))
            }
        }
    }
    match (file, pos, before_gtids, after_gtids) {
        (Some(f), Some(p), None, None) => Ok(UntilCondition::Position { file: f, pos: p }),
        (None, None, Some(s), None) => Ok(UntilCondition::SqlBeforeGtids(s)),
        (None, None, None, Some(s)) => Ok(UntilCondition::SqlAfterGtids(s)),
        _ => Err(error(crate::error::catalog::ER_PARSE_ERROR,
            "UNTIL requires SOURCE_LOG_FILE+SOURCE_LOG_POS, or SQL_BEFORE_GTIDS, or SQL_AFTER_GTIDS, or SQL_AFTER_MTS_GAPS — not a mix",
        )),
    }
}

/// Emit a warning for a `CHANGE REPLICATION SOURCE TO` clause that mydb accepts
/// for wire compatibility but does not act on (advisory under its
/// single-threaded, always-retry, always-ROW applier). Code 1287 matches the
/// project convention for "accepted but has no effect" (never a silent no-op).
fn advisory_change_source_clause(clause: &str, why: &str) {
    crate::engine::api::push_statement_warning(
        crate::error::ConditionLevel::Warning,
        crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
        format!("{clause} is accepted but has no effect: {why}"),
    );
}

/// Extract just the `FOR CHANNEL 'x'` name from a CHANGE statement (empty
/// default channel when absent) so the caller can load that channel's current
/// config as the merge base.
fn parse_change_source_channel(sql: &str) -> String {
    let body = match sql.to_ascii_uppercase().find(" TO ") {
        Some(pos) => &sql[pos + 4..],
        None => return DEFAULT_CHANNEL.to_string(),
    };
    if let Some(pos) = body.to_ascii_uppercase().rfind(" FOR CHANNEL ") {
        unquote_sql_value(body[pos + " FOR CHANNEL ".len()..].trim())
    } else {
        DEFAULT_CHANNEL.to_string()
    }
}

#[cfg(test)]
fn parse_change_source(sql: &str) -> Result<ReplicationChannelConfig, QueryError> {
    parse_change_source_onto(sql, ReplicationChannelConfig::default())
}

/// Parse a `CHANGE REPLICATION SOURCE TO` statement, applying each clause on
/// top of `cfg` (the channel's current configuration). Only fields named in
/// the statement are overwritten — incremental CHANGE per MySQL semantics.
fn parse_change_source_onto(
    sql: &str,
    mut cfg: ReplicationChannelConfig,
) -> Result<ReplicationChannelConfig, QueryError> {
    let mut body = if let Some(pos) = sql.to_ascii_uppercase().find(" TO ") {
        sql[pos + 4..].trim()
    } else {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "CHANGE REPLICATION SOURCE TO requires assignments",
        ));
    };
    if let Some(pos) = body.to_ascii_uppercase().rfind(" FOR CHANNEL ") {
        cfg.channel = unquote_sql_value(body[pos + " FOR CHANNEL ".len()..].trim());
        body = body[..pos].trim();
    }
    for part in split_top_level_commas(body) {
        let Some((raw_key, raw_value)) = part.split_once('=') else {
            return Err(error(
                crate::error::catalog::ER_PARSE_ERROR,
                format!("bad replication option '{part}'"),
            ));
        };
        let key = raw_key.trim().to_ascii_uppercase();
        let value = unquote_sql_value(raw_value.trim());
        match key.as_str() {
            "SOURCE_HOST" | "MASTER_HOST" => cfg.source_host = value,
            "SOURCE_PORT" | "MASTER_PORT" => {
                cfg.source_port = value.parse().map_err(|_| {
                    error(
                        crate::error::catalog::ER_PARSE_ERROR,
                        format!("invalid source port '{value}'"),
                    )
                })?
            }
            "SOURCE_USER" | "MASTER_USER" => cfg.source_user = value,
            "SOURCE_PASSWORD" | "MASTER_PASSWORD" => cfg.source_password = value,
            "SOURCE_LOG_FILE" | "MASTER_LOG_FILE" => cfg.source_log_file = value,
            "SOURCE_LOG_POS" | "MASTER_LOG_POS" => {
                cfg.source_log_pos = value.parse().map_err(|_| {
                    error(
                        crate::error::catalog::ER_PARSE_ERROR,
                        format!("invalid source log position '{value}'"),
                    )
                })?
            }
            "SOURCE_AUTO_POSITION" | "MASTER_AUTO_POSITION" => {
                cfg.auto_position = matches!(value.as_str(), "1" | "ON" | "on" | "TRUE" | "true")
            }
            "SOURCE_SERVER_ID" | "SERVER_ID" => {
                cfg.server_id = value.parse().map_err(|_| {
                    error(
                        crate::error::catalog::ER_PARSE_ERROR,
                        format!("invalid replica server id '{value}'"),
                    )
                })?
            }
            "SOURCE_CONNECT_RETRY_MS" | "CONNECT_RETRY_MS" => {
                cfg.connect_retry_ms = value.parse().map_err(|_| {
                    error(
                        crate::error::catalog::ER_PARSE_ERROR,
                        format!("invalid connect retry '{value}'"),
                    )
                })?
            }
            "SOURCE_SSL" | "MASTER_SSL" => {
                let enabled = matches!(value.as_str(), "1" | "ON" | "on" | "TRUE" | "true");
                if enabled && cfg.ssl_mode == ReplicationSslMode::Disabled {
                    cfg.ssl_mode = ReplicationSslMode::Required;
                } else if !enabled {
                    cfg.ssl_mode = ReplicationSslMode::Disabled;
                }
            }
            "SOURCE_SSL_MODE" | "MASTER_SSL_MODE" => {
                cfg.ssl_mode = ReplicationSslMode::from_label(&value).ok_or_else(|| {
                    error(crate::error::catalog::ER_PARSE_ERROR,
                        format!("invalid SOURCE_SSL_MODE '{value}'; expected DISABLED|REQUIRED|VERIFY_CA|VERIFY_IDENTITY"),
                    )
                })?;
            }
            "SOURCE_SSL_CA" | "MASTER_SSL_CA" => {
                cfg.ssl_ca = (!value.is_empty()).then(|| value);
            }
            "SOURCE_SSL_CERT" | "MASTER_SSL_CERT" => {
                cfg.ssl_cert = (!value.is_empty()).then(|| value);
            }
            "SOURCE_SSL_KEY" | "MASTER_SSL_KEY" => {
                cfg.ssl_key = (!value.is_empty()).then(|| value);
            }
            "SOURCE_SSL_CIPHER" | "MASTER_SSL_CIPHER" => {
                cfg.ssl_cipher = (!value.is_empty()).then(|| value);
            }
            "SOURCE_SSL_VERIFY_SERVER_CERT" | "MASTER_SSL_VERIFY_SERVER_CERT" => {
                let verify = matches!(value.as_str(), "1" | "ON" | "on" | "TRUE" | "true");
                if verify
                    && matches!(
                        cfg.ssl_mode,
                        ReplicationSslMode::Required | ReplicationSslMode::Disabled
                    )
                {
                    cfg.ssl_mode = ReplicationSslMode::VerifyIdentity;
                }
            }
            "REPLICATE_DO_DB" | "DO_DB" => {
                cfg.do_databases = parse_csv_or_paren_list(&value);
            }
            "REPLICATE_IGNORE_DB" | "IGNORE_DB" => {
                cfg.ignore_databases = parse_csv_or_paren_list(&value);
            }
            "REPLICATE_DO_TABLE" | "DO_TABLE" => {
                cfg.do_tables = parse_csv_or_paren_list(&value);
            }
            "REPLICATE_IGNORE_TABLE" | "IGNORE_TABLE" => {
                cfg.ignore_tables = parse_csv_or_paren_list(&value);
            }
            "REPLICATE_WILD_DO_TABLE" | "WILD_DO_TABLE" => {
                cfg.wild_do_tables = parse_csv_or_paren_list(&value);
            }
            "REPLICATE_WILD_IGNORE_TABLE" | "WILD_IGNORE_TABLE" => {
                cfg.wild_ignore_tables = parse_csv_or_paren_list(&value);
            }
            "SOURCE_DELAY" | "MASTER_DELAY" => {
                cfg.source_delay_secs = value.parse().map_err(|_| {
                    error(
                        crate::error::catalog::ER_PARSE_ERROR,
                        format!("invalid SOURCE_DELAY '{value}'"),
                    )
                })?;
            }
            // MySQL expresses connect-retry in *seconds*; mydb stores ms. Honor
            // it so tools that send SOURCE_CONNECT_RETRY (orchestrator/MHA)
            // actually change the backoff instead of hitting a hard error.
            "SOURCE_CONNECT_RETRY" | "MASTER_CONNECT_RETRY" => {
                let secs: u64 = value.parse().map_err(|_| {
                    error(
                        crate::error::catalog::ER_PARSE_ERROR,
                        format!("invalid SOURCE_CONNECT_RETRY '{value}'"),
                    )
                })?;
                cfg.connect_retry_ms = secs.saturating_mul(1000).max(100);
            }
            // Clauses mydb accepts for wire compatibility but that are advisory
            // under its single-threaded, always-retry, always-ROW applier.
            // Accept + warn (never a silent no-op, never a hard error) so a
            // vanilla MySQL `CHANGE REPLICATION SOURCE TO` from an
            // orchestrator drives mydb without breaking.
            "SOURCE_RETRY_COUNT" | "MASTER_RETRY_COUNT" => {
                advisory_change_source_clause(
                    &key,
                    "mydb retries the source connection until STOP REPLICA; the retry count is not bounded",
                );
            }
            "SOURCE_HEARTBEAT_PERIOD" | "MASTER_HEARTBEAT_PERIOD" => {
                advisory_change_source_clause(
                    &key,
                    "mydb requests a fixed long heartbeat period from the source; a custom period has no effect",
                );
            }
            "GET_SOURCE_PUBLIC_KEY" | "GET_MASTER_PUBLIC_KEY" => {
                // The pure-Rust client already fetches the caching_sha2 RSA
                // public key from the source automatically over a plaintext
                // channel, so this is effectively always satisfied.
                advisory_change_source_clause(
                    &key,
                    "mydb's replica client always retrieves the source public key when needed for caching_sha2_password over an unencrypted connection",
                );
            }
            "SOURCE_ZSTD_COMPRESSION_LEVEL"
            | "MASTER_ZSTD_COMPRESSION_LEVEL"
            | "SOURCE_COMPRESSION_ALGORITHMS"
            | "MASTER_COMPRESSION_ALGORITHMS" => {
                advisory_change_source_clause(
                    &key,
                    "mydb's replica requests an uncompressed binlog stream; connection compression settings have no effect",
                );
            }
            "SOURCE_PUBLIC_KEY_PATH"
            | "MASTER_PUBLIC_KEY_PATH"
            | "SOURCE_TLS_VERSION"
            | "MASTER_TLS_VERSION"
            | "SOURCE_TLS_CIPHERSUITES"
            | "SOURCE_BIND"
            | "MASTER_BIND"
            | "NETWORK_NAMESPACE" => {
                advisory_change_source_clause(
                    &key,
                    "mydb's replica connection does not honor this transport option; it is accepted for compatibility only",
                );
            }
            "IGNORE_SERVER_IDS" => {
                advisory_change_source_clause(
                    &key,
                    "mydb does not filter events by originating server id",
                );
            }
            "PRIVILEGE_CHECKS_USER"
            | "REQUIRE_TABLE_PRIMARY_KEY_CHECK"
            | "ASSIGN_GTIDS_TO_ANONYMOUS_TRANSACTIONS"
            | "GTID_ONLY" => {
                advisory_change_source_clause(
                    &key,
                    "mydb applies replicated rows with its own privilege and GTID handling; this clause is accepted but not separately enforced",
                );
            }
            // mydb only ever applies row-based events, so REQUIRE_ROW_FORMAT is
            // already satisfied — accept it as a silent success (its intent
            // holds), not a warning.
            "REQUIRE_ROW_FORMAT" => {}
            other => {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("replication option {other} is not supported by mydb"),
                ))
            }
        }
    }
    Ok(cfg)
}

fn parse_channel_only(sql: &str, prefixes: &[&str]) -> Result<Option<String>, QueryError> {
    let upper = sql.to_ascii_uppercase();
    let Some(prefix) = prefixes.iter().find(|prefix| upper.starts_with(**prefix)) else {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            format!("bad replication command '{sql}'"),
        ));
    };
    let rest = sql[prefix.len()..].trim();
    if rest.is_empty() {
        return Ok(None);
    }
    let rest_upper = rest.to_ascii_uppercase();
    if rest_upper.starts_with("FOR CHANNEL ") {
        let channel = rest["FOR CHANNEL ".len()..].trim();
        if channel.is_empty() {
            return Err(error(
                crate::error::catalog::ER_PARSE_ERROR,
                "FOR CHANNEL requires a channel name",
            ));
        }
        return Ok(Some(unquote_sql_value(channel)));
    }
    Err(error(
        crate::error::catalog::ER_NOT_SUPPORTED_YET,
        format!("replication command option '{rest}' is not supported by mydb"),
    ))
}

fn split_top_level_commas(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if let Some(q) = quote {
            cur.push(ch);
            if ch == q {
                if chars.peek() == Some(&q) {
                    cur.push(chars.next().unwrap());
                } else {
                    quote = None;
                }
            }
            continue;
        }
        match ch {
            '\'' | '"' | '`' => {
                quote = Some(ch);
                cur.push(ch);
            }
            ',' => {
                if !cur.trim().is_empty() {
                    out.push(cur.trim().to_string());
                }
                cur.clear();
            }
            _ => cur.push(ch),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

fn unquote_sql_value(value: &str) -> String {
    let value = value.trim();
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        let quote = bytes[0];
        if matches!(quote, b'\'' | b'"' | b'`') && bytes[value.len() - 1] == quote {
            return value[1..value.len() - 1].replace(
                std::str::from_utf8(&[quote, quote]).unwrap_or("''"),
                std::str::from_utf8(&[quote]).unwrap_or("'"),
            );
        }
    }
    value.to_string()
}

fn parse_gtid_set(gtids: &str) -> Result<Vec<mysql_common::packets::Sid<'static>>, QueryError> {
    if gtids.trim().is_empty() {
        return Ok(Vec::new());
    }
    gtids
        .split(',')
        .filter(|part| !part.trim().is_empty())
        .map(|part| {
            mysql_common::packets::Sid::from_str(part.trim()).map_err(|err| {
                error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("invalid GTID set '{gtids}': {err}"),
                )
            })
        })
        .collect()
}

fn format_gtid(sid: [u8; 16], gno: u64) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}:{gno}",
        sid[0], sid[1], sid[2], sid[3], sid[4], sid[5], sid[6], sid[7], sid[8], sid[9], sid[10],
        sid[11], sid[12], sid[13], sid[14], sid[15]
    )
}

/// Translate the channel's `ssl_mode` + cert paths into mysql's `SslOpts`.
/// `Disabled` returns `None` (caller leaves opts untouched).
fn build_ssl_opts(cfg: &ReplicationChannelConfig) -> Result<Option<mysql::SslOpts>, QueryError> {
    if !cfg.ssl_mode.requires_tls() {
        return Ok(None);
    }
    let mut opts = mysql::SslOpts::default();
    if let Some(ca) = &cfg.ssl_ca {
        let path: std::path::PathBuf = std::path::PathBuf::from(ca.clone());
        opts = opts.with_root_cert_path(Some(std::borrow::Cow::Owned(path)));
    }
    // Client identity (mTLS): if the operator configured both SOURCE_SSL_CERT
    // and SOURCE_SSL_KEY (typical for `CREATE USER 'repl' REQUIRE X509`),
    // wire them through to the TLS handshake so the source can authenticate
    // us. Previously stored+echoed but never handed to the TLS stack — the
    // handshake ran without any client cert, and `REQUIRE X509` users would
    // fail auth with an opaque error while `SHOW REPLICA STATUS` displayed
    // the paths as if mTLS were active. Also flag one-of-two as an error at
    // connect time so the operator sees the misconfiguration immediately
    // rather than an unrelated auth failure.
    match (cfg.ssl_cert.as_deref(), cfg.ssl_key.as_deref()) {
        (Some(cert), Some(key)) if !cert.is_empty() && !key.is_empty() => {
            let cert_path = std::path::PathBuf::from(cert);
            let key_path = std::path::PathBuf::from(key);
            let identity = mysql::ClientIdentity::new(cert_path, key_path);
            opts = opts.with_client_identity(Some(identity));
        }
        (Some(cert), None) if !cert.is_empty() => {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "SOURCE_SSL_CERT is set but SOURCE_SSL_KEY is missing; mTLS requires both",
            ));
        }
        (None, Some(key)) if !key.is_empty() => {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "SOURCE_SSL_KEY is set but SOURCE_SSL_CERT is missing; mTLS requires both",
            ));
        }
        _ => {}
    }
    // Loosen verification per ssl_mode: Required does not verify (matches MySQL semantics);
    // VerifyCa verifies CA but not the hostname; VerifyIdentity verifies both.
    match cfg.ssl_mode {
        ReplicationSslMode::Required => {
            opts = opts
                .with_danger_skip_domain_validation(true)
                .with_danger_accept_invalid_certs(cfg.ssl_ca.is_none());
        }
        ReplicationSslMode::VerifyCa => {
            opts = opts.with_danger_skip_domain_validation(true);
        }
        ReplicationSslMode::VerifyIdentity => {
            // Verify both CA chain and hostname — the default for SslOpts.
        }
        ReplicationSslMode::Disabled => unreachable!(),
    }
    Ok(Some(opts))
}

/// Accept either a bare comma-separated list (`db1,db2,db3`) or a
/// parenthesised list (`(db1, db2, db3)`) — MySQL accepts both for the
/// `CHANGE REPLICATION SOURCE TO REPLICATE_*` clauses.
pub(crate) fn parse_csv_or_paren_list(raw: &str) -> Vec<String> {
    let trimmed = raw.trim();
    let inner = if trimmed.starts_with('(') && trimmed.ends_with(')') {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    };
    inner
        .split(',')
        .map(|item| {
            item.trim()
                .trim_matches(|c| c == '\'' || c == '"' || c == '`')
                .to_string()
        })
        .filter(|item| !item.is_empty())
        .collect()
}

/// MySQL `--replicate-do-db` / `--replicate-ignore-db` semantics. When the
/// DDL has no current schema, `database` is empty and we follow MySQL: an
/// empty database is allowed only when both lists are empty.
pub(crate) fn filter_allows_database(cfg: &ReplicationChannelConfig, database: &str) -> bool {
    let db = crate::storage::normalize_name(database);
    if !cfg.ignore_databases.is_empty()
        && cfg
            .ignore_databases
            .iter()
            .any(|item| crate::storage::normalize_name(item) == db)
    {
        return false;
    }
    if !cfg.do_databases.is_empty()
        && !cfg
            .do_databases
            .iter()
            .any(|item| crate::storage::normalize_name(item) == db)
    {
        return false;
    }
    true
}

/// MySQL's combined table-level filter:
/// 1. database-level ignore wins
/// 2. database-level do_db gates everything else
/// 3. exact table-level ignore wins over allow-list
/// 4. wildcard ignore wins over allow-list
/// 5. if any table allow-list (exact or wildcard) is non-empty, require at
///    least one match
/// 6. otherwise allow
pub(crate) fn filter_allows_table(
    cfg: &ReplicationChannelConfig,
    database: &str,
    table: &str,
) -> bool {
    if !filter_allows_database(cfg, database) {
        return false;
    }
    filter_allows_table_rules(cfg, database, table)
}

/// The table-level portion of [`filter_allows_table`] — steps 2–6 above
/// (builtin ignore, exact/wildcard deny, allow-list) WITHOUT the
/// database-level (`--replicate-*-db`) gate.
///
/// MySQL applies the database-level filters for statement-based DDL against
/// the connection's **default** schema, not the schema of the table the DDL
/// happens to name. So the DDL apply path checks the default db once via
/// [`filter_allows_database`] and then evaluates only these table rules
/// against the resolved target table — using the full `filter_allows_table`
/// here would wrongly re-apply `--replicate-do-db` to a qualified target's
/// own schema (e.g. `USE foo; CREATE TABLE otherdb.t` with `do-db=foo` must
/// replicate, matching MySQL).
pub(crate) fn filter_allows_table_rules(
    cfg: &ReplicationChannelConfig,
    database: &str,
    table: &str,
) -> bool {
    if is_replication_builtin_ignore(database, table) {
        return false;
    }
    let qual = format!(
        "{}.{}",
        crate::storage::normalize_name(database),
        crate::storage::normalize_name(table)
    );
    // Exact deny
    if cfg
        .ignore_tables
        .iter()
        .any(|raw| crate::storage::normalize_name(raw) == qual)
    {
        return false;
    }
    // Wildcard deny
    if cfg
        .wild_ignore_tables
        .iter()
        .any(|pat| like_match(&crate::storage::normalize_name(pat), &qual))
    {
        return false;
    }
    let has_allow = !cfg.do_tables.is_empty() || !cfg.wild_do_tables.is_empty();
    if !has_allow {
        return true;
    }
    let exact_allow = cfg
        .do_tables
        .iter()
        .any(|raw| crate::storage::normalize_name(raw) == qual);
    if exact_allow {
        return true;
    }
    cfg.wild_do_tables
        .iter()
        .any(|pat| like_match(&crate::storage::normalize_name(pat), &qual))
}

/// True for mydb-virtual tables under the `mysql` system schema that mydb
/// generates internally — replicating row changes or DDL on them would
/// conflict with mydb's own representation. Vanilla MySQL writes
/// `TRUNCATE/INSERT mysql.time_zone*` to the binlog when the operator runs
/// `mysql_tzinfo_to_sql` (e.g. on Docker image startup); a generic slave
/// must tolerate that out of the box without forcing the operator to add a
/// `--replicate-ignore-table` for every server-managed name.
pub(crate) fn is_replication_builtin_ignore(database: &str, table: &str) -> bool {
    let db = crate::storage::normalize_name(database);
    if db != "mysql" {
        return false;
    }
    let tbl = crate::storage::normalize_name(table);
    matches!(
        tbl.as_str(),
        // Time-zone catalog populated by mysql_tzinfo_to_sql; mydb reads the
        // host zoneinfo directly so these are no-ops.
        "time_zone"
            | "time_zone_name"
            | "time_zone_transition"
            | "time_zone_transition_type"
            | "time_zone_leap_second"
            // Documentation / help corpus that ships with MySQL.
            | "help_topic"
            | "help_category"
            | "help_keyword"
            | "help_relation"
            // InnoDB-managed statistics and DDL log tables.
            | "innodb_index_stats"
            | "innodb_table_stats"
            | "innodb_dynamic_metadata"
            | "innodb_ddl_log"
            // Optimizer cost-model tables; mydb has its own CBO state.
            | "server_cost"
            | "engine_cost"
            // Replication metadata the slave writes itself.
            | "slave_master_info"
            | "slave_relay_log_info"
            | "slave_worker_info"
            | "gtid_executed"
            // Logging tables; mydb stores these per `MYDB_LOG_OUTPUT`.
            | "general_log"
            | "slow_log"
            // Plugin/component registries — mydb has no plugin loader.
            | "component"
            | "plugin"
    )
}

/// Minimal MySQL-style LIKE: `%` matches any sequence, `_` matches one char.
/// No escapes — MySQL's CLI for `--replicate-wild-*-table` likewise treats
/// the input as a plain LIKE pattern.
///
/// B6: delegates to the shared iterative kernel so pattern matching
/// stays consistent across replication rules, GRANT host matching, and
/// SQL LIKE. The previous per-file recursive matcher walked bytes with
/// no UTF-8 awareness (`_` would advance a byte instead of a code
/// point, silently dropping matches for multi-byte identifiers) and
/// was O(2^k) on adversarial `%%%...` inputs.
pub(crate) fn like_match(pattern: &str, input: &str) -> bool {
    // Replication rules are identifier-shaped strings — no escape, no
    // CI folding (MySQL's replicate-wild-* is case-exact). Route
    // through the byte-exact path so pattern semantics stay verbatim.
    crate::engine::expr::match_like_pattern(input, pattern, None)
}

fn require_secure_transport_for_replication() -> bool {
    matches!(
        crate::util::env::var("MYDB_REPL_REQUIRE_SECURE_TRANSPORT")
            .ok()
            .as_deref(),
        Some("1") | Some("on") | Some("ON") | Some("true") | Some("TRUE")
    )
}

fn require_replication_source_setting(
    name: &str,
    value: Option<&str>,
    required: &str,
) -> Result<(), QueryError> {
    let Some(value) = value else {
        mark_io_error_fatal();
        return Err(error(
            crate::error::catalog::CR_SERVER_LOST,
            format!("replication source returned no value for @@global.{name}"),
        ));
    };
    if value.eq_ignore_ascii_case(required) {
        return Ok(());
    }
    mark_io_error_fatal();
    Err(error(
        crate::error::catalog::ER_NOT_SUPPORTED_YET,
        format!(
            "replication source has {name}={value}; mydb replica requires {name}={required} \
             (set it on the source and restart its binary log)"
        ),
    ))
}

/// Compare the trailing CRC32 against the recomputed digest. Returns `true`
/// for events that don't carry a CRC (footer alg=NONE) so callers don't
/// false-fail on legitimately-unchecksummed streams.
pub fn verify_binlog_event_crc32(event: &mysql_common::binlog::events::Event) -> bool {
    use mysql_common::binlog::consts::BinlogChecksumAlg;
    let Some(checksum) = event.checksum() else {
        return true;
    };
    let alg = match event.footer().get_checksum_alg() {
        Ok(Some(alg)) => alg,
        _ => return true,
    };
    if !matches!(alg, BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32) {
        return true;
    }
    let expected = u32::from_le_bytes(checksum);
    let actual = event.calc_checksum(alg);
    expected == actual
}

thread_local! {
    /// Set by IO-thread error sites that are *deterministic* — reconnecting to
    /// the same source position will re-hit them identically: connect-time
    /// source-precondition rejections (binlog_format≠ROW, binlog_row_image≠FULL,
    /// secure-transport). `run_io` runs `run_once` on this same thread, so a
    /// plain `Cell` is race-free. Checksum mismatches are classified by their
    /// dedicated code (1593) instead, so mid-stream *transient* 1235s
    /// (table-not-found while the SQL thread lags a DDL, a momentarily-missing
    /// TABLE_MAP) are NOT mistaken for poison-pills. (R8)
    static IO_ERROR_FATAL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Flag the error about to be returned from the IO path as a deterministic
/// poison-pill so `run_io` stops the thread instead of retrying forever.
fn mark_io_error_fatal() {
    IO_ERROR_FATAL.with(|f| f.set(true));
}

/// Read and clear the poison-pill flag (called once per `run_once` attempt).
fn take_io_error_fatal() -> bool {
    IO_ERROR_FATAL.with(|f| f.replace(false))
}

/// Whether an IO-thread error must stop the thread rather than trigger an
/// unbounded reconnect loop. Deterministic errors — a CRC32 checksum mismatch
/// (1593; `ER_BINLOG_FATAL_ERROR`)
/// or a site that explicitly flagged itself fatal — stop; connection/network
/// errors (2003) and racy mid-stream conditions retry. (R8)
fn io_error_is_fatal(error: &QueryError, marked_fatal: bool) -> bool {
    marked_fatal || error.is(crate::error::catalog::ER_BINLOG_FATAL_ERROR)
}

// A disposable applier session cannot reproduce source-session table lifetime.
// Refuse before allocation instead of silently discarding dependencies needed by
// later statement events. ROW sources do not log these temporary-table DDLs.
fn reject_temporary_statement_replication(sql: &str) -> Result<(), QueryError> {
    let clean = strip_leading_sql_comments(sql);
    let mut words = clean.split_ascii_whitespace();
    let first = words.next().unwrap_or_default();
    let second = words.next().unwrap_or_default();
    if (first.eq_ignore_ascii_case("CREATE") || first.eq_ignore_ascii_case("DROP"))
        && (second.eq_ignore_ascii_case("TEMPORARY") || second.eq_ignore_ascii_case("TEMP"))
    {
        return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "Replication of session temporary-table statements is unsupported; use binlog_format=ROW on the source"));
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn temporary_audit_replication_refuses_session_ddl() {
    for sql in [
        "CREATE TEMPORARY TABLE t(id INT)",
        "DROP TEMPORARY TABLE IF EXISTS t",
        "/* prefix */ CREATE /*!80000 TEMPORARY */ TABLE t(id INT)",
        "DROP\nTEMPORARY\tTABLE t",
    ] {
        assert!(
            reject_temporary_statement_replication(sql).is_err(),
            "{sql}"
        );
    }
    for sql in [
        "CREATE TABLE t(id INT)",
        "DROP TABLE t",
        "CREATE TABLE temporary_t(id INT)",
    ] {
        reject_temporary_statement_replication(sql).unwrap();
    }
}

fn is_session_query(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    upper.starts_with("USE ")
        || upper.starts_with("SAVEPOINT ")
        || upper.starts_with("ROLLBACK TO ")
}

fn validate_row_query_event(sql: &str, sql_mode: u64) -> Result<(), QueryError> {
    // Auth DDL is deliberately filtered by the applier. It still needs a
    // committed relay boundary, rather than being mistaken for a SET no-op.
    if is_user_management_ddl(sql) {
        return Ok(());
    }
    let modes = crate::binlog::sql_mode_bits_to_str(sql_mode);
    let _mode = crate::parser::ParserSqlModeGuard::activate(
        crate::engine::api::parse_temporal_sql_mode_bits(&modes),
    );
    let ddl = strip_ctas_start_txn(sql);
    let statement =
        crate::parser::parse_statement(ddl.as_deref().unwrap_or(sql)).map_err(|err| {
            error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("unsupported ROW/FULL QUERY_EVENT: {err}"),
            )
        })?;
    let effects = crate::statement_effects::statement_effects(&statement);
    if matches!(statement, Statement::CreateTableAsSelect { .. })
        || !(effects.changes_catalog || effects.ddl_or_maintenance)
    {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "statement-based data or session effects in QUERY_EVENT are unsupported; use ROW/FULL row events and catalog DDL",
        ));
    }
    Ok(())
}

/// Identify user/role/privilege management DDL — these mutate the global
/// authentication catalog, which mydb manages independently of any source
/// it replicates from. Default-skip on the apply path. Operators who want
/// to mirror upstream grants can do so out-of-band (mysqldump-style).
/// Strip leading SQL comments before DDL prefix classification. Plain block
/// comments (`/* ... */`) and line comments (`-- `, `#`) are removed
/// entirely; MySQL version-conditional comments (`/*! ... */`,
/// `/*!NNNNN ... */`) are *unwrapped* so their inner statement is classified
/// (modern MySQL executes them). Only the leading run is processed — enough
/// to reach the first real keyword for prefix/target matching. The original
/// SQL is still executed verbatim; this rewrite only feeds classification.
fn strip_leading_sql_comments(sql: &str) -> String {
    let mode = crate::sql_lex::SqlLexMode::default();
    let Ok(prepared) = crate::sql_lex::PreparedSql::new(sql, mode, crate::parser::MYSQL_VERSION_ID)
    else {
        return sql.to_string();
    };
    crate::sql_lex::mask_comments(
        prepared.effective(),
        mode,
        crate::parser::MYSQL_VERSION_ID,
        false,
    )
    .map(|cleaned| cleaned.trim_start().to_string())
    .unwrap_or_else(|_| sql.to_string())
}

fn is_user_management_ddl(sql: &str) -> bool {
    let sql = strip_leading_sql_comments(sql);
    let mut words = sql
        .split(|c: char| c.is_whitespace() || c == ';')
        .filter(|w| !w.is_empty());
    let w1 = match words.next() {
        Some(w) => w.to_ascii_uppercase(),
        None => return false,
    };
    let w2 = words.next().map(|w| w.to_ascii_uppercase());
    match w1.as_str() {
        "GRANT" | "REVOKE" => true,
        "CREATE" | "DROP" | "ALTER" | "RENAME" => {
            matches!(w2.as_deref(), Some("USER") | Some("ROLE"))
        }
        "SET" => matches!(w2.as_deref(), Some("PASSWORD") | Some("DEFAULT")),
        "FLUSH" => matches!(w2.as_deref(), Some("PRIVILEGES")),
        _ => false,
    }
}

/// Extract the primary `(database, table)` target from a binlog QUERY
/// event's DDL SQL — enough to apply table-level replication filters
/// before the engine touches the catalog. Returns the source's default
/// schema when the statement names an unqualified table. Returns `None`
/// for shapes we don't recognise; the caller still applies the schema
/// filter via `filter_allows_database`.
///
/// Multi-target DDL (`DROP TABLE a, b`, `RENAME TABLE a TO b, c TO d`) is
/// folded to the first target — the row events for each affected table
/// flow through the row-level filter separately, so the only risk is
/// mis-attributing a multi-table DROP. Acceptable for a fast-path.
/// MySQL 8.0.21+ writes atomic CTAS (`CREATE TABLE … AS SELECT`) as a Query
/// event ending in `<sql> START TRANSACTION`. mydb's SQL parser only sees the
/// leading `CREATE TABLE` shape and rejects the trailing marker (1064).
/// Return the CREATE portion with the marker stripped when we detect that
/// exact shape; return `None` otherwise (caller uses the original SQL).
fn strip_ctas_start_txn(sql: &str) -> Option<String> {
    let trimmed = sql.trim_end_matches(|c: char| c == ';' || c.is_whitespace());
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("CREATE") {
        return None;
    }
    // Case-insensitive: does the SQL end with `START TRANSACTION` (with
    // any run of whitespace between the two words)?
    let suffix = "START TRANSACTION";
    let sfx_len = suffix.len();
    if upper.len() < sfx_len {
        return None;
    }
    if !upper[upper.len() - sfx_len..].eq_ignore_ascii_case(suffix) {
        return None;
    }
    // Snip out the suffix (using the original SQL's byte length up to the
    // matched slice — same lengths since `START TRANSACTION` is ASCII).
    let cut = trimmed.len() - sfx_len;
    let mut head = trimmed[..cut].to_string();
    while head
        .chars()
        .next_back()
        .is_some_and(|c| c.is_whitespace() || c == ',')
    {
        head.pop();
    }
    Some(head)
}

fn strip_quotes(s: &str) -> &str {
    s.trim_matches(|c: char| c == '`' || c == '"' || c == '\'')
}

fn split_qualified(token: &str, default_db: &str) -> (String, String) {
    let token = strip_quotes(token);
    if let Some((db, tbl)) = token.split_once('.') {
        (
            crate::storage::normalize_name(strip_quotes(db)),
            crate::storage::normalize_name(strip_quotes(tbl)),
        )
    } else {
        (
            crate::storage::normalize_name(default_db),
            crate::storage::normalize_name(token),
        )
    }
}

/// Return every table target of a multi-target DDL — `DROP TABLE a, b, c`
/// yields three (db, tbl) entries, `RENAME TABLE a TO x, b TO y` yields the
/// four source-and-destination pairs. Single-target DDL (`CREATE TABLE t`,
/// `ALTER TABLE t`, `TRUNCATE TABLE t`) returns a one-element vector via the
/// legacy [`extract_ddl_target_table`] path. Returns an empty vector for DDL
/// shapes we cannot decompose (in which case the caller must fall back to
/// applying the statement unconditionally rather than filtering it).
fn extract_ddl_target_tables(sql: &str, default_db: &str) -> Vec<(String, String)> {
    let stripped = strip_leading_sql_comments(sql);
    let tokens: Vec<&str> = stripped
        .split(|c: char| c.is_whitespace() || c == '(' || c == ';' || c == ',')
        .filter(|w| !w.is_empty())
        .collect();
    if tokens.len() < 2 {
        return Vec::new();
    }
    let head = tokens[0].to_ascii_uppercase();
    // Skip a leading run of keywords (like the private `skip` in the
    // single-target extractor).
    let skip_kw = |start: usize, kw: &[&str]| -> usize {
        let mut i = start;
        while i < tokens.len() {
            let t = tokens[i].to_ascii_uppercase();
            if kw.iter().any(|k| t == *k) {
                i += 1;
                continue;
            }
            break;
        }
        i
    };
    match head.as_str() {
        "DROP" => {
            let second = tokens.get(1).map(|s| s.to_ascii_uppercase());
            if matches!(second.as_deref(), Some("TABLE") | Some("TABLES")) {
                let start = skip_kw(1, &["TABLE", "TABLES", "IF", "EXISTS", "TEMPORARY"]);
                // Everything after the keyword run and before an optional
                // `CASCADE`/`RESTRICT` tail is a comma-separated target.
                tokens[start..]
                    .iter()
                    .take_while(|t| {
                        let u = t.to_ascii_uppercase();
                        u != "CASCADE" && u != "RESTRICT"
                    })
                    .map(|t| split_qualified(t, default_db))
                    .collect()
            } else {
                extract_ddl_target_table(sql, default_db)
                    .into_iter()
                    .collect()
            }
        }
        "RENAME" => {
            let start = skip_kw(1, &["TABLE", "TABLES"]);
            // `RENAME TABLE a TO x, b TO y` tokenises to
            // ["RENAME", "TABLE", "a", "TO", "x", "b", "TO", "y"] — every
            // triple contributes a source (position 0 mod 3) and a
            // destination (position 2 mod 3). MySQL evaluates the filter
            // against both so a `--replicate-ignore-table=old.t` filter
            // catches both the source and the destination name.
            let mut out = Vec::new();
            let mut i = start;
            while i < tokens.len() {
                out.push(split_qualified(tokens[i], default_db));
                if i + 2 < tokens.len() && tokens[i + 1].eq_ignore_ascii_case("TO") {
                    out.push(split_qualified(tokens[i + 2], default_db));
                }
                i += 3;
            }
            out
        }
        _ => extract_ddl_target_table(sql, default_db)
            .into_iter()
            .collect(),
    }
}

fn extract_ddl_target_table(sql: &str, default_db: &str) -> Option<(String, String)> {
    let sql = strip_leading_sql_comments(sql);
    let tokens: Vec<&str> = sql
        .split(|c: char| c.is_whitespace() || c == '(' || c == ';' || c == ',')
        .filter(|w| !w.is_empty())
        .collect();
    if tokens.len() < 2 {
        return None;
    }
    let head = tokens[0].to_ascii_uppercase();
    // Skip `kw` keywords starting at `start` and return the first
    // non-keyword token index.
    let skip = |start: usize, kw: &[&str]| -> usize {
        let mut i = start;
        while i < tokens.len() {
            let t = tokens[i].to_ascii_uppercase();
            if kw.iter().any(|k| t == *k) {
                i += 1;
                continue;
            }
            break;
        }
        i
    };
    let target = match head.as_str() {
        "TRUNCATE" => {
            let i = skip(1, &["TABLE"]);
            tokens.get(i)?
        }
        "DROP" => {
            let second = tokens.get(1).map(|s| s.to_ascii_uppercase());
            if matches!(second.as_deref(), Some("TABLE") | Some("TABLES")) {
                let i = skip(1, &["TABLE", "TABLES", "IF", "EXISTS", "TEMPORARY"]);
                tokens.get(i)?
            } else if matches!(second.as_deref(), Some("INDEX")) {
                // DROP INDEX <name> ON <table>
                let mut i = 2;
                while i + 1 < tokens.len() && !tokens[i].eq_ignore_ascii_case("ON") {
                    i += 1;
                }
                tokens.get(i + 1)?
            } else {
                return None;
            }
        }
        "CREATE" => {
            let i = skip(
                1,
                &[
                    "TEMPORARY",
                    "UNIQUE",
                    "FULLTEXT",
                    "SPATIAL",
                    "INDEX",
                    "TABLE",
                    "IF",
                    "NOT",
                    "EXISTS",
                    "OR",
                    "REPLACE",
                ],
            );
            // CREATE INDEX <name> ON <table> — find ON.
            let create_kind = tokens.get(1).map(|s| s.to_ascii_uppercase());
            if matches!(
                create_kind.as_deref(),
                Some("INDEX") | Some("UNIQUE") | Some("FULLTEXT") | Some("SPATIAL")
            ) {
                let mut j = i;
                while j < tokens.len() && !tokens[j].eq_ignore_ascii_case("ON") {
                    j += 1;
                }
                if j + 1 < tokens.len() {
                    tokens.get(j + 1)?
                } else {
                    tokens.get(i)?
                }
            } else {
                tokens.get(i)?
            }
        }
        "ALTER" | "RENAME" => {
            let i = skip(1, &["TABLE", "TABLES"]);
            tokens.get(i)?
        }
        _ => return None,
    };
    Some(split_qualified(target, default_db))
}

/// Extract a `DdlSessionState` from the source-side QUERY_EVENT
/// `status_vars` block. Any var the source did not emit (or that we don't
/// understand) is left at the conservative default.
fn ddl_session_state_from_query_event(
    ev: &mysql::binlog::events::QueryEvent<'_>,
) -> DdlSessionState {
    use mysql_common::binlog::events::StatusVarVal;
    let mut state = DdlSessionState::default();
    state.foreign_key_checks = true;
    state.unique_checks = true;
    state.explicit_defaults_for_timestamp = true;
    for var in ev.status_vars().iter() {
        match var.get_value() {
            Ok(StatusVarVal::Flags2(flags)) => {
                let bits: u32 = flags.0;
                // MySQL server flag positions (sql/sql_class.h):
                //   OPTION_NO_FOREIGN_KEY_CHECKS  = (1UL << 26) = 0x04000000
                //   OPTION_RELAXED_UNIQUE_CHECKS  = (1UL << 27) = 0x08000000
                state.foreign_key_checks = (bits & 0x0400_0000) == 0;
                state.unique_checks = (bits & 0x0800_0000) == 0;
            }
            Ok(StatusVarVal::SqlMode(mode)) => {
                state.sql_mode = mode.0;
            }
            Ok(StatusVarVal::TimeZone(bytes)) => {
                let s = String::from_utf8_lossy(bytes.as_bytes()).into_owned();
                if !s.is_empty() {
                    state.time_zone = Some(s);
                }
            }
            Ok(StatusVarVal::Charset {
                charset_client,
                collation_connection,
                collation_server,
            }) => {
                state.charset_client_id = Some(charset_client);
                state.collation_connection_id = Some(collation_connection);
                state.collation_server_id = Some(collation_server);
            }
            Ok(StatusVarVal::ExplicitDefaultsForTimestamp(flag)) => {
                state.explicit_defaults_for_timestamp = flag;
            }
            _ => {}
        }
    }
    state
}

fn decode_query_event_sql(
    event: &mysql::binlog::events::QueryEvent<'_>,
    state: &DdlSessionState,
) -> Result<String, QueryError> {
    let charset = state
        .charset_client_id
        .map(crate::charset::resolve_collation_id_checked)
        .transpose()?
        .map_or(crate::charset::DEFAULT_CHARSET, |info| info.charset);
    // QueryEvent::query() uses lossy UTF-8. The status block specifies the
    // actual source SQL encoding; malformed bytes must stop replication.
    crate::charset::decode_bytes(charset, event.query_raw())
}

/// Apply a `DdlSessionState` to the synthesized replica session so the
/// downstream DDL sees the same effective session as the source.
fn apply_ddl_session_state(session: &mut SessionContext, state: &DdlSessionState) {
    if state.sql_mode != 0 {
        session.sql_mode = crate::binlog::sql_mode_bits_to_str(state.sql_mode);
        session.temporal_sql_mode_bits =
            crate::engine::api::parse_temporal_sql_mode_bits(&session.sql_mode);
    }
    session.foreign_key_checks = state.foreign_key_checks;
    session.sysvars.unique_checks = state.unique_checks;
    if let Some(tz) = &state.time_zone {
        if !tz.is_empty() {
            session.sysvars.time_zone = tz.clone();
        }
    }
    if let Some(id) = state.charset_client_id {
        if let Some(info) = crate::charset::resolve_collation_id(id) {
            session.character_set_client = info.charset.to_string();
        }
    }
    if let Some(id) = state.collation_connection_id {
        if let Some(info) = crate::charset::resolve_collation_id(id) {
            session.character_set_connection = info.charset.to_string();
            session.collation_connection = info.name.to_string();
        }
    }
}

/// A timeout leaves the live handle in its slot. Only a completed join may
/// clear it; callers must not reset stop flags while a previous worker lives.
fn join_thread_with_timeout(
    slot: &mut Option<JoinHandle<()>>,
    timeout: Option<Duration>,
) -> Result<(), &'static str> {
    let Some(handle) = slot.as_ref() else {
        return Ok(());
    };
    let start = Instant::now();
    while !handle.is_finished() {
        #[cfg(test)]
        let virtual_elapsed = tests::observe_join_wait(timeout);
        if timeout.is_some_and(|timeout| {
            #[cfg(test)]
            if let Some(elapsed) = virtual_elapsed {
                return elapsed >= timeout;
            }
            start.elapsed() >= timeout
        }) {
            return Err("replication thread stop timed out; worker is still stopping");
        }
        thread::sleep(Duration::from_millis(10));
    }
    slot.take()
        .unwrap()
        .join()
        .map_err(|_| "replication thread panicked")
}

#[cfg(test)]
#[test]
fn shutdown_replication_retains_timed_out_worker_until_joined() {
    let (release, blocked) = std::sync::mpsc::channel();
    let mut slot = Some(thread::spawn(move || {
        blocked.recv().unwrap();
    }));
    assert!(join_thread_with_timeout(&mut slot, Some(Duration::ZERO)).is_err());
    assert!(slot.is_some(), "timed out writer must remain joinable");
    let (entered, observed) = std::sync::mpsc::channel();
    assert!(tests::past_old_join_deadline(entered, || {
        join_thread_with_timeout(&mut slot, Some(Duration::from_secs(15)))
    })
    .is_err());
    assert_eq!(observed.try_recv().unwrap(), Some(Duration::from_secs(15)));
    assert!(slot.is_some(), "virtual timeout must retain the worker too");
    release.send(()).unwrap();
    join_thread_with_timeout(&mut slot, None).unwrap();
    assert!(slot.is_none());
}

#[cfg(test)]
#[test]
fn shutdown_replication_reports_worker_panic() {
    let mut slot = Some(thread::spawn(|| panic!("injected replication failure")));
    assert_eq!(
        join_thread_with_timeout(&mut slot, None),
        Err("replication thread panicked")
    );
    assert!(slot.is_none());
}

fn seconds_behind(event_ts: u64) -> Option<u64> {
    if event_ts == 0 {
        return None;
    }
    Some(unix_secs().saturating_sub(event_ts))
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn env_bool(name: &str) -> bool {
    crate::util::env::flag(name, false)
}

fn env_bool_default(name: &str, default: bool) -> bool {
    crate::util::env::flag(name, default)
}

fn env_parse<T: FromStr>(name: &str, default: T) -> T {
    crate::util::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_csv(name: &str) -> Vec<String> {
    crate::util::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(|item| item.trim().to_string())
                .filter(|item| !item.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

fn replication_dir(data_dir: &Path) -> PathBuf {
    crate::storage::naming::system_replication_dir(data_dir)
}

fn channel_tombstone(data_dir: &Path, channel: &str) -> PathBuf {
    replication_dir(data_dir).join(format!("{channel}.disabled"))
}

fn channel_path(data_dir: &Path, channel: &str) -> PathBuf {
    replication_dir(data_dir)
        .join("channels")
        .join(format!("{channel}.json"))
}

// ---------------------------------------------------------------------------
// SHOW RELAYLOG EVENTS — P0-B2
// ---------------------------------------------------------------------------

/// Map a `RelayOp` variant to the closest MySQL binlog event-type label so
/// existing tooling (orchestrator, mha, mysqlbinlog) recognises the row
/// shape. mydb's relay log is JSON-Lines, not the binary MySQL format —
/// the labels are a best-effort translation.
fn relay_op_event_type(op: &RelayOp, gtid: &Option<String>) -> &'static str {
    if gtid.is_some()
        && matches!(
            op,
            RelayOp::Ddl { .. }
                | RelayOp::TxnCommit { .. }
                | RelayOp::WriteRows { .. }
                | RelayOp::UpdateRows { .. }
                | RelayOp::DeleteRows { .. }
        )
    {
        // The Gtid_log_event precedes the actual statement in MySQL's
        // binary format. Surface it once per record-with-gtid.
        return "Gtid_log_event";
    }
    match op {
        RelayOp::RawRows { .. } => "Rows_log_event",
        RelayOp::Noop => "Rotate_log_event",
        RelayOp::TxnCommit { .. } => "Xid_log_event",
        RelayOp::XaPrepare { .. } => "XA_prepare_log_event",
        RelayOp::XaFinish { .. } => "Query_log_event",
        RelayOp::Ddl { .. } => "Query_log_event",
        RelayOp::WriteRows { .. } => "Write_rows_log_event_v2",
        RelayOp::UpdateRows { .. } => "Update_rows_log_event_v2",
        RelayOp::DeleteRows { .. } => "Delete_rows_log_event_v2",
    }
}

/// Build the `Info` text MySQL clients expect — short, one-line, mirroring
/// `mysqlbinlog -v` output where possible.
fn relay_op_info(op: &RelayOp, gtid: &Option<String>) -> String {
    if let Some(gtid) = gtid {
        return format!("SET @@SESSION.GTID_NEXT= '{gtid}'");
    }
    match op {
        RelayOp::RawRows { event_type, .. } => format!("source rows event type {event_type}"),
        RelayOp::Noop => String::new(),
        RelayOp::TxnCommit { event_count, .. } => format!("COMMIT /* events={event_count} */"),
        RelayOp::XaPrepare { xid, .. } => format!("XA PREPARE {}", xid.render_data(true)),
        RelayOp::XaFinish { xid, commit, .. } => format!(
            "XA {} {}",
            if *commit { "COMMIT" } else { "ROLLBACK" },
            xid.render_data(true)
        ),
        RelayOp::Ddl { database, sql, .. } => {
            if database.is_empty() {
                sql.clone()
            } else {
                format!("use `{database}`; {sql}")
            }
        }
        RelayOp::WriteRows {
            database,
            table,
            rows,
        } => format!(
            "table_id: {database}.{table} flags: STMT_END_F rows: {}",
            rows.len()
        ),
        RelayOp::UpdateRows {
            database,
            table,
            rows,
        } => format!(
            "table_id: {database}.{table} flags: STMT_END_F rows: {}",
            rows.len()
        ),
        RelayOp::DeleteRows {
            database,
            table,
            rows,
        } => format!(
            "table_id: {database}.{table} flags: STMT_END_F rows: {}",
            rows.len()
        ),
    }
}

fn show_relaylog_events(sql: &str) -> Result<QueryResponse, QueryError> {
    let req = parse_show_relaylog_events_clause(sql)?;
    let columns = show_relaylog_events_columns();
    // Resolve which channel(s) to scan. mydb's relay logs live at
    // `<data>/system/replication/<channel>.relay.jsonl`. Without an active
    // manager (or matching channel), MySQL returns an empty result set
    // rather than an error; we mirror that behaviour.
    let Some(manager) = global_manager() else {
        return Ok(QueryResponse::ResultSet(ResultSet {
            columns,
            rows: Vec::new(),
        }));
    };
    let channel = req
        .channel
        .clone()
        .unwrap_or_else(|| DEFAULT_CHANNEL.to_string());
    let path = relay_log_path(&manager.data_dir, &channel);
    let bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(QueryResponse::ResultSet(ResultSet {
                columns,
                rows: Vec::new(),
            }));
        }
        Err(err) => return Err(io_error("read relay log", err)),
    };
    let log_name = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| channel.clone());
    if let Some(requested) = &req.log_name {
        // MySQL errors with 1220 ER_UNKNOWN_TARGET_BINLOG when the IN
        // clause names a log that doesn't exist for this channel.
        if !log_name.eq_ignore_ascii_case(requested) && !requested.eq_ignore_ascii_case("") {
            return Err(error(
                crate::error::catalog::ER_ERROR_WHEN_EXECUTING_COMMAND,
                format!(
                    "Error when executing command SHOW RELAYLOG EVENTS: \
                         Could not find target log '{requested}'"
                ),
            ));
        }
    }
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut pos: u64 = 0;
    let mut produced = 0usize;
    let mut skipped = 0usize;
    let skip = req.offset;
    let limit = req.limit.unwrap_or(usize::MAX);
    for line in bytes.split(|&b| b == b'\n') {
        let line_len = line.len() as u64 + 1; // +1 for the trailing \n
        let start_pos = pos;
        pos = pos.saturating_add(line_len);
        if line.is_empty() {
            continue;
        }
        if start_pos < req.from {
            continue;
        }
        let record: RelayRecord = match serde_json::from_slice(line) {
            Ok(r) => r,
            // A corrupt tail just terminates the scan — matches MySQL
            // mysqlbinlog's "stop at first bad event" behaviour.
            Err(_) => break,
        };
        if skipped < skip {
            skipped += 1;
            continue;
        }
        if produced >= limit {
            break;
        }
        let event_type = relay_op_event_type(&record.op, &record.gtid).to_string();
        let info = relay_op_info(&record.op, &record.gtid);
        rows.push(vec![
            Value::String(log_name.clone()),
            Value::UInt(start_pos),
            Value::String(event_type),
            Value::Int(0), // Server_id (not tracked per-event in mydb relay)
            Value::UInt(start_pos + line_len),
            Value::String(info),
        ]);
        produced += 1;
    }
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

fn relay_reclaim_path(data_dir: &Path, channel: &str) -> PathBuf {
    replication_dir(data_dir).join(format!("{channel}.relay-reclaim"))
}

fn sync_replication_directory(data_dir: &Path) -> Result<(), QueryError> {
    crate::platform::fs::sync_path(replication_dir(data_dir))
        .map_err(|e| io_error("sync replication directory", e))
}

fn recover_relay_reclaim(data_dir: &Path, channel: &str) -> Result<(), QueryError> {
    let marker = relay_reclaim_path(data_dir, channel);
    let bytes = match fs::read(&marker) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(io_error("read relay reclaim", e)),
    };
    let persisted: PersistedChannel = serde_json::from_slice(&bytes).map_err(|e| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("corrupt relay reclaim: {e}"),
        )
    })?;
    if persisted.config.channel != channel || persisted.status.relay_log_pos != 0 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "invalid relay reclaim identity/position",
        ));
    }
    let retained = marker.with_extension("retained");
    if retained.exists() {
        fs::rename(&retained, relay_log_path(data_dir, channel))
            .map_err(|e| io_error("install retained relay", e))?;
        sync_replication_directory(data_dir)?;
    }
    // If the retained file is absent, the rename already completed before
    // the crash. Never truncate the newly installed tail on replay.
    persist_channel(data_dir, &persisted)?;
    sync_replication_directory(data_dir)?;
    fs::remove_file(marker).map_err(|e| io_error("remove relay reclaim", e))?;
    sync_replication_directory(data_dir)
}

fn relay_log_path(data_dir: &Path, channel: &str) -> PathBuf {
    replication_dir(data_dir).join(format!("{channel}.relay.jsonl"))
}

/// Truncate a relay log to its last complete transaction boundary and return
/// that offset. A crash can leave, past the last fsynced TxnCommit/DDL
/// record, a tail of intra-transaction row records (page-cache flushed but
/// never committed) and possibly one torn final line. If left in place the IO
/// thread would append re-fetched events *after* that stale tail, and the SQL
/// thread would then splice the stale partial transaction together with the
/// re-fetched one — double-applying rows — besides spinning a core because
/// the durable gate (= raw file length) never lines up with a commit boundary.
/// Must be called with no IO thread running (start_channel, pre-spawn).
#[cfg(test)]
fn truncate_relay_to_last_commit(path: &Path) -> Result<u64, QueryError> {
    Ok(recover_relay_boundary(path)?.0)
}

fn recover_relay_boundary(path: &Path) -> Result<(u64, Option<RelayRecord>), QueryError> {
    if !path.exists() {
        return Ok((0, None));
    }
    let file = File::open(path).map_err(|err| io_error("open relay log", err))?;
    let mut reader = BufReader::new(file);
    let mut offset: u64 = 0;
    let mut last_commit: u64 = 0;
    let mut boundary = None;
    let mut buf = Vec::new();
    loop {
        buf.clear();
        let read = reader
            .read_until(b'\n', &mut buf)
            .map_err(|err| io_error("read relay log", err))?;
        if read == 0 {
            break;
        }
        // A line without a trailing newline is a torn final write — stop; the
        // last good boundary is whatever full commit record preceded it.
        if !matches!(buf.last(), Some(b'\n')) {
            break;
        }
        let next_offset = offset.saturating_add(read as u64);
        let trimmed: &[u8] = {
            let mut end = buf.len();
            while matches!(buf.get(end.wrapping_sub(1)), Some(b'\n' | b'\r')) {
                end -= 1;
            }
            &buf[..end]
        };
        if trimmed.is_empty() {
            offset = next_offset;
            continue;
        }
        match serde_json::from_slice::<RelayRecord>(trimmed) {
            Ok(record) => {
                if matches!(
                    record.op,
                    RelayOp::TxnCommit { .. }
                        | RelayOp::XaPrepare { .. }
                        | RelayOp::XaFinish { .. }
                        | RelayOp::Ddl {
                            transactional: false,
                            ..
                        }
                ) {
                    last_commit = next_offset;
                    boundary = Some(record);
                }
                offset = next_offset;
            }
            // A parse failure mid-file is a torn record: stop at the last
            // known commit boundary.
            Err(_) => break,
        }
    }
    let len = std::fs::metadata(path)
        .map(|meta| meta.len())
        .unwrap_or(last_commit);
    if last_commit < len {
        let f = OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|err| io_error("open relay log for truncate", err))?;
        f.set_len(last_commit)
            .map_err(|err| io_error("truncate relay log tail", err))?;
        f.sync_all()
            .map_err(|err| io_error("fsync relay log truncate", err))?;
    }
    Ok((last_commit, boundary))
}

fn persist_channel(data_dir: &Path, persisted: &PersistedChannel) -> Result<(), QueryError> {
    let dir = replication_dir(data_dir).join("channels");
    fs::create_dir_all(&dir).map_err(|err| io_error("create replication directory", err))?;
    let path = channel_path(data_dir, &persisted.config.channel);
    // Unique temp name per writer: the IO and SQL threads both persist
    // progress on their own commit boundaries, at nearly the same frequency.
    // A shared fixed `<channel>.json.tmp` let the two `File::create(O_TRUNC)`
    // + interleaved writes clobber each other, so the installed
    // `<channel>.json` could be a byte-spliced half-write that fails to parse
    // on restart (1030) and bricks the channel. A pid+seq suffix keeps each
    // writer's temp file private; the final atomic `rename` is still the
    // single publish point.
    static PERSIST_SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = PERSIST_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp = path.with_extension(format!("json.tmp.{}.{seq}", std::process::id()));
    let write = || -> Result<(), QueryError> {
        let mut file =
            File::create(&tmp).map_err(|err| io_error("create replication metadata", err))?;
        serde_json::to_writer_pretty(&mut file, persisted).map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("serialize replication metadata failed: {err}"),
            )
        })?;
        file.write_all(b"\n")
            .and_then(|_| file.sync_all())
            .map_err(|err| io_error("sync replication metadata", err))?;
        fs::rename(&tmp, &path).map_err(|err| io_error("install replication metadata", err))?;
        Ok(())
    };
    let result = write();
    if result.is_err() {
        // Don't leave the private temp file behind on failure.
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn io_error(action: &str, err: std::io::Error) -> QueryError {
    crate::util::io_error::map_io_error(action, err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_common::binlog::consts::{EventFlags, EventType, TransactionPayloadCompressionType};
    use mysql_common::binlog::events::{BinlogEventHeader, QueryEvent, TransactionPayloadEvent};
    use mysql_common::proto::MySerialize;

    struct JoinWaitClock {
        elapsed: Duration,
        entered: Option<std::sync::mpsc::Sender<Option<Duration>>>,
    }

    thread_local! {
        static JOIN_WAIT_CLOCK: std::cell::RefCell<Option<JoinWaitClock>> = const {
            std::cell::RefCell::new(None)
        };
    }

    /// Observe the actual wait loop and move its clock beyond the old SQL
    /// deadline. IO timeout tests and all production waits keep their clocks.
    pub(super) fn observe_join_wait(timeout: Option<Duration>) -> Option<Duration> {
        JOIN_WAIT_CLOCK.with(|slot| {
            let mut clock = slot.borrow_mut();
            let clock = clock.as_mut()?;
            if let Some(entered) = clock.entered.take() {
                entered.send(timeout).unwrap();
            }
            Some(clock.elapsed)
        })
    }

    pub(super) fn past_old_join_deadline<T>(
        entered: std::sync::mpsc::Sender<Option<Duration>>,
        run: impl FnOnce() -> T,
    ) -> T {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                JOIN_WAIT_CLOCK.with(|slot| *slot.borrow_mut() = None);
            }
        }
        JOIN_WAIT_CLOCK.with(|slot| {
            assert!(slot.borrow().is_none());
            *slot.borrow_mut() = Some(JoinWaitClock {
                elapsed: Duration::from_secs(16),
                entered: Some(entered),
            });
        });
        let _reset = Reset;
        run()
    }

    #[test]
    fn shutdown_joins_applier_before_checkpoint_past_old_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute("CREATE TABLE shutdown_applied (id INT PRIMARY KEY)")
            .unwrap();
        let manager = ReplicationManager {
            data_dir: dir.path().to_path_buf(),
            engine: Arc::clone(&engine),
            channels: Mutex::new(HashMap::new()),
        };
        manager
            .change_source(
                parse_change_source(
                    "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_USER='repl'",
                )
                .unwrap(),
            )
            .unwrap();
        let runtime = manager.ensure_channel(DEFAULT_CHANNEL).unwrap();
        let (release, blocked) = std::sync::mpsc::channel();
        let writer = Arc::clone(&engine);
        *runtime.sql_join.lock().unwrap() = Some(thread::spawn(move || {
            blocked.recv().unwrap();
            with_replication_applier(|| writer.execute("INSERT INTO shutdown_applied VALUES (9)"))
                .unwrap();
        }));
        runtime.sql_stop.store(true, Ordering::Release);
        let err = manager
            .start_channel_threads(DEFAULT_CHANNEL, None, ReplThreadSel::Sql)
            .unwrap_err();
        assert!(err.message().contains("still stopping"));
        let (done, completed) = std::sync::mpsc::channel();
        let (entered, waiting) = std::sync::mpsc::channel();
        let shutdown = thread::spawn(move || {
            past_old_join_deadline(entered, || manager.stop_all_channels()).unwrap();
            manager.engine.shutdown().unwrap();
            done.send(()).unwrap();
        });
        // Old code discarded the SQL applier after 15s and checkpointed while
        // it was still alive. Advance only the join clock, keeping the writer
        // gated until the shutdown path actually enters its unbounded wait.
        let timeout = waiting.recv_timeout(Duration::from_secs(5));
        let premature = completed.try_recv().is_ok();
        release.send(()).unwrap();
        shutdown.join().unwrap();
        assert_eq!(
            timeout.unwrap(),
            None,
            "shutdown must join without a deadline"
        );
        assert!(!premature, "checkpoint ran before applier exited");
        let scan = crate::wal::LogManager::scan(dir.path()).unwrap();
        assert!(matches!(
            scan.records.as_slice(),
            [crate::wal::WalRecord {
                payload: crate::wal::WalPayload::CheckpointEnd { .. },
                ..
            }]
        ));
        let QueryResponse::ResultSet(rows) =
            engine.execute("SELECT * FROM shutdown_applied").unwrap()
        else {
            panic!("expected rows");
        };
        assert_eq!(rows.rows, vec![vec![Value::Int(9)]]);
    }

    #[test]
    fn replication_source_preflight_fails_closed() {
        assert!(require_replication_source_setting("binlog_format", Some("row"), "ROW").is_ok());
        let missing = require_replication_source_setting("binlog_format", None, "ROW").unwrap_err();
        assert!(missing.is(crate::error::catalog::CR_SERVER_LOST));
        let incompatible =
            require_replication_source_setting("binlog_row_image", Some("MINIMAL"), "FULL")
                .unwrap_err();
        assert!(incompatible.is(crate::error::catalog::ER_NOT_SUPPORTED_YET));
        // Do not leak the test's thread-local poison flag into later tests on
        // the same nextest worker thread.
        assert!(take_io_error_fatal());
    }

    /// P1-4 regression: mydb_replica_read_only must reject the same
    /// physical-write DDL surface that Role::Replica already rejects on the
    /// mydb→mydb path (P0-7). Previously RENAME/OPTIMIZE/REPAIR/SEQUENCE/
    /// TABLESPACE fell through the allowlist and silently mutated state under
    /// the row-based applier.
    #[test]
    fn is_replica_write_statement_covers_rename_optimize_sequence_tablespace() {
        for sql in [
            "RENAME TABLE t TO t2",
            "OPTIMIZE TABLE t",
            "REPAIR TABLE t",
            "CREATE SEQUENCE s1",
            "ALTER SEQUENCE s1 INCREMENT BY 5",
            "DROP SEQUENCE s1",
            "CREATE TABLESPACE ts1 ADD DATAFILE 'x'",
            "DROP TABLESPACE ts1",
            // sanity anchor — already covered pre-P1-4
            "INSERT INTO t VALUES (1)",
        ] {
            let stmt = crate::parser::parse_statement(sql)
                .unwrap_or_else(|e| panic!("parse {sql:?}: {e:?}"));
            assert!(
                is_replica_write_statement(&stmt),
                "replica read-only must classify as write: {sql}"
            );
        }
    }

    #[test]
    fn change_source_parser_accepts_mysql8_names() {
        let cfg = parse_change_source(
            "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=3307, SOURCE_USER='repl', SOURCE_PASSWORD='pw', SOURCE_LOG_FILE='bin.000001', SOURCE_LOG_POS=42 FOR CHANNEL 'c1'",
        )
        .unwrap();
        assert_eq!(cfg.channel, "c1");
        assert_eq!(cfg.source_host, "127.0.0.1");
        assert_eq!(cfg.source_port, 3307);
        assert_eq!(cfg.source_user, "repl");
        assert_eq!(cfg.source_password, "pw");
        assert_eq!(cfg.source_log_file, "bin.000001");
        assert_eq!(cfg.source_log_pos, 42);
    }

    #[test]
    fn timestamp2_text_decodes_as_utc_instant_after_2038() {
        assert_eq!(
            timestamp_text_to_datetime("0"),
            Some(crate::types::ZERO_DATETIME)
        );
        assert_eq!(
            timestamp_text_to_datetime("0.000000"),
            Some(crate::types::ZERO_DATETIME)
        );
        let dt = chrono::NaiveDate::from_ymd_opt(2040, 7, 15)
            .unwrap()
            .and_hms_micro_opt(10, 20, 30, 123_456)
            .unwrap();
        let expected = dt.and_utc().timestamp_micros();
        let text = format!("{}.{:06}", expected / 1_000_000, expected % 1_000_000);
        assert_eq!(timestamp_text_to_datetime(&text), Some(expected));
    }

    #[test]
    fn like_match_handles_percent_underscore_and_anchors() {
        assert!(like_match("%", "anything"));
        assert!(like_match("test.%", "test.users"));
        assert!(!like_match("test.%", "other.users"));
        assert!(like_match("test._sers", "test.users"));
        assert!(!like_match("test._sers", "test.user"));
        assert!(like_match("", ""));
        assert!(!like_match("a", ""));
    }

    #[test]
    fn filter_table_combines_do_ignore_and_wildcards_per_mysql_semantics() {
        let mut cfg = ReplicationChannelConfig::default();
        cfg.do_databases = vec!["app".to_string()];
        cfg.do_tables = vec!["app.users".to_string()];
        cfg.wild_do_tables = vec!["app.audit_%".to_string()];
        cfg.ignore_tables = vec!["app.audit_secret".to_string()];
        // do_db gates everything
        assert!(!filter_allows_table(&cfg, "other", "users"));
        // exact do_table match
        assert!(filter_allows_table(&cfg, "app", "users"));
        // wildcard do_table match
        assert!(filter_allows_table(&cfg, "app", "audit_login"));
        // exact ignore wins over wild allow
        assert!(!filter_allows_table(&cfg, "app", "audit_secret"));
        // missing allow → deny
        assert!(!filter_allows_table(&cfg, "app", "orders"));
        // wild ignore wins over allow
        cfg.wild_ignore_tables.push("app.audit_%".to_string());
        assert!(!filter_allows_table(&cfg, "app", "audit_login"));
    }

    // B7: DDL database-level filtering must use the connection's default
    // schema, not the schema named by a qualified target table. MySQL:
    // `USE foo; CREATE TABLE otherdb.t` under `--replicate-do-db=foo`
    // replicates, because the default db `foo` is allowed — the target's own
    // `otherdb` must NOT be re-checked against do-db.
    #[test]
    fn ddl_do_db_uses_default_schema_not_qualified_target() {
        let mut cfg = ReplicationChannelConfig::default();
        cfg.do_databases = vec!["foo".to_string()];

        // Default db `foo` passes the db-level gate.
        assert!(filter_allows_database(&cfg, "foo"));
        // The qualified target lives in `otherdb`, which is NOT in do-db.
        let (db, tbl) = extract_ddl_target_table("CREATE TABLE otherdb.t (id INT)", "foo").unwrap();
        assert_eq!((db.as_str(), tbl.as_str()), ("otherdb", "t"));
        // Table-rules-only must allow it (no table-level do/ignore rules),
        // whereas the full filter would wrongly re-apply do-db to `otherdb`.
        assert!(filter_allows_table_rules(&cfg, &db, &tbl));
        assert!(!filter_allows_table(&cfg, &db, &tbl));

        // Table-level rules still bite on the resolved target.
        cfg.ignore_tables = vec!["otherdb.t".to_string()];
        assert!(!filter_allows_table_rules(&cfg, &db, &tbl));
    }

    // B8: leading SQL comments must be stripped before DDL prefix/target
    // classification. Plain block comments are dropped; version-conditional
    // `/*! */` comments are unwrapped so their inner statement classifies.
    #[test]
    fn leading_comments_stripped_for_ddl_classification() {
        // Plain block comment hiding a user-management statement.
        assert!(is_user_management_ddl(
            "/* audit */ GRANT SELECT ON *.* TO u"
        ));
        assert!(is_user_management_ddl("  -- note\n GRANT ALL ON a.* TO u"));
        assert!(is_user_management_ddl("# hash\nDROP USER u"));
        // Version-conditional comment is unwrapped, revealing the real head.
        assert!(is_user_management_ddl("/*!80000 CREATE USER u */"));
        assert!(!is_user_management_ddl("/*!99999 CREATE USER u */"));
        assert!(!is_user_management_ddl("--x\nCREATE USER u"));
        assert!(!is_user_management_ddl(
            "/* c */ CREATE TABLE app.t (id INT)"
        ));

        // Target extraction sees through both comment forms.
        let (db, tbl) =
            extract_ddl_target_table("/* c */ CREATE TABLE app.t (id INT)", "def").unwrap();
        assert_eq!((db.as_str(), tbl.as_str()), ("app", "t"));
        let (db, tbl) =
            extract_ddl_target_table("/*!40000 ALTER TABLE shop.orders ADD x INT */", "def")
                .unwrap();
        assert_eq!((db.as_str(), tbl.as_str()), ("shop", "orders"));
        // Unqualified target still resolves against the default db.
        let (db, tbl) = extract_ddl_target_table("/* x */ TRUNCATE TABLE cart", "shop").unwrap();
        assert_eq!((db.as_str(), tbl.as_str()), ("shop", "cart"));
    }

    #[test]
    fn audit14_idle_caught_up_replica_reports_zero_lag() {
        let cfg = ReplicationChannelConfig::default();
        let tmp = tempfile::tempdir().unwrap();
        let mut status = ReplicationStatus::from_config(&cfg, tmp.path());
        status.running = true;
        status.sql_running = true;
        status.source_log_file = "binlog.000001".into();
        status.exec_source_log_file = status.source_log_file.clone();
        status.source_log_pos = 100;
        status.exec_source_log_pos = 100;
        status.seconds_behind_source = Some(109);
        assert_eq!(status.row(&cfg)[32], Value::UInt(0));
        status.source_log_pos = 200;
        assert_eq!(status.row(&cfg)[32], Value::UInt(109));
        status.sql_running = false;
        assert_eq!(status.row(&cfg)[32], Value::Null);
    }

    #[test]
    fn show_replica_status_reports_real_sql_errno_and_timestamp() {
        let cfg = ReplicationChannelConfig::default();
        let tmp = tempfile::tempdir().unwrap();
        let mut status = ReplicationStatus::from_config(&cfg, tmp.path());
        status.last_sql_error = Some(ReplThreadError {
            code: 1032,
            message: "replication update could not find row".to_string(),
            at_unix_secs: 1_700_000_000,
        });
        let row = status.row(&cfg);
        // Col 37 Last_SQL_Errno (idx 36) / col 38 Last_SQL_Error (idx 37).
        assert_eq!(row[36], Value::UInt(1032));
        assert!(matches!(&row[37], Value::String(s) if s.contains("could not find row")));
        // Col 49 Last_SQL_Error_Timestamp (idx 48) rendered non-empty.
        assert!(matches!(&row[48], Value::String(s) if !s.is_empty()));
        // Col 35 Last_IO_Errno (idx 34) stays 0 — the failure was SQL-side.
        assert_eq!(row[34], Value::UInt(0));
        assert!(matches!(&row[47], Value::String(s) if s.is_empty()));
        // Col 19 Last_Errno (idx 18) mirrors the SQL thread.
        assert_eq!(row[18], Value::UInt(1032));
    }

    #[test]
    fn show_replica_status_reports_real_io_errno() {
        let cfg = ReplicationChannelConfig::default();
        let tmp = tempfile::tempdir().unwrap();
        let mut status = ReplicationStatus::from_config(&cfg, tmp.path());
        status.last_io_error = Some(ReplThreadError {
            code: 2003,
            message: "connect replication source failed".to_string(),
            at_unix_secs: 1_700_000_000,
        });
        let row = status.row(&cfg);
        // Col 35 Last_IO_Errno (idx 34) / col 36 Last_IO_Error (idx 35).
        assert_eq!(row[34], Value::UInt(2003));
        assert!(matches!(&row[35], Value::String(s) if s.contains("connect replication source")));
        // Col 48 Last_IO_Error_Timestamp (idx 47) non-empty.
        assert!(matches!(&row[47], Value::String(s) if !s.is_empty()));
        // SQL side stays clean.
        assert_eq!(row[36], Value::UInt(0));
        // Last_Errno mirrors IO when no SQL error present.
        assert_eq!(row[18], Value::UInt(2003));
    }

    #[test]
    fn change_source_honors_connect_retry_seconds() {
        let cfg = parse_change_source(
            "CHANGE REPLICATION SOURCE TO SOURCE_HOST='h', SOURCE_USER='u', \
             SOURCE_PASSWORD='p', SOURCE_CONNECT_RETRY=7",
        )
        .unwrap();
        // MySQL seconds → mydb milliseconds.
        assert_eq!(cfg.connect_retry_ms, 7000);
    }

    #[test]
    fn change_source_accepts_advisory_mysql_clauses_without_error() {
        // Clauses a vanilla MySQL orchestrator sends that mydb treats as
        // advisory must parse successfully (accept + warning), NOT hard-error.
        let cfg = parse_change_source(
            "CHANGE REPLICATION SOURCE TO SOURCE_HOST='h', SOURCE_USER='u', SOURCE_PASSWORD='p', \
             SOURCE_RETRY_COUNT=10, SOURCE_HEARTBEAT_PERIOD=30, GET_SOURCE_PUBLIC_KEY=1, \
             SOURCE_ZSTD_COMPRESSION_LEVEL=3, SOURCE_COMPRESSION_ALGORITHMS='zstd', \
             IGNORE_SERVER_IDS='(1,2)', REQUIRE_ROW_FORMAT=1, \
             REQUIRE_TABLE_PRIMARY_KEY_CHECK='STREAM', SOURCE_TLS_VERSION='TLSv1.3'",
        )
        .expect("advisory clauses should be accepted");
        assert_eq!(cfg.source_host, "h");
    }

    #[test]
    fn change_source_still_rejects_truly_unknown_clause() {
        let err = parse_change_source(
            "CHANGE REPLICATION SOURCE TO SOURCE_HOST='h', SOURCE_USER='u', \
             SOURCE_PASSWORD='p', TOTALLY_MADE_UP_OPTION=1",
        )
        .expect_err("unknown clause must error");
        assert_eq!(err.code(), 1235);
        assert!(err.message().contains("TOTALLY_MADE_UP_OPTION"));
    }

    #[test]
    fn parse_start_replica_handles_until_position() {
        let (channel, until, sel) = parse_start_replica(
            "START REPLICA UNTIL SOURCE_LOG_FILE='bin.000003', SOURCE_LOG_POS=512 FOR CHANNEL 'c1'",
        )
        .unwrap();
        assert_eq!(sel, ReplThreadSel::Both);
        assert_eq!(channel.as_deref(), Some("c1"));
        match until.unwrap() {
            UntilCondition::Position { file, pos } => {
                assert_eq!(file, "bin.000003");
                assert_eq!(pos, 512);
            }
            other => panic!("expected Position, got {:?}", other),
        }
    }

    #[test]
    fn parse_start_replica_handles_until_sql_after_gtids() {
        let (_, until, _) =
            parse_start_replica("START REPLICA UNTIL SQL_AFTER_GTIDS = 'uuid:1-9'").unwrap();
        match until.unwrap() {
            UntilCondition::SqlAfterGtids(set) => assert_eq!(set, "uuid:1-9"),
            other => panic!("expected SqlAfterGtids, got {:?}", other),
        }
    }

    #[test]
    fn parse_start_replica_handles_until_mts_gaps() {
        let (_, until, _) = parse_start_replica("START REPLICA UNTIL SQL_AFTER_MTS_GAPS").unwrap();
        assert!(matches!(until, Some(UntilCondition::SqlAfterMtsGaps)));
    }

    #[test]
    fn parse_start_replica_selects_individual_threads() {
        let (_, _, sel) = parse_start_replica("START REPLICA IO_THREAD").unwrap();
        assert_eq!(sel, ReplThreadSel::Io);
        let (_, _, sel) = parse_start_replica("START REPLICA SQL_THREAD").unwrap();
        assert_eq!(sel, ReplThreadSel::Sql);
        let (_, _, sel) = parse_start_replica("START REPLICA IO_THREAD, SQL_THREAD").unwrap();
        assert_eq!(sel, ReplThreadSel::Both);
        // SQL_THREAD with UNTIL + channel still parses.
        let (channel, until, sel) = parse_start_replica(
            "START REPLICA SQL_THREAD UNTIL SQL_AFTER_MTS_GAPS FOR CHANNEL 'c1'",
        )
        .unwrap();
        assert_eq!(sel, ReplThreadSel::Sql);
        assert_eq!(channel.as_deref(), Some("c1"));
        assert!(matches!(until, Some(UntilCondition::SqlAfterMtsGaps)));
    }

    #[test]
    fn parse_stop_replica_selects_individual_threads() {
        let (channel, sel) =
            parse_stop_replica("STOP REPLICA SQL_THREAD", &["STOP REPLICA"]).unwrap();
        assert_eq!(sel, ReplThreadSel::Sql);
        assert_eq!(channel, None);
        let (channel, sel) =
            parse_stop_replica("STOP REPLICA IO_THREAD FOR CHANNEL 'c2'", &["STOP REPLICA"])
                .unwrap();
        assert_eq!(sel, ReplThreadSel::Io);
        assert_eq!(channel.as_deref(), Some("c2"));
        let (_, sel) = parse_stop_replica("STOP REPLICA", &["STOP REPLICA"]).unwrap();
        assert_eq!(sel, ReplThreadSel::Both);
    }

    #[test]
    fn parse_start_replica_rejects_unknown_until_option() {
        let err = parse_start_replica("START REPLICA UNTIL JUNK_OPTION='foo'").unwrap_err();
        assert!(
            err.is(crate::error::catalog::ER_NOT_SUPPORTED_YET)
                || err.is(crate::error::catalog::ER_PARSE_ERROR)
        );
    }

    #[test]
    fn parse_start_replica_rejects_mixed_until_options() {
        let err = parse_start_replica(
            "START REPLICA UNTIL SOURCE_LOG_FILE='f', SOURCE_LOG_POS=1, SQL_AFTER_GTIDS='g'",
        )
        .unwrap_err();
        assert_eq!(err.code(), 1064);
    }

    #[test]
    fn change_source_parser_accepts_source_delay() {
        let cfg = parse_change_source(
            "CHANGE REPLICATION SOURCE TO SOURCE_HOST='h', SOURCE_USER='u', SOURCE_PASSWORD='p', SOURCE_DELAY=30",
        )
        .unwrap();
        assert_eq!(cfg.source_delay_secs, 30);
    }

    #[test]
    fn change_source_parser_accepts_filter_clauses() {
        let cfg = parse_change_source(
            "CHANGE REPLICATION SOURCE TO SOURCE_HOST='h', SOURCE_USER='u', SOURCE_PASSWORD='p', REPLICATE_DO_DB='app', REPLICATE_DO_TABLE='app.users', REPLICATE_WILD_DO_TABLE='app.audit_%', REPLICATE_IGNORE_TABLE='app.audit_secret'",
        )
        .unwrap();
        assert_eq!(cfg.do_databases, vec!["app".to_string()]);
        assert_eq!(cfg.do_tables, vec!["app.users".to_string()]);
        assert_eq!(cfg.wild_do_tables, vec!["app.audit_%".to_string()]);
        assert_eq!(cfg.ignore_tables, vec!["app.audit_secret".to_string()]);
    }

    #[test]
    fn parse_csv_or_paren_list_handles_both_forms() {
        assert_eq!(
            parse_csv_or_paren_list("a,b,c"),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
        assert_eq!(
            parse_csv_or_paren_list("(a, b, c)"),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
        assert!(parse_csv_or_paren_list("").is_empty());
        assert!(parse_csv_or_paren_list(",,").is_empty());
    }

    #[test]
    fn change_source_parser_accepts_ssl_clauses() {
        let cfg = parse_change_source(
            "CHANGE REPLICATION SOURCE TO SOURCE_HOST='h', SOURCE_USER='u', SOURCE_PASSWORD='p', SOURCE_SSL=1, SOURCE_SSL_MODE='VERIFY_CA', SOURCE_SSL_CA='/etc/ca.pem', SOURCE_SSL_CERT='/etc/cli.pem', SOURCE_SSL_KEY='/etc/cli.key'",
        )
        .unwrap();
        assert_eq!(cfg.ssl_mode, ReplicationSslMode::VerifyCa);
        assert_eq!(cfg.ssl_ca.as_deref(), Some("/etc/ca.pem"));
        assert_eq!(cfg.ssl_cert.as_deref(), Some("/etc/cli.pem"));
        assert_eq!(cfg.ssl_key.as_deref(), Some("/etc/cli.key"));
        let cfg2 = parse_change_source(
            "CHANGE REPLICATION SOURCE TO SOURCE_HOST='h', SOURCE_USER='u', SOURCE_PASSWORD='p', SOURCE_SSL=0",
        )
        .unwrap();
        assert_eq!(cfg2.ssl_mode, ReplicationSslMode::Disabled);
    }

    #[test]
    fn change_source_parser_rejects_invalid_ssl_mode() {
        let err = parse_change_source(
            "CHANGE REPLICATION SOURCE TO SOURCE_HOST='h', SOURCE_USER='u', SOURCE_PASSWORD='p', SOURCE_SSL_MODE='NOT_A_MODE'",
        )
        .unwrap_err();
        assert_eq!(err.code(), 1064);
        assert!(err.message().contains("SOURCE_SSL_MODE"));
    }

    #[test]
    fn ssl_opts_built_only_when_tls_required() {
        let mut cfg = ReplicationChannelConfig::default();
        cfg.ssl_mode = ReplicationSslMode::Disabled;
        assert!(build_ssl_opts(&cfg).unwrap().is_none());
        cfg.ssl_mode = ReplicationSslMode::Required;
        assert!(build_ssl_opts(&cfg).unwrap().is_some());
    }

    #[test]
    fn gtid_append_is_idempotent() {
        let gtid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1";
        assert_eq!(append_gtid("", gtid), gtid);
        assert_eq!(append_gtid(gtid, gtid), gtid);
        assert_eq!(
            append_gtid(gtid, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:2"),
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-2"
        );
    }

    #[test]
    fn parse_channel_only_rejects_trailing_thread_token() {
        // START/STOP REPLICA now accept IO_THREAD/SQL_THREAD via their own
        // parsers, but parse_channel_only (used by RESET REPLICA, which has no
        // per-thread form) still rejects a stray thread qualifier.
        let err = parse_channel_only("RESET REPLICA SQL_THREAD", &["RESET REPLICA"]).unwrap_err();
        assert_eq!(err.code(), 1235);
        assert!(err.message().contains("SQL_THREAD"));
    }

    #[test]
    fn binlog_file_name_trims_checksum_tail_bytes() {
        assert_eq!(
            clean_binlog_file_name(b"mysql-bin.000001\x86\xea\x6a\x54"),
            "mysql-bin.000001"
        );
        assert_eq!(
            clean_binlog_file_name(b"mysql-bin.0000020\x80\x81\x82"),
            "mysql-bin.000002"
        );
    }

    #[test]
    fn row_apply_insert_update_delete_recovery_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20))")
            .unwrap();
        let db = engine.database_for_replication();

        let txn = db.begin_txn().unwrap();
        let row1 = vec![Value::Int(1), Value::String("a".to_string())];
        apply_write_rows(db, txn, "test", "t", &[row1.clone()]).unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        let txn = db.begin_txn().unwrap();
        with_recovery_idempotent(|| apply_write_rows(db, txn, "test", "t", &[row1.clone()]))
            .unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        let txn = db.begin_txn().unwrap();
        let row2 = vec![Value::Int(1), Value::String("b".to_string())];
        apply_update_rows(
            db,
            txn,
            "test",
            "t",
            &[RowUpdate {
                before: row1.clone(),
                after: row2.clone(),
            }],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        let txn = db.begin_txn().unwrap();
        apply_delete_rows(db, txn, "test", "t", &[row2]).unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        let result = engine
            .execute("SELECT COUNT(*) FROM t")
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(result) = result else {
            panic!("expected result set");
        };
        assert_eq!(result.rows[0][0], Value::Int(0));
    }

    #[test]
    fn mysql_row_replay_widens_zone_map_for_insert_update_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute(
                "CREATE TABLE zm_repl (id INT PRIMARY KEY, v INT, pad VARCHAR(700)) \
                 ZONE_MAP=ON ZONE_SIZE=64K",
            )
            .unwrap();
        let db = engine.database_for_replication();

        let first = vec![Value::Int(1), Value::Int(10), Value::String("a".into())];
        let txn = db.begin_txn().unwrap();
        apply_write_rows(db, txn, "test", "zm_repl", &[first.clone()]).unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        let updated = vec![Value::Int(1), Value::Int(90_000), Value::String("b".into())];
        let txn = db.begin_txn().unwrap();
        apply_update_rows(
            db,
            txn,
            "test",
            "zm_repl",
            &[RowUpdate {
                before: first,
                after: updated.clone(),
            }],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        let zone = engine
            .execute("SELECT /*+ ZONE_MAP(zm_repl) */ id FROM zm_repl WHERE v=90000")
            .unwrap();
        let oracle = engine
            .execute("SELECT /*+ NO_ZONE_MAP(zm_repl) */ id FROM zm_repl WHERE v=90000")
            .unwrap();
        assert_eq!(format!("{zone:?}"), format!("{oracle:?}"));

        let txn = db.begin_txn().unwrap();
        apply_delete_rows(db, txn, "test", "zm_repl", &[updated]).unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        let result = engine
            .execute("SELECT /*+ ZONE_MAP(zm_repl) */ COUNT(*) FROM zm_repl")
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(result) = result else {
            panic!("expected result set");
        };
        assert_eq!(result.rows[0][0], Value::Int(0));
    }

    #[test]
    fn geometry_row_value_preserves_raw_srid_wkb_bytes() {
        use crate::storage::schema::GeometrySubtype;
        // Internal `SRID(4 LE) + WKB` for POINT(1 2), SRID 0. The IEEE-754
        // coordinate bytes (0xF0/0x40 …) are not valid UTF-8, so the old
        // `from_utf8_lossy` fallback would replace them with U+FFFD and
        // corrupt the geometry. The Geometry arm must pass the bytes through
        // verbatim as `Value::Geometry`.
        let mut bytes = vec![0u8, 0, 0, 0]; // SRID 0
        bytes.push(1); // little-endian byte order
        bytes.extend_from_slice(&1u32.to_le_bytes()); // wkbPoint
        bytes.extend_from_slice(&1.0f64.to_le_bytes()); // X = 1.0
        bytes.extend_from_slice(&2.0f64.to_le_bytes()); // Y = 2.0
        assert!(
            bytes.iter().any(|b| *b >= 0x80),
            "test bytes must be non-UTF-8"
        );

        let converted = mysql_value_to_mydb(
            &mysql::Value::Bytes(bytes.clone()),
            &DataTypeKind::Geometry {
                subtype: GeometrySubtype::Point,
                srid: None,
            },
            false,
            None,
        )
        .unwrap();
        assert_eq!(converted, Value::Geometry(bytes));
    }

    #[test]
    fn update_matches_by_primary_key_despite_divergent_before_image() {
        // A PRIMARY KEY before-image locates the target row even when a
        // non-key column diverges from the stored form (as JSON key
        // ordering / float formatting can). Before the PK-authoritative
        // match this raised a spurious 1032 "could not find row".
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(40))")
            .unwrap();
        let db = engine.database_for_replication();

        let txn = db.begin_txn().unwrap();
        apply_write_rows(
            db,
            txn,
            "test",
            "t",
            &[vec![Value::Int(1), Value::String("stored".to_string())]],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        let txn = db.begin_txn().unwrap();
        apply_update_rows(
            db,
            txn,
            "test",
            "t",
            &[RowUpdate {
                before: vec![Value::Int(1), Value::String("DIVERGENT".to_string())],
                after: vec![Value::Int(1), Value::String("new".to_string())],
            }],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        let result = engine
            .execute("SELECT v FROM t WHERE id = 1")
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(result) = result else {
            panic!("expected result set");
        };
        assert_eq!(result.rows[0][0], Value::String("new".to_string()));
    }

    // B6: with no PK but an all-NOT-NULL UNIQUE key, the unique key IS the row
    // identity, so a non-key column whose before-image diverges from the stored
    // form (as JSON re-serialisation / DOUBLE-DECIMAL formatting can) must NOT
    // defeat the match. Before this fix the non-PK unique path ran a full-row
    // `value_to_string` compare and raised a spurious 1032 (update) / silently
    // missed the delete. A divergent non-key column stands in for any
    // non-byte-round-tripping type.
    #[test]
    fn update_delete_match_by_not_null_unique_key_ignoring_nonkey_divergence() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        // No PRIMARY KEY; `uk` is UNIQUE NOT NULL → the identity key. `note` is
        // a non-key column, deliberately given a divergent before-image value.
        engine
            .execute("CREATE TABLE t (uk INT NOT NULL UNIQUE, note VARCHAR(20))")
            .unwrap();
        let db = engine.database_for_replication();

        let txn = db.begin_txn().unwrap();
        apply_write_rows(
            db,
            txn,
            "test",
            "t",
            &[vec![Value::Int(7), Value::String("orig".to_string())]],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        // Before-image's non-key column diverges from what is stored; the unique
        // key (uk=7) must still locate the row.
        let txn = db.begin_txn().unwrap();
        apply_update_rows(
            db,
            txn,
            "test",
            "t",
            &[RowUpdate {
                before: vec![Value::Int(7), Value::String("DIVERGENT".to_string())],
                after: vec![Value::Int(7), Value::String("updated".to_string())],
            }],
        )
        .expect("unique-key match must not be defeated by non-key divergence");
        db.commit_txn(txn.txn_id).unwrap();

        let result = engine
            .execute("SELECT note FROM t WHERE uk = 7")
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(rs) = result else {
            panic!("expected result set");
        };
        assert_eq!(rs.rows[0][0], Value::String("updated".to_string()));

        // DELETE by a divergent before-image must also match by the unique key.
        let txn = db.begin_txn().unwrap();
        apply_delete_rows(
            db,
            txn,
            "test",
            "t",
            &[vec![
                Value::Int(7),
                Value::String("DIVERGENT-AGAIN".to_string()),
            ]],
        )
        .expect("unique-key delete must not be defeated by non-key divergence");
        db.commit_txn(txn.txn_id).unwrap();

        let result = engine
            .execute("SELECT COUNT(*) FROM t")
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(rs) = result else {
            panic!("expected result set");
        };
        assert_eq!(rs.rows[0][0], Value::Int(0));
    }

    // R8: the IO thread must stop on a deterministic poison-pill (CRC32
    // checksum mismatch, or a site that explicitly flagged itself fatal such
    // as a connect-time precondition rejection) instead of reconnecting to the
    // same position forever. Transient errors (connection refused; a racy
    // mid-stream 1235 like a missing TABLE_MAP / a table the SQL thread has not
    // yet created from a preceding DDL) must keep retrying.
    #[test]
    fn io_error_fatality_classifies_poison_pills_but_not_transient() {
        // Checksum mismatch → fatal by its dedicated code, regardless of flag.
        assert!(io_error_is_fatal(
            &error(crate::error::catalog::ER_BINLOG_FATAL_ERROR, "checksum",),
            false
        ));
        // Connect-time precondition rejection: reported as 1235 but explicitly
        // marked fatal at the return site.
        assert!(io_error_is_fatal(
            &error(crate::error::catalog::ER_NOT_SUPPORTED_YET, "fatal"),
            true
        ));
        // Transient / racy: connection refused (2003) and an unmarked
        // mid-stream 1235 must NOT stop the thread.
        assert!(!io_error_is_fatal(
            &error(crate::error::catalog::CR_CONN_HOST_ERROR, "connect"),
            false
        ));
        assert!(!io_error_is_fatal(
            &error(crate::error::catalog::ER_NOT_SUPPORTED_YET, "retry"),
            false
        ));

        // The thread-local flag round-trips and is cleared by `take`.
        assert!(!take_io_error_fatal());
        mark_io_error_fatal();
        assert!(take_io_error_fatal());
        assert!(!take_io_error_fatal());
    }

    #[test]
    fn bit_row_value_preserves_bytes_not_lossy_string() {
        // BIT(8) = 53 arrives as the single big-endian byte 0x35. The old `_`
        // arm ran `from_utf8_lossy(b"\x35")` = "5" → coerced to 5 (silent data
        // corruption). The Bit arm must hand the raw bytes to coerce as
        // `Value::Bytes` so it reads 53 big-endian.
        let converted = mysql_value_to_mydb(
            &mysql::Value::Bytes(vec![0x35]),
            &DataTypeKind::Bit { len: Some(8) },
            false,
            None,
        )
        .unwrap();
        assert_eq!(converted, Value::Bytes(vec![0x35]));

        // High-bit value 0x81 (129): the old path produced U+FFFD and broke
        // apply; now it survives as raw bytes.
        let converted = mysql_value_to_mydb(
            &mysql::Value::Bytes(vec![0x81]),
            &DataTypeKind::Bit { len: Some(8) },
            false,
            None,
        )
        .unwrap();
        assert_eq!(converted, Value::Bytes(vec![0x81]));
    }

    #[test]
    fn vector_row_value_preserves_native_envelopes() {
        use crate::vector::VectorDataType;
        for (datatype, text) in [
            (VectorDataType::Float32, "[1,-2.5]"),
            (VectorDataType::Float16, "[1,-2.5]"),
            (VectorDataType::BFloat16, "[1,-2.5]"),
            (VectorDataType::Int8, "[-128,127]"),
            (VectorDataType::Binary, "[1,0,1,1,0,0,0,0,1]"),
        ] {
            let vector = crate::engine::vector::codec::parse_typed_text(text, datatype).unwrap();
            let mut envelope = crate::engine::vector::codec::VECTOR_BINLOG_MAGIC.to_vec();
            envelope.extend(vector.encode());
            let converted = mysql_value_to_mydb(
                &mysql::Value::Bytes(envelope),
                &DataTypeKind::Vector {
                    dim: Some(vector.dimension),
                    datatype,
                },
                false,
                None,
            )
            .unwrap();
            assert_eq!(converted, Value::Vector(vector));
        }
    }

    #[test]
    fn replicated_vector_delete_tombstone_follows_apply_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute("CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR(2) NOT NULL)")
            .unwrap();
        engine
            .execute("CREATE INDEX vx ON v(emb) USING HNSW")
            .unwrap();
        engine
            .execute("INSERT INTO v VALUES (1, '[1,0]'), (2, '[2,0]')")
            .unwrap();
        let db = engine.database_for_replication();
        db.vector_graph_warm_all_for_test();
        let schema = db.table("test.v").unwrap();
        let index = schema.indexes.iter().find(|i| i.name == "vx").unwrap();
        let direct_ids = || {
            db.vector_index_search_for_partition(
                &schema,
                index,
                schema.id,
                &crate::engine::vector::codec::from_f32(&[1.0, 0.0]),
                crate::engine::vector::VectorSearchRequest {
                    candidate_limit: 8,
                    rerank_limit: 8,
                    effort: crate::engine::vector::VectorSearchEffort::Hnsw { ef: 32 },
                },
            )
            .unwrap()
            .candidates
            .into_iter()
            .map(|(rid, _)| rid)
            .collect::<Vec<_>>()
        };
        let before = direct_ids();
        assert_eq!(before.len(), 2);

        let txn = db.begin_txn().unwrap();
        apply_delete_rows(
            db,
            txn,
            "test",
            "v",
            &[vec![
                Value::Int(1),
                Value::Vector(crate::engine::vector::codec::from_f32(&[1.0, 0.0])),
            ]],
        )
        .unwrap();
        db.rollback_txn(txn.txn_id).unwrap();
        assert_eq!(
            direct_ids().len(),
            2,
            "rolled-back apply must keep graph node live"
        );

        let txn = db.begin_txn().unwrap();
        apply_delete_rows(
            db,
            txn,
            "test",
            "v",
            &[vec![
                Value::Int(1),
                Value::Vector(crate::engine::vector::codec::from_f32(&[1.0, 0.0])),
            ]],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        assert_eq!(
            direct_ids().len(),
            1,
            "committed apply must tombstone graph node"
        );
    }

    #[test]
    fn delete_missing_row_errors_in_strict_but_skips_in_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20))")
            .unwrap();
        let db = engine.database_for_replication();

        // STRICT (default): DELETE whose before-image is absent stops the
        // applier with 1032, matching MySQL and apply_update_rows.
        set_slave_exec_mode(false);
        let txn = db.begin_txn().unwrap();
        let err = apply_delete_rows(
            db,
            txn,
            "test",
            "t",
            &[vec![Value::Int(99), Value::String("gone".to_string())]],
        )
        .unwrap_err();
        assert_eq!(err.code(), 1032);
        let _ = db.rollback_txn(txn.txn_id);

        // IDEMPOTENT: the same absent DELETE is a silent skip.
        set_slave_exec_mode(true);
        let txn = db.begin_txn().unwrap();
        apply_delete_rows(
            db,
            txn,
            "test",
            "t",
            &[vec![Value::Int(99), Value::String("gone".to_string())]],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
        set_slave_exec_mode(false);
    }

    #[test]
    fn write_mixed_batch_inserts_new_and_skips_existing_duplicate() {
        // A WRITE_ROWS batch containing one already-present row (id=1) and one
        // genuinely new row (id=2): the new row must be inserted and the
        // duplicate skipped, instead of the whole batch erroring and dropping
        // the new row.
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20))")
            .unwrap();
        let db = engine.database_for_replication();

        let txn = db.begin_txn().unwrap();
        with_recovery_idempotent(|| {
            apply_write_rows(
                db,
                txn,
                "test",
                "t",
                &[vec![Value::Int(1), Value::String("orig".to_string())]],
            )
        })
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        let txn = db.begin_txn().unwrap();
        with_recovery_idempotent(|| {
            apply_write_rows(
                db,
                txn,
                "test",
                "t",
                &[
                    vec![Value::Int(1), Value::String("orig".to_string())],
                    vec![Value::Int(2), Value::String("new".to_string())],
                ],
            )
        })
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        let result = engine
            .execute("SELECT id, v FROM t ORDER BY id")
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(result) = result else {
            panic!("expected result set");
        };
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[1][1], Value::String("new".to_string()));
    }

    #[test]
    fn keyless_table_hash_scan_matches_update_and_delete() {
        // PF3: a table with no PRIMARY/UNIQUE key still locates before-images
        // correctly via the HASH_SCAN path, including duplicate rows.
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute("CREATE TABLE t (a INT, b VARCHAR(20))")
            .unwrap();
        let db = engine.database_for_replication();

        let txn = db.begin_txn().unwrap();
        apply_write_rows(
            db,
            txn,
            "test",
            "t",
            &[
                vec![Value::Int(1), Value::String("x".to_string())],
                vec![Value::Int(1), Value::String("x".to_string())], // duplicate row
                vec![Value::Int(2), Value::String("y".to_string())],
            ],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        // UPDATE one of the two duplicate (1,'x') rows to (1,'z').
        let txn = db.begin_txn().unwrap();
        apply_update_rows(
            db,
            txn,
            "test",
            "t",
            &[RowUpdate {
                before: vec![Value::Int(1), Value::String("x".to_string())],
                after: vec![Value::Int(1), Value::String("z".to_string())],
            }],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        // DELETE the (2,'y') row.
        let txn = db.begin_txn().unwrap();
        apply_delete_rows(
            db,
            txn,
            "test",
            "t",
            &[vec![Value::Int(2), Value::String("y".to_string())]],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();

        let QueryResponse::ResultSet(rs) = engine
            .execute("SELECT a, b FROM t ORDER BY a, b")
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!("expected result set");
        };
        // Remaining: (1,'x') [the untouched duplicate], (1,'z') [updated].
        assert_eq!(rs.rows.len(), 2);
        assert_eq!(rs.rows[0][1], Value::String("x".to_string()));
        assert_eq!(rs.rows[1][1], Value::String("z".to_string()));
    }

    #[test]
    fn incremental_change_source_preserves_unspecified_fields() {
        // C1: a second CHANGE that only sets SOURCE_DELAY must keep host /
        // user / password / auto_position from the first, not reset them to
        // defaults.
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let manager = ReplicationManager {
            data_dir: dir.path().to_path_buf(),
            engine,
            channels: Mutex::new(HashMap::new()),
        };

        let sql1 = "CHANGE REPLICATION SOURCE TO SOURCE_HOST='10.0.0.5', SOURCE_PORT=3310, \
             SOURCE_USER='repl', SOURCE_PASSWORD='secret', SOURCE_AUTO_POSITION=1";
        let base = manager.base_config_for_change(&parse_change_source_channel(sql1));
        let full = parse_change_source_onto(sql1, base).unwrap();
        manager.change_source(full).unwrap();

        let channel = parse_change_source_channel("CHANGE REPLICATION SOURCE TO SOURCE_DELAY=60");
        let base = manager.base_config_for_change(&channel);
        let merged =
            parse_change_source_onto("CHANGE REPLICATION SOURCE TO SOURCE_DELAY=60", base).unwrap();

        assert_eq!(merged.source_host, "10.0.0.5");
        assert_eq!(merged.source_port, 3310);
        assert_eq!(merged.source_user, "repl");
        assert_eq!(merged.source_password, "secret");
        assert!(merged.auto_position);
        assert_eq!(merged.source_delay_secs, 60);
    }

    #[test]
    fn audit1022_channel_metadata_cannot_alias_native_replication_markers() {
        std::env::set_var("MYDB_SKIP_REPLICA_START", "1");
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let manager = ReplicationManager {
            data_dir: dir.path().to_path_buf(),
            engine,
            channels: Mutex::new(HashMap::new()),
        };
        crate::repl::replica::progress::write_resync_marker(dir.path(), "native").unwrap();
        for name in ["role", "replica_progress", "resync_required"] {
            let mut config = parse_change_source(
                "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1',SOURCE_USER='repl'",
            )
            .unwrap();
            config.channel = name.into();
            manager.change_source(config).unwrap();
        }
        manager.channels.lock().unwrap().clear();
        manager.restore_persisted_channels();
        assert_eq!(manager.channels.lock().unwrap().len(), 3);
        assert_eq!(
            crate::repl::replica::progress::resync_marker_reason(dir.path()).as_deref(),
            Some("native")
        );
    }

    #[test]
    fn persisted_channel_is_restored_and_reported_after_restart() {
        // C2: a channel configured via SQL must survive a restart — loaded
        // into memory (reported by SHOW REPLICA STATUS) with running=No, not
        // silently dropped. Skip auto-start so the test doesn't dial a source.
        std::env::set_var("MYDB_SKIP_REPLICA_START", "1");
        let dir = tempfile::tempdir().unwrap();

        {
            let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
            let manager = Arc::new(ReplicationManager {
                data_dir: dir.path().to_path_buf(),
                engine,
                channels: Mutex::new(HashMap::new()),
            });
            let cfg = parse_change_source(
                "CHANGE REPLICATION SOURCE TO SOURCE_HOST='10.1.2.3', SOURCE_PORT=3307, SOURCE_USER='repl'",
            )
            .unwrap();
            manager.change_source(cfg).unwrap();
        }

        // Simulate a restart: brand-new manager over the same data dir.
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let manager = Arc::new(ReplicationManager {
            data_dir: dir.path().to_path_buf(),
            engine,
            channels: Mutex::new(HashMap::new()),
        });
        manager.restore_persisted_channels();

        let QueryResponse::ResultSet(rs) = manager.show_status(None).unwrap() else {
            panic!("expected result set");
        };
        assert_eq!(rs.rows.len(), 1, "restored channel must be reported");
        // Column 2 is Source_Host; columns 11/12 are IO/SQL running.
        let host_col = rs
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case("Source_Host"))
            .unwrap();
        assert_eq!(rs.rows[0][host_col], Value::String("10.1.2.3".to_string()));
        let io_run = rs
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case("Replica_IO_Running"))
            .unwrap();
        assert_eq!(rs.rows[0][io_run], Value::String("No".to_string()));
        std::env::remove_var("MYDB_SKIP_REPLICA_START");
    }

    #[test]
    fn audit_raw_rows_missing_table_is_sql_error_and_reclaim_is_restartable() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let (runtime, runner) = make_split_runner(dir.path(), engine.clone());
        let mut map = vec![1, 0, 0, 0, 0, 0, 0, 0, 4];
        map.extend_from_slice(b"test\0");
        map.extend_from_slice(&[1, b't', 0, 1, 3, 0, 0]);
        let rows = vec![1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 1, 0, 42, 0, 0, 0];
        let mut reader = EventStreamReader::new(BinlogVersion::Version4);
        let mut file = "bin.000001".to_string();
        let mut maps = HashMap::new();
        let mut txn = TxnBuffer::default();
        for (kind, body, pos) in [
            (EventType::TABLE_MAP_EVENT, map, 80),
            (EventType::WRITE_ROWS_EVENT, rows, 120),
        ] {
            let bytes = test_event_bytes(kind, pos, body);
            let event = reader.read(&bytes[..]).unwrap().unwrap();
            runner
                .handle_source_event(&event, &mut file, pos as u64, &mut maps, &mut txn)
                .unwrap();
        }
        runner.io_commit_txn(&file, 180, &mut txn).unwrap();
        let durable = runtime.relay_gate.lock().unwrap().durable_offset;
        assert_eq!(
            runner.sql_apply_available(durable).unwrap_err().code(),
            1146
        );
        assert_eq!(runtime.status.lock().unwrap().relay_log_pos, 0);
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY)")
            .unwrap();
        runner.sql_apply_available(durable).unwrap();
        runner.reclaim_applied_relay(1).unwrap();
        assert_eq!(
            fs::metadata(relay_log_path(dir.path(), DEFAULT_CHANNEL))
                .unwrap()
                .len(),
            0
        );
        assert_eq!(runtime.status.lock().unwrap().relay_log_pos, 0);
        let persisted: PersistedChannel =
            serde_json::from_slice(&fs::read(channel_path(dir.path(), DEFAULT_CHANNEL)).unwrap())
                .unwrap();
        assert_eq!(persisted.status.exec_source_log_pos, 180);
        // Simulate a crash after the reclaim intent but before truncation.
        fs::write(
            relay_log_path(dir.path(), DEFAULT_CHANNEL),
            b"old consumed bytes",
        )
        .unwrap();
        fs::write(
            relay_reclaim_path(dir.path(), DEFAULT_CHANNEL).with_extension("retained"),
            b"",
        )
        .unwrap();
        fs::write(
            relay_reclaim_path(dir.path(), DEFAULT_CHANNEL),
            serde_json::to_vec(&persisted).unwrap(),
        )
        .unwrap();
        recover_relay_reclaim(dir.path(), DEFAULT_CHANNEL).unwrap();
        recover_relay_reclaim(dir.path(), DEFAULT_CHANNEL).unwrap();
        assert_eq!(
            fs::metadata(relay_log_path(dir.path(), DEFAULT_CHANNEL))
                .unwrap()
                .len(),
            0
        );
        let QueryResponse::ResultSet(result) = engine
            .execute("SELECT id FROM t")
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(result.rows, vec![vec![Value::Int(42)]]);
    }

    #[test]
    fn audit_relay_reclaim_preserves_unapplied_tail() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let (runtime, runner) = make_split_runner(dir.path(), engine);
        let mut txn = TxnBuffer::default();
        runner.io_commit_txn("bin.000001", 100, &mut txn).unwrap();
        runner.io_commit_txn("bin.000001", 200, &mut txn).unwrap();
        let consumed = runtime.relay_gate.lock().unwrap().durable_offset;
        runner.sql_apply_available(consumed).unwrap();
        runner.io_commit_txn("bin.000001", 300, &mut txn).unwrap();
        let original = runtime.relay_gate.lock().unwrap().durable_offset;
        runner.reclaim_applied_relay(1).unwrap();
        let retained = runtime.relay_gate.lock().unwrap().durable_offset;
        assert_eq!(retained, original - consumed);
        assert!(retained > 0);
        assert_eq!(runtime.status.lock().unwrap().relay_log_pos, 0);
        runner.sql_apply_available(retained).unwrap();
        assert_eq!(runtime.status.lock().unwrap().exec_source_log_pos, 300);
    }

    #[test]
    fn audit_full_metadata_decodes_source_latin1_into_utf8_destination() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute("CREATE TABLE t(v VARCHAR(20)) DEFAULT CHARSET=utf8mb4")
            .unwrap();
        let (runtime, runner) = make_split_runner(dir.path(), engine.clone());
        let mut map = vec![1, 0, 0, 0, 0, 0, 0, 0, 4];
        map.extend_from_slice(b"test\0");
        // VARCHAR(20), COLUMN_CHARSET metadata: latin1_swedish_ci (8).
        map.extend_from_slice(&[1, b't', 0, 1, 15, 2, 20, 0, 0, 3, 1, 8]);
        let mut rows = vec![1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 1, 0, 4];
        rows.extend_from_slice(b"caf\xe9");
        let mut reader = EventStreamReader::new(BinlogVersion::Version4);
        let mut file = "bin.000001".to_string();
        let mut maps = HashMap::new();
        let mut txn = TxnBuffer::default();
        for (kind, body, pos) in [
            (EventType::TABLE_MAP_EVENT, map, 80),
            (EventType::WRITE_ROWS_EVENT, rows, 120),
        ] {
            let bytes = test_event_bytes(kind, pos, body);
            let event = reader.read(&bytes[..]).unwrap().unwrap();
            runner
                .handle_source_event(&event, &mut file, pos as u64, &mut maps, &mut txn)
                .unwrap();
        }
        runner.io_commit_txn(&file, 180, &mut txn).unwrap();
        let durable = runtime.relay_gate.lock().unwrap().durable_offset;
        runner.sql_apply_available(durable).unwrap();
        let QueryResponse::ResultSet(result) = engine
            .execute("SELECT v FROM t")
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(result.rows, vec![vec![Value::String("café".into())]]);
    }

    #[test]
    fn audit_strings_never_replace_invalid_utf8() {
        assert!(decode_replicated_string(b"caf\xe9", Some("utf8mb4")).is_err());
        assert_eq!(
            decode_replicated_string(b"caf\xe9", Some("latin1")).unwrap(),
            Value::String("café".into())
        );
        assert_eq!(
            decode_replicated_string("中文".as_bytes(), Some("utf8mb4")).unwrap(),
            Value::String("中文".into())
        );
    }

    #[test]
    fn audit_change_and_reset_clear_cached_io_position() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let manager = ReplicationManager {
            data_dir: dir.path().to_path_buf(),
            engine,
            channels: Mutex::new(HashMap::new()),
        };
        let cfg = parse_change_source("CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_USER='repl', SOURCE_LOG_FILE='bin.000002', SOURCE_LOG_POS=456").unwrap();
        manager.change_source(cfg.clone()).unwrap();
        let runtime = manager.ensure_channel(DEFAULT_CHANNEL).unwrap();
        *runtime.io_committed_pos.lock().unwrap() = Some(("old.000001".into(), 123));
        manager.change_source(cfg).unwrap();
        assert!(runtime.io_committed_pos.lock().unwrap().is_none());
        *runtime.io_committed_pos.lock().unwrap() = Some(("old.000001".into(), 123));
        manager.reset_channel(DEFAULT_CHANNEL).unwrap();
        assert!(runtime.io_committed_pos.lock().unwrap().is_none());
    }

    #[test]
    fn reset_replica_all_forgets_channel() {
        // C4: RESET REPLICA ALL removes the stored config and the channel is
        // no longer reported by SHOW REPLICA STATUS.
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let manager = Arc::new(ReplicationManager {
            data_dir: dir.path().to_path_buf(),
            engine,
            channels: Mutex::new(HashMap::new()),
        });
        let cfg = parse_change_source(
            "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=3307, SOURCE_USER='repl'",
        )
        .unwrap();
        manager.change_source(cfg).unwrap();
        assert!(channel_path(dir.path(), DEFAULT_CHANNEL).exists());

        manager.reset_channel_all(DEFAULT_CHANNEL).unwrap();
        assert!(!channel_path(dir.path(), DEFAULT_CHANNEL).exists());
        let QueryResponse::ResultSet(rs) = manager.show_status(None).unwrap() else {
            panic!("expected result set");
        };
        assert!(rs.rows.is_empty(), "channel should no longer be reported");
    }

    #[test]
    fn round2_relay_boundary_excludes_transactional_ddl_and_recovers_source_position() {
        let dir = tempfile::tempdir().unwrap();
        append_test_relay(
            dir.path(),
            &RelayRecord {
                source_file: "bin.000003".into(),
                source_pos: 800,
                gtid: None,
                op: RelayOp::TxnCommit {
                    event_count: 1,
                    commit_ts: 0,
                },
            },
        );
        let path = relay_log_path(dir.path(), DEFAULT_CHANNEL);
        let expected = fs::metadata(&path).unwrap().len();
        append_test_relay(
            dir.path(),
            &RelayRecord {
                source_file: "bin.000003".into(),
                source_pos: 900,
                gtid: None,
                op: RelayOp::Ddl {
                    source_ts: 0,
                    database: "test".into(),
                    sql: "CREATE TABLE t(id INT)".into(),
                    transactional: true,
                    session: Default::default(),
                },
            },
        );
        let (offset, boundary) = recover_relay_boundary(&path).unwrap();
        assert_eq!(offset, expected);
        let boundary = boundary.unwrap();
        assert_eq!(
            (boundary.source_file.as_str(), boundary.source_pos),
            ("bin.000003", 800)
        );
    }

    #[test]
    fn truncate_relay_drops_uncommitted_and_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let gtid = Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1".to_string());
        // Committed transaction: one row + its commit marker.
        append_test_relay(
            dir.path(),
            &RelayRecord {
                source_file: "bin.000001".to_string(),
                source_pos: 120,
                gtid: gtid.clone(),
                op: RelayOp::WriteRows {
                    database: "test".to_string(),
                    table: "t".to_string(),
                    rows: vec![vec![Value::Int(1)]],
                },
            },
        );
        append_test_relay(
            dir.path(),
            &RelayRecord {
                source_file: "bin.000001".to_string(),
                source_pos: 180,
                gtid: gtid.clone(),
                op: RelayOp::TxnCommit {
                    event_count: 1,
                    commit_ts: 0,
                },
            },
        );
        let path = relay_log_path(dir.path(), DEFAULT_CHANNEL);
        let commit_offset = std::fs::metadata(&path).unwrap().len();

        // Uncommitted intra-transaction row (no commit marker) that a crash
        // left page-cache flushed…
        append_test_relay(
            dir.path(),
            &RelayRecord {
                source_file: "bin.000001".to_string(),
                source_pos: 240,
                gtid: None,
                op: RelayOp::WriteRows {
                    database: "test".to_string(),
                    table: "t".to_string(),
                    rows: vec![vec![Value::Int(2)]],
                },
            },
        );
        // …plus a torn final line (no trailing newline).
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(b"{\"source_file\":\"bin.000001\",\"partial")
                .unwrap();
        }
        assert!(std::fs::metadata(&path).unwrap().len() > commit_offset);

        let truncated = truncate_relay_to_last_commit(&path).unwrap();
        assert_eq!(truncated, commit_offset);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), commit_offset);
    }

    #[test]
    fn relay_replay_applies_committed_tail_once() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20))")
            .unwrap();
        let mut cfg = ReplicationChannelConfig::default();
        cfg.source_host = "127.0.0.1".to_string();
        cfg.source_log_file = "bin.000001".to_string();
        let runtime = Arc::new(ChannelRuntime {
            config: Mutex::new(cfg.clone()),
            status: Arc::new(Mutex::new(ReplicationStatus::from_config(&cfg, dir.path()))),
            stop: Arc::new(AtomicBool::new(false)),
            io_stop: Arc::new(AtomicBool::new(false)),
            sql_stop: Arc::new(AtomicBool::new(false)),
            io_join: Mutex::new(None),
            sql_join: Mutex::new(None),
            until: Mutex::new(None),
            flush_policy: ReplicationFlushPolicy {
                relay_sync_txns: 1,
                ..ReplicationFlushPolicy::default()
            },
            relay_writer: Mutex::new(None),
            relay_sync: Mutex::new(RelaySyncState::default()),
            relay_gate: Mutex::new(RelayGate::default()),
            relay_cond: Condvar::new(),
            durable_io_progress: Mutex::new(DurableIoProgress::from_status(
                &ReplicationStatus::from_config(&cfg, dir.path()),
            )),
            progress_persist: Mutex::new(ProgressPersistState::default()),
            io_committed_pos: Mutex::new(None),
        });
        let runner = ChannelRunner::new(
            dir.path().to_path_buf(),
            Arc::clone(&engine),
            Arc::clone(&runtime),
        );
        let gtid = Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1".to_string());
        append_test_relay(
            dir.path(),
            &RelayRecord {
                source_file: "bin.000001".to_string(),
                source_pos: 120,
                gtid: gtid.clone(),
                op: RelayOp::WriteRows {
                    database: "test".to_string(),
                    table: "t".to_string(),
                    rows: vec![vec![Value::Int(1), Value::String("a".to_string())]],
                },
            },
        );
        append_test_relay(
            dir.path(),
            &RelayRecord {
                source_file: "bin.000001".to_string(),
                source_pos: 180,
                gtid: gtid.clone(),
                op: RelayOp::TxnCommit {
                    event_count: 1,
                    commit_ts: 0,
                },
            },
        );

        runtime.relay_gate.lock().unwrap().durable_offset =
            truncate_relay_to_last_commit(&relay_log_path(dir.path(), DEFAULT_CHANNEL)).unwrap();
        runner.replay_relay_tail().unwrap();
        let status = runtime.status.lock().unwrap().clone();
        // SQL-side recovery advances the *exec* position (the IO thread owns
        // the read position `source_log_*`).
        assert_eq!(status.exec_source_log_file, "bin.000001");
        assert_eq!(status.exec_source_log_pos, 180);
        assert!(status.relay_log_pos > 0);
        assert!(gtid_set_contains(
            &status.executed_gtid_set,
            gtid.as_deref().unwrap()
        ));

        runtime.relay_gate.lock().unwrap().durable_offset =
            truncate_relay_to_last_commit(&relay_log_path(dir.path(), DEFAULT_CHANNEL)).unwrap();
        runner.replay_relay_tail().unwrap();
        let result = engine
            .execute("SELECT COUNT(*) FROM t WHERE id = 1 AND v = 'a'")
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(result) = result else {
            panic!("expected result set");
        };
        assert_eq!(result.rows[0][0], Value::Int(1));
    }

    #[test]
    fn audit_reconnect_discards_partial_delete_and_preserves_commits() {
        for grouped in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
            engine
                .execute("CREATE TABLE t (id INT PRIMARY KEY)")
                .unwrap();
            engine.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
            let (runtime, runner) = make_split_runner_with_policy(
                dir.path(),
                engine.clone(),
                ReplicationFlushPolicy {
                    relay_sync_txns: if grouped { u64::MAX } else { 1 },
                    relay_sync_interval: Duration::from_secs(3600),
                    ..ReplicationFlushPolicy::default()
                },
            );
            let delete = |ids: &[i64]| RelayOp::DeleteRows {
                database: "test".into(),
                table: "t".into(),
                rows: ids.iter().map(|id| vec![Value::Int(*id)]).collect(),
            };
            runner
                .append_relay("bin.000001", 80, None, &delete(&[3]))
                .unwrap();
            runner
                .io_commit_txn("bin.000001", 100, &mut TxnBuffer::default())
                .unwrap();
            runner
                .append_relay("bin.000001", 120, None, &delete(&[1]))
                .unwrap();
            // Both EOF/error and explicit STOP use this cleanup path.
            runner.finish_io_attempt().unwrap();
            let boundary = runtime.relay_gate.lock().unwrap().durable_offset;
            assert_eq!(
                fs::metadata(relay_log_path(dir.path(), DEFAULT_CHANNEL))
                    .unwrap()
                    .len(),
                boundary
            );
            assert_eq!(
                *runtime.io_committed_pos.lock().unwrap(),
                Some(("bin.000001".into(), 100))
            );
            runner
                .append_relay("bin.000001", 140, None, &delete(&[1, 2]))
                .unwrap();
            runner
                .io_commit_txn("bin.000001", 160, &mut TxnBuffer::default())
                .unwrap();
            runner.finish_io_attempt().unwrap();
            let durable = runtime.relay_gate.lock().unwrap().durable_offset;
            runner.sql_apply_available(durable).unwrap();
            let QueryResponse::ResultSet(result) = engine
                .execute("SELECT COUNT(*) FROM t")
                .unwrap()
                .materialize()
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(result.rows[0][0], Value::Int(0));
            assert_eq!(runtime.status.lock().unwrap().relay_log_pos, durable);
        }
    }

    #[test]
    fn audit_reclaim_retains_pending_boundary_for_reconnect() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let (runtime, runner) = make_split_runner_with_policy(
            dir.path(),
            engine,
            ReplicationFlushPolicy {
                relay_sync_txns: u64::MAX,
                relay_sync_interval: Duration::from_secs(3600),
                ..ReplicationFlushPolicy::default()
            },
        );
        // Large applied prefix followed by a complete unsynced transaction and
        // an interrupted prefix. Reclaim replaces the inode before reconnect.
        for i in 0..10 {
            runner
                .io_commit_txn("bin.000001", 100 + i, &mut TxnBuffer::default())
                .unwrap();
        }
        runner.sync_pending_relay(true).unwrap();
        let durable = runtime.relay_gate.lock().unwrap().durable_offset;
        runner.sql_apply_available(durable).unwrap();
        runner
            .io_commit_txn("bin.000001", 200, &mut TxnBuffer::default())
            .unwrap();
        runner
            .append_relay("bin.000001", 210, None, &RelayOp::Noop)
            .unwrap();
        runner.reclaim_applied_relay(1).unwrap();
        runner.finish_io_attempt().unwrap();
        let bytes = fs::read(relay_log_path(dir.path(), DEFAULT_CHANNEL)).unwrap();
        assert_eq!(bytes.iter().filter(|b| **b == b'\n').count(), 1);
        assert_eq!(runtime.durable_io_progress.lock().unwrap().source_pos, 200);
        assert_eq!(
            runtime.relay_gate.lock().unwrap().durable_offset,
            bytes.len() as u64
        );
    }

    #[test]
    fn audit_stop_waits_for_sql_worker_past_fifteen_seconds() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let manager = ReplicationManager {
            data_dir: dir.path().to_path_buf(),
            engine,
            channels: Mutex::new(HashMap::new()),
        };
        manager
            .change_source(
                parse_change_source(
                    "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_USER='repl'",
                )
                .unwrap(),
            )
            .unwrap();
        let runtime = manager.ensure_channel(DEFAULT_CHANNEL).unwrap();
        let finished = Arc::new(AtomicBool::new(false));
        let flag = finished.clone();
        let (release, blocked) = std::sync::mpsc::channel();
        *runtime.sql_join.lock().unwrap() = Some(thread::spawn(move || {
            blocked.recv().unwrap();
            flag.store(true, Ordering::Release);
        }));
        let (entered, waiting) = std::sync::mpsc::channel();
        let stopping = thread::spawn(move || {
            past_old_join_deadline(entered, || {
                manager.stop_channel_threads(DEFAULT_CHANNEL, ReplThreadSel::Sql)
            })
        });
        let timeout = waiting.recv_timeout(Duration::from_secs(5));
        let premature = stopping.is_finished();
        release.send(()).unwrap();
        stopping.join().unwrap().unwrap();
        assert_eq!(
            timeout.unwrap(),
            None,
            "SQL stop must join without a deadline"
        );
        assert!(!premature, "STOP returned before the SQL worker finished");
        assert!(finished.load(Ordering::Acquire));
        assert!(runtime.sql_join.lock().unwrap().is_none());
    }

    #[test]
    fn audit_sync_never_publishes_uncommitted_tail() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let (runtime, runner) = make_split_runner_with_policy(
            dir.path(),
            engine,
            ReplicationFlushPolicy {
                relay_sync_txns: u64::MAX,
                relay_sync_interval: Duration::from_secs(3600),
                ..ReplicationFlushPolicy::default()
            },
        );
        runner
            .io_commit_txn("bin.000001", 100, &mut TxnBuffer::default())
            .unwrap();
        runner
            .append_relay("bin.000001", 120, None, &RelayOp::Noop)
            .unwrap();
        runner.sync_pending_relay(true).unwrap();
        let durable = runtime.relay_gate.lock().unwrap().durable_offset;
        let bytes = fs::read(relay_log_path(dir.path(), DEFAULT_CHANNEL)).unwrap();
        let first_end = bytes.iter().position(|b| *b == b'\n').unwrap() + 1;
        assert_eq!(
            durable, first_end as u64,
            "only complete transactions may be published"
        );
    }

    #[test]
    fn audit_strict_write_conflicts_surface_divergence() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT UNIQUE)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES (1,99)").unwrap();
        let db = engine.database_for_replication();
        for row in [
            vec![Value::Int(1), Value::Int(55)],
            vec![Value::Int(1), Value::Int(99)],
            vec![Value::Int(2), Value::Int(99)],
        ] {
            let txn = db.begin_txn().unwrap();
            let result = apply_write_rows(db, txn, "test", "t", &[row]);
            db.rollback_txn(txn.txn_id).unwrap();
            assert!(result.unwrap_err().is(crate::error::catalog::ER_DUP_ENTRY));
        }
    }

    #[test]
    fn audit_quoted_channel_preserves_unicode() {
        assert_eq!(
            extract_quoted_string("'渠道🙂' LIMIT 2").unwrap(),
            ("渠道🙂".into(), " LIMIT 2")
        );
        assert_eq!(
            extract_quoted_string("'渠''道' rest").unwrap(),
            ("渠'道".into(), " rest")
        );
    }

    #[test]
    fn audit_poisoned_relay_writer_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let (runtime, runner) = make_split_runner(dir.path(), engine);
        runtime.status.lock().unwrap().relay_log_pos = 1;
        let r = runtime.clone();
        assert!(thread::spawn(move || {
            let _guard = r.relay_writer.lock().unwrap();
            panic!("inject poison");
        })
        .join()
        .is_err());
        assert!(runner.reclaim_applied_relay(1).is_err());
        assert!(runner
            .append_relay("bin.000001", 100, None, &RelayOp::Noop)
            .is_err());
    }

    fn make_split_runner(
        dir: &Path,
        engine: Arc<StorageQueryEngine>,
    ) -> (Arc<ChannelRuntime>, ChannelRunner) {
        make_split_runner_with_policy(
            dir,
            engine,
            ReplicationFlushPolicy {
                relay_sync_txns: 1,
                ..ReplicationFlushPolicy::default()
            },
        )
    }

    fn make_split_runner_with_policy(
        dir: &Path,
        engine: Arc<StorageQueryEngine>,
        flush_policy: ReplicationFlushPolicy,
    ) -> (Arc<ChannelRuntime>, ChannelRunner) {
        let mut cfg = ReplicationChannelConfig::default();
        cfg.source_host = "127.0.0.1".to_string();
        cfg.source_log_file = "bin.000001".to_string();
        let runtime = Arc::new(ChannelRuntime {
            config: Mutex::new(cfg.clone()),
            status: Arc::new(Mutex::new(ReplicationStatus::from_config(&cfg, dir))),
            stop: Arc::new(AtomicBool::new(false)),
            io_stop: Arc::new(AtomicBool::new(false)),
            sql_stop: Arc::new(AtomicBool::new(false)),
            io_join: Mutex::new(None),
            sql_join: Mutex::new(None),
            until: Mutex::new(None),
            flush_policy,
            relay_writer: Mutex::new(None),
            relay_sync: Mutex::new(RelaySyncState::default()),
            relay_gate: Mutex::new(RelayGate::default()),
            relay_cond: Condvar::new(),
            durable_io_progress: Mutex::new(DurableIoProgress::from_status(
                &ReplicationStatus::from_config(&cfg, dir),
            )),
            progress_persist: Mutex::new(ProgressPersistState::default()),
            io_committed_pos: Mutex::new(None),
        });
        let runner = ChannelRunner::new(dir.to_path_buf(), engine, Arc::clone(&runtime));
        (runtime, runner)
    }

    #[test]
    fn io_relay_write_hands_off_to_sql_apply_via_durable_gate() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20))")
            .unwrap();
        let (runtime, runner) = make_split_runner(dir.path(), Arc::clone(&engine));

        let gtid = Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1".to_string());
        // IO thread: buffer a row into the relay, then write the commit marker.
        let op = RelayOp::WriteRows {
            database: "test".to_string(),
            table: "t".to_string(),
            rows: vec![vec![Value::Int(7), Value::String("io".to_string())]],
        };
        runner
            .append_relay("bin.000001", 120, gtid.clone(), &op)
            .unwrap();
        let mut txn = TxnBuffer {
            gtid: gtid.clone(),
            ops: vec![],
            event_count: 1,
            ..TxnBuffer::default()
        };
        runner.io_commit_txn("bin.000001", 180, &mut txn).unwrap();

        // The durable gate advanced (IO side), but nothing is applied yet and
        // the IO read position moved to 180 while the SQL applied offset is 0.
        let durable = runtime.relay_gate.lock().unwrap().durable_offset;
        assert!(durable > 0);
        {
            let status = runtime.status.lock().unwrap();
            // Nothing applied yet: SQL applied offset unmoved, IO read at 180.
            assert_eq!(status.relay_log_pos, 0);
            assert_eq!(status.source_log_pos, 180);
            assert_ne!(status.exec_source_log_pos, 180);
        }

        // SQL thread: apply everything durable.
        runner.sql_apply_available(durable).unwrap();
        let status = runtime.status.lock().unwrap().clone();
        assert_eq!(status.relay_log_pos, durable);
        assert_eq!(status.exec_source_log_pos, 180);
        assert!(gtid_set_contains(
            &status.executed_gtid_set,
            gtid.as_deref().unwrap()
        ));
        let result = engine
            .execute("SELECT v FROM t WHERE id = 7")
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(result) = result else {
            panic!("expected result set");
        };
        assert_eq!(result.rows[0][0], Value::String("io".to_string()));
    }

    #[test]
    fn release_audit_logical_applier_pauses_for_native_capture_and_covers_deferred_commit() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute("CREATE TABLE t(id INT PRIMARY KEY)")
            .unwrap();
        let (_runtime, runner) = make_split_runner(dir.path(), engine.clone());
        let db = engine.database_for_replication();
        let gate = crate::storage::capture::logical_apply_gate(db).unwrap();
        let capture = gate.write_arc();
        let (started, starting) = std::sync::mpsc::channel();
        let (done, completed) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            started.send(()).unwrap();
            let result = runner.apply_records(
                vec![RelayOp::WriteRows {
                    database: "test".into(),
                    table: "t".into(),
                    rows: vec![vec![Value::Int(1)]],
                }],
                None,
            );
            done.send(result).unwrap();
            runner
        });
        starting.recv().unwrap();
        assert!(completed.recv_timeout(Duration::from_millis(50)).is_err());
        drop(capture);
        completed
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        let runner = worker.join().unwrap();
        let mut commit = runner
            .prepare_row_records(
                vec![RelayOp::WriteRows {
                    database: "test".into(),
                    table: "t".into(),
                    rows: vec![vec![Value::Int(2)]],
                }],
                None,
            )
            .unwrap()
            .unwrap();
        assert!(
            gate.try_write_arc().is_none(),
            "capture must include deferred durability/sidecars"
        );
        db.finish_deferred_commits(&mut [&mut commit]).unwrap();
        drop(commit);
        assert!(gate.try_write_arc().is_some());
    }

    #[test]
    fn sql_applier_batches_independent_source_commits_under_one_durability_barrier() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20))")
            .unwrap();
        let (_runtime, runner) = make_split_runner(dir.path(), Arc::clone(&engine));
        let db = engine.database_for_replication();
        let before = db.storage_status().group_commit_batches;

        for (id, pos) in [(1_i64, 180_u64), (2, 240)] {
            let gtid = Some(format!("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:{id}"));
            let op = RelayOp::WriteRows {
                database: "test".to_string(),
                table: "t".to_string(),
                rows: vec![vec![Value::Int(id), Value::String(format!("v{id}"))]],
            };
            runner
                .append_relay("bin.000001", pos - 20, gtid.clone(), &op)
                .unwrap();
            let mut txn = TxnBuffer {
                gtid,
                event_count: 1,
                ..TxnBuffer::default()
            };
            runner.io_commit_txn("bin.000001", pos, &mut txn).unwrap();
        }

        let durable = runner.runtime.relay_gate.lock().unwrap().durable_offset;
        runner.sql_apply_available(durable).unwrap();
        let after = db.storage_status().group_commit_batches;
        assert_eq!(
            after - before,
            1,
            "two source commits should share one local WAL durability barrier"
        );
        let result = engine
            .execute("SELECT id, v FROM t ORDER BY id")
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(result) = result else {
            panic!("expected result set");
        };
        assert_eq!(
            result.rows,
            vec![
                vec![Value::Int(1), Value::String("v1".to_string())],
                vec![Value::Int(2), Value::String("v2".to_string())],
            ]
        );
    }

    #[test]
    fn relay_group_sync_publishes_only_durable_io_progress() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let policy = ReplicationFlushPolicy {
            relay_sync_txns: 2,
            relay_sync_interval: Duration::from_secs(3600),
            progress_sync_txns: u64::MAX,
            progress_sync_interval: Duration::from_secs(3600),
            ..ReplicationFlushPolicy::default()
        };
        let (runtime, runner) =
            make_split_runner_with_policy(dir.path(), Arc::clone(&engine), policy);
        let gtid1 = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1".to_string();
        let mut first = TxnBuffer {
            gtid: Some(gtid1.clone()),
            ..TxnBuffer::default()
        };
        runner.io_commit_txn("bin.000001", 180, &mut first).unwrap();
        assert_eq!(runtime.relay_gate.lock().unwrap().durable_offset, 0);
        assert_eq!(runtime.status.lock().unwrap().source_log_pos, 180);

        // Even a forced metadata publication must cap the restart point at
        // the last relay fsync, not the live (still buffered) read position.
        let manager = ReplicationManager {
            data_dir: dir.path().to_path_buf(),
            engine: Arc::clone(&engine),
            channels: Mutex::new(HashMap::new()),
        };
        manager.persist_runtime(&runtime).unwrap();
        let persisted = manager.load_channel(DEFAULT_CHANNEL).unwrap();
        assert_eq!(persisted.status.source_log_pos, 4);
        assert!(persisted.status.retrieved_gtid_set.is_empty());

        let gtid2 = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:2".to_string();
        let mut second = TxnBuffer {
            gtid: Some(gtid2.clone()),
            ..TxnBuffer::default()
        };
        runner
            .io_commit_txn("bin.000001", 240, &mut second)
            .unwrap();
        assert!(runtime.relay_gate.lock().unwrap().durable_offset > 0);
        let durable = runtime.durable_io_progress.lock().unwrap().clone();
        assert_eq!(durable.source_pos, 240);
        assert!(gtid_set_contains(&durable.retrieved_gtid_set, &gtid1));
        assert!(gtid_set_contains(&durable.retrieved_gtid_set, &gtid2));
    }

    #[test]
    fn channel_progress_metadata_is_group_persisted() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let policy = ReplicationFlushPolicy {
            relay_sync_txns: 1,
            relay_sync_interval: Duration::from_secs(3600),
            progress_sync_txns: 2,
            progress_sync_interval: Duration::from_secs(3600),
            ..ReplicationFlushPolicy::default()
        };
        let (_runtime, runner) =
            make_split_runner_with_policy(dir.path(), Arc::clone(&engine), policy);
        let path = channel_path(dir.path(), DEFAULT_CHANNEL);
        let mut first = TxnBuffer {
            gtid: Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1".to_string()),
            ..TxnBuffer::default()
        };
        runner.io_commit_txn("bin.000001", 180, &mut first).unwrap();
        assert!(!path.exists(), "first update must stay in memory");

        let mut second = TxnBuffer {
            gtid: Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:2".to_string()),
            ..TxnBuffer::default()
        };
        runner
            .io_commit_txn("bin.000001", 240, &mut second)
            .unwrap();
        assert!(path.exists(), "threshold update must persist metadata");
        let persisted: PersistedChannel =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(persisted.status.source_log_pos, 240);
        assert!(gtid_set_contains(
            &persisted.status.retrieved_gtid_set,
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:2"
        ));
    }

    #[test]
    fn ddl_applied_progress_bypasses_metadata_grouping() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let policy = ReplicationFlushPolicy {
            relay_sync_txns: 1,
            relay_sync_interval: Duration::from_secs(3600),
            progress_sync_txns: u64::MAX,
            progress_sync_interval: Duration::from_secs(3600),
            ..ReplicationFlushPolicy::default()
        };
        let (runtime, runner) =
            make_split_runner_with_policy(dir.path(), Arc::clone(&engine), policy);
        {
            let mut durable = runtime.durable_io_progress.lock().unwrap();
            durable.source_file = "bin.000001".to_string();
            durable.source_pos = 180;
        }

        let relay_path = relay_log_path(dir.path(), DEFAULT_CHANNEL);
        runner
            .advance_applied(&relay_path, 91, "bin.000001", 180, None, true)
            .unwrap();

        let persisted: PersistedChannel = serde_json::from_slice(
            &std::fs::read(channel_path(dir.path(), DEFAULT_CHANNEL)).unwrap(),
        )
        .unwrap();
        assert_eq!(persisted.status.relay_log_pos, 91);
        assert_eq!(persisted.status.exec_source_log_pos, 180);
        assert_eq!(persisted.status.source_log_pos, 180);
    }

    #[test]
    fn sql_apply_error_leaves_relay_position_unadvanced() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20))")
            .unwrap();
        let (runtime, runner) = make_split_runner(dir.path(), Arc::clone(&engine));

        // IO writes a transaction targeting a table that does not exist; the
        // SQL apply must fail and NOT advance the applied relay offset (so the
        // IO thread can keep buffering while the SQL thread stops).
        let gtid = Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1".to_string());
        let op = RelayOp::WriteRows {
            database: "test".to_string(),
            table: "ghost_table".to_string(),
            rows: vec![vec![Value::Int(1), Value::String("x".to_string())]],
        };
        runner
            .append_relay("bin.000001", 120, gtid.clone(), &op)
            .unwrap();
        let mut txn = TxnBuffer {
            gtid,
            ops: vec![],
            event_count: 1,
            ..TxnBuffer::default()
        };
        runner.io_commit_txn("bin.000001", 180, &mut txn).unwrap();
        let durable = runtime.relay_gate.lock().unwrap().durable_offset;

        let err = runner.sql_apply_available(durable).unwrap_err();
        assert!(!err.message().is_empty());
        // Applied offset stays at 0 — the failed transaction is not marked done.
        assert_eq!(runtime.status.lock().unwrap().relay_log_pos, 0);
        // But the IO side's durable gate still reflects the buffered write.
        assert!(durable > 0);
    }

    #[test]
    fn sql_apply_batch_durably_advances_prior_commits_before_later_error() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20))")
            .unwrap();
        let (runtime, runner) = make_split_runner(dir.path(), Arc::clone(&engine));
        let first_gtid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1".to_string();
        let first = RelayOp::WriteRows {
            database: "test".to_string(),
            table: "t".to_string(),
            rows: vec![vec![Value::Int(1), Value::String("ok".to_string())]],
        };
        runner
            .append_relay("bin.000001", 120, Some(first_gtid.clone()), &first)
            .unwrap();
        let mut first_txn = TxnBuffer {
            gtid: Some(first_gtid.clone()),
            event_count: 1,
            ..TxnBuffer::default()
        };
        runner
            .io_commit_txn("bin.000001", 180, &mut first_txn)
            .unwrap();
        let first_offset = runtime.relay_gate.lock().unwrap().durable_offset;

        let second_gtid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:2".to_string();
        let failing = RelayOp::WriteRows {
            database: "test".to_string(),
            table: "missing".to_string(),
            rows: vec![vec![Value::Int(2), Value::String("bad".to_string())]],
        };
        runner
            .append_relay("bin.000001", 200, Some(second_gtid.clone()), &failing)
            .unwrap();
        let mut second_txn = TxnBuffer {
            gtid: Some(second_gtid.clone()),
            event_count: 1,
            ..TxnBuffer::default()
        };
        runner
            .io_commit_txn("bin.000001", 240, &mut second_txn)
            .unwrap();
        let durable = runtime.relay_gate.lock().unwrap().durable_offset;

        runner.sql_apply_available(durable).unwrap_err();
        let status = runtime.status.lock().unwrap().clone();
        assert_eq!(status.relay_log_pos, first_offset);
        assert!(gtid_set_contains(&status.executed_gtid_set, &first_gtid));
        assert!(!gtid_set_contains(&status.executed_gtid_set, &second_gtid));
        let result = engine
            .execute("SELECT v FROM t WHERE id = 1")
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(result) = result else {
            panic!("expected result set");
        };
        assert_eq!(result.rows, vec![vec![Value::String("ok".to_string())]]);
    }

    #[test]
    fn sql_apply_batch_finishes_prior_commit_before_corrupt_relay_error() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20))")
            .unwrap();
        let (runtime, runner) = make_split_runner(dir.path(), Arc::clone(&engine));
        let gtid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1".to_string();
        let op = RelayOp::WriteRows {
            database: "test".to_string(),
            table: "t".to_string(),
            rows: vec![vec![Value::Int(1), Value::String("ok".to_string())]],
        };
        runner
            .append_relay("bin.000001", 120, Some(gtid.clone()), &op)
            .unwrap();
        let mut txn = TxnBuffer {
            gtid: Some(gtid.clone()),
            event_count: 1,
            ..TxnBuffer::default()
        };
        runner.io_commit_txn("bin.000001", 180, &mut txn).unwrap();
        let first_offset = runtime.relay_gate.lock().unwrap().durable_offset;

        let path = relay_log_path(dir.path(), DEFAULT_CHANNEL);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"{not-valid-json}\n").unwrap();
        file.sync_all().unwrap();
        let durable = file.metadata().unwrap().len();
        runtime.relay_gate.lock().unwrap().durable_offset = durable;

        let err = runner.sql_apply_available(durable).unwrap_err();
        assert_eq!(err.code(), 1030);
        let status = runtime.status.lock().unwrap().clone();
        assert_eq!(status.relay_log_pos, first_offset);
        assert!(gtid_set_contains(&status.executed_gtid_set, &gtid));
        let result = engine
            .execute("SELECT v FROM t WHERE id = 1")
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(result) = result else {
            panic!("expected result set");
        };
        assert_eq!(result.rows, vec![vec![Value::String("ok".to_string())]]);
    }

    #[test]
    fn compressed_transaction_payload_replays_inner_events() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageQueryEngine::open(dir.path()).unwrap());
        let mut cfg = ReplicationChannelConfig::default();
        cfg.source_host = "127.0.0.1".to_string();
        cfg.source_log_file = "mysql-bin.000001".to_string();
        let runtime = Arc::new(ChannelRuntime {
            config: Mutex::new(cfg.clone()),
            status: Arc::new(Mutex::new(ReplicationStatus::from_config(&cfg, dir.path()))),
            stop: Arc::new(AtomicBool::new(false)),
            io_stop: Arc::new(AtomicBool::new(false)),
            sql_stop: Arc::new(AtomicBool::new(false)),
            io_join: Mutex::new(None),
            sql_join: Mutex::new(None),
            until: Mutex::new(None),
            flush_policy: ReplicationFlushPolicy {
                relay_sync_txns: 1,
                ..ReplicationFlushPolicy::default()
            },
            relay_writer: Mutex::new(None),
            relay_sync: Mutex::new(RelaySyncState::default()),
            relay_gate: Mutex::new(RelayGate::default()),
            relay_cond: Condvar::new(),
            durable_io_progress: Mutex::new(DurableIoProgress::from_status(
                &ReplicationStatus::from_config(&cfg, dir.path()),
            )),
            progress_persist: Mutex::new(ProgressPersistState::default()),
            io_committed_pos: Mutex::new(None),
        });
        let runner = ChannelRunner::new(dir.path().to_path_buf(), engine, Arc::clone(&runtime));

        let mut inner = Vec::new();
        inner.extend(test_query_event_bytes("BEGIN", 120));
        inner.extend(test_query_event_bytes("COMMIT", 180));
        let compressed = zstd::stream::encode_all(&inner[..], 0).unwrap();
        let tpe = TransactionPayloadEvent::new(
            compressed.len() as u64,
            TransactionPayloadCompressionType::ZSTD,
            inner.len() as u64,
            compressed,
        );
        let mut tpe_body = Vec::new();
        tpe.serialize(&mut tpe_body);
        let tpe_bytes = test_event_bytes(EventType::TRANSACTION_PAYLOAD_EVENT, 240, tpe_body);
        let mut event_reader = EventStreamReader::new(BinlogVersion::Version4);
        let event = event_reader.read(&tpe_bytes[..]).unwrap().unwrap();

        let mut current_file = "mysql-bin.000001".to_string();
        let mut table_maps = HashMap::new();
        let mut txn = TxnBuffer::default();
        runner
            .handle_source_event(&event, &mut current_file, 240, &mut table_maps, &mut txn)
            .unwrap();

        let status = runtime.status.lock().unwrap().clone();
        assert_eq!(status.source_log_file, "mysql-bin.000001");
        assert_eq!(status.source_log_pos, 240);
        assert_eq!(status.received_events, 2);
    }

    #[test]
    fn audit_query_event_decodes_client_charset_and_rejects_invalid_bytes() {
        let query = QueryEvent::new(vec![4, 8, 0, 8, 0, 255, 0], b"test".to_vec())
            .with_query(b"CREATE TABLE t(id INT) COMMENT='caf\xe9'".to_vec());
        let state = ddl_session_state_from_query_event(&query);
        assert_eq!(state.charset_client_id, Some(8));
        assert_eq!(
            decode_query_event_sql(&query, &state).unwrap(),
            "CREATE TABLE t(id INT) COMMENT='café'"
        );
        let query = QueryEvent::new(vec![4, 255, 0, 255, 0, 255, 0], b"test".to_vec())
            .with_query(b"SELECT '\xff'".to_vec());
        assert!(
            decode_query_event_sql(&query, &ddl_session_state_from_query_event(&query)).is_err()
        );
    }

    #[test]
    fn audit_query_events_reject_statement_data_but_accept_catalog_ddl() {
        for sql in [
            "INSERT INTO t VALUES(UUID())",
            "/* source session override */ UPDATE t SET id=2",
            "DELETE FROM t",
            "REPLACE INTO t VALUES(1)",
            "CALL p()",
            "SELECT UUID()",
            "DO UUID()",
            "SET @v=UUID()",
            "WITH c AS (SELECT 1) SELECT * FROM c",
            "CREATE TABLE t AS SELECT UUID()",
            "/*!80000 INSERT INTO t VALUES(1) */",
        ] {
            assert_eq!(
                validate_row_query_event(sql, 0).unwrap_err().code(),
                1235,
                "{sql}"
            );
        }
        assert!(!is_session_query("SET @v=UUID()"));
        for sql in [
            "CREATE TABLE t(id INT)",
            "CREATE TABLE t(id INT) START TRANSACTION",
            "ALTER TABLE t ADD COLUMN v INT",
            "TRUNCATE TABLE t",
            "CREATE VIEW v AS SELECT UUID()",
            "SET PASSWORD FOR 'u'@'%' = 'filtered auth metadata'",
        ] {
            validate_row_query_event(sql, 0).unwrap();
        }
        validate_row_query_event(
            "CREATE TABLE \"quoted\"(\"id\" INT)",
            crate::binlog::sql_mode_str_to_bits("ANSI_QUOTES"),
        )
        .unwrap();
    }

    fn append_test_relay(data_dir: &Path, record: &RelayRecord) {
        let path = relay_log_path(data_dir, DEFAULT_CHANNEL);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        serde_json::to_writer(&mut file, record).unwrap();
        file.write_all(b"\n").unwrap();
    }

    fn test_query_event_bytes(sql: &str, log_pos: u32) -> Vec<u8> {
        let query =
            QueryEvent::new(Vec::<u8>::new(), b"test".to_vec()).with_query(sql.as_bytes().to_vec());
        let mut body = Vec::new();
        query.serialize(&mut body);
        test_event_bytes(EventType::QUERY_EVENT, log_pos, body)
    }

    fn test_event_bytes(event_type: EventType, log_pos: u32, body: Vec<u8>) -> Vec<u8> {
        let event_size = (BinlogEventHeader::LEN + body.len()) as u32;
        let header =
            BinlogEventHeader::new(0, event_type, 1, event_size, log_pos, EventFlags::empty());
        let mut out = Vec::with_capacity(event_size as usize);
        header.serialize(&mut out);
        out.extend(body);
        out
    }
    include!("replication_audit_1028_tests.rs");
}

fn poisoned_lock(message: &str) -> QueryError {
    error(crate::error::catalog::ER_GET_ERRNO, message)
}
