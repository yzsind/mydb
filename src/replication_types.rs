//! Replication metadata shared by enabled and disabled receiver builds.

use crate::types::{Column, ColumnType};
use serde::{Deserialize, Serialize};

#[cfg_attr(not(feature = "mysql-replica"), derive(Default))]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationChannelConfig {
    pub channel: String,
    pub source_host: String,
    pub source_port: u16,
    pub source_user: String,
    pub source_password: String,
    pub server_id: u32,
    pub source_log_file: String,
    pub source_log_pos: u64,
    pub auto_position: bool,
    pub connect_retry_ms: u64,
    #[serde(default)]
    pub do_databases: Vec<String>,
    #[serde(default)]
    pub ignore_databases: Vec<String>,
    /// Exact `db.table` allow-list. When non-empty, a row event is only
    /// applied if its `db.table` matches one of these (or a wildcard rule
    /// below). Matches the MySQL CLI flag `--replicate-do-table`.
    #[serde(default)]
    pub do_tables: Vec<String>,
    /// Exact `db.table` deny-list. Wins over `do_tables` and `wild_do_tables`.
    /// Matches `--replicate-ignore-table`.
    #[serde(default)]
    pub ignore_tables: Vec<String>,
    /// LIKE-style `db.table` allow-list. Matches `--replicate-wild-do-table`.
    /// `%` matches any sequence of characters; `_` matches a single character.
    #[serde(default)]
    pub wild_do_tables: Vec<String>,
    /// LIKE-style `db.table` deny-list. Matches `--replicate-wild-ignore-table`.
    #[serde(default)]
    pub wild_ignore_tables: Vec<String>,
    /// TLS / SSL options for the source connection. Map to MySQL's
    /// `CHANGE REPLICATION SOURCE TO SOURCE_SSL_*` clauses; an `ssl_mode` of
    /// `disabled` skips TLS entirely (legacy default). `required` and
    /// `verify_ca`/`verify_identity` upgrade the connection.
    #[serde(default)]
    pub ssl_mode: ReplicationSslMode,
    #[serde(default)]
    pub ssl_ca: Option<String>,
    #[serde(default)]
    pub ssl_cert: Option<String>,
    #[serde(default)]
    pub ssl_key: Option<String>,
    #[serde(default)]
    pub ssl_cipher: Option<String>,
    /// `MASTER_DELAY` / `SOURCE_DELAY` in seconds — the replica deliberately
    /// lags the source by this many seconds. Implemented by stalling each
    /// event until its timestamp is ≤ `now - delay`. `0` disables.
    #[serde(default)]
    pub source_delay_secs: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationSslMode {
    #[default]
    #[serde(rename = "DISABLED", alias = "disabled")]
    Disabled,
    #[serde(rename = "REQUIRED", alias = "required")]
    Required,
    #[serde(rename = "VERIFY_CA", alias = "verify_ca")]
    VerifyCa,
    #[serde(rename = "VERIFY_IDENTITY", alias = "verify_identity")]
    VerifyIdentity,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationStatus {
    pub channel: String,
    pub configured: bool,
    pub running: bool,
    pub io_state: String,
    pub sql_state: String,
    pub last_error: Option<String>,
    pub source_host: String,
    pub source_port: u16,
    pub source_log_file: String,
    pub source_log_pos: u64,
    pub relay_log_file: String,
    pub relay_log_pos: u64,
    pub retrieved_gtid_set: String,
    pub executed_gtid_set: String,
    pub received_events: u64,
    pub applied_events: u64,
    pub applied_transactions: u64,
    pub last_event_timestamp: u64,
    pub seconds_behind_source: Option<u64>,
    /// Count of binlog events received from the source whose CRC32 trailer
    /// disagreed with the recomputed digest. Persisted to the status JSON
    /// so reconnects don't reset it; exposed via SHOW REPLICA STATUS.
    #[serde(default)]
    pub binlog_checksum_failures: u64,
    /// Count of source events suppressed by the replication filter
    /// (do/ignore DB/table, wild patterns, builtin mydb-virtual table list).
    /// Exposed via SHOW REPLICA STATUS so operators can verify their
    /// filter shape behaves as expected.
    #[serde(default)]
    pub filtered_events: u64,
    /// Last error attributed to the IO thread (connect / read binlog stream /
    /// event parse / checksum). Drives `Last_IO_Errno` / `Last_IO_Error` /
    /// `Last_IO_Error_Timestamp`.
    #[serde(default)]
    pub last_io_error: Option<ReplThreadError>,
    /// Last error attributed to the SQL thread (applying a row/DDL event).
    /// Drives `Last_SQL_Errno` / `Last_SQL_Error` / `Last_SQL_Error_Timestamp`.
    #[serde(default)]
    pub last_sql_error: Option<ReplThreadError>,
    /// Whether the SQL (applier) thread is running. `running` tracks the IO
    /// thread; the two are independently startable/stoppable
    /// (`START/STOP REPLICA IO_THREAD|SQL_THREAD`).
    #[serde(default)]
    pub sql_running: bool,
    /// Source log file/position the SQL thread has *applied* through
    /// (`Exec_Source_Log_Pos` / `Relay_Source_Log_File`). Distinct from
    /// `source_log_*`, which the IO thread advances as it *reads*.
    #[serde(default)]
    pub exec_source_log_file: String,
    #[serde(default)]
    pub exec_source_log_pos: u64,
    /// The source server's `@@global.server_id` / `@@global.server_uuid`,
    /// probed once at connect time. Surfaced as `Source_Server_Id` /
    /// `Source_UUID` in `SHOW REPLICA STATUS`. Zero / empty before the IO
    /// thread has connected.
    #[serde(default)]
    pub source_server_id: u32,
    #[serde(default)]
    pub source_uuid: String,
    /// Number of heartbeat events received from the source since this
    /// channel was reset. MySQL exposes it via
    /// `performance_schema.replication_connection_status.COUNT_RECEIVED_HEARTBEATS`.
    #[serde(default)]
    pub count_received_heartbeats: u64,
    /// Unix timestamp (seconds) of the last heartbeat event received.
    /// MySQL exposes it via `LAST_HEARTBEAT_TIMESTAMP` in the same PFS table.
    /// Zero before the first heartbeat.
    #[serde(default)]
    pub last_heartbeat_at_unix_secs: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplThreadError {
    pub code: u16,
    pub message: String,
    pub at_unix_secs: u64,
}

impl ReplicationSslMode {
    #[cfg(feature = "mysql-replica")]
    pub(crate) fn from_label(s: &str) -> Option<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "DISABLED" => Some(Self::Disabled),
            "PREFERRED" | "REQUIRED" => Some(Self::Required),
            "VERIFY_CA" => Some(Self::VerifyCa),
            "VERIFY_IDENTITY" => Some(Self::VerifyIdentity),
            _ => None,
        }
    }

    pub fn requires_tls(self) -> bool {
        !matches!(self, Self::Disabled)
    }
}

const REPLICA_STATUS_COLUMN_DEFS: &[(&str, ColumnType)] = &[
    ("Replica_IO_State", ColumnType::VarString),
    ("Source_Host", ColumnType::VarString),
    ("Source_User", ColumnType::VarString),
    ("Source_Port", ColumnType::LongLong),
    ("Connect_Retry", ColumnType::LongLong),
    ("Source_Log_File", ColumnType::VarString),
    ("Read_Source_Log_Pos", ColumnType::LongLong),
    ("Relay_Log_File", ColumnType::VarString),
    ("Relay_Log_Pos", ColumnType::LongLong),
    ("Relay_Source_Log_File", ColumnType::VarString),
    ("Replica_IO_Running", ColumnType::VarString),
    ("Replica_SQL_Running", ColumnType::VarString),
    ("Replicate_Do_DB", ColumnType::VarString),
    ("Replicate_Ignore_DB", ColumnType::VarString),
    ("Replicate_Do_Table", ColumnType::VarString),
    ("Replicate_Ignore_Table", ColumnType::VarString),
    ("Replicate_Wild_Do_Table", ColumnType::VarString),
    ("Replicate_Wild_Ignore_Table", ColumnType::VarString),
    ("Last_Errno", ColumnType::LongLong),
    ("Last_Error", ColumnType::VarString),
    ("Skip_Counter", ColumnType::LongLong),
    ("Exec_Source_Log_Pos", ColumnType::LongLong),
    ("Relay_Log_Space", ColumnType::LongLong),
    ("Until_Condition", ColumnType::VarString),
    ("Until_Log_File", ColumnType::VarString),
    ("Until_Log_Pos", ColumnType::LongLong),
    ("Source_SSL_Allowed", ColumnType::VarString),
    ("Source_SSL_CA_File", ColumnType::VarString),
    ("Source_SSL_CA_Path", ColumnType::VarString),
    ("Source_SSL_Cert", ColumnType::VarString),
    ("Source_SSL_Cipher", ColumnType::VarString),
    ("Source_SSL_Key", ColumnType::VarString),
    ("Seconds_Behind_Source", ColumnType::LongLong),
    ("Source_SSL_Verify_Server_Cert", ColumnType::VarString),
    ("Last_IO_Errno", ColumnType::LongLong),
    ("Last_IO_Error", ColumnType::VarString),
    ("Last_SQL_Errno", ColumnType::LongLong),
    ("Last_SQL_Error", ColumnType::VarString),
    ("Replicate_Ignore_Server_Ids", ColumnType::VarString),
    ("Source_Server_Id", ColumnType::LongLong),
    ("Source_UUID", ColumnType::VarString),
    ("Source_Info_File", ColumnType::VarString),
    ("SQL_Delay", ColumnType::LongLong),
    ("SQL_Remaining_Delay", ColumnType::LongLong),
    ("Replica_SQL_Running_State", ColumnType::VarString),
    ("Source_Retry_Count", ColumnType::LongLong),
    ("Source_Bind", ColumnType::VarString),
    ("Last_IO_Error_Timestamp", ColumnType::VarString),
    ("Last_SQL_Error_Timestamp", ColumnType::VarString),
    ("Source_SSL_Crl", ColumnType::VarString),
    ("Source_SSL_Crlpath", ColumnType::VarString),
    ("Retrieved_Gtid_Set", ColumnType::VarString),
    ("Executed_Gtid_Set", ColumnType::VarString),
    ("Auto_Position", ColumnType::LongLong),
    ("Replicate_Rewrite_DB", ColumnType::VarString),
    ("Channel_Name", ColumnType::VarString),
    ("Source_TLS_Version", ColumnType::VarString),
    ("Source_public_key_path", ColumnType::VarString),
    ("Get_Source_public_key", ColumnType::LongLong),
    ("Network_Namespace", ColumnType::VarString),
];

impl ReplicationStatus {
    pub(crate) fn columns(legacy: bool) -> Vec<Column> {
        if legacy {
            Self::build_columns::<true>()
        } else {
            Self::build_columns::<false>()
        }
    }

    fn build_columns<const LEGACY: bool>() -> Vec<Column> {
        REPLICA_STATUS_COLUMN_DEFS
            .iter()
            .map(|(name, column_type)| Column {
                source: Default::default(),
                name: if LEGACY {
                    crate::replication_columns::legacy_replica_column_alias(name).unwrap_or(name)
                } else {
                    name
                }
                .to_string(),
                column_type: *column_type,
            })
            .collect()
    }
}
