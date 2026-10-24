//! MySQL-style, explicit single-file startup configuration.
//!
//! The server only reads an option file when `--config PATH` is present.  The
//! resolved values live in a process-local read-only overlay; the OS
//! environment is never mutated, which is important because `RESTART`
//! re-executes the server and must re-read a possibly changed file.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::engine::sysvar::meta::DataType;
use crate::engine::sysvar::value::SysVarValue;

pub const DEFAULT_PORT: u16 = 8738;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    message: String,
}

impl ConfigError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ConfigError {}

#[derive(Debug, Clone)]
pub struct StartupSysVarAssignment {
    pub name: String,
    pub value: SysVarValue,
}

#[derive(Debug, Clone)]
pub struct StartupConfig {
    pub config_path: Option<PathBuf>,
    pub warnings: Vec<String>,
    pub sysvars: Vec<StartupSysVarAssignment>,
    pub help: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Group {
    Mysqld,
    Mydb,
    Ignored,
}

#[derive(Debug, Clone)]
struct Entry {
    group: Group,
    key: String,
    value: String,
    line: usize,
}

#[derive(Debug, Clone, Copy)]
enum OptionKind {
    Bool,
    U16,
    U32,
    U64,
    U64Range(u64, u64),
    Bytes,
    BytesRange(u64, u64),
    Mebibytes,
    NonNegativeFloat,
    LogOutput,
    IpAddr,
    String,
    NonEmpty,
    Enum(&'static [&'static str]),
    Removed(&'static str),
}

#[derive(Debug, Clone, Copy)]
struct OptionSpec {
    name: &'static str,
    env: &'static str,
    kind: OptionKind,
    aliases: &'static [&'static str],
    cli: bool,
    sensitive: bool,
}

macro_rules! spec {
    ($name:literal, $env:literal, $kind:expr) => {
        OptionSpec {
            name: $name,
            env: $env,
            kind: $kind,
            aliases: &[],
            cli: false,
            sensitive: false,
        }
    };
}

// Deliberately explicit: adding a new MYDB_* variable elsewhere does not make
// it a supported option-file setting.  Registered GLOBAL sysvars are handled
// separately below, using their registry metadata as the whitelist.
static OPTION_SPECS: &[OptionSpec] = &[
    OptionSpec {
        name: "init_connect",
        env: "MYDB_INIT_CONNECT",
        kind: OptionKind::String,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "host",
        env: "MYDB_HOST",
        kind: OptionKind::IpAddr,
        aliases: &["bind_address"],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "port",
        env: "MYDB_PORT",
        kind: OptionKind::U16,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "data_dir",
        env: "MYDB_DATA_DIR",
        kind: OptionKind::NonEmpty,
        aliases: &["datadir"],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "slow_sql_ms",
        env: "MYDB_SLOW_SQL_MS",
        kind: OptionKind::U64,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "ssl_cert",
        env: "MYDB_SSL_CERT",
        kind: OptionKind::NonEmpty,
        aliases: &["ssl_certificate"],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "ssl_key",
        env: "MYDB_SSL_KEY",
        kind: OptionKind::NonEmpty,
        aliases: &[],
        cli: true,
        sensitive: true,
    },
    spec!("ssl_ca", "MYDB_SSL_CA", OptionKind::NonEmpty),
    OptionSpec {
        name: "ssl_mode",
        env: "MYDB_SSL_MODE",
        kind: OptionKind::Enum(&[
            "auto", "on", "enabled", "enable", "disabled", "disable", "off", "0", "false",
        ]),
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "general_log",
        env: "MYDB_GENERAL_LOG",
        kind: OptionKind::Bool,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "general_log_file",
        env: "MYDB_GENERAL_LOG_FILE",
        kind: OptionKind::NonEmpty,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "slow_query_log",
        env: "MYDB_SLOW_QUERY_LOG",
        kind: OptionKind::Bool,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "slow_query_log_file",
        env: "MYDB_SLOW_QUERY_LOG_FILE",
        kind: OptionKind::NonEmpty,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "log_output",
        env: "MYDB_LOG_OUTPUT",
        kind: OptionKind::LogOutput,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "long_query_time",
        env: "MYDB_LONG_QUERY_TIME",
        kind: OptionKind::NonNegativeFloat,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "min_examined_row_limit",
        env: "MYDB_MIN_EXAMINED_ROW_LIMIT",
        kind: OptionKind::U64,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "log_slow_admin_statements",
        env: "MYDB_LOG_SLOW_ADMIN_STATEMENTS",
        kind: OptionKind::Bool,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "log_queries_not_using_indexes",
        env: "MYDB_LOG_QUERIES_NOT_USING_INDEXES",
        kind: OptionKind::Bool,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "log_error",
        env: "MYDB_LOG_ERROR",
        kind: OptionKind::NonEmpty,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "log_error_verbosity",
        env: "MYDB_LOG_ERROR_VERBOSITY",
        kind: OptionKind::U64Range(1, 3),
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "log_timestamps",
        env: "MYDB_LOG_TIMESTAMPS",
        kind: OptionKind::Enum(&["UTC", "SYSTEM"]),
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "log_bin",
        env: "MYDB_LOG_BIN",
        kind: OptionKind::Bool,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "server_id",
        env: "MYDB_SERVER_ID",
        kind: OptionKind::U32,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "server_uuid",
        env: "MYDB_SERVER_UUID",
        kind: OptionKind::NonEmpty,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "binlog_base_name",
        env: "MYDB_BINLOG_BASE_NAME",
        kind: OptionKind::NonEmpty,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "binlog_checksum",
        env: "MYDB_BINLOG_CHECKSUM",
        kind: OptionKind::Enum(&["CRC32", "NONE", "OFF"]),
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "binlog_retention_hours",
        env: "MYDB_BINLOG_RETENTION_HOURS",
        kind: OptionKind::U64,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "binlog_retention_bytes",
        env: "MYDB_BINLOG_RETENTION_BYTES",
        kind: OptionKind::Bytes,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "log_replica_updates",
        env: "MYDB_LOG_REPLICA_UPDATES",
        kind: OptionKind::Bool,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "sync_binlog",
        env: "MYDB_SYNC_BINLOG",
        kind: OptionKind::U64,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "binlog_rotate_bytes",
        env: "MYDB_BINLOG_ROTATE_BYTES",
        kind: OptionKind::Bytes,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "tokio_workers",
        env: "MYDB_TOKIO_WORKERS",
        kind: OptionKind::U64Range(1, u64::MAX),
        aliases: &[],
        cli: false,
        sensitive: false,
    },
    OptionSpec {
        name: "event_workers",
        env: "MYDB_EVENT_WORKERS",
        kind: OptionKind::U64Range(1, 32),
        aliases: &[],
        cli: false,
        sensitive: false,
    },
    OptionSpec {
        name: "thread_qos",
        env: "MYDB_THREAD_QOS",
        kind: OptionKind::Enum(&["interactive", "initiated"]),
        aliases: &[],
        cli: false,
        sensitive: false,
    },
    spec!(
        "durability",
        "MYDB_DURABILITY",
        OptionKind::Enum(&["strict", "relaxed", "memory"])
    ),
    spec!("redo_enabled", "MYDB_REDO_ENABLED", OptionKind::Bool),
    OptionSpec {
        name: "zone_map_enabled",
        env: "MYDB_ZONE_MAP_ENABLED",
        aliases: &["mydb_zone_map_enabled"],
        kind: OptionKind::Bool,
        cli: false,
        sensitive: false,
    },
    OptionSpec {
        name: "wal_sync_method",
        env: "MYDB_WAL_SYNC_METHOD",
        kind: OptionKind::Enum(&["fdatasync", "fsync", "fullfsync", "barrierfsync", "nosync"]),
        aliases: &["mydb_wal_sync_method"],
        cli: false,
        sensitive: false,
    },
    OptionSpec {
        name: "wal_write_buffer_bytes",
        env: "MYDB_WAL_WRITE_BUFFER_BYTES",
        kind: OptionKind::BytesRange(0, crate::wal::MAX_WAL_WRITE_BUFFER_BYTES),
        aliases: &["mydb_wal_write_buffer_bytes"],
        cli: false,
        sensitive: false,
    },
    OptionSpec {
        name: "wal_segment_bytes",
        env: "MYDB_WAL_SEGMENT_BYTES",
        kind: OptionKind::Bytes,
        aliases: &["mydb_wal_segment_bytes"],
        cli: false,
        sensitive: false,
    },
    spec!(
        "checkpoint_interval_ms",
        "MYDB_CHECKPOINT_INTERVAL_MS",
        OptionKind::U64
    ),
    OptionSpec {
        name: "checkpoint_wal_bytes",
        env: "MYDB_CHECKPOINT_WAL_BYTES",
        kind: OptionKind::Bytes,
        aliases: &["mydb_checkpoint_wal_bytes"],
        cli: false,
        sensitive: false,
    },
    spec!("lock_wait_ms", "MYDB_LOCK_WAIT_MS", OptionKind::U64),
    OptionSpec {
        name: "group_commit_us",
        env: "MYDB_GROUP_COMMIT_US",
        kind: OptionKind::U64,
        aliases: &["mydb_group_commit_us"],
        cli: false,
        sensitive: false,
    },
    OptionSpec {
        name: "group_commit_adaptive",
        env: "MYDB_GROUP_COMMIT_ADAPTIVE",
        kind: OptionKind::Bool,
        aliases: &["mydb_group_commit_adaptive"],
        cli: false,
        sensitive: false,
    },
    OptionSpec {
        name: "memory_target",
        env: "MYDB_MEMORY_TARGET",
        kind: OptionKind::Mebibytes,
        aliases: &["mydb_memory_target_mb"],
        cli: false,
        sensitive: false,
    },
    OptionSpec {
        name: "executor_memory_target",
        env: "MYDB_EXECUTOR_MEMORY_TARGET",
        kind: OptionKind::Mebibytes,
        aliases: &["mydb_executor_memory_target_mb"],
        cli: false,
        sensitive: false,
    },
    spec!(
        "executor_memory_pct",
        "MYDB_EXECUTOR_MEMORY_PCT",
        OptionKind::U64Range(1, 100)
    ),
    OptionSpec {
        name: "buffer_pool_pages",
        env: "MYDB_BUFFER_POOL_PAGES",
        kind: OptionKind::U64,
        aliases: &["mydb_buffer_pool_pages"],
        cli: false,
        sensitive: false,
    },
    spec!(
        "buffer_pool_pct",
        "MYDB_BUFFER_POOL_PCT",
        OptionKind::U64Range(1, 100)
    ),
    spec!(
        "buffer_pool_io_threads",
        "MYDB_BUFFER_POOL_IO_THREADS",
        OptionKind::U64
    ),
    spec!(
        "buffer_pool_decoded_cache",
        "MYDB_BUFFER_POOL_DECODED_CACHE",
        OptionKind::Removed("decoded cache has been removed")
    ),
    spec!(
        "buffer_pool_decoded_cache_max_pct",
        "MYDB_BUFFER_POOL_DECODED_CACHE_MAX_PCT",
        OptionKind::Removed("decoded cache has been removed")
    ),
    OptionSpec {
        name: "buffer_pool_prefetch_pages",
        env: "MYDB_BUFFER_POOL_PREFETCH_PAGES",
        kind: OptionKind::U64,
        aliases: &["mydb_buffer_pool_prefetch_pages"],
        cli: false,
        sensitive: false,
    },
    OptionSpec {
        name: "buffer_pool_small_scan_pct",
        env: "MYDB_BUFFER_POOL_SMALL_SCAN_PCT",
        kind: OptionKind::U64Range(0, 100),
        aliases: &["mydb_buffer_pool_small_scan_pct"],
        cli: false,
        sensitive: false,
    },
    spec!(
        "buffer_pool_file_cache_max",
        "MYDB_BUFFER_POOL_FILE_CACHE_MAX",
        OptionKind::U64
    ),
    OptionSpec {
        name: "undo_retention_ms",
        env: "MYDB_UNDO_RETENTION_MS",
        kind: OptionKind::U64,
        aliases: &["mydb_undo_retention_ms"],
        cli: false,
        sensitive: false,
    },
    OptionSpec {
        name: "undo_segment_bytes",
        env: "MYDB_UNDO_SEGMENT_BYTES",
        kind: OptionKind::Bytes,
        aliases: &["mydb_undo_segment_bytes"],
        cli: false,
        sensitive: false,
    },
    OptionSpec {
        name: "undo_purge_interval_ms",
        env: "MYDB_UNDO_PURGE_INTERVAL_MS",
        kind: OptionKind::U64,
        aliases: &["mydb_undo_purge_interval_ms"],
        cli: false,
        sensitive: false,
    },
    OptionSpec {
        name: "undo_purge_batch_records",
        env: "MYDB_UNDO_PURGE_BATCH_RECORDS",
        kind: OptionKind::U64Range(1, u64::MAX),
        aliases: &["mydb_undo_purge_batch_records"],
        cli: false,
        sensitive: false,
    },
    OptionSpec {
        name: "undo_tablespace_max_bytes",
        env: "MYDB_UNDO_TABLESPACE_MAX_BYTES",
        kind: OptionKind::Bytes,
        aliases: &["mydb_undo_tablespace_max_bytes"],
        cli: false,
        sensitive: false,
    },
    spec!(
        "undo_mem_soft_limit",
        "MYDB_UNDO_MEM_SOFT_LIMIT",
        OptionKind::Bytes
    ),
    OptionSpec {
        name: "undo_txn_max_bytes",
        env: "MYDB_UNDO_TXN_MAX_BYTES",
        kind: OptionKind::Bytes,
        aliases: &["mydb_undo_txn_max_bytes"],
        cli: false,
        sensitive: false,
    },
    OptionSpec {
        name: "undo_retention_guarantee",
        env: "MYDB_UNDO_RETENTION_GUARANTEE",
        kind: OptionKind::Bool,
        aliases: &["mydb_undo_retention_guarantee"],
        cli: false,
        sensitive: false,
    },
    spec!("parallel_degree", "MYDB_PARALLEL_DEGREE", OptionKind::U64),
    spec!(
        "parallel_threshold_rows",
        "MYDB_PARALLEL_THRESHOLD_ROWS",
        OptionKind::U64
    ),
    spec!(
        "parallel_min_pages",
        "MYDB_PARALLEL_MIN_PAGES",
        OptionKind::U64
    ),
    spec!(
        "parallel_disable",
        "MYDB_PARALLEL_DISABLE",
        OptionKind::Bool
    ),
    spec!(
        "vectorized",
        "MYDB_VECTORIZED",
        OptionKind::Enum(&["off", "on", "auto"])
    ),
    spec!("plan_cache", "MYDB_PLAN_CACHE", OptionKind::Bool),
    OptionSpec {
        name: "query_mem_limit_mb",
        env: "MYDB_QUERY_MEM_LIMIT_MB",
        kind: OptionKind::Mebibytes,
        aliases: &["mydb_query_mem_limit_mb"],
        cli: false,
        sensitive: false,
    },
    spec!(
        "query_mem_quota_pct",
        "MYDB_QUERY_MEM_QUOTA_PCT",
        OptionKind::U64Range(1, 100)
    ),
    OptionSpec {
        name: "process_mem_limit_mb",
        env: "MYDB_PROCESS_MEM_LIMIT_MB",
        kind: OptionKind::Mebibytes,
        aliases: &["mydb_process_mem_limit_mb"],
        cli: false,
        sensitive: false,
    },
    OptionSpec {
        name: "idle_memory_reclaim",
        env: "MYDB_IDLE_MEMORY_RECLAIM",
        kind: OptionKind::Bool,
        aliases: &["mydb_idle_memory_reclaim"],
        cli: false,
        sensitive: false,
    },
    spec!(
        "session_temp_table_limit",
        "MYDB_SESSION_TEMP_TABLE_LIMIT",
        OptionKind::U64
    ),
    spec!(
        "session_temp_storage_limit_bytes",
        "MYDB_SESSION_TEMP_STORAGE_LIMIT",
        OptionKind::U64
    ),
    spec!(
        "session_temp_undo_limit_bytes",
        "MYDB_SESSION_TEMP_UNDO_LIMIT",
        OptionKind::U64
    ),
    spec!(
        "temp_disk_limit_bytes",
        "MYDB_TEMP_DISK_LIMIT",
        OptionKind::U64
    ),
    spec!(
        "temp_file_limit_per_query_bytes",
        "MYDB_TEMP_FILE_LIMIT_PER_QUERY",
        OptionKind::U64
    ),
    spec!(
        "temp_disk_reserve_pct",
        "MYDB_TEMP_DISK_RESERVE_PCT",
        OptionKind::U64Range(0, 90)
    ),
    spec!(
        "work_area_oversubscribe_pct",
        "MYDB_WORK_AREA_OVERSUBSCRIBE_PCT",
        OptionKind::U64Range(100, 100_000)
    ),
    OptionSpec {
        name: "hash_join_mem_limit_mb",
        env: "MYDB_HASH_JOIN_MEM_LIMIT_MB",
        kind: OptionKind::Mebibytes,
        aliases: &["mydb_hash_join_mem_limit_mb"],
        cli: false,
        sensitive: false,
    },
    spec!(
        "hash_join_grace_partitions",
        "MYDB_HASH_JOIN_GRACE_PARTITIONS",
        OptionKind::U64Range(2, 256)
    ),
    spec!(
        "hash_join_grace_max_depth",
        "MYDB_HASH_JOIN_GRACE_MAX_DEPTH",
        OptionKind::U64Range(1, 8)
    ),
    spec!(
        "hash_join_parallelism",
        "MYDB_HASH_JOIN_PARALLELISM",
        OptionKind::U64Range(1, 64)
    ),
    spec!(
        "hash_join_work_area",
        "MYDB_HASH_JOIN_WORK_AREA",
        OptionKind::Bool
    ),
    spec!(
        "hash_join_fallback",
        "MYDB_HASH_JOIN_FALLBACK",
        OptionKind::Enum(&["chunked", "error"])
    ),
    spec!(
        "vector_storage",
        "MYDB_VECTOR_STORAGE",
        OptionKind::Enum(&["paged", "memory"])
    ),
    spec!(
        "max_connections",
        "MYDB_MAX_CONNECTIONS",
        OptionKind::U64Range(1, 1_000_000)
    ),
    spec!(
        "max_allowed_packet",
        "MYDB_MAX_ALLOWED_PACKET",
        OptionKind::BytesRange(1024, 1024 * 1024 * 1024)
    ),
    spec!("open_files_limit", "MYDB_OPEN_FILES_LIMIT", OptionKind::U64),
    spec!("listen_backlog", "MYDB_LISTEN_BACKLOG", OptionKind::U64),
    spec!(
        "handshake_timeout",
        "MYDB_HANDSHAKE_TIMEOUT",
        OptionKind::U64
    ),
    OptionSpec {
        name: "root_password",
        env: "MYDB_ROOT_PASSWORD",
        kind: OptionKind::String,
        aliases: &[],
        cli: false,
        sensitive: true,
    },
    spec!(
        "default_auth_plugin",
        "MYDB_DEFAULT_AUTH_PLUGIN",
        OptionKind::Enum(&[
            "caching_sha2_password",
            "mysql_native_password",
            "sha256_password"
        ])
    ),
    spec!(
        "repl_source_host",
        "MYDB_REPL_SOURCE_HOST",
        OptionKind::NonEmpty
    ),
    spec!("repl_source_port", "MYDB_REPL_SOURCE_PORT", OptionKind::U16),
    spec!("repl_user", "MYDB_REPL_USER", OptionKind::NonEmpty),
    OptionSpec {
        name: "repl_password",
        env: "MYDB_REPL_PASSWORD",
        kind: OptionKind::String,
        aliases: &[],
        cli: false,
        sensitive: true,
    },
    spec!(
        "repl_source_log_file",
        "MYDB_REPL_SOURCE_LOG_FILE",
        OptionKind::String
    ),
    spec!(
        "repl_source_log_pos",
        "MYDB_REPL_SOURCE_LOG_POS",
        OptionKind::U64
    ),
    spec!("repl_server_id", "MYDB_REPL_SERVER_ID", OptionKind::U32),
    spec!(
        "repl_auto_position",
        "MYDB_REPL_AUTO_POSITION",
        OptionKind::Bool
    ),
    spec!(
        "repl_connect_retry_ms",
        "MYDB_REPL_CONNECT_RETRY_MS",
        OptionKind::U64
    ),
    spec!(
        "repl_relay_sync_txns",
        "MYDB_REPL_RELAY_SYNC_TXNS",
        OptionKind::U64Range(1, 1_000_000)
    ),
    spec!(
        "repl_relay_sync_interval_ms",
        "MYDB_REPL_RELAY_SYNC_INTERVAL_MS",
        OptionKind::U64Range(1, 60_000)
    ),
    spec!(
        "repl_progress_sync_txns",
        "MYDB_REPL_PROGRESS_SYNC_TXNS",
        OptionKind::U64Range(1, 1_000_000)
    ),
    spec!(
        "repl_progress_sync_interval_ms",
        "MYDB_REPL_PROGRESS_SYNC_INTERVAL_MS",
        OptionKind::U64Range(1, 60_000)
    ),
    spec!(
        "repl_apply_batch_txns",
        "MYDB_REPL_APPLY_BATCH_TXNS",
        OptionKind::U64Range(1, 1_000_000)
    ),
    spec!(
        "repl_apply_batch_bytes",
        "MYDB_REPL_APPLY_BATCH_BYTES",
        OptionKind::BytesRange(1, 1_099_511_627_776)
    ),
    spec!("repl_do_db", "MYDB_REPL_DO_DB", OptionKind::String),
    spec!("repl_ignore_db", "MYDB_REPL_IGNORE_DB", OptionKind::String),
    spec!("repl_channel", "MYDB_REPL_CHANNEL", OptionKind::String),
    spec!(
        "repl_ssl_mode",
        "MYDB_REPL_SSL_MODE",
        OptionKind::Enum(&[
            "DISABLED",
            "PREFERRED",
            "REQUIRED",
            "VERIFY_CA",
            "VERIFY_IDENTITY"
        ])
    ),
    spec!("repl_ssl_ca", "MYDB_REPL_SSL_CA", OptionKind::NonEmpty),
    spec!("repl_ssl_cert", "MYDB_REPL_SSL_CERT", OptionKind::NonEmpty),
    OptionSpec {
        name: "repl_ssl_key",
        env: "MYDB_REPL_SSL_KEY",
        kind: OptionKind::NonEmpty,
        aliases: &[],
        cli: false,
        sensitive: true,
    },
    spec!(
        "repl_ssl_cipher",
        "MYDB_REPL_SSL_CIPHER",
        OptionKind::String
    ),
    spec!("repl_auto_start", "MYDB_REPL_AUTO_START", OptionKind::Bool),
    spec!(
        "repl_require_secure_transport",
        "MYDB_REPL_REQUIRE_SECURE_TRANSPORT",
        OptionKind::Bool
    ),
    spec!(
        "repl_mode",
        "MYDB_REPL_MODE",
        OptionKind::Enum(&[
            "async",
            "asynchronous",
            "semi_sync",
            "semisync",
            "semi-sync"
        ])
    ),
    spec!(
        "role",
        "MYDB_ROLE",
        OptionKind::Enum(&[
            "standalone",
            "auto",
            "master",
            "primary",
            "replica",
            "secondary",
            "slave"
        ])
    ),
    spec!(
        "sequence_sync_policy",
        "MYDB_SEQUENCE_SYNC_POLICY",
        OptionKind::Enum(&["local", "remote_apply"])
    ),
    spec!(
        "sequence_sync_replicas",
        "MYDB_SEQUENCE_SYNC_REPLICAS",
        OptionKind::NonEmpty
    ),
    spec!(
        "sequence_wal_batch",
        "MYDB_SEQUENCE_WAL_BATCH",
        OptionKind::U64Range(1, i64::MAX as u64)
    ),
    spec!(
        "repl_bind_host",
        "MYDB_REPL_BIND_HOST",
        OptionKind::NonEmpty
    ),
    spec!(
        "repl_master_host",
        "MYDB_REPL_MASTER_HOST",
        OptionKind::NonEmpty
    ),
    spec!("repl_master_port", "MYDB_REPL_MASTER_PORT", OptionKind::U16),
    spec!("repl_port", "MYDB_REPL_PORT", OptionKind::U16),
    spec!(
        "repl_semi_sync_timeout_ms",
        "MYDB_REPL_SEMI_SYNC_TIMEOUT_MS",
        OptionKind::U64
    ),
    spec!(
        "repl_semi_sync_wait_count",
        "MYDB_REPL_SEMI_SYNC_WAIT_COUNT",
        OptionKind::U64
    ),
    spec!(
        "repl_heartbeat_ms",
        "MYDB_REPL_HEARTBEAT_MS",
        OptionKind::U64
    ),
    spec!(
        "repl_reconnect_ms",
        "MYDB_REPL_RECONNECT_MS",
        OptionKind::U64
    ),
    spec!(
        "repl_chunk_bytes",
        "MYDB_REPL_CHUNK_BYTES",
        OptionKind::Bytes
    ),
    spec!(
        "repl_slot_timeout_ms",
        "MYDB_REPL_SLOT_TIMEOUT_MS",
        OptionKind::U64
    ),
    spec!(
        "repl_max_inflight_bytes",
        "MYDB_REPL_MAX_INFLIGHT_BYTES",
        OptionKind::Bytes
    ),
    spec!(
        "repl_max_retained_bytes",
        "MYDB_REPL_MAX_RETAINED_BYTES",
        OptionKind::Bytes
    ),
    spec!(
        "repl_tls_mode",
        "MYDB_REPL_TLS_MODE",
        OptionKind::Enum(&["auto", "disabled", "verify_identity"])
    ),
    spec!("repl_tls_ca", "MYDB_REPL_TLS_CA", OptionKind::NonEmpty),
    spec!("repl_tls_cert", "MYDB_REPL_TLS_CERT", OptionKind::NonEmpty),
    OptionSpec {
        name: "repl_tls_key",
        env: "MYDB_REPL_TLS_KEY",
        kind: OptionKind::NonEmpty,
        aliases: &[],
        cli: false,
        sensitive: true,
    },
    spec!(
        "repl_tls_server_name",
        "MYDB_REPL_TLS_SERVER_NAME",
        OptionKind::NonEmpty
    ),
    spec!(
        "repl_max_connections",
        "MYDB_REPL_MAX_CONNECTIONS",
        OptionKind::U64
    ),
    spec!(
        "repl_max_base_backups",
        "MYDB_REPL_MAX_BASE_BACKUPS",
        OptionKind::U64
    ),
    spec!(
        "repl_apply_queue_bytes",
        "MYDB_REPL_APPLY_QUEUE_BYTES",
        OptionKind::Bytes
    ),
    spec!(
        "repl_base_backup_disk_reserve_pct",
        "MYDB_REPL_BASE_BACKUP_DISK_RESERVE_PCT",
        OptionKind::U64
    ),
    OptionSpec {
        name: "backup_repository",
        env: "MYDB_BACKUP_REPOSITORY",
        kind: OptionKind::NonEmpty,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "base",
        env: "MYDB_BACKUP_BASE",
        kind: OptionKind::NonEmpty,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    OptionSpec {
        name: "runtime_dir",
        env: "MYDB_BACKUP_RUNTIME_DIR",
        kind: OptionKind::NonEmpty,
        aliases: &[],
        cli: true,
        sensitive: false,
    },
    spec!(
        "backup_workers",
        "MYDB_BACKUP_WORKERS",
        OptionKind::U64Range(1, 8)
    ),
    spec!(
        "backup_queue_capacity",
        "MYDB_BACKUP_QUEUE_CAPACITY",
        OptionKind::U64Range(1, 256)
    ),
    spec!(
        "backup_copy_threads",
        "MYDB_BACKUP_COPY_THREADS",
        OptionKind::U64Range(1, 64)
    ),
    spec!(
        "backup_open_files",
        "MYDB_BACKUP_OPEN_FILES",
        OptionKind::U64Range(16, 4096)
    ),
    spec!(
        "backup_bandwidth_bytes_per_sec",
        "MYDB_BACKUP_BANDWIDTH_BYTES_PER_SEC",
        OptionKind::U64
    ),
    spec!(
        "backup_chunk_memory_bytes",
        "MYDB_BACKUP_CHUNK_MEMORY_BYTES",
        OptionKind::U64Range(8 << 20, 1 << 30)
    ),
    spec!(
        "backup_spool_limit_bytes",
        "MYDB_BACKUP_SPOOL_LIMIT_BYTES",
        OptionKind::U64Range(4 << 20, u64::MAX)
    ),
    spec!(
        "backup_foreground_commit_target_us",
        "MYDB_BACKUP_FOREGROUND_COMMIT_TARGET_US",
        OptionKind::U64Range(0, 60_000_000)
    ),
    spec!(
        "backup_freeze_budget_ms",
        "MYDB_BACKUP_FREEZE_BUDGET_MS",
        OptionKind::U64Range(1, 30000)
    ),
    OptionSpec {
        name: "backup_key_file",
        env: "MYDB_BACKUP_KEY_FILE",
        kind: OptionKind::NonEmpty,
        aliases: &[],
        cli: false,
        sensitive: true,
    },
];

static STARTUP_OVERRIDES: OnceLock<BTreeMap<String, String>> = OnceLock::new();

/// Read a stable startup option without mutating the OS environment.
pub fn var(key: &str) -> Result<String, std::env::VarError> {
    if let Some(value) = STARTUP_OVERRIDES.get().and_then(|map| map.get(key)) {
        return Ok(value.clone());
    }
    std::env::var(key)
}

pub fn var_os(key: &str) -> Option<std::ffi::OsString> {
    if let Some(value) = STARTUP_OVERRIDES.get().and_then(|map| map.get(key)) {
        return Some(value.clone().into());
    }
    std::env::var_os(key)
}

/// Parse CLI + the optional file, validate all supported options, and install
/// the immutable startup overlay. Must run before any worker thread starts.
pub fn bootstrap(args: &[String]) -> Result<StartupConfig, ConfigError> {
    let (config_path, help) = scan_config_path(args)?;
    let entries = match &config_path {
        Some(path) => read_option_file(path)?,
        None => Vec::new(),
    };
    let env = snapshot_environment()?;
    let mut resolved = resolve_fixed_entries(&entries, &env).map_err(|err| match &config_path {
        Some(path) => ConfigError::new(format!("{}: {err}", path.display())),
        None => err,
    })?;
    apply_cli(args, &mut resolved)?;
    validate_cross_options(&resolved, &env)?;

    STARTUP_OVERRIDES
        .set(resolved.clone())
        .map_err(|_| ConfigError::new("startup configuration was initialized more than once"))?;

    let (sysvars, warnings) =
        resolve_sysvar_entries(&entries, &resolved, &env, config_path.as_deref())?;
    Ok(StartupConfig {
        config_path,
        warnings,
        sysvars,
        help,
    })
}

fn snapshot_environment() -> Result<BTreeMap<String, String>, ConfigError> {
    let mut env = BTreeMap::new();
    for spec in OPTION_SPECS {
        let Some(raw) = std::env::var_os(spec.env) else {
            continue;
        };
        let value = raw.into_string().map_err(|_| {
            ConfigError::new(format!(
                "environment variable {} is not valid UTF-8",
                spec.env
            ))
        })?;
        env.insert(spec.env.to_string(), value);
    }
    Ok(env)
}

fn scan_config_path(args: &[String]) -> Result<(Option<PathBuf>, bool), ConfigError> {
    let mut path = None;
    let mut help = false;
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "--help" || arg == "-h" {
            help = true;
        } else if arg == "--config" {
            let value = args
                .get(i + 1)
                .ok_or_else(|| ConfigError::new("--config requires a file path"))?;
            if value.starts_with("--") {
                return Err(ConfigError::new("--config requires a file path"));
            }
            set_config_path(&mut path, value)?;
            i += 1;
        } else if let Some(value) = arg.strip_prefix("--config=") {
            set_config_path(&mut path, value)?;
        }
        i += 1;
    }
    Ok((path, help))
}

fn set_config_path(target: &mut Option<PathBuf>, raw: &str) -> Result<(), ConfigError> {
    if target.is_some() {
        return Err(ConfigError::new("--config may be specified only once"));
    }
    if raw.is_empty() {
        return Err(ConfigError::new("--config requires a non-empty file path"));
    }
    *target = Some(PathBuf::from(raw));
    Ok(())
}

fn read_option_file(path: &Path) -> Result<Vec<Entry>, ConfigError> {
    let metadata = fs::metadata(path).map_err(|err| {
        ConfigError::new(format!(
            "cannot access configuration file {}: {err}",
            path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(ConfigError::new(format!(
            "configuration path {} is not a regular file",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o022 != 0 {
            return Err(ConfigError::new(format!(
                "configuration file {} is writable by group or others",
                path.display()
            )));
        }
    }
    let bytes = fs::read(path).map_err(|err| {
        ConfigError::new(format!(
            "cannot read configuration file {}: {err}",
            path.display()
        ))
    })?;
    parse_option_file(&bytes, path)
}

fn parse_option_file(bytes: &[u8], path: &Path) -> Result<Vec<Entry>, ConfigError> {
    let text = std::str::from_utf8(bytes).map_err(|err| {
        ConfigError::new(format!(
            "{} is not valid UTF-8 at byte {}",
            path.display(),
            err.valid_up_to()
        ))
    })?;
    let mut group = None;
    let mut entries = Vec::new();
    for (idx, raw_line) in text.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.starts_with('!') {
            return Err(at(path, line_no, "include directives are not supported"));
        }
        if line.starts_with('[') {
            if !line.ends_with(']') {
                return Err(at(path, line_no, "unterminated option group"));
            }
            let name = line[1..line.len() - 1].trim().to_ascii_lowercase();
            if name.is_empty() {
                return Err(at(path, line_no, "empty option group"));
            }
            group = Some(match name.as_str() {
                "mysqld" => Group::Mysqld,
                "mydb" => Group::Mydb,
                _ => Group::Ignored,
            });
            continue;
        }
        let Some(current_group) = group else {
            return Err(at(path, line_no, "option appears before any group"));
        };
        if current_group == Group::Ignored {
            continue;
        }
        let (raw_key, raw_value) = split_assignment(line)?;
        let key = normalize_key(raw_key);
        if key.is_empty() {
            return Err(at(path, line_no, "empty option name"));
        }
        let value = parse_value(raw_value).map_err(|message| at(path, line_no, message))?;
        entries.push(Entry {
            group: current_group,
            key,
            value,
            line: line_no,
        });
    }
    Ok(entries)
}

fn split_assignment(line: &str) -> Result<(&str, &str), ConfigError> {
    if let Some(eq) = line.find('=') {
        Ok((line[..eq].trim(), line[eq + 1..].trim()))
    } else {
        Ok((line.trim(), "ON"))
    }
}

fn parse_value(raw: &str) -> Result<String, &'static str> {
    let mut out = String::new();
    let mut quote = None;
    let mut escaped = false;
    for ch in raw.chars() {
        if escaped {
            match ch {
                'b' => out.push('\u{0008}'),
                't' => out.push('\t'),
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                's' => out.push(' '),
                '\\' | '\'' | '"' => out.push(ch),
                other => {
                    out.push('\\');
                    out.push(other);
                }
            }
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        match quote {
            Some(q) if ch == q => quote = None,
            Some(_) => out.push(ch),
            None if ch == '\'' || ch == '"' => quote = Some(ch),
            None if ch == '#' || ch == ';' => break,
            None => out.push(ch),
        }
    }
    if escaped {
        out.push('\\');
    }
    if quote.is_some() {
        return Err("unterminated quoted value");
    }
    Ok(out.trim().to_string())
}

fn resolve_fixed_entries(
    entries: &[Entry],
    env: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, ConfigError> {
    let mut file_values: HashMap<&'static str, (&Entry, String)> = HashMap::new();
    for wanted_group in [Group::Mysqld, Group::Mydb] {
        for entry in entries.iter().filter(|entry| entry.group == wanted_group) {
            let Some((spec, inverted)) = find_spec(&entry.key) else {
                continue;
            };
            let raw = if inverted {
                invert_bool(&entry.value).ok_or_else(|| {
                    ConfigError::new(format!("invalid boolean value for {}", spec.name))
                })?
            } else {
                entry.value.clone()
            };
            let value = validate_spec(spec, &raw)
                .map_err(|message| ConfigError::new(format!("line {}: {message}", entry.line)))?;
            file_values.insert(spec.env, (entry, value));
        }
    }
    let mut resolved = BTreeMap::new();
    for (env_name, (_, value)) in file_values {
        if !env.contains_key(env_name) {
            resolved.insert(env_name.to_string(), value);
        }
    }
    // Existing environment values are higher priority and are validated with
    // the same rules as file/CLI values, but they are not copied to the overlay.
    for spec in OPTION_SPECS {
        if let Some(raw) = env.get(spec.env) {
            validate_spec(spec, raw).map_err(ConfigError::new)?;
        }
    }
    Ok(resolved)
}

fn apply_cli(args: &[String], resolved: &mut BTreeMap<String, String>) -> Result<(), ConfigError> {
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "--help" || arg == "-h" {
            i += 1;
            continue;
        }
        if arg == "--config" {
            i += 2;
            continue;
        }
        if arg.starts_with("--config=") {
            i += 1;
            continue;
        }
        let Some(long) = arg.strip_prefix("--") else {
            return Err(ConfigError::new(format!(
                "unexpected positional argument: {arg}"
            )));
        };
        let (key_raw, inline_value) = match long.split_once('=') {
            Some((key, value)) => (key, Some(value)),
            None => (long, None),
        };
        let key = normalize_key(key_raw);
        let Some((spec, inverted)) = find_spec(&key).filter(|(spec, _)| spec.cli) else {
            return Err(ConfigError::new(format!("unknown argument: --{key_raw}")));
        };
        let raw = if matches!(spec.kind, OptionKind::Bool) && inline_value.is_none() {
            "ON".to_string()
        } else if let Some(value) = inline_value {
            value.to_string()
        } else {
            let value = args
                .get(i + 1)
                .ok_or_else(|| ConfigError::new(format!("--{} requires a value", key_raw)))?;
            if value.starts_with("--") {
                return Err(ConfigError::new(format!("--{} requires a value", key_raw)));
            }
            i += 1;
            value.clone()
        };
        let raw = if inverted {
            invert_bool(&raw)
                .ok_or_else(|| ConfigError::new(format!("invalid boolean value for --{key_raw}")))?
        } else {
            raw
        };
        let value = validate_spec(spec, &raw).map_err(ConfigError::new)?;
        resolved.insert(spec.env.to_string(), value);
        i += 1;
    }
    Ok(())
}

fn validate_cross_options(
    resolved: &BTreeMap<String, String>,
    env: &BTreeMap<String, String>,
) -> Result<(), ConfigError> {
    let effective = |name: &str| resolved.get(name).or_else(|| env.get(name));
    let cert = effective("MYDB_SSL_CERT");
    let key = effective("MYDB_SSL_KEY");
    if cert.is_some() != key.is_some() {
        return Err(ConfigError::new(
            "ssl_cert and ssl_key must be configured together",
        ));
    }
    let disabled = effective("MYDB_SSL_MODE")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "disabled" | "disable" | "off" | "0" | "false"
            )
        })
        .unwrap_or(false);
    if disabled && cert.is_some() {
        return Err(ConfigError::new(
            "ssl_mode=disabled cannot be combined with ssl_cert/ssl_key",
        ));
    }
    let repl_ca = effective("MYDB_REPL_TLS_CA");
    let repl_cert = effective("MYDB_REPL_TLS_CERT");
    let repl_key = effective("MYDB_REPL_TLS_KEY");
    let repl_count = usize::from(repl_ca.is_some())
        + usize::from(repl_cert.is_some())
        + usize::from(repl_key.is_some());
    if repl_count != 0 && repl_count != 3 {
        return Err(ConfigError::new(
            "repl_tls_ca, repl_tls_cert and repl_tls_key must be configured together",
        ));
    }
    Ok(())
}

fn resolve_sysvar_entries(
    entries: &[Entry],
    fixed: &BTreeMap<String, String>,
    env: &BTreeMap<String, String>,
    path: Option<&Path>,
) -> Result<(Vec<StartupSysVarAssignment>, Vec<String>), ConfigError> {
    let registry = crate::engine::sysvar::registry();
    let mut selected: HashMap<String, (&Entry, SysVarValue)> = HashMap::new();
    let mut warnings = Vec::new();
    for wanted_group in [Group::Mysqld, Group::Mydb] {
        for entry in entries.iter().filter(|entry| entry.group == wanted_group) {
            if find_spec(&entry.key).is_some() {
                continue;
            }
            // A registered `mydb_foo` variable may be written either by its
            // formal sysvar name or by the option-file spelling obtained from
            // `MYDB_FOO` (`foo`).  The registry lookup is the whitelist: this
            // does not expose arbitrary MYDB_* debug/fault environment knobs.
            let prefixed_name = format!("mydb_{}", entry.key);
            let Some(var) = registry
                .get(&entry.key)
                .or_else(|| registry.get(&prefixed_name))
            else {
                let location = path
                    .map(|p| format!("{}:{}", p.display(), entry.line))
                    .unwrap_or_else(|| format!("line {}", entry.line));
                if entry.group == Group::Mydb {
                    return Err(ConfigError::new(format!(
                        "{location}: unknown option '{}' in [mydb]",
                        entry.key
                    )));
                }
                warnings.push(format!(
                    "{location}: ignoring unsupported [mysqld] option '{}'",
                    entry.key
                ));
                continue;
            };
            if !var.def.scope.allows_global() || var.write.is_none() {
                let location = path
                    .map(|p| format!("{}:{}", p.display(), entry.line))
                    .unwrap_or_else(|| format!("line {}", entry.line));
                let message = format!(
                    "option '{}' cannot be assigned at server startup",
                    entry.key
                );
                if entry.group == Group::Mydb {
                    return Err(ConfigError::new(format!("{location}: {message}")));
                }
                warnings.push(format!("{location}: ignoring {message}"));
                continue;
            }
            let raw = normalize_sysvar_bool(&var.def.data_type, &entry.value);
            let coerced = var
                .def
                .data_type
                .coerce(var.def.name, SysVarValue::String(raw))
                .map_err(|err| {
                    let value = if is_sensitive(&entry.key) {
                        "<redacted>"
                    } else {
                        &entry.value
                    };
                    ConfigError::new(format!(
                        "line {}: invalid value {value:?} for '{}': {}",
                        entry.line,
                        entry.key,
                        err.message()
                    ))
                })?;
            selected.insert(var.def.name.to_string(), (entry, coerced));
        }
    }
    let mut assignments = Vec::new();
    for (name, (_entry, value)) in selected {
        if let Some(env_name) = sysvar_env_name(&name) {
            if env.contains_key(&env_name)
                || fixed.contains_key(&env_name)
                || std::env::var_os(&env_name).is_some()
            {
                continue;
            }
        }
        assignments.push(StartupSysVarAssignment { name, value });
    }
    assignments.sort_by(|a, b| a.name.cmp(&b.name));
    Ok((assignments, warnings))
}

fn find_spec(key: &str) -> Option<(&'static OptionSpec, bool)> {
    let normalized = normalize_key(key);
    for spec in OPTION_SPECS {
        if normalized == spec.name || spec.aliases.iter().any(|alias| normalized == *alias) {
            return Some((spec, false));
        }
        if matches!(spec.kind, OptionKind::Bool)
            && normalized.strip_prefix("skip_") == Some(spec.name)
        {
            return Some((spec, true));
        }
    }
    None
}

fn validate_spec(spec: &OptionSpec, raw: &str) -> Result<String, String> {
    let invalid = |detail: &str| {
        let shown = if spec.sensitive { "<redacted>" } else { raw };
        format!("invalid value {shown:?} for '{}': {detail}", spec.name)
    };
    match spec.kind {
        OptionKind::Bool => parse_bool(raw)
            .map(|value| if value { "ON" } else { "OFF" }.to_string())
            .ok_or_else(|| invalid("expected ON/OFF, TRUE/FALSE, YES/NO, or 1/0")),
        OptionKind::U16 => raw
            .trim()
            .parse::<u16>()
            .map(|value| value.to_string())
            .map_err(|_| invalid("expected an unsigned 16-bit integer")),
        OptionKind::U32 => raw
            .trim()
            .parse::<u32>()
            .map(|value| value.to_string())
            .map_err(|_| invalid("expected an unsigned 32-bit integer")),
        OptionKind::U64 => raw
            .trim()
            .parse::<u64>()
            .map(|value| value.to_string())
            .map_err(|_| invalid("expected an unsigned integer")),
        OptionKind::U64Range(min, max) => raw
            .trim()
            .parse::<u64>()
            .map_err(|_| invalid("expected an unsigned integer"))
            .and_then(|value| {
                if (min..=max).contains(&value) {
                    Ok(value.to_string())
                } else {
                    Err(invalid(&format!("expected a value in {min}..={max}")))
                }
            }),
        OptionKind::Bytes => parse_scaled_u64(raw, 1)
            .map(|value| value.to_string())
            .ok_or_else(|| invalid("expected bytes with optional K/M/G/T suffix")),
        OptionKind::BytesRange(min, max) => parse_scaled_u64(raw, 1)
            .ok_or_else(|| invalid("expected bytes with optional K/M/G/T suffix"))
            .and_then(|value| {
                if (min..=max).contains(&value) {
                    Ok(value.to_string())
                } else {
                    Err(invalid(&format!("expected a byte size in {min}..={max}")))
                }
            }),
        OptionKind::Mebibytes => parse_scaled_u64(raw, 1024 * 1024)
            .filter(|bytes| bytes % (1024 * 1024) == 0)
            .map(|bytes| (bytes / (1024 * 1024)).to_string())
            .ok_or_else(|| invalid("expected a whole number of MiB or a K/M/G/T size")),
        OptionKind::NonNegativeFloat => raw
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite() && *value >= 0.0)
            .map(|_| raw.trim().to_string())
            .ok_or_else(|| invalid("expected a non-negative finite number")),
        OptionKind::LogOutput => {
            let mut file = false;
            let mut table = false;
            let items = raw
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .collect::<Vec<_>>();
            if items.len() == 1 && items[0].eq_ignore_ascii_case("NONE") {
                return Ok("NONE".to_string());
            }
            for item in items {
                if item.eq_ignore_ascii_case("FILE") {
                    file = true;
                } else if item.eq_ignore_ascii_case("TABLE") {
                    table = true;
                } else {
                    return Err(invalid("expected FILE, TABLE, FILE,TABLE, or NONE"));
                }
            }
            match (file, table) {
                (true, true) => Ok("FILE,TABLE".to_string()),
                (true, false) => Ok("FILE".to_string()),
                (false, true) => Ok("TABLE".to_string()),
                (false, false) => Err(invalid("value must not be empty")),
            }
        }
        OptionKind::IpAddr => raw
            .trim()
            .parse::<IpAddr>()
            .map(|value| value.to_string())
            .map_err(|_| invalid("expected an IP address")),
        OptionKind::String => Ok(raw.to_string()),
        OptionKind::NonEmpty if raw.is_empty() => Err(invalid("value must not be empty")),
        OptionKind::NonEmpty => Ok(raw.to_string()),
        OptionKind::Enum(values) => values
            .iter()
            .find(|candidate| candidate.eq_ignore_ascii_case(raw.trim()))
            .map(|value| (*value).to_string())
            .ok_or_else(|| invalid(&format!("expected one of {}", values.join(", ")))),
        OptionKind::Removed(message) => {
            Err(format!("option '{}' is invalid: {message}", spec.name))
        }
    }
}

pub(crate) fn parse_scaled_u64(raw: &str, bare_unit: u64) -> Option<u64> {
    let trimmed = raw.trim();
    let (number, multiplier) = match trimmed.as_bytes().last().copied() {
        Some(b'k' | b'K') => (&trimmed[..trimmed.len() - 1], 1024_u64),
        Some(b'm' | b'M') => (&trimmed[..trimmed.len() - 1], 1024_u64.pow(2)),
        Some(b'g' | b'G') => (&trimmed[..trimmed.len() - 1], 1024_u64.pow(3)),
        Some(b't' | b'T') => (&trimmed[..trimmed.len() - 1], 1024_u64.pow(4)),
        _ => (trimmed, bare_unit),
    };
    number.trim().parse::<u64>().ok()?.checked_mul(multiplier)
}

fn parse_bool(raw: &str) -> Option<bool> {
    crate::util::env::parse_bool(raw)
}

fn invert_bool(raw: &str) -> Option<String> {
    parse_bool(raw).map(|value| if value { "OFF" } else { "ON" }.to_string())
}

fn normalize_sysvar_bool(data_type: &DataType, raw: &str) -> String {
    if matches!(data_type, DataType::Bool) {
        if let Some(value) = parse_bool(raw) {
            return if value { "ON" } else { "OFF" }.to_string();
        }
    }
    raw.to_string()
}

fn sysvar_env_name(name: &str) -> Option<String> {
    let suffix = name.strip_prefix("mydb_")?;
    Some(format!(
        "MYDB_{}",
        suffix.replace('.', "_").to_ascii_uppercase()
    ))
}

fn is_sensitive(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.contains("password") || name.contains("secret") || name.ends_with("_key")
}

fn normalize_key(raw: &str) -> String {
    raw.trim()
        .chars()
        .map(|ch| match ch {
            '-' => '_',
            other => other.to_ascii_lowercase(),
        })
        .collect()
}

fn at(path: &Path, line: usize, message: impl fmt::Display) -> ConfigError {
    ConfigError::new(format!("{}:{line}: {message}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &[u8]) -> Result<Vec<Entry>, ConfigError> {
        parse_option_file(text, Path::new("test.cnf"))
    }

    #[test]
    fn parses_groups_quotes_comments_and_escapes() {
        let entries = parse(
            br#"
                [mysqld]
                port = 3307 # comment
                general-log
                log-error = "logs/my # error.log"
                [client]
                password=ignored
                [mydb]
                data-dir = 'var\sdata'
            "#,
        )
        .unwrap();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].key, "port");
        assert_eq!(entries[0].value, "3307");
        assert_eq!(entries[1].value, "ON");
        assert_eq!(entries[2].value, "logs/my # error.log");
        assert_eq!(entries[3].key, "data_dir");
        assert_eq!(entries[3].value, "var data");
    }

    #[test]
    fn wal_write_buffer_config_accepts_alias_and_validates_size() {
        for name in ["wal_write_buffer_bytes", "mydb_wal_write_buffer_bytes"] {
            let entries = parse(format!("[mydb]\n{name}=2M\n").as_bytes()).unwrap();
            let resolved = resolve_fixed_entries(&entries, &BTreeMap::new()).unwrap();
            assert_eq!(
                resolved
                    .get("MYDB_WAL_WRITE_BUFFER_BYTES")
                    .map(String::as_str),
                Some("2097152")
            );
            let env =
                BTreeMap::from([("MYDB_WAL_WRITE_BUFFER_BYTES".to_owned(), "4096".to_owned())]);
            assert!(!resolve_fixed_entries(&entries, &env)
                .unwrap()
                .contains_key("MYDB_WAL_WRITE_BUFFER_BYTES"));
        }
        for raw in ["-1", "2G", "abc"] {
            let entries =
                parse(format!("[mydb]\nwal_write_buffer_bytes={raw}\n").as_bytes()).unwrap();
            assert!(resolve_fixed_entries(&entries, &BTreeMap::new()).is_err());
        }
    }

    #[test]
    fn rejects_invalid_utf8_and_include() {
        assert!(parse(&[0xff]).unwrap_err().to_string().contains("UTF-8"));
        let err = parse(b"[mydb]\n!include other.cnf\n").unwrap_err();
        assert!(err.to_string().contains("include directives"));
    }

    #[test]
    fn mydb_group_wins_regardless_of_textual_order() {
        let entries = parse(b"[mydb]\nport=4406\n[mysqld]\nport=3307\n").unwrap();
        let resolved = resolve_fixed_entries(&entries, &BTreeMap::new()).unwrap();
        assert_eq!(resolved.get("MYDB_PORT").map(String::as_str), Some("4406"));
    }

    #[test]
    fn environment_wins_over_file() {
        let entries = parse(b"[mydb]\nport=4406\n").unwrap();
        let env = BTreeMap::from([("MYDB_PORT".to_string(), "5506".to_string())]);
        let resolved = resolve_fixed_entries(&entries, &env).unwrap();
        assert!(!resolved.contains_key("MYDB_PORT"));
    }

    #[test]
    fn spill_governance_options_validate_and_environment_wins() {
        let entries = parse(
            b"[mydb]\ntemp_disk_limit_bytes=1048576\ntemp_file_limit_per_query_bytes=65536\ntemp_disk_reserve_pct=25\nwork_area_oversubscribe_pct=150\n",
        )
        .unwrap();
        let resolved = resolve_fixed_entries(&entries, &BTreeMap::new()).unwrap();
        assert_eq!(
            resolved.get("MYDB_TEMP_DISK_LIMIT").map(String::as_str),
            Some("1048576")
        );
        assert_eq!(
            resolved
                .get("MYDB_TEMP_FILE_LIMIT_PER_QUERY")
                .map(String::as_str),
            Some("65536")
        );
        assert_eq!(
            resolved
                .get("MYDB_TEMP_DISK_RESERVE_PCT")
                .map(String::as_str),
            Some("25")
        );
        assert_eq!(
            resolved
                .get("MYDB_WORK_AREA_OVERSUBSCRIBE_PCT")
                .map(String::as_str),
            Some("150")
        );

        let env = BTreeMap::from([("MYDB_TEMP_DISK_LIMIT".to_string(), "2048".to_string())]);
        let resolved = resolve_fixed_entries(&entries, &env).unwrap();
        assert!(!resolved.contains_key("MYDB_TEMP_DISK_LIMIT"));

        let bad = parse(b"[mydb]\ntemp_disk_reserve_pct=91\n").unwrap();
        assert!(resolve_fixed_entries(&bad, &BTreeMap::new()).is_err());
    }

    #[test]
    fn cli_wins_and_missing_values_are_rejected() {
        let mut resolved = BTreeMap::from([("MYDB_PORT".to_string(), "3307".to_string())]);
        apply_cli(&["--port=4406".to_string()], &mut resolved).unwrap();
        assert_eq!(resolved.get("MYDB_PORT").map(String::as_str), Some("4406"));

        let err = apply_cli(
            &["--port".to_string(), "--host".to_string()],
            &mut BTreeMap::new(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("--port requires a value"));
        assert!(scan_config_path(&["--config".into(), "--port".into()]).is_err());
    }

    #[test]
    fn zone_map_global_switch_configuration() {
        let entries =
            parse(b"[mydb]\nmydb_zone_map_enabled=off\n[mysqld]\nzone_map_enabled=on\n").unwrap();
        let resolved = resolve_fixed_entries(&entries, &BTreeMap::new()).unwrap();
        assert_eq!(resolved["MYDB_ZONE_MAP_ENABLED"], "OFF");
        let env = BTreeMap::from([("MYDB_ZONE_MAP_ENABLED".into(), "yes".into())]);
        assert!(!resolve_fixed_entries(&entries, &env)
            .unwrap()
            .contains_key("MYDB_ZONE_MAP_ENABLED"));
        let env = BTreeMap::from([("MYDB_ZONE_MAP_ENABLED".into(), "invalid".into())]);
        assert!(resolve_fixed_entries(&entries, &env).is_err());
        let invalid = parse(b"[mydb]\nzone_map_enabled=invalid\n").unwrap();
        assert!(resolve_fixed_entries(&invalid, &BTreeMap::new()).is_err());
    }

    #[test]
    fn size_and_boolean_values_are_canonicalized() {
        let max_packet = find_spec("max-allowed-packet").unwrap().0;
        assert_eq!(validate_spec(max_packet, "128M").unwrap(), "134217728");
        let general = find_spec("general_log").unwrap().0;
        assert_eq!(validate_spec(general, "yes").unwrap(), "ON");
        let plan_cache = find_spec("plan_cache").unwrap().0;
        assert_eq!(validate_spec(plan_cache, "off").unwrap(), "OFF");
        assert!(validate_spec(plan_cache, "maybe").is_err());
        assert_eq!(invert_bool("yes").as_deref(), Some("OFF"));

        let redo = find_spec("redo_enabled").unwrap().0;
        assert_eq!(redo.env, "MYDB_REDO_ENABLED");
        assert_eq!(validate_spec(redo, "off").unwrap(), "OFF");
        assert!(validate_spec(redo, "unsafe").is_err());
    }

    #[test]
    fn removed_decoded_cache_options_are_fatal() {
        for option in [
            "buffer_pool_decoded_cache=auto",
            "buffer_pool_decoded_cache_max_pct=25",
        ] {
            let entries = parse(format!("[mydb]\n{option}\n").as_bytes()).unwrap();
            let err = resolve_fixed_entries(&entries, &BTreeMap::new()).unwrap_err();
            assert!(err.to_string().contains("decoded cache has been removed"));
        }
        for env_name in [
            "MYDB_BUFFER_POOL_DECODED_CACHE",
            "MYDB_BUFFER_POOL_DECODED_CACHE_MAX_PCT",
        ] {
            let env = BTreeMap::from([(env_name.to_string(), "1".to_string())]);
            let err = resolve_fixed_entries(&[], &env).unwrap_err();
            assert!(err.to_string().contains("decoded cache has been removed"));
        }
    }

    #[test]
    fn small_scan_pct_startup_option_accepts_boundaries() {
        for pct in [0, 2, 100] {
            let expected = pct.to_string();
            let entries =
                parse(format!("[mydb]\nbuffer_pool_small_scan_pct={pct}\n").as_bytes()).unwrap();
            let resolved = resolve_fixed_entries(&entries, &BTreeMap::new()).unwrap();
            assert_eq!(
                resolved
                    .get("MYDB_BUFFER_POOL_SMALL_SCAN_PCT")
                    .map(String::as_str),
                Some(expected.as_str())
            );
        }
        let entries = parse(b"[mydb]\nbuffer_pool_small_scan_pct=101\n").unwrap();
        assert!(resolve_fixed_entries(&entries, &BTreeMap::new()).is_err());
    }

    #[test]
    fn file_cache_limit_is_a_stable_startup_option() {
        for limit in [0, 1024] {
            let expected = limit.to_string();
            let entries =
                parse(format!("[mydb]\nbuffer_pool_file_cache_max={limit}\n").as_bytes()).unwrap();
            let resolved = resolve_fixed_entries(&entries, &BTreeMap::new()).unwrap();
            assert_eq!(
                resolved
                    .get("MYDB_BUFFER_POOL_FILE_CACHE_MAX")
                    .map(String::as_str),
                Some(expected.as_str())
            );
        }
    }

    #[test]
    fn hash_join_startup_options_validate_low_memory_bounds() {
        let entries = parse(
            b"[mydb]\n\
              hash_join_mem_limit_mb=0\n\
              hash_join_grace_partitions=256\n\
              hash_join_grace_max_depth=8\n\
              hash_join_parallelism=64\n\
              hash_join_work_area=on\n\
              hash_join_fallback=chunked\n",
        )
        .unwrap();
        let resolved = resolve_fixed_entries(&entries, &BTreeMap::new()).unwrap();
        assert_eq!(resolved["MYDB_HASH_JOIN_MEM_LIMIT_MB"], "0");
        assert_eq!(resolved["MYDB_HASH_JOIN_GRACE_PARTITIONS"], "256");
        assert_eq!(resolved["MYDB_HASH_JOIN_GRACE_MAX_DEPTH"], "8");
        assert_eq!(resolved["MYDB_HASH_JOIN_PARALLELISM"], "64");
        assert_eq!(resolved["MYDB_HASH_JOIN_WORK_AREA"], "ON");

        for invalid in [
            "hash_join_grace_partitions=1",
            "hash_join_grace_partitions=257",
            "hash_join_grace_max_depth=0",
            "hash_join_grace_max_depth=9",
            "hash_join_parallelism=0",
            "hash_join_parallelism=65",
        ] {
            let text = format!("[mydb]\n{invalid}\n");
            let entries = parse(text.as_bytes()).unwrap();
            assert!(
                resolve_fixed_entries(&entries, &BTreeMap::new()).is_err(),
                "invalid option should fail: {invalid}"
            );
        }
    }

    #[test]
    fn errors_include_source_line() {
        let entries = parse(b"[mydb]\nport=70000\n").unwrap();
        let err = resolve_fixed_entries(&entries, &BTreeMap::new()).unwrap_err();
        assert!(err.to_string().contains("line 2"), "{err}");
    }

    #[test]
    fn mydb_unknown_is_fatal_but_mysqld_unknown_warns() {
        let strict = parse(b"[mydb]\ndebug_magic=1\n").unwrap();
        let err = resolve_sysvar_entries(
            &strict,
            &BTreeMap::new(),
            &BTreeMap::new(),
            Some(Path::new("test.cnf")),
        )
        .unwrap_err();
        assert!(err.to_string().contains("test.cnf:2"));

        let compatible = parse(b"[mysqld]\ninnodb_fake_option=1\n").unwrap();
        let (_, warnings) = resolve_sysvar_entries(
            &compatible,
            &BTreeMap::new(),
            &BTreeMap::new(),
            Some(Path::new("test.cnf")),
        )
        .unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("test.cnf:2"));
    }

    #[test]
    fn accepts_registered_global_sysvar() {
        let entries = parse(b"[mydb]\nread_only=ON\n").unwrap();
        let (assignments, warnings) = resolve_sysvar_entries(
            &entries,
            &BTreeMap::new(),
            &BTreeMap::new(),
            Some(Path::new("test.cnf")),
        )
        .unwrap();
        assert!(warnings.is_empty());
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].name, "read_only");
        assert_eq!(assignments[0].value.to_display_string(), "ON");
    }

    #[test]
    fn init_connect_startup_file_env_and_cli_precedence() {
        let entries = parse(b"[mysqld]\ninit-connect=\"SET @a='x;y'; SELECT @a\"\n[mydb]\ninit_connect='SET @a=2'\n").unwrap();
        let mut env = BTreeMap::new();
        let mut resolved = resolve_fixed_entries(&entries, &env).unwrap();
        assert_eq!(resolved["MYDB_INIT_CONNECT"], "SET @a=2");
        env.insert("MYDB_INIT_CONNECT".into(), "SET @a=3".into());
        resolved = resolve_fixed_entries(&entries, &env).unwrap();
        // Environment values take precedence without being copied to the
        // immutable overlay; util::env reads them from the OS environment.
        assert!(!resolved.contains_key("MYDB_INIT_CONNECT"));
        apply_cli(
            &["--init-connect=SET @a=4; SELECT @a".into()],
            &mut resolved,
        )
        .unwrap();
        assert_eq!(resolved["MYDB_INIT_CONNECT"], "SET @a=4; SELECT @a");
        apply_cli(&["--init_connect".into(), String::new()], &mut resolved).unwrap();
        assert_eq!(resolved["MYDB_INIT_CONNECT"], "");
        assert!(apply_cli(&["--init-connect".into()], &mut resolved).is_err());
    }

    #[test]
    fn accepts_show_gipk_metadata_visibility_sysvar() {
        let name = "show_gipk_in_create_table_and_information_schema";
        for spelling in [name.to_string(), name.replace('_', "-")] {
            let entries = parse(format!("[mydb]\n{spelling}=OFF\n").as_bytes()).unwrap();
            let (assignments, warnings) = resolve_sysvar_entries(
                &entries,
                &BTreeMap::new(),
                &BTreeMap::new(),
                Some(Path::new("test.cnf")),
            )
            .unwrap();
            assert!(warnings.is_empty());
            assert_eq!(assignments.len(), 1);
            assert_eq!(assignments[0].name, name);
            assert_eq!(assignments[0].value, SysVarValue::Bool(false));
        }
    }

    #[test]
    fn sensitive_values_are_redacted() {
        let password = find_spec("root_password").unwrap().0;
        assert_eq!(validate_spec(password, "anything").unwrap(), "anything");
        let key = find_spec("ssl_key").unwrap().0;
        let err = validate_spec(key, "").unwrap_err();
        assert!(err.contains("<redacted>"));
        assert!(!err.contains("\"\""));
    }

    #[test]
    fn tls_pair_is_validated_before_startup() {
        let resolved = BTreeMap::from([("MYDB_SSL_CERT".to_string(), "server.pem".to_string())]);
        let err = validate_cross_options(&resolved, &BTreeMap::new()).unwrap_err();
        assert!(err.to_string().contains("configured together"));
        assert!(!err.to_string().contains("server.pem"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_group_writable_config_file() {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;

        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "[mydb]\nport=3307").unwrap();
        fs::set_permissions(file.path(), fs::Permissions::from_mode(0o660)).unwrap();
        let err = read_option_file(file.path()).unwrap_err();
        assert!(err.to_string().contains("writable by group or others"));
    }
}
