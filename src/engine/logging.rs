use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use chrono::{Local, Utc};
use serde::{Deserialize, Serialize};

use crate::engine::api::{QueryResponse, SessionContext};
use crate::storage::error;
use crate::types::{QueryError, Value};

#[derive(Debug, Clone)]
pub struct QueryLogConfig {
    pub slow_sql_threshold: Duration,
}

impl QueryLogConfig {
    pub const DEFAULT_SLOW_SQL_MS: u64 = 1000;

    pub fn with_slow_sql_threshold_ms(ms: u64) -> Self {
        Self {
            slow_sql_threshold: Duration::from_millis(ms),
        }
    }

    pub fn slow_sql_threshold_ms(&self) -> u128 {
        self.slow_sql_threshold.as_millis()
    }
}

impl Default for QueryLogConfig {
    fn default() -> Self {
        Self::with_slow_sql_threshold_ms(Self::DEFAULT_SLOW_SQL_MS)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogTimestampMode {
    System,
    Utc,
}

impl LogTimestampMode {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_uppercase().as_str() {
            "SYSTEM" | "LOCAL" => Some(Self::System),
            "UTC" => Some(Self::Utc),
            _ => None,
        }
    }

    pub fn as_mysql_value(self) -> &'static str {
        match self {
            Self::System => "SYSTEM",
            Self::Utc => "UTC",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogOutput(u8);

impl LogOutput {
    const FILE: u8 = 1;
    const TABLE: u8 = 2;

    pub const TABLE_ONLY: Self = Self(Self::TABLE);
    pub const NONE: Self = Self(0);

    pub fn parse(value: &str) -> Result<Self, QueryError> {
        let mut bits = 0u8;
        let raw = value.trim();
        if raw.eq_ignore_ascii_case("NONE") {
            return Ok(Self::NONE);
        }
        for item in raw
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
        {
            match item.to_ascii_uppercase().as_str() {
                "FILE" => bits |= Self::FILE,
                "TABLE" => bits |= Self::TABLE,
                "NONE" if bits == 0 => return Ok(Self::NONE),
                other => {
                    return Err(error(
                        crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                        format!("Variable 'log_output' can't be set to the value of '{other}'"),
                    ))
                }
            }
        }
        if bits == 0 {
            return Err(error(
                crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                "Variable 'log_output' can't be set to an empty value",
            ));
        }
        Ok(Self(bits))
    }

    fn has_file(self) -> bool {
        self.0 & Self::FILE != 0
    }

    fn has_table(self) -> bool {
        self.0 & Self::TABLE != 0
    }

    pub fn as_mysql_value(self) -> &'static str {
        match self.0 {
            0 => "NONE",
            Self::FILE => "FILE",
            Self::TABLE => "TABLE",
            _ => "FILE,TABLE",
        }
    }
}

impl Default for LogOutput {
    fn default() -> Self {
        Self::TABLE_ONLY
    }
}

#[derive(Debug, Clone)]
pub struct SqlLogConfig {
    pub general_log: bool,
    pub slow_query_log: bool,
    pub log_output: LogOutput,
    pub general_log_file: PathBuf,
    pub slow_query_log_file: PathBuf,
    pub error_log_file: PathBuf,
    pub long_query_time: Duration,
    pub min_examined_row_limit: u64,
    pub log_slow_admin_statements: bool,
    pub log_queries_not_using_indexes: bool,
    pub log_timestamps: LogTimestampMode,
    pub log_error_verbosity: u8,
}

impl SqlLogConfig {
    pub fn from_env(data_dir: &Path, query_log: &QueryLogConfig) -> Self {
        let logs_dir = logs_dir(data_dir);
        let default_general = logs_dir.join("mydb.log");
        let default_slow = logs_dir.join("mydb-slow.log");
        let default_error = logs_dir.join("mydb.err");
        let general_log = env_bool("MYDB_GENERAL_LOG").unwrap_or(false);
        let slow_query_log = env_bool("MYDB_SLOW_QUERY_LOG").unwrap_or(true);
        let log_output = env_value("MYDB_LOG_OUTPUT")
            .ok()
            .and_then(|raw| LogOutput::parse(&raw).ok())
            .unwrap_or_default();
        let long_query_time = env_value("MYDB_LONG_QUERY_TIME")
            .ok()
            .and_then(|raw| parse_seconds_duration(&raw).ok())
            .unwrap_or(query_log.slow_sql_threshold);
        let min_examined_row_limit = env_value("MYDB_MIN_EXAMINED_ROW_LIMIT")
            .ok()
            .and_then(|raw| raw.parse().ok())
            .unwrap_or(0);
        let log_timestamps = env_value("MYDB_LOG_TIMESTAMPS")
            .ok()
            .and_then(|raw| LogTimestampMode::parse(&raw))
            .unwrap_or(LogTimestampMode::System);
        let log_error_verbosity = env_value("MYDB_LOG_ERROR_VERBOSITY")
            .ok()
            .and_then(|raw| raw.parse().ok())
            .filter(|value| (1..=3).contains(value))
            .unwrap_or(2);

        Self {
            general_log,
            slow_query_log,
            log_output,
            general_log_file: env_value("MYDB_GENERAL_LOG_FILE")
                .ok()
                .map(PathBuf::from)
                .unwrap_or(default_general),
            slow_query_log_file: env_value("MYDB_SLOW_QUERY_LOG_FILE")
                .ok()
                .map(PathBuf::from)
                .unwrap_or(default_slow),
            error_log_file: env_value("MYDB_LOG_ERROR")
                .ok()
                .map(PathBuf::from)
                .unwrap_or(default_error),
            long_query_time,
            min_examined_row_limit,
            log_slow_admin_statements: env_bool("MYDB_LOG_SLOW_ADMIN_STATEMENTS").unwrap_or(false),
            log_queries_not_using_indexes: env_bool("MYDB_LOG_QUERIES_NOT_USING_INDEXES")
                .unwrap_or(false),
            log_timestamps,
            log_error_verbosity,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlushLogKind {
    All,
    General,
    Slow,
    Error,
}

#[derive(Debug)]
pub struct SqlLogService {
    data_dir: PathBuf,
    config: RwLock<SqlLogConfig>,
    files: Mutex<LogFiles>,
    general_on: AtomicBool,
    slow_on: AtomicBool,
    output_bits: AtomicU32,
    long_query_micros: AtomicU64,
}

#[derive(Debug)]
struct LogFiles {
    general: Option<File>,
    slow: Option<File>,
    error: File,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GeneralLogRow {
    event_time: String,
    user_host: String,
    thread_id: u64,
    server_id: u32,
    command_type: String,
    argument: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SlowLogRow {
    start_time: String,
    user_host: String,
    query_time: String,
    lock_time: String,
    rows_sent: u64,
    rows_examined: u64,
    db: String,
    last_insert_id: u64,
    insert_id: u64,
    server_id: u32,
    sql_text: String,
    thread_id: u64,
}

#[derive(Debug, Clone)]
struct ErrorLogEvent {
    logged: String,
    thread_id: Option<u64>,
    prio: String,
    error_code: Option<String>,
    subsystem: String,
    data: String,
}

fn active_service_cell() -> &'static RwLock<Option<Arc<SqlLogService>>> {
    static ACTIVE: OnceLock<RwLock<Option<Arc<SqlLogService>>>> = OnceLock::new();
    ACTIVE.get_or_init(|| RwLock::new(None))
}

fn error_events() -> &'static Mutex<VecDeque<ErrorLogEvent>> {
    static EVENTS: OnceLock<Mutex<VecDeque<ErrorLogEvent>>> = OnceLock::new();
    EVENTS.get_or_init(|| Mutex::new(VecDeque::with_capacity(256)))
}

/// Serializes JSONL table-log readers, appenders and truncation.  System-table
/// scans are lazy and may therefore read `mysql.slow_log` on a worker while
/// the connection thread records that very SELECT.  Holding one process-wide
/// lock prevents a reader from observing serde's multi-write partial object.
/// These diagnostic tables are low-volume and never sit on the data path.
fn table_log_io_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

impl SqlLogService {
    pub fn open(
        data_dir: impl Into<PathBuf>,
        query_log: &QueryLogConfig,
    ) -> Result<Arc<Self>, QueryError> {
        let data_dir = data_dir.into();
        fs::create_dir_all(logs_dir(&data_dir)).map_err(io_err)?;
        let config = SqlLogConfig::from_env(&data_dir, query_log);
        let files = LogFiles::open_initial(&config)?;
        let service = Arc::new(Self {
            data_dir,
            general_on: AtomicBool::new(config.general_log),
            slow_on: AtomicBool::new(config.slow_query_log),
            output_bits: AtomicU32::new(config.log_output.0 as u32),
            long_query_micros: AtomicU64::new(duration_to_micros(config.long_query_time)),
            config: RwLock::new(config),
            files: Mutex::new(files),
        });
        register_active(Arc::clone(&service));
        Ok(service)
    }

    pub fn config_snapshot(&self) -> SqlLogConfig {
        self.config
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_else(|_| SqlLogConfig::from_env(&self.data_dir, &QueryLogConfig::default()))
    }

    pub fn set_global_variable(&self, name: &str, raw: &str) -> Result<bool, QueryError> {
        let mut config = self.config.write().map_err(|_| {
            error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "logging config lock poisoned",
            )
        })?;
        match name {
            "general_log" | "slow_query_log" | "log_output" => {
                let mut candidate = config.clone();
                match name {
                    "general_log" => candidate.general_log = parse_bool(raw)?,
                    "slow_query_log" => candidate.slow_query_log = parse_bool(raw)?,
                    _ => candidate.log_output = LogOutput::parse(raw)?,
                }
                // Validate/open file outputs before publishing their enabled
                // flags, so failed activation leaves the old config intact.
                self.files
                    .lock()
                    .map_err(|_| {
                        error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            "logging file lock poisoned",
                        )
                    })?
                    .ensure_active(&candidate)?;
                self.general_on
                    .store(candidate.general_log, Ordering::Release);
                self.slow_on
                    .store(candidate.slow_query_log, Ordering::Release);
                self.output_bits
                    .store(candidate.log_output.0 as u32, Ordering::Release);
                *config = candidate;
            }
            "long_query_time" => {
                config.long_query_time = parse_seconds_duration(raw)?;
                self.long_query_micros.store(
                    duration_to_micros(config.long_query_time),
                    Ordering::Release,
                );
            }
            "min_examined_row_limit" => {
                config.min_examined_row_limit = raw.parse::<u64>().map_err(|_| {
                    error(
                        crate::error::catalog::ER_WRONG_TYPE_FOR_VAR,
                        format!("incorrect numeric value: '{raw}'"),
                    )
                })?;
            }
            "log_slow_admin_statements" => config.log_slow_admin_statements = parse_bool(raw)?,
            "log_queries_not_using_indexes" => {
                config.log_queries_not_using_indexes = parse_bool(raw)?
            }
            "log_timestamps" => {
                config.log_timestamps = LogTimestampMode::parse(raw).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                        format!("Variable 'log_timestamps' can't be set to the value of '{raw}'"),
                    )
                })?;
            }
            "log_error_verbosity" => {
                let value = raw.parse::<u8>().map_err(|_| {
                    error(
                        crate::error::catalog::ER_WRONG_TYPE_FOR_VAR,
                        format!("incorrect numeric value: '{raw}'"),
                    )
                })?;
                if !(1..=3).contains(&value) {
                    return Err(error(
                        crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                        format!(
                            "Variable 'log_error_verbosity' can't be set to the value of '{raw}'"
                        ),
                    ));
                }
                config.log_error_verbosity = value;
            }
            "general_log_file" => {
                let mut candidate = config.clone();
                candidate.general_log_file = PathBuf::from(raw);
                let files = LogFiles::open(&candidate)?;
                *self.files.lock().map_err(|_| {
                    error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "logging file lock poisoned",
                    )
                })? = files;
                *config = candidate;
            }
            "slow_query_log_file" => {
                let mut candidate = config.clone();
                candidate.slow_query_log_file = PathBuf::from(raw);
                let files = LogFiles::open(&candidate)?;
                *self.files.lock().map_err(|_| {
                    error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "logging file lock poisoned",
                    )
                })? = files;
                *config = candidate;
            }
            "log_error" => {
                let mut candidate = config.clone();
                candidate.error_log_file = PathBuf::from(raw);
                let files = LogFiles::open(&candidate)?;
                *self.files.lock().map_err(|_| {
                    error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "logging file lock poisoned",
                    )
                })? = files;
                *config = candidate;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    pub fn record_general(&self, session: &SessionContext, command_type: &str, argument: &str) {
        if !self.general_on.load(Ordering::Acquire) || session.sql_log_off {
            return;
        }
        let output = LogOutput(self.output_bits.load(Ordering::Acquire) as u8);
        if output == LogOutput::NONE {
            return;
        }
        let config = self.config_snapshot();
        let row = GeneralLogRow {
            event_time: now_ts6(config.log_timestamps),
            user_host: user_host(session),
            thread_id: session.connection_id as u64,
            server_id: crate::binlog::server_id(),
            command_type: command_type.to_string(),
            argument: redact_sql_for_log(argument),
        };
        if output.has_file() {
            if let Ok(mut files) = self.files.lock() {
                if let Some(file) = files.general.as_mut() {
                    let _ = writeln!(
                        file,
                        "{}\t{}\t{}\t{}",
                        row.event_time, row.thread_id, row.command_type, row.argument
                    );
                }
            }
        }
        if output.has_table() {
            let _ = append_json_line(general_table_path(&self.data_dir), &row);
        }
    }

    pub(crate) fn record_slow(
        &self,
        session: &SessionContext,
        sql: &str,
        elapsed: Duration,
        metadata: QueryLogMetadata,
        result: &Result<QueryResponse, QueryError>,
    ) {
        let threshold = Duration::from_micros(self.long_query_micros.load(Ordering::Acquire));
        if elapsed < threshold {
            return;
        }
        // `Slow_queries` counts every statement over `long_query_time`,
        // independent of whether the slow query log is enabled or where it
        // is routed (MySQL semantics). Do this before the `slow_on` /
        // output gates below, which only govern log emission.
        crate::engine::status_counters::record_slow_query(session);
        if !self.slow_on.load(Ordering::Acquire) {
            return;
        }
        let output = LogOutput(self.output_bits.load(Ordering::Acquire) as u8);
        if output == LogOutput::NONE {
            return;
        }
        let config = self.config_snapshot();
        let rows_sent = response_rows_sent(result);
        // P1-D3: prefer the per-statement counter populated by storage
        // scans; fall back to rows_sent when the statement never went
        // through a scan that publishes the counter (e.g. DDL or a
        // protocol-level command). max() guards against a partial scan
        // that surfaced fewer rows than rows_sent (shouldn't happen in
        // practice but keeps the log monotonic vs. what the client
        // observed).
        let rows_examined = metadata.rows_examined.max(rows_sent);
        if rows_examined < config.min_examined_row_limit {
            return;
        }
        let sql_text = redact_sql_for_log(sql.trim());
        let row = SlowLogRow {
            start_time: now_ts6(config.log_timestamps),
            user_host: user_host(session),
            query_time: format_time6(elapsed),
            lock_time: format_time6(Duration::from_micros(
                (metadata.lock_wait_ms.max(0.0) * 1000.0) as u64,
            )),
            rows_sent,
            rows_examined,
            db: session.current_database.clone(),
            last_insert_id: session
                .last_insert_id
                .load(std::sync::atomic::Ordering::Relaxed),
            insert_id: session.statement_insert_id,
            server_id: crate::binlog::server_id(),
            sql_text,
            thread_id: session.connection_id as u64,
        };
        if output.has_file() {
            if let Ok(mut files) = self.files.lock() {
                if let Some(file) = files.slow.as_mut() {
                    let _ = writeln!(file,
                        "# Time: {}\n# User@Host: {}\n# Query_time: {} Lock_time: {} Rows_sent: {} Rows_examined: {}\nuse {};\n{};",
                        row.start_time, row.user_host, duration_secs(elapsed), row.lock_time,
                        row.rows_sent, row.rows_examined, row.db, row.sql_text.trim_end_matches(';'));
                }
            }
        }
        if output.has_table() {
            let _ = append_json_line(slow_table_path(&self.data_dir), &row);
        }
    }

    pub fn flush(&self, kind: FlushLogKind) -> Result<(), QueryError> {
        let config = self.config_snapshot();
        let mut files = self.files.lock().map_err(|_| {
            error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "logging file lock poisoned",
            )
        })?;
        match kind {
            FlushLogKind::All | FlushLogKind::General => {
                if let Some(file) = files.general.as_mut() {
                    file.flush().map_err(io_err)?;
                }
            }
            _ => {}
        }
        match kind {
            FlushLogKind::All | FlushLogKind::Slow => {
                if let Some(file) = files.slow.as_mut() {
                    file.flush().map_err(io_err)?;
                }
            }
            _ => {}
        }
        match kind {
            FlushLogKind::All | FlushLogKind::Error => files.error.flush().map_err(io_err)?,
            _ => {}
        }
        *files = LogFiles::open(&config)?;
        Ok(())
    }
}

impl LogFiles {
    fn open_initial(config: &SqlLogConfig) -> Result<Self, QueryError> {
        // Table-only logging does not use general/slow file outputs. Explicit
        // startup paths are still opened eagerly to preserve IO validation.
        let general = (config.general_log && config.log_output.has_file())
            || env_value("MYDB_GENERAL_LOG_FILE").is_ok();
        let slow = (config.slow_query_log && config.log_output.has_file())
            || env_value("MYDB_SLOW_QUERY_LOG_FILE").is_ok();
        Ok(Self {
            general: if general {
                Some(append_file(&config.general_log_file)?)
            } else {
                None
            },
            slow: if slow {
                Some(append_file(&config.slow_query_log_file)?)
            } else {
                None
            },
            error: append_file(&config.error_log_file)?,
        })
    }

    fn ensure_active(&mut self, config: &SqlLogConfig) -> Result<(), QueryError> {
        if config.log_output.has_file() {
            if config.general_log && self.general.is_none() {
                self.general = Some(append_file(&config.general_log_file)?);
            }
            if config.slow_query_log && self.slow.is_none() {
                self.slow = Some(append_file(&config.slow_query_log_file)?);
            }
        }
        Ok(())
    }

    fn open(config: &SqlLogConfig) -> Result<Self, QueryError> {
        Ok(Self {
            general: Some(append_file(&config.general_log_file)?),
            slow: Some(append_file(&config.slow_query_log_file)?),
            error: append_file(&config.error_log_file)?,
        })
    }
}

pub fn active_service() -> Option<Arc<SqlLogService>> {
    active_service_cell()
        .read()
        .ok()
        .and_then(|guard| guard.clone())
}

pub fn register_active(service: Arc<SqlLogService>) {
    if let Ok(mut guard) = active_service_cell().write() {
        *guard = Some(service);
    }
}

pub fn set_global_variable(name: &str, raw: &str) -> Result<bool, QueryError> {
    match active_service() {
        Some(service) => service.set_global_variable(name, raw),
        None => Ok(false),
    }
}

/// Defaults are independent of the active configuration. Only filesystem
/// location depends on the service, which may open after registry creation.
pub(crate) fn compiled_variable_default(name: &str) -> String {
    match name {
        "general_log" | "log_slow_admin_statements" | "log_queries_not_using_indexes" => {
            "OFF".into()
        }
        "slow_query_log" => "ON".into(),
        "log_output" => LogOutput::default().as_mysql_value().into(),
        "long_query_time" => duration_secs(QueryLogConfig::default().slow_sql_threshold),
        "min_examined_row_limit" => "0".into(),
        "log_timestamps" => "SYSTEM".into(),
        "log_error_verbosity" => "2".into(),
        "log_error_services" => "log_filter_internal; log_sink_internal".into(),
        "general_log_file" | "slow_query_log_file" | "log_error" => {
            let file = match name {
                "general_log_file" => "mydb.log",
                "slow_query_log_file" => "mydb-slow.log",
                _ => "mydb.err",
            };
            active_service()
                .map(|service| logs_dir(&service.data_dir).join(file))
                .unwrap_or_else(|| PathBuf::from(file))
                .display()
                .to_string()
        }
        _ => unreachable!("unregistered logging variable"),
    }
}

pub fn variables_snapshot() -> Vec<(&'static str, String)> {
    let Some(service) = active_service() else {
        return Vec::new();
    };
    let config = service.config_snapshot();
    vec![
        ("general_log", on_off(config.general_log).to_string()),
        (
            "general_log_file",
            config.general_log_file.display().to_string(),
        ),
        ("log_output", config.log_output.as_mysql_value().to_string()),
        ("slow_query_log", on_off(config.slow_query_log).to_string()),
        (
            "slow_query_log_file",
            config.slow_query_log_file.display().to_string(),
        ),
        ("long_query_time", duration_secs(config.long_query_time)),
        (
            "min_examined_row_limit",
            config.min_examined_row_limit.to_string(),
        ),
        (
            "log_slow_admin_statements",
            on_off(config.log_slow_admin_statements).to_string(),
        ),
        (
            "log_queries_not_using_indexes",
            on_off(config.log_queries_not_using_indexes).to_string(),
        ),
        (
            "log_timestamps",
            config.log_timestamps.as_mysql_value().to_string(),
        ),
        ("log_error", config.error_log_file.display().to_string()),
        (
            "log_error_verbosity",
            config.log_error_verbosity.to_string(),
        ),
        (
            "log_error_services",
            "log_filter_internal; log_sink_internal".to_string(),
        ),
    ]
}

pub fn record_error_log_line(prio: &str, subsystem: &str, data: &str) {
    let mode = active_service()
        .map(|service| service.config_snapshot().log_timestamps)
        .unwrap_or(LogTimestampMode::System);
    let event = ErrorLogEvent {
        logged: now_ts6(mode),
        thread_id: None,
        prio: prio.to_string(),
        error_code: None,
        subsystem: subsystem.to_string(),
        data: data.to_string(),
    };
    if let Some(service) = active_service() {
        if let Ok(mut files) = service.files.lock() {
            let _ = writeln!(
                files.error,
                "{} [{}] [{}] {}",
                event.logged, prio, subsystem, data
            );
        }
    }
    if let Ok(mut events) = error_events().lock() {
        if events.len() >= 256 {
            events.pop_front();
        }
        events.push_back(event);
    }
}

pub fn error_log_rows() -> Vec<Vec<Value>> {
    error_events()
        .lock()
        .map(|events| {
            events
                .iter()
                .map(|event| {
                    vec![
                        Value::String(event.logged.clone()),
                        event.thread_id.map(Value::UInt).unwrap_or(Value::Null),
                        Value::String(event.prio.clone()),
                        event
                            .error_code
                            .as_ref()
                            .map(|value| Value::String(value.clone()))
                            .unwrap_or(Value::Null),
                        Value::String(event.subsystem.clone()),
                        Value::String(event.data.clone()),
                    ]
                })
                .collect()
        })
        .unwrap_or_default()
}

pub fn general_log_rows(data_dir: &Path) -> Result<Vec<Vec<Value>>, QueryError> {
    read_json_lines::<GeneralLogRow>(&general_table_path(data_dir)).map(|rows| {
        rows.into_iter()
            .map(|row| {
                vec![
                    Value::String(row.event_time),
                    Value::String(row.user_host),
                    Value::UInt(row.thread_id),
                    Value::UInt(row.server_id as u64),
                    Value::String(row.command_type),
                    Value::Bytes(row.argument.into_bytes()),
                ]
            })
            .collect()
    })
}

pub fn slow_log_rows(data_dir: &Path) -> Result<Vec<Vec<Value>>, QueryError> {
    read_json_lines::<SlowLogRow>(&slow_table_path(data_dir)).map(|rows| {
        rows.into_iter()
            .map(|row| {
                vec![
                    Value::String(row.start_time),
                    Value::String(row.user_host),
                    Value::String(row.query_time),
                    Value::String(row.lock_time),
                    Value::UInt(row.rows_sent),
                    Value::UInt(row.rows_examined),
                    Value::String(row.db),
                    Value::UInt(row.last_insert_id),
                    Value::UInt(row.insert_id),
                    Value::UInt(row.server_id as u64),
                    Value::Bytes(row.sql_text.into_bytes()),
                    Value::UInt(row.thread_id),
                ]
            })
            .collect()
    })
}

pub fn truncate_log_table(data_dir: &Path, table: &str) -> Result<bool, QueryError> {
    let path = match table.to_ascii_lowercase().as_str() {
        "general_log" => general_table_path(data_dir),
        "slow_log" => slow_table_path(data_dir),
        _ => return Ok(false),
    };
    let _guard = table_log_io_lock().lock().map_err(|_| {
        error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "table log I/O lock poisoned",
        )
    })?;
    fs::create_dir_all(logs_dir(data_dir)).map_err(io_err)?;
    File::create(path).map_err(io_err)?;
    Ok(true)
}

fn append_file(path: &Path) -> Result<File, QueryError> {
    if crate::storage::image_io::immutable_path(path) {
        return Err(crate::storage::image_io::read_only_error());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(io_err)?;
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(io_err)
}

fn append_json_line<T: Serialize>(path: PathBuf, row: &T) -> Result<(), QueryError> {
    if crate::storage::image_io::immutable_path(&path) {
        return Err(crate::storage::image_io::read_only_error());
    }
    let _guard = table_log_io_lock().lock().map_err(|_| {
        error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "table log I/O lock poisoned",
        )
    })?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(io_err)?;
    }
    let mut encoded = serde_json::to_vec(row).map_err(|err| {
        error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            format!("write log row failed: {err}"),
        )
    })?;
    encoded.push(b'\n');
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(io_err)?;
    file.write_all(&encoded).map_err(io_err)
}

fn read_json_lines<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Vec<T>, QueryError> {
    let _guard = table_log_io_lock().lock().map_err(|_| {
        error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "table log I/O lock poisoned",
        )
    })?;
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(io_err(err)),
    };
    let mut reader = BufReader::new(file);
    let mut rows = Vec::new();
    let mut invalid_rows = 0usize;
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line).map_err(io_err)? == 0 {
            break;
        }
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match serde_json::from_slice::<T>(&line) {
            Ok(row) => rows.push(row),
            Err(_) => invalid_rows += 1,
        }
    }
    if invalid_rows > 0 {
        eprintln!(
            "mydb: skipped {invalid_rows} malformed JSONL row(s) while reading {}",
            path.display()
        );
    }
    Ok(rows)
}

fn logs_dir(data_dir: &Path) -> PathBuf {
    crate::storage::naming::system_dir(data_dir).join("logs")
}

fn general_table_path(data_dir: &Path) -> PathBuf {
    logs_dir(data_dir).join("mysql.general_log.jsonl")
}

fn slow_table_path(data_dir: &Path) -> PathBuf {
    logs_dir(data_dir).join("mysql.slow_log.jsonl")
}

fn env_bool(name: &str) -> Option<bool> {
    env_value(name).ok().and_then(|raw| parse_bool(&raw).ok())
}

fn env_value(name: &str) -> Result<String, std::env::VarError> {
    crate::util::env::var(name)
}

fn parse_bool(raw: &str) -> Result<bool, QueryError> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "1" | "ON" | "TRUE" | "YES" => Ok(true),
        "0" | "OFF" | "FALSE" | "NO" => Ok(false),
        other => Err(error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
            format!("incorrect boolean value: '{other}'"),
        )),
    }
}

fn parse_seconds_duration(raw: &str) -> Result<Duration, QueryError> {
    let seconds = raw.trim().parse::<f64>().map_err(|_| {
        error(
            crate::error::catalog::ER_WRONG_TYPE_FOR_VAR,
            format!("incorrect numeric value: '{raw}'"),
        )
    })?;
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
            format!("incorrect duration value: '{raw}'"),
        ));
    }
    Ok(Duration::from_micros((seconds * 1_000_000.0) as u64))
}

fn duration_to_micros(value: Duration) -> u64 {
    value.as_micros().min(u64::MAX as u128) as u64
}

fn now_ts6(mode: LogTimestampMode) -> String {
    match mode {
        LogTimestampMode::System => Local::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
        LogTimestampMode::Utc => Utc::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
    }
}

fn user_host(session: &SessionContext) -> String {
    let peer = &session.host;
    let hostname = if matches!(peer.as_str(), "127.0.0.1" | "::1") {
        "localhost"
    } else {
        peer.as_str()
    };
    format!(
        "{}[{}] @ {} [{}]",
        session.user, session.authenticated_as.user, hostname, peer
    )
}

pub(crate) fn redact_sql_for_log(sql: &str) -> String {
    if crate::engine::ai::sql::sql_may_have_ai(sql) {
        return "[redacted AI statement]".to_string();
    }
    fn starts_with_ascii_case_insensitive(haystack: &str, needle: &[u8]) -> bool {
        haystack
            .as_bytes()
            .get(..needle.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(needle))
    }
    fn contains_ascii_case_insensitive(haystack: &str, needle: &[u8], anchor: usize) -> bool {
        let bytes = haystack.as_bytes();
        let letter = needle[anchor];
        memchr::memchr2_iter(
            letter.to_ascii_lowercase(),
            letter.to_ascii_uppercase(),
            bytes,
        )
        .filter_map(|position| position.checked_sub(anchor))
        .any(|start| {
            bytes[start..]
                .get(..needle.len())
                .is_some_and(|candidate| candidate.eq_ignore_ascii_case(needle))
        })
    }

    // Preserve the exact credential-redaction surface while avoiding an owned,
    // uppercased copy of a multi-megabyte statement. Search for a selective
    // letter inside each pattern rather than testing every separator space.
    let trimmed = sql.trim_start();
    if starts_with_ascii_case_insensitive(trimmed, b"ALTER USER")
        || starts_with_ascii_case_insensitive(trimmed, b"CREATE USER")
        || starts_with_ascii_case_insensitive(trimmed, b"SET PASSWORD")
        || contains_ascii_case_insensitive(sql, b" IDENTIFIED BY ", 13)
        || contains_ascii_case_insensitive(sql, b" PASSWORD(", 5)
    {
        return "[redacted credential statement]".to_string();
    }
    truncate_sql_for_log(sql)
}

fn response_rows_sent(result: &Result<QueryResponse, QueryError>) -> u64 {
    match result {
        Ok(QueryResponse::ResultSet(result)) => result.rows.len() as u64,
        Ok(QueryResponse::SharedResultSet(result)) => result.rows.len() as u64,
        Ok(QueryResponse::StreamingResultSet(result)) => result.found_rows.unwrap_or(0),
        Ok(QueryResponse::MultiResult(results)) => results.iter().map(response_rows_sent_ok).sum(),
        Ok(QueryResponse::Ok | QueryResponse::OkAffected { .. }) | Err(_) => 0,
    }
}

fn response_rows_sent_ok(response: &QueryResponse) -> u64 {
    match response {
        QueryResponse::ResultSet(result) => result.rows.len() as u64,
        QueryResponse::SharedResultSet(result) => result.rows.len() as u64,
        QueryResponse::StreamingResultSet(result) => result.found_rows.unwrap_or(0),
        QueryResponse::MultiResult(results) => results.iter().map(response_rows_sent_ok).sum(),
        QueryResponse::Ok | QueryResponse::OkAffected { .. } => 0,
    }
}

fn format_time6(value: Duration) -> String {
    let total = value.as_secs();
    let hours = total / 3600;
    let minutes = (total % 3600) / 60;
    let seconds = total % 60;
    format!(
        "{hours:02}:{minutes:02}:{seconds:02}.{:06}",
        value.subsec_micros()
    )
}

fn duration_secs(value: Duration) -> String {
    format!("{:.6}", value.as_secs_f64())
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn on_off(value: bool) -> &'static str {
    if value {
        "ON"
    } else {
        "OFF"
    }
}

fn io_err(err: std::io::Error) -> QueryError {
    error(
        crate::error::catalog::ER_UNKNOWN_ERROR,
        format!("log I/O error: {err}"),
    )
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct QueryLogMetadata {
    pub(crate) is_ddl: bool,
    /// A successful statement may have created reclaimable undo/LOB pages or
    /// advanced the transaction boundary that makes them purge-safe.
    pub(crate) wake_purge: bool,
    pub(crate) lock_wait_count: u64,
    pub(crate) lock_wait_ms: f64,
    /// P1-D3 — rows actually inspected by storage (sequential scans,
    /// index range scans, etc.), distinct from rows the client saw.
    /// Filled in by the dispatcher right before slow-log emission by
    /// draining the per-thread counter
    /// (`perf_events::take_rows_examined`).
    pub(crate) rows_examined: u64,
    pub(crate) sequential_rows_examined: u64,
    pub(crate) dml_path: &'static str,
    pub(crate) dml_rows_input: u64,
    pub(crate) dml_rows_batched: u64,
    pub(crate) dml_conflict_rows: u64,
    pub(crate) dml_legacy_rows: u64,
    pub(crate) dml_race_rollbacks: u64,
}

pub(crate) fn truncate_sql_for_log(sql: &str) -> String {
    const MAX_SQL_LOG_BYTES: usize = 1024;
    if sql.len() <= MAX_SQL_LOG_BYTES {
        return sql.to_string();
    }

    let marker = format!(" ... [truncated, {} bytes total] ... ", sql.len());
    if marker.len() >= MAX_SQL_LOG_BYTES {
        return marker.chars().take(MAX_SQL_LOG_BYTES).collect();
    }

    let keep_bytes = MAX_SQL_LOG_BYTES - marker.len();
    let head_bytes = keep_bytes / 2;
    let tail_bytes = keep_bytes - head_bytes;
    let head_end = floor_char_boundary(sql, head_bytes);
    let tail_start = ceil_char_boundary(sql, sql.len().saturating_sub(tail_bytes));

    format!("{}{}{}", &sql[..head_end], marker, &sql[tail_start..])
}

fn floor_char_boundary(value: &str, mut idx: usize) -> usize {
    idx = idx.min(value.len());
    while !value.is_char_boundary(idx) {
        idx -= 1;
    }
    idx
}

fn ceil_char_boundary(value: &str, mut idx: usize) -> usize {
    idx = idx.min(value.len());
    while idx < value.len() && !value.is_char_boundary(idx) {
        idx += 1;
    }
    idx
}

#[cfg(test)]
mod redaction_tests {
    use super::*;

    #[test]
    fn malformed_jsonl_row_does_not_hide_valid_log_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = general_table_path(dir.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let valid = serde_json::to_string(&GeneralLogRow {
            event_time: "2026-01-01 00:00:00.000000".to_string(),
            user_host: "root[root] @ localhost [127.0.0.1]".to_string(),
            thread_id: 1,
            server_id: 1,
            command_type: "Query".to_string(),
            argument: "SELECT 1".to_string(),
        })
        .unwrap();
        let mut bytes = format!("{valid}\n{{\"partial\":\n{valid}\n").into_bytes();
        bytes.extend_from_slice(&[0xff, b'\n']);
        std::fs::write(&path, bytes).unwrap();
        let rows = general_log_rows(dir.path()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0][1].to_display_string(),
            "root[root] @ localhost [127.0.0.1]"
        );
    }

    #[test]
    fn user_host_separates_login_identity_and_peer_address() {
        let mut session = SessionContext::default();
        session.host = "127.0.0.1".to_string();
        session.authenticated_as = crate::engine::auth::AccountKey::new("root", "%");
        assert_eq!(user_host(&session), "root[root] @ localhost [127.0.0.1]");
    }

    #[test]
    fn selective_credential_scan_preserves_full_text_matches() {
        for pattern in [" IDENTIFIED BY ", " PASSWORD("] {
            for spelling in [
                pattern.to_string(),
                pattern.to_ascii_lowercase(),
                pattern
                    .chars()
                    .enumerate()
                    .map(|(i, c)| {
                        if i % 2 == 0 {
                            c.to_ascii_lowercase()
                        } else {
                            c
                        }
                    })
                    .collect(),
            ] {
                for prefix in ["", "Y", "é🙂", "Y W identifier "] {
                    let sql = format!("{prefix}{spelling}secret");
                    assert_eq!(redact_sql_for_log(&sql), "[redacted credential statement]");
                }
            }
        }
        for sql in [
            "y",
            "w",
            "y password",
            " PASSWORD",
            " IDENTIFIED BY",
            "SELECT '中 Y abc W'",
        ] {
            assert_eq!(redact_sql_for_log(sql), sql);
        }
    }

    #[test]
    fn credential_statements_are_redacted_before_any_log_sink() {
        for sql in [
            "CREATE USER 'u'@'%' IDENTIFIED BY 'TopSecret!'".to_string(),
            "ALTER USER 'u'@'%' IDENTIFIED BY 'NewSecret!'".to_string(),
            "SET PASSWORD FOR 'u'@'%' = 'AnotherSecret!'".to_string(),
            "GRANT ALL ON *.* TO 'u'@'%' IDENTIFIED BY 'GrantSecret!'".to_string(),
            format!(
                "CREATE USER 'u'@'%' {} IDENTIFIED BY 'TailSecret!'",
                "COMMENT 'padding' ".repeat(200)
            ),
        ] {
            let redacted = redact_sql_for_log(&sql);
            assert_eq!(redacted, "[redacted credential statement]");
            assert!(!redacted.contains("Secret"));
        }
        assert_eq!(redact_sql_for_log("SELECT 1"), "SELECT 1");
    }
}

#[cfg(test)]
mod file_output_tests {
    use super::*;

    #[test]
    fn unused_file_outputs_open_on_activation_and_preserve_path_errors() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        crate::util::env::with_vars(
            &[
                ("MYDB_GENERAL_LOG", Some("off")),
                ("MYDB_SLOW_QUERY_LOG", Some("on")),
                ("MYDB_LOG_OUTPUT", Some("TABLE")),
                ("MYDB_GENERAL_LOG_FILE", None),
                ("MYDB_SLOW_QUERY_LOG_FILE", None),
                ("MYDB_LOG_ERROR", None),
            ],
            || {
                let service = SqlLogService::open(dir.path(), &QueryLogConfig::default()).unwrap();
                let config = service.config_snapshot();
                assert!(config.error_log_file.is_file());
                assert!(!config.general_log_file.exists());
                assert!(!config.slow_query_log_file.exists());
                service.set_global_variable("log_output", "FILE").unwrap();
                assert!(config.slow_query_log_file.is_file());
                assert!(!config.general_log_file.exists());
                service.set_global_variable("general_log", "on").unwrap();
                service.record_general(&SessionContext::default(), "Query", "SELECT 42");
                assert!(fs::read_to_string(&config.general_log_file)
                    .unwrap()
                    .contains("SELECT 42"));
                let blocked = dir.path().join("not-a-directory");
                fs::write(&blocked, b"blocked").unwrap();
                let invalid = blocked.join("log");
                assert!(service
                    .set_global_variable("general_log_file", invalid.to_str().unwrap())
                    .is_err());
                assert_eq!(
                    service.config_snapshot().general_log_file,
                    config.general_log_file
                );
                crate::util::env::with_vars(&[("MYDB_GENERAL_LOG_FILE", invalid.to_str())], || {
                    assert!(SqlLogService::open(
                        dir.path().join("explicit"),
                        &QueryLogConfig::default()
                    )
                    .is_err());
                });
                // Explicit FLUSH keeps its eager reopen/error contract.
                service.flush(FlushLogKind::All).unwrap();
            },
        );
    }
}
