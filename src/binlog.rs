use crate::gtid::{
    append_gtid, format_gtid_interval_map, merge_intervals, parse_gtid_interval_map,
};
use crate::sql_lex::normalize_admin_sql;
use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{IoSlice, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex, OnceLock, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::NaiveDate;
use crc32fast::Hasher;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::engine::api::{QueryResponse, ResultSet, SessionContext};
use crate::mysql::protocol::write_lenenc_int;
use crate::parser::Statement;
use crate::storage::error;
use crate::storage::schema::{DataTypeKind, TableSchema};
use crate::types::{Column, ColumnType, QueryError, Value};

mod admin;
pub(crate) use admin::admin_result_columns;
use admin::AdminCommand;
#[cfg(test)]
mod audit_tests;
mod disk;
pub(crate) mod prepared;
#[cfg(test)]
mod value_encoding_tests;

const BINLOG_MAGIC: &[u8; 4] = b"\xfe\x62\x69\x6e";
const BINLOG_VERSION: u16 = 4;
const EVENT_HEADER_LEN: u8 = 19;
const DEFAULT_BASE_NAME: &str = "mydb-bin";
const DEFAULT_SERVER_ID: u32 = 1;
const MYSQL_SERVER_VERSION: &str = crate::server_version::MYSQL_COMPAT_VERSION;

const QUERY_EVENT: u8 = 0x02;
const ROTATE_EVENT: u8 = 0x04;
const FORMAT_DESCRIPTION_EVENT: u8 = 0x0f;
const XID_EVENT: u8 = 0x10;
const TABLE_MAP_EVENT: u8 = 0x13;
const HEARTBEAT_EVENT: u8 = 0x1b;
const WRITE_ROWS_EVENT_V2: u8 = 0x1e;
const UPDATE_ROWS_EVENT_V2: u8 = 0x1f;
const DELETE_ROWS_EVENT_V2: u8 = 0x20;
const GTID_EVENT: u8 = 0x21;
const ANONYMOUS_GTID_EVENT: u8 = 0x22;
const PREVIOUS_GTIDS_EVENT: u8 = 0x23;

const OPTIONAL_METADATA_SIGNEDNESS: u8 = 0x01;
const OPTIONAL_METADATA_COLUMN_NAME: u8 = 0x04;

const MYSQL_TYPE_TINY: u8 = 0x01;
const MYSQL_TYPE_SHORT: u8 = 0x02;
const MYSQL_TYPE_LONG: u8 = 0x03;
const MYSQL_TYPE_FLOAT: u8 = 0x04;
const MYSQL_TYPE_DOUBLE: u8 = 0x05;
const MYSQL_TYPE_TIMESTAMP: u8 = 0x07;
const MYSQL_TYPE_LONGLONG: u8 = 0x08;
const MYSQL_TYPE_INT24: u8 = 0x09;
const MYSQL_TYPE_DATE: u8 = 0x0a;
const MYSQL_TYPE_TIME: u8 = 0x0b;
const MYSQL_TYPE_DATETIME: u8 = 0x0c;
const MYSQL_TYPE_YEAR: u8 = 0x0d;
const MYSQL_TYPE_VARCHAR: u8 = 0x0f;
const MYSQL_TYPE_BIT: u8 = 0x10;
const MYSQL_TYPE_TIMESTAMP2: u8 = 0x11;
const MYSQL_TYPE_DATETIME2: u8 = 0x12;
const MYSQL_TYPE_TIME2: u8 = 0x13;
const MYSQL_TYPE_NEWDECIMAL: u8 = 0xf6;
const MYSQL_TYPE_ENUM: u8 = 0xf7;
const MYSQL_TYPE_SET: u8 = 0xf8;
const MYSQL_TYPE_JSON: u8 = 0xf5;
const MYSQL_TYPE_BLOB: u8 = 0xfc;
const MYSQL_TYPE_STRING: u8 = 0xfe;
const MYSQL_TYPE_GEOMETRY: u8 = 0xff;

const BINLOG_DUMP_NON_BLOCK: u16 = 0x0001;
const DEFAULT_BINLOG_ROW_EVENT_MAX_BYTES: usize = 64 * 1024 * 1024;
const BINLOG_FILE_POSITION_LIMIT: u64 = u32::MAX as u64;

fn binlog_row_event_max_bytes() -> usize {
    crate::util::env::var("MYDB_BINLOG_ROW_EVENT_MAX_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_BINLOG_ROW_EVENT_MAX_BYTES)
        .clamp(1024, (u32::MAX as usize).saturating_sub(64))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinlogChecksum {
    Crc32,
    None,
}

impl BinlogChecksum {
    pub fn from_env_value(value: &str) -> Result<Self, QueryError> {
        match value.trim().to_ascii_uppercase().as_str() {
            "CRC32" => Ok(Self::Crc32),
            "NONE" | "OFF" => Ok(Self::None),
            other => Err(error(
                crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
                format!("unsupported MYDB_BINLOG_CHECKSUM value '{other}'; use CRC32 or NONE"),
            )),
        }
    }

    pub fn as_mysql_value(self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::None => "NONE",
        }
    }

    fn descriptor_byte(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Crc32 => 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinlogConfig {
    pub enabled: bool,
    pub server_id: u32,
    pub server_uuid: String,
    pub base_name: String,
    pub checksum: BinlogChecksum,
    pub retention_hours: Option<u64>,
    pub retention_bytes: Option<u64>,
    #[serde(default = "default_log_replica_updates")]
    pub log_replica_updates: bool,
    #[serde(default = "default_sync_binlog")]
    pub sync_binlog: u64,
    #[serde(default)]
    pub rotate_bytes: Option<u64>,
    /// `rpl_semi_sync_source_enabled` (alias `rpl_semi_sync_master_enabled`).
    /// When ON and at least one semi-sync replica is connected, a committing
    /// transaction blocks after its binlog/WAL fsync until enough replicas ack
    /// its end position (or the timeout fires and it degrades to async).
    #[serde(default)]
    pub semi_sync_source_enabled: bool,
    /// `rpl_semi_sync_source_timeout` in milliseconds (default 10000).
    #[serde(default = "default_semi_sync_timeout")]
    pub semi_sync_source_timeout: u64,
    /// `rpl_semi_sync_source_wait_for_replica_count` (default 1).
    #[serde(default = "default_semi_sync_wait_count")]
    pub semi_sync_source_wait_for_replica_count: u64,
    /// `rpl_semi_sync_source_wait_no_replica` (default ON): when a commit finds
    /// too few semi-sync replicas connected, ON waits for the quorum until
    /// timeout; OFF degrades immediately when the quorum is unavailable.
    #[serde(default = "default_true")]
    pub semi_sync_source_wait_no_replica: bool,
}

impl Default for BinlogConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            server_id: DEFAULT_SERVER_ID,
            server_uuid: generate_server_uuid(),
            base_name: DEFAULT_BASE_NAME.to_string(),
            checksum: BinlogChecksum::Crc32,
            retention_hours: None,
            retention_bytes: None,
            log_replica_updates: default_log_replica_updates(),
            sync_binlog: default_sync_binlog(),
            rotate_bytes: None,
            semi_sync_source_enabled: false,
            semi_sync_source_timeout: default_semi_sync_timeout(),
            semi_sync_source_wait_for_replica_count: default_semi_sync_wait_count(),
            semi_sync_source_wait_no_replica: true,
        }
    }
}

fn default_semi_sync_timeout() -> u64 {
    10_000
}

fn default_semi_sync_wait_count() -> u64 {
    1
}

fn default_true() -> bool {
    true
}

/// Status-vars carried by a QUERY_EVENT. Captured from the session at the
/// point a DDL is emitted (or built per-transaction for DML BEGIN); the
/// replica replays these into a temporary session before executing the SQL.
#[derive(Debug, Clone)]
pub struct BinlogQueryContext {
    pub thread_id: u32,
    pub sql_mode: u64,
    pub auto_increment_increment: u16,
    pub auto_increment_offset: u16,
    pub client_charset_id: u16,
    pub connection_collation_id: u16,
    pub server_collation_id: u16,
    pub time_zone: String,
    pub lc_time_names: u16,
    pub charset_database_id: u16,
    pub explicit_defaults_for_timestamp: bool,
    pub foreign_key_checks: bool,
    pub unique_checks: bool,
}

impl Default for BinlogQueryContext {
    fn default() -> Self {
        Self {
            thread_id: 0,
            sql_mode: DEFAULT_SQL_MODE_BITS,
            auto_increment_increment: 1,
            auto_increment_offset: 1,
            client_charset_id: crate::charset::collation_id("utf8mb4_0900_ai_ci"),
            connection_collation_id: crate::charset::collation_id("utf8mb4_0900_ai_ci"),
            server_collation_id: crate::charset::collation_id("utf8mb4_0900_ai_ci"),
            time_zone: "SYSTEM".to_string(),
            lc_time_names: 0, // en_US
            charset_database_id: crate::charset::collation_id("utf8mb4_0900_ai_ci"),
            explicit_defaults_for_timestamp: true,
            foreign_key_checks: true,
            unique_checks: true,
        }
    }
}

/// MySQL 8.0 sql_mode bits. Subset of the canonical mapping in `sql/sql_const.h`;
/// values not in the mapping are silently dropped so we never write an
/// unrecognised bit (replicas would reject as invalid mode).
pub fn sql_mode_str_to_bits(mode: &str) -> u64 {
    thread_local! {
        // Every prepared SELECT/DML execution revalidates the session's
        // sql_mode fingerprint, and a connection's mode string almost never
        // changes. A one-entry memo turns the token scan into one memcmp.
        static LAST: std::cell::RefCell<(String, u64)> =
            const { std::cell::RefCell::new((String::new(), 0)) };
    }
    if let Some(bits) = LAST.with(|last| {
        let last = last.borrow();
        (last.0 == mode).then_some(last.1)
    }) {
        return bits;
    }
    let bits = parse_sql_mode_bits(mode);
    LAST.with(|last| {
        let mut last = last.borrow_mut();
        last.0.clear();
        last.0.push_str(mode);
        last.1 = bits;
    });
    bits
}

fn parse_sql_mode_bits(mode: &str) -> u64 {
    let mut bits: u64 = 0;
    for token in mode.split(',') {
        // The longest recognised mode name is 24 bytes. Folding into a stack
        // buffer keeps this per-statement fingerprint check allocation-free.
        let token = token.trim().as_bytes();
        let mut folded = [0u8; 32];
        let Some(slot) = folded.get_mut(..token.len()) else {
            continue;
        };
        slot.copy_from_slice(token);
        slot.make_ascii_uppercase();
        let Ok(token) = std::str::from_utf8(slot) else {
            continue;
        };
        bits |= match token {
            "REAL_AS_FLOAT" => 0x0000_0001,
            "PIPES_AS_CONCAT" => 0x0000_0002,
            "ANSI_QUOTES" => 0x0000_0004,
            "IGNORE_SPACE" => 0x0000_0008,
            "ONLY_FULL_GROUP_BY" => 0x0000_0020,
            "NO_UNSIGNED_SUBTRACTION" => 0x0000_0040,
            "NO_DIR_IN_CREATE" => 0x0000_0080,
            "ANSI" => 0x0004_0000,
            "NO_AUTO_VALUE_ON_ZERO" => 0x0008_0000,
            "NO_BACKSLASH_ESCAPES" => 0x0010_0000,
            "STRICT_TRANS_TABLES" => 0x0020_0000,
            "STRICT_ALL_TABLES" => 0x0040_0000,
            "NO_ZERO_IN_DATE" => 0x0080_0000,
            "NO_ZERO_DATE" => 0x0100_0000,
            "ALLOW_INVALID_DATES" | "INVALID_DATES" => 0x0200_0000,
            "ERROR_FOR_DIVISION_BY_ZERO" => 0x0400_0000,
            "TRADITIONAL" => 0x0800_0000,
            "HIGH_NOT_PRECEDENCE" => 0x2000_0000,
            "NO_ENGINE_SUBSTITUTION" => 0x4000_0000,
            "PAD_CHAR_TO_FULL_LENGTH" => 0x8000_0000,
            "TIME_TRUNCATE_FRACTIONAL" => 0x1_0000_0000,
            _ => 0,
        };
    }
    bits
}

/// Inverse of `sql_mode_str_to_bits`: a comma-separated token list suitable
/// for `SET sql_mode = '...'`.
pub fn sql_mode_bits_to_str(bits: u64) -> String {
    let mut tokens: Vec<&'static str> = Vec::new();
    let table: &[(u64, &str)] = &[
        (0x0000_0001, "REAL_AS_FLOAT"),
        (0x0000_0002, "PIPES_AS_CONCAT"),
        (0x0000_0004, "ANSI_QUOTES"),
        (0x0000_0008, "IGNORE_SPACE"),
        (0x0000_0020, "ONLY_FULL_GROUP_BY"),
        (0x0000_0040, "NO_UNSIGNED_SUBTRACTION"),
        (0x0000_0080, "NO_DIR_IN_CREATE"),
        (0x0004_0000, "ANSI"),
        (0x0008_0000, "NO_AUTO_VALUE_ON_ZERO"),
        (0x0010_0000, "NO_BACKSLASH_ESCAPES"),
        (0x0020_0000, "STRICT_TRANS_TABLES"),
        (0x0040_0000, "STRICT_ALL_TABLES"),
        (0x0080_0000, "NO_ZERO_IN_DATE"),
        (0x0100_0000, "NO_ZERO_DATE"),
        (0x0200_0000, "ALLOW_INVALID_DATES"),
        (0x0400_0000, "ERROR_FOR_DIVISION_BY_ZERO"),
        (0x0800_0000, "TRADITIONAL"),
        (0x2000_0000, "HIGH_NOT_PRECEDENCE"),
        (0x4000_0000, "NO_ENGINE_SUBSTITUTION"),
        (0x8000_0000, "PAD_CHAR_TO_FULL_LENGTH"),
        (0x1_0000_0000, "TIME_TRUNCATE_FRACTIONAL"),
    ];
    for (bit, name) in table {
        if bits & bit != 0 {
            tokens.push(name);
        }
    }
    tokens.join(",")
}

/// MySQL 8.0 default sql_mode (`ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,
/// NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION`).
pub const DEFAULT_SQL_MODE_BITS: u64 = 0x0000_0020 // ONLY_FULL_GROUP_BY
    | 0x0020_0000 // STRICT_TRANS_TABLES
    | 0x0080_0000 // NO_ZERO_IN_DATE
    | 0x0100_0000 // NO_ZERO_DATE
    | 0x0400_0000 // ERROR_FOR_DIVISION_BY_ZERO
    | 0x4000_0000; // NO_ENGINE_SUBSTITUTION

impl BinlogConfig {
    pub fn normalize(mut self) -> Self {
        if self.server_id == 0 {
            self.server_id = DEFAULT_SERVER_ID;
        }
        if self.server_uuid.trim().is_empty() {
            self.server_uuid = generate_server_uuid();
        }
        if self.base_name.trim().is_empty() {
            self.base_name = DEFAULT_BASE_NAME.to_string();
        }
        self
    }
}

fn default_log_replica_updates() -> bool {
    true
}

fn default_sync_binlog() -> u64 {
    1
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinlogPosition {
    pub file: String,
    pub position: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct BinlogStatusSnapshot {
    pub(crate) position: BinlogPosition,
    pub(crate) executed_gtid_set: String,
}

#[derive(Debug, Clone)]
pub struct BinlogEventChunk {
    pub file: String,
    pub start_pos: u64,
    pub end_pos: u64,
    pub event_type: u8,
    pub gtid: Option<String>,
    pub bytes: Arc<Vec<u8>>,
}

impl BinlogEventChunk {}

/// Exclusion is transaction-scoped and survives bounded dump batches. File
/// metadata is always sent so a receiver can follow rotation and checksum FDEs.
pub(crate) struct BinlogGtidFilter {
    excluded: crate::gtid::GtidSet,
    skipping: bool,
}

impl BinlogGtidFilter {
    pub(crate) fn new(executed: &str) -> Self {
        Self {
            excluded: crate::gtid::GtidSet::parse(executed),
            skipping: false,
        }
    }

    pub(crate) fn should_send(&mut self, event: &BinlogEventChunk) -> bool {
        match event.event_type {
            FORMAT_DESCRIPTION_EVENT | ROTATE_EVENT | PREVIOUS_GTIDS_EVENT => {
                self.skipping = false;
                true
            }
            GTID_EVENT | ANONYMOUS_GTID_EVENT => {
                self.skipping = event
                    .gtid
                    .as_deref()
                    .is_some_and(|gtid| self.excluded.contains(gtid));
                !self.skipping
            }
            _ => !self.skipping,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BinlogDumpRequest {
    pub file: String,
    pub position: u64,
    pub flags: u16,
    pub slave_server_id: u32,
}

#[derive(Debug, Clone)]
pub struct BinlogDumpGtidRequest {
    pub file: String,
    pub position: u64,
    pub flags: u16,
    pub slave_server_id: u32,
    pub executed_gtid_set: String,
}

impl BinlogDumpGtidRequest {
    pub fn non_blocking(&self) -> bool {
        self.flags & BINLOG_DUMP_NON_BLOCK != 0
    }
}

impl BinlogDumpRequest {
    pub fn non_blocking(&self) -> bool {
        self.flags & BINLOG_DUMP_NON_BLOCK != 0
    }
}

#[derive(Debug, Clone)]
pub struct BinaryLogInfo {
    pub name: String,
    pub size: u64,
}

#[derive(Debug)]
pub struct BinlogCommit {
    reference: crate::wal::WalBinlogRef,
    chunks: Vec<BinlogEventChunk>,
    /// Process-local append sequence. Transaction commits use this to preserve
    /// binlog order at the WAL-append and dump-publication boundaries while
    /// allowing the expensive binlog/WAL fsyncs to run outside COMMIT_GATE.
    order: u64,
    order_state: Arc<BinlogCommitOrder>,
    group_sync: Arc<BinlogGroupSync>,
    durable: AtomicBool,
    published: bool,
}

impl BinlogCommit {
    pub fn wal_ref(&self) -> &crate::wal::WalBinlogRef {
        &self.reference
    }
}

impl Drop for BinlogCommit {
    fn drop(&mut self) {
        if self.published {
            return;
        }
        let mut group = self.group_sync.state.lock();
        group.writes.remove(&self.order);
        while group.syncing && group.durable_order < self.order && group.fatal_error.is_none() {
            self.group_sync.cv.wait(&mut group);
        }
        let appended = self.durable.load(Ordering::Acquire)
            || (group.durable_order >= self.order && group.fatal_error.is_none());
        // Even an unwritten reservation leaves a byte-position gap. A later
        // append cannot safely continue until recovery/reset reconstructs the
        // tail. Wake every waiter instead of leaving a permanent order barrier.
        let message = group
            .fatal_error
            .get_or_insert_with(|| {
                format!(
                    "binlog commit {}:{}-{} was abandoned before publication; restart required",
                    self.reference.file, self.reference.start_pos, self.reference.end_pos
                )
            })
            .clone();
        self.group_sync.cv.notify_all();
        drop(group);
        self.order_state.fail(message);
        if !appended {
            self.order_state.abort(self.order);
        }
    }
}

#[derive(Debug)]
struct BinlogCommitOrderState {
    next_alloc: u64,
    next_wal: u64,
    next_publish: u64,
    wal_skips: HashSet<u64>,
    publish_skips: HashSet<u64>,
    fatal_error: Option<String>,
}

impl Default for BinlogCommitOrderState {
    fn default() -> Self {
        Self {
            next_alloc: 1,
            next_wal: 1,
            next_publish: 1,
            wal_skips: HashSet::new(),
            publish_skips: HashSet::new(),
            fatal_error: None,
        }
    }
}

#[derive(Debug, Default)]
struct BinlogCommitOrder {
    state: parking_lot::Mutex<BinlogCommitOrderState>,
    cv: parking_lot::Condvar,
}

impl BinlogCommitOrder {
    fn allocate(&self, needs_wal: bool) -> u64 {
        let mut state = self.state.lock();
        let order = state.next_alloc;
        state.next_alloc = state.next_alloc.saturating_add(1);
        if !needs_wal {
            state.wal_skips.insert(order);
            Self::advance_wal_skips(&mut state);
            self.cv.notify_all();
        }
        order
    }

    fn advance_wal_skips(state: &mut BinlogCommitOrderState) {
        while state.wal_skips.remove(&state.next_wal) {
            state.next_wal = state.next_wal.saturating_add(1);
        }
    }

    fn wait_wal(&self, order: u64) -> Result<(), QueryError> {
        let mut state = self.state.lock();
        loop {
            if let Some(message) = &state.fatal_error {
                return Err(io_error(std::io::Error::other(message.clone())));
            }
            if state.next_wal == order {
                return Ok(());
            }
            self.cv.wait(&mut state);
        }
    }

    fn finish_wal(&self, order: u64) {
        let mut state = self.state.lock();
        debug_assert_eq!(state.next_wal, order);
        if state.next_wal == order {
            state.next_wal = state.next_wal.saturating_add(1);
            Self::advance_wal_skips(&mut state);
        }
        self.cv.notify_all();
    }

    fn wait_publish(&self, order: u64) -> Result<(), QueryError> {
        let mut state = self.state.lock();
        loop {
            if let Some(message) = &state.fatal_error {
                return Err(io_error(std::io::Error::other(message.clone())));
            }
            if state.next_publish == order {
                return Ok(());
            }
            self.cv.wait(&mut state);
        }
    }

    fn fail(&self, message: String) {
        self.state.lock().fatal_error.get_or_insert(message);
        self.cv.notify_all();
    }

    fn finish_publish(&self, order: u64) {
        let mut state = self.state.lock();
        debug_assert_eq!(state.next_publish, order);
        if state.next_publish == order {
            state.next_publish = state.next_publish.saturating_add(1);
            Self::advance_publish_skips(&mut state);
        }
        self.cv.notify_all();
    }

    fn advance_publish_skips(state: &mut BinlogCommitOrderState) {
        while state.publish_skips.remove(&state.next_publish) {
            state.next_publish = state.next_publish.saturating_add(1);
        }
    }

    fn abort(&self, order: u64) {
        let mut state = self.state.lock();
        if order >= state.next_wal {
            state.wal_skips.insert(order);
            Self::advance_wal_skips(&mut state);
        }
        if order >= state.next_publish {
            state.publish_skips.insert(order);
            Self::advance_publish_skips(&mut state);
        }
        self.cv.notify_all();
    }
}

#[derive(Debug, Default)]
struct BinlogGroupSyncState {
    requested_order: u64,
    durable_order: u64,
    syncing: bool,
    /// Encoded event chunks waiting for the group leader to append. Keeping
    /// Arc<Vec<u8>> chunks here is cheap (the BinlogCommit owns the other
    /// references) and, crucially, prevents a later append(2) from blocking
    /// behind an fsync already running on the same file.
    writes: BTreeMap<u64, Vec<BinlogEventChunk>>,
    /// Owned by the leader while I/O is in flight, otherwise parked here.
    /// Never hold the group-state latch across file operations.
    writer: Option<BinlogAppendFile>,
    fatal_error: Option<String>,
}

#[derive(Debug)]
struct BinlogAppendFile {
    name: String,
    file: File,
}

#[derive(Debug, Default)]
struct BinlogGroupSync {
    state: parking_lot::Mutex<BinlogGroupSyncState>,
    cv: parking_lot::Condvar,
    requests: AtomicU64,
    batches: AtomicU64,
    batch_max: AtomicU64,
    write_calls: AtomicU64,
    file_opens: AtomicU64,
    write_bytes: AtomicU64,
    sync_total_us: AtomicU64,
    sync_max_us: AtomicU64,
    groups_since_sync: AtomicU64,
}

/// Write one already-ordered binlog file batch with scatter/gather I/O.
///
/// Group commit previously called `write(2)` once per GTID/BEGIN/TABLE_MAP/
/// ROWS/XID event. That preserved order but left most of the syscall cost
/// untouched even when dozens of transactions shared one fsync. `writev(2)`
/// preserves the exact byte stream without copying Arc-backed event payloads
/// into another contiguous buffer. The loop is required because a valid
/// vectored write may be partial (and platforms cap one call at IOV_MAX).
fn write_event_chunks_vectored<W: Write>(
    writer: &mut W,
    chunks: &[&BinlogEventChunk],
) -> std::io::Result<(u64, u64)> {
    let total_bytes = chunks.iter().fold(0u64, |total, chunk| {
        total.saturating_add(chunk.bytes.len() as u64)
    });
    let mut slices: Vec<IoSlice<'_>> = chunks
        .iter()
        .filter(|chunk| !chunk.bytes.is_empty())
        .map(|chunk| IoSlice::new(chunk.bytes.as_slice()))
        .collect();
    let mut remaining = slices.as_mut_slice();
    let mut calls = 0u64;
    while !remaining.is_empty() {
        match writer.write_vectored(remaining) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write staged binlog event batch",
                ));
            }
            Ok(written) => {
                calls = calls.saturating_add(1);
                IoSlice::advance_slices(&mut remaining, written);
            }
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok((calls, total_bytes))
}

/// Outcome of a semi-sync commit wait. `Acked` and `NoReplicaDegrade` both mean
/// "release the committer"; only `TimedOut` counts as a hard degrade after a
/// real wait. All three release the committer — semi-sync never blocks a commit
/// forever, it degrades to async and records the fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemiSyncOutcome {
    /// Semi-sync inactive (disabled): committed without any wait.
    Inactive,
    /// Enough replicas acked the commit position within the timeout.
    Acked,
    /// Too few semi-sync replicas connected and `wait_no_replica=OFF`: degraded to
    /// async immediately.
    NoReplicaDegrade,
    /// Waited up to the timeout without enough acks: degraded to async.
    TimedOut,
}

/// Master-side semi-sync state. The ack gate is a `(Mutex<generation>, Condvar)`
/// bumped on every replica ack; a committer parked in `semi_sync_wait` re-checks
/// the per-replica acked positions (held in the `DumpRegistry`) each time the
/// generation advances or the deadline elapses. Ported in shape from the native
/// `src/repl` semi-sync waiter, but the wait key here is a binlog `(file, pos)`
/// tuple rather than an LSN, so the two stay separate (each its own oracle).
#[derive(Debug)]
struct SemiSyncSource {
    enabled: AtomicBool,
    timeout_ms: AtomicU64,
    wait_for_replica_count: AtomicU64,
    wait_no_replica: AtomicBool,
    /// Commits released after enough acks arrived in time.
    yes_tx: AtomicU64,
    /// Commits that degraded to async (timeout or no-replica).
    no_tx: AtomicU64,
    /// Committers currently parked in `semi_sync_wait` (gauge).
    wait_sessions: AtomicUsize,
    /// Monotonic ack generation + condvar: bumped and notified on every ack.
    ack_gate: StdMutex<u64>,
    ack_cond: Condvar,
    degraded_at: StdMutex<Option<(String, u64)>>,
}

impl SemiSyncSource {
    fn new(config: &BinlogConfig) -> Self {
        Self {
            enabled: AtomicBool::new(config.semi_sync_source_enabled),
            timeout_ms: AtomicU64::new(config.semi_sync_source_timeout),
            wait_for_replica_count: AtomicU64::new(config.semi_sync_source_wait_for_replica_count),
            wait_no_replica: AtomicBool::new(config.semi_sync_source_wait_no_replica),
            yes_tx: AtomicU64::new(0),
            no_tx: AtomicU64::new(0),
            wait_sessions: AtomicUsize::new(0),
            ack_gate: StdMutex::new(0),
            ack_cond: Condvar::new(),
            degraded_at: StdMutex::new(None),
        }
    }

    fn notify_ack(&self) {
        let mut gen = self.ack_gate.lock().unwrap();
        *gen = gen.wrapping_add(1);
        self.ack_cond.notify_all();
    }
}

/// True if binlog stream position `a = (file, pos)` is at or beyond `b`. Binlog
/// file names share a base and a zero-padded numeric suffix; compare by that
/// suffix (robust to a rollover in digit width) then by byte position. An empty
/// file name (a replica that has never acked) sorts before any real position.
fn binlog_pos_ge(a: (&str, u64), b: (&str, u64)) -> bool {
    binlog_pos_cmp(a, b) != std::cmp::Ordering::Less
}

fn binlog_pos_cmp(a: (&str, u64), b: (&str, u64)) -> std::cmp::Ordering {
    let (fa, pa) = a;
    let (fb, pb) = b;
    binlog_file_ord(fa)
        .cmp(&binlog_file_ord(fb))
        .then_with(|| fa.cmp(fb))
        .then_with(|| pa.cmp(&pb))
}

/// Numeric ordering key for a binlog file name (`binlog.000007` → 7). Empty or
/// suffix-less names sort first (key 0).
fn binlog_file_ord(name: &str) -> u64 {
    let start = name
        .as_bytes()
        .iter()
        .rposition(|byte| !byte.is_ascii_digit())
        .map_or(0, |index| index + 1);
    name[start..].parse::<u64>().unwrap_or(0)
}

#[derive(Debug)]
pub struct BinlogSourceManager {
    config: BinlogConfig,
    /// Runtime-writable mirror of `config.sync_binlog` read on the hot append
    /// path. `config` is the snapshot captured at `open`; `SET GLOBAL
    /// sync_binlog=N` updates this atomic (and the global display config) so
    /// the change takes effect without reopening the manager. See
    /// `set_runtime_config`.
    sync_binlog: AtomicU64,
    rotate_bytes: AtomicU64,
    active_dump_leases: AtomicUsize,
    reset_generation: AtomicU64,
    dir: PathBuf,
    index_path: PathBuf,
    manifest_path: PathBuf,
    notify: Notify,
    state: parking_lot::Mutex<BinlogState>,
    commit_order: Arc<BinlogCommitOrder>,
    group_sync: Arc<BinlogGroupSync>,
    dump_registry: parking_lot::Mutex<DumpRegistry>,
    semi_sync: SemiSyncSource,
}

/// Tracks active and previously-registered binlog dump consumers
/// (`COM_REGISTER_SLAVE`). The session-side handler `handle_register_slave`
/// inserts a `RegisteredReplica`; when the dump session ends or the
/// connection drops, the `_RegisteredReplicaGuard` returned by
/// `register_replica` removes it on drop.
#[derive(Debug, Default)]
pub struct DumpRegistry {
    entries: std::collections::BTreeMap<u32, RegisteredReplica>,
    /// Monotonic counter used to disambiguate two replicas that share the same
    /// `server_id` — the guard tracks its own registration id so Drop only
    /// removes the entry that this guard owns.
    next_regid: u64,
}

#[derive(Debug, Clone)]
pub struct RegisteredReplica {
    pub server_id: u32,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub master_id: u32,
    pub replication_rank: u32,
    pub connected_at: SystemTime,
    /// Set once the replica sent `SET @rpl_semi_sync_slave=1` before its dump
    /// (semi-sync capability). Only such replicas count toward the wait quorum
    /// and receive `need_ack` events.
    pub semi_sync: bool,
    /// Latest binlog `(file, pos)` this replica has acknowledged (durably
    /// received). Advanced by `record_semi_sync_ack`.
    pub acked_file: String,
    pub acked_pos: u64,
    /// The replica's server UUID, taken from the `@replica_uuid` / `@slave_uuid`
    /// session variable it set before `COM_REGISTER_SLAVE`. Empty when the
    /// replica did not advertise one — surfaced as the `Replica_UUID` column of
    /// `SHOW REPLICAS` / `SHOW SLAVE HOSTS`.
    pub replica_uuid: String,
    /// Registry-owned unique id used to guard against duplicate-server_id
    /// removal races (see `DumpRegistry.next_regid`).
    #[doc(hidden)]
    pub regid: u64,
}

impl DumpRegistry {
    /// Insert; returns the regid stamped onto the entry so the caller's
    /// Drop guard can remove only the entry it owns.
    pub fn insert(&mut self, mut info: RegisteredReplica) -> u64 {
        self.next_regid = self.next_regid.wrapping_add(1);
        let regid = self.next_regid.max(1);
        info.regid = regid;
        // Production registration rejects duplicate server IDs under this lock.
        self.entries.insert(info.server_id, info);
        regid
    }
    pub fn remove_regid(&mut self, server_id: u32, regid: u64) {
        if let Some(entry) = self.entries.get(&server_id) {
            if entry.regid == regid {
                self.entries.remove(&server_id);
            }
        }
    }
    pub fn list(&self) -> Vec<RegisteredReplica> {
        self.entries.values().cloned().collect()
    }
}

/// RAII removes a replica from the dump registry when dropped (i.e. when the
/// dump session ends). Owned by the session that called `register_replica`.
pub struct RegisteredReplicaGuard {
    manager: std::sync::Arc<BinlogSourceManager>,
    server_id: u32,
    regid: u64,
}

pub(crate) struct BinlogDumpLease(Arc<BinlogSourceManager>);

impl Drop for BinlogDumpLease {
    fn drop(&mut self) {
        self.0.active_dump_leases.fetch_sub(1, Ordering::Release);
    }
}

impl Drop for RegisteredReplicaGuard {
    fn drop(&mut self) {
        self.manager
            .dump_registry
            .lock()
            .remove_regid(self.server_id, self.regid);
        self.manager.semi_sync.notify_ack();
    }
}

/// Parse the `COM_REGISTER_SLAVE` payload. The MySQL wire format:
/// ```text
/// 4 bytes — server_id
/// 1 byte  — slave_host_len; N bytes host
/// 1 byte  — slave_user_len; N bytes user
/// 1 byte  — slave_password_len; N bytes password
/// 2 bytes — slave_port
/// 4 bytes — replication_rank
/// 4 bytes — master_id
/// ```
/// Returns the parsed `RegisteredReplica` (sans `connected_at`, which the
/// caller stamps).
pub fn parse_register_slave_payload(payload: &[u8]) -> Result<RegisteredReplica, QueryError> {
    let truncated = || {
        error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "truncated COM_REGISTER_SLAVE payload",
        )
    };
    let mut p = payload;
    if p.len() < 4 {
        return Err(truncated());
    }
    let server_id = u32::from_le_bytes([p[0], p[1], p[2], p[3]]);
    p = &p[4..];
    let host = read_lenenc_u8_string(&mut p).ok_or_else(truncated)?;
    let user = read_lenenc_u8_string(&mut p).ok_or_else(truncated)?;
    // password is sensitive; we drop it after length-validating
    let _password = read_lenenc_u8_string(&mut p).ok_or_else(truncated)?;
    if p.len() < 2 {
        return Err(truncated());
    }
    let port = u16::from_le_bytes([p[0], p[1]]);
    p = &p[2..];
    if p.len() < 4 {
        return Err(truncated());
    }
    let replication_rank = u32::from_le_bytes([p[0], p[1], p[2], p[3]]);
    p = &p[4..];
    if p.len() < 4 {
        return Err(truncated());
    }
    let master_id = u32::from_le_bytes([p[0], p[1], p[2], p[3]]);
    p = &p[4..];
    if !p.is_empty() {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "COM_REGISTER_SLAVE payload has unexpected trailing bytes",
        ));
    }
    Ok(RegisteredReplica {
        server_id,
        host,
        port,
        user,
        master_id,
        replication_rank,
        connected_at: SystemTime::now(),
        semi_sync: false,
        acked_file: String::new(),
        acked_pos: 0,
        // The wire payload carries no UUID; the caller fills it from the
        // replica's `@replica_uuid` / `@slave_uuid` session variable.
        replica_uuid: String::new(),
        regid: 0, // stamped by DumpRegistry::insert on registration
    })
}

/// Parse a replica's semi-sync ACK packet payload: `[0xEF][pos: u64 LE][file]`.
/// Returns `(position, filename)`. Returns `None` if the magic byte is wrong or
/// the packet is truncated (caller ignores non-ack traffic).
pub fn parse_semi_sync_ack(payload: &[u8]) -> Option<(u64, String)> {
    // 1 magic byte + 8 position bytes, then the (possibly empty) filename.
    if payload.len() < 9 || payload[0] != 0xEF {
        return None;
    }
    let pos = u64::from_le_bytes([
        payload[1], payload[2], payload[3], payload[4], payload[5], payload[6], payload[7],
        payload[8],
    ]);
    let filename = String::from_utf8_lossy(&payload[9..])
        .trim_end_matches('\0')
        .to_string();
    Some((pos, filename))
}

fn read_lenenc_u8_string(payload: &mut &[u8]) -> Option<String> {
    if payload.is_empty() {
        return None;
    }
    let len = payload[0] as usize;
    if payload.len() < 1 + len {
        return None;
    }
    let s = String::from_utf8_lossy(&payload[1..1 + len]).into_owned();
    *payload = &payload[1 + len..];
    Some(s)
}

#[derive(Debug, Default)]
struct BinlogState {
    active_file: String,
    active_pos: u64,
    files: Vec<BinaryLogInfo>,
    events: Vec<BinlogEventChunk>,
    live_event_bytes: usize,
    // Published prefixes recovered at open or evicted from the live cache;
    // their payloads stay on disk.
    disk_prefixes: HashMap<String, u64>,
    pending: HashMap<u64, PendingBinlogTxn>,
    gtid_executed: String,
    gtid_purged: String,
    next_gno: u64,
    sequence_number: u64,
    /// First index in `events` belonging to each binlog file. Lets
    /// `events_after` skip past purged history in O(1) instead of scanning
    /// the entire vector to find the start of the requested file. Updated
    /// in lock-step with every mutation of `events`.
    file_first: HashMap<String, usize>,
}

impl BinlogState {
    /// The append cursor may include staged transactions. Only published
    /// lengths may be exposed to readers or persisted as recovery boundaries.
    fn published_position(&self) -> u64 {
        self.files
            .last()
            .filter(|info| info.name == self.active_file)
            .map_or(4, |info| info.size)
    }

    /// Rebuild `file_first` from `events`. Cheap (one linear pass);
    /// called after operations that destructure the events vector (purge,
    /// reset_master, startup).
    fn rebuild_file_index(&mut self) {
        self.file_first.clear();
        self.live_event_bytes = 0;
        for (idx, ev) in self.events.iter().enumerate() {
            self.live_event_bytes = self.live_event_bytes.saturating_add(ev.bytes.len());
            if !self.file_first.contains_key(&ev.file) {
                self.file_first.insert(ev.file.clone(), idx);
            }
        }
    }

    /// Append a chunk and update the per-file start cache so subsequent
    /// `events_after(file, ...)` finds its head in O(1).
    fn push_event(&mut self, chunk: BinlogEventChunk) {
        if let Some(info) = self
            .files
            .iter_mut()
            .rev()
            .find(|info| info.name == chunk.file)
        {
            info.size = info.size.max(chunk.end_pos);
        }
        let idx = self.events.len();
        if !self.file_first.contains_key(&chunk.file) {
            self.file_first.insert(chunk.file.clone(), idx);
        }
        self.live_event_bytes = self.live_event_bytes.saturating_add(chunk.bytes.len());
        self.events.push(chunk);
    }

    /// Published bytes already exist in the file. Keep only a small recent
    /// tail for caught-up dump consumers; lagging readers use the same bounded
    /// disk path as recovered history. Both payload bytes and descriptor count
    /// are bounded, including tiny events and single oversized row events.
    fn trim_published_events(&mut self) {
        const MAX_EVENTS: usize = 4096;
        const MAX_BYTES: usize = 4 * 1024 * 1024;
        if self.events.len() <= MAX_EVENTS && self.live_event_bytes <= MAX_BYTES {
            return;
        }
        let mut evicted = 0;
        let mut bytes = self.live_event_bytes;
        while evicted < self.events.len()
            && (self.events.len() - evicted > MAX_EVENTS / 2 || bytes > MAX_BYTES / 2)
        {
            let event = &self.events[evicted];
            // Called only after physical append and logical publication. Never
            // advance this watermark from staged/unpublished transaction data.
            self.disk_prefixes
                .entry(event.file.clone())
                .and_modify(|end| *end = (*end).max(event.end_pos))
                .or_insert(event.end_pos);
            bytes = bytes.saturating_sub(event.bytes.len());
            evicted += 1;
        }
        self.events.drain(..evicted);
        if self.events.capacity() > MAX_EVENTS * 2 {
            self.events.shrink_to(MAX_EVENTS);
        }
        self.rebuild_file_index();
    }
}

#[derive(Debug, Clone)]
struct AllocatedGtid {
    uuid: String,
    gno: u64,
    sequence_number: u64,
}

impl std::fmt::Display for AllocatedGtid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.uuid.to_ascii_lowercase(), self.gno)
    }
}

#[derive(Debug, Clone)]
enum BinlogTxnOp {
    Encoded {
        events: Vec<(u8, Vec<u8>, bool)>,
    },
    Query {
        database: String,
        sql: String,
        ctx: BinlogQueryContext,
    },
    Write {
        table: BinlogTable,
        rows: Vec<Vec<Value>>,
    },
    Update {
        table: BinlogTable,
        rows: Vec<(Vec<Value>, Vec<Value>)>,
    },
    Delete {
        table: BinlogTable,
        rows: Vec<Vec<Value>>,
    },
}

#[derive(Debug)]
struct PendingBinlogTxn {
    ops: Vec<BinlogTxnOp>,
    memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

/// Keeps ownership across every fallible staging step without cloning rows.
struct PendingBinlogLease<'a> {
    manager: &'a BinlogSourceManager,
    txn_id: u64,
    pending: Option<PendingBinlogTxn>,
}

impl Drop for PendingBinlogLease<'_> {
    fn drop(&mut self) {
        if let Some(pending) = self.pending.take() {
            self.manager
                .state
                .lock()
                .pending
                .insert(self.txn_id, pending);
        }
    }
}

fn binlog_row_resident_bytes(row: &[Value], capacity: usize) -> usize {
    capacity
        .saturating_mul(std::mem::size_of::<Value>())
        .saturating_add(
            row.iter()
                .map(Value::heap_capacity)
                .fold(0usize, usize::saturating_add),
        )
}

fn binlog_table_resident_bytes(table: &BinlogTable) -> usize {
    let columns = table
        .columns
        .iter()
        .map(|column| {
            column
                .name
                .capacity()
                .saturating_add(column.metadata.capacity())
        })
        .fold(0usize, usize::saturating_add);
    table
        .database
        .capacity()
        .saturating_add(table.name.capacity())
        .saturating_add(
            table
                .columns
                .capacity()
                .saturating_mul(std::mem::size_of::<BinlogColumn>()),
        )
        .saturating_add(columns)
}

/// Owned heap capacity below the operation enum itself. The enclosing pending
/// vector is charged separately by its capacity, so unused Vec slots are not
/// missed and live elements are not double-counted.
fn binlog_op_nested_bytes(op: &BinlogTxnOp) -> usize {
    match op {
        BinlogTxnOp::Encoded { events } => events
            .capacity()
            .saturating_mul(std::mem::size_of::<(u8, Vec<u8>, bool)>())
            .saturating_add(
                events
                    .iter()
                    .map(|(_, bytes, _)| bytes.capacity())
                    .sum::<usize>(),
            ),
        BinlogTxnOp::Query { database, sql, .. } => {
            database.capacity().saturating_add(sql.capacity())
        }
        BinlogTxnOp::Write { table, rows } | BinlogTxnOp::Delete { table, rows } => rows
            .capacity()
            .saturating_mul(std::mem::size_of::<Vec<Value>>())
            .saturating_add(binlog_table_resident_bytes(table))
            .saturating_add(rows.iter().fold(0usize, |total, row| {
                total.saturating_add(binlog_row_resident_bytes(row, row.capacity()))
            })),
        BinlogTxnOp::Update { table, rows } => rows
            .capacity()
            .saturating_mul(std::mem::size_of::<(Vec<Value>, Vec<Value>)>())
            .saturating_add(binlog_table_resident_bytes(table))
            .saturating_add(rows.iter().fold(0usize, |total, (before, after)| {
                total
                    .saturating_add(binlog_row_resident_bytes(before, before.capacity()))
                    .saturating_add(binlog_row_resident_bytes(after, after.capacity()))
            })),
    }
}

#[derive(Debug, Clone)]
struct BinlogTable {
    table_id: u64,
    database: String,
    name: String,
    columns: Vec<BinlogColumn>,
}

#[derive(Debug, Clone)]
struct BinlogColumn {
    name: String,
    physical_index: usize,
    column_type: u8,
    metadata: Vec<u8>,
    nullable: bool,
    unsigned: bool,
    // For MYSQL_TYPE_ENUM/SET only: the declared member list, used to
    // translate stored string values (`"medium"`, `"a,b"`) back to the
    // ordinal / bitmask the wire format demands. Without this the row
    // event encoder would try to `parse::<u64>()` a member name and
    // fail with 1366, breaking every DML that touches ENUM/SET columns
    // while binlog is enabled.
    enum_set_values: Option<Arc<Vec<String>>>,
    /// Native VECTOR dimension when this column is exposed on the MySQL 8.0
    /// wire as LONG BLOB + `MYDBVEC1` envelope.
    vector_dim: Option<u32>,
    /// None for binary/numeric columns; storage text is UTF-8 and must be
    /// transcoded when the declared column uses another wire character set.
    charset: Option<&'static str>,
    collation_id: u16,
}

#[derive(Debug, Clone)]
struct BinlogGlobal {
    config: BinlogConfig,
    manager: Option<Arc<BinlogSourceManager>>,
}

static GLOBAL: OnceLock<RwLock<BinlogGlobal>> = OnceLock::new();
static FALLBACK_CONFIG: OnceLock<BinlogConfig> = OnceLock::new();
static COMMIT_GATE: OnceLock<parking_lot::Mutex<()>> = OnceLock::new();
static GLOBAL_READ_LOCK: AtomicBool = AtomicBool::new(false);
static GLOBAL_READ_LOCK_CHANGED: parking_lot::Condvar = parking_lot::Condvar::new();
// The atomic is only an empty-set fast path. Ownership changes and checks
// serialize on the map, so one session can never overwrite another owner.
static GLOBAL_READ_LOCK_OWNERS: std::sync::LazyLock<
    parking_lot::Mutex<std::collections::HashSet<(u64, u32)>>,
> = std::sync::LazyLock::new(Default::default);

pub fn config_from_env_defaults() -> BinlogConfig {
    let mut config = BinlogConfig::default();
    config.enabled = env_truthy("MYDB_LOG_BIN");
    config.server_id = crate::util::env::var("MYDB_SERVER_ID")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(DEFAULT_SERVER_ID);
    config.server_uuid =
        crate::util::env::var("MYDB_SERVER_UUID").unwrap_or_else(|_| generate_server_uuid());
    config.base_name = crate::util::env::var("MYDB_BINLOG_BASE_NAME")
        .unwrap_or_else(|_| DEFAULT_BASE_NAME.to_string());
    config.checksum = crate::util::env::var("MYDB_BINLOG_CHECKSUM")
        .ok()
        .and_then(|raw| match BinlogChecksum::from_env_value(&raw) {
            Ok(c) => Some(c),
            Err(err) => {
                eprintln!(
                    "[binlog] ignoring MYDB_BINLOG_CHECKSUM={raw:?}: {} — falling back to CRC32",
                    err.message()
                );
                None
            }
        })
        .unwrap_or(BinlogChecksum::Crc32);
    config.retention_hours = crate::util::env::var("MYDB_BINLOG_RETENTION_HOURS")
        .ok()
        .and_then(|raw| raw.parse().ok());
    config.retention_bytes = crate::util::env::var("MYDB_BINLOG_RETENTION_BYTES")
        .ok()
        .and_then(|raw| raw.parse().ok());
    config.log_replica_updates = crate::util::env::var("MYDB_LOG_REPLICA_UPDATES")
        .ok()
        .map(|raw| env_value_truthy(&raw))
        .unwrap_or(true);
    config.sync_binlog = crate::util::env::var("MYDB_SYNC_BINLOG")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or_else(default_sync_binlog);
    config.rotate_bytes = crate::util::env::var("MYDB_BINLOG_ROTATE_BYTES")
        .ok()
        .and_then(|raw| raw.parse().ok());
    config.normalize()
}

pub fn init_global(data_dir: PathBuf, config: BinlogConfig) -> Result<(), QueryError> {
    let mut config = config.normalize();
    let manager = if config.enabled {
        Some(Arc::new(BinlogSourceManager::open(
            data_dir,
            config.clone(),
        )?))
    } else {
        None
    };
    if let Some(manager) = &manager {
        // open() restores the durable UUID. Report the same identity that
        // subsequent GTID events actually use.
        config = manager.config.clone();
        if manager.config.retention_hours.is_some() || manager.config.retention_bytes.is_some() {
            let weak = Arc::downgrade(manager);
            std::thread::Builder::new()
                .name("binlog-retention".into())
                .spawn(move || loop {
                    std::thread::sleep(Duration::from_secs(60));
                    let Some(manager) = weak.upgrade() else {
                        break;
                    };
                    let _gate = commit_gate();
                    if let Err(err) = manager.maintain_retention(SystemTime::now()) {
                        crate::util::logging::log_message(format_args!(
                            "binlog retention: {}",
                            err.message()
                        ));
                    }
                })
                .map_err(|err| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!("start binlog retention: {err}"),
                    )
                })?;
        }
    }
    let global = GLOBAL.get_or_init(|| {
        RwLock::new(BinlogGlobal {
            config: fallback_config(),
            manager: None,
        })
    });
    let mut guard = global.write().map_err(|_| {
        error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "binlog global lock poisoned",
        )
    })?;
    *guard = BinlogGlobal { config, manager };
    Ok(())
}

pub fn config_snapshot() -> BinlogConfig {
    GLOBAL
        .get()
        .and_then(|lock| lock.read().ok().map(|guard| guard.config.clone()))
        .unwrap_or_else(fallback_config)
}

fn fallback_config() -> BinlogConfig {
    FALLBACK_CONFIG.get_or_init(BinlogConfig::default).clone()
}

/// Runtime-mutate a binlog GUC (`SET GLOBAL`). Updates the display source of
/// truth (`GLOBAL.config`, read by `variables_snapshot`/`config_snapshot`) and,
/// for behavioural knobs, the live manager mirror. env stays the startup
/// default (LOB M7 pattern); the change is process-local and lost on restart.
fn update_runtime_config(f: impl FnOnce(&mut BinlogConfig)) -> Result<(), QueryError> {
    let global = GLOBAL.get_or_init(|| {
        RwLock::new(BinlogGlobal {
            config: fallback_config(),
            manager: None,
        })
    });
    let mut guard = global.write().map_err(|_| {
        error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "binlog global lock poisoned",
        )
    })?;
    f(&mut guard.config);
    if let Some(manager) = &guard.manager {
        // Keep the manager's hot-path mirrors in step with the display config.
        manager
            .sync_binlog
            .store(guard.config.sync_binlog, Ordering::Relaxed);
        manager.rotate_bytes.store(
            guard.config.rotate_bytes.unwrap_or(1024 * 1024 * 1024),
            Ordering::Relaxed,
        );
        let ss = &manager.semi_sync;
        if ss
            .enabled
            .swap(guard.config.semi_sync_source_enabled, Ordering::Relaxed)
            != guard.config.semi_sync_source_enabled
        {
            *ss.degraded_at.lock().unwrap() = None;
        }
        ss.timeout_ms
            .store(guard.config.semi_sync_source_timeout, Ordering::Relaxed);
        ss.wait_for_replica_count.store(
            guard.config.semi_sync_source_wait_for_replica_count,
            Ordering::Relaxed,
        );
        ss.wait_no_replica.store(
            guard.config.semi_sync_source_wait_no_replica,
            Ordering::Relaxed,
        );
        // A committer may be parked waiting for a quorum that just shrank (e.g.
        // wait_count lowered or semi-sync disabled); re-evaluate promptly.
        ss.notify_ack();
    }
    Ok(())
}

/// `SET GLOBAL sync_binlog = N` — group-commit fsync cadence for the binary
/// log. Takes effect on the next append without reopening the manager.
pub fn set_sync_binlog(value: u64) -> Result<(), QueryError> {
    update_runtime_config(|config| config.sync_binlog = value)
}

pub fn set_max_binlog_size(value: u64) -> Result<(), QueryError> {
    if !(4096..=1024 * 1024 * 1024).contains(&value) {
        return Err(error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
            "max_binlog_size must be between 4096 and 1073741824 bytes",
        ));
    }
    update_runtime_config(|config| config.rotate_bytes = Some(value))
}

/// `SET GLOBAL log_replica_updates = {ON|OFF}` — whether row changes applied by
/// the MySQL-replica applier are re-logged to this server's binary log (chained
/// replication). Read live via `config_snapshot` on the record path.
pub fn set_log_replica_updates(value: bool) -> Result<(), QueryError> {
    update_runtime_config(|config| config.log_replica_updates = value)
}

/// `SET GLOBAL rpl_semi_sync_source_enabled = {ON|OFF}` (alias
/// `rpl_semi_sync_master_enabled`).
pub fn set_semi_sync_source_enabled(value: bool) -> Result<(), QueryError> {
    update_runtime_config(|config| config.semi_sync_source_enabled = value)
}

/// `SET GLOBAL rpl_semi_sync_source_timeout = N` (milliseconds).
pub fn set_semi_sync_source_timeout(value: u64) -> Result<(), QueryError> {
    update_runtime_config(|config| config.semi_sync_source_timeout = value)
}

/// `SET GLOBAL rpl_semi_sync_source_wait_for_replica_count = N`.
pub fn set_semi_sync_source_wait_for_replica_count(value: u64) -> Result<(), QueryError> {
    update_runtime_config(|config| config.semi_sync_source_wait_for_replica_count = value.max(1))
}

/// `SET GLOBAL rpl_semi_sync_source_wait_no_replica = {ON|OFF}`.
pub fn set_semi_sync_source_wait_no_replica(value: bool) -> Result<(), QueryError> {
    update_runtime_config(|config| config.semi_sync_source_wait_no_replica = value)
}

/// The receiver library supports asynchronous binlog only. Enabling semi-sync
/// must return an explicit error instead of reporting ON without durable ACKs.
static SEMI_SYNC_REPLICA_ENABLED: AtomicBool = AtomicBool::new(false);

pub fn semi_sync_replica_enabled() -> bool {
    SEMI_SYNC_REPLICA_ENABLED.load(Ordering::Relaxed)
}

pub fn set_semi_sync_replica_enabled(value: bool) -> Result<(), QueryError> {
    if value {
        return Err(error(crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "semi-synchronous replication receiver is not supported; mydb receives MySQL binlog asynchronously"));
    }
    SEMI_SYNC_REPLICA_ENABLED.store(false, Ordering::Relaxed);
    Ok(())
}

/// Commit-side semi-sync barrier. Called from the storage commit path once a
/// binlogged transaction's WAL/binlog are durable and its events are visible to
/// the dump loop. No-op unless a manager exists and semi-sync is enabled.
/// Returns the outcome for observability/testing (ignored by the commit path).
pub fn semi_sync_wait_after_commit(file: &str, end_pos: u64) -> SemiSyncOutcome {
    match manager() {
        Some(manager) => manager.semi_sync_wait(file, end_pos),
        None => SemiSyncOutcome::Inactive,
    }
}

/// `SHOW STATUS LIKE 'Rpl_semi_sync%'` rows (master side). Empty when binlog is
/// disabled.
pub fn semi_sync_status_pairs() -> Vec<(String, String)> {
    match manager() {
        Some(manager) => manager.semi_sync_status_pairs(),
        None => Vec::new(),
    }
}

/// Validate a `SET GLOBAL binlog_format = <v>` write. mydb only ever emits
/// row-based events, so `ROW` is an idempotent no-op and anything else is a
/// hard error (aligns with the design boundary; MySQL errno 1231 for a bad
/// system-variable value).
pub fn validate_binlog_format(value: &str) -> Result<(), QueryError> {
    if value.eq_ignore_ascii_case("ROW") {
        Ok(())
    } else {
        Err(error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
            format!(
                "Variable 'binlog_format' can't be set to the value of '{value}': \
                 mydb only supports ROW-based binary logging"
            ),
        ))
    }
}

/// Validate `SET GLOBAL binlog_row_image = <v>`. Only `FULL` is supported
/// (design boundary: no MINIMAL/NOBLOB).
pub fn validate_binlog_row_image(value: &str) -> Result<(), QueryError> {
    if value.eq_ignore_ascii_case("FULL") {
        Ok(())
    } else {
        Err(error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
            format!(
                "Variable 'binlog_row_image' can't be set to the value of '{value}': \
                 mydb only supports FULL row images"
            ),
        ))
    }
}

/// Validate `SET GLOBAL binlog_row_metadata = <v>`. mydb always writes FULL
/// column metadata into TABLE_MAP events.
pub fn validate_binlog_row_metadata(value: &str) -> Result<(), QueryError> {
    if value.eq_ignore_ascii_case("FULL") {
        Ok(())
    } else {
        Err(error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
            format!(
                "Variable 'binlog_row_metadata' can't be set to the value of '{value}': \
                 mydb always writes FULL table metadata"
            ),
        ))
    }
}

/// Validate `SET GLOBAL binlog_row_value_options = <v>`. PARTIAL_JSON is
/// accepted. The writer may legally fall back to a full JSON after-image when
/// it cannot produce a smaller diff, which is the current source-side policy.
pub fn validate_binlog_row_value_options(value: &str) -> Result<(), QueryError> {
    if value.trim().is_empty() || value.trim().eq_ignore_ascii_case("PARTIAL_JSON") {
        Ok(())
    } else {
        Err(error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
            format!(
                "Variable 'binlog_row_value_options' can't be set to the value of '{value}': \
                 allowed values are '' and 'PARTIAL_JSON'"
            ),
        ))
    }
}

/// Validate `SET GLOBAL binlog_transaction_compression = <v>`. Only `OFF` is
/// supported for now (writing TransactionPayloadEvent is future work; the
/// replica side can already decompress).
pub fn validate_binlog_transaction_compression(value: &str) -> Result<(), QueryError> {
    if value.eq_ignore_ascii_case("OFF") || value == "0" || value.eq_ignore_ascii_case("FALSE") {
        Ok(())
    } else {
        Err(error(
            crate::error::catalog::ER_WRONG_VALUE_FOR_VAR,
            format!(
                "Variable 'binlog_transaction_compression' can't be set to the value of '{value}': \
                 mydb does not yet generate compressed transaction payloads"
            ),
        ))
    }
}

pub fn enabled() -> bool {
    manager().is_some()
}

pub fn should_record_table_rows(table: &TableSchema) -> bool {
    !should_skip_recording() && !crate::storage::is_temp_table_id(table.id)
}

pub fn manager() -> Option<Arc<BinlogSourceManager>> {
    GLOBAL
        .get()
        .and_then(|lock| lock.read().ok().and_then(|guard| guard.manager.clone()))
}

pub fn require_manager() -> Result<Arc<BinlogSourceManager>, QueryError> {
    manager().ok_or_else(disabled_error)
}

pub fn commit_gate() -> parking_lot::MutexGuard<'static, ()> {
    COMMIT_GATE
        .get_or_init(|| parking_lot::Mutex::new(()))
        .lock()
}

pub fn disabled_error() -> QueryError {
    error(
        crate::error::catalog::ER_NOT_SUPPORTED_YET,
        "binary logging is disabled",
    )
}

pub fn current_position() -> Result<BinlogPosition, QueryError> {
    require_manager()?.current_position()
}

pub(crate) fn current_status_snapshot() -> Result<BinlogStatusSnapshot, QueryError> {
    Ok(require_manager()?.status_snapshot())
}

/// The caller holds COMMIT_GATE, preventing new byte/SCN reservations.
/// Earlier committers finish WAL and publication without that gate.
pub(crate) fn drain_commits_for_snapshot() -> Result<(), QueryError> {
    let manager = require_manager()?;
    manager.flush_all_staged_writes()?;
    let barrier = manager.commit_order.state.lock().next_alloc;
    manager.commit_order.wait_publish(barrier)
}

pub fn binary_logs() -> Result<Vec<BinaryLogInfo>, QueryError> {
    require_manager()?.binary_logs()
}

pub fn variables_snapshot() -> Vec<(&'static str, String)> {
    let config = config_snapshot();
    let (gtid_executed, gtid_purged) = manager()
        .map(|manager| {
            let state = manager.state.lock();
            (state.gtid_executed.clone(), state.gtid_purged.clone())
        })
        .unwrap_or_default();
    vec![
        ("log_bin", if enabled() { "ON" } else { "OFF" }.to_string()),
        ("binlog_format", "ROW".to_string()),
        ("binlog_row_image", "FULL".to_string()),
        ("binlog_row_metadata", "FULL".to_string()),
        ("binlog_row_value_options", String::new()),
        ("binlog_transaction_compression", "OFF".to_string()),
        (
            "binlog_checksum",
            config.checksum.as_mysql_value().to_string(),
        ),
        (
            "binlog_expire_logs_seconds",
            config
                .retention_hours
                .map(|hours| hours.saturating_mul(3600))
                .unwrap_or(0)
                .to_string(),
        ),
        ("sync_binlog", config.sync_binlog.to_string()),
        (
            "max_binlog_size",
            config
                .rotate_bytes
                .unwrap_or(1024 * 1024 * 1024)
                .to_string(),
        ),
        (
            "expire_logs_days",
            (config.retention_hours.unwrap_or(0) / 24).to_string(),
        ),
        ("max_binlog_cache_size", i64::MAX.to_string()),
        ("max_binlog_stmt_cache_size", i64::MAX.to_string()),
        (
            "log_replica_updates",
            if config.log_replica_updates {
                "ON"
            } else {
                "OFF"
            }
            .to_string(),
        ),
        ("server_id", config.server_id.to_string()),
        ("server_uuid", config.server_uuid),
        (
            "gtid_mode",
            if enabled() { "ON" } else { "OFF" }.to_string(),
        ),
        (
            "enforce_gtid_consistency",
            if enabled() { "ON" } else { "OFF" }.to_string(),
        ),
        ("gtid_executed", gtid_executed),
        ("gtid_purged", gtid_purged),
        (
            "rpl_semi_sync_source_enabled",
            on_off(config.semi_sync_source_enabled),
        ),
        (
            "rpl_semi_sync_master_enabled",
            on_off(config.semi_sync_source_enabled),
        ),
        (
            "rpl_semi_sync_source_timeout",
            config.semi_sync_source_timeout.to_string(),
        ),
        (
            "rpl_semi_sync_master_timeout",
            config.semi_sync_source_timeout.to_string(),
        ),
        (
            "rpl_semi_sync_source_wait_for_replica_count",
            config.semi_sync_source_wait_for_replica_count.to_string(),
        ),
        (
            "rpl_semi_sync_master_wait_for_slave_count",
            config.semi_sync_source_wait_for_replica_count.to_string(),
        ),
        (
            "rpl_semi_sync_source_wait_no_replica",
            on_off(config.semi_sync_source_wait_no_replica),
        ),
        (
            "rpl_semi_sync_master_wait_no_slave",
            on_off(config.semi_sync_source_wait_no_replica),
        ),
    ]
}

fn on_off(v: bool) -> String {
    if v { "ON" } else { "OFF" }.to_string()
}

pub fn server_id() -> u32 {
    config_snapshot().server_id
}

/// Wait before executing any write effects. The ownership mutex and condvar
/// share the predicate, so releasing the last owner cannot lose a wakeup.
/// The owner itself must fail instead of waiting for its own UNLOCK TABLES.
pub(crate) fn wait_for_global_read_lock_scoped(
    statement: &Statement,
    scope: u64,
    session: &SessionContext,
) -> Result<(), QueryError> {
    if !GLOBAL_READ_LOCK.load(Ordering::Acquire)
        || crate::replication::replication_applier_active()
        || !crate::replication::is_replica_write_statement(statement)
    {
        return Ok(());
    }
    let started = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(
        session
            .sysvars
            .lock_wait_timeout
            .unwrap_or_else(crate::engine::api::global_lock_wait_timeout),
    );
    let _stage = crate::engine::cancel::enter_process_stage(
        crate::engine::session_registry::ProcessState::WaitingGlobalMetadataLock,
    );
    let mut owners = GLOBAL_READ_LOCK_OWNERS.lock();
    loop {
        session.check_cancelled()?;
        let mut blocked = false;
        for &(held_scope, owner) in owners.iter() {
            if held_scope == 0 || scope == 0 || held_scope == scope {
                if owner == session.connection_id {
                    return global_read_lock_error();
                }
                blocked = true;
            }
        }
        if !blocked {
            return Ok(());
        }
        let remaining = timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(error(
                crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                "Lock wait timeout exceeded waiting for global read lock",
            ));
        }
        // Cancellation has its own signal; bound its latency without busy polling.
        GLOBAL_READ_LOCK_CHANGED.wait_for(
            &mut owners,
            remaining.min(std::time::Duration::from_millis(50)),
        );
    }
}

fn global_read_lock_error() -> Result<(), QueryError> {
    Err(error(
        crate::error::catalog::ER_CANT_UPDATE_WITH_READLOCK,
        "Can't execute the statement because FLUSH TABLES WITH READ LOCK is active",
    ))
}

pub fn unlock_tables_for_session_scoped(session: &SessionContext, scope: u64) -> bool {
    if !GLOBAL_READ_LOCK.load(Ordering::Acquire) {
        return false;
    }
    let mut owners = GLOBAL_READ_LOCK_OWNERS.lock();
    let before = owners.len();
    owners.retain(|(held_scope, owner)| {
        !((scope == 0 || *held_scope == 0 || *held_scope == scope)
            && *owner == session.connection_id)
    });
    GLOBAL_READ_LOCK.store(!owners.is_empty(), Ordering::Release);
    let released = owners.len() != before;
    if released {
        GLOBAL_READ_LOCK_CHANGED.notify_all();
    }
    released
}

#[cfg(test)]
fn test_reset_wal_boundary(manager: &BinlogSourceManager) -> Result<u64, QueryError> {
    let log = Arc::new(crate::wal::LogMutex::new(crate::wal::LogManager::open(
        manager.dir.parent().unwrap(),
    )?));
    crate::wal::LogManager::sync_allocation_boundary(&log)
}

#[cfg(test)]
pub fn handle_admin_sql(sql: &str, session: &SessionContext) -> Result<QueryResponse, QueryError> {
    handle_admin_sql_scoped(sql, session, 0, || {
        test_reset_wal_boundary(require_manager()?.as_ref())
    })
}

pub fn handle_admin_sql_scoped(
    sql: &str,
    session: &SessionContext,
    scope: u64,
    wal_boundary: impl FnOnce() -> Result<u64, QueryError>,
) -> Result<QueryResponse, QueryError> {
    match AdminCommand::parse(sql)? {
        AdminCommand::MasterStatus => show_master_status(session),
        AdminCommand::BinaryLogs => show_binary_logs(),
        AdminCommand::Replicas => show_replicas(),
        AdminCommand::FlushBinaryLogs => {
            let _commit_gate = commit_gate();
            require_manager()?.rotate()?;
            Ok(QueryResponse::Ok)
        }
        AdminCommand::ResetMaster => {
            let _commit_gate = commit_gate();
            require_manager()?.reset_master(wal_boundary)?;
            Ok(QueryResponse::Ok)
        }
        AdminCommand::LockTables => {
            let mut owners = GLOBAL_READ_LOCK_OWNERS.lock();
            owners.insert((scope, session.connection_id));
            GLOBAL_READ_LOCK.store(true, Ordering::Release);
            Ok(QueryResponse::Ok)
        }
        AdminCommand::UnlockTables => {
            unlock_tables_for_session_scoped(session, scope);
            Ok(QueryResponse::Ok)
        }
        // MySQL 5.7+: SHOW PROFILES / SHOW PROFILE are deprecated. mydb has no
        // statement profiler — return an empty result set with the canonical
        // column shape so clients (and JDBC metadata probes) parse cleanly.
        AdminCommand::Profiles => Ok(empty_show_profiles_result()),
        AdminCommand::Profile => Ok(empty_show_profile_result()),
        // SHOW BINLOG EVENTS uses the bounded historical/live event reader.
        AdminCommand::BinlogEvents => show_binlog_events(sql),
        AdminCommand::PurgeTo(prefix) => {
            let target = unquote_admin_string(sql, prefix)?;
            require_manager()?.purge_to(&target)?;
            Ok(QueryResponse::Ok)
        }
        AdminCommand::PurgeBefore(prefix) => {
            let target = unquote_admin_string(sql, prefix)?;
            require_manager()?.purge_before(&target)?;
            Ok(QueryResponse::Ok)
        }
    }
}

/// Empty result set matching MySQL's `SHOW PROFILES` shape.
fn empty_show_profiles_result() -> QueryResponse {
    QueryResponse::ResultSet(ResultSet {
        columns: AdminCommand::Profiles.columns(),
        rows: Vec::new(),
    })
}

/// Empty result set matching MySQL's `SHOW PROFILE` shape.
fn empty_show_profile_result() -> QueryResponse {
    QueryResponse::ResultSet(ResultSet {
        columns: AdminCommand::Profile.columns(),
        rows: Vec::new(),
    })
}

/// Parse and execute `SHOW BINLOG EVENTS [IN 'file'] [FROM pos] [LIMIT [offset,] n]`.
/// Uses the same bounded historical/live reader as `COM_BINLOG_DUMP`.
fn show_binlog_events(sql: &str) -> Result<QueryResponse, QueryError> {
    const MAX_SHOW_EVENTS: usize = 100_000;
    const MAX_SHOW_BYTES: usize = 64 * 1024 * 1024;
    let req = parse_show_binlog_events_clause(sql)?;
    // If binlog is disabled, MySQL returns an empty result set rather than
    // an error — match that for JDBC metadata probes.
    let Some(manager) = manager() else {
        return Ok(QueryResponse::ResultSet(ResultSet {
            columns: show_binlog_events_columns(),
            rows: Vec::new(),
        }));
    };
    if req.limit == Some(0) {
        return Ok(QueryResponse::ResultSet(ResultSet {
            columns: show_binlog_events_columns(),
            rows: Vec::new(),
        }));
    }
    let requested = req.offset.saturating_add(req.limit.unwrap_or(usize::MAX));
    let (chunks, more) = manager.show_events_bounded(
        &req.file,
        req.from,
        requested.min(MAX_SHOW_EVENTS.saturating_add(1)),
        MAX_SHOW_BYTES,
    )?;
    let chunk_bytes = chunks
        .iter()
        .fold(0usize, |sum, chunk| sum.saturating_add(chunk.bytes.len()));
    if chunks.len() > MAX_SHOW_EVENTS
        || chunk_bytes > MAX_SHOW_BYTES
        || (more && (chunks.len() < requested || req.limit.is_none()))
    {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "SHOW BINLOG EVENTS result exceeds 100000 events or 64 MiB; use LIMIT to request a smaller range",
        ));
    }
    let mut rows = Vec::with_capacity(chunks.len());
    let skip = req.offset;
    let limit = req.limit.unwrap_or(usize::MAX);
    for (idx, chunk) in chunks.into_iter().enumerate() {
        if idx < skip {
            continue;
        }
        if rows.len() >= limit {
            break;
        }
        rows.push(vec![
            Value::String(chunk.file.clone()),
            Value::UInt(chunk.start_pos),
            Value::String(binlog_event_type_name(chunk.event_type).to_string()),
            Value::UInt(read_event_server_id(&chunk.bytes) as u64),
            Value::UInt(chunk.end_pos),
            Value::String(format_binlog_event_info(&chunk, manager.config.checksum)),
        ]);
    }
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: show_binlog_events_columns(),
        rows,
    }))
}

fn show_binlog_events_columns() -> Vec<Column> {
    AdminCommand::BinlogEvents.columns()
}

#[derive(Debug)]
struct ShowBinlogEventsRequest {
    file: String,
    from: u64,
    offset: usize,
    limit: Option<usize>,
}

/// Parser for `SHOW BINLOG EVENTS [IN 'file'] [FROM pos] [LIMIT [offset,] n]`.
/// Token-stream based to stay close to MySQL's permissive whitespace handling.
fn parse_show_binlog_events_clause(sql: &str) -> Result<ShowBinlogEventsRequest, QueryError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("SHOW BINLOG EVENTS") {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "expected SHOW BINLOG EVENTS prefix",
        ));
    }
    let mut tail = trimmed["SHOW BINLOG EVENTS".len()..].trim();
    let mut req = ShowBinlogEventsRequest {
        file: String::new(),
        from: 4,
        offset: 0,
        limit: None,
    };
    let upper_tail = tail.to_ascii_uppercase();
    if let Some(rest) = upper_tail.strip_prefix("IN ") {
        let pos_after_in = tail.len() - rest.len();
        let value_part = tail[pos_after_in..].trim();
        let (quoted, after) = extract_quoted_string(value_part)?;
        req.file = quoted;
        tail = after.trim();
    }
    let upper_tail = tail.to_ascii_uppercase();
    if let Some(rest) = upper_tail.strip_prefix("FROM ") {
        let pos_after_from = tail.len() - rest.len();
        let value_part = tail[pos_after_from..].trim();
        let (num_str, after) = take_number(value_part)?;
        req.from = num_str.parse::<u64>().map_err(|_| {
            error(
                crate::error::catalog::ER_PARSE_ERROR,
                format!("invalid FROM position '{num_str}'"),
            )
        })?;
        tail = after.trim();
    }
    let upper_tail = tail.to_ascii_uppercase();
    if let Some(rest) = upper_tail.strip_prefix("LIMIT ") {
        let pos_after_limit = tail.len() - rest.len();
        let limit_part = tail[pos_after_limit..].trim();
        let (first, after) = take_number(limit_part)?;
        let after = after.trim();
        if after.starts_with(',') {
            let after_comma = after[1..].trim();
            let (second, after2) = take_number(after_comma)?;
            req.offset = first.parse::<usize>().map_err(|_| {
                error(
                    crate::error::catalog::ER_PARSE_ERROR,
                    format!("invalid LIMIT offset '{first}'"),
                )
            })?;
            req.limit = Some(second.parse::<usize>().map_err(|_| {
                error(
                    crate::error::catalog::ER_PARSE_ERROR,
                    format!("invalid LIMIT count '{second}'"),
                )
            })?);
            tail = after2.trim();
        } else {
            req.limit = Some(first.parse::<usize>().map_err(|_| {
                error(
                    crate::error::catalog::ER_PARSE_ERROR,
                    format!("invalid LIMIT '{first}'"),
                )
            })?);
            tail = after;
        }
    }
    if !tail.is_empty() {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            format!("unexpected trailing tokens in SHOW BINLOG EVENTS: '{tail}'"),
        ));
    }
    if req.from < 4 {
        req.from = 4;
    }
    Ok(req)
}

fn extract_quoted_string(input: &str) -> Result<(String, &str), QueryError> {
    let bytes = input.as_bytes();
    if bytes.is_empty() {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "expected quoted string",
        ));
    }
    let quote = bytes[0];
    if quote != b'\'' && quote != b'"' && quote != b'`' {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            format!(
                "expected quoted string, saw '{}'",
                input.chars().next().unwrap_or('?')
            ),
        ));
    }
    let mut end = 1;
    while end < bytes.len() {
        if bytes[end] == quote {
            let value = std::str::from_utf8(&bytes[1..end])
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_PARSE_ERROR,
                        "string is not valid UTF-8",
                    )
                })?
                .to_string();
            return Ok((value, &input[end + 1..]));
        }
        end += 1;
    }
    Err(error(
        crate::error::catalog::ER_PARSE_ERROR,
        "unterminated quoted string",
    ))
}

fn take_number(input: &str) -> Result<(&str, &str), QueryError> {
    let bytes = input.as_bytes();
    let mut end = 0;
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    if end == 0 {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            format!(
                "expected an integer, saw '{}'",
                &input[..input.len().min(8)]
            ),
        ));
    }
    Ok((&input[..end], &input[end..]))
}

fn binlog_event_type_name(event_type: u8) -> &'static str {
    match event_type {
        0x00 => "Unknown",
        0x01 => "Start_v3",
        0x02 => "Query",
        0x03 => "Stop",
        0x04 => "Rotate",
        0x05 => "Intvar",
        0x06 => "Load",
        0x07 => "Slave",
        0x08 => "Create_file",
        0x09 => "Append_block",
        0x0a => "Exec_load",
        0x0b => "Delete_file",
        0x0c => "New_load",
        0x0d => "Rand",
        0x0e => "User_var",
        0x0f => "Format_desc",
        0x10 => "Xid",
        0x11 => "Begin_load_query",
        0x12 => "Execute_load_query",
        0x13 => "Table_map",
        0x14 => "Write_rows_v1",
        0x15 => "Update_rows_v1",
        0x16 => "Delete_rows_v1",
        0x17 => "Incident",
        0x18 => "Heartbeat_v1",
        0x19 => "Ignorable",
        0x1a => "Rows_query",
        0x1b => "Heartbeat",
        0x1c => "Ignorable",
        0x1d => "Rows_query",
        0x1e => "Write_rows",
        0x1f => "Update_rows",
        0x20 => "Delete_rows",
        0x21 => "Gtid",
        0x22 => "Anonymous_Gtid",
        0x23 => "Previous_gtids",
        0x24 => "Transaction_payload",
        _ => "Unknown",
    }
}

/// Read the `server_id` field out of the 19-byte common event header.
/// Layout: `timestamp(4) + event_type(1) + server_id(4) + event_size(4) + log_pos(4) + flags(2)`.
fn read_event_server_id(bytes: &[u8]) -> u32 {
    if bytes.len() < 9 {
        return 0;
    }
    u32::from_le_bytes([bytes[5], bytes[6], bytes[7], bytes[8]])
}

/// One-line `Info` cell mimicking `SHOW BINLOG EVENTS` formatting.
/// Pure cosmetics — clients show this as the human-readable description of
/// the event ("Format_desc: binlog v4", "GTID: <uuid>:<gno>", "Query: BEGIN", …).
fn format_binlog_event_info(chunk: &BinlogEventChunk, checksum: BinlogChecksum) -> String {
    match chunk.event_type {
        FORMAT_DESCRIPTION_EVENT => format!("Server ver: {} ... binlog v4", MYSQL_SERVER_VERSION),
        ROTATE_EVENT => {
            // Payload: 8 bytes pos + filename
            if chunk.bytes.len() >= EVENT_HEADER_LEN as usize + 8 {
                let payload = &chunk.bytes[EVENT_HEADER_LEN as usize..];
                let pos = u64::from_le_bytes(payload[..8].try_into().unwrap_or([0; 8]));
                let name = String::from_utf8_lossy(trim_trailing_checksum(&payload[8..], checksum));
                format!("Rotate to {name} pos {pos}")
            } else {
                "Rotate".to_string()
            }
        }
        QUERY_EVENT => {
            // Skip header(19), thread_id(4), exec_time(4), db_len(1), error_code(2), status_vars_len(2)
            let off = EVENT_HEADER_LEN as usize + 4 + 4 + 1 + 2 + 2;
            if chunk.bytes.len() <= off {
                return "Query".to_string();
            }
            let svl = u16::from_le_bytes([
                chunk.bytes[EVENT_HEADER_LEN as usize + 11],
                chunk.bytes[EVENT_HEADER_LEN as usize + 12],
            ]) as usize;
            let db_len = chunk.bytes[EVENT_HEADER_LEN as usize + 8] as usize;
            let sql_off = off + svl + db_len + 1;
            if chunk.bytes.len() <= sql_off {
                return "Query".to_string();
            }
            let body = &chunk.bytes[sql_off..];
            let trimmed = trim_trailing_checksum(body, checksum);
            let charset = query_event_client_charset(&chunk.bytes[off..off + svl])
                .unwrap_or(crate::charset::DEFAULT_CHARSET);
            crate::charset::decode_bytes(charset, trimmed)
                .unwrap_or_else(|_| "Query (invalid character encoding)".to_string())
        }
        GTID_EVENT => chunk
            .gtid
            .clone()
            .map(|s| format!("SET @@SESSION.GTID_NEXT='{}'", s))
            .unwrap_or_else(|| "Gtid".to_string()),
        ANONYMOUS_GTID_EVENT => "SET @@SESSION.GTID_NEXT='ANONYMOUS'".to_string(),
        XID_EVENT => {
            if chunk.bytes.len() >= EVENT_HEADER_LEN as usize + 8 {
                let payload = &chunk.bytes[EVENT_HEADER_LEN as usize..];
                let xid = u64::from_le_bytes(payload[..8].try_into().unwrap_or([0; 8]));
                format!("COMMIT /* xid={xid} */")
            } else {
                "COMMIT".to_string()
            }
        }
        TABLE_MAP_EVENT => "table_map".to_string(),
        WRITE_ROWS_EVENT_V2 => "write_rows".to_string(),
        UPDATE_ROWS_EVENT_V2 => "update_rows".to_string(),
        DELETE_ROWS_EVENT_V2 => "delete_rows".to_string(),
        HEARTBEAT_EVENT => "Heartbeat".to_string(),
        PREVIOUS_GTIDS_EVENT => "previous_gtids".to_string(),
        _ => String::new(),
    }
}

fn trim_trailing_checksum(body: &[u8], checksum: BinlogChecksum) -> &[u8] {
    if checksum == BinlogChecksum::Crc32 && body.len() >= 4 {
        &body[..body.len() - 4]
    } else {
        body
    }
}

/// Decode the prefix emitted by encode_query_status_vars without requiring
/// the optional MySQL client dependency in a source-only build.
fn query_event_client_charset(mut status: &[u8]) -> Option<&'static str> {
    while let Some((&code, rest)) = status.split_first() {
        let skip = match code {
            0 => 4,                               // Q_FLAGS2
            1 => 8,                               // Q_SQL_MODE
            3 => 4,                               // Q_AUTO_INCREMENT
            6 => usize::from(*rest.first()?) + 1, // Q_CATALOG_NZ
            4 => {
                let id = u16::from_le_bytes(rest.get(..2)?.try_into().ok()?);
                return crate::charset::resolve_collation_id(id).map(|info| info.charset);
            }
            _ => return None,
        };
        status = rest.get(skip..)?;
    }
    None
}

/// Privilege mapping shares the executor's command classifier, including aliases.
pub(crate) fn admin_privilege(sql: &str) -> Option<crate::engine::auth::Privilege> {
    use crate::engine::auth::Privilege;
    Some(match AdminCommand::classify(sql)? {
        AdminCommand::MasterStatus | AdminCommand::BinaryLogs => Privilege::ReplClient,
        AdminCommand::BinlogEvents | AdminCommand::Replicas => Privilege::ReplSlave,
        AdminCommand::PurgeTo(_) | AdminCommand::PurgeBefore(_) => {
            Privilege::Dynamic("BINLOG_ADMIN".into())
        }
        AdminCommand::ResetMaster | AdminCommand::FlushBinaryLogs | AdminCommand::LockTables => {
            Privilege::Reload
        }
        AdminCommand::UnlockTables | AdminCommand::Profiles | AdminCommand::Profile => return None,
    })
}

pub fn is_binlog_admin_sql(sql: &str) -> bool {
    // Head guard — only these keywords ever start a binlog admin command.
    // Without this gate `normalize_admin_sql` allocated two full-payload
    // copies of `sql` (split+join + upper) just to discover the head was
    // something like INSERT.
    let (head_buf, head_len) = crate::parser::extract_first_keyword(sql);
    if !matches!(
        &head_buf[..head_len],
        b"SHOW" | b"FLUSH" | b"UNLOCK" | b"PURGE" | b"RESET"
    ) {
        return false;
    }
    AdminCommand::classify(sql).is_some()
}

pub fn parse_binlog_dump_request(payload: &[u8]) -> Result<BinlogDumpRequest, QueryError> {
    if payload.len() < 10 {
        return Err(error(
            crate::error::catalog::ER_UNKNOWN_COM_ERROR,
            "bad COM_BINLOG_DUMP packet",
        ));
    }
    let position = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as u64;
    let flags = u16::from_le_bytes(payload[4..6].try_into().unwrap());
    // MySQL replicas also send 0x0002 in file/position mode. Only NON_BLOCK
    // affects this command; tolerate other bits as the source protocol does.
    let slave_server_id = u32::from_le_bytes(payload[6..10].try_into().unwrap());
    let raw_file = payload[10..].strip_suffix(&[0]).unwrap_or(&payload[10..]);
    let file = std::str::from_utf8(raw_file)
        .map_err(|_| {
            error(
                crate::error::catalog::ER_UNKNOWN_COM_ERROR,
                "COM_BINLOG_DUMP file name is not valid UTF-8",
            )
        })?
        .to_string();
    validate_dump_file_name(&file)?;
    Ok(BinlogDumpRequest {
        file,
        position,
        flags,
        slave_server_id,
    })
}

pub fn parse_binlog_dump_gtid_request(payload: &[u8]) -> Result<BinlogDumpGtidRequest, QueryError> {
    if payload.len() < 2 + 4 + 4 + 8 + 4 {
        return Err(error(
            crate::error::catalog::ER_UNKNOWN_COM_ERROR,
            "bad COM_BINLOG_DUMP_GTID packet",
        ));
    }
    let mut pos = 0usize;
    let flags = read_u16_le(payload, &mut pos)?;
    const GTID_ALLOWED_FLAGS: u16 = 0x0001 | 0x0002 | 0x0004;
    if flags & !GTID_ALLOWED_FLAGS != 0 {
        return Err(error(
            crate::error::catalog::ER_UNKNOWN_COM_ERROR,
            format!("unsupported COM_BINLOG_DUMP_GTID flags 0x{flags:04x}"),
        ));
    }
    let slave_server_id = read_u32_le(payload, &mut pos)?;
    let file_len = read_u32_le(payload, &mut pos)? as usize;
    if payload.len().saturating_sub(pos) < file_len + 8 + 4 {
        return Err(error(
            crate::error::catalog::ER_UNKNOWN_COM_ERROR,
            "bad COM_BINLOG_DUMP_GTID packet",
        ));
    }
    let file = std::str::from_utf8(&payload[pos..pos + file_len])
        .map_err(|_| {
            error(
                crate::error::catalog::ER_UNKNOWN_COM_ERROR,
                "COM_BINLOG_DUMP_GTID file name is not valid UTF-8",
            )
        })?
        .to_string();
    validate_dump_file_name(&file)?;
    pos += file_len;
    let position = read_u64_le(payload, &mut pos)?;
    let sid_data_len = read_u32_le(payload, &mut pos)? as usize;
    if payload.len().saturating_sub(pos) < sid_data_len {
        return Err(error(
            crate::error::catalog::ER_UNKNOWN_COM_ERROR,
            "bad COM_BINLOG_DUMP_GTID SID block",
        ));
    }
    if payload.len().saturating_sub(pos) != sid_data_len {
        return Err(error(
            crate::error::catalog::ER_UNKNOWN_COM_ERROR,
            "COM_BINLOG_DUMP_GTID packet has unexpected trailing bytes",
        ));
    }
    let executed_gtid_set = parse_sid_block_to_gtid_set(&payload[pos..pos + sid_data_len])?;
    Ok(BinlogDumpGtidRequest {
        file,
        position,
        flags,
        slave_server_id,
        executed_gtid_set,
    })
}

fn validate_dump_file_name(file: &str) -> Result<(), QueryError> {
    if file.len() > 512
        || file.contains('/')
        || file.contains('\\')
        || file.contains('\0')
        || file == "."
        || file == ".."
    {
        return Err(error(
            crate::error::catalog::ER_UNKNOWN_COM_ERROR,
            format!("invalid binary log file name '{file}'"),
        ));
    }
    Ok(())
}

pub fn record_write_rows(
    txn_id: u64,
    table: &TableSchema,
    rows: Vec<Vec<Value>>,
) -> Result<(), QueryError> {
    if should_skip_recording() || rows.is_empty() || crate::storage::is_temp_table_id(table.id) {
        return Ok(());
    }
    let manager = require_manager()?;
    let table = binlog_table_from_schema(table)?;
    let rows = rows
        .into_iter()
        .map(|row| project_visible_row(&table, row))
        .collect();
    manager.push_op(txn_id, BinlogTxnOp::Write { table, rows })
}

pub fn record_update_rows(
    txn_id: u64,
    table: &TableSchema,
    rows: Vec<(Vec<Value>, Vec<Value>)>,
) -> Result<(), QueryError> {
    if should_skip_recording() || rows.is_empty() || crate::storage::is_temp_table_id(table.id) {
        return Ok(());
    }
    let manager = require_manager()?;
    let table = binlog_table_from_schema(table)?;
    let rows = rows
        .into_iter()
        .map(|(before, after)| {
            (
                project_visible_row(&table, before),
                project_visible_row(&table, after),
            )
        })
        .collect();
    manager.push_op(txn_id, BinlogTxnOp::Update { table, rows })
}

pub fn record_delete_rows(
    txn_id: u64,
    table: &TableSchema,
    rows: Vec<Vec<Value>>,
) -> Result<(), QueryError> {
    if should_skip_recording() || rows.is_empty() || crate::storage::is_temp_table_id(table.id) {
        return Ok(());
    }
    let manager = require_manager()?;
    let table = binlog_table_from_schema(table)?;
    let rows = rows
        .into_iter()
        .map(|row| project_visible_row(&table, row))
        .collect();
    manager.push_op(txn_id, BinlogTxnOp::Delete { table, rows })
}

#[cfg(test)]
pub fn record_ddl_autocommit(database: &str, sql: &str) -> Result<(), QueryError> {
    record_ddl_autocommit_with(database, sql, &BinlogQueryContext::default())
}

pub fn record_ddl_autocommit_with(
    database: &str,
    sql: &str,
    ctx: &BinlogQueryContext,
) -> Result<(), QueryError> {
    if should_skip_recording() {
        return Ok(());
    }
    let manager = require_manager()?;
    let _commit_gate = commit_gate();
    manager.append_query_event(database, sql, ctx)
}

/// Stage a DDL query event inside an existing row transaction. Atomic CTAS
/// uses this so the resolved CREATE, WRITE_ROWS payload and XID are published
/// as one crash-consistent binlog transaction instead of relying on a later
/// compensating DROP.
pub fn record_ddl_transactional_with(
    txn_id: u64,
    database: &str,
    sql: &str,
    ctx: &BinlogQueryContext,
) -> Result<(), QueryError> {
    if should_skip_recording() {
        return Ok(());
    }
    let manager = require_manager()?;
    manager.push_op(
        txn_id,
        BinlogTxnOp::Query {
            database: database.to_string(),
            sql: sql.to_string(),
            ctx: ctx.clone(),
        },
    )
}

pub fn stage_txn_commit(txn_id: u64, commit_scn: u64) -> Result<Option<BinlogCommit>, QueryError> {
    let Some(manager) = manager() else {
        return Ok(None);
    };
    manager.stage_txn_commit(txn_id, commit_scn)
}

/// Append a staged transaction's encoded binlog bytes, and make them durable
/// according to `sync_binlog`. The group coordinator performs one vectored
/// write pass and at most one configured durability sync per file for all
/// commits in the same batch.
pub fn sync_staged_txn_commit(commit: Option<&BinlogCommit>) -> Result<(), QueryError> {
    let Some(commit) = commit else {
        return Ok(());
    };
    let manager = require_manager()?;
    manager.sync_staged_commit(commit)
}

/// Run the WAL append for a binlogged commit in the same order in which its
/// bytes were appended to the binlog. The turn covers only in-memory/WAL
/// append work; fsync remains outside it so WAL group commit can batch.
pub fn with_wal_commit_order<T>(
    commit: Option<&BinlogCommit>,
    append: impl FnOnce() -> Result<T, QueryError>,
) -> Result<T, QueryError> {
    let Some(commit) = commit else {
        return append();
    };
    let manager = require_manager()?;
    manager.commit_order.wait_wal(commit.order)?;
    let result = append();
    if let Err(err) = &result {
        manager.commit_order.fail(err.message().to_string());
    }
    manager.commit_order.finish_wal(commit.order);
    result
}

pub fn publish_txn_commit(commit: Option<BinlogCommit>) -> Result<(), QueryError> {
    if commit.is_none() {
        return Ok(());
    }
    require_manager()?.publish_txn_commit(commit)
}

/// Persist a contiguous, published binlog prefix before checkpoint WAL
/// compaction can discard the corresponding `WalBinlogRef` records.
///
/// `COMMIT_GATE` is held only while capturing the logical tail and allocating
/// the publish barrier. Holding it while waiting would stop a committer that
/// has not reserved its position yet and can turn a background checkpoint
/// into a global write stall. Transactions that already reserved a position
/// have a lower commit-order token and must publish (which happens only after
/// their WAL commit is durable) before the barrier advances. Later
/// transactions may reserve bytes concurrently, but cannot publish across the
/// barrier and their WAL references are newer than CheckpointBegin.
pub fn checkpoint_manifest() -> Result<(), QueryError> {
    let Some(manager) = manager() else {
        return Ok(());
    };
    let target = {
        let _commit_gate = commit_gate();
        manager.checkpoint_manifest_target()
    };
    manager.persist_checkpoint_manifest(target)
}

pub fn commit_txn(txn_id: u64, commit_scn: u64) -> Result<(), QueryError> {
    let Some(manager) = manager() else {
        return Ok(());
    };
    manager.commit_txn(txn_id, commit_scn)
}

pub fn rollback_txn(txn_id: u64) {
    if let Some(manager) = manager() {
        manager.rollback_txn(txn_id);
    }
}

pub fn txn_len(txn_id: u64) -> usize {
    manager().map(|m| m.txn_len(txn_id)).unwrap_or(0)
}

pub fn truncate_txn(txn_id: u64, len: usize) {
    if let Some(manager) = manager() {
        manager.truncate_txn(txn_id, len);
    }
}

/// Called with the database directory lock held, before startup can prune WAL.
/// Opening publishes WAL-proven positions and GTIDs atomically to the manifest.
pub(crate) fn recover_before_checkpoint(data_dir: &Path) -> Result<(), QueryError> {
    if let Some(manifest) = read_manifest_file(&data_dir.join("binlog/manifest.json"))? {
        let _ = BinlogSourceManager::open(data_dir.to_path_buf(), manifest.config)?;
    }
    Ok(())
}

impl BinlogSourceManager {
    fn open(data_dir: PathBuf, mut config: BinlogConfig) -> Result<Self, QueryError> {
        let dir = data_dir.join("binlog");
        fs::create_dir_all(&dir).map_err(io_error)?;
        let index_path = dir.join(format!("{}.index", config.base_name));
        let manifest_path = dir.join("manifest.json");
        finish_interrupted_reset(&dir, &config)?;
        let manifest = read_manifest_file(&manifest_path)?;
        if manifest
            .as_ref()
            .is_some_and(|m| m.config.checksum != config.checksum)
        {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "binlog checksum differs from the existing manifest; restart with the stored checksum algorithm",
            ));
        }
        if crate::util::env::var("MYDB_SERVER_UUID").is_err() {
            if let Some(stored) = manifest
                .as_ref()
                .map(|m| m.config.server_uuid.trim())
                .filter(|uuid| !uuid.is_empty())
            {
                config.server_uuid = stored.to_string();
            }
        }
        let active_file = manifest
            .as_ref()
            .map(|m| m.active_file.clone())
            .unwrap_or_else(|| format!("{}.000001", config.base_name));
        let mut file_names = read_index_file(&index_path)?;
        let new_log = file_names.is_empty() && manifest.is_none();
        if file_names.is_empty() && manifest.is_none() {
            file_names.push(active_file.clone());
        }
        let active_file = if manifest.is_none() {
            file_names.last().cloned().unwrap_or(active_file)
        } else {
            active_file
        };
        if file_names.last() != Some(&active_file) {
            return Err(error(crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
                format!("binlog index '{}' does not end at manifest active file '{active_file}'; restore the index before startup", index_path.display())));
        }
        let indexed: HashSet<&str> = file_names.iter().map(String::as_str).collect();
        let prefix = format!("{}.", config.base_name);
        for entry in fs::read_dir(&dir).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with(&prefix)
                && name.rsplit_once('.').is_some_and(|(_, suffix)| {
                    suffix.len() >= 6 && suffix.bytes().all(|b| b.is_ascii_digit())
                })
                && !indexed.contains(name.as_str())
            {
                crate::util::logging::log_message(format_args!("WARNING: unindexed binlog file '{name}' preserved; it will not be reused by rotation"));
            }
        }

        let mut files = Vec::new();
        let mut events = Vec::new();
        let mut disk_prefixes = HashMap::new();
        for file_name in file_names {
            let path = dir.join(&file_name);
            if new_log {
                ensure_binlog_file(&path)?;
            }
            let mut size = disk::validated_file_size(&path)?;
            // Without a manifest there is no trusted publication boundary.
            // Recover complete events using bounded scratch instead.
            if manifest.is_none() {
                size = disk::recover_complete_prefix(&path, size)?;
            }
            if size == 4 {
                let bootstrap =
                    bootstrap_file_events(&config, 4, manifest_gtid_executed(&manifest));
                append_bytes(&path, &concat_event_bytes(&bootstrap))?;
                size = fs::metadata(&path).map_err(io_error)?.len();
                events.extend(chunks_from_encoded_events(&file_name, 4, bootstrap));
            } else {
                disk_prefixes.insert(file_name.clone(), size);
            }
            files.push(BinaryLogInfo {
                name: file_name,
                size,
            });
        }
        if files.is_empty() {
            let active_path = dir.join(&active_file);
            ensure_binlog_file(&active_path)?;
            let bootstrap = bootstrap_file_events(&config, 4, manifest_gtid_executed(&manifest));
            append_bytes(&active_path, &concat_event_bytes(&bootstrap))?;
            let size = fs::metadata(&active_path).map_err(io_error)?.len();
            files.push(BinaryLogInfo {
                name: active_file.clone(),
                size,
            });
            events.extend(chunks_from_encoded_events(&active_file, 4, bootstrap));
        }

        let manifest_active_pos = manifest
            .as_ref()
            .filter(|m| m.active_file == active_file)
            .map(|m| m.active_position);
        let durable_refs = committed_binlog_refs_from_wal(&data_dir)?;
        let active_pos = reconcile_binlog_tail(
            &dir,
            &active_file,
            manifest_active_pos,
            &durable_refs,
            &mut files,
            &mut events,
        )?;
        if let Some(end) = disk_prefixes.get_mut(&active_file) {
            *end = active_pos;
        }
        let mut gtid_executed = manifest
            .as_ref()
            .map(|m| m.gtid_executed.clone())
            .unwrap_or_default();
        for reference in &durable_refs {
            if files
                .iter()
                .any(|info| info.name == reference.file && info.size >= reference.end_pos)
            {
                if let Some(gtid) = &reference.gtid {
                    gtid_executed = append_gtid(&gtid_executed, gtid);
                }
            }
        }
        let gtid_purged = manifest
            .as_ref()
            .map(|m| m.gtid_purged.clone())
            .unwrap_or_default();
        let next_gno = manifest
            .as_ref()
            .map(|m| m.next_gno)
            .filter(|gno| *gno > 0)
            .unwrap_or(1)
            .max(next_gno_for_uuid(&gtid_executed, &config.server_uuid));
        let sequence_number = manifest
            .as_ref()
            .map(|m| m.sequence_number)
            .unwrap_or_default()
            .max(next_gno.saturating_sub(1));
        write_index_file(&index_path, &files)?;
        write_manifest_file(
            &manifest_path,
            &config,
            &active_file,
            active_pos,
            &gtid_executed,
            &gtid_purged,
            next_gno,
            sequence_number,
        )?;

        let mut binlog_state = BinlogState {
            active_file,
            active_pos,
            files,
            events,
            live_event_bytes: 0,
            disk_prefixes,
            pending: HashMap::new(),
            gtid_executed,
            gtid_purged,
            next_gno,
            sequence_number,
            file_first: HashMap::new(),
        };
        binlog_state.rebuild_file_index();
        let sync_binlog = AtomicU64::new(config.sync_binlog);
        let rotate_bytes = AtomicU64::new(config.rotate_bytes.unwrap_or(1024 * 1024 * 1024));
        let semi_sync = SemiSyncSource::new(&config);
        Ok(Self {
            config,
            sync_binlog,
            rotate_bytes,
            active_dump_leases: AtomicUsize::new(0),
            reset_generation: AtomicU64::new(0),
            dir,
            index_path,
            manifest_path,
            notify: Notify::new(),
            state: parking_lot::Mutex::new(binlog_state),
            commit_order: Arc::new(BinlogCommitOrder::default()),
            group_sync: Arc::new(BinlogGroupSync::default()),
            dump_registry: parking_lot::Mutex::new(DumpRegistry::default()),
            semi_sync,
        })
    }

    /// Register a binlog dump consumer (typically a replica). Returns a guard
    /// that removes the registration on drop, so a disconnect cleans up.
    pub fn register_replica(
        self: &std::sync::Arc<Self>,
        info: RegisteredReplica,
    ) -> Result<RegisteredReplicaGuard, QueryError> {
        let server_id = info.server_id;
        let mut registry = self.dump_registry.lock();
        if registry.entries.contains_key(&server_id) {
            return Err(error(
                crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
                format!("replica server_id {server_id} is already registered"),
            ));
        }
        let regid = registry.insert(info);
        Ok(RegisteredReplicaGuard {
            manager: self.clone(),
            server_id,
            regid,
        })
    }

    /// Snapshot of all currently-registered dump consumers, sorted by
    /// `server_id`. Used by `SHOW REPLICAS` / `SHOW SLAVE HOSTS`.
    pub fn registered_replicas(&self) -> Vec<RegisteredReplica> {
        self.dump_registry.lock().list()
    }

    /// Whether master-side semi-sync is enabled (`rpl_semi_sync_source_enabled`).
    pub fn semi_sync_source_enabled(&self) -> bool {
        self.semi_sync.enabled.load(Ordering::Relaxed)
    }

    /// Mark a registered replica as semi-sync capable (it sent
    /// `SET @rpl_semi_sync_slave=1` before its dump). Idempotent.
    pub fn semi_sync_mark_replica(&self, server_id: u32) {
        let mut reg = self.dump_registry.lock();
        if let Some(entry) = reg.entries.get_mut(&server_id) {
            entry.semi_sync = true;
        }
    }

    /// Record a semi-sync ACK `(file, pos)` from a replica and wake any parked
    /// committer. Positions only ever advance (a stale/duplicate ack is
    /// ignored). Called from the dump loop when it reads a `0xEF` ack packet.
    pub fn record_semi_sync_ack(&self, server_id: u32, file: &str, pos: u64) {
        {
            let mut reg = self.dump_registry.lock();
            let Some(entry) = reg.entries.get_mut(&server_id) else {
                return;
            };
            entry.semi_sync = true;
            if binlog_pos_ge((file, pos), (&entry.acked_file, entry.acked_pos)) {
                entry.acked_file = file.to_string();
                entry.acked_pos = pos;
            }
        }
        {
            let mut degraded = self.semi_sync.degraded_at.lock().unwrap();
            if degraded.as_ref().is_some_and(|(file, pos)| {
                self.semi_sync_acked_count(file, *pos)
                    >= self
                        .semi_sync
                        .wait_for_replica_count
                        .load(Ordering::Relaxed)
                        .max(1)
            }) {
                *degraded = None;
            }
        }
        self.semi_sync.notify_ack();
    }

    /// Count of connected semi-sync-capable replicas (`Rpl_semi_sync_*_clients`).
    pub fn semi_sync_client_count(&self) -> u64 {
        self.dump_registry
            .lock()
            .entries
            .values()
            .filter(|e| e.semi_sync)
            .count() as u64
    }

    /// How many semi-sync replicas have acknowledged position `(file, pos)`.
    fn semi_sync_acked_count(&self, file: &str, pos: u64) -> u64 {
        self.dump_registry
            .lock()
            .entries
            .values()
            .filter(|e| e.semi_sync && binlog_pos_ge((&e.acked_file, e.acked_pos), (file, pos)))
            .count() as u64
    }

    /// Commit-side semi-sync wait (MySQL AFTER_SYNC semantics): called after the
    /// transaction's binlog/WAL are durable and the event is visible to the dump
    /// loop. Blocks until `wait_for_replica_count` semi-sync replicas ack
    /// `(file, end_pos)`, or the timeout fires and the commit degrades to async.
    /// Never blocks forever; always releases the committer.
    pub fn semi_sync_wait(&self, file: &str, end_pos: u64) -> SemiSyncOutcome {
        let ss = &self.semi_sync;
        if !ss.enabled.load(Ordering::Relaxed) {
            return SemiSyncOutcome::Inactive;
        }
        let wait_count = ss.wait_for_replica_count.load(Ordering::Relaxed).max(1);
        // Fast path: quorum already met (common under steady low-latency links).
        if self.semi_sync_acked_count(file, end_pos) >= wait_count {
            ss.yes_tx.fetch_add(1, Ordering::Relaxed);
            return SemiSyncOutcome::Acked;
        }
        if ss.degraded_at.lock().unwrap().is_some() {
            ss.no_tx.fetch_add(1, Ordering::Relaxed);
            return SemiSyncOutcome::TimedOut;
        }
        if !ss.wait_no_replica.load(Ordering::Relaxed) && self.semi_sync_client_count() < wait_count
        {
            ss.no_tx.fetch_add(1, Ordering::Relaxed);
            return SemiSyncOutcome::NoReplicaDegrade;
        }
        let timeout = Duration::from_millis(ss.timeout_ms.load(Ordering::Relaxed).max(1));
        let deadline = std::time::Instant::now() + timeout;
        ss.wait_sessions.fetch_add(1, Ordering::Relaxed);
        let outcome = loop {
            let gen = ss.ack_gate.lock().unwrap();
            let wait_count = ss.wait_for_replica_count.load(Ordering::Relaxed).max(1);
            if self.semi_sync_acked_count(file, end_pos) >= wait_count {
                ss.yes_tx.fetch_add(1, Ordering::Relaxed);
                break SemiSyncOutcome::Acked;
            }
            if ss.degraded_at.lock().unwrap().is_some() {
                ss.no_tx.fetch_add(1, Ordering::Relaxed);
                break SemiSyncOutcome::TimedOut;
            }
            // If semi-sync was turned off underneath us, stop waiting.
            if !ss.enabled.load(Ordering::Relaxed) {
                ss.no_tx.fetch_add(1, Ordering::Relaxed);
                break SemiSyncOutcome::TimedOut;
            }
            if !ss.wait_no_replica.load(Ordering::Relaxed)
                && self.semi_sync_client_count() < wait_count
            {
                ss.no_tx.fetch_add(1, Ordering::Relaxed);
                break SemiSyncOutcome::NoReplicaDegrade;
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                let mut degraded = ss.degraded_at.lock().unwrap();
                if degraded.as_ref().is_none_or(|(old_file, old_pos)| {
                    binlog_pos_ge((file, end_pos), (old_file, *old_pos))
                }) {
                    *degraded = Some((file.to_owned(), end_pos));
                }
                ss.ack_cond.notify_all();
                ss.no_tx.fetch_add(1, Ordering::Relaxed);
                break SemiSyncOutcome::TimedOut;
            }
            let remaining = deadline - now;
            let (_gen, _res) = ss.ack_cond.wait_timeout(gen, remaining).unwrap();
        };
        ss.wait_sessions.fetch_sub(1, Ordering::Relaxed);
        outcome
    }

    /// `SHOW STATUS LIKE 'Rpl_semi_sync%'` rows for the master side. Both the
    /// 8.0.26+ `source` names and the legacy `master` aliases are surfaced so
    /// existing monitoring keeps working.
    pub fn semi_sync_status_pairs(&self) -> Vec<(String, String)> {
        let ss = &self.semi_sync;
        let enabled = ss.enabled.load(Ordering::Relaxed);
        let clients = self.semi_sync_client_count();
        let wait_count = ss.wait_for_replica_count.load(Ordering::Relaxed).max(1);
        // Operating synchronously iff enabled and a full quorum is connected.
        let status = if enabled && clients >= wait_count && ss.degraded_at.lock().unwrap().is_none()
        {
            "ON"
        } else {
            "OFF"
        };
        let yes_tx = ss.yes_tx.load(Ordering::Relaxed);
        let no_tx = ss.no_tx.load(Ordering::Relaxed);
        let wait_sessions = ss.wait_sessions.load(Ordering::Relaxed) as u64;
        let base: [(&str, String); 5] = [
            ("status", status.to_string()),
            ("clients", clients.to_string()),
            ("yes_tx", yes_tx.to_string()),
            ("no_tx", no_tx.to_string()),
            ("wait_sessions", wait_sessions.to_string()),
        ];
        let mut out = Vec::with_capacity(base.len() * 2 + 8);
        for (suffix, value) in &base {
            out.push((format!("Rpl_semi_sync_source_{suffix}"), value.clone()));
            out.push((format!("Rpl_semi_sync_master_{suffix}"), value.clone()));
        }
        let (cached_events, cached_bytes) = {
            let state = self.state.lock();
            (state.events.len(), state.live_event_bytes)
        };
        out.push((
            "Mydb_binlog_cached_events".into(),
            cached_events.to_string(),
        ));
        out.push((
            "Mydb_binlog_cached_payload_bytes".into(),
            cached_bytes.to_string(),
        ));
        let requests = self.group_sync.requests.load(Ordering::Relaxed);
        let batches = self.group_sync.batches.load(Ordering::Relaxed);
        out.push((
            "Mydb_binlog_group_sync_requests".into(),
            requests.to_string(),
        ));
        out.push(("Mydb_binlog_group_sync_batches".into(), batches.to_string()));
        out.push((
            "Mydb_binlog_group_sync_avg_batch_size".into(),
            format!(
                "{:.2}",
                if batches == 0 {
                    0.0
                } else {
                    requests as f64 / batches as f64
                }
            ),
        ));
        out.push((
            "Mydb_binlog_group_sync_batch_max".into(),
            self.group_sync
                .batch_max
                .load(Ordering::Relaxed)
                .to_string(),
        ));
        out.push((
            "Mydb_binlog_write_calls".into(),
            self.group_sync
                .write_calls
                .load(Ordering::Relaxed)
                .to_string(),
        ));
        out.push((
            "Mydb_binlog_file_opens".into(),
            self.group_sync
                .file_opens
                .load(Ordering::Relaxed)
                .to_string(),
        ));
        out.push((
            "Mydb_binlog_write_bytes".into(),
            self.group_sync
                .write_bytes
                .load(Ordering::Relaxed)
                .to_string(),
        ));
        out.push((
            "Mydb_binlog_sync_total_us".into(),
            self.group_sync
                .sync_total_us
                .load(Ordering::Relaxed)
                .to_string(),
        ));
        out.push((
            "Mydb_binlog_sync_max_us".into(),
            self.group_sync
                .sync_max_us
                .load(Ordering::Relaxed)
                .to_string(),
        ));
        out
    }

    pub fn config(&self) -> &BinlogConfig {
        &self.config
    }

    /// Serialize dump admission with RESET, including consumers which do not
    /// send COM_REGISTER_SLAVE (for example mysqlbinlog).
    pub(crate) fn lease_dump(self: &Arc<Self>) -> BinlogDumpLease {
        let _state = self.state.lock();
        self.active_dump_leases.fetch_add(1, Ordering::AcqRel);
        BinlogDumpLease(Arc::clone(self))
    }

    fn check_read_generation(&self, generation: u64) -> Result<(), QueryError> {
        if self.reset_generation.load(Ordering::Acquire) != generation {
            return Err(error(
                crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
                "binary logs were reset during the read; restart the request",
            ));
        }
        Ok(())
    }

    pub fn current_position(&self) -> Result<BinlogPosition, QueryError> {
        let state = self.state.lock();
        Ok(BinlogPosition {
            file: state.active_file.clone(),
            position: state.published_position(),
        })
    }

    fn status_snapshot(&self) -> BinlogStatusSnapshot {
        let state = self.state.lock();
        BinlogStatusSnapshot {
            position: BinlogPosition {
                file: state.active_file.clone(),
                position: state.published_position(),
            },
            executed_gtid_set: state.gtid_executed.clone(),
        }
    }

    pub fn binary_logs(&self) -> Result<Vec<BinaryLogInfo>, QueryError> {
        let state = self.state.lock();
        Ok(state.files.clone())
    }

    /// Build the prefix bytes vanilla MySQL replicas expect at the start of
    /// any `COM_BINLOG_DUMP[_GTID]` stream:
    ///
    /// 1. A *fake* `ROTATE_EVENT` whose timestamp is zero — tells the
    ///    replica "open this relay-log file, ignore my (artificial) header
    ///    position." Without this, the SQL thread doesn't know which
    ///    relay-log name to write into and rejects events with "could not
    ///    parse relay log event entry."
    /// 2. A fresh `FORMAT_DESCRIPTION_EVENT` so the replica picks up our
    ///    binlog version, post-header sizes, and (critically) the
    ///    checksum-algorithm descriptor byte. Without this, the SQL
    ///    thread defaults to NONE-checksum and chokes on the CRC32
    ///    trailer attached to every subsequent event.
    pub fn dump_preamble(&self, file: &str, start_position: u64) -> Vec<Vec<u8>> {
        let state = self.state.lock();
        let target_file = if file.is_empty() {
            state.active_file.clone()
        } else {
            file.to_string()
        };
        drop(state);
        let mut out = Vec::with_capacity(2);
        // Fake rotate: header timestamp = 0 so the replica recognises it as
        // synthetic, payload = (next_position, next_file_name). The position MUST
        // be where the stream actually begins (the resolved GTID/file start), not
        // a hardcoded 4: with GTID auto-position the master skips already-applied
        // transactions and starts mid-file, and the replica seeds its
        // `Read_Source_Log_Pos` from this rotate. A wrong (too-low) position
        // leaves the IO thread's received position — and thus the semi-sync ack
        // it sends — lagging the real stream, so a committer waiting on the true
        // end position never sees a covering ack. Clamp to the 4-byte magic
        // header minimum.
        let mut rotate_payload = Vec::with_capacity(8 + target_file.len());
        rotate_payload.extend_from_slice(&start_position.max(4).to_le_bytes());
        rotate_payload.extend_from_slice(target_file.as_bytes());
        let mut rotate =
            encode_event_with_timestamp(&self.config, ROTATE_EVENT, 0, 0, &rotate_payload, false);
        mark_dump_event_artificial(&mut rotate, self.config.checksum == BinlogChecksum::Crc32);
        out.push(rotate);
        // At file start the real FDE is already the next event. Mid-file
        // requests need a synthetic FDE to negotiate the checksum first.
        if start_position > 4 {
            let mut fde = encode_format_description_event(&self.config, 0);
            mark_dump_event_artificial(&mut fde, true);
            out.push(fde);
        }
        out
    }

    #[cfg(test)]
    pub fn events_after(
        &self,
        file: &str,
        position: u64,
    ) -> Result<Vec<BinlogEventChunk>, QueryError> {
        self.events_after_bounded(file, position, usize::MAX, usize::MAX)
    }

    /// Return a bounded prefix of events following `(file, position)`.
    /// Replication dump sessions use this instead of cloning the entire
    /// remaining binlog when a replica is far behind.
    pub fn events_after_bounded(
        &self,
        file: &str,
        position: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<BinlogEventChunk>, QueryError> {
        let state = self.state.lock();
        let requested_file = if file.is_empty() {
            &state.active_file
        } else {
            file
        };
        let cold = state
            .disk_prefixes
            .get(requested_file)
            .is_some_and(|end| position.max(4) < *end);
        if cold {
            // Keep the caught-up path allocation-free apart from its result.
            // A lagging reader snapshots bounded live descriptors, then does
            // disk I/O without holding the commit/publication latch.
            let generation = self.reset_generation.load(Ordering::Relaxed);
            let snapshot = (
                state.active_file.clone(),
                state.files.clone(),
                state.disk_prefixes.clone(),
                state.file_first.clone(),
                state.events.clone(),
            );
            drop(state);
            let events = self.events_after_from_index(
                file,
                position,
                max_events,
                max_bytes,
                &snapshot.0,
                &snapshot.1,
                &snapshot.2,
                &snapshot.3,
                &snapshot.4,
                false,
            )?;
            self.check_read_generation(generation)?;
            return Ok(events);
        }
        self.events_after_from_index(
            file,
            position,
            max_events,
            max_bytes,
            &state.active_file,
            &state.files,
            &state.disk_prefixes,
            &state.file_first,
            &state.events,
            false,
        )
    }

    /// Administrative SHOW requests may scan large disk prefixes. Clone the
    /// small, bounded live index once and release the commit latch before I/O.
    fn show_events_bounded(
        &self,
        file: &str,
        position: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<(Vec<BinlogEventChunk>, bool), QueryError> {
        let state = self.state.lock();
        let generation = self.reset_generation.load(Ordering::Relaxed);
        let snapshot = (
            state.active_file.clone(),
            state.files.clone(),
            state.disk_prefixes.clone(),
            state.file_first.clone(),
            state.events.clone(),
        );
        drop(state);
        let chunks = self.events_after_from_index(
            file,
            position,
            max_events,
            max_bytes,
            &snapshot.0,
            &snapshot.1,
            &snapshot.2,
            &snapshot.3,
            &snapshot.4,
            true,
        )?;
        self.check_read_generation(generation)?;
        let (next_file, next_pos) = chunks
            .last()
            .map(|chunk| (chunk.file.as_str(), chunk.end_pos))
            .unwrap_or((file, position));
        let next_file = if next_file.is_empty() {
            snapshot.0.as_str()
        } else {
            next_file
        };
        let more = snapshot
            .2
            .iter()
            .any(|(name, end)| binlog_pos_cmp((name, *end), (next_file, next_pos)).is_gt())
            || snapshot.4.iter().any(|event| {
                binlog_pos_cmp((&event.file, event.end_pos), (next_file, next_pos)).is_gt()
            });
        Ok((chunks, more))
    }

    fn events_after_from_index(
        &self,
        file: &str,
        position: u64,
        max_events: usize,
        max_bytes: usize,
        active_file: &str,
        files: &[BinaryLogInfo],
        disk_prefixes: &HashMap<String, u64>,
        file_first: &HashMap<String, usize>,
        events: &[BinlogEventChunk],
        strict_byte_limit: bool,
    ) -> Result<Vec<BinlogEventChunk>, QueryError> {
        let file = if file.is_empty() { active_file } else { file };
        if !files.iter().any(|info| info.name == file) {
            return Err(error(
                crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
                format!("requested binlog file '{file}' does not exist"),
            ));
        }
        let end = files.iter().find(|info| info.name == file).unwrap().size;
        if position > end {
            return Err(error(
                crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
                format!(
                    "requested binlog position {position} exceeds '{file}' published size {end}"
                ),
            ));
        }
        let position = position.max(4);
        if position < end {
            if let Some(&cold_end) = disk_prefixes.get(file).filter(|end| position < **end) {
                // Normal reads validate the header while reading the batch.
                // A zero-budget protocol preflight must validate it as well.
                if max_events == 0 {
                    disk::validate_position(&self.dir.join(file), position, cold_end)?;
                }
            } else {
                let valid = file_first.get(file).is_some_and(|&start| {
                    let count = events[start..].partition_point(|event| event.file == file);
                    let events = &events[start..start + count];
                    let next = events.partition_point(|event| event.end_pos <= position);
                    events
                        .get(next)
                        .is_some_and(|event| event.start_pos == position)
                });
                if !valid {
                    return Err(error(
                        crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
                        format!("requested binlog position {position} is not an event boundary in '{file}'"),
                    ));
                }
            }
        }
        if max_events == 0 {
            return Ok(Vec::new());
        }
        // Read recovered prefixes from disk, then merge live published events
        // in file order. Staged but unpublished bytes are never exposed.
        let file_order = files
            .iter()
            .position(|info| info.name == file)
            .expect("file existence checked above");
        let mut out = Vec::new();
        let mut bytes = 0usize;
        for info in &files[file_order..] {
            let position = if info.name == file { position } else { 4 };
            if let Some(&end) = disk_prefixes.get(&info.name) {
                if position < end && out.len() < max_events {
                    let complete = disk::read_events(
                        &self.dir.join(&info.name),
                        &info.name,
                        position,
                        end,
                        max_events,
                        max_bytes,
                        &mut bytes,
                        &mut out,
                        strict_byte_limit,
                    )?;
                    if !complete {
                        break;
                    }
                }
            }
            let Some(&start) = file_first.get(&info.name) else {
                continue;
            };
            let count = events[start..].partition_point(|event| event.file == info.name);
            let within_file = &events[start..start + count];
            let unsent = within_file
                .partition_point(|event| event.file == info.name && event.end_pos <= position);
            for event in &within_file[unsent..] {
                if event.file != info.name {
                    break;
                }
                let next = bytes.saturating_add(event.bytes.len());
                if strict_byte_limit && event.bytes.len() > max_bytes {
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "SHOW BINLOG EVENTS contains an event larger than 64 MiB",
                    ));
                }
                if out.len() >= max_events || (!out.is_empty() && next > max_bytes) {
                    return Ok(out);
                }
                bytes = next;
                out.push(event.clone());
            }
        }
        Ok(out)
    }

    pub fn position_for_gtid_dump(
        &self,
        requested_file: &str,
        requested_position: u64,
        executed_gtid_set: &str,
    ) -> Result<BinlogPosition, QueryError> {
        let state = self.state.lock();
        if !gtid_subset(&state.gtid_purged, executed_gtid_set) {
            return Err(error(
                crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
                "requested GTID set is older than the binary logs available on this server",
            ));
        }
        if executed_gtid_set.trim().is_empty() {
            let file = if requested_file.is_empty() {
                state
                    .files
                    .first()
                    .map(|info| info.name.clone())
                    .unwrap_or_else(|| state.active_file.clone())
            } else {
                requested_file.to_string()
            };
            return Ok(BinlogPosition {
                file,
                position: requested_position.max(4),
            });
        }
        let executed = crate::gtid::GtidSet::parse(executed_gtid_set);
        for info in &state.files {
            if let Some(&end) = state.disk_prefixes.get(&info.name) {
                let mut found = None;
                disk::visit_gtids(&self.dir.join(&info.name), end, |pos, gtid| {
                    if !executed.contains(gtid) {
                        found = Some(pos);
                        false
                    } else {
                        true
                    }
                })?;
                if let Some(position) = found {
                    return Ok(BinlogPosition {
                        file: info.name.clone(),
                        position,
                    });
                }
            }
            if let Some(&start) = state.file_first.get(&info.name) {
                for event in state.events[start..]
                    .iter()
                    .take_while(|e| e.file == info.name)
                {
                    if let Some(gtid) = &event.gtid {
                        if !executed.contains(gtid) {
                            return Ok(BinlogPosition {
                                file: event.file.clone(),
                                position: event.start_pos,
                            });
                        }
                    }
                }
            }
        }
        Ok(BinlogPosition {
            file: state.active_file.clone(),
            position: state.published_position(),
        })
    }

    pub fn active_file_for_empty_request(&self) -> String {
        self.state.lock().active_file.clone()
    }

    pub fn wait_for_new_events(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }

    pub fn heartbeat_event(&self) -> Result<Vec<u8>, QueryError> {
        let state = self.state.lock();
        // MySQL Heartbeat_log_event semantics: `timestamp = 0` marks it
        // synthetic, and `log_pos` must equal the source's current tail
        // (state.active_pos) — the very position where the *next* real
        // event will be written. The generic encode_event flow computes
        // end_log_pos = start_pos + event_size, which would send a
        // position that is *below* the last real event's end_log_pos in
        // MySQL's stream tracker; the replica then hits ER_HEARTBEAT_QUEUE_FAILURE
        // ("heartbeat event contained the position N which is smaller
        // than the position M that was computed from earlier events") and
        // aborts the IO thread.
        encode_heartbeat_event(
            &self.config,
            state.published_position(),
            state.active_file.as_bytes(),
        )
    }

    pub fn rotate(&self) -> Result<(), QueryError> {
        self.flush_all_staged_writes()?;
        // The caller holds COMMIT_GATE, so no new order can be allocated.
        // Wait without holding manager.state: earlier commits must publish
        // their rows before ROTATE/FDE enter the dump stream or retention can
        // remove their file. It also makes PREVIOUS_GTIDS complete.
        let barrier = self.commit_order.state.lock().next_alloc;
        self.commit_order.wait_publish(barrier)?;
        let mut state = self.state.lock();
        self.rotate_locked(&mut state)?;
        drop(state);
        self.notify.notify_waiters();
        Ok(())
    }

    /// MySQL `RESET MASTER` / 8.4 `RESET BINARY LOGS AND GTIDS`: delete all
    /// binary log files, clear executed-GTID set, restart numbering from
    /// `.000001`. Rejects when any replica is mid-dump — clearing the binlog
    /// under a connected replica would corrupt its position.
    pub fn reset_master(
        &self,
        wal_boundary: impl FnOnce() -> Result<u64, QueryError>,
    ) -> Result<(), QueryError> {
        let active_replicas = self.dump_registry.lock().list();
        if !active_replicas.is_empty() {
            return Err(error(crate::error::catalog::ER_EMPTY_PIPELINE_FOR_ERROR_LOG_SERVICE,
                format!(
                    "cannot RESET MASTER while {} replica(s) are connected; STOP REPLICA on each first",
                    active_replicas.len()
                ),
            ));
        }
        let mut state = self.state.lock();
        if self.active_dump_leases.load(Ordering::Acquire) != 0 {
            return Err(error(
                crate::error::catalog::ER_EMPTY_PIPELINE_FOR_ERROR_LOG_SERVICE,
                "cannot RESET MASTER while binary log dump readers are active",
            ));
        }
        // COMMIT_GATE prevents new staging, but an earlier commit may still
        // be between binlog sync and WAL/publication. Deleting its file would
        // leave a durable WAL reference to an unrelated, reused filename.
        // Pending row images also belong to active transactions, not RESET.
        let commits_pending = {
            let order = self.commit_order.state.lock();
            order.next_publish != order.next_alloc
        };
        if commits_pending || !state.pending.is_empty() {
            return Err(error(
                crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                "cannot RESET MASTER while binlog transactions are pending; retry after they finish",
            ));
        }
        // The caller holds COMMIT_GATE. Read the active allocator and persist
        // its boundary before the reset intent: a live filesystem scan misses
        // buffered references and can destructively trim an active WAL file.
        // Persisting the boundary also prevents post-restart LSN reuse from
        // making the first new commit disappear behind an inclusive cutoff.
        let cutoff = wal_boundary()?;
        write_reset_marker(&self.dir, "reset-pending", cutoff)?;
        self.reset_generation.fetch_add(1, Ordering::Release);
        // RESET reuses the .000001 name with a new inode. Close the cached
        // descriptor before unlinking any old files.
        let binlog_failed = self.group_sync.state.lock().fatal_error.is_some();
        if !binlog_failed {
            self.flush_all_staged_writes()?;
        } else {
            let mut group = self.group_sync.state.lock();
            while group.syncing {
                self.group_sync.cv.wait(&mut group);
            }
        }
        self.group_sync.state.lock().writer = None;
        // Drop in-memory event index and pending txns.
        state.events.clear();
        state.live_event_bytes = 0;
        state.disk_prefixes.clear();
        state.file_first.clear();
        state.pending.clear();
        // Remove on-disk binary log files.
        for info in state.files.drain(..) {
            let _ = fs::remove_file(self.dir.join(&info.name));
        }
        // Restart numbering.
        let new_file = format!("{}.000001", self.config.base_name);
        let path = self.dir.join(&new_file);
        ensure_binlog_file(&path)?;
        let bootstrap = bootstrap_file_events(&self.config, 4, "");
        let mut bootstrap_writer = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(io_error)?;
        bootstrap_writer.write_all(BINLOG_MAGIC).map_err(io_error)?;
        for (_, bytes) in &bootstrap {
            bootstrap_writer.write_all(bytes).map_err(io_error)?;
        }
        crate::wal::sync_wal_file(&bootstrap_writer)?;
        let size = fs::metadata(&path).map_err(io_error)?.len();
        state.files.push(BinaryLogInfo {
            name: new_file.clone(),
            size,
        });
        for chunk in chunks_from_encoded_events(&new_file, 4, bootstrap) {
            state.push_event(chunk);
        }
        state.active_file = new_file;
        state.active_pos = size;
        state.gtid_executed.clear();
        state.gtid_purged.clear();
        state.next_gno = 1;
        state.sequence_number = 0;
        self.group_sync
            .groups_since_sync
            .store(0, Ordering::Relaxed);
        write_index_file(&self.index_path, &state.files)?;
        self.persist_locked(&state)?;
        write_reset_marker(&self.dir, "reset-cutoff", cutoff)?;
        fs::remove_file(self.dir.join("reset-pending")).map_err(io_error)?;
        crate::platform::fs::sync_path(&self.dir).map_err(io_error)?;
        if binlog_failed {
            let mut group = self.group_sync.state.lock();
            group.writes.clear();
            group.requested_order = 0;
            group.durable_order = 0;
            group.fatal_error = None;
            self.commit_order.state.lock().fatal_error = None;
        }
        drop(state);
        self.notify.notify_waiters();
        Ok(())
    }

    /// P2-B2 phase 1: `PURGE BINARY LOGS BEFORE 'YYYY-MM-DD HH:MM:SS'`.
    ///
    /// Removes every binary log file whose file-system modification
    /// time is strictly older than the supplied cut-off. The active
    /// file is preserved regardless — purging it would orphan the
    /// in-flight write position. Used by PITR runbooks (see
    /// `scripts/pitr_restore.sh`) to trim the binlog retention window
    /// down to "what we still need to replay from", and by
    /// retention-driven housekeeping.
    pub fn purge_before(&self, cut_off: &str) -> Result<(), QueryError> {
        use chrono::{Local, NaiveDateTime};
        let parsed = NaiveDateTime::parse_from_str(cut_off, "%Y-%m-%d %H:%M:%S")
            .or_else(|_| NaiveDateTime::parse_from_str(cut_off, "%Y-%m-%dT%H:%M:%S"))
            .map_err(|err| {
                error(
                    crate::error::catalog::ER_WRONG_VALUE,
                    format!("PURGE BINARY LOGS BEFORE: bad datetime '{cut_off}': {err}"),
                )
            })?;
        let cut_off_local = match parsed.and_local_timezone(Local).single() {
            Some(dt) => dt,
            None => {
                return Err(error(
                    crate::error::catalog::ER_WRONG_VALUE,
                    format!("PURGE BINARY LOGS BEFORE: ambiguous local datetime '{cut_off}'"),
                ))
            }
        };
        let cut_off_epoch = cut_off_local.timestamp();
        if cut_off_epoch <= 0 {
            return Ok(());
        }
        let cut_off_epoch = cut_off_epoch as u64;
        let mut state = self.state.lock();
        // The active file is always kept; only purge files that
        // strictly precede the cut-off and aren't the writer's
        // current target.
        let mut purge_indices: Vec<usize> = Vec::new();
        for (idx, info) in state.files.iter().enumerate() {
            if info.name == state.active_file {
                continue;
            }
            let path = self.dir.join(&info.name);
            let mtime = match std::fs::metadata(&path).and_then(|m| m.modified()) {
                Ok(modified) => modified
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
                Err(_) => continue, // missing file: skip silently.
            };
            if mtime < cut_off_epoch {
                purge_indices.push(idx);
            }
        }
        if purge_indices.is_empty() {
            return Ok(());
        }
        let mut cold_purged = state.gtid_purged.clone();
        for &idx in &purge_indices {
            self.append_disk_gtids(&state, &state.files[idx].name, &mut cold_purged)?;
        }
        // Drain in reverse order so earlier indices stay valid as we go.
        let mut purged: Vec<String> = Vec::with_capacity(purge_indices.len());
        for idx in purge_indices.into_iter().rev() {
            purged.push(state.files.remove(idx).name);
        }
        purged.reverse();
        self.finish_purge_locked(&mut state, purged, cold_purged)
    }

    pub fn purge_to(&self, target_file: &str) -> Result<(), QueryError> {
        let mut state = self.state.lock();
        let Some(target_idx) = state.files.iter().position(|info| info.name == target_file) else {
            return Err(error(
                crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
                format!("binary log file '{target_file}' does not exist"),
            ));
        };
        let mut cold_purged = state.gtid_purged.clone();
        for info in &state.files[..target_idx] {
            self.append_disk_gtids(&state, &info.name, &mut cold_purged)?;
        }
        let purged: Vec<String> = state
            .files
            .drain(..target_idx)
            .map(|info| info.name)
            .collect();
        if purged.is_empty() {
            return Ok(());
        }
        self.finish_purge_locked(&mut state, purged, cold_purged)
    }

    /// Merge only removed events into gtid_purged, then persist the live
    /// index/manifest before unlinking files. Both purge selectors use this
    /// ordering while retaining the state lock throughout.
    fn finish_purge_locked(
        &self,
        state: &mut BinlogState,
        purged: Vec<String>,
        cold_purged: String,
    ) -> Result<(), QueryError> {
        let purged_set: HashSet<&str> = purged.iter().map(String::as_str).collect();
        let mut purged_gtid = cold_purged;
        for event in state.events.iter() {
            if purged_set.contains(event.file.as_str()) {
                if let Some(gtid) = &event.gtid {
                    purged_gtid = append_gtid(&purged_gtid, gtid);
                }
            }
        }
        state.gtid_purged = purged_gtid;
        state
            .events
            .retain(|event| !purged_set.contains(event.file.as_str()));
        state
            .disk_prefixes
            .retain(|name, _| !purged_set.contains(name.as_str()));
        state.rebuild_file_index();
        write_index_file(&self.index_path, &state.files)?;
        write_manifest_file(
            &self.manifest_path,
            &self.config,
            &state.active_file,
            state.published_position(),
            &state.gtid_executed,
            &state.gtid_purged,
            state.next_gno,
            state.sequence_number,
        )?;
        for file in purged {
            let _ = fs::remove_file(self.dir.join(file));
        }
        Ok(())
    }

    fn push_op(&self, txn_id: u64, op: BinlogTxnOp) -> Result<(), QueryError> {
        use std::collections::hash_map::Entry;

        if let Some(message) = self.group_sync.state.lock().fatal_error.clone() {
            return Err(io_error(std::io::Error::other(message)));
        }

        let nested_bytes = binlog_op_nested_bytes(&op);
        let mut state = self.state.lock();
        match state.pending.entry(txn_id) {
            Entry::Vacant(entry) => {
                let mut ops = Vec::with_capacity(1);
                let bytes = ops
                    .capacity()
                    .saturating_mul(std::mem::size_of::<BinlogTxnOp>())
                    .saturating_add(nested_bytes);
                let memory_charge =
                    crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                        crate::engine::executor::mem_budget::ControlledMemoryCategory::BinlogTransient,
                        bytes,
                    )?;
                ops.push(op);
                entry.insert(PendingBinlogTxn { ops, memory_charge });
            }
            Entry::Occupied(mut entry) => {
                let pending = entry.get_mut();
                if pending.ops.len() == pending.ops.capacity() {
                    // Allocate the replacement first so an admission failure
                    // can drop it without mutating the retained transaction.
                    let mut replacement =
                        Vec::with_capacity(pending.ops.len().saturating_mul(2).max(1));
                    let capacity_growth = replacement
                        .capacity()
                        .saturating_sub(pending.ops.capacity())
                        .saturating_mul(std::mem::size_of::<BinlogTxnOp>());
                    if !pending
                        .memory_charge
                        .try_grow_optional(capacity_growth.saturating_add(nested_bytes))
                    {
                        // Spare capacity is optional. Under memory pressure
                        // retain the minimal-growth admission behavior rather
                        // than rejecting a transaction just for this cache.
                        replacement = Vec::with_capacity(pending.ops.len().saturating_add(1));
                        let minimal_growth = replacement
                            .capacity()
                            .saturating_sub(pending.ops.capacity())
                            .saturating_mul(std::mem::size_of::<BinlogTxnOp>());
                        pending
                            .memory_charge
                            .try_grow(minimal_growth.saturating_add(nested_bytes))?;
                    }
                    replacement.append(&mut pending.ops);
                    replacement.push(op);
                    pending.ops = replacement;
                } else {
                    pending.memory_charge.try_grow(nested_bytes)?;
                    pending.ops.push(op);
                }
            }
        }
        Ok(())
    }

    fn append_query_event(
        &self,
        database: &str,
        sql: &str,
        ctx: &BinlogQueryContext,
    ) -> Result<(), QueryError> {
        // DDL writes directly to the active file. Drain every earlier staged
        // transaction first so a direct append can never overtake queued
        // transaction bytes.
        self.flush_all_staged_writes()?;
        let mut state = self.state.lock();
        let gtid = self.next_gtid_locked(&mut state)?;
        let encoded = vec![
            (
                GTID_EVENT,
                encode_gtid_payload(&self.config, gtid.gno, gtid.sequence_number, true),
                false,
            ),
            (
                QUERY_EVENT,
                encode_query_payload_with(database, sql, ctx)?,
                false,
            ),
        ];
        let encoded_bytes = encoded_event_bytes(&self.config, &encoded)?;
        let configured_limit = Some(
            self.rotate_bytes
                .load(Ordering::Relaxed)
                .min(self.config.retention_bytes.unwrap_or(u64::MAX))
                .max(4),
        );
        let should_rotate = configured_limit.is_some_and(|limit| {
            state.active_pos >= limit || state.active_pos.saturating_add(encoded_bytes) > limit
        }) || state
            .active_pos
            .checked_add(encoded_bytes)
            .is_none_or(|end| end > BINLOG_FILE_POSITION_LIMIT);
        if should_rotate {
            drop(state);
            self.rotate()?;
            state = self.state.lock();
        }
        if state
            .active_pos
            .checked_add(encoded_bytes)
            .is_none_or(|end| end > BINLOG_FILE_POSITION_LIMIT)
        {
            return Err(error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "binary log query event exceeds the 4 GiB file-position range",
            ));
        }
        let chunks = self.append_events_locked(&mut state, encoded, false, true, true)?;
        let order = self.commit_order.allocate(false);
        drop(state);
        // A DDL event can be appended after a transaction whose group fsync /
        // WAL barrier is still running. Publish it only after that earlier
        // transaction so the in-memory dump stream remains byte-for-byte file
        // ordered.
        self.commit_order.wait_publish(order)?;
        let mut state = self.state.lock();
        for chunk in chunks {
            state.push_event(chunk);
        }
        state.trim_published_events();
        state.gtid_executed = append_gtid(&state.gtid_executed, &gtid.to_string());
        let persist_result = self.persist_locked(&state);
        drop(state);
        self.commit_order.finish_publish(order);
        persist_result?;
        self.notify.notify_waiters();
        Ok(())
    }

    fn stage_txn_commit(
        &self,
        txn_id: u64,
        commit_scn: u64,
    ) -> Result<Option<BinlogCommit>, QueryError> {
        if let Some(message) = self.group_sync.state.lock().fatal_error.clone() {
            return Err(io_error(std::io::Error::other(message)));
        }
        let pending = {
            let mut state = self.state.lock();
            match state.pending.remove(&txn_id) {
                Some(pending) if !pending.ops.is_empty() => pending,
                _ => return Ok(None),
            }
        };
        let mut lease = PendingBinlogLease {
            manager: self,
            txn_id,
            pending: Some(pending),
        };
        let encoded = encode_pending_ops(&lease.pending.as_ref().unwrap().ops)?;
        let staged = self.stage_encoded_txn_commit(encoded, commit_scn)?;
        lease.pending.take();
        Ok(Some(staged))
    }

    fn stage_encoded_txn_commit(
        &self,
        body: Vec<(u8, Vec<u8>, bool)>,
        commit_scn: u64,
    ) -> Result<BinlogCommit, QueryError> {
        let may_have_statement = body.iter().any(|(kind, _, _)| *kind == QUERY_EVENT);
        // Storage DDL still participates in WAL commit/publication ordering,
        // but MySQL executes ordinary DDL as a standalone implicit commit.
        // Atomic CTAS uses MySQL's explicit marker and retains BEGIN/XID even
        // when its SELECT produces no row events.
        let standalone_ddl = body.len() == 1
            && body[0].0 == QUERY_EVENT
            && !body[0].1.ends_with(b" START TRANSACTION");
        let mut encoded = Vec::with_capacity(body.len().saturating_add(3));
        encoded.push((GTID_EVENT, Vec::new(), false));
        if !standalone_ddl {
            encoded.push((QUERY_EVENT, encode_query_payload(0, "", "BEGIN"), false));
        }
        encoded.extend(body);
        if !standalone_ddl {
            encoded.push((XID_EVENT, commit_scn.to_le_bytes().to_vec(), false));
        }
        let mut state = self.state.lock();
        let gtid = self.next_gtid_locked(&mut state)?;
        let gtid_string = gtid.to_string();
        encoded[0].1 = encode_gtid_payload(
            &self.config,
            gtid.gno,
            gtid.sequence_number,
            may_have_statement,
        );
        let encoded_bytes = encoded_event_bytes(&self.config, &encoded)?;
        let configured_limit = Some(
            self.rotate_bytes
                .load(Ordering::Relaxed)
                .min(self.config.retention_bytes.unwrap_or(u64::MAX))
                .max(4),
        );
        let would_cross_configured = configured_limit.is_some_and(|limit| {
            state.active_pos >= limit || state.active_pos.saturating_add(encoded_bytes) > limit
        });
        let would_overflow_position = state
            .active_pos
            .checked_add(encoded_bytes)
            .is_none_or(|end| end > BINLOG_FILE_POSITION_LIMIT);
        if would_cross_configured || would_overflow_position {
            drop(state);
            self.rotate()?;
            state = self.state.lock();
        }
        if state
            .active_pos
            .checked_add(encoded_bytes)
            .is_none_or(|end| end > BINLOG_FILE_POSITION_LIMIT)
        {
            return Err(error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "binary log transaction exceeds the 4 GiB file-position range",
            ));
        }
        let start_file = state.active_file.clone();
        let start_pos = state.active_pos;
        // Reserve byte positions under COMMIT_GATE, then queue the immutable
        // Arc-backed chunks. The group leader performs the physical writes and
        // optional fsync outside that gate. This avoids append(2) calls being
        // serialized behind an fsync already running on the same file.
        let chunks = self.append_events_locked(&mut state, encoded, false, false, false)?;
        let order = self.commit_order.allocate(true);
        let reference = crate::wal::WalBinlogRef {
            file: start_file,
            start_pos,
            end_pos: state.active_pos,
            gtid: Some(gtid_string),
        };
        if let Err(err) = self.register_staged_write(order, chunks.clone()) {
            self.commit_order.abort(order);
            return Err(err);
        }
        Ok(BinlogCommit {
            reference,
            chunks,
            order,
            order_state: Arc::clone(&self.commit_order),
            group_sync: Arc::clone(&self.group_sync),
            durable: AtomicBool::new(false),
            published: false,
        })
    }

    fn commit_txn(&self, txn_id: u64, commit_scn: u64) -> Result<(), QueryError> {
        let commit = self.stage_txn_commit(txn_id, commit_scn)?;
        if let Some(commit) = commit.as_ref() {
            self.sync_staged_commit(commit)?;
            self.commit_order.wait_wal(commit.order)?;
            self.commit_order.finish_wal(commit.order);
        }
        self.publish_txn_commit(commit)
    }

    fn sync_staged_commit(&self, commit: &BinlogCommit) -> Result<(), QueryError> {
        self.group_sync.requests.fetch_add(1, Ordering::Relaxed);
        let result = self.flush_staged_through(commit.order);
        if result.is_ok() {
            commit.durable.store(true, Ordering::Release);
        }
        result
    }

    fn register_staged_write(
        &self,
        order: u64,
        chunks: Vec<BinlogEventChunk>,
    ) -> Result<(), QueryError> {
        let mut state = self.group_sync.state.lock();
        if let Some(message) = state.fatal_error.as_ref() {
            return Err(io_error(std::io::Error::other(message.clone())));
        }
        state.requested_order = state.requested_order.max(order);
        let replaced = state.writes.insert(order, chunks);
        debug_assert!(replaced.is_none(), "duplicate binlog commit order");
        self.group_sync.cv.notify_all();
        Ok(())
    }

    fn flush_all_staged_writes(&self) -> Result<(), QueryError> {
        let target = self.group_sync.state.lock().requested_order;
        if target == 0 {
            return Ok(());
        }
        self.flush_staged_through(target)
    }

    fn flush_staged_through(&self, required_order: u64) -> Result<(), QueryError> {
        let mut state = self.group_sync.state.lock();
        loop {
            if let Some(message) = state.fatal_error.clone() {
                state.writes.clear();
                return Err(io_error(std::io::Error::other(message)));
            }
            if state.durable_order >= required_order {
                return Ok(());
            }
            if state.syncing {
                self.group_sync.cv.wait(&mut state);
                continue;
            }

            state.syncing = true;
            // Drain already admitted transactions immediately. Later arrivals
            // accumulate naturally during I/O for the next leader; a timer
            // penalizes lone/low-rate writers without creating useful batches.
            let target = state.requested_order;
            let writes = std::mem::take(&mut state.writes);
            let mut writer = state.writer.take();
            let batch_size = writes.len() as u64;
            drop(state);

            let sync_result = self.write_staged_batch(&writes, &mut writer);
            drop(writes);

            state = self.group_sync.state.lock();
            match sync_result {
                Ok(()) => {
                    state.durable_order = state.durable_order.max(target);
                    state.writer = writer;
                    self.group_sync.batches.fetch_add(1, Ordering::Relaxed);
                    self.group_sync
                        .batch_max
                        .fetch_max(batch_size, Ordering::Relaxed);
                }
                Err(err) => {
                    state.fatal_error = Some(err.message().to_string());
                    state.writes.clear();
                }
            }
            state.syncing = false;
            self.group_sync.cv.notify_all();
        }
    }

    fn write_staged_batch(
        &self,
        writes: &BTreeMap<u64, Vec<BinlogEventChunk>>,
        writer: &mut Option<BinlogAppendFile>,
    ) -> Result<(), QueryError> {
        let mut file_chunks: Vec<&BinlogEventChunk> = Vec::new();
        let sync = !writes.is_empty() && self.sync_due(false);
        for chunks in writes.values() {
            for chunk in chunks {
                if writer.as_ref().map(|writer| writer.name.as_str()) != Some(chunk.file.as_str()) {
                    self.finish_staged_file(writer.as_mut(), &file_chunks, sync)?;
                    file_chunks.clear();
                    *writer = Some(BinlogAppendFile {
                        name: chunk.file.clone(),
                        file: OpenOptions::new()
                            .append(true)
                            .open(self.dir.join(&chunk.file))
                            .map_err(io_error)?,
                    });
                    self.group_sync.file_opens.fetch_add(1, Ordering::Relaxed);
                }
                file_chunks.push(chunk);
            }
        }
        self.finish_staged_file(writer.as_mut(), &file_chunks, sync)?;
        Ok(())
    }

    fn finish_staged_file(
        &self,
        writer: Option<&mut BinlogAppendFile>,
        chunks: &[&BinlogEventChunk],
        sync: bool,
    ) -> Result<(), QueryError> {
        if chunks.is_empty() {
            return Ok(());
        }
        let file = &mut writer.expect("nonempty batch has an open file").file;
        let (write_calls, write_bytes) =
            write_event_chunks_vectored(file, chunks).map_err(io_error)?;
        self.group_sync
            .write_calls
            .fetch_add(write_calls, Ordering::Relaxed);
        self.group_sync
            .write_bytes
            .fetch_add(write_bytes, Ordering::Relaxed);
        file.flush().map_err(io_error)?;
        if sync {
            self.sync_file(file)?;
        }
        Ok(())
    }

    fn sync_due(&self, force: bool) -> bool {
        let interval = self.sync_binlog.load(Ordering::Relaxed);
        if interval == 0 {
            self.group_sync
                .groups_since_sync
                .store(0, Ordering::Relaxed);
            return false;
        }
        let groups = self
            .group_sync
            .groups_since_sync
            .fetch_add(1, Ordering::Relaxed);
        if force || groups >= interval - 1 {
            self.group_sync
                .groups_since_sync
                .store(0, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    fn sync_file(&self, file: &File) -> Result<(), QueryError> {
        let started = Instant::now();
        // Match WAL's configured platform durability primitive.
        crate::wal::sync_wal_file(file)?;
        let elapsed_us = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.group_sync
            .sync_total_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        self.group_sync
            .sync_max_us
            .fetch_max(elapsed_us, Ordering::Relaxed);
        Ok(())
    }

    fn publish_txn_commit(&self, commit: Option<BinlogCommit>) -> Result<(), QueryError> {
        let Some(mut commit) = commit else {
            return Ok(());
        };
        self.commit_order.wait_publish(commit.order)?;
        let mut state = self.state.lock();
        let Some(gtid) = commit.reference.gtid.as_deref() else {
            drop(state);
            self.commit_order.finish_publish(commit.order);
            commit.published = true;
            return Ok(());
        };
        for chunk in std::mem::take(&mut commit.chunks) {
            state.push_event(chunk);
        }
        state.trim_published_events();
        state.gtid_executed = append_gtid(&state.gtid_executed, gtid);
        drop(state);
        self.commit_order.finish_publish(commit.order);
        commit.published = true;
        self.notify.notify_waiters();
        Ok(())
    }

    fn checkpoint_manifest_target(&self) -> (u64, String, u64) {
        // The caller owns COMMIT_GATE while this snapshot is captured, so all
        // byte ranges through active_pos belong to orders lower than barrier.
        let barrier = self.commit_order.allocate(false);
        let state = self.state.lock();
        (barrier, state.active_file.clone(), state.active_pos)
    }

    fn persist_checkpoint_manifest(
        &self,
        (barrier, active_file, active_pos): (u64, String, u64),
    ) -> Result<(), QueryError> {
        self.commit_order.wait_publish(barrier)?;
        let state = self.state.lock();
        let result = write_index_file(&self.index_path, &state.files).and_then(|()| {
            write_manifest_file(
                &self.manifest_path,
                &self.config,
                &active_file,
                active_pos,
                &state.gtid_executed,
                &state.gtid_purged,
                state.next_gno,
                state.sequence_number,
            )
        });
        drop(state);
        // Never strand later committers if the metadata write fails. The
        // checkpoint itself still fails, which prevents WAL compaction.
        self.commit_order.finish_publish(barrier);
        result
    }

    #[cfg(test)]
    fn checkpoint_manifest(&self) -> Result<(), QueryError> {
        let target = self.checkpoint_manifest_target();
        self.persist_checkpoint_manifest(target)
    }

    fn rollback_txn(&self, txn_id: u64) {
        self.state.lock().pending.remove(&txn_id);
    }

    fn txn_len(&self, txn_id: u64) -> usize {
        self.state
            .lock()
            .pending
            .get(&txn_id)
            .map(|pending| pending.ops.len())
            .unwrap_or(0)
    }

    fn truncate_txn(&self, txn_id: u64, len: usize) {
        if let Some(pending) = self.state.lock().pending.get_mut(&txn_id) {
            let released = pending
                .ops
                .iter()
                .skip(len)
                .map(binlog_op_nested_bytes)
                .fold(0usize, usize::saturating_add);
            pending.ops.truncate(len);
            pending.memory_charge.shrink(released);
        }
    }

    fn next_gtid_locked(&self, state: &mut BinlogState) -> Result<AllocatedGtid, QueryError> {
        let gno = state.next_gno.max(1);
        if gno >= i64::MAX as u64 || state.sequence_number >= i64::MAX as u64 {
            return Err(error(
                crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
                "binary log GTID or logical sequence number is exhausted",
            ));
        }
        state.next_gno = gno + 1;
        state.sequence_number = state.sequence_number.saturating_add(1).max(1);
        Ok(AllocatedGtid {
            uuid: self.config.server_uuid.clone(),
            gno,
            sequence_number: state.sequence_number,
        })
    }

    fn append_events_locked(
        &self,
        state: &mut BinlogState,
        events: Vec<(u8, Vec<u8>, bool)>,
        publish: bool,
        sync_now: bool,
        write_now: bool,
    ) -> Result<Vec<BinlogEventChunk>, QueryError> {
        if let Some(message) = self.group_sync.state.lock().fatal_error.clone() {
            return Err(io_error(std::io::Error::other(message)));
        }
        let path = self.dir.join(&state.active_file);
        let mut position = state.active_pos;
        let mut appended = Vec::with_capacity(events.len());
        for (event_type, payload, force_checksum) in events {
            let start_pos = position;
            let event_size = encoded_event_size(&self.config, payload.len(), force_checksum)?;
            let end_pos = start_pos
                .checked_add(event_size)
                .filter(|end| *end <= BINLOG_FILE_POSITION_LIMIT)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "binary log event exceeds the 4 GiB event/file-position range",
                    )
                })?;
            let bytes = encode_event(
                &self.config,
                event_type,
                start_pos,
                &payload,
                force_checksum,
            );
            debug_assert_eq!(event_size, bytes.len() as u64);
            let chunk = BinlogEventChunk {
                file: state.active_file.clone(),
                start_pos,
                end_pos,
                event_type,
                gtid: extract_gtid_from_event(&bytes),
                bytes: Arc::new(bytes),
            };
            position = end_pos;
            appended.push(chunk);
        }
        if write_now {
            let result: Result<(), QueryError> = (|| {
                let mut file = OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .map_err(io_error)?;
                let chunk_refs: Vec<&BinlogEventChunk> = appended.iter().collect();
                write_event_chunks_vectored(&mut file, &chunk_refs).map_err(io_error)?;
                file.flush().map_err(io_error)?;
                let rotation = appended
                    .iter()
                    .any(|chunk| chunk.event_type == ROTATE_EVENT);
                if sync_now && self.sync_due(rotation) {
                    self.sync_file(&file)?;
                }
                Ok(())
            })();
            if let Err(err) = result {
                // A partial write cannot be followed by another append at the
                // old reserved position. Leave readers at the published tail.
                self.group_sync.state.lock().fatal_error = Some(err.message().to_string());
                self.group_sync.cv.notify_all();
                return Err(err);
            }
        }
        state.active_pos = position;
        if publish {
            for chunk in &appended {
                state.push_event(chunk.clone());
            }
        }
        if publish && write_now {
            state.trim_published_events();
        }
        Ok(appended)
    }

    fn rotate_locked(&self, state: &mut BinlogState) -> Result<(), QueryError> {
        // rotate() drained and published earlier transactions before taking
        // this latch. Retention may now safely unlink the old file.
        self.group_sync.state.lock().writer = None;
        let mut next_file = next_binlog_file_name(&self.config.base_name, &state.active_file);
        while self.dir.join(&next_file).exists() {
            next_file = next_binlog_file_name(&self.config.base_name, &next_file);
        }
        // MySQL Rotate_log_event payload's position field is the offset of
        // the first real event in the *new* file — always 4 (right after
        // the 4-byte magic). Writing the outgoing file's active_pos leaves
        // the replica seeding Read_Source_Log_Pos to a bogus offset.
        let rotate_payload = encode_rotate_payload(4, &next_file);
        self.append_events_locked(
            state,
            vec![(ROTATE_EVENT, rotate_payload, false)],
            true,
            true,
            true,
        )?;

        let new_path = self.dir.join(&next_file);
        ensure_binlog_file(&new_path)?;
        let start = 4_u64;
        let bootstrap = bootstrap_file_events(&self.config, start, &state.gtid_executed);
        append_bytes(&new_path, &concat_event_bytes(&bootstrap))?;
        let mut pos = start;
        for (event_type, bytes) in bootstrap {
            let end_pos = pos + bytes.len() as u64;
            let chunk = BinlogEventChunk {
                file: next_file.clone(),
                start_pos: pos,
                end_pos,
                event_type,
                gtid: extract_gtid_from_event(&bytes),
                bytes: Arc::new(bytes),
            };
            state.push_event(chunk);
            pos = end_pos;
        }
        state.active_file = next_file.clone();
        state.active_pos = pos;
        state.files.push(BinaryLogInfo {
            name: next_file,
            size: pos,
        });
        self.apply_retention_locked(state)?;
        self.persist_locked(state)
    }

    fn append_disk_gtids(
        &self,
        state: &BinlogState,
        name: &str,
        set: &mut String,
    ) -> Result<(), QueryError> {
        if let Some(&end) = state.disk_prefixes.get(name) {
            disk::visit_gtids(&self.dir.join(name), end, |_, gtid| {
                *set = append_gtid(set, gtid);
                true
            })?;
        }
        Ok(())
    }

    fn maintain_retention(&self, now: SystemTime) -> Result<(), QueryError> {
        // Serialize with commit staging and wait for publication before purge.
        self.flush_all_staged_writes()?;
        let barrier = self.commit_order.state.lock().next_alloc;
        self.commit_order.wait_publish(barrier)?;
        let mut state = self.state.lock();
        self.apply_retention_locked(&mut state)?;
        if let Some(hours) = self.config.retention_hours.filter(|h| *h > 0) {
            while state.files.len() > 1 {
                let name = state.files[0].name.clone();
                let modified = fs::metadata(self.dir.join(&name))
                    .and_then(|m| m.modified())
                    .map_err(|err| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            format!("stat binlog retention: {err}"),
                        )
                    })?;
                if now.duration_since(modified).unwrap_or_default()
                    < Duration::from_secs(hours.saturating_mul(3600))
                {
                    break;
                }
                let mut purged = state.gtid_purged.clone();
                self.append_disk_gtids(&state, &name, &mut purged)?;
                for event in &state.events {
                    if event.file == name {
                        if let Some(gtid) = &event.gtid {
                            purged = append_gtid(&purged, gtid);
                        }
                    }
                }
                state.gtid_purged = purged;
                state.files.remove(0);
                state.events.retain(|e| e.file != name);
                state.disk_prefixes.remove(&name);
                state.rebuild_file_index();
                self.persist_locked(&state)?;
                fs::remove_file(self.dir.join(name)).map_err(|err| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!("purge expired binlog: {err}"),
                    )
                })?;
            }
        }
        Ok(())
    }

    fn apply_retention_locked(&self, state: &mut BinlogState) -> Result<(), QueryError> {
        if let Some(max_bytes) = self.config.retention_bytes {
            while state.files.len() > 1
                && state.files.iter().map(|info| info.size).sum::<u64>() > max_bytes
            {
                let mut purged_gtid = state.gtid_purged.clone();
                self.append_disk_gtids(state, &state.files[0].name, &mut purged_gtid)?;
                let removed = state.files.remove(0);
                if removed.name == state.active_file {
                    state.files.insert(0, removed);
                    break;
                }
                for event in state.events.iter() {
                    if event.file == removed.name {
                        if let Some(gtid) = &event.gtid {
                            purged_gtid = append_gtid(&purged_gtid, gtid);
                        }
                    }
                }
                state.gtid_purged = purged_gtid;
                state.events.retain(|event| event.file != removed.name);
                state.disk_prefixes.remove(&removed.name);
                state.rebuild_file_index();
                self.persist_locked(state)?;
                let _ = fs::remove_file(self.dir.join(removed.name));
            }
        }
        state.rebuild_file_index();
        Ok(())
    }

    fn persist_locked(&self, state: &BinlogState) -> Result<(), QueryError> {
        write_index_file(&self.index_path, &state.files)?;
        write_manifest_file(
            &self.manifest_path,
            &self.config,
            &state.active_file,
            state.published_position(),
            &state.gtid_executed,
            &state.gtid_purged,
            state.next_gno,
            state.sequence_number,
        )
    }
}

fn encode_pending_ops(ops: &[BinlogTxnOp]) -> Result<Vec<(u8, Vec<u8>, bool)>, QueryError> {
    let mut encoded = Vec::new();
    for op in ops {
        match op {
            BinlogTxnOp::Encoded { events } => encoded.extend(events.iter().cloned()),
            BinlogTxnOp::Query { database, sql, ctx } => {
                encoded.push((
                    QUERY_EVENT,
                    encode_query_payload_with(&database, &sql, &ctx)?,
                    false,
                ));
            }
            BinlogTxnOp::Write { table, rows } => {
                encoded.push((TABLE_MAP_EVENT, encode_table_map_payload(&table), false));
                for payload in encode_rows_payloads(
                    &table,
                    RowsPayload::Write(rows),
                    binlog_row_event_max_bytes(),
                )? {
                    encoded.push((WRITE_ROWS_EVENT_V2, payload, false));
                }
            }
            BinlogTxnOp::Update { table, rows } => {
                encoded.push((TABLE_MAP_EVENT, encode_table_map_payload(&table), false));
                for payload in encode_rows_payloads(
                    &table,
                    RowsPayload::Update(rows),
                    binlog_row_event_max_bytes(),
                )? {
                    encoded.push((UPDATE_ROWS_EVENT_V2, payload, false));
                }
            }
            BinlogTxnOp::Delete { table, rows } => {
                encoded.push((TABLE_MAP_EVENT, encode_table_map_payload(&table), false));
                for payload in encode_rows_payloads(
                    &table,
                    RowsPayload::Delete(rows),
                    binlog_row_event_max_bytes(),
                )? {
                    encoded.push((DELETE_ROWS_EVENT_V2, payload, false));
                }
            }
        }
    }
    Ok(encoded)
}

enum RowsPayload<'a> {
    Write(&'a [Vec<Value>]),
    Update(&'a [(Vec<Value>, Vec<Value>)]),
    Delete(&'a [Vec<Value>]),
}

fn show_master_status(session: &SessionContext) -> Result<QueryResponse, QueryError> {
    let snapshot = session
        .binlog_snapshot_status
        .as_deref()
        .cloned()
        .map(Ok)
        .unwrap_or_else(current_status_snapshot)?;
    let position = snapshot.position;
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: AdminCommand::MasterStatus.columns(),
        rows: vec![vec![
            Value::String(position.file),
            Value::UInt(position.position),
            Value::String(String::new()),
            Value::String(String::new()),
            Value::String(snapshot.executed_gtid_set),
        ]],
    }))
}

fn show_binary_logs() -> Result<QueryResponse, QueryError> {
    let logs = binary_logs()?;
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: AdminCommand::BinaryLogs.columns(),
        rows: logs
            .into_iter()
            .map(|log| {
                vec![
                    Value::String(log.name),
                    Value::UInt(log.size),
                    Value::String("No".to_string()),
                ]
            })
            .collect(),
    }))
}

/// MySQL 8.0 `SHOW REPLICAS` (older alias `SHOW SLAVE HOSTS`). Lists the
/// replicas that have registered via `COM_REGISTER_SLAVE`. The `Replica_UUID`
/// column is populated from the `@slave_uuid` / `@replica_uuid` session
/// variable the replica set before registering (captured into the registry
/// entry by `handle_register_slave`); empty when the replica advertised none.
/// Columns match MySQL 8.0.
fn show_replicas() -> Result<QueryResponse, QueryError> {
    let manager = require_manager()?;
    let entries = manager.registered_replicas();
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: AdminCommand::Replicas.columns(),
        rows: entries
            .into_iter()
            .map(|info| {
                vec![
                    Value::UInt(info.server_id as u64),
                    Value::String(info.host),
                    Value::UInt(info.port as u64),
                    Value::UInt(info.master_id as u64),
                    Value::String(info.replica_uuid),
                ]
            })
            .collect(),
    }))
}

fn should_skip_recording() -> bool {
    !enabled()
        || (crate::replication::replication_applier_active()
            && !config_snapshot().log_replica_updates)
        || !current_session_sql_log_bin()
}

thread_local! {
    /// Per-thread snapshot of the executing session's `sql_log_bin`. Set
    /// by [`SessionSqlLogBinGuard`] at the top of `execute_statement`,
    /// restored on drop. `true` (the default) means binlog recording
    /// proceeds; `false` makes every `record_*` call a no-op until the
    /// guard restores the previous value.
    static SQL_LOG_BIN_ENABLED: Cell<bool> = Cell::new(true);
    static DDL_TXN_CONTEXT: RefCell<Option<DdlTxnContext>> = const { RefCell::new(None) };
}

#[derive(Clone)]
struct DdlTxnContext {
    database: String,
    sql: String,
    query: BinlogQueryContext,
    staged: bool,
}

#[must_use]
pub struct DdlTxnContextGuard {
    prior: Option<DdlTxnContext>,
    _replica_receipt: Option<crate::replication::DdlReceiptGuard>,
}

pub fn enter_ddl_txn_context(
    database: &str,
    sql: &str,
    query: &BinlogQueryContext,
) -> DdlTxnContextGuard {
    let current = DdlTxnContext {
        database: database.to_string(),
        sql: sql.to_string(),
        query: query.clone(),
        staged: false,
    };
    let prior = DDL_TXN_CONTEXT.with(|slot| slot.replace(Some(current)));
    DdlTxnContextGuard {
        prior,
        _replica_receipt: None,
    }
}

/// Internal catalog compensation must not replicate the rejected outer DDL.
pub(crate) fn suspend_ddl_txn_context() -> DdlTxnContextGuard {
    DdlTxnContextGuard {
        prior: DDL_TXN_CONTEXT.with(|slot| slot.take()),
        _replica_receipt: Some(crate::replication::suspend_ddl_receipt()),
    }
}

pub fn stage_current_ddl_transaction(txn_id: u64) -> Result<bool, QueryError> {
    if !enabled() {
        return Ok(false);
    }
    DDL_TXN_CONTEXT.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(context) = slot.as_mut() else {
            return Ok(false);
        };
        if context.staged {
            return Ok(true);
        }
        // CatalogTxn suppresses row-level DD binlogging. Re-enable recording
        // only for the outer logical DDL event; the context was installed
        // only when the issuing session had sql_log_bin enabled.
        let _logical_ddl = SessionSqlLogBinGuard::new(true);
        record_ddl_transactional_with(txn_id, &context.database, &context.sql, &context.query)?;
        context.staged = true;
        Ok(true)
    })
}

pub fn current_ddl_transaction_staged() -> bool {
    DDL_TXN_CONTEXT.with(|slot| slot.borrow().as_ref().is_some_and(|context| context.staged))
}

impl Drop for DdlTxnContextGuard {
    fn drop(&mut self) {
        DDL_TXN_CONTEXT.with(|slot| {
            slot.replace(self.prior.take());
        });
    }
}

fn current_session_sql_log_bin() -> bool {
    SQL_LOG_BIN_ENABLED.with(|c| c.get())
}

/// RAII guard that swaps the thread-local `sql_log_bin` flag for the
/// duration of one statement execution and restores it on drop. The
/// guard is `must_use` to make accidental drop-immediately bugs (the
/// pattern `let _ = ...` is the right way to anchor it) visible at the
/// call site.
#[must_use]
pub struct SessionSqlLogBinGuard {
    prior: bool,
}

impl SessionSqlLogBinGuard {
    pub fn new(enabled: bool) -> Self {
        let prior = SQL_LOG_BIN_ENABLED.with(|c| {
            let p = c.get();
            c.set(enabled);
            p
        });
        Self { prior }
    }
}

impl Drop for SessionSqlLogBinGuard {
    fn drop(&mut self) {
        SQL_LOG_BIN_ENABLED.with(|c| c.set(self.prior));
    }
}

fn binlog_table_from_schema(schema: &TableSchema) -> Result<BinlogTable, QueryError> {
    let table_id = schema.object_id.max(schema.id) & 0x0000_ffff_ffff_ffff;
    let columns = schema
        .iter_visible_columns()
        .map(|(idx, column)| {
            let (column_type, mut metadata) = column_binlog_type(&column.data_type);
            let textual = matches!(
                column.data_type,
                DataTypeKind::Char { .. }
                    | DataTypeKind::VarChar { .. }
                    | DataTypeKind::Text { .. }
                    | DataTypeKind::Enum { .. }
                    | DataTypeKind::Set { .. }
            );
            let (charset, collation_id) = if textual {
                let charset = crate::charset::resolve_charset(
                    column.charset.as_deref().unwrap_or(&schema.charset),
                )?;
                let collation = if let Some(name) = column.collation.as_deref() {
                    crate::charset::resolve_collation(name)?
                } else if column.charset.is_none() {
                    crate::charset::resolve_collation(&schema.collation)?
                } else {
                    crate::charset::resolve_collation(charset.default_collation)?
                };
                if collation.charset != charset.name {
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        format!(
                            "binlog column '{}' has inconsistent charset and collation",
                            column.name
                        ),
                    ));
                }
                match column.data_type {
                    DataTypeKind::VarChar { len } => {
                        let bytes = (len.unwrap_or(255).max(1) as u32)
                            .saturating_mul(u32::from(charset.max_len))
                            .min(u16::MAX as u32) as u16;
                        metadata = bytes.to_le_bytes().to_vec();
                    }
                    DataTypeKind::Char { len } => {
                        let bytes =
                            (len.unwrap_or(1).max(1).min(255) as u16) * u16::from(charset.max_len);
                        metadata = vec![
                            MYSQL_TYPE_STRING ^ ((bytes & 0x300) >> 4) as u8,
                            bytes as u8,
                        ];
                    }
                    _ => {}
                }
                (Some(charset.name), collation.id)
            } else {
                (None, crate::charset::BINARY_COLLATION_ID)
            };
            let enum_set_values = match &column.data_type {
                DataTypeKind::Enum { values } | DataTypeKind::Set { values } => {
                    Some(Arc::new(values.clone()))
                }
                _ => None,
            };
            Ok(BinlogColumn {
                name: column.name.clone(),
                physical_index: idx,
                column_type,
                metadata,
                nullable: column.nullable,
                unsigned: column.unsigned,
                enum_set_values,
                vector_dim: match &column.data_type {
                    DataTypeKind::Vector { dim, .. } => Some(dim.unwrap_or(0)),
                    _ => None,
                },
                charset,
                collation_id,
            })
        })
        .collect::<Result<Vec<_>, QueryError>>()?;
    Ok(BinlogTable {
        table_id,
        database: schema.database.clone(),
        name: schema.name.clone(),
        columns,
    })
}

fn project_visible_row(table: &BinlogTable, row: Vec<Value>) -> Vec<Value> {
    if row.len() == table.columns.len()
        && table
            .columns
            .iter()
            .enumerate()
            .all(|(idx, col)| col.physical_index == idx)
    {
        return row;
    }
    let mut projected = Vec::with_capacity(table.columns.len());
    for column in &table.columns {
        projected.push(
            row.get(column.physical_index)
                .cloned()
                .unwrap_or(Value::Null),
        );
    }
    projected
}

fn column_binlog_type(data_type: &DataTypeKind) -> (u8, Vec<u8>) {
    match data_type {
        // MySQL 8.0 has no native VECTOR type. LONG BLOB is understood by every
        // 8.0 replica; `encode_value` adds a magic+dimension envelope.
        DataTypeKind::Vector { .. } => (MYSQL_TYPE_BLOB, vec![4]),
        DataTypeKind::TinyInt | DataTypeKind::Bool => (MYSQL_TYPE_TINY, vec![]),
        DataTypeKind::SmallInt => (MYSQL_TYPE_SHORT, vec![]),
        DataTypeKind::MediumInt => (MYSQL_TYPE_INT24, vec![]),
        DataTypeKind::Int => (MYSQL_TYPE_LONG, vec![]),
        DataTypeKind::BigInt => (MYSQL_TYPE_LONGLONG, vec![]),
        DataTypeKind::Float { .. } => (MYSQL_TYPE_FLOAT, vec![4]),
        DataTypeKind::Double { .. } => (MYSQL_TYPE_DOUBLE, vec![8]),
        DataTypeKind::Decimal { precision, scale } => (
            MYSQL_TYPE_NEWDECIMAL,
            vec![precision.unwrap_or(18), scale.unwrap_or(0)],
        ),
        // BINARY(n): MYSQL_TYPE_STRING on the wire (same shape as CHAR),
        // metadata packs the byte length using the XOR trick.
        DataTypeKind::Binary { len } => {
            let bytes = len.unwrap_or(1).max(1).min(255) as u16;
            let hi = ((bytes & 0x300) >> 4) as u8;
            let meta0 = MYSQL_TYPE_STRING ^ hi;
            let meta1 = (bytes & 0xff) as u8;
            (MYSQL_TYPE_STRING, vec![meta0, meta1])
        }
        // VARBINARY(n): MYSQL_TYPE_VARCHAR (same wire framing as VARCHAR)
        // with a 2-byte metadata carrying the declared byte length.
        DataTypeKind::VarBinary { len } => {
            let bytes = (len.unwrap_or(255) as u32).min(u16::MAX as u32) as u16;
            (MYSQL_TYPE_VARCHAR, bytes.to_le_bytes().to_vec())
        }
        // BLOB family: metadata[0] = length-prefix width matching declared
        // size (TINYBLOB=1 / BLOB=2 / MEDIUMBLOB=3 / LONGBLOB=4). Emitting
        // metadata=4 for a small BLOB makes the replica error "cannot
        // convert longblob to blob".
        DataTypeKind::Blob { max_len } => {
            let width = if *max_len <= 0xFF {
                1
            } else if *max_len <= 0xFFFF {
                2
            } else if *max_len <= 0xFF_FFFF {
                3
            } else {
                4
            };
            (MYSQL_TYPE_BLOB, vec![width])
        }
        DataTypeKind::Json => (MYSQL_TYPE_JSON, vec![4]),
        DataTypeKind::Geometry { .. } => (MYSQL_TYPE_GEOMETRY, vec![4]),
        DataTypeKind::Date => (MYSQL_TYPE_DATE, vec![]),
        DataTypeKind::Time { fsp } => (MYSQL_TYPE_TIME2, vec![(*fsp).min(6)]),
        DataTypeKind::DateTime { fsp } => (MYSQL_TYPE_DATETIME2, vec![(*fsp).min(6)]),
        DataTypeKind::Timestamp { fsp } => (MYSQL_TYPE_TIMESTAMP2, vec![(*fsp).min(6)]),
        DataTypeKind::Year => (MYSQL_TYPE_YEAR, vec![]),
        DataTypeKind::Bit { len } => {
            let bits = len.unwrap_or(1).max(1);
            let rem = (bits % 8) as u8;
            let full_bytes = (bits / 8) as u8;
            (MYSQL_TYPE_BIT, vec![rem, full_bytes])
        }
        DataTypeKind::Enum { values } => {
            let width = crate::storage::enum_storage_width(values.len());
            (MYSQL_TYPE_ENUM, vec![MYSQL_TYPE_ENUM, width])
        }
        DataTypeKind::Set { values } => {
            let width = crate::storage::set_storage_width(values.len());
            (MYSQL_TYPE_SET, vec![MYSQL_TYPE_SET, width])
        }
        // CHAR/VARCHAR — emit the declared byte length as the metadata so
        // downstream replicas (vanilla MySQL chief among them) accept the
        // event without falling back to "Column N cannot be converted
        // from varchar(65535(bytes))" — a default we used to emit when
        // the table_map metadata wasn't byte-tight. `len` is the column's
        // character length; assume worst-case utf8mb4 (4 bytes/char),
        // which matches MySQL's own table_map emission for `utf8mb4`.
        DataTypeKind::VarChar { len } => {
            let chars = len.unwrap_or(255).max(1) as u32;
            let bytes = (chars.saturating_mul(4)).min(u16::MAX as u32) as u16;
            let mut meta = Vec::with_capacity(2);
            meta.extend_from_slice(&bytes.to_le_bytes());
            (MYSQL_TYPE_VARCHAR, meta)
        }
        // CHAR is emitted as MYSQL_TYPE_STRING (0xfe). MySQL packs the
        // 10-bit `max_bytes` (chars × mbmaxlen, up to 1020 for utf8mb4) into
        // two metadata bytes: `metadata[0] = 0xfe XOR ((max & 0x300) >> 4)`
        // and `metadata[1] = max & 0xff`. Emitting VARCHAR here makes the
        // replica error "cannot be converted from varchar to char(utf8mb4)".
        DataTypeKind::Char { len } => {
            let chars = len.unwrap_or(1).max(1).min(255) as u32;
            let max_bytes = (chars.saturating_mul(4)).min(1023) as u16;
            let hi = ((max_bytes & 0x300) >> 4) as u8;
            let meta0 = MYSQL_TYPE_STRING ^ hi;
            let meta1 = (max_bytes & 0xff) as u8;
            (MYSQL_TYPE_STRING, vec![meta0, meta1])
        }
        // TEXT family stores as BLOB on the wire; the metadata byte is
        // the on-disk length-prefix width (1/2/3/4 bytes for
        // TINYTEXT/TEXT/MEDIUMTEXT/LONGTEXT respectively). Pick the
        // smallest prefix that fits.
        DataTypeKind::Text { max_len } => {
            let width = if *max_len <= 0xFF {
                1
            } else if *max_len <= 0xFFFF {
                2
            } else if *max_len <= 0xFF_FFFF {
                3
            } else {
                4
            };
            (MYSQL_TYPE_BLOB, vec![width])
        }
    }
}

fn encode_format_description_event(config: &BinlogConfig, start_pos: u64) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&BINLOG_VERSION.to_le_bytes());
    let mut server_version = [0u8; 50];
    let version_bytes = MYSQL_SERVER_VERSION.as_bytes();
    let len = version_bytes.len().min(server_version.len());
    server_version[..len].copy_from_slice(&version_bytes[..len]);
    payload.extend_from_slice(&server_version);
    payload.extend_from_slice(&current_unix_seconds().to_le_bytes());
    payload.push(EVENT_HEADER_LEN);

    let mut post_header = vec![0u8; 41];
    set_post_header_len(&mut post_header, QUERY_EVENT, 13);
    set_post_header_len(&mut post_header, ROTATE_EVENT, 8);
    set_post_header_len(&mut post_header, FORMAT_DESCRIPTION_EVENT, 57 + 41);
    set_post_header_len(&mut post_header, XID_EVENT, 0);
    set_post_header_len(&mut post_header, TABLE_MAP_EVENT, 8);
    set_post_header_len(&mut post_header, HEARTBEAT_EVENT, 0);
    set_post_header_len(&mut post_header, WRITE_ROWS_EVENT_V2, 10);
    set_post_header_len(&mut post_header, UPDATE_ROWS_EVENT_V2, 10);
    set_post_header_len(&mut post_header, DELETE_ROWS_EVENT_V2, 10);
    set_post_header_len(&mut post_header, GTID_EVENT, 42);
    set_post_header_len(&mut post_header, ANONYMOUS_GTID_EVENT, 42);
    set_post_header_len(&mut post_header, PREVIOUS_GTIDS_EVENT, 0);
    payload.extend_from_slice(&post_header);
    payload.push(config.checksum.descriptor_byte());
    encode_event(config, FORMAT_DESCRIPTION_EVENT, start_pos, &payload, true)
}

fn set_post_header_len(post_header: &mut [u8], event_type: u8, len: u8) {
    let idx = event_type.saturating_sub(1) as usize;
    if let Some(slot) = post_header.get_mut(idx) {
        *slot = len;
    }
}

fn encode_event(
    config: &BinlogConfig,
    event_type: u8,
    start_pos: u64,
    payload: &[u8],
    force_checksum: bool,
) -> Vec<u8> {
    encode_event_with_timestamp(
        config,
        event_type,
        current_unix_seconds(),
        start_pos,
        payload,
        force_checksum,
    )
}

fn encoded_event_size(
    config: &BinlogConfig,
    payload_len: usize,
    force_checksum: bool,
) -> Result<u64, QueryError> {
    let checksum_len = usize::from(force_checksum || config.checksum == BinlogChecksum::Crc32) * 4;
    let size = (EVENT_HEADER_LEN as usize)
        .checked_add(payload_len)
        .and_then(|size| size.checked_add(checksum_len))
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                "binary log event size overflow",
            )
        })?;
    u32::try_from(size).map(u64::from).map_err(|_| {
        error(
            crate::error::catalog::ER_TOO_BIG_ROWSIZE,
            "binary log event exceeds the 4 GiB event-size range",
        )
    })
}

fn encoded_event_bytes(
    config: &BinlogConfig,
    events: &[(u8, Vec<u8>, bool)],
) -> Result<u64, QueryError> {
    events
        .iter()
        .try_fold(0u64, |total, (_, payload, force_checksum)| {
            total
                .checked_add(encoded_event_size(config, payload.len(), *force_checksum)?)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
                        "binary log transaction size overflow",
                    )
                })
        })
}

/// Encode a synthetic `HEARTBEAT_LOG_EVENT` with:
/// - `timestamp = 0` (marks it artificial per MySQL convention)
/// - `end_log_pos = current_tail` exactly (not `tail + event_size`), so
///   the replica's IO thread does not see the source's stream position
///   go backwards.
/// - `flags = LOG_EVENT_ARTIFICIAL_F (0x0020)` as a belt-and-suspenders
///   marker.
fn encode_heartbeat_event(
    config: &BinlogConfig,
    current_tail: u64,
    file_name: &[u8],
) -> Result<Vec<u8>, QueryError> {
    let has_checksum = config.checksum == BinlogChecksum::Crc32;
    let event_size = EVENT_HEADER_LEN as usize + file_name.len() + usize::from(has_checksum) * 4;
    let mut out = Vec::with_capacity(event_size);
    out.extend_from_slice(&0u32.to_le_bytes()); // timestamp = 0 (synthetic)
    out.push(HEARTBEAT_EVENT);
    out.extend_from_slice(&config.server_id.to_le_bytes());
    out.extend_from_slice(&(event_size as u32).to_le_bytes());
    // log_pos = current tail (NOT tail + event_size): per MySQL
    // Heartbeat_log_event this reports the source's currently-durable
    // position, from which the replica advances Read_Source_Log_Pos.
    out.extend_from_slice(&(current_tail as u32).to_le_bytes());
    out.extend_from_slice(&0x0020u16.to_le_bytes()); // LOG_EVENT_ARTIFICIAL_F
    out.extend_from_slice(file_name);
    if has_checksum {
        let mut hasher = Hasher::new();
        hasher.update(&out);
        out.extend_from_slice(&hasher.finalize().to_le_bytes());
    }
    Ok(out)
}

// Artificial events carry no on-disk next position. Only FDE always has a
// checksum trailer; fake ROTATE follows the negotiated file algorithm.
fn mark_dump_event_artificial(event: &mut [u8], has_checksum: bool) {
    event[0..4].fill(0);
    event[13..17].fill(0);
    event[17..19].copy_from_slice(&0x20u16.to_le_bytes());
    if has_checksum {
        let end = event.len() - 4;
        let checksum = crc32fast::hash(&event[..end]);
        event[end..].copy_from_slice(&checksum.to_le_bytes());
    }
}

/// Lower-level variant of [`encode_event`] that takes an explicit timestamp
/// — used to synthesize a *fake* `ROTATE_EVENT` at dump-stream start
/// (timestamp 0 → "this is artificial, ignore my header position").
fn encode_event_with_timestamp(
    config: &BinlogConfig,
    event_type: u8,
    timestamp: u32,
    start_pos: u64,
    payload: &[u8],
    force_checksum: bool,
) -> Vec<u8> {
    let has_checksum = force_checksum || config.checksum == BinlogChecksum::Crc32;
    let event_size = EVENT_HEADER_LEN as usize + payload.len() + usize::from(has_checksum) * 4;
    let next_pos = start_pos.saturating_add(event_size as u64);
    let mut out = Vec::with_capacity(event_size);
    out.extend_from_slice(&timestamp.to_le_bytes());
    out.push(event_type);
    out.extend_from_slice(&config.server_id.to_le_bytes());
    out.extend_from_slice(&(event_size as u32).to_le_bytes());
    out.extend_from_slice(&(next_pos as u32).to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(payload);
    if has_checksum {
        let mut hasher = Hasher::new();
        hasher.update(&out);
        out.extend_from_slice(&hasher.finalize().to_le_bytes());
    }
    out
}

fn encode_query_payload(thread_id: u32, database: &str, sql: &str) -> Vec<u8> {
    let mut ctx = BinlogQueryContext::default();
    ctx.thread_id = thread_id;
    encode_query_payload_bytes(database, sql.as_bytes(), &ctx)
}

fn encode_query_status_vars(ctx: &BinlogQueryContext) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    // Q_FLAGS2_CODE = 0x00 — OPTIONS_WRITTEN_TO_BIN_LOG
    let mut flags2: u32 = 0;
    if !ctx.foreign_key_checks {
        flags2 |= 0x0400_0000; // OPTION_NO_FOREIGN_KEY_CHECKS = 1UL<<26
    }
    if !ctx.unique_checks {
        flags2 |= 0x0800_0000; // OPTION_RELAXED_UNIQUE_CHECKS = 1UL<<27
    }
    buf.push(0x00);
    buf.extend_from_slice(&flags2.to_le_bytes());

    // Q_SQL_MODE_CODE = 0x01
    buf.push(0x01);
    buf.extend_from_slice(&ctx.sql_mode.to_le_bytes());

    // Q_CATALOG_NZ_CODE = 0x06 — non-NUL terminated catalog "std"
    let catalog = b"std";
    buf.push(0x06);
    buf.push(catalog.len() as u8);
    buf.extend_from_slice(catalog);

    // Q_AUTO_INCREMENT = 0x03 — emit only when non-default
    if ctx.auto_increment_increment != 1 || ctx.auto_increment_offset != 1 {
        buf.push(0x03);
        buf.extend_from_slice(&ctx.auto_increment_increment.to_le_bytes());
        buf.extend_from_slice(&ctx.auto_increment_offset.to_le_bytes());
    }

    // Q_CHARSET_CODE = 0x04
    buf.push(0x04);
    buf.extend_from_slice(&ctx.client_charset_id.to_le_bytes());
    buf.extend_from_slice(&ctx.connection_collation_id.to_le_bytes());
    buf.extend_from_slice(&ctx.server_collation_id.to_le_bytes());

    // Q_TIME_ZONE_CODE = 0x05 — empty length suppresses; emit when non-SYSTEM
    if !ctx.time_zone.is_empty() && ctx.time_zone.to_ascii_uppercase() != "SYSTEM" {
        let tz_bytes = ctx.time_zone.as_bytes();
        let tz_len = tz_bytes.len().min(u8::MAX as usize);
        buf.push(0x05);
        buf.push(tz_len as u8);
        buf.extend_from_slice(&tz_bytes[..tz_len]);
    }

    // Q_LC_TIME_NAMES_CODE = 0x07
    buf.push(0x07);
    buf.extend_from_slice(&ctx.lc_time_names.to_le_bytes());

    // Q_CHARSET_DATABASE_CODE = 0x08
    buf.push(0x08);
    buf.extend_from_slice(&ctx.charset_database_id.to_le_bytes());

    // Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP = 0x10
    buf.push(0x10);
    buf.push(if ctx.explicit_defaults_for_timestamp {
        1
    } else {
        0
    });
    buf
}

fn encode_query_payload_with(
    database: &str,
    sql: &str,
    ctx: &BinlogQueryContext,
) -> Result<Vec<u8>, QueryError> {
    if database.len() > u8::MAX as usize {
        return Err(error(
            crate::error::catalog::ER_DATA_TOO_LONG,
            "database name exceeds the binary log QUERY_EVENT length field",
        ));
    }
    let charset = crate::charset::resolve_collation_id_checked(ctx.client_charset_id)?.charset;
    crate::charset::resolve_client_charset(charset)?;
    let bytes = if charset == "utf8mb4" {
        Cow::Borrowed(sql.as_bytes())
    } else {
        Cow::Owned(crate::charset::encode_string(charset, sql)?)
    };
    Ok(encode_query_payload_bytes(database, &bytes, ctx))
}

fn encode_query_payload_bytes(database: &str, sql: &[u8], ctx: &BinlogQueryContext) -> Vec<u8> {
    let schema = database.as_bytes();
    let schema_len = schema.len().min(u8::MAX as usize);
    let status_vars = encode_query_status_vars(ctx);
    let status_len = status_vars.len().min(u16::MAX as usize) as u16;
    let mut payload = Vec::with_capacity(13 + status_vars.len() + schema_len + 1 + sql.len());
    payload.extend_from_slice(&ctx.thread_id.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes()); // execution_time
    payload.push(schema_len as u8);
    payload.extend_from_slice(&0u16.to_le_bytes()); // error_code
    payload.extend_from_slice(&status_len.to_le_bytes());
    payload.extend_from_slice(&status_vars[..status_len as usize]);
    payload.extend_from_slice(&schema[..schema_len]);
    payload.push(0); // schema NUL
    payload.extend_from_slice(sql);
    payload
}

fn encode_gtid_payload(
    config: &BinlogConfig,
    gno: u64,
    sequence_number: u64,
    may_have_statement: bool,
) -> Vec<u8> {
    let sid = uuid_string_to_bytes(&config.server_uuid).unwrap_or([0u8; 16]);
    let mut payload = Vec::with_capacity(64);
    // MAY_HAVE_SBR is required for DDL even in a ROW-format binary log.
    // Clearing it promises MySQL that BEGIN/COMMIT are the only Query events.
    payload.push(u8::from(may_have_statement));
    payload.extend_from_slice(&sid);
    payload.extend_from_slice(&gno.to_le_bytes());
    payload.push(2); // LOGICAL_TIMESTAMP_TYPECODE
                     // last_committed = seq - 1 keeps the layout compatible but forces
                     // replicas to serialise MTS apply. A dedicated fix (group-commit
                     // parent) is tracked in M2-9.
    payload.extend_from_slice(&sequence_number.saturating_sub(1).to_le_bytes());
    payload.extend_from_slice(&sequence_number.to_le_bytes());

    // 8.0.1+: immediate_commit_timestamp (7 bytes, low 56 bits micros).
    // Setting the MSB (bit 55) means original_commit_timestamp differs and
    // follows; clearing it means "originated on this server, no original
    // follows". We always clear it (mydb is the origin) — 7 bytes total.
    let now_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
        & ((1u64 << 55) - 1);
    let ts_le = now_micros.to_le_bytes();
    payload.extend_from_slice(&ts_le[..7]);

    // 8.0.2+: transaction_length (lenenc). 0 = "unknown"; replicas that
    // want it for skip-optimisation gracefully fall back to per-event walk.
    payload.push(0);

    // 8.0.14+: immediate_server_version (4 bytes LE). MSB set signals
    // original_server_version follows; keep it aligned with the advertised
    // compatibility version used by handshake and executable comments.
    let mydb_as_mysql_version = crate::server_version::MYSQL_COMPAT_VERSION_ID;
    payload.extend_from_slice(&mydb_as_mysql_version.to_le_bytes());
    payload
}

fn encode_previous_gtids_payload(gtid_set: &str) -> Vec<u8> {
    let map = parse_gtid_interval_map(gtid_set);
    let mut payload = Vec::new();
    payload.extend_from_slice(&(map.len() as u64).to_le_bytes());
    for (uuid, intervals) in map {
        let sid = uuid_string_to_bytes(&uuid).unwrap_or([0u8; 16]);
        payload.extend_from_slice(&sid);
        payload.extend_from_slice(&(intervals.len() as u64).to_le_bytes());
        for (lo, hi) in intervals {
            payload.extend_from_slice(&lo.to_le_bytes());
            payload.extend_from_slice(&hi.saturating_add(1).to_le_bytes());
        }
    }
    payload
}

fn encode_rotate_payload(position: u64, next_file: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(8 + next_file.len());
    payload.extend_from_slice(&position.to_le_bytes());
    payload.extend_from_slice(next_file.as_bytes());
    payload
}

fn encode_table_map_payload(table: &BinlogTable) -> Vec<u8> {
    let mut payload = Vec::new();
    write_u48_le(&mut payload, table.table_id);
    payload.extend_from_slice(&0u16.to_le_bytes());
    payload.push(table.database.len().min(u8::MAX as usize) as u8);
    payload.extend_from_slice(
        &table.database.as_bytes()[..table.database.len().min(u8::MAX as usize)],
    );
    payload.push(0);
    payload.push(table.name.len().min(u8::MAX as usize) as u8);
    payload.extend_from_slice(&table.name.as_bytes()[..table.name.len().min(u8::MAX as usize)]);
    payload.push(0);
    write_lenenc_int(&mut payload, table.columns.len() as u64);
    for column in &table.columns {
        payload.push(column.column_type);
    }
    let mut metadata = Vec::new();
    for column in &table.columns {
        metadata.extend_from_slice(&column.metadata);
    }
    write_lenenc_int(&mut payload, metadata.len() as u64);
    payload.extend_from_slice(&metadata);
    payload.extend_from_slice(&nullable_bitmap(table));
    payload.extend_from_slice(&table_map_optional_metadata(table));
    payload
}

fn nullable_bitmap(table: &BinlogTable) -> Vec<u8> {
    let mut bitmap = vec![0u8; (table.columns.len() + 7) / 8];
    for (idx, column) in table.columns.iter().enumerate() {
        if column.nullable {
            bitmap[idx / 8] |= 1 << (idx % 8);
        }
    }
    bitmap
}

fn table_map_optional_metadata(table: &BinlogTable) -> Vec<u8> {
    let mut out = Vec::new();
    if let Some(signedness) = signedness_metadata(table) {
        push_optional_metadata_field(&mut out, OPTIONAL_METADATA_SIGNEDNESS, &signedness);
    }
    push_optional_metadata_field(
        &mut out,
        OPTIONAL_METADATA_COLUMN_NAME,
        &column_name_metadata(table),
    );
    // COLUMN_CHARSET and ENUM_AND_SET_COLUMN_CHARSET contain packed
    // collation IDs in the order of their respective wire column classes.
    for (kind, enum_set) in [(3, false), (11, true)] {
        let mut ids = Vec::new();
        for column in &table.columns {
            let matches = if enum_set {
                matches!(column.column_type, MYSQL_TYPE_ENUM | MYSQL_TYPE_SET)
            } else {
                matches!(
                    column.column_type,
                    MYSQL_TYPE_STRING | MYSQL_TYPE_VARCHAR | MYSQL_TYPE_BLOB
                )
            };
            if matches {
                write_lenenc_int(&mut ids, u64::from(column.collation_id));
            }
        }
        if !ids.is_empty() {
            push_optional_metadata_field(&mut out, kind, &ids);
        }
    }
    out
}

fn push_optional_metadata_field(out: &mut Vec<u8>, field_type: u8, value: &[u8]) {
    out.push(field_type);
    write_lenenc_int(out, value.len() as u64);
    out.extend_from_slice(value);
}

fn signedness_metadata(table: &BinlogTable) -> Option<Vec<u8>> {
    let numeric_columns = table
        .columns
        .iter()
        .filter(|column| is_numeric_binlog_type(column.column_type))
        .count();
    if numeric_columns == 0 {
        return None;
    }
    let mut bitmap = vec![0u8; (numeric_columns + 7) / 8];
    let mut numeric_idx = 0usize;
    for column in &table.columns {
        if !is_numeric_binlog_type(column.column_type) {
            continue;
        }
        if column.unsigned {
            bitmap[numeric_idx / 8] |= 1 << (7 - numeric_idx % 8);
        }
        numeric_idx += 1;
    }
    Some(bitmap)
}

fn is_numeric_binlog_type(column_type: u8) -> bool {
    matches!(
        column_type,
        MYSQL_TYPE_TINY
            | MYSQL_TYPE_SHORT
            | MYSQL_TYPE_INT24
            | MYSQL_TYPE_LONG
            | MYSQL_TYPE_LONGLONG
            | MYSQL_TYPE_NEWDECIMAL
            | MYSQL_TYPE_FLOAT
            | MYSQL_TYPE_DOUBLE
            | MYSQL_TYPE_YEAR
    )
}

fn column_name_metadata(table: &BinlogTable) -> Vec<u8> {
    let mut out = Vec::new();
    for column in &table.columns {
        let bytes = column.name.as_bytes();
        write_lenenc_int(&mut out, bytes.len() as u64);
        out.extend_from_slice(bytes);
    }
    out
}

fn rows_payload_prefix(table: &BinlogTable, update: bool) -> Vec<u8> {
    let mut payload = Vec::new();
    write_u48_le(&mut payload, table.table_id);
    // Patched to STMT_END_F on the final chunk only.
    payload.extend_from_slice(&0u16.to_le_bytes());
    payload.extend_from_slice(&2u16.to_le_bytes());
    write_lenenc_int(&mut payload, table.columns.len() as u64);
    let columns_bitmap = all_columns_bitmap(table.columns.len());
    payload.extend_from_slice(&columns_bitmap);
    if update {
        payload.extend_from_slice(&columns_bitmap);
    }
    payload
}

fn append_encoded_row_chunk(
    chunks: &mut Vec<Vec<u8>>,
    prefix: &[u8],
    current: &mut Vec<u8>,
    row: &[u8],
    max_payload_bytes: usize,
) {
    if current.len() > prefix.len() && current.len().saturating_add(row.len()) > max_payload_bytes {
        chunks.push(std::mem::replace(current, prefix.to_vec()));
    }
    current.extend_from_slice(row);
}

/// Encode a statement as one or more row events. A single row may exceed the
/// configured chunk target (it remains indivisible), but can never overflow
/// MySQL's u32 event-size field. Only the last event carries STMT_END_F.
fn encode_rows_payloads(
    table: &BinlogTable,
    rows: RowsPayload<'_>,
    max_payload_bytes: usize,
) -> Result<Vec<Vec<u8>>, QueryError> {
    let update = matches!(&rows, RowsPayload::Update(_));
    let prefix = rows_payload_prefix(table, update);
    let max_payload_bytes = max_payload_bytes.max(prefix.len().saturating_add(1));
    let mut chunks = Vec::new();
    let mut current = prefix.clone();
    match rows {
        RowsPayload::Write(rows) => {
            for row in rows {
                let mut encoded_row = Vec::new();
                encode_row_values(table, &row, &mut encoded_row)?;
                append_encoded_row_chunk(
                    &mut chunks,
                    &prefix,
                    &mut current,
                    &encoded_row,
                    max_payload_bytes,
                );
            }
        }
        RowsPayload::Delete(rows) => {
            for row in rows {
                let mut encoded_row = Vec::new();
                encode_row_values(table, &row, &mut encoded_row)?;
                append_encoded_row_chunk(
                    &mut chunks,
                    &prefix,
                    &mut current,
                    &encoded_row,
                    max_payload_bytes,
                );
            }
        }
        RowsPayload::Update(rows) => {
            for (before, after) in rows {
                let mut encoded_row = Vec::new();
                encode_row_values(table, &before, &mut encoded_row)?;
                encode_row_values(table, &after, &mut encoded_row)?;
                append_encoded_row_chunk(
                    &mut chunks,
                    &prefix,
                    &mut current,
                    &encoded_row,
                    max_payload_bytes,
                );
            }
        }
    }
    chunks.push(current);
    if let Some(last) = chunks.last_mut() {
        last[6..8].copy_from_slice(&1u16.to_le_bytes());
    }
    Ok(chunks)
}

fn all_columns_bitmap(count: usize) -> Vec<u8> {
    let mut bitmap = vec![0u8; (count + 7) / 8];
    for idx in 0..count {
        bitmap[idx / 8] |= 1 << (idx % 8);
    }
    bitmap
}

fn encode_row_values(
    table: &BinlogTable,
    row: &[Value],
    out: &mut Vec<u8>,
) -> Result<(), QueryError> {
    // Enforce the same row shape the caller declared in table.columns —
    // a short row silently null-pads value bytes but does not extend the
    // null bitmap, which desynchronises the two streams and lets the
    // replica interpret adjacent columns as random bytes.
    if row.len() != table.columns.len() {
        return Err(error(
            crate::error::catalog::ER_REPLICA_CORRUPT_EVENT,
            format!(
                "binlog row-value width mismatch: table '{}' declares {} columns, row has {}",
                table.name,
                table.columns.len(),
                row.len()
            ),
        ));
    }
    let mut null_bitmap = vec![0u8; (table.columns.len() + 7) / 8];
    for (idx, value) in row.iter().enumerate() {
        if matches!(value, Value::Null) {
            null_bitmap[idx / 8] |= 1 << (idx % 8);
        }
    }
    out.extend_from_slice(&null_bitmap);
    for (idx, column) in table.columns.iter().enumerate() {
        let value = &row[idx];
        if matches!(value, Value::Null) {
            continue;
        }
        encode_value(column, value, out)?;
    }
    Ok(())
}

fn encode_value(column: &BinlogColumn, value: &Value, out: &mut Vec<u8>) -> Result<(), QueryError> {
    match column.column_type {
        MYSQL_TYPE_TINY => out.push(value_i64(value)? as i8 as u8),
        MYSQL_TYPE_SHORT => out.extend_from_slice(&(value_i64(value)? as i16).to_le_bytes()),
        MYSQL_TYPE_INT24 => {
            let v = value_i64(value)? as i32;
            let bytes = v.to_le_bytes();
            out.extend_from_slice(&bytes[..3]);
        }
        MYSQL_TYPE_LONG => {
            if column.unsigned {
                out.extend_from_slice(&(value_u64(value)? as u32).to_le_bytes());
            } else {
                out.extend_from_slice(&(value_i64(value)? as i32).to_le_bytes());
            }
        }
        MYSQL_TYPE_LONGLONG => {
            if column.unsigned {
                out.extend_from_slice(&value_u64(value)?.to_le_bytes());
            } else {
                out.extend_from_slice(&value_i64(value)?.to_le_bytes());
            }
        }
        MYSQL_TYPE_FLOAT => out.extend_from_slice(&(value_f64(value)? as f32).to_le_bytes()),
        MYSQL_TYPE_DOUBLE => out.extend_from_slice(&value_f64(value)?.to_le_bytes()),
        MYSQL_TYPE_YEAR => out.push(value_year(value)?),
        MYSQL_TYPE_DATE => encode_date_value(value, out)?,
        MYSQL_TYPE_TIME => encode_time_value(value, out)?,
        MYSQL_TYPE_DATETIME => encode_datetime_value(value, out)?,
        MYSQL_TYPE_TIMESTAMP => {
            let micros = match value {
                Value::Timestamp(v) | Value::DateTime(v) => *v,
                Value::Date(v) => date_days_to_midnight_micros(*v).unwrap_or(0),
                _ => value_i64(value)?,
            };
            let secs = i32::try_from(micros.div_euclid(1_000_000)).map_err(|_| {
                error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "TIMESTAMP is outside the representable MySQL binlog range",
                )
            })?;
            out.extend_from_slice(&secs.to_le_bytes());
        }
        MYSQL_TYPE_TIME2 => encode_time2_value(column, value, out)?,
        MYSQL_TYPE_DATETIME2 => encode_datetime2_value(column, value, out)?,
        MYSQL_TYPE_TIMESTAMP2 => encode_timestamp2_value(column, value, out)?,
        MYSQL_TYPE_NEWDECIMAL => encode_decimal_value(column, value, out)?,
        MYSQL_TYPE_BIT => encode_bit_value(column, value, out)?,
        MYSQL_TYPE_ENUM => {
            // Storage keeps ENUM values as `Value::String(<member name>)`
            // (constraints coerces the input to a matched member). The row
            // event, however, must carry the 1-based ordinal. Fall back to
            // numeric parsing only when the row was already numeric (0 = ''
            // sentinel, or an integer literal that skipped the string path).
            let v = enum_value_to_ordinal(column, value)?;
            if column.metadata.get(1).copied().unwrap_or(1) == 1 {
                out.push(v.min(u8::MAX as u64) as u8);
            } else {
                out.extend_from_slice(&(v.min(u16::MAX as u64) as u16).to_le_bytes());
            }
        }
        MYSQL_TYPE_SET => {
            // Same story: stored as `"a,b"` CSV; the row event carries the
            // little-endian bitmask over `metadata[1]` bytes.
            let bytes = column.metadata.get(1).copied().unwrap_or(1) as usize;
            let mut v = set_value_to_bitmask(column, value)?;
            for _ in 0..bytes {
                out.push((v & 0xff) as u8);
                v >>= 8;
            }
        }
        MYSQL_TYPE_JSON => {
            let owned;
            let json = match value {
                Value::Json(bytes) => bytes.as_slice(),
                other => {
                    owned =
                        crate::engine::json::parse_text(&other.to_display_string(), 1, "binlog")?;
                    owned.as_slice()
                }
            };
            let bytes = crate::engine::json::mysql_binary::encode(json)?;
            let width = column.metadata.first().copied().unwrap_or(4).clamp(1, 4) as usize;
            if bytes.len() >> (width * 8) != 0 {
                return Err(error(
                    crate::error::catalog::ER_DATA_TOO_LONG,
                    format!(
                        "JSON value too large for binlog column '{}' (len={}, prefix width={})",
                        column.name,
                        bytes.len(),
                        width
                    ),
                ));
            }
            out.extend_from_slice(&(bytes.len() as u64).to_le_bytes()[..width]);
            out.extend_from_slice(&bytes);
        }
        MYSQL_TYPE_BLOB | MYSQL_TYPE_GEOMETRY => {
            let bytes = binlog_string_bytes(column, value)?;
            // Length prefix width comes from the column's TABLE_MAP metadata
            // (`vec![1|2|3|4]` for TINY/normal/MEDIUM/LONG BLOB or TEXT); the
            // replica reads exactly `metadata[0]` bytes. Emitting a fixed
            // 4-byte prefix for TINYTEXT/TEXT/MEDIUMTEXT desynchronises the
            // row and the replica reports ER_SLAVE_CORRUPT_EVENT (1610).
            let width = column.metadata.first().copied().unwrap_or(4).clamp(1, 4) as usize;
            let envelope_len = if column.vector_dim.is_some() {
                crate::engine::vector::codec::VECTOR_BINLOG_MAGIC.len() + bytes.len()
            } else {
                bytes.len()
            };
            if envelope_len >> (width * 8) != 0 {
                return Err(error(
                    crate::error::catalog::ER_DATA_TOO_LONG,
                    format!(
                        "value too large for binlog column '{}' (len={}, prefix width={})",
                        column.name, envelope_len, width
                    ),
                ));
            }
            let len_le = (envelope_len as u64).to_le_bytes();
            out.extend_from_slice(&len_le[..width]);
            if column.vector_dim.is_some() {
                out.extend_from_slice(crate::engine::vector::codec::VECTOR_BINLOG_MAGIC);
            }
            out.extend_from_slice(&bytes);
        }
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_STRING => {
            let bytes = binlog_string_bytes(column, value)?;
            // Decode the declared `max_bytes`. For VARCHAR the metadata is
            // a little-endian u16; for STRING (CHAR) the wire packs the
            // 10-bit max into `[meta0 = 0xfe XOR ((max & 0x300) >> 4), meta1
            // = max & 0xff]`, so undo the XOR to recover the high 2 bits.
            let meta0 = column.metadata.first().copied().unwrap_or(0);
            let meta1 = column.metadata.get(1).copied().unwrap_or(0);
            let max_len = if column.column_type == MYSQL_TYPE_STRING {
                let hi = ((meta0 ^ MYSQL_TYPE_STRING) as u16 & 0x30) << 4;
                hi | (meta1 as u16)
            } else {
                u16::from_le_bytes([meta0, meta1])
            };
            if bytes.len() > usize::from(max_len) {
                return Err(error(
                    crate::error::catalog::ER_DATA_TOO_LONG,
                    format!(
                        "value too large for binlog column '{}' (len={}, maximum={max_len})",
                        column.name,
                        bytes.len()
                    ),
                ));
            }
            // The row-image length-prefix width follows the declared
            // max byte length: 1 byte when < 256, otherwise 2 bytes —
            // exactly how a replica decodes it.
            if max_len < 256 {
                out.push(bytes.len() as u8);
            } else {
                out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            }
            out.extend_from_slice(&bytes);
        }
        other => {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!(
                    "cannot encode binlog column '{}' with MySQL type 0x{other:02x}",
                    column.name
                ),
            ))
        }
    }
    Ok(())
}

fn binlog_string_bytes<'a>(
    column: &BinlogColumn,
    value: &'a Value,
) -> Result<Cow<'a, [u8]>, QueryError> {
    let bytes = value_bytes(value);
    let Some(charset) = column.charset else {
        return Ok(bytes);
    };
    // Validated UTF-8/ASCII storage lanes can be borrowed directly.
    if matches!(charset, "utf8mb4" | "utf8mb3" | "ascii" | "binary") {
        return Ok(bytes);
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| {
        error(
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
            format!(
                "invalid UTF-8 storage text for binlog column '{}'",
                column.name
            ),
        )
    })?;
    crate::charset::encode_string(charset, text).map(Cow::Owned)
}

fn value_i64(value: &Value) -> Result<i64, QueryError> {
    match value {
        Value::Int(v) => Ok(*v),
        Value::UInt(v) => i64::try_from(*v).map_err(|_| {
            error(
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                "integer out of range",
            )
        }),
        Value::Float(v) => Ok(*v as i64),
        Value::Double(v) => Ok(*v as i64),
        Value::DecimalCompact(v, scale) => Ok(if *scale == 0 {
            *v
        } else {
            *v / 10_i64.saturating_pow(*scale as u32)
        }),
        Value::Decimal128(v, scale) => crate::util::decimal::rescale_trunc_i128(*v, *scale, 0)
            .and_then(|v| i64::try_from(v).ok())
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "integer out of range",
                )
            }),
        Value::DecimalWide(v) => v
            .magnitude
            .to_string()
            .parse::<i64>()
            .map(|m| {
                if v.scale == 0 {
                    m
                } else {
                    m / 10_i64.saturating_pow(v.scale as u32)
                }
            })
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "integer out of range",
                )
            }),
        Value::Year(y) => Ok(if *y == 0 { 0 } else { 1900 + *y as i64 }),
        Value::Date(v) => Ok(*v as i64),
        Value::Time(v) | Value::DateTime(v) | Value::Timestamp(v) => Ok(*v),
        Value::String(s) | Value::Decimal(s) => s.trim().parse().map_err(|_| {
            error(
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                "incorrect integer value for binlog encoding",
            )
        }),
        Value::SmallString(b) => unsafe { std::str::from_utf8_unchecked(b) }
            .trim()
            .parse()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                    "incorrect integer value for binlog encoding",
                )
            }),
        Value::Bytes(bytes) => String::from_utf8_lossy(bytes).trim().parse().map_err(|_| {
            error(
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                "incorrect integer value for binlog encoding",
            )
        }),
        Value::Json(b) => Ok(crate::engine::json::coerce_i64(b)),
        Value::Geometry(_) => Ok(0),
        Value::Vector(_) => Ok(0),
        Value::Null => Ok(0),
    }
}

fn enum_value_to_ordinal(column: &BinlogColumn, value: &Value) -> Result<u64, QueryError> {
    // Numeric inputs are the raw ordinal (0 = '' sentinel).
    match value {
        Value::UInt(v) => return Ok(*v),
        Value::Int(v) if *v >= 0 => return Ok(*v as u64),
        _ => {}
    }
    let s = match value {
        Value::String(s) | Value::Decimal(s) => s.as_str(),
        Value::SmallString(b) => unsafe { std::str::from_utf8_unchecked(b) },
        Value::Bytes(b) => std::str::from_utf8(b).unwrap_or(""),
        _ => return value_u64(value),
    };
    if s.is_empty() {
        return Ok(0);
    }
    if let Some(values) = column.enum_set_values.as_ref() {
        if let Some(idx) = values.iter().position(|v| v == s) {
            return Ok(idx as u64 + 1);
        }
        // Numeric-in-string fallback ("2" → 2) — preserves legacy behaviour.
        if let Ok(parsed) = s.trim().parse::<u64>() {
            return Ok(parsed);
        }
        return Err(error(
            crate::error::catalog::WARN_DATA_TRUNCATED,
            format!(
                "cannot map ENUM value {:?} for binlog column '{}'",
                s, column.name
            ),
        ));
    }
    // No member list attached (should not happen for schema-derived columns);
    // fall back to the pre-fix numeric parse so tests without full schemas
    // still work.
    value_u64(value)
}

fn set_value_to_bitmask(column: &BinlogColumn, value: &Value) -> Result<u64, QueryError> {
    match value {
        Value::UInt(v) => return Ok(*v),
        Value::Int(v) if *v >= 0 => return Ok(*v as u64),
        _ => {}
    }
    let s = match value {
        Value::String(s) | Value::Decimal(s) => s.as_str(),
        Value::SmallString(b) => unsafe { std::str::from_utf8_unchecked(b) },
        Value::Bytes(b) => std::str::from_utf8(b).unwrap_or(""),
        _ => return value_u64(value),
    };
    if s.is_empty() {
        return Ok(0);
    }
    if let Some(values) = column.enum_set_values.as_ref() {
        let mut mask: u64 = 0;
        for token in s.split(',') {
            let trimmed = token.trim_end_matches(' ');
            if trimmed.is_empty() {
                continue;
            }
            if let Some(idx) = values.iter().position(|v| v == trimmed) {
                if idx < 64 {
                    mask |= 1u64 << idx;
                }
                continue;
            }
            return Err(error(
                crate::error::catalog::WARN_DATA_TRUNCATED,
                format!(
                    "cannot map SET token {:?} for binlog column '{}'",
                    trimmed, column.name
                ),
            ));
        }
        return Ok(mask);
    }
    value_u64(value)
}

fn value_u64(value: &Value) -> Result<u64, QueryError> {
    match value {
        Value::UInt(v) => Ok(*v),
        Value::Int(v) if *v >= 0 => Ok(*v as u64),
        Value::DecimalCompact(v, scale) if *v >= 0 => Ok(if *scale == 0 {
            *v as u64
        } else {
            (*v / 10_i64.saturating_pow(*scale as u32)) as u64
        }),
        Value::String(s) | Value::Decimal(s) => s.trim().parse().map_err(|_| {
            error(
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                "incorrect unsigned integer value for binlog encoding",
            )
        }),
        Value::SmallString(b) => unsafe { std::str::from_utf8_unchecked(b) }
            .trim()
            .parse()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                    "incorrect unsigned integer value for binlog encoding",
                )
            }),
        Value::Bytes(bytes) => String::from_utf8_lossy(bytes).trim().parse().map_err(|_| {
            error(
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                "incorrect unsigned integer value for binlog encoding",
            )
        }),
        _ => value_i64(value).and_then(|v| {
            if v < 0 {
                Err(error(
                    crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                    "unsigned integer out of range",
                ))
            } else {
                Ok(v as u64)
            }
        }),
    }
}

fn value_f64(value: &Value) -> Result<f64, QueryError> {
    match value {
        Value::Float(v) => Ok(*v as f64),
        Value::Double(v) => Ok(*v),
        Value::Int(v) => Ok(*v as f64),
        Value::UInt(v) => Ok(*v as f64),
        Value::DecimalCompact(v, scale) => Ok(*v as f64 / 10f64.powi(*scale as i32)),
        Value::Decimal128(v, scale) => Ok(*v as f64 / 10f64.powi(*scale as i32)),
        Value::DecimalWide(v) => {
            crate::util::decimal::format_big_decimal_parts(&v.magnitude, v.scale)
                .parse()
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                        "incorrect floating point value for binlog encoding",
                    )
                })
        }
        Value::String(s) | Value::Decimal(s) => s.trim().parse().map_err(|_| {
            error(
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                "incorrect floating point value for binlog encoding",
            )
        }),
        Value::SmallString(b) => unsafe { std::str::from_utf8_unchecked(b) }
            .trim()
            .parse()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                    "incorrect floating point value for binlog encoding",
                )
            }),
        Value::Bytes(bytes) => String::from_utf8_lossy(bytes).trim().parse().map_err(|_| {
            error(
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                "incorrect floating point value for binlog encoding",
            )
        }),
        Value::Date(v) => Ok(*v as f64),
        Value::Time(v) | Value::DateTime(v) | Value::Timestamp(v) => Ok(*v as f64),
        Value::Year(v) => Ok(if *v == 0 { 0.0 } else { 1900.0 + *v as f64 }),
        Value::Json(b) => Ok(crate::engine::json::coerce_f64(b)),
        Value::Geometry(_) => Ok(0.0),
        Value::Vector(_) => Ok(0.0),
        Value::Null => Ok(0.0),
    }
}

fn value_year(value: &Value) -> Result<u8, QueryError> {
    Ok(match value {
        Value::Year(v) => *v,
        Value::Int(0) | Value::UInt(0) => 0,
        Value::Int(v) => (*v - 1900).clamp(0, u8::MAX as i64) as u8,
        Value::UInt(v) => v.saturating_sub(1900).min(u8::MAX as u64) as u8,
        Value::String(s) | Value::Decimal(s) => s
            .trim()
            .parse::<i64>()
            .map(|v| (v - 1900).clamp(0, u8::MAX as i64) as u8)
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                    "incorrect YEAR value for binlog encoding",
                )
            })?,
        _ => value_i64(value).map(|v| (v - 1900).clamp(0, u8::MAX as i64) as u8)?,
    })
}

fn encode_date_value(value: &Value, out: &mut Vec<u8>) -> Result<(), QueryError> {
    let (year, month, day) = match value {
        Value::Date(days) => crate::types::date_components(*days),
        Value::DateTime(micros) => {
            let (year, month, day, ..) = crate::types::datetime_components(*micros);
            (year, month, day)
        }
        _ => {
            let raw = value_i64(value)?;
            let year = (raw / 10000).max(0) as u32;
            let month = ((raw / 100) % 100).max(0) as u32;
            let day = (raw % 100).max(0) as u32;
            (year, month, day)
        }
    };
    let packed = (year << 9) | (month << 5) | day;
    let bytes = packed.to_le_bytes();
    out.extend_from_slice(&bytes[..3]);
    Ok(())
}

fn encode_time_value(value: &Value, out: &mut Vec<u8>) -> Result<(), QueryError> {
    let micros = match value {
        Value::Time(v) => *v,
        _ => value_i64(value)?.saturating_mul(1_000_000),
    };
    let total = micros.unsigned_abs() / 1_000_000;
    let hours = (total / 3600).min(838);
    let minutes = (total / 60) % 60;
    let seconds = total % 60;
    let packed = (hours * 10000 + minutes * 100 + seconds) as u32;
    let bytes = packed.to_le_bytes();
    out.extend_from_slice(&bytes[..3]);
    Ok(())
}

fn encode_datetime_value(value: &Value, out: &mut Vec<u8>) -> Result<(), QueryError> {
    let raw = match value {
        Value::DateTime(v) => {
            let (year, month, day, hour, minute, second, _) = crate::types::datetime_components(*v);
            (year as u64 * 10000 + month as u64 * 100 + day as u64) * 1_000_000
                + (hour as u64 * 10000 + minute as u64 * 100 + second as u64)
        }
        Value::Date(days) => {
            let (year, month, day) = crate::types::date_components(*days);
            (year as u64 * 10000 + month as u64 * 100 + day as u64) * 1_000_000
        }
        _ => value_u64(value)?,
    };
    out.extend_from_slice(&raw.to_le_bytes());
    Ok(())
}

/// Number of fractional-second bytes for a given fsp, per MySQL 5.6+ binlog format.
/// fsp 0 → 0, fsp 1-2 → 1, fsp 3-4 → 2, fsp 5-6 → 3.
fn temporal_frac_len(fsp: u8) -> usize {
    ((fsp.min(6) as usize) + 1) / 2
}

/// Encode microsecond fraction in MySQL big-endian fractional layout.
/// fsp 1-2 stores hundredths (0..99) in 1 byte;
/// fsp 3-4 stores ten-thousandths (0..9999) in 2 bytes BE;
/// fsp 5-6 stores micros (0..999999) in 3 bytes BE.
fn encode_temporal_fraction(micros_abs: u32, fsp: u8, out: &mut Vec<u8>) {
    let fsp = fsp.min(6);
    match temporal_frac_len(fsp) {
        0 => {}
        1 => {
            let scale = 10_000;
            let v = (micros_abs / scale) as u8;
            out.push(v);
        }
        2 => {
            let scale = 100;
            let v = (micros_abs / scale) as u16;
            out.extend_from_slice(&v.to_be_bytes());
        }
        3 => {
            let bytes = micros_abs.to_be_bytes();
            out.extend_from_slice(&bytes[1..]);
        }
        _ => {}
    }
}

/// Encode TIME2 (MySQL 5.6+): 3-byte big-endian signed int with bias 0x800000,
/// followed by `temporal_frac_len(fsp)` fraction bytes.
/// Bit layout of int part (24 bits): sign(1) | unused(1) | hour(10) | min(6) | sec(6).
fn encode_time2_value(
    column: &BinlogColumn,
    value: &Value,
    out: &mut Vec<u8>,
) -> Result<(), QueryError> {
    let fsp = column.metadata.first().copied().unwrap_or(0).min(6);
    let micros_signed = match value {
        Value::Time(v) => *v,
        Value::DateTime(v) => {
            // DATETIME→TIME: extract time component (per MySQL semantics)
            let secs_of_day = v.rem_euclid(86_400_000_000) as i64;
            secs_of_day
        }
        _ => value_i64(value)?.saturating_mul(1_000_000),
    };
    // Port of MySQL's `my_time_packed_to_binary`. A negative TIME is stored as
    // the two's-complement of the whole packed longlong, NOT as a sign-flipped
    // integer part with a positive fraction — so the fraction bytes for a
    // negative value are themselves signed/complemented. The previous code
    // wrote the integer part with the right bias but a *positive* fraction
    // magnitude, which a real MySQL replica decoded back with the wrong sign
    // (or lost the sign entirely when the integer part was zero, e.g.
    // `-00:00:00.5`). Round-trips against `mysql_common::my_time_packed_from_binary`.
    const TIMEF_INT_OFS: i64 = 0x800000;
    const TIMEF_OFS: i64 = 0x800000000000;
    let neg = micros_signed < 0;
    let mag = micros_signed.unsigned_abs();
    let total_secs = mag / 1_000_000;
    let micros_frac = (mag % 1_000_000) as i64;
    let hours = (total_secs / 3600).min(838) as i64;
    let minutes = ((total_secs / 60) % 60) as i64;
    let seconds = (total_secs % 60) as i64;
    let hms = (hours << 12) | (minutes << 6) | seconds; // my_packed_time int part
    let tmp = (hms << 24) + micros_frac; // my_packed_time_make(hms, micros)
    let packed: i64 = if neg { -tmp } else { tmp };
    // my_packed_time_get_int_part / _frac_part (arithmetic shift / signed rem).
    let int_part = packed >> 24;
    let frac_part = packed % (1 << 24);
    let store_int3 = |out: &mut Vec<u8>| {
        let stored = (TIMEF_INT_OFS + int_part) as u32;
        out.extend_from_slice(&stored.to_be_bytes()[1..4]);
    };
    match fsp {
        1 | 2 => {
            store_int3(out);
            out.push(((frac_part / 10_000) as i8) as u8);
        }
        3 | 4 => {
            store_int3(out);
            let f = (frac_part / 100) as i16;
            out.extend_from_slice(&f.to_be_bytes());
        }
        5 | 6 => {
            // Whole packed value with the 6-byte offset, big-endian.
            let stored = (TIMEF_OFS + packed) as u64;
            out.extend_from_slice(&stored.to_be_bytes()[2..8]);
        }
        _ => store_int3(out),
    }
    Ok(())
}

/// Encode DATETIME2 (MySQL 5.6+): 5-byte big-endian bit-packed payload
/// (sign(1) | year*13+month(17) | day(5) | hour(5) | min(6) | sec(6)) followed
/// by fractional bytes. The sign bit is always 1 for valid DATETIME.
fn encode_datetime2_value(
    column: &BinlogColumn,
    value: &Value,
    out: &mut Vec<u8>,
) -> Result<(), QueryError> {
    let fsp = column.metadata.first().copied().unwrap_or(0).min(6);
    let (year, month, day, hour, min, sec, micros_frac) = match value {
        Value::DateTime(v) => crate::types::datetime_components(*v),
        Value::Date(days) => {
            let (y, m, d) = crate::types::date_components(*days);
            (y, m, d, 0, 0, 0, 0)
        }
        _ => {
            // Fall back to integer parse (yyyymmddhhmmss form).
            let raw = value_u64(value)?;
            let date_part = raw / 1_000_000;
            let time_part = raw % 1_000_000;
            let year = (date_part / 10_000) as u32;
            let month = ((date_part / 100) % 100) as u32;
            let day = (date_part % 100) as u32;
            let hour = (time_part / 10_000) as u32;
            let min = ((time_part / 100) % 100) as u32;
            let sec = (time_part % 100) as u32;
            (year, month, day, hour, min, sec, 0)
        }
    };
    let ym = (year as u64) * 13 + (month as u64);
    let intpart: u64 = (1u64 << 39)
        | ((ym & 0x1_FFFF) << 22)
        | ((day as u64 & 0x1F) << 17)
        | ((hour as u64 & 0x1F) << 12)
        | ((min as u64 & 0x3F) << 6)
        | (sec as u64 & 0x3F);
    let bytes = intpart.to_be_bytes();
    out.extend_from_slice(&bytes[3..8]);
    encode_temporal_fraction(micros_frac, fsp, out);
    Ok(())
}

/// Encode TIMESTAMP2 (MySQL 5.6+): 4-byte big-endian unsigned Unix seconds
/// followed by fractional bytes.
fn encode_timestamp2_value(
    column: &BinlogColumn,
    value: &Value,
    out: &mut Vec<u8>,
) -> Result<(), QueryError> {
    let fsp = column.metadata.first().copied().unwrap_or(0).min(6);
    let micros = match value {
        Value::Timestamp(v) | Value::DateTime(v) => *v,
        Value::Date(v) => date_days_to_midnight_micros(*v).unwrap_or(0),
        _ => value_i64(value)?,
    };
    // TIMESTAMP2 reserves an all-zero payload for the SQL zero timestamp.
    let micros = if micros == crate::types::ZERO_DATETIME {
        0
    } else {
        micros
    };
    let secs_i64 = micros.div_euclid(1_000_000);
    // MySQL TIMESTAMP2 wire format is *unsigned* big-endian u32 seconds
    // since epoch (see `my_timestamp_binary_from_binary` in MySQL source).
    // Storing a negative value as i32 with `to_be_bytes` writes `0xFF..`
    // which the replica reads back as ~4.29 billion → year 2106. Reject
    // pre-1970 explicitly rather than silently corrupting.
    if secs_i64 < 0 {
        return Err(error(
            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            format!(
                "TIMESTAMP value {} predates 1970-01-01 UTC — MySQL binlog TIMESTAMP2 is unsigned",
                value.to_display_string()
            ),
        ));
    }
    let secs = u32::try_from(secs_i64).map_err(|_| {
        error(
            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            "TIMESTAMP exceeds the MySQL TIMESTAMP2 binlog limit (2106-02-07 06:28:15 UTC); use mydb native WAL replication or disable sql_log_bin for this write",
        )
    })?;
    let frac = micros.rem_euclid(1_000_000) as u32;
    out.extend_from_slice(&secs.to_be_bytes());
    encode_temporal_fraction(frac, fsp, out);
    Ok(())
}

fn encode_decimal_value(
    column: &BinlogColumn,
    value: &Value,
    out: &mut Vec<u8>,
) -> Result<(), QueryError> {
    let precision = column.metadata.first().copied().unwrap_or(18);
    let scale = column.metadata.get(1).copied().unwrap_or(0);
    let text = match value {
        Value::Decimal(text) | Value::String(text) => Cow::Borrowed(text.as_str()),
        Value::SmallString(_) => Cow::Borrowed(crate::types::value_str(value).unwrap()),
        other => Cow::Owned(other.to_display_string()),
    };
    encode_mysql_decimal_binary(&text, precision as usize, scale as usize, out)
}

fn encode_bit_value(
    column: &BinlogColumn,
    value: &Value,
    out: &mut Vec<u8>,
) -> Result<(), QueryError> {
    let nbits = column.metadata.get(1).copied().unwrap_or(1) as usize * 8
        + column.metadata.first().copied().unwrap_or(0) as usize;
    let nbytes = (nbits.max(1) + 7) / 8;
    match value {
        Value::Bytes(bytes) => {
            if bytes.len() >= nbytes {
                out.extend_from_slice(&bytes[bytes.len() - nbytes..]);
            } else {
                out.extend(std::iter::repeat(0).take(nbytes - bytes.len()));
                out.extend_from_slice(bytes);
            }
        }
        _ => {
            let v = value_u64(value)?;
            let be = v.to_be_bytes();
            out.extend_from_slice(&be[be.len() - nbytes..]);
        }
    }
    Ok(())
}

fn value_bytes(value: &Value) -> Cow<'_, [u8]> {
    match value {
        Value::Bytes(bytes) | Value::Geometry(bytes) => Cow::Borrowed(bytes),
        Value::Vector(bytes) => Cow::Owned(bytes.encode()),
        Value::String(text) | Value::Decimal(text) => Cow::Borrowed(text.as_bytes()),
        Value::SmallString(bytes) => Cow::Borrowed(bytes.as_slice()),
        other => Cow::Owned(other.to_display_string().into_bytes()),
    }
}

fn date_days_to_midnight_micros(days: i32) -> Option<i64> {
    if days == crate::types::ZERO_DATE {
        return Some(crate::types::ZERO_DATETIME);
    }
    if let Some((year, month, day)) = crate::types::decode_tagged_date(days) {
        return crate::types::encode_tagged_datetime(year, month, day, 0, 0, 0, 0);
    }
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    let date = epoch.checked_add_signed(chrono::Duration::days(days as i64))?;
    Some(date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros())
}

fn mysql_decimal_bin_size(precision: usize, scale: usize) -> usize {
    const DIG_PER_DEC: usize = 9;
    const DIG_TO_BYTES: [usize; DIG_PER_DEC + 1] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
    let intg = precision.saturating_sub(scale);
    let intg0 = intg / DIG_PER_DEC;
    let frac0 = scale / DIG_PER_DEC;
    let intg0x = intg - intg0 * DIG_PER_DEC;
    let frac0x = scale - frac0 * DIG_PER_DEC;
    intg0 * 4 + DIG_TO_BYTES[intg0x] + frac0 * 4 + DIG_TO_BYTES[frac0x]
}

fn encode_mysql_decimal_binary(
    text: &str,
    precision: usize,
    scale: usize,
    out: &mut Vec<u8>,
) -> Result<(), QueryError> {
    const DIG_PER_DEC: usize = 9;
    const DIG_TO_BYTES: [usize; DIG_PER_DEC + 1] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
    let mut text = text.trim();
    let negative = text.strip_prefix('-').is_some();
    if negative || text.starts_with('+') {
        text = &text[1..];
    }
    let (int_raw, frac_raw) = text.split_once('.').unwrap_or((text, ""));
    if !int_raw.chars().all(|ch| ch.is_ascii_digit())
        || !frac_raw.chars().all(|ch| ch.is_ascii_digit())
    {
        return Err(error(
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
            "incorrect DECIMAL value for binlog encoding",
        ));
    }
    let intg = precision.saturating_sub(scale);
    let int_trimmed = int_raw.trim_start_matches('0');
    if int_trimmed.len() > intg {
        return Err(error(
            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            format!("DECIMAL value '{text}' does not fit DECIMAL({precision},{scale})"),
        ));
    }
    let mut int_digits = String::with_capacity(intg);
    int_digits.extend(std::iter::repeat('0').take(intg.saturating_sub(int_trimmed.len())));
    int_digits.push_str(if int_trimmed.is_empty() {
        ""
    } else {
        int_trimmed
    });

    let mut frac_digits = frac_raw.chars().take(scale).collect::<String>();
    frac_digits.extend(std::iter::repeat('0').take(scale.saturating_sub(frac_digits.len())));

    let start = out.len();
    out.reserve(mysql_decimal_bin_size(precision, scale));
    // Preserve append atomicity if group encoding ever rejects an input.
    let encoded = (|| -> Result<(), QueryError> {
        let mask = if negative { -1_i32 } else { 0_i32 };
        let int_prefix = intg % DIG_PER_DEC;
        let mut offset = 0usize;
        if int_prefix > 0 {
            write_decimal_group(
                &int_digits[offset..offset + int_prefix],
                DIG_TO_BYTES[int_prefix],
                mask,
                out,
            )?;
            offset += int_prefix;
        }
        while offset < int_digits.len() {
            write_decimal_group(&int_digits[offset..offset + DIG_PER_DEC], 4, mask, out)?;
            offset += DIG_PER_DEC;
        }
        let mut frac_offset = 0usize;
        while frac_offset + DIG_PER_DEC <= frac_digits.len() {
            write_decimal_group(
                &frac_digits[frac_offset..frac_offset + DIG_PER_DEC],
                4,
                mask,
                out,
            )?;
            frac_offset += DIG_PER_DEC;
        }
        let suffix = frac_digits.len().saturating_sub(frac_offset);
        if suffix > 0 {
            write_decimal_group(&frac_digits[frac_offset..], DIG_TO_BYTES[suffix], mask, out)?;
        }
        if let Some(first) = out.get_mut(start) {
            *first ^= 0x80;
        }
        Ok(())
    })();
    if encoded.is_err() {
        out.truncate(start);
    }
    encoded
}

fn write_decimal_group(
    digits: &str,
    bytes: usize,
    mask: i32,
    out: &mut Vec<u8>,
) -> Result<(), QueryError> {
    if bytes == 0 {
        return Ok(());
    }
    let value = digits.parse::<i32>().map_err(|_| {
        error(
            crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
            "incorrect DECIMAL value for binlog encoding",
        )
    })? ^ mask;
    let be = value.to_be_bytes();
    out.extend_from_slice(&be[be.len() - bytes..]);
    Ok(())
}

fn write_u48_le(out: &mut Vec<u8>, value: u64) {
    let bytes = value.to_le_bytes();
    out.extend_from_slice(&bytes[..6]);
}

fn read_reset_marker(path: &Path) -> Result<Option<u64>, QueryError> {
    match fs::read(path) {
        Ok(bytes) => {
            let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "invalid binlog reset marker",
                )
            })?;
            Ok(Some(u64::from_le_bytes(bytes)))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(io_error(err)),
    }
}

fn write_reset_marker(dir: &Path, name: &str, cutoff: u64) -> Result<(), QueryError> {
    let path = dir.join(name);
    crate::repl::durable::atomic_write_sync(
        &path,
        &path.with_extension("tmp"),
        &cutoff.to_le_bytes(),
    )
    .map_err(io_error)?;
    crate::platform::fs::sync_path(dir).map_err(io_error)
}

fn finish_interrupted_reset(dir: &Path, config: &BinlogConfig) -> Result<(), QueryError> {
    let Some(cutoff) = read_reset_marker(&dir.join("reset-pending"))? else {
        return Ok(());
    };
    // Only remove this binlog's numbered files and metadata, never database WAL.
    let prefix = format!("{}.", config.base_name);
    for entry in fs::read_dir(dir).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == "manifest.json"
            || name == format!("{}.index", config.base_name)
            || name.strip_prefix(&prefix).is_some_and(|suffix| {
                !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit())
            })
        {
            fs::remove_file(entry.path()).map_err(io_error)?;
        }
    }
    write_reset_marker(dir, "reset-cutoff", cutoff)?;
    fs::remove_file(dir.join("reset-pending")).map_err(io_error)?;
    crate::platform::fs::sync_path(dir).map_err(io_error)
}

fn committed_binlog_refs_from_wal(
    data_dir: &Path,
) -> Result<Vec<crate::wal::WalBinlogRef>, QueryError> {
    let scan = crate::wal::LogManager::scan(data_dir)?;
    let cutoff = read_reset_marker(&data_dir.join("binlog/reset-cutoff"))?.unwrap_or(0);
    Ok(scan
        .records
        .into_iter()
        .filter(|record| record.lsn > cutoff)
        .filter_map(|record| match record.payload {
            crate::wal::WalPayload::CommitTxn {
                binlog: Some(reference),
                ..
            } => Some(reference),
            _ => None,
        })
        .collect())
}

fn reconcile_binlog_tail(
    dir: &Path,
    active_file: &str,
    manifest_active_pos: Option<u64>,
    durable_refs: &[crate::wal::WalBinlogRef],
    files: &mut [BinaryLogInfo],
    events: &mut Vec<BinlogEventChunk>,
) -> Result<u64, QueryError> {
    let Some(active_idx) = files.iter().position(|info| info.name == active_file) else {
        return Ok(4);
    };
    let active_path = dir.join(active_file);
    let current_len = fs::metadata(&active_path).map_err(io_error)?.len();
    let mut durable_pos = manifest_active_pos.unwrap_or(current_len).max(4);
    for reference in durable_refs {
        let Some(info) = files.iter().find(|info| info.name == reference.file) else {
            if files
                .first()
                .is_some_and(|first| !binlog_pos_ge((&reference.file, 0), (&first.name, 0)))
            {
                continue; // PURGE published its index before unlinking these files.
            }
            return Err(error(
                crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
                format!(
                    "WAL references missing binary log file '{}' for committed transaction",
                    reference.file
                ),
            ));
        };
        if info.size < reference.end_pos {
            return Err(error(
                crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
                format!(
                    "binary log '{}' is shorter than WAL committed position {}",
                    reference.file, reference.end_pos
                ),
            ));
        }
        // WAL may advance beyond the manifest after a crash. Validate those
        // newly recovered bytes before publishing them; trusted historical
        // prefixes are checked when a reader first accesses them.
        let start = if reference.file == active_file {
            reference.start_pos.max(manifest_active_pos.unwrap_or(4))
        } else {
            reference.start_pos
        };
        if start < reference.end_pos {
            disk::validate_range(&dir.join(&reference.file), start, reference.end_pos)?;
        }
        if reference.file == active_file {
            durable_pos = durable_pos.max(reference.end_pos);
        }
    }
    if durable_pos > current_len {
        return Err(error(
            crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
            format!(
                "binary log '{active_file}' is shorter than durable WAL position {durable_pos}"
            ),
        ));
    }
    if current_len > durable_pos {
        OpenOptions::new()
            .write(true)
            .open(&active_path)
            .and_then(|file| file.set_len(durable_pos))
            .map_err(io_error)?;
    }
    files[active_idx].size = durable_pos;
    events.retain(|event| event.file != active_file || event.end_pos <= durable_pos);
    Ok(durable_pos)
}

fn ensure_binlog_file(path: &Path) -> Result<(), QueryError> {
    if !path.exists() {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .map_err(io_error)?;
        file.write_all(BINLOG_MAGIC).map_err(io_error)?;
        file.flush().map_err(io_error)?;
    }
    Ok(())
}

fn bootstrap_file_events(
    config: &BinlogConfig,
    start_pos: u64,
    previous_gtids: &str,
) -> Vec<(u8, Vec<u8>)> {
    let fde = encode_format_description_event(config, start_pos);
    let prev_start = start_pos + fde.len() as u64;
    let prev = encode_event(
        config,
        PREVIOUS_GTIDS_EVENT,
        prev_start,
        &encode_previous_gtids_payload(previous_gtids),
        false,
    );
    vec![
        (FORMAT_DESCRIPTION_EVENT, fde),
        (PREVIOUS_GTIDS_EVENT, prev),
    ]
}

fn chunks_from_encoded_events(
    file_name: &str,
    start_pos: u64,
    events: Vec<(u8, Vec<u8>)>,
) -> Vec<BinlogEventChunk> {
    let mut pos = start_pos;
    let mut out = Vec::with_capacity(events.len());
    for (event_type, bytes) in events {
        let end_pos = pos + bytes.len() as u64;
        out.push(BinlogEventChunk {
            file: file_name.to_string(),
            start_pos: pos,
            end_pos,
            event_type,
            gtid: extract_gtid_from_event(&bytes),
            bytes: Arc::new(bytes),
        });
        pos = end_pos;
    }
    out
}

fn concat_event_bytes(events: &[(u8, Vec<u8>)]) -> Vec<u8> {
    let len = events.iter().map(|(_, bytes)| bytes.len()).sum();
    let mut out = Vec::with_capacity(len);
    for (_, bytes) in events {
        out.extend_from_slice(bytes);
    }
    out
}

fn append_bytes(path: &Path, bytes: &[u8]) -> Result<(), QueryError> {
    let mut file = OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(io_error)?;
    file.write_all(bytes).map_err(io_error)?;
    file.flush().map_err(io_error)?;
    crate::wal::sync_wal_file(&file)?;
    Ok(())
}

fn write_index_file(path: &Path, files: &[BinaryLogInfo]) -> Result<(), QueryError> {
    let mut out = String::new();
    for file in files {
        out.push_str(&file.name);
        out.push('\n');
    }
    crate::repl::durable::atomic_write_sync(path, &path.with_extension("index.tmp"), out.as_bytes())
        .map_err(io_error)
}

fn read_index_file(path: &Path) -> Result<Vec<String>, QueryError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text = fs::read_to_string(path).map_err(io_error)?;
    let names: Vec<String> = text.lines().map(str::to_string).collect();
    if (!text.is_empty() && !text.ends_with('\n'))
        || names.iter().any(|name| {
            name.contains('/')
                || name.contains('\\')
                || !name.rsplit_once('.').is_some_and(|(base, suffix)| {
                    !base.is_empty()
                        && suffix.len() >= 6
                        && suffix.bytes().all(|b| b.is_ascii_digit())
                })
        })
        || names
            .windows(2)
            .any(|pair| binlog_pos_ge((&pair[0], 0), (&pair[1], 0)))
    {
        return Err(error(
            crate::error::catalog::ER_SOURCE_FATAL_ERROR_READING_BINLOG,
            format!(
                "corrupt binlog index '{}': invalid, incomplete or unordered file names",
                path.display()
            ),
        ));
    }
    Ok(names)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BinlogManifest {
    version: u32,
    active_file: String,
    active_position: u64,
    #[serde(default)]
    gtid_executed: String,
    #[serde(default)]
    gtid_purged: String,
    #[serde(default)]
    next_gno: u64,
    #[serde(default)]
    sequence_number: u64,
    config: BinlogConfig,
}

fn read_manifest_file(path: &Path) -> Result<Option<BinlogManifest>, QueryError> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path).map_err(io_error)?;
    let manifest = serde_json::from_slice(&bytes).map_err(|err| {
        error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            format!(
                "failed to decode binlog manifest '{}': {err}",
                path.display()
            ),
        )
    })?;
    Ok(Some(manifest))
}

fn manifest_gtid_executed(manifest: &Option<BinlogManifest>) -> &str {
    manifest
        .as_ref()
        .map(|m| m.gtid_executed.as_str())
        .unwrap_or("")
}

fn write_manifest_file(
    path: &Path,
    config: &BinlogConfig,
    active_file: &str,
    active_pos: u64,
    gtid_executed: &str,
    gtid_purged: &str,
    next_gno: u64,
    sequence_number: u64,
) -> Result<(), QueryError> {
    #[derive(Serialize)]
    struct Manifest<'a> {
        version: u32,
        active_file: &'a str,
        active_position: u64,
        gtid_executed: &'a str,
        gtid_purged: &'a str,
        next_gno: u64,
        sequence_number: u64,
        config: &'a BinlogConfig,
    }
    let manifest = Manifest {
        version: 2,
        active_file,
        active_position: active_pos,
        gtid_executed,
        gtid_purged,
        next_gno,
        sequence_number,
        config,
    };
    let bytes = serde_json::to_vec_pretty(&manifest).map_err(|err| {
        error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            format!("failed to encode binlog manifest: {err}"),
        )
    })?;
    let tmp = path.with_extension("json.tmp");
    let result = crate::repl::durable::atomic_write_sync(path, &tmp, &bytes).map_err(io_error);
    if result.is_err() {
        let _ = fs::remove_file(tmp);
    }
    result
}

fn io_error(err: std::io::Error) -> QueryError {
    // ENOSPC surfaces as 3675 ER_DISK_FULL_NOWAIT so operators can wire
    // dedicated alerts; every other I/O failure keeps the historical
    // 1105 ER_UNKNOWN_ERROR that mydb's binlog writer has always used.
    crate::util::io_error::map_io_error_with_default(
        "binlog I/O",
        err,
        crate::error::catalog::ER_ERROR_ON_WRITE,
    )
}

fn read_u16_le(bytes: &[u8], pos: &mut usize) -> Result<u16, QueryError> {
    crate::util::bytes::read_word(bytes, pos)
        .map(u16::from_le_bytes)
        .ok_or_else(|| error(crate::error::catalog::ER_UNKNOWN_COM_ERROR, "short packet"))
}

fn read_u32_le(bytes: &[u8], pos: &mut usize) -> Result<u32, QueryError> {
    crate::util::bytes::read_word(bytes, pos)
        .map(u32::from_le_bytes)
        .ok_or_else(|| error(crate::error::catalog::ER_UNKNOWN_COM_ERROR, "short packet"))
}

fn read_u64_le(bytes: &[u8], pos: &mut usize) -> Result<u64, QueryError> {
    crate::util::bytes::read_word(bytes, pos)
        .map(u64::from_le_bytes)
        .ok_or_else(|| error(crate::error::catalog::ER_UNKNOWN_COM_ERROR, "short packet"))
}

fn parse_sid_block_to_gtid_set(bytes: &[u8]) -> Result<String, QueryError> {
    let mut pos = 0usize;
    if bytes.is_empty() {
        return Ok(String::new());
    }
    let sid_count = read_u64_le(bytes, &mut pos)? as usize;
    let mut map = BTreeMap::new();
    for _ in 0..sid_count {
        if bytes.len().saturating_sub(pos) < 16 {
            return Err(error(
                crate::error::catalog::ER_UNKNOWN_COM_ERROR,
                "bad COM_BINLOG_DUMP_GTID SID",
            ));
        }
        let uuid = format_uuid(&bytes[pos..pos + 16]);
        pos += 16;
        let interval_count = read_u64_le(bytes, &mut pos)? as usize;
        let intervals = map.entry(uuid).or_insert_with(Vec::new);
        for _ in 0..interval_count {
            let start = read_u64_le(bytes, &mut pos)?;
            let end_exclusive = read_u64_le(bytes, &mut pos)?;
            if start == 0 || end_exclusive <= start || end_exclusive > i64::MAX as u64 {
                return Err(error(
                    crate::error::catalog::ER_UNKNOWN_COM_ERROR,
                    "invalid COM_BINLOG_DUMP_GTID interval",
                ));
            }
            intervals.push((start, end_exclusive - 1));
        }
        merge_intervals(intervals);
    }
    if pos != bytes.len() {
        return Err(error(
            crate::error::catalog::ER_UNKNOWN_COM_ERROR,
            "COM_BINLOG_DUMP_GTID SID block has unexpected trailing bytes",
        ));
    }
    Ok(format_gtid_interval_map(&map))
}

fn extract_gtid_from_event(bytes: &[u8]) -> Option<String> {
    if bytes.get(4).copied()? != GTID_EVENT || bytes.len() < EVENT_HEADER_LEN as usize + 25 {
        return None;
    }
    let payload = &bytes[EVENT_HEADER_LEN as usize..];
    let uuid = format_uuid(payload.get(1..17)?);
    let gno = u64::from_le_bytes(payload.get(17..25)?.try_into().ok()?);
    Some(format!("{uuid}:{gno}"))
}

fn uuid_string_to_bytes(value: &str) -> Option<[u8; 16]> {
    crate::util::uuid::parse(value)
}

fn format_uuid(bytes: &[u8]) -> String {
    let mut padded = [0; 16];
    let len = bytes.len().min(padded.len());
    padded[..len].copy_from_slice(&bytes[..len]);
    crate::util::uuid::format(&padded)
}

pub fn gtid_subset(needed: &str, have: &str) -> bool {
    let have = parse_gtid_interval_map(have);
    parse_gtid_interval_map(needed)
        .into_iter()
        .all(|(uuid, intervals)| {
            let Some(have_intervals) = have.get(&uuid) else {
                return intervals.is_empty();
            };
            intervals.into_iter().all(|(lo, hi)| {
                have_intervals
                    .iter()
                    .any(|(have_lo, have_hi)| *have_lo <= lo && hi <= *have_hi)
            })
        })
}

fn next_gno_for_uuid(gtid_set: &str, uuid: &str) -> u64 {
    parse_gtid_interval_map(gtid_set)
        .get(&uuid.to_ascii_lowercase())
        .and_then(|intervals| intervals.iter().map(|(_, hi)| *hi).max())
        .unwrap_or(0)
        .saturating_add(1)
}

fn next_binlog_file_name(base_name: &str, active_file: &str) -> String {
    let suffix = active_file
        .rsplit_once('.')
        .and_then(|(_, suffix)| suffix.parse::<u32>().ok())
        .unwrap_or(1)
        .saturating_add(1);
    format!("{base_name}.{suffix:06}")
}

fn unquote_admin_string(sql: &str, prefix: &str) -> Result<String, QueryError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let value = trimmed.get(prefix.len()..).map(str::trim).ok_or_else(|| {
        error(
            crate::error::catalog::ER_PARSE_ERROR,
            "missing binary log file name",
        )
    })?;
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        let quote = bytes[0];
        if matches!(quote, b'\'' | b'"' | b'`') && bytes[value.len() - 1] == quote {
            return Ok(value[1..value.len() - 1].replace(
                std::str::from_utf8(&[quote, quote]).unwrap_or("''"),
                std::str::from_utf8(&[quote]).unwrap_or("'"),
            ));
        }
    }
    Ok(value.to_string())
}

fn current_unix_seconds() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(u32::MAX as u64) as u32
}

fn generate_server_uuid() -> String {
    if let Ok(raw) = crate::util::env::var("MYDB_SERVER_UUID") {
        if !raw.trim().is_empty() {
            return raw;
        }
    }
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    crate::util::uuid::format(&bytes)
}

fn env_truthy(name: &str) -> bool {
    env_value_truthy(&crate::util::env::var(name).unwrap_or_default())
}

fn env_value_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "on" | "true" | "yes"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_audit_binlog_encoding_failure_preserves_pending_commit() {
        let dir = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(dir.path().into(), test_config(dir.path())).unwrap();
        let mut table = binlog_table_from_schema(&test_table()).unwrap();
        table.columns[1].column_type = MYSQL_TYPE_TIMESTAMP2;
        table.columns[1].metadata = vec![0];
        manager
            .push_op(
                91,
                BinlogTxnOp::Write {
                    table,
                    rows: vec![vec![
                        Value::Int(1),
                        Value::Timestamp((u32::MAX as i64 + 1) * 1_000_000),
                    ]],
                },
            )
            .unwrap();
        for _ in 0..2 {
            let err = manager.stage_txn_commit(91, 7).unwrap_err();
            assert!(err.is(crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE));
            assert_eq!(manager.txn_len(91), 1);
        }
        manager.rollback_txn(91);
        assert_eq!(manager.txn_len(91), 0);
    }

    #[test]
    fn transaction_audit_prepared_binlog_chunks_restore_and_reject_corruption() {
        let dir = tempfile::tempdir().unwrap();
        init_global(dir.path().into(), test_config(dir.path())).unwrap();
        let manager = manager().unwrap();
        let mut table = binlog_table_from_schema(&test_table()).unwrap();
        table.columns[1].column_type = MYSQL_TYPE_BLOB;
        table.columns[1].metadata = vec![4];
        manager
            .push_op(
                92,
                BinlogTxnOp::Write {
                    table,
                    rows: vec![vec![
                        Value::Int(1),
                        Value::Bytes(vec![0xab; 2 * 1024 * 1024 + 1]),
                    ]],
                },
            )
            .unwrap();
        let expected = encode_pending_ops(&manager.state.lock().pending[&92].ops).unwrap();
        let intent = prepared::prepare(92, |path, bytes| {
            let path = dir.path().join(path);
            fs::create_dir_all(path.parent().unwrap()).map_err(io_error)?;
            fs::write(path, bytes).map_err(io_error)
        })
        .unwrap()
        .unwrap();
        assert!(intent.chunks >= 3);
        manager.rollback_txn(92);
        prepared::restore(dir.path(), 92, &intent).unwrap();
        assert_eq!(
            encode_pending_ops(&manager.state.lock().pending[&92].ops).unwrap(),
            expected
        );
        manager.rollback_txn(92);
        let path = dir.path().join(prepared::chunk_path(92, 1));
        let mut bytes = fs::read(&path).unwrap();
        bytes[0] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(prepared::restore(dir.path(), 92, &intent)
            .unwrap_err()
            .message()
            .contains("checksum"));
        assert_eq!(manager.txn_len(92), 0);
        fs::remove_file(path).unwrap();
        assert!(prepared::restore(dir.path(), 92, &intent).is_err());
        assert_eq!(manager.txn_len(92), 0);
    }
    use crate::storage::schema::{ColumnSchema, TableSchema};
    use crate::storage::{ColumnState, TableStats};

    #[derive(Default)]
    struct PartialVectoredWriter {
        bytes: Vec<u8>,
        calls: u64,
        max_per_call: usize,
    }

    impl Write for PartialVectoredWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let len = buf.len().min(self.max_per_call);
            self.bytes.extend_from_slice(&buf[..len]);
            self.calls += 1;
            Ok(len)
        }

        fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
            let mut left = self.max_per_call;
            let mut written = 0usize;
            for buf in bufs {
                let len = buf.len().min(left);
                self.bytes.extend_from_slice(&buf[..len]);
                written += len;
                left -= len;
                if left == 0 {
                    break;
                }
            }
            self.calls += 1;
            Ok(written)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn raw_test_chunk(bytes: &[u8]) -> BinlogEventChunk {
        BinlogEventChunk {
            file: "mydb-bin.000001".to_string(),
            start_pos: 0,
            end_pos: bytes.len() as u64,
            event_type: QUERY_EVENT,
            gtid: None,
            bytes: Arc::new(bytes.to_vec()),
        }
    }

    #[test]
    fn vectored_binlog_batch_preserves_order_and_handles_partial_writes() {
        let chunks = [
            raw_test_chunk(b"gtid"),
            raw_test_chunk(b"begin"),
            raw_test_chunk(b"rows"),
            raw_test_chunk(b"xid"),
        ];
        let refs: Vec<&BinlogEventChunk> = chunks.iter().collect();

        let mut single = PartialVectoredWriter {
            max_per_call: usize::MAX,
            ..PartialVectoredWriter::default()
        };
        let stats = write_event_chunks_vectored(&mut single, &refs).unwrap();
        assert_eq!(single.bytes, b"gtidbeginrowsxid");
        assert_eq!(single.calls, 1);
        assert_eq!(stats, (1, 16));

        let mut partial = PartialVectoredWriter {
            max_per_call: 3,
            ..PartialVectoredWriter::default()
        };
        let stats = write_event_chunks_vectored(&mut partial, &refs).unwrap();
        assert_eq!(partial.bytes, single.bytes);
        assert_eq!(stats.0, partial.calls);
        assert_eq!(stats.1, 16);
        assert!(partial.calls > 1);
    }

    /// Port of `mysql_common::binlog::misc::my_time_packed_from_binary` — the
    /// exact decoder a real MySQL replica uses — so we can assert TIME2
    /// round-trips. Returns signed micros.
    fn decode_time2(bytes: &[u8], fsp: u8) -> i64 {
        const TIMEF_INT_OFS: i64 = 0x800000;
        const TIMEF_OFS: i64 = 0x800000000000;
        let u24 = |b: &[u8]| ((b[0] as i64) << 16) | ((b[1] as i64) << 8) | b[2] as i64;
        let packed = match fsp {
            1 | 2 => {
                let mut intpart = u24(&bytes[0..3]) - TIMEF_INT_OFS;
                let mut frac = bytes[3] as i32;
                if intpart < 0 && frac > 0 {
                    intpart += 1;
                    frac -= 0x100;
                }
                (intpart << 24) + (frac as i64) * 10_000
            }
            3 | 4 => {
                let mut intpart = u24(&bytes[0..3]) - TIMEF_INT_OFS;
                let mut frac = ((bytes[3] as i32) << 8) | bytes[4] as i32;
                if intpart < 0 && frac > 0 {
                    intpart += 1;
                    frac -= 0x10000;
                }
                (intpart << 24) + (frac as i64) * 100
            }
            5 | 6 => {
                let mut v: i64 = 0;
                for &b in &bytes[0..6] {
                    v = (v << 8) | b as i64;
                }
                v - TIMEF_OFS
            }
            _ => (u24(&bytes[0..3]) - TIMEF_INT_OFS) << 24,
        };
        // my_packed_time → signed micros (hms int part + frac).
        let neg = packed < 0;
        let tmp = packed.abs();
        let hms = tmp >> 24;
        let frac = tmp % (1 << 24);
        let h = (hms >> 12) & 0x3ff;
        let m = (hms >> 6) & 0x3f;
        let s = hms & 0x3f;
        let micros = (h * 3600 + m * 60 + s) * 1_000_000 + frac;
        if neg {
            -micros
        } else {
            micros
        }
    }

    fn time2_col(fsp: u8) -> BinlogColumn {
        BinlogColumn {
            name: "t".into(),
            physical_index: 0,
            enum_set_values: None,
            vector_dim: None,
            charset: None,
            collation_id: crate::charset::BINARY_COLLATION_ID,
            column_type: MYSQL_TYPE_TIME2,
            metadata: vec![fsp],
            nullable: true,
            unsigned: false,
        }
    }

    #[test]
    fn time2_encoding_round_trips_including_negative_fractions() {
        // (micros, fsp) covering positive, negative-integer, and the
        // previously-broken negative-with-fraction cases at each fsp width.
        let cases: &[(i64, u8)] = &[
            (0, 0),
            (45_296_000_000, 0), // 12:34:56
            (-3_723_000_000, 0), // -01:02:03
            (-500_000, 1),       // -00:00:00.5  (sign was lost)
            (45_296_500_000, 1), // 12:34:56.5
            (-3_723_500_000, 1), // -01:02:03.5  (magnitude+frac were wrong)
            (-123_000, 3),       // -00:00:00.123
            (3_723_123_000, 3),  // 01:02:03.123
            (-3_723_123_456, 6), // -01:02:03.123456
            (45_296_654_321, 6), // 12:34:56.654321
        ];
        for &(micros, fsp) in cases {
            let mut out = Vec::new();
            encode_time2_value(&time2_col(fsp), &Value::Time(micros), &mut out).unwrap();
            let decoded = decode_time2(&out, fsp);
            // Compare at the declared precision (fsp<6 drops finer digits).
            let quantum = 10i64.pow(6 - fsp as u32);
            let expect = (micros / quantum) * quantum;
            assert_eq!(
                decoded, expect,
                "TIME2 round-trip failed for micros={micros} fsp={fsp}: bytes={out:02x?}"
            );
        }
    }

    pub(super) fn test_config(dir: &Path) -> BinlogConfig {
        BinlogConfig {
            enabled: true,
            server_id: 42,
            server_uuid: "00000000-0000-4000-8000-000000000042".to_string(),
            base_name: dir
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("mydb-bin-test")
                .to_string(),
            checksum: BinlogChecksum::Crc32,
            retention_hours: None,
            retention_bytes: None,
            log_replica_updates: true,
            sync_binlog: 1,
            rotate_bytes: None,
            ..BinlogConfig::default()
        }
    }

    pub(super) fn test_table() -> TableSchema {
        let mut table = TableSchema {
            id: 10,
            object_id: 10,
            active_generation: 1,
            database: "test".to_string(),
            name: "t".to_string(),
            engine: "InnoDB".to_string(),
            charset: "utf8mb4".to_string(),
            collation: crate::charset::DEFAULT_COLLATION.to_string(),
            comment: String::new(),
            created_ms: 0,
            updated_ms: 0,
            table_type: "BASE TABLE".to_string(),
            columns: vec![
                ColumnSchema {
                    not_null_from_scn: 0,
                    name: "id".to_string(),
                    data_type: DataTypeKind::Int,
                    nullable: false,
                    unsigned: false,
                    invisible: false,
                    zerofill: false,
                    auto_increment: false,
                    default: None,
                    read_backfill_default: None,
                    default_expr: None,
                    on_update_expr: None,
                    column_id: 1,
                    state: ColumnState::Public,
                    physical_pos: 0,
                    logical_pos: 0,
                    ordinal_position: 1,
                    charset: None,
                    collation: None,
                    comment: String::new(),
                    generated_expr: None,
                    generated_stored: false,
                    generated_column_names: Vec::new(),
                    generated_expr_ast: None,
                    generated_program: None,
                },
                ColumnSchema {
                    not_null_from_scn: 0,
                    name: "name".to_string(),
                    data_type: DataTypeKind::VarChar { len: Some(255) },
                    nullable: true,
                    unsigned: false,
                    invisible: false,
                    zerofill: false,
                    auto_increment: false,
                    default: None,
                    read_backfill_default: None,
                    default_expr: None,
                    on_update_expr: None,
                    column_id: 2,
                    state: ColumnState::Public,
                    physical_pos: 1,
                    logical_pos: 1,
                    ordinal_position: 2,
                    charset: None,
                    collation: None,
                    comment: String::new(),
                    generated_expr: None,
                    generated_stored: false,
                    generated_column_names: Vec::new(),
                    generated_expr_ast: None,
                    generated_program: None,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            fk_referenced_by: Vec::new(),
            stats: TableStats::empty(),
            system: false,
            schema_version: 0,
            system_time_epoch_us: 0,
            row_generation_txn_id: 0,
            next_column_id: 3,
            active_layout_id: 1,
            row_layouts: Vec::new(),
            pending_primary_key: None,
            primary_definition_scn: 0,
            next_index_id: 0,
            primary_publication_job_id: 0,
            partitioning: None,
            pctfree: 0,
            pctused: 0,
            mysql_table_options: Vec::new(),
            column_name_map: Default::default(),
            column_id_map: Default::default(),
            row_codec: Default::default(),
        };
        table.build_column_index();
        table
    }

    #[test]
    fn vector_row_binlog_uses_portable_long_blob_envelope() {
        let mut table = test_table();
        table.columns[1].data_type = DataTypeKind::Vector {
            dim: Some(3),
            datatype: crate::vector::VectorDataType::Float32,
        };
        assert_eq!(
            column_binlog_type(&table.columns[1].data_type),
            (MYSQL_TYPE_BLOB, vec![4])
        );
        let wire = binlog_table_from_schema(&table).unwrap();
        let column = &wire.columns[1];
        let raw = crate::engine::vector::codec::from_f32(&[1.0, 2.0, 3.0]);
        let mut encoded = Vec::new();
        encode_value(column, &Value::Vector(raw.clone()), &mut encoded).unwrap();
        let len = u32::from_le_bytes(encoded[..4].try_into().unwrap()) as usize;
        assert_eq!(len, 8 + 12 + raw.len());
        assert_eq!(&encoded[4..12], b"MYDBVEC2");
        assert_eq!(&encoded[12..16], &[1, 0, 0, 0]);
        assert_eq!(u32::from_le_bytes(encoded[16..20].try_into().unwrap()), 3);
        assert_eq!(&encoded[24..], &raw.payload);
        assert_eq!(
            crate::engine::vector::coerce_bytes_to_internal(&encoded[4..]).unwrap(),
            raw
        );
    }

    #[test]
    fn encodes_event_header_and_crc() {
        let config = BinlogConfig {
            enabled: true,
            server_id: 7,
            server_uuid: "u".to_string(),
            base_name: "mydb-bin".to_string(),
            checksum: BinlogChecksum::Crc32,
            retention_hours: None,
            retention_bytes: None,
            log_replica_updates: true,
            sync_binlog: 1,
            rotate_bytes: None,
            ..BinlogConfig::default()
        };
        let bytes = encode_event(&config, QUERY_EVENT, 4, b"abc", false);
        assert_eq!(bytes[4], QUERY_EVENT);
        assert_eq!(u32::from_le_bytes(bytes[5..9].try_into().unwrap()), 7);
        assert_eq!(
            u32::from_le_bytes(bytes[9..13].try_into().unwrap()) as usize,
            bytes.len()
        );
        assert_eq!(
            u32::from_le_bytes(bytes[13..17].try_into().unwrap()),
            4 + bytes.len() as u32
        );
        let expected = {
            let mut hasher = Hasher::new();
            hasher.update(&bytes[..bytes.len() - 4]);
            hasher.finalize()
        };
        assert_eq!(
            u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap()),
            expected
        );
    }

    #[test]
    fn commits_row_transaction_to_file() {
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        let table = test_table();
        manager
            .push_op(
                11,
                BinlogTxnOp::Write {
                    table: binlog_table_from_schema(&table).unwrap(),
                    rows: vec![vec![Value::Int(1), Value::String("a".to_string())]],
                },
            )
            .unwrap();
        manager.commit_txn(11, 99).unwrap();
        let logs = manager.binary_logs().unwrap();
        assert_eq!(logs.len(), 1);
        assert!(logs[0].size > 4);
        let events = manager.events_after(&logs[0].name, 4).unwrap();
        assert!(events
            .iter()
            .any(|event| event.bytes[4] == FORMAT_DESCRIPTION_EVENT));
        assert!(events.iter().any(|event| event.bytes[4] == TABLE_MAP_EVENT));
        assert!(events
            .iter()
            .any(|event| event.bytes[4] == WRITE_ROWS_EVENT_V2));
        assert!(events.iter().any(|event| event.bytes[4] == XID_EVENT));
    }

    #[test]
    fn row_events_split_by_bytes_and_only_last_sets_stmt_end() {
        let table = binlog_table_from_schema(&test_table()).unwrap();
        let rows: Vec<_> = (0..5)
            .map(|id| vec![Value::Int(id), Value::String("x".repeat(700))])
            .collect();
        let payloads = encode_rows_payloads(&table, RowsPayload::Write(&rows), 1_024).unwrap();
        assert!(payloads.len() > 1, "byte limit must split the row event");
        for (idx, payload) in payloads.iter().enumerate() {
            let flags = u16::from_le_bytes(payload[6..8].try_into().unwrap());
            assert_eq!(flags, u16::from(idx + 1 == payloads.len()));
            // Each test row is smaller than the target, so every chunk must be.
            assert!(payload.len() <= 1_024, "oversized chunk: {}", payload.len());
        }
    }

    #[test]
    fn pending_binlog_rows_hold_and_release_governor_reservation() {
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        let before = crate::engine::executor::mem_budget::controlled_memory_snapshot().2[11];
        manager
            .push_op(
                77,
                BinlogTxnOp::Write {
                    table: binlog_table_from_schema(&test_table()).unwrap(),
                    rows: vec![vec![Value::Int(1), Value::String("x".repeat(32_000))]],
                },
            )
            .unwrap();
        let during = crate::engine::executor::mem_budget::controlled_memory_snapshot().2[11];
        assert!(during >= before + 32_000);
        manager.rollback_txn(77);
        assert_eq!(
            crate::engine::executor::mem_budget::controlled_memory_snapshot().2[11],
            before
        );
    }

    #[test]
    fn transaction_rotates_before_crossing_configured_file_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = test_config(tmp.path());
        config.rotate_bytes = Some(512);
        let manager = BinlogSourceManager::open(tmp.path().to_path_buf(), config).unwrap();
        let mut table = test_table();
        table.columns[1].data_type = DataTypeKind::VarChar { len: Some(2048) };
        manager
            .push_op(
                88,
                BinlogTxnOp::Write {
                    table: binlog_table_from_schema(&table).unwrap(),
                    rows: vec![vec![Value::Int(1), Value::String("x".repeat(2_000))]],
                },
            )
            .unwrap();
        let commit = manager.stage_txn_commit(88, 1).unwrap().unwrap();
        assert!(
            commit.reference.file.ends_with(".000002"),
            "transaction started in {}",
            commit.reference.file
        );
    }

    #[test]
    fn staged_row_transaction_is_not_streamed_until_published() {
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        let table = test_table();
        manager
            .push_op(
                11,
                BinlogTxnOp::Write {
                    table: binlog_table_from_schema(&table).unwrap(),
                    rows: vec![vec![Value::Int(1), Value::String("a".to_string())]],
                },
            )
            .unwrap();
        let staged = manager.stage_txn_commit(11, 99).unwrap();
        let log = manager.binary_logs().unwrap().remove(0);
        let events = manager.events_after(&log.name, 4).unwrap();
        assert!(!events
            .iter()
            .any(|event| event.bytes[4] == WRITE_ROWS_EVENT_V2));
        assert!(!events.iter().any(|event| event.bytes[4] == XID_EVENT));

        manager.publish_txn_commit(staged).unwrap();
        let events = manager.events_after(&log.name, 4).unwrap();
        assert!(events
            .iter()
            .any(|event| event.bytes[4] == WRITE_ROWS_EVENT_V2));
        assert!(events.iter().any(|event| event.bytes[4] == XID_EVENT));
    }

    #[test]
    fn commit_order_blocks_later_wal_append_and_skips_ddl_turns() {
        let order = Arc::new(BinlogCommitOrder::default());
        let first = order.allocate(true);
        let ddl = order.allocate(false);
        let third = order.allocate(true);
        assert_eq!((first, ddl, third), (1, 2, 3));

        let (tx, rx) = std::sync::mpsc::channel();
        let waiter = {
            let order = order.clone();
            std::thread::spawn(move || {
                order.wait_wal(third).unwrap();
                tx.send(()).unwrap();
                order.finish_wal(third);
            })
        };
        assert!(
            rx.recv_timeout(Duration::from_millis(20)).is_err(),
            "later WAL append passed the earlier transaction"
        );
        order.finish_wal(first);
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        waiter.join().unwrap();
    }

    #[test]
    fn reset_preserves_pending_and_unpublished_transaction_references() {
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        manager
            .push_op(
                1,
                BinlogTxnOp::Write {
                    table: binlog_table_from_schema(&test_table()).unwrap(),
                    rows: vec![vec![Value::Int(1), Value::String("pending".into())]],
                },
            )
            .unwrap();
        let start = manager.current_position().unwrap().position;
        assert_eq!(
            manager
                .reset_master(|| test_reset_wal_boundary(&manager))
                .unwrap_err()
                .code(),
            1205
        );
        assert_eq!(manager.txn_len(1), 1);
        assert_eq!(manager.current_position().unwrap().position, start);
        let commit = manager.stage_txn_commit(1, 1).unwrap().unwrap();
        manager.sync_staged_commit(&commit).unwrap();
        assert_eq!(
            manager
                .reset_master(|| test_reset_wal_boundary(&manager))
                .unwrap_err()
                .code(),
            1205
        );
        assert_commit_bytes(&manager, &commit);
        manager.commit_order.finish_wal(commit.order);
        manager.publish_txn_commit(Some(commit)).unwrap();
        manager
            .reset_master(|| test_reset_wal_boundary(&manager))
            .unwrap();
    }

    #[test]
    fn cached_append_file_survives_batches_and_reopens_after_rotate_and_reset() {
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        for txn_id in 1..=2 {
            let commit = stage_test_write(&manager, txn_id);
            manager.sync_staged_commit(&commit).unwrap();
            assert_commit_bytes(&manager, &commit);
            manager.commit_order.finish_wal(commit.order);
            manager.publish_txn_commit(Some(commit)).unwrap();
        }
        assert_eq!(manager.group_sync.file_opens.load(Ordering::Relaxed), 1);
        manager.rotate().unwrap();
        let rotated = stage_test_write(&manager, 3);
        manager.sync_staged_commit(&rotated).unwrap();
        assert_commit_bytes(&manager, &rotated);
        manager.commit_order.finish_wal(rotated.order);
        manager.publish_txn_commit(Some(rotated)).unwrap();
        assert_eq!(manager.group_sync.file_opens.load(Ordering::Relaxed), 2);
        manager
            .reset_master(|| test_reset_wal_boundary(&manager))
            .unwrap();
        let reset = stage_test_write(&manager, 4);
        assert!(reset.reference.file.ends_with(".000001"));
        manager.sync_staged_commit(&reset).unwrap();
        assert_commit_bytes(&manager, &reset);
        assert_eq!(manager.group_sync.file_opens.load(Ordering::Relaxed), 3);
        // Reset again while .000001 itself is cached: same pathname, new inode.
        manager.commit_order.finish_wal(reset.order);
        manager.publish_txn_commit(Some(reset)).unwrap();
        manager
            .reset_master(|| test_reset_wal_boundary(&manager))
            .unwrap();
        let same_name = stage_test_write(&manager, 5);
        manager.sync_staged_commit(&same_name).unwrap();
        assert_commit_bytes(&manager, &same_name);
        assert_eq!(manager.group_sync.file_opens.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn staged_write_failure_reaches_all_waiters_without_advancing_durability() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = Arc::new(
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap(),
        );
        let commits: Vec<_> = (1..=8).map(|id| stage_test_write(&manager, id)).collect();
        let name = commits[0].reference.file.clone();
        let path = manager.dir.join(&name);
        let before = fs::read(&path).unwrap();
        // A read-only descriptor produces a real write error without relying
        // on disk exhaustion, permissions of the invoking user or timing.
        manager.group_sync.state.lock().writer = Some(BinlogAppendFile {
            name,
            file: File::open(&path).unwrap(),
        });
        let barrier = Arc::new(std::sync::Barrier::new(commits.len()));
        let workers: Vec<_> = commits
            .into_iter()
            .map(|commit| {
                let manager = manager.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    manager
                        .sync_staged_commit(&commit)
                        .unwrap_err()
                        .message()
                        .to_owned()
                })
            })
            .collect();
        let messages: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
        assert!(messages.iter().all(|message| message == &messages[0]));
        assert_eq!(fs::read(path).unwrap(), before);
        let state = manager.group_sync.state.lock();
        assert_eq!(state.durable_order, 0);
        assert!(!state.syncing);
        assert!(state.writer.is_none());
        assert!(state.fatal_error.is_some());
        assert!(state.writes.is_empty());
        drop(state);
        // Every failed BinlogCommit releases its WAL/publication order on
        // drop. RESET can then clear the fatal state after storage recovers.
        manager
            .reset_master(|| test_reset_wal_boundary(&manager))
            .unwrap();
        let recovered = stage_test_write(&manager, 99);
        manager.sync_staged_commit(&recovered).unwrap();
        manager.commit_order.finish_wal(recovered.order);
        manager.publish_txn_commit(Some(recovered)).unwrap();
    }

    #[test]
    fn pending_growth_preserves_savepoints_and_capacity_accounting() {
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        let mut last_capacity = 0;
        let mut growths = 0;
        for id in 0..65 {
            manager
                .push_op(
                    1,
                    BinlogTxnOp::Write {
                        table: binlog_table_from_schema(&test_table()).unwrap(),
                        rows: vec![vec![Value::Int(id), Value::String("镜像".repeat(100))]],
                    },
                )
                .unwrap();
            let state = manager.state.lock();
            let pending = &state.pending[&1];
            if pending.ops.capacity() != last_capacity {
                growths += 1;
                last_capacity = pending.ops.capacity();
            }
            let expected = pending.ops.capacity() * std::mem::size_of::<BinlogTxnOp>()
                + pending
                    .ops
                    .iter()
                    .map(binlog_op_nested_bytes)
                    .sum::<usize>();
            assert_eq!(pending.memory_charge.bytes(), expected);
        }
        assert!(growths <= 8, "transaction operations grew {growths} times");
        manager.truncate_txn(1, 17);
        {
            let state = manager.state.lock();
            let pending = &state.pending[&1];
            let expected = pending.ops.capacity() * std::mem::size_of::<BinlogTxnOp>()
                + pending
                    .ops
                    .iter()
                    .map(binlog_op_nested_bytes)
                    .sum::<usize>();
            assert_eq!(pending.memory_charge.bytes(), expected);
        }
        let commit = manager.stage_txn_commit(1, 1).unwrap().unwrap();
        assert_eq!(
            commit
                .chunks
                .iter()
                .filter(|c| c.event_type == WRITE_ROWS_EVENT_V2)
                .count(),
            17
        );
        manager.sync_staged_commit(&commit).unwrap();
        assert_commit_bytes(&manager, &commit);
    }

    #[test]
    fn automatic_rotation_drains_earlier_staged_transaction_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        let first = stage_test_write(&manager, 1);
        manager
            .rotate_bytes
            .store(first.reference.end_pos + 1, Ordering::Relaxed);
        let manager = Arc::new(manager);
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = {
            let manager = manager.clone();
            std::thread::spawn(move || tx.send(stage_test_write(&manager, 2)).unwrap())
        };
        assert!(rx.recv_timeout(Duration::from_millis(20)).is_err());
        assert!(!manager
            .state
            .lock()
            .events
            .iter()
            .any(|e| e.event_type == ROTATE_EVENT));
        manager.sync_staged_commit(&first).unwrap();
        assert_commit_bytes(&manager, &first);
        let first_file = first.reference.file.clone();
        manager.commit_order.finish_wal(first.order);
        manager.publish_txn_commit(Some(first)).unwrap();
        let second = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        worker.join().unwrap();
        assert_ne!(first_file, second.reference.file);
        manager.sync_staged_commit(&second).unwrap();
        assert_commit_bytes(&manager, &second);
        manager.commit_order.finish_wal(second.order);
        manager.publish_txn_commit(Some(second)).unwrap();
        let state = manager.state.lock();
        assert!(
            state.events.windows(2).all(|pair| {
                (&pair[0].file, pair[0].end_pos) < (&pair[1].file, pair[1].end_pos)
            }),
            "dump events must remain file/position ordered across rotation"
        );
    }

    pub(super) fn stage_test_write(manager: &BinlogSourceManager, txn_id: u64) -> BinlogCommit {
        manager
            .push_op(
                txn_id,
                BinlogTxnOp::Write {
                    table: binlog_table_from_schema(&test_table()).unwrap(),
                    rows: vec![vec![Value::Int(txn_id as i64), Value::String("row".into())]],
                },
            )
            .unwrap();
        manager.stage_txn_commit(txn_id, txn_id).unwrap().unwrap()
    }

    fn assert_commit_bytes(manager: &BinlogSourceManager, commit: &BinlogCommit) {
        let bytes = fs::read(manager.dir.join(&commit.reference.file)).unwrap();
        for chunk in &commit.chunks {
            assert_eq!(
                &bytes[chunk.start_pos as usize..chunk.end_pos as usize],
                chunk.bytes.as_slice(),
                "binlog bytes differ from reserved position in {}",
                chunk.file
            );
        }
    }

    #[test]
    fn concurrent_staged_commits_share_binlog_sync_batches() {
        const N: usize = 16;
        let tmp = tempfile::tempdir().unwrap();
        let manager = Arc::new(
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap(),
        );
        let table = binlog_table_from_schema(&test_table()).unwrap();
        let mut commits = Vec::with_capacity(N);
        for txn_id in 1..=N as u64 {
            manager
                .push_op(
                    txn_id,
                    BinlogTxnOp::Write {
                        table: table.clone(),
                        rows: vec![vec![
                            Value::Int(txn_id as i64),
                            Value::String("v".to_string()),
                        ]],
                    },
                )
                .unwrap();
            commits.push(manager.stage_txn_commit(txn_id, txn_id).unwrap().unwrap());
        }

        let barrier = Arc::new(std::sync::Barrier::new(N));
        let workers: Vec<_> = commits
            .into_iter()
            .map(|commit| {
                let manager = manager.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    manager.sync_staged_commit(&commit).unwrap();
                    commit
                })
            })
            .collect();
        for worker in workers {
            let commit = worker.join().unwrap();
            assert_commit_bytes(&manager, &commit);
            manager.commit_order.finish_wal(commit.order);
            manager.publish_txn_commit(Some(commit)).unwrap();
        }

        assert_eq!(
            manager.group_sync.requests.load(Ordering::Relaxed),
            N as u64
        );
        assert!(
            manager.group_sync.batches.load(Ordering::Relaxed) < N as u64,
            "concurrent commits were not coalesced"
        );
        assert!(manager.group_sync.batch_max.load(Ordering::Relaxed) >= 2);
    }

    #[test]
    fn open_truncates_uncommitted_staged_binlog_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let bootstrap_pos = {
            let manager =
                BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path()))
                    .unwrap();
            let table = test_table();
            let pos = manager.current_position().unwrap().position;
            manager
                .push_op(
                    11,
                    BinlogTxnOp::Write {
                        table: binlog_table_from_schema(&table).unwrap(),
                        rows: vec![vec![Value::Int(1), Value::String("a".to_string())]],
                    },
                )
                .unwrap();
            manager.stage_txn_commit(11, 99).unwrap().unwrap();
            assert!(manager.state.lock().active_pos > pos);
            assert_eq!(manager.current_position().unwrap().position, pos);
            pos
        };

        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        let log = manager.binary_logs().unwrap().remove(0);
        assert_eq!(log.size, bootstrap_pos);
        let events = manager.events_after(&log.name, 4).unwrap();
        assert!(!events
            .iter()
            .any(|event| event.bytes[4] == WRITE_ROWS_EVENT_V2));
        assert!(!events.iter().any(|event| event.bytes[4] == XID_EVENT));
    }

    #[test]
    fn checkpoint_manifest_keeps_committed_tail_without_wal_refs() {
        let tmp = tempfile::tempdir().unwrap();
        let committed_pos = {
            let manager =
                BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path()))
                    .unwrap();
            manager
                .push_op(
                    11,
                    BinlogTxnOp::Write {
                        table: binlog_table_from_schema(&test_table()).unwrap(),
                        rows: vec![vec![Value::Int(1), Value::String("a".to_string())]],
                    },
                )
                .unwrap();
            manager.commit_txn(11, 99).unwrap();
            let committed_pos = manager.current_position().unwrap().position;
            manager.checkpoint_manifest().unwrap();
            committed_pos
        };

        // Model restart after checkpoint compaction: the transaction's
        // WalBinlogRef is no longer present, so only the durable manifest can
        // distinguish the committed prefix from an uncommitted staged tail.
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        let log = manager.binary_logs().unwrap().remove(0);
        assert_eq!(log.size, committed_pos);
        let events = manager.events_after(&log.name, 4).unwrap();
        assert!(events
            .iter()
            .any(|event| event.bytes[4] == WRITE_ROWS_EVENT_V2));
        assert!(events.iter().any(|event| event.bytes[4] == XID_EVENT));
        assert_eq!(manager.current_position().unwrap().position, committed_pos);
    }

    #[test]
    fn open_keeps_wal_committed_staged_binlog_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let reference = {
            let manager =
                BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path()))
                    .unwrap();
            let table = test_table();
            manager
                .push_op(
                    11,
                    BinlogTxnOp::Write {
                        table: binlog_table_from_schema(&table).unwrap(),
                        rows: vec![vec![Value::Int(1), Value::String("a".to_string())]],
                    },
                )
                .unwrap();
            let staged = manager.stage_txn_commit(11, 99).unwrap().unwrap();
            // The production commit path drains the staged binlog group before
            // it appends the WAL CommitTxn reference.
            manager.sync_staged_commit(&staged).unwrap();
            staged.wal_ref().clone()
        };
        {
            let mut log = crate::wal::LogManager::open(tmp.path()).unwrap();
            log.append(
                11,
                crate::wal::WalPayload::CommitTxn {
                    commit_scn: 99,
                    commit_unix_us: 1,
                    binlog: Some(reference.clone()),
                },
            )
            .unwrap();
        }

        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        let log = manager.binary_logs().unwrap().remove(0);
        assert_eq!(log.size, reference.end_pos);
        let events = manager.events_after(&log.name, 4).unwrap();
        assert!(events
            .iter()
            .any(|event| event.bytes[4] == WRITE_ROWS_EVENT_V2));
        assert!(events.iter().any(|event| event.bytes[4] == XID_EVENT));
        assert_eq!(manager.state.lock().gtid_executed, reference.gtid.unwrap());
    }

    #[test]
    fn startup_publication_survives_wal_reclamation_and_advances_gtid() {
        let tmp = tempfile::tempdir().unwrap();
        let reference = {
            let manager =
                BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path()))
                    .unwrap();
            let table = test_table();
            manager
                .push_op(
                    11,
                    BinlogTxnOp::Write {
                        table: binlog_table_from_schema(&table).unwrap(),
                        rows: vec![vec![Value::Int(1), Value::String("a".to_string())]],
                    },
                )
                .unwrap();
            let staged = manager.stage_txn_commit(11, 99).unwrap().unwrap();
            // The production commit path drains the staged binlog group before
            // it appends the WAL CommitTxn reference.
            manager.sync_staged_commit(&staged).unwrap();
            staged.wal_ref().clone()
        };
        {
            let mut log = crate::wal::LogManager::open(tmp.path()).unwrap();
            log.append(
                11,
                crate::wal::WalPayload::CommitTxn {
                    commit_scn: 99,
                    commit_unix_us: 1,
                    binlog: Some(reference.clone()),
                },
            )
            .unwrap();
        }

        recover_before_checkpoint(tmp.path()).unwrap();
        // Model checkpoint collapse after publication, followed by another crash.
        fs::remove_dir_all(tmp.path().join("system/wal")).unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        assert_eq!(manager.state.lock().next_gno, 2);
        let log = manager.binary_logs().unwrap().remove(0);
        assert_eq!(log.size, reference.end_pos);
        let events = manager.events_after(&log.name, 4).unwrap();
        assert!(events
            .iter()
            .any(|event| event.bytes[4] == WRITE_ROWS_EVENT_V2));
        assert!(events.iter().any(|event| event.bytes[4] == XID_EVENT));
        assert_eq!(manager.state.lock().gtid_executed, reference.gtid.unwrap());
    }

    #[test]
    fn timestamp2_rejects_values_outside_wire_range_without_silent_wrap() {
        // MySQL wire format for TIMESTAMP2 is unsigned 32-bit seconds, so
        // 1970..2106-02-07 06:28:15 is representable. Pre-1970 (negative)
        // used to silently wrap to 2106; the new encoder rejects it as
        // ER_DATA_OUT_OF_RANGE (1264).
        let column = BinlogColumn {
            name: "ts".to_string(),
            physical_index: 0,
            column_type: MYSQL_TYPE_TIMESTAMP2,
            metadata: vec![6],
            nullable: false,
            unsigned: false,
            enum_set_values: None,
            vector_dim: None,
            charset: None,
            collation_id: crate::charset::BINARY_COLLATION_ID,
        };
        let mut zero = Vec::new();
        encode_timestamp2_value(
            &column,
            &Value::Timestamp(crate::types::ZERO_DATETIME),
            &mut zero,
        )
        .unwrap();
        assert_eq!(zero, vec![0; 7]);
        // Pre-1970: rejected (was silently wrapped by the old i32 encoding).
        let pre1970 = chrono::NaiveDate::from_ymd_opt(1969, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let err = encode_timestamp2_value(
            &column,
            &Value::Timestamp(pre1970.and_utc().timestamp_micros()),
            &mut Vec::new(),
        )
        .unwrap_err();
        assert_eq!(err.code(), 1264);
        let err = encode_timestamp2_value(
            &column,
            &Value::Timestamp(crate::types::MAX_DATETIME_MICROS),
            &mut Vec::new(),
        )
        .unwrap_err();
        assert_eq!(err.code(), 1264);
        assert!(err.message().contains("native WAL replication"));
        // Post-2106: also rejected (was silently truncated by i32 cast).
        let post2106 = chrono::NaiveDate::from_ymd_opt(2107, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let err = encode_timestamp2_value(
            &column,
            &Value::Timestamp(post2106.and_utc().timestamp_micros()),
            &mut Vec::new(),
        )
        .unwrap_err();
        assert_eq!(err.code(), 1264);
        // Within u32 range (2040): must accept.
        let dt2040 = chrono::NaiveDate::from_ymd_opt(2040, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let mut buf = Vec::new();
        encode_timestamp2_value(
            &column,
            &Value::Timestamp(dt2040.and_utc().timestamp_micros()),
            &mut buf,
        )
        .expect("2040 is within TIMESTAMP2's u32 range");
    }

    #[test]
    fn mysql_common_decodes_committed_row_events() {
        use mysql_common::binlog::consts::BinlogVersion;
        use mysql_common::binlog::events::EventData;
        use mysql_common::binlog::EventStreamReader;

        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        let table = binlog_table_from_schema(&test_table()).unwrap();
        manager
            .push_op(
                11,
                BinlogTxnOp::Write {
                    table: table.clone(),
                    rows: vec![vec![Value::Int(1), Value::String("a".to_string())]],
                },
            )
            .unwrap();
        manager
            .push_op(
                11,
                BinlogTxnOp::Update {
                    table: table.clone(),
                    rows: vec![(
                        vec![Value::Int(1), Value::String("a".to_string())],
                        vec![Value::Int(1), Value::String("b".to_string())],
                    )],
                },
            )
            .unwrap();
        manager
            .push_op(
                11,
                BinlogTxnOp::Delete {
                    table,
                    rows: vec![vec![Value::Int(1), Value::String("b".to_string())]],
                },
            )
            .unwrap();
        manager.commit_txn(11, 99).unwrap();

        let log = manager.binary_logs().unwrap().remove(0);
        let events = manager.events_after(&log.name, 4).unwrap();
        let mut reader = EventStreamReader::new(BinlogVersion::Version4);
        let mut row_events = 0;
        let mut decoded_images = 0;
        for chunk in events {
            let event = reader
                .read(std::io::Cursor::new(chunk.bytes.as_ref().as_slice()))
                .unwrap()
                .unwrap();
            if let EventData::RowsEvent(rows) = event.read_data().unwrap().unwrap() {
                let table_map = reader.get_tme(rows.table_id()).unwrap();
                for row in rows.rows(table_map) {
                    let (before, after) = row.unwrap();
                    decoded_images += usize::from(before.is_some());
                    decoded_images += usize::from(after.is_some());
                }
                row_events += 1;
            }
        }

        assert_eq!(row_events, 3);
        assert_eq!(decoded_images, 4);
    }

    #[test]
    fn table_map_full_metadata_exposes_column_names_and_unsigned_flags() {
        use mysql_common::binlog::consts::BinlogVersion;
        use mysql_common::binlog::events::{EventData, OptionalMetadataField};
        use mysql_common::binlog::EventStreamReader;
        use mysql_common::constants::ColumnType as MySqlColumnType;

        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        let mut table = test_table();
        table.columns[0].unsigned = true;
        table.build_column_index();
        manager
            .push_op(
                11,
                BinlogTxnOp::Write {
                    table: binlog_table_from_schema(&table).unwrap(),
                    rows: vec![vec![Value::UInt(1), Value::String("a".to_string())]],
                },
            )
            .unwrap();
        manager.commit_txn(11, 99).unwrap();

        let log = manager.binary_logs().unwrap().remove(0);
        let events = manager.events_after(&log.name, 4).unwrap();
        let mut reader = EventStreamReader::new(BinlogVersion::Version4);
        let mut names = None;
        let mut signedness = None;
        let mut charsets = None;
        for chunk in events {
            let event = reader
                .read(std::io::Cursor::new(chunk.bytes.as_ref().as_slice()))
                .unwrap()
                .unwrap();
            if let EventData::TableMapEvent(table_map) = event.read_data().unwrap().unwrap() {
                charsets = Some(
                    mysql_common::binlog::events::OptionalMetaExtractor::new(
                        table_map.iter_optional_meta(),
                    )
                    .unwrap()
                    .iter_charset()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
                );
                assert_eq!(
                    table_map.get_column_type(0).unwrap(),
                    Some(MySqlColumnType::MYSQL_TYPE_LONG)
                );
                for field in table_map.iter_optional_meta() {
                    match field.unwrap() {
                        OptionalMetadataField::ColumnName(column_names) => {
                            names = Some(
                                column_names
                                    .iter_names()
                                    .map(|name| name.unwrap().name().into_owned())
                                    .collect::<Vec<_>>(),
                            );
                        }
                        OptionalMetadataField::Signedness(flags) => {
                            signedness = Some(flags.iter().map(|flag| *flag).collect::<Vec<_>>());
                        }
                        _ => {}
                    }
                }
            }
        }

        assert_eq!(names.unwrap(), vec!["id".to_string(), "name".to_string()]);
        assert_eq!(signedness.unwrap(), vec![true]);
        assert_eq!(
            charsets.unwrap(),
            vec![crate::charset::UTF8MB4_0900_AI_CI_ID]
        );
    }

    #[test]
    fn commits_gtid_events_and_resolves_gtid_dump_start() {
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        let table = binlog_table_from_schema(&test_table()).unwrap();
        manager
            .push_op(
                11,
                BinlogTxnOp::Write {
                    table,
                    rows: vec![vec![Value::Int(1), Value::String("a".to_string())]],
                },
            )
            .unwrap();
        manager.commit_txn(11, 99).unwrap();

        let log = manager.binary_logs().unwrap().remove(0);
        let events = manager.events_after(&log.name, 4).unwrap();
        let gtid = events
            .iter()
            .find_map(|event| event.gtid.clone())
            .expect("gtid event");
        assert_eq!(gtid, "00000000-0000-4000-8000-000000000042:1");
        assert_eq!(manager.state.lock().gtid_executed, gtid);

        let start = manager.position_for_gtid_dump("", 4, "").unwrap();
        assert_eq!(start.position, 4);
        let caught_up = manager.position_for_gtid_dump("", 4, &gtid).unwrap();
        assert_eq!(caught_up.file, log.name);
        assert_eq!(caught_up.position, log.size);
    }

    #[tokio::test]
    async fn audit_rotate_wakes_registered_waiter_and_expiration_runs_without_rotate() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = test_config(tmp.path());
        config.retention_hours = Some(1);
        let manager = BinlogSourceManager::open(tmp.path().to_path_buf(), config).unwrap();
        let notified = manager.wait_for_new_events();
        tokio::pin!(notified);
        notified.as_mut().enable();
        manager.rotate().unwrap();
        tokio::time::timeout(Duration::from_millis(100), &mut notified)
            .await
            .unwrap();
        manager
            .maintain_retention(SystemTime::now() + Duration::from_secs(7200))
            .unwrap();
        assert_eq!(manager.binary_logs().unwrap().len(), 1);
    }

    #[test]
    fn audit_duplicate_registration_and_semi_sync_recovery() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = Arc::new(
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap(),
        );
        let guard = manager.register_replica(semi_sync_replica(9)).unwrap();
        manager.semi_sync_mark_replica(9);
        assert!(manager.register_replica(semi_sync_replica(9)).is_err());
        manager.semi_sync.enabled.store(true, Ordering::Relaxed);
        manager.semi_sync.timeout_ms.store(30, Ordering::Relaxed);
        assert_eq!(
            manager.semi_sync_wait("binlog.000001", 100),
            SemiSyncOutcome::TimedOut
        );
        let start = std::time::Instant::now();
        assert_eq!(
            manager.semi_sync_wait("binlog.000001", 200),
            SemiSyncOutcome::TimedOut
        );
        assert!(start.elapsed() < Duration::from_millis(20));
        manager.record_semi_sync_ack(9, "binlog.000001", 200);
        assert!(manager.semi_sync.degraded_at.lock().unwrap().is_none());
        assert_eq!(
            manager.semi_sync_wait("binlog.000001", 200),
            SemiSyncOutcome::Acked
        );
        drop(guard);
        assert!(manager.registered_replicas().is_empty());
    }

    #[test]
    fn audit_dump_position_and_reset_dispatch() {
        assert!(is_binlog_admin_sql("RESET MASTER"));
        assert!(is_binlog_admin_sql("RESET BINARY LOGS AND GTIDS"));
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        let pos = manager.current_position().unwrap();
        assert!(manager.events_after(&pos.file, pos.position + 1).is_err());
        manager.rotate().unwrap();
        assert!(manager.events_after(&pos.file, u64::MAX).is_err());
        let mut file = pos.file;
        let mut offset = pos.position;
        for _ in 0..8 {
            let events = manager
                .events_after_bounded(&file, offset, 1, usize::MAX)
                .unwrap();
            if events.is_empty() {
                return;
            }
            let event = &events[0];
            assert!(
                event.file != file || event.end_pos > offset,
                "dump cursor must advance"
            );
            file = event.file.clone();
            offset = event.end_pos;
        }
        panic!("dump failed to reach tail");
    }

    #[test]
    fn purge_selectors_preserve_live_and_reopened_gtid_history() {
        for reopen in [false, true] {
            for by_time in [false, true] {
                let tmp = tempfile::tempdir().unwrap();
                let mut manager =
                    BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path())).unwrap();
                let mut gtids = Vec::new();
                for txn in 1..=3 {
                    let commit = stage_test_write(&manager, txn);
                    gtids.push(commit.reference.gtid.clone().unwrap());
                    manager.sync_staged_commit(&commit).unwrap();
                    manager.commit_order.finish_wal(commit.order);
                    manager.publish_txn_commit(Some(commit)).unwrap();
                    if txn < 3 {
                        manager.rotate().unwrap();
                    }
                }
                let logs = manager.binary_logs().unwrap();
                let active = manager.current_position().unwrap();
                manager.checkpoint_manifest().unwrap();
                if reopen {
                    drop(manager);
                    manager = BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path()))
                        .unwrap();
                    assert!(manager.state.lock().events.is_empty());
                }
                if by_time {
                    manager.purge_before("2099-12-31 23:59:59").unwrap();
                } else {
                    manager.purge_to(&active.file).unwrap();
                }
                let purged = append_gtid(&gtids[0], &gtids[1]);
                assert_eq!(manager.state.lock().gtid_purged, purged);
                assert_eq!(manager.binary_logs().unwrap().len(), 1);
                for log in &logs[..2] {
                    assert!(!manager.dir.join(&log.name).exists());
                }
                assert!(!manager.events_after(&active.file, 4).unwrap().is_empty());
                drop(manager);
                let manager =
                    BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path())).unwrap();
                assert_eq!(manager.state.lock().gtid_purged, purged);
                assert_eq!(manager.current_position().unwrap().file, active.file);
                assert_eq!(
                    manager.current_position().unwrap().position,
                    active.position
                );
                assert!(manager.position_for_gtid_dump("", 4, &purged).is_ok());
                assert!(manager.position_for_gtid_dump("", 4, &gtids[2]).is_err());
            }
        }
    }

    #[test]
    fn rotates_and_purges_binary_logs() {
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        let first = manager.current_position().unwrap().file;
        manager.rotate().unwrap();
        let logs = manager.binary_logs().unwrap();
        assert_eq!(logs.len(), 2);
        assert_ne!(logs[1].name, first);
        manager.purge_to(&logs[1].name).unwrap();
        let logs = manager.binary_logs().unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].name, manager.current_position().unwrap().file);
    }

    #[test]
    fn audit_preamble_has_one_fde_and_artificial_positions() {
        let dir = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(dir.path().to_path_buf(), test_config(dir.path())).unwrap();
        let file = manager.current_position().unwrap().file;
        let preamble = manager.dump_preamble(&file, 4);
        assert_eq!(preamble.len(), 1);
        let rotate = &preamble[0];
        assert_eq!(rotate[4], ROTATE_EVENT);
        assert_eq!(&rotate[13..17], &[0; 4]);
        assert_eq!(&rotate[17..19], &[0x20, 0]);
        assert_eq!(u64::from_le_bytes(rotate[19..27].try_into().unwrap()), 4);
        let events = manager.events_after(&file, 4).unwrap();
        assert_eq!(events[0].bytes[4], FORMAT_DESCRIPTION_EVENT);
        let middle = manager.dump_preamble(&file, 1234);
        assert_eq!(middle.len(), 2);
        assert_eq!(middle[1][4], FORMAT_DESCRIPTION_EVENT);
        for event in &middle {
            let end = event.len() - 4;
            assert_eq!(
                crc32fast::hash(&event[..end]),
                u32::from_le_bytes(event[end..].try_into().unwrap())
            );
            assert_eq!(&event[13..17], &[0; 4]);
        }
        assert_eq!(
            u64::from_le_bytes(middle[0][19..27].try_into().unwrap()),
            1234
        );
    }

    #[test]
    fn audit_unsupported_semisync_receiver_is_explicit() {
        assert!(set_semi_sync_replica_enabled(true)
            .unwrap_err()
            .is(crate::error::catalog::ER_NOT_SUPPORTED_YET));
        assert!(!semi_sync_replica_enabled());
        set_semi_sync_replica_enabled(false).unwrap();
    }

    #[test]
    fn audit_dump_accepts_mysql_file_position_flags() {
        for flags in [0x0002u16, 0x0003, 0x8000] {
            let mut payload = 4u32.to_le_bytes().to_vec();
            payload.extend_from_slice(&flags.to_le_bytes());
            payload.extend_from_slice(&77u32.to_le_bytes());
            payload.extend_from_slice(b"mydb-bin.000001");
            let request = parse_binlog_dump_request(&payload).unwrap();
            assert_eq!(request.non_blocking(), flags & 1 != 0);
        }
    }

    #[test]
    fn parses_com_binlog_dump_request() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1234u32.to_le_bytes());
        payload.extend_from_slice(&BINLOG_DUMP_NON_BLOCK.to_le_bytes());
        payload.extend_from_slice(&77u32.to_le_bytes());
        payload.extend_from_slice(b"mydb-bin.000001");
        let request = parse_binlog_dump_request(&payload).unwrap();
        assert_eq!(request.position, 1234);
        assert!(request.non_blocking());
        assert_eq!(request.slave_server_id, 77);
        assert_eq!(request.file, "mydb-bin.000001");
    }

    #[test]
    fn parses_com_binlog_dump_gtid_request() {
        let mut sid_block = Vec::new();
        sid_block.extend_from_slice(&1u64.to_le_bytes());
        sid_block.extend_from_slice(
            &uuid_string_to_bytes("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap(),
        );
        sid_block.extend_from_slice(&1u64.to_le_bytes());
        sid_block.extend_from_slice(&1u64.to_le_bytes());
        sid_block.extend_from_slice(&3u64.to_le_bytes());

        let mut payload = Vec::new();
        payload.extend_from_slice(&0x0004u16.to_le_bytes());
        payload.extend_from_slice(&77u32.to_le_bytes());
        payload.extend_from_slice(&(b"mysql-bin.000123".len() as u32).to_le_bytes());
        payload.extend_from_slice(b"mysql-bin.000123");
        payload.extend_from_slice(&4u64.to_le_bytes());
        payload.extend_from_slice(&(sid_block.len() as u32).to_le_bytes());
        payload.extend_from_slice(&sid_block);

        let request = parse_binlog_dump_gtid_request(&payload).unwrap();
        assert_eq!(request.slave_server_id, 77);
        assert_eq!(request.file, "mysql-bin.000123");
        assert_eq!(
            request.executed_gtid_set,
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-2"
        );
    }

    #[test]
    fn encodes_decimal_date_and_bit_rows() {
        let mut table = test_table();
        table.columns.push(ColumnSchema {
            not_null_from_scn: 0,
            name: "amount".to_string(),
            data_type: DataTypeKind::Decimal {
                precision: Some(10),
                scale: Some(2),
            },
            nullable: false,
            unsigned: false,
            invisible: false,
            zerofill: false,
            auto_increment: false,
            default: None,
            read_backfill_default: None,
            default_expr: None,
            on_update_expr: None,
            column_id: 3,
            state: ColumnState::Public,
            physical_pos: 2,
            logical_pos: 2,
            ordinal_position: 3,
            charset: None,
            collation: None,
            comment: String::new(),
            generated_expr: None,
            generated_stored: false,
            generated_column_names: Vec::new(),
            generated_expr_ast: None,
            generated_program: None,
        });
        table.columns.push(ColumnSchema {
            not_null_from_scn: 0,
            name: "d".to_string(),
            data_type: DataTypeKind::Date,
            nullable: false,
            unsigned: false,
            invisible: false,
            zerofill: false,
            auto_increment: false,
            default: None,
            read_backfill_default: None,
            default_expr: None,
            on_update_expr: None,
            column_id: 4,
            state: ColumnState::Public,
            physical_pos: 3,
            logical_pos: 3,
            ordinal_position: 4,
            charset: None,
            collation: None,
            comment: String::new(),
            generated_expr: None,
            generated_stored: false,
            generated_column_names: Vec::new(),
            generated_expr_ast: None,
            generated_program: None,
        });
        table.columns.push(ColumnSchema {
            not_null_from_scn: 0,
            name: "b".to_string(),
            data_type: DataTypeKind::Bit { len: Some(9) },
            nullable: false,
            unsigned: false,
            invisible: false,
            zerofill: false,
            auto_increment: false,
            default: None,
            read_backfill_default: None,
            default_expr: None,
            on_update_expr: None,
            column_id: 5,
            state: ColumnState::Public,
            physical_pos: 4,
            logical_pos: 4,
            ordinal_position: 5,
            charset: None,
            collation: None,
            comment: String::new(),
            generated_expr: None,
            generated_stored: false,
            generated_column_names: Vec::new(),
            generated_expr_ast: None,
            generated_program: None,
        });
        table.next_column_id = 6;
        table.build_column_index();

        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        manager
            .push_op(
                11,
                BinlogTxnOp::Write {
                    table: binlog_table_from_schema(&table).unwrap(),
                    rows: vec![vec![
                        Value::Int(1),
                        Value::String("a".to_string()),
                        Value::DecimalCompact(1234, 2),
                        Value::Date(0),
                        Value::UInt(0b1_0000_0001),
                    ]],
                },
            )
            .unwrap();
        manager.commit_txn(11, 99).unwrap();

        let log = manager.binary_logs().unwrap().remove(0);
        let events = manager.events_after(&log.name, 4).unwrap();
        let mut reader = mysql_common::binlog::EventStreamReader::new(
            mysql_common::binlog::consts::BinlogVersion::Version4,
        );
        let mut saw_rows = false;
        for chunk in events {
            let event = reader
                .read(std::io::Cursor::new(chunk.bytes.as_ref().as_slice()))
                .unwrap()
                .unwrap();
            if let mysql_common::binlog::events::EventData::RowsEvent(rows) =
                event.read_data().unwrap().unwrap()
            {
                let table_map = reader.get_tme(rows.table_id()).unwrap();
                let row = rows.rows(table_map).next().unwrap().unwrap().1.unwrap();
                assert!(row.as_ref(2).is_some());
                assert!(row.as_ref(3).is_some());
                assert!(row.as_ref(4).is_some());
                saw_rows = true;
            }
        }
        assert!(saw_rows);
    }

    #[test]
    fn parses_show_binlog_events_clause_variants() {
        let req = parse_show_binlog_events_clause("SHOW BINLOG EVENTS").unwrap();
        assert_eq!(req.file, "");
        assert_eq!(req.from, 4);
        assert_eq!(req.offset, 0);
        assert!(req.limit.is_none());

        let req = parse_show_binlog_events_clause(
            "SHOW BINLOG EVENTS IN 'mydb-bin.000003' FROM 120 LIMIT 5, 20",
        )
        .unwrap();
        assert_eq!(req.file, "mydb-bin.000003");
        assert_eq!(req.from, 120);
        assert_eq!(req.offset, 5);
        assert_eq!(req.limit, Some(20));

        let req = parse_show_binlog_events_clause("SHOW BINLOG EVENTS LIMIT 10;").unwrap();
        assert_eq!(req.limit, Some(10));

        let err = parse_show_binlog_events_clause("SHOW BINLOG EVENTS FROM abc").unwrap_err();
        assert_eq!(err.code(), 1064);

        let err = parse_show_binlog_events_clause("SHOW BINLOG EVENTS bogus").unwrap_err();
        assert_eq!(err.code(), 1064);
    }

    #[test]
    fn parses_well_formed_register_slave_payload() {
        // 4 bytes server_id, 1 byte host_len, host, 1 byte user_len, user,
        // 1 byte password_len, password, 2 bytes port, 4 bytes repl_rank,
        // 4 bytes master_id.
        let mut payload = Vec::new();
        payload.extend_from_slice(&123u32.to_le_bytes());
        payload.push(9);
        payload.extend_from_slice(b"replica-1");
        payload.push(8);
        payload.extend_from_slice(b"replicat");
        payload.push(0); // password
        payload.extend_from_slice(&3306u16.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&1u32.to_le_bytes());
        let info = parse_register_slave_payload(&payload).expect("parse");
        assert_eq!(info.server_id, 123);
        assert_eq!(info.host, "replica-1");
        assert_eq!(info.user, "replicat");
        assert_eq!(info.port, 3306);
        assert_eq!(info.master_id, 1);
        assert_eq!(info.replication_rank, 0);
    }

    #[test]
    fn rejects_truncated_register_slave_payload() {
        let payload = vec![1, 0, 0, 0, 5];
        let err = parse_register_slave_payload(&payload).unwrap_err();
        assert_eq!(err.code(), 1235);
    }

    #[test]
    fn explicit_crc32_check_rejects_corrupted_event() {
        // We assert mydb's slave verifier (`verify_binlog_event_crc32`)
        // rejects an event whose trailing CRC32 has been flipped. Asserts
        // CRC integrity even though mysql_common itself doesn't validate it.
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        manager
            .append_query_event(
                "test",
                "CREATE TABLE t(id INT)",
                &BinlogQueryContext::default(),
            )
            .unwrap();
        let log = manager.binary_logs().unwrap().remove(0);
        let chunks = manager.events_after(&log.name, 4).unwrap();
        let mut reader = mysql_common::binlog::EventStreamReader::new(
            mysql_common::binlog::consts::BinlogVersion::Version4,
        );
        let mut bad_event: Option<mysql_common::binlog::events::Event> = None;
        for chunk in chunks {
            if chunk.event_type == FORMAT_DESCRIPTION_EVENT {
                reader
                    .read(std::io::Cursor::new(chunk.bytes.as_ref().as_slice()))
                    .unwrap()
                    .unwrap();
            }
            if chunk.event_type == QUERY_EVENT {
                let mut bytes = (*chunk.bytes).clone();
                *bytes.last_mut().unwrap() ^= 0xff;
                let event = reader
                    .read(std::io::Cursor::new(bytes.as_slice()))
                    .unwrap()
                    .unwrap();
                bad_event = Some(event);
                break;
            }
        }
        let bad = bad_event.expect("expected a QUERY_EVENT in the stream");
        assert!(
            !crate::replication::verify_binlog_event_crc32(&bad),
            "slave CRC32 verifier must reject the corrupted event"
        );
    }

    #[test]
    fn live_binlog_cache_eviction_preserves_dump_and_publication_boundaries() {
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path())).unwrap();
        let first = manager.binary_logs().unwrap()[0].name.clone();
        let mut expected = manager.events_after(&first, 4).unwrap();
        let commits: Vec<_> = (1..=1200)
            .map(|txn| stage_test_write(&manager, txn))
            .collect();
        manager.sync_staged_commit(commits.last().unwrap()).unwrap();
        for commit in commits {
            expected.extend(commit.chunks.clone());
            manager.commit_order.finish_wal(commit.order);
            manager.publish_txn_commit(Some(commit)).unwrap();
        }
        {
            let state = manager.state.lock();
            assert!(state.events.len() <= 4096);
            assert!(state.live_event_bytes <= 4 * 1024 * 1024);
            assert!(state.disk_prefixes[&first] > 4);
            assert_eq!(
                state.live_event_bytes,
                state
                    .events
                    .iter()
                    .map(|event| event.bytes.len())
                    .sum::<usize>()
            );
        }
        let staged = stage_test_write(&manager, 1201);
        manager.sync_staged_commit(&staged).unwrap();
        // The cold prefix and hot tail must exclude bytes physically written
        // for a transaction whose WAL/publication turn has not completed.
        let mut actual = Vec::new();
        let mut pos = 4;
        loop {
            let batch = manager.events_after_bounded(&first, pos, 37, 4096).unwrap();
            if batch.is_empty() {
                break;
            }
            pos = batch.last().unwrap().end_pos;
            actual.extend(batch);
        }
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(&expected) {
            assert_eq!(
                (actual.start_pos, actual.end_pos),
                (expected.start_pos, expected.end_pos)
            );
            assert_eq!(actual.bytes, expected.bytes);
        }
        manager.commit_order.finish_wal(staged.order);
        expected.extend(staged.chunks.clone());
        manager.publish_txn_commit(Some(staged)).unwrap();
        manager.checkpoint_manifest().unwrap();
        drop(manager);
        let reopened =
            BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path())).unwrap();
        let actual = reopened.events_after(&first, 4).unwrap();
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            assert_eq!(actual.bytes, expected.bytes);
        }
    }

    #[test]
    fn live_binlog_cache_evicts_a_single_oversized_published_event() {
        let mut state = BinlogState::default();
        state.push_event(BinlogEventChunk {
            file: "binlog.000001".into(),
            start_pos: 4,
            end_pos: 5 * 1024 * 1024 + 4,
            event_type: QUERY_EVENT,
            gtid: None,
            bytes: Arc::new(vec![0; 5 * 1024 * 1024]),
        });
        state.trim_published_events();
        assert!(state.events.is_empty());
        assert!(state.file_first.is_empty());
        assert_eq!(state.live_event_bytes, 0);
        assert_eq!(state.disk_prefixes["binlog.000001"], 5 * 1024 * 1024 + 4);
    }

    #[test]
    fn reopened_binlog_reads_bounded_cold_and_live_events_in_order() {
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path())).unwrap();
        for txn in 1..=3 {
            let commit = stage_test_write(&manager, txn);
            manager.sync_staged_commit(&commit).unwrap();
            manager.commit_order.finish_wal(commit.order);
            manager.publish_txn_commit(Some(commit)).unwrap();
            manager.rotate().unwrap();
        }
        manager.checkpoint_manifest().unwrap();
        let first = manager.binary_logs().unwrap()[0].name.clone();
        let expected = manager.events_after(&first, 4).unwrap();
        drop(manager);
        let manager =
            BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path())).unwrap();
        assert!(
            manager.state.lock().events.is_empty(),
            "restart must not materialize history"
        );
        assert_eq!(manager.state.lock().disk_prefixes.len(), 4);
        let commit = stage_test_write(&manager, 4);
        manager.sync_staged_commit(&commit).unwrap();
        // Physical bytes exist, but publication has not happened yet.
        assert_eq!(
            manager.events_after(&first, 4).unwrap().len(),
            expected.len()
        );
        manager.commit_order.finish_wal(commit.order);
        let live = commit.chunks.clone();
        manager.publish_txn_commit(Some(commit)).unwrap();
        let expected = expected.into_iter().chain(live).collect::<Vec<_>>();
        let mut file = first;
        let mut pos = 4;
        let mut actual = Vec::new();
        loop {
            let batch = manager.events_after_bounded(&file, pos, 2, 128).unwrap();
            if batch.is_empty() {
                break;
            }
            assert!(batch.len() <= 2);
            assert!(batch.len() == 1 || batch.iter().map(|e| e.bytes.len()).sum::<usize>() <= 128);
            let last = batch.last().unwrap();
            file = last.file.clone();
            pos = last.end_pos;
            actual.extend(batch);
        }
        assert_eq!(actual.len(), expected.len());
        for (a, b) in actual.iter().zip(&expected) {
            assert_eq!(
                (&a.file, a.start_pos, a.end_pos, &a.gtid, &a.bytes),
                (&b.file, b.start_pos, b.end_pos, &b.gtid, &b.bytes)
            );
        }
        assert!(manager
            .events_after_bounded(&file, 4, 0, 0)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn reopened_binlog_gtid_lookup_purge_and_reset_include_disk_history() {
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path())).unwrap();
        let mut gtids = Vec::new();
        for txn in 1..=3 {
            let commit = stage_test_write(&manager, txn);
            gtids.push(commit.reference.gtid.clone().unwrap());
            manager.sync_staged_commit(&commit).unwrap();
            manager.commit_order.finish_wal(commit.order);
            manager.publish_txn_commit(Some(commit)).unwrap();
            manager.rotate().unwrap();
        }
        manager.checkpoint_manifest().unwrap();
        let logs = manager.binary_logs().unwrap();
        let expected = manager.position_for_gtid_dump("", 4, &gtids[0]).unwrap();
        drop(manager);
        let manager =
            BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path())).unwrap();
        let actual = manager.position_for_gtid_dump("", 4, &gtids[0]).unwrap();
        assert_eq!(
            (actual.file, actual.position),
            (expected.file, expected.position)
        );
        manager.purge_to(&logs[1].name).unwrap();
        assert_eq!(manager.state.lock().gtid_purged, gtids[0]);
        assert_eq!(manager.state.lock().disk_prefixes.len(), 3);
        assert!(manager.position_for_gtid_dump("", 4, &gtids[1]).is_err());
        manager
            .reset_master(|| test_reset_wal_boundary(&manager))
            .unwrap();
        assert!(manager.state.lock().disk_prefixes.is_empty());
        assert!(!manager.events_after("", 4).unwrap().is_empty());
    }

    #[test]
    fn reopened_binlog_rejects_short_durable_file_and_invalid_read_position() {
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path())).unwrap();
        let pos = manager.current_position().unwrap();
        manager.checkpoint_manifest().unwrap();
        drop(manager);
        let manager =
            BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path())).unwrap();
        assert!(manager.events_after(&pos.file, 5).is_err());
        drop(manager);
        OpenOptions::new()
            .write(true)
            .open(tmp.path().join("binlog").join(pos.file))
            .unwrap()
            .set_len(pos.position - 1)
            .unwrap();
        assert!(BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path())).is_err());
    }

    #[test]
    fn release_audit_sync_zero_refuses_lost_committed_tail() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let tmp = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let mut config = test_config(tmp.path());
        config.sync_binlog = 0;
        let manager = BinlogSourceManager::open(tmp.path().into(), config.clone()).unwrap();
        let commit = stage_test_write(&manager, 1);
        manager.sync_staged_commit(&commit).unwrap();
        let reference = commit.reference.clone();
        manager.commit_order.finish_wal(commit.order);
        manager.publish_txn_commit(Some(commit)).unwrap();
        manager.checkpoint_manifest().unwrap();
        drop(manager);
        OpenOptions::new()
            .write(true)
            .open(tmp.path().join("binlog").join(&reference.file))
            .unwrap()
            .set_len(reference.start_pos)
            .unwrap();
        assert!(
            BinlogSourceManager::open(tmp.path().into(), config).is_err(),
            "lost published binlog transactions must not silently disappear from replication"
        );
    }

    #[test]
    fn reopened_binlog_retention_collects_cold_gtids() {
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path())).unwrap();
        let commit = stage_test_write(&manager, 1);
        let gtid = commit.reference.gtid.clone().unwrap();
        manager.sync_staged_commit(&commit).unwrap();
        manager.commit_order.finish_wal(commit.order);
        manager.publish_txn_commit(Some(commit)).unwrap();
        manager.rotate().unwrap();
        manager.checkpoint_manifest().unwrap();
        drop(manager);
        let mut manager =
            BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path())).unwrap();
        manager.config.retention_bytes = Some(1);
        manager.rotate().unwrap();
        assert_eq!(manager.binary_logs().unwrap().len(), 1);
        assert_eq!(manager.state.lock().gtid_purged, gtid);
        assert!(manager.state.lock().disk_prefixes.is_empty());
    }

    #[test]
    fn manifestless_binlog_recovery_streams_and_truncates_torn_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path())).unwrap();
        let pos = manager.current_position().unwrap();
        let expected = manager.events_after(&pos.file, 4).unwrap();
        drop(manager);
        let path = tmp.path().join("binlog").join(&pos.file);
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[1, 2, 3])
            .unwrap();
        fs::remove_file(tmp.path().join("binlog/manifest.json")).unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path())).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), pos.position);
        assert!(manager.state.lock().events.is_empty());
        let actual = manager.events_after(&pos.file, 4).unwrap();
        assert_eq!(actual.len(), expected.len());
        for (a, b) in actual.iter().zip(expected) {
            assert_eq!(a.bytes, b.bytes);
        }
    }

    #[test]
    fn reopened_binlog_rejects_malformed_wal_committed_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path())).unwrap();
        let commit = stage_test_write(&manager, 1);
        manager.sync_staged_commit(&commit).unwrap();
        let reference = commit.reference.clone();
        drop(manager);
        let path = tmp.path().join("binlog").join(&reference.file);
        let mut file = OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(reference.start_pos + 9)).unwrap();
        file.write_all(&1u32.to_le_bytes()).unwrap();
        let mut log = crate::wal::LogManager::open(tmp.path()).unwrap();
        log.append(
            1,
            crate::wal::WalPayload::CommitTxn {
                commit_scn: 99,
                commit_unix_us: 1,
                binlog: Some(reference),
            },
        )
        .unwrap();
        drop(log);
        assert!(BinlogSourceManager::open(tmp.path().into(), test_config(tmp.path())).is_err());
    }

    #[test]
    fn events_after_index_seeks_directly_to_requested_file() {
        // Build many rotated files and verify events_after returns the right
        // slice starting at the requested file's first event. Smoke test for
        // the `file_first` cache invariant.
        let tmp = tempfile::tempdir().unwrap();
        let manager = Arc::new(
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap(),
        );
        for _ in 0..5 {
            manager.rotate().unwrap();
        }
        let logs = manager.binary_logs().unwrap();
        // Asking for events starting at file[3] must skip files 0,1,2.
        let target = logs[3].name.clone();
        let events = manager.events_after(&target, 0).unwrap();
        assert!(events.iter().all(|ev| {
            let file_idx = logs.iter().position(|l| l.name == ev.file).unwrap();
            file_idx >= 3
        }));
        let target_events = events
            .iter()
            .take_while(|event| event.file == target)
            .collect::<Vec<_>>();
        assert!(target_events.len() >= 2);
        let seek_pos = target_events[target_events.len() / 2].end_pos;
        let sought = manager.events_after(&target, seek_pos).unwrap();
        assert!(sought
            .iter()
            .filter(|event| event.file == target)
            .all(|event| event.end_pos > seek_pos));
        assert_eq!(
            sought.len(),
            events
                .iter()
                .filter(|event| event.file != target || event.end_pos > seek_pos)
                .count()
        );
        // file_first map must contain every file with events.
        let state = manager.state.lock();
        for info in &logs {
            assert!(state.file_first.contains_key(&info.name));
        }
    }

    #[test]
    fn reset_wal_boundary_failure_preserves_existing_logs() {
        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        manager.rotate().unwrap();
        let files = manager.binary_logs().unwrap();
        let before = files
            .iter()
            .map(|file| fs::read(manager.dir.join(&file.name)).unwrap())
            .collect::<Vec<_>>();
        let result = manager.reset_master(|| {
            Err(error(
                crate::error::catalog::ER_ERROR_ON_WRITE,
                "injected WAL barrier failure",
            ))
        });
        assert!(result.is_err());
        for (file, bytes) in files.iter().zip(before) {
            assert_eq!(fs::read(manager.dir.join(&file.name)).unwrap(), bytes);
        }
        assert!(!manager.dir.join("reset-pending").exists());
        assert!(!manager.dir.join("reset-cutoff").exists());
    }

    #[test]
    fn reset_cutoff_covers_buffered_commit_references() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let manager = BinlogSourceManager::open(tmp.path().to_path_buf(), config.clone()).unwrap();
        let mut wal = crate::wal::LogManager::open(tmp.path()).unwrap();
        wal.set_write_buffer_enabled(true).unwrap();
        let persisted = wal.append(0, crate::wal::WalPayload::EndTxn).unwrap();
        wal.flush().unwrap();
        let commit = stage_test_write(&manager, 1);
        manager.sync_staged_commit(&commit).unwrap();
        let reference = commit.reference.clone();
        let commit_lsn = wal
            .append(
                1,
                crate::wal::WalPayload::CommitTxn {
                    commit_scn: 1,
                    commit_unix_us: 1,
                    binlog: Some(reference),
                },
            )
            .unwrap();
        // A filesystem scan sees only the flushed prefix, not allocated WAL
        // records still in the writer's buffer that can survive a later crash.
        let scan = crate::wal::LogManager::scan(tmp.path()).unwrap();
        assert_eq!(scan.records.last().unwrap().lsn, persisted);
        assert!(commit_lsn > persisted);
        manager.commit_order.finish_wal(commit.order);
        manager.publish_txn_commit(Some(commit)).unwrap();
        let wal = Arc::new(crate::wal::LogMutex::new(wal));
        manager
            .reset_master(|| crate::wal::LogManager::sync_allocation_boundary(&wal))
            .unwrap();
        let cutoff = read_reset_marker(&manager.dir.join("reset-cutoff"))
            .unwrap()
            .unwrap();
        assert!(cutoff >= commit_lsn);
        assert_eq!(wal.lock().next_lsn(), cutoff + 1);
        assert!(committed_binlog_refs_from_wal(tmp.path())
            .unwrap()
            .is_empty());
        drop(manager);
        drop(wal);
        let reopened = BinlogSourceManager::open(tmp.path().to_path_buf(), config).unwrap();
        let wal = Arc::new(crate::wal::LogMutex::new(
            crate::wal::LogManager::open(tmp.path()).unwrap(),
        ));
        assert!(wal.lock().next_lsn() > cutoff);
        let next = stage_test_write(&reopened, 2);
        reopened.sync_staged_commit(&next).unwrap();
        let new_lsn = wal
            .lock()
            .append(
                2,
                crate::wal::WalPayload::CommitTxn {
                    commit_scn: 2,
                    commit_unix_us: 2,
                    binlog: Some(next.reference.clone()),
                },
            )
            .unwrap();
        crate::wal::LogManager::sync_durable_to(&wal, new_lsn).unwrap();
        assert!(new_lsn > cutoff);
        assert_eq!(
            committed_binlog_refs_from_wal(tmp.path()).unwrap(),
            vec![next.reference.clone()]
        );
        reopened.commit_order.finish_wal(next.order);
        reopened.publish_txn_commit(Some(next)).unwrap();
    }

    #[test]
    fn round2_interrupted_reset_is_completed_before_wal_reconciliation() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let manager = BinlogSourceManager::open(tmp.path().to_path_buf(), config.clone()).unwrap();
        manager.rotate().unwrap();
        let dir = tmp.path().join("binlog");
        let cutoff = test_reset_wal_boundary(&manager).unwrap();
        write_reset_marker(&dir, "reset-pending", cutoff).unwrap();
        drop(manager);
        let reopened = BinlogSourceManager::open(tmp.path().to_path_buf(), config).unwrap();
        assert_eq!(reopened.binary_logs().unwrap().len(), 1);
        assert_eq!(
            read_reset_marker(&dir.join("reset-cutoff")).unwrap(),
            Some(cutoff)
        );
        assert!(!dir.join("reset-pending").exists());
    }

    #[test]
    fn reset_master_clears_logs_and_restarts_numbering() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = Arc::new(
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap(),
        );
        manager.rotate().unwrap();
        manager.rotate().unwrap();
        assert!(manager.binary_logs().unwrap().len() >= 3);
        manager
            .reset_master(|| test_reset_wal_boundary(&manager))
            .unwrap();
        let logs = manager.binary_logs().unwrap();
        assert_eq!(logs.len(), 1);
        assert!(logs[0].name.ends_with(".000001"));
        // gtid_executed cleared
        let snapshot = manager.state.lock();
        assert!(snapshot.gtid_executed.is_empty());
    }

    #[test]
    fn purge_before_rejects_malformed_datetime() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = Arc::new(
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap(),
        );
        let err = manager.purge_before("not a date").unwrap_err();
        assert_eq!(err.code(), 1525);
        assert!(
            err.message().contains("bad datetime"),
            "got: {}",
            err.message()
        );
    }

    #[test]
    fn purge_before_preserves_active_file() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = Arc::new(
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap(),
        );
        // A future cut-off would otherwise purge every file; the
        // active writer file must still be retained.
        manager.purge_before("2099-12-31 23:59:59").unwrap();
        let logs = manager.binary_logs().unwrap();
        assert!(!logs.is_empty(), "active binlog must survive PURGE BEFORE");
    }

    #[test]
    fn purge_before_with_past_cutoff_keeps_all() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = Arc::new(
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap(),
        );
        // Cut-off far in the past — none of the just-created files
        // are old enough; nothing should be purged.
        let before = manager.binary_logs().unwrap();
        manager.purge_before("1970-01-02 00:00:00").unwrap();
        let after = manager.binary_logs().unwrap();
        assert_eq!(before.len(), after.len());
    }

    #[test]
    fn reset_master_refuses_while_replicas_are_registered() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = Arc::new(
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap(),
        );
        let _guard = manager
            .register_replica(RegisteredReplica {
                server_id: 5,
                host: "h".to_string(),
                port: 3306,
                user: "u".to_string(),
                master_id: 1,
                replication_rank: 0,
                connected_at: SystemTime::now(),
                semi_sync: false,
                acked_file: String::new(),
                acked_pos: 0,
                replica_uuid: String::new(),
                regid: 0,
            })
            .unwrap();
        let err = manager
            .reset_master(|| test_reset_wal_boundary(&manager))
            .unwrap_err();
        assert_eq!(err.code(), 3702);
    }

    #[test]
    fn register_replica_appears_in_show_replicas_and_drops_on_guard_drop() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = Arc::new(
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap(),
        );
        assert!(manager.registered_replicas().is_empty());
        let guard = manager
            .register_replica(RegisteredReplica {
                server_id: 42,
                host: "h".to_string(),
                port: 3306,
                user: "u".to_string(),
                master_id: 1,
                replication_rank: 0,
                connected_at: SystemTime::now(),
                semi_sync: false,
                acked_file: String::new(),
                acked_pos: 0,
                replica_uuid: "3e11fa47-71ca-11e1-9e33-c80aa9429562".to_string(),
                regid: 0,
            })
            .unwrap();
        let listed = manager.registered_replicas();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].server_id, 42);
        // M4: the replica's `@replica_uuid` flows through the registry so
        // SHOW REPLICAS can surface it in the Replica_UUID column.
        assert_eq!(
            listed[0].replica_uuid,
            "3e11fa47-71ca-11e1-9e33-c80aa9429562"
        );
        drop(guard);
        assert!(manager.registered_replicas().is_empty());
    }

    pub(super) fn semi_sync_replica(server_id: u32) -> RegisteredReplica {
        RegisteredReplica {
            server_id,
            host: "h".to_string(),
            port: 3306,
            user: "u".to_string(),
            master_id: 1,
            replication_rank: 0,
            connected_at: SystemTime::now(),
            semi_sync: false,
            acked_file: String::new(),
            acked_pos: 0,
            replica_uuid: String::new(),
            regid: 0,
        }
    }

    #[test]
    fn binlog_pos_ordering_by_file_then_position() {
        // Same file: compare by byte position.
        assert!(binlog_pos_ge(
            ("binlog.000001", 200),
            ("binlog.000001", 100)
        ));
        assert!(!binlog_pos_ge(
            ("binlog.000001", 50),
            ("binlog.000001", 100)
        ));
        // Later file wins regardless of position (incl. digit-width rollover).
        assert!(binlog_pos_ge(
            ("binlog.000002", 4),
            ("binlog.000001", 9_999)
        ));
        assert!(binlog_pos_ge(
            ("binlog.1000000", 4),
            ("binlog.999999", 9_999)
        ));
        // Never-acked (empty file) is below any real position.
        assert!(!binlog_pos_ge(("", 0), ("binlog.000001", 4)));
        // Equal positions are "at or beyond".
        assert!(binlog_pos_ge(
            ("binlog.000001", 100),
            ("binlog.000001", 100)
        ));
    }

    #[test]
    fn parse_semi_sync_ack_roundtrips_position_and_file() {
        let mut payload = vec![0xEFu8];
        payload.extend_from_slice(&1234u64.to_le_bytes());
        payload.extend_from_slice(b"binlog.000007");
        let (pos, file) = parse_semi_sync_ack(&payload).unwrap();
        assert_eq!(pos, 1234);
        assert_eq!(file, "binlog.000007");
        // Wrong magic / truncated → None (ignored as non-ack traffic).
        assert!(parse_semi_sync_ack(&[0x00, 1, 2, 3]).is_none());
        assert!(parse_semi_sync_ack(&[0xEF, 1, 2]).is_none());
    }

    #[test]
    fn semi_sync_wait_inactive_when_disabled() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = Arc::new(
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap(),
        );
        // Default: semi-sync disabled → no wait, no counters move.
        assert_eq!(
            manager.semi_sync_wait("binlog.000001", 100),
            SemiSyncOutcome::Inactive
        );
        assert_eq!(manager.semi_sync.yes_tx.load(Ordering::Relaxed), 0);
        assert_eq!(manager.semi_sync.no_tx.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn semi_sync_wait_degrades_immediately_with_no_replica() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = Arc::new(
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap(),
        );
        manager.semi_sync.enabled.store(true, Ordering::Relaxed);
        manager
            .semi_sync
            .wait_no_replica
            .store(false, Ordering::Relaxed);
        // No semi-sync replica connected + wait_no_replica=OFF → degrade async.
        assert_eq!(
            manager.semi_sync_wait("binlog.000001", 100),
            SemiSyncOutcome::NoReplicaDegrade
        );
        assert_eq!(manager.semi_sync.no_tx.load(Ordering::Relaxed), 1);
        assert_eq!(manager.semi_sync.yes_tx.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn semi_sync_wait_fast_path_when_already_acked() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = Arc::new(
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap(),
        );
        manager.semi_sync.enabled.store(true, Ordering::Relaxed);
        let _guard = manager.register_replica(semi_sync_replica(9)).unwrap();
        manager.semi_sync_mark_replica(9);
        // Replica already acked past the target → immediate Acked, yes_tx++.
        manager.record_semi_sync_ack(9, "binlog.000001", 500);
        assert_eq!(
            manager.semi_sync_wait("binlog.000001", 100),
            SemiSyncOutcome::Acked
        );
        assert_eq!(manager.semi_sync.yes_tx.load(Ordering::Relaxed), 1);
        assert_eq!(manager.semi_sync_client_count(), 1);
    }

    #[test]
    fn semi_sync_wait_times_out_when_replica_never_acks() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = Arc::new(
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap(),
        );
        manager.semi_sync.enabled.store(true, Ordering::Relaxed);
        manager.semi_sync.timeout_ms.store(50, Ordering::Relaxed);
        let _guard = manager.register_replica(semi_sync_replica(9)).unwrap();
        manager.semi_sync_mark_replica(9);
        // Replica connected but acks below the target → wait out timeout, degrade.
        manager.record_semi_sync_ack(9, "binlog.000001", 10);
        let start = std::time::Instant::now();
        assert_eq!(
            manager.semi_sync_wait("binlog.000001", 100),
            SemiSyncOutcome::TimedOut
        );
        assert!(start.elapsed() >= Duration::from_millis(45));
        assert_eq!(manager.semi_sync.no_tx.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn semi_sync_wait_released_by_late_ack_from_another_thread() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = Arc::new(
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap(),
        );
        manager.semi_sync.enabled.store(true, Ordering::Relaxed);
        manager.semi_sync.timeout_ms.store(5_000, Ordering::Relaxed);
        let _guard = manager.register_replica(semi_sync_replica(9)).unwrap();
        manager.semi_sync_mark_replica(9);
        let acker = Arc::clone(&manager);
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            acker.record_semi_sync_ack(9, "binlog.000001", 100);
        });
        // Blocks until the background ack wakes the condvar (well under timeout).
        let start = std::time::Instant::now();
        assert_eq!(
            manager.semi_sync_wait("binlog.000001", 100),
            SemiSyncOutcome::Acked
        );
        assert!(start.elapsed() < Duration::from_millis(2_000));
        handle.join().unwrap();
        assert_eq!(manager.semi_sync.yes_tx.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn semi_sync_status_pairs_report_source_and_master_aliases() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = Arc::new(
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap(),
        );
        manager.semi_sync.enabled.store(true, Ordering::Relaxed);
        let _guard = manager.register_replica(semi_sync_replica(9)).unwrap();
        manager.semi_sync_mark_replica(9);
        manager.semi_sync.yes_tx.store(3, Ordering::Relaxed);
        let pairs = manager.semi_sync_status_pairs();
        let get = |name: &str| {
            pairs
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.clone())
                .unwrap()
        };
        // One quorum-worth of clients + enabled → operating synchronously.
        assert_eq!(get("Rpl_semi_sync_source_status"), "ON");
        assert_eq!(get("Rpl_semi_sync_master_status"), "ON");
        assert_eq!(get("Rpl_semi_sync_source_clients"), "1");
        assert_eq!(get("Rpl_semi_sync_master_clients"), "1");
        assert_eq!(get("Rpl_semi_sync_source_yes_tx"), "3");
        assert_eq!(get("Rpl_semi_sync_master_yes_tx"), "3");
    }

    #[test]
    fn semi_sync_source_sysvars_roundtrip_through_snapshot() {
        // Runtime setters update the display snapshot read by SHOW VARIABLES.
        set_semi_sync_source_enabled(true).unwrap();
        set_semi_sync_source_timeout(2500).unwrap();
        set_semi_sync_source_wait_for_replica_count(2).unwrap();
        set_semi_sync_source_wait_no_replica(false).unwrap();
        let snap = variables_snapshot();
        let get = |name: &str| {
            snap.iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| v.clone())
                .unwrap()
        };
        assert_eq!(get("rpl_semi_sync_source_enabled"), "ON");
        assert_eq!(get("rpl_semi_sync_master_enabled"), "ON");
        assert_eq!(get("rpl_semi_sync_source_timeout"), "2500");
        assert_eq!(get("rpl_semi_sync_source_wait_for_replica_count"), "2");
        assert_eq!(get("rpl_semi_sync_master_wait_for_slave_count"), "2");
        assert_eq!(get("rpl_semi_sync_source_wait_no_replica"), "OFF");
        // Restore defaults so other tests reading the global snapshot are stable.
        set_semi_sync_source_enabled(false).unwrap();
        set_semi_sync_source_timeout(10_000).unwrap();
        set_semi_sync_source_wait_for_replica_count(1).unwrap();
        set_semi_sync_source_wait_no_replica(true).unwrap();
    }

    #[test]
    fn sql_mode_bits_roundtrip_handles_default_and_custom() {
        // Default mode used by MySQL 8.0 — bits roundtrip back to canonical
        // comma-separated form.
        let default = sql_mode_bits_to_str(DEFAULT_SQL_MODE_BITS);
        assert_eq!(sql_mode_str_to_bits(&default), DEFAULT_SQL_MODE_BITS);
        let custom = "ANSI_QUOTES,STRICT_ALL_TABLES,TIME_TRUNCATE_FRACTIONAL";
        let bits = sql_mode_str_to_bits(custom);
        let custom_back = sql_mode_bits_to_str(bits);
        // Order-independent check: each token round-trips.
        for tok in custom.split(',') {
            assert!(
                custom_back.split(',').any(|t| t == tok),
                "lost token {tok} in {custom_back}"
            );
        }
        // Unknown tokens are silently dropped, not garbage-bitted.
        assert_eq!(
            sql_mode_str_to_bits("UNKNOWN_FAKE_MODE,STRICT_TRANS_TABLES"),
            0x0020_0000
        );
    }

    #[test]
    fn sql_mode_fingerprint_memo_tracks_every_string_change() {
        // The per-thread one-entry memo must never leak the previous mode.
        let strict = "STRICT_TRANS_TABLES,NO_ZERO_DATE";
        let ansi = "ansi_quotes, PIPES_AS_CONCAT";
        let expected_strict = 0x0020_0000 | 0x0100_0000;
        let expected_ansi = 0x0000_0004 | 0x0000_0002;
        for _ in 0..3 {
            assert_eq!(sql_mode_str_to_bits(strict), expected_strict);
            assert_eq!(sql_mode_str_to_bits(strict), expected_strict);
            assert_eq!(sql_mode_str_to_bits(ansi), expected_ansi);
            assert_eq!(sql_mode_str_to_bits(""), 0);
            assert_eq!(sql_mode_str_to_bits(""), 0);
        }
    }

    #[test]
    fn query_event_status_vars_roundtrip_via_mysql_common() {
        use mysql_common::binlog::events::StatusVarVal;
        let mut ctx = BinlogQueryContext::default();
        ctx.thread_id = 4242;
        ctx.sql_mode = sql_mode_str_to_bits("STRICT_ALL_TABLES,NO_ZERO_DATE");
        ctx.time_zone = "+08:00".to_string();
        ctx.foreign_key_checks = false;
        ctx.unique_checks = false;
        ctx.lc_time_names = 0;
        ctx.client_charset_id = crate::charset::collation_id("utf8mb4_bin");
        ctx.connection_collation_id = crate::charset::collation_id("utf8mb4_general_ci");
        ctx.server_collation_id = crate::charset::collation_id("utf8mb4_0900_ai_ci");

        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        manager
            .append_query_event("test", "CREATE TABLE t(id INT)", &ctx)
            .unwrap();
        let log = manager.binary_logs().unwrap().remove(0);
        let events = manager.events_after(&log.name, 4).unwrap();
        let mut reader = mysql_common::binlog::EventStreamReader::new(
            mysql_common::binlog::consts::BinlogVersion::Version4,
        );
        let mut got_query = false;
        for chunk in events {
            let event = reader
                .read(std::io::Cursor::new(chunk.bytes.as_ref().as_slice()))
                .unwrap()
                .unwrap();
            if let mysql_common::binlog::events::EventData::QueryEvent(qe) =
                event.read_data().unwrap().unwrap()
            {
                assert_eq!(qe.thread_id(), 4242);
                assert_eq!(qe.query(), "CREATE TABLE t(id INT)");
                assert_eq!(qe.schema(), "test");
                let mut saw_flags = false;
                let mut saw_sql_mode = false;
                let mut saw_charset = false;
                let mut saw_tz = false;
                let mut saw_explicit = false;
                let mut saw_lc_time = false;
                for v in qe.status_vars().iter() {
                    match v.get_value() {
                        Ok(StatusVarVal::Flags2(f)) => {
                            assert!(f.0 & 0x0400_0000 != 0, "FK off bit");
                            assert!(f.0 & 0x0800_0000 != 0, "unique off bit");
                            saw_flags = true;
                        }
                        Ok(StatusVarVal::SqlMode(m)) => {
                            assert_eq!(m.0, ctx.sql_mode);
                            saw_sql_mode = true;
                        }
                        Ok(StatusVarVal::Charset {
                            charset_client,
                            collation_connection,
                            collation_server,
                        }) => {
                            assert_eq!(charset_client, ctx.client_charset_id);
                            assert_eq!(collation_connection, ctx.connection_collation_id);
                            assert_eq!(collation_server, ctx.server_collation_id);
                            saw_charset = true;
                        }
                        Ok(StatusVarVal::TimeZone(b)) => {
                            assert_eq!(b.as_bytes(), b"+08:00");
                            saw_tz = true;
                        }
                        Ok(StatusVarVal::ExplicitDefaultsForTimestamp(b)) => {
                            assert!(b);
                            saw_explicit = true;
                        }
                        Ok(StatusVarVal::LcTimeNames(v)) => {
                            assert_eq!(v, 0);
                            saw_lc_time = true;
                        }
                        _ => {}
                    }
                }
                assert!(
                    saw_flags
                        && saw_sql_mode
                        && saw_charset
                        && saw_tz
                        && saw_explicit
                        && saw_lc_time
                );
                got_query = true;
            }
        }
        assert!(got_query);
    }

    #[test]
    fn encodes_temporal_fsp_roundtrip_via_mysql_common() {
        // Build a table with TIME(3), DATETIME(6), TIMESTAMP(3) and verify
        // round-trip through mysql_common decode preserves fractional seconds.
        let mut table = test_table();
        table.columns.push(ColumnSchema {
            not_null_from_scn: 0,
            name: "t3".to_string(),
            data_type: DataTypeKind::Time { fsp: 3 },
            nullable: false,
            unsigned: false,
            invisible: false,
            zerofill: false,
            auto_increment: false,
            default: None,
            read_backfill_default: None,
            default_expr: None,
            on_update_expr: None,
            column_id: 3,
            state: ColumnState::Public,
            physical_pos: 2,
            logical_pos: 2,
            ordinal_position: 3,
            charset: None,
            collation: None,
            comment: String::new(),
            generated_expr: None,
            generated_stored: false,
            generated_column_names: Vec::new(),
            generated_expr_ast: None,
            generated_program: None,
        });
        table.columns.push(ColumnSchema {
            not_null_from_scn: 0,
            name: "dt6".to_string(),
            data_type: DataTypeKind::DateTime { fsp: 6 },
            nullable: false,
            unsigned: false,
            invisible: false,
            zerofill: false,
            auto_increment: false,
            default: None,
            read_backfill_default: None,
            default_expr: None,
            on_update_expr: None,
            column_id: 4,
            state: ColumnState::Public,
            physical_pos: 3,
            logical_pos: 3,
            ordinal_position: 4,
            charset: None,
            collation: None,
            comment: String::new(),
            generated_expr: None,
            generated_stored: false,
            generated_column_names: Vec::new(),
            generated_expr_ast: None,
            generated_program: None,
        });
        table.columns.push(ColumnSchema {
            not_null_from_scn: 0,
            name: "ts3".to_string(),
            data_type: DataTypeKind::Timestamp { fsp: 3 },
            nullable: false,
            unsigned: false,
            invisible: false,
            zerofill: false,
            auto_increment: false,
            default: None,
            read_backfill_default: None,
            default_expr: None,
            on_update_expr: None,
            column_id: 5,
            state: ColumnState::Public,
            physical_pos: 4,
            logical_pos: 4,
            ordinal_position: 5,
            charset: None,
            collation: None,
            comment: String::new(),
            generated_expr: None,
            generated_stored: false,
            generated_column_names: Vec::new(),
            generated_expr_ast: None,
            generated_program: None,
        });
        table.next_column_id = 6;
        table.build_column_index();

        // TIME 12:34:56.789
        let time_micros: i64 = (12i64 * 3600 + 34 * 60 + 56) * 1_000_000 + 789_000;
        // DATETIME 2024-03-15 14:25:36.123456 → micros since unix epoch
        let dt = chrono::NaiveDate::from_ymd_opt(2024, 3, 15)
            .unwrap()
            .and_hms_micro_opt(14, 25, 36, 123_456)
            .unwrap();
        let dt_micros = dt.and_utc().timestamp_micros();
        // TIMESTAMP 2024-03-15 14:25:36.789 → micros since unix epoch.
        // A separate test verifies that post-2038 mydb values are rejected
        // explicitly at this MySQL-compatible signed-32-bit wire boundary.
        let ts = chrono::NaiveDate::from_ymd_opt(2024, 3, 15)
            .unwrap()
            .and_hms_micro_opt(14, 25, 36, 789_000)
            .unwrap();
        let ts_micros = ts.and_utc().timestamp_micros();

        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        manager
            .push_op(
                17,
                BinlogTxnOp::Write {
                    table: binlog_table_from_schema(&table).unwrap(),
                    rows: vec![vec![
                        Value::Int(1),
                        Value::String("a".to_string()),
                        Value::Time(time_micros),
                        Value::DateTime(dt_micros),
                        Value::Timestamp(ts_micros),
                    ]],
                },
            )
            .unwrap();
        manager.commit_txn(17, 99).unwrap();

        let log = manager.binary_logs().unwrap().remove(0);
        let events = manager.events_after(&log.name, 4).unwrap();
        let mut reader = mysql_common::binlog::EventStreamReader::new(
            mysql_common::binlog::consts::BinlogVersion::Version4,
        );
        let mut saw_rows = false;
        for chunk in events {
            let event = reader
                .read(std::io::Cursor::new(chunk.bytes.as_ref().as_slice()))
                .unwrap()
                .unwrap();
            if let mysql_common::binlog::events::EventData::RowsEvent(rows) =
                event.read_data().unwrap().unwrap()
            {
                let table_map = reader.get_tme(rows.table_id()).unwrap();
                let row = rows.rows(table_map).next().unwrap().unwrap().1.unwrap();
                fn unwrap_value<'a>(
                    v: &mysql_common::binlog::value::BinlogValue<'a>,
                ) -> mysql_common::value::Value {
                    match v {
                        mysql_common::binlog::value::BinlogValue::Value(v) => v.clone(),
                        other => panic!("expected Value(Value), got {:?}", other),
                    }
                }
                let bv_time = unwrap_value(row.as_ref(2).expect("time"));
                let bv_dt = unwrap_value(row.as_ref(3).expect("datetime"));
                let bv_ts = unwrap_value(row.as_ref(4).expect("timestamp"));
                // mysql crate decoded TIME(3): expect 12h 34m 56s 789000us
                match bv_time {
                    mysql_common::value::Value::Time(neg, days, h, m, s, us) => {
                        assert!(!neg);
                        assert_eq!(days * 24 + h as u32, 12);
                        assert_eq!(m, 34);
                        assert_eq!(s, 56);
                        assert_eq!(us, 789_000);
                    }
                    other => panic!("expected Time, got {:?}", other),
                }
                match bv_dt {
                    mysql_common::value::Value::Date(y, mo, d, h, mi, s, us) => {
                        assert_eq!((y, mo, d), (2024, 3, 15));
                        assert_eq!((h, mi, s), (14, 25, 36));
                        assert_eq!(us, 123_456);
                    }
                    other => panic!("expected Date(datetime), got {:?}", other),
                }
                // mysql_common decodes TIMESTAMP2 as `Bytes("<secs>.<usecs>")`.
                match bv_ts {
                    mysql_common::value::Value::Bytes(bytes) => {
                        let s = std::str::from_utf8(&bytes).expect("utf8 timestamp");
                        let expected_secs = ts_micros / 1_000_000;
                        let expected_us = (ts_micros % 1_000_000) as u32;
                        let expected = if expected_us == 0 {
                            format!("{}", expected_secs)
                        } else {
                            format!("{}.{:06}", expected_secs, expected_us)
                        };
                        assert_eq!(s, expected);
                    }
                    other => panic!("expected Bytes(timestamp), got {:?}", other),
                }
                saw_rows = true;
            }
        }
        assert!(saw_rows);
    }

    #[test]
    fn short_varchar_row_prefix_roundtrips_with_trailing_column() {
        // Regression: a VARCHAR whose declared byte length is < 256 must use a
        // 1-byte row-image length prefix. Emitting a fixed 2-byte prefix
        // inserts a stray byte that desynchronises every following column, so
        // a vanilla MySQL replica rejects the event (ER_SLAVE_CORRUPT_EVENT).
        // The bug is invisible when the VARCHAR is the last column (the extra
        // byte is tolerated) and when it is VARCHAR(>=64) (metadata >= 256, so
        // a 2-byte prefix is correct) — hence a SHORT varchar FOLLOWED by
        // another column. Decoding the trailing INT via mysql_common proves
        // the columns stay aligned.
        let mut table = test_table();
        // col2: VARCHAR(30) → metadata 30*4 = 120 < 256 → 1-byte prefix.
        table.columns.push(ColumnSchema {
            not_null_from_scn: 0,
            name: "sv".to_string(),
            data_type: DataTypeKind::VarChar { len: Some(30) },
            nullable: true,
            unsigned: false,
            invisible: false,
            zerofill: false,
            auto_increment: false,
            default: None,
            read_backfill_default: None,
            default_expr: None,
            on_update_expr: None,
            column_id: 3,
            state: ColumnState::Public,
            physical_pos: 2,
            logical_pos: 2,
            ordinal_position: 3,
            charset: None,
            collation: None,
            comment: String::new(),
            generated_expr: None,
            generated_stored: false,
            generated_column_names: Vec::new(),
            generated_expr_ast: None,
            generated_program: None,
        });
        // col3: trailing INT — misaligned if the VARCHAR prefix is wrong.
        table.columns.push(ColumnSchema {
            not_null_from_scn: 0,
            name: "n".to_string(),
            data_type: DataTypeKind::Int,
            nullable: false,
            unsigned: false,
            invisible: false,
            zerofill: false,
            auto_increment: false,
            default: None,
            read_backfill_default: None,
            default_expr: None,
            on_update_expr: None,
            column_id: 4,
            state: ColumnState::Public,
            physical_pos: 3,
            logical_pos: 3,
            ordinal_position: 4,
            charset: None,
            collation: None,
            comment: String::new(),
            generated_expr: None,
            generated_stored: false,
            generated_column_names: Vec::new(),
            generated_expr_ast: None,
            generated_program: None,
        });
        table.next_column_id = 5;
        table.build_column_index();

        let tmp = tempfile::tempdir().unwrap();
        let manager =
            BinlogSourceManager::open(tmp.path().to_path_buf(), test_config(tmp.path())).unwrap();
        manager
            .push_op(
                21,
                BinlogTxnOp::Write {
                    table: binlog_table_from_schema(&table).unwrap(),
                    rows: vec![vec![
                        Value::Int(1),
                        Value::String("name".to_string()),
                        Value::String("hi".to_string()),
                        Value::Int(77),
                    ]],
                },
            )
            .unwrap();
        manager.commit_txn(21, 99).unwrap();

        let log = manager.binary_logs().unwrap().remove(0);
        let events = manager.events_after(&log.name, 4).unwrap();
        let mut reader = mysql_common::binlog::EventStreamReader::new(
            mysql_common::binlog::consts::BinlogVersion::Version4,
        );
        let mut saw_rows = false;
        for chunk in events {
            let event = reader
                .read(std::io::Cursor::new(chunk.bytes.as_ref().as_slice()))
                .unwrap()
                .unwrap();
            if let mysql_common::binlog::events::EventData::RowsEvent(rows) =
                event.read_data().unwrap().unwrap()
            {
                let table_map = reader.get_tme(rows.table_id()).unwrap();
                let row = rows.rows(table_map).next().unwrap().unwrap().1.unwrap();
                let unwrap = |v: &mysql_common::binlog::value::BinlogValue<'_>| match v {
                    mysql_common::binlog::value::BinlogValue::Value(v) => v.clone(),
                    other => panic!("expected Value, got {:?}", other),
                };
                // Short VARCHAR decodes as its bytes …
                match unwrap(row.as_ref(2).expect("sv")) {
                    mysql_common::value::Value::Bytes(b) => assert_eq!(b, b"hi"),
                    other => panic!("expected Bytes(sv), got {:?}", other),
                }
                // … and the trailing INT stays aligned at 77.
                match unwrap(row.as_ref(3).expect("n")) {
                    mysql_common::value::Value::Int(n) => assert_eq!(n, 77),
                    other => panic!("expected Int(n)=77, got {:?}", other),
                }
                saw_rows = true;
            }
        }
        assert!(saw_rows);
    }

    #[test]
    fn boundary_sysvar_validators_accept_supported_reject_others() {
        // Supported values are idempotent no-ops.
        assert!(validate_binlog_format("ROW").is_ok());
        assert!(validate_binlog_format("row").is_ok());
        assert!(validate_binlog_row_image("FULL").is_ok());
        assert!(validate_binlog_row_metadata("FULL").is_ok());
        assert!(validate_binlog_row_value_options("").is_ok());
        assert!(validate_binlog_row_value_options("   ").is_ok());
        assert!(validate_binlog_row_value_options("PARTIAL_JSON").is_ok());
        assert!(validate_binlog_transaction_compression("OFF").is_ok());
        assert!(validate_binlog_transaction_compression("0").is_ok());

        // Everything else is a hard, MySQL-aligned error (errno 1231) — never a
        // silent no-op or a bare read-only rejection.
        for (err, label) in [
            (
                validate_binlog_format("STATEMENT"),
                "binlog_format=STATEMENT",
            ),
            (validate_binlog_format("MIXED"), "binlog_format=MIXED"),
            (validate_binlog_row_image("MINIMAL"), "row_image=MINIMAL"),
            (validate_binlog_row_image("NOBLOB"), "row_image=NOBLOB"),
            (
                validate_binlog_row_metadata("MINIMAL"),
                "row_metadata=MINIMAL",
            ),
            (
                validate_binlog_row_value_options("UNKNOWN"),
                "row_value_options=UNKNOWN",
            ),
            (
                validate_binlog_transaction_compression("ON"),
                "txn_compression=ON",
            ),
        ] {
            let err = err.expect_err(label);
            assert_eq!(err.code(), 1231, "{label} should report errno 1231");
        }
    }

    #[test]
    fn set_sync_binlog_updates_snapshot_and_manager_mirror() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = test_config(tmp.path());
        config.sync_binlog = 1;
        init_global(tmp.path().to_path_buf(), config).unwrap();

        // Startup default visible via the snapshot pipeline.
        let read = |name: &str| {
            variables_snapshot()
                .into_iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| v)
                .unwrap()
        };
        assert_eq!(read("sync_binlog"), "1");

        // SET GLOBAL sync_binlog = 0 → snapshot + manager hot-path mirror both move.
        set_sync_binlog(0).unwrap();
        assert_eq!(read("sync_binlog"), "0");
        assert_eq!(manager().unwrap().sync_binlog.load(Ordering::Relaxed), 0);

        set_sync_binlog(100).unwrap();
        assert_eq!(read("sync_binlog"), "100");
        assert_eq!(manager().unwrap().sync_binlog.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn set_log_replica_updates_updates_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = test_config(tmp.path());
        config.log_replica_updates = true;
        init_global(tmp.path().to_path_buf(), config).unwrap();

        let read = || {
            variables_snapshot()
                .into_iter()
                .find(|(n, _)| *n == "log_replica_updates")
                .map(|(_, v)| v)
                .unwrap()
        };
        assert_eq!(read(), "ON");
        set_log_replica_updates(false).unwrap();
        assert_eq!(read(), "OFF");
        assert!(!config_snapshot().log_replica_updates);
    }
}
