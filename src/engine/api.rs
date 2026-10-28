use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use crate::mysql::errors as mysql_errors;
use crate::parser::{ExplainFormat, Statement, TempTableOnCommit};
use crate::storage::{
    Database, IndexKey, IndexSchema, IsolationLevel, MdlRequest, SnapshotGuard, TableSchema,
    DEFAULT_DATABASE,
};
use crate::types::{Column, QueryError, Value};

use super::auth::{
    AccountKey, AuthOutcome, AuthRequest, AuthorizationContext, RequiredPrivilege, ResourceLimits,
};
use super::cache::CacheStats;
use super::cancel::QueryCancelToken;
use super::session_registry::SessionCancelHandle;

/// Process-wide default for the EXPLAIN format. Initialised to
/// `Traditional` so out-of-the-box behaviour still matches the MySQL
/// table-style output, and updated by `SET GLOBAL explain_format = ...`.
/// New sessions read this at construction time so a global override
/// takes effect for subsequent connections without needing a restart.
static GLOBAL_EXPLAIN_FORMAT: AtomicU8 = AtomicU8::new(EXPLAIN_FORMAT_TRADITIONAL);
static GLOBAL_TRANSACTION_ISOLATION: AtomicU8 = AtomicU8::new(TRANSACTION_ISOLATION_READ_COMMITTED);
/// `@@global.transaction_read_only` — flipped by `SET GLOBAL transaction_read_only = ...`
/// and inherited by new sessions. Default off (read-write).
static GLOBAL_TRANSACTION_READ_ONLY: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
/// `@@global.sql_log_bin` — global default for sql_log_bin inherited by
/// new sessions; default ON.
static GLOBAL_SQL_LOG_BIN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);
/// `@@global.innodb_lock_wait_timeout` — row-lock wait timeout (seconds)
/// inherited by new sessions. MySQL default 50.
static GLOBAL_INNODB_LOCK_WAIT_TIMEOUT: AtomicU64 = AtomicU64::new(50);
/// `@@global.lock_wait_timeout` — metadata-lock (MDL) wait timeout (seconds)
/// MySQL display default is 31536000 (1 year). Zero means no SQL override
/// has been configured, so the executor retains the startup lock-wait default.
static GLOBAL_LOCK_WAIT_TIMEOUT: AtomicU64 = AtomicU64::new(0);

const EXPLAIN_FORMAT_TRADITIONAL: u8 = 0;
const EXPLAIN_FORMAT_TREE: u8 = 1;
const EXPLAIN_FORMAT_JSON: u8 = 2;
const TRANSACTION_ISOLATION_READ_UNCOMMITTED: u8 = 0;
const TRANSACTION_ISOLATION_READ_COMMITTED: u8 = 1;
const TRANSACTION_ISOLATION_REPEATABLE_READ: u8 = 2;
const TRANSACTION_ISOLATION_SERIALIZABLE: u8 = 3;

fn encode_explain_format(format: ExplainFormat) -> u8 {
    match format {
        ExplainFormat::Traditional => EXPLAIN_FORMAT_TRADITIONAL,
        ExplainFormat::Tree => EXPLAIN_FORMAT_TREE,
        ExplainFormat::Json => EXPLAIN_FORMAT_JSON,
    }
}

fn decode_explain_format(value: u8) -> ExplainFormat {
    match value {
        EXPLAIN_FORMAT_TREE => ExplainFormat::Tree,
        EXPLAIN_FORMAT_JSON => ExplainFormat::Json,
        _ => ExplainFormat::Traditional,
    }
}

pub fn global_explain_format() -> ExplainFormat {
    decode_explain_format(GLOBAL_EXPLAIN_FORMAT.load(Ordering::Relaxed))
}

pub fn set_global_explain_format(format: ExplainFormat) {
    GLOBAL_EXPLAIN_FORMAT.store(encode_explain_format(format), Ordering::Relaxed);
}

pub fn global_innodb_lock_wait_timeout() -> u64 {
    GLOBAL_INNODB_LOCK_WAIT_TIMEOUT.load(Ordering::Relaxed)
}

pub fn set_global_innodb_lock_wait_timeout(secs: u64) {
    GLOBAL_INNODB_LOCK_WAIT_TIMEOUT.store(secs, Ordering::Relaxed);
}

pub fn global_lock_wait_timeout() -> u64 {
    configured_global_lock_wait_timeout().unwrap_or(31_536_000)
}

pub(crate) fn configured_global_lock_wait_timeout() -> Option<u64> {
    let seconds = GLOBAL_LOCK_WAIT_TIMEOUT.load(Ordering::Relaxed);
    (seconds != 0).then_some(seconds)
}

pub fn set_global_lock_wait_timeout(secs: u64) {
    GLOBAL_LOCK_WAIT_TIMEOUT.store(secs, Ordering::Relaxed);
}

pub fn parse_explain_format_name(value: &str) -> Option<ExplainFormat> {
    match value.trim().to_ascii_uppercase().as_str() {
        "TRADITIONAL" => Some(ExplainFormat::Traditional),
        "TREE" => Some(ExplainFormat::Tree),
        "JSON" => Some(ExplainFormat::Json),
        _ => None,
    }
}

pub fn explain_format_name(format: ExplainFormat) -> &'static str {
    match format {
        ExplainFormat::Traditional => "TRADITIONAL",
        ExplainFormat::Tree => "TREE",
        ExplainFormat::Json => "JSON",
    }
}

fn encode_transaction_isolation(isolation: IsolationLevel) -> u8 {
    match isolation {
        IsolationLevel::ReadUncommitted => TRANSACTION_ISOLATION_READ_UNCOMMITTED,
        IsolationLevel::ReadCommitted => TRANSACTION_ISOLATION_READ_COMMITTED,
        IsolationLevel::RepeatableRead => TRANSACTION_ISOLATION_REPEATABLE_READ,
        IsolationLevel::Serializable => TRANSACTION_ISOLATION_SERIALIZABLE,
    }
}

fn decode_transaction_isolation(value: u8) -> IsolationLevel {
    match value {
        TRANSACTION_ISOLATION_READ_UNCOMMITTED => IsolationLevel::ReadUncommitted,
        TRANSACTION_ISOLATION_READ_COMMITTED => IsolationLevel::ReadCommitted,
        TRANSACTION_ISOLATION_SERIALIZABLE => IsolationLevel::Serializable,
        _ => IsolationLevel::RepeatableRead,
    }
}

pub fn global_transaction_isolation() -> IsolationLevel {
    decode_transaction_isolation(GLOBAL_TRANSACTION_ISOLATION.load(Ordering::Relaxed))
}

pub fn set_global_transaction_isolation(isolation: IsolationLevel) {
    GLOBAL_TRANSACTION_ISOLATION.store(encode_transaction_isolation(isolation), Ordering::Relaxed);
}

pub fn global_transaction_read_only() -> bool {
    GLOBAL_TRANSACTION_READ_ONLY.load(Ordering::Relaxed)
}

pub fn set_global_transaction_read_only(v: bool) {
    GLOBAL_TRANSACTION_READ_ONLY.store(v, Ordering::Relaxed);
}

pub fn global_sql_log_bin() -> bool {
    GLOBAL_SQL_LOG_BIN.load(Ordering::Relaxed)
}

pub fn set_global_sql_log_bin(v: bool) {
    GLOBAL_SQL_LOG_BIN.store(v, Ordering::Relaxed);
}

#[derive(Debug)]
pub enum QueryResponse {
    Ok,
    OkAffected { affected_rows: u64 },
    ResultSet(ResultSet),
    SharedResultSet(SharedResultSet),
    StreamingResultSet(StreamingResultSet),
    MultiResult(Vec<QueryResponse>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResultSet {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SharedResultSet {
    pub columns: Arc<[Column]>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug)]
pub struct StreamingResultSet {
    pub columns: Vec<Column>,
    pub found_rows: Option<u64>,
    pub cursor: Box<dyn ResultCursor>,
}

pub type SqlWarning = crate::error::SqlCondition;
pub use crate::error::conditions::SqlConditions as SqlWarnings;
#[derive(Debug, Default)]
struct StatementWarningState {
    warnings: parking_lot::Mutex<SqlWarnings>,
    once: parking_lot::Mutex<std::collections::HashSet<(u64, u16, String)>>,
}

/// Warning storage and statement-wide constant-diagnostic identity travel
/// together across workers; draining conditions must not reset the identity.
#[derive(Clone, Debug, Default)]
pub(crate) struct StatementWarningSink(std::sync::Arc<StatementWarningState>);

impl std::ops::Deref for StatementWarningSink {
    type Target = parking_lot::Mutex<SqlWarnings>;
    fn deref(&self) -> &Self::Target {
        &self.0.warnings
    }
}

pub(crate) fn shared_statement_warning_should_publish(
    context: u64,
    code: u16,
    message: &str,
) -> Option<bool> {
    STATEMENT_WARNING_SINK.with(|slot| {
        slot.borrow().as_ref().map(|sink| {
            sink.0
                .once
                .lock()
                .insert((context, code, message.to_string()))
        })
    })
}

thread_local! {
    static STATEMENT_WARNINGS: std::cell::RefCell<SqlWarnings> = const { std::cell::RefCell::new(SqlWarnings::new()) };
    /// Query workers cannot publish diagnostics into the connection thread's
    /// TLS directly.  The immutable execution context installs this shared
    /// sink in every producer; the result cursor drains it at the batch
    /// boundary before returning rows to the connection thread.
    static STATEMENT_WARNING_SINK: std::cell::RefCell<Option<StatementWarningSink>> = const { std::cell::RefCell::new(None) };
    static STATEMENT_WARNING_BATCH: std::cell::RefCell<Option<SqlWarnings>> = const { std::cell::RefCell::new(None) };
    static STATEMENT_INSERT_ID: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static STATEMENT_OK_INSERT_ID: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    static STATEMENT_INSERT_ID_FORCED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Carries the matched-row count for the most recent UPDATE/INSERT to
    /// the protocol layer. `None` = "no separate matched count available;
    /// use affected_rows" — keeps the protocol layer's fast path on
    /// statements that don't distinguish (DELETE, single-row INSERT).
    static STATEMENT_MATCHED_ROWS: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    /// Logical row identities selected by DML access paths while an EXPLAIN
    /// ANALYZE DML scope is active. `None` keeps ordinary DML allocation-free;
    /// `Some(n)` is incremented at the candidate-producing index/point paths.
    static STATEMENT_DML_CANDIDATE_ROWS: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    static STATEMENT_DML_CANDIDATE_REPORTED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Free-form `info` text trailer for the next OK packet — the
    /// `Rows matched: N Changed: M Warnings: W` (UPDATE) / `Records: N
    /// Duplicates: D Warnings: W` (multi-row INSERT) MySQL diagnostic
    /// strings. Empty = no trailer.
    static STATEMENT_INFO: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
    static SHARED_RESULT_METADATA_ALLOWED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Running duplicate-row count for the in-flight INSERT (rows skipped by
    /// IGNORE, matched by ON DUPLICATE KEY UPDATE, or replaced by REPLACE).
    /// Accumulated across INSERT…SELECT chunks; consumed once by the
    /// top-level insert dispatch to build the `Records: N Duplicates: D
    /// Warnings: W` OK-packet info trailer.
    static STATEMENT_INSERT_DUPLICATES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static STATEMENT_ODKU_STATS: std::cell::Cell<StatementOdkuStats> = const { std::cell::Cell::new(StatementOdkuStats::new()) };
}

pub(crate) fn with_shared_result_metadata<R>(f: impl FnOnce() -> R) -> R {
    crate::util::scoped_cell::with_value(&SHARED_RESULT_METADATA_ALLOWED, true, f)
}

pub(crate) fn shared_result_metadata_allowed() -> bool {
    SHARED_RESULT_METADATA_ALLOWED.with(std::cell::Cell::get)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct StatementOdkuStats {
    pub rows_input: u64,
    pub batch_attempts: u64,
    pub batched_rows: u64,
    pub conflict_rows: u64,
    pub legacy_rows: u64,
    pub race_rollbacks: u64,
}

impl StatementOdkuStats {
    const fn new() -> Self {
        Self {
            rows_input: 0,
            batch_attempts: 0,
            batched_rows: 0,
            conflict_rows: 0,
            legacy_rows: 0,
            race_rollbacks: 0,
        }
    }
}

/// Temporal-relevant `sql_mode` bits, cached on the session (see
/// `SessionContext::temporal_sql_mode_bits`) and snapshotted into the
/// expression-layer session view so storage-side coercion can read them
/// without re-splitting the mode string per value.
pub const TEMPORAL_MODE_STRICT: u16 = 1;
pub const TEMPORAL_MODE_NO_ZERO_DATE: u16 = 1 << 1;
pub const TEMPORAL_MODE_NO_ZERO_IN_DATE: u16 = 1 << 2;
pub const TEMPORAL_MODE_ALLOW_INVALID_DATES: u16 = 1 << 3;
pub const TEMPORAL_MODE_TIME_TRUNCATE_FRACTIONAL: u16 = 1 << 4;
/// Set whenever the session has a non-empty `sql_mode`. mydb's default
/// (empty) sql_mode keeps the historical strict posture for temporal
/// writes — invalid dates are errors; MySQL's non-strict
/// warning-plus-zero-date behaviour applies only when the user
/// explicitly configured a non-strict sql_mode.
pub const TEMPORAL_MODE_EXPLICIT: u16 = 1 << 5;
/// Numeric `sql_mode` bits, carried in the same cached byte (the name
/// `temporal_sql_mode_bits` predates them). `ERROR_FOR_DIVISION_BY_ZERO`
/// is part of MySQL's default sql_mode, so with mydb's default (empty)
/// sql_mode the division-by-zero warning fires unless the user explicitly
/// configures a mode without this flag — same "empty = MySQL default
/// posture" convention as the temporal strict bit.
pub const SQL_MODE_ERROR_FOR_DIVISION_BY_ZERO: u16 = 1 << 6;
pub const SQL_MODE_NO_UNSIGNED_SUBTRACTION: u16 = 1 << 7;
/// `NO_AUTO_VALUE_ON_ZERO`: an explicit `0` in an AUTO_INCREMENT column is
/// stored verbatim instead of triggering a fresh sequence value (only NULL
/// triggers it). Carried in the same cached mode word.
pub const SQL_MODE_NO_AUTO_VALUE_ON_ZERO: u16 = 1 << 8;
/// `ANSI_QUOTES`: `"..."` becomes an identifier delimiter (like backticks)
/// rather than a string literal. Parser-time bit — consumed by
/// `MydbDialect` via the parser thread-local so the SQL text tokenizes
/// differently from the moment sqlparser sees it.
pub const SQL_MODE_ANSI_QUOTES: u16 = 1 << 9;
/// `NO_BACKSLASH_ESCAPES`: backslash inside string literals is a literal
/// backslash instead of an escape prefix. Parser-time bit — same threading
/// path as ANSI_QUOTES.
pub const SQL_MODE_NO_BACKSLASH_ESCAPES: u16 = 1 << 10;
/// `STRICT_ALL_TABLES` also makes conversion failures fatal for
/// non-transactional engines. `STRICT_TRANS_TABLES` deliberately does not.
pub const SQL_MODE_STRICT_ALL_TABLES: u16 = 1 << 11;
/// `REAL_AS_FLOAT`: REAL resolves to FLOAT instead of MySQL's default DOUBLE.
pub const SQL_MODE_REAL_AS_FLOAT: u16 = 1 << 12;
/// `PIPES_AS_CONCAT`: `||` is string concatenation, parsed with MySQL's
/// high OR_OR precedence. Without this bit `||` is the low-precedence OR
/// synonym. This is parser state and must therefore participate in parsed-
/// statement and plan-cache identities.
pub const SQL_MODE_PIPES_AS_CONCAT: u16 = 1 << 13;
/// `HIGH_NOT_PRECEDENCE`: keyword `NOT` binds like the `!` prefix operator
/// instead of its default position between BETWEEN and AND.
pub const SQL_MODE_HIGH_NOT_PRECEDENCE: u16 = 1 << 14;
/// `IGNORE_SPACE`: native function names may be separated from their opening
/// parenthesis by whitespace/comments. This is parser state and therefore is
/// part of prepared-statement and plan-cache identity.
pub const SQL_MODE_IGNORE_SPACE: u16 = 1 << 15;

pub fn parse_temporal_sql_mode_bits(sql_mode: &str) -> u16 {
    // Called several times per statement with the session's (rarely
    // changing) sql_mode. Remember the last parse per thread; a byte compare
    // of the short mode string is much cheaper than re-tokenizing it.
    thread_local! {
        static LAST: std::cell::RefCell<(String, u16)> =
            const { std::cell::RefCell::new((String::new(), 0)) };
    }
    if let Some(bits) = LAST.with(|last| {
        let last = last.borrow();
        (last.0 == sql_mode && !sql_mode.is_empty()).then_some(last.1)
    }) {
        return bits;
    }
    let bits = parse_temporal_sql_mode_bits_uncached(sql_mode);
    LAST.with(|last| {
        let mut last = last.borrow_mut();
        last.0.clear();
        last.0.push_str(sql_mode);
        last.1 = bits;
    });
    bits
}

fn parse_temporal_sql_mode_bits_uncached(sql_mode: &str) -> u16 {
    let mut bits = 0u16;
    if !sql_mode.trim().is_empty() {
        bits |= TEMPORAL_MODE_EXPLICIT;
    }
    for mode in sql_mode.split(',') {
        let mode = mode.trim();
        if mode.eq_ignore_ascii_case("STRICT_TRANS_TABLES") {
            bits |= TEMPORAL_MODE_STRICT;
        } else if mode.eq_ignore_ascii_case("STRICT_ALL_TABLES")
            || mode.eq_ignore_ascii_case("TRADITIONAL")
        {
            bits |= TEMPORAL_MODE_STRICT | SQL_MODE_STRICT_ALL_TABLES;
        } else if mode.eq_ignore_ascii_case("NO_ZERO_DATE") {
            bits |= TEMPORAL_MODE_NO_ZERO_DATE;
        } else if mode.eq_ignore_ascii_case("NO_ZERO_IN_DATE") {
            bits |= TEMPORAL_MODE_NO_ZERO_IN_DATE;
        } else if mode.eq_ignore_ascii_case("ALLOW_INVALID_DATES") {
            bits |= TEMPORAL_MODE_ALLOW_INVALID_DATES;
        } else if mode.eq_ignore_ascii_case("TIME_TRUNCATE_FRACTIONAL") {
            bits |= TEMPORAL_MODE_TIME_TRUNCATE_FRACTIONAL;
        } else if mode.eq_ignore_ascii_case("ERROR_FOR_DIVISION_BY_ZERO") {
            bits |= SQL_MODE_ERROR_FOR_DIVISION_BY_ZERO;
        } else if mode.eq_ignore_ascii_case("NO_UNSIGNED_SUBTRACTION") {
            bits |= SQL_MODE_NO_UNSIGNED_SUBTRACTION;
        } else if mode.eq_ignore_ascii_case("NO_AUTO_VALUE_ON_ZERO") {
            bits |= SQL_MODE_NO_AUTO_VALUE_ON_ZERO;
        } else if mode.eq_ignore_ascii_case("ANSI_QUOTES") {
            bits |= SQL_MODE_ANSI_QUOTES;
        } else if mode.eq_ignore_ascii_case("NO_BACKSLASH_ESCAPES") {
            bits |= SQL_MODE_NO_BACKSLASH_ESCAPES;
        } else if mode.eq_ignore_ascii_case("REAL_AS_FLOAT") {
            bits |= SQL_MODE_REAL_AS_FLOAT;
        } else if mode.eq_ignore_ascii_case("PIPES_AS_CONCAT") {
            bits |= SQL_MODE_PIPES_AS_CONCAT;
        } else if mode.eq_ignore_ascii_case("HIGH_NOT_PRECEDENCE") {
            bits |= SQL_MODE_HIGH_NOT_PRECEDENCE;
        } else if mode.eq_ignore_ascii_case("IGNORE_SPACE") {
            bits |= SQL_MODE_IGNORE_SPACE;
        } else if mode.eq_ignore_ascii_case("ANSI") {
            // MySQL's ANSI umbrella implies ANSI_QUOTES + REAL_AS_FLOAT +
            // IGNORE_SPACE + ONLY_FULL_GROUP_BY + PIPES_AS_CONCAT.
            bits |= SQL_MODE_ANSI_QUOTES
                | SQL_MODE_REAL_AS_FLOAT
                | SQL_MODE_PIPES_AS_CONCAT
                | SQL_MODE_IGNORE_SPACE;
        }
    }
    // MySQL's TRADITIONAL umbrella implies NO_ZERO_DATE / NO_ZERO_IN_DATE /
    // ERROR_FOR_DIVISION_BY_ZERO.
    if sql_mode
        .split(',')
        .any(|m| m.trim().eq_ignore_ascii_case("TRADITIONAL"))
    {
        bits |= TEMPORAL_MODE_NO_ZERO_DATE
            | TEMPORAL_MODE_NO_ZERO_IN_DATE
            | SQL_MODE_ERROR_FOR_DIVISION_BY_ZERO;
    }
    bits
}

pub(crate) fn clear_statement_warnings() {
    STATEMENT_WARNINGS.with(|warnings| warnings.borrow_mut().clear());
    if let Some(sink) = current_statement_warning_sink() {
        sink.lock().clear();
        sink.0.once.lock().clear();
    }
    crate::engine::expr::clear_expression_warning_dedup();
}

/// Retain diagnostics matching `keep` across both the coordinator-local
/// condition area and the shared worker sink. This is used only at semantic
/// statement boundaries where MySQL suppresses a source-expression warning
/// after a surrounding DML sink has consumed the converted value.
/// Lengths of the local condition area and the shared worker sink, for a
/// later [`truncate_statement_warnings`] that discards a speculative attempt.
pub(crate) fn statement_warning_marks() -> (usize, usize) {
    (
        STATEMENT_WARNINGS.with(|warnings| warnings.borrow().len()),
        current_statement_warning_sink()
            .map(|sink| sink.lock().len())
            .unwrap_or(0),
    )
}

pub(crate) fn truncate_statement_warnings((local, shared): (usize, usize)) {
    fn truncate(warnings: &mut SqlWarnings, len: usize) {
        if warnings.len() > len {
            drop(warnings.split_off(len));
        }
    }
    STATEMENT_WARNINGS.with(|warnings| truncate(&mut warnings.borrow_mut(), local));
    if let Some(sink) = current_statement_warning_sink() {
        truncate(&mut sink.lock(), shared);
    }
}

pub(crate) fn retain_statement_warnings(mut keep: impl FnMut(&SqlWarning) -> bool) {
    STATEMENT_WARNINGS.with(|warnings| warnings.borrow_mut().retain(&mut keep));
    if let Some(sink) = current_statement_warning_sink() {
        sink.lock().retain(keep);
    }
}

// -- Per-statement NOW() freeze (temporal-review-2026-07 B-24) --
//
// MySQL 8.0 guarantees NOW() / CURRENT_TIMESTAMP / CURDATE() / CURTIME()
// / UNIX_TIMESTAMP() return the same value for every call within one
// statement — mydb used to re-read `chrono::Utc::now()` on each call,
// producing microsecond drift. We cache the first read in a
// thread-local and invalidate at every statement boundary (same hook
// as clear_statement_warnings). SYSDATE() bypasses the freeze, per
// MySQL semantics.
thread_local! {
    static STMT_NOW_MICROS: std::cell::Cell<Option<i64>> = const { std::cell::Cell::new(None) };
}

pub(crate) fn clear_statement_now_freeze() {
    STMT_NOW_MICROS.with(|c| c.set(None));
}

/// Read the statement-frozen UTC now, computing and caching on first
/// call. Callers pass a closure returning the microsecond value they
/// would compute if not frozen — the closure only runs on the first
/// call inside the statement.
pub(crate) fn stmt_frozen_utc_now_micros(compute: impl FnOnce() -> i64) -> i64 {
    STMT_NOW_MICROS.with(|c| {
        if let Some(v) = c.get() {
            return v;
        }
        let v = compute();
        c.set(Some(v));
        v
    })
}

pub(crate) fn push_statement_warning(
    level: crate::error::ConditionLevel,
    spec: crate::error::ErrorSpec,
    message: impl Into<String>,
) {
    let message = message.into();
    if !crate::engine::expr::expression_warning_should_publish(spec.code().get(), &message) {
        return;
    }
    push_sql_warning(SqlWarning::known(level, spec, message));
}

pub(crate) fn push_statement_condition(
    level: crate::error::ConditionLevel,
    error: &crate::types::QueryError,
) {
    push_sql_warning(SqlWarning::from_error(level, error));
}

/// Literal diagnostics have the same routing and suppression rules as owned
/// messages, but bulk producers do not allocate identical text per condition.
pub(crate) fn push_static_statement_warning(
    level: crate::error::ConditionLevel,
    spec: crate::error::ErrorSpec,
    message: &'static str,
) {
    if crate::engine::expr::expression_warning_should_publish(spec.code().get(), message) {
        push_sql_warning(SqlWarning::known_static(level, spec, message));
    }
}

pub(crate) fn push_sql_warning(warning: SqlWarning) {
    let warning = STATEMENT_WARNING_BATCH.with(|slot| {
        let mut batch = slot.borrow_mut();
        if let Some(batch) = batch.as_mut() {
            batch.push(warning);
            if batch.len() >= 256 {
                publish_warning_batch(batch);
            }
            None
        } else {
            Some(warning)
        }
    });
    if let Some(warning) = warning {
        STATEMENT_WARNING_SINK.with(|slot| {
            if let Some(sink) = slot.borrow().as_ref() {
                sink.lock().push(warning);
            } else {
                push_sql_warning_local(warning);
            }
        });
    }
}

fn publish_warning_batch(batch: &mut SqlWarnings) {
    if batch.is_empty() {
        return;
    }
    STATEMENT_WARNING_SINK.with(|slot| {
        if let Some(sink) = slot.borrow().as_ref() {
            sink.lock().append(batch);
        } else {
            STATEMENT_WARNINGS.with(|warnings| warnings.borrow_mut().append(batch));
        }
    });
}

fn flush_statement_warning_batch() {
    STATEMENT_WARNING_BATCH.with(|slot| {
        if let Some(batch) = slot.borrow_mut().as_mut() {
            publish_warning_batch(batch);
        }
    });
}

#[cfg(test)]
mod warning_batch_tests {
    use super::*;

    #[test]
    fn draining_bulk_warnings_transfers_storage_and_preserves_local_prefix() {
        clear_statement_warnings();
        let scope = StatementWarningSinkScope::enter();
        for _ in 0..513 {
            push_static_statement_warning(
                crate::error::ConditionLevel::Warning,
                crate::error::catalog::ER_WARN_TOO_MANY_RECORDS,
                "extra field",
            );
        }
        let original_storage = scope.sink.lock().clone();
        scope.drain_to_local();
        STATEMENT_WARNINGS.with(|warnings| {
            let warnings = warnings.borrow();
            assert_eq!(*warnings, original_storage);
            assert_eq!(warnings.len(), 513);
        });
        emit(99);
        scope.drain_to_local();
        let warnings = take_statement_warnings();
        assert_eq!(warnings.len(), 514);
        assert!(warnings
            .iter()
            .take(513)
            .all(|warning| warning.message() == "extra field"));
        assert_eq!(warnings[513].message(), "99");
        assert!(scope.sink.lock().is_empty());
    }

    #[test]
    fn constant_diagnostic_identity_is_shared_and_survives_condition_draining() {
        let sink = StatementWarningSink::default();
        let emit_constant = |sink: StatementWarningSink| {
            with_statement_warning_sink(sink, || {
                crate::engine::expr::with_expression_warning_dedup(123, || {
                    push_statement_warning(
                        crate::error::ConditionLevel::Warning,
                        crate::error::catalog::ER_WRONG_VALUE,
                        "Incorrect TIME value: 'zz'",
                    );
                });
            });
        };
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let sink = sink.clone();
                scope.spawn(move || emit_constant(sink));
            }
        });
        assert_eq!(sink.lock().len(), 1);
        let _drained = std::mem::take(&mut *sink.lock());
        std::thread::scope(|scope| {
            let sink = sink.clone();
            scope.spawn(move || emit_constant(sink));
        });
        assert!(sink.lock().is_empty());
        with_statement_warning_sink(sink.clone(), clear_statement_warnings);
        emit_constant(sink.clone());
        assert_eq!(sink.lock().len(), 1);
    }

    fn emit(value: usize) {
        push_statement_warning(
            crate::error::ConditionLevel::Warning,
            crate::error::catalog::ER_WARN_TOO_MANY_RECORDS,
            value.to_string(),
        );
    }

    #[test]
    fn batched_warning_workers_keep_every_condition_and_flush_partial_batches() {
        let sink: StatementWarningSink = Default::default();
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let sink = sink.clone();
                scope.spawn(move || {
                    with_batched_statement_warning_sink(sink, || {
                        for value in 0..1031 {
                            emit(worker * 1031 + value);
                        }
                    });
                });
            }
        });
        let mut messages: Vec<_> = sink
            .lock()
            .iter()
            .map(|warning| warning.message().parse::<usize>().unwrap())
            .collect();
        messages.sort_unstable();
        assert_eq!(messages, (0..8 * 1031).collect::<Vec<_>>());
    }

    #[test]
    fn batched_warning_inspection_nesting_and_unwind_restore_routing() {
        let outer: StatementWarningSink = Default::default();
        let inner: StatementWarningSink = Default::default();
        with_batched_statement_warning_sink(outer.clone(), || {
            emit(1);
            assert_eq!(statement_warning_count(), 1);
            with_batched_statement_warning_sink(inner.clone(), || emit(2));
            emit(3);
            assert_eq!(statement_warnings_snapshot().len(), 2);
        });
        assert_eq!(outer.lock().len(), 2);
        assert_eq!(inner.lock().len(), 1);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_batched_statement_warning_sink(inner.clone(), || {
                emit(4);
                panic!("test worker unwind");
            });
        }));
        assert!(result.is_err());
        assert_eq!(inner.lock().len(), 2);
        assert!(current_statement_warning_sink().is_none());
    }
}

/// Bulk producers publish bounded batches instead of contending on the shared
/// diagnostics mutex for every row. Keep every condition and flush even on an
/// error or unwind; consumers inspect diagnostics after joining the workers.
pub(crate) fn with_batched_statement_warning_sink<R>(
    sink: StatementWarningSink,
    f: impl FnOnce() -> R,
) -> R {
    with_statement_warning_sink(sink, || {
        struct Restore(Option<SqlWarnings>);
        impl Drop for Restore {
            fn drop(&mut self) {
                flush_statement_warning_batch();
                STATEMENT_WARNING_BATCH.with(|slot| *slot.borrow_mut() = self.0.take());
            }
        }
        let previous =
            STATEMENT_WARNING_BATCH.with(|slot| slot.borrow_mut().replace(SqlWarnings::new()));
        let _restore = Restore(previous);
        f()
    })
}

pub(crate) fn push_sql_warning_local(warning: SqlWarning) {
    STATEMENT_WARNINGS.with(|warnings| warnings.borrow_mut().push(warning));
}

pub(crate) fn current_statement_warning_sink() -> Option<StatementWarningSink> {
    flush_statement_warning_batch();
    STATEMENT_WARNING_SINK.with(|slot| slot.borrow().clone())
}

pub(crate) fn with_statement_warning_sink<R>(
    sink: StatementWarningSink,
    f: impl FnOnce() -> R,
) -> R {
    flush_statement_warning_batch();
    struct Restore(Option<StatementWarningSink>);
    impl Drop for Restore {
        fn drop(&mut self) {
            flush_statement_warning_batch();
            STATEMENT_WARNING_SINK.with(|slot| {
                *slot.borrow_mut() = self.0.take();
            });
        }
    }
    let previous = STATEMENT_WARNING_SINK.with(|slot| slot.borrow_mut().replace(sink));
    let _restore = Restore(previous);
    f()
}

/// Root statement scope shared by every nested execution context. This is
/// deliberately a guard rather than a closure so the dispatcher can leave it
/// installed until synchronous diagnostics are finalized. Deferred result
/// cursors own a separate sink so this scope cannot drain unfinished warnings.
pub(crate) struct StatementWarningSinkScope {
    previous: Option<StatementWarningSink>,
    sink: StatementWarningSink,
}

impl StatementWarningSinkScope {
    pub(crate) fn enter() -> Self {
        let sink = current_statement_warning_sink().unwrap_or_default();
        let previous = STATEMENT_WARNING_SINK.with(|slot| slot.borrow_mut().replace(sink.clone()));
        Self { previous, sink }
    }

    pub(crate) fn drain_to_local(&self) {
        flush_statement_warning_batch();
        let mut pending = std::mem::take(&mut *self.sink.lock());
        STATEMENT_WARNINGS.with(|warnings| {
            let mut warnings = warnings.borrow_mut();
            if warnings.is_empty() {
                // Transfer the allocation as well as the conditions. Copying
                // a large bulk statement's diagnostics one element at a time
                // doubles live storage and serializes statement completion.
                *warnings = pending;
            } else {
                warnings.append(&mut pending);
            }
        });
    }
}

impl Drop for StatementWarningSinkScope {
    fn drop(&mut self) {
        STATEMENT_WARNING_SINK.with(|slot| {
            *slot.borrow_mut() = self.previous.take();
        });
    }
}

pub(crate) fn take_statement_warnings() -> SqlWarnings {
    // Reset any per-statement "warn once" flags so the same connection's
    // next statement gets a fresh chance to emit its diagnostics.
    JSON_MERGE_DEPRECATION_WARNED.with(|c| c.set(false));
    let mut warnings =
        STATEMENT_WARNINGS.with(|warnings| std::mem::take(&mut *warnings.borrow_mut()));
    if let Some(sink) = current_statement_warning_sink() {
        warnings.append(&mut std::mem::take(&mut *sink.lock()));
    }
    warnings
}

/// Transfer diagnostics without expanding their repeated conditions.
pub(crate) fn append_statement_warnings(mut warnings: SqlWarnings) {
    if let Some(sink) = current_statement_warning_sink() {
        sink.lock().append(&mut warnings);
    } else {
        STATEMENT_WARNINGS.with(|local| local.borrow_mut().append(&mut warnings));
    }
}

thread_local! {
    static JSON_MERGE_DEPRECATION_WARNED: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

/// M2/P3-3: emit the JSON_MERGE 1287 deprecation warning at most once
/// per statement. `JSON_MERGE` is a per-row expression call, so raising
/// the warning on every invocation would surface millions of copies for
/// `SELECT JSON_MERGE(...) FROM huge_table` — MySQL emits it exactly
/// once, and the client's `SHOW WARNINGS` view assumes at most one
/// entry per unique deprecation.
pub(crate) fn warn_json_merge_deprecation_once() {
    JSON_MERGE_DEPRECATION_WARNED.with(|c| {
        if c.get() {
            return;
        }
        c.set(true);
        push_statement_warning(
            crate::error::ConditionLevel::Warning,
             crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,
            "'JSON_MERGE' is deprecated and will be removed in a future release. Please use JSON_MERGE_PRESERVE/JSON_MERGE_PATCH instead",
        );
    });
}

/// Non-consuming count of warnings accumulated for the in-flight
/// statement. Used by LOAD DATA to fill the `Warnings: W` field of its
/// `Records: N Deleted: M Skipped: K Warnings: W` info trailer while the
/// warnings themselves are still pending drain into the session.
pub(crate) fn statement_warning_count() -> usize {
    STATEMENT_WARNINGS.with(|warnings| warnings.borrow().len())
        + current_statement_warning_sink()
            .map(|sink| sink.lock().len())
            .unwrap_or(0)
}

/// Snapshot in-flight diagnostics without draining them. Conversion
/// materialization uses this when warning provenance matters (for example an
/// oversized exact literal is a non-fatal 1292 warning, while a malformed
/// string converted to DECIMAL is a fatal 1292 under strict CTAS).
pub(crate) fn statement_warnings_snapshot() -> SqlWarnings {
    let mut warnings = STATEMENT_WARNINGS.with(|warnings| warnings.borrow().clone());
    if let Some(sink) = current_statement_warning_sink() {
        warnings.append(&mut sink.lock().clone());
    }
    warnings
}

/// Bound diagnostics of one code emitted after `start`, retaining every
/// earlier condition and every condition of other codes. Blocking operators
/// use this after applying an output LIMIT when their child necessarily
/// finalized more aggregate groups than the client can observe.
pub(crate) fn cap_statement_warnings_since(start: usize, code: u16, max_count: usize) {
    let cap = |warnings: &mut SqlWarnings| {
        let mut kept = 0usize;
        let mut index = 0usize;
        warnings.retain(|warning| {
            let before_checkpoint = index < start;
            index += 1;
            if before_checkpoint || warning.code() != code {
                return true;
            }
            if kept < max_count {
                kept += 1;
                true
            } else {
                false
            }
        });
    };
    if let Some(sink) = current_statement_warning_sink() {
        cap(&mut sink.lock());
    } else {
        STATEMENT_WARNINGS.with(|warnings| cap(&mut warnings.borrow_mut()));
    }
}

pub(crate) fn clear_statement_insert_id() {
    STATEMENT_INSERT_ID.with(|id| id.set(0));
    STATEMENT_OK_INSERT_ID.with(|id| id.set(None));
    STATEMENT_INSERT_ID_FORCED.with(|forced| forced.set(false));
}

pub(crate) fn set_statement_insert_id(value: u64) {
    if value != 0 {
        let installed = STATEMENT_INSERT_ID.with(|id| {
            if id.get() == 0 {
                id.set(value);
                true
            } else {
                false
            }
        });
        if installed {
            STATEMENT_OK_INSERT_ID.with(|id| id.set(None));
        }
    }
}

pub(crate) fn force_statement_insert_id(value: u64) {
    STATEMENT_INSERT_ID.with(|id| id.set(value));
    STATEMENT_OK_INSERT_ID.with(|id| id.set(None));
    STATEMENT_INSERT_ID_FORCED.with(|forced| forced.set(true));
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct StatementInsertIdSnapshot(u64, Option<u64>, bool);

pub(crate) fn statement_insert_id_is_set() -> bool {
    STATEMENT_INSERT_ID.with(|id| id.get() != 0)
        || STATEMENT_INSERT_ID_FORCED.with(|forced| forced.get())
}

pub(crate) fn statement_insert_id_value() -> u64 {
    STATEMENT_INSERT_ID.with(|id| id.get())
}

pub(crate) fn snapshot_statement_insert_id() -> StatementInsertIdSnapshot {
    StatementInsertIdSnapshot(
        STATEMENT_INSERT_ID.with(|id| id.get()),
        STATEMENT_OK_INSERT_ID.with(|id| id.get()),
        STATEMENT_INSERT_ID_FORCED.with(|forced| forced.get()),
    )
}

pub(crate) fn restore_statement_insert_id(snapshot: StatementInsertIdSnapshot) {
    STATEMENT_INSERT_ID.with(|id| id.set(snapshot.0));
    STATEMENT_OK_INSERT_ID.with(|id| id.set(snapshot.1));
    STATEMENT_INSERT_ID_FORCED.with(|forced| forced.set(snapshot.2));
}

/// A pure-update ODKU reports its row identity in OK without publishing a
/// new session LAST_INSERT_ID. Explicit LAST_INSERT_ID(expr) takes priority.
pub(crate) fn set_statement_ok_insert_id(value: u64) {
    if value != 0
        && STATEMENT_INSERT_ID.with(|id| id.get() == 0)
        && !STATEMENT_INSERT_ID_FORCED.with(|forced| forced.get())
    {
        STATEMENT_OK_INSERT_ID.with(|id| id.set(Some(value)));
    }
}

pub(crate) fn take_statement_ok_insert_id() -> Option<u64> {
    STATEMENT_OK_INSERT_ID.with(|id| id.take())
}

pub(crate) fn take_statement_insert_id() -> u64 {
    STATEMENT_INSERT_ID.with(|id| id.replace(0))
}

pub(crate) fn clear_statement_matched_rows() {
    STATEMENT_MATCHED_ROWS.with(|cell| cell.set(None));
}

pub(crate) fn set_statement_matched_rows(matched: u64) {
    STATEMENT_MATCHED_ROWS.with(|cell| cell.set(Some(matched)));
}

pub(crate) fn take_statement_matched_rows() -> Option<u64> {
    STATEMENT_MATCHED_ROWS.with(|cell| cell.replace(None))
}

pub(crate) fn statement_matched_rows() -> Option<u64> {
    STATEMENT_MATCHED_ROWS.with(std::cell::Cell::get)
}

pub(crate) fn begin_statement_dml_candidate_count() {
    STATEMENT_DML_CANDIDATE_ROWS.with(|cell| cell.set(Some(0)));
    STATEMENT_DML_CANDIDATE_REPORTED.with(|cell| cell.set(false));
}

pub(crate) fn add_statement_dml_candidate_rows(rows: u64) {
    STATEMENT_DML_CANDIDATE_ROWS.with(|cell| {
        if let Some(current) = cell.get() {
            cell.set(Some(current.saturating_add(rows)));
            STATEMENT_DML_CANDIDATE_REPORTED.with(|reported| reported.set(true));
        }
    });
}

pub(crate) fn take_statement_dml_candidate_rows() -> Option<u64> {
    let rows = STATEMENT_DML_CANDIDATE_ROWS.with(|cell| cell.replace(None));
    let reported = STATEMENT_DML_CANDIDATE_REPORTED.with(|cell| cell.replace(false));
    reported.then_some(rows.unwrap_or(0))
}

pub(crate) fn clear_statement_info() {
    STATEMENT_INFO.with(|cell| cell.borrow_mut().clear());
}

pub(crate) fn clear_insert_duplicates() {
    STATEMENT_INSERT_DUPLICATES.with(|cell| cell.set(0));
}

pub(crate) fn clear_statement_odku_stats() {
    STATEMENT_ODKU_STATS.with(|cell| cell.set(StatementOdkuStats::new()));
}

pub(crate) fn record_statement_odku_input(n: u64) {
    STATEMENT_ODKU_STATS.with(|cell| {
        let mut stats = cell.get();
        stats.rows_input = stats.rows_input.saturating_add(n);
        cell.set(stats);
    });
}

pub(crate) fn record_statement_odku_batch_attempt() {
    STATEMENT_ODKU_STATS.with(|cell| {
        let mut stats = cell.get();
        stats.batch_attempts = stats.batch_attempts.saturating_add(1);
        cell.set(stats);
    });
}

pub(crate) fn record_statement_odku_batched_rows(n: u64) {
    STATEMENT_ODKU_STATS.with(|cell| {
        let mut stats = cell.get();
        stats.batched_rows = stats.batched_rows.saturating_add(n);
        cell.set(stats);
    });
}

pub(crate) fn record_statement_odku_conflict_row() {
    STATEMENT_ODKU_STATS.with(|cell| {
        let mut stats = cell.get();
        stats.conflict_rows = stats.conflict_rows.saturating_add(1);
        cell.set(stats);
    });
}

pub(crate) fn record_statement_odku_legacy_rows(n: u64) {
    STATEMENT_ODKU_STATS.with(|cell| {
        let mut stats = cell.get();
        stats.legacy_rows = stats.legacy_rows.saturating_add(n);
        cell.set(stats);
    });
}

pub(crate) fn record_statement_odku_race_rollback() {
    STATEMENT_ODKU_STATS.with(|cell| {
        let mut stats = cell.get();
        stats.race_rollbacks = stats.race_rollbacks.saturating_add(1);
        cell.set(stats);
    });
}

pub(crate) fn take_statement_odku_stats() -> StatementOdkuStats {
    STATEMENT_ODKU_STATS.with(|cell| cell.replace(StatementOdkuStats::new()))
}

pub(crate) fn add_insert_duplicates(n: u64) {
    STATEMENT_INSERT_DUPLICATES.with(|cell| cell.set(cell.get() + n));
}

pub(crate) fn take_insert_duplicates() -> u64 {
    STATEMENT_INSERT_DUPLICATES.with(|cell| cell.replace(0))
}

#[allow(dead_code)]
pub(crate) fn set_statement_info(info: impl Into<String>) {
    STATEMENT_INFO.with(|cell| {
        let mut buf = cell.borrow_mut();
        buf.clear();
        buf.push_str(&info.into());
    });
}

pub(crate) fn take_statement_info() -> String {
    STATEMENT_INFO.with(|cell| std::mem::take(&mut *cell.borrow_mut()))
}

/// Non-consuming clone of the pending statement info trailer. Used by
/// `finish_statement_diagnostics` to copy the trailer into the session
/// context so it survives the hop back from the blocking execution
/// thread to the protocol thread (thread-locals do not cross threads),
/// without disturbing the same-thread test path that reads the
/// thread-local directly via `take_statement_info`.
pub(crate) fn peek_statement_info() -> String {
    STATEMENT_INFO.with(|cell| cell.borrow().clone())
}

/// Internal consumers can retain a producer's typed batch and its memory
/// reservation together. Row-only cursors keep their existing semantics.
#[derive(Debug)]
pub enum ResultChunk {
    Row(Vec<Value>),
    Columnar(ResultBatch),
}

pub struct ResultBatch {
    pub(crate) batch: crate::engine::executor::vec::batch::Batch,
    pub(crate) charge: Option<crate::engine::executor::mem_budget::ScopedCharge>,
}

impl std::fmt::Debug for ResultBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultBatch")
            .field("rows", &self.batch.visible_rows())
            .field("columns", &self.batch.columns().len())
            .finish_non_exhaustive()
    }
}

pub trait ResultCursor: Send + std::fmt::Debug {
    fn external_http_io(&self) -> bool {
        false
    }
    /// External streams own their relational input and need a dedicated worker.
    fn external_io(&self) -> bool {
        false
    }
    fn external_batch_rows(&self) -> usize {
        1
    }

    fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError>;

    fn next_chunk(&mut self) -> Result<Option<ResultChunk>, QueryError> {
        self.next_row().map(|row| row.map(ResultChunk::Row))
    }

    /// Drain diagnostics produced by workers behind this cursor. Consumers
    /// choose the correct owner: nested executor sinks publish them into the
    /// current statement, while the protocol writer attaches them directly
    /// to its connection context.
    fn take_warnings(&mut self) -> SqlWarnings {
        SqlWarnings::new()
    }
}

#[derive(Debug)]
pub struct VecResultCursor {
    rows: std::vec::IntoIter<Vec<Value>>,
}

impl VecResultCursor {
    pub fn new(rows: Vec<Vec<Value>>) -> Self {
        Self {
            rows: rows.into_iter(),
        }
    }
}

impl ResultCursor for VecResultCursor {
    fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        Ok(self.rows.next())
    }
}

#[derive(Debug)]
struct TimestampLocalizingCursor {
    inner: Box<dyn ResultCursor>,
    time_zone: String,
}

impl ResultCursor for TimestampLocalizingCursor {
    fn external_http_io(&self) -> bool {
        self.inner.external_http_io()
    }
    fn external_io(&self) -> bool {
        self.inner.external_io()
    }
    fn external_batch_rows(&self) -> usize {
        self.inner.external_batch_rows()
    }

    fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        let Some(mut row) = self.inner.next_row()? else {
            return Ok(None);
        };
        localize_timestamp_row(&mut row, &self.time_zone)?;
        Ok(Some(row))
    }

    fn take_warnings(&mut self) -> SqlWarnings {
        self.inner.take_warnings()
    }
}

pub(crate) fn localize_timestamp_row(row: &mut [Value], time_zone: &str) -> Result<(), QueryError> {
    for value in row {
        if let Value::Timestamp(utc_micros) = value {
            *value = Value::DateTime(
                crate::engine::expr::utc_timestamp_to_local_datetime_micros_in_tz(
                    *utc_micros,
                    time_zone,
                )?,
            );
        }
    }
    Ok(())
}

/// Concatenates several cursors end-to-end: drains the first fully, then
/// the next, and so on. Powers streaming `UNION ALL` — each branch's rows
/// flow straight through to the consumer with no intermediate materialised
/// `Vec`. A branch that is itself a streaming scan keeps its worker thread
/// producing batches lazily; a branch already in memory is wrapped in a
/// `VecResultCursor`. Nested `UNION ALL` composes naturally (a chained
/// cursor can hold other chained cursors).
#[derive(Debug)]
pub struct ChainedCursor {
    remaining: std::collections::VecDeque<Box<dyn ResultCursor>>,
    current: Option<Box<dyn ResultCursor>>,
    warnings: SqlWarnings,
}

impl ChainedCursor {
    pub fn new(cursors: Vec<Box<dyn ResultCursor>>) -> Self {
        let mut remaining: std::collections::VecDeque<Box<dyn ResultCursor>> = cursors.into();
        let current = remaining.pop_front();
        Self {
            remaining,
            current,
            warnings: SqlWarnings::new(),
        }
    }
}

impl ResultCursor for ChainedCursor {
    fn external_io(&self) -> bool {
        self.current
            .iter()
            .chain(self.remaining.iter())
            .any(|c| c.external_io())
    }
    fn external_http_io(&self) -> bool {
        self.current
            .iter()
            .chain(self.remaining.iter())
            .any(|c| c.external_http_io())
    }
    fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        while let Some(cursor) = self.current.as_mut() {
            match cursor.next_row()? {
                Some(row) => return Ok(Some(row)),
                None => {
                    self.warnings.append(&mut cursor.take_warnings());
                    self.current = self.remaining.pop_front();
                }
            }
        }
        Ok(None)
    }

    fn take_warnings(&mut self) -> SqlWarnings {
        if let Some(cursor) = self.current.as_mut() {
            self.warnings.append(&mut cursor.take_warnings());
        }
        std::mem::take(&mut self.warnings)
    }
}

impl PartialEq for QueryResponse {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (QueryResponse::Ok, QueryResponse::Ok) => true,
            (
                QueryResponse::OkAffected {
                    affected_rows: left,
                },
                QueryResponse::OkAffected {
                    affected_rows: right,
                },
            ) => left == right,
            (QueryResponse::ResultSet(left), QueryResponse::ResultSet(right)) => left == right,
            (QueryResponse::SharedResultSet(left), QueryResponse::SharedResultSet(right)) => {
                left == right
            }
            (QueryResponse::ResultSet(left), QueryResponse::SharedResultSet(right))
            | (QueryResponse::SharedResultSet(right), QueryResponse::ResultSet(left)) => {
                left.columns.as_slice() == right.columns.as_ref() && left.rows == right.rows
            }
            (QueryResponse::MultiResult(left), QueryResponse::MultiResult(right)) => left == right,
            _ => false,
        }
    }
}

impl QueryResponse {
    /// Row count exposed by the completed response; streaming counts stay cursor-owned.
    pub(crate) fn found_rows(&self) -> u64 {
        match self {
            QueryResponse::ResultSet(result) => result.rows.len() as u64,
            QueryResponse::SharedResultSet(result) => result.rows.len() as u64,
            QueryResponse::StreamingResultSet(result) => result.found_rows.unwrap_or(0),
            QueryResponse::MultiResult(results) => results
                .iter()
                .rev()
                .find_map(|response| match response {
                    QueryResponse::ResultSet(result) => Some(result.rows.len() as u64),
                    QueryResponse::SharedResultSet(result) => Some(result.rows.len() as u64),
                    _ => None,
                })
                .unwrap_or(0),
            QueryResponse::Ok | QueryResponse::OkAffected { .. } => 0,
        }
    }

    pub(crate) fn localize_timestamps(self, time_zone: &str) -> Result<Self, QueryError> {
        match self {
            QueryResponse::ResultSet(mut result) => {
                for row in &mut result.rows {
                    localize_timestamp_row(row, time_zone)?;
                }
                Ok(QueryResponse::ResultSet(result))
            }
            QueryResponse::SharedResultSet(mut result) => {
                for row in &mut result.rows {
                    localize_timestamp_row(row, time_zone)?;
                }
                Ok(QueryResponse::SharedResultSet(result))
            }
            QueryResponse::StreamingResultSet(mut result) => {
                result.cursor = Box::new(TimestampLocalizingCursor {
                    inner: result.cursor,
                    time_zone: time_zone.to_string(),
                });
                Ok(QueryResponse::StreamingResultSet(result))
            }
            QueryResponse::MultiResult(results) => Ok(QueryResponse::MultiResult(
                results
                    .into_iter()
                    .map(|response| response.localize_timestamps(time_zone))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            other => Ok(other),
        }
    }

    pub(crate) fn columns(self) -> Vec<Column> {
        match self {
            QueryResponse::ResultSet(result) => result.columns,
            QueryResponse::SharedResultSet(result) => result.columns.to_vec(),
            QueryResponse::StreamingResultSet(result) => result.columns,
            QueryResponse::MultiResult(mut results) => results
                .drain(..)
                .next()
                .map(QueryResponse::columns)
                .unwrap_or_default(),
            QueryResponse::Ok | QueryResponse::OkAffected { .. } => Vec::new(),
        }
    }

    pub fn materialize(self) -> Result<Self, QueryError> {
        match self {
            QueryResponse::SharedResultSet(result) => Ok(QueryResponse::ResultSet(ResultSet {
                columns: result.columns.to_vec(),
                rows: result.rows,
            })),
            QueryResponse::StreamingResultSet(mut result) => {
                let mut rows = Vec::new();
                loop {
                    match result.cursor.next_row() {
                        Ok(Some(row)) => rows.push(row),
                        Ok(None) => break,
                        Err(err) => {
                            for warning in result.cursor.take_warnings() {
                                push_sql_warning(warning);
                            }
                            return Err(err);
                        }
                    }
                }
                for warning in result.cursor.take_warnings() {
                    push_sql_warning(warning);
                }
                Ok(QueryResponse::ResultSet(ResultSet {
                    columns: result.columns,
                    rows,
                }))
            }
            QueryResponse::MultiResult(results) => Ok(QueryResponse::MultiResult(
                results
                    .into_iter()
                    .map(QueryResponse::materialize)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            other => Ok(other),
        }
    }
}

/// Per-prepared-handle L0 memo. It borrows the immutable cross-session
/// compiled plan and retains only handle-local presentation plus its last
/// authorized catalog epoch. Privilege/MDL recipes live on the shared plan;
/// deferred key parts bind at EXECUTE time.
#[derive(Debug)]
pub struct PreparedSelectMemo {
    pub(crate) semantics: crate::engine::cache::PlanSemantics,
    pub(crate) optimizer_switch: Arc<str>,
    /// Per-handle L0 memo points at the same immutable artifact used by the
    /// cross-session compiled SELECT cache.
    pub(crate) compiled: Arc<crate::engine::cache::CompiledSelectPlan>,
    /// Handle-local presentation/placeholder AST. This is deliberately not
    /// taken from the shared plan because column spelling belongs to PREPARE.
    pub(crate) presentation_select: Arc<crate::parser::Select>,
    /// Parameter-independent cache key for the normalized semantic template.
    /// Reused when a different parameter tier needs another physical plan.
    pub(crate) structural_fingerprint: Arc<str>,
    /// Runtime parameter comparison classes and LIMIT/OFFSET magnitude
    /// buckets used when the physical plan was chosen. MySQL permits a
    /// prepared handle to change parameter types between EXECUTEs; such a
    /// change can alter whether a character-column index probe is semantic.
    pub(crate) parameter_plan_signature: Arc<str>,
    /// The epoch memo is valid only for this exact authorization identity.
    /// Physical DML plans may be shared across sessions.
    pub(crate) authorization_account: AccountKey,
    pub(crate) authorized_auth_epoch: AtomicU64,
    /// Other memos of the same handle, most recent first, built for different
    /// parameter tiers. Parameter values that alternate between tiers then
    /// switch memos instead of recompiling on every EXECUTE.
    pub(crate) alternates: parking_lot::Mutex<Vec<Arc<PreparedSelectMemo>>>,
}

/// Plan-cache-off prepared SELECT template.  This deliberately contains no
/// [`crate::planner::PhysicalPlan`] and no compiled operator: every EXECUTE
/// still performs current-parameter cost estimation and physical planning.
/// Only parameter/statistics-independent semantic preparation plus the
/// authorization/MDL recipes are retained.
#[derive(Debug)]
pub struct PreparedSelectTemplate {
    pub(crate) select: Arc<crate::parser::Select>,
    pub(crate) deps: smallvec::SmallVec<[crate::engine::cache::PlanDep; 4]>,
    pub(crate) object_deps: smallvec::SmallVec<[crate::engine::cache::ObjectDep; 2]>,
    pub(crate) semantics: crate::engine::cache::PlanSemantics,
    /// Raw optimizer_switch spelling for allocation-free equality checks on
    /// the plan-cache-off hot path. The hashed fingerprint remains in
    /// `semantics` for canonical cache-key compatibility.
    pub(crate) optimizer_switch: Arc<str>,
    pub(crate) required_privileges: Arc<[RequiredPrivilege]>,
    pub(crate) mdl_requests: Arc<[MdlRequest]>,
    /// The epoch memo is valid only for this exact authorization identity.
    /// Physical DML plans may be shared across sessions.
    pub(crate) authorization_account: AccountKey,
    pub(crate) authorized_auth_epoch: AtomicU64,
    /// Optional-cache reservation follows the final Arc owner.  If this
    /// reservation cannot be obtained, template construction is skipped and
    /// the protocol falls back to the legacy uncached path.
    pub(crate) _memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

/// Per-`PreparedStatement` compiled SELECT memo state.
/// `Unknown` until the first execute classifies the statement; `Ineligible`
/// caches a negative result so we don't re-attempt compilation on every
/// execute of a statement not represented by a physical SELECT artifact
/// (writes and special catalog/view envelopes); `Ready` holds the reusable
/// L0 memo.
#[derive(Debug, Clone, Default)]
pub enum PreparedSelectMemoState {
    #[default]
    Unknown,
    Ineligible,
    Ready(Arc<PreparedSelectMemo>),
    /// Normalized semantic template used only when the physical plan cache is
    /// disabled.  No physical access path survives an EXECUTE boundary.
    Uncached(Arc<PreparedSelectTemplate>),
}

/// A compiled, reusable per-statement memo for a prepared INSERT / UPDATE /
/// DELETE. Mirrors [`PreparedSelectMemo`] in spirit but tailored to the
/// write side: there is no `PhysicalPlan` here (DML walks the storage
/// API directly), only the bits the engine recomputes on every COM_STMT_EXECUTE
/// today and can safely cache once per prepared handle:
///
///   * `statement` — qualified AST. Holding it as an `Arc` lets the fast
///     path borrow the cached form (no `Statement::clone` per execute).
///   * `epoch` — process-wide plan-cache epoch snapshot, invalidated by
///     any DDL. Combined with `table_schema_version` it lets the engine
///     recompile only when the catalog actually changed.
///   * `required_privileges` / `mdl_requests` — precomputed authorisation
///     and MDL request lists; the legacy path rebuilds both vectors for
///     every execute.
///   * `authorized_auth_epoch` — atomic memo of the last successful auth
///     check; reused as long as the auth catalog epoch matches (same
///     trick as `PreparedSelectMemo`).
#[derive(Debug)]
pub struct PreparedDmlPlan {
    pub(crate) semantics: crate::engine::cache::PlanSemantics,
    pub(crate) optimizer_switch: Arc<str>,
    pub(crate) statement: Arc<Statement>,
    pub(crate) epoch: u64,
    /// Cost-model epoch is relevant for the locking SELECT operator stored in
    /// this otherwise-DML-shaped memo.
    pub(crate) cbo_epoch: u64,
    pub(crate) table_key: Arc<str>,
    pub(crate) table_id: u64,
    pub(crate) table_schema_version: u64,
    pub(crate) required_privileges: Arc<[RequiredPrivilege]>,
    pub(crate) mdl_requests: Arc<[MdlRequest]>,
    pub(crate) locking_point_read: Option<PreparedLockingPointRead>,
    pub(crate) point_write: Option<PreparedPointWrite>,
    pub(crate) range_update: Option<PreparedRangeUpdate>,
    /// The epoch memo is valid only for this exact authorization identity.
    /// Physical DML plans may be shared across sessions.
    pub(crate) authorization_account: AccountKey,
    pub(crate) authorized_auth_epoch: AtomicU64,
    /// Last catalog publication against which this immutable binding was
    /// validated. Zero forces a check for a newly compiled plan.
    pub(crate) validated_catalog_epoch: AtomicU64,
    /// Per-parameter-domain memo of the runtime coverage proof for
    /// `locking_point_read` and `point_write` respectively.
    pub(crate) locking_read_cover_memo: AtomicU64,
    pub(crate) point_write_cover_memo: AtomicU64,
}

#[derive(Debug)]
pub(crate) struct PreparedLockingPointRead {
    pub(crate) table: Arc<TableSchema>,
    pub(crate) index: IndexSchema,
    pub(crate) key: IndexKey,
    pub(crate) projection: crate::engine::cache::CompiledProjectedRead,
    /// Positions inside `projection.columns` that reconstruct a plain-column
    /// unique key after the row lock is held. `None` retains the second B-tree
    /// probe for functional/prefix/otherwise complex indexes.
    pub(crate) key_positions: Option<Arc<[usize]>>,
    pub(crate) result_columns: Arc<[Column]>,
}

/// Compile-once metadata for a prepared UPDATE/DELETE whose WHERE clause is
/// fully covered by one exact unique-key lookup. The bound key keeps
/// placeholders and is materialised from the current COM_STMT_EXECUTE
/// parameters by the normal index lookup path.
#[derive(Debug)]
pub(crate) struct PreparedPointWrite {
    pub(crate) table: Arc<TableSchema>,
    pub(crate) index: IndexSchema,
    pub(crate) key: IndexKey,
    pub(crate) kind: PreparedPointWriteKind,
}

#[derive(Debug)]
pub(crate) enum PreparedPointWriteKind {
    Update(PreparedPointUpdate),
    Delete,
}

/// Compiled assignments and post-lock predicate for indexed range UPDATEs.
#[derive(Debug)]
pub(crate) struct PreparedRangeUpdate {
    pub(crate) table: Arc<TableSchema>,
    pub(crate) predicate: crate::engine::expr_program::CompiledTableExpr,
    pub(crate) update: PreparedPointUpdate,
}

#[derive(Debug)]
pub(crate) struct PreparedPointUpdate {
    pub(crate) replay_safe: bool,
    pub(crate) assignments: Arc<[PreparedPointAssignment]>,
    pub(crate) changed_columns: Arc<[usize]>,
    pub(crate) user_changed_columns: Arc<[usize]>,
    pub(crate) on_update_columns: Arc<[usize]>,
    /// Columns read by assignment RHS expressions plus every target/before
    /// value needed for sparse undo. Prepared descriptor-only DML decodes only
    /// these slots; unrelated LOB descriptors remain untouched in the heap.
    pub(crate) required_columns: Arc<[usize]>,
    pub(crate) decode_recipe: Option<
        Arc<(
            crate::storage::RowDecodePlan,
            crate::engine::executor::mem_budget::ControlledMemoryCharge,
        )>,
    >,
    /// Schema-validated index positions affected by `changed_columns`, after
    /// generated-column dependency expansion. The prepared memo's schema
    /// version guard makes these positions statement-stable.
    pub(crate) affected_indexes: Arc<[usize]>,
}

#[derive(Debug)]
pub(crate) struct PreparedPointAssignment {
    pub(crate) column_index: usize,
    pub(crate) expression: crate::engine::expr_program::CompiledTableExpr,
}

impl PreparedPointAssignment {
    pub(crate) fn eval(
        &self,
        table: &crate::storage::TableSchema,
        values: &[Value],
    ) -> Result<Value, QueryError> {
        let value = self.expression.eval(table, values)?;
        if matches!(
            table.columns[self.column_index].data_type,
            crate::storage::DataTypeKind::Binary { .. }
                | crate::storage::DataTypeKind::VarBinary { .. }
                | crate::storage::DataTypeKind::Blob { .. }
        ) {
            crate::engine::expr::encode_binary_assignment_value(
                value,
                self.expression.source_charset(table),
            )
        } else {
            Ok(value)
        }
    }
}

/// Per-`PreparedStatement` memo state for the compile-once DML fast path.
/// Same classification idea as [`PreparedSelectMemoState`]: `Unknown` →
/// `Ready(...)` on the first eligible execute, or `Ineligible` if the
/// statement (or session shape) doesn't qualify, so later EXECUTEs of
/// the same handle avoid the per-execute classification.
#[derive(Debug, Clone, Default)]
pub enum PreparedDmlState {
    #[default]
    Unknown,
    Ineligible,
    Ready(Arc<PreparedDmlPlan>),
}

/// One async subscription to a row-lock release shard. The shared waiter
/// count lets the storage release path preserve its zero-allocation early
/// return when no protocol task is awaiting a row.
pub struct AsyncRowLockSignal {
    receiver: tokio::sync::watch::Receiver<u64>,
    observed_generation: u64,
    active: Arc<AtomicU64>,
}

impl AsyncRowLockSignal {
    pub(crate) fn new(
        receiver: tokio::sync::watch::Receiver<u64>,
        observed_generation: u64,
        active: Arc<AtomicU64>,
    ) -> Self {
        active.fetch_add(1, Ordering::AcqRel);
        Self {
            receiver,
            observed_generation,
            active,
        }
    }

    pub async fn changed(&mut self) -> Result<(), tokio::sync::watch::error::RecvError> {
        let current = *self.receiver.borrow_and_update();
        if current != self.observed_generation {
            self.observed_generation = current;
            return Ok(());
        }
        self.receiver.changed().await?;
        self.observed_generation = *self.receiver.borrow_and_update();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn generation(&self) -> u64 {
        *self.receiver.borrow()
    }
}

impl Drop for AsyncRowLockSignal {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Statement-local metadata retained by the compiled text-DML batch executor.
/// These plans cannot run triggers or change tracked session variables.
#[derive(Debug, Clone)]
pub struct DmlStatementResult {
    pub affected_rows: u64,
    pub matched_rows: Option<u64>,
    pub insert_id: u64,
    pub warnings: SqlWarnings,
    pub info: String,
    pub autocommit: bool,
    pub in_transaction: bool,
}

pub trait QueryEngine: Send + Sync + 'static {
    /// Conservative catalog-aware scheduling hint, never an execution step.
    fn sql_may_call_http(&self, sql: &str) -> bool {
        crate::engine::http::mentions(sql)
    }

    fn execute(&self, session: &mut SessionContext, sql: &str)
        -> Result<QueryResponse, QueryError>;

    /// None means no statement was executed; the protocol uses its general
    /// sequential path. A handled batch retains successful-prefix metadata.
    fn execute_text_dml_batch(
        &self,
        _session: &mut SessionContext,
        _sql: &str,
    ) -> Option<(Vec<DmlStatementResult>, Result<QueryResponse, QueryError>)> {
        None
    }

    /// Subscribe to row-lock release progress for async inline retry. Storage
    /// engines return a per-database watch receiver; lightweight/mock engines
    /// keep the default `None` and use the blocking executor fallback.
    fn row_lock_async_signal(&self) -> Option<AsyncRowLockSignal> {
        None
    }

    /// Bind protocol-memory reclamation for the current synchronous poll.
    fn enter_memory_reclaim_scope(
        &self,
    ) -> Option<crate::engine::executor::mem_budget::ReclaimDatabaseScope> {
        None
    }

    /// Make bounded room for the fixed protocol/session allocations that occur
    /// before the first command can install a query-owned database context.
    /// Storage-backed engines may evict optional buffer frames; mocks and
    /// non-storage engines keep the no-op default.
    fn reclaim_connection_admission_memory(
        &self,
        _requested_bytes: usize,
    ) -> Result<(), QueryError> {
        Ok(())
    }

    /// Parse an exact SQL source into an unqualified, shareable AST.
    /// Storage-backed engines override this with `ParsedStatementCache`;
    /// callers must clone before qualification, placeholder numbering, or
    /// auto-parameterization mutates the tree.
    fn parse_cached_statement(
        &self,
        session: &SessionContext,
        sql: &str,
    ) -> Result<Arc<Statement>, QueryError> {
        let parse_bits = parse_temporal_sql_mode_bits(&session.sql_mode);
        let parsed = {
            let _mode_guard = crate::parser::ParserSqlModeGuard::activate(parse_bits);
            crate::parser::parse_statement(sql)
                .map_err(|err| mysql_errors::error(mysql_errors::ER_PARSE_ERROR, err.to_string()))?
        };
        Ok(Arc::new(parsed))
    }

    /// Account for an exact-SQL parser artifact reused by a protocol-local
    /// L0 memo. Storage engines expose this beside their shared-cache hit
    /// counter; engines without cache metrics can keep the default no-op.
    fn record_select_parse_cache_hit(&self) {}

    /// Prepare a statement template, returning a shareable, qualified AST.
    /// `expected_param_count` is the number of `?` placeholders the client
    /// reported (counted from the raw SQL bytes); cache misses validate that
    /// parsing produced the same number, returning an error otherwise so the
    /// caller can fall back to text-substitution binding.
    ///
    /// The default implementation simply parses the SQL and forwards through
    /// `prepare_statement_template`; engines that maintain a cross-session
    /// statement cache (such as `StorageQueryEngine`) override this to reuse
    /// previously qualified ASTs.
    fn prepare_cached(
        &self,
        session: &mut SessionContext,
        sql: &str,
        expected_param_count: usize,
    ) -> Result<Arc<Statement>, QueryError> {
        let mut parsed = (*self.parse_cached_statement(session, sql)?).clone();
        let mut next = 0;
        crate::parser::number_statement_placeholders(&mut parsed, &mut next);
        if next != expected_param_count {
            return Err(mysql_errors::error(
                mysql_errors::ER_WRONG_ARGUMENTS,
                format!(
                    "placeholder count mismatch: parsed {next}, expected {expected_param_count}"
                ),
            ));
        }
        let qualified = self.prepare_statement_template(session, sql, parsed)?;
        Ok(Arc::new(qualified))
    }

    /// Hint that any cached statement-derived state may be stale (e.g. after
    /// a DDL). Default implementation is a no-op.
    fn invalidate_statement_cache(&self) {}

    /// Wake engine-owned waiters after a KILL request. Engines without
    /// lock waits can ignore this; storage-backed execution uses it to
    /// make row-lock waiters re-check their session cancel generation.
    fn notify_lock_waiters_for_kill(&self) {}

    /// Snapshot of the engine's statement cache, if any. Used by
    /// `SHOW ENGINE MYDB STATUS` for observability.
    fn statement_cache_stats(&self) -> Option<CacheStats> {
        None
    }

    /// Record a MySQL general-log command event. Engines without a SQL log
    /// surface can ignore this; storage-backed execution writes to the
    /// configured FILE/TABLE destinations.
    fn log_general_event(&self, _session: &SessionContext, _command_type: &str, _argument: &str) {}

    /// Flush/reopen server log files.
    fn flush_logs(&self, _kind: crate::engine::logging::FlushLogKind) -> Result<(), QueryError> {
        Ok(())
    }

    fn prepare_statement_template(
        &self,
        _session: &mut SessionContext,
        _sql: &str,
        statement: Statement,
    ) -> Result<Statement, QueryError> {
        Ok(statement)
    }

    fn describe_prepared_statement(
        &self,
        _session: &mut SessionContext,
        _sql: &str,
    ) -> Result<Vec<Column>, QueryError> {
        Ok(Vec::new())
    }

    /// Optional bounded, cache-only PREPARE path. It must retain normal
    /// placeholder validation and session qualification, and return `None`
    /// whenever parsing, planning or blocking execution could be required.
    fn try_prepare_cached_inline(
        &self,
        _session: &mut SessionContext,
        _sql: &str,
        _expected_param_count: usize,
    ) -> Option<Result<(Arc<Statement>, Vec<Column>), QueryError>> {
        None
    }

    fn execute_prepared_statement(
        &self,
        _session: &mut SessionContext,
        _sql: &str,
        _statement: Statement,
    ) -> Option<Result<QueryResponse, QueryError>> {
        None
    }

    /// P2-A1 COM_FIELD_LIST — return one already-encoded ColumnDefinition41
    /// packet payload per matching column of `table_name` on
    /// the session's current database. `wildcard` is the MySQL LIKE pattern from the
    /// command's optional trailing bytes; an empty string means "every
    /// column". The default impl ships an empty list so test doubles that
    /// don't model a catalog don't need to opt in.
    fn field_list_columns(
        &self,
        _session: &SessionContext,
        _table_name: &str,
        _wildcard: &str,
    ) -> Result<Vec<Vec<u8>>, QueryError> {
        Ok(Vec::new())
    }

    /// Resolve and run the prepared handle's shared compiled SELECT artifact.
    ///
    /// On the first execute of an eligible statement the engine compiles a
    /// reusable `PreparedSelectMemo` and stores it in `state`; subsequent
    /// executes reuse it without AST cloning, qualification or planning. Returns:
    ///   * `Some(result)` — executed via the cached plan.
    ///   * `None` — the caller must fall back to the legacy
    ///     `execute_prepared_statement` path (statement not eligible for the
    ///     compiled SELECT envelope, or the session requires another
    ///     transaction vehicle). `state`
    ///     records the classification so repeat executes stay cheap.
    ///
    /// `params` is the already-decoded bind-parameter slice; the engine
    /// installs it in the thread-local parameter context for the duration
    /// of execution. The default implementation has no compiled runner.
    fn execute_prepared_select(
        &self,
        _session: &mut SessionContext,
        _state: &mut PreparedSelectMemoState,
        _parsed: &Arc<Statement>,
        _sql: &str,
        _params: &[Value],
    ) -> Option<Result<QueryResponse, QueryError>> {
        None
    }

    /// Compile-once / late-bind fast path for prepared INSERT / UPDATE /
    /// DELETE. Same shape as [`Self::execute_prepared_select`]: the first
    /// eligible execute compiles a reusable `PreparedDmlPlan` and stores
    /// it in `state`; subsequent executes reuse it, skipping the eight
    /// dispatch wrappers, the `session.clone()` inside the lock-wait
    /// retry loop, and the per-execute `Statement::clone()`. Returns
    /// `None` for any ineligible statement or session-state shape so the
    /// caller can fall back to the legacy `execute_prepared_statement`
    /// path. The default implementation has no fast path.
    fn execute_prepared_dml(
        &self,
        _session: &mut SessionContext,
        _state: &mut PreparedDmlState,
        _parsed: &Arc<Statement>,
        _sql: &str,
        _params: &[Value],
    ) -> Option<Result<QueryResponse, QueryError>> {
        None
    }

    /// Attempt an autocommit COM_QUERY DML on the caller's runtime worker.
    /// Implementations must return `None` without executing unless the SQL is
    /// proved eligible for their nonblocking/compile-once DML path; the caller
    /// then falls back to the regular blocking-pool dispatch.
    fn execute_large_text_dml_inline(
        &self,
        _session: &mut SessionContext,
        _sql: &str,
    ) -> Option<Result<QueryResponse, QueryError>> {
        None
    }

    /// Look up a parser-verified literal statement shape (SELECT or
    /// single-statement INSERT/UPDATE/DELETE). The returned AST holds
    /// placeholders and the value vector is in their exact parser-proven
    /// order. A cold/ineligible shape returns `None` so the caller follows the
    /// ordinary exact parse path, which may seed the cache.
    fn cached_text_statement(
        &self,
        _session: &SessionContext,
        _sql: &str,
    ) -> Option<(Arc<Statement>, Vec<Value>, Arc<str>)> {
        None
    }

    /// Execute an already-parameterized text SELECT artifact. Callers may run
    /// this inline for a proven nonblocking read or on the blocking pool for a
    /// locking read; no parsing or auto-parameter rewrite is repeated.
    fn execute_parsed_with_params(
        &self,
        session: &mut SessionContext,
        sql: &str,
        _statement: Arc<Statement>,
        _params: Vec<Value>,
        _shape_sql: Arc<str>,
    ) -> Result<QueryResponse, QueryError> {
        self.execute(session, sql)
    }

    /// Execute a SQL statement that the caller has already parsed.
    ///
    /// Used by the COM_QUERY inline route for non-blocking read-only
    /// SELECTs: the session pre-parses to confirm the statement cannot
    /// take row locks, then hands the parsed AST here so we don't
    /// re-tokenize inside the engine. The contract is identical to
    /// [`Self::execute`] — same logging, plan-cache, txn snapshot, and
    /// auto-parameterization behaviour — only the parsing step is
    /// skipped. Implementations without an already-parsed route simply
    /// fall back to `self.execute(session, sql)`.
    fn execute_parsed(
        &self,
        session: &mut SessionContext,
        sql: &str,
        _statement: Arc<Statement>,
    ) -> Result<QueryResponse, QueryError> {
        self.execute(session, sql)
    }

    fn execute_load_data_statement(
        &self,
        _session: &mut SessionContext,
        _statement: Statement,
    ) -> Result<QueryResponse, QueryError> {
        Err(mysql_errors::unsupported("LOAD DATA by this engine"))
    }

    fn execute_load_data_statement_from_reader(
        &self,
        session: &mut SessionContext,
        mut statement: Statement,
        mut reader: Box<dyn std::io::Read + Send>,
    ) -> Result<QueryResponse, QueryError> {
        // Compatibility fallback for lightweight/mock engines. The storage
        // engine overrides this method and consumes the reader incrementally.
        let Statement::LoadData(load) = &mut statement else {
            return Err(mysql_errors::unsupported(
                "streaming LOAD DATA by this engine",
            ));
        };
        let mut data = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut data).map_err(|err| {
            crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                format!("LOAD DATA LOCAL reader: {err}"),
            )
        })?;
        load.data = Some(data);
        self.execute_load_data_statement(session, statement)
    }

    fn close_session(&self, _session: &mut SessionContext) -> Result<(), QueryError> {
        Ok(())
    }

    fn session_cleanup_is_trivial(&self, _session: &SessionContext) -> bool {
        false
    }

    /// Reset connection-scoped SQL state without releasing the authenticated
    /// account's live-connection charge. This backs COM_RESET_CONNECTION;
    /// implementations should roll back transactions and release temporary
    /// tables/locks just like `close_session`, while preserving identity and
    /// the transport connection.
    fn reset_session(&self, _session: &mut SessionContext) -> Result<(), QueryError> {
        Ok(())
    }

    fn protocol_temp_dir(&self) -> std::path::PathBuf {
        std::env::temp_dir().join("mydb-protocol")
    }

    fn shutdown(&self) -> Result<(), QueryError> {
        Ok(())
    }

    fn authenticate(
        &self,
        username: &str,
        host: &str,
        auth_response: &[u8],
        auth_plugin: Option<&str>,
    ) -> Result<AuthorizationContext, QueryError> {
        self.authenticate_request(AuthRequest::legacy(
            username,
            host,
            auth_response,
            auth_plugin,
        ))
        .map(|outcome| outcome.context)
    }

    /// Return the authentication plugin selected by the matching account, if
    /// the engine can answer before password verification. Protocol sessions
    /// use this to issue a MySQL AuthSwitchRequest when the advertised default
    /// plugin differs from the account's plugin.
    fn auth_plugin_for_user(
        &self,
        _username: &str,
        _host: &str,
    ) -> Result<Option<String>, QueryError> {
        Ok(None)
    }

    /// True if the account behind `(username, peer_host)` is currently in
    /// the caching_sha2_password server-side cache. Used by `session.rs` to
    /// decide between the fast (`AuthMoreData{0x03}`) path and the full
    /// (`AuthMoreData{0x04}` + RSA) path. Default impl returns `false` so
    /// every connection on a stub engine takes the full path.
    fn is_in_auth_cache(&self, _username: &str, _peer_host: &str) -> bool {
        false
    }

    fn authenticate_request(&self, request: AuthRequest<'_>) -> Result<AuthOutcome, QueryError> {
        let _ = (request.auth_response, request.auth_plugin);
        let context = AuthorizationContext {
            authenticated_as: AccountKey::new(request.username, request.host),
            active_roles: Default::default(),
            epoch: 0,
        };
        Ok(AuthOutcome {
            context,
            password_expired: false,
            account_locked: false,
            resource_limits: ResourceLimits::default(),
            current_user_is_system: false,
        })
    }

    /// N1/N2: caching_sha2_password / sha256_password full-authentication
    /// path. Called by `session.rs` after the fast (hash-based) verify
    /// fails and the server has gone through the RSA public-key dance to
    /// recover the plaintext password. Default impl always denies (so
    /// engines that don't support the feature simply refuse the fallback).
    fn authenticate_with_plaintext_password(
        &self,
        _username: &str,
        _host: &str,
        _password: &str,
        _request: &AuthRequest<'_>,
    ) -> Result<AuthOutcome, QueryError> {
        Err(crate::engine::auth::access_denied(_username, _host))
    }

    fn kill_session(
        &self,
        _session: &SessionContext,
        _connection_id: u32,
    ) -> Result<(), QueryError> {
        Err(crate::storage::error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "Connection cancellation is unavailable for this engine",
        ))
    }

    fn authorize_global_privilege(
        &self,
        _session: &SessionContext,
        _privilege: crate::engine::auth::Privilege,
    ) -> Result<(), QueryError> {
        Ok(())
    }

    fn init_session_database(
        &self,
        session: &mut SessionContext,
        database: &str,
    ) -> Result<(), QueryError> {
        session.current_database = crate::storage::normalize_name(database);
        session.mark_pfs_variables_dirty();
        Ok(())
    }
}

/// Per-transaction execution owner shared by every temporary session clone.
/// It serializes statements sharing a transaction and has a fixed,
/// governor-accounted resident footprint.
#[derive(Debug)]
pub(crate) struct TxnRuntime {
    pub(crate) txn_id: u64,
    pub(crate) wal_buffer_ready: std::sync::atomic::AtomicBool,
    statement_gate: parking_lot::ReentrantMutex<()>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl TxnRuntime {
    fn reset_for(&mut self, txn_id: u64) {
        // Arc::get_mut proves no session clone, continuation or weak observer
        // can still see this runtime. Future transaction-local state must be
        // reset here alongside the WAL initialization flag.
        self.txn_id = txn_id;
        self.wal_buffer_ready
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    fn try_new(txn_id: u64) -> Result<Self, QueryError> {
        let bytes = std::mem::size_of::<Self>();
        let charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
            bytes,
        )?;
        Ok(Self {
            txn_id,
            wal_buffer_ready: std::sync::atomic::AtomicBool::new(false),
            statement_gate: parking_lot::ReentrantMutex::new(()),
            _charge: charge,
        })
    }

    pub(crate) fn enter_statement(&self) -> parking_lot::ReentrantMutexGuard<'_, ()> {
        self.statement_gate.lock()
    }
}

#[derive(Debug, Default)]
pub(crate) struct SqlPreparedStatements {
    statements: HashMap<String, SessionSqlPreparedStatement>,
    accounted: std::collections::HashSet<String>,
}

impl Clone for SqlPreparedStatements {
    fn clone(&self) -> Self {
        // Execution workers clone session snapshots for read-only evaluation.
        // Those copies are not new client PREPARE handles.
        Self {
            statements: self.statements.clone(),
            accounted: std::collections::HashSet::new(),
        }
    }
}

impl Drop for SqlPreparedStatements {
    fn drop(&mut self) {
        crate::engine::status_counters::adjust_prepared_stmt_count(-(self.accounted.len() as i64));
    }
}

impl std::ops::Deref for SqlPreparedStatements {
    type Target = HashMap<String, SessionSqlPreparedStatement>;

    fn deref(&self) -> &Self::Target {
        &self.statements
    }
}

#[derive(Debug, Clone)]
pub struct SessionContext {
    pub autocommit: bool,
    pub isolation: IsolationLevel,
    pub next_txn_isolation: Option<IsolationLevel>,
    /// MySQL 8.0 `SET TRANSACTION READ ONLY` / `READ WRITE`: when set, the
    /// next transaction that begins on this session will pick up this
    /// access mode in place of `read_only_txn` (the session default).
    /// Cleared on every BEGIN/START TRANSACTION.
    pub next_txn_read_only: Option<bool>,
    pub(crate) active_txn_isolation: Option<IsolationLevel>,
    pub current_database: String,
    pub user: String,
    pub host: String,
    pub authenticated_as: AccountKey,
    pub login_user: String,
    pub active_roles: super::auth::AuthCow<std::collections::BTreeSet<AccountKey>>,
    pub auth_epoch: u64,
    pub password_expired: bool,
    pub account_locked: bool,
    pub auth_sandbox: bool,
    pub auth_factors_done: u8,
    pub account_limits: ResourceLimits,
    pub current_user_is_system: bool,
    /// True only for server-created recovery/apply/maintenance contexts.
    /// Unlike the SQL SYSTEM_USER dynamic privilege this cannot be granted by
    /// SQL and is the sole blanket authorization bypass.
    pub internal_system_session: bool,
    pub connection_id: u32,
    /// Per-connection MySQL status counters. Protocol sessions install a
    /// governed shared object before the handshake; embedded callers leave it
    /// unset because they are not registered server connections.
    pub(crate) status: Option<Arc<crate::engine::status_counters::SessionStatus>>,
    pub ssl_cipher: Option<String>,
    pub ssl_version: Option<String>,
    pub x509_subject: Option<String>,
    pub x509_issuer: Option<String>,
    pub user_variables: HashMap<String, Value>,
    /// Cached performance_schema variable rows only need rebuilding after a
    /// session-visible mutation (or a process-wide GLOBAL sysvar write).
    /// Keeping this local avoids a full sysvar-registry snapshot after every
    /// SELECT/DML/protocol command.
    pub(crate) pfs_variables_dirty: bool,
    /// Canonical name of the sole system variable changed since the last PFS
    /// publication.  A second distinct change promotes the state to
    /// `pfs_variables_dirty`, so the common single-`SET` path is incremental
    /// without retaining an unbounded collection in the session.
    pub(crate) pfs_variable_dirty_name: Option<&'static str>,
    pub(crate) pfs_global_variable_epoch: u64,
    /// SQL-level prepared statements created by `PREPARE` inside stored
    /// procedures. MySQL scopes these to the connection, not to the routine
    /// frame, so a later CALL may EXECUTE or DEALLOCATE the same statement.
    pub(crate) sql_prepared_statements: SqlPreparedStatements,
    /// Shared with expression snapshots: streaming LAST_INSERT_ID(expr)
    /// updates the connection directly, without work on ordinary statements.
    pub last_insert_id: Arc<std::sync::atomic::AtomicU64>,
    pub statement_insert_id: u64,
    /// Info trailer (`Records: N ...` / `Rows matched: ...`) captured on
    /// the execution thread so the protocol thread can emit it in the OK
    /// packet. Thread-locals don't cross the blocking-pool boundary, so
    /// this carries the trailer back. `None` = no trailer.
    pub pending_statement_info: Option<String>,
    /// Matched rows must cross the execution-thread boundary with the OK metadata.
    pub pending_statement_matched_rows: Option<u64>,
    pub last_row_count: i64,
    /// ROW_COUNT in the retained diagnostics area, independent of the most
    /// recent diagnostics-reading statement's ROW_COUNT() return value.
    pub(crate) diagnostics_row_count: i64,
    pub found_rows: u64,
    /// PostgreSQL sequence per-session state: CACHE blocks + currval/lastval.
    /// `RefCell` so `nextval`/`setval` can update it through the shared session
    /// pointer the expression evaluator holds during a statement.
    pub seq_state: std::cell::RefCell<crate::storage::sequence::SessionSeqState>,
    pub(crate) current_txn: Option<u64>,
    /// Shared transaction-local runtime for every clone of this session.
    ///
    /// A connection normally executes one statement at a time, but parallel
    /// INSERT/SELECT workers and detached blocking continuations can hold
    /// clones concurrently.  The re-entrant statement gate gives all of those
    /// clones one serial mutation strand without serialising unrelated
    /// transactions.  It is deliberately re-entrant because triggers and
    /// stored programs may execute nested statements on the same thread.
    /// Later transaction-local caches live behind the same owner so their
    /// reservation follows the final clone.
    pub(crate) txn_runtime: Option<Arc<TxnRuntime>>,
    /// At most one governed, inactive runtime per autocommit connection.
    /// Shared runtimes are never mutated or reused across transaction ids.
    pub(crate) spare_txn_runtime: Option<Arc<TxnRuntime>>,
    pub(crate) mdl_fast_cache: std::cell::RefCell<crate::storage::mdl::MdlFastCache>,
    pub(crate) txn_snapshot_scn: Option<u64>,
    /// Lifetime pin for a REPEATABLE READ transaction's consistent-read
    /// snapshot. Statement guards protect an executing SELECT, but RR must
    /// also keep its undo history between statements. `Arc` preserves the
    /// pin across the short-lived session clones used by parallel workers;
    /// the registry entry is removed when the last clone or the transaction
    /// itself drops the guard.
    pub(crate) txn_snapshot_guard: Option<Arc<SnapshotGuard>>,
    pub(crate) binlog_snapshot_status: Option<Arc<crate::binlog::BinlogStatusSnapshot>>,
    pub(crate) explicit_txn: bool,
    pub(crate) read_only_txn: bool,
    /// Sticky connection-local evidence that this session has attempted DML.
    /// Mixed-workload scheduling uses it to distinguish a read-only pool
    /// connection running with `autocommit=0` from a read-write connection
    /// whose parallel SELECT is commonly the first step of a transaction.
    /// COM_RESET_CONNECTION creates a fresh context and clears the evidence.
    pub(crate) write_intent_seen: bool,
    /// Set by `COMMIT`/`ROLLBACK ... RELEASE` (or `completion_type = RELEASE`):
    /// the protocol loop closes the connection after sending this statement's
    /// response, as though the client had issued `COM_QUIT`.
    pub(crate) close_after_response: bool,
    /// XA branch currently associated with this session (ACTIVE or IDLE).
    /// Once the branch is PREPARED it is detached from the session and
    /// lives in the engine-global `Database.prepared_xa` registry, so any
    /// session can XA COMMIT / XA ROLLBACK it.
    pub(crate) xa_branch: Option<crate::xa::XaBranch>,
    /// Per-transaction memo of transaction-duration MDL grants:
    /// `(txn_id, requests already granted for that txn)`. The MDL manager
    /// is a single global mutex and every statement used to take it twice
    /// (acquire + no-op release) even though transactional statements hold
    /// their tickets until commit — on OLTP profiles that was several
    /// percent of CPU. A statement whose requests are all memoized for the
    /// *current* txn id skips the manager entirely; the memo self-
    /// invalidates when `current_txn` changes (every commit / rollback
    /// allocates a new txn id), so no release-side hook is needed.
    /// `RefCell` for the same reason as `seq_state`: the acquire path only
    /// has `&SessionContext`.
    pub(crate) mdl_txn_grants: std::cell::RefCell<(u64, Vec<crate::storage::MdlRequest>)>,
    /// Session-local generation for transaction-duration MDL when no storage
    /// transaction id exists yet. Plain `BEGIN` and `SET autocommit=0`
    /// establish a lazy read transaction whose first non-locking SELECT can
    /// avoid allocating a storage txn, but its transaction-duration MDL still
    /// lives until COMMIT/ROLLBACK. This generation lets the MDL memo track
    /// that lazy transaction without forcing a storage txn id onto read-only
    /// workloads.
    pub(crate) mdl_txn_generation: u64,
    /// CALL keeps its statement-duration routine locks across body statements
    /// and explicit transaction boundaries, until the outermost CALL returns.
    pub(crate) routine_mdl_depth: usize,
    /// An owning function/trigger statement holds nested DML finalization.
    /// Avoid opening a savepoint for every function row inside that statement.
    pub(crate) routine_atomic_depth: usize,
    /// Recursion depth limit for `WITH RECURSIVE`. Mirrors MySQL's
    /// `cte_max_recursion_depth` system variable.
    pub cte_max_recursion_depth: u32,
    /// MySQL `max_sp_recursion_depth`: how many times a stored procedure may
    /// recurse (call itself, directly or indirectly). Default 0 → procedure
    /// recursion disabled; exceeding it raises error 1456. Stored functions
    /// ignore this and forbid recursion entirely (error 1424).
    pub max_sp_recursion_depth: u32,
    /// MySQL `foreign_key_checks` session variable. When false, FK
    /// child-side existence checks and parent-side cascades are skipped
    /// for this session's DML (re-enabling does not retro-validate).
    pub foreign_key_checks: bool,
    /// MySQL `sql_log_off` session variable. mydb applies it to the
    /// general query log only; diagnostic slow/error logs remain controlled
    /// by their own global switches.
    pub sql_log_off: bool,
    /// MySQL `sql_log_bin` (Both scope, SuperReadWrite). When false, this
    /// session's row-level mutations are NOT written to the binary log.
    /// mysqldump's standard output starts with `SET @@SESSION.SQL_LOG_BIN=0`
    /// so dumps can be loaded back without re-replicating; we need to
    /// honour that path. Default ON (true).
    pub sql_log_bin: bool,
    /// mydb extension: when false, append-only writes to newly allocated
    /// storage may use the crash-safe reduced-redo protocol. Transaction
    /// boundaries, undo and in-place page changes remain WAL protected.
    pub redo_enabled: bool,
    /// MySQL 8.0 `group_concat_max_len` session variable. Caps the byte
    /// length of `GROUP_CONCAT` output; the joined string is truncated
    /// at a UTF-8 codepoint boundary once it grows past this many bytes.
    /// MySQL's documented default is 1024; mydb mirrors it. Setting this
    /// per session lets BI tools and connectors raise the cap when they
    /// expect long concatenations.
    pub group_concat_max_len: u64,
    pub sql_mode: String,
    /// Cached parse of the temporal-relevant `sql_mode` flags (strict /
    /// NO_ZERO_DATE / NO_ZERO_IN_DATE / ALLOW_INVALID_DATES /
    /// TIME_TRUNCATE_FRACTIONAL), refreshed by the `sql_mode` sysvar
    /// setter so per-value temporal coercion on the INSERT hot path
    /// reads a plain bitmask instead of re-splitting the mode string.
    pub temporal_sql_mode_bits: u16,
    pub character_set_client: String,
    pub character_set_connection: String,
    pub character_set_results: Option<String>,
    pub collation_connection: String,
    /// Default `EXPLAIN` output format used when the query omits an
    /// explicit `FORMAT=...` clause. Mirrors MySQL 8.x's
    /// `explain_format` session variable; initialised from the global
    /// default at session creation so `SET GLOBAL explain_format = ...`
    /// applies to subsequent connections.
    pub explain_format: ExplainFormat,
    pub(crate) cancel_handle: Option<SessionCancelHandle>,
    pub(crate) query_cancel_generation: u64,
    pub(crate) connection_kill_generation: u64,
    /// Live cancel token for this connection. Built once when the
    /// session is registered (`set_cancel_handle`) and refreshed at the
    /// top of every command in the MySQL session loop via
    /// `refresh_cancel_generations` so the generation baseline starts
    /// at the moment the command begins. Nested executor entries
    /// (subqueries, stored programs) **must not** rebuild this — they
    /// `install_session_cancel_context(session)` which just `enter`s
    /// the same token onto the current thread.
    pub(crate) current_query_token: Option<QueryCancelToken>,
    pub warnings: SqlWarnings,
    /// Connection attributes the client sent at handshake
    /// (CLIENT_CONNECT_ATTRS, e.g. `program_name`, `_client_name`).
    /// Surfaced via performance_schema.session_connect_attrs /
    /// session_account_connect_attrs.
    pub connection_attrs: std::collections::BTreeMap<String, String>,
    /// Typed CLIENT_QUERY_ATTRIBUTES for the currently executing top-level
    /// statement. Cleared at the next command boundary/reset.
    pub statement_query_attributes: std::collections::BTreeMap<String, Value>,
    /// MySQL `resultset_metadata` session enum: true=FULL, false=NONE.
    pub resultset_metadata_full: bool,
    /// Render VECTOR results as readable strings on the MySQL text protocol.
    pub vector_result_format_text: bool,
    pub(crate) temp_tables: HashMap<String, SessionTempTable>,
    pub(crate) temp_quota: Arc<crate::storage::temp_quota::SessionTempQuota>,
    pub(crate) temp_table_journal: Vec<TempTableJournalEntry>,
    /// CREATE/DROP TEMPORARY are not transactional in MySQL. When they occur
    /// inside an explicit or autocommit-off transaction, ROLLBACK leaves the
    /// object operation in place and reports 1751/1752.
    pub(crate) created_temp_table_not_rollbackable: bool,
    pub(crate) dropped_temp_table_not_rollbackable: bool,
    pub(crate) explicit_table_locks: HashMap<String, SessionTableLock>,
    pub(crate) view_security_overrides: Vec<ViewSecurityOverride>,
    pub(crate) cte_result_cache: std::sync::Arc<std::sync::Mutex<SessionCteResultCache>>,
    /// MySQL 8.0 CLIENT_SESSION_TRACK: pending `SESSION_TRACK_SCHEMA`
    /// entry queued by a `USE <db>` / COM_INIT_DB / `DROP DATABASE` that
    /// reset the current schema. The next OK packet sent to the client
    /// drains this into the OK payload (when the client negotiated the
    /// capability) and clears the field. `None` = no schema change to
    /// report on this OK.
    pub pending_session_track_schema: Option<String>,
    pub session_track_schema: bool,
    pub session_track_state_change: bool,
    /// MySQL 8.0 CLIENT_SESSION_TRACK aggregator bit: set whenever any
    /// tracked channel queued a payload this statement. The next OK
    /// packet ORs `SERVER_SESSION_STATE_CHANGED` into the status flags
    /// and appends a `SESSION_TRACK_STATE_CHANGE` entry, then clears
    /// the flag.
    pub session_state_changed: bool,
    /// MySQL 8.0 `session_track_system_variables`: the comma-separated
    /// list of system variables whose changes are reported back to the
    /// client via `SESSION_TRACK_SYSTEM_VARIABLES`. `"*"` tracks every
    /// variable; empty disables tracking. Seeded from the global default
    /// at connect, overridable with `SET [SESSION] session_track_system_variables`.
    pub session_track_system_variables: String,
    /// Queued `(name, new_value)` pairs for the next OK packet's
    /// `SESSION_TRACK_SYSTEM_VARIABLES` block. Filled by `SET` of a
    /// subscribed variable, drained (and the buffer cleared) by
    /// `take_session_track_payload`.
    pub pending_session_track_sysvars: Vec<(String, String)>,
    /// Per-session sysvar overrides that aren't backed by a dedicated
    /// SessionContext field. Centralising them in one struct keeps the
    /// `Default` impl + clone semantics tidy and avoids touching every
    /// call site each time a new MySQL-compat variable lands.
    pub sysvars: SessionSysVarState,
}

/// Per-session storage for the MySQL system variables that didn't have
/// a dedicated `SessionContext` field before the sysvar registry landed.
/// Fields here mirror MySQL 8.0 defaults so a fresh connection behaves
/// like a stock MySQL session.
#[derive(Debug, Clone)]
pub struct SessionSysVarState {
    pub auto_increment_increment: u64,
    pub auto_increment_offset: u64,
    pub transaction_read_only: bool,
    pub max_allowed_packet: u64,
    pub max_user_connections: u64,
    /// MySQL's connection-local state for unseeded `RAND()`. The two
    /// compatibility system variables expose the components separately,
    /// while the expression evaluator advances them atomically. `Arc`
    /// keeps short-lived session snapshots/parallel execution contexts on
    /// the same connection sequence instead of cloning a divergent RNG.
    pub rand_state: std::sync::Arc<parking_lot::Mutex<(u64, u64)>>,
    /// Per-session UNIQUE-key enforcement toggle (`SET unique_checks = 0`
    /// is the canonical bulk-load shortcut).
    pub unique_checks: bool,
    /// `innodb_strict_mode` compatibility value. mydb has no InnoDB
    /// execution semantics, but clients expect this dynamic variable to be
    /// session-scoped and to round-trip after SET.
    pub innodb_strict_mode: bool,
    /// `sql_safe_updates`: reject UPDATE/DELETE that miss a WHERE/LIMIT
    /// or don't use a key. Default OFF (MySQL).
    pub sql_safe_updates: bool,
    /// `sql_select_limit`: implicit LIMIT applied to bare SELECTs.
    /// `u64::MAX` is the MySQL DEFAULT sentinel for "no limit".
    pub sql_select_limit: u64,
    /// `max_execution_time` in milliseconds (0 = no limit).
    pub max_execution_time_ms: u64,
    /// `sql_quote_show_create`: backtick identifiers in SHOW CREATE.
    pub sql_quote_show_create: bool,
    /// GIPK metadata visibility preference. mydb does not generate GIPKs;
    /// explicit primary keys and invisible columns remain user metadata.
    pub show_gipk_in_create_table_and_information_schema: bool,
    /// `sql_notes`: accumulate NOTE-level warnings.
    pub sql_notes: bool,
    /// `sql_warnings`: emit single-row INSERTs' warning summary.
    pub sql_warnings: bool,
    /// `sql_big_selects`: prevent expected-huge SELECTs when 0.
    /// mydb advisory (no scan-row estimate gate yet); default ON.
    pub sql_big_selects: bool,
    /// `sql_buffer_result`: force materialisation of SELECT into tmp.
    /// mydb advisory; default OFF.
    pub sql_buffer_result: bool,
    /// `sql_auto_is_null`: legacy MySQL 4.x compat. Default OFF.
    pub sql_auto_is_null: bool,
    /// Prefer spill-backed implementations for internal temporary results.
    /// Operators remain governed by MYDB_MEMORY_TARGET in either mode; this
    /// compatibility flag may force an earlier spill but never changes the
    /// selected physical plan or SQL result.
    pub big_tables: bool,
    /// `optimizer_switch`: MySQL planner flags. Stored verbatim. Index Merge,
    /// invisible-index, Zone Map and LIKE-prefilter flags are consulted by
    /// their planner scopes; remaining flags are compatibility/capability
    /// reporting (see `default_optimizer_switch`).
    pub optimizer_switch: String,
    /// Cached parse of the `use_invisible_indexes` flag inside
    /// `optimizer_switch`, refreshed by the sysvar setter so the
    /// per-statement executor read is a plain bool instead of a
    /// substring scan on the hot path.
    pub use_invisible_indexes: bool,
    /// `optimizer_trace` / `optimizer_trace_offset` / `optimizer_trace_limit` —
    /// session-level dump knobs. mydb keeps them as text only.
    pub optimizer_trace: String,
    /// Session time zone — `SYSTEM` or `±HH:MM` or a named zone.
    /// Functions like `NOW()` honour it once Phase 2 time-zone work
    /// lands.
    pub time_zone: String,
    /// `timestamp` — session epoch override in seconds (fractional part
    /// allowed). `0.0` means "use the real clock". When set, NOW() /
    /// CURDATE() / CURTIME() / UTC_* / UNIX_TIMESTAMP() report this
    /// instant; SYSDATE() keeps reading the real clock (MySQL semantics).
    pub timestamp_override: f64,
    /// Legacy first-TIMESTAMP implicit DEFAULT/ON UPDATE behaviour. MySQL 8
    /// defaults this ON (explicit defaults), but keeps it session-settable.
    pub explicit_defaults_for_timestamp: bool,
    /// Session-level character-set / collation defaults for `character_set_database`
    /// and `collation_database`. Empty means "derive from current database".
    pub character_set_database: Option<String>,
    pub collation_database: Option<String>,
    /// `character_set_filesystem` — MySQL controls how filenames passed
    /// to `LOAD DATA` are interpreted. Default `binary`.
    pub character_set_filesystem: String,
    /// `default_storage_engine` / `default_tmp_storage_engine` — purely
    /// for ORM compatibility (mydb has one engine).
    pub default_storage_engine: String,
    pub default_tmp_storage_engine: String,
    /// MySQL's choice between the TempTable and MEMORY implementations for
    /// internal temporary tables. mydb's operators use their own governed
    /// spill implementation, but retaining the session value keeps the
    /// compatibility variable truthful and available for future routing.
    pub internal_tmp_mem_storage_engine: String,
    /// `range_optimizer_max_mem_size` — advisory, mydb doesn't gate on it.
    pub range_optimizer_max_mem_size: u64,
    /// Maximum age of INFORMATION_SCHEMA statistics in seconds. Metadata reads
    /// currently bypass any expiry cache and always consult the storage engine.
    pub information_schema_stats_expiry: u64,
    /// `tmp_table_size` / `max_heap_table_size` — advisory.
    pub tmp_table_size: u64,
    pub max_heap_table_size: u64,
    /// Per-connection buffer sizes — advisory.
    pub sort_buffer_size: u64,
    pub join_buffer_size: u64,
    pub read_buffer_size: u64,
    pub read_rnd_buffer_size: u64,
    /// Server-side SELECT export buffering and durability controls.
    pub select_into_buffer_size: u64,
    pub select_into_disk_sync: bool,
    pub select_into_disk_sync_delay_ms: u64,
    pub bulk_insert_buffer_size: u64,
    pub net_buffer_length: u64,
    pub net_read_timeout: u64,
    pub net_write_timeout: u64,
    pub wait_timeout: u64,
    pub interactive_timeout: u64,
    /// Block encryption mode for `AES_ENCRYPT` family.
    pub block_encryption_mode: String,
    /// `default_week_format` — implicit mode for 1-arg `WEEK()` (0-7).
    pub default_week_format: u64,
    /// `lc_messages` / `lc_time_names` — localisation.
    pub lc_messages: String,
    pub lc_time_names: String,
    /// `collation_database` is mirrored above; keep `collation_server`
    /// as a session override of the global (mostly cosmetic).
    pub collation_server: String,
    pub character_set_server: String,
    /// `div_precision_increment` — extra decimal scale for `/`.
    pub div_precision_increment: u64,
    /// `transaction_alloc_block_size` — advisory.
    pub transaction_alloc_block_size: u64,
    /// `transaction_prealloc_size` — advisory.
    pub transaction_prealloc_size: u64,
    /// `completion_type` — default COMMIT/ROLLBACK completion behaviour:
    /// 0 = NO_CHAIN (+ NO_RELEASE), 1 = CHAIN, 2 = RELEASE. Explicit
    /// `AND [NO] CHAIN` / `[NO] RELEASE` clauses on the statement override it.
    pub completion_type: u8,
    /// `innodb_lock_wait_timeout` — per-session row-lock wait timeout in
    /// seconds; drives the storage-layer wait loop (M3). `None` = inherit the
    /// database-level `config.lock_wait` default (no override); `Some(secs)` =
    /// the session explicitly `SET` it.
    pub innodb_lock_wait_timeout: Option<u64>,
    /// `lock_wait_timeout` — metadata-lock (MDL) wait timeout in seconds.
    /// `None` = inherit the global default. MySQL default 31536000 (1 year).
    pub lock_wait_timeout: Option<u64>,
    /// Local copy of `eq_range_index_dive_limit` — advisory.
    pub eq_range_index_dive_limit: u64,
    /// `range_alloc_block_size` — advisory.
    pub range_alloc_block_size: u64,
    /// `query_alloc_block_size` / `query_prealloc_size` — advisory.
    pub query_alloc_block_size: u64,
    pub query_prealloc_size: u64,
    /// `mydb_hnsw_ef_search` — base candidate-pool size for KNN over an
    /// HNSW index. Effective value at plan time is
    /// `max(ef_search, k * ef_dynamic_factor)`. Per-query
    /// `/*+ HNSW_EF(n) */` overrides this. Default 64.
    pub mydb_hnsw_ef_search: u64,
    /// `mydb_hnsw_ef_dynamic_factor` — multiplier that scales `ef_search`
    /// with the user's `LIMIT k`. Set to 0 to disable scaling. Default 4.
    pub mydb_hnsw_ef_dynamic_factor: u64,
    /// `mydb_hnsw_iterative_scan` — filter-aware widening policy when a
    /// residual WHERE drops candidates below k:
    /// * `off` — single pass, accept short results.
    /// * `relaxed_order` — widen aggressively, do not preserve distance order beyond k.
    /// * `strict_order` — widen and re-sort to keep strict ascending distance order (default).
    pub mydb_hnsw_iterative_scan: String,
    /// Distinguishes an explicit `SET mydb_hnsw_ef_search = 64` from the
    /// untouched built-in default, so it can override a per-index `EF_SEARCH`.
    pub mydb_hnsw_ef_search_explicit: bool,
    /// `mydb_hnsw_max_scan_tuples` — iterative-scan upper bound on the
    /// candidate pool. Default 20000.
    pub mydb_hnsw_max_scan_tuples: u64,
    /// `mydb_hnsw_brute_threshold` — table row-count threshold below
    /// which the planner skips HNSW and falls back to brute (seq scan +
    /// sort). Default 10000.
    pub mydb_hnsw_brute_threshold: u64,
    /// `mydb_hnsw_brute_filter_rows` — estimated filter-output row
    /// count below which the planner skips HNSW. Default 1000.
    pub mydb_hnsw_brute_filter_rows: u64,
    /// `mydb_ivf_nprobe` — number of inverted-file cells an IVF KNN query
    /// probes. Query-time effort knob (the IVF analogue of `ef_search`).
    /// Per-query `/*+ IVF_NPROBE(n) */` and a per-index `NPROBE=` override
    /// this. Default 8.
    pub mydb_ivf_nprobe: u64,
    /// Distinguishes an explicit `SET ... = 8` from the untouched built-in
    /// default, so it can override a per-index `NPROBE` of another value.
    pub mydb_ivf_nprobe_explicit: bool,
    /// Exact heap rerank pool multiplier, independent of `nprobe`.
    pub mydb_ivf_rerank_mult: u64,
    pub mydb_ivf_rerank_mult_explicit: bool,
    pub mydb_hnsw_rerank_mult: u64,
    pub mydb_hnsw_rerank_mult_explicit: bool,
    /// IVF-specific iterative widening policy.
    pub mydb_ivf_iterative_scan: String,
    /// IVF-specific tuple budget across iterative candidate widening.
    pub mydb_ivf_max_scan_tuples: u64,
    /// Session override for executor worker parallelism.
    pub mydb_parallel_degree: u64,
}

impl Default for SessionSysVarState {
    fn default() -> Self {
        let (server_charset, server_collation) =
            crate::engine::sysvar::builtin::charset::server_charset_defaults();
        const RAND_MAX: u64 = 0x3FFF_FFFF;
        Self {
            max_allowed_packet:
                crate::engine::sysvar::builtin::connection::max_allowed_packet_bytes(),
            max_user_connections: crate::engine::sysvar::builtin::connection::max_user_connections(
            ),

            rand_state: std::sync::Arc::new(parking_lot::Mutex::new((
                u64::from(rand::random::<u32>()) % RAND_MAX,
                u64::from(rand::random::<u32>()) % RAND_MAX,
            ))),
            unique_checks: true,
            auto_increment_increment: 1,
            auto_increment_offset: 1,
            transaction_read_only: global_transaction_read_only(),
            innodb_strict_mode:
                crate::engine::sysvar::builtin::orm_stubs::global_innodb_strict_mode(),
            sql_safe_updates: false,
            sql_select_limit: crate::engine::sysvar::builtin::query::global_sql_select_limit(),
            max_execution_time_ms: 0,
            sql_quote_show_create: true,
            show_gipk_in_create_table_and_information_schema: true,
            sql_notes: true,
            sql_warnings: false,
            sql_big_selects: true,
            sql_buffer_result: false,
            sql_auto_is_null: false,
            big_tables: false,
            optimizer_switch: default_optimizer_switch().to_string(),
            use_invisible_indexes: false,
            optimizer_trace: "enabled=off,one_line=off".to_string(),
            time_zone: crate::engine::sysvar::builtin::tz::global_time_zone(),
            timestamp_override: 0.0,
            explicit_defaults_for_timestamp:
                crate::engine::sysvar::builtin::tz::global_explicit_defaults_for_timestamp(),
            character_set_database: None,
            collation_database: None,
            character_set_filesystem: "binary".to_string(),
            default_storage_engine: "InnoDB".to_string(),
            default_tmp_storage_engine: "InnoDB".to_string(),
            internal_tmp_mem_storage_engine: "TempTable".to_string(),
            range_optimizer_max_mem_size: 8 * 1024 * 1024,
            information_schema_stats_expiry: 86_400,
            tmp_table_size: 16 * 1024 * 1024,
            max_heap_table_size: 16 * 1024 * 1024,
            sort_buffer_size: 262_144,
            join_buffer_size: 262_144,
            read_buffer_size: 131_072,
            read_rnd_buffer_size: 262_144,
            select_into_buffer_size:
                crate::engine::sysvar::builtin::query::global_select_into_buffer_size(),
            select_into_disk_sync:
                crate::engine::sysvar::builtin::query::global_select_into_disk_sync(),
            select_into_disk_sync_delay_ms:
                crate::engine::sysvar::builtin::query::global_select_into_disk_sync_delay(),
            bulk_insert_buffer_size: 8 * 1024 * 1024,
            net_buffer_length: crate::engine::sysvar::builtin::connection::net_buffer_length(),
            net_read_timeout: crate::engine::sysvar::builtin::connection::net_read_timeout_secs(),
            net_write_timeout: crate::engine::sysvar::builtin::connection::net_write_timeout_secs(),
            wait_timeout: crate::engine::sysvar::builtin::connection::wait_timeout_secs(),
            interactive_timeout:
                crate::engine::sysvar::builtin::connection::interactive_timeout_secs(),
            block_encryption_mode:
                crate::engine::sysvar::builtin::charset::global_block_encryption_mode(),
            default_week_format: 0,
            lc_messages: "en_US".to_string(),
            lc_time_names: "en_US".to_string(),
            collation_server: server_collation.to_string(),
            character_set_server: server_charset.to_string(),
            div_precision_increment: 4,
            transaction_alloc_block_size: 8192,
            transaction_prealloc_size: 4096,
            completion_type: 0,
            innodb_lock_wait_timeout: None,
            lock_wait_timeout: None,
            eq_range_index_dive_limit: 200,
            range_alloc_block_size: 4096,
            query_alloc_block_size: 8192,
            query_prealloc_size: 8192,
            // M5.1/M5.2/M5.3 — HNSW knobs. Each accepts an env-var
            // override so operators can pin defaults at startup and tests
            // can scope an override with `crate::util::env::with_vars`.
            // The env names match the GUC table in CLAUDE.md.
            mydb_hnsw_ef_search: hnsw_env_u64("MYDB_HNSW_EF_SEARCH", 64),
            mydb_hnsw_ef_dynamic_factor: hnsw_env_u64("MYDB_HNSW_EF_DYNAMIC_FACTOR", 4),
            mydb_hnsw_iterative_scan: hnsw_env_iterative(),
            mydb_hnsw_ef_search_explicit: false,
            mydb_hnsw_max_scan_tuples: hnsw_env_u64("MYDB_HNSW_MAX_SCAN_TUPLES", 20_000),
            mydb_hnsw_brute_threshold: hnsw_env_u64("MYDB_HNSW_BRUTE_THRESHOLD", 10_000),
            mydb_hnsw_brute_filter_rows: hnsw_env_u64("MYDB_HNSW_BRUTE_FILTER_ROWS", 1_000),
            mydb_ivf_nprobe: hnsw_env_u64("MYDB_IVF_NPROBE", 8),
            mydb_ivf_nprobe_explicit: false,
            mydb_ivf_rerank_mult: 4,
            mydb_ivf_rerank_mult_explicit: false,
            mydb_hnsw_rerank_mult: 4,
            mydb_hnsw_rerank_mult_explicit: false,
            mydb_ivf_iterative_scan: vector_env_iterative("MYDB_IVF_ITERATIVE_SCAN"),
            mydb_ivf_max_scan_tuples: hnsw_env_u64("MYDB_IVF_MAX_SCAN_TUPLES", 20_000),
            mydb_parallel_degree: u64::from(crate::planner::default_parallel_degree()),
        }
    }
}

fn hnsw_env_u64(name: &str, default: u64) -> u64 {
    // Release builds cache per knob: this runs inside
    // `SessionSysVarState::default()`, which the session layer may build
    // far more often than once per connection, and `getenv` serializes on
    // the libc environ lock. Test builds read fresh so
    // `util::env::with_vars` overrides keep working.
    #[cfg(not(test))]
    {
        static CACHE: std::sync::OnceLock<parking_lot::Mutex<Vec<(String, u64)>>> =
            std::sync::OnceLock::new();
        let cache = CACHE.get_or_init(|| parking_lot::Mutex::new(Vec::new()));
        let mut cache = cache.lock();
        if let Some((_, value)) = cache.iter().find(|(k, _)| k == name) {
            return *value;
        }
        let value = hnsw_env_u64_uncached(name, default);
        cache.push((name.to_string(), value));
        value
    }
    #[cfg(test)]
    hnsw_env_u64_uncached(name, default)
}

fn hnsw_env_u64_uncached(name: &str, default: u64) -> u64 {
    crate::util::env::var(name)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn hnsw_env_iterative() -> String {
    vector_env_iterative("MYDB_HNSW_ITERATIVE_SCAN")
}

fn vector_env_iterative(name: &str) -> String {
    crate::util::env::var(name)
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| matches!(s.as_str(), "off" | "relaxed_order" | "strict_order"))
        .unwrap_or_else(|| "strict_order".to_string())
}

pub(crate) fn default_optimizer_switch() -> &'static str {
    // MySQL 8.0 flag list in canonical order. Honesty rule: a flag
    // defaults to `on` only when mydb actually performs the behaviour —
    // skip_scan is not implemented yet and therefore remains honest `off`.
    // Index Merge is implemented by the planner/executor and follows the
    // MySQL 8.0 default-on surface. `index_condition_pushdown` is also real:
    // executor pre-filters index entries before the heap lookup
    // (`read_plan_table_ids_range_icp`, gated by `MYDB_ICP_DISABLE`), so it
    // is reported `on` to match both MySQL and mydb's actual behaviour.
    // `use_invisible_indexes` is the only flag whose *toggle* the planner
    // consults (via `InvisibleIndexScope`); the rest describe capabilities
    // mydb performs but does not expose a runtime on/off switch for.
    "index_merge=on,index_merge_union=on,index_merge_sort_union=on,\
     index_merge_intersection=on,engine_condition_pushdown=on,\
     index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,\
     batched_key_access=off,materialization=on,semijoin=on,loosescan=on,\
     firstmatch=on,duplicateweedout=on,subquery_materialization_cost_based=on,\
     use_index_extensions=on,condition_fanout_filter=on,derived_merge=on,\
     use_invisible_indexes=off,skip_scan=off,hash_join=on,subquery_to_derived=off,\
     prefer_ordering_index=on,hypergraph_optimizer=off,derived_condition_pushdown=on,zone_map=on,\
     like_ngram_prefilter=on"
}

#[derive(Debug, Clone)]
pub(crate) struct ViewSecurityOverride {
    pub(crate) database: String,
    pub(crate) table: String,
    pub(crate) context: AuthorizationContext,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionTempTable {
    pub(crate) internal_key: String,
    pub(crate) on_commit: TempTableOnCommit,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionSqlPreparedStatement {
    pub(crate) pfs: crate::engine::session_registry::PreparedStatementSnapshot,
    /// Retained source text for diagnostics/reprepare and exact capacity
    /// accounting (`Arc<str>` has no spare capacity).
    pub(crate) original_sql: std::sync::Arc<str>,
    pub(crate) parser_sql_mode_bits: u16,
    /// Parsed and placeholder-numbered once at PREPARE time. EXECUTE clones
    /// the compact AST but never reparses the SQL text.
    pub(crate) statement: std::sync::Arc<crate::parser::Statement>,
    /// Default schema captured at PREPARE time, matching session prepared
    /// statement name resolution when EXECUTE occurs from another routine.
    pub(crate) database: String,
    /// Exact number of numbered placeholders, checked before EXECUTE so a
    /// mismatch returns ER_WRONG_ARGUMENTS without entering an executor.
    pub(crate) parameter_count: usize,
    /// Result metadata bound at PREPARE time. Declared domains stay fixed;
    /// untyped parameter-derived domains are resolved at EXECUTE time.
    pub(crate) result_columns: std::sync::Arc<[crate::types::Column]>,
    /// Hard-governor reservation for all retained template capacity. Shared
    /// with transient EXECUTE clones and released by the last owner.
    pub(crate) memory_charge:
        std::sync::Arc<crate::engine::executor::mem_budget::ControlledMemoryCharge>,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionTableLock {
    pub(crate) table: String,
    pub(crate) alias: Option<String>,
    pub(crate) mode: crate::parser::LockTableMode,
}

#[derive(Debug, Clone)]
pub(crate) enum TempTableJournalEntry {
    Create {
        visible_key: String,
        internal_key: String,
    },
    Drop {
        visible_key: String,
        internal_key: String,
        on_commit: TempTableOnCommit,
    },
}

/// Backing for a CTE result whose rows were spilled to disk instead of
/// held in memory (see `cte::CteSpillBuilder`). Owned by the
/// `CteResult` in the CTE registry, so the temp file lives exactly as
/// long as the CTE is referenceable and is cleaned up (RAII +
/// temp-disk-quota release) when the registry drops the result.
pub struct CteSpillFile {
    /// RAII guard — removes the temp directory and releases its
    /// temp-disk-quota bytes on drop.
    pub(crate) _dir: std::sync::Arc<crate::engine::executor::select::grace::SpillTmpDir>,
    pub(crate) path: std::path::PathBuf,
    pub(crate) row_count: usize,
}

impl std::fmt::Debug for CteSpillFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CteSpillFile")
            .field("rows", &self.row_count)
            .field("path", &self.path)
            .finish()
    }
}

/// Per-column statistics gathered while materialising a small CTE (P1-4).
/// Exact over the materialised rows, with `ndv` capped: once a column
/// exceeds the distinct-value tracking cap it is recorded as `None`
/// (treated as high-cardinality ≈ row count by the planner). Consumed by the
/// CBO to refine a filtered `CteScan`'s estimated row count — never affects
/// results, only plan choice.
#[derive(Debug, Clone, Default)]
pub struct CteColumnStats {
    /// Number of non-NULL values seen in this column.
    pub non_null: u64,
    /// Number of distinct values, or `None` if the column blew past the
    /// tracking cap (high cardinality).
    pub ndv: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct CteStats {
    pub rows: u64,
    pub columns: Vec<CteColumnStats>,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionCteResultCacheEntry {
    pub(crate) snapshot_scn: u64,
    /// When `Some`, invalidation is per dependency table:
    /// `(table_id, schema_version, write_epoch)` snapshots taken at store
    /// time. The entry stays fresh while every dep table still matches, so
    /// unrelated background commits (which advance `visible_scn` but touch
    /// no dep table) don't evict it. `None` falls back to the conservative
    /// `snapshot_scn == visible_scn` check — used when the CTE's base tables
    /// can't be statically determined (e.g. a chain of CTE references).
    pub(crate) table_versions: Option<Vec<(u64, u64, u64)>>,
    pub(crate) result: std::sync::Arc<CteResult>,
    pub(crate) bytes: usize,
}

#[derive(Debug, Default)]
pub(crate) struct SessionCteResultCache {
    pub(crate) entries: HashMap<String, SessionCteResultCacheEntry>,
    pub(crate) order: std::collections::VecDeque<String>,
    pub(crate) bytes: usize,
}

#[derive(Debug, Clone)]
pub struct CteResult {
    pub columns: Vec<crate::types::Column>,
    /// Materialised rows live inside the store. Multiple references to a
    /// CTE share the same `Arc<CteStore>`; the rows themselves are never
    /// deep-copied per reference (see `cte_store.rs` for layout). Empty
    /// store when `spill` is `Some` (rows live on disk instead).
    pub store: std::sync::Arc<crate::engine::executor::cte_store::CteStore>,
    /// P1-4 per-column statistics, when gathered at materialisation.
    pub stats: Option<std::sync::Arc<CteStats>>,
    /// CBO row-count estimate used when planning references to this CTE
    /// (`CteScan` nodes). For a materialised result this is exactly the
    /// store's row count, since the body has actually been executed.
    /// During EXPLAIN — where CTEs are *not* materialised and the store
    /// is empty — this instead carries the CTE body plan's estimated row
    /// count, so `CteScan` cost/row estimates do not collapse to 0.
    pub estimated_rows: f64,
    /// When `Some`, the body's rows were spilled to disk (large CTE) and
    /// the store is empty. Consumers must read rows via the spill file.
    pub spill: Option<std::sync::Arc<CteSpillFile>>,
    /// How many times this CTE is referenced in the enclosing query (its
    /// own body's `FROM`/`JOIN` plus every sibling CTE body). Stamped by
    /// `materialize_ctes` once the whole set is built, so it reflects this
    /// query's structure even for session-cache-shared results. Drives the
    /// cross-reference hash-build cache gate in `try_hash_join`: the shared
    /// full-store build only amortises when consumed more than once.
    /// Defaults to 1 (no sharing) for results built outside the
    /// materialisation path (EXPLAIN stubs, recursive bodies).
    pub consumer_count: usize,
}

impl CteResult {
    /// Build a fully-materialised in-memory CTE result. The row-count
    /// estimate is exact (`store.nrows()`) because the body has actually
    /// been executed.
    pub fn materialized(
        columns: Vec<crate::types::Column>,
        rows: Vec<Vec<crate::types::Value>>,
    ) -> Self {
        let estimated_rows = rows.len() as f64;
        let store =
            crate::engine::executor::cte_store::CteStore::from_dense_rows(columns.clone(), rows);
        Self {
            columns,
            store,
            stats: None,
            estimated_rows,
            spill: None,
            consumer_count: 1,
        }
    }

    pub(crate) fn materialized_charged(
        columns: Vec<crate::types::Column>,
        rows: Vec<Vec<crate::types::Value>>,
        memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    ) -> Self {
        let estimated_rows = rows.len() as f64;
        let store = crate::engine::executor::cte_store::CteStore::from_dense_rows_charged(
            columns.clone(),
            rows,
            memory_charge,
        );
        Self {
            columns,
            store,
            stats: None,
            estimated_rows,
            spill: None,
            consumer_count: 1,
        }
    }

    /// Attach per-column statistics (P1-4). Builder-style; returns self.
    pub fn with_stats(mut self, stats: Option<std::sync::Arc<CteStats>>) -> Self {
        self.stats = stats;
        self
    }

    /// Build a CTE result whose rows were spilled to disk. The store is
    /// empty; `estimated_rows` carries the real spilled row count.
    pub fn spilled(
        columns: Vec<crate::types::Column>,
        spill: std::sync::Arc<CteSpillFile>,
    ) -> Self {
        let estimated_rows = spill.row_count as f64;
        let store = crate::engine::executor::cte_store::CteStore::empty(columns.clone());
        Self {
            columns,
            store,
            stats: None,
            estimated_rows,
            spill: Some(spill),
            consumer_count: 1,
        }
    }

    /// Build an empty stub used by EXPLAIN paths: the CTE body is *not*
    /// executed; `estimated_rows` carries the planner's row-count guess
    /// so downstream `CteScan` cost estimates stay non-zero. The store is
    /// empty (no rows materialised).
    pub fn stub_for_explain(columns: Vec<crate::types::Column>, estimated_rows: f64) -> Self {
        let store = crate::engine::executor::cte_store::CteStore::empty(columns.clone());
        Self {
            columns,
            store,
            stats: None,
            estimated_rows,
            spill: None,
            consumer_count: 1,
        }
    }

    /// Borrow the materialised row set. When the CTE was spilled the
    /// returned slice is empty (callers must check `self.spill`).
    #[inline]
    pub fn rows(&self) -> &[Vec<crate::types::Value>] {
        self.store.dense_rows()
    }

    /// Number of materialised rows. Zero when spilled (use the spill
    /// file's `row_count` for the on-disk count).
    #[inline]
    pub fn nrows(&self) -> usize {
        self.store.nrows()
    }
}

impl Default for SessionContext {
    fn default() -> Self {
        let mut session = Self::without_sysvar_inheritance();
        crate::engine::sysvar::registry().inherit_session_defaults(&mut session);
        session
    }
}

impl SessionContext {
    pub(crate) fn without_sysvar_inheritance() -> Self {
        let sql_mode = crate::engine::sysvar::builtin::query::global_sql_mode();
        let temporal_sql_mode_bits =
            parse_temporal_sql_mode_bits(&sql_mode) | TEMPORAL_MODE_EXPLICIT;
        let sysvars = SessionSysVarState::default();
        Self {
            autocommit: true,
            isolation: global_transaction_isolation(),
            next_txn_isolation: None,
            next_txn_read_only: None,
            active_txn_isolation: None,
            current_database: DEFAULT_DATABASE.to_string(),
            user: "root".to_string(),
            host: "localhost".to_string(),
            authenticated_as: AccountKey::new("root", "localhost"),
            login_user: "root".to_string(),
            active_roles: Default::default(),
            auth_epoch: 0,
            password_expired: false,
            account_locked: false,
            auth_sandbox: false,
            auth_factors_done: 0,
            account_limits: ResourceLimits::default(),
            current_user_is_system: false,
            internal_system_session: false,
            connection_id: 1,
            status: None,
            ssl_cipher: None,
            ssl_version: None,
            x509_subject: None,
            x509_issuer: None,
            user_variables: HashMap::new(),
            pfs_variables_dirty: true,
            pfs_variable_dirty_name: None,
            pfs_global_variable_epoch: 0,
            sql_prepared_statements: SqlPreparedStatements::default(),
            last_insert_id: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            statement_insert_id: 0,
            pending_statement_info: None,
            pending_statement_matched_rows: None,
            seq_state: std::cell::RefCell::new(Default::default()),
            last_row_count: 0,
            diagnostics_row_count: 0,
            found_rows: 0,
            current_txn: None,
            txn_runtime: None,
            spare_txn_runtime: None,
            mdl_fast_cache: std::cell::RefCell::new(Default::default()),
            txn_snapshot_scn: None,
            txn_snapshot_guard: None,
            binlog_snapshot_status: None,
            explicit_txn: false,
            read_only_txn: sysvars.transaction_read_only,
            write_intent_seen: false,
            close_after_response: false,
            xa_branch: None,
            mdl_txn_grants: std::cell::RefCell::new((0, Vec::new())),
            mdl_txn_generation: 1,
            routine_mdl_depth: 0,
            routine_atomic_depth: 0,
            cte_max_recursion_depth: 1000,
            max_sp_recursion_depth: 0,
            foreign_key_checks: true,
            sql_log_off: false,
            sql_log_bin: true,
            redo_enabled: crate::engine::sysvar::builtin::mydb_storage::global_redo_enabled(),
            group_concat_max_len: 1024,
            sql_mode,
            temporal_sql_mode_bits,
            character_set_client: crate::charset::DEFAULT_CHARSET.to_string(),
            character_set_connection: crate::charset::DEFAULT_CHARSET.to_string(),
            character_set_results: Some(crate::charset::DEFAULT_CHARSET.to_string()),
            collation_connection: crate::charset::DEFAULT_COLLATION.to_string(),
            explain_format: global_explain_format(),
            cancel_handle: None,
            query_cancel_generation: 0,
            connection_kill_generation: 0,
            current_query_token: None,
            warnings: SqlWarnings::new(),
            connection_attrs: std::collections::BTreeMap::new(),
            statement_query_attributes: std::collections::BTreeMap::new(),
            resultset_metadata_full: true,
            vector_result_format_text: false,
            temp_tables: HashMap::new(),
            temp_quota: Arc::new(crate::storage::temp_quota::SessionTempQuota::default()),
            temp_table_journal: Vec::new(),
            created_temp_table_not_rollbackable: false,
            dropped_temp_table_not_rollbackable: false,
            explicit_table_locks: HashMap::new(),
            view_security_overrides: Vec::new(),
            cte_result_cache: std::sync::Arc::new(std::sync::Mutex::new(
                SessionCteResultCache::default(),
            )),
            pending_session_track_schema: None,
            session_track_schema: true,
            session_track_state_change: false,
            session_state_changed: false,
            session_track_system_variables:
                crate::engine::sysvar::builtin::connection::session_track_system_variables_default(),
            pending_session_track_sysvars: Vec::new(),
            sysvars,
        }
    }
}

impl SessionContext {
    /// Build the post-COM_RESET_CONNECTION context. MySQL resets SQL/session
    /// state to fresh-connection defaults but keeps the authenticated account,
    /// transport identity, connection attributes and current default schema.
    pub fn fresh_after_connection_reset(&self) -> Self {
        let mut fresh = Self::default();
        fresh.current_database = self.current_database.clone();
        fresh.user = self.user.clone();
        fresh.login_user = self.login_user.clone();
        fresh.host = self.host.clone();
        fresh.authenticated_as = self.authenticated_as.clone();
        fresh.active_roles = self.active_roles.clone();
        fresh.auth_epoch = self.auth_epoch;
        fresh.password_expired = self.password_expired;
        fresh.account_locked = self.account_locked;
        fresh.auth_sandbox = self.auth_sandbox;
        fresh.auth_factors_done = self.auth_factors_done;
        fresh.account_limits = self.account_limits.clone();
        fresh.current_user_is_system = self.current_user_is_system;
        fresh.internal_system_session = self.internal_system_session;
        fresh.connection_id = self.connection_id;
        fresh.status = self.status.clone();
        fresh.ssl_cipher = self.ssl_cipher.clone();
        fresh.ssl_version = self.ssl_version.clone();
        fresh.x509_subject = self.x509_subject.clone();
        fresh.x509_issuer = self.x509_issuer.clone();
        fresh.connection_attrs = self.connection_attrs.clone();
        fresh.cancel_handle = self.cancel_handle.clone();
        fresh.refresh_cancel_generations();
        fresh
    }

    /// Pin a transaction-wide consistent-read snapshot until COMMIT or
    /// ROLLBACK. Calling this again for the same SCN is intentionally a no-op
    /// so every executor fast path can establish the invariant cheaply.
    pub(crate) fn pin_transaction_snapshot(&mut self, db: &Arc<Database>, snapshot_scn: u64) {
        if self.txn_snapshot_scn == Some(snapshot_scn) && self.txn_snapshot_guard.is_some() {
            return;
        }
        self.txn_snapshot_guard = Some(Arc::new(db.register_statement_snapshot(snapshot_scn)));
        self.txn_snapshot_scn = Some(snapshot_scn);
    }

    /// Install a guard created atomically with snapshot selection. This avoids
    /// reopening the purge race by dropping that guard and registering the same
    /// SCN a second time.
    pub(crate) fn pin_transaction_snapshot_with_guard(
        &mut self,
        snapshot_scn: u64,
        guard: SnapshotGuard,
    ) {
        if self.txn_snapshot_scn == Some(snapshot_scn) && self.txn_snapshot_guard.is_some() {
            return;
        }
        self.txn_snapshot_guard = Some(Arc::new(guard));
        self.txn_snapshot_scn = Some(snapshot_scn);
    }

    /// End the transaction-wide read view and release its undo-retention pin.
    pub(crate) fn clear_transaction_snapshot(&mut self) {
        self.txn_snapshot_scn = None;
        self.txn_snapshot_guard = None;
    }

    /// Synthesise a privileged session for internal callers (PERSIST
    /// apply-on-startup, recovery, sandbox helpers). `current_user_is_system`
    /// bypasses every dynamic-privilege check including `SYSTEM_VARIABLES_ADMIN`.
    pub fn system_default() -> Self {
        Self {
            current_user_is_system: true,
            internal_system_session: true,
            // Recovery, replication appliers and other internal maintenance
            // must never inherit an operator's reduced-redo default.
            redo_enabled: true,
            ..Self::default()
        }
    }

    pub fn authorization_context(&self) -> AuthorizationContext {
        AuthorizationContext {
            authenticated_as: self.authenticated_as.clone(),
            active_roles: self.active_roles.clone(),
            epoch: self.auth_epoch,
        }
    }

    pub fn in_transaction(&self) -> bool {
        self.current_txn.is_some() || self.explicit_txn || self.active_txn_isolation.is_some()
    }

    pub(crate) fn note_write_intent(&mut self) {
        self.write_intent_seen = true;
    }

    /// CLIENT_SESSION_TRACK: queue a `SESSION_TRACK_SCHEMA` payload for
    /// the next OK packet. Called by `USE <db>`, COM_INIT_DB, and the
    /// `DROP DATABASE` rollback path (when the current schema vanishes).
    /// Also flips `session_state_changed` so the state-change channel
    /// fires in the same OK.
    pub fn note_schema_change(&mut self, new_schema: impl Into<String>) {
        if !self.session_track_schema {
            return;
        }
        self.pending_session_track_schema = Some(new_schema.into());
        self.session_state_changed = true;
    }

    /// CLIENT_SESSION_TRACK: queue a `SESSION_TRACK_SYSTEM_VARIABLES`
    /// entry for the next OK packet after a `SET` of a subscribed
    /// variable. Also flips `session_state_changed` so the aggregate
    /// channel fires in the same OK.
    pub fn note_sysvar_change(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.pending_session_track_sysvars
            .push((name.into(), value.into()));
        self.session_state_changed = true;
    }

    /// Whether `name` is covered by this session's
    /// `session_track_system_variables` subscription (`"*"` = all,
    /// comma-separated list otherwise, case-insensitive).
    pub fn tracks_sysvar(&self, name: &str) -> bool {
        let sub = self.session_track_system_variables.trim();
        if sub.is_empty() {
            return false;
        }
        if sub == "*" {
            return true;
        }
        sub.split(',')
            .any(|entry| entry.trim().eq_ignore_ascii_case(name))
    }

    /// Drain any queued CLIENT_SESSION_TRACK payloads into the wire
    /// `session_state_info` block. Returns the encoded bytes plus a
    /// `state_changed` flag that the caller should OR into the OK
    /// packet's status word as `SERVER_SESSION_STATE_CHANGED`. Calling
    /// this clears the queue — if the caller subsequently decides not to
    /// emit an OK packet, that state is lost (matches MySQL: the
    /// per-statement tracker resets at the OK boundary regardless).
    ///
    /// The order of entries follows MySQL 8.0's reference server: the
    /// per-channel payloads (SCHEMA, SYSTEM_VARIABLES, …) come first,
    /// then `SESSION_TRACK_STATE_CHANGE` last so a client reading
    /// linearly sees the data before the "did anything change?" bit.
    pub fn take_session_track_payload(&mut self) -> (Vec<u8>, bool) {
        let mut buf = Vec::new();
        for (name, value) in self.pending_session_track_sysvars.drain(..) {
            crate::mysql::protocol::encode_session_track_system_variables(&mut buf, &name, &value);
        }
        if let Some(schema) = self.pending_session_track_schema.take() {
            crate::mysql::protocol::encode_session_track_schema(&mut buf, &schema);
        }
        let state_changed = self.session_state_changed;
        if state_changed && self.session_track_state_change {
            crate::mysql::protocol::encode_session_track_state_change(&mut buf);
        }
        self.session_state_changed = false;
        (buf, state_changed)
    }

    pub fn effective_isolation(&self) -> IsolationLevel {
        self.active_txn_isolation
            .or(self.next_txn_isolation)
            .unwrap_or(self.isolation)
    }

    pub(crate) fn start_transaction_isolation(&mut self) -> IsolationLevel {
        if let Some(isolation) = self.active_txn_isolation {
            return isolation;
        }
        if let Some(read_only) = self.next_txn_read_only.take() {
            self.read_only_txn = read_only;
        }
        let isolation = self.next_txn_isolation.take().unwrap_or(self.isolation);
        self.active_txn_isolation = Some(isolation);
        isolation
    }

    pub(crate) fn autocommit_statement_isolation(&mut self) -> IsolationLevel {
        // The read-view fast path completes a whole autocommit transaction.
        // Consume its one-shot access mode too, without changing the session
        // default used by the following transaction.
        self.next_txn_read_only = None;
        self.next_txn_isolation.take().unwrap_or(self.isolation)
    }

    pub(crate) fn clear_active_transaction_isolation(&mut self) {
        if let Some(handle) = &self.cancel_handle {
            handle.session_handle_ref().clear_lazy_transaction();
        }
        let previous = self.effective_isolation();
        self.active_txn_isolation = None;
        if self.effective_isolation() != previous {
            self.mark_pfs_variables_dirty();
        }
    }

    pub(crate) fn temp_visible_key(&self, name: &str) -> String {
        let (database, table) = crate::storage::split_table_name(&self.current_database, name);
        crate::storage::table_key(&database, &table)
    }

    pub(crate) fn resolve_temp_table_name(&self, name: &str) -> Option<String> {
        // Hot path: avoid the three string allocations inside
        // `temp_visible_key` when the session has no temporary tables.
        // For prepared-OLTP workloads (TPC-C, sysbench) this skips
        // most table-reference checks entirely.
        if self.temp_tables.is_empty() {
            return None;
        }
        let key = self.temp_visible_key(name);
        self.temp_tables
            .get(&key)
            .map(|table| table.internal_key.clone())
    }

    pub(crate) fn has_temp_table(&self, name: &str) -> bool {
        if self.temp_tables.is_empty() {
            return false;
        }
        self.temp_tables.contains_key(&self.temp_visible_key(name))
    }

    pub(crate) fn has_explicit_table_locks(&self) -> bool {
        !self.explicit_table_locks.is_empty()
    }

    pub(crate) fn add_temp_table(
        &mut self,
        visible_key: String,
        internal_key: String,
        on_commit: TempTableOnCommit,
    ) {
        self.temp_tables.insert(
            visible_key,
            SessionTempTable {
                internal_key,
                on_commit,
            },
        );
    }

    pub fn set_user_variable(&mut self, name: &str, value: Value) {
        self.user_variables
            .insert(normalize_user_variable_name(name), value);
        self.mark_pfs_variables_dirty();
    }

    pub(crate) fn mark_pfs_variables_dirty(&mut self) {
        self.pfs_variables_dirty = true;
        self.pfs_variable_dirty_name = None;
    }

    /// Record one canonical session-system-variable mutation without forcing
    /// a full registry snapshot.  More than one distinct variable changed in
    /// the same command safely falls back to the full publication path.
    pub(crate) fn mark_pfs_variable_dirty(&mut self, name: &'static str) {
        if self.pfs_variables_dirty {
            return;
        }
        match self.pfs_variable_dirty_name {
            None => self.pfs_variable_dirty_name = Some(name),
            Some(current) if current == name => {}
            Some(_) => self.mark_pfs_variables_dirty(),
        }
    }

    pub(crate) fn pfs_variables_need_publish(&self) -> bool {
        self.pfs_variables_dirty
            || self.pfs_variable_dirty_name.is_some()
            || self.pfs_global_variable_epoch != crate::engine::sysvar::pfs_global_variable_epoch()
    }

    pub(crate) fn pfs_incremental_variable(&self) -> Option<&'static str> {
        (!self.pfs_variables_dirty
            && self.pfs_global_variable_epoch == crate::engine::sysvar::pfs_global_variable_epoch())
        .then_some(self.pfs_variable_dirty_name)
        .flatten()
    }

    pub(crate) fn mark_pfs_variables_published(&mut self) {
        self.pfs_variables_dirty = false;
        self.pfs_variable_dirty_name = None;
        self.pfs_global_variable_epoch = crate::engine::sysvar::pfs_global_variable_epoch();
    }

    pub fn user_variable(&self, name: &str) -> Option<&Value> {
        self.user_variables.get(&normalize_user_variable_name(name))
    }

    pub fn clear_warnings(&mut self) {
        self.warnings.clear();
    }

    pub fn set_warnings(&mut self, warnings: impl Into<SqlWarnings>) {
        self.warnings = warnings.into();
    }

    pub fn warning_count(&self) -> u16 {
        self.warnings.len().min(u16::MAX as usize) as u16
    }

    pub fn error_count(&self) -> u16 {
        self.warnings
            .count_matching(|condition| condition.level() == crate::error::ConditionLevel::Error)
            .min(u16::MAX as usize) as u16
    }

    pub fn is_strict_sql_mode(&self) -> bool {
        // Same "empty sql_mode = strict default" convention as the coercion
        // single source (`constraints::coercion_strict`): an explicit STRICT_*
        // / TRADITIONAL mode is strict, and so is the default *empty* mode
        // (no EXPLICIT bit). Only an explicitly-set non-empty mode without a
        // STRICT_* flag relaxes to non-strict. The prior version scanned only
        // the raw string and so treated empty sql_mode as non-strict, which
        // disagreed with INSERT's coercion posture and let ALTER TABLE
        // narrowing silently truncate/clamp stored data (issue #60).
        self.temporal_sql_mode_bits & TEMPORAL_MODE_STRICT != 0
            || self.temporal_sql_mode_bits & TEMPORAL_MODE_EXPLICIT == 0
    }

    pub(crate) fn set_cancel_handle(&mut self, handle: Option<SessionCancelHandle>) {
        self.cancel_handle = handle;
        self.refresh_cancel_generations();
    }

    pub(crate) fn publish_sql_prepared_memory(&self) {
        let bytes = self
            .sql_prepared_statements
            .iter()
            .fold(0u64, |total, (_, prepared)| {
                debug_assert!(prepared.memory_charge.bytes() >= prepared.original_sql.len());
                total.saturating_add(prepared.memory_charge.bytes() as u64)
            });
        if let Some(handle) = &self.cancel_handle {
            handle.set_sql_prepared_memory_bytes(bytes);
            handle.session_handle_ref().publish_pfs_sql_prepared(
                self.sql_prepared_statements
                    .values()
                    .map(|prepared| prepared.pfs.clone())
                    .collect(),
            );
        }
    }

    pub(crate) fn record_sql_prepared_execute(
        &mut self,
        name: &str,
        elapsed: std::time::Duration,
        failed: bool,
    ) {
        let Some(prepared) = self
            .sql_prepared_statements
            .statements
            .get_mut(&name.to_ascii_lowercase())
        else {
            return;
        };
        let stat = &mut prepared.pfs;
        let timer = u64::try_from(elapsed.as_nanos().saturating_mul(1_000)).unwrap_or(u64::MAX);
        stat.count_execute = stat.count_execute.saturating_add(1);
        stat.sum_timer_execute = stat.sum_timer_execute.saturating_add(timer);
        stat.min_timer_execute = if stat.count_execute == 1 {
            timer
        } else {
            stat.min_timer_execute.min(timer)
        };
        stat.max_timer_execute = stat.max_timer_execute.max(timer);
        stat.sum_errors = stat.sum_errors.saturating_add(u64::from(failed));
        if let Some(handle) = &self.cancel_handle {
            handle
                .session_handle_ref()
                .publish_pfs_prepared_statement(stat.clone());
        }
    }

    pub(crate) fn remove_sql_prepared_statement(&mut self, name: &str) -> bool {
        let removed = self
            .sql_prepared_statements
            .statements
            .remove(name)
            .is_some();
        if removed {
            if self.sql_prepared_statements.accounted.remove(name) {
                crate::engine::status_counters::adjust_prepared_stmt_count(-1);
            }
            self.publish_sql_prepared_memory();
        }
        removed
    }

    pub(crate) fn insert_sql_prepared_statement(
        &mut self,
        name: String,
        prepared: SessionSqlPreparedStatement,
    ) -> Result<(), QueryError> {
        let cap = crate::engine::sysvar::builtin::orm_stubs::max_prepared_stmt_count();
        if !crate::engine::status_counters::try_reserve_prepared_stmt(cap) {
            return Err(crate::error::catalog::ER_MAX_PREPARED_STMT_COUNT_REACHED.error(
                format!("Can't create more than max_prepared_stmt_count statements (current value: {cap})"),
            ));
        }
        let replaced = self
            .sql_prepared_statements
            .statements
            .insert(name.clone(), prepared);
        debug_assert!(
            replaced.is_none(),
            "SQL PREPARE must remove the old name first"
        );
        self.sql_prepared_statements.accounted.insert(name);
        self.publish_sql_prepared_memory();
        Ok(())
    }

    pub(crate) fn clear_sql_prepared_statements(&mut self) {
        let live = self.sql_prepared_statements.accounted.len() as i64;
        self.sql_prepared_statements.statements.clear();
        self.sql_prepared_statements.accounted.clear();
        crate::engine::status_counters::adjust_prepared_stmt_count(-live);
        self.publish_sql_prepared_memory();
    }

    /// Set the session's current transaction id, mirroring it onto the session
    /// registry handle so `performance_schema.data_locks` can join a held lock
    /// back to this connection's `THREAD_ID`. Use this instead of assigning
    /// `current_txn` directly at transaction begin / commit / rollback.
    pub(crate) fn set_current_txn(&mut self, txn: Option<u64>) {
        if txn != self.current_txn {
            if self.autocommit && !self.explicit_txn && self.xa_branch.is_none() {
                if let Some(runtime) = self.txn_runtime.take() {
                    self.spare_txn_runtime = Some(runtime);
                }
            } else {
                self.txn_runtime = None;
                self.spare_txn_runtime = None;
            }
        }
        self.current_txn = txn;
        if let Some(handle) = &self.cancel_handle {
            handle.set_current_txn(txn);
        }
    }

    /// Return the runtime owned by `txn_id`, creating and charging its fixed
    /// resident footprint on first use. The charge is SystemMetadata because
    /// this state is mandatory transaction bookkeeping and cannot spill.
    pub(crate) fn ensure_txn_runtime(
        &mut self,
        txn_id: u64,
    ) -> Result<Arc<TxnRuntime>, QueryError> {
        if self.current_txn != Some(txn_id) {
            return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                .error("transaction runtime does not match the active transaction"));
        }
        if let Some(runtime) = self
            .txn_runtime
            .as_ref()
            .filter(|runtime| runtime.txn_id == txn_id)
        {
            return Ok(Arc::clone(runtime));
        }
        let runtime = self.spare_txn_runtime.take().and_then(|mut runtime| {
            Arc::get_mut(&mut runtime)?.reset_for(txn_id);
            Some(runtime)
        });
        let runtime = match runtime {
            Some(runtime) => runtime,
            None => Arc::new(TxnRuntime::try_new(txn_id)?),
        };
        self.txn_runtime = Some(Arc::clone(&runtime));
        Ok(runtime)
    }

    /// Advance the lazy-transaction MDL memo epoch. Called at transaction
    /// boundaries that may not allocate a storage txn id (plain BEGIN,
    /// COMMIT/ROLLBACK of a read-only transaction, SET autocommit=0).
    pub(crate) fn advance_mdl_txn_generation(&mut self) {
        self.mdl_txn_generation = self.mdl_txn_generation.wrapping_add(1).max(1);
        let mut memo = self.mdl_txn_grants.borrow_mut();
        memo.0 = self.mdl_txn_generation;
        memo.1.clear();
    }

    /// Re-snapshot the cancel generations onto this session and rebuild
    /// the live [`QueryCancelToken`]. Called from the MySQL session loop
    /// once per command — typically at the very top, before any work that
    /// could check `check_cancelled` runs — so the baseline is the
    /// instant before the command begins. **Don't call this from inside a
    /// running statement** (subqueries, stored programs, triggers) — that
    /// would advance the baseline past any KILL that landed mid-stmt and
    /// effectively eat the cancel request.
    pub(crate) fn refresh_cancel_generations(&mut self) {
        if let Some(handle) = &self.cancel_handle {
            self.query_cancel_generation = handle.query_cancel_generation();
            self.connection_kill_generation = handle.connection_kill_generation();
            // Steady state (no KILL since the last statement): the
            // existing token is bound to the same handle with the same
            // generation baselines and has not latched, so re-minting
            // would produce an identical token. Reuse it and skip the
            // per-command `Arc<Inner>` alloc/free.
            if self.current_query_token.as_ref().is_some_and(|token| {
                token.is_reusable_for(
                    handle.session_handle_ref(),
                    self.query_cancel_generation,
                    self.connection_kill_generation,
                )
            }) {
                return;
            }
            self.current_query_token =
                Some(QueryCancelToken::from_session(handle.session_handle_arc()));
        } else {
            self.query_cancel_generation = 0;
            self.connection_kill_generation = 0;
            self.current_query_token = None;
        }
    }

    /// Borrow the active statement-level cancel token. Returns `None`
    /// before the first `refresh_cancel_generations` (i.e. on a fresh
    /// `SessionContext` not yet bound to a session).
    pub(crate) fn current_cancel_token(&self) -> Option<&QueryCancelToken> {
        self.current_query_token.as_ref()
    }

    pub(crate) fn cancelled_error(&self) -> Option<QueryError> {
        // Single source of truth: the token's `check()` does the
        // generation comparison + latches the sticky bit, so the
        // session reading `cancelled_error` and a worker calling
        // `check_cancelled` see the same result.
        self.current_query_token
            .as_ref()
            .and_then(|t| t.check().err())
    }

    pub(crate) fn check_cancelled(&self) -> Result<(), QueryError> {
        if let Some(err) = self.cancelled_error() {
            Err(err)
        } else {
            Ok(())
        }
    }
}

pub(crate) fn normalize_user_variable_name(name: &str) -> String {
    name.trim_start_matches('@').to_ascii_lowercase()
}

pub(crate) fn with_frozen_statement_time<R>(micros: i64, f: impl FnOnce() -> R) -> R {
    struct Restore(Option<i64>);
    impl Drop for Restore {
        fn drop(&mut self) {
            STMT_NOW_MICROS.with(|c| c.set(self.0));
        }
    }
    let _restore = Restore(STMT_NOW_MICROS.with(|c| c.replace(Some(micros))));
    f()
}
