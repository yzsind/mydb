//! Process-global server status counters that back the MySQL-shaped
//! `SHOW [GLOBAL] STATUS` output (and the mirrored
//! `performance_schema.{global,session}_status` tables).
//!
//! These complement the two pre-existing data sources —
//! [`crate::storage::core::StorageStatus`] (storage-engine counters) and
//! [`crate::mysql::connection_manager::ConnectionMetrics`] (connection
//! accounting) — with the protocol / command / query-execution counters
//! that monitoring stacks (mysqld_exporter, PMM, custom dashboards) scrape
//! most. Every counter here has a genuine data source and is incremented at
//! a real event site; we never fabricate a zero-valued counter for
//! instrumentation mydb has not built.
//!
//! The single reader is [`crate::engine::perf_schema::global_status_pairs`].

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;

use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::types::QueryError;

static SERVER_START: OnceLock<Instant> = OnceLock::new();
static LAST_FLUSH_UPTIME_SECS: AtomicU64 = AtomicU64::new(0);

pub(crate) fn initialize_server_clock() {
    let _ = SERVER_START.set(Instant::now());
}

pub(crate) fn uptime_seconds() -> u64 {
    SERVER_START.get_or_init(Instant::now).elapsed().as_secs()
}

pub(crate) fn uptime_since_flush_status_seconds() -> u64 {
    uptime_seconds().saturating_sub(LAST_FLUSH_UPTIME_SECS.load(Ordering::Acquire))
}

pub(crate) fn record_flush_status() {
    LAST_FLUSH_UPTIME_SECS.store(uptime_seconds(), Ordering::Release);
}

// Session-capable status is kept per connection. Each connection is the
// overwhelmingly dominant writer to its own array, so the relaxed atomics are
// contention-free in the normal case while still allowing SHOW STATUS and PFS
// readers to take a lock-free snapshot from another runtime worker.
const SESSION_FIXED_COUNT: usize = 19;
const SESSION_COM_OFFSET: usize = SESSION_FIXED_COUNT;
pub(crate) const SESSION_STATUS_COUNTER_COUNT: usize = SESSION_FIXED_COUNT + COM_COUNT;

#[derive(Clone, Copy)]
pub(crate) enum SessionCounter {
    BytesReceived = 0,
    BytesSent,
    Questions,
    Queries,
    SlowQueries,
    RowsRead,
    HandlerReadRndNext,
    HandlerWrite,
    HandlerUpdate,
    HandlerDelete,
    CreatedTmpFiles,
    CreatedTmpTables,
    CreatedTmpDiskTables,
    SortMergePasses,
    HandlerCommit,
    HandlerRollback,
    SequenceValues,
    SequenceCacheHits,
    SequenceRefills,
}

#[allow(clippy::declare_interior_mutable_const)]
const SESSION_ZERO: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(crate) struct SessionStatus {
    counters: [AtomicU64; SESSION_STATUS_COUNTER_COUNT],
    // Production sessions reserve this fixed allocation from the unified
    // governor. Embedded/test sessions may use the untracked constructor: in
    // that case the status object is short-lived harness state and never
    // enters the server registry.
    _memory_charge: Option<ControlledMemoryCharge>,
}

impl SessionStatus {
    pub(crate) fn try_new_governed() -> Result<Arc<Self>, QueryError> {
        let bytes = std::mem::size_of::<Self>().saturating_add(2 * std::mem::size_of::<usize>()); // Arc strong/weak header
        let charge =
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::SystemMetadata, bytes)?;
        Ok(Arc::new(Self {
            counters: [SESSION_ZERO; SESSION_STATUS_COUNTER_COUNT],
            _memory_charge: Some(charge),
        }))
    }

    #[cfg(test)]
    pub(crate) fn new_untracked() -> Arc<Self> {
        Arc::new(Self {
            counters: [SESSION_ZERO; SESSION_STATUS_COUNTER_COUNT],
            _memory_charge: None,
        })
    }

    #[inline]
    pub(crate) fn add(&self, counter: SessionCounter, value: u64) {
        if value != 0 {
            self.counters[counter as usize].fetch_add(value, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn record_com(&self, kind: ComKind) {
        self.counters[SESSION_COM_OFFSET + kind as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> SessionStatusValues {
        let mut values = [0; SESSION_STATUS_COUNTER_COUNT];
        for (value, counter) in values.iter_mut().zip(&self.counters) {
            *value = counter.load(Ordering::Relaxed);
        }
        SessionStatusValues { values }
    }

    pub(crate) fn reset(&self) {
        for counter in &self.counters {
            counter.store(0, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SessionStatusValues {
    values: [u64; SESSION_STATUS_COUNTER_COUNT],
}

impl Default for SessionStatusValues {
    fn default() -> Self {
        Self {
            values: [0; SESSION_STATUS_COUNTER_COUNT],
        }
    }
}

impl SessionStatusValues {
    pub(crate) fn add_snapshot(&mut self, snapshot: &SessionStatus) {
        for (value, counter) in self.values.iter_mut().zip(&snapshot.counters) {
            *value = value.saturating_add(counter.load(Ordering::Relaxed));
        }
    }

    pub(crate) fn value(&self, counter: SessionCounter) -> u64 {
        self.values[counter as usize]
    }

    pub(crate) fn add_values(&mut self, other: &Self) {
        for (value, other) in self.values.iter_mut().zip(&other.values) {
            *value = value.saturating_add(*other);
        }
    }

    pub(crate) fn session_pfs_counters(&self) -> impl Iterator<Item = (&'static str, u64)> + '_ {
        const COUNTERS: [(&str, SessionCounter); 18] = [
            ("Bytes_received", SessionCounter::BytesReceived),
            ("Bytes_sent", SessionCounter::BytesSent),
            ("Created_tmp_files", SessionCounter::CreatedTmpFiles),
            ("Created_tmp_tables", SessionCounter::CreatedTmpTables),
            (
                "Created_tmp_disk_tables",
                SessionCounter::CreatedTmpDiskTables,
            ),
            ("Handler_commit", SessionCounter::HandlerCommit),
            ("Handler_delete", SessionCounter::HandlerDelete),
            ("Handler_read_rnd_next", SessionCounter::HandlerReadRndNext),
            ("Handler_rollback", SessionCounter::HandlerRollback),
            ("Handler_update", SessionCounter::HandlerUpdate),
            ("Handler_write", SessionCounter::HandlerWrite),
            ("Questions", SessionCounter::Questions),
            ("Queries", SessionCounter::Queries),
            ("Slow_queries", SessionCounter::SlowQueries),
            ("Sort_merge_passes", SessionCounter::SortMergePasses),
            ("Mydb_sequence_values", SessionCounter::SequenceValues),
            (
                "Mydb_sequence_cache_hits",
                SessionCounter::SequenceCacheHits,
            ),
            ("Mydb_sequence_refills", SessionCounter::SequenceRefills),
        ];
        COUNTERS
            .into_iter()
            .map(|(name, counter)| (name, self.value(counter)))
    }

    pub(crate) fn session_counter_by_name(&self, name: &str) -> Option<u64> {
        self.session_pfs_counters()
            .find_map(|(candidate, value)| candidate.eq_ignore_ascii_case(name).then_some(value))
    }

    pub(crate) fn com_counters(&self) -> impl Iterator<Item = (&'static str, u64)> + '_ {
        COM_NAMES
            .iter()
            .enumerate()
            .map(|(index, name)| (*name, self.values[SESSION_COM_OFFSET + index]))
    }

    pub(crate) fn com_counter_by_name(&self, name: &str) -> Option<u64> {
        COM_NAMES
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(name))
            .map(|index| self.values[SESSION_COM_OFFSET + index])
    }
}

static STATUS_AGGREGATION_DROPPED: AtomicU64 = AtomicU64::new(0);
static CREATED_TMP_FILES: AtomicU64 = AtomicU64::new(0);
static CREATED_TMP_TABLES: AtomicU64 = AtomicU64::new(0);
static CREATED_TMP_DISK_TABLES: AtomicU64 = AtomicU64::new(0);
static SORT_MERGE_PASSES: AtomicU64 = AtomicU64::new(0);

pub(crate) fn record_status_aggregation_dropped() {
    STATUS_AGGREGATION_DROPPED.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn status_aggregation_dropped() -> u64 {
    STATUS_AGGREGATION_DROPPED.load(Ordering::Relaxed)
}

pub(crate) fn record_sort_runs(runs: u64, merge_passes: u64) {
    if runs != 0 {
        CREATED_TMP_FILES.fetch_add(runs, Ordering::Relaxed);
    }
    if merge_passes != 0 {
        SORT_MERGE_PASSES.fetch_add(merge_passes, Ordering::Relaxed);
    }
    crate::engine::expr::with_active_session(|session| {
        if let Some(session) = session {
            add_session_counter(session, SessionCounter::CreatedTmpFiles, runs);
            add_session_counter(session, SessionCounter::SortMergePasses, merge_passes);
        }
    });
}

/// Count executor-owned materialized relations, never CREATE TEMPORARY TABLE.
/// A spilled relation contributes once to both counters, not once per spill file.
pub(crate) fn record_internal_temp_table(disk: bool) {
    CREATED_TMP_TABLES.fetch_add(1, Ordering::Relaxed);
    add_active_session_counter(SessionCounter::CreatedTmpTables, 1);
    if disk {
        CREATED_TMP_DISK_TABLES.fetch_add(1, Ordering::Relaxed);
        add_active_session_counter(SessionCounter::CreatedTmpDiskTables, 1);
    }
}

pub(crate) fn created_tmp_tables() -> u64 {
    CREATED_TMP_TABLES.load(Ordering::Relaxed)
}

pub(crate) fn created_tmp_disk_tables() -> u64 {
    CREATED_TMP_DISK_TABLES.load(Ordering::Relaxed)
}

pub(crate) fn created_tmp_files() -> u64 {
    CREATED_TMP_FILES.load(Ordering::Relaxed)
}

pub(crate) fn sort_merge_passes() -> u64 {
    SORT_MERGE_PASSES.load(Ordering::Relaxed)
}

#[inline]
pub(crate) fn add_session_counter(
    session: &crate::engine::api::SessionContext,
    counter: SessionCounter,
    value: u64,
) {
    if let Some(status) = &session.status {
        status.add(counter, value);
    }
}

#[inline]
pub(crate) fn add_active_session_counter(counter: SessionCounter, value: u64) {
    if value == 0 {
        return;
    }
    crate::engine::expr::with_active_session(|session| {
        if let Some(session) = session {
            add_session_counter(session, counter, value);
        }
    });
}

#[inline]
pub(crate) fn record_session_com(session: &crate::engine::api::SessionContext, kind: ComKind) {
    if let Some(status) = &session.status {
        status.record_com(kind);
    }
}

#[derive(Debug)]
pub(crate) struct RetiredSessionStatus {
    counters: [AtomicU64; SESSION_STATUS_COUNTER_COUNT],
}

impl Default for RetiredSessionStatus {
    fn default() -> Self {
        Self {
            counters: [SESSION_ZERO; SESSION_STATUS_COUNTER_COUNT],
        }
    }
}

impl RetiredSessionStatus {
    pub(crate) fn merge(&self, status: &SessionStatus) {
        for (dst, src) in self.counters.iter().zip(&status.counters) {
            let value = src.load(Ordering::Relaxed);
            if value != 0 {
                dst.fetch_add(value, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn snapshot(&self) -> SessionStatusValues {
        let mut values = [0; SESSION_STATUS_COUNTER_COUNT];
        for (value, counter) in values.iter_mut().zip(&self.counters) {
            *value = counter.load(Ordering::Relaxed);
        }
        SessionStatusValues { values }
    }
}

// --- Network throughput -------------------------------------------------
// Logical MySQL packet bytes (4-byte header + payload) crossing the client
// codec, summed across every connection. Bumped once per packet in
// `crate::mysql::packet`, so the cost is per-command, not per-row.
static BYTES_RECEIVED: AtomicU64 = AtomicU64::new(0);
static BYTES_SENT: AtomicU64 = AtomicU64::new(0);

/// Count bytes read from clients (called from the packet reader).
#[inline]
pub(crate) fn add_bytes_received(n: u64) {
    BYTES_RECEIVED.fetch_add(n, Ordering::Relaxed);
}

/// Count bytes written to clients (called from the packet writer).
#[inline]
pub(crate) fn add_bytes_sent(n: u64) {
    BYTES_SENT.fetch_add(n, Ordering::Relaxed);
}

pub(crate) fn bytes_received() -> u64 {
    BYTES_RECEIVED.load(Ordering::Relaxed)
}

pub(crate) fn bytes_sent() -> u64 {
    BYTES_SENT.load(Ordering::Relaxed)
}

// --- Command counters (Com_xxx) ----------------------------------------
// One counter per SQL command / protocol command that mydb can classify.
// Statement-level kinds (`Com_select` … `Com_truncate`) are classified from
// the leading keyword of a COM_QUERY once per statement; protocol-command
// kinds (`Com_ping`, `Com_stmt_*`, …) are bumped at the COM dispatch match.
// Both stay MySQL-named so existing exporter scripts read them verbatim.

/// Command kinds mydb tracks. The discriminant is the array index; the
/// MySQL status name is in [`COM_NAMES`] at the same index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ComKind {
    // Statement-level (COM_QUERY, keyword-classified).
    Select = 0,
    Insert,
    Update,
    Delete,
    Replace,
    Commit,
    Rollback,
    Begin,
    SetOption,
    CallProcedure,
    Load,
    CreateTable,
    DropTable,
    AlterTable,
    CreateIndex,
    DropIndex,
    CreateDb,
    DropDb,
    Truncate,
    ShowStatus,
    ShowVariables,
    ShowProcesslist,
    ShowTables,
    ShowDatabases,
    ShowFields,
    ShowKeys,
    Explain,
    Analyze,
    Check,
    Optimize,
    Flush,
    // Protocol-command level (COM dispatch byte).
    StmtPrepare,
    StmtExecute,
    StmtClose,
    StmtReset,
    StmtFetch,
    StmtSendLongData,
    Ping,
    InitDb,
    ChangeUser,
    Quit,
    FieldList,
    ResetConnection,
    Statistics,
    ProcessInfo,
    Refresh,
    Debug,
}

/// MySQL `SHOW STATUS` names, indexed by `ComKind as usize`.
const COM_NAMES: &[&str] = &[
    "Com_select",
    "Com_insert",
    "Com_update",
    "Com_delete",
    "Com_replace",
    "Com_commit",
    "Com_rollback",
    "Com_begin",
    "Com_set_option",
    "Com_call_procedure",
    "Com_load",
    "Com_create_table",
    "Com_drop_table",
    "Com_alter_table",
    "Com_create_index",
    "Com_drop_index",
    "Com_create_db",
    "Com_drop_db",
    "Com_truncate",
    "Com_show_status",
    "Com_show_variables",
    "Com_show_processlist",
    "Com_show_tables",
    "Com_show_databases",
    "Com_show_fields",
    "Com_show_keys",
    "Com_explain_other",
    "Com_analyze",
    "Com_check",
    "Com_optimize",
    "Com_flush",
    "Com_stmt_prepare",
    "Com_stmt_execute",
    "Com_stmt_close",
    "Com_stmt_reset",
    "Com_stmt_fetch",
    "Com_stmt_send_long_data",
    "Com_ping",
    "Com_init_db",
    "Com_change_user",
    "Com_quit",
    "Com_field_list",
    "Com_reset_connection",
    "Com_statistics",
    "Com_process_info",
    "Com_refresh",
    "Com_debug",
];

const COM_COUNT: usize = COM_NAMES.len();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatusScope {
    Global,
    Both,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatusReset {
    Never,
    FlushSession,
    FlushEpoch,
    CurrentGauge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatusSourceGroup {
    Storage,
    Connection,
    Session,
    Replication,
    Mydb,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StatusVarDef {
    pub name: &'static str,
    pub scope: StatusScope,
    pub reset: StatusReset,
    pub source: StatusSourceGroup,
    pub pfs_visible: bool,
    pub mydb_extension: bool,
}

/// Canonical status catalog for the implemented MySQL-compatible core and
/// mydb extensions. Values remain in their owning subsystems; this registry is
/// the single metadata authority for scope, reset and surface visibility.
pub(crate) fn status_var_defs() -> &'static [StatusVarDef] {
    static DEFS: std::sync::LazyLock<Vec<StatusVarDef>> = std::sync::LazyLock::new(|| {
        let mut defs = Vec::new();
        let mut add =
            |names: &[&'static str], scope, reset, source, pfs_visible, mydb_extension| {
                defs.extend(names.iter().copied().map(|name| StatusVarDef {
                    name,
                    scope,
                    reset,
                    source,
                    pfs_visible,
                    mydb_extension,
                }));
            };
        add(
            &[
                "Innodb_buffer_pool_read_requests",
                "Innodb_buffer_pool_reads",
                "Innodb_buffer_pool_pages_dirty",
                "Innodb_buffer_pool_pages_total",
                "Innodb_buffer_pool_pages_data",
                "Innodb_buffer_pool_pages_free",
                "Innodb_buffer_pool_wait_free",
                "Innodb_buffer_pool_write_requests",
                "Innodb_pages_read",
                "Innodb_pages_written",
                "Innodb_data_reads",
                "Innodb_data_writes",
                "Innodb_row_lock_waits",
                "Innodb_row_lock_current_waits",
                "Innodb_row_lock_time",
                "Innodb_row_lock_time_avg",
                "Innodb_row_lock_time_max",
                "Innodb_deadlocks",
                "Innodb_log_writes",
                "Innodb_log_write_requests",
                "Innodb_os_log_written",
                "Innodb_os_log_fsyncs",
                "Innodb_rows_inserted",
                "Innodb_rows_updated",
                "Innodb_rows_deleted",
                "Innodb_rows_read",
                "Table_locks_immediate",
                "Table_locks_waited",
            ],
            StatusScope::Global,
            StatusReset::Never,
            StatusSourceGroup::Storage,
            true,
            false,
        );
        add(
            &[
                "Threads_connected",
                "Connections",
                "Aborted_connects",
                "Aborted_clients",
                "Threads_created",
                "Threads_running",
                "Threads_cached",
                "Connection_errors_max_connections",
                "Prepared_stmt_count",
                "Performance_schema_digest_lost",
                "Uptime",
            ],
            StatusScope::Global,
            StatusReset::Never,
            StatusSourceGroup::Connection,
            true,
            false,
        );
        add(
            &["Queries"],
            StatusScope::Both,
            StatusReset::FlushSession,
            StatusSourceGroup::Connection,
            true,
            false,
        );
        add(
            &["Max_used_connections"],
            StatusScope::Global,
            StatusReset::CurrentGauge,
            StatusSourceGroup::Connection,
            true,
            false,
        );
        add(
            &["Uptime_since_flush_status"],
            StatusScope::Global,
            StatusReset::FlushEpoch,
            StatusSourceGroup::Connection,
            true,
            false,
        );
        add(
            &[
                "Bytes_received",
                "Bytes_sent",
                "Created_tmp_files",
                "Created_tmp_tables",
                "Created_tmp_disk_tables",
                "Questions",
                "Handler_write",
                "Handler_update",
                "Handler_delete",
                "Handler_commit",
                "Handler_rollback",
                "Handler_read_rnd_next",
                "Slow_queries",
                "Sort_merge_passes",
            ],
            StatusScope::Both,
            StatusReset::FlushSession,
            StatusSourceGroup::Session,
            true,
            false,
        );
        add(
            COM_NAMES,
            StatusScope::Both,
            StatusReset::FlushSession,
            StatusSourceGroup::Session,
            false,
            false,
        );
        add(
            &["Ssl_cipher", "Ssl_version"],
            StatusScope::Both,
            StatusReset::Never,
            StatusSourceGroup::Session,
            true,
            false,
        );
        add(
            &[
                "Rpl_semi_sync_source_status",
                "Rpl_semi_sync_source_clients",
                "Rpl_semi_sync_source_yes_tx",
                "Rpl_semi_sync_source_no_tx",
                "Rpl_semi_sync_source_wait_sessions",
                "Rpl_semi_sync_master_status",
                "Rpl_semi_sync_master_clients",
                "Rpl_semi_sync_master_yes_tx",
                "Rpl_semi_sync_master_no_tx",
                "Rpl_semi_sync_master_wait_sessions",
                "Rpl_semi_sync_replica_status",
                "Rpl_semi_sync_slave_status",
            ],
            StatusScope::Global,
            StatusReset::Never,
            StatusSourceGroup::Replication,
            true,
            false,
        );
        add(
            &[
                "Mydb_sequence_values",
                "Mydb_sequence_cache_hits",
                "Mydb_sequence_refills",
            ],
            StatusScope::Both,
            StatusReset::Never,
            StatusSourceGroup::Session,
            true,
            false,
        );
        add(
            &[
                "Mydb_active_transactions",
                "Mydb_undo_bytes",
                "Mydb_undo_records",
                "Mydb_wal_bytes",
                "Mydb_wal_syncs",
                "Mydb_sequence_wal_records",
                "Mydb_sequence_checkpoint_rows",
                "Mydb_sequence_local_sync_us",
                "Mydb_sequence_remote_sync_us",
                "Mydb_sequence_remote_failures",
                "Mydb_sequence_dirty_generations",
                "Mydb_checkpoints_total",
                "Mydb_buffer_pool_evictions",
                "Mydb_buffer_pool_prefetches",
                "Mydb_buffer_pool_prefetch_hits",
                "Mydb_plan_cache_hits",
                "Mydb_plan_cache_misses",
                "Mydb_select_parse_cache_hits",
                "Mydb_select_compiled_plan_hits",
                "Mydb_select_operator_compiles",
                "Mydb_prepared_plan_reuses",
                "Mydb_prepared_plan_compiles",
                "Mydb_snapshot_too_old",
                "Mydb_undo_peak_bytes",
                "Mydb_setop_spill_count",
                "Mydb_setop_spill_bytes_written",
                "Mydb_setop_spill_bytes_read",
                "Mydb_setop_spill_partitions",
                "Mydb_setop_spill_repartitions",
                "Mydb_setop_spill_max_depth",
                "Mydb_setop_spill_file_opens",
                "Mydb_setop_spill_merge_passes",
                "Mydb_status_aggregation_dropped",
                "Mydb_txn_admission_waits",
                "Mydb_txn_admission_wait_us",
                "Mydb_mixed_admission_waits",
                "Mydb_mixed_admission_wait_us",
                "Mydb_parallel_read_pressure_entries",
                "Mydb_parallel_read_pressure_suppressed_rw_txn",
                "Mydb_parallel_read_pressure_active",
                "Mydb_parallel_read_gate_waits",
                "Mydb_parallel_read_gate_wait_us",
                "Mydb_replica_channels_running",
                "Mydb_replica_channels_total",
                "Mydb_like_btree_selected",
                "Mydb_like_zone_selected",
                "Mydb_like_ngram_selected",
                "Mydb_like_ngram_no_safe_token",
                "Mydb_like_ngram_posting_rows",
                "Mydb_like_ngram_candidate_rows",
                "Mydb_like_runtime_reclassify",
                "Mydb_fulltext_multi_intersection",
                "Mydb_fulltext_aware_full_scan",
                "Mydb_binlog_group_sync_requests",
                "Mydb_binlog_group_sync_batches",
                "Mydb_binlog_group_sync_avg_batch_size",
                "Mydb_binlog_group_sync_batch_max",
                "Mydb_binlog_cached_events",
                "Mydb_binlog_cached_payload_bytes",
                "Mydb_binlog_file_opens",
                "Mydb_binlog_write_calls",
                "Mydb_binlog_write_bytes",
                "Mydb_binlog_sync_total_us",
                "Mydb_binlog_sync_max_us",
            ],
            StatusScope::Global,
            StatusReset::Never,
            StatusSourceGroup::Mydb,
            true,
            true,
        );
        defs.sort_unstable_by(|left, right| left.name.cmp(right.name));
        defs
    });
    DEFS.as_slice()
}

pub(crate) fn status_var_def(name: &str) -> Option<&'static StatusVarDef> {
    let defs = status_var_defs();
    defs.binary_search_by(|def| def.name.cmp(name))
        .ok()
        .map(|index| &defs[index])
}

// `AtomicU64` is not `Copy`, so build the fixed-size array element by
// element rather than with `[AtomicU64::new(0); N]`.
#[allow(clippy::declare_interior_mutable_const)]
const COM_ZERO: AtomicU64 = AtomicU64::new(0);
static COM_COUNTERS: [AtomicU64; COM_COUNT] = [COM_ZERO; COM_COUNT];

/// Iterate every command counter as `(mysql_name, value)`.
pub(crate) fn com_counters() -> impl Iterator<Item = (&'static str, u64)> {
    COM_NAMES
        .iter()
        .zip(COM_COUNTERS.iter())
        .map(|(name, counter)| (*name, counter.load(Ordering::Relaxed)))
}

pub(crate) fn com_counter_by_name(name: &str) -> Option<u64> {
    COM_NAMES
        .iter()
        .position(|candidate| candidate.eq_ignore_ascii_case(name))
        .map(|index| COM_COUNTERS[index].load(Ordering::Relaxed))
}

/// Classify a COM_QUERY into its statement-level [`ComKind`] from the
/// leading SQL keyword, mirroring what MySQL bumps for the equivalent
/// command. Returns `None` for statements mydb does not surface a dedicated
/// `Com_*` counter for (the vast majority of DDL/admin verbs), so the caller
/// simply skips them rather than inventing a name.
///
/// This is a deliberately cheap leading-token scan — it skips leading
/// whitespace, line/block comments, and an opening parenthesis (e.g.
/// `(SELECT …) UNION …`), then matches the first word case-insensitively.
/// It is the single classification point for every COM_QUERY, so it is
/// immune to the fast-path branching in the executor (txn control, `SELECT
/// @@var`, stub probes) that a per-AST hook would miss.
pub(crate) fn classify_query_com(sql: &str) -> Option<ComKind> {
    let rest = strip_leading_noise(sql);
    let word = leading_word(rest);
    // Match the first keyword. `eq_ignore_ascii_case` avoids allocating an
    // upper-cased copy on this per-statement path.
    let ci = |kw: &str| word.eq_ignore_ascii_case(kw);
    let kind = if ci("select") || ci("with") || ci("table") || ci("values") {
        // WITH/TABLE/VALUES are the 8.0 read-statement forms that resolve to
        // a SELECT-shaped result; MySQL counts them under Com_select.
        ComKind::Select
    } else if ci("insert") {
        ComKind::Insert
    } else if ci("update") {
        ComKind::Update
    } else if ci("delete") {
        ComKind::Delete
    } else if ci("replace") {
        ComKind::Replace
    } else if ci("commit") {
        ComKind::Commit
    } else if ci("rollback") {
        ComKind::Rollback
    } else if ci("begin") || ci("start") {
        // START TRANSACTION / BEGIN.
        ComKind::Begin
    } else if ci("set") {
        ComKind::SetOption
    } else if ci("call") {
        ComKind::CallProcedure
    } else if ci("load") {
        ComKind::Load
    } else if ci("use") {
        ComKind::InitDb
    } else if ci("truncate") {
        ComKind::Truncate
    } else if ci("explain") || ci("describe") || ci("desc") {
        ComKind::Explain
    } else if ci("analyze") {
        ComKind::Analyze
    } else if ci("check") {
        ComKind::Check
    } else if ci("optimize") {
        ComKind::Optimize
    } else if ci("flush") {
        ComKind::Flush
    } else if ci("show") {
        let next = leading_word(strip_leading_noise(&rest[word.len()..]));
        if next.eq_ignore_ascii_case("global")
            || next.eq_ignore_ascii_case("session")
            || next.eq_ignore_ascii_case("local")
        {
            let after_scope =
                strip_leading_noise(&strip_leading_noise(&rest[word.len()..])[next.len()..]);
            let scoped = leading_word(after_scope);
            if scoped.eq_ignore_ascii_case("status") {
                ComKind::ShowStatus
            } else if scoped.eq_ignore_ascii_case("variables") {
                ComKind::ShowVariables
            } else {
                return None;
            }
        } else if next.eq_ignore_ascii_case("status") {
            ComKind::ShowStatus
        } else if next.eq_ignore_ascii_case("variables") {
            ComKind::ShowVariables
        } else if next.eq_ignore_ascii_case("processlist") {
            ComKind::ShowProcesslist
        } else if next.eq_ignore_ascii_case("tables") || next.eq_ignore_ascii_case("table") {
            ComKind::ShowTables
        } else if next.eq_ignore_ascii_case("databases") || next.eq_ignore_ascii_case("schemas") {
            ComKind::ShowDatabases
        } else if next.eq_ignore_ascii_case("columns") || next.eq_ignore_ascii_case("fields") {
            ComKind::ShowFields
        } else if next.eq_ignore_ascii_case("index")
            || next.eq_ignore_ascii_case("indexes")
            || next.eq_ignore_ascii_case("keys")
        {
            ComKind::ShowKeys
        } else {
            return None;
        }
    } else if ci("create") {
        // Disambiguate CREATE TABLE / INDEX / DATABASE from other CREATE
        // verbs (VIEW, TRIGGER, …) that MySQL does not fold into these.
        match leading_word(strip_leading_noise(&rest[word.len()..])) {
            w if w.eq_ignore_ascii_case("table") => ComKind::CreateTable,
            w if w.eq_ignore_ascii_case("unique")
                || w.eq_ignore_ascii_case("index")
                || w.eq_ignore_ascii_case("fulltext")
                || w.eq_ignore_ascii_case("spatial") =>
            {
                ComKind::CreateIndex
            }
            w if w.eq_ignore_ascii_case("database") || w.eq_ignore_ascii_case("schema") => {
                ComKind::CreateDb
            }
            _ => return None,
        }
    } else if ci("drop") {
        match leading_word(strip_leading_noise(&rest[word.len()..])) {
            w if w.eq_ignore_ascii_case("table") => ComKind::DropTable,
            w if w.eq_ignore_ascii_case("index") => ComKind::DropIndex,
            w if w.eq_ignore_ascii_case("database") || w.eq_ignore_ascii_case("schema") => {
                ComKind::DropDb
            }
            _ => return None,
        }
    } else if ci("alter") {
        match leading_word(strip_leading_noise(&rest[word.len()..])) {
            w if w.eq_ignore_ascii_case("table") => ComKind::AlterTable,
            _ => return None,
        }
    } else {
        return None;
    };
    Some(kind)
}

pub(crate) fn classify_statement_com(statement: &crate::parser::Statement) -> Option<ComKind> {
    use crate::parser::Statement as S;
    Some(match statement {
        S::Select(_)
        | S::SetOperation(_)
        | S::WithCte(_)
        | S::SelectIntoVars { .. }
        | S::SelectIntoFile { .. } => ComKind::Select,
        S::Insert(_) | S::CreateTableAsSelect { .. } => ComKind::Insert,
        S::Update(_) => ComKind::Update,
        S::Delete(_) => ComKind::Delete,
        S::Commit { .. } => ComKind::Commit,
        S::Rollback { .. } | S::RollbackToSavepoint(_) => ComKind::Rollback,
        S::Begin { .. } => ComKind::Begin,
        S::SetAutocommit(_)
        | S::SetTransactionIsolation { .. }
        | S::Set(_)
        | S::SetNames { .. }
        | S::SetCharacterSet { .. }
        | S::SetRole(_)
        | S::SetDefaultRole(_)
        | S::SetPassword { .. }
        | S::SetTransactionCharacteristics { .. } => ComKind::SetOption,
        S::Call(_) => ComKind::CallProcedure,
        S::LoadData(_) => ComKind::Load,
        S::CreateTable(_) | S::CreateTableLike { .. } => ComKind::CreateTable,
        S::DropTable { .. } => ComKind::DropTable,
        S::AlterTable(_) => ComKind::AlterTable,
        S::CreateIndex(_) => ComKind::CreateIndex,
        S::DropIndex { .. } => ComKind::DropIndex,
        S::CreateDatabase { .. } => ComKind::CreateDb,
        S::DropDatabase { .. } => ComKind::DropDb,
        S::TruncateTable { .. } => ComKind::Truncate,
        S::UseDatabase(_) => ComKind::InitDb,
        S::ShowStatus { .. } => ComKind::ShowStatus,
        S::ShowVariables { .. } => ComKind::ShowVariables,
        S::ShowProcesslist { .. } => ComKind::ShowProcesslist,
        S::ShowTables { .. } | S::ShowTableStatus { .. } => ComKind::ShowTables,
        S::ShowDatabases { .. } => ComKind::ShowDatabases,
        S::ShowColumns { .. } | S::Describe { .. } => ComKind::ShowFields,
        S::ShowIndex { .. } => ComKind::ShowKeys,
        S::Explain { .. } | S::ExplainForConnection { .. } => ComKind::Explain,
        S::AnalyzeTable { .. } => ComKind::Analyze,
        S::CheckTable { .. } => ComKind::Check,
        S::OptimizeTable { .. } => ComKind::Optimize,
        S::FlushPrivileges
        | S::FlushUserResources
        | S::FlushHosts
        | S::FlushStatus
        | S::FlushTables { .. }
        | S::FlushQueryCache
        | S::FlushLogs { .. } => ComKind::Flush,
        _ => return None,
    })
}

/// Classify protocol commands that have a MySQL `Com_*` status variable.
/// COM_QUERY is deliberately excluded because it is classified by SQL text.
pub(crate) fn classify_protocol_com(command: u8) -> Option<ComKind> {
    use crate::mysql::protocol::*;
    Some(match command {
        COM_STMT_PREPARE => ComKind::StmtPrepare,
        COM_STMT_EXECUTE => ComKind::StmtExecute,
        COM_STMT_CLOSE => ComKind::StmtClose,
        COM_STMT_RESET => ComKind::StmtReset,
        COM_STMT_FETCH => ComKind::StmtFetch,
        COM_STMT_SEND_LONG_DATA => ComKind::StmtSendLongData,
        COM_PING => ComKind::Ping,
        COM_INIT_DB => ComKind::InitDb,
        COM_CHANGE_USER => ComKind::ChangeUser,
        COM_QUIT => ComKind::Quit,
        COM_FIELD_LIST => ComKind::FieldList,
        COM_RESET_CONNECTION => ComKind::ResetConnection,
        COM_STATISTICS => ComKind::Statistics,
        COM_PROCESS_INFO => ComKind::ProcessInfo,
        COM_REFRESH => ComKind::Refresh,
        COM_DEBUG => ComKind::Debug,
        COM_SET_OPTION => ComKind::SetOption,
        _ => return None,
    })
}

/// Skip leading whitespace, `-- …` / `# …` line comments, and `/* … */`
/// block comments, plus a single opening parenthesis, returning the
/// remaining slice.
fn strip_leading_noise(mut s: &str) -> &str {
    loop {
        let trimmed = s.trim_start();
        if let Some(after) = trimmed.strip_prefix("/*") {
            if let Some(end) = after.find("*/") {
                s = &after[end + 2..];
                continue;
            }
            return "";
        }
        if let Some(after) = trimmed
            .strip_prefix("--")
            .or_else(|| trimmed.strip_prefix('#'))
        {
            match after.find('\n') {
                Some(nl) => {
                    s = &after[nl + 1..];
                    continue;
                }
                None => return "",
            }
        }
        if let Some(after) = trimmed.strip_prefix('(') {
            s = after;
            continue;
        }
        return trimmed;
    }
}

/// The leading identifier word (`[A-Za-z_]+`) of `s`, or an empty slice.
fn leading_word(s: &str) -> &str {
    let end = s
        .find(|c: char| !(c.is_ascii_alphabetic() || c == '_'))
        .unwrap_or(s.len());
    &s[..end]
}

// --- Query-execution counters ------------------------------------------

/// Statements slower than `long_query_time` (`Slow_queries`).
static SLOW_QUERIES: AtomicU64 = AtomicU64::new(0);
static FLUSH_TABLES: AtomicU64 = AtomicU64::new(0);

pub(crate) fn record_flush_tables() {
    FLUSH_TABLES.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn flush_tables() -> u64 {
    FLUSH_TABLES.load(Ordering::Relaxed)
}

/// Cumulative rows read/examined by scans, aggregated once per statement
/// from the per-thread `rows_examined` counter — never a per-row atomic.
/// Backs `Innodb_rows_read`.
static ROWS_READ: AtomicU64 = AtomicU64::new(0);
static HANDLER_READ_RND_NEXT: AtomicU64 = AtomicU64::new(0);

/// Internal transaction commits / rollbacks (`Handler_commit`,
/// `Handler_rollback`) — every autocommit, explicit, and fast-path
/// transaction boundary flows through the storage commit/rollback path.
static HANDLER_COMMIT: AtomicU64 = AtomicU64::new(0);
static HANDLER_ROLLBACK: AtomicU64 = AtomicU64::new(0);

/// Live prepared-statement gauge across all sessions (`Prepared_stmt_count`).
/// `AtomicI64` so a transient teardown ordering can never wrap; reads clamp
/// negatives to 0.
static PREPARED_STMT_COUNT: AtomicI64 = AtomicI64::new(0);

static LIKE_BTREE_SELECTED: AtomicU64 = AtomicU64::new(0);
static LIKE_ZONE_SELECTED: AtomicU64 = AtomicU64::new(0);
static LIKE_NGRAM_SELECTED: AtomicU64 = AtomicU64::new(0);
static LIKE_NGRAM_NO_SAFE_TOKEN: AtomicU64 = AtomicU64::new(0);
static LIKE_NGRAM_POSTING_ROWS: AtomicU64 = AtomicU64::new(0);
static LIKE_NGRAM_CANDIDATE_ROWS: AtomicU64 = AtomicU64::new(0);
static LIKE_RUNTIME_RECLASSIFY: AtomicU64 = AtomicU64::new(0);
static FULLTEXT_MULTI_INTERSECTION: AtomicU64 = AtomicU64::new(0);
static FULLTEXT_AWARE_FULL_SCAN: AtomicU64 = AtomicU64::new(0);

macro_rules! access_counter {
    ($record:ident, $counter:ident) => {
        #[inline]
        pub(crate) fn $record() {
            $counter.fetch_add(1, Ordering::Relaxed);
        }
    };
}

access_counter!(record_like_btree_selected, LIKE_BTREE_SELECTED);
access_counter!(record_like_zone_selected, LIKE_ZONE_SELECTED);
access_counter!(record_like_ngram_selected, LIKE_NGRAM_SELECTED);
access_counter!(record_like_ngram_no_safe_token, LIKE_NGRAM_NO_SAFE_TOKEN);
access_counter!(record_like_runtime_reclassify, LIKE_RUNTIME_RECLASSIFY);
access_counter!(
    record_fulltext_multi_intersection,
    FULLTEXT_MULTI_INTERSECTION
);
access_counter!(record_fulltext_aware_full_scan, FULLTEXT_AWARE_FULL_SCAN);

pub(crate) fn record_like_ngram_rows(posting_rows: u64, candidate_rows: u64) {
    LIKE_NGRAM_POSTING_ROWS.fetch_add(posting_rows, Ordering::Relaxed);
    LIKE_NGRAM_CANDIDATE_ROWS.fetch_add(candidate_rows, Ordering::Relaxed);
}

pub(crate) fn like_access_counters() -> [(&'static str, u64); 9] {
    [
        (
            "Mydb_like_btree_selected",
            LIKE_BTREE_SELECTED.load(Ordering::Relaxed),
        ),
        (
            "Mydb_like_zone_selected",
            LIKE_ZONE_SELECTED.load(Ordering::Relaxed),
        ),
        (
            "Mydb_like_ngram_selected",
            LIKE_NGRAM_SELECTED.load(Ordering::Relaxed),
        ),
        (
            "Mydb_like_ngram_no_safe_token",
            LIKE_NGRAM_NO_SAFE_TOKEN.load(Ordering::Relaxed),
        ),
        (
            "Mydb_like_ngram_posting_rows",
            LIKE_NGRAM_POSTING_ROWS.load(Ordering::Relaxed),
        ),
        (
            "Mydb_like_ngram_candidate_rows",
            LIKE_NGRAM_CANDIDATE_ROWS.load(Ordering::Relaxed),
        ),
        (
            "Mydb_like_runtime_reclassify",
            LIKE_RUNTIME_RECLASSIFY.load(Ordering::Relaxed),
        ),
        (
            "Mydb_fulltext_multi_intersection",
            FULLTEXT_MULTI_INTERSECTION.load(Ordering::Relaxed),
        ),
        (
            "Mydb_fulltext_aware_full_scan",
            FULLTEXT_AWARE_FULL_SCAN.load(Ordering::Relaxed),
        ),
    ]
}

#[inline]
pub(crate) fn record_slow_query(session: &crate::engine::api::SessionContext) {
    SLOW_QUERIES.fetch_add(1, Ordering::Relaxed);
    add_session_counter(session, SessionCounter::SlowQueries, 1);
}

pub(crate) fn slow_queries() -> u64 {
    SLOW_QUERIES.load(Ordering::Relaxed)
}

/// Add this statement's examined-row total to the global counter. Called
/// once per statement at drain time, so the hot per-row path stays a
/// no-atomic thread-local.
#[inline]
pub(crate) fn add_rows_read(
    session: &crate::engine::api::SessionContext,
    total: u64,
    sequential: u64,
) {
    if total != 0 {
        ROWS_READ.fetch_add(total, Ordering::Relaxed);
        add_session_counter(session, SessionCounter::RowsRead, total);
    }
    if sequential != 0 {
        HANDLER_READ_RND_NEXT.fetch_add(sequential, Ordering::Relaxed);
        add_session_counter(session, SessionCounter::HandlerReadRndNext, sequential);
    }
}

pub(crate) fn rows_read() -> u64 {
    ROWS_READ.load(Ordering::Relaxed)
}

pub(crate) fn handler_read_rnd_next() -> u64 {
    HANDLER_READ_RND_NEXT.load(Ordering::Relaxed)
}

#[inline]
pub(crate) fn record_handler_commit() {
    HANDLER_COMMIT.fetch_add(1, Ordering::Relaxed);
    add_active_session_counter(SessionCounter::HandlerCommit, 1);
}

#[inline]
pub(crate) fn record_handler_rollback() {
    HANDLER_ROLLBACK.fetch_add(1, Ordering::Relaxed);
    add_active_session_counter(SessionCounter::HandlerRollback, 1);
}

pub(crate) fn handler_commit() -> u64 {
    HANDLER_COMMIT.load(Ordering::Relaxed)
}

pub(crate) fn handler_rollback() -> u64 {
    HANDLER_ROLLBACK.load(Ordering::Relaxed)
}

/// Adjust the live prepared-statement gauge by `delta` (positive on
/// PREPARE, negative on CLOSE / reset / session teardown).
#[inline]
pub(crate) fn adjust_prepared_stmt_count(delta: i64) {
    if delta != 0 {
        PREPARED_STMT_COUNT.fetch_add(delta, Ordering::Relaxed);
    }
}

pub(crate) fn prepared_stmt_count() -> u64 {
    PREPARED_STMT_COUNT.load(Ordering::Relaxed).max(0) as u64
}

/// Atomically reserve one slot under MySQL's global
/// `max_prepared_stmt_count`. The old per-session length check allowed every
/// connection to reach the limit independently.
pub(crate) fn try_reserve_prepared_stmt(limit: u64) -> bool {
    let mut current = PREPARED_STMT_COUNT.load(Ordering::Acquire).max(0);
    loop {
        if current as u64 >= limit {
            return false;
        }
        match PREPARED_STMT_COUNT.compare_exchange_weak(
            current,
            current.saturating_add(1),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return true,
            Err(observed) => current = observed.max(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_status_isolated_and_retired_merge_is_exact() {
        let first = SessionStatus::new_untracked();
        let second = SessionStatus::new_untracked();
        first.add(SessionCounter::Questions, 3);
        second.add(SessionCounter::Questions, 7);
        first.record_com(ComKind::Select);

        assert_eq!(first.snapshot().value(SessionCounter::Questions), 3);
        assert_eq!(second.snapshot().value(SessionCounter::Questions), 7);
        assert_eq!(first.snapshot().com_counter_by_name("Com_select"), Some(1));
        assert_eq!(second.snapshot().com_counter_by_name("Com_select"), Some(0));

        let retired = RetiredSessionStatus::default();
        retired.merge(&first);
        retired.merge(&second);
        let snapshot = retired.snapshot();
        assert_eq!(snapshot.value(SessionCounter::Questions), 10);
        assert_eq!(snapshot.com_counter_by_name("Com_select"), Some(1));
    }

    #[test]
    fn governed_session_status_charges_fixed_capacity_once() {
        let status = SessionStatus::try_new_governed().unwrap();
        let charged = status
            ._memory_charge
            .as_ref()
            .expect("governed status owns a reservation")
            .bytes();
        assert!(charged >= std::mem::size_of::<SessionStatus>());
        let clone = status.clone();
        assert!(std::sync::Arc::ptr_eq(&status, &clone));
        assert_eq!(clone._memory_charge.as_ref().unwrap().bytes(), charged);
    }

    #[test]
    fn status_catalog_is_sorted_and_unique() {
        let defs = status_var_defs();
        assert!(defs.windows(2).all(|pair| pair[0].name < pair[1].name));
        assert!(status_var_def("Questions").is_some());
        assert!(status_var_def("Com_show_status").is_some());
        assert!(status_var_def("Mydb_status_aggregation_dropped").is_some());
    }

    #[test]
    fn command_classifier_covers_scoped_show_and_admin_core() {
        assert_eq!(
            classify_query_com("/* probe */ SHOW GLOBAL STATUS LIKE 'Threads%'"),
            Some(ComKind::ShowStatus)
        );
        assert_eq!(
            classify_query_com("SHOW SESSION VARIABLES"),
            Some(ComKind::ShowVariables)
        );
        assert_eq!(
            classify_query_com("EXPLAIN SELECT 1"),
            Some(ComKind::Explain)
        );
        assert_eq!(
            classify_protocol_com(crate::mysql::protocol::COM_SET_OPTION),
            Some(ComKind::SetOption)
        );
    }

    #[test]
    fn mysql8046_fixture_locks_scope_and_pfs_command_policy() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../docs/fixtures/mysql80-show-status-8.0.46.json"
        ))
        .unwrap();
        assert_eq!(fixture["oracle_mysql_version"], "8.0.46");
        let global = fixture["global_names"].as_array().unwrap();
        let session = fixture["session_names"].as_array().unwrap();
        for name in ["Questions", "Queries", "Ssl_cipher", "Ssl_version"] {
            assert!(global.iter().any(|value| value == name));
            assert!(session.iter().any(|value| value == name));
        }
        let pfs_global = fixture["pfs_global_names"].as_array().unwrap();
        assert!(!pfs_global.iter().any(|value| value == "Com_select"));
        assert!(pfs_global.iter().any(|value| value == "Com_stmt_reprepare"));
    }
}
