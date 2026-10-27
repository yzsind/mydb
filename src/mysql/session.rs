use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, BufReader, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::rustls::ProtocolVersion;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;

use crate::engine::api::{
    QueryEngine, QueryResponse, ResultSet, SessionContext, StreamingResultSet,
};
use crate::engine::auth::{AuthRequest, Privilege};
use crate::engine::session_registry::{ProcessState, SessionHandle, SessionRegistry};
use crate::mysql::connection_manager::{
    handshake_timeout, record_admin_reserve_rejection, record_handshake_failure,
    record_handshake_success,
};
use crate::mysql::packet::{inbound_read_buffer_bytes, Packet, PacketBatchWriter, PacketCodec};
use crate::mysql::protocol::{
    auth_more_data_fast_success, auth_switch_request_packet, column_definition_packet_full,
    column_definition_packet_full_into_with_fallback, column_definition_packet_full_with_fallback,
    command_err_packet_for_spec, eof_as_ok_packet_with_status_warnings, eof_packet_with_status,
    eof_packet_with_status_warnings, initial_handshake_packet_into,
    ok_packet_with_full_payload_capabilities_into, parameter_definition_packet,
    parse_change_user_request, parse_handshake_response, parse_query_attributes, parse_ssl_request,
    parse_stmt_execute_params, server_capabilities, stmt_param_to_sql, stmt_prepare_ok_packet,
    uses_caching_sha2, write_lenenc_int, NegotiatedCapabilities, StmtParamType, StmtParamValue,
    CLIENT_COMPRESS, CLIENT_DEPRECATE_EOF, CLIENT_FOUND_ROWS, CLIENT_INTERACTIVE,
    CLIENT_MULTI_RESULTS, CLIENT_MULTI_STATEMENTS, CLIENT_OPTIONAL_RESULTSET_METADATA,
    CLIENT_PLUGIN_AUTH, CLIENT_PROTOCOL_41, CLIENT_PS_MULTI_RESULTS, CLIENT_QUERY_ATTRIBUTES,
    CLIENT_SESSION_TRACK, CLIENT_SSL, CLIENT_ZSTD_COMPRESSION_ALGORITHM, COM_BINLOG_DUMP,
    COM_BINLOG_DUMP_GTID, COM_CHANGE_USER, COM_DEBUG, COM_INIT_DB, COM_PING, COM_PROCESS_INFO,
    COM_PROCESS_KILL, COM_QUERY, COM_QUIT, COM_REFRESH, COM_REGISTER_SLAVE, COM_RESET_CONNECTION,
    COM_SET_OPTION, COM_STATISTICS, COM_STMT_CLOSE, COM_STMT_EXECUTE, COM_STMT_FETCH,
    COM_STMT_PREPARE, COM_STMT_RESET, COM_STMT_SEND_LONG_DATA, SERVER_MORE_RESULTS_EXISTS,
    SERVER_PS_OUT_PARAMS, SERVER_SESSION_STATE_CHANGED, SERVER_STATUS_AUTOCOMMIT,
    SERVER_STATUS_CURSOR_EXISTS, SERVER_STATUS_IN_TRANS, SERVER_STATUS_LAST_ROW_SENT,
};
#[cfg(test)]
use crate::parser::{
    number_statement_placeholders, Assignment, Delete, Insert, InsertSource, LimitValue, Literal,
    OrderBy, Update,
};
use crate::parser::{parse_statement, Expr, Select, SelectItem, Statement, SubQuery};
use crate::storage::error;
use crate::types::{Column, QueryError};
use crate::util::logging::log_message;

/// The worker owns the real context while an engine call runs. If it panics,
/// the blocking pool catches the panic after this guard has put the context
/// back where the session can recover and roll back its transaction.
#[derive(Default)]
struct ParkedSessionState {
    context: Option<Box<SessionContext>>,
    in_flight: bool,
    cancel: Option<crate::engine::cancel::QueryCancelToken>,
}

#[derive(Default)]
struct ParkedSessionRecovery {
    state: parking_lot::Mutex<ParkedSessionState>,
    finished: parking_lot::Condvar,
}

struct ParkedSessionContext {
    context: Option<Box<SessionContext>>,
    recovery: Arc<ParkedSessionRecovery>,
}

impl std::ops::Deref for ParkedSessionContext {
    type Target = SessionContext;

    fn deref(&self) -> &Self::Target {
        self.context.as_deref().expect("parked context present")
    }
}

impl std::ops::DerefMut for ParkedSessionContext {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.context.as_deref_mut().expect("parked context present")
    }
}

impl Drop for ParkedSessionContext {
    fn drop(&mut self) {
        if let Some(context) = self.context.take() {
            let mut state = self.recovery.state.lock();
            state.context = Some(context);
            state.in_flight = false;
            state.cancel = None;
            self.recovery.finished.notify_all();
        }
    }
}

impl ParkedSessionContext {
    fn into_inner(mut self) -> Box<SessionContext> {
        let context = self.context.take().expect("parked context present");
        let mut state = self.recovery.state.lock();
        state.in_flight = false;
        state.cancel = None;
        self.recovery.finished.notify_all();
        context
    }

    fn with_inner(mut self, context: Box<SessionContext>) -> Self {
        self.context = Some(context);
        self
    }
}

pub struct Session<E: QueryEngine> {
    connection_id: u32,
    blocking_affinity: Option<crate::util::blocking_pool::Affinity>,
    peer_addr: SocketAddr,
    stream: MysqlStream,
    /// Wire-protocol codec — plaintext until the post-handshake call to
    /// `enable_zlib_compression` (gated on the client also negotiating
    /// `CLIENT_COMPRESS`). All `read_packet` / `write_packet` /
    /// `PacketBatchWriter` traffic flows through it so the compressed
    /// vs plaintext branch is taken once per call site, not per byte.
    codec: PacketCodec,
    tls_acceptor: Option<TlsAcceptor>,
    engine: Arc<E>,
    registry: Arc<SessionRegistry>,
    handle: Option<Arc<SessionHandle>>,
    kill_flag: Arc<AtomicBool>,
    capabilities: NegotiatedCapabilities,
    /// True after an account connection quota has been charged. Every exit
    /// path must clear it through engine session cleanup exactly once.
    authenticated: bool,
    /// Configured ordinary-connection limit when this session occupied one of
    /// the administrator-reserved slots. Authentication must require
    /// CONNECTION_ADMIN before the final handshake OK is sent.
    admin_connection_limit: Option<usize>,
    /// True only when the connection used a normal protocol close path.
    /// A clean TCP EOF without COM_QUIT is still an aborted client in MySQL.
    normal_close: bool,
    context: Box<SessionContext>,
    /// Recycled placeholder installed in `self.context` while the real
    /// context is moved into a blocking-pool closure. `SessionContext::
    /// default()` allocates ~15 sysvar `String`s and reads several env
    /// vars; at tens of thousands of cross-thread dispatches per second
    /// that construction (and the matching drop when the real context is
    /// restored) showed up as ~1.5% of CPU on TPC-C profiles. Parking /
    /// restoring swaps the same placeholder back and forth instead, so
    /// the per-dispatch cost is two pointer-sized swaps.
    spare_context: Option<Box<SessionContext>>,
    parked_recovery: Arc<ParkedSessionRecovery>,
    /// Per-connection exact-SQL parser memo. The shared parsed-statement LRU
    /// remains the cross-connection L1, while repeated COM_QUERY traffic on a
    /// connection avoids hashing and contending on its single LRU shard.
    /// Parser mode bits are part of the identity, so a semantic sql_mode
    /// change cannot reuse an incompatible AST.
    parsed_statement_l0: Option<ParsedStatementMemo>,
    /// Per-connection compiled SELECT memos keyed by parser-verified text
    /// shape. This gives COM_QUERY the same borrow-only execution envelope as
    /// a server-side prepared handle without changing wire semantics.
    text_shape_memos: Option<TextShapeMemoCache>,
    /// statement_id → PreparedStatement. Looked up once per
    /// COM_STMT_EXECUTE and COM_STMT_CLOSE; ahash is ~2.9× cheaper than
    /// std SipHash-1-3 on the `u32` key so every prepared-heavy client
    /// (sysbench, JDBC, ORM) takes a small but free win here.
    prepared: PreparedMap,
    /// Incrementally maintained total for the memory retained by `prepared`.
    /// `performance_schema` reads this through the session registry, so the
    /// command hot path must not rescan every prepared handle after each
    /// COM_STMT_EXECUTE.
    prepared_memory_bytes: u64,
    prepared_execute_failed: bool,
    next_statement_id: u32,
    /// Database explicitly selected by the client. Execution retains mydb's
    /// internal `test` fallback, but MySQL PROCESSLIST must report NULL until
    /// the handshake, COM_INIT_DB, or USE selects a schema.
    process_database: Option<String>,
    command_response_sequence: u8,
    /// Transaction-admission permit (opt-in via `MYDB_TXN_ADMISSION`). Held
    /// from this session's first lock-taking statement until the transaction
    /// commits or rolls back, bounding the
    /// number of concurrently open, lock-holding transactions. `None` when the
    /// feature is disabled or between transactions. Released on drop, so a
    /// dropped/aborted connection frees its slot automatically.
    txn_admission: Option<crate::util::exec_permit::AdmissionPermit>,
    /// RAII handle for an active replica registration. Created on a
    /// well-formed `COM_REGISTER_SLAVE`; dropped at session teardown so
    /// `SHOW REPLICAS` reflects who's actually still connected.
    registered_replica_guard: Option<crate::binlog::RegisteredReplicaGuard>,
    /// Set when this dump connection sent `SET @rpl_semi_sync_slave=1` (or the
    /// 8.0.26+ `@rpl_semi_sync_replica`) before its `COM_BINLOG_DUMP`: it is a
    /// semi-sync-capable replica, so the dump stream frames events with the
    /// `0xEF` semi-sync header and reads back ack packets.
    semi_sync_replica_requested: bool,
    /// `server_id` of this connection's registered replica, so a semi-sync ack
    /// read on the dump stream can be attributed to the right registry entry.
    dump_server_id: Option<u32>,
    /// Per-connection 20-byte scramble ("auth-plugin-data") generated fresh
    /// for this connection and sent in the initial handshake / AuthSwitch /
    /// full-auth XOR. Password verifiers challenge against this value so a
    /// scramble captured on the wire cannot be replayed on a new connection.
    handshake_nonce: [u8; crate::mysql::protocol::AUTH_PLUGIN_DATA_LEN],
    /// Reusable payload buffer for OK packets. The packet layer already
    /// recycles frame buffers; this keeps the smaller OK payload itself
    /// from allocating on every DML/COMMIT response.
    ok_packet_scratch: Vec<u8>,
    /// Present only while emitting the children of one COM_QUERY.
    text_batch: Option<TextBatchProgress>,
    /// Bounded OK packet coalescing; metadata is encoded before the next child executes.
    text_batch_packets: Vec<(u8, Vec<u8>)>,
    /// Reused by cached-metadata prepared result sets. Its separate governor
    /// charge tracks the Vec's actual capacity as it grows.
    binary_row_scratch: Vec<u8>,
    binary_row_scratch_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    /// Reservation for the fixed per-connection BufReader and OK scratch.
    /// Dynamic packet/result buffers carry their own capacity reservations.
    _connection_buffer_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

#[derive(Clone, Copy)]
struct TextBatchProgress {
    sequence_id: u8,
    more: bool,
    failed: bool,
}

type PreparedMap = hashbrown::HashMap<u32, PreparedStatement, ahash::RandomState>;

#[cfg(not(unix))]
const LOCAL_INFILE_MEMORY_LIMIT: usize = 1024 * 1024;
const ASYNC_ROW_LOCK_RETRY_LIMIT: usize = 1;
const ASYNC_ROW_LOCK_WAIT_SLICE: std::time::Duration = std::time::Duration::from_millis(2);
static ASYNC_ROW_LOCK_RETRIES: AtomicU64 = AtomicU64::new(0);
static ASYNC_ROW_LOCK_WAKEUPS: AtomicU64 = AtomicU64::new(0);
static ASYNC_ROW_LOCK_FALLBACKS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static TEST_ASYNC_ROW_LOCK_RETRY_ENABLED: AtomicBool = AtomicBool::new(false);

pub(crate) fn async_row_lock_retry_stats() -> (u64, u64, u64) {
    (
        ASYNC_ROW_LOCK_RETRIES.load(Ordering::Relaxed),
        ASYNC_ROW_LOCK_WAKEUPS.load(Ordering::Relaxed),
        ASYNC_ROW_LOCK_FALLBACKS.load(Ordering::Relaxed),
    )
}

fn async_row_lock_retry_enabled() -> bool {
    #[cfg(test)]
    {
        return TEST_ASYNC_ROW_LOCK_RETRY_ENABLED.load(Ordering::Relaxed);
    }
    #[cfg(not(test))]
    static ENABLED: OnceLock<bool> = OnceLock::new();
    #[cfg(not(test))]
    *ENABLED.get_or_init(|| {
        !matches!(
            crate::util::env::var("MYDB_ASYNC_ROW_LOCK_RETRY")
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "false" | "off" | "no"
        )
    })
}

fn build_ok_packet_into(
    context: &mut SessionContext,
    client_capabilities: u32,
    affected_rows: u64,
    status: u16,
    payload: &mut Vec<u8>,
) {
    let local_matched = crate::engine::api::take_statement_matched_rows();
    let matched = context
        .pending_statement_matched_rows
        .take()
        .or(local_matched);
    let mut info = match context.pending_statement_info.take() {
        Some(info) => info,
        None => crate::engine::api::take_statement_info(),
    };
    let warnings = context.warning_count();
    if info.is_empty() {
        if let Some(matched_rows) = matched {
            info = format!(
                "Rows matched: {} Changed: {} Warnings: {}",
                matched_rows, affected_rows, warnings
            );
        }
    }
    let reported_rows = if client_capabilities & CLIENT_FOUND_ROWS != 0 {
        matched.unwrap_or(affected_rows)
    } else {
        affected_rows
    };
    let track_negotiated = client_capabilities & CLIENT_SESSION_TRACK != 0;
    let (state_info, state_changed) = if track_negotiated {
        context.take_session_track_payload()
    } else {
        context.pending_session_track_schema = None;
        context.session_state_changed = false;
        (Vec::new(), false)
    };
    let status = if state_changed {
        status | SERVER_SESSION_STATE_CHANGED
    } else {
        status
    };
    ok_packet_with_full_payload_capabilities_into(
        reported_rows,
        context.statement_insert_id,
        status,
        warnings,
        &info,
        &state_info,
        client_capabilities,
        payload,
    );
}

#[cfg(not(unix))]
struct ChargedCursor {
    cursor: std::io::Cursor<Vec<u8>>,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

#[cfg(not(unix))]
impl std::io::Read for ChargedCursor {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        std::io::Read::read(&mut self.cursor, buf)
    }
}

#[cfg(not(unix))]
enum LocalInfileStaging {
    Memory {
        bytes: Vec<u8>,
        charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
    },
    Spill(crate::engine::executor::spill_quota::ProtocolSpillFile),
}

#[cfg(not(unix))]
impl LocalInfileStaging {
    fn new() -> Self {
        Self::Memory {
            bytes: Vec::new(),
            charge: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::ProtocolTransient,
                0,
            )
            .expect("zero-byte LOCAL INFILE reservation"),
        }
    }

    fn push(
        &mut self,
        input: &[u8],
        data_dir: &std::path::Path,
        connection_id: u32,
    ) -> Result<(), QueryError> {
        match self {
            Self::Memory { bytes, charge }
                if bytes.len().saturating_add(input.len()) <= LOCAL_INFILE_MEMORY_LIMIT =>
            {
                let required = bytes.len().saturating_add(input.len());
                if required > bytes.capacity() {
                    let old = bytes.capacity();
                    let requested = required - old;
                    charge.try_grow(requested)?;
                    if bytes.try_reserve_exact(requested).is_err() {
                        charge.shrink(requested);
                        return Err(error(
                            crate::error::catalog::ER_OUTOFMEMORY,
                            "out of memory while buffering LOAD DATA LOCAL",
                        ));
                    }
                    let actual = bytes.capacity().saturating_sub(old);
                    if actual > requested {
                        charge.try_grow(actual - requested)?;
                    }
                }
                bytes.extend_from_slice(input);
                Ok(())
            }
            Self::Memory { bytes, .. } => {
                let mut spill = crate::engine::executor::spill_quota::ProtocolSpillFile::create(
                    data_dir,
                    connection_id,
                )?;
                std::io::Write::write_all(&mut spill, bytes).map_err(|io_err| {
                    crate::engine::executor::spill_quota::query_error_from_io(&io_err)
                        .unwrap_or_else(|| {
                            error(
                                crate::error::catalog::ER_UNKNOWN_ERROR,
                                format!("LOAD DATA LOCAL spill write: {io_err}"),
                            )
                        })
                })?;
                std::io::Write::write_all(&mut spill, input).map_err(|io_err| {
                    crate::engine::executor::spill_quota::query_error_from_io(&io_err)
                        .unwrap_or_else(|| {
                            error(
                                crate::error::catalog::ER_UNKNOWN_ERROR,
                                format!("LOAD DATA LOCAL spill write: {io_err}"),
                            )
                        })
                })?;
                *self = Self::Spill(spill);
                Ok(())
            }
            Self::Spill(spill) => std::io::Write::write_all(spill, input).map_err(|io_err| {
                crate::engine::executor::spill_quota::query_error_from_io(&io_err).unwrap_or_else(
                    || {
                        error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            format!("LOAD DATA LOCAL spill write: {io_err}"),
                        )
                    },
                )
            }),
        }
    }

    fn into_reader(mut self) -> Result<Box<dyn std::io::Read + Send>, QueryError> {
        match &mut self {
            Self::Spill(spill) => spill.rewind()?,
            Self::Memory { .. } => {}
        }
        Ok(match self {
            Self::Memory { bytes, charge } => Box::new(ChargedCursor {
                cursor: std::io::Cursor::new(bytes),
                _charge: charge,
            }),
            Self::Spill(spill) => Box::new(spill),
        })
    }
}

#[derive(Debug, Clone)]
struct ParsedStatementMemo {
    sql: Arc<str>,
    parser_mode_bits: u16,
    statement: Arc<Statement>,
    execution_class: Option<crate::engine::cache::SelectExecutionClass>,
}

struct TextShapeExecutionMemo {
    shape_sql: Arc<str>,
    state: crate::engine::api::PreparedSelectMemoState,
    dml_state: crate::engine::api::PreparedDmlState,
}

const TEXT_SHAPE_MEMO_SLOTS: usize = 32;

struct TextShapeMemoCache {
    database: String,
    slots: Box<[Option<TextShapeExecutionMemo>; TEXT_SHAPE_MEMO_SLOTS]>,
    next_victim: usize,
    _charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl TextShapeMemoCache {
    fn try_new() -> Option<Self> {
        // Each Ready state owns one small Arc<PreparedSelectMemo> allocation;
        // the compiled plan and shape text are shared and charged by their
        // existing cache owners, so only the slot array + memo envelope belong
        // here. Reserve the fixed capacity once and skip this optional cache
        // entirely under memory pressure.
        let bytes = std::mem::size_of::<[Option<TextShapeExecutionMemo>; TEXT_SHAPE_MEMO_SLOTS]>()
            .saturating_add(TEXT_SHAPE_MEMO_SLOTS * 256);
        let charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::ProtocolTransient,
            bytes,
        )?;
        Some(Self {
            database: String::new(),
            slots: Box::new(std::array::from_fn(|_| None)),
            next_victim: 0,
            _charge: charge,
        })
    }

    fn take(&mut self, shape_sql: &str) -> Option<TextShapeExecutionMemo> {
        self.slots
            .iter_mut()
            .find(|slot| {
                slot.as_ref()
                    .is_some_and(|memo| memo.shape_sql.as_ref() == shape_sql)
            })
            .and_then(Option::take)
    }

    fn take_or_new(&mut self, shape_sql: &Arc<str>) -> (TextShapeExecutionMemo, bool) {
        match self.take(shape_sql) {
            Some(memo) => (memo, false),
            None => (
                TextShapeExecutionMemo {
                    shape_sql: Arc::clone(shape_sql),
                    state: crate::engine::api::PreparedSelectMemoState::default(),
                    dml_state: crate::engine::api::PreparedDmlState::default(),
                },
                true,
            ),
        }
    }

    fn put(&mut self, memo: TextShapeExecutionMemo) {
        if let Some(slot) = self.slots.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(memo);
            return;
        }
        self.slots[self.next_victim] = Some(memo);
        self.next_victim = (self.next_victim + 1) % TEXT_SHAPE_MEMO_SLOTS;
    }
}

impl<E: QueryEngine> Drop for Session<E> {
    fn drop(&mut self) {
        // Dropping an async JoinHandle does not stop its blocking worker.
        // Cancel and join ownership before rollback, including task aborts
        // and panics outside the ordinary command-loop teardown path.
        {
            let mut state = self.parked_recovery.state.lock();
            if state.in_flight {
                if let Some(cancel) = &state.cancel {
                    cancel.cancel_local();
                }
                while state.in_flight {
                    self.parked_recovery.finished.wait(&mut state);
                }
            }
        }
        self.recover_parked_context();
        if std::thread::panicking() {
            if self.authenticated {
                crate::mysql::connection_manager::record_aborted_client();
            }
            log_message(format_args!(
                "connection {} from {} panicked; cleaning up session",
                self.connection_id, self.peer_addr
            ));
        }
        if self.authenticated {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = self.engine.close_session(&mut self.context);
            }));
            self.authenticated = false;
        }
        self.context.set_cancel_handle(None);
        self.handle = None;
        self.registry.unregister(self.connection_id);
        crate::engine::cursor_registry::named_close_for_connection(self.connection_id);
        // Keep the global `Prepared_stmt_count` gauge accurate when a
        // connection disconnects without an explicit COM_STMT_CLOSE /
        // reset for each statement (the common case).
        let live = self.prepared.len() as i64;
        if live != 0 {
            crate::engine::status_counters::adjust_prepared_stmt_count(-live);
        }
    }
}

#[derive(Debug)]
struct PreparedStatement {
    /// Client text retained only for general/slow logging.
    original_sql: Arc<str>,
    /// Redaction verdict for `original_sql`, which every EXECUTE publishes to
    /// the process list and performance_schema.
    original_sql_sensitivity: crate::engine::http::SensitivityMemo,
    /// Equal-length, version-gated SQL used by every semantic path. `Arc<str>`
    /// avoids a full text copy on each COM_STMT_EXECUTE.
    sql: Arc<str>,
    /// Lower-cased / whitespace-collapsed SQL captured once at PREPARE.
    /// Reused on every COM_STMT_EXECUTE so the engine no longer needs to
    /// rerun `normalize_sql` (4-5% of CPU on sysbench oltp_read_only).
    normalized_sql: Arc<str>,
    /// Snapshot of `session.current_database` at PREPARE time. Cached as
    /// `Arc<str>` so the prepared-hint hand-off into the engine costs
    /// only an `Arc::clone` per execute.
    database: Arc<str>,
    /// Parser-affecting sql_mode snapshot captured at COM_STMT_PREPARE.
    /// EXECUTE must not reinterpret `||`, NOT or lexical quoting after a
    /// session mode change.
    parser_sql_mode_bits: u16,
    param_count: usize,
    param_types: Option<Vec<StmtParamType>>,
    /// Reusable late-bound Value vector. Capacity is reserved and charged at
    /// PREPARE time, borrowed for one EXECUTE, then cleared and returned on
    /// every compiled SELECT/DML fast path.
    value_scratch: Vec<crate::types::Value>,
    parsed: Option<Arc<Statement>>,
    /// `statement_has_computed_ordering(parsed)`, derived once: the parsed
    /// template never changes for the lifetime of the handle.
    computed_ordering: bool,
    /// `Com_*` status classification of `sql`, derived once at PREPARE.
    com_kind: Option<crate::engine::status_counters::ComKind>,
    long_data: Vec<Option<Vec<u8>>>,
    long_data_error: Option<QueryError>,
    cursor: Option<PreparedCursor>,
    result_metadata: Option<PreparedResultMetadata>,
    /// PREPARE-time SQL sample/digest derivatives, charged with this handle.
    /// Passed into whichever blocking worker executes the statement so the
    /// logging hot path does not hash the full SQL text again.
    perf_texts: crate::engine::perf_events::PreparedStatementTexts,
    /// Prepared-handle L0 memo for the shared compiled SELECT plan.
    select_memo_state: crate::engine::api::PreparedSelectMemoState,
    /// Same idea for prepared INSERT / UPDATE / DELETE: the first
    /// eligible EXECUTE compiles a `PreparedDmlPlan` (authz + MDL
    /// requests + catalog generation snapshot); subsequent EXECUTEs
    /// short-circuit the 8 dispatch wrappers and reuse it. Independent
    /// from `select_memo_state` so SELECT and write-side memos don't share a
    /// classification slot.
    dml_state: crate::engine::api::PreparedDmlState,
    /// Last contribution to `Session::prepared_memory_bytes`. Recomputed only
    /// for the handle whose mutable protocol state changed.
    memory_bytes: u64,
    timer_prepare: u64,
    count_execute: u64,
    sum_errors: u64,
    sum_timer_execute: u64,
    min_timer_execute: u64,
    max_timer_execute: u64,
    /// Hard-governor reservation, including long-data/parameter capacities.
    memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl PreparedStatement {
    fn estimated_memory_bytes(&self) -> u64 {
        let long_data = self.long_data.iter().fold(0u64, |sum, value| {
            sum.saturating_add(
                value
                    .as_ref()
                    .map(|bytes| bytes.capacity() as u64)
                    .unwrap_or(0),
            )
        });
        let metadata = self.result_metadata.as_ref().map_or(0u64, |meta| {
            meta.column_count_packet.len() as u64
                + meta
                    .column_definitions
                    .iter()
                    .map(|packet| packet.capacity() as u64)
                    .sum::<u64>()
                + (meta.columns.len() * std::mem::size_of::<Column>()) as u64
        });
        (std::mem::size_of::<PreparedStatement>() as u64)
            .saturating_add(self.original_sql.len() as u64)
            .saturating_add(self.sql.len() as u64)
            .saturating_add(self.normalized_sql.len() as u64)
            .saturating_add(self.database.len() as u64)
            .saturating_add(
                self.param_types
                    .as_ref()
                    .map(|types| (types.capacity() * std::mem::size_of::<StmtParamType>()) as u64)
                    .unwrap_or(0),
            )
            .saturating_add(
                (self.value_scratch.capacity() * std::mem::size_of::<crate::types::Value>()) as u64,
            )
            .saturating_add(
                (self.long_data.capacity() * std::mem::size_of::<Option<Vec<u8>>>()) as u64,
            )
            .saturating_add(long_data)
            .saturating_add(metadata)
            .saturating_add(self.perf_texts.estimated_memory_bytes() as u64)
            .saturating_add(if self.parsed.is_some() { 256 } else { 0 })
    }
}

#[derive(Debug, Clone)]
struct PreparedResultMetadata {
    fallback_collation: u16,
    columns: Arc<[Column]>,
    column_count_packet: Arc<[u8]>,
    column_definitions: Arc<[Vec<u8>]>,
}

impl PreparedResultMetadata {
    fn from_columns(columns: &[Column], fallback_collation: u16) -> Option<Self> {
        if columns.is_empty() {
            return None;
        }
        let mut column_count_packet = Vec::new();
        write_lenenc_int(&mut column_count_packet, columns.len() as u64);
        let column_definitions: Arc<[Vec<u8>]> = columns
            .iter()
            .map(|column| {
                column_definition_packet_full_with_fallback(
                    &column.name,
                    column.column_type,
                    &column.source,
                    fallback_collation,
                )
            })
            .collect::<Vec<_>>()
            .into();
        Some(Self {
            fallback_collation,
            columns: Arc::from(columns),
            column_count_packet: column_count_packet.into(),
            column_definitions,
        })
    }

    fn matches_columns(&self, columns: &[Column]) -> bool {
        std::ptr::eq(self.columns.as_ref(), columns) || self.columns.as_ref() == columns
    }
}

/// Server-side cursor backing `COM_STMT_FETCH`. Holds the executor's
/// streaming result iterator instead of a materialized `Vec<Vec<Value>>`,
/// so a `SELECT * FROM big_table` with `useCursorFetch=true` no longer
/// pins the entire result set in server memory: each FETCH pulls at most
/// the requested number of rows from the underlying plan.
///
/// `lookahead` lets EXECUTE distinguish "result set has at least one row
/// → return CURSOR_EXISTS" from "result set is empty → return
/// LAST_ROW_SENT" without materializing the stream. We pull a single row
/// at EXECUTE time; if the stream is already exhausted we never even
/// install the cursor on the prepared entry.
///
/// `_registry_handle` ties this cursor's lifetime to the process-wide
/// `cursor_registry`: dropping the cursor (FETCH exhausts, RESET, CLOSE,
/// session disconnect, idle eviction) removes the entry from the
/// registry and bumps the corresponding stats. The handle itself is
/// never read directly — it just rides along as an RAII token.
#[derive(Debug)]
struct PreparedCursor {
    columns: Vec<Column>,
    inner: Box<dyn crate::engine::api::ResultCursor>,
    lookahead: Option<Vec<crate::types::Value>>,
    exhausted: bool,
    cancel: crate::engine::cancel::QueryCancelToken,
    _registry_handle: crate::engine::cursor_registry::CursorHandle,
}

impl PreparedCursor {
    fn pull_row(&mut self) -> Result<Option<Vec<crate::types::Value>>, crate::types::QueryError> {
        self.cancel.check()?;
        if let Some(row) = self.lookahead.take() {
            return Ok(Some(row));
        }
        if self.exhausted {
            return Ok(None);
        }
        match self.inner.next_row()? {
            Some(row) => Ok(Some(row)),
            None => {
                self.exhausted = true;
                Ok(None)
            }
        }
    }

    fn record_fetch(&self, rows: u64) {
        self._registry_handle.record_fetch(rows);
    }
}

/// Count the `?` bind-parameter placeholders in a COM_STMT_PREPARE SQL string.
///
/// MySQL's binary protocol only treats a top-level `?` as a parameter; a `?`
/// inside a string literal (`'...'` / `"..."`), a quoted identifier
/// (`` `...` ``), or a comment (`-- `, `#`, `/* */`) is ordinary text. The
/// previous implementation counted raw `?` bytes, so a statement like
/// `WHERE name = ? AND note <> 'lit?eral'` reported two parameters and
/// desynchronised the client's binary parameter layout.
fn count_placeholders(
    sql: &str,
    mode: crate::sql_lex::SqlLexMode,
) -> Result<usize, crate::sql_lex::SqlLexError> {
    crate::sql_lex::count_code_byte(sql, b'?', mode, crate::parser::MYSQL_VERSION_ID)
}

/// MySQL picks the per-session idle timeout at connect: interactive
/// clients (those advertising `CLIENT_INTERACTIVE`) copy the global
/// `interactive_timeout`, everyone else copies the global `wait_timeout`.
fn connect_wait_timeout_secs(interactive: bool, wait: u64, interactive_to: u64) -> u64 {
    if interactive {
        interactive_to
    } else {
        wait
    }
}

fn connection_log_sample_rate() -> u64 {
    static RATE: OnceLock<u64> = OnceLock::new();
    *RATE.get_or_init(|| {
        std::env::var("MYDB_CONNECTION_LOG_SAMPLE")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(0)
    })
}

fn should_log_connection_success() -> bool {
    static COUNT: AtomicU64 = AtomicU64::new(0);
    let rate = connection_log_sample_rate();
    rate > 0 && COUNT.fetch_add(1, Ordering::Relaxed) % rate == 0
}

/// Binlog dump heartbeat interval respecting the replica's
/// `SET @master_heartbeat_period = <nanoseconds>` / `@source_heartbeat_period`.
/// The MySQL contract: replica sends nanoseconds as a 64-bit unsigned
/// integer (sometimes a fractional decimal — we round). Defaults to 30s
/// matching prior mydb behaviour and the MySQL default.
fn heartbeat_interval_from_session(
    ctx: &crate::engine::api::SessionContext,
) -> std::time::Duration {
    let probe = ["@master_heartbeat_period", "@source_heartbeat_period"];
    for name in probe {
        // SessionContext stores user-variable keys without the leading '@'.
        // Use its normalizing accessor; a direct map lookup made every
        // replica-requested heartbeat silently fall back to 30 seconds.
        if let Some(value) = ctx.user_variable(name) {
            if let Some(ns) = user_var_to_u64(value) {
                if ns == 0 {
                    return std::time::Duration::from_secs(3600); // effectively off
                }
                return std::time::Duration::from_nanos(ns.min(i64::MAX as u64));
            }
        }
    }
    std::time::Duration::from_secs(30)
}

fn user_var_to_u64(v: &crate::types::Value) -> Option<u64> {
    use crate::types::Value;
    match v {
        Value::Int(i) => Some((*i).max(0) as u64),
        Value::UInt(u) => Some(*u),
        Value::Float(f) => Some(f.max(0.0).round() as u64),
        Value::Double(d) => Some(d.max(0.0).round() as u64),
        Value::Decimal(s) | Value::String(s) => s
            .parse::<u64>()
            .ok()
            .or_else(|| s.parse::<f64>().ok().map(|f| f.max(0.0).round() as u64)),
        Value::DecimalCompact(v, _) => Some((*v).max(0) as u64),
        _ => None,
    }
}

impl<E> Session<E>
where
    E: QueryEngine,
{
    #[inline]
    fn client_capabilities(&self) -> u32 {
        self.capabilities.runtime()
    }

    fn result_fallback_collation_id(&self) -> u16 {
        thread_local! {
            /// Last resolved `character_set_results`; it rarely changes, while
            /// every result set asks for its default collation.
            static LAST: std::cell::RefCell<Option<(Box<str>, u16)>> =
                const { std::cell::RefCell::new(None) };
        }
        let Some(charset) = self.context.character_set_results.as_deref() else {
            return 0;
        };
        LAST.with(|last| {
            let mut last = last.borrow_mut();
            if let Some((_, id)) = last.as_ref().filter(|(name, _)| name.as_ref() == charset) {
                return *id;
            }
            let id = crate::charset::default_collation_for_charset(charset)
                .ok()
                .and_then(|name| crate::charset::resolve_collation(name).ok())
                .map(|collation| collation.id)
                .unwrap_or(crate::charset::UTF8MB4_0900_AI_CI_ID);
            *last = Some((Box::from(charset), id));
            id
        })
    }

    fn server_status(&self) -> u16 {
        let mut status = 0;
        if self.context.autocommit {
            status |= SERVER_STATUS_AUTOCOMMIT;
        }
        if self.context.in_transaction() {
            status |= SERVER_STATUS_IN_TRANS;
            if self.context.read_only_txn {
                status |= 0x2000; // SERVER_STATUS_IN_TRANS_READONLY
            }
        }
        if self.context.temporal_sql_mode_bits & crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES
            != 0
        {
            status |= 0x0200; // SERVER_STATUS_NO_BACKSLASH_ESCAPES
        }
        status
    }

    /// Drop every prepared statement for this session, keeping the global
    /// `Prepared_stmt_count` gauge in sync. The single funnel for all
    /// bulk-clear sites (reset connection / change user / teardown).
    fn clear_prepared(&mut self) {
        if let Some(cache) = self.text_shape_memos.as_mut() {
            cache.slots.fill_with(|| None);
        }
        let live = self.prepared.len() as i64;
        self.prepared.clear();
        self.prepared_memory_bytes = 0;
        crate::engine::status_counters::adjust_prepared_stmt_count(-live);
        self.publish_prepared_memory();
    }

    fn allocate_statement_id(&mut self) -> Option<u32> {
        let start = self.next_statement_id.max(1);
        let mut candidate = start;
        loop {
            if !self.prepared.contains_key(&candidate) {
                self.next_statement_id = candidate.wrapping_add(1).max(1);
                return Some(candidate);
            }
            candidate = candidate.wrapping_add(1).max(1);
            if candidate == start {
                return None;
            }
        }
    }

    /// Refresh one mutable prepared handle's contribution. Protocol commands
    /// can change parameter-type or long-data buffers, but never more than one
    /// handle at a time; keeping a cached contribution turns the former O(N)
    /// per-execute diagnostic scan into O(1) map lookup plus O(parameter count).
    fn refresh_prepared_memory(&mut self, statement_id: u32) {
        let Some(prepared) = self.prepared.get_mut(&statement_id) else {
            return;
        };
        let old = prepared.memory_bytes;
        let new = prepared.estimated_memory_bytes();
        prepared.memory_bytes = new;
        self.prepared_memory_bytes = self
            .prepared_memory_bytes
            .saturating_sub(old)
            .saturating_add(new);
    }

    fn publish_prepared_memory(&self) {
        if let Some(handle) = &self.handle {
            handle.set_protocol_prepared_memory_bytes(self.prepared_memory_bytes);
            let prepared = self
                .prepared
                .iter()
                .map(|(&statement_id, prepared)| {
                    self.prepared_statement_snapshot(statement_id, prepared)
                })
                .collect();
            handle.publish_pfs_redacted_prepared(prepared);
        }
    }

    fn prepared_statement_snapshot(
        &self,
        statement_id: u32,
        prepared: &PreparedStatement,
    ) -> crate::engine::session_registry::PreparedStatementSnapshot {
        crate::engine::session_registry::PreparedStatementSnapshot {
            statement_name: None,
            object_instance: ((self.connection_id as u64) << 32) | statement_id as u64,
            statement_id: statement_id as u64,
            sql_text: if prepared
                .original_sql_sensitivity
                .is_sensitive(&prepared.original_sql)
            {
                Arc::from(crate::engine::http::redaction_marker(
                    &prepared.original_sql,
                    false,
                ))
            } else {
                Arc::clone(&prepared.original_sql)
            },
            owner_thread_id: self.connection_id as u64,
            timer_prepare: prepared.timer_prepare,
            count_execute: prepared.count_execute,
            sum_timer_execute: prepared.sum_timer_execute,
            min_timer_execute: if prepared.count_execute == 0 {
                0
            } else {
                prepared.min_timer_execute
            },
            max_timer_execute: prepared.max_timer_execute,
            sum_errors: prepared.sum_errors,
            memory_bytes: prepared.memory_bytes,
        }
    }

    /// One EXECUTE mutates one prepared handle. Publish that row in place
    /// instead of rebuilding every statement snapshot owned by the session.
    fn publish_prepared_statement(&self, statement_id: u32) {
        let (Some(handle), Some(prepared)) = (&self.handle, self.prepared.get(&statement_id))
        else {
            return;
        };
        handle.set_protocol_prepared_memory_bytes(self.prepared_memory_bytes);
        handle.publish_pfs_redacted_prepared_statement(
            self.prepared_statement_snapshot(statement_id, prepared),
        );
    }

    fn publish_pfs_variables(&mut self) {
        if !self.context.pfs_variables_need_publish() {
            return;
        }
        let Some(handle) = &self.handle else {
            return;
        };
        if let Some(name) = self.context.pfs_incremental_variable() {
            if let Ok(value) = crate::engine::sysvar::registry().read(
                name,
                crate::engine::sysvar::Scope::Session,
                Some(&self.context),
                None,
            ) {
                handle.publish_pfs_variable(name, value.to_display_string());
                self.context.mark_pfs_variables_published();
                return;
            }
            // The name came from the immutable registry, so a failed read is
            // unexpected. Retain correctness by falling through to the full
            // snapshot rather than leaving PFS stale.
        }
        let session_variables = crate::engine::sysvar::snapshot(
            crate::engine::sysvar::Scope::Session,
            Some(&self.context),
            None,
        )
        .into_iter()
        .map(|row| (row.name.to_string(), row.value))
        .collect();
        let user_variables = self
            .context
            .user_variables
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        handle.publish_pfs_variables(
            session_variables,
            user_variables,
            self.context.autocommit,
            self.context
                .effective_isolation()
                .as_mysql_name()
                .replace('-', " "),
        );
        self.context.mark_pfs_variables_published();
    }

    fn record_prepared_execute(
        &mut self,
        statement_id: u32,
        elapsed: std::time::Duration,
        failed: bool,
    ) {
        let Some(prepared) = self.prepared.get_mut(&statement_id) else {
            return;
        };
        let timer = u64::try_from(elapsed.as_nanos().saturating_mul(1_000)).unwrap_or(u64::MAX);
        prepared.count_execute = prepared.count_execute.saturating_add(1);
        prepared.sum_errors = prepared.sum_errors.saturating_add(u64::from(failed));
        prepared.sum_timer_execute = prepared.sum_timer_execute.saturating_add(timer);
        prepared.min_timer_execute = prepared.min_timer_execute.min(timer);
        prepared.max_timer_execute = prepared.max_timer_execute.max(timer);
    }

    fn server_status_with_more(&self, more: bool) -> u16 {
        let status = self.server_status();
        if more {
            status | SERVER_MORE_RESULTS_EXISTS
        } else {
            status
        }
    }

    fn ok_packet(&mut self) -> Vec<u8> {
        let status = self.server_status();
        self.ok_packet_with_affected_rows_full(0, status)
    }

    /// S2/Q2 — emit the canonical OK packet for a DML statement,
    /// honouring CLIENT_FOUND_ROWS (return matched count instead of
    /// changed count when the bit is set) and appending the MySQL `info`
    /// trailer (`Rows matched: N Changed: M Warnings: W` for UPDATE,
    /// `Records: N Duplicates: D Warnings: W` for multi-row INSERT)
    /// when the executor has stashed one in the thread-local channel.
    ///
    /// Also drains any queued CLIENT_SESSION_TRACK payloads — `USE
    /// <db>` and other state changes parked the data on the session,
    /// and the OK packet boundary is where MySQL flushes it to the
    /// client. We only emit the state-info block when the client
    /// actually negotiated `CLIENT_SESSION_TRACK` at handshake;
    /// otherwise the queued data is dropped (matches MySQL).
    fn ok_packet_with_affected_rows_full(&mut self, affected_rows: u64, status: u16) -> Vec<u8> {
        let mut payload = Vec::new();
        self.ok_packet_with_affected_rows_full_into(affected_rows, status, &mut payload);
        payload
    }

    fn ok_packet_with_affected_rows_full_into(
        &mut self,
        affected_rows: u64,
        status: u16,
        payload: &mut Vec<u8>,
    ) {
        let client_capabilities = self.client_capabilities();
        build_ok_packet_into(
            &mut self.context,
            client_capabilities,
            affected_rows,
            status,
            payload,
        );
    }

    async fn write_ok_packet_full(
        &mut self,
        sequence_id: u8,
        affected_rows: u64,
        status: u16,
    ) -> io::Result<()> {
        let mut payload = std::mem::take(&mut self.ok_packet_scratch);
        self.ok_packet_with_affected_rows_full_into(affected_rows, status, &mut payload);
        let result = self
            .codec
            .write_packet(&mut self.stream, sequence_id, &payload)
            .await;
        payload.clear();
        if payload.capacity() > 8 * 1024 {
            payload = Vec::new();
        }
        self.ok_packet_scratch = payload;
        result
    }

    /// Keep packet sequencing and connection policy at each command's call site.
    async fn write_error_packet(
        &mut self,
        sequence_id: u8,
        err: &crate::types::QueryError,
    ) -> io::Result<()> {
        self.codec
            .write_packet(
                &mut self.stream,
                sequence_id,
                &crate::mysql::protocol::err_packet(err),
            )
            .await
    }

    async fn write_ok_packet(&mut self, sequence_id: u8) -> io::Result<()> {
        let status = self.server_status();
        self.write_ok_packet_full(sequence_id, 0, status).await
    }

    async fn write_ok_packet_with_affected_rows(
        &mut self,
        sequence_id: u8,
        affected_rows: u64,
    ) -> io::Result<()> {
        let status = self.server_status();
        self.write_ok_packet_full(sequence_id, affected_rows, status)
            .await
    }

    async fn write_ok_packet_with_more(&mut self, sequence_id: u8, more: bool) -> io::Result<()> {
        let status = self.server_status_with_more(more);
        self.write_ok_packet_full(sequence_id, 0, status).await
    }

    async fn write_ok_packet_with_affected_rows_more(
        &mut self,
        sequence_id: u8,
        affected_rows: u64,
        more: bool,
    ) -> io::Result<()> {
        let status = self.server_status_with_more(more);
        self.write_ok_packet_full(sequence_id, affected_rows, status)
            .await
    }

    fn eof_packet(&self) -> [u8; 5] {
        eof_packet_with_status(self.server_status())
    }

    pub fn new(
        connection_id: u32,
        peer_addr: SocketAddr,
        stream: TcpStream,
        tls_acceptor: Option<TlsAcceptor>,
        engine: Arc<E>,
        registry: Arc<SessionRegistry>,
    ) -> Self {
        Self::try_new(
            connection_id,
            peer_addr,
            stream,
            tls_acceptor,
            engine,
            registry,
        )
        .expect("protocol connection buffer reservation")
    }

    pub fn try_new(
        connection_id: u32,
        peer_addr: SocketAddr,
        stream: TcpStream,
        tls_acceptor: Option<TlsAcceptor>,
        engine: Arc<E>,
        registry: Arc<SessionRegistry>,
    ) -> Result<Self, (io::Error, TcpStream)> {
        // Keep the socket on admission failure so the listener can send ERR.
        let admission = (|| -> io::Result<_> {
            let mut context = SessionContext::default();
            context.current_database.clear();
            context.connection_id = connection_id;
            context.host = peer_addr.to_string();
            let inbound_capacity = inbound_read_buffer_bytes();
            let fixed_admission_bytes = inbound_capacity
                .saturating_add(128)
                .saturating_add(2 * std::mem::size_of::<SessionContext>())
                .saturating_add(std::mem::size_of::<
                    crate::engine::status_counters::SessionStatus,
                >());
            engine
                .reclaim_connection_admission_memory(fixed_admission_bytes)
                .map_err(|err| io::Error::new(io::ErrorKind::OutOfMemory, err.message()))?;
            let status = crate::engine::status_counters::SessionStatus::try_new_governed()
                .map_err(|err| io::Error::new(io::ErrorKind::OutOfMemory, err.message()))?;
            context.status = Some(Arc::clone(&status));
            let connection_buffer_charge =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::ProtocolTransient,
                inbound_capacity
                    .saturating_add(128)
                    .saturating_add(2 * std::mem::size_of::<SessionContext>()),
            )
            .map_err(|err| io::Error::new(io::ErrorKind::OutOfMemory, err.message()))?;
            let binary_row_scratch_charge =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::ProtocolTransient,
                0,
            )
            .map_err(|err| io::Error::new(io::ErrorKind::OutOfMemory, err.message()))?;
            Ok((
                context,
                inbound_capacity,
                status,
                connection_buffer_charge,
                binary_row_scratch_charge,
            ))
        })();
        let (
            context,
            inbound_capacity,
            status,
            connection_buffer_charge,
            binary_row_scratch_charge,
        ) = match admission {
            Ok(admission) => admission,
            Err(err) => return Err((err, stream)),
        };
        Ok(Self {
            connection_id,
            blocking_affinity: crate::util::blocking_pool::Affinity::configured(),
            peer_addr,
            stream: MysqlStream::Plain(BufReader::with_capacity(inbound_capacity, stream)),
            codec: PacketCodec::plain_with_status(status),
            tls_acceptor,
            engine,
            registry,
            handle: None,
            kill_flag: Arc::new(AtomicBool::new(false)),
            capabilities: NegotiatedCapabilities::default(),
            authenticated: false,
            admin_connection_limit: None,
            normal_close: false,
            context: Box::new(context),
            spare_context: None,
            parked_recovery: Arc::new(ParkedSessionRecovery::default()),
            parsed_statement_l0: None,
            text_shape_memos: TextShapeMemoCache::try_new(),
            prepared: PreparedMap::default(),
            prepared_memory_bytes: 0,
            prepared_execute_failed: false,
            next_statement_id: 1,
            process_database: None,
            command_response_sequence: 1,
            txn_admission: None,
            registered_replica_guard: None,
            semi_sync_replica_requested: false,
            dump_server_id: None,
            handshake_nonce: crate::mysql::protocol::generate_auth_nonce(),
            ok_packet_scratch: Vec::with_capacity(128),
            text_batch: None,
            text_batch_packets: Vec::new(),
            binary_row_scratch: Vec::new(),
            binary_row_scratch_charge,
            _connection_buffer_charge: connection_buffer_charge,
        })
    }

    pub(crate) fn require_connection_admin(&mut self, ordinary_limit: usize) {
        self.admin_connection_limit = Some(ordinary_limit);
    }

    pub async fn run(&mut self) -> io::Result<()> {
        let engine = Arc::clone(&self.engine);
        let mut run = std::pin::pin!(self.run_inner());
        std::future::poll_fn(|cx| {
            // Restore TLS before Pending so another connection on this Tokio
            // worker never inherits our database. Covers packet growth and
            // parameter/result encoding before/after the SQL budget exists.
            let _scope = engine.enter_memory_reclaim_scope();
            std::future::Future::poll(run.as_mut(), cx)
        })
        .await
    }

    async fn run_inner(&mut self) -> io::Result<()> {
        let handshake_result = tokio::select! {
            biased;
            _ = crate::mysql::shutdown::shutdown_notified() => {
                Err(io::Error::new(io::ErrorKind::Interrupted, "server shutdown during handshake"))
            }
            result = async { match handshake_timeout() {
            Some(timeout) => match tokio::time::timeout(timeout, self.handshake()).await {
                Ok(result) => result,
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "handshake timeout exceeded",
                )),
            },
            None => self.handshake().await,
        } } => result,
        };
        match handshake_result {
            Ok(()) => record_handshake_success(),
            Err(err) => {
                record_handshake_failure();
                if self.authenticated {
                    let _ = self.close_session_blocking().await;
                    self.authenticated = false;
                }
                return Err(err);
            }
        }
        let handle = self.registry.register_authenticated(
            self.connection_id,
            self.context.user.clone(),
            if self.peer_addr.ip().is_loopback() {
                format!("localhost:{}", self.peer_addr.port())
            } else {
                self.peer_addr.to_string()
            },
            self.process_database.clone().unwrap_or_default(),
            self.context
                .status
                .clone()
                .expect("protocol sessions install governed status before handshake"),
            self.context.authenticated_as.clone(),
        );
        handle.set_connection_attrs(self.context.connection_attrs.clone());
        self.kill_flag = Arc::clone(handle.kill_flag());
        self.context.set_cancel_handle(Some(handle.cancel_handle()));
        self.handle = Some(handle);
        self.publish_pfs_variables();
        self.engine.log_general_event(
            &self.context,
            "Connect",
            &format!("{} on {}", self.context.user, self.context.current_database),
        );
        let result = match self.run_init_connect().await {
            Ok(()) => self.command_loop_with_disconnect().await,
            Err(err) => {
                log_message(format_args!(
                    "connection {} init_connect command failed: {} ({})",
                    self.connection_id,
                    err.message(),
                    err.sql_state()
                ));
                // MySQL sends authentication OK first, then rejects the first
                // command with ER_NEW_ABORTING_CONNECTION and closes. Read via
                // the negotiated codec, respecting idle timeout, KILL and shutdown.
                match self.read_command_packet(self.idle_command_timeout()).await {
                    Ok(Some(packet)) => {
                        let aborted = error(
                            crate::error::catalog::ER_NEW_ABORTING_CONNECTION,
                            format!(
                                "Aborted connection {} to db: '{}' user: '{}' host: '{}' (init_connect command failed)",
                                self.connection_id,
                                if self.context.current_database.is_empty() { "unconnected" } else { &self.context.current_database },
                                self.context.user, self.context.host
                            ),
                        );
                        self.write_error_packet(packet.sequence_id.wrapping_add(1), &aborted)
                            .await
                    }
                    Ok(None) => Ok(()),
                    Err(err) => Err(err),
                }
            }
        };
        if crate::mysql::shutdown::is_shutting_down() {
            self.normal_close = true;
        }
        if !self.normal_close {
            crate::mysql::connection_manager::record_aborted_client();
        }
        self.context.set_cancel_handle(None);
        // Retain visibility until rollback and resource cleanup finish.
        // Discard any SQL-level cursors this session left open (e.g.
        // client disconnected mid-`FETCH`). Prepared-statement cursors
        // are cleaned up implicitly when `self.prepared` drops at the
        // end of the session task — their RAII handles call
        // `cursor_registry::unregister`.
        crate::engine::cursor_registry::named_close_for_connection(self.connection_id);
        if self.authenticated {
            if let Err(err) = self.close_session_blocking().await {
                log_message(format_args!(
                    "connection {} cleanup failed: {} ({})",
                    self.connection_id,
                    err.message(),
                    err.sql_state()
                ));
            }
        }
        self.handle = None;
        self.registry.unregister(self.connection_id);
        self.authenticated = false;
        result
    }

    async fn command_loop_with_disconnect(&mut self) -> io::Result<()> {
        let peer = self.stream.clone_socket()?;
        let handle = self.handle.as_ref().map(Arc::clone);
        let commands = self.command_loop();
        tokio::pin!(commands);
        tokio::select! {
            biased;
            result = &mut commands => result,
            _ = wait_peer_closed(&peer) => {
                if let Some(handle) = handle { handle.peer_disconnected(); }
                // Retain ownership of the in-flight context until cooperative
                // cancellation completes; ordinary teardown then rolls back.
                commands.await
            }
        }
    }

    async fn run_init_connect(&mut self) -> Result<(), QueryError> {
        let Some(sql) = crate::engine::sysvar::builtin::connection::init_connect_sql() else {
            return Ok(());
        };
        if self.context.password_expired
            || self
                .engine
                .authorize_global_privilege(
                    &self.context,
                    Privilege::Dynamic("CONNECTION_ADMIN".to_string()),
                )
                .is_ok()
        {
            return Ok(());
        }
        let engine = Arc::clone(&self.engine);
        let mut context = self.park_context();
        let (context, result) =
            crate::util::blocking_pool::spawn_affine(self.blocking_affinity.as_ref(), move || {
                let result = execute_init_connect_sql(engine.as_ref(), &mut context, &sql);
                (context, result)
            })
            .await
            .map_err(|err| self.recover_join_error(err))?;
        self.restore_context(context);
        self.process_database = (!self.context.current_database.is_empty())
            .then(|| self.context.current_database.clone());
        if let Some(handle) = &self.handle {
            handle.set_identity(
                self.context.user.clone(),
                self.process_database.clone().unwrap_or_default(),
                self.context.authenticated_as.clone(),
            );
        }
        self.publish_pfs_variables();
        result
    }

    async fn handshake(&mut self) -> io::Result<()> {
        let nonce = self.handshake_nonce;
        let mut payload = Vec::with_capacity(96);
        initial_handshake_packet_into(
            self.connection_id,
            self.tls_acceptor.is_some(),
            &nonce,
            &mut payload,
        );
        self.codec
            .write_packet(&mut self.stream, 0, &payload)
            .await?;

        let Some(mut response) = self.codec.read_packet(&mut self.stream).await? else {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "client disconnected during handshake",
            ));
        };
        if response.first_sequence_id != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "handshake response sequence id {} out of order; expected 1",
                    response.first_sequence_id
                ),
            ));
        }
        let mut ssl_request_capabilities = None;
        if is_ssl_request(&response.payload) {
            let request_sequence_id = response.sequence_id;
            match parse_ssl_request(&response.payload) {
                Ok(ssl_request) => {
                    let Some(tls_acceptor) = self.tls_acceptor.clone() else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "client requested SSL but SSL is not configured",
                        ));
                    };
                    ssl_request_capabilities = Some(ssl_request.capability_flags);
                    self.upgrade_to_tls(tls_acceptor).await?;
                    response =
                        self.codec
                            .read_packet(&mut self.stream)
                            .await?
                            .ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::UnexpectedEof,
                                    "client disconnected after SSLRequest",
                                )
                            })?;
                    if response.first_sequence_id != request_sequence_id.wrapping_add(1) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "post-TLS handshake sequence id {} out of order; expected {}",
                                response.first_sequence_id,
                                request_sequence_id.wrapping_add(1)
                            ),
                        ));
                    }
                }
                Err(message) => {
                    let err = command_err_packet_for_spec(
                        crate::mysql::errors::ER_HANDSHAKE_ERROR,
                        format!("bad SSLRequest: {message}"),
                    );
                    self.codec
                        .write_packet(&mut self.stream, request_sequence_id.wrapping_add(1), &err)
                        .await?;
                    return Err(io::Error::new(io::ErrorKind::InvalidData, message));
                }
            }
        }

        if response.payload.len() >= 2
            && u16::from_le_bytes([response.payload[0], response.payload[1]]) as u32
                & CLIENT_PROTOCOL_41
                == 0
        {
            let message =
                "HandshakeResponse is missing CLIENT_PROTOCOL_41; pre-4.1 protocol is unsupported";
            let err = crate::mysql::protocol::pre_handshake_err_packet(
                crate::mysql::errors::ER_HANDSHAKE_ERROR,
                message,
            );
            self.codec
                .write_packet(&mut self.stream, response.sequence_id.wrapping_add(1), &err)
                .await?;
            return Err(io::Error::new(io::ErrorKind::InvalidData, message));
        }
        match parse_handshake_response(&response.payload) {
            Ok(handshake) => {
                let database = handshake.database.as_deref().unwrap_or("");
                if let Some(ssl_caps) = ssl_request_capabilities {
                    if let Err(message) =
                        validate_post_tls_capabilities(ssl_caps, handshake.capability_flags)
                    {
                        let err = command_err_packet_for_spec(
                            crate::mysql::errors::ER_HANDSHAKE_ERROR,
                            message.clone(),
                        );
                        self.codec
                            .write_packet(
                                &mut self.stream,
                                response.sequence_id.wrapping_add(1),
                                &err,
                            )
                            .await?;
                        return Err(io::Error::new(io::ErrorKind::InvalidData, message));
                    }
                }
                self.capabilities = NegotiatedCapabilities::new(
                    handshake.capability_flags,
                    server_capabilities(self.tls_acceptor.is_some()),
                );
                let handshake_collation = match crate::charset::resolve_collation_id_checked(
                    handshake.character_set as u16,
                ) {
                    Ok(collation) => collation,
                    Err(err) => {
                        self.write_error_packet(response.sequence_id.wrapping_add(1), &err)
                            .await?;
                        return Err(io::Error::new(io::ErrorKind::InvalidData, err.message()));
                    }
                };
                if crate::engine::sysvar::builtin::orm_stubs::require_secure_transport()
                    && !matches!(self.stream, MysqlStream::Tls(_))
                {
                    let err = crate::error::catalog::ER_SECURE_TRANSPORT_REQUIRED.error(
                        "Connections using insecure transport are prohibited while require_secure_transport=ON".to_string());
                    self.write_error_packet(response.sequence_id.wrapping_add(1), &err)
                        .await?;
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        err.message(),
                    ));
                }
                let client_host = self.peer_addr.ip().to_string();
                let mut auth_response = handshake.auth_response.clone();
                let mut auth_plugin_name = handshake.auth_plugin_name.clone();
                let mut last_client_sequence_id = response.sequence_id;
                let advertised_plugin = crate::engine::auth::default_auth_plugin();
                let client_plugin = auth_plugin_name.as_deref().unwrap_or(&advertised_plugin);
                match self
                    .engine
                    .auth_plugin_for_user(&handshake.username, &client_host)
                {
                    Ok(Some(account_plugin)) if account_plugin != client_plugin => {
                        if !self.capabilities.has(CLIENT_PLUGIN_AUTH) {
                            let err = command_err_packet_for_spec(
                                crate::mysql::errors::ER_NOT_SUPPORTED_AUTH_MODE,
                                format!(
                                    "Client does not support authentication plugin switch to '{account_plugin}'"
                                ),
                            );
                            self.codec
                                .write_packet(
                                    &mut self.stream,
                                    response.sequence_id.wrapping_add(1),
                                    &err,
                                )
                                .await?;
                            return Err(io::Error::new(
                                io::ErrorKind::PermissionDenied,
                                "client does not support authentication plugin switch",
                            ));
                        }
                        auth_response = self
                            .switch_auth_plugin(
                                &account_plugin,
                                &mut last_client_sequence_id,
                                "client disconnected during authentication plugin switch",
                            )
                            .await?;
                        auth_plugin_name = Some(account_plugin);
                    }
                    Ok(_) => {}
                    Err(err) => {
                        self.write_error_packet(response.sequence_id.wrapping_add(1), &err)
                            .await?;
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            err.message(),
                        ));
                    }
                }
                // Empty-password clients have finished their plugin exchange and expect
                // a final OK/ERR, not AuthMoreData (including fast-auth-success).
                let caching_sha2_exchange = uses_caching_sha2(auth_plugin_name.as_deref())
                    && !matches!(auth_response.as_slice(), [] | [0]);
                let (auth_outcome, took_full_auth) = match self
                    .authenticate_exchange(
                        &handshake.username,
                        &client_host,
                        &auth_response,
                        auth_plugin_name.as_deref(),
                        caching_sha2_exchange,
                        &mut last_client_sequence_id,
                    )
                    .await
                {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        self.write_error_packet(last_client_sequence_id.wrapping_add(1), &err)
                            .await?;
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            err.message(),
                        ));
                    }
                };
                self.context.user = handshake.username.clone();
                self.context.login_user = handshake.username.clone();
                self.context.host = client_host;
                Self::apply_auth_outcome(&mut self.context, auth_outcome);
                self.context.connection_attrs = handshake.connection_attrs.clone();
                self.context.character_set_client = handshake_collation.charset.to_string();
                self.context.character_set_connection = handshake_collation.charset.to_string();
                self.context.character_set_results = Some(handshake_collation.charset.to_string());
                self.context.collation_connection = handshake_collation.name.to_string();
                self.authenticated = true;
                self.init_connection_timeouts();
                if let Some(limit) = self.admin_connection_limit {
                    let required = Privilege::Dynamic("CONNECTION_ADMIN".to_string());
                    if self
                        .engine
                        .authorize_global_privilege(&self.context, required)
                        .is_err()
                    {
                        record_admin_reserve_rejection();
                        let mut seq = last_client_sequence_id.wrapping_add(1);
                        // Nonempty caching_sha2 fast authentication requires
                        // its success marker before the final result packet.
                        seq = self
                            .auth_result_sequence(seq, caching_sha2_exchange, took_full_auth)
                            .await?;
                        let err = command_err_packet_for_spec(
                            crate::error::catalog::ER_CON_COUNT_ERROR,
                            format!("Too many connections (limit {limit})"),
                        );
                        self.codec.write_packet(&mut self.stream, seq, &err).await?;
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            format!("Too many connections (limit {limit})"),
                        ));
                    }
                }
                if !database.is_empty() {
                    if let Err(err) = self
                        .engine
                        .init_session_database(&mut self.context, database)
                    {
                        // Use the *last* client packet's sequence id, not the
                        // initial handshake response's: after an AuthSwitch or
                        // caching_sha2 full-auth round-trip the sequence has
                        // advanced, and replying off the stale id desyncs the
                        // client's packet-sequence tracking.
                        self.write_error_packet(last_client_sequence_id.wrapping_add(1), &err)
                            .await?;
                        return Err(io::Error::new(io::ErrorKind::InvalidData, err.message()));
                    }
                    self.process_database = Some(self.context.current_database.clone());
                }
                if should_log_connection_success() {
                    log_message(format_args!(
                        "connection {} from {} authenticated as '{}' database '{}'",
                        self.connection_id, self.peer_addr, handshake.username, database
                    ));
                }
                let ok = self.ok_packet();
                let mut seq = last_client_sequence_id.wrapping_add(1);
                // Protocol-mandated `AuthMoreData(fast_auth_success)` packet
                // for `caching_sha2_password`. mariadb-connector-c's plugin
                // strictly checks for the 1-byte `0x03` payload (after
                // stripping the `0x01` AuthMoreData marker) before reading
                // the OK; if it sees an OK packet directly, it falls into
                // the "RSA login required" branch and on a TLS link sends
                // the password in plaintext at the next sequence, which
                // command_loop then misinterprets as a command.
                // Empty-password and completed full-auth exchanges instead
                // require the final OK directly.
                seq = self
                    .auth_result_sequence(seq, caching_sha2_exchange, took_full_auth)
                    .await?;
                self.codec.write_packet(&mut self.stream, seq, &ok).await?;
                // MySQL 8.0 CLIENT_COMPRESS / CLIENT_ZSTD_COMPRESSION_ALGORITHM
                // semantics: the handshake (including the final Auth
                // OK) is sent in plaintext. The compressed protocol
                // kicks in for *all* packets exchanged *after* the OK
                // has been seen by the client. Enabling is gated by
                // the negotiated capability AND the server-side GUC,
                // so an operator can disable the on-wire codec without
                // rebuilding. When the client opted into both zlib and
                // zstd, zstd wins — it dominates zlib on every
                // ratio/CPU axis and is the only path that mysql 8.4+
                // CLI clients keep shipping.
                let negotiated_capabilities = self.client_capabilities();
                enable_negotiated_compression(
                    &mut self.codec,
                    negotiated_capabilities,
                    handshake.zstd_compression_level,
                )?;
                Ok(())
            }
            Err(message) => {
                let err = command_err_packet_for_spec(
                    crate::mysql::errors::ER_HANDSHAKE_ERROR,
                    format!("bad handshake response: {message}"),
                );
                self.codec
                    .write_packet(&mut self.stream, response.sequence_id.wrapping_add(1), &err)
                    .await?;
                Err(io::Error::new(io::ErrorKind::InvalidData, message))
            }
        }
    }

    async fn switch_auth_plugin(
        &mut self,
        plugin: &str,
        last_sequence: &mut u8,
        disconnect_message: &'static str,
    ) -> io::Result<Vec<u8>> {
        self.handshake_nonce = crate::mysql::protocol::generate_auth_nonce();
        let switch = auth_switch_request_packet(plugin, &self.handshake_nonce);
        self.codec
            .write_packet(&mut self.stream, last_sequence.wrapping_add(1), &switch)
            .await?;
        let mut response = self
            .codec
            .read_packet(&mut self.stream)
            .await?
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, disconnect_message))?;
        *last_sequence = response.sequence_id;
        Ok(std::mem::take(&mut response.payload))
    }

    /// Empty-password and completed full-auth exchanges go directly to OK/ERR.
    async fn auth_result_sequence(
        &mut self,
        mut sequence: u8,
        caching_sha2_exchange: bool,
        took_full_auth: bool,
    ) -> io::Result<u8> {
        if caching_sha2_exchange && !took_full_auth {
            self.codec
                .write_packet(&mut self.stream, sequence, &auth_more_data_fast_success())
                .await?;
            sequence = sequence.wrapping_add(1);
        }
        Ok(sequence)
    }

    fn auth_request<'a>(
        &'a self,
        username: &'a str,
        host: &'a str,
        auth_response: &'a [u8],
        auth_plugin: Option<&'a str>,
    ) -> AuthRequest<'a> {
        AuthRequest {
            username,
            host,
            auth_response,
            auth_plugin,
            nonce: &self.handshake_nonce,
            client_capabilities: self.client_capabilities(),
            tls_established: matches!(self.stream, MysqlStream::Tls(_)),
            tls_cipher: self.context.ssl_cipher.as_deref(),
            tls_version: self.context.ssl_version.as_deref(),
            x509_subject: self.context.x509_subject.as_deref(),
            x509_issuer: self.context.x509_issuer.as_deref(),
        }
    }

    /// Both entry points verify the scramble first and use full authentication
    /// only for an access-denied result. Callers own their disconnect policy.
    async fn authenticate_exchange(
        &mut self,
        username: &str,
        host: &str,
        response: &[u8],
        plugin: Option<&str>,
        caching_sha2_exchange: bool,
        last_sequence: &mut u8,
    ) -> Result<(crate::engine::auth::AuthOutcome, bool), QueryError> {
        let result = self
            .engine
            .authenticate_request(self.auth_request(username, host, response, plugin));
        match result {
            Ok(outcome) => Ok((outcome, false)),
            Err(err)
                if err.is(crate::mysql::errors::ER_ACCESS_DENIED_ERROR)
                    && caching_sha2_exchange =>
            {
                self.try_caching_sha2_full_auth(last_sequence, username, host)
                    .await
                    .map(|outcome| (outcome, true))
            }
            Err(err)
                if err.is(crate::mysql::errors::ER_ACCESS_DENIED_ERROR)
                    && plugin == Some("sha256_password") =>
            {
                self.try_sha256_full_auth(last_sequence, username, host, response)
                    .await
                    .map(|outcome| (outcome, true))
            }
            Err(err) => Err(err),
        }
    }

    fn apply_auth_outcome(context: &mut SessionContext, outcome: crate::engine::auth::AuthOutcome) {
        context.authenticated_as = outcome.context.authenticated_as;
        context.active_roles = outcome.context.active_roles;
        context.auth_epoch = outcome.context.epoch;
        context.password_expired = outcome.password_expired;
        context.account_locked = outcome.account_locked;
        context.auth_sandbox = outcome.password_expired;
        context.auth_factors_done = 1;
        context.account_limits = outcome.resource_limits;
        context.current_user_is_system = outcome.current_user_is_system;
    }

    /// N1/N6: caching_sha2_password full authentication. Called when the
    /// hash-path verify fails for a `caching_sha2_password` account. We
    /// negotiate the MySQL "perform_full_auth" → RSA public-key → RSA
    /// encrypted plaintext flow and re-verify against the plain password.
    /// On TLS we still go through the public-key dance because most
    /// clients (mariadb-connector-c, mysql-connector-python) consult their
    /// own state machine and won't downgrade. Returns the resulting
    /// `AuthOutcome` on success.
    async fn try_caching_sha2_full_auth(
        &mut self,
        last_sequence: &mut u8,
        username: &str,
        client_host: &str,
    ) -> Result<crate::engine::auth::AuthOutcome, crate::types::QueryError> {
        use crate::engine::auth::{access_denied, rsa_keys, xor_password_with_seed};
        use crate::mysql::protocol::{
            auth_more_data_perform_full_auth, auth_more_data_with_payload,
            CACHING_SHA2_REQUEST_PUBLIC_KEY,
        };
        // 1. Tell the client "perform full auth".
        let mut seq = last_sequence.wrapping_add(1);
        if let Err(e) = self
            .codec
            .write_packet(&mut self.stream, seq, &auth_more_data_perform_full_auth())
            .await
        {
            return Err(crate::mysql::errors::error(
                crate::mysql::errors::ER_HANDSHAKE_ERROR,
                format!("write perform_full_auth: {e}"),
            ));
        }

        // 2. Read whatever the client sends next: either a single-byte
        //    `0x02` RequestPublicKey, or the RSA-encrypted password blob.
        let mut next = match self.codec.read_packet(&mut self.stream).await {
            Ok(Some(p)) => p,
            _ => {
                return Err(crate::mysql::errors::error(
                    crate::mysql::errors::ER_HANDSHAKE_ERROR,
                    "client disconnected during full-auth public-key request",
                ));
            }
        };
        seq = next.sequence_id;

        let rsa_blob: Vec<u8>;
        if next.payload == [CACHING_SHA2_REQUEST_PUBLIC_KEY] {
            // 3a. Client asked for the public key — send the PEM as-is.
            //     MySQL 8.0 sends the PEM ASCII payload *without* a nul
            //     terminator; sqlx's strict PEM decoder (via the `rsa` /
            //     `pkcs8` crates) rejects the trailing nul with
            //     "PEM error in pre-encapsulation boundary". Connector/J,
            //     mysql-connector-python and go-sql-driver all tolerated
            //     the extra byte, but sqlx is the strict litmus.
            let pem = rsa_keys::public_key_pem();
            if let Err(e) = self
                .codec
                .write_packet(
                    &mut self.stream,
                    seq.wrapping_add(1),
                    &auth_more_data_with_payload(pem.as_bytes()),
                )
                .await
            {
                return Err(crate::mysql::errors::error(
                    crate::mysql::errors::ER_HANDSHAKE_ERROR,
                    format!("write public key: {e}"),
                ));
            }
            let mut blob_packet = match self.codec.read_packet(&mut self.stream).await {
                Ok(Some(p)) => p,
                _ => {
                    return Err(crate::mysql::errors::error(
                        crate::mysql::errors::ER_HANDSHAKE_ERROR,
                        "client disconnected after public key",
                    ));
                }
            };
            seq = blob_packet.sequence_id;
            rsa_blob = std::mem::take(&mut blob_packet.payload);
        } else {
            // 3b. Client skipped the public-key request and sent the RSA
            //     blob directly. This is the path mysql-connector-python
            //     takes when it already has the public key cached on disk.
            rsa_blob = std::mem::take(&mut next.payload);
        }
        *last_sequence = seq;

        // Only TLS can carry a plaintext full-auth password. Cleartext TCP
        // must supply a valid RSA ciphertext, regardless of its length.
        let mut decrypted: Vec<u8> = if matches!(self.stream, MysqlStream::Tls(_)) {
            // Plaintext path.
            rsa_blob.clone()
        } else {
            match rsa_keys::decrypt_either(&rsa_blob) {
                Ok(plain) => {
                    let mut buf = plain;
                    xor_password_with_seed(&mut buf, &self.handshake_nonce);
                    buf
                }
                Err(_) => return Err(access_denied(username, client_host)),
            }
        };
        if decrypted.last() == Some(&0) {
            decrypted.pop();
        }
        let plaintext =
            String::from_utf8(decrypted).map_err(|_| access_denied(username, client_host))?;

        // 5. Verify via the plaintext path (which charges failed_login
        //    on its own).
        let auth_req = self.auth_request(username, client_host, &[], Some("caching_sha2_password"));
        self.engine.authenticate_with_plaintext_password(
            username,
            client_host,
            &plaintext,
            &auth_req,
        )
    }

    async fn try_sha256_full_auth(
        &mut self,
        last_sequence: &mut u8,
        username: &str,
        client_host: &str,
        initial_response: &[u8],
    ) -> Result<crate::engine::auth::AuthOutcome, crate::types::QueryError> {
        use crate::engine::auth::{access_denied, rsa_keys, xor_password_with_seed};
        use crate::mysql::protocol::auth_more_data_with_payload;

        // sha256_password uses 0x01 as RequestPublicKey and, unlike
        // caching_sha2_password, has no preceding perform-full-auth packet.
        let rsa_blob = if initial_response == [0x01] {
            let seq = last_sequence.wrapping_add(1);
            self.codec
                .write_packet(
                    &mut self.stream,
                    seq,
                    &auth_more_data_with_payload(rsa_keys::public_key_pem().as_bytes()),
                )
                .await
                .map_err(|err| {
                    crate::mysql::errors::error(
                        crate::mysql::errors::ER_HANDSHAKE_ERROR,
                        format!("write sha256 public key: {err}"),
                    )
                })?;
            let mut packet = self
                .codec
                .read_packet(&mut self.stream)
                .await
                .map_err(|_| access_denied(username, client_host))?
                .ok_or_else(|| access_denied(username, client_host))?;
            *last_sequence = packet.sequence_id;
            std::mem::take(&mut packet.payload)
        } else {
            initial_response.to_vec()
        };

        let mut plain = if matches!(self.stream, MysqlStream::Tls(_)) {
            // TLS protects the plaintext password.
            rsa_blob
        } else {
            let mut decrypted = rsa_keys::decrypt_either(&rsa_blob)
                .map_err(|_| access_denied(username, client_host))?;
            xor_password_with_seed(&mut decrypted, &self.handshake_nonce);
            decrypted
        };
        if plain.last() == Some(&0) {
            plain.pop();
        }
        let password =
            String::from_utf8(plain).map_err(|_| access_denied(username, client_host))?;
        let request = self.auth_request(username, client_host, &[], Some("sha256_password"));
        self.engine
            .authenticate_with_plaintext_password(username, client_host, &password, &request)
    }

    async fn upgrade_to_tls(&mut self, tls_acceptor: TlsAcceptor) -> io::Result<()> {
        let stream = std::mem::replace(&mut self.stream, MysqlStream::Upgrading);
        let MysqlStream::Plain(stream) = stream else {
            self.stream = stream;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "connection is not in plaintext state",
            ));
        };
        match tls_acceptor.accept(stream).await {
            Ok(tls_stream) => {
                let (_, connection) = tls_stream.get_ref();
                self.context.ssl_cipher = connection
                    .negotiated_cipher_suite()
                    .map(|suite| format!("{:?}", suite.suite()));
                self.context.ssl_version = connection
                    .protocol_version()
                    .map(mysql_tls_version_name)
                    .map(str::to_string);
                if let Some(cert) = connection
                    .peer_certificates()
                    .and_then(|certs| certs.first())
                {
                    if let Ok((_, parsed)) = x509_parser::parse_x509_certificate(cert.as_ref()) {
                        self.context.x509_subject = Some(parsed.subject().to_string());
                        self.context.x509_issuer = Some(parsed.issuer().to_string());
                    }
                }
                self.stream = MysqlStream::Tls(Box::new(tls_stream));
                Ok(())
            }
            Err(err) => {
                self.stream = MysqlStream::Upgrading;
                Err(err)
            }
        }
    }

    fn sql_may_call_http(&self, sql: &str) -> bool {
        self.engine
            .sql_may_call_http(crate::engine::http::bound_sql(&self.context, sql))
    }

    fn mark_busy(&self, command: &'static str, info: Option<String>) {
        if let Some(sql) = &info {
            self.sql_may_call_http(sql);
        }
        if let Some(handle) = &self.handle {
            handle.mark_busy(
                command,
                info,
                self.process_database.as_deref().unwrap_or(""),
            );
        }
    }

    fn mark_idle(&self) {
        if let Some(handle) = &self.handle {
            handle.mark_idle(self.process_database.as_deref().unwrap_or(""));
        }
    }

    async fn command_loop(&mut self) -> io::Result<()> {
        loop {
            // Recompute each iteration so `SET [SESSION] wait_timeout = N`
            // takes effect on this connection's *next* idle wait, matching
            // MySQL (which reads the session value when the thread goes
            // idle). The value is the session's own copy, seeded at connect
            // from `wait_timeout` or `interactive_timeout` per the
            // CLIENT_INTERACTIVE capability.
            let idle_timeout = self.idle_command_timeout();
            let packet = match self.read_command_packet(idle_timeout).await {
                Ok(packet) => packet,
                Err(err) => {
                    if let Some(limit) = err
                        .get_ref()
                        .and_then(|e| e.downcast_ref::<crate::mysql::packet::PacketTooLarge>())
                    {
                        let packet = command_err_packet_for_spec(
                            crate::mysql::errors::ER_NET_PACKET_TOO_LARGE,
                            "Got a packet bigger than 'max_allowed_packet' bytes",
                        );
                        self.codec
                            .write_packet(&mut self.stream, limit.response_sequence_id, &packet)
                            .await?;
                    }
                    return Err(err);
                }
            };
            let Some(packet) = packet else {
                return Ok(());
            };
            if packet.first_sequence_id != 0 {
                let err = command_err_packet_for_spec(
                    crate::mysql::errors::ER_UNKNOWN_COM_ERROR,
                    format!(
                        "command packet sequence id {} out of order; expected 0",
                        packet.first_sequence_id
                    ),
                );
                self.codec
                    .write_packet(&mut self.stream, packet.sequence_id.wrapping_add(1), &err)
                    .await?;
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "out-of-order MySQL command packet",
                ));
            }
            self.command_response_sequence = packet.sequence_id.wrapping_add(1);
            self.context.statement_query_attributes.clear();
            if packet.payload.is_empty() {
                let err = command_err_packet_for_spec(
                    crate::mysql::errors::ER_UNKNOWN_COM_ERROR,
                    "empty command packet",
                );
                self.codec
                    .write_packet(&mut self.stream, self.command_response_sequence, &err)
                    .await?;
                continue;
            }

            let command = packet.payload[0];
            if let Some(kind) = crate::engine::status_counters::classify_protocol_com(command) {
                crate::engine::status_counters::record_session_com(&self.context, kind);
            }
            match command {
                COM_QUIT => {
                    self.engine.log_general_event(&self.context, "Quit", "");
                    self.normal_close = true;
                    return Ok(());
                }
                COM_INIT_DB => {
                    self.context.refresh_cancel_generations();
                    self.mark_busy("Init DB", None);
                    self.handle_init_db(&packet.payload[1..]).await?;
                }
                crate::mysql::protocol::COM_FIELD_LIST => {
                    self.context.refresh_cancel_generations();
                    self.engine
                        .log_general_event(&self.context, "Field List", "");
                    self.mark_busy("Field List", None);
                    self.handle_field_list(&packet.payload[1..]).await?;
                }
                COM_PING => {
                    self.engine.log_general_event(&self.context, "Ping", "");
                    let ok = self.ok_packet();
                    self.codec
                        .write_packet(&mut self.stream, self.command_response_sequence, &ok)
                        .await?
                }
                COM_STATISTICS => {
                    self.mark_busy("Statistics", None);
                    self.handle_statistics(&packet.payload[1..]).await?;
                }
                COM_PROCESS_INFO => {
                    self.context.refresh_cancel_generations();
                    self.mark_busy("Processlist", None);
                    self.handle_process_info(&packet.payload[1..]).await?;
                }
                COM_REFRESH => {
                    self.context.refresh_cancel_generations();
                    self.mark_busy("Refresh", None);
                    self.handle_refresh(&packet.payload[1..]).await?;
                }
                COM_DEBUG => {
                    self.mark_busy("Debug", None);
                    self.handle_debug(&packet.payload[1..]).await?;
                }
                COM_PROCESS_KILL => {
                    self.context.refresh_cancel_generations();
                    self.engine
                        .log_general_event(&self.context, "Query", "COM_PROCESS_KILL");
                    self.mark_busy("Query", Some("COM_PROCESS_KILL".to_string()));
                    self.handle_process_kill(&packet.payload[1..]).await?;
                }
                COM_CHANGE_USER => {
                    self.context.refresh_cancel_generations();
                    self.engine
                        .log_general_event(&self.context, "Change user", "");
                    self.mark_busy("Change user", None);
                    self.handle_change_user(&packet.payload[1..]).await?;
                }
                COM_RESET_CONNECTION => {
                    self.context.refresh_cancel_generations();
                    self.mark_busy("Reset connection", None);
                    self.handle_reset_connection(&packet.payload[1..]).await?;
                }
                COM_REGISTER_SLAVE => {
                    self.context.refresh_cancel_generations();
                    self.mark_busy("Register Slave", None);
                    self.handle_register_slave(&packet.payload[1..]).await?;
                }
                COM_BINLOG_DUMP => {
                    self.context.refresh_cancel_generations();
                    self.mark_busy("Binlog Dump", None);
                    self.handle_binlog_dump(&packet.payload[1..]).await?;
                }
                COM_BINLOG_DUMP_GTID => {
                    self.context.refresh_cancel_generations();
                    self.mark_busy("Binlog Dump", Some("GTID".to_string()));
                    self.handle_binlog_dump_gtid(&packet.payload[1..]).await?;
                }
                COM_QUERY => {
                    let _memory_activity =
                        crate::engine::memory_reclaimer::StatementActivityGuard::enter();
                    self.context.refresh_cancel_generations();
                    // Strip the optional CLIENT_QUERY_ATTRIBUTES preamble
                    // before treating the rest as SQL bytes. Connector/J
                    // 8.0.26+ sends `[lenenc 0][lenenc 1]` even when no
                    // attribute was set, so this is the common path —
                    // not an opt-in. On a malformed preamble we send a
                    // protocol error rather than running the corrupted
                    // SQL slice.
                    let body = &packet.payload[1..];
                    let sql_bytes = if self.client_capabilities() & CLIENT_QUERY_ATTRIBUTES != 0 {
                        match parse_query_attributes(body) {
                            Ok(parsed) => {
                                let charset = self.context.character_set_connection.clone();
                                for (name, value) in parsed.attributes {
                                    if name.is_empty() {
                                        continue;
                                    }
                                    if let Ok(value) = stmt_param_to_value(&value, &charset) {
                                        self.context.statement_query_attributes.insert(name, value);
                                    }
                                }
                                parsed.sql
                            }
                            Err(message) => {
                                let err = command_err_packet_for_spec(
                                    crate::mysql::errors::ER_UNKNOWN_COM_ERROR,
                                    format!("invalid query attributes: {message}"),
                                );
                                self.codec
                                    .write_packet(
                                        &mut self.stream,
                                        self.command_response_sequence,
                                        &err,
                                    )
                                    .await?;
                                continue;
                            }
                        }
                    } else {
                        body
                    };
                    // SQL is captured here so SHOW PROCESSLIST sees the
                    // currently-running statement — same behaviour as
                    // before. The String allocation is unavoidable for
                    // observability; only the registry locking around
                    // it has been reduced.
                    let sql_for_info = decode_session_sql(&self.context, sql_bytes)
                        .unwrap_or_else(|_| String::from_utf8_lossy(sql_bytes).into_owned());
                    // Statement-level Com_* classification, at the single
                    // client COM_QUERY boundary — one bump per client
                    // statement, never inflated by internal sub-statements
                    // (routines/triggers) that reuse the executor directly.
                    if !crate::engine::executor::has_non_trailing_statement_separator(&sql_for_info)
                    {
                        if let Some(kind) =
                            crate::engine::status_counters::classify_query_com(&sql_for_info)
                        {
                            crate::engine::status_counters::record_session_com(&self.context, kind);
                        }
                    }
                    crate::engine::perf_events::begin_statement(&self.context, &sql_for_info);
                    self.mark_busy("Query", Some(sql_for_info));
                    crate::mysql::connection_manager::record_statement();
                    crate::engine::status_counters::add_session_counter(
                        &self.context,
                        crate::engine::status_counters::SessionCounter::Questions,
                        1,
                    );
                    crate::engine::status_counters::add_session_counter(
                        &self.context,
                        crate::engine::status_counters::SessionCounter::Queries,
                        1,
                    );
                    self.handle_query(sql_bytes).await?;
                }
                COM_STMT_PREPARE => {
                    self.context.refresh_cancel_generations();
                    self.mark_busy("Prepare", None);
                    self.handle_stmt_prepare(&packet.payload[1..]).await?;
                }
                COM_STMT_EXECUTE => {
                    let _memory_activity =
                        crate::engine::memory_reclaimer::StatementActivityGuard::enter();
                    self.context.refresh_cancel_generations();
                    self.mark_busy("Execute", None);
                    crate::mysql::connection_manager::record_statement();
                    crate::engine::status_counters::add_session_counter(
                        &self.context,
                        crate::engine::status_counters::SessionCounter::Questions,
                        1,
                    );
                    crate::engine::status_counters::add_session_counter(
                        &self.context,
                        crate::engine::status_counters::SessionCounter::Queries,
                        1,
                    );
                    let statement_id = command_statement_id(&packet.payload);
                    let execute_started = std::time::Instant::now();
                    self.prepared_execute_failed = false;
                    let execute_result = self.handle_stmt_execute(&packet.payload[1..]).await;
                    if let Some(statement_id) = statement_id {
                        self.record_prepared_execute(
                            statement_id,
                            execute_started.elapsed(),
                            execute_result.is_err() || self.prepared_execute_failed,
                        );
                        self.refresh_prepared_memory(statement_id);
                    }
                    execute_result?;
                    if let Some(statement_id) = statement_id {
                        self.publish_prepared_statement(statement_id);
                    }
                }
                COM_STMT_SEND_LONG_DATA => {
                    self.context.refresh_cancel_generations();
                    self.mark_busy("Long Data", None);
                    self.handle_stmt_send_long_data(&packet.payload[1..])
                        .await?;
                    if let Some(statement_id) = command_statement_id(&packet.payload) {
                        self.refresh_prepared_memory(statement_id);
                        self.publish_prepared_statement(statement_id);
                    }
                }
                COM_STMT_CLOSE => self.handle_stmt_close(&packet.payload[1..]).await?,
                COM_STMT_RESET => {
                    self.handle_stmt_reset(&packet.payload[1..]).await?;
                    // A reset mutates exactly one handle; republishing every
                    // handle rebuilt and rescanned the session's whole set.
                    if let Some(statement_id) = command_statement_id(&packet.payload) {
                        self.refresh_prepared_memory(statement_id);
                        self.publish_prepared_statement(statement_id);
                    }
                }
                COM_STMT_FETCH => {
                    let _memory_activity =
                        crate::engine::memory_reclaimer::StatementActivityGuard::enter();
                    self.context.refresh_cancel_generations();
                    self.mark_busy("Fetch", None);
                    self.handle_stmt_fetch(&packet.payload[1..]).await?;
                    self.publish_prepared_memory();
                }
                COM_SET_OPTION => self.handle_set_option(&packet.payload[1..]).await?,
                crate::mysql::protocol::COM_SHUTDOWN => {
                    self.engine.log_general_event(&self.context, "Shutdown", "");
                    // Optional trailing shutdown-level byte (only
                    // SHUTDOWN_DEFAULT was ever meaningful in MySQL) is
                    // read and ignored.
                    if let Err(err) = self
                        .engine
                        .authorize_global_privilege(&self.context, Privilege::Shutdown)
                    {
                        self.write_error_packet(self.command_response_sequence, &err)
                            .await?;
                        // Privilege denied: server keeps running.
                        self.mark_idle();
                        continue;
                    }
                    // MySQL's classic COM_SHUTDOWN acknowledges with OK,
                    // then the server goes down; this connection closes.
                    let ok = self.ok_packet();
                    self.codec
                        .write_packet(&mut self.stream, self.command_response_sequence, &ok)
                        .await?;
                    crate::mysql::shutdown::request_shutdown(
                        crate::mysql::shutdown::ShutdownSource::Com {
                            conn_id: self.context.connection_id,
                        },
                        crate::mysql::shutdown::ShutdownIntent::Halt,
                    );
                    self.normal_close = true;
                    return Ok(());
                }
                other => {
                    let err = command_err_packet_for_spec(
                        crate::mysql::errors::ER_UNKNOWN_COM_ERROR,
                        format!("unsupported MySQL command: 0x{other:02x}"),
                    );
                    self.codec
                        .write_packet(&mut self.stream, self.command_response_sequence, &err)
                        .await?;
                }
            }

            // COMMIT/ROLLBACK ... RELEASE (or completion_type=RELEASE)
            // disconnects after the response is sent, like COM_QUIT.
            if self.context.close_after_response {
                self.context.close_after_response = false;
                self.engine.log_general_event(&self.context, "Quit", "");
                self.normal_close = true;
                return Ok(());
            }

            self.publish_pfs_variables();
            self.mark_idle();
            self.context.refresh_cancel_generations();
            // Free the transaction-admission slot as soon as this session leaves
            // its transaction (commit/rollback/autocommit end), handing it to a
            // waiting transaction. No-op when admission is disabled.
            self.release_txn_admission_if_idle();

            if self.kill_flag.load(Ordering::Relaxed) {
                return Ok(());
            }
            // A SHUTDOWN / RESTART / COM_SHUTDOWN has been requested (this
            // connection may be the one that issued it, having just flushed
            // its OK). Close so the accept loop's drain can converge.
            if crate::mysql::shutdown::is_shutting_down() {
                return Ok(());
            }
        }
    }

    /// Seed this session's idle-timeout copy at connect. MySQL clients
    /// that announced `CLIENT_INTERACTIVE` (e.g. the `mysql` CLI) use the
    /// global `interactive_timeout`; everyone else uses the global
    /// `wait_timeout`. The chosen value lands in the session's own
    /// `wait_timeout` field, so a later `SET SESSION wait_timeout = N`
    /// overrides it and `SET GLOBAL` does not retro-affect this
    /// connection — matching MySQL.
    fn init_connection_timeouts(&mut self) {
        use crate::engine::sysvar::builtin::connection;
        let interactive = self.client_capabilities() & CLIENT_INTERACTIVE != 0;
        let global_interactive = connection::interactive_timeout_secs();
        self.context.sysvars.wait_timeout = connect_wait_timeout_secs(
            interactive,
            connection::wait_timeout_secs(),
            global_interactive,
        );
        self.context.sysvars.interactive_timeout = global_interactive;
        // Seed the SESSION_TRACK_SYSTEM_VARIABLES subscription from the
        // global default; the client may override it with SET SESSION.
        self.context.session_track_system_variables =
            connection::session_track_system_variables_default();
        self.context.session_track_schema = connection::session_track_schema_default();
        self.context.session_track_state_change = connection::session_track_state_change_default();
    }

    /// Idle timeout between client commands. Reads this session's own
    /// `wait_timeout` copy (seeded by `init_connection_timeouts`, settable
    /// via `SET [SESSION] wait_timeout`). Match the sysvar/environment minimum
    /// of one second even for an internally constructed context.
    fn idle_command_timeout(&self) -> Option<std::time::Duration> {
        Some(std::time::Duration::from_secs(
            self.context.sysvars.wait_timeout.max(1),
        ))
    }

    async fn read_command_packet(
        &mut self,
        idle_timeout: Option<std::time::Duration>,
    ) -> io::Result<Option<Packet>> {
        let Some(handle) = self.handle.as_ref().map(Arc::clone) else {
            return self.codec.read_packet(&mut self.stream).await;
        };
        loop {
            let timed_out = {
                let read = self.codec.read_packet(&mut self.stream);
                tokio::pin!(read);

                // Pipelined clients and request/response clients running on
                // another core can already have the next command in the socket
                // receive buffer after the previous response is flushed.  Poll
                // the read once before allocating and registering timeout,
                // shutdown, and KILL futures.  A genuinely idle connection falls
                // through to the unchanged `select!` path below.
                let first = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(std::future::Future::poll(read.as_mut(), cx))
                })
                .await;
                if let std::task::Poll::Ready(result) = first {
                    return result;
                }

                match idle_timeout {
                    Some(dur) => {
                        let timeout = tokio::time::sleep(dur);
                        tokio::pin!(timeout);
                        tokio::select! {
                            result = &mut read => return result,
                            _ = &mut timeout => true,
                            _ = crate::mysql::shutdown::shutdown_notified() => return Ok(None),
                            _ = handle.notified() => {
                                if self.kill_flag.load(Ordering::Acquire) {
                                    return Ok(None);
                                }
                                false
                            }
                        }
                    }
                    None => {
                        tokio::select! {
                            result = &mut read => return result,
                            _ = crate::mysql::shutdown::shutdown_notified() => return Ok(None),
                            _ = handle.notified() => {
                                if self.kill_flag.load(Ordering::Acquire) {
                                    return Ok(None);
                                }
                                false
                            }
                        }
                    }
                }
            };
            if timed_out {
                let err = error(
                    crate::error::catalog::ER_CLIENT_INTERACTION_TIMEOUT,
                    "The client was disconnected by the server because of inactivity. See wait_timeout and interactive_timeout.",
                );
                self.write_error_packet(0, &err).await?;
                return Ok(None);
            }
        }
    }

    async fn handle_statistics(&mut self, payload: &[u8]) -> io::Result<()> {
        if !payload.is_empty() {
            let err = command_err_packet_for_spec(
                crate::mysql::errors::ER_UNKNOWN_COM_ERROR,
                "COM_STATISTICS packet must not contain a payload",
            );
            return self
                .codec
                .write_packet(&mut self.stream, self.command_response_sequence, &err)
                .await;
        }
        let uptime = crate::engine::status_counters::uptime_seconds();
        let metrics = crate::mysql::connection_manager::connection_metrics();
        let questions = self
            .registry
            .global_session_status()
            .value(crate::engine::status_counters::SessionCounter::Questions);
        let qps = if uptime == 0 {
            0.0
        } else {
            questions as f64 / uptime as f64
        };
        // mydb has no MySQL table-open cache; its opens and resident cache
        // entries are both zero. FLUSH TABLES is a real operation and is
        // counted separately below.
        let stats = format!(
            "Uptime: {uptime}  Threads: {}  Questions: {}  Slow queries: {}  Opens: 0  Flush tables: {}  Open tables: 0  Queries per second avg: {qps:.3}",
            metrics.active,
            questions,
            crate::engine::status_counters::slow_queries(),
            crate::engine::status_counters::flush_tables(),
        );
        self.codec
            .write_packet(
                &mut self.stream,
                self.command_response_sequence,
                stats.as_bytes(),
            )
            .await
    }

    async fn handle_process_info(&mut self, payload: &[u8]) -> io::Result<()> {
        if !payload.is_empty() {
            let err = command_err_packet_for_spec(
                crate::mysql::errors::ER_UNKNOWN_COM_ERROR,
                "COM_PROCESS_INFO packet must not contain a payload",
            );
            return self
                .codec
                .write_packet(&mut self.stream, self.command_response_sequence, &err)
                .await;
        }
        let result = self.engine.execute(&mut self.context, "SHOW PROCESSLIST");
        self.write_query_response(result).await
    }

    async fn handle_refresh(&mut self, payload: &[u8]) -> io::Result<()> {
        if payload.len() != 1 {
            let err = command_err_packet_for_spec(
                crate::mysql::errors::ER_UNKNOWN_COM_ERROR,
                format!("COM_REFRESH payload must be 1 byte, got {}", payload.len()),
            );
            return self
                .codec
                .write_packet(&mut self.stream, self.command_response_sequence, &err)
                .await;
        }
        const REFRESH_GRANT: u8 = 1;
        const REFRESH_LOG: u8 = 2;
        const REFRESH_TABLES: u8 = 4;
        const REFRESH_HOSTS: u8 = 8;
        const REFRESH_STATUS: u8 = 16;
        const REFRESH_THREADS: u8 = 32;
        const REFRESH_REPLICA: u8 = 64;
        const REFRESH_MASTER: u8 = 128;
        let flags = payload[0];
        if flags == 0 || flags & REFRESH_THREADS != 0 {
            let err = crate::mysql::errors::unsupported(if flags == 0 {
                "empty COM_REFRESH operation"
            } else {
                "COM_REFRESH thread-cache reset (mydb has no MySQL thread cache)"
            });
            return self
                .write_error_packet(self.command_response_sequence, &err)
                .await;
        }
        let operations = [
            (REFRESH_GRANT, "FLUSH PRIVILEGES"),
            (REFRESH_LOG, "FLUSH LOGS"),
            (REFRESH_TABLES, "FLUSH TABLES"),
            (REFRESH_HOSTS, "FLUSH HOSTS"),
            (REFRESH_STATUS, "FLUSH STATUS"),
            (REFRESH_REPLICA, "RESET REPLICA"),
            (REFRESH_MASTER, "RESET MASTER"),
        ];
        for (bit, sql) in operations {
            if flags & bit == 0 {
                continue;
            }
            let result = self.execute_engine_blocking(sql.to_string()).await;
            if let Err(err) = result {
                return self
                    .write_error_packet(self.command_response_sequence, &err)
                    .await;
            }
        }
        self.write_ok_packet(self.command_response_sequence).await
    }

    async fn handle_debug(&mut self, payload: &[u8]) -> io::Result<()> {
        if !payload.is_empty() {
            let err = command_err_packet_for_spec(
                crate::mysql::errors::ER_UNKNOWN_COM_ERROR,
                "COM_DEBUG packet must not contain a payload",
            );
            return self
                .codec
                .write_packet(&mut self.stream, self.command_response_sequence, &err)
                .await;
        }
        if let Err(err) = self
            .engine
            .authorize_global_privilege(&self.context, Privilege::Super)
        {
            return self
                .write_error_packet(self.command_response_sequence, &err)
                .await;
        }
        let metrics = crate::mysql::connection_manager::connection_metrics();
        let questions = self
            .registry
            .global_session_status()
            .value(crate::engine::status_counters::SessionCounter::Questions);
        log_message(format_args!(
            "COM_DEBUG conn={} user={} active_connections={} questions={} bytes_in={} bytes_out={}",
            self.connection_id,
            self.context.user,
            metrics.active,
            questions,
            crate::engine::status_counters::bytes_received(),
            crate::engine::status_counters::bytes_sent(),
        ));
        self.write_ok_packet(self.command_response_sequence).await
    }

    async fn handle_process_kill(&mut self, payload: &[u8]) -> io::Result<()> {
        if let Some(err) = self.context.cancelled_error() {
            return self.write_query_response(Err(err)).await;
        }
        let Some(connection_id) = parse_process_kill_connection_id(payload) else {
            let err = command_err_packet_for_spec(
                crate::mysql::errors::ER_UNKNOWN_COM_ERROR,
                "bad COM_PROCESS_KILL packet",
            );
            self.codec
                .write_packet(&mut self.stream, self.command_response_sequence, &err)
                .await?;
            return Ok(());
        };
        let response = self
            .engine
            .kill_session(&self.context, connection_id)
            .map(|()| QueryResponse::Ok);
        self.write_query_response(response).await
    }

    async fn handle_reset_connection(&mut self, payload: &[u8]) -> io::Result<()> {
        if !payload.is_empty() {
            let err = command_err_packet_for_spec(
                crate::mysql::errors::ER_UNKNOWN_COM_ERROR,
                "COM_RESET_CONNECTION packet must not contain a payload",
            );
            self.codec
                .write_packet(&mut self.stream, self.command_response_sequence, &err)
                .await?;
            return Ok(());
        }

        let engine = Arc::clone(&self.engine);
        let mut context = self.park_context();
        let reset =
            crate::util::blocking_pool::spawn_affine(self.blocking_affinity.as_ref(), move || {
                let result = engine.reset_session(&mut context);
                (context, result)
            })
            .await;
        let (old_context, result) = match reset {
            Ok(pair) => pair,
            Err(join_err) => {
                let err = self.recover_join_error(join_err);
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            }
        };
        if let Err(err) = result {
            self.restore_context(old_context);
            self.write_error_packet(self.command_response_sequence, &err)
                .await?;
            return Ok(());
        }

        let fresh = old_context.fresh_after_connection_reset();
        self.restore_context(old_context.with_inner(Box::new(fresh)));
        self.clear_prepared();
        self.parsed_statement_l0 = None;
        self.txn_admission = None;
        self.registered_replica_guard = None;
        self.semi_sync_replica_requested = false;
        self.dump_server_id = None;
        crate::engine::cursor_registry::named_close_for_connection(self.connection_id);
        self.init_connection_timeouts();
        if let Some(handle) = &self.handle {
            handle.set_identity(
                self.context.user.clone(),
                self.process_database.clone().unwrap_or_default(),
                self.context.authenticated_as.clone(),
            );
            handle.set_connection_attrs(self.context.connection_attrs.clone());
        }
        self.write_ok_packet(self.command_response_sequence).await
    }

    async fn handle_change_user(&mut self, payload: &[u8]) -> io::Result<()> {
        if let Some(err) = self.context.cancelled_error() {
            return self.write_query_response(Err(err)).await;
        }
        let request = match parse_change_user_request(payload, self.client_capabilities()) {
            Ok(request) => request,
            Err(message) => {
                let err = command_err_packet_for_spec(
                    crate::mysql::errors::ER_HANDSHAKE_ERROR,
                    format!("bad COM_CHANGE_USER packet: {message}"),
                );
                self.codec
                    .write_packet(&mut self.stream, self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            }
        };

        // Prepare encoding first; database access is checked only after
        // authentication, using the newly authenticated identity.
        let client_host = self.peer_addr.ip().to_string();
        let mut context = SessionContext::default();
        context.current_database.clear();
        context.connection_id = self.connection_id;
        context.host = self.peer_addr.to_string();
        context.ssl_cipher = self.context.ssl_cipher.clone();
        context.ssl_version = self.context.ssl_version.clone();
        context.x509_subject = self.context.x509_subject.clone();
        context.x509_issuer = self.context.x509_issuer.clone();
        context.connection_attrs = request.connection_attrs.clone();
        context.set_cancel_handle(self.handle.as_ref().map(|handle| handle.cancel_handle()));
        if let Some(collation_id) = request.character_set {
            match crate::charset::resolve_collation_id_checked(collation_id) {
                Ok(collation) => {
                    context.character_set_client = collation.charset.to_string();
                    context.character_set_connection = collation.charset.to_string();
                    context.character_set_results = Some(collation.charset.to_string());
                    context.collation_connection = collation.name.to_string();
                }
                Err(err) => {
                    self.write_error_packet(self.command_response_sequence, &err)
                        .await?;
                    return Ok(());
                }
            }
        }

        let mut auth_response = request.auth_response.clone();
        let mut auth_plugin_name = request.auth_plugin_name.clone();
        let mut last_client_sequence_id = self.command_response_sequence.wrapping_sub(1);
        let advertised_plugin = crate::engine::auth::default_auth_plugin();
        let client_plugin = auth_plugin_name.as_deref().unwrap_or(&advertised_plugin);
        match self
            .engine
            .auth_plugin_for_user(&request.username, &client_host)
        {
            Ok(Some(account_plugin)) if account_plugin != client_plugin => {
                if self.client_capabilities() & CLIENT_PLUGIN_AUTH == 0 {
                    let err = command_err_packet_for_spec(
                        crate::mysql::errors::ER_NOT_SUPPORTED_AUTH_MODE,
                        format!(
                            "Client does not support authentication plugin switch to '{account_plugin}'"
                        ),
                    );
                    self.codec
                        .write_packet(&mut self.stream, self.command_response_sequence, &err)
                        .await?;
                    return Ok(());
                }
                self.handshake_nonce = crate::mysql::protocol::generate_auth_nonce();
                auth_response = self
                    .switch_auth_plugin(
                        &account_plugin,
                        &mut last_client_sequence_id,
                        "client disconnected during COM_CHANGE_USER authentication plugin switch",
                    )
                    .await?;
                auth_plugin_name = Some(account_plugin);
            }
            Ok(_) => {}
            Err(err) => {
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            }
        }

        // Empty-password clients have finished their plugin exchange and expect
        // a final OK/ERR, not AuthMoreData (including fast-auth-success).
        let caching_sha2_exchange = uses_caching_sha2(auth_plugin_name.as_deref())
            && !matches!(auth_response.as_slice(), [] | [0]);
        let (auth_outcome, took_full_auth) = match self
            .authenticate_exchange(
                &request.username,
                &client_host,
                &auth_response,
                auth_plugin_name.as_deref(),
                caching_sha2_exchange,
                &mut last_client_sequence_id,
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                self.write_error_packet(last_client_sequence_id.wrapping_add(1), &err)
                    .await?;
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "COM_CHANGE_USER authentication failed",
                ));
            }
        };
        context.user = request.username.clone();
        context.login_user = request.username.clone();
        Self::apply_auth_outcome(&mut context, auth_outcome);

        context.status = self.context.status.clone();
        if let Some(database) = request.database.as_deref() {
            if let Err(err) = self.engine.init_session_database(&mut context, database) {
                crate::engine::auth::release_connection(&context.authenticated_as);
                self.write_error_packet(last_client_sequence_id.wrapping_add(1), &err)
                    .await?;
                return Ok(());
            }
        }

        // A COM_CHANGE_USER on an administrator-reserved socket must not be
        // able to switch the connection to an ordinary account. Check the new
        // identity before tearing down the still-valid old session.
        if let Some(limit) = self.admin_connection_limit {
            let required = Privilege::Dynamic("CONNECTION_ADMIN".to_string());
            if self
                .engine
                .authorize_global_privilege(&context, required)
                .is_err()
            {
                crate::engine::auth::release_connection(&context.authenticated_as);
                let mut seq = last_client_sequence_id.wrapping_add(1);
                seq = self
                    .auth_result_sequence(seq, caching_sha2_exchange, took_full_auth)
                    .await?;
                let err = command_err_packet_for_spec(
                    crate::error::catalog::ER_CON_COUNT_ERROR,
                    format!("Too many connections (limit {limit})"),
                );
                self.codec.write_packet(&mut self.stream, seq, &err).await?;
                return Ok(());
            }
        }

        if let Err(err) = self.close_session_blocking().await {
            crate::engine::auth::release_connection(&context.authenticated_as);
            self.authenticated = false;
            self.write_error_packet(last_client_sequence_id.wrapping_add(1), &err)
                .await?;
            return Err(io::Error::other("COM_CHANGE_USER cleanup failed"));
        }

        self.clear_prepared();
        self.context = Box::new(context);
        self.init_connection_timeouts();
        self.process_database = request
            .database
            .as_ref()
            .map(|_| self.context.current_database.clone());
        if let Some(handle) = &self.handle {
            handle.set_identity(
                self.context.user.clone(),
                self.process_database.clone().unwrap_or_default(),
                self.context.authenticated_as.clone(),
            );
            handle.set_connection_attrs(self.context.connection_attrs.clone());
        }
        let ok = self.ok_packet();
        let mut seq = last_client_sequence_id.wrapping_add(1);
        seq = self
            .auth_result_sequence(seq, caching_sha2_exchange, took_full_auth)
            .await?;
        self.codec.write_packet(&mut self.stream, seq, &ok).await
    }

    async fn handle_register_slave(&mut self, payload: &[u8]) -> io::Result<()> {
        if let Err(err) = self
            .engine
            .authorize_global_privilege(&self.context, Privilege::ReplSlave)
        {
            self.write_error_packet(self.command_response_sequence, &err)
                .await?;
            return Ok(());
        }
        let manager = match crate::binlog::require_manager() {
            Ok(m) => m,
            Err(err) => {
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            }
        };
        // Unsupported/malformed protocol must never be acknowledged as a
        // successful no-op: replication tooling otherwise believes it is
        // registered while SHOW REPLICAS and semi-sync quorum do not.
        match crate::binlog::parse_register_slave_payload(payload) {
            Ok(mut info) => {
                let server_id = info.server_id;
                self.dump_server_id = Some(server_id);
                // MySQL replicas advertise their server UUID via
                // `SET @replica_uuid = '...'` (8.0.26+; older: `@slave_uuid`)
                // before COM_REGISTER_SLAVE. Capture it so SHOW REPLICAS can
                // report it.
                if let Some(uuid) = self
                    .context
                    .user_variable("replica_uuid")
                    .or_else(|| self.context.user_variable("slave_uuid"))
                    .and_then(crate::types::value_str)
                {
                    info.replica_uuid = uuid.to_string();
                }
                // Re-registration on this connection releases its old identity.
                self.registered_replica_guard.take();
                match manager.register_replica(info) {
                    Ok(guard) => self.registered_replica_guard = Some(guard),
                    Err(err) => {
                        self.dump_server_id = None;
                        return self
                            .write_error_packet(self.command_response_sequence, &err)
                            .await;
                    }
                }
            }
            Err(err) => {
                return self
                    .write_error_packet(self.command_response_sequence, &err)
                    .await;
            }
        }
        let ok = self.ok_packet();
        self.codec
            .write_packet(&mut self.stream, self.command_response_sequence, &ok)
            .await
    }

    async fn handle_binlog_dump_gtid(&mut self, payload: &[u8]) -> io::Result<()> {
        if let Err(err) = self
            .engine
            .authorize_global_privilege(&self.context, Privilege::ReplSlave)
        {
            self.write_error_packet(self.command_response_sequence, &err)
                .await?;
            return Ok(());
        }
        let request = match crate::binlog::parse_binlog_dump_gtid_request(payload) {
            Ok(request) => request,
            Err(err) => {
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            }
        };
        let manager = match crate::binlog::require_manager() {
            Ok(manager) => manager,
            Err(err) => {
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            }
        };
        let _dump_lease = manager.lease_dump();
        let start = match manager.position_for_gtid_dump(
            &request.file,
            request.position,
            &request.executed_gtid_set,
        ) {
            Ok(position) => position,
            Err(err) => {
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            }
        };
        self.stream_binlog_events(
            manager,
            start.file,
            start.position,
            request.non_blocking(),
            request.executed_gtid_set,
        )
        .await
    }

    async fn handle_binlog_dump(&mut self, payload: &[u8]) -> io::Result<()> {
        if let Err(err) = self
            .engine
            .authorize_global_privilege(&self.context, Privilege::ReplSlave)
        {
            self.write_error_packet(self.command_response_sequence, &err)
                .await?;
            return Ok(());
        }
        let request = match crate::binlog::parse_binlog_dump_request(payload) {
            Ok(request) => request,
            Err(err) => {
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            }
        };
        let manager = match crate::binlog::require_manager() {
            Ok(manager) => manager,
            Err(err) => {
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            }
        };

        let _dump_lease = manager.lease_dump();
        let non_blocking = request.non_blocking();
        let file = if request.file.is_empty() {
            manager.active_file_for_empty_request()
        } else {
            request.file
        };
        let position = request.position.max(4);
        self.stream_binlog_events(manager, file, position, non_blocking, String::new())
            .await
    }

    // (helper below)
    // Cancellation covers both an idle tail and socket backpressure. The dump
    // command ends the connection, so dropping a partial packet write is safe.
    async fn stream_binlog_events(
        &mut self,
        manager: std::sync::Arc<crate::binlog::BinlogSourceManager>,
        file: String,
        position: u64,
        non_blocking: bool,
        excluded_gtids: String,
    ) -> io::Result<()> {
        let handle = self.handle.as_ref().map(Arc::clone);
        let killed = async {
            if let Some(handle) = handle {
                loop {
                    handle.notified().await;
                    if handle.kill_flag().load(Ordering::Acquire) {
                        return;
                    }
                }
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::select! {
            biased;
            _ = crate::mysql::shutdown::shutdown_notified() => Ok(()),
            _ = killed => Ok(()),
            result = self.stream_binlog_events_inner(manager, file, position, non_blocking, excluded_gtids) => result,
        }
    }

    async fn stream_binlog_events_inner(
        &mut self,
        manager: std::sync::Arc<crate::binlog::BinlogSourceManager>,
        mut file: String,
        mut position: u64,
        non_blocking: bool,
        excluded_gtids: String,
    ) -> io::Result<()> {
        let mut gtid_filter = crate::binlog::BinlogGtidFilter::new(&excluded_gtids);
        if let Err(err) = crate::repl::ensure_logical_source_supported() {
            self.write_error_packet(self.command_response_sequence, &err)
                .await?;
            return Ok(());
        }
        let mut sequence_id = self.command_response_sequence;
        if let Err(err) = manager.events_after_bounded(&file, position, 0, 0) {
            self.write_error_packet(sequence_id, &err).await?;
            return Ok(());
        }
        if let Some(handle) = &self.handle {
            handle.set_base_state(ProcessState::SendingBinlogEventToReplica);
        }
        // Vanilla MySQL replicas refuse to write the relay log unless the
        // very first packets in a dump stream are a fake ROTATE + an FDE
        // — the FDE in particular communicates the checksum algorithm,
        // and without it the SQL thread defaults to NONE and rejects the
        // CRC32 trailer on every subsequent event. mydb's events_after
        // never includes these, so prepend them here.
        // A replica advertises semi-sync (SET @rpl_semi_sync_slave=1) before its
        // dump. This fixes the connection's framing even when the source
        // temporarily stops requesting ACKs; a runtime OFF must not remove
        // headers which the receiver plugin still expects.
        let semi_sync_capable = self.semi_sync_replica_requested;
        let dump_server_id = self.dump_server_id;
        // MySQL's receiver plugin advertises capability AFTER registration.
        // Count the replica at dump start, once negotiation is complete.
        if semi_sync_capable {
            if let Some(server_id) = dump_server_id {
                manager.semi_sync_mark_replica(server_id);
            }
        }
        for ev in manager.dump_preamble(&file, position) {
            let mut packet = Vec::with_capacity(ev.len() + 3);
            frame_binlog_packet(&mut packet, &ev, semi_sync_capable, false);
            if self
                .codec
                .write_packet(&mut self.stream, sequence_id, &packet)
                .await
                .is_err()
            {
                return Ok(());
            }
            sequence_id = sequence_id.wrapping_add(1);
        }
        // Persistent scratch for cancel-safe semi-sync ack reads: `ack_buf`
        // accumulates raw socket bytes across loop iterations so a cancelled
        // read never loses a partial frame; `ack_read_tmp` is the per-read
        // landing buffer.
        const MAX_SEMI_SYNC_ACK_FRAME: usize = 1024 + 4;
        let _ack_memory = match crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::ProtocolTransient,
            MAX_SEMI_SYNC_ACK_FRAME,
        ) {
            Ok(charge) => charge,
            Err(err) => {
                self.write_error_packet(sequence_id, &err).await?;
                return Ok(());
            }
        };
        let mut ack_buf: Vec<u8> = Vec::with_capacity(MAX_SEMI_SYNC_ACK_FRAME);
        let mut ack_read_tmp = [0u8; 512];
        loop {
            if let Err(err) = crate::repl::ensure_logical_source_supported() {
                self.write_error_packet(sequence_id, &err).await?;
                return Ok(());
            }
            if self.kill_flag.load(Ordering::Acquire) {
                return Ok(());
            }
            let semi_sync_active = semi_sync_capable && manager.semi_sync_source_enabled();
            // Register before checking the published tail so rotate/commit
            // cannot wake us in the gap between checking and parking.
            let notified = manager.wait_for_new_events();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let mut events =
                match manager.events_after_bounded(&file, position, 256, 8 * 1024 * 1024) {
                    Ok(events) => events,
                    Err(err) => {
                        self.write_error_packet(sequence_id, &err).await?;
                        return Ok(());
                    }
                };
            if !events.is_empty() {
                let tail = events
                    .last()
                    .map(|event| (event.file.clone(), event.end_pos))
                    .unwrap();
                events.retain(|event| gtid_filter.should_send(event));
                if events.is_empty() {
                    (file, position) = tail;
                    // Excluded batches perform no socket I/O. Yield so an
                    // arbitrarily long excluded history remains cancellable.
                    tokio::task::yield_now().await;
                    continue;
                }
                if let Some(handle) = &self.handle {
                    handle.set_base_state(ProcessState::SendingBinlogEventToReplica);
                }
                let last = events.len().saturating_sub(1);
                for (idx, event) in events.into_iter().enumerate() {
                    // One ACK per batch covers every earlier commit. The
                    // receiver resets its packet counter while sending that
                    // ACK; reset the outgoing counter at this event boundary,
                    // never on delayed ACK arrival (the stream may be ahead).
                    let need_ack = semi_sync_active && idx == last;
                    let mut packet = Vec::with_capacity(event.bytes.len() + 3);
                    frame_binlog_packet(&mut packet, &event.bytes, semi_sync_capable, need_ack);
                    let written = if semi_sync_capable {
                        // Read ACKs from the same poll that advances a pending
                        // write. Never cancel a partially transmitted frame.
                        let mut writer = BinlogAckWriter {
                            stream: &mut self.stream,
                            manager: &manager,
                            server_id: dump_server_id,
                            pending: &mut ack_buf,
                            scratch: &mut ack_read_tmp,
                            max_frame: MAX_SEMI_SYNC_ACK_FRAME,
                        };
                        self.codec
                            .write_packet_seq(&mut writer, sequence_id, &packet)
                            .await
                    } else {
                        self.codec
                            .write_packet_seq(&mut self.stream, sequence_id, &packet)
                            .await
                    };
                    let next_sequence = match written {
                        Ok(next) => next,
                        Err(_) => return Ok(()),
                    };
                    sequence_id = if need_ack { 1 } else { next_sequence };
                }
                // Advance past excluded transactions too, including a whole
                // batch containing no packets to send.
                (file, position) = tail;
                continue;
            }

            if non_blocking {
                let eof = self.eof_packet();
                self.codec
                    .write_packet(&mut self.stream, sequence_id, &eof)
                    .await?;
                return Ok(());
            }

            if let Some(handle) = &self.handle {
                handle.set_base_state(ProcessState::WaitingForBinlogUpdates);
            }

            let heartbeat_interval = heartbeat_interval_from_session(&self.context);
            tokio::select! {
                // Consume ready acknowledgements before waiting for more work.
                biased;
                // Drain semi-sync ack packets ([0xEF][pos][file]) the replica
                // sends on the same connection. Uses a cancel-safe raw `read`
                // (NOT the codec's read_exact-based read_packet, which loses
                // partially-consumed bytes if this select branch is cancelled by
                // a new event / heartbeat and desyncs the dump stream) into a
                // persistent buffer, then parses whole MySQL frames out of it.
                res = self.stream.read(&mut ack_read_tmp) => {
                    match res {
                        Ok(0) | Err(_) => return Ok(()), // replica closed / errored
                        Ok(n) => {
                            if !semi_sync_capable {
                                return Ok(());
                            }
                            ack_buf.extend_from_slice(&ack_read_tmp[..n]);
                            let (acks, _) = parse_semi_sync_ack_frames(&mut ack_buf);
                            if ack_buf.len() > MAX_SEMI_SYNC_ACK_FRAME {
                                let err = command_err_packet_for_spec(
                                    crate::mysql::errors::ER_UNKNOWN_COM_ERROR,
                                    "semi-sync ACK frame exceeds 1 KiB",
                                );
                                self.codec.write_packet(&mut self.stream, sequence_id, &err).await?;
                                return Ok(());
                            }
                            if let Some(sid) = dump_server_id {
                                for (pos, ack_file) in acks {
                                    manager.record_semi_sync_ack(sid, &ack_file, pos);
                                }
                            }
                        }
                    }
                }
                _ = &mut notified => {}
                _ = tokio::time::sleep(heartbeat_interval) => {
                    match manager.heartbeat_event() {
                        Ok(heartbeat) => {
                            if let Some(handle) = &self.handle {
                                handle.set_base_state(ProcessState::SendingBinlogEventToReplica);
                            }
                            let mut packet = Vec::with_capacity(heartbeat.len() + 3);
                            frame_binlog_packet(&mut packet, &heartbeat, semi_sync_capable, false);
                            if self.codec.write_packet(&mut self.stream, sequence_id, &packet).await.is_err() {
                                return Ok(());
                            }
                            sequence_id = sequence_id.wrapping_add(1);
                            if let Some(handle) = &self.handle {
                                handle.set_base_state(ProcessState::WaitingForBinlogUpdates);
                            }
                        }
                        Err(err) => {
                            self.write_error_packet(sequence_id, &err).await?;
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    async fn handle_query(&mut self, sql_bytes: &[u8]) -> io::Result<()> {
        let sql = match decode_session_sql(&self.context, sql_bytes) {
            Ok(sql) => sql,
            Err(err) => return self.write_query_response(Err(err)).await,
        };
        if sql
            .trim_matches(|ch: char| ch.is_ascii_whitespace() || ch == ';')
            .is_empty()
        {
            return self
                .write_query_response(Err(
                    crate::error::catalog::ER_EMPTY_QUERY.error("Query was empty")
                ))
                .await;
        }
        if !self.capabilities.has(CLIENT_MULTI_STATEMENTS) || !sql.as_bytes().contains(&b';') {
            return self.handle_single_query(sql).await;
        }
        let mode_bits = crate::engine::api::parse_temporal_sql_mode_bits(&self.context.sql_mode);
        let mode = crate::sql_lex::SqlLexMode::from_parser_bits(mode_bits);
        if !crate::engine::executor::has_non_trailing_statement_separator_with_mode(&sql, mode) {
            return self.handle_single_query(sql).await;
        }
        let sql = if !self.context.auth_sandbox && looks_like_text_dml_candidate(&sql, mode) {
            self.acquire_txn_admission().await;
            match self.try_handle_text_dml_batch(sql).await? {
                Some(sql) => sql,
                None => return Ok(()),
            }
        } else {
            sql
        };
        let batch = crate::parser::split_query_statements(&sql, mode_bits);
        if batch.1.len() <= 1 {
            return self.handle_single_query(sql).await;
        }
        self.text_batch = Some(TextBatchProgress {
            sequence_id: self.command_response_sequence,
            more: true,
            failed: false,
        });
        let result = self.handle_query_parts(&batch.0, batch.1, mode_bits).await;
        let flush = if result.is_ok() {
            self.flush_text_batch_packets().await
        } else {
            Ok(())
        };
        self.text_batch_packets.clear();
        self.text_batch = None;
        result.and(flush)
    }

    /// Keep the compiled batch/cache and physical row coalescing route, but
    /// encode one OK from each logical statement's own metadata.
    async fn try_handle_text_dml_batch(&mut self, sql: String) -> io::Result<Option<String>> {
        let engine = Arc::clone(&self.engine);
        let mut context = self.park_context();
        let (context, sql, batch) =
            crate::util::blocking_pool::spawn_session(self.blocking_affinity.as_ref(), move || {
                let batch = engine.execute_text_dml_batch(&mut context, &sql);
                (context, sql, batch)
            })
            .await
            .map_err(|err| {
                self.recover_parked_context();
                io::Error::other(err.to_string())
            })?;
        self.restore_context(context);
        self.context.refresh_cancel_generations();
        let Some((statements, result)) = batch else {
            return Ok(Some(sql));
        };
        self.engine.log_general_event(&self.context, "Query", &sql);
        let error = result.err();
        let count = statements.len() + usize::from(error.is_some());
        let additional = count.saturating_sub(1) as u64;
        crate::mysql::connection_manager::record_statements(additional);
        for counter in [
            crate::engine::status_counters::SessionCounter::Questions,
            crate::engine::status_counters::SessionCounter::Queries,
        ] {
            crate::engine::status_counters::add_session_counter(&self.context, counter, additional);
        }
        if self.kill_flag.load(Ordering::Acquire) {
            return Ok(None);
        }
        let capabilities = self.client_capabilities();
        let escape_status = self.server_status() & 0x0200;
        let mut writer = self.codec.batch_writer(&mut self.stream);
        let mut payload = Vec::new();
        for (index, statement) in statements.into_iter().enumerate() {
            let mut status = escape_status
                | if statement.autocommit {
                    SERVER_STATUS_AUTOCOMMIT
                } else {
                    0
                }
                | if statement.in_transaction {
                    SERVER_STATUS_IN_TRANS
                } else {
                    0
                };
            if index + 1 < count {
                status |= SERVER_MORE_RESULTS_EXISTS;
            }
            let affected = if capabilities & CLIENT_FOUND_ROWS != 0 {
                statement.matched_rows.unwrap_or(statement.affected_rows)
            } else {
                statement.affected_rows
            };
            ok_packet_with_full_payload_capabilities_into(
                affected,
                statement.insert_id,
                status,
                statement.warnings.len().min(u16::MAX as usize) as u16,
                &statement.info,
                &[],
                capabilities,
                &mut payload,
            );
            writer
                .write_packet(
                    (index as u8).wrapping_add(self.command_response_sequence),
                    &payload,
                )
                .await?;
            payload.clear();
        }
        if let Some(error) = error {
            writer
                .write_packet(
                    (count as u8)
                        .wrapping_sub(1)
                        .wrapping_add(self.command_response_sequence),
                    &crate::mysql::protocol::err_packet(&error),
                )
                .await?;
        }
        writer.finish().await?;
        self.context.pending_statement_info = None;
        self.context.pending_statement_matched_rows = None;
        crate::engine::api::take_statement_info();
        crate::engine::api::take_statement_matched_rows();
        Ok(None)
    }

    async fn handle_query_parts(
        &mut self,
        sql: &str,
        ranges: Vec<std::ops::Range<usize>>,
        mut mode_bits: u16,
    ) -> io::Result<()> {
        let mut remaining = ranges;
        let mut effective = std::borrow::Cow::Borrowed(sql);
        let mut index = 0;
        let mut executed = 0;
        while index < remaining.len() {
            let range = remaining[index].clone();
            self.text_batch.as_mut().unwrap().more = index + 1 < remaining.len();
            let child = effective[range.clone()].to_string();
            if executed > 0 {
                crate::mysql::connection_manager::record_statement();
                for counter in [
                    crate::engine::status_counters::SessionCounter::Questions,
                    crate::engine::status_counters::SessionCounter::Queries,
                ] {
                    crate::engine::status_counters::add_session_counter(&self.context, counter, 1);
                }
            }
            if let Some(kind) = crate::engine::status_counters::classify_query_com(&child) {
                crate::engine::status_counters::record_session_com(&self.context, kind);
            }
            self.context.refresh_cancel_generations();
            self.handle_single_query(child).await?;
            executed += 1;
            if self.text_batch.as_ref().unwrap().failed
                || self.context.close_after_response
                || self.kill_flag.load(Ordering::Acquire)
            {
                break;
            }
            index += 1;
            let next_mode =
                crate::engine::api::parse_temporal_sql_mode_bits(&self.context.sql_mode);
            if next_mode != mode_bits && index < remaining.len() {
                // A preceding SET sql_mode controls how the remaining source is split.
                let tail = &effective[range.end + 1..];
                let (next_sql, next_ranges) =
                    crate::parser::split_query_statements(tail, next_mode);
                effective = std::borrow::Cow::Owned(next_sql.into_owned());
                remaining = next_ranges;
                index = 0;
                mode_bits = next_mode;
            }
        }
        Ok(())
    }

    async fn handle_single_query(&mut self, sql: String) -> io::Result<()> {
        if let Some(cache) = self.text_shape_memos.as_mut() {
            if cache.database != self.context.current_database {
                cache.slots.fill_with(|| None);
                cache.database.clone_from(&self.context.current_database);
            }
        }
        if let Some(err) = self.context.cancelled_error() {
            return self.write_query_response(Err(err)).await;
        }
        if let Some(handle) = &self.handle {
            handle.set_base_state(ProcessState::Executing);
        }
        let lex_mode = crate::sql_lex::SqlLexMode::from_parser_bits(
            crate::engine::api::parse_temporal_sql_mode_bits(&self.context.sql_mode),
        );
        if !self.capabilities.has(CLIENT_MULTI_STATEMENTS) {
            match query_requires_multi_statements(
                sql.as_str(),
                crate::engine::api::parse_temporal_sql_mode_bits(&self.context.sql_mode),
            ) {
                Ok(false) => {}
                Ok(true) => {
                    return self
                        .write_query_response(Err(error(
                            crate::error::catalog::ER_PARSE_ERROR,
                            "CLIENT_MULTI_STATEMENTS is required for multiple statements",
                        )))
                        .await;
                }
                Err(err) => return self.write_query_response(Err(err)).await,
            }
        }
        // MySQL 9.x clients issue `select $$` during login bootstrap as an
        // expired-password sandbox probe. It is intentionally invalid SQL; keep
        // the wire-visible 1054 response, but avoid logging it as an engine
        // failure on every successful connection.
        if is_mysql_client_sandbox_probe(sql.as_str()) {
            return self
                .write_query_response(Err(error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    "Unknown column '$$' in 'field list'",
                )))
                .await;
        }
        // PROCESSLIST is an operational escape hatch. Running this tiny,
        // read-only registry snapshot through the blocking executor pool can
        // make it unobservable precisely when every worker is occupied by a
        // long SLEEP/lock wait. Execute it on the connection task instead;
        // no storage I/O or row scan is involved.
        if is_show_processlist_sql(&sql) {
            let result = self.engine.execute(&mut self.context, &sql);
            return self.write_query_response(result).await;
        }
        // A replica advertises semi-sync capability by issuing
        // `SET @rpl_semi_sync_slave=1` (8.0.26+: `@rpl_semi_sync_replica`)
        // before its COM_BINLOG_DUMP. Record it on the session so the later
        // dump stream frames events with the semi-sync header and reads acks.
        // The statement still executes normally (sets the user variable, OK).
        if let Some(on) = detect_semi_sync_slave_request(sql.as_str()) {
            self.semi_sync_replica_requested = on;
        }
        self.engine
            .log_general_event(&self.context, "Query", sql.as_str());
        if self.context.auth_sandbox && !is_password_change_statement(sql.as_str()) {
            return self
                .write_query_response(Err(error(
                    crate::error::catalog::ER_MUST_CHANGE_PASSWORD,
                    "You must reset your password using ALTER USER before executing this statement",
                )))
                .await;
        }
        match ddl_cache_scope(sql.as_str(), lex_mode) {
            DdlCacheScope::None => {}
            // Prepared handles have a lifetime independent from DDL and
            // capture their prepare-time database. Table plans validate
            // schema-version dependencies lazily on the next EXECUTE.
            DdlCacheScope::SessionOnly | DdlCacheScope::TableScoped => {}
            DdlCacheScope::Global => {
                self.engine.invalidate_statement_cache();
            }
        }

        // Catalog-aware dispatch includes HTTP hidden behind views, routines,
        // or triggers before any inline SQL path can block a Tokio worker.
        if self.sql_may_call_http(sql.as_str())
            && !looks_like_load_data_local(sql.as_str(), lex_mode)
        {
            let result = self.execute_engine_blocking(sql.to_string()).await;
            return self.write_query_response(result).await;
        }

        // Bare transaction control never waits for a row lock: COMMIT
        // releases row locks before its durability barrier and ROLLBACK only
        // applies this session's own undo. Its blocking work (group-commit
        // fsync) therefore matches the autocommit DML that already executes
        // inline on this runtime worker, so it follows the same policy instead
        // of paying a blocking-pool round trip on every transaction.
        // A plain session-variable SET (e.g. JDBC's per-query
        // `SET SQL_SELECT_LIMIT`) is bounded the same way: it evaluates no
        // subquery or function and changes only this session's state.
        if prepared_dml_inline_enabled()
            && (crate::engine::executor::is_simple_txn_control_sql(sql.as_str())
                || is_simple_session_set_sql(sql.as_str()))
        {
            let result = self.engine.execute(&mut self.context, &sql);
            if self.kill_flag.load(Ordering::Acquire) {
                return Ok(());
            }
            return self.write_query_response(result).await;
        }

        // Inline scheduling route for non-blocking read-only SELECTs.
        //
        // The default COM_QUERY path hands every statement to
        // `tokio::task::spawn_blocking`, which costs a thread-pool task
        // switch (~5–15µs on macOS) plus a `SessionContext` move dance.
        // Pure read-only SELECTs never park on the row-lock condvar, so
        // running them on the current tokio worker is safe and trims
        // a meaningful slice of the per-query latency for trivial
        // queries like `SELECT 2`.
        //
        // The dispatch below mirrors `execute_prepared_statement_blocking`'s
        // `statement_is_lock_safe_read` check: a parse + classification
        // gate blocks anything that could take a row lock (FOR UPDATE,
        // any DML, DDL, txn control) from the inline path. On a sniff
        // miss, parse error, or non-lock-safe verdict we fall through
        // to the blocking-pool route so LOAD DATA LOCAL, multi-statement,
        // DML kernels and lock-wait semantics remain unchanged.
        let mut needs_txn_admission = false;
        let mut parsed_for_blocking = None;
        if looks_like_inline_select_candidate(sql.as_str())
            && !crate::engine::executor::source_syntax_warning_scan_required(
                sql.as_str(),
                crate::engine::api::parse_temporal_sql_mode_bits(&self.context.sql_mode),
            )
        {
            let parser_mode_bits =
                crate::engine::api::parse_temporal_sql_mode_bits(&self.context.sql_mode);
            // Literal-heavy COM_QUERY clients rarely repeat exact bytes, but
            // do repeat a small set of parser-proven placeholder shapes. A
            // shape hit carries both AST and values; unlike the earlier
            // inline-only shortcut, keep it for locking SELECTs too so the
            // blocking worker does not parse either.
            let shaped = self
                .engine
                .cached_text_statement(&self.context, sql.as_str())
                .map(|(parsed, params, shape_sql)| {
                    let class = match parsed.as_ref() {
                        Statement::Select(select) => Some(
                            crate::engine::cache::SelectExecutionClass::from_select(select),
                        ),
                        _ => None,
                    };
                    (parsed, class, Some((params, shape_sql)))
                });
            let cached = shaped.or_else(|| {
                self.parsed_statement_l0
                    .as_ref()
                    .filter(|memo| {
                        memo.parser_mode_bits == parser_mode_bits
                            && memo.sql.as_ref() == sql.as_str()
                    })
                    .map(|memo| (Arc::clone(&memo.statement), memo.execution_class, None))
            });
            let parsed_and_class = match cached {
                Some(value) => {
                    if value.2.is_none() {
                        self.engine.record_select_parse_cache_hit();
                    }
                    Ok(value)
                }
                None => self
                    .engine
                    .parse_cached_statement(&self.context, sql.as_str())
                    .map(|parsed| {
                        let execution_class = match parsed.as_ref() {
                            Statement::Select(select) => Some(
                                crate::engine::cache::SelectExecutionClass::from_select(select),
                            ),
                            _ => None,
                        };
                        self.parsed_statement_l0 = Some(ParsedStatementMemo {
                            sql: Arc::from(sql.as_str()),
                            parser_mode_bits,
                            statement: Arc::clone(&parsed),
                            execution_class,
                        });
                        (parsed, execution_class, None)
                    }),
            };
            if let Ok((parsed, execution_class, params)) = parsed_and_class {
                if execution_class
                    == Some(crate::engine::cache::SelectExecutionClass::ReadOnlyNonBlocking)
                    && statement_is_lock_safe_read(parsed.as_ref())
                    && context_allows_inline_lock_safe_read(&self.context)
                {
                    // Deliberately no `materialize` here: when the engine
                    // returns a `StreamingResultSet`, `write_query_response`
                    // (and onward `write_text_streaming_result_set` /
                    // `write_binary_streaming_result_set`) consumes the
                    // cursor on demand. Materializing up front would
                    // collect the whole result in server memory before the
                    // first wire packet, which is precisely what blew up
                    // clients before server-side cursor support landed.
                    let result = match params {
                        Some((params, shape_sql)) => {
                            let (mut memo, memo_is_new) = self
                                .text_shape_memos
                                .as_mut()
                                .map(|cache| cache.take_or_new(&shape_sql))
                                .map_or((None, false), |(memo, is_new)| (Some(memo), is_new));
                            // Parser-shape artifacts are deliberately shared
                            // across databases and therefore remain
                            // unqualified. Prepared SELECT compilation,
                            // however, expects the PREPARE-time qualified AST.
                            // Qualify one short-lived clone only when this
                            // per-connection shape memo is born; Ready states
                            // never inspect `parsed` again.
                            let qualified = self.qualify_new_text_shape(&sql, &parsed, memo_is_new);
                            let compile_parsed = qualified.as_ref().unwrap_or(&parsed);
                            // Bind Performance Schema's digest to the shape,
                            // not to this execution's literal bytes. Without
                            // it every literal spelling is a fresh key in the
                            // per-thread digest cache: the tokenizer + SHA-256
                            // rerun on each statement, and the cache overflows
                            // continuously. `execute_parsed_with_params`
                            // (the compile-miss fallback just below) already
                            // scopes the same override.
                            let compiled = memo.as_mut().and_then(|memo| {
                                crate::engine::perf_events::with_statement_digest_sql_override(
                                    Arc::clone(&shape_sql),
                                    || {
                                        crate::engine::api::with_shared_result_metadata(|| {
                                            self.engine.execute_prepared_select(
                                                &mut self.context,
                                                &mut memo.state,
                                                compile_parsed,
                                                sql.as_str(),
                                                &params,
                                            )
                                        })
                                    },
                                )
                            });
                            if let (Some(cache), Some(memo)) =
                                (self.text_shape_memos.as_mut(), memo)
                            {
                                cache.put(memo);
                            }
                            if compiled.as_ref().is_some_and(|result| {
                                result.as_ref().is_err_and(|err| {
                                    crate::storage::is_lock_wait_pending_error(err)
                                        || err.is(crate::error::catalog::ER_NEED_REPREPARE)
                                })
                            }) {
                                // MDL can block even a pure SELECT. Retry through
                                // the authoritative dispatcher on a blocking worker,
                                // choosing a fresh view/plan after the DDL barrier.
                                let result = self
                                    .execute_engine_blocking_parsed(
                                        sql,
                                        parsed,
                                        Some((params, shape_sql)),
                                    )
                                    .await;
                                return self.write_query_response(result).await;
                            }
                            match compiled {
                                Some(result) => result,
                                None => self.engine.execute_parsed_with_params(
                                    &mut self.context,
                                    sql.as_str(),
                                    parsed,
                                    params,
                                    shape_sql,
                                ),
                            }
                        }
                        None => self
                            .engine
                            .execute_parsed(&mut self.context, sql.as_str(), parsed),
                    };
                    return self.write_query_response(result).await;
                }
                // A parsed SELECT that cannot use the non-blocking route may
                // acquire row locks (FOR UPDATE / FOR SHARE). Gate it before
                // it consumes a blocking-pool worker, just like a prepared
                // locking read.
                needs_txn_admission = !statement_is_lock_safe_read(parsed.as_ref())
                    || !context_allows_inline_lock_safe_read(&self.context);
                parsed_for_blocking = Some((parsed, params));
            }
        }

        // Compile-once / late-bind route for literal COM_QUERY DML.
        //
        // Server-side prepared INSERT/UPDATE/DELETE already reuse a compiled
        // plan and bind values late (`execute_prepared_dml`). A client that
        // sends the same statement shape as text with the literals inlined —
        // every JDBC client without `useServerPrepStmts`, and every hand-built
        // SQL string — got none of that: a full tokenize + parse + plan on
        // every execution, then a blocking-pool hand-off. Measured on TPC-C at
        // 4 warehouses / 20 terminals, that gap was the whole difference
        // between the two protocols.
        //
        // The shape lookup is only a *hit* after the same shape has already
        // completed a full parse + auto-parameterization pass whose extracted
        // values matched this scanner's byte for byte
        // (`cache_text_statement_shape`), so the AST executed here is the
        // parser's, never the scanner's guess.
        if parsed_for_blocking.is_none() && looks_like_text_dml_candidate(sql.as_str(), lex_mode) {
            if let Some((parsed, params, shape_sql)) = self
                .engine
                .cached_text_statement(&self.context, sql.as_str())
            {
                if matches!(
                    parsed.as_ref(),
                    Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
                ) {
                    return self
                        .execute_text_dml_shape(sql, parsed, params, shape_sql)
                        .await;
                }
            }
        }

        // Avoid parsing the full statement just to detect LOAD DATA LOCAL.
        // The dispatch path needs to know about LOCAL up-front because the
        // client streams the file inline over the wire — every other path
        // (including server-side LOAD DATA) goes through the engine. Sniff
        // the leading keywords at the byte level so 500KB-payload bulk
        // INSERTs no longer pay for a full sqlparser pass here.
        if looks_like_load_data_local(sql.as_str(), lex_mode) {
            let _mode_guard = crate::parser::ParserSqlModeGuard::activate(
                crate::engine::api::parse_temporal_sql_mode_bits(&self.context.sql_mode),
            );
            if let Ok(Statement::LoadData(ref load_stmt)) = parse_statement(&sql) {
                if load_stmt.local {
                    return self
                        .handle_load_data_local(sql.to_string(), load_stmt.clone())
                        .await;
                }
            }
        }

        // A plain autocommit UPDATE carrying a large direct literal can use
        // the engine's compile-once/late-bind DML vehicle on this runtime
        // worker, matching COM_STMT_EXECUTE's established inline policy. The
        // engine performs the authoritative AST/session eligibility check and
        // returns `None` before execution for every other shape, which then
        // follows the normal blocking-pool route below.
        if prepared_dml_inline_allowed(&self.context)
            && looks_like_large_text_update_candidate(sql.as_str())
        {
            if let Some(result) = self
                .engine
                .execute_large_text_dml_inline(&mut self.context, sql.as_str())
            {
                if self.kill_flag.load(Ordering::Acquire) {
                    return Ok(());
                }
                return self.write_query_response(result).await;
            }
        }

        // Capture only the bits of `sql` we need after the blocking call
        // (the auth-sandbox path peeks at the leading keyword), then move
        // the `String` itself into `execute_engine_blocking` so the
        // 500KB body never gets cloned just to satisfy the `'static`
        // bound on `spawn_blocking`.
        let needs_password_check = self.context.auth_sandbox;
        let selects_database = sql
            .trim_start()
            .split_ascii_whitespace()
            .next()
            .is_some_and(|keyword| keyword.eq_ignore_ascii_case("USE"));
        let pw_check_prefix: Option<String> = if needs_password_check {
            // is_password_change_statement only inspects the first few
            // tokens. Capture a prefix that's safely longer than the
            // longest matched form (`set password` / `alter user …
            // identified …`) so the post-call check sees enough.
            let prefix_len = sql.len().min(128);
            Some(sql[..prefix_len].to_string())
        } else {
            None
        };
        if needs_txn_admission || looks_like_text_dml_candidate(sql.as_str(), lex_mode) {
            self.acquire_txn_admission().await;
        }
        let result = match parsed_for_blocking {
            Some((parsed, Some((params, shape_sql)))) if self.text_shape_memos.is_some() => {
                let (mut memo, memo_is_new) = self
                    .text_shape_memos
                    .as_mut()
                    .expect("shape cache checked above")
                    .take_or_new(&shape_sql);
                let qualified = self.qualify_new_text_shape(&sql, &parsed, memo_is_new);
                let compile_parsed = qualified.unwrap_or_else(|| Arc::clone(&parsed));
                let (dml_state, result) = self
                    .execute_engine_blocking_shape(
                        sql,
                        compile_parsed,
                        params,
                        Arc::clone(&shape_sql),
                        std::mem::take(&mut memo.dml_state),
                    )
                    .await;
                memo.dml_state = dml_state;
                if let Some(cache) = self.text_shape_memos.as_mut() {
                    cache.put(memo);
                }
                result
            }
            Some((parsed, params)) => {
                self.execute_engine_blocking_parsed(sql, parsed, params)
                    .await
            }
            None => self.execute_engine_blocking(sql).await,
        };
        if result.is_ok() && selects_database {
            self.process_database = Some(self.context.current_database.clone());
        }
        if result.is_ok()
            && needs_password_check
            && pw_check_prefix
                .as_deref()
                .is_some_and(is_password_change_statement)
        {
            self.context.auth_sandbox = false;
            self.context.password_expired = false;
        }
        if self.kill_flag.load(Ordering::Acquire) && !self.context.close_after_response {
            return Ok(());
        }
        self.write_query_response(result).await
    }

    async fn flush_text_batch_packets(&mut self) -> io::Result<()> {
        if self.text_batch_packets.is_empty() {
            return Ok(());
        }
        let mut writer = self.codec.batch_writer(&mut self.stream);
        for (sequence, payload) in self.text_batch_packets.drain(..) {
            writer.write_packet(sequence, &payload).await?;
        }
        writer.finish().await
    }

    async fn write_query_response(
        &mut self,
        result: Result<QueryResponse, QueryError>,
    ) -> io::Result<()> {
        if let Some(handle) = &self.handle {
            handle.set_base_state(ProcessState::QueryEnd);
        }
        let _sending = self
            .handle
            .as_ref()
            .map(|handle| handle.enter_stage(ProcessState::SendingToClient));
        if self.context.close_after_response {
            if let Some(progress) = &mut self.text_batch {
                progress.more = false;
            }
        }
        if let Some(progress) = self.text_batch {
            let affected = match &result {
                Ok(QueryResponse::Ok) => Some(0),
                Ok(QueryResponse::OkAffected { affected_rows }) => Some(*affected_rows),
                _ => None,
            };
            if let Some(affected) = affected {
                let payload = self.ok_packet_with_affected_rows_full(
                    affected,
                    self.server_status_with_more(progress.more),
                );
                self.text_batch_packets
                    .push((progress.sequence_id, payload));
                self.text_batch.as_mut().unwrap().sequence_id =
                    progress.sequence_id.wrapping_add(1);
                if !progress.more || self.text_batch_packets.len() >= 64 {
                    self.flush_text_batch_packets().await?;
                }
                return Ok(());
            }
            self.flush_text_batch_packets().await?;
            let result = match result {
                Ok(QueryResponse::MultiResult(_))
                    if !self.capabilities.has(CLIENT_MULTI_RESULTS) =>
                {
                    Err(error(
                        crate::error::catalog::ER_SP_BADSELECT,
                        "Stored procedure requires CLIENT_MULTI_RESULTS",
                    ))
                }
                other => other,
            };
            let next_sequence = match result {
                Ok(response) => {
                    self.write_text_multi_result_seq(
                        vec![response],
                        progress.sequence_id,
                        progress.more,
                    )
                    .await?
                }
                Err(err) => {
                    self.text_batch.as_mut().unwrap().failed = true;
                    self.write_error_packet(progress.sequence_id, &err).await?;
                    progress.sequence_id.wrapping_add(1)
                }
            };
            self.text_batch.as_mut().unwrap().sequence_id = next_sequence;
            return Ok(());
        }
        match result {
            Ok(QueryResponse::Ok) => self.write_ok_packet(self.command_response_sequence).await?,
            Ok(QueryResponse::OkAffected { affected_rows }) => {
                self.write_ok_packet_with_affected_rows(
                    self.command_response_sequence,
                    affected_rows,
                )
                .await?
            }
            Ok(QueryResponse::ResultSet(result)) => self.write_text_result_set(&result).await?,
            Ok(QueryResponse::SharedResultSet(result)) => {
                self.write_text_shared_result_set(&result).await?
            }
            Ok(QueryResponse::StreamingResultSet(result)) => {
                self.write_text_streaming_result_set(result).await?
            }
            Ok(QueryResponse::MultiResult(results)) => {
                if self.capabilities.has(CLIENT_MULTI_RESULTS) {
                    self.write_text_multi_result(results).await?
                } else {
                    let err = error(
                        crate::error::catalog::ER_SP_BADSELECT,
                        "Stored procedure returned multiple result sets, but CLIENT_MULTI_RESULTS was not negotiated",
                    );
                    self.write_error_packet(self.command_response_sequence, &err)
                        .await?;
                }
            }
            Err(err) => {
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
            }
        }
        Ok(())
    }

    async fn write_query_response_at(
        &mut self,
        result: Result<QueryResponse, QueryError>,
        sequence_id: u8,
    ) -> io::Result<()> {
        let standalone = self.text_batch.is_none();
        let progress = self.text_batch.get_or_insert(TextBatchProgress {
            sequence_id,
            more: false,
            failed: false,
        });
        progress.sequence_id = sequence_id;
        let result = self.write_query_response(result).await;
        if standalone {
            self.text_batch = None;
        }
        result
    }

    async fn handle_load_data_local(
        &mut self,
        _sql: String,
        #[allow(unused_mut)] mut load_stmt: crate::parser::LoadData,
    ) -> io::Result<()> {
        if let Some(err) = self.context.cancelled_error() {
            return self.write_query_response(Err(err)).await;
        }
        if !self
            .capabilities
            .has(crate::mysql::protocol::CLIENT_LOCAL_FILES)
        {
            let err = error(
                crate::error::catalog::ER_CLIENT_LOCAL_FILES_DISABLED,
                "Loading local data is disabled by the client capability flags",
            );
            return self.write_query_response(Err(err)).await;
        }
        // G5 — `local_infile` gate. MySQL 8.0 refuses a client-side
        // LOCAL INFILE request when the server has `local_infile=OFF`,
        // replying with ER_LOAD_INFILE_CAPABILITY_DISABLED (3948) and
        // NOT sending the 0xFB file-request packet. We must decide this
        // before writing 0xFB, otherwise the client would stream the
        // whole file only to be rejected afterwards.
        let local_infile_on = crate::engine::sysvar::read_var(
            "local_infile",
            crate::engine::sysvar::Scope::Global,
            Some(&self.context),
            None,
        )
        .ok()
        .and_then(|v| match v {
            crate::types::Value::Int(n) => Some(n != 0),
            crate::types::Value::UInt(n) => Some(n != 0),
            crate::types::Value::String(s) => {
                Some(s.eq_ignore_ascii_case("on") || s == "1" || s.eq_ignore_ascii_case("true"))
            }
            _ => None,
        })
        .unwrap_or(true);
        if !local_infile_on {
            let err = error(
                crate::error::catalog::ER_CLIENT_LOCAL_FILES_DISABLED,
                "Loading local data is disabled; this must be enabled on both the client and \
                 server sides",
            );
            return self.write_query_response(Err(err)).await;
        }
        self.flush_text_batch_packets().await?;
        let filename = load_stmt.file_path.clone();
        let mut request_payload = Vec::with_capacity(1 + filename.len());
        request_payload.push(crate::mysql::protocol::LOCAL_INFILE_REQUEST);
        request_payload.extend_from_slice(filename.as_bytes());
        self.codec
            .write_packet(
                &mut self.stream,
                self.text_batch
                    .map_or(self.command_response_sequence, |progress| {
                        progress.sequence_id
                    }),
                &request_payload,
            )
            .await?;

        #[cfg(unix)]
        {
            return self
                .handle_load_data_local_streaming_after_request(load_stmt)
                .await;
        }

        #[cfg(not(unix))]
        {
            // Track the sequence id of the last packet we received from the
            // client. The MySQL wire protocol requires every reply in a
            // command exchange to increment by one against the previous
            // packet — for LOCAL INFILE that previous packet is the empty
            // terminator the client just sent, not the LOCAL_INFILE_REQUEST
            // we sent at seq=1. Replying with a hardcoded seq=1 lets the
            // file data land on disk but then breaks the connection: MySQL
            // / JDBC clients abort with "Lost connection to MySQL server
            // during query" (ER 2013) as soon as they see the non-monotonic
            // sequence. Seed with 1 (the request we just wrote) so a client
            // that disconnects before sending any data still produces a
            // legal seq=2 reply on the (likely broken) socket.
            let data_dir = self.engine.protocol_temp_dir();
            let mut staging = LocalInfileStaging::new();
            let mut staging_error: Option<QueryError> = None;
            let last_seq: u8;
            loop {
                match self
                    .codec
                    .read_packet_with_timeout(
                        &mut self.stream,
                        crate::mysql::packet::net_read_timeout(),
                    )
                    .await?
                {
                    Some(packet) if packet.payload.is_empty() => {
                        last_seq = packet.sequence_id;
                        break;
                    }
                    Some(packet) => {
                        if staging_error.is_none() {
                            if let Err(err) =
                                staging.push(&packet.payload, &data_dir, self.connection_id)
                            {
                                // Continue draining until the client's empty
                                // terminator so the connection stays in sync, but
                                // stop retaining/writing bytes after first error.
                                staging_error = Some(err);
                            }
                        }
                    }
                    None => return Ok(()),
                }
            }
            let response_seq = last_seq.wrapping_add(1);
            if let Some(err) = staging_error {
                return self.write_query_response_at(Err(err), response_seq).await;
            }
            let reader = match staging.into_reader() {
                Ok(reader) => reader,
                Err(err) => {
                    return self.write_query_response_at(Err(err), response_seq).await;
                }
            };
            load_stmt.data = None;

            let engine = Arc::clone(&self.engine);
            let mut context = self.park_context();
            let join_result = crate::util::blocking_pool::spawn_affine(
                self.blocking_affinity.as_ref(),
                move || {
                    let statement = Statement::LoadData(load_stmt);
                    let result = engine
                        .execute_load_data_statement_from_reader(&mut context, statement, reader)
                        .and_then(QueryResponse::materialize);
                    (context, result)
                },
            )
            .await;
            let (context, result) = match join_result {
                Ok(pair) => pair,
                Err(e) => {
                    let err = self.recover_join_error(e);
                    return self.write_query_response_at(Err(err), response_seq).await;
                }
            };
            self.restore_context(context);
            if self.kill_flag.load(Ordering::Acquire) {
                return Ok(());
            }

            self.write_query_response_at(result, response_seq).await
        }
    }

    /// Stream LOCAL INFILE packets into the blocking LOAD executor through a
    /// bounded Unix socket pair. The async writer and synchronous reader run
    /// concurrently, so multi-gigabyte imports no longer perform a complete
    /// protocol-spill write followed by a second full-file read. Socket
    /// backpressure bounds resident/kernel buffering; non-Unix targets retain
    /// the staging-file implementation above.
    #[cfg(unix)]
    async fn handle_load_data_local_streaming_after_request(
        &mut self,
        mut load_stmt: crate::parser::LoadData,
    ) -> io::Result<()> {
        use tokio::io::AsyncWriteExt;

        let (reader, mut writer) = tokio::net::UnixStream::pair()?;
        let std_reader = reader.into_std()?;
        std_reader.set_nonblocking(false)?;
        load_stmt.data = None;

        let engine = Arc::clone(&self.engine);
        let mut context = self.park_context();
        let input_cancel = context.current_cancel_token().cloned();
        let execution =
            crate::util::blocking_pool::spawn_affine(self.blocking_affinity.as_ref(), move || {
                let statement = Statement::LoadData(load_stmt);
                let reader: Box<dyn std::io::Read + Send> = Box::new(std_reader);
                let result = engine
                    .execute_load_data_statement_from_reader(&mut context, statement, reader)
                    .and_then(QueryResponse::materialize);
                (context, result)
            });

        let mut pipe_error: Option<QueryError> = None;
        let last_seq = loop {
            let packet = match self
                .codec
                .read_packet_with_timeout(
                    &mut self.stream,
                    crate::mysql::packet::net_read_timeout(),
                )
                .await
            {
                Ok(packet) => packet,
                Err(err) => {
                    if let Some(token) = &input_cancel {
                        token.cancel_local();
                    }
                    drop(writer);
                    if let Ok((context, _)) = execution.await {
                        self.restore_context(context);
                    }
                    return Err(err);
                }
            };
            match packet {
                Some(packet) if packet.payload.is_empty() => break packet.sequence_id,
                Some(packet) => {
                    if pipe_error.is_none() {
                        if let Err(err) = writer.write_all(&packet.payload).await {
                            // Keep draining through the empty terminator so the
                            // command exchange remains framed. A closed reader
                            // normally means the executor already has a more
                            // precise SQL error; retain this I/O error only if
                            // execution would otherwise report success.
                            pipe_error = Some(error(
                                crate::error::catalog::ER_UNKNOWN_ERROR,
                                format!("LOAD DATA LOCAL stream pipe: {err}"),
                            ));
                        }
                    }
                }
                None => {
                    // EOF without the protocol's empty terminator is an
                    // aborted upload, never a successful partial LOAD.
                    if let Some(token) = &input_cancel {
                        token.cancel_local();
                    }
                    drop(writer);
                    if let Ok((context, _)) = execution.await {
                        self.restore_context(context);
                    }
                    return Ok(());
                }
            }
        };
        let response_seq = last_seq.wrapping_add(1);
        let _ = writer.shutdown().await;
        drop(writer);

        let join_result = execution.await;
        let (context, mut result) = match join_result {
            Ok(pair) => pair,
            Err(err) => {
                let err = self.recover_join_error(err);
                return self.write_query_response_at(Err(err), response_seq).await;
            }
        };
        self.restore_context(context);
        if result.is_ok() {
            if let Some(err) = pipe_error {
                result = Err(err);
            }
        }
        if self.kill_flag.load(Ordering::Acquire) {
            return Ok(());
        }
        self.write_query_response_at(result, response_seq).await
    }

    async fn handle_stmt_prepare(&mut self, sql_bytes: &[u8]) -> io::Result<()> {
        let prepare_started = std::time::Instant::now();
        self.context.clear_warnings();
        crate::engine::api::clear_statement_warnings();
        let sql = match decode_session_sql(&self.context, sql_bytes) {
            Ok(sql) => sql,
            Err(err) => {
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            }
        };
        self.engine
            .log_general_event(&self.context, "Prepare", sql.as_str());
        let mode_bits = crate::engine::api::parse_temporal_sql_mode_bits(&self.context.sql_mode);
        let lex_mode = crate::sql_lex::SqlLexMode::from_parser_bits(mode_bits);
        let prepared_sql =
            match crate::sql_lex::PreparedSql::new(&sql, lex_mode, crate::parser::MYSQL_VERSION_ID)
            {
                Ok(prepared) => prepared,
                Err(err) => {
                    let packet = command_err_packet_for_spec(
                        crate::mysql::errors::ER_PARSE_ERROR,
                        format!("{} at position {}", err.message, err.offset),
                    );
                    self.codec
                        .write_packet(&mut self.stream, self.command_response_sequence, &packet)
                        .await?;
                    return Ok(());
                }
            };
        if crate::sql_lex::effective_is_empty(
            prepared_sql.effective(),
            lex_mode,
            crate::parser::MYSQL_VERSION_ID,
        )
        .unwrap_or(false)
        {
            let spec = if sql
                .trim_matches(|ch: char| ch.is_ascii_whitespace() || ch == ';')
                .is_empty()
            {
                crate::error::catalog::ER_EMPTY_QUERY
            } else {
                crate::error::catalog::ER_UNSUPPORTED_PS
            };
            self.write_error_packet(
                self.command_response_sequence,
                &spec.error("Query contains no preparable statement"),
            )
            .await?;
            return Ok(());
        }

        let effective_sql = prepared_sql.effective().to_string();
        let param_count = match count_placeholders(&effective_sql, lex_mode) {
            Ok(count) => count,
            Err(err) => {
                let err = error(crate::error::catalog::ER_PARSE_ERROR, err.to_string());
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            }
        };
        if param_count > u16::MAX as usize {
            let err = error(
                crate::error::catalog::ER_PS_MANY_PARAM,
                "Prepared statement contains too many placeholders",
            );
            self.write_error_packet(self.command_response_sequence, &err)
                .await?;
            return Ok(());
        }
        if crate::engine::executor::has_non_trailing_statement_separator_with_mode(
            &effective_sql,
            lex_mode,
        ) {
            let err = error(
                crate::error::catalog::ER_PARSE_ERROR,
                "Server-side prepared statements must contain exactly one statement",
            );
            self.write_error_packet(self.command_response_sequence, &err)
                .await?;
            return Ok(());
        }
        let preparation = match self.engine.try_prepare_cached_inline(
            &mut self.context,
            &effective_sql,
            param_count,
        ) {
            Some(result) => result,
            None => {
                self.prepare_cached_blocking(effective_sql.clone(), param_count)
                    .await
            }
        };
        let (parsed, columns) = match preparation {
            Ok((parsed, columns)) => (Some(parsed), columns),
            Err(err) => {
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            }
        };
        if columns.len() > u16::MAX as usize {
            self.write_error_packet(
                self.command_response_sequence,
                &error(
                    crate::error::catalog::ER_TOO_MANY_FIELDS,
                    "Prepared statement contains too many result columns",
                ),
            )
            .await?;
            return Ok(());
        }
        if parsed
            .as_deref()
            .is_some_and(|stmt| matches!(stmt, crate::parser::Statement::LoadData(_)))
        {
            let err = error(
                crate::error::catalog::ER_UNSUPPORTED_PS,
                "LOAD DATA is not supported in prepared statements",
            );
            self.write_error_packet(self.command_response_sequence, &err)
                .await?;
            return Ok(());
        }
        let normalized_sql: Arc<str> = Arc::from(
            crate::engine::system::normalize_sql_with_mode(&effective_sql, lex_mode).as_str(),
        );
        let database: Arc<str> = Arc::from(self.context.current_database.as_str());
        let result_metadata =
            PreparedResultMetadata::from_columns(&columns, self.result_fallback_collation_id());
        let perf_texts = crate::engine::perf_events::prepared_statement_texts(&effective_sql);
        let cap = crate::engine::sysvar::builtin::orm_stubs::max_prepared_stmt_count();
        if !crate::engine::status_counters::try_reserve_prepared_stmt(cap) {
            let err = command_err_packet_for_spec(
                crate::mysql::errors::ER_MAX_PREPARED_STMT_COUNT_REACHED,
                format!(
                    "Can't create more than max_prepared_stmt_count statements (current value: {cap})"
                ),
            );
            self.codec
                .write_packet(&mut self.stream, self.command_response_sequence, &err)
                .await?;
            return Ok(());
        }
        let Some(statement_id) = self.allocate_statement_id() else {
            crate::engine::status_counters::adjust_prepared_stmt_count(-1);
            let err = command_err_packet_for_spec(
                crate::mysql::errors::ER_MAX_PREPARED_STMT_COUNT_REACHED,
                "No prepared statement identifiers are available",
            );
            self.codec
                .write_packet(&mut self.stream, self.command_response_sequence, &err)
                .await?;
            return Ok(());
        };
        let mut prepared = PreparedStatement {
            original_sql_sensitivity: crate::engine::http::SensitivityMemo::new(&sql),
            original_sql: Arc::from(sql.as_str()),
            sql: Arc::from(effective_sql.as_str()),
            normalized_sql,
            database,
            parser_sql_mode_bits: mode_bits,
            param_count,
            param_types: None,
            value_scratch: Vec::with_capacity(param_count),
            computed_ordering: parsed
                .as_deref()
                .is_some_and(|statement| statement_has_computed_ordering(statement)),
            com_kind: crate::engine::status_counters::classify_query_com(&effective_sql),
            parsed,
            long_data: vec![None; param_count],
            long_data_error: None,
            cursor: None,
            result_metadata,
            perf_texts,
            select_memo_state: crate::engine::api::PreparedSelectMemoState::default(),
            dml_state: crate::engine::api::PreparedDmlState::default(),
            memory_bytes: 0,
            timer_prepare: u64::try_from(
                prepare_started.elapsed().as_nanos().saturating_mul(1_000),
            )
            .unwrap_or(u64::MAX),
            count_execute: 0,
            sum_errors: 0,
            sum_timer_execute: 0,
            min_timer_execute: u64::MAX,
            max_timer_execute: 0,
            memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
                0,
            )
            .expect("zero-byte prepared memory charge"),
        };
        prepared.memory_bytes = prepared.estimated_memory_bytes();
        prepared.memory_charge =
            match crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
                prepared.memory_bytes as usize,
            ) {
                Ok(charge) => charge,
                Err(err) => {
                    crate::engine::status_counters::adjust_prepared_stmt_count(-1);
                    self.write_error_packet(self.command_response_sequence, &err)
                        .await?;
                    return Ok(());
                }
            };
        self.prepared_memory_bytes = self
            .prepared_memory_bytes
            .saturating_add(prepared.memory_bytes);
        self.prepared.insert(statement_id, prepared);
        self.publish_prepared_memory();
        crate::engine::executor::warn_successful_source_syntax_with_mode(&effective_sql, mode_bits);
        self.context
            .set_warnings(crate::engine::api::take_statement_warnings());
        let deprecate_eof = self.client_capabilities() & CLIENT_DEPRECATE_EOF != 0;
        let optional_metadata = self.capabilities.has(CLIENT_OPTIONAL_RESULTSET_METADATA);
        let send_prepare_metadata = !optional_metadata || self.context.resultset_metadata_full;

        let prepare_ok = stmt_prepare_ok_packet(
            statement_id,
            columns.len(),
            param_count,
            self.context.warning_count(),
            self.client_capabilities(),
            if send_prepare_metadata {
                crate::mysql::protocol::RESULTSET_METADATA_FULL
            } else {
                crate::mysql::protocol::RESULTSET_METADATA_NONE
            },
        );
        // A COM_STMT_PREPARE response is one logical protocol response, but
        // can contain dozens of small physical packets (OK, one definition
        // per parameter, EOF, result-column definitions, EOF).  Writing each
        // frame through `TcpStream::write_all` separately turns every packet
        // into a send syscall on the common plaintext path.  Connector/J and
        // ORMs create short-lived expanded batch statements frequently, so
        // that syscall fan-out can dominate otherwise cached PREPAREs.
        //
        // Stage the bounded response in the same governed packet batch used
        // by result sets.  Sequence ids and individual MySQL frame boundaries
        // are unchanged; only the number of kernel writes is reduced.  The
        // batch writer also preserves the compressed-protocol framing rules.
        let prepare_metadata = self
            .prepared
            .get(&statement_id)
            .and_then(|prepared| prepared.result_metadata.clone());
        let eof = (!deprecate_eof).then(|| self.eof_packet());
        let fallback_column_definitions =
            if send_prepare_metadata && prepare_metadata.is_none() && !columns.is_empty() {
                Some(
                    columns
                        .iter()
                        .map(|column| {
                            column_definition_packet_full(
                                &column.name,
                                column.column_type,
                                &column.source,
                            )
                        })
                        .collect::<Vec<_>>(),
                )
            } else {
                None
            };
        let mut batch = self.codec.batch_writer(&mut self.stream);
        batch
            .write_packet(self.command_response_sequence, &prepare_ok)
            .await?;
        let mut sequence_id = self.command_response_sequence.wrapping_add(1);
        for idx in 0..if send_prepare_metadata {
            param_count
        } else {
            0
        } {
            batch
                .write_packet(sequence_id, &parameter_definition_packet(idx + 1))
                .await?;
            sequence_id = sequence_id.wrapping_add(1);
        }
        if send_prepare_metadata && param_count > 0 && !deprecate_eof {
            batch
                .write_packet(sequence_id, eof.as_ref().expect("EOF packet is available"))
                .await?;
            sequence_id = sequence_id.wrapping_add(1);
        }
        if send_prepare_metadata {
            if let Some(metadata) = prepare_metadata.as_ref() {
                for packet in metadata.column_definitions.iter() {
                    batch.write_packet(sequence_id, packet).await?;
                    sequence_id = sequence_id.wrapping_add(1);
                }
            } else if let Some(definitions) = fallback_column_definitions.as_ref() {
                for packet in definitions {
                    batch.write_packet(sequence_id, packet).await?;
                    sequence_id = sequence_id.wrapping_add(1);
                }
            }
        }
        if send_prepare_metadata && !columns.is_empty() && !deprecate_eof {
            batch
                .write_packet(sequence_id, eof.as_ref().expect("EOF packet is available"))
                .await?;
        }
        batch.finish().await?;
        Ok(())
    }

    async fn handle_stmt_execute(&mut self, payload: &[u8]) -> io::Result<()> {
        if let Some(err) = self.context.cancelled_error() {
            return self.write_stmt_execute_response(Err(err)).await;
        }
        if payload.len() < 4 {
            let err = command_err_packet_for_spec(
                crate::mysql::errors::ER_UNKNOWN_COM_ERROR,
                "bad COM_STMT_EXECUTE packet",
            );
            self.codec
                .write_packet(&mut self.stream, self.command_response_sequence, &err)
                .await?;
            return Ok(());
        }
        let statement_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        // Hot path: avoid `prepared.cloned()` which deep-clones the parsed
        // AST. Extract the small fields we need with a borrow, then clone
        // only the AST template once when we actually go to bind it.
        // Bind-parameter decode happens inside the same borrow scope so the
        // per-execute `param_types` / `long_data` Vec clones (paid on every
        // COM_STMT_EXECUTE just to satisfy the borrow checker) are gone —
        // `parse_stmt_execute_params` is synchronous and only reads them.
        let (
            sql,
            original_sql,
            param_count,
            has_parsed_template,
            normalized_sql,
            database,
            parser_sql_mode_bits,
            perf_texts,
            com_kind,
            original_sql_sensitive,
            parse_result,
        ) = {
            let Some(prepared) = self.prepared.get(&statement_id) else {
                let err = command_err_packet_for_spec(
                    crate::mysql::errors::ER_UNKNOWN_STMT_HANDLER,
                    "unknown prepared statement handler",
                );
                self.codec
                    .write_packet(&mut self.stream, self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            };
            if let Some(err) = &prepared.long_data_error {
                self.prepared_execute_failed = true;
                self.codec
                    .write_packet(
                        &mut self.stream,
                        self.command_response_sequence,
                        &crate::mysql::protocol::err_packet(err),
                    )
                    .await?;
                return Ok(());
            }
            let parse_result = parse_stmt_execute_params(
                payload,
                prepared.param_count,
                prepared.param_types.as_deref(),
                Some(prepared.long_data.as_slice()),
                self.client_capabilities() & CLIENT_QUERY_ATTRIBUTES != 0,
            );
            (
                prepared.sql.clone(),
                prepared.original_sql.clone(),
                prepared.param_count,
                prepared.parsed.is_some(),
                Arc::clone(&prepared.normalized_sql),
                Arc::clone(&prepared.database),
                prepared.parser_sql_mode_bits,
                prepared.perf_texts.clone(),
                prepared.com_kind,
                prepared
                    .original_sql_sensitivity
                    .is_sensitive(&prepared.original_sql),
                parse_result,
            )
        };
        if let Some(kind) = com_kind {
            crate::engine::status_counters::record_session_com(&self.context, kind);
        }
        if let Some(handle) = &self.handle {
            handle.set_process_info_prechecked(Arc::clone(&original_sql), original_sql_sensitive);
            handle.set_base_state(ProcessState::Executing);
        }
        self.engine
            .log_general_event(&self.context, "Execute", &original_sql);
        let mut parsed = match parse_result {
            Ok(parsed) => parsed,
            Err(message) => {
                self.prepared_execute_failed = true;
                let err =
                    command_err_packet_for_spec(crate::error::catalog::ER_WRONG_ARGUMENTS, message);
                self.codec
                    .write_packet(&mut self.stream, self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            }
        };
        if !parsed.query_attributes.is_empty() {
            let charset = self.context.character_set_connection.clone();
            for (name, value) in parsed.query_attributes.drain(..) {
                if let Ok(value) = stmt_param_to_value(&value, &charset) {
                    self.context.statement_query_attributes.insert(name, value);
                }
            }
        }
        let cursor_requested = match stmt_execute_cursor_mode(parsed.flags) {
            Ok(cursor) => cursor,
            Err(err) => {
                self.prepared_execute_failed = true;
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            }
        };
        if let Some(param_types) = parsed.param_types.take() {
            if let Some(entry) = self.prepared.get_mut(&statement_id) {
                let new_bytes = param_types
                    .capacity()
                    .saturating_mul(std::mem::size_of::<StmtParamType>());
                if let Err(err) = entry.memory_charge.try_grow(new_bytes) {
                    self.prepared_execute_failed = true;
                    self.write_error_packet(self.command_response_sequence, &err)
                        .await?;
                    return Ok(());
                }
                let old_bytes = entry
                    .param_types
                    .as_ref()
                    .map(|types| {
                        types
                            .capacity()
                            .saturating_mul(std::mem::size_of::<StmtParamType>())
                    })
                    .unwrap_or(0);
                entry.param_types = Some(param_types);
                entry.memory_charge.shrink(old_bytes);
            }
        }
        if let Some(entry) = self.prepared.get_mut(&statement_id) {
            entry.cursor = None;
        }
        if has_parsed_template {
            if !cursor_requested && param_count == 0 {
                let parsed_arc = self
                    .prepared
                    .get(&statement_id)
                    .and_then(|entry| entry.parsed.clone());
                if let Some(arc) = parsed_arc.as_ref().filter(|arc| {
                    matches!(
                        arc.as_ref(),
                        Statement::Begin { .. }
                            | Statement::Commit { .. }
                            | Statement::Rollback { .. }
                    )
                }) {
                    if let Some(result) = crate::engine::perf_events::with_prepared_statement_texts(
                        &perf_texts,
                        || {
                            self.engine.execute_prepared_statement(
                                &mut self.context,
                                &sql,
                                arc.as_ref().clone(),
                            )
                        },
                    ) {
                        if self.kill_flag.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        self.write_stmt_execute_response_for(Some(statement_id), result)
                            .await?;
                        if let Some(entry) = self.prepared.get_mut(&statement_id) {
                            clear_prepared_long_data(entry);
                        }
                        return Ok(());
                    }
                }
            }

            // Decode bind parameters once for either the compiled SELECT
            // runner or a statement-specific fallback executor.
            let mut values = self
                .prepared
                .get_mut(&statement_id)
                .map(|entry| std::mem::take(&mut entry.value_scratch))
                .unwrap_or_default();
            values.clear();
            for param in &parsed.params {
                match stmt_param_to_value(param, &self.context.character_set_connection) {
                    Ok(value) => values.push(value),
                    Err(err) => {
                        self.prepared_execute_failed = true;
                        if let Some(entry) = self.prepared.get_mut(&statement_id) {
                            values.clear();
                            entry.value_scratch = values;
                        }
                        self.write_error_packet(self.command_response_sequence, &err)
                            .await?;
                        return Ok(());
                    }
                }
            }

            // Compile-once / late-bind SELECT runner: an eligible read-only
            // SELECT reuses a memoized physical plan held on the prepared
            // statement, so this EXECUTE skips the AST clone + re-planning
            // and binds the parameters late. The memo is taken out of the
            // entry (so the engine borrows the session without also holding
            // the prepared-map borrow) and written back afterwards. Cursor
            // and non-cursor consumers share this plan. A `None` result means the
            // statement/session needs a different execution vehicle.
            {
                let (parsed_arc, computed_ordering, mut select_memo_state) = {
                    let entry = self
                        .prepared
                        .get_mut(&statement_id)
                        .expect("prepared entry present moments ago");
                    (
                        entry.parsed.clone(),
                        entry.computed_ordering,
                        std::mem::take(&mut entry.select_memo_state),
                    )
                };
                let mut compiled_result = None;
                if let Some(arc) = parsed_arc.as_ref() {
                    if computed_ordering {
                        let engine = Arc::clone(&self.engine);
                        let mut context = self.park_context();
                        let parsed = Arc::clone(arc);
                        let sql = Arc::clone(&sql);
                        let texts = perf_texts.clone();
                        let mut state = std::mem::take(&mut select_memo_state);
                        let params = std::mem::take(&mut values);
                        let needs_async_progress = statement_contains_fusion(arc.as_ref())
                            || statement_contains_blocking_function(arc.as_ref());
                        let work = move || {
                            let result = crate::engine::perf_events::with_prepared_statement_texts(
                                &texts,
                                || {
                                    crate::engine::api::with_shared_result_metadata(|| {
                                        engine.execute_prepared_select(
                                            &mut context,
                                            &mut state,
                                            &parsed,
                                            &sql,
                                            &params,
                                        )
                                    })
                                },
                            );
                            (context, state, params, result)
                        };
                        let job = if needs_async_progress {
                            crate::util::blocking_pool::spawn_affine(
                                self.blocking_affinity.as_ref(),
                                work,
                            )
                        } else {
                            crate::util::blocking_pool::spawn_session(
                                self.blocking_affinity.as_ref(),
                                work,
                            )
                        };
                        match job.await {
                            Ok((context, state, params, result)) => {
                                self.restore_context(context);
                                select_memo_state = state;
                                values = params;
                                compiled_result = result;
                            }
                            Err(err) => compiled_result = Some(Err(self.recover_join_error(err))),
                        }
                    } else {
                        compiled_result = crate::engine::perf_events::with_prepared_statement_texts(
                            &perf_texts,
                            || {
                                crate::engine::api::with_shared_result_metadata(|| {
                                    self.engine.execute_prepared_select(
                                        &mut self.context,
                                        &mut select_memo_state,
                                        arc,
                                        &sql,
                                        &values,
                                    )
                                })
                            },
                        );
                    }
                }
                if let Some(entry) = self.prepared.get_mut(&statement_id) {
                    entry.select_memo_state = select_memo_state;
                }
                if let Some(result) = compiled_result {
                    let result = match result {
                        Err(err)
                            if crate::storage::is_lock_wait_pending_error(&err)
                                || err.is(crate::error::catalog::ER_NEED_REPREPARE) =>
                        {
                            match parsed_arc {
                                Some(statement) => {
                                    self.execute_engine_blocking_parsed(
                                        sql.to_string(),
                                        statement,
                                        Some((std::mem::take(&mut values), Arc::clone(&sql))),
                                    )
                                    .await
                                }
                                None => Err(err),
                            }
                        }
                        result => result,
                    };
                    if self.kill_flag.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    if cursor_requested {
                        self.write_stmt_cursor_execute_response(statement_id, result)
                            .await?;
                    } else {
                        self.write_stmt_execute_response_for(Some(statement_id), result)
                            .await?;
                    }
                    if let Some(entry) = self.prepared.get_mut(&statement_id) {
                        clear_prepared_long_data(entry);
                        values.clear();
                        entry.value_scratch = values;
                    }
                    return Ok(());
                }
            }

            // Compile-once / late-bind fast path for prepared
            // INSERT / UPDATE / DELETE. Same lifecycle as the SELECT
            // memo above (take state out of the prepared entry so the
            // engine borrows the session without also holding the
            // prepared-map borrow; write back afterwards). Autocommit
            // prepared DML can execute inline to avoid one cross-thread
            // dispatch per COM_STMT_EXECUTE. Eligible explicit-transaction
            // commands also attempt inline execution with nonblocking row-lock
            // waits and statement-savepoint rollback. Conflicts fall back to
            // the blocking pool; they must never park a Tokio worker while
            // waiting for another connection to commit and release its lock.
            if !cursor_requested {
                let (parsed_arc, dml_state) = {
                    let entry = self
                        .prepared
                        .get_mut(&statement_id)
                        .expect("prepared entry present moments ago");
                    (entry.parsed.clone(), std::mem::take(&mut entry.dml_state))
                };
                let (dml_state, dml_result) = if let Some(arc) = parsed_arc {
                    if prepared_dml_inline_allowed(&self.context)
                        && prepared_dml_statement_allows_inline_execution(arc.as_ref())
                    {
                        self.try_execute_prepared_dml_inline(
                            &sql,
                            &arc,
                            dml_state,
                            &values,
                            &perf_texts,
                        )
                    } else if prepared_dml_transactional_inline_retry_allowed(arc.as_ref()) {
                        // Explicit / autocommit-off transaction: attempt
                        // inline with nonblocking lock waits. On conflict,
                        // the savepoint path rolls back cleanly before the
                        // statement is re-run on the blocking pool. Do this on
                        // every execution: the compiled plan is shared by all
                        // connections, so remembering one conflict on the plan
                        // permanently forced every session onto the blocking
                        // pool and turned a rare row collision into steady
                        // cross-thread dispatch overhead.
                        //
                        // Admission must happen before the inline attempt:
                        // that attempt can acquire the transaction's first row
                        // lock. Waiting for a permit only after a later
                        // conflict would let an unadmitted transaction pin a
                        // lock needed by an admitted one, defeating the
                        // deadlock-free admission invariant.
                        self.acquire_txn_admission().await;
                        let (state, result) = crate::storage::with_nonblocking_lock_waits(|| {
                            self.try_execute_prepared_dml_inline(
                                &sql,
                                &arc,
                                dml_state,
                                &values,
                                &perf_texts,
                            )
                        });
                        let would_block = matches!(
                            &result,
                            Some(Err(err)) if crate::storage::is_lock_would_block_error(err)
                        );
                        if would_block {
                            crate::storage::record_inline_lock_would_block_retry();
                            let (state, result, completed_inline) = self
                                .retry_prepared_dml_after_async_row_wake(
                                    &sql,
                                    &arc,
                                    state,
                                    &values,
                                    &perf_texts,
                                )
                                .await;
                            if completed_inline {
                                (state, result)
                            } else {
                                let (state, result, returned_values) = self
                                    .try_execute_prepared_dml_blocking(
                                        sql.clone(),
                                        arc,
                                        state,
                                        values,
                                        perf_texts.clone(),
                                    )
                                    .await;
                                values = returned_values;
                                (state, result)
                            }
                        } else {
                            (state, result)
                        }
                    } else {
                        let (state, result, returned_values) = self
                            .try_execute_prepared_dml_blocking(
                                sql.clone(),
                                arc,
                                dml_state,
                                values,
                                perf_texts.clone(),
                            )
                            .await;
                        values = returned_values;
                        (state, result)
                    }
                } else {
                    (dml_state, None)
                };
                if let Some(entry) = self.prepared.get_mut(&statement_id) {
                    entry.dml_state = dml_state;
                }
                if let Some(result) = dml_result {
                    if self.kill_flag.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    self.write_stmt_execute_response_for(Some(statement_id), result)
                        .await?;
                    if let Some(entry) = self.prepared.get_mut(&statement_id) {
                        clear_prepared_long_data(entry);
                        values.clear();
                        entry.value_scratch = values;
                    }
                    return Ok(());
                }
            }

            // Legacy late-binding path: the cached AST is shared via
            // `Arc<Statement>` and is *not* mutated to substitute
            // placeholders. We clone the inner `Statement` (cheap relative
            // to a full re-parse + qualify) only because the engine trait
            // currently takes an owned `Statement`; the `Expr::Placeholder`
            // nodes survive into execution and are resolved against the
            // thread-local param context installed below.
            let statement = self
                .prepared
                .get(&statement_id)
                .and_then(|p| p.parsed.as_ref().map(|arc| (**arc).clone()))
                .expect("prepared.parsed was Some moments ago");
            let cache_scope = statement_ddl_cache_scope(&statement);
            if let Some(result) = self
                .execute_prepared_statement_blocking(
                    sql.clone(),
                    statement,
                    values,
                    Some((Arc::clone(&normalized_sql), Arc::clone(&database))),
                    perf_texts.clone(),
                )
                .await
            {
                return self
                    .finish_prepared_statement_execute(
                        statement_id,
                        cursor_requested,
                        cache_scope,
                        result,
                    )
                    .await;
            }
        }
        let lex_mode = crate::sql_lex::SqlLexMode::from_parser_bits(parser_sql_mode_bits);
        let sql = match bind_sql_params(&sql, &parsed.params, lex_mode) {
            Ok(sql) => sql,
            Err(message) => {
                self.prepared_execute_failed = true;
                let err =
                    command_err_packet_for_spec(crate::mysql::errors::ER_WRONG_ARGUMENTS, message);
                self.codec
                    .write_packet(&mut self.stream, self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            }
        };
        let cache_scope = ddl_cache_scope(&sql, lex_mode);
        let result = self
            .execute_engine_blocking_with_parser_mode(sql, parser_sql_mode_bits)
            .await;
        self.finish_prepared_statement_execute(statement_id, cursor_requested, cache_scope, result)
            .await
    }

    /// Complete a fallback execution only after the response is written. A
    /// killed connection or a write error must not run the cleanup below.
    async fn finish_prepared_statement_execute(
        &mut self,
        statement_id: u32,
        cursor_requested: bool,
        cache_scope: DdlCacheScope,
        result: Result<QueryResponse, crate::types::QueryError>,
    ) -> io::Result<()> {
        if self.kill_flag.load(Ordering::Acquire) {
            return Ok(());
        }
        if cursor_requested {
            self.write_stmt_cursor_execute_response(statement_id, result)
                .await?;
        } else {
            self.write_stmt_execute_response_for(Some(statement_id), result)
                .await?;
        }
        if let Some(entry) = self.prepared.get_mut(&statement_id) {
            clear_prepared_long_data(entry);
        }
        match cache_scope {
            DdlCacheScope::None => {}
            DdlCacheScope::SessionOnly | DdlCacheScope::TableScoped => {}
            DdlCacheScope::Global => {
                self.engine.invalidate_statement_cache();
            }
        }
        Ok(())
    }

    async fn write_stmt_execute_response(
        &mut self,
        result: Result<QueryResponse, crate::types::QueryError>,
    ) -> io::Result<()> {
        self.write_stmt_execute_response_for(None, result).await
    }

    async fn write_stmt_execute_response_for(
        &mut self,
        statement_id: Option<u32>,
        result: Result<QueryResponse, crate::types::QueryError>,
    ) -> io::Result<()> {
        if let Some(handle) = &self.handle {
            handle.set_base_state(ProcessState::QueryEnd);
        }
        let _sending = self
            .handle
            .as_ref()
            .map(|handle| handle.enter_stage(ProcessState::SendingToClient));
        let fallback_collation = self.result_fallback_collation_id();
        let metadata = statement_id
            .and_then(|id| self.prepared.get_mut(&id))
            .and_then(|prepared| prepared.result_metadata.as_mut())
            .map(|metadata| {
                if metadata.fallback_collation != fallback_collation {
                    *metadata =
                        PreparedResultMetadata::from_columns(&metadata.columns, fallback_collation)
                            .expect("prepared result metadata has columns");
                }
                // Keep the validated immutable column owner, making later
                // shared results an identity check rather than repeated deep
                // comparison of every column/source string. Equality is still
                // checked after reprepare or any different result shape.
                if let Ok(QueryResponse::SharedResultSet(result)) = &result {
                    if !Arc::ptr_eq(&metadata.columns, &result.columns)
                        && metadata.matches_columns(&result.columns)
                    {
                        metadata.columns = Arc::clone(&result.columns);
                    }
                }
                metadata.clone()
            });
        match result {
            Ok(QueryResponse::Ok) => {
                self.write_ok_packet(self.command_response_sequence).await?;
            }
            Ok(QueryResponse::OkAffected { affected_rows }) => {
                self.write_ok_packet_with_affected_rows(
                    self.command_response_sequence,
                    affected_rows,
                )
                .await?;
            }
            Ok(QueryResponse::ResultSet(result)) => {
                if let Some(metadata) = metadata
                    .as_ref()
                    .filter(|metadata| metadata.matches_columns(&result.columns))
                {
                    self.write_binary_result_set_with_metadata(&result, metadata)
                        .await?;
                } else {
                    self.write_binary_result_set(&result).await?;
                }
            }
            Ok(QueryResponse::SharedResultSet(result)) => {
                if let Some(metadata) = metadata
                    .as_ref()
                    .filter(|metadata| metadata.matches_columns(&result.columns))
                {
                    self.write_binary_prepared_rows_with_metadata(&result.rows, metadata)
                        .await?;
                } else {
                    let owned = ResultSet {
                        columns: result.columns.to_vec(),
                        rows: result.rows,
                    };
                    self.write_binary_result_set(&owned).await?;
                }
            }
            Ok(QueryResponse::StreamingResultSet(result)) => {
                self.write_binary_streaming_result_set(result).await?;
            }
            Ok(QueryResponse::MultiResult(results)) => {
                if self.capabilities.has(CLIENT_PS_MULTI_RESULTS) {
                    self.write_binary_multi_result(results).await?;
                } else {
                    let err = error(
                        crate::error::catalog::ER_SP_BADSELECT,
                        "Prepared CALL returned multiple result sets, but CLIENT_PS_MULTI_RESULTS was not negotiated",
                    );
                    self.write_error_packet(self.command_response_sequence, &err)
                        .await?;
                }
            }
            Err(err) => {
                self.prepared_execute_failed = true;
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
            }
        }
        Ok(())
    }

    async fn write_text_result_set(&mut self, result: &ResultSet) -> io::Result<()> {
        self.write_text_result_set_parts_with_status_seq(
            &result.columns,
            &result.rows,
            self.server_status(),
            self.command_response_sequence,
        )
        .await
        .map(|_| ())
    }

    async fn write_text_shared_result_set(
        &mut self,
        result: &crate::engine::api::SharedResultSet,
    ) -> io::Result<()> {
        self.write_text_result_set_parts_with_status_seq(
            &result.columns,
            &result.rows,
            self.server_status(),
            self.command_response_sequence,
        )
        .await
        .map(|_| ())
    }

    async fn write_text_result_set_parts_with_status_seq(
        &mut self,
        columns: &[Column],
        rows: &[Vec<crate::types::Value>],
        server_status: u16,
        first_sequence_id: u8,
    ) -> io::Result<u8> {
        let encoding = StreamRowEncoding::Text(crate::charset::resolve_result_charset(
            self.context.character_set_results.as_deref(),
        ));
        self.write_materialized_result_set(
            columns,
            rows,
            server_status,
            first_sequence_id,
            encoding,
        )
        .await
    }

    async fn write_materialized_result_set(
        &mut self,
        columns: &[Column],
        rows: &[Vec<crate::types::Value>],
        server_status: u16,
        first_sequence_id: u8,
        encoding: StreamRowEncoding,
    ) -> io::Result<u8> {
        let encoding = encoding.with_vector_text(self.context.vector_result_format_text);
        let columns = crate::mysql::protocol::vector_text_result_columns(
            columns,
            matches!(encoding, StreamRowEncoding::VectorText(_)),
        );
        let columns = columns.as_ref();
        let client_capabilities = self.client_capabilities();
        let fallback_collation = self.result_fallback_collation_id();
        let mut batch = self.codec.batch_writer(&mut self.stream);
        let mut sequence_id = write_result_set_header_batched(
            &mut batch,
            columns,
            client_capabilities,
            self.context.resultset_metadata_full,
            fallback_collation,
            server_status,
            first_sequence_id,
        )
        .await?;

        let mut row_payload = Vec::new();
        let zerofill_spec = match encoding {
            StreamRowEncoding::Text(_) | StreamRowEncoding::VectorText(_) => {
                crate::mysql::protocol::zerofill_pad_spec(columns)
            }
            StreamRowEncoding::Binary(_) => None,
        };
        for row in rows {
            if let Some(err) = self.context.cancelled_error() {
                return finish_failed_stream(batch, sequence_id, &err, &mut self.text_batch).await;
            }
            encode_stream_row(
                row,
                columns,
                zerofill_spec.as_deref(),
                encoding,
                &mut row_payload,
            );
            sequence_id = batch.write_packet_seq(sequence_id, &row_payload).await?;
        }
        write_result_set_terminator_batched(
            &mut batch,
            sequence_id,
            client_capabilities,
            server_status,
            self.context.warning_count(),
        )
        .await?;
        batch.finish().await?;
        Ok(sequence_id.wrapping_add(1))
    }

    async fn write_text_streaming_result_set(
        &mut self,
        result: StreamingResultSet,
    ) -> io::Result<()> {
        self.write_text_streaming_result_set_with_status_seq(
            result,
            self.server_status(),
            self.command_response_sequence,
        )
        .await
        .map(|_| ())
    }

    async fn write_text_streaming_result_set_with_status_seq(
        &mut self,
        result: StreamingResultSet,
        server_status: u16,
        first_sequence_id: u8,
    ) -> io::Result<u8> {
        let charset =
            crate::charset::resolve_result_charset(self.context.character_set_results.as_deref());
        self.write_streaming_result_set_with_status_seq(
            result,
            server_status,
            first_sequence_id,
            StreamRowEncoding::Text(charset),
        )
        .await
    }

    async fn write_binary_result_set(&mut self, result: &ResultSet) -> io::Result<()> {
        self.write_binary_result_set_with_status_seq(
            result,
            self.server_status(),
            self.command_response_sequence,
        )
        .await
        .map(|_| ())
    }

    async fn write_binary_result_set_with_status_seq(
        &mut self,
        result: &ResultSet,
        server_status: u16,
        first_sequence_id: u8,
    ) -> io::Result<u8> {
        self.write_materialized_result_set(
            &result.columns,
            &result.rows,
            server_status,
            first_sequence_id,
            StreamRowEncoding::Binary(crate::charset::resolve_result_charset(
                self.context.character_set_results.as_deref(),
            )),
        )
        .await
    }

    async fn write_binary_result_set_with_metadata(
        &mut self,
        result: &ResultSet,
        metadata: &PreparedResultMetadata,
    ) -> io::Result<()> {
        self.write_binary_prepared_rows_with_metadata(&result.rows, metadata)
            .await
    }

    async fn write_binary_prepared_rows_with_metadata(
        &mut self,
        rows: &[Vec<crate::types::Value>],
        metadata: &PreparedResultMetadata,
    ) -> io::Result<()> {
        let needed = rows
            .iter()
            .map(|row| crate::mysql::protocol::estimate_binary_row_packet_bytes(row))
            .max()
            .unwrap_or(0);
        ensure_governed_scratch_capacity(
            &mut self.binary_row_scratch,
            &mut self.binary_row_scratch_charge,
            needed,
        )
        .map_err(|err| io::Error::new(io::ErrorKind::OutOfMemory, err.message()))?;
        let result_charset =
            crate::charset::resolve_result_charset(self.context.character_set_results.as_deref());
        let row_payload = std::mem::take(&mut self.binary_row_scratch);
        let server_status = self.server_status();
        let client_capabilities = self.client_capabilities();
        let mut row_payload = ReusableByteScratch::new(&mut self.binary_row_scratch, row_payload);
        let mut batch = self.codec.batch_writer(&mut self.stream);
        let mut sequence_id = if client_capabilities & CLIENT_OPTIONAL_RESULTSET_METADATA != 0
            && !self.context.resultset_metadata_full
        {
            write_cached_result_set_header_metadata_none_batched(
                &mut batch,
                metadata.columns.len(),
                self.command_response_sequence,
            )
            .await?;
            self.command_response_sequence.wrapping_add(1)
        } else {
            write_cached_result_set_header_batched(
                &mut batch,
                metadata,
                client_capabilities,
                server_status,
                self.command_response_sequence,
            )
            .await?;
            result_row_start_sequence_id(
                &metadata.columns,
                client_capabilities,
                true,
                self.command_response_sequence,
            )
        };
        for row in rows {
            if let Some(err) = self.context.cancelled_error() {
                return finish_error_result_set(batch, sequence_id, &err).await;
            }
            crate::mysql::protocol::binary_row_packet_into_with_charset(
                &metadata.columns,
                row,
                row_payload.as_mut(),
                result_charset,
            );
            sequence_id = batch
                .write_packet_seq(sequence_id, row_payload.as_ref())
                .await?;
        }
        write_result_set_terminator_batched(
            &mut batch,
            sequence_id,
            client_capabilities,
            server_status,
            self.context.warning_count(),
        )
        .await?;
        batch.finish().await
    }

    async fn write_binary_streaming_result_set(
        &mut self,
        result: StreamingResultSet,
    ) -> io::Result<()> {
        self.write_binary_streaming_result_set_with_status_seq(
            result,
            self.server_status(),
            self.command_response_sequence,
        )
        .await
        .map(|_| ())
    }

    async fn write_binary_streaming_result_set_with_status_seq(
        &mut self,
        result: StreamingResultSet,
        server_status: u16,
        first_sequence_id: u8,
    ) -> io::Result<u8> {
        self.write_streaming_result_set_with_status_seq(
            result,
            server_status,
            first_sequence_id,
            StreamRowEncoding::Binary(crate::charset::resolve_result_charset(
                self.context.character_set_results.as_deref(),
            )),
        )
        .await
    }

    /// Shared streaming result-set writer that pulls and encodes rows off
    /// the async worker. Each round moves the cursor into the blocking
    /// pool, pulls + encodes up to `stream_offload_batch_rows()` rows
    /// (where `next_row` may block on a std mpsc recv or a synchronous
    /// heap read), and hands ownership back with the encoded packets; the
    /// async task then batch-writes them, honouring the same KILL /
    /// cancellation interlocks as the legacy inline loop. This is the
    /// symmetric analogue of `handle_stmt_fetch`, extended to the whole
    /// (unbounded) stream.
    async fn write_streaming_result_set_with_status_seq(
        &mut self,
        result: StreamingResultSet,
        server_status: u16,
        first_sequence_id: u8,
        encoding: StreamRowEncoding,
    ) -> io::Result<u8> {
        if result.cursor.external_io() || stream_blocking_offload_enabled() {
            return self
                .write_streaming_result_set_offloaded(
                    result,
                    server_status,
                    first_sequence_id,
                    encoding,
                )
                .await;
        }
        self.write_streaming_result_set_inline(result, server_status, first_sequence_id, encoding)
            .await
    }

    async fn write_streaming_result_set_inline(
        &mut self,
        mut result: StreamingResultSet,
        server_status: u16,
        first_sequence_id: u8,
        encoding: StreamRowEncoding,
    ) -> io::Result<u8> {
        let encoding = encoding.with_vector_text(self.context.vector_result_format_text);
        if let std::borrow::Cow::Owned(columns) = crate::mysql::protocol::vector_text_result_columns(
            &result.columns,
            matches!(encoding, StreamRowEncoding::VectorText(_)),
        ) {
            result.columns = columns;
        }
        let client_capabilities = self.client_capabilities();
        // Clone the session handle up front so the per-row `select!`
        // below can race the write against `KILL [CONNECTION|QUERY]`
        // without re-borrowing `self`. The handle's `Notify` is woken
        // by `kill_connection` / `cancel_query`, so a KILL that lands
        // while a slow client is gluing up TCP backpressure aborts the
        // stream immediately instead of waiting for the row count to
        // drain naturally.
        let kill_notify = self.handle.as_ref().map(Arc::clone);
        let fallback_collation = self.result_fallback_collation_id();
        let mut batch = self.codec.batch_writer(&mut self.stream);
        let mut sequence_id = write_result_set_header_batched(
            &mut batch,
            &result.columns,
            client_capabilities,
            self.context.resultset_metadata_full,
            fallback_collation,
            server_status,
            first_sequence_id,
        )
        .await?;
        batch.flush().await?;

        let mut row_payload = Vec::new();
        let mut sent_rows = 0_u64;
        let zerofill_spec = match encoding {
            StreamRowEncoding::Text(_) | StreamRowEncoding::VectorText(_) => {
                crate::mysql::protocol::zerofill_pad_spec(&result.columns)
            }
            StreamRowEncoding::Binary(_) => None,
        };
        loop {
            let row = match result.cursor.next_row() {
                Ok(Some(row)) => row,
                Ok(None) => break,
                Err(err) => {
                    return finish_failed_stream(batch, sequence_id, &err, &mut self.text_batch)
                        .await;
                }
            };
            if let Some(err) = self.context.cancelled_error() {
                return finish_failed_stream(batch, sequence_id, &err, &mut self.text_batch).await;
            }
            encode_stream_row(
                &row,
                &result.columns,
                zerofill_spec.as_deref(),
                encoding,
                &mut row_payload,
            );
            sequence_id = match &kill_notify {
                Some(notify) => {
                    write_cancellable_stream_packet(&mut batch, sequence_id, &row_payload, notify)
                        .await?
                }
                None => batch.write_packet_seq(sequence_id, &row_payload).await?,
            };
            sent_rows = sent_rows.saturating_add(1);
        }
        StreamCompletion {
            sequence_id,
            client_capabilities,
            server_status,
            found_rows: result.found_rows,
            sent_rows,
        }
        .finish(
            batch,
            &mut self.context,
            &mut self.text_batch,
            result.cursor.as_mut(),
        )
        .await
    }

    async fn write_streaming_result_set_offloaded(
        &mut self,
        mut result: StreamingResultSet,
        server_status: u16,
        first_sequence_id: u8,
        encoding: StreamRowEncoding,
    ) -> io::Result<u8> {
        let encoding = encoding.with_vector_text(self.context.vector_result_format_text);
        if let std::borrow::Cow::Owned(columns) = crate::mysql::protocol::vector_text_result_columns(
            &result.columns,
            matches!(encoding, StreamRowEncoding::VectorText(_)),
        ) {
            result.columns = columns;
        }
        let client_capabilities = self.client_capabilities();
        let kill_notify = self.handle.as_ref().map(Arc::clone);
        let fallback_collation = self.result_fallback_collation_id();
        let mut batch = self.codec.batch_writer(&mut self.stream);
        let mut sequence_id = write_result_set_header_batched(
            &mut batch,
            &result.columns,
            client_capabilities,
            self.context.resultset_metadata_full,
            fallback_collation,
            server_status,
            first_sequence_id,
        )
        .await?;
        batch.flush().await?;

        let mut sent_rows = 0_u64;

        // Read-only per-result-set state shared with each blocking pull.
        // `columns` is moved out of `result` (not cloned) and wrapped in an
        // `Arc`; `zerofill` is derived once. Both are `Arc`-shared so each
        // round captures a cheap clone. `encoding` is `Copy`.
        let columns = Arc::new(std::mem::take(&mut result.columns));
        let zerofill = Arc::new(crate::mysql::protocol::zerofill_pad_spec(
            columns.as_slice(),
        ));
        let found_rows = result.found_rows;
        let mut cursor = result.cursor;
        let external = cursor.external_io();
        let http = cursor.external_http_io();
        let batch_rows = if external {
            cursor
                .external_batch_rows()
                .max(1)
                .min(stream_offload_batch_rows())
        } else {
            stream_offload_batch_rows()
        };
        let batch_bytes = stream_offload_batch_bytes();

        loop {
            let cols = Arc::clone(&columns);
            let zf = Arc::clone(&zerofill);
            let _admission = if external {
                match if http {
                    crate::engine::http::admit_coordinator()
                } else {
                    crate::engine::ai::runtime::admit_coordinator()
                } {
                    Ok(p) => Some(p),
                    Err(e) => {
                        return finish_failed_stream(batch, sequence_id, &e, &mut self.text_batch)
                            .await
                    }
                }
            } else {
                None
            };
            let work = move || {
                let mut encoded: Vec<Vec<u8>> = Vec::with_capacity(batch_rows);
                let mut encoded_bytes = 0usize;
                let mut memory_charge =
                    crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                        crate::engine::executor::mem_budget::ControlledMemoryCategory::ProtocolTransient,
                        0,
                    )
                    .expect("zero-byte protocol reservation cannot fail");
                let mut err: Option<QueryError> = None;
                let mut exhausted = false;
                let mut scratch = Vec::new();
                while encoded.len() < batch_rows {
                    match cursor.next_row() {
                        Ok(Some(row)) => {
                            let estimate = estimate_stream_row_bytes(&row, &cols, encoding);
                            if let Err(e) = memory_charge.try_grow(estimate) {
                                err = Some(e);
                                break;
                            }
                            if scratch.try_reserve_exact(estimate).is_err() {
                                memory_charge.shrink(estimate);
                                err = Some(error(
                                    crate::error::catalog::ER_OUTOFMEMORY,
                                    "out of memory while encoding MySQL result row",
                                ));
                                break;
                            }
                            encode_stream_row(
                                &row,
                                cols.as_slice(),
                                zf.as_deref(),
                                encoding,
                                &mut scratch,
                            );
                            if scratch.capacity() > estimate {
                                if let Err(e) =
                                    memory_charge.try_grow(scratch.capacity() - estimate)
                                {
                                    err = Some(e);
                                    break;
                                }
                            }
                            encoded_bytes = encoded_bytes.saturating_add(scratch.capacity());
                            encoded.push(std::mem::take(&mut scratch));
                            if encoded_bytes >= batch_bytes {
                                break;
                            }
                        }
                        Ok(None) => {
                            exhausted = true;
                            break;
                        }
                        Err(e) => {
                            err = Some(e);
                            break;
                        }
                    }
                }
                (cursor, encoded, err, exhausted, memory_charge)
            };
            let pulled = if http {
                crate::engine::http::spawn_coordinator(work)
            } else if external {
                crate::engine::ai::runtime::spawn_coordinator(work)
            } else {
                crate::util::blocking_pool::spawn_session(self.blocking_affinity.as_ref(), work)
            }
            .await;
            let (ret_cursor, encoded, pull_err, exhausted, _encoded_charge) = match pulled {
                Ok(tuple) => tuple,
                Err(join_err) => {
                    // Blocking task panicked — surface as an I/O error that
                    // tears the connection, matching how the legacy loop's
                    // `next_row` error propagated.
                    let qerr = query_join_error(join_err);
                    return Err(io::Error::new(io::ErrorKind::Other, qerr.message()));
                }
            };
            cursor = ret_cursor;

            for packet in &encoded {
                if let Some(err) = self.context.cancelled_error() {
                    return finish_failed_stream(batch, sequence_id, &err, &mut self.text_batch)
                        .await;
                }
                sequence_id = match &kill_notify {
                    Some(notify) => {
                        write_cancellable_stream_packet(&mut batch, sequence_id, packet, notify)
                            .await?
                    }
                    None => batch.write_packet_seq(sequence_id, packet).await?,
                };
                sent_rows = sent_rows.saturating_add(1);
            }

            if let Some(e) = pull_err {
                return finish_failed_stream(batch, sequence_id, &e, &mut self.text_batch).await;
            }
            if exhausted {
                break;
            }
        }

        StreamCompletion {
            sequence_id,
            client_capabilities,
            server_status,
            found_rows: found_rows,
            sent_rows,
        }
        .finish(
            batch,
            &mut self.context,
            &mut self.text_batch,
            cursor.as_mut(),
        )
        .await
    }

    async fn write_text_multi_result(&mut self, results: Vec<QueryResponse>) -> io::Result<()> {
        self.write_text_multi_result_seq(results, self.command_response_sequence, false)
            .await
            .map(|_| ())
    }

    async fn write_text_multi_result_seq(
        &mut self,
        results: Vec<QueryResponse>,
        sequence_id: u8,
        outer_more: bool,
    ) -> io::Result<u8> {
        self.write_multi_result_seq::<false>(results, sequence_id, outer_more)
            .await
    }

    fn multi_result_encoding<const BINARY: bool>(&self) -> StreamRowEncoding {
        if BINARY {
            StreamRowEncoding::Binary(crate::charset::resolve_result_charset(
                self.context.character_set_results.as_deref(),
            ))
        } else {
            StreamRowEncoding::Text(crate::charset::resolve_result_charset(
                self.context.character_set_results.as_deref(),
            ))
        }
    }

    async fn write_multi_result_seq<const BINARY: bool>(
        &mut self,
        results: Vec<QueryResponse>,
        mut sequence_id: u8,
        outer_more: bool,
    ) -> io::Result<u8> {
        let results = flatten_multi_results(results);
        if (BINARY || self.text_batch.is_none())
            && results.iter().all(|result| {
                matches!(result, QueryResponse::Ok | QueryResponse::OkAffected { .. })
            })
        {
            return self
                .write_ok_multi_result(results, sequence_id, outer_more)
                .await;
        }
        let len = results.len();
        for (idx, result) in results.into_iter().enumerate() {
            let more = idx + 1 < len || outer_more;
            sequence_id = match result {
                QueryResponse::Ok => {
                    self.write_ok_packet_with_more(sequence_id, more).await?;
                    sequence_id.wrapping_add(1)
                }
                QueryResponse::OkAffected { affected_rows } => {
                    self.write_ok_packet_with_affected_rows_more(sequence_id, affected_rows, more)
                        .await?;
                    sequence_id.wrapping_add(1)
                }
                QueryResponse::ResultSet(mut result) => {
                    let mut out_params = false;
                    if BINARY {
                        for column in &mut result.columns {
                            if column.source.org_table
                                == crate::engine::executor::routines::PREPARED_CALL_OUT_MARKER
                            {
                                out_params = true;
                                column.source.org_table.clear();
                            }
                        }
                    }
                    self.write_materialized_result_set(
                        &result.columns,
                        &result.rows,
                        self.server_status_with_more(more)
                            | if out_params { SERVER_PS_OUT_PARAMS } else { 0 },
                        sequence_id,
                        self.multi_result_encoding::<BINARY>(),
                    )
                    .await?
                }
                QueryResponse::SharedResultSet(result) => {
                    self.write_materialized_result_set(
                        &result.columns,
                        &result.rows,
                        self.server_status_with_more(more),
                        sequence_id,
                        self.multi_result_encoding::<BINARY>(),
                    )
                    .await?
                }
                QueryResponse::StreamingResultSet(result) => {
                    self.write_streaming_result_set_with_status_seq(
                        result,
                        self.server_status_with_more(more),
                        sequence_id,
                        self.multi_result_encoding::<BINARY>(),
                    )
                    .await?
                }
                QueryResponse::MultiResult(_) => unreachable!("flattened multi-result"),
            };
            if !BINARY && self.text_batch.is_some_and(|progress| progress.failed) {
                break;
            }
        }
        Ok(sequence_id)
    }

    async fn write_binary_multi_result(&mut self, results: Vec<QueryResponse>) -> io::Result<()> {
        self.write_multi_result_seq::<true>(results, self.command_response_sequence, false)
            .await
            .map(|_| ())
    }

    /// Batch an OK-only multi-result response into one governed socket write.
    /// Each element remains a distinct MySQL packet with the same sequence id
    /// and MORE_RESULTS status; only the kernel write fan-out changes.
    async fn write_ok_multi_result(
        &mut self,
        results: Vec<QueryResponse>,
        first_sequence_id: u8,
        outer_more: bool,
    ) -> io::Result<u8> {
        let base_status = self.server_status();
        let client_capabilities = self.client_capabilities();
        let context = &mut self.context;
        let mut payload = std::mem::take(&mut self.ok_packet_scratch);
        let result_count = results.len();
        let result = {
            let mut batch = self.codec.batch_writer(&mut self.stream);
            let mut error = None;
            for (idx, response) in results.into_iter().enumerate() {
                payload.clear();
                let affected_rows = match response {
                    QueryResponse::Ok => 0,
                    QueryResponse::OkAffected { affected_rows } => affected_rows,
                    _ => unreachable!("OK-only multi-result preflight"),
                };
                let status = if idx + 1 < result_count || outer_more {
                    base_status | SERVER_MORE_RESULTS_EXISTS
                } else {
                    base_status
                };
                build_ok_packet_into(
                    context,
                    client_capabilities,
                    affected_rows,
                    status,
                    &mut payload,
                );
                if let Err(err) = batch
                    .write_packet(first_sequence_id.wrapping_add(idx as u8), &payload)
                    .await
                {
                    error = Some(err);
                    break;
                }
            }
            match error {
                Some(err) => Err(err),
                None => batch.finish().await,
            }
        };
        payload.clear();
        if payload.capacity() > 8 * 1024 {
            payload = Vec::new();
        }
        self.ok_packet_scratch = payload;
        result.map(|_| first_sequence_id.wrapping_add(result_count as u8))
    }

    async fn write_stmt_cursor_execute_response(
        &mut self,
        statement_id: u32,
        result: Result<QueryResponse, QueryError>,
    ) -> io::Result<()> {
        if let Some(handle) = &self.handle {
            handle.set_base_state(ProcessState::QueryEnd);
        }
        let _sending = self
            .handle
            .as_ref()
            .map(|handle| handle.enter_stage(ProcessState::SendingToClient));
        match result {
            Ok(QueryResponse::ResultSet(result)) => {
                let columns = result.columns.clone();
                let cursor: Box<dyn crate::engine::api::ResultCursor> =
                    Box::new(crate::engine::api::VecResultCursor::new(result.rows));
                self.install_cursor_and_send_metadata(statement_id, columns, cursor)
                    .await?;
            }
            Ok(QueryResponse::SharedResultSet(result)) => {
                let columns = result.columns.to_vec();
                let cursor: Box<dyn crate::engine::api::ResultCursor> =
                    Box::new(crate::engine::api::VecResultCursor::new(result.rows));
                self.install_cursor_and_send_metadata(statement_id, columns, cursor)
                    .await?;
            }
            Ok(QueryResponse::StreamingResultSet(result)) => {
                let columns = result.columns.clone();
                self.install_cursor_and_send_metadata(statement_id, columns, result.cursor)
                    .await?;
            }
            Ok(QueryResponse::MultiResult(_)) => {
                let err = crate::mysql::errors::unsupported(
                    "server-side cursors for multi-result prepared statements",
                );
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
            }
            Ok(other) => self.write_stmt_execute_response(Ok(other)).await?,
            Err(err) => {
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
            }
        }
        Ok(())
    }

    /// Stand up a streaming server-side cursor after `COM_STMT_EXECUTE` with
    /// `CURSOR_TYPE_READ_ONLY`. We:
    ///   1. Reserve a slot in the process-wide `cursor_registry` — this
    ///      enforces the `mydb_cursor_per_session_max` /
    ///      `mydb_cursor_global_max` caps and gives admin views a record
    ///      of every cursor before the client sees its first row.
    ///   2. Peek one row from the executor to choose the terminator
    ///      status (`CURSOR_EXISTS` vs `LAST_ROW_SENT`). If the stream
    ///      is empty we never install the cursor; the handle is dropped
    ///      so the registry slot is released too.
    ///   3. Attach the cursor (+ its RAII registry handle) to the
    ///      prepared statement so subsequent `COM_STMT_FETCH` packets
    ///      pull rows directly from the executor.
    async fn install_cursor_and_send_metadata(
        &mut self,
        statement_id: u32,
        columns: Vec<Column>,
        mut cursor: Box<dyn crate::engine::api::ResultCursor>,
    ) -> io::Result<()> {
        let sql_for_registry = self
            .prepared
            .get(&statement_id)
            .map(|entry| Arc::clone(&entry.normalized_sql))
            .unwrap_or_else(|| Arc::from(""));
        let key = crate::engine::cursor_registry::CursorKey {
            connection_id: self.connection_id,
            statement_id,
        };
        let registry_handle = match crate::engine::cursor_registry::global().try_register(
            key,
            sql_for_registry,
            columns.len() as u32,
        ) {
            Ok(handle) => handle,
            Err(err) => {
                if let Some(entry) = self.prepared.get_mut(&statement_id) {
                    entry.cursor = None;
                }
                let q_err = err.into_query_error();
                self.codec
                    .write_packet(
                        &mut self.stream,
                        self.command_response_sequence,
                        &crate::mysql::protocol::err_packet(&q_err),
                    )
                    .await?;
                return Ok(());
            }
        };
        // A streaming cursor may block until its first partition is ready.
        // Peek on the same pool as FETCH, so opening a cursor cannot starve
        // the network runtime that must receive a concurrent KILL.
        let cancel = self
            .context
            .current_cancel_token()
            .cloned()
            .unwrap_or_else(crate::engine::cancel::QueryCancelToken::noop);
        let external = cursor.external_io();
        let http = cursor.external_http_io();
        let _admission = if external {
            match if http {
                crate::engine::http::admit_coordinator()
            } else {
                crate::engine::ai::runtime::admit_coordinator()
            } {
                Ok(p) => Some(p),
                Err(e) => {
                    self.write_error_packet(self.command_response_sequence, &e)
                        .await?;
                    return Ok(());
                }
            }
        } else {
            None
        };
        let work = move || {
            let first = cancel.check().and_then(|()| cursor.next_row());
            (cursor, first)
        };
        let peek = if http {
            crate::engine::http::spawn_coordinator(work)
        } else if external {
            crate::engine::ai::runtime::spawn_coordinator(work)
        } else {
            crate::util::blocking_pool::spawn_session(self.blocking_affinity.as_ref(), work)
        }
        .await;
        let first = match peek {
            Ok((returned_cursor, first)) => {
                cursor = returned_cursor;
                first
            }
            Err(err) => {
                drop(registry_handle);
                let err = self.recover_join_error(err);
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            }
        };
        let lookahead = match first {
            Ok(row) => row,
            Err(err) => {
                // Drops `registry_handle` ⇒ unregister, then surface the
                // executor error to the client.
                drop(registry_handle);
                if let Some(entry) = self.prepared.get_mut(&statement_id) {
                    entry.cursor = None;
                }
                self.write_error_packet(self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            }
        };
        let has_rows = lookahead.is_some();
        let status = self.server_status()
            | if has_rows {
                SERVER_STATUS_CURSOR_EXISTS
            } else {
                SERVER_STATUS_LAST_ROW_SENT
            };
        if has_rows {
            if let Some(entry) = self.prepared.get_mut(&statement_id) {
                entry.cursor = Some(PreparedCursor {
                    columns: columns.clone(),
                    inner: cursor,
                    lookahead,
                    exhausted: false,
                    cancel: self
                        .context
                        .current_cancel_token()
                        .cloned()
                        .unwrap_or_else(crate::engine::cancel::QueryCancelToken::noop),
                    _registry_handle: registry_handle,
                });
            }
            // If the prepared entry vanished between EXECUTE start and
            // now (very unlikely — single-threaded session loop), the
            // handle drops here and auto-unregisters.
        } else {
            // Empty result: registry handle goes out of scope here and
            // the slot is released. `entry.cursor` is already None for
            // a fresh EXECUTE; defensively clear it anyway.
            drop(registry_handle);
            if let Some(entry) = self.prepared.get_mut(&statement_id) {
                entry.cursor = None;
            }
        }
        self.write_result_set_metadata_only(&columns, status, self.command_response_sequence)
            .await?;
        Ok(())
    }

    async fn write_result_set_metadata_only(
        &mut self,
        columns: &[Column],
        server_status: u16,
        first_sequence_id: u8,
    ) -> io::Result<u8> {
        let client_capabilities = self.client_capabilities();
        let fallback_collation = self.result_fallback_collation_id();
        let mut batch = self.codec.batch_writer(&mut self.stream);
        let sequence_id = write_result_set_header_batched(
            &mut batch,
            columns,
            client_capabilities,
            self.context.resultset_metadata_full,
            fallback_collation,
            server_status,
            first_sequence_id,
        )
        .await?;

        if client_capabilities & CLIENT_DEPRECATE_EOF != 0 {
            write_result_set_terminator_batched(
                &mut batch,
                sequence_id,
                client_capabilities,
                server_status,
                self.context.warning_count(),
            )
            .await?;
        }
        batch.finish().await?;
        Ok(sequence_id.wrapping_add(1))
    }

    async fn handle_stmt_send_long_data(&mut self, payload: &[u8]) -> io::Result<()> {
        // This command has no response, including all error paths. Keep a
        // known statement's error sticky until COM_STMT_RESET.
        let Some(id) = payload.get(..4) else {
            return Ok(());
        };
        let statement_id = u32::from_le_bytes(id.try_into().unwrap());
        let Some(entry) = self.prepared.get_mut(&statement_id) else {
            return Ok(());
        };
        if entry.long_data_error.is_some() {
            return Ok(());
        }
        if payload.len() < 6 {
            entry.long_data_error = Some(error(
                crate::mysql::errors::ER_WRONG_ARGUMENTS,
                "bad COM_STMT_SEND_LONG_DATA packet",
            ));
            return Ok(());
        }
        let param_id = u16::from_le_bytes([payload[4], payload[5]]) as usize;
        self.engine.log_general_event(
            &self.context,
            "Long Data",
            &format!(
                "statement_id={statement_id} param_id={param_id} bytes={}",
                payload.len() - 6
            ),
        );
        if param_id >= entry.param_count {
            entry.long_data_error = Some(error(
                crate::mysql::errors::ER_WRONG_ARGUMENTS,
                format!("prepared parameter index {param_id} is out of range"),
            ));
        } else if let Err(err) = append_prepared_long_data(entry, param_id, &payload[6..]) {
            entry.long_data_error = Some(err);
        }
        Ok(())
    }

    async fn handle_stmt_fetch(&mut self, payload: &[u8]) -> io::Result<()> {
        if payload.len() < 8 {
            let err = command_err_packet_for_spec(
                crate::mysql::errors::ER_UNKNOWN_COM_ERROR,
                "bad COM_STMT_FETCH packet",
            );
            self.codec
                .write_packet(&mut self.stream, self.command_response_sequence, &err)
                .await?;
            return Ok(());
        }
        let statement_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let requested = u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]);
        self.engine.log_general_event(
            &self.context,
            "Fetch",
            &format!("statement_id={statement_id} rows={requested}"),
        );
        if !self.prepared.contains_key(&statement_id) {
            let err = command_err_packet_for_spec(
                crate::mysql::errors::ER_UNKNOWN_STMT_HANDLER,
                "unknown prepared statement handler",
            );
            self.codec
                .write_packet(&mut self.stream, self.command_response_sequence, &err)
                .await?;
            return Ok(());
        }
        let count = requested as usize;
        // Pull + binary-encode rows on the blocking pool in byte-bounded
        // chunks. `requested` is client-controlled and may be u32::MAX; it
        // must never be used as a Vec capacity or make one FETCH retain every
        // encoded row before the socket can make progress.
        //
        // Two reasons not to do this on the async task:
        //   1. `ResultCursor::next_row` can synchronously touch the
        //      buffer pool / heap on a cold scan, which would park a
        //      tokio worker thread (the runtime has only `num_cpus`
        //      of them under the multi-thread scheduler).
        //   2. `binary_row_packet` per row does measurable CPU on wide
        //      result sets; moving the loop off-worker keeps the
        //      socket-write task free to multiplex other connections.
        //
        // We `take()` the cursor out of the prepared entry so the
        // closure owns it; on the way back we either re-install it (if
        // there are more rows to FETCH) or drop it (end-of-stream or
        // executor error). The cursor stays open across FETCHes until
        // either it signals end-of-stream or the client closes / resets
        // the statement.
        let cursor_handle = self
            .prepared
            .get_mut(&statement_id)
            .expect("prepared existence checked above")
            .cursor
            .take();
        let Some(mut prepared_cursor) = cursor_handle else {
            let err = command_err_packet_for_spec(
                crate::mysql::errors::ER_UNKNOWN_STMT_HANDLER,
                "prepared statement has no open cursor",
            );
            self.codec
                .write_packet(&mut self.stream, self.command_response_sequence, &err)
                .await?;
            return Ok(());
        };
        let result_charset =
            crate::charset::resolve_result_charset(self.context.character_set_results.as_deref());
        let columns = prepared_cursor.columns.clone();
        let mut remaining = count;
        let mut sequence_id = self.command_response_sequence;
        let mut fetch_err: Option<QueryError> = None;
        let mut exhausted = false;
        while remaining != 0 && !exhausted {
            let cols = columns.clone();
            let external = prepared_cursor.inner.external_io();
            let http = prepared_cursor.inner.external_http_io();
            let _admission = if external {
                match if http {
                    crate::engine::http::admit_coordinator()
                } else {
                    crate::engine::ai::runtime::admit_coordinator()
                } {
                    Ok(p) => Some(p),
                    Err(e) => {
                        fetch_err = Some(e);
                        break;
                    }
                }
            } else {
                None
            };
            let chunk_rows = remaining.min(stream_offload_batch_rows()).min(if external {
                prepared_cursor.inner.external_batch_rows().max(1)
            } else {
                usize::MAX
            });
            let chunk_bytes = stream_offload_batch_bytes();
            let work = move || {
                let mut encoded: Vec<Vec<u8>> = Vec::with_capacity(chunk_rows);
                let mut encoded_bytes = 0usize;
                let mut memory_charge =
                    crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                        crate::engine::executor::mem_budget::ControlledMemoryCategory::ProtocolTransient,
                        0,
                    )
                    .expect("zero-byte protocol reservation cannot fail");
                let mut err: Option<QueryError> = None;
                while encoded.len() < chunk_rows {
                    match prepared_cursor.pull_row() {
                        Ok(Some(row)) => {
                            let estimate =
                                crate::mysql::protocol::estimate_binary_row_packet_bytes(&row);
                            if let Err(e) = memory_charge.try_grow(estimate) {
                                err = Some(e);
                                break;
                            }
                            let mut packet = Vec::new();
                            if packet.try_reserve_exact(estimate).is_err() {
                                memory_charge.shrink(estimate);
                                err = Some(error(
                                    crate::error::catalog::ER_OUTOFMEMORY,
                                    "out of memory while encoding cursor row",
                                ));
                                break;
                            }
                            crate::mysql::protocol::binary_row_packet_into_with_charset(
                                &cols,
                                &row,
                                &mut packet,
                                result_charset,
                            );
                            if packet.capacity() > estimate {
                                if let Err(e) = memory_charge.try_grow(packet.capacity() - estimate)
                                {
                                    err = Some(e);
                                    break;
                                }
                            }
                            encoded_bytes = encoded_bytes.saturating_add(packet.capacity());
                            encoded.push(packet);
                            if encoded_bytes >= chunk_bytes {
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            err = Some(e);
                            break;
                        }
                    }
                }
                if !encoded.is_empty() {
                    prepared_cursor.record_fetch(encoded.len() as u64);
                }
                (prepared_cursor, encoded, err, memory_charge)
            };
            let join_result = if http {
                crate::engine::http::spawn_coordinator(work)
            } else if external {
                crate::engine::ai::runtime::spawn_coordinator(work)
            } else {
                crate::util::blocking_pool::spawn_session(self.blocking_affinity.as_ref(), work)
            }
            .await;
            let (returned_cursor, encoded, err, _encoded_charge) = match join_result {
                Ok(tuple) => tuple,
                Err(join_err) => {
                    let err = self.recover_join_error(join_err);
                    self.write_error_packet(sequence_id, &err).await?;
                    return Ok(());
                }
            };
            prepared_cursor = returned_cursor;
            let produced = encoded.len();
            if produced != 0 {
                sequence_id = self.write_binary_fetch_chunk(&encoded, sequence_id).await?;
                remaining = remaining.saturating_sub(produced);
            }
            if let Some(err) = err {
                fetch_err = Some(err);
                break;
            }
            exhausted = prepared_cursor.exhausted && prepared_cursor.lookahead.is_none();
            if produced == 0 {
                break;
            }
        }
        exhausted = prepared_cursor.exhausted && prepared_cursor.lookahead.is_none();
        if fetch_err.is_none() && !exhausted {
            if let Some(entry) = self.prepared.get_mut(&statement_id) {
                entry.cursor = Some(prepared_cursor);
            }
        }
        // On error or end-of-stream the cursor (and any underlying
        // executor snapshot / scan state) is dropped here.
        if let Some(err) = fetch_err {
            self.write_error_packet(sequence_id, &err).await?;
            return Ok(());
        }
        let status = self.server_status()
            | if exhausted {
                SERVER_STATUS_LAST_ROW_SENT
            } else {
                SERVER_STATUS_CURSOR_EXISTS
            };
        self.finish_binary_fetch(sequence_id, status).await
    }

    /// Write pre-encoded binary row packets (built off-worker by
    /// `handle_stmt_fetch`) followed by the terminating EOF/OK. Using
    /// `PacketBatchWriter` coalesces small per-row syscalls into one
    /// vectored write; especially worth it under TLS, where each
    /// individual write turns into a separate record.
    async fn write_binary_fetch_chunk(
        &mut self,
        encoded: &[Vec<u8>],
        mut sequence_id: u8,
    ) -> io::Result<u8> {
        if let Some(handle) = &self.handle {
            handle.set_base_state(ProcessState::QueryEnd);
        }
        let _sending = self
            .handle
            .as_ref()
            .map(|handle| handle.enter_stage(ProcessState::SendingToClient));
        let mut batch = self.codec.batch_writer(&mut self.stream);
        for packet in encoded {
            sequence_id = batch.write_packet_seq(sequence_id, packet).await?;
        }
        batch.finish().await?;
        Ok(sequence_id)
    }

    async fn finish_binary_fetch(&mut self, sequence_id: u8, server_status: u16) -> io::Result<()> {
        let client_capabilities = self.client_capabilities();
        let mut batch = self.codec.batch_writer(&mut self.stream);
        write_result_set_terminator_batched(
            &mut batch,
            sequence_id,
            client_capabilities,
            server_status,
            self.context.warning_count(),
        )
        .await?;
        batch.finish().await
    }

    async fn handle_stmt_close(&mut self, payload: &[u8]) -> io::Result<()> {
        if payload.len() >= 4 {
            let statement_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
            self.engine.log_general_event(
                &self.context,
                "Close stmt",
                &format!("statement_id={statement_id}"),
            );
            if let Some(prepared) = self.prepared.remove(&statement_id) {
                self.prepared_memory_bytes = self
                    .prepared_memory_bytes
                    .saturating_sub(prepared.memory_bytes);
                crate::engine::status_counters::adjust_prepared_stmt_count(-1);
                self.publish_prepared_memory();
            }
        }
        Ok(())
    }

    async fn handle_stmt_reset(&mut self, payload: &[u8]) -> io::Result<()> {
        if payload.len() < 4 {
            let err = command_err_packet_for_spec(
                crate::mysql::errors::ER_UNKNOWN_COM_ERROR,
                "bad COM_STMT_RESET packet",
            );
            self.codec
                .write_packet(&mut self.stream, self.command_response_sequence, &err)
                .await?;
        } else {
            let statement_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
            self.engine.log_general_event(
                &self.context,
                "Reset stmt",
                &format!("statement_id={statement_id}"),
            );
            let Some(entry) = self.prepared.get_mut(&statement_id) else {
                let err = command_err_packet_for_spec(
                    crate::mysql::errors::ER_UNKNOWN_STMT_HANDLER,
                    "unknown prepared statement handler",
                );
                self.codec
                    .write_packet(&mut self.stream, self.command_response_sequence, &err)
                    .await?;
                return Ok(());
            };
            {
                clear_prepared_long_data(entry);
                entry.long_data_error = None;
                entry.cursor = None;
            }
            let ok = self.ok_packet();
            self.codec
                .write_packet(&mut self.stream, self.command_response_sequence, &ok)
                .await?;
        }
        Ok(())
    }

    /// COM_FIELD_LIST (0x04) — legacy `mysql_list_fields` surface used by
    /// old mysqldump / ODBC / Perl-DBI probes. Payload is a NUL-terminated
    /// table name followed by an optional wildcard pattern (LIKE syntax,
    /// MySQL only supports `%` and `_`, no ESCAPE).
    ///
    /// Response: one ColumnDefinition41 packet per matching column,
    /// followed by an EOF packet. Errors out with 1146 ER_NO_SUCH_TABLE
    /// when the named table doesn't exist on the session's current DB.
    async fn handle_field_list(&mut self, payload: &[u8]) -> io::Result<()> {
        if self.context.current_database.is_empty() {
            let err = crate::error::catalog::ER_NO_DB_ERROR.error("No database selected");
            return self
                .write_error_packet(self.command_response_sequence, &err)
                .await;
        }
        let nul_idx = payload
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(payload.len());
        let table_bytes = &payload[..nul_idx];
        let wildcard_bytes = if nul_idx < payload.len() {
            &payload[(nul_idx + 1).min(payload.len())..]
        } else {
            &[][..]
        };
        let table_name = match decode_session_sql(&self.context, table_bytes) {
            Ok(value) => value,
            Err(err) => {
                return self
                    .write_error_packet(self.command_response_sequence, &err)
                    .await;
            }
        };
        let wildcard = match decode_session_sql(&self.context, wildcard_bytes) {
            Ok(value) => value,
            Err(err) => {
                return self
                    .write_error_packet(self.command_response_sequence, &err)
                    .await;
            }
        };
        let columns = match self
            .engine
            .field_list_columns(&self.context, &table_name, &wildcard)
        {
            Ok(c) => c,
            Err(err) => {
                let err_packet = crate::mysql::protocol::err_packet(&err);
                return self
                    .codec
                    .write_packet(
                        &mut self.stream,
                        self.command_response_sequence,
                        &err_packet,
                    )
                    .await;
            }
        };
        let mut seq: u8 = self.command_response_sequence;
        for packet in columns {
            self.codec
                .write_packet(&mut self.stream, seq, &packet)
                .await?;
            seq = seq.wrapping_add(1);
        }
        // Trailing EOF — MySQL's COM_FIELD_LIST always uses the legacy
        // EOF packet format (the deprecate-EOF capability bit doesn't
        // apply to this command surface).
        let eof = eof_packet_with_status(self.server_status());
        self.codec.write_packet(&mut self.stream, seq, &eof).await
    }

    async fn handle_init_db(&mut self, db_bytes: &[u8]) -> io::Result<()> {
        let db_name = match decode_session_sql(&self.context, db_bytes) {
            Ok(value) => value,
            Err(err) => {
                return self
                    .write_error_packet(self.command_response_sequence, &err)
                    .await;
            }
        };
        self.engine
            .log_general_event(&self.context, "Init DB", db_name.as_str());
        let sql = format!("USE `{}`", db_name.replace('`', "``"));
        match self.execute_engine_blocking(sql).await {
            Ok(_) => {
                self.process_database = Some(self.context.current_database.clone());
                let ok = self.ok_packet();
                self.codec
                    .write_packet(&mut self.stream, self.command_response_sequence, &ok)
                    .await
            }
            Err(err) => {
                self.write_error_packet(self.command_response_sequence, &err)
                    .await
            }
        }
    }

    async fn handle_set_option(&mut self, payload: &[u8]) -> io::Result<()> {
        if payload.len() != 2 {
            let err = command_err_packet_for_spec(
                crate::mysql::errors::ER_UNKNOWN_COM_ERROR,
                format!(
                    "COM_SET_OPTION payload must be 2 bytes, got {}",
                    payload.len()
                ),
            );
            return self
                .codec
                .write_packet(&mut self.stream, self.command_response_sequence, &err)
                .await;
        }
        match u16::from_le_bytes([payload[0], payload[1]]) {
            0 => self.capabilities.set_multi_statements(true),
            1 => self.capabilities.set_multi_statements(false),
            option => {
                let err = command_err_packet_for_spec(
                    crate::mysql::errors::ER_UNKNOWN_COM_ERROR,
                    format!("unsupported COM_SET_OPTION operation {option}"),
                );
                return self
                    .codec
                    .write_packet(&mut self.stream, self.command_response_sequence, &err)
                    .await;
            }
        }
        self.write_ok_packet(self.command_response_sequence).await
    }

    /// Move the session context out for a blocking-pool closure and install
    /// the recycled placeholder. Pair with [`Self::restore_context`]. If the
    /// worker panics, `ParkedSessionContext::drop` returns the real context
    /// through `parked_recovery` before the join error reaches this session.
    fn park_context(&mut self) -> ParkedSessionContext {
        self.recover_parked_context();
        let placeholder = self
            .spare_context
            .take()
            .unwrap_or_else(|| Box::new(SessionContext::default()));
        {
            let mut state = self.parked_recovery.state.lock();
            debug_assert!(!state.in_flight);
            state.in_flight = true;
            state.cancel = self.context.current_cancel_token().cloned();
        }
        ParkedSessionContext {
            context: Some(std::mem::replace(&mut self.context, placeholder)),
            recovery: Arc::clone(&self.parked_recovery),
        }
    }

    /// Put the real context back and stash the placeholder for reuse by
    /// the next [`Self::park_context`].
    fn restore_context(&mut self, context: ParkedSessionContext) {
        self.spare_context = Some(std::mem::replace(&mut self.context, context.into_inner()));
    }

    fn recover_parked_context(&mut self) {
        if let Some(context) = self.parked_recovery.state.lock().context.take() {
            self.spare_context = Some(std::mem::replace(&mut self.context, context));
        }
    }

    fn recover_join_error(&mut self, err: crate::util::blocking_pool::BlockingError) -> QueryError {
        self.recover_parked_context();
        // A failed dispatch cannot guarantee that the engine left its
        // transaction in a usable state. Send the error, then let ordinary
        // connection teardown roll back and release its locks.
        self.kill_flag.store(true, Ordering::Release);
        query_join_error(err)
    }

    /// Acquire a transaction-admission permit for this session's current
    /// transaction, unless the feature is disabled (`MYDB_TXN_ADMISSION=0`)
    /// or we already hold one. Awaited on the session's async task — it never
    /// occupies a blocking-pool/worker thread and holds no lock while waiting,
    /// so a session parked here cannot block an already-admitted transaction.
    async fn acquire_txn_admission(&mut self) {
        if self.txn_admission.is_some() {
            return;
        }
        if crate::util::exec_permit::global().is_some() {
            let _waiting = self
                .handle
                .as_ref()
                .map(|handle| handle.enter_stage(ProcessState::WaitingOnCond));
            // `acquire_owned` only errors when the semaphore is closed, which
            // never happens here; on that impossible error we run un-admitted.
            if let Some(permit) = crate::util::exec_permit::acquire().await {
                self.txn_admission = Some(permit);
            }
        }
    }

    /// Release the admission permit once the session no longer has an active
    /// write transaction (commit / rollback). Keyed off `current_txn` rather
    /// than `in_transaction()` — the latter is permanently true whenever
    /// `autocommit=0` (as JDBC/BenchmarkSQL run), which would pin every permit
    /// forever. An IDLE XA branch still owns row locks after detaching
    /// `current_txn`. Release only when both session transaction owners end.
    /// Re-acquire (holding no locks) at the next transaction's first
    /// lock-taking statement — the invariant that keeps admission deadlock-free.
    fn release_txn_admission_if_idle(&mut self) {
        if self.txn_admission.is_some()
            && self.context.current_txn.is_none()
            && self.context.xa_branch.is_none()
        {
            self.txn_admission = None;
        }
    }

    async fn execute_engine_blocking(&mut self, sql: String) -> Result<QueryResponse, QueryError> {
        // No `.and_then(QueryResponse::materialize)` here: streaming
        // result sets are consumed by the text/binary streaming writers
        // on the main task. The blocking pool only runs the engine's
        // synchronous dispatch + scan setup; the rows themselves are
        // pulled lazily during socket writeback, so server memory stays
        // bounded even on a `SELECT * FROM huge_table`.
        let ai = crate::engine::ai::sql::sql_may_have_ai(&sql);
        let http = self.sql_may_call_http(&sql);
        let _http_session = if http {
            Some(crate::engine::http::admit_session(&self.context).await?)
        } else {
            None
        };
        let _admission = if http {
            Some(crate::engine::http::admit_coordinator()?)
        } else if ai {
            Some(crate::engine::ai::runtime::admit_coordinator()?)
        } else {
            None
        };
        let engine = Arc::clone(&self.engine);
        let mut context = self.park_context();
        let work = move || {
            let result = engine.execute(&mut context, &sql);
            (context, result)
        };
        let job = if http {
            crate::engine::http::spawn_coordinator(work)
        } else if ai {
            crate::engine::ai::runtime::spawn_coordinator(work)
        } else {
            crate::util::blocking_pool::spawn_affine(self.blocking_affinity.as_ref(), work)
        };
        let (context, result) = job.await.map_err(|err| self.recover_join_error(err))?;
        self.restore_context(context);

        result
    }

    async fn execute_engine_blocking_parsed(
        &mut self,
        sql: String,
        statement: Arc<Statement>,
        params: Option<(Vec<crate::types::Value>, Arc<str>)>,
    ) -> Result<QueryResponse, QueryError> {
        // The protocol already parsed this SELECT to decide whether it may
        // run inline. Locking reads still belong on the blocking pool, but
        // there is no reason to throw away that authoritative AST and parse
        // the identical COM_QUERY a second time on the worker.
        let ai = crate::engine::ai::sql::statement_has_ai(&statement);
        let http = self.sql_may_call_http(&sql);
        let _http_session = if http {
            Some(crate::engine::http::admit_session(&self.context).await?)
        } else {
            None
        };
        let _admission = if http {
            Some(crate::engine::http::admit_coordinator()?)
        } else if ai {
            Some(crate::engine::ai::runtime::admit_coordinator()?)
        } else {
            None
        };
        let engine = Arc::clone(&self.engine);
        // Fusion and blocking functions need independent transport progress.
        // Retain ordinary request-worker scheduling for other SELECTs.
        let needs_async_progress = statement_contains_fusion(statement.as_ref())
            || statement_contains_blocking_function(statement.as_ref());
        let mut context = self.park_context();
        let work = move || {
            let result = match params {
                Some((params, shape_sql)) => engine.execute_parsed_with_params(
                    &mut context,
                    &sql,
                    statement,
                    params,
                    shape_sql,
                ),
                None => engine.execute_parsed(&mut context, &sql, statement),
            };
            (context, result)
        };
        let job = if http {
            crate::engine::http::spawn_coordinator(work)
        } else if ai {
            crate::engine::ai::runtime::spawn_coordinator(work)
        } else if needs_async_progress {
            crate::util::blocking_pool::spawn_affine(self.blocking_affinity.as_ref(), work)
        } else {
            crate::util::blocking_pool::spawn_session(self.blocking_affinity.as_ref(), work)
        };
        let (context, result) = job.await.map_err(|err| self.recover_join_error(err))?;
        self.restore_context(context);

        result
    }

    /// Shared parser artifacts remain unqualified. Only a new per-session
    /// execution memo needs a qualified clone; normal execution owns any error.
    fn qualify_new_text_shape(
        &mut self,
        sql: &str,
        parsed: &Statement,
        is_new: bool,
    ) -> Option<Arc<Statement>> {
        is_new
            .then(|| {
                self.engine
                    .prepare_statement_template(&mut self.context, sql, parsed.clone())
            })
            .transpose()
            .ok()
            .flatten()
            .map(Arc::new)
    }

    /// Execute a shape-matched literal COM_QUERY DML statement through the
    /// compiled prepared-DML vehicle, using the same scheduling policy
    /// COM_STMT_EXECUTE applies to the identical plan.
    ///
    /// Autocommit statements run inline on this runtime worker. Statements
    /// inside an explicit transaction first try inline with non-blocking lock
    /// waits and fall back to the blocking pool on conflict, so a row-lock
    /// wait never parks a tokio worker. Anything the compiled vehicle declines
    /// (`None`) replays on the ordinary blocking-pool path, which owns the
    /// authoritative semantics.
    async fn execute_text_dml_shape(
        &mut self,
        sql: String,
        parsed: Arc<Statement>,
        params: Vec<crate::types::Value>,
        shape_sql: Arc<str>,
    ) -> io::Result<()> {
        let Some(cache) = self.text_shape_memos.as_mut() else {
            let result = self.execute_engine_blocking(sql).await;
            return self.write_query_response(result).await;
        };
        let (mut memo, memo_is_new) = cache.take_or_new(&shape_sql);
        // Parser-shape artifacts are shared across databases and therefore
        // stay unqualified; the compiled DML vehicle expects the PREPARE-time
        // qualified AST. Qualify one short-lived clone only when this
        // per-connection shape memo is born.
        let qualified = self.qualify_new_text_shape(&sql, &parsed, memo_is_new);
        let compile_parsed = qualified.unwrap_or(parsed);

        let dml_state = std::mem::take(&mut memo.dml_state);
        let (dml_state, result) = self
            .dispatch_text_dml_shape(&sql, &compile_parsed, dml_state, &params, &shape_sql)
            .await;
        memo.dml_state = dml_state;
        if let Some(cache) = self.text_shape_memos.as_mut() {
            cache.put(memo);
        }
        let result = match result {
            Some(result) => result,
            // The compiled vehicle declined this statement. Replay it on the
            // ordinary path rather than reporting a failure: that path is the
            // authority for every shape the vehicle does not cover.
            None => self.execute_engine_blocking(sql).await,
        };
        if self.kill_flag.load(Ordering::Acquire) {
            return Ok(());
        }
        self.write_query_response(result).await
    }

    /// Scheduling half of [`Self::execute_text_dml_shape`]: mirrors the
    /// inline / non-blocking-retry / blocking-pool ladder that
    /// COM_STMT_EXECUTE runs for prepared DML.
    async fn dispatch_text_dml_shape(
        &mut self,
        sql: &str,
        parsed: &Arc<Statement>,
        dml_state: crate::engine::api::PreparedDmlState,
        params: &[crate::types::Value],
        shape_sql: &Arc<str>,
    ) -> (
        crate::engine::api::PreparedDmlState,
        Option<Result<QueryResponse, QueryError>>,
    ) {
        if prepared_dml_inline_allowed(&self.context) {
            return self.run_text_dml_shape_inline(sql, parsed, dml_state, params, shape_sql);
        }
        if prepared_dml_transactional_inline_retry_allowed(parsed.as_ref()) {
            // The inline attempt may take this transaction's first row lock;
            // enter admission while it still holds no locks.
            self.acquire_txn_admission().await;
            let (state, result) = crate::storage::with_nonblocking_lock_waits(|| {
                self.run_text_dml_shape_inline(sql, parsed, dml_state, params, shape_sql)
            });
            let would_block = matches!(
                &result,
                Some(Err(err)) if crate::storage::is_lock_would_block_error(err)
            );
            if !would_block {
                return (state, result);
            }
            crate::storage::record_inline_lock_would_block_retry();
            return self
                .run_text_dml_shape_blocking(sql, parsed, state, params, shape_sql)
                .await;
        }
        self.run_text_dml_shape_blocking(sql, parsed, dml_state, params, shape_sql)
            .await
    }

    fn run_text_dml_shape_inline(
        &mut self,
        sql: &str,
        parsed: &Arc<Statement>,
        mut dml_state: crate::engine::api::PreparedDmlState,
        params: &[crate::types::Value],
        shape_sql: &Arc<str>,
    ) -> (
        crate::engine::api::PreparedDmlState,
        Option<Result<QueryResponse, QueryError>>,
    ) {
        let engine = Arc::clone(&self.engine);
        let result = crate::engine::perf_events::with_statement_digest_sql_override(
            Arc::clone(shape_sql),
            || engine.execute_prepared_dml(&mut self.context, &mut dml_state, parsed, sql, params),
        )
        .map(|result| result.and_then(QueryResponse::materialize));

        (dml_state, result)
    }

    async fn run_text_dml_shape_blocking(
        &mut self,
        sql: &str,
        parsed: &Arc<Statement>,
        dml_state: crate::engine::api::PreparedDmlState,
        params: &[crate::types::Value],
        shape_sql: &Arc<str>,
    ) -> (
        crate::engine::api::PreparedDmlState,
        Option<Result<QueryResponse, QueryError>>,
    ) {
        // Transaction admission: gate concurrent open transactions at the
        // first lock-taking statement, exactly as the prepared blocking path
        // does. No-op when disabled or a permit is already held.
        self.acquire_txn_admission().await;
        let engine = Arc::clone(&self.engine);
        let mut context = self.park_context();
        let sql = sql.to_owned();
        let parsed = Arc::clone(parsed);
        let params = params.to_vec();
        let shape_sql = Arc::clone(shape_sql);
        let mut state = dml_state;
        match crate::util::blocking_pool::spawn_session(
            self.blocking_affinity.as_ref(),
            move || {
                let result = crate::engine::perf_events::with_statement_digest_sql_override(
                    shape_sql,
                    || {
                        engine.execute_prepared_dml(
                            &mut context,
                            &mut state,
                            &parsed,
                            &sql,
                            &params,
                        )
                    },
                )
                .map(|result| result.and_then(QueryResponse::materialize));
                (context, state, result)
            },
        )
        .await
        {
            Ok((context, state, result)) => {
                self.restore_context(context);

                (state, result)
            }
            Err(err) => (
                crate::engine::api::PreparedDmlState::default(),
                Some(Err(self.recover_join_error(err))),
            ),
        }
    }

    async fn execute_engine_blocking_shape(
        &mut self,
        sql: String,
        statement: Arc<Statement>,
        params: Vec<crate::types::Value>,
        shape_sql: Arc<str>,
        mut dml_state: crate::engine::api::PreparedDmlState,
    ) -> (
        crate::engine::api::PreparedDmlState,
        Result<QueryResponse, QueryError>,
    ) {
        let engine = Arc::clone(&self.engine);
        let mut context = self.park_context();
        let joined =
            crate::util::blocking_pool::spawn_session(self.blocking_affinity.as_ref(), move || {
                let result = crate::engine::api::with_shared_result_metadata(|| {
                    // Same shape-scoped digest binding as the inline route; the
                    // `execute_parsed_with_params` fallback installs it itself, so
                    // only the compiled-DML branch needs the wrapper here.
                    match crate::engine::perf_events::with_statement_digest_sql_override(
                        Arc::clone(&shape_sql),
                        || {
                            engine.execute_prepared_dml(
                                &mut context,
                                &mut dml_state,
                                &statement,
                                &sql,
                                &params,
                            )
                        },
                    ) {
                        Some(result) => result,
                        None => engine.execute_parsed_with_params(
                            &mut context,
                            &sql,
                            statement,
                            params,
                            shape_sql,
                        ),
                    }
                });
                (context, dml_state, result)
            })
            .await;
        match joined {
            Ok((context, dml_state, result)) => {
                self.restore_context(context);

                (dml_state, result)
            }
            Err(err) => (
                crate::engine::api::PreparedDmlState::default(),
                Err(self.recover_join_error(err)),
            ),
        }
    }

    async fn execute_engine_blocking_with_parser_mode(
        &mut self,
        sql: String,
        parser_sql_mode_bits: u16,
    ) -> Result<QueryResponse, QueryError> {
        let engine = Arc::clone(&self.engine);
        let mut context = self.park_context();
        let (context, result) =
            crate::util::blocking_pool::spawn_affine(self.blocking_affinity.as_ref(), move || {
                let _override =
                    crate::parser::ParserSqlModeOverrideGuard::activate(parser_sql_mode_bits);
                let result = engine.execute(&mut context, &sql);
                (context, result)
            })
            .await
            .map_err(|err| self.recover_join_error(err))?;
        self.restore_context(context);

        result
    }

    /// Bind the template and describe its result in one blocking-pool job.
    /// Neither phase needs an intervening network exchange or async wakeup.
    async fn prepare_cached_blocking(
        &mut self,
        sql: String,
        expected_param_count: usize,
    ) -> Result<(Arc<Statement>, Vec<Column>), QueryError> {
        let engine = Arc::clone(&self.engine);
        let mut context = self.park_context();
        let (context, result) =
            crate::util::blocking_pool::spawn_affine(self.blocking_affinity.as_ref(), move || {
                let result = engine
                    .prepare_cached(&mut context, &sql, expected_param_count)
                    .and_then(|parsed| {
                        engine
                            .describe_prepared_statement(&mut context, &sql)
                            .map(|columns| (parsed, columns))
                    });
                (context, result)
            })
            .await
            .map_err(|err| self.recover_join_error(err))?;
        self.restore_context(context);

        result
    }

    async fn execute_prepared_statement_blocking(
        &mut self,
        sql: Arc<str>,
        statement: Statement,
        params: Vec<crate::types::Value>,
        hint_keys: Option<(Arc<str>, Arc<str>)>,
        perf_texts: crate::engine::perf_events::PreparedStatementTexts,
    ) -> Option<Result<QueryResponse, QueryError>> {
        // Statements that may take row locks have to run on the
        // `spawn_blocking` pool: the executor's row-lock acquire path
        // parks the OS thread on `Condvar::wait_timeout`. Running such a
        // query directly on a tokio worker would block one of the (small)
        // `num_cpus` worker threads — under TPC-C all workers can end up
        // blocked on locks held by transactions whose own commit task is
        // queued behind them, producing a distributed deadlock that
        // surfaces as "lock wait timeout exceeded".
        //
        // Pure read-only SELECT (no FOR UPDATE / FOR SHARE) never calls
        // the blocking lock-wait primitives, so we can run it inline on
        // the current task and skip the ~10-30µs `spawn_blocking` task
        // switch. Buffer-pool disk I/O for cold pages can briefly stall
        // the worker, but that is the same cost a sync syscall would have
        // and matches existing async storage-server design.
        //
        // `params` is installed in the thread-local parameter context for
        // the duration of execution so any `Expr::Placeholder(idx)` left
        // in the AST resolves to the bound value at eval time. This is
        // what lets prepared statements share a single cached AST/plan
        // across many parameter sets.
        // We deliberately do NOT call `QueryResponse::materialize` here:
        // when an engine returns `StreamingResultSet`, the session writers
        // (`write_stmt_execute_response`, `write_binary_multi_result`,
        // `write_stmt_cursor_execute_response`) all consume the cursor on
        // demand. Materializing up front would defeat server-side cursor
        // fetching (`useCursorFetch=true`) by buffering the full result
        // set in memory before the FETCH loop even starts.
        if !self.sql_may_call_http(&sql)
            && statement_is_lock_safe_read(&statement)
            && !statement_has_computed_ordering(&statement)
            && context_allows_inline_lock_safe_read(&self.context)
        {
            let engine = Arc::clone(&self.engine);
            let result =
                crate::engine::perf_events::with_prepared_statement_texts(&perf_texts, || {
                    crate::engine::params::with_params(params, || {
                        let run = || {
                            engine.execute_prepared_statement(&mut self.context, &sql, statement)
                        };
                        match hint_keys {
                            Some((normalized_sql, database)) => {
                                crate::engine::cache::with_prepared_hint(
                                    crate::engine::cache::PreparedHint {
                                        normalized_sql,
                                        database,
                                    },
                                    run,
                                )
                            }
                            None => run(),
                        }
                    })
                });
            return result;
        }
        // Transaction admission: a lock-taking statement is the point a
        // transaction starts holding hot-row locks, so gate concurrent open
        // transactions here. No-op when disabled or a permit is already held.
        if !statement_is_lock_safe_read(&statement)
            || !context_allows_inline_lock_safe_read(&self.context)
        {
            self.acquire_txn_admission().await;
        }
        let ai = crate::engine::ai::sql::statement_has_ai(&statement);
        let http = self.sql_may_call_http(&sql);
        let _http_session = if http {
            match crate::engine::http::admit_session(&self.context).await {
                Ok(p) => Some(p),
                Err(e) => return Some(Err(e)),
            }
        } else {
            None
        };
        let _admission = if http {
            match crate::engine::http::admit_coordinator() {
                Ok(p) => Some(p),
                Err(e) => return Some(Err(e)),
            }
        } else if ai {
            match if http {
                crate::engine::http::admit_coordinator()
            } else {
                crate::engine::ai::runtime::admit_coordinator()
            } {
                Ok(p) => Some(p),
                Err(e) => return Some(Err(e)),
            }
        } else {
            None
        };
        let engine = Arc::clone(&self.engine);
        let mut context = self.park_context();
        let work = move || {
            let result =
                crate::engine::perf_events::with_prepared_statement_texts(&perf_texts, || {
                    crate::engine::params::with_params(params, || {
                        let run =
                            || engine.execute_prepared_statement(&mut context, &sql, statement);
                        match hint_keys {
                            Some((normalized_sql, database)) => {
                                crate::engine::cache::with_prepared_hint(
                                    crate::engine::cache::PreparedHint {
                                        normalized_sql,
                                        database,
                                    },
                                    run,
                                )
                            }
                            None => run(),
                        }
                    })
                });
            (context, result)
        };
        let job = if http {
            crate::engine::http::spawn_coordinator(work)
        } else if ai {
            crate::engine::ai::runtime::spawn_coordinator(work)
        } else {
            crate::util::blocking_pool::spawn_affine(self.blocking_affinity.as_ref(), work)
        };
        match job.await {
            Ok((context, result)) => {
                self.restore_context(context);

                result
            }
            Err(err) => Some(Err(self.recover_join_error(err))),
        }
    }

    /// After an inline exact-point command reports an internal row-lock
    /// conflict, keep the connection task on Tokio and wait for storage's
    /// monotonic row-release signal. Each retry still runs under the
    /// nonblocking lock policy, so it can never park a runtime worker. The
    /// The conflict records the release generation before probing, so a
    /// release racing with subscription is observed without an unconditional
    /// replay. Complex/long waits retain the bounded blocking-pool oracle
    /// after one directed wake opportunity (or a quiet 2ms slice).
    async fn retry_prepared_dml_after_async_row_wake(
        &mut self,
        sql: &str,
        parsed: &Arc<Statement>,
        mut state: crate::engine::api::PreparedDmlState,
        params: &[crate::types::Value],
        perf_texts: &crate::engine::perf_events::PreparedStatementTexts,
    ) -> (
        crate::engine::api::PreparedDmlState,
        Option<Result<QueryResponse, QueryError>>,
        bool,
    ) {
        if !async_row_lock_retry_enabled() {
            return (state, None, false);
        }
        let Some(mut signal) = self.engine.row_lock_async_signal() else {
            ASYNC_ROW_LOCK_FALLBACKS.fetch_add(1, Ordering::Relaxed);
            return (state, None, false);
        };
        for _ in 0..ASYNC_ROW_LOCK_RETRY_LIMIT {
            match tokio::time::timeout(ASYNC_ROW_LOCK_WAIT_SLICE, signal.changed()).await {
                Ok(Ok(())) => {
                    ASYNC_ROW_LOCK_WAKEUPS.fetch_add(1, Ordering::Relaxed);
                }
                Ok(Err(_)) | Err(_) => {
                    ASYNC_ROW_LOCK_FALLBACKS.fetch_add(1, Ordering::Relaxed);
                    return (state, None, false);
                }
            }
            ASYNC_ROW_LOCK_RETRIES.fetch_add(1, Ordering::Relaxed);
            let (next_state, result) = crate::storage::with_nonblocking_lock_waits(|| {
                self.try_execute_prepared_dml_inline(sql, parsed, state, params, perf_texts)
            });
            state = next_state;
            let would_block = matches!(
                &result,
                Some(Err(err)) if crate::storage::is_lock_would_block_error(err)
            );
            if !would_block {
                return (state, result, true);
            }
        }
        ASYNC_ROW_LOCK_FALLBACKS.fetch_add(1, Ordering::Relaxed);
        (state, None, false)
    }

    /// Run the prepared DML fast path on the blocking pool. Returns the
    /// (possibly updated) state and the owned parameter buffer alongside an
    /// `Option<Result<...>>`:
    /// `Some(_)` means the fast path classified and executed the
    /// statement, `None` means the caller should drop into the legacy
    /// path. This fallback allows blocking row-lock waits on `Condvar`.
    /// Transactional inline attempts instead use nonblocking lock waits and
    /// statement-savepoint rollback, so they cannot park a Tokio worker while
    /// another connection needs it to commit and release its locks.
    async fn try_execute_prepared_dml_blocking(
        &mut self,
        sql: Arc<str>,
        parsed: Arc<Statement>,
        dml_state: crate::engine::api::PreparedDmlState,
        params: Vec<crate::types::Value>,
        perf_texts: crate::engine::perf_events::PreparedStatementTexts,
    ) -> (
        crate::engine::api::PreparedDmlState,
        Option<Result<QueryResponse, QueryError>>,
        Vec<crate::types::Value>,
    ) {
        // A SELECT memo can decline complex reads, which then pass through
        // this optional DML vehicle before the general executor. Decline
        // unsupported statement kinds before acquiring a transaction permit:
        // an ordinary read does not become a lock-holding transaction merely
        // because it missed an optimization. The general path still applies
        // its own locking/isolation admission checks (including SERIALIZABLE).
        if !prepared_dml_statement_candidate(parsed.as_ref()) {
            return (dml_state, None, params);
        }
        self.acquire_txn_admission().await;
        let engine = Arc::clone(&self.engine);
        let mut context = self.park_context();
        let mut state = dml_state;
        match crate::util::blocking_pool::spawn_session(
            self.blocking_affinity.as_ref(),
            move || {
                let result =
                    crate::engine::perf_events::with_prepared_statement_texts(&perf_texts, || {
                        crate::engine::api::with_shared_result_metadata(|| {
                            engine.execute_prepared_dml(
                                &mut context,
                                &mut state,
                                &parsed,
                                &sql,
                                &params,
                            )
                        })
                    })
                    .map(|r| r.and_then(QueryResponse::materialize));
                (context, state, result, params)
            },
        )
        .await
        {
            Ok((context, state, result, params)) => {
                self.restore_context(context);

                (state, result, params)
            }
            Err(err) => (
                crate::engine::api::PreparedDmlState::default(),
                Some(Err(self.recover_join_error(err))),
                Vec::new(),
            ),
        }
    }

    fn try_execute_prepared_dml_inline(
        &mut self,
        sql: &str,
        parsed: &Arc<Statement>,
        mut dml_state: crate::engine::api::PreparedDmlState,
        params: &[crate::types::Value],
        perf_texts: &crate::engine::perf_events::PreparedStatementTexts,
    ) -> (
        crate::engine::api::PreparedDmlState,
        Option<Result<QueryResponse, QueryError>>,
    ) {
        if self.sql_may_call_http(sql) {
            return (dml_state, None);
        }
        // The storage engine is synchronous: even an uncontended autocommit
        // DML reaches WAL pwrite/fsync and group-commit Condvar waits. Tokio's
        // `block_in_place` can compensate for that by starting replacement
        // workers, but doing so for every short autocommit statement creates a
        // second runnable worker population and amplifies scheduler/park-wake
        // traffic. With the normal worker count (4 * CPU, at least 16), the
        // connection tasks themselves already provide enough concurrency for
        // group commit. Keep `block_in_place` as an opt-in escape hatch for
        // deployments with an unusually small `MYDB_TOKIO_WORKERS` setting.
        let use_block_in_place = prepared_dml_block_in_place_enabled()
            && tokio::runtime::Handle::try_current()
                .map(|handle| handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
                .unwrap_or(false);
        // Borrow the engine: cloning the process-wide Arc on every statement
        // makes all connections contend on one reference count.
        let engine = &self.engine;
        let mut execute = || {
            crate::engine::api::with_shared_result_metadata(|| {
                engine.execute_prepared_dml(&mut self.context, &mut dml_state, parsed, sql, params)
            })
        };
        let result = crate::engine::perf_events::with_prepared_statement_texts(perf_texts, || {
            if use_block_in_place {
                tokio::task::block_in_place(execute)
            } else {
                execute()
            }
        })
        .map(|r| {
            r.and_then(|response| match response {
                // A locking read's shared rows are written as-is; owning
                // them would deep-copy the column metadata and then force a
                // deep comparison against the cached prepared metadata.
                QueryResponse::SharedResultSet(_) => Ok(response),
                response => response.materialize(),
            })
        });

        (dml_state, result)
    }

    async fn close_session_blocking(&mut self) -> Result<(), QueryError> {
        // Session cleanup is broader than transaction rollback: it releases
        // account connection quota, temporary tables, explicit/metadata
        // locks, named locks and AUTO_INCREMENT leftovers. Skipping the
        // engine call merely because no transaction is active leaks all of
        // those resources on the overwhelmingly common autocommit disconnect.
        if self.engine.session_cleanup_is_trivial(&self.context) {
            let result = self.engine.close_session(&mut self.context);
            self.context.refresh_cancel_generations();
            return result;
        }
        let engine = Arc::clone(&self.engine);
        let mut context = self.park_context();
        let (context, result) =
            crate::util::blocking_pool::spawn_affine(self.blocking_affinity.as_ref(), move || {
                let result = engine.close_session(&mut context);
                (context, result)
            })
            .await
            .map_err(|err| self.recover_join_error(err))?;
        self.restore_context(context);
        self.context.refresh_cancel_generations();
        result
    }
}

fn prepared_dml_inline_enabled() -> bool {
    static CACHED: OnceLock<bool> = OnceLock::new();
    *CACHED.get_or_init(|| {
        !matches!(
            crate::util::env::var("MYDB_PREPARED_DML_INLINE")
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "off" | "false" | "no"
        )
    })
}

/// Whether a DML statement running inside an explicit / autocommit-off
/// transaction may first be attempted inline on the runtime worker, with
/// non-blocking lock waits and a blocking-pool retry on conflict.
///
/// All three write statement kinds and exact locking point reads compile to the same
/// `execute_prepared_dml_plan` vehicle, and that vehicle is wrapped in
/// `execute_dml_in_transaction_with_session`, which takes a statement
/// savepoint and rolls back to it on error — including the lock-wait-pending
/// case, which rolls back while preserving the wait. A would-block attempt
/// therefore leaves no partial effect behind for any of them, so restricting
/// the attempt to UPDATE only bought a cross-thread dispatch per INSERT and
/// per DELETE. TPC-C runs every statement inside an explicit transaction, so
/// that was the whole NewOrder insert stream plus Delivery's deletes.
///
/// The retry is a re-execution, so a statement that consumed an
/// AUTO_INCREMENT value on its first attempt takes a fresh one on the retry.
/// That matches MySQL, where a rolled-back INSERT likewise leaves an
/// auto-increment gap.
fn prepared_dml_transactional_inline_retry_allowed(statement: &Statement) -> bool {
    prepared_dml_inline_enabled()
        && !crate::engine::http::statement_has_http(statement)
        && prepared_dml_statement_candidate(statement)
}

fn prepared_dml_statement_candidate(statement: &Statement) -> bool {
    match statement {
        Statement::Update(_) | Statement::Insert(_) | Statement::Delete(_) => true,
        Statement::Select(select) => select.for_update || select.lock_clause.is_some(),
        _ => false,
    }
}

fn prepared_dml_statement_allows_inline_execution<'q>(
    statement: impl Into<crate::parser::QueryRef<'q>>,
) -> bool {
    let statement = statement.into();
    !matches!(
        statement,
        crate::parser::QueryRef::Select(select) if select.for_update || select.lock_clause.is_some()
    )
}

fn prepared_dml_block_in_place_enabled() -> bool {
    static CACHED: OnceLock<bool> = OnceLock::new();
    *CACHED.get_or_init(|| {
        matches!(
            crate::util::env::var("MYDB_PREPARED_DML_BLOCK_IN_PLACE")
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "1" | "on" | "true" | "yes"
        )
    })
}

fn prepared_dml_inline_allowed(context: &SessionContext) -> bool {
    prepared_dml_inline_enabled()
        && context.autocommit
        && !context.explicit_txn
        && context.current_txn.is_none()
}

fn mysql_tls_version_name(version: ProtocolVersion) -> &'static str {
    match version {
        ProtocolVersion::TLSv1_2 => "TLSv1.2",
        ProtocolVersion::TLSv1_3 => "TLSv1.3",
        _ => "UNKNOWN",
    }
}

// Most SQL has no introducer. Avoid a second lexical pass for ordinary
// statements, including identifiers that merely contain underscores.
fn has_quoted_charset_introducer(bytes: &[u8]) -> bool {
    memchr::memchr_iter(b'_', bytes).any(|start| {
        if start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_') {
            return false;
        }
        let mut end = start + 1;
        while end < bytes.len() && bytes[end].is_ascii_alphanumeric() {
            end += 1;
        }
        let mut next = end;
        while next < bytes.len() && bytes[next].is_ascii_whitespace() {
            next += 1;
        }
        matches!(bytes.get(next), Some(b'\'' | b'"'))
            && std::str::from_utf8(&bytes[start + 1..end])
                .ok()
                .is_some_and(|name| crate::charset::resolve_charset(name).is_ok())
    })
}

fn decode_session_sql(session: &SessionContext, bytes: &[u8]) -> Result<String, QueryError> {
    match crate::charset::decode_bytes(&session.character_set_client, bytes) {
        Ok(sql) if !has_quoted_charset_introducer(bytes) => Ok(sql),
        decoded => {
            let mode = crate::sql_lex::SqlLexMode::from_parser_bits(
                crate::engine::api::parse_temporal_sql_mode_bits(&session.sql_mode),
            );
            let mut sql = String::new();
            for segment in
                crate::sql_lex::segments_bytes(bytes, mode, crate::parser::MYSQL_VERSION_ID)
            {
                let segment = segment
                    .map_err(|err| error(crate::error::catalog::ER_PARSE_ERROR, err.to_string()))?;
                let raw = &bytes[segment.range.clone()];
                let introducer = matches!(segment.kind, crate::sql_lex::SqlSegmentKind::Quoted { quote }
                    if quote == b'\'' || (quote == b'"' && !mode.ansi_quotes))
                    && {
                        let token = sql
                            .trim_end()
                            .rsplit(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                            .next()
                            .unwrap_or("");
                        token
                            .strip_prefix('_')
                            .is_some_and(|name| crate::charset::resolve_charset(name).is_ok())
                    };
                if !introducer {
                    if let Ok(text) =
                        crate::charset::decode_bytes(&session.character_set_client, raw)
                    {
                        sql.push_str(&text);
                        continue;
                    }
                }
                match segment.kind {
                    crate::sql_lex::SqlSegmentKind::Quoted { quote }
                        if quote == b'\'' || (quote == b'"' && !mode.ansi_quotes) =>
                    {
                        // The parser accepts UTF-8 source; represent raw literal
                        // octets as a hex literal without transcoding their value.
                        let trimmed = sql.trim_end();
                        if trimmed
                            .as_bytes()
                            .get(trimmed.len().saturating_sub(7)..)
                            .is_some_and(|suffix| suffix.eq_ignore_ascii_case(b"_binary"))
                        {
                            sql.truncate(trimmed.len() - 7);
                        }
                        if introducer && !sql.ends_with(char::is_whitespace) {
                            sql.push(' ');
                        }
                        sql.push_str("X'");
                        let mut pos = 1;
                        while pos + 1 < raw.len() {
                            let mut byte = raw[pos];
                            pos += 1;
                            if byte == quote && raw.get(pos) == Some(&quote) {
                                pos += 1;
                            } else if byte == b'\\'
                                && !mode.no_backslash_escapes
                                && pos + 1 < raw.len()
                            {
                                byte = raw[pos];
                                pos += 1;
                                byte = match byte {
                                    b'0' => 0,
                                    b'n' => b'\n',
                                    b'r' => b'\r',
                                    b't' => b'\t',
                                    b'b' => 8,
                                    b'Z' => 26,
                                    b'%' | b'_' => {
                                        sql.push_str("5c");
                                        byte
                                    }
                                    other => other,
                                };
                            }
                            const HEX: &[u8] = b"0123456789abcdef";
                            sql.push(HEX[(byte >> 4) as usize] as char);
                            sql.push(HEX[(byte & 15) as usize] as char);
                        }
                        sql.push('\'');
                    }
                    crate::sql_lex::SqlSegmentKind::ExecutableComment {
                        active: true,
                        body,
                        ..
                    } => {
                        sql.push_str(&crate::charset::decode_bytes(
                            &session.character_set_client,
                            &bytes[segment.range.start..body.start],
                        )?);
                        sql.push_str(&decode_session_sql(session, &bytes[body.clone()])?);
                        sql.push_str(&crate::charset::decode_bytes(
                            &session.character_set_client,
                            &bytes[body.end..segment.range.end],
                        )?);
                    }
                    _ => {
                        return Err(decoded.err().unwrap_or_else(|| {
                            error(
                                crate::error::catalog::ER_INVALID_CHARACTER_STRING,
                                "Invalid SQL client encoding",
                            )
                        }))
                    }
                }
            }
            if let Ok(original) = decoded {
                crate::parser::preserve_decoded_projection_labels(
                    &original,
                    &mut sql,
                    crate::engine::api::parse_temporal_sql_mode_bits(&session.sql_mode),
                );
            }
            Ok(sql)
        }
    }
}

fn query_join_error(err: crate::util::blocking_pool::BlockingError) -> QueryError {
    crate::mysql::errors::error(
        crate::error::catalog::ER_UNKNOWN_ERROR,
        format!("query execution task failed: {err}"),
    )
}

fn flatten_multi_results(results: Vec<QueryResponse>) -> Vec<QueryResponse> {
    let mut out = Vec::new();
    for result in results {
        match result {
            QueryResponse::MultiResult(nested) => out.extend(flatten_multi_results(nested)),
            other => out.push(other),
        }
    }
    out
}

fn is_ssl_request(payload: &[u8]) -> bool {
    payload.len() == 32
        && u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) & CLIENT_SSL != 0
}

fn validate_post_tls_capabilities(ssl_caps: u32, handshake_caps: u32) -> Result<(), String> {
    // CLIENT_SSL is consumed by the SSLRequest and is not needed after the
    // transport has been upgraded. Connector/J 8.0.16 clears that bit from
    // the encrypted HandshakeResponse41, while current libmysqlclient keeps
    // it set. Both shapes describe the same already-established TLS channel.
    // Keep checking the bits that affect packet parsing or the post-auth wire
    // codec so a real renegotiation mismatch still fails deterministically.
    let stable = CLIENT_PROTOCOL_41
        | CLIENT_COMPRESS
        | CLIENT_ZSTD_COMPRESSION_ALGORITHM
        | CLIENT_PLUGIN_AUTH
        | crate::mysql::protocol::CLIENT_SECURE_CONNECTION;
    let difference = (ssl_caps ^ handshake_caps) & stable;
    if difference == 0 {
        Ok(())
    } else {
        Err(format!(
            "HandshakeResponse capabilities disagree with SSLRequest: SSLRequest=0x{ssl_caps:08x}, HandshakeResponse=0x{handshake_caps:08x}, difference=0x{difference:08x}"
        ))
    }
}

/// Select and enable the wire-compression codec after a successful
/// handshake. Order of preference: `zstd > zlib > plain`.
/// `client_zstd_level` is the level the client advertised in its
/// HandshakeResponse (`HandshakeResponse.zstd_compression_level`);
/// when `None` we fall back to the MySQL default (3). When neither
/// CLIENT_ZSTD_COMPRESSION_ALGORITHM nor CLIENT_COMPRESS was negotiated,
/// or the per-algorithm GUC kill switch is off, the codec stays in
/// plaintext mode — exactly what the existing W2 path did, just routed
/// through one decision point.
fn enable_negotiated_compression(
    codec: &mut crate::mysql::packet::PacketCodec,
    client_capabilities: u32,
    client_zstd_level: Option<u8>,
) -> io::Result<()> {
    if !crate::mysql::packet::compression_protocol_enabled() {
        return Ok(());
    }
    let wants_zstd = client_capabilities & CLIENT_ZSTD_COMPRESSION_ALGORITHM != 0
        && crate::mysql::packet::zstd_compression_enabled();
    let wants_zlib = client_capabilities & CLIENT_COMPRESS != 0
        && crate::mysql::packet::zlib_compression_enabled();
    if wants_zstd {
        let level = client_zstd_level
            .map(|n| n as i32)
            .unwrap_or_else(crate::mysql::packet::default_zstd_level);
        codec.enable_zstd_compression(level)?;
    } else if wants_zlib {
        codec.enable_zlib_compression();
    }
    Ok(())
}

fn result_row_start_sequence_id(
    columns: &[Column],
    client_capabilities: u32,
    metadata_full: bool,
    first_sequence_id: u8,
) -> u8 {
    if client_capabilities & CLIENT_OPTIONAL_RESULTSET_METADATA != 0 && !metadata_full {
        return first_sequence_id.wrapping_add(1);
    }
    let eof_count = if client_capabilities & CLIENT_DEPRECATE_EOF != 0 {
        0
    } else {
        1
    };
    first_sequence_id
        .wrapping_add(1)
        .wrapping_add(columns.len() as u8)
        .wrapping_add(eof_count)
}

fn stmt_execute_cursor_mode(flags: u8) -> Result<bool, QueryError> {
    const CURSOR_TYPE_NO_CURSOR: u8 = 0x00;
    const CURSOR_TYPE_READ_ONLY: u8 = 0x01;
    const CURSOR_TYPE_FOR_UPDATE: u8 = 0x02;
    const CURSOR_TYPE_SCROLLABLE: u8 = 0x04;
    const PARAMETER_COUNT_AVAILABLE: u8 = 0x08;
    let unsupported = flags & !(CURSOR_TYPE_READ_ONLY | PARAMETER_COUNT_AVAILABLE);
    if unsupported != 0 {
        return Err(crate::mysql::errors::unsupported(format!(
            "unsupported COM_STMT_EXECUTE flags: 0x{flags:02x}"
        )));
    }
    match flags & !PARAMETER_COUNT_AVAILABLE {
        CURSOR_TYPE_NO_CURSOR => Ok(false),
        CURSOR_TYPE_READ_ONLY => Ok(true),
        CURSOR_TYPE_FOR_UPDATE | CURSOR_TYPE_SCROLLABLE => Err(crate::mysql::errors::unsupported(
            "COM_STMT_EXECUTE cursor type other than READ_ONLY",
        )),
        other if other & !CURSOR_TYPE_READ_ONLY == 0 => Ok(true),
        other => Err(crate::mysql::errors::unsupported(format!(
            "unsupported COM_STMT_EXECUTE flags: 0x{other:02x}"
        ))),
    }
}

#[inline]
fn command_statement_id(payload: &[u8]) -> Option<u32> {
    let bytes: [u8; 4] = payload.get(1..5)?.try_into().ok()?;
    Some(u32::from_le_bytes(bytes))
}

fn append_prepared_long_data(
    entry: &mut PreparedStatement,
    param_id: usize,
    incoming: &[u8],
) -> Result<(), QueryError> {
    let (old_capacity, old_len) = entry.long_data[param_id]
        .as_ref()
        .map(|slot| (slot.capacity(), slot.len()))
        .unwrap_or((0, 0));
    let required = old_len.saturating_add(incoming.len());
    let combined = entry
        .long_data
        .iter()
        .enumerate()
        .fold(required, |sum, (idx, value)| {
            if idx == param_id {
                sum
            } else {
                sum.saturating_add(value.as_ref().map_or(0, Vec::len))
            }
        });
    let packet_limit = crate::mysql::packet::max_allowed_packet_bytes();
    if combined > packet_limit {
        let err = error(
            crate::error::catalog::ER_NET_PACKET_TOO_LARGE,
            format!(
                "Prepared long data {combined} bytes exceeds max_allowed_packet {packet_limit}"
            ),
        );
        return Err(err);
    }
    let minimum_growth = required.saturating_sub(old_capacity);
    if let Err(err) = entry.memory_charge.try_grow(minimum_growth) {
        return Err(err);
    }
    let slot = entry.long_data[param_id].get_or_insert_with(Vec::new);
    if slot.try_reserve_exact(required - old_len).is_err() {
        entry.memory_charge.shrink(minimum_growth);
        let err = error(
            crate::error::catalog::ER_OUTOFMEMORY,
            "out of memory while buffering COM_STMT_SEND_LONG_DATA",
        );
        return Err(err);
    }
    let actual_growth = slot.capacity().saturating_sub(old_capacity);
    if actual_growth > minimum_growth {
        let extra = actual_growth - minimum_growth;
        if let Err(err) = entry.memory_charge.try_grow(extra) {
            entry.long_data[param_id] = None;
            entry
                .memory_charge
                .shrink(old_capacity.saturating_add(minimum_growth));
            return Err(err);
        }
    }
    entry.long_data[param_id]
        .as_mut()
        .expect("long-data slot was initialized")
        .extend_from_slice(incoming);
    Ok(())
}

fn clear_prepared_long_data(statement: &mut PreparedStatement) {
    let released = statement
        .long_data
        .iter()
        .filter_map(Option::as_ref)
        .map(Vec::capacity)
        .sum();
    for item in &mut statement.long_data {
        *item = None;
    }
    statement.memory_charge.shrink(released);
}

async fn write_result_set_header_batched<W>(
    batch: &mut PacketBatchWriter<'_, W>,
    columns: &[Column],
    client_capabilities: u32,
    metadata_full: bool,
    fallback_collation: u16,
    server_status: u16,
    first_sequence_id: u8,
) -> io::Result<u8>
where
    W: AsyncWrite + Unpin,
{
    // CLIENT_OPTIONAL_RESULTSET_METADATA: the column-count packet is
    // prefixed with a `metadata_follows` byte (1 = FULL, 0 = NONE).
    // mydb always sends FULL — emitting NONE requires the client to
    // already hold matching cached metadata, which we have no SQL
    // surface to opt into (`SET resultset_metadata=NONE` is the only
    // toggle and accepting it without honouring would violate the
    // protocol). Default FULL is the wire-compliant baseline that
    // Connector/J 8.0+ and mysql 8.4 CLI expect when they advertise
    // the bit at handshake.
    let mut column_count = Vec::new();
    let send_metadata =
        client_capabilities & CLIENT_OPTIONAL_RESULTSET_METADATA == 0 || metadata_full;
    if client_capabilities & CLIENT_OPTIONAL_RESULTSET_METADATA != 0 {
        column_count.push(if send_metadata {
            crate::mysql::protocol::RESULTSET_METADATA_FULL
        } else {
            crate::mysql::protocol::RESULTSET_METADATA_NONE
        });
    }
    write_lenenc_int(&mut column_count, columns.len() as u64);
    batch.write_packet(first_sequence_id, &column_count).await?;

    let mut sequence_id = first_sequence_id.wrapping_add(1);
    // Reuse one buffer across all columns — the `_into` variant clears it
    // per column, so a wide result set no longer allocates one `Vec` per
    // column-definition packet on every query.
    let mut col_def = Vec::new();
    for column in if send_metadata { columns } else { &[] } {
        column_definition_packet_full_into_with_fallback(
            &mut col_def,
            &column.name,
            column.column_type,
            &column.source,
            fallback_collation,
        );
        batch.write_packet(sequence_id, &col_def).await?;
        sequence_id = sequence_id.wrapping_add(1);
    }

    if send_metadata && client_capabilities & CLIENT_DEPRECATE_EOF == 0 {
        let eof = eof_packet_with_status(server_status);
        batch.write_packet(sequence_id, &eof).await?;
    }
    Ok(result_row_start_sequence_id(
        columns,
        client_capabilities,
        metadata_full,
        first_sequence_id,
    ))
}

async fn write_cached_result_set_header_batched<W>(
    batch: &mut PacketBatchWriter<'_, W>,
    metadata: &PreparedResultMetadata,
    client_capabilities: u32,
    server_status: u16,
    first_sequence_id: u8,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    // Mirror `write_result_set_header_batched`'s metadata_follows byte.
    // The cached `column_count_packet` is the bare lenenc int; we
    // prepend the byte at write time rather than caching two variants,
    // because flipping CLIENT_OPTIONAL_RESULTSET_METADATA is a
    // per-connection negotiation and the prepared metadata is shared
    // across sessions.
    let column_count_with_flag: Vec<u8>;
    let column_count_bytes: &[u8] = if client_capabilities & CLIENT_OPTIONAL_RESULTSET_METADATA != 0
    {
        column_count_with_flag = std::iter::once(crate::mysql::protocol::RESULTSET_METADATA_FULL)
            .chain(metadata.column_count_packet.iter().copied())
            .collect();
        &column_count_with_flag
    } else {
        &metadata.column_count_packet
    };
    batch
        .write_packet(first_sequence_id, column_count_bytes)
        .await?;

    let mut sequence_id = first_sequence_id.wrapping_add(1);
    for packet in metadata.column_definitions.iter() {
        batch.write_packet(sequence_id, packet).await?;
        sequence_id = sequence_id.wrapping_add(1);
    }

    if client_capabilities & CLIENT_DEPRECATE_EOF == 0 {
        let eof = eof_packet_with_status(server_status);
        batch.write_packet(sequence_id, &eof).await?;
    }
    Ok(())
}

async fn write_cached_result_set_header_metadata_none_batched<W>(
    batch: &mut PacketBatchWriter<'_, W>,
    column_count: usize,
    first_sequence_id: u8,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut payload = Vec::with_capacity(10);
    payload.push(crate::mysql::protocol::RESULTSET_METADATA_NONE);
    write_lenenc_int(&mut payload, column_count as u64);
    batch.write_packet(first_sequence_id, &payload).await
}

async fn write_result_set_terminator_batched<W>(
    batch: &mut PacketBatchWriter<'_, W>,
    sequence_id: u8,
    client_capabilities: u32,
    server_status: u16,
    warnings: u16,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    if client_capabilities & CLIENT_DEPRECATE_EOF != 0 {
        let payload = eof_as_ok_packet_with_status_warnings(server_status, warnings);
        return batch.write_packet(sequence_id, &payload).await;
    }
    let payload = eof_packet_with_status_warnings(server_status, warnings);
    batch.write_packet(sequence_id, &payload).await
}

/// Both pull strategies must observe cancellation at EOF before publishing
/// found_rows or draining worker warnings.
struct StreamCompletion {
    sequence_id: u8,
    client_capabilities: u32,
    server_status: u16,
    found_rows: Option<u64>,
    sent_rows: u64,
}

impl StreamCompletion {
    async fn finish<W: AsyncWrite + Unpin>(
        self,
        mut batch: PacketBatchWriter<'_, W>,
        context: &mut SessionContext,
        progress: &mut Option<TextBatchProgress>,
        cursor: &mut dyn crate::engine::api::ResultCursor,
    ) -> io::Result<u8> {
        if let Some(err) = context.cancelled_error() {
            return finish_failed_stream(batch, self.sequence_id, &err, progress).await;
        }
        context.found_rows = self.found_rows.unwrap_or(self.sent_rows);
        context.warnings.append(&mut cursor.take_warnings());
        write_result_set_terminator_batched(
            &mut batch,
            self.sequence_id,
            self.client_capabilities,
            self.server_status,
            context.warning_count(),
        )
        .await?;
        batch.finish().await?;
        Ok(self.sequence_id.wrapping_add(1))
    }
}

async fn finish_failed_stream<W: AsyncWrite + Unpin>(
    batch: PacketBatchWriter<'_, W>,
    sequence_id: u8,
    err: &QueryError,
    progress: &mut Option<TextBatchProgress>,
) -> io::Result<u8> {
    if let Some(progress) = progress {
        progress.failed = true;
    }
    finish_error_result_set(batch, sequence_id, err).await?;
    Ok(sequence_id.wrapping_add(1))
}

/// How to encode a streaming result row off the async worker. `Copy` so
/// it can be captured verbatim by each per-batch blocking closure.
#[derive(Clone, Copy)]
enum StreamRowEncoding {
    /// Text protocol; carries the once-resolved result charset descriptor.
    Text(Option<&'static crate::charset::CharsetInfo>),
    /// Text protocol with coordinate-list VECTOR results.
    VectorText(Option<&'static crate::charset::CharsetInfo>),
    /// Binary (prepared-statement) protocol.
    Binary(Option<&'static crate::charset::CharsetInfo>),
}

impl StreamRowEncoding {
    fn with_vector_text(self, enabled: bool) -> Self {
        match self {
            Self::Text(charset) if enabled => Self::VectorText(charset),
            _ => self,
        }
    }
}

struct ReusableByteScratch<'a> {
    slot: &'a mut Vec<u8>,
    value: Vec<u8>,
}

impl<'a> ReusableByteScratch<'a> {
    fn new(slot: &'a mut Vec<u8>, value: Vec<u8>) -> Self {
        Self { slot, value }
    }

    fn as_ref(&self) -> &[u8] {
        &self.value
    }

    fn as_mut(&mut self) -> &mut Vec<u8> {
        &mut self.value
    }
}

impl Drop for ReusableByteScratch<'_> {
    fn drop(&mut self) {
        self.value.clear();
        *self.slot = std::mem::take(&mut self.value);
    }
}

fn ensure_governed_scratch_capacity(
    scratch: &mut Vec<u8>,
    charge: &mut crate::engine::executor::mem_budget::ControlledMemoryCharge,
    needed: usize,
) -> Result<(), QueryError> {
    let old_capacity = scratch.capacity();
    if old_capacity >= needed {
        return Ok(());
    }
    let reserved_growth = needed.saturating_sub(old_capacity);
    charge.try_grow(reserved_growth)?;
    if scratch
        .try_reserve_exact(needed.saturating_sub(scratch.len()))
        .is_err()
    {
        charge.shrink(reserved_growth);
        return Err(error(
            crate::error::catalog::ER_OUTOFMEMORY,
            "out of memory while growing MySQL binary-row scratch",
        ));
    }
    let actual_growth = scratch.capacity().saturating_sub(old_capacity);
    if actual_growth > reserved_growth {
        if let Err(err) = charge.try_grow(actual_growth - reserved_growth) {
            *scratch = Vec::new();
            charge.shrink(old_capacity.saturating_add(reserved_growth));
            return Err(err);
        }
    } else if reserved_growth > actual_growth {
        charge.shrink(reserved_growth - actual_growth);
    }
    Ok(())
}

fn estimate_stream_row_bytes(
    row: &[crate::types::Value],
    columns: &[Column],
    encoding: StreamRowEncoding,
) -> usize {
    match encoding {
        StreamRowEncoding::Text(_) => crate::mysql::protocol::estimate_text_row_packet_bytes(row),
        StreamRowEncoding::VectorText(charset) => {
            crate::mysql::protocol::estimate_vector_text_row_packet_bytes(
                row,
                charset,
                Some(columns),
            )
        }
        StreamRowEncoding::Binary(_) => {
            crate::mysql::protocol::estimate_binary_row_packet_bytes(row)
        }
    }
}

/// Encode one streaming result row into `out`, applying ZEROFILL padding
/// first when the result set declares any zerofill column. Shared by the
/// text and binary off-worker streaming writers so the two paths stay
/// byte-identical to their legacy inline loops.
fn encode_stream_row(
    row: &[crate::types::Value],
    columns: &[Column],
    zerofill: Option<&[u16]>,
    encoding: StreamRowEncoding,
    out: &mut Vec<u8>,
) {
    let padded;
    let row: &[crate::types::Value] = match zerofill {
        Some(spec) => {
            padded = crate::mysql::protocol::zerofill_padded_row(row, spec);
            &padded
        }
        None => row,
    };
    match encoding {
        StreamRowEncoding::Text(charset) => {
            crate::mysql::protocol::text_row_packet_into_with_columns_info(
                row,
                out,
                charset,
                Some(columns),
            );
        }
        StreamRowEncoding::VectorText(charset) => {
            crate::mysql::protocol::vector_text_row_packet_into(row, out, charset, columns);
        }
        StreamRowEncoding::Binary(charset) => {
            crate::mysql::protocol::binary_row_packet_into_with_charset(columns, row, out, charset);
        }
    }
}

/// Whether streaming result sets pull + encode rows in the blocking pool
/// (default) instead of running the blocking cursor `next_row` directly on
/// the async worker. `MYDB_STREAM_BLOCKING_OFFLOAD=0` forces the legacy
/// inline loop, kept as a differential oracle.
fn stream_blocking_offload_enabled() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        !matches!(
            std::env::var("MYDB_STREAM_BLOCKING_OFFLOAD")
                .ok()
                .as_deref(),
            Some("0") | Some("off") | Some("false") | Some("no")
        )
    })
}

/// Rows pulled + encoded per blocking-pool round on the streaming path.
/// Larger batches amortise the pool hand-off; smaller batches bound how
/// long a KILL waits before the write side notices. Default 256, tunable
/// via `MYDB_STREAM_OFFLOAD_BATCH_ROWS` (clamped to >= 1).
fn stream_offload_batch_rows() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MYDB_STREAM_OFFLOAD_BATCH_ROWS")
            .ok()
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .filter(|n| *n >= 1)
            .unwrap_or(256)
    })
}

/// Encoded bytes retained by one blocking-pool result hand-off. The limit is
/// deliberately independent from the row-count knob: one wide LOB row is
/// always allowed, then the batch is yielded immediately.
fn stream_offload_batch_bytes() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MYDB_STREAM_OFFLOAD_BATCH_BYTES")
            .ok()
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .filter(|n| *n >= 64 * 1024)
            .unwrap_or(8 * 1024 * 1024)
    })
}

/// Finish the current protocol frame after KILL QUERY, then let the caller
/// emit ERR at the next row boundary. Dropping a partial write corrupts the
/// protocol (including compressed frames). Only KILL CONNECTION may do so.
async fn write_cancellable_stream_packet<W: AsyncWrite + Unpin>(
    batch: &mut PacketBatchWriter<'_, W>,
    sequence_id: u8,
    payload: &[u8],
    notify: &SessionHandle,
) -> io::Result<u8> {
    let write = batch.write_packet_seq(sequence_id, payload);
    tokio::pin!(write);
    loop {
        if notify.kill_flag().load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "connection killed during result write",
            ));
        }
        tokio::select! {
            biased;
            res = &mut write => return res,
            _ = notify.notified() => {},
        }
    }
}

/// Terminate a result set that failed mid-stream. MySQL replaces the next row
/// (or the closing EOF/OK) with an ERR packet carrying the query error. This
/// covers both cancellation and storage/executor cursor errors. It is called at a row
/// boundary — where the batch buffer holds only whole frames — so
/// flushing the buffered rows and appending the ERR yields a well-framed,
/// in-sequence response the client can parse, instead of the historical
/// behaviour of dropping the `PacketBatchWriter` unfinished (silently
/// discarding buffered bytes and desyncing the connection: the next
/// command's reply was misread as result-set continuation, or — for the
/// materialized paths that had not flushed the header yet — the client
/// got nothing and hung until `wait_timeout`). Consumes `batch`; its
/// `finish` flushes to the socket.
async fn finish_error_result_set<W>(
    mut batch: PacketBatchWriter<'_, W>,
    sequence_id: u8,
    err: &QueryError,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let payload = crate::mysql::protocol::err_packet(err);
    batch.write_packet(sequence_id, &payload).await?;
    batch.finish().await
}

#[cfg(test)]
fn parse_prepared_statement_template(sql: &str, param_count: usize) -> Option<Statement> {
    let mut statement = parse_statement(sql).ok()?;
    let mut next = 0;
    number_statement_placeholders(&mut statement, &mut next);
    (next == param_count).then_some(statement)
}

#[cfg(test)]
fn bind_prepared_statement(
    template: &Statement,
    params: &[StmtParamValue],
) -> Result<Statement, String> {
    let mut statement = template.clone();
    bind_statement_placeholders(&mut statement, params)?;
    Ok(statement)
}

// The `bind_*_placeholders` family eagerly substitutes `Expr::Placeholder`
// nodes with `Expr::Literal(value)` by mutating the AST. Phase 2 of the
// caching design replaced this with late binding via the thread-local
// `params` context, so production paths no longer call any of these
// helpers. They remain compiled in test builds because the unit tests
// for the prepared-statement protocol layer still exercise them
// (`parse_prepared_statement_template` + `bind_prepared_statement`).
#[cfg(test)]
fn bind_statement_placeholders<'q>(
    statement: impl Into<crate::parser::QueryMut<'q>>,
    params: &[StmtParamValue],
) -> Result<(), String> {
    let statement = statement.into();
    match statement {
        crate::parser::QueryMut::Other(Statement::Insert(stmt)) => {
            bind_insert_placeholders(stmt, params)
        }
        crate::parser::QueryMut::Select(stmt) => bind_select_placeholders(stmt, params),
        crate::parser::QueryMut::Other(Statement::Explain { target, .. }) => {
            bind_statement_placeholders(target, params)
        }
        crate::parser::QueryMut::Other(Statement::Update(stmt)) => {
            bind_update_placeholders(stmt, params)
        }
        crate::parser::QueryMut::Other(Statement::Delete(stmt)) => {
            bind_delete_placeholders(stmt, params)
        }
        crate::parser::QueryMut::Rrf(rrf) => {
            for branch in &mut rrf.branches {
                bind_statement_placeholders(&mut branch.query, params)?;
            }
            bind_expr_placeholders(&mut rrf.rank_constant, params)?;
            for weight in &mut rrf.weights {
                bind_expr_placeholders(weight, params)?;
            }
            Ok(())
        }
        crate::parser::QueryMut::SetOperation(stmt) => {
            bind_statement_placeholders(&mut stmt.left, params)?;
            bind_statement_placeholders(&mut stmt.right, params)?;
            for OrderBy { expr, .. } in &mut stmt.order_by {
                bind_expr_placeholders(expr, params)?;
            }
            if let Some(limit) = &mut stmt.limit {
                bind_limit_placeholder(limit, params)?;
            }
            if let Some(offset) = &mut stmt.offset {
                bind_limit_placeholder(offset, params)?;
            }
            Ok(())
        }
        crate::parser::QueryMut::Other(Statement::CreateTableAsSelect { query, .. }) => {
            bind_statement_placeholders(query, params)
        }
        crate::parser::QueryMut::WithCte(mut with) => {
            for cte in with.ctes.iter_mut() {
                bind_statement_placeholders(&mut cte.query, params)?;
            }
            bind_statement_placeholders(&mut with.body, params)
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
fn bind_insert_placeholders(stmt: &mut Insert, params: &[StmtParamValue]) -> Result<(), String> {
    match &mut stmt.source {
        InsertSource::Values(rows) => {
            for row in rows {
                for expr in row {
                    bind_expr_placeholders(expr, params)?;
                }
            }
        }
        InsertSource::Query(query) => bind_statement_placeholders(query, params)?,
    }
    if let Some(assignments) = &mut stmt.on_duplicate_key_update {
        for Assignment { value, .. } in assignments {
            bind_expr_placeholders(value, params)?;
        }
    }
    bind_returning_placeholders(&mut stmt.returning, params)?;
    Ok(())
}

#[cfg(test)]
fn bind_update_placeholders(stmt: &mut Update, params: &[StmtParamValue]) -> Result<(), String> {
    for Assignment { value, .. } in &mut stmt.assignments {
        bind_expr_placeholders(value, params)?;
    }
    if let Some(selection) = &mut stmt.selection {
        bind_expr_placeholders(selection, params)?;
    }
    for OrderBy { expr, .. } in &mut stmt.order_by {
        bind_expr_placeholders(expr, params)?;
    }
    if let Some(limit) = &mut stmt.limit {
        bind_limit_placeholder(limit, params)?;
    }
    bind_returning_placeholders(&mut stmt.returning, params)?;
    Ok(())
}

#[cfg(test)]
fn bind_delete_placeholders(stmt: &mut Delete, params: &[StmtParamValue]) -> Result<(), String> {
    if let Some(selection) = &mut stmt.selection {
        bind_expr_placeholders(selection, params)?;
    }
    for OrderBy { expr, .. } in &mut stmt.order_by {
        bind_expr_placeholders(expr, params)?;
    }
    if let Some(limit) = &mut stmt.limit {
        bind_limit_placeholder(limit, params)?;
    }
    bind_returning_placeholders(&mut stmt.returning, params)?;
    Ok(())
}

#[cfg(test)]
fn bind_returning_placeholders(
    returning: &mut Option<crate::parser::ReturningClause>,
    params: &[StmtParamValue],
) -> Result<(), String> {
    if let Some(returning) = returning {
        for item in &mut returning.projection {
            if let crate::parser::SelectItem::Expr { expr, .. } = item {
                bind_expr_placeholders(expr, params)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
fn bind_select_placeholders(stmt: &mut Select, params: &[StmtParamValue]) -> Result<(), String> {
    for item in &mut stmt.projection {
        if let SelectItem::Expr { expr, .. } = item {
            bind_expr_placeholders(expr, params)?;
        }
    }
    if let Some(derived) = &mut stmt.derived {
        match derived {
            SubQuery::Select(s) => bind_select_placeholders(s, params)?,
            stmt @ (SubQuery::SetOp(_) | SubQuery::Rrf(_) | SubQuery::WithCte(_)) => {
                bind_statement_placeholders(stmt, params)?
            }
        }
    }
    for join in &mut stmt.joins {
        if let Some(condition) = &mut join.condition {
            bind_expr_placeholders(condition, params)?;
        }
    }
    if let Some(selection) = &mut stmt.selection {
        bind_expr_placeholders(selection, params)?;
    }
    for expr in &mut stmt.group_by {
        bind_expr_placeholders(expr, params)?;
    }
    if let Some(having) = &mut stmt.having {
        bind_expr_placeholders(having, params)?;
    }
    for OrderBy { expr, .. } in &mut stmt.order_by {
        bind_expr_placeholders(expr, params)?;
    }
    if let Some(limit) = &mut stmt.limit {
        bind_limit_placeholder(limit, params)?;
    }
    if let Some(offset) = &mut stmt.offset {
        bind_limit_placeholder(offset, params)?;
    }
    Ok(())
}

#[cfg(test)]
fn bind_limit_placeholder(limit: &mut LimitValue, params: &[StmtParamValue]) -> Result<(), String> {
    let (LimitValue::Placeholder(idx) | LimitValue::OffsetCommaPlaceholder(idx)) = limit else {
        return Ok(());
    };
    let value = params
        .get(*idx)
        .ok_or_else(|| "not enough prepared statement parameters".to_string())?;
    let rows = stmt_param_to_limit(value)?;
    *limit = LimitValue::Rows(rows);
    Ok(())
}

#[cfg(test)]
fn bind_expr_placeholders(expr: &mut Expr, params: &[StmtParamValue]) -> Result<(), String> {
    struct Binder<'a> {
        params: &'a [StmtParamValue],
        result: Result<(), String>,
    }
    impl crate::parser::visit::AstVisitorMut for Binder<'_> {
        fn enter_expr(&mut self, expr: &mut Expr) {
            if self.result.is_err() {
                return;
            }
            if let Expr::Placeholder(idx) = expr {
                self.result = self
                    .params
                    .get(*idx)
                    .ok_or_else(|| "not enough prepared statement parameters".to_string())
                    .and_then(stmt_param_to_expr)
                    .map(|bound| *expr = bound);
            }
        }
        fn enter_limit(&mut self, limit: &mut LimitValue) {
            if self.result.is_ok() {
                self.result = bind_limit_placeholder(limit, self.params);
            }
        }
        fn walk_expr_children(&self, _: &Expr) -> bool {
            self.result.is_ok()
        }
        fn walk_select_children(&self, _: &Select) -> bool {
            self.result.is_ok()
        }
    }
    let mut binder = Binder {
        params,
        result: Ok(()),
    };
    crate::parser::visit::visit_expr_mut(&mut binder, expr);
    binder.result
}

#[cfg(test)]
fn stmt_param_to_expr(value: &StmtParamValue) -> Result<Expr, String> {
    let literal = match value {
        StmtParamValue::Null => Literal::Null,
        StmtParamValue::Int(value) => Literal::Int(*value),
        StmtParamValue::UInt(value) => Literal::UInt(*value),
        StmtParamValue::Float(value) => Literal::Float(*value as f64),
        StmtParamValue::Double(value) => Literal::Float(*value),
        StmtParamValue::Bytes(value) => {
            Literal::String(String::from_utf8_lossy(value).into_owned())
        }
        StmtParamValue::String(value) => Literal::String(value.clone()),
        StmtParamValue::Decimal { magnitude, scale } => {
            let v = crate::types::Value::DecimalCompact(*magnitude, *scale);
            Literal::String(v.to_display_string())
        }
    };
    Ok(Expr::Literal(literal))
}

/// Convert a wire-protocol `StmtParamValue` to a runtime `Value`. Used by
/// the late-binding EXECUTE path: instead of substituting `Expr::Placeholder`
/// with `Expr::Literal` in a clone of the AST, we collect parameters into
/// `Vec<Value>` and install them in the thread-local param context for the
/// duration of the execution.
///
/// `Bytes` is decoded as a UTF-8 `String` to match the legacy
/// (Phase-1 `bind_statement_placeholders` → `stmt_param_to_expr`) path.
/// MySQL's binary protocol encodes VARCHAR / VAR_STRING / STRING / BLOB
/// all as length-encoded bytes, so the wire decoder can't tell apart a
/// text column from a blob — INSERT / UPDATE go through `coerce_row`,
/// which collapses both back into a `Value::String` for VARCHAR/CHAR and
/// `Value::Bytes` for BLOB. SELECT placeholder evaluation, however,
/// short-circuits straight into `KeyPart::from(Value)` for index lookup.
/// Returning `Value::Bytes` here would yield `KeyPart::Bytes(...)` while
/// the index entries created by INSERT (`Value::String` → coerced →
/// `KeyPart::String(...)`) sit under a different enum variant, and
/// `lookup_index_visible` would silently miss every row. Surfaced as
/// "Customer not found" in TPC-C OrderStatus by C_LAST. Standardising
/// on `Value::String` keeps the comparison space aligned with INSERT.
#[cfg(test)]
pub(crate) fn stmt_param_to_value_for_test(value: &StmtParamValue) -> crate::types::Value {
    stmt_param_to_value(value, crate::charset::DEFAULT_CHARSET).unwrap()
}

fn stmt_param_to_value(
    value: &StmtParamValue,
    connection_charset: &str,
) -> Result<crate::types::Value, QueryError> {
    use crate::types::Value;
    Ok(match value {
        StmtParamValue::Null => Value::Null,
        StmtParamValue::Int(v) => Value::Int(*v),
        StmtParamValue::UInt(v) => Value::UInt(*v),
        StmtParamValue::Float(v) => Value::Float(*v),
        StmtParamValue::Double(v) => Value::Double(*v),
        StmtParamValue::Bytes(v) => {
            // Binary-protocol byte params are normally text in the connection
            // charset. Raw binary (e.g. a `VECTOR` sent as little-endian f32
            // bytes, or any BLOB) is not valid in a text charset like
            // utf8mb4 and used to fail with 1366 here; keep it as raw
            // `Value::Bytes` instead so column coercion can interpret it
            // (a VECTOR column does a validated memcpy with zero text
            // parsing — the fast binary ingestion path). Valid text still
            // decodes to `Value::String` exactly as before, so no previously
            // succeeding path changes behaviour.
            match crate::charset::decode_bytes(connection_charset, v) {
                Ok(s) => Value::String(s),
                Err(_) => Value::Bytes(v.clone()),
            }
        }
        StmtParamValue::String(v) => Value::String(v.clone()),
        // NEWDECIMAL params arrive pre-parsed so arithmetic /
        // coercion / codec see them in binary form end-to-end.
        StmtParamValue::Decimal { magnitude, scale } => Value::DecimalCompact(*magnitude, *scale),
    })
}

#[cfg(test)]
fn stmt_param_to_limit(value: &StmtParamValue) -> Result<usize, String> {
    match value {
        StmtParamValue::Int(value) if *value >= 0 => {
            usize::try_from(*value).map_err(|_| "LIMIT parameter is out of range".to_string())
        }
        StmtParamValue::UInt(value) => {
            usize::try_from(*value).map_err(|_| "LIMIT parameter is out of range".to_string())
        }
        StmtParamValue::String(value) => value
            .parse::<usize>()
            .map_err(|_| "LIMIT parameter must be a non-negative integer".to_string()),
        StmtParamValue::Bytes(value) => String::from_utf8_lossy(value)
            .parse::<usize>()
            .map_err(|_| "LIMIT parameter must be a non-negative integer".to_string()),
        _ => Err("LIMIT parameter must be a non-negative integer".to_string()),
    }
}

/// Plain TCP stream is wrapped in a `BufReader` so MySQL packet
/// reads (4-byte header + N-byte payload, two `read_exact` calls per
/// packet) coalesce into a single `read()` syscall. The 8 KiB default
/// buffer comfortably holds an entire `COM_QUERY` / `COM_STMT_EXECUTE`
/// for the OLTP-style traffic this matters for. Writes pass through
/// to the underlying `TcpStream` unchanged; `BufReader` does not add
/// write buffering.
///
/// On a TLS upgrade we hand the `BufReader<TcpStream>` directly to
/// `tokio_rustls::TlsAcceptor::accept`. This is critical: the MySQL
/// client sends the SSLRequest packet immediately followed by the
/// TLS ClientHello, often inside a single TCP segment, so by the
/// time we read the SSLRequest the `BufReader` has typically already
/// pulled the ClientHello bytes into its internal buffer. Letting the
/// TLS layer read *through* that same `BufReader` preserves those
/// bytes; calling `into_inner()` would discard them and the TLS
/// handshake would silently stall.
type PlainStream = BufReader<TcpStream>;
type SecureStream = TlsStream<PlainStream>;

enum MysqlStream {
    Plain(PlainStream),
    Tls(Box<SecureStream>),
    Upgrading,
}

impl MysqlStream {
    /// A separate reactor registration observes EOF without reading protocol
    /// bytes, including pipelined requests or TLS records buffered by the codec.
    fn clone_socket(&self) -> io::Result<TcpStream> {
        let stream = match self {
            Self::Plain(stream) => stream.get_ref(),
            Self::Tls(stream) => stream.get_ref().0.get_ref(),
            Self::Upgrading => {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "TLS upgrade in progress",
                ))
            }
        };
        #[cfg(unix)]
        let socket = {
            use std::os::fd::AsFd;
            std::net::TcpStream::from(stream.as_fd().try_clone_to_owned()?)
        };
        #[cfg(windows)]
        let socket = {
            use std::os::windows::io::AsSocket;
            std::net::TcpStream::from(stream.as_socket().try_clone_to_owned()?)
        };
        TcpStream::from_std(socket)
    }
}

async fn wait_peer_closed(stream: &TcpStream) {
    let mut byte = [0u8; 1];
    loop {
        match stream.ready(tokio::io::Interest::READABLE).await {
            Ok(ready) if ready.is_read_closed() || ready.is_error() => return,
            Err(_) => return,
            _ => {}
        }
        match stream.peek(&mut byte).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {
                // Never consume another command. A pipelined request may stay
                // unread while a worker runs, so avoid spinning on readiness.
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }
    }
}

impl AsyncRead for MysqlStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MysqlStream::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            MysqlStream::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
            MysqlStream::Upgrading => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "connection is upgrading to TLS",
            ))),
        }
    }
}

impl AsyncWrite for MysqlStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match self.get_mut() {
            MysqlStream::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            MysqlStream::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
            MysqlStream::Upgrading => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "connection is upgrading to TLS",
            ))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.get_mut() {
            MysqlStream::Plain(stream) => Pin::new(stream).poll_flush(cx),
            MysqlStream::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
            MysqlStream::Upgrading => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "connection is upgrading to TLS",
            ))),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.get_mut() {
            MysqlStream::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            MysqlStream::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
            MysqlStream::Upgrading => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "connection is upgrading to TLS",
            ))),
        }
    }
}

fn parse_process_kill_connection_id(payload: &[u8]) -> Option<u32> {
    let bytes = payload.get(..4)?;
    Some(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

/// Return the ordinary, whitespace-leading first keyword without invoking
/// the full MySQL comment/version-comment lexer. `None` asks callers to keep
/// their lexical oracle for comment-leading or otherwise unusual input.
#[inline]
fn simple_first_keyword(sql: &str) -> Option<&[u8]> {
    let bytes = sql.trim_start().as_bytes();
    if !bytes.first().is_some_and(u8::is_ascii_alphabetic) {
        return None;
    }
    let end = bytes
        .iter()
        .position(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_')
        .unwrap_or(bytes.len());
    Some(&bytes[..end])
}

/// Cheap byte-level sniff that says: this SQL *might* be a lock-safe
/// read-only SELECT, so it's worth a single `parse_statement` call to
/// confirm. Designed to be conservative and zero-allocation: a "no"
/// here always falls through to the existing `spawn_blocking` path,
/// preserving every behaviour the slow path already supports
/// (LOAD DATA, multi-statement, SHOW compatibility handlers, fast-insert,
/// lock-wait dispatch, txn control). False positives only cost one parser-
/// cache lookup (and a parse on cold miss).
///
/// We require:
///   - leading whitespace and SQL comments stripped
///   - first keyword is `SELECT`
///   - the next char is whitespace (so `SELECTOR`, etc. don't match)
/// Cheap byte-level prefix sniff for `LOAD DATA LOCAL …`. Returns true
/// when the next non-whitespace / non-comment tokens are
/// `LOAD` `DATA` … `LOCAL`, with no required ordering between the LOCAL
/// modifier and the optional LOW_PRIORITY/CONCURRENT modifiers (i.e.
/// `LOAD DATA LOW_PRIORITY LOCAL …` and `LOAD DATA LOCAL INFILE …`
/// both pass). False negatives are safe: we'll just skip the LOCAL fast
/// path and let the engine handle a server-side LOAD DATA normally.
fn looks_like_load_data_local(sql: &str, mode: crate::sql_lex::SqlLexMode) -> bool {
    if simple_first_keyword(sql).is_some_and(|word| !word.eq_ignore_ascii_case(b"LOAD")) {
        return false;
    }
    let Ok(words) =
        crate::sql_lex::leading_keyword_ranges(sql, mode, crate::parser::MYSQL_VERSION_ID, 6)
    else {
        return false;
    };
    let word = |index: usize| words.get(index).map(|range| &sql.as_bytes()[range.clone()]);
    if !word(0).is_some_and(|w| w.eq_ignore_ascii_case(b"LOAD"))
        || !word(1)
            .is_some_and(|w| w.eq_ignore_ascii_case(b"DATA") || w.eq_ignore_ascii_case(b"XML"))
    {
        return false;
    }
    for range in words.iter().skip(2) {
        let word = &sql.as_bytes()[range.clone()];
        if word.eq_ignore_ascii_case(b"INFILE") {
            return false;
        }
        if word.eq_ignore_ascii_case(b"LOCAL") {
            return true;
        }
    }
    false
}

/// Cheap COM_QUERY admission classifier for statements that can open a write
/// transaction and park on row locks. It intentionally only answers `true`
/// for unambiguous leading DML keywords; false negatives still run correctly
/// on the blocking pool, while false positives would unnecessarily throttle
/// unrelated reads. Locking SELECTs are classified from their parsed AST in
/// `handle_query`.
fn looks_like_text_dml_candidate(sql: &str, mode: crate::sql_lex::SqlLexMode) -> bool {
    if let Some(word) = simple_first_keyword(sql) {
        return [
            b"INSERT".as_slice(),
            b"REPLACE".as_slice(),
            b"UPDATE".as_slice(),
            b"DELETE".as_slice(),
            b"LOAD".as_slice(),
        ]
        .iter()
        .any(|keyword| word.eq_ignore_ascii_case(keyword));
    }
    let Ok(words) =
        crate::sql_lex::leading_keyword_ranges(sql, mode, crate::parser::MYSQL_VERSION_ID, 1)
    else {
        return false;
    };
    let Some(range) = words.first() else {
        return false;
    };
    let word = &sql.as_bytes()[range.clone()];
    [
        b"INSERT".as_slice(),
        b"REPLACE".as_slice(),
        b"UPDATE".as_slice(),
        b"DELETE".as_slice(),
        b"LOAD".as_slice(),
    ]
    .iter()
    .any(|keyword| word.eq_ignore_ascii_case(keyword))
}

fn looks_like_large_text_update_candidate(sql: &str) -> bool {
    const LARGE_LITERAL_MIN_SQL_BYTES: usize = 64 * 1024;
    sql.len() >= LARGE_LITERAL_MIN_SQL_BYTES
        && simple_first_keyword(sql).is_some_and(|word| word.eq_ignore_ascii_case(b"UPDATE"))
}

fn looks_like_inline_select_candidate(sql: &str) -> bool {
    let mut s = sql.trim_start();
    loop {
        if let Some(rest) = s.strip_prefix("--") {
            match rest.find('\n') {
                Some(idx) => s = rest[idx + 1..].trim_start(),
                None => return false,
            }
        } else if let Some(rest) = s.strip_prefix("/*") {
            match rest.find("*/") {
                Some(idx) => s = rest[idx + 2..].trim_start(),
                None => return false,
            }
        } else {
            break;
        }
    }
    let bytes = s.as_bytes();
    if bytes.len() < 7 {
        return false;
    }
    if !bytes[..6].eq_ignore_ascii_case(b"SELECT") {
        return false;
    }
    if !matches!(bytes[6], b' ' | b'\t' | b'\r' | b'\n') {
        return false;
    }
    true
}

/// Conservative sniff for a single `SET` of session-scoped variables whose
/// values are plain literals/identifiers: no parenthesis (subquery, function
/// call), no statement separator, and none of the keywords that reach global,
/// persisted, account or role state.
fn is_simple_session_set_sql(sql: &str) -> bool {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let bytes = trimmed.as_bytes();
    if bytes.len() < 4
        || !bytes[..3].eq_ignore_ascii_case(b"SET")
        || !bytes[3].is_ascii_whitespace()
        || bytes
            .iter()
            .any(|b| matches!(b, b'(' | b';' | b'#' | b'/' | b'-'))
    {
        return false;
    }
    !trimmed
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .any(|word| {
            [
                "GLOBAL",
                "PERSIST",
                "PERSIST_ONLY",
                "PASSWORD",
                "ROLE",
                "TRANSACTION",
            ]
            .iter()
            .any(|keyword| word.eq_ignore_ascii_case(keyword))
        })
}

fn is_show_processlist_sql(sql: &str) -> bool {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let mut words = trimmed.split_ascii_whitespace();
    if !words
        .next()
        .is_some_and(|word| word.eq_ignore_ascii_case("SHOW"))
    {
        return false;
    }
    match words.next() {
        Some(word) if word.eq_ignore_ascii_case("PROCESSLIST") => words.next().is_none(),
        Some(word) if word.eq_ignore_ascii_case("FULL") => {
            words
                .next()
                .is_some_and(|word| word.eq_ignore_ascii_case("PROCESSLIST"))
                && words.next().is_none()
        }
        _ => false,
    }
}

/// A compound stored program is one COM_QUERY statement. The lexical fast
/// check is sufficient for ordinary SQL, but semicolons inside BEGIN/END must
/// be classified by the same routine-aware parser used by the executor.
fn query_requires_multi_statements(sql: &str, sql_mode_bits: u16) -> Result<bool, QueryError> {
    let mode = crate::sql_lex::SqlLexMode::from_parser_bits(sql_mode_bits);
    if !crate::engine::executor::has_non_trailing_statement_separator_with_mode(sql, mode) {
        return Ok(false);
    }
    let _mode = crate::parser::ParserSqlModeGuard::activate(sql_mode_bits);
    crate::parser::parse_statements(sql)
        .map(|statements| statements.len() > 1)
        .map_err(|err| error(crate::error::catalog::ER_PARSE_ERROR, err.to_string()))
}

/// Run server-supplied SQL through the ordinary executor as the login user.
/// Split one child at a time so successful prefixes survive later parse
/// errors, and SET sql_mode changes the lexer for the remaining source.
fn execute_init_connect_sql<E: QueryEngine>(
    engine: &E,
    context: &mut SessionContext,
    sql: &str,
) -> Result<(), QueryError> {
    let mut mode_bits = crate::engine::api::parse_temporal_sql_mode_bits(&context.sql_mode);
    let (mut effective, mut ranges) = crate::parser::split_query_statements(sql, mode_bits);
    let mut index = 0;
    while index < ranges.len() {
        let range = ranges[index].clone();
        let _mode = crate::parser::ParserSqlModeOverrideGuard::activate(mode_bits);
        let result = engine
            .execute(context, &effective[range.clone()])
            .and_then(|response| discard_init_connect_response(response, context));
        // Discard protocol trailers, retaining the last SQL diagnostic area
        // for SHOW WARNINGS / @@warning_count like a regular statement.
        let mut deferred = crate::engine::api::take_statement_warnings();
        context.warnings.append(&mut deferred);
        crate::engine::api::take_statement_info();
        crate::engine::api::take_statement_matched_rows();
        context.pending_statement_info = None;
        context.pending_statement_matched_rows = None;
        result?;
        index += 1;
        let next_mode = crate::engine::api::parse_temporal_sql_mode_bits(&context.sql_mode);
        if next_mode != mode_bits && index < ranges.len() {
            let (next_sql, next_ranges) =
                crate::parser::split_query_statements(&effective[range.end + 1..], next_mode);
            effective = std::borrow::Cow::Owned(next_sql.into_owned());
            ranges = next_ranges;
            index = 0;
            mode_bits = next_mode;
        }
    }
    Ok(())
}

fn discard_init_connect_response(
    response: QueryResponse,
    context: &mut SessionContext,
) -> Result<(), QueryError> {
    match response {
        QueryResponse::StreamingResultSet(mut result) => {
            let mut rows = 0;
            let drained = loop {
                match result.cursor.next_row() {
                    Ok(Some(_)) => rows += 1,
                    Ok(None) => break Ok(()),
                    Err(err) => break Err(err),
                }
            };
            context.warnings.append(&mut result.cursor.take_warnings());
            drained?;
            context.found_rows = result.found_rows.unwrap_or(rows);
        }
        QueryResponse::MultiResult(results) => {
            for result in results {
                discard_init_connect_response(result, context)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn is_mysql_client_sandbox_probe(sql: &str) -> bool {
    // MySQL 9.x bootstrap probe is the literal "select $$" (9 bytes after
    // trim). Don't allocate a 500KB normalized clone of a bulk-INSERT just
    // to compare against a 9-byte string — sniff bytes first, then fall
    // back to the canonical normalize-and-compare on plausible candidates
    // (short queries where the leading byte is `s`/`S`).
    let trimmed = sql.trim_start();
    if trimmed.len() < 9 || trimmed.len() > 64 {
        return false;
    }
    let first = trimmed.as_bytes()[0];
    if (first != b's' && first != b'S') || memchr::memmem::find(trimmed.as_bytes(), b"$$").is_none()
    {
        return false;
    }
    crate::engine::system::normalize_sql(sql) == "select $$"
}

/// Detect a replica's semi-sync capability handshake. A MySQL 8.0 replica sends
/// this as `SET @rpl_semi_sync_replica = 1, @rpl_semi_sync_slave = 1` (both user
/// variables in one multi-assignment), older ones as a single
/// `SET @rpl_semi_sync_slave = 1`. Returns `Some(true)` when either variable is
/// set truthy (1/ON/TRUE), `Some(false)` for an explicit disable, `None` if the
/// statement is not that handshake. Byte-sniffs first so a bulk INSERT is never
/// normalized.
fn detect_semi_sync_slave_request(sql: &str) -> Option<bool> {
    let trimmed = sql.trim_start();
    if trimmed.len() > 160 {
        return None;
    }
    let first = trimmed.as_bytes().first().copied()?;
    if first != b's' && first != b'S' {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("set") {
        return None;
    }
    // Scan each comma-separated assignment; a truthy value on either the
    // `slave` or the `replica` variable wins.
    let mut result: Option<bool> = None;
    for assign in lower[3..].split(',') {
        let assign = assign.trim();
        let rest = if let Some(r) = assign.strip_prefix("@rpl_semi_sync_slave") {
            r
        } else if let Some(r) = assign.strip_prefix("@rpl_semi_sync_replica") {
            r
        } else {
            continue;
        };
        let Some(value) = rest.trim_start().strip_prefix('=') else {
            continue; // e.g. `@rpl_semi_sync_slave_enabled` — not the user var.
        };
        let value = value.trim().trim_end_matches(';').trim();
        match value {
            "1" | "on" | "true" => return Some(true),
            "0" | "off" | "false" => result = Some(false),
            _ => {}
        }
    }
    result
}

/// Frame one binlog event into a dump packet payload. Normal (async) framing is
/// `[0x00][event]`; a semi-sync replica gets `[0x00][0xEF][flag][event]` where
/// flag `0x01` requests an ack. Matches MySQL's `updateSyncHeader`.
fn frame_binlog_packet(packet: &mut Vec<u8>, event: &[u8], semi_sync: bool, need_ack: bool) {
    packet.clear();
    packet.push(0x00);
    if semi_sync {
        packet.push(0xEF);
        packet.push(if need_ack { 0x01 } else { 0x00 });
    }
    packet.extend_from_slice(event);
}

/// Keep the read half live while socket backpressure suspends a binlog write.
/// The wrapper is stack-owned and keeps the codec's write future intact for
/// plaintext, TLS, and compressed output. Partial ACK frames remain in pending.
struct BinlogAckWriter<'a, S> {
    stream: &'a mut S,
    manager: &'a crate::binlog::BinlogSourceManager,
    server_id: Option<u32>,
    pending: &'a mut Vec<u8>,
    scratch: &'a mut [u8],
    max_frame: usize,
}

impl<S: AsyncRead + AsyncWrite + Unpin> BinlogAckWriter<'_, S> {
    fn consume_ready_acks(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        // Bound per-poll work so an ACK flood cannot starve outgoing data.
        for _ in 0..8 {
            let room = self.max_frame.saturating_sub(self.pending.len());
            if room == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "semi-sync ACK frame exceeds 1 KiB",
                ));
            }
            // Keep partial frames inside the admitted buffer capacity, even
            // when a valid frame and its successor arrive in one socket read.
            let read_len = room.min(self.scratch.len());
            let mut bytes = ReadBuf::new(&mut self.scratch[..read_len]);
            match Pin::new(&mut *self.stream).poll_read(cx, &mut bytes) {
                Poll::Pending => return Ok(()),
                Poll::Ready(Err(error)) => return Err(error),
                Poll::Ready(Ok(())) => {
                    if bytes.filled().is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "replica closed during binlog write",
                        ));
                    }
                    self.pending.extend_from_slice(bytes.filled());
                    let (acks, _) = parse_semi_sync_ack_frames(self.pending);
                    if self.pending.len() > self.max_frame {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "semi-sync ACK frame exceeds 1 KiB",
                        ));
                    }
                    if let Some(server_id) = self.server_id {
                        for (position, file) in acks {
                            self.manager
                                .record_semi_sync_ack(server_id, &file, position);
                        }
                    }
                }
            }
        }
        // Readiness may remain set without another edge; retain progress even
        // when the write side is still blocked and more ACK bytes are queued.
        cx.waker().wake_by_ref();
        Ok(())
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for BinlogAckWriter<'_, S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let writer = self.get_mut();
        if let Err(error) = writer.consume_ready_acks(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut *writer.stream).poll_write(cx, bytes)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let writer = self.get_mut();
        if let Err(error) = writer.consume_ready_acks(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut *writer.stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.get_mut().stream).poll_shutdown(cx)
    }
}

/// Read ready ACK bytes even while the sender has a continuous event backlog.
/// Polling AsyncRead once is cancellation-safe for both TCP and TLS streams.
#[cfg(test)]
async fn read_ready_binlog_ack<R: AsyncRead + Unpin>(
    stream: &mut R,
    bytes: &mut [u8],
) -> io::Result<Option<usize>> {
    std::future::poll_fn(|cx| {
        let mut buf = ReadBuf::new(bytes);
        match Pin::new(&mut *stream).poll_read(cx, &mut buf) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(Some(buf.filled().len()))),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Ready(Ok(None)),
        }
    })
    .await
}

/// Parse whole MySQL frames out of `buf` (accumulated raw socket bytes),
/// returning every semi-sync ack (`[0xEF][pos][file]` payload) as `(pos, file)`
/// plus the sequence_id of the LAST fully-consumed frame. Consumed frames are
/// drained from the front of `buf`; a trailing partial frame is left for the
/// next read. The returned sequence is informational: ACK arrival must not
/// reset the sender's counter because later events may already be in flight.
/// The sender resets at the event that requested the ACK instead.
fn parse_semi_sync_ack_frames(buf: &mut Vec<u8>) -> (Vec<(u64, String)>, Option<u8>) {
    let mut consumed = 0usize;
    let mut last_seq = None;
    let mut acks = Vec::new();
    loop {
        let rest = &buf[consumed..];
        if rest.len() < 4 {
            break; // need the 3-byte length + 1-byte sequence header
        }
        let payload_len =
            (rest[0] as usize) | ((rest[1] as usize) << 8) | ((rest[2] as usize) << 16);
        let frame_end = 4 + payload_len;
        if rest.len() < frame_end {
            break; // partial frame; wait for more bytes
        }
        last_seq = Some(rest[3]);
        if let Some(ack) = crate::binlog::parse_semi_sync_ack(&rest[4..frame_end]) {
            acks.push(ack);
        }
        consumed += frame_end;
    }
    if consumed > 0 {
        buf.drain(..consumed);
    }
    (acks, last_seq)
}

fn is_password_change_statement(sql: &str) -> bool {
    // Cheap byte-prefix sniff: only proceed with the (allocating) normalize
    // when the statement plausibly starts with `SET` or `ALTER`. Bulk-INSERT
    // / SELECT / DML never start with either, so this short-circuits
    // without touching a 500KB body.
    let trimmed = sql.trim_start();
    let prefix = trimmed.as_bytes();
    let starts_with_set = prefix.len() >= 4
        && prefix[..3].eq_ignore_ascii_case(b"set")
        && matches!(prefix[3], b' ' | b'\t' | b'\r' | b'\n');
    let starts_with_alter = prefix.len() >= 6
        && prefix[..5].eq_ignore_ascii_case(b"alter")
        && matches!(prefix[5], b' ' | b'\t' | b'\r' | b'\n');
    if !starts_with_set && !starts_with_alter {
        return false;
    }
    let normalized = crate::engine::system::normalize_sql(sql);
    normalized.starts_with("set password")
        || (normalized.starts_with("alter user ") && normalized.contains(" identified "))
}

/// How a DDL-looking COM_QUERY affects the SQL caches.
///
/// The split exists because the cross-session plan cache validates every
/// entry's `(table_id, schema_version)` deps against the live catalog on
/// each hit, and every table-mutating storage DDL op (`ALTER` actions,
/// `CREATE`/`DROP INDEX`, `RENAME TABLE`, trigger attach/detach, …) bumps
/// the affected table's `schema_version`. Table-scoped DDL therefore
/// needs *no* global epoch bump — affected entries lazily evict on their
/// own, and entries for unrelated tables keep their compiled plans.
/// Before this split, a `USE` or any `CREATE TABLE` cleared the entire
/// process-wide statement + plan cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DdlCacheScope {
    /// Not catalog-mutating: caches untouched.
    None,
    /// Session-local namespace/cache event (`USE`, `ALTER SYSTEM …`). Wire
    /// prepared handles survive and keep their prepare-time database.
    SessionOnly,
    /// Table-scoped DDL: per-entry dependency validation covers the
    /// shared caches; prepared handles survive and lazily recompile.
    TableScoped,
    /// Catalog-wide event (views / routines / triggers / events /
    /// databases / users / unknown second keyword): bump the global
    /// epoch, clearing every cached statement + plan.
    Global,
}

fn ddl_cache_scope(sql: &str, mode: crate::sql_lex::SqlLexMode) -> DdlCacheScope {
    if simple_first_keyword(sql).is_some_and(|word| {
        ![
            b"USE".as_slice(),
            b"TRUNCATE".as_slice(),
            b"RENAME".as_slice(),
            b"ANALYZE".as_slice(),
            b"CREATE".as_slice(),
            b"ALTER".as_slice(),
            b"DROP".as_slice(),
        ]
        .iter()
        .any(|keyword| word.eq_ignore_ascii_case(keyword))
    }) {
        return DdlCacheScope::None;
    }
    let Ok(words) =
        crate::sql_lex::leading_keyword_ranges(sql, mode, crate::parser::MYSQL_VERSION_ID, 2)
    else {
        return DdlCacheScope::None;
    };
    let Some(first) = words.first() else {
        return DdlCacheScope::None;
    };
    let kw = &sql.as_bytes()[first.clone()];
    if !matches!(kw.len(), 3..=8) {
        return DdlCacheScope::None;
    }
    if kw.eq_ignore_ascii_case(b"USE") {
        // Statement / plan cache keys carry the database name; only the
        // session's prepared statements were qualified against the old
        // default database.
        return DdlCacheScope::SessionOnly;
    }
    if kw.eq_ignore_ascii_case(b"TRUNCATE") || kw.eq_ignore_ascii_case(b"RENAME") {
        // TRUNCATE leaves the schema untouched (cached plans stay
        // correct); RENAME TABLE bumps the renamed table's
        // schema_version, so dependency validation evicts its plans.
        return DdlCacheScope::TableScoped;
    }
    if kw.eq_ignore_ascii_case(b"ANALYZE") {
        // Statistics refresh: plans everywhere were costed against the
        // old stats — keep the conservative full replan.
        return DdlCacheScope::Global;
    }
    let object_scoped = kw.eq_ignore_ascii_case(b"CREATE")
        || kw.eq_ignore_ascii_case(b"ALTER")
        || kw.eq_ignore_ascii_case(b"DROP");
    if !object_scoped {
        return DdlCacheScope::None;
    }
    let kw2 = words
        .get(1)
        .map(|range| &sql.as_bytes()[range.clone()])
        .unwrap_or_default();
    if kw.eq_ignore_ascii_case(b"ALTER") && kw2.eq_ignore_ascii_case(b"SYSTEM") {
        // ALTER SYSTEM CHECKPOINT / FLUSH … drive their own cache
        // maintenance in the executor.
        return DdlCacheScope::SessionOnly;
    }
    let table_scoped = kw2.eq_ignore_ascii_case(b"TABLE")
        || kw2.eq_ignore_ascii_case(b"TEMPORARY")
        || kw2.eq_ignore_ascii_case(b"INDEX")
        || (kw.eq_ignore_ascii_case(b"CREATE")
            && (kw2.eq_ignore_ascii_case(b"UNIQUE")
                || kw2.eq_ignore_ascii_case(b"FULLTEXT")
                || kw2.eq_ignore_ascii_case(b"SPATIAL")));
    if table_scoped {
        DdlCacheScope::TableScoped
    } else {
        DdlCacheScope::Global
    }
}

/// True iff `statement` is a plain SELECT (or SELECT-of-SELECT) that
/// cannot end up parked on a row-lock condvar. The current executor
/// blocks on `Condvar::wait_timeout` only when acquiring row/unique-key
/// locks, which happens via `lock_rows_blocking` from
/// `SELECT ... FOR UPDATE` / `FOR SHARE`, or from any DML
/// (INSERT / UPDATE / DELETE) path. Pure MVCC reads walk the undo chain
/// and never block on locks. Returning true lets the session run the
/// statement inline on the tokio task and skip the `spawn_blocking`
/// hand-off; returning false routes through the blocking pool so the
/// runtime worker is never parked.
fn statement_contains_fusion<'q>(statement: impl Into<crate::parser::QueryRef<'q>>) -> bool {
    match statement.into() {
        crate::parser::QueryRef::Select(select) => {
            struct Fusion(bool);
            impl crate::parser::visit::AstVisitor for Fusion {
                fn enter_query(&mut self, query: &crate::parser::QueryExpr) {
                    self.0 |= matches!(query, crate::parser::QueryExpr::Rrf(_));
                }
                fn walk_query_children(&self, _: &crate::parser::QueryExpr) -> bool {
                    !self.0
                }
                fn continue_expr_walk(&self) -> bool {
                    !self.0
                }
            }
            let mut fusion = Fusion(false);
            crate::parser::visit::visit_select(&mut fusion, select);
            fusion.0
        }
        crate::parser::QueryRef::Rrf(_) => true,
        crate::parser::QueryRef::WithCte(with) => {
            statement_contains_fusion(&with.body)
                || with
                    .ctes
                    .iter()
                    .any(|cte| statement_contains_fusion(&cte.query))
        }
        crate::parser::QueryRef::SetOperation(set) => {
            statement_contains_fusion(&set.left) || statement_contains_fusion(&set.right)
        }
        crate::parser::QueryRef::Other(Statement::Explain {
            target, analyze, ..
        }) => *analyze && statement_contains_fusion(target),
        _ => false,
    }
}

fn statement_has_computed_ordering<'q>(statement: impl Into<crate::parser::QueryRef<'q>>) -> bool {
    let statement = statement.into();
    if statement_contains_fusion(statement) {
        return true;
    }
    match statement {
        crate::parser::QueryRef::Select(select) => {
            crate::engine::cache::select_has_computed_ordering(select)
        }
        crate::parser::QueryRef::WithCte(with) => statement_has_computed_ordering(&with.body),
        crate::parser::QueryRef::SetOperation(set) => {
            statement_has_computed_ordering(&set.left)
                || statement_has_computed_ordering(&set.right)
        }
        crate::parser::QueryRef::Other(Statement::Explain {
            target, analyze, ..
        }) => *analyze && statement_has_computed_ordering(target),
        _ => false,
    }
}

pub(crate) fn statement_is_lock_safe_read<'q>(
    statement: impl Into<crate::parser::QueryRef<'q>>,
) -> bool {
    let statement = statement.into();
    if statement_contains_blocking_function(statement) {
        return false;
    }
    match statement {
        crate::parser::QueryRef::Select(stmt) => !stmt.for_update,
        crate::parser::QueryRef::Other(Statement::Explain {
            target, analyze, ..
        }) => !*analyze || statement_is_lock_safe_read(target),
        crate::parser::QueryRef::WithCte(stmt) => statement_is_lock_safe_read(&stmt.body),
        crate::parser::QueryRef::Rrf(rrf) => rrf
            .branches
            .iter()
            .all(|branch| statement_is_lock_safe_read(&branch.query)),
        crate::parser::QueryRef::SetOperation(stmt) => {
            statement_is_lock_safe_read(&stmt.left) && statement_is_lock_safe_read(&stmt.right)
        }
        crate::parser::QueryRef::Other(Statement::Describe { .. })
        | crate::parser::QueryRef::Other(Statement::ShowDatabases { .. })
        | crate::parser::QueryRef::Other(Statement::ShowTables { .. })
        | crate::parser::QueryRef::Other(Statement::ShowTableStatus { .. })
        | crate::parser::QueryRef::Other(Statement::ShowColumns { .. })
        | crate::parser::QueryRef::Other(Statement::ShowIndex { .. })
        | crate::parser::QueryRef::Other(Statement::ShowCreateDatabase { .. })
        | crate::parser::QueryRef::Other(Statement::ShowCreateTable { .. })
        | crate::parser::QueryRef::Other(Statement::ShowCreateRoutine { .. })
        | crate::parser::QueryRef::Other(Statement::ShowRoutineStatus { .. })
        | crate::parser::QueryRef::Other(Statement::ShowCharset { .. })
        | crate::parser::QueryRef::Other(Statement::ShowCollation { .. })
        | crate::parser::QueryRef::Other(Statement::ShowVariables { .. })
        | crate::parser::QueryRef::Other(Statement::ShowStatus { .. })
        | crate::parser::QueryRef::Other(Statement::ShowDiagnostics { .. })
        | crate::parser::QueryRef::Other(Statement::ShowCountDiagnostics { .. })
        | crate::parser::QueryRef::Other(Statement::ShowEngines)
        | crate::parser::QueryRef::Other(Statement::ShowEngineMydbStatus)
        | crate::parser::QueryRef::Other(Statement::ShowMydbMemory)
        | crate::parser::QueryRef::Other(Statement::ShowMydbMemorySessions)
        | crate::parser::QueryRef::Other(Statement::ShowEngine { .. })
        | crate::parser::QueryRef::Other(Statement::ShowPlugins)
        | crate::parser::QueryRef::Other(Statement::ShowOpenTables { .. })
        | crate::parser::QueryRef::Other(Statement::ShowProcesslist { .. })
        | crate::parser::QueryRef::Other(Statement::ShowGrants)
        | crate::parser::QueryRef::Other(Statement::BinlogAdmin { .. })
        | crate::parser::QueryRef::Other(Statement::ShowPrivileges) => true,
        _ => false,
    }
}

fn context_allows_inline_lock_safe_read(context: &SessionContext) -> bool {
    context.effective_isolation() != crate::storage::IsolationLevel::Serializable
        || (context.autocommit && !context.explicit_txn && context.current_txn.is_none())
}

fn statement_contains_blocking_function<'q>(
    statement: impl Into<crate::parser::QueryRef<'q>>,
) -> bool {
    let statement = statement.into();
    match statement {
        crate::parser::QueryRef::Select(stmt) => select_contains_blocking_function(stmt),
        crate::parser::QueryRef::Other(Statement::Explain { target, .. }) => {
            statement_contains_blocking_function(target)
        }
        crate::parser::QueryRef::WithCte(stmt) => {
            stmt.ctes
                .iter()
                .any(|cte| statement_contains_blocking_function(&cte.query))
                || statement_contains_blocking_function(&stmt.body)
        }
        crate::parser::QueryRef::Rrf(rrf) => rrf
            .branches
            .iter()
            .any(|branch| statement_contains_blocking_function(&branch.query)),
        crate::parser::QueryRef::SetOperation(stmt) => {
            statement_contains_blocking_function(&stmt.left)
                || statement_contains_blocking_function(&stmt.right)
        }
        _ => false,
    }
}

fn select_contains_blocking_function(stmt: &Select) -> bool {
    stmt.projection
        .iter()
        .any(|item| select_item_contains_blocking_function(item))
        || stmt
            .selection
            .as_ref()
            .is_some_and(expr_contains_blocking_function)
        || stmt.joins.iter().any(|join| {
            join.condition
                .as_ref()
                .is_some_and(expr_contains_blocking_function)
                || join.derived.as_ref().is_some_and(|derived| match derived {
                    SubQuery::Select(s) => select_contains_blocking_function(s),
                    stmt @ (SubQuery::SetOp(_) | SubQuery::Rrf(_) | SubQuery::WithCte(_)) => {
                        statement_contains_blocking_function(stmt)
                    }
                })
        })
        || stmt.group_by.iter().any(expr_contains_blocking_function)
        || stmt
            .having
            .as_ref()
            .is_some_and(expr_contains_blocking_function)
        || stmt
            .order_by
            .iter()
            .any(|order| expr_contains_blocking_function(&order.expr))
        || stmt.derived.as_ref().is_some_and(|derived| match derived {
            SubQuery::Select(s) => select_contains_blocking_function(s),
            stmt @ (SubQuery::SetOp(_) | SubQuery::Rrf(_) | SubQuery::WithCte(_)) => {
                statement_contains_blocking_function(stmt)
            }
        })
}

fn select_item_contains_blocking_function(item: &SelectItem) -> bool {
    match item {
        SelectItem::Expr { expr, .. } => expr_contains_blocking_function(expr),
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    }
}

fn expr_contains_blocking_function(expr: &Expr) -> bool {
    crate::parser::visit::any_expr(expr, |expr| match expr {
        Expr::WindowFunction { .. } => true,
        Expr::Function { name, .. } => {
            crate::engine::ai::is_function(name)
                || crate::engine::http::is_function(name)
                || name.eq_ignore_ascii_case("SLEEP")
                || name.eq_ignore_ascii_case("GET_LOCK")
                || name.eq_ignore_ascii_case("NEXTVAL")
                || name.eq_ignore_ascii_case("SETVAL")
                || name.eq_ignore_ascii_case("CURRVAL")
                || name.eq_ignore_ascii_case("LASTVAL")
        }
        _ => false,
    })
}

/// AST-level twin of [`ddl_cache_scope`] for the prepared-statement
/// execute path, where the statement is already parsed.
fn statement_ddl_cache_scope(statement: &Statement) -> DdlCacheScope {
    match statement {
        Statement::UseDatabase(_) => DdlCacheScope::SessionOnly,
        Statement::CreateTable(_)
        | Statement::CreateTableLike { .. }
        | Statement::AlterTable(_)
        | Statement::DropTable { .. }
        | Statement::TruncateTable { .. }
        | Statement::CreateIndex(_)
        | Statement::DropIndex { .. } => DdlCacheScope::TableScoped,
        Statement::CreateDatabase { .. }
        | Statement::DropDatabase { .. }
        | Statement::AnalyzeTable { .. } => DdlCacheScope::Global,
        _ => DdlCacheScope::None,
    }
}

fn bind_sql_params(
    sql: &str,
    params: &[crate::mysql::protocol::StmtParamValue],
    mode: crate::sql_lex::SqlLexMode,
) -> Result<String, String> {
    let mut out = String::with_capacity(sql.len() + params.len() * 8);
    let mut param_idx = 0;
    for segment in crate::sql_lex::segments(sql, mode, crate::parser::MYSQL_VERSION_ID) {
        let segment = segment.map_err(|err| err.to_string())?;
        let text = &sql[segment.range];
        if !matches!(segment.kind, crate::sql_lex::SqlSegmentKind::Code) {
            out.push_str(text);
            continue;
        }
        let mut start = 0usize;
        for (offset, byte) in text.bytes().enumerate() {
            if byte != b'?' {
                continue;
            }
            out.push_str(&text[start..offset]);
            let Some(value) = params.get(param_idx) else {
                return Err("not enough prepared statement parameters".to_string());
            };
            out.push_str(&stmt_param_to_sql(value, mode.no_backslash_escapes));
            param_idx += 1;
            start = offset + 1;
        }
        out.push_str(&text[start..]);
    }
    if param_idx != params.len() {
        return Err("too many prepared statement parameters".to_string());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn peer_monitor_preserves_pipeline_bytes_and_observes_closed_transport() {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let mut stream = MysqlStream::Plain(BufReader::new(server));
        let peer = stream.clone_socket().unwrap();
        client.write_all(b"queued command").await.unwrap();
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(75),
            wait_peer_closed(&peer)
        )
        .await
        .is_err());
        let mut bytes = [0u8; 14];
        stream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"queued command");
        drop(client);
        tokio::time::timeout(std::time::Duration::from_secs(2), wait_peer_closed(&peer))
            .await
            .unwrap();
    }

    #[test]
    fn hybrid_rrf_nested_prepared_inputs_stay_off_reactor() {
        for sql in [
            "SELECT * FROM ((SELECT 1 AS id ORDER BY id LIMIT 1) RRF (SELECT 1 AS id ORDER BY id LIMIT 1)) h",
            "SELECT (SELECT COUNT(*) FROM ((SELECT 1 AS id ORDER BY id LIMIT 1) RRF (SELECT 1 AS id ORDER BY id LIMIT 1)) h)",
        ] {
            assert!(statement_has_computed_ordering(&parse_statement(sql).unwrap()));
        }
        assert!(!statement_has_computed_ordering(
            &parse_statement("SELECT 1").unwrap()
        ));
    }

    struct PanicCleanupEngine(std::sync::atomic::AtomicUsize);

    impl QueryEngine for PanicCleanupEngine {
        fn execute(
            &self,
            _session: &mut SessionContext,
            _sql: &str,
        ) -> Result<QueryResponse, QueryError> {
            Ok(QueryResponse::Ok)
        }

        fn close_session(&self, _session: &mut SessionContext) -> Result<(), QueryError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ok_multi_result_preserves_sequence_wrap_and_outer_more() {
        use crate::mysql::packet::read_packet;
        for first in [1u8, 254] {
            for outer_more in [false, true] {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let mut client = TcpStream::connect(listener.local_addr().unwrap())
                    .await
                    .unwrap();
                let (stream, peer) = listener.accept().await.unwrap();
                let mut session = Session::try_new(
                    43,
                    peer,
                    stream,
                    None,
                    Arc::new(PanicCleanupEngine(std::sync::atomic::AtomicUsize::new(0))),
                    Arc::new(SessionRegistry::new()),
                )
                .unwrap();
                session.capabilities =
                    NegotiatedCapabilities::new(CLIENT_PROTOCOL_41, CLIENT_PROTOCOL_41);
                let next = session
                    .write_text_multi_result_seq(
                        vec![
                            QueryResponse::Ok,
                            QueryResponse::MultiResult(vec![QueryResponse::OkAffected {
                                affected_rows: 3,
                            }]),
                            QueryResponse::Ok,
                        ],
                        first,
                        outer_more,
                    )
                    .await
                    .unwrap();
                assert_eq!(next, first.wrapping_add(3));
                for index in 0..3u8 {
                    let packet = read_packet(&mut client).await.unwrap().unwrap();
                    assert_eq!(packet.sequence_id, first.wrapping_add(index));
                    assert_eq!(packet.payload[0], 0);
                    assert_eq!(packet.payload[1], if index == 1 { 3 } else { 0 });
                    let status = u16::from_le_bytes([packet.payload[3], packet.payload[4]]);
                    assert_eq!(
                        status & SERVER_MORE_RESULTS_EXISTS != 0,
                        index < 2 || outer_more
                    );
                }
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn session_panic_unregisters_and_closes_engine_context() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, peer) = listener.accept().await.unwrap();
        let engine = Arc::new(PanicCleanupEngine(std::sync::atomic::AtomicUsize::new(0)));
        let registry = Arc::new(SessionRegistry::new());
        let mut session = Session::try_new(
            42,
            peer,
            stream,
            None,
            Arc::clone(&engine),
            Arc::clone(&registry),
        )
        .unwrap();
        session.handle =
            Some(registry.register(42, "user".into(), "localhost".into(), "test".into()));
        session.authenticated = true;
        assert_eq!(registry.snapshot().len(), 1);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _session = session;
            panic!("injected protocol task panic");
        }));
        assert_eq!(engine.0.load(Ordering::Relaxed), 1);
        assert!(registry.snapshot().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn source_audit_b1_worker_panic_releases_real_mdl_and_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(crate::engine::StorageQueryEngine::open(dir.path()).unwrap());
        engine.execute("CREATE DATABASE audit_panic").unwrap();
        engine
            .execute("CREATE TABLE audit_panic.t (id INT PRIMARY KEY)")
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, peer) = listener.accept().await.unwrap();
        let registry = Arc::new(SessionRegistry::new());
        let mut session =
            Session::try_new(4242, peer, stream, None, engine.clone(), registry).unwrap();
        session.authenticated = true;
        engine
            .execute_with_session(&mut session.context, "BEGIN")
            .unwrap();
        engine
            .execute_with_session(&mut session.context, "INSERT INTO audit_panic.t VALUES (1)")
            .unwrap();
        assert!(session.context.current_txn.is_some());
        assert!(!session.context.mdl_txn_grants.borrow().1.is_empty());
        let parked = session.park_context();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _parked = parked;
            panic!("injected worker panic with real transaction and MDL");
        }));
        assert!(result.is_err());
        drop(session);
        let QueryResponse::ResultSet(rows) = engine
            .execute("SELECT COUNT(*) FROM audit_panic.t")
            .unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(rows.rows[0][0].to_display_string(), "0");
        let mut ddl = SessionContext::default();
        ddl.connection_id = 4343;
        engine
            .execute_with_session(&mut ddl, "SET innodb_lock_wait_timeout=1")
            .unwrap();
        engine
            .execute_with_session(&mut ddl, "DROP DATABASE audit_panic")
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropped_session_joins_cancelled_worker_before_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(crate::engine::StorageQueryEngine::open(dir.path()).unwrap());
        engine
            .execute("CREATE TABLE drop_worker (id INT PRIMARY KEY)")
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, peer) = listener.accept().await.unwrap();
        let registry = Arc::new(SessionRegistry::new());
        let handle = registry.register(4243, "root".into(), "localhost".into(), "test".into());
        let mut session =
            Session::try_new(4243, peer, stream, None, engine.clone(), registry).unwrap();
        session
            .context
            .set_cancel_handle(Some(handle.cancel_handle()));
        session.authenticated = true;
        engine
            .execute_with_session(&mut session.context, "BEGIN")
            .unwrap();
        engine
            .execute_with_session(
                &mut session.context,
                "INSERT INTO test.drop_worker VALUES (1)",
            )
            .unwrap();
        let parked = session.park_context();
        let worker = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while parked.check_cancelled().is_ok() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "session drop did not cancel worker"
                );
                std::thread::yield_now();
            }
            // The worker still owns the transaction until it returns/drops.
            assert!(parked.current_txn.is_some());
            drop(parked);
        });
        drop(session);
        worker.join().unwrap();
        let result = engine
            .execute("SELECT COUNT(*) FROM drop_worker")
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(result) = result else {
            panic!("expected result")
        };
        assert_eq!(result.rows[0][0].to_display_string(), "0");
        engine.execute("DROP TABLE drop_worker").unwrap();
    }

    #[test]
    fn panicked_worker_returns_real_session_context() {
        let recovery = Arc::new(ParkedSessionRecovery::default());
        let mut context = SessionContext::default();
        context.connection_id = 42;
        context.explicit_txn = true;
        let parked = ParkedSessionContext {
            context: Some(Box::new(context)),
            recovery: Arc::clone(&recovery),
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _context = parked;
            panic!("injected engine panic");
        }));
        let returned = recovery
            .state
            .lock()
            .context
            .take()
            .expect("context recovered");
        assert_eq!(returned.connection_id, 42);
        assert!(returned.explicit_txn);
    }

    #[test]
    fn protocol_audit_blocking_failures_encode_server_errors() {
        for failure in [
            crate::util::blocking_pool::BlockingError::Cancelled,
            crate::util::blocking_pool::BlockingError::Panicked("injected failure".into()),
        ] {
            let err = query_join_error(failure);
            let packet = crate::mysql::protocol::err_packet(&err);
            assert_eq!(packet[0], 0xff);
            assert_eq!(u16::from_le_bytes([packet[1], packet[2]]), 1105);
        }
    }

    #[test]
    fn post_tls_capabilities_accept_connector_j_clearing_client_ssl() {
        // Captured from Connector/J 8.0.16, as bundled by HyBench. The
        // encrypted response differs from its SSLRequest only by CLIENT_SSL.
        let ssl_request_caps = 0x012e_aa8f;
        let handshake_response_caps = 0x012e_a28f;
        assert_eq!(ssl_request_caps ^ handshake_response_caps, CLIENT_SSL);
        validate_post_tls_capabilities(ssl_request_caps, handshake_response_caps).unwrap();

        let incompatible = handshake_response_caps ^ CLIENT_PLUGIN_AUTH;
        let err = validate_post_tls_capabilities(ssl_request_caps, incompatible).unwrap_err();
        assert!(err.contains("difference=0x00080000"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_completion_preserves_kill_write_error_and_cleanup_order() {
        use crate::engine::api::QueryEngine;
        use std::sync::atomic::AtomicUsize;
        use tokio::io::AsyncWriteExt;

        #[derive(Default)]
        struct Engine {
            invalidations: AtomicUsize,
        }
        impl QueryEngine for Engine {
            fn execute(
                &self,
                _: &mut SessionContext,
                _: &str,
            ) -> Result<QueryResponse, QueryError> {
                Ok(QueryResponse::Ok)
            }
            fn invalidate_statement_cache(&self) {
                self.invalidations.fetch_add(1, Ordering::Relaxed);
            }
        }
        for (killed, write_error, query_error) in [
            (true, false, false),
            (false, true, false),
            (false, false, false),
            (false, false, true),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (stream, peer) = listener.accept().await.unwrap();
            let engine = Arc::new(Engine::default());
            let mut session = Session::new(
                1,
                peer,
                stream,
                None,
                engine.clone(),
                Arc::new(SessionRegistry::new()),
            );
            let mut prepared = empty_prepared_statement();
            append_prepared_long_data(&mut prepared, 0, b"payload").unwrap();
            session.prepared.insert(1, prepared);
            session.kill_flag.store(killed, Ordering::Release);
            if write_error {
                session.stream.shutdown().await.unwrap();
            }
            let response = if query_error {
                Err(crate::types::QueryError::known(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "query error",
                ))
            } else {
                Ok(QueryResponse::Ok)
            };
            let result = session
                .finish_prepared_statement_execute(1, false, DdlCacheScope::Global, response)
                .await;
            assert_eq!(result.is_err(), write_error);
            let completed = !killed && !write_error;
            assert_eq!(session.prepared[&1].long_data[0].is_none(), completed);
            assert_eq!(
                engine.invalidations.load(Ordering::Relaxed),
                usize::from(completed)
            );
            if completed {
                let packet = crate::mysql::packet::read_packet(&mut client)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(packet.payload[0], if query_error { 0xff } else { 0x00 });
            }
        }
    }

    fn empty_prepared_statement() -> PreparedStatement {
        PreparedStatement {
            original_sql_sensitivity: crate::engine::http::SensitivityMemo::new("SELECT ?"),
            original_sql: Arc::from("SELECT ?"),
            sql: Arc::from("SELECT ?"),
            normalized_sql: Arc::from("select ?"),
            database: Arc::from("test"),
            parser_sql_mode_bits: 0,
            param_count: 1,
            param_types: None,
            value_scratch: Vec::with_capacity(1),
            parsed: None,
            computed_ordering: false,
            com_kind: None,
            long_data: vec![None],
            long_data_error: None,
            cursor: None,
            result_metadata: None,
            perf_texts: crate::engine::perf_events::prepared_statement_texts("SELECT ?"),
            select_memo_state: crate::engine::api::PreparedSelectMemoState::default(),
            dml_state: crate::engine::api::PreparedDmlState::default(),
            memory_bytes: 0,
            timer_prepare: 0,
            count_execute: 0,
            sum_errors: 0,
            sum_timer_execute: 0,
            min_timer_execute: u64::MAX,
            max_timer_execute: 0,
            memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
                0,
            )
            .unwrap(),
        }
    }

    #[test]
    fn protocol_audit_long_data_spare_capacity_is_fully_charged() {
        let mut prepared = empty_prepared_statement();
        let mut slot = Vec::with_capacity(64);
        slot.extend_from_slice(b"seed");
        prepared.memory_charge.try_grow(slot.capacity()).unwrap();
        prepared.long_data[0] = Some(slot);
        for size in [70, 1, 200, 33] {
            append_prepared_long_data(&mut prepared, 0, &vec![b'x'; size]).unwrap();
            let slot = prepared.long_data[0].as_ref().unwrap();
            assert_eq!(prepared.memory_charge.bytes(), slot.capacity());
        }
        let slot = prepared.long_data[0].as_ref().unwrap();
        assert_eq!(slot.len(), 4 + 70 + 1 + 200 + 33);
        assert_eq!(&slot[..4], b"seed");
        clear_prepared_long_data(&mut prepared);
        assert_eq!(prepared.memory_charge.bytes(), 0);
        assert!(prepared.long_data[0].is_none());
    }

    #[test]
    fn prepared_memory_estimate_tracks_only_retained_dynamic_capacity() {
        let mut prepared = empty_prepared_statement();
        let base = prepared.estimated_memory_bytes();

        prepared.long_data[0] = Some(Vec::with_capacity(257));
        let with_long_data = prepared.estimated_memory_bytes();
        assert_eq!(with_long_data - base, 257);

        prepared.long_data[0] = None;
        prepared.param_types = Some(Vec::with_capacity(3));
        assert_eq!(
            prepared.estimated_memory_bytes() - base,
            (3 * std::mem::size_of::<StmtParamType>()) as u64
        );
    }

    #[test]
    fn governed_binary_row_scratch_tracks_capacity_and_reuses_it() {
        let mut scratch = Vec::new();
        let mut charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::ProtocolTransient,
            0,
        )
        .unwrap();
        ensure_governed_scratch_capacity(&mut scratch, &mut charge, 257).unwrap();
        assert!(scratch.capacity() >= 257);
        assert_eq!(charge.bytes(), scratch.capacity());
        let capacity = scratch.capacity();
        ensure_governed_scratch_capacity(&mut scratch, &mut charge, 64).unwrap();
        assert_eq!(scratch.capacity(), capacity);
        assert_eq!(charge.bytes(), capacity);
    }

    #[test]
    fn command_statement_id_reads_full_command_packet() {
        assert_eq!(
            command_statement_id(&[COM_STMT_EXECUTE, 0x78, 0x56, 0x34, 0x12]),
            Some(0x1234_5678)
        );
        assert_eq!(command_statement_id(&[COM_STMT_EXECUTE, 1, 2, 3]), None);
    }

    #[test]
    fn detect_semi_sync_slave_request_matches_replica_handshake() {
        // The two capability handshakes a MySQL replica sends before its dump.
        assert_eq!(
            detect_semi_sync_slave_request("SET @rpl_semi_sync_slave = 1"),
            Some(true)
        );
        assert_eq!(
            detect_semi_sync_slave_request("SET @rpl_semi_sync_replica=1"),
            Some(true)
        );
        // The exact form MySQL 8.0 sends: multi-assignment of both user vars.
        assert_eq!(
            detect_semi_sync_slave_request(
                "SET @rpl_semi_sync_replica = 1, @rpl_semi_sync_slave = 1"
            ),
            Some(true)
        );
        // A prefix-only var name must not match.
        assert_eq!(
            detect_semi_sync_slave_request("SET @rpl_semi_sync_slave_enabled = 1"),
            None
        );
        // Case / whitespace / terminator tolerant, ON/OFF accepted.
        assert_eq!(
            detect_semi_sync_slave_request("set   @RPL_SEMI_SYNC_SLAVE = ON ;"),
            Some(true)
        );
        assert_eq!(
            detect_semi_sync_slave_request("SET @rpl_semi_sync_slave = 0"),
            Some(false)
        );
        // Unrelated statements are not the handshake.
        assert_eq!(detect_semi_sync_slave_request("SELECT 1"), None);
        assert_eq!(detect_semi_sync_slave_request("SET @other_var = 1"), None);
        assert_eq!(
            detect_semi_sync_slave_request(
                "SET @master_binlog_checksum = @@global.binlog_checksum"
            ),
            None
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn audit_busy_dump_consumes_ack_before_backlog_is_drained() {
        use crate::mysql::packet::{read_packet, write_packet};
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        crate::binlog::init_global(
            dir.path().to_path_buf(),
            crate::binlog::BinlogConfig {
                enabled: true,
                sync_binlog: 0,
                ..crate::binlog::BinlogConfig::default()
            },
        )
        .unwrap();
        let statement = format!("DO '{}'", "x".repeat(4096));
        // The regression needs a large committed stream, not one metadata
        // fsync per query. Keep all 2048 payloads (8 MiB) and multiple commit
        // boundaries so ACKs must advance with later transactions queued.
        for txn_id in 1..=64 {
            for _ in 0..32 {
                crate::binlog::record_ddl_transactional_with(
                    txn_id,
                    "test",
                    &statement,
                    &crate::binlog::BinlogQueryContext::default(),
                )
                .unwrap();
            }
            crate::binlog::commit_txn(txn_id, txn_id).unwrap();
        }
        let manager = crate::binlog::manager().unwrap();
        crate::binlog::set_semi_sync_source_enabled(true).unwrap();
        let _registration = manager
            .register_replica(crate::binlog::RegisteredReplica {
                server_id: 42,
                host: "localhost".into(),
                port: 3306,
                user: "repl".into(),
                master_id: 1,
                replication_rank: 0,
                connected_at: std::time::SystemTime::now(),
                semi_sync: false,
                acked_file: String::new(),
                acked_pos: 0,
                replica_uuid: String::new(),
                regid: 0,
            })
            .unwrap();
        let position = manager.current_position().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        // Keep the receive window much smaller than the queued dump. Otherwise
        // host TCP autotuning can drain the sender before this test observes ACKs.
        socket.set_recv_buffer_size(16 * 1024).unwrap();
        let mut client = socket
            .connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        // Do not let Nagle buffering delay the small ACK beyond the bounded
        // backlog prefix: this fixture checks sender progress, not TCP timers.
        client.set_nodelay(true).unwrap();
        let (stream, peer) = listener.accept().await.unwrap();
        let mut session = Session::new(
            42,
            peer,
            stream,
            None,
            Arc::new(PanicCleanupEngine(std::sync::atomic::AtomicUsize::new(0))),
            Arc::new(SessionRegistry::new()),
        );
        session.semi_sync_replica_requested = true;
        session.dump_server_id = Some(42);
        let source = manager.clone();
        let file = position.file.clone();
        let sender = tokio::spawn(async move {
            session
                .stream_binlog_events_inner(source, file, 4, false, String::new())
                .await
        });
        let mut received = 0;
        let mut expected_sequence = 1u8;
        let acknowledged = loop {
            let packet = tokio::time::timeout(Duration::from_secs(5), read_packet(&mut client))
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(packet.sequence_id, expected_sequence);
            expected_sequence = if packet.payload[2] == 1 {
                1
            } else {
                expected_sequence.wrapping_add(1)
            };
            received += 1;
            if packet.payload[2] == 1 {
                let event = &packet.payload[3..];
                break u32::from_le_bytes(event[13..17].try_into().unwrap()) as u64;
            }
        };
        assert!(acknowledged < position.position);
        let mut ack = vec![0xef];
        ack.extend_from_slice(&acknowledged.to_le_bytes());
        ack.extend_from_slice(position.file.as_bytes());
        write_packet(&mut client, 0, &ack).await.unwrap();
        // Continue draining a bounded prefix to release socket backpressure.
        // There are thousands of events still queued: idle-only ACK reads fail.
        for _ in 0..256 {
            let packet = tokio::time::timeout(Duration::from_secs(5), read_packet(&mut client))
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(
                packet.sequence_id, expected_sequence,
                "late ACK must not reset an in-flight stream"
            );
            expected_sequence = if packet.payload[2] == 1 {
                1
            } else {
                expected_sequence.wrapping_add(1)
            };
            received += 1;
            if manager.registered_replicas()[0].acked_pos >= acknowledged {
                break;
            }
            // Park briefly so the reactor can observe the inbound ACK while
            // the bounded prefix releases write backpressure. The receive
            // window prevents an idle-only sender from exhausting the backlog.
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let recorded = manager.registered_replicas()[0].acked_pos;
        sender.abort();
        let _ = sender.await;
        assert!(received < 1024);
        assert_eq!(
            recorded, acknowledged,
            "ACK must progress while events remain queued"
        );
        assert_eq!(
            manager.semi_sync_wait(&position.file, acknowledged),
            crate::binlog::SemiSyncOutcome::Acked
        );
    }

    #[tokio::test]
    async fn binlog_ack_progresses_while_write_remains_blocked() {
        use std::time::Duration;
        use tokio::io::AsyncWriteExt;
        let dir = tempfile::tempdir().unwrap();
        crate::binlog::init_global(
            dir.path().to_path_buf(),
            crate::binlog::BinlogConfig {
                enabled: true,
                sync_binlog: 0,
                ..crate::binlog::BinlogConfig::default()
            },
        )
        .unwrap();
        crate::binlog::record_ddl_autocommit("test", "DO 1").unwrap();
        let manager = crate::binlog::manager().unwrap();
        crate::binlog::set_semi_sync_source_enabled(true).unwrap();
        let _registration = manager
            .register_replica(crate::binlog::RegisteredReplica {
                server_id: 42,
                host: "localhost".into(),
                port: 3306,
                user: "repl".into(),
                master_id: 1,
                replication_rank: 0,
                connected_at: std::time::SystemTime::now(),
                semi_sync: true,
                acked_file: String::new(),
                acked_pos: 0,
                replica_uuid: String::new(),
                regid: 0,
            })
            .unwrap();
        let position = manager.current_position().unwrap();
        let (mut stream, mut peer) = tokio::io::duplex(64);
        let mut pending = Vec::with_capacity(1028);
        let mut scratch = [0; 512];
        let mut writer = BinlogAckWriter {
            stream: &mut stream,
            manager: &manager,
            server_id: Some(42),
            pending: &mut pending,
            scratch: &mut scratch,
            max_frame: 1028,
        };
        let payload = [0x55; 256];
        let mut write = Box::pin(writer.write_all(&payload));
        assert!(tokio::time::timeout(Duration::from_millis(10), &mut write)
            .await
            .is_err());
        let mut ack = vec![0xef];
        ack.extend_from_slice(&position.position.to_le_bytes());
        ack.extend_from_slice(position.file.as_bytes());
        crate::mysql::packet::write_packet(&mut peer, 0, &ack)
            .await
            .unwrap();
        // The peer never drains output, so the original frame must remain
        // pending while its read side processes the complete ACK.
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut write)
            .await
            .is_err());
        assert_eq!(
            manager.registered_replicas()[0].acked_pos,
            position.position
        );
    }

    #[tokio::test]
    async fn audit_busy_binlog_sender_consumes_fragmented_ack_without_waiting() {
        use tokio::io::AsyncWriteExt;
        let (mut receiver, mut peer) = tokio::io::duplex(1024);
        let mut scratch = [0; 512];
        assert_eq!(
            read_ready_binlog_ack(&mut receiver, &mut scratch)
                .await
                .unwrap(),
            None
        );
        let mut payload = vec![0xef];
        payload.extend_from_slice(&1234u64.to_le_bytes());
        payload.extend_from_slice(b"bin.000001");
        let mut frame = (payload.len() as u32).to_le_bytes().to_vec();
        frame.extend_from_slice(&payload);
        let mut pending = Vec::new();
        for fragment in [&frame[..7], &frame[7..]] {
            peer.write_all(fragment).await.unwrap();
            let count = read_ready_binlog_ack(&mut receiver, &mut scratch)
                .await
                .unwrap()
                .unwrap();
            pending.extend_from_slice(&scratch[..count]);
        }
        let (acks, seq) = parse_semi_sync_ack_frames(&mut pending);
        assert_eq!(acks, vec![(1234, "bin.000001".into())]);
        assert_eq!(seq, Some(0));
        assert!(pending.is_empty());
    }

    #[test]
    fn parse_semi_sync_ack_frames_consumes_whole_frames_and_returns_last_sequence() {
        // MySQL frame = [len(3 LE)][seq(1)][payload]; ack payload = [0xEF][pos][file].
        let ack_frame = |seq: u8, pos: u64| {
            let mut payload = vec![0xEFu8];
            payload.extend_from_slice(&pos.to_le_bytes());
            payload.extend_from_slice(b"mydb-bin.000001");
            let mut frame = Vec::new();
            let len = payload.len() as u32;
            frame.extend_from_slice(&[
                (len & 0xff) as u8,
                ((len >> 8) & 0xff) as u8,
                ((len >> 16) & 0xff) as u8,
                seq,
            ]);
            frame.extend_from_slice(&payload);
            frame
        };
        // Two whole frames + a trailing partial one.
        let mut buf = ack_frame(0, 100);
        buf.extend_from_slice(&ack_frame(0, 200));
        buf.extend_from_slice(&[0x18, 0x00, 0x00]); // partial header, no body
        let (acks, last_seq) = parse_semi_sync_ack_frames(&mut buf);
        assert_eq!(
            acks,
            vec![
                (100, "mydb-bin.000001".to_string()),
                (200, "mydb-bin.000001".to_string())
            ]
        );
        // The replica net_clear's to seq 0 for each ack; the resync uses the last.
        assert_eq!(last_seq, Some(0));
        // Only the partial trailing frame is left for the next read.
        assert_eq!(buf, vec![0x18, 0x00, 0x00]);
        // Partial-only buffers return no acks / no seq and are left intact.
        let mut only_partial = vec![0x18, 0x00, 0x00, 0x00];
        let (acks2, seq2) = parse_semi_sync_ack_frames(&mut only_partial);
        assert!(acks2.is_empty());
        assert_eq!(seq2, None);
        assert_eq!(only_partial.len(), 4);
    }

    #[test]
    fn frame_binlog_packet_applies_semi_sync_header_only_when_negotiated() {
        let event = [0xAAu8, 0xBB, 0xCC];
        // Async framing: just the leading OK byte.
        let mut p = Vec::new();
        frame_binlog_packet(&mut p, &event, false, false);
        assert_eq!(p, vec![0x00, 0xAA, 0xBB, 0xCC]);
        // Semi-sync, ack requested: [0x00][0xEF][0x01][event].
        let mut p = Vec::new();
        frame_binlog_packet(&mut p, &event, true, true);
        assert_eq!(p, vec![0x00, 0xEF, 0x01, 0xAA, 0xBB, 0xCC]);
        // Semi-sync, no ack: flag byte is 0x00.
        let mut p = Vec::new();
        frame_binlog_packet(&mut p, &event, true, false);
        assert_eq!(p, vec![0x00, 0xEF, 0x00, 0xAA, 0xBB, 0xCC]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn binlog_semi_sync_framing_survives_disabled_source_policy() {
        use crate::mysql::packet::read_packet;
        use std::time::Duration;
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        for initially_enabled in [false, true] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            crate::binlog::init_global(
                dir.path().into(),
                crate::binlog::BinlogConfig {
                    enabled: true,
                    sync_binlog: 0,
                    semi_sync_source_enabled: initially_enabled,
                    ..crate::binlog::BinlogConfig::default()
                },
            )
            .unwrap();
            let manager = crate::binlog::manager().unwrap();
            let file = manager.current_position().unwrap().file;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (stream, peer) = listener.accept().await.unwrap();
            let mut session = Session::new(
                73,
                peer,
                stream,
                None,
                Arc::new(PanicCleanupEngine(std::sync::atomic::AtomicUsize::new(0))),
                Arc::new(SessionRegistry::new()),
            );
            session.semi_sync_replica_requested = true;
            let sender = tokio::spawn(async move {
                session
                    .stream_binlog_events_inner(manager, file, 4, false, String::new())
                    .await
            });
            // Fake ROTATE, FDE and PREVIOUS_GTIDS all keep the negotiated header.
            let mut expected_sequence = 1;
            for _ in 0..3 {
                let packet = tokio::time::timeout(Duration::from_secs(2), read_packet(&mut client))
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert_eq!(&packet.payload[..2], &[0, 0xef]);
                assert_eq!(packet.first_sequence_id, expected_sequence);
                expected_sequence = if packet.payload[2] == 1 {
                    1
                } else {
                    packet.sequence_id.wrapping_add(1)
                };
            }
            crate::binlog::set_semi_sync_source_enabled(false).unwrap();
            let statement = if initially_enabled {
                format!(
                    "CREATE VIEW framed AS SELECT 1 /*{}*/",
                    "x".repeat(17 * 1024 * 1024)
                )
            } else {
                "CREATE TABLE framed(id INT)".to_string()
            };
            crate::binlog::record_ddl_autocommit("test", &statement).unwrap();
            crate::binlog::record_ddl_autocommit("test", "CREATE TABLE following(id INT)").unwrap();
            for event_type in [0x21, 0x02, 0x21, 0x02] {
                let packet = tokio::time::timeout(Duration::from_secs(2), read_packet(&mut client))
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert_eq!(&packet.payload[..3], &[0, 0xef, 0]);
                assert_eq!(packet.payload[7], event_type);
                assert_eq!(
                    packet.first_sequence_id, expected_sequence,
                    "the next event must follow every physical frame of a large event"
                );
                expected_sequence = packet.sequence_id.wrapping_add(1);
            }
            drop(client);
            tokio::time::timeout(Duration::from_secs(2), sender)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            crate::binlog::init_global(dir.path().into(), crate::binlog::BinlogConfig::default())
                .unwrap();
        }
    }

    #[test]
    fn connect_wait_timeout_picks_interactive_for_interactive_clients() {
        // Non-interactive clients copy the global wait_timeout.
        assert_eq!(connect_wait_timeout_secs(false, 28_800, 600), 28_800);
        // Interactive clients (CLIENT_INTERACTIVE) copy interactive_timeout.
        assert_eq!(connect_wait_timeout_secs(true, 28_800, 600), 600);
    }

    #[test]
    fn stmt_bytes_param_falls_back_to_raw_when_not_charset_text() {
        use crate::types::Value;
        // Valid utf8mb4 text decodes to a String exactly as before.
        let text = StmtParamValue::Bytes(b"[1,2,3]".to_vec());
        assert!(matches!(
            stmt_param_to_value(&text, "utf8mb4").unwrap(),
            Value::String(s) if s == "[1,2,3]"
        ));
        // Raw little-endian f32 bytes (a binary VECTOR parameter) are not valid
        // utf8mb4; instead of erroring (old behaviour: 1366), they stay as raw
        // Value::Bytes so a VECTOR column can memcpy them with zero parsing.
        let raw = StmtParamValue::Bytes(vec![0x00, 0x00, 0x80, 0x3f, 0xff, 0xfe, 0x00, 0x01]);
        match stmt_param_to_value(&raw, "utf8mb4").unwrap() {
            Value::Bytes(b) => assert_eq!(b, vec![0x00, 0x00, 0x80, 0x3f, 0xff, 0xfe, 0x00, 0x01]),
            other => panic!("expected raw Value::Bytes, got {other:?}"),
        }
    }

    #[test]
    fn ddl_cache_scope_classifies_by_object() {
        use DdlCacheScope::*;
        let mode = crate::sql_lex::SqlLexMode::default();
        // Table-scoped DDL: per-entry (table_id, schema_version) deps
        // validation covers the shared caches.
        for sql in [
            "CREATE TABLE t (id INT)",
            "create temporary table t (id INT)",
            "  /* c */ ALTER TABLE t ADD COLUMN x INT",
            "DROP TABLE t",
            "DROP TEMPORARY TABLE t",
            "CREATE INDEX i ON t (a)",
            "CREATE UNIQUE INDEX i ON t (a)",
            "CREATE FULLTEXT INDEX i ON t (a)",
            "CREATE SPATIAL INDEX i ON t (g)",
            "DROP INDEX i ON t",
            "TRUNCATE TABLE t",
            "TRUNCATE t",
            "RENAME TABLE a TO b",
        ] {
            assert_eq!(ddl_cache_scope(sql, mode), TableScoped, "{sql}");
        }
        // Catalog-wide events keep the global epoch bump.
        for sql in [
            "CREATE VIEW v AS SELECT 1",
            "CREATE OR REPLACE VIEW v AS SELECT 1",
            "DROP VIEW v",
            "ALTER VIEW v AS SELECT 2",
            "CREATE DATABASE d",
            "DROP DATABASE d",
            "CREATE PROCEDURE p() BEGIN END",
            "DROP TRIGGER trg",
            "CREATE USER u",
            "DROP ROLE r",
            "ANALYZE TABLE t",
        ] {
            assert_eq!(ddl_cache_scope(sql, mode), Global, "{sql}");
        }
        // Session-only: nothing in the shared caches depends on the
        // session default database or on ALTER SYSTEM maintenance.
        assert_eq!(ddl_cache_scope("USE test", mode), SessionOnly);
        assert_eq!(
            ddl_cache_scope("ALTER SYSTEM CHECKPOINT", mode),
            SessionOnly
        );
        assert_eq!(
            ddl_cache_scope("ALTER SYSTEM FLUSH SQL_CACHE", mode),
            SessionOnly,
            "flush drives its own cache maintenance in the executor"
        );
        // Plain DML / queries leave every cache alone.
        for sql in [
            "SELECT * FROM t",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "-- comment\nSELECT 1",
        ] {
            assert_eq!(ddl_cache_scope(sql, mode), None, "{sql}");
        }
        assert_eq!(
            ddl_cache_scope("CREATE /* comment */ TABLE t(a INT)", mode),
            TableScoped
        );
        assert_eq!(
            ddl_cache_scope("/*!80000 CREATE TABLE t(a INT) */", mode),
            TableScoped
        );
        assert_eq!(
            ddl_cache_scope("/*!99999 CREATE TABLE t(a INT) */", mode),
            None
        );
        assert_eq!(ddl_cache_scope("--x CREATE TABLE t(a INT)", mode), None);
    }

    #[test]
    fn binlog_heartbeat_uses_normalized_session_variable_name() {
        let mut ctx = crate::engine::api::SessionContext::default();
        ctx.set_user_variable(
            "@master_heartbeat_period",
            crate::types::Value::UInt(100_000_000),
        );
        assert_eq!(
            heartbeat_interval_from_session(&ctx),
            std::time::Duration::from_millis(100)
        );
    }

    #[test]
    fn load_local_sniff_uses_shared_comment_rules() {
        let mode = crate::sql_lex::SqlLexMode::default();
        assert!(looks_like_load_data_local(
            "/* lead */ LOAD /* gap */ DATA /* gap */ LOCAL INFILE '/tmp/x' INTO TABLE t",
            mode
        ));
        assert!(looks_like_load_data_local(
            "/*!80000 LOAD DATA LOCAL INFILE '/tmp/x' INTO TABLE t */",
            mode
        ));
        assert!(!looks_like_load_data_local(
            "/*!99999 LOAD DATA LOCAL INFILE '/tmp/x' INTO TABLE t */",
            mode
        ));
        assert!(!looks_like_load_data_local(
            "--x LOAD DATA LOCAL INFILE '/tmp/x' INTO TABLE t",
            mode
        ));
    }

    #[test]
    fn count_placeholders_skips_literals_and_comments() {
        let mode = crate::sql_lex::SqlLexMode::default();
        let count = |sql: &str| count_placeholders(sql, mode).unwrap();
        assert_eq!(count("SELECT ?"), 1);
        assert_eq!(count("SELECT ?, ?, ?"), 3);
        // `?` inside a single-quoted string literal is not a parameter.
        assert_eq!(
            count("SELECT id FROM t WHERE name=? AND note='lit?eral'"),
            1
        );
        // Doubled quotes inside a string are literal quotes, not terminators.
        assert_eq!(count("SELECT 'a''?''b', ?"), 1);
        // Double-quoted string literal (MySQL default) hides the `?`.
        assert_eq!(count("SELECT \"x?y\", ?"), 1);
        // Backtick-quoted identifier hides the `?`.
        assert_eq!(count("SELECT `a?b`, ?"), 1);
        // Line comments (`-- ` and `#`) hide the `?`.
        assert_eq!(count("SELECT ? -- ignore ?\n, ?"), 2);
        assert_eq!(count("SELECT ? # ignore ?\n"), 1);
        // Block comment hides the `?`.
        assert_eq!(count("SELECT ? /* ? ? */, ?"), 2);
        // Backslash-escaped quote keeps us inside the string.
        assert_eq!(count("SELECT 'a\\'?', ?"), 1);
        assert!(count_placeholders("SELECT ? /* trailing ?", mode).is_err());
        // `--x` (no whitespace) is the minus operator, not a comment.
        assert_eq!(count("SELECT 1--?\n, ?"), 2);

        let active = crate::sql_lex::PreparedSql::new(
            "SELECT /*!80000 ? */, /* ? */, /*!99999 ? */ ?",
            mode,
            crate::parser::MYSQL_VERSION_ID,
        )
        .unwrap();
        assert_eq!(count(active.effective()), 2);
    }

    #[test]
    fn audit13_sequence_calls_stay_off_the_async_inline_lane() {
        for sql in [
            "SELECT NEXTVAL('s')",
            "SELECT SETVAL('s',1)",
            "SELECT CURRVAL('s')",
            "SELECT LASTVAL()",
        ] {
            assert!(
                !statement_is_lock_safe_read(&parse_statement(sql).unwrap()),
                "{sql}"
            );
        }
    }

    #[test]
    fn audit13_fallback_binding_honors_no_backslash_escapes() {
        for no_backslash_escapes in [false, true] {
            let mode = crate::sql_lex::SqlLexMode {
                no_backslash_escapes,
                ..Default::default()
            };
            let value = "quote' and backslash\\ and nul\0";
            let bound =
                bind_sql_params("SELECT ?", &[StmtParamValue::String(value.into())], mode).unwrap();
            let bits = if no_backslash_escapes {
                crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES
            } else {
                0
            };
            let _mode = crate::parser::ParserSqlModeGuard::activate(bits);
            let Statement::Select(select) = crate::parser::parse_statement(&bound).unwrap() else {
                panic!("SELECT")
            };
            let crate::parser::SelectItem::Expr {
                expr: Expr::Literal(crate::parser::Literal::String(actual)),
                ..
            } = &select.projection[0]
            else {
                panic!("string literal")
            };
            assert_eq!(actual, value);
        }
    }

    #[test]
    fn fallback_binding_uses_the_same_comment_scanner() {
        let mode = crate::sql_lex::SqlLexMode::default();
        let prepared = crate::sql_lex::PreparedSql::new(
            "SELECT ? /* ? */, /*!80000 ? */, /*!99999 ? */ 1",
            mode,
            crate::parser::MYSQL_VERSION_ID,
        )
        .unwrap();
        let bound = bind_sql_params(
            prepared.effective(),
            &[StmtParamValue::Int(10), StmtParamValue::Int(20)],
            mode,
        )
        .unwrap();
        assert!(bound.contains("10 /* ? */"), "{bound}");
        assert!(bound.contains("20"), "{bound}");
        assert!(
            !bound.contains("99999"),
            "future wrapper must be masked: {bound}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn com_stmt_prepare_comment_parameter_metadata_matches_effective_sql() {
        use tokio::net::{TcpListener, TcpStream};

        use crate::engine::session_registry::SessionRegistry;
        use crate::mysql::packet::{read_packet, write_packet};
        use crate::mysql::protocol::{CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION};

        struct PrepareEngine;
        impl QueryEngine for PrepareEngine {
            fn execute(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
            ) -> Result<QueryResponse, QueryError> {
                Ok(QueryResponse::Ok)
            }
        }

        async fn prepare_count(client: &mut TcpStream, sql: &str) -> u16 {
            let mut command = vec![COM_STMT_PREPARE];
            command.extend_from_slice(sql.as_bytes());
            write_packet(client, 0, &command).await.unwrap();
            let ok = read_packet(client).await.unwrap().unwrap();
            assert_eq!(ok.payload[0], 0x00, "{sql}: {:?}", ok.payload);
            let columns = u16::from_le_bytes(ok.payload[5..7].try_into().unwrap());
            let params = u16::from_le_bytes(ok.payload[7..9].try_into().unwrap());
            for _ in 0..params {
                let _ = read_packet(client).await.unwrap().unwrap();
            }
            if params > 0 {
                let _ = read_packet(client).await.unwrap().unwrap();
            }
            for _ in 0..columns {
                let _ = read_packet(client).await.unwrap().unwrap();
            }
            if columns > 0 {
                let _ = read_packet(client).await.unwrap().unwrap();
            }
            params
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let mut session = Session::new(
                1,
                peer_addr,
                stream,
                None,
                Arc::new(PrepareEngine),
                Arc::new(SessionRegistry::new()),
            );
            let _ = session.run().await;
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let _handshake = read_packet(&mut client).await.unwrap().unwrap();
        let flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION;
        let mut response = Vec::new();
        response.extend_from_slice(&flags.to_le_bytes());
        response.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
        response.push(crate::charset::UTF8MB4_0900_AI_CI_ID as u8);
        response.extend_from_slice(&[0_u8; 23]);
        response.extend_from_slice(b"root\0");
        response.push(0);
        write_packet(&mut client, 1, &response).await.unwrap();
        let auth_ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(auth_ok.payload[0], 0x00);

        assert_eq!(prepare_count(&mut client, "SELECT ? /* ? */").await, 1);
        assert_eq!(prepare_count(&mut client, "SELECT /*!80000 ? */").await, 1);
        assert_eq!(
            prepare_count(&mut client, "SELECT 1 /*!99999 , ? */").await,
            0
        );
        assert_eq!(
            prepare_count(&mut client, "SELECT 1 /*+ UNKNOWN(?) */").await,
            0
        );

        drop(client);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), server).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn com_set_option_enables_multi_statements_without_handshake_capability() {
        use tokio::net::{TcpListener, TcpStream};

        use crate::engine::session_registry::SessionRegistry;
        use crate::mysql::packet::{read_packet, write_packet};
        use crate::mysql::protocol::{
            CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION, COM_QUERY, COM_SET_OPTION,
        };

        struct MultiStatementOptionEngine;

        impl QueryEngine for MultiStatementOptionEngine {
            fn execute(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
            ) -> Result<QueryResponse, QueryError> {
                Ok(QueryResponse::Ok)
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let mut session = Session::new(
                1,
                peer_addr,
                stream,
                None,
                Arc::new(MultiStatementOptionEngine),
                Arc::new(SessionRegistry::new()),
            );
            let _ = session.run().await;
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let _handshake = read_packet(&mut client).await.unwrap().unwrap();
        // Connector/J does not request CLIENT_MULTI_STATEMENTS unless
        // allowMultiQueries=true. rewriteBatchedStatements instead toggles
        // the capability around its internally generated batch.
        let flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_MULTI_RESULTS;
        let mut response = Vec::new();
        response.extend_from_slice(&flags.to_le_bytes());
        response.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
        response.push(crate::charset::UTF8MB4_0900_AI_CI_ID as u8);
        response.extend_from_slice(&[0_u8; 23]);
        response.extend_from_slice(b"root\0");
        response.push(0);
        write_packet(&mut client, 1, &response).await.unwrap();
        let auth_ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(auth_ok.payload[0], 0x00);

        let multi_query = {
            let mut command = vec![COM_QUERY];
            command.extend_from_slice(b"INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)");
            command
        };

        write_packet(&mut client, 0, &multi_query).await.unwrap();
        let disabled_error = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(disabled_error.payload[0], 0xff);

        write_packet(&mut client, 0, &[COM_SET_OPTION, 0, 0])
            .await
            .unwrap();
        let enabled_ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(enabled_ok.payload[0], 0x00);

        write_packet(&mut client, 0, &multi_query).await.unwrap();
        let first_batch_ok = read_packet(&mut client).await.unwrap().unwrap();
        let second_batch_ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(first_batch_ok.sequence_id, 1);
        assert_eq!(second_batch_ok.sequence_id, 2);
        assert_eq!(first_batch_ok.payload[0], 0x00);
        assert_eq!(second_batch_ok.payload[0], 0x00);
        let first_status = u16::from_le_bytes(first_batch_ok.payload[3..5].try_into().unwrap());
        let second_status = u16::from_le_bytes(second_batch_ok.payload[3..5].try_into().unwrap());
        assert_ne!(first_status & SERVER_MORE_RESULTS_EXISTS, 0);
        assert_eq!(second_status & SERVER_MORE_RESULTS_EXISTS, 0);

        write_packet(&mut client, 0, &[COM_SET_OPTION, 1, 0])
            .await
            .unwrap();
        let disabled_ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(disabled_ok.payload[0], 0x00);

        write_packet(&mut client, 0, &multi_query).await.unwrap();
        let disabled_again_error = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(disabled_again_error.payload[0], 0xff);

        drop(client);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), server).await;
    }

    #[test]
    fn rewritten_order_line_prepared_batch_binds_each_row_separately() {
        let sql = "INSERT INTO bmsql_order_line (\
            ol_o_id, ol_d_id, ol_w_id, ol_number, \
            ol_i_id, ol_supply_w_id, ol_quantity, \
            ol_amount, ol_dist_info) VALUES \
            (?, ?, ?, ?, ?, ?, ?, ?, ?),\
            (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        let template = parse_prepared_statement_template(sql, 18).unwrap();
        let statement = bind_prepared_statement(
            &template,
            &[
                StmtParamValue::Int(3001),
                StmtParamValue::Int(1),
                StmtParamValue::Int(1),
                StmtParamValue::Int(1),
                StmtParamValue::Int(101),
                StmtParamValue::Int(1),
                StmtParamValue::Int(5),
                StmtParamValue::Double(12.34),
                StmtParamValue::String("dist-01".into()),
                StmtParamValue::Int(3001),
                StmtParamValue::Int(1),
                StmtParamValue::Int(1),
                StmtParamValue::Int(2),
                StmtParamValue::Int(102),
                StmtParamValue::Int(1),
                StmtParamValue::Int(6),
                StmtParamValue::Double(23.45),
                StmtParamValue::String("dist-01".into()),
            ],
        )
        .unwrap();

        let Statement::Insert(insert) = statement else {
            panic!("expected insert");
        };
        let InsertSource::Values(rows) = insert.source else {
            panic!("expected values source");
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][3], Expr::Literal(Literal::Int(1)));
        assert_eq!(rows[1][3], Expr::Literal(Literal::Int(2)));
    }

    /// Statement classifier used by `execute_prepared_statement_blocking`
    /// to decide between inline-on-runtime-worker and `spawn_blocking`.
    /// Misclassifying a row-locking statement as "lock-safe read"
    /// produced the TPC-C deadlock that motivated this dispatcher.
    /// Misclassifying a SELECT as not-safe falls back to `spawn_blocking`
    /// per query and erases the OLTP-read latency gain. Both directions
    /// matter, so pin the table.
    #[test]
    fn statement_is_lock_safe_read_classifies_each_variant() {
        // Lock-safe: pure SELECT and SHOW family.
        for sql in [
            "SELECT 1",
            "SELECT id FROM t WHERE id = 1",
            "SELECT id FROM t ORDER BY id LIMIT 10",
            "SELECT id FROM t WHERE id IN (SELECT id FROM t)",
            "EXPLAIN SELECT id FROM t WHERE id = 1",
            "WITH x AS (SELECT 1 AS v) SELECT v FROM x",
            "SHOW DATABASES",
            "SHOW TABLES",
            "SHOW COLUMNS FROM t",
            "SHOW INDEX FROM t",
            "SHOW CREATE TABLE t",
            "SHOW ENGINES",
            "SHOW PROCESSLIST",
            "SHOW GRANTS",
            "DESCRIBE t",
        ] {
            let statement = parse_statement(sql).expect(sql);
            assert!(
                statement_is_lock_safe_read(&statement),
                "expected `{}` to be classified lock-safe",
                sql
            );
        }

        // NOT lock-safe: any DML, DDL, lock-clause SELECT, or txn
        // control statement. Each of these can park the executor on a
        // row-lock condvar (or take latches that should not block a
        // tokio worker).
        for sql in [
            "SELECT id FROM t WHERE id = 1 FOR UPDATE",
            "SELECT id FROM t WHERE id = 1 FOR SHARE",
            "SELECT SLEEP(1)",
            "SELECT COUNT(*) OVER (ORDER BY id) FROM t",
            "SELECT SUM(id) OVER (ORDER BY id RANGE BETWEEN 100 PRECEDING AND 100 FOLLOWING) FROM t",
            "SELECT * FROM (SELECT ROW_NUMBER() OVER () AS rn FROM t) AS x",
            "WITH x AS (SELECT ROW_NUMBER() OVER () AS rn FROM t) SELECT * FROM x",
            "SELECT id FROM t WHERE SLEEP(1) = 0",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET v = 1 WHERE id = 1",
            "DELETE FROM t WHERE id = 1",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT s",
            "ROLLBACK TO SAVEPOINT s",
            "CREATE TABLE x (id INT PRIMARY KEY)",
            "DROP TABLE x",
            "TRUNCATE TABLE x",
            "CREATE INDEX ix ON t (v)",
            "DROP INDEX ix ON t",
            "ANALYZE TABLE t",
            "USE other_db",
        ] {
            let statement = parse_statement(sql).expect(sql);
            assert!(
                !statement_is_lock_safe_read(&statement),
                "expected `{}` to NOT be classified lock-safe",
                sql
            );
        }
    }

    #[test]
    fn locking_prepared_select_uses_nonblocking_transactional_inline_retry() {
        let mut context = SessionContext::default();
        context.current_database.clear();
        assert!(prepared_dml_inline_allowed(&context));
        context.explicit_txn = true;
        assert!(
            !prepared_dml_inline_allowed(&context),
            "BEGIN without a lazily-created storage txn is still transactional"
        );

        for sql in [
            "SELECT id FROM t WHERE id = ? FOR UPDATE",
            "SELECT id FROM t WHERE id = ? FOR SHARE",
            "SELECT id FROM t WHERE id = ? FOR UPDATE NOWAIT",
            "SELECT id FROM t WHERE id = ? FOR UPDATE SKIP LOCKED",
        ] {
            let statement = parse_statement(sql).expect(sql);
            assert!(
                !prepared_dml_statement_allows_inline_execution(&statement),
                "locking prepared SELECT must run once on the blocking pool: {sql}"
            );
        }

        // All three write kinds compile to the same savepoint-protected
        // vehicle, so all three may attempt the non-blocking inline pass
        // before falling back to the pool.
        for sql in [
            "INSERT INTO t VALUES (?)",
            "UPDATE t SET v = ? WHERE id = ?",
            "DELETE FROM t WHERE id = ?",
            "REPLACE INTO t VALUES (?)",
        ] {
            let statement = parse_statement(sql).expect(sql);
            assert!(
                prepared_dml_statement_allows_inline_execution(&statement),
                "transactional DML should retain the uncontended inline fast path: {sql}"
            );
            assert!(
                prepared_dml_transactional_inline_retry_allowed(&statement),
                "transactional DML should attempt the non-blocking inline pass: {sql}"
            );
        }

        // Plain reads retain their compiled SELECT route. Exact locking reads
        // use the savepoint-protected prepared-DML vehicle: they may acquire an
        // uncontended row inline, but the nonblocking wait marker sends a real
        // conflict to the blocking pool instead of parking a runtime worker.
        let statement = parse_statement("SELECT id FROM t WHERE id = ?").unwrap();
        assert!(!prepared_dml_transactional_inline_retry_allowed(&statement));
        for sql in [
            "SELECT id FROM t WHERE id = ? FOR UPDATE",
            "SELECT id FROM t WHERE id = ? FOR SHARE",
            "SELECT id FROM t WHERE id = ? FOR UPDATE NOWAIT",
            "SELECT id FROM t WHERE id = ? FOR UPDATE SKIP LOCKED",
        ] {
            let statement = parse_statement(sql).expect(sql);
            assert!(
                prepared_dml_transactional_inline_retry_allowed(&statement),
                "locking SELECT should use the nonblocking inline-retry ladder: {sql}"
            );
        }
    }

    #[tokio::test]
    async fn declined_read_dml_probe_does_not_acquire_transaction_admission() {
        struct DecliningEngine;
        impl crate::engine::api::QueryEngine for DecliningEngine {
            fn execute(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
            ) -> Result<QueryResponse, QueryError> {
                Ok(QueryResponse::Ok)
            }
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (client, accepted) = tokio::join!(
            tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept(),
        );
        let _client = client.unwrap();
        let (stream, peer) = accepted.unwrap();
        let mut session = Session::try_new(
            1,
            peer,
            stream,
            None,
            Arc::new(DecliningEngine),
            Arc::new(SessionRegistry::new()),
        )
        .unwrap();
        session.context.autocommit = false;
        for sql in [
            "SELECT COUNT(*) FROM t WHERE id IN (SELECT id FROM u)",
            "WITH q AS (SELECT id FROM t) SELECT COUNT(*) FROM q",
            "SELECT SLEEP(0)",
        ] {
            let (_, result, _) = session
                .try_execute_prepared_dml_blocking(
                    Arc::from(sql),
                    Arc::new(parse_statement(sql).unwrap()),
                    crate::engine::api::PreparedDmlState::default(),
                    Vec::new(),
                    crate::engine::perf_events::prepared_statement_texts(sql),
                )
                .await;
            assert!(result.is_none());
            assert!(
                session.txn_admission.is_none(),
                "an ineligible DML probe must not admit a read-only statement: {sql}"
            );
        }
        for sql in [
            "UPDATE t SET v = 1 WHERE id = 1",
            "SELECT id FROM t FOR SHARE",
        ] {
            let (_, result, _) = session
                .try_execute_prepared_dml_blocking(
                    Arc::from(sql),
                    Arc::new(parse_statement(sql).unwrap()),
                    crate::engine::api::PreparedDmlState::default(),
                    Vec::new(),
                    crate::engine::perf_events::prepared_statement_texts(sql),
                )
                .await;
            assert!(result.is_none());
            if crate::util::exec_permit::global().is_some() {
                assert!(
                    session.txn_admission.is_some(),
                    "locking fallback still needs admission"
                );
            }
            session.release_txn_admission_if_idle();
        }
    }

    #[test]
    fn simple_session_set_sniff_accepts_only_plain_session_assignments() {
        for sql in [
            "SET SQL_SELECT_LIMIT=1",
            "set sql_select_limit = DEFAULT;",
            "SET NAMES 'utf8mb4'",
            "SET @@session.autocommit = 0",
            "SET @a = 1, sql_mode = 'ANSI_QUOTES'",
        ] {
            assert!(is_simple_session_set_sql(sql), "{sql}");
        }
        for sql in [
            "SETTINGS",
            "SET",
            "SET GLOBAL sql_select_limit = 1",
            "SET @@global.max_connections = 10",
            "SET PERSIST max_connections = 10",
            "SET PASSWORD = 'x'",
            "SET DEFAULT ROLE ALL TO u",
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
            "SET @a = (SELECT 1)",
            "SET @a = SLEEP(1)",
            "SET @a = 1; SELECT 1",
            "SET /* c */ @a = 1",
            "SELECT 1",
        ] {
            assert!(!is_simple_session_set_sql(sql), "{sql}");
        }
    }

    /// Sniff guard for the COM_QUERY inline route. False positives
    /// only cost a parser-cache lookup (the AST classifier blocks unsafe
    /// statements anyway), but every false negative falls back to
    /// `spawn_blocking` and erases the latency win, so pin both
    /// directions.
    #[test]
    fn looks_like_inline_select_candidate_accepts_plain_select() {
        for sql in [
            "SELECT 1",
            "SELECT 2",
            "select 1",
            "  SELECT 1",
            "\tSELECT 1",
            "/* hint */ SELECT 1",
            "-- a comment\nSELECT 1",
            "SELECT id FROM t WHERE id = 1",
            "SELECT @@version",
            "SELECT  @@version_comment",
            "SELECT id FROM t WHERE id = 1 FOR UPDATE",
        ] {
            assert!(
                looks_like_inline_select_candidate(sql),
                "expected `{}` to be sniff-positive",
                sql
            );
        }
    }

    #[test]
    fn looks_like_inline_select_candidate_rejects_non_select_and_special_forms() {
        for sql in [
            "",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET v = 1",
            "DELETE FROM t",
            "BEGIN",
            "COMMIT",
            "SET autocommit = 0",
            "USE other_db",
            "SHOW VARIABLES LIKE 'mydb_%'",
            "SHOW TABLES",
            "DESCRIBE t",
            "EXPLAIN SELECT 1",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "SELECTOR",
            "/* unterminated",
            "-- only a comment",
        ] {
            assert!(
                !looks_like_inline_select_candidate(sql),
                "expected `{}` to be sniff-negative",
                sql
            );
        }
    }

    #[test]
    fn text_dml_admission_sniff_is_narrow_and_comment_aware() {
        let mode = crate::sql_lex::SqlLexMode::default();
        for sql in [
            "INSERT INTO t VALUES (1)",
            "replace into t values (1)",
            " UPDATE t SET v = 1",
            "-- comment\nDELETE FROM t",
            "/* import */ LOAD DATA INFILE '/tmp/a' INTO TABLE t",
        ] {
            assert!(
                looks_like_text_dml_candidate(sql, mode),
                "expected `{sql}` to require admission"
            );
        }
        for sql in [
            "SELECT 1",
            "SHOW TABLES",
            "BEGIN",
            "COMMIT",
            "CREATE TABLE t (id INT)",
            "UPDATED_STATS",
            "-- only a comment",
        ] {
            assert!(
                !looks_like_text_dml_candidate(sql, mode),
                "expected `{sql}` to bypass DML admission"
            );
        }
    }

    #[test]
    fn large_text_update_inline_sniff_checks_size_and_keyword_only() {
        let payload = "x".repeat(64 * 1024);
        assert!(looks_like_large_text_update_candidate(&format!(
            "UPDATE t SET b='{payload}' WHERE id=1"
        )));
        assert!(!looks_like_large_text_update_candidate(&format!(
            "INSERT INTO t VALUES ('{payload}')"
        )));
        assert!(!looks_like_large_text_update_candidate(
            "UPDATE t SET b='small' WHERE id=1"
        ));
    }

    #[test]
    fn detects_mysql_client_sandbox_probe_exactly() {
        for sql in ["select $$", " SELECT $$; ", "SeLeCt $$"] {
            assert!(is_mysql_client_sandbox_probe(sql));
        }

        for sql in ["select 1", "select $$$", "select '$$'", "select $$ + 1"] {
            assert!(!is_mysql_client_sandbox_probe(sql));
        }
    }

    #[test]
    fn parses_com_process_kill_payload_connection_id() {
        assert_eq!(COM_PROCESS_KILL, 0x0c);
        assert_eq!(
            parse_process_kill_connection_id(&42_u32.to_le_bytes()),
            Some(42)
        );
        assert_eq!(parse_process_kill_connection_id(&[1, 2, 3]), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn administrator_reserve_is_checked_after_authentication() {
        use tokio::net::{TcpListener, TcpStream};

        use crate::engine::api::{QueryEngine, QueryResponse, SessionContext};
        use crate::engine::auth::{Privilege, PrivilegeScope};
        use crate::engine::session_registry::SessionRegistry;
        use crate::mysql::packet::{read_packet, write_packet};
        use crate::mysql::protocol::{CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION};
        use crate::types::QueryError;

        struct ReservedConnectionEngine {
            allow_connection_admin: bool,
        }

        impl QueryEngine for ReservedConnectionEngine {
            fn execute(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
            ) -> Result<QueryResponse, QueryError> {
                Ok(QueryResponse::Ok)
            }

            fn authorize_global_privilege(
                &self,
                _session: &SessionContext,
                privilege: Privilege,
            ) -> Result<(), QueryError> {
                if self.allow_connection_admin {
                    Ok(())
                } else {
                    Err(crate::engine::auth::privilege_denied(
                        &privilege,
                        &PrivilegeScope::Global,
                    ))
                }
            }
        }

        for allow_connection_admin in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (stream, peer_addr) = listener.accept().await.unwrap();
                let mut session = Session::new(
                    1,
                    peer_addr,
                    stream,
                    None,
                    Arc::new(ReservedConnectionEngine {
                        allow_connection_admin,
                    }),
                    Arc::new(SessionRegistry::new()),
                );
                session.require_connection_admin(1);
                session.run().await
            });

            let mut client = TcpStream::connect(addr).await.unwrap();
            let handshake = read_packet(&mut client).await.unwrap().unwrap();
            assert_eq!(handshake.sequence_id, 0);

            let mut response = Vec::new();
            let flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION;
            response.extend_from_slice(&flags.to_le_bytes());
            response.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
            response.push(crate::charset::UTF8MB4_0900_AI_CI_ID as u8);
            response.extend_from_slice(&[0_u8; 23]);
            response.extend_from_slice(b"root\0");
            response.push(0);
            write_packet(&mut client, 1, &response).await.unwrap();

            let result = read_packet(&mut client).await.unwrap().unwrap();
            if allow_connection_admin {
                assert_eq!(result.payload[0], 0x00, "administrator should receive OK");
                drop(client);
            } else {
                assert_eq!(result.payload[0], 0xff, "ordinary user should receive ERR");
                assert_eq!(
                    u16::from_le_bytes([result.payload[1], result.payload[2]]),
                    1040
                );
            }
            let session_result = tokio::time::timeout(std::time::Duration::from_secs(1), server)
                .await
                .expect("reserved connection session did not finish")
                .expect("reserved connection session panicked");
            if allow_connection_admin {
                let _ = session_result;
            } else {
                assert!(session_result.is_err());
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn idle_session_observes_global_shutdown_notification() {
        use std::time::Duration;

        use tokio::net::{TcpListener, TcpStream};

        use crate::engine::api::{QueryEngine, QueryResponse, SessionContext};
        use crate::engine::session_registry::SessionRegistry;
        use crate::mysql::packet::{read_packet, write_packet};
        use crate::mysql::protocol::{CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION};
        use crate::mysql::shutdown::{ShutdownIntent, ShutdownSource};
        use crate::types::QueryError;

        struct ShutdownTestEngine;

        impl QueryEngine for ShutdownTestEngine {
            fn execute(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
            ) -> Result<QueryResponse, QueryError> {
                Ok(QueryResponse::Ok)
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let registry = Arc::new(SessionRegistry::new());
        let server_registry = Arc::clone(&registry);
        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let mut session = Session::new(
                1,
                peer_addr,
                stream,
                None,
                Arc::new(ShutdownTestEngine),
                server_registry,
            );
            session.run().await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let handshake = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(handshake.sequence_id, 0);

        let mut response = Vec::new();
        let flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION;
        response.extend_from_slice(&flags.to_le_bytes());
        response.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
        response.push(crate::charset::UTF8MB4_0900_AI_CI_ID as u8);
        response.extend_from_slice(&[0_u8; 23]);
        response.extend_from_slice(b"root\0");
        response.push(0);
        write_packet(&mut client, 1, &response).await.unwrap();

        let auth_ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(auth_ok.payload[0], 0x00, "expected OK header after auth");
        assert_eq!(registry.snapshot().len(), 1);

        crate::mysql::shutdown::request_shutdown(
            ShutdownSource::Signal("test"),
            ShutdownIntent::Halt,
        );

        let session_result = tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("idle session did not stop after global shutdown notification")
            .expect("session task panicked");
        session_result.unwrap();
        assert!(registry.snapshot().is_empty());

        let eof = tokio::time::timeout(Duration::from_secs(1), read_packet(&mut client))
            .await
            .expect("client socket did not close")
            .unwrap();
        assert!(eof.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handshake_switches_to_account_auth_plugin() {
        use std::sync::Mutex;
        use tokio::net::{TcpListener, TcpStream};

        use crate::engine::api::{QueryEngine, QueryResponse, SessionContext};
        use crate::engine::auth::{
            AccountKey, AuthOutcome, AuthRequest, AuthorizationContext, ResourceLimits,
        };
        use crate::engine::session_registry::SessionRegistry;
        use crate::mysql::packet::{read_packet, write_packet};
        use crate::mysql::protocol::{
            CLIENT_PLUGIN_AUTH, CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION,
        };
        use crate::types::QueryError;

        #[derive(Default)]
        struct AuthSwitchTestEngine {
            seen: Mutex<Option<(Option<String>, Vec<u8>)>>,
        }

        impl QueryEngine for AuthSwitchTestEngine {
            fn execute(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
            ) -> Result<QueryResponse, QueryError> {
                Ok(QueryResponse::Ok)
            }

            fn auth_plugin_for_user(
                &self,
                _username: &str,
                _host: &str,
            ) -> Result<Option<String>, QueryError> {
                Ok(Some("mysql_native_password".to_string()))
            }

            fn authenticate_request(
                &self,
                request: AuthRequest<'_>,
            ) -> Result<AuthOutcome, QueryError> {
                *self.seen.lock().unwrap() = Some((
                    request.auth_plugin.map(str::to_string),
                    request.auth_response.to_vec(),
                ));
                Ok(AuthOutcome {
                    context: AuthorizationContext {
                        authenticated_as: AccountKey::new(request.username, request.host),
                        active_roles: Default::default(),
                        epoch: 0,
                    },
                    password_expired: false,
                    account_locked: false,
                    resource_limits: ResourceLimits::default(),
                    current_user_is_system: false,
                })
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = Arc::new(AuthSwitchTestEngine::default());
        let registry = Arc::new(SessionRegistry::new());

        let server_engine = Arc::clone(&engine);
        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let mut session = Session::new(1, peer_addr, stream, None, server_engine, registry);
            let _ = session.run().await;
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let handshake = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(handshake.sequence_id, 0);

        let mut hsr = Vec::new();
        let flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
        hsr.extend_from_slice(&flags.to_le_bytes());
        hsr.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
        hsr.push(crate::charset::UTF8MB4_0900_AI_CI_ID as u8);
        hsr.extend_from_slice(&[0_u8; 23]);
        hsr.extend_from_slice(b"native\0");
        hsr.push(0);
        hsr.extend_from_slice(b"caching_sha2_password\0");
        write_packet(&mut client, 1, &hsr).await.unwrap();

        let switch = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(switch.sequence_id, 2);
        assert_eq!(switch.payload[0], 0xfe);
        assert!(switch.payload[1..].starts_with(b"mysql_native_password\0"));

        write_packet(&mut client, 3, b"native-token").await.unwrap();
        let ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(ok.sequence_id, 4);
        assert_eq!(ok.payload[0], 0x00);

        let seen = engine.seen.lock().unwrap().clone().unwrap();
        assert_eq!(seen.0.as_deref(), Some("mysql_native_password"));
        assert_eq!(seen.1, b"native-token");

        drop(client);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), server).await;
    }

    /// Pins the `caching_sha2_password` fast-path handshake shape:
    /// after the client's HandshakeResponse the server MUST issue
    /// `AuthMoreData([0x01, 0x03])` (fast_auth_success) followed by an
    /// OK packet whenever the hash-path verify succeeds. This is the
    /// documented MySQL 8.0 cache-hit shape; the cache-miss /
    /// `perform_full_auth(0x04)` path is exercised only when the
    /// initial verify fails. Sending perform_full_auth unconditionally
    /// would break mysql CLI over non-TLS without
    /// `--get-server-public-key` (ER 2061).
    #[tokio::test(flavor = "current_thread")]
    async fn handshake_caching_sha2_emits_fast_success() {
        use tokio::net::{TcpListener, TcpStream};

        use crate::engine::api::{QueryEngine, QueryResponse, SessionContext};
        use crate::engine::auth::{
            AccountKey, AuthOutcome, AuthRequest, AuthorizationContext, ResourceLimits,
        };
        use crate::engine::session_registry::SessionRegistry;
        use crate::mysql::packet::{read_packet, write_packet};
        use crate::mysql::protocol::{
            CLIENT_PLUGIN_AUTH, CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION,
        };
        use crate::types::QueryError;

        struct CachingSha2Engine;
        impl QueryEngine for CachingSha2Engine {
            fn execute(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
            ) -> Result<QueryResponse, QueryError> {
                Ok(QueryResponse::Ok)
            }
            fn auth_plugin_for_user(
                &self,
                _username: &str,
                _host: &str,
            ) -> Result<Option<String>, QueryError> {
                Ok(Some("caching_sha2_password".to_string()))
            }
            fn authenticate_request(
                &self,
                request: AuthRequest<'_>,
            ) -> Result<AuthOutcome, QueryError> {
                Ok(AuthOutcome {
                    context: AuthorizationContext {
                        authenticated_as: AccountKey::new(request.username, request.host),
                        active_roles: Default::default(),
                        epoch: 0,
                    },
                    password_expired: false,
                    account_locked: false,
                    resource_limits: ResourceLimits::default(),
                    current_user_is_system: false,
                })
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = Arc::new(CachingSha2Engine);
        let registry = Arc::new(SessionRegistry::new());

        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let mut session = Session::new(1, peer_addr, stream, None, engine, registry);
            let _ = session.run().await;
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let handshake = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(handshake.sequence_id, 0);

        let flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
        let mut hsr = Vec::new();
        hsr.extend_from_slice(&flags.to_le_bytes());
        hsr.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
        hsr.push(crate::charset::UTF8MB4_0900_AI_CI_ID as u8);
        hsr.extend_from_slice(&[0_u8; 23]);
        hsr.extend_from_slice(b"root\0");
        // 32-byte scramble — content doesn't matter, the stub engine
        // accepts anything.
        hsr.push(32);
        hsr.extend_from_slice(&[0xab_u8; 32]);
        hsr.extend_from_slice(b"caching_sha2_password\0");
        write_packet(&mut client, 1, &hsr).await.unwrap();

        // The stub engine returns Ok unconditionally — the hash-path
        // verify "succeeds" — so the next server-bound packet must be
        // AuthMoreData([0x01, 0x03]) (fast_auth_success) at seq 2.
        let auth_more = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(
            auth_more.sequence_id, 2,
            "AuthMoreData must come right after HandshakeResponse"
        );
        assert_eq!(
            auth_more.payload,
            vec![0x01, 0x03],
            "fast_auth_success marker must be [0x01, 0x03]; got {:02x?}",
            auth_more.payload
        );

        drop(client);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), server).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn caching_sha2_empty_password_returns_final_packet() {
        use tokio::net::{TcpListener, TcpStream};

        use crate::engine::api::{QueryEngine, QueryResponse, SessionContext};
        use crate::engine::auth::{
            AccountKey, AuthOutcome, AuthRequest, AuthorizationContext, ResourceLimits,
        };
        use crate::engine::session_registry::SessionRegistry;
        use crate::mysql::packet::{read_packet, write_packet};
        use crate::mysql::protocol::{
            CLIENT_PLUGIN_AUTH, CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION,
        };
        use crate::types::QueryError;

        struct CachingSha2Engine;
        impl QueryEngine for CachingSha2Engine {
            fn execute(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
            ) -> Result<QueryResponse, QueryError> {
                Ok(QueryResponse::Ok)
            }
            fn auth_plugin_for_user(
                &self,
                _username: &str,
                _host: &str,
            ) -> Result<Option<String>, QueryError> {
                Ok(Some("caching_sha2_password".to_string()))
            }
            fn authenticate_request(
                &self,
                request: AuthRequest<'_>,
            ) -> Result<AuthOutcome, QueryError> {
                assert!(matches!(request.auth_response, [] | [0]));
                if request.username == "denied" {
                    return Err(crate::engine::auth::access_denied(
                        request.username,
                        request.host,
                    ));
                }
                Ok(AuthOutcome {
                    context: AuthorizationContext {
                        authenticated_as: AccountKey::new(request.username, request.host),
                        active_roles: Default::default(),
                        epoch: 0,
                    },
                    password_expired: false,
                    account_locked: false,
                    resource_limits: ResourceLimits::default(),
                    current_user_is_system: false,
                })
            }
        }

        for token in [vec![], vec![0]] {
            for allowed in [false, true] {
                for switch_plugin in [false, true] {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    let engine = Arc::new(CachingSha2Engine);
                    let registry = Arc::new(SessionRegistry::new());

                    let server = tokio::spawn(async move {
                        let (stream, peer_addr) = listener.accept().await.unwrap();
                        let mut session =
                            Session::new(1, peer_addr, stream, None, engine, registry);
                        let _ = session.run().await;
                    });

                    let mut client = TcpStream::connect(addr).await.unwrap();
                    let handshake = read_packet(&mut client).await.unwrap().unwrap();
                    assert_eq!(handshake.sequence_id, 0);

                    let flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
                    let mut hsr = Vec::new();
                    hsr.extend_from_slice(&flags.to_le_bytes());
                    hsr.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
                    hsr.push(crate::charset::UTF8MB4_0900_AI_CI_ID as u8);
                    hsr.extend_from_slice(&[0_u8; 23]);
                    hsr.extend_from_slice(if allowed { b"allowed\0" } else { b"denied\0" });
                    hsr.push(token.len() as u8);
                    hsr.extend_from_slice(&token);
                    hsr.extend_from_slice(if switch_plugin {
                        b"mysql_native_password\0"
                    } else {
                        b"caching_sha2_password\0"
                    });
                    write_packet(&mut client, 1, &hsr).await.unwrap();
                    let final_seq = if switch_plugin {
                        let switch = read_packet(&mut client).await.unwrap().unwrap();
                        assert_eq!(switch.payload[0], 0xfe);
                        write_packet(&mut client, 3, &token).await.unwrap();
                        4
                    } else {
                        2
                    };
                    let result = read_packet(&mut client).await.unwrap().unwrap();
                    assert_eq!(result.sequence_id, final_seq);
                    assert_eq!(result.payload[0], if allowed { 0x00 } else { 0xff });
                    if !allowed {
                        assert_eq!(
                            &result.payload[1..9],
                            &[0x15, 0x04, b'#', b'2', b'8', b'0', b'0', b'0']
                        );
                    } else {
                        // Exercise both success and rejection on an existing connection.
                        for username in ["allowed", "denied"] {
                            let mut change = vec![COM_CHANGE_USER];
                            change.extend_from_slice(username.as_bytes());
                            change.push(0);
                            change.push(token.len() as u8);
                            change.extend_from_slice(&token);
                            change.push(0); // no database
                            change.extend_from_slice(
                                &crate::charset::UTF8MB4_0900_AI_CI_ID.to_le_bytes(),
                            );
                            change.extend_from_slice(b"caching_sha2_password\0");
                            write_packet(&mut client, 0, &change).await.unwrap();
                            let result = read_packet(&mut client).await.unwrap().unwrap();
                            assert_eq!(result.sequence_id, 1);
                            assert_eq!(
                                result.payload[0],
                                if username == "allowed" { 0x00 } else { 0xff }
                            );
                            if username == "denied" {
                                let closed = tokio::time::timeout(
                                    std::time::Duration::from_secs(2),
                                    read_packet(&mut client),
                                )
                                .await
                                .expect("failed change-user must close the connection");
                                assert!(matches!(closed, Ok(None) | Err(_)));
                            }
                        }
                    }

                    drop(client);
                    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), server).await;
                }
            }
        }
    }

    /// When the AuthSwitchRequest path lands on `caching_sha2_password`
    /// (client first offered a different plugin, account uses sha2), the
    /// server must also drive the fast-success flow on a successful
    /// hash-path verify.
    #[tokio::test(flavor = "current_thread")]
    async fn handshake_caching_sha2_after_switch_emits_fast_success() {
        use tokio::net::{TcpListener, TcpStream};

        use crate::engine::api::{QueryEngine, QueryResponse, SessionContext};
        use crate::engine::auth::{
            AccountKey, AuthOutcome, AuthRequest, AuthorizationContext, ResourceLimits,
        };
        use crate::engine::session_registry::SessionRegistry;
        use crate::mysql::packet::{read_packet, write_packet};
        use crate::mysql::protocol::{
            CLIENT_PLUGIN_AUTH, CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION,
        };
        use crate::types::QueryError;

        struct Sha2AccountEngine;
        impl QueryEngine for Sha2AccountEngine {
            fn execute(
                &self,
                _s: &mut SessionContext,
                _: &str,
            ) -> Result<QueryResponse, QueryError> {
                Ok(QueryResponse::Ok)
            }
            fn auth_plugin_for_user(
                &self,
                _u: &str,
                _h: &str,
            ) -> Result<Option<String>, QueryError> {
                Ok(Some("caching_sha2_password".to_string()))
            }
            fn authenticate_request(&self, r: AuthRequest<'_>) -> Result<AuthOutcome, QueryError> {
                Ok(AuthOutcome {
                    context: AuthorizationContext {
                        authenticated_as: AccountKey::new(r.username, r.host),
                        active_roles: Default::default(),
                        epoch: 0,
                    },
                    password_expired: false,
                    account_locked: false,
                    resource_limits: ResourceLimits::default(),
                    current_user_is_system: false,
                })
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = Arc::new(Sha2AccountEngine);
        let registry = Arc::new(SessionRegistry::new());
        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let mut session = Session::new(1, peer_addr, stream, None, engine, registry);
            let _ = session.run().await;
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let _ = read_packet(&mut client).await.unwrap().unwrap();

        // Client claims mysql_native_password; server must switch to
        // caching_sha2_password (account's plugin).
        let flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
        let mut hsr = Vec::new();
        hsr.extend_from_slice(&flags.to_le_bytes());
        hsr.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
        hsr.push(crate::charset::UTF8MB4_0900_AI_CI_ID as u8);
        hsr.extend_from_slice(&[0_u8; 23]);
        hsr.extend_from_slice(b"root\0");
        hsr.push(0);
        hsr.extend_from_slice(b"mysql_native_password\0");
        write_packet(&mut client, 1, &hsr).await.unwrap();

        let switch = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(switch.sequence_id, 2);
        assert_eq!(switch.payload[0], 0xfe);
        assert!(switch.payload[1..].starts_with(b"caching_sha2_password\0"));

        // Reply to AuthSwitchRequest with a 32-byte sha2 scramble.
        write_packet(&mut client, 3, &[0xab_u8; 32]).await.unwrap();

        let auth_more = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(auth_more.sequence_id, 4);
        assert_eq!(
            auth_more.payload,
            vec![0x01, 0x03],
            "post-switch caching_sha2 must also emit fast_auth_success on hash-path verify"
        );

        drop(client);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), server).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn com_change_user_reauthenticates_and_switches_plugin() {
        use std::sync::Mutex;
        use tokio::net::{TcpListener, TcpStream};

        use crate::engine::api::{QueryEngine, QueryResponse, SessionContext};
        use crate::engine::auth::{
            AccountKey, AuthOutcome, AuthRequest, AuthorizationContext, ResourceLimits,
        };
        use crate::engine::session_registry::SessionRegistry;
        use crate::mysql::packet::{read_packet, write_packet};
        use crate::mysql::protocol::{
            CLIENT_PLUGIN_AUTH, CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION,
        };
        use crate::types::QueryError;

        #[derive(Default)]
        struct ChangeUserEngine {
            seen: Mutex<Vec<(String, Option<String>, Vec<u8>)>>,
        }

        impl QueryEngine for ChangeUserEngine {
            fn execute(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
            ) -> Result<QueryResponse, QueryError> {
                Ok(QueryResponse::Ok)
            }

            fn init_session_database(
                &self,
                context: &mut SessionContext,
                database: &str,
            ) -> Result<(), QueryError> {
                assert_eq!(context.authenticated_as.user, "native");
                assert_eq!(context.user, "native");
                assert!(
                    context.status.is_some(),
                    "CHANGE_USER must retain session counters"
                );
                context.current_database = database.into();
                Ok(())
            }

            fn auth_plugin_for_user(
                &self,
                username: &str,
                _host: &str,
            ) -> Result<Option<String>, QueryError> {
                if username == "native" {
                    Ok(Some("mysql_native_password".to_string()))
                } else {
                    Ok(Some("caching_sha2_password".to_string()))
                }
            }

            fn authenticate_request(
                &self,
                request: AuthRequest<'_>,
            ) -> Result<AuthOutcome, QueryError> {
                self.seen.lock().unwrap().push((
                    request.username.to_string(),
                    request.auth_plugin.map(str::to_string),
                    request.auth_response.to_vec(),
                ));
                Ok(AuthOutcome {
                    context: AuthorizationContext {
                        authenticated_as: AccountKey::new(request.username, request.host),
                        active_roles: Default::default(),
                        epoch: 0,
                    },
                    password_expired: false,
                    account_locked: false,
                    resource_limits: ResourceLimits::default(),
                    current_user_is_system: false,
                })
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = Arc::new(ChangeUserEngine::default());
        let registry = Arc::new(SessionRegistry::new());

        let server_engine = Arc::clone(&engine);
        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let mut session = Session::new(1, peer_addr, stream, None, server_engine, registry);
            let _ = session.run().await;
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let handshake = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(handshake.sequence_id, 0);

        let flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
        let mut hsr = Vec::new();
        hsr.extend_from_slice(&flags.to_le_bytes());
        hsr.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
        hsr.push(crate::charset::UTF8MB4_0900_AI_CI_ID as u8);
        hsr.extend_from_slice(&[0_u8; 23]);
        hsr.extend_from_slice(b"root\0");
        hsr.push(0);
        hsr.extend_from_slice(b"caching_sha2_password\0");
        write_packet(&mut client, 1, &hsr).await.unwrap();
        let ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(ok.sequence_id, 2);
        assert_eq!(ok.payload[0], 0x00);

        let mut change = Vec::new();
        change.push(COM_CHANGE_USER);
        change.extend_from_slice(b"native\0");
        change.push(0);
        change.extend_from_slice(b"test\0");
        change.extend_from_slice(&crate::charset::UTF8MB4_0900_AI_CI_ID.to_le_bytes());
        change.extend_from_slice(b"caching_sha2_password\0");
        write_packet(&mut client, 0, &change).await.unwrap();

        let switch = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(switch.sequence_id, 1);
        assert_eq!(switch.payload[0], 0xfe);
        assert!(switch.payload[1..].starts_with(b"mysql_native_password\0"));

        write_packet(&mut client, 2, b"native-token").await.unwrap();
        let ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(ok.sequence_id, 3);
        assert_eq!(ok.payload[0], 0x00);

        let seen = engine.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[1].0, "native");
        assert_eq!(seen[1].1.as_deref(), Some("mysql_native_password"));
        assert_eq!(seen[1].2, b"native-token");

        drop(client);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), server).await;
    }

    #[test]
    fn stmt_cursor_mode_accepts_read_only_and_rejects_other_modes() {
        assert_eq!(stmt_execute_cursor_mode(0).unwrap(), false);
        assert_eq!(stmt_execute_cursor_mode(1).unwrap(), true);
        assert_eq!(stmt_execute_cursor_mode(8).unwrap(), false);
        assert_eq!(stmt_execute_cursor_mode(9).unwrap(), true);
        assert!(stmt_execute_cursor_mode(2).is_err());
        assert!(stmt_execute_cursor_mode(4).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execute_uses_optional_metadata_none_when_cached() {
        use tokio::net::{TcpListener, TcpStream};

        use crate::engine::api::{QueryEngine, QueryResponse, ResultSet, SessionContext};
        use crate::engine::session_registry::SessionRegistry;
        use crate::mysql::packet::{read_packet, write_packet};
        use crate::mysql::protocol::{
            CLIENT_OPTIONAL_RESULTSET_METADATA, CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION,
            COM_QUERY, COM_STMT_EXECUTE, COM_STMT_PREPARE, RESULTSET_METADATA_NONE,
        };
        use crate::types::{Column, ColumnType, QueryError, Value};

        struct PreparedMetadataEngine;

        impl QueryEngine for PreparedMetadataEngine {
            fn execute(
                &self,
                session: &mut SessionContext,
                sql: &str,
            ) -> Result<QueryResponse, QueryError> {
                if sql
                    .trim()
                    .eq_ignore_ascii_case("SET SESSION resultset_metadata=NONE")
                {
                    session.resultset_metadata_full = false;
                }
                Ok(QueryResponse::Ok)
            }

            fn describe_prepared_statement(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
            ) -> Result<Vec<Column>, QueryError> {
                Ok(vec![Column {
                    name: "v".to_string(),
                    // Deliberately conservative metadata with a typed runtime
                    // payload: derived CTEs such as HyBench AP Q10 can expose
                    // this exact shape.
                    column_type: ColumnType::VarString,
                    source: Default::default(),
                }])
            }

            fn execute_prepared_statement(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
                _statement: Statement,
            ) -> Option<Result<QueryResponse, QueryError>> {
                Some(Ok(QueryResponse::ResultSet(ResultSet {
                    columns: vec![Column {
                        name: "v".to_string(),
                        column_type: ColumnType::VarString,
                        source: Default::default(),
                    }],
                    rows: vec![vec![Value::Int(42)]],
                })))
            }
        }

        fn binary_string_row(payload: &[u8]) -> &str {
            assert_eq!(payload[0], 0x00);
            assert_eq!(payload[1], 0x00);
            let len = usize::from(payload[2]);
            std::str::from_utf8(&payload[3..3 + len]).unwrap()
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = Arc::new(PreparedMetadataEngine);
        let registry = Arc::new(SessionRegistry::new());

        let server_engine = Arc::clone(&engine);
        let server_registry = Arc::clone(&registry);
        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let mut session =
                Session::new(1, peer_addr, stream, None, server_engine, server_registry);
            let _ = session.run().await;
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let handshake = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(handshake.sequence_id, 0);

        let flags =
            CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_OPTIONAL_RESULTSET_METADATA;
        let mut hsr = Vec::new();
        hsr.extend_from_slice(&flags.to_le_bytes());
        hsr.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
        hsr.push(crate::charset::UTF8MB4_0900_AI_CI_ID as u8);
        hsr.extend_from_slice(&[0_u8; 23]);
        hsr.extend_from_slice(b"root\0");
        hsr.push(0);
        write_packet(&mut client, 1, &hsr).await.unwrap();
        let auth_ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(auth_ok.payload[0], 0x00);

        let mut prepare = vec![COM_STMT_PREPARE];
        prepare.extend_from_slice(b"SELECT v");
        write_packet(&mut client, 0, &prepare).await.unwrap();
        let prepare_ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(prepare_ok.payload[0], 0x00);
        let statement_id = u32::from_le_bytes(prepare_ok.payload[1..5].try_into().unwrap());
        let prepare_column = read_packet(&mut client).await.unwrap().unwrap();
        assert!(
            !prepare_column.payload.is_empty(),
            "PREPARE must send full metadata once"
        );
        let _prepare_eof = read_packet(&mut client).await.unwrap().unwrap();

        let mut set_none = vec![COM_QUERY];
        set_none.extend_from_slice(b"SET SESSION resultset_metadata=NONE");
        write_packet(&mut client, 0, &set_none).await.unwrap();
        let set_ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(set_ok.payload[0], 0x00);

        let mut execute = vec![COM_STMT_EXECUTE];
        execute.extend_from_slice(&statement_id.to_le_bytes());
        execute.push(0);
        execute.extend_from_slice(&1_u32.to_le_bytes());
        write_packet(&mut client, 0, &execute).await.unwrap();

        let column_count = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(column_count.sequence_id, 1);
        assert_eq!(
            column_count.payload,
            vec![RESULTSET_METADATA_NONE, 1],
            "EXECUTE should reuse prepared metadata instead of resending column definitions"
        );
        let row = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(row.sequence_id, 2);
        assert_eq!(binary_string_row(&row.payload), "42");
        let eof = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(eof.sequence_id, 3);
        assert_eq!(eof.payload[0], 0xfe);

        drop(client);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), server).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stmt_execute_read_only_cursor_fetches_rows_in_batches() {
        use tokio::net::{TcpListener, TcpStream};

        use crate::engine::api::{QueryEngine, QueryResponse, ResultSet, SessionContext};
        use crate::engine::session_registry::SessionRegistry;
        use crate::mysql::packet::{read_packet, write_packet};
        use crate::mysql::protocol::{
            CLIENT_PLUGIN_AUTH, CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION,
        };
        use crate::types::{Column, ColumnType, QueryError, Value};

        struct CursorEngine;

        impl QueryEngine for CursorEngine {
            fn execute(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
            ) -> Result<QueryResponse, QueryError> {
                Ok(QueryResponse::Ok)
            }

            fn describe_prepared_statement(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
            ) -> Result<Vec<Column>, QueryError> {
                Ok(vec![Column {
                    name: "v".to_string(),
                    column_type: ColumnType::Long,
                    source: Default::default(),
                }])
            }

            fn execute_prepared_statement(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
                _statement: Statement,
            ) -> Option<Result<QueryResponse, QueryError>> {
                Some(Ok(QueryResponse::ResultSet(ResultSet {
                    columns: vec![Column {
                        name: "v".to_string(),
                        column_type: ColumnType::Long,
                        source: Default::default(),
                    }],
                    rows: vec![
                        vec![Value::Int(10)],
                        vec![Value::Int(20)],
                        vec![Value::Int(30)],
                    ],
                })))
            }
        }

        fn binary_i32_row(payload: &[u8]) -> i32 {
            assert_eq!(payload[0], 0x00);
            assert_eq!(payload[1], 0x00);
            i32::from_le_bytes(payload[2..6].try_into().unwrap())
        }

        fn eof_status(payload: &[u8]) -> u16 {
            assert_eq!(payload[0], 0xfe);
            u16::from_le_bytes([payload[3], payload[4]])
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = Arc::new(CursorEngine);
        let registry = Arc::new(SessionRegistry::new());

        let server_engine = Arc::clone(&engine);
        let killer = Arc::clone(&registry);
        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let mut session = Session::new(1, peer_addr, stream, None, server_engine, registry);
            let _ = session.run().await;
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let _handshake = read_packet(&mut client).await.unwrap().unwrap();
        let flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
        let mut hsr = Vec::new();
        hsr.extend_from_slice(&flags.to_le_bytes());
        hsr.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
        hsr.push(crate::charset::UTF8MB4_0900_AI_CI_ID as u8);
        hsr.extend_from_slice(&[0_u8; 23]);
        hsr.extend_from_slice(b"root\0");
        hsr.push(0);
        hsr.extend_from_slice(b"caching_sha2_password\0");
        write_packet(&mut client, 1, &hsr).await.unwrap();
        let ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(ok.sequence_id, 2);
        assert_eq!(ok.payload[0], 0x00);

        let mut prepare = vec![COM_STMT_PREPARE];
        prepare.extend_from_slice(b"SELECT v");
        write_packet(&mut client, 0, &prepare).await.unwrap();
        let prepare_ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(prepare_ok.payload[0], 0x00);
        let statement_id = u32::from_le_bytes(prepare_ok.payload[1..5].try_into().unwrap());
        assert_eq!(
            u16::from_le_bytes(prepare_ok.payload[5..7].try_into().unwrap()),
            1
        );
        assert_eq!(
            u16::from_le_bytes(prepare_ok.payload[7..9].try_into().unwrap()),
            0
        );
        let _column = read_packet(&mut client).await.unwrap().unwrap();
        let _eof = read_packet(&mut client).await.unwrap().unwrap();

        let mut execute = vec![COM_STMT_EXECUTE];
        execute.extend_from_slice(&statement_id.to_le_bytes());
        execute.push(1); // CURSOR_TYPE_READ_ONLY
        execute.extend_from_slice(&1_u32.to_le_bytes());
        write_packet(&mut client, 0, &execute).await.unwrap();
        let column_count = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(column_count.payload, vec![1]);
        let _column = read_packet(&mut client).await.unwrap().unwrap();
        let eof = read_packet(&mut client).await.unwrap().unwrap();
        assert!(eof_status(&eof.payload) & SERVER_STATUS_CURSOR_EXISTS != 0);

        let mut fetch = vec![COM_STMT_FETCH];
        fetch.extend_from_slice(&statement_id.to_le_bytes());
        fetch.extend_from_slice(&2_u32.to_le_bytes());
        write_packet(&mut client, 0, &fetch).await.unwrap();
        let row1 = read_packet(&mut client).await.unwrap().unwrap();
        let row2 = read_packet(&mut client).await.unwrap().unwrap();
        let eof = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(binary_i32_row(&row1.payload), 10);
        assert_eq!(binary_i32_row(&row2.payload), 20);
        assert!(eof_status(&eof.payload) & SERVER_STATUS_CURSOR_EXISTS != 0);

        write_packet(&mut client, 0, &fetch).await.unwrap();
        let row3 = read_packet(&mut client).await.unwrap().unwrap();
        let eof = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(binary_i32_row(&row3.payload), 30);
        assert!(eof_status(&eof.payload) & SERVER_STATUS_LAST_ROW_SENT != 0);

        // Re-executing must reopen a cursor; cancellation also covers
        // materialized results and the prefetched lookahead row.
        write_packet(&mut client, 0, &execute).await.unwrap();
        for _ in 0..3 {
            read_packet(&mut client).await.unwrap().unwrap();
        }
        killer.kill_query(1).unwrap();
        write_packet(&mut client, 0, &fetch).await.unwrap();
        let err = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(err.payload[0], 0xff);
        assert_eq!(u16::from_le_bytes([err.payload[1], err.payload[2]]), 1317);
        write_packet(&mut client, 0, &[COM_PING]).await.unwrap();
        assert_eq!(
            read_packet(&mut client).await.unwrap().unwrap().payload[0],
            0
        );
        drop(client);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), server).await;
    }

    /// Pins the true-streaming server-side cursor: when the engine
    /// returns a `StreamingResultSet`, the session must NOT materialize
    /// the whole stream at COM_STMT_EXECUTE time and then slice. It must
    /// pull rows on demand from the underlying `ResultCursor` driven by
    /// each `COM_STMT_FETCH`. We assert this by counting `next_row`
    /// invocations on a mock cursor as the protocol exchange unfolds: a
    /// single peek at EXECUTE (so we know whether to advertise
    /// CURSOR_EXISTS vs LAST_ROW_SENT), then exactly `fetch_size - 1`
    /// additional pulls per FETCH (since one row comes from the peek
    /// lookahead), and a final end-of-stream pull to drain.
    #[tokio::test(flavor = "current_thread")]
    async fn stmt_cursor_pulls_streaming_rows_on_demand() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::net::{TcpListener, TcpStream};

        use crate::engine::api::{
            QueryEngine, QueryResponse, ResultCursor, SessionContext, StreamingResultSet,
        };
        use crate::engine::session_registry::SessionRegistry;
        use crate::mysql::packet::{read_packet, write_packet};
        use crate::mysql::protocol::{
            CLIENT_PLUGIN_AUTH, CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION,
        };
        use crate::types::{Column, ColumnType, QueryError, Value};

        const TOTAL_ROWS: i64 = 10;

        #[derive(Debug)]
        struct CountingCursor {
            counter: Arc<AtomicUsize>,
            next_value: i64,
            limit: i64,
        }

        impl ResultCursor for CountingCursor {
            fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
                self.counter.fetch_add(1, Ordering::SeqCst);
                if self.next_value > self.limit {
                    return Ok(None);
                }
                let row = vec![Value::Int(self.next_value)];
                self.next_value += 1;
                Ok(Some(row))
            }
        }

        struct StreamingCursorEngine {
            counter: Arc<AtomicUsize>,
        }

        impl QueryEngine for StreamingCursorEngine {
            fn execute(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
            ) -> Result<QueryResponse, QueryError> {
                Ok(QueryResponse::Ok)
            }

            fn describe_prepared_statement(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
            ) -> Result<Vec<Column>, QueryError> {
                Ok(vec![Column {
                    name: "v".to_string(),
                    column_type: ColumnType::LongLong,
                    source: Default::default(),
                }])
            }

            fn execute_prepared_statement(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
                _statement: Statement,
            ) -> Option<Result<QueryResponse, QueryError>> {
                let cursor = CountingCursor {
                    counter: Arc::clone(&self.counter),
                    next_value: 1,
                    limit: TOTAL_ROWS,
                };
                Some(Ok(QueryResponse::StreamingResultSet(StreamingResultSet {
                    columns: vec![Column {
                        name: "v".to_string(),
                        column_type: ColumnType::LongLong,
                        source: Default::default(),
                    }],
                    found_rows: None,
                    cursor: Box::new(cursor),
                })))
            }
        }

        fn binary_i64_row(payload: &[u8]) -> i64 {
            assert_eq!(payload[0], 0x00);
            assert_eq!(payload[1], 0x00);
            i64::from_le_bytes(payload[2..10].try_into().unwrap())
        }

        fn eof_status(payload: &[u8]) -> u16 {
            assert_eq!(payload[0], 0xfe);
            u16::from_le_bytes([payload[3], payload[4]])
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let counter = Arc::new(AtomicUsize::new(0));
        let engine = Arc::new(StreamingCursorEngine {
            counter: Arc::clone(&counter),
        });
        let registry = Arc::new(SessionRegistry::new());

        let server_engine = Arc::clone(&engine);
        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let mut session = Session::new(1, peer_addr, stream, None, server_engine, registry);
            let _ = session.run().await;
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let _handshake = read_packet(&mut client).await.unwrap().unwrap();
        let flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
        let mut hsr = Vec::new();
        hsr.extend_from_slice(&flags.to_le_bytes());
        hsr.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
        hsr.push(crate::charset::UTF8MB4_0900_AI_CI_ID as u8);
        hsr.extend_from_slice(&[0_u8; 23]);
        hsr.extend_from_slice(b"root\0");
        hsr.push(0);
        hsr.extend_from_slice(b"caching_sha2_password\0");
        write_packet(&mut client, 1, &hsr).await.unwrap();
        let ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(ok.sequence_id, 2);
        assert_eq!(ok.payload[0], 0x00);

        let mut prepare = vec![COM_STMT_PREPARE];
        prepare.extend_from_slice(b"SELECT v");
        write_packet(&mut client, 0, &prepare).await.unwrap();
        let prepare_ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(prepare_ok.payload[0], 0x00);
        let statement_id = u32::from_le_bytes(prepare_ok.payload[1..5].try_into().unwrap());
        let _column = read_packet(&mut client).await.unwrap().unwrap();
        let _eof = read_packet(&mut client).await.unwrap().unwrap();

        let mut execute = vec![COM_STMT_EXECUTE];
        execute.extend_from_slice(&statement_id.to_le_bytes());
        execute.push(1); // CURSOR_TYPE_READ_ONLY
        execute.extend_from_slice(&1_u32.to_le_bytes());
        write_packet(&mut client, 0, &execute).await.unwrap();
        let column_count = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(column_count.payload, vec![1]);
        let _column = read_packet(&mut client).await.unwrap().unwrap();
        let eof = read_packet(&mut client).await.unwrap().unwrap();
        assert!(eof_status(&eof.payload) & SERVER_STATUS_CURSOR_EXISTS != 0);
        // EXECUTE pulled exactly one row from the underlying stream (the
        // lookahead used to decide CURSOR_EXISTS vs LAST_ROW_SENT). If
        // the server were still materializing, we'd see TOTAL_ROWS + 1.
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        let mut fetch3 = vec![COM_STMT_FETCH];
        fetch3.extend_from_slice(&statement_id.to_le_bytes());
        fetch3.extend_from_slice(&3_u32.to_le_bytes());
        write_packet(&mut client, 0, &fetch3).await.unwrap();
        let row1 = read_packet(&mut client).await.unwrap().unwrap();
        let row2 = read_packet(&mut client).await.unwrap().unwrap();
        let row3 = read_packet(&mut client).await.unwrap().unwrap();
        let eof = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(binary_i64_row(&row1.payload), 1);
        assert_eq!(binary_i64_row(&row2.payload), 2);
        assert_eq!(binary_i64_row(&row3.payload), 3);
        assert!(eof_status(&eof.payload) & SERVER_STATUS_CURSOR_EXISTS != 0);
        // 3 rows delivered = 1 lookahead + 2 fresh pulls from the cursor.
        assert_eq!(counter.load(Ordering::SeqCst), 3);

        let mut fetch5 = vec![COM_STMT_FETCH];
        fetch5.extend_from_slice(&statement_id.to_le_bytes());
        fetch5.extend_from_slice(&5_u32.to_le_bytes());
        write_packet(&mut client, 0, &fetch5).await.unwrap();
        for expected in 4..=8 {
            let row = read_packet(&mut client).await.unwrap().unwrap();
            assert_eq!(binary_i64_row(&row.payload), expected);
        }
        let eof = read_packet(&mut client).await.unwrap().unwrap();
        assert!(eof_status(&eof.payload) & SERVER_STATUS_CURSOR_EXISTS != 0);
        assert_eq!(counter.load(Ordering::SeqCst), 8);

        // An attacker-sized FETCH count must remain chunked and bounded: only
        // the two remaining rows are encoded, with no `with_capacity(u32::MAX)`
        // allocation. The cursor also performs one end-of-stream pull.
        let mut fetch_max = vec![COM_STMT_FETCH];
        fetch_max.extend_from_slice(&statement_id.to_le_bytes());
        fetch_max.extend_from_slice(&u32::MAX.to_le_bytes());
        write_packet(&mut client, 0, &fetch_max).await.unwrap();
        let row9 = read_packet(&mut client).await.unwrap().unwrap();
        let row10 = read_packet(&mut client).await.unwrap().unwrap();
        let eof = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(binary_i64_row(&row9.payload), 9);
        assert_eq!(binary_i64_row(&row10.payload), 10);
        assert!(eof_status(&eof.payload) & SERVER_STATUS_LAST_ROW_SENT != 0);
        assert_eq!(counter.load(Ordering::SeqCst), 11);

        // Cursor is exhausted; any further FETCH must error with
        // ER_UNKNOWN_STMT_HANDLER (cursor cleared after LAST_ROW_SENT).
        write_packet(&mut client, 0, &fetch5).await.unwrap();
        let err = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(err.payload[0], 0xff);

        drop(client);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), server).await;
    }

    /// End-to-end LOCAL INFILE protocol round-trip against a real
    /// `Session` bound to a loopback TCP listener. Pins the wire-level
    /// sequence_id behaviour: the server's OK reply must be
    /// `last_client_seq + 1`, not a hardcoded `1`. With the old code
    /// the client (mysql CLI, JDBC, Go driver, ...) sees a
    /// non-monotonic sequence and aborts with ER 2013 "Lost connection
    /// during query" even though the file data has already landed on
    /// disk. Also asserts the engine actually received the
    /// concatenated file body via `execute_load_data_statement`.
    #[tokio::test(flavor = "current_thread")]
    async fn local_infile_response_sequence_id_follows_last_client_packet() {
        check_local_infile_sequence(false, false).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn audit14_local_infile_multiframe_request_keeps_sequence() {
        check_local_infile_sequence(true, false).await;
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn local_infile_disconnect_cancels_before_reader_eof() {
        check_local_infile_sequence(false, true).await;
    }

    async fn check_local_infile_sequence(multiframe: bool, abort_upload: bool) {
        use std::sync::Mutex;
        use tokio::net::{TcpListener, TcpStream};

        use crate::engine::api::{QueryEngine, QueryResponse, SessionContext};
        use crate::engine::session_registry::SessionRegistry;
        use crate::mysql::packet::{read_packet, write_packet};
        use crate::mysql::protocol::{
            CLIENT_LOCAL_FILES, CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION, COM_QUERY,
            LOCAL_INFILE_REQUEST,
        };
        use crate::parser::Statement;
        use crate::types::QueryError;

        #[derive(Default)]
        struct LocalInfileTestEngine {
            captured: Mutex<Vec<u8>>,
            cancelled: AtomicBool,
        }

        impl QueryEngine for LocalInfileTestEngine {
            fn execute_load_data_statement_from_reader(
                &self,
                session: &mut SessionContext,
                mut statement: Statement,
                mut reader: Box<dyn std::io::Read + Send>,
            ) -> Result<QueryResponse, QueryError> {
                let mut bytes = Vec::new();
                std::io::Read::read_to_end(&mut reader, &mut bytes).unwrap();
                if let Some(err) = session.cancelled_error() {
                    self.cancelled.store(true, Ordering::Release);
                    return Err(err);
                }
                let Statement::LoadData(load) = &mut statement else {
                    panic!("expected LOAD DATA");
                };
                load.data = Some(bytes);
                self.execute_load_data_statement(session, statement)
            }

            fn execute(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
            ) -> Result<QueryResponse, QueryError> {
                Ok(QueryResponse::Ok)
            }

            fn execute_load_data_statement(
                &self,
                _session: &mut SessionContext,
                statement: Statement,
            ) -> Result<QueryResponse, QueryError> {
                let Statement::LoadData(load) = statement else {
                    panic!("expected LoadData statement, got {statement:?}");
                };
                let data = load.data.expect("session must populate LoadData.data");
                let affected = data.len() as u64;
                *self.captured.lock().unwrap() = data;
                Ok(QueryResponse::OkAffected {
                    affected_rows: affected,
                })
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = Arc::new(LocalInfileTestEngine::default());
        let registry = Arc::new(SessionRegistry::new());

        let server_engine = Arc::clone(&engine);
        let server_registry = Arc::clone(&registry);
        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let mut session =
                Session::new(1, peer_addr, stream, None, server_engine, server_registry);
            let _ = session.run().await;
        });

        let mut client = TcpStream::connect(addr).await.unwrap();

        // Initial handshake from server.
        let handshake = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(handshake.sequence_id, 0);

        // Minimal handshake response: CLIENT_PROTOCOL_41 +
        // CLIENT_SECURE_CONNECTION (no SSL, no DB, empty auth). The
        // default `QueryEngine::authenticate` accepts any credentials,
        // so this is enough to reach the command loop.
        let mut hsr = Vec::new();
        let flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_LOCAL_FILES;
        hsr.extend_from_slice(&flags.to_le_bytes());
        hsr.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
        hsr.push(crate::charset::UTF8MB4_0900_AI_CI_ID as u8);
        hsr.extend_from_slice(&[0_u8; 23]);
        hsr.extend_from_slice(b"root\0");
        hsr.push(0); // zero-length auth response
        write_packet(&mut client, 1, &hsr).await.unwrap();

        let auth_ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(auth_ok.sequence_id, 2);
        assert_eq!(auth_ok.payload[0], 0x00, "expected OK header after auth");

        // COM_QUERY: LOAD DATA LOCAL INFILE.
        let mut query = vec![COM_QUERY];
        query.extend_from_slice(b"LOAD DATA LOCAL INFILE '/tmp/mydb-test-local-infile.csv' INTO TABLE t FIELDS TERMINATED BY ','");
        if multiframe {
            query.extend_from_slice(b" /*");
            query.resize(query.len() + 17 * 1024 * 1024, b'x');
            query.extend_from_slice(b"*/");
        }
        let first_response = if multiframe { 2 } else { 1 };
        write_packet(&mut client, 0, &query).await.unwrap();

        // The upload request follows the last command frame.
        let infile_req = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(infile_req.sequence_id, first_response);
        assert_eq!(infile_req.payload[0], LOCAL_INFILE_REQUEST);

        // Two data packets + empty terminator. The empty packet at
        // the final response follows the client's empty terminator.
        write_packet(&mut client, first_response + 1, b"1,foo\n")
            .await
            .unwrap();
        write_packet(&mut client, first_response + 2, b"2,bar\n")
            .await
            .unwrap();
        if abort_upload {
            drop(client);
            tokio::time::timeout(std::time::Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap();
            assert!(engine.cancelled.load(Ordering::Acquire));
            assert!(engine.captured.lock().unwrap().is_empty());
            return;
        }
        write_packet(&mut client, first_response + 3, b"")
            .await
            .unwrap();

        let response = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(
            response.sequence_id,
            first_response + 4,
            "LOAD DATA LOCAL response must use last_client_seq + 1, not a hardcoded 1"
        );
        assert_eq!(response.payload[0], 0x00, "expected OK header");

        assert_eq!(
            engine.captured.lock().unwrap().as_slice(),
            b"1,foo\n2,bar\n",
            "engine must observe the concatenated file body"
        );

        // Close the client so the session loop exits cleanly.
        drop(client);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), server).await;
    }

    #[tokio::test]
    async fn cancelled_partial_stream_write_preserves_query_frames() {
        use crate::engine::session_registry::SessionRegistry;
        use tokio::io::AsyncReadExt;

        for compressed in [false, true] {
            for kill_connection in [false, true] {
                let registry = SessionRegistry::new();
                let handle = registry.register(7, "root".into(), "localhost".into(), "test".into());
                let (mut writer, mut reader) = tokio::io::duplex(8);
                let task = tokio::spawn(async move {
                    let mut codec = PacketCodec::plain();
                    if compressed {
                        codec.enable_zlib_compression();
                    }
                    let mut batch = codec.batch_writer(&mut writer);
                    let next = write_cancellable_stream_packet(
                        &mut batch,
                        4,
                        &vec![42; 128 * 1024],
                        &handle,
                    )
                    .await?;
                    finish_error_result_set(
                        batch,
                        next,
                        &crate::engine::cancel::interrupted_error(),
                    )
                    .await
                });
                // A received prefix proves the write is blocked mid-frame.
                let mut prefix = [0; 8];
                reader.read_exact(&mut prefix).await.unwrap();
                if kill_connection {
                    registry.kill_connection(7).unwrap();
                } else {
                    registry.kill_query(7).unwrap();
                }
                let drain = tokio::spawn(async move {
                    let mut bytes = prefix.to_vec();
                    reader.read_to_end(&mut bytes).await.unwrap();
                    bytes
                });
                let result = tokio::time::timeout(std::time::Duration::from_secs(2), task)
                    .await
                    .unwrap()
                    .unwrap();
                let bytes = drain.await.unwrap();
                if kill_connection {
                    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::ConnectionAborted);
                } else {
                    result.unwrap();
                    let mut codec = PacketCodec::plain();
                    if compressed {
                        codec.enable_zlib_compression();
                    }
                    let mut input = bytes.as_slice();
                    let row = codec.read_packet(&mut input).await.unwrap().unwrap();
                    assert_eq!(row.sequence_id, 4);
                    assert_eq!(row.payload, vec![42; 128 * 1024]);
                    let err = codec.read_packet(&mut input).await.unwrap().unwrap();
                    assert_eq!(err.sequence_id, 5);
                    assert_eq!(err.payload[0], 0xff);
                    assert_eq!(u16::from_le_bytes([err.payload[1], err.payload[2]]), 1317);
                }
            }
        }
    }

    #[tokio::test]
    async fn finish_error_result_set_emits_err_terminator() {
        // A failed result set must be terminated with an ERR packet at the next
        // sequence id, and the batch must be flushed — never dropped
        // unfinished (which would silently discard buffered rows and
        // desync the client). Verifies both the framing and the seq id.
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut batch = PacketBatchWriter::new(&mut sink);
            // A couple of already-buffered "rows" precede the terminator.
            batch.write_packet(1, b"row-a").await.unwrap();
            batch.write_packet(2, b"row-b").await.unwrap();
            let err = crate::error::catalog::ER_QUERY_INTERRUPTED
                .error("Query execution was interrupted".to_string());
            finish_error_result_set(batch, 3, &err).await.unwrap();
        }
        // The last frame in the stream must be the ERR packet at seq 3.
        // Walk the frames: [len(3) seq(1) payload], starting at offset 0.
        let mut off = 0usize;
        let mut last: Option<(u8, Vec<u8>)> = None;
        while off + 4 <= sink.len() {
            let len = (sink[off] as usize)
                | ((sink[off + 1] as usize) << 8)
                | ((sink[off + 2] as usize) << 16);
            let seq = sink[off + 3];
            let payload = sink[off + 4..off + 4 + len].to_vec();
            last = Some((seq, payload));
            off += 4 + len;
        }
        let (seq, payload) = last.expect("at least one frame");
        assert_eq!(seq, 3, "ERR terminator carries the next sequence id");
        assert_eq!(payload[0], 0xff, "ERR packet marker");
        let code = (payload[1] as u16) | ((payload[2] as u16) << 8);
        assert_eq!(code, 1317, "interruption error code propagated");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn streaming_oom_returns_1037_and_keeps_connection_usable() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::net::{TcpListener, TcpStream};

        use crate::engine::api::{QueryEngine, ResultCursor};
        use crate::engine::session_registry::SessionRegistry;
        use crate::mysql::packet::{read_packet, write_packet};
        use crate::mysql::protocol::{CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION, COM_QUERY};
        use crate::types::{ColumnType, Value};

        #[derive(Debug)]
        struct FailingCursor;

        impl ResultCursor for FailingCursor {
            fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
                Err(crate::error::catalog::ER_OUTOFMEMORY
                    .error("query memory limit exceeded".to_string()))
            }
        }

        struct ErrorThenOkEngine(AtomicUsize);

        impl QueryEngine for ErrorThenOkEngine {
            fn execute(
                &self,
                _session: &mut SessionContext,
                _sql: &str,
            ) -> Result<QueryResponse, QueryError> {
                if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(QueryResponse::StreamingResultSet(StreamingResultSet {
                        columns: vec![Column {
                            name: "v".to_string(),
                            column_type: ColumnType::LongLong,
                            source: Default::default(),
                        }],
                        found_rows: None,
                        cursor: Box::new(FailingCursor),
                    }))
                } else {
                    Ok(QueryResponse::Ok)
                }
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = Arc::new(ErrorThenOkEngine(AtomicUsize::new(0)));
        let registry = Arc::new(SessionRegistry::new());
        let server = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let mut session = Session::new(1, peer_addr, stream, None, engine, registry);
            let _ = session.run().await;
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let _handshake = read_packet(&mut client).await.unwrap().unwrap();
        let flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION;
        let mut hsr = Vec::new();
        hsr.extend_from_slice(&flags.to_le_bytes());
        hsr.extend_from_slice(&(16_u32 * 1024 * 1024).to_le_bytes());
        hsr.push(crate::charset::UTF8MB4_0900_AI_CI_ID as u8);
        hsr.extend_from_slice(&[0_u8; 23]);
        hsr.extend_from_slice(b"root\0");
        hsr.push(0);
        write_packet(&mut client, 1, &hsr).await.unwrap();
        let auth_ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(auth_ok.payload[0], 0x00);

        let mut query = vec![COM_QUERY];
        query.extend_from_slice(b"SELECT 1");
        write_packet(&mut client, 0, &query).await.unwrap();
        let column_count = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(column_count.payload, vec![1]);
        let _column = read_packet(&mut client).await.unwrap().unwrap();
        let _metadata_eof = read_packet(&mut client).await.unwrap().unwrap();
        let err = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(err.payload[0], 0xff);
        let code = (err.payload[1] as u16) | ((err.payload[2] as u16) << 8);
        assert_eq!(code, 1037);
        assert_eq!(&err.payload[3..9], b"#HY001");

        // A second command on the same socket must receive a fresh response;
        // the cursor error is a SQL failure, not a connection I/O failure.
        write_packet(&mut client, 0, &query).await.unwrap();
        let ok = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(ok.sequence_id, 1);
        assert_eq!(ok.payload[0], 0x00);

        drop(client);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), server).await;
    }
}

#[cfg(test)]
mod routine_query_capability_tests {
    use super::query_requires_multi_statements;

    #[test]
    fn compound_routine_is_one_protocol_statement() {
        for sql in [
            "CREATE PROCEDURE p() BEGIN IF 1 SELECT 1; END IF; END",
            "CREATE PROCEDURE p() BEGIN SET x=; END",
            "SET @side_effect=1; SELECT FROM",
        ] {
            let err = query_requires_multi_statements(sql, 0).unwrap_err();
            assert!(!err.to_string().contains("CLIENT_MULTI_STATEMENTS"));
        }

        for sql in [
            "CREATE PROCEDURE p() BEGIN SELECT 1; SELECT 2; END",
            "CREATE FUNCTION f() RETURNS INT BEGIN DECLARE x INT; SET x=1; RETURN x; END;",
            "/* comment */ CREATE PROCEDURE p() BEGIN IF 1 THEN SELECT ';'; END IF; END; /* trailing */",
        ] {
            assert!(!query_requires_multi_statements(sql, 0).unwrap(), "{sql}");
        }
        for sql in [
            "SELECT 1; SELECT 2",
            "CREATE PROCEDURE p() BEGIN SELECT 1; END; SELECT 2",
            "CREATE FUNCTION f() RETURNS INT BEGIN RETURN 1; END; DROP TABLE t",
            "SELECT 1; /*!80000 SELECT 2 */",
        ] {
            assert!(query_requires_multi_statements(sql, 0).unwrap(), "{sql}");
        }
    }
}

#[cfg(test)]
mod multi_query_tests;

#[cfg(test)]
mod init_connect_tests;

#[cfg(test)]
mod stream_tests;

#[cfg(test)]
mod release_audit_sql_tests {
    use super::*;

    #[test]
    fn release_audit_introducer_expression_labels_keep_source_spelling() {
        let session = SessionContext::default();
        for sql in [
            "SELECT CHARSET(_utf8mb4'x');",
            "SELECT CHARSET(_utf8mb4'x'), HEX(_utf8mb4'中') AS explicit_name",
            "SELECT * FROM (SELECT CHARSET(_utf8mb4'x')) d",
        ] {
            let decoded = decode_session_sql(&session, sql.as_bytes()).unwrap();
            assert!(decoded.contains("AS `CHARSET(_utf8mb4'x')`"), "{decoded}");
            crate::parser::parse_statement(&decoded).unwrap();
        }
    }

    #[test]
    fn release_audit_introducers_keep_raw_client_bytes() {
        let mut session = SessionContext::default();
        assert_eq!(
            decode_session_sql(&session, "SELECT _latin1'中'".as_bytes()).unwrap(),
            "SELECT _latin1 X'e4b8ad'"
        );
        session.character_set_client = "latin1".into();
        assert_eq!(
            decode_session_sql(&session, b"SELECT _utf8mb4'\xc3\xa9'").unwrap(),
            "SELECT _utf8mb4 X'c3a9'"
        );
        assert_eq!(
            decode_session_sql(&session, b"SELECT 'plain_ascii', a_b FROM t").unwrap(),
            "SELECT 'plain_ascii', a_b FROM t"
        );
    }

    #[test]
    fn release_audit_binary_literals_preserve_octets() {
        let mut session = SessionContext::default();
        session.character_set_client = "utf8mb4".into();
        for (sql, expected) in [
            (
                &b"INSERT INTO t VALUES ('\xff\\0\\n\\'\\\\')"[..],
                "INSERT INTO t VALUES (X'ff000a275c')",
            ),
            (&b"SELECT _binary'\xff'"[..], "SELECT X'ff'"),
            (
                &b"SELECT /*!80000 '\xff' */"[..],
                "SELECT /*!80000 X'ff' */",
            ),
        ] {
            assert_eq!(decode_session_sql(&session, sql).unwrap(), expected);
        }
        assert!(decode_session_sql(&session, b"SELECT \xff").is_err());
        assert!(decode_session_sql(&session, b"SELECT `\xff`").is_err());
    }
}
