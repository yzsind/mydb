//! Runtime event buffers for the core `performance_schema` event tables.
//!
//! The registry in `perf_schema.rs` owns MySQL-compatible table/column shapes;
//! this module owns the bounded, process-local data behind the core
//! current/history/history_long and summary tables. It deliberately records
//! only events that have a real source in the executor today.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use super::perf_statement_stats::{StatementMetrics, StatementSample};
use crate::engine::api::{QueryResponse, SessionContext};
use crate::engine::logging::QueryLogMetadata;
use crate::types::{QueryError, Value};
use crate::util::text::char_prefix;

const STATEMENT_HISTORY_PER_THREAD: usize = 10;
const WAIT_HISTORY_PER_THREAD: usize = 10;
const HISTORY_LONG_MAX: usize = 10_000;
const MAX_TEXT_LEN: usize = 1024;
const WAIT_EVENT_NAME: &str = "wait/lock/table/sql/handler";
const ONE_CONNECTION_THREAD: &str = "thread/sql/one_connection";
const PERF_EVENT_SHARDS: usize = 64;

fn statement_history_limit() -> usize {
    static LIMIT: OnceLock<usize> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        crate::engine::perf_setup::configured_usize(
            "performance_schema_events_statements_history_size",
            STATEMENT_HISTORY_PER_THREAD,
        )
    })
}

fn wait_history_limit() -> usize {
    static LIMIT: OnceLock<usize> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        crate::engine::perf_setup::configured_usize(
            "performance_schema_events_waits_history_size",
            WAIT_HISTORY_PER_THREAD,
        )
    })
}

fn history_long_limit() -> usize {
    static LIMIT: OnceLock<usize> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        crate::engine::perf_setup::configured_usize(
            "performance_schema_events_statements_history_long_size",
            HISTORY_LONG_MAX,
        )
    })
}

fn wait_history_long_limit() -> usize {
    static LIMIT: OnceLock<usize> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        crate::engine::perf_setup::configured_usize(
            "performance_schema_events_waits_history_long_size",
            HISTORY_LONG_MAX,
        )
    })
}

#[derive(Clone)]
struct StatementEvent {
    thread_id: u64,
    event_id: u64,
    end_event_id: Option<u64>,
    event_name: &'static str,
    timer_start: u64,
    timer_end: u64,
    timer_wait: u64,
    lock_time: u64,
    sql_text: Arc<str>,
    digest: Option<Arc<str>>,
    digest_text: Option<Arc<str>>,
    current_schema: String,
    mysql_errno: Option<u16>,
    returned_sqlstate: Option<String>,
    message_text: Option<String>,
    errors: u64,
    warnings: u64,
    rows_affected: u64,
    rows_sent: u64,
    rows_examined: u64,
}

#[derive(Clone)]
struct WaitEvent {
    thread_id: u64,
    event_id: u64,
    end_event_id: u64,
    timer_start: u64,
    timer_end: u64,
    timer_wait: u64,
    object_instance: u64,
    operation: &'static str,
}

#[derive(Clone, Default)]
struct ThreadEvents {
    next_event_id: u64,
    current_statement: Option<StatementEvent>,
    statement_history: VecDeque<StatementEvent>,
    current_wait: Option<WaitEvent>,
    wait_history: VecDeque<WaitEvent>,
}

impl ThreadEvents {
    fn next_event_id(&mut self) -> u64 {
        self.next_event_id = self.next_event_id.saturating_add(1).max(1);
        self.next_event_id
    }
}

#[derive(Clone)]
struct StatementAggregate {
    event_name: &'static str,
    metrics: StatementMetrics,
}

impl StatementAggregate {
    fn new(event_name: &'static str) -> Self {
        Self {
            event_name,
            metrics: StatementMetrics::default(),
        }
    }

    fn record(&mut self, event: &StatementEvent) {
        self.metrics.record(StatementSample {
            timer_wait: event.timer_wait,
            lock_time: event.lock_time,
            errors: event.errors,
            warnings: event.warnings,
            rows_affected: event.rows_affected,
            rows_sent: event.rows_sent,
            rows_examined: event.rows_examined,
        });
    }
}

#[derive(Clone)]
struct WaitAggregate {
    event_name: &'static str,
    object_instance: u64,
    count_star: u64,
    sum_timer_wait: u64,
    min_timer_wait: u64,
    max_timer_wait: u64,
}

impl WaitAggregate {
    fn new(event_name: &'static str, object_instance: u64) -> Self {
        Self {
            event_name,
            object_instance,
            count_star: 0,
            sum_timer_wait: 0,
            min_timer_wait: u64::MAX,
            max_timer_wait: 0,
        }
    }

    fn record(&mut self, event: &WaitEvent) {
        self.count_star = self.count_star.saturating_add(1);
        self.sum_timer_wait = self.sum_timer_wait.saturating_add(event.timer_wait);
        self.min_timer_wait = self.min_timer_wait.min(event.timer_wait);
        self.max_timer_wait = self.max_timer_wait.max(event.timer_wait);
    }
}

/// Operations shared by statement and wait summaries. Generic callers retain
/// static dispatch; recording individual events does not pass through this trait.
trait SummaryAggregate: Clone {
    fn is_empty(&self) -> bool;
    fn merge(&mut self, other: &Self);
}

impl SummaryAggregate for StatementAggregate {
    #[inline]
    fn is_empty(&self) -> bool {
        self.metrics.count_star == 0
    }
    fn merge(&mut self, other: &Self) {
        self.metrics.merge(&other.metrics);
    }
}

impl SummaryAggregate for WaitAggregate {
    #[inline]
    fn is_empty(&self) -> bool {
        self.count_star == 0
    }
    fn merge(&mut self, other: &Self) {
        self.count_star = self.count_star.saturating_add(other.count_star);
        self.sum_timer_wait = self.sum_timer_wait.saturating_add(other.sum_timer_wait);
        self.min_timer_wait = self.min_timer_wait.min(other.min_timer_wait);
        self.max_timer_wait = self.max_timer_wait.max(other.max_timer_wait);
    }
}

type ThreadSummaryMap<A> = BTreeMap<(u64, &'static str), LiveAggregate<A>>;
type IdentitySummaryMap<A> = BTreeMap<(String, &'static str), A>;
type AccountSummaryMap<A> = BTreeMap<(String, String, &'static str), A>;

/// Live account dimensions share the thread aggregate until TRUNCATE gives
/// them different reset epochs. Split counters once on that cold path, then
/// update them in place: no per-statement account keys or string allocations.
struct LiveAggregate<A> {
    thread: A,
    rollups: Option<Box<RollupAggregate<A>>>,
}

enum RollupAggregate<A> {
    // Resetting thread counters does not change the three account epochs.
    Shared(A),
    Separate([A; 3]),
}

impl<A: Clone> LiveAggregate<A> {
    fn new(thread: A) -> Self {
        Self {
            thread,
            rollups: None,
        }
    }

    fn rollup(&self, dimension: usize) -> &A {
        match self.rollups.as_deref() {
            None => &self.thread,
            Some(RollupAggregate::Shared(value)) => value,
            Some(RollupAggregate::Separate(values)) => &values[dimension],
        }
    }

    fn reset(&mut self, dimension: usize, empty: A) {
        let rollups = self
            .rollups
            .get_or_insert_with(|| Box::new(RollupAggregate::Shared(self.thread.clone())));
        if dimension == 0 {
            self.thread = empty;
        } else {
            if let RollupAggregate::Shared(value) = rollups.as_ref() {
                **rollups = RollupAggregate::Separate(std::array::from_fn(|_| value.clone()));
            }
            let RollupAggregate::Separate(values) = rollups.as_mut() else {
                unreachable!()
            };
            values[dimension - 1] = empty;
        }
    }

    fn record(&mut self, mut record: impl FnMut(&mut A)) {
        record(&mut self.thread);
        match self.rollups.as_deref_mut() {
            None => {}
            Some(RollupAggregate::Shared(value)) => record(value),
            Some(RollupAggregate::Separate(values)) => {
                for aggregate in values.iter_mut() {
                    record(aggregate);
                }
            }
        }
    }
}

struct PerfEvents {
    threads: BTreeMap<u64, ThreadEvents>,
    thread_accounts: BTreeMap<u64, (String, String)>,
    statement_history_long: VecDeque<StatementEvent>,
    wait_history_long: VecDeque<WaitEvent>,
    statement_global: BTreeMap<&'static str, StatementAggregate>,
    statement_by_thread: ThreadSummaryMap<StatementAggregate>,
    statement_by_user: IdentitySummaryMap<StatementAggregate>,
    statement_by_host: IdentitySummaryMap<StatementAggregate>,
    statement_by_account: AccountSummaryMap<StatementAggregate>,
    wait_global: BTreeMap<&'static str, WaitAggregate>,
    wait_by_thread: ThreadSummaryMap<WaitAggregate>,
    wait_by_user: IdentitySummaryMap<WaitAggregate>,
    wait_by_host: IdentitySummaryMap<WaitAggregate>,
    wait_by_account: AccountSummaryMap<WaitAggregate>,
    wait_by_instance: BTreeMap<(&'static str, u64), WaitAggregate>,
    statement_history_long_limit: usize,
    wait_history_long_limit: usize,
    _memory_charge: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl Default for PerfEvents {
    fn default() -> Self {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        let statement_per_shard = history_long_limit().div_ceil(PERF_EVENT_SHARDS);
        let wait_per_shard = wait_history_long_limit().div_ceil(PERF_EVENT_SHARDS);
        let desired = statement_per_shard
            .saturating_mul(std::mem::size_of::<StatementEvent>())
            .saturating_add(wait_per_shard.saturating_mul(std::mem::size_of::<WaitEvent>()))
            .saturating_add(std::mem::size_of::<Self>());
        let admitted = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::PerformanceSchema,
            desired,
        );
        let (statement_limit, wait_limit, charge) = if let Some(charge) = admitted {
            (statement_per_shard, wait_per_shard, charge)
        } else {
            (
                0,
                0,
                ControlledMemoryCharge::try_new_optional(
                    ControlledMemoryCategory::PerformanceSchema,
                    0,
                )
                .expect("zero-byte PFS reservation cannot fail"),
            )
        };
        Self {
            threads: BTreeMap::new(),
            thread_accounts: BTreeMap::new(),
            statement_history_long: VecDeque::with_capacity(statement_limit),
            wait_history_long: VecDeque::with_capacity(wait_limit),
            statement_global: BTreeMap::new(),
            statement_by_thread: BTreeMap::new(),
            statement_by_user: BTreeMap::new(),
            statement_by_host: BTreeMap::new(),
            statement_by_account: BTreeMap::new(),
            wait_global: BTreeMap::new(),
            wait_by_thread: BTreeMap::new(),
            wait_by_user: BTreeMap::new(),
            wait_by_host: BTreeMap::new(),
            wait_by_account: BTreeMap::new(),
            wait_by_instance: BTreeMap::new(),
            statement_history_long_limit: statement_limit,
            wait_history_long_limit: wait_limit,
            _memory_charge: charge,
        }
    }
}

fn states() -> &'static [RwLock<Option<PerfEvents>>] {
    static STATES: OnceLock<Box<[RwLock<Option<PerfEvents>>]>> = OnceLock::new();
    STATES.get_or_init(|| {
        (0..PERF_EVENT_SHARDS)
            .map(|_| RwLock::new(None))
            .collect::<Vec<_>>()
            .into_boxed_slice()
    })
}

fn state_for_thread(thread_id: u64) -> &'static RwLock<Option<PerfEvents>> {
    &states()[(thread_id as usize) & (PERF_EVENT_SHARDS - 1)]
}

/// Keep related maps under one shard read guard for the complete visit.
#[inline]
fn for_each_state(mut visit: impl FnMut(&PerfEvents)) {
    for shard in states() {
        let guard = shard.read();
        if let Some(state) = guard.as_ref() {
            visit(state);
        }
    }
}

fn collect_shard_rows(
    mut append: impl FnMut(&PerfEvents, &mut Vec<Vec<Value>>),
) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for_each_state(|state| append(state, &mut rows));
    rows
}

fn merged_summary_map<K: Ord + Clone, A: Clone>(
    select: impl Fn(&PerfEvents) -> &BTreeMap<K, A>,
    merge: impl Fn(&mut A, &A),
) -> BTreeMap<K, A> {
    let mut merged = BTreeMap::new();
    for_each_state(|state| {
        for (key, aggregate) in select(state) {
            merged
                .entry(key.clone())
                .and_modify(|current| merge(current, aggregate))
                .or_insert_with(|| aggregate.clone());
        }
    });
    merged
}

/// Read retired and live counters together: disconnect moves counters between
/// these maps under the shard's write guard. Separate passes could miss that
/// move and omit a completed event. Each live dimension keeps its own reset epoch.
#[inline]
fn account_summary_snapshot<K: Ord + Clone, A: SummaryAggregate, const DIMENSION: usize>(
    retired: impl Fn(&PerfEvents) -> &BTreeMap<K, A>,
    live: impl Fn(&PerfEvents) -> &ThreadSummaryMap<A>,
    key: impl Fn(&str, &str, &'static str) -> K,
) -> BTreeMap<K, A> {
    let mut merged = BTreeMap::new();
    for_each_state(|state| {
        for (key, aggregate) in retired(state) {
            merged
                .entry(key.clone())
                .and_modify(|current: &mut A| current.merge(aggregate))
                .or_insert_with(|| aggregate.clone());
        }
        for ((thread_id, event), counters) in live(state) {
            let aggregate = counters.rollup(DIMENSION);
            if aggregate.is_empty() {
                continue;
            }
            let Some((user, host)) = state.thread_accounts.get(thread_id) else {
                continue;
            };
            merged
                .entry(key(user, host, event))
                .and_modify(|current| current.merge(aggregate))
                .or_insert_with(|| aggregate.clone());
        }
    });
    merged
}

fn statement_by_user_snapshot() -> IdentitySummaryMap<StatementAggregate> {
    account_summary_snapshot::<_, _, 0>(
        |state| &state.statement_by_user,
        |state| &state.statement_by_thread,
        |user, _, event| (user.to_owned(), event),
    )
}

fn statement_by_host_snapshot() -> IdentitySummaryMap<StatementAggregate> {
    account_summary_snapshot::<_, _, 1>(
        |state| &state.statement_by_host,
        |state| &state.statement_by_thread,
        |_, host, event| (host.to_owned(), event),
    )
}

fn statement_by_account_snapshot() -> AccountSummaryMap<StatementAggregate> {
    account_summary_snapshot::<_, _, 2>(
        |state| &state.statement_by_account,
        |state| &state.statement_by_thread,
        |user, host, event| (user.to_owned(), host.to_owned(), event),
    )
}

fn wait_by_user_snapshot() -> IdentitySummaryMap<WaitAggregate> {
    account_summary_snapshot::<_, _, 0>(
        |state| &state.wait_by_user,
        |state| &state.wait_by_thread,
        |user, _, event| (user.to_owned(), event),
    )
}

fn wait_by_host_snapshot() -> IdentitySummaryMap<WaitAggregate> {
    account_summary_snapshot::<_, _, 1>(
        |state| &state.wait_by_host,
        |state| &state.wait_by_thread,
        |_, host, event| (host.to_owned(), event),
    )
}

fn wait_by_account_snapshot() -> AccountSummaryMap<WaitAggregate> {
    account_summary_snapshot::<_, _, 2>(
        |state| &state.wait_by_account,
        |state| &state.wait_by_thread,
        |user, host, event| (user.to_owned(), host.to_owned(), event),
    )
}

fn timer_origin() -> &'static Instant {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    ORIGIN.get_or_init(Instant::now)
}

thread_local! {
    static THREAD_LOCK_WAIT_PS: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
    /// Per-statement rows-examined counter for slow query log. Bumped
    /// by storage scan helpers as visible rows are surfaced, drained
    /// when the statement finishes. Lives in the same thread-local
    /// channel pattern as lock waits so the storage layer doesn't need
    /// a `&mut SessionContext` plumbed through every scan signature.
    static THREAD_ROWS_EXAMINED: Cell<u64> = const { Cell::new(0) };
    static THREAD_SCAN_ROWS_EXAMINED: Cell<u64> = const { Cell::new(0) };
    static SHARED_ROWS_EXAMINED: RefCell<Option<Arc<RowsExaminedCounters>>> = const { RefCell::new(None) };
}

#[derive(Default)]
pub(crate) struct RowsExaminedCounters {
    total: AtomicU64,
    sequential: AtomicU64,
}

impl RowsExaminedCounters {
    pub(crate) fn take(&self) -> (u64, u64) {
        (
            self.total.swap(0, Ordering::Relaxed),
            self.sequential.swap(0, Ordering::Relaxed),
        )
    }
}

pub(crate) struct RowsExaminedScope {
    previous: Option<Arc<RowsExaminedCounters>>,
    previous_local: u64,
    previous_scan_local: u64,
}

impl Drop for RowsExaminedScope {
    fn drop(&mut self) {
        let local = THREAD_ROWS_EXAMINED.with(|cell| cell.replace(self.previous_local));
        let scan_local =
            THREAD_SCAN_ROWS_EXAMINED.with(|cell| cell.replace(self.previous_scan_local));
        SHARED_ROWS_EXAMINED.with(|slot| {
            if let Some(counter) = slot.borrow().as_ref() {
                counter.total.fetch_add(local, Ordering::Relaxed);
                counter.sequential.fetch_add(scan_local, Ordering::Relaxed);
            }
            *slot.borrow_mut() = self.previous.take();
        });
    }
}

pub(crate) fn enter_rows_examined_counter(
    counter: Option<Arc<RowsExaminedCounters>>,
) -> RowsExaminedScope {
    let previous = SHARED_ROWS_EXAMINED.with(|slot| slot.replace(counter));
    let previous_local = THREAD_ROWS_EXAMINED.with(|cell| cell.replace(0));
    let previous_scan_local = THREAD_SCAN_ROWS_EXAMINED.with(|cell| cell.replace(0));
    RowsExaminedScope {
        previous,
        previous_local,
        previous_scan_local,
    }
}

pub(crate) fn current_rows_examined_counter() -> Option<Arc<RowsExaminedCounters>> {
    SHARED_ROWS_EXAMINED.with(|slot| slot.borrow().clone())
}

pub(crate) fn start_statement_rows_examined() {
    SHARED_ROWS_EXAMINED
        .with(|slot| *slot.borrow_mut() = Some(Arc::new(RowsExaminedCounters::default())));
}

pub(crate) fn clear_statement_rows_examined() {
    let local = THREAD_ROWS_EXAMINED.with(|cell| cell.replace(0));
    let scan_local = THREAD_SCAN_ROWS_EXAMINED.with(|cell| cell.replace(0));
    SHARED_ROWS_EXAMINED.with(|slot| {
        if let Some(counter) = slot.borrow().as_ref() {
            counter.total.fetch_add(local, Ordering::Relaxed);
            counter.sequential.fetch_add(scan_local, Ordering::Relaxed);
        }
        *slot.borrow_mut() = None;
    });
}

/// Accumulate scan rows locally and publish one counter update per scan.
pub(crate) struct RowsExaminedBatch {
    rows: Cell<u64>,
    sequential: bool,
}

impl RowsExaminedBatch {
    pub(crate) fn new() -> Self {
        Self {
            rows: Cell::new(0),
            sequential: true,
        }
    }

    pub(crate) fn new_index() -> Self {
        Self {
            rows: Cell::new(0),
            sequential: false,
        }
    }

    #[inline]
    pub(crate) fn add(&self, n: u64) {
        self.rows.set(self.rows.get().saturating_add(n));
    }
}

impl Drop for RowsExaminedBatch {
    fn drop(&mut self) {
        if self.sequential {
            record_scan_rows_examined(self.rows.get());
        } else {
            record_rows_examined(self.rows.get());
        }
    }
}

pub(crate) fn record_lock_wait_sample(elapsed: Duration) {
    let sample_ps = duration_to_ps(elapsed);
    if sample_ps == 0 {
        return;
    }
    THREAD_LOCK_WAIT_PS.with(|cell| {
        let (count, total_ps) = cell.get();
        cell.set((count.saturating_add(1), total_ps.saturating_add(sample_ps)));
    });
}

pub(crate) fn clear_thread_wait_samples() {
    THREAD_LOCK_WAIT_PS.with(|cell| cell.set((0, 0)));
    THREAD_ROWS_EXAMINED.with(|cell| cell.set(0));
    THREAD_SCAN_ROWS_EXAMINED.with(|cell| cell.set(0));
}

fn take_thread_wait_samples() -> (u64, u64) {
    THREAD_LOCK_WAIT_PS.with(|cell| {
        let value = cell.get();
        cell.set((0, 0));
        value
    })
}

/// Bump the rows-examined counter for the in-flight statement. Called
/// by storage scan paths (sequential, range, index) every time they
/// surface a visible row. Saturating so a runaway scan can't wrap.
pub(crate) fn record_rows_examined(n: u64) {
    if n == 0 {
        return;
    }
    THREAD_ROWS_EXAMINED.with(|cell| cell.set(cell.get().saturating_add(n)));
}

pub(crate) fn record_scan_rows_examined(n: u64) {
    record_rows_examined(n);
    THREAD_SCAN_ROWS_EXAMINED.with(|cell| cell.set(cell.get().saturating_add(n)));
}

/// Drain the rows-examined counter for the current statement. Logging
/// (`SqlLogService::record_slow`) calls this when it formats the
/// statement's slow-log row.
pub(crate) fn take_rows_examined() -> u64 {
    let local = THREAD_ROWS_EXAMINED.with(|cell| cell.replace(0));
    SHARED_ROWS_EXAMINED.with(|slot| {
        slot.borrow().as_ref().map_or(local, |counter| {
            counter
                .total
                .swap(0, Ordering::Relaxed)
                .saturating_add(local)
        })
    })
}

pub(crate) fn take_scan_rows_examined() -> u64 {
    let local = THREAD_SCAN_ROWS_EXAMINED.with(|cell| cell.replace(0));
    SHARED_ROWS_EXAMINED.with(|slot| {
        slot.borrow().as_ref().map_or(local, |counter| {
            counter
                .sequential
                .swap(0, Ordering::Relaxed)
                .saturating_add(local)
        })
    })
}

pub(crate) fn rows_examined() -> u64 {
    let local = THREAD_ROWS_EXAMINED.with(Cell::get);
    SHARED_ROWS_EXAMINED.with(|slot| {
        slot.borrow().as_ref().map_or(local, |counter| {
            counter.total.load(Ordering::Relaxed).saturating_add(local)
        })
    })
}

/// Publish an in-flight statement row before parsing/planning/execution. The
/// completion path reuses its EVENT_ID, so another connection can observe a
/// genuine current event rather than only the last completed statement.
pub(crate) fn begin_statement(session: &SessionContext, sql: &str) {
    if !crate::engine::perf_setup::enabled()
        || crate::engine::perf_setup::statement_consumer_mask()
            & crate::engine::perf_setup::CONSUMER_STATEMENTS_CURRENT
            == 0
        || !crate::engine::perf_setup::global_instrumentation_enabled()
        || !crate::engine::perf_setup::thread_instrumentation_enabled()
        || !crate::engine::perf_setup::thread_enabled(ONE_CONNECTION_THREAD)
        || !crate::engine::perf_setup::actor_enabled(&session.user, &session.host)
    {
        return;
    }
    let prepared_texts = PREPARED_STATEMENT_TEXTS.with(|slot| slot.borrow().clone());
    let raw_trimmed = prepared_texts
        .as_ref()
        .map_or_else(|| sql.trim(), |texts| texts.0.sql_text.as_ref());
    if !raw_trimmed.is_empty() {
        let slot = state_for_thread(session.connection_id as u64).read();
        if slot.as_ref().is_some_and(|state| {
            state
                .threads
                .get(&(session.connection_id as u64))
                .and_then(|thread| thread.current_statement.as_ref())
                .is_some_and(|event| {
                    event.end_event_id.is_none() && event.sql_text.as_ref() == raw_trimmed
                })
        }) {
            return;
        }
    }
    let safe = prepared_texts
        .is_none()
        .then(|| crate::engine::logging::redact_sql_for_log(sql));
    let trimmed = prepared_texts.as_ref().map_or_else(
        || safe.as_deref().unwrap_or_default().trim(),
        |texts| texts.0.sql_text.as_ref(),
    );
    if trimmed.is_empty() {
        return;
    }
    let event_class = statement_event_class(trimmed);
    let event_name = event_class.name;
    if !statement_instrument_enabled(event_class) {
        return;
    }
    let mut slot = state_for_thread(session.connection_id as u64).write();
    let guard = slot.get_or_insert_with(PerfEvents::default);
    let thread = guard
        .threads
        .entry(session.connection_id as u64)
        .or_default();
    let event_id = thread.next_event_id();
    thread.current_statement = Some(StatementEvent {
        thread_id: session.connection_id as u64,
        event_id,
        end_event_id: None,
        event_name,
        timer_start: now_timer_ps(),
        timer_end: 0,
        timer_wait: 0,
        lock_time: 0,
        sql_text: prepared_texts.as_ref().map_or_else(
            || Arc::from(char_prefix(trimmed, MAX_TEXT_LEN)),
            |texts| Arc::clone(&texts.0.sql_text),
        ),
        digest: None,
        digest_text: None,
        current_schema: session.current_database.clone(),
        mysql_errno: None,
        returned_sqlstate: None,
        message_text: None,
        errors: 0,
        warnings: 0,
        rows_affected: 0,
        rows_sent: 0,
        rows_examined: 0,
    });
}

/// Per-thread cache of the derived texts for a statement: the (truncated)
/// SQL sample, and — when the statement has a digestible shape — the
/// normalized digest text plus its SHA-256 hex. `statement_digest_text`
/// re-tokenizes the SQL and `sha256_hex` runs a full hash; at OLTP rates
/// (tens of thousands of prepared executes per second of a few dozen
/// distinct statements) both showed up in TPC-C CPU profiles (~1.5%
/// combined). Prepared statements re-submit byte-identical SQL, so an
/// exact-text cache hits nearly always. Bounded; cleared wholesale on
/// overflow (a burst of distinct literal-heavy texts just stops caching).
#[derive(Debug)]
struct StatementTexts {
    sql_text: Arc<str>,
    digest: Option<(Arc<str>, Arc<str>)>,
}

/// PREPARE-time statement text derivatives. Keeping this on the prepared
/// handle lets COM_STMT_EXECUTE bypass the per-worker SQL-text hash lookup;
/// blocking-pool migration otherwise makes even a tiny per-thread cache pay
/// the full SQL hash on every execution.
#[derive(Clone, Debug)]
pub(crate) struct PreparedStatementTexts(Arc<StatementTexts>);

impl PreparedStatementTexts {
    pub(crate) fn estimated_memory_bytes(&self) -> usize {
        let texts = self.0.as_ref();
        std::mem::size_of::<Self>()
            .saturating_add(texts.sql_text.len())
            .saturating_add(
                texts
                    .digest
                    .as_ref()
                    .map_or(0, |(digest_text, digest)| digest_text.len() + digest.len()),
            )
    }
}

thread_local! {
    static STATEMENT_TEXT_CACHE: RefCell<hashbrown::HashMap<Box<str>, StatementTexts, ahash::RandomState>> =
        RefCell::new(hashbrown::HashMap::default());
    /// Digest artifacts keyed by a *shape* (literal-normalized SQL), kept
    /// apart from [`STATEMENT_TEXT_CACHE`]'s exact-text entries.
    ///
    /// Sharing one map made the two populations fight: literal-bearing
    /// COM_QUERY text is a fresh key on every execution, so the map filled to
    /// its cap within milliseconds and the overflow `clear()` took the handful
    /// of hot shape entries with it. Every clear then re-ran the digest
    /// tokenizer + SHA-256 for each live shape — TPC-C profiling attributed
    /// ~4.5% of on-CPU time to that rebuild loop. Shapes are few and bounded
    /// by the workload's distinct statement forms, so their map effectively
    /// never overflows once separated.
    static STATEMENT_SHAPE_DIGEST_CACHE: RefCell<hashbrown::HashMap<Box<str>, Option<(Arc<str>, Arc<str>)>, ahash::RandomState>> =
        RefCell::new(hashbrown::HashMap::default());
    static STATEMENT_DIGEST_SQL_OVERRIDE: RefCell<Option<Arc<str>>> = const { RefCell::new(None) };
    static PREPARED_STATEMENT_TEXTS: RefCell<Option<PreparedStatementTexts>> = const { RefCell::new(None) };
}

pub(crate) fn prepared_statement_texts(sql: &str) -> PreparedStatementTexts {
    // PREPARE is frequently repeated for byte-identical SQL even when the
    // client closes the server-side handle after each transaction. Reuse the
    // same bounded, per-worker derivative cache as text-protocol statements.
    // Copy the cached strings into this handle rather than sharing their Arcs:
    // PreparedStatement's hard-governor reservation then owns exactly these
    // capacities, with no duplicate billing between cache entries or handles.
    // Redact once at PREPARE rather than once per EXECUTE. This is both the
    // PFS/logging security boundary and the immutable text cached on the
    // prepared handle; record_statement and begin_statement can therefore
    // borrow it without rescanning or allocating on every execution.
    let safe = crate::engine::logging::redact_sql_for_log(sql);
    let cached = statement_texts(safe.trim());
    PreparedStatementTexts(Arc::new(StatementTexts {
        sql_text: Arc::from(cached.sql_text.as_ref()),
        digest: cached
            .digest
            .as_ref()
            .map(|(text, digest)| (Arc::from(text.as_ref()), Arc::from(digest.as_ref()))),
    }))
}

pub(crate) fn with_prepared_statement_texts<R>(
    texts: &PreparedStatementTexts,
    f: impl FnOnce() -> R,
) -> R {
    PREPARED_STATEMENT_TEXTS.with(|slot| {
        let previous = slot.replace(Some(texts.clone()));
        struct RestorePreparedTexts(Option<PreparedStatementTexts>);

        impl Drop for RestorePreparedTexts {
            fn drop(&mut self) {
                PREPARED_STATEMENT_TEXTS.with(|slot| {
                    slot.replace(self.0.take());
                });
            }
        }

        let _restore = RestorePreparedTexts(previous);
        f()
    })
}

pub(crate) fn with_statement_digest_sql_override<R>(
    sql: impl Into<Arc<str>>,
    f: impl FnOnce() -> R,
) -> R {
    STATEMENT_DIGEST_SQL_OVERRIDE.with(|slot| {
        let previous = slot.replace(Some(sql.into()));
        struct RestoreDigestSql(Option<Arc<str>>);

        impl Drop for RestoreDigestSql {
            fn drop(&mut self) {
                STATEMENT_DIGEST_SQL_OVERRIDE.with(|slot| {
                    slot.replace(self.0.take());
                });
            }
        }

        let _restore = RestoreDigestSql(previous);
        f()
    })
}

const STATEMENT_TEXT_CACHE_MAX: usize = 512;

/// Texts longer than this are derived directly instead of cached:
/// literal-heavy bulk / multi-statement bodies are unique per execution,
/// so caching them only churns the map (Box<str> copy per statement) and
/// periodically evicts the hot prepared-statement entries that repeat
/// verbatim.
const STATEMENT_TEXT_CACHE_MAX_KEY_LEN: usize = 4096;

/// Fetch (or compute and cache) the derived texts for `trimmed`.
fn statement_texts(trimmed: &str) -> StatementTexts {
    if let Some(texts) = PREPARED_STATEMENT_TEXTS.with(|slot| slot.borrow().clone()) {
        return StatementTexts {
            sql_text: Arc::clone(&texts.0.sql_text),
            digest: texts
                .0
                .digest
                .as_ref()
                .map(|(text, digest)| (Arc::clone(text), Arc::clone(digest))),
        };
    }
    let digest_override = STATEMENT_DIGEST_SQL_OVERRIDE.with(|slot| slot.borrow().clone());
    if let Some(shape) = digest_override {
        // Literal COM_QUERY traffic has a distinct SQL sample on every
        // execution but a small, parser-proven set of digest shapes. Cache
        // the expensive tokenizer + SHA-256 result by that shape while still
        // returning the exact (truncated) SQL_TEXT sample. Reuse the existing
        // bounded cache rather than adding another thread-local allocation
        // owner; shape entries use an empty sql_text because only their digest
        // is shared.
        return STATEMENT_SHAPE_DIGEST_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            if let Some(entry) = cache.get(shape.as_ref()) {
                return StatementTexts {
                    sql_text: Arc::from(char_prefix(trimmed, MAX_TEXT_LEN)),
                    digest: entry
                        .as_ref()
                        .map(|(text, hash)| (Arc::clone(text), Arc::clone(hash))),
                };
            }
            let digest_text = crate::engine::expr::statement_digest_text(shape.as_ref());
            let digest = if digest_text.is_empty() {
                None
            } else {
                let digest_text = char_prefix(&digest_text, MAX_TEXT_LEN);
                let hash = sha256_hex(digest_text);
                Some((Arc::<str>::from(digest_text), Arc::<str>::from(hash)))
            };
            if cache.len() >= STATEMENT_TEXT_CACHE_MAX {
                cache.clear();
            }
            cache.insert(
                Box::from(shape.as_ref()),
                digest
                    .as_ref()
                    .map(|(text, hash)| (Arc::clone(text), Arc::clone(hash))),
            );
            StatementTexts {
                sql_text: Arc::from(char_prefix(trimmed, MAX_TEXT_LEN)),
                digest,
            }
        });
    }
    if trimmed.len() > STATEMENT_TEXT_CACHE_MAX_KEY_LEN {
        return compute_statement_texts(trimmed);
    }
    STATEMENT_TEXT_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(entry) = cache.get(trimmed) {
            return StatementTexts {
                sql_text: Arc::clone(&entry.sql_text),
                digest: entry
                    .digest
                    .as_ref()
                    .map(|(t, h)| (Arc::clone(t), Arc::clone(h))),
            };
        }
        let texts = compute_statement_texts(trimmed);
        if cache.len() >= STATEMENT_TEXT_CACHE_MAX {
            cache.clear();
        }
        cache.insert(
            Box::from(trimmed),
            StatementTexts {
                sql_text: Arc::clone(&texts.sql_text),
                digest: texts
                    .digest
                    .as_ref()
                    .map(|(t, h)| (Arc::clone(t), Arc::clone(h))),
            },
        );
        texts
    })
}

fn compute_statement_texts(trimmed: &str) -> StatementTexts {
    let sql_text: Arc<str> = Arc::from(char_prefix(trimmed, MAX_TEXT_LEN));
    let digest_text = STATEMENT_DIGEST_SQL_OVERRIDE.with(|slot| {
        let slot = slot.borrow();
        crate::engine::expr::statement_digest_text(slot.as_deref().unwrap_or(trimmed))
    });
    let digest = if digest_text.is_empty() {
        None
    } else {
        let digest_text = char_prefix(&digest_text, MAX_TEXT_LEN);
        let hash = sha256_hex(digest_text);
        Some((
            Arc::<str>::from(digest_text),
            Arc::<str>::from(hash.as_str()),
        ))
    };
    StatementTexts { sql_text, digest }
}

pub(crate) fn record_statement(
    session: &SessionContext,
    sql: &str,
    elapsed: Duration,
    metadata: QueryLogMetadata,
    result: &Result<QueryResponse, QueryError>,
) {
    if !crate::engine::perf_setup::enabled() {
        clear_thread_wait_samples();
        return;
    }
    let digest_enabled = crate::engine::perf_setup::statements_digest_enabled();
    let statement_consumers = crate::engine::perf_setup::statement_consumer_mask();
    let instrumentation_enabled = crate::engine::perf_setup::global_instrumentation_enabled()
        && crate::engine::perf_setup::thread_instrumentation_enabled();
    let (sampled_wait_count, sampled_wait_ps) = take_thread_wait_samples();
    let lock_wait_count = if sampled_wait_count > 0 {
        sampled_wait_count
    } else {
        metadata.lock_wait_count
    };
    let lock_wait_ps = if sampled_wait_ps > 0 {
        sampled_wait_ps
    } else {
        ms_to_ps(metadata.lock_wait_ms)
    };
    if !digest_enabled && !instrumentation_enabled && lock_wait_count == 0 {
        return;
    }

    let prepared_texts = PREPARED_STATEMENT_TEXTS.with(|slot| slot.borrow().clone());
    let safe_sql = prepared_texts
        .is_none()
        .then(|| crate::engine::logging::redact_sql_for_log(sql));
    let trimmed = prepared_texts.as_ref().map_or_else(
        || safe_sql.as_deref().unwrap_or_default().trim(),
        |texts| texts.0.sql_text.as_ref(),
    );
    if trimmed.is_empty() {
        return;
    }

    let event_class = statement_event_class(trimmed);
    let event_name = event_class.name;
    let statement_instrumented = instrumentation_enabled
        && statement_instrument_enabled(event_class)
        && crate::engine::perf_setup::thread_enabled(ONE_CONNECTION_THREAD)
        && crate::engine::perf_setup::actor_enabled(&session.user, &session.host);
    let wait_instrumented = lock_wait_count > 0
        && instrumentation_enabled
        && crate::engine::perf_setup::instrument_enabled(WAIT_EVENT_NAME)
        && crate::engine::perf_setup::thread_enabled(ONE_CONNECTION_THREAD)
        && crate::engine::perf_setup::actor_enabled(&session.user, &session.host);

    let (rows_affected, rows_sent) = match result {
        Ok(response) => (response_affected_rows(response), response.found_rows()),
        Err(_) => (0, 0),
    };

    let want_digest = digest_enabled && statement_instrumented;
    if !want_digest && !statement_instrumented && !wait_instrumented {
        return;
    }
    let texts = statement_texts(trimmed);
    if want_digest {
        if let Some((digest_text, digest_hash)) = texts.digest.as_ref() {
            crate::engine::perf_digest::record(
                &session.current_database,
                trimmed,
                digest_text,
                digest_hash,
                elapsed,
                result.is_err(),
                session.warnings.len() as u64,
                ps_to_ms(lock_wait_ps),
                rows_affected,
                rows_sent,
                metadata.rows_examined,
            );
        }
    }

    if !statement_instrumented && !wait_instrumented {
        return;
    }

    let timer_wait = duration_to_ps(elapsed);
    let timer_end = now_timer_ps();
    let timer_start = timer_end.saturating_sub(timer_wait);
    let lock_time = lock_wait_ps;
    let thread_id = session.connection_id as u64;
    let user = session.user.as_str();
    let host = session.host.as_str();

    let mut slot = state_for_thread(thread_id).write();
    let guard = slot.get_or_insert_with(PerfEvents::default);
    let thread = guard.threads.entry(thread_id).or_default();

    if statement_instrumented {
        let event_id = thread
            .current_statement
            .as_ref()
            .filter(|event| event.end_event_id.is_none())
            .map(|event| event.event_id)
            .unwrap_or_else(|| thread.next_event_id());
        let (errno, sql_state, message) = match result {
            Err(err) => (
                Some(err.code()),
                Some(err.sql_state().to_string()),
                Some(char_prefix(&err.message(), 128).to_owned()),
            ),
            Ok(_) => (None, None, None),
        };
        let (digest_text, digest) = match texts.digest.as_ref() {
            Some((text, hash)) => (Some(Arc::clone(text)), Some(Arc::clone(hash))),
            None => (None, None),
        };
        let event = StatementEvent {
            thread_id,
            event_id,
            end_event_id: Some(event_id),
            event_name,
            timer_start,
            timer_end,
            timer_wait,
            lock_time,
            sql_text: Arc::clone(&texts.sql_text),
            digest,
            digest_text,
            current_schema: session.current_database.clone(),
            mysql_errno: errno,
            returned_sqlstate: sql_state,
            message_text: message,
            errors: result.is_err() as u64,
            warnings: session.warnings.len() as u64,
            rows_affected,
            rows_sent,
            rows_examined: metadata.rows_examined,
        };
        record_statement_event(guard, thread_id, user, host, event, statement_consumers);
    }

    if wait_instrumented {
        let wait_mask = crate::engine::perf_setup::wait_consumer_mask();
        let thread = guard.threads.entry(thread_id).or_default();
        let event_id = thread.next_event_id();
        let event = WaitEvent {
            thread_id,
            event_id,
            end_event_id: event_id,
            timer_start: timer_end.saturating_sub(lock_time),
            timer_end,
            timer_wait: lock_time,
            object_instance: fnv1a_64(WAIT_EVENT_NAME),
            operation: "lock wait",
        };
        record_wait_event(guard, thread_id, user, host, event, wait_mask);
    }
}

fn record_statement_event(
    state: &mut PerfEvents,
    thread_id: u64,
    user: &str,
    host: &str,
    event: StatementEvent,
    consumer_mask: u32,
) {
    state
        .thread_accounts
        .entry(thread_id)
        .or_insert_with(|| (user.to_string(), host.to_string()));
    let thread = state.threads.entry(thread_id).or_default();
    if consumer_mask & crate::engine::perf_setup::CONSUMER_STATEMENTS_CURRENT != 0 {
        thread.current_statement = Some(event.clone());
    }
    if consumer_mask & crate::engine::perf_setup::CONSUMER_STATEMENTS_HISTORY != 0
        && crate::engine::perf_setup::thread_history_enabled(ONE_CONNECTION_THREAD)
        && crate::engine::perf_setup::actor_history_enabled(user, host)
    {
        push_bounded(
            &mut thread.statement_history,
            event.clone(),
            statement_history_limit(),
        );
    }
    if consumer_mask & crate::engine::perf_setup::CONSUMER_STATEMENTS_HISTORY_LONG != 0
        && crate::engine::perf_setup::thread_history_enabled(ONE_CONNECTION_THREAD)
        && crate::engine::perf_setup::actor_history_enabled(user, host)
    {
        let limit = state.statement_history_long_limit;
        push_bounded(&mut state.statement_history_long, event.clone(), limit);
    }
    upsert_statement(&mut state.statement_global, event.event_name, &event);
    state
        .statement_by_thread
        .entry((thread_id, event.event_name))
        .or_insert_with(|| LiveAggregate::new(StatementAggregate::new(event.event_name)))
        .record(|aggregate| aggregate.record(&event));
}

fn record_wait_event(
    state: &mut PerfEvents,
    thread_id: u64,
    user: &str,
    host: &str,
    event: WaitEvent,
    consumer_mask: u32,
) {
    state
        .thread_accounts
        .entry(thread_id)
        .or_insert_with(|| (user.to_string(), host.to_string()));
    let thread = state.threads.entry(thread_id).or_default();
    if consumer_mask & crate::engine::perf_setup::CONSUMER_WAITS_CURRENT != 0 {
        thread.current_wait = Some(event.clone());
    }
    if consumer_mask & crate::engine::perf_setup::CONSUMER_WAITS_HISTORY != 0
        && crate::engine::perf_setup::thread_history_enabled(ONE_CONNECTION_THREAD)
        && crate::engine::perf_setup::actor_history_enabled(user, host)
    {
        push_bounded(
            &mut thread.wait_history,
            event.clone(),
            wait_history_limit(),
        );
    }
    if consumer_mask & crate::engine::perf_setup::CONSUMER_WAITS_HISTORY_LONG != 0
        && crate::engine::perf_setup::thread_history_enabled(ONE_CONNECTION_THREAD)
        && crate::engine::perf_setup::actor_history_enabled(user, host)
    {
        let limit = state.wait_history_long_limit;
        push_bounded(&mut state.wait_history_long, event.clone(), limit);
    }
    upsert_wait(&mut state.wait_global, WAIT_EVENT_NAME, &event);
    state
        .wait_by_thread
        .entry((thread_id, WAIT_EVENT_NAME))
        .or_insert_with(|| {
            LiveAggregate::new(WaitAggregate::new(WAIT_EVENT_NAME, event.object_instance))
        })
        .record(|aggregate| aggregate.record(&event));
    upsert_wait(
        &mut state.wait_by_instance,
        (WAIT_EVENT_NAME, event.object_instance),
        &event,
    );
}

fn upsert_statement<K: Ord>(
    map: &mut BTreeMap<K, StatementAggregate>,
    key: K,
    event: &StatementEvent,
) {
    map.entry(key)
        .or_insert_with(|| StatementAggregate::new(event.event_name))
        .record(event);
}

fn upsert_wait<K: Ord>(map: &mut BTreeMap<K, WaitAggregate>, key: K, event: &WaitEvent) {
    map.entry(key)
        .or_insert_with(|| WaitAggregate::new(WAIT_EVENT_NAME, event.object_instance))
        .record(event);
}

fn push_bounded<T>(queue: &mut VecDeque<T>, item: T, max: usize) {
    if max == 0 {
        return;
    }
    if queue.len() >= max {
        queue.pop_front();
    }
    queue.push_back(item);
}

pub(crate) fn statement_current_rows() -> Vec<Vec<Value>> {
    collect_shard_rows(|state, rows| {
        rows.extend(
            state
                .threads
                .values()
                .filter_map(|thread| thread.current_statement.as_ref())
                .map(statement_row),
        );
    })
}

pub(crate) fn statement_history_rows() -> Vec<Vec<Value>> {
    collect_shard_rows(|state, rows| {
        rows.extend(
            state
                .threads
                .values()
                .flat_map(|thread| thread.statement_history.iter().map(statement_row)),
        );
    })
}

pub(crate) fn statement_history_long_rows() -> Vec<Vec<Value>> {
    collect_shard_rows(|state, rows| {
        rows.extend(state.statement_history_long.iter().map(statement_row));
    })
}

pub(crate) fn wait_current_rows() -> Vec<Vec<Value>> {
    collect_shard_rows(|state, rows| {
        rows.extend(
            state
                .threads
                .values()
                .filter_map(|thread| thread.current_wait.as_ref())
                .map(wait_row),
        );
    })
}

pub(crate) fn wait_history_rows() -> Vec<Vec<Value>> {
    collect_shard_rows(|state, rows| {
        rows.extend(
            state
                .threads
                .values()
                .flat_map(|thread| thread.wait_history.iter().map(wait_row)),
        );
    })
}

pub(crate) fn wait_history_long_rows() -> Vec<Vec<Value>> {
    collect_shard_rows(|state, rows| {
        rows.extend(state.wait_history_long.iter().map(wait_row));
    })
}

pub(crate) fn statement_summary_global_rows() -> Vec<Vec<Value>> {
    merged_summary_map(|state| &state.statement_global, StatementAggregate::merge)
        .into_values()
        .map(|agg| aggregate_row([s(agg.event_name)], statement_aggregate_tail(&agg)))
        .collect()
}

pub(crate) fn statement_summary_by_thread_rows() -> Vec<Vec<Value>> {
    collect_shard_rows(|state, rows| {
        for ((thread_id, _), live) in &state.statement_by_thread {
            let agg = &live.thread;
            if agg.metrics.count_star == 0 {
                continue;
            }
            rows.push(aggregate_row(
                [u(*thread_id), s(agg.event_name)],
                statement_aggregate_tail(agg),
            ));
        }
    })
}

pub(crate) fn statement_summary_by_user_rows() -> Vec<Vec<Value>> {
    statement_by_user_snapshot()
        .into_iter()
        .map(|((user, _), agg)| {
            aggregate_row(
                [s(&user), s(agg.event_name)],
                statement_aggregate_tail(&agg),
            )
        })
        .collect()
}

pub(crate) fn statement_summary_by_host_rows() -> Vec<Vec<Value>> {
    statement_by_host_snapshot()
        .into_iter()
        .map(|((host, _), agg)| {
            aggregate_row(
                [s(&host), s(agg.event_name)],
                statement_aggregate_tail(&agg),
            )
        })
        .collect()
}

pub(crate) fn statement_summary_by_account_rows() -> Vec<Vec<Value>> {
    statement_by_account_snapshot()
        .into_iter()
        .map(|((user, host, _), agg)| {
            aggregate_row(
                [s(&user), s(&host), s(agg.event_name)],
                statement_aggregate_tail(&agg),
            )
        })
        .collect()
}

pub(crate) fn wait_summary_global_rows() -> Vec<Vec<Value>> {
    merged_summary_map(|state| &state.wait_global, WaitAggregate::merge)
        .into_values()
        .map(|agg| aggregate_row([s(agg.event_name)], wait_aggregate_tail(&agg)))
        .collect()
}

pub(crate) fn wait_summary_by_thread_rows() -> Vec<Vec<Value>> {
    collect_shard_rows(|state, rows| {
        for ((thread_id, _), live) in &state.wait_by_thread {
            let agg = &live.thread;
            if agg.count_star == 0 {
                continue;
            }
            rows.push(aggregate_row(
                [u(*thread_id), s(agg.event_name)],
                wait_aggregate_tail(agg),
            ));
        }
    })
}

pub(crate) fn wait_summary_by_user_rows() -> Vec<Vec<Value>> {
    wait_by_user_snapshot()
        .into_iter()
        .map(|((user, _), agg)| {
            aggregate_row([s(&user), s(agg.event_name)], wait_aggregate_tail(&agg))
        })
        .collect()
}

pub(crate) fn wait_summary_by_host_rows() -> Vec<Vec<Value>> {
    wait_by_host_snapshot()
        .into_iter()
        .map(|((host, _), agg)| {
            aggregate_row([s(&host), s(agg.event_name)], wait_aggregate_tail(&agg))
        })
        .collect()
}

pub(crate) fn wait_summary_by_account_rows() -> Vec<Vec<Value>> {
    wait_by_account_snapshot()
        .into_iter()
        .map(|((user, host, _), agg)| {
            aggregate_row(
                [s(&user), s(&host), s(agg.event_name)],
                wait_aggregate_tail(&agg),
            )
        })
        .collect()
}

pub(crate) fn wait_summary_by_instance_rows() -> Vec<Vec<Value>> {
    merged_summary_map(|state| &state.wait_by_instance, WaitAggregate::merge)
        .into_values()
        .map(|agg| {
            aggregate_row(
                [s(agg.event_name), u(agg.object_instance)],
                wait_aggregate_tail(&agg),
            )
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn reset() {
    for shard in states() {
        *shard.write() = None;
    }
}

pub(crate) fn truncate(table: &str) -> bool {
    if !matches!(
        table,
        "events_statements_current"
            | "events_statements_history"
            | "events_statements_history_long"
            | "events_waits_current"
            | "events_waits_history"
            | "events_waits_history_long"
            | "events_statements_summary_global_by_event_name"
            | "events_statements_summary_by_thread_by_event_name"
            | "events_statements_summary_by_user_by_event_name"
            | "events_statements_summary_by_host_by_event_name"
            | "events_statements_summary_by_account_by_event_name"
            | "events_waits_summary_global_by_event_name"
            | "events_waits_summary_by_thread_by_event_name"
            | "events_waits_summary_by_user_by_event_name"
            | "events_waits_summary_by_host_by_event_name"
            | "events_waits_summary_by_account_by_event_name"
            | "events_waits_summary_by_instance"
    ) {
        return false;
    }
    for shard in states() {
        let mut slot = shard.write();
        let Some(guard) = slot.as_mut() else {
            continue;
        };
        match table {
            "events_statements_current" => {
                for thread in guard.threads.values_mut() {
                    thread.current_statement = None;
                }
            }
            "events_statements_history" => {
                for thread in guard.threads.values_mut() {
                    thread.statement_history.clear();
                }
            }
            "events_statements_history_long" => guard.statement_history_long.clear(),
            "events_waits_current" => {
                for thread in guard.threads.values_mut() {
                    thread.current_wait = None;
                }
            }
            "events_waits_history" => {
                for thread in guard.threads.values_mut() {
                    thread.wait_history.clear();
                }
            }
            "events_waits_history_long" => guard.wait_history_long.clear(),
            "events_statements_summary_global_by_event_name" => guard.statement_global.clear(),
            "events_statements_summary_by_thread_by_event_name" => {
                reset_statement_dimension(guard, 0)
            }
            "events_statements_summary_by_user_by_event_name" => {
                reset_statement_dimension(guard, 1)
            }
            "events_statements_summary_by_host_by_event_name" => {
                reset_statement_dimension(guard, 2)
            }
            "events_statements_summary_by_account_by_event_name" => {
                reset_statement_dimension(guard, 3)
            }
            "events_waits_summary_global_by_event_name" => guard.wait_global.clear(),
            "events_waits_summary_by_thread_by_event_name" => reset_wait_dimension(guard, 0),
            "events_waits_summary_by_user_by_event_name" => reset_wait_dimension(guard, 1),
            "events_waits_summary_by_host_by_event_name" => reset_wait_dimension(guard, 2),
            "events_waits_summary_by_account_by_event_name" => reset_wait_dimension(guard, 3),
            "events_waits_summary_by_instance" => guard.wait_by_instance.clear(),
            _ => unreachable!("validated event table"),
        }
    }
    true
}

fn reset_statement_dimension(state: &mut PerfEvents, dimension: usize) {
    match dimension {
        0 => {}
        1 => state.statement_by_user.clear(),
        2 => state.statement_by_host.clear(),
        3 => state.statement_by_account.clear(),
        _ => unreachable!("summary dimension"),
    }
    for live in state.statement_by_thread.values_mut() {
        live.reset(dimension, StatementAggregate::new(live.thread.event_name));
    }
}

fn reset_wait_dimension(state: &mut PerfEvents, dimension: usize) {
    match dimension {
        0 => {}
        1 => state.wait_by_user.clear(),
        2 => state.wait_by_host.clear(),
        3 => state.wait_by_account.clear(),
        _ => unreachable!("summary dimension"),
    }
    for live in state.wait_by_thread.values_mut() {
        live.reset(
            dimension,
            WaitAggregate::new(live.thread.event_name, live.thread.object_instance),
        );
    }
}

/// Defer identity-string allocation until a nonempty reset epoch is retired.
fn merge_nonempty_summary<K: Ord, A: SummaryAggregate>(
    summaries: &mut BTreeMap<K, A>,
    key: impl FnOnce() -> K,
    aggregate: &A,
) {
    if !aggregate.is_empty() {
        summaries
            .entry(key())
            .and_modify(|current| current.merge(aggregate))
            .or_insert_with(|| aggregate.clone());
    }
}

fn merge_thread_summary_maps<A: SummaryAggregate>(
    thread_id: u64,
    user: &str,
    host: &str,
    live: &ThreadSummaryMap<A>,
    by_user: &mut IdentitySummaryMap<A>,
    by_host: &mut IdentitySummaryMap<A>,
    by_account: &mut AccountSummaryMap<A>,
) {
    for ((_, event), counters) in live
        .range((thread_id, "")..)
        .take_while(|((candidate, _), _)| *candidate == thread_id)
    {
        merge_nonempty_summary(by_user, || (user.to_owned(), *event), counters.rollup(0));
        merge_nonempty_summary(by_host, || (host.to_owned(), *event), counters.rollup(1));
        merge_nonempty_summary(
            by_account,
            || (user.to_owned(), host.to_owned(), *event),
            counters.rollup(2),
        );
    }
}

fn merge_thread_rollups(guard: &mut PerfEvents, thread_id: u64) {
    let Some((user, host)) = guard.thread_accounts.get(&thread_id) else {
        return;
    };
    merge_thread_summary_maps(
        thread_id,
        user,
        host,
        &guard.statement_by_thread,
        &mut guard.statement_by_user,
        &mut guard.statement_by_host,
        &mut guard.statement_by_account,
    );
    merge_thread_summary_maps(
        thread_id,
        user,
        host,
        &guard.wait_by_thread,
        &mut guard.wait_by_user,
        &mut guard.wait_by_host,
        &mut guard.wait_by_account,
    );
}

pub(crate) fn unregister_thread(thread_id: u64) {
    let mut slot = state_for_thread(thread_id).write();
    let Some(guard) = slot.as_mut() else {
        return;
    };
    merge_thread_rollups(guard, thread_id);
    guard.threads.remove(&thread_id);
    guard.thread_accounts.remove(&thread_id);
    guard
        .statement_by_thread
        .retain(|(candidate, _), _| *candidate != thread_id);
    guard
        .wait_by_thread
        .retain(|(candidate, _), _| *candidate != thread_id);
}

fn statement_row(event: &StatementEvent) -> Vec<Value> {
    let row = vec![
        u(event.thread_id),
        u(event.event_id),
        event.end_event_id.map(u).unwrap_or(Value::Null),
        s(event.event_name),
        Value::Null,
        u(event.timer_start),
        u(event.timer_end),
        u(event.timer_wait),
        u(event.lock_time),
        s(&event.sql_text),
        opt_arc_str(event.digest.as_deref()),
        opt_arc_str(event.digest_text.as_deref()),
        s(&event.current_schema),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        event
            .mysql_errno
            .map(|v| Value::Int(v as i64))
            .unwrap_or(Value::Null),
        opt_string(event.returned_sqlstate.clone()),
        opt_string(event.message_text.clone()),
        u(event.errors),
        u(event.warnings),
        u(event.rows_affected),
        u(event.rows_sent),
        u(event.rows_examined),
        // Thirteen CREATED_TMP/SELECT/SORT/NO_INDEX counters precede nesting metadata.
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        Value::Null,
        Value::Null,
        Value::Int(0),
        u(event.event_id),
        u(0),
        u(0),
        u(0),
        s("PRIMARY"),
    ];
    debug_assert_eq!(row.len(), 46);
    row
}

fn wait_row(event: &WaitEvent) -> Vec<Value> {
    vec![
        u(event.thread_id),
        u(event.event_id),
        u(event.end_event_id),
        s(WAIT_EVENT_NAME),
        Value::Null,
        u(event.timer_start),
        u(event.timer_end),
        u(event.timer_wait),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        u(event.object_instance),
        Value::Null,
        Value::Null,
        s(event.operation),
        Value::Null,
        Value::Null,
    ]
}

fn aggregate_row<const PREFIX: usize, const TAIL: usize>(
    prefix: [Value; PREFIX],
    tail: [Value; TAIL],
) -> Vec<Value> {
    let mut row = Vec::with_capacity(PREFIX + TAIL);
    row.extend(prefix);
    row.extend(tail);
    row
}

fn statement_aggregate_tail(agg: &StatementAggregate) -> [Value; 28] {
    [
        u(agg.metrics.count_star),
        u(agg.metrics.sum_timer_wait),
        u(min_value(
            agg.metrics.min_timer_wait,
            agg.metrics.count_star,
        )),
        u(avg(agg.metrics.sum_timer_wait, agg.metrics.count_star)),
        u(agg.metrics.max_timer_wait),
        u(agg.metrics.sum_lock_time),
        u(agg.metrics.sum_errors),
        u(agg.metrics.sum_warnings),
        u(agg.metrics.sum_rows_affected),
        u(agg.metrics.sum_rows_sent),
        u(agg.metrics.sum_rows_examined),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
        u(0),
    ]
}

fn wait_aggregate_tail(agg: &WaitAggregate) -> [Value; 5] {
    [
        u(agg.count_star),
        u(agg.sum_timer_wait),
        u(min_value(agg.min_timer_wait, agg.count_star)),
        u(avg(agg.sum_timer_wait, agg.count_star)),
        u(agg.max_timer_wait),
    ]
}

#[derive(Clone, Copy)]
struct StatementEventClass {
    name: &'static str,
    index: usize,
}

// Specific forms precede their shorter prefixes. Unknown extensions retain
// the registered abstract Query instrument instead of disappearing entirely.
const STATEMENT_EVENT_NAMES: &[(&str, &str)] = &[
    ("select", "statement/sql/select"),
    ("insert", "statement/sql/insert"),
    ("update", "statement/sql/update"),
    ("delete", "statement/sql/delete"),
    (
        "create spatial reference system",
        "statement/sql/create_spatial_reference_system",
    ),
    (
        "drop spatial reference system",
        "statement/sql/drop_spatial_reference_system",
    ),
    ("show binary log status", "statement/sql/show_master_status"),
    ("show create table", "statement/sql/show_create_table"),
    ("show table status", "statement/sql/show_table_status"),
    (
        "rollback to savepoint",
        "statement/sql/rollback_to_savepoint",
    ),
    ("show open tables", "statement/sql/show_open_tables"),
    (
        "show procedure status",
        "statement/sql/show_procedure_status",
    ),
    ("show function status", "statement/sql/show_function_status"),
    ("show create event", "statement/sql/show_create_event"),
    ("show create trigger", "statement/sql/show_create_trigger"),
    ("show create user", "statement/sql/show_create_user"),
    (
        "create resource group",
        "statement/sql/create_resource_group",
    ),
    ("alter resource group", "statement/sql/alter_resource_group"),
    ("drop resource group", "statement/sql/drop_resource_group"),
    ("set resource group", "statement/sql/set_resource_group"),
    (
        "change replication source",
        "statement/sql/change_replication_source",
    ),
    (
        "change replication filter",
        "statement/sql/change_repl_filter",
    ),
    ("show binary logs", "statement/sql/show_binlogs"),
    ("show master status", "statement/sql/show_master_status"),
    ("show replica status", "statement/sql/show_slave_status"),
    ("show slave status", "statement/sql/show_slave_status"),
    ("show slave hosts", "statement/sql/show_slave_hosts"),
    ("show binlog events", "statement/sql/show_binlog_events"),
    ("show relaylog events", "statement/sql/show_relaylog_events"),
    ("show storage engines", "statement/sql/show_storage_engines"),
    ("show character set", "statement/sql/show_charsets"),
    ("show create procedure", "statement/sql/show_create_proc"),
    ("show create function", "statement/sql/show_create_func"),
    ("explain for connection", "statement/sql/explain_other"),
    ("create table", "statement/sql/create_table"),
    ("create index", "statement/sql/create_index"),
    ("alter table", "statement/sql/alter_table"),
    ("drop table", "statement/sql/drop_table"),
    ("drop index", "statement/sql/drop_index"),
    ("show databases", "statement/sql/show_databases"),
    ("show tables", "statement/sql/show_tables"),
    ("show variables", "statement/sql/show_variables"),
    ("show status", "statement/sql/show_status"),
    ("show processlist", "statement/sql/show_processlist"),
    ("show grants", "statement/sql/show_grants"),
    ("show triggers", "statement/sql/show_triggers"),
    ("lock tables", "statement/sql/lock_tables"),
    ("unlock tables", "statement/sql/unlock_tables"),
    ("release savepoint", "statement/sql/release_savepoint"),
    ("rename table", "statement/sql/rename_table"),
    ("show warnings", "statement/sql/show_warnings"),
    ("show errors", "statement/sql/show_errors"),
    ("show privileges", "statement/sql/show_privileges"),
    ("create user", "statement/sql/create_user"),
    ("drop user", "statement/sql/drop_user"),
    ("rename user", "statement/sql/rename_user"),
    ("create procedure", "statement/sql/create_procedure"),
    ("create function", "statement/sql/create_function"),
    ("drop procedure", "statement/sql/drop_procedure"),
    ("alter procedure", "statement/sql/alter_procedure"),
    ("alter function", "statement/sql/alter_function"),
    ("create view", "statement/sql/create_view"),
    ("drop view", "statement/sql/drop_view"),
    ("create trigger", "statement/sql/create_trigger"),
    ("drop trigger", "statement/sql/drop_trigger"),
    ("xa start", "statement/sql/xa_start"),
    ("xa end", "statement/sql/xa_end"),
    ("xa prepare", "statement/sql/xa_prepare"),
    ("xa commit", "statement/sql/xa_commit"),
    ("xa rollback", "statement/sql/xa_rollback"),
    ("xa recover", "statement/sql/xa_recover"),
    ("alter tablespace", "statement/sql/alter_tablespace"),
    ("install plugin", "statement/sql/install_plugin"),
    ("uninstall plugin", "statement/sql/uninstall_plugin"),
    ("show plugins", "statement/sql/show_plugins"),
    ("create server", "statement/sql/create_server"),
    ("drop server", "statement/sql/drop_server"),
    ("alter server", "statement/sql/alter_server"),
    ("create event", "statement/sql/create_event"),
    ("alter event", "statement/sql/alter_event"),
    ("drop event", "statement/sql/drop_event"),
    ("show events", "statement/sql/show_events"),
    ("show profile", "statement/sql/show_profile"),
    ("show profiles", "statement/sql/show_profiles"),
    ("get diagnostics", "statement/sql/get_diagnostics"),
    ("alter user", "statement/sql/alter_user"),
    ("set password", "statement/sql/set_password"),
    ("alter instance", "statement/sql/alter_instance"),
    ("install component", "statement/sql/install_component"),
    ("uninstall component", "statement/sql/uninstall_component"),
    ("create role", "statement/sql/create_role"),
    ("drop role", "statement/sql/drop_role"),
    ("set role", "statement/sql/set_role"),
    ("lock instance", "statement/sql/lock_instance"),
    ("unlock instance", "statement/sql/unlock_instance"),
    ("deallocate prepare", "statement/sql/dealloc_sql"),
    ("drop prepare", "statement/sql/dealloc_sql"),
    ("start transaction", "statement/sql/begin"),
    ("start replica", "statement/sql/slave_start"),
    ("start slave", "statement/sql/slave_start"),
    ("stop replica", "statement/sql/slave_stop"),
    ("stop slave", "statement/sql/slave_stop"),
    ("change master", "statement/sql/change_replication_source"),
    ("show replicas", "statement/sql/show_slave_hosts"),
    ("show engines", "statement/sql/show_storage_engines"),
    ("show columns", "statement/sql/show_fields"),
    ("show fields", "statement/sql/show_fields"),
    ("show index", "statement/sql/show_keys"),
    ("show indexes", "statement/sql/show_keys"),
    ("show keys", "statement/sql/show_keys"),
    ("show charset", "statement/sql/show_charsets"),
    ("show collation", "statement/sql/show_collations"),
    ("alter view", "statement/sql/create_view"),
    ("create database", "statement/sql/create_db"),
    ("alter database", "statement/sql/alter_db"),
    ("drop database", "statement/sql/drop_db"),
    ("create schema", "statement/sql/create_db"),
    ("alter schema", "statement/sql/alter_db"),
    ("drop schema", "statement/sql/drop_db"),
    ("truncate", "statement/sql/truncate"),
    ("load", "statement/sql/load"),
    ("grant", "statement/sql/grant"),
    ("repair", "statement/sql/repair"),
    ("replace", "statement/sql/replace"),
    ("revoke", "statement/sql/revoke"),
    ("optimize", "statement/sql/optimize"),
    ("check", "statement/sql/check"),
    ("flush", "statement/sql/flush"),
    ("kill", "statement/sql/kill"),
    ("analyze", "statement/sql/analyze"),
    ("rollback", "statement/sql/rollback"),
    ("commit", "statement/sql/commit"),
    ("savepoint", "statement/sql/savepoint"),
    ("begin", "statement/sql/begin"),
    ("reset", "statement/sql/reset"),
    ("purge", "statement/sql/purge"),
    ("do", "statement/sql/do"),
    ("help", "statement/sql/help"),
    ("checksum", "statement/sql/checksum"),
    ("binlog", "statement/sql/binlog"),
    ("signal", "statement/sql/signal"),
    ("resignal", "statement/sql/resignal"),
    ("shutdown", "statement/sql/shutdown"),
    ("import", "statement/sql/import"),
    ("restart", "statement/sql/restart"),
    ("set", "statement/sql/set_option"),
    ("use", "statement/sql/change_db"),
    ("prepare", "statement/sql/prepare_sql"),
    ("execute", "statement/sql/execute_sql"),
    ("call", "statement/sql/call_procedure"),
    ("explain", "statement/sql/select"),
    ("describe", "statement/sql/show_fields"),
    ("desc", "statement/sql/show_fields"),
    ("", "statement/abstract/Query"),
];

/// Read only the command header, without allocating or scanning SQL bodies.
/// Comments between keywords have the same meaning as leading comments.
fn next_statement_word<'a>(sql: &mut &'a str) -> &'a str {
    loop {
        *sql = sql.trim_start();
        if sql.starts_with("/*") {
            if let Some(end) = sql.find("*/") {
                *sql = &sql[end + 2..];
                continue;
            }
            return "";
        }
        if sql.starts_with('#')
            || (sql.starts_with("--")
                && sql
                    .as_bytes()
                    .get(2)
                    .is_none_or(|b| b.is_ascii_whitespace()))
        {
            *sql = sql.find('\n').map_or("", |end| &sql[end + 1..]);
            continue;
        }
        let end = sql
            .bytes()
            .take_while(|b| b.is_ascii_alphanumeric() || *b == b'_')
            .count();
        let word = &sql[..end];
        *sql = &sql[end..];
        return word;
    }
}

/// `prefix` is a catalog constant of single-space separated keywords. Walking
/// it with `split_once` rejects on the first differing keyword without the
/// Unicode-aware whitespace iterator, which dominated classification of the
/// non-DML statements (COMMIT, SET, ...) that scan the whole catalog.
#[inline]
fn statement_prefix_matches(prefix: &str, words: &[&str; 5]) -> bool {
    let mut rest = prefix;
    for word in words {
        if rest.is_empty() {
            break;
        }
        let (keyword, tail) = rest.split_once(' ').unwrap_or((rest, ""));
        if !keyword.eq_ignore_ascii_case(word) {
            return false;
        }
        rest = tail;
    }
    true
}

fn statement_event_class(mut sql: &str) -> StatementEventClass {
    let first = next_statement_word(&mut sql);
    for (index, &(keyword, name)) in STATEMENT_EVENT_NAMES[..4].iter().enumerate() {
        if first.eq_ignore_ascii_case(keyword) {
            return StatementEventClass { name, index };
        }
    }
    let mut words = [""; 5];
    words[0] = first;
    for word in &mut words[1..] {
        *word = next_statement_word(&mut sql);
        // Optional command-header modifiers do not change the instrument.
        while [
            "temporary",
            "global",
            "session",
            "full",
            "unique",
            "or",
            "replace",
        ]
        .iter()
        .any(|modifier| word.eq_ignore_ascii_case(modifier))
        {
            *word = next_statement_word(&mut sql);
        }
    }
    for (offset, &(prefix, name)) in STATEMENT_EVENT_NAMES[4..STATEMENT_EVENT_NAMES.len() - 1]
        .iter()
        .enumerate()
    {
        if statement_prefix_matches(prefix, &words) {
            return StatementEventClass {
                name,
                index: offset + 4,
            };
        }
    }
    StatementEventClass {
        name: "statement/abstract/Query",
        index: STATEMENT_EVENT_NAMES.len() - 1,
    }
}

fn statement_instrument_enabled(class: StatementEventClass) -> bool {
    // Resolve the fixture's large name catalog once. Every statement only
    // reads the selected instrument's runtime flag, as on the original path.
    static INDEXES: OnceLock<[Option<usize>; STATEMENT_EVENT_NAMES.len()]> = OnceLock::new();
    let indexes = INDEXES.get_or_init(|| {
        std::array::from_fn(|index| {
            crate::engine::perf_setup::instrument_index(STATEMENT_EVENT_NAMES[index].1)
        })
    });
    indexes[class.index].is_some_and(crate::engine::perf_setup::instrument_enabled_at)
}

fn response_affected_rows(response: &QueryResponse) -> u64 {
    match response {
        QueryResponse::OkAffected { affected_rows } => *affected_rows,
        QueryResponse::MultiResult(results) => results.iter().map(response_affected_rows).sum(),
        QueryResponse::Ok
        | QueryResponse::ResultSet(_)
        | QueryResponse::SharedResultSet(_)
        | QueryResponse::StreamingResultSet(_) => 0,
    }
}

fn now_timer_ps() -> u64 {
    duration_to_ps(timer_origin().elapsed())
}

fn duration_to_ps(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos().saturating_mul(1_000)).unwrap_or(u64::MAX)
}

fn ms_to_ps(ms: f64) -> u64 {
    if ms > 0.0 {
        (ms * 1_000_000_000.0) as u64
    } else {
        0
    }
}

fn ps_to_ms(ps: u64) -> f64 {
    ps as f64 / 1_000_000_000.0
}

fn avg(sum: u64, count: u64) -> u64 {
    if count == 0 {
        0
    } else {
        sum / count
    }
}

fn min_value(min: u64, count: u64) -> u64 {
    if count == 0 || min == u64::MAX {
        0
    } else {
        min
    }
}

fn s(value: &str) -> Value {
    Value::String(value.to_string())
}

fn u(value: u64) -> Value {
    Value::UInt(value)
}

fn opt_string(value: Option<String>) -> Value {
    value.map(Value::String).unwrap_or(Value::Null)
}

fn opt_arc_str(value: Option<&str>) -> Value {
    value
        .map(|v| Value::String(v.to_string()))
        .unwrap_or(Value::Null)
}

fn sha256_hex(text: &str) -> String {
    use sha2::Digest;
    crate::util::hex::encode_lower(&sha2::Sha256::digest(text.as_bytes()))
}

fn fnv1a_64(text: &str) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for byte in text.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pfs_audit_classification_uses_registered_instruments() {
        for &(_, name) in STATEMENT_EVENT_NAMES {
            assert!(
                crate::engine::perf_setup::instrument_index(name).is_some(),
                "{name}"
            );
        }
        for (sql, expected) in [
            ("/*+ hint */ SELECT 1", "select"),
            ("-- comment\nSELECT 2", "select"),
            ("# comment\n/* second */ INSERT INTO t VALUES (1)", "insert"),
            (
                "CREATE /* comment */ TEMPORARY TABLE t(x INT)",
                "create_table",
            ),
            ("CREATE UNIQUE INDEX i ON t(x)", "create_index"),
            ("CREATE OR REPLACE VIEW v AS SELECT 1", "create_view"),
            ("SET GLOBAL max_connections=100", "set_option"),
            ("SHOW FULL TABLES", "show_tables"),
            ("SHOW SESSION STATUS", "show_status"),
            ("PREPARE s FROM 'SELECT 1'", "prepare_sql"),
            ("EXECUTE s", "execute_sql"),
            ("DEALLOCATE PREPARE s", "dealloc_sql"),
            ("EXPLAIN SELECT 1", "select"),
            ("TRUNCATE TABLE t", "truncate"),
            ("ROLLBACK TO SAVEPOINT a", "rollback_to_savepoint"),
            ("ALTER DATABASE test CHARACTER SET utf8mb4", "alter_db"),
            ("GRANT SELECT ON test.* TO a", "grant"),
        ] {
            assert_eq!(
                statement_event_class(sql).name,
                format!("statement/sql/{expected}"),
                "{sql}"
            );
        }
        assert!(statement_instrument_enabled(statement_event_class(
            "MYDB_EXTENSION something"
        )));
    }

    #[test]
    fn pfs_audit_statement_rows_match_fixture_and_summary_truncate_isolated() {
        reset();
        let mut session = SessionContext::default();
        session.connection_id = 7_800_001;
        record_statement(
            &session,
            "SELECT 123",
            Duration::from_micros(10),
            QueryLogMetadata {
                lock_wait_count: 1,
                lock_wait_ms: 1.0,
                ..Default::default()
            },
            &Ok(QueryResponse::Ok),
        );
        for (table, rows) in [
            ("events_statements_current", statement_current_rows()),
            ("events_statements_history", statement_history_rows()),
        ] {
            let columns = crate::engine::information_schema::columns_for_table_name(&format!(
                "performance_schema.{table}"
            ))
            .unwrap();
            let row = rows.iter().find(|row| row[0] == u(7_800_001)).unwrap();
            assert_eq!(row.len(), columns.len());
            assert_eq!(row[37], u(0));
            assert_eq!(row[39], Value::Null);
            assert_eq!(row[40], Value::Int(0));
            assert_eq!(row[41], row[1]);
            assert_eq!(row[44], u(0));
            assert_eq!(row[45], s("PRIMARY"));
        }
        let global = statement_summary_global_rows();
        let users = statement_summary_by_user_rows();
        assert!(truncate(
            "events_statements_summary_by_thread_by_event_name"
        ));
        assert!(statement_summary_by_thread_rows().is_empty());
        assert_eq!(statement_summary_global_rows(), global);
        assert_eq!(statement_summary_by_user_rows(), users);
        let waits = wait_summary_global_rows();
        assert!(!waits.is_empty());
        assert!(truncate("events_waits_summary_by_instance"));
        assert!(wait_summary_by_instance_rows().is_empty());
        assert_eq!(wait_summary_global_rows(), waits);
        unregister_thread(7_800_001);
    }

    #[test]
    fn pfs_audit_all_wait_dimensions_and_disconnect_preserve_resets() {
        reset();
        let mut session = SessionContext::default();
        session.connection_id = 7_800_003;
        type Rows = fn() -> Vec<Vec<Value>>;
        let dimensions: [(&str, Rows); 6] = [
            ("global_by_event_name", wait_summary_global_rows),
            ("by_thread_by_event_name", wait_summary_by_thread_rows),
            ("by_user_by_event_name", wait_summary_by_user_rows),
            ("by_host_by_event_name", wait_summary_by_host_rows),
            ("by_account_by_event_name", wait_summary_by_account_rows),
            ("by_instance", wait_summary_by_instance_rows),
        ];
        for (index, (target, _)) in dimensions.iter().enumerate() {
            record_statement(
                &session,
                "SELECT 1",
                Duration::from_micros(10),
                QueryLogMetadata {
                    lock_wait_count: 1,
                    lock_wait_ms: 1.0,
                    ..Default::default()
                },
                &Ok(QueryResponse::Ok),
            );
            let before: Vec<_> = dimensions.iter().map(|(_, rows)| rows()).collect();
            truncate(&format!("events_waits_summary_{target}"));
            for (other, (_, rows)) in dimensions.iter().enumerate() {
                if other == index {
                    assert!(rows().is_empty(), "{target}");
                } else {
                    assert_eq!(rows(), before[other], "{target}");
                }
            }
        }
        truncate("events_waits_summary_by_user_by_event_name");
        let accounts = wait_summary_by_account_rows();
        unregister_thread(session.connection_id as u64);
        assert!(wait_summary_by_user_rows().is_empty());
        assert_eq!(wait_summary_by_account_rows(), accounts);
    }

    #[test]
    fn bounded_push_keeps_last_items() {
        let mut queue = VecDeque::new();
        for value in 0..12 {
            push_bounded(&mut queue, value, 10);
        }
        assert_eq!(queue.len(), 10);
        assert_eq!(queue.front(), Some(&2));
        assert_eq!(queue.back(), Some(&11));
    }

    #[test]
    fn aggregate_min_avg_max_tracks_samples() {
        let mut agg = StatementAggregate::new("statement/sql/select");
        let base = StatementEvent {
            thread_id: 1,
            event_id: 1,
            end_event_id: Some(1),
            event_name: "statement/sql/select",
            timer_start: 0,
            timer_end: 10,
            timer_wait: 10,
            lock_time: 3,
            sql_text: Arc::from("SELECT 1"),
            digest: None,
            digest_text: None,
            current_schema: "test".to_string(),
            mysql_errno: None,
            returned_sqlstate: None,
            message_text: None,
            errors: 1,
            warnings: 2,
            rows_affected: 4,
            rows_sent: 5,
            rows_examined: 6,
        };
        agg.record(&base);
        let mut slower = base.clone();
        slower.timer_wait = 30;
        slower.lock_time = 7;
        slower.errors = 0;
        slower.warnings = 8;
        slower.rows_affected = 9;
        slower.rows_sent = 10;
        slower.rows_examined = 11;
        agg.record(&slower);
        assert_eq!(agg.metrics.count_star, 2);
        assert_eq!(agg.metrics.min_timer_wait, 10);
        assert_eq!(avg(agg.metrics.sum_timer_wait, agg.metrics.count_star), 20);
        assert_eq!(agg.metrics.max_timer_wait, 30);
        assert_eq!(
            &statement_aggregate_tail(&agg)[..11],
            &[
                u(2),
                u(40),
                u(10),
                u(20),
                u(30),
                u(10),
                u(1),
                u(10),
                u(13),
                u(15),
                u(17),
            ]
        );
        let mut merged = StatementAggregate::new(base.event_name);
        merged.merge(&agg);
        assert_eq!(
            statement_aggregate_tail(&merged),
            statement_aggregate_tail(&agg)
        );
    }

    #[test]
    fn begin_statement_publishes_in_flight_current_row() {
        reset();
        let mut session = SessionContext::default();
        session.connection_id = 7_700_001;
        begin_statement(&session, "SELECT SLEEP(1)");
        let row = statement_current_rows()
            .into_iter()
            .find(|row| row[0] == Value::UInt(7_700_001))
            .expect("in-flight row");
        assert_eq!(row[2], Value::Null);
        unregister_thread(7_700_001);
    }

    #[test]
    fn sharded_recording_is_lossless_under_concurrency() {
        reset();
        crate::engine::perf_digest::reset();
        let workers = (0..16)
            .map(|worker| {
                std::thread::spawn(move || {
                    let mut session = SessionContext::default();
                    session.connection_id = 8_000_000 + worker;
                    for _ in 0..100 {
                        record_statement(
                            &session,
                            "SELECT 1",
                            Duration::from_micros(10),
                            QueryLogMetadata::default(),
                            &Ok(QueryResponse::Ok),
                        );
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
        let rows = statement_summary_global_rows();
        let row = rows
            .iter()
            .find(|row| row[0] == Value::String("statement/sql/select".to_string()))
            .expect("SELECT summary");
        assert_eq!(row[1], Value::UInt(1_600));
        for worker in 0..16 {
            unregister_thread(8_000_000 + worker);
        }
    }

    #[test]
    fn digest_sql_override_is_restored_after_unwind() {
        let result = std::panic::catch_unwind(|| {
            with_statement_digest_sql_override("SELECT ?".to_string(), || {
                panic!("exercise override guard");
            });
        });
        assert!(result.is_err());
        STATEMENT_DIGEST_SQL_OVERRIDE.with(|slot| assert!(slot.borrow().is_none()));
    }

    #[test]
    fn prepared_sized_long_statement_texts_are_cached() {
        STATEMENT_TEXT_CACHE.with(|cache| cache.borrow_mut().clear());
        let sql = format!("SELECT {} FROM t WHERE id = ?", "wide_column,".repeat(80));
        assert!(sql.len() > 512);
        assert!(sql.len() <= STATEMENT_TEXT_CACHE_MAX_KEY_LEN);

        let first = statement_texts(&sql);
        let second = statement_texts(&sql);
        assert!(Arc::ptr_eq(&first.sql_text, &second.sql_text));
        let (first_digest_text, first_digest) = first.digest.unwrap();
        let (second_digest_text, second_digest) = second.digest.unwrap();
        assert!(Arc::ptr_eq(&first_digest_text, &second_digest_text));
        assert!(Arc::ptr_eq(&first_digest, &second_digest));
    }

    #[test]
    fn prepared_statement_texts_reuse_worker_derivation_and_override_execution_lookup() {
        STATEMENT_TEXT_CACHE.with(|cache| cache.borrow_mut().clear());
        let sql = "SELECT payload FROM t WHERE id = ?";
        let prepared = prepared_statement_texts(sql);
        let prepared_again = prepared_statement_texts(sql);

        assert!(!Arc::ptr_eq(
            &prepared.0.sql_text,
            &prepared_again.0.sql_text
        ));
        assert_eq!(prepared.0.sql_text, prepared_again.0.sql_text);
        let (digest_text, digest) = prepared.0.digest.as_ref().unwrap();
        let (digest_text_again, digest_again) = prepared_again.0.digest.as_ref().unwrap();
        assert!(!Arc::ptr_eq(digest_text, digest_text_again));
        assert!(!Arc::ptr_eq(digest, digest_again));
        assert_eq!(digest_text, digest_text_again);
        assert_eq!(digest, digest_again);

        let texts = with_prepared_statement_texts(&prepared, || statement_texts(sql));
        assert!(Arc::ptr_eq(&texts.sql_text, &prepared.0.sql_text));
        let (digest_text, digest) = texts.digest.unwrap();
        let (prepared_digest_text, prepared_digest) = prepared.0.digest.as_ref().unwrap();
        assert!(Arc::ptr_eq(&digest_text, prepared_digest_text));
        assert!(Arc::ptr_eq(&digest, prepared_digest));
        STATEMENT_TEXT_CACHE.with(|cache| assert_eq!(cache.borrow().len(), 1));
    }

    #[test]
    fn prepared_statement_texts_redact_once_before_caching() {
        STATEMENT_TEXT_CACHE.with(|cache| cache.borrow_mut().clear());
        let raw = "ALTER USER u IDENTIFIED BY 'secret-value'";
        let prepared = prepared_statement_texts(raw);
        assert!(!prepared.0.sql_text.contains("secret-value"));
        assert_ne!(prepared.0.sql_text.as_ref(), raw);
    }

    #[test]
    fn literal_sql_override_reuses_digest_but_preserves_exact_sql_text() {
        STATEMENT_TEXT_CACHE.with(|cache| cache.borrow_mut().clear());
        STATEMENT_SHAPE_DIGEST_CACHE.with(|cache| cache.borrow_mut().clear());
        let first =
            with_statement_digest_sql_override("SELECT ?", || statement_texts("SELECT 123"));
        let second =
            with_statement_digest_sql_override("SELECT ?", || statement_texts("SELECT 456"));
        assert_eq!(first.sql_text.as_ref(), "SELECT 123");
        assert_eq!(second.sql_text.as_ref(), "SELECT 456");
        let (first_text, first_hash) = first.digest.unwrap();
        let (second_text, second_hash) = second.digest.unwrap();
        assert!(Arc::ptr_eq(&first_text, &second_text));
        assert!(Arc::ptr_eq(&first_hash, &second_hash));
        // The shape owns one entry in its own map; the exact-text map is
        // untouched, so literal spellings never accumulate there.
        STATEMENT_SHAPE_DIGEST_CACHE.with(|cache| assert_eq!(cache.borrow().len(), 1));
        STATEMENT_TEXT_CACHE.with(|cache| assert!(cache.borrow().is_empty()));
    }

    #[test]
    fn exact_text_churn_does_not_evict_shape_digests() {
        // Regression guard for the two populations sharing one map: exact
        // texts arrive once per execution and overflow the cap continuously,
        // and the overflow `clear()` used to take the handful of hot shape
        // entries with it — forcing a digest tokenize + SHA-256 rebuild for
        // every live shape on each wrap-around.
        STATEMENT_TEXT_CACHE.with(|cache| cache.borrow_mut().clear());
        STATEMENT_SHAPE_DIGEST_CACHE.with(|cache| cache.borrow_mut().clear());
        let seeded = with_statement_digest_sql_override("SELECT ?", || statement_texts("SELECT 1"));
        let (seeded_text, seeded_hash) = seeded.digest.unwrap();

        for i in 0..(STATEMENT_TEXT_CACHE_MAX * 2) {
            let _ = statement_texts(&format!("SELECT {i} FROM churn"));
        }

        let after = with_statement_digest_sql_override("SELECT ?", || statement_texts("SELECT 2"));
        let (after_text, after_hash) = after.digest.unwrap();
        assert!(Arc::ptr_eq(&seeded_text, &after_text));
        assert!(Arc::ptr_eq(&seeded_hash, &after_hash));
    }
}

#[cfg(test)]
#[path = "perf_events/rollup_tests.rs"]
mod rollup_tests;
