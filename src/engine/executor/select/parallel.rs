//! Parallel-execution worker dispatch for the Volcano-style executor.
//!
//! Phase 1 introduces two operators:
//! - `ParallelSeqScan`: each of `degree` worker threads scans a disjoint
//!   contiguous heap-page range computed from `page_count / degree`,
//!   applies WHERE / runtime filters locally, and returns its slice of
//!   `PlanRow`s.
//! - `Exchange`: the parallel boundary. Spawns the workers via
//!   `std::thread::scope`, collects per-worker output, and concatenates.
//!   `ExchangeOrder::Sorted` performs a k-way merge using `merge_keys`;
//!   `Unordered` is a plain extend.
//!
//! Worker threads share `&Arc<Database>` (`Database` is `Sync`), copy a
//! per-thread `TxnView` (which is `Copy`), and re-establish the session
//! parameter slice via `crate::engine::params::with_params_slice`. The
//! buffer pool is sharded so concurrent reads on disjoint pages do not
//! contend on a single mutex.
//!
//! The fall-through to the existing serial `scan_plan_table` happens
//! when `degree <= 1` or `page_count` is too small for partitioning to
//! be worthwhile.

use super::super::*;
use crate::parser::{Expr, OrderBy};
use crate::planner::{ExchangeOrder, PhysicalPlan, PlanTable, RuntimeFilter};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

use super::eval::EvalQueryContext;
use super::plan::{runtime_filters_admit, stored_row_to_plan_row, ColMeta, PlanField, PlanRow};
use crate::storage::{IndexBound, IndexSchema};

// --- Phase 6: cumulative parallel-execution metrics ----------------------
//
// Process-global counters surfaced via `SHOW ENGINE MYDB STATUS`. They
// give operators visibility into how often each parallel operator is
// firing, how many worker threads were spawned, and whether any worker
// panicked.

/// Total number of `Exchange` boundaries entered.
pub(crate) static EXCHANGE_COUNT: AtomicU64 = AtomicU64::new(0);
/// Total number of `ParallelSeqScan` operators entered.
pub(crate) static PARALLEL_SEQ_SCAN_COUNT: AtomicU64 = AtomicU64::new(0);
/// Total number of parallel sequential scans whose page work was pruned and
/// scheduled from Zone Map candidate ranges.
pub(crate) static PARALLEL_ZONE_MAP_SCAN_COUNT: AtomicU64 = AtomicU64::new(0);
pub(crate) static ZONE_MAP_CANDIDATE_BUILD_NS: AtomicU64 = AtomicU64::new(0);
pub(crate) static ZONE_MAP_SCAN_TASKS_SCHEDULED: AtomicU64 = AtomicU64::new(0);
pub(crate) static ZONE_MAP_SCAN_PAGES_SCHEDULED: AtomicU64 = AtomicU64::new(0);
pub(crate) static ZONE_MAP_SCAN_PAGES_SKIPPED: AtomicU64 = AtomicU64::new(0);
pub(crate) static ZONE_MAP_RUNTIME_FALLBACKS: AtomicU64 = AtomicU64::new(0);
/// Total number of `ParallelIndexFastScan` operators entered.
pub(crate) static PARALLEL_INDEX_FAST_SCAN_COUNT: AtomicU64 = AtomicU64::new(0);
/// Total number of `ParallelIndexRangeScan` operators entered.
pub(crate) static PARALLEL_INDEX_RANGE_SCAN_COUNT: AtomicU64 = AtomicU64::new(0);
/// Total number of `ParallelIndexAggregate` operators entered.
pub(crate) static PARALLEL_INDEX_AGGREGATE_COUNT: AtomicU64 = AtomicU64::new(0);
/// Total number of `ParallelHashAggregate` operators entered.
pub(crate) static PARALLEL_HASH_AGGREGATE_COUNT: AtomicU64 = AtomicU64::new(0);
/// Total number of `ParallelSort` operators entered.
pub(crate) static PARALLEL_SORT_COUNT: AtomicU64 = AtomicU64::new(0);
/// Cumulative number of worker threads spawned across all parallel
/// operators (degree summed across each operator invocation).
pub(crate) static PARALLEL_WORKERS_SPAWNED: AtomicU64 = AtomicU64::new(0);
/// Number of times a worker thread panicked (as observed by the
/// `JoinHandle::join` Err path). Production should never increment this
/// — non-zero is a bug.
pub(crate) static PARALLEL_WORKER_PANIC_COUNT: AtomicU64 = AtomicU64::new(0);

/// Compute disjoint contiguous `[lo, hi)` page-id partitions covering
/// `0..page_count`. Distributes the remainder across the first
/// `page_count % degree` partitions so the largest partition is at
/// most one page wider than the smallest.
pub(super) fn page_partition(page_count: u64, degree: u32) -> Vec<(u64, u64)> {
    let p = degree.max(1) as u64;
    if page_count == 0 {
        return (0..p).map(|_| (0u64, 0u64)).collect();
    }
    let chunk = page_count / p;
    let rem = page_count % p;
    (0..p)
        .map(|i| {
            let lo = i * chunk + i.min(rem);
            let extra = if i < rem { 1 } else { 0 };
            (lo, lo + chunk + extra)
        })
        .collect()
}

/// One independently schedulable physical-heap page range. Keeping the
/// storage id in the task lets a parallel Zone Map scan split ranges inside a
/// single surviving partition instead of being capped by partition count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ScanTask {
    pub(super) storage_id: u64,
    pub(super) page_lo: u64,
    pub(super) page_hi: u64,
}

/// Sink used by decoded-predicate scans. Variable-width text is compared
/// while it is still borrowed from the pinned heap page; one reusable truth
/// slot per compiled leaf supports boolean trees and multiple predicates over
/// the same column without constructing `String`/`Value` rows.
pub(crate) struct DecodedPredicateSink<'a> {
    program: &'a super::plan::DecodedPredicateProgram,
    results: &'a mut [Option<super::plan::DecodedTruth>],
    unsupported: bool,
}

impl<'a> DecodedPredicateSink<'a> {
    #[inline]
    pub(crate) fn new(
        program: &'a super::plan::DecodedPredicateProgram,
        results: &'a mut [Option<super::plan::DecodedTruth>],
    ) -> Self {
        results.fill(None);
        Self {
            program,
            results,
            unsupported: false,
        }
    }

    #[inline]
    pub(crate) fn finish(&self) -> Option<bool> {
        if self.unsupported {
            None
        } else {
            self.program.finish(self.results)
        }
    }

    #[inline]
    fn unsupported(&mut self) {
        self.unsupported = true;
    }
}

impl crate::storage::RowDecodeSink for DecodedPredicateSink<'_> {
    #[inline]
    fn validates_utf8_in_put_string(&self) -> bool {
        true
    }

    fn put_null(&mut self, col: usize) {
        if !self.program.put_null(col, self.results) {
            self.unsupported();
        }
    }

    fn put_string(&mut self, col: usize, bytes: &[u8]) {
        if !self.program.put_string(col, bytes, self.results) {
            self.unsupported();
        }
    }

    fn put_i64(&mut self, col: usize, value: i64) {
        if !self.program.put_numeric(
            col,
            super::super::vec::scan_bridge::PredicateValue::I64(value),
            self.results,
        ) {
            self.unsupported();
        }
    }
    fn put_u64(&mut self, col: usize, value: u64) {
        if !self.program.put_numeric(
            col,
            super::super::vec::scan_bridge::PredicateValue::U64(value),
            self.results,
        ) {
            self.unsupported();
        }
    }
    fn put_f32(&mut self, col: usize, value: f32) {
        if !self.program.put_numeric(
            col,
            super::super::vec::scan_bridge::PredicateValue::F64(f64::from(value)),
            self.results,
        ) {
            self.unsupported();
        }
    }
    fn put_f64(&mut self, col: usize, value: f64) {
        if !self.program.put_numeric(
            col,
            super::super::vec::scan_bridge::PredicateValue::F64(value),
            self.results,
        ) {
            self.unsupported();
        }
    }
    fn put_date(&mut self, col: usize, value: i32) {
        if !self.program.put_numeric(
            col,
            super::super::vec::scan_bridge::PredicateValue::Date(value),
            self.results,
        ) {
            self.unsupported();
        }
    }
    fn put_year(&mut self, col: usize, value: u8) {
        if !self.program.put_numeric(
            col,
            super::super::vec::scan_bridge::PredicateValue::Year(value),
            self.results,
        ) {
            self.unsupported();
        }
    }
    fn put_time(&mut self, col: usize, value: i64) {
        if !self.program.put_numeric(
            col,
            super::super::vec::scan_bridge::PredicateValue::Time(value),
            self.results,
        ) {
            self.unsupported();
        }
    }
    fn put_datetime(&mut self, col: usize, value: i64) {
        if !self.program.put_numeric(
            col,
            super::super::vec::scan_bridge::PredicateValue::DateTime(value),
            self.results,
        ) {
            self.unsupported();
        }
    }
    fn put_timestamp(&mut self, col: usize, value: i64) {
        if !self.program.put_numeric(
            col,
            super::super::vec::scan_bridge::PredicateValue::Timestamp(value),
            self.results,
        ) {
            self.unsupported();
        }
    }
    fn put_decimal_compact(&mut self, col: usize, value: i64, scale: u8) {
        self.put_decimal_wide(col, i128::from(value), scale);
    }
    fn put_decimal_wide(&mut self, col: usize, value: i128, scale: u8) {
        if !self.program.put_numeric(
            col,
            super::super::vec::scan_bridge::PredicateValue::Decimal(value, scale),
            self.results,
        ) {
            self.unsupported();
        }
    }
    reject_decode_lanes!(unsupported; put_bytes, put_json, put_geometry, put_vector, put_value);
}

impl ScanTask {
    pub(super) fn pages(self) -> u64 {
        self.page_hi.saturating_sub(self.page_lo)
    }
}

pub(super) type ParallelScanTask = ScanTask;

/// Resolve the physical heaps currently in scope and return the exact page
/// ranges a scan must visit.  This is the single execution-time source of
/// truth for both SeqScan and ZoneMapScan consumers.  Zone Map metadata is
/// deliberately consulted here rather than cached in the physical plan so
/// prepared parameters and concurrently-published generations are honoured.
pub(super) fn scan_tasks(
    db: &Arc<Database>,
    table: &PlanTable,
    filter: Option<&Expr>,
    use_zone_map: bool,
) -> Result<Vec<ScanTask>, QueryError> {
    let candidate_started = use_zone_map.then(std::time::Instant::now);
    let mut storage_ids = if table.schema.is_partitioned() {
        let all_storage_ids = table.schema.storage_ids();
        let planned = table.resolved_partition_storage_ids(filter);
        let current = crate::engine::executor::partition_router::current_pruned_partitions();
        match (current, planned) {
            (Some(current), Some(planned)) => current
                .into_iter()
                .filter(|storage_id| planned.contains(storage_id))
                .collect(),
            (Some(current), None) => current
                .into_iter()
                .filter(|storage_id| all_storage_ids.contains(storage_id))
                .collect(),
            (None, Some(planned)) => planned,
            (None, None) => all_storage_ids,
        }
    } else {
        vec![table.schema.id]
    };
    storage_ids.sort_unstable();
    storage_ids.dedup();

    let mut tasks = Vec::new();
    let mut fallback_tasks = 0u64;
    let mut fallback_pages = 0u64;
    for storage_id in storage_ids {
        let page_count = db.heap_page_count_for_storage(storage_id)?;
        let candidates = use_zone_map.then(|| filter).flatten().and_then(|expr| {
            db.zone_map_candidate_ranges_for_storage_for_plan(&table.schema, storage_id, expr)
        });
        let ranges = match candidates {
            Some(ranges)
                if ranges
                    .iter()
                    .all(|&(page_lo, page_hi)| page_lo <= page_hi && page_hi <= page_count) =>
            {
                ranges
            }
            // A derived sidecar is never allowed to turn malformed or stale
            // page coordinates into a false negative. Fall back independently
            // for this storage heap while retaining pruning on healthy heaps.
            Some(_) | None => {
                if use_zone_map {
                    ZONE_MAP_RUNTIME_FALLBACKS.fetch_add(1, AtomicOrdering::Relaxed);
                    fallback_tasks = fallback_tasks.saturating_add(u64::from(page_count > 0));
                    fallback_pages = fallback_pages.saturating_add(page_count);
                }
                crate::storage::ZoneMapRanges::full(page_count)
            }
        };
        for (page_lo, page_hi) in ranges {
            if page_lo < page_hi {
                tasks.push(ScanTask {
                    storage_id,
                    page_lo,
                    page_hi,
                });
            }
        }
    }
    tasks.sort_unstable_by_key(|task| (task.storage_id, task.page_lo, task.page_hi));
    if let Some(started) = candidate_started.filter(|_| fallback_tasks > 0) {
        // Successful candidate calls publish their exact ranges/pages through
        // the storage counters.  Only publish work here for heaps that fell
        // back, otherwise EXPLAIN ANALYZE double-counts the single immutable
        // artifact as both a storage candidate and a scheduler task set.
        ZONE_MAP_CANDIDATE_BUILD_NS.fetch_add(
            started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            AtomicOrdering::Relaxed,
        );
        ZONE_MAP_SCAN_TASKS_SCHEDULED.fetch_add(fallback_tasks, AtomicOrdering::Relaxed);
        ZONE_MAP_SCAN_PAGES_SCHEDULED.fetch_add(fallback_pages, AtomicOrdering::Relaxed);
    }
    Ok(tasks)
}

pub(super) fn parallel_zone_map_morsels(
    db: &Arc<Database>,
    table: &PlanTable,
    filter: Option<&Expr>,
    degree: u32,
) -> Result<(Vec<ScanTask>, usize), QueryError> {
    scan_morsels(db, table, filter, true, degree)
}

pub(super) fn scan_morsels(
    db: &Arc<Database>,
    table: &PlanTable,
    filter: Option<&Expr>,
    use_zone_map: bool,
    degree: u32,
) -> Result<(Vec<ScanTask>, usize), QueryError> {
    let ranges = scan_tasks(db, table, filter, use_zone_map)?;

    let surviving_pages = ranges.iter().map(|task| task.pages()).sum::<u64>();
    if surviving_pages == 0 {
        return Ok((Vec::new(), 0));
    }
    let workers = effective_degree_for_pages(degree, surviving_pages) as usize;
    // Keep enough morsels in flight to absorb skew from MVCC chains, cold
    // pages and expensive predicates, while bounding an atomic claim to at
    // a bounded sequential read. Small metadata zones should not force every
    // scan callback to rebuild decode/predicate state for a 1 MiB fragment;
    // use the same 4 MiB upper granularity as native parallel heap scanning.
    let target_morsels = if use_zone_map { 8 } else { 1 };
    let target_pages = surviving_pages
        .div_ceil((workers as u64).saturating_mul(target_morsels))
        .max(1);
    let chunk_pages = if use_zone_map {
        target_pages
            .min((table.schema.zone_pages() as u64).max(256))
            .max(1)
    } else {
        target_pages
    };
    let mut tasks = Vec::new();
    for range in ranges {
        let mut page_lo = range.page_lo;
        while page_lo < range.page_hi {
            let page_hi = page_lo.saturating_add(chunk_pages).min(range.page_hi);
            tasks.push(ParallelScanTask {
                storage_id: range.storage_id,
                page_lo,
                page_hi,
            });
            page_lo = page_hi;
        }
    }

    // Physical order maximises prefetch/locality for the common case. Dynamic
    // claim order still prevents an expensive early morsel from pinning one
    // worker while peers go idle.
    tasks.sort_unstable_by_key(|task| (task.storage_id, task.page_lo));
    Ok((tasks, workers))
}

pub(super) fn record_parallel_task_partition_visits(
    table: &PlanTable,
    assignments: &[Vec<ParallelScanTask>],
) {
    if !table.schema.is_partitioned() {
        return;
    }
    let mut storage_ids = std::collections::BTreeSet::new();
    for task in assignments.iter().flatten() {
        storage_ids.insert(task.storage_id);
    }
    for _ in storage_ids {
        crate::engine::executor::partition_router::record_partition_visit();
    }
}

/// Streaming variant of `worker_full_row`: surfaces every accepted
/// row through `on_row(row) -> Result<bool>`. The closure returns
/// `false` to ask the worker to stop early (consumer closed). Used
/// by [`stream::parallel_exchange_unordered_stream`] so worker rows
/// reach the consumer as batches form rather than after the whole
/// page slice materialises.
pub(super) fn worker_full_row_streaming(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: Option<&Expr>,
    runtime_filters: &[RuntimeFilter],
    storage_id: u64,
    page_lo: u64,
    page_hi: u64,
    mut on_row: impl FnMut(PlanRow) -> Result<bool, QueryError>,
) -> Result<(), QueryError> {
    let mut eval_ctx = EvalQueryContext::default();
    for stored in db.scan_rows_storage_range(&table.schema, storage_id, txn, page_lo, page_hi)? {
        let row = stored_row_to_plan_row(table, stored.values)?;
        if !runtime_filters_admit(runtime_filters, &row)? {
            continue;
        }
        let keep = match filter {
            Some(expr) => super::eval::eval_plan_bool_with_db(db, txn, expr, &row, &mut eval_ctx)?,
            None => true,
        };
        if !keep {
            continue;
        }
        if !on_row(row)? {
            return Ok(());
        }
    }
    Ok(())
}

/// Streaming variant of `worker_projected`: see
/// [`worker_full_row_streaming`] for the contract.
pub(super) fn worker_projected_streaming(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: Option<&Expr>,
    runtime_filters: &[RuntimeFilter],
    projection: &[usize],
    storage_id: u64,
    page_lo: u64,
    page_hi: u64,
    mut on_row: impl FnMut(PlanRow) -> Result<bool, QueryError>,
) -> Result<(), QueryError> {
    // Per-column metadata is interned in the global pool — once we have
    // these `&'static` pointers the visitor avoids re-cloning them per
    // row.
    let column_metas: Vec<super::plan::ColMetaRef> = projection
        .iter()
        .map(|&i| {
            crate::engine::executor::select::plan::intern_col_meta(ColMeta::from_table(table, i))
        })
        .collect();
    let mut eval_ctx = EvalQueryContext::default();
    let filter_has_subquery = filter
        .map(super::simple::expr_has_subquery)
        .unwrap_or(false);
    let compiled_filter = if filter_has_subquery {
        None
    } else {
        filter.and_then(|expr| {
            let projected_schema = super::plan::projected_schema_view(&table.schema, projection);
            super::plan::compile_scan_predicate(expr, &projected_schema)
        })
    };
    // Pre-resolve runtime bloom-filter key expressions to column indices
    // inside the projected `&[Value]` slice. When every bloom key maps to
    // a plain column reference covered by the projection (the common case
    // for hash-join driven bloom probes — TPC-H Q21's
    // `l1.l_suppkey = s_suppkey` flavour), we can reject ~96 % of rows
    // *before* the per-row `PlanRow` build, skipping the
    // `Vec<PlanField>` allocation and the n column-clones for rejected
    // rows. Falls back to the post-PlanRow path when any key isn't a
    // bare projected column (rare; covers arithmetic-key bloom filters).
    let bloom_specs: Vec<Option<super::plan::CompiledRuntimeFilterProbe>> = runtime_filters
        .iter()
        .map(|rf| {
            super::plan::compile_runtime_filter_probe_projected(rf, &table.schema, projection)
        })
        .collect();
    let bloom_fast_path = !runtime_filters.is_empty() && bloom_specs.iter().all(|s| s.is_some());
    let shared_lookup = super::plan::new_join_lookup();
    // Hoist out the four shape decisions that don't change across rows.
    // The visitor closure pays one `bool` test per row instead of a
    // chain of `is_none()` / `is_empty()` / `is_some_and()` checks.
    let has_runtime_filters = !runtime_filters.is_empty();
    let has_uncompiled_filter =
        filter.is_some() && compiled_filter.is_none() && !filter_has_subquery;
    let needs_post_alloc_work =
        (has_runtime_filters && !bloom_fast_path) || has_uncompiled_filter || filter_has_subquery;
    let n_cols = projection.len();
    // Avoid the early-stop callback panicking the visitor closure;
    // surface signals via the `RefCell` slots instead.
    let stop_signal = std::cell::Cell::new(false);
    let on_row_err: std::cell::RefCell<Option<QueryError>> = std::cell::RefCell::new(None);
    let runtime_admit_err: std::cell::RefCell<Option<QueryError>> = std::cell::RefCell::new(None);
    // The visitor sees one row at a time; legacy filter walking
    // matches the materialised projected_worker pattern.
    let mut process_values = |values: &[Value]| -> Result<(), QueryError> {
        if stop_signal.get() {
            return Ok(());
        }
        if let Some(cp) = compiled_filter.as_ref() {
            if !cp.eval(values)? {
                return Ok(());
            }
        }
        // Bloom probe BEFORE PlanRow construction. For the TPC-H Q21
        // shape the runtime bloom is the supplier-hash filter
        // (saudi-arabia suppliers, ~4 % selectivity), so this
        // short-circuits 96 % of rows out of the per-row Vec alloc +
        // n column clones the legacy path was paying. Mirrors the
        // serial `scan_plan_table_projected` fast path.
        if bloom_fast_path
            && !super::plan::runtime_filters_admit_values_indexed(&bloom_specs, values)
        {
            return Ok(());
        }
        // Build `fields` directly rather than `templates.clone()`
        // followed by a per-field `Value` overwrite. The previous
        // shape wrote `Value::Null` into every cell of the cloned
        // vector and then replaced each one with `values[i].clone()`;
        // building in one pass skips the redundant null writes.
        let mut fields: Vec<PlanField> = Vec::with_capacity(n_cols);
        for i in 0..n_cols {
            fields.push(PlanField {
                meta: column_metas[i].clone(),
                value: values[i].clone(),
            });
        }
        let row = PlanRow {
            fields,
            group: None,
            agg_values: None,
            schema: shared_lookup.clone().into(),
        };
        if needs_post_alloc_work {
            if has_runtime_filters && !bloom_fast_path {
                match runtime_filters_admit(runtime_filters, &row) {
                    Ok(true) => {}
                    Ok(false) => return Ok(()),
                    Err(e) => {
                        *runtime_admit_err.borrow_mut() = Some(e);
                        return Ok(());
                    }
                }
            }
            if filter_has_subquery {
                // Defer to a post-scan pass — same as the materialised
                // worker, which buffers and replays. In streaming
                // mode we don't have that luxury; ignore subquery
                // filters here and let the caller fall back to the
                // materialised path. Stream caller guards against
                // calling us in that case.
                return Ok(());
            }
            if has_uncompiled_filter {
                if let Some(expr) = filter {
                    match super::eval::eval_plan_bool_with_db(db, txn, expr, &row, &mut eval_ctx) {
                        Ok(false) => return Ok(()),
                        Ok(true) => {}
                        Err(e) => {
                            *on_row_err.borrow_mut() = Some(e);
                            stop_signal.set(true);
                            return Ok(());
                        }
                    }
                }
            }
        }
        match on_row(row) {
            Ok(true) => Ok(()),
            Ok(false) => {
                stop_signal.set(true);
                Ok(())
            }
            Err(e) => {
                *on_row_err.borrow_mut() = Some(e);
                stop_signal.set(true);
                Ok(())
            }
        }
    };
    let outside_latch = filter.is_some()
        && !compiled_filter
            .as_ref()
            .is_some_and(super::plan::CompiledScanPredicate::page_latch_safe)
        || runtime_filters.iter().any(|rf| {
            super::plan::compile_runtime_filter_probe_projected(rf, &table.schema, projection)
                .is_none()
        });
    super::plan::scan_projected_range(
        db,
        &table.schema,
        storage_id,
        txn,
        projection,
        page_lo,
        page_hi,
        outside_latch,
        &mut process_values,
    )?;
    if let Some(err) = on_row_err.into_inner() {
        return Err(err);
    }
    if let Some(err) = runtime_admit_err.into_inner() {
        return Err(err);
    }
    Ok(())
}

/// Late-materialising scan for the parallel DISTINCT Top-K operator.
///
/// A simple single-column ORDER BY can reject nearly every input row from the
/// bounded heap using only its decoded scalar key. Constructing a PlanRow
/// before that check allocates and clones every projected column even though
/// at most K rows per worker survive. This path decodes into fixed-width
/// predicate lanes, evaluates a typed filter when present, and materialises a
/// PlanRow only after `would_accept` succeeds. Unsupported layouts or
/// predicate values fall back per row to the Value oracle.
#[allow(clippy::too_many_arguments)]
fn worker_projected_distinct_topk_typed(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: Option<&Expr>,
    projection: &[usize],
    order_slot: usize,
    storage_id: u64,
    page_lo: u64,
    page_hi: u64,
    collector: &mut WorkerDistinctTopK<'_>,
) -> Result<bool, QueryError> {
    if filter.is_some_and(super::simple::expr_has_subquery) {
        return Ok(false);
    }
    let projected_schema = super::plan::projected_schema_view(&table.schema, projection);
    let compiled =
        filter.and_then(|expr| super::plan::CompiledPredicate::compile(expr, &projected_schema));
    if filter.is_some()
        && !compiled
            .as_ref()
            .is_some_and(super::plan::CompiledPredicate::supports_typed_eval)
    {
        return Ok(false);
    }

    let column_metas: Vec<super::plan::ColMetaRef> = projection
        .iter()
        .map(|&index| {
            crate::engine::executor::select::plan::intern_col_meta(ColMeta::from_table(
                table, index,
            ))
        })
        .collect();
    let shared_lookup = super::plan::new_join_lookup();
    let mut typed_values =
        vec![crate::engine::executor::vec::scan_bridge::PredicateValue::Null; projection.len()];
    let mut fallback_values = Vec::with_capacity(projection.len());
    let mut on_payload = |hit: crate::storage::PayloadStreamHit<'_>| -> Result<(), QueryError> {
        match hit {
            crate::storage::PayloadStreamHit::Payload(payload) => {
                let fast = crate::storage::decode_row_into_sink_fast(
                    payload.payload,
                    payload.codec,
                    payload.layout_id,
                    payload.plan,
                    &mut crate::engine::executor::vec::scan_bridge::PredicateValueSink::new(
                        &mut typed_values,
                    ),
                    payload.has_external,
                )?;
                let typed_keep = if fast {
                    match &compiled {
                        Some(compiled) => compiled.eval_typed(&typed_values)?,
                        None => Some(true),
                    }
                } else {
                    None
                };
                if fast {
                    if let Some(keep) = typed_keep {
                        collector.typed_predicate_rows =
                            collector.typed_predicate_rows.saturating_add(1);
                        if !keep {
                            return Ok(());
                        }
                        let key = typed_values[order_slot].to_value();
                        if !collector.would_accept_single_key(&key) {
                            return Ok(());
                        }
                        collector.late_materialized_rows =
                            collector.late_materialized_rows.saturating_add(1);
                        let fields = column_metas
                            .iter()
                            .zip(&typed_values)
                            .map(|(meta, value)| PlanField {
                                meta: meta.clone(),
                                value: value.to_value(),
                            })
                            .collect();
                        return collector.push_prepared(PlanRow {
                            fields,
                            group: None,
                            agg_values: None,
                            schema: shared_lookup.clone().into(),
                        });
                    }

                    // The fast decoder already populated the complete
                    // projection.  A semantic typed-lane miss must consult
                    // the Value oracle, but decoding the same payload again
                    // only duplicates row-codec work.  Losslessly convert the
                    // scratch lanes instead; uncommon values were retained as
                    // PredicateValue::Value by the sink.
                    collector.predicate_fallback_rows =
                        collector.predicate_fallback_rows.saturating_add(1);
                    fallback_values.clear();
                    fallback_values.extend(typed_values.iter().map(|value| value.to_value()));
                } else {
                    match crate::storage::fast_decode_fallback_reason(
                        payload.payload,
                        payload.layout_id,
                        payload.plan,
                    ) {
                        crate::storage::FastDecodeFallbackReason::MissingLayout => {
                            collector.missing_layout_fallback_rows =
                                collector.missing_layout_fallback_rows.saturating_add(1)
                        }
                        crate::storage::FastDecodeFallbackReason::ProjectionNotCovered => {
                            collector.projection_fallback_rows =
                                collector.projection_fallback_rows.saturating_add(1)
                        }
                        crate::storage::FastDecodeFallbackReason::StoredCountMismatch => {
                            collector.count_fallback_rows =
                                collector.count_fallback_rows.saturating_add(1)
                        }
                        crate::storage::FastDecodeFallbackReason::Unknown => {
                            collector.unknown_layout_fallback_rows =
                                collector.unknown_layout_fallback_rows.saturating_add(1)
                        }
                    }
                    fallback_values.clear();
                    crate::storage::decode_row_schema_columns_into_with_plan_ext_for_layout(
                        payload.payload,
                        payload.codec,
                        payload.layout_id,
                        payload.plan,
                        &mut fallback_values,
                        payload.has_external,
                    )?;
                }
            }
            crate::storage::PayloadStreamHit::Values(values) => {
                fallback_values.clear();
                fallback_values.extend_from_slice(values);
            }
        }
        if let Some(compiled) = &compiled {
            if !compiled.eval(&fallback_values)? {
                return Ok(());
            }
        }
        if !collector.would_accept_single_key(&fallback_values[order_slot]) {
            return Ok(());
        }
        collector.late_materialized_rows = collector.late_materialized_rows.saturating_add(1);
        let fields = column_metas
            .iter()
            .zip(&fallback_values)
            .map(|(meta, value)| PlanField {
                meta: meta.clone(),
                value: value.clone(),
            })
            .collect();
        collector.push_prepared(PlanRow {
            fields,
            group: None,
            agg_values: None,
            schema: shared_lookup.clone().into(),
        })
    };
    db.scan_rows_payloads_storage_streaming_range(
        &table.schema,
        storage_id,
        txn,
        projection,
        page_lo,
        page_hi,
        &mut crate::storage::DirectScanScratch::default(),
        &mut on_payload,
    )?;
    Ok(true)
}

/// Execute one worker's slice of a parallel sequential scan: full-row
/// path. Mirrors `super::plan::scan_plan_table` but reads only the
/// page range `[page_lo, page_hi)` using the `with_heap_read`-backed
/// `Database::scan_rows_range`.
fn worker_full_row_storage(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: Option<&Expr>,
    runtime_filters: &[RuntimeFilter],
    storage_id: u64,
    page_lo: u64,
    page_hi: u64,
) -> Result<Vec<PlanRow>, QueryError> {
    let mut eval_ctx = EvalQueryContext::default();
    let mut out = Vec::new();
    for stored in db.scan_rows_storage_range(&table.schema, storage_id, txn, page_lo, page_hi)? {
        let row = stored_row_to_plan_row(table, stored.values)?;
        if !runtime_filters_admit(runtime_filters, &row)? {
            continue;
        }
        let keep = match filter {
            Some(expr) => super::eval::eval_plan_bool_with_db(db, txn, expr, &row, &mut eval_ctx)?,
            None => true,
        };
        if keep {
            out.push(row);
        }
    }
    Ok(out)
}

fn worker_full_row_limited(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: Option<&Expr>,
    runtime_filters: &[RuntimeFilter],
    page_lo: u64,
    page_hi: u64,
    accepted: &AtomicUsize,
    max_rows: usize,
) -> Result<Vec<PlanRow>, QueryError> {
    let mut eval_ctx = EvalQueryContext::default();
    let mut out = Vec::new();
    let qualified = db.storage_key_for_table(&table.schema);
    for stored in db.scan_rows_range(&qualified, txn, page_lo, page_hi)? {
        if accepted.load(AtomicOrdering::Relaxed) >= max_rows {
            break;
        }
        let row = stored_row_to_plan_row(table, stored.values)?;
        if !runtime_filters_admit(runtime_filters, &row)? {
            continue;
        }
        let keep = match filter {
            Some(expr) => super::eval::eval_plan_bool_with_db(db, txn, expr, &row, &mut eval_ctx)?,
            None => true,
        };
        if keep {
            let reserved = accepted
                .fetch_update(
                    AtomicOrdering::Relaxed,
                    AtomicOrdering::Relaxed,
                    |current| (current < max_rows).then_some(current + 1),
                )
                .is_ok();
            if !reserved {
                break;
            }
            out.push(row);
        }
    }
    Ok(out)
}

/// Execute one worker's slice with column pruning. Mirrors
/// `super::plan::scan_plan_table_projected` but limited to a single
/// page range. Subquery-bearing filters are deferred to a post-scan
/// pass exactly as the serial path does, to avoid re-entering the
/// heap lock from inside the visitor callback.
fn worker_projected_storage(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: Option<&Expr>,
    runtime_filters: &[RuntimeFilter],
    projection: &[usize],
    storage_id: u64,
    page_lo: u64,
    page_hi: u64,
) -> Result<Vec<PlanRow>, QueryError> {
    // Twin of the optimisation in `scan_plan_table_projected`: cache the
    // per-projection `&'static ColMeta` references and build each
    // emitted `PlanRow.fields` straight from `(meta, value)` — avoids
    // the two-step "clone a template full of Value::Null then overwrite
    // every slot" pattern that copied N PlanFields per row for no gain.
    let metas: Vec<_> = projection
        .iter()
        .map(|&i| {
            crate::engine::executor::select::plan::intern_col_meta(ColMeta::from_table(table, i))
        })
        .collect();
    let mut out: Vec<PlanRow> = Vec::new();
    let mut eval_ctx = EvalQueryContext::default();
    let filter_has_subquery = filter
        .map(super::simple::expr_has_subquery)
        .unwrap_or(false);
    // Compile the WHERE filter once per worker so per-row eval becomes a
    // `compare_values` against the projected slice — no PlanRow build,
    // no per-column name lookup, no per-row `String` clones (the legacy
    // walker clones for every comparison). Subquery filters and shapes
    // the compiler does not cover fall back to the legacy AST walker.
    let compiled_filter = if filter_has_subquery {
        None
    } else {
        filter.and_then(|expr| {
            let projected_schema = super::plan::projected_schema_view(&table.schema, projection);
            super::plan::compile_scan_predicate(expr, &projected_schema)
        })
    };
    let runtime_admit_err: std::cell::RefCell<Option<QueryError>> = std::cell::RefCell::new(None);
    // O3 — every row this scan emits has the same schema, so build the
    // field-name lookup slot once and share it across all rows.
    let shared_lookup = super::plan::new_join_lookup();
    let outside_latch = filter.is_some()
        && !compiled_filter
            .as_ref()
            .is_some_and(super::plan::CompiledScanPredicate::page_latch_safe)
        || runtime_filters.iter().any(|rf| {
            super::plan::compile_runtime_filter_probe_projected(rf, &table.schema, projection)
                .is_none()
        });
    super::plan::scan_projected_range(
        db,
        &table.schema,
        storage_id,
        txn,
        projection,
        page_lo,
        page_hi,
        outside_latch,
        |values| {
            // Fast path: compiled WHERE eval on the projected slice
            // BEFORE materialising the PlanRow — most rejected rows
            // never become PlanRows.
            if let Some(cp) = compiled_filter.as_ref() {
                if !cp.eval(values)? {
                    return Ok(());
                }
            }
            let fields: Vec<PlanField> = metas
                .iter()
                .zip(values.iter())
                .map(|(meta, value)| PlanField {
                    meta: meta.clone(),
                    value: value.clone(),
                })
                .collect();
            let row = PlanRow {
                fields,
                group: None,
                agg_values: None,
                schema: shared_lookup.clone().into(),
            };
            match runtime_filters_admit(runtime_filters, &row) {
                Ok(true) => {}
                Ok(false) => return Ok(()),
                Err(e) => {
                    *runtime_admit_err.borrow_mut() = Some(e);
                    return Ok(());
                }
            }
            if filter_has_subquery {
                out.push(row);
                return Ok(());
            }
            // Legacy fallback when the compiler could not handle this
            // filter shape (compiled_filter == None) and a non-subquery
            // filter is still present.
            if compiled_filter.is_none() {
                if let Some(expr) = filter {
                    if !super::eval::eval_plan_bool_with_db(db, txn, expr, &row, &mut eval_ctx)? {
                        return Ok(());
                    }
                }
            }
            out.push(row);
            Ok(())
        },
    )?;
    if let Some(err) = runtime_admit_err.into_inner() {
        return Err(err);
    }
    if filter_has_subquery {
        if let Some(expr) = filter {
            let mut filtered = Vec::with_capacity(out.len());
            for row in out {
                if super::eval::eval_plan_bool_with_db(db, txn, expr, &row, &mut eval_ctx)? {
                    filtered.push(row);
                }
            }
            out = filtered;
        }
    }
    Ok(out)
}

/// Drive a `ParallelSeqScan`. Spawns `degree` workers in
/// `std::thread::scope`, each scanning its assigned `[lo, hi)` page
/// range. Falls back to a single in-thread serial scan when the heap
/// is too small to benefit from partitioning.
pub(super) fn execute_parallel_seq_scan(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: Option<&Expr>,
    runtime_filters: &[RuntimeFilter],
    projected_columns: Option<&[usize]>,
    degree: u32,
    use_zone_map: bool,
) -> Result<Vec<PlanRow>, QueryError> {
    if use_zone_map {
        return execute_parallel_zone_map_scan(
            db,
            txn,
            table,
            filter,
            runtime_filters,
            projected_columns,
            degree,
        );
    }
    if table.schema.is_partitioned() {
        return execute_parallel_partitioned_seq_scan(
            db,
            txn,
            table,
            filter,
            runtime_filters,
            projected_columns,
            degree,
            use_zone_map,
        );
    }
    let qualified = db.storage_key_for_table(&table.schema);
    let page_count = db.heap_page_count_by_name(&qualified)?;
    let effective_degree = effective_degree_for_pages(degree, page_count);
    if effective_degree <= 1 {
        // Degenerate: one worker, run inline without spawning a thread.
        return run_one_worker(
            db,
            txn,
            table,
            filter,
            runtime_filters,
            projected_columns,
            0,
            page_count,
        );
    }

    let parts = page_partition(page_count, effective_degree);
    PARALLEL_SEQ_SCAN_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
    PARALLEL_WORKERS_SPAWNED.fetch_add(effective_degree as u64, AtomicOrdering::Relaxed);
    let execution_context = super::execution_context::ExecutionContext::capture(txn);

    // Worker threads start with an empty thread-local CTE registry; snapshot
    // the parent's lexical CTE scope here so a WHERE/JOIN subquery referencing
    // a CTE can still resolve it when evaluated inside a worker (else 1146).

    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(effective_degree as usize);
        for (lo, hi) in parts {
            let table_ref = table;
            let filter_ref = filter;
            let runtime_ref = runtime_filters;
            let projected_ref = projected_columns;
            let db_ref = db;
            let worker_context = execution_context.clone();

            handles.push(s.spawn(move || -> Result<Vec<PlanRow>, QueryError> {
                let worker = || {
                    worker_context.run(db_ref, || {
                        run_one_worker(
                            db_ref,
                            txn,
                            table_ref,
                            filter_ref,
                            runtime_ref,
                            projected_ref,
                            lo,
                            hi,
                        )
                    })
                };
                worker()
            }));
        }
        collect_scan_workers(handles, "parallel scan worker panicked")
    })
}

/// Materialising parallel Zone Map operator. Candidate ranges are generated
/// once in the coordinator, split by surviving page count, and then scanned
/// by workers without re-reading metadata. This is deliberately separate from
/// the ordinary contiguous `ParallelSeqScan` dispatcher: a worker may own
/// several disjoint ranges and ranges may belong to different partition
/// heaps.
fn execute_parallel_zone_map_scan(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: Option<&Expr>,
    runtime_filters: &[RuntimeFilter],
    projected_columns: Option<&[usize]>,
    degree: u32,
) -> Result<Vec<PlanRow>, QueryError> {
    let (tasks, requested_workers) = parallel_zone_map_morsels(db, table, filter, degree)?;
    if tasks.is_empty() {
        return Ok(Vec::new());
    }
    let all_tasks = vec![tasks.clone()];
    record_parallel_task_partition_visits(table, &all_tasks);
    if requested_workers <= 1 || tasks.len() == 1 {
        let mut out = Vec::new();
        for task in &tasks {
            out.extend(run_one_worker_storage(
                db,
                txn,
                table,
                filter,
                runtime_filters,
                projected_columns,
                task.storage_id,
                task.page_lo,
                task.page_hi,
            )?);
        }
        return Ok(out);
    }

    let worker_count = requested_workers.min(tasks.len());
    PARALLEL_SEQ_SCAN_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
    PARALLEL_ZONE_MAP_SCAN_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
    PARALLEL_WORKERS_SPAWNED.fetch_add(worker_count as u64, AtomicOrdering::Relaxed);
    let execution_context = super::execution_context::ExecutionContext::capture(txn);

    let tasks = Arc::new(tasks);
    let next_task = Arc::new(AtomicUsize::new(0));
    let mut results: Vec<Result<Vec<PlanRow>, QueryError>> =
        (0..worker_count).map(|_| Ok(Vec::new())).collect();
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let worker_context = execution_context.clone();

            let next_task = Arc::clone(&next_task);
            let tasks = Arc::clone(&tasks);
            handles.push(scope.spawn(move || -> Result<Vec<PlanRow>, QueryError> {
                let worker = || {
                    worker_context.run(db, || {
                        let mut out = Vec::new();
                        loop {
                            let task_index = next_task.fetch_add(1, AtomicOrdering::Relaxed);
                            let Some(task) = tasks.get(task_index).copied() else {
                                break;
                            };
                            out.extend(run_one_worker_storage(
                                db,
                                txn,
                                table,
                                filter,
                                runtime_filters,
                                projected_columns,
                                task.storage_id,
                                task.page_lo,
                                task.page_hi,
                            )?);
                        }
                        Ok(out)
                    })
                };
                worker()
            }));
        }
        for (index, handle) in handles.into_iter().enumerate() {
            results[index] = match handle.join() {
                Ok(result) => result,
                Err(_) => {
                    PARALLEL_WORKER_PANIC_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
                    Err(error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "parallel zone map scan worker panicked",
                    ))
                }
            };
        }
    });

    let mut out = Vec::new();
    for result in results {
        out.extend(result?);
    }
    Ok(out)
}

fn execute_parallel_partitioned_seq_scan(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: Option<&Expr>,
    runtime_filters: &[RuntimeFilter],
    projected_columns: Option<&[usize]>,
    degree: u32,
    use_zone_map: bool,
) -> Result<Vec<PlanRow>, QueryError> {
    let pruned = table.resolved_partition_storage_ids(filter);
    let storage_ids = pruned.unwrap_or_else(|| table.schema.storage_ids());
    if storage_ids.is_empty() {
        return Ok(Vec::new());
    }
    let effective_degree = std::cmp::min(degree.max(1) as usize, storage_ids.len());
    if effective_degree <= 1 {
        return crate::engine::executor::partition_router::with_pruned_partitions(
            storage_ids,
            || {
                super::plan::scan_plan_table(
                    db,
                    txn,
                    table,
                    &filter.cloned(),
                    runtime_filters,
                    projected_columns,
                    use_zone_map,
                )
            },
        );
    }

    PARALLEL_SEQ_SCAN_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
    PARALLEL_WORKERS_SPAWNED.fetch_add(effective_degree as u64, AtomicOrdering::Relaxed);

    let mut groups = vec![Vec::new(); effective_degree];
    for (idx, sid) in storage_ids.into_iter().enumerate() {
        groups[idx % effective_degree].push(sid);
    }

    let execution_context = super::execution_context::ExecutionContext::capture(txn);

    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(effective_degree);
        for group in groups {
            let table_ref = table;
            let filter_ref = filter;
            let runtime_ref = runtime_filters;
            let db_ref = db;
            let worker_context = execution_context.clone();

            handles.push(s.spawn(move || -> Result<Vec<PlanRow>, QueryError> {
                let worker = || {
                    worker_context.run(db_ref, || {
                        scan_partition_group(
                            db_ref,
                            txn,
                            table_ref,
                            filter_ref,
                            runtime_ref,
                            projected_columns,
                            use_zone_map,
                            group,
                        )
                    })
                };
                worker()
            }));
        }
        collect_scan_workers(handles, "parallel partition scan worker panicked")
    })
}

fn scan_partition_group(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: Option<&Expr>,
    runtime_filters: &[RuntimeFilter],
    projected_columns: Option<&[usize]>,
    use_zone_map: bool,
    storage_ids: Vec<u64>,
) -> Result<Vec<PlanRow>, QueryError> {
    crate::engine::executor::partition_router::with_pruned_partitions(storage_ids, || {
        super::plan::scan_plan_table(
            db,
            txn,
            table,
            &filter.cloned(),
            runtime_filters,
            projected_columns,
            use_zone_map,
        )
    })
}

pub(super) fn execute_parallel_seq_scan_limited(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: Option<&Expr>,
    runtime_filters: &[RuntimeFilter],
    projected_columns: Option<&[usize]>,
    degree: u32,
    max_rows: usize,
    use_zone_map: bool,
) -> Result<Vec<PlanRow>, QueryError> {
    if max_rows == 0 {
        return Ok(Vec::new());
    }
    if use_zone_map {
        let mut rows = execute_parallel_zone_map_scan(
            db,
            txn,
            table,
            filter,
            runtime_filters,
            projected_columns,
            degree,
        )?;
        rows.truncate(max_rows);
        return Ok(rows);
    }
    if projected_columns.is_some() {
        let mut rows = execute_parallel_seq_scan(
            db,
            txn,
            table,
            filter,
            runtime_filters,
            projected_columns,
            degree,
            use_zone_map,
        )?;
        rows.truncate(max_rows);
        return Ok(rows);
    }

    let qualified = db.storage_key_for_table(&table.schema);
    let page_count = db.heap_page_count_by_name(&qualified)?;
    let effective_degree = effective_degree_for_pages(degree, page_count);
    if effective_degree <= 1 {
        let accepted = AtomicUsize::new(0);
        return worker_full_row_limited(
            db,
            txn,
            table,
            filter,
            runtime_filters,
            0,
            page_count,
            &accepted,
            max_rows,
        );
    }

    let parts = page_partition(page_count, effective_degree);
    PARALLEL_SEQ_SCAN_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
    PARALLEL_WORKERS_SPAWNED.fetch_add(effective_degree as u64, AtomicOrdering::Relaxed);
    let accepted = AtomicUsize::new(0);
    let execution_context = super::execution_context::ExecutionContext::capture(txn);

    let mut results: Vec<Result<Vec<PlanRow>, QueryError>> = (0..effective_degree as usize)
        .map(|_| Ok(Vec::new()))
        .collect();

    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(effective_degree as usize);
        for (lo, hi) in parts {
            let table_ref = table;
            let filter_ref = filter;
            let runtime_ref = runtime_filters;
            let db_ref = db;
            let accepted_ref = &accepted;
            let worker_context = execution_context.clone();

            handles.push(s.spawn(move || -> Result<Vec<PlanRow>, QueryError> {
                let worker = || {
                    worker_context.run(db_ref, || {
                        worker_full_row_limited(
                            db_ref,
                            txn,
                            table_ref,
                            filter_ref,
                            runtime_ref,
                            lo,
                            hi,
                            accepted_ref,
                            max_rows,
                        )
                    })
                };
                worker()
            }));
        }
        for (i, h) in handles.into_iter().enumerate() {
            results[i] = join_worker(h, "parallel scan worker panicked");
        }
    });

    let mut out = Vec::with_capacity(max_rows.min(64));
    for r in results {
        out.extend(r?);
        if out.len() >= max_rows {
            out.truncate(max_rows);
            break;
        }
    }
    Ok(out)
}

pub(super) fn execute_parallel_index_fast_scan(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    filter: Option<&Expr>,
    projected_columns: Option<&[usize]>,
    degree: u32,
) -> Result<Vec<PlanRow>, QueryError> {
    if db.has_pending_index_repairs_for(table.schema.id, &index.name) {
        return super::plan::read_plan_table_entries_chunked_for_parallel_fallback(
            db,
            txn,
            table,
            index,
            low,
            high,
            filter,
            projected_columns,
        );
    }
    let leaf_ids = db.range_index_leaf_ids(&table.schema, index, low.clone(), high.clone())?;
    let effective_degree = effective_degree_for_items(degree, leaf_ids.len());
    if effective_degree <= 1 {
        let heap = super::plan::index_scan_heap(db, table)?;
        let mut rows = Vec::new();
        let mut eval_ctx = super::eval::EvalQueryContext::default();
        let filter_owned = filter.cloned();
        let mut repairs =
            db.snapshot_leaf_repairs(&table.schema, index, low.clone(), high.clone(), txn);
        // A writer retiring keys mid-stream restarts the scan in buffered mode.
        while !db.visit_index_leaf_entry_chunks_for_snapshot(
            &mut repairs,
            &table.schema,
            index,
            &leaf_ids,
            None,
            low.clone(),
            high.clone(),
            |entries, page_max_trx_id| {
                super::plan::append_plan_table_entry_refs_chunk(
                    db,
                    txn,
                    table,
                    index,
                    &heap,
                    entries,
                    page_max_trx_id,
                    &filter_owned,
                    projected_columns,
                    &mut rows,
                    &mut eval_ctx,
                )
            },
        )? {
            rows.clear();
        }
        return Ok(rows);
    }

    let parts = item_partition(leaf_ids.len(), effective_degree);
    PARALLEL_INDEX_FAST_SCAN_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
    PARALLEL_WORKERS_SPAWNED.fetch_add(effective_degree as u64, AtomicOrdering::Relaxed);
    let execution_context = super::execution_context::ExecutionContext::capture(txn);

    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(effective_degree as usize);
        for (lo_idx, hi_idx) in parts {
            let leaf_slice = &leaf_ids[lo_idx..hi_idx];
            let seed_leaf = (lo_idx > 0).then(|| leaf_ids[lo_idx - 1]);
            let db_ref = db;
            let table_ref = table;
            let index_ref = index;
            let low_ref = low.clone();
            let high_ref = high.clone();
            let filter_owned = filter.cloned();
            let projected_ref = projected_columns;
            let worker_context = execution_context.clone();

            handles.push(s.spawn(move || -> Result<Vec<PlanRow>, QueryError> {
                let worker = || -> Result<Vec<PlanRow>, QueryError> {
                    worker_context.run(db_ref, || {
                        let heap = super::plan::index_scan_heap(db_ref, table_ref)?;
                        let mut rows = Vec::new();
                        let mut eval_ctx = super::eval::EvalQueryContext::default();
                        let mut repairs = db_ref.snapshot_leaf_repairs(
                            &table_ref.schema,
                            index_ref,
                            low_ref.clone(),
                            high_ref.clone(),
                            txn,
                        );
                        while !db_ref.visit_index_leaf_entry_chunks_for_snapshot(
                            &mut repairs,
                            &table_ref.schema,
                            index_ref,
                            leaf_slice,
                            seed_leaf,
                            low_ref.clone(),
                            high_ref.clone(),
                            |entries, page_max_trx_id| {
                                super::plan::append_plan_table_entry_refs_chunk(
                                    db_ref,
                                    txn,
                                    table_ref,
                                    index_ref,
                                    &heap,
                                    entries,
                                    page_max_trx_id,
                                    &filter_owned,
                                    projected_ref,
                                    &mut rows,
                                    &mut eval_ctx,
                                )
                            },
                        )? {
                            rows.clear();
                        }
                        Ok(rows)
                    })
                };
                worker()
            }));
        }
        collect_scan_workers(handles, "parallel index scan worker panicked")
    })
}

/// Heap-fetching parallel index range scan. Partitions the index leaf
/// chain bounded by `low`/`high` across `degree` workers; each worker
/// pulls its slice of `(IndexKey, RowId)` entries, fetches the rows
/// from the heap, and applies the optional WHERE filter + projection.
/// Output order is unordered — must run beneath a `Exchange`.
///
/// Mirrors [`execute_parallel_index_fast_scan`] but does the per-row
/// heap fetch via [`super::plan::read_visible_row_id_chunk_with_heap`]
/// instead of projecting from the index key. The serial counterpart is
/// [`super::plan::read_plan_table_ids_range_chunked`].
pub(super) fn execute_parallel_index_range_scan(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    filter: Option<&Expr>,
    projected_columns: Option<&[usize]>,
    degree: u32,
) -> Result<Vec<PlanRow>, QueryError> {
    if db.has_pending_index_repairs_for(table.schema.id, &index.name) {
        return super::plan::read_plan_table_entries_chunked_for_parallel_fallback(
            db,
            txn,
            table,
            index,
            low,
            high,
            filter,
            projected_columns,
        );
    }
    let leaf_ids = db.range_index_leaf_ids(&table.schema, index, low.clone(), high.clone())?;
    let effective_degree = effective_degree_for_items(degree, leaf_ids.len());
    if effective_degree <= 1 {
        // Tiny range — fall back to the serial cursor path so we don't
        // pay the worker-spawn tax for ~one leaf's worth of rows.
        let mut repairs =
            db.snapshot_leaf_repairs(&table.schema, index, low.clone(), high.clone(), txn);
        let row_ids = db.range_index_row_ids_for_leaf_ids_for_snapshot(
            &mut repairs,
            &table.schema,
            index,
            &leaf_ids,
            low,
            high,
        )?;
        return read_index_range_row_ids_with_heap(
            db,
            txn,
            table,
            index,
            row_ids,
            &filter.cloned(),
            projected_columns,
        );
    }

    let parts = item_partition(leaf_ids.len(), effective_degree);
    PARALLEL_INDEX_RANGE_SCAN_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
    PARALLEL_WORKERS_SPAWNED.fetch_add(effective_degree as u64, AtomicOrdering::Relaxed);
    let execution_context = super::execution_context::ExecutionContext::capture(txn);

    let filter_for_icp = filter.cloned();
    let icp = super::plan::IndexConditionPushdown::try_new(
        table,
        index,
        low.as_ref(),
        high.as_ref(),
        &filter_for_icp,
    );
    let icp = icp.as_ref();
    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(effective_degree as usize);
        for (lo_idx, hi_idx) in parts {
            let leaf_slice = &leaf_ids[lo_idx..hi_idx];
            // Dedup seed for the borrowed-entry walk: the last leaf of
            // the previous worker's slice (multi-valued indexes can
            // repeat a row across the boundary).
            let seed_leaf = (lo_idx > 0).then(|| leaf_ids[lo_idx - 1]);
            let db_ref = db;
            let table_ref = table;
            let index_ref = index;
            let low_ref = low.clone();
            let high_ref = high.clone();
            let filter_owned = filter.cloned();
            let projected_ref = projected_columns;
            let worker_context = execution_context.clone();

            let range_plan_key = super::analyze_stats::current_range_scan_plan_key();

            handles.push(s.spawn(move || -> Result<Vec<PlanRow>, QueryError> {
                let worker = || -> Result<Vec<PlanRow>, QueryError> {
                    super::analyze_stats::with_range_scan_plan_key_if_present(
                        range_plan_key,
                        || {
                            worker_context.run(db_ref, || {
                                let mut repairs = db_ref.snapshot_leaf_repairs(
                                    &table_ref.schema,
                                    index_ref,
                                    low_ref.clone(),
                                    high_ref.clone(),
                                    txn,
                                );
                                let row_ids = if let Some(icp) = icp {
                                    // ICP: pre-filter entries on the index key
                                    // before the heap fetch.
                                    let mut out: Vec<RowId> = Vec::new();
                                    let mut icp_ctx = super::eval::EvalQueryContext::default();
                                    db_ref.visit_index_leaf_entries_once_per_row_for_snapshot(
                                        &mut repairs,
                                        &table_ref.schema,
                                        index_ref,
                                        leaf_slice,
                                        seed_leaf,
                                        low_ref.clone(),
                                        high_ref.clone(),
                                        |entries| {
                                            for (key, row_id) in entries {
                                                if icp.matches(db_ref, txn, key, &mut icp_ctx)? {
                                                    out.push(*row_id);
                                                }
                                            }
                                            Ok(())
                                        },
                                    )?;
                                    out
                                } else {
                                    db_ref.range_index_row_ids_for_leaf_ids_for_snapshot(
                                        &mut repairs,
                                        &table_ref.schema,
                                        index_ref,
                                        leaf_slice,
                                        low_ref,
                                        high_ref,
                                    )?
                                };
                                read_index_range_row_ids_with_heap(
                                    db_ref,
                                    txn,
                                    table_ref,
                                    index_ref,
                                    row_ids,
                                    &filter_owned,
                                    projected_ref,
                                )
                            })
                        },
                    )
                };
                worker()
            }));
        }
        collect_scan_workers(handles, "parallel index range scan worker panicked")
    })
}

/// Worker count for `execute_parallel_index_multi_lookup`. Honours
/// `MYDB_PARALLEL_DEGREE` / `MYDB_PARALLEL_DISABLE`; defaults to
/// `default_parallel_degree()` so it matches the parallel SeqScan
/// settings used elsewhere on the same install.
fn index_multi_lookup_parallelism() -> u32 {
    if crate::util::env::var("MYDB_PARALLEL_DISABLE")
        .ok()
        .as_deref()
        == Some("1")
    {
        return 1;
    }
    crate::util::env::var("MYDB_INDEX_MULTI_LOOKUP_PARALLELISM")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or_else(crate::planner::default_parallel_degree)
        .max(1)
}

/// Minimum keys × estimated rows/key before the parallel fan-out is
/// worth the worker-spawn overhead. Override via env for differential
/// testing. Chosen to keep small `IN (...)` lookups serial — the cost
/// model already biases the runtime-filter rewrite toward small probe
/// keysets, but `IN` lists from user SQL can land here too.
const PARALLEL_INDEX_MULTI_LOOKUP_MIN_ROWS: usize = 4096;

/// Parallel non-covering `IndexMultiLookup`. Partitions the probe key
/// list across `degree` workers; each worker walks its slice through
/// `execute_index_multi_lookup_ids` + `read_plan_table_ids_with_heap`,
/// applying the optional filter and projection independently. Results
/// are concatenated in input-key order (workers receive contiguous key
/// chunks, so the concat preserves the same row ordering the serial
/// path emits).
///
/// Falls back to the serial path when:
///   * `degree <= 1` after env/disable clamps,
///   * the keyset × estimated rows-per-key is below
///     `PARALLEL_INDEX_MULTI_LOOKUP_MIN_ROWS` (worker spawn would
///     dominate),
///   * the table is partitioned (each partition has its own page
///     numbering; the existing serial path routes that case correctly).
///
/// For Q21 / Q17-shape probes (small build × big fact joined on a
/// secondary index) this turns the 240K row read into ~60K reads per
/// worker, the same way a parallel seq scan does for full-table scans.
pub(super) fn execute_parallel_index_multi_lookup(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    keys: &[IndexKey],
    exact: bool,
    filter: &Option<Expr>,
    projection: Option<&[usize]>,
    estimated_rows: f64,
) -> Result<Vec<PlanRow>, QueryError> {
    let ordered_keys = super::plan::runtime_ordered_multi_lookup_keys(keys)?;
    let keys = ordered_keys.as_ref();
    if table.schema.is_partitioned() {
        let ids = super::plan::execute_index_multi_lookup_ids(
            db,
            txn,
            &table.schema,
            index,
            keys,
            exact,
        )?;
        return super::plan::read_plan_table_ids(db, txn, table, ids, filter, projection);
    }
    let degree = index_multi_lookup_parallelism();
    let effective = effective_degree_for_items(degree, keys.len());
    if effective <= 1 || (estimated_rows as usize) < PARALLEL_INDEX_MULTI_LOOKUP_MIN_ROWS {
        let ids = super::plan::execute_index_multi_lookup_ids(
            db,
            txn,
            &table.schema,
            index,
            keys,
            exact,
        )?;
        return super::plan::read_plan_table_ids(db, txn, table, ids, filter, projection);
    }
    let parts = item_partition(keys.len(), effective);
    PARALLEL_WORKERS_SPAWNED.fetch_add(effective as u64, AtomicOrdering::Relaxed);

    let execution_context = super::execution_context::ExecutionContext::capture(txn);

    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(effective as usize);
        for (lo_idx, hi_idx) in parts {
            let key_slice = &keys[lo_idx..hi_idx];
            let db_ref = db;
            let table_ref = table;
            let index_ref = index;
            let filter_owned = filter.clone();
            let projection_ref = projection;
            let worker_context = execution_context.clone();

            handles.push(s.spawn(move || -> Result<Vec<PlanRow>, QueryError> {
                let worker = || -> Result<Vec<PlanRow>, QueryError> {
                    worker_context.run(db_ref, || {
                        let ids = super::plan::execute_index_multi_lookup_ids(
                            db_ref,
                            txn,
                            &table_ref.schema,
                            index_ref,
                            key_slice,
                            exact,
                        )?;
                        super::plan::read_plan_table_ids(
                            db_ref,
                            txn,
                            table_ref,
                            ids,
                            &filter_owned,
                            projection_ref,
                        )
                    })
                };
                worker()
            }));
        }
        collect_scan_workers(handles, "parallel index multi-lookup worker panicked")
    })
}

/// Worker-internal heap fetch for a precomputed slice of index entries.
/// Chunks the entries to amortise `EvalQueryContext` setup and to bound
/// the temporary RowId vector inside `read_visible_row_id_chunk_with_heap`.
/// RowId-only sibling of `read_index_range_entries_with_heap`: the
/// parallel index-range-scan path is always non-covering (covering
/// scans route to IndexFastScan / the covering aggregate pipeline), so
/// the index keys the old path materialised per entry were dropped
/// unread. Workers now feed bare RowIds straight into the heap lookup.
fn read_index_range_row_ids_with_heap(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    index: &IndexSchema,
    row_ids: Vec<RowId>,
    filter: &Option<Expr>,
    projection: Option<&[usize]>,
) -> Result<Vec<PlanRow>, QueryError> {
    const CHUNK_ROWS: usize = 4096;
    let heap = db.cached_heap_handle(table.schema.id)?;
    let mut rows = Vec::with_capacity(row_ids.len());
    let mut eval_ctx = super::eval::EvalQueryContext::default();
    let mut seen = (!index.unique).then(std::collections::HashSet::new);
    let mut iter = row_ids.into_iter();
    loop {
        let chunk: Vec<RowId> = iter.by_ref().take(CHUNK_ROWS).collect();
        if chunk.is_empty() {
            break;
        }
        super::plan::read_visible_row_id_chunk_with_heap(
            db,
            txn,
            table,
            &heap,
            filter,
            &mut eval_ctx,
            &mut rows,
            &mut seen,
            chunk,
            projection,
        )?;
    }
    Ok(rows)
}

pub(super) fn execute_parallel_index_aggregate(
    db: &Arc<Database>,
    txn: TxnView,
    runtime_plan: &PhysicalPlan,
    table: &PlanTable,
    index: &IndexSchema,
    low: Option<IndexBound>,
    high: Option<IndexBound>,
    group_by: &[Expr],
    having: Option<&Expr>,
    aggregates: &[Expr],
    degree: u32,
) -> Result<Vec<PlanRow>, QueryError> {
    if db.has_pending_index_repairs_for(table.schema.id, &index.name) {
        return super::plan::execute_covering_index_aggregate(
            db,
            txn,
            runtime_plan,
            table,
            index,
            low,
            high,
            group_by,
            having,
            aggregates,
        );
    }
    let spec = super::plan::build_covering_index_aggregate_spec(
        table, index, group_by, having, aggregates,
    )?;
    if low.is_none() && high.is_none() {
        let page_upper = db.index_page_id_upper_bound(&table.schema, index)?;
        let effective_degree = effective_degree_for_pages(degree, page_upper.saturating_sub(1));
        if effective_degree <= 1 {
            let partial = super::plan::scan_covering_index_aggregate_page_range(
                db, txn, table, index, 0, page_upper, None, None, &spec,
            )?;
            return finalize_ordered_index_aggregate(
                runtime_plan,
                table,
                having,
                &spec,
                vec![partial],
            );
        }

        let parts = page_partition(page_upper, effective_degree);
        PARALLEL_INDEX_AGGREGATE_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
        PARALLEL_WORKERS_SPAWNED.fetch_add(effective_degree as u64, AtomicOrdering::Relaxed);
        let execution_context = super::execution_context::ExecutionContext::capture(txn);

        let mut results: Vec<Result<super::plan::CoveringIndexAggregatePartial, QueryError>> = (0
            ..effective_degree as usize)
            .map(|_| {
                Ok(super::plan::CoveringIndexAggregatePartial {
                    groups: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
                    native_rows: 0,
                })
            })
            .collect();

        std::thread::scope(|s| {
            let mut handles = Vec::with_capacity(effective_degree as usize);
            for (page_lo, page_hi) in parts {
                let db_ref = db;
                let table_ref = table;
                let index_ref = index;
                let spec_ref = &spec;
                let worker_context = execution_context.clone();

                handles.push(s.spawn(
                    move || -> Result<super::plan::CoveringIndexAggregatePartial, QueryError> {
                        let worker = || {
                            worker_context.run(db_ref, || {
                                super::plan::scan_covering_index_aggregate_page_range(
                                    db_ref, txn, table_ref, index_ref, page_lo, page_hi, None,
                                    None, spec_ref,
                                )
                            })
                        };
                        worker()
                    },
                ));
            }
            for (i, h) in handles.into_iter().enumerate() {
                results[i] = join_worker(h, "parallel index aggregate worker panicked");
            }
        });
        let mut partials = Vec::with_capacity(results.len());
        for result in results {
            partials.push(result?);
        }
        return finalize_ordered_index_aggregate(runtime_plan, table, having, &spec, partials);
    }
    let leaf_ids = db.range_index_leaf_ids(&table.schema, index, low.clone(), high.clone())?;
    let effective_degree = effective_degree_for_items(degree, leaf_ids.len());
    if effective_degree <= 1 {
        let partial = super::plan::scan_covering_index_aggregate_leaf_ids(
            db, txn, table, index, low, high, &leaf_ids, None, &spec,
        )?;
        return finalize_ordered_index_aggregate(runtime_plan, table, having, &spec, vec![partial]);
    }

    let parts = item_partition(leaf_ids.len(), effective_degree);
    PARALLEL_INDEX_AGGREGATE_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
    PARALLEL_WORKERS_SPAWNED.fetch_add(effective_degree as u64, AtomicOrdering::Relaxed);
    let execution_context = super::execution_context::ExecutionContext::capture(txn);

    let mut results: Vec<Result<super::plan::CoveringIndexAggregatePartial, QueryError>> = (0
        ..effective_degree as usize)
        .map(|_| {
            Ok(super::plan::CoveringIndexAggregatePartial {
                groups: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
                native_rows: 0,
            })
        })
        .collect();

    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(effective_degree as usize);
        for (lo_idx, hi_idx) in parts {
            let leaf_slice = &leaf_ids[lo_idx..hi_idx];
            let seed_leaf_id = if lo_idx > 0 {
                Some(leaf_ids[lo_idx - 1])
            } else {
                None
            };
            let db_ref = db;
            let table_ref = table;
            let index_ref = index;
            let low_ref = low.clone();
            let high_ref = high.clone();
            let spec_ref = &spec;
            let worker_context = execution_context.clone();

            handles.push(s.spawn(
                move || -> Result<super::plan::CoveringIndexAggregatePartial, QueryError> {
                    let worker = || {
                        worker_context.run(db_ref, || {
                            super::plan::scan_covering_index_aggregate_leaf_ids(
                                db_ref,
                                txn,
                                table_ref,
                                index_ref,
                                low_ref,
                                high_ref,
                                leaf_slice,
                                seed_leaf_id,
                                spec_ref,
                            )
                        })
                    };
                    worker()
                },
            ));
        }
        for (i, h) in handles.into_iter().enumerate() {
            results[i] = join_worker(h, "parallel index aggregate worker panicked");
        }
    });
    let mut partials = Vec::with_capacity(results.len());
    for result in results {
        partials.push(result?);
    }
    finalize_ordered_index_aggregate(runtime_plan, table, having, &spec, partials)
}

fn finalize_ordered_index_aggregate(
    runtime_plan: &PhysicalPlan,
    table: &PlanTable,
    having: Option<&Expr>,
    spec: &super::plan::CoveringIndexAggregateSpec,
    partials: Vec<super::plan::CoveringIndexAggregatePartial>,
) -> Result<Vec<PlanRow>, QueryError> {
    let native_rows = partials.iter().map(|partial| partial.native_rows).sum();
    let rows =
        super::plan::finalize_covering_index_aggregate_partials(table, having, spec, partials)?;
    super::analyze_stats::add_aggregate_runtime(runtime_plan, "ordered_index", native_rows, 0, 0);
    Ok(rows)
}

fn run_one_worker(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: Option<&Expr>,
    runtime_filters: &[RuntimeFilter],
    projected_columns: Option<&[usize]>,
    page_lo: u64,
    page_hi: u64,
) -> Result<Vec<PlanRow>, QueryError> {
    run_one_worker_storage(
        db,
        txn,
        table,
        filter,
        runtime_filters,
        projected_columns,
        table.schema.id,
        page_lo,
        page_hi,
    )
}

fn run_one_worker_storage(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: Option<&Expr>,
    runtime_filters: &[RuntimeFilter],
    projected_columns: Option<&[usize]>,
    storage_id: u64,
    page_lo: u64,
    page_hi: u64,
) -> Result<Vec<PlanRow>, QueryError> {
    if let Some(projection) = projected_columns {
        worker_projected_storage(
            db,
            txn,
            table,
            filter,
            runtime_filters,
            projection,
            storage_id,
            page_lo,
            page_hi,
        )
    } else {
        worker_full_row_storage(
            db,
            txn,
            table,
            filter,
            runtime_filters,
            storage_id,
            page_lo,
            page_hi,
        )
    }
}

/// Cap the requested degree at `page_count` (no point spawning a worker
/// with no pages to scan) and at the global parallelism budget. Returns
/// `1` for degenerate cases so the caller takes the inline path.
pub(super) fn effective_degree_for_pages(requested: u32, page_count: u64) -> u32 {
    let cap_pages = page_count.max(1) as u32;
    requested.max(1).min(cap_pages).max(1)
}

fn effective_degree_for_items(requested: u32, item_count: usize) -> u32 {
    requested.max(1).min(item_count.max(1) as u32).max(1)
}

fn item_partition(item_count: usize, degree: u32) -> Vec<(usize, usize)> {
    let p = degree.max(1) as usize;
    if item_count == 0 {
        return (0..p).map(|_| (0usize, 0usize)).collect();
    }
    let chunk = item_count / p;
    let rem = item_count % p;
    (0..p)
        .map(|i| {
            let lo = i * chunk + i.min(rem);
            let extra = usize::from(i < rem);
            (lo, lo + chunk + extra)
        })
        .collect()
}

/// Fused parallel scan+aggregate: each worker streams its page range
/// directly into thread-local accumulators, bypassing PlanRow
/// materialisation entirely. Per-worker group states are merged at the
/// end. Returns `Ok(None)` when the plan shape doesn't match.
/// Vectorized counterpart to `try_execute_parallel_hash_aggregate_fused`.
///
/// Same plan-shape gate (`Exchange { ParallelSeqScan { table, filter } }`),
/// same page partitioning across workers, but each worker pushes its
/// scanned rows through a private `HashAggregateCore`. Workers return their
/// per-partition state snapshots; the main thread merges
/// across workers using `vec_hash_agg::merge_worker_partition_snapshots`. The
/// result is hand-built into `PlanRow`s matching the existing fused
/// path's output layout (Null-padded table-column fields + agg_values
/// cache keyed by `aggregate_key`) so the legacy projection layer can
/// consume it untouched.
///
/// HAVING is applied row-wise on the materialised post-aggregate output:
/// every aggregate referenced by HAVING (whether it also appears in the
/// SELECT list or only inside HAVING) gets its own vec slot so the
/// per-row `agg_values` cache is complete before `eval_aggregate_bool`
/// runs. For high-cardinality GROUP BY where HAVING culls most groups
/// (TPC-H Q18's subquery: 1.5M groups → ~60 survivors) the probe runs
/// against a minimal PlanRow that skips per-table-column PlanField
/// construction; survivors then build the full PlanRow.
///
/// Returns `Ok(None)` when:
///   * `MYDB_VECTORIZED` does not allow this input
///   * GROUP BY is empty (single implicit group: row-fused path owns it)
///   * an aggregate isn't in `{COUNT(*), COUNT, SUM, MIN, MAX, AVG}` or
///     its argument isn't a simple column reference / DISTINCT
///   * HAVING references an aggregate the vec path can't translate
///   * any GROUP BY expression isn't a simple column reference
///   * the planner picked `degree <= 1` for this page count
///   * filter compilation against the projected schema fails
///
/// How a worker computes the per-row value handed to an aggregate's
/// `consume_row` / `bump_implicit_*` call. Lives at module scope so
/// the typed-batch fast path (`WorkerAggStaging`) can describe its
/// per-agg input source uniformly.
#[derive(Clone, Eq)]
struct CachedRollupKey {
    hash: u64,
    values: Vec<crate::engine::expr::ValueKey>,
}

impl PartialEq for CachedRollupKey {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.values == other.values
    }
}

impl std::hash::Hash for CachedRollupKey {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl CachedRollupKey {
    #[inline]
    fn new(values: Vec<crate::engine::expr::ValueKey>, state: &ahash::RandomState) -> Self {
        let hash = Self::hash_values(&values, state);
        Self { hash, values }
    }

    #[inline]
    fn hash_values(values: &[crate::engine::expr::ValueKey], state: &ahash::RandomState) -> u64 {
        use std::hash::{BuildHasher, Hash, Hasher};
        let mut hasher = state.build_hasher();
        values.hash(&mut hasher);
        hasher.finish()
    }
}

pub(super) fn execute_parallel_rollup_aggregate(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    group_by: &[Expr],
    having: Option<&Expr>,
    aggregates: &[Expr],
    degree: u32,
    estimated_rows: f64,
) -> Result<Vec<PlanRow>, QueryError> {
    if degree <= 1 || !crate::planner::plan_contains_parallel(input) {
        return super::plan::execute_hash_rollup_aggregate(
            db, txn, input, group_by, having, aggregates,
        );
    }
    PARALLEL_HASH_AGGREGATE_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
    use super::aggregate::{collect_aggregate_slots, AggregateSlot};
    use crate::engine::expr::ValueKey;
    use hashbrown::hash_map::{Entry, RawEntryMut};
    use std::hash::{BuildHasher, Hash, Hasher};

    let mut slot_exprs: Vec<&Expr> = aggregates.iter().collect();
    if let Some(h) = having {
        slot_exprs.push(h);
    }
    let mut slots: Vec<AggregateSlot> = collect_aggregate_slots(&slot_exprs);
    super::aggregate::bind_slot_comparisons(&mut slots, input);
    // Prefix derivation now walks sorted finest groups with one live state
    // per level. Wide grouping keys no longer require duplicate hash maps or
    // a serial sort over all raw input rows. Unsupported aggregates still
    // select the canonical row/spill path before opening their input.
    if estimated_rows >= 100.0 {
        if let Some(finest) =
            super::vec_bridge::vec_rollup_finest_groups(db, txn, input, group_by, &slots)?
        {
            return super::plan::rollup_emit_from_vec_finest(finest, group_by, &slots, having);
        }
    }
    // See `aggregate_rollup_plan_rows`: the per-group representative row is only
    // needed for the MySQL hidden-column extension, detectable from the slots.
    let needs_sample_fields = slots.iter().any(|slot| {
        slot.name
            .eq_ignore_ascii_case(crate::parser::HIDDEN_FIRST_VALUE_FN)
    });
    let (input_rows, _input_charge) = match super::plan::prepare_rollup_input(db, txn, input)? {
        super::plan::PreparedRollupInput::Rows(rows, charge) => (rows, charge),
        super::plan::PreparedRollupInput::Stream(stream) => {
            return super::plan::aggregate_rollup_plan_stream_spilled_with_degree(
                db,
                stream,
                group_by,
                having,
                &slots,
                degree as usize,
            );
        }
    };
    let p = degree.max(1) as usize;
    // Under budget pressure each worker's partial rollup map would itself
    // blow the budget; route to the serial sort-based spilling path instead.
    if super::plan::should_spill_row_hash_agg(input_rows.len(), group_by, &slots) {
        return super::plan::aggregate_rollup_plan_rows_spilled(
            db, input_rows, group_by, having, &slots,
        );
    }
    if input_rows.is_empty() || input_rows.len() < 2 * p {
        return super::plan::aggregate_rollup_plan_rows(db, input_rows, group_by, having, &slots);
    }

    type PartialMap = hashbrown::HashMap<
        (usize, CachedRollupKey),
        super::plan::RollupGroupState,
        ahash::RandomState,
    >;

    let num_partitions: usize = {
        let raw = crate::util::env::var("MYDB_PARALLEL_AGG_PARTITIONS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or((p * 2).max(8));
        raw.next_power_of_two().min(64).max(p)
    };
    let partition_mask = num_partitions - 1;
    let hasher_state = ahash::RandomState::new();
    let group_by_len = group_by.len();
    let chunk = input_rows.len().div_ceil(p);
    let mut input_chunks: Vec<Vec<PlanRow>> = Vec::with_capacity(p);
    {
        let mut iter = input_rows.into_iter();
        for _ in 0..p {
            let mut bucket = Vec::with_capacity(chunk);
            for _ in 0..chunk {
                match iter.next() {
                    Some(row) => bucket.push(row),
                    None => break,
                }
            }
            input_chunks.push(bucket);
        }
    }

    PARALLEL_WORKERS_SPAWNED.fetch_add(p as u64, AtomicOrdering::Relaxed);
    let execution_context = super::execution_context::ExecutionContext::capture(txn);

    let slots_ref = &slots;
    let group_by_ref = group_by;
    let hasher_ref = &hasher_state;
    let mut results: Vec<Result<Vec<PartialMap>, QueryError>> =
        (0..p).map(|_| Ok(Vec::new())).collect();

    std::thread::scope(|s| {
        let mut handles: Vec<
            Option<std::thread::ScopedJoinHandle<'_, Result<Vec<PartialMap>, QueryError>>>,
        > = Vec::with_capacity(p);
        for chunk in input_chunks {
            if chunk.is_empty() {
                handles.push(None);
                continue;
            }
            let db_ref = db;
            let worker_context = execution_context.clone();

            handles.push(Some(s.spawn(
                move || -> Result<Vec<PartialMap>, QueryError> {
                    let worker = || -> Result<Vec<PartialMap>, QueryError> {
                        worker_context.run(db_ref, || {
                            // Per-worker two-phase rollup over this chunk: aggregate the
                            // finest set with ONE probe per row, then derive coarser
                            // prefix levels locally over the (far fewer) finest groups.
                            // Cross-worker combination happens later via the existing
                            // partition merge — rollup is associative, so a worker's
                            // partial level-k group merges with another worker's via
                            // `merge_rollup_group_state`. This replaces the old
                            // N+1-probes-per-row scatter (the dominant cost on wide
                            // joins) and also drops the unconditional `row.fields.clone()`.
                            type LocalMap = hashbrown::HashMap<
                                CachedRollupKey,
                                super::plan::RollupGroupState,
                                ahash::RandomState,
                            >;
                            let mut finer: LocalMap =
                                hashbrown::HashMap::with_hasher(ahash::RandomState::new());
                            let mut tick: u32 = 0;
                            for row in &chunk {
                                tick = tick.wrapping_add(1);
                                if tick & 0xffff == 0 {
                                    crate::engine::cancel::check_cancelled()?;
                                }
                                let full_values = group_by_ref
                                    .iter()
                                    .map(|expr| super::eval::eval_plan_expr(expr, row))
                                    .collect::<Result<Vec<_>, _>>()?;
                                let slot_inputs = super::plan::rollup_slot_inputs(slots_ref, row)?;
                                let key = CachedRollupKey::new(
                                    full_values.iter().map(ValueKey::from).collect(),
                                    hasher_ref,
                                );
                                match finer.entry(key) {
                                    Entry::Occupied(mut e) => {
                                        super::plan::update_rollup_accumulators_ordered(
                                            e.get_mut(),
                                            slots_ref,
                                            &slot_inputs,
                                            row,
                                        )?;
                                    }
                                    Entry::Vacant(e) => {
                                        let mut state = super::plan::RollupGroupState {
                                            prefix_len: group_by_len,
                                            key_values: full_values,
                                            sample_fields: if needs_sample_fields {
                                                row.fields.clone()
                                            } else {
                                                Vec::new()
                                            },
                                            accumulators: slots_ref
                                                .iter()
                                                .map(|slot| slot.make_accumulator())
                                                .collect(),
                                        };
                                        super::plan::update_rollup_accumulators_ordered(
                                            &mut state,
                                            slots_ref,
                                            &slot_inputs,
                                            row,
                                        )?;
                                        e.insert(state);
                                    }
                                }
                            }

                            let mut local_parts: Vec<PartialMap> =
                                (0..num_partitions).map(|_| PartialMap::default()).collect();
                            // Scatter a finished level's groups into the partitioned
                            // maps, keyed by (prefix_len, key) so identical groups from
                            // other workers land in the same partition for merge.
                            let scatter = |prefix_len: usize,
                                           key: CachedRollupKey,
                                           state: super::plan::RollupGroupState,
                                           local_parts: &mut Vec<PartialMap>| {
                                let mut hasher = hasher_ref.build_hasher();
                                prefix_len.hash(&mut hasher);
                                key.hash(&mut hasher);
                                let pidx = (hasher.finish() as usize) & partition_mask;
                                match local_parts[pidx].entry((prefix_len, key)) {
                                    Entry::Occupied(mut e) => {
                                        super::plan::merge_rollup_group_state(e.get_mut(), state);
                                    }
                                    Entry::Vacant(e) => {
                                        e.insert(state);
                                    }
                                }
                            };

                            let mut finer_prefix = group_by_len;
                            for prefix_len in (0..group_by_len).rev() {
                                let mut level: LocalMap =
                                    hashbrown::HashMap::with_hasher(ahash::RandomState::new());
                                for (finer_key, finer_state) in finer.iter() {
                                    let prefix = &finer_key.values[..prefix_len];
                                    let key_hash = CachedRollupKey::hash_values(prefix, hasher_ref);
                                    let map_hash = {
                                        let mut hasher = level.hasher().build_hasher();
                                        hasher.write_u64(key_hash);
                                        hasher.finish()
                                    };
                                    match level.raw_entry_mut().from_hash(map_hash, |key| {
                                        key.hash == key_hash && key.values.as_slice() == prefix
                                    }) {
                                        RawEntryMut::Occupied(mut e) => {
                                            let dst = e.get_mut();
                                            for (acc, src) in dst
                                                .accumulators
                                                .iter_mut()
                                                .zip(finer_state.accumulators.iter())
                                            {
                                                acc.merge_from(src.clone_box().into_any());
                                            }
                                        }
                                        RawEntryMut::Vacant(e) => {
                                            e.insert_hashed_nocheck(
                                                map_hash,
                                                CachedRollupKey {
                                                    hash: key_hash,
                                                    values: prefix.to_vec(),
                                                },
                                                super::plan::RollupGroupState {
                                                    prefix_len,
                                                    key_values: finer_state.key_values
                                                        [..prefix_len]
                                                        .to_vec(),
                                                    sample_fields: if needs_sample_fields {
                                                        finer_state.sample_fields.clone()
                                                    } else {
                                                        Vec::new()
                                                    },
                                                    accumulators: finer_state
                                                        .accumulators
                                                        .iter()
                                                        .map(|a| a.clone_box())
                                                        .collect(),
                                                },
                                            );
                                        }
                                    }
                                }
                                for (key, state) in finer.drain() {
                                    scatter(finer_prefix, key, state, &mut local_parts);
                                }
                                finer = level;
                                finer_prefix = prefix_len;
                            }
                            for (key, state) in finer.drain() {
                                scatter(finer_prefix, key, state, &mut local_parts);
                            }
                            Ok(local_parts)
                        })
                    };
                    worker()
                },
            )));
        }
        for (i, handle) in handles.into_iter().enumerate() {
            if let Some(handle) = handle {
                results[i] = join_worker(handle, "parallel rollup aggregate worker panicked");
            }
        }
    });

    let mut phase1: Vec<Vec<PartialMap>> = Vec::with_capacity(p);
    for result in results {
        phase1.push(result?);
    }
    let mut per_partition: Vec<Vec<PartialMap>> = (0..num_partitions).map(|_| Vec::new()).collect();
    for worker_partials in phase1.into_iter() {
        for (pidx, partial) in worker_partials.into_iter().enumerate() {
            if !partial.is_empty() {
                per_partition[pidx].push(partial);
            }
        }
    }

    let p2_workers = p.min(num_partitions);
    let p2_chunk = num_partitions.div_ceil(p2_workers.max(1));
    PARALLEL_WORKERS_SPAWNED.fetch_add(p2_workers as u64, AtomicOrdering::Relaxed);
    let mut p2_results: Vec<Result<Vec<PlanRow>, QueryError>> =
        (0..p2_workers).map(|_| Ok(Vec::new())).collect();

    let rollup_topk_hint = super::plan::current_rollup_window_topk_hint();
    let rollup_topk_hint = &rollup_topk_hint;
    std::thread::scope(|s| {
        let mut handles: Vec<
            Option<std::thread::ScopedJoinHandle<'_, Result<Vec<PlanRow>, QueryError>>>,
        > = Vec::with_capacity(p2_workers);
        let mut partition_iter = per_partition.into_iter();
        for _ in 0..p2_workers {
            let mut my_parts: Vec<Vec<PartialMap>> = Vec::with_capacity(p2_chunk);
            for _ in 0..p2_chunk {
                if let Some(partition) = partition_iter.next() {
                    my_parts.push(partition);
                } else {
                    break;
                }
            }
            if my_parts.iter().all(|partition| partition.is_empty()) {
                handles.push(None);
                continue;
            }

            let db_ref = db;
            let worker_context = execution_context.clone();

            handles.push(Some(s.spawn(
                move || -> Result<Vec<PlanRow>, QueryError> {
                    let worker = || -> Result<Vec<PlanRow>, QueryError> {
                        worker_context.run(db_ref, || {
                            let mut states = Vec::new();
                            for partition in my_parts.into_iter() {
                                if partition.is_empty() {
                                    continue;
                                }
                                let mut merged: PartialMap = PartialMap::default();
                                for partial in partition {
                                    for (key, worker_state) in partial {
                                        match merged.entry(key) {
                                            Entry::Vacant(e) => {
                                                e.insert(worker_state);
                                            }
                                            Entry::Occupied(mut e) => {
                                                super::plan::merge_rollup_group_state(
                                                    e.get_mut(),
                                                    worker_state,
                                                );
                                            }
                                        }
                                    }
                                }
                                states.extend(merged.into_values());
                            }
                            super::plan::emit_parallel_rollup_group_states(
                                group_by_ref,
                                slots_ref,
                                group_by_len,
                                states,
                                having,
                                rollup_topk_hint.clone(),
                            )
                        })
                    };
                    worker()
                },
            )));
        }
        for (i, handle) in handles.into_iter().enumerate() {
            if let Some(handle) = handle {
                p2_results[i] = join_worker(handle, "parallel rollup merge worker panicked");
            }
        }
    });

    let mut out = Vec::new();
    for result in p2_results {
        out.extend(result?);
    }
    Ok(out)
}

/// Slice `rows` into chunks of size `chunk_size` (the final chunk may
/// be smaller), preserving the original order.
///
/// The naive `while !rows.is_empty() { chunks.push(rows.drain(..take).collect()); }`
/// drains from the front, which forces `Vec` to memmove every
/// remaining element down to fill the gap on each iteration —
/// `~1.5×N` extra bytes-moved on top of the unavoidable per-chunk
/// allocation. We instead pre-allocate each chunk with the right
/// capacity and `extend` it from a single `IntoIter` so every element
/// moves exactly once with no memmove of the remainder. Source order
/// is preserved and chunk sizes are as balanced as `div_ceil` allows
/// (only the last chunk may be smaller).
pub(super) fn split_off_into_chunks<T>(rows: Vec<T>, chunk_size: usize) -> Vec<Vec<T>> {
    if rows.is_empty() || chunk_size == 0 {
        return Vec::new();
    }
    let n = rows.len();
    let num_chunks = n.div_ceil(chunk_size);
    let mut chunks: Vec<Vec<T>> = Vec::with_capacity(num_chunks);
    let mut iter = rows.into_iter();
    for _ in 0..num_chunks {
        let mut chunk = Vec::with_capacity(chunk_size);
        chunk.extend(iter.by_ref().take(chunk_size));
        chunks.push(chunk);
    }
    chunks
}

/// Phase 4 parallel sort: chunk the materialised input into `degree`
/// equal slices, sort each slice in its own worker, then k-way-merge
/// the per-worker outputs. Total work is O(N log N / P + N log P)
/// instead of the serial O(N log N).
///
/// Falls back to a single-thread serial sort when:
///   - `degree <= 1`
///   - input is shorter than `2 * degree` rows (chunking overhead
///     dominates)
pub(super) fn execute_parallel_sort(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    order_by: &[OrderBy],
    degree: u32,
) -> Result<Vec<PlanRow>, QueryError> {
    let rows = super::plan::execute_plan_rows(db, txn, input)?;
    let p = degree.max(1) as usize;
    if p <= 1 || rows.len() < 2 * p {
        return super::external_sort::sort_plan_rows(db.data_dir(), rows, order_by);
    }

    let sort_work_bytes = super::external_sort::estimate_sort_work_bytes(&rows, order_by);
    let mut sort_charge = None;
    if let Some(budget) = crate::engine::executor::mem_budget::current_budget() {
        match crate::engine::executor::mem_budget::OwnedMemCharge::new(budget, sort_work_bytes) {
            Ok(charge) => sort_charge = Some(charge),
            Err(_) => {
                return super::external_sort::sort_plan_rows_parallel_spill(
                    db.data_dir(),
                    rows,
                    order_by,
                    p,
                    Some((db, super::execution_context::ExecutionContext::capture(txn))),
                );
            }
        }
    }
    let _sort_charge = sort_charge;

    // Equal-size chunks. `split_off_into_chunks` avoids the O(N) memmove
    // that `drain(..take).collect()` triggers — each iteration there
    // shifts the remaining elements down to fill the gap, accumulating
    // ~1.5× N extra bytes-moved on top of the chunk allocations. Tail-
    // first split_off leaves the front in place and copies only each
    // chunk's worth of rows once.
    let total = rows.len();
    let chunk_size = total.div_ceil(p);
    let chunks = split_off_into_chunks(rows, chunk_size);
    PARALLEL_SORT_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
    PARALLEL_WORKERS_SPAWNED.fetch_add(chunks.len() as u64, AtomicOrdering::Relaxed);
    let _ = total;

    let execution_context = super::execution_context::ExecutionContext::capture(txn);
    let mut sorted_chunks: Vec<Option<Result<PreparedParallelSortChunk, QueryError>>> =
        (0..chunks.len()).map(|_| None).collect();
    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            let context = execution_context.clone();
            handles.push(
                s.spawn(move || context.run(db, || prepare_parallel_sort_chunk(chunk, order_by))),
            );
        }
        for (i, h) in handles.into_iter().enumerate() {
            sorted_chunks[i] = Some(join_worker(h, "parallel sort worker panicked"));
        }
    });

    // Collect successful chunks, propagate first error.
    let mut chunks_ok: Vec<Vec<ParallelSortRow>> = Vec::with_capacity(sorted_chunks.len());
    let mut key_charges = Vec::with_capacity(sorted_chunks.len());
    for r in sorted_chunks {
        let chunk = r.expect("all worker handles joined")?;
        chunks_ok.push(chunk.rows);
        key_charges.push(chunk.charge);
    }
    let result = k_way_merge_sorted(chunks_ok, order_by);
    drop(key_charges);
    result
}

struct PreparedParallelSortChunk {
    rows: Vec<ParallelSortRow>,
    charge: crate::engine::executor::mem_budget::ScopedCharge,
}

struct ParallelSortRow {
    keys: smallvec::SmallVec<[Value; 2]>,
    encoded_single_int: Option<u64>,
    row: PlanRow,
}

struct ParallelDistinctTopKPayload {
    row: PlanRow,
    key: Arc<[crate::engine::expr::ValueKey]>,
    key_charge: usize,
}

impl crate::engine::executor::memory_footprint::AllocatedBytes for ParallelDistinctTopKPayload {
    fn allocated_bytes(&self) -> usize {
        self.row.allocated_bytes().saturating_add(self.key_charge)
    }
}

struct ParallelDistinctTopKChunk {
    candidates: crate::engine::executor::sort::TopKCandidates<ParallelDistinctTopKPayload>,
    input_rows: u64,
    typed_predicate_rows: u64,
    missing_layout_fallback_rows: u64,
    projection_fallback_rows: u64,
    count_fallback_rows: u64,
    unknown_layout_fallback_rows: u64,
    predicate_fallback_rows: u64,
    late_materialized_rows: u64,
    // Reservations move with the candidates to the coordinator and therefore
    // remain live until the last owner is merged or dropped.
}

struct WorkerDistinctTopK<'a> {
    order_by: &'a [OrderBy],
    projection: &'a [crate::parser::SelectItem],
    topk: crate::engine::executor::sort::BoundedTopK<ParallelDistinctTopKPayload>,
    retained: crate::engine::executor::sort::TopKSet<Arc<[crate::engine::expr::ValueKey]>>,
    order_program: Option<super::eval::PlanOrderProgram>,
    scratch: Vec<Value>,
    prepared_rows: u64,
    typed_predicate_rows: u64,
    missing_layout_fallback_rows: u64,
    projection_fallback_rows: u64,
    count_fallback_rows: u64,
    unknown_layout_fallback_rows: u64,
    predicate_fallback_rows: u64,
    late_materialized_rows: u64,
}

impl<'a> WorkerDistinctTopK<'a> {
    fn new(
        order_by: &'a [OrderBy],
        projection: &'a [crate::parser::SelectItem],
        k: usize,
        fields: &[crate::engine::executor::sort::SortField],
    ) -> Result<Self, QueryError> {
        let retained = crate::engine::executor::sort::TopKSet::new();
        Ok(Self {
            order_by,
            projection,
            topk: crate::engine::executor::sort::BoundedTopK::new(&fields, k)?,
            retained,
            order_program: None,
            scratch: Vec::with_capacity(order_by.len()),
            prepared_rows: 0,
            typed_predicate_rows: 0,
            missing_layout_fallback_rows: 0,
            projection_fallback_rows: 0,
            count_fallback_rows: 0,
            unknown_layout_fallback_rows: 0,
            predicate_fallback_rows: 0,
            late_materialized_rows: 0,
        })
    }

    fn push(&mut self, row: PlanRow) -> Result<(), QueryError> {
        self.scratch.clear();
        let program = self.order_program.get_or_insert_with(|| {
            super::eval::PlanOrderProgram::compile_once(&row, self.order_by)
        });
        if self.order_by.len() == 1 {
            self.scratch.push(program.eval_single_key(&row)?);
        } else {
            self.scratch.extend(program.eval_keys(&row)?);
        }
        self.prepared_rows = self.prepared_rows.saturating_add(1);
        if !self.topk.would_accept(&self.scratch) {
            return Ok(());
        }
        self.push_prepared(row)
    }

    /// Check a pre-evaluated single ORDER BY key before materialising the
    /// input PlanRow. The caller must invoke `push_prepared` immediately for
    /// an accepted key; both methods share `scratch` to avoid a second key
    /// clone/evaluation.
    fn would_accept_single_key(&mut self, key: &Value) -> bool {
        debug_assert_eq!(self.order_by.len(), 1);
        self.scratch.clear();
        self.scratch.push(key.clone());
        self.prepared_rows = self.prepared_rows.saturating_add(1);
        self.topk.would_accept(&self.scratch)
    }

    fn push_prepared(&mut self, row: PlanRow) -> Result<(), QueryError> {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        use crate::engine::expr::ValueKey;

        let mut values = Vec::with_capacity(self.projection.len());
        for item in self.projection {
            for value in super::eval::project_item(&row, item) {
                values.push(ValueKey::from(&value?));
            }
        }
        let key: Arc<[ValueKey]> = values.into();
        if self.retained.contains(&key) {
            return Ok(());
        }
        let key_charge = std::mem::size_of::<usize>()
            .saturating_mul(2)
            .saturating_add(key.len().saturating_mul(std::mem::size_of::<ValueKey>()))
            .saturating_add(
                key.iter()
                    .map(AllocatedBytes::allocated_bytes)
                    .sum::<usize>(),
            );
        self.retained.insert(key.clone())?;
        let payload = ParallelDistinctTopKPayload {
            row,
            key,
            key_charge,
        };
        let (accepted, evicted) = self.topk.push_with_evicted(payload, self.scratch.clone())?;
        debug_assert!(accepted, "would_accept and push_with_evicted diverged");
        if let Some(evicted) = evicted {
            self.retained.remove(&evicted.item.key);
        }
        Ok(())
    }

    fn finish(self) -> ParallelDistinctTopKChunk {
        super::eval::ORDER_BY_KEY_ROWS_PREPARED
            .fetch_add(self.prepared_rows, std::sync::atomic::Ordering::Relaxed);
        ParallelDistinctTopKChunk {
            candidates: self.topk.drain_sorted(),
            input_rows: self.prepared_rows,
            typed_predicate_rows: self.typed_predicate_rows,
            missing_layout_fallback_rows: self.missing_layout_fallback_rows,
            projection_fallback_rows: self.projection_fallback_rows,
            count_fallback_rows: self.count_fallback_rows,
            unknown_layout_fallback_rows: self.unknown_layout_fallback_rows,
            predicate_fallback_rows: self.predicate_fallback_rows,
            late_materialized_rows: self.late_materialized_rows,
        }
    }
}

/// Push a DISTINCT TopK through an unordered Exchange over a parallel heap
/// scan. Each worker retains at most K distinct tuples; the coordinator sees
/// at most P*K candidates. A tuple absent from a partition's local top K had
/// at least K better distinct tuples in that same partition, so it cannot be
/// part of the global top K. This is therefore exact, not an approximation.
fn try_execute_parallel_distinct_topk_scan(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    order_by: &[OrderBy],
    projection: &[crate::parser::SelectItem],
    k: usize,
) -> Result<Option<Vec<PlanRow>>, QueryError> {
    let PhysicalPlan::Exchange {
        input: scan,
        degree,
        output_order: ExchangeOrder::Unordered,
        ..
    } = input
    else {
        return Ok(None);
    };
    let (table, filter, runtime_filters, projected_columns, use_zone_map) = match scan.as_ref() {
        PhysicalPlan::ParallelSeqScan {
            table,
            filter,
            runtime_filters,
            projected_columns,
            ..
        } => (table, filter, runtime_filters, projected_columns, false),
        PhysicalPlan::ParallelZoneMapScan {
            table,
            filter,
            runtime_filters,
            projected_columns,
            ..
        } => (table, filter, runtime_filters, projected_columns, true),
        _ => return Ok(None),
    };
    if table.schema.is_partitioned()
        || filter
            .as_ref()
            .is_some_and(super::simple::expr_has_subquery)
    {
        return Ok(None);
    }
    let (tasks, worker_count) = scan_morsels(db, table, filter.as_ref(), use_zone_map, *degree)?;
    if worker_count <= 1 || tasks.is_empty() {
        return Ok(None);
    }

    EXCHANGE_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
    PARALLEL_SEQ_SCAN_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
    if use_zone_map {
        PARALLEL_ZONE_MAP_SCAN_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
        record_parallel_task_partition_visits(table, &[tasks.clone()]);
    }
    PARALLEL_SORT_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
    PARALLEL_WORKERS_SPAWNED.fetch_add(worker_count as u64, AtomicOrdering::Relaxed);

    let analyze_started = super::analyze_stats::is_recording().then(std::time::Instant::now);
    let analyze_io_start = analyze_started.map(|_| db.buffer_stats_counts());
    let direct_order_slot = if order_by.len() == 1 && runtime_filters.is_empty() {
        projected_columns.as_deref().and_then(|projected| {
            super::plan::simple_column_index_of(&order_by[0].expr, &table.schema)
                .and_then(|column| projected.iter().position(|candidate| *candidate == column))
        })
    } else {
        None
    };

    let mut fields = crate::engine::executor::sort::fields_from_order_by(order_by);
    for (field, order) in fields.iter_mut().zip(order_by) {
        field.collation_id = crate::engine::expr::expr_collation(&order.expr, &table.schema)
            .map(|info| info.collation_id);
    }
    let fields = &fields;
    let tasks = Arc::new(tasks);
    let next_task = Arc::new(AtomicUsize::new(0));
    let execution_context =
        super::execution_context::ExecutionContext::capture(txn).fork_worker_cancellation();
    let mut chunks: Vec<Result<ParallelDistinctTopKChunk, QueryError>> = (0..worker_count)
        .map(|_| {
            Err(error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "worker not started",
            ))
        })
        .collect();
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let tasks = Arc::clone(&tasks);
            let next_task = Arc::clone(&next_task);
            let worker_context = execution_context.clone();
            handles.push(scope.spawn(move || {
                worker_context.run(db, || {
                    let mut collector = WorkerDistinctTopK::new(order_by, projection, k, fields)?;
                    loop {
                        let task_index = next_task.fetch_add(1, AtomicOrdering::Relaxed);
                        let Some(task) = tasks.get(task_index) else {
                            break;
                        };
                        if let Some(projected) = projected_columns.as_deref() {
                            let late_materialized = match direct_order_slot {
                                Some(order_slot) => worker_projected_distinct_topk_typed(
                                    db,
                                    txn,
                                    table,
                                    filter.as_ref(),
                                    projected.as_slice(),
                                    order_slot,
                                    task.storage_id,
                                    task.page_lo,
                                    task.page_hi,
                                    &mut collector,
                                )?,
                                None => false,
                            };
                            if !late_materialized {
                                worker_projected_streaming(
                                    db,
                                    txn,
                                    table,
                                    filter.as_ref(),
                                    runtime_filters,
                                    projected.as_slice(),
                                    task.storage_id,
                                    task.page_lo,
                                    task.page_hi,
                                    |row| {
                                        collector.push(row)?;
                                        Ok(true)
                                    },
                                )?;
                            }
                        } else {
                            worker_full_row_streaming(
                                db,
                                txn,
                                table,
                                filter.as_ref(),
                                runtime_filters,
                                task.storage_id,
                                task.page_lo,
                                task.page_hi,
                                |row| {
                                    collector.push(row)?;
                                    Ok(true)
                                },
                            )?;
                        }
                    }
                    Ok(collector.finish())
                })
            }));
        }
        for (index, handle) in handles.into_iter().enumerate() {
            chunks[index] = match handle.join() {
                Ok(result) => result,
                Err(_) => {
                    PARALLEL_WORKER_PANIC_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
                    Err(error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "parallel distinct top-k worker panicked",
                    ))
                }
            };
        }
    });

    if let Some(started) = analyze_started {
        let elapsed = started.elapsed().as_nanos();
        let input_rows = chunks
            .iter()
            .filter_map(|chunk| chunk.as_ref().ok())
            .map(|chunk| chunk.input_rows)
            .sum();
        let exchanged_rows = chunks
            .iter()
            .filter_map(|chunk| chunk.as_ref().ok())
            .map(|chunk| chunk.candidates.len() as u64)
            .sum();
        let io_start = analyze_io_start.expect("parallel distinct TopK I/O snapshot missing");
        super::analyze_stats::record_with_io_snapshot(
            db,
            scan.as_ref(),
            elapsed,
            input_rows,
            io_start,
        );
        super::analyze_stats::record_with_io_snapshot(db, input, elapsed, exchanged_rows, io_start);
        let typed_rows = chunks
            .iter()
            .filter_map(|chunk| chunk.as_ref().ok())
            .map(|chunk| chunk.typed_predicate_rows)
            .sum();
        let fallback_rows = chunks
            .iter()
            .filter_map(|chunk| chunk.as_ref().ok())
            .map(|chunk| {
                chunk
                    .missing_layout_fallback_rows
                    .saturating_add(chunk.projection_fallback_rows)
                    .saturating_add(chunk.count_fallback_rows)
                    .saturating_add(chunk.unknown_layout_fallback_rows)
                    .saturating_add(chunk.predicate_fallback_rows)
            })
            .sum();
        let late_materialized_rows = chunks
            .iter()
            .filter_map(|chunk| chunk.as_ref().ok())
            .map(|chunk| chunk.late_materialized_rows)
            .sum();
        super::analyze_stats::add_vector_runtime(
            scan.as_ref(),
            0,
            late_materialized_rows,
            worker_count as u32,
        );
        if typed_rows > 0 || fallback_rows > 0 {
            let missing_layout_rows: u64 = chunks
                .iter()
                .filter_map(|chunk| chunk.as_ref().ok())
                .map(|chunk| chunk.missing_layout_fallback_rows)
                .sum();
            let projection_rows: u64 = chunks
                .iter()
                .filter_map(|chunk| chunk.as_ref().ok())
                .map(|chunk| chunk.projection_fallback_rows)
                .sum();
            let count_rows: u64 = chunks
                .iter()
                .filter_map(|chunk| chunk.as_ref().ok())
                .map(|chunk| chunk.count_fallback_rows)
                .sum();
            let semantic_rows: u64 = chunks
                .iter()
                .filter_map(|chunk| chunk.as_ref().ok())
                .map(|chunk| chunk.predicate_fallback_rows)
                .sum();
            let mode = if fallback_rows == 0 {
                "typed"
            } else if missing_layout_rows == fallback_rows {
                "mixed_missing_layout"
            } else if projection_rows == fallback_rows {
                "mixed_projection"
            } else if count_rows == fallback_rows {
                "mixed_stored_count"
            } else if semantic_rows == fallback_rows {
                "mixed_semantic"
            } else {
                "mixed"
            };
            super::analyze_stats::add_expression_runtime(
                scan.as_ref(),
                mode,
                typed_rows,
                fallback_rows,
            );
        }
    }

    let mut topk = crate::engine::executor::sort::BoundedTopK::new(&fields, k)?;
    let mut retained = crate::engine::executor::sort::TopKSet::new();
    for chunk in &mut chunks {
        let chunk = chunk.as_mut().map_err(|error| error.clone())?;
        for candidate in chunk.candidates.by_ref() {
            if !topk.would_accept(&candidate.keys) || retained.contains(&candidate.item.key) {
                continue;
            }
            retained.insert(candidate.item.key.clone())?;
            let (accepted, evicted) = topk.push_candidate(candidate)?;
            debug_assert!(accepted, "would_accept and push_with_evicted diverged");
            if let Some(evicted) = evicted {
                retained.remove(&evicted.item.key);
            }
        }
    }
    Ok(Some(
        topk.into_sorted_vec()?
            .into_iter()
            .map(|payload| payload.row)
            .collect(),
    ))
}

pub(super) fn execute_parallel_distinct_topk(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    order_by: &[OrderBy],
    projection: &[crate::parser::SelectItem],
    k: usize,
    _degree: u32,
) -> Result<Vec<PlanRow>, QueryError> {
    if k == 0 {
        let _ = super::plan::execute_plan_rows(db, txn, input)?;
        return Ok(Vec::new());
    }
    if let Some(rows) =
        try_execute_parallel_distinct_topk_scan(db, txn, input, order_by, projection, k)?
    {
        return Ok(rows);
    }
    super::plan::execute_distinct_topk(db, txn, input, order_by, projection, k)
}

fn prepare_parallel_sort_chunk(
    chunk: Vec<PlanRow>,
    order_by: &[OrderBy],
) -> Result<PreparedParallelSortChunk, QueryError> {
    let program = chunk
        .first()
        .map(|row| super::eval::PlanOrderProgram::compile_once(row, order_by));
    let mut charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    // The coordinator reserves rows and key slots. Account for the stable
    // sort scratch buffer and actual key heap allocations here, with the
    // same owner retained until the coordinator has consumed these keys.
    charge.add_non_spillable(
        chunk
            .len()
            .saturating_mul(std::mem::size_of::<ParallelSortRow>()),
    )?;
    let fields = super::eval::sort_fields_for_plan_rows(order_by, chunk.first());
    let mut keyed = crate::engine::executor::mem_budget::try_vec_with_capacity(
        chunk.len(),
        "parallel sort keys",
    )?;
    for row in chunk {
        crate::engine::cancel::check_cancelled()?;
        let keys = match program.as_ref() {
            Some(program) => program.eval_keys_inline(&row)?,
            None => smallvec::SmallVec::new(),
        };
        let heap_bytes = keys
            .iter()
            .map(|value| {
                crate::engine::executor::mem_budget::estimate_value_bytes(value)
                    .saturating_sub(std::mem::size_of::<Value>())
            })
            .sum::<usize>();
        charge.add_non_spillable(heap_bytes)?;
        let encoded_single_int = match keys.as_slice() {
            [Value::Int(value)] => Some(crate::engine::executor::sort::encode_i64_for_sort(
                *value,
                order_by[0].asc,
            )),
            _ => None,
        };
        keyed.push(ParallelSortRow {
            keys,
            encoded_single_int,
            row,
        });
    }
    super::eval::ORDER_BY_KEY_ROWS_PREPARED
        .fetch_add(keyed.len() as u64, std::sync::atomic::Ordering::Relaxed);
    keyed.sort_by(
        |left, right| match (left.encoded_single_int, right.encoded_single_int) {
            (Some(left), Some(right)) => left.cmp(&right),
            _ => crate::engine::executor::sort::compare_sort_keys(&fields, &left.keys, &right.keys),
        },
    );
    Ok(PreparedParallelSortChunk {
        rows: keyed,
        charge,
    })
}

/// Parallel TopK: mirror of `execute_parallel_sort` but each worker
/// retains only its local top-`k` via `BoundedTopK`, capping per-worker
/// CPU at O((N/P) log k) and per-worker memory at O(k). The leader then
/// merges up to `P * k` rows down to the global top-`k`.
///
/// `k` here matches the planner's `TopK.k` — `offset + limit`. The
/// downstream `Limit` strips the first `offset` rows. We never emit
/// more than `k`.
///
/// Falls back to the serial `execute_topk` when the worker count or
/// row count is too low to justify the chunking overhead.
pub(super) fn execute_parallel_topk(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    order_by: &[OrderBy],
    k: usize,
    degree: u32,
) -> Result<Vec<PlanRow>, QueryError> {
    if k == 0 {
        let _ = super::plan::execute_plan_rows(db, txn, input)?;
        return Ok(Vec::new());
    }
    let rows = super::plan::execute_plan_rows(db, txn, input)?;
    let p = degree.max(1) as usize;
    // Heuristics: below this, the serial path's allocation savings
    // outweigh any parallelism win. We keep the serial path also for
    // small inputs where a fan-out + merge would dominate.
    if p <= 1 || rows.len() < 2 * p || rows.len() <= k.saturating_mul(2) {
        return execute_topk_serial_from_rows(rows, order_by, k);
    }

    // Equal-size chunks; identical mechanics to execute_parallel_sort.
    // Uses split_off (tail-first) to avoid drain's O(N) memmove per
    // chunk — see comment in execute_parallel_sort for the bookkeeping.
    let fields = super::eval::sort_fields_for_plan_rows(order_by, rows.first());
    let total = rows.len();
    let chunk_size = total.div_ceil(p);
    let chunks = split_off_into_chunks(rows, chunk_size);
    PARALLEL_SORT_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
    PARALLEL_WORKERS_SPAWNED.fetch_add(chunks.len() as u64, AtomicOrdering::Relaxed);
    let _ = total;

    let execution_context = super::execution_context::ExecutionContext::capture(txn);

    let order_by_ref = order_by;
    let fields_ref = &fields;
    let mut per_worker: Vec<
        Result<crate::engine::executor::sort::TopKCandidates<PlanRow>, QueryError>,
    > = (0..chunks.len())
        .map(|_| {
            Err(error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "TopK worker not started",
            ))
        })
        .collect();

    std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            let worker_context = execution_context.clone();
            let db_ref = db;

            handles.push(
                s.spawn(move || -> Result<crate::engine::executor::sort::TopKCandidates<PlanRow>, QueryError> {

                    let do_topk = || -> Result<crate::engine::executor::sort::TopKCandidates<PlanRow>, QueryError> {
                        worker_context.run(db_ref, || worker_local_topk(chunk, order_by_ref, fields_ref, k))
                    };
                    do_topk()
                }),
            );
        }
        for (i, h) in handles.into_iter().enumerate() {
            per_worker[i] = join_worker(h, "parallel top-k worker panicked");
        }
    });

    // Propagate first error, then merge: each worker returned a sorted
    // (keys, row) vector of length ≤ k. The simplest merge is to
    // concatenate (total ≤ P * k) and reduce to top-K via the same
    // `BoundedTopK`. For typical P ≤ 4 and k ≤ 10k, P*k stays small
    // enough that a final `sort_unstable_by` is fine; we use the bounded
    // heap to also enforce the `k` cap on output.
    let mut topk = crate::engine::executor::sort::BoundedTopK::new(&fields, k)?;
    for chunk_res in per_worker {
        let chunk = chunk_res?;
        for candidate in chunk {
            topk.push_candidate(candidate)?;
        }
    }
    Ok(topk.into_sorted_vec()?)
}

fn accumulate_topk_rows(
    chunk: Vec<PlanRow>,
    order_by: &[OrderBy],
    fields: &[crate::engine::executor::sort::SortField],
    k: usize,
) -> Result<crate::engine::executor::sort::BoundedTopK<PlanRow>, QueryError> {
    let mut topk = crate::engine::executor::sort::BoundedTopK::new(fields, k)?;
    let mut scratch: Vec<Value> = Vec::with_capacity(order_by.len());
    let mut prepared_rows = 0u64;
    let program = chunk
        .first()
        .map(|row| super::eval::PlanOrderProgram::compile_once(row, order_by));
    for row in chunk {
        crate::engine::cancel::check_cancelled()?;
        scratch.clear();
        if order_by.len() == 1 {
            scratch.push(
                program
                    .as_ref()
                    .expect("non-empty chunk has an order program")
                    .eval_single_key(&row)?,
            );
        } else {
            scratch.extend(
                program
                    .as_ref()
                    .expect("non-empty chunk has an order program")
                    .eval_keys(&row)?,
            );
        }
        prepared_rows = prepared_rows.saturating_add(1);
        if topk.would_accept(&scratch) {
            topk.push(row, scratch.clone())?;
        }
    }
    super::eval::ORDER_BY_KEY_ROWS_PREPARED
        .fetch_add(prepared_rows, std::sync::atomic::Ordering::Relaxed);
    Ok(topk)
}

fn worker_local_topk(
    chunk: Vec<PlanRow>,
    order_by: &[OrderBy],
    fields: &[crate::engine::executor::sort::SortField],
    k: usize,
) -> Result<crate::engine::executor::sort::TopKCandidates<PlanRow>, QueryError> {
    Ok(accumulate_topk_rows(chunk, order_by, fields, k)?.drain_sorted())
}

/// Serial fallback over rows already materialised by the input plan.
fn execute_topk_serial_from_rows(
    rows: Vec<PlanRow>,
    order_by: &[OrderBy],
    k: usize,
) -> Result<Vec<PlanRow>, QueryError> {
    if k == 0 {
        return Ok(Vec::new());
    }
    let fields = super::eval::sort_fields_for_plan_rows(order_by, rows.first());
    Ok(accumulate_topk_rows(rows, order_by, &fields, k)?.into_sorted_vec()?)
}

/// Stable k-way merge of pre-sorted `Vec<PlanRow>` runs. Switched from
/// a `BinaryHeap<Reverse<…>>` to a loser tree (`super::loser_tree`):
/// per output row the BinaryHeap path paid `2 log m` comparisons + an
/// equivalent number of element-sized swaps (each entry carries a
/// ~50-byte `(keys, PlanRow, ptr)` triple); the loser tree path pays
/// `log m` comparisons and zero payload swaps. The tie-break that
/// used to live in `HeapEntry::cmp` (`then(run.cmp)`) is provided by
/// `LoserTree`'s implicit smaller-leaf-wins rule.
fn k_way_merge_sorted(
    runs: Vec<Vec<ParallelSortRow>>,
    order_by: &[OrderBy],
) -> Result<Vec<PlanRow>, QueryError> {
    if order_by.is_empty() {
        return Ok(runs.into_iter().flatten().map(|entry| entry.row).collect());
    }
    if runs.len() == 1 {
        return Ok(runs
            .into_iter()
            .next()
            .unwrap_or_default()
            .into_iter()
            .map(|entry| entry.row)
            .collect());
    }
    let fields = super::eval::sort_fields_for_plan_rows(
        order_by,
        runs.iter()
            .find_map(|run| run.first().map(|entry| &entry.row)),
    );
    let total: usize = runs.iter().map(|r| r.len()).sum();
    let mut iters: Vec<std::vec::IntoIter<ParallelSortRow>> =
        runs.into_iter().map(|r| r.into_iter()).collect();

    // Thin Ord wrapper over the precomputed key vector. Borrows the
    // `&[OrderBy]` via a raw pointer the same way the legacy
    // `HeapEntry` did — the slice's lifetime trivially outlives this
    // function so no real lifetime escapes the loser tree.
    struct MergeKey {
        keys: smallvec::SmallVec<[Value; 2]>,
        encoded_single_int: Option<u64>,
        order_by_ptr: *const [crate::engine::executor::sort::SortField],
    }
    // SAFETY: order_by_ptr only ever dereferences inside this function
    // body; the slice it points at is alive for the entire call.
    unsafe impl Send for MergeKey {}
    impl PartialEq for MergeKey {
        fn eq(&self, other: &Self) -> bool {
            self.cmp(other) == std::cmp::Ordering::Equal
        }
    }
    impl Eq for MergeKey {}
    impl PartialOrd for MergeKey {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for MergeKey {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            if let (Some(left), Some(right)) = (self.encoded_single_int, other.encoded_single_int) {
                return left.cmp(&right);
            }
            let ob: &[crate::engine::executor::sort::SortField] = unsafe { &*self.order_by_ptr };
            crate::engine::executor::sort::compare_sort_keys(ob, &self.keys, &other.keys)
        }
    }

    let order_by_slice_ptr: *const [crate::engine::executor::sort::SortField] = fields.as_slice();
    let mut initial_heads: Vec<Option<(MergeKey, PlanRow)>> = Vec::with_capacity(iters.len());
    for iter in &mut iters {
        let head = match iter.next() {
            Some(entry) => Some((
                MergeKey {
                    keys: entry.keys,
                    encoded_single_int: entry.encoded_single_int,
                    order_by_ptr: order_by_slice_ptr,
                },
                entry.row,
            )),
            None => None,
        };
        initial_heads.push(head);
    }

    let mut tree = super::loser_tree::LoserTree::new(initial_heads);
    let mut out: Vec<PlanRow> = Vec::with_capacity(total);
    while let Some((_, row, leaf)) = tree.pop_winner() {
        out.push(row);
        let next = match iters[leaf].next() {
            Some(entry) => Some((
                MergeKey {
                    keys: entry.keys,
                    encoded_single_int: entry.encoded_single_int,
                    order_by_ptr: order_by_slice_ptr,
                },
                entry.row,
            )),
            None => None,
        };
        tree.refill(leaf, next);
    }
    Ok(out)
}

/// Drive a `Exchange` node. The `Sorted` output_order does a sort over
/// the unioned per-worker outputs on `merge_keys`. (When upstream is a
/// `ParallelSort` we already get sorted per-worker chunks but we still
/// re-sort defensively because individual workers may have produced
/// rows in arbitrary order.)
pub(super) fn execute_exchange(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    _degree: u32,
    output_order: ExchangeOrder,
    merge_keys: &[OrderBy],
) -> Result<Vec<PlanRow>, QueryError> {
    EXCHANGE_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
    let mut rows = super::plan::execute_plan_rows(db, txn, input)?;
    if matches!(output_order, ExchangeOrder::Sorted) && !merge_keys.is_empty() {
        if matches!(input, PhysicalPlan::ParallelSort { order_by, .. } if order_by == merge_keys) {
            return Ok(rows);
        }
        super::eval::sort_plan_rows_by_order(&mut rows, merge_keys)?;
    }
    Ok(rows)
}

pub(super) fn execute_exchange_limited(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    degree: u32,
    output_order: ExchangeOrder,
    merge_keys: &[OrderBy],
    max_rows: usize,
) -> Result<Vec<PlanRow>, QueryError> {
    if max_rows == 0 {
        EXCHANGE_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
        return Ok(Vec::new());
    }
    if matches!(output_order, ExchangeOrder::Sorted) || !merge_keys.is_empty() {
        let mut rows = execute_exchange(db, txn, input, degree, output_order, merge_keys)?;
        rows.truncate(max_rows);
        return Ok(rows);
    }
    let scan = match input {
        PhysicalPlan::ParallelSeqScan {
            table,
            filter,
            runtime_filters,
            projected_columns,
            degree,
            ..
        } => Some((
            table,
            filter,
            runtime_filters,
            projected_columns,
            degree,
            false,
        )),
        PhysicalPlan::ParallelZoneMapScan {
            table,
            filter,
            runtime_filters,
            projected_columns,
            degree,
            ..
        } => Some((
            table,
            filter,
            runtime_filters,
            projected_columns,
            degree,
            true,
        )),
        _ => None,
    };
    if let Some((table, filter, runtime_filters, projected_columns, degree, use_zone_map)) = scan {
        EXCHANGE_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
        let start = super::analyze_stats::is_recording().then(std::time::Instant::now);
        let io_start = start.map(|_| db.buffer_stats_counts());
        let rows = execute_parallel_seq_scan_limited(
            db,
            txn,
            table,
            filter.as_ref(),
            runtime_filters,
            projected_columns.as_deref().map(|v| v.as_slice()),
            *degree,
            max_rows,
            use_zone_map,
        )?;
        if let Some(start) = start {
            super::analyze_stats::record_with_io_snapshot(
                db,
                input,
                start.elapsed().as_nanos(),
                rows.len() as u64,
                io_start.unwrap(),
            );
        }
        return Ok(rows);
    }
    let mut rows = execute_exchange(db, txn, input, degree, output_order, merge_keys)?;
    rows.truncate(max_rows);
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parallel_sort_key_charge_survives_worker_and_releases_on_error() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        let budget = Arc::new(MemBudget::new(1024 * 1024));
        let row = || {
            PlanRow::from_fields(vec![super::super::plan::PlanField::new(
                "",
                None,
                "s",
                crate::types::ColumnType::VarString,
                Value::String("x".repeat(4096)),
            )])
        };
        let order = vec![OrderBy {
            expr: Expr::Identifier("s".into()),
            asc: true,
            nulls_first: None,
        }];
        let chunk = std::thread::scope(|scope| {
            let budget = budget.clone();
            let order = &order;
            scope
                .spawn(move || {
                    with_query_budget(budget, || prepare_parallel_sort_chunk(vec![row()], order))
                })
                .join()
                .unwrap()
                .unwrap()
        });
        assert!(budget.used() >= 4096);
        drop(chunk);
        assert_eq!(budget.used(), 0);
        let tiny = Arc::new(MemBudget::new(1024));
        assert!(
            with_query_budget(tiny.clone(), || prepare_parallel_sort_chunk(
                vec![row()],
                &order
            ))
            .is_err()
        );
        assert_eq!(tiny.used(), 0);
    }

    #[test]
    fn page_partition_distributes_remainder() {
        // 10 pages over 4 workers => sizes 3,3,2,2
        let parts = page_partition(10, 4);
        assert_eq!(parts, vec![(0, 3), (3, 6), (6, 8), (8, 10)]);
    }

    #[test]
    fn split_off_into_chunks_preserves_order_and_partitions_evenly() {
        let rows: Vec<i32> = (0..10).collect();
        let chunks = split_off_into_chunks(rows, 3);
        // Expect: [[0,1,2], [3,4,5], [6,7,8], [9]] — source-order
        // preserved despite tail-first slicing.
        assert_eq!(
            chunks,
            vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7, 8], vec![9],]
        );
    }

    #[test]
    fn split_off_into_chunks_exact_multiple_has_no_trailing_short_chunk() {
        let rows: Vec<i32> = (0..12).collect();
        let chunks = split_off_into_chunks(rows, 3);
        assert_eq!(chunks.len(), 4);
        for c in &chunks {
            assert_eq!(c.len(), 3);
        }
    }

    #[test]
    fn split_off_into_chunks_handles_empty_and_chunk_larger_than_input() {
        assert!(split_off_into_chunks::<i32>(Vec::new(), 4).is_empty());
        let chunks = split_off_into_chunks(vec![1i32, 2, 3], 100);
        assert_eq!(chunks, vec![vec![1, 2, 3]]);
    }

    #[test]
    fn split_off_into_chunks_chunk_size_zero_returns_empty() {
        // Defensive: chunk_size == 0 is a caller bug; we choose to
        // return Vec::new() rather than loop forever.
        assert!(split_off_into_chunks(vec![1i32, 2, 3], 0).is_empty());
    }

    #[test]
    fn page_partition_zero_pages() {
        let parts = page_partition(0, 4);
        assert_eq!(parts, vec![(0, 0); 4]);
    }

    #[test]
    fn page_partition_single_worker_returns_full_range() {
        let parts = page_partition(7, 1);
        assert_eq!(parts, vec![(0, 7)]);
    }

    #[test]
    fn page_partition_more_workers_than_pages_yields_empty_tail() {
        let parts = page_partition(2, 4);
        // 2 / 4 = 0 chunk, rem 2 -> first two get 1 each, rest empty
        assert_eq!(parts, vec![(0, 1), (1, 2), (2, 2), (2, 2)]);
    }

    #[test]
    fn effective_degree_caps_at_page_count() {
        assert_eq!(effective_degree_for_pages(8, 0), 1);
        assert_eq!(effective_degree_for_pages(8, 1), 1);
        assert_eq!(effective_degree_for_pages(8, 5), 5);
        assert_eq!(effective_degree_for_pages(8, 100), 8);
        assert_eq!(effective_degree_for_pages(0, 100), 1);
    }
}

/// Join every worker at the call site, retaining task order and the original
/// per-operator error message. A panic is a query error, not a scope panic.
fn join_worker<T>(
    handle: std::thread::ScopedJoinHandle<'_, Result<T, QueryError>>,
    panic_message: &'static str,
) -> Result<T, QueryError> {
    handle.join().unwrap_or_else(|_| {
        PARALLEL_WORKER_PANIC_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
        Err(error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            panic_message,
        ))
    })
}

/// Preserve partition order and join every worker before propagating an error.
/// Returning early while handles remain would let scope turn a later worker
/// panic into a caller panic instead of the established QueryError.
fn collect_scan_workers(
    handles: Vec<std::thread::ScopedJoinHandle<'_, Result<Vec<PlanRow>, QueryError>>>,
    panic_message: &'static str,
) -> Result<Vec<PlanRow>, QueryError> {
    let results: Vec<_> = handles
        .into_iter()
        .map(|handle| join_worker(handle, panic_message))
        .collect();
    let mut rows = Vec::new();
    for result in results {
        rows.extend(result?);
    }
    Ok(rows)
}

#[cfg(test)]
mod scan_worker_completion_tests {
    use super::*;

    #[test]
    fn scan_worker_errors_join_later_panics_and_preserve_first_error() {
        let finished = AtomicUsize::new(0);
        let result = std::thread::scope(|scope| {
            let handles = vec![
                scope.spawn(|| {
                    Err(error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "first error",
                    ))
                }),
                scope.spawn(|| panic!("later worker panic")),
                scope.spawn(|| {
                    finished.fetch_add(1, AtomicOrdering::Relaxed);
                    Ok(Vec::new())
                }),
            ];
            collect_scan_workers(handles, "scan panic")
        });
        assert_eq!(result.unwrap_err().message(), "first error");
        assert_eq!(finished.load(AtomicOrdering::Relaxed), 1);
        let result = std::thread::scope(|scope| {
            collect_scan_workers(
                vec![scope.spawn(|| panic!("first worker panic"))],
                "scan panic",
            )
        });
        assert_eq!(result.unwrap_err().message(), "scan panic");
    }
}
