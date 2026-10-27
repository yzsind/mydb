use super::plan::{empty_result_columns, execute_plan_stream, ColMeta, PlanRow};
use super::*;
use crate::planner::RuntimeFilter;

use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;

static ACTIVE_STREAM_WORKERS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
thread_local! {
    /// Nesting depth on an OS thread currently producing a channel-backed
    /// query result. A physical child SELECT consumed by that producer must
    /// stay synchronous: putting another producer onto the same bounded pool
    /// can starve forever when the memory target permits only one worker.
    static ACTIVE_STREAM_WORKER_DEPTH: std::cell::Cell<u32> = const {
        std::cell::Cell::new(0)
    };
}

pub(in crate::engine::executor) struct ActiveStreamWorker;

impl ActiveStreamWorker {
    pub(in crate::engine::executor) fn enter() -> Self {
        ACTIVE_STREAM_WORKERS.fetch_add(1, Ordering::Relaxed);
        ACTIVE_STREAM_WORKER_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
        Self
    }
}

impl Drop for ActiveStreamWorker {
    fn drop(&mut self) {
        ACTIVE_STREAM_WORKER_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
        ACTIVE_STREAM_WORKERS.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(super) fn on_stream_worker() -> bool {
    ACTIVE_STREAM_WORKER_DEPTH.with(|depth| depth.get() != 0)
}

pub(crate) fn active_stream_workers() -> u64 {
    ACTIVE_STREAM_WORKERS.load(Ordering::Relaxed)
}

/// Per-worker row batch sent over the gather channel. Sized so atomic
/// channel ops are noise vs. the work needed to produce a batch.
const GATHER_BATCH_ROWS: usize = 1024;

/// Default per-batch row count for the streaming channels used by
/// `PlanStreamCursor`. Row-level mpsc would
/// take/release the channel mutex once per row — at multi-million-row
/// throughput, that mutex starts to dominate the streaming SELECT
/// per-row cost. Sending in batches of `STREAM_BATCH_ROWS` cuts the
/// number of channel ops by `STREAM_BATCH_ROWS`× without giving up
/// early-row latency for any reasonable client.
///
/// Overridable via `MYDB_STREAM_BATCH_ROWS` so wide-row workloads can
/// shrink the batch to keep the in-flight memory bounded.
/// Default depth of the streaming channel (number of in-flight
/// batches). Combined with the statement snapshot's stream batch size this caps
/// in-flight rows at 4 × 64 = 256, matching the old row-level
/// `STREAM_CHANNEL_ROWS = 256` so memory headroom is the same. The
/// difference is that mpsc ops happen at most 4 times per result-set
/// fill, not 256 times.
///
/// Overridable via `MYDB_STREAM_CHANNEL_BATCHES`.
pub(super) fn stream_batch_rows() -> usize {
    crate::engine::executor::tuning::current_or_capture().stream_batch_rows
}

pub(super) fn stream_channel_batches() -> usize {
    crate::engine::executor::tuning::current_or_capture().stream_channel_batches
}

pub(super) fn collect_plan_stream(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
) -> Result<Vec<PlanRow>, QueryError> {
    let mut stream = execute_plan_stream(db, txn, plan)?;
    let mut rows = Vec::new();
    while let Some(row) = stream.next()? {
        rows.push(row);
    }
    Ok(rows)
}

/// A closed result channel is a normal end of stream only when the producer
/// returned. A producer that unwound dropped its sender too, so the consumer
/// joins it and reports the panic instead of returning a truncated result.
fn stream_producer_panicked(detail: &str) -> QueryError {
    crate::error::catalog::ER_INTERNAL_ERROR
        .error(format!("result stream producer panicked: {detail}"))
}

pub(in crate::engine::executor) fn join_stream_thread(
    worker: thread::JoinHandle<()>,
) -> Result<(), QueryError> {
    worker.join().map_err(|payload| {
        let detail = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&'static str>().copied())
            .unwrap_or("unknown panic payload");
        stream_producer_panicked(detail)
    })
}

pub(super) struct ChannelPlanStream {
    rx: Option<mpsc::Receiver<Result<Vec<PlanRow>, QueryError>>>,
    worker: Option<thread::JoinHandle<()>>,
    cancellation: crate::engine::cancel::CancellationSnapshot,
    /// Iterator over the currently-buffered batch. Rows are popped
    /// from here until empty; then we pull the next batch from `rx`.
    current: std::vec::IntoIter<PlanRow>,
}

impl std::fmt::Debug for ChannelPlanStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelPlanStream")
            .field("worker_active", &self.worker.is_some())
            .finish_non_exhaustive()
    }
}

impl super::plan::PlanStream for ChannelPlanStream {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        // P3-3: drain the current batch first, only touch the mpsc
        // mutex once the batch is exhausted. At default settings the
        // channel cost drops 64× vs. the old row-level shape.
        loop {
            if let Some(row) = self.current.next() {
                return Ok(Some(row));
            }
            match self.rx.as_ref().expect("stream receiver present").recv() {
                Ok(Ok(batch)) => self.current = batch.into_iter(),
                Ok(Err(err)) => return Err(err),
                Err(_) => {
                    if let Some(worker) = self.worker.take() {
                        join_stream_thread(worker)?;
                    }
                    return Ok(None);
                }
            }
        }
    }
}

impl Drop for ChannelPlanStream {
    fn drop(&mut self) {
        self.cancellation
            .finish_channel_worker(&mut self.rx, &mut self.worker);
    }
}

pub(super) fn seq_scan_stream(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: &Option<Expr>,
    runtime_filters: &[RuntimeFilter],
    projected_columns: Option<&[usize]>,
    use_zone_map: bool,
) -> Result<Box<dyn super::plan::PlanStream>, QueryError> {
    if filter
        .as_ref()
        .is_some_and(super::simple::expr_has_subquery)
    {
        return Ok(Box::new(super::plan::MaterializedStream::new(
            super::plan::scan_plan_table(
                db,
                txn,
                table,
                filter,
                runtime_filters,
                projected_columns,
                use_zone_map,
            )?,
        )));
    }

    let batch_rows = stream_batch_rows();
    let channel_batches = stream_channel_batches();
    let (tx, rx) = mpsc::sync_channel::<Result<Vec<PlanRow>, QueryError>>(channel_batches);
    let db = Arc::clone(db);
    let table = table.clone();
    let filter = filter.clone();
    let runtime_filters = runtime_filters.to_vec();
    let projection: Vec<usize> = projected_columns
        .map(|cols| cols.to_vec())
        .unwrap_or_else(|| super::plan::logical_visible_column_indices(&table.schema));
    let candidate_ranges = use_zone_map
        .then(|| filter.as_ref())
        .flatten()
        .and_then(|expr| db.zone_map_candidate_ranges_for_plan(&table.schema, expr));
    let budget = crate::engine::executor::mem_budget::current_budget().unwrap_or_else(|| {
        std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::from_env())
    });
    // Compute partition pruning in the parent (where we hold the
    // table schema + filter) and ship the result to the worker. The
    // worker thread doesn't inherit thread-locals from us, so the
    // installation has to happen inside the spawned closure.
    let filter_pruned: Option<Vec<u64>> = table.resolved_partition_storage_ids(filter.as_ref());
    let inherited_pruned = crate::engine::executor::partition_router::current_pruned_partitions();
    let pruned_partitions = match (inherited_pruned, filter_pruned) {
        (Some(inherited), Some(filtered)) => Some(
            inherited
                .into_iter()
                .filter(|id| filtered.contains(id))
                .collect(),
        ),
        (Some(inherited), None) => Some(inherited),
        (None, filtered) => filtered,
    };
    let execution_context =
        super::execution_context::ExecutionContext::capture_with_budget(txn, budget)
            .with_pruned_partitions(pruned_partitions)
            .fork_worker_cancellation();
    let cancellation = execution_context.cancellation().clone();

    let worker = thread::spawn(move || {
        let _active_worker = ActiveStreamWorker::enter();
        let mut consumer_gone = false;
        let run = || -> Result<(), QueryError> {
            // Pre-intern each projected column's metadata to an
            // `&'static ColMeta` arc so the per-row loop only pays the
            // `Value::clone()` from storage's borrowed slice — no
            // ColMeta re-intern, no double-clone of a `Vec<PlanField>`
            // template.
            let metas: Vec<_> = projection
                .iter()
                .map(|&i| {
                    crate::engine::executor::select::plan::intern_col_meta(ColMeta::from_table(
                        &table, i,
                    ))
                })
                .collect();
            let projected_schema = super::plan::projected_schema_view(&table.schema, &projection);
            let row_schema = super::plan::RowSchema::from_metas(&metas);
            let compiled_filter = filter
                .as_ref()
                .and_then(|expr| super::plan::CompiledPredicate::compile(expr, &projected_schema));
            let outside_latch = (filter.is_some()
                && !compiled_filter
                    .as_ref()
                    .is_some_and(super::plan::CompiledPredicate::page_latch_safe))
                || runtime_filters.iter().any(|rf| {
                    super::plan::compile_runtime_filter_probe_projected(
                        rf,
                        &table.schema,
                        &projection,
                    )
                    .is_none()
                });
            let mut eval_ctx = EvalQueryContext::default();
            // Batch accumulator — flushed to `tx` every `batch_rows`
            // rows so the channel mutex is taken once per batch, not
            // once per row.
            let mut batch: Vec<PlanRow> = Vec::with_capacity(batch_rows);
            let visit = |values: &[Value]| {
                crate::engine::cancel::check_cancelled()?;
                if runtime_filters.is_empty() {
                    if let Some(compiled) = compiled_filter.as_ref() {
                        if !compiled.eval(values)? {
                            return Ok(());
                        }
                    }
                }
                // Single-pass construction: zip the pre-interned metas
                // with the borrowed values. The previous shape was
                // `templates.clone()` (N PlanField shallow-clones with
                // Value::Null placeholders) followed by an in-place
                // `field.value = values[i].clone()`. That copied the
                // template's Null/Arc<ColMeta> slots once and then
                // overwrote them — pure waste. Going straight from
                // (meta, value) skips the placeholder pass.
                let fields: Vec<PlanField> = metas
                    .iter()
                    .zip(values.iter())
                    .map(|(meta, value)| PlanField {
                        meta: meta.clone(),
                        value: value.clone(),
                    })
                    .collect();
                let row = PlanRow::builder(fields).schema(row_schema.clone()).build();
                if !super::plan::runtime_filters_admit(&runtime_filters, &row)? {
                    super::plan::HASH_JOIN_BLOOM_PROBE_FILTERED.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                let keep = match (&filter, compiled_filter.as_ref()) {
                    (Some(_), Some(compiled)) => {
                        runtime_filters.is_empty() || compiled.eval(values)?
                    }
                    (Some(expr), None) => {
                        eval_plan_bool_with_db(&db, txn, expr, &row, &mut eval_ctx)?
                    }
                    (None, _) => true,
                };
                if keep {
                    batch.push(row);
                    if batch.len() >= batch_rows {
                        let chunk = std::mem::replace(&mut batch, Vec::with_capacity(batch_rows));
                        if tx.send(Ok(chunk)).is_err() {
                            consumer_gone = true;
                            return Err(error(
                                crate::error::catalog::ER_QUERY_INTERRUPTED,
                                "stream consumer closed",
                            ));
                        }
                    }
                }
                Ok(())
            };
            super::plan::scan_projected_table(
                &db,
                &table.schema,
                txn,
                &projection,
                filter.as_ref().filter(|_| use_zone_map),
                candidate_ranges.as_deref(),
                outside_latch,
                visit,
            )?;
            // Final flush — batch may carry a tail < batch_rows.
            if !batch.is_empty() && !consumer_gone {
                let chunk = std::mem::take(&mut batch);
                if tx.send(Ok(chunk)).is_err() {
                    consumer_gone = true;
                    return Err(error(
                        crate::error::catalog::ER_QUERY_INTERRUPTED,
                        "stream consumer closed",
                    ));
                }
            }
            Ok(())
        };
        let result = execution_context.run(&db, run);
        if let Err(err) = result {
            let err = execution_context.terminal_error(err);
            if !consumer_gone {
                let _ = tx.send(Err(err));
            }
        }
    });

    Ok(Box::new(ChannelPlanStream {
        rx: Some(rx),
        worker: Some(worker),
        cancellation,
        current: Vec::new().into_iter(),
    }))
}

pub(super) struct PlanStreamCursor {
    /// Channel carries *batches* of rows (`Vec<Vec<Value>>`) instead of
    /// individual rows. With the default settings (64 rows / batch,
    /// 4 in-flight batches) the worker thread touches the mpsc mutex
    /// once per 64 rows instead of once per row — the row-level shape
    /// became visible as a hotspot on multi-million-row streaming
    /// SELECTs.
    rx: Option<mpsc::Receiver<Result<Vec<Vec<Value>>, QueryError>>>,
    worker: Option<crate::util::blocking_pool::BlockingJoinHandle>,
    cancellation: crate::engine::cancel::CancellationSnapshot,
    budget: Arc<crate::engine::executor::mem_budget::MemBudget>,
    /// In-progress batch the cursor is currently draining. Rows are
    /// popped from here one at a time so `ResultCursor::next_row`
    /// stays row-shaped from the protocol layer's perspective.
    current: std::vec::IntoIter<Vec<Value>>,
    warning_sink: crate::engine::api::StatementWarningSink,
    /// Keep a grouping phase active for the complete lazy cursor lifetime.
    /// Creating it on the connection thread avoids a gap before the worker
    /// starts and lets the fixed stage counters compose with protocol writes.
    _grouping_stage: Option<crate::engine::session_registry::ProcessStageGuard>,
}

impl std::fmt::Debug for PlanStreamCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlanStreamCursor")
            .field("worker_active", &self.worker.is_some())
            .finish_non_exhaustive()
    }
}

/// Normalize executor-native values at the final SELECT boundary.
///
/// The batch graph deliberately keeps native YEAR values and unconstrained
/// DECIMAL/FLOAT lanes while operators compose.  Top-level SQL expressions,
/// however, expose MySQL's resolved result shape: MIN/MAX(YEAR) is returned as
/// the canonical integer year, aggregate DECIMALs use their declared scale,
/// and FLOAT results retain the single-precision lane.  Both the streaming
/// cursor and the synchronously drained bounded-result path must pass through
/// this exact boundary so response scheduling cannot change SQL values.
pub(super) struct SelectResultNormalizer {
    adjustments: smallvec::SmallVec<[ResultColumnAdjustment; 4]>,
}

struct ResultColumnAdjustment {
    index: usize,
    min_max: bool,
    decimal_scale: Option<u8>,
    single_precision: bool,
}

impl SelectResultNormalizer {
    pub(super) fn new(
        columns: &[crate::types::Column],
        projection: &[crate::parser::SelectItem],
    ) -> Self {
        let mut adjustments = smallvec::SmallVec::new();
        for (index, (column, projection)) in columns.iter().zip(projection).enumerate() {
            let (min_max, aggregate) = match projection {
                crate::parser::SelectItem::Expr {
                    expr: crate::parser::Expr::Function { name, .. },
                    ..
                } => {
                    let min_max =
                        name.eq_ignore_ascii_case("MIN") || name.eq_ignore_ascii_case("MAX");
                    (
                        min_max,
                        min_max
                            || name.eq_ignore_ascii_case("SUM")
                            || name.eq_ignore_ascii_case("AVG"),
                    )
                }
                _ => (false, false),
            };
            let decimal_scale = match column.column_type {
                crate::types::ColumnType::Decimal { scale, .. } if aggregate => Some(scale),
                _ => None,
            };
            let single_precision = column.column_type == crate::types::ColumnType::Float;
            if min_max || decimal_scale.is_some() || single_precision {
                adjustments.push(ResultColumnAdjustment {
                    index,
                    min_max,
                    decimal_scale,
                    single_precision,
                });
            }
        }
        Self { adjustments }
    }

    pub(super) fn apply(&self, row: &mut [Value]) {
        // This flag belongs to the current execution context, so read it at
        // the row boundary rather than capturing it while preparing metadata.
        if !crate::engine::executor::cte::cte_materialization_active() {
            for value in row.iter_mut() {
                if let Value::SmallString(bytes) = value {
                    if !bytes.is_empty() {
                        // SmallString's type invariant guarantees valid UTF-8.
                        let text =
                            unsafe { std::str::from_utf8_unchecked(bytes.as_slice()) }.to_owned();
                        *value = Value::String(text);
                    }
                }
            }
        }
        for adjustment in &self.adjustments {
            let Some(value) = row.get_mut(adjustment.index) else {
                continue;
            };
            if adjustment.min_max {
                if let Value::Year(year) = value {
                    let year = *year;
                    *value = Value::Int(if year == 0 { 0 } else { 1900 + i64::from(year) });
                }
            }
            if let Some(scale) = adjustment.decimal_scale {
                *value = super::vec_bridge::decimal_value_at_scale(
                    std::mem::replace(value, Value::Null),
                    scale,
                );
            }
            if adjustment.single_precision {
                if let Value::Double(number) = value {
                    *value = Value::Float(*number as f32);
                }
            }
        }
    }
}

impl PlanStreamCursor {
    pub(super) fn spawn(
        db: Arc<Database>,
        stmt: Select,
        plan: Arc<PhysicalPlan>,
        execution_context: super::execution_context::ExecutionContext,
        result_columns: Vec<crate::types::Column>,
    ) -> Self {
        let grouping_stage = (!stmt.group_by.is_empty()).then(|| {
            crate::engine::cancel::enter_process_stage(
                crate::engine::session_registry::ProcessState::SortingForGroup,
            )
        });
        let grouping_stage = grouping_stage.flatten();
        let execution_context = execution_context
            .fork_worker_cancellation()
            .fork_cursor_warnings();
        let txn = execution_context.txn();
        let batch_rows = stream_batch_rows();
        let channel_batches = stream_channel_batches();
        let (tx, rx) = mpsc::sync_channel::<Result<Vec<Vec<Value>>, QueryError>>(channel_batches);
        let cancellation = execution_context.cancellation().clone();
        let query_budget = Arc::clone(execution_context.budget());
        let warning_sink = execution_context.warning_sink().clone();
        // Register before spawning so the caller's statement guard and the
        // worker guard overlap: there is no hand-off gap in which purge can
        // reclaim undo needed by the fixed `txn` snapshot.
        let snapshot_guard = db.register_statement_snapshot(txn.snapshot_scn);
        // The producer owns non-Send operator state for its complete lifetime,
        // but it does not need a brand-new OS thread. Running the closure as
        // one indivisible task on the process blocking pool preserves that
        // thread affinity, the bounded producer/consumer channel, and the
        // synchronous Drop join while amortizing worker creation across
        // statements.
        let worker = crate::util::blocking_pool::spawn_joinable(move || {
            let _snapshot_guard = snapshot_guard;
            let _active_worker = ActiveStreamWorker::enter();
            // `RAND(N)` state is thread-local because scalar expression
            // evaluation is thread-affine, but blocking-pool threads are
            // reused by unrelated statements. The dispatcher resets its own
            // thread before spawning us; reset the actual producer thread as
            // well so a cached expression occurrence cannot continue the LCG
            // sequence left by a previous statement that happened to run on
            // this worker.
            crate::engine::expr::clear_statement_rand_state();
            let result = execution_context.run(&db, || -> Result<(), QueryError> {
                let warning_checkpoint = crate::engine::api::statement_warning_count();
                let group_concat_slots = super::plan::group_concat_slots_per_output_group(&plan);
                let mut emitted_rows = 0usize;
                let normalizer = SelectResultNormalizer::new(&result_columns, &stmt.projection);
                let normalize_result_row = |mut row: Vec<Value>| {
                    normalizer.apply(&mut row);
                    row
                };
                if let Some(mut input) = super::vec_bridge::try_native_result_batch_input(
                    &db,
                    txn,
                    &plan,
                    &stmt.projection,
                    &super::plan::wildcard_source_order(&stmt),
                )? {
                    let mut output: Vec<Vec<Value>> = Vec::with_capacity(batch_rows);
                    let mut scalar_eval_ctx = super::eval::EvalQueryContext::default();
                    while let Some(batch) = input.stream.next_batch()? {
                        crate::engine::cancel::check_cancelled()?;
                        let (computed, _retained) = if input.row_major_projection {
                            super::vec_bridge::precompute_read_only_projection(
                                &input.projection,
                                &batch,
                            )
                        } else {
                            let columns = input
                                .projection
                                .iter()
                                .map(|projection| match projection {
                                    super::vec_bridge::VecResultProjection::Column(_) => Ok(None),
                                    super::vec_bridge::VecResultProjection::Expr(program) => {
                                        crate::engine::executor::vec::expr_batch::eval_to_column(
                                            program, &batch,
                                        )
                                        .map(Some)
                                    }
                                    super::vec_bridge::VecResultProjection::Scalar(_) => {
                                        unreachable!(
                                            "scalar projection must use row-major evaluation"
                                        )
                                    }
                                })
                                .collect::<Result<_, QueryError>>()?;
                            (columns, batch.memory_charge())
                        };
                        let needs_row = input.row_major_projection
                            && input
                                .projection
                                .iter()
                                .enumerate()
                                .any(|(index, projection)| {
                                    matches!(
                                        projection,
                                        super::vec_bridge::VecResultProjection::Scalar(_)
                                    ) || matches!(
                                        projection,
                                        super::vec_bridge::VecResultProjection::Expr(_)
                                    ) && computed.get(index).and_then(Option::as_ref).is_none()
                                });
                        let mut row_values = Vec::with_capacity(batch.columns().len());
                        let mut emit = |row: usize| -> Result<bool, QueryError> {
                            let projected = if input.row_major_projection {
                                if needs_row {
                                    row_values.clear();
                                    row_values.extend(
                                        batch.columns().iter().zip(batch.schema().fields()).map(
                                            |(column, field)| {
                                                column.typed_value_at(row, field.column_type)
                                            },
                                        ),
                                    );
                                }
                                let view =
                                    crate::engine::expr_program::RowView::from_values(&row_values);
                                input
                                    .projection
                                    .iter()
                                    .enumerate()
                                    .map(|(index, projection)| match projection {
                                        super::vec_bridge::VecResultProjection::Column(column) => {
                                            Ok(batch.columns()[*column].value_at(row))
                                        }
                                        super::vec_bridge::VecResultProjection::Expr(program) => {
                                            if let Some(column) =
                                                computed.get(index).and_then(Option::as_ref)
                                            {
                                                Ok(column.value_at(row))
                                            } else {
                                                crate::engine::expr_program::with_eval_scratch(
                                                    |scratch| program.eval_row(&view, scratch),
                                                )
                                            }
                                        }
                                        super::vec_bridge::VecResultProjection::Scalar(expr) => {
                                            let plan_row = super::vec_bridge::values_to_plan_row(
                                                row_values.clone(),
                                                &input.template,
                                            );
                                            super::eval::eval_plan_expr_with_db(
                                                &db,
                                                txn,
                                                expr,
                                                &plan_row,
                                                &mut scalar_eval_ctx,
                                            )
                                        }
                                    })
                                    .collect::<Result<Vec<_>, QueryError>>()?
                            } else {
                                input
                                    .projection
                                    .iter()
                                    .enumerate()
                                    .map(|(index, projection)| match projection {
                                        super::vec_bridge::VecResultProjection::Column(column) => {
                                            batch.columns()[*column].value_at(row)
                                        }
                                        super::vec_bridge::VecResultProjection::Expr(_) => computed
                                            [index]
                                            .as_ref()
                                            .expect("computed batch projection missing")
                                            .value_at(row),
                                        super::vec_bridge::VecResultProjection::Scalar(_) => {
                                            unreachable!(
                                                "scalar projection must use row-major evaluation"
                                            )
                                        }
                                    })
                                    .collect()
                            };
                            output.push(normalize_result_row(projected));
                            emitted_rows = emitted_rows.saturating_add(1);
                            if output.len() >= batch_rows {
                                let chunk =
                                    std::mem::replace(&mut output, Vec::with_capacity(batch_rows));
                                if tx.send(Ok(chunk)).is_err() {
                                    return Ok(false);
                                }
                            }
                            Ok(true)
                        };
                        match batch.selection() {
                            Some(selection) => {
                                for row in selection.iter() {
                                    if !emit(row)? {
                                        return Ok(());
                                    }
                                }
                            }
                            None => {
                                for row in 0..batch.num_rows() {
                                    if !emit(row)? {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }
                    if !output.is_empty() {
                        let _ = tx.send(Ok(output));
                    }
                    super::plan::cap_limited_group_concat_warnings(
                        warning_checkpoint,
                        group_concat_slots,
                        emitted_rows,
                    );
                    return Ok(());
                }

                let mut stream = execute_plan_stream(&db, txn, &plan)?;
                let mut projection_program = None;
                let mut batch: Vec<Vec<Value>> = Vec::with_capacity(batch_rows);
                while let Some(row) = stream.next()? {
                    crate::engine::cancel::check_cancelled()?;
                    let program = projection_program.get_or_insert_with(|| {
                        PlanProjectionProgram::compile_once(&row, &stmt.projection)
                    });
                    let values = normalize_result_row(program.project_row_owned(row)?);
                    batch.push(values);
                    emitted_rows = emitted_rows.saturating_add(1);
                    if batch.len() >= batch_rows {
                        let chunk = std::mem::replace(&mut batch, Vec::with_capacity(batch_rows));
                        if tx.send(Ok(chunk)).is_err() {
                            return Ok(());
                        }
                    }
                }
                if !batch.is_empty() {
                    let _ = tx.send(Ok(std::mem::take(&mut batch)));
                }
                super::plan::cap_limited_group_concat_warnings(
                    warning_checkpoint,
                    group_concat_slots,
                    emitted_rows,
                );
                Ok(())
            });
            if let Err(err) = result {
                let err = execution_context.terminal_error(err);
                let _ = tx.send(Err(err));
            }
        });
        Self {
            rx: Some(rx),
            worker: Some(worker),
            cancellation,
            budget: query_budget,
            current: Vec::new().into_iter(),
            warning_sink,
            _grouping_stage: grouping_stage,
        }
    }
}

impl crate::engine::api::ResultCursor for PlanStreamCursor {
    fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        // P3-3: row-level interface, batch-level transport — pop from
        // the in-progress batch first, only touch the channel when
        // the batch is exhausted.
        loop {
            // The producer latches terminal OOM before cancelling its worker
            // tree. Resolve the primary error only on failure, without adding
            // a budget/TLS lookup to every successfully streamed row.
            if let Err(error) = self.cancellation.check() {
                return Err(self.budget.terminal_oom().unwrap_or(error));
            }
            if let Some(row) = self.current.next() {
                return Ok(Some(row));
            }
            match self.rx.as_ref().expect("stream receiver present").recv() {
                Ok(Ok(batch)) => self.current = batch.into_iter(),
                Ok(Err(err)) => return Err(self.budget.terminal_oom().unwrap_or(err)),
                Err(_) => {
                    if let Some(error) = self.budget.terminal_oom() {
                        return Err(error);
                    }
                    if let Some(worker) = self.worker.take() {
                        match worker.join() {
                            Ok(()) => {}
                            Err(crate::util::blocking_pool::BlockingError::Panicked(detail)) => {
                                return Err(stream_producer_panicked(&detail));
                            }
                            Err(crate::util::blocking_pool::BlockingError::Cancelled) => {
                                return Err(stream_producer_panicked(
                                    "producer task was cancelled",
                                ));
                            }
                        }
                    }
                    return Ok(None);
                }
            }
        }
    }

    fn take_warnings(&mut self) -> crate::engine::api::SqlWarnings {
        let warnings = std::mem::take(&mut *self.warning_sink.lock());
        warnings
    }
}

impl Drop for PlanStreamCursor {
    fn drop(&mut self) {
        if self
            .worker
            .as_ref()
            .is_some_and(|worker| !worker.is_finished())
        {
            self.cancellation.cancel_local();
        }
        drop(self.rx.take());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// Streaming `Exchange { Unordered }` over `ParallelSeqScan`. Each
/// worker reads its page range, accumulates rows into a row batch
/// (size [`GATHER_BATCH_ROWS`]), and sends the batch over a shared
/// `mpsc::sync_channel`. Main thread pops batches and iterates them
/// row-by-row, so an `OFFSET / LIMIT` or `PlanStreamCursor` downstream
/// can stop early without forcing every worker to materialise its
/// full slice — and so cache-friendly batches amortise per-row alloc
/// against many rows of join/agg work upstream.
///
/// `output_order` must be `Unordered`; sorted Exchange still goes
/// through `execute_exchange`'s row-level k-way merge until a
/// streaming merge lands.
pub(super) fn parallel_exchange_unordered_stream(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: &Option<Expr>,
    runtime_filters: &[RuntimeFilter],
    projected_columns: Option<&[usize]>,
    degree: u32,
    use_zone_map: bool,
) -> Result<Box<dyn super::plan::PlanStream>, QueryError> {
    use std::sync::atomic::AtomicBool;

    super::parallel::EXCHANGE_COUNT.fetch_add(1, Ordering::Relaxed);
    // Partition-aware parallel scan: a partitioned table has no
    // single "page range" to slice — each partition has its own
    // page numbering. Dispatch to a dedicated path that distributes
    // partitions across workers when there are at least 2.
    if table.schema.is_partitioned() && !use_zone_map {
        return parallel_exchange_partitioned_stream(
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
    let (tasks, worker_count) =
        super::parallel::scan_morsels(db, table, filter.as_ref(), use_zone_map, degree)?;
    if tasks.is_empty() {
        return Ok(Box::new(super::plan::MaterializedStream::new(Vec::new())));
    }
    if worker_count <= 1 {
        // Degenerate: use the ordinary streaming leaf so LIMIT/cursor
        // cancellation does not pay channel setup for a single worker.
        return seq_scan_stream(
            db,
            txn,
            table,
            filter,
            runtime_filters,
            projected_columns,
            use_zone_map,
        );
    }
    if use_zone_map {
        super::parallel::record_parallel_task_partition_visits(table, &[tasks.clone()]);
    }
    if worker_count > 1 {
        super::parallel::PARALLEL_SEQ_SCAN_COUNT.fetch_add(1, Ordering::Relaxed);
        if use_zone_map {
            super::parallel::PARALLEL_ZONE_MAP_SCAN_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        super::parallel::PARALLEL_WORKERS_SPAWNED.fetch_add(worker_count as u64, Ordering::Relaxed);
    }
    // Channel capacity is per-worker so each producer always has at
    // least one slot of headroom — keeps fast workers from blocking
    // on slow consumer waits when the others are still ramping up.
    let (tx, rx) = mpsc::sync_channel::<Result<Vec<PlanRow>, QueryError>>(worker_count.max(1) * 2);
    let consumer_alive = std::sync::Arc::new(AtomicBool::new(true));
    let mut worker_handles: Vec<thread::JoinHandle<()>> = Vec::with_capacity(worker_count);
    let tasks = Arc::new(tasks);
    let next_task = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let execution_context =
        super::execution_context::ExecutionContext::capture(txn).fork_worker_cancellation();
    let cancellation = execution_context.cancellation().clone();

    for _ in 0..worker_count {
        let tx = tx.clone();
        let alive = consumer_alive.clone();
        let db = Arc::clone(db);
        let table_owned = table.clone();
        let filter_owned = filter.clone();
        let runtime_owned = runtime_filters.to_vec();
        let projection_owned: Option<Vec<usize>> = projected_columns.map(|c| c.to_vec());
        let tasks = Arc::clone(&tasks);
        let next_task = Arc::clone(&next_task);
        let worker_context = execution_context.clone();
        worker_handles.push(thread::spawn(move || {
            let _active_worker = ActiveStreamWorker::enter();
            let mut buf: Vec<PlanRow> = Vec::with_capacity(GATHER_BATCH_ROWS);
            let flush = |buf: &mut Vec<PlanRow>,
                         alive: &std::sync::Arc<AtomicBool>|
             -> Result<(), QueryError> {
                if buf.is_empty() {
                    return Ok(());
                }
                let batch = std::mem::replace(buf, Vec::with_capacity(GATHER_BATCH_ROWS));
                if tx.send(Ok(batch)).is_err() {
                    alive.store(false, Ordering::Relaxed);
                    return Err(error(
                        crate::error::catalog::ER_QUERY_INTERRUPTED,
                        "exchange consumer closed",
                    ));
                }
                Ok(())
            };
            let run = || -> Result<(), QueryError> {
                loop {
                    let task_idx = next_task.fetch_add(1, Ordering::Relaxed);
                    let Some(task) = tasks.get(task_idx) else {
                        break;
                    };
                    let scan = if let Some(projection) = projection_owned.as_deref() {
                        super::parallel::worker_projected_streaming(
                            &db,
                            txn,
                            &table_owned,
                            filter_owned.as_ref(),
                            &runtime_owned,
                            projection,
                            task.storage_id,
                            task.page_lo,
                            task.page_hi,
                            |row| -> Result<bool, QueryError> {
                                if !alive.load(Ordering::Relaxed) {
                                    return Ok(false);
                                }
                                buf.push(row);
                                if buf.len() >= GATHER_BATCH_ROWS {
                                    flush(&mut buf, &alive)?;
                                }
                                Ok(true)
                            },
                        )
                    } else {
                        super::parallel::worker_full_row_streaming(
                            &db,
                            txn,
                            &table_owned,
                            filter_owned.as_ref(),
                            &runtime_owned,
                            task.storage_id,
                            task.page_lo,
                            task.page_hi,
                            |row| -> Result<bool, QueryError> {
                                if !alive.load(Ordering::Relaxed) {
                                    return Ok(false);
                                }
                                buf.push(row);
                                if buf.len() >= GATHER_BATCH_ROWS {
                                    flush(&mut buf, &alive)?;
                                }
                                Ok(true)
                            },
                        )
                    };
                    scan?;
                }
                flush(&mut buf, &alive)?;
                Ok(())
            };
            let result = worker_context.run(&db, run);
            if let Err(err) = result {
                let err = worker_context.terminal_error(err);
                if alive.load(Ordering::Relaxed) {
                    let _ = tx.send(Err(err));
                }
            }
        }));
    }
    drop(tx);

    Ok(Box::new(ParallelExchangeStream {
        rx,
        workers: Some(worker_handles),
        current: Vec::new().into_iter(),
        consumer_alive,
        cancellation,
    }))
}

pub(super) struct ParallelExchangeStream {
    rx: mpsc::Receiver<Result<Vec<PlanRow>, QueryError>>,
    workers: Option<Vec<thread::JoinHandle<()>>>,
    current: std::vec::IntoIter<PlanRow>,
    consumer_alive: std::sync::Arc<std::sync::atomic::AtomicBool>,
    cancellation: crate::engine::cancel::CancellationSnapshot,
}

impl super::plan::PlanStream for ParallelExchangeStream {
    fn next(&mut self) -> Result<Option<PlanRow>, QueryError> {
        loop {
            if let Some(row) = self.current.next() {
                return Ok(Some(row));
            }
            match self.rx.recv() {
                Ok(Ok(batch)) => {
                    self.current = batch.into_iter();
                }
                Ok(Err(err)) => return Err(err),
                Err(_) => {
                    if let Some(handles) = self.workers.take() {
                        let mut first_error = None;
                        for handle in handles {
                            if let Err(error) = join_stream_thread(handle) {
                                first_error.get_or_insert(error);
                            }
                        }
                        if let Some(error) = first_error {
                            return Err(error);
                        }
                    }
                    return Ok(None);
                }
            }
        }
    }
}

impl Drop for ParallelExchangeStream {
    fn drop(&mut self) {
        if self
            .workers
            .as_ref()
            .is_some_and(|workers| workers.iter().any(|worker| !worker.is_finished()))
        {
            self.cancellation.cancel_local();
        }
        // Tell still-running workers we're gone so their `send`
        // returns Err quickly instead of stalling on the channel.
        self.consumer_alive
            .store(false, std::sync::atomic::Ordering::Relaxed);
        // Drain whatever they've already produced and discard so
        // sync_channel slots free up and workers reach their next
        // `send` (which now fails) promptly.
        while let Ok(_) = self.rx.try_recv() {}
        if let Some(handles) = self.workers.take() {
            for h in handles {
                let _ = h.join();
            }
        }
    }
}

/// Partition-aware variant of [`parallel_exchange_unordered_stream`].
/// Distributes the partitioned table's `storage_id`s across
/// `min(degree, partition_count)` workers; each worker scans its
/// assigned partitions sequentially and ships rows back through a
/// shared mpsc channel.
///
/// Workers honour the planner-installed partition prune set (set by
/// `with_pruned_partitions` higher in the stack) by intersecting
/// against it before distributing.
pub(super) fn parallel_exchange_partitioned_stream(
    db: &Arc<Database>,
    txn: TxnView,
    table: &PlanTable,
    filter: &Option<Expr>,
    runtime_filters: &[RuntimeFilter],
    projected_columns: Option<&[usize]>,
    degree: u32,
    use_zone_map: bool,
) -> Result<Box<dyn super::plan::PlanStream>, QueryError> {
    use std::sync::atomic::AtomicBool;

    // Compute the candidate partition set in the parent (so pruning
    // applies) and split across workers.
    let pruned = table.resolved_partition_storage_ids(filter.as_ref());
    let storage_ids: Vec<u64> = match pruned {
        Some(ids) => ids,
        None => table.schema.storage_ids(),
    };
    let parts = storage_ids.len();
    let effective = std::cmp::min(degree.max(1) as usize, parts);
    if effective <= 1 {
        return seq_scan_stream(
            db,
            txn,
            table,
            filter,
            runtime_filters,
            projected_columns,
            use_zone_map,
        );
    }
    super::parallel::PARALLEL_SEQ_SCAN_COUNT.fetch_add(1, Ordering::Relaxed);
    super::parallel::PARALLEL_WORKERS_SPAWNED.fetch_add(effective as u64, Ordering::Relaxed);

    // Round-robin partitions into worker groups so workers' wall-time
    // is balanced even if a few partitions are heavy.
    let mut groups: Vec<Vec<u64>> = vec![Vec::new(); effective];
    for (i, sid) in storage_ids.iter().enumerate() {
        groups[i % effective].push(*sid);
    }

    let (tx, rx) = mpsc::sync_channel::<Result<Vec<PlanRow>, QueryError>>(effective * 2);
    let consumer_alive = std::sync::Arc::new(AtomicBool::new(true));
    let mut worker_handles: Vec<thread::JoinHandle<()>> = Vec::with_capacity(effective);

    let execution_context =
        super::execution_context::ExecutionContext::capture(txn).fork_worker_cancellation();
    let cancellation = execution_context.cancellation().clone();

    for group in groups.into_iter() {
        let tx = tx.clone();
        let alive = consumer_alive.clone();
        let db = Arc::clone(db);
        let table_owned = table.clone();
        let filter_owned = filter.clone();
        let runtime_owned = runtime_filters.to_vec();
        let projection_owned: Option<Vec<usize>> = projected_columns.map(|c| c.to_vec());
        let worker_context = execution_context.clone();
        worker_handles.push(thread::spawn(move || {
            let _active_worker = ActiveStreamWorker::enter();
            let mut buf: Vec<PlanRow> = Vec::with_capacity(GATHER_BATCH_ROWS);
            let flush = |buf: &mut Vec<PlanRow>,
                         alive: &std::sync::Arc<AtomicBool>|
             -> Result<(), QueryError> {
                if buf.is_empty() {
                    return Ok(());
                }
                let batch = std::mem::replace(buf, Vec::with_capacity(GATHER_BATCH_ROWS));
                if tx.send(Ok(batch)).is_err() {
                    alive.store(false, Ordering::Relaxed);
                    return Err(error(
                        crate::error::catalog::ER_QUERY_INTERRUPTED,
                        "exchange consumer closed",
                    ));
                }
                Ok(())
            };
            let run = || -> Result<(), QueryError> {
                let projection: Vec<usize> = projection_owned.clone().unwrap_or_else(|| {
                    super::plan::logical_visible_column_indices(&table_owned.schema)
                });
                let metas: Vec<_> = projection
                    .iter()
                    .map(|&i| {
                        crate::engine::executor::select::plan::intern_col_meta(ColMeta::from_table(
                            &table_owned,
                            i,
                        ))
                    })
                    .collect();
                let projected_schema =
                    super::plan::projected_schema_view(&table_owned.schema, &projection);
                let row_schema = super::plan::RowSchema::from_metas(&metas);
                let compiled_filter = filter_owned.as_ref().and_then(|expr| {
                    super::plan::CompiledPredicate::compile(expr, &projected_schema)
                });
                let outside_latch = (filter_owned.is_some()
                    && !compiled_filter
                        .as_ref()
                        .is_some_and(super::plan::CompiledPredicate::page_latch_safe))
                    || runtime_owned.iter().any(|rf| {
                        super::plan::compile_runtime_filter_probe_projected(
                            rf,
                            &table_owned.schema,
                            &projection,
                        )
                        .is_none()
                    });
                let mut eval_ctx = EvalQueryContext::default();
                crate::engine::executor::partition_router::with_pruned_partitions(
                    group.clone(),
                    || -> Result<(), QueryError> {
                        let mut visit = |values: &[Value]| {
                            crate::engine::cancel::check_cancelled()?;
                            if runtime_owned.is_empty() {
                                if let Some(compiled) = compiled_filter.as_ref() {
                                    if !compiled.eval(values)? {
                                        return Ok(());
                                    }
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
                            let row = PlanRow::builder(fields).schema(row_schema.clone()).build();
                            if !super::plan::runtime_filters_admit(&runtime_owned, &row)? {
                                super::plan::HASH_JOIN_BLOOM_PROBE_FILTERED
                                    .fetch_add(1, Ordering::Relaxed);
                                return Ok(());
                            }
                            let keep = match (&filter_owned, compiled_filter.as_ref()) {
                                (Some(_), Some(compiled)) => {
                                    runtime_owned.is_empty() || compiled.eval(values)?
                                }
                                (Some(expr), None) => {
                                    eval_plan_bool_with_db(&db, txn, expr, &row, &mut eval_ctx)?
                                }
                                (None, _) => true,
                            };
                            if keep {
                                buf.push(row);
                                if buf.len() >= GATHER_BATCH_ROWS {
                                    flush(&mut buf, &alive)?;
                                }
                            }
                            Ok(())
                        };
                        for storage_id in db.scan_storage_ids(&table_owned.schema) {
                            let ranges = use_zone_map
                                .then(|| filter_owned.as_ref())
                                .flatten()
                                .and_then(|expr| {
                                    db.zone_map_candidate_ranges_for_storage_for_plan(
                                        &table_owned.schema,
                                        storage_id,
                                        expr,
                                    )
                                });
                            if let Some(ranges) = ranges {
                                for (lo, hi) in ranges {
                                    super::plan::scan_projected_range(
                                        &db,
                                        &table_owned.schema,
                                        storage_id,
                                        txn,
                                        &projection,
                                        lo,
                                        hi,
                                        outside_latch,
                                        &mut visit,
                                    )?;
                                }
                            } else {
                                let pages = db.heap_page_count_for_storage(storage_id)?;
                                super::plan::scan_projected_range(
                                    &db,
                                    &table_owned.schema,
                                    storage_id,
                                    txn,
                                    &projection,
                                    0,
                                    pages,
                                    outside_latch,
                                    &mut visit,
                                )?;
                            }
                        }
                        Ok(())
                    },
                )?;
                flush(&mut buf, &alive)?;
                Ok(())
            };
            let result = worker_context.run(&db, run);
            if let Err(err) = result {
                let err = worker_context.terminal_error(err);
                if alive.load(Ordering::Relaxed) {
                    let _ = tx.send(Err(err));
                }
            }
        }));
    }
    // Drop the parent's tx so the receiver completes when workers do.
    drop(tx);

    Ok(Box::new(ParallelExchangeStream {
        rx,
        workers: Some(worker_handles),
        current: Vec::new().into_iter(),
        consumer_alive,
        cancellation,
    }))
}

pub(super) fn streaming_result_set_for_select(
    db: Arc<Database>,
    txn: TxnView,
    stmt: Select,
    display_plan: &PhysicalPlan,
) -> Result<QueryResponse, QueryError> {
    super::vec_bridge::preflight_runtime_parameters(display_plan)?;
    // Column-definition packets are fixed before the first batch.  The
    // physical plan already carries catalog-derived `ColMeta` for every
    // input (declared widths, flags, collation and origin included), so bind
    // the projection directly against that immutable schema.  Calling the
    // generic descriptor first is not merely redundant for a multi-table
    // SELECT: `describe_select_columns` has to invoke `plan_select` to obtain
    // a physical schema, repeating the complete join-order search even when
    // `display_plan` came from the shared compiled-plan cache.
    //
    // Keep the catalog descriptor as a compatibility fallback for unusual
    // shapes whose compact plan cannot resolve a projected expression.  The
    // common path therefore preserves source-dependent metadata without a
    // second planning pass, while unsupported metadata shapes retain their
    // previous behaviour and clear error handling.
    let columns = empty_result_columns(&stmt, display_plan)
        .or_else(|_| super::describe_select_columns(&db, &stmt))?;
    let plan = Arc::new(display_plan.clone());
    if super::analyze_stats::is_recording() {
        super::plan::bind_plan_stats_keys(&plan, display_plan);
    }
    let budget = crate::engine::executor::mem_budget::current_budget().unwrap_or_else(|| {
        std::sync::Arc::new(crate::engine::executor::mem_budget::MemBudget::from_env())
    });
    let execution_context =
        super::execution_context::ExecutionContext::capture_with_budget(txn, budget);
    let cursor_columns = columns.clone();
    Ok(QueryResponse::StreamingResultSet(StreamingResultSet {
        columns,
        found_rows: None,
        cursor: Box::new(PlanStreamCursor::spawn(
            db,
            stmt,
            plan,
            execution_context,
            cursor_columns,
        )),
    }))
}

#[cfg(test)]
mod cursor_terminal_error_tests {
    use super::*;
    use crate::engine::api::ResultCursor;
    use crate::engine::executor::mem_budget::MemBudget;

    #[test]
    fn producer_oom_precedes_worker_cancellation_without_consumer_tls() {
        for oom in [false, true] {
            let budget = Arc::new(MemBudget::new(1024));
            let context = super::super::execution_context::ExecutionContext::capture_with_budget(
                TxnView::read_only(0, crate::storage::IsolationLevel::ReadCommitted),
                budget.clone(),
            )
            .fork_worker_cancellation();
            let cancellation = context.cancellation().clone();
            let error = if oom {
                context.terminal_error(
                    crate::error::catalog::ER_OUTOFMEMORY.error("stream output budget exhausted"),
                )
            } else {
                cancellation.cancel_local();
                crate::engine::cancel::interrupted_error()
            };
            assert_eq!(cancellation.check().unwrap_err().code(), 1317);
            let (tx, rx) = mpsc::sync_channel(1);
            tx.send(Err(error)).unwrap();
            drop(tx);
            let code = thread::spawn(move || {
                assert!(crate::engine::executor::mem_budget::current_budget().is_none());
                let mut cursor = PlanStreamCursor {
                    rx: Some(rx),
                    worker: None,
                    cancellation,
                    budget,
                    current: Vec::new().into_iter(),
                    warning_sink: Default::default(),
                    _grouping_stage: None,
                };
                cursor.next_row().unwrap_err().code()
            })
            .join()
            .unwrap();
            assert_eq!(code, if oom { 1037 } else { 1317 });
        }
    }

    fn one_row_batch() -> Vec<Vec<Value>> {
        vec![vec![Value::Int(1)]]
    }

    #[test]
    fn plan_cursor_reports_producer_panic_instead_of_truncating() {
        for panics in [false, true] {
            let (tx, rx) = mpsc::sync_channel(4);
            let worker = crate::util::blocking_pool::spawn_joinable(move || {
                tx.send(Ok(one_row_batch())).unwrap();
                if panics {
                    panic!("injected producer panic");
                }
            });
            let mut cursor = PlanStreamCursor {
                rx: Some(rx),
                worker: Some(worker),
                cancellation: crate::engine::cancel::CancellationSnapshot::capture(),
                budget: Arc::new(MemBudget::new(1 << 20)),
                current: Vec::new().into_iter(),
                warning_sink: Default::default(),
                _grouping_stage: None,
            };
            assert!(cursor.next_row().unwrap().is_some());
            match cursor.next_row() {
                Ok(None) => assert!(!panics),
                Err(error) => {
                    assert!(panics);
                    assert!(error.to_string().contains("injected producer panic"));
                }
                Ok(Some(_)) => panic!("unexpected extra row"),
            }
        }
    }

    #[test]
    fn channel_streams_report_producer_panic_instead_of_truncating() {
        use super::super::plan::PlanStream;
        for panics in [false, true] {
            let (tx, rx) = mpsc::sync_channel::<Result<Vec<PlanRow>, QueryError>>(4);
            let worker = thread::spawn(move || {
                tx.send(Ok(Vec::new())).unwrap();
                if panics {
                    panic!("injected channel producer panic");
                }
            });
            let mut stream = ChannelPlanStream {
                rx: Some(rx),
                worker: Some(worker),
                cancellation: crate::engine::cancel::CancellationSnapshot::capture(),
                current: Vec::new().into_iter(),
            };
            assert_eq!(stream.next().is_err(), panics);

            let (tx, rx) = mpsc::sync_channel::<Result<Vec<PlanRow>, QueryError>>(4);
            let workers = (0..3)
                .map(|index| {
                    let tx = tx.clone();
                    thread::spawn(move || {
                        tx.send(Ok(Vec::new())).unwrap();
                        if panics && index == 1 {
                            panic!("injected exchange worker panic");
                        }
                    })
                })
                .collect();
            drop(tx);
            let mut exchange = ParallelExchangeStream {
                rx,
                workers: Some(workers),
                current: Vec::new().into_iter(),
                consumer_alive: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                cancellation: crate::engine::cancel::CancellationSnapshot::capture(),
            };
            assert_eq!(exchange.next().is_err(), panics);
        }
    }
}
