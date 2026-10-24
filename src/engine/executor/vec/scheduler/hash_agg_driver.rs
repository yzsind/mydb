//! Parallel hash-aggregate driver with bounded batch queues.
//!
//! Workers each own a [`HashAggregateCore`] sink and consume batches from
//! owned scan partitions or a bounded upstream producer queue. Every worker
//! aggregator emits one [`PartitionPartial`] per hash partition,
//! which the coordinator folds into a streaming result. The merge is
//! partition-local — its working set is bounded by the largest
//! partition, not the full result.
//!
//! When a query budget is supplied, every worker uses the same
//! spill-capable `HashAggregateCore` implementation as the serial path. Worker
//! spill segments are merged back into raw partition snapshots before the
//! coordinator performs its cross-worker merge.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::engine::executor::mem_budget::MemBudget;
use crate::engine::executor::vec::batch::{Batch, DEFAULT_BATCH_ROWS};
use crate::engine::executor::vec::hash_agg::{
    self as vec_hash_agg, AggMetrics, AggregateSpec, HashAggregateCore, SharedHashAggSpill,
};
use crate::engine::executor::vec::schema::BatchSchema;
use crate::engine::executor::vec::stream::{BatchOperator, BatchProperties};
use crate::types::{QueryError, Value};

use super::{Morsel, Sink};

/// Optional, semantics-preserving batch preparation performed by aggregate
/// workers immediately before they update local state.  Keeping this inside
/// the worker loop lets deterministic computed aggregate inputs scale with
/// the aggregate itself instead of serialising the whole pipeline in the
/// producer thread.
pub(crate) trait HashAggBatchTransform: Send + Sync {
    fn transform(&self, batch: Batch) -> Result<Batch, QueryError>;
    fn prepare_and_consume(
        &self,
        batch: Batch,
        consume: &mut dyn FnMut(Batch) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        consume(self.transform(batch)?)
    }
}

#[derive(Clone)]
struct ParallelSpillContext {
    input_schema: Arc<BatchSchema>,
    budget: Arc<MemBudget>,
    data_dir: PathBuf,
    shared_work_area: Option<Arc<crate::engine::executor::work_area::WorkAreaHandle>>,
    shared_spill: Option<Arc<SharedHashAggSpill>>,
}

fn parallel_spill_context(
    input_schema: Arc<BatchSchema>,
    budget: Arc<MemBudget>,
    data_dir: PathBuf,
    group_columns: &[usize],
    aggs: &[AggregateSpec],
) -> ParallelSpillContext {
    let shared_work_area = Some(Arc::new(
        crate::engine::executor::work_area::WorkAreaHandle::register(),
    ));
    let shared_spill = Some({
        Arc::new(SharedHashAggSpill::new(
            data_dir.clone(),
            input_schema.clone(),
            group_columns,
            aggs.to_vec(),
        ))
    });
    ParallelSpillContext {
        input_schema,
        budget,
        data_dir,
        shared_work_area,
        shared_spill,
    }
}

/// Marks the producer-side queue as aborted when a worker exits through an
/// error or a panic. `SyncSender::send` cannot observe that situation while
/// the coordinator still owns the shared `Receiver`, so a full queue would
/// otherwise leave the query blocked forever.
struct WorkerAbortGuard {
    abort: Arc<AtomicBool>,
    completed: bool,
}

impl WorkerAbortGuard {
    fn new(abort: Arc<AtomicBool>) -> Self {
        Self {
            abort,
            completed: false,
        }
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for WorkerAbortGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.abort.store(true, Ordering::Release);
        }
    }
}

fn send_batch_with_abort(
    sender: &std::sync::mpsc::SyncSender<Batch>,
    mut batch: Batch,
    abort: &AtomicBool,
    closed_message: &'static str,
) -> Result<(), QueryError> {
    loop {
        if abort.load(Ordering::Acquire) {
            return Err(crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                closed_message,
            ));
        }
        match sender.try_send(batch) {
            Ok(()) => return Ok(()),
            Err(std::sync::mpsc::TrySendError::Full(returned)) => {
                batch = returned;
                crate::engine::cancel::check_cancelled()?;
                // A full queue means aggregate workers are compute-bound.
                // Yielding immediately keeps this producer runnable and can
                // steal CPU from the very consumers that must make progress.
                // Use the same short, cancellation-bounded backoff as the
                // parallel scan exchange.
                std::thread::park_timeout(std::time::Duration::from_micros(50));
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    closed_message,
                ));
            }
        }
    }
}

fn make_worker_agg(
    group_columns: Vec<usize>,
    aggs: Vec<AggregateSpec>,
    spill: Option<&ParallelSpillContext>,
) -> HashAggregateCore {
    match spill {
        Some(ctx) => {
            let aggregate = match &ctx.shared_work_area {
                Some(work_area) => HashAggregateCore::with_budget_and_work_area(
                    group_columns,
                    aggs,
                    ctx.input_schema.clone(),
                    ctx.budget.clone(),
                    ctx.data_dir.clone(),
                    Arc::clone(work_area),
                ),
                None => HashAggregateCore::with_budget(
                    group_columns,
                    aggs,
                    ctx.input_schema.clone(),
                    ctx.budget.clone(),
                    ctx.data_dir.clone(),
                ),
            };
            match &ctx.shared_spill {
                Some(spill) => aggregate.with_shared_spill(Arc::clone(spill)),
                None => aggregate,
            }
        }
        None => HashAggregateCore::new(group_columns, aggs),
    }
}

fn make_parallel_worker_agg(
    group_columns: Vec<usize>,
    aggs: Vec<AggregateSpec>,
    spill: Option<&ParallelSpillContext>,
) -> HashAggregateCore {
    let mut aggregate = make_worker_agg(group_columns, aggs, spill);
    aggregate.enable_parallel_i64_decimal_dedup();
    aggregate
}

fn make_serial_fallback_agg(
    group_columns: Vec<usize>,
    aggs: Vec<AggregateSpec>,
    spill: Option<&ParallelSpillContext>,
) -> HashAggregateCore {
    let serial_context = spill.cloned().map(|mut context| {
        // A single fallback operator must replay its own private files during
        // `finalize`; the shared coordinator is consumed only by the true
        // multi-worker merge path below.
        context.shared_spill = None;
        context
    });
    make_worker_agg(group_columns, aggs, serial_context.as_ref())
}

/// Sink that runs a per-worker [`HashAggregateCore`].
struct HashAggSink {
    agg: HashAggregateCore,
}

impl Sink for HashAggSink {
    fn consume(&mut self, morsel: Morsel) -> Result<(), QueryError> {
        for b in &morsel.batches {
            self.agg.consume_batch(b)?;
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<(), QueryError> {
        Ok(())
    }
}

const PARALLEL_WORKER_QUEUE_CAPACITY: usize = 2;

#[derive(Clone, Copy)]
struct ParallelWorkerMessages {
    queue_closed: &'static str,
    worker_panicked: &'static str,
}

const HASH_AGG_WORKER_MESSAGES: ParallelWorkerMessages = ParallelWorkerMessages {
    queue_closed: "vector hash-aggregate worker queue closed",
    worker_panicked: "vector hash-aggregate worker panicked",
};

const ROLLUP_WORKER_MESSAGES: ParallelWorkerMessages = ParallelWorkerMessages {
    queue_closed: "vector ROLLUP worker queue closed",
    worker_panicked: "vector ROLLUP worker panicked",
};

/// Run the common bounded-channel lifecycle used by every parallel hash
/// aggregate consumer. The producer remains on the calling thread because a
/// `BatchOperator` is not required to be `Send`; workers receive only owned
/// batches and install the complete execution context before touching them.
fn run_parallel_hash_agg_workers<PrepareBatch>(
    stream: &mut dyn BatchOperator,
    initial: Vec<Batch>,
    group_columns: &[usize],
    aggs: &[AggregateSpec],
    worker_count: usize,
    spill_context: Option<&ParallelSpillContext>,
    prepare_batch: PrepareBatch,
    messages: ParallelWorkerMessages,
) -> Result<Vec<HashAggSink>, QueryError>
where
    PrepareBatch:
        Fn(Batch, &mut dyn FnMut(Batch) -> Result<(), QueryError>) -> Result<(), QueryError> + Sync,
{
    debug_assert!(worker_count > 1);
    debug_assert!(!initial.is_empty());

    let mut senders = Vec::with_capacity(worker_count);
    let mut receivers = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let (sender, receiver) =
            std::sync::mpsc::sync_channel::<Batch>(PARALLEL_WORKER_QUEUE_CAPACITY);
        senders.push(sender);
        receivers.push(receiver);
    }

    let abort = Arc::new(AtomicBool::new(false));
    let session_info = crate::engine::expr::current_session_expr_info();
    let params_snapshot = crate::engine::params::snapshot_current_context();
    let cancel_token = crate::engine::cancel::current_token();
    let vector_mode = crate::engine::executor::vec::config::effective_mode();
    let spill_context = spill_context.cloned();

    std::thread::scope(|scope| -> Result<Vec<HashAggSink>, QueryError> {
        let mut handles = Vec::with_capacity(worker_count);
        for receiver in receivers {
            let groups = group_columns.to_vec();
            let worker_aggs = aggs.to_vec();
            let token = cancel_token.clone();
            let session_info = &session_info;
            let spill = spill_context.clone();
            let abort = Arc::clone(&abort);
            let prepare_batch = &prepare_batch;
            let params = params_snapshot.clone();
            handles.push(scope.spawn(move || {
                let mut abort_guard = WorkerAbortGuard::new(abort);
                let _cancel_guard = token.map(crate::engine::cancel::enter);
                let _vector_mode = crate::engine::executor::vec::config::enter_mode(vector_mode);
                let result =
                    crate::engine::expr::with_session_expr_info_option(session_info, || {
                        let run = || {
                            let mut sink = HashAggSink {
                                agg: make_parallel_worker_agg(groups, worker_aggs, spill.as_ref()),
                            };
                            while let Ok(batch) = receiver.recv() {
                                prepare_batch(batch, &mut |batch| sink.agg.consume_batch(&batch))?;
                            }
                            Ok::<_, QueryError>(sink)
                        };
                        let run_with_budget = || match spill.as_ref() {
                            Some(context) => {
                                crate::engine::executor::mem_budget::with_query_budget(
                                    context.budget.clone(),
                                    run,
                                )
                            }
                            None => run(),
                        };
                        match params.as_ref() {
                            Some(params) => params.run(run_with_budget),
                            None => run_with_budget(),
                        }
                    });
                if result.is_ok() {
                    abort_guard.complete();
                }
                result
            }));
        }

        let produce = (|| -> Result<(), QueryError> {
            let mut next_worker = 0usize;
            for batch in initial {
                send_batch_with_abort(&senders[next_worker], batch, &abort, messages.queue_closed)?;
                next_worker = (next_worker + 1) % senders.len();
            }
            while let Some(batch) = stream.next_batch()? {
                crate::engine::cancel::check_cancelled()?;
                send_batch_with_abort(&senders[next_worker], batch, &abort, messages.queue_closed)?;
                next_worker = (next_worker + 1) % senders.len();
            }
            Ok(())
        })();
        drop(senders);

        let mut sinks = Vec::with_capacity(worker_count);
        let mut worker_error = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(sink)) => sinks.push(sink),
                Ok(Err(error)) => {
                    worker_error.get_or_insert(error);
                }
                Err(_) => {
                    worker_error.get_or_insert_with(|| {
                        crate::storage::error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            messages.worker_panicked,
                        )
                    });
                }
            }
        }
        if let Some(error) = worker_error {
            return Err(error);
        }
        produce?;
        Ok(sinks)
    })
}

struct ParallelWorkerPartials {
    partitions: Vec<Vec<Option<vec_hash_agg::PartitionPartial>>>,
    metrics: AggMetrics,
}

/// Drain worker-local and shared-spill raw states while combining their
/// resource metrics. Plain GROUP BY and ROLLUP then choose only the final
/// state materialisation they need.
fn take_parallel_worker_partials(
    sinks: &mut [HashAggSink],
    spill_context: Option<&ParallelSpillContext>,
) -> Result<ParallelWorkerPartials, QueryError> {
    debug_assert!(!sinks.is_empty());
    HashAggregateCore::align_parallel_partitioning(sinks.iter_mut().map(|sink| &mut sink.agg))?;
    let num_partitions = sinks[0].agg.num_partitions();
    let mut partitions = Vec::with_capacity(sinks.len() + usize::from(spill_context.is_some()));
    let mut metrics = AggMetrics {
        is_streaming: true,
        workers: sinks.len() as u32,
        aggregate_mode: Some("grouped_typed"),
        ..AggMetrics::default()
    };

    for sink in sinks.iter_mut() {
        let worker_metrics = sink.agg.metrics();
        metrics.peak_memory_bytes = metrics
            .peak_memory_bytes
            .saturating_add(worker_metrics.peak_memory_bytes);
        metrics.spill_bytes = metrics
            .spill_bytes
            .saturating_add(worker_metrics.spill_bytes);
        metrics.spill_runs = metrics.spill_runs.saturating_add(worker_metrics.spill_runs);
        metrics.spill_passes = metrics.spill_passes.max(worker_metrics.spill_passes);
        metrics.spill_partitions = metrics
            .spill_partitions
            .saturating_add(worker_metrics.spill_partitions);
        metrics.did_spill |= worker_metrics.did_spill;
        metrics.batches = metrics.batches.saturating_add(worker_metrics.batches);
        metrics.native_aggregate_rows = metrics
            .native_aggregate_rows
            .saturating_add(worker_metrics.native_aggregate_rows);
        metrics.fallback_aggregate_rows = metrics
            .fallback_aggregate_rows
            .saturating_add(worker_metrics.fallback_aggregate_rows);

        let mut worker_partitions = Vec::with_capacity(num_partitions);
        for partition in 0..num_partitions {
            worker_partitions.push(sink.agg.take_partition_merged_snapshot(partition)?);
        }
        partitions.push(worker_partitions);
    }

    if let Some(shared) = spill_context.and_then(|context| context.shared_spill.as_ref()) {
        let shared_partitions = shared.partition_snapshots()?;
        metrics.spill_partitions = shared_partitions
            .iter()
            .filter(|partial| partial.is_some())
            .count() as u64;
        metrics.did_spill |= metrics.spill_partitions > 0;
        partitions.push(shared_partitions);
    }
    if metrics.fallback_aggregate_rows > 0 {
        metrics.aggregate_mode = Some("scalar_batch");
    }

    Ok(ParallelWorkerPartials {
        partitions,
        metrics,
    })
}

/// Build the finest ROLLUP groups with the production parallel workers.
/// Empty or unsupported group layouts return `None` for the row executor.
pub fn run_rollup_finest_parallel(
    stream: &mut dyn BatchOperator,
    group_columns: Vec<usize>,
    mut aggs: Vec<AggregateSpec>,
    worker_count: usize,
    budget: Option<Arc<MemBudget>>,
    data_dir: PathBuf,
) -> Result<Option<RollupFinestResult>, QueryError> {
    vec_hash_agg::bind_aggregate_comparison_domains(&mut aggs, stream.schema());
    let spill_context = budget.map(|budget| {
        parallel_spill_context(
            stream.schema().clone(),
            budget,
            data_dir,
            &group_columns,
            &aggs,
        )
    });
    let scheduler_min_rows = 1;
    let mut initial = Vec::new();
    let mut initial_rows = 0usize;
    while initial_rows < scheduler_min_rows {
        match stream.next_batch()? {
            Some(batch) => {
                crate::engine::cancel::check_cancelled()?;
                initial_rows = initial_rows.saturating_add(batch.num_rows());
                initial.push(batch);
            }
            None => {
                break;
            }
        }
    }
    if initial.is_empty() {
        return Ok(None);
    }
    if worker_count <= 1 {
        let mut agg = make_serial_fallback_agg(group_columns, aggs, spill_context.as_ref());
        for batch in &initial {
            agg.consume_batch(batch)?;
        }
        while let Some(batch) = stream.next_batch()? {
            crate::engine::cancel::check_cancelled()?;
            agg.consume_batch(&batch)?;
        }
        let metrics = AggMetrics {
            is_streaming: true,
            ..agg.metrics()
        };
        let nparts = agg.num_partitions();
        let mut buckets = Vec::new();
        for p in 0..nparts {
            if let Some(part) = agg.take_partition_merged_snapshot(p)? {
                buckets.extend(part.into_buckets());
            }
        }
        return Ok(if buckets.is_empty() {
            None
        } else {
            Some(RollupFinestResult { buckets, metrics })
        });
    }

    let mut sinks = run_parallel_hash_agg_workers(
        stream,
        initial,
        &group_columns,
        &aggs,
        worker_count,
        spill_context.as_ref(),
        |batch, consume| consume(batch),
        ROLLUP_WORKER_MESSAGES,
    )?;
    if sinks.is_empty() {
        return Ok(None);
    }
    // Keep shared spill files partitioned until finalization. The old path
    // replayed every partition on the coordinator, then hashed all snapshots
    // again serially despite having selected a parallel aggregate.
    let mut resident_context = spill_context.clone();
    if let Some(context) = resident_context.as_mut() {
        context.shared_spill = None;
    }
    let worker_output = take_parallel_worker_partials(&mut sinks, resident_context.as_ref())?;
    let mut metrics = worker_output.metrics;
    let shared = spill_context
        .as_ref()
        .and_then(|context| context.shared_spill.as_ref());
    if let Some(shared) = shared {
        metrics.spill_partitions = shared.written_partition_count() as u64;
        metrics.did_spill |= metrics.spill_partitions > 0;
    }
    let buckets = merge_rollup_partitions(
        worker_output.partitions,
        &aggs,
        worker_count,
        spill_context.as_ref(),
    )?;
    Ok(if buckets.is_empty() {
        None
    } else {
        Some(RollupFinestResult { buckets, metrics })
    })
}

/// Bounded parallel replay/merge, retaining partition order and each worker's
/// representative order within a partition. Session semantics, cancellation
/// and the query grant are installed just as in ordinary aggregate finalize.
fn merge_rollup_partitions(
    partials: Vec<Vec<Option<vec_hash_agg::PartitionPartial>>>,
    aggs: &[AggregateSpec],
    workers: usize,
    spill: Option<&ParallelSpillContext>,
) -> Result<Vec<vec_hash_agg::GroupBucketSnapshot>, QueryError> {
    let count = partials.first().map_or(0, Vec::len);
    if count == 0 {
        return Ok(Vec::new());
    }
    let mut partitions: Vec<_> = (0..count).map(|_| Vec::new()).collect();
    for worker in partials {
        for (partition, part) in worker.into_iter().enumerate() {
            partitions[partition].push(vec![part]);
        }
    }
    // Each replay owns a transient hash table as well as its final states.
    let budget_workers = spill.map_or(4, |context| {
        (context.budget.limit() / (8 * 1024 * 1024)).max(1)
    });
    let workers = workers.max(1).min(4).min(count).min(budget_workers);
    let mut jobs: Vec<Vec<_>> = (0..workers).map(|_| Vec::new()).collect();
    for (partition, parts) in partitions.into_iter().enumerate() {
        jobs[partition % workers].push((partition, parts));
    }
    let session = crate::engine::expr::current_session_expr_info();
    let params = crate::engine::params::snapshot_current_context();
    let cancellation = crate::engine::cancel::current_token();
    let mode = crate::engine::executor::vec::config::effective_mode();
    let merged = std::thread::scope(|scope| {
        let handles = jobs
            .into_iter()
            .map(|jobs| {
                let session = &session;
                let params = params.clone();
                let cancellation = cancellation.clone();
                scope.spawn(move || {
                    let _cancel = cancellation.map(crate::engine::cancel::enter);
                    let _mode = crate::engine::executor::vec::config::enter_mode(mode);
                    let run = || -> Result<Vec<_>, QueryError> {
                        let mut output = Vec::with_capacity(jobs.len());
                        for (partition, mut parts) in jobs {
                            crate::engine::cancel::check_cancelled()?;
                            if let Some(shared) =
                                spill.and_then(|context| context.shared_spill.as_ref())
                            {
                                parts.push(vec![shared.partition_snapshot(partition)?]);
                            }
                            output.push((
                                partition,
                                vec_hash_agg::merge_worker_partition_snapshots_into_buckets(
                                    parts, aggs,
                                ),
                            ));
                        }
                        Ok(output)
                    };
                    let budgeted = || match spill {
                        Some(context) => crate::engine::executor::mem_budget::with_query_budget(
                            context.budget.clone(),
                            run,
                        ),
                        None => run(),
                    };
                    crate::engine::expr::with_session_expr_info_option(session, || {
                        match params.as_ref() {
                            Some(params) => params.run(budgeted),
                            None => budgeted(),
                        }
                    })
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| {
                handle.join().map_err(|_| {
                    crate::storage::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "ROLLUP partition merge worker panicked",
                    )
                })?
            })
            .collect::<Result<Vec<_>, QueryError>>()
    })?;
    let mut partitions = merged.into_iter().flatten().collect::<Vec<_>>();
    partitions.sort_unstable_by_key(|(partition, _)| *partition);
    Ok(partitions
        .into_iter()
        .flat_map(|(_, buckets)| buckets)
        .collect())
}

/// Raw finest-group state and resource metrics consumed by the ROLLUP phase-2
/// derivation. Keeping metrics attached makes spill behavior observable to
/// EXPLAIN/test callers without changing the aggregate state representation.
pub struct RollupFinestResult {
    pub buckets: Vec<vec_hash_agg::GroupBucketSnapshot>,
    pub metrics: AggMetrics,
}

/// Wrap [`run_hash_agg_parallel`] into the same `(BatchOperator,
/// metrics)` shape `HashAggregateCore::finalize_stream` + `metrics()`
/// returns, so the vec-bridge caller can swap implementations without
/// otherwise touching its post-processing pipeline. The output
/// `Batch` carries the merged group rows (one column per group key +
/// one per finalised agg) keyed off `output_schema`.
enum ParallelStreamBuild {
    Serial(HashAggregateCore),
    Parallel(Vec<HashAggSink>),
}

fn align_worker_dictionaries(
    sinks: &mut [HashAggSink],
    budget: Option<&Arc<MemBudget>>,
) -> Result<(), QueryError> {
    HashAggregateCore::align_parallel_partitioning(sinks.iter_mut().map(|sink| &mut sink.agg))?;
    for sink in sinks.iter_mut() {
        sink.agg.retire_dictionary_build_lookups()?;
    }
    let serial = |sinks: &mut [HashAggSink]| {
        let mut registries = Vec::new();
        for sink in sinks {
            sink.agg.align_parallel_dictionary_tokens(&mut registries);
        }
    };
    let spill = |sinks: &mut [HashAggSink]| -> Result<(), QueryError> {
        // Scan/aggregate workers have joined. Reuse the same planned degree
        // for this spill phase instead of making the coordinator serialize
        // all shards. Each writer uses bounded, governed batches.
        const STACK_BYTES: usize = 2 * 1024 * 1024;
        let stacks =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_recoverable(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
                sinks.len().saturating_mul(STACK_BYTES + 4096),
            );
        let (Some(budget), Ok(_stacks)) = (budget, stacks) else {
            for sink in sinks {
                sink.agg.spill_dictionary_for_merge()?;
            }
            return Ok(());
        };
        let cancel = crate::engine::cancel::CancellationSnapshot::capture();
        std::thread::scope(|scope| -> Result<(), QueryError> {
            let mut handles = Vec::with_capacity(sinks.len());
            for sink in sinks {
                let budget = budget.clone();
                let cancel = cancel.clone();
                handles.push(
                    std::thread::Builder::new()
                        .stack_size(STACK_BYTES)
                        .spawn_scoped(scope, move || {
                            let _cancel = cancel.enter();
                            crate::engine::executor::mem_budget::with_query_budget(budget, || {
                                sink.agg.spill_dictionary_for_merge()
                            })
                        })
                        .map_err(|error| {
                            crate::error::catalog::ER_GET_ERRNO
                                .error(format!("cannot start aggregate spill worker: {error}"))
                        })?,
                );
            }
            for handle in handles {
                handle.join().map_err(|_| {
                    crate::error::catalog::ER_GET_ERRNO.error("aggregate spill worker panicked")
                })??;
            }
            Ok(())
        })?;
        Ok(())
    };
    let active = |sink: &HashAggSink| {
        let metrics = sink.agg.metrics();
        metrics.native_aggregate_rows != 0 || metrics.fallback_aggregate_rows != 0
    };
    let Some(mut layout) = sinks
        .iter()
        .find_map(|sink| sink.agg.parallel_dictionary_layout())
    else {
        serial(sinks);
        return Ok(());
    };
    // A local cardinality sample may choose a different direct integer lane.
    // Pick the most common compatible encoding, then repack minority workers
    // before token alignment; raw local u128 keys are not comparable otherwise.
    let mut best = (0, 0);
    for sink in sinks.iter() {
        if let Some(candidate) = sink.agg.parallel_dictionary_layout() {
            let count = sinks
                .iter()
                .filter(|sink| sink.agg.parallel_dictionary_layout().as_ref() == Some(&candidate))
                .count();
            // First-batch NDV can be misleading for clustered input. At this
            // phase the actual primitive dictionaries are known: prefer the
            // compatible layout that removes the most dictionary entries,
            // using frequency only to break ties. This avoids converting a
            // high-cardinality raw integer into a huge merge dictionary.
            let eliminated = candidate
                .iter()
                .enumerate()
                .filter(|(_, column)| column.0 & 0x80 != 0)
                .map(|(column, _)| {
                    sinks
                        .iter()
                        .map(|sink| sink.agg.dictionary_token_count(column))
                        .sum::<usize>()
                })
                .sum::<usize>();
            if (eliminated, count) > best {
                layout = candidate;
                best = (eliminated, count);
            }
        }
    }
    // All workers can make the same bad first-batch choice on clustered
    // input. Also consider a new compatible packing, using the complete
    // primitive domains rather than only layouts that happened to be sampled.
    if layout.iter().any(|column| matches!(column.0, 1..=3)) {
        let domains = (0..layout.len())
            .map(|column| {
                sinks
                    .iter()
                    .map(|sink| sink.agg.dictionary_primitive_domain(column))
                    .sum::<usize>()
            })
            .collect::<smallvec::SmallVec<[usize; 8]>>();
        layout = vec_hash_agg::preferred_dictionary_layout(&layout, &domains);
    }
    if sinks
        .iter()
        .any(|sink| active(sink) && sink.agg.parallel_dictionary_layout().as_ref() != Some(&layout))
    {
        let Some(budget) = budget else {
            return spill(sinks);
        };
        let Ok(_metadata) = crate::engine::executor::mem_budget::OwnedMemCharge::new(
            budget.clone(),
            sinks.len().saturating_mul(4096),
        ) else {
            return spill(sinks);
        };
        let cancel = crate::engine::cancel::CancellationSnapshot::capture();
        let normalized = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(sinks.len());
            for sink in sinks.iter_mut() {
                if !active(sink) || sink.agg.parallel_dictionary_layout().as_ref() == Some(&layout)
                {
                    continue;
                }
                let layout = &layout;
                let budget = budget.clone();
                let cancel = cancel.clone();
                handles.push(scope.spawn(move || {
                    let _cancel = cancel.enter();
                    crate::engine::executor::mem_budget::with_query_budget(budget, || {
                        sink.agg.normalize_dictionary_layout(layout)
                    })
                }));
            }
            let mut normalized = true;
            let mut failure = None;
            for handle in handles {
                let result = handle.join().unwrap_or_else(|_| {
                    Err(crate::error::catalog::ER_GET_ERRNO
                        .error("dictionary normalization worker panicked"))
                });
                match result {
                    Ok(value) => normalized &= value,
                    Err(error) => {
                        if failure.is_none() {
                            failure = Some(error);
                        }
                    }
                }
            }
            match failure {
                Some(error) => Err(error),
                None => Ok(normalized),
            }
        })?;
        if !normalized {
            return spill(sinks);
        }
    }
    let keys = sinks
        .iter()
        .map(|sink| {
            (0..layout.len())
                .map(|column| sink.agg.dictionary_token_count(column))
                .sum::<usize>()
        })
        .sum::<usize>();
    if sinks.len() < 2 || keys < 128 * 1024 || budget.is_none() {
        serial(sinks);
        return Ok(());
    }
    let workers = sinks.len().min(16);
    let shards_count = 1usize << workers.ilog2();
    // Each unique key holds an Arc and a u32. Reserve a conservative complete
    // hash-table capacity envelope before parallel growth; canonical bytes are
    // shared with the encoders. No budget means spill, never uncharged growth.
    let bytes = keys.saturating_mul(64).saturating_add(shards_count * 256);
    let Ok(charge) =
        crate::engine::executor::mem_budget::OwnedMemCharge::new(budget.unwrap().clone(), bytes)
    else {
        return spill(sinks);
    };
    for sink in sinks.iter_mut() {
        if sink.agg.reserve_dictionary_merge_tokens().is_err() {
            drop(charge);
            return spill(sinks);
        }
    }
    for (column, descriptor) in layout.iter().enumerate() {
        if descriptor.0 & 0x80 == 0 && matches!(descriptor.0, 1..=3) {
            // Packed primitive domains use exact raw bits. Their registry is
            // covered by the same reserved capacity envelope as text shards;
            // no temporary canonical bytes or Value envelopes are needed.
            let mut registry = hashbrown::HashMap::with_hasher(ahash::RandomState::new());
            let mut overflow = false;
            for sink in sinks.iter() {
                sink.agg.visit_primitive_dictionary_values(column, |value| {
                    if overflow {
                        return;
                    }
                    let next = registry.len() as u64 + 1;
                    if let hashbrown::hash_map::Entry::Vacant(entry) = registry.entry(value) {
                        if next > u64::from(descriptor.4) {
                            overflow = true;
                        } else {
                            entry.insert(next as u32);
                        }
                    }
                });
            }
            if overflow {
                drop(registry);
                drop(charge);
                return spill(sinks);
            }
            for sink in sinks.iter_mut() {
                sink.agg
                    .install_primitive_dictionary_tokens(column, &registry);
            }
            continue;
        }
        if descriptor.0 != 0 {
            continue;
        }
        let shards = std::thread::scope(|scope| -> Result<Vec<_>, QueryError> {
            let mut handles = Vec::with_capacity(shards_count);
            for shard in 0..shards_count {
                let sinks = &*sinks;
                handles.push(scope.spawn(move || {
                    let mut map = hashbrown::HashMap::<
                            Arc<[u8]>,
                            u32,
                            vec_hash_agg::U128BuildHasher,
                        >::with_hasher(
                            vec_hash_agg::U128BuildHasher
                        );
                    for sink in sinks {
                        sink.agg.visit_dictionary_tokens(column, |key, hash| {
                            if hash as usize & (shards_count - 1) != shard {
                                return;
                            }
                            let next = map.len() as u32 + 1;
                            if let hashbrown::hash_map::RawEntryMut::Vacant(entry) = map
                                .raw_entry_mut()
                                .from_hash(hash, |candidate| candidate.as_ref() == key.as_ref())
                            {
                                entry.insert_hashed_nocheck(hash, key.clone(), next);
                            }
                        });
                    }
                    map
                }));
            }
            handles
                .into_iter()
                .map(|handle| {
                    handle.join().map_err(|_| {
                        crate::error::catalog::ER_UNKNOWN_ERROR
                            .error("parallel dictionary merge worker panicked")
                    })
                })
                .collect()
        })?;
        let unique = shards.iter().map(|shard| shard.len() as u64).sum::<u64>();
        if unique > u64::from(descriptor.4) {
            drop(shards);
            drop(charge);
            return spill(sinks);
        }
        let mut offsets = [0u32; 16];
        for shard in 1..shards_count {
            offsets[shard] = offsets[shard - 1] + shards[shard - 1].len() as u32;
        }
        std::thread::scope(|scope| -> Result<(), QueryError> {
            let mut handles = Vec::with_capacity(sinks.len());
            for sink in sinks.iter_mut() {
                let shards = &shards;
                let offsets = &offsets[..shards_count];
                handles.push(scope.spawn(move || {
                    sink.agg
                        .install_dictionary_merge_tokens(column, shards, offsets)
                }));
            }
            for handle in handles {
                handle.join().map_err(|_| {
                    crate::error::catalog::ER_UNKNOWN_ERROR
                        .error("parallel dictionary assignment worker panicked")
                })?;
            }
            Ok(())
        })?;
    }
    Ok(())
}

fn build_parallel_stream_sinks(
    stream: &mut dyn BatchOperator,
    group_columns: Vec<usize>,
    aggs: Vec<AggregateSpec>,
    worker_count: usize,
    spill_context: Option<ParallelSpillContext>,
    transform: Option<Arc<dyn HashAggBatchTransform>>,
) -> Result<ParallelStreamBuild, QueryError> {
    if let Some(workers) = stream.take_parallel_workers(worker_count)? {
        let abort = Arc::new(AtomicBool::new(false));
        let sinks = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(workers.len());
            for worker in workers {
                let abort = abort.clone();
                let groups = group_columns.clone();
                let aggs = aggs.clone();
                let spill = spill_context.clone();
                let transform = transform.clone();
                handles.push(scope.spawn(move || {
                    let mut guard = WorkerAbortGuard::new(abort.clone());
                    let mut sink = HashAggSink {
                        agg: make_parallel_worker_agg(groups, aggs, spill.as_ref()),
                    };
                    worker.run(
                        &mut |batch| {
                            if abort.load(Ordering::Relaxed) {
                                return Err(crate::storage::error(
                                    crate::error::catalog::ER_QUERY_INTERRUPTED,
                                    "parallel aggregate aborted",
                                ));
                            }
                            match &transform {
                                Some(transform) => transform
                                    .prepare_and_consume(batch, &mut |batch| {
                                        sink.agg.consume_batch(&batch)
                                    }),
                                None => sink.agg.consume_batch(&batch),
                            }
                        },
                        &abort,
                    )?;
                    guard.complete();
                    Ok::<_, QueryError>(sink)
                }));
            }
            let mut sinks = Vec::with_capacity(handles.len());
            let mut error = None;
            for handle in handles {
                match handle.join() {
                    Ok(Ok(sink)) => sinks.push(sink),
                    Ok(Err(err)) => {
                        // Prefer the originating error over a sibling's abort.
                        if error
                            .as_ref()
                            .is_none_or(|old: &QueryError| old.code() == 1317)
                        {
                            error = Some(err);
                        }
                    }
                    Err(_) => {
                        error = Some(crate::storage::error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            "parallel aggregate worker panicked",
                        ));
                    }
                }
            }
            match error {
                Some(error) => Err(error),
                None => Ok(sinks),
            }
        })?;
        if sinks.iter().all(|sink| {
            let metrics = sink.agg.metrics();
            metrics.native_aggregate_rows == 0 && metrics.fallback_aggregate_rows == 0
        }) {
            return Ok(ParallelStreamBuild::Serial(make_serial_fallback_agg(
                group_columns,
                aggs,
                spill_context.as_ref(),
            )));
        }
        return Ok(ParallelStreamBuild::Parallel(sinks));
    }
    let Some(first) = stream.next_batch()? else {
        return Ok(ParallelStreamBuild::Serial(make_serial_fallback_agg(
            group_columns,
            aggs,
            spill_context.as_ref(),
        )));
    };
    if worker_count <= 1 {
        let mut agg = make_serial_fallback_agg(group_columns, aggs, spill_context.as_ref());
        let mut consume = |batch: Batch| agg.consume_batch(&batch);
        match transform.as_ref() {
            Some(transform) => transform.prepare_and_consume(first, &mut consume)?,
            None => consume(first)?,
        }
        while let Some(batch) = stream.next_batch()? {
            match transform.as_ref() {
                Some(transform) => transform.prepare_and_consume(batch, &mut consume)?,
                None => consume(batch)?,
            }
        }
        return Ok(ParallelStreamBuild::Serial(agg));
    }

    let prepare_batch =
        move |batch, consume: &mut dyn FnMut(Batch) -> Result<(), QueryError>| match transform
            .as_ref()
        {
            Some(transform) => transform.prepare_and_consume(batch, consume),
            None => consume(batch),
        };
    let sinks = run_parallel_hash_agg_workers(
        stream,
        vec![first],
        &group_columns,
        &aggs,
        worker_count,
        spill_context.as_ref(),
        prepare_batch,
        HASH_AGG_WORKER_MESSAGES,
    )?;
    Ok(ParallelStreamBuild::Parallel(sinks))
}

struct ParallelHashAggOutputStream {
    schema: Arc<BatchSchema>,
    aggs: Vec<AggregateSpec>,
    receiver: Option<std::sync::mpsc::Receiver<ParallelFinalizeMessage>>,
    workers: Vec<std::thread::JoinHandle<()>>,
    workers_remaining: usize,
    abort: Arc<AtomicBool>,
    pending: std::collections::VecDeque<Vec<Value>>,
}

enum ParallelFinalizeMessage {
    Rows(Vec<Vec<Value>>),
    Error(QueryError),
    Done,
}

fn finalize_worker_count(requested: usize, budget: Option<usize>, replay_bytes: usize) -> usize {
    let requested = requested.max(1);
    match budget {
        // Leave half of the work area for resident tails and queued output.
        // Preserve full parallelism when no shared spill needs expansion.
        Some(budget) if replay_bytes != 0 => requested.min((budget / 2 / replay_bytes).max(1)),
        _ => requested,
    }
}

fn parallel_finalize_stream(
    schema: Arc<BatchSchema>,
    aggs: Vec<AggregateSpec>,
    sinks: Vec<HashAggSink>,
    shared_spill: Option<Arc<SharedHashAggSpill>>,
    budget: Option<Arc<MemBudget>>,
    final_filter: Option<vec_hash_agg::AggregateFinalFilter>,
    count_topk: Option<vec_hash_agg::AggregateCountTopKHint>,
) -> ParallelHashAggOutputStream {
    let worker_count = finalize_worker_count(
        sinks.len(),
        budget.as_ref().map(|budget| budget.limit()),
        shared_spill
            .as_ref()
            .map_or(0, |spill| spill.max_partition_replay_bytes()),
    );
    let num_partitions = sinks
        .first()
        .map(|sink| sink.agg.num_partitions())
        .unwrap_or(0);
    let sinks = Arc::new(
        sinks
            .into_iter()
            .map(Mutex::new)
            .collect::<Vec<Mutex<HashAggSink>>>(),
    );
    // Each message owns at most one normal output batch. Keeping the channel
    // this small bounds finalized rows while still allowing every worker to
    // overlap partition merge with downstream Top-N consumption.
    let (sender, receiver) = std::sync::mpsc::sync_channel(worker_count.saturating_mul(2).max(2));
    let abort = Arc::new(AtomicBool::new(false));
    let session_info = crate::engine::expr::current_session_expr_info();
    let params = crate::engine::params::snapshot_current_context();
    let cancel_token = crate::engine::cancel::current_token();
    let vector_mode = crate::engine::executor::vec::config::effective_mode();
    let mut workers = Vec::with_capacity(worker_count);

    for worker in 0..worker_count {
        let sender = sender.clone();
        let sinks = Arc::clone(&sinks);
        let shared_spill = shared_spill.clone();
        let budget = budget.clone();
        let final_filter = final_filter.clone();
        let aggs = aggs.clone();
        let abort = Arc::clone(&abort);
        let session_info = session_info.clone();
        let params = params.clone();
        let cancel_token = cancel_token.clone();
        workers.push(std::thread::spawn(move || {
            let _cancel_guard = cancel_token.map(crate::engine::cancel::enter);
            let _vector_mode = crate::engine::executor::vec::config::enter_mode(vector_mode);
            let run = || -> Result<(), QueryError> {
                for partition in (worker..num_partitions).step_by(worker_count) {
                    if abort.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                    crate::engine::cancel::check_cancelled()?;
                    let mut partials = Vec::with_capacity(sinks.len() + 1);
                    for sink in sinks.iter() {
                        let mut sink = sink.lock().map_err(|_| {
                            crate::storage::error(
                                crate::error::catalog::ER_GET_ERRNO,
                                "parallel hash-aggregate finalize lock poisoned",
                            )
                        })?;
                        partials.push(vec![sink.agg.take_partition_merged_snapshot(partition)?]);
                    }
                    let compact = match (&shared_spill, count_topk, &final_filter) {
                        (Some(shared), Some(hint), None) => {
                            shared.try_partition_count_topk(partition, &mut partials, hint)?
                        }
                        _ => None,
                    };
                    let rows = if let Some(rows) = compact {
                        rows
                    } else {
                        if let Some(shared) = &shared_spill {
                            partials.push(vec![shared.partition_snapshot(partition)?]);
                        }
                        vec_hash_agg::merge_worker_partition_snapshots_filtered_topk(
                            partials,
                            &aggs,
                            final_filter.as_ref(),
                            count_topk,
                        )?
                    };
                    let mut chunk = Vec::with_capacity(DEFAULT_BATCH_ROWS);
                    for row in rows {
                        chunk.push(row);
                        if chunk.len() == DEFAULT_BATCH_ROWS {
                            if sender
                                .send(ParallelFinalizeMessage::Rows(std::mem::take(&mut chunk)))
                                .is_err()
                            {
                                return Ok(());
                            }
                            chunk = Vec::with_capacity(DEFAULT_BATCH_ROWS);
                        }
                    }
                    if !chunk.is_empty()
                        && sender.send(ParallelFinalizeMessage::Rows(chunk)).is_err()
                    {
                        return Ok(());
                    }
                }
                Ok(())
            };
            let run_with_budget = || match budget {
                Some(budget) => crate::engine::executor::mem_budget::with_query_budget(budget, run),
                None => run(),
            };
            let result =
                crate::engine::expr::with_session_expr_info_option(&session_info, || match params
                    .as_ref()
                {
                    Some(params) => params.run(run_with_budget),
                    None => run_with_budget(),
                });
            match result {
                Ok(()) => {
                    let _ = sender.send(ParallelFinalizeMessage::Done);
                }
                Err(error) => {
                    abort.store(true, Ordering::Relaxed);
                    let _ = sender.send(ParallelFinalizeMessage::Error(error));
                }
            }
        }));
    }
    drop(sender);

    ParallelHashAggOutputStream {
        schema,
        aggs,
        receiver: Some(receiver),
        workers,
        workers_remaining: worker_count,
        abort,
        pending: std::collections::VecDeque::new(),
    }
}

fn try_parallel_global_distinct_count(
    sinks: &[HashAggSink],
    budget: &Arc<MemBudget>,
) -> Result<Option<usize>, QueryError> {
    use crate::engine::executor::hash_aggregate::{parallel_distinct, DistinctState};
    if sinks.len() > 16 {
        return Ok(None);
    }
    let mut states = smallvec::SmallVec::<[&DistinctState; 16]>::new();
    for sink in sinks {
        let Some(state) = sink.agg.unspilled_global_distinct_state() else {
            return Ok(None);
        };
        if let Some(state) = state {
            states.push(state);
        }
    }
    let Some(first) = states.first() else {
        return Ok(Some(0));
    };
    macro_rules! merge {
        ($variant:ident) => {{
            let mut sources = smallvec::SmallVec::<[_; 16]>::new();
            for state in &states {
                let DistinctState::$variant(keys) = state else {
                    return Ok(None);
                };
                sources.push(keys);
            }
            parallel_distinct::count_union(&sources, budget)
        }};
    }
    match first {
        DistinctState::CountKeys(_) => merge!(CountKeys),
        DistinctState::I64Set(_) => merge!(I64Set),
        DistinctState::U64Set(_) => merge!(U64Set),
        _ => Ok(None),
    }
}

impl BatchOperator for ParallelHashAggOutputStream {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        while self.pending.len() < DEFAULT_BATCH_ROWS && self.workers_remaining > 0 {
            crate::engine::cancel::check_cancelled()?;
            let message = self
                .receiver
                .as_ref()
                .expect("parallel finalize receiver lives with workers")
                .recv()
                .map_err(|_| {
                    crate::storage::error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "parallel hash-aggregate finalize channel closed",
                    )
                })?;
            match message {
                ParallelFinalizeMessage::Rows(rows) => self.pending.extend(rows),
                ParallelFinalizeMessage::Error(error) => return Err(error),
                ParallelFinalizeMessage::Done => {
                    self.workers_remaining = self.workers_remaining.saturating_sub(1);
                }
            }
        }
        if self.pending.is_empty() {
            return Ok(None);
        }
        let take = self.pending.len().min(DEFAULT_BATCH_ROWS);
        let mut columns: Vec<Vec<Value>> = (0..self.schema.fields().len())
            .map(|_| Vec::with_capacity(take))
            .collect();
        for _ in 0..take {
            let row = self.pending.pop_front().expect("pending row count checked");
            for (column, value) in row.into_iter().enumerate() {
                columns[column].push(value);
            }
        }
        let group_count = self.schema.fields().len().saturating_sub(self.aggs.len());
        Ok(Some(Batch::new(
            self.schema.clone(),
            vec_hash_agg::build_agg_output_columns(columns, group_count, &self.aggs),
        )))
    }
}

impl Drop for ParallelHashAggOutputStream {
    fn drop(&mut self) {
        self.abort.store(true, Ordering::Relaxed);
        // Dropping the receiver wakes workers blocked on the bounded channel.
        drop(self.receiver.take());
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

pub fn run_hash_agg_parallel_stream(
    stream: &mut dyn BatchOperator,
    group_columns: Vec<usize>,
    aggs: Vec<AggregateSpec>,
    output_schema: Arc<BatchSchema>,
    input_schema: Arc<BatchSchema>,
    worker_count: usize,
    budget: Option<Arc<MemBudget>>,
    data_dir: PathBuf,
    transform: Option<Arc<dyn HashAggBatchTransform>>,
    final_filter: Option<vec_hash_agg::AggregateFinalFilter>,
) -> Result<(Box<dyn BatchOperator>, AggMetrics, bool), QueryError> {
    run_hash_agg_parallel_stream_with_count_topk(
        stream,
        group_columns,
        aggs,
        output_schema,
        input_schema,
        worker_count,
        budget,
        data_dir,
        transform,
        final_filter,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn run_hash_agg_parallel_stream_with_count_topk(
    stream: &mut dyn BatchOperator,
    group_columns: Vec<usize>,
    mut aggs: Vec<AggregateSpec>,
    output_schema: Arc<BatchSchema>,
    input_schema: Arc<BatchSchema>,
    worker_count: usize,
    budget: Option<Arc<MemBudget>>,
    data_dir: PathBuf,
    transform: Option<Arc<dyn HashAggBatchTransform>>,
    final_filter: Option<vec_hash_agg::AggregateFinalFilter>,
    count_topk: Option<vec_hash_agg::AggregateCountTopKHint>,
) -> Result<(Box<dyn BatchOperator>, AggMetrics, bool), QueryError> {
    vec_hash_agg::bind_aggregate_comparison_domains(&mut aggs, &input_schema);
    let implicit_group = group_columns.is_empty();
    let spill_context = budget.map(|budget| {
        parallel_spill_context(input_schema, budget, data_dir, &group_columns, &aggs)
    });
    let built = build_parallel_stream_sinks(
        stream,
        group_columns,
        aggs.clone(),
        worker_count,
        spill_context.clone(),
        transform,
    )?;
    match built {
        ParallelStreamBuild::Serial(agg) => {
            let metrics = AggMetrics {
                is_streaming: true,
                ..agg.metrics()
            };
            Ok((agg.finalize_stream()?, metrics, false))
        }
        ParallelStreamBuild::Parallel(mut sinks) => {
            // Compact dictionary keys use worker-local tokens while building.
            // Align those small dictionaries once before partition handoff so
            // finalize can merge fixed-width keys directly without rebuilding
            // millions of canonical `GroupBucket` allocations.
            align_worker_dictionaries(
                &mut sinks,
                spill_context.as_ref().map(|context| &context.budget),
            )?;
            let mut metrics = AggMetrics {
                is_streaming: true,
                workers: sinks.len() as u32,
                ..AggMetrics::default()
            };
            for sink in &sinks {
                let worker = sink.agg.metrics();
                metrics.peak_memory_bytes = metrics
                    .peak_memory_bytes
                    .saturating_add(worker.peak_memory_bytes);
                metrics.spill_bytes = metrics.spill_bytes.saturating_add(worker.spill_bytes);
                metrics.spill_runs = metrics.spill_runs.saturating_add(worker.spill_runs);
                metrics.spill_passes = metrics.spill_passes.max(worker.spill_passes);
                metrics.batches = metrics.batches.saturating_add(worker.batches);
                metrics.native_aggregate_rows = metrics
                    .native_aggregate_rows
                    .saturating_add(worker.native_aggregate_rows);
                metrics.fallback_aggregate_rows = metrics
                    .fallback_aggregate_rows
                    .saturating_add(worker.fallback_aggregate_rows);
            }
            let shared_spill = spill_context
                .as_ref()
                .and_then(|context| context.shared_spill.clone());
            metrics.spill_partitions = shared_spill
                .as_ref()
                .map(|spill| spill.written_partition_count() as u64)
                .unwrap_or(0);
            metrics.did_spill = metrics.spill_partitions > 0;
            metrics.aggregate_mode = Some(if metrics.fallback_aggregate_rows > 0 {
                "scalar_batch"
            } else if implicit_group {
                "typed_batch"
            } else {
                "grouped_typed"
            });
            if implicit_group
                && !metrics.did_spill
                && final_filter.is_none()
                && count_topk.is_none()
                && !matches!(
                    crate::engine::executor::vec::config::effective_mode(),
                    crate::engine::executor::vec::config::VecMode::Off
                )
            {
                if let Some(context) = &spill_context {
                    if let Some(count) =
                        try_parallel_global_distinct_count(&sinks, &context.budget)?
                    {
                        drop(sinks);
                        return Ok((
                            Box::new(ParallelHashAggOutputStream {
                                schema: output_schema,
                                aggs,
                                receiver: None,
                                workers: Vec::new(),
                                workers_remaining: 0,
                                abort: Arc::new(AtomicBool::new(false)),
                                pending: std::collections::VecDeque::from([vec![Value::Int(
                                    count as i64,
                                )]]),
                            }),
                            metrics,
                            false,
                        ));
                    }
                }
            }
            Ok((
                Box::new(parallel_finalize_stream(
                    output_schema,
                    aggs,
                    sinks,
                    shared_spill,
                    spill_context.as_ref().map(|context| context.budget.clone()),
                    final_filter.clone(),
                    count_topk,
                )),
                metrics,
                final_filter.is_some(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spilled_finalize_limits_simultaneous_expanded_partitions() {
        let mib = 1024 * 1024;
        assert_eq!(finalize_worker_count(16, Some(112 * mib), 14 * mib), 4);
        assert_eq!(finalize_worker_count(16, Some(112 * mib), 256 * mib), 1);
        assert_eq!(finalize_worker_count(16, Some(112 * mib), 0), 16);
        assert_eq!(finalize_worker_count(16, Some(1024 * mib), 14 * mib), 16);
        assert_eq!(finalize_worker_count(0, Some(0), usize::MAX), 1);
    }

    #[test]
    fn terminal_layout_corrects_a_shared_clustered_first_batch_choice() {
        use super::super::super::{batch::ColumnVector, schema::BatchField};
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(BatchSchema::new(
            ["id", "bucket", "label"]
                .into_iter()
                .map(|name| BatchField {
                    table: "t".into(),
                    alias: None,
                    name: name.into(),
                    column_type: if name == "label" {
                        crate::types::ColumnType::VarString
                    } else {
                        crate::types::ColumnType::LongLong
                    },
                    source: Default::default(),
                })
                .collect(),
        ));
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: vec_hash_agg::AggregateKind::CountStar,
            input_column: 0,
        }];
        let budget = Arc::new(MemBudget::new(512 * 1024 * 1024));
        let context = parallel_spill_context(
            schema.clone(),
            budget.clone(),
            dir.path().to_path_buf(),
            &[0, 1, 2],
            &specs,
        );
        let mut sinks = (0..2)
            .map(|_| HashAggSink {
                agg: make_parallel_worker_agg(vec![0, 1, 2], specs.clone(), Some(&context)),
            })
            .collect::<Vec<_>>();
        for sink in &mut sinks {
            for prefix in [true, false] {
                let n = if prefix { 512 } else { 2000 };
                let batch = Batch::new(
                    schema.clone(),
                    vec![
                        ColumnVector::I64 {
                            data: (0..n).map(|row| if prefix { 0 } else { row + 1 }).collect(),
                            nulls: None,
                        },
                        ColumnVector::I64 {
                            data: (0..n).map(|row| row % 4 - 2).collect(),
                            nulls: None,
                        },
                        ColumnVector::from_values(vec![Value::String("label".into()); n as usize]),
                    ],
                );
                sink.agg.consume_batch(&batch).unwrap();
            }
            assert!(sink.agg.dictionary_token_count(0) > 1000);
        }
        align_worker_dictionaries(&mut sinks, Some(&budget)).unwrap();
        assert!(sinks
            .iter()
            .all(|sink| sink.agg.dictionary_token_count(0) == 0));
        assert!(sinks.iter().all(|sink| !sink.agg.metrics().did_spill));
        let partials = take_parallel_worker_partials(&mut sinks, Some(&context)).unwrap();
        let rows =
            vec_hash_agg::merge_worker_partition_snapshots(partials.partitions, &specs).unwrap();
        assert_eq!(rows.len(), 2004);
        for row in rows {
            assert_eq!(
                row[3],
                Value::Int(if row[0] == Value::Int(0) { 256 } else { 2 })
            );
        }
    }

    #[test]
    fn different_worker_dictionary_layouts_merge_identical_groups_exactly() {
        use super::super::super::{batch::ColumnVector, schema::BatchField};
        for force_spill in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let schema = Arc::new(BatchSchema::new(
                ["id", "bucket", "name"]
                    .iter()
                    .map(|name| BatchField {
                        table: "t".into(),
                        alias: None,
                        name: (*name).into(),
                        column_type: if *name == "name" {
                            crate::types::ColumnType::VarString
                        } else {
                            crate::types::ColumnType::LongLong
                        },
                        source: Default::default(),
                    })
                    .collect(),
            ));
            let specs = vec![AggregateSpec {
                comparison_collation: None,
                func: vec_hash_agg::AggregateKind::CountStar,
                input_column: 0,
            }];
            let budget = Arc::new(MemBudget::new(512 * 1024 * 1024));
            let context = parallel_spill_context(
                schema.clone(),
                budget.clone(),
                dir.path().to_path_buf(),
                &[0, 1, 2],
                &specs,
            );
            let data = Batch::new(
                schema.clone(),
                vec![
                    ColumnVector::I64 {
                        data: (0..70_000).collect(),
                        nulls: None,
                    },
                    ColumnVector::I64 {
                        data: (0..70_000).map(|id| id % 7).collect(),
                        nulls: None,
                    },
                    ColumnVector::from_values(
                        (0..70_000)
                            .map(|id| Value::String(format!("key-{id}")))
                            .collect(),
                    ),
                ],
            );
            let mut sinks = (0..2)
                .map(|_| HashAggSink {
                    agg: make_parallel_worker_agg(vec![0, 1, 2], specs.clone(), Some(&context)),
                })
                .collect::<Vec<_>>();
            for (worker, sink) in sinks.iter_mut().enumerate() {
                // Keep distinct sampled layouts for the terminal/cancellation
                // test; eager correction is exercised separately.
                sink.agg.retain_sampled_dictionary_layout_for_test();
                let seed = Batch::new(
                    schema.clone(),
                    vec![
                        ColumnVector::I64 {
                            data: (0..2048)
                                .map(|id| if worker == 0 { id } else { 0 })
                                .collect(),
                            nulls: None,
                        },
                        ColumnVector::I64 {
                            data: (0..2048)
                                .map(|id| if worker == 0 { 0 } else { id % 60 })
                                .collect(),
                            nulls: None,
                        },
                        ColumnVector::from_values(vec![Value::String("seed".into()); 2048]),
                    ],
                );
                sink.agg.consume_batch(&seed).unwrap();
                sink.agg.consume_batch(&data).unwrap();
            }
            assert_ne!(
                sinks[0].agg.parallel_dictionary_layout(),
                sinks[1].agg.parallel_dictionary_layout()
            );
            let layout = sinks[0].agg.parallel_dictionary_layout().unwrap();
            let used = budget.used();
            {
                let cancel = crate::engine::cancel::CancellationSnapshot::capture().fork_local();
                cancel.cancel_local();
                let _scope = cancel.enter();
                assert_eq!(
                    sinks[1]
                        .agg
                        .normalize_dictionary_layout(&layout)
                        .unwrap_err()
                        .code(),
                    1317
                );
            }
            assert_eq!(budget.used(), used);
            let small = Arc::new(MemBudget::new(1024));
            align_worker_dictionaries(&mut sinks, Some(if force_spill { &small } else { &budget }))
                .unwrap();
            let partials = take_parallel_worker_partials(&mut sinks, Some(&context)).unwrap();
            let rows = vec_hash_agg::merge_worker_partition_snapshots(partials.partitions, &specs)
                .unwrap();
            assert_eq!(
                rows.len(),
                72_107,
                "GROUP BY must emit one row per canonical group"
            );
            for row in rows {
                if matches!(&row[2], Value::String(key) if key.starts_with("key-")) {
                    assert_eq!(row[3], Value::Int(2));
                }
            }
        }
    }

    #[test]
    fn mixed_primitive_and_string_dictionaries_align_exactly_across_workers() {
        use super::super::super::{batch::ColumnVector, schema::BatchField};
        for kind in 1..=3 {
            let dir = tempfile::tempdir().unwrap();
            let schema = Arc::new(BatchSchema::new(
                ["id", "bucket", "name"]
                    .iter()
                    .map(|name| BatchField {
                        table: "t".into(),
                        alias: None,
                        name: (*name).into(),
                        column_type: if *name == "name" {
                            crate::types::ColumnType::VarString
                        } else if *name == "bucket" && kind == 3 {
                            crate::types::ColumnType::Date
                        } else {
                            crate::types::ColumnType::LongLong
                        },
                        source: Default::default(),
                    })
                    .collect(),
            ));
            let specs = vec![AggregateSpec {
                comparison_collation: None,
                func: vec_hash_agg::AggregateKind::CountStar,
                input_column: 0,
            }];
            let budget = Arc::new(MemBudget::new(512 * 1024 * 1024));
            let context = parallel_spill_context(
                schema.clone(),
                budget.clone(),
                dir.path().to_path_buf(),
                &[0, 1, 2],
                &specs,
            );
            let mut sinks = (0..2)
                .map(|_| HashAggSink {
                    agg: make_parallel_worker_agg(vec![0, 1, 2], specs.clone(), Some(&context)),
                })
                .collect::<Vec<_>>();
            for (worker, sink) in sinks.iter_mut().enumerate() {
                for (rows, seed) in [(2048, true), (70_000, false)] {
                    let ids = (0..rows)
                        .map(|row| if worker == 0 { row } else { rows - row - 1 })
                        .collect::<Vec<_>>();
                    let mut bucket = ColumnVector::from_values(
                        ids.iter()
                            .map(|id| {
                                if id % 13 == 0 {
                                    return Value::Null;
                                }
                                match kind {
                                    1 => Value::Int((id % 7) as i64 - 3),
                                    2 => Value::UInt(u64::MAX - (id % 7) as u64),
                                    _ => Value::Date((id % 7) as i32 - 3),
                                }
                            })
                            .collect(),
                    );
                    if kind == 3 {
                        let mut valid =
                            super::super::super::bitmap::Bitmap::new_all_valid(ids.len());
                        for (row, id) in ids.iter().enumerate() {
                            if id % 13 == 0 {
                                valid.set(row, false);
                            }
                        }
                        bucket = ColumnVector::Date32 {
                            data: ids.iter().map(|id| (id % 7) as i32 - 3).collect(),
                            nulls: Some(valid),
                        };
                    }
                    let names = ColumnVector::from_values(
                        ids.iter()
                            .map(|id| {
                                Value::String(if seed {
                                    "seed".into()
                                } else if worker == 0 {
                                    format!("ключ-{id}")
                                } else {
                                    format!("КЛЮЧ-{id}")
                                })
                            })
                            .collect(),
                    );
                    sink.agg
                        .consume_batch(&Batch::new(
                            schema.clone(),
                            vec![
                                ColumnVector::I64 {
                                    data: ids.iter().map(|id| *id as i64).collect(),
                                    nulls: None,
                                },
                                bucket,
                                names,
                            ],
                        ))
                        .unwrap();
                }
            }
            let tag = sinks[0].agg.parallel_dictionary_layout().unwrap()[1].0;
            assert_eq!(tag & 0x7f, kind);
            if kind < 3 {
                assert_eq!(tag & 0x80, 0, "64-bit primitive must use its dictionary");
            }
            align_worker_dictionaries(&mut sinks, Some(&budget)).unwrap();
            let partials = take_parallel_worker_partials(&mut sinks, Some(&context)).unwrap();
            let rows = vec_hash_agg::merge_worker_partition_snapshots(partials.partitions, &specs)
                .unwrap();
            assert_eq!(rows.len(), 72_048);
            assert!(rows.iter().all(|row| row[3] == Value::Int(2)));
        }
    }

    #[test]
    fn terminal_parallel_distinct_matches_existing_partial_merge() {
        use super::super::super::batch::ColumnVector;
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(BatchSchema::new(vec![
            super::super::super::schema::BatchField {
                table: "t".into(),
                alias: None,
                name: "key".into(),
                column_type: crate::types::ColumnType::VarString,
                source: Default::default(),
            },
        ]));
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: vec_hash_agg::AggregateKind::CountDistinct,
            input_column: 0,
        }];
        let budget = Arc::new(MemBudget::new(128 * 1024 * 1024));
        let context = parallel_spill_context(
            schema.clone(),
            budget.clone(),
            dir.path().to_path_buf(),
            &[],
            &specs,
        );
        for unsigned in [false, true] {
            let mut sinks = (0..2)
                .map(|_| HashAggSink {
                    agg: make_parallel_worker_agg(vec![], specs.clone(), Some(&context)),
                })
                .collect::<Vec<_>>();
            for (worker, sink) in sinks.iter_mut().enumerate() {
                let column = if unsigned {
                    ColumnVector::U64 {
                        data: (0..70_000)
                            .map(|key| (key + worker * 35_000) as u64)
                            .collect(),
                        nulls: None,
                    }
                } else {
                    let mut offsets = vec![0];
                    let mut data = Vec::new();
                    for key in 0..70_000 {
                        data.extend_from_slice(format!("key-{}", key + worker * 35_000).as_bytes());
                        offsets.push(data.len() as u32);
                    }
                    ColumnVector::Bytes {
                        offsets,
                        data,
                        nulls: None,
                    }
                };
                sink.agg
                    .consume_batch(&Batch::new(schema.clone(), vec![column]))
                    .unwrap();
            }
            assert_eq!(
                try_parallel_global_distinct_count(&sinks, &budget).unwrap(),
                Some(105_000),
                "unsigned={unsigned}"
            );
            let partials = take_parallel_worker_partials(&mut sinks, Some(&context)).unwrap();
            let rows = vec_hash_agg::merge_worker_partition_snapshots(partials.partitions, &specs)
                .unwrap();
            assert_eq!(rows, vec![vec![Value::Int(105_000)]]);
        }
    }

    #[test]
    fn parallel_dictionary_alignment_keeps_exact_counts_and_pinned_tokens() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(BatchSchema::new(vec![
            super::super::super::schema::BatchField {
                table: "t".into(),
                alias: None,
                name: "id".into(),
                column_type: crate::types::ColumnType::LongLong,
                source: Default::default(),
            },
            super::super::super::schema::BatchField {
                table: "t".into(),
                alias: None,
                name: "name".into(),
                column_type: crate::types::ColumnType::VarString,
                source: Default::default(),
            },
        ]));
        let budget = Arc::new(MemBudget::new(512 * 1024 * 1024));
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: vec_hash_agg::AggregateKind::CountStar,
            input_column: 0,
        }];
        let context = parallel_spill_context(
            schema.clone(),
            budget.clone(),
            dir.path().to_path_buf(),
            &[0, 1],
            &specs,
        );
        let seed = Batch::new(
            schema.clone(),
            vec![
                super::super::super::batch::ColumnVector::I64 {
                    data: vec![0; 2048],
                    nulls: None,
                },
                super::super::super::batch::ColumnVector::from_values(vec![
                    Value::String(
                        "seed".into()
                    );
                    2048
                ]),
            ],
        );
        let data = Batch::new(
            schema.clone(),
            vec![
                super::super::super::batch::ColumnVector::I64 {
                    data: (0..70_000).map(|i| i % 7).collect(),
                    nulls: None,
                },
                super::super::super::batch::ColumnVector::from_values(
                    (0..70_000)
                        .map(|i| Value::String(format!("key-{i}")))
                        .collect(),
                ),
            ],
        );
        let mut sinks = (0..2)
            .map(|_| HashAggSink {
                agg: make_parallel_worker_agg(vec![0, 1], specs.clone(), Some(&context)),
            })
            .collect::<Vec<_>>();
        for sink in &mut sinks {
            sink.agg.consume_batch(&seed).unwrap();
            sink.agg.consume_batch(&data).unwrap();
        }
        assert!(sinks
            .iter()
            .all(|sink| sink.agg.parallel_dictionary_layout().is_some()));
        align_worker_dictionaries(&mut sinks, Some(&budget)).unwrap();
        let partials = take_parallel_worker_partials(&mut sinks, Some(&context)).unwrap();
        drop(sinks);
        let rows =
            vec_hash_agg::merge_worker_partition_snapshots(partials.partitions, &specs).unwrap();
        assert_eq!(rows.len(), 70_001);
        let total = rows
            .iter()
            .map(|row| match row[2] {
                Value::Int(count) => count,
                _ => panic!(),
            })
            .sum::<i64>();
        assert_eq!(total, 2 * (70_000 + 2048));
        assert!(rows
            .iter()
            .all(|row| matches!(row[2], Value::Int(2) | Value::Int(4096))));
    }

    struct OwnedTestWorker {
        batches: Vec<Batch>,
        fail: bool,
    }

    impl crate::engine::executor::vec::stream::ParallelBatchWorker for OwnedTestWorker {
        fn run(
            self: Box<Self>,
            consume: &mut dyn FnMut(Batch) -> Result<(), QueryError>,
            _abort: &AtomicBool,
        ) -> Result<(), QueryError> {
            if self.fail {
                return Err(crate::error::catalog::ER_OUT_OF_RESOURCES.error("test source failure"));
            }
            for batch in self.batches {
                consume(batch)?;
            }
            Ok(())
        }
    }

    struct OwnedTestSource {
        schema: Arc<BatchSchema>,
        workers: Option<Vec<Box<dyn crate::engine::executor::vec::stream::ParallelBatchWorker>>>,
    }

    impl BatchOperator for OwnedTestSource {
        fn schema(&self) -> &Arc<BatchSchema> {
            &self.schema
        }
        fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
            panic!("native partitions must bypass the exchange producer")
        }
        fn take_parallel_workers(
            &mut self,
            degree: usize,
        ) -> Result<
            Option<Vec<Box<dyn crate::engine::executor::vec::stream::ParallelBatchWorker>>>,
            QueryError,
        > {
            assert_eq!(degree, 2);
            Ok(self.workers.take())
        }
    }

    #[test]
    fn owned_scan_partitions_merge_duplicates_and_propagate_failure() {
        let schema = schema_kv();
        for fail in [false, true] {
            let mut source = OwnedTestSource {
                schema: schema.clone(),
                workers: Some(vec![
                    Box::new(OwnedTestWorker {
                        batches: vec![make_batch(&schema, &[(1, 10), (2, 20)])],
                        fail,
                    }),
                    Box::new(OwnedTestWorker {
                        batches: vec![make_batch(&schema, &[(1, 30), (3, 40)])],
                        fail: false,
                    }),
                ]),
            };
            let specs = vec![AggregateSpec {
                comparison_collation: None,
                func: vec_hash_agg::AggregateKind::CountStar,
                input_column: 0,
            }];
            let result =
                build_parallel_stream_sinks(&mut source, vec![0], specs.clone(), 2, None, None);
            if fail {
                assert!(result.is_err());
                continue;
            }
            let ParallelStreamBuild::Parallel(mut sinks) = result.unwrap() else {
                panic!("expected owned workers")
            };
            let output = take_parallel_worker_partials(&mut sinks, None).unwrap();
            let mut rows =
                vec_hash_agg::merge_worker_partition_snapshots(output.partitions, &specs).unwrap();
            rows.sort_by_key(|row| match row[0] {
                Value::Int(key) => key,
                _ => unreachable!(),
            });
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int(1), Value::Int(2)],
                    vec![Value::Int(2), Value::Int(1)],
                    vec![Value::Int(3), Value::Int(1)]
                ]
            );
        }
    }

    #[test]
    fn empty_owned_scan_retains_implicit_aggregate_row() {
        let mut source = OwnedTestSource {
            schema: schema_kv(),
            workers: Some(vec![
                Box::new(OwnedTestWorker {
                    batches: vec![],
                    fail: false,
                }),
                Box::new(OwnedTestWorker {
                    batches: vec![],
                    fail: false,
                }),
            ]),
        };
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: vec_hash_agg::AggregateKind::CountStar,
            input_column: 0,
        }];
        let ParallelStreamBuild::Serial(agg) =
            build_parallel_stream_sinks(&mut source, vec![], specs, 2, None, None).unwrap()
        else {
            panic!("empty scan must finalize one implicit group")
        };
        let mut output = agg.finalize_stream().unwrap();
        let batch = output.next_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.columns()[0].value_at(0), Value::Int(0));
        assert!(output.next_batch().unwrap().is_none());
    }
    use crate::engine::executor::vec::batch::ColumnVector;
    use crate::engine::executor::vec::hash_agg::AggregateKind;
    use crate::engine::executor::vec::schema::{BatchField, BatchSchema};
    use crate::engine::executor::vec::stream::MaterializedBatchOperator;
    use crate::types::ColumnType;

    fn schema_kv() -> Arc<BatchSchema> {
        Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "k".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "v".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
        ]))
    }

    fn make_batch(s: &Arc<BatchSchema>, pairs: &[(i64, i64)]) -> Batch {
        let ks: Vec<Value> = pairs.iter().map(|(k, _)| Value::Int(*k)).collect();
        let vs: Vec<Value> = pairs.iter().map(|(_, v)| Value::Int(*v)).collect();
        Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(ks), ColumnVector::from_values(vs)],
        )
    }

    fn collect_parallel_hash_agg(
        stream: &mut dyn BatchOperator,
        group_columns: Vec<usize>,
        aggs: Vec<AggregateSpec>,
        worker_count: usize,
    ) -> Result<Vec<Vec<Value>>, QueryError> {
        let directory = tempfile::tempdir_in("/tmp/mydb").unwrap();
        collect_parallel_hash_agg_with_metrics(
            stream,
            group_columns,
            aggs,
            worker_count,
            None,
            directory.path().to_path_buf(),
        )
        .map(|(rows, _)| rows)
    }

    fn collect_parallel_hash_agg_with_metrics(
        stream: &mut dyn BatchOperator,
        group_columns: Vec<usize>,
        aggs: Vec<AggregateSpec>,
        worker_count: usize,
        budget: Option<Arc<MemBudget>>,
        data_dir: PathBuf,
    ) -> Result<(Vec<Vec<Value>>, AggMetrics), QueryError> {
        let input_schema = stream.schema().clone();
        let mut fields = group_columns
            .iter()
            .map(|column| input_schema.fields()[*column].clone())
            .collect::<Vec<_>>();
        fields.extend(aggs.iter().enumerate().map(|(index, _)| BatchField {
            table: String::new(),
            alias: None,
            name: format!("aggregate_{index}"),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }));
        let (mut output, metrics, _) = run_hash_agg_parallel_stream(
            stream,
            group_columns,
            aggs,
            Arc::new(BatchSchema::new(fields)),
            input_schema,
            worker_count,
            budget,
            data_dir,
            None,
            None,
        )?;
        let mut rows = Vec::new();
        while let Some(batch) = output.next_batch()? {
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| column.value_at(row))
                        .collect(),
                );
            }
        }
        Ok((rows, metrics))
    }

    #[test]
    fn exec_audit_rollup_single_worker_consumes_all_batches() {
        let schema = schema_kv();
        let batches = vec![
            make_batch(&schema, &[(1, 10), (2, 20)]),
            make_batch(&schema, &[(1, 30)]),
            make_batch(&schema, &[(2, 40), (3, 50)]),
        ];
        for workers in [0, 1, 2] {
            let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let mut stream = MaterializedBatchOperator::new(schema.clone(), batches.clone());
            let result = run_rollup_finest_parallel(
                &mut stream,
                vec![0],
                vec![AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::CountStar,
                    input_column: 0,
                }],
                workers,
                None,
                root.path().to_path_buf(),
            )
            .unwrap()
            .unwrap();
            assert!(stream.next_batch().unwrap().is_none());
            assert_eq!(result.buckets.len(), 3);
            assert_eq!(
                result.buckets.iter().map(|b| b.aggs[0].count).sum::<u64>(),
                5
            );
        }
    }

    #[test]
    fn parallel_matches_serial_count_sum_by_group() {
        let s = schema_kv();
        let batches = vec![
            make_batch(&s, &[(1, 10), (2, 20), (1, 5)]),
            make_batch(&s, &[(2, 8), (3, 100)]),
            make_batch(&s, &[(1, 1), (3, 2), (2, 4)]),
            make_batch(&s, &[(4, 50)]),
        ];

        // Serial via the existing run_hash_agg helper.
        let mut serial_stream = MaterializedBatchOperator::new(s.clone(), batches.clone());
        let serial = vec_hash_agg::run_hash_agg(
            &mut serial_stream,
            vec![0],
            vec![
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::CountStar,
                    input_column: 0,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Sum,
                    input_column: 1,
                },
            ],
        )
        .unwrap();

        // Parallel.
        let mut parallel_stream = MaterializedBatchOperator::new(s, batches);
        let parallel = collect_parallel_hash_agg(
            &mut parallel_stream,
            vec![0],
            vec![
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::CountStar,
                    input_column: 0,
                },
                AggregateSpec {
                    comparison_collation: None,
                    func: AggregateKind::Sum,
                    input_column: 1,
                },
            ],
            4,
        )
        .unwrap();

        fn sort(mut v: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
            v.sort_by_key(|r| match &r[0] {
                Value::Int(x) => *x,
                _ => 0,
            });
            v
        }
        assert_eq!(sort(serial), sort(parallel));
    }

    #[test]
    fn bounded_queue_parallel_path_matches_serial() {
        let schema = schema_kv();
        let pairs: Vec<(i64, i64)> = (0..52_000i64).map(|row| (row % 17, row % 101)).collect();
        let batches: Vec<Batch> = pairs
            .chunks(crate::engine::executor::vec::batch::DEFAULT_BATCH_ROWS)
            .map(|chunk| make_batch(&schema, chunk))
            .collect();
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::Sum,
            input_column: 1,
        }];
        let mut serial_stream = MaterializedBatchOperator::new(schema.clone(), batches.clone());
        let mut expected =
            vec_hash_agg::run_hash_agg(&mut serial_stream, vec![0], specs.clone()).unwrap();
        let mut parallel_stream = MaterializedBatchOperator::new(schema, batches);
        let mut actual =
            collect_parallel_hash_agg(&mut parallel_stream, vec![0], specs, 4).unwrap();
        expected.sort_by_key(|row| format!("{:?}", row[0]));
        actual.sort_by_key(|row| format!("{:?}", row[0]));
        assert_eq!(actual, expected);
    }

    #[test]
    fn parallel_stream_chunks_high_cardinality_output() {
        let schema = schema_kv();
        let row_count = DEFAULT_BATCH_ROWS * 2 + 17;
        let pairs: Vec<(i64, i64)> = (0..row_count as i64).map(|row| (row, 1)).collect();
        let batches: Vec<Batch> = pairs
            .chunks(DEFAULT_BATCH_ROWS)
            .map(|chunk| make_batch(&schema, chunk))
            .collect();
        let mut input = MaterializedBatchOperator::new(schema.clone(), batches);
        let (mut output, _, _) = run_hash_agg_parallel_stream(
            &mut input,
            vec![0],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::CountStar,
                input_column: 1,
            }],
            schema.clone(),
            schema,
            2,
            None,
            tempfile::tempdir().unwrap().path().to_path_buf(),
            None,
            None,
        )
        .unwrap();

        let mut output_batches = 0usize;
        let mut output_rows = 0usize;
        while let Some(batch) = output.next_batch().unwrap() {
            output_batches += 1;
            output_rows += batch.num_rows();
            assert!(batch.num_rows() <= DEFAULT_BATCH_ROWS);
        }
        assert_eq!(output_batches, 3);
        assert_eq!(output_rows, row_count);
    }

    #[derive(Clone)]
    struct DoubleValueTransform {
        schema: Arc<BatchSchema>,
    }

    impl HashAggBatchTransform for DoubleValueTransform {
        fn transform(&self, mut batch: Batch) -> Result<Batch, QueryError> {
            let doubled = (0..batch.num_rows())
                .map(|row| match batch.columns()[1].value_at(row) {
                    Value::Int(value) => Value::Int(value * 2),
                    other => other,
                })
                .collect();
            let selection = batch.selection.take();
            let mut columns = batch.take_columns()?;
            columns.push(ColumnVector::from_values(doubled));
            let mut transformed = Batch::from_owned_columns(self.schema.clone(), columns);
            if let Some(selection) = selection {
                transformed = transformed.with_selection(selection);
            }
            Ok(transformed)
        }
    }

    #[derive(Clone)]
    struct ParamMultiplierTransform {
        schema: Arc<BatchSchema>,
    }

    impl HashAggBatchTransform for ParamMultiplierTransform {
        fn transform(&self, mut batch: Batch) -> Result<Batch, QueryError> {
            let Some(Value::Int(multiplier)) = crate::engine::params::current_param(0) else {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "parallel hash-aggregate worker lost parameter context",
                ));
            };
            if !crate::engine::params::auto_parameterized_active() {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "parallel hash-aggregate worker lost parameter provenance",
                ));
            }
            let multiplied = (0..batch.num_rows())
                .map(|row| match batch.columns()[1].value_at(row) {
                    Value::Int(value) => Value::Int(value * multiplier),
                    other => other,
                })
                .collect();
            let selection = batch.selection.take();
            let mut columns = batch.take_columns()?;
            columns.push(ColumnVector::from_values(multiplied));
            let mut transformed = Batch::from_owned_columns(self.schema.clone(), columns);
            if let Some(selection) = selection {
                transformed = transformed.with_selection(selection);
            }
            Ok(transformed)
        }
    }

    #[test]
    fn parallel_stream_runs_computed_inputs_inside_workers() {
        let base_schema = schema_kv();
        let mut fields = base_schema.fields().to_vec();
        fields.push(BatchField {
            table: "t".into(),
            alias: None,
            name: "v2".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        });
        let input_schema = Arc::new(BatchSchema::new(fields));
        let batches = (0..4)
            .map(|batch| {
                let start = batch * DEFAULT_BATCH_ROWS as i64;
                let rows = (start..start + DEFAULT_BATCH_ROWS as i64)
                    .map(|row| (row % 2, 1))
                    .collect::<Vec<_>>();
                make_batch(&base_schema, &rows)
            })
            .collect();
        let mut input = MaterializedBatchOperator::new(base_schema, batches);
        let output_schema = schema_kv();
        let (mut output, metrics, _) = run_hash_agg_parallel_stream(
            &mut input,
            vec![0],
            vec![AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column: 2,
            }],
            output_schema,
            input_schema.clone(),
            4,
            None,
            tempfile::tempdir().unwrap().path().to_path_buf(),
            Some(Arc::new(DoubleValueTransform {
                schema: input_schema,
            })),
            None,
        )
        .unwrap();
        let mut rows = Vec::new();
        while let Some(batch) = output.next_batch().unwrap() {
            for row in 0..batch.num_rows() {
                rows.push((
                    batch.columns()[0].value_at(row),
                    batch.columns()[1].value_at(row),
                ));
            }
        }
        rows.sort_by_key(|(key, _)| key.to_display_string());
        assert_eq!(metrics.workers, 4);
        assert_eq!(rows.len(), 2);
        let expected = (DEFAULT_BATCH_ROWS * 4).to_string();
        assert!(
            rows.iter()
                .all(|(_, sum)| sum.to_display_string() == expected),
            "computed worker sums diverged: {rows:?}"
        );
    }

    #[test]
    fn parallel_worker_orchestration_propagates_full_param_context() {
        let base_schema = schema_kv();
        let mut fields = base_schema.fields().to_vec();
        fields.push(BatchField {
            table: "t".into(),
            alias: None,
            name: "scaled_v".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        });
        let input_schema = Arc::new(BatchSchema::new(fields));
        let batches = (0..4)
            .map(|batch| {
                let start = batch * DEFAULT_BATCH_ROWS as i64;
                let rows = (start..start + DEFAULT_BATCH_ROWS as i64)
                    .map(|row| (row % 2, 1))
                    .collect::<Vec<_>>();
                make_batch(&base_schema, &rows)
            })
            .collect();
        let mut input = MaterializedBatchOperator::new(base_schema, batches);
        let transform = ParamMultiplierTransform {
            schema: input_schema.clone(),
        };
        let (mut output, metrics, _) =
            crate::engine::params::with_auto_params(vec![Value::Int(3)], || {
                run_hash_agg_parallel_stream(
                    &mut input,
                    vec![0],
                    vec![AggregateSpec {
                        comparison_collation: None,
                        func: AggregateKind::Sum,
                        input_column: 2,
                    }],
                    schema_kv(),
                    input_schema,
                    4,
                    None,
                    tempfile::tempdir().unwrap().path().to_path_buf(),
                    Some(Arc::new(transform)),
                    None,
                )
            })
            .unwrap();

        let mut sums = Vec::new();
        while let Some(batch) = output.next_batch().unwrap() {
            for row in 0..batch.num_rows() {
                sums.push(batch.columns()[1].value_at(row));
            }
        }
        assert_eq!(metrics.workers, 4);
        assert_eq!(sums.len(), 2);
        let expected = (DEFAULT_BATCH_ROWS * 6).to_string();
        assert!(
            sums.iter().all(|sum| sum.to_display_string() == expected),
            "parameterized worker sums diverged: {sums:?}"
        );
    }

    #[test]
    fn parallel_workers_spill_and_merge_raw_states() {
        let schema = schema_kv();
        let pairs: Vec<(i64, i64)> = (0..52_000i64).map(|row| (row % 4_096, row % 101)).collect();
        let batches: Vec<Batch> = pairs
            .chunks(crate::engine::executor::vec::batch::DEFAULT_BATCH_ROWS)
            .map(|chunk| make_batch(&schema, chunk))
            .collect();
        let specs = vec![
            AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column: 1,
            },
            AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Avg,
                input_column: 1,
            },
        ];
        let mut serial_stream = MaterializedBatchOperator::new(schema.clone(), batches.clone());
        let mut expected =
            vec_hash_agg::run_hash_agg(&mut serial_stream, vec![0], specs.clone()).unwrap();

        let root = tempfile::tempdir().unwrap();
        let budget = Arc::new(MemBudget::new(128 * 1024));
        let mut parallel_stream = MaterializedBatchOperator::new(schema.clone(), batches);
        let (mut actual, metrics) =
            crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
                collect_parallel_hash_agg_with_metrics(
                    &mut parallel_stream,
                    vec![0],
                    specs,
                    4,
                    Some(budget),
                    root.path().to_path_buf(),
                )
            })
            .unwrap();
        expected.sort_by_key(|row| format!("{:?}", row[0]));
        actual.sort_by_key(|row| format!("{:?}", row[0]));
        assert_eq!(actual, expected);
        assert!(metrics.did_spill, "tight shared budget must spill");
        assert!(metrics.spill_bytes > 0);
        assert!(
            metrics.spill_runs <= vec_hash_agg::NUM_PARTITIONS as u64,
            "one physical aggregate may create at most one first-level file per radix partition: {:?}",
            metrics
        );
        let tmp = root.path().join("tmp");
        assert!(
            !tmp.exists() || std::fs::read_dir(tmp).unwrap().next().is_none(),
            "worker spill directories must be removed after merge"
        );
    }

    #[test]
    fn parallel_workers_share_one_physical_work_area_registration() {
        let schema = schema_kv();
        let budget = Arc::new(MemBudget::new(8 * 1024 * 1024));
        let root = tempfile::tempdir().unwrap();
        let before = crate::engine::executor::work_area::snapshot().active_handles;
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::CountStar,
            input_column: 1,
        }];
        let context =
            parallel_spill_context(schema, budget, root.path().to_path_buf(), &[0], &specs);
        assert!(context.shared_work_area.is_some());
        assert_eq!(
            crate::engine::executor::work_area::snapshot().active_handles,
            before + 1
        );
        let workers: Vec<HashAggregateCore> = (0..8)
            .map(|_| make_worker_agg(vec![0], specs.clone(), Some(&context)))
            .collect();
        assert_eq!(
            crate::engine::executor::work_area::snapshot().active_handles,
            before + 1,
            "worker shards must not register private grants"
        );
        drop(workers);
        drop(context);
        assert_eq!(
            crate::engine::executor::work_area::snapshot().active_handles,
            before
        );
    }

    #[test]
    fn parallel_worker_failure_unblocks_full_producer_queue() {
        let schema = schema_kv();
        let pairs: Vec<(i64, i64)> = (0..52_000i64).map(|row| (row, row % 101)).collect();
        let batches: Vec<Batch> = pairs
            .chunks(crate::engine::executor::vec::batch::DEFAULT_BATCH_ROWS)
            .map(|chunk| make_batch(&schema, chunk))
            .collect();
        let specs = vec![AggregateSpec {
            comparison_collation: None,
            func: AggregateKind::Sum,
            input_column: 1,
        }];

        // A regular file cannot host `<data_dir>/tmp/hashagg-*`. With a
        // tiny budget every worker attempts to spill its first high-cardinality
        // batch and fails while the producer still has more than the bounded
        // queue capacity left to send. The driver must return that error rather
        // than wait forever on a full SyncSender.
        let invalid_root = tempfile::NamedTempFile::new().unwrap();
        let budget = Arc::new(MemBudget::new(1024));
        let mut stream = MaterializedBatchOperator::new(schema, batches);
        let result = crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
            collect_parallel_hash_agg_with_metrics(
                &mut stream,
                vec![0],
                specs,
                2,
                Some(budget),
                invalid_root.path().to_path_buf(),
            )
        });
        assert!(result.is_err(), "invalid spill root must be propagated");
    }

    #[test]
    fn rollup_parallel_spill_preserves_finest_raw_states() {
        let schema = schema_kv();
        let pairs: Vec<(i64, i64)> = (0..52_000i64).map(|row| (row % 4_096, row % 101)).collect();
        let batches: Vec<Batch> = pairs
            .chunks(crate::engine::executor::vec::batch::DEFAULT_BATCH_ROWS)
            .map(|chunk| make_batch(&schema, chunk))
            .collect();
        let specs = vec![
            AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Sum,
                input_column: 1,
            },
            AggregateSpec {
                comparison_collation: None,
                func: AggregateKind::Avg,
                input_column: 1,
            },
        ];
        let mut serial_stream = MaterializedBatchOperator::new(schema.clone(), batches.clone());
        let mut expected =
            vec_hash_agg::run_hash_agg(&mut serial_stream, vec![0], specs.clone()).unwrap();

        let root = tempfile::tempdir().unwrap();
        let budget = Arc::new(MemBudget::new(128 * 1024));
        let mut parallel_stream = MaterializedBatchOperator::new(schema, batches);
        let result = crate::engine::executor::mem_budget::with_query_budget(budget.clone(), || {
            run_rollup_finest_parallel(
                &mut parallel_stream,
                vec![0],
                specs,
                4,
                Some(budget),
                root.path().to_path_buf(),
            )
        })
        .unwrap()
        .expect("non-empty finest grouping");
        let mut actual: Vec<Vec<Value>> = result
            .buckets
            .into_iter()
            .map(|bucket| {
                let sum = crate::engine::executor::select::aggregate::numeric_state_to_value(
                    &bucket.aggs[0].sum,
                );
                let avg = crate::engine::executor::select::aggregate::finalize_avg_value(
                    &bucket.aggs[1].sum,
                    bucket.aggs[1].count,
                );
                vec![bucket.key_values[0].clone(), sum, avg]
            })
            .collect();
        expected.sort_by_key(|row| format!("{:?}", row[0]));
        actual.sort_by_key(|row| format!("{:?}", row[0]));
        assert_eq!(actual, expected);
        assert!(result.metrics.did_spill, "tight shared budget must spill");
        assert!(result.metrics.spill_bytes > 0);
        let tmp = root.path().join("tmp");
        assert!(
            !tmp.exists() || std::fs::read_dir(tmp).unwrap().next().is_none(),
            "ROLLUP worker spill directories must be removed after merge"
        );
    }
}
