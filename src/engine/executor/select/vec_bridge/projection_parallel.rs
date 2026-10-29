//! Fuse diagnostic-free projection into already-planned native scan workers.

use super::*;
use crate::engine::executor::vec::stream::ParallelBatchWorker;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

pub(super) fn try_parallel_projection<'a>(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
    source: &mut Box<dyn BatchOperator + 'a>,
    projection: &[VecResultProjection],
    template: &[PlanField],
) -> Result<Option<Box<dyn BatchOperator + 'a>>, QueryError> {
    if matches!(
        crate::engine::executor::vec::config::effective_mode(),
        crate::engine::executor::vec::config::VecMode::Off
    ) || !source.is_native_parallel_scan()
        || !matches!(source.properties().ordering, BatchOrdering::Unordered)
        || !projection
            .iter()
            .any(|item| matches!(item, VecResultProjection::Expr(_)))
        || !projection.iter().all(|item| match item {
            VecResultProjection::Column(_) => true,
            VecResultProjection::Expr(program) => program.column_parallel_safe(source.schema()),
            VecResultProjection::Scalar(_) => false,
        })
    {
        return Ok(None);
    }
    let degree = match source.properties().partitioning {
        BatchPartitioning::RoundRobin { partitions } => partitions,
        _ => return Ok(None),
    };
    if degree <= 1 {
        return Ok(None);
    }
    // Explicitly account for the OS worker stacks and immutable descriptors
    // before transferring the source. Rejection leaves the source unopened.
    const STACK_BYTES: usize = 4 * 1024 * 1024;
    let bytes = degree
        .saturating_mul(STACK_BYTES + std::mem::size_of::<std::thread::JoinHandle<()>>())
        .saturating_add(std::mem::size_of_val(projection));
    let Some(charge) = mem_budget::ControlledMemoryCharge::try_new_optional(
        mem_budget::ControlledMemoryCategory::StorageScratch,
        bytes,
    ) else {
        return Ok(None);
    };
    let Some(workers) = source.take_parallel_workers(degree)? else {
        return Ok(None);
    };
    let context =
        super::super::execution_context::ExecutionContext::capture(txn).fork_worker_cancellation();
    Ok(Some(Box::new(ParallelProjection {
        db: db.clone(),
        schema: template_to_batch_schema(template),
        projection: Arc::new(projection.to_vec()),
        pending: Some(workers),
        execution: ScanWorkers::new(context, "parallel projection worker panicked"),
        abort: Arc::new(AtomicBool::new(false)),
        active: Arc::new(AtomicUsize::new(0)),
        key: analyze_stats::plan_key(peel_projection(plan)),
        _charge: Arc::new(charge),
    })))
}

struct ParallelProjection {
    db: Arc<Database>,
    schema: Arc<BatchSchema>,
    projection: Arc<Vec<VecResultProjection>>,
    pending: Option<Vec<Box<dyn ParallelBatchWorker>>>,
    execution: ScanWorkers,
    abort: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
    key: Option<crate::planner::PlanNodeId>,
    _charge: Arc<mem_budget::ControlledMemoryCharge>,
}

fn project(
    batch: Batch,
    projection: &[VecResultProjection],
    schema: &Arc<BatchSchema>,
) -> Result<Batch, QueryError> {
    crate::engine::cancel::check_cancelled()?;
    let rows = batch.num_rows();
    let selection = batch.selection().cloned();
    let mut retained = batch.memory_charge();
    retained.add_force(
        projection
            .len()
            .saturating_mul(std::mem::size_of::<ColumnVector>() * 2),
    )?;
    let mut computed = projection
        .iter()
        .map(|item| match item {
            VecResultProjection::Expr(program) => {
                let reserve = program.column_retained_bound(&batch).unwrap_or(0);
                retained.add_force(reserve)?;
                let column = expr_batch::eval_to_column(program, &batch)?;
                let actual = column.memory_bytes();
                if actual > reserve {
                    retained.add_force(actual - reserve)?;
                } else {
                    retained.release_bytes(reserve - actual);
                }
                Ok(Some(column))
            }
            _ => Ok(None),
        })
        .collect::<Result<Vec<_>, QueryError>>()?;
    let mut uses = vec![0usize; batch.columns().len()];
    for item in projection {
        if let VecResultProjection::Column(index) = item {
            uses[*index] += 1;
        }
    }
    let (columns, input_charge) = batch.into_columns()?.into_parts();
    let mut columns = columns.into_iter().map(Some).collect::<Vec<_>>();
    let output = projection
        .iter()
        .enumerate()
        .map(|(slot, item)| match item {
            VecResultProjection::Column(index) => {
                uses[*index] -= 1;
                if uses[*index] == 0 {
                    Ok(columns[*index].take().expect("projected input column"))
                } else {
                    let column = columns[*index]
                        .as_ref()
                        .expect("shared projected input column");
                    retained.add_force(column.memory_bytes())?;
                    Ok(column.clone())
                }
            }
            VecResultProjection::Expr(_) => {
                Ok(computed[slot].take().expect("computed projection column"))
            }
            VecResultProjection::Scalar(_) => unreachable!("parallel projection is fully bound"),
        })
        .collect::<Result<Vec<_>, QueryError>>()?;
    let mut output = Batch::with_row_count(schema.clone(), output, rows);
    output.selection = selection;
    let output = output.with_shared_charge(
        crate::engine::executor::vec::batch::BatchCharge::shared(retained)?,
    )?;
    if let Some(owner) = input_charge {
        output.with_shared_charge(owner)
    } else {
        Ok(output)
    }
}

impl ParallelProjection {
    fn start(&mut self) -> Result<(), QueryError> {
        let Some(workers) = self.pending.take() else {
            return Ok(());
        };
        self.execution.txn_pressure = self
            .execution
            .worker_context
            .enter_parallel_read_pressure()?;
        let (tx, rx) = std::sync::mpsc::sync_channel(workers.len().saturating_mul(2).max(1));
        self.execution.receiver = Some(rx);
        for worker in workers {
            let tx = tx.clone();
            let db = self.db.clone();
            let context = self.execution.worker_context.clone();
            let projection = self.projection.clone();
            let schema = self.schema.clone();
            let abort = self.abort.clone();
            let active = self.active.clone();
            let key = self.key;
            let charge = self._charge.clone();
            let handle = std::thread::Builder::new()
                .name("mydb-projection".into())
                .stack_size(4 * 1024 * 1024)
                .spawn(move || {
                    let _charge = charge;
                    let mut observed = false;
                    let result = worker.run(
                        &mut |batch| {
                            context.run(&db, || {
                                context.cancellation().check()?;
                                let rows = batch.visible_rows();
                                if rows == 0 {
                                    return Ok(());
                                }
                                if !observed {
                                    observed = true;
                                    active.fetch_add(1, Ordering::Relaxed);
                                }
                                let output = project(batch, &projection, &schema)?;
                                analyze_stats::add_parallel_projection_runtime(
                                    key,
                                    rows as u64,
                                    active.load(Ordering::Relaxed) as u32,
                                );
                                if !send_parallel_scan_message(&tx, &context, Ok(output)) {
                                    return Err(crate::engine::cancel::interrupted_error());
                                }
                                Ok(())
                            })
                        },
                        &abort,
                    );
                    if let Err(error) = result {
                        abort.store(true, Ordering::Relaxed);
                        let _ = send_parallel_scan_message(
                            &tx,
                            &context,
                            Err(context.terminal_error(error)),
                        );
                    }
                })
                .map_err(|error| {
                    crate::error::catalog::ER_OUTOFMEMORY
                        .error(format!("cannot start projection worker: {error}"))
                })?;
            self.execution.workers.push(handle);
        }
        drop(tx);
        Ok(())
    }
}

impl BatchOperator for ParallelProjection {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.start()?;
        self.execution.next_batch()
    }
}

impl Drop for ParallelProjection {
    fn drop(&mut self) {
        self.abort.store(true, Ordering::Relaxed);
    }
}
