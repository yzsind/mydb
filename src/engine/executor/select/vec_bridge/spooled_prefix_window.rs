//! Fixed-width prefix windows over bounded, columnar external-sort runs.
use super::*;
use crate::engine::executor::dispatch::WindowPrefixState;
use crate::engine::executor::vec::sort::{ExternalSortBatchOperator, SortKeySource};
use crate::parser::{WindowFrameBound, WindowFrameExclusion, WindowFrameUnits};

pub(super) fn supported(
    template: &[PlanField],
    windows: &[crate::planner::PhysicalWindowSpec],
) -> bool {
    let Some(first) = windows.first() else {
        return false;
    };
    !first.partition_by.is_empty()
        && template.iter().all(|field| compact_window::fixed_width(field.column_type).is_some())
        && first.partition_by.iter().all(|expr| expr_column_index_in_template(expr, template).is_some())
        && first.order_by.iter().all(|order| expr_column_index_in_template(&order.expr, template).is_some())
        && windows.iter().all(|window| {
            window.partition_by == first.partition_by && window.order_by == first.order_by
                && window.frame.as_ref().is_some_and(|frame| {
                    matches!(frame.units, WindowFrameUnits::Rows)
                        && matches!(frame.start, WindowFrameBound::UnboundedPreceding)
                        && matches!(frame.end, WindowFrameBound::CurrentRow)
                        && matches!(frame.exclusion, WindowFrameExclusion::NoOthers)
                })
                && WindowPrefixState::new(&window.name,
                    matches!(window.args.as_slice(), [Expr::Wildcard])).is_some()
                && matches!(window.args.as_slice(), [argument]
                    if (window.name.eq_ignore_ascii_case("COUNT") && matches!(argument, Expr::Wildcard))
                        || expr_column_index_in_template(argument, template).is_some())
        })
}

struct OrdinalInput<'a> {
    input: Box<dyn BatchOperator + 'a>,
    schema: Arc<BatchSchema>,
    next: i64,
    charge: Option<mem_budget::ScopedCharge>,
}

impl BatchOperator for OrdinalInput<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.charge = None;
        let Some(batch) = self.input.next_batch()? else {
            return Ok(None);
        };
        let rows = batch.num_rows();
        let count = i64::try_from(rows)
            .ok()
            .and_then(|count| self.next.checked_add(count))
            .ok_or_else(|| mem_budget::allocation_error("window input ordinal overflow"))?;
        self.charge = Some(mem_budget::force_charge_scoped(rows.saturating_mul(8))?);
        let ordinals = (self.next..count).collect();
        self.next = count;
        let selection = batch.selection().cloned();
        let mut columns = batch.into_columns()?;
        columns.push(ColumnVector::I64 {
            data: ordinals,
            nulls: None,
        });
        let output = Batch::from_owned_columns(self.schema.clone(), columns);
        Ok(Some(match selection {
            Some(selection) => output.with_selection(selection),
            None => output,
        }))
    }
}

struct PrefixPartitions<'a> {
    input: Box<dyn BatchOperator + 'a>,
    schema: Arc<BatchSchema>,
    plan: &'a PhysicalPlan,
    partition_columns: Vec<usize>,
    argument_columns: Vec<Option<usize>>,
    states: Vec<WindowPrefixState>,
    previous_key: Vec<u8>,
    key: Vec<u8>,
    started_partition: bool,
    pending: Option<Batch>,
    visible: Vec<usize>,
    position: usize,
    partition_rows: u64,
    peak_partition_rows: u64,
    total_rows: u64,
    charge: Option<mem_budget::ScopedCharge>,
    finished: bool,
}

impl BatchOperator for PrefixPartitions<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.charge = None;
        if self.finished {
            return Ok(None);
        }
        loop {
            if self.position < self.visible.len() {
                break;
            }
            self.pending = self.input.next_batch()?;
            let Some(batch) = self.pending.as_ref() else {
                self.finished = true;
                analyze_stats::add_window_runtime(
                    self.plan,
                    "columnar_prefix_spool",
                    self.total_rows,
                    self.peak_partition_rows,
                );
                return Ok(None);
            };
            self.visible.clear();
            self.visible.extend(batch.visible_indices());
            self.position = 0;
        }
        crate::engine::cancel::check_cancelled()?;
        let end = self.visible.len().min(
            self.position
                .saturating_add(scan_decode_batch_rows(&self.schema)),
        );
        let rows = end - self.position;
        // Reserve conversion scratch and output before allocating. The input
        // sort retains only its governed merge batch; this consumer retains
        // constant accumulator state even for a single enormous partition.
        let bytes = rows
            .saturating_mul(self.schema.len())
            .saturating_mul(std::mem::size_of::<Value>())
            .saturating_mul(2);
        let mut charge = mem_budget::force_charge_scoped(bytes)?;
        let mut values = (0..self.states.len())
            .map(|_| Vec::with_capacity(rows))
            .collect::<Vec<_>>();
        let batch = self.pending.as_ref().expect("prefix input batch present");
        let indices = &self.visible[self.position..end];
        for &row in indices {
            self.key.clear();
            for &column in &self.partition_columns {
                crate::engine::executor::vec::key_encode::encode_from_column_row_with_collation(
                    &batch.columns()[column],
                    row,
                    batch.schema.fields()[column].source.collation_id,
                    &mut self.key,
                );
            }
            if !self.started_partition || self.key != self.previous_key {
                self.previous_key.clone_from(&self.key);
                self.started_partition = true;
                self.partition_rows = 0;
                for state in &mut self.states {
                    state.reset()
                }
            }
            self.partition_rows += 1;
            self.peak_partition_rows = self.peak_partition_rows.max(self.partition_rows);
            self.total_rows += 1;
            for ((state, argument), values) in self
                .states
                .iter_mut()
                .zip(&self.argument_columns)
                .zip(&mut values)
            {
                state.update(
                    argument.map_or(Value::Null, |column| batch.columns()[column].value_at(row)),
                )?;
                values.push(state.value());
            }
        }
        let indices = indices.iter().map(|&row| row as u32).collect::<Vec<_>>();
        let mut columns = batch
            .columns()
            .iter()
            .map(|column| column.take_indices(&indices))
            .collect::<Vec<_>>();
        columns.extend(values.into_iter().map(ColumnVector::from_values));
        self.position = end;
        let output = Batch::with_row_count(self.schema.clone(), columns, rows);
        let actual = output.memory_bytes();
        if actual > bytes {
            charge.add_force(actual - bytes)?
        } else {
            charge.release_bytes(bytes - actual)
        }
        self.charge = Some(charge);
        Ok(Some(output))
    }
}

struct RemoveOrdinal<'a> {
    input: Box<dyn BatchOperator + 'a>,
    schema: Arc<BatchSchema>,
    properties: BatchProperties,
    column: usize,
}
impl BatchOperator for RemoveOrdinal<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn properties(&self) -> BatchProperties {
        self.properties.clone()
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        let Some(batch) = self.input.next_batch()? else {
            return Ok(None);
        };
        let selection = batch.selection().cloned();
        let mut columns = batch.into_columns()?;
        columns.remove(self.column);
        let output = Batch::from_owned_columns(self.schema.clone(), columns);
        Ok(Some(match selection {
            Some(selection) => output.with_selection(selection),
            None => output,
        }))
    }
}

fn prepare_sorted_prefix<'a>(
    input: Box<dyn BatchOperator + 'a>,
    template: &[PlanField],
    windows: &[crate::planner::PhysicalWindowSpec],
    output_schema: Arc<BatchSchema>,
    properties: BatchProperties,
    plan: &'a PhysicalPlan,
    data_dir: &std::path::Path,
    restore_order: bool,
) -> Box<dyn BatchOperator + 'a> {
    debug_assert!(supported(template, windows));
    let ordinal = template.len();
    let mut ordinal_name = "__mydb_prefix_window_ordinal".to_string();
    while output_schema
        .fields()
        .iter()
        .any(|field| field.name.eq_ignore_ascii_case(&ordinal_name))
    {
        ordinal_name.push('_')
    }
    let mut input_fields = input.schema().fields().to_vec();
    input_fields.push(BatchField {
        table: String::new(),
        alias: None,
        name: ordinal_name,
        column_type: ColumnType::LongLong,
        source: Default::default(),
    });
    let input_schema = Arc::new(BatchSchema::new(input_fields));
    let input = OrdinalInput {
        input,
        schema: input_schema.clone(),
        next: 0,
        charge: None,
    };
    let partition_columns = windows[0]
        .partition_by
        .iter()
        .map(|expr| expr_column_index_in_template(expr, template).expect("bound partition column"))
        .collect::<Vec<_>>();
    let mut sources = partition_columns
        .iter()
        .copied()
        .map(SortKeySource::Column)
        .collect::<Vec<_>>();
    let mut fields = partition_columns
        .iter()
        .map(|&column| crate::engine::executor::sort::SortField {
            ascending: true,
            nulls_first: true,
            collation_id: input_schema.fields()[column].source.collation_id,
        })
        .collect::<Vec<_>>();
    for order in &windows[0].order_by {
        let column =
            expr_column_index_in_template(&order.expr, template).expect("bound order column");
        sources.push(SortKeySource::Column(column));
        let mut field = crate::engine::executor::sort::SortField::from_order_by(order);
        field.collation_id = input_schema.fields()[column].source.collation_id;
        fields.push(field);
    }
    let ordinal_field = crate::engine::executor::sort::SortField {
        ascending: true,
        nulls_first: true,
        collation_id: None,
    };
    sources.push(SortKeySource::Column(ordinal));
    fields.push(ordinal_field);
    let sorted = ExternalSortBatchOperator::new(
        Some(plan),
        Box::new(input),
        input_schema.clone(),
        BatchProperties::UNORDERED_SINGLE,
        sources,
        fields,
        data_dir,
    );
    let mut output_fields = input_schema.fields().to_vec();
    output_fields.extend_from_slice(&output_schema.fields()[ordinal..]);
    let schema = Arc::new(BatchSchema::new(output_fields));
    let prefix = PrefixPartitions {
        input: Box::new(sorted),
        schema: schema.clone(),
        plan,
        partition_columns,
        argument_columns: windows
            .iter()
            .map(|window| expr_column_index_in_template(&window.args[0], template))
            .collect(),
        states: windows
            .iter()
            .map(|window| {
                WindowPrefixState::new(
                    &window.name,
                    matches!(window.args.as_slice(), [Expr::Wildcard]),
                )
                .expect("prefix function supported")
            })
            .collect(),
        previous_key: Vec::new(),
        key: Vec::new(),
        started_partition: false,
        pending: None,
        visible: Vec::new(),
        position: 0,
        partition_rows: 0,
        peak_partition_rows: 0,
        total_rows: 0,
        charge: None,
        finished: false,
    };
    let result: Box<dyn BatchOperator + 'a> = if restore_order {
        Box::new(ExternalSortBatchOperator::new(
            Some(plan),
            Box::new(prefix),
            schema,
            properties.clone(),
            vec![SortKeySource::Column(ordinal)],
            vec![ordinal_field],
            data_dir,
        ))
    } else {
        // A relation materializer accepts unordered output. Keep the ordinal
        // as a stable ROWS peer tie-breaker but avoid sorting the full result
        // again solely to restore an order its consumer does not require.
        Box::new(prefix)
    };
    Box::new(RemoveOrdinal {
        input: result,
        schema: output_schema,
        properties,
        column: ordinal,
    })
}

struct HashPartitions<'a> {
    input: Option<Box<dyn BatchOperator + 'a>>,
    input_schema: Arc<BatchSchema>,
    schema: Arc<BatchSchema>,
    properties: BatchProperties,
    template: Vec<PlanField>,
    windows: Vec<crate::planner::PhysicalWindowSpec>,
    plan: &'a PhysicalPlan,
    data_dir: std::path::PathBuf,
    dir: Option<crate::engine::executor::select::grace::SpillTmpDir>,
    output: Option<Box<dyn BatchOperator + 'a>>,
    partition: usize,
    worker_context: Option<(
        Arc<Database>,
        super::super::execution_context::ExecutionContext,
    )>,
    parallel_output: Option<ParallelPartitionOutput>,
}

const WINDOW_PARTITIONS: usize = 64;

#[cfg(test)]
static PARALLEL_PREFIX_PARTITION_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

enum PartitionMessage {
    Batch(Batch, mem_budget::ScopedCharge),
    Error(QueryError),
    Done,
}

/// Unordered relation output may interleave independent SQL partitions. Each
/// partition keeps its original peer ordinals and canonical prefix evaluator.
struct ParallelPartitionOutput {
    schema: Arc<BatchSchema>,
    receiver: Option<std::sync::mpsc::Receiver<PartitionMessage>>,
    workers: Vec<std::thread::JoinHandle<()>>,
    remaining: usize,
    abort: Arc<std::sync::atomic::AtomicBool>,
    current_charge: Option<mem_budget::ScopedCharge>,
    _stacks: mem_budget::ControlledMemoryCharge,
}

impl BatchOperator for ParallelPartitionOutput {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.current_charge = None;
        while self.remaining != 0 {
            crate::engine::cancel::check_cancelled()?;
            match self
                .receiver
                .as_ref()
                .expect("active partition receiver")
                .recv_timeout(std::time::Duration::from_millis(10))
            {
                Ok(PartitionMessage::Batch(batch, charge)) => {
                    self.current_charge = Some(charge);
                    return Ok(Some(batch));
                }
                Ok(PartitionMessage::Error(error)) => return Err(error),
                Ok(PartitionMessage::Done) => self.remaining -= 1,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(crate::error::catalog::ER_GET_ERRNO
                        .error("window partition worker ended unexpectedly"))
                }
            }
        }
        Ok(None)
    }
}

impl Drop for ParallelPartitionOutput {
    fn drop(&mut self) {
        self.abort.store(true, std::sync::atomic::Ordering::Release);
        self.receiver = None;
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

impl HashPartitions<'_> {
    fn start_parallel_partitions(&mut self) -> Result<Option<ParallelPartitionOutput>, QueryError> {
        let Some((db, context)) = self.worker_context.take() else {
            return Ok(None);
        };
        let dir = self.dir.as_ref().expect("partition files created");
        let mut total_bytes = 0usize;
        let mut largest = 0usize;
        for partition in 0..WINDOW_PARTITIONS {
            let bytes = dir
                .path()
                .join(format!("partition-{partition}"))
                .metadata()
                .map_or(0, |metadata| metadata.len() as usize);
            total_bytes = total_bytes.saturating_add(bytes);
            largest = largest.max(bytes);
        }
        if total_bytes < 1024 * 1024 {
            return Ok(None);
        }
        let tuning = crate::engine::executor::tuning::current_or_capture();
        let limit = mem_budget::current_budget().map_or(8 * 1024 * 1024, |budget| budget.limit());
        // Divide one existing work area, leaving half for queued output and
        // ancestors. Skewed files reduce admission before any worker starts.
        let per_partition = largest.saturating_mul(5).max(8 * 1024 * 1024);
        let count = tuning
            .scheduler_parallelism
            .min(WINDOW_PARTITIONS)
            .min((limit / 2 / per_partition).max(1));
        if count < 2 {
            return Ok(None);
        }
        const STACK_BYTES: usize = 2 * 1024 * 1024;
        let Ok(stacks) = mem_budget::ControlledMemoryCharge::try_new_recoverable(
            mem_budget::ControlledMemoryCategory::Executor,
            count * (STACK_BYTES + 4096),
        ) else {
            return Ok(None);
        };
        let quota = (limit / 2 / count).max(32 * 1024);
        let paths = Arc::new(
            (0..WINDOW_PARTITIONS)
                .map(|partition| dir.path().join(format!("partition-{partition}")))
                .collect::<Vec<_>>(),
        );
        let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let plan = Arc::new(self.plan.clone());
        let (sender, receiver) = std::sync::mpsc::sync_channel(count * 2);
        let abort = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut output = ParallelPartitionOutput {
            schema: self.schema.clone(),
            receiver: Some(receiver),
            workers: Vec::new(),
            remaining: count,
            abort: abort.clone(),
            current_charge: None,
            _stacks: stacks,
        };
        for _ in 0..count {
            let db = db.clone();
            let context = context
                .clone()
                .with_scheduler_parallelism(1)
                .fork_worker_cancellation();
            let next = next.clone();
            let paths = paths.clone();
            let plan = plan.clone();
            let template = self.template.clone();
            let windows = self.windows.clone();
            let schema = self.schema.clone();
            let input_schema = self.input_schema.clone();
            let charger = dir.charger();
            let data_dir = self.data_dir.clone();
            let sender = sender.clone();
            let abort = abort.clone();
            let mut worker_tuning = tuning.clone();
            worker_tuning.scheduler_parallelism = 1;
            worker_tuning.sort_run_bytes = Some(quota / 4);
            let worker = std::thread::Builder::new()
                .name("mydb-window-spill".into())
                .stack_size(STACK_BYTES)
                .spawn(move || {
                    let result = context.run(&db, || {
                        let _tuning = crate::engine::executor::tuning::enter(worker_tuning);
                        let run = || -> Result<(), QueryError> {
                            loop {
                                if abort.load(std::sync::atomic::Ordering::Acquire) {
                                    return Ok(());
                                }
                                crate::engine::cancel::check_cancelled()?;
                                let partition =
                                    next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                if partition >= paths.len() {
                                    return Ok(());
                                }
                                let input =
                                    Box::new(super::grace_join::VecJoinSpillBatchStream::open(
                                        &paths[partition],
                                        input_schema.clone(),
                                        charger.clone(),
                                    )?);
                                match compact_window::prepare_with_limit(
                                    input,
                                    &template,
                                    &windows,
                                    schema.clone(),
                                    &plan,
                                    quota,
                                )? {
                                    compact_window::Preparation::Ready(batches, mut charge) => {
                                        for batch in batches {
                                            let bytes = batch.memory_bytes();
                                            if bytes > charge.charged() {
                                                charge.add_force(bytes - charge.charged())?;
                                            }
                                            let held = charge.split(bytes);
                                            if sender
                                                .send(PartitionMessage::Batch(batch, held))
                                                .is_err()
                                            {
                                                return Ok(());
                                            }
                                        }
                                    }
                                    compact_window::Preparation::Streaming(input) => {
                                        let mut stream = prepare_sorted_prefix(
                                            input,
                                            &template,
                                            &windows,
                                            schema.clone(),
                                            BatchProperties::UNORDERED_SINGLE,
                                            &plan,
                                            &data_dir,
                                            false,
                                        );
                                        while let Some(batch) = stream.next_batch()? {
                                            let held = mem_budget::force_charge_scoped(
                                                batch.memory_bytes(),
                                            )?;
                                            if sender
                                                .send(PartitionMessage::Batch(batch, held))
                                                .is_err()
                                            {
                                                return Ok(());
                                            }
                                        }
                                    }
                                }
                            }
                        };
                        run()
                    });
                    if let Err(error) = result {
                        abort.store(true, std::sync::atomic::Ordering::Release);
                        let _ = sender.send(PartitionMessage::Error(error));
                    }
                    let _ = sender.send(PartitionMessage::Done);
                })
                .map_err(|error| {
                    crate::error::catalog::ER_GET_ERRNO
                        .error(format!("cannot start window spill worker: {error}"))
                })?;
            output.workers.push(worker);
        }
        drop(sender);
        analyze_stats::add_vector_runtime(self.plan, 0, 0, count as u32);
        #[cfg(test)]
        PARALLEL_PREFIX_PARTITION_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Some(output))
    }
}

impl BatchOperator for HashPartitions<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn properties(&self) -> BatchProperties {
        self.properties.clone()
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if let Some(output) = self.parallel_output.as_mut() {
            return output.next_batch();
        }
        if let Some(mut input) = self.input.take() {
            let dir = crate::engine::executor::select::grace::SpillTmpDir::create_kind(
                &self.data_dir,
                "window",
            )?;
            let mut writers = crate::engine::executor::select::grace::LazyValueRowWriterPool::new_with_concurrent_pools(
                dir.path(), "partition", WINDOW_PARTITIONS, self.input_schema.len(), dir.charger(), WINDOW_PARTITIONS, 1,
            );
            let partition_columns = self.windows[0]
                .partition_by
                .iter()
                .map(|expr| {
                    expr_column_index_in_template(expr, &self.template)
                        .expect("bound partition column")
                })
                .collect::<Vec<_>>();
            let mut key = Vec::new();
            while let Some(batch) = input.next_batch()? {
                crate::engine::cancel::check_cancelled()?;
                for row in batch.visible_indices() {
                    key.clear();
                    for &column in &partition_columns {
                        crate::engine::executor::vec::key_encode::encode_from_column_row_with_collation(
                            &batch.columns()[column], row, batch.schema.fields()[column].source.collation_id, &mut key,
                        );
                    }
                    let partition = crate::engine::executor::vec::key_encode::fxhash(&key) as usize
                        % WINDOW_PARTITIONS;
                    writers.append_batch_row(partition, &batch, row)?;
                }
            }
            let bytes = writers.finish()?;
            analyze_stats::add_spill_bytes(self.plan, bytes);
            self.dir = Some(dir);
            drop(input);
            self.parallel_output = self.start_parallel_partitions()?;
            if let Some(output) = self.parallel_output.as_mut() {
                return output.next_batch();
            }
        }
        loop {
            if let Some(output) = self.output.as_mut() {
                if let Some(batch) = output.next_batch()? {
                    return Ok(Some(batch));
                }
                self.output = None;
            }
            if self.partition == WINDOW_PARTITIONS {
                self.dir = None;
                return Ok(None);
            }
            let dir = self.dir.as_ref().expect("window partitions created");
            let path = dir.path().join(format!("partition-{}", self.partition));
            self.partition += 1;
            let input = Box::new(super::grace_join::VecJoinSpillBatchStream::open(
                &path,
                self.input_schema.clone(),
                dir.charger(),
            )?);
            let prepared = compact_window::prepare(
                input,
                &self.template,
                &self.windows,
                self.schema.clone(),
                self.plan,
            )?;
            self.output = Some(match prepared {
                compact_window::Preparation::Ready(batches, charge) => {
                    Box::new(ChargedMaterializedBatchOperator {
                        inner: MaterializedBatchOperator::with_properties(
                            self.schema.clone(),
                            batches,
                            self.properties.clone(),
                        ),
                        _charge: charge,
                    })
                }
                compact_window::Preparation::Streaming(input) => prepare_sorted_prefix(
                    input,
                    &self.template,
                    &self.windows,
                    self.schema.clone(),
                    self.properties.clone(),
                    self.plan,
                    &self.data_dir,
                    false,
                ),
            });
        }
    }
}

impl Drop for HashPartitions<'_> {
    fn drop(&mut self) {
        // Stop/join readers before removing their partition files.
        self.parallel_output = None;
        self.output = None;
        self.dir = None;
    }
}

pub(super) fn prepare<'a>(
    input: Box<dyn BatchOperator + 'a>,
    template: &[PlanField],
    windows: &[crate::planner::PhysicalWindowSpec],
    output_schema: Arc<BatchSchema>,
    properties: BatchProperties,
    plan: &'a PhysicalPlan,
    data_dir: &std::path::Path,
    restore_order: bool,
    worker_context: Option<(
        Arc<Database>,
        super::super::execution_context::ExecutionContext,
    )>,
) -> Box<dyn BatchOperator + 'a> {
    if restore_order {
        return prepare_sorted_prefix(
            input,
            template,
            windows,
            output_schema,
            properties,
            plan,
            data_dir,
            true,
        );
    }
    Box::new(HashPartitions {
        input_schema: input.schema().clone(),
        input: Some(input),
        schema: output_schema,
        properties,
        template: template.to_vec(),
        windows: windows.to_vec(),
        plan,
        data_dir: data_dir.to_path_buf(),
        dir: None,
        output: None,
        partition: 0,
        worker_context,
        parallel_output: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spooled_prefix_windows_match_canonical_partitions_and_restore_ordinals() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let types = [
            ColumnType::LongLong,
            ColumnType::LongLong,
            ColumnType::Decimal {
                precision: 18,
                scale: 2,
            },
            ColumnType::LongLong,
        ];
        let fields = ["g", "t", "v", "__mydb_prefix_window_ordinal"]
            .into_iter()
            .zip(types)
            .map(|(name, column_type)| BatchField {
                table: String::new(),
                alias: None,
                name: name.into(),
                column_type,
                source: Default::default(),
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(BatchSchema::new(fields));
        let template = schema
            .fields()
            .iter()
            .map(batch_field_template)
            .collect::<Vec<_>>();
        let rows = (0..40_000)
            .rev()
            .map(|id| {
                vec![
                    if id % 13 == 0 {
                        Value::Null
                    } else {
                        Value::Int(id % 7)
                    },
                    if id % 11 == 0 {
                        Value::Null
                    } else {
                        Value::Int(id % 9)
                    },
                    if id % 5 == 0 {
                        Value::Null
                    } else {
                        Value::DecimalCompact(id % 17 - 8, 2)
                    },
                    Value::Int(id),
                ]
            })
            .collect::<Vec<_>>();
        let Expr::WindowFunction {spec, ..} = crate::parser::parse_expr_fragment(
            "SUM(v) OVER (PARTITION BY g ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
        ).unwrap() else { panic!("window expression") };
        let crate::parser::WindowSpec { frame, .. } = spec.as_ref();

        let mut windows = Vec::new();
        for (index, name) in ["SUM", "AVG", "MIN", "MAX", "COUNT", "COUNT"]
            .into_iter()
            .enumerate()
        {
            windows.push(crate::planner::PhysicalWindowSpec {
                name: name.into(),
                args: vec![if index == 5 {
                    Expr::Wildcard
                } else {
                    Expr::Identifier("v".into())
                }],
                partition_by: vec![Expr::Identifier("g".into())],
                order_by: vec![crate::parser::OrderBy {
                    expr: Expr::Identifier("t".into()),
                    asc: true,
                    nulls_first: Some(false),
                }],
                frame: frame.clone(),
                output_alias: format!("w{index}"),
                output_type: if name == "COUNT" {
                    ColumnType::LongLong
                } else {
                    ColumnType::Decimal {
                        precision: 65,
                        scale: 2,
                    }
                },
                top_k: None,
            });
        }
        assert!(supported(&template, &windows));
        let mut output_fields = schema.fields().to_vec();
        output_fields.extend(windows.iter().map(|window| BatchField {
            table: String::new(),
            alias: None,
            name: window.output_alias.clone(),
            column_type: window.output_type,
            source: Default::default(),
        }));
        let output_schema = Arc::new(BatchSchema::new(output_fields));
        let plan = PhysicalPlan::ValuesScan {
            rows: Vec::new(),
            columns: Vec::new(),
            filter: None,
            estimated_rows: rows.len() as f64,
            cost: Default::default(),
        };
        let columns = schema
            .fields()
            .iter()
            .map(|field| crate::types::Column {
                name: field.name.clone(),
                column_type: field.column_type,
                source: field.source.clone(),
            })
            .collect::<Vec<_>>();
        let oracle_budget = Arc::new(mem_budget::MemBudget::new(64 * 1024 * 1024));
        let expected = mem_budget::with_query_budget(oracle_budget, || {
            let parts = crate::engine::executor::dispatch::compute_window_partitions(
                &columns,
                &rows,
                &windows[0].partition_by,
                &windows[0].order_by,
                None,
            )
            .unwrap();
            let mut expected = rows.clone();
            for (index, window) in windows.iter().enumerate() {
                let mut result = vec![Value::Null; rows.len()];
                for part in &parts {
                    let values = crate::engine::executor::dispatch::evaluate_window_prefix_values(
                        &window.name,
                        index == 5,
                        part.iter().map(|&row| rows[row][2].clone()),
                    )
                    .unwrap();
                    for (&row, value) in part.iter().zip(values) {
                        result[row] = value
                    }
                }
                for (row, value) in expected.iter_mut().zip(result) {
                    row.push(value)
                }
            }
            expected
        });
        let db = Arc::new(Database::open(root.path()).unwrap());
        for (restore_order, parallel) in [(true, false), (false, false), (false, true)] {
            let budget = Arc::new(mem_budget::MemBudget::new(if parallel {
                32 * 1024 * 1024
            } else {
                32 * 1024
            }));
            mem_budget::with_query_budget(budget.clone(), || {
                let mut tuning = crate::engine::executor::tuning::current_or_capture();
                tuning.scheduler_parallelism = if parallel { 4 } else { 1 };
                let _tuning = crate::engine::executor::tuning::enter(tuning);
                let hits =
                    PARALLEL_PREFIX_PARTITION_HITS.load(std::sync::atomic::Ordering::Relaxed);
                let batches = value_rows_into_batches(schema.clone(), rows.clone());
                let input = Box::new(MaterializedBatchOperator::new(schema.clone(), batches));
                let mut stream = prepare(
                    input,
                    &template,
                    &windows,
                    output_schema.clone(),
                    BatchProperties::UNORDERED_SINGLE,
                    &plan,
                    root.path(),
                    restore_order,
                    parallel.then(|| {
                        (
                            db.clone(),
                            super::super::super::execution_context::ExecutionContext::capture(
                                TxnView::read_only(
                                    0,
                                    crate::storage::IsolationLevel::RepeatableRead,
                                ),
                            ),
                        )
                    }),
                );
                let mut actual = Vec::new();
                while let Some(batch) = stream.next_batch().unwrap() {
                    actual.extend(batch.visible_indices().map(|row| batch.row(row)));
                }
                let mut expected = expected.clone();
                if !restore_order {
                    actual.sort_by_key(|row| match row[3] {
                        Value::Int(value) => value,
                        _ => panic!("integer ordinal"),
                    });
                    expected.sort_by_key(|row| match row[3] {
                        Value::Int(value) => value,
                        _ => panic!("integer ordinal"),
                    });
                }
                assert_eq!(actual, expected);
                drop(stream);
                assert_eq!(budget.used(), 0);
                if parallel {
                    assert!(
                        PARALLEL_PREFIX_PARTITION_HITS.load(std::sync::atomic::Ordering::Relaxed)
                            > hits
                    );
                    let batches = value_rows_into_batches(schema.clone(), rows.clone());
                    let mut stream = prepare(
                        Box::new(MaterializedBatchOperator::new(schema.clone(), batches)),
                        &template,
                        &windows,
                        output_schema.clone(),
                        BatchProperties::UNORDERED_SINGLE,
                        &plan,
                        root.path(),
                        false,
                        Some((
                            db.clone(),
                            super::super::super::execution_context::ExecutionContext::capture(
                                TxnView::read_only(
                                    0,
                                    crate::storage::IsolationLevel::RepeatableRead,
                                ),
                            ),
                        )),
                    );
                    assert!(stream.next_batch().unwrap().is_some());
                    drop(stream);
                    assert_eq!(
                        budget.used(),
                        0,
                        "early consumer drop must join and release queued batches"
                    );
                }
            });
        }
    }
}
