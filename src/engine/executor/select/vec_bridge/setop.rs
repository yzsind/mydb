//! setop execution and preparation.

use super::*;

pub(super) struct ChargedMaterializedBatchOperator {
    pub(super) inner: MaterializedBatchOperator,
    pub(super) _charge: crate::engine::executor::mem_budget::ScopedCharge,
}

impl BatchOperator for ChargedMaterializedBatchOperator {
    fn schema(&self) -> &Arc<BatchSchema> {
        self.inner.schema()
    }

    fn properties(&self) -> BatchProperties {
        self.inner.properties()
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.inner.next_batch()
    }
}

/// Physical set operator over two lowered batch subgraphs. `UNION ALL`
/// concatenates lazily; count-based variants use the shared spillable
/// hash-count kernel without constructing joined/plan rows.
pub(super) struct SetOpBatchOperator<'a> {
    pub(super) db: &'a Arc<Database>,
    pub(super) plan: &'a PhysicalPlan,
    pub(super) left: Option<Box<dyn BatchOperator + 'a>>,
    pub(super) right: Option<Box<dyn BatchOperator + 'a>>,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) op: crate::parser::SetOp,
    pub(super) all: bool,
    pub(super) data_dir: std::path::PathBuf,
    pub(super) left_done: bool,
    pub(super) blocking_output: Option<crate::engine::executor::dispatch::IncrementalSetOutput>,
    pub(super) started: Option<std::time::Instant>,
    pub(super) io_start: Option<(u64, u64)>,
    pub(super) batches: u64,
    pub(super) output_rows: u64,
    pub(super) stats_recorded: bool,
}

/// Pulls visible rows from a batch stream without draining the whole branch.
/// A blocking set operator therefore retains at most one input batch in
/// addition to its bounded hash-count/spill state.
pub(super) struct SetOpRowPuller<'a> {
    pub(super) input: Box<dyn BatchOperator + 'a>,
    pub(super) source_schema: Arc<BatchSchema>,
    pub(super) schema: Arc<BatchSchema>,
    pub(super) batch: Option<Batch>,
    pub(super) visible: Vec<usize>,
    pub(super) position: usize,
}

impl SetOpRowPuller<'_> {
    pub(super) fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        loop {
            if let Some(batch) = self.batch.as_ref() {
                if let Some(&row_index) = self.visible.get(self.position) {
                    self.position += 1;
                    let mut row = batch.row(row_index);
                    for (column_index, (value, field)) in
                        row.iter_mut().zip(self.schema.fields()).enumerate()
                    {
                        let passthrough = self
                            .source_schema
                            .fields()
                            .get(column_index)
                            .is_some_and(|source| {
                                SetOpBatchOperator::can_passthrough_field(source, field)
                            });
                        if !passthrough {
                            *value = SetOpBatchOperator::coerce_value(
                                std::mem::replace(value, Value::Null),
                                field,
                            )?;
                        }
                    }
                    return Ok(Some(row));
                }
            }
            let Some(batch) = self.input.next_batch()? else {
                self.batch = None;
                self.visible.clear();
                return Ok(None);
            };
            crate::engine::cancel::check_cancelled()?;
            self.visible = batch.visible_indices().collect();
            self.position = 0;
            self.batch = Some(batch);
        }
    }
}

impl SetOpBatchOperator<'_> {
    #[inline]
    pub(super) fn can_passthrough_field(source: &BatchField, target: &BatchField) -> bool {
        if source.column_type == target.column_type {
            return true;
        }
        matches!(
            (source.column_type, target.column_type),
            (
                ColumnType::Decimal {
                    precision: source_precision,
                    scale: source_scale,
                },
                ColumnType::Decimal {
                    precision: target_precision,
                    scale: target_scale,
                },
            ) if source_scale == target_scale && source_precision <= target_precision
        )
    }

    pub(super) fn ensure_started(&mut self) {
        if self.started.is_none() && analyze_stats::is_recording() {
            self.started = Some(std::time::Instant::now());
            self.io_start = Some(self.db.buffer_stats_counts());
        }
    }

    pub(super) fn record_stats(&mut self) {
        if self.stats_recorded {
            return;
        }
        self.stats_recorded = true;
        if let (Some(started), Some(io_start)) = (self.started, self.io_start) {
            analyze_stats::add_vector_runtime(self.plan, self.batches, 0, 1);
            analyze_stats::record_with_io_snapshot(
                self.db,
                self.plan,
                started.elapsed().as_nanos(),
                self.output_rows,
                io_start,
            );
        }
    }

    pub(super) fn coerce_value(value: Value, field: &BatchField) -> Result<Value, QueryError> {
        crate::engine::executor::dispatch::coerce_setop_value(
            value,
            field.column_type,
            &field.source,
        )
    }

    /// Whether set-operation metadata unification leaves a physical column
    /// byte-compatible with the output schema. UNION type resolution often
    /// only widens a DECIMAL's declared precision while retaining its scale;
    /// the magnitude representation is already identical in that case.
    #[inline]
    pub(super) fn can_passthrough_column(
        column: &ColumnVector,
        source: &BatchField,
        target: &BatchField,
    ) -> bool {
        if matches!(column, ColumnVector::Null { .. })
            || Self::can_passthrough_field(source, target)
        {
            return true;
        }
        false
    }

    pub(super) fn coerce_schema(&self, mut batch: Batch) -> Result<Batch, QueryError> {
        let row_count = batch.num_rows();
        let selection = batch.selection.clone();
        let mut columns = Vec::with_capacity(batch.columns.len());
        let (input_columns, owner) = batch.take_columns()?.into_parts();
        for (column_index, (column, field)) in input_columns
            .into_iter()
            .zip(self.schema.fields())
            .enumerate()
        {
            if batch
                .schema
                .fields()
                .get(column_index)
                .is_some_and(|source| Self::can_passthrough_column(&column, source, field))
            {
                columns.push(column);
                continue;
            }
            let mut values = Vec::with_capacity(row_count);
            for row in 0..row_count {
                values.push(Self::coerce_value(column.value_at(row), field)?);
            }
            columns.push(ColumnVector::from_values(values));
        }
        let mut output = Batch::from_owned_columns(
            self.schema.clone(),
            crate::engine::executor::vec::batch::OwnedColumns::from_parts(
                columns, owner, row_count,
            ),
        );
        if let Some(selection) = selection {
            output = output.with_selection(selection);
        }
        Ok(output)
    }

    pub(super) fn initialize_blocking(&mut self) -> Result<(), QueryError> {
        if self.blocking_output.is_some() {
            return Ok(());
        }
        let schema = self.schema.clone();
        let left_input = self
            .left
            .take()
            .expect("set-op left input is present before blocking initialization");
        let right_input = self
            .right
            .take()
            .expect("set-op right input is present before blocking initialization");
        let left_source_schema = left_input.schema().clone();
        let right_source_schema = right_input.schema().clone();
        let mut left = Some(SetOpRowPuller {
            input: left_input,
            source_schema: left_source_schema,
            schema: schema.clone(),
            batch: None,
            visible: Vec::new(),
            position: 0,
        });
        let mut right = Some(SetOpRowPuller {
            input: right_input,
            source_schema: right_source_schema,
            schema,
            batch: None,
            visible: Vec::new(),
            position: 0,
        });
        self.blocking_output = Some(
            crate::engine::executor::dispatch::combine_set_sources_spillable(
                &self
                    .schema
                    .fields()
                    .iter()
                    .map(|field| field.source.collation_id)
                    .collect::<Vec<_>>(),
                self.op,
                self.all,
                self.schema.len(),
                &self.data_dir,
                || {
                    let result = left
                        .as_mut()
                        .expect("set-op left input requested after EOF")
                        .next_row()?;
                    if result.is_none() {
                        // EOF is a lifetime boundary: completed HashJoin,
                        // HashAgg and scan work areas must not remain live
                        // while the right branch and SetOp state grow.
                        left = None;
                    }
                    Ok(result)
                },
                || {
                    let result = right
                        .as_mut()
                        .expect("set-op right input requested after EOF")
                        .next_row()?;
                    if result.is_none() {
                        right = None;
                    }
                    Ok(result)
                },
            )?,
        );
        let (
            spill_bytes,
            spill_partitions,
            spill_file_opens,
            peak_memory,
            spill_repartitions,
            spill_max_depth,
            spill_merge_passes,
        ) = analyze_stats::take_pending_setop_spill();
        if spill_bytes > 0 {
            analyze_stats::add_spill_bytes(self.plan, spill_bytes);
            analyze_stats::add_spill_shape(self.plan, 0, 1, spill_partitions, "memory_limit");
            analyze_stats::add_setop_spill_runtime(
                self.plan,
                spill_file_opens,
                spill_repartitions,
                spill_max_depth,
                spill_merge_passes,
            );
            analyze_stats::add_peak_memory(self.plan, peak_memory);
        }
        Ok(())
    }
}

impl BatchOperator for SetOpBatchOperator<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        crate::util::stack::with_stack(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
            || self.next_batch_checked(),
        )?
    }
}

impl SetOpBatchOperator<'_> {
    #[inline(never)]
    fn next_batch_checked(&mut self) -> Result<Option<Batch>, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        self.ensure_started();
        if self.op == crate::parser::SetOp::Union && self.all {
            if !self.left_done {
                let next = self
                    .left
                    .as_mut()
                    .expect("UNION ALL left input is present before EOF")
                    .next_batch()?;
                if let Some(batch) = next {
                    let batch = self.coerce_schema(batch)?;
                    self.batches = self.batches.saturating_add(1);
                    self.output_rows = self.output_rows.saturating_add(batch.visible_rows() as u64);
                    return Ok(Some(batch));
                }
                self.left_done = true;
                self.left = None;
            }
            let Some(right) = self.right.as_mut() else {
                self.record_stats();
                return Ok(None);
            };
            let next = right.next_batch()?;
            return match next {
                Some(batch) => {
                    let batch = self.coerce_schema(batch)?;
                    self.batches = self.batches.saturating_add(1);
                    self.output_rows = self.output_rows.saturating_add(batch.visible_rows() as u64);
                    Ok(Some(batch))
                }
                None => {
                    self.right = None;
                    self.record_stats();
                    Ok(None)
                }
            };
        }
        self.initialize_blocking()?;
        let output = self
            .blocking_output
            .as_mut()
            .expect("blocking SetOp output initialized");
        let mut rows = Vec::with_capacity(DEFAULT_BATCH_ROWS);
        while rows.len() < DEFAULT_BATCH_ROWS {
            let Some(row) = output.next_row()? else {
                break;
            };
            rows.push(row);
        }
        if rows.is_empty() {
            self.record_stats();
            Ok(None)
        } else {
            let batch = value_rows_into_batches(self.schema.clone(), rows)
                .into_iter()
                .next();
            if let Some(batch) = batch.as_ref() {
                self.batches = self.batches.saturating_add(1);
                self.output_rows = self.output_rows.saturating_add(batch.visible_rows() as u64);
            }
            Ok(batch)
        }
    }
}

impl Drop for SetOpBatchOperator<'_> {
    fn drop(&mut self) {
        if self.started.is_some() {
            self.record_stats();
        }
    }
}

pub(super) fn try_build_setop_batch<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    crate::util::stack::with_stack(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
        || try_build_setop_batch_checked(db, txn, plan),
    )?
}

fn try_build_setop_batch_checked<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let PhysicalPlan::SetOp {
        left,
        right,
        op,
        all,
        ..
    } = plan
    else {
        return Ok(None);
    };
    let Some(left_input) = try_build_query_output_batch(db, txn, left)? else {
        return Ok(None);
    };
    let Some(right_input) = try_build_query_output_batch(db, txn, right)? else {
        return Ok(None);
    };
    #[cfg(test)]
    if crate::util::env::var_os("MYDB_DEBUG_SETOP_BATCH").is_some() {
        eprintln!(
            "setop runtime left={:?} right={:?}",
            left_input
                .template
                .iter()
                .map(|field| &field.column)
                .collect::<Vec<_>>(),
            right_input
                .template
                .iter()
                .map(|field| &field.column)
                .collect::<Vec<_>>()
        );
    }
    let template = unify_setop_templates(left_input.template, right_input.template)?;
    let schema = template_to_batch_schema(&template);
    Ok(Some(VecBatchInput {
        stream: Box::new(SetOpBatchOperator {
            db,
            plan,
            left: Some(left_input.stream),
            right: Some(right_input.stream),
            schema,
            op: *op,
            all: *all,
            data_dir: db.data_dir().to_path_buf(),
            left_done: false,
            blocking_output: None,
            started: None,
            io_start: None,
            batches: 0,
            output_rows: 0,
            stats_recorded: false,
        }),
        template,
    }))
}

impl BatchOperator for ScalarAggregateKernelBatchOperator<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.output_schema
    }

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        self.initialize()?;
        Ok(self.output.as_mut().and_then(Iterator::next))
    }
}
