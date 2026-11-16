//! window execution and preparation.

use super::*;

/// Admit each input row before cloning it or requesting another batch.
/// The soft miss selects the canonical spill evaluator; the retained input
/// and its conversion remain governed until that evaluator is externalized.
fn collect_window_value_rows(
    upstream: &mut dyn BatchOperator,
) -> Result<(Vec<Vec<Value>>, mem_budget::ScopedCharge, bool), QueryError> {
    let mut rows = Vec::new();
    let mut charge = mem_budget::ScopedCharge::new(0);
    let mut spill = false;
    while let Some(batch) = upstream.next_batch()? {
        crate::engine::cancel::check_cancelled()?;
        for row in batch.visible_indices() {
            let bytes = batch.columns().iter().fold(0usize, |n, column| {
                n.saturating_add(column.owned_value_bytes(row))
            });
            if spill {
                charge.add_force(bytes)?;
            } else if charge.add(bytes).is_err() {
                spill = true;
                charge.add_force(bytes)?;
            }
            if rows.len() == rows.capacity() {
                let additional = rows.capacity().max(batch.visible_rows()).max(1);
                mem_budget::try_reserve_vec_hard(
                    &mut rows,
                    additional,
                    &mut charge,
                    "window retained rows",
                )?;
            }
            let mut values =
                mem_budget::try_vec_with_capacity(batch.columns().len(), "window retained row")?;
            for column in batch.columns() {
                values.push(column.try_value_at(row)?);
            }
            rows.push(values);
        }
    }
    Ok((rows, charge, spill))
}

/// Blocking window operator inside the common batch graph. The ordinary path
/// evaluates partitions directly over value rows extracted from typed batches
/// and moves the results back into typed output batches. If the value-row work
/// area cannot be reserved, the same already-consumed input is handed to the
/// canonical partition-spool implementation; the upstream is never reopened.
pub(super) struct WindowBatchOperator<'a> {
    pub(super) db: &'a Arc<Database>,
    pub(super) txn: TxnView,
    pub(super) plan: &'a PhysicalPlan,
    pub(super) upstream: Option<Box<dyn BatchOperator + 'a>>,
    pub(super) input_template: Vec<PlanField>,
    pub(super) windows: &'a [crate::planner::PhysicalWindowSpec],
    pub(super) schema: Arc<BatchSchema>,
    pub(super) properties: BatchProperties,
    pub(super) output: std::vec::IntoIter<Batch>,
    pub(super) memory_charge: Option<crate::engine::executor::mem_budget::ScopedCharge>,
    pub(super) partition_output:
        Option<Box<dyn crate::engine::executor::select::plan::PlanStream + 'a>>,
    pub(super) co_partitioned: bool,
    pub(super) prefix_output: Option<Box<dyn BatchOperator + 'a>>,
}

impl WindowBatchOperator<'_> {
    fn install_output(
        &mut self,
        batches: Vec<Batch>,
        mut charge: mem_budget::ScopedCharge,
    ) -> Result<(), QueryError> {
        let actual = batches.iter().fold(0usize, |n, batch| {
            n.saturating_add(batch.memory_bytes())
                .saturating_add(
                    batch
                        .columns()
                        .len()
                        .saturating_mul(std::mem::size_of::<ColumnVector>()),
                )
                .saturating_add(128)
        });
        if actual > charge.charged() {
            charge.add_force(actual - charge.charged())?;
        } else {
            charge.release_bytes(charge.charged() - actual);
        }
        let owner = crate::engine::executor::vec::batch::BatchCharge::shared(charge)?;
        self.output = batches
            .into_iter()
            .map(|batch| batch.with_shared_charge(owner.clone()))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter();
        self.memory_charge = None;
        Ok(())
    }
}

impl BatchOperator for WindowBatchOperator<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        self.properties.clone()
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if let Some(output) = self.prefix_output.as_mut() {
            return output.next_batch();
        }
        if self.co_partitioned {
            if let Some(upstream) = self.upstream.take() {
                let upstream = match compact_window::prepare(
                    upstream,
                    &self.input_template,
                    self.windows,
                    self.schema.clone(),
                    self.plan,
                )? {
                    compact_window::Preparation::Ready(batches, charge) => {
                        self.co_partitioned = false;
                        self.install_output(batches, charge)?;
                        return Ok(self.output.next());
                    }
                    compact_window::Preparation::Streaming(upstream) => upstream,
                };
                if crate::engine::executor::cte::cte_materialization_active()
                    && matches!(self.properties.ordering, BatchOrdering::Unordered)
                    && spooled_prefix_window::supported(&self.input_template, self.windows)
                {
                    self.prefix_output = Some(spooled_prefix_window::prepare(
                        upstream,
                        &self.input_template,
                        self.windows,
                        self.schema.clone(),
                        self.properties.clone(),
                        self.plan,
                        self.db.data_dir(),
                        false,
                        Some((
                            self.db.clone(),
                            super::super::execution_context::ExecutionContext::capture(self.txn),
                        )),
                    ));
                    return self
                        .prefix_output
                        .as_mut()
                        .expect("prefix window prepared")
                        .next_batch();
                }
                let mut ordinal_name = "__mydb_window_input_ordinal".to_string();
                while self
                    .input_template
                    .iter()
                    .any(|f| f.column.eq_ignore_ascii_case(&ordinal_name))
                    || self
                        .windows
                        .iter()
                        .any(|w| w.output_alias.eq_ignore_ascii_case(&ordinal_name))
                {
                    ordinal_name.push('_');
                }
                let input = BatchPlanRowStream {
                    input: upstream,
                    template: self.input_template.clone(),
                    current: None,
                    visible: Vec::new(),
                    position: 0,
                    finished: false,
                };
                self.partition_output = Some(
                    crate::engine::executor::select::plan::window_partition_input_stream(
                        self.db,
                        Box::new(input),
                        self.windows,
                        self.plan,
                        ordinal_name,
                        self.input_template.len(),
                    )?,
                );
            }
            let Some(stream) = self.partition_output.as_mut() else {
                return Ok(None);
            };
            let mut values = Vec::new();
            while values.len() < DEFAULT_BATCH_ROWS {
                let Some(row) = stream.next()? else {
                    break;
                };
                values.push(row.fields.into_iter().map(|field| field.value).collect());
            }
            if values.is_empty() {
                self.partition_output = None;
                return Ok(None);
            }
            return Ok(value_rows_into_batches(self.schema.clone(), values)
                .into_iter()
                .next());
        }
        if let Some(mut upstream) = self.upstream.take() {
            use crate::engine::executor::dispatch::{
                compute_window_partitions, evaluate_window_with_partitions, RankKind,
            };

            let (mut value_rows, mut charge, requires_spill) =
                collect_window_value_rows(upstream.as_mut())?;
            if value_rows.is_empty() {
                return Ok(None);
            }

            let columns: Vec<crate::types::Column> = self
                .input_template
                .iter()
                .map(|field| crate::types::Column {
                    source: field.source.clone(),
                    name: field.column.clone(),
                    column_type: field.column_type,
                })
                .collect();
            if requires_spill {
                charge.add_force(
                    value_rows.len().saturating_mul(
                        std::mem::size_of::<PlanRow>()
                            + self
                                .input_template
                                .len()
                                .saturating_mul(std::mem::size_of::<PlanField>()),
                    ),
                )?;
                let metas = template_metas(&self.input_template);
                let rows: Vec<PlanRow> = value_rows
                    .into_iter()
                    .map(|values| PlanRow {
                        fields: metas
                            .iter()
                            .cloned()
                            .zip(values)
                            .map(|(meta, value)| PlanField { meta, value })
                            .collect(),
                        ..Default::default()
                    })
                    .collect();
                analyze_stats::add_pending_conversion_rows(rows.len() as u64);
                let rows = crate::engine::executor::select::plan::execute_window_agg(
                    self.db,
                    rows,
                    self.windows,
                    self.plan,
                )?;
                let values = rows
                    .into_iter()
                    .map(|row| row.fields.into_iter().map(|field| field.value).collect())
                    .collect();
                let batches = value_rows_into_batches(self.schema.clone(), values);
                self.install_output(batches, charge)?;
                return Ok(self.output.next());
            }

            let mut group_keys: Vec<String> = Vec::new();
            let mut group_members: Vec<Vec<usize>> = Vec::new();
            for (index, window) in self.windows.iter().enumerate() {
                let key = format!("{:?}|{:?}", window.partition_by, window.order_by);
                if let Some(position) = group_keys.iter().position(|candidate| candidate == &key) {
                    group_members[position].push(index);
                } else {
                    group_keys.push(key);
                    group_members.push(vec![index]);
                }
            }
            let top_k = if self.windows.len() == 1 {
                self.windows[0]
                    .top_k
                    .and_then(|k| RankKind::from_name(&self.windows[0].name).map(|kind| (k, kind)))
            } else {
                None
            };
            let mut kept_indices = None;
            let mut peak_partition_rows = 0u64;
            let mut window_values = vec![Vec::new(); self.windows.len()];
            for members in &group_members {
                let leader = &self.windows[members[0]];
                let group_top_k = (self.windows.len() == 1).then_some(top_k).flatten();
                let partitions = compute_window_partitions(
                    &columns,
                    &value_rows,
                    &leader.partition_by,
                    &leader.order_by,
                    group_top_k,
                )?;
                peak_partition_rows = peak_partition_rows
                    .max(partitions.iter().map(Vec::len).max().unwrap_or(0) as u64);
                if group_top_k.is_some() {
                    kept_indices = Some(partitions.iter().flatten().copied().collect::<Vec<_>>());
                }
                for &window_index in members {
                    let window = &self.windows[window_index];
                    let expression = Expr::WindowFunction {
                        name: window.name.clone(),
                        args: window.args.clone(),
                        spec: Box::new(crate::parser::WindowSpec {
                            partition_by: window.partition_by.clone(),
                            order_by: window.order_by.clone(),
                            frame: window.frame.clone(),
                            inherits: None,
                        }),
                    };
                    window_values[window_index] = evaluate_window_with_partitions(
                        &expression,
                        &partitions,
                        &columns,
                        &value_rows,
                    )?;
                }
            }

            let output_value_bytes = window_values.iter().flatten().fold(0usize, |bytes, value| {
                bytes.saturating_add(mem_budget::estimate_value_bytes(value))
            });
            // Output is mandatory once evaluation has consumed the input;
            // only the optional work copy uses the soft spill watermark.
            charge.add_force(output_value_bytes)?;
            if let Some(kept) = kept_indices {
                let mut keep = vec![false; value_rows.len()];
                for index in kept {
                    keep[index] = true;
                }
                let mut output = Vec::with_capacity(keep.iter().filter(|value| **value).count());
                for (row_index, mut row) in value_rows.into_iter().enumerate() {
                    if !keep[row_index] {
                        continue;
                    }
                    row.extend(
                        window_values
                            .iter_mut()
                            .map(|values| std::mem::replace(&mut values[row_index], Value::Null)),
                    );
                    output.push(row);
                }
                value_rows = output;
            } else {
                for (row_index, row) in value_rows.iter_mut().enumerate() {
                    row.extend(
                        window_values
                            .iter_mut()
                            .map(|values| std::mem::replace(&mut values[row_index], Value::Null)),
                    );
                }
            }
            analyze_stats::add_window_runtime(
                self.plan,
                "batch_partition_state",
                value_rows.len() as u64,
                peak_partition_rows,
            );
            charge.add_force(
                value_rows
                    .len()
                    .saturating_mul(self.schema.len())
                    .saturating_mul(std::mem::size_of::<Value>())
                    .saturating_mul(2),
            )?;
            let batches = value_rows_into_batches(self.schema.clone(), value_rows);
            self.install_output(batches, charge)?;
        }
        let batch = self.output.next();
        if batch.is_none() {
            self.memory_charge = None;
        }
        Ok(batch)
    }
}

pub(super) fn try_build_window_batch<'a>(
    db: &'a Arc<Database>,
    txn: TxnView,
    plan: &'a PhysicalPlan,
) -> Result<Option<VecBatchInput<'a>>, QueryError> {
    let PhysicalPlan::WindowAgg { input, windows, .. } = plan else {
        return Ok(None);
    };
    let Some(VecBatchInput {
        stream,
        mut template,
    }) = try_native_vec_input_stream(db, txn, input)?
    else {
        return Ok(None);
    };
    let input_template = template.clone();
    template.extend(windows.iter().map(|window| PlanField {
        meta: crate::engine::executor::select::plan::intern_col_meta(ColMeta {
            invisible: false,
            table: String::new(),
            alias: None,
            column: window.output_alias.clone(),
            column_type: window.output_type,
            source: Default::default(),
        }),
        value: Value::Null,
    }));
    let schema = template_to_batch_schema(&template);
    let input_schema = template_to_batch_schema(&input_template);
    let mut co_partitioned = windows.first().is_some_and(|first| {
        !first.partition_by.is_empty()
            && windows.iter().all(|w| w.partition_by == first.partition_by)
    });
    // Clustering evaluates rows in a different order before restoring their
    // ordinals. Volatile/session-dependent expressions retain the existing
    // evaluator so side effects and per-row evaluation order cannot change.
    if co_partitioned {
        for expr in windows.iter().flat_map(|w| {
            w.partition_by
                .iter()
                .chain(w.order_by.iter().map(|o| &o.expr))
                .chain(w.args.iter())
        }) {
            if matches!(expr, Expr::Wildcard) {
                continue;
            }
            let safe = expr_batch::compile_for_batch(expr, &input_schema)?.is_some_and(|program| {
                let capability = program.capability();
                capability.deterministic && !capability.depends_on_session
            });
            if !safe {
                co_partitioned = false;
                break;
            }
        }
    }
    let properties = stream.properties();
    Ok(Some(VecBatchInput {
        stream: Box::new(WindowBatchOperator {
            db,
            txn,
            plan,
            upstream: Some(stream),
            input_template,
            windows,
            schema,
            properties,
            output: Vec::new().into_iter(),
            memory_charge: None,
            partition_output: None,
            co_partitioned,
            prefix_output: None,
        }),
        template,
    }))
}

#[cfg(test)]
mod input_memory_tests {
    use super::*;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};

    #[test]
    fn general_window_admits_each_input_before_requesting_the_next_batch() {
        struct Input {
            budget: Arc<MemBudget>,
            schema: Arc<BatchSchema>,
            yielded: bool,
        }
        impl BatchOperator for Input {
            fn schema(&self) -> &Arc<BatchSchema> {
                &self.schema
            }
            fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
                if self.yielded {
                    assert!(
                        self.budget.used() >= 8192,
                        "window retained its first input without a reservation"
                    );
                    return Ok(None);
                }
                self.yielded = true;
                Ok(Some(Batch::new(
                    self.schema.clone(),
                    vec![ColumnVector::Generic {
                        values: vec![Value::Bytes(vec![1; 8192])],
                    }],
                )))
            }
        }
        let budget = Arc::new(MemBudget::new(1024 * 1024));
        with_query_budget(budget.clone(), || {
            let schema = Arc::new(BatchSchema::new(vec![BatchField {
                table: String::new(),
                alias: None,
                name: "v".into(),
                column_type: ColumnType::Blob,
                source: Default::default(),
            }]));
            let mut input = Input {
                budget: budget.clone(),
                schema,
                yielded: false,
            };
            let result = collect_window_value_rows(&mut input).unwrap();
            drop(result);
            assert_eq!(budget.used(), 0);
        });
    }
}
