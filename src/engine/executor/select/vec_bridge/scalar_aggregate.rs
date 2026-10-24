//! scalar aggregate execution and preparation.

use super::*;

pub(super) struct ScalarAggregateRuntimeSlot {
    pub(super) slot: crate::engine::executor::select::aggregate::AggregateSlot,
    pub(super) input_column: Option<usize>,
    pub(super) order_columns: Vec<usize>,
    pub(super) fixed_output_scale: Option<u8>,
    pub(super) approximate_input: bool,
}

/// Bounded adapter for physical inputs that do not yet have a native batch
/// implementation. The row evaluator remains the expression/correctness
/// oracle, while grouping, state, memory and spill stay in the common Hash
/// Aggregate core. No input is restarted or materialised by this adapter.
pub(super) struct PlanRowBatchOperator {
    pub(super) input: Box<dyn crate::engine::executor::select::plan::PlanStream>,
    pub(super) schema: Arc<BatchSchema>,
}

impl BatchOperator for PlanRowBatchOperator {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }

    fn properties(&self) -> BatchProperties {
        BatchProperties::UNORDERED_SINGLE
    }

    fn install_runtime_filter(&mut self, filter: crate::planner::RuntimeFilter) -> bool {
        self.input.install_runtime_filter(filter)
    }

    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        let mut columns = self
            .schema
            .fields()
            .iter()
            .map(|field| ColumnAccumulator::for_field(field, DEFAULT_BATCH_ROWS))
            .collect::<Vec<_>>();
        let mut rows = 0;
        while rows < DEFAULT_BATCH_ROWS {
            let Some(row) = self.input.next()? else {
                break;
            };
            if row.fields.len() != columns.len() {
                return Err(crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "row-to-batch HashAggregate input schema mismatch",
                ));
            }
            for (column, field) in columns.iter_mut().zip(row.fields.iter()) {
                column.push(&field.value);
            }
            rows += 1;
        }
        if rows == 0 {
            return Ok(None);
        }
        Ok(Some(Batch::with_row_count(
            self.schema.clone(),
            columns.into_iter().map(ColumnAccumulator::finish).collect(),
            rows,
        )))
    }
}

pub(super) fn row_batch_input<'a>(
    db: &Arc<Database>,
    txn: TxnView,
    plan: &PhysicalPlan,
) -> Result<VecBatchInput<'a>, QueryError> {
    let template = plan_output_template(plan);
    let schema = template_to_batch_schema(&template);
    let input = crate::engine::executor::select::plan::execute_plan_stream(db, txn, plan)?;
    Ok(VecBatchInput {
        stream: Box::new(PlanRowBatchOperator { input, schema }),
        template,
    })
}

pub(super) struct ScalarAggregateGroup {
    pub(super) key_values: Vec<Value>,
    pub(super) accumulators: Vec<Box<dyn crate::engine::executor::select::aggregate::Accumulator>>,
    /// High-water mark already reserved for variable-size accumulator state.
    /// Accumulators can retain capacity after a logical shrink, so charging
    /// only positive deltas from the previous row could charge the same
    /// allocation twice.  Keeping the per-group high-water mark makes the
    /// incremental accounting both bounded and conservative.
    pub(super) accounted_accumulator_heap_bytes: usize,
    pub(super) accounted_slot_heap_bytes: Vec<usize>,
}

/// Compact state for permissive GROUP BY projections whose only internal
/// aggregate slots are `__hidden_first_value`.  Such slots need exactly one
/// owned value per group; allocating a boxed trait-object accumulator for
/// every slot/group adds substantial allocator and virtual-dispatch overhead
/// on high-cardinality analytical queries without buying any extra semantics.
pub(super) struct ScalarFirstValueGroup {
    pub(super) key_values: Vec<Value>,
    pub(super) first_values: Vec<Value>,
}

/// Semantic-complete scalar kernel behind the common BatchOperator protocol.
/// It is used for ordered/holistic/spatial aggregates and any expression lane
/// that has no typed aggregate implementation yet. Input is never re-scanned:
/// expressions are appended to each batch once, and only accumulator updates
/// are scalar. This is deliberately an internal kernel choice, not a second
/// physical executor.
pub(super) struct ScalarAggregateKernelBatchOperator<'a> {
    pub(super) input: Box<dyn BatchOperator + 'a>,
    pub(super) input_schema: Arc<BatchSchema>,
    pub(super) output_schema: Arc<BatchSchema>,
    pub(super) group_columns: Vec<usize>,
    pub(super) slots: Vec<ScalarAggregateRuntimeSlot>,
    pub(super) output: Option<std::vec::IntoIter<Batch>>,
    pub(super) rows_consumed: u64,
    pub(super) batches_consumed: u64,
    pub(super) memory_charge: crate::engine::executor::mem_budget::ScopedCharge,
}

impl ScalarAggregateKernelBatchOperator<'_> {
    pub(super) fn hidden_first_value_only(&self) -> bool {
        !self.slots.is_empty()
            && self.slots.iter().all(|runtime| {
                runtime
                    .slot
                    .name
                    .eq_ignore_ascii_case(crate::parser::HIDDEN_FIRST_VALUE_FN)
                    && !runtime.slot.distinct
                    && runtime.slot.order_by.is_empty()
                    && runtime.input_column.is_some()
                    && !runtime.approximate_input
                    && runtime.fixed_output_scale.is_none()
            })
    }

    pub(super) fn first_value_container_capacity_bytes(
        by_key: &hashbrown::HashMap<Vec<u8>, (), ahash::RandomState>,
        groups_capacity: usize,
    ) -> usize {
        by_key
            .capacity()
            .saturating_mul(std::mem::size_of::<Vec<u8>>() + 64)
            .saturating_add(
                groups_capacity.saturating_mul(std::mem::size_of::<ScalarFirstValueGroup>()),
            )
    }

    pub(super) fn first_value_group_owned_capacity_bytes(group: &ScalarFirstValueGroup) -> usize {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        let values = |values: &[Value], capacity: usize| {
            capacity
                .saturating_mul(std::mem::size_of::<Value>())
                .saturating_add(
                    values
                        .iter()
                        .map(AllocatedBytes::allocated_bytes)
                        .sum::<usize>(),
                )
        };
        values(&group.key_values, group.key_values.capacity())
            .saturating_add(values(&group.first_values, group.first_values.capacity()))
    }

    /// Run the semantic-equivalent fixed-state kernel for hidden first-value
    /// slots. Group insertion order and the first row (including NULL) are
    /// identical to the generic accumulator path; only the physical state
    /// representation changes.
    pub(super) fn initialize_hidden_first_values(&mut self) -> Result<(), QueryError> {
        let mut by_key: hashbrown::HashMap<Vec<u8>, (), ahash::RandomState> =
            hashbrown::HashMap::with_hasher(ahash::RandomState::new());
        let mut groups: Vec<ScalarFirstValueGroup> = Vec::new();
        let mut key = Vec::with_capacity(64);
        let key_program = crate::engine::executor::hash_aggregate::GroupKeyProgram::compile(
            &self.group_columns,
            &self.input_schema,
        )
        .ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "scalar HashAggregate group-key program is invalid",
            )
        })?;
        let mut saw_row = false;

        while let Some(batch) = self.input.next_batch()? {
            crate::engine::cancel::check_cancelled()?;
            self.batches_consumed = self.batches_consumed.saturating_add(1);
            for row in batch.visible_indices() {
                self.rows_consumed = self.rows_consumed.saturating_add(1);
                saw_row = true;
                key_program.encode_row(&batch, row, &mut key);
                if by_key.contains_key(&key) {
                    continue;
                }

                let containers_before =
                    Self::first_value_container_capacity_bytes(&by_key, groups.capacity());
                let owned_key = key.clone();
                let group = ScalarFirstValueGroup {
                    key_values: self
                        .group_columns
                        .iter()
                        .map(|&column| batch.columns()[column].value_at(row))
                        .collect(),
                    first_values: self
                        .slots
                        .iter()
                        .map(|runtime| {
                            batch.columns()[runtime.input_column.expect("preflighted")]
                                .value_at(row)
                        })
                        .collect(),
                };
                let owned_bytes = owned_key
                    .capacity()
                    .saturating_add(Self::first_value_group_owned_capacity_bytes(&group));
                groups.push(group);
                by_key.insert(owned_key, ());
                let container_growth =
                    Self::first_value_container_capacity_bytes(&by_key, groups.capacity())
                        .saturating_sub(containers_before);
                self.memory_charge
                    .add_with_workspace(container_growth.saturating_add(owned_bytes))?;
            }
        }

        if !saw_row && self.group_columns.is_empty() {
            groups.push(ScalarFirstValueGroup {
                key_values: Vec::new(),
                first_values: vec![Value::Null; self.slots.len()],
            });
        }

        let rows = groups
            .into_iter()
            .map(|mut group| {
                group.key_values.append(&mut group.first_values);
                group.key_values
            })
            .collect();
        self.output = Some(value_rows_into_batches(self.output_schema.clone(), rows).into_iter());
        Ok(())
    }

    /// Capacity owned by the two top-level hash-aggregate containers.  This
    /// is O(1); individual keys/groups are charged once when inserted.
    pub(super) fn container_capacity_bytes(
        by_key: &hashbrown::HashMap<Vec<u8>, usize, ahash::RandomState>,
        groups_capacity: usize,
    ) -> usize {
        by_key
            .capacity()
            .saturating_mul(std::mem::size_of::<Vec<u8>>() + std::mem::size_of::<usize>() + 64)
            .saturating_add(
                groups_capacity.saturating_mul(std::mem::size_of::<ScalarAggregateGroup>()),
            )
    }

    /// Capacity exclusively owned by one group, excluding the group header
    /// stored in the outer Vec and the encoded-key allocation stored in the
    /// HashMap.  Called once per new group instead of once per group per input
    /// batch.
    pub(super) fn group_owned_capacity_bytes(group: &ScalarAggregateGroup) -> usize {
        use crate::engine::executor::memory_footprint::AllocatedBytes;
        group
            .key_values
            .capacity()
            .saturating_mul(std::mem::size_of::<Value>())
            .saturating_add(
                group
                    .key_values
                    .iter()
                    .map(AllocatedBytes::allocated_bytes)
                    .sum::<usize>(),
            )
            .saturating_add(
                group
                    .accumulators
                    .capacity()
                    .saturating_mul(std::mem::size_of::<
                        Box<dyn crate::engine::executor::select::aggregate::Accumulator>,
                    >()),
            )
            .saturating_add(group.accounted_accumulator_heap_bytes)
            .saturating_add(
                group
                    .accounted_slot_heap_bytes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<usize>()),
            )
    }

    pub(super) fn make_group(&self, key_values: Vec<Value>) -> ScalarAggregateGroup {
        let accumulators: Vec<Box<dyn crate::engine::executor::select::aggregate::Accumulator>> =
            self.slots
                .iter()
                .map(|runtime| {
                    runtime.slot.make_accumulator_with_collation(
                        runtime
                            .input_column
                            .and_then(|column| self.input_schema.fields().get(column))
                            .and_then(|field| field.source.collation_id),
                    )
                })
                .collect();
        let accounted_slot_heap_bytes: Vec<usize> = accumulators
            .iter()
            .map(|accumulator| accumulator.resident_bytes())
            .collect();
        let accounted_accumulator_heap_bytes = accounted_slot_heap_bytes.iter().sum();
        ScalarAggregateGroup {
            key_values,
            accumulators,
            accounted_accumulator_heap_bytes,
            accounted_slot_heap_bytes,
        }
    }

    pub(super) fn initialize(&mut self) -> Result<(), QueryError> {
        if self.output.is_some() {
            return Ok(());
        }
        if self.hidden_first_value_only() {
            return self.initialize_hidden_first_values();
        }
        let mut by_key: hashbrown::HashMap<Vec<u8>, usize, ahash::RandomState> =
            hashbrown::HashMap::with_hasher(ahash::RandomState::new());
        let mut groups: Vec<ScalarAggregateGroup> = Vec::new();
        let mut key = Vec::with_capacity(64);
        let key_program = crate::engine::executor::hash_aggregate::GroupKeyProgram::compile(
            &self.group_columns,
            &self.input_schema,
        )
        .ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "scalar HashAggregate group-key program is invalid",
            )
        })?;
        let mut saw_row = false;

        while let Some(batch) = self.input.next_batch()? {
            crate::engine::cancel::check_cancelled()?;
            self.batches_consumed = self.batches_consumed.saturating_add(1);
            for row in batch.visible_indices() {
                self.rows_consumed = self.rows_consumed.saturating_add(1);
                saw_row = true;
                key_program.encode_row(&batch, row, &mut key);
                let group_index = if let Some(&index) = by_key.get(&key) {
                    index
                } else {
                    let containers_before =
                        Self::container_capacity_bytes(&by_key, groups.capacity());
                    let index = groups.len();
                    let owned_key = key.clone();
                    let key_values = self
                        .group_columns
                        .iter()
                        .map(|&column| batch.columns()[column].value_at(row))
                        .collect();
                    let group = self.make_group(key_values);
                    let owned_bytes = owned_key
                        .capacity()
                        .saturating_add(Self::group_owned_capacity_bytes(&group));
                    groups.push(group);
                    by_key.insert(owned_key, index);
                    let container_growth =
                        Self::container_capacity_bytes(&by_key, groups.capacity())
                            .saturating_sub(containers_before);
                    self.memory_charge
                        .add_with_workspace(container_growth.saturating_add(owned_bytes))?;
                    index
                };
                let group = &mut groups[group_index];
                for (slot, (runtime, accumulator)) in
                    self.slots.iter().zip(&mut group.accumulators).enumerate()
                {
                    if runtime.slot.is_count_star {
                        accumulator.update_count_star();
                        continue;
                    }
                    // Cover the argument/order copies before value_at can
                    // allocate them. Reconcile this slot's retained state
                    // before releasing the transient input owner.
                    let input_bytes = runtime
                        .input_column
                        .iter()
                        .chain(&runtime.order_columns)
                        .fold(std::mem::size_of::<Vec<Value>>(), |n, &column| {
                            n.saturating_add(batch.columns()[column].owned_value_bytes(row))
                        });
                    let _input_charge = mem_budget::force_charge_scoped(input_bytes)?;
                    let mut value = runtime
                        .input_column
                        .map(|column| batch.columns()[column].try_value_at(row))
                        .transpose()?
                        .unwrap_or(Value::Null);
                    if runtime.approximate_input && !matches!(value, Value::Null) {
                        value = Value::Double(crate::engine::expr::numeric(&value)?);
                    }
                    if runtime.slot.has_order_by() {
                        let mut order = mem_budget::try_vec_with_capacity(
                            runtime.order_columns.len(),
                            "scalar aggregate order keys",
                        )?;
                        for &column in &runtime.order_columns {
                            order.push(batch.columns()[column].try_value_at(row)?);
                        }
                        accumulator.update_ordered(value, order)?;
                    } else {
                        accumulator.update(value)?;
                    }
                    let heap = accumulator.resident_bytes();
                    if heap > group.accounted_slot_heap_bytes[slot] {
                        let extra = heap - group.accounted_slot_heap_bytes[slot];
                        self.memory_charge.add_with_workspace(extra)?;
                        group.accounted_accumulator_heap_bytes =
                            group.accounted_accumulator_heap_bytes.saturating_add(extra);
                        group.accounted_slot_heap_bytes[slot] = heap;
                    }
                }
            }
        }

        // SQL aggregate without GROUP BY always returns one row, including
        // for an empty input (COUNT=0, SUM/AVG/MIN/MAX=NULL, etc.).
        if !saw_row && self.group_columns.is_empty() {
            groups.push(self.make_group(Vec::new()));
        }

        let mut rows = Vec::with_capacity(groups.len());
        for group in groups {
            let mut row = group.key_values;
            for (slot_index, (runtime, accumulator)) in
                self.slots.iter().zip(group.accumulators).enumerate()
            {
                let value = accumulator.finalize()?;
                let value = match (runtime.fixed_output_scale, value) {
                    (Some(scale), Value::Double(value)) => {
                        Value::Decimal(format!("{value:.scale$}", scale = usize::from(scale)))
                    }
                    (_, value) => value,
                };
                let value = match self
                    .output_schema
                    .fields()
                    .get(self.group_columns.len() + slot_index)
                    .map(|field| field.column_type)
                {
                    Some(ColumnType::Decimal { scale, .. }) => decimal_value_at_scale(value, scale),
                    _ => value,
                };
                row.push(value);
            }
            rows.push(row);
        }
        self.output = Some(value_rows_into_batches(self.output_schema.clone(), rows).into_iter());
        Ok(())
    }
}
