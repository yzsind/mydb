//! aggregate execution and preparation.

use super::*;

pub(in crate::engine::executor::select) fn execute_hash_aggregate_keyed(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    group_by: &[Expr],
    having: Option<&Expr>,
    aggregates: &[Expr],
    plan_for_stats: Option<&PhysicalPlan>,
) -> Result<Vec<PlanRow>, QueryError> {
    let partition_wise_active = PARTITION_WISE_AGG_ACTIVE.with(std::cell::Cell::get);
    if !partition_wise_active {
        if let Some(storage_ids) = partition_wise_aggregate_storage_ids(input, group_by) {
            let mut output = Vec::new();
            PARTITION_WISE_AGG_ACTIVE.with(|active| active.set(true));
            let result = (|| {
                for storage_id in storage_ids {
                    let mut rows =
                        crate::engine::executor::partition_router::with_pruned_partitions(
                            vec![storage_id],
                            || {
                                execute_hash_aggregate_keyed(
                                    db,
                                    txn,
                                    input,
                                    group_by,
                                    having,
                                    aggregates,
                                    plan_for_stats,
                                )
                            },
                        )?;
                    output.append(&mut rows);
                }
                Ok(output)
            })();
            PARTITION_WISE_AGG_ACTIVE.with(|active| active.set(false));
            return result;
        }
    }
    let mut slots: Vec<crate::engine::executor::select::aggregate::AggregateSlot> = {
        let mut slot_exprs: Vec<&Expr> = aggregates.iter().collect();
        if let Some(h) = having {
            slot_exprs.push(h);
        }
        crate::engine::executor::select::aggregate::collect_aggregate_slots(&slot_exprs)
    };
    crate::engine::executor::select::aggregate::bind_slot_comparisons(&mut slots, input);

    if fixed_scale_approximate_aggregate_requires_row_oracle(input, &slots) {
        // Fast aggregate routes intentionally prune input descriptors, but a
        // fixed-scale FLOAT/DOUBLE SUM/AVG needs the declared scale for
        // HAVING and protocol finalization. Materialize the chosen physical
        // input and delegate to the schema-preserving row oracle.
        let rows = execute_plan_rows(db, txn, input)?;
        return aggregate_plan_rows(rows, group_by, having, &slots);
    }

    // HashAggregate is lowered through the common BatchOperator compiler
    // before consulting any of the historical scan/join-shape helpers.  The
    // compiler performs a complete capability preflight before opening a
    // source and uses scalar lanes for expressions without a typed kernel, so
    // a successful lowering covers HashJoin/INLJ/scan inputs without a
    // query-shape-specific execution branch or joined PlanRow materialization.
    if let Some(rows) = crate::engine::executor::select::vec_bridge::try_vec_hash_aggregate(
        db, txn, input, group_by, having, aggregates,
    )? {
        if let Some(plan) = plan_for_stats {
            crate::engine::executor::select::vec_bridge::forward_vec_hash_agg_metrics(plan);
        }
        return Ok(rows);
    }

    // The common batch compiler rejected the node before opening its input.
    // Keep one generic row-stream oracle while the remaining aggregate functions
    // are migrated; do not consult scan/join-shape-specific executors here.
    let estimated_input_rows = crate::planner::estimated_rows(input)
        .ceil()
        .max(0.0)
        .min(usize::MAX as f64) as usize;
    if should_stream_spill_row_hash_agg(estimated_input_rows, group_by, &slots) {
        return aggregate_plan_stream_spilled(db, txn, input, group_by, having, &slots);
    }

    // S1d incremental consumption at the chain top: when the input is a chained streaming join, the whole join result is no longer
    // materialized into a Vec before aggregating — rows stream one by one into the in-memory aggregate, eliminating the last full
    // materialization point at the chain tail. Group-state memory still goes through the group_charge budget as usual.
    //
    if crate::engine::executor::select::join_stream::chain_applicable(db, input) {
        // Estimate-INDEPENDENT spill: try the in-memory streaming agg, but
        // if its group table overflows the budget at runtime (CBO
        // under-estimated the cardinality — TPC-DS Q14/Q23), retry through
        // the disk-spilling path. The input plan is re-streamable, so this
        // degrades a 1037 into a one-time re-scan + spill.
        match aggregate_plan_stream_in_memory(db, txn, input, group_by, having, &slots) {
            Err(e) if is_hash_agg_stream_overflow(&e) => {
                record_row_hash_agg_adaptive_spill();
                return aggregate_plan_stream_spilled(db, txn, input, group_by, having, &slots);
            }
            other => return other,
        }
    }

    let input_rows = execute_plan_rows(db, txn, input)?;
    aggregate_plan_rows_maybe_spilled(db, input_rows, group_by, having, &slots)
}

/// Stream rows without materializing the input. The shared state preserves
/// the single-key map and the stream's retryable memory-overflow contract.
pub(super) fn aggregate_plan_stream_in_memory(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    group_by: &[Expr],
    having: Option<&Expr>,
    slots: &[crate::engine::executor::select::aggregate::AggregateSlot],
) -> Result<Vec<PlanRow>, QueryError> {
    let mut stream = execute_plan_stream(db, txn, input)?;
    aggregate_row_stream(stream.as_mut(), group_by, having, slots)
}

pub(super) fn aggregate_row_stream(
    stream: &mut dyn PlanStream,
    group_by: &[Expr],
    having: Option<&Expr>,
    slots: &[crate::engine::executor::select::aggregate::AggregateSlot],
) -> Result<Vec<PlanRow>, QueryError> {
    let mut program = None;
    if group_by.is_empty() {
        const STREAM_AGG_CHUNK: usize = 1024;
        let any_ordered = slots.iter().any(|slot| slot.has_order_by());
        let mut state = None;
        let mut chunk = Vec::with_capacity(STREAM_AGG_CHUNK);
        while let Some(row) = stream.next()? {
            crate::engine::cancel::check_cancelled()?;
            let program = program
                .get_or_insert_with(|| RowAggregateProgram::new(Some(&row), group_by, slots, true));
            let state = state.get_or_insert_with(|| program.implicit_group(Some(&row)));
            if any_ordered {
                program.update(state, &row)?;
            } else {
                chunk.push(row);
                if chunk.len() >= STREAM_AGG_CHUNK {
                    program.update_batch(state, &chunk)?;
                    chunk.clear();
                }
            }
        }
        if !chunk.is_empty() {
            program
                .as_ref()
                .expect("nonempty stream has compiled inputs")
                .update_batch(state.as_mut().expect("nonempty stream has a group"), &chunk)?;
        }
        // The streaming implicit-group result has no input field template.
        let state = state.unwrap_or_else(|| RowAggregateGroup::empty(slots));
        Ok(state.finish(&[], &[], slots, having)?.into_iter().collect())
    } else {
        let mut groups = RowAggregateGroups::new();
        while let Some(row) = stream.next()? {
            crate::engine::cancel::check_cancelled()?;
            let program = program
                .get_or_insert_with(|| RowAggregateProgram::new(Some(&row), group_by, slots, true));
            groups.update::<true>(program, &row)?;
        }
        match program {
            Some(program) => groups.finish(&program, group_by, having),
            None => Ok(Vec::new()),
        }
    }
}

/// Pure-input version of `execute_hash_aggregate`'s body. Once the
/// caller has materialised input rows (serial: via `execute_plan_rows`;
/// parallel: via hash-partitioning), this helper builds the per-group
/// accumulators, finalises them, and applies HAVING. Used by both the
/// serial path and Phase 3's `ParallelHashAggregate` workers.
pub(super) fn row_hash_agg_spill_partitions() -> usize {
    crate::engine::executor::vec::hash_agg::NUM_PARTITIONS
}

/// Internal sentinel: the streaming in-memory hash aggregate's group
/// table exceeded the per-query / process budget at *runtime*. The CBO
/// estimate said it would fit, so `execute_hash_aggregate_keyed` catches
/// this and retries through the eager disk-spilling path (the input plan
/// is re-streamable). This makes the spill decision estimate-INDEPENDENT:
/// a cardinality under-estimate degrades to a one-time re-scan + spill
/// instead of a 1037.
pub(in crate::engine::executor::select) fn hash_agg_stream_overflow() -> QueryError {
    QueryError::internal_control(
        crate::error::InternalErrorKind::HashAggregateNeedsSpill,
        crate::error::catalog::ER_OUTOFMEMORY,
        "row hash aggregate exceeded the in-memory work area; retry via spill",
    )
}

pub(in crate::engine::executor::select) fn is_hash_agg_stream_overflow(e: &QueryError) -> bool {
    e.is_internal(crate::error::InternalErrorKind::HashAggregateNeedsSpill)
}

/// Charge `extra` against the streaming aggregate's group-table budget.
/// On a real 1037 (per-query or process limit), convert it into the
/// re-tryable [`hash_agg_stream_overflow`] sentinel so the operator
/// spills instead of failing. Any other error propagates unchanged.
pub(super) fn charge_group_or_overflow(
    group_charge: &mut crate::engine::executor::mem_budget::ScopedCharge,
    extra: usize,
) -> Result<(), QueryError> {
    match group_charge.add(extra) {
        Ok(()) => Ok(()),
        Err(e) if e.is(crate::error::catalog::ER_OUTOFMEMORY) => Err(hash_agg_stream_overflow()),
        Err(e) => Err(e),
    }
}

pub(in crate::engine::executor::select) fn should_spill_row_hash_agg(
    input_rows: usize,
    group_by: &[Expr],
    slots: &[crate::engine::executor::select::aggregate::AggregateSlot],
) -> bool {
    if group_by.is_empty() || input_rows < 1024 {
        return false;
    }
    let Some(budget) = crate::engine::executor::mem_budget::current_budget() else {
        return false;
    };
    let per_group = 64usize
        .saturating_add(slots.len().saturating_mul(32))
        .saturating_add(group_by.len().saturating_mul(32));
    let estimate = input_rows.saturating_mul(per_group);
    estimate > budget.remaining().max(budget.limit() / 4)
}

pub(super) fn should_stream_spill_row_hash_agg(
    estimated_input_rows: usize,
    group_by: &[Expr],
    slots: &[crate::engine::executor::select::aggregate::AggregateSlot],
) -> bool {
    if should_spill_row_hash_agg(estimated_input_rows, group_by, slots) {
        return true;
    }
    if group_by.is_empty() {
        return false;
    }
    crate::engine::executor::mem_budget::current_budget()
        .map(|budget| budget.limit() <= 4 * 1024 * 1024)
        .unwrap_or(false)
}

pub(super) fn aggregate_plan_rows_maybe_spilled(
    db: &Arc<Database>,
    input_rows: Vec<PlanRow>,
    group_by: &[Expr],
    having: Option<&Expr>,
    slots: &[crate::engine::executor::select::aggregate::AggregateSlot],
) -> Result<Vec<PlanRow>, QueryError> {
    if should_spill_row_hash_agg(input_rows.len(), group_by, slots) {
        return aggregate_plan_rows_spilled(db, input_rows, group_by, having, slots);
    }
    aggregate_plan_rows(input_rows, group_by, having, slots)
}

pub(super) fn simple_group_field_positions(
    group_by: &[Expr],
    template: &[PlanField],
) -> Vec<Option<usize>> {
    group_by
        .iter()
        .map(|expr| {
            let (qualifier, column) = match expr {
                Expr::Identifier(name) => (None, name.as_str()),
                Expr::CompoundIdentifier(parts) if !parts.is_empty() => {
                    let column = parts.last()?.as_str();
                    let qualifier = parts
                        .len()
                        .checked_sub(2)
                        .and_then(|idx| parts.get(idx))
                        .map(String::as_str);
                    (qualifier, column)
                }
                _ => return None,
            };
            let mut found = None;
            for (idx, field) in template.iter().enumerate() {
                if !field.column.eq_ignore_ascii_case(column) {
                    continue;
                }
                if let Some(q) = qualifier {
                    if !crate::engine::executor::select::eval::field_matches_qualifier(field, q) {
                        continue;
                    }
                }
                if found.is_some() {
                    return None;
                }
                found = Some(idx);
            }
            found
        })
        .collect()
}

pub(super) fn write_group_keys_to_simple_fields(
    fields: &mut [PlanField],
    positions: &[Option<usize>],
    key_values: &[Value],
) {
    for (position, value) in positions.iter().zip(key_values.iter()) {
        if let Some(idx) = position {
            if let Some(field) = fields.get_mut(*idx) {
                field.value = value.clone();
            }
        }
    }
}

struct RowAggregateSpillWriters<'a> {
    writers: Vec<Option<crate::engine::executor::spill_quota::TrackedSpillWriter>>,
    tmp: &'a SpillTmpDir,
    capacity: usize,
}

impl<'a> RowAggregateSpillWriters<'a> {
    fn new(tmp: &'a SpillTmpDir, partitions: usize) -> Self {
        Self {
            writers: (0..partitions).map(|_| None).collect(),
            tmp,
            capacity: crate::engine::executor::spill_quota::writer_buffer_capacity(partitions),
        }
    }

    fn writer(
        &mut self,
        partition: usize,
    ) -> Result<&mut crate::engine::executor::spill_quota::TrackedSpillWriter, QueryError> {
        let slot = &mut self.writers[partition];
        if slot.is_none() {
            let path = self.tmp.path().join(format!("rowagg-{partition:03}.bin"));
            *slot = Some(
                crate::engine::executor::spill_quota::TrackedSpillWriter::create_new(
                    &path,
                    self.tmp.charger(),
                    self.capacity,
                )?,
            );
        }
        Ok(slot.as_mut().expect("occupied partition has a writer"))
    }

    fn finish(self) -> Result<Vec<usize>, QueryError> {
        let mut occupied = Vec::new();
        for (partition, writer) in self.writers.into_iter().enumerate() {
            if let Some(writer) = writer {
                writer.finish()?;
                occupied.push(partition);
            }
        }
        Ok(occupied)
    }
}

pub(super) fn merge_row_hash_agg_spill_partitions(
    tmp: &SpillTmpDir,
    occupied_partitions: &[usize],
    schema_template: &Arc<Vec<PlanField>>,
    group_field_positions: &[Option<usize>],
    slots: &[crate::engine::executor::select::aggregate::AggregateSlot],
    having: Option<&Expr>,
) -> Result<Vec<PlanRow>, QueryError> {
    use crate::engine::expr::ValueKey;
    use hashbrown::hash_map::Entry;

    let template = PlanRow::from_fields(schema_template.as_ref().clone());
    let mut bound_slots = slots.to_vec();
    super::super::aggregate::bind_slot_comparisons_for_row(&mut bound_slots, &template);
    let slots = bound_slots.as_slice();
    let mut out = Vec::new();
    for &idx in occupied_partitions {
        crate::engine::cancel::check_cancelled()?;
        let path = tmp.path().join(format!("rowagg-{idx:03}.bin"));
        let mut reader = crate::engine::executor::spill_quota::TrackedSpillReader::open(
            &path,
            tmp.charger(),
            crate::engine::executor::spill_quota::writer_buffer_capacity(1),
        )?;
        let mut groups: hashbrown::HashMap<Vec<ValueKey>, RowAggregateGroup, ahash::RandomState> =
            hashbrown::HashMap::with_hasher(ahash::RandomState::new());
        let mut group_charge = crate::engine::executor::mem_budget::ScopedCharge::new(0);
        while let Some((key_values, slot_inputs, order_keys, sample_values)) =
            read_row_hash_agg_spill_record(&mut reader)?
        {
            let key: Vec<ValueKey> = key_values.iter().map(ValueKey::from).collect();
            let charge =
                key.iter().map(|vk| 24 + vk.payload_size()).sum::<usize>() + 64 + slots.len() * 32;
            match groups.entry(key) {
                Entry::Occupied(mut e) => {
                    update_spilled_accumulators(
                        slots,
                        &mut e.get_mut().accumulators,
                        &slot_inputs,
                        &order_keys,
                    )?;
                }
                Entry::Vacant(e) => {
                    group_charge.add_or_fallback(charge)?;
                    let mut state = RowAggregateGroup {
                        key_values,
                        sample_values,
                        accumulators: slots.iter().map(|slot| slot.make_accumulator()).collect(),
                    };
                    update_spilled_accumulators(
                        slots,
                        &mut state.accumulators,
                        &slot_inputs,
                        &order_keys,
                    )?;
                    e.insert(state);
                }
            }
        }
        for state in groups.into_values() {
            if let Some(row) =
                state.finish(schema_template, group_field_positions, slots, having)?
            {
                out.push(row);
            }
        }
    }
    Ok(out)
}

pub(in crate::engine::executor::select) fn aggregate_plan_rows_spilled(
    db: &Arc<Database>,
    input_rows: Vec<PlanRow>,
    group_by: &[Expr],
    having: Option<&Expr>,
    slots: &[crate::engine::executor::select::aggregate::AggregateSlot],
) -> Result<Vec<PlanRow>, QueryError> {
    use crate::engine::expr::ValueKey;
    use std::hash::{BuildHasher, Hash, Hasher};

    if input_rows.is_empty() {
        return aggregate_plan_rows(input_rows, group_by, having, slots);
    }

    let partitions = row_hash_agg_spill_partitions();
    let partition_mask = partitions - 1;
    let hasher_state = ahash::RandomState::new();
    let tmp = SpillTmpDir::create_kind(db.data_dir(), "hashagg")?;
    // Keep logical fanout and hashing unchanged, but reserve files/buffers
    // only for occupied buckets. Replay must still open every claimed file.
    let mut writers = RowAggregateSpillWriters::new(&tmp, partitions);

    let schema_template: Arc<Vec<PlanField>> = Arc::new(
        input_rows[0]
            .fields
            .iter()
            .map(|f| PlanField {
                meta: f.meta.clone(),
                value: Value::Null,
            })
            .collect(),
    );
    let group_field_positions = simple_group_field_positions(group_by, schema_template.as_slice());
    let preserve_sample_values = aggregate_slots_need_sample_fields(slots);

    for row in &input_rows {
        crate::engine::cancel::check_cancelled()?;
        let key_values = group_by
            .iter()
            .map(|expr| eval_plan_expr(expr, row))
            .collect::<Result<Vec<_>, _>>()?;
        let key: Vec<ValueKey> = key_values.iter().map(ValueKey::from).collect();
        let mut hasher = hasher_state.build_hasher();
        key.hash(&mut hasher);
        let pidx = (hasher.finish() as usize) & partition_mask;
        let slot_inputs = rollup_slot_inputs(slots, row)?;
        let order_keys = spill_aggregate_order_keys(slots, row)?;
        let sample_values: Vec<Value> = if preserve_sample_values {
            row.fields.iter().map(|f| f.value.clone()).collect()
        } else {
            Vec::new()
        };
        write_row_hash_agg_spill_record(
            writers.writer(pidx)?,
            &key_values,
            &slot_inputs,
            &order_keys,
            &sample_values,
        )?;
    }
    let occupied_partitions = writers.finish()?;

    merge_row_hash_agg_spill_partitions(
        &tmp,
        &occupied_partitions,
        &schema_template,
        &group_field_positions,
        slots,
        having,
    )
}

pub(in crate::engine::executor::select) fn aggregate_plan_stream_spilled(
    db: &Arc<Database>,
    txn: TxnView,
    input: &PhysicalPlan,
    group_by: &[Expr],
    having: Option<&Expr>,
    slots: &[crate::engine::executor::select::aggregate::AggregateSlot],
) -> Result<Vec<PlanRow>, QueryError> {
    use crate::engine::expr::ValueKey;
    use std::hash::{BuildHasher, Hash, Hasher};

    let partitions = row_hash_agg_spill_partitions();
    let partition_mask = partitions - 1;
    let hasher_state = ahash::RandomState::new();
    let tmp = SpillTmpDir::create_kind(db.data_dir(), "hashagg")?;
    // Keep logical fanout and hashing unchanged, but reserve files/buffers
    // only for occupied buckets. Replay must still open every claimed file.
    let mut writers = RowAggregateSpillWriters::new(&tmp, partitions);

    let mut stream = execute_plan_stream(db, txn, input)?;
    let mut schema_template: Option<Arc<Vec<PlanField>>> = None;
    let preserve_sample_values = aggregate_slots_need_sample_fields(slots);
    while let Some(row) = stream.next()? {
        crate::engine::cancel::check_cancelled()?;
        if schema_template.is_none() {
            schema_template = Some(Arc::new(
                row.fields
                    .iter()
                    .map(|f| PlanField {
                        meta: f.meta.clone(),
                        value: Value::Null,
                    })
                    .collect(),
            ));
        }
        let key_values = group_by
            .iter()
            .map(|expr| eval_plan_expr(expr, &row))
            .collect::<Result<Vec<_>, _>>()?;
        let key: Vec<ValueKey> = key_values.iter().map(ValueKey::from).collect();
        let mut hasher = hasher_state.build_hasher();
        key.hash(&mut hasher);
        let pidx = (hasher.finish() as usize) & partition_mask;
        let slot_inputs = rollup_slot_inputs(slots, &row)?;
        let order_keys = spill_aggregate_order_keys(slots, &row)?;
        let sample_values: Vec<Value> = if preserve_sample_values {
            row.fields.iter().map(|f| f.value.clone()).collect()
        } else {
            Vec::new()
        };
        write_row_hash_agg_spill_record(
            writers.writer(pidx)?,
            &key_values,
            &slot_inputs,
            &order_keys,
            &sample_values,
        )?;
    }
    let occupied_partitions = writers.finish()?;
    let Some(schema_template) = schema_template else {
        return aggregate_plan_rows(Vec::new(), group_by, having, slots);
    };
    let group_field_positions = simple_group_field_positions(group_by, schema_template.as_slice());

    merge_row_hash_agg_spill_partitions(
        &tmp,
        &occupied_partitions,
        &schema_template,
        &group_field_positions,
        slots,
        having,
    )
}

pub(super) fn write_u32_local<W: std::io::Write>(w: &mut W, n: usize) -> Result<(), QueryError> {
    let n = u32::try_from(n).map_err(|_| {
        crate::storage::error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "hash aggregate spill record is too large",
        )
    })?;
    w.write_all(&n.to_le_bytes()).map_err(io_err)
}

pub(super) fn read_u32_local<R: std::io::Read>(r: &mut R) -> Result<Option<u32>, QueryError> {
    let mut b = [0u8; 4];
    match r.read_exact(&mut b) {
        Ok(()) => Ok(Some(u32::from_le_bytes(b))),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(io_err(e)),
    }
}

pub(super) fn write_values_local<W: std::io::Write>(
    w: &mut W,
    values: &[Value],
) -> Result<(), QueryError> {
    write_u32_local(w, values.len())?;
    for value in values {
        crate::engine::executor::vec::spill::write_value(w, value)?;
    }
    Ok(())
}

pub(super) fn read_values_local<R: std::io::Read>(
    r: &mut R,
    len: usize,
) -> Result<Vec<Value>, QueryError> {
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(crate::engine::executor::vec::spill::read_value(r)?);
    }
    Ok(values)
}

pub(super) fn write_row_hash_agg_spill_record<W: std::io::Write>(
    w: &mut W,
    key_values: &[Value],
    slot_inputs: &[Option<Value>],
    order_keys: &[Vec<Value>],
    sample_values: &[Value],
) -> Result<(), QueryError> {
    write_values_local(w, key_values)?;
    write_u32_local(w, slot_inputs.len())?;
    for input in slot_inputs {
        crate::engine::executor::vec::spill::write_value(
            w,
            input.as_ref().unwrap_or(&Value::Null),
        )?;
    }
    write_u32_local(w, order_keys.len())?;
    for keys in order_keys {
        write_values_local(w, keys)?;
    }
    write_values_local(w, sample_values)
}

pub(super) fn spill_aggregate_order_keys(
    slots: &[crate::engine::executor::select::aggregate::AggregateSlot],
    row: &PlanRow,
) -> Result<Vec<Vec<Value>>, QueryError> {
    if !slots.iter().any(|slot| slot.has_order_by()) {
        return Ok(Vec::new());
    }
    slots
        .iter()
        .map(|slot| slot.eval_order_keys(|expr| eval_plan_expr(expr, row)))
        .collect()
}

pub(super) fn update_spilled_accumulators(
    slots: &[crate::engine::executor::select::aggregate::AggregateSlot],
    accumulators: &mut [Box<dyn crate::engine::executor::select::aggregate::Accumulator>],
    inputs: &[Value],
    order_keys: &[Vec<Value>],
) -> Result<(), QueryError> {
    for (index, ((slot, accumulator), input)) in
        slots.iter().zip(accumulators).zip(inputs).enumerate()
    {
        if slot.is_count_star {
            accumulator.update_count_star();
        } else if slot.has_order_by() {
            let keys = order_keys.get(index).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "ordered aggregate spill is missing sort keys",
                )
            })?;
            accumulator.update_ordered(input.clone(), keys.clone())?;
        } else {
            accumulator.update_ref(input)?;
        }
    }
    Ok(())
}

pub(super) type RowHashAggSpillRecord = (Vec<Value>, Vec<Value>, Vec<Vec<Value>>, Vec<Value>);

pub(super) fn read_row_hash_agg_spill_record<R: std::io::Read>(
    r: &mut R,
) -> Result<Option<RowHashAggSpillRecord>, QueryError> {
    let Some(key_len) = read_u32_local(r)? else {
        return Ok(None);
    };
    let key_values = read_values_local(r, key_len as usize)?;
    let Some(slot_len) = read_u32_local(r)? else {
        return Err(crate::storage::error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "truncated hash aggregate spill record",
        ));
    };
    let slot_inputs = read_values_local(r, slot_len as usize)?;
    let Some(order_slots) = read_u32_local(r)? else {
        return Err(error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "truncated aggregate spill order keys",
        ));
    };
    let mut order_keys = Vec::with_capacity(order_slots as usize);
    for _ in 0..order_slots {
        let Some(length) = read_u32_local(r)? else {
            return Err(error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "truncated aggregate spill order keys",
            ));
        };
        order_keys.push(read_values_local(r, length as usize)?);
    }
    let Some(sample_len) = read_u32_local(r)? else {
        return Err(crate::storage::error(
            crate::error::catalog::ER_UNKNOWN_ERROR,
            "truncated hash aggregate spill record",
        ));
    };
    let sample_values = read_values_local(r, sample_len as usize)?;
    Ok(Some((key_values, slot_inputs, order_keys, sample_values)))
}

/// Materialized input uses the same row aggregate state as streaming and
/// parallel callers, while retaining its whole-input batch fast path.
pub(in crate::engine::executor::select) fn aggregate_plan_rows(
    input_rows: Vec<PlanRow>,
    group_by: &[Expr],
    having: Option<&Expr>,
    slots: &[crate::engine::executor::select::aggregate::AggregateSlot],
) -> Result<Vec<PlanRow>, QueryError> {
    let retain_sample = slots.iter().any(|slot| {
        slot.name
            .eq_ignore_ascii_case(crate::parser::HIDDEN_FIRST_VALUE_FN)
    });
    let program = RowAggregateProgram::new(input_rows.first(), group_by, slots, retain_sample);
    if group_by.is_empty() {
        const BATCH_PATH_MIN_ROWS: usize = 64;
        let mut state = program.implicit_group(input_rows.first());
        if input_rows.len() >= BATCH_PATH_MIN_ROWS && !slots.iter().any(|slot| slot.has_order_by())
        {
            program.update_batch(&mut state, &input_rows)?;
        } else {
            for row in &input_rows {
                crate::engine::cancel::check_cancelled()?;
                program.update(&mut state, row)?;
            }
        }
        Ok(state
            .finish(&program.template, &[], slots, having)?
            .into_iter()
            .collect())
    } else {
        let mut groups = RowAggregateGroups::new();
        for row in &input_rows {
            crate::engine::cancel::check_cancelled()?;
            groups.update::<false>(&program, row)?;
        }
        groups.finish(&program, group_by, having)
    }
}
