//! Common row HashAggregate state. Input ownership, batching and spill retry
//! remain caller policies; grouping, expression evaluation and output do not.
use super::*;
use crate::engine::executor::mem_budget::ScopedCharge;
use crate::engine::executor::select::aggregate::{group_marker_field, Accumulator, AggregateSlot};
use crate::engine::expr::ValueKey;
use hashbrown::hash_map::Entry;

type GroupMap<K> = hashbrown::HashMap<K, RowAggregateGroup, ahash::RandomState>;

pub(super) struct RowAggregateGroup {
    pub(super) key_values: Vec<Value>,
    pub(super) sample_values: Vec<Value>,
    pub(super) accumulators: Vec<Box<dyn Accumulator>>,
}

impl RowAggregateGroup {
    pub(super) fn empty(slots: &[AggregateSlot]) -> Self {
        Self {
            key_values: Vec::new(),
            sample_values: Vec::new(),
            accumulators: slots.iter().map(|slot| slot.make_accumulator()).collect(),
        }
    }

    /// Used by resident groups and by row spill replay. Keep HAVING after
    /// fallible finalization, and restore plain group columns before emitting
    /// the synthetic group markers consumed by projection.
    pub(super) fn finish(
        self,
        template: &[PlanField],
        positions: &[Option<usize>],
        slots: &[AggregateSlot],
        having: Option<&Expr>,
    ) -> Result<Option<PlanRow>, QueryError> {
        let mut cache = HashMap::with_capacity(slots.len());
        for (slot, acc) in slots.iter().zip(self.accumulators) {
            cache.insert(slot.key, acc.finalize()?);
        }
        let mut fields = template.to_vec();
        for (field, value) in fields.iter_mut().zip(self.sample_values) {
            field.value = value;
        }
        write_group_keys_to_simple_fields(&mut fields, positions, &self.key_values);
        fields.extend(
            self.key_values
                .into_iter()
                .enumerate()
                .map(|(i, value)| group_marker_field(i, value)),
        );
        let row = PlanRow {
            fields,
            group: Some(Vec::new()),
            agg_values: Some(Arc::new(cache)),
            schema: PlanRow::empty_schema(),
        };
        if having
            .map(|expr| eval_aggregate_bool(expr, &[], &row))
            .transpose()?
            .unwrap_or(true)
        {
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }
}

pub(super) struct RowAggregateProgram {
    slots: Vec<AggregateSlot>,
    group_by: Vec<CompiledPlanExpr>,
    inputs: Vec<Option<CompiledPlanExpr>>,
    pub(super) template: Vec<PlanField>,
    retain_sample: bool,
}

impl RowAggregateProgram {
    pub(super) fn new(
        row: Option<&PlanRow>,
        group_by: &[Expr],
        slots: &[AggregateSlot],
        retain_sample: bool,
    ) -> Self {
        let schema = row.map(input_schema_for_plan_row);
        let mut slots = slots.to_vec();
        if let Some(schema) = &schema {
            for slot in &mut slots {
                if matches!(
                    slot.kind(),
                    Some(
                        super::super::aggregate::AggregateKind::Min
                            | super::super::aggregate::AggregateKind::Max
                    )
                ) {
                    slot.comparison_collation = slot.args.first().and_then(|expr| {
                        crate::engine::expr_program::input_expr_type(expr, schema).collation_id
                    });
                }
            }
        }
        Self {
            group_by: schema
                .as_ref()
                .map(|schema| {
                    group_by
                        .iter()
                        .map(|expr| CompiledPlanExpr::compile_once(expr, schema))
                        .collect()
                })
                .unwrap_or_default(),
            inputs: schema
                .as_ref()
                .map(|schema| {
                    slots
                        .iter()
                        .map(|slot| {
                            slot.args
                                .first()
                                .map(|expr| CompiledPlanExpr::compile_once(expr, schema))
                        })
                        .collect()
                })
                .unwrap_or_default(),
            template: row
                .map(|row| {
                    row.fields
                        .iter()
                        .map(|field| PlanField {
                            meta: field.meta.clone(),
                            value: Value::Null,
                        })
                        .collect()
                })
                .unwrap_or_default(),
            retain_sample,
            slots,
        }
    }

    fn new_group(&self, row: &PlanRow, key_values: Vec<Value>) -> RowAggregateGroup {
        RowAggregateGroup {
            key_values,
            sample_values: if self.retain_sample {
                row.fields.iter().map(|f| f.value.clone()).collect()
            } else {
                Vec::new()
            },
            accumulators: self
                .slots
                .iter()
                .map(|slot| slot.make_accumulator())
                .collect(),
        }
    }

    pub(super) fn implicit_group(&self, row: Option<&PlanRow>) -> RowAggregateGroup {
        row.map(|row| self.new_group(row, Vec::new()))
            .unwrap_or_else(|| RowAggregateGroup::empty(&self.slots))
    }

    #[inline]
    fn input(
        &self,
        index: usize,
        slot: &AggregateSlot,
        row: &PlanRow,
    ) -> Result<Value, QueryError> {
        self.inputs
            .get(index)
            .and_then(Option::as_ref)
            .map(|expr| expr.eval(row))
            .unwrap_or_else(|| eval_plan_expr(&slot.args[0], row))
    }

    pub(super) fn update(
        &self,
        state: &mut RowAggregateGroup,
        row: &PlanRow,
    ) -> Result<(), QueryError> {
        for (index, (slot, acc)) in self.slots.iter().zip(&mut state.accumulators).enumerate() {
            if slot.is_count_star {
                acc.update_count_star();
            } else if slot.args.is_empty() {
                acc.update(Value::Null)?;
            } else {
                let value = self.input(index, slot, row)?;
                if slot.has_order_by() {
                    let keys = slot.eval_order_keys(|expr| eval_plan_expr(expr, row))?;
                    acc.update_ordered(value, keys)?;
                } else {
                    acc.update(value)?;
                }
            }
        }
        Ok(())
    }

    /// The caller chooses the original batch size and ordered-aggregate
    /// fallback. Slot-major evaluation order and cancellation are preserved.
    pub(super) fn update_batch(
        &self,
        state: &mut RowAggregateGroup,
        rows: &[PlanRow],
    ) -> Result<(), QueryError> {
        for (index, (slot, acc)) in self.slots.iter().zip(&mut state.accumulators).enumerate() {
            crate::engine::cancel::check_cancelled()?;
            if slot.is_count_star {
                acc.update_count_star_batch(rows.len());
            } else if slot.args.is_empty() {
                acc.update_batch(&vec![Value::Null; rows.len()])?;
            } else {
                let mut values = Vec::with_capacity(rows.len());
                for row in rows {
                    values.push(self.input(index, slot, row)?);
                }
                acc.update_batch(&values)?;
            }
        }
        Ok(())
    }
}

pub(super) struct RowAggregateGroups {
    single: GroupMap<ValueKey>,
    multi: GroupMap<Vec<ValueKey>>,
    charge: ScopedCharge,
}

impl RowAggregateGroups {
    pub(super) fn new() -> Self {
        Self {
            single: GroupMap::default(),
            multi: GroupMap::default(),
            charge: ScopedCharge::new(0),
        }
    }

    /// Monomorphize the overflow policy: only the re-streamable entry point
    /// may request a spill retry. Materialized/parallel callers keep 1037.
    #[inline]
    pub(super) fn update<const RETRY_SPILL: bool>(
        &mut self,
        program: &RowAggregateProgram,
        row: &PlanRow,
    ) -> Result<(), QueryError> {
        let per_group_bytes = 64 + program.slots.len() * 32;
        if program.group_by.len() == 1 {
            let value = program.group_by[0].eval(row)?;
            let key = ValueKey::from(&value);
            let bytes = 24 + key.payload_size() + per_group_bytes;
            match self.single.entry(key) {
                Entry::Occupied(mut entry) => program.update(entry.get_mut(), row),
                Entry::Vacant(entry) => {
                    Self::charge::<RETRY_SPILL>(&mut self.charge, bytes)?;
                    let mut state = program.new_group(row, vec![value]);
                    program.update(&mut state, row)?;
                    entry.insert(state);
                    Ok(())
                }
            }
        } else {
            let values = program
                .group_by
                .iter()
                .map(|expr| expr.eval(row))
                .collect::<Result<Vec<_>, _>>()?;
            let key: Vec<ValueKey> = values.iter().map(ValueKey::from).collect();
            let bytes =
                key.iter().map(|key| 24 + key.payload_size()).sum::<usize>() + per_group_bytes;
            match self.multi.entry(key) {
                Entry::Occupied(mut entry) => program.update(entry.get_mut(), row),
                Entry::Vacant(entry) => {
                    Self::charge::<RETRY_SPILL>(&mut self.charge, bytes)?;
                    let mut state = program.new_group(row, values);
                    program.update(&mut state, row)?;
                    entry.insert(state);
                    Ok(())
                }
            }
        }
    }

    #[inline]
    fn charge<const RETRY_SPILL: bool>(
        charge: &mut ScopedCharge,
        bytes: usize,
    ) -> Result<(), QueryError> {
        if RETRY_SPILL {
            charge_group_or_overflow(charge, bytes)
        } else {
            charge.add_or_fallback(bytes)
        }
    }

    pub(super) fn finish(
        self,
        program: &RowAggregateProgram,
        group_by: &[Expr],
        having: Option<&Expr>,
    ) -> Result<Vec<PlanRow>, QueryError> {
        let positions = simple_group_field_positions(group_by, &program.template);
        let mut out = Vec::with_capacity(self.single.len() + self.multi.len());
        // Both maps keep their native iterator; no boxed iterator or per-group
        // dispatch. The reservation lives through finalization and HAVING.
        for state in self.single.into_values().chain(self.multi.into_values()) {
            if let Some(row) =
                state.finish(&program.template, &positions, &program.slots, having)?
            {
                out.push(row);
            }
        }
        Ok(out)
    }
}
