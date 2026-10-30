//! Budgeted columnar window input. Fixed-width batches stay compact while
//! one partition at a time uses the canonical SQL window evaluator. A failed
//! optional reservation replays the consumed prefix into the spill pipeline.

use super::*;
use crate::engine::executor::dispatch::{
    cancellable_window_sort, evaluate_window_prefix_values, evaluate_window_with_partitions,
};
use crate::engine::expr::ValueKey;
use std::collections::VecDeque;

enum PartitionInput {
    Column(usize),
    Expression(Arc<crate::engine::expr_program::ExprProgram>),
}

fn try_charge_work(charge: &mut mem_budget::ScopedCharge, bytes: usize, limit: usize) -> bool {
    if crate::engine::executor::cte::cte_materialization_active()
        && mem_budget::current_budget().is_some_and(|budget| budget.tracks_process())
    {
        // Producer state and one bounded window work area overlap. The caller
        // separately bounds their input/index/output total by the unchanged
        // work-area limit; admission still checks the hard process target.
        charge.add_within(bytes, limit)
    } else {
        charge.add(bytes).is_ok()
    }
}

fn trace_layout(reason: &'static str, slots: usize, input_bytes: usize, work_bytes: usize) {
    if crate::engine::expr::current_optimizer_trace_context().is_some_and(|(_, enabled)| enabled) {
        let budget = mem_budget::current_budget();
        crate::engine::cbo::trace::event(
            "window_layout",
            serde_json::json!({
                "reason": reason, "rows": slots, "input_bytes": input_bytes, "work_bytes": work_bytes,
                "query_used": budget.as_ref().map(|budget| budget.used()),
                "query_limit": budget.as_ref().map(|budget| budget.limit()),
            }),
        );
    }
}

pub(super) enum Preparation<'a> {
    Ready(Vec<Batch>, mem_budget::ScopedCharge),
    Streaming(Box<dyn BatchOperator + 'a>),
}

pub(super) fn fixed_width(column_type: ColumnType) -> Option<usize> {
    match column_type {
        ColumnType::Tiny
        | ColumnType::Short
        | ColumnType::MediumInt
        | ColumnType::Long
        | ColumnType::LongLong
        | ColumnType::Float
        | ColumnType::Double
        | ColumnType::Year
        | ColumnType::Bit { .. }
        | ColumnType::Time
        | ColumnType::DateTime
        | ColumnType::Timestamp => Some(8),
        ColumnType::Date => Some(4),
        ColumnType::Decimal { .. } => Some(16),
        _ => None,
    }
}

fn fixed_column(column: &ColumnVector) -> bool {
    matches!(
        column,
        ColumnVector::Null { .. }
            | ColumnVector::Bool { .. }
            | ColumnVector::I64 { .. }
            | ColumnVector::U64 { .. }
            | ColumnVector::F64 { .. }
            | ColumnVector::Date32 { .. }
            | ColumnVector::Time64 { .. }
            | ColumnVector::DateTime64 { .. }
            | ColumnVector::Timestamp64 { .. }
            | ColumnVector::Decimal32 { .. }
            | ColumnVector::Decimal64 { .. }
            | ColumnVector::Decimal128 { .. }
    )
}

fn fixed_result_scatter_safe(
    window: &crate::planner::PhysicalWindowSpec,
    template: &[PlanField],
    batches: &[(Batch, usize)],
) -> bool {
    let name = window.name.to_ascii_uppercase();
    if matches!(
        name.as_str(),
        "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "PERCENT_RANK" | "CUME_DIST"
    ) || (name == "COUNT" && matches!(window.args.as_slice(), [Expr::Wildcard]))
    {
        return true;
    }
    if !matches!(name.as_str(), "SUM" | "MIN" | "MAX" | "COUNT") {
        return false;
    }
    let [argument] = window.args.as_slice() else {
        return false;
    };
    let Some(column) = expr_column_index_in_template(argument, template) else {
        return false;
    };
    let mut family = None;
    for (batch, _) in batches {
        let next = match &batch.columns()[column] {
            ColumnVector::Null { .. } => continue,
            ColumnVector::Bool { .. } | ColumnVector::I64 { .. } => (0, 0),
            ColumnVector::U64 { .. } => (1, 0),
            ColumnVector::F64 { .. } => (2, 0),
            ColumnVector::Decimal32 { scale, .. }
            | ColumnVector::Decimal64 { scale, .. }
            | ColumnVector::Decimal128 { scale, .. } => (3, *scale),
            ColumnVector::Date32 { .. } => (4, 0),
            ColumnVector::Time64 { .. } => (5, 0),
            ColumnVector::DateTime64 { .. } => (6, 0),
            ColumnVector::Timestamp64 { .. } => (7, 0),
            _ => return false,
        };
        if family.is_some_and(|family| family != next) {
            return false;
        }
        family = Some(next);
    }
    true
}

/// Scatter canonical partition results directly into fixed-width output
/// lanes. This avoids retaining a full Value array beside its typed mirror.
fn scatter_fixed_result(
    column: &mut ColumnVector,
    row: usize,
    value: Value,
    width: usize,
) -> Result<(), QueryError> {
    use crate::util::decimal::ExactDecimal;
    if matches!(value, Value::Null) {
        return Ok(());
    }
    let value = if matches!(
        value,
        Value::Decimal(_)
            | Value::DecimalCompact(..)
            | Value::Decimal128(..)
            | Value::DecimalWide(_)
    ) {
        match value.exact_decimal() {
            Some(ExactDecimal::I64(magnitude, scale)) => {
                Value::Decimal128(i128::from(magnitude), scale)
            }
            Some(ExactDecimal::I128(magnitude, scale)) => Value::Decimal128(magnitude, scale),
            _ => {
                return Err(crate::error::catalog::ER_INTERNAL_ERROR
                    .error("window result exceeded its fixed-width proof"))
            }
        }
    } else {
        value
    };
    if let ColumnVector::Null { len } = column {
        let len = *len;
        let nulls = Some(crate::engine::executor::vec::bitmap::Bitmap::new_all_invalid(len));
        *column = match &value {
            Value::Int(_) => ColumnVector::I64 {
                data: vec![0; len],
                nulls,
            },
            Value::UInt(_) => ColumnVector::U64 {
                data: vec![0; len],
                nulls,
            },
            Value::Float(_) | Value::Double(_) => ColumnVector::F64 {
                data: vec![0.0; len],
                nulls,
            },
            Value::Date(_) => ColumnVector::Date32 {
                data: vec![0; len],
                nulls,
            },
            Value::Time(_) => ColumnVector::Time64 {
                data: vec![0; len],
                nulls,
            },
            Value::DateTime(_) => ColumnVector::DateTime64 {
                data: vec![0; len],
                nulls,
            },
            Value::Timestamp(_) => ColumnVector::Timestamp64 {
                data: vec![0; len],
                nulls,
            },
            Value::Decimal128(_, scale) if width == 8 => ColumnVector::Decimal64 {
                data: vec![0; len],
                scale: *scale,
                nulls,
            },
            Value::Decimal128(_, scale) => ColumnVector::Decimal128 {
                data: vec![0; len],
                scale: *scale,
                nulls,
            },
            _ => {
                return Err(crate::error::catalog::ER_INTERNAL_ERROR
                    .error("window result has no proven scatter lane"))
            }
        };
    }
    macro_rules! put {
        ($data:expr, $nulls:expr, $value:expr) => {{
            $data[row] = $value;
            $nulls
                .as_mut()
                .expect("scatter validity allocated")
                .set(row, true);
        }};
    }
    match (column, value) {
        (ColumnVector::I64 { data, nulls }, Value::Int(value)) => put!(data, nulls, value),
        (ColumnVector::U64 { data, nulls }, Value::UInt(value)) => put!(data, nulls, value),
        (ColumnVector::F64 { data, nulls }, Value::Double(value)) => put!(data, nulls, value),
        (ColumnVector::F64 { data, nulls }, Value::Float(value)) => {
            put!(data, nulls, f64::from(value))
        }
        (ColumnVector::Date32 { data, nulls }, Value::Date(value)) => put!(data, nulls, value),
        (ColumnVector::Time64 { data, nulls }, Value::Time(value))
        | (ColumnVector::DateTime64 { data, nulls }, Value::DateTime(value))
        | (ColumnVector::Timestamp64 { data, nulls }, Value::Timestamp(value)) => {
            put!(data, nulls, value)
        }
        (
            ColumnVector::Decimal64 { data, nulls, scale },
            Value::Decimal128(value, source_scale),
        ) if *scale == source_scale => {
            let value = i64::try_from(value).map_err(|_| {
                crate::error::catalog::ER_INTERNAL_ERROR
                    .error("window result exceeded its narrow decimal proof")
            })?;
            put!(data, nulls, value);
        }
        (
            ColumnVector::Decimal128 { data, nulls, scale },
            Value::Decimal128(value, source_scale),
        ) if *scale == source_scale => put!(data, nulls, value),
        _ => {
            return Err(crate::error::catalog::ER_INTERNAL_ERROR
                .error("window result changed its proven scatter domain"))
        }
    }
    Ok(())
}

fn supported_spec(window: &crate::planner::PhysicalWindowSpec, template: &[PlanField]) -> bool {
    if window
        .order_by
        .iter()
        .any(|item| expr_column_index_in_template(&item.expr, template).is_none())
    {
        return false;
    }
    match window.name.to_ascii_uppercase().as_str() {
        "FIRST_VALUE" | "LAST_VALUE" => {
            matches!(window.args.as_slice(), [arg] if expr_column_index_in_template(arg,template).is_some())
        }
        "LAG" | "LEAD" => {
            (1..=3).contains(&window.args.len())
                && expr_column_index_in_template(&window.args[0], template).is_some()
                && window.args.get(1).is_none_or(
                    |arg| matches!(arg,Expr::Literal(crate::parser::Literal::Int(n)) if *n>=0),
                )
                && window
                    .args
                    .get(2)
                    .is_none_or(|arg| matches!(arg, Expr::Literal(crate::parser::Literal::Null)))
        }
        "NTH_VALUE" => {
            matches!(window.args.as_slice(),[arg,Expr::Literal(crate::parser::Literal::Int(n))] if *n>0 && expr_column_index_in_template(arg,template).is_some())
        }
        "NTILE" => {
            matches!(window.args.as_slice(),[Expr::Literal(crate::parser::Literal::Int(n))] if *n>0)
        }
        "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "PERCENT_RANK" | "CUME_DIST" => {
            window.args.is_empty()
        }
        "COUNT" if matches!(window.args.as_slice(), [Expr::Wildcard]) => true,
        "SUM" | "AVG" | "MIN" | "MAX" | "COUNT" | "STD" | "STDDEV" | "STDDEV_POP"
        | "STDDEV_SAMP" | "VARIANCE" | "VAR_POP" | "VAR_SAMP" => {
            matches!(window.args.as_slice(), [argument] if expr_column_index_in_template(argument, template).is_some())
        }
        _ => false,
    }
}

fn fixed_window_references(
    window: &crate::planner::PhysicalWindowSpec,
    template: &[PlanField],
) -> bool {
    let safe = |expr: &Expr| {
        !crate::parser::visit::any_expr(expr, |node| {
            if matches!(node, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                return expr_column_index_in_template(node, template)
                    .is_none_or(|i| fixed_width(template[i].column_type).is_none());
            }
            false
        })
    };
    if !window
        .partition_by
        .iter()
        .chain(window.order_by.iter().map(|order| &order.expr))
        .chain(window.args.iter())
        .all(safe)
    {
        return false;
    }
    if let Some(frame) = &window.frame {
        for bound in [&frame.start, &frame.end] {
            if let crate::parser::WindowFrameBound::Preceding(expr)
            | crate::parser::WindowFrameBound::Following(expr) = bound
            {
                if !safe(expr) {
                    return false;
                }
            }
        }
    }
    true
}

// Wide SQL DECIMAL metadata does not imply wide physical payloads. SUM
// remains in i128 when the aligned maximum input magnitude times partition
// length fits; MIN/MAX return an existing fixed-width input value unchanged.
fn fixed_output_width(
    window: &crate::planner::PhysicalWindowSpec,
    template: &[PlanField],
    batches: &[(Batch, usize)],
    partition_rows: usize,
) -> Option<usize> {
    let ColumnType::Decimal { precision, .. } = window.output_type else {
        return fixed_width(window.output_type);
    };
    let [argument] = window.args.as_slice() else {
        return None;
    };
    let column = expr_column_index_in_template(argument, template)?;
    if window.name.eq_ignore_ascii_case("MIN") || window.name.eq_ignore_ascii_case("MAX") {
        return Some(
            if batches.iter().any(|(batch, _)| {
                matches!(batch.columns()[column], ColumnVector::Decimal128 { .. })
            }) {
                16
            } else {
                8
            },
        );
    }
    if !window.name.eq_ignore_ascii_case("SUM") {
        return (precision <= 38).then_some(16);
    }
    let mut lanes = Vec::with_capacity(batches.len());
    for (batch, _) in batches {
        let lane = match &batch.columns()[column] {
            ColumnVector::Null { .. } => (0, 0),
            ColumnVector::I64 { data, .. } => (
                0,
                data.iter()
                    .map(|v| v.unsigned_abs() as u128)
                    .max()
                    .unwrap_or(0),
            ),
            ColumnVector::U64 { data, .. } => (0, data.iter().copied().max().unwrap_or(0) as u128),
            ColumnVector::Decimal32 { data, scale, .. } => (
                *scale,
                data.iter()
                    .map(|v| v.unsigned_abs() as u128)
                    .max()
                    .unwrap_or(0),
            ),
            ColumnVector::Decimal64 { data, scale, .. } => (
                *scale,
                data.iter()
                    .map(|v| v.unsigned_abs() as u128)
                    .max()
                    .unwrap_or(0),
            ),
            ColumnVector::Decimal128 { data, scale, .. } => (
                *scale,
                data.iter().map(|v| v.unsigned_abs()).max().unwrap_or(0),
            ),
            _ => return None,
        };
        lanes.push(lane);
    }
    let scale = lanes.iter().map(|(scale, _)| *scale).max().unwrap_or(0);
    let mut maximum = 0u128;
    for (source_scale, magnitude) in lanes {
        let bound = 10u128
            .checked_pow((scale - source_scale) as u32)?
            .checked_mul(magnitude)?
            .checked_mul(partition_rows as u128)?;
        maximum = maximum.max(bound);
    }
    if maximum <= i64::MAX as u128 {
        Some(8)
    } else if maximum <= i128::MAX as u128 {
        Some(16)
    } else {
        None
    }
}

struct Prefix<'a> {
    input: Box<dyn BatchOperator + 'a>,
    batches: VecDeque<(Batch, usize)>,
    charge: mem_budget::ScopedCharge,
}

impl BatchOperator for Prefix<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        self.input.schema()
    }
    fn properties(&self) -> BatchProperties {
        self.input.properties()
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if let Some((batch, bytes)) = self.batches.pop_front() {
            self.charge.release_bytes(bytes);
            return Ok(Some(batch));
        }
        self.input.next_batch()
    }
}

fn replay<'a>(
    input: Box<dyn BatchOperator + 'a>,
    batches: Vec<(Batch, usize)>,
    charge: mem_budget::ScopedCharge,
) -> Preparation<'a> {
    Preparation::Streaming(Box::new(Prefix {
        input,
        batches: batches.into(),
        charge,
    }))
}

/// Compare homogeneous integer/temporal lanes without constructing Values.
/// Other numeric representations retain the canonical SQL comparator.
fn compare_fixed_rows(
    left: &ColumnVector,
    left_row: usize,
    right: &ColumnVector,
    right_row: usize,
    field: crate::engine::executor::sort::SortField,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left.is_null(left_row), right.is_null(right_row)) {
        (true, true) => return Ordering::Equal,
        (true, false) => {
            return if field.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (false, true) => {
            return if field.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        _ => {}
    }
    let order = match (left, right) {
        (ColumnVector::I64 { data: a, .. }, ColumnVector::I64 { data: b, .. })
        | (ColumnVector::Time64 { data: a, .. }, ColumnVector::Time64 { data: b, .. })
        | (ColumnVector::Timestamp64 { data: a, .. }, ColumnVector::Timestamp64 { data: b, .. }) => {
            a[left_row].cmp(&b[right_row])
        }
        (ColumnVector::U64 { data: a, .. }, ColumnVector::U64 { data: b, .. }) => {
            a[left_row].cmp(&b[right_row])
        }
        (ColumnVector::Date32 { data: a, .. }, ColumnVector::Date32 { data: b, .. }) => {
            let (a, b) = (a[left_row], b[right_row]);
            if crate::types::decode_tagged_date(a).is_some()
                || crate::types::decode_tagged_date(b).is_some()
            {
                crate::types::date_components(a).cmp(&crate::types::date_components(b))
            } else {
                a.cmp(&b)
            }
        }
        (ColumnVector::DateTime64 { data: a, .. }, ColumnVector::DateTime64 { data: b, .. }) => {
            let (a, b) = (a[left_row], b[right_row]);
            if crate::types::decode_tagged_datetime(a).is_some()
                || crate::types::decode_tagged_datetime(b).is_some()
            {
                crate::types::datetime_components(a).cmp(&crate::types::datetime_components(b))
            } else {
                a.cmp(&b)
            }
        }
        _ => {
            return crate::engine::executor::sort::compare_value_for_sort(
                &left.value_at(left_row),
                &right.value_at(right_row),
                field,
            )
        }
    };
    if field.ascending {
        order
    } else {
        order.reverse()
    }
}

pub(super) fn prepare<'a>(
    input: Box<dyn BatchOperator + 'a>,
    template: &[PlanField],
    windows: &[crate::planner::PhysicalWindowSpec],
    output_schema: Arc<BatchSchema>,
    plan: &PhysicalPlan,
) -> Result<Preparation<'a>, QueryError> {
    let limit = mem_budget::current_budget().map_or(8 * 1024 * 1024, |budget| budget.limit());
    prepare_with_limit(input, template, windows, output_schema, plan, limit)
}

pub(super) fn prepare_with_limit<'a>(
    mut input: Box<dyn BatchOperator + 'a>,
    template: &[PlanField],
    windows: &[crate::planner::PhysicalWindowSpec],
    output_schema: Arc<BatchSchema>,
    plan: &PhysicalPlan,
    limit: usize,
) -> Result<Preparation<'a>, QueryError> {
    let Some(leader) = windows.first() else {
        return Ok(Preparation::Streaming(input));
    };
    // Newly admitted variable payloads must retain their existing bounded
    // candidate prefilter instead of materializing all ranking input.
    if windows.len() == 1
        && leader.top_k.is_some()
        && crate::engine::executor::dispatch::RankKind::from_name(&leader.name).is_some()
        && template
            .iter()
            .any(|field| fixed_width(field.column_type).is_none())
    {
        return Ok(Preparation::Streaming(input));
    }
    let positions = leader
        .partition_by
        .iter()
        .map(|expr| {
            if let Some(column) = expr_column_index_in_template(expr, template) {
                return Some(PartitionInput::Column(column));
            }
            let program = expr_batch::compile_for_batch(expr, input.schema()).ok()??;
            // Computed integer keys retain ValueKey's exact equality domain.
            // Potential diagnostics and collated/coerced keys retain spill evaluation.
            (matches!(
                program.result_type().column_type(),
                ColumnType::Tiny
                    | ColumnType::Short
                    | ColumnType::MediumInt
                    | ColumnType::Long
                    | ColumnType::LongLong
            ) && program.column_parallel_safe(input.schema()))
            .then_some(PartitionInput::Expression(program))
        })
        .collect::<Option<Vec<_>>>();
    let widths = windows
        .iter()
        .map(|window| fixed_width(window.output_type))
        .collect::<Option<Vec<_>>>();
    let (Some(positions), Some(_)) = (positions, widths) else {
        trace_layout("binding_or_output_type", 0, 0, 0);
        return Ok(Preparation::Streaming(input));
    };
    if positions.is_empty()
        || windows.iter().any(|window| {
            !supported_spec(window, template) || !fixed_window_references(window, template)
        })
    {
        trace_layout("input_type_or_expression", 0, 0, 0);
        return Ok(Preparation::Streaming(input));
    }
    let mut batches = Vec::new();
    let mut input_charge = mem_budget::ScopedCharge::new(0);
    let mut partition_charge = mem_budget::ScopedCharge::new(0);
    let mut partition_reserved = 0usize;
    let mut input_bytes = 0usize;
    let mut slots = 0usize;
    let mut lookup: hashbrown::HashMap<
        smallvec::SmallVec<[ValueKey; 2]>,
        usize,
        ahash::RandomState,
    > = hashbrown::HashMap::with_hasher(ahash::RandomState::new());
    let mut partitions: Vec<Vec<(u32, u32)>> = Vec::new();
    let mut declined = false;
    while let Some(batch) = input.next_batch()? {
        crate::engine::cancel::check_cancelled()?;
        if batch.visible_rows() == 0 {
            continue;
        }
        let bytes = batch
            .memory_bytes()
            .saturating_add(std::mem::size_of::<(Batch, usize)>() * 2)
            .saturating_add(if batch.selection().is_some() {
                batch.num_rows().saturating_mul(4) + 128
            } else {
                0
            });
        if batches.len() >= u32::MAX as usize
            || !batch.columns().iter().zip(template).all(|(column, field)| {
                fixed_width(field.column_type).is_none() || fixed_column(column)
            })
            || input_bytes
                .saturating_add(partition_reserved)
                .saturating_add(bytes)
                > limit
            || !try_charge_work(&mut input_charge, bytes, limit)
        {
            batches.push((batch, 0));
            declined = true;
            break;
        }
        input_bytes += bytes;
        let computed_count = positions
            .iter()
            .filter(|key| matches!(key, PartitionInput::Expression(_)))
            .count();
        let scratch_bytes = batch
            .num_rows()
            .saturating_mul(64)
            .saturating_mul(computed_count);
        let mut scratch_charge = mem_budget::ScopedCharge::new(0);
        if input_bytes
            .saturating_add(partition_reserved)
            .saturating_add(scratch_bytes)
            > limit
            || !try_charge_work(&mut scratch_charge, scratch_bytes, limit)
        {
            batches.push((batch, bytes));
            declined = true;
            break;
        }
        let keys = positions
            .iter()
            .map(|key| match key {
                PartitionInput::Column(column) => {
                    Some(std::borrow::Cow::Borrowed(&batch.columns()[*column]))
                }
                PartitionInput::Expression(program) => {
                    program.try_eval_column(&batch).map(std::borrow::Cow::Owned)
                }
            })
            .collect::<Option<Vec<_>>>();
        let Some(keys) = keys else {
            batches.push((batch, bytes));
            declined = true;
            break;
        };
        slots = slots.saturating_add(batch.num_rows());
        let batch_index = batches.len() as u32;
        let index_bytes = batch.visible_rows().saturating_mul(16);
        if input_bytes
            .saturating_add(partition_reserved)
            .saturating_add(scratch_bytes)
            .saturating_add(index_bytes)
            > limit
            || !try_charge_work(&mut partition_charge, index_bytes, limit)
        {
            drop(keys);
            batches.push((batch, bytes));
            declined = true;
            break;
        }
        partition_reserved += index_bytes;
        for row in batch.visible_indices() {
            let key = keys
                .iter()
                .map(|column| ValueKey::from(&column.value_at(row)))
                .collect::<smallvec::SmallVec<[_; 2]>>();
            let group = if let Some(&group) = lookup.get(&key) {
                group
            } else {
                // Hash buckets, owned key lanes and the outer partition Vec.
                let bytes = 192usize.saturating_add(
                    positions
                        .len()
                        .saturating_mul(std::mem::size_of::<ValueKey>() + 32),
                );
                if input_bytes
                    .saturating_add(partition_reserved)
                    .saturating_add(scratch_bytes)
                    .saturating_add(bytes)
                    > limit
                    || !try_charge_work(&mut partition_charge, bytes, limit)
                {
                    declined = true;
                    break;
                }
                partition_reserved += bytes;
                let group = partitions.len();
                partitions.push(Vec::new());
                lookup.insert(key, group);
                group
            };
            partitions[group].push((batch_index, row as u32));
        }
        drop(keys);
        batches.push((batch, bytes));
        if declined {
            break;
        }
    }
    drop(lookup);
    if declined {
        trace_layout(
            "input_budget_or_lane",
            slots,
            input_bytes,
            partition_reserved,
        );
        drop(partitions);
        drop(partition_charge);
        return Ok(replay(input, batches, input_charge));
    }
    let index_bytes = partitions
        .capacity()
        .saturating_mul(std::mem::size_of::<Vec<(u32, u32)>>())
        .saturating_add(
            partitions
                .iter()
                .map(|p| p.capacity().saturating_mul(8))
                .sum::<usize>(),
        );
    partition_charge.release_bytes(partition_reserved.saturating_sub(index_bytes));
    let peak_partition = partitions.iter().map(Vec::len).max().unwrap_or(0);
    let Some(widths) = windows
        .iter()
        .map(|window| fixed_output_width(window, template, &batches, peak_partition))
        .collect::<Option<Vec<_>>>()
    else {
        trace_layout("unproven_result_width", slots, input_bytes, index_bytes);
        drop(partitions);
        drop(partition_charge);
        return Ok(replay(input, batches, input_charge));
    };
    let max_order = windows
        .iter()
        .map(|window| window.order_by.len())
        .max()
        .unwrap_or(0);
    let scatter = windows
        .iter()
        .map(|window| fixed_result_scatter_safe(window, template, &batches))
        .collect::<Vec<_>>();
    let scalar_bytes = if scatter.iter().all(|safe| *safe) {
        0
    } else {
        slots.saturating_mul(std::mem::size_of::<Value>())
    };
    let sort_workers =
        crate::engine::executor::dispatch::window_parallel_degree(slots, partitions.len()).max(1);
    let evaluation_scratch = peak_partition.saturating_mul(
        (template.len() + max_order + 2).saturating_mul(std::mem::size_of::<Value>()) + 128,
    );
    // Sorting and evaluation are separate phases. Parallel sorting needs
    // only row references, not a full scalar partition per worker.
    let sorting_scratch = peak_partition
        .saturating_mul(sort_workers)
        .saturating_mul(std::mem::size_of::<(u32, u32)>());
    let scratch_bytes = evaluation_scratch.max(sorting_scratch);
    let output_headers = batches.len().saturating_mul(
        windows
            .len()
            .saturating_mul(std::mem::size_of::<ColumnVector>())
            + std::mem::size_of::<Vec<ColumnVector>>()
            + std::mem::size_of::<Vec<Value>>()
            + std::mem::size_of::<Batch>(),
    );
    let typed_bytes = slots
        .saturating_mul(widths.iter().map(|width| width + 1).sum::<usize>())
        .saturating_add(output_headers);
    let mut output_charge = mem_budget::ScopedCharge::new(0);
    let work_bytes = scalar_bytes
        .saturating_add(scratch_bytes)
        .saturating_add(typed_bytes);
    if input_bytes
        .saturating_add(index_bytes)
        .saturating_add(work_bytes)
        > limit
        || !try_charge_work(&mut output_charge, work_bytes, limit)
    {
        trace_layout(
            "output_budget",
            slots,
            input_bytes,
            index_bytes.saturating_add(work_bytes),
        );
        drop(partitions);
        drop(partition_charge);
        return Ok(replay(input, batches, input_charge));
    }
    // Every optional reservation succeeds before evaluating any window. No
    // partial evaluation (or duplicate warning) can precede spill fallback.
    drop(input);
    let columns = template
        .iter()
        .map(|field| crate::types::Column {
            name: field.column.clone(),
            column_type: field.column_type,
            source: field.source.clone(),
        })
        .collect::<Vec<_>>();
    let mut output_columns = batches
        .iter()
        .map(|_| (0..windows.len()).map(|_| None).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    // Co-partitioned windows may have different orderings. Sort each ordering
    // once, directly over batch/row references, and scatter by those references
    // so selection vectors and the input-order contract remain intact.
    let mut groups: Vec<Vec<usize>> = Vec::new();
    for (index, window) in windows.iter().enumerate() {
        if let Some(group) = groups
            .iter_mut()
            .find(|group| windows[group[0]].order_by == window.order_by)
        {
            group.push(index);
        } else {
            groups.push(vec![index]);
        }
    }
    for group in groups {
        let order_by = &windows[group[0]].order_by;
        let order_positions = order_by
            .iter()
            .map(|order| expr_column_index_in_template(&order.expr, template).expect("bound order"))
            .collect::<Vec<_>>();
        let sort_fields = order_by
            .iter()
            .zip(&order_positions)
            .map(|(order, &position)| {
                let mut field = crate::engine::executor::sort::SortField::from_order_by(order);
                field.collation_id = template[position].source.collation_id;
                field
            })
            .collect::<Vec<_>>();
        let sort = |partitions: &mut [Vec<(u32, u32)>]| -> Result<(), QueryError> {
            for partition in partitions {
                cancellable_window_sort(partition, |left, right| {
                    for (&position, &field) in order_positions.iter().zip(&sort_fields) {
                        let order = compare_fixed_rows(
                            &batches[left.0 as usize].0.columns()[position],
                            left.1 as usize,
                            &batches[right.0 as usize].0.columns()[position],
                            right.1 as usize,
                            field,
                        );
                        if !order.is_eq() {
                            return order;
                        }
                    }
                    left.cmp(&right)
                })?;
            }
            Ok(())
        };
        if sort_workers == 1 {
            sort(&mut partitions)?;
        } else {
            let cancellation = crate::engine::cancel::CancellationSnapshot::capture();
            let session = crate::engine::expr::current_session_expr_info();
            let budget = mem_budget::current_budget();
            let chunk = partitions.len().div_ceil(sort_workers).max(1);
            std::thread::scope(|scope| {
                let handles = partitions
                    .chunks_mut(chunk)
                    .map(|partitions| {
                        let sort = &sort;
                        let session = &session;
                        let cancellation = cancellation.clone();
                        let budget = budget.clone();
                        scope.spawn(move || {
                            let _cancel = cancellation.enter();
                            crate::engine::expr::with_session_expr_info_option(session, || {
                                match budget {
                                    Some(budget) => {
                                        mem_budget::with_query_budget(budget, || sort(partitions))
                                    }
                                    None => sort(partitions),
                                }
                            })
                        })
                    })
                    .collect::<Vec<_>>();
                let mut failure = None;
                for handle in handles {
                    let result = handle.join().unwrap_or_else(|_| {
                        Err(crate::storage::error(
                            crate::error::catalog::ER_INTERNAL_ERROR,
                            "window partition sort worker panicked",
                        ))
                    });
                    if let Err(error) = result {
                        if failure.is_none() {
                            failure = Some(error);
                        }
                    }
                }
                failure.map_or(Ok(()), Err)
            })?;
        }
        for window_index in group {
            let window = &windows[window_index];
            let mut results = if scatter[window_index] {
                Vec::new()
            } else {
                batches
                    .iter()
                    .map(|(batch, _)| vec![Value::Null; batch.num_rows()])
                    .collect::<Vec<_>>()
            };
            let mut typed = if scatter[window_index] {
                batches
                    .iter()
                    .map(|(batch, _)| ColumnVector::Null {
                        len: batch.num_rows(),
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
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
            let prefix = window.frame.as_ref().is_some_and(|frame| {
                matches!(frame.units, crate::parser::WindowFrameUnits::Rows)
                    && matches!(
                        frame.start,
                        crate::parser::WindowFrameBound::UnboundedPreceding
                    )
                    && matches!(frame.end, crate::parser::WindowFrameBound::CurrentRow)
                    && matches!(
                        frame.exclusion,
                        crate::parser::WindowFrameExclusion::NoOthers
                    )
            }) && matches!(
                window.name.to_ascii_uppercase().as_str(),
                "SUM" | "AVG" | "MIN" | "MAX" | "COUNT"
            );
            let name = window.name.to_ascii_uppercase();
            let count_star = name == "COUNT" && matches!(window.args.as_slice(), [Expr::Wildcard]);
            let argument = window
                .args
                .first()
                .and_then(|arg| expr_column_index_in_template(arg, template));
            for partition in &partitions {
                crate::engine::cancel::check_cancelled()?;
                let values = if prefix {
                    evaluate_window_prefix_values(
                        &name,
                        count_star,
                        partition.iter().map(|&(batch, row)| {
                            argument.map_or(Value::Int(1), |position| {
                                batches[batch as usize].0.columns()[position].value_at(row as usize)
                            })
                        }),
                    )?
                } else {
                    let rows = partition
                        .iter()
                        .map(|&(batch, row)| {
                            batches[batch as usize]
                                .0
                                .columns()
                                .iter()
                                .zip(template)
                                .map(|(column, field)| {
                                    if fixed_width(field.column_type).is_some() {
                                        column.value_at(row as usize)
                                    } else {
                                        Value::Null
                                    }
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();
                    let ordered = vec![(0..rows.len()).collect::<Vec<_>>()];
                    evaluate_window_with_partitions(&expression, &ordered, &columns, &rows)?
                };
                for ((batch, row), value) in partition.iter().copied().zip(values) {
                    if scatter[window_index] {
                        scatter_fixed_result(
                            &mut typed[batch as usize],
                            row as usize,
                            value,
                            widths[window_index],
                        )?;
                    } else {
                        results[batch as usize][row as usize] = value;
                    }
                }
            }
            let columns = if scatter[window_index] {
                typed
            } else {
                results.into_iter().map(ColumnVector::from_values).collect()
            };
            for (output, column) in output_columns.iter_mut().zip(columns) {
                output[window_index] = Some(column);
            }
        }
    }
    let output_columns = output_columns
        .into_iter()
        .map(|columns| {
            columns
                .into_iter()
                .map(|column| column.expect("evaluated window"))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let actual_output = output_columns
        .iter()
        .flatten()
        .map(ColumnVector::memory_bytes)
        .sum::<usize>()
        .saturating_add(output_headers);
    if actual_output > typed_bytes {
        output_charge.add_force(actual_output - typed_bytes)?;
    } else {
        output_charge.release_bytes(typed_bytes - actual_output);
    }
    output_charge.release_bytes(scalar_bytes.saturating_add(scratch_bytes));
    input_charge.absorb(&mut output_charge);
    let result = batches
        .into_iter()
        .zip(output_columns)
        .map(|((batch, _), extra)| -> Result<Batch, QueryError> {
            let selection = batch.selection().cloned();
            let mut columns = batch.into_columns()?;
            columns.extend(extra);
            let batch = Batch::from_owned_columns(output_schema.clone(), columns);
            Ok(match selection {
                Some(selection) => batch.with_selection(selection),
                None => batch,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    analyze_stats::add_window_runtime(plan, "columnar_partitions", 0, peak_partition as u64);
    trace_layout("columnar_partitions", slots, input_bytes, actual_output);
    super::COMPACT_WINDOW_BATCH_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(Preparation::Ready(result, input_charge))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_pipeline_overlap_remains_bounded_and_preserves_explicit_quotas() {
        for tracked in [false, true] {
            let budget = Arc::new(if tracked {
                mem_budget::MemBudget::process_tracked_for_test(65536)
            } else {
                mem_budget::MemBudget::new(65536)
            });
            mem_budget::with_query_budget(budget.clone(), || {
                let ancestor = mem_budget::force_charge_scoped(65536).unwrap();
                crate::engine::executor::cte::with_cte_materialization_active(true, || {
                    let mut window = mem_budget::ScopedCharge::new(0);
                    assert_eq!(try_charge_work(&mut window, 8192, 65536), tracked);
                    assert!(!try_charge_work(&mut window, 65537, 65536));
                    assert!(budget.terminal_oom().is_none());
                });
                assert_eq!(budget.used(), 65536);
                drop(ancestor);
                assert_eq!(budget.used(), 0);
            });
        }
    }

    #[test]
    fn scatter_results_match_canonical_lanes_across_fixed_domains_and_nulls() {
        for (values, width) in [
            (
                vec![Value::Int(i64::MIN), Value::Null, Value::Int(i64::MAX)],
                8,
            ),
            (vec![Value::UInt(u64::MAX), Value::Null, Value::UInt(0)], 8),
            (
                vec![Value::Double(-0.0), Value::Null, Value::Double(1.25)],
                8,
            ),
            (vec![Value::Date(-100), Value::Null, Value::Date(100)], 4),
            (vec![Value::Time(-100), Value::Null, Value::Time(100)], 8),
            (
                vec![Value::DateTime(-100), Value::Null, Value::DateTime(100)],
                8,
            ),
            (
                vec![Value::Timestamp(-100), Value::Null, Value::Timestamp(100)],
                8,
            ),
            (
                vec![
                    Value::DecimalCompact(i64::MIN, 2),
                    Value::Null,
                    Value::DecimalCompact(99, 2),
                ],
                8,
            ),
            (
                vec![
                    Value::Decimal128(i128::MAX / 100, 3),
                    Value::Null,
                    Value::DecimalCompact(99, 3),
                ],
                16,
            ),
        ] {
            let expected = ColumnVector::from_values(values.clone());
            let mut actual = ColumnVector::Null { len: values.len() };
            for row in [2, 0, 1] {
                scatter_fixed_result(&mut actual, row, values[row].clone(), width).unwrap();
            }
            for row in 0..values.len() {
                assert_eq!(actual.value_at(row), expected.value_at(row));
            }
            assert!(actual.memory_bytes() <= values.len() * width + 8);
        }
        let mut column = ColumnVector::Null { len: 1 };
        let error =
            scatter_fixed_result(&mut column, 0, Value::Decimal128(i128::MAX, 0), 8).unwrap_err();
        assert!(error.message().contains("narrow decimal proof"));
    }

    #[test]
    fn fixed_window_order_matches_scalar_for_nulls_and_numeric_domains() {
        let columns = vec![
            ColumnVector::from_values(vec![
                Value::Null,
                Value::Int(i64::MIN),
                Value::Int(0),
                Value::Int(i64::MAX),
            ]),
            ColumnVector::from_values(vec![Value::UInt(u64::MAX), Value::Null, Value::UInt(0)]),
            ColumnVector::from_values(vec![Value::Date(-20), Value::Null, Value::Date(20)]),
            ColumnVector::from_values(vec![Value::Time(-100), Value::Null, Value::Time(100)]),
            ColumnVector::from_values(vec![
                Value::DateTime(-100),
                Value::Null,
                Value::DateTime(100),
            ]),
            ColumnVector::from_values(vec![
                Value::Timestamp(-100),
                Value::Null,
                Value::Timestamp(100),
            ]),
            ColumnVector::from_values(vec![Value::Double(-0.0), Value::Null, Value::Double(1.25)]),
            ColumnVector::from_values(vec![
                Value::DecimalCompact(-10, 2),
                Value::Null,
                Value::DecimalCompact(99, 2),
            ]),
            ColumnVector::from_values(vec![
                Value::Decimal128(i128::MAX / 100, 3),
                Value::Null,
                Value::Decimal128(99, 3),
            ]),
            ColumnVector::from_values(vec![
                Value::Date(0),
                Value::Date(crate::types::encode_tagged_date(2000, 0, 1).unwrap()),
                Value::Null,
            ]),
            ColumnVector::from_values(vec![
                Value::DateTime(0),
                Value::DateTime(
                    crate::types::encode_tagged_datetime(2000, 0, 1, 1, 2, 3, 4).unwrap(),
                ),
                Value::Null,
            ]),
        ];
        for ascending in [true, false] {
            for nulls_first in [true, false] {
                let field = crate::engine::executor::sort::SortField {
                    ascending,
                    nulls_first,
                    collation_id: None,
                };
                for column in &columns {
                    for left in 0..column.len() {
                        for right in 0..column.len() {
                            assert_eq!(
                                compare_fixed_rows(column, left, column, right, field),
                                crate::engine::executor::sort::compare_value_for_sort(
                                    &column.value_at(left),
                                    &column.value_at(right),
                                    field
                                )
                            );
                        }
                    }
                }
            }
        }
    }
}
