//! Optional exact fixed-width set keys. Consumed prefixes stay replayable
//! until every physical lane, key width and memory reservation is proven.
use super::*;
use crate::engine::executor::mem_budget::{self, ScopedCharge};
use std::collections::VecDeque;

#[derive(Clone, Copy)]
enum Lane {
    Integer,
    Date,
    Time,
    DateTime,
    Timestamp,
    Decimal(u8),
}

fn lanes(schema: &BatchSchema) -> Option<Vec<Lane>> {
    if schema.len() == 0 || schema.len() > 16 {
        return None;
    }
    schema
        .fields()
        .iter()
        .map(|field| {
            if field.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0 {
                return None;
            }
            Some(match field.column_type {
                ColumnType::Tiny
                | ColumnType::Short
                | ColumnType::MediumInt
                | ColumnType::Long
                | ColumnType::LongLong => Lane::Integer,
                ColumnType::Date => Lane::Date,
                ColumnType::Time => Lane::Time,
                ColumnType::DateTime => Lane::DateTime,
                ColumnType::Timestamp => Lane::Timestamp,
                ColumnType::Decimal { scale, .. } => Lane::Decimal(scale),
                _ => return None,
            })
        })
        .collect()
}

fn value(lane: Lane, column: &ColumnVector, row: usize) -> Option<Option<i128>> {
    if column.is_null(row) {
        return Some(None);
    }
    Some(Some(match (lane, column) {
        (Lane::Integer, ColumnVector::I64 { data, .. }) => i128::from(data[row]),
        (Lane::Integer, ColumnVector::Bool { values, .. }) => i128::from(values.get(row)),
        (Lane::Date, ColumnVector::Date32 { data, .. }) => i128::from(data[row]),
        (Lane::Time, ColumnVector::Time64 { data, .. })
        | (Lane::DateTime, ColumnVector::DateTime64 { data, .. })
        | (Lane::Timestamp, ColumnVector::Timestamp64 { data, .. }) => i128::from(data[row]),
        (Lane::Decimal(expected), ColumnVector::Decimal32 { data, scale, .. })
            if expected == *scale =>
        {
            i128::from(data[row])
        }
        (Lane::Decimal(expected), ColumnVector::Decimal64 { data, scale, .. })
            if expected == *scale =>
        {
            i128::from(data[row])
        }
        (Lane::Decimal(expected), ColumnVector::Decimal128 { data, scale, .. })
            if expected == *scale =>
        {
            data[row]
        }
        _ => return None,
    }))
}

struct Replay<'a> {
    schema: Arc<BatchSchema>,
    prefix: VecDeque<Batch>,
    rest: Option<Box<dyn BatchOperator + 'a>>,
}
impl BatchOperator for Replay<'_> {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        if let Some(batch) = self.prefix.pop_front() {
            return Ok(Some(batch));
        }
        match self.rest.as_mut() {
            Some(rest) => rest.next_batch(),
            None => Ok(None),
        }
    }
}

fn restore<'a>(
    inputs: &mut [&mut Option<Box<dyn BatchOperator + 'a>>; 2],
    frames: Vec<(bool, Batch)>,
    schemas: &[Arc<BatchSchema>; 2],
) {
    let mut prefixes = [VecDeque::new(), VecDeque::new()];
    for (right, batch) in frames {
        prefixes[usize::from(right)].push_back(batch);
    }
    for side in 0..2 {
        let rest = inputs[side].take();
        *inputs[side] = Some(Box::new(Replay {
            schema: schemas[side].clone(),
            prefix: std::mem::take(&mut prefixes[side]),
            rest,
        }));
    }
}

pub(super) fn prepare<'a>(
    left: &mut Option<Box<dyn BatchOperator + 'a>>,
    right: &mut Option<Box<dyn BatchOperator + 'a>>,
    schema: Arc<BatchSchema>,
    op: crate::parser::SetOp,
) -> Result<Option<Vec<Batch>>, QueryError> {
    let Some(types) = lanes(&schema) else {
        return Ok(None);
    };
    let schemas = [
        left.as_ref().unwrap().schema().clone(),
        right.as_ref().unwrap().schema().clone(),
    ];
    if schemas.iter().any(|source| {
        source.len() != schema.len()
            || source.fields().iter().zip(schema.fields()).any(|(a, b)| {
                !SetOpBatchOperator::can_passthrough_field(a, b)
                    || a.source.flags & crate::mysql::protocol::UNSIGNED_FLAG != 0
            })
    }) {
        return Ok(None);
    }
    let mut inputs = [left, right];
    let cap = mem_budget::current_budget().map_or(8 * 1024 * 1024, |budget| budget.limit() / 3);
    let mut retained = 0usize;
    let mut frames = Vec::new();
    let mut bounds = vec![(i128::MAX, i128::MIN, false); types.len()];
    for side in 0..2 {
        loop {
            let Some(mut batch) = inputs[side].as_mut().unwrap().next_batch()? else {
                *inputs[side] = None;
                break;
            };
            crate::engine::cancel::check_cancelled()?;
            let bytes = batch.memory_bytes().saturating_add(1024);
            let mut charge = batch.memory_charge();
            if retained.saturating_add(bytes) > cap
                || Arc::strong_count(&batch.columns) != 1
                || !charge.add_within(bytes, usize::MAX)
            {
                frames.push((side == 1, batch));
                restore(&mut inputs, frames, &schemas);
                return Ok(None);
            }
            retained = retained.saturating_add(bytes);
            let selection = batch.selection().cloned();
            let mut columns = batch.take_columns()?;
            columns.attach_charge(charge);
            batch = Batch::from_owned_columns(schema.clone(), columns);
            if let Some(selection) = selection {
                batch = batch.with_selection(selection);
            }
            let mut compatible = true;
            for row in batch.visible_indices() {
                for (column, &lane) in types.iter().enumerate() {
                    match value(lane, &batch.columns()[column], row) {
                        Some(Some(value)) => {
                            bounds[column].0 = bounds[column].0.min(value);
                            bounds[column].1 = bounds[column].1.max(value);
                            bounds[column].2 = true;
                        }
                        Some(None) => {}
                        None => {
                            compatible = false;
                            break;
                        }
                    }
                }
                if !compatible {
                    break;
                }
            }
            frames.push((side == 1, batch));
            if !compatible {
                restore(&mut inputs, frames, &schemas);
                return Ok(None);
            }
        }
    }
    match combine(&frames, &types, &bounds, schema, op)? {
        Some(output) => Ok(Some(output)),
        None => {
            restore(&mut inputs, frames, &schemas);
            Ok(None)
        }
    }
}

#[derive(Clone, Copy)]
struct Entry {
    frame: u32,
    row: u32,
    sides: u8,
}

fn combine(
    frames: &[(bool, Batch)],
    types: &[Lane],
    bounds: &[(i128, i128, bool)],
    schema: Arc<BatchSchema>,
    op: crate::parser::SetOp,
) -> Result<Option<Vec<Batch>>, QueryError> {
    let mut offsets = Vec::with_capacity(types.len());
    let mut bits = 0u32;
    for &(min, max, present) in bounds {
        let (min, max) = if present { (min, max) } else { (0, 0) };
        let Some(largest) = max
            .checked_sub(min)
            .and_then(|span| (span as u128).checked_add(1))
        else {
            return Ok(None);
        };
        let width = (128 - largest.leading_zeros()).max(1);
        offsets.push((min, bits));
        bits += width;
    }
    if bits > 128 || frames.len() > u32::MAX as usize {
        return Ok(None);
    }
    let rows = frames
        .iter()
        .map(|(_, batch)| batch.visible_rows())
        .sum::<usize>();
    if rows > u32::MAX as usize {
        return Ok(None);
    }
    let mut scratch = ScopedCharge::new(0);
    if !scratch.add_within(rows.saturating_mul(96).saturating_add(65536), usize::MAX) {
        return Ok(None);
    }
    let mut lookup =
        hashbrown::HashMap::<u128, u32, ahash::RandomState>::with_hasher(ahash::RandomState::new());
    lookup
        .try_reserve(rows)
        .map_err(|_| mem_budget::allocation_error("fixed set key index"))?;
    let mut entries =
        mem_budget::try_vec_with_capacity::<Entry>(rows, "fixed set representatives")?;
    for (frame, (right, batch)) in frames.iter().enumerate() {
        let side = if *right { 2 } else { 1 };
        for row in batch.visible_indices() {
            if row % 1024 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            let mut key = 0u128;
            for (column, &lane) in types.iter().enumerate() {
                let token = match value(lane, &batch.columns()[column], row)
                    .expect("validated fixed lane")
                {
                    None => 0,
                    Some(value) => (value - offsets[column].0) as u128 + 1,
                };
                key |= token << offsets[column].1;
            }
            match lookup.entry(key) {
                hashbrown::hash_map::Entry::Occupied(entry) => {
                    entries[*entry.get() as usize].sides |= side
                }
                hashbrown::hash_map::Entry::Vacant(entry) => {
                    entry.insert(entries.len() as u32);
                    entries.push(Entry {
                        frame: frame as u32,
                        row: row as u32,
                        sides: side,
                    });
                }
            }
        }
    }
    let kept = |entry: &Entry| match op {
        crate::parser::SetOp::Union => true,
        crate::parser::SetOp::Intersect => entry.sides == 3,
        crate::parser::SetOp::Except => entry.sides == 1,
    };
    let output_rows = entries.iter().filter(|entry| kept(entry)).count();
    let mut output_charge = ScopedCharge::new(0);
    let per_row = types.len().saturating_mul(32).saturating_add(16);
    let upper_batches = output_rows
        .div_ceil(DEFAULT_BATCH_ROWS)
        .saturating_add(frames.len());
    if !output_charge.add_within(
        output_rows
            .saturating_mul(per_row)
            .saturating_add(upper_batches.saturating_mul(types.len() * 128 + 1024)),
        usize::MAX,
    ) {
        return Ok(None);
    }
    let mut output = Vec::new();
    let mut selected = Vec::with_capacity(DEFAULT_BATCH_ROWS);
    let mut current = None;
    let emit = |frame: usize,
                selected: &mut Vec<u32>,
                output: &mut Vec<Batch>,
                charge: &mut ScopedCharge|
     -> Result<(), QueryError> {
        if selected.is_empty() {
            return Ok(());
        }
        let reservation = selected
            .len()
            .saturating_mul(per_row)
            .saturating_add(types.len() * 128 + 1024);
        let owned = charge.split(reservation);
        let columns = frames[frame]
            .1
            .columns()
            .iter()
            .map(|column| column.take_indices_exact(selected))
            .collect();
        output.push(Batch::new(schema.clone(), columns).with_memory_charge(owned)?);
        selected.clear();
        Ok(())
    };
    for entry in entries.iter().filter(|entry| kept(entry)) {
        if selected.len() % 1024 == 0 {
            crate::engine::cancel::check_cancelled()?;
        }
        if current != Some(entry.frame as usize) || selected.len() == DEFAULT_BATCH_ROWS {
            if let Some(frame) = current {
                emit(frame, &mut selected, &mut output, &mut output_charge)?;
            }
            current = Some(entry.frame as usize);
        }
        selected.push(entry.row);
    }
    if let Some(frame) = current {
        emit(frame, &mut selected, &mut output, &mut output_charge)?;
    }
    Ok(Some(output))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use crate::engine::executor::vec::schema::BatchField;

    fn fixture(generic_tail: bool, wide: bool) -> (Arc<BatchSchema>, Vec<Batch>, Vec<Batch>) {
        let schema = Arc::new(BatchSchema::new(
            [
                ("id", ColumnType::LongLong),
                ("day", ColumnType::Date),
                (
                    "amount",
                    ColumnType::Decimal {
                        precision: 38,
                        scale: 2,
                    },
                ),
            ]
            .into_iter()
            .map(|(name, column_type)| BatchField {
                table: "t".into(),
                alias: None,
                name: name.into(),
                column_type,
                source: Default::default(),
            })
            .collect(),
        ));
        let make = |start: i64, end: i64, generic: bool| {
            let columns = (0..3)
                .map(|column| {
                    let values = (start..end)
                        .map(|row| match column {
                            0 if row % 7 == 0 => Value::Null,
                            0 => Value::Int(if wide {
                                if row % 2 == 0 {
                                    i64::MIN
                                } else {
                                    i64::MAX
                                }
                            } else {
                                row % 29 - 14
                            }),
                            1 if row % 11 == 0 => Value::Null,
                            1 => Value::Date((18000 + row % 5) as i32),
                            _ if row % 13 == 0 => Value::Null,
                            _ if wide => Value::Decimal128(
                                if row % 2 == 0 {
                                    i128::MIN + 1
                                } else {
                                    i128::MAX
                                },
                                2,
                            ),
                            _ => Value::DecimalCompact(row % 17 - 8, 2),
                        })
                        .collect();
                    if generic && column == 2 {
                        ColumnVector::Generic { values }
                    } else {
                        ColumnVector::from_values(values)
                    }
                })
                .collect();
            Batch::new(schema.clone(), columns)
        };
        let a = make(0, 128, false).with_selection(
            crate::engine::executor::vec::batch::Selection::Indices(vec![
                0, 1, 2, 3, 3, 7, 11, 13, 17, 29, 57, 99,
            ]),
        );
        (
            schema.clone(),
            vec![a, make(100, 256, generic_tail)],
            vec![make(128, 350, false)],
        )
    }
    fn rows(batches: &[Batch]) -> Vec<Vec<Value>> {
        batches
            .iter()
            .flat_map(|batch| batch.visible_indices().map(|row| batch.row(row)))
            .collect()
    }
    fn scalar(
        left: Vec<Vec<Value>>,
        right: Vec<Vec<Value>>,
        op: crate::parser::SetOp,
    ) -> Vec<Vec<Value>> {
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let mut l = left.into_iter();
        let mut r = right.into_iter();
        let mut result = crate::engine::executor::dispatch::combine_set_sources_spillable(
            &[None; 3],
            op,
            false,
            3,
            root.path(),
            || Ok(l.next()),
            || Ok(r.next()),
        )
        .unwrap();
        let mut out = Vec::new();
        while let Some(row) = result.next_row().unwrap() {
            out.push(row);
        }
        out
    }
    fn drain(input: &mut Option<Box<dyn BatchOperator>>) -> Vec<Vec<Value>> {
        let mut out = Vec::new();
        while let Some(batch) = input.as_mut().unwrap().next_batch().unwrap() {
            out.extend(rows(&[batch]));
        }
        out
    }
    #[test]
    fn compact_sets_match_scalar_and_transfer_result_reservations() {
        for op in [
            crate::parser::SetOp::Union,
            crate::parser::SetOp::Intersect,
            crate::parser::SetOp::Except,
        ] {
            let (schema, l, r) = fixture(false, false);
            let expected = scalar(rows(&l), rows(&r), op);
            let budget = Arc::new(MemBudget::new(16 * 1024 * 1024));
            with_query_budget(budget.clone(), || {
                let mut left: Option<Box<dyn BatchOperator>> =
                    Some(Box::new(MaterializedBatchOperator::new(schema.clone(), l)));
                let mut right: Option<Box<dyn BatchOperator>> =
                    Some(Box::new(MaterializedBatchOperator::new(schema.clone(), r)));
                let output = prepare(&mut left, &mut right, schema, op)
                    .unwrap()
                    .expect("fixed numeric sets should keep native columns");
                assert!(left.is_none() && right.is_none());
                assert_eq!(rows(&output), expected);
                assert!(
                    budget.used() > 0,
                    "output buffers must retain their reservation after inputs end"
                );
                drop(output);
                assert_eq!(budget.used(), 0);
            });
            assert_eq!(budget.used(), 0);
        }
    }
    #[test]
    fn optional_set_decline_replays_every_consumed_row_once() {
        for (generic, wide, limit) in [
            (true, false, 16 * 1024 * 1024),
            (false, true, 16 * 1024 * 1024),
            (false, false, 1024),
        ] {
            let (schema, l, r) = fixture(generic, wide);
            let expected_l = rows(&l);
            let expected_r = rows(&r);
            let budget = Arc::new(MemBudget::new(limit));
            with_query_budget(budget.clone(), || {
                let mut left: Option<Box<dyn BatchOperator>> =
                    Some(Box::new(MaterializedBatchOperator::new(schema.clone(), l)));
                let mut right: Option<Box<dyn BatchOperator>> =
                    Some(Box::new(MaterializedBatchOperator::new(schema.clone(), r)));
                assert!(
                    prepare(&mut left, &mut right, schema, crate::parser::SetOp::Union)
                        .unwrap()
                        .is_none()
                );
                assert_eq!(drain(&mut left), expected_l);
                assert_eq!(drain(&mut right), expected_r);
                drop(left);
                drop(right);
                assert_eq!(budget.used(), 0);
                assert!(budget.terminal_oom().is_none());
            });
        }
    }
}
