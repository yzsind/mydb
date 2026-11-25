//! Column-preserving assignment for unary recursive batches.

use crate::engine::executor::vec::batch::Batch;
use crate::types::{Column, ColumnType, QueryError, Value};

pub(super) fn assign_batch(
    batch: &Batch,
    columns: &[Column],
    identity: &[bool],
    coerce_row: impl Fn(&mut [Value]) -> Result<(), QueryError>,
) -> Result<Option<Batch>, QueryError> {
    use crate::engine::executor::vec::batch::{Batch, ColumnVector};
    crate::engine::cancel::check_cancelled()?;
    let needed = batch.resident_bytes().saturating_mul(3).saturating_add(
        batch
            .visible_rows()
            .saturating_mul(columns.len() * std::mem::size_of::<Value>()),
    );
    let mut scratch = crate::engine::executor::mem_budget::ScopedCharge::new(0);
    // Keep headroom for the two spillable consumers. A rejected speculative
    // batch allocation must not cancel a query that can advance row by row.
    if !scratch.add_within(needed, scratch.remaining() / 2) {
        return Ok(None);
    }
    let indices: Vec<u32> = batch.visible_indices().map(|i| i as u32).collect();
    let mut values: Vec<Vec<Value>> = identity
        .iter()
        .map(|&same| {
            if same {
                Vec::new()
            } else {
                Vec::with_capacity(indices.len())
            }
        })
        .collect();
    let mut row = vec![Value::Null; columns.len()];
    for &index in &indices {
        for (field, column) in columns.iter().enumerate() {
            if !identity[field] {
                row[field] =
                    batch.columns()[field].typed_value_at(index as usize, column.column_type);
                if matches!(column.column_type, ColumnType::Decimal { .. }) {
                    row[field] =
                        crate::engine::expr::decimal_result_value(&row[field], column.column_type)
                            .into_owned();
                }
            }
        }
        coerce_row(&mut row)?;
        for (field, output) in values.iter_mut().enumerate() {
            if !identity[field] {
                output.push(std::mem::replace(&mut row[field], Value::Null));
            }
        }
    }
    let lanes = values
        .into_iter()
        .enumerate()
        .map(|(field, values)| {
            if identity[field] {
                batch.columns()[field].take_indices_exact(&indices)
            } else {
                ColumnVector::from_values(values)
            }
        })
        .collect();
    let output = Batch::with_row_count(batch.schema().clone(), lanes, indices.len());
    output.with_memory_charge(scratch).map(Some)
}

pub(super) fn typed_row(batch: &Batch, columns: &[Column], index: usize) -> Vec<Value> {
    columns
        .iter()
        .enumerate()
        .map(|(field, column)| {
            let value = batch.columns()[field].typed_value_at(index, column.column_type);
            if matches!(column.column_type, ColumnType::Decimal { .. }) {
                crate::engine::expr::decimal_result_value(&value, column.column_type).into_owned()
            } else {
                value
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::{
        mem_budget,
        vec::{batch::ColumnVector, schema::BatchSchema},
    };
    use std::sync::Arc;

    #[test]
    fn recursive_assignment_declines_batch_under_pressure_without_losing_rows() {
        let schema = Arc::new(BatchSchema::new(vec![
            crate::engine::executor::vec::schema::BatchField {
                table: String::new(),
                alias: None,
                name: "n".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
        ]));
        let batch = Batch::new(
            schema,
            vec![ColumnVector::from_values(
                (0..1024).map(Value::Int).collect(),
            )],
        );
        let columns = vec![Column {
            name: "n".into(),
            column_type: ColumnType::LongLong,
            ..Default::default()
        }];
        let budget = Arc::new(mem_budget::MemBudget::new(64 * 1024));
        mem_budget::with_query_budget(budget.clone(), || {
            assert!(assign_batch(&batch, &columns, &[true], |_| panic!(
                "rejected batch must not evaluate assignment"
            ))
            .unwrap()
            .is_none());
            assert_eq!(budget.used(), 0);
            let values = batch
                .visible_indices()
                .map(|i| typed_row(&batch, &columns, i))
                .collect::<Vec<_>>();
            assert_eq!(values.len(), 1024);
            assert_eq!(values[1023], vec![Value::Int(1023)]);
            let _next = mem_budget::force_charge_scoped(1024).unwrap();
        });
        assert_eq!(budget.used(), 0);
    }
}
