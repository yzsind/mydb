//! Byte-bounded evaluation of computed columns with owning output chunks.
//! Variable results are evaluated once per logical input occurrence. A chunk
//! may exceed its target by one indivisible row, subject to hard admission.

use super::batch::{Batch, ColumnVector, DEFAULT_BATCH_ROWS};
use super::schema::BatchSchema;
use crate::engine::executor::mem_budget::{self, ScopedCharge};
use crate::engine::expr_program::{ExprProgram, RowView};
use crate::engine::expr_type::ExprTypeKind;
use crate::types::{ColumnType, QueryError, Value};
use std::sync::Arc;

pub(crate) fn byte_target() -> usize {
    mem_budget::current_budget().map_or(2 * 1024 * 1024, |budget| {
        (budget.limit() / 8).clamp(64 * 1024, 2 * 1024 * 1024)
    })
}

/// A static result bound admits the existing typed kernels without probing
/// values or evaluating SQL twice. Unbounded domains use the row cursor.
pub(crate) fn result_bound(program: &ExprProgram) -> Option<usize> {
    let header = std::mem::size_of::<Value>();
    let result = program.result_type();
    let heap = match result.kind {
        ExprTypeKind::String { kind } => {
            let bytes = result.max_octets? as usize;
            // Wire widths use the declared charset; owned character Values
            // are UTF-8 even for single-byte connection/source charsets.
            let utf8 = result
                .collation_id
                .and_then(crate::charset::resolve_collation_id)
                .is_some_and(|collation| collation.charset == "utf8mb4");
            let binary = matches!(
                kind,
                crate::engine::expr_type::StringKind::Binary
                    | crate::engine::expr_type::StringKind::Blob
            );
            bytes.saturating_mul(if utf8 || binary { 1 } else { 4 })
        }
        ExprTypeKind::Json
        | ExprTypeKind::Geometry { .. }
        | ExprTypeKind::Vector { .. }
        | ExprTypeKind::Unknown => result.max_octets? as usize,
        ExprTypeKind::Decimal { precision, .. } if precision > 38 => 128,
        _ => 0,
    };
    Some(header.saturating_add(heap))
}

pub(crate) fn needs_chunks(programs: &[Arc<ExprProgram>], rows: usize) -> bool {
    programs
        .iter()
        .try_fold(0usize, |sum, program| {
            Some(sum.saturating_add(result_bound(program)?))
        })
        .is_none_or(|width| width.saturating_mul(rows) > byte_target())
}

pub(crate) struct ComputedRowCursor {
    input: Batch,
    visible: Vec<usize>,
    position: usize,
    input_row_bound: Option<usize>,
    indices_charge: ScopedCharge,
    failure: Option<QueryError>,
}

impl ComputedRowCursor {
    pub(crate) fn new(input: Batch) -> Result<Self, QueryError> {
        let mut indices_charge = input.memory_charge();
        indices_charge.add_force(
            input
                .visible_rows()
                .saturating_mul(std::mem::size_of::<usize>()),
        )?;
        let mut visible =
            mem_budget::try_vec_with_capacity(input.visible_rows(), "computed input indices")?;
        visible.extend(input.visible_indices());
        // Fixed lanes need no per-row sizing. For byte lanes, inspect only
        // selected offsets once; retain exact sizing for heterogeneous Values.
        let input_row_bound = input.columns().iter().try_fold(0usize, |sum, column| {
            let payload = match column {
                ColumnVector::Generic { .. } | ColumnVector::DecimalWide { .. } => return None,
                ColumnVector::Utf8 { offsets, .. }
                | ColumnVector::Bytes { offsets, .. }
                | ColumnVector::Json { offsets, .. }
                | ColumnVector::Geometry { offsets, .. }
                | ColumnVector::Vector { offsets, .. } => visible
                    .iter()
                    .map(|&row| (offsets[row + 1] - offsets[row]) as usize)
                    .max()
                    .unwrap_or(0),
                _ => 0,
            };
            Some(
                sum.saturating_add(std::mem::size_of::<Value>())
                    .saturating_add(payload),
            )
        });
        Ok(Self {
            input,
            visible,
            position: 0,
            input_row_bound,
            indices_charge,
            failure: None,
        })
    }

    /// Compact a byte-bounded input morsel while preserving physical lanes.
    /// Bounded expressions can then use the same typed kernels as small
    /// batches; crossing the byte target must not imply scalar evaluation.
    pub(crate) fn next_input_chunk(
        &mut self,
        result_width: usize,
    ) -> Result<Option<Batch>, QueryError> {
        if self.position == self.visible.len() {
            return Ok(None);
        }
        let target = byte_target();
        let start = self.position;
        let mut end = start;
        let mut input_bytes = 0usize;
        let mut total_bytes = 0usize;
        if let Some(width) = self.input_row_bound {
            let row_bytes = width.saturating_add(result_width.saturating_mul(2)).max(1);
            let count = (target / row_bytes)
                .clamp(1, DEFAULT_BATCH_ROWS)
                .min(self.visible.len() - start);
            end += count;
            input_bytes = width.saturating_mul(count);
        }
        while self.input_row_bound.is_none()
            && end < self.visible.len()
            && end - start < DEFAULT_BATCH_ROWS
        {
            let row = self.visible[end];
            let bytes = self.input.columns().iter().fold(0usize, |n, column| {
                n.saturating_add(column.owned_value_bytes(row))
            });
            // Include the result conversion allowance in the chunk limit.
            let next = total_bytes
                .saturating_add(bytes)
                .saturating_add(result_width.saturating_mul(2));
            if end > start && next > target {
                break;
            }
            input_bytes = input_bytes.saturating_add(bytes);
            total_bytes = next;
            end += 1;
        }
        let count = end - start;
        let mut scratch = self.indices_charge.sibling();
        scratch.add_force(count.saturating_mul(std::mem::size_of::<u32>()))?;
        let mut indices = mem_budget::try_vec_with_capacity(count, "computed chunk indices")?;
        indices.extend(self.visible[start..end].iter().map(|&row| row as u32));
        let mut charge = self.indices_charge.sibling();
        charge.add_force(
            input_bytes
                .saturating_mul(2)
                .saturating_add(
                    self.input
                        .columns()
                        .len()
                        .saturating_mul(std::mem::size_of::<ColumnVector>() + 32),
                )
                .saturating_add(128),
        )?;
        let mut columns = mem_budget::try_vec_with_capacity(
            self.input.columns().len(),
            "computed chunk columns",
        )?;
        for column in self.input.columns() {
            columns.push(column.take_indices_exact(&indices));
        }
        let batch = Batch::with_row_count(self.input.schema().clone(), columns, count)
            .with_memory_charge(charge)?;
        self.position = end;
        Ok(Some(batch))
    }

    pub(crate) fn next(
        &mut self,
        programs: &[Arc<ExprProgram>],
        schema: &Arc<BatchSchema>,
    ) -> Result<Option<Batch>, QueryError> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        let result = self.next_chunk(programs, schema);
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }

    fn next_chunk(
        &mut self,
        programs: &[Arc<ExprProgram>],
        schema: &Arc<BatchSchema>,
    ) -> Result<Option<Batch>, QueryError> {
        if self.position == self.visible.len() {
            return Ok(None);
        }
        let input_width = self.input.columns().len();
        if input_width.checked_add(programs.len()) != Some(schema.len()) {
            return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                .error("computed chunk schema width mismatch"));
        }
        let target = byte_target();
        let row_width = schema
            .len()
            .saturating_mul(std::mem::size_of::<Value>())
            .max(1);
        let capacity = (target / row_width / 2)
            .clamp(1, DEFAULT_BATCH_ROWS)
            .min(self.visible.len() - self.position);
        let mut charge = self.indices_charge.sibling();
        charge.add_force(
            capacity
                .saturating_mul(row_width)
                .saturating_add(
                    schema
                        .len()
                        .saturating_mul(std::mem::size_of::<ColumnVector>() + 24),
                )
                .saturating_add(128),
        )?;
        let mut columns =
            mem_budget::try_vec_with_capacity(schema.len(), "computed output columns")?;
        for _ in 0..schema.len() {
            columns.push(mem_budget::try_vec_with_capacity(
                capacity,
                "computed output lane",
            )?);
        }
        // Reuse row containers and their scratch reservation for the whole
        // chunk. Reserve retained payload in small blocks, refunding unused
        // headroom before handing the owning batch to its consumer.
        let mut scratch = self.indices_charge.sibling();
        let mut payload = mem_budget::try_vec_with_capacity(input_width, "computed input row")?;
        let mut typed = mem_budget::try_vec_with_capacity(input_width, "computed typed input")?;
        let mut prepaid = 0usize;
        let mut count = 0;
        let mut logical_bytes = 0usize;
        while count < capacity && (count == 0 || logical_bytes < target) {
            crate::engine::cancel::check_cancelled()?;
            let row = self.visible[self.position];
            let raw_bytes = self.input.columns().iter().fold(0usize, |n, column| {
                n.saturating_add(column.owned_value_bytes(row))
            });
            let raw_heap =
                raw_bytes.saturating_sub(input_width.saturating_mul(std::mem::size_of::<Value>()));
            charge_payload(&mut charge, &mut prepaid, raw_heap)?;
            let scratch_bytes = raw_bytes
                .saturating_mul(2)
                .saturating_add(2 * std::mem::size_of::<Vec<Value>>());
            scratch.add_force(scratch_bytes.saturating_sub(scratch.charged()))?;
            payload.clear();
            typed.clear();
            for column in self.input.columns() {
                payload.push(column.try_value_at(row)?);
            }
            // The canonical batch evaluator observes declared FLOAT inputs as
            // Value::Float. Keep the forwarded payload's physical tag intact.
            for (value, field) in payload.iter().zip(self.input.schema().fields()) {
                typed.push(match (field.column_type, value) {
                    (ColumnType::Float, Value::Double(value)) => Value::Float(*value as f32),
                    _ => mem_budget::try_clone_value(value, "computed typed input value")?,
                });
            }
            let view = RowView::from_values(&typed);
            logical_bytes = logical_bytes.saturating_add(raw_bytes);
            for (index, program) in programs.iter().enumerate() {
                let value = crate::engine::expr_program::with_eval_scratch(|scratch| {
                    program.eval_row(&view, scratch)
                })?;
                let bytes = mem_budget::estimate_value_bytes(&value);
                charge_payload(
                    &mut charge,
                    &mut prepaid,
                    bytes.saturating_sub(std::mem::size_of::<Value>()),
                )?;
                logical_bytes = logical_bytes.saturating_add(bytes);
                columns[input_width + index].push(value);
            }
            for (column, value) in columns.iter_mut().zip(payload.drain(..)) {
                column.push(value);
            }
            self.position += 1;
            count += 1;
        }
        charge.release_bytes(prepaid);
        // Typed lanes keep adjacent fixed-key/column kernels available. The
        // conversion is optional: a hard admission miss keeps the already
        // owned Generic lanes instead of failing an otherwise feasible row.
        let conversion_bytes = columns.iter().fold(0usize, |n, values| {
            n.saturating_add(
                values
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Value>()),
            )
            .saturating_add(
                values
                    .iter()
                    .map(|value| {
                        mem_budget::estimate_value_bytes(value)
                            .saturating_sub(std::mem::size_of::<Value>())
                    })
                    .sum::<usize>(),
            )
            .saturating_add(128)
        });
        let mut conversion = charge.sibling();
        let typed = conversion.add_within(conversion_bytes, usize::MAX);
        let columns = columns
            .into_iter()
            .enumerate()
            .map(|(index, values)| {
                let preserve = if index < input_width {
                    matches!(self.input.columns()[index], ColumnVector::Generic { .. })
                } else {
                    matches!(
                        programs[index - input_width].result_type().kind,
                        ExprTypeKind::Unknown
                    )
                };
                if typed && !preserve {
                    ColumnVector::from_values(values)
                } else {
                    ColumnVector::Generic { values }
                }
            })
            .collect();
        charge.absorb(&mut conversion);
        Batch::with_row_count(schema.clone(), columns, count)
            .with_memory_charge(charge)
            .map(Some)
    }
}

/// Amortize global/query accounting without dropping hard admission. A
/// failed speculative block reservation retries the exact required size.
fn charge_payload(
    charge: &mut ScopedCharge,
    prepaid: &mut usize,
    bytes: usize,
) -> Result<(), QueryError> {
    if bytes > *prepaid {
        let required = bytes - *prepaid;
        let block = required.max(64 * 1024);
        if charge.add_within(block, usize::MAX) {
            *prepaid = prepaid.saturating_add(block);
        } else {
            charge.add_force(required)?;
            *prepaid = prepaid.saturating_add(required);
        }
    }
    *prepaid -= bytes;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{batch::Selection, schema::BatchField};
    use super::*;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};

    fn fixture(expr: &str, count: usize) -> (Batch, Arc<ExprProgram>, Arc<BatchSchema>) {
        let field = |name: &str, column_type| BatchField {
            table: String::new(),
            alias: None,
            name: name.into(),
            column_type,
            source: Default::default(),
        };
        let input_schema = Arc::new(BatchSchema::new(vec![field("n", ColumnType::LongLong)]));
        let expr = crate::parser::parse_expr_fragment(expr).unwrap();
        let program = super::super::expr_batch::compile_for_batch(&expr, &input_schema)
            .unwrap()
            .unwrap();
        let mut fields = input_schema.fields().to_vec();
        fields.push(field("v", ColumnType::VarString));
        let schema = Arc::new(BatchSchema::new(fields));
        let input = Batch::new(
            input_schema,
            vec![ColumnVector::from_values(
                (0..count).map(|n| Value::Int(n as i64)).collect(),
            )],
        );
        (input, program, schema)
    }

    #[test]
    fn wide_computed_results_are_incremental_and_outlive_their_cursor() {
        let budget = Arc::new(MemBudget::new(1024 * 1024));
        with_query_budget(budget.clone(), || {
            let (input, program, schema) = fixture("REPEAT('x',65536)", 32);
            assert!(needs_chunks(
                std::slice::from_ref(&program),
                input.num_rows()
            ));
            let mut cursor = ComputedRowCursor::new(input).unwrap();
            let mut count = 0;
            let mut held = None;
            while let Some(batch) = cursor
                .next(std::slice::from_ref(&program), &schema)
                .unwrap()
            {
                assert!(batch.num_rows() < 32);
                for row in batch.visible_indices() {
                    assert_eq!(batch.columns()[0].value_at(row), Value::Int(count));
                    assert_eq!(
                        crate::types::value_str(&batch.columns()[1].value_at(row))
                            .unwrap()
                            .len(),
                        65536
                    );
                    count += 1;
                }
                held = Some(batch);
            }
            assert_eq!(count, 32);
            drop(cursor);
            assert!(
                budget.used() >= 65536,
                "consumer-held payload lost its owner"
            );
            assert!(
                budget.peak_used() < 1024 * 1024,
                "computed entire 2 MiB input: {}",
                budget.peak_used()
            );
            drop(held);
            assert_eq!(budget.used(), 0);
        });
    }

    #[test]
    fn computed_chunks_compact_selection_and_preserve_duplicates() {
        let (input, program, schema) = fixture("CONCAT(n,REPEAT('x',65536))", 3);
        let input = input.with_selection(Selection::Indices(vec![2, 0, 2]));
        let mut cursor = ComputedRowCursor::new(input).unwrap();
        let mut actual = Vec::new();
        while let Some(batch) = cursor
            .next(std::slice::from_ref(&program), &schema)
            .unwrap()
        {
            assert!(batch.selection().is_none());
            for row in batch.visible_indices() {
                let Value::Int(n) = batch.columns()[0].value_at(row) else {
                    panic!("input");
                };
                let text = batch.columns()[1].value_at(row);
                assert!(crate::types::value_str(&text)
                    .unwrap()
                    .starts_with(&n.to_string()));
                actual.push(n);
            }
        }
        assert_eq!(actual, vec![2, 0, 2]);
    }
    #[test]
    fn bounded_input_chunks_preserve_selection_tags_nulls_and_skewed_payloads() {
        use super::super::batch::Selection;
        let budget = Arc::new(MemBudget::new(1024 * 1024));
        with_query_budget(budget.clone(), || {
            // One invisible outlier must not size the selected copy.
            let fields = vec![
                BatchField {
                    table: String::new(),
                    alias: None,
                    name: "v".into(),
                    column_type: ColumnType::VarString,
                    source: Default::default(),
                },
                BatchField {
                    table: String::new(),
                    alias: None,
                    name: "f".into(),
                    column_type: ColumnType::Float,
                    source: Default::default(),
                },
            ];
            let input = Batch::new(
                Arc::new(BatchSchema::new(fields)),
                vec![
                    ColumnVector::from_values(vec![
                        Value::String("small".into()),
                        Value::String("x".repeat(2 * 1024 * 1024)),
                        Value::Null,
                        Value::String("last".into()),
                    ]),
                    ColumnVector::Generic {
                        values: vec![
                            Value::Float(1.0),
                            Value::Double(2.0),
                            Value::Null,
                            Value::Double(3.0),
                        ],
                    },
                ],
            );
            let selections = [
                Selection::Indices(vec![3, 0, 3, 2]),
                Selection::Range { start: 2, len: 2 },
                Selection::Mask({
                    let mut mask = super::super::bitmap::Bitmap::new_all_invalid(4);
                    mask.set_true(0);
                    mask.set_true(2);
                    mask
                }),
            ];
            for selection in selections {
                let selected = input.clone().with_selection(selection);
                let expected = selected
                    .visible_indices()
                    .map(|row| selected.row(row))
                    .collect::<Vec<_>>();
                let mut cursor = ComputedRowCursor::new(selected).unwrap();
                let mut actual = Vec::new();
                let mut held = None;
                // Force one row per chunk without evaluating an expression.
                while let Some(batch) = cursor.next_input_chunk(40 * 1024).unwrap() {
                    assert_eq!(batch.num_rows(), 1);
                    assert!(matches!(&batch.columns()[1], ColumnVector::Generic { .. }));
                    actual.push(batch.row(0));
                    held = Some(batch);
                }
                assert_eq!(actual, expected);
                drop(cursor);
                assert!(budget.used() > 0);
                drop(held);
                assert_eq!(budget.used(), 0);
            }
            assert!(budget.peak_used() < 64 * 1024);
        });
    }
}
