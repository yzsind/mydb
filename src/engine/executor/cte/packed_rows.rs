//! Bounded row-to-column framing for incrementally materialized results.

use super::CteSpillBuilder;
use crate::engine::api::CteResult;
use crate::engine::executor::mem_budget::{self, ScopedCharge};
use crate::engine::executor::memory_footprint::AllocatedBytes;
use crate::engine::executor::vec::batch::{Batch, ColumnVector};
use crate::engine::executor::vec::schema::{BatchField, BatchSchema};
use crate::types::{Column, QueryError, Value};
use std::path::PathBuf;
use std::sync::Arc;

/// Recursive deltas are often a single row. Retaining one allocation per
/// output row wastes space even though the final consumer accepts batches.
/// Frame a bounded prefix before handing it to the ordinary spillable store.
pub(super) struct PackedRows {
    inner: CteSpillBuilder,
    schema: Arc<BatchSchema>,
    columns: Vec<Vec<Value>>,
    rows: usize,
    charge: ScopedCharge,
    pending_bytes: usize,
    row_limit: usize,
    byte_limit: usize,
}

impl PackedRows {
    pub(super) fn new(columns: Vec<Column>, data_dir: PathBuf) -> Self {
        let budget = mem_budget::current_budget().map(|budget| budget.limit());
        let width = columns.len().max(1) * std::mem::size_of::<Value>();
        let byte_limit = budget.map_or(64 * 1024, |bytes| (bytes / 64).clamp(1024, 64 * 1024));
        let row_limit = (byte_limit / width).clamp(1, 1024);
        let schema = Arc::new(BatchSchema::new(
            columns
                .iter()
                .map(|column| BatchField {
                    table: String::new(),
                    alias: None,
                    name: column.name.clone(),
                    column_type: column.column_type,
                    source: column.source.clone(),
                })
                .collect(),
        ));
        Self {
            inner: CteSpillBuilder::new_with_stats(columns, data_dir),
            schema,
            columns: Vec::new(),
            rows: 0,
            charge: ScopedCharge::new(0),
            pending_bytes: 0,
            row_limit,
            byte_limit,
        }
    }

    pub(super) fn push(&mut self, row: Vec<Value>) -> Result<(), QueryError> {
        let heap = row.iter().map(Value::allocated_bytes).sum::<usize>();
        let bytes = self.prepare_row(row.len(), heap)?;
        for (column, value) in self.columns.iter_mut().zip(row) {
            column.push(value);
        }
        self.finish_row(bytes)
    }

    /// Borrow a reusable recursive delta. Scalar Values are copied into
    /// column buffers without allocating a separate vector for every row.
    pub(super) fn push_values(&mut self, row: &[Value]) -> Result<(), QueryError> {
        let heap = row.iter().map(Value::allocated_bytes).sum::<usize>();
        let bytes = self.prepare_row(row.len(), heap)?;
        for (column, value) in self.columns.iter_mut().zip(row) {
            column.push(value.clone());
        }
        self.finish_row(bytes)
    }

    fn prepare_row(&mut self, width: usize, heap: usize) -> Result<usize, QueryError> {
        if width != self.schema.len() {
            return Err(crate::error::catalog::ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT
                .error("The used SELECT statements have a different number of columns"));
        }
        let bytes = width * std::mem::size_of::<Value>() + heap;
        if self.rows != 0 && self.pending_bytes.saturating_add(bytes) > self.byte_limit {
            self.flush()?;
        }
        if self.rows == 0 {
            self.charge.add_force(self.schema.len().saturating_mul(
                self.row_limit * std::mem::size_of::<Value>() + std::mem::size_of::<Vec<Value>>(),
            ))?;
            self.columns =
                mem_budget::try_vec_with_capacity(self.schema.len(), "packed CTE columns")?;
            for _ in 0..self.schema.len() {
                self.columns.push(mem_budget::try_vec_with_capacity(
                    self.row_limit,
                    "packed CTE values",
                )?);
            }
        }
        self.charge.add_force(heap)?;
        Ok(bytes)
    }

    fn finish_row(&mut self, bytes: usize) -> Result<(), QueryError> {
        self.rows += 1;
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
        if self.rows >= self.row_limit || self.pending_bytes >= self.byte_limit {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), QueryError> {
        if self.rows == 0 {
            return Ok(());
        }
        crate::engine::cancel::check_cancelled()?;
        let count = self.rows;
        // The pending rows remain charged until both transposition and the
        // resident/spill copy complete. Include typed lanes and byte growth
        // alongside the moved Values; one indivisible wide row may exceed
        // the framing target, but still requires hard admission.
        let mut scratch = self.charge.sibling();
        scratch.add_force(
            self.pending_bytes
                .saturating_mul(3)
                .saturating_add(self.schema.len().saturating_mul(512)),
        )?;
        let batch = Batch::with_row_count(
            self.schema.clone(),
            std::mem::take(&mut self.columns)
                .into_iter()
                .map(ColumnVector::from_values)
                .collect(),
            count,
        );
        self.inner.push_batch(&batch)?;
        drop(batch);
        self.charge = ScopedCharge::new(0);
        self.pending_bytes = 0;
        self.rows = 0;
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<CteResult, QueryError> {
        self.flush()?;
        self.inner.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ColumnType;

    #[test]
    fn recursive_output_packs_single_row_deltas_into_bounded_native_batches() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let budget = Arc::new(mem_budget::MemBudget::new(16 * 1024 * 1024));
        mem_budget::with_query_budget(budget.clone(), || {
            let mut output = PackedRows::new(
                vec![Column {
                    name: "n".into(),
                    column_type: ColumnType::LongLong,
                    ..Default::default()
                }],
                dir.path().to_path_buf(),
            );
            for n in 1..=10_000 {
                output.push(vec![Value::Int(n)]).unwrap();
            }
            let result = output.finish().unwrap();
            let batches = result
                .store
                .native_batches()
                .expect("compact native output");
            assert_eq!(batches.len(), 10);
            assert_eq!(result.store.nrows(), 10_000);
            assert!(batches.iter().all(|batch| batch.num_rows() <= 1024));
            assert!(batches.iter().map(Batch::resident_bytes).sum::<usize>() < 100_000);
            assert_eq!(result.store.copy_row(0).unwrap(), Some(vec![Value::Int(1)]));
            assert_eq!(
                result.store.copy_row(9999).unwrap(),
                Some(vec![Value::Int(10_000)])
            );
            assert!(budget.peak_used() < 512 * 1024);
        });
        assert_eq!(budget.used(), 0);
    }
}
