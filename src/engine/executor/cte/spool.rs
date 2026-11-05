//! Bounded columnar materialization with a compatibility row cursor.

use crate::engine::executor::mem_budget::{self, ScopedCharge};
use crate::engine::executor::memory_footprint::AllocatedBytes;
use crate::engine::executor::spill_quota::{
    self, SpillCharger, TrackedSpillReader, TrackedSpillWriter,
};
use crate::engine::executor::vec::batch::{Batch, ColumnVector, Selection};
use crate::engine::executor::vec::schema::BatchSchema;
use crate::types::{ColumnType, QueryError, Value};
use std::path::Path;
use std::sync::Arc;

const ROWS_PER_FRAME: usize = 1024;
const ROW_BYTES_PER_FRAME: usize = 1024 * 1024;

pub(super) struct Writer {
    writer: TrackedSpillWriter,
    pub(super) schema: Arc<BatchSchema>,
    rows: Vec<Vec<Value>>,
    charge: ScopedCharge,
    pending_bytes: usize,
    owned_bytes: usize,
    row_limit: usize,
    byte_limit: usize,
}

impl Writer {
    pub(super) fn create(
        path: &Path,
        schema: Arc<BatchSchema>,
        charger: SpillCharger,
    ) -> Result<Self, QueryError> {
        let quota = mem_budget::current_budget().map(|budget| budget.limit());
        let row_limit = quota.map_or(ROWS_PER_FRAME, |bytes| {
            (bytes / 64 / std::mem::size_of::<Vec<Value>>()).clamp(1, ROWS_PER_FRAME)
        });
        let byte_limit = quota.map_or(ROW_BYTES_PER_FRAME, |bytes| {
            (bytes / 8).clamp(1024, ROW_BYTES_PER_FRAME)
        });
        Ok(Self {
            writer: TrackedSpillWriter::create_new(
                path,
                charger,
                spill_quota::writer_buffer_capacity(1),
            )?,
            schema,
            rows: Vec::new(),
            charge: ScopedCharge::new(0),
            pending_bytes: 0,
            owned_bytes: 0,
            row_limit,
            byte_limit,
        })
    }

    pub(super) fn append(&mut self, row: Vec<Value>) -> Result<(), QueryError> {
        self.append_inner(row, true)
    }

    /// The builder retains its resident reservation until this prefix has
    /// been flushed. Moving its rows must not charge their payload twice.
    pub(super) fn append_precharged(&mut self, row: Vec<Value>) -> Result<(), QueryError> {
        self.append_inner(row, false)
    }

    fn append_inner(&mut self, row: Vec<Value>, owned: bool) -> Result<(), QueryError> {
        if row.len() != self.schema.len() {
            return Err(super::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "CTE spill row width mismatch",
            ));
        }
        if self.rows.capacity() == 0 {
            self.charge
                .add_force(self.row_limit * std::mem::size_of::<Vec<Value>>())?;
            self.rows
                .try_reserve_exact(self.row_limit)
                .map_err(|_| mem_budget::allocation_error("CTE spill row buffer"))?;
        }
        let bytes = row.allocated_bytes();
        if owned {
            self.charge.add_force(bytes)?;
            self.owned_bytes = self.owned_bytes.saturating_add(bytes);
        }
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
        self.rows.push(row);
        if self.rows.len() >= self.row_limit || self.pending_bytes >= self.byte_limit {
            self.flush_rows()?;
        }
        Ok(())
    }

    pub(super) fn flush_rows(&mut self) -> Result<(), QueryError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        crate::engine::cancel::check_cancelled()?;
        let count = self.rows.len();
        let columns = self.schema.len();
        let mut scratch = self.charge.sibling();
        scratch.add_force(
            count
                .saturating_mul(columns)
                .saturating_mul(std::mem::size_of::<Value>())
                .saturating_add(columns.saturating_mul(
                    std::mem::size_of::<ColumnVector>() + std::mem::size_of::<Vec<Value>>(),
                ))
                .saturating_add(256),
        )?;
        let mut values = mem_budget::try_vec_with_capacity(columns, "CTE spill transpose")?;
        for _ in 0..columns {
            values.push(mem_budget::try_vec_with_capacity(
                count,
                "CTE spill column",
            )?);
        }
        for row in self.rows.drain(..) {
            for (value, column) in row.into_iter().zip(&mut values) {
                column.push(value);
            }
        }
        // Move the scalar payloads. A row-origin prefix need not clone text
        // merely to acquire a typed lane; the canonical codec handles it.
        let batch = Batch::with_row_count(
            self.schema.clone(),
            values
                .into_iter()
                .map(|values| ColumnVector::Generic { values })
                .collect(),
            count,
        );
        crate::engine::executor::vec::spill::write_batch(&mut self.writer, &batch)?;
        drop(batch);
        self.charge.release_bytes(self.owned_bytes);
        self.owned_bytes = 0;
        self.pending_bytes = 0;
        Ok(())
    }

    pub(super) fn append_batch(&mut self, batch: &Batch) -> Result<(), QueryError> {
        if batch.columns().len() != self.schema.len() {
            return Err(super::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "CTE spill batch width mismatch",
            ));
        }
        // Preserve the FLOAT32 rounding formerly performed at the row
        // boundary. Other native lanes can be serialized without conversion.
        if self
            .schema
            .fields()
            .iter()
            .zip(batch.columns())
            .any(|(field, column)| {
                field.column_type == ColumnType::Float
                    && matches!(
                        column,
                        ColumnVector::F64 { .. } | ColumnVector::Generic { .. }
                    )
            })
        {
            for row in batch.visible_indices() {
                self.append_batch_row(batch, row)?;
            }
            return Ok(());
        }
        self.flush_rows()?;
        if batch.visible_rows() != 0 {
            crate::engine::executor::vec::spill::write_batch(&mut self.writer, batch)?;
        }
        Ok(())
    }

    pub(super) fn append_batch_row(&mut self, batch: &Batch, row: usize) -> Result<(), QueryError> {
        let mut scratch = self.charge.sibling();
        scratch.add_force(
            batch
                .columns()
                .iter()
                .map(|column| column.owned_value_bytes(row))
                .sum(),
        )?;
        let mut values = mem_budget::try_vec_with_capacity(self.schema.len(), "CTE logical row")?;
        for (column, field) in batch.columns().iter().zip(self.schema.fields()) {
            let value = column.try_value_at(row)?;
            values.push(match (field.column_type, value) {
                (ColumnType::Float, Value::Double(value)) => Value::Float(value as f32),
                (_, value) => value,
            });
        }
        self.append(values)
    }

    pub(super) fn finish(mut self) -> Result<u64, QueryError> {
        self.flush_rows()?;
        self.writer.finish()
    }
}

pub(crate) struct Reader {
    reader: TrackedSpillReader,
    schema: Arc<BatchSchema>,
    expected_rows: usize,
    loaded_rows: usize,
    batch: Option<Batch>,
    position: usize,
    decoded: ScopedCharge,
    batch_row_cap: usize,
}

impl Reader {
    pub(crate) fn open(
        file: &crate::engine::api::CteSpillFile,
    ) -> Result<Option<Self>, QueryError> {
        let charger = file._dir.charger();
        if charger.absent_partition(&file.path)? {
            if file.row_count != 0 {
                return Err(super::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "materialized CTE spill file is missing",
                ));
            }
            return Ok(None);
        }
        let reader =
            TrackedSpillReader::open(&file.path, charger, spill_quota::writer_buffer_capacity(1))?;
        let decoded = reader.memory_charge();
        Ok(Some(Self {
            reader,
            schema: file.batch_schema.clone(),
            expected_rows: file.row_count,
            loaded_rows: 0,
            batch: None,
            position: 0,
            decoded,
            batch_row_cap: 0,
        }))
    }

    fn load_batch(&mut self) -> Result<bool, QueryError> {
        if self.batch.is_some() {
            return Ok(true);
        }
        loop {
            crate::engine::cancel::check_cancelled()?;
            let Some(batch) = crate::engine::executor::vec::spill::read_owned_batch(
                &mut self.reader,
                self.schema.clone(),
            )?
            else {
                if self.loaded_rows != self.expected_rows {
                    return Err(super::error(
                        crate::error::catalog::ER_UNKNOWN_ERROR,
                        "materialized CTE spill row count mismatch",
                    ));
                }
                return Ok(false);
            };
            self.loaded_rows = self
                .loaded_rows
                .checked_add(batch.num_rows())
                .ok_or_else(|| mem_budget::allocation_error("CTE spill row count overflow"))?;
            if self.loaded_rows > self.expected_rows {
                return Err(super::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "materialized CTE spill contains excess rows",
                ));
            }
            if batch.num_rows() == 0 {
                continue;
            }
            self.batch = Some(batch);
            self.position = 0;
            return Ok(true);
        }
    }

    pub(crate) fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if !self.load_batch()? {
            return Ok(None);
        }
        let batch = self.batch.take().expect("loaded CTE batch");
        let start = std::mem::take(&mut self.position);
        if start == 0 {
            return Ok(Some(batch));
        }
        let mut indices = mem_budget::try_vec_with_capacity(
            batch.num_rows() - start,
            "CTE remaining row positions",
        )?;
        indices.extend((start..batch.num_rows()).map(|row| row as u32));
        Ok(Some(batch.with_selection(Selection::Indices(indices))))
    }

    fn copy_next_row(
        &mut self,
        charge: &mut ScopedCharge,
    ) -> Result<Option<Vec<Value>>, QueryError> {
        if !self.load_batch()? {
            return Ok(None);
        }
        let batch = self.batch.as_ref().expect("loaded CTE batch");
        let bytes = batch
            .columns()
            .iter()
            .map(|column| column.owned_value_bytes(self.position))
            .sum::<usize>()
            .saturating_add(std::mem::size_of::<Vec<Value>>());
        charge.add_force(bytes)?;
        let mut row =
            mem_budget::try_vec_with_capacity(self.schema.len(), "CTE compatibility row")?;
        for (column, field) in batch.columns().iter().zip(self.schema.fields()) {
            let value = column.try_value_at(self.position)?;
            row.push(match (field.column_type, value) {
                (ColumnType::Float, Value::Double(value)) => Value::Float(value as f32),
                (_, value) => value,
            });
        }
        self.position += 1;
        if self.position == batch.num_rows() {
            self.batch = None;
            self.position = 0;
        }
        Ok(Some(row))
    }

    pub(crate) fn next(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
        let mut charge = self.reader.memory_charge();
        let result = self.copy_next_row(&mut charge);
        self.decoded = charge;
        result
    }

    pub(crate) fn next_owned(&mut self) -> Result<Option<(Vec<Value>, ScopedCharge)>, QueryError> {
        Ok(self.next()?.map(|row| (row, self.take_decode_charge())))
    }

    pub(crate) fn take_decode_charge(&mut self) -> ScopedCharge {
        let empty = self.reader.memory_charge();
        std::mem::replace(&mut self.decoded, empty)
    }

    pub(crate) fn column_count(&self) -> usize {
        self.schema.len()
    }
    pub(crate) fn decode_limit(&self) -> usize {
        self.reader.decode_limit()
    }
    pub(crate) fn is_exhausted(&self) -> bool {
        self.batch.is_none()
            && self.reader.remaining_bytes() == 0
            && self.loaded_rows == self.expected_rows
    }

    pub(crate) fn reserve_decode_batch(&mut self, max_rows: usize) -> Result<usize, QueryError> {
        self.decoded = self.reader.memory_charge();
        let bytes = self
            .schema
            .len()
            .saturating_mul(std::mem::size_of::<Value>())
            .saturating_mul(2);
        self.batch_row_cap = max_rows.min((self.decode_limit() / bytes.max(1)).max(1));
        self.decoded
            .add_force(self.batch_row_cap.saturating_mul(bytes))?;
        Ok(self.batch_row_cap)
    }

    pub(crate) fn read_columns(
        &mut self,
        columns: &mut [Vec<Value>],
        max_rows: usize,
    ) -> Result<usize, QueryError> {
        if columns.len() != self.schema.len() {
            return Err(super::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "CTE replay column width mismatch",
            ));
        }
        let mut charge = self.take_decode_charge();
        let mut count = 0;
        while count < max_rows.min(self.batch_row_cap) {
            let Some(row) = self.copy_next_row(&mut charge)? else {
                break;
            };
            for (column, value) in columns.iter_mut().zip(row) {
                column.push(value);
            }
            count += 1;
        }
        self.decoded = charge;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::cte::CteSpillBuilder;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};

    fn build(root: &Path, rows: usize) -> crate::engine::api::CteResult {
        let mut builder = CteSpillBuilder::new(
            vec![crate::types::Column {
                name: "k".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            }],
            root.to_path_buf(),
        );
        builder.threshold = 0;
        for row in 0..rows {
            builder.push(vec![Value::Int(row as i64)]).unwrap();
        }
        builder.finish().unwrap()
    }

    #[test]
    fn columnar_and_row_replay_can_be_interleaved_without_losing_positions() {
        let root = tempfile::tempdir().unwrap();
        let result = build(root.path(), 2051);
        let mut reader = result
            .spill
            .as_ref()
            .unwrap()
            .open_reader()
            .unwrap()
            .unwrap();
        let mut expected = 0;
        assert_eq!(reader.next().unwrap().unwrap(), vec![Value::Int(expected)]);
        expected += 1;
        let batch = reader.next_batch().unwrap().unwrap();
        for row in batch.visible_indices() {
            assert_eq!(batch.columns()[0].value_at(row), Value::Int(expected));
            expected += 1;
        }
        drop(batch);
        let (row, charge) = reader.next_owned().unwrap().unwrap();
        assert_eq!(row, vec![Value::Int(expected)]);
        expected += 1;
        drop(charge);
        reader.reserve_decode_batch(7).unwrap();
        let mut columns = vec![Vec::new()];
        let count = reader.read_columns(&mut columns, 7).unwrap();
        assert_eq!(count, 7);
        for value in columns.pop().unwrap() {
            assert_eq!(value, Value::Int(expected));
            expected += 1;
        }
        while let Some(batch) = reader.next_batch().unwrap() {
            for row in batch.visible_indices() {
                assert_eq!(batch.columns()[0].value_at(row), Value::Int(expected));
                expected += 1;
            }
        }
        assert_eq!(expected, 2051);
    }

    #[test]
    fn cte_replay_rejects_missing_files_and_complete_frame_row_count_mismatch() {
        let root = tempfile::tempdir().unwrap();
        let mut result = build(root.path(), 19);
        let file = Arc::get_mut(result.spill.as_mut().unwrap()).unwrap();
        file.row_count += 1;
        let mut reader = file.open_reader().unwrap().unwrap();
        assert!(reader.next_batch().unwrap().is_some());
        assert!(
            !reader.is_exhausted(),
            "declared rows still require validation"
        );
        assert!(reader.next_batch().is_err());
        drop(reader);
        std::fs::remove_file(&file.path).unwrap();
        assert!(file.open_reader().is_err());
    }

    #[test]
    fn replay_batch_retains_its_memory_owner_after_reader_and_spool_drop() {
        let root = tempfile::tempdir().unwrap();
        let budget = Arc::new(MemBudget::new(8 * 1024 * 1024));
        with_query_budget(budget.clone(), || {
            let result = build(root.path(), 65);
            let mut reader = result
                .spill
                .as_ref()
                .unwrap()
                .open_reader()
                .unwrap()
                .unwrap();
            let batch = reader.next_batch().unwrap().unwrap();
            drop(reader);
            drop(result);
            assert!(budget.used() > 0);
            assert_eq!(batch.columns()[0].value_at(64), Value::Int(64));
            drop(batch);
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.spill_bytes(), 0);
        });
    }

    #[test]
    fn zero_width_spool_preserves_empty_rows() {
        let root = tempfile::tempdir().unwrap();
        let mut builder = CteSpillBuilder::new(Vec::new(), root.path().to_path_buf());
        builder.threshold = 0;
        for _ in 0..17 {
            builder.push(Vec::new()).unwrap();
        }
        let result = builder.finish().unwrap();
        let mut reader = result
            .spill
            .as_ref()
            .unwrap()
            .open_reader()
            .unwrap()
            .unwrap();
        for _ in 0..17 {
            assert_eq!(reader.next().unwrap(), Some(Vec::new()));
        }
        assert!(reader.next().unwrap().is_none());
    }
}
