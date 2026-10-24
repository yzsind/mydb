//! Batch framing shared by plain and predicate-first storage decoders.
use super::*;

pub(super) struct ScanBatchBuilder {
    pub(super) columns: Vec<ColumnAccumulator>,
    schema: Arc<BatchSchema>,
    batch_rows: usize,
    rows: usize,
    completed: Vec<Batch>,
}

impl ScanBatchBuilder {
    pub(super) fn new(schema: Arc<BatchSchema>, batch_rows: usize) -> Self {
        debug_assert!(batch_rows > 0);
        Self {
            columns: schema
                .fields()
                .iter()
                .map(|field| ColumnAccumulator::for_field(field, batch_rows))
                .collect(),
            schema,
            batch_rows,
            rows: 0,
            completed: Vec::new(),
        }
    }

    #[inline]
    pub(super) fn finish_row(&mut self) -> Result<(), QueryError> {
        self.rows += 1;
        if self.rows == self.batch_rows {
            let next = self
                .schema
                .fields()
                .iter()
                .map(|field| ColumnAccumulator::for_field(field, self.batch_rows))
                .collect();
            let finished = std::mem::replace(&mut self.columns, next);
            self.publish(finished)?;
        }
        Ok(())
    }

    fn publish(&mut self, columns: Vec<ColumnAccumulator>) -> Result<(), QueryError> {
        let columns = columns
            .into_iter()
            .map(ColumnAccumulator::finish_validated)
            .collect::<Result<Vec<_>, _>>()?;
        self.completed.push(Batch::with_row_count(
            self.schema.clone(),
            columns,
            self.rows,
        ));
        self.rows = 0;
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<Vec<Batch>, QueryError> {
        if self.rows != 0 {
            let tail = std::mem::take(&mut self.columns);
            self.publish(tail)?;
        }
        Ok(self.completed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_framing_preserves_nullable_values_and_tail_rows() {
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "v".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }]));
        for width in [64, 1024] {
            for count in [0usize, 1, 63, 64, 65, 1023, 1024, 1025] {
                let values: Vec<_> = (0..count)
                    .map(|n| {
                        if n % 3 == 0 {
                            Value::Null
                        } else {
                            Value::Int(n as i64)
                        }
                    })
                    .collect();
                let mut output = ScanBatchBuilder::new(schema.clone(), width);
                for value in &values {
                    output.columns[0].push(value);
                    output.finish_row().unwrap();
                }
                let batches = output.finish().unwrap();
                assert_eq!(batches.len(), count.div_ceil(width));
                assert!(batches
                    .iter()
                    .all(|batch| batch.num_rows() > 0 && batch.num_rows() <= width));
                let actual: Vec<_> = batches
                    .iter()
                    .flat_map(|batch| {
                        (0..batch.num_rows()).map(|row| batch.columns()[0].value_at(row))
                    })
                    .collect();
                assert_eq!(actual, values);
            }
        }
        // COUNT(*) sources may have rows but no projected columns.
        let mut output = ScanBatchBuilder::new(Arc::new(BatchSchema::new(Vec::new())), 2);
        for _ in 0..3 {
            output.finish_row().unwrap();
        }
        let batches = output.finish().unwrap();
        assert_eq!(
            batches.iter().map(Batch::num_rows).collect::<Vec<_>>(),
            [2, 1]
        );
    }
}
