//! Immutable payload batches for key-only page selection under memory pressure.
use super::*;

pub(super) struct Spool {
    directory: super::super::super::grace::SpillTmpDir,
}

impl Spool {
    pub(super) fn new(data_dir: &std::path::Path) -> Result<Self, QueryError> {
        Ok(Self {
            directory: super::super::super::grace::SpillTmpDir::create_kind(data_dir, "sort")?,
        })
    }

    pub(super) fn store(&self, index: usize, batch: &Batch) -> Result<(), QueryError> {
        // Row references keep physical positions. Selected batches remain
        // resident or use the original sorter rather than renumbering them.
        debug_assert!(batch.selection().is_none());
        let path = self.directory.path().join(format!("payload-{index}.batch"));
        let mut writer = crate::engine::executor::spill_quota::TrackedSpillWriter::create_new(
            &path,
            self.directory.charger(),
            32 * 1024,
        )?;
        crate::engine::executor::vec::spill::write_batch(&mut writer, batch)?;
        writer.finish()?;
        Ok(())
    }

    pub(super) fn read(&self, index: usize, schema: Arc<BatchSchema>) -> Result<Batch, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        let path = self.directory.path().join(format!("payload-{index}.batch"));
        let mut reader = crate::engine::executor::spill_quota::TrackedSpillReader::open(
            &path,
            self.directory.charger(),
            32 * 1024,
        )?;
        crate::engine::executor::vec::spill::read_owned_batch(&mut reader, schema)?.ok_or_else(
            || crate::error::catalog::ER_UNKNOWN_ERROR.error("empty page payload spill batch"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_spool_preserves_typed_rows_and_releases_query_and_disk_charges() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let budget = Arc::new(mem_budget::MemBudget::new(4 * 1024 * 1024));
        mem_budget::with_query_budget(budget.clone(), || {
            let schema = Arc::new(BatchSchema::new(vec![BatchField {
                table: "t".into(),
                alias: None,
                name: "v".into(),
                column_type: ColumnType::VarString,
                source: Default::default(),
            }]));
            let batch = Batch::new(
                schema.clone(),
                vec![ColumnVector::from_values(vec![
                    Value::String("数据".repeat(300)),
                    Value::Null,
                    Value::String("x".repeat(4096)),
                ])],
            );
            let spool = Spool::new(dir.path()).unwrap();
            spool.store(7, &batch).unwrap();
            let decoded = spool.read(7, schema).unwrap();
            for row in 0..3 {
                assert_eq!(decoded.row(row), batch.row(row));
            }
            let path = spool.directory.path().to_path_buf();
            drop(decoded);
            drop(spool);
            assert!(!path.exists());
        });
        assert_eq!(budget.used(), 0);
        assert!(budget.peak_used() <= budget.limit());
    }
}
