//! Bounded FIFO for join fanout. Producers finish a probe batch before the
//! consumer resumes; overflow therefore belongs on quota-owned disk.
use super::{Batch, BatchSchema};
use crate::engine::executor::{
    mem_budget,
    select::grace::SpillTmpDir,
    spill_quota::{TrackedSpillReader, TrackedSpillWriter},
};
use crate::types::QueryError;
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::Arc,
};

pub(crate) struct JoinOutputQueue {
    memory: VecDeque<Batch>,
    charge: mem_budget::ScopedCharge,
    cap: usize,
    data_dir: Option<PathBuf>,
    schema: Option<Arc<BatchSchema>>,
    writer: Option<TrackedSpillWriter>,
    reader: Option<TrackedSpillReader>,
    dir: Option<SpillTmpDir>,
    disk_batches: usize,
}

impl JoinOutputQueue {
    pub(crate) fn new() -> Self {
        Self {
            memory: VecDeque::new(),
            charge: mem_budget::ScopedCharge::new(0),
            // Normal probe morsels can emit several batches. Let them share
            // a bounded fraction of the query grant; every resident byte is
            // still charged, and large fanout or quota pressure still spills.
            cap: mem_budget::current_budget()
                .map_or(1024 * 1024, |b| (b.limit() / 16).clamp(1, 16 * 1024 * 1024)),
            data_dir: None,
            schema: None,
            writer: None,
            reader: None,
            dir: None,
            disk_batches: 0,
        }
    }
    pub(crate) fn set_data_dir(&mut self, path: &Path) {
        self.data_dir = Some(path.to_path_buf());
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.memory.is_empty() && self.disk_batches == 0
    }
    pub(crate) fn push_back(&mut self, batch: Batch) -> Result<(), QueryError> {
        crate::engine::cancel::check_cancelled()?;
        let bytes = batch.memory_bytes();
        // Keep a single governed batch in memory even when it exceeds the
        // soft high-water mark. Unique-key joins should not write each normal
        // output batch merely because its payload is wider than the target.
        if self.dir.is_none()
            && (self.memory.is_empty() || self.charge.charged().saturating_add(bytes) <= self.cap)
        {
            if self.charge.add(bytes).is_ok() {
                self.memory.push_back(batch);
                return Ok(());
            }
        }
        if self.dir.is_none() {
            let path = self.data_dir.as_ref().ok_or_else(|| {
                mem_budget::allocation_error("hash join output requires spill storage")
            })?;
            let dir = SpillTmpDir::create_kind(path, "vec-join-output")?;
            self.writer = Some(TrackedSpillWriter::create_new(
                &dir.path().join("output"),
                dir.charger(),
                32 * 1024,
            )?);
            self.schema = Some(batch.schema().clone());
            self.dir = Some(dir);
            while let Some(buffered) = self.memory.pop_front() {
                self.write(&buffered)?;
                self.charge.release_bytes(buffered.memory_bytes());
            }
        }
        self.write(&batch)
    }
    fn write(&mut self, batch: &Batch) -> Result<(), QueryError> {
        // Selection is not part of the spill format.
        if batch.selection().is_some() {
            let indices: Vec<u32> = batch.visible_indices().map(|i| i as u32).collect();
            let compact = Batch::with_row_count(
                batch.schema().clone(),
                batch
                    .columns()
                    .iter()
                    .map(|c| c.take_indices(&indices))
                    .collect(),
                indices.len(),
            );
            super::spill::write_batch(
                self.writer.as_mut().expect("producer precedes consumer"),
                &compact,
            )?;
        } else {
            super::spill::write_batch(
                self.writer.as_mut().expect("producer precedes consumer"),
                batch,
            )?;
        }
        self.disk_batches += 1;
        Ok(())
    }
    pub(crate) fn pop_front(&mut self) -> Result<Option<Batch>, QueryError> {
        crate::engine::cancel::check_cancelled()?;
        if let Some(batch) = self.memory.pop_front() {
            self.charge.release_bytes(batch.memory_bytes());
            return Ok(Some(batch));
        }
        if self.disk_batches == 0 {
            return Ok(None);
        }
        if let Some(writer) = self.writer.take() {
            // QuotaFile accounts each write, including the final flush.
            writer.finish()?;
            let dir = self.dir.as_ref().unwrap();
            self.reader = Some(TrackedSpillReader::open(
                &dir.path().join("output"),
                dir.charger(),
                32 * 1024,
            )?);
        }
        let batch = super::spill::read_owned_batch(
            self.reader.as_mut().unwrap(),
            self.schema.as_ref().unwrap().clone(),
        )?
        .ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                "truncated hash join output",
            )
        })?;
        self.disk_batches -= 1;
        if self.disk_batches == 0 {
            self.reader = None;
            self.dir = None;
            self.schema = None;
        }
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::vec::{BatchField, ColumnVector, Selection};
    use crate::types::{ColumnType, Value};

    fn schema() -> Arc<BatchSchema> {
        Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "v".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }]))
    }
    fn root() -> tempfile::TempDir {
        std::fs::create_dir_all("/tmp/mydb/join-fix").unwrap();
        tempfile::tempdir_in("/tmp/mydb/join-fix").unwrap()
    }
    #[test]
    fn join_output_uses_query_grant_for_normal_multi_batch_output() {
        let root = root();
        let budget = Arc::new(mem_budget::MemBudget::new(128 * 1024 * 1024));
        mem_budget::with_query_budget(budget.clone(), || {
            let mut queue = JoinOutputQueue::new();
            queue.set_data_dir(root.path());
            for _ in 0..4 {
                queue
                    .push_back(Batch::new(
                        schema(),
                        vec![ColumnVector::from_values(vec![Value::Int(1); 65_536])],
                    ))
                    .unwrap();
            }
            assert!(
                queue.dir.is_none(),
                "ordinary multi-batch output fits its query grant"
            );
            for _ in 0..4 {
                assert_eq!(queue.pop_front().unwrap().unwrap().num_rows(), 65_536);
            }
            assert!(queue.pop_front().unwrap().is_none());
            assert_eq!(budget.used(), 0);
        });
    }

    #[test]
    fn join_output_spill_preserves_selection_order_and_releases_resources() {
        let root = root();
        let budget = Arc::new(mem_budget::MemBudget::new(1024 * 1024));
        mem_budget::with_query_budget(budget.clone(), || {
            let mut queue = JoinOutputQueue::new();
            queue.set_data_dir(root.path());
            queue.cap = 128;
            for round in 0..2 {
                for i in 0..100 {
                    queue
                        .push_back(
                            Batch::new(
                                schema(),
                                vec![ColumnVector::from_values(vec![
                                    Value::Int(i),
                                    Value::Null,
                                    Value::Int(-1),
                                ])],
                            )
                            .with_selection(Selection::Indices(vec![1, 0, 0])),
                        )
                        .unwrap();
                    assert!(queue.charge.charged() <= 128 || queue.memory.len() == 1);
                }
                assert!(queue.dir.is_some());
                for i in 0..100 {
                    let batch = queue.pop_front().unwrap().unwrap();
                    assert_eq!(batch.num_rows(), 3, "round {round}");
                    assert_eq!(batch.row(0), vec![Value::Null]);
                    assert_eq!(batch.row(1), vec![Value::Int(i)]);
                    assert_eq!(batch.row(2), vec![Value::Int(i)]);
                }
                assert!(queue.pop_front().unwrap().is_none());
                assert!(queue.dir.is_none());
                assert_eq!(budget.used(), 0);
                assert_eq!(budget.spill_bytes(), 0);
            }
        });
    }
    #[test]
    fn join_output_cancelled_consumer_releases_spill() {
        let root = root();
        let budget = Arc::new(mem_budget::MemBudget::new(1024 * 1024));
        mem_budget::with_query_budget(budget.clone(), || {
            let token = crate::engine::cancel::QueryCancelToken::noop().fork_local();
            let _scope = crate::engine::cancel::enter(token.clone());
            let mut queue = JoinOutputQueue::new();
            queue.set_data_dir(root.path());
            queue.cap = 0;
            for _ in 0..3 {
                queue
                    .push_back(Batch::new(
                        schema(),
                        vec![ColumnVector::from_values(vec![Value::Int(1)])],
                    ))
                    .unwrap();
            }
            let path = queue.dir.as_ref().unwrap().path().to_path_buf();
            token.cancel_local();
            assert_eq!(queue.pop_front().unwrap_err().code(), 1317);
            drop(queue);
            assert!(!path.exists());
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.spill_bytes(), 0);
        });
    }

    #[test]
    fn join_output_early_drop_removes_spill_and_charges() {
        let root = root();
        let budget = Arc::new(mem_budget::MemBudget::new(1024 * 1024));
        mem_budget::with_query_budget(budget.clone(), || {
            let path;
            {
                let mut queue = JoinOutputQueue::new();
                queue.set_data_dir(root.path());
                queue.cap = 0;
                for _ in 0..3 {
                    queue
                        .push_back(Batch::new(
                            schema(),
                            vec![ColumnVector::from_values(vec![Value::Int(1)])],
                        ))
                        .unwrap();
                }
                path = queue.dir.as_ref().unwrap().path().to_path_buf();
                queue.pop_front().unwrap();
            }
            assert!(!path.exists());
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.spill_bytes(), 0);
        });
    }
}
