//! Bounded worker candidate spools and the final stable external reduction.

use super::*;
use crate::engine::executor::mem_budget;
use crate::engine::executor::row_spool::{RowSpoolBatches, RowSpoolBuilder};
use crate::engine::executor::vec::batch::Batch;
use crate::engine::executor::vec::schema::{BatchField, BatchSchema};
use crate::engine::executor::vec::sort::{ExternalSortBatchOperator, SortKeySource};
use std::path::Path;

pub(super) struct WorkerSpill {
    builder: RowSpoolBuilder,
}

impl WorkerSpill {
    pub(super) fn new(data_dir: &Path, columns: usize) -> Result<Self, QueryError> {
        Ok(Self {
            builder: RowSpoolBuilder::new(data_dir, columns + 1)?,
        })
    }

    pub(super) fn append(
        &mut self,
        values: Vec<Value>,
        ordinal: u64,
        charge: ScopedCharge,
    ) -> Result<(), QueryError> {
        let mut owned = crate::engine::executor::row_spool::OwnedRow { values, charge };
        mem_budget::try_reserve_vec_hard(
            &mut owned.values,
            1,
            &mut owned.charge,
            "parallel TopN ordinal",
        )?;
        owned.values.push(Value::UInt(ordinal));
        self.builder.append(&owned.values)?;
        drop(owned);
        Ok(())
    }

    pub(super) fn drain(&mut self, heap: &mut VectorTopK) -> Result<(), QueryError> {
        // Retain the old backing array and each candidate owner until its
        // corresponding record has been encoded. No payload is re-evaluated.
        let empty = VectorTopK::new_unreserved(heap.fields_for_spill(), 0, heap.memory_charge());
        for candidate in std::mem::replace(heap, empty).drain_sorted() {
            let (keys, values, ordinal, charge) = candidate.into_parts();
            drop(keys);
            self.append(values, ordinal, charge)?;
        }
        Ok(())
    }
}

pub(crate) enum ParallelTopNOutput {
    Resident(Vec<Vec<Value>>, ScopedCharge),
    External(Box<dyn BatchOperator>),
}

struct CandidateInputs {
    spools: std::collections::VecDeque<Arc<crate::engine::executor::row_spool::RowSpool>>,
    current: Option<RowSpoolBatches>,
    schema: Arc<BatchSchema>,
}

impl BatchOperator for CandidateInputs {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        loop {
            if let Some(current) = &mut self.current {
                if let Some(batch) = current.next_batch()? {
                    return Ok(Some(batch));
                }
                self.current = None;
            }
            let Some(spool) = self.spools.pop_front() else {
                return Ok(None);
            };
            self.current = Some(RowSpoolBatches::new(&spool, self.schema.clone())?);
        }
    }
}

pub(super) fn finish(
    mut sinks: Vec<TopKSink>,
    keys: Vec<SortKey>,
    fields: Arc<[SortField]>,
    charge: ScopedCharge,
    k: usize,
    offset: usize,
    schema: Arc<BatchSchema>,
    data_dir: Option<&Path>,
) -> Result<ParallelTopNOutput, QueryError> {
    if !sinks.iter().any(|sink| sink.spill.is_some()) {
        let mut master = VectorTopK::new_unreserved(fields, k, charge);
        for sink in sinks {
            master.merge_from(sink.topk)?;
        }
        let (rows, charge) = master.into_sorted_vec_charged_strict()?;
        return Ok(ParallelTopNOutput::Resident(rows, charge));
    }
    let data_dir = data_dir.expect("spilled workers have a data directory");
    let mut spools = std::collections::VecDeque::new();
    for sink in &mut sinks {
        if sink.spill.is_none() {
            sink.spill = Some(WorkerSpill::new(data_dir, schema.len())?);
            sink.spill.as_mut().unwrap().drain(&mut sink.topk)?;
        }
        spools.push_back(sink.spill.take().unwrap().builder.finish()?);
    }
    drop(sinks);
    drop(charge);
    let mut tagged_fields = schema.fields().to_vec();
    tagged_fields.push(BatchField {
        table: "__topn".into(),
        alias: None,
        name: "ordinal".into(),
        column_type: crate::types::ColumnType::LongLong,
        source: Default::default(),
    });
    let input = CandidateInputs {
        spools,
        current: None,
        schema: Arc::new(BatchSchema::new(tagged_fields)),
    };
    let mut sources = Vec::new();
    let mut sort_fields = Vec::new();
    for key in keys {
        sources.push(SortKeySource::Column(key.column));
        sort_fields.push(SortField {
            ascending: key.ascending,
            nulls_first: key.nulls_first,
            collation_id: key.collation_id,
        });
    }
    let ordinal_column = schema.len();
    Ok(ParallelTopNOutput::External(Box::new(
        ExternalSortBatchOperator::new(
            None,
            Box::new(input),
            schema,
            Default::default(),
            sources,
            sort_fields,
            data_dir,
        )
        .with_topn_candidates(k, offset)
        .with_payload_ties(ordinal_column),
    )))
}

pub(super) fn copy_payload(batch: &Batch, row: usize) -> Result<Vec<Value>, QueryError> {
    let mut values =
        mem_budget::try_vec_with_capacity(batch.columns().len(), "parallel TopN payload")?;
    for column in batch.columns() {
        values.push(column.try_value_at(row)?);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use crate::engine::executor::vec::batch::{ColumnVector, Selection};
    use crate::engine::executor::vec::stream::MaterializedBatchOperator;
    use crate::types::ColumnType;

    struct QuerySpillLimit(u64);
    impl QuerySpillLimit {
        fn set(bytes: u64) -> Self {
            let scope = Self(crate::engine::executor::spill_quota::per_query_limit_bytes());
            crate::engine::executor::spill_quota::set_per_query_limit_bytes(bytes);
            scope
        }
    }
    impl Drop for QuerySpillLimit {
        fn drop(&mut self) {
            crate::engine::executor::spill_quota::set_per_query_limit_bytes(self.0);
        }
    }

    fn fixture() -> MaterializedBatchOperator {
        let fields = [
            ColumnType::VarString,
            ColumnType::Blob,
            ColumnType::VarString,
        ]
        .into_iter()
        .enumerate()
        .map(|(n, column_type)| BatchField {
            table: "t".into(),
            alias: None,
            name: format!("c{n}"),
            column_type,
            source: Default::default(),
        })
        .collect();
        let schema = Arc::new(BatchSchema::new(fields));
        let batches = (0..32)
            .map(|chunk| {
                let mut columns = vec![Vec::new(), Vec::new(), Vec::new()];
                for row in 0..32 {
                    let n = chunk * 32 + row;
                    columns[0].push(match n % 4 {
                        0 => Value::Null,
                        1 => Value::String("A ".into()),
                        2 => Value::String("a".into()),
                        _ => Value::String("B".into()),
                    });
                    columns[1].push(match n % 9 {
                        0 => Value::Null,
                        1 => Value::Int(-7),
                        2 => Value::UInt(u64::MAX),
                        3 => Value::Double(1.5),
                        4 => Value::DecimalCompact(175, 2),
                        5 => Value::String("a ".into()),
                        6 => Value::Bytes(vec![0, 255]),
                        7 => Value::Time(-123),
                        _ => Value::Float(-0.0),
                    });
                    columns[2].push(Value::String(format!("{n:08}{}", "x".repeat(4096))));
                }
                Batch::new(
                    schema.clone(),
                    columns
                        .into_iter()
                        .map(|values| ColumnVector::Generic { values })
                        .collect(),
                )
                .with_selection(Selection::Indices(vec![
                    17, 3, 8, 31, 0, 3, 25, 7, 11, 9, 12, 22, 16, 2, 13, 5,
                ]))
            })
            .collect();
        MaterializedBatchOperator::new(schema, batches)
    }

    fn keys() -> Vec<SortKey> {
        vec![SortKey {
            column: 0,
            ascending: false,
            nulls_first: false,
            collation_id: Some(33),
        }]
    }

    #[test]
    fn parallel_topn_spill_preserves_collation_payload_ties_and_selection_occurrences() {
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let expected = super::super::run_topn_parallel(&mut fixture(), keys(), 233, 4).unwrap();
        for workers in [1, 2, 4] {
            let budget = Arc::new(MemBudget::new(256 * 1024));
            with_query_budget(budget.clone(), || {
                let ParallelTopNOutput::External(mut output) =
                    super::super::run_topn_parallel_spillable(
                        &mut fixture(),
                        keys(),
                        233,
                        workers,
                        root.path(),
                        13,
                    )
                    .unwrap()
                else {
                    panic!("wide candidates must externalize");
                };
                let mut observed = 13;
                while let Some(batch) = output.next_batch().unwrap() {
                    for row in batch.visible_indices() {
                        let values = batch
                            .columns()
                            .iter()
                            .map(|column| column.value_at(row))
                            .collect::<Vec<_>>();
                        assert_eq!(
                            values, expected[observed],
                            "workers={workers}, row={observed}"
                        );
                        observed += 1;
                    }
                }
                assert_eq!(observed, 233);
            });
            assert_eq!(budget.used(), 0);
            assert_eq!(budget.spill_bytes(), 0);
            assert!(budget.peak_used() < 4 * 1024 * 1024);
        }
    }

    #[test]
    fn parallel_topn_abort_releases_worker_spools_and_merge_buffers() {
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let budget = Arc::new(MemBudget::new(256 * 1024));
        with_query_budget(budget.clone(), || {
            let ParallelTopNOutput::External(mut output) =
                super::super::run_topn_parallel_spillable(
                    &mut fixture(),
                    keys(),
                    233,
                    4,
                    root.path(),
                    0,
                )
                .unwrap()
            else {
                panic!("wide candidates must externalize");
            };
            let batch = output.next_batch().unwrap().unwrap();
            assert!(budget.spill_bytes() > 0);
            drop(batch);
            drop(output);
        });
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.spill_bytes(), 0);
        let tmp = crate::storage::naming::system_tmp_dir(root.path());
        assert!(!tmp.exists() || std::fs::read_dir(tmp).unwrap().next().is_none());
    }

    #[test]
    fn parallel_topn_quota_failure_keeps_the_original_query_owner_and_cleans_up() {
        let _restore = QuerySpillLimit::set(8192);
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let budget = Arc::new(MemBudget::new(256 * 1024));
        with_query_budget(budget.clone(), || {
            let error = match super::super::run_topn_parallel_spillable(
                &mut fixture(),
                keys(),
                233,
                4,
                root.path(),
                0,
            ) {
                Err(error) => error,
                Ok(_) => panic!("worker spill must enforce its query's quota"),
            };
            assert_eq!(error.code(), 1041);
        });
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.spill_bytes(), 0);
    }

    #[test]
    fn parallel_topn_payload_ties_do_not_duplicate_wide_payloads_as_sort_keys() {
        let _restore = QuerySpillLimit::set(5 * 1024 * 1024);
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let budget = Arc::new(MemBudget::new(256 * 1024));
        with_query_budget(budget.clone(), || {
            let ParallelTopNOutput::External(mut output) =
                super::super::run_topn_parallel_spillable(
                    &mut fixture(),
                    keys(),
                    233,
                    4,
                    root.path(),
                    0,
                )
                .unwrap()
            else {
                panic!("fixture must exercise candidate spill");
            };
            let mut rows = 0;
            while let Some(batch) = output.next_batch().unwrap() {
                rows += batch.visible_rows();
            }
            assert_eq!(rows, 233);
        });
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.spill_bytes(), 0);
    }

    #[test]
    fn parallel_topn_uses_available_shared_headroom_before_spilling() {
        let root = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let budget = Arc::new(MemBudget::new(8 * 1024 * 1024));
        with_query_budget(budget.clone(), || {
            let ParallelTopNOutput::Resident(rows, charge) =
                super::super::run_topn_parallel_spillable(
                    &mut fixture(),
                    keys(),
                    512,
                    4,
                    root.path(),
                    0,
                )
                .unwrap()
            else {
                panic!("candidate state and bounded queues fit the available query grant");
            };
            assert_eq!(rows.len(), 512);
            drop(rows);
            drop(charge);
        });
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.spill_bytes(), 0);
        assert!(budget.peak_used() < budget.limit());
    }
}
