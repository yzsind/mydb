//! Morsel-driven parallel Top-K (ORDER BY ... LIMIT K).
//!
//! Each worker owns a bounded max-heap of capacity K. A bounded producer queue
//! hands batches from the pull pipeline to worker-local heaps. Once the worker
//! pool drains, the main thread reduces at most `workers * K` candidates into
//! the global Top-K.

use std::sync::Arc;

use crate::engine::executor::vec::stream::BatchOperator;
use crate::types::{QueryError, Value};

use super::{Morsel, Sink};
use crate::engine::executor::mem_budget::ScopedCharge;
#[cfg(test)]
use crate::engine::executor::memory_footprint::AllocatedBytes;
use crate::engine::executor::sort::{BoundedTopK, PayloadThenOrdinal, SortField};

mod spill;
pub(crate) use spill::ParallelTopNOutput;

/// Sort-key direction per ORDER BY field.
#[derive(Debug, Clone, Copy)]
pub struct SortKey {
    pub column: usize,
    pub ascending: bool,
    pub nulls_first: bool,
    pub collation_id: Option<u16>,
}

type VectorTopK = BoundedTopK<Vec<Value>, PayloadThenOrdinal>;

struct TopKSink {
    topk: VectorTopK,
    keys: Vec<SortKey>,
    data_dir: Option<Arc<std::path::PathBuf>>,
    candidate_limit: usize,
    spill: Option<spill::WorkerSpill>,
}

impl Sink for TopKSink {
    fn consume(&mut self, morsel: Morsel) -> Result<(), QueryError> {
        let mut base_row = morsel.base_row;
        for batch in &morsel.batches {
            crate::engine::cancel::check_cancelled()?;
            crate::engine::executor::select::eval::record_order_by_key_rows_prepared(
                batch.visible_rows(),
            );
            for r in batch.visible_indices() {
                let seq = base_row.checked_add(r as u64).ok_or_else(|| {
                    crate::error::catalog::ER_UNKNOWN_ERROR.error("parallel TopN ordinal overflow")
                })?;
                if let Some(data_dir) = &self.data_dir {
                    // Worker shares partition one candidate budget; neither
                    // K nor worker count can multiply the allowed footprint.
                    let payload = batch.columns().iter().fold(0usize, |n, column| {
                        n.saturating_add(column.owned_value_bytes(r))
                    });
                    let key_bytes = self.keys.iter().fold(0usize, |n, key| {
                        n.saturating_add(batch.columns()[key.column].owned_value_bytes(r))
                    });
                    let predicted = payload.saturating_add(key_bytes).saturating_add(128);
                    let mut pending = self.topk.memory_charge();
                    if self.spill.is_none()
                        && (self.topk.resident_bytes().saturating_add(predicted)
                            > self.candidate_limit
                            || pending.add(predicted).is_err())
                    {
                        drop(pending);
                        self.spill =
                            Some(spill::WorkerSpill::new(data_dir, batch.columns().len())?);
                        self.spill.as_mut().unwrap().drain(&mut self.topk)?;
                        pending = self.topk.memory_charge();
                    }
                    if let Some(spill) = &mut self.spill {
                        pending.add_force(payload.saturating_add(128))?;
                        let values = spill::copy_payload(batch, r)?;
                        spill.append(values, seq, pending)?;
                    } else {
                        let mut keys = crate::engine::executor::mem_budget::try_vec_with_capacity(
                            self.keys.len(),
                            "parallel TopN keys",
                        )?;
                        for key in &self.keys {
                            keys.push(batch.columns()[key.column].try_value_at(r)?);
                        }
                        self.topk.push_reserved_lazy_at(keys, seq, pending, || {
                            spill::copy_payload(batch, r)
                        })?;
                    }
                    continue;
                }
                let keys: Vec<Value> = self
                    .keys
                    .iter()
                    .map(|k| batch.columns()[k.column].value_at(r))
                    .collect();
                // Global scan-order index: base of this batch + physical
                // row index (visible rows preserve ascending `r`).
                self.topk.push_lazy_at(keys, seq, || {
                    (0..batch.columns().len())
                        .map(|c| batch.columns()[c].value_at(r))
                        .collect()
                })?;
            }
            base_row = base_row
                .checked_add(batch.num_rows() as u64)
                .ok_or_else(|| {
                    crate::error::catalog::ER_UNKNOWN_ERROR.error("parallel TopN ordinal overflow")
                })?;
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<(), QueryError> {
        Ok(())
    }
}

/// Queue ownership includes the active batch, not configured empty slots.
/// These bounded buffers cannot spill; the ordinary heap retains its strict
/// query-budget admission and the process governor still bounds both owners.
fn charge_batch(
    batch: &crate::engine::executor::vec::batch::Batch,
) -> Result<ScopedCharge, QueryError> {
    use crate::engine::executor::vec::batch::Selection;
    let selection_bytes = match batch.selection() {
        Some(Selection::Indices(indices)) => indices
            .capacity()
            .saturating_mul(std::mem::size_of::<u32>()),
        Some(Selection::Mask(mask)) => mask.memory_bytes(),
        _ => 0,
    };
    let bytes = batch
        .memory_bytes()
        .saturating_add(batch.columns.capacity().saturating_mul(std::mem::size_of::<
            crate::engine::executor::vec::batch::ColumnVector,
        >()))
        .saturating_add(selection_bytes)
        .saturating_add(std::mem::size_of::<Morsel>())
        .saturating_add(std::mem::size_of::<
            crate::engine::executor::vec::batch::Batch,
        >());
    let mut charge = ScopedCharge::new(0);
    charge.add_force(bytes)?;
    Ok(charge)
}

/// Bounded producer/consumer Top-K with per-worker heaps and a final
/// coordinator reduction. The caller keeps pulling its ordinary batch stream
/// while fixed worker queues evaluate ORDER BY keys. At most one queued and
/// one active batch per worker are retained, so input size does not affect
/// retained memory.
pub fn run_topn_parallel(
    input: &mut dyn BatchOperator,
    keys: Vec<SortKey>,
    k: usize,
    worker_count: usize,
) -> Result<Vec<Vec<Value>>, QueryError> {
    run_topn_parallel_charged(input, keys, k, worker_count).map(|(rows, _charge)| rows)
}

pub(crate) fn run_topn_parallel_charged(
    input: &mut dyn BatchOperator,
    keys: Vec<SortKey>,
    k: usize,
    worker_count: usize,
) -> Result<(Vec<Vec<Value>>, ScopedCharge), QueryError> {
    match run_topn_parallel_impl(input, keys, k, worker_count, None, 0)? {
        ParallelTopNOutput::Resident(rows, charge) => Ok((rows, charge)),
        ParallelTopNOutput::External(_) => {
            unreachable!("legacy test adapter has no spill directory")
        }
    }
}

pub(crate) fn run_topn_parallel_spillable(
    input: &mut dyn BatchOperator,
    keys: Vec<SortKey>,
    k: usize,
    worker_count: usize,
    data_dir: &std::path::Path,
    offset: usize,
) -> Result<ParallelTopNOutput, QueryError> {
    run_topn_parallel_impl(
        input,
        keys,
        k,
        worker_count,
        Some(Arc::new(data_dir.to_path_buf())),
        offset,
    )
}

fn run_topn_parallel_impl(
    input: &mut dyn BatchOperator,
    keys: Vec<SortKey>,
    k: usize,
    worker_count: usize,
    data_dir: Option<Arc<std::path::PathBuf>>,
    offset: usize,
) -> Result<ParallelTopNOutput, QueryError> {
    let schema = input.schema().clone();
    let candidate_limit = crate::engine::executor::mem_budget::current_budget()
        .map_or(64 * 1024 * 1024, |budget| budget.limit() / 2)
        / worker_count.max(1);
    let mut charge = ScopedCharge::new(0);
    if k == 0 {
        return Ok(ParallelTopNOutput::Resident(Vec::new(), charge));
    }

    charge.add(
        keys.len()
            .saturating_mul(worker_count.max(1).saturating_add(2))
            .saturating_mul(std::mem::size_of::<SortKey>()),
    )?;
    let fields: Arc<[SortField]> = keys
        .iter()
        .map(|key| SortField {
            ascending: key.ascending,
            nulls_first: key.nulls_first,
            collation_id: key.collation_id,
        })
        .collect::<Vec<_>>()
        .into();

    if worker_count <= 1 {
        let mut sink = TopKSink {
            topk: VectorTopK::new_unreserved(fields.clone(), k, charge.split(0)),
            keys: keys.clone(),
            data_dir: data_dir.clone(),
            candidate_limit,
            spill: None,
        };
        let mut next_base = 0u64;
        while let Some(batch) = input.next_batch()? {
            let rows = batch.num_rows() as u64;
            let _batch_charge = charge_batch(&batch)?;
            sink.consume(Morsel {
                batches: vec![batch],
                base_row: next_base,
            })?;
            next_base = next_base.saturating_add(rows);
        }
        return spill::finish(
            vec![sink],
            keys,
            fields,
            charge,
            k,
            offset,
            schema,
            data_dir.as_deref().map(std::path::PathBuf::as_path),
        );
    }

    // Avoid creating a second worker pool for an empty or single-batch input.
    // Two batches are also the bounded seed for the parallel producer below.
    let Some(first) = input.next_batch()? else {
        return Ok(ParallelTopNOutput::Resident(Vec::new(), charge));
    };
    let first_charge = charge_batch(&first)?;
    let Some(second) = input.next_batch()? else {
        let mut sink = TopKSink {
            topk: VectorTopK::new_unreserved(fields.clone(), k, charge.split(0)),
            keys: keys.clone(),
            data_dir: data_dir.clone(),
            candidate_limit,
            spill: None,
        };
        sink.consume(Morsel {
            batches: vec![first],
            base_row: 0,
        })?;
        drop(first_charge);
        return spill::finish(
            vec![sink],
            keys,
            fields,
            charge,
            k,
            offset,
            schema,
            data_dir.as_deref().map(std::path::PathBuf::as_path),
        );
    };
    let second_charge = charge_batch(&second)?;
    let mut prefetched =
        std::collections::VecDeque::from([(first, first_charge), (second, second_charge)]);

    let worker_count = worker_count.max(1);
    let cancellation = crate::engine::cancel::CancellationSnapshot::capture().fork_local();
    let session_info = crate::engine::expr::current_session_expr_info();
    let budget = crate::engine::executor::mem_budget::current_budget();
    let env_snapshot = crate::util::env::override_snapshot();
    let mut producer_error = None;
    let sinks = std::thread::scope(|scope| {
        let mut senders = Vec::with_capacity(worker_count);
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            // One queued batch overlaps scan and comparison while one batch
            // is active, keeping the producer/consumer boundary bounded.
            let (sender, receiver) = std::sync::mpsc::sync_channel::<(Morsel, ScopedCharge)>(1);
            senders.push(sender);
            let worker_charge = charge.split(0);
            let worker_keys = keys.clone();
            let worker_fields = fields.clone();
            let worker_cancel = cancellation.clone();
            let session_info = &session_info;
            let worker_data_dir = data_dir.clone();
            let worker_budget = budget.clone();
            let worker_env = env_snapshot.clone();
            handles.push(scope.spawn(move || -> Result<TopKSink, QueryError> {
                let _cancel_guard = worker_cancel.enter();
                let run = || {
                    crate::util::env::with_override_snapshot(worker_env, || {
                        crate::engine::expr::with_session_expr_info_option(session_info, || {
                            let mut sink = TopKSink {
                                topk: VectorTopK::new_unreserved(worker_fields, k, worker_charge),
                                keys: worker_keys,
                                data_dir: worker_data_dir,
                                candidate_limit,
                                spill: None,
                            };
                            while let Ok((morsel, _batch_charge)) = receiver.recv() {
                                crate::engine::cancel::check_cancelled()?;
                                sink.consume(morsel)?;
                            }
                            sink.finish()?;
                            Ok(sink)
                        })
                    })
                };
                match worker_budget {
                    Some(budget) => {
                        crate::engine::executor::mem_budget::with_query_budget(budget, run)
                    }
                    None => run(),
                }
            }));
        }

        let mut next_worker = 0usize;
        let mut next_base = 0u64;
        loop {
            let (batch, batch_charge) = match prefetched.pop_front() {
                Some(batch) => batch,
                None => match input.next_batch() {
                    Ok(Some(batch)) => match charge_batch(&batch) {
                        Ok(charge) => (batch, charge),
                        Err(error) => {
                            producer_error = Some(error);
                            break;
                        }
                    },
                    Ok(None) => break,
                    Err(error) => {
                        producer_error = Some(error);
                        break;
                    }
                },
            };
            let rows = batch.num_rows() as u64;
            if senders[next_worker]
                .send((
                    Morsel {
                        batches: vec![batch],
                        base_row: next_base,
                    },
                    batch_charge,
                ))
                .is_err()
            {
                break;
            }
            next_base = next_base.saturating_add(rows);
            next_worker = (next_worker + 1) % worker_count;
        }
        drop(senders);

        handles
            .into_iter()
            .map(|handle| match handle.join() {
                Ok(result) => result,
                Err(_) => Err(crate::storage::error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    "parallel Top-K worker panicked",
                )),
            })
            .collect::<Result<Vec<_>, QueryError>>()
    })?;
    if let Some(error) = producer_error {
        return Err(error);
    }

    spill::finish(
        sinks,
        keys,
        fields,
        charge,
        k,
        offset,
        schema,
        data_dir.as_deref().map(std::path::PathBuf::as_path),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::vec::batch::{Batch, ColumnVector};
    use crate::engine::executor::vec::schema::{BatchField, BatchSchema};
    use crate::engine::executor::vec::stream::MaterializedBatchOperator;
    use crate::types::ColumnType;

    fn schema_iv() -> Arc<BatchSchema> {
        Arc::new(BatchSchema::new(vec![
            BatchField {
                table: "t".into(),
                alias: None,
                name: "id".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
            BatchField {
                table: "t".into(),
                alias: None,
                name: "v".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            },
        ]))
    }

    fn make_batch(s: &Arc<BatchSchema>, rows: &[(i64, i64)]) -> Batch {
        let ids: Vec<Value> = rows.iter().map(|(i, _)| Value::Int(*i)).collect();
        let vs: Vec<Value> = rows.iter().map(|(_, v)| Value::Int(*v)).collect();
        Batch::new(
            s.clone(),
            vec![
                ColumnVector::from_values(ids),
                ColumnVector::from_values(vs),
            ],
        )
    }

    #[test]
    fn parallel_topk_budget_survives_worker_handoff() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        let budget = Arc::new(MemBudget::new(64 * 1024));
        with_query_budget(budget.clone(), || {
            let schema = Arc::new(BatchSchema::new(vec![BatchField {
                table: "t".into(),
                alias: None,
                name: "v".into(),
                column_type: ColumnType::LongLong,
                source: Default::default(),
            }]));
            let batches = (0..8)
                .map(|i| {
                    Batch::new(
                        schema.clone(),
                        vec![ColumnVector::from_values(vec![Value::Int(8 - i)])],
                    )
                })
                .collect();
            let mut input = MaterializedBatchOperator::new(schema, batches);
            let keys = vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: true,
                collation_id: None,
            }];
            let (rows, charge) = run_topn_parallel_charged(&mut input, keys, 1000000, 3).unwrap();
            assert_eq!(rows.len(), 8);
            assert_eq!(rows[0], vec![Value::Int(1)]);
            assert_eq!(budget.used(), rows.allocated_bytes());
            drop(rows);
            drop(charge);
        });
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn topn_descending_matches_serial() {
        // Produce ~100 rows across 5 batches, then take top-5 ordered
        // by v desc. Compare parallel run against an explicit
        // sort+truncate oracle.
        let s = schema_iv();
        let batches: Vec<Batch> = (0..5)
            .map(|chunk| {
                let rows: Vec<(i64, i64)> = (0..20)
                    .map(|i| (chunk * 20 + i, (chunk * 20 + i) * 7 % 97))
                    .collect();
                make_batch(&s, &rows)
            })
            .collect();

        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        for b in &batches {
            for r in 0..b.num_rows() {
                all_rows.push(
                    (0..b.columns().len())
                        .map(|c| b.columns()[c].value_at(r))
                        .collect(),
                );
            }
        }
        all_rows.sort_by_key(|r| {
            let v = match &r[1] {
                Value::Int(x) => *x,
                _ => 0,
            };
            -v
        });
        let oracle: Vec<Vec<Value>> = all_rows.into_iter().take(5).collect();

        let mut stream = MaterializedBatchOperator::new(s, batches);
        let got = run_topn_parallel(
            &mut stream,
            vec![SortKey {
                column: 1,
                ascending: false,
                nulls_first: false,
                collation_id: None,
            }],
            5,
            4,
        )
        .unwrap();

        // Same row set (sorted) — strict order checked via the keys.
        let key_of = |r: &Vec<Value>| match &r[1] {
            Value::Int(x) => *x,
            _ => 0,
        };
        let got_keys: Vec<i64> = got.iter().map(key_of).collect();
        let oracle_keys: Vec<i64> = oracle.iter().map(key_of).collect();
        assert_eq!(
            got_keys, oracle_keys,
            "got rows: {got:?} oracle: {oracle:?}"
        );
    }

    #[test]
    fn streaming_parallel_unicode_collation_matches_serial_topn() {
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "s".into(),
            column_type: ColumnType::VarString,
            source: crate::types::ColumnSource {
                collation_id: Some(255),
                ..Default::default()
            },
        }]));
        let values = [
            "Журнал",
            "éclair",
            "ECLAIR",
            "中文",
            "ångström",
            "apple",
            " Берег",
            "zebra",
            "Örebro",
        ];
        let batches = values
            .chunks(3)
            .map(|chunk| {
                Batch::new(
                    schema.clone(),
                    vec![ColumnVector::from_values(
                        chunk
                            .iter()
                            .map(|value| Value::String((*value).into()))
                            .collect(),
                    )],
                )
            })
            .collect::<Vec<_>>();

        let mut serial = MaterializedBatchOperator::new(schema.clone(), batches.clone());
        let expected = crate::engine::executor::vec::topn::run_topn(
            &mut serial,
            &[crate::engine::executor::vec::topn::SortColumnSpec {
                column_index: 0,
                ascending: true,
                nulls_first: true,
            }],
            &[0],
            &[0],
            5,
        )
        .unwrap();

        let mut parallel = MaterializedBatchOperator::new(schema, batches);
        let actual = run_topn_parallel(
            &mut parallel,
            vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: true,
                collation_id: Some(255),
            }],
            5,
            3,
        )
        .unwrap();
        assert_eq!(actual, expected);
    }
    #[test]
    fn payload_ties_survive_worker_and_batch_boundaries() {
        let schema = schema_iv();
        let rows = [(9, 1), (2, 1), (8, 1), (1, 1), (5, 1), (3, 1)];
        for batch_size in [1, 2, 6] {
            for workers in [1, 2, 4] {
                let batches = rows
                    .chunks(batch_size)
                    .map(|rows| make_batch(&schema, rows))
                    .collect();
                let mut stream = MaterializedBatchOperator::new(schema.clone(), batches);
                let actual = run_topn_parallel(
                    &mut stream,
                    vec![SortKey {
                        column: 1,
                        ascending: true,
                        nulls_first: true,
                        collation_id: None,
                    }],
                    3,
                    workers,
                )
                .unwrap();
                assert_eq!(
                    actual,
                    vec![
                        vec![Value::Int(1), Value::Int(1)],
                        vec![Value::Int(2), Value::Int(1)],
                        vec![Value::Int(3), Value::Int(1)]
                    ]
                );
            }
        }
    }
}
