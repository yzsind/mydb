//! Top-N operator over a `BatchOperator`.
//!
//! Maintains a max-heap of `K` rows keyed by the supplied ORDER BY
//! expressions and feeds rows in from upstream batches. The benefit over
//! the legacy `Vec::sort_by` + `truncate(K)` path is asymptotic — the
//! heap path is `O(N log K)` against the legacy `O(N log N)` — and the
//! peak memory is `K` rows rather than `N` rows. For very small inputs
//! the planner is expected to fall back to a full sort.
//!
//! Phase 3A scope: integer / float / string / date sort keys with a fixed
//! single-table input. Multi-key tie-break is supported. NULL ordering
//! follows MySQL semantics: NULLs sort first by default for ASC.

#[cfg(test)]
use std::sync::Arc;

use crate::types::{QueryError, Value};

use super::stream::BatchOperator;
use crate::engine::executor::mem_budget::ScopedCharge;
#[cfg(test)]
use crate::engine::executor::memory_footprint::AllocatedBytes;
use crate::engine::executor::sort::{BoundedTopK, SortField};

/// Upper bound on `K` for the heap-based Top-N. Above this we expect
/// the legacy quickselect path in `apply_ordered_limit` to be the better
/// pick (its memory peak is bounded by the input size rather than K).
/// Phase 5 CBO will refine this once heap-vs-sort calibration data is
/// available.
pub const DEFAULT_MAX_TOPN_K: usize = 10_000;

/// Budget-aware admission cap for the vec Top-N heap. The static
/// 10 000-row floor always passes; beyond it, k is admitted while the
/// heap's worst-case footprint (k rows × a conservative 256 B width
/// estimate) stays within 1/8 of the query's remaining memory budget,
/// hard-capped at the planner's own TopK routing ceiling (1M rows).
/// Without a budget in scope, the static floor applies unchanged.
pub fn max_topn_k_for_budget() -> usize {
    const EST_ROW_BYTES: usize = 256;
    const HARD_CAP: usize = 1_000_000;
    let budget_cap = crate::engine::executor::mem_budget::current_budget()
        .map(|b| b.remaining() / 8 / EST_ROW_BYTES)
        .unwrap_or(0);
    DEFAULT_MAX_TOPN_K.max(budget_cap.min(HARD_CAP))
}

/// Push-style Top-N accumulator for callers that compute sort keys
/// outside the batch schema (e.g. ORDER BY expressions evaluated via
/// `expr_batch`). `specs[i].column_index` indexes into each pushed
/// `keys` tuple, not a batch schema. Same tie semantics as `run_topn`:
/// first-seen rows win on equal keys.
pub struct TopNAccumulator {
    topk: BoundedTopK<Vec<Value>>,
}

impl TopNAccumulator {
    pub fn new(specs: Vec<SortColumnSpec>, k: usize) -> Result<Self, QueryError> {
        let fields = specs
            .iter()
            .map(|spec| SortField {
                ascending: spec.ascending,
                nulls_first: spec.nulls_first,
                collation_id: None,
            })
            .collect::<Vec<_>>();
        let topk = BoundedTopK::from_fields(fields, k)?;
        Ok(Self { topk })
    }

    /// Reserve a bounded startup window before opening the upstream producer.
    pub(crate) fn new_with_width(
        specs: Vec<SortColumnSpec>,
        k: usize,
        row_columns: usize,
    ) -> Result<Self, QueryError> {
        let mut accumulator = Self::new(specs, k)?;
        accumulator.topk.reserve_payload_width(row_columns);
        Ok(accumulator)
    }

    pub fn with_collations(mut self, collations: Vec<Option<u16>>) -> Self {
        self.topk = self.topk.with_collations(collations);
        self
    }

    /// Materialise output columns only for candidates that enter the heap.
    pub fn push_lazy<F: FnOnce() -> Vec<Value>>(
        &mut self,
        keys: Vec<Value>,
        make_row: F,
    ) -> Result<(), QueryError> {
        self.topk.push_lazy(keys, make_row)
    }

    pub fn finish(self) -> Result<Vec<Vec<Value>>, QueryError> {
        self.finish_charged().map(|(rows, _charge)| rows)
    }

    pub(crate) fn finish_charged(self) -> Result<(Vec<Vec<Value>>, ScopedCharge), QueryError> {
        self.topk.into_sorted_vec_charged()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SortColumnSpec {
    pub column_index: usize,
    pub ascending: bool,
    /// MySQL convention: ascending → NULLs first, descending → NULLs last.
    pub nulls_first: bool,
}

impl SortColumnSpec {
    #[cfg(test)]
    pub fn asc(column_index: usize) -> Self {
        Self {
            column_index,
            ascending: true,
            nulls_first: true,
        }
    }
    #[cfg(test)]
    pub fn desc(column_index: usize) -> Self {
        Self {
            column_index,
            ascending: false,
            nulls_first: false,
        }
    }
}

/// Consume a `BatchOperator`, retain the top `k` rows ordered by `specs`,
/// and produce them as `Vec<Vec<Value>>` in sorted order. Returns rows in
/// final ascending (or whatever ORDER BY requests) order.
pub fn run_topn(
    stream: &mut dyn BatchOperator,
    specs: &[SortColumnSpec],
    sort_columns: &[usize], // indices into the *batch* schema for the sort keys
    projection: &[usize],   // indices into the batch schema for the output columns
    k: usize,
) -> Result<Vec<Vec<Value>>, QueryError> {
    if k == 0 {
        return Ok(Vec::new());
    }
    assert_eq!(specs.len(), sort_columns.len(), "spec/key count mismatch");
    let collations = sort_columns
        .iter()
        .map(|&column| stream.schema().fields()[column].source.collation_id)
        .collect();
    let mut accumulator = TopNAccumulator::new_with_width(specs.to_vec(), k, projection.len())?
        .with_collations(collations);
    while let Some(batch) = stream.next_batch()? {
        crate::engine::cancel::check_cancelled()?;
        crate::engine::executor::select::eval::record_order_by_key_rows_prepared(
            batch.visible_rows(),
        );
        for r in batch.visible_indices() {
            let keys = sort_columns
                .iter()
                .map(|&c| batch.columns()[c].value_at(r))
                .collect();
            accumulator.push_lazy(keys, || {
                projection
                    .iter()
                    .map(|&c| batch.columns()[c].value_at(r))
                    .collect()
            })?;
        }
    }
    accumulator.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::vec::batch::{Batch, ColumnVector};
    use crate::engine::executor::vec::schema::{BatchField, BatchSchema};
    use crate::engine::executor::vec::stream::MaterializedBatchOperator;
    use crate::types::ColumnType;

    fn schema_i64() -> Arc<BatchSchema> {
        Arc::new(BatchSchema::new(vec![BatchField {
            table: "t".into(),
            alias: None,
            name: "v".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }]))
    }

    #[test]
    fn topn_retains_wide_results_above_producer_soft_watermark() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        let budget = Arc::new(MemBudget::process_tracked_for_test(8192));
        with_query_budget(budget.clone(), || {
            let mut acc =
                TopNAccumulator::new_with_width(vec![SortColumnSpec::asc(0)], 20, 2).unwrap();
            let mut producer = ScopedCharge::new(0);
            producer.add_force(8192).unwrap();
            for value in (0..100).rev() {
                acc.push_lazy(vec![Value::Int(value)], || {
                    vec![Value::Int(value), Value::String("x".repeat(1024))]
                })
                .unwrap();
            }
            let (rows, charge) = acc.finish_charged().unwrap();
            assert_eq!(rows.len(), 20);
            for (value, row) in rows.iter().enumerate() {
                assert_eq!(row[0], Value::Int(value as i64));
            }
            assert_eq!(budget.used(), 8192 + rows.allocated_bytes());
            drop(rows);
            drop(charge);
            assert_eq!(budget.used(), 8192);
        });
        assert_eq!(budget.used(), 0);
        assert!(budget.peak_used() < 64 * 1024);
    }

    #[test]
    fn topn_large_k_and_wide_rows_use_resident_budget() {
        use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
        let budget = Arc::new(MemBudget::new(16 * 1024));
        with_query_budget(budget.clone(), || {
            let mut acc = TopNAccumulator::new(vec![SortColumnSpec::asc(0)], usize::MAX).unwrap();
            for value in [3, 1, 2] {
                acc.push_lazy(vec![Value::Int(value)], || {
                    vec![Value::String("x".repeat(256))]
                })
                .unwrap();
            }
            assert!(budget.used() < 4096);
            let err = acc
                .push_lazy(vec![Value::Int(0)], || {
                    vec![Value::Bytes(vec![0; 32 * 1024])]
                })
                .unwrap_err();
            assert_eq!(err.code(), 1037);
            let (rows, charge) = acc.finish_charged().unwrap();
            assert_eq!(rows.len(), 3);
            assert_eq!(budget.used(), rows.allocated_bytes());
            drop(rows);
            drop(charge);
        });
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn topn_asc_basic() {
        let s = schema_i64();
        let b = Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Int(7),
                Value::Int(2),
                Value::Int(5),
                Value::Int(1),
                Value::Int(9),
            ])],
        );
        let mut stream = MaterializedBatchOperator::new(s, vec![b]);
        let out = run_topn(&mut stream, &[SortColumnSpec::asc(0)], &[0], &[0], 3).unwrap();
        let got: Vec<i64> = out
            .into_iter()
            .map(|r| match r[0] {
                Value::Int(x) => x,
                _ => panic!(),
            })
            .collect();
        assert_eq!(got, vec![1, 2, 5]);
    }

    #[test]
    fn topn_desc_basic() {
        let s = schema_i64();
        let b = Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Int(7),
                Value::Int(2),
                Value::Int(5),
                Value::Int(1),
                Value::Int(9),
            ])],
        );
        let mut stream = MaterializedBatchOperator::new(s, vec![b]);
        let out = run_topn(&mut stream, &[SortColumnSpec::desc(0)], &[0], &[0], 2).unwrap();
        let got: Vec<i64> = out
            .into_iter()
            .map(|r| match r[0] {
                Value::Int(x) => x,
                _ => panic!(),
            })
            .collect();
        assert_eq!(got, vec![9, 7]);
    }

    #[test]
    fn topn_nulls_first_default_ascending() {
        let s = schema_i64();
        let b = Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Int(3),
                Value::Null,
                Value::Int(1),
                Value::Null,
                Value::Int(2),
            ])],
        );
        let mut stream = MaterializedBatchOperator::new(s, vec![b]);
        let out = run_topn(&mut stream, &[SortColumnSpec::asc(0)], &[0], &[0], 3).unwrap();
        // Expect: NULL, NULL, 1
        assert!(matches!(out[0][0], Value::Null));
        assert!(matches!(out[1][0], Value::Null));
        assert!(matches!(out[2][0], Value::Int(1)));
    }

    #[test]
    fn topn_across_multiple_batches() {
        let s = schema_i64();
        let b1 = Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Int(10),
                Value::Int(4),
            ])],
        );
        let b2 = Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Int(2),
                Value::Int(7),
                Value::Int(1),
            ])],
        );
        let mut stream = MaterializedBatchOperator::new(s, vec![b1, b2]);
        let out = run_topn(&mut stream, &[SortColumnSpec::asc(0)], &[0], &[0], 3).unwrap();
        let got: Vec<i64> = out
            .into_iter()
            .map(|r| match r[0] {
                Value::Int(x) => x,
                _ => panic!(),
            })
            .collect();
        assert_eq!(got, vec![1, 2, 4]);
    }

    #[test]
    fn topn_k_larger_than_input_returns_all_sorted() {
        let s = schema_i64();
        let b = Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Int(3),
                Value::Int(1),
                Value::Int(2),
            ])],
        );
        let mut stream = MaterializedBatchOperator::new(s, vec![b]);
        let out = run_topn(&mut stream, &[SortColumnSpec::asc(0)], &[0], &[0], 100).unwrap();
        let got: Vec<i64> = out
            .into_iter()
            .map(|r| match r[0] {
                Value::Int(x) => x,
                _ => panic!(),
            })
            .collect();
        assert_eq!(got, vec![1, 2, 3]);
    }

    #[test]
    fn topn_k_zero_yields_nothing() {
        let s = schema_i64();
        let b = Batch::new(
            s.clone(),
            vec![ColumnVector::from_values(vec![Value::Int(1)])],
        );
        let mut stream = MaterializedBatchOperator::new(s, vec![b]);
        let out = run_topn(&mut stream, &[SortColumnSpec::asc(0)], &[0], &[0], 0).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn accumulator_materializes_payload_only_for_heap_candidates() {
        use std::cell::Cell;

        let materialized = Cell::new(0usize);
        let mut acc = TopNAccumulator::new(vec![SortColumnSpec::asc(0)], 2).unwrap();
        for key in [1_i64, 2, 100, 200] {
            acc.push_lazy(vec![Value::Int(key)], || {
                materialized.set(materialized.get() + 1);
                vec![Value::String(format!("wide-payload-{key}"))]
            })
            .unwrap();
        }

        assert_eq!(materialized.get(), 2, "losing rows must not clone payloads");
        assert_eq!(acc.finish().unwrap().len(), 2);
    }
}

#[cfg(test)]
#[path = "topn_bench.rs"]
mod refactor_benchmark;
