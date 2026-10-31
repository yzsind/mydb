//! `BatchOperator` — the pull-based columnar execution protocol.
//!
//! Mirrors the row-based `PlanStream` interface (`fn next() -> Option<Row>`)
//! but at batch granularity, which is the throughput axis that matters
//! for the executor. A `BatchOperator::next_batch` call is
//! expected to amortise channel / virtual-call overhead across thousands
//! of rows.

use std::sync::Arc;

use crate::types::QueryError;

use super::batch::Batch;
use super::schema::BatchSchema;

/// One output ordering key, expressed against the operator's output schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchSortKey {
    pub column_index: usize,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// Ordering guaranteed for every batch and across successive batches.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum BatchOrdering {
    #[default]
    Unknown,
    Unordered,
    Ordered(Vec<BatchSortKey>),
}

/// Partitioning guaranteed by the producing operator.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum BatchPartitioning {
    #[default]
    Unknown,
    Single,
    #[allow(dead_code)] // Published by the forthcoming Exchange operator.
    Hash {
        columns: Vec<usize>,
        partitions: usize,
    },
    #[allow(dead_code)] // Published by the forthcoming Exchange/Exchange operator.
    RoundRobin {
        partitions: usize,
    },
}

/// Physical properties published by a batch operator. Runtime memory
/// pressure may change batch size and prefetch depth, but never these
/// plan-derived properties.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BatchProperties {
    pub ordering: BatchOrdering,
    pub partitioning: BatchPartitioning,
}

impl BatchProperties {
    pub const UNORDERED_SINGLE: Self = Self {
        ordering: BatchOrdering::Unordered,
        partitioning: BatchPartitioning::Single,
    };
}

/// Pull-based columnar operator. Intentionally **not** `Send`: the
/// legacy `PlanStream` it commonly wraps owns a thread-local mpsc
/// receiver and isn't safely sendable across threads either. Phase 4's
/// morsel scheduler runs batches across worker threads via a separate
/// `MorselSource` / `MorselSink` API that owns its data outright.
/// An owned partition of a native scan. The source establishes its captured
/// execution context around both decoding and the adjacent consumer.
pub trait ParallelBatchWorker: Send {
    fn run(
        self: Box<Self>,
        consume: &mut dyn FnMut(Batch) -> Result<(), QueryError>,
        abort: &std::sync::atomic::AtomicBool,
    ) -> Result<(), QueryError>;
}

pub trait BatchOperator {
    fn schema(&self) -> &Arc<BatchSchema>;
    /// An adjacent index lookup benefits from grouping these output keys.
    /// This is an optional pre-open locality hint, NOT an ordering promise.
    /// Rejection must not pull rows or alter the operator's SQL semantics.
    fn prefer_key_locality(&mut self, _columns: &[usize]) -> bool {
        false
    }

    /// The adjacent consumer has proved that these output columns are only
    /// observed through bounded prefixes (characters for text, bytes for
    /// binary). An unopened source may avoid reading the unused LOB suffix.
    /// Intervening operators must remove any column they observe in full;
    /// the default declines without changing input or consuming rows.
    fn limit_column_prefixes(&mut self, _limits: &[(usize, usize)]) {}
    fn properties(&self) -> BatchProperties {
        BatchProperties::default()
    }
    /// Unopened native storage source whose physical lanes come from catalog
    /// types, rather than runtime Value inference at an expression boundary.
    fn is_native_parallel_scan(&self) -> bool {
        false
    }
    /// Transfer unopened native scan partitions to an adjacent consumer.
    /// Rejection must not consume input. The requested degree must agree with
    /// the planned source topology; this only removes an intermediate exchange.
    fn take_parallel_workers(
        &mut self,
        _degree: usize,
    ) -> Result<Option<Vec<Box<dyn ParallelBatchWorker>>>, QueryError> {
        Ok(None)
    }
    /// Restrict an unopened, disposable discovery scan to evenly spaced heap
    /// pages. This is never an exact-query source: the caller must reopen the
    /// complete scan at the same snapshot and prove its result independently.
    /// The caller reserves capacity for up to `max_pages` page descriptors
    /// until this source is dropped. Rejection must not consume input.
    fn sample_scan_pages(&mut self, _max_pages: usize) -> bool {
        false
    }
    /// Install a build-derived runtime filter into a still-unopened child.
    /// Operators that cannot push the filter to a matching source return
    /// false. This changes only candidate batching inside the fixed physical
    /// plan; the parent join remains the semantic correctness check.
    fn install_runtime_filter(&mut self, _filter: crate::planner::RuntimeFilter) -> bool {
        false
    }
    /// Tighten a still-unopened index source with a statement-materialized
    /// scalar predicate. The parent predicate remains in the graph as the
    /// semantic check; this only turns its resolved constant into a physical
    /// seek bound inside the planner-selected index path.
    fn install_runtime_index_bound(&mut self, _predicate: &crate::parser::Expr) -> bool {
        false
    }
    /// Increase a row-goal scan's candidate window after an adjacent
    /// residual filter rejected the entire previous batch. This is runtime
    /// batching feedback only and must not change the physical access path.
    fn grow_candidate_batch_rows(&mut self) -> Option<usize> {
        None
    }
    /// Give an adjacent HashAggregate sink a chance to consume native join
    /// match indices directly. The default is a strict pre-I/O rejection:
    /// returning `false` must not pull any input. Join operators override this
    /// for layouts where group/aggregate columns can be addressed on the
    /// build/probe batches without first gathering a joined payload batch.
    fn consume_into_hash_aggregate(
        &mut self,
        _aggregate: &mut crate::engine::executor::vec::hash_agg::HashAggregateCore,
    ) -> Result<bool, QueryError> {
        Ok(false)
    }
    /// Variant of the adjacent join→aggregate sink where aggregate arguments
    /// are deterministic expressions appended by the caller. Join operators
    /// may evaluate probe-only expressions once per probe row and then apply
    /// exact match multiplicity.
    fn consume_computed_into_hash_aggregate(
        &mut self,
        _aggregate: &mut crate::engine::executor::vec::hash_agg::HashAggregateCore,
        _expressions: &[crate::parser::Expr],
    ) -> Result<bool, QueryError> {
        Ok(false)
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError>;
}

/// Operator that wraps a pre-materialised `Vec<Batch>`. Useful as a unit-test
/// harness, as a bridge from legacy row-based plans, and as the underlying
/// storage for materialised CTEs.
pub struct MaterializedBatchOperator {
    schema: Arc<BatchSchema>,
    properties: BatchProperties,
    batches: std::vec::IntoIter<Batch>,
}

impl MaterializedBatchOperator {
    pub fn new(schema: Arc<BatchSchema>, batches: Vec<Batch>) -> Self {
        Self {
            schema,
            properties: BatchProperties::default(),
            batches: batches.into_iter(),
        }
    }

    pub fn with_properties(
        schema: Arc<BatchSchema>,
        batches: Vec<Batch>,
        properties: BatchProperties,
    ) -> Self {
        Self {
            schema,
            properties,
            batches: batches.into_iter(),
        }
    }

    /// Recover the not-yet-consumed batches. Used when an operator switches
    /// from serial probing to a partitioned worker build before reading this
    /// materialized source.
    pub fn into_batches(self) -> Vec<Batch> {
        self.batches.collect()
    }
}

impl BatchOperator for MaterializedBatchOperator {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        Ok(self.batches.next())
    }

    fn properties(&self) -> BatchProperties {
        self.properties.clone()
    }
}

/// Pull every batch into memory. Convenience for tests; production paths
/// should treat materialisation as a deliberate operator (CTE, build side
/// of a hash join, etc.).
#[cfg(test)]
pub fn collect_batches(stream: &mut dyn BatchOperator) -> Result<Vec<Batch>, QueryError> {
    let mut out = Vec::new();
    while let Some(b) = stream.next_batch()? {
        out.push(b);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::vec::batch::{Batch, ColumnVector};
    use crate::engine::executor::vec::schema::BatchField;
    use crate::types::{ColumnType, Value};

    #[test]
    fn materialized_stream_yields_batches() {
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: String::new(),
            alias: None,
            name: "x".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }]));
        let b1 = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(vec![
                Value::Int(1),
                Value::Int(2),
            ])],
        );
        let b2 = Batch::new(
            schema.clone(),
            vec![ColumnVector::from_values(vec![Value::Int(3)])],
        );
        let mut s = MaterializedBatchOperator::new(schema, vec![b1, b2]);
        let v = collect_batches(&mut s).unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].num_rows(), 2);
        assert_eq!(v[1].num_rows(), 1);
    }

    #[test]
    fn materialized_operator_preserves_declared_physical_properties() {
        let schema = Arc::new(BatchSchema::new(vec![BatchField {
            table: String::new(),
            alias: None,
            name: "x".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        }]));
        let properties = BatchProperties {
            ordering: BatchOrdering::Ordered(vec![BatchSortKey {
                column_index: 0,
                ascending: true,
                nulls_first: true,
            }]),
            partitioning: BatchPartitioning::Single,
        };
        let stream =
            MaterializedBatchOperator::with_properties(schema, Vec::new(), properties.clone());
        assert_eq!(stream.properties(), properties);
    }
}
