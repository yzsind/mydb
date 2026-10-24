//! Parallel aggregation and Top-N pipeline drivers.
//!
//! Drivers use bounded batch queues and owned scan workers. Morsel and Sink
//! are the shared input contract for their per-worker accumulators.

pub mod hash_agg_driver;
pub mod topn_driver;

use super::batch::Batch;
use crate::types::QueryError;

/// A unit of work flowing through the executor — typically a single
/// upstream `Batch`, but may carry a small bundle for amortisation.
#[derive(Debug)]
pub struct Morsel {
    pub batches: Vec<Batch>,
    /// Global scan-order index of the first row in `batches[0]`. Operators
    /// that need a stable first-seen tie-break (e.g. parallel Top-N) derive
    /// each row's sequence number as `base_row + physical_row_index`.
    /// Drivers that don't care leave it 0.
    pub base_row: u64,
}

/// Sink side of a pipeline — typically a hash table builder, a sort
/// build phase, or the final result-set writer.
pub trait Sink: Send {
    fn consume(&mut self, morsel: Morsel) -> Result<(), QueryError>;
    fn finish(&mut self) -> Result<(), QueryError>;
}
