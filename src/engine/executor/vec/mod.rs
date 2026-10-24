//! Vectorized / columnar executor.
//!
//! The vectorized executor lives parallel to the legacy row-based pipeline
//! under `executor::select`. It pulls data through the operator graph as
//! [`Batch`]es of column vectors instead of as `PlanRow`s, which removes
//! the per-row metadata clone, the per-row mpsc channel hop, and the
//! per-row expression interpreter dispatch that dominate today's profiles.
//!
//! Operator implementations (Top-N, HashAgg, HashJoin, Distinct, Window)
//! share the same [`BatchOperator`] contract. Scalar and vector kernels are
//! selected inside that protocol; the scalar kernel remains the semantic
//! ground truth.

pub mod batch;
pub mod bitmap;
pub mod config;
pub mod distinct;
pub(crate) mod evaluated_batch;
pub mod expr_batch;
pub mod hash_agg;
pub mod hash_join;
pub mod index_scan;
mod integer_cmp;
pub(crate) mod join_output;
pub mod key_encode;
pub mod scan_bridge;
pub mod scheduler;
pub mod schema;
pub mod sort;
pub mod spill;
pub mod stream;
pub mod topn;
pub mod window;

#[allow(unused_imports)]
pub use batch::{Batch, ColumnVector, Selection, DEFAULT_BATCH_ROWS};
#[allow(unused_imports)]
pub use bitmap::Bitmap;
#[cfg(test)]
pub use config::with_vec_mode;
#[allow(unused_imports)]
pub use config::{current_mode, effective_mode, VecMode};
#[allow(unused_imports)]
pub use schema::{BatchField, BatchSchema};
#[allow(unused_imports)]
pub use stream::{
    BatchOperator, BatchOrdering, BatchPartitioning, BatchProperties, BatchSortKey,
    MaterializedBatchOperator,
};
