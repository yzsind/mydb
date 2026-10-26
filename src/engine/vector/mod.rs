//! Native typed vectors, exact distance functions and HNSW/IVF indexes.
//! Heap values retain their datatype and compact payload. ANN backends produce
//! candidates; the executor rechecks MVCC visibility and native heap values.

pub(crate) mod bulk_hnsw;
pub(crate) mod codec;
pub(crate) mod distance;
pub(crate) mod graph;
pub(crate) mod hnsw;
pub(crate) mod hnsw_core;
pub(crate) mod hnsw_paged;
pub(crate) mod ivf;
pub(crate) mod native_hnsw;
pub(crate) mod native_ivf;
pub(crate) mod persistence;
pub(crate) mod session;
pub(crate) mod spill_rows;
pub(crate) mod sql;

#[cfg(test)]
mod hnsw_bug_audit_tests;

#[cfg(test)]
mod hnsw_nav_recall_tests;

use crate::types::QueryError;

pub(crate) use codec::{
    coerce_bytes_to_internal, compare, normalize_internal, to_f32, to_text, to_text_into,
    validate_internal, vector_err,
};
pub(crate) use distance::Metric;
pub(crate) use graph::{
    VectorGraph, VectorIndexRuntimeStats, VectorSearchEffort, VectorSearchRequest,
    VectorSearchResult,
};
pub(crate) use hnsw::{
    update_inplace_cfg, CompactHnswBuilder, CompactHnswStats, HnswIndex, DEFAULT_EF_CONSTRUCTION,
    DEFAULT_EF_SEARCH, DEFAULT_M,
};
pub(crate) use ivf::IvfParams;

/// Single MySQL error mapping for vector-domain failures. Parser/name errors
/// remain the parser/planner's 1064/1054; malformed values and dimensions are
/// 50001/22018 (invalid value) or 50002/22023 (dimension mismatch), while an
/// unavailable vector capability is 1235/42000.
#[derive(Debug)]
pub(crate) enum VectorError {
    InvalidValue(String),
    DimensionMismatch { actual: usize, declared: u32 },
    Unsupported(String),
}

impl VectorError {
    pub(crate) fn into_query_error(self) -> QueryError {
        match self {
            Self::InvalidValue(message) => crate::mysql::errors::error(
                crate::mysql::errors::MYDB_ER_INVALID_VECTOR_VALUE,
                message,
            ),
            Self::DimensionMismatch { actual, declared } => crate::mysql::errors::error(
                crate::mysql::errors::MYDB_ER_VECTOR_DIMENSION_MISMATCH,
                format!(
                    "VECTOR dimension mismatch: value has {actual} dimensions but the column is VECTOR({declared})"
                ),
            ),
            Self::Unsupported(message) => crate::storage::error(crate::error::catalog::ER_NOT_SUPPORTED_YET, message),
        }
    }
}

pub(crate) mod quantization;
