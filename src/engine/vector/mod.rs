//! High-performance vector search for a MySQL-compatible `VECTOR(n)` type and
//! an HNSW approximate-nearest-neighbour index.
//!
//! A `VECTOR` value is a bare little-endian `f32` array (see [`codec`]); the
//! HNSW graph ([`hnsw`]) is an in-memory candidate generator that persists like
//! the spatial R-tree (derived data, rebuilt on a missing/corrupt file). The
//! SQL surface (`DISTANCE`, `STRING_TO_VECTOR`, …) lives in [`sql`], routed from
//! `engine::expr`.
//!
//! Submodules mirror `engine::geo`:
//!   * [`codec`]    — internal byte format ⇄ `f32` / text, validation.
//!   * [`distance`] — SIMD-friendly L2 / cosine / dot kernels + [`Metric`].
//!   * [`hnsw`]     — the HNSW graph and its serialisation.
//!   * [`sql`]      — the vector SQL function library.

pub(crate) mod codec;
pub(crate) mod distance;
pub(crate) mod graph;
pub(crate) mod hnsw;
pub(crate) mod hnsw_core;
pub(crate) mod hnsw_paged;
pub(crate) mod ivf;
pub(crate) mod ivf_paged;
pub(crate) mod session;
pub(crate) mod spill_rows;
pub(crate) mod sql;

#[cfg(test)]
mod hnsw_bug_audit_tests;

#[cfg(test)]
mod ivf_bug_audit_tests;

#[cfg(test)]
mod ivf_nav_recall_tests;

use crate::types::QueryError;

pub(crate) use codec::{
    coerce_bytes_to_internal, coerce_bytes_to_internal_owned, compare, dim, normalize_internal,
    normalize_internal_owned, to_f32, to_text, to_text_into, validate_internal, vector_err,
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
pub(crate) use ivf::{IvfParams, IvfSq8Index};

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

/// Validate an internal vector against a column's declared dimension. A `None`
/// declared dimension (`VECTOR` with no length) accepts any dimension; a
/// `Some(d)` requires an exact match. Always rejects malformed bytes.
pub(crate) fn validate_dimension(internal: &[u8], declared: Option<u32>) -> Result<(), QueryError> {
    if internal.len() % 4 != 0 {
        return Err(vector_err(
            "invalid VECTOR value: byte length is not a multiple of 4",
        ));
    }
    let actual = dim(internal);
    if let Some(d) = declared {
        if actual != d as usize {
            return Err(VectorError::DimensionMismatch {
                actual,
                declared: d,
            }
            .into_query_error());
        }
    }
    Ok(())
}
