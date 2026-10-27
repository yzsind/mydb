pub(crate) mod ai;
pub(crate) mod http;
pub mod api;
pub mod audit_log;
pub mod auth;
pub mod auto_analyze;
pub mod auto_param;
pub(crate) mod background;
pub mod cache;
pub mod cache_registry;
pub(crate) mod cancel;
pub mod cbo;
pub mod cursor_registry;
pub(crate) mod executor;
pub use executor::mem_budget::controlled_memory_snapshot;
pub mod vector_maintenance;
pub mod zone_map_maintenance;

/// Bench-only re-exports for the `decimal_sum_simd_bench` binary. The
/// vectorized lanes are crate-internal in production builds — exposed
/// here behind the `tools` feature so the bench can drive the real
/// `ColumnVector` / SIMD kernels rather than copies.
#[cfg(feature = "tools")]
pub mod vec_bench_api {
    pub use super::executor::vec::batch::{Batch, ColumnVector};
    pub use super::executor::vec::expr_batch::{
        sum_decimal_column, sum_decimal_column_with_nulls, sum_i128_simd, sum_i32_simd,
        sum_i64_simd,
    };
    pub use super::executor::vec::hash_agg::{AggregateKind, AggregateSpec, HashAggregateCore};
    pub use super::executor::vec::schema::{BatchField, BatchSchema};

    /// Drive the production SME exact-L2 batch kernel from the standalone
    /// vector benchmark. `rows` is row-major and must contain a whole number
    /// of vectors with the same dimension as `query`.
    pub fn sme_l2_batch(query: &[f32], rows: &[f32], out: &mut [f32]) -> bool {
        let q = unsafe { std::slice::from_raw_parts(query.as_ptr().cast::<u8>(), query.len() * 4) };
        let r = unsafe { std::slice::from_raw_parts(rows.as_ptr().cast::<u8>(), rows.len() * 4) };
        super::vector::distance::l2_sq_sme_batch(q, r, out)
    }

    pub fn sme_available() -> bool {
        super::vector::distance::brute_sme_available()
    }
}

/// Tools-only expression compiler and execution microbenchmarks.
#[cfg(feature = "tools")]
pub mod expression_bench;

/// Bench-only access to the production partition router. Keeping this behind
/// `tools` avoids widening the SQL engine's public API for normal builds.
#[cfg(feature = "tools")]
pub mod partition_bench_api {
    pub use super::executor::partition_router::route_row;
}
pub(crate) mod builtin_catalog;
pub(crate) mod expr;
pub(crate) mod expr_program;
pub(crate) mod expr_type;
pub(crate) mod func_cache;
pub(crate) mod geo;
pub(crate) mod information_schema;
pub(crate) mod information_schema_mysql80_character_metadata;
pub(crate) mod information_schema_mysql80_keywords;
pub(crate) mod json;
pub(crate) mod keyring;
pub mod logging;
pub(crate) mod memory_reclaimer;
pub(crate) mod memory_status;
pub(crate) mod mysql_schema;
pub mod params;
pub mod password_policy;
pub(crate) mod perf_digest;
pub(crate) mod perf_errors;
pub(crate) mod perf_events;
pub(crate) mod perf_schema;
pub(crate) mod perf_setup;
mod perf_statement_stats;
pub(crate) mod performance_schema_mysql80_generated;
pub mod session_registry;
mod show;
pub(crate) mod status_counters;
pub(crate) mod sys_schema;
pub(crate) mod system;
pub mod sysvar;
pub(crate) mod table_stats_view;
pub(crate) mod time_locale;
pub(crate) mod tz;
pub(crate) mod vector;

pub use crate::types::{Column, ColumnType, QueryError, Value};
pub use api::{
    QueryEngine, QueryResponse, ResultCursor, ResultSet, SessionContext, StreamingResultSet,
    VecResultCursor,
};
pub use executor::StorageQueryEngine;
pub use logging::QueryLogConfig;
pub use session_registry::SessionRegistry;
pub use system::{normalize_sql, StubQueryEngine};

#[doc(hidden)]
pub fn initialize_server_status_clock() {
    status_counters::initialize_server_clock();
}
