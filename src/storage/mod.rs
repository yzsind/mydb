pub(crate) mod capture;
pub(crate) mod catalog;
mod core;
pub(crate) mod data_dictionary;
pub(crate) mod dir_lock;
pub(crate) mod file_generation;
pub mod format_versions;
pub(crate) mod fulltext;
pub mod image;
pub(crate) mod image_io;
pub(crate) mod mdl;
pub mod naming;
pub mod partitioning;
pub(crate) mod read_schema;
pub(crate) mod recovery_context;
pub(crate) mod schema;
pub(crate) mod sequence;
pub(crate) mod snapshot;
mod stats;
pub(crate) mod temp_quota;
mod temporal;
pub use recovery_context::DatabaseOpenMode;
mod txn;
pub(crate) use txn::{metadata_wait_timeout, MetadataWaitTimeoutGuard};
mod undo;
mod value;
mod view_query;
pub(crate) use value::to_i64;

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_decode_row(bytes: &[u8]) {
    let _ = core::decode_row(bytes);
}
pub mod wait_events;

pub(crate) use core::backup_page_valid;
#[cfg(test)]
pub(crate) use core::commit_visibility_test_hook;
pub(crate) use core::default_buffer_pool_bytes;
pub(crate) use core::index_build_extended_perf_snapshot;
pub(crate) use core::is_lock_wait_pending_error;
pub(crate) use core::quantize_time_text_micros;
#[cfg(test)]
pub(crate) use core::set_update_delta_enabled_for_test;
#[cfg(test)]
pub(crate) use core::source_audit_vector_file;
#[cfg(feature = "mysql-replica")]
pub(crate) use core::DeferredCommit;
pub(crate) use core::DmlIgnoreCoercionGuard;
pub(crate) use core::ExactIndexVisibility;
#[cfg(test)]
pub(crate) use core::VECTOR_BUILD_BEFORE_PUBLISH_HOOK;
pub(crate) use core::{
    canonicalize_loaded_timestamp_value, coerce_routine_typed_value, ignorable_update_error,
    with_enum_field_copy_coercion, LoadDataIndexBuilders, LoadDataIndexKeyBatch,
    ShadowGenerationBuilder, ShadowHeapAppender,
};
pub(crate) use core::{
    decode_row_into_sink_fast, decode_row_schema_columns_into_with_plan_ext_for_layout,
    f32_from_order_u32, f64_from_order_u64, fast_decode_fallback_reason, format_decimal_v3,
    DirectScanScratch, FastDecodeFallbackReason, OnlineWalApplyContext, PayloadStreamHit,
    RowDecodeSink, ORDER_STATS_CACHE_HITS, ORDER_STATS_CACHE_INVALIDATIONS,
    ORDER_STATS_CACHE_MISSES,
};
pub(crate) use core::{
    encode_visible_row_payload, is_temp_table_id, parse_date_to_days_bytes,
    parse_datetime_to_micros, parse_time_to_micros, quantize_datetime_text_micros,
    CompiledDirectRowEncoder, ZoneMapWorkerBatch,
};
pub(crate) use core::{expand_changed_columns_for_generated, index_may_be_affected_by_columns};
#[cfg(test)]
pub(crate) use core::{
    lob_full_materializations_for_test, prepared_point_sparse_patch_hits_for_test,
    record_prepared_point_sparse_patch_hit_for_test,
};
#[cfg(test)]
pub(crate) use core::{
    mysql_status_snapshot_calls, reset_zone_map_cost_estimate_calls, zone_map_cost_estimate_calls,
};
pub use core::{resolve_buffer_pool_pages, ZoneMapRangeIter, ZoneMapRanges};
pub(crate) use core::{
    ZONE_MAP_METADATA_DECODE_NS, ZONE_MAP_RUNTIME_CANDIDATE_NS, ZONE_MAP_RUNTIME_PAGES,
    ZONE_MAP_RUNTIME_PAGES_SKIPPED, ZONE_MAP_RUNTIME_TASKS,
};
// ENUM/SET on-disk width helpers — single source of truth (see the
// `set_storage_width` / `enum_storage_width` doc comments in row_codec).
pub(crate) use core::CatalogTxnBatchGuard;
pub(crate) use core::{enum_storage_width, set_storage_width};
pub use core::{
    error, DataLockRow, DataLockWaitRow, Database, DurabilityMode, IndexBuildOptions,
    InsertRowsResult, MysqlStatus, RedoScopeGuard, RowId, SnapshotGuard, StartupValidationMode,
    StorageConfig, StorageStatus, LOCK_WAIT_PENDING_MESSAGE,
};
pub(crate) use core::{fk_checks_enabled, fk_max_depth_value, set_fk_max_depth, FkChecksGuard};
// M7 — runtime-tunable LOB GUC sysvar adapters (getter/setter, u64-encoded).
pub(crate) use core::coerce_updated_row;
#[cfg(test)]
pub(crate) use core::reorg_peak_batch_rows;
pub(crate) use core::{
    current_shared_operator_buffer_stats, query_buffer_stats_counts, with_query_buffer_stats,
    with_shared_operator_buffer_stats, with_shared_operator_buffer_stats_if_present,
    QueryBufferStats,
};
#[cfg(test)]
pub(crate) use core::{fk_visited_len, reset_row_migrations_for_test};
pub(crate) use core::{record_spill_bytes_read, record_spill_bytes_written};
pub use core::{
    row_migrations_total, update_delta_hits, update_delta_misses, update_in_place_total,
    update_migrate_total,
};
#[cfg(test)]
pub(crate) use core::{spill_bytes_read, spill_bytes_written};
pub(crate) use core::{
    sysvar_lob_compress_min_bytes_get, sysvar_lob_compress_min_bytes_set,
    sysvar_lob_compression_get, sysvar_lob_compression_set, sysvar_lob_extent_min_pages_get,
    sysvar_lob_extent_min_pages_set, sysvar_lob_inline_max_get, sysvar_lob_inline_max_set,
    sysvar_lob_prefetch_disable_get, sysvar_lob_prefetch_disable_set,
    sysvar_lob_reuse_unchanged_get, sysvar_lob_reuse_unchanged_set, sysvar_lob_shrink_disable_get,
    sysvar_lob_shrink_disable_set, sysvar_lob_shrink_min_free_pages_get,
    sysvar_lob_shrink_min_free_pages_set,
};
pub(crate) use core::{CompiledIndexKeyProgram, EncodedPendingIndexRecord};
pub(crate) use core::{
    HeapHandle, IndexHandle, IndexRangeCursor, IndexRangeLease, IndexRangeReverseCursor,
    ProjectedReadPlan, RowDecodePlan, PAGE_SIZE,
};
pub(crate) use core::{NodeId, VecId, VectorIndexFile};
pub use mdl::{MdlDuration, MdlLockType, MdlObjectKey, MdlObjectType, MdlRequest, MetadataLockRow};
pub use naming::{
    event_key, is_system_database, normalize_name, routine_key, split_table_name, table_key,
    trigger_key,
};
pub use partitioning::{
    compare_row_to_range_bound, compare_value, hash_value_to_bucket, key_hash_bucket,
    linear_hash_to_bucket, linear_key_bucket, no_partition_for_value, Partition, PartitionBounds,
    PartitionInfo, PartitionLeaf, PartitionLiteral, PartitionMethod, PartitionPruningPlan,
    PartitionSet, RoutedPartition, TableStorageLayout,
};
pub use schema::{
    build_fk_runtime, compile_index_key_expressions, Catalog, ColumnSchema, ColumnState,
    DataTypeKind, DatabaseSchema, DmlRowRef, DmlStoredRow, EventIntervalSchema,
    EventOnCompletionSchema, EventScheduleSchema, EventSchema, EventStatusSchema, ForeignKeySchema,
    FulltextMeta, InboundFk, IndexBound, IndexColumnDirection, IndexExpression, IndexKey,
    IndexKeyPart, IndexSchema, IndexState, KeyPart, ParamTarget, ReferentialAction,
    RoutineKindSchema, RoutineParamModeSchema, RoutineParamSchema, RoutineSchema,
    RoutineSecuritySchema, RoutineSqlDataAccessSchema, RowLayoutColumnSchema, RowLayoutSchema,
    RowLayoutState, StoredRow, TableSchema, TriggerEventSchema, TriggerOrderKindSchema,
    TriggerOrderSchema, TriggerSchema, TriggerTimingSchema, ViewColumnDef, ViewDependency,
    ViewRoutineDependency, ViewSchema, CATALOG_VERSION, DEFAULT_DATABASE,
};
pub use stats::multi_column_group_key;
pub use stats::{
    ColumnStats, FrequencySource, FrequencyStats, FulltextStats, HistogramBucket, IndexStats,
    MultiColumnStats, PartitionStats, TableStats, TokenFrequency, ValueFrequency,
};
pub use txn::{
    inline_lock_would_block_log_suppressed_count, inline_lock_would_block_retry_count,
    is_lock_would_block_error, record_inline_lock_would_block_log_suppressed,
    record_inline_lock_would_block_retry, with_nonblocking_lock_waits, IsolationLevel, LockMode,
    LockWaitTimeoutGuard, StatementSavepoint, TableLockMode, TxnView,
};
pub(crate) use txn::{take_nonblocking_row_wait_target, NonblockingRowWaitTarget};
pub use value::value_to_string;

#[cfg(test)]
pub(crate) use txn::with_durability_sync_failure;

pub(crate) use value::is_numeric_value;
