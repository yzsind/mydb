//! Complete raw diagnostics, independent of the concise engine monitor.

use crate::storage::Database;

fn statement_cache_field(
    project: impl FnOnce(&crate::engine::cache::CacheStats) -> String,
) -> String {
    crate::engine::cache::global_stats()
        .as_ref()
        .map(project)
        .unwrap_or_else(|| "0".to_string())
}

fn plan_cache_field(
    project: impl FnOnce(&crate::engine::cache::PlanCacheStats) -> String,
) -> String {
    crate::engine::cache::global_plan_stats()
        .as_ref()
        .map(project)
        .unwrap_or_else(|| "0".to_string())
}

fn prepared_dml_cache_field(
    project: impl FnOnce(&crate::engine::cache::PlanCacheStats) -> String,
) -> String {
    crate::engine::cache::global_prepared_dml_stats()
        .as_ref()
        .map(project)
        .unwrap_or_else(|| "0".to_string())
}

fn prepared_dml_batch_cache_field(
    project: impl FnOnce(&crate::engine::cache::PlanCacheStats) -> String,
) -> String {
    crate::engine::cache::global_prepared_dml_batch_stats()
        .as_ref()
        .map(project)
        .unwrap_or_else(|| "0".to_string())
}

fn plan_cache_hit_pct_string(stats: &crate::engine::cache::PlanCacheStats) -> String {
    let total = stats.hits.saturating_add(stats.misses);
    if total == 0 {
        "0".to_string()
    } else {
        (stats.hits.saturating_mul(100) / total).to_string()
    }
}

/// Process peak resident bytes (Windows peak working set). Best-effort —
/// returns 0 if the syscall fails. This is the OS ground truth that the
/// engine's `tracked_memory_bytes` roll-up is measured against; the gap
/// between them is untracked memory (allocator overhead, HNSW graphs,
/// per-thread caches, catalog).
fn peak_resident_bytes() -> u64 {
    crate::platform::memory::peak_resident_bytes().unwrap_or(0)
}

// Shared status-row layout. Metric expressions retain their native formatting
// and evaluate once, in declaration order; no dynamic dispatch is introduced.
macro_rules! status_rows {
    ($($name:literal => $value:expr),* $(,)?) => {
        vec![$(($name, $value)),*]
    };
}

pub(super) fn collect(db: &Database) -> Vec<(String, String)> {
    let status = db.storage_status();
    let exec_permit = crate::util::exec_permit::status_snapshot();
    let async_row_lock = crate::mysql::session::async_row_lock_retry_stats();
    let async_row_timing = db.async_row_lock_timing_stats();
    let ddl_copy = db.ddl_copy_perf_snapshot();
    let online_constraint = db.online_constraint_perf_snapshot();
    let load_shadow = db.load_shadow_perf_snapshot();
    let index_build_extended = crate::storage::index_build_extended_perf_snapshot();
    let connection_metrics = crate::mysql::server::connection_metrics();
    let (controlled_current, controlled_peak, controlled_categories, controlled_rejections) =
        crate::engine::executor::mem_budget::controlled_memory_snapshot();
    let known_cache_bytes = crate::engine::cache_registry::collect()
        .into_iter()
        .filter(|cache| {
            cache.memory_accounting
                == crate::engine::cache_registry::CacheMemoryAccounting::IndependentEstimate
                && cache.scope == "process"
        })
        .fold(0u128, |sum, cache| {
            sum.saturating_add(cache.bytes.unwrap_or(0) as u128)
        });
    let admission_reclaim = crate::engine::executor::mem_budget::admission_reclaim_snapshot();
    let idle_reclaim = crate::engine::memory_reclaimer::snapshot();
    let controlled_rejections_by_category =
        crate::engine::executor::mem_budget::controlled_memory_rejections_by_category();
    let work_area = crate::engine::executor::work_area::snapshot();
    let rows = status_rows! {
        "recyclebin_pressure" => db.recyclebin_status()[0].to_string(),
        "recyclebin_pressure_purges" => db.recyclebin_status()[1].to_string(),
        "recyclebin_cleanup_failures" => db.recyclebin_status()[2].to_string(),
        "durability" => status.durability.to_string(),
        "ddl_copy_total" => ddl_copy[0].to_string(),
        "ddl_copy_scan_us" => ddl_copy[1].to_string(),
        "ddl_copy_convert_us" => ddl_copy[2].to_string(),
        "ddl_copy_index_us" => ddl_copy[3].to_string(),
        "ddl_copy_replay_us" => ddl_copy[4].to_string(),
        "ddl_copy_swap_us" => ddl_copy[5].to_string(),
        "ddl_copy_source_bytes" => ddl_copy[6].to_string(),
        "ddl_copy_shadow_bytes" => ddl_copy[7].to_string(),
        "ddl_copy_spill_bytes" => ddl_copy[8].to_string(),
        "ddl_copy_direct_heap_builds" => ddl_copy[9].to_string(),
        "ddl_copy_peak_batch_bytes" => ddl_copy[10].to_string(),
        "ddl_copy_index_feed_us" => ddl_copy[11].to_string(),
        "ddl_copy_index_build_us" => ddl_copy[12].to_string(),
        "ddl_final_barrier_count" => ddl_copy[13].to_string(),
        "ddl_final_barrier_total_us" => ddl_copy[14].to_string(),
        "ddl_final_barrier_max_us" => ddl_copy[15].to_string(),
        "ddl_final_barrier_le_1ms" => ddl_copy[16].to_string(),
        "ddl_final_barrier_le_10ms" => ddl_copy[17].to_string(),
        "ddl_final_barrier_le_100ms" => ddl_copy[18].to_string(),
        "ddl_final_barrier_le_1s" => ddl_copy[19].to_string(),
        "ddl_final_barrier_gt_1s" => ddl_copy[20].to_string(),
        "ddl_copy_barrier_wait_us" => ddl_copy[21].to_string(),
        "ddl_copy_lock_hold_us" => ddl_copy[22].to_string(),
        "ddl_copy_lock_hold_max_us" => ddl_copy[23].to_string(),
        "ddl_copy_final_tail_rows" => ddl_copy[24].to_string(),
        "load_shadow_hits" => load_shadow[0].to_string(),
        "load_shadow_fallbacks" => load_shadow[1].to_string(),
        "load_shadow_source_bytes" => load_shadow[2].to_string(),
        "load_shadow_heap_bytes" => load_shadow[3].to_string(),
        "load_shadow_index_bytes" => load_shadow[4].to_string(),
        "load_shadow_heap_write_us" => load_shadow[5].to_string(),
        "load_shadow_index_feed_us" => load_shadow[6].to_string(),
        "load_shadow_index_finish_us" => load_shadow[7].to_string(),
        "load_shadow_sync_swap_us" => load_shadow[8].to_string(),
        "load_shadow_parse_encode_us" => load_shadow[9].to_string(),
        "load_direct_decode_rows" => load_shadow[10].to_string(),
        "load_direct_decode_fallback_rows" => load_shadow[11].to_string(),
        "load_auto_increment_fast_batches" => load_shadow[12].to_string(),
        "load_auto_increment_fast_rows" => load_shadow[13].to_string(),
        "load_auto_increment_assign_batches" => load_shadow[14].to_string(),
        "load_auto_increment_assign_rows" => load_shadow[15].to_string(),
        "current_lsn" => status.current_lsn.to_string(),
        "durable_lsn" => status.durable_lsn.to_string(),
        // WAL improvement plan Phase 4 observability:
        // - sync method actually in force (platform/durability-resolved + overrides)
        // - durable lag = appended-but-not-yet-fsynced LSN distance
        "wal_sync_method" => crate::wal::resolved_wal_sync_method().as_str().to_string(),
        "wal_durable_lag_lsn" => status .current_lsn .saturating_sub(status.durable_lsn) .to_string(),
        "wal_bytes" => status.wal_bytes.to_string(),
        "wal_logical_records" => status.wal_logical_records.to_string(),
        "wal_page_fallback_records" => status.wal_page_fallback_records.to_string(),
        "wal_index_batch_records" => status.wal_index_batch_records.to_string(),
        "wal_heap_payload_bytes" => status.wal_heap_payload_bytes.to_string(),
        "wal_index_payload_bytes" => status.wal_index_payload_bytes.to_string(),
        "wal_page_payload_bytes" => status.wal_page_payload_bytes.to_string(),
        "wal_heap_fpi_payload_bytes" => status.wal_heap_fpi_payload_bytes.to_string(),
        "reduced_redo_transactions" => status.reduced_redo_transactions.to_string(),
        "reduced_redo_skipped_records" => status.reduced_redo_skipped_records.to_string(),
        "reduced_redo_skipped_payload_bytes" => status.reduced_redo_skipped_payload_bytes.to_string(),
        "reduced_redo_fallback_records" => status.reduced_redo_fallback_records.to_string(),
        "reduced_redo_force_flushes" => status.reduced_redo_force_flushes.to_string(),
        "reduced_redo_force_flush_us" => status.reduced_redo_force_flush_us.to_string(),
        "wal_index_page_payload_bytes" => status.wal_index_page_payload_bytes.to_string(),
        "wal_other_page_payload_bytes" => status.wal_other_page_payload_bytes.to_string(),
        "wal_sync_total_us" => status.wal_sync_total_us.to_string(),
        "wal_sync_avg_us" => status.wal_sync_avg_us.to_string(),
        "wal_sync_max_us" => status.wal_sync_max_us.to_string(),
        "wal_rotation_count" => status.wal_rotation_count.to_string(),
        "wal_rotation_critical_total_us" => status.wal_rotation_critical_total_us.to_string(),
        "wal_rotation_critical_max_us" => status.wal_rotation_critical_max_us.to_string(),
        "wal_pending_sealed_sync" => status.wal_pending_sealed_sync.to_string(),
        "checkpoint_lsn" => status.checkpoint_lsn.to_string(),
        "checkpoint_age_lsn" => status.checkpoint_age_lsn.to_string(),
        "checkpoints_total" => status.checkpoints_total.to_string(),
        "checkpoint_failures_total" => status.checkpoint_failures_total.to_string(),
        "checkpoint_preflush_failures_total" => status.checkpoint_preflush_failures_total.to_string(),
        "checkpoint_redo_lsn" => status.checkpoint_redo_lsn.to_string(),
        "checkpoint_redo_age_lsn" => status.checkpoint_redo_age_lsn.to_string(),
        "last_checkpoint_dirty_pages" => status.last_checkpoint_dirty_pages.to_string(),
        "last_checkpoint_wal_reclaimed_bytes" => status.last_checkpoint_wal_reclaimed_bytes.to_string(),
        "checkpoint_wal_reclaimed_bytes_total" => status.checkpoint_wal_reclaimed_bytes_total.to_string(),
        "restartpoints_total" => status.restartpoints_total.to_string(),
        "replica_applied_lsn" => status.replica_applied_lsn.to_string(),
        "last_checkpoint_trigger" => status.last_checkpoint_trigger.to_string(),
        "last_checkpoint_kind" => status.last_checkpoint_kind.to_string(),
        "last_checkpoint_snapshot_us" => status.last_checkpoint_snapshot_us.to_string(),
        "last_checkpoint_flush_us" => status.last_checkpoint_flush_us.to_string(),
        "last_checkpoint_sync_us" => status.last_checkpoint_sync_us.to_string(),
        "last_checkpoint_compact_us" => status.last_checkpoint_compact_us.to_string(),
        "last_checkpoint_duration_us" => status.last_checkpoint_duration_us.to_string(),
        "checkpoint_duration_total_us" => status.checkpoint_duration_total_us.to_string(),
        "checkpoint_duration_max_us" => status.checkpoint_duration_max_us.to_string(),
        "buffer_hits" => status.buffer_hits.to_string(),
        "buffer_misses" => status.buffer_misses.to_string(),
        "buffer_hit_pct" => status.buffer_hit_pct.to_string(),
        "dirty_heap_pages" => status.dirty_heap_pages.to_string(),
        "dirty_index_pages" => status.dirty_index_pages.to_string(),
        "buffer_pool_used_pages" => status.buffer_pool_used_pages.to_string(),
        "buffer_pool_dirty_pages" => status.buffer_pool_dirty_pages.to_string(),
        "buffer_pool_freeable_pages" => status.buffer_pool_freeable_pages.to_string(),
        "buffer_pool_evictions" => status.buffer_pool_evictions.to_string(),
        "buffer_pool_flushes" => status.buffer_pool_flushes.to_string(),
        "buffer_pool_flush_errors" => status.buffer_pool_flush_errors.to_string(),
        "buffer_pool_background_io_errors" => status.buffer_pool_background_io_errors.to_string(),
        "buffer_pool_pinned_pages" => status.buffer_pool_pinned_pages.to_string(),
        "buffer_pool_waits" => status.buffer_pool_waits.to_string(),
        "buffer_pool_wait_ms" => status.buffer_pool_wait_ms.to_string(),
        "buffer_pool_background_flushes" => status.buffer_pool_background_flushes.to_string(),
        "buffer_pool_shards" => status.buffer_pool_shards.to_string(),
        "buffer_pool_read_ios" => status.buffer_pool_read_ios.to_string(),
        "buffer_pool_read_ios_heap" => status.buffer_pool_read_ios_by_kind[0].to_string(),
        "buffer_pool_read_ios_index" => status.buffer_pool_read_ios_by_kind[1].to_string(),
        "buffer_pool_read_ios_overflow" => status.buffer_pool_read_ios_by_kind[2].to_string(),
        "buffer_pool_read_ios_vector" => status.buffer_pool_read_ios_by_kind[3].to_string(),
        "buffer_pool_read_ios_zone_map" => status.buffer_pool_read_ios_by_kind[4].to_string(),
        "buffer_pool_read_syscalls" => status.buffer_pool_read_syscalls.to_string(),
        "buffer_pool_batch_read_ops" => status.buffer_pool_batch_read_ops.to_string(),
        "buffer_pool_batch_read_pages" => status.buffer_pool_batch_read_pages.to_string(),
        "buffer_pool_write_ios" => status.buffer_pool_write_ios.to_string(),
        "buffer_pool_write_syscalls" => status.buffer_pool_write_syscalls.to_string(),
        "buffer_pool_prefetches" => status.buffer_pool_prefetches.to_string(),
        "buffer_pool_prefetch_hits" => status.buffer_pool_prefetch_hits.to_string(),
        "buffer_pool_prefetch_useful" =>
            // A prefetch hit means the prefetched page was subsequently used;
            // both labels intentionally report that same physical event.
            status.buffer_pool_prefetch_hits.to_string(),
        "buffer_pool_prefetch_wasted" => status.buffer_pool_prefetch_wasted.to_string(),
        "buffer_pool_prefetch_demand_waits" => status.buffer_pool_prefetch_demand_waits.to_string(),
        "buffer_pool_eviction_scans" => status.buffer_pool_eviction_scans.to_string(),
        "buffer_pool_io_threads" => status.buffer_pool_io_threads.to_string(),
        "buffer_pool_prefetch_pages" => status.buffer_pool_prefetch_pages.to_string(),
        "buffer_pool_small_scan_pct" => status.buffer_pool_small_scan_pct.to_string(),
        "buffer_pool_small_scan_threshold_pages" => status.buffer_pool_small_scan_threshold_pages.to_string(),
        "buffer_pool_small_scan_accesses" => status.buffer_pool_small_scan_accesses.to_string(),
        "buffer_pool_small_scan_cold_accesses" => status.buffer_pool_small_scan_cold_accesses.to_string(),
        "buffer_pool_small_scan_promotions" => status.buffer_pool_small_scan_promotions.to_string(),
        "buffer_pool_small_scan_read_ios" => status.buffer_pool_small_scan_read_ios.to_string(),
        // P0-1 - extended buffer pool observability.
        "buffer_pool_read_ios_normal" => status.buffer_pool_read_ios_normal.to_string(),
        "buffer_pool_read_ios_scan" => status.buffer_pool_read_ios_scan.to_string(),
        "buffer_pool_read_ios_prefetch" => status.buffer_pool_read_ios_prefetch.to_string(),
        "buffer_pool_prefetch_queue_drops" => status.buffer_pool_prefetch_queue_drops.to_string(),
        "buffer_pool_prefetch_duplicate_drops" => status.buffer_pool_prefetch_duplicate_drops.to_string(),
        "buffer_pool_prefetch_pressure_drops" => status.buffer_pool_prefetch_pressure_drops.to_string(),
        "buffer_pool_prefetch_queue_full_drops" => status.buffer_pool_prefetch_queue_full_drops.to_string(),
        "buffer_pool_prefetch_stale_drops" => status.buffer_pool_prefetch_stale_drops.to_string(),
        "buffer_pool_prefetch_queue_wait_avg_us" => status.buffer_pool_prefetch_queue_wait_avg_us.to_string(),
        "buffer_pool_prefetch_lead_avg_us" => status.buffer_pool_prefetch_lead_avg_us.to_string(),
        "buffer_pool_ensure_capacity_busy_us" => status.buffer_pool_ensure_capacity_busy_us.to_string(),
        "buffer_pool_file_handle_wait_ns" => status.buffer_pool_file_handle_wait_ns.to_string(),
        "buffer_pool_dirty_ratio_samples" => status.buffer_pool_dirty_ratio_samples.to_string(),
        "buffer_pool_dirty_ratio_histogram" =>
            format!( "0-12:{} 12-25:{} 25-37:{} 37-50:{} 50-62:{} 62-75:{} 75-87:{} 87-100:{}", status.buffer_pool_dirty_ratio_histogram[0], status.buffer_pool_dirty_ratio_histogram[1], status.buffer_pool_dirty_ratio_histogram[2], status.buffer_pool_dirty_ratio_histogram[3], status.buffer_pool_dirty_ratio_histogram[4], status.buffer_pool_dirty_ratio_histogram[5], status.buffer_pool_dirty_ratio_histogram[6], status.buffer_pool_dirty_ratio_histogram[7], ),
        "buffer_pool_shard_imbalance_pct" => status.buffer_pool_shard_imbalance_pct.to_string(),
        "buffer_pool_shard_max_hits" => status.buffer_pool_shard_max_hits.to_string(),
        "buffer_pool_shard_min_hits" => status.buffer_pool_shard_min_hits.to_string(),
        "buffer_pool_scan_origin_share_pct" => status.buffer_pool_scan_origin_share_pct.to_string(),
        "buffer_pool_scan_origin_evictions" => status.buffer_pool_scan_origin_evictions.to_string(),
        "buffer_pool_cold_recycle_hits" => status.buffer_pool_cold_recycle_hits.to_string(),
        "buffer_pool_cold_recycle_fallbacks" => status.buffer_pool_cold_recycle_fallbacks.to_string(),
        "buffer_pool_raw_scan_pages" => status.buffer_pool_raw_scan_pages.to_string(),
        "buffer_pool_raw_page_allocations" => status.buffer_pool_raw_page_allocations.to_string(),
        "buffer_pool_raw_page_reuse_hits" => status.buffer_pool_raw_page_reuse_hits.to_string(),
        "buffer_pool_raw_page_reuse_misses" => status.buffer_pool_raw_page_reuse_misses.to_string(),
        "buffer_pool_recycled_raw_pages" => status.buffer_pool_recycled_raw_pages.to_string(),
        "buffer_pool_pages_heap" => status.buffer_pool_pages_heap.to_string(),
        "buffer_pool_pages_index" => status.buffer_pool_pages_index.to_string(),
        "buffer_pool_pages_lob" => status.buffer_pool_pages_lob.to_string(),
        "buffer_pool_pages_vector" => status.buffer_pool_pages_vector.to_string(),
        "buffer_pool_pages_zone_map" => status.buffer_pool_pages_zone_map.to_string(),
        "buffer_pool_quota_evictions" => status.buffer_pool_quota_evictions.to_string(),
        "buffer_pool_file_syncs" => status.buffer_pool_file_syncs.to_string(),
        "buffer_pool_file_sync_skipped" => status.buffer_pool_file_sync_skipped.to_string(),
        "buffer_pool_flush_data_cache_pages" => status.buffer_pool_flush_data_cache_pages.to_string(),
        "buffer_pool_flush_data_cache_us" => status.buffer_pool_flush_data_cache_us.to_string(),
        "buffer_pool_flush_data_cache_short_circuits" =>
            status .buffer_pool_flush_data_cache_short_circuits .to_string(),
        "buffer_pool_evict_sync_dirty_flushes" => status.buffer_pool_evict_sync_dirty_flushes.to_string(),
        "buffer_pool_evict_sync_dirty_flush_us" => status.buffer_pool_evict_sync_dirty_flush_us.to_string(),
        "buffer_pool_cooldown_deferred_flushes" => status.buffer_pool_cooldown_deferred_flushes.to_string(),
        "buffer_pool_headroom_flushes" => status.buffer_pool_headroom_flushes.to_string(),
        "buffer_pool_checkpoint_flush_pages" => status.buffer_pool_checkpoint_flush_pages.to_string(),
        "buffer_pool_checkpoint_flush_us" => status.buffer_pool_checkpoint_flush_us.to_string(),
        "buffer_pool_checkpoint_preflush_pages" => status.buffer_pool_checkpoint_preflush_pages.to_string(),
        "buffer_pool_checkpoint_preflush_us" => status.buffer_pool_checkpoint_preflush_us.to_string(),
        "buffer_pool_prefetch_queue_depth_peak" => status.buffer_pool_prefetch_queue_depth_peak.to_string(),
        "buffer_pool_oldest_dirty_lsn" => status.buffer_pool_oldest_dirty_lsn.to_string(),
        "buffer_pool_redo_age_target_lsn" => status.buffer_pool_redo_age_target_lsn.to_string(),
        "buffer_pool_deferred_drain_requests" => status.buffer_pool_deferred_drain_requests.to_string(),
        "bulk_index_direct_builds" => status.bulk_index_direct_builds.to_string(),
        "bulk_index_direct_pages" => status.bulk_index_direct_pages.to_string(),
        "bulk_index_direct_us" => status.bulk_index_direct_us.to_string(),
        "bulk_index_buffered_builds" => status.bulk_index_buffered_builds.to_string(),
        "bulk_index_buffered_pages" => status.bulk_index_buffered_pages.to_string(),
        "bulk_index_buffered_us" => status.bulk_index_buffered_us.to_string(),
        "index_build_requested_workers" => status.index_build_requested_workers.to_string(),
        "index_build_effective_workers" => status.index_build_effective_workers.to_string(),
        "index_build_parallel_builds" => status.index_build_parallel_builds.to_string(),
        "index_build_serial_builds" => status.index_build_serial_builds.to_string(),
        "index_build_degree_downgrades" => status.index_build_degree_downgrades.to_string(),
        "online_constraint_lock_hold_us" => online_constraint[0].to_string(),
        "online_constraint_lock_hold_max_us" => online_constraint[1].to_string(),
        "online_constraint_scanned_rows" => online_constraint[2].to_string(),
        "online_constraint_replayed_rows" => online_constraint[3].to_string(),
        "online_constraint_final_tail_max" => online_constraint[4].to_string(),
        "online_constraint_index_merge_accepted" => online_constraint[5].to_string(),
        "online_constraint_index_merge_fallbacks" => online_constraint[6].to_string(),
        "online_constraint_index_merge_entries" => online_constraint[7].to_string(),
        "online_constraint_baseline_builds" => online_constraint[8].to_string(),
        "online_constraint_index_merge_parallel_runs" => online_constraint[9].to_string(),
        "online_constraint_index_merge_workers_peak" => online_constraint[10].to_string(),
        "online_constraint_baseline_parallel_runs" => online_constraint[11].to_string(),
        "online_constraint_baseline_workers_peak" => online_constraint[12].to_string(),
        "online_index_prepare_us" => status.online_index_prepare_us.to_string(),
        "online_index_replay_us" => status.online_index_replay_us.to_string(),
        "online_index_validation_us" => status.online_index_validation_us.to_string(),
        "online_index_publish_us" => status.online_index_publish_us.to_string(),
        "online_index_barrier_wait_us" => status.online_index_barrier_wait_us.to_string(),
        "online_index_tail_entries_peak" => status.online_index_tail_entries_peak.to_string(),
        "online_index_tail_bytes_peak" => status.online_index_tail_bytes_peak.to_string(),
        "online_index_log_bytes_peak" => status.online_index_log_bytes_peak.to_string(),
        "index_build_scan_us" => status.index_build_scan_us.to_string(),
        "index_build_sort_us" => status.index_build_sort_us.to_string(),
        "index_build_leaf_us" => status.index_build_leaf_us.to_string(),
        "index_build_internal_us" => status.index_build_internal_us.to_string(),
        "index_build_fsync_us" => status.index_build_fsync_us.to_string(),
        "index_build_rows" => status.index_build_rows.to_string(),
        "index_build_heap_scans" => status.index_build_heap_scans.to_string(),
        "index_build_merge_us" => status.index_build_merge_us.to_string(),
        "index_build_spill_write_us" => status.index_build_spill_write_us.to_string(),
        "index_build_spill_read_us" => status.index_build_spill_read_us.to_string(),
        "index_build_peak_active_workers" => status.index_build_peak_active_workers.to_string(),
        "index_build_dictionary_builds" => index_build_extended[0].to_string(),
        "index_build_dictionary_rows" => index_build_extended[1].to_string(),
        "index_build_encoded_runs" => index_build_extended[2].to_string(),
        "index_build_scratch_granted_peak_bytes" => index_build_extended[3].to_string(),
        "index_build_queue_wait_us" => index_build_extended[4].to_string(),
        "index_build_merge_passes" => index_build_extended[5].to_string(),
        "index_build_stream_us" => index_build_extended[6].to_string(),
        "index_build_spill_read_bytes" => index_build_extended[7].to_string(),
        "index_build_spill_write_bytes" => index_build_extended[8].to_string(),
        "index_build_pipeline_workers_peak" => index_build_extended[9].to_string(),
        "buffer_pool_dirty_oldest_len" => status.buffer_pool_dirty_oldest_len.to_string(),
        "spill_bytes_written" => status.spill_bytes_written.to_string(),
        "spill_bytes_read" => status.spill_bytes_read.to_string(),
        "catalog_txn_commits" => status.catalog_txn_commits.to_string(),
        "catalog_txn_rollbacks" => status.catalog_txn_rollbacks.to_string(),
        "catalog_mutated_rows" => status.catalog_mutated_rows.to_string(),
        "catalog_epoch" => status.catalog_epoch.to_string(),
        "catalog_cache_limit_bytes" => status.catalog_cache_limit_bytes.to_string(),
        "catalog_cache_bytes" => status.catalog_cache_bytes.to_string(),
        "catalog_cache_entries" => status.catalog_cache_entries.to_string(),
        "dml_insert_rows" => status.dml_insert_rows.to_string(),
        "dml_update_rows" => status.dml_update_rows.to_string(),
        "dml_delete_rows" => status.dml_delete_rows.to_string(),
        "dml_ignore_warnings" => status.dml_ignore_warnings.to_string(),
        "dml_rollback_inserted_rows" => status.dml_rollback_inserted_rows.to_string(),
        "dml_lob_leaks_recovered" => status.dml_lob_leaks_recovered.to_string(),
        "dml_upsert_fast_path_hits" => status.dml_upsert_fast_path_hits.to_string(),
        "dml_upsert_batch_attempts" => status.dml_upsert_batch_attempts.to_string(),
        "dml_upsert_batch_rows" => status.dml_upsert_batch_rows.to_string(),
        "dml_upsert_batch_conflict_rows" => status.dml_upsert_batch_conflict_rows.to_string(),
        "dml_upsert_batch_fallback_rows" => status.dml_upsert_batch_fallback_rows.to_string(),
        "dml_upsert_batch_race_rollbacks" => status.dml_upsert_batch_race_rollbacks.to_string(),
        "dml_fk_cycle_short_circuits" => status.dml_fk_cycle_short_circuits.to_string(),
        "load_data_active" => status.load_data_active.to_string(),
        "load_data_rows_loaded" => status.load_data_rows_loaded.to_string(),
        // M1/M6 (LOB) — chain counts and bytes for large-field chain reuse / externalization.
        "dml_lob_chains_reused" => status.dml_lob_chains_reused.to_string(),
        "dml_lob_bytes_reused" => status.dml_lob_bytes_reused.to_string(),
        "dml_lob_chains_externalized" => status.dml_lob_chains_externalized.to_string(),
        "dml_lob_bytes_externalized" => status.dml_lob_bytes_externalized.to_string(),
        "dml_lob_pages_shrunk" => status.dml_lob_pages_shrunk.to_string(),
        "dml_lob_leaks_unrecovered" => status.dml_lob_leaks_unrecovered.to_string(),
        "active_transactions" => status.active_transactions.to_string(),
        "oldest_snapshot_scn" => status.oldest_snapshot_scn.to_string(),
        "oldest_active_txn_ms" => status.oldest_active_txn_ms.to_string(),
        "latch_waits" => status.latch_waits.to_string(),
        "lock_waits" => status.lock_waits.to_string(),
        "active_lock_waits" => status.active_lock_waits.to_string(),
        "oldest_lock_wait_ms" => status.oldest_lock_wait_ms.to_string(),
        "deadlocks" => status.deadlocks.to_string(),
        "lock_wait_ms" => status.lock_wait_ms.to_string(),
        "xa_prepared_active" => db.xa_status_counters().0.to_string(),
        "xa_commits_total" => db.xa_status_counters().1.to_string(),
        "xa_rollbacks_total" => db.xa_status_counters().2.to_string(),
        // rowid-6bytes M2 — slot reclaim. `slot_reclaim_total` counts
        // tombstones (`ROW_FLAG_DELETED`) flipped to `ROW_FLAG_FREE`
        // since startup; `slot_reclaim_runs` is the number of undo
        // purge passes that did so; `slot_reclaim_errors` is the
        // per-slot reclaim failures (heap open / WAL append). Failures
        // leave the slot in DELETED state — correctness is preserved,
        // only slot reuse stalls.
        "deferred_index_purge_rows" => db.deferred_index_purge_metrics().0.to_string(),
        "deferred_index_purge_us" => db.deferred_index_purge_metrics().1.to_string(),
        "slot_reclaim_total" => db.slot_reclaim_status_counters().0.to_string(),
        "slot_reclaim_runs" => db.slot_reclaim_status_counters().1.to_string(),
        "slot_reclaim_errors" => db.slot_reclaim_status_counters().2.to_string(),
        // P0/4 — slot reclaim retry queue. Failed pairs sit in the
        // quarantine until they either succeed on retry or hit the
        // per-pair attempt cap, at which point they count as
        // permanent failures.
        "slot_reclaim_quarantine_size" => db.slot_reclaim_quarantine_counters().0.to_string(),
        "slot_reclaim_permanent_failures" => db.slot_reclaim_quarantine_counters().1.to_string(),
        // P2/14 — undo I/O latency observability. `_samples` count the
        // per-frame buffered or write-through path; `_ns_total` is the
        // cumulative wall-clock the undo path spent inside that I/O
        // (sum across all worker threads since process start); `_p50`
        // / `_p99` are log-bucket approximations (factor of 10).
        // `scan_total_ns` is the startup-recovery scan cost (once
        // per process unless re-opened in tests).
        "undo_io_write_frame_samples" => db.undo_io_latency_snapshot().0.to_string(),
        "undo_io_write_frame_ns_total" => db.undo_io_latency_snapshot().1.to_string(),
        "undo_io_write_frame_ns_p50" => db.undo_io_latency_snapshot().2.to_string(),
        "undo_io_write_frame_ns_p99" => db.undo_io_latency_snapshot().3.to_string(),
        "undo_io_flush_samples" => db.undo_io_latency_snapshot().4.to_string(),
        "undo_io_flush_ns_total" => db.undo_io_latency_snapshot().5.to_string(),
        "undo_io_flush_ns_p50" => db.undo_io_latency_snapshot().6.to_string(),
        "undo_io_flush_ns_p99" => db.undo_io_latency_snapshot().7.to_string(),
        "undo_io_scan_ns_total" => db.undo_io_latency_snapshot().8.to_string(),
        "undo_disk_cap_softfail_warns_emitted" => db.undo_disk_cap_softfail_warns_emitted().to_string(),
        // P1/6 — number of rollback-segment shards in use. Equal to
        // `MYDB_UNDO_WRITE_SHARDS` (default 8); operators read this
        // to confirm sharding is live before tuning the env.
        "undo_write_shards" => db.undo_shard_count().to_string(),
        // Server-side cursor counters (COM_STMT_EXECUTE with
        // CURSOR_TYPE_READ_ONLY). `_active` is the snapshot count from
        // the registry; the totals are monotonic counters since
        // process start. `_evicted_idle_total` and the two `rejected_*`
        // counters are the protective signals — non-zero values mean
        // either the idle-timeout fired or the per-session / global
        // cap was hit, both of which are worth investigating.
        "mydb_cursors_active" => crate::engine::cursor_registry::global() .active_count() .to_string(),
        "mydb_cursors_opened_total" =>
            crate::engine::cursor_registry::global() .stats() .opened_total .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_cursors_closed_total" =>
            crate::engine::cursor_registry::global() .stats() .closed_total .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_cursors_rows_fetched_total" =>
            crate::engine::cursor_registry::global() .stats() .rows_fetched_total .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_cursors_evicted_idle_total" =>
            crate::engine::cursor_registry::global() .stats() .evicted_idle_total .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_cursors_rejected_session_cap_total" =>
            crate::engine::cursor_registry::global() .stats() .rejected_session_cap .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_cursors_rejected_global_cap_total" =>
            crate::engine::cursor_registry::global() .stats() .rejected_global_cap .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        // SQL-level cursors opened by `DECLARE <name> CURSOR FOR ...`.
        // Same shape as the prepared-cursor counters above; presented
        // separately so an operator can tell which surface is generating
        // pressure.
        "mydb_named_cursors_active" => crate::engine::cursor_registry::named_active_count().to_string(),
        "mydb_named_cursors_opened_total" =>
            crate::engine::cursor_registry::named_stats_snapshot() .opened_total .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_named_cursors_closed_total" =>
            crate::engine::cursor_registry::named_stats_snapshot() .closed_total .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_named_cursors_rows_fetched_total" =>
            crate::engine::cursor_registry::named_stats_snapshot() .rows_fetched_total .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_named_cursors_evicted_idle_total" =>
            crate::engine::cursor_registry::named_stats_snapshot() .evicted_idle_total .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_named_cursors_rejected_session_cap_total" =>
            crate::engine::cursor_registry::named_stats_snapshot() .rejected_session_cap .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_named_cursors_rejected_global_cap_total" =>
            crate::engine::cursor_registry::named_stats_snapshot() .rejected_global_cap .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "group_commit_us" => status.group_commit_us.to_string(),
        "group_commit_adaptive" => u8::from(status.group_commit_adaptive).to_string(),
        "group_commit_batches" => status.group_commit_batches.to_string(),
        "group_commit_waiters" => status.group_commit_waiters.to_string(),
        "avg_group_commit_size" => status.avg_group_commit_size.to_string(),
        "durability_worker_enabled" => u8::from(status.durability_worker_enabled).to_string(),
        "durability_queue_depth" => status.durability_queue_depth.to_string(),
        "durability_queue_depth_peak" => status.durability_queue_depth_peak.to_string(),
        "durability_batches" => status.durability_batches.to_string(),
        "durability_requests" => status.durability_requests.to_string(),
        "durability_batch_max" => status.durability_batch_max.to_string(),
        "group_commit_leader_wait_total_us" => status.group_commit_leader_wait_total_us.to_string(),
        "group_commit_follower_wait_total_us" => status.group_commit_follower_wait_total_us.to_string(),
        "group_commit_sync_op_total_us" => status.group_commit_sync_op_total_us.to_string(),
        "group_commit_leader_wait_max_us" => status.group_commit_leader_wait_max_us.to_string(),
        "group_commit_follower_wait_max_us" => status.group_commit_follower_wait_max_us.to_string(),
        "group_commit_sync_op_max_us" => status.group_commit_sync_op_max_us.to_string(),
        "group_commit_high_water_released" => status.group_commit_high_water_released.to_string(),
        "group_commit_recent_avg_batch_size" =>
            format!( "{}.{:02}", status.group_commit_recent_avg_batch_size_x100 / 100, status.group_commit_recent_avg_batch_size_x100 % 100, ),
        "group_commit_batch_p50" => status.group_commit_batch_p50.to_string(),
        "group_commit_batch_p99" => status.group_commit_batch_p99.to_string(),
        "wal_syncs" => status.wal_syncs.to_string(),
        "wal_segment_bytes" => status.wal_segment_bytes.to_string(),
        "buffer_pool_pages" => status.buffer_pool_pages.to_string(),
        "row_migrations" => status.row_migrations.to_string(),
        // Phase 4c: process-wide ratios. `row_migrations_total` is the
        // aggregate across every heap (matches `row_migrations` for the
        // engine status's "current" heap; differs when the executor
        // touched several tables). `update_in_place` / `update_migrate`
        // tell us how often UPDATE landed in-place vs grew the row out
        // of its page. PCTFREE is good when migrate stays near zero.
        "row_migrations_total" => crate::storage::row_migrations_total().to_string(),
        "update_in_place_total" => crate::storage::update_in_place_total().to_string(),
        "update_migrate_total" => crate::storage::update_migrate_total().to_string(),
        "update_delta_hits" => crate::storage::update_delta_hits().to_string(),
        "update_delta_misses" => crate::storage::update_delta_misses().to_string(),
        "undo_bytes" => status.undo_bytes.to_string(),
        "undo_records" => status.undo_records.to_string(),
        "undo_retention_ms" => status.undo_retention_ms.to_string(),
        "temporal_earliest_available_unix_us" => status.temporal_earliest_available_unix_us.to_string(),
        "temporal_commit_ledger_bytes" => status.temporal_commit_ledger_bytes.to_string(),
        "temporal_history_index_bytes" => status.temporal_history_index_bytes.to_string(),
        "temporal_history_index_entries" => status.temporal_history_index_entries.to_string(),
        "temporal_history_compactions" => status.temporal_history_compactions.to_string(),
        "temporal_history_last_compaction_unix_us" =>
            status.temporal_history_last_compaction_unix_us.to_string(),
        "temporal_queries" => status.temporal_queries.to_string(),
        "temporal_history_index_hits" => status.temporal_history_index_hits.to_string(),
        "temporal_heap_fallbacks" => status.temporal_heap_fallbacks.to_string(),
        "temporal_hlc_rollbacks" => status.temporal_hlc_rollbacks.to_string(),
        "undo_purged_records" => status.undo_purged_records.to_string(),
        "plan_cache_hits" => plan_cache_field(|s| s.hits.to_string()),
        "plan_cache_misses" => plan_cache_field(|s| s.misses.to_string()),
        "plan_cache_hit_pct" => plan_cache_field(plan_cache_hit_pct_string),
        "txn_admission_waits" => exec_permit.txn_admission_waits.to_string(),
        "txn_admission_wait_us" => exec_permit.txn_admission_wait_us.to_string(),
        "mixed_admission_waits" => exec_permit.mixed_admission_waits.to_string(),
        "mixed_admission_wait_us" => exec_permit.mixed_admission_wait_us.to_string(),
        "parallel_read_pressure_entries" => exec_permit.parallel_read_pressure_entries.to_string(),
        "parallel_read_pressure_suppressed_rw_txn" =>
            exec_permit .parallel_read_pressure_suppressed_rw_txn .to_string(),
        "parallel_read_pressure_active" => exec_permit.parallel_read_pressure_active.to_string(),
        "parallel_read_gate_waits" => exec_permit.parallel_read_gate_waits.to_string(),
        "parallel_read_gate_wait_us" => exec_permit.parallel_read_gate_wait_us.to_string(),
        "select_parse_cache_hits" => status.select_parse_cache_hits.to_string(),
        "select_compiled_plan_hits" => status.select_compiled_plan_hits.to_string(),
        "select_operator_compiles" => status.select_operator_compiles.to_string(),
        // Compatibility counters remapped to the prepared-handle L0 memo.
        "prepared_plan_reuses" => status.prepared_plan_reuses.to_string(),
        "prepared_plan_compiles" => status.prepared_plan_compiles.to_string(),
        // Same shape for the prepared DML (INSERT/UPDATE/DELETE) fast path —
        // skip the dispatch wrappers + per-execute statement/session clones.
        "prepared_dml_reuses" => status.prepared_dml_reuses.to_string(),
        "prepared_sparse_batch_hits" => crate::engine::executor::prepared_sparse_batch_hits().to_string(),
        "prepared_dml_compiles" => status.prepared_dml_compiles.to_string(),
        // Cross-session statement (parse + qualify) cache exposed by the
        // engine. Distinct from the prepared-statement counters above.
        "statement_cache_hits" => statement_cache_field(|s| s.hits.to_string()),
        "statement_cache_misses" => statement_cache_field(|s| s.misses.to_string()),
        "statement_cache_stale_evictions" => statement_cache_field(|s| s.stale_evictions.to_string()),
        "statement_cache_lru_evictions" => statement_cache_field(|s| s.lru_evictions.to_string()),
        "statement_cache_size" => statement_cache_field(|s| s.size.to_string()),
        "statement_cache_capacity" => statement_cache_field(|s| s.capacity.to_string()),
        "statement_cache_approx_bytes" => statement_cache_field(|s| s.approx_bytes.to_string()),
        "statement_cache_hit_pct" =>
            statement_cache_field(|s| { let total = s.hits.saturating_add(s.misses); if total == 0 { "0".to_string() } else { (s.hits.saturating_mul(100) / total).to_string() } }),
        // PhysicalPlan cache (COM_QUERY SELECT, no placeholders).
        "physical_plan_cache_hits" => plan_cache_field(|s| s.hits.to_string()),
        "physical_plan_cache_misses" => plan_cache_field(|s| s.misses.to_string()),
        "physical_plan_cache_stale_evictions" => plan_cache_field(|s| s.stale_evictions.to_string()),
        "physical_plan_cache_deps_invalidations" => plan_cache_field(|s| s.deps_invalidations.to_string()),
        "physical_plan_cache_lru_evictions" => plan_cache_field(|s| s.lru_evictions.to_string()),
        "physical_plan_cache_size" => plan_cache_field(|s| s.size.to_string()),
        "physical_plan_cache_capacity" => plan_cache_field(|s| s.capacity.to_string()),
        "physical_plan_cache_approx_bytes" => plan_cache_field(|s| s.approx_bytes.to_string()),
        "physical_plan_cache_hit_pct" => plan_cache_field(plan_cache_hit_pct_string),
        "prepared_dml_cache_hits" => prepared_dml_cache_field(|s| s.hits.to_string()),
        "prepared_dml_cache_misses" => prepared_dml_cache_field(|s| s.misses.to_string()),
        "prepared_dml_cache_stale_evictions" => prepared_dml_cache_field(|s| s.stale_evictions.to_string()),
        "prepared_dml_cache_deps_invalidations" =>
            prepared_dml_cache_field(|s| s.deps_invalidations.to_string()),
        "prepared_dml_cache_lru_evictions" => prepared_dml_cache_field(|s| s.lru_evictions.to_string()),
        "prepared_dml_cache_size" => prepared_dml_cache_field(|s| s.size.to_string()),
        "prepared_dml_cache_capacity" => prepared_dml_cache_field(|s| s.capacity.to_string()),
        "prepared_dml_cache_hit_pct" => prepared_dml_cache_field(plan_cache_hit_pct_string),
        "prepared_dml_batch_cache_hits" => prepared_dml_batch_cache_field(|s| s.hits.to_string()),
        "prepared_dml_batch_cache_misses" => prepared_dml_batch_cache_field(|s| s.misses.to_string()),
        "prepared_dml_batch_cache_stale_evictions" =>
            prepared_dml_batch_cache_field(|s| s.stale_evictions.to_string()),
        "prepared_dml_batch_cache_deps_invalidations" =>
            prepared_dml_batch_cache_field(|s| s.deps_invalidations.to_string()),
        "prepared_dml_batch_cache_lru_evictions" =>
            prepared_dml_batch_cache_field(|s| s.lru_evictions.to_string()),
        "prepared_dml_batch_cache_size" => prepared_dml_batch_cache_field(|s| s.size.to_string()),
        "prepared_dml_batch_cache_capacity" => prepared_dml_batch_cache_field(|s| s.capacity.to_string()),
        "prepared_dml_batch_cache_hit_pct" => prepared_dml_batch_cache_field(plan_cache_hit_pct_string),
        "prepared_dml_inline_would_block_retries" =>
            crate::storage::inline_lock_would_block_retry_count().to_string(),
        "prepared_dml_inline_retry_log_suppressed" =>
            crate::storage::inline_lock_would_block_log_suppressed_count().to_string(),
        "blocking_pool_base_workers" => crate::util::blocking_pool::stack_budget_status().0.to_string(),
        "blocking_pool_max_workers" => crate::util::blocking_pool::stack_budget_status().1.to_string(),
        "blocking_pool_managed_stack_bytes" =>
            crate::util::blocking_pool::stack_budget_status().2.to_string(),
        "async_row_lock_retries" => async_row_lock.0.to_string(),
        "async_row_lock_wakeups" => async_row_lock.1.to_string(),
        "async_row_lock_fallbacks" => async_row_lock.2.to_string(),
        "async_row_lock_wait_count" => async_row_timing.waits.count.to_string(),
        "async_row_lock_wait_total_us" => async_row_timing.waits.total_us.to_string(),
        "async_row_lock_wait_max_us" => async_row_timing.waits.max_us.to_string(),
        "async_row_lock_wake_delay_count" => async_row_timing.wake_delay.count.to_string(),
        "async_row_lock_wake_delay_total_us" => async_row_timing.wake_delay.total_us.to_string(),
        "async_row_lock_wake_delay_max_us" => async_row_timing.wake_delay.max_us.to_string(),
        // Compiled-expression-program cache and fallback counters.
        // Lets operators see how often the row-eval hot path stays on
        // the compiled tree versus falling back to the legacy interpreter,
        // and whether the thread-local program cache is too small.
        "expr_compile_cache_hits" =>
            crate::engine::expr_program::expr_program_stats() .compile_cache_hits .to_string(),
        "expr_compile_cache_misses" =>
            crate::engine::expr_program::expr_program_stats() .compile_cache_misses .to_string(),
        "expr_compile_unsupported" =>
            crate::engine::expr_program::expr_program_stats() .compile_unsupported .to_string(),
        "expr_program_eval_rows" =>
            crate::engine::expr_program::expr_program_stats() .program_eval_rows .to_string(),
        "expr_column_eval_rows" =>
            crate::engine::expr_program::expr_program_stats().column_eval_rows.to_string(),
        "expr_scalar_eval_rows" => {
            let stats = crate::engine::expr_program::expr_program_stats();
            stats.program_eval_rows.saturating_sub(stats.column_eval_rows).to_string()
        },
        "expr_legacy_fallback_rows" =>
            crate::engine::expr_program::expr_program_stats() .legacy_fallback_rows .to_string(),
        // Sub-expression escapes: rows where an unsupported sub-tree dropped
        // to legacy while the rest of the expression stayed compiled. Rising
        // here while `expr_legacy_fallback_rows` falls is the intended shift.
        "expr_legacy_escape_rows" =>
            crate::engine::expr_program::expr_program_stats() .legacy_escape_rows .to_string(),
        "expr_compile_source_sensitive_rejections" =>
            crate::engine::expr_program::expr_program_stats() .source_sensitive_rejections .to_string(),
        "expr_compile_local_escapes" =>
            crate::engine::expr_program::expr_program_stats() .local_escape_compiles .to_string(),
        "expr_compile_unescapable_rejections" =>
            crate::engine::expr_program::expr_program_stats() .unescapable_rejections .to_string(),
        "expr_legacy_fallback_pct" =>
            format!( "{:.2}", crate::engine::expr_program::expr_program_stats().legacy_fallback_pct() ),
        // Function-input compile caches (regex patterns / JSON paths) and
        // the column-metadata intern pool gauge — see
        // information_schema.MYDB_CACHES for the unified view.
        "func_cache_regex_hits" => crate::engine::func_cache::stats().regex_hits.to_string(),
        "func_cache_regex_misses" => crate::engine::func_cache::stats().regex_misses.to_string(),
        "func_cache_json_path_hits" => crate::engine::func_cache::stats() .json_path_hits .to_string(),
        "func_cache_json_path_misses" => crate::engine::func_cache::stats() .json_path_misses .to_string(),
        "col_meta_intern_entries" => crate::engine::executor::col_meta_intern_stats().0.to_string(),
        "col_meta_intern_bytes" => crate::engine::executor::col_meta_intern_stats().1.to_string(),
        "auto_parameterize_mode" =>
            match crate::engine::auto_param::AutoParameterizeMode::from_env() { crate::engine::auto_param::AutoParameterizeMode::Off => "off".into(), crate::engine::auto_param::AutoParameterizeMode::Select => "select".into(), crate::engine::auto_param::AutoParameterizeMode::All => "all".into(), },
        "checkpoint_interval_ms" => status.checkpoint_interval_ms.to_string(),
        "checkpoint_wal_bytes" => status.checkpoint_wal_bytes.to_string(),
        "startup_validate_indexes" => status.startup_validate_indexes.to_string(),
        "last_checkpoint_unix_ms" =>
            status .last_checkpoint_unix_ms .map(|value| value.to_string()) .unwrap_or_default(),
        "last_recovery_summary" => status.last_recovery_summary,
        // M1.5 - Extended observability fields (Oracle-style)
        "snapshot_too_old_count" => status.snapshot_too_old_count.to_string(),
        "read_only_txn_count" => status.read_only_txn_count.to_string(),
        "min_retained_txn_id" => status.min_retained_txn_id.to_string(),
        "index_page_max_trx_shortcut_hits" => status.index_page_max_trx_shortcut_hits.to_string(),
        "index_page_max_trx_shortcut_misses" => status.index_page_max_trx_shortcut_misses.to_string(),
        "undo_peak_bytes" => status.undo_peak_bytes.to_string(),
        "undo_peak_records" => status.undo_peak_records.to_string(),
        "undo_longest_chain" => status.undo_longest_chain.to_string(),
        "undo_retention_pressure" => status.undo_retention_pressure.to_string(),
        "undo_segment_count" => status.undo_segment_count.to_string(),
        "undo_segment_bytes" => status.undo_segment_bytes.to_string(),
        "undo_disk_bytes" => status.undo_disk_bytes.to_string(),
        "undo_reclaimable_bytes" => status.undo_reclaimable_bytes.to_string(),
        "undo_reclaimed_bytes" => status.undo_reclaimed_bytes.to_string(),
        "undo_tablespace_max_bytes" => status.undo_tablespace_max_bytes.to_string(),
        "undo_mem_soft_limit_bytes" => status.undo_mem_soft_limit_bytes.to_string(),
        "undo_mem_cache_limit_bytes" => status.undo_mem_cache_limit_bytes.to_string(),
        "undo_resident_bytes" => status.undo_resident_bytes.to_string(),
        "undo_evicted_records" => status.undo_evicted_records.to_string(),
        "undo_txn_max_bytes" => status.undo_txn_max_bytes.to_string(),
        "undo_largest_txn_id" => status.undo_largest_txn_id.to_string(),
        "undo_largest_txn_bytes" => status.undo_largest_txn_bytes.to_string(),
        "undo_mem_pressure" => status.undo_mem_pressure.to_string(),
        "undo_retention_guarantee" => u8::from(status.undo_retention_guarantee).to_string(),
        "undo_purge_interval_ms" => status.undo_purge_interval_ms.to_string(),
        "undo_purge_batch_records" => status.undo_purge_batch_records.to_string(),
        "undo_purge_runs" => status.undo_purge_runs.to_string(),
        "undo_purge_errors" => status.undo_purge_errors.to_string(),
        "undo_purge_last_unix_ms" => status.undo_purge_last_unix_ms.to_string(),
        "undo_cache_hits" => status.undo_cache_hits.to_string(),
        "undo_cache_misses" => status.undo_cache_misses.to_string(),
        "row_x_lock_acquires" => status.row_x_lock_acquires.to_string(),
        "row_s_lock_acquires" => status.row_s_lock_acquires.to_string(),
        "lock_upgrades" => status.lock_upgrades.to_string(),
        "lock_upgrade_failures" => status.lock_upgrade_failures.to_string(),
        "table_lock_acquires" => status.table_lock_acquires.to_string(),
        "chunked_release_chunks" => status.chunked_release_chunks.to_string(),
        "lock_wait_histogram_ms" =>
            format!( "0:{} 1-9:{} 10-99:{} 100-999:{} 1000-4999:{} 5000-9999:{} 10000-29999:{} 30000+:{}", status.lock_wait_histogram[0], status.lock_wait_histogram[1], status.lock_wait_histogram[2], status.lock_wait_histogram[3], status.lock_wait_histogram[4], status.lock_wait_histogram[5], status.lock_wait_histogram[6], status.lock_wait_histogram[7], ),
        "wait_event_summary" =>
            if status.wait_event_summary.is_empty() { "none".to_string() } else { status .wait_event_summary .iter() .map(|(name, count, total_us, max_us)| { format!("{}={{count={},total_us={},max_us={}}}", name, count, total_us, max_us) }) .collect::<Vec<_>>() .join(", ") },
        "wal_segment_count" => status.wal_segment_count.to_string(),
        "wal_active_segment_id" => status.wal_active_segment_id.to_string(),
        "wal_archived_segment_count" => status.wal_archived_segment_count.to_string(),
        "wal_archive_enabled" => status.wal_archive_enabled.to_string(),
        "hash_join_count" =>
            crate::engine::executor::select::plan::HASH_JOIN_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "bounded_inner_join_hits" =>
            crate::engine::executor::select::plan::BOUNDED_INNER_JOIN_HITS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_stream_chained" =>
            crate::engine::executor::select::join_stream::HASH_JOIN_STREAM_CHAINED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_stream_rows" =>
            crate::engine::executor::select::join_stream::HASH_JOIN_STREAM_ROWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_stream_fallback" =>
            crate::engine::executor::select::join_stream::HASH_JOIN_STREAM_FALLBACK .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_stream_prebuilt" =>
            crate::engine::executor::select::join_stream::HASH_JOIN_STREAM_PREBUILT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_stream_inlj_tail" =>
            crate::engine::executor::select::join_stream::HASH_JOIN_STREAM_INLJ_TAIL .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_materialized_out_rows" =>
            crate::engine::executor::select::join_stream::HASH_JOIN_MATERIALIZED_OUT_ROWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_build_rows" =>
            crate::engine::executor::select::plan::HASH_JOIN_BUILD_ROWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_probe_rows" =>
            crate::engine::executor::select::plan::HASH_JOIN_PROBE_ROWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_null_skipped" =>
            crate::engine::executor::select::plan::HASH_JOIN_NULL_SKIPPED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_mem_exceeded" =>
            crate::engine::executor::select::plan::HASH_JOIN_MEM_EXCEEDED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_fallback_to_nlj" =>
            crate::engine::executor::select::plan::HASH_JOIN_FALLBACK_TO_NLJ .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_parallel_partitions" =>
            crate::engine::executor::select::plan::HASH_JOIN_PARALLEL_PARTITIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_bloom_built" =>
            crate::engine::executor::select::plan::HASH_JOIN_BLOOM_BUILT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_bloom_filtered" =>
            crate::engine::executor::select::plan::HASH_JOIN_BLOOM_FILTERED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_probe_bloom_builds" =>
            crate::engine::executor::select::plan::INLJ_PROBE_BLOOM_BUILDS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_probe_bloom_keys_probed" =>
            crate::engine::executor::select::plan::INLJ_PROBE_BLOOM_KEYS_PROBED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_probe_bloom_keys_filtered" =>
            crate::engine::executor::select::plan::INLJ_PROBE_BLOOM_KEYS_FILTERED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_mrr_lru_hits" =>
            crate::engine::executor::select::plan::INLJ_MRR_LRU_HITS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_mrr_lru_misses" =>
            crate::engine::executor::select::plan::INLJ_MRR_LRU_MISSES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_mrr_lru_evictions" =>
            crate::engine::executor::select::plan::INLJ_MRR_LRU_EVICTIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mrr_windows" =>
            crate::engine::executor::select::plan::MRR_WINDOWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mrr_rids" =>
            crate::engine::executor::select::plan::MRR_RIDS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mrr_unique_pages" =>
            crate::engine::executor::select::plan::MRR_UNIQUE_PAGES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_agg_dense_integer_activations" => crate::engine::executor::vec::hash_agg::DENSE_INTEGER_ACTIVATIONS.load(std::sync::atomic::Ordering::Relaxed).to_string(),
        "hash_agg_dense_integer_fallbacks" => crate::engine::executor::vec::hash_agg::DENSE_INTEGER_FALLBACKS.load(std::sync::atomic::Ordering::Relaxed).to_string(),
        "group_count_witness_candidates" => crate::engine::executor::select::vec_bridge::count_witness::CANDIDATES.load(std::sync::atomic::Ordering::Relaxed).to_string(),
        "group_count_witness_activations" => crate::engine::executor::select::vec_bridge::count_witness::ACTIVATIONS.load(std::sync::atomic::Ordering::Relaxed).to_string(),
        "group_count_witness_rows_skipped" =>
            crate::engine::executor::select::vec_bridge::count_witness::ROWS_SKIPPED.load(std::sync::atomic::Ordering::Relaxed).to_string(),
        "mrr_page_reuses" =>
            crate::engine::executor::select::plan::MRR_PAGE_REUSES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mrr_workarea_flushes" =>
            crate::engine::executor::select::plan::MRR_WORKAREA_FLUSHES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mrr_peak_bytes" =>
            crate::engine::executor::select::plan::MRR_PEAK_BYTES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_grace_partitions" =>
            crate::engine::executor::select::plan::HASH_JOIN_GRACE_PARTITIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_spill_bytes" =>
            crate::engine::executor::select::plan::HASH_JOIN_SPILL_BYTES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_spill_read_bytes" =>
            crate::engine::executor::select::plan::HASH_JOIN_SPILL_READ_BYTES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "setop_spill_count" =>
            crate::engine::executor::SETOP_SPILL_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "setop_spill_bytes_written" =>
            crate::engine::executor::SETOP_SPILL_BYTES_WRITTEN .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "setop_spill_bytes_read" =>
            crate::engine::executor::SETOP_SPILL_BYTES_READ .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "setop_spill_partitions" =>
            crate::engine::executor::SETOP_SPILL_PARTITIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "setop_spill_repartitions" =>
            crate::engine::executor::SETOP_SPILL_REPARTITIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "setop_spill_max_depth" =>
            crate::engine::executor::SETOP_SPILL_MAX_DEPTH .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "setop_spill_file_opens" =>
            crate::engine::executor::SETOP_SPILL_FILE_OPENS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "setop_spill_merge_passes" =>
            crate::engine::executor::SETOP_SPILL_MERGE_PASSES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_grace_max_depth" =>
            crate::engine::executor::select::plan::HASH_JOIN_GRACE_MAX_DEPTH .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_grace_repartitions" =>
            crate::engine::executor::select::plan::HASH_JOIN_GRACE_REPARTITIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_skew_chunks" =>
            crate::engine::executor::select::plan::HASH_JOIN_SKEW_CHUNKS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_peak_charged_bytes" =>
            crate::engine::executor::select::plan::HASH_JOIN_PEAK_CHARGED_BYTES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_charged_bytes" =>
            crate::engine::executor::select::plan::HASH_JOIN_CHARGED_BYTES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_mem_denied_operator" =>
            crate::engine::executor::select::plan::HASH_JOIN_MEM_DENIED_OPERATOR .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_mem_denied_query" =>
            crate::engine::executor::select::plan::HASH_JOIN_MEM_DENIED_QUERY .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_mem_denied_work_area" =>
            crate::engine::executor::select::plan::HASH_JOIN_MEM_DENIED_WORK_AREA .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "order_by_key_rows_prepared" =>
            crate::engine::executor::select::eval::ORDER_BY_KEY_ROWS_PREPARED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "join_aggregate_multiplicity_rows_saved" =>
            crate::engine::executor::select::vec_bridge::JOIN_AGG_MULTIPLICITY_ROWS_SAVED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hidden_first_value_fd_group_rows" =>
            crate::engine::executor::select::vec_bridge::HIDDEN_FIRST_VALUE_FD_GROUP_ROWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "rand_order_stats_cache_hits" =>
            crate::storage::ORDER_STATS_CACHE_HITS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "rand_order_stats_cache_misses" =>
            crate::storage::ORDER_STATS_CACHE_MISSES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "rand_order_stats_cache_invalidations" =>
            crate::storage::ORDER_STATS_CACHE_INVALIDATIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "active_stream_workers" =>
            crate::engine::executor::select::stream::active_stream_workers().to_string(),
        "row_hash_agg_adaptive_spill" =>
            crate::engine::executor::select::plan::row_hash_agg_adaptive_spill_count() .to_string(),
        "hash_join_typed_key_fast" =>
            crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_FAST .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_typed_key_arity_1" =>
            crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_ARITY_1 .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_typed_key_arity_2" =>
            crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_ARITY_2 .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_typed_key_arity_3" =>
            crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_ARITY_3 .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_typed_key_arity_4" =>
            crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_ARITY_4 .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_typed_key_arity_5_plus" =>
            crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_ARITY_5_PLUS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_typed_key_fallback_uint_range" =>
            crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_FALLBACK_UINT_RANGE .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_typed_key_fallback_runtime_type" =>
            crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_FALLBACK_RUNTIME_TYPE .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_parallel_build_preserved" =>
            crate::engine::executor::select::plan::HASH_JOIN_PARALLEL_BUILD_PRESERVED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_residual_precheck" =>
            crate::engine::executor::select::plan::HASH_JOIN_RESIDUAL_PRECHECK .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_projection_fused" =>
            crate::engine::executor::select::plan::HASH_JOIN_PROJECTION_FUSED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "vec_hash_join_hits" =>
            crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_HITS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "vec_hash_join_range_partition_count" =>
            crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_RANGE_PARTITION_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "vec_hash_join_grace_parallel_workers" =>
            crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_GRACE_PARALLEL_WORKERS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "cte_batch_scan_hits" =>
            crate::engine::executor::select::vec_bridge::CTE_BATCH_SCAN_HITS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "vec_filter_input_hits" =>
            crate::engine::executor::select::vec_bridge::VEC_FILTER_INPUT_HITS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_parallel_scatter" =>
            crate::engine::executor::select::plan::HASH_JOIN_PARALLEL_SCATTER .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_parallel_null_scattered" =>
            crate::engine::executor::select::plan::HASH_JOIN_PARALLEL_NULL_SCATTERED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "nlj_hash_fast_path" =>
            crate::engine::executor::select::plan::NLJ_HASH_FAST_PATH .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_covering_existence_fast" =>
            crate::engine::executor::select::plan::INLJ_COVERING_EXISTENCE_FAST .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "adaptive_join_count" =>
            crate::engine::executor::select::plan::ADAPTIVE_JOIN_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "adaptive_join_chosen_hash" =>
            crate::engine::executor::select::plan::ADAPTIVE_JOIN_CHOSEN_HASH .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "adaptive_join_chosen_index_nlj" =>
            crate::engine::executor::select::plan::ADAPTIVE_JOIN_CHOSEN_INDEX_NLJ .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "adaptive_join_chosen_nlj" =>
            crate::engine::executor::select::plan::ADAPTIVE_JOIN_CHOSEN_NLJ .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "adaptive_join_switched_from_planner" =>
            crate::engine::executor::select::plan::ADAPTIVE_JOIN_SWITCHED_FROM_PLANNER .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        // WS-N8 NLJ/INLJ observability counters: bare NLJ main loop, Block-NL slots,
        // INLJ per-row fallback, outer/batch parallel chunks, fused agg hits,
        // N4 Range promote and N5 MRR cross-chunk sharing slots.
        "nlj_executions" =>
            crate::engine::executor::select::plan::NLJ_EXECUTIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "nlj_pairs_compared" =>
            crate::engine::executor::select::plan::NLJ_PAIRS_COMPARED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "nlj_pairs_kept" =>
            crate::engine::executor::select::plan::NLJ_PAIRS_KEPT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "nlj_block_executions" =>
            crate::engine::executor::select::plan::NLJ_BLOCK_EXECUTIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "nlj_block_pairs_evaluated" =>
            crate::engine::executor::select::plan::NLJ_BLOCK_PAIRS_EVALUATED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "nlj_block_pairs_kept" =>
            crate::engine::executor::select::plan::NLJ_BLOCK_PAIRS_KEPT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        // WS-N3: number of outer blocks that took the SIMD vectorized mask fill
        // (a numeric-compare residual hit the vectorized path instead of whole-block fallback to rows).
        "nlj_simd_fill_blocks" =>
            crate::engine::executor::select::plan::NLJ_SIMD_FILL_BLOCKS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "nlj_outer_parallel_chunks" =>
            crate::engine::executor::select::plan::NLJ_OUTER_PARALLEL_CHUNKS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_per_row_executions" =>
            crate::engine::executor::select::plan::INLJ_PER_ROW_EXECUTIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_outer_parallel_chunks" =>
            crate::engine::executor::select::plan::INLJ_OUTER_PARALLEL_CHUNKS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_batch_parallel_chunks" =>
            crate::engine::executor::select::plan::INLJ_BATCH_PARALLEL_CHUNKS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_probe_parallel_decisions" =>
            crate::engine::executor::select::plan::INLJ_PROBE_PARALLEL_DECISIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_probe_parallel_tasks" =>
            crate::engine::executor::select::plan::INLJ_PROBE_PARALLEL_TASKS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_probe_parallel_rows" =>
            crate::engine::executor::select::plan::INLJ_PROBE_PARALLEL_ROWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_fused_semi_anti" =>
            crate::engine::executor::select::plan::INLJ_FUSED_SEMI_ANTI_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_batch_range_promoted" =>
            crate::engine::executor::select::plan::INLJ_BATCH_RANGE_PROMOTED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_mrr_chunk_shared_hits" =>
            crate::engine::executor::select::plan::INLJ_MRR_CHUNK_SHARED_HITS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_probe_keys" =>
            crate::engine::executor::select::plan::INLJ_PROBE_KEYS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_index_entries" =>
            crate::engine::executor::select::plan::INLJ_INDEX_ENTRIES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_candidate_row_ids" =>
            crate::engine::executor::select::plan::INLJ_CANDIDATE_ROW_IDS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_predicate_first_batches" =>
            crate::engine::executor::select::plan::INLJ_PREDICATE_FIRST_BATCHES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_predicate_first_input_rows" =>
            crate::engine::executor::select::plan::INLJ_PREDICATE_FIRST_INPUT_ROWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_predicate_first_output_rows" =>
            crate::engine::executor::select::plan::INLJ_PREDICATE_FIRST_OUTPUT_ROWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_runtime_keyset_intersect_attempts" =>
            crate::engine::executor::select::plan::INLJ_RUNTIME_KEYSET_INTERSECT_ATTEMPTS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_runtime_keyset_intersect_chosen" =>
            crate::engine::executor::select::plan::INLJ_RUNTIME_KEYSET_INTERSECT_CHOSEN .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_runtime_keyset_intersect_cost_rejected" =>
            crate::engine::executor::select::plan::INLJ_RUNTIME_KEYSET_INTERSECT_COST_REJECTED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_runtime_keyset_intersect_budget_rejected" =>
            crate::engine::executor::select::plan::INLJ_RUNTIME_KEYSET_INTERSECT_BUDGET_REJECTED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_runtime_keyset_intersect_index_entries" =>
            crate::engine::executor::select::plan::INLJ_RUNTIME_KEYSET_INTERSECT_INDEX_ENTRIES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_runtime_keyset_candidate_rejected" =>
            crate::engine::executor::select::plan::INLJ_RUNTIME_KEYSET_CANDIDATE_REJECTED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        // Phase 6: parallel-execution counters.
        "exchange_count" =>
            crate::engine::executor::select::parallel::EXCHANGE_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "parallel_seq_scan_count" =>
            crate::engine::executor::select::parallel::PARALLEL_SEQ_SCAN_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "parallel_zone_map_scan_count" =>
            crate::engine::executor::select::parallel::PARALLEL_ZONE_MAP_SCAN_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "zone_map_candidate_build_ns" =>
            crate::engine::executor::select::parallel::ZONE_MAP_CANDIDATE_BUILD_NS.load(std::sync::atomic::Ordering::Relaxed).saturating_add(crate::storage::ZONE_MAP_RUNTIME_CANDIDATE_NS.load(std::sync::atomic::Ordering::Relaxed)).to_string(),
        "zone_map_scan_tasks_scheduled" =>
            crate::engine::executor::select::parallel::ZONE_MAP_SCAN_TASKS_SCHEDULED.load(std::sync::atomic::Ordering::Relaxed).saturating_add(crate::storage::ZONE_MAP_RUNTIME_TASKS.load(std::sync::atomic::Ordering::Relaxed)).to_string(),
        "zone_map_scan_pages_scheduled" =>
            crate::engine::executor::select::parallel::ZONE_MAP_SCAN_PAGES_SCHEDULED.load(std::sync::atomic::Ordering::Relaxed).saturating_add(crate::storage::ZONE_MAP_RUNTIME_PAGES.load(std::sync::atomic::Ordering::Relaxed)).to_string(),
        "zone_map_scan_pages_skipped" =>
            crate::engine::executor::select::parallel::ZONE_MAP_SCAN_PAGES_SKIPPED.load(std::sync::atomic::Ordering::Relaxed).saturating_add(crate::storage::ZONE_MAP_RUNTIME_PAGES_SKIPPED.load(std::sync::atomic::Ordering::Relaxed)).to_string(),
        "zone_map_runtime_fallbacks" =>
            crate::engine::executor::select::parallel::ZONE_MAP_RUNTIME_FALLBACKS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "zone_map_batch_scan_hits" =>
            crate::engine::executor::select::vec_bridge::ZONE_MAP_BATCH_SCAN_HITS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "parallel_index_fast_scan_count" =>
            crate::engine::executor::select::parallel::PARALLEL_INDEX_FAST_SCAN_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "parallel_index_aggregate_count" =>
            crate::engine::executor::select::parallel::PARALLEL_INDEX_AGGREGATE_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "parallel_hash_aggregate_count" =>
            crate::engine::executor::select::parallel::PARALLEL_HASH_AGGREGATE_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "parallel_sort_count" =>
            crate::engine::executor::select::parallel::PARALLEL_SORT_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "parallel_workers_spawned" =>
            crate::engine::executor::select::parallel::PARALLEL_WORKERS_SPAWNED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "parallel_worker_panic_count" =>
            crate::engine::executor::select::parallel::PARALLEL_WORKER_PANIC_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "process_query_mem_used_bytes" =>
            crate::engine::executor::mem_budget::process_query_mem_used().to_string(),
        "process_query_mem_high_water_bytes" =>
            crate::engine::executor::mem_budget::process_query_mem_high_water().to_string(),
        "process_query_mem_limit_bytes" =>
            crate::engine::executor::mem_budget::process_mem_limit_bytes().to_string(),
        "query_mem_limit_bytes" => crate::engine::executor::mem_budget::query_mem_limit_bytes().to_string(),
        "query_memory_reservation_rejections" =>
            crate::engine::executor::mem_budget::query_reservation_rejections().to_string(),
        "process_memory_reservation_rejections" =>
            crate::engine::executor::mem_budget::process_reservation_rejections().to_string(),
        "executor_terminal_oom_cancellations" =>
            crate::engine::executor::mem_budget::terminal_oom_cancellations().to_string(),
        "physical_memory_pressure_rejections" =>
            crate::engine::memory_reclaimer::physical_pressure_rejections().to_string(),
        "memory_reclaim_attempts" => admission_reclaim.attempts.to_string(),
        "memory_reclaim_retry_successes" => admission_reclaim.retry_successes.to_string(),
        "memory_reclaim_cache_bytes" => admission_reclaim.cache_bytes.to_string(),
        "memory_reclaim_buffer_bytes" => admission_reclaim.buffer_bytes.to_string(),
        "memory_reclaim_allocator_bytes" => admission_reclaim.allocator_bytes.to_string(),
        "memory_oom_after_reclaim" => admission_reclaim.oom_after_retry.to_string(),
        // PGA-style work-area arbiter summary. The advisor view
        // `information_schema.MYDB_MEMORY_ADVISOR` breaks each of
        // these out one-per-row with explanatory text.
        "memory_target_bytes" => crate::engine::executor::mem_budget::memory_target_bytes().to_string(),
        "memory_soft_target_configured_bytes" =>
            crate::engine::executor::mem_budget::configured_memory_target_bytes() .unwrap_or(0) .to_string(),
        "effective_memory_limit_bytes" =>
            crate::engine::executor::mem_budget::effective_memory_limit_bytes().to_string(),
        "sql_memory_hard_limit_bytes" =>
            crate::engine::executor::mem_budget::operational_memory_limit_bytes().to_string(),
        "process_memory_hard_limit_bytes" =>
            crate::engine::executor::mem_budget::managed_memory_limit_bytes().to_string(),
        "executor_memory_target_bytes" =>
            crate::engine::executor::mem_budget::executor_memory_target_bytes().to_string(),
        "idle_memory_reclaim_enabled" => u8::from(crate::engine::memory_reclaimer::enabled()).to_string(),
        "idle_memory_reclaim_active_statements" => idle_reclaim.active_statements.to_string(),
        "idle_memory_reclaim_attempts" => idle_reclaim.attempts.to_string(),
        "idle_memory_reclaim_successes" => idle_reclaim.successes.to_string(),
        "idle_memory_reclaim_skipped_active" => idle_reclaim.skipped_active.to_string(),
        "idle_memory_reclaim_unsupported" => idle_reclaim.unsupported.to_string(),
        "idle_memory_reclaim_last_before_bytes" => idle_reclaim.last_before_bytes.to_string(),
        "idle_memory_reclaim_last_after_bytes" => idle_reclaim.last_after_bytes.to_string(),
        "idle_memory_reclaim_last_released_bytes" => idle_reclaim.last_released_bytes.to_string(),
        "idle_memory_reclaim_last_duration_us" => idle_reclaim.last_duration_us.to_string(),
        "controlled_memory_current_bytes" => controlled_current.to_string(),
        "controlled_memory_managed_limit_bytes" =>
            crate::engine::executor::mem_budget::managed_memory_limit_bytes().to_string(),
        "controlled_memory_operational_limit_bytes" =>
            crate::engine::executor::mem_budget::operational_memory_limit_bytes().to_string(),
        "controlled_memory_performance_schema_limit_bytes" =>
            crate::engine::executor::mem_budget::performance_schema_memory_limit_bytes() .to_string(),
        "controlled_memory_peak_bytes" => controlled_peak.to_string(),
        "controlled_memory_buffer_raw_bytes" => controlled_categories[0].to_string(),
        "controlled_memory_storage_scratch_bytes" => controlled_categories[1].to_string(),
        "controlled_memory_executor_bytes" => controlled_categories[2].to_string(),
        "controlled_memory_derived_cache_bytes" => controlled_categories[3].to_string(),
        "controlled_memory_partition_dml_batch_bytes" => controlled_categories[4].to_string(),
        "controlled_memory_partition_ddl_bytes" => controlled_categories[5].to_string(),
        "controlled_memory_partition_stats_bytes" => controlled_categories[6].to_string(),
        "controlled_memory_ddl_copy_bytes" => controlled_categories[7].to_string(),
        "controlled_memory_system_metadata_bytes" => controlled_categories[8].to_string(),
        "controlled_memory_temporal_history_bytes" => controlled_categories[9].to_string(),
        "controlled_memory_protocol_transient_bytes" => controlled_categories[10].to_string(),
        "controlled_memory_binlog_transient_bytes" => controlled_categories[11].to_string(),
        "controlled_memory_parser_transient_bytes" => controlled_categories[12].to_string(),
        "controlled_memory_rejections" => controlled_rejections.to_string(),
        "controlled_memory_native_replication_transient_bytes" => controlled_categories[13].to_string(),
        "controlled_memory_performance_schema_bytes" => controlled_categories[14].to_string(),
        "controlled_memory_ddl_online_log_bytes" => controlled_categories[15].to_string(),
        "controlled_memory_rejections_buffer_raw" => controlled_rejections_by_category[0].to_string(),
        "controlled_memory_rejections_storage_scratch" => controlled_rejections_by_category[1].to_string(),
        "controlled_memory_rejections_executor" => controlled_rejections_by_category[2].to_string(),
        "controlled_memory_rejections_derived_cache" => controlled_rejections_by_category[3].to_string(),
        "controlled_memory_rejections_partition_dml_batch" => controlled_rejections_by_category[4].to_string(),
        "controlled_memory_rejections_partition_ddl" => controlled_rejections_by_category[5].to_string(),
        "controlled_memory_rejections_partition_stats" => controlled_rejections_by_category[6].to_string(),
        "controlled_memory_rejections_ddl_copy" => controlled_rejections_by_category[7].to_string(),
        "controlled_memory_rejections_system_metadata" => controlled_rejections_by_category[8].to_string(),
        "controlled_memory_rejections_temporal_history" => controlled_rejections_by_category[9].to_string(),
        "controlled_memory_rejections_protocol_transient" => controlled_rejections_by_category[10].to_string(),
        "controlled_memory_rejections_binlog_transient" => controlled_rejections_by_category[11].to_string(),
        "controlled_memory_rejections_parser_transient" => controlled_rejections_by_category[12].to_string(),
        "controlled_memory_rejections_native_replication_transient" => controlled_rejections_by_category[13].to_string(),
        "controlled_memory_rejections_performance_schema" => controlled_rejections_by_category[14].to_string(),
        "controlled_memory_rejections_ddl_online_log" => controlled_rejections_by_category[15].to_string(),
        "work_area_allocated_bytes" => work_area.allocated_bytes.to_string(),
        "work_area_high_water_bytes" => work_area.high_water_bytes.to_string(),
        "work_area_active_handles" => work_area.active_handles.to_string(),
        "work_area_optimal_grants" => work_area.optimal_grants.to_string(),
        "work_area_one_pass_grants" => work_area.one_pass_grants.to_string(),
        "work_area_min_grants" => work_area.min_grants.to_string(),
        "work_area_spill_requests" => work_area.spill_requests.to_string(),
        "work_area_spill_estimated_bytes" => work_area.spill_estimated_bytes.to_string(),
        "work_area_force_overflow_bytes" => work_area.force_overflow_bytes.to_string(),
        "work_area_force_overflow_events" => work_area.force_overflow_events.to_string(),
        // P0-3: temp-disk spill quota live state.
        "temp_disk_live_bytes" => crate::engine::executor::spill_quota::global_spilled_bytes().to_string(),
        "temp_disk_high_water_bytes" =>
            crate::engine::executor::spill_quota::global_spilled_high_water().to_string(),
        "temp_disk_quota_rejections" => crate::engine::executor::spill_quota::quota_rejections().to_string(),
        "temp_disk_global_quota_rejections" =>
            crate::engine::executor::spill_quota::global_quota_rejections().to_string(),
        "temp_disk_query_quota_rejections" =>
            crate::engine::executor::spill_quota::per_query_quota_rejections().to_string(),
        "active_spill_files" => crate::engine::executor::spill_quota::active_spill_files().to_string(),
        "spill_file_opens" => crate::engine::executor::spill_quota::spill_file_opens().to_string(),
        "orphan_spill_bytes" => crate::engine::executor::spill_quota::orphan_spill_bytes().to_string(),
        "spill_cleanup_failures" => crate::engine::executor::spill_quota::cleanup_failures().to_string(),
        "sort_spill_count" => crate::engine::executor::spill_quota::sort_spill_count().to_string(),
        "hash_join_spill_count" => crate::engine::executor::spill_quota::hash_join_spill_count().to_string(),
        "hash_agg_spill_count" => crate::engine::executor::spill_quota::hash_agg_spill_count().to_string(),
        "distinct_spill_count" => crate::engine::executor::spill_quota::distinct_spill_count().to_string(),
        "cte_spill_count" => crate::engine::executor::spill_quota::cte_spill_count().to_string(),
        // P0/D1: roll-up of the engine's independently-accounted memory
        // pools so operators can watch total tracked memory approach
        // physical RAM in one place. `tracked_memory_bytes` is a FLOOR —
        // buffer pool (resident frames) + executor operator state +
        // in-memory undo mirror — and intentionally excludes still-untracked
        // pools (HNSW graphs, per-thread visibility LRUs, catalog) and
        // allocator overhead. `resident_set_peak_bytes` (getrusage) is the
        // OS ground truth; a large gap above the tracked floor is untracked
        // memory. (Unifying these under one arbiter is P1/A.)
        "buffer_pool_resident_bytes" =>
            ((controlled_categories[0] as u128) .saturating_add(controlled_categories[1] as u128)) .to_string(),
        "tracked_memory_bytes" =>
            ((controlled_current as u128) .saturating_add(known_cache_bytes)) .to_string(),
        "resident_set_peak_bytes" => peak_resident_bytes().to_string(),
        "active_connections" => connection_metrics.active.to_string(),
        "max_connections" => connection_metrics.max_connections.to_string(),
        "effective_max_connections" => connection_metrics.effective_max_connections.to_string(),
        "admin_connections" => connection_metrics.admin_connections.to_string(),
        "admin_connection_reserve" => connection_metrics.admin_connection_reserve.to_string(),
        "accepted_connections" => connection_metrics.accepted.to_string(),
        "rejected_connections" => connection_metrics.rejected.to_string(),
        "connection_errors_max_connections" =>
            connection_metrics .connection_errors_max_connections .to_string(),
        "max_used_connections" => connection_metrics.max_used_connections.to_string(),
        "handshake_successes" => connection_metrics.handshake_successes.to_string(),
        "handshake_failures" => connection_metrics.handshake_failures.to_string(),
        "closed_connections" => connection_metrics.closed.to_string(),
        "listen_backlog" => connection_metrics.listen_backlog.to_string(),
        "handshake_timeout_secs" => connection_metrics.handshake_timeout_secs.to_string(),
        "open_files_soft_limit" => connection_metrics.open_files_soft_limit.to_string(),
        "open_files_hard_limit" => connection_metrics.open_files_hard_limit.to_string(),
        "open_files_requested_limit" => connection_metrics.open_files_requested_limit.to_string(),
        "open_files_reserve" => connection_metrics.open_files_reserve.to_string(),
        "max_allowed_packet_bytes" => crate::mysql::packet::max_allowed_packet_bytes().to_string(),
        "packet_write_buffer_max_bytes" => crate::mysql::packet::batch_buffer_max_bytes().to_string(),
        "net_write_timeout_secs" =>
            crate::mysql::packet::net_write_timeout() .map(|d| d.as_secs().to_string()) .unwrap_or_else(|| "0".to_string()),
        "killed_queries" => crate::engine::cancel::snapshot_kill_stats() .killed_queries .to_string(),
        "killed_connections" => crate::engine::cancel::snapshot_kill_stats() .killed_connections .to_string(),
        "kill_observed" => crate::engine::cancel::snapshot_kill_stats() .kill_observed .to_string(),
    };
    // Background Auto-ANALYZE observability.
    let aa = crate::engine::auto_analyze::stats_snapshot();
    let mut rows = rows;
    rows.extend([
        (
            "auto_analyze_enabled",
            if aa.enabled { "1" } else { "0" }.to_string(),
        ),
        ("auto_analyze_interval_s", aa.interval_s.to_string()),
        ("auto_analyze_ratio", format!("{:.4}", aa.ratio)),
        (
            "auto_analyze_last_run_unix_ms",
            aa.last_run_unix_ms.to_string(),
        ),
        ("auto_analyze_total_runs", aa.total_runs.to_string()),
        (
            "auto_analyze_total_tables_analyzed",
            aa.total_tables_analyzed.to_string(),
        ),
    ]);
    rows.extend(
        db.sequence_status_pairs()
            .map(|(name, value)| (name, value.to_string())),
    );
    rows.extend(db.zone_map_status_rows());
    rows.extend(
        crate::engine::status_counters::like_access_counters()
            .into_iter()
            .map(|(name, value)| (name, value.to_string())),
    );
    rows.into_iter()
        .map(|(name, value)| (name.to_string(), value))
        .chain(crate::binlog::semi_sync_status_pairs())
        .chain(hnsw_status_rows(db))
        .collect()
}

/// HNSW vector-index section for `information_schema.MYDB_ENGINE_STATUS`: a `hnsw_indexes`
/// count header plus one packed-value row per index. Warm indexes report live
/// runtime stats (node counts, stale ratio, backend, persisted LSN); cold
/// (never-queried) indexes report their catalog build parameters with a
/// `(cold — not yet built)` marker. Shares its data source with
/// `information_schema.MYDB_VECTOR_INDEXES`.
fn hnsw_status_rows(db: &Database) -> Vec<(String, String)> {
    use crate::storage::naming::normalize_name;
    let runtime = db.vector_index_runtime_stats();
    let mut indexes: Vec<(String, String)> = Vec::new();
    for header in db.catalog().tables.values() {
        let table = if header.columns.is_empty() {
            match db.table(&header.qualified_name()) {
                Ok(table) => table,
                Err(_) => continue,
            }
        } else {
            header.clone()
        };
        for index in table.iter_public_indexes() {
            if !index.is_vector() {
                continue;
            }
            let meta = index.vector_meta();
            let label = format!("hnsw.{}.{}.{}", table.database, table.name, index.name);
            let quant = index
                .vector
                .as_ref()
                .map_or("none", |v| v.quantization.name());
            let value = match runtime.get(&(table.id, normalize_name(&index.name))) {
                Some(st) => {
                    let build = st.last_build_mode.map_or_else(String::new, |mode| {
                        format!(
                            " build_mode={} build_rows={} build_stats_ms={} \
                             build_encode_ms={} build_link_ms={} build_materialize_ms={} \
                             build_total_ms={} build_peak_scratch_bytes={} \
                             build_cache_hits={} build_cache_misses={} build_chunks={}",
                            mode,
                            st.last_build_rows,
                            st.last_build_stats_ms,
                            st.last_build_encode_ms,
                            st.last_build_link_ms,
                            st.last_build_materialize_ms,
                            st.last_build_total_ms,
                            st.last_build_peak_scratch_bytes,
                            st.last_build_cache_hits,
                            st.last_build_cache_misses,
                            st.last_build_chunks,
                        )
                    });
                    format!(
                        "backend={} metric={} dim={} total_nodes={} live_nodes={} \
                         stale_nodes={} stale_ratio={}% partitions={} generation={} \
                         last_persisted_lsn={} nav_pages={} sq8_exact_fallback_nodes={}{}",
                        st.backend,
                        meta.metric,
                        st.dim,
                        st.total_nodes,
                        st.live_nodes,
                        st.stale_nodes,
                        st.stale_ratio_pct(),
                        st.partitions_loaded,
                        st.generation,
                        st.last_persisted_lsn,
                        st.nav_pages,
                        st.sq8_exact_fallback_nodes,
                        build,
                    )
                }
                None => format!(
                    "metric={} m={} ef_construction={} (cold — not yet built)",
                    meta.metric, meta.m, meta.ef_construction,
                ),
            };
            let value=format!("{value} quantization={quant}{}",runtime.get(&(table.id,normalize_name(&index.name))).map(|st|format!(" quantizer_bytes={} compressed_bytes={} exceptions={} candidates={} heap_reranks={} exact_fallbacks={}",st.quantizer_bytes,st.compressed_bytes,st.quantization_exceptions,st.candidates_returned,st.heap_reranks,st.exact_fallbacks)).unwrap_or_default());
            indexes.push((label, value));
        }
    }
    let mut rows = vec![
        ("hnsw_indexes".to_string(), indexes.len().to_string()),
        (
            "hnsw_tombstones_rolled_back".to_string(),
            db.hnsw_tombstones_rolled_back().to_string(),
        ),
    ];
    rows.extend(indexes);
    rows
}
