impl Database {
    pub fn set_storage_variable(&self, name: &str, value: u64) -> Result<(), QueryError> {
        match normalize_name(name).as_str() {
            "mydb_buffer_pool_pages" => self.set_buffer_pool_pages(value),
            "mydb_buffer_pool_quota_heap_pct" => self.set_buffer_pool_quota_pct(0, value),
            "mydb_buffer_pool_quota_index_pct" => self.set_buffer_pool_quota_pct(1, value),
            "mydb_buffer_pool_quota_lob_pct" => self.set_buffer_pool_quota_pct(2, value),
            "mydb_buffer_pool_quota_zone_map_pct" => self.set_buffer_pool_quota_pct(4, value),
            "mydb_buffer_pool_prefetch_pages" => self.set_buffer_pool_prefetch_pages(value),
            "mydb_buffer_pool_small_scan_pct" => self.set_buffer_pool_small_scan_pct(value),
            "mydb_undo_retention_ms" => self.set_undo_retention_ms(value),
            "mydb_undo_segment_bytes" => self.set_undo_segment_bytes(value),
            "mydb_wal_write_buffer_bytes" => self.set_wal_write_buffer_bytes(value),
            "mydb_undo_buffer_bytes" => self.set_undo_buffer_bytes(value),
            "mydb_undo_purge_interval_ms" => self.set_undo_purge_interval_ms(value),
            "mydb_undo_purge_batch_records" => self.set_undo_purge_batch_records(value),
            "mydb_undo_tablespace_max_bytes" => self.set_undo_tablespace_max_bytes(value),
            "mydb_undo_mem_soft_limit" => self.set_undo_mem_soft_limit_bytes(value),
            "mydb_undo_mem_cache_limit" => self.set_undo_mem_cache_limit_bytes(value),
            "mydb_undo_txn_max_bytes" => self.set_undo_txn_max_bytes(value),
            "mydb_undo_retention_guarantee" => self.set_undo_retention_guarantee(value != 0),
            "mydb_undo_disk_cap_softfail_warn" => self.set_undo_disk_cap_softfail_warn(value != 0),
            "mydb_checkpoint_wal_bytes" => self.set_checkpoint_wal_bytes(value),
            "mydb_group_commit_us" => self.set_group_commit_us(value),
            "mydb_group_commit_adaptive" => self.set_group_commit_adaptive(value != 0),
            "mydb_query_mem_limit_mb" => {
                crate::engine::executor::mem_budget::set_query_mem_limit_mb(value as usize)
            }
            "mydb_process_mem_limit_mb" => {
                crate::engine::executor::mem_budget::set_process_mem_limit_mb(value as usize)
            }
            "mydb_cursor_per_session_max" => {
                crate::engine::cursor_registry::set_per_session_max(value as usize)
            }
            "mydb_cursor_global_max" => {
                crate::engine::cursor_registry::set_global_max(value as usize)
            }
            "mydb_cursor_max_idle_ms" => crate::engine::cursor_registry::set_max_idle_ms(value),
            _ => Err(error(
                crate::error::catalog::ER_UNKNOWN_SYSTEM_VARIABLE,
                format!("unknown system variable '{name}'"),
            )),
        }
    }

    fn snapshot_stats(&self) -> (u64, u64) {
        let now = current_unix_ms();
        let mut oldest_scn: Option<u64> = None;
        let mut oldest_ms: u64 = 0;
        for shard in self.active_snapshots.iter() {
            if let Ok(snapshots) = shard.lock() {
                for snapshot in snapshots.values() {
                    oldest_scn = Some(match oldest_scn {
                        Some(current) => current.min(snapshot.snapshot_scn),
                        None => snapshot.snapshot_scn,
                    });
                    oldest_ms = oldest_ms.max(now.saturating_sub(snapshot.started_unix_ms));
                }
            }
        }
        (oldest_scn.unwrap_or(0), oldest_ms)
    }

    fn row_migration_count(&self) -> u64 {
        let handles: Vec<_> = self.heap_cache.read().values().cloned().collect();
        handles
            .into_iter()
            .map(|heap| heap.read().row_migration_count())
            .sum()
    }
}
