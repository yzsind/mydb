//! Stable, low-cardinality process metrics for external monitoring.
//!
//! This snapshot is intentionally narrower than
//! `information_schema.MYDB_ENGINE_STATUS`: it exposes the counters an
//! operator or an automated SRE agent needs to detect the common failure
//! modes (WAL fsync latency, checkpoint backlogs, buffer-pool pressure,
//! lock contention, memory admission, disk pressure, replication lag)
//! without publishing every internal diagnostic counter. The REST layer
//! formats it as Prometheus text exposition.

use crate::engine::session_registry::SessionRegistry;
use crate::engine::status_counters::{self, SessionCounter};
use crate::storage::Database;

/// Point-in-time values behind `GET /metrics`.
///
/// Counters are process-lifetime cumulative values; gauges are current.
/// Subsystems are sampled sequentially rather than atomically, matching the
/// existing status surfaces.
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    pub uptime_seconds: u64,
    pub build: BuildMetrics,
    pub connections: ConnectionMetrics,
    pub queries: QueryMetrics,
    pub commands: Vec<(String, u64)>,
    pub transactions: TransactionMetrics,
    pub locks: LockMetrics,
    pub wal: WalMetrics,
    pub checkpoints: CheckpointMetrics,
    pub group_commit: GroupCommitMetrics,
    pub buffer_pool: BufferPoolMetrics,
    pub memory: MemoryMetrics,
    pub undo: UndoMetrics,
    pub spill: SpillMetrics,
    pub disk: DiskMetrics,
    pub recyclebin: RecyclebinMetrics,
    pub replication: ReplicationMetrics,
}

#[derive(Debug, Clone, Default)]
pub struct BuildMetrics {
    pub version: String,
    pub mysql_version: String,
    pub durability: String,
    pub wal_sync_method: String,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ConnectionMetrics {
    pub active: u64,
    pub max_used: u64,
    pub limit: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub handshake_failures: u64,
    pub aborted_clients: u64,
    pub threads_running: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct QueryMetrics {
    pub questions: u64,
    pub queries: u64,
    pub slow_queries: u64,
    pub bytes_received: u64,
    pub bytes_sent: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TransactionMetrics {
    pub active: u64,
    pub oldest_active_ms: u64,
    pub oldest_snapshot_scn: u64,
    pub xa_prepared: u64,
    pub snapshot_too_old: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LockMetrics {
    pub row_waits: u64,
    pub active_waits: u64,
    pub wait_time_ms: u64,
    pub wait_time_max_ms: u64,
    pub deadlocks: u64,
    pub latch_waits: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct WalMetrics {
    pub size_bytes: u64,
    pub current_lsn: u64,
    pub durable_lsn: u64,
    pub durable_lag_lsn: u64,
    pub syncs: u64,
    pub sync_total_us: u64,
    pub sync_max_us: u64,
    pub physical_write_calls: u64,
    pub physical_write_bytes: u64,
    pub logical_records: u64,
    pub rotation_count: u64,
    pub rotation_critical_total_us: u64,
    pub rotation_critical_max_us: u64,
    pub pending_sealed_sync: u64,
    pub segments: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CheckpointMetrics {
    pub total: u64,
    pub failures: u64,
    pub preflush_failures: u64,
    pub age_lsn: u64,
    pub redo_age_lsn: u64,
    pub duration_total_us: u64,
    pub duration_max_us: u64,
    pub last_duration_us: u64,
    pub last_snapshot_us: u64,
    pub last_flush_us: u64,
    pub last_sync_us: u64,
    pub last_compact_us: u64,
    pub last_dirty_pages: u64,
    pub last_wal_reclaimed_bytes: u64,
    pub wal_reclaimed_bytes_total: u64,
    pub restartpoints: u64,
    /// Unix milliseconds of the most recent completed checkpoint. `None`
    /// until the first checkpoint succeeds (the startup timer seed is not a
    /// completed checkpoint).
    pub last_success_unix_ms: Option<u64>,
    /// WAL bytes accumulated since the most recent checkpoint.
    pub wal_bytes_since_checkpoint: u64,
    /// Configured WAL growth threshold that triggers a size checkpoint.
    pub wal_threshold_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GroupCommitMetrics {
    pub batches: u64,
    pub average_batch_size: u64,
    pub queue_depth: u64,
    pub queue_depth_peak: u64,
    pub leader_wait_total_us: u64,
    pub leader_wait_max_us: u64,
    pub follower_wait_total_us: u64,
    pub follower_wait_max_us: u64,
    pub sync_total_us: u64,
    pub sync_max_us: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BufferPoolMetrics {
    pub pages: u64,
    pub used_pages: u64,
    pub dirty_pages: u64,
    pub freeable_pages: u64,
    pub pinned_pages: u64,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub flushes: u64,
    pub write_requests: u64,
    pub read_ios: u64,
    pub write_ios: u64,
    pub wait_count: u64,
    pub wait_ms: u64,
    pub flush_errors: u64,
    pub background_io_errors: u64,
    pub checkpoint_flush_pages: u64,
    pub checkpoint_flush_us: u64,
    pub checkpoint_preflush_pages: u64,
    pub checkpoint_preflush_us: u64,
    pub file_syncs: u64,
    pub file_sync_skipped: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MemoryMetrics {
    pub controlled_bytes: u64,
    pub controlled_peak_bytes: u64,
    pub controlled_limit_bytes: u64,
    pub controlled_target_bytes: u64,
    pub controlled_rejections: u64,
    pub process_peak_resident_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UndoMetrics {
    pub bytes: u64,
    pub records: u64,
    pub peak_bytes: u64,
    pub retention_pressure: u64,
    pub purge_errors: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SpillMetrics {
    pub bytes_written: u64,
    pub bytes_read: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DiskMetrics {
    pub pressure_enabled: bool,
    pub write_limited: bool,
    pub available_bytes: u64,
    pub recovery_reserve_armed: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RecyclebinMetrics {
    pub pressure: u64,
    pub purges: u64,
    pub cleanup_failures: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ReplicationMetrics {
    pub channels_running: u64,
    pub channels_total: u64,
    pub max_seconds_behind_source: Option<u64>,
}

/// Collect a status snapshot from the storage engine and shared session
/// registry. Every source is an atomic read or a short metadata lock; no
/// query planning or disk I/O is performed.
pub(crate) fn collect(db: &Database, registry: &SessionRegistry) -> MetricsSnapshot {
    let status = db.storage_status();
    let connections = crate::mysql::connection_manager::connection_metrics();
    let session_status = registry.global_session_status();
    let (controlled_bytes, controlled_peak, _controlled_categories, controlled_rejections) =
        crate::engine::executor::mem_budget::controlled_memory_snapshot();
    let [disk_write_limited, disk_available_bytes, disk_recovery_reserve_armed] =
        db.disk_pressure_status();
    let [recyclebin_pressure, recyclebin_purges, recyclebin_cleanup_failures] =
        db.recyclebin_status();
    let (xa_prepared, _xa_commits, _xa_rollbacks) = db.xa_status_counters();
    let (mut channels_running, mut channels_total, mut max_lag) = (0u64, 0u64, None);
    if let Some(manager) = crate::replication::global_manager() {
        for (_config, replica) in manager.snapshot_all_channels() {
            channels_total = channels_total.saturating_add(1);
            if replica.running {
                channels_running = channels_running.saturating_add(1);
            }
            if let Some(lag) = replica.seconds_behind_source {
                max_lag = Some(max_lag.map_or(lag, |current: u64| current.max(lag)));
            }
        }
    }
    MetricsSnapshot {
        uptime_seconds: status_counters::uptime_seconds(),
        build: BuildMetrics {
            version: env!("CARGO_PKG_VERSION").to_string(),
            mysql_version: crate::server_version::MYSQL_COMPAT_VERSION.to_string(),
            durability: status.durability.to_string(),
            wal_sync_method: crate::wal::resolved_wal_sync_method().as_str().to_string(),
        },
        connections: ConnectionMetrics {
            active: connections.active,
            max_used: connections.max_used_connections,
            limit: connections.effective_max_connections,
            accepted: connections.accepted,
            rejected: connections.rejected,
            handshake_failures: connections.handshake_failures,
            aborted_clients: connections.aborted_clients,
            threads_running: registry.running_foreground_count(),
        },
        queries: QueryMetrics {
            questions: session_status.value(SessionCounter::Questions),
            queries: connections.queries_executed,
            slow_queries: status_counters::slow_queries(),
            bytes_received: status_counters::bytes_received(),
            bytes_sent: status_counters::bytes_sent(),
        },
        commands: session_status
            .com_counters()
            .map(|(name, value)| {
                (
                    name.strip_prefix("Com_")
                        .unwrap_or(name)
                        .to_ascii_lowercase(),
                    value,
                )
            })
            .collect(),
        transactions: TransactionMetrics {
            active: status.active_transactions as u64,
            oldest_active_ms: status.oldest_active_txn_ms,
            oldest_snapshot_scn: status.oldest_snapshot_scn,
            xa_prepared: xa_prepared as u64,
            snapshot_too_old: status.snapshot_too_old_count,
        },
        locks: LockMetrics {
            row_waits: status.lock_waits,
            active_waits: status.active_lock_waits as u64,
            wait_time_ms: status.lock_wait_time_ms,
            wait_time_max_ms: status.lock_wait_time_max_ms,
            deadlocks: status.deadlocks,
            latch_waits: status.latch_waits,
        },
        wal: WalMetrics {
            size_bytes: status.wal_bytes,
            current_lsn: status.current_lsn,
            durable_lsn: status.durable_lsn,
            durable_lag_lsn: status.current_lsn.saturating_sub(status.durable_lsn),
            syncs: status.wal_syncs,
            sync_total_us: status.wal_sync_total_us,
            sync_max_us: status.wal_sync_max_us,
            physical_write_calls: status.wal_physical_write_calls,
            physical_write_bytes: status.wal_physical_write_bytes,
            logical_records: status.wal_logical_records,
            rotation_count: status.wal_rotation_count,
            rotation_critical_total_us: status.wal_rotation_critical_total_us,
            rotation_critical_max_us: status.wal_rotation_critical_max_us,
            pending_sealed_sync: status.wal_pending_sealed_sync,
            segments: status.wal_segment_count,
        },
        checkpoints: CheckpointMetrics {
            total: status.checkpoints_total,
            failures: status.checkpoint_failures_total,
            preflush_failures: status.checkpoint_preflush_failures_total,
            age_lsn: status.checkpoint_age_lsn,
            redo_age_lsn: status.checkpoint_redo_age_lsn,
            duration_total_us: status.checkpoint_duration_total_us,
            duration_max_us: status.checkpoint_duration_max_us,
            last_duration_us: status.last_checkpoint_duration_us,
            last_snapshot_us: status.last_checkpoint_snapshot_us,
            last_flush_us: status.last_checkpoint_flush_us,
            last_sync_us: status.last_checkpoint_sync_us,
            last_compact_us: status.last_checkpoint_compact_us,
            last_dirty_pages: status.last_checkpoint_dirty_pages,
            last_wal_reclaimed_bytes: status.last_checkpoint_wal_reclaimed_bytes,
            wal_reclaimed_bytes_total: status.checkpoint_wal_reclaimed_bytes_total,
            restartpoints: status.restartpoints_total,
            last_success_unix_ms: if status.checkpoints_total > 0 {
                status.last_checkpoint_unix_ms
            } else {
                None
            },
            wal_bytes_since_checkpoint: db.wal_bytes_since_checkpoint(),
            wal_threshold_bytes: status.checkpoint_wal_bytes,
        },
        group_commit: GroupCommitMetrics {
            batches: status.group_commit_batches,
            average_batch_size: status.avg_group_commit_size,
            queue_depth: status.durability_queue_depth,
            queue_depth_peak: status.durability_queue_depth_peak,
            leader_wait_total_us: status.group_commit_leader_wait_total_us,
            leader_wait_max_us: status.group_commit_leader_wait_max_us,
            follower_wait_total_us: status.group_commit_follower_wait_total_us,
            follower_wait_max_us: status.group_commit_follower_wait_max_us,
            sync_total_us: status.group_commit_sync_op_total_us,
            sync_max_us: status.group_commit_sync_op_max_us,
        },
        buffer_pool: BufferPoolMetrics {
            pages: status.buffer_pool_pages,
            used_pages: status.buffer_pool_used_pages,
            dirty_pages: status.buffer_pool_dirty_pages,
            freeable_pages: status.buffer_pool_freeable_pages,
            pinned_pages: status.buffer_pool_pinned_pages,
            hits: status.buffer_hits,
            misses: status.buffer_misses,
            evictions: status.buffer_pool_evictions,
            flushes: status.buffer_pool_flushes,
            write_requests: status.buffer_pool_write_requests,
            read_ios: status.buffer_pool_read_ios,
            write_ios: status.buffer_pool_write_ios,
            wait_count: status.buffer_pool_waits,
            wait_ms: status.buffer_pool_wait_ms,
            flush_errors: status.buffer_pool_flush_errors,
            background_io_errors: status.buffer_pool_background_io_errors,
            checkpoint_flush_pages: status.buffer_pool_checkpoint_flush_pages,
            checkpoint_flush_us: status.buffer_pool_checkpoint_flush_us,
            checkpoint_preflush_pages: status.buffer_pool_checkpoint_preflush_pages,
            checkpoint_preflush_us: status.buffer_pool_checkpoint_preflush_us,
            file_syncs: status.buffer_pool_file_syncs,
            file_sync_skipped: status.buffer_pool_file_sync_skipped,
        },
        memory: MemoryMetrics {
            controlled_bytes: controlled_bytes as u64,
            controlled_peak_bytes: controlled_peak as u64,
            controlled_limit_bytes:
                crate::engine::executor::mem_budget::operational_memory_limit_bytes() as u64,
            controlled_target_bytes: crate::engine::executor::mem_budget::memory_target_bytes()
                as u64,
            controlled_rejections,
            process_peak_resident_bytes: crate::platform::memory::peak_resident_bytes()
                .unwrap_or(0),
        },
        undo: UndoMetrics {
            bytes: status.undo_bytes,
            records: status.undo_records as u64,
            peak_bytes: status.undo_peak_bytes,
            retention_pressure: status.undo_retention_pressure,
            purge_errors: status.undo_purge_errors,
        },
        spill: SpillMetrics {
            bytes_written: status.spill_bytes_written,
            bytes_read: status.spill_bytes_read,
        },
        disk: DiskMetrics {
            pressure_enabled: db.disk_pressure_enabled(),
            write_limited: disk_write_limited != 0,
            available_bytes: disk_available_bytes,
            recovery_reserve_armed: disk_recovery_reserve_armed != 0,
        },
        recyclebin: RecyclebinMetrics {
            pressure: recyclebin_pressure,
            purges: recyclebin_purges,
            cleanup_failures: recyclebin_cleanup_failures,
        },
        replication: ReplicationMetrics {
            channels_running,
            channels_total,
            max_seconds_behind_source: max_lag,
        },
    }
}
