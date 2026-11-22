//! Prometheus text exposition for the shared REST/MCP listener.
//!
//! The output follows the Prometheus text format 0.0.4. Families are emitted
//! in a fixed order with stable names and bounded label sets; no SQL text,
//! database object names, account names or API keys are ever included.

use crate::engine::api::QueryEngine;
use crate::engine::metrics::MetricsSnapshot;
use std::fmt::Display;

/// Transport-level counters owned by the shared REST/MCP runtime. These are
/// collected directly from the runtime state so they remain available even
/// when the engine has no storage metrics provider.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RuntimeMetrics {
    sessions: usize,
    inflight: usize,
    tracked_requests: usize,
    memory_bytes: usize,
    executed: u64,
    succeeded: u64,
    failed: u64,
    cancelled: u64,
    received_bytes: u64,
    sent_bytes: u64,
    elapsed_us: u64,
}

pub(crate) fn runtime_metrics<E: QueryEngine>(
    state: &crate::sql_service::State<E>,
) -> RuntimeMetrics {
    RuntimeMetrics {
        sessions: state.sessions.lock().len(),
        inflight: state
            .config
            .max_inflight
            .saturating_sub(state.inflight.available_permits()),
        tracked_requests: state.requests.lock().len(),
        memory_bytes: state.used.load(std::sync::atomic::Ordering::Acquire),
        executed: state.executed.load(std::sync::atomic::Ordering::Acquire),
        succeeded: state.succeeded.load(std::sync::atomic::Ordering::Acquire),
        failed: state.failed.load(std::sync::atomic::Ordering::Acquire),
        cancelled: state.cancelled.load(std::sync::atomic::Ordering::Acquire),
        received_bytes: state
            .received_bytes
            .load(std::sync::atomic::Ordering::Acquire),
        sent_bytes: state.sent_bytes.load(std::sync::atomic::Ordering::Acquire),
        elapsed_us: state.elapsed_us.load(std::sync::atomic::Ordering::Acquire),
    }
}

/// Render one scrape. `snapshot` is absent for engines without storage
/// metrics; the endpoint still exposes process readiness and transport
/// counters in that case.
pub(crate) fn render(
    snapshot: Option<&MetricsSnapshot>,
    runtime: &RuntimeMetrics,
    shutting_down: bool,
) -> String {
    let mut out = String::with_capacity(65536);
    let has_engine_snapshot = snapshot.is_some();
    let default = MetricsSnapshot::default();
    let snapshot = snapshot.unwrap_or(&default);

    family(
        &mut out,
        "mydb_up",
        "Whether the server process is running.",
        "gauge",
    );
    sample(&mut out, "mydb_up", &[], 1);
    family(
        &mut out,
        "mydb_ready",
        "Whether the server is accepting work and not shutting down.",
        "gauge",
    );
    sample(&mut out, "mydb_ready", &[], u64::from(!shutting_down));
    family(
        &mut out,
        "mydb_uptime_seconds",
        "Seconds since the server started.",
        "gauge",
    );
    let uptime_seconds = if has_engine_snapshot {
        snapshot.uptime_seconds
    } else {
        crate::engine::status_counters::uptime_seconds()
    };
    sample(&mut out, "mydb_uptime_seconds", &[], uptime_seconds);
    family(
        &mut out,
        "mydb_engine_metrics_available",
        "Whether engine-level metrics are available in this scrape (1) or not (0).",
        "gauge",
    );
    sample(
        &mut out,
        "mydb_engine_metrics_available",
        &[],
        u64::from(has_engine_snapshot),
    );
    if has_engine_snapshot {
        family(
            &mut out,
            "mydb_build_info",
            "Build and durability configuration.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_build_info",
            &[
                ("version", &snapshot.build.version),
                ("mysql_version", &snapshot.build.mysql_version),
                ("durability", &snapshot.build.durability),
                ("wal_sync_method", &snapshot.build.wal_sync_method),
            ],
            1,
        );
        let connections = &snapshot.connections;
        family(
            &mut out,
            "mydb_connections_active",
            "Currently connected clients.",
            "gauge",
        );
        sample(&mut out, "mydb_connections_active", &[], connections.active);
        family(
            &mut out,
            "mydb_connections_max_used",
            "Peak concurrent connections since startup.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_connections_max_used",
            &[],
            connections.max_used,
        );
        family(
            &mut out,
            "mydb_connections_limit",
            "Effective connection limit.",
            "gauge",
        );
        sample(&mut out, "mydb_connections_limit", &[], connections.limit);
        family(
            &mut out,
            "mydb_connections_accepted_total",
            "Accepted connection attempts.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_connections_accepted_total",
            &[],
            connections.accepted,
        );
        family(
            &mut out,
            "mydb_connections_rejected_total",
            "Connection attempts rejected by server policy.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_connections_rejected_total",
            &[],
            connections.rejected,
        );
        family(
            &mut out,
            "mydb_handshake_failures_total",
            "Connection handshakes that failed.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_handshake_failures_total",
            &[],
            connections.handshake_failures,
        );
        family(
            &mut out,
            "mydb_aborted_clients_total",
            "Clients that disconnected without a clean close.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_aborted_clients_total",
            &[],
            connections.aborted_clients,
        );
        family(
            &mut out,
            "mydb_threads_running",
            "Foreground sessions currently executing a command.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_threads_running",
            &[],
            connections.threads_running,
        );

        let queries = &snapshot.queries;
        family(
            &mut out,
            "mydb_questions_total",
            "Statements received from clients.",
            "counter",
        );
        sample(&mut out, "mydb_questions_total", &[], queries.questions);
        family(
            &mut out,
            "mydb_queries_total",
            "Queries executed by the server.",
            "counter",
        );
        sample(&mut out, "mydb_queries_total", &[], queries.queries);
        family(
            &mut out,
            "mydb_slow_queries_total",
            "Queries that exceeded the slow-query threshold.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_slow_queries_total",
            &[],
            queries.slow_queries,
        );
        family(
            &mut out,
            "mydb_network_received_bytes_total",
            "Client protocol bytes received.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_network_received_bytes_total",
            &[],
            queries.bytes_received,
        );
        family(
            &mut out,
            "mydb_network_sent_bytes_total",
            "Client protocol bytes sent.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_network_sent_bytes_total",
            &[],
            queries.bytes_sent,
        );
        family(
            &mut out,
            "mydb_commands_total",
            "Commands executed, by protocol command kind.",
            "counter",
        );
        for (command, value) in &snapshot.commands {
            sample(
                &mut out,
                "mydb_commands_total",
                &[("command", command)],
                *value,
            );
        }

        let transactions = &snapshot.transactions;
        family(
            &mut out,
            "mydb_transactions_active",
            "Storage transactions currently active.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_transactions_active",
            &[],
            transactions.active,
        );
        family(
            &mut out,
            "mydb_transactions_oldest_active_seconds",
            "Age of the oldest active transaction.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_transactions_oldest_active_seconds",
            &[],
            seconds_ms(transactions.oldest_active_ms),
        );
        family(
            &mut out,
            "mydb_transactions_oldest_snapshot_scn",
            "Oldest active snapshot SCN.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_transactions_oldest_snapshot_scn",
            &[],
            transactions.oldest_snapshot_scn,
        );
        family(
            &mut out,
            "mydb_xa_prepared",
            "Prepared XA transactions waiting for a decision.",
            "gauge",
        );
        sample(&mut out, "mydb_xa_prepared", &[], transactions.xa_prepared);
        family(
            &mut out,
            "mydb_snapshot_too_old_total",
            "Statements rejected because their snapshot was too old.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_snapshot_too_old_total",
            &[],
            transactions.snapshot_too_old,
        );

        let locks = &snapshot.locks;
        family(
            &mut out,
            "mydb_locks_row_waits_total",
            "Row-lock waits.",
            "counter",
        );
        sample(&mut out, "mydb_locks_row_waits_total", &[], locks.row_waits);
        family(
            &mut out,
            "mydb_locks_current_waits",
            "Transactions currently waiting for a row lock.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_locks_current_waits",
            &[],
            locks.active_waits,
        );
        family(
            &mut out,
            "mydb_locks_wait_seconds_total",
            "Cumulative row-lock wait time.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_locks_wait_seconds_total",
            &[],
            seconds_ms(locks.wait_time_ms),
        );
        family(
            &mut out,
            "mydb_locks_wait_seconds_max",
            "Longest observed row-lock wait.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_locks_wait_seconds_max",
            &[],
            seconds_ms(locks.wait_time_max_ms),
        );
        family(
            &mut out,
            "mydb_locks_deadlocks_total",
            "Detected deadlocks.",
            "counter",
        );
        sample(&mut out, "mydb_locks_deadlocks_total", &[], locks.deadlocks);

        let wal = &snapshot.wal;
        family(
            &mut out,
            "mydb_wal_size_bytes",
            "Current WAL size.",
            "gauge",
        );
        sample(&mut out, "mydb_wal_size_bytes", &[], wal.size_bytes);
        family(
            &mut out,
            "mydb_wal_current_lsn",
            "Current WAL log sequence number.",
            "gauge",
        );
        sample(&mut out, "mydb_wal_current_lsn", &[], wal.current_lsn);
        family(
            &mut out,
            "mydb_wal_durable_lsn",
            "Durable WAL log sequence number.",
            "gauge",
        );
        sample(&mut out, "mydb_wal_durable_lsn", &[], wal.durable_lsn);
        family(
            &mut out,
            "mydb_wal_durable_lag_lsn",
            "Appended but not yet durable WAL distance.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_wal_durable_lag_lsn",
            &[],
            wal.durable_lag_lsn,
        );
        family(
            &mut out,
            "mydb_wal_syncs_total",
            "WAL durability syncs.",
            "counter",
        );
        sample(&mut out, "mydb_wal_syncs_total", &[], wal.syncs);
        family(
            &mut out,
            "mydb_wal_sync_seconds_total",
            "Cumulative WAL sync latency.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_wal_sync_seconds_total",
            &[],
            seconds_us(wal.sync_total_us),
        );
        family(
            &mut out,
            "mydb_wal_sync_seconds_max",
            "Longest observed WAL sync latency.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_wal_sync_seconds_max",
            &[],
            seconds_us(wal.sync_max_us),
        );
        family(
            &mut out,
            "mydb_wal_physical_write_calls_total",
            "WAL physical write calls.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_wal_physical_write_calls_total",
            &[],
            wal.physical_write_calls,
        );
        family(
            &mut out,
            "mydb_wal_physical_write_bytes_total",
            "WAL physical bytes written.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_wal_physical_write_bytes_total",
            &[],
            wal.physical_write_bytes,
        );
        family(
            &mut out,
            "mydb_wal_logical_records_total",
            "WAL logical records appended.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_wal_logical_records_total",
            &[],
            wal.logical_records,
        );
        family(
            &mut out,
            "mydb_wal_rotations_total",
            "Completed WAL segment rotations.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_wal_rotations_total",
            &[],
            wal.rotation_count,
        );
        family(
            &mut out,
            "mydb_wal_rotation_critical_seconds_total",
            "Cumulative time in the rotation critical path.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_wal_rotation_critical_seconds_total",
            &[],
            seconds_us(wal.rotation_critical_total_us),
        );
        family(
            &mut out,
            "mydb_wal_rotation_critical_seconds_max",
            "Longest observed rotation critical path.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_wal_rotation_critical_seconds_max",
            &[],
            seconds_us(wal.rotation_critical_max_us),
        );
        family(
            &mut out,
            "mydb_wal_pending_sealed_sync",
            "Sealed WAL segments awaiting durability sync.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_wal_pending_sealed_sync",
            &[],
            wal.pending_sealed_sync,
        );
        family(
            &mut out,
            "mydb_wal_segments",
            "WAL segments currently present.",
            "gauge",
        );
        sample(&mut out, "mydb_wal_segments", &[], wal.segments);

        let group = &snapshot.group_commit;
        family(
            &mut out,
            "mydb_group_commit_batches_total",
            "Durability groups committed.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_group_commit_batches_total",
            &[],
            group.batches,
        );
        family(
            &mut out,
            "mydb_group_commit_average_batch_size",
            "Average commits per durability group.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_group_commit_average_batch_size",
            &[],
            group.average_batch_size,
        );
        family(
            &mut out,
            "mydb_group_commit_queue_depth",
            "Current durability queue depth.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_group_commit_queue_depth",
            &[],
            group.queue_depth,
        );
        family(
            &mut out,
            "mydb_group_commit_queue_depth_peak",
            "Peak durability queue depth.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_group_commit_queue_depth_peak",
            &[],
            group.queue_depth_peak,
        );
        family(
            &mut out,
            "mydb_group_commit_wait_seconds_total",
            "Cumulative commit wait time inside durability groups.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_group_commit_wait_seconds_total",
            &[("role", "leader")],
            seconds_us(group.leader_wait_total_us),
        );
        sample(
            &mut out,
            "mydb_group_commit_wait_seconds_total",
            &[("role", "follower")],
            seconds_us(group.follower_wait_total_us),
        );
        family(
            &mut out,
            "mydb_group_commit_wait_seconds_max",
            "Longest observed commit wait inside a durability group.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_group_commit_wait_seconds_max",
            &[("role", "leader")],
            seconds_us(group.leader_wait_max_us),
        );
        sample(
            &mut out,
            "mydb_group_commit_wait_seconds_max",
            &[("role", "follower")],
            seconds_us(group.follower_wait_max_us),
        );
        family(
            &mut out,
            "mydb_group_commit_sync_seconds_total",
            "Cumulative sync operation time in durability groups.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_group_commit_sync_seconds_total",
            &[],
            seconds_us(group.sync_total_us),
        );
        family(
            &mut out,
            "mydb_group_commit_sync_seconds_max",
            "Longest observed sync operation in a durability group.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_group_commit_sync_seconds_max",
            &[],
            seconds_us(group.sync_max_us),
        );

        let checkpoint = &snapshot.checkpoints;
        family(
            &mut out,
            "mydb_checkpoints_total",
            "Completed checkpoints.",
            "counter",
        );
        sample(&mut out, "mydb_checkpoints_total", &[], checkpoint.total);
        family(
            &mut out,
            "mydb_checkpoint_failures_total",
            "Failed checkpoints.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_checkpoint_failures_total",
            &[],
            checkpoint.failures,
        );
        family(
            &mut out,
            "mydb_checkpoint_preflush_failures_total",
            "Checkpoint preflush failures.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_checkpoint_preflush_failures_total",
            &[],
            checkpoint.preflush_failures,
        );
        family(
            &mut out,
            "mydb_checkpoint_age_lsn",
            "WAL distance since the last checkpoint.",
            "gauge",
        );
        sample(&mut out, "mydb_checkpoint_age_lsn", &[], checkpoint.age_lsn);
        family(
            &mut out,
            "mydb_checkpoint_redo_age_lsn",
            "Redo distance since the last checkpoint.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_checkpoint_redo_age_lsn",
            &[],
            checkpoint.redo_age_lsn,
        );
        family(
            &mut out,
            "mydb_checkpoint_duration_seconds_total",
            "Cumulative checkpoint duration.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_checkpoint_duration_seconds_total",
            &[],
            seconds_us(checkpoint.duration_total_us),
        );
        family(
            &mut out,
            "mydb_checkpoint_duration_seconds_max",
            "Longest checkpoint duration.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_checkpoint_duration_seconds_max",
            &[],
            seconds_us(checkpoint.duration_max_us),
        );
        family(
            &mut out,
            "mydb_checkpoint_last_duration_seconds",
            "Duration of the most recent checkpoint.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_checkpoint_last_duration_seconds",
            &[],
            seconds_us(checkpoint.last_duration_us),
        );
        family(
            &mut out,
            "mydb_checkpoint_last_phase_seconds",
            "Time spent in each phase of the most recent checkpoint.",
            "gauge",
        );
        for (phase, value) in [
            ("snapshot", checkpoint.last_snapshot_us),
            ("flush", checkpoint.last_flush_us),
            ("sync", checkpoint.last_sync_us),
            ("compact", checkpoint.last_compact_us),
        ] {
            sample(
                &mut out,
                "mydb_checkpoint_last_phase_seconds",
                &[("phase", phase)],
                seconds_us(value),
            );
        }
        family(
            &mut out,
            "mydb_checkpoint_last_success_timestamp_seconds",
            "Unix time of the most recent completed checkpoint; NaN before the first checkpoint.",
            "gauge",
        );
        match checkpoint.last_success_unix_ms {
            Some(unix_ms) => sample(
                &mut out,
                "mydb_checkpoint_last_success_timestamp_seconds",
                &[],
                seconds_ms(unix_ms),
            ),
            None => sample(
                &mut out,
                "mydb_checkpoint_last_success_timestamp_seconds",
                &[],
                "NaN",
            ),
        }
        family(
            &mut out,
            "mydb_wal_bytes_since_checkpoint",
            "WAL bytes accumulated since the most recent checkpoint.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_wal_bytes_since_checkpoint",
            &[],
            checkpoint.wal_bytes_since_checkpoint,
        );
        family(
            &mut out,
            "mydb_checkpoint_wal_threshold_bytes",
            "Configured WAL growth threshold for a size-triggered checkpoint.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_checkpoint_wal_threshold_bytes",
            &[],
            checkpoint.wal_threshold_bytes,
        );
        family(
            &mut out,
            "mydb_checkpoint_last_dirty_pages",
            "Dirty pages included in the most recent checkpoint.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_checkpoint_last_dirty_pages",
            &[],
            checkpoint.last_dirty_pages,
        );
        family(
            &mut out,
            "mydb_checkpoint_last_wal_reclaimed_bytes",
            "WAL bytes reclaimed by the most recent checkpoint.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_checkpoint_last_wal_reclaimed_bytes",
            &[],
            checkpoint.last_wal_reclaimed_bytes,
        );
        family(
            &mut out,
            "mydb_checkpoint_wal_reclaimed_bytes_total",
            "WAL bytes reclaimed by checkpoints.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_checkpoint_wal_reclaimed_bytes_total",
            &[],
            checkpoint.wal_reclaimed_bytes_total,
        );
        family(
            &mut out,
            "mydb_restartpoints_total",
            "Completed restartpoints.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_restartpoints_total",
            &[],
            checkpoint.restartpoints,
        );

        let buffer = &snapshot.buffer_pool;
        family(
            &mut out,
            "mydb_buffer_pool_pages",
            "Buffer-pool pages by state.",
            "gauge",
        );
        for (state, value) in [
            ("total", buffer.pages),
            ("used", buffer.used_pages),
            ("dirty", buffer.dirty_pages),
            ("freeable", buffer.freeable_pages),
            ("pinned", buffer.pinned_pages),
        ] {
            sample(
                &mut out,
                "mydb_buffer_pool_pages",
                &[("state", state)],
                value,
            );
        }
        family(
            &mut out,
            "mydb_buffer_pool_hit_ratio",
            "Buffer-pool hit ratio in the range 0..1; NaN when there have been no accesses.",
            "gauge",
        );
        let accesses = buffer.hits.saturating_add(buffer.misses);
        let hit_ratio = if accesses == 0 {
            "NaN".to_string()
        } else {
            format!("{:.6}", buffer.hits as f64 / accesses as f64)
        };
        sample(&mut out, "mydb_buffer_pool_hit_ratio", &[], hit_ratio);
        family(
            &mut out,
            "mydb_buffer_pool_hits_total",
            "Buffer-pool hits.",
            "counter",
        );
        sample(&mut out, "mydb_buffer_pool_hits_total", &[], buffer.hits);
        family(
            &mut out,
            "mydb_buffer_pool_misses_total",
            "Buffer-pool misses.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_buffer_pool_misses_total",
            &[],
            buffer.misses,
        );
        family(
            &mut out,
            "mydb_buffer_pool_evictions_total",
            "Buffer-pool evictions.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_buffer_pool_evictions_total",
            &[],
            buffer.evictions,
        );
        family(
            &mut out,
            "mydb_buffer_pool_flushes_total",
            "Buffer-pool flushes.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_buffer_pool_flushes_total",
            &[],
            buffer.flushes,
        );
        family(
            &mut out,
            "mydb_buffer_pool_write_requests_total",
            "Buffer-pool write requests.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_buffer_pool_write_requests_total",
            &[],
            buffer.write_requests,
        );
        family(
            &mut out,
            "mydb_buffer_pool_read_io_total",
            "Buffer-pool read I/O operations.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_buffer_pool_read_io_total",
            &[],
            buffer.read_ios,
        );
        family(
            &mut out,
            "mydb_buffer_pool_write_io_total",
            "Buffer-pool write I/O operations.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_buffer_pool_write_io_total",
            &[],
            buffer.write_ios,
        );
        family(
            &mut out,
            "mydb_buffer_pool_waits_total",
            "Buffer-pool capacity waits.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_buffer_pool_waits_total",
            &[],
            buffer.wait_count,
        );
        family(
            &mut out,
            "mydb_buffer_pool_wait_seconds_total",
            "Cumulative buffer-pool capacity wait time.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_buffer_pool_wait_seconds_total",
            &[],
            seconds_ms(buffer.wait_ms),
        );
        family(
            &mut out,
            "mydb_buffer_pool_flush_errors_total",
            "Buffer-pool flush errors.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_buffer_pool_flush_errors_total",
            &[],
            buffer.flush_errors,
        );
        family(
            &mut out,
            "mydb_buffer_pool_background_io_errors_total",
            "Buffer-pool background I/O errors.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_buffer_pool_background_io_errors_total",
            &[],
            buffer.background_io_errors,
        );
        family(
            &mut out,
            "mydb_buffer_pool_checkpoint_flush_pages",
            "Pages flushed by the most recent full buffer-pool flush.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_buffer_pool_checkpoint_flush_pages",
            &[],
            buffer.checkpoint_flush_pages,
        );
        family(
            &mut out,
            "mydb_buffer_pool_checkpoint_flush_seconds",
            "Duration of the most recent full buffer-pool flush.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_buffer_pool_checkpoint_flush_seconds",
            &[],
            seconds_us(buffer.checkpoint_flush_us),
        );
        family(
            &mut out,
            "mydb_buffer_pool_checkpoint_preflush_pages_total",
            "Pages flushed by checkpoint preflush.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_buffer_pool_checkpoint_preflush_pages_total",
            &[],
            buffer.checkpoint_preflush_pages,
        );
        family(
            &mut out,
            "mydb_buffer_pool_checkpoint_preflush_seconds_total",
            "Cumulative checkpoint preflush time.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_buffer_pool_checkpoint_preflush_seconds_total",
            &[],
            seconds_us(buffer.checkpoint_preflush_us),
        );
        family(
            &mut out,
            "mydb_buffer_pool_file_syncs_total",
            "Data-file sync operations.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_buffer_pool_file_syncs_total",
            &[],
            buffer.file_syncs,
        );
        family(
            &mut out,
            "mydb_buffer_pool_file_sync_skipped_total",
            "Data-file sync operations skipped.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_buffer_pool_file_sync_skipped_total",
            &[],
            buffer.file_sync_skipped,
        );

        let memory = &snapshot.memory;
        family(
            &mut out,
            "mydb_memory_controlled_bytes",
            "Memory currently charged to the controlled memory governor.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_memory_controlled_bytes",
            &[],
            memory.controlled_bytes,
        );
        family(
            &mut out,
            "mydb_memory_controlled_peak_bytes",
            "Peak controlled memory.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_memory_controlled_peak_bytes",
            &[],
            memory.controlled_peak_bytes,
        );
        family(
            &mut out,
            "mydb_memory_controlled_limit_bytes",
            "Operational controlled memory limit.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_memory_controlled_limit_bytes",
            &[],
            memory.controlled_limit_bytes,
        );
        family(
            &mut out,
            "mydb_memory_controlled_target_bytes",
            "Controlled memory target.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_memory_controlled_target_bytes",
            &[],
            memory.controlled_target_bytes,
        );
        family(
            &mut out,
            "mydb_memory_controlled_rejections_total",
            "Memory admission rejections.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_memory_controlled_rejections_total",
            &[],
            memory.controlled_rejections,
        );
        family(
            &mut out,
            "mydb_process_peak_resident_bytes",
            "Peak process resident memory.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_process_peak_resident_bytes",
            &[],
            memory.process_peak_resident_bytes,
        );

        let undo = &snapshot.undo;
        family(&mut out, "mydb_undo_bytes", "Current undo bytes.", "gauge");
        sample(&mut out, "mydb_undo_bytes", &[], undo.bytes);
        family(
            &mut out,
            "mydb_undo_records",
            "Current undo records.",
            "gauge",
        );
        sample(&mut out, "mydb_undo_records", &[], undo.records);
        family(
            &mut out,
            "mydb_undo_peak_bytes",
            "Peak undo bytes.",
            "gauge",
        );
        sample(&mut out, "mydb_undo_peak_bytes", &[], undo.peak_bytes);
        family(
            &mut out,
            "mydb_undo_retention_pressure_total",
            "Undo retention pressure events.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_undo_retention_pressure_total",
            &[],
            undo.retention_pressure,
        );

        let spill = &snapshot.spill;
        family(
            &mut out,
            "mydb_spill_bytes_written_total",
            "Bytes written to spill files.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_spill_bytes_written_total",
            &[],
            spill.bytes_written,
        );
        family(
            &mut out,
            "mydb_spill_bytes_read_total",
            "Bytes read from spill files.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_spill_bytes_read_total",
            &[],
            spill.bytes_read,
        );

        let disk = &snapshot.disk;
        family(
            &mut out,
            "mydb_disk_pressure_enabled",
            "Whether disk-pressure monitoring is active (1) or not (0).",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_disk_pressure_enabled",
            &[],
            u64::from(disk.pressure_enabled),
        );
        family(
            &mut out,
            "mydb_disk_write_limited",
            "Whether disk pressure is limiting writes (1) or not (0).",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_disk_write_limited",
            &[],
            u64::from(disk.write_limited),
        );
        family(
            &mut out,
            "mydb_disk_available_bytes",
            "Disk bytes available to the server; NaN when disk-pressure monitoring is disabled.",
            "gauge",
        );
        if disk.pressure_enabled {
            sample(
                &mut out,
                "mydb_disk_available_bytes",
                &[],
                disk.available_bytes,
            );
        } else {
            sample(&mut out, "mydb_disk_available_bytes", &[], "NaN");
        }
        family(
            &mut out,
            "mydb_disk_recovery_reserve_armed",
            "Whether the disk recovery reserve is armed (1) or not (0).",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_disk_recovery_reserve_armed",
            &[],
            u64::from(disk.recovery_reserve_armed),
        );

        let recyclebin = &snapshot.recyclebin;
        family(
            &mut out,
            "mydb_recyclebin_pressure",
            "Whether recycle-bin pressure is currently active (1) or not (0).",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_recyclebin_pressure",
            &[],
            recyclebin.pressure,
        );
        family(
            &mut out,
            "mydb_recyclebin_purges_total",
            "Recycle-bin pressure purges.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_recyclebin_purges_total",
            &[],
            recyclebin.purges,
        );
        family(
            &mut out,
            "mydb_recyclebin_cleanup_failures_total",
            "Recycle-bin cleanup failures.",
            "counter",
        );
        sample(
            &mut out,
            "mydb_recyclebin_cleanup_failures_total",
            &[],
            recyclebin.cleanup_failures,
        );

        let replication = &snapshot.replication;
        family(
            &mut out,
            "mydb_replication_channels_running",
            "Replication channels whose I/O thread is running.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_replication_channels_running",
            &[],
            replication.channels_running,
        );
        family(
            &mut out,
            "mydb_replication_channels_configured",
            "Configured replication channels, including stopped channels.",
            "gauge",
        );
        sample(
            &mut out,
            "mydb_replication_channels_configured",
            &[],
            replication.channels_total,
        );
        family(
            &mut out,
            "mydb_replication_seconds_behind_source_max",
            "Largest reported replication lag across channels; NaN when no channel reports lag.",
            "gauge",
        );
        match replication.max_seconds_behind_source {
            Some(lag) => sample(
                &mut out,
                "mydb_replication_seconds_behind_source_max",
                &[],
                lag,
            ),
            None => sample(
                &mut out,
                "mydb_replication_seconds_behind_source_max",
                &[],
                "NaN",
            ),
        }
    }
    family(
        &mut out,
        "mydb_http_sessions",
        "Persistent SQL sessions on the shared REST/MCP listener.",
        "gauge",
    );
    sample(&mut out, "mydb_http_sessions", &[], runtime.sessions as u64);
    family(
        &mut out,
        "mydb_http_inflight",
        "In-flight requests on the shared REST/MCP listener.",
        "gauge",
    );
    sample(&mut out, "mydb_http_inflight", &[], runtime.inflight as u64);
    family(
        &mut out,
        "mydb_http_requests_tracked",
        "Requests tracked for status/cancellation (in-flight and retained).",
        "gauge",
    );
    sample(
        &mut out,
        "mydb_http_requests_tracked",
        &[],
        runtime.tracked_requests as u64,
    );
    family(
        &mut out,
        "mydb_http_memory_bytes",
        "Memory currently charged to shared REST/MCP protocol work.",
        "gauge",
    );
    sample(
        &mut out,
        "mydb_http_memory_bytes",
        &[],
        runtime.memory_bytes as u64,
    );
    family(
        &mut out,
        "mydb_http_requests_total",
        "Completed SQL requests on the shared REST/MCP listener, by outcome.",
        "counter",
    );
    for (outcome, value) in [("succeeded", runtime.succeeded), ("failed", runtime.failed)] {
        sample(
            &mut out,
            "mydb_http_requests_total",
            &[("outcome", outcome)],
            value,
        );
    }
    family(
        &mut out,
        "mydb_http_statements_total",
        "SQL statements attempted on the shared REST/MCP listener.",
        "counter",
    );
    sample(
        &mut out,
        "mydb_http_statements_total",
        &[],
        runtime.executed,
    );
    family(
        &mut out,
        "mydb_http_cancel_requests_total",
        "Accepted cancellation requests on the shared REST/MCP listener.",
        "counter",
    );
    sample(
        &mut out,
        "mydb_http_cancel_requests_total",
        &[],
        runtime.cancelled,
    );
    family(
        &mut out,
        "mydb_http_received_bytes_total",
        "Request body bytes read on the shared REST/MCP listener.",
        "counter",
    );
    sample(
        &mut out,
        "mydb_http_received_bytes_total",
        &[],
        runtime.received_bytes,
    );
    family(
        &mut out,
        "mydb_http_sent_bytes_total",
        "Response body bytes written on the shared REST/MCP listener.",
        "counter",
    );
    sample(
        &mut out,
        "mydb_http_sent_bytes_total",
        &[],
        runtime.sent_bytes,
    );
    family(
        &mut out,
        "mydb_http_request_seconds_total",
        "Cumulative request execution time on the shared REST/MCP listener.",
        "counter",
    );
    sample(
        &mut out,
        "mydb_http_request_seconds_total",
        &[],
        seconds_us(runtime.elapsed_us),
    );

    out
}

fn family(out: &mut String, name: &str, help: &str, kind: &str) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    escape_help(out, help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(kind);
    out.push('\n');
}

fn sample(out: &mut String, name: &str, labels: &[(&str, &str)], value: impl Display) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (index, (key, value)) in labels.iter().enumerate() {
            if index != 0 {
                out.push(',');
            }
            out.push_str(key);
            out.push_str("=\"");
            escape_label(out, value);
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    use std::fmt::Write;
    write!(out, "{value}").expect("writing metrics into a String cannot fail");
    out.push('\n');
}

fn escape_help(out: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
}

fn escape_label(out: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
}

fn seconds_us(microseconds: u64) -> String {
    format!(
        "{}.{:06}",
        microseconds / 1_000_000,
        microseconds % 1_000_000
    )
}

fn seconds_ms(milliseconds: u64) -> String {
    format!("{}.{:03}", milliseconds / 1_000, milliseconds % 1_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposition_escapes_labels_and_formats_seconds() {
        let mut snapshot = MetricsSnapshot::default();
        snapshot.build.version = "1.0.1".into();
        snapshot.checkpoints.last_flush_us = 2_500_123;
        snapshot.commands = vec![("select".into(), 7)];
        let mut runtime = RuntimeMetrics::default();
        runtime.executed = 5;
        runtime.succeeded = 3;
        runtime.failed = 2;
        runtime.cancelled = 1;
        let text = render(Some(&snapshot), &runtime, false);
        assert!(text.contains("mydb_up 1\n"));
        assert!(text.contains("mydb_ready 1\n"));
        assert!(text.contains("mydb_checkpoint_last_phase_seconds{phase=\"flush\"} 2.500123\n"));
        assert!(text.contains("mydb_commands_total{command=\"select\"} 7\n"));
        assert!(text.contains("# TYPE mydb_wal_syncs_total counter\n"));
        // Requests and statements are different units and must not share a
        // family; cancellation is a separate counter as well.
        assert!(text.contains("mydb_http_requests_total{outcome=\"succeeded\"} 3\n"));
        assert!(text.contains("mydb_http_requests_total{outcome=\"failed\"} 2\n"));
        assert!(text.contains("mydb_http_statements_total 5\n"));
        assert!(text.contains("mydb_http_cancel_requests_total 1\n"));
        // Per-flush values are gauges, not monotonic counters.
        assert!(text.contains("# TYPE mydb_buffer_pool_checkpoint_flush_pages gauge\n"));
        assert!(text.contains("# TYPE mydb_buffer_pool_checkpoint_flush_seconds gauge\n"));
        // Unimplemented diagnostics must not be exported as healthy zeros.
        assert!(!text.contains("mydb_latch_waits_total"));
        assert!(!text.contains("mydb_undo_purge_errors_total"));
    }

    #[test]
    fn exposition_reports_shutdown_readiness() {
        let text = render(None, &RuntimeMetrics::default(), true);
        assert!(text.contains("mydb_ready 0\n"));
        assert!(text.contains("mydb_engine_metrics_available 0\n"));
        assert!(!text.contains("mydb_wal_syncs_total"));
        assert!(!text.contains("mydb_build_info{"));
    }

    #[test]
    fn exposition_is_structurally_valid_with_unique_series() {
        use std::collections::HashSet;
        let mut snapshot = MetricsSnapshot::default();
        snapshot.build.version = "1.0.1".into();
        snapshot.build.mysql_version = "8.0.46-mydb-1.0.1".into();
        snapshot.build.durability = "strict".into();
        snapshot.build.wal_sync_method = "fdatasync".into();
        snapshot.commands = vec![("select".into(), 1), ("insert".into(), 2)];
        let text = render(Some(&snapshot), &RuntimeMetrics::default(), false);
        let mut help = HashSet::new();
        let mut types = std::collections::HashMap::new();
        let mut series = HashSet::new();
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("# HELP ") {
                let name = rest.split_once(' ').unwrap().0;
                assert!(help.insert(name.to_string()), "duplicate HELP: {name}");
            } else if let Some(rest) = line.strip_prefix("# TYPE ") {
                let (name, kind) = rest.split_once(' ').unwrap();
                assert!(matches!(kind, "counter" | "gauge"), "{line}");
                assert!(
                    types.insert(name.to_string(), kind).is_none(),
                    "duplicate TYPE: {name}"
                );
            } else if !line.starts_with('#') {
                let (sample, value) = line.rsplit_once(' ').expect(line);
                assert!(value.parse::<f64>().is_ok(), "invalid value: {line}");
                let name = sample.split('{').next().unwrap();
                let mut chars = name.chars();
                assert!(
                    chars
                        .next()
                        .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_')
                        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_'),
                    "invalid metric name: {name}"
                );
                assert!(help.contains(name), "missing HELP: {name}");
                assert!(types.contains_key(name), "missing TYPE: {name}");
                assert!(
                    series.insert(sample.to_string()),
                    "duplicate series: {sample}"
                );
            }
        }
        for (name, kind) in types {
            if kind == "counter" {
                assert!(name.ends_with("_total"), "counter lacks _total: {name}");
            }
        }
    }
}
