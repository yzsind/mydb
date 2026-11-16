//! Human-readable engine monitor and the independent raw diagnostic interface.

use std::fmt::{Display, Write};

use crate::engine::api::{QueryResponse, ResultSet, SessionContext};
use crate::engine::executor::mem_budget::{self, ControlledMemoryCategory};
use crate::engine::session_registry::SessionRegistry;
use crate::storage::Database;
use crate::types::Value;

mod metrics;

const DETAIL_LIMIT: usize = 10;
const RULE: &str = "----------------------------------------------------------------";

pub(crate) fn metric_pairs(db: &Database) -> Vec<(String, String)> {
    metrics::collect(db)
}

pub(crate) fn show(
    db: &Database,
    registry: &SessionRegistry,
    session: &SessionContext,
    engine: &str,
) -> QueryResponse {
    // Copy each source once. Formatting below holds no storage/session guards.
    let now = chrono::Utc::now();
    let now_ms = now.timestamp_millis().max(0) as u64;
    let uptime = crate::engine::status_counters::uptime_seconds();
    let st = db.storage_status();
    let details = db.engine_monitor_details(DETAIL_LIMIT);
    let mut txn_ids: Vec<_> = details
        .transactions
        .iter()
        .map(|t| t.transaction_id)
        .collect();
    for wait in &details.waits {
        txn_ids.extend([wait.requesting_transaction_id, wait.blocking_transaction_id]);
    }
    txn_ids.sort_unstable();
    txn_ids.dedup();
    let mut sessions = registry.engine_monitor_sessions(&txn_ids);
    // Direct API sessions need not have a protocol registry entry.
    if let Some(txn) = session.current_txn {
        if txn_ids.contains(&txn) && !sessions.iter().any(|(id, _, _)| *id == txn) {
            sessions.push((txn, session.connection_id, None));
        }
    }
    let connections = crate::mysql::server::connection_metrics();
    let (memory, memory_peak, categories, rejections) = mem_budget::controlled_memory_snapshot();
    let memory_limit = mem_budget::operational_memory_limit_bytes();
    let memory_target = mem_budget::memory_target_bytes();
    let rss_peak = crate::platform::memory::peak_resident_bytes();
    let xa_prepared = db.xa_status_counters().0;
    let caches = crate::engine::cache_registry::collect();
    let wal_sync_method = crate::wal::resolved_wal_sync_method();
    // Startup arms the checkpoint timer with a timestamp before any checkpoint
    // succeeds. Do not present that timer seed as a completed checkpoint.
    let last_checkpoint_time = (st.checkpoints_total > 0)
        .then_some(st.last_checkpoint_unix_ms)
        .flatten();

    let connection = |txn| {
        sessions
            .iter()
            .find(|(id, _, _)| *id == txn)
            .map(|(_, id, _)| id.to_string())
            .unwrap_or_else(|| "unknown".into())
    };
    let mut out = String::with_capacity(8192);
    writeln!(out, "{RULE}\nMYDB MONITOR OUTPUT\n{RULE}").unwrap();
    writeln!(
        out,
        "Collected: {} UTC",
        now.format("%Y-%m-%d %H:%M:%S%.3f")
    )
    .unwrap();
    writeln!(
        out,
        "Subsystems are near-time snapshots, not one atomic snapshot."
    )
    .unwrap();
    writeln!(
        out,
        "Counters are cumulative for their source lifetime; gauges are current."
    )
    .unwrap();

    section(&mut out, "OVERVIEW");
    line(&mut out, "Server uptime", format_args!("{uptime} s"));
    line(&mut out, "Durability", st.durability);
    line(
        &mut out,
        "Connections (active / limit)",
        format_args!(
            "{} / {}",
            connections.active, connections.effective_max_connections
        ),
    );
    line(
        &mut out,
        "Connections rejected (total)",
        connections.rejected,
    );

    section(&mut out, "TRANSACTIONS");
    line(
        &mut out,
        "Active storage transactions",
        st.active_transactions,
    );
    line(
        &mut out,
        "Oldest active transaction",
        format_args!("{} ms", st.oldest_active_txn_ms),
    );
    line(&mut out, "Oldest snapshot SCN", st.oldest_snapshot_scn);
    line(&mut out, "Prepared XA transactions", xa_prepared);
    writeln!(out, "Oldest transactions (up to {DETAIL_LIMIT}):").unwrap();
    if details.transactions.is_empty() {
        writeln!(out, "  none").unwrap();
    }
    for txn in &details.transactions {
        let access = if txn.read_only {
            "read-only"
        } else if txn.is_writer {
            "writer"
        } else {
            "read-write"
        };
        writeln!(
            out,
            "  trx={} connection={} state={} age={} ms access={} undo={}",
            txn.transaction_id,
            connection(txn.transaction_id),
            txn.state,
            now_ms.saturating_sub(txn.started_unix_ms),
            access,
            txn.has_undo
        )
        .unwrap();
        if let Some(sql) = sessions
            .iter()
            .find(|(id, _, _)| *id == txn.transaction_id)
            .and_then(|(_, _, sql)| sql.as_deref())
        {
            writeln!(out, "    SQL: {}", single_line(sql, 256)).unwrap();
        }
    }
    omitted(
        &mut out,
        details.transaction_count,
        details.transactions.len(),
    );

    section(&mut out, "LOCKS");
    line(&mut out, "Active lock waits", st.active_lock_waits);
    line(
        &mut out,
        "Oldest lock wait",
        format_args!("{} ms", st.oldest_lock_wait_ms),
    );
    line(
        &mut out,
        "Lock waits / deadlocks (total)",
        format_args!("{} / {}", st.lock_waits, st.deadlocks),
    );
    line(
        &mut out,
        "Wait time total / avg / max",
        format_args!(
            "{} / {} / {} ms",
            st.lock_wait_time_ms, st.lock_wait_time_avg_ms, st.lock_wait_time_max_ms
        ),
    );
    line(
        &mut out,
        "Configured lock wait timeout",
        format_args!("{} ms", st.lock_wait_ms),
    );
    writeln!(out, "Oldest wait edges (up to {DETAIL_LIMIT}):").unwrap();
    if details.waits.is_empty() {
        writeln!(out, "  none").unwrap();
    }
    for wait in &details.waits {
        let object = wait
            .object
            .as_ref()
            .map(|(schema, table)| format!("{schema}.{table}"))
            .unwrap_or_else(|| format!("table_id={}", wait.table_id));
        writeln!(
            out,
            "  trx={} connection={} blocked_by={} connection={} wait={} ms",
            wait.requesting_transaction_id,
            connection(wait.requesting_transaction_id),
            wait.blocking_transaction_id,
            connection(wait.blocking_transaction_id),
            now_ms.saturating_sub(wait.started_unix_ms)
        )
        .unwrap();
        writeln!(
            out,
            "    object={} resource={}",
            single_line(&object, 256),
            single_line(&wait.resource, 256)
        )
        .unwrap();
    }
    omitted(&mut out, details.wait_edge_count, details.waits.len());

    section(&mut out, "WAL AND CHECKPOINT");
    line(
        &mut out,
        "Next allocatable / durable LSN",
        format_args!("{} / {}", st.current_lsn, st.durable_lsn),
    );
    line(
        &mut out,
        "Checkpoint / redo lower bound LSN",
        format_args!("{} / {}", st.checkpoint_lsn, st.checkpoint_redo_lsn),
    );
    line(
        &mut out,
        "Checkpoint / redo age (LSN)",
        format_args!("{} / {}", st.checkpoint_age_lsn, st.checkpoint_redo_age_lsn),
    );
    line(&mut out, "Replica applied LSN", st.replica_applied_lsn);
    line(&mut out, "WAL size", bytes(st.wal_bytes));
    line(&mut out, "WAL sync method", wal_sync_method.as_str());
    line(&mut out, "WAL syncs (total)", st.wal_syncs);
    line(
        &mut out,
        "WAL sync avg / max",
        format_args!("{} / {} us", st.wal_sync_avg_us, st.wal_sync_max_us),
    );
    line(
        &mut out,
        "Group commit batches / avg size",
        format_args!("{} / {}", st.group_commit_batches, st.avg_group_commit_size),
    );
    line(
        &mut out,
        "Group commit waiters (total)",
        st.group_commit_waiters,
    );
    line(
        &mut out,
        "Checkpoints / failures (total)",
        format_args!(
            "{} / {}",
            st.checkpoints_total, st.checkpoint_failures_total
        ),
    );
    line(
        &mut out,
        "Preflush failures (total)",
        st.checkpoint_preflush_failures_total,
    );
    line(
        &mut out,
        "Last successful checkpoint",
        last_checkpoint_time
            .map(timestamp)
            .unwrap_or_else(|| "never".into()),
    );
    if last_checkpoint_time.is_some() {
        line(
            &mut out,
            "Last checkpoint kind / trigger",
            format_args!(
                "{} / {}",
                st.last_checkpoint_kind, st.last_checkpoint_trigger
            ),
        );
        line(
            &mut out,
            "Last checkpoint duration",
            format_args!("{} us", st.last_checkpoint_duration_us),
        );
        line(
            &mut out,
            "Last checkpoint WAL reclaimed",
            bytes(st.last_checkpoint_wal_reclaimed_bytes),
        );
    }

    section(&mut out, "BUFFER POOL AND I/O");
    line(
        &mut out,
        "Capacity / used / dirty (pages)",
        format_args!(
            "{} / {} / {}",
            st.buffer_pool_pages, st.buffer_pool_used_pages, st.buffer_pool_dirty_pages
        ),
    );
    line(
        &mut out,
        "Pinned / freeable (pages)",
        format_args!(
            "{} / {}",
            st.buffer_pool_pinned_pages, st.buffer_pool_freeable_pages
        ),
    );
    line(
        &mut out,
        "Buffer + storage scratch memory",
        bytes(
            (categories[ControlledMemoryCategory::BufferRaw as usize] as u64).saturating_add(
                categories[ControlledMemoryCategory::StorageScratch as usize] as u64,
            ),
        ),
    );
    line(
        &mut out,
        "Buffer hits / misses (total)",
        format_args!("{} / {}", st.buffer_hits, st.buffer_misses),
    );
    line(
        &mut out,
        "Buffer hit rate",
        hit_rate(st.buffer_hits, st.buffer_misses),
    );
    line(
        &mut out,
        "Pages read / written (total)",
        format_args!("{} / {}", st.buffer_pool_read_ios, st.buffer_pool_write_ios),
    );
    line(
        &mut out,
        "Evictions / flushes (total)",
        format_args!("{} / {}", st.buffer_pool_evictions, st.buffer_pool_flushes),
    );
    line(
        &mut out,
        "Buffer waits / time (total)",
        format_args!("{} / {} ms", st.buffer_pool_waits, st.buffer_pool_wait_ms),
    );
    line(
        &mut out,
        "Flush / background I/O errors",
        format_args!(
            "{} / {}",
            st.buffer_pool_flush_errors, st.buffer_pool_background_io_errors
        ),
    );

    section(&mut out, "MEMORY AND EXECUTION");
    line(
        &mut out,
        "Controlled / admission ceiling",
        format_args!("{} / {}", bytes(memory as u64), bytes(memory_limit as u64)),
    );
    line(
        &mut out,
        "Working-set memory target",
        bytes(memory_target as u64),
    );
    line(
        &mut out,
        "Controlled memory peak",
        bytes(memory_peak as u64),
    );
    line(
        &mut out,
        "Process RSS peak (OS)",
        rss_peak.map(bytes).unwrap_or_else(|| "unavailable".into()),
    );
    line(
        &mut out,
        "Executor memory",
        bytes(categories[ControlledMemoryCategory::Executor as usize] as u64),
    );
    line(&mut out, "Memory rejections (total)", rejections);
    line(
        &mut out,
        "Spill written / read (total)",
        format_args!(
            "{} / {}",
            bytes(st.spill_bytes_written),
            bytes(st.spill_bytes_read)
        ),
    );
    line(
        &mut out,
        "Inserted / updated / deleted rows",
        format_args!(
            "{} / {} / {}",
            st.dml_insert_rows, st.dml_update_rows, st.dml_delete_rows
        ),
    );
    writeln!(
        out,
        "  RSS and tracked pools have different accounting; do not add them."
    )
    .unwrap();

    section(&mut out, "UNDO AND PURGE");
    line(
        &mut out,
        "Undo resident / disk",
        format_args!(
            "{} / {}",
            bytes(st.undo_resident_bytes),
            bytes(st.undo_disk_bytes)
        ),
    );
    line(
        &mut out,
        "Undo records / purged (total)",
        format_args!("{} / {}", st.undo_records, st.undo_purged_records),
    );
    line(
        &mut out,
        "Undo retention",
        format_args!("{} ms", st.undo_retention_ms),
    );
    line(
        &mut out,
        "Retention / memory pressure",
        format_args!("{} / {}", st.undo_retention_pressure, st.undo_mem_pressure),
    );
    line(
        &mut out,
        "Purge runs / errors (total)",
        format_args!("{} / {}", st.undo_purge_runs, st.undo_purge_errors),
    );
    line(
        &mut out,
        "Largest undo transaction",
        format_args!(
            "{} ({})",
            st.undo_largest_txn_id,
            bytes(st.undo_largest_txn_bytes)
        ),
    );

    section(&mut out, "CACHES");
    line(
        &mut out,
        "Logical plan hits / misses",
        format_args!(
            "{} / {} ({}), entries unavailable",
            st.plan_cache_hits,
            st.plan_cache_misses,
            hit_rate(st.plan_cache_hits, st.plan_cache_misses)
        ),
    );
    for name in [
        "statement_ast",
        "physical_plan",
        "prepared_dml",
        "prepared_dml_batch",
    ] {
        if let Some(cache) = caches.iter().find(|cache| cache.name == name) {
            line(
                &mut out,
                name,
                format_args!(
                    "hits={} misses={} hit_rate={} entries={}",
                    cache.hits,
                    cache.misses,
                    hit_rate(cache.hits, cache.misses),
                    cache.size_entries
                ),
            );
        } else {
            line(&mut out, name, "not initialized");
        }
    }
    writeln!(out, "{RULE}\nEND OF MYDB MONITOR OUTPUT").unwrap();
    QueryResponse::ResultSet(ResultSet {
        columns: crate::engine::show::show_engine_mydb_status_columns(),
        rows: vec![vec![
            Value::String(
                if engine.eq_ignore_ascii_case("innodb") {
                    "InnoDB"
                } else {
                    "mydb"
                }
                .into(),
            ),
            Value::String(String::new()),
            Value::String(out),
        ]],
    })
}

fn section(out: &mut String, title: &str) {
    writeln!(out, "\n{RULE}\n{title}\n{RULE}").unwrap();
}

fn line(out: &mut String, label: &str, value: impl Display) {
    writeln!(out, "{label:<36}: {value}").unwrap();
}

fn omitted(out: &mut String, total: usize, shown: usize) {
    if total > shown {
        writeln!(out, "  ... {} more omitted", total - shown).unwrap();
    }
}

fn hit_rate(hits: u64, misses: u64) -> String {
    let total = u128::from(hits) + u128::from(misses);
    if total == 0 {
        "N/A".into()
    } else {
        format!("{:.2}%", hits as f64 * 100.0 / total as f64)
    }
}

fn bytes(value: u64) -> String {
    if value < 1024 {
        return format!("{value} B");
    }
    let mut scaled = value as f64;
    let mut unit = "B";
    for candidate in ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB"] {
        scaled /= 1024.0;
        unit = candidate;
        if scaled < 1024.0 {
            break;
        }
    }
    format!("{scaled:.2} {unit}")
}

fn timestamp(ms: u64) -> String {
    i64::try_from(ms)
        .ok()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|time| format!("{} UTC", time.format("%Y-%m-%d %H:%M:%S%.3f")))
        .unwrap_or_else(|| "unavailable".into())
}

fn single_line(value: &str, limit: usize) -> String {
    let mut chars = value.chars();
    let mut result = String::new();
    for ch in chars.by_ref().take(limit) {
        match ch {
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            '\\' => result.push_str("\\\\"),
            ch if ch.is_control() => write!(result, "\\u{{{:x}}}", ch as u32).unwrap(),
            ch => result.push(ch),
        }
    }
    if chars.next().is_some() {
        result.push_str("...");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monitor_formats_empty_large_and_control_values() {
        assert_eq!(hit_rate(0, 0), "N/A");
        assert_eq!(hit_rate(u64::MAX, u64::MAX), "50.00%");
        assert_eq!(bytes(0), "0 B");
        assert_eq!(bytes(1024), "1.00 KiB");
        assert_eq!(bytes(u64::MAX), "16.00 EiB");
        assert_eq!(timestamp(0), "1970-01-01 00:00:00.000 UTC");
        assert_eq!(timestamp(u64::MAX), "unavailable");
        assert_eq!(single_line("中\n\t\r\0\\文", 6), "中\\n\\t\\r\\u{0}\\\\...");
        assert_eq!(single_line("中文", 2), "中文");
    }
}
