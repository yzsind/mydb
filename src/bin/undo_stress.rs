use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mydb::parser::{ColumnDef, DataType};
use mydb::storage::{
    Database, DurabilityMode, IsolationLevel, RowId, StartupValidationMode, StorageConfig,
};
use mydb::types::{QueryError, Value};

const ROWS: i64 = 1_000;

fn main() -> Result<(), QueryError> {
    let workload = env::args().nth(1).unwrap_or_else(|| "update-chain".into());
    let data_dir = env::var_os("MYDB_UNDO_STRESS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| env::temp_dir().join("mydb-undo-stress"));
    let _ = std::fs::remove_dir_all(&data_dir);
    let started = Instant::now();
    match workload.as_str() {
        "update-chain" => update_chain(&data_dir)?,
        "long-snapshot-purge" => long_snapshot_purge(&data_dir)?,
        "rollback-savepoint" => rollback_savepoint(&data_dir)?,
        "bulk-rollback" => bulk_rollback(&data_dir)?,
        "xa-recovery" => xa_recovery(&data_dir)?,
        "concurrent-writes" => concurrent_writes(&data_dir)?,
        other => {
            eprintln!(
                "unknown workload '{other}', expected update-chain|long-snapshot-purge|rollback-savepoint|bulk-rollback|xa-recovery|concurrent-writes"
            );
            std::process::exit(2);
        }
    }
    println!("elapsed_ms={}", started.elapsed().as_millis());
    Ok(())
}

fn open_db(data_dir: impl Into<PathBuf>) -> Result<Arc<Database>, QueryError> {
    // `MYDB_UNDO_STRESS_DURABILITY=memory|relaxed|strict` lets a probe
    // isolate which shared serializer dominates: `memory` skips WAL and
    // undo-segment persistence (undo still publishes into the in-memory
    // mirror under the global undo write lock), so comparing memory-vs-
    // relaxed throughput attributes a serialization stall to WAL vs undo.
    let durability = match std::env::var("MYDB_UNDO_STRESS_DURABILITY").as_deref() {
        Ok("memory") => DurabilityMode::Memory,
        Ok("strict") => DurabilityMode::Strict,
        _ => DurabilityMode::Relaxed,
    };
    let group_commit_window = std::env::var("MYDB_GROUP_COMMIT_US")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_micros)
        .unwrap_or(Duration::ZERO);
    Database::open_with_config(
        data_dir,
        StorageConfig {
            durability,
            checkpoint_interval: Duration::from_secs(60),
            checkpoint_wal_bytes: 1024 * 1024 * 1024,
            lock_wait: Duration::from_secs(5),
            group_commit_window,
            wal_segment_bytes: 64 * 1024 * 1024,
            buffer_pool_pages: 8_192,
            buffer_pool_io_threads: 2,
            buffer_pool_prefetch_pages: 16,
            undo_retention: Duration::from_millis(0),
            heap_update_reserve_bytes: 4096,
            startup_validation: StartupValidationMode::Auto,
            startup_validate_indexes: true,
        },
    )
    .map(Arc::new)
}

fn create_table(db: &Database) -> Result<(), QueryError> {
    db.create_table(
        "undo_s",
        vec![
            column("id", DataType::Int, false),
            column("v", DataType::Int, false),
            column("body", DataType::VarChar(Some(128)), false),
        ],
        vec!["id".into()],
        vec![],
    )
}

fn load_rows(db: &Database) -> Result<(), QueryError> {
    load_n_rows(db, ROWS)
}

fn load_n_rows(db: &Database, n: i64) -> Result<(), QueryError> {
    let txn = db.begin_txn()?;
    db.insert_rows(
        txn,
        "test.undo_s",
        (0..n)
            .map(|id| {
                vec![
                    Value::Int(id),
                    Value::Int(0),
                    Value::String(format!("row-{id}")),
                ]
            })
            .collect(),
    )?;
    db.commit_txn(txn.txn_id)?;
    Ok(())
}

struct StressMetrics {
    started: Instant,
    latencies_us: Vec<u128>,
}

impl StressMetrics {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            latencies_us: Vec::new(),
        }
    }

    fn record(&mut self, started: Instant) {
        self.latencies_us.push(started.elapsed().as_micros());
    }

    fn ops(&self) -> u64 {
        self.latencies_us.len() as u64
    }

    fn percentile_us(&self, pct: f64) -> u128 {
        if self.latencies_us.is_empty() {
            return 0;
        }
        let mut values = self.latencies_us.clone();
        values.sort_unstable();
        let idx = ((values.len() as f64 * pct).ceil() as usize)
            .saturating_sub(1)
            .min(values.len() - 1);
        values[idx]
    }

    fn throughput_ops_s(&self) -> f64 {
        let secs = self.started.elapsed().as_secs_f64();
        if secs == 0.0 {
            0.0
        } else {
            self.ops() as f64 / secs
        }
    }
}

fn update_chain(data_dir: &PathBuf) -> Result<(), QueryError> {
    let db = open_db(data_dir)?;
    create_table(&db)?;
    load_rows(&db)?;
    let mut metrics = StressMetrics::new();
    for round in 0..50 {
        let op_started = Instant::now();
        let txn = db.begin_txn()?;
        db.update_rows(
            txn,
            "test.undo_s",
            None,
            |_| Ok(true),
            |row| Ok(vec![row[0].clone(), Value::Int(round), row[2].clone()]),
        )?;
        db.commit_txn(txn.txn_id)?;
        metrics.record(op_started);
    }
    report(&db, "update_chain", &metrics);
    Ok(())
}

fn long_snapshot_purge(data_dir: &PathBuf) -> Result<(), QueryError> {
    let db = open_db(data_dir)?;
    create_table(&db)?;
    load_rows(&db)?;
    let snapshot = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let guard = db.register_statement_snapshot(snapshot.snapshot_scn);
    let mut metrics = StressMetrics::new();
    for round in 0..10 {
        let op_started = Instant::now();
        let txn = db.begin_txn()?;
        db.update_rows(
            txn,
            "test.undo_s",
            None,
            |row| Ok(matches!(row.first(), Some(Value::Int(id)) if id % 2 == 0)),
            |row| Ok(vec![row[0].clone(), Value::Int(round), row[2].clone()]),
        )?;
        db.commit_txn(txn.txn_id)?;
        db.checkpoint()?;
        metrics.record(op_started);
    }
    let visible = db.scan_rows("test.undo_s", snapshot)?;
    assert_eq!(visible.len(), ROWS as usize);
    drop(guard);
    db.checkpoint()?;
    report(&db, "long_snapshot_purge", &metrics);
    Ok(())
}

fn rollback_savepoint(data_dir: &PathBuf) -> Result<(), QueryError> {
    let db = open_db(data_dir)?;
    create_table(&db)?;
    load_rows(&db)?;
    let mut metrics = StressMetrics::new();
    let op_started = Instant::now();
    let txn = db.begin_txn()?;
    let savepoint = db.statement_savepoint(txn.txn_id)?;
    db.update_rows(
        txn,
        "test.undo_s",
        None,
        |_| Ok(true),
        |row| Ok(vec![row[0].clone(), Value::Int(99), row[2].clone()]),
    )?;
    db.rollback_to_statement_savepoint(txn.txn_id, savepoint)?;
    db.commit_txn(txn.txn_id)?;
    metrics.record(op_started);
    report(&db, "rollback_savepoint", &metrics);
    Ok(())
}

/// Exercise real row updates and measure rollback separately from setup.
fn bulk_rollback(data_dir: &PathBuf) -> Result<(), QueryError> {
    let rows = env::var("MYDB_UNDO_STRESS_ROWS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(1_000_001);
    assert!(rows > 0);
    let db = open_db(data_dir)?;
    create_table(&db)?;
    // Keep setup memory bounded independently of the transaction under test.
    for start in (0..rows).step_by(4096) {
        let txn = db.begin_txn()?;
        db.insert_rows(
            txn,
            "test.undo_s",
            (start..(start + 4096).min(rows))
                .map(|id| {
                    vec![
                        Value::Int(id),
                        Value::Int(0),
                        Value::String(format!("row-{id}")),
                    ]
                })
                .collect(),
        )?;
        db.commit_txn(txn.txn_id)?;
    }
    let txn = db.begin_txn()?;
    let started = Instant::now();
    let updated = db.update_rows(
        txn,
        "test.undo_s",
        None,
        |_| Ok(true),
        |row| Ok(vec![row[0].clone(), Value::Int(99), row[2].clone()]),
    )?;
    println!(
        "rows={rows} updated={updated} update_ms={}",
        started.elapsed().as_millis()
    );
    let started = Instant::now();
    let result = db.rollback_txn(txn.txn_id);
    println!(
        "rollback_ms={} result={result:?}",
        started.elapsed().as_millis()
    );
    result?;
    let view = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
    let restored = db.scan_rows("test.undo_s", view)?;
    assert_eq!(restored.len(), rows as usize);
    assert!(restored.iter().all(|row| row.values[1] == Value::Int(0)));
    println!("verified_rows={}", restored.len());
    if env::var("MYDB_UNDO_STRESS_REOPEN").as_deref() == Ok("1") {
        drop(restored);
        drop(db);
        let reopened = open_db(data_dir)?;
        let view = reopened.read_only_statement_view(IsolationLevel::ReadCommitted, None);
        let restored = reopened.scan_rows("test.undo_s", view)?;
        assert_eq!(restored.len(), rows as usize);
        assert!(restored.iter().all(|row| row.values[1] == Value::Int(0)));
        println!("reopen_verified_rows={}", restored.len());
    }
    Ok(())
}

fn xa_recovery(data_dir: &PathBuf) -> Result<(), QueryError> {
    let mut metrics = StressMetrics::new();
    let op_started = Instant::now();
    {
        let db = open_db(data_dir)?;
        create_table(&db)?;
        let txn = db.begin_txn()?;
        db.insert_rows(
            txn,
            "test.undo_s",
            vec![vec![
                Value::Int(1),
                Value::Int(10),
                Value::String("prepared".into()),
            ]],
        )?;
        let xid = mydb::xa::Xid::new(1, b"undo-stress".to_vec(), Vec::new())?;
        db.prepare_xa(txn.txn_id, txn.txn_id, xid)?;
        std::mem::forget(db);
    }
    let db = open_db(data_dir)?;
    let xid = mydb::xa::Xid::new(1, b"undo-stress".to_vec(), Vec::new())?;
    db.rollback_prepared(&xid)?;
    let rows = db.scan_rows(
        "test.undo_s",
        db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
    )?;
    assert!(rows.is_empty());
    metrics.record(op_started);
    report(&db, "xa_recovery", &metrics);
    Ok(())
}

/// Concurrent-write contention probe. `MYDB_UNDO_STRESS_THREADS` writer
/// threads each run `MYDB_UNDO_STRESS_ROUNDS` rounds over a DISJOINT row
/// partition (`id % threads == tid`), so txns never conflict on rows and
/// the only shared serialization point is the undo tablespace + WAL. Total
/// undo work is held constant across thread counts (each thread does
/// `rounds` rounds, so total = threads*rounds*(ROWS/threads) = rounds*ROWS
/// records regardless of `threads`), so comparing wall time at T=1 vs T=N
/// isolates how much the undo/commit path actually parallelizes.
fn concurrent_writes(data_dir: &PathBuf) -> Result<(), QueryError> {
    let threads: i64 = std::env::var("MYDB_UNDO_STRESS_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let rounds: i64 = std::env::var("MYDB_UNDO_STRESS_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    // Fixed work PER THREAD: each thread owns a disjoint contiguous chunk
    // of `chunk` rows and rewrites all of them every round for `rounds`
    // rounds. Adding threads adds proportional undo work, so aggregate
    // undo throughput should scale ~linearly with `threads` IF the undo
    // write path parallelizes. A plateau/decline is the contention signal
    // that would justify sharding the undo manager state. (The full-table
    // scan in `update_rows` grows with the table = threads*chunk, which
    // biases this test AGAINST high thread counts — so throughput that
    // still climbs is a strong "undo is not the bottleneck" result.)
    let chunk: i64 = std::env::var("MYDB_UNDO_STRESS_CHUNK")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let total_rows = threads * chunk;
    let db = open_db(data_dir)?;
    create_table(&db)?;
    load_n_rows(&db, total_rows)?;
    // Pre-resolve every row's heap RowId ONCE so the writer threads issue
    // scan-free point updates (`update_row_by_id`). This removes the
    // full-table-scan confound in `update_rows` (whose O(table) scan per op
    // grows with the thread count and otherwise dominates the wall time),
    // leaving the undo tablespace + WAL as the only shared serialization
    // point — the thing we actually want to measure.
    let table = db.table("test.undo_s")?;
    let mut id_to_rid: Vec<(i64, RowId)> = db
        .scan_rows(
            "test.undo_s",
            db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
        )?
        .into_iter()
        .filter_map(|r| match r.values.first() {
            Some(Value::Int(id)) => Some((*id, r.row_id)),
            _ => None,
        })
        .collect();
    id_to_rid.sort_by_key(|(id, _)| *id);
    let rids: Vec<RowId> = id_to_rid.into_iter().map(|(_, rid)| rid).collect();
    let started = Instant::now();
    let mut handles = Vec::new();
    for tid in 0..threads {
        let db = Arc::clone(&db);
        let table = Arc::clone(&table);
        let lo = (tid * chunk) as usize;
        let my_rids: Vec<RowId> = rids[lo..lo + chunk as usize].to_vec();
        // `MYDB_UNDO_STRESS_COMMIT_EVERY` rounds per commit (default 1).
        // Raising it amortizes begin/commit (TM lock) over more updates, so
        // comparing throughput across values attributes serialization to the
        // commit/TM path vs the per-update path (heap latch / buffer / alloc).
        let commit_every: i64 = std::env::var("MYDB_UNDO_STRESS_COMMIT_EVERY")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|v| *v >= 1)
            .unwrap_or(1);
        handles.push(std::thread::spawn(move || -> Result<u64, QueryError> {
            let mut rows_written = 0u64;
            let mut txn = db.begin_txn()?;
            for round in 0..rounds {
                for &rid in &my_rids {
                    db.update_row_by_id(txn, table.as_ref(), rid, move |row| {
                        Ok(vec![row[0].clone(), Value::Int(round), row[2].clone()])
                    })?;
                }
                rows_written += chunk as u64;
                if (round + 1) % commit_every == 0 && round + 1 < rounds {
                    db.commit_txn(txn.txn_id)?;
                    txn = db.begin_txn()?;
                }
            }
            db.commit_txn(txn.txn_id)?;
            Ok(rows_written)
        }));
    }
    let mut undo_records_written = 0u64;
    for h in handles {
        undo_records_written += h.join().expect("writer thread panicked")?;
    }
    let elapsed = started.elapsed();
    let status = db.storage_status();
    let (publock_ns, publock_samples) = db.undo_publish_lock_stats();
    let secs = elapsed.as_secs_f64();
    let undo_tps = if secs > 0.0 {
        undo_records_written as f64 / secs
    } else {
        0.0
    };
    let per_thread_tps = undo_tps / threads as f64;
    // What fraction of the total wall time (summed across threads) was spent
    // waiting for / holding the undo publish lock. total_thread_ns =
    // elapsed * threads (each thread ran the whole window).
    let total_thread_ns = elapsed.as_nanos() as f64 * threads as f64;
    let publock_pct = if total_thread_ns > 0.0 {
        publock_ns as f64 / total_thread_ns * 100.0
    } else {
        0.0
    };
    let publock_avg_ns = if publock_samples > 0 {
        publock_ns / publock_samples
    } else {
        0
    };
    println!(
        "workload=concurrent_writes threads={threads} rounds={rounds} chunk={chunk} \
         total_rows={total_rows} elapsed_ms={} undo_records_written={undo_records_written} \
         undo_records_per_s={undo_tps:.0} per_thread_records_per_s={per_thread_tps:.0} \
         publock_avg_ns={publock_avg_ns} publock_pct_of_walltime={publock_pct:.1} \
         undo_peak_records={} undo_disk_bytes={} \
         wal_bytes={} wal_syncs={} wal_sync_total_us={} wal_sync_avg_us={} \
         gc_batches={} gc_avg_batch={} gc_leader_wait_us={} gc_follower_wait_us={} \
         gc_syncop_us={} gc_hiwater_released={}",
        elapsed.as_millis(),
        status.undo_peak_records,
        status.undo_disk_bytes,
        status.wal_bytes,
        status.wal_syncs,
        status.wal_sync_total_us,
        status.wal_sync_avg_us,
        status.group_commit_batches,
        status.avg_group_commit_size,
        status.group_commit_leader_wait_total_us,
        status.group_commit_follower_wait_total_us,
        status.group_commit_sync_op_total_us,
        status.group_commit_high_water_released,
    );
    Ok(())
}

fn report(db: &Database, name: &str, metrics: &StressMetrics) {
    let status = db.storage_status();
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0);
    let purge_lag_ms = if status.undo_purge_last_unix_ms == 0 {
        0
    } else {
        now_ms.saturating_sub(status.undo_purge_last_unix_ms)
    };
    println!(
        "workload={name} ops={} throughput_ops_s={:.2} p95_us={} p99_us={} undo_records={} undo_bytes={} undo_peak_bytes={} undo_peak_records={} undo_disk_bytes={} undo_reclaimable_bytes={} undo_reclaimed_bytes={} undo_purge_runs={} undo_purge_errors={} undo_purge_lag_ms={} undo_cache_hits={} undo_cache_misses={}",
        metrics.ops(),
        metrics.throughput_ops_s(),
        metrics.percentile_us(0.95),
        metrics.percentile_us(0.99),
        status.undo_records,
        status.undo_bytes,
        status.undo_peak_bytes,
        status.undo_peak_records,
        status.undo_disk_bytes,
        status.undo_reclaimable_bytes,
        status.undo_reclaimed_bytes,
        status.undo_purge_runs,
        status.undo_purge_errors,
        purge_lag_ms,
        status.undo_cache_hits,
        status.undo_cache_misses
    );
}

fn column(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type,
        unsigned: false,
        zerofill: false,
        invisible: false,
        nullable,
        primary_key: false,
        unique: false,
        auto_increment: false,
        default: None,
        on_update: None,
        checks: Vec::new(),
        generated: None,
        charset: None,
        collation: None,
    }
}
