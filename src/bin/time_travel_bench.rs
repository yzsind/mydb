//! Generic time-travel benchmark for the persistent old-key sidecar.
//!
//! Environment knobs:
//!   MYDB_TIME_TRAVEL_ROWS   initial rows (default 100_000; use 1_000_000 for acceptance)
//!   MYDB_TIME_TRAVEL_OPS    point lookups per round (default 20_000)
//!   MYDB_TIME_TRAVEL_ROUNDS measured rounds (default 5)
//!   MYDB_TIME_TRAVEL_DIR    fresh data-directory path

use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use mydb::parser::{ColumnDef, DataType, IndexDef};
use mydb::storage::{
    Database, DurabilityMode, IndexBound, IndexKey, IsolationLevel, KeyPart, StartupValidationMode,
    StorageConfig, TxnView,
};
use mydb::types::{QueryError, Value};

fn main() -> Result<(), QueryError> {
    let rows = env_usize("MYDB_TIME_TRAVEL_ROWS", 100_000).max(1);
    let ops = env_usize("MYDB_TIME_TRAVEL_OPS", 20_000).max(1);
    let rounds = env_usize("MYDB_TIME_TRAVEL_ROUNDS", 5).max(1);
    let updated = (rows / 10).max(1);
    let range_width = env_usize("MYDB_TIME_TRAVEL_RANGE_WIDTH", 32).clamp(1, updated.max(1));
    let range_ops = (ops / 4).max(1_000);
    let dir = env::var_os("MYDB_TIME_TRAVEL_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            env::temp_dir().join(format!("mydb-time-travel-bench-{}", std::process::id()))
        });
    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(io_err)?;
    }

    println!(
        "[config] rows={rows} updated={updated} ops={ops} range_ops={range_ops} range_width={range_width} rounds={rounds} dir={}",
        dir.display()
    );
    let db = Database::open_with_config(dir.clone(), storage_config())?;
    create_schema(&db)?;
    bulk_load(&db, rows)?;
    let historical_scn = db
        .read_only_statement_view(IsolationLevel::ReadCommitted, None)
        .snapshot_scn;

    let wal_before = db.storage_status().wal_bytes;
    let update_started = Instant::now();
    let txn = db.begin_txn()?;
    db.update_rows_with_changed_columns(
        txn,
        "test.temporal_bench",
        None,
        Some(&[1, 2]),
        |values| Ok(matches!(values.first(), Some(Value::Int(id)) if *id >= 0 && (*id as usize) < updated)),
        |values| {
            let mut next = values.to_vec();
            if let Some(Value::Int(k)) = next.get_mut(1) {
                *k = k.saturating_add(rows as i64);
            }
            if let Some(Value::Int(payload)) = next.get_mut(2) {
                *payload = payload.saturating_add(1);
            }
            Ok(next)
        },
    )?;
    db.commit_txn(txn.txn_id)?;
    let update_elapsed = update_started.elapsed();
    let status = db.storage_status();
    println!(
        "[write] rows={} elapsed_ms={:.3} rows_per_s={:.0} wal_bytes_per_row={:.2} undo_bytes={} history_index_bytes={} history_entries={}",
        updated,
        update_elapsed.as_secs_f64() * 1_000.0,
        updated as f64 / update_elapsed.as_secs_f64(),
        status.wal_bytes.saturating_sub(wal_before) as f64 / updated as f64,
        status.undo_bytes,
        status.temporal_history_index_bytes,
        status.temporal_history_index_entries,
    );

    let table = db.table("test.temporal_bench")?;
    let index = table
        .indexes
        .iter()
        .find(|index| index.name == "idx_k")
        .expect("idx_k");
    let mut current_rounds = Vec::with_capacity(rounds);
    let mut history_rounds = Vec::with_capacity(rounds);
    let mut current_range_rounds = Vec::with_capacity(rounds);
    let mut history_range_rounds = Vec::with_capacity(rounds);
    // Warm both heap/undo and B-tree/history-sidecar pages before measured
    // rounds. Acceptance compares steady-state medians, not startup page-in.
    let warmup_ops = ops.min(1_024);
    let _ = point_round(
        &db,
        &table,
        index,
        db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
        warmup_ops,
        updated,
        rows as i64,
        u64::MAX - 1,
    )?;
    let _ = point_round(
        &db,
        &table,
        index,
        TxnView::historical(historical_scn),
        warmup_ops,
        updated,
        0,
        u64::MAX,
    )?;
    for round in 0..rounds {
        let current = point_round(
            &db,
            &table,
            index,
            db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
            ops,
            updated,
            rows as i64,
            round as u64,
        )?;
        let historical = point_round(
            &db,
            &table,
            index,
            TxnView::historical(historical_scn),
            ops,
            updated,
            0,
            round as u64,
        )?;
        let current_range = range_round(
            &db,
            &table,
            index,
            db.read_only_statement_view(IsolationLevel::ReadCommitted, None),
            range_ops,
            updated,
            rows as i64,
            range_width,
            round as u64,
        )?;
        let historical_range = range_round(
            &db,
            &table,
            index,
            TxnView::historical(historical_scn),
            range_ops,
            updated,
            0,
            range_width,
            round as u64,
        )?;
        println!(
            "[round {}] point_current_p95_us={:.2} point_history_p95_us={:.2} point_ratio={:.3} point_current_qps={:.0} point_history_qps={:.0} range_current_p95_us={:.2} range_history_p95_us={:.2} range_ratio={:.3}",
            round + 1,
            current.p95_us,
            historical.p95_us,
            historical.p95_us / current.p95_us.max(0.001),
            current.qps,
            historical.qps,
            current_range.p95_us,
            historical_range.p95_us,
            historical_range.p95_us / current_range.p95_us.max(0.001),
        );
        current_rounds.push(current);
        history_rounds.push(historical);
        current_range_rounds.push(current_range);
        history_range_rounds.push(historical_range);
    }
    current_rounds.sort_by(|a, b| a.p95_us.total_cmp(&b.p95_us));
    history_rounds.sort_by(|a, b| a.p95_us.total_cmp(&b.p95_us));
    current_range_rounds.sort_by(|a, b| a.p95_us.total_cmp(&b.p95_us));
    history_range_rounds.sort_by(|a, b| a.p95_us.total_cmp(&b.p95_us));
    let current = current_rounds[current_rounds.len() / 2];
    let historical = history_rounds[history_rounds.len() / 2];
    let current_range = current_range_rounds[current_range_rounds.len() / 2];
    let historical_range = history_range_rounds[history_range_rounds.len() / 2];
    println!(
        "[median] point_current_p95_us={:.2} point_history_p95_us={:.2} point_ratio={:.3} point_gate_2x={} range_current_p95_us={:.2} range_history_p95_us={:.2} range_ratio={:.3} range_gate_2x={} history_index_hits={} heap_fallbacks={}",
        current.p95_us,
        historical.p95_us,
        historical.p95_us / current.p95_us.max(0.001),
        historical.p95_us <= current.p95_us * 2.0,
        current_range.p95_us,
        historical_range.p95_us,
        historical_range.p95_us / current_range.p95_us.max(0.001),
        historical_range.p95_us <= current_range.p95_us * 2.0,
        db.storage_status().temporal_history_index_hits,
        db.storage_status().temporal_heap_fallbacks,
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn range_round(
    db: &Database,
    table: &mydb::storage::TableSchema,
    index: &mydb::storage::IndexSchema,
    view: TxnView,
    ops: usize,
    key_space: usize,
    key_offset: i64,
    width: usize,
    seed: u64,
) -> Result<RoundResult, QueryError> {
    let mut state = seed ^ 0xd1b5_4a32_d192_ed03;
    let mut samples = Vec::with_capacity(ops);
    let started = Instant::now();
    let start_space = key_space.saturating_sub(width).max(1);
    for _ in 0..ops {
        state = state
            .wrapping_mul(2862933555777941757)
            .wrapping_add(3037000493);
        let low = (state as usize % start_space) as i64 + key_offset;
        let high = low.saturating_add(width.saturating_sub(1) as i64);
        let one = Instant::now();
        let ids = db.range_index_bounds_visible(
            table,
            index,
            Some(IndexBound {
                key: IndexKey::from(vec![KeyPart::Int(low)]),
                inclusive: true,
            }),
            Some(IndexBound {
                key: IndexKey::from(vec![KeyPart::Int(high)]),
                inclusive: true,
            }),
            view,
        )?;
        for row_id in ids {
            let _ = db.read_row(table, row_id, view)?;
        }
        samples.push(one.elapsed().as_nanos() as u64);
    }
    let elapsed = started.elapsed();
    samples.sort_unstable();
    let p95 = samples[(samples.len() - 1) * 95 / 100] as f64 / 1_000.0;
    Ok(RoundResult {
        p95_us: p95,
        qps: ops as f64 / elapsed.as_secs_f64(),
    })
}

#[derive(Clone, Copy)]
struct RoundResult {
    p95_us: f64,
    qps: f64,
}

fn point_round(
    db: &Database,
    table: &mydb::storage::TableSchema,
    index: &mydb::storage::IndexSchema,
    view: TxnView,
    ops: usize,
    key_space: usize,
    key_offset: i64,
    seed: u64,
) -> Result<RoundResult, QueryError> {
    let mut state = seed ^ 0x9e37_79b9_7f4a_7c15;
    let mut samples = Vec::with_capacity(ops);
    let started = Instant::now();
    for _ in 0..ops {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key = (state as usize % key_space) as i64 + key_offset;
        let one = Instant::now();
        let ids =
            db.lookup_index_visible(table, index, IndexKey::from(vec![KeyPart::Int(key)]), view)?;
        for row_id in ids {
            let _ = db.read_row(table, row_id, view)?;
        }
        samples.push(one.elapsed().as_nanos() as u64);
    }
    let elapsed = started.elapsed();
    samples.sort_unstable();
    let p95 = samples[(samples.len() - 1) * 95 / 100] as f64 / 1_000.0;
    Ok(RoundResult {
        p95_us: p95,
        qps: ops as f64 / elapsed.as_secs_f64(),
    })
}

fn storage_config() -> StorageConfig {
    StorageConfig {
        durability: DurabilityMode::Strict,
        checkpoint_interval: Duration::from_secs(60),
        checkpoint_wal_bytes: 4 * 1024 * 1024 * 1024,
        lock_wait: Duration::from_secs(5),
        group_commit_window: Duration::from_micros(500),
        wal_segment_bytes: 256 * 1024 * 1024,
        buffer_pool_pages: 16_384,
        buffer_pool_io_threads: 2,
        buffer_pool_prefetch_pages: 16,
        undo_retention: Duration::from_secs(60),
        heap_update_reserve_bytes: 1024,
        startup_validation: StartupValidationMode::Off,
        startup_validate_indexes: false,
    }
}

fn create_schema(db: &Database) -> Result<(), QueryError> {
    db.create_table(
        "temporal_bench",
        vec![col("id"), col("k"), col("payload")],
        vec!["id".into()],
        vec![IndexDef {
            name: Some("idx_k".into()),
            columns: vec!["k".into()],
            column_prefix_lengths: Vec::new(),
            column_descending: Vec::new(),
            key_expressions: vec![None],
            key_expression_multi_valued: vec![false],
            unique: false,
            included_columns: Vec::new(),
            index_type: "BTREE".into(),
            visible: true,
            comment: String::new(),
            fulltext_parser: String::new(),
        }],
    )
}

fn col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type: DataType::BigInt,
        unsigned: false,
        zerofill: false,
        invisible: false,
        nullable: false,
        default: None,
        auto_increment: false,
        primary_key: false,
        unique: false,
        on_update: None,
        checks: Vec::new(),
        generated: None,
        charset: None,
        collation: None,
    }
}

fn bulk_load(db: &Database, rows: usize) -> Result<(), QueryError> {
    for start in (0..rows).step_by(1_000) {
        let end = (start + 1_000).min(rows);
        let txn = db.begin_txn()?;
        db.insert_rows(
            txn,
            "test.temporal_bench",
            (start..end)
                .map(|id| vec![Value::Int(id as i64), Value::Int(id as i64), Value::Int(0)])
                .collect(),
        )?;
        db.commit_txn(txn.txn_id)?;
    }
    Ok(())
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn io_err(err: std::io::Error) -> QueryError {
    mydb::storage::error(mydb::error::catalog::ER_GET_ERRNO, err.to_string())
}
