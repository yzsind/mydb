use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use mydb::parser::{ColumnDef, DataType, IndexDef};
use mydb::storage::{
    Database, DurabilityMode, IndexBound, IndexKey, KeyPart, StartupValidationMode, StorageConfig,
};
use mydb::types::{QueryError, Value};

const ROWS: i64 = 5_000;

fn main() -> Result<(), QueryError> {
    let profile = env::var("MYDB_STORAGE_BASELINE_PROFILE")
        .unwrap_or_else(|_| "legacy".into())
        .to_lowercase();
    match profile.as_str() {
        "buffer" | "buffer-pool" | "bufferpool" => run_buffer_profile(),
        "checkpoint" => run_checkpoint_profile(),
        _ => run_legacy_profile(),
    }
}

/// Generic large-DPT checkpoint benchmark. Examples:
///
/// ```text
/// MYDB_STORAGE_BASELINE_PROFILE=checkpoint \
/// MYDB_CHECKPOINT_BENCH_PAGES=100000 cargo run --release --features tools --bin storage_baseline
/// ```
///
/// The loader observes the public buffer-pool counters and keeps inserting a
/// normal wide-row workload until the requested dirty-page working set is
/// reached; there is no benchmark-specific engine path.
fn run_checkpoint_profile() -> Result<(), QueryError> {
    let pages = env_usize("MYDB_CHECKPOINT_BENCH_PAGES", 10_000);
    let runs = env_usize("MYDB_CHECKPOINT_BENCH_RUNS", 5).max(1);
    let io_threads = env_usize("MYDB_CHECKPOINT_BENCH_IO_THREADS", 4).max(1);
    let payload_bytes = env_usize("MYDB_CHECKPOINT_BENCH_PAYLOAD_BYTES", 3_000).max(1);
    let buffer_pages = env_u64(
        "MYDB_CHECKPOINT_BENCH_BUFFER_PAGES",
        (pages as u64).saturating_add(2_048),
    );
    let root = data_dir().join("checkpoint");
    let mut checkpoint_samples = Vec::with_capacity(runs);

    println!(
        "[config] profile=checkpoint pages={pages} runs={runs} io_threads={io_threads} \
         payload_bytes={payload_bytes} buffer_pages={buffer_pages}"
    );
    for run in 0..runs {
        let dir = root.join(format!("run-{run}"));
        if dir.exists() {
            std::fs::remove_dir_all(&dir).map_err(|err| {
                mydb::storage::error(
                    mydb::error::catalog::ER_GET_ERRNO,
                    format!("failed to clean checkpoint benchmark dir {dir:?}: {err}"),
                )
            })?;
        }
        let config = StorageConfig {
            durability: DurabilityMode::Relaxed,
            checkpoint_interval: Duration::ZERO,
            checkpoint_wal_bytes: u64::MAX,
            lock_wait: Duration::from_secs(30),
            group_commit_window: Duration::ZERO,
            wal_segment_bytes: 64 * 1024 * 1024,
            buffer_pool_pages: buffer_pages,
            buffer_pool_io_threads: io_threads,
            buffer_pool_prefetch_pages: 4,
            undo_retention: Duration::ZERO,
            heap_update_reserve_bytes: 16 * 1024 / 4,
            startup_validation: StartupValidationMode::Off,
            startup_validate_indexes: false,
        };
        let db = Database::open_with_config(&dir, config)?;
        db.create_table(
            "checkpoint_bench",
            vec![
                column("id", DataType::Int, false),
                column("payload", DataType::VarChar(Some(payload_bytes)), false),
            ],
            vec!["id".to_string()],
            Vec::new(),
        )?;

        const BATCH: usize = 64;
        let payload = "x".repeat(payload_bytes);
        let load_started = Instant::now();
        let mut commit_samples = Vec::new();
        let mut loaded_rows = 0_usize;
        while db.storage_status().buffer_pool_dirty_pages < pages as u64 {
            let start = loaded_rows;
            let end = start.saturating_add(BATCH);
            let txn = db.begin_txn()?;
            db.insert_rows(
                txn,
                "test.checkpoint_bench",
                (start..end)
                    .map(|id| vec![Value::Int(id as i64), Value::String(payload.clone())])
                    .collect(),
            )?;
            let commit_started = Instant::now();
            db.commit_txn(txn.txn_id)?;
            commit_samples.push(commit_started.elapsed());
            loaded_rows = end;
            assert!(
                loaded_rows <= pages.saturating_mul(64).saturating_add(BATCH),
                "could not create the requested dirty-page working set"
            );
        }
        let load_elapsed = load_started.elapsed();
        let commit_stats = LatencyStats::from(commit_samples, load_elapsed);
        let before = db.storage_status();
        let (_, managed_peak_before, _, _) = mydb::engine::controlled_memory_snapshot();
        let checkpoint_started = Instant::now();
        db.maybe_checkpoint()?;
        let checkpoint_wall = checkpoint_started.elapsed();
        let after = db.storage_status();
        let (managed_current, managed_peak_after, _, _) =
            mydb::engine::controlled_memory_snapshot();
        assert!(after.checkpoints_total > before.checkpoints_total);
        checkpoint_samples.push(checkpoint_wall);
        println!(
            "[checkpoint run={run}] dirty_pages_before={} dirty_pages_snapshot={} checkpoint_us={} snapshot_us={} \
             flush_us={} sync_us={} compact_us={} redo_lsn={} frame_bytes=64 \
             wal_reclaimed_bytes={} managed_current_bytes={} managed_peak_delta_bytes={} \
             load_tps={:.0} commit_p95_us={} commit_p99_us={}",
            before.buffer_pool_dirty_pages,
            after.last_checkpoint_dirty_pages,
            checkpoint_wall.as_micros(),
            after.last_checkpoint_snapshot_us,
            after.last_checkpoint_flush_us,
            after.last_checkpoint_sync_us,
            after.last_checkpoint_compact_us,
            after.checkpoint_redo_lsn,
            after.last_checkpoint_wal_reclaimed_bytes,
            managed_current,
            managed_peak_after.saturating_sub(managed_peak_before),
            loaded_rows as f64 / load_elapsed.as_secs_f64().max(1e-9),
            commit_stats.p95_us,
            commit_stats.p99_us,
        );
    }
    checkpoint_samples.sort();
    println!(
        "[checkpoint summary] runs={} median_us={}",
        checkpoint_samples.len(),
        checkpoint_samples[checkpoint_samples.len() / 2].as_micros()
    );
    Ok(())
}

fn run_legacy_profile() -> Result<(), QueryError> {
    let data_dir = data_dir();
    if data_dir.exists() {
        std::fs::remove_dir_all(&data_dir).map_err(|err| {
            mydb::storage::error(
                mydb::error::catalog::ER_GET_ERRNO,
                format!("failed to clean baseline data dir: {err}"),
            )
        })?;
    }

    let mut timings = Vec::new();
    let open_start = Instant::now();
    let db = Database::open_with_config(data_dir.clone(), legacy_storage_config())?;
    timings.push(("open_empty", open_start.elapsed()));
    create_schema(&db)?;

    let started = Instant::now();
    let txn = db.begin_txn()?;
    db.insert_rows(
        txn,
        "test.baseline",
        (0..ROWS)
            .map(|id| {
                vec![
                    Value::Int(id),
                    Value::Int(id % 128),
                    Value::String(format!("payload-{id:05}")),
                ]
            })
            .collect(),
    )?;
    db.commit_txn(txn.txn_id)?;
    timings.push(("bulk_insert_with_indexes", started.elapsed()));

    let table = db.table("test.baseline")?;
    let primary = table
        .indexes
        .iter()
        .find(|index| index.primary)
        .expect("primary index should exist");
    let secondary = table
        .indexes
        .iter()
        .find(|index| index.name == "k_idx")
        .expect("secondary index should exist");

    let started = Instant::now();
    for id in (0..ROWS).step_by(50) {
        let row_ids = db.lookup_index(&table, primary, IndexKey::from(vec![KeyPart::Int(id)]))?;
        for row_id in row_ids {
            let txn =
                db.read_only_statement_view(mydb::storage::IsolationLevel::ReadCommitted, None);
            let row = db.read_row(&table, row_id, txn)?;
            assert!(row.is_some());
        }
    }
    timings.push(("primary_point_lookup", started.elapsed()));

    let started = Instant::now();
    let row_ids = db.range_index_bounds_visible(
        &table,
        secondary,
        Some(IndexBound {
            key: IndexKey::from(vec![KeyPart::Int(40)]),
            inclusive: true,
        }),
        Some(IndexBound {
            key: IndexKey::from(vec![KeyPart::Int(60)]),
            inclusive: true,
        }),
        db.read_only_statement_view(mydb::storage::IsolationLevel::ReadCommitted, None),
    )?;
    assert!(!row_ids.is_empty());
    timings.push(("secondary_range_scan", started.elapsed()));

    let started = Instant::now();
    let txn = db.begin_txn()?;
    let updated = db.update_rows_with_changed_columns(
        txn,
        "test.baseline",
        None,
        Some(&[1, 2]),
        |values| Ok(matches!(values.first(), Some(Value::Int(id)) if id % 10 == 0)),
        |values| {
            let id = match values.first() {
                Some(Value::Int(id)) => *id,
                _ => 0,
            };
            Ok(vec![
                Value::Int(id),
                Value::Int(1_000 + (id % 128)),
                Value::String(format!("updated-{id:05}")),
            ])
        },
    )?;
    assert!(updated > 0);
    let deleted = db.delete_rows(txn, "test.baseline", None, |values| {
        Ok(matches!(values.first(), Some(Value::Int(id)) if id % 17 == 0))
    })?;
    assert!(deleted > 0);
    db.commit_txn(txn.txn_id)?;
    timings.push(("update_delete_mvcc_indexes", started.elapsed()));

    db.flush_dirty_data()?;
    let txn = db.begin_txn()?;
    db.insert_rows(
        txn,
        "test.baseline",
        (ROWS..ROWS + 100)
            .map(|id| {
                vec![
                    Value::Int(id),
                    Value::Int(id % 128),
                    Value::String(format!("recover-{id:05}")),
                ]
            })
            .collect(),
    )?;
    db.commit_txn(txn.txn_id)?;
    std::mem::forget(db);

    let started = Instant::now();
    let recovered = Database::open_with_config(data_dir.clone(), legacy_storage_config())?;
    timings.push(("recovery_open_uncheckpointed", started.elapsed()));
    recovered.check_consistency()?;
    recovered.checkpoint()?;

    for (name, elapsed) in timings {
        println!("{name}: {} us", elapsed.as_micros());
    }
    Ok(())
}

fn data_dir() -> PathBuf {
    env::var_os("MYDB_STORAGE_BASELINE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| env::temp_dir().join("mydb-storage-baseline"))
}

fn legacy_storage_config() -> StorageConfig {
    StorageConfig {
        durability: DurabilityMode::Relaxed,
        checkpoint_interval: Duration::from_secs(60),
        checkpoint_wal_bytes: 1024 * 1024 * 1024,
        lock_wait: Duration::from_secs(5),
        group_commit_window: Duration::ZERO,
        wal_segment_bytes: 64 * 1024 * 1024,
        buffer_pool_pages: 2_048,
        buffer_pool_io_threads: 1,
        buffer_pool_prefetch_pages: 4,
        undo_retention: Duration::from_secs(60),
        heap_update_reserve_bytes: 16 * 1024 / 4,
        startup_validation: StartupValidationMode::Auto,
        startup_validate_indexes: true,
    }
}

fn create_schema(db: &Database) -> Result<(), QueryError> {
    db.create_table(
        "baseline",
        vec![
            column("id", DataType::Int, false),
            column("k", DataType::Int, false),
            column("payload", DataType::VarChar(Some(64)), false),
        ],
        vec!["id".to_string()],
        vec![IndexDef {
            name: Some("k_idx".to_string()),
            columns: vec!["k".to_string()],
            column_prefix_lengths: Vec::new(),
            column_descending: Vec::new(),
            key_expressions: Vec::new(),
            key_expression_multi_valued: Vec::new(),
            unique: false,
            included_columns: Vec::new(),
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            fulltext_parser: String::new(),
            vector_options: None,
        }],
    )
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

// ---------------------------------------------------------------------------
// P0-2 buffer-pool microbenchmark profile.
//
// Five workloads designed to stress different buffer pool behaviors so the
// optimization roadmap (N-1..M-1) has a regression / progress gate that doesn't
// require a full TPC-H or TPC-C run.  Each workload writes one line to stdout:
//
//   [<name>] ops=<n> p50_us=<v> p99_us=<v> throughput_ops_per_s=<v> \
//            buffer_hit_pct=<v> evictions=<v> prefetches=<v> prefetch_hits=<v> \
//            read_ios=<v> ensure_capacity_busy_us=<v>
//
// Knobs (env vars):
//   MYDB_STORAGE_BASELINE_PROFILE=buffer        — opt into this path
//   MYDB_STORAGE_BASELINE_ROWS=<n>              — rows per table (default 200k)
//   MYDB_STORAGE_BASELINE_OPS=<n>               — ops per workload (default 20k)
//   MYDB_STORAGE_BASELINE_BUFFER_PAGES=<n>      — page cap (default 1024 = 16MB)
//   MYDB_STORAGE_BASELINE_WORKLOADS=a,b,c       — comma list to filter
// ---------------------------------------------------------------------------

fn run_buffer_profile() -> Result<(), QueryError> {
    let rows = env_usize("MYDB_STORAGE_BASELINE_ROWS", 200_000);
    let ops = env_usize("MYDB_STORAGE_BASELINE_OPS", 20_000);
    let buffer_pages = env_u64("MYDB_STORAGE_BASELINE_BUFFER_PAGES", 1_024);
    let filter = env::var("MYDB_STORAGE_BASELINE_WORKLOADS").ok();
    let filter_set: Option<Vec<String>> = filter.as_ref().map(|s| {
        s.split(',')
            .map(|t| t.trim().to_lowercase())
            .filter(|t| !t.is_empty())
            .collect()
    });

    println!(
        "[config] profile=buffer rows={rows} ops={ops} buffer_pages={buffer_pages} ({} MB)",
        buffer_pages * 16 / 1024
    );

    let workloads: Vec<(&str, fn(WorkloadParams) -> Result<(), QueryError>)> = vec![
        ("random", workload_random),
        ("sequential", workload_sequential),
        ("zipfian", workload_zipfian),
        ("heap_plus_index", workload_heap_plus_index),
        ("spill_interference", workload_spill_interference),
    ];

    for (name, runner) in workloads {
        if let Some(set) = filter_set.as_ref() {
            if !set.iter().any(|t| t == name) {
                continue;
            }
        }
        runner(WorkloadParams {
            name,
            rows,
            ops,
            buffer_pages,
        })?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct WorkloadParams {
    name: &'static str,
    rows: usize,
    ops: usize,
    buffer_pages: u64,
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn buffer_storage_config(pages: u64) -> StorageConfig {
    StorageConfig {
        durability: DurabilityMode::Relaxed,
        checkpoint_interval: Duration::from_secs(60),
        checkpoint_wal_bytes: 1024 * 1024 * 1024,
        lock_wait: Duration::from_secs(5),
        group_commit_window: Duration::ZERO,
        wal_segment_bytes: 64 * 1024 * 1024,
        buffer_pool_pages: pages,
        buffer_pool_io_threads: 2,
        buffer_pool_prefetch_pages: 4,
        undo_retention: Duration::from_secs(60),
        heap_update_reserve_bytes: 16 * 1024 / 4,
        startup_validation: StartupValidationMode::Off,
        startup_validate_indexes: false,
    }
}

fn open_fresh(name: &str, pages: u64) -> Result<(Database, PathBuf), QueryError> {
    let dir = env::temp_dir().join(format!("mydb-storage-baseline-buffer-{name}"));
    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(|err| {
            mydb::storage::error(
                mydb::error::catalog::ER_GET_ERRNO,
                format!("failed to clean baseline data dir {dir:?}: {err}"),
            )
        })?;
    }
    let db = Database::open_with_config(dir.clone(), buffer_storage_config(pages))?;
    Ok((db, dir))
}

fn load_baseline_rows(db: &Database, rows: usize) -> Result<(), QueryError> {
    create_schema(db)?;
    // Batch inserts to keep the WAL write loop short. 4k rows per
    // commit is enough to amortise commit overhead without blowing
    // the undo budget on tiny configs.
    const BATCH: usize = 4_096;
    let mut id = 0_i64;
    while (id as usize) < rows {
        let end = (id as usize + BATCH).min(rows) as i64;
        let txn = db.begin_txn()?;
        db.insert_rows(
            txn,
            "test.baseline",
            (id..end)
                .map(|i| {
                    vec![
                        Value::Int(i),
                        Value::Int(i % 128),
                        Value::String(format!("payload-{i:08}")),
                    ]
                })
                .collect(),
        )?;
        db.commit_txn(txn.txn_id)?;
        id = end;
    }
    db.checkpoint()?;
    Ok(())
}

struct LatencyStats {
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    p999_us: u64,
    throughput_ops_per_s: f64,
}

impl LatencyStats {
    fn from(mut samples: Vec<Duration>, total: Duration) -> Self {
        if samples.is_empty() {
            return Self {
                p50_us: 0,
                p95_us: 0,
                p99_us: 0,
                p999_us: 0,
                throughput_ops_per_s: 0.0,
            };
        }
        samples.sort();
        let n = samples.len();
        let pick = |q: usize| {
            let mut idx = (n * q + 999) / 1_000;
            if idx >= n {
                idx = n - 1;
            }
            samples[idx].as_micros().min(u128::from(u64::MAX)) as u64
        };
        Self {
            p50_us: pick(500),
            p95_us: pick(950),
            p99_us: pick(990),
            p999_us: pick(999),
            throughput_ops_per_s: n as f64 / total.as_secs_f64().max(1e-9),
        }
    }
}

fn run_status_snapshot(db: &Database) -> StatusSnapshot {
    let s = db.storage_status();
    StatusSnapshot {
        hit_pct: s.buffer_hit_pct,
        evictions: s.buffer_pool_evictions,
        prefetches: s.buffer_pool_prefetches,
        prefetch_hits: s.buffer_pool_prefetch_hits,
        read_ios: s.buffer_pool_read_ios,
        write_ios: s.buffer_pool_write_ios,
        ensure_capacity_busy_us: s.buffer_pool_ensure_capacity_busy_us,
        scan_intent_share_pct: percent_of(
            s.buffer_pool_read_ios_scan,
            s.buffer_pool_read_ios.max(1),
        ),
        spill_bytes_written: s.spill_bytes_written,
        spill_bytes_read: s.spill_bytes_read,
        shard_imbalance_pct: s.buffer_pool_shard_imbalance_pct,
    }
}

fn percent_of(num: u64, den: u64) -> u64 {
    if den == 0 {
        0
    } else {
        (num.saturating_mul(100) / den).min(100)
    }
}

struct StatusSnapshot {
    hit_pct: u64,
    evictions: u64,
    prefetches: u64,
    prefetch_hits: u64,
    read_ios: u64,
    write_ios: u64,
    ensure_capacity_busy_us: u64,
    scan_intent_share_pct: u64,
    spill_bytes_written: u64,
    spill_bytes_read: u64,
    shard_imbalance_pct: u64,
}

fn report(name: &str, samples: Vec<Duration>, total: Duration, status: StatusSnapshot) {
    let stats = LatencyStats::from(samples, total);
    println!(
        "[{name}] ops={ops} p50_us={p50} p99_us={p99} p999_us={p999} \
         throughput_ops_per_s={tp:.1} hit_pct={hp} evictions={ev} prefetches={pf} \
         prefetch_hits={ph} read_ios={ri} write_ios={wi} \
         ensure_capacity_busy_us={ec} scan_intent_share_pct={si} \
         spill_bytes_written={sw} spill_bytes_read={sr} shard_imbalance_pct={sh}",
        ops = ops_count(),
        p50 = stats.p50_us,
        p99 = stats.p99_us,
        p999 = stats.p999_us,
        tp = stats.throughput_ops_per_s,
        hp = status.hit_pct,
        ev = status.evictions,
        pf = status.prefetches,
        ph = status.prefetch_hits,
        ri = status.read_ios,
        wi = status.write_ios,
        ec = status.ensure_capacity_busy_us,
        si = status.scan_intent_share_pct,
        sw = status.spill_bytes_written,
        sr = status.spill_bytes_read,
        sh = status.shard_imbalance_pct,
    );
}

// We carry the ops count alongside the latency vector so the {ops} field in
// the printed line matches the actual call count (some workloads issue more
// than `params.ops` operations).
fn ops_count() -> usize {
    OPS_COUNT.with(|c| c.get())
}

thread_local! {
    static OPS_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn set_ops_count(n: usize) {
    OPS_COUNT.with(|c| c.set(n));
}

// LCG so we don't pull in rand. Approximately uniform over u64 — good
// enough for picking random keys.
struct Lcg(u64);
impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
}

// Zipfian (s ~ 1.0) generator backed by inverse CDF on log buckets.
// Approximate, but cheap and skewed enough that a small `n` of the
// universe receives most accesses.
struct Zipf {
    n: u64,
    lcg: Lcg,
}
impl Zipf {
    fn new(n: u64, seed: u64) -> Self {
        Self {
            n: n.max(1),
            lcg: Lcg::new(seed),
        }
    }
    fn next(&mut self) -> u64 {
        // r in (0, 1)
        let r = (self.lcg.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        let r = r.max(1e-12);
        // Pick rank by x^(-1/s) with s ≈ 1.0 — i.e. rank = n^(r-1)/r-ish.
        // Concretely: rank = (n+1)^r - 1, which gives the bottom ranks
        // ~most of the mass. Bias of order one is fine; we just need a
        // working set < buffer.
        let rank = ((self.n as f64 + 1.0).powf(r) - 1.0).floor() as u64;
        rank.min(self.n - 1)
    }
}

fn workload_random(params: WorkloadParams) -> Result<(), QueryError> {
    let (db, _dir) = open_fresh(params.name, params.buffer_pages)?;
    load_baseline_rows(&db, params.rows)?;
    let table = db.table("test.baseline")?;
    let primary = table
        .indexes
        .iter()
        .find(|i| i.primary)
        .expect("primary index");
    let mut lcg = Lcg::new(0xdead_beef);
    let mut samples = Vec::with_capacity(params.ops);
    let total_start = Instant::now();
    for _ in 0..params.ops {
        let id = (lcg.next_u64() % params.rows as u64) as i64;
        let t = Instant::now();
        let row_ids = db.lookup_index(&table, primary, IndexKey::from(vec![KeyPart::Int(id)]))?;
        for row_id in row_ids {
            let txn =
                db.read_only_statement_view(mydb::storage::IsolationLevel::ReadCommitted, None);
            let _ = db.read_row(&table, row_id, txn)?;
        }
        samples.push(t.elapsed());
    }
    let total = total_start.elapsed();
    set_ops_count(params.ops);
    report(params.name, samples, total, run_status_snapshot(&db));
    Ok(())
}

fn workload_sequential(params: WorkloadParams) -> Result<(), QueryError> {
    // Sequential scan walks every key in primary order; the per-page
    // cost reflects prefetch hit ratio + cache-friendliness of the
    // index leaf chain.
    let (db, _dir) = open_fresh(params.name, params.buffer_pages)?;
    load_baseline_rows(&db, params.rows)?;
    let table = db.table("test.baseline")?;
    let primary = table
        .indexes
        .iter()
        .find(|i| i.primary)
        .expect("primary index");
    let mut samples = Vec::with_capacity(params.ops);
    let total_start = Instant::now();
    let stride = (params.rows / params.ops).max(1) as i64;
    let mut cursor: i64 = 0;
    for _ in 0..params.ops {
        let t = Instant::now();
        let row_ids =
            db.lookup_index(&table, primary, IndexKey::from(vec![KeyPart::Int(cursor)]))?;
        for row_id in row_ids {
            let txn =
                db.read_only_statement_view(mydb::storage::IsolationLevel::ReadCommitted, None);
            let _ = db.read_row(&table, row_id, txn)?;
        }
        samples.push(t.elapsed());
        cursor = (cursor + stride) % params.rows as i64;
    }
    let total = total_start.elapsed();
    set_ops_count(params.ops);
    report(params.name, samples, total, run_status_snapshot(&db));
    Ok(())
}

fn workload_zipfian(params: WorkloadParams) -> Result<(), QueryError> {
    let (db, _dir) = open_fresh(params.name, params.buffer_pages)?;
    load_baseline_rows(&db, params.rows)?;
    let table = db.table("test.baseline")?;
    let primary = table
        .indexes
        .iter()
        .find(|i| i.primary)
        .expect("primary index");
    let mut zipf = Zipf::new(params.rows as u64, 0xc0ffee);
    let mut samples = Vec::with_capacity(params.ops);
    let total_start = Instant::now();
    for _ in 0..params.ops {
        let id = zipf.next() as i64;
        let t = Instant::now();
        let row_ids = db.lookup_index(&table, primary, IndexKey::from(vec![KeyPart::Int(id)]))?;
        for row_id in row_ids {
            let txn =
                db.read_only_statement_view(mydb::storage::IsolationLevel::ReadCommitted, None);
            let _ = db.read_row(&table, row_id, txn)?;
        }
        samples.push(t.elapsed());
    }
    let total = total_start.elapsed();
    set_ops_count(params.ops);
    report(params.name, samples, total, run_status_snapshot(&db));
    Ok(())
}

fn workload_heap_plus_index(params: WorkloadParams) -> Result<(), QueryError> {
    // Alternate: hit primary index (heap row materialise) vs scan
    // secondary index range. Stresses both heap and index page
    // residency at once.
    let (db, _dir) = open_fresh(params.name, params.buffer_pages)?;
    load_baseline_rows(&db, params.rows)?;
    let table = db.table("test.baseline")?;
    let primary = table
        .indexes
        .iter()
        .find(|i| i.primary)
        .expect("primary index");
    let secondary = table
        .indexes
        .iter()
        .find(|i| i.name == "k_idx")
        .expect("secondary k_idx");
    let mut lcg = Lcg::new(0x1234_5678);
    let mut samples = Vec::with_capacity(params.ops);
    let total_start = Instant::now();
    for i in 0..params.ops {
        let t = Instant::now();
        if i % 2 == 0 {
            let id = (lcg.next_u64() % params.rows as u64) as i64;
            let row_ids =
                db.lookup_index(&table, primary, IndexKey::from(vec![KeyPart::Int(id)]))?;
            for row_id in row_ids {
                let txn =
                    db.read_only_statement_view(mydb::storage::IsolationLevel::ReadCommitted, None);
                let _ = db.read_row(&table, row_id, txn)?;
            }
        } else {
            // Narrow secondary range (4 keys deep) — exercises a
            // single index leaf without dragging the heap.
            let k = (lcg.next_u64() % 124) as i64;
            let txn =
                db.read_only_statement_view(mydb::storage::IsolationLevel::ReadCommitted, None);
            let _ = db.range_index_bounds_visible(
                &table,
                secondary,
                Some(IndexBound {
                    key: IndexKey::from(vec![KeyPart::Int(k)]),
                    inclusive: true,
                }),
                Some(IndexBound {
                    key: IndexKey::from(vec![KeyPart::Int(k + 3)]),
                    inclusive: true,
                }),
                txn,
            )?;
        }
        samples.push(t.elapsed());
    }
    let total = total_start.elapsed();
    set_ops_count(params.ops);
    report(params.name, samples, total, run_status_snapshot(&db));
    Ok(())
}

fn workload_spill_interference(params: WorkloadParams) -> Result<(), QueryError> {
    // We don't drive grace/hash spill from this microbench (would
    // require an executor), so the "spill" pressure is approximated
    // by a full-table seq scan ahead of a random workload — i.e.,
    // the scan pollutes the buffer pool with cold pages and we then
    // measure how well random hot-key reads survive.
    let (db, _dir) = open_fresh(params.name, params.buffer_pages)?;
    load_baseline_rows(&db, params.rows)?;
    let table = db.table("test.baseline")?;
    let primary = table
        .indexes
        .iter()
        .find(|i| i.primary)
        .expect("primary index");

    // Pollute the pool: scan every 50th key, forcing the cache to
    // touch a fraction of all heap pages.
    {
        let txn = db.read_only_statement_view(mydb::storage::IsolationLevel::ReadCommitted, None);
        let _ = db.range_index_bounds_visible(
            &table,
            primary,
            Some(IndexBound {
                key: IndexKey::from(vec![KeyPart::Int(0)]),
                inclusive: true,
            }),
            Some(IndexBound {
                key: IndexKey::from(vec![KeyPart::Int(params.rows as i64 - 1)]),
                inclusive: true,
            }),
            txn,
        )?;
    }

    // Now run random hot-key reads against a small working set.
    let working_set = (params.rows / 32).max(1) as u64;
    let mut lcg = Lcg::new(0xabad_cafe);
    let mut samples = Vec::with_capacity(params.ops);
    let total_start = Instant::now();
    for _ in 0..params.ops {
        let id = (lcg.next_u64() % working_set) as i64;
        let t = Instant::now();
        let row_ids = db.lookup_index(&table, primary, IndexKey::from(vec![KeyPart::Int(id)]))?;
        for row_id in row_ids {
            let txn =
                db.read_only_statement_view(mydb::storage::IsolationLevel::ReadCommitted, None);
            let _ = db.read_row(&table, row_id, txn)?;
        }
        samples.push(t.elapsed());
    }
    let total = total_start.elapsed();
    set_ops_count(params.ops);
    report(params.name, samples, total, run_status_snapshot(&db));
    Ok(())
}
