// dml_bench — Baseline DML throughput / WAL / storage footprint for
// the heap+index improvement roadmap (Phases 1-5).
//
// Seven workloads × two schemas, plus a tiny configurable knob set.
// Every workload prints one line per schema with:
//
//   [<schema>/<workload>] ops=<n> p50_us=<v> p99_us=<v>
//   throughput_ops_per_s=<v> wal_bytes_per_op=<v> heap_bytes=<v>
//   index_bytes=<v> row_migrations=<v>
//
// Schemas
//   narrow : 5 BIGINT columns + PRIMARY KEY (id) + 1 secondary index on k
//   wide   : 30 columns (mix BIGINT + VARCHAR(64)) + PK + 3 secondary indexes
//
// Workloads
//   insert         : raw INSERT throughput, fresh empty table
//   auto_increment : 1/8/32-thread single-row AUTO_INCREMENT autocommit
//   update_1col    : UPDATE one non-indexed column for a random row
//   update_ncol    : UPDATE three columns including one indexed
//   delete         : DELETE one random row
//   point_read     : PK lookup of one random row + row materialise
//   range_scan     : secondary index range scan, ~64 rows per op
//   index_only     : secondary index range scan with no heap fetch
//                    (covered by index columns)
//
// Knobs (env vars):
//   MYDB_DML_BENCH_ROWS    initial rows per table (default 100_000)
//   MYDB_DML_BENCH_OPS     ops per workload (default 20_000)
//   MYDB_DML_BENCH_FULL    if "1": force ROWS=1_000_000 OPS=200_000
//   MYDB_DML_BENCH_FILTER  comma list to run only some workloads
//   MYDB_DML_BENCH_SCHEMAS comma list ("narrow,wide"), default both
//   MYDB_DML_BENCH_AI_THREADS comma list (default "1,8,32")
//   MYDB_DML_BENCH_DIR     base dir for fresh data dirs (default tmp)
//
// Durability is set to Strict so WAL bytes/op is comparable across
// phases. Each (schema, workload) opens a fresh data dir so file
// sizes reflect only that workload's footprint.

use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use mydb::parser::{ColumnDef, DataType, IndexDef};
use mydb::storage::{
    Database, DurabilityMode, IndexBound, IndexKey, IsolationLevel, KeyPart, StartupValidationMode,
    StorageConfig,
};
use mydb::types::{QueryError, Value};

fn main() -> Result<(), QueryError> {
    let cfg = BenchConfig::from_env();
    println!(
        "[config] rows={} ops={} schemas={:?} workloads={:?}",
        cfg.rows, cfg.ops, cfg.schemas, cfg.workloads
    );

    let workloads: Vec<(&str, fn(&BenchConfig, SchemaKind) -> Result<(), QueryError>)> = vec![
        ("insert", workload_insert),
        ("auto_increment", workload_auto_increment),
        ("update_1col", workload_update_1col),
        ("update_ncol", workload_update_ncol),
        ("delete", workload_delete),
        ("point_read", workload_point_read),
        ("range_scan", workload_range_scan),
        ("index_only", workload_index_only),
    ];

    for schema in &cfg.schemas.clone() {
        for (name, runner) in &workloads {
            if !cfg.workloads.iter().any(|w| w == name) {
                continue;
            }
            runner(&cfg, *schema)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct BenchConfig {
    rows: usize,
    ops: usize,
    schemas: Vec<SchemaKind>,
    workloads: Vec<String>,
    base_dir: PathBuf,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum SchemaKind {
    Narrow,
    Wide,
}

impl SchemaKind {
    fn name(self) -> &'static str {
        match self {
            SchemaKind::Narrow => "narrow",
            SchemaKind::Wide => "wide",
        }
    }
}

impl BenchConfig {
    fn from_env() -> Self {
        let full = env::var("MYDB_DML_BENCH_FULL").ok().as_deref() == Some("1");
        let rows = env_usize(
            "MYDB_DML_BENCH_ROWS",
            if full { 1_000_000 } else { 100_000 },
        );
        let ops = env_usize("MYDB_DML_BENCH_OPS", if full { 200_000 } else { 20_000 });
        let schemas = env::var("MYDB_DML_BENCH_SCHEMAS")
            .ok()
            .map(|s| {
                s.split(',')
                    .filter_map(|t| match t.trim().to_lowercase().as_str() {
                        "narrow" => Some(SchemaKind::Narrow),
                        "wide" => Some(SchemaKind::Wide),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| vec![SchemaKind::Narrow, SchemaKind::Wide]);
        let workloads = env::var("MYDB_DML_BENCH_FILTER")
            .ok()
            .map(|s| {
                s.split(',')
                    .map(|t| t.trim().to_lowercase())
                    .filter(|t| !t.is_empty())
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| {
                vec![
                    "insert",
                    "auto_increment",
                    "update_1col",
                    "update_ncol",
                    "delete",
                    "point_read",
                    "range_scan",
                    "index_only",
                ]
                .into_iter()
                .map(String::from)
                .collect()
            });
        let base_dir = env::var_os("MYDB_DML_BENCH_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| env::temp_dir().join("mydb-dml-bench"));
        Self {
            rows,
            ops,
            schemas,
            workloads,
            base_dir,
        }
    }
}

// ---------------------------------------------------------------------------
// DB open / schema
// ---------------------------------------------------------------------------

fn bench_storage_config() -> StorageConfig {
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
        undo_retention: Duration::from_secs(10),
        heap_update_reserve_bytes: 16 * 1024 / 16,
        startup_validation: StartupValidationMode::Off,
        startup_validate_indexes: false,
    }
}

fn open_fresh(
    cfg: &BenchConfig,
    schema: SchemaKind,
    tag: &str,
) -> Result<(Database, PathBuf), QueryError> {
    let dir = cfg.base_dir.join(format!("{}-{}", schema.name(), tag));
    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(io_err)?;
    }
    let db = Database::open_with_config(dir.clone(), bench_storage_config())?;
    create_schema(&db, schema)?;
    Ok((db, dir))
}

fn create_schema(db: &Database, schema: SchemaKind) -> Result<(), QueryError> {
    match schema {
        SchemaKind::Narrow => db.create_table(
            "bench",
            vec![
                col_int("id"),
                col_int("k"),
                col_int("c1"),
                col_int("c2"),
                col_int("c3"),
            ],
            vec!["id".into()],
            vec![sec_index("k_idx", &["k"])],
        ),
        SchemaKind::Wide => {
            let mut columns = vec![col_int("id")];
            for i in 1..=3 {
                columns.push(col_int(&format!("k{i}")));
            }
            for i in 1..=10 {
                columns.push(col_int(&format!("n{i}")));
            }
            for i in 1..=16 {
                columns.push(col_varchar(&format!("s{i}"), 64));
            }
            db.create_table(
                "bench",
                columns,
                vec!["id".into()],
                vec![
                    sec_index("k1_idx", &["k1"]),
                    sec_index("k2_idx", &["k2"]),
                    sec_index("k3_idx", &["k3"]),
                ],
            )
        }
    }
}

fn col_int(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type: DataType::BigInt,
        unsigned: false,
        zerofill: false,
        invisible: false,
        nullable: false,
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

fn col_varchar(name: &str, len: u32) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type: DataType::VarChar(Some(len as usize)),
        unsigned: false,
        zerofill: false,
        invisible: false,
        nullable: false,
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

fn sec_index(name: &str, cols: &[&str]) -> IndexDef {
    let n = cols.len();
    IndexDef {
        name: Some(name.into()),
        columns: cols.iter().map(|s| s.to_string()).collect(),
        column_prefix_lengths: Vec::new(),
        column_descending: Vec::new(),
        key_expressions: vec![None; n],
        key_expression_multi_valued: vec![false; n],
        unique: false,
        included_columns: Vec::new(),
        index_type: "BTREE".into(),
        visible: true,
        comment: String::new(),
        fulltext_parser: String::new(),
    }
}

// ---------------------------------------------------------------------------
// Row factories
// ---------------------------------------------------------------------------

fn row_for(schema: SchemaKind, id: i64) -> Vec<Value> {
    match schema {
        SchemaKind::Narrow => vec![
            Value::Int(id),
            Value::Int(id % 1024),
            Value::Int(id * 3),
            Value::Int(id * 5),
            Value::Int(id * 7),
        ],
        SchemaKind::Wide => {
            let mut row = Vec::with_capacity(30);
            row.push(Value::Int(id));
            for i in 1..=3 {
                row.push(Value::Int((id + i) % 1024));
            }
            for i in 1..=10 {
                row.push(Value::Int(id * (i as i64 + 1)));
            }
            for i in 1..=16 {
                row.push(Value::String(format!("s{}-{:09}", i, id)));
            }
            row
        }
    }
}

// Position of an indexed `k` (or `k1`) column, used by range scans.
fn index_col_offset(schema: SchemaKind) -> usize {
    match schema {
        SchemaKind::Narrow => 1,
        SchemaKind::Wide => 1,
    }
}

// Position of a non-indexed column to mutate in update_1col.
fn nonindexed_col(schema: SchemaKind) -> usize {
    match schema {
        SchemaKind::Narrow => 2, // c1
        SchemaKind::Wide => 4,   // n1
    }
}

// ---------------------------------------------------------------------------
// Bulk load
// ---------------------------------------------------------------------------

fn bulk_load(db: &Database, schema: SchemaKind, rows: usize) -> Result<(), QueryError> {
    const BATCH: usize = 4_096;
    let mut next = 0i64;
    while (next as usize) < rows {
        let end = (next as usize + BATCH).min(rows) as i64;
        let txn = db.begin_txn()?;
        db.insert_rows(
            txn,
            "test.bench",
            (next..end).map(|i| row_for(schema, i)).collect(),
        )?;
        db.commit_txn(txn.txn_id)?;
        next = end;
    }
    db.flush_dirty_data()?;
    db.checkpoint()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Workloads
// ---------------------------------------------------------------------------

fn workload_insert(cfg: &BenchConfig, schema: SchemaKind) -> Result<(), QueryError> {
    let (db, dir) = open_fresh(cfg, schema, "insert")?;
    let wal_before = db.storage_status().wal_bytes;

    let mut samples = Vec::with_capacity(cfg.ops);
    let total_start = Instant::now();
    // One row per transaction so latency reflects realistic single-row
    // INSERT throughput, not bulk.
    for id in 0..cfg.ops as i64 {
        let t = Instant::now();
        let txn = db.begin_txn()?;
        db.insert_rows(txn, "test.bench", vec![row_for(schema, id)])?;
        db.commit_txn(txn.txn_id)?;
        samples.push(t.elapsed());
    }
    let total = total_start.elapsed();
    db.flush_dirty_data()?;

    let wal_after = db.storage_status().wal_bytes;
    let migrations = db.storage_status().row_migrations;
    report(
        schema,
        "insert",
        samples,
        total,
        wal_after - wal_before,
        &dir,
        migrations,
    );
    Ok(())
}

fn workload_auto_increment(cfg: &BenchConfig, schema: SchemaKind) -> Result<(), QueryError> {
    if schema != SchemaKind::Narrow {
        return Ok(());
    }
    let threads = env::var("MYDB_DML_BENCH_AI_THREADS")
        .unwrap_or_else(|_| "1,8,32".to_string())
        .split(',')
        .filter_map(|value| value.trim().parse::<usize>().ok())
        .filter(|threads| *threads > 0)
        .collect::<Vec<_>>();
    for threads in threads {
        let tag = format!("auto_increment_c{threads}");
        let dir = cfg.base_dir.join(format!("narrow-{tag}"));
        if dir.exists() {
            std::fs::remove_dir_all(&dir).map_err(io_err)?;
        }
        let db = std::sync::Arc::new(Database::open_with_config(
            dir.clone(),
            bench_storage_config(),
        )?);
        let mut id = col_int("id");
        id.auto_increment = true;
        db.create_table(
            "bench_ai",
            vec![id, col_int("v")],
            vec!["id".into()],
            Vec::new(),
        )?;
        let wal_before = db.storage_status().wal_bytes;
        let total_start = Instant::now();
        let mut handles = Vec::with_capacity(threads);
        for worker in 0..threads {
            let db = std::sync::Arc::clone(&db);
            let begin = cfg.ops.saturating_mul(worker) / threads;
            let end = cfg.ops.saturating_mul(worker + 1) / threads;
            handles.push(std::thread::spawn(
                move || -> Result<Vec<Duration>, QueryError> {
                    let mut samples = Vec::with_capacity(end.saturating_sub(begin));
                    for value in begin..end {
                        let started = Instant::now();
                        let txn = db.begin_txn()?;
                        db.insert_rows(
                            txn,
                            "test.bench_ai",
                            vec![vec![Value::Null, Value::Int(value as i64)]],
                        )?;
                        db.commit_txn(txn.txn_id)?;
                        samples.push(started.elapsed());
                    }
                    Ok(samples)
                },
            ));
        }
        let mut samples = Vec::with_capacity(cfg.ops);
        for handle in handles {
            samples.extend(handle.join().map_err(|_| {
                io_err(std::io::Error::other(
                    "AUTO_INCREMENT benchmark worker panicked",
                ))
            })??);
        }
        let total = total_start.elapsed();
        db.flush_dirty_data()?;
        let wal_after = db.storage_status().wal_bytes;
        report(
            SchemaKind::Narrow,
            &tag,
            samples,
            total,
            wal_after.saturating_sub(wal_before),
            &dir,
            db.storage_status().row_migrations,
        );
    }
    Ok(())
}

fn workload_update_1col(cfg: &BenchConfig, schema: SchemaKind) -> Result<(), QueryError> {
    let (db, dir) = open_fresh(cfg, schema, "update_1col")?;
    bulk_load(&db, schema, cfg.rows)?;

    let migrations_before = db.storage_status().row_migrations;
    let wal_before = db.storage_status().wal_bytes;
    let col_to_change = nonindexed_col(schema);
    let mut lcg = Lcg::new(0xdead_beef);

    let mut samples = Vec::with_capacity(cfg.ops);
    let total_start = Instant::now();
    for _ in 0..cfg.ops {
        let id = (lcg.next_u64() % cfg.rows as u64) as i64;
        let t = Instant::now();
        let txn = db.begin_txn()?;
        db.update_rows_with_changed_columns(
            txn,
            "test.bench",
            None,
            Some(&[col_to_change]),
            |vals| Ok(matches!(vals.first(), Some(Value::Int(v)) if *v == id)),
            |vals| {
                let mut new = vals.to_vec();
                if let Some(slot) = new.get_mut(col_to_change) {
                    if let Value::Int(v) = slot {
                        *v = v.wrapping_add(1);
                    }
                }
                Ok(new)
            },
        )?;
        db.commit_txn(txn.txn_id)?;
        samples.push(t.elapsed());
    }
    let total = total_start.elapsed();
    db.flush_dirty_data()?;
    let wal_after = db.storage_status().wal_bytes;
    let migrations = db.storage_status().row_migrations - migrations_before;
    report(
        schema,
        "update_1col",
        samples,
        total,
        wal_after - wal_before,
        &dir,
        migrations,
    );
    Ok(())
}

fn workload_update_ncol(cfg: &BenchConfig, schema: SchemaKind) -> Result<(), QueryError> {
    let (db, dir) = open_fresh(cfg, schema, "update_ncol")?;
    bulk_load(&db, schema, cfg.rows)?;

    let migrations_before = db.storage_status().row_migrations;
    let wal_before = db.storage_status().wal_bytes;
    // Three columns: one indexed + two non-indexed. Tests
    // index_may_be_affected_by_columns hot path and WAL/undo for
    // multi-col delta.
    let cols_to_change: Vec<usize> = match schema {
        SchemaKind::Narrow => vec![1, 2, 3], // k, c1, c2
        SchemaKind::Wide => vec![1, 4, 5],   // k1, n1, n2
    };
    let mut lcg = Lcg::new(0xfeed_face);

    let mut samples = Vec::with_capacity(cfg.ops);
    let total_start = Instant::now();
    for i in 0..cfg.ops {
        let id = (lcg.next_u64() % cfg.rows as u64) as i64;
        let bump = i as i64;
        let t = Instant::now();
        let txn = db.begin_txn()?;
        let cols_ref = cols_to_change.clone();
        db.update_rows_with_changed_columns(
            txn,
            "test.bench",
            None,
            Some(&cols_ref),
            |vals| Ok(matches!(vals.first(), Some(Value::Int(v)) if *v == id)),
            |vals| {
                let mut new = vals.to_vec();
                for c in &cols_ref {
                    if let Some(Value::Int(v)) = new.get_mut(*c) {
                        *v = v.wrapping_add(bump);
                    }
                }
                Ok(new)
            },
        )?;
        db.commit_txn(txn.txn_id)?;
        samples.push(t.elapsed());
    }
    let total = total_start.elapsed();
    db.flush_dirty_data()?;
    let wal_after = db.storage_status().wal_bytes;
    let migrations = db.storage_status().row_migrations - migrations_before;
    report(
        schema,
        "update_ncol",
        samples,
        total,
        wal_after - wal_before,
        &dir,
        migrations,
    );
    Ok(())
}

fn workload_delete(cfg: &BenchConfig, schema: SchemaKind) -> Result<(), QueryError> {
    let (db, dir) = open_fresh(cfg, schema, "delete")?;
    bulk_load(&db, schema, cfg.rows)?;

    let wal_before = db.storage_status().wal_bytes;
    let migrations_before = db.storage_status().row_migrations;
    let ops = cfg.ops.min(cfg.rows / 2); // keep table non-empty
    let stride = (cfg.rows / ops.max(1)).max(1) as i64;
    let mut cursor: i64 = 0;

    let mut samples = Vec::with_capacity(ops);
    let total_start = Instant::now();
    for _ in 0..ops {
        let id = cursor;
        cursor += stride;
        let t = Instant::now();
        let txn = db.begin_txn()?;
        db.delete_rows(txn, "test.bench", None, |vals| {
            Ok(matches!(vals.first(), Some(Value::Int(v)) if *v == id))
        })?;
        db.commit_txn(txn.txn_id)?;
        samples.push(t.elapsed());
    }
    let total = total_start.elapsed();
    db.flush_dirty_data()?;
    let wal_after = db.storage_status().wal_bytes;
    let migrations = db.storage_status().row_migrations - migrations_before;
    report(
        schema,
        "delete",
        samples,
        total,
        wal_after - wal_before,
        &dir,
        migrations,
    );
    Ok(())
}

fn workload_point_read(cfg: &BenchConfig, schema: SchemaKind) -> Result<(), QueryError> {
    let (db, dir) = open_fresh(cfg, schema, "point_read")?;
    bulk_load(&db, schema, cfg.rows)?;

    let wal_before = db.storage_status().wal_bytes;
    let table = db.table("test.bench")?;
    let pk = table.indexes.iter().find(|i| i.primary).expect("pk");
    let mut lcg = Lcg::new(0xc0ff_ee);

    let mut samples = Vec::with_capacity(cfg.ops);
    let total_start = Instant::now();
    for _ in 0..cfg.ops {
        let id = (lcg.next_u64() % cfg.rows as u64) as i64;
        let t = Instant::now();
        let row_ids = db.lookup_index(&table, pk, IndexKey::from(vec![KeyPart::Int(id)]))?;
        for rid in row_ids {
            let txn = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
            let _ = db.read_row(&table, rid, txn)?;
        }
        samples.push(t.elapsed());
    }
    let total = total_start.elapsed();
    let wal_after = db.storage_status().wal_bytes;
    report(
        schema,
        "point_read",
        samples,
        total,
        wal_after - wal_before,
        &dir,
        0,
    );
    Ok(())
}

fn workload_range_scan(cfg: &BenchConfig, schema: SchemaKind) -> Result<(), QueryError> {
    let (db, dir) = open_fresh(cfg, schema, "range_scan")?;
    bulk_load(&db, schema, cfg.rows)?;

    let wal_before = db.storage_status().wal_bytes;
    let table = db.table("test.bench")?;
    let sec = table
        .indexes
        .iter()
        .find(|i| matches!(i.name.as_str(), "k_idx" | "k1_idx"))
        .expect("secondary index");
    let mut lcg = Lcg::new(0x1234_5678);
    let _ = index_col_offset(schema);

    let mut samples = Vec::with_capacity(cfg.ops);
    let total_start = Instant::now();
    for _ in 0..cfg.ops {
        // Each scan returns ~64 keys' worth of rows (k % 1024 has
        // roughly ceil(rows/1024) per value).
        let k_lo = (lcg.next_u64() % 1024) as i64;
        let k_hi = k_lo + 1;
        let t = Instant::now();
        let txn = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
        let row_ids = db.range_index_bounds_visible(
            &table,
            sec,
            Some(IndexBound {
                key: IndexKey::from(vec![KeyPart::Int(k_lo)]),
                inclusive: true,
            }),
            Some(IndexBound {
                key: IndexKey::from(vec![KeyPart::Int(k_hi)]),
                inclusive: false,
            }),
            txn,
        )?;
        for rid in row_ids {
            let txn = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
            let _ = db.read_row(&table, rid, txn)?;
        }
        samples.push(t.elapsed());
    }
    let total = total_start.elapsed();
    let wal_after = db.storage_status().wal_bytes;
    report(
        schema,
        "range_scan",
        samples,
        total,
        wal_after - wal_before,
        &dir,
        0,
    );
    Ok(())
}

fn workload_index_only(cfg: &BenchConfig, schema: SchemaKind) -> Result<(), QueryError> {
    // Index-only: same secondary range scan but skip the heap fetch.
    // Leaf writer bounds can skip visibility probes; otherwise only
    // heap MVCC headers are read, with heap/undo fallback as needed.
    let (db, dir) = open_fresh(cfg, schema, "index_only")?;
    bulk_load(&db, schema, cfg.rows)?;
    // Checkpoint the loaded data before timing index-only scans.
    db.checkpoint()?;

    let wal_before = db.storage_status().wal_bytes;
    let table = db.table("test.bench")?;
    let sec = table
        .indexes
        .iter()
        .find(|i| matches!(i.name.as_str(), "k_idx" | "k1_idx"))
        .expect("secondary index");
    let mut lcg = Lcg::new(0xabad_cafe);

    let mut samples = Vec::with_capacity(cfg.ops);
    let total_start = Instant::now();
    for _ in 0..cfg.ops {
        let k_lo = (lcg.next_u64() % 1024) as i64;
        let k_hi = k_lo + 1;
        let t = Instant::now();
        let txn = db.read_only_statement_view(IsolationLevel::ReadCommitted, None);
        let _ = db.range_index_bounds_visible(
            &table,
            sec,
            Some(IndexBound {
                key: IndexKey::from(vec![KeyPart::Int(k_lo)]),
                inclusive: true,
            }),
            Some(IndexBound {
                key: IndexKey::from(vec![KeyPart::Int(k_hi)]),
                inclusive: false,
            }),
            txn,
        )?;
        samples.push(t.elapsed());
    }
    let total = total_start.elapsed();
    let wal_after = db.storage_status().wal_bytes;
    report(
        schema,
        "index_only",
        samples,
        total,
        wal_after - wal_before,
        &dir,
        0,
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

fn report(
    schema: SchemaKind,
    workload: &str,
    samples: Vec<Duration>,
    total: Duration,
    wal_bytes: u64,
    dir: &PathBuf,
    migrations: u64,
) {
    let n = samples.len().max(1);
    let stats = LatencyStats::from(samples, total);
    let wal_per_op = wal_bytes as f64 / n as f64;
    let heap_dir = mydb::storage::naming::schema_tables_dir(dir, "test");
    let idx_dir = mydb::storage::naming::schema_indexes_dir(dir, "test");
    let heap_bytes = total_size(&heap_dir).unwrap_or(0);
    let index_bytes = total_size(&idx_dir).unwrap_or(0);
    let hits = mydb::storage::update_delta_hits();
    let misses = mydb::storage::update_delta_misses();
    println!(
        "[{schema}/{workload}] ops={n} p50_us={p50} p99_us={p99} \
         throughput_ops_per_s={tp:.1} wal_bytes_per_op={wal:.1} \
         heap_bytes={heap} index_bytes={idx} row_migrations={mig} \
         delta_hits={hits} delta_misses={misses}",
        schema = schema.name(),
        workload = workload,
        n = n,
        p50 = stats.p50_us,
        p99 = stats.p99_us,
        tp = stats.throughput_ops_per_s,
        wal = wal_per_op,
        heap = heap_bytes,
        idx = index_bytes,
        mig = migrations,
        hits = hits,
        misses = misses,
    );
}

struct LatencyStats {
    p50_us: u64,
    p99_us: u64,
    throughput_ops_per_s: f64,
}

impl LatencyStats {
    fn from(mut samples: Vec<Duration>, total: Duration) -> Self {
        if samples.is_empty() {
            return Self {
                p50_us: 0,
                p99_us: 0,
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
            p99_us: pick(990),
            throughput_ops_per_s: n as f64 / total.as_secs_f64().max(1e-9),
        }
    }
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn io_err(err: std::io::Error) -> QueryError {
    mydb::storage::error(
        mydb::error::catalog::ER_GET_ERRNO,
        format!("dml_bench IO: {err}"),
    )
}

fn total_size(dir: &PathBuf) -> Result<u64, QueryError> {
    let mut acc = 0u64;
    if !dir.exists() {
        return Ok(0);
    }
    for entry in std::fs::read_dir(dir).map_err(io_err)? {
        let entry = entry.map_err(io_err)?;
        let m = entry.metadata().map_err(io_err)?;
        if m.is_file() {
            acc += m.len();
        } else if m.is_dir() {
            acc += total_size(&entry.path())?;
        }
    }
    Ok(acc)
}

// LCG for cheap random key picks; no rand dependency.
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
